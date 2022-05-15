package client

import (
	"bytes"
	"context"
	"fmt"
	"github.com/hhzhhzhhz/k8s-job-scheduler/apiserver"
	"github.com/hhzhhzhhz/k8s-job-scheduler/config"
	"github.com/hhzhhzhhz/k8s-job-scheduler/deploy"
	"github.com/hhzhhzhhz/k8s-job-scheduler/election"
	"github.com/hhzhhzhhz/k8s-job-scheduler/entity"
	"github.com/hhzhhzhhz/k8s-job-scheduler/infrastructure/rabbitmq"
	"github.com/hhzhhzhhz/k8s-job-scheduler/log"
	"github.com/hhzhhzhhz/k8s-job-scheduler/pkg/constant"
	"github.com/hhzhhzhhz/k8s-job-scheduler/pkg/utils"
	json "github.com/json-iterator/go"
	"github.com/streadway/amqp"
	v1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"runtime"
	"sync"
)

const (
	defaultRetryTimes = 3
)

func NewExcellent(ctx context.Context, delay rabbitmq.DelayQueue, common rabbitmq.CommonQueue, api apiserver.Apiserver, election election.Election, cfg *config.Config) *Excellent {
	ctx, cancel := context.WithCancel(ctx)
	return &Excellent{
		ctx:      ctx,
		cancel:   cancel,
		cfg:      cfg,
		delayMq:  delay,
		commonMq: common,
		election: election,
		collect:  deploy.NewCollect(ctx, api, common),
		deploy:   deploy.NewJobDeploy(common, api, cfg.JobStatusTopic),
		event:    apiserver.NewKubeEvent(ctx, api),
		cache:    utils.NewDeduplication(),
	}
}

type Excellent struct {
	ctx      context.Context
	cancel   context.CancelFunc
	cfg      *config.Config
	mux      sync.Mutex
	sw       utils.WaitGroupWrapper
	delayMq  rabbitmq.DelayQueue
	commonMq rabbitmq.CommonQueue
	deploy   deploy.JobDeploy
	event    apiserver.KubeEvent
	election election.Election
	collect  deploy.Collect
	cache    *utils.Deduplication
}

// Run Excellent start
func (e *Excellent) Run() error {
	var err error
	warpC := rabbitmq.NewHandlerWrapper(e.runningJobHandler)
	if err = e.commonMq.Subscription(e.cfg.JobOperateTopic, e.cfg.JobOperateTopic, warpC.HandlerWrap); err != nil {
		return err
	}
	topic := e.cfg.JobPushTopic
	// 订阅指定路由数据,单播
	warpRouterDelay := rabbitmq.NewHandlerWrapper(e.createJobHandler)
	if err = e.delayMq.Subscription(topic, e.cfg.Client.ID, []string{fmt.Sprintf("%s.*", e.cfg.Client.ID)}, warpRouterDelay.HandlerWrap); err != nil {
		return fmt.Errorf("delayMq.Subscription cause=%s", err.Error())
	}
	// 订阅无路由数据,单播
	warpD := rabbitmq.NewHandlerWrapper(e.createJobHandler)
	if err = e.delayMq.Subscription(topic, topic, []string{fmt.Sprintf("%s.*", topic)}, warpD.HandlerWrap); err != nil {
		return fmt.Errorf("delayMq.Subscription cause=%s", err.Error())
	}
	e.election.Register(election.Leader, func() {
		log.Logger().Info("Subscribe k8s events namespace=%s", utils.NameSpace(e.cfg.Client.EventNamespace))
		if err := e.event.Subscribe(e.cfg.Client.EventNamespace, e.EventHandler); err != nil {
			log.Logger().Error("Subscribe namespaces %s EventHandler failed cause=%s", utils.NameSpace(e.cfg.Client.EventNamespace), err.Error())
		}
	})
	e.election.Register(election.Follower, func() {
		log.Logger().Info("Unsubscribe k8s events namespace=%s", utils.NameSpace(e.cfg.Client.EventNamespace))
		e.event.Unsubscribe(e.cfg.Client.EventNamespace)
	})
	return err
}

// EventHandler kube event
func (e *Excellent) EventHandler(event *apiserver.Event) {
	e.mux.Lock()
	defer e.mux.Unlock()
	defer func() {
		if err := recover(); err != nil {
			var buf [4096]byte
			n := runtime.Stack(buf[:], false)
			log.Logger().Error("Excellent.EventHandler cause=%s stack=%s", err, string(buf[:n]))
		}
	}()
	switch event.Type {
	case apiserver.PodAdd, apiserver.PodUpdate:
		e.podEvent(event)
	case apiserver.OnceDelete, apiserver.OnceUpdate, apiserver.OnceAdd:
		e.jobEvent(event)
	case apiserver.CronDelete:
		e.cronJobEvent(event)
	}
}

// podEvent handler core event for collection log
func (e *Excellent) podEvent(event *apiserver.Event) {
	pod := event.Data.(*corev1.Pod)
	if !utils.CheckEvent(pod.Labels) {
		return
	}
	jobId, jobName, jobType := utils.ParseEventLabels(pod.Labels)

	// record event message
	log.Logger().Info(
		"PodEvent %s action=%s namespace=%s job_name=%s pod_name=%s job_type=%s job_id=%s message=%+v",
		utils.Time(pod.CreationTimestamp.Time),
		apiserver.GetEventType(event.Type),
		pod.Namespace,
		jobName,
		pod.Name,
		jobType,
		jobId,
		utils.ParsePodStatus(pod.Status),
	)
	phase := pod.Status.Phase
	if phase != corev1.PodFailed && phase != corev1.PodSucceeded {
		return
	}
	topic := e.cfg.JobStatusTopic
	jot := entity.Once
	if jobType == constant.LabelTypeCron {
		jot = entity.Cron
	}
	wi := &deploy.WorkInfo{JobType: jot,
		JobId:      jobId,
		PodName:    pod.Name,
		JobName:    jobName,
		Namespace:  pod.Namespace,
		Topic:      topic,
		CreateTime: pod.CreationTimestamp.Time.Unix(),
	}
	// 容器错误信息
	// TODO 该事件后采集不到完整容器日志
	if phase == corev1.PodFailed {
		e.collect.FailureLog(pod, wi)
		//e.collect.RunLog(wi)
	}
	// 容器运行日志
	_, ok := pod.Labels[utils.LogCollect]
	if !ok {
		return
	}
	e.collect.RunLog(wi)
}

func (e *Excellent) cronJobEvent(event *apiserver.Event) {
	job := event.Data.(*v1.CronJob)
	if !utils.CheckEvent(job.Labels) {
		return
	}
	jobId, jobName, jobType := utils.ParseEventLabels(job.Labels)
	ty := apiserver.GetEventType(event.Type)
	// record event message
	log.Logger().Info(
		"CronJobEvent %s action=%s namespace=%s job_name=%s job_type=%s job_id=%s",
		utils.Time(job.CreationTimestamp.Time),
		ty,
		job.Namespace,
		jobName,
		jobType,
		jobId,
	)
	// 过滤重复事件
	uid := fmt.Sprintf("%s-%s", ty, jobId)
	if e.cache.Exist(uid) {
		log.Logger().Info("Excellent.CronJobEvent skip uid=%s", uid)
		return
	}
	topic := e.cfg.JobStatusTopic
	message := &entity.JobExecStatusMessage{
		JobType:            entity.Cron,
		JobState:           entity.DeleteSuccess,
		JobId:              jobId,
		JobName:            jobName,
		Time:               job.CreationTimestamp.Time.Unix(),
		JobExecInformation: map[string]string{"failure": ""},
	}
	e.commonMq.Publish(topic, message)
}

// jobEvent handler job event
func (e *Excellent) jobEvent(event *apiserver.Event) {
	job := event.Data.(*v1.Job)
	if !utils.CheckEvent(job.Labels) {
		return
	}
	jobId, jobName, jobType := utils.ParseEventLabels(job.Labels)
	state, cause, skip := e.ParseJobState(event.Type, jobType, job.Status.Conditions)

	// record event message
	ty := apiserver.GetEventType(event.Type)
	log.Logger().Info(
		"jobEvent %s state=%d skip=%t action=%s namespace=%s job_type=%s job_id=%s message=%s",
		utils.Time(job.CreationTimestamp.Time),
		state,
		skip,
		ty,
		job.Namespace,
		jobType,
		jobId,
		cause,
	)
	if skip {
		return
	}
	// 使用控制器uid 防止更新时重复
	uid := fmt.Sprintf("%s-%d-%s-%s", ty, state, job.UID, jobId)
	// 过滤重复事件
	if e.cache.Exist(uid) {
		log.Logger().Info("Excellent.PodEvent skip uid=%s", uid)
		return
	}
	// 事件回调
	e.Callback(state, job.Annotations[utils.CallbackUrl], jobId, cause)
	// 推送状态
	topic := e.cfg.JobStatusTopic
	parent := entity.Once
	if jobType == constant.LabelTypeCron {
		parent = entity.Cron
	}
	e.commonMq.Publish(topic, &entity.JobExecStatusMessage{
		JobType:            entity.Once,
		JobState:           state,
		JobId:              jobId,
		Parent:             parent,
		JobName:            jobName,
		Time:               job.CreationTimestamp.Time.Unix(),
		JobExecInformation: map[string]string{"failure": cause},
	})
}

// Callback 成功与失败情况才回调
func (e *Excellent) Callback(state entity.JobState, url, jobId, message string) {
	if url == "" || (state != entity.JobSuccess && state != entity.Failed) {
		return
	}
	success := true
	if state == entity.Failed {
		success = false
	}
	uid := fmt.Sprintf("callback-%t-%s", success, jobId)
	if e.cache.Exist(uid) {
		return
	}
	msg := &CallbackMessage{JobId: jobId, Success: success, Message: message}
	b, _ := json.Marshal(msg)
	e.sw.Wrap(func() {
		if err := utils.Retry(defaultRetryTimes, func() error {
			return utils.Post(url, bytes.NewReader(b))
		}); err != nil {
			log.Logger().Error("Excellent.Callback failed url=%s message=%+v cause=%s", url, msg, err.Error())
			return
		}
		log.Logger().Info("Excellent.Callback success url=%s message=%+v", url, msg)
	})
}

// ParseJobState 事件解析
func (e *Excellent) ParseJobState(t apiserver.EventType, jobType string, cs []v1.JobCondition) (state entity.JobState, cause string, skip bool) {
	if len(cs) == 0 {
		if t == apiserver.OnceAdd && jobType == constant.LabelTypeCron {
			return entity.CreateSuccess, cause, false
		}
		return state, cause, true
	}
	// status 不为空
	c := cs[0]
	success := false
	switch t {
	case apiserver.OnceDelete:
		if c.Status == corev1.ConditionTrue {
			// 跳过cronjob 生成的job 删除事件
			if jobType == constant.LabelTypeCron {
				skip = true
			}
			success = true
			state = entity.DeleteSuccess
		}
	case apiserver.OnceUpdate:
		if c.Status == corev1.ConditionTrue && c.Type == v1.JobComplete {
			success = true
			state = entity.JobSuccess
		}
	case apiserver.OnceAdd:
		if jobType == constant.LabelTypeOnce {
			skip = true
		}
		success = true
		state = entity.CreateSuccess
	}
	if !success {
		state = entity.Failed
	}
	cause = fmt.Sprintf("type=%s stauts=%s cause=%s,%s", c.Type, c.Status, c.Reason, c.Message)
	return state, cause, skip
}

// ------- Job
// createJobHandler handler job create
func (e *Excellent) createJobHandler(msg *amqp.Delivery) error {
	job := &entity.JobMessage{}
	if err := json.Unmarshal(msg.Body, job); err != nil {
		log.Logger().Warn("Excellent.JobReceiver Unmarshal failed cause=%s msg=%s", err.Error(), string(msg.Body))
		return nil
	}
	defer func() {
		if err := recover(); err != nil {
			var buf [4096]byte
			n := runtime.Stack(buf[:], false)
			log.Logger().Error("Excellent.CreateJobHandler cause=%s stack=%s job=%+v", err, string(buf[:n]), job)
		}
	}()
	if job.JobAction == entity.Create {
		log.Logger().Info("Excellent.CreateJobHandler receive job_id=%s", job.JobId)
		switch job.JobType {
		case entity.Once:
			return e.deploy.CreateOnceJob(job)
		case entity.Cron:
			return e.deploy.CreateCronJob(job)
		}
	}
	log.Logger().Warn("Excellent.CreateJobHandler skip unknown message=%+v", job)
	return nil
}

// ----------- running job
// runningJobHandler handler job delete
func (e *Excellent) runningJobHandler(msg *amqp.Delivery) error {
	job := &entity.JobMessage{}
	if err := json.Unmarshal(msg.Body, job); err != nil {
		log.Logger().Warn("Excellent.JobReceiver Unmarshal failed cause=%s msg=%s", err.Error(), string(msg.Body))
		return nil
	}
	defer func() {
		if err := recover(); err != nil {
			var buf [4096]byte
			n := runtime.Stack(buf[:], false)
			log.Logger().Error("Excellent.CreateJobHandler cause=%s stack=%s job=%+v", err, string(buf[:n]), job)
		}
	}()
	if job.JobAction == entity.Delete {
		switch job.JobType {
		case entity.Once:
			return e.deploy.DeleteOnceJob(job)
		case entity.Cron:
			return e.deploy.DeleteCronJob(job)
		}
	}
	log.Logger().Warn("Excellent.createJobHandler unknown job job_id=%s job_name=%s namespace=%s", job.JobId, job.JobName, job.Namespace)
	return nil
}

func (e *Excellent) Close() error {
	e.cancel()
	e.sw.Wait()
	return nil
}
