package api

import (
	"context"
	"fmt"
	"github.com/hhzhhzhhz/k8s-job-scheduler/config"
	"github.com/hhzhhzhhz/k8s-job-scheduler/entity"
	"github.com/hhzhhzhhz/k8s-job-scheduler/infrastructure"
	"github.com/hhzhhzhhz/k8s-job-scheduler/infrastructure/storage"
	"github.com/hhzhhzhhz/k8s-job-scheduler/log"
	"github.com/hhzhhzhhz/k8s-job-scheduler/pkg/errors"
	"github.com/hhzhhzhhz/k8s-job-scheduler/pkg/protocol"
	"github.com/hhzhhzhhz/k8s-job-scheduler/pkg/utils"
	"github.com/hhzhhzhhz/k8s-job-scheduler/pkg/verify"
	"github.com/hhzhhzhhz/k8s-job-scheduler/recipient"
	json "github.com/json-iterator/go"
	"github.com/julienschmidt/httprouter"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
)

func GetCtx() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*config.GetCfg().ApiTimeoutSecond)
	return ctx
}

func CreateOnceJob(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	ctx := GetCtx()
	r.ParseForm()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		protocol.FailedJson(w, errors.BodyReadError, err.Error())
		return
	}

	jobc := &entity.CreateOnceJob{}
	if err := json.Unmarshal(body, jobc); err != nil {
		protocol.FailedJson(w, errors.BodyDecodeError, err.Error())
		return
	}
	if err := verify.VerifyCreateOnceJob(jobc); err != nil {
		protocol.FailedJson(w, errors.VerifyJobError, err.Error())
		return
	}
	var metadata string
	if jobc.Metadata != nil {
		b, _ := json.Marshal(jobc.Metadata)
		metadata = string(b)
	}
	var n int64
	if n, err = storage.GetFactory().ExistOnceJobByJobName(ctx, jobc.JobName); err != nil {
		protocol.FailedJson(w, errors.SqlQueryError, err.Error())
		return
	}
	if n > 0 {
		protocol.FailedJson(w, errors.AlreadyExistsError, err.Error())
		return
	}
	var jobId string
	var delay time.Duration
	// send to delayQueue
	jobId = utils.UUID()
	now := time.Now().Unix()
	jobc.JobId = jobId
	spec, err := utils.GenOnceJob(jobc)
	if err != nil {
		protocol.FailedJson(w, errors.GenJobError, err.Error())
		return
	}
	// save work tasks
	_, err = storage.GetFactory().CreateOnceJob(ctx,
		&entity.OnceJob{
			JobId:       jobId,
			JobName:     jobc.JobName,
			Metadata:    metadata,
			Namespace:   jobc.Namespace,
			JobExecTime: jobc.JobExecTime,
			Image:       jobc.Image,
			Command:     jobc.Command,
			CreateTime:  now,
			UpdateTime:  now,
		})
	if err != nil {
		protocol.FailedJson(w, errors.SqlCreateError, err.Error())
		return
	}

	_, err = storage.GetFactory().CreateJobStatus(ctx, &entity.JobStatus{
		JobId:      jobId,
		JobName:    jobc.JobName,
		JobType:    entity.Once,
		JobState:   entity.Submit,
		CreateTime: now,
		UpdateTime: now,
	})
	if err != nil {
		protocol.FailedJson(w, errors.SqlQueryError, err.Error())
		return
	}

	if jobc.JobExecTime > now {
		delay = time.Duration(jobc.JobExecTime-now) * time.Second
	}
	err = infrastructure.GetDelayMq().DeferredPublish(
		config.GetCfg().JobPushTopic,
		delay,
		&entity.JobMessage{
			JobType:   entity.Once,
			JobAction: entity.Create,
			JobId:     jobId,
			JobName:   jobc.JobName,
			Namespace: jobc.Namespace,
			OnceJob:   spec,
		})
	if err != nil {
		protocol.FailedJson(w, errors.PublishError, err.Error())
		return
	}

	log.Logger().Debug("create once_job success job_id=%s job_name=%s namespace=%s exec_time=%s", jobId, jobc.JobName, jobc.Namespace, utils.Time(time.Unix(jobc.JobExecTime, 0)))
	protocol.SuccessJson(w, entity.JobCreateSuccess{JobId: jobId})
}

func UpdateOnceJob(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	ctx := GetCtx()
	r.ParseForm()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		protocol.FailedJson(w, errors.BodyReadError, err.Error())
		return
	}
	jobup := &entity.UpdateOnceJob{}
	if err := json.Unmarshal(body, jobup); err != nil {
		protocol.FailedJson(w, errors.BodyDecodeError, err.Error())
		return
	}
	if err := verify.VerifyUpdateOnceJob(jobup); err != nil {
		protocol.FailedJson(w, errors.VerifyJobError, err.Error())
		return
	}
	jobId := jobup.JobId
	jobdb, err := storage.GetFactory().GetOnceJobByJobIdJoinStatus(ctx, jobId)
	if err != nil {
		protocol.FailedJson(w, errors.SqlQueryError, err.Error())
		return
	}
	var delay time.Duration
	now := time.Now().Unix()
	jobName := jobdb.JobName
	spec, err := utils.GenOnceJob(&entity.CreateOnceJob{
		JobId:     jobup.JobId,
		JobName:   jobName,
		Namespace: jobup.Namespace,
		Metadata:  jobup.Metadata,
		Image:     jobup.Image,
		Command:   jobup.Command,
		Timeout:   jobup.Timeout,
	})
	if err != nil {
		protocol.FailedJson(w, errors.GenJobError, err.Error())
		return
	}
	switch jobdb.JobState {
	case entity.Submit:
		protocol.FailedJson(w, errors.JobSubmittedError, fmt.Sprintf("job_name=%s job_status=%d submitted waiting for job to run", jobName, jobdb.JobState))
		return
	case entity.CreateSuccess, entity.Failed, entity.JobSuccess:
		err = infrastructure.GetDelayMq().Publish(
			config.GetCfg().JobOperateTopic,
			&entity.JobMessage{
				JobType:   entity.Once,
				JobAction: entity.Delete,
				JobId:     jobId,
				JobName:   jobName,
				Namespace: jobdb.Namespace,
			})
		if err != nil {
			protocol.FailedJson(w, errors.PublishError, err.Error())
			return
		}
		// wait delete callback
		notify := &utils.WaiterMessage{
			Name:       jobId,
			CreateTime: time.Now().Unix(),
			Watch:      make(chan struct{}, 1),
		}
		recipient.GetRecipient().Watch(notify)
		select {
		case <-notify.Watch:
		case <-time.After(config.GetCfg().Client.OperateMaxTimeout):
			protocol.FailedJson(w, errors.DeleteJobTimeoutError, err.Error())
			return
		}
		if notify.JobStatus != entity.DeleteSuccess {
			protocol.FailedJson(w, errors.DeleteJobTimeoutError, err.Error())
			return
		}
	}
	if jobup.JobExecTime > now {
		delay = time.Duration(jobup.JobExecTime-now) * time.Second
	}
	topic := config.GetCfg().JobPushTopic
	// create work tasks
	var metadata string
	b, _ := json.Marshal(jobup.Metadata)
	metadata = string(b)
	_, err = storage.GetFactory().UpdateOnceJob(ctx, &entity.OnceJob{
		JobId:      jobId,
		Metadata:   metadata,
		Namespace:  jobup.Namespace,
		Image:      jobup.Image,
		Command:    jobup.Command,
		UpdateTime: now,
	})
	if err != nil {
		protocol.FailedJson(w, errors.SqlUpdateError, err.Error())
		return
	}
	if _, err = storage.GetFactory().UpdateJobState(ctx, &entity.JobStatus{JobState: entity.Submit, JobId: jobId, UpdateTime: now}); err != nil {
		protocol.FailedJson(w, errors.SqlUpdateError, err.Error())
		return
	}
	err = infrastructure.GetDelayMq().DeferredPublish(topic, delay, &entity.JobMessage{
		JobType:   entity.Once,
		JobAction: entity.Create,
		JobId:     jobId,
		JobName:   jobName,
		Namespace: jobup.Namespace,
		OnceJob:   spec,
	})
	if err != nil {
		protocol.FailedJson(w, errors.PublishError, err.Error())
		return
	}
	protocol.SuccessMsg(w, "update success.")
}

func DeleteOnceJob(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	jobId := p.ByName("job_id")
	if jobId == "" {
		protocol.FailedJson(w, errors.InvalidParameter, "job_id is empty")
		return
	}
	ctx := GetCtx()
	job, err := storage.GetFactory().GetOnceJobByJobIdJoinStatus(ctx, jobId)
	if err != nil {
		protocol.FailedJson(w, errors.SqlQueryError, err.Error())
		return
	}
	switch job.JobState {
	case entity.DeleteSuccess:
		protocol.SuccessMsg(w, "job_status is delete")
		return
	case entity.Submit:
		protocol.FailedJson(w, errors.DeleteJobError, fmt.Sprintf("job_id=%s submitted waiting for job to run", job.JobId))
		return
	}
	err = infrastructure.GetDelayMq().Publish(
		config.GetCfg().JobOperateTopic,
		&entity.JobMessage{
			JobType:   entity.Once,
			JobAction: entity.Delete,
			JobId:     job.JobId,
			JobName:   job.JobName,
			Namespace: job.Namespace,
		})
	if err != nil {
		protocol.FailedJson(w, errors.PublishError, err.Error())
		return
	}
	// wait delete callback
	notify := &utils.WaiterMessage{
		Name:       job.JobId,
		CreateTime: time.Now().Unix(),
		Watch:      make(chan struct{}, 1),
	}
	recipient.GetRecipient().Watch(notify)
	select {
	case <-notify.Watch:
	case <-time.After(config.GetCfg().Client.OperateMaxTimeout):
		protocol.FailedJson(w, errors.DeleteJobTimeoutError, "")
		return
	}
	if notify.JobStatus != entity.DeleteSuccess {
		protocol.FailedJson(w, errors.DeleteJobTimeoutError, fmt.Sprintf("delete once_job job_id=%s status=%d failed notify=%s", jobId, notify.JobStatus, notify.Cause))
		return
	}
	if _, err = storage.GetFactory().UpdateJobState(ctx, &entity.JobStatus{JobState: entity.DeleteSuccess, JobId: jobId, UpdateTime: time.Now().Unix()}); err != nil {
		protocol.FailedJson(w, errors.SqlUpdateError, err.Error())
		return
	}
	protocol.SuccessMsg(w, fmt.Sprintf("delete once_job success job_id: %s", jobId))
}

func ListOnceJobs(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	r.ParseForm()
	pageNum, pageSize := 1, 10
	var page = r.FormValue("page_num")
	if len(page) != 0 {
		pageNum, _ = strconv.Atoi(page)
		if pageNum <= 0 {
			pageNum = 1
		}
	}
	var size = r.FormValue("page_size")
	if len(size) != 0 {
		pageSize, _ = strconv.Atoi(size)
	}
	var total int64
	offset, limit := (pageNum-1)*pageSize, pageSize
	var res []*entity.OnceJob
	var err error
	ctx := GetCtx()
	total, err = storage.GetFactory().CountByOnceJobs(ctx)
	if res, err = storage.GetFactory().PageOnceJobs(ctx, offset, limit); err != nil {
		protocol.FailedJson(w, errors.SqlQueryError, err.Error())
		return
	}
	protocol.SuccessJson(w, &entity.PageOnceJob{
		Total:    total,
		PageNum:  pageNum,
		PageSize: pageSize,
		Data:     res,
	})
}

func DescribeOnceJob(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	jobId := p.ByName("job_id")
	if jobId == "" {
		protocol.FailedJson(w, errors.InvalidParameter, "job_id is empty.")
	}
	ctx := GetCtx()
	job, err := storage.GetFactory().GetOnceJobByJobIdJoinStatus(ctx, jobId)
	if err != nil {
		protocol.FailedJson(w, errors.SqlQueryError, err.Error())
		return
	}
	protocol.SuccessJson(w, job)
}
