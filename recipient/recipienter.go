package recipient

import (
	"context"
	"github.com/hhzhhzhhz/k8s-job-scheduler/config"
	"github.com/hhzhhzhhz/k8s-job-scheduler/election"
	"github.com/hhzhhzhhz/k8s-job-scheduler/entity"
	"github.com/hhzhhzhhz/k8s-job-scheduler/infrastructure"
	"github.com/hhzhhzhhz/k8s-job-scheduler/infrastructure/storage"
	"github.com/hhzhhzhhz/k8s-job-scheduler/log"
	"github.com/hhzhhzhhz/k8s-job-scheduler/pkg/utils"
	json "github.com/json-iterator/go"
	"github.com/nsqio/go-nsq"
	"sync"
	"time"
)

const (
	defaultHandlerTimeout = 5 * time.Second
	rotationTime          = 20 * time.Second
)

var (
	recipient *Recipient
)

func GetRecipient() *Recipient {
	return recipient
}

func NewRecipient(ctx context.Context, mq infrastructure.Delaymq, election election.Election, cfg *config.Config) *Recipient {
	ctx, cancel := context.WithCancel(ctx)
	recipient = &Recipient{
		ctx:      ctx,
		cancel:   cancel,
		cfg:      cfg,
		mq:       mq,
		election: election,
		waiter:   utils.NewWaiter(),
	}
	return recipient
}

type Recipient struct {
	cfg      *config.Config
	sw       utils.WaitGroupWrapper
	mux      sync.Mutex
	ctx      context.Context
	cancel   context.CancelFunc
	mq       infrastructure.Delaymq
	election election.Election
	waiter   utils.Waiter
	logBuf   []*entity.JobLogs
}

func (r *Recipient) Run() {
	recipient.asyncHandlerLog()
	r.election.Register(election.Leader, func() {
		log.Logger().Info("Subscribe Mq topic=%s", r.cfg.JobStatusTopic)
		if err := r.SubscribeJobStatus(r.cfg.JobStatusTopic); err != nil {
			log.Logger().Error("Subscribe Mq topic=%s failed cause=%s", r.cfg.JobStatusTopic, err.Error())
		}
	})
	r.election.Register(election.Follower, func() {
		log.Logger().Info("Unsubscribe Mq topic=%s", r.cfg.JobStatusTopic)
		r.mq.UnSubscription(r.cfg.JobStatusTopic)
	})

}

// SubscribeJobStatus service server start
func (r *Recipient) SubscribeJobStatus(jobStatusTopic string) error {
	r.mux.Lock()
	defer r.mux.Unlock()
	return r.mq.Subscription(jobStatusTopic, jobStatusTopic, infrastructure.NewEmptyHandler(r.JobStatusReceive))
}

func (r *Recipient) Watch(watch *utils.WaiterMessage) {
	r.waiter.Watch(watch)
}

// JobStatusReceive handler client return info
func (r *Recipient) JobStatusReceive(msg *nsq.Message) error {
	ctx, _ := context.WithTimeout(context.Background(), defaultHandlerTimeout)
	statusMessage := &entity.JobExecStatusMessage{}

	var err error
	if err = json.Unmarshal(msg.Body, statusMessage); err != nil {
		log.Logger().Warn("Recipient.jobStatusReceive Unmarshal failed message=[%s] cause=%s", string(msg.Body), err.Error())
		return nil
	}
	switch statusMessage.JobType {
	case entity.Once:
		return r.handlerOnce(ctx, statusMessage)
	case entity.Cron:
		return r.handlerCron(ctx, statusMessage)
	}
	log.Logger().Error("Recipient.JobStatusReceive skip unknown  message=%+v", statusMessage)
	return nil
}

func (r *Recipient) handlerLogs(ctx context.Context, message *entity.JobExecStatusMessage) error {
	// *(*string)(unsafe.Pointer(&message.Logs))
	r.mux.Lock()
	r.logBuf = append(r.logBuf, &entity.JobLogs{
		JobId:      message.JobId,
		PodName:    message.PodName,
		RunLogs:    message.Logs,
		CreateTime: message.Time},
	)
	r.mux.Unlock()
	return nil
}

func (r *Recipient) asyncHandlerLog() {
	r.sw.Wrap(func() {
		tick := time.Tick(rotationTime)
		for {
			select {
			case <-r.ctx.Done():
				r.insertLogs()
				return
			case <-tick:
				r.insertLogs()
			}
		}
	})
}

func (r *Recipient) insertLogs() {
	r.mux.Lock()
	if len(r.logBuf) == 0 {
		r.mux.Unlock()
		return
	}
	buf := r.logBuf[:]
	r.logBuf = r.logBuf[:0]
	r.mux.Unlock()
	_, err := storage.GetFactory().BulkInsertJobLogs(context.Background(), buf)
	if err != nil {
		log.Logger().Warn("Recipient.HandlerLogs bulk insert logs failed cause=%s", err.Error())
	}
}

// handlerCron 由于cronjob 是创建出job 因此cronjob只有删除、创建、失败
func (r *Recipient) handlerCron(ctx context.Context, msg *entity.JobExecStatusMessage) error {
	var execInfo string
	if msg.JobExecInformation != nil {
		b, _ := json.Marshal(msg.JobExecInformation)
		execInfo = string(b)
	}
	jobId := msg.JobId
	state := msg.JobState
	jobName := msg.JobName
	switch state {
	case entity.CreateSuccess, entity.DeleteSuccess:
		if state != entity.CreateSuccess {
			r.waiter.Done(&utils.WaiterMessage{
				Name:      jobId,
				JobStatus: state,
			})
		}
		return r.UpdateCronJobState(ctx, &entity.JobStatus{
			JobId:      jobId,
			JobName:    jobName,
			JobState:   state,
			UpdateTime: msg.Time,
		})
	case entity.Failed:
		r.waiter.Done(&utils.WaiterMessage{
			Name:      jobId,
			JobStatus: state,
		})
		return r.UpdateCronJobStateAndExecInfo(ctx, &entity.JobStatus{
			JobId:              jobId,
			JobName:            jobName,
			JobState:           state,
			JobExecInformation: execInfo,
			UpdateTime:         msg.Time,
		})

	case entity.RunLogs:
		return r.handlerLogs(ctx, msg)
	default:
		log.Logger().Warn("skip unknown message=%+v", msg)
	}
	return nil
}

func (r *Recipient) handlerOnce(ctx context.Context, msg *entity.JobExecStatusMessage) error {
	var execInfo string
	if msg.JobExecInformation != nil {
		b, _ := json.Marshal(msg.JobExecInformation)
		execInfo = string(b)
	}
	jobId := msg.JobId
	status := msg.JobState
	jobName := msg.JobName

	switch status {
	case entity.JobSuccess:
		if msg.Parent == entity.Cron {
			return r.UpdateCronSuccessTimes(ctx, &entity.JobStatus{
				JobId:      jobId,
				UpdateTime: msg.Time,
			})
		}
		return r.UpdateSuccessTimesAndState(ctx, &entity.JobStatus{
			JobId:              jobId,
			JobName:            jobName,
			JobState:           status,
			JobExecInformation: execInfo,
			UpdateTime:         msg.Time,
		})
	case entity.CreateSuccess:
		if msg.Parent == entity.Cron {
			return r.UpdateCronJobTimes(ctx, &entity.JobStatus{
				JobId:      jobId,
				UpdateTime: msg.Time,
			})
		}
		r.UpdateJobTimesAndState(ctx, &entity.JobStatus{
			JobId:              jobId,
			JobName:            jobName,
			JobState:           status,
			JobExecInformation: execInfo,
			UpdateTime:         msg.Time,
		})
	case entity.DeleteSuccess:
		r.waiter.Done(&utils.WaiterMessage{
			Name:      jobId,
			JobStatus: status,
		})
		return r.UpdateJobState(ctx, &entity.JobStatus{
			JobId:              jobId,
			JobName:            jobName,
			JobState:           status,
			JobExecInformation: execInfo,
			UpdateTime:         msg.Time,
		})
	case entity.Failed:
		if msg.Parent == entity.Cron {
			return nil
		}
		r.waiter.Done(&utils.WaiterMessage{
			Name:      jobId,
			JobStatus: status,
		})
		return r.UpdateJobStateAndExecInfo(ctx, &entity.JobStatus{
			JobId:              jobId,
			JobName:            jobName,
			JobState:           status,
			JobExecInformation: execInfo,
			UpdateTime:         msg.Time,
		})
	case entity.RunLogs:
		return r.handlerLogs(ctx, msg)
	default:
		log.Logger().Warn("skip unknown message=%+v", msg)
	}
	return nil
}

func (r *Recipient) UpdateJobState(ctx context.Context, job *entity.JobStatus) error {
	n, err := storage.GetFactory().UpdateJobState(ctx, job)
	if err != nil {
		log.Logger().Warn("Recipient.UpdateJobState job_id=%s update job_status=%d failed cause=%s", job.JobId, job.JobState, err.Error())
		return err
	}
	if n < 1 {
		log.Logger().Warn("Recipient.UpdateJobState job_id=%s update job_status=%d failed update nums=%d need nums=%d", job.JobId, job.JobState, n, 1)
	}
	return nil
}

func (r *Recipient) UpdateJobStateAndExecInfo(ctx context.Context, job *entity.JobStatus) error {
	n, err := storage.GetFactory().UpdateJobStateAndExecInfo(ctx, job)
	if err != nil {
		log.Logger().Warn("Recipient.UpdateJobStateAndExecInfo job_id=%s update job_status=%d failed cause=%s", job.JobId, job.JobState, err.Error())
		return err
	}
	if n < 1 {
		log.Logger().Warn("Recipient.UpdateJobStateAndExecInfo job_id=%s update job_status=%d failed update nums=%d need nums=%d", job.JobId, job.JobState, n, 1)
	}
	return nil
}

func (r *Recipient) UpdateCronJobState(ctx context.Context, job *entity.JobStatus) error {
	n, err := storage.GetFactory().UpdateCronJobState(ctx, job)
	if err != nil {
		log.Logger().Warn("Recipient.UpdateCronJobState job_id=%s update job_status=%d failed cause=%s", job.JobId, job.JobState, err.Error())
		return err
	}
	if n < 1 {
		log.Logger().Warn("Recipient.UpdateCronJobState job_id=%s update job_status=%d failed update nums=%d need nums=%d", job.JobId, job.JobState, n, 1)
	}
	return nil
}

func (r *Recipient) UpdateCronJobStateAndExecInfo(ctx context.Context, job *entity.JobStatus) error {
	n, err := storage.GetFactory().UpdateCronJobStateAndExecInfo(ctx, job)
	if err != nil {
		log.Logger().Warn("Recipient.UpdateCronJobStateAndExecInfo job_id=%s update job_status=%d failed cause=%s", job.JobId, job.JobState, err.Error())
		return err
	}
	if n < 1 {
		log.Logger().Warn("Recipient.UpdateCronJobStateAndExecInfo job_id=%s update job_status=%d failed update nums=%d need nums=%d", job.JobId, job.JobState, n, 1)
	}
	return nil
}

func (r *Recipient) UpdateJobTimesAndState(ctx context.Context, job *entity.JobStatus) error {
	n, err := storage.GetFactory().UpdateJobTimesAndState(ctx, job)
	if err != nil {
		log.Logger().Warn("Recipient.UpdateJobTimesAndState job_id=%s update failed cause=%s", job.JobId, job.JobName, err.Error())
		return err
	}
	if n < 1 {
		log.Logger().Warn("Recipient.UpdateJobTimesAndState job_id=%s update failed update nums=%d need nums=%d", job.JobId, job.JobName, n, 1)
	}
	return nil
}

func (r *Recipient) UpdateSuccessTimesAndState(ctx context.Context, job *entity.JobStatus) error {
	n, err := storage.GetFactory().UpdateSuccessTimesAndState(ctx, job)
	if err != nil {
		log.Logger().Warn("Recipient.UpdateSuccessTimesAndState job_id=%s update failed cause=%s", job.JobId, job.JobName, err.Error())
		return err
	}
	if n < 1 {
		log.Logger().Warn("Recipient.UpdateSuccessTimesAndState job_id=%s update failed update nums=%d need nums=%d", job.JobId, job.JobName, n, 1)
	}
	return nil
}

func (r *Recipient) UpdateCronSuccessTimes(ctx context.Context, job *entity.JobStatus) error {
	n, err := storage.GetFactory().UpdateCronSuccessTimes(ctx, job)
	if err != nil {
		log.Logger().Warn("Recipient.UpdateCronSuccessTimes job_id=%s update failed cause=%s", job.JobId, job.JobName, err.Error())
		return err
	}
	if n < 1 {
		log.Logger().Warn("Recipient.UpdateCronSuccessTimes job_id=%s update failed update nums=%d need nums=%d", job.JobId, job.JobName, n, 1)
	}
	return nil
}

func (r *Recipient) UpdateCronJobTimes(ctx context.Context, job *entity.JobStatus) error {
	n, err := storage.GetFactory().UpdateCronJobTimes(ctx, job)
	if err != nil {
		log.Logger().Warn("Recipient.UpdateCronJobTimes job_id=%s update failed cause=%s", job.JobId, job.JobName, err.Error())
		return err
	}
	if n < 1 {
		log.Logger().Warn("Recipient.UpdateCronJobTimes job_id=%s update failed update nums=%d need nums=%d", job.JobId, job.JobName, n, 1)
	}
	return nil
}

func (r *Recipient) Close() error {
	r.cancel()
	r.sw.Wait()
	return nil
}
