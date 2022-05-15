package deploy

import (
	"context"
	"github.com/hhzhhzhhz/k8s-job-scheduler/apiserver"
	"github.com/hhzhhzhhz/k8s-job-scheduler/entity"
	"github.com/hhzhhzhhz/k8s-job-scheduler/infrastructure/rabbitmq"
	"github.com/hhzhhzhhz/k8s-job-scheduler/log"
	errorsv1 "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"time"
)

type JobDeploy interface {
	CreateOnceJob(job *entity.JobMessage) error
	UpdateOnceJob(job *entity.JobMessage) error
	DeleteOnceJob(job *entity.JobMessage) error
	CreateCronJob(job *entity.JobMessage) error
	UpdateCronJob(job *entity.JobMessage) error
	DeleteCronJob(job *entity.JobMessage) error
}

func NewJobDeploy(mq rabbitmq.CommonQueue, api apiserver.Apiserver, topic string) JobDeploy {
	return &jobDeploy{
		mq:          mq,
		api:         api,
		noticeTopic: topic,
	}
}

type jobDeploy struct {
	api         apiserver.Apiserver
	mq          rabbitmq.CommonQueue
	noticeTopic string
}

func (j *jobDeploy) CreateOnceJob(job *entity.JobMessage) error {
	res, err := j.api.CreateOnceJob(context.Background(), job.Namespace, job.OnceJob)
	// send failed kubeEvent
	if err != nil {
		j.mq.RetryPublish(
			j.noticeTopic,
			&entity.JobExecStatusMessage{
				JobId:              job.JobId,
				JobName:            job.JobName,
				JobType:            entity.Once,
				JobState:           entity.Failed,
				JobExecInformation: map[string]string{"failure": err.Error(), "step": "submit job"},
				Time:               time.Now().Unix(),
			})
		log.Logger().Error("JobDeploy.CreateOnceJob submit failed start retry job_id=%s namespaces=%s job_name=%s cause=%s", job.JobId, job.Namespace, job.JobName, err.Error())
		return err
	}
	j.mq.RetryPublish(
		j.noticeTopic,
		&entity.JobExecStatusMessage{
			JobId:    job.JobId,
			JobName:  job.JobName,
			JobType:  entity.Once,
			JobState: entity.CreateSuccess,
			Time:     time.Now().Unix(),
		})
	log.Logger().Debug("JobDeploy.CreateOnceJob submit jobs success job_id=%s namespaces=%s job_name=%s", job.JobId, res.Namespace, res.Name)
	return nil
}

func (j *jobDeploy) UpdateOnceJob(job *entity.JobMessage) error {
	err := j.api.DeleteOnceJob(context.Background(), job.Namespace, job.JobName)
	// send failed kubeEvent
	if err != nil {
		j.mq.RetryPublish(
			j.noticeTopic,
			&entity.JobExecStatusMessage{
				JobId:              job.JobId,
				JobName:            job.JobName,
				JobType:            entity.Once,
				JobState:           entity.Failed,
				JobExecInformation: map[string]string{"failure": err.Error(), "step": "delete job"},
				Time:               time.Now().Unix(),
			})
		log.Logger().Error("JobDeploy.UpdateOnceJob delete old job failed job_id=%s namespaces=%s job_name=%s cause=%s", job.JobId, job.Namespace, job.JobName, err.Error())
		return nil
	}
	res, err := j.api.CreateOnceJob(context.Background(), job.Namespace, job.OnceJob)
	if err != nil {
		j.mq.RetryPublish(
			j.noticeTopic,
			&entity.JobExecStatusMessage{
				JobId:              job.JobId,
				JobName:            job.JobName,
				JobType:            entity.Once,
				JobState:           entity.Failed,
				JobExecInformation: map[string]string{"failure": err.Error(), "step": "create job"},
				Time:               time.Now().Unix(),
			})
		log.Logger().Error("JobDeploy.UpdateOnceJob create new job failed job_id=%s namespaces=%s job_name=%s cause=%s", job.JobId, job.Namespace, job.JobName, err.Error())
		return nil
	}
	j.mq.RetryPublish(
		j.noticeTopic,
		&entity.JobExecStatusMessage{
			JobId:    job.JobId,
			JobName:  job.JobName,
			JobType:  entity.Once,
			JobState: entity.CreateSuccess,
			Time:     time.Now().Unix(),
		})
	log.Logger().Debug("JobDeploy.UpdateOnceJob update success job_id=%s name=%s namespaces=%s", job.JobId, res.Name, res.Namespace)
	return nil
}

func (j *jobDeploy) DeleteOnceJob(job *entity.JobMessage) error {
	// send failed kubeEvent
	if err := j.api.DeleteOnceJob(context.Background(), job.Namespace, job.JobName); err != nil {
		status := err.(*errorsv1.StatusError)
		switch status.ErrStatus.Reason {
		case meta.StatusReasonNotFound:
			return nil
		}
		j.mq.RetryPublish(
			j.noticeTopic,
			&entity.JobExecStatusMessage{
				JobId:              job.JobId,
				JobName:            job.JobName,
				JobType:            entity.Once,
				JobState:           entity.Failed,
				JobExecInformation: map[string]string{"failure": err.Error(), "step": "delete job"},
				Time:               time.Now().Unix(),
			})
		log.Logger().Error("JobDeploy.DeleteOnceJob delete job failed job_id=%s job_name=%s namespace=%s cause=%s", job.JobId, job.JobName, job.Namespace, err.Error())
		return nil
	}
	j.mq.RetryPublish(
		j.noticeTopic,
		&entity.JobExecStatusMessage{
			JobId:    job.JobId,
			JobName:  job.JobName,
			JobType:  entity.Once,
			JobState: entity.DeleteSuccess,
			Time:     time.Now().Unix(),
		})
	log.Logger().Debug("JobDeploy.DeleteOnceJob delete once job success job_id=%s job_name=%s namespace=%s", job.JobId, job.JobName, job.Namespace)
	return nil
}

func (j *jobDeploy) CreateCronJob(job *entity.JobMessage) error {
	res, err := j.api.CreateCronJob(context.Background(), job.Namespace, job.CronJob)
	// send failed kubeEvent
	if err != nil {
		j.mq.RetryPublish(
			j.noticeTopic,
			&entity.JobExecStatusMessage{
				JobId:              job.JobId,
				JobName:            job.JobName,
				JobType:            entity.Cron,
				JobState:           entity.Failed,
				JobExecInformation: map[string]string{"failure": err.Error(), "step": "submit job"},
				Time:               time.Now().Unix(),
			})
		log.Logger().Error("JobDeploy.CreateCronJob submit failed start retry job_id=%s namespaces=%s job_name=%s cause=%s", job.JobId, job.Namespace, job.JobName, err.Error())
		return err
	}
	j.mq.RetryPublish(
		j.noticeTopic,
		&entity.JobExecStatusMessage{
			JobId:    job.JobId,
			JobName:  job.JobName,
			JobType:  entity.Cron,
			JobState: entity.CreateSuccess,
			Time:     time.Now().Unix(),
		})
	log.Logger().Debug("JobDeploy.CreateCronJob submit jobs success job_id=%s namespaces=%s job_name=%s", job.JobId, res.Namespace, res.Name)
	return nil
}

func (j *jobDeploy) UpdateCronJob(job *entity.JobMessage) error {
	err := j.api.DeleteCronJob(context.Background(), job.Namespace, job.JobName)
	// send failed kubeEvent
	if err != nil {
		j.mq.RetryPublish(
			j.noticeTopic,
			&entity.JobExecStatusMessage{
				JobId:              job.JobId,
				JobName:            job.JobName,
				JobType:            entity.Cron,
				JobState:           entity.Failed,
				JobExecInformation: map[string]string{"failure": err.Error(), "step": "delete job"},
				Time:               time.Now().Unix(),
			})
		log.Logger().Error("JobDeploy.UpdateCronJob delete old job failed job_id=%s job_name=%s namespace=%s cause=%s", job.JobId, job.JobName, job.Namespace, err.Error())
		return nil
	}
	res, err := j.api.CreateCronJob(context.Background(), job.Namespace, job.CronJob)
	if err != nil {
		j.mq.RetryPublish(
			j.noticeTopic,
			&entity.JobExecStatusMessage{
				JobId:              job.JobId,
				JobName:            job.JobName,
				JobType:            entity.Cron,
				JobState:           entity.Failed,
				JobExecInformation: map[string]string{"failure": err.Error(), "step": "create job"},
				Time:               time.Now().Unix(),
			})
		log.Logger().Error("JobDeploy.UpdateCronJob create new job failed job_id=%s job_name=%s namespace=%s cause=%s", job.JobId, job.JobName, job.Namespace, err.Error())
		return nil
	}
	j.mq.RetryPublish(
		j.noticeTopic,
		&entity.JobExecStatusMessage{
			JobId:    job.JobId,
			JobName:  job.JobName,
			JobType:  entity.Cron,
			JobState: entity.CreateSuccess,
			Time:     time.Now().Unix(),
		})
	log.Logger().Info("JobDeploy.UpdateCronJob update success job_id=%s name=%s namespaces=%s", job.JobId, res.Name, res.Namespace)
	return nil
}

func (j *jobDeploy) DeleteCronJob(job *entity.JobMessage) error {
	// send failed kubeEvent
	now := time.Now().Unix()
	if err := j.api.DeleteCronJob(context.Background(), job.Namespace, job.JobName); err != nil {
		j.mq.RetryPublish(
			j.noticeTopic,
			&entity.JobExecStatusMessage{
				JobId:              job.JobId,
				JobName:            job.JobName,
				JobType:            entity.Once,
				JobState:           entity.Failed,
				JobExecInformation: map[string]string{"failure": err.Error(), "step": "delete job"},
				Time:               now,
			})
		log.Logger().Error("JobDeploy.DeleteCronJob delete job failed job_id=%s job_name=%s namespace=%s cause=%s", job.JobId, job.JobName, job.Namespace, err.Error())
		return nil
	}
	j.mq.RetryPublish(
		j.noticeTopic,
		&entity.JobExecStatusMessage{
			JobId:    job.JobId,
			JobName:  job.JobName,
			JobType:  entity.Cron,
			JobState: entity.DeleteSuccess,
			Time:     now,
		})

	log.Logger().Debug("JobDeploy.DeleteCronJob delete once job success job_id=%s job_name=%s namespaces=%s", job.JobId, job.JobName, job.Namespace)
	return nil
}
