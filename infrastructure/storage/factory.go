package storage

import (
	"context"
	"github.com/hhzhhzhhz/k8s-job-scheduler/entity"
	"sync"
)

var (
	fac     Factory
	faconne sync.Once
)

func GetFactory() Factory {
	faconne.Do(func() {
		if fac == nil {
			fac = &factory{
				NewOnceJob(),
				NewCronJob(),
				NewJobLogs(),
				NewJobStatus(),
			}
		}
	})
	return fac
}

type Factory interface {
	OnceJob
	CronJob
	JobLogs
	JobStatus
}

type factory struct {
	OnceJob
	CronJob
	JobLogs
	JobStatus
}

type OnceJob interface {
	CreateOnceJob(ctx context.Context, job *entity.OnceJob) (int64, error)
	UpdateOnceJob(ctx context.Context, job *entity.OnceJob) (int64, error)
	GetOnceJobByJobId(ctx context.Context, jobId string) (*entity.OnceJob, error)
	GetOnceJobByJobIdJoinStatus(ctx context.Context, jobId string) (*entity.OnceJob, error)
	ExistOnceJobByJobName(ctx context.Context, jobId string) (int64, error)
	ExistOnceJobByJobId(ctx context.Context, jobId string) (int, error)
	PageOnceJobs(ctx context.Context, offset, limit int) ([]*entity.OnceJob, error)
	CountByOnceJobs(ctx context.Context) (int64, error)
}

type CronJob interface {
	CreateCronJob(ctx context.Context, job *entity.CronJob) (int64, error)
	UpdateCronJob(ctx context.Context, job *entity.CronJob) (int64, error)
	ExistCronJobByJobName(ctx context.Context, jobName string) (int64, error)
	GetCronJobByJobIdJoinStatus(ctx context.Context, jobId string) (*entity.CronJob, error)
	GetCronJob(ctx context.Context, jobId string) (*entity.CronJob, error)
	PageCronJobs(ctx context.Context, offset, limit int) ([]*entity.CronJob, error)
	CountByCronJobs(ctx context.Context) (int64, error)
}

type JobStatus interface {
	CreateJobStatus(ctx context.Context, job *entity.JobStatus) (int64, error)
	UpdateJobState(ctx context.Context, job *entity.JobStatus) (int64, error)
	UpdateJobStateAndExecInfo(ctx context.Context, job *entity.JobStatus) (int64, error)
	UpdateCronJobState(ctx context.Context, job *entity.JobStatus) (int64, error)
	UpdateCronJobStateAndExecInfo(ctx context.Context, job *entity.JobStatus) (int64, error)
	UpdateJobTimesAndState(ctx context.Context, job *entity.JobStatus) (int64, error)
	UpdateCronJobTimes(ctx context.Context, job *entity.JobStatus) (int64, error)
	UpdateSuccessTimesAndState(ctx context.Context, job *entity.JobStatus) (int64, error)
	UpdateCronSuccessTimes(ctx context.Context, job *entity.JobStatus) (int64, error)
	GetJobStatusByJobId(ctx context.Context, jobId string) (*entity.JobStatus, error)
}

type JobLogs interface {
	InsertJobLogs(ctx context.Context, log *entity.JobLogs) (int64, error)
	BulkInsertJobLogs(ctx context.Context, logs []*entity.JobLogs) (int64, error)
	GetJobLogsByPods(ctx context.Context, jobId string) ([]*entity.JobLogs, error)
}
