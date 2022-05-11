package storage

import (
	"context"
	"github.com/hhzhhzhhz/k8s-job-scheduler/entity"
)

func NewJobStatus() JobStatus {
	return &jobStatus{}
}

type jobStatus struct {
}

func (j *jobStatus) CreateJobStatus(ctx context.Context, job *entity.JobStatus) (int64, error) {
	res, err := db().ExecContext(ctx, "insert into job_status(job_id, job_type, job_state, create_time, update_time) values (?, ?, ?, ?, ?)",
		job.JobId,
		job.JobType,
		job.JobState,
		job.CreateTime,
		job.UpdateTime,
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (j *jobStatus) UpdateJobState(ctx context.Context, job *entity.JobStatus) (int64, error) {
	res, err := db().ExecContext(ctx, "update job_status set job_state = ?, update_time = ? where job_id = ? and job_type = 0", job.JobState, job.UpdateTime, job.JobId)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (j *jobStatus) UpdateJobStateAndExecInfo(ctx context.Context, job *entity.JobStatus) (int64, error) {
	res, err := db().ExecContext(ctx, "update job_status set job_state = ?, job_exec_information = ?, update_time = ? where job_id = ? and job_type = 0", job.JobState, job.JobExecInformation, job.UpdateTime, job.JobId)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (j *jobStatus) UpdateCronJobState(ctx context.Context, job *entity.JobStatus) (int64, error) {
	res, err := db().ExecContext(ctx, "update job_status set job_state = ?, update_time = ? where job_id = ? and job_type = 1", job.JobState, job.UpdateTime, job.JobId)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (j *jobStatus) UpdateCronJobStateAndExecInfo(ctx context.Context, job *entity.JobStatus) (int64, error) {
	res, err := db().ExecContext(ctx, "update job_status set job_state = ?, job_exec_information = ?, update_time = ? where job_id = ? and job_type = 1", job.JobState, job.JobExecInformation, job.UpdateTime, job.JobId)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (j *jobStatus) UpdateJobTimesAndState(ctx context.Context, job *entity.JobStatus) (int64, error) {
	res, err := db().ExecContext(ctx, "update job_status set job_state = ?, job_times = job_times+1, update_time = ? where job_id = ? and job_type = 0", job.JobState, job.UpdateTime, job.JobId)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (j *jobStatus) UpdateCronJobTimes(ctx context.Context, job *entity.JobStatus) (int64, error) {
	res, err := db().ExecContext(ctx, "update job_status set job_times = job_times+1, update_time = ? where job_id = ? and job_type = 1", job.UpdateTime, job.JobId)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (j *jobStatus) UpdateSuccessTimesAndState(ctx context.Context, job *entity.JobStatus) (int64, error) {
	res, err := db().ExecContext(ctx, "update job_status set job_state = ?, job_success_times = job_success_times+1, update_time = ? where job_id = ? and job_type = 0", job.JobState, job.UpdateTime, job.JobId)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (j *jobStatus) UpdateCronSuccessTimes(ctx context.Context, job *entity.JobStatus) (int64, error) {
	res, err := db().ExecContext(ctx, "update job_status set job_success_times = job_success_times+1, update_time = ? where job_id = ? and job_type = 1", job.UpdateTime, job.JobId)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (o *jobStatus) GetJobStatusByJobId(ctx context.Context, jobId string) (*entity.JobStatus, error) {
	res := &entity.JobStatus{}
	err := db().GetContext(ctx, res, "select * from job_status where job_id = ?;", jobId)
	if err != nil {
		return res, err
	}
	return res, nil
}
