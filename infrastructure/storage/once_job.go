package storage

import (
	"context"
	"github.com/hhzhhzhhz/k8s-job-scheduler/entity"
)

func NewOnceJob() OnceJob {
	return &onceJob{}
}

type onceJob struct{}

func (o *onceJob) CreateOnceJob(ctx context.Context, job *entity.OnceJob) (int64, error) {
	res, err := db().ExecContext(ctx, "insert into once_job(job_id, job_name, job_exec_time, metadata, namespace, image, command, create_time, update_time) values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
		job.JobId,
		job.JobName,
		job.JobExecTime,
		job.Metadata,
		job.Namespace,
		job.Image,
		job.Command,
		job.CreateTime,
		job.UpdateTime,
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (o *onceJob) UpdateOnceJob(ctx context.Context, job *entity.OnceJob) (int64, error) {
	res, err := db().ExecContext(ctx, "update once_job set metadata = ?, job_exec_time= ?, namespace = ?, image = ?, command = ?, update_time = ? where job_id = ?",
		job.Metadata, job.JobExecTime, job.Namespace, job.Image, job.Command, job.UpdateTime, job.JobId)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

//func (o *onceJob) UpdateOnceJob(ctx context.Context, job []*entity.OnceJob) (int64, error) {
//	res, err := db.NamedExecContext(ctx, "insert into once_job(job_status, evn_id, job_exec_information, job_logs, update_time) values (:job_status, :evn_id, :job_exec_information, :job_logs, update_time)", job)
//	if err != nil {
//		return 0, err
//	}
//	return res.RowsAffected()
//}

func (o *onceJob) GetOnceJobByJobId(ctx context.Context, jobId string) (*entity.OnceJob, error) {
	res := &entity.OnceJob{}
	err := db().GetContext(ctx, res, "select * from once_job where job_id = ?;", jobId)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *onceJob) GetOnceJobByJobIdJoinStatus(ctx context.Context, jobId string) (*entity.OnceJob, error) {
	res := &entity.OnceJob{}
	err := db().GetContext(ctx, res, "select oj.*, js.job_state, js.job_times, js.job_success_times from once_job oj INNER JOIN job_status js on oj.job_id = js.job_id and oj.job_id = ?;", jobId)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *onceJob) ExistOnceJobByJobName(ctx context.Context, jobName string) (int64, error) {
	var count int64
	if err := db().GetContext(ctx, &count, "select count(*) from once_job where job_name = ?", jobName); err != nil {
		return 0, err
	}
	return count, nil
}

func (o *onceJob) ExistOnceJobByJobId(ctx context.Context, jobId string) (int, error) {
	var count int
	if err := db().GetContext(ctx, &count, "select count(*) from once_job where job_id = ?", jobId); err != nil {
		return 0, err
	}
	return count, nil
}

func (o *onceJob) PageOnceJobs(ctx context.Context, offset, limit int) ([]*entity.OnceJob, error) {
	var res []*entity.OnceJob
	err := db().SelectContext(ctx, &res, "select oj.*, js.job_state, js.job_times, js.job_success_times from once_job oj INNER JOIN job_status js on oj.job_id = js.job_id ORDER BY oj.id ASC limit ?, ?;", offset, limit)
	if err != nil {
		return res, err
	}
	return res, err
}

func (o *onceJob) CountByOnceJobs(ctx context.Context) (int64, error) {
	var count int64
	if err := db().GetContext(ctx, &count, "select count(*) from once_job oj INNER JOIN job_status js on oj.job_id = js.job_id;"); err != nil {
		return 0, err
	}
	return count, nil
}
