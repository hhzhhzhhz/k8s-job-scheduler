package storage

import (
	"context"
	"github.com/hhzhhzhhz/k8s-job-scheduler/entity"
)

func NewCronJob() CronJob {
	return &cronJob{}
}

type cronJob struct{}

func (c *cronJob) CreateCronJob(ctx context.Context, job *entity.CronJob) (int64, error) {
	res, err := db().ExecContext(ctx, "insert into cron_job(job_id, job_name, metadata, schedule, namespace, image, command, create_time, update_time) values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
		job.JobId,
		job.JobName,
		job.Metadata,
		job.Schedule,
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

func (c *cronJob) UpdateCronJob(ctx context.Context, job *entity.CronJob) (int64, error) {
	res, err := db().ExecContext(ctx, "update cron_job set schedule = ?, namespace = ?, image = ?, command = ? update_time = ? where job_id = ?",
		job.Schedule, job.Namespace, job.Image, job.Command, job.UpdateTime, job.JobId)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (c *cronJob) GetCronJob(ctx context.Context, jobId string) (*entity.CronJob, error) {
	res := &entity.CronJob{}
	err := db().GetContext(ctx, res, "select cj.*, js.job_state, js.job_times, js.job_success_times from cron_job cj INNER JOIN job_status js on cj.job_id = js.job_id and cj.job_id = ?;", jobId)
	if err != nil {
		return res, err
	}
	return res, nil
	return nil, nil
}

func (c *cronJob) PageCronJobs(ctx context.Context, offset, limit int) ([]*entity.CronJob, error) {
	var res []*entity.CronJob
	err := db().SelectContext(ctx, &res, "select cj.*, js.job_state, js.job_times, js.job_success_times from cron_job cj INNER JOIN job_status js on cj.job_id = js.job_id ORDER BY cj.id ASC limit ?, ?;", offset, limit)
	if err != nil {
		return res, err
	}
	return res, err
}

func (o *cronJob) ExistCronJobByJobName(ctx context.Context, jobName string) (int64, error) {
	var count int64
	if err := db().GetContext(ctx, &count, "select count(*) from cron_job where job_name = ?", jobName); err != nil {
		return 0, err
	}
	return count, nil
}

func (o *cronJob) GetCronJobByJobIdJoinStatus(ctx context.Context, jobId string) (*entity.CronJob, error) {
	res := &entity.CronJob{}
	err := db().GetContext(ctx, res, "select cj.*, js.job_state, js.job_times, js.job_success_times from cron_job cj INNER JOIN job_status js on cj.job_id = js.job_id and cj.job_id = ?;", jobId)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (c *cronJob) CountByCronJobs(ctx context.Context) (int64, error) {
	var count int64
	if err := db().GetContext(ctx, &count, "select count(*) from cron_job cj INNER JOIN job_status js on cj.job_id = js.job_id;"); err != nil {
		return 0, err
	}
	return count, nil
}
