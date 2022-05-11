package storage

import (
	"context"
	"github.com/hhzhhzhhz/k8s-job-scheduler/entity"
)

func NewJobLogs() JobLogs {
	return &joblogs{}
}

type joblogs struct{}

func (j *joblogs) GetJobLogsByPods(ctx context.Context, jobId string) ([]*entity.JobLogs, error) {
	var res []*entity.JobLogs
	err := db().SelectContext(ctx, &res, "select * from job_logs WHERE job_id = ?", jobId)
	if err != nil {
		return res, err
	}
	return res, err
}

//func (j *joblogs) GetJobLogsByPods(ctx context.Context, pods []string) ([]*entity.JobLogs, error) {
//	var res []*entity.JobLogs
//	query, args, err := sqlx.In("select * from job_logs WHERE pod_name in (?)", pods)
//	if err != nil {
//		return nil, err
//	}
//	if err := db().SelectContext(ctx, &res, query, args...); err != nil {
//		return nil, err
//	}
//	return res, nil
//}

func (j *joblogs) InsertJobLogs(ctx context.Context, log *entity.JobLogs) (int64, error) {
	res, err := db().ExecContext(ctx, "insert into job_logs(job_id, pod_name, run_logs, create_time) values (?, ?, ?, ?)", log.JobId, log.PodName, log.RunLogs, log.CreateTime)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (j *joblogs) BulkInsertJobLogs(ctx context.Context, logs []*entity.JobLogs) (int64, error) {
	res, err := db().NamedExecContext(ctx, "insert into job_logs(job_id, pod_name, run_logs, create_time) values (:job_id, :pod_name, :run_logs, :create_time)", logs)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
