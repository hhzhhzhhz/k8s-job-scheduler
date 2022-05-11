package api

import (
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

func CreateCronJob(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	ctx := GetCtx()
	r.ParseForm()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		protocol.FailedJson(w, errors.BodyReadError, err.Error())
		return
	}

	jobn := &entity.CreateCronJob{}
	if err := json.Unmarshal(body, jobn); err != nil {
		protocol.FailedJson(w, errors.BodyDecodeError, err.Error())
		return
	}
	if err := verify.VerifyCreateCronJob(jobn); err != nil {
		protocol.FailedJson(w, errors.VerifyJobError, err.Error())
		return
	}
	var metadata string
	if jobn.Metadata != nil {
		b, _ := json.Marshal(jobn.Metadata)
		metadata = string(b)
	}
	var n int64
	if n, err = storage.GetFactory().ExistCronJobByJobName(ctx, jobn.JobName); err != nil {
		protocol.FailedJson(w, errors.SqlQueryError, err.Error())
		return
	}
	if n > 0 {
		protocol.FailedJson(w, errors.AlreadyExistsError, "")
		return
	}
	var delay time.Duration
	// send to delayQueue
	jobId := utils.UUID()
	now := time.Now().Unix()
	jobn.JobId = jobId
	spec, err := utils.GenCronJob(jobn)
	if err != nil {
		protocol.FailedJson(w, errors.GenJobError, err.Error())
		return
	}
	// save work tasks
	_, err = storage.GetFactory().CreateCronJob(ctx, &entity.CronJob{
		JobId:      jobId,
		JobName:    jobn.JobName,
		Namespace:  jobn.Namespace,
		Metadata:   metadata,
		Schedule:   jobn.Schedule,
		Image:      jobn.Image,
		Command:    jobn.Command,
		CreateTime: now,
		UpdateTime: now,
	})
	if err != nil {
		protocol.FailedJson(w, errors.SqlCreateError, err.Error())
		return
	}
	_, err = storage.GetFactory().CreateJobStatus(ctx, &entity.JobStatus{
		JobId:      jobId,
		JobType:    entity.Cron,
		JobState:   entity.Submit,
		CreateTime: now,
		UpdateTime: now,
	})
	if err != nil {
		protocol.FailedJson(w, errors.SqlCreateError, err.Error())
		return
	}
	if jobn.JobExecTime > now {
		delay = time.Duration(jobn.JobExecTime-now) * time.Second
	}
	err = infrastructure.GetDelayMq().DeferredPublish(
		config.GetCfg().JobPushTopic,
		delay,
		&entity.JobMessage{
			JobType:   entity.Cron,
			JobAction: entity.Create,
			JobId:     jobId,
			JobName:   jobn.JobName,
			Namespace: jobn.Namespace,
			CronJob:   spec,
		})
	if err != nil {
		protocol.FailedJson(w, errors.PublishError, err.Error())
		return
	}
	protocol.SuccessJson(w, entity.JobCreateSuccess{JobId: jobId})
}

func UpdateCronJob(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	protocol.SuccessMsg(w, "TODO")
}

func DeleteCronJob(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	jobId := p.ByName("job_id")
	if jobId == "" {
		protocol.FailedJson(w, errors.InvalidParameter, "job_id is empty")
		return
	}
	ctx := GetCtx()
	job, err := storage.GetFactory().GetCronJobByJobIdJoinStatus(ctx, jobId)
	if err != nil {
		protocol.FailedJson(w, errors.SqlQueryError, err.Error())
		return
	}
	switch job.JobState {
	case entity.DeleteSuccess:
		protocol.SuccessMsg(w, "job_status is delete")
		return
	case entity.Submit:
		protocol.FailedJson(w, errors.JobSubmittedError, fmt.Sprintf("job_id=%s submitted waiting for job to run", job.JobId))
		return
	}

	err = infrastructure.GetDelayMq().Publish(
		config.GetCfg().JobOperateTopic,
		&entity.JobMessage{
			JobType:   entity.Cron,
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
	case <-time.After(config.GetCfg().OperateMaxTimeout):
		protocol.FailedJson(w, errors.DeleteJobTimeoutError, "")
		return
	}
	if notify.JobStatus != entity.DeleteSuccess {
		protocol.FailedJson(w, errors.DeleteJobTimeoutError, fmt.Sprintf("delete cron_job job_id=%s status=%d failed notify=%s", jobId, notify.JobStatus, notify.Cause))
		return
	}
	if _, err = storage.GetFactory().UpdateJobState(ctx, &entity.JobStatus{JobState: entity.DeleteSuccess, JobId: jobId, UpdateTime: time.Now().Unix()}); err != nil {
		protocol.FailedJson(w, errors.SqlUpdateError, err.Error())
		return
	}
	protocol.SuccessMsg(w, fmt.Sprintf("delete cron_job success job_id: %s", jobId))
}

func ListCronJobs(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
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
	var res []*entity.CronJob
	var err error
	ctx := GetCtx()
	total, _ = storage.GetFactory().CountByCronJobs(ctx)
	if res, err = storage.GetFactory().PageCronJobs(ctx, offset, limit); err != nil {
		protocol.FailedJson(w, errors.SqlQueryError, err.Error())
		return
	}
	log.Logger().Info("ListCronJobs success page_num=%d page_size=%d", pageNum, pageSize)
	protocol.SuccessJson(w, &entity.PageCronJob{
		Total:    total,
		PageNum:  pageNum,
		PageSize: pageSize,
		Data:     res,
	})
}

func DescribeCronJob(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	jobId := p.ByName("job_id")
	if jobId == "" {
		protocol.FailedJson(w, errors.InvalidParameter, "job_id is empty")
	}
	ctx := GetCtx()
	job, err := storage.GetFactory().GetCronJobByJobIdJoinStatus(ctx, jobId)
	if err != nil {
		protocol.FailedJson(w, errors.SqlQueryError, err.Error())
		return
	}
	protocol.SuccessJson(w, job)
}
