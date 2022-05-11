package api

import (
	"github.com/hhzhhzhhz/k8s-job-scheduler/entity"
	"github.com/hhzhhzhhz/k8s-job-scheduler/infrastructure/storage"
	"github.com/hhzhhzhhz/k8s-job-scheduler/pkg/errors"
	"github.com/hhzhhzhhz/k8s-job-scheduler/pkg/protocol"
	"github.com/hhzhhzhhz/k8s-job-scheduler/pkg/utils"
	"github.com/julienschmidt/httprouter"
	"net/http"
)

// JobLogs query job exec logs
func JobLogs(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	ctx := GetCtx()
	jobId := p.ByName("job_id")
	if jobId == "" {
		protocol.FailedJson(w, errors.JobIdError, "job_id is empty.")
		return
	}
	var err error
	var res []*entity.JobLogs
	if res, err = storage.GetFactory().GetJobLogsByPods(ctx, jobId); err != nil {
		protocol.FailedJson(w, errors.SqlQueryError, err.Error())
		return
	}
	for _, re := range res {
		deco, _ := utils.Unzip(re.RunLogs)
		re.RunLogs = []byte{}
		re.Logs = deco
	}
	protocol.SuccessJson(w, res)
}
