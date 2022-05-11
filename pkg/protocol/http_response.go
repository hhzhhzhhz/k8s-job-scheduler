package protocol

import (
	"github.com/hhzhhzhhz/k8s-job-scheduler/log"
	"github.com/hhzhhzhhz/k8s-job-scheduler/pkg/errors"
	"github.com/hhzhhzhhz/k8s-job-scheduler/pkg/utils"
	json "github.com/json-iterator/go"
	"net/http"
)

const (
	failed = -1
)

var (
	success      = HttpResponse{Code: http.StatusOK, Cause: ""}
	unknownError = []byte(`{"Code":-1,"cause":"service unknown error"}`)
)

type HttpResponse struct {
	Code     int64       `json:"code"`
	Cause    string      `json:"cause"`
	ErrorMsg string      `json:"error_msg"`
	Data     interface{} `json:"data"`
}

func SuccessMsg(w http.ResponseWriter, msg string) error {
	resp := &HttpResponse{Code: 0, Data: msg}
	return writeResponse(w, resp)
}

func SuccessJson(w http.ResponseWriter, obj interface{}) error {
	resp := HttpResponse{Code: http.StatusOK, Cause: "", Data: obj}
	return writeResponse(w, resp)
}

func FailedJson(w http.ResponseWriter, error errors.Error, cause string) error {
	resp := HttpResponse{Code: failed, Cause: cause}
	return writeResponse(w, resp)
}

func writeResponse(w http.ResponseWriter, data interface{}) error {
	b, err := json.MarshalIndent(data, "", "    ")
	if err != nil {
		log.Logger().Error("locate=%s Response marshal failed cause=%s message=%+v", utils.Caller(2), err.Error(), data)
		return err
	}
	if _, err := w.Write(b); err != nil {
		log.Logger().Error("locate=%s Response write failed cause=%s message=%s", utils.Caller(2), err.Error(), string(b))
		return err
	}
	return nil
}
