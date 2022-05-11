package server

import (
	"fmt"
	"github.com/hhzhhzhhz/k8s-job-scheduler/api"
	"github.com/hhzhhzhhz/k8s-job-scheduler/election"
	"github.com/hhzhhzhhz/k8s-job-scheduler/log"
	"github.com/hhzhhzhhz/k8s-job-scheduler/pkg/errors"
	"github.com/hhzhhzhhz/k8s-job-scheduler/pkg/protocol"
	"github.com/julienschmidt/httprouter"
	"github.com/vulcand/oxy/forward"
	"net/http"
	"net/url"
	"sync/atomic"
)

func NewAppServer(addr string, election election.Election) *AppServer {
	return &AppServer{addr: addr, election: election}
}

type AppServer struct {
	close    int32
	addr     string
	fwd      *forward.Forwarder
	election election.Election
}

func (app *AppServer) Run() error {
	var err error
	app.fwd, err = forward.New()
	if err != nil {
		return fmt.Errorf("forward.New failed cause=%s", err.Error())
	}
	router := httprouter.New()

	// once_job
	router.GET("/api/once_job/lists", app.proxy(api.ListOnceJobs))
	router.GET("/api/once_job/describe/:job_id", app.proxy(api.DescribeOnceJob))
	router.POST("/api/once_job/create", app.proxy(api.CreateOnceJob))
	router.POST("/api/once_job/update", app.proxy(api.UpdateOnceJob))
	router.DELETE("/api/once_job/delete/:job_id", app.proxy(api.DeleteOnceJob))

	// cron_job
	router.GET("/api/cron_job/lists", app.proxy(api.ListCronJobs))
	router.GET("/api/cron_job/describe/:job_id", app.proxy(api.DescribeCronJob))
	router.POST("/api/cron_job/create", app.proxy(api.CreateCronJob))
	router.POST("/api/cron_job/update", app.proxy(api.UpdateCronJob))
	router.DELETE("/api/cron_job/delete/:job_id", app.proxy(api.DeleteCronJob))

	// job_logs
	router.GET("/api/job_logs/:job_id", app.proxy(api.JobLogs))
	return http.ListenAndServe(app.addr, router)
}

func (app *AppServer) proxy(next httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		if atomic.LoadInt32(&app.close) < 0 {
			protocol.FailedJson(w, errors.ServerClosedError, "")
			return
		}
		if r.Method != http.MethodGet {
			identity := app.election.Leadership()
			if identity != "" {
				log.Logger().Info("%s %s need forward=%s", r.Method, r.RequestURI, identity)
				url, err := url.ParseRequestURI(fmt.Sprintf("http://%s%s", identity, r.RequestURI))
				if err != nil {
					protocol.FailedJson(w, errors.ForwarderUrlError, fmt.Sprintf("forwarder parse url=%s failed cause=%s", url, identity))
					return
				}
				r.URL = url
				app.fwd.ServeHTTP(w, r)
				return
			}
		}
		log.Logger().Info("%s %s", r.Method, r.RequestURI)
		next(w, r, p)
	}
}

func (app *AppServer) Close() error {
	atomic.AddInt32(&app.close, -1)
	return app.election.Close()
}
