package api

import (
	"fmt"
	"github.com/hhzhhzhhz/k8s-job-scheduler/entity"
	json "github.com/json-iterator/go"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
)

var cli = http.DefaultClient

type Handler struct {
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println(string(b))
	w.Write([]byte("xx"))
}

func Test_CallbackServer(t *testing.T) {
	if err := http.ListenAndServe(":9999", &Handler{}); err != nil {
		t.Error(err.Error())
	}
}

func Test_OnceJob_api(t *testing.T) {
	t.Run("tail", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			c := entity.CreateOnceJob{
				JobName:     fmt.Sprintf("k8s-job-once-test-%d", i),
				JobExecTime: 0,
				Namespace:   "default",
				Metadata: map[string]string{"active_deadline_seconds": "600",
					"ttl_seconds_after_finished": "600", "completions": "2", "callback_url": "http://127.0.0.1:9999/",
					"log_collect": "true"},
				Image:   "perl",
				Command: "tail,-f,/var/log/alternatives.log",
			}
			b, err := json.Marshal(c)
			if err != nil {
				t.Error(err.Error())
				continue
			}

			r, err := http.NewRequest("POST", "http://127.0.0.1:65532/api/once_job/create", strings.NewReader(string(b)))
			if err != nil {
				t.Error(err.Error())
				continue
			}
			resp, err := cli.Do(r)
			if err != nil {
				t.Error(err.Error())
				continue
			}
			defer resp.Body.Close()
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				t.Error(err.Error())
				continue
			}
			t.Log(string(body))
		}
	})
	t.Run("echo", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			c := entity.CreateOnceJob{
				JobName:     fmt.Sprintf("k8s-job-once-echo-%d", i),
				JobExecTime: 0,
				Namespace:   "default",
				Metadata: map[string]string{"active_deadline_seconds": "600", "ttl_seconds_after_finished": "600", "completions": "2", "callback_url": "http://127.0.0.1:9999/",
					"log_collect": "true"},
				Image:   "perl",
				Command: "echo,hello world !!!!!!!",
			}
			b, err := json.Marshal(c)
			if err != nil {
				t.Error(err.Error())
				continue
			}

			r, err := http.NewRequest("POST", "http://127.0.0.1:65532/api/once_job/create", strings.NewReader(string(b)))
			if err != nil {
				t.Error(err.Error())
				continue
			}
			resp, err := cli.Do(r)
			if err != nil {
				t.Error(err.Error())
				continue
			}
			defer resp.Body.Close()
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				t.Error(err.Error())
				continue
			}
			t.Log(string(body))
		}
	})
}

type Resp struct {
	JobId string `json:"job_id"`
	Code  int    `json:"code"`
	Cause string `json:"cause"`
}

func Test_Cron_delete(t *testing.T) {

}

func Test_Cron_Create(t *testing.T) {
	t.Run("tail", func(t *testing.T) {
		c := entity.CreateCronJob{
			JobName:     "cron-test-tail",
			JobExecTime: 0,
			Namespace:   "default",
			Metadata: map[string]string{"active_deadline_seconds": "600", "ttl_seconds_after_finished": "600", "completions": "2", "callback_url": "http://127.0.0.1:9999/",
				"log_collect": "true"},
			Image:    "perl",
			Command:  "tail,-f,/var/log/alternatives.log",
			Schedule: "*/1 * * * *",
		}
		b, err := json.Marshal(c)
		if err != nil {
			t.Error(err.Error())
			return
		}

		r, err := http.NewRequest("POST", "http://127.0.0.1:65532/api/cron_job/create", strings.NewReader(string(b)))
		if err != nil {
			t.Error(err.Error())
			return
		}
		resp, err := cli.Do(r)
		if err != nil {
			t.Error(err.Error())
			return
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Error(err.Error())
			return
		}
		data := &Resp{}
		if err := json.Unmarshal(body, data); err != nil {
			t.Error(err.Error())
			return
		}
		t.Log(string(body))
	})
	t.Run("echo", func(t *testing.T) {
		c := entity.CreateCronJob{
			JobName:     "cron-test-echo",
			JobExecTime: 0,
			Namespace:   "default",
			Metadata: map[string]string{"active_deadline_seconds": "600", "ttl_seconds_after_finished": "600", "completions": "2", "callback_url": "http://127.0.0.1:9999/",
				"log_collect": "true"},
			Image:    "perl",
			Command:  "echo,/var/log/alternatives.log",
			Schedule: "*/1 * * * *",
		}
		b, err := json.Marshal(c)
		if err != nil {
			t.Error(err.Error())
			return
		}

		r, err := http.NewRequest("POST", "http://127.0.0.1:65532/api/cron_job/create", strings.NewReader(string(b)))
		if err != nil {
			t.Error(err.Error())
			return
		}
		resp, err := cli.Do(r)
		if err != nil {
			t.Error(err.Error())
			return
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Error(err.Error())
			return
		}
		data := &Resp{}
		if err := json.Unmarshal(body, data); err != nil {
			t.Error(err.Error())
			return
		}
		t.Log(string(body))
	})
}

func Test_Delete_Job(t *testing.T) {

	t.Run("cron_delete", func(t *testing.T) {
		r, err := http.NewRequest("DELETE", "http://127.0.0.1:65532/api/cron_job/delete/e1326ded-6ff4-431f-8b21-f98feb8a9f6d", strings.NewReader(""))
		if err != nil {
			t.Error(err.Error())
			return
		}
		resp, err := cli.Do(r)
		if err != nil {
			t.Error(err.Error())
			return
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Error(err.Error())
			return
		}
		data := &Resp{}
		if err := json.Unmarshal(body, data); err != nil {
			t.Error(err.Error())
			return
		}
		t.Log(string(body))
	})
}
