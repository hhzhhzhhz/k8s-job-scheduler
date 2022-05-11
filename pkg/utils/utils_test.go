package utils

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	v1 "k8s.io/api/batch/v1"
	"os"
	"testing"
	"time"
)

func Test_Parse_yaml(t *testing.T) {
	r, err := ioutil.ReadFile("./job.yml")
	if err != nil {
		t.Error(err)
	}
	job := &v1.Job{}
	if err := UnmarshalYaml(r, job); err != nil {
		t.Error(err.Error())
	}
	t.Log(job)
}

func Test_retry(t *testing.T) {
	t.Run("allegro", func(t *testing.T) {
		retry := 3
		nums := 0
		Retry(retry, func() error {
			nums++
			return fmt.Errorf("xxx")
		})
		if retry != nums-1 {
			t.Errorf("nums=%d, retry=%d", nums, retry)
		}
	})
	t.Run("normal", func(t *testing.T) {
		retry := 0
		nums := 0
		Retry(0, func() error {
			nums++
			return nil
		})
		if retry != nums-1 {
			t.Errorf("nums=%d, retry=%d", nums, retry)
		}
	})
	t.Run("running error", func(t *testing.T) {
		retry := 3
		nums := 0
		Retry(retry, func() error {
			nums++
			if nums == 1 {
				return fmt.Errorf("xxx")
			}
			return nil
		})
		if nums != 2 {
			t.Errorf("nums=%d, retry=%d", nums, retry)
		}
	})

}

func Test_Waiter(t *testing.T) {
	w := NewWaiter()
	notify := &WaiterMessage{
		Name:       "xxx",
		CreateTime: time.Now().Unix(),
		Watch:      make(chan struct{}, 1),
	}
	w.Watch(notify)
	go func() {
		select {
		case <-notify.Watch:
			t.Log("done")
		case <-time.After(3 * time.Second):
			t.Log("timeout")
		}
	}()
	time.Sleep(2 * time.Second)
	w.Done(&WaiterMessage{
		Name:  "xxx",
		Cause: "causet",
	})
	time.Sleep(2 * time.Second)
	w.Done(&WaiterMessage{
		Name:  "xxx",
		Cause: "causet",
	})
	t.Logf("%+v", notify)
}

func Test_Ip(t *testing.T) {
	ip, err := GetPodIP()
	if err != nil {
		t.Error(err.Error())
	}
	t.Log(ip)
}

func Test_Tick(t *testing.T) {
	tick := time.NewTimer(1 * time.Second)
	select {
	case <-tick.C:
		if !tick.Stop() {
			t.Log("xxx")
			<-tick.C
		}
	}

}

func Test_Gzip(t *testing.T) {
	var buf bytes.Buffer
	g := gzip.NewWriter(&buf)
	//_, err := g.Write([]byte("hello world!!!"))
	//if err != nil {
	//	t.Error(err)
	//	return
	//}
	//_, err = g.Write([]byte("hello world!!!"))
	//if err != nil {
	//	t.Error(err)
	//	return
	//}
	if err := g.Close(); err != nil {
		t.Error(err)
		return
	}
	t.Log(buf.Len())
	zr, err := gzip.NewReader(bytes.NewReader([]byte(buf.String())))
	if err != nil {
		t.Error(err)
		return
	}
	if _, err := io.Copy(os.Stdout, zr); err != nil {
		t.Error(err)
	}
}
