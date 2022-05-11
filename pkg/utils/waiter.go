package utils

import (
	"github.com/hhzhhzhhz/k8s-job-scheduler/entity"
	"sync"
	"time"
)

const (
	defaultTimeout       = 5 * time.Minute
	rotationTime         = 1 * time.Minute
	defaultWaiterBufSize = 10
)

type WaiterMessage struct {
	JobStatus  entity.JobState
	Name       string
	Times      int
	CreateTime int64
	Cause      string
	Watch      chan struct{}
}

type Waiter interface {
	Done(waiter *WaiterMessage)
	Watch(waiter *WaiterMessage)
}

type waiterMessage struct {
	wait       *entity.JobExecStatusMessage
	createTime int64
}

func NewWaiter() Waiter {
	wait := &waiter{
		wimp:    make(map[string]*WaiterMessage, defaultWaiterBufSize),
		timeout: defaultTimeout,
	}
	wait.AsyncLoop()
	return wait
}

type waiter struct {
	mux     sync.Mutex
	sw      WaitGroupWrapper
	timeout time.Duration
	wimp    map[string]*WaiterMessage
}

// Watch Will there be multiple identical data?
func (w *waiter) Watch(waiter *WaiterMessage) {
	w.mux.Lock()
	defer w.mux.Unlock()
	//_, ok := w.wimp[waiter.Name]
	//if ok {
	//	return
	//}
	if waiter.Times <= 0 {
		waiter.Times = 1
	}
	w.wimp[waiter.Name] = waiter
}

func (w *waiter) Done(done *WaiterMessage) {
	w.mux.Lock()
	defer w.mux.Unlock()
	waiter, ok := w.wimp[done.Name]
	if ok && waiter.Times > 0 {
		waiter.Times--
		waiter.Cause = done.Cause
		waiter.JobStatus = done.JobStatus
		waiter.Watch <- struct{}{}
	}
}

func (w *waiter) AsyncLoop() {
	tick := time.Tick(rotationTime)
	w.sw.Wrap(func() {
		for {
			select {
			case <-tick:
				w.clearCache()
			}
		}
	})

}

func (w *waiter) clearCache() {
	w.mux.Lock()
	defer w.mux.Unlock()
	dels := []string{}
	now := time.Now().Unix()
	for del, waiter := range w.wimp {
		if now-waiter.CreateTime > int64(w.timeout.Seconds()) {
			dels = append(dels, del)
		}
	}
	for _, del := range dels {
		delete(w.wimp, del)
	}
}
