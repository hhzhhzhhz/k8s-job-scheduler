package deploy

import (
	"context"
	"fmt"
	"github.com/hhzhhzhhz/k8s-job-scheduler/apiserver"
	"github.com/hhzhhzhhz/k8s-job-scheduler/entity"
	"github.com/hhzhhzhhz/k8s-job-scheduler/infrastructure"
	"github.com/hhzhhzhhz/k8s-job-scheduler/log"
	"github.com/hhzhhzhhz/k8s-job-scheduler/pkg/utils"
	"io"
	corev1 "k8s.io/api/core/v1"
	"sync"
	"time"
)

const (
	defaultWorkTimeoutMinute = 1 * time.Minute
	defaultMapSize           = 10
	defaultRotationTime      = 1 * time.Minute
	defaultClearWorkTime     = 10 * time.Minute
	defaultReadBufSize       = 2048
	defaultGzipMinSize       = 24
)

type WorkInfo struct {
	JobType    entity.JobType
	Namespace  string
	JobId      string
	JobName    string
	PodName    string
	Topic      string
	Timeout    time.Duration
	CreateTime int64
	running    bool
}

func (w *WorkInfo) copy(podName string) *WorkInfo {
	return &WorkInfo{
		Namespace:  w.Namespace,
		PodName:    podName,
		Topic:      w.Topic,
		Timeout:    w.Timeout,
		CreateTime: w.CreateTime,
	}
}

var byteSlicePool = sync.Pool{
	New: func() interface{} {
		return make([]byte, defaultReadBufSize)
	},
}

type Collect interface {
	RunLog(c *WorkInfo)
	FailureLog(pod *corev1.Pod, w *WorkInfo)
	Close() error
}

func NewCollect(ctx context.Context, api apiserver.Apiserver, mq infrastructure.Delaymq) Collect {
	ctx, cancel := context.WithCancel(ctx)
	col := &collect{
		ctx:      ctx,
		cancel:   cancel,
		api:      api,
		mq:       mq,
		pods:     make(map[string]*WorkInfo, defaultMapSize),
		rotation: defaultRotationTime,
	}
	col.clear()
	return col
}

type collect struct {
	ctx      context.Context
	cancel   context.CancelFunc
	mux      sync.Mutex
	api      apiserver.Apiserver
	sw       utils.WaitGroupWrapper
	mq       infrastructure.Delaymq
	pods     map[string]*WorkInfo
	rotation time.Duration
}

// Running GetLog should be pushed to storage
func (e *collect) Running(w *WorkInfo) {
	var podName string
	var ctx context.Context
	ctx, _ = context.WithTimeout(context.Background(), w.Timeout)
	if w.Timeout <= 0 {
		ctx, _ = context.WithTimeout(context.Background(), defaultWorkTimeoutMinute)
	}
	podName = w.PodName
	gz := utils.NewGzip()
	reader, err := e.api.GetLogs(ctx, w.Namespace, podName)
	if err != nil {
		log.Logger().Error("Collect.RunLog work GetLogs failed pod_name=%s namespace=%s cause=%s", podName, w.Namespace, err.Error())
		return
	}
	nums := 0
	for {
		temp := byteSlicePool.Get().([]byte)
		n, err := reader.Read(temp)
		if err != nil {
			break
		}
		gz.Write(temp[:n])
		nums++
		temp = temp[:cap(temp)]
		byteSlicePool.Put(temp)
	}
	if nums <= 0 {
		log.Logger().Warn("Collect.RunLog log collection completed no data pod_name=%s namespace=%s", podName, w.Namespace)
		return
	}
	e.mq.RetryPublish(
		w.Topic,
		&entity.JobExecStatusMessage{
			JobType:  w.JobType,
			JobName:  w.JobName,
			JobId:    w.JobId,
			JobState: entity.RunLogs,
			PodName:  w.PodName,
			Logs:     gz.Bytes(),
			Time:     w.CreateTime,
		})
	log.Logger().Info("Collect.RunLog log collection completed pod_name=%s namespace=%s", podName, w.Namespace)
}

// RunLog May submits the same pod multiple times
func (e *collect) RunLog(work *WorkInfo) {
	e.mux.Lock()
	defer e.mux.Unlock()
	w := work
	pod, ok := e.pods[w.PodName]
	if !ok {
		w.running = true
		e.pods[w.PodName] = w
		e.sw.Wrap(func() {
			log.Logger().Info("Collect.RunLog collection run log pod_name=%s namespace=%s", w.PodName, w.Namespace)
			e.Running(w)
		})
		return
	}
	if ok && !pod.running {
		pod.running = true
		e.sw.Wrap(func() {
			log.Logger().Info("Collect.RunLog collection run log pod_name=%s namespace=%s", w.PodName, w.Namespace)
			e.Running(pod)
		})
	}

}

func (e *collect) FailureLog(pod *corev1.Pod, w *WorkInfo) {
	e.mux.Lock()
	id := string(pod.UID)
	_, ok := e.pods[id]
	if ok {
		e.mux.Unlock()
		return
	}
	e.pods[id] = w
	e.mux.Unlock()
	log.Logger().Info("Collect.RunLog collection FailureLog pod_name=%s namespace=%s", w.PodName, w.Namespace)
	gz := utils.NewGzip()
	e.containerStatusLog(gz, pod.Status.InitContainerStatuses)
	e.containerStatusLog(gz, pod.Status.ContainerStatuses)
	e.containerStatusLog(gz, pod.Status.EphemeralContainerStatuses)
	e.mq.RetryPublish(
		w.Topic,
		&entity.JobExecStatusMessage{
			JobType:  w.JobType,
			JobName:  w.JobName,
			JobId:    w.JobId,
			JobState: entity.RunLogs,
			PodName:  w.PodName,
			Logs:     gz.Bytes(),
			Time:     w.CreateTime,
		})
}

func (e *collect) containerStatusLog(w io.Writer, cs []corev1.ContainerStatus) {
	if len(cs) == 0 {
		return
	}
	c := cs[0]
	terminal := c.State.Terminated
	if terminal != nil {
		w.Write([]byte(fmt.Sprintf("State.Terminated exitCode=%d reason=%s message=%s\n", terminal.ExitCode, terminal.Reason, terminal.Message)))
	}
	wait := c.State.Waiting
	if wait != nil {
		w.Write([]byte(fmt.Sprintf("State.Waiting reason=%s message=%s\n", wait.Reason, wait.Message)))
	}
	return
}

func (e *collect) Close() error {
	e.cancel()
	e.sw.Wait()
	return nil
}

func (e *collect) clear() {
	tick := time.Tick(e.rotation)
	e.sw.Wrap(func() {
		for {
			select {
			case <-tick:
				now := time.Now().Unix()
				e.mux.Lock()
				e.clearPods(now)
				e.mux.Unlock()
			case <-e.ctx.Done():
				return
			}
		}
	})
}

func (e *collect) clearPods(now int64) {
	var dells []string
	for pod, work := range e.pods {
		if now-work.CreateTime > int64(defaultClearWorkTime.Seconds()) {
			dells = append(dells, pod)
		}
	}
	for _, del := range dells {
		delete(e.pods, del)
	}
}
