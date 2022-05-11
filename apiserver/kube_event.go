package apiserver

import (
	"context"
	"github.com/hhzhhzhhz/k8s-job-scheduler/pkg/utils"
	v1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/tools/cache"
	"sync"
	"time"
)

const (
	cronJobs = "Cronjobs"
	jobs     = "Jobs"
	pods     = "pods"
)

type EventType int

const (
	PodAdd EventType = iota
	PodUpdate
	PodDelete
	OnceAdd
	OnceDelete
	OnceUpdate
	CronAdd
	CronDelete
	CronUpdate
)

type Event struct {
	Type EventType
	Data interface{}
}

func GetEventType(t EventType) string {
	var str string
	switch t {
	case PodAdd:
		str = "PodAdd"
	case PodUpdate:
		str = "PodUpdate"
	case PodDelete:
		str = "PodDelete"
	case OnceAdd:
		str = "OnceAdd"
	case OnceUpdate:
		str = "OnceUpdate"
	case OnceDelete:
		str = "OnceDelete"
	case CronAdd:
		str = "CronAdd"
	case CronDelete:
		str = "CronDelete"
	case CronUpdate:
		str = "CronUpdate"
	}
	return str
}

type KubeEvent interface {
	Subscribe(namespace string, f func(event *Event)) error
	Unsubscribe(namespace string)
}

func NewCatchControllerWrapper(c cache.Controller) *controllerWrapper {
	return &controllerWrapper{
		stop: make(chan struct{}, 1),
		c:    c,
	}
}

type controllerWrapper struct {
	stop chan struct{}
	c    cache.Controller
}

func (w *controllerWrapper) Run() {
	w.c.Run(w.stop)
}

func (w *controllerWrapper) Close() {
	w.stop <- struct{}{}
}

type bridge struct {
	namespace string
	fun       func(event *Event)
	sw        utils.WaitGroupWrapper
	pod       *controllerWrapper
	once      *controllerWrapper
	cron      *controllerWrapper
}

func (b *bridge) Run() {
	b.sw.Wrap(func() {
		b.pod.Run()
	})
	b.sw.Wrap(func() {
		b.once.Run()
	})
	b.sw.Wrap(func() {
		b.cron.Run()
	})
}

func (b *bridge) Close() {
	b.pod.Close()
	b.once.Close()
	b.cron.Close()
	b.sw.Wait()
}

func NewKubeEvent(ctx context.Context, api Apiserver) KubeEvent {
	ctx, cancel := context.WithCancel(ctx)
	return &kubeEvent{
		ctx:     ctx,
		cancel:  cancel,
		api:     api,
		bridges: make(map[string]*bridge, 1),
	}
}

type kubeEvent struct {
	ctx     context.Context
	cancel  context.CancelFunc
	mux     sync.Mutex
	sw      utils.WaitGroupWrapper
	bridges map[string]*bridge
	api     Apiserver
}

func (e *kubeEvent) Subscribe(ns string, fun func(event *Event)) error {
	e.mux.Lock()
	defer e.mux.Unlock()
	_, ok := e.bridges[ns]
	if ok {
		return nil
	}

	if ns != "" {
		if err := e.api.CreateNamespace(e.withCtx(), ns); err != nil {
			return err
		}
	}

	_, pod := cache.NewInformer(
		cache.NewListWatchFromClient(
			e.api.CoreV1().RESTClient(),
			pods,
			ns,
			fields.Everything(),
		),
		&corev1.Pod{},
		time.Duration(0),
		e.eventHandler(fun),
	)

	_, once := cache.NewInformer(
		cache.NewListWatchFromClient(
			e.api.BatchV1().RESTClient(),
			jobs,
			ns,
			fields.Everything(),
		),
		&v1.Job{},
		time.Duration(0),
		e.eventHandler(fun),
	)

	_, cron := cache.NewInformer(
		cache.NewListWatchFromClient(
			e.api.BatchV1().RESTClient(),
			cronJobs,
			ns,
			fields.Everything(),
		),
		&v1.CronJob{},
		time.Duration(0),
		e.eventHandler(fun),
	)
	bride := &bridge{
		namespace: ns,
		fun:       fun,
		pod:       NewCatchControllerWrapper(pod),
		once:      NewCatchControllerWrapper(once),
		cron:      NewCatchControllerWrapper(cron),
	}
	bride.Run()
	e.bridges[ns] = bride
	return nil
}

func (e *kubeEvent) Unsubscribe(ns string) {
	e.mux.Lock()
	defer e.mux.Unlock()
	bride, ok := e.bridges[ns]
	if ok {
		bride.Close()
		delete(e.bridges, ns)
	}
}

func (e *kubeEvent) withCtx() context.Context {
	ctx, _ := context.WithCancel(e.ctx)
	return ctx
}

func (e *kubeEvent) Close() error {
	e.cancel()
	for _, bride := range e.bridges {
		bride.Close()
	}
	e.sw.Wait()
	return nil
}

// eventHandler
// job/cronjob add不处理, job部署成功后通知, 部署模块处理
// pod只处理 add 事件, 当运行完成或者失败采集容器日志
func (e *kubeEvent) eventHandler(fun func(event *Event)) cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			switch obj.(type) {
			case *v1.Job:
				fun(&Event{Type: OnceAdd, Data: obj})
			//case *v1.CronJob:
			//	fun(&Event{Type: CronAdd, Data: obj})
			case *corev1.Pod:
				fun(&Event{Type: PodAdd, Data: obj})
			}
		},
		DeleteFunc: func(obj interface{}) {
			switch obj.(type) {
			case *v1.Job:
				fun(&Event{Type: OnceDelete, Data: obj})
			case *v1.CronJob:
				fun(&Event{Type: CronDelete, Data: obj})
				//case *corev1.Pod:
				//	fun(&Event{Type: PodDelete, Data: obj})
			}
		},
		UpdateFunc: func(objo, objn interface{}) {
			//if reflect.DeepEqual(objo, objn) {
			//	return
			//}
			switch objn.(type) {
			case *v1.Job:
				fun(&Event{Type: OnceUpdate, Data: objn})
			case *v1.CronJob:
				fun(&Event{Type: CronUpdate, Data: objn})
			case *corev1.Pod:
				fun(&Event{Type: PodUpdate, Data: objn})
			}
		},
	}
}
