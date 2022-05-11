package apiserver

import (
	"context"
	"fmt"
	"io"
	v1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	kubeclient "k8s.io/client-go/kubernetes"
	batchv1 "k8s.io/client-go/kubernetes/typed/batch/v1"
	kcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

var (
	deletePolicy = metav1.DeletePropagationBackground
	defaultKind  = "Job"
)

type Apiserver interface {
	WatchCoreEvent(ctx context.Context, namespace string) (watch.Interface, error)
	WatchOnceJobEvent(ctx context.Context, namespace string) (watch.Interface, error)
	WatchCronJobEvent(ctx context.Context, namespace string) (watch.Interface, error)
	CreateOnceJob(ctx context.Context, namespace string, job *v1.Job) (*v1.Job, error)
	UpdateOnceJob(ctx context.Context, namespace string, job *v1.Job) (*v1.Job, error)
	CreateCronJob(ctx context.Context, namespace string, job *v1.CronJob) (*v1.CronJob, error)
	DeleteOnceJob(ctx context.Context, namespace, jobName string) error
	DeleteCronJob(ctx context.Context, namespace, jobName string) error
	GetOnceJob(ctx context.Context, namespace, jobName string) (*v1.Job, error)
	CreateNamespace(ctx context.Context, ns string) error
	GetLogs(ctx context.Context, namespace, podName string) (io.ReadCloser, error)
	GetPodName(ctx context.Context, jobName, namespace, uid string) (*corev1.EventList, error)
	BatchV1() batchv1.BatchV1Interface
	CoreV1() kcorev1.CoreV1Interface
}

func NewApiserver(kubecli kubeclient.Interface) Apiserver {
	return &apiserver{kibble: kubecli}
}

type apiserver struct {
	kibble kubeclient.Interface
}

// WatchCoreEvent WatchPodEvent severely delayed delete events
func (e *apiserver) WatchCoreEvent(ctx context.Context, namespace string) (watch.Interface, error) {
	return e.kibble.CoreV1().Events(namespace).Watch(ctx, metav1.ListOptions{Watch: true})
}

// WatchOnceJobEvent Can immediately receive job deletion events
func (e *apiserver) WatchOnceJobEvent(ctx context.Context, namespace string) (watch.Interface, error) {
	return e.kibble.BatchV1().Jobs(namespace).Watch(ctx, metav1.ListOptions{Watch: true})
}

func (e *apiserver) WatchCronJobEvent(ctx context.Context, namespace string) (watch.Interface, error) {
	return e.kibble.BatchV1().CronJobs(namespace).Watch(ctx, metav1.ListOptions{Watch: true})
}

func (e *apiserver) GetPodName(ctx context.Context, jobName, namespace, uid string) (*corev1.EventList, error) {
	fieldSelector := e.kibble.CoreV1().Events(namespace).GetFieldSelector(&jobName, &namespace, &defaultKind, &uid)
	initialOpts := metav1.ListOptions{FieldSelector: fieldSelector.String()}
	return e.kibble.CoreV1().Events(namespace).List(ctx, initialOpts)
}

func (e *apiserver) CreateOnceJob(ctx context.Context, namespace string, job *v1.Job) (*v1.Job, error) {
	job, err := e.kibble.BatchV1().Jobs(namespace).Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}
	return job, nil
}

func (e *apiserver) UpdateOnceJob(ctx context.Context, namespace string, job *v1.Job) (*v1.Job, error) {
	job, err := e.kibble.BatchV1().Jobs(namespace).Update(context.TODO(), job, metav1.UpdateOptions{})
	if err != nil {
		return nil, err
	}
	return job, nil
}

func (e *apiserver) CreateCronJob(ctx context.Context, namespace string, job *v1.CronJob) (*v1.CronJob, error) {
	job, err := e.kibble.BatchV1().CronJobs(namespace).Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}
	return job, nil
}

func (e *apiserver) DeleteOnceJob(ctx context.Context, namespace, jobName string) error {
	return e.kibble.BatchV1().Jobs(namespace).Delete(context.TODO(), jobName, metav1.DeleteOptions{PropagationPolicy: &deletePolicy})
}

func (e *apiserver) DeleteCronJob(ctx context.Context, namespace, jobName string) error {
	return e.kibble.BatchV1().CronJobs(namespace).Delete(ctx, jobName, metav1.DeleteOptions{PropagationPolicy: &deletePolicy})
}

func (e *apiserver) GetOnceJob(ctx context.Context, namespace, jobId string) (*v1.Job, error) {
	job, err := e.kibble.BatchV1().Jobs(namespace).Get(ctx, jobId, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return job, nil
}

func (e *apiserver) DescribePod(ctx context.Context, namespace, podId string) (*corev1.Pod, error) {
	pod, err := e.kibble.CoreV1().Pods(namespace).Get(ctx, podId, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return pod, nil
}

func (e *apiserver) CreateNamespace(ctx context.Context, ns string) error {
	nsResp, _ := e.kibble.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
	if nsResp != nil && nsResp.Name == ns {
		return nil
	}
	res, err := e.kibble.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	if res.Name != ns {
		return fmt.Errorf("create namespace: %s but return: %s", ns, res.Name)
	}
	return nil
}

func (e *apiserver) GetLogs(ctx context.Context, namespace, podName string) (io.ReadCloser, error) {
	recoil := e.kibble.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{Follow: true})
	return recoil.Stream(ctx)
}

func (e *apiserver) BatchV1() batchv1.BatchV1Interface {
	return e.kibble.BatchV1()
}

func (e *apiserver) CoreV1() kcorev1.CoreV1Interface {
	return e.kibble.CoreV1()
}
