package kubernetes

import (
	"context"
	"fmt"
	"github.com/hhzhhzhhz/k8s-job-scheduler/election"
	"github.com/hhzhhzhhz/k8s-job-scheduler/log"
	"io/ioutil"
	v1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"testing"
	"time"
)

var cfg *rest.Config

func init() {
	var err error
	cfg, err = clientcmd.BuildConfigFromFlags("", "D:\\Go\\src\\github.com\\hhzhhzhhz\\k8s-job-scheduler\\etc\\config")
	if err != nil {
		panic(err)
	}
	if err := InitKubeClient(cfg); err != nil {
		panic(err)
	}
}

func Test_K8sJob(t *testing.T) {
	t.Run("createJob", func(t *testing.T) {
		ttl := int32(600)
		active := int64(600)
		jb := &v1.Job{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Job",
				APIVersion: "batch/v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				//GenerateName: fmt.Sprintf("%s-", "test-jobs3"),
				Name:      "k8s-job-434",
				Namespace: "default",
			},
			Spec: v1.JobSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						RestartPolicy: corev1.RestartPolicyNever,
						Containers: []corev1.Container{
							{
								Name:    "k8s-job-434",
								Image:   "perl",
								Command: []string{"tail", "-f", "/var/log/alternatives.log"},
							},
						},
					},
				},
				TTLSecondsAfterFinished: &ttl,
				ActiveDeadlineSeconds:   &active,
			},
		}
		//cron := &v1.CronJob{}
		j, err := KubernetesClient().BatchV1().Jobs("default").Create(context.TODO(), jb, metav1.CreateOptions{})
		if err != nil {
			t.Error(err)
		}
		t.Log(j)
		time.Sleep(20 * time.Second)
		//j.Labels["update"] = "update"
		j.Spec.Template.Spec.Containers = []corev1.Container{
			{
				Name:    "k8s-job-434",
				Image:   "perl",
				Command: []string{"echo ", "hello world!!!"},
			},
		}
		i := true
		j.Spec.Suspend = &i
		KubernetesClient().BatchV1().Jobs("default").Update(context.TODO(), j, metav1.UpdateOptions{DryRun: []string{"All"}})
		//KubernetesClient().BatchV1().Jobs().Update()
		//t.Log(cron)
		t.Log(j)
	})

	t.Run("updateJob", func(t *testing.T) {
		ttl := int32(3000)
		active := int64(60)
		jb := &v1.Job{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Job",
				APIVersion: "batch/v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: fmt.Sprintf("%s-xxx", "test-jobs"),
				Name:         "test-jobs",
				Namespace:    "default",
				Labels:       map[string]string{"controller-uid": "6e541f7e-5538-4f35-8a8f-d18d0b7720da", "job-name": "test-jobs"},
			},
			Spec: v1.JobSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						RestartPolicy: corev1.RestartPolicyNever,
						Containers: []corev1.Container{
							{
								Name:    "test-jobs",
								Image:   "perl",
								Command: []string{"awk", "BEGIN{for(i=1; i<=10; i++) print i}"},
							},
						},
					},
				},
				TTLSecondsAfterFinished: &ttl,
				ActiveDeadlineSeconds:   &active,
				Selector: &metav1.LabelSelector{
					MatchLabels:      map[string]string{},
					MatchExpressions: []metav1.LabelSelectorRequirement{},
				},
			},
		}
		//cron := &v1.CronJob{}
		j, err := KubernetesClient().BatchV1().Jobs("default").Update(context.TODO(), jb, metav1.UpdateOptions{})
		if err != nil {
			t.Error(err)
		}
		//KubernetesClient().BatchV1().Jobs().Update()
		//t.Log(cron)
		t.Log(j)

	})

	t.Run("deleteJob", func(t *testing.T) {
		dp := metav1.DeletePropagationBackground
		err := KubernetesClient().BatchV1().Jobs("default").Delete(context.TODO(), "myjob-2vp9j", metav1.DeleteOptions{PropagationPolicy: &dp})
		if err != nil {
			t.Error(err.Error())
		}
	})

	t.Run("GetOnceJobByJobId", func(t *testing.T) {
		res, err := KubernetesClient().BatchV1().Jobs("default").Get(context.TODO(), "test-jobs2", metav1.GetOptions{})
		if err != nil {
			t.Error(err.Error())
		}
		t.Log(res)

		//KubernetesClient().CoreV1().Events()
	})

	t.Run("create_namespaces", func(t *testing.T) {
		name := "hhz"
		exist, err := KubernetesClient().CoreV1().Namespaces().Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			t.Log(err.Error())
		}
		if exist.Name == name {
			t.Logf("namespaces %s exist", name)
			return
		}
		res, err := KubernetesClient().CoreV1().Namespaces().Create(context.TODO(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}, metav1.CreateOptions{})
		if err != nil {
			t.Error(err.Error())
		}
		t.Log(res)

	})
	t.Run("pod-logs", func(t *testing.T) {
		pod_id := "test-jobs-c7m85"
		logs := KubernetesClient().CoreV1().Pods("default").GetLogs(pod_id, &corev1.PodLogOptions{})
		b, err := logs.Do(context.TODO()).Raw()
		if err != nil {
			t.Error(err.Error())
		}
		t.Log(string(b))
	})
}

func Test_K8sCrontroller(t *testing.T) {

	watch, err := KubernetesClient().BatchV1().Jobs("job").Watch(context.Background(), metav1.ListOptions{Watch: true, AllowWatchBookmarks: true})
	if err != nil {
		t.Error(err.Error())
	}
	for {
		select {
		case event, ok := <-watch.ResultChan():
			if !ok {
				t.Log(event)
				watch, err = KubernetesClient().BatchV1().Jobs("job").Watch(context.Background(), metav1.ListOptions{Watch: true})
				if err != nil {
					t.Error(err.Error())
					return
				}
			}
			t.Logf("action: %s, body: [%v]", event.Type, event.Object)
		}
	}
}

func Test_k8s_eve(t *testing.T) {

	watch, err := KubernetesClient().CoreV1().Events("default").Watch(context.Background(), metav1.ListOptions{Watch: true})
	if err != nil {
		t.Error(err.Error())
	}
	for {
		select {
		case event, ok := <-watch.ResultChan():
			if !ok {
				t.Log(event)
				watch, err = KubernetesClient().CoreV1().Events("default").Watch(context.Background(), metav1.ListOptions{Watch: true})
				if err != nil {
					t.Error(err.Error())
					return
				}
			}
			t.Logf("action: %s, body: [%v]", event.Type, event.Object)
		}
	}
}

func Test_EVENT_Once_job(t *testing.T) {

	watchlist := cache.NewListWatchFromClient(KubernetesClient().BatchV1().RESTClient(), "Jobs", "",
		fields.Everything())

	_, cron := cache.NewInformer(
		watchlist,
		&v1.Job{},
		time.Second*0,
		createResourceEventHandler(),
	)
	ch := make(chan struct{}, 1)
	cron.Run(ch)

	<-ch
}

func createResourceEventHandler() cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			ss := obj.(*v1.Job)
			log.Logger().Info("%+v", ss)
		},
		DeleteFunc: func(obj interface{}) {
			ss := obj.(*v1.Job)
			log.Logger().Info("%+v", ss)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			ss := newObj.(*v1.Job)
			log.Logger().Info("%+v", ss)
		},
	}
}

func Test_EVENT_Cron_job(t *testing.T) {

	watchlist := cache.NewListWatchFromClient(KubernetesClient().BatchV1().RESTClient(), "Cronjobs", "",
		fields.Everything())

	_, cron := cache.NewInformer(
		watchlist,
		&v1.CronJob{},
		time.Second*0,
		createResourceEventHandlerCron(),
	)
	ch := make(chan struct{}, 1)
	cron.Run(ch)

	<-ch
}

func createResourceEventHandlerCron() cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			ss := obj.(*v1.CronJob)
			log.Logger().Info("%+v", ss)
		},
		DeleteFunc: func(obj interface{}) {
			ss := obj.(*v1.CronJob)
			log.Logger().Info("%+v", ss)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			ss := newObj.(*v1.CronJob)
			log.Logger().Info("%+v", ss)
		},
	}
}

func Test_EVENT_Core(t *testing.T) {

	watchlist := cache.NewListWatchFromClient(KubernetesClient().CoreV1().RESTClient(), "pods", "",
		fields.Everything())

	_, cron := cache.NewInformer(
		watchlist,
		&corev1.Pod{},
		time.Second*0,
		createResourceEventHandlerCore(),
	)
	ch := make(chan struct{}, 1)
	cron.Run(ch)

	<-ch
}

func createResourceEventHandlerCore() cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			ss := obj.(*corev1.Pod)
			log.Logger().Info("%+v", ss)
		},
		DeleteFunc: func(obj interface{}) {
			ss := obj.(*corev1.Pod)
			log.Logger().Info("%+v", ss)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			ss := newObj.(*corev1.Pod)
			log.Logger().Info("%+v", ss)
		},
	}
}

func Test_JobName_GET_POD_Name(t *testing.T) {
	name := "k8s-job-jobxxxxx131"
	namespace := "default"
	kind := "Job"
	uid := "375af3e2-5006-48f7-9890-febf51152978"
	fieldSelector := KubernetesClient().CoreV1().Events("default").GetFieldSelector(&name, &namespace, &kind, &uid)
	initialOpts := metav1.ListOptions{FieldSelector: fieldSelector.String()}
	ress, err := KubernetesClient().CoreV1().Events("default").List(context.TODO(), initialOpts)
	if err != nil {
		t.Error(err.Error())
	}
	t.Log(ress)
}

func Test_log(t *testing.T) {
	re := KubernetesClient().CoreV1().Pods("default").GetLogs("k8s-job-xewrewwrw125-rjvcm", &corev1.PodLogOptions{Follow: true})
	r, err := re.Stream(context.TODO())
	//watch, err := re.Watch(context.TODO())
	if err != nil {
		t.Error(err.Error())
		return
	}
	b, err := ioutil.ReadAll(r)
	if err != nil {
		t.Error(err.Error())
		return
	}
	t.Log(string(b))
	//for {
	//	select {
	//	case event, ok := <-watch.ResultChan():
	//		if !ok {
	//			t.Log(event)
	//			return
	//		}
	//		t.Logf("action: %s, body: [%v]", event.Type, event.Object)
	//	}
	//}
}

var (
	namespace = "default"
)

func Test_DistributedLock(t *testing.T) {

	t.Run("lock1", func(t *testing.T) {
		e := election.NewElection(context.TODO(), "", KubernetesClient(), namespace)
		e.Run()
		e.Register(election.Leader, func() {
			t.Log("im leader")
		})
		e.Register(election.Follower, func() {
			t.Log("leader lost")
		})
		for {
			time.Sleep(2 * time.Second)
		}
	})
	t.Run("lock2", func(t *testing.T) {
		e := election.NewElection(context.TODO(), "", KubernetesClient(), namespace)
		e.Run()
		e.Register(election.Leader, func() {
			t.Log("im leader")
		})
		e.Register(election.Follower, func() {
			t.Log("leader lost")
		})
		for {
			time.Sleep(2 * time.Second)
		}
	})
	t.Run("lock3", func(t *testing.T) {
		e := election.NewElection(context.TODO(), "", KubernetesClient(), namespace)
		e.Run()
		e.Register(election.Leader, func() {
			t.Log("im leader")
		})
		e.Register(election.Follower, func() {
			t.Log("leader lost")
		})
		for {
			time.Sleep(2 * time.Second)
		}
	})

}

func Test_Cron_Job(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		ttl := int32(600)
		active := int64(600)
		jb := &v1.CronJob{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Job",
				APIVersion: "batch/v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				//GenerateName: fmt.Sprintf("%s-", "test-jobs3"),
				Name:      "k8s-job-434",
				Namespace: "default",
			},
			Spec: v1.CronJobSpec{
				JobTemplate: v1.JobTemplateSpec{
					Spec: v1.JobSpec{
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{
								RestartPolicy: corev1.RestartPolicyNever,
								Containers: []corev1.Container{
									{
										Name:    "k8s-job-434",
										Image:   "perl",
										Command: []string{"echo", "/var/log/alternatives.log"},
									},
								},
							},
						},
						TTLSecondsAfterFinished: &ttl,
						ActiveDeadlineSeconds:   &active,
					},
				},
				Schedule: "*/1 * * * *",
			},
		}
		//cron := &v1.CronJob{}
		j, err := KubernetesClient().BatchV1().CronJobs("default").Create(context.TODO(), jb, metav1.CreateOptions{})
		if err != nil {
			t.Error(err)
		}
		t.Log(j)
	})
}
