package specjob

import (
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
)

// TODO 自定义控制器

type Controller struct {
	kubeCli  kubernetes.Interface
	worker   workqueue.RateLimitingInterface
	recorder record.EventRecorder
	cache    cache.InformerSynced
}
