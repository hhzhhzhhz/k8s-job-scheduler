package kubernetes

import (
	kubeclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var (
	//mux                       sync.Mutex
	kubecli kubeclient.Interface = nil
)

func InitKubeClient(cfg *rest.Config) error {
	//mux.Lock()
	//defer mux.Unlock()
	var err error
	kubecli, err = kubeclient.NewForConfig(cfg)
	return err
}

func KubernetesClient() kubeclient.Interface {
	//mux.Lock()
	//defer mux.Unlock()
	return kubecli
}
