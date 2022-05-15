package config

import (
	"flag"
	"fmt"
	"github.com/magiconair/properties"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"os"
	"path/filepath"
	"time"
)

type Env string

const (
	dev Env = "dev"
)

type Config struct {
	Env                string        `properties:"env,default=dev"`
	PprofPort          string        `properties:"pprof_port,default=:7424"`
	ApiPort            string        `properties:"api_port,default=:65532"`
	Dsn                string        `properties:"dsn,default=root:1234567@tcp(127.0.0.1:3306)/job_scheduling?charset=utf8&parseTime=True&loc=%s&readTimeout=10s&timeout=30s"`
	AmqpUrl            string        `properties:"amqp_url,default=amqp://guest:guest@127.0.0.1:5672/"`
	JobPushTopic       string        `properties:"job_push_topic,default=job_push_topic"`
	JobStatusTopic     string        `properties:"job_status_topic,default=job_status_topic"`
	JobOperateTopic    string        `properties:"job_operate_topic,default=job_operate_topic"`
	ApiTimeoutSecond   time.Duration `properties:"api_timeout_second,default=60s"`
	LeaseLockNamespace string        `properties:"lease_lock_namespace,default=job"`
	OperateMaxTimeout  time.Duration `properties:"operate_max_timeout,default=15s"`
	Client             Client        `properties:"client"`
}

type Client struct {
	ID                 string `properties:"id,default=first_id"`
	LeaseLockNamespace string `properties:"lease_lock_namespace,default=jobs"`
	EventNamespace     string `properties:"event_namespace,default="`
}

var (
	cfg  *Config
	kube *rest.Config
)

func GetCfg() *Config {
	return cfg
}

func GetKubeCfg() *rest.Config {
	return kube
}

func Init() error {
	var configure string
	var kubeConfig string
	flag.StringVar(&configure, "configure", "D:\\Go\\src\\github.com\\hhzhhzhhz\\k8s-job-scheduler\\etc\\config.properties", "configure file for k8s-job-scheduler")
	flag.StringVar(&kubeConfig, "kubeconfig", "", "configure file for kubernetes ApiServer")
	flag.Parse()
	p := properties.MustLoadFile(configure, properties.UTF8)
	config := &Config{}
	if err := p.Decode(config); err != nil {
		return fmt.Errorf("Server.Init Config.Decode failed cause=%s", err.Error())
	}
	cfg = config
	var kluge *rest.Config
	var err error
	if kubeConfig == "" {
		kluge, err = rest.InClusterConfig()
		if err != nil {
			kubeConfig = filepath.Join(os.Getenv("HOME"), "/etc", "config")
			kluge, err = clientcmd.BuildConfigFromFlags("", kubeConfig)
			if err != nil {
				return fmt.Errorf("Server.Init BuildConfigFromFlags kluge failed cause=%s", err.Error())
			}
		}
	} else {
		kluge, err = clientcmd.BuildConfigFromFlags("", kubeConfig)
		if err != nil {
			return fmt.Errorf("Server.Init BuildConfigFromFlags kluge failed cause=%s", err.Error())
		}
	}
	kube = kluge
	return err
}
