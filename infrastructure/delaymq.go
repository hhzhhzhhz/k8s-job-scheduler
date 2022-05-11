// Package infrastructure currently no free and reliable delay queue and the paid version of rocketmq can be selected conditionally
package infrastructure

import (
	"fmt"
	"github.com/hhzhhzhhz/k8s-job-scheduler/entity"
	"github.com/hhzhhzhhz/k8s-job-scheduler/log"
	"github.com/hhzhhzhhz/k8s-job-scheduler/pkg/utils"
	json "github.com/json-iterator/go"
	"github.com/nsqio/go-nsq"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const (
	nsqInterval  = 30
	nsqNodesApi  = "http://%s/nodes"
	defaultRetry = 3
)

var (
	mqt Delaymq
)

type MqProcess func(message *nsq.Message) error

type Delaymq interface {
	RetryPublish(topic string, message interface{})
	DeferredPublish(topic string, delay time.Duration, message interface{}) error
	Publish(topic string, message interface{}) error
	Subscription(topic, channel string, handler nsq.Handler) error
	UnSubscription(topic string)
	Close() error
}

func NewEmptyHandler(p MqProcess) *emptyHandler {
	return &emptyHandler{f: p}
}

type emptyHandler struct {
	f MqProcess
}

func (e *emptyHandler) HandleMessage(message *nsq.Message) error {
	return e.f(message)
}

func GetDelayMq() Delaymq {
	return mqt
}

type Consumers struct {
	topic string
	c     *nsq.Consumer
}

type delaymq struct {
	loopaddr  string
	mux       sync.Mutex
	topic     string
	producer  *nsq.Producer
	consumers []*Consumers
	nsqdAddrs []string
}

func InitDelaymq(loopaddr string) error {
	dq := &delaymq{
		loopaddr: loopaddr,
	}
	if err := dq.nsqdParse(loopaddr); err != nil {
		return err
	}
	if err := dq.initproducer(); err != nil {
		return err
	}
	mqt = dq
	return nil
}

func (d *delaymq) initproducer() error {
	p, err := nsq.NewProducer(d.nsqdAddrs[0], nsq.NewConfig())
	if err != nil {
		return err
	}
	p.SetLoggerLevel(nsq.LogLevelError)
	d.producer = p
	return nil
}

// todo build connect pool
func (d *delaymq) nsqdParse(nsloopup string) error {
	addr := fmt.Sprintf(nsqNodesApi, nsloopup)
	resp, err := http.Get(addr)
	if err != nil {
		return err
	}
	defer func() {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
	}()
	endpoint, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	lupresp := &entity.LookupResp{}
	if err := json.Unmarshal(endpoint, lupresp); err != nil {
		return err
	}
	var nsqdAddrs []string
	for _, producer := range lupresp.Producers {
		broadcastAddress := producer.BroadcastAddress
		port := producer.TCPPort
		joined := net.JoinHostPort(broadcastAddress, strconv.Itoa(port))
		nsqdAddrs = append(nsqdAddrs, joined)
	}
	if len(nsqdAddrs) == 0 {
		return fmt.Errorf("nsqd addrs is empty")
	}
	d.mux.Lock()
	d.nsqdAddrs = nsqdAddrs
	d.mux.Unlock()
	return nil
}

// DeferredPublish Publish send message to delay mq
func (d *delaymq) DeferredPublish(topic string, delay time.Duration, message interface{}) error {
	d.mux.Lock()
	defer d.mux.Unlock()
	var b []byte
	b, ok := message.([]byte)
	if !ok {
		var err error
		b, err = json.Marshal(message)
		if err != nil {
			return fmt.Errorf("json.marshal failed cause: %s", err.Error())
		}
	}

	return d.producer.DeferredPublish(topic, delay, b)
}

func (d *delaymq) Publish(topic string, message interface{}) error {
	d.mux.Lock()
	defer d.mux.Unlock()
	var b []byte
	b, ok := message.([]byte)
	if !ok {
		var err error
		b, err = json.Marshal(message)
		if err != nil {
			return fmt.Errorf("json.marshal failed cause: %s", err.Error())
		}
	}
	return d.producer.Publish(topic, b)
}

func (d *delaymq) RetryPublish(topic string, message interface{}) {
	utils.Retry(defaultRetry, func() error {
		return d.publish(topic, message)
	})
}

func (d *delaymq) publish(topic string, message interface{}) error {
	d.mux.Lock()
	defer d.mux.Unlock()
	var b []byte
	b, ok := message.([]byte)
	if !ok {
		var err error
		b, err = json.Marshal(message)
		if err != nil {
			log.Logger().Error("locate=%s Publish failed message=[%+v] cause=%s", utils.Caller(4), message, err.Error())
			return err
		}
	}
	if err := d.producer.Publish(topic, b); err != nil {
		log.Logger().Error("locate=%s Publish failed message=[%+v] cause=%s", utils.Caller(4), message, err.Error())
		return err
	}
	return nil
}

func (d *delaymq) Subscription(topic, channel string, handler nsq.Handler) error {
	if channel == "" {
		channel = topic
	}
	cfg := nsq.NewConfig()
	cfg.LookupdPollInterval = nsqInterval * time.Second
	c, err := nsq.NewConsumer(topic, channel, cfg)
	if err != nil {
		return err
	}
	c.SetLoggerLevel(nsq.LogLevelError)
	c.AddHandler(handler)
	err = c.ConnectToNSQLookupd(d.loopaddr)
	if err != nil {
		return err
	}

	d.consumers = append(d.consumers, &Consumers{topic: topic, c: c})
	return nil
}

func (d *delaymq) UnSubscription(topic string) {
	for _, v := range d.consumers {
		if topic == v.topic {
			v.c.Stop()
		}
	}
}

func (d *delaymq) Close() error {
	d.mux.Lock()
	defer d.mux.Unlock()
	var errs error
	if d.producer != nil {
		d.producer.Stop()
	}
	for _, c := range d.consumers {
		c.c.Stop()
	}
	return errs
}
