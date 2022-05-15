package rabbitmq

import (
	"fmt"
	"github.com/hhzhhzhhz/k8s-job-scheduler/pkg/utils"
	"github.com/streadway/amqp"
	"go.uber.org/multierr"
	"sync"
	"time"
)

var (
	defaultRotationTime = 30 * time.Second
	defaultRetry        = 3
)

type ExchangeType string

const (
	Topic           ExchangeType = "topic"             // 普通队列 高级路由单播、广播
	Fanout          ExchangeType = "fanout"            // 普通队列 无路由单播、广播
	XDelayedMessage ExchangeType = "x-delayed-message" // 延迟队列 高级路由单播、广播
)

type handler func(deliveries <-chan amqp.Delivery)

var (
	delay  DelayQueue
	common CommonQueue
)

func NewHandlerWrapper(f func(msg *amqp.Delivery) error) *handlerWrapper {
	return &handlerWrapper{fun: f}
}

type handlerWrapper struct {
	fun func(msg *amqp.Delivery) error
}

func (h *handlerWrapper) HandlerWrap(deliveries <-chan amqp.Delivery) {
	for msg := range deliveries {
		if err := h.fun(&msg); err == nil {
			msg.Ack(false)
		}
	}
}

func InitRabbitMq(url string) error {
	delay = NewDelayQueue(url)
	common = NewCommonQueue(url)
	return nil
}

func GetDelayQueue() DelayQueue {
	return delay
}

func GetCommonQueue() CommonQueue {
	return common
}

type DelayQueue interface {
	Publish(topic, router string, delay int64, message interface{}) error
	Subscription(topic, id string, match []string, h handler) error
	UnSubscription(topic string)
	Close() error
}

func NewDelayQueue(url string) DelayQueue {
	return &delayQueue{
		url:       url,
		producers: map[string]*DelayTopicProducer{},
		consumers: map[string][]*DelayTopicConsumer{},
	}
}

type delayQueue struct {
	url       string
	mux       sync.Mutex
	producers map[string]*DelayTopicProducer
	consumers map[string][]*DelayTopicConsumer
}

func (d *delayQueue) Publish(topic, router string, delay int64, message interface{}) error {
	d.mux.Lock()
	defer d.mux.Unlock()
	p, ok := d.producers[topic]
	if ok {
		return p.Publish(router, delay, message)
	}
	p = NewDelayTopicProducer(d.url, topic)
	d.producers[topic] = p
	return p.Publish(router, delay, message)
}

func (d *delayQueue) Subscription(topic, id string, match []string, h handler) error {
	d.mux.Lock()
	defer d.mux.Unlock()
	cs, ok := d.consumers[topic]
	if ok {
		c, err := NewDelayTopicConsumer(d.url, topic, id, match, h)
		if err != nil {
			return err
		}
		cs = append(cs, c)
		d.consumers[topic] = cs
		return nil
	}
	c, err := NewDelayTopicConsumer(d.url, topic, id, match, h)
	if err != nil {
		return err
	}
	d.consumers[topic] = []*DelayTopicConsumer{c}
	return nil
}

func (d *delayQueue) UnSubscription(topic string) {
	d.mux.Lock()
	defer d.mux.Unlock()
	var dels []string
	for top, cs := range d.consumers {
		if top == topic {
			for _, c := range cs {
				c.Close()
			}
			dels = append(dels, top)
		}
	}
	for _, del := range dels {
		delete(d.consumers, del)
	}
}

func (d *delayQueue) Close() error {
	d.mux.Lock()
	defer d.mux.Unlock()
	var errs error
	for _, cs := range d.consumers {
		for _, c := range cs {
			errs = multierr.Append(errs, c.Close())
		}
	}
	for _, p := range d.producers {
		errs = multierr.Append(errs, p.close())
	}
	return errs
}

type CommonQueue interface {
	Publish(topic string, message interface{}) error
	RetryPublish(topic string, message interface{})
	Subscription(topic, id string, h handler) error
	UnSubscription(topic string)
	Close() error
}

func NewCommonQueue(url string) CommonQueue {
	return &commonQueue{
		url:       url,
		producers: map[string]*FanoutProducer{},
		consumers: map[string]*FanoutConsumer{},
	}
}

type commonQueue struct {
	url       string
	mux       sync.Mutex
	producers map[string]*FanoutProducer
	consumers map[string]*FanoutConsumer
}

func (c *commonQueue) Publish(topic string, message interface{}) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	p, ok := c.producers[topic]
	if ok {
		return p.Publish(topic, message)
	}
	p = NewFanoutProducer(c.url, topic)
	c.producers[topic] = p
	return p.Publish(topic, message)
}

func (c *commonQueue) RetryPublish(topic string, message interface{}) {
	utils.Retry(defaultRetry, func() error {
		return c.Publish(topic, message)
	})
}

func (c *commonQueue) Subscription(topic, id string, h handler) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	_, ok := c.consumers[topic]
	if ok {
		return fmt.Errorf("topic=%s already exists", topic)
	}
	f, err := NewFanoutConsumer(c.url, topic, id, h)
	if err != nil {
		return err
	}
	c.consumers[topic] = f
	return nil
}

func (c *commonQueue) UnSubscription(topic string) {
	c.mux.Lock()
	defer c.mux.Unlock()
	var dels []string
	for top, c := range c.consumers {
		if c.topic == topic {
			c.Close()
			dels = append(dels, top)
		}
	}
	for _, del := range dels {
		delete(c.consumers, del)
	}
}

func (c *commonQueue) Close() error {
	c.mux.Lock()
	defer c.mux.Unlock()
	var errs error
	for _, c := range c.consumers {
		errs = multierr.Append(errs, c.Close())
	}
	for _, c := range c.producers {
		errs = multierr.Append(errs, c.close())
	}
	return errs
}

func Close() error {
	var errs error
	if delay != nil {
		errs = multierr.Append(errs, delay.Close())
	}
	if common != nil {
		errs = multierr.Append(errs, common.Close())
	}
	return errs
}
