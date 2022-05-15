package rabbitmq

import (
	"context"
	"fmt"
	"github.com/hhzhhzhhz/k8s-job-scheduler/log"
	"github.com/hhzhhzhhz/k8s-job-scheduler/pkg/utils"
	json "github.com/json-iterator/go"
	"github.com/streadway/amqp"
	"go.uber.org/multierr"
	"time"
)

var (
	tableHeader = "x-delayed-type"
	xDelay      = "x-delay"
)

func NewDelayTopicProducer(url, topic string) *DelayTopicProducer {
	return &DelayTopicProducer{url: url, topic: topic}
}

type DelayTopicProducer struct {
	url   string
	topic string
	ch    *amqp.Channel
	conn  *amqp.Connection
}

// Publish router: cluster_2.info
func (c *DelayTopicProducer) Publish(router string, delay int64, message interface{}) error {
	if c.conn == nil || c.conn.IsClosed() {
		if err := c.connect(); err != nil {
			return err
		}
	}
	b, err := json.Marshal(message)
	if err != nil {
		return err
	}
	publish := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Body:         b,
	}
	if delay > 0 {
		publish.Headers = amqp.Table{xDelay: delay}
	}
	return c.ch.Publish(c.topic, router, false, false,
		publish,
	)
}

func (c *DelayTopicProducer) connect() error {
	conn, err := amqp.Dial(c.url)
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("conn.Channel error=%s", err.Error())
	}
	if err := ch.ExchangeDeclare(c.topic, string(XDelayedMessage), true, false, false, false, amqp.Table{tableHeader: string(Topic)}); err != nil {
		return fmt.Errorf("ch.ExchangeDeclare error=%s", err.Error())
	}
	c.conn = conn
	c.ch = ch
	return nil
}

func (c *DelayTopicProducer) close() error {
	var errs error
	if c.ch != nil {
		errs = multierr.Append(errs, c.ch.Close())
	}
	if c.conn != nil {
		errs = multierr.Append(errs, c.conn.Close())
	}
	return errs
}

type DelayTopicConsumer struct {
	ctx     context.Context
	cancel  context.CancelFunc
	url     string
	topic   string
	match   []string
	id      string
	handler handler
	ch      *amqp.Channel
	conn    *amqp.Connection
}

// NewDelayTopicConsumer matchs: cluster_2.*
func NewDelayTopicConsumer(url, topic, id string, match []string, h handler) (*DelayTopicConsumer, error) {
	if id == "" {
		return nil, fmt.Errorf("id is empty")
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &DelayTopicConsumer{
		ctx:     ctx,
		cancel:  cancel,
		url:     url,
		topic:   topic,
		match:   match,
		handler: h,
		id:      id,
	}
	if err := c.connect(); err != nil {
		return c, err
	}
	utils.Wrap(func() {
		c.loop()
	})
	return c, nil
}

func (d *DelayTopicConsumer) connect() error {
	conn, err := amqp.Dial(d.url)
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	if err := ch.ExchangeDeclare(d.topic, string(XDelayedMessage), true, false, false, false, amqp.Table{tableHeader: string(Topic)}); err != nil {
		return err
	}
	q, err := ch.QueueDeclare(d.id, true, false, false, false, nil)
	if err != nil {
		return err
	}
	for _, m := range d.match {
		if err := ch.QueueBind(q.Name, m, d.topic, false, nil); err != nil {
			return err
		}
	}
	megs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("ch.Consume failed cause=%s", err.Error())
	}
	d.conn = conn
	d.ch = ch
	utils.Wrap(func() {
		d.handler(megs)
	})
	return nil
}

func (d *DelayTopicConsumer) loop() {
	tick := time.Tick(defaultRotationTime)
	for {
		select {
		case <-tick:
			if d.conn.IsClosed() {
				d.close()
				if err := d.connect(); err != nil {
					log.Logger().Warn("DelayTopicConsumer.Loop reconnect failed cuase=%s", err.Error())
				}
				log.Logger().Info("DelayTopicConsumer.Loop reconnect success")
			}
		case <-d.ctx.Done():
			return
		}
	}
}

func (d *DelayTopicConsumer) close() error {
	var errs error
	if d.ch != nil {
		errs = multierr.Append(errs, d.ch.Close())
	}
	if d.conn != nil {
		errs = multierr.Append(errs, d.conn.Close())
	}
	return errs
}

func (d *DelayTopicConsumer) Close() error {
	d.cancel()
	return d.close()
}
