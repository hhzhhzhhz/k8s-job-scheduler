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

type FanoutProducer struct {
	url   string
	topic string
	conn  *amqp.Connection
	ch    *amqp.Channel
}

func NewFanoutProducer(url, topic string) *FanoutProducer {
	return &FanoutProducer{url: url, topic: topic}
}

func (f *FanoutProducer) reconnect() error {
	var err error
	conn, err := amqp.Dial(f.url)
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("conn.Channel error=%s", err.Error())
	}
	if err = ch.ExchangeDeclare(f.topic, string(Fanout), true, false, false, false, nil); err != nil {
		return fmt.Errorf("ch.ExchangeDeclare error=%s", err.Error())
	}
	f.conn = conn
	f.ch = ch
	return nil
}

func (f *FanoutProducer) Publish(topic string, message interface{}) error {
	if topic != f.topic {
		return fmt.Errorf("topic is not the same param=%s current=%s", topic, f.topic)
	}
	b, err := json.Marshal(message)
	if err != nil {
		return err
	}
	if f.conn == nil || f.conn.IsClosed() {
		if err := f.reconnect(); err != nil {
			return err
		}
	}
	return f.ch.Publish(topic, "", false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Body:         b,
	})
}

func (f *FanoutProducer) close() error {
	var errs error
	if f.ch != nil {
		errs = multierr.Append(errs, f.ch.Close())
	}
	if f.conn != nil {
		errs = multierr.Append(errs, f.conn.Close())
	}
	return errs
}

type FanoutConsumer struct {
	ctx     context.Context
	cancel  context.CancelFunc
	url     string
	topic   string
	id      string
	conn    *amqp.Connection
	ch      *amqp.Channel
	handler handler
}

func NewFanoutConsumer(url, topic, id string, h handler) (*FanoutConsumer, error) {
	if id == "" {
		return nil, fmt.Errorf("id is empty")
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &FanoutConsumer{
		ctx:     ctx,
		cancel:  cancel,
		url:     url,
		topic:   topic,
		id:      id,
		handler: h,
	}
	if err := c.connect(); err != nil {
		return c, err
	}
	utils.Wrap(func() {
		c.loop()
	})
	return c, nil
}

func (f *FanoutConsumer) connect() error {
	conn, err := amqp.Dial(f.url)
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	if err := ch.ExchangeDeclare(f.topic, string(Fanout), true, false, false, false, nil); err != nil {
		return err
	}
	q, err := ch.QueueDeclare(f.id, true, false, false, false, nil)
	if err != nil {
		return err
	}
	if err := ch.QueueBind(q.Name, "", f.topic, false, nil); err != nil {
		return err
	}
	megs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("ch.Consume failed cause=%s", err.Error())
	}
	f.conn = conn
	f.ch = ch
	utils.Wrap(func() {
		f.handler(megs)
	})
	return nil
}

func (f *FanoutConsumer) loop() {
	tick := time.Tick(defaultRotationTime)
	for {
		select {
		case <-tick:
			if f.conn.IsClosed() {
				f.close()
				if err := f.connect(); err != nil {
					log.Logger().Warn("FanoutConsumer.Loop reconnect failed cuase=%s", err.Error())
				}
				log.Logger().Info("FanoutConsumer.Loop reconnect success")
			}
		case <-f.ctx.Done():
			return
		}
	}
}

func (f *FanoutConsumer) close() error {
	var errs error
	if f.ch != nil {
		errs = multierr.Append(errs, f.ch.Close())
	}
	if f.conn != nil {
		errs = multierr.Append(errs, f.conn.Close())
	}
	return errs
}

func (f *FanoutConsumer) Close() error {
	f.cancel()
	return f.close()
}
