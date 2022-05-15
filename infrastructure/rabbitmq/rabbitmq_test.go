package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"testing"
	"time"
)

var url = "amqp://guest:guest@127.0.0.1:5672/"

func Test_DelayTopic(t *testing.T) {
	var topic = "delay_exchange_10"
	t.Run("delay_topic_1", func(t *testing.T) {
		p := NewDelayTopicProducer(url, topic)
		for i := 0; i < 3; i++ {
			if err := p.Publish("all.hhz", 0, []byte(fmt.Sprintf("hello world %d topic !!!", i))); err != nil {
				t.Log(err.Error())
			}
		}
	})
	t.Run("delay_consumer_1", func(t *testing.T) {
		_, err := NewDelayTopicConsumer(url, topic, "id1", []string{"cluster1.*", "all"}, func(deliveries <-chan amqp.Delivery) {
			for v := range deliveries {
				t.Log(string(v.Body))
				v.Ack(false)
			}

		})
		if err != nil {
			t.Error(err.Error())
			return
		}
		ch := make(chan struct{}, 0)
		ch <- struct{}{}
	})

	t.Run("delay_consumer_2", func(t *testing.T) {
		_, err := NewDelayTopicConsumer(url, topic, "id1", []string{"cluster2.*", "all.*"}, func(deliveries <-chan amqp.Delivery) {
			for v := range deliveries {
				t.Log(string(v.Body))
				v.Ack(false)
			}
		})
		if err != nil {
			t.Error(err.Error())
			return
		}
		ch := make(chan struct{}, 0)
		ch <- struct{}{}
	})
}

func Test_fanout(t *testing.T) {
	tp := "fanout_4"
	t.Run("Publish", func(t *testing.T) {
		f := NewFanoutProducer(url, tp)
		for i := 0; i < 10; i++ {
			f.Publish(tp, []byte(fmt.Sprintf("hello world %d fanout !!!", i)))
		}
	})
	t.Run("consumer_1", func(t *testing.T) {
		f, err := NewFanoutConsumer(url, tp, "id", func(deliveries <-chan amqp.Delivery) {
			for d := range deliveries {
				log.Printf("Received a message: %s", d.Body)
				d.Ack(false)
			}
		})
		if err != nil {
			t.Error(err.Error())
			return
		}
		if err != nil {
			t.Error(err.Error())
			return
		}
		time.Sleep(20 * time.Second)
		t.Log("close")
		if err := f.conn.Close(); err != nil {
			t.Error(err.Error())
		}
		ch := make(chan struct{}, 0)
		ch <- struct{}{}
	})

	t.Run("consumer_2", func(t *testing.T) {
		_, err := NewFanoutConsumer(url, tp, "id2", func(deliveries <-chan amqp.Delivery) {
			for d := range deliveries {
				log.Printf("Received a message: %s", d.Body)
				d.Ack(false)
			}
		})
		if err != nil {
			t.Error(err.Error())
			return
		}
		ch := make(chan struct{}, 0)
		ch <- struct{}{}
	})
}
