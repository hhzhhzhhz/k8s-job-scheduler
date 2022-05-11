package infrastructure

import (
	"fmt"
	"github.com/nsqio/go-nsq"
	"testing"
	"time"
)

func Test_delaytmq(t *testing.T) {
	url := "125.124.225.87:4150"
	producer, err := nsq.NewProducer(url, nsq.NewConfig())
	if err != nil {
		panic(err)
	}
	for i := 0; i < 1; i++ {
		err = producer.DeferredPublish("status_job_topic", time.Duration(i)*time.Second, []byte("hello world"))
		if err != nil {
			panic(err)
		}
	}

	producer.Stop()

}

func Test_pull(t *testing.T) {
	url := "125.124.225.87:4161"

	//defer waiter.Done()
	config := nsq.NewConfig()
	config.LookupdPollInterval = 20 * time.Second
	//config.MaxInFlight=9

	consumer, err := nsq.NewConsumer("status_job_topic", "struggle", config)
	if nil != err {
		fmt.Println("err", err)
		return
	}
	consumer.AddHandler(&NSQHandler{})
	//
	err = consumer.ConnectToNSQLookupd(url)
	if nil != err {
		fmt.Println("err", err)
		return
	}
	time.Sleep(10 * time.Minute)
}

type NSQHandler struct {
	count int
}

func (this *NSQHandler) HandleMessage(msg *nsq.Message) error {
	this.count++
	fmt.Println("receive", this.count, msg.NSQDAddress, "message:", string(msg.Body))
	return nil
}
