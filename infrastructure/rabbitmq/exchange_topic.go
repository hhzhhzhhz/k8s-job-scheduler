package rabbitmq

//type TopicProducer struct {
//	url   string
//	topic string
//	ch    *amqp.Channel
//	conn  *amqp.Connection
//}
//
//func NewTopicProducer(url, topic string) *TopicProducer {
//	return &TopicProducer{url: url, topic: topic}
//}
//
//func (c *TopicProducer) connect() error {
//	conn, err := amqp.Dial(c.url)
//	if err != nil {
//		return err
//	}
//	ch, err := conn.Channel()
//	if err != nil {
//		return fmt.Errorf("conn.Channel error=%s", err.Error())
//	}
//	if err := ch.ExchangeDeclare(c.topic, string(Topic), true, false, false, false, nil); err != nil {
//		return fmt.Errorf("ch.ExchangeDeclare error=%s", err.Error())
//	}
//	c.conn = conn
//	c.ch = ch
//	return nil
//}
//
//// Publish router: cluster_2.info
//func (c *TopicProducer) Publish(router string, msg []byte) error {
//	if c.conn == nil || c.conn.IsClosed() {
//		if err := c.connect(); err != nil {
//			return err
//		}
//	}
//	return c.ch.Publish(c.topic, router, true, false,
//		amqp.Publishing{
//			DeliveryMode: amqp.Persistent,
//			Body:         msg,
//		},
//	)
//}
//
//func (c *TopicProducer) close() error {
//	var errs error
//	if c.ch != nil {
//		errs = multierr.Append(errs, c.ch.Close())
//	}
//	if c.conn != nil {
//		errs = multierr.Append(errs, c.conn.Close())
//	}
//	return errs
//}
//
//type TopicConsumer struct {
//	ctx context.Context
//	cancel context.CancelFunc
//	url     string
//	topic  string
//	matchs []string
//	id     string
//	handler handler
//	ch      *amqp.Channel
//	conn    *amqp.Connection
//}
//
//// NewDelayTopicConsumer matchs: cluster_2.*
//func NewTopicConsumer(url, topic, id string, matchs []string,  h handler) (*TopicConsumer, error) {
//	if id == "" {
//		return nil, fmt.Errorf("id is empty")
//	}
//	ctx, cancel := context.WithCancel(context.Background())
//	c := &TopicConsumer{
//		ctx: ctx,
//		cancel: cancel,
//		url:     url,
//		topic:   topic,
//		matchs:  matchs,
//		handler: h,
//		id:      id,
//	}
//	if err := c.connect(); err != nil {
//		return c, err
//	}
//	utils.Wrap(func() {
//		c.loop()
//	})
//	return c, nil
//}
//
//func (c *TopicConsumer) connect() error {
//	conn, err := amqp.Dial(c.url)
//	if err != nil {
//		return err
//	}
//	ch, err := conn.Channel()
//	if err != nil {
//		return err
//	}
//	if err := ch.ExchangeDeclare(c.topic, string(Topic), true, false, false, false, nil); err != nil {
//		return err
//	}
//	q, err := ch.QueueDeclare(c.id, true, false, false, false, nil)
//	if err != nil {
//		return err
//	}
//	for _, m := range c.matchs {
//		if err := ch.QueueBind(q.Name, m, c.topic, false, nil); err != nil {
//			return err
//		}
//	}
//	megs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
//	if err != nil {
//		return fmt.Errorf("ch.Consume failed cause=%s", err.Error())
//	}
//	utils.Wrap(func() {
//		c.handler(megs)
//	})
//	return nil
//}
//
//func (c *TopicConsumer) loop() {
//	tick := time.Tick(defaultRotationTime)
//	for {
//		select {
//		case <- tick:
//			if c.conn.IsClosed() {
//				c.close()
//				if err := c.connect(); err != nil {
//					log.Logger().Warn("DelayTopicConsumer.Loop reconnect failed cuase=%s", err.Error())
//				}
//				log.Logger().Info("DelayTopicConsumer.Loop reconnect success")
//			}
//		case <- c.ctx.Done():
//			return
//		}
//	}
//}
//
//func (c *TopicConsumer) close() error {
//	var errs error
//	if c.ch != nil {
//		errs = multierr.Append(errs, c.ch.Close())
//	}
//	if c.conn != nil {
//		errs = multierr.Append(errs, c.conn.Close())
//	}
//	return errs
//}
//
//func (c *TopicConsumer) Close() error {
//	c.cancel()
//	return c.close()
//}
//
