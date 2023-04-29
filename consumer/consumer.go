package consumer

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type Consumer interface {
	Start() error
	Messages() <-chan *sarama.ConsumerMessage
	Close() error
	Closed() bool
}

type consumer struct {
	config       *Config
	consumer     sarama.ConsumerGroup
	msgChan      chan *sarama.ConsumerMessage
	closeOnce    sync.Once
	closeChannel chan struct{}
	mutex        sync.Mutex
	closed       bool
}

func (c *consumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		select {
		case <-c.closeChannel:
			return nil
		default:
			c.mutex.Lock()
			if !c.closed {
				c.msgChan <- msg
				sess.MarkMessage(msg, "")
			}
			c.mutex.Unlock()
		}
	}
	return nil
}

func (c *consumer) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (c *consumer) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func NewConsumer(config *Config) (Consumer, error) {
	if config == nil {
		return nil, errors.New("config cannot be nil")
	}

	if len(config.Brokers) == 0 {
		return nil, errors.New("at least one broker is required")
	}

	if config.Topic == "" {
		return nil, errors.New("topic cannot be empty")
	}

	if config.GroupID == "" {
		return nil, errors.New("group id cannot be empty")
	}

	if config.Logger == nil {
		config.Logger = log.New(log.Writer(), log.Prefix(), log.Flags())
	}

	return &consumer{
		config:       config,
		msgChan:      make(chan *sarama.ConsumerMessage),
		closeChannel: make(chan struct{}),
	}, nil
}

func (c *consumer) Start() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.closed {
		return errors.New("consumer is already closed")
	}

	if c.consumer != nil {
		return errors.New("consumer is already started")
	}

	config := sarama.NewConfig()
	if config.Version != sarama.V2_8_0_0 {
		config.Version = sarama.V2_8_0_0
	}

	if config.Consumer.Offsets.Initial != 0 {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	if config.Consumer.Group.Session.Timeout == 0 {
		config.Consumer.Group.Session.Timeout = 60 * time.Second
	}

	if config.Consumer.Group.Heartbeat.Interval == 0 {
		config.Consumer.Group.Heartbeat.Interval = 10 * time.Second
	}

	if config.Consumer.Group.Rebalance.Timeout == 0 {
		config.Consumer.Group.Rebalance.Timeout = 60 * time.Second
	}

	if config.Consumer.Group.Rebalance.Retry.Max == 0 {
		config.Consumer.Group.Rebalance.Retry.Max = 4
	}

	if config.Consumer.Group.Rebalance.Retry.Backoff == 0 {
		config.Consumer.Group.Rebalance.Retry.Backoff = 2 * time.Second
	}

	if config.Consumer.Offsets.AutoCommit.Enable == false {
		config.Consumer.Offsets.AutoCommit.Enable = true
		if config.Consumer.Offsets.AutoCommit.Interval == 0 {
			config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
		}
	}
	if config.Consumer.Offsets.Retry.Max == 0 {
		config.Consumer.Offsets.Retry.Max = 3
	}

	// increase the number of partitions to consume from
	config.Consumer.Fetch.Default = 1024 * 1024 * 1024

	if c.config.MinBytes > 0 {
		config.Consumer.Fetch.Min = c.config.MinBytes
	}

	if c.config.MaxBytes > 0 {
		config.Consumer.Fetch.Max = c.config.MaxBytes
	}

	if c.config.MaxWait > 0 {
		config.Consumer.MaxWaitTime = c.config.MaxWait
	}

	consumer, err := sarama.NewConsumerGroup(c.config.Brokers, c.config.GroupID, config)
	if err != nil {
		return err
	}

	c.consumer = consumer

	if config.Consumer.Fetch.Default == 0 {
		config.Consumer.Fetch.Default = 1024 * 1024 * 1024
	}

	// start the message consumption loop in a separate goroutine
	go func() {
		for {
			if err := consumer.Consume(context.Background(), []string{c.config.Topic}, c); err != nil {
				c.config.Logger.Printf("Error from consumer: %v", err)
			}
			// check if the consumer is closed
			if c.Closed() {
				return
			}
		}
	}()

	return nil
}

func (c *consumer) StartAsync() {
	go c.consume()
}

func (c *consumer) StartSync() {
	c.consume()
}

func (c *consumer) Messages() <-chan *sarama.ConsumerMessage {
	return c.msgChan
}

func (c *consumer) Close() error {
	c.closeOnce.Do(func() {
		close(c.closeChannel)
		close(c.msgChan)
		_ = c.consumer.Close()
	})
	return nil
}

func (c *consumer) consume() {
	for {
		select {
		case <-c.closeChannel:
			return
		default:
			err := c.consumer.Consume(context.Background(), []string{c.config.Topic}, c)
			if err != nil {
				if err == sarama.ErrClosedConsumerGroup {
					return
				}
				c.config.Logger.Printf("Error consuming messages: %s", err)
			}
		}
	}
}

type Config struct {
	Brokers  []string
	Topic    string
	GroupID  string
	Logger   *log.Logger
	MinBytes int32
	MaxBytes int32
	MaxWait  time.Duration
}

func (c *consumer) Closed() bool {
	select {
	case _, ok := <-c.msgChan:
		if ok {
			close(c.msgChan)
		}
		return !ok
	default:
		return false
	}
}
