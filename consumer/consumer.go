package consumer

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type Config struct {
	Brokers  []string
	Topic    string
	GroupID  string
	Logger   *log.Logger
	MinBytes int
	MaxBytes int
	MaxWait  time.Duration
}

type Consumer interface {
	Start() error
	Messages() <-chan *sarama.ConsumerMessage
	Close() error
}

type consumer struct {
	config       *Config
	consumer     sarama.ConsumerGroup
	msgChan      chan *sarama.ConsumerMessage
	closeOnce    sync.Once
	closeChannel chan struct{}
}

func (c *consumer) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (c *consumer) Cleanup(session sarama.ConsumerGroupSession) error {
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
		closeChannel: make(chan struct{}),
	}, nil
}

func (c *consumer) Start() error {
	config := sarama.NewConfig()
	config.Version = sarama.V3_1_2_0
	config.Consumer.Group.Member.UserData = []byte(c.config.GroupID)
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Session.Timeout = 60 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 10 * time.Second

	if c.config.MinBytes > 0 {
		config.Consumer.Fetch.Min = int32(c.config.MinBytes)
	}

	if c.config.MaxBytes > 0 {
		config.Consumer.Fetch.Max = int32(c.config.MaxBytes)
	}

	if c.config.MaxWait > 0 {
		config.Consumer.MaxWaitTime = c.config.MaxWait
	}

	consumer, err := sarama.NewConsumerGroup(c.config.Brokers, c.config.GroupID, config)
	if err != nil {
		return err
	}

	c.consumer = consumer

	go c.consume()

	return nil
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
	c.msgChan = make(chan *sarama.ConsumerMessage)

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

func (c *consumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg := <-claim.Messages():
			c.msgChan <- msg
			sess.MarkMessage(msg, "")
		case <-c.closeChannel:
			return nil
		}
	}
}
