package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type Consumer interface {
	Stream() (Stream, error)
	Close() error
	Subscribe([]string) error
}

type consumerImpl struct {
	saramaConfig   *sarama.Config
	config         Config
	stream         *streamImpl
	mutex          sync.Mutex
	closed         bool
	consumer       sarama.ConsumerGroup
	ctx            context.Context
	cancel         context.CancelFunc
	topics         []string
	rebalancedOnce sync.Once
}

type Config struct {
	Brokers        []string
	GroupID        string
	Heartbeat      time.Duration
	SessionTimeout time.Duration
	MaxProcessing  int
	CommitInterval time.Duration
	RebalancedWait time.Duration
	Topic          string
}

func NewConsumerGroup(config Config, saramaConfig *sarama.Config) (Consumer, error) {
	consumer, err := sarama.NewConsumerGroup(config.Brokers, config.GroupID, saramaConfig)
	if err != nil {
		fmt.Println("Error creating consumer group client: %v", err)
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	messages := make(chan *sarama.ConsumerMessage)
	errors := make(chan error)
	stream := &streamImpl{
		consumer: consumer,
		ctx:      ctx,
		messages: messages,
		errors:   errors,
		closed:   false,
	}
	return &consumerImpl{
		saramaConfig: saramaConfig,
		config:       config,
		stream:       stream,
		mutex:        sync.Mutex{},
		closed:       false,
		consumer:     consumer,
		ctx:          ctx,
		cancel:       cancel,
		topics:       nil,
	}, nil
}

func (c *consumerImpl) Stream() (Stream, error) {
	return c.stream, nil
}

func (c *consumerImpl) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	c.cancel()
	return c.stream.Close()
}

func (s *streamImpl) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	s.wg.Add(1)
	defer s.wg.Done()

	for {
		select {
		case <-s.ctx.Done():
			return nil
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			s.mutex.Lock()
			if s.closed {
				s.mutex.Unlock()
				return nil
			}
			s.messages <- message
			s.mutex.Unlock()
			session.MarkMessage(message, "")
		case <-session.Context().Done():
			return nil
		}
	}
}

func (c *consumerImpl) Subscribe(topics []string) error {
	c.topics = topics
	return nil
}
