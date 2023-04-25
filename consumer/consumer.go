package consumer

import (
	"context"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type Stream interface {
	Messages() <-chan *sarama.ConsumerMessage
	Errors() <-chan error
	Close() error
}

type Consumer interface {
	Stream() Stream
	Close() error
}

type streamImpl struct {
	consumer sarama.ConsumerGroup
	ctx      context.Context
	messages chan *sarama.ConsumerMessage
	errors   chan error
	wg       sync.WaitGroup
	mutex    sync.Mutex
	closed   bool
}

type consumerImpl struct {
	saramaConfig *sarama.Config
	config       Config
	stream       *streamImpl
	mutex        sync.Mutex
	closed       bool
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

func NewConsumer(config Config, saramaConfig *sarama.Config) (Consumer, error) {
	consumer, err := sarama.NewConsumerGroup(config.Brokers, config.GroupID, saramaConfig)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
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
	}, nil
}

func (c *consumerImpl) Stream() Stream {
	return c.stream
}

func (c *consumerImpl) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	return c.stream.Close()
}

func (s *streamImpl) Messages() <-chan *sarama.ConsumerMessage {
	return s.messages
}

func (s *streamImpl) Errors() <-chan error {
	return s.errors
}

func (s *streamImpl) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	close(s.messages)
	close(s.errors)
	s.wg.Wait()
	return s.consumer.Close()
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
