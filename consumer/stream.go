package consumer

import (
	"context"
	"github.com/Shopify/sarama"
	"log"
	"sync"
)

type Stream interface {
	Messages() <-chan *sarama.ConsumerMessage
	Errors() <-chan error
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

func (s *streamImpl) Messages() <-chan *sarama.ConsumerMessage {
	return s.messages
}

func (s *streamImpl) Errors() <-chan error {
	return s.errors
}
func (s *streamImpl) Close() error {
	s.mutex.Lock()
	if s.closed {
		s.mutex.Unlock()
		return nil
	}
	s.closed = true
	s.mutex.Unlock()

	if err := s.consumer.Close(); err != nil {
		return err
	}

	s.wg.Wait()
	close(s.messages)
	close(s.errors)

	return nil
}

func (s *streamImpl) Setup(session sarama.ConsumerGroupSession) error {
	// Assign a partition to this consumer instance
	for _, partition := range session.Claims() {
		log.Printf("Consumer %s joined partition %d\n", session.MemberID(), partition)
	}

	// Initialize any state that is required for consuming messages

	return nil
}

func (s *streamImpl) Cleanup(session sarama.ConsumerGroupSession) error {
	// Commit the offsets for this consumer instance
	session.Commit()
	close(s.messages)
	close(s.errors)
	return nil
}
