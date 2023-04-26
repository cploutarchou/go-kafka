package producer

import (
	"errors"
	"log"
	"sync"

	"github.com/Shopify/sarama"
)

// Producer Package producer provides a Kafka producer that can be used to send messages to Kafka.
//
// The producer interface allows for sending messages synchronously and asynchronously to a Kafka cluster.
// This package uses the Sarama library to interact with Kafka.
//
// The producer implementation is thread-safe and concurrent, and it provides a way to gracefully shut down the
// producer and release all resources associated with it.
type Producer interface {
	Send(topic string, key []byte, value []byte) error
	SendMsg(message *Message) error
	SendAsync(topic string, key []byte, value []byte) error
	Close() error
}

// Impl is an implementation of the Producer interface that uses the Sarama library.
type Impl struct {
	sync.Mutex
	producer      sarama.SyncProducer
	producerAsync sarama.AsyncProducer
	closed        bool
	wg            *sync.WaitGroup
}

type Message struct {
	Topic string
	Key   string
	Value []byte
}

// Send sends a synchronous message to the given Kafka topic with the specified key and value.
// This method blocks until the message is sent successfully or an error occurs.
// If the producer has been closed, an error is returned.
func (p *Impl) Send(topic string, key []byte, value []byte) error {
	p.Lock()
	defer p.Unlock()
	if p.closed {
		return errors.New("producer is closed")
	}
	message := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}
	_, _, err := p.producer.SendMessage(message)
	return err
}

// SendAsync sends an asynchronous message to the given Kafka topic with the specified key and value.
// This method returns immediately and does not block.
// If the producer has been closed, an error is returned.
func (p *Impl) SendAsync(topic string, key []byte, value []byte) error {
	p.Lock()
	defer p.Unlock()
	if p.closed {
		return errors.New("producer is closed")
	}
	message := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}
	p.producerAsync.Input() <- message
	return nil
}

// Close closes the producer and releases all resources associated with it.
// This method blocks until all messages have been sent and all goroutines have finished.
// If the producer has already been closed, this method returns nil.
func (p *Impl) Close() error {
	p.Lock()
	defer p.Unlock()
	if p.closed {
		return nil
	}
	err := p.producer.Close()
	if err == nil {
		p.closed = true
		p.producerAsync.AsyncClose()
		p.wg.Wait() // Wait for all goroutines to finish
	}
	return err
}

func (p *Impl) SendMsg(message *Message) error {
	if message == nil {
		return errors.New("message is nil")
	}

	return p.Send(message.Topic, []byte(message.Key), message.Value)
}

// NewProducer creates a new Kafka producer with the given client.
// Returns a Producer interface or an error if the producer cannot be created.
func NewProducer(client *sarama.Client) (Producer, error) {
	syncProducer, err := sarama.NewSyncProducerFromClient(*client)
	if err != nil {
		return nil, err
	}
	asyncProducer, err := sarama.NewAsyncProducerFromClient(*client)
	if err != nil {
		return nil, err
	}

	// Use a WaitGroup to ensure all goroutines are finished before returning the producer
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case msg := <-asyncProducer.Successes():
				log.Printf("Produced message to topic %s partition %d offset %d\n",
					msg.Topic, msg.Partition, msg.Offset)
			case err := <-asyncProducer.Errors():
				log.Printf("Failed to produce message: %s\n", err.Error())
			}
		}
	}()

	// Return a new Producer object
	return &Impl{
		producer:      syncProducer,
		producerAsync: asyncProducer,
		wg:            &wg,
	}, nil
}
