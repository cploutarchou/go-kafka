package producer

import (
	"errors"
	"go-kafka/types"
	"log"
	"os"

	"github.com/Shopify/sarama"
)

// Producer interface defines the methods that can be used to produce messages to Kafka.
type Producer interface {
	//Send sends a message to Kafka and returns an error if it fails.
	Send(topic string, key []byte, value []byte) error
	// SendAsync sends a message to Kafka asynchronously and returns an error if it fails.
	SendAsync(topic string, key []byte, value []byte) error
	// Close closes the producer.
	Close() error
}

type producerImpl struct {
	producer sarama.SyncProducer
	Config   *types.Config
}

// NewProducer returns a new instance of a Kafka producer.
func NewProducer(brokers []string, topic string, config *types.Config) (Producer, error) {
	if len(brokers) == 0 {
		return nil, errors.New("at least one broker is required")
	}
	if topic == "" {
		return nil, errors.New("topic cannot be empty")
	}

	// Set default config values if no configuration is provided.
	if config == nil {
		config = &types.Config{
			Brokers:                brokers,
			Topic:                  topic,
			AllowAutoTopicCreation: true,
		}
	} else {
		config.Brokers = brokers
		config.Topic = topic
	}

	// Set default logger if no logger is provided.
	if config.Logger == nil {
		config.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	// Configure Sarama with required settings.
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Retry.Max = 3
	saramaConfig.Producer.Return.Successes = true

	// Create a new Sarama SyncProducer.
	producer, err := sarama.NewSyncProducer(config.Brokers, saramaConfig)
	if err != nil {
		return nil, err
	}

	// Return the producer implementation.
	return &producerImpl{
		producer: producer,
		Config:   config,
	}, nil
}

// Send sends a message synchronously to Kafka.
func (p *producerImpl) Send(topic string, key []byte, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	// Send the message and log success or failure.
	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		p.Config.Logger.Printf("Failed to send message to topic %s: %s", topic, err)
		return err
	}
	p.Config.Logger.Printf("Message sent to topic %s, partition %d, offset %d", topic, partition, offset)
	return nil
}

// SendAsync sends a message asynchronously to Kafka.
func (p *producerImpl) SendAsync(topic string, key []byte, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	// Send the message asynchronously.
	p.producer.SendMessages([]*sarama.ProducerMessage{msg})
	return nil
}

// Close closes the producer.
func (p *producerImpl) Close() error {
	return p.producer.Close()
}
