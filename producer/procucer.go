package producer

import (
	"errors"
	"github.com/Shopify/sarama"
	"go-kafka/types"
	"log"
	"os"
)

type Producer interface {
	Send(topic string, key []byte, value []byte) error
	SendAsync(topic string, key []byte, value []byte) error
	Close() error
}

type producerImpl struct {
	producer sarama.SyncProducer
	Config   *types.WriterConfig
}

func NewProducer(brokers []string, topic string, config *types.WriterConfig) (Producer, error) {
	if len(brokers) == 0 {
		return nil, errors.New("at least one broker is required")
	}
	if topic == "" {
		return nil, errors.New("topic cannot be empty")
	}
	if config == nil {
		// Set default config values
		config = &types.WriterConfig{
			Addr:                   brokers,
			Topic:                  topic,
			AllowAutoTopicCreation: true,
		}
	} else {
		config.Addr = brokers
		config.Topic = topic
	}

	if config.Logger == nil {
		config.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Retry.Max = 3
	saramaConfig.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(config.Addr, saramaConfig)
	if err != nil {
		return nil, err
	}

	return &producerImpl{
		producer: producer,
		Config:   config,
	}, nil
}

func (p *producerImpl) Send(topic string, key []byte, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}
	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		p.Config.Logger.Printf("Failed to send message to topic %s: %s", topic, err)
		return err
	}
	p.Config.Logger.Printf("Message sent to topic %s, partition %d, offset %d", topic, partition, offset)
	return nil
}

func (p *producerImpl) SendAsync(topic string, key []byte, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}
	p.producer.SendMessages([]*sarama.ProducerMessage{msg})
	return nil
}

func (p *producerImpl) Close() error {
	return p.producer.Close()
}
