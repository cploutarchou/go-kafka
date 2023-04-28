package producer

import (
	"context"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"go-kafka/types"
)

type Producer interface {
	Send(topic string, key []byte, value []byte) error
	SendAsync(topic string, key []byte, value []byte) error
	Close() error
}

type Impl struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string, config *types.WriterConfig) (Producer, error) {
	fmt.Println(topic)
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

	writer := kafka.Writer{
		Addr:                   kafka.TCP(config.Addr...),
		Topic:                  config.Topic,
		AllowAutoTopicCreation: config.AllowAutoTopicCreation,
	}

	return &Impl{
		writer: &writer,
	}, nil

}

func (k *Impl) Send(topic string, key []byte, value []byte) error {
	k.writer.Async = false
	k.writer.Topic = topic
	return k.writer.WriteMessages(context.Background(), kafka.Message{
		Key:   key,
		Value: value,
	})
}

func (k *Impl) SendAsync(topic string, key []byte, value []byte) error {
	k.writer.Async = true
	k.writer.Topic = topic
	return k.writer.WriteMessages(context.Background(), kafka.Message{
		Key:   key,
		Value: value,
	})
}

func (k *Impl) Close() error {
	return k.writer.Close()
}
