package Client

import (
	"context"
	"errors"
	"github.com/segmentio/kafka-go"
	"go-kafka/producer"
	"go-kafka/types"
	"time"
)

type kafkaImpl struct {
	writer *kafka.Writer
	config *types.WriterConfig
}

type Config struct {
	Brokers []string
	Topic   string
	GroupID string
}

type Kafka interface {
	Producer() (producer.Producer, error)
	Close() error
	Ping() error
}

func NewKafka(config *Config) (Kafka, error) {
	if config == nil {
		return nil, errors.New("config cannot be nil")
	}

	if len(config.Brokers) == 0 {
		return nil, errors.New("at least one broker is required")
	}

	if config.Topic == "" {
		return nil, errors.New("topic cannot be empty")
	}

	config_ := types.WriterConfig{
		Addr:  config.Brokers,
		Topic: config.Topic,
	}
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  config_.Addr,
		Topic:    config_.Topic,
		Balancer: &kafka.LeastBytes{},
	})

	return &kafkaImpl{
		writer: writer,
		config: &config_,
	}, nil
}

func (k *kafkaImpl) Producer() (producer.Producer, error) {
	return producer.NewProducer(k.config.Addr, k.config.Topic, k.config)
}

func (k *kafkaImpl) Close() error {
	var err error
	if k.writer != nil {
		err = k.writer.Close()
	}
	//if k.reader != nil {
	//	err2 := k.reader.Close()
	//	if err == nil {
	//		err = err2
	//	}
	//}
	return err
}

func (k *kafkaImpl) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, "client_id", "test")
	//_, err := k.reader.FetchMessage(ctx)
	//if err != nil {
	//	return err
	//}

	return nil
}
