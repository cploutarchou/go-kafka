package producer

import (
	"context"
	"errors"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
	"go-kafka/types"
	"log"
	"os"
	"time"
)

var ctx = context.Background()

type Producer interface {
	Send(topic string, key []byte, value []byte) error
	SendAsync(topic string, key []byte, value []byte) error
	Close() error
}

type Impl struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string, config *types.WriterConfig) (Producer, error) {
	if config == nil {
		// Set default config values
		config = &types.WriterConfig{
			Addr:                   brokers,
			Topic:                  topic,
			Balancer:               nil,
			MaxAttempts:            10,
			WriteBackoffMin:        50 * time.Millisecond,
			WriteBackoffMax:        5 * time.Second,
			BatchSize:              100,
			BatchBytes:             1024 * 1024,
			BatchTimeout:           1 * time.Second,
			ReadTimeout:            10 * time.Second,
			WriteTimeout:           10 * time.Second,
			RequiredAcks:           1,
			Async:                  true,
			Compression:            nil,
			Logger:                 nil,
			ErrorLogger:            nil,
			Transport:              nil,
			AllowAutoTopicCreation: false,
		}
	}

	if len(config.Addr) == 0 {
		return nil, errors.New("at least one broker is required")
	}
	if config.Topic == "" {
		return nil, errors.New("topic cannot be empty")
	}
	if config.Balancer == nil {
		config.Balancer = &kafka.LeastBytes{}
	}
	if config.Logger == nil {
		// Set default logger
		config.Logger = log.New(os.Stderr, "kafka writer: ", log.LstdFlags)

	}
	if config.ErrorLogger == nil {
		// Set default error logger
		config.ErrorLogger = log.New(os.Stderr, "kafka writer error: ", log.LstdFlags)
	}

	// Check if compression is set
	var writeComp compress.Compression
	switch config.Compression.Name() {
	case "gzip":
		config.Compression = compress.Gzip.Codec()
		writeComp = compress.Gzip
	case "snappy":
		config.Compression = compress.Snappy.Codec()
		writeComp = compress.Snappy
	case "lz4":
		config.Compression = compress.Lz4.Codec()
		writeComp = compress.Lz4
	case "zstd":
		config.Compression = compress.Zstd.Codec()
		writeComp = compress.Zstd
	default:
		config.Compression = compress.Snappy.Codec()
		writeComp = compress.Snappy

	}
	// Check if transport is kafka.Transport{} using deep equal
	if config.Transport == nil {
		transport := &kafka.Transport{
			Dial:        nil,
			DialTimeout: 10 * time.Second,
			IdleTimeout: 10 * time.Second,
			MetadataTTL: 5 * time.Minute,
			ClientID:    "go-kafka",
			TLS:         nil,
			SASL:        nil,
			Resolver:    nil,
			Context:     ctx,
		}
		config.Transport = transport

	}

	writer := kafka.Writer{
		Addr:                   kafka.TCP(config.Addr...),
		Topic:                  config.Topic,
		Balancer:               config.Balancer,
		MaxAttempts:            config.MaxAttempts,
		WriteBackoffMin:        config.WriteBackoffMin,
		WriteBackoffMax:        config.WriteBackoffMax,
		BatchSize:              config.BatchSize,
		BatchBytes:             int64(config.BatchBytes),
		BatchTimeout:           config.BatchTimeout,
		ReadTimeout:            config.ReadTimeout,
		WriteTimeout:           config.WriteTimeout,
		RequiredAcks:           kafka.RequiredAcks(config.RequiredAcks),
		Async:                  config.Async,
		Compression:            writeComp,
		Logger:                 config.Logger,
		ErrorLogger:            config.ErrorLogger,
		Transport:              config.Transport,
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
