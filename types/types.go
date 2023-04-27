package types

import (
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

type WriterConfig struct {
	Addr                   []string
	Topic                  string
	Balancer               kafka.Balancer
	MaxAttempts            int
	WriteBackoffMin        time.Duration
	WriteBackoffMax        time.Duration
	BatchSize              int
	BatchBytes             int
	BatchTimeout           time.Duration
	ReadTimeout            time.Duration
	WriteTimeout           time.Duration
	RequiredAcks           int
	Async                  bool
	Compression            kafka.CompressionCodec
	Logger                 *log.Logger
	ErrorLogger            *log.Logger
	Transport              *kafka.Transport
	AllowAutoTopicCreation bool
}
