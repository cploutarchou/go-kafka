package types

import (
	"fmt"
	"log"
	"math"
	"time"

	"github.com/Shopify/sarama"
)

type Config struct {
	Brokers                []string
	Topic                  string
	BalanceStrategy        string
	MaxAttempts            int
	WriteBackoffMin        time.Duration
	WriteBackoffMax        time.Duration
	BatchSize              int
	BatchBytes             int
	BatchTimeout           time.Duration
	ReadTimeout            time.Duration
	WriteTimeout           time.Duration
	RequiredAcks           int16
	Async                  bool
	Compression            string
	Logger                 *log.Logger
	ErrorLogger            *log.Logger
	AllowAutoTopicCreation bool
}

func (c *Config) saramaConfig() (*sarama.Config, error) {
	config := sarama.NewConfig()

	switch c.BalanceStrategy {
	case "Range":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	case "RoundRobin":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	case "Sticky":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	default:
		return nil, fmt.Errorf("invalid balance strategy: %s", c.BalanceStrategy)
	}

	config.Producer.RequiredAcks = sarama.RequiredAcks(c.RequiredAcks)
	config.Producer.Retry.Max = c.MaxAttempts
	config.Producer.Retry.Backoff = c.WriteBackoffMin
	config.Producer.Retry.BackoffFunc = func(retries, maxRetries int) time.Duration {
		backoff := c.WriteBackoffMin * time.Duration(math.Pow(2, float64(retries)))
		if backoff > c.WriteBackoffMax {
			backoff = c.WriteBackoffMax
		}
		return backoff
	}

	config.Producer.Flush.Bytes = c.BatchBytes
	config.Producer.Flush.Frequency = c.BatchTimeout
	config.Producer.Flush.Messages = c.BatchSize
	config.Net.ReadTimeout = c.ReadTimeout
	config.Net.WriteTimeout = c.WriteTimeout
	config.Producer.Compression = sarama.CompressionSnappy
	config.ClientID = "kafka-go"

	return config, nil
}
