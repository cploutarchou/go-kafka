package client

import (
	"crypto/tls"
	"errors"
	"fmt"
	"go-kafka/consumer"
	"go-kafka/producer"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type kafkaImpl struct {
	producer producer.Producer
	consumer consumer.Consumer
	client   sarama.Client
	Config   Config
}

type Config struct {
	Brokers           []string
	Net               *net.Dialer
	TLS               *tls.Config
	SASL              *sarama.Config
	Compression       string
	MaxRetryBackoff   time.Duration
	MaxMessageBytes   int
	Timeout           time.Duration
	RetryBackoff      time.Duration
	RequiredAcks      sarama.RequiredAcks
	Partitioner       sarama.PartitionerConstructor
	ClientID          string
	GroupID           string
	Heartbeat         time.Duration
	SessionTimeout    time.Duration
	CommitInterval    time.Duration
	RebalancedWait    time.Duration
	Topic             string
	MaxProcessing     int
	MaxProcessingTime time.Duration
}

type Kafka interface {
	Producer() producer.Producer
	Close() error
	Consumer() consumer.Consumer
	Ping() error
}

var (
	clientMutex sync.Mutex
)

func (k *kafkaImpl) Close() error {
	// Release resources
	k.producer.Close()
	k.consumer.Close()
	k.client.Close()

	return nil
}

func (k *kafkaImpl) Consumer() consumer.Consumer {
	return k.consumer
}

func (k *kafkaImpl) Ping() error {
	// Synchronize access to the Sarama client
	clientMutex.Lock()
	defer clientMutex.Unlock()

	// Ping the Kafka cluster
	v := k.client.Brokers()
	for _, broker := range v {
		if err := broker.Open(k.client.Config()); err != nil {
			return err
		}
	}
	if len(v) == 0 {
		return errors.New("unable to ping Kafka cluster")
	}
	return nil
}

func NewKafka(config Config) (Kafka, error) {
	var err error
	var producer_ producer.Producer
	var client_ sarama.Client

	// Create a new Sarama configuration object with default values
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Retry.Max = 5
	saramaConfig.Producer.Return.Successes = true

	// Apply TLS configuration if provided
	if config.TLS != nil {
		saramaConfig.Net.TLS.Enable = true
		saramaConfig.Net.TLS.Config = config.TLS
	}

	// Apply SASL configuration if provided
	if config.SASL != nil {
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.User = config.SASL.Net.SASL.User
		saramaConfig.Net.SASL.Password = config.SASL.Net.SASL.Password
		saramaConfig.Net.SASL.Handshake = config.SASL.Net.SASL.Handshake
		saramaConfig.Net.SASL.Mechanism = config.SASL.Net.SASL.Mechanism
	}

	// Apply compression algorithm if provided
	if config.Compression != "" {
		var compression sarama.CompressionCodec
		switch strings.ToLower(config.Compression) {
		case "gzip":
			compression = sarama.CompressionGZIP
		case "snappy":
			compression = sarama.CompressionSnappy
		case "lz4":
			compression = sarama.CompressionLZ4
		case "zstd":
			compression = sarama.CompressionZSTD
		default:
			return nil, fmt.Errorf("unknown compression algorithm: %s", config.Compression)

		}

		saramaConfig.Producer.Compression = compression
	}

	// Apply maximum retry backoff if provided
	if config.MaxRetryBackoff != 0 {
		saramaConfig.Producer.Retry.Max = int(config.MaxRetryBackoff)
	}

	// Apply maximum message bytes if provided
	if config.MaxMessageBytes != 0 {
		saramaConfig.Producer.MaxMessageBytes = config.MaxMessageBytes
	}

	// Apply timeout if provided
	if config.Timeout != 0 {
		saramaConfig.Net.DialTimeout = config.Timeout
		saramaConfig.Net.ReadTimeout = config.Timeout
		saramaConfig.Net.WriteTimeout = config.Timeout
	}

	// Apply retry backoff if provided
	if config.RetryBackoff != 0 {
		saramaConfig.Producer.Retry.Backoff = config.RetryBackoff
	}

	// Apply required acks if provided
	if config.RequiredAcks != 0 {
		saramaConfig.Producer.RequiredAcks = config.RequiredAcks
	}

	// Apply partitioner if provided
	if config.Partitioner != nil {
		saramaConfig.Producer.Partitioner = config.Partitioner
	}

	// Apply client ID if provided
	if config.ClientID != "" {
		saramaConfig.ClientID = config.ClientID
	}

	// Apply group ID if provided
	if config.GroupID != "" {
		saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	}

	// Apply heartbeat if provided
	if config.Heartbeat != 0 {
		saramaConfig.Consumer.Group.Heartbeat.Interval = config.Heartbeat
	}

	// Apply session timeout if provided
	if config.SessionTimeout != 0 {
		saramaConfig.Consumer.Group.Session.Timeout = config.SessionTimeout
	}

	// Apply max processing if provided
	if config.MaxProcessing != 0 {
		saramaConfig.Consumer.MaxProcessingTime = config.MaxProcessingTime
	}

	// Apply commit interval if provided
	if config.CommitInterval != 0 {
		saramaConfig.Consumer.Group.Rebalance.Timeout = config.CommitInterval
	}

	// Apply rebalanced wait if provided
	if config.RebalancedWait != 0 {
		saramaConfig.Consumer.Group.Rebalance.Timeout = config.RebalancedWait
	}

	// Create a new Sarama client
	client_, err = sarama.NewClient(config.Brokers, saramaConfig)
	if err != nil {
		return nil, err
	}

	// Create a new Sarama producer
	producer_, err = producer.NewProducer(&client_)
	if err != nil {
		return nil, err
	}

	conConf := consumer.Config{
		GroupID:        config.GroupID,
		Heartbeat:      config.Heartbeat,
		SessionTimeout: config.SessionTimeout,
		MaxProcessing:  config.MaxProcessing,
		CommitInterval: config.CommitInterval,
		RebalancedWait: config.RebalancedWait,
	}

	// Create a new Sarama consumer group
	consumerGroup_, err := consumer.NewConsumerGroup(conConf, saramaConfig, client_)
	if err != nil {
		return nil, err
	}

	// Create a new Kafka client
	k := &kafkaImpl{
		client:   client_,
		producer: producer_,
		consumer: consumerGroup_,
	}

	return k, nil
}

func (k *kafkaImpl) Producer() producer.Producer {
	return k.producer
}
