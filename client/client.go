package client

import (
	"crypto/tls"
	"fmt"
	"github.com/Shopify/sarama"
	"go-kafka/consumer"
	"go-kafka/producer"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

var (
	mu            sync.Mutex
	clientMutex   sync.Mutex
	producerMutex sync.Mutex
)

// Kafka represents a client for interacting with a Kafka cluster.
type Kafka interface {
	// Producer returns a producer that can be used to send messages to Kafka.
	Producer() producer.Producer

	// Close closes the underlying client and releases all resources associated with it.
	Close() error

	Consumer() consumer.Consumer
}

// kafkaImpl is an implementation of the Kafka interface that uses the Sarama library.
type kafkaImpl struct {
	producer producer.Producer
	client   sarama.Client
	Config   Config
}

// Producer returns a producer that can be used to send messages to Kafka.
func (k *kafkaImpl) Producer() producer.Producer {
	return k.producer
}

// Close closes the underlying client and releases all resources associated with it.
func (k *kafkaImpl) Close() error {
	return k.client.Close()
}

// Config represents the configuration options for a Kafka client.
type Config struct {
	// Brokers is a list of Kafka broker addresses in the format "host:port".
	Brokers []string

	// Net is an optional dialer that allows for custom network dialer options.
	Net *net.Dialer

	// TLS is an optional config that enables TLS communication with Kafka brokers.
	TLS *tls.Config

	// SASL is an optional config that enables SASL authentication with Kafka brokers.
	SASL *sarama.Config

	// Compression is an optional string that sets the compression algorithm to use for messages.
	Compression string

	// MaxRetryBackoff is an optional duration that sets the maximum amount of time to wait between retries.
	MaxRetryBackoff time.Duration

	// MaxMessageBytes is an optional integer that sets the maximum size of a message in bytes.
	MaxMessageBytes int

	// Timeout is an optional duration that sets the timeout for network requests to Kafka brokers.
	Timeout time.Duration

	// RetryBackoff is an optional duration that sets the amount of time to wait between retries.
	RetryBackoff time.Duration

	// RequiredAcks is an optional setting that specifies the number of acks required for a message to be considered sent.
	RequiredAcks sarama.RequiredAcks

	// Partitioner is an optional constructor for a custom partitioner to use when sending messages.
	Partitioner sarama.PartitionerConstructor

	// ClientID is an optional string that specifies the client identifier to use for Kafka client.
	ClientID string

	// GroupID is an optional string that specifies the consumer group identifier to use for Kafka client.
	GroupID string
}

// NewKafka creates a new Kafka client with the given configuration options.
// Returns a Kafka interface or an error if the client cannot be created.
func NewKafka(config Config) (Kafka, error) {
	mu.Lock()
	defer mu.Unlock()

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
		default:
			// Print a warning message and set compression to none
			log.Default().Println("Invalid compression type. Setting compression to none.")
			compression = sarama.CompressionNone
		}

		// Synchronize access to the Producer fields
		producerMutex.Lock()
		defer producerMutex.Unlock()

		// Update the Producer fields with the new compression codec
		saramaConfig.Producer.Compression = compression
	}

	// Apply maximum retry backoff if provided
	if config.MaxRetryBackoff != 0 {
		saramaConfig.Producer.Retry.Backoff = config.MaxRetryBackoff
	}

	// Apply maximum message bytes if provided
	if config.MaxMessageBytes != 0 {
		saramaConfig.Producer.MaxMessageBytes = config.MaxMessageBytes
	}

	// Apply timeout for network requests if provided
	if config.Timeout != 0 {
		saramaConfig.Producer.Timeout = config.Timeout
	}

	// Apply retry backoff time if provided
	if config.RetryBackoff != 0 {
		saramaConfig.Producer.Retry.Backoff = config.RetryBackoff
	}

	// Synchronize access to the Sarama client and producer creation
	clientMutex.Lock()
	defer clientMutex.Unlock()

	fmt.Println(171)
	// Create a new Sarama client object with the given broker addresses and configuration options
	client_, err = sarama.NewClient(config.Brokers, saramaConfig)
	if err != nil {
		return nil, err
	}
	fmt.Println(177)
	// Create a new producer object using the Sarama client
	producer_, err = producer.NewProducer(client_)
	if err != nil {
		return nil, err
	}
	// Return a new Kafka client object
	return &kafkaImpl{
		producer: producer_,
		client:   client_,
		Config:   config,
	}, nil
}

func (k *kafkaImpl) Consumer() consumer.Consumer {
	// Create a new Sarama configuration object with default values
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_0_0_0
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaConfig.Consumer.Group.Session.Timeout = 10 * time.Second
	saramaConfig.Consumer.Group.Heartbeat.Interval = 3 * time.Second
	saramaConfig.Consumer.Group.Rebalance.Timeout = 60 * time.Second
	// Create a new consumer configuration object with the given broker addresses and configuration options
	consumerConfig := consumer.Config{
		Brokers:        k.Config.Brokers,
		GroupID:        k.Config.GroupID,
		Heartbeat:      3 * time.Second,
		SessionTimeout: 10 * time.Second,
		MaxProcessing:  100,
		CommitInterval: 1 * time.Second,
		RebalancedWait: 60 * time.Second,
	}

	// Create a new consumer object using the consumer configuration
	c, err := consumer.NewConsumer(consumerConfig, saramaConfig)
	if err != nil {
		log.Fatal(err)
	}

	return c
}
