package consumer

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

// Consumer defines an interface for consuming messages from a Kafka topic.
type Consumer interface {
	// Start starts consuming messages from the Kafka topic.
	Start() error
	// Messages returns a channel that can be used to receive messages
	// from the Kafka topic.
	Messages() <-chan *sarama.ConsumerMessage
	// Close stops the consumer and releases any resources it holds.
	Close() error
	// Closed returns true if the consumer has been closed.
	Closed() bool
}

// consumer represents a Kafka consumer.
type consumer struct {
	config       *Config                      // The configuration used for the consumer.
	consumer     sarama.ConsumerGroup         // The underlying Sarama consumer.
	msgChan      chan *sarama.ConsumerMessage // The channel used to pass received messages to the client.
	closeOnce    sync.Once                    // Ensures that the Close() method is called only once.
	closeChannel chan struct{}                // Channel used to signal the closing of the consumer.
	mutex        sync.Mutex                   // Mutex used to synchronize access to the consumer state.
	closed       bool                         // Indicates whether the consumer has been closed.
}

// ConsumeClaim is the method called by the consumer interface when a consumer group claim is created.
func (c *consumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		select {
		case <-c.closeChannel:
			return nil
		default:
			c.mutex.Lock()
			if !c.closed {
				c.msgChan <- msg
				sess.MarkMessage(msg, "")
			}
			c.mutex.Unlock()
		}
	}
	return nil
}

// Setup is called at the beginning of a new session, before ConsumeClaim.
// It is used to set up any necessary resources for the session.
func (c *consumer) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is called at the end of a session, once all ConsumeClaim goroutines have exited.
// It is used to clean up any resources that were set up during the session.
func (c *consumer) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// NewConsumer creates a new Kafka consumer with the given configuration.
// Returns an error if the config is nil, or if any required fields are empty.
// The returned consumer object can be used to start consuming messages.
func NewConsumer(config *Config) (Consumer, error) {
	if config == nil {
		return nil, errors.New("config cannot be nil")
	}

	if len(config.Brokers) == 0 {
		return nil, errors.New("at least one broker is required")
	}

	if config.Topic == "" {
		return nil, errors.New("topic cannot be empty")
	}

	if config.GroupID == "" {
		return nil, errors.New("group id cannot be empty")
	}

	if config.Logger == nil {
		config.Logger = log.New(log.Writer(), log.Prefix(), log.Flags())
	}

	return &consumer{
		config:       config,
		msgChan:      make(chan *sarama.ConsumerMessage),
		closeChannel: make(chan struct{}),
	}, nil
}

// Start starts consuming messages from the Kafka topic.
func (c *consumer) Start() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.closed {
		return errors.New("consumer is already closed")
	}

	if c.consumer != nil {
		return errors.New("consumer is already started")
	}

	config := sarama.NewConfig()
	if config.Version != sarama.V2_8_0_0 {
		config.Version = sarama.V2_8_0_0
	}

	if config.Consumer.Offsets.Initial != 0 {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	if config.Consumer.Group.Session.Timeout == 0 {
		config.Consumer.Group.Session.Timeout = 60 * time.Second
	}

	if config.Consumer.Group.Heartbeat.Interval == 0 {
		config.Consumer.Group.Heartbeat.Interval = 10 * time.Second
	}

	if config.Consumer.Group.Rebalance.Timeout == 0 {
		config.Consumer.Group.Rebalance.Timeout = 60 * time.Second
	}

	if config.Consumer.Group.Rebalance.Retry.Max == 0 {
		config.Consumer.Group.Rebalance.Retry.Max = 4
	}

	if config.Consumer.Group.Rebalance.Retry.Backoff == 0 {
		config.Consumer.Group.Rebalance.Retry.Backoff = 2 * time.Second
	}

	if !config.Consumer.Offsets.AutoCommit.Enable {
		config.Consumer.Offsets.AutoCommit.Enable = true
		if config.Consumer.Offsets.AutoCommit.Interval == 0 {
			config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
		}
	}
	if config.Consumer.Offsets.Retry.Max == 0 {
		config.Consumer.Offsets.Retry.Max = 3
	}

	// increase the number of partitions to consume from
	config.Consumer.Fetch.Default = 1024 * 1024 * 1024

	if c.config.MinBytes > 0 {
		config.Consumer.Fetch.Min = c.config.MinBytes
	}

	if c.config.MaxBytes > 0 {
		config.Consumer.Fetch.Max = c.config.MaxBytes
	}

	if c.config.MaxWait > 0 {
		config.Consumer.MaxWaitTime = c.config.MaxWait
	}

	consumer_, err := sarama.NewConsumerGroup(c.config.Brokers, c.config.GroupID, config)
	if err != nil {
		return err
	}

	c.consumer = consumer_

	if config.Consumer.Fetch.Default == 0 {
		config.Consumer.Fetch.Default = 1024 * 1024 * 1024
	}

	// start the message consumption loop in a separate goroutine
	go func() {
		for {
			if err := consumer_.Consume(context.Background(), []string{c.config.Topic}, c); err != nil {
				c.config.Logger.Printf("Error from consumer: %v", err)
			}
			// check if the consumer is closed
			if c.Closed() {
				return
			}
		}
	}()

	return nil
}

// StartAsync starts consuming messages from the Kafka topic in a separate goroutine.
func (c *consumer) StartAsync() {
	go c.consume()
}

// StartSync starts consuming messages from the Kafka topic in the current goroutine.
func (c *consumer) StartSync() {
	c.consume()
}

// Messages returns a channel of messages consumed from the Kafka topic.
func (c *consumer) Messages() <-chan *sarama.ConsumerMessage {
	return c.msgChan
}

// Close closes the consumer.
func (c *consumer) Close() error {
	c.closeOnce.Do(func() {
		close(c.closeChannel)
		close(c.msgChan)
		_ = c.consumer.Close()
	})
	return nil
}

// consume starts consuming messages from the Kafka topic.
func (c *consumer) consume() {
	for {
		select {
		case <-c.closeChannel:
			return
		default:
			err := c.consumer.Consume(context.Background(), []string{c.config.Topic}, c)
			if err != nil {
				if err == sarama.ErrClosedConsumerGroup {
					return
				}
				c.config.Logger.Printf("Error consuming messages: %s", err)
			}
		}
	}
}

// Config is the configuration for creating a new consumer
type Config struct {
	Brokers  []string      // A list of host:port addresses to use for establishing the initial connection to the Kafka cluster.
	Topic    string        // Kafka topic to be consumed
	GroupID  string        // A name for the consumer group
	Logger   *log.Logger   // Logger used to log connection errors; defaults to log.New(os.Stderr, "", log.LstdFlags)
	MinBytes int32         // The minimum number of bytes to fetch in a request
	MaxBytes int32         // The maximum number of bytes to fetch in a request
	MaxWait  time.Duration // The maximum amount of time the broker will wait for Consumer.Fetch.Min bytes to become available before it returns fewer than that anyways
}

// Closed returns true if the consumer is closed
func (c *consumer) Closed() bool {
	select {
	case _, ok := <-c.msgChan:
		if ok {
			close(c.msgChan)
		}
		return !ok
	default:
		return false
	}
}
