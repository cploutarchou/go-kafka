## Producer: the interface for the producer.

* Send(topic string, key []byte, value []byte) error: sends a synchronous message to the Kafka topic.
* SendAsync(topic string, key []byte, value []byte) error: sends an asynchronous message to the Kafka topic.
* Close() error: closes the underlying Kafka producer.
* producerImpl: the implementation of the Producer interface.

### Functions:

* NewProducer(brokers []string, topic string, config *types.Config) (Producer, error): creates a new Producer instance with the specified brokers, topic and configuration.

### Usage:

To create a new producer, call the NewProducer function, passing in the list of Kafka brokers, the topic to send messages to, and an optional configuration. If no configuration is provided, the default configuration values will be used.

The resulting producer instance can be used to send messages to the specified Kafka topic, either synchronously or asynchronously, using the Send or SendAsync methods, respectively.

To close the producer and release its resources, call the Close method on the producer instance.

### Example:

```go
brokers := []string{"localhost:9092"}
topic := "test-topic"

producer, err := NewProducer(brokers, topic, nil)
if err != nil {
  log.Fatalf("Failed to create producer: %v", err)
}

err = producer.Send(topic, []byte("key"), []byte("value"))
if err != nil {
  log.Fatalf("Failed to send message: %v", err)
}

err = producer.Close()
if err != nil {
  log.Fatalf("Failed to close producer: %v", err)
}
```
____________________
## Consumer
The consumer package provides an interface and implementation for consuming messages from a Kafka topic using the Sarama library.


### Consumer: the interface defines the following methods:

* Start() error: starts consuming messages from the Kafka topic.
* Messages() <-chan *sarama.ConsumerMessage: returns a channel that can be used to receive messages from the Kafka topic.
* Close() error: stops the consumer and releases any resources it holds.
* Closed() bool: returns true if the consumer has been closed.

### The `consumer` struct also includes the following methods:
* ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error: the method called by the consumer interface when a consumer group claim is created.
* Setup(_ sarama.ConsumerGroupSession) error: called at the beginning of a new session, before ConsumeClaim().
* Cleanup(_ sarama.ConsumerGroupSession) error: called at the end of a session, once all ConsumeClaim() goroutines have exited.
* Start() error: starts consuming messages from the Kafka topic.
* StartAsync(): starts consuming messages from the Kafka topic in a separate goroutine.
* StartSync(): starts consuming messages from the Kafka topic in the current goroutine.
* Messages() <-chan *sarama.ConsumerMessage: returns a channel of messages consumed from the Kafka topic.
* Close() error: closes the consumer.
* Closed() bool: returns true if the consumer is closed.
* Config : The Config struct is the configuration for creating a new consumer. It includes the following fields:

### Usage
To create a new consumer, call the NewConsumer function, passing in the list of Kafka brokers, the topic to consume messages from, and an optional configuration. If no configuration is provided, the default configuration values will be used.

The returned consumer instance can be used to start consuming messages from the specified Kafka topic using the Start method. The method starts a goroutine that consumes messages from the Kafka topic and passes them to the client through the channel returned by the Messages method.

To stop the consumer, call the Close method on the consumer instance. This stops the message consumption loop and releases any resources held by the consumer.

Here's an example usage of the consumer:

```go
package main

import (
	"fmt"
	"log"

	"github.com/mycompany/kafka/consumer"
)

func main() {
	// create a new Kafka consumer
	brokers := []string{"localhost:9092"}
	topic := "my-topic"
	config := &consumer.Config{
		Brokers: brokers,
		Topic:   topic,
	}
	consumer, err := consumer.NewConsumer(config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// start consuming messages
	err = consumer.Start()
	if err != nil {
		log.Fatalf("Failed to start consumer: %v", err)
	}

	// process incoming messages
	for msg := range consumer.Messages() {
		fmt.Printf("Received message: key=%s, value=%s\n", string(msg.Key), string(msg.Value))
	}
}
```

In this example, a new Kafka consumer is created with the given configuration, and then started using the Start method. Messages are consumed from the Kafka topic by reading from the channel returned by the Messages method. The Close method is deferred to ensure that the consumer is closed when the function retur