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