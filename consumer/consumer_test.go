package consumer

import (
	"log"
	"os"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/cploutarchou/gokafka/producer"
	"github.com/stretchr/testify/assert"

	"testing"
	"time"
)

func TestConsumer(t *testing.T) {
	// create a test topic and start a producer
	topic := "test_topic"
	brokers := []string{"192.168.88.50:9092"}
	p, err := producer.NewProducer(brokers, topic, nil)
	assert.NoError(t, err)

	// create a mew standard  logger
	logger := log.New(os.Stderr, "", log.LstdFlags)
	// star a consumer
	cfg := &Config{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  "test_consumer",
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  120 * time.Second,
		Logger:   logger,
	}
	c, err := NewConsumer(cfg)
	assert.NoError(t, err)

	// start consuming messages
	err = c.Start()
	assert.NoError(t, err)

	// produce messages to the topic
	numMessages := 10
	var wg sync.WaitGroup
	for i := 0; i < numMessages; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value := []byte("test message")
			err := p.Send(topic, []byte("key"), value)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	// verify that the consumer receives all the messages
	receivedMessages := make([]*sarama.ConsumerMessage, 0, numMessages)
	timeout := time.After(120 * time.Second)
	for len(receivedMessages) < numMessages {
		select {
		case msg := <-c.Messages():
			receivedMessages = append(receivedMessages, msg)
		case <-timeout:
			assert.FailNow(t, "timed out while waiting for messages")
		}
	}

	// verify that the received messages match the send messages
	for _, msg := range receivedMessages {
		assert.Equal(t, "test message", string(msg.Value))
	}

	// clean up
	err = c.Close()
	assert.NoError(t, err)
	err = p.Close()
	assert.NoError(t, err)
}
