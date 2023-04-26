package go_kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"go-kafka/client"
	"go-kafka/producer"
	"testing"
	"time"
)

func TestKafkaConsumer(t *testing.T) {
	type args struct {
		config client.Config
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "TestNewConsumer",
			args: args{
				config: client.Config{
					Brokers: []string{"192.168.88.50:9092"},
					Topic:   "test1",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kafka, err := client.NewKafka(tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewKafka() error = %v, wantErr %v", err, tt.wantErr)
			}
			if kafka == nil || kafka.Producer() == nil || kafka.Consumer() == nil {
				t.Errorf("NewKafka() = %v, want non-nil", kafka)
			}

			err = kafka.Ping()
			if err != nil {
				t.Errorf("Ping() error = %v, want nil", err)
				return
			}

			// Create a new stream object
			stream, err := kafka.Consumer().Stream()
			if err != nil {
				t.Errorf("Stream() error = %v, want nil", err)
				return
			}

			// Subscribe to the test topic
			err = kafka.Consumer().Subscribe([]string{"test-topic"})
			if err != nil {
				t.Errorf("Subscribe() error = %v, want nil", err)
				return
			}

			// Send a test message to the Kafka topic
			msg := &producer.Message{
				Topic: "test-topic",
				Key:   "test-key",
				Value: []byte("hello world"),
			}
			err = kafka.Producer().SendMsg(msg)
			if err != nil {
				t.Errorf("SendMsg() error = %v, want nil", err)
				return
			}

			// Wait for the message to be consumed by the consumer
			var message *sarama.ConsumerMessage
			select {
			case <-time.After(5 * time.Second):
				t.Errorf("Expected message not received")
				return
			case message = <-stream.Messages():
				if string(message.Value) != "hello world" {
					t.Errorf("Received message does not match expected value")
					return
				}
				// Convert the consumed message to a *producer.Message object
				consumedMsg := &producer.Message{
					Topic: message.Topic,
					Key:   string(message.Key),
					Value: message.Value,
				}
				// Print the consumed message to the console
				fmt.Printf("Consumed message: topic=%s, key=%s, value=%s\n", consumedMsg.Topic, consumedMsg.Key, string(consumedMsg.Value))
			}

			// Close the stream
			err = stream.Close()
			if err != nil {
				t.Errorf("Close() error = %v, want nil", err)
				return
			}
		})
	}
}
