package consumer

import (
	producer2 "go-kafka/producer"
	"testing"
	"time"
)

func TestNewConsumer(t *testing.T) {
	type args struct {
		config *Config
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "TestNewConsumer",
			args: args{
				config: &Config{
					Brokers: []string{"127.0.0.1:9092"},
					Topic:   "test",
					GroupID: "test",
				},
			},
			wantErr: false,
		}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewConsumer(tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewConsumer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == nil {
				t.Errorf("NewConsumer() got = %v", got)
				return
			}
			if err = got.Start(); err != nil {
				t.Errorf("NewConsumer() error = %v", err)
				return
			}

			producer, err := producer2.NewProducer(tt.args.config.Brokers, tt.args.config.Topic, nil)
			if err != nil {
				t.Errorf("NewConsumer() error creating producer = %v", err)
				return
			}

			// Send message to kafka for test consumer receive message from kafka
			// produce messages to topic (asynchronously)
			go func() {
				for {
					err := producer.Send("test", []byte("test"), []byte("test"))
					if err != nil {
						t.Errorf("NewConsumer() error = %v, wantErr %v", err, tt.wantErr)
						return
					}
				}
			}()

			// Consume messages from topic (asynchronously)
			timeout := time.After(60 * time.Second)
			received := false
			for {
				select {
				case msg := <-got.Messages():
					t.Logf("NewConsumer() msg = %v", string(msg.Value))
					if string(msg.Value) == "test" {
						received = true
					}
				case <-timeout:
					t.Errorf("timeout while waiting for message")
					return
				}
				if received {
					break
				}
			}

			// Close consumer
			if err = got.Close(); err != nil {
				t.Errorf("NewConsumer() error = %v", err)
				return
			}

			// Close producer
			if err = producer.Close(); err != nil {
				t.Errorf("NewConsumer() error = %v", err)
				return
			}

		})
	}
}
