package producer

import (
	"fmt"
	"go-kafka/types"
	"testing"
)

func TestNewProducer(t *testing.T) {

	type args struct {
		brokers []string
		topic   string
		config  *types.WriterConfig
	}
	tests := []struct {
		name    string
		args    args
		want    Producer
		wantErr bool
	}{
		{
			name: "TestNewProducer",
			args: args{
				brokers: []string{"192.168.88.50:9092"},
				topic:   "test-22",
				config: &types.WriterConfig{
					Addr:            []string{"192.168.88.50:9092"},
					Topic:           "test-22",
					Balancer:        nil,
					MaxAttempts:     10,
					WriteBackoffMin: 50,
					WriteBackoffMax: 5,
					BatchSize:       100,
					BatchBytes:      1024 * 1024,
					BatchTimeout:    1,
					ReadTimeout:     10,
					WriteTimeout:    10,
					RequiredAcks:    1,
					Async:           true,
					Compression:     nil,
					Logger:          nil,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewProducer(tt.args.brokers, tt.args.topic, tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewProducer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == nil {
				t.Errorf("NewProducer() got = %v, want %v", got, tt.want)
				return
			}

			if err := got.Close(); err != nil {
				t.Errorf("NewProducer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

		})
	}
}

func TestImpl_Send(t *testing.T) {
	type args struct {
		brokers []string
		topic   string
		config  *types.WriterConfig
	}
	tests := []struct {
		name    string
		args    args
		want    Producer
		wantErr bool
	}{
		{
			name: "TestNewProducer",
			args: args{
				brokers: []string{"192.168.88.50:9092"},
				topic:   "test-22",
				config: &types.WriterConfig{
					Addr:                   []string{"192.168.88.50:9092"},
					Topic:                  "test-22",
					Balancer:               nil,
					MaxAttempts:            10,
					WriteBackoffMin:        50,
					WriteBackoffMax:        5,
					BatchSize:              100,
					BatchBytes:             1024 * 1024,
					BatchTimeout:           1,
					ReadTimeout:            10,
					WriteTimeout:           10,
					RequiredAcks:           1,
					Async:                  true,
					Compression:            nil,
					Logger:                 nil,
					AllowAutoTopicCreation: true,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewProducer(tt.args.brokers, tt.args.topic, tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewProducer() error = %v, wantErr %v", err, tt.wantErr)
			}
			// send 100 message in loop to test
			for i := 0; i < 100; i++ {
				err := got.Send(tt.args.topic, []byte(fmt.Sprintf("test message %d", i)), []byte(fmt.Sprintf("test message %d", i)))
				fmt.Println("send message ", i)
				if err != nil {
					t.Errorf("NewProducer() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
			}
			if err := got.Close(); err != nil {
				t.Errorf("NewProducer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			fmt.Println("send success")
		})
	}
}
