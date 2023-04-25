package go_kafka

import (
	"encoding/json"
	"go-kafka/client"
	"testing"
)

func TestSyncProducer(t *testing.T) {
	type args struct {
		config client.Config
	}
	tests := []struct {
		name    string
		args    args
		want    client.Kafka
		wantErr bool
	}{
		{
			name: "TestNewProducer",
			args: args{
				config: client.Config{
					Brokers: []string{"192.168.0.1:9092"},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := client.NewKafka(tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewKafka() error = %v, wantErr %v", err, tt.wantErr)
			}
			// if client is nil  , if producer is nil , if consumer is nil
			if got == nil || got.Producer() == nil {
				t.Errorf("NewKafka() = %v, want %v", got, tt.want)
			}

			// test producer
			type TestMessage struct {
				Data    string `json:"data"`
				Address string `json:"address"`
			}
			var testMessage TestMessage
			testMessage.Data = "test"
			testMessage.Address = "test"

			data, err := json.Marshal(testMessage)
			if err != nil {
				t.Errorf("NewKafka() error = %v, wantErr %v", err, tt.wantErr)
			}

			err = got.Producer().Send("test1", []byte("test"), data)
			if err != nil {
				t.Errorf("NewKafka() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestASyncProducer(t *testing.T) {
	type args struct {
		config client.Config
	}
	tests := []struct {
		name    string
		args    args
		want    client.Kafka
		wantErr bool
	}{
		{
			name: "TestNewProducer",
			args: args{
				config: client.Config{
					Brokers: []string{"192.168.0.1:9092"},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := client.NewKafka(tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewKafka() error = %v, wantErr %v", err, tt.wantErr)
			}
			// if client is nil  , if producer is nil , if consumer is nil
			if got == nil || got.Producer() == nil {
				t.Errorf("NewKafka() = %v, want %v", got, tt.want)
			}

			// test producer
			type TestMessage struct {
				Data    string `json:"data"`
				Address string `json:"address"`
			}
			var testMessage TestMessage
			testMessage.Data = "test"
			testMessage.Address = "test"

			data, err := json.Marshal(testMessage)
			if err != nil {
				t.Errorf("NewKafka() error = %v, wantErr %v", err, tt.wantErr)
			}

			err = got.Producer().SendAsync("test1", []byte("test"), data)
			if err != nil {
				t.Errorf("NewKafka() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
