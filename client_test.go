package go_kafka

import (
	"go-kafka/client"
	"testing"
)

func TestNewKafka(t *testing.T) {
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
			name: "TestNewKafka",
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
				return
			}
			// if client is nil  , if producer is nil , if consumer is nil
			if got == nil || got.Producer() == nil || got.Consumer() == nil {
				t.Errorf("NewKafka() = %v, want %v", got, tt.want)
			}

			err = got.Ping()
			if err != nil {
				t.Errorf("NewKafka() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

		})
	}
}
