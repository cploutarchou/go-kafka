package client

import (
	"testing"
)

func TestNewKafka(t *testing.T) {
	type args struct {
		config Config
	}
	tests := []struct {
		name    string
		args    args
		want    Kafka
		wantErr bool
	}{
		{
			name: "TestNewKafka",
			args: args{
				config: Config{
					Brokers: []string{"localhost:9092"},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewKafka(tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewKafka() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// if client is nil  , if producer is nil , if consumer is nil
			if got == nil || got.Producer() == nil || got.Consumer() == nil {
				t.Errorf("NewKafka() = %v, want %v", got, tt.want)
			}
		})
	}
}
