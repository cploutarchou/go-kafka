package producer

import (
	"fmt"

	"testing"
	"time"

	"github.com/cploutarchou/gokafka/types"
)

func TestNewProducer(t *testing.T) {

	type args struct {
		config *types.Config
	}
	tests := []struct {
		name    string
		args    args
		want    Producer
		wantErr bool
	}{
		{
			name:    "TestNewProducer",
			args:    args{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewProducer([]string{"192.168.88.50:9092"}, "test", tt.args.config)
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
		config *types.Config
	}
	tests := []struct {
		name    string
		args    args
		want    Producer
		wantErr bool
	}{
		{
			name:    "TestNewProducer",
			args:    args{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewProducer([]string{"192.168.88.50:9092"}, "test", tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewProducer() error = %v, wantErr %v", err, tt.wantErr)
			}
			time.Sleep(5 * time.Second)
			// send 100 message in loop to test
			for i := 0; i < 10; i++ {
				err := got.Send("test", []byte(fmt.Sprintf("test message %d", i)), []byte(fmt.Sprintf("test message %d", i)))
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

func TestImpl_AsyncSend(t *testing.T) {
	type args struct {
		config *types.Config
	}
	tests := []struct {
		name    string
		args    args
		want    Producer
		wantErr bool
	}{
		{
			name:    "TestNewProducer",
			args:    args{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewProducer([]string{"192.168.88.50:9092"}, "test", tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewProducer() error = %v, wantErr %v", err, tt.wantErr)
			}
			time.Sleep(5 * time.Second)
			// send 100 message in loop to test
			for i := 0; i < 100; i++ {
				err := got.SendAsync("test", []byte(fmt.Sprintf("test message %d", i)), []byte(fmt.Sprintf("test message %d", i)))
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
