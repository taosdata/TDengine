package unified

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	commontmq "github.com/taosdata/driver-go/v3/common/tmq"
)

// TestTMQConfigMapToConfigWrong verifies the expected behavior for this scenario.
func TestTMQConfigMapToConfigWrong(t *testing.T) {
	type args struct {
		m commontmq.ConfigMap
	}
	tests := []struct {
		name    string
		args    args
		wantErr string
	}{
		{
			name: "url",
			args: args{
				m: commontmq.ConfigMap{
					"ws.url": 123,
				},
			},
			wantErr: "ws.url expects type string, not int",
		},
		{
			name: "empty url",
			args: args{
				m: commontmq.ConfigMap{
					"ws.url": "",
				},
			},
			wantErr: "ws.url required",
		},
		{
			name: "channelLen",
			args: args{
				m: commontmq.ConfigMap{
					"ws.url":                "ws://127.0.0.1:6041",
					"ws.message.channelLen": "not a uint",
				},
			},
			wantErr: "ws.message.channelLen expects type uint, not string",
		},
		{
			name: "ws.message.timeout",
			args: args{
				m: commontmq.ConfigMap{
					"ws.url":             "ws://127.0.0.1:6041",
					"ws.message.timeout": "xx",
				},
			},
			wantErr: "ws.message.timeout expects type time.Duration, not string",
		},
		{
			name: "ws.message.writeWait",
			args: args{
				m: commontmq.ConfigMap{
					"ws.url":               "ws://127.0.0.1:6041",
					"ws.message.writeWait": "xx",
				},
			},
			wantErr: "ws.message.writeWait expects type time.Duration, not string",
		},
		{
			name: "td.connect.user",
			args: args{
				m: commontmq.ConfigMap{
					"ws.url":          "ws://127.0.0.1:6041",
					"td.connect.user": 123,
				},
			},
			wantErr: "td.connect.user expects type string, not int",
		},
		{
			name: "td.connect.pass",
			args: args{
				m: commontmq.ConfigMap{
					"ws.url":          "ws://127.0.0.1:6041",
					"td.connect.pass": 123,
				},
			},
			wantErr: "td.connect.pass expects type string, not int",
		},
		{
			name: "group.id",
			args: args{
				m: commontmq.ConfigMap{
					"ws.url":   "ws://127.0.0.1:6041",
					"group.id": 123,
				},
			},
			wantErr: "group.id expects type string, not int",
		},
		{
			name: "client.id",
			args: args{
				m: commontmq.ConfigMap{
					"ws.url":    "ws://127.0.0.1:6041",
					"client.id": 123,
				},
			},
			wantErr: "client.id expects type string, not int",
		},
		{
			name: "auto.offset.reset",
			args: args{
				m: commontmq.ConfigMap{
					"ws.url":            "ws://127.0.0.1:6041",
					"auto.offset.reset": 123,
				},
			},
			wantErr: "auto.offset.reset expects type string, not int",
		},
		{
			name: "enable.auto.commit",
			args: args{
				m: commontmq.ConfigMap{
					"ws.url":             "ws://127.0.0.1:6041",
					"enable.auto.commit": 123,
				},
			},
			wantErr: "enable.auto.commit expects type string, not int",
		},
		{
			name: "auto.commit.interval.ms",
			args: args{
				m: commontmq.ConfigMap{
					"ws.url":                  "ws://127.0.0.1:6041",
					"auto.commit.interval.ms": 123,
				},
			},
			wantErr: "auto.commit.interval.ms expects type string, not int",
		},
		{
			name: "experimental.snapshot.enable",
			args: args{
				m: commontmq.ConfigMap{
					"ws.url":                       "ws://127.0.0.1:6041",
					"experimental.snapshot.enable": 123,
				},
			},
			wantErr: "experimental.snapshot.enable expects type string, not int",
		},
		{
			name: "msg.with.table.name",
			args: args{
				m: commontmq.ConfigMap{
					"ws.url":              "ws://127.0.0.1:6041",
					"msg.with.table.name": 123,
				},
			},
			wantErr: "msg.with.table.name expects type string, not int",
		},
		{
			name: "ws.message.enableCompression",
			args: args{
				m: commontmq.ConfigMap{
					"ws.url":                       "ws://127.0.0.1:6041",
					"ws.message.enableCompression": 123,
				},
			},
			wantErr: "ws.message.enableCompression expects type bool, not int",
		},
		{
			name: "ws.message.timeout < 1s",
			args: args{
				m: commontmq.ConfigMap{
					"ws.url":             "ws://127.0.0.1:6041",
					"ws.message.timeout": time.Millisecond,
				},
			},
			wantErr: "ws.message.timeout cannot be less than 1 second",
		},
		{
			name: "ws.message.writeWait < 1s",
			args: args{
				m: commontmq.ConfigMap{
					"ws.url":               "ws://127.0.0.1:6041",
					"ws.message.writeWait": time.Millisecond,
				},
			},
			wantErr: "ws.message.writeWait cannot be less than 1 second",
		},
		{
			name: "ws.autoReconnect",
			args: args{
				m: commontmq.ConfigMap{
					"ws.url":           "ws://127.0.0.1:6041",
					"ws.autoReconnect": 123,
				},
			},
			wantErr: "ws.autoReconnect expects type bool, not int",
		},
		{
			name: "ws.reconnectIntervalMs",
			args: args{
				m: commontmq.ConfigMap{
					"ws.url":                 "ws://127.0.0.1:6041",
					"ws.reconnectIntervalMs": "not int",
				},
			},
			wantErr: "ws.reconnectIntervalMs expects type int, not string",
		},
		{
			name: "ws.reconnectRetryCount",
			args: args{
				m: commontmq.ConfigMap{
					"ws.url":                 "ws://127.0.0.1:6041",
					"ws.reconnectRetryCount": "not int",
				},
			},
			wantErr: "ws.reconnectRetryCount expects type int, not string",
		},
		{
			name: "session.timeout.ms",
			args: args{
				m: commontmq.ConfigMap{
					"ws.url":             "ws://127.0.0.1:6041",
					"session.timeout.ms": 123,
				},
			},
			wantErr: "session.timeout.ms expects type string, not int",
		},
		{
			name: "max.poll.interval.ms",
			args: args{
				m: commontmq.ConfigMap{
					"ws.url":               "ws://127.0.0.1:6041",
					"max.poll.interval.ms": 123,
				},
			},
			wantErr: "max.poll.interval.ms expects type string, not int",
		},
		{
			name: "expect string value",
			args: args{
				m: commontmq.ConfigMap{
					"ws.url":        "ws://127.0.0.1:6041",
					"min.poll.rows": 123,
				},
			},
			wantErr: "config min.poll.rows value must be string",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := configMapToConfig(tt.args.m)
			assert.Nil(t, got)
			if assert.Error(t, err) {
				assert.Equal(t, tt.wantErr, err.Error())
			}
		})
	}
}
