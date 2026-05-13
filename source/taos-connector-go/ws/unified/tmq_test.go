package unified

import (
	"testing"

	"github.com/stretchr/testify/require"
	commontmq "github.com/taosdata/driver-go/v3/common/tmq"
)

// TestNewTMQConsumerNilConfig verifies the expected behavior for this scenario.
func TestNewTMQConsumerNilConfig(t *testing.T) {
	consumer, err := NewTMQConsumer(nil)
	require.ErrorIs(t, err, ErrNilConfig)
	require.Nil(t, consumer)
}

// TestNewTMQConsumerConfigValidationError verifies the expected behavior for this scenario.
func TestNewTMQConsumerConfigValidationError(t *testing.T) {
	cfg := commontmq.ConfigMap{}
	consumer, err := NewTMQConsumer(&cfg)
	require.EqualError(t, err, "ws.url required")
	require.Nil(t, consumer)
}

// TestTMQConsumerNilReceiver verifies the expected behavior for this scenario.
func TestTMQConsumerNilReceiver(t *testing.T) {
	var consumer *TMQConsumer
	require.ErrorIs(t, consumer.Close(), ErrTMQConsumerUninitialized)
	ev := consumer.Poll(100)
	require.NotNil(t, ev)
}

// TestTMQParseEndpointsFromURLList verifies the expected behavior for this scenario.
func TestTMQParseEndpointsFromURLList(t *testing.T) {
	endpoints, err := parseTMQEndpoints("ws://127.0.0.1:6041, ws://127.0.0.1:6042/ws?token=abc, ws://127.0.0.1:6041")
	require.NoError(t, err)
	require.Equal(t, []string{
		"ws://127.0.0.1:6041/rest/tmq",
		"ws://127.0.0.1:6042/rest/tmq?token=abc",
	}, endpoints)
}

// TestTMQConfigMapParsesMultipleEndpoints verifies the expected behavior for this scenario.
func TestTMQConfigMapParsesMultipleEndpoints(t *testing.T) {
	cfg, err := configMapToConfig(commontmq.ConfigMap{
		"ws.url": "ws://127.0.0.1:6041,ws://127.0.0.1:6042",
	})
	require.NoError(t, err)
	require.Equal(t, []string{
		"ws://127.0.0.1:6041/rest/tmq",
		"ws://127.0.0.1:6042/rest/tmq",
	}, cfg.Endpoints)
	require.Equal(t, "ws://127.0.0.1:6041/rest/tmq", cfg.Url)
}

// TestBuildTMQTimeoutMessageRedactsSensitiveArgs verifies timeout message keeps context while masking secrets.
func TestBuildTMQTimeoutMessageRedactsSensitiveArgs(t *testing.T) {
	args := []byte(`{
		"user":"root",
		"password":"raw-pass",
		"config":{
			"td.connect.pass":"raw-td-pass",
			"api_token":"raw-token",
			"safe":"ok"
		},
		"endpoint":"ws://127.0.0.1:6041/rest/tmq?token=raw-query-token&x=1"
	}`)
	message := buildTMQTimeoutMessage("subscribe", 12345, args)

	require.Contains(t, message, "tmq message timeout")
	require.Contains(t, message, "action=subscribe")
	require.Contains(t, message, "req_id=12345")
	require.Contains(t, message, `"password":"***"`)
	require.Contains(t, message, `"td.connect.pass":"***"`)
	require.Contains(t, message, `"api_token":"***"`)
	require.Contains(t, message, `"safe":"ok"`)
	require.NotContains(t, message, "raw-pass")
	require.NotContains(t, message, "raw-td-pass")
	require.NotContains(t, message, "raw-token")
	require.NotContains(t, message, "raw-query-token")
}

// TestBuildTMQTimeoutMessageHandlesInvalidJSONArgs verifies malformed args are handled without leaking payload.
func TestBuildTMQTimeoutMessageHandlesInvalidJSONArgs(t *testing.T) {
	message := buildTMQTimeoutMessage("poll", 9, []byte(`{"bad"`))
	require.Contains(t, message, "action=poll")
	require.Contains(t, message, "req_id=9")
	require.Contains(t, message, "<invalid_json")
}
