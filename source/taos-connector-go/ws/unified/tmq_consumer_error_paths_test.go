package unified

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commontmq "github.com/taosdata/driver-go/v3/common/tmq"
)

// --- NewTMQConsumer error paths ---

func TestNewTMQConsumerInvalidAutoCommitInterval(t *testing.T) {
	consumer, err := NewTMQConsumer(&commontmq.ConfigMap{
		"ws.url":                  "ws://127.0.0.1:6041",
		"auto.commit.interval.ms": "not_a_number",
	})
	require.Nil(t, consumer)
	var numErr *strconv.NumError
	require.ErrorAs(t, err, &numErr)
}

// --- parseTMQEndpoints error paths ---

func TestParseTMQEndpointsEmpty(t *testing.T) {
	_, err := parseTMQEndpoints("")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ws.url required")
}

func TestParseTMQEndpointsWhitespaceOnly(t *testing.T) {
	_, err := parseTMQEndpoints("   ")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ws.url required")
}

func TestParseTMQEndpointsAllEmptyItems(t *testing.T) {
	_, err := parseTMQEndpoints(",,,")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ws.url required")
}

func TestParseTMQEndpointsInvalidScheme(t *testing.T) {
	_, err := parseTMQEndpoints("http://127.0.0.1:6041")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid websocket endpoint scheme")
}

func TestParseTMQEndpointsMissingHost(t *testing.T) {
	_, err := parseTMQEndpoints("ws://")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid websocket endpoint")
}

func TestParseTMQEndpointsDedup(t *testing.T) {
	endpoints, err := parseTMQEndpoints("ws://a:6041,ws://a:6041,ws://b:6041")
	require.NoError(t, err)
	require.Len(t, endpoints, 2)
}

func TestParseTMQEndpointsNormalizesPath(t *testing.T) {
	endpoints, err := parseTMQEndpoints("ws://host:6041/whatever")
	require.NoError(t, err)
	require.Len(t, endpoints, 1)
	assert.Contains(t, endpoints[0], "/rest/tmq")
	assert.NotContains(t, endpoints[0], "whatever")
}

func TestParseTMQEndpointsSchemeCase(t *testing.T) {
	endpoints, err := parseTMQEndpoints("WS://host:6041")
	require.NoError(t, err)
	assert.Contains(t, endpoints[0], "ws://")

	endpoints, err = parseTMQEndpoints("WSS://host:6041")
	require.NoError(t, err)
	assert.Contains(t, endpoints[0], "wss://")
}

// --- ensureInitialized / nil consumer paths ---

func TestTMQConsumerNilEnsureInitialized(t *testing.T) {
	var c *TMQConsumer
	require.ErrorIs(t, c.ensureInitialized(), ErrTMQConsumerUninitialized)
}

func TestTMQConsumerNilClientEnsureInitialized(t *testing.T) {
	c := &TMQConsumer{}
	require.ErrorIs(t, c.ensureInitialized(), ErrTMQConsumerUninitialized)
}

func TestTMQConsumerNilClose(t *testing.T) {
	var c *TMQConsumer
	require.ErrorIs(t, c.Close(), ErrTMQConsumerUninitialized)
}

func TestTMQConsumerNilSubscribe(t *testing.T) {
	var c *TMQConsumer
	require.ErrorIs(t, c.Subscribe("t", nil), ErrTMQConsumerUninitialized)
}

func TestTMQConsumerNilSubscribeTopics(t *testing.T) {
	var c *TMQConsumer
	require.ErrorIs(t, c.SubscribeTopics([]string{"t"}, nil), ErrTMQConsumerUninitialized)
}

func TestTMQConsumerNilPoll(t *testing.T) {
	var c *TMQConsumer
	ev := c.Poll(100)
	require.NotNil(t, ev)
	tmqErr, ok := ev.(commontmq.Error)
	require.True(t, ok)
	assert.Contains(t, tmqErr.Error(), "not initialized")
}

func TestTMQConsumerNilCommit(t *testing.T) {
	var c *TMQConsumer
	_, err := c.Commit()
	require.ErrorIs(t, err, ErrTMQConsumerUninitialized)
}

func TestTMQConsumerNilUnsubscribe(t *testing.T) {
	var c *TMQConsumer
	require.ErrorIs(t, c.Unsubscribe(), ErrTMQConsumerUninitialized)
}

func TestTMQConsumerNilAssignment(t *testing.T) {
	var c *TMQConsumer
	_, err := c.Assignment()
	require.ErrorIs(t, err, ErrTMQConsumerUninitialized)
}

func TestTMQConsumerNilSeek(t *testing.T) {
	var c *TMQConsumer
	topic := "t"
	err := c.Seek(commontmq.TopicPartition{Topic: &topic}, 0)
	require.ErrorIs(t, err, ErrTMQConsumerUninitialized)
}

func TestTMQConsumerNilCommitted(t *testing.T) {
	var c *TMQConsumer
	_, err := c.Committed(nil, 0)
	require.ErrorIs(t, err, ErrTMQConsumerUninitialized)
}

func TestTMQConsumerNilCommitOffsets(t *testing.T) {
	var c *TMQConsumer
	_, err := c.CommitOffsets(nil)
	require.ErrorIs(t, err, ErrTMQConsumerUninitialized)
}

func TestTMQConsumerNilPosition(t *testing.T) {
	var c *TMQConsumer
	_, err := c.Position(nil)
	require.ErrorIs(t, err, ErrTMQConsumerUninitialized)
}

// --- getErr propagation ---

func TestTMQConsumerPollWithStoredError(t *testing.T) {
	c := &TMQConsumer{client: &Client{}}
	stored := errors.New("previous ws error")
	c.setErr(stored)

	ev := c.Poll(100)
	require.NotNil(t, ev)
	tmqErr, ok := ev.(commontmq.Error)
	require.True(t, ok)
	assert.Contains(t, tmqErr.Error(), "previous ws error")
}

func TestTMQConsumerDoSubscribeWithStoredError(t *testing.T) {
	c := &TMQConsumer{client: &Client{}}
	c.setErr(errors.New("conn broken"))
	err := c.doSubscribe([]string{"topic"}, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "conn broken")
}

func TestTMQConsumerUnsubscribeWithStoredError(t *testing.T) {
	c := &TMQConsumer{client: &Client{}}
	c.setErr(errors.New("conn broken"))
	err := c.Unsubscribe()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "conn broken")
}

func TestTMQConsumerDoCommitWithStoredError(t *testing.T) {
	c := &TMQConsumer{client: &Client{}}
	c.setErr(errors.New("conn broken"))
	err := c.doCommit()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "conn broken")
}

// --- isClosed / runtime on nil ---

func TestTMQConsumerIsClosedNil(t *testing.T) {
	var c *TMQConsumer
	assert.True(t, c.isClosed())
}

func TestTMQConsumerIsClosedNilClient(t *testing.T) {
	c := &TMQConsumer{}
	assert.True(t, c.isClosed())
}

func TestTMQConsumerRuntimeNil(t *testing.T) {
	var c *TMQConsumer
	assert.Nil(t, c.runtime())
}

func TestTMQConsumerRuntimeNilClient(t *testing.T) {
	c := &TMQConsumer{}
	assert.Nil(t, c.runtime())
}

// --- handleError ---

func TestTMQConsumerHandleErrorNoAutoReconnect(t *testing.T) {
	c := &TMQConsumer{autoReconnect: false}
	c.handleError(errors.New("connection reset"))
	err := c.getErr()
	require.NotNil(t, err)
	var wsErr *WSError
	require.True(t, errors.As(err, &wsErr))
	assert.Contains(t, wsErr.Error(), "connection reset")
}

func TestTMQConsumerHandleErrorWithAutoReconnect(t *testing.T) {
	c := &TMQConsumer{autoReconnect: true}
	c.handleError(errors.New("connection reset"))
	// autoReconnect=true means error is NOT stored
	assert.Nil(t, c.getErr())
}

// --- WSError ---

func TestWSErrorFormat(t *testing.T) {
	cause := errors.New("read timeout")
	wsErr := NewWSError(cause)
	assert.Contains(t, wsErr.Error(), "websocket close with error")
	assert.Contains(t, wsErr.Error(), "read timeout")
	assert.Same(t, cause, wsErr.Cause)
}

// --- tryScheduleAutoCommit ---

func TestTryScheduleAutoCommitFirstCall(t *testing.T) {
	c := &TMQConsumer{autoCommitInterval: 5 * time.Second}
	// First call initializes the timer, returns false.
	assert.False(t, c.tryScheduleAutoCommit(time.Now()))
}

func TestTryScheduleAutoCommitBeforeInterval(t *testing.T) {
	c := &TMQConsumer{autoCommitInterval: 5 * time.Second}
	now := time.Now()
	c.tryScheduleAutoCommit(now) // init

	// Call before interval elapses
	assert.False(t, c.tryScheduleAutoCommit(now.Add(3*time.Second)))
}

func TestTryScheduleAutoCommitAfterInterval(t *testing.T) {
	c := &TMQConsumer{autoCommitInterval: 5 * time.Second}
	now := time.Now()
	c.tryScheduleAutoCommit(now) // init

	// Call after interval elapses
	assert.True(t, c.tryScheduleAutoCommit(now.Add(6*time.Second)))
	// Next call before new interval should be false
	assert.False(t, c.tryScheduleAutoCommit(now.Add(7*time.Second)))
}

// --- topics state ---

func TestTMQConsumerTopicsSnapshot(t *testing.T) {
	c := &TMQConsumer{}
	// Initially empty
	assert.Nil(t, c.topicsSnapshot())

	c.setTopics([]string{"a", "b"})
	snap := c.topicsSnapshot()
	assert.Equal(t, []string{"a", "b"}, snap)

	// Mutation of snapshot does not affect internal state
	snap[0] = "x"
	assert.Equal(t, []string{"a", "b"}, c.topicsSnapshot())
}

// --- lastMessageID state ---

func TestTMQConsumerLastMessageID(t *testing.T) {
	c := &TMQConsumer{}
	assert.Equal(t, uint64(0), c.getLastMessageID())

	c.setLastMessageID(42)
	assert.Equal(t, uint64(42), c.getLastMessageID())
}

// --- clearErr ---

func TestTMQConsumerClearErr(t *testing.T) {
	c := &TMQConsumer{}
	c.setErr(errors.New("some error"))
	require.NotNil(t, c.getErr())

	c.clearErr()
	assert.Nil(t, c.getErr())
}

// --- sendTextWithClient returns ClosedErr when consumer is closed ---

func TestTMQConsumerSendTextWithClientClosed(t *testing.T) {
	cfg := NewConfig([]string{"ws://localhost:6041"})
	wsClient, err := NewClient(cfg, "/rest/tmq")
	require.NoError(t, err)
	wsClient.Close()

	c := &TMQConsumer{client: wsClient}
	_, _, sendErr := c.sendTextWithClient(1, nil, nil)
	require.Error(t, sendErr)
	assert.True(t, errors.Is(sendErr, ClosedErr))
}

// --- configMapToConfig: invalid timezone ---

func TestConfigMapToConfigInvalidTimezone(t *testing.T) {
	_, err := configMapToConfig(commontmq.ConfigMap{
		"ws.url":   "ws://127.0.0.1:6041",
		"timezone": "Not/A/Real/Zone",
	})
	require.Error(t, err)
}

// --- configMapToConfig: happy path with all options ---

func TestConfigMapToConfigHappyPath(t *testing.T) {
	cfg, err := configMapToConfig(commontmq.ConfigMap{
		"ws.url":                  "ws://127.0.0.1:6041",
		"td.connect.user":         "root",
		"td.connect.pass":         "taosdata",
		"group.id":                "g1",
		"client.id":               "c1",
		"auto.offset.reset":       "earliest",
		"enable.auto.commit":      "true",
		"auto.commit.interval.ms": "1000",
		"msg.with.table.name":     "true",
		"custom.option":           "custom_value",
	})
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, "root", cfg.User)
	assert.Equal(t, "taosdata", cfg.Password)
	assert.Equal(t, "g1", cfg.GroupID)
	assert.Equal(t, "c1", cfg.ClientID)
	assert.Equal(t, "earliest", cfg.OffsetRest)
	assert.Equal(t, "true", cfg.AutoCommit)
	assert.Equal(t, "1000", cfg.AutoCommitIntervalMS)
	assert.Equal(t, "true", cfg.WithTableName)
	assert.Equal(t, "custom_value", cfg.OtherOptions["custom.option"])
}
