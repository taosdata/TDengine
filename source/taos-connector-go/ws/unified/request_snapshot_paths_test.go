package unified

import (
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/ws/client"
)

func closedRuntimeForSnapshotPathTest() *client.Client {
	runtime := client.NewClient(nil, 1)
	runtime.Close()
	return runtime
}

func envelopeForSnapshotPathTest() *client.Envelope {
	envelope := client.GlobalEnvelopePool.Get()
	envelope.Type = websocket.TextMessage
	envelope.Msg.Reset()
	_, _ = envelope.Msg.WriteString("{}")
	return envelope
}

// TestSendEnvelopeWithRuntimeSnapshotFastPath verifies the expected behavior for this scenario.
func TestSendEnvelopeWithRuntimeSnapshotFastPath(t *testing.T) {
	cfg := NewConfig([]string{"ws://localhost:6041"})
	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	runtime := closedRuntimeForSnapshotPathTest()
	c.lock.Lock()
	c.runtime = runtime
	c.runtimeGen = 11
	c.publishRuntimeSnapshotLocked()
	c.lock.Unlock()

	reqID := uint64(1001)
	envelope := envelopeForSnapshotPathTest()
	defer client.GlobalEnvelopePool.Put(envelope)

	_, acked, runtimeGen, err := c.sendEnvelopeWithRuntime(runtime, reqID, envelope, 0, nil)
	require.ErrorIs(t, err, client.ClosedError)
	require.False(t, acked)
	require.Equal(t, uint64(11), runtimeGen)
	require.False(t, pendingRequestExistsForTest(c, reqID))
}

// TestSendEnvelopeWithRuntimeSnapshotFallbackPath verifies the expected behavior for this scenario.
func TestSendEnvelopeWithRuntimeSnapshotFallbackPath(t *testing.T) {
	c := &Client{
		config: Config{ReadTimeout: 1},
	}
	runtime := closedRuntimeForSnapshotPathTest()
	c.lock.Lock()
	c.runtime = runtime
	c.runtimeGen = 22
	c.lock.Unlock()

	reqID := uint64(1002)
	envelope := envelopeForSnapshotPathTest()
	defer client.GlobalEnvelopePool.Put(envelope)

	_, acked, runtimeGen, err := c.sendEnvelopeWithRuntime(runtime, reqID, envelope, 0, nil)
	require.ErrorIs(t, err, client.ClosedError)
	require.False(t, acked)
	require.Equal(t, uint64(22), runtimeGen)
	require.False(t, pendingRequestExistsForTest(c, reqID))
}

// TestSendEnvelopeNoResponseSnapshotFastPath verifies the expected behavior for this scenario.
func TestSendEnvelopeNoResponseSnapshotFastPath(t *testing.T) {
	cfg := NewConfig([]string{"ws://localhost:6041"})
	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	runtime := closedRuntimeForSnapshotPathTest()
	c.lock.Lock()
	c.runtime = runtime
	c.runtimeGen = 33
	c.publishRuntimeSnapshotLocked()
	c.lock.Unlock()

	envelope := envelopeForSnapshotPathTest()
	defer client.GlobalEnvelopePool.Put(envelope)

	err = c.sendEnvelopeNoResponse(runtime, envelope)
	require.ErrorIs(t, err, client.ClosedError)
}

// TestSendEnvelopeNoResponseSnapshotFallbackPath verifies the expected behavior for this scenario.
func TestSendEnvelopeNoResponseSnapshotFallbackPath(t *testing.T) {
	c := &Client{
		config: Config{ReadTimeout: 1},
	}
	runtime := closedRuntimeForSnapshotPathTest()
	c.lock.Lock()
	c.runtime = runtime
	c.runtimeGen = 44
	c.lock.Unlock()

	envelope := envelopeForSnapshotPathTest()
	defer client.GlobalEnvelopePool.Put(envelope)

	err := c.sendEnvelopeNoResponse(runtime, envelope)
	require.ErrorIs(t, err, client.ClosedError)
}
