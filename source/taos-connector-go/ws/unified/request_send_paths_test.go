package unified

import (
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/ws/client"
)

func newClientWithRuntimeForRequestPathTest(t *testing.T) (*Client, *client.Client, uint64) {
	t.Helper()

	cfg := NewConfig([]string{"ws://127.0.0.1:6041"})
	cfg.ReadTimeout = 100 * time.Millisecond

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)

	runtime := client.NewClient(nil, 1)
	runtimeGen := uint64(77)

	c.lock.Lock()
	c.runtime = runtime
	c.runtimeGen = runtimeGen
	c.publishRuntimeSnapshotLocked()
	c.lock.Unlock()

	return c, runtime, runtimeGen
}

func newAckedEnvelopeForRequestPathTest() *client.Envelope {
	envelope := client.GlobalEnvelopePool.Get()
	envelope.Type = websocket.TextMessage
	envelope.Msg.Reset()
	_, _ = envelope.Msg.WriteString(`{"req_id":1}`)
	// Pre-ack write so tests can focus on request routing/timeouts without write pump dependency.
	envelope.ErrorChan <- nil
	return envelope
}

func waitPendingRegistrationForRequestPathTest(c *Client, reqID uint64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pendingRequestExistsForTest(c, reqID) {
			return true
		}
		time.Sleep(100 * time.Microsecond)
	}
	return false
}

// TestSendEnvelopeWithRuntimeTimeout verifies timeout branch and pending cleanup.
func TestSendEnvelopeWithRuntimeTimeout(t *testing.T) {
	c, runtime, runtimeGen := newClientWithRuntimeForRequestPathTest(t)
	defer c.Close()

	envelope := newAckedEnvelopeForRequestPathTest()
	defer client.GlobalEnvelopePool.Put(envelope)

	reqID := uint64(1001)
	timeoutErr := &Error{
		Type:              ErrorTypeMessageTimeout,
		Message:           "custom request timeout",
		ConnectionRelated: true,
	}

	resp, acked, gotRuntimeGen, err := c.sendEnvelopeWithRuntime(runtime, reqID, envelope, 5*time.Millisecond, timeoutErr)
	require.ErrorIs(t, err, timeoutErr)
	require.Nil(t, resp)
	require.True(t, acked)
	require.Equal(t, runtimeGen, gotRuntimeGen)
	require.False(t, pendingRequestExistsForTest(c, reqID))
}

// TestSendEnvelopeWithRuntimeRuntimeClosed verifies runtime.Done branch when no response is routed.
func TestSendEnvelopeWithRuntimeRuntimeClosed(t *testing.T) {
	c, runtime, runtimeGen := newClientWithRuntimeForRequestPathTest(t)
	defer c.Close()

	envelope := newAckedEnvelopeForRequestPathTest()
	defer client.GlobalEnvelopePool.Put(envelope)

	reqID := uint64(1002)
	go func() {
		if !waitPendingRegistrationForRequestPathTest(c, reqID, time.Second) {
			return
		}
		runtime.Close()
	}()

	resp, acked, gotRuntimeGen, err := c.sendEnvelopeWithRuntime(runtime, reqID, envelope, time.Second, nil)
	require.ErrorIs(t, err, client.ClosedError)
	require.Nil(t, resp)
	require.True(t, acked)
	require.Equal(t, runtimeGen, gotRuntimeGen)
	require.False(t, pendingRequestExistsForTest(c, reqID))
}

// TestSendEnvelopeWithRuntimePrefersResponseWhenRuntimeClosed verifies response wins even if runtime closes concurrently.
func TestSendEnvelopeWithRuntimePrefersResponseWhenRuntimeClosed(t *testing.T) {
	c, runtime, runtimeGen := newClientWithRuntimeForRequestPathTest(t)
	defer c.Close()

	envelope := newAckedEnvelopeForRequestPathTest()
	defer client.GlobalEnvelopePool.Put(envelope)

	reqID := uint64(1003)
	want := []byte(`{"code":0,"req_id":1003}`)

	go func() {
		if !waitPendingRegistrationForRequestPathTest(c, reqID, time.Second) {
			return
		}
		c.handleMessage(want, reqID)
		runtime.Close()
	}()

	resp, acked, gotRuntimeGen, err := c.sendEnvelopeWithRuntime(runtime, reqID, envelope, time.Second, nil)
	require.NoError(t, err)
	require.Equal(t, want, resp)
	require.True(t, acked)
	require.Equal(t, runtimeGen, gotRuntimeGen)
	require.False(t, pendingRequestExistsForTest(c, reqID))
}
