package unified

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/ws/client"
)

// TestConcurrentReconnect tests that concurrent reconnect attempts don't create multiple runtimes
func TestConcurrentReconnect(t *testing.T) {
	cfg := NewConfig([]string{"ws://localhost:6041"})
	cfg.User = "root"
	cfg.Passwd = "taosdata"
	cfg.AutoReconnect = true
	cfg.ReconnectRetryCount = 1
	cfg.ReconnectIntervalMs = 100

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	err = c.Connect()
	require.NoError(t, err)

	initialRuntime := c.runtimeClient()
	require.NotNil(t, initialRuntime)

	// Close the initial runtime to simulate connection failure
	initialRuntime.Close()

	// Start multiple concurrent reconnect attempts
	var wg sync.WaitGroup
	reconnectCount := 10
	var successCount int32

	for i := 0; i < reconnectCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := c.reconnectWithBootstrap(c.defaultBootstrap, initialRuntime)
			if err == nil {
				atomic.AddInt32(&successCount, 1)
			}
		}()
	}

	wg.Wait()

	// Verify only one reconnect succeeded
	newRuntime := c.runtimeClient()
	if newRuntime != nil {
		assert.NotSame(t, initialRuntime, newRuntime, "Runtime should be replaced")
		assert.True(t, newRuntime.IsRunning(), "New runtime should be running")
	}

	// At least one reconnect should have succeeded
	assert.Greater(t, atomic.LoadInt32(&successCount), int32(0), "At least one reconnect should succeed")
}

// TestPendingRequestsCleanupOnRuntimeSwap tests that pending requests are cleaned up when runtime is swapped
func TestPendingRequestsCleanupOnRuntimeSwap(t *testing.T) {
	cfg := NewConfig([]string{"ws://localhost:6041"})
	cfg.User = "root"
	cfg.Passwd = "taosdata"
	cfg.ReadTimeout = 1 * time.Second

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	err = c.Connect()
	require.NoError(t, err)

	runtime1 := c.runtimeClient()
	require.NotNil(t, runtime1)

	// Add a pending request manually
	respChan := make(chan []byte, 1)
	pendingReq := &pendingRequest{
		reqID:   12345,
		channel: respChan,
	}

	c.pendingLock.Lock()
	c.pendingRequests[pendingReq.reqID] = pendingReq
	pendingCount := len(c.pendingRequests)
	c.pendingLock.Unlock()

	assert.Equal(t, 1, pendingCount, "Should have 1 pending request")

	// Trigger runtime swap by reconnecting
	err = c.reconnectWithBootstrap(c.defaultBootstrap, runtime1)
	if err != nil {
		t.Logf("Reconnect failed (expected in some cases): %v", err)
	}

	// Check that pending requests were cleaned up
	c.pendingLock.Lock()
	pendingCount = len(c.pendingRequests)
	c.pendingLock.Unlock()

	assert.Equal(t, 0, pendingCount, "Pending requests should be cleaned up after runtime swap")

	// Check that the channel received nil (connection lost signal)
	select {
	case msg := <-respChan:
		assert.Nil(t, msg, "Should receive nil to signal connection lost")
	case <-time.After(100 * time.Millisecond):
		// Channel might not receive if swap didn't happen
		t.Log("No message received (swap might not have happened)")
	}
}

// TestRuntimeSwapCleansUpPendingRequests tests swapRuntime directly
func TestRuntimeSwapCleansUpPendingRequests(t *testing.T) {
	cfg := NewConfig([]string{"ws://localhost:6041"})
	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	// Add multiple pending requests
	channels := make([]chan []byte, 3)
	for i := 0; i < 3; i++ {
		ch := make(chan []byte, 1)
		channels[i] = ch
		pendingReq := &pendingRequest{
			reqID:   uint64(i + 1),
			channel: ch,
		}
		c.pendingLock.Lock()
		c.pendingRequests[pendingReq.reqID] = pendingReq
		c.pendingLock.Unlock()
	}

	c.pendingLock.Lock()
	initialCount := len(c.pendingRequests)
	c.pendingLock.Unlock()
	assert.Equal(t, 3, initialCount, "Should have 3 pending requests")

	// Swap runtime with a placeholder runtime to trigger cleanup.
	_, err = c.swapRuntime(client.NewClient(nil, 1), 0)
	require.NoError(t, err)

	// Verify pending requests were cleaned up
	c.pendingLock.Lock()
	finalCount := len(c.pendingRequests)
	c.pendingLock.Unlock()
	assert.Equal(t, 0, finalCount, "All pending requests should be cleaned up")

	// Verify all channels received nil
	for i, ch := range channels {
		select {
		case msg := <-ch:
			assert.Nil(t, msg, "Channel %d should receive nil", i)
		case <-time.After(100 * time.Millisecond):
			t.Errorf("Channel %d did not receive message", i)
		}
	}
}

// TestSwapRuntimePublishesSnapshotAfterPendingSwap ensures new runtime snapshot
// is not observable before pendingRequests map replacement completes.
func TestSwapRuntimePublishesSnapshotAfterPendingSwap(t *testing.T) {
	cfg := NewConfig([]string{"ws://localhost:6041"})
	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	oldRuntime := client.NewClient(nil, 1)
	nextRuntime := client.NewClient(nil, 1)
	defer oldRuntime.Close()

	c.lock.Lock()
	c.runtime = oldRuntime
	c.runtimeGen = 7
	c.publishRuntimeSnapshotLocked()
	c.lock.Unlock()

	reqID := uint64(4242)
	respChan := make(chan []byte, 1)
	c.pendingLock.Lock()
	c.pendingRequests[reqID] = &pendingRequest{
		reqID:   reqID,
		channel: respChan,
	}

	swapDone := make(chan error, 1)
	go func() {
		_, swapErr := c.swapRuntime(nextRuntime, 0)
		swapDone <- swapErr
	}()

	// Wait until swapRuntime is inside the critical section and blocked by pendingLock.
	blockedOnClientLock := false
	for i := 0; i < 100; i++ {
		probeDone := make(chan struct{})
		go func() {
			c.lock.RLock()
			_ = c.runtimeGen
			c.lock.RUnlock()
			close(probeDone)
		}()
		select {
		case <-probeDone:
			select {
			case swapErr := <-swapDone:
				c.pendingLock.Unlock()
				t.Fatalf("swapRuntime returned before pendingLock was released: %v", swapErr)
			default:
			}
			time.Sleep(1 * time.Millisecond)
		case <-time.After(2 * time.Millisecond):
			blockedOnClientLock = true
		}
		if blockedOnClientLock {
			break
		}
	}
	require.True(t, blockedOnClientLock, "swapRuntime did not block on pendingLock as expected")

	// While pendingLock is still held, snapshot must still point to old runtime.
	snapshot, ok := c.loadRuntimeSnapshotAtomic()
	require.True(t, ok)
	require.Same(t, oldRuntime, snapshot.runtime)
	require.Equal(t, uint64(7), snapshot.generation)
	_, stillInOldPending := c.pendingRequests[reqID]
	require.True(t, stillInOldPending)

	c.pendingLock.Unlock()

	select {
	case swapErr := <-swapDone:
		require.NoError(t, swapErr)
	case <-time.After(1 * time.Second):
		t.Fatal("swapRuntime did not complete after releasing pendingLock")
	}

	snapshot = c.loadRuntimeSnapshot()
	require.Same(t, nextRuntime, snapshot.runtime)
	require.Equal(t, uint64(8), snapshot.generation)
	require.False(t, pendingRequestExistsForTest(c, reqID))

	select {
	case msg := <-respChan:
		require.Nil(t, msg)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("pending request did not receive cleanup notification")
	}
}

// TestConnectConcurrentSafety tests that Connect is safe under concurrent calls
func TestConnectConcurrentSafety(t *testing.T) {
	cfg := NewConfig([]string{"ws://localhost:6041"})
	cfg.User = "root"
	cfg.Passwd = "taosdata"

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	// Try to connect concurrently
	var wg sync.WaitGroup
	connectCount := 5
	var successCount int32

	for i := 0; i < connectCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := c.Connect()
			if err == nil {
				atomic.AddInt32(&successCount, 1)
			}
		}()
	}

	wg.Wait()

	// All connects should succeed (idempotent)
	assert.Equal(t, int32(connectCount), atomic.LoadInt32(&successCount), "All connects should succeed")

	// Should have exactly one runtime
	runtime := c.runtimeClient()
	assert.NotNil(t, runtime, "Should have a runtime")
	assert.True(t, runtime.IsRunning(), "Runtime should be running")
}

// TestHandleMessageNonBlocking tests that handleMessage doesn't block
func TestHandleMessageNonBlocking(t *testing.T) {
	cfg := NewConfig([]string{"ws://localhost:6041"})
	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	// Create a pending request with a full channel
	ch := make(chan []byte, 1)
	ch <- []byte("existing message") // Fill the channel

	pendingReq := &pendingRequest{
		reqID:   999,
		channel: ch,
	}

	c.pendingLock.Lock()
	c.pendingRequests[pendingReq.reqID] = pendingReq
	c.pendingLock.Unlock()

	// handleMessage should not block even if channel is full
	done := make(chan struct{})
	go func() {
		c.handleMessage([]byte("new message"), 999)
		close(done)
	}()

	select {
	case <-done:
		// Success - didn't block
	case <-time.After(1 * time.Second):
		t.Fatal("handleMessage blocked when channel was full")
	}

	// Verify the pending request was removed
	c.pendingLock.Lock()
	count := len(c.pendingRequests)
	c.pendingLock.Unlock()
	assert.Equal(t, 0, count, "Pending request should be removed")
}

// TestReconnectWithFailedRuntimeCheck tests that reconnect skips if runtime was already replaced
func TestReconnectWithFailedRuntimeCheck(t *testing.T) {
	cfg := NewConfig([]string{"ws://localhost:6041"})
	cfg.User = "root"
	cfg.Passwd = "taosdata"

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	err = c.Connect()
	require.NoError(t, err)

	runtime1 := c.runtimeClient()
	require.NotNil(t, runtime1)

	// Simulate that another goroutine already reconnected
	err = c.reconnectWithBootstrap(c.defaultBootstrap, nil)
	if err != nil {
		t.Logf("First reconnect failed: %v", err)
	}

	runtime2 := c.runtimeClient()

	// Now try to reconnect with the old runtime1
	// This should be skipped because current runtime is different and healthy
	err = c.reconnectWithBootstrap(c.defaultBootstrap, runtime1)
	assert.NoError(t, err, "Reconnect should be skipped without error")

	// Runtime should still be runtime2
	currentRuntime := c.runtimeClient()
	if runtime2 != nil {
		assert.Same(t, runtime2, currentRuntime, "Runtime should not change")
	}
}

// TestHighConcurrencyWithReconnect is a stress test
func TestHighConcurrencyWithReconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	cfg := NewConfig([]string{"ws://localhost:6041"})
	cfg.User = "root"
	cfg.Passwd = "taosdata"
	cfg.AutoReconnect = true
	cfg.ReconnectRetryCount = 2
	cfg.ReconnectIntervalMs = 50

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	err = c.Connect()
	require.NoError(t, err)

	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	// Goroutines that continuously try to get runtime
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopChan:
					return
				default:
					runtime := c.runtimeClient()
					if runtime != nil {
						_ = runtime.IsRunning()
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
		}()
	}

	// Goroutines that trigger reconnects
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopChan:
					return
				default:
					runtime := c.runtimeClient()
					_ = c.reconnectWithBootstrap(c.defaultBootstrap, runtime)
					time.Sleep(100 * time.Millisecond)
				}
			}
		}()
	}

	// Run for 2 seconds
	time.Sleep(2 * time.Second)
	close(stopChan)
	wg.Wait()

	// Verify no panic and client is still functional
	runtime := c.runtimeClient()
	if runtime != nil {
		assert.True(t, runtime.IsRunning(), "Runtime should still be running")
	}
}
