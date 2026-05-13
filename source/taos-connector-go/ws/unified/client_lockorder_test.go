package unified

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestLockOrderDeadlock tests that there's no deadlock between c.lock and pendingLock
func TestLockOrderDeadlock(t *testing.T) {
	cfg := NewConfig([]string{"ws://localhost:6041"})
	cfg.User = "root"
	cfg.Passwd = "taosdata"

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	err = c.Connect()
	require.NoError(t, err)

	// Simulate concurrent operations that could cause deadlock
	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	// Goroutine 1: Continuously add and remove pending requests
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopChan:
					return
				default:
					respChan := make(chan []byte, 1)
					pendingReq := &pendingRequest{
						reqID:   uint64(time.Now().UnixNano()),
						channel: respChan,
					}

					c.pendingLock.Lock()
					c.pendingRequests[pendingReq.reqID] = pendingReq
					c.pendingLock.Unlock()

					// Call runtimeClient() while holding pendingLock conceptually
					runtime := c.runtimeClient()
					_ = runtime

					c.pendingLock.Lock()
					delete(c.pendingRequests, pendingReq.reqID)
					c.pendingLock.Unlock()

					time.Sleep(1 * time.Millisecond)
				}
			}
		}()
	}

	// Goroutine 2: Continuously trigger reconnects (which call swapRuntime)
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
					time.Sleep(50 * time.Millisecond)
				}
			}
		}()
	}

	// Run for 1 second - if there's a deadlock, test will timeout
	time.Sleep(1 * time.Second)
	close(stopChan)
	wg.Wait()

	// If we reach here, no deadlock occurred
	t.Log("No deadlock detected")
}

// TestSwapRuntimeRaceCondition tests swapRuntime under concurrent access
func TestSwapRuntimeRaceCondition(t *testing.T) {
	cfg := NewConfig([]string{"ws://localhost:6041"})
	cfg.User = "root"
	cfg.Passwd = "taosdata"

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	err = c.Connect()
	require.NoError(t, err)

	var wg sync.WaitGroup

	// Add some pending requests
	for i := 0; i < 10; i++ {
		respChan := make(chan []byte, 1)
		pendingReq := &pendingRequest{
			reqID:   uint64(i),
			channel: respChan,
		}
		c.pendingLock.Lock()
		c.pendingRequests[pendingReq.reqID] = pendingReq
		c.pendingLock.Unlock()
	}

	// Goroutine 1: Try to swap runtime
	wg.Add(1)
	go func() {
		defer wg.Done()
		runtime := c.runtimeClient()
		if runtime != nil {
			_ = c.reconnectWithBootstrap(c.defaultBootstrap, runtime)
		}
	}()

	// Goroutine 2: Try to access runtime and pending requests
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			runtime := c.runtimeClient()
			_ = runtime

			c.pendingLock.RLock()
			count := len(c.pendingRequests)
			c.pendingLock.RUnlock()
			_ = count

			time.Sleep(1 * time.Millisecond)
		}
	}()

	wg.Wait()
	t.Log("No race condition detected")
}

// TestReconnectLockOrder tests that reconnectWithBootstrap doesn't cause lock order issues
func TestReconnectLockOrder(t *testing.T) {
	cfg := NewConfig([]string{"ws://localhost:6041"})
	cfg.User = "root"
	cfg.Passwd = "taosdata"

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	err = c.Connect()
	require.NoError(t, err)

	var wg sync.WaitGroup

	// Goroutine 1: Call reconnectWithBootstrap
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runtime := c.runtimeClient()
			_ = c.reconnectWithBootstrap(c.defaultBootstrap, runtime)
		}()
	}

	// Goroutine 2: Continuously access runtimeClient()
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				runtime := c.runtimeClient()
				if runtime != nil {
					_ = runtime.IsRunning()
				}
				time.Sleep(1 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
	t.Log("No lock order issue detected")
}
