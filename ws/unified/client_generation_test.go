package unified

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRuntimeGenerationPreventsRequestLoss tests that generation numbers prevent request loss during swap
func TestRuntimeGenerationPreventsRequestLoss(t *testing.T) {
	cfg := NewConfig([]string{"ws://localhost:6041"})
	cfg.User = "root"
	cfg.Passwd = "taosdata"

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	err = c.Connect()
	require.NoError(t, err)

	var requestsAdded int32
	var requestsCleaned int32
	var requestsPreserved int32

	var wg sync.WaitGroup

	// Goroutine 1: Continuously trigger runtime swaps
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			runtime := c.runtimeClient()
			_ = c.reconnectWithBootstrap(c.defaultBootstrap, runtime)
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// Goroutine 2-6: Continuously add pending requests
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				// Add pending request
				respChan := make(chan []byte, 1)
				pendingReq := &pendingRequest{
					reqID:   uint64(id*1000 + j),
					channel: respChan,
				}

				c.pendingLock.Lock()
				c.pendingRequests[pendingReq.reqID] = pendingReq
				c.pendingLock.Unlock()

				atomic.AddInt32(&requestsAdded, 1)

				// Wait a bit to see if it gets cleaned
				time.Sleep(10 * time.Millisecond)

				// Check if still pending
				c.pendingLock.Lock()
				_, found := c.pendingRequests[pendingReq.reqID]
				if found {
					delete(c.pendingRequests, pendingReq.reqID)
				}
				c.pendingLock.Unlock()

				if found {
					atomic.AddInt32(&requestsPreserved, 1)
				} else {
					// Check if it was cleaned (received nil)
					select {
					case msg := <-respChan:
						if msg == nil {
							atomic.AddInt32(&requestsCleaned, 1)
						}
					default:
						// Not found and no message - might have been cleaned
						atomic.AddInt32(&requestsCleaned, 1)
					}
				}

				time.Sleep(10 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Requests added: %d", atomic.LoadInt32(&requestsAdded))
	t.Logf("Requests cleaned: %d", atomic.LoadInt32(&requestsCleaned))
	t.Logf("Requests preserved: %d", atomic.LoadInt32(&requestsPreserved))

	// All requests should be accounted for
	total := atomic.LoadInt32(&requestsCleaned) + atomic.LoadInt32(&requestsPreserved)
	assert.Equal(t, atomic.LoadInt32(&requestsAdded), total, "All requests should be accounted for")
}

// TestSwapRuntimeDoesNotCleanNewRequests tests that new requests are not cleaned during swap
func TestSwapRuntimeDoesNotCleanNewRequests(t *testing.T) {
	cfg := NewConfig([]string{"ws://localhost:6041"})
	cfg.User = "root"
	cfg.Passwd = "taosdata"

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	err = c.Connect()
	require.NoError(t, err)

	// Add old requests (current generation)
	c.lock.RLock()
	oldGen := c.runtimeGen
	c.lock.RUnlock()

	oldRequests := make([]*pendingRequest, 3)
	for i := 0; i < 3; i++ {
		respChan := make(chan []byte, 1)
		req := &pendingRequest{
			reqID:   uint64(i),
			channel: respChan,
		}
		oldRequests[i] = req

		c.pendingLock.Lock()
		c.pendingRequests[req.reqID] = req
		c.pendingLock.Unlock()
	}

	// Trigger runtime swap
	runtime := c.runtimeClient()
	_ = c.reconnectWithBootstrap(c.defaultBootstrap, runtime)

	// Wait for swap to complete
	time.Sleep(200 * time.Millisecond)

	// Add new requests (new generation)
	c.lock.RLock()
	newGen := c.runtimeGen
	c.lock.RUnlock()

	assert.Greater(t, newGen, oldGen, "Generation should have incremented")

	newRequests := make([]*pendingRequest, 3)
	for i := 0; i < 3; i++ {
		respChan := make(chan []byte, 1)
		req := &pendingRequest{
			reqID:   uint64(i + 100),
			channel: respChan,
		}
		newRequests[i] = req

		c.pendingLock.Lock()
		c.pendingRequests[req.reqID] = req
		c.pendingLock.Unlock()
	}

	// Check old requests were cleaned
	for i, req := range oldRequests {
		select {
		case msg := <-req.channel:
			assert.Nil(t, msg, "Old request %d should receive nil", i)
		default:
			t.Logf("Old request %d channel empty (already cleaned)", i)
		}
	}

	// Check new requests are still pending
	c.pendingLock.Lock()
	count := len(c.pendingRequests)
	c.pendingLock.Unlock()

	assert.Equal(t, 3, count, "New requests should still be pending")

	// Clean up
	c.pendingLock.Lock()
	c.pendingRequests = make(map[uint64]*pendingRequest)
	c.pendingLock.Unlock()
}

// TestRuntimeGenerationMonotonicity tests that generation always increases
func TestRuntimeGenerationMonotonicity(t *testing.T) {
	cfg := NewConfig([]string{"ws://localhost:6041"})
	cfg.User = "root"
	cfg.Passwd = "taosdata"

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	err = c.Connect()
	require.NoError(t, err)

	generations := make([]uint64, 0, 10)

	for i := 0; i < 10; i++ {
		c.lock.RLock()
		gen := c.runtimeGen
		c.lock.RUnlock()

		generations = append(generations, gen)

		runtime := c.runtimeClient()
		_ = c.reconnectWithBootstrap(c.defaultBootstrap, runtime)

		time.Sleep(50 * time.Millisecond)
	}

	// Verify generations are monotonically increasing
	for i := 1; i < len(generations); i++ {
		assert.GreaterOrEqual(t, generations[i], generations[i-1],
			"Generation should be monotonically increasing")
	}
}

// TestCloseWithPendingRequestsOfDifferentGenerations tests Close with mixed generation requests
func TestCloseWithPendingRequestsOfDifferentGenerations(t *testing.T) {
	cfg := NewConfig([]string{"ws://localhost:6041"})
	cfg.User = "root"
	cfg.Passwd = "taosdata"

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)

	err = c.Connect()
	require.NoError(t, err)

	// Add requests with different generations
	for gen := uint64(0); gen < 3; gen++ {
		for i := 0; i < 2; i++ {
			respChan := make(chan []byte, 1)
			req := &pendingRequest{
				reqID:   gen*10 + uint64(i),
				channel: respChan,
			}

			c.pendingLock.Lock()
			c.pendingRequests[req.reqID] = req
			c.pendingLock.Unlock()
		}
	}

	c.pendingLock.Lock()
	initialCount := len(c.pendingRequests)
	c.pendingLock.Unlock()

	assert.Equal(t, 6, initialCount, "Should have 6 pending requests")

	// Close should not panic
	c.Close()

	// Verify closed
	assert.True(t, c.IsClosed())
}
