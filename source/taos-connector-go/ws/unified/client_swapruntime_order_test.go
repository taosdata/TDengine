package unified

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/ws/client"
)

// TestSwapRuntimeDoesNotChangeActiveOnOldHostPortLookupError verifies active endpoint is unchanged on early validation failure.
func TestSwapRuntimeDoesNotChangeActiveOnOldHostPortLookupError(t *testing.T) {
	c := &Client{
		failover: &failoverState{
			endpoints:         []string{"ws://a:1/ws", "ws://b:2/ws"},
			endpointHostPorts: []string{"a:1"},
			activeIndex:       1,
		},
		pendingRequests: make(map[uint64]*pendingRequest),
		closeChan:       make(chan struct{}),
	}
	existing := client.NewClient(nil, 1)
	defer existing.Close()
	c.runtime = existing

	next := client.NewClient(nil, 1)
	defer next.Close()

	oldRuntime, err := c.swapRuntime(next, 0)
	require.Nil(t, oldRuntime)
	require.ErrorIs(t, err, ErrInvalidEndpointIndex)
	require.Equal(t, 1, c.failover.active().Index)
	require.Same(t, existing, c.runtime)
}
