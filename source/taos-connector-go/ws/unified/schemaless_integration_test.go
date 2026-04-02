package unified

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/ws/client"
)

// TestSchemalessWithNilRuntime tests sending with nil runtime (no connection)
func TestSchemalessWithNilRuntime(t *testing.T) {
	cfg := NewConfig([]string{"ws://localhost:6041"})
	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	// Don't connect, runtime should be nil
	err = c.SchemalessInsert(1, "measurement,host=host1 field1=2i 1577837300000", 1, "ms", 0, "")
	assert.Equal(t, client.ClosedError, err)
}

// TestSchemalessInsertAfterCloseDoesNotReconnect tests that insert after close returns error immediately
func TestSchemalessInsertAfterCloseDoesNotReconnect(t *testing.T) {
	cfg := NewConfig([]string{"ws://localhost:6041"})
	cfg.User = "root"
	cfg.Passwd = "taosdata"
	cfg.AutoReconnect = true
	cfg.ReconnectRetryCount = 1

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)

	err = c.Connect()
	require.NoError(t, err)

	c.Close()

	err = c.SchemalessInsert(1, "measurement,host=host1 field1=2i 1577837300000", 1, "ms", 0, "")
	assert.Equal(t, ErrUnifiedClosed, err)
	assert.NotContains(t, err.Error(), "reconnect failed")
}

// TestSchemalessRuntimeStability tests that runtime remains stable during operations
func TestSchemalessRuntimeStability(t *testing.T) {
	cfg := NewConfig([]string{"ws://localhost:6041"})
	cfg.User = "root"
	cfg.Passwd = "taosdata"
	cfg.ReadTimeout = 5 * time.Second

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	err = c.Connect()
	require.NoError(t, err)

	runtime1 := c.runtimeClient()
	require.NotNil(t, runtime1)

	// Multiple operations should use the same runtime
	for i := 0; i < 3; i++ {
		runtime := c.runtimeClient()
		assert.Same(t, runtime1, runtime, "Runtime should remain stable across operations")
	}
}

// TestSchemalessClosedClientBehavior tests behavior when client is closed
func TestSchemalessClosedClientBehavior(t *testing.T) {
	cfg := NewConfig([]string{"ws://localhost:6041"})
	cfg.User = "root"
	cfg.Passwd = "taosdata"

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)

	err = c.Connect()
	require.NoError(t, err)

	// Close the client
	c.Close()

	// Verify client is closed
	assert.True(t, c.IsClosed())

	// Runtime should be nil after close
	runtime := c.runtimeClient()
	assert.Nil(t, runtime)

	// Operations should fail with ErrUnifiedClosed
	err = c.SchemalessInsert(1, "measurement,host=host1 field1=2i 1577837300000", 1, "ms", 0, "")
	assert.Equal(t, ErrUnifiedClosed, err)
}
