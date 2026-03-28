package schemaless

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified"
)

func TestMapUnifiedError(t *testing.T) {
	assert.NoError(t, mapUnifiedError(nil))
	assert.Equal(t, SchemalessClosedErr, mapUnifiedError(unified.ErrUnifiedClosed))
	assert.Equal(t, SchemalessClosedErr, mapUnifiedError(client.ClosedError))

	connectTimeoutErr := &unified.Error{
		Type:    unified.ErrorTypeConnectTimeout,
		Message: "connect timeout",
	}
	assert.Equal(t, ConnectTimeoutErr, mapUnifiedError(connectTimeoutErr))

	other := errors.New("other")
	assert.Equal(t, other, mapUnifiedError(other))
}
