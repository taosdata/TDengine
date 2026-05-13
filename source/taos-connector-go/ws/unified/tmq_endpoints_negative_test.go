package unified

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestParseTMQEndpointsInvalidInputs verifies invalid endpoint formats are rejected.
func TestParseTMQEndpointsInvalidInputs(t *testing.T) {
	tests := []struct {
		name        string
		raw         string
		errContains string
	}{
		{
			name:        "empty",
			raw:         " , , ",
			errContains: "ws.url required",
		},
		{
			name:        "invalid scheme",
			raw:         "http://127.0.0.1:6041",
			errContains: "invalid websocket endpoint scheme",
		},
		{
			name:        "missing host",
			raw:         "ws:///rest/tmq",
			errContains: "invalid websocket endpoint",
		},
		{
			name:        "invalid url syntax",
			raw:         "ws://127.0.0.1:6041%%%/x",
			errContains: "%",
		},
	}

	for i := 0; i < len(tests); i++ {
		tc := tests[i]
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseTMQEndpoints(tc.raw)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errContains)
		})
	}
}
