package unified

import "testing"

func resetGlobalConnCounterForTest(t *testing.T) {
	t.Helper()
	globalHostPortConnCounts.reset()
	t.Cleanup(func() {
		globalHostPortConnCounts.reset()
	})
}

func hostPortKeyForEndpointForTest(t *testing.T, endpoint string) string {
	t.Helper()
	key, err := endpointHostPortKey(endpoint)
	if err != nil {
		t.Fatalf("failed to parse endpoint %q: %v", endpoint, err)
	}
	return key
}

func addEndpointConnCountForTest(t *testing.T, endpoint string, times int) {
	t.Helper()
	key := hostPortKeyForEndpointForTest(t, endpoint)
	for i := 0; i < times; i++ {
		globalHostPortConnCounts.inc(key)
	}
}

func endpointConnCountForTest(t *testing.T, endpoint string) int64 {
	t.Helper()
	key := hostPortKeyForEndpointForTest(t, endpoint)
	return globalHostPortConnCounts.get(key)
}
