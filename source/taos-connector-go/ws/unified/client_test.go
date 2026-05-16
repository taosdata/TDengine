package unified

import (
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/taosdata/driver-go/v3/ws/client"
)

// TestNewClientNormalizesEndpoints verifies the expected behavior for this scenario.
func TestNewClientNormalizesEndpoints(t *testing.T) {
	c, err := NewClient(NewConfig([]string{"ws://127.0.0.1:6041"}), "/ws")
	if err != nil {
		t.Fatal(err)
	}
	cfg := c.Config()
	if len(cfg.Endpoints) != 1 || cfg.Endpoints[0] != "ws://127.0.0.1:6041/ws" {
		t.Fatalf("unexpected endpoints: %+v", cfg.Endpoints)
	}
}

// TestClientConnectFailoverToNextEndpoint verifies the expected behavior for this scenario.
func TestClientConnectFailoverToNextEndpoint(t *testing.T) {
	resetGlobalConnCounterForTest(t)
	cfg := NewConfig([]string{"ws://a:1", "ws://b:2"})
	attempts := make([]string, 0, 2)
	c, err := NewClient(cfg, "/ws",
		WithDialFunc(func(endpoint string) (*websocket.Conn, error) {
			attempts = append(attempts, endpoint)
			if endpoint == "ws://a:1/ws" {
				return nil, newInvalidStateErrorf("dial failed")
			}
			return nil, nil
		}),
		WithClientFactory(func(_ *websocket.Conn, chanLength uint) *client.Client {
			return client.NewClient(nil, chanLength)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	err = c.connectWithBootstrap(nil)
	if err != nil {
		t.Fatal(err)
	}

	wantAttempts := []string{"ws://a:1/ws", "ws://b:2/ws"}
	if len(attempts) != len(wantAttempts) {
		t.Fatalf("unexpected attempt order: %v, want %v", attempts, wantAttempts)
	}
	for i := 0; i < len(wantAttempts); i++ {
		if attempts[i] != wantAttempts[i] {
			t.Fatalf("unexpected attempt order: %v, want %v", attempts, wantAttempts)
		}
	}

	// Active endpoint should be b:2 after failover from a:1.
	active := c.failover.active()
	if active.Index != 1 || active.URL != "ws://b:2/ws" {
		t.Fatalf("unexpected active endpoint: %+v", active)
	}
}

// TestClientReconnectTriesActiveEndpointFirstThenFallsBack verifies the expected behavior for this scenario.
func TestClientReconnectTriesActiveEndpointFirstThenFallsBack(t *testing.T) {
	resetGlobalConnCounterForTest(t)
	cfg := NewConfig([]string{"ws://a:1", "ws://b:2", "ws://c:3"})
	attempts := make([]string, 0, 4)
	var failActiveOnReconnect int32
	c, err := NewClient(cfg, "/ws",
		WithDialFunc(func(endpoint string) (*websocket.Conn, error) {
			attempts = append(attempts, endpoint)
			if atomic.LoadInt32(&failActiveOnReconnect) == 1 && endpoint == "ws://a:1/ws" {
				return nil, newInvalidStateErrorf("forced active endpoint failure")
			}
			return nil, nil
		}),
		WithClientFactory(func(_ *websocket.Conn, chanLength uint) *client.Client {
			return client.NewClient(nil, chanLength)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err = c.connectWithBootstrap(nil); err != nil {
		t.Fatal(err)
	}

	// Get the active endpoint after first connect
	active := c.failover.active()
	firstConnectEndpoint := active.URL

	reconnectAttemptStart := len(attempts)
	atomic.StoreInt32(&failActiveOnReconnect, 1)
	if err = c.reconnectWithBootstrap(nil, nil); err != nil {
		t.Fatal(err)
	}

	if len(attempts) < reconnectAttemptStart+2 {
		t.Fatalf("unexpected attempts: %v", attempts)
	}

	// First connect should match the active endpoint.
	if attempts[0] != firstConnectEndpoint {
		t.Fatalf("first connect endpoint %s doesn't match active %s", attempts[0], firstConnectEndpoint)
	}

	// Reconnect should try active endpoint first to tolerate transient network glitches.
	if attempts[reconnectAttemptStart] != "ws://a:1/ws" {
		t.Fatalf("unexpected first reconnect attempt: %s", attempts[reconnectAttemptStart])
	}

	// After active endpoint fails, reconnect should fallback by least-connection order.
	if attempts[reconnectAttemptStart+1] != "ws://b:2/ws" {
		t.Fatalf("unexpected fallback reconnect endpoint: %s", attempts[reconnectAttemptStart+1])
	}

	// Active endpoint should have changed after reconnect.
	activeAfterReconnect := c.failover.active()
	if activeAfterReconnect.URL != "ws://b:2/ws" {
		t.Fatalf("active endpoint should be ws://b:2/ws after reconnect, got: %s", activeAfterReconnect.URL)
	}
}

// TestClientHostPortConnectionCountLifecycle verifies the expected behavior for this scenario.
func TestClientHostPortConnectionCountLifecycle(t *testing.T) {
	resetGlobalConnCounterForTest(t)
	cfg := NewConfig([]string{"ws://a:1", "ws://b:2"})
	c, err := NewClient(cfg, "/ws",
		WithDialFunc(func(endpoint string) (*websocket.Conn, error) {
			return nil, nil
		}),
		WithClientFactory(func(_ *websocket.Conn, chanLength uint) *client.Client {
			return client.NewClient(nil, chanLength)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	if err = c.connectWithBootstrap(nil); err != nil {
		t.Fatal(err)
	}
	if got := endpointConnCountForTest(t, "ws://a:1/ws"); got != 1 {
		t.Fatalf("unexpected a:1 count after first connect, got %d", got)
	}
	if got := endpointConnCountForTest(t, "ws://b:2/ws"); got != 0 {
		t.Fatalf("unexpected b:2 count after first connect, got %d", got)
	}

	if err = c.reconnectWithBootstrap(nil, nil); err != nil {
		t.Fatal(err)
	}
	if got := endpointConnCountForTest(t, "ws://a:1/ws"); got != 1 {
		t.Fatalf("unexpected a:1 count after reconnect, got %d", got)
	}
	if got := endpointConnCountForTest(t, "ws://b:2/ws"); got != 0 {
		t.Fatalf("unexpected b:2 count after reconnect, got %d", got)
	}

	c.Close()
	if got := endpointConnCountForTest(t, "ws://a:1/ws"); got != 0 {
		t.Fatalf("unexpected a:1 count after close, got %d", got)
	}
}

// TestClientCloseRejectsConnect verifies the expected behavior for this scenario.
func TestClientCloseRejectsConnect(t *testing.T) {
	cfg := NewConfig([]string{"ws://a:1"})
	c, err := NewClient(cfg, "/ws",
		WithDialFunc(func(endpoint string) (*websocket.Conn, error) {
			return nil, nil
		}),
		WithClientFactory(func(_ *websocket.Conn, chanLength uint) *client.Client {
			return client.NewClient(nil, chanLength)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	if err = c.connectWithBootstrap(nil); err == nil {
		t.Fatal("expect close error")
	}
}

// TestNewClientFromDSN verifies the expected behavior for this scenario.
func TestNewClientFromDSN(t *testing.T) {
	c, err := NewClientFromDSN("user:passwd@ws(127.0.0.1:6041)/db?token=abc", "/ws")
	if err != nil {
		t.Fatal(err)
	}
	cfg := c.Config()
	if len(cfg.Endpoints) != 1 || cfg.Endpoints[0] != "ws://127.0.0.1:6041/ws?token=abc" {
		t.Fatalf("unexpected endpoints: %+v", cfg.Endpoints)
	}
	if cfg.User != "user" || cfg.Passwd != "passwd" || cfg.DbName != "db" {
		t.Fatalf("unexpected cfg auth/db: %+v", cfg)
	}
}

// TestClearPendingRequestsNotifiesWaiters verifies the expected behavior for this scenario.
func TestClearPendingRequestsNotifiesWaiters(t *testing.T) {
	c := &Client{}
	waiters := []*pendingRequest{
		{reqID: 1, channel: make(chan []byte, 1)},
		{reqID: 34, channel: make(chan []byte, 1)},
	}
	for i := 0; i < len(waiters); i++ {
		registerPendingRequestForTest(c, waiters[i])
	}

	clearPendingRequestsForTest(c)

	for i := 0; i < len(waiters); i++ {
		select {
		case msg := <-waiters[i].channel:
			if msg != nil {
				t.Fatalf("waiter %d should receive nil, got %v", i, msg)
			}
		default:
			t.Fatalf("waiter %d did not receive cleanup notification", i)
		}
		if pendingRequestExistsForTest(c, waiters[i].reqID) {
			t.Fatalf("pending request %d still exists after cleanup", waiters[i].reqID)
		}
	}
}

// TestCloseNotifiesPendingWaiters verifies Close drains pending requests and notifies waiters immediately.
func TestCloseNotifiesPendingWaiters(t *testing.T) {
	c := &Client{
		pendingRequests: make(map[uint64]*pendingRequest),
		closeChan:       make(chan struct{}),
	}
	waiters := []*pendingRequest{
		{reqID: 11, channel: make(chan []byte, 1)},
		{reqID: 22, channel: make(chan []byte, 1)},
	}
	for i := 0; i < len(waiters); i++ {
		registerPendingRequestForTest(c, waiters[i])
	}

	c.Close()

	for i := 0; i < len(waiters); i++ {
		select {
		case msg := <-waiters[i].channel:
			if msg != nil {
				t.Fatalf("waiter %d should receive nil, got %v", i, msg)
			}
		default:
			t.Fatalf("waiter %d did not receive close notification", i)
		}
		if pendingRequestExistsForTest(c, waiters[i].reqID) {
			t.Fatalf("pending request %d still exists after Close", waiters[i].reqID)
		}
	}

	select {
	case <-c.closeChan:
	default:
		t.Fatal("closeChan should be closed after Close")
	}
}

// TestRemovePendingRequestExpectedPointerMatch verifies pointer identity protects
// against removing a different request with the same req_id.
func TestRemovePendingRequestExpectedPointerMatch(t *testing.T) {
	c := &Client{
		pendingRequests: make(map[uint64]*pendingRequest),
	}
	reqID := uint64(77)
	registered := &pendingRequest{reqID: reqID, channel: make(chan []byte, 1)}
	other := &pendingRequest{reqID: reqID, channel: make(chan []byte, 1)}
	c.pendingRequests[reqID] = registered

	if removed := c.removePendingRequest(reqID, other); removed != nil {
		t.Fatalf("unexpected removal when expected pointer mismatched: %+v", removed)
	}
	if !pendingRequestExistsForTest(c, reqID) {
		t.Fatalf("request %d should still be present after mismatch remove", reqID)
	}

	removed := c.removePendingRequest(reqID, registered)
	if removed != registered {
		t.Fatalf("expected registered request to be removed, got %+v", removed)
	}
	if pendingRequestExistsForTest(c, reqID) {
		t.Fatalf("request %d should be removed after pointer match", reqID)
	}
}
