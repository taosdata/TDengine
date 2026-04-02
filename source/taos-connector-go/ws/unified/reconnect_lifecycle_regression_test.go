package unified

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/ws/client"
)

var reconnectLifecycleUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func wsEndpointFromHTTP(serverURL string) string {
	return "ws" + strings.TrimPrefix(serverURL, "http")
}

func isVersionActionText(text string) bool {
	return strings.Contains(text, `"action":"version"`) || strings.Contains(text, `"action": "version"`)
}

func writeVersionResponse(conn *websocket.Conn) error {
	return conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"version","version":"3.3.6.0"}`))
}

// TestSchemalessInsertReplayAfterWriteAckDisconnect verifies the expected behavior for this scenario.
func TestSchemalessInsertReplayAfterWriteAckDisconnect(t *testing.T) {
	var insertCount int32
	var connCount int32

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := reconnectLifecycleUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() {
			if closeErr := conn.Close(); closeErr != nil {
				t.Logf("close websocket connection: %v", closeErr)
			}
		}()
		atomic.AddInt32(&connCount, 1)

		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			text := string(msg)
			switch {
			case isVersionActionText(text):
				_ = writeVersionResponse(conn)
			case strings.Contains(text, `"action":"conn"`):
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"conn","req_id":0}`))
			case strings.Contains(text, `"action":"insert"`):
				if atomic.AddInt32(&insertCount, 1) == 1 {
					// Disconnect after the server has read the insert, before response.
					_ = conn.UnderlyingConn().Close()
					return
				}
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"insert","req_id":1}`))
			}
		}
	}))
	defer s.Close()

	cfg := NewConfig([]string{wsEndpointFromHTTP(s.URL)})
	cfg.User = "root"
	cfg.Passwd = "taosdata"
	cfg.AutoReconnect = true
	cfg.ReconnectRetryCount = 1
	cfg.ReconnectIntervalMs = 10

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.Connect())

	err = c.SchemalessInsert(1, "measurement,host=host1 field1=2i 1577837300000", 1, "ms", 0, "")
	require.NoError(t, err)
	assert.Equal(t, int32(2), atomic.LoadInt32(&insertCount), "insert should be replayed after write-acked disconnect")
	assert.Equal(t, int32(2), atomic.LoadInt32(&connCount), "should reconnect and replay after write-acked disconnect")
}

// TestSchemalessInsertRespectsAutoReconnect verifies the expected behavior for this scenario.
func TestSchemalessInsertRespectsAutoReconnect(t *testing.T) {
	var connCount int32

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := reconnectLifecycleUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() {
			if closeErr := conn.Close(); closeErr != nil {
				t.Logf("close websocket connection: %v", closeErr)
			}
		}()
		atomic.AddInt32(&connCount, 1)

		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			text := string(msg)
			switch {
			case isVersionActionText(text):
				_ = writeVersionResponse(conn)
			case strings.Contains(text, `"action":"conn"`):
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"conn","req_id":0}`))
			case strings.Contains(text, `"action":"insert"`):
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"insert","req_id":2}`))
			}
		}
	}))
	defer s.Close()

	cfg := NewConfig([]string{wsEndpointFromHTTP(s.URL)})
	cfg.User = "root"
	cfg.Passwd = "taosdata"
	cfg.AutoReconnect = false
	cfg.ReconnectRetryCount = 1
	cfg.ReconnectIntervalMs = 10

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.Connect())

	runtime := c.runtimeClient()
	require.NotNil(t, runtime)
	runtime.Close()

	err = c.SchemalessInsert(2, "measurement,host=host1 field1=2i 1577837300000", 1, "ms", 0, "")
	require.Error(t, err)
	assert.ErrorIs(t, err, client.ClosedError)
	assert.Equal(t, int32(1), atomic.LoadInt32(&connCount), "auto reconnect disabled should not open new connections")
}

// TestSetErrorHandlerBeforeConnectPersistsAfterReconnect verifies the expected behavior for this scenario.
func TestSetErrorHandlerBeforeConnectPersistsAfterReconnect(t *testing.T) {
	var (
		connMu sync.Mutex
		conns  []*websocket.Conn
	)

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := reconnectLifecycleUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		connMu.Lock()
		conns = append(conns, conn)
		connMu.Unlock()
		defer func() {
			if closeErr := conn.Close(); closeErr != nil {
				t.Logf("close websocket connection: %v", closeErr)
			}
		}()

		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			text := string(msg)
			if isVersionActionText(text) {
				_ = writeVersionResponse(conn)
				continue
			}
			if strings.Contains(text, `"action":"conn"`) {
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"conn","req_id":0}`))
			}
		}
	}))
	defer s.Close()

	cfg := NewConfig([]string{wsEndpointFromHTTP(s.URL)})
	cfg.User = "root"
	cfg.Passwd = "taosdata"
	cfg.ReconnectRetryCount = 1
	cfg.ReconnectIntervalMs = 10

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	var callbackCount int32
	c.SetErrorHandler(func(error) {
		atomic.AddInt32(&callbackCount, 1)
	})

	require.NoError(t, c.Connect())
	firstRuntime := c.runtimeClient()
	require.NotNil(t, firstRuntime)

	var firstConn *websocket.Conn
	require.Eventually(t, func() bool {
		connMu.Lock()
		defer connMu.Unlock()
		if len(conns) < 1 {
			return false
		}
		firstConn = conns[0]
		return true
	}, time.Second, 10*time.Millisecond)

	_ = firstConn.UnderlyingConn().Close()
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&callbackCount) >= 1
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, c.reconnectWithBootstrap(c.defaultBootstrap, firstRuntime))

	var secondConn *websocket.Conn
	require.Eventually(t, func() bool {
		connMu.Lock()
		defer connMu.Unlock()
		if len(conns) < 2 {
			return false
		}
		secondConn = conns[1]
		return true
	}, time.Second, 10*time.Millisecond)

	_ = secondConn.UnderlyingConn().Close()
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&callbackCount) >= 2
	}, time.Second, 10*time.Millisecond)
}

// TestConnectAfterCloseReturnsClosedError verifies the expected behavior for this scenario.
func TestConnectAfterCloseReturnsClosedError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := reconnectLifecycleUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() {
			if closeErr := conn.Close(); closeErr != nil {
				t.Logf("close websocket connection: %v", closeErr)
			}
		}()

		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			text := string(msg)
			if isVersionActionText(text) {
				_ = writeVersionResponse(conn)
				continue
			}
			if strings.Contains(text, `"action":"conn"`) {
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"conn","req_id":0}`))
			}
		}
	}))
	defer s.Close()

	cfg := NewConfig([]string{wsEndpointFromHTTP(s.URL)})
	cfg.User = "root"
	cfg.Passwd = "taosdata"

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)

	require.NoError(t, c.Connect())
	c.Close()

	err = c.Connect()
	require.ErrorIs(t, err, ErrUnifiedClosed)
}
