package unified

import (
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

var queryLifecycleUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func binaryAction(msg []byte) uint64 {
	if len(msg) < 24 {
		return 0
	}
	return binary.LittleEndian.Uint64(msg[16:24])
}

func binaryReqID(msg []byte) uint64 {
	if len(msg) < 8 {
		return 0
	}
	return binary.LittleEndian.Uint64(msg[0:8])
}

func writeMockQueryResponse(conn *websocket.Conn, reqID uint64, resultID uint64, isUpdate bool) error {
	resp := fmt.Sprintf(
		`{"code":0,"message":"","action":"query","req_id":%d,"id":%d,"is_update":%t,"affected_rows":1,"fields_count":1,"fields_names":["v"],"fields_types":[4],"fields_lengths":[4],"fields_precisions":[0],"fields_scales":[0],"precision":0}`,
		reqID, resultID, isUpdate,
	)
	return conn.WriteMessage(websocket.TextMessage, []byte(resp))
}

// TestQueryReplayAfterWriteAckDisconnect verifies the expected behavior for this scenario.
func TestQueryReplayAfterWriteAckDisconnect(t *testing.T) {
	var connCount int32
	var queryCount int32

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := queryLifecycleUpgrader.Upgrade(w, r, nil)
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
			mt, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			switch {
			case mt == websocket.TextMessage && isVersionActionText(string(msg)):
				_ = writeVersionResponse(conn)
			case mt == websocket.TextMessage && strings.Contains(string(msg), `"action":"conn"`):
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"conn","req_id":0}`))
			case mt == websocket.BinaryMessage && binaryAction(msg) == proto.BinaryQueryMessage:
				reqID := binaryReqID(msg)
				if atomic.AddInt32(&queryCount, 1) == 1 {
					// Disconnect after the first request write-ack, before query response.
					_ = conn.UnderlyingConn().Close()
					return
				}
				_ = writeMockQueryResponse(conn, reqID, 101, false)
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

	rs, err := c.Query(1, "select 1")
	require.NoError(t, err)
	require.NotNil(t, rs)
	assert.Equal(t, int32(2), atomic.LoadInt32(&queryCount), "query should be replayed after write-acked disconnect")
	assert.Equal(t, int32(2), atomic.LoadInt32(&connCount), "should reconnect and replay after write-acked disconnect")
}

// TestQueryRespectsAutoReconnect verifies the expected behavior for this scenario.
func TestQueryRespectsAutoReconnect(t *testing.T) {
	var connCount int32

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := queryLifecycleUpgrader.Upgrade(w, r, nil)
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
			if isVersionActionText(string(msg)) {
				_ = writeVersionResponse(conn)
				continue
			}
			if strings.Contains(string(msg), `"action":"conn"`) {
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"conn","req_id":0}`))
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

	_, err = c.Query(2, "select 1")
	require.Error(t, err)
	assert.ErrorIs(t, err, client.ClosedError)
	assert.True(t, IsConnectionDisconnectedError(err))
	assert.Equal(t, int32(1), atomic.LoadInt32(&connCount), "auto reconnect disabled should not open new connections")
}

// TestQueryResultFetchNoReconnectAfterDisconnect verifies the expected behavior for this scenario.
func TestQueryResultFetchNoReconnectAfterDisconnect(t *testing.T) {
	var connCount int32
	var fetchCount int32
	var queryCount int32

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := queryLifecycleUpgrader.Upgrade(w, r, nil)
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
			mt, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			switch {
			case mt == websocket.TextMessage && isVersionActionText(string(msg)):
				_ = writeVersionResponse(conn)
			case mt == websocket.TextMessage && strings.Contains(string(msg), `"action":"conn"`):
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"conn","req_id":0}`))
			case mt == websocket.BinaryMessage && binaryAction(msg) == proto.BinaryQueryMessage:
				atomic.AddInt32(&queryCount, 1)
				_ = writeMockQueryResponse(conn, binaryReqID(msg), 99, false)
			case mt == websocket.BinaryMessage && binaryAction(msg) == proto.FetchRawBlockMessage:
				atomic.AddInt32(&fetchCount, 1)
				_ = conn.UnderlyingConn().Close()
				return
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
	cfg.ReadTimeout = 3 * time.Second

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.Connect())

	result, err := c.Query(3, "select 1")
	require.NoError(t, err)
	require.NotNil(t, result)

	start := time.Now()
	_, _, err = result.fetchRawBlock(4)
	elapsed := time.Since(start)
	require.Error(t, err)
	assert.True(t, IsConnectionDisconnectedError(err), "fetch should report disconnected result-connection")
	assert.Equal(t, int32(1), atomic.LoadInt32(&fetchCount))
	assert.Equal(t, int32(1), atomic.LoadInt32(&connCount), "fetch must not trigger reconnect")
	assert.Less(t, elapsed, 2*time.Second, "disconnect should be sensed quickly")

	// Subsequent new query is stateless and should trigger reconnect successfully.
	_, err = c.Query(5, "select 1")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, atomic.LoadInt32(&connCount), int32(2))
	assert.GreaterOrEqual(t, atomic.LoadInt32(&queryCount), int32(2))
}
