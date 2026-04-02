package unified

import (
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	commonstmt "github.com/taosdata/driver-go/v3/common/stmt"
	wsClient "github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

type stmtReqID struct {
	ReqID uint64 `json:"req_id"`
}

// TestStmtExecReplaysAfterWriteAckDisconnect verifies the expected behavior for this scenario.
func TestStmtExecReplaysAfterWriteAckDisconnect(t *testing.T) {
	var execReadCount int32
	var connCount int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := reconnectLifecycleUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() {
			_ = conn.Close()
		}()
		atomic.AddInt32(&connCount, 1)

		stmtID := uint64(1000 + atomic.LoadInt32(&connCount))
		for {
			mt, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			switch mt {
			case websocket.TextMessage:
				text := string(msg)
				if isVersionActionText(text) {
					_ = writeVersionResponse(conn)
					continue
				}
				var action wsClient.WSAction
				if err = json.Unmarshal(msg, &action); err != nil {
					return
				}

				var req stmtReqID
				_ = json.Unmarshal(action.Args, &req)

				switch action.Action {
				case proto.Connect:
					resp := fmt.Sprintf(`{"code":0,"message":"","action":"conn","req_id":%d}`, req.ReqID)
					_ = conn.WriteMessage(websocket.TextMessage, []byte(resp))
				case proto.STMT2Init:
					resp := fmt.Sprintf(`{"code":0,"message":"","action":"stmt2_init","req_id":%d,"stmt_id":%d}`, req.ReqID, stmtID)
					_ = conn.WriteMessage(websocket.TextMessage, []byte(resp))
				case proto.STMT2Prepare:
					resp := fmt.Sprintf(`{"code":0,"message":"","action":"stmt2_prepare","req_id":%d,"stmt_id":%d,"is_insert":false,"fields_count":0}`, req.ReqID, stmtID)
					_ = conn.WriteMessage(websocket.TextMessage, []byte(resp))
				case proto.STMT2Exec:
					n := atomic.AddInt32(&execReadCount, 1)
					if n == 1 {
						// Simulate disconnect after request write-ack, before exec response.
						_ = conn.UnderlyingConn().Close()
						return
					}
					resp := fmt.Sprintf(`{"code":0,"message":"","action":"stmt2_exec","req_id":%d,"stmt_id":%d,"affected":1}`, req.ReqID, stmtID)
					_ = conn.WriteMessage(websocket.TextMessage, []byte(resp))
				}
			case websocket.BinaryMessage:
				if len(msg) < 24 {
					continue
				}
				if binary.LittleEndian.Uint64(msg[16:24]) != proto.Stmt2BindMessage {
					continue
				}
				reqID := binary.LittleEndian.Uint64(msg[0:8])
				resp := fmt.Sprintf(`{"code":0,"message":"","action":"stmt2_bind","req_id":%d,"stmt_id":%d}`, reqID, stmtID)
				_ = conn.WriteMessage(websocket.TextMessage, []byte(resp))
			}
		}
	}))
	defer server.Close()

	cfg := NewConfig([]string{wsEndpointFromHTTP(server.URL)})
	cfg.User = "root"
	cfg.Passwd = "taosdata"
	cfg.AutoReconnect = true
	cfg.ReconnectIntervalMs = 10
	cfg.ReconnectRetryCount = 3

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.Connect())

	stmt := &Stmt{
		client:  c,
		runtime: c.runtimeClient(),
		id:      1,
		sql:     "select ?",
	}

	resp, err := stmt.execWithReconnectLocked(0, []byte{1})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, 1, resp.Affected)
	require.Equal(t, int32(2), atomic.LoadInt32(&execReadCount), "exec should be replayed after write-acked disconnect")
	require.GreaterOrEqual(t, atomic.LoadInt32(&connCount), int32(2), "should reconnect after disconnect")
}

// TestStmtExecReplaysAfterBindWriteAckDisconnect verifies bind-stage replay.
func TestStmtExecReplaysAfterBindWriteAckDisconnect(t *testing.T) {
	var bindReadCount int32
	var execReadCount int32
	var connCount int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := reconnectLifecycleUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() {
			_ = conn.Close()
		}()
		atomic.AddInt32(&connCount, 1)

		stmtID := uint64(2000 + atomic.LoadInt32(&connCount))
		for {
			mt, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			switch mt {
			case websocket.TextMessage:
				text := string(msg)
				if isVersionActionText(text) {
					_ = writeVersionResponse(conn)
					continue
				}
				var action wsClient.WSAction
				if err = json.Unmarshal(msg, &action); err != nil {
					return
				}

				var req stmtReqID
				_ = json.Unmarshal(action.Args, &req)

				switch action.Action {
				case proto.Connect:
					resp := fmt.Sprintf(`{"code":0,"message":"","action":"conn","req_id":%d}`, req.ReqID)
					_ = conn.WriteMessage(websocket.TextMessage, []byte(resp))
				case proto.STMT2Init:
					resp := fmt.Sprintf(`{"code":0,"message":"","action":"stmt2_init","req_id":%d,"stmt_id":%d}`, req.ReqID, stmtID)
					_ = conn.WriteMessage(websocket.TextMessage, []byte(resp))
				case proto.STMT2Prepare:
					resp := fmt.Sprintf(`{"code":0,"message":"","action":"stmt2_prepare","req_id":%d,"stmt_id":%d,"is_insert":false,"fields_count":0}`, req.ReqID, stmtID)
					_ = conn.WriteMessage(websocket.TextMessage, []byte(resp))
				case proto.STMT2Exec:
					atomic.AddInt32(&execReadCount, 1)
					resp := fmt.Sprintf(`{"code":0,"message":"","action":"stmt2_exec","req_id":%d,"stmt_id":%d,"affected":1}`, req.ReqID, stmtID)
					_ = conn.WriteMessage(websocket.TextMessage, []byte(resp))
				}
			case websocket.BinaryMessage:
				if len(msg) < 24 {
					continue
				}
				if binary.LittleEndian.Uint64(msg[16:24]) != proto.Stmt2BindMessage {
					continue
				}
				if atomic.AddInt32(&bindReadCount, 1) == 1 {
					// Disconnect after the bind request has been read, before bind response.
					_ = conn.UnderlyingConn().Close()
					return
				}
				reqID := binary.LittleEndian.Uint64(msg[0:8])
				resp := fmt.Sprintf(`{"code":0,"message":"","action":"stmt2_bind","req_id":%d,"stmt_id":%d}`, reqID, stmtID)
				_ = conn.WriteMessage(websocket.TextMessage, []byte(resp))
			}
		}
	}))
	defer server.Close()

	cfg := NewConfig([]string{wsEndpointFromHTTP(server.URL)})
	cfg.User = "root"
	cfg.Passwd = "taosdata"
	cfg.AutoReconnect = true
	cfg.ReconnectIntervalMs = 10
	cfg.ReconnectRetryCount = 3

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.Connect())

	stmt := &Stmt{
		client:  c,
		runtime: c.runtimeClient(),
		id:      1,
		sql:     "select ?",
	}

	resp, err := stmt.execWithReconnectLocked(0, []byte{1})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, 1, resp.Affected)
	require.Equal(t, int32(2), atomic.LoadInt32(&bindReadCount), "bind should be replayed after write-acked disconnect")
	require.Equal(t, int32(1), atomic.LoadInt32(&execReadCount), "exec should only run after bind replay succeeds")
	require.GreaterOrEqual(t, atomic.LoadInt32(&connCount), int32(2), "should reconnect after bind-stage disconnect")
}

// TestStmtExecRespectsAutoReconnect verifies stmt exec does not reconnect when disabled.
func TestStmtExecRespectsAutoReconnect(t *testing.T) {
	var connCount int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := reconnectLifecycleUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() {
			_ = conn.Close()
		}()
		atomic.AddInt32(&connCount, 1)

		stmtID := uint64(3000 + atomic.LoadInt32(&connCount))
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

			var action wsClient.WSAction
			if err = json.Unmarshal(msg, &action); err != nil {
				return
			}

			var req stmtReqID
			_ = json.Unmarshal(action.Args, &req)

			switch action.Action {
			case proto.Connect:
				resp := fmt.Sprintf(`{"code":0,"message":"","action":"conn","req_id":%d}`, req.ReqID)
				_ = conn.WriteMessage(websocket.TextMessage, []byte(resp))
			case proto.STMT2Init:
				resp := fmt.Sprintf(`{"code":0,"message":"","action":"stmt2_init","req_id":%d,"stmt_id":%d}`, req.ReqID, stmtID)
				_ = conn.WriteMessage(websocket.TextMessage, []byte(resp))
			case proto.STMT2Prepare:
				resp := fmt.Sprintf(`{"code":0,"message":"","action":"stmt2_prepare","req_id":%d,"stmt_id":%d,"is_insert":false,"fields_count":1}`, req.ReqID, stmtID)
				_ = conn.WriteMessage(websocket.TextMessage, []byte(resp))
			}
		}
	}))
	defer server.Close()

	cfg := NewConfig([]string{wsEndpointFromHTTP(server.URL)})
	cfg.User = "root"
	cfg.Passwd = "taosdata"
	cfg.AutoReconnect = false
	cfg.ReconnectIntervalMs = 10
	cfg.ReconnectRetryCount = 1

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.Connect())

	stmt, err := c.InitStmt(0)
	require.NoError(t, err)
	defer func() {
		_ = stmt.Close(0)
	}()
	require.NoError(t, stmt.Prepare(0, "select ?"))
	require.NoError(t, stmt.Bind([]*commonstmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{{int32(1)}},
		},
	}))

	runtime := c.runtimeClient()
	require.NotNil(t, runtime)
	runtime.Close()

	_, err = stmt.Exec(0)
	require.Error(t, err)
	require.Truef(t, errors.Is(err, wsClient.ClosedError) || errors.Is(err, ErrStmtConnectionLost), "unexpected stmt exec error: %v", err)
	require.Equal(t, int32(1), atomic.LoadInt32(&connCount), "auto reconnect disabled should not open new connections")
}
