package unified

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

func newStmtCloseTestClient(t *testing.T, closeCode int, closeMessage string) (*Client, *int32, <-chan proto.Stmt2CloseRequest) {
	t.Helper()

	var closeCount int32
	reqCh := make(chan proto.Stmt2CloseRequest, 1)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := reconnectLifecycleUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() {
			_ = conn.Close()
		}()

		for {
			mt, msg, readErr := conn.ReadMessage()
			if readErr != nil {
				return
			}
			if mt != websocket.TextMessage {
				continue
			}
			text := string(msg)
			switch {
			case isVersionActionText(text):
				_ = writeVersionResponse(conn)
				continue
			}

			var action client.WSAction
			if err = json.Unmarshal(msg, &action); err != nil {
				return
			}

			switch action.Action {
			case proto.Connect:
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"conn","req_id":0}`))
			case proto.STMT2Close:
				atomic.AddInt32(&closeCount, 1)
				var req proto.Stmt2CloseRequest
				if err = json.Unmarshal(action.Args, &req); err != nil {
					return
				}
				select {
				case reqCh <- req:
				default:
				}
				resp := fmt.Sprintf(`{"code":%d,"message":%q,"action":"stmt2_close","req_id":%d,"stmt_id":%d}`, closeCode, closeMessage, req.ReqID, req.StmtID)
				_ = conn.WriteMessage(websocket.TextMessage, []byte(resp))
			}
		}
	}))
	t.Cleanup(s.Close)

	cfg := NewConfig([]string{wsEndpointFromHTTP(s.URL)})
	cfg.User = "root"
	cfg.Passwd = "taosdata"

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	t.Cleanup(c.Close)
	require.NoError(t, c.Connect())
	return c, &closeCount, reqCh
}

// TestStmtCloseWaitsAndParsesSuccessResponse verifies the expected behavior for this scenario.
func TestStmtCloseWaitsAndParsesSuccessResponse(t *testing.T) {
	c, closeCount, reqCh := newStmtCloseTestClient(t, 0, "")
	stmt := &Stmt{
		client:  c,
		runtime: c.runtimeClient(),
		id:      9527,
		state:   newStmtCompatState(),
	}

	require.NoError(t, stmt.Close(123))
	select {
	case req := <-reqCh:
		require.Equal(t, uint64(123), req.ReqID)
		require.Equal(t, uint64(9527), req.StmtID)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting stmt2_close request")
	}
	require.Equal(t, int32(1), atomic.LoadInt32(closeCount))

	// Second close is idempotent and must not send another request.
	require.NoError(t, stmt.Close(123))
	time.Sleep(30 * time.Millisecond)
	require.Equal(t, int32(1), atomic.LoadInt32(closeCount))
}

// TestStmtCloseReturnsServerErrorResponse verifies the expected behavior for this scenario.
func TestStmtCloseReturnsServerErrorResponse(t *testing.T) {
	c, _, _ := newStmtCloseTestClient(t, 65535, "close failed")
	stmt := &Stmt{
		client:  c,
		runtime: c.runtimeClient(),
		id:      100,
		state:   newStmtCompatState(),
	}

	err := stmt.Close(77)
	require.Error(t, err)
	require.Contains(t, err.Error(), "close failed")
}
