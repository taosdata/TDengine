package unified

import (
	"encoding/json"
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

// TestDefaultBootstrapSendsTimezone verifies the expected behavior for this scenario.
func TestDefaultBootstrapSendsTimezone(t *testing.T) {
	tzCh := make(chan string, 1)
	errCh := make(chan error, 1)

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := queryLifecycleUpgrader.Upgrade(w, r, nil)
		if err != nil {
			errCh <- err
			return
		}
		defer func() {
			_ = conn.Close()
		}()

		for {
			_, msg, readErr := conn.ReadMessage()
			if readErr != nil {
				errCh <- readErr
				return
			}
			text := string(msg)
			if isVersionActionText(text) {
				if writeErr := writeVersionResponse(conn); writeErr != nil {
					errCh <- writeErr
					return
				}
				continue
			}
			var action client.WSAction
			if err = json.Unmarshal(msg, &action); err != nil {
				errCh <- err
				return
			}
			if strings.ToLower(action.Action) != "conn" {
				continue
			}
			var req proto.WSConnectReq
			if err = json.Unmarshal(action.Args, &req); err != nil {
				errCh <- err
				return
			}
			tzCh <- req.TZ
			err = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"conn","req_id":0}`))
			if err != nil {
				errCh <- err
			}
			return
		}
	}))
	defer s.Close()

	loc, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)

	cfg := NewConfig([]string{wsEndpointFromHTTP(s.URL)})
	cfg.User = "root"
	cfg.Passwd = "taosdata"
	cfg.Timezone = loc

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.Connect())

	select {
	case tz := <-tzCh:
		assert.Equal(t, "Asia/Shanghai", tz)
	case err = <-errCh:
		t.Fatalf("bootstrap server failed: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for bootstrap request")
	}
}

// TestClientPingSendsPingFrame verifies the expected behavior for this scenario.
func TestClientPingSendsPingFrame(t *testing.T) {
	var pingSeen uint32

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := queryLifecycleUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() {
			_ = conn.Close()
		}()
		conn.SetPingHandler(func(string) error {
			atomic.StoreUint32(&pingSeen, 1)
			return nil
		})

		for {
			mt, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			switch {
			case mt == websocket.TextMessage && json.Valid(msg):
				if isVersionActionText(string(msg)) {
					if err = writeVersionResponse(conn); err != nil {
						return
					}
					continue
				}
				if err = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"conn","req_id":0}`)); err != nil {
					return
				}
			}
		}
	}))
	defer s.Close()

	cfg := NewConfig([]string{wsEndpointFromHTTP(s.URL)})
	cfg.User = "root"
	cfg.Passwd = "taosdata"
	cfg.ReadTimeout = time.Second
	cfg.WriteTimeout = time.Second

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.Connect())
	require.NoError(t, c.Ping())
	require.Eventually(t, func() bool {
		return atomic.LoadUint32(&pingSeen) == 1
	}, time.Second, 10*time.Millisecond)
}

// TestClientPingAfterClose verifies the expected behavior for this scenario.
func TestClientPingAfterClose(t *testing.T) {
	cfg := NewConfig([]string{"ws://127.0.0.1:6041/ws"})
	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)

	c.Close()
	err = c.Ping()
	require.ErrorIs(t, err, ErrUnifiedClosed)
}
