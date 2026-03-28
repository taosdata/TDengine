package unified

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

// TestDefaultBootstrapIncludesAuthAndSecurityFields verifies the expected behavior for this scenario.
func TestDefaultBootstrapIncludesAuthAndSecurityFields(t *testing.T) {
	reqCh := make(chan proto.WSConnectReq, 1)
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
			reqCh <- req
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
	cfg.User = "u_test"
	cfg.Passwd = "p_test"
	cfg.DbName = "db_test"
	cfg.TotpCode = "123456"
	cfg.BearerToken = "token_test"
	cfg.Timezone = loc

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.Connect())

	select {
	case req := <-reqCh:
		assert.Equal(t, "u_test", req.User)
		assert.Equal(t, "p_test", req.Password)
		assert.Equal(t, "db_test", req.DB)
		assert.Equal(t, "123456", req.TOTPCode)
		assert.Equal(t, "token_test", req.BearerToken)
		assert.Equal(t, "Asia/Shanghai", req.TZ)
		assert.Equal(t, common.GetProcessName(), req.App)
		assert.Equal(t, common.GetConnectorInfo("ws"), req.Connector)
	case err = <-errCh:
		t.Fatalf("bootstrap server failed: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting bootstrap request")
	}
}

// TestDefaultBootstrapTimeout verifies the expected behavior for this scenario.
func TestDefaultBootstrapTimeout(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := queryLifecycleUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() {
			_ = conn.Close()
		}()
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if isVersionActionText(string(msg)) {
			if err = writeVersionResponse(conn); err != nil {
				return
			}
		}
		_, _, _ = conn.ReadMessage()
		time.Sleep(200 * time.Millisecond)
	}))
	defer s.Close()

	cfg := NewConfig([]string{wsEndpointFromHTTP(s.URL)})
	cfg.User = "root"
	cfg.Passwd = "taosdata"
	cfg.ReadTimeout = 30 * time.Millisecond
	cfg.WriteTimeout = time.Second

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	err = c.Connect()
	require.Error(t, err)
	if !errors.Is(err, ErrConnectTimeout) {
		assert.True(t, strings.Contains(strings.ToLower(err.Error()), "timeout") || strings.Contains(strings.ToLower(err.Error()), "eof"))
	}
}

// TestDefaultBootstrapHandlesServerErrorResponse verifies the expected behavior for this scenario.
func TestDefaultBootstrapHandlesServerErrorResponse(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := queryLifecycleUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() {
			_ = conn.Close()
		}()
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if isVersionActionText(string(msg)) {
			if err = writeVersionResponse(conn); err != nil {
				return
			}
		}
		_, _, _ = conn.ReadMessage()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":65535,"message":"mock failure","action":"conn","req_id":0}`))
	}))
	defer s.Close()

	cfg := NewConfig([]string{wsEndpointFromHTTP(s.URL)})
	cfg.User = "root"
	cfg.Passwd = "taosdata"

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	err = c.Connect()
	require.Error(t, err)
	assert.Contains(t, strings.ToLower(err.Error()), "mock failure")
}

// TestDefaultBootstrapHandlesInvalidJSONResponse verifies the expected behavior for this scenario.
func TestDefaultBootstrapHandlesInvalidJSONResponse(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := queryLifecycleUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() {
			_ = conn.Close()
		}()
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if isVersionActionText(string(msg)) {
			if err = writeVersionResponse(conn); err != nil {
				return
			}
		}
		_, _, _ = conn.ReadMessage()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`not-json`))
	}))
	defer s.Close()

	cfg := NewConfig([]string{wsEndpointFromHTTP(s.URL)})
	cfg.User = "root"
	cfg.Passwd = "taosdata"

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	err = c.Connect()
	require.Error(t, err)
}

// TestDefaultBootstrapHandlesReadError verifies the expected behavior for this scenario.
func TestDefaultBootstrapHandlesReadError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := queryLifecycleUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if isVersionActionText(string(msg)) {
			if err = writeVersionResponse(conn); err != nil {
				return
			}
		}
		_, _, _ = conn.ReadMessage()
		_ = conn.Close()
	}))
	defer s.Close()

	cfg := NewConfig([]string{wsEndpointFromHTTP(s.URL)})
	cfg.User = "root"
	cfg.Passwd = "taosdata"
	cfg.ReadTimeout = time.Second

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	err = c.Connect()
	require.Error(t, err)
}

// TestHandleBinaryMessageRoutesPendingRequest verifies the expected behavior for this scenario.
func TestHandleBinaryMessageRoutesPendingRequest(t *testing.T) {
	respCh := make(chan []byte, 1)
	c := &Client{
		pendingRequests: make(map[uint64]*pendingRequest),
	}
	registerPendingRequestForTest(c, &pendingRequest{
		reqID:   42,
		channel: respCh,
	})

	msg := make([]byte, 16)
	binary.LittleEndian.PutUint64(msg[8:16], 42)
	c.handleBinaryMessage(msg)

	select {
	case got := <-respCh:
		assert.Equal(t, msg, got)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting routed binary response")
	}
	assert.Equal(t, 0, pendingRequestCountForTest(c))
}

// TestHandleTextMessageRoutesPendingRequest verifies the expected behavior for this scenario.
func TestHandleTextMessageRoutesPendingRequest(t *testing.T) {
	respCh := make(chan []byte, 1)
	c := &Client{
		pendingRequests: make(map[uint64]*pendingRequest),
	}
	registerPendingRequestForTest(c, &pendingRequest{
		reqID:   66,
		channel: respCh,
	})

	msg := []byte(`{"action":"query","req_id":66,"code":0}`)
	c.handleTextMessage(msg)

	select {
	case got := <-respCh:
		assert.Equal(t, msg, got)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting routed text response")
	}
	assert.Equal(t, 0, pendingRequestCountForTest(c))
}

// TestHandleTextMessageIgnoresInvalidPayload verifies the expected behavior for this scenario.
func TestHandleTextMessageIgnoresInvalidPayload(t *testing.T) {
	respCh := make(chan []byte, 1)
	c := &Client{
		pendingRequests: make(map[uint64]*pendingRequest),
	}
	registerPendingRequestForTest(c, &pendingRequest{
		reqID:   77,
		channel: respCh,
	})

	c.handleTextMessage([]byte(`{"action":"query","code":0}`))
	c.handleTextMessage([]byte(`{"action":"query"`))

	select {
	case <-respCh:
		t.Fatal("unexpected routed response for invalid text payload")
	default:
	}
	assert.Equal(t, 1, pendingRequestCountForTest(c))
}

// TestHandleBinaryMessageIgnoresInvalidFrame verifies the expected behavior for this scenario.
func TestHandleBinaryMessageIgnoresInvalidFrame(t *testing.T) {
	respCh := make(chan []byte, 1)
	c := &Client{
		pendingRequests: make(map[uint64]*pendingRequest),
	}
	registerPendingRequestForTest(c, &pendingRequest{
		reqID:   7,
		channel: respCh,
	})

	c.handleBinaryMessage([]byte{1, 2, 3})

	select {
	case <-respCh:
		t.Fatal("unexpected routed response for invalid frame")
	default:
	}
	assert.Equal(t, 1, pendingRequestCountForTest(c))
}

func pendingRequestCountForTest(c *Client) int {
	c.pendingLock.RLock()
	count := len(c.pendingRequests)
	c.pendingLock.RUnlock()
	return count
}

// TestExtractReqIDFromBinaryMessageExtendedHeader verifies the expected behavior for this scenario.
func TestExtractReqIDFromBinaryMessageExtendedHeader(t *testing.T) {
	msg := make([]byte, 34)
	binary.LittleEndian.PutUint64(msg[0:8], 0xffffffffffffffff)
	binary.LittleEndian.PutUint64(msg[26:34], 99)

	reqID, err := extractReqIDFromBinaryMessage(msg)
	require.NoError(t, err)
	assert.Equal(t, uint64(99), reqID)
}

// TestExtractReqIDFromBinaryMessageErrors verifies the expected behavior for this scenario.
func TestExtractReqIDFromBinaryMessageErrors(t *testing.T) {
	_, err := extractReqIDFromBinaryMessage([]byte{1, 2, 3})
	require.ErrorIs(t, err, ErrBinaryMessageTooShort)

	msg := make([]byte, 20)
	binary.LittleEndian.PutUint64(msg[0:8], 0xffffffffffffffff)
	_, err = extractReqIDFromBinaryMessage(msg)
	require.ErrorIs(t, err, ErrBinaryMessageExtendedHeaderTooShort)
}

// TestExtractReqIDFromTextMessage verifies the expected behavior for this scenario.
func TestExtractReqIDFromTextMessage(t *testing.T) {
	reqID, err := extractReqIDFromTextMessage([]byte(`{"code":0,"req_id":123,"message":""}`))
	require.NoError(t, err)
	assert.Equal(t, uint64(123), reqID)
}

// TestExtractReqIDFromTextMessageIgnoresNestedReqID verifies nested req_id is ignored.
func TestExtractReqIDFromTextMessageIgnoresNestedReqID(t *testing.T) {
	reqID, err := extractReqIDFromTextMessage([]byte(`{"payload":{"req_id":999,"code":0},"req_id":123}`))
	require.NoError(t, err)
	assert.Equal(t, uint64(123), reqID)
}

// TestExtractReqIDFromTextMessageIgnoresReqIDInsideString verifies req_id in string does not confuse extraction.
func TestExtractReqIDFromTextMessageIgnoresReqIDInsideString(t *testing.T) {
	reqID, err := extractReqIDFromTextMessage([]byte(`{"sql":",\\\"req_id\\\":999","req_id":123}`))
	require.NoError(t, err)
	assert.Equal(t, uint64(123), reqID)
}

// TestExtractReqIDFromTextMessageErrors verifies the expected behavior for this scenario.
func TestExtractReqIDFromTextMessageErrors(t *testing.T) {
	reqID, err := extractReqIDFromTextMessage([]byte(`{"code":0,"message":""}`))
	require.NoError(t, err)
	require.Equal(t, uint64(0), reqID)

	_, err = extractReqIDFromTextMessage([]byte(`{"code":0,`))
	require.Error(t, err)
}

// TestExtractReqIDFromTextMessageUint64Boundaries verifies uint64 boundary handling.
func TestExtractReqIDFromTextMessageUint64Boundaries(t *testing.T) {
	reqID, err := extractReqIDFromTextMessage([]byte(`{"req_id":18446744073709551615}`))
	require.NoError(t, err)
	require.Equal(t, uint64(^uint64(0)), reqID)

	_, err = extractReqIDFromTextMessage([]byte(`{"req_id":18446744073709551616}`))
	require.Error(t, err)
}

// TestExtractReqIDFromTextMessageRejectsInvalidTrailingTokens verifies malformed
// tails are rejected even when there is whitespace after req_id.
func TestExtractReqIDFromTextMessageRejectsInvalidTrailingTokens(t *testing.T) {
	invalidMessages := []string{
		`{"req_id":1 abc}`,
		`{"req_id":1    xyz}`,
		`{"req_id":1}xyz`,
		`{"req_id":1    `,
	}
	for _, message := range invalidMessages {
		_, err := extractReqIDFromTextMessage([]byte(message))
		require.Error(t, err, "message=%s", message)
	}
}
