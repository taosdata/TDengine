package unified

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	tLog "github.com/taosdata/driver-go/v3/log"
	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

var packetLogIntegrationMu sync.Mutex

func withPacketLogTestSetup(t *testing.T, enabled bool) *bytes.Buffer {
	t.Helper()
	packetLogIntegrationMu.Lock()

	oldLevel := tLog.GetLevel()
	oldPacketEnabled := tLog.IsPacketLoggingEnabled()
	var out bytes.Buffer

	tLog.SetOutput(&out)
	tLog.SetLevel(tLog.LogLevelInfo)
	tLog.SetPacketLogging(enabled)

	t.Cleanup(func() {
		tLog.SetPacketLogging(oldPacketEnabled)
		tLog.SetLevel(oldLevel)
		tLog.SetOutput(os.Stderr)
		packetLogIntegrationMu.Unlock()
	})
	return &out
}

func startPacketLogIntegrationServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := queryLifecycleUpgrader.Upgrade(w, r, nil)
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
			switch {
			case mt == websocket.TextMessage && isVersionActionText(string(msg)):
				_ = writeVersionResponse(conn)
			case mt == websocket.TextMessage && strings.Contains(string(msg), `"action":"conn"`):
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"conn","req_id":0}`))
			case mt == websocket.BinaryMessage && binaryAction(msg) == proto.BinaryQueryMessage:
				_ = writeMockQueryResponse(conn, binaryReqID(msg), 101, false)
			}
		}
	}))
}

func TestPacketLoggingEnabled_LogsSendAndReceiveContent(t *testing.T) {
	logOutput := withPacketLogTestSetup(t, true)
	s := startPacketLogIntegrationServer(t)
	defer s.Close()

	cfg := NewConfig([]string{wsEndpointFromHTTP(s.URL)})
	cfg.User = "root"
	cfg.Passwd = "taosdata"

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.Connect())

	rs, err := c.Query(123, "select 1")
	require.NoError(t, err)
	require.NotNil(t, rs)
	_ = rs.Close()

	logText := logOutput.String()
	require.Contains(t, logText, "sending binary packet")
	require.Contains(t, logText, "received text packet")
	require.Contains(t, logText, "\"action\":\"query\"")
	require.NotContains(t, logText, "taosdata")
	// "select" in hex content
	require.Contains(t, logText, "73656c656374")
}

func TestPacketLoggingDisabled_DoesNotLogPacketContent(t *testing.T) {
	logOutput := withPacketLogTestSetup(t, false)
	s := startPacketLogIntegrationServer(t)
	defer s.Close()

	cfg := NewConfig([]string{wsEndpointFromHTTP(s.URL)})
	cfg.User = "root"
	cfg.Passwd = "taosdata"

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.Connect())

	rs, err := c.Query(456, "select 1")
	require.NoError(t, err)
	require.NotNil(t, rs)
	_ = rs.Close()

	logText := logOutput.String()
	require.NotContains(t, logText, "sending binary packet")
	require.NotContains(t, logText, "received text packet")
	require.NotContains(t, logText, "content:")
}
