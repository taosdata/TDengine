package unified

import (
	"net/url"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	tLog "github.com/taosdata/driver-go/v3/log"
)

func TestPacketContentForLogTextJSONRedactsSensitiveValues(t *testing.T) {
	oldMax := tLog.GetMaxPacketLogBytes()
	tLog.SetMaxPacketLogBytes(4096)
	t.Cleanup(func() {
		tLog.SetMaxPacketLogBytes(oldMax)
	})

	payload := []byte(`{"action":"conn","password":"raw-pass","token":"raw-token","url":"ws://127.0.0.1:6041/ws?token=query-token&x=1","nested":{"authorization":"Bearer abc"}}`)
	got := packetContentForLog(websocket.TextMessage, payload)

	require.Contains(t, got, `"password":"***"`)
	require.Contains(t, got, `"token":"***"`)
	require.Contains(t, got, `"authorization":"***"`)
	require.NotContains(t, got, "raw-pass")
	require.NotContains(t, got, "raw-token")
	require.NotContains(t, got, "query-token")
	require.NotContains(t, got, "Bearer abc")
}

func TestPacketContentForLogTextFallbackRedactsFreeText(t *testing.T) {
	oldMax := tLog.GetMaxPacketLogBytes()
	tLog.SetMaxPacketLogBytes(4096)
	t.Cleanup(func() {
		tLog.SetMaxPacketLogBytes(oldMax)
	})

	payload := []byte(`non-json password=raw-pass token:raw-token`)
	got := packetContentForLog(websocket.TextMessage, payload)
	require.Contains(t, got, "password=***")
	require.Contains(t, got, "token:***")
	require.NotContains(t, got, "raw-pass")
	require.NotContains(t, got, "raw-token")
}

func TestSanitizeEndpointForLogRedactsQueryToken(t *testing.T) {
	raw := "ws://127.0.0.1:6041/ws?token=abc123&x=1"
	got := sanitizeEndpointForLog(raw)

	parsed, err := url.Parse(got)
	require.NoError(t, err)
	require.Equal(t, "***", parsed.Query().Get("token"))
	require.Equal(t, "1", parsed.Query().Get("x"))
	require.NotContains(t, got, "abc123")
}

func TestSanitizeEndpointForLogRedactsUserInfoPassword(t *testing.T) {
	raw := "ws://alice:raw-pass@127.0.0.1:6041/ws?x=1"
	got := sanitizeEndpointForLog(raw)

	parsed, err := url.Parse(got)
	require.NoError(t, err)
	require.NotNil(t, parsed.User)
	password, has := parsed.User.Password()
	require.True(t, has)
	require.Equal(t, "***", password)
	require.Equal(t, "alice", parsed.User.Username())
	require.NotContains(t, got, "raw-pass")
}

func TestShouldLogPacketInfoAndWarnRespectsLevel(t *testing.T) {
	oldLevel := tLog.GetLevel()
	oldPacketEnabled := tLog.IsPacketLoggingEnabled()
	t.Cleanup(func() {
		tLog.SetPacketLogging(oldPacketEnabled)
		tLog.SetLevel(oldLevel)
	})

	tLog.SetPacketLogging(true)
	tLog.SetLevel(tLog.LogLevelWarn)
	require.False(t, shouldLogPacketInfo())
	require.True(t, shouldLogPacketWarn())

	tLog.SetLevel(tLog.LogLevelError)
	require.False(t, shouldLogPacketInfo())
	require.False(t, shouldLogPacketWarn())
}
