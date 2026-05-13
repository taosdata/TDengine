package unified

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

// TestNewConnectorNilConfig verifies the expected behavior for this scenario.
func TestNewConnectorNilConfig(t *testing.T) {
	connector, err := NewConnector(nil, "/ws")
	require.Error(t, err)
	assert.Nil(t, connector)
	assert.ErrorIs(t, err, ErrNilConfig)
}

// TestConnectorConnect verifies the expected behavior for this scenario.
func TestConnectorConnect(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := queryLifecycleUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() {
			_ = conn.Close()
		}()

		for {
			_, msg, readErr := conn.ReadMessage()
			if readErr != nil {
				return
			}
			if isVersionActionText(string(msg)) {
				if err = writeVersionResponse(conn); err != nil {
					return
				}
				continue
			}
			var action client.WSAction
			if err = json.Unmarshal(msg, &action); err != nil {
				return
			}
			var req proto.WSConnectReq
			if err = json.Unmarshal(action.Args, &req); err != nil {
				return
			}
			_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"conn","req_id":0}`))
			return
		}
	}))
	defer s.Close()

	cfg := NewConfig([]string{wsEndpointFromHTTP(s.URL)})
	cfg.User = "root"
	cfg.Passwd = "taosdata"

	connector, err := NewConnector(cfg, "/ws")
	require.NoError(t, err)

	c, err := connector.Connect()
	require.NoError(t, err)
	require.NotNil(t, c)
	c.Close()
}

// TestNewConnectorFromDSN verifies the expected behavior for this scenario.
func TestNewConnectorFromDSN(t *testing.T) {
	connector, err := NewConnectorFromDSN("user:passwd@ws(127.0.0.1:6041)/db", "/ws")
	require.NoError(t, err)
	cfg := connector.Config()
	assert.Equal(t, "user", cfg.User)
	assert.Equal(t, "passwd", cfg.Passwd)
	assert.Equal(t, "db", cfg.DbName)
}
