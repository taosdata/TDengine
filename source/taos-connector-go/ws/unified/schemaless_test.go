package unified

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var schemalessTestUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

// Mock server that handles schemaless insert requests
func wsSchemalessServer(w http.ResponseWriter, r *http.Request) {
	conn, err := schemalessTestUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			return
		}
	}()

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}

		// Simple response based on message content
		var resp string
		text := string(msg)
		if isVersionActionText(text) {
			resp = `{"code":0,"message":"","action":"version","version":"3.3.6.0"}`
		} else if strings.Contains(text, `"action":"conn"`) {
			resp = `{"code":0,"message":"","action":"conn","req_id":0}`
		} else if strings.Contains(text, `"action":"insert"`) {
			resp = `{"code":0,"message":"","action":"insert","req_id":1}`
		} else {
			resp = `{"code":0,"message":"","req_id":0}`
		}

		err = conn.WriteMessage(websocket.TextMessage, []byte(resp))
		if err != nil {
			return
		}
	}
}

// TestSchemalessInsertBasic tests basic schemaless insert
func TestSchemalessInsertBasic(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(wsSchemalessServer))
	defer s.Close()

	ep := "ws" + strings.TrimPrefix(s.URL, "http")
	cfg := NewConfig([]string{ep})
	cfg.User = "root"
	cfg.Passwd = "taosdata"

	client, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer client.Close()

	err = client.Connect()
	require.NoError(t, err)

	err = client.SchemalessInsert(1, "measurement,host=host1 field1=2i 1577837300000", 1, "ms", 0, "")
	assert.NoError(t, err)
}

func TestSchemalessInsertWithTableNameKey(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		conn, err := schemalessTestUpgrader.Upgrade(w, r, nil)
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
			if writeErr := writeVersionResponse(conn); writeErr != nil {
				return
			}
		}

		_, _, err = conn.ReadMessage()
		if err != nil {
			return
		}
		if writeErr := conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"conn","req_id":0}`)); writeErr != nil {
			return
		}

		_, msg, err = conn.ReadMessage()
		if err != nil {
			return
		}
		if !strings.Contains(string(msg), `"table_name_key":"metric"`) {
			_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":65535,"message":"table_name_key missing","action":"insert","req_id":1}`))
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"insert","req_id":1}`))
	}

	s := httptest.NewServer(http.HandlerFunc(handler))
	defer s.Close()

	ep := "ws" + strings.TrimPrefix(s.URL, "http")
	cfg := NewConfig([]string{ep})
	cfg.User = "root"
	cfg.Passwd = "taosdata"

	client, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer client.Close()

	err = client.Connect()
	require.NoError(t, err)

	err = client.SchemalessInsert(1, "measurement,host=host1 field1=2i 1577837300000", 1, "ms", 0, "metric")
	require.NoError(t, err)
}

// TestSchemalessInsertAfterClose tests that insert fails after close
func TestSchemalessInsertAfterClose(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(wsSchemalessServer))
	defer s.Close()

	ep := "ws" + strings.TrimPrefix(s.URL, "http")
	cfg := NewConfig([]string{ep})

	client, err := NewClient(cfg, "/ws")
	require.NoError(t, err)

	err = client.Connect()
	require.NoError(t, err)

	client.Close()

	err = client.SchemalessInsert(1, "measurement,host=host1 field1=2i 1577837300000", 1, "ms", 0, "")
	assert.Error(t, err)
	assert.Equal(t, ErrUnifiedClosed, err)
}

// TestSchemalessResponseBeforeServerClose tests receiving response before server closes
func TestSchemalessResponseBeforeServerClose(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		conn, err := schemalessTestUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() {
			if closeErr := conn.Close(); closeErr != nil {
				t.Logf("close websocket connection: %v", closeErr)
			}
		}()

		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if isVersionActionText(string(msg)) {
			if writeErr := writeVersionResponse(conn); writeErr != nil {
				return
			}
		}
		_, _, err = conn.ReadMessage()
		if err != nil {
			return
		}
		// Send response then close
		resp := `{"code":0,"message":"","action":"conn","req_id":0}`
		_ = conn.WriteMessage(websocket.TextMessage, []byte(resp))

		_, _, err = conn.ReadMessage()
		if err != nil {
			return
		}
		resp = `{"code":0,"message":"","action":"insert","req_id":1}`
		_ = conn.WriteMessage(websocket.TextMessage, []byte(resp))
		_ = conn.UnderlyingConn().Close()
	}

	s := httptest.NewServer(http.HandlerFunc(handler))
	defer s.Close()

	ep := "ws" + strings.TrimPrefix(s.URL, "http")
	cfg := NewConfig([]string{ep})

	client, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer client.Close()

	err = client.Connect()
	require.NoError(t, err)

	err = client.SchemalessInsert(1, "measurement,host=host1 field1=2i 1577837300000", 1, "ms", 0, "")
	require.NoError(t, err)
}
