package ws

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/tools/otp"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
	"github.com/taosdata/taosadapter/v3/tools/testtools/testenv"
	"github.com/taosdata/taosadapter/v3/version"
)

func TestWSConnect(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()

	// wrong password
	connReq := connRequest{ReqID: 1, User: "root", Password: "wrong"}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp connResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, "Authentication failure", connResp.Message)
	assert.Equal(t, 0x357, connResp.Code, connResp.Message)

	// connect
	connReq = connRequest{ReqID: 1, User: "root", Password: "taosdata"}
	resp, err = doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)
	//duplicate connections
	connReq = connRequest{ReqID: 1, User: "root", Password: "taosdata"}
	resp, err = doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0xffff, connResp.Code)
	assert.Equal(t, "duplicate connections", connResp.Message)
	assert.Equal(t, version.TaosClientVersion, connResp.Version)
}

func TestMode(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()

	wrongMode := 999
	connReq := connRequest{ReqID: 1, User: "root", Password: "taosdata", Mode: &wrongMode}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp connResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0xffff, connResp.Code)
	assert.Equal(t, fmt.Sprintf("unexpected mode:%d", wrongMode), connResp.Message)

	//bi
	biMode := 0
	connReq = connRequest{ReqID: 1, User: "root", Password: "taosdata", Mode: &biMode}
	resp, err = doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

}

func TestConnectionOptions(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()
	connReq := connRequest{
		ReqID:     1,
		User:      "root",
		Password:  "taosdata",
		TZ:        "Asia/Shanghai",
		App:       "ws_test_conn_protocol",
		IP:        "192.168.44.55",
		Connector: "ws_test_connector_info",
	}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp connResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	// check connection options
	got := false
	for i := 0; i < 10; i++ {
		queryResp := restQuery("select conn_id from performance_schema.perf_connections where user_app = 'ws_test_conn_protocol' and user_ip = '192.168.44.55' and connector_info = 'ws_test_connector_info'", "")
		if queryResp.Code == 0 && len(queryResp.Data) > 0 {
			got = true
			break
		}
		time.Sleep(time.Second)
	}
	assert.True(t, got)
}

func TestWSConnectTotp(t *testing.T) {
	if !testenv.IsEnterpriseTest() {
		t.Skip("totp test only for enterprise edition")
		return
	}
	user := "ws_test_totp_user"
	totpSeed := "iomwzmh6iRQ86jGq"
	pass := "k163MxPDrhHCqoNC"
	code, message := doRestful(fmt.Sprintf("create user %s pass '%s' TOTPSEED '%s'", user, pass, totpSeed), "")
	assert.Equal(t, 0, code, message)
	defer func() {
		code, message = doRestful("drop user ws_test_totp_user", "")
		assert.Equal(t, 0, code, message)
	}()
	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()
	totpSecret := otp.GenerateTOTPSecret([]byte(totpSeed))
	totpCode := otp.GenerateTOTPCode(totpSecret, uint64(time.Now().Unix())/30, 6)
	totpCodeStr := strconv.Itoa(totpCode)
	// connect
	connReq := connRequest{ReqID: 1, User: user, Password: pass, TOTPCode: totpCodeStr}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp connResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)
	//duplicate connections
	resp, err = doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0xffff, connResp.Code)
	assert.Equal(t, "duplicate connections", connResp.Message)
	assert.Equal(t, version.TaosClientVersion, connResp.Version)
}

func TestWSConnectToken(t *testing.T) {
	if !testenv.IsEnterpriseTest() {
		t.Skip("token test only for enterprise edition")
		return
	}
	user := "ws_test_token_user"
	pass := "N@6W$KOMF#2N7dMh"
	code, message := doRestful(fmt.Sprintf("create user %s pass '%s'", user, pass), "")
	assert.Equal(t, 0, code, message)
	defer func() {
		code, message = doRestful(fmt.Sprintf("drop user %s", user), "")
		assert.Equal(t, 0, code, message)
	}()
	createTokenResp := restQuery(fmt.Sprintf("create token test_token_ws_conn from user %s", user), "")
	if createTokenResp.Code != 0 {
		t.Errorf("create token failed: %d,%s", createTokenResp.Code, createTokenResp.Desc)
		return
	}
	token := createTokenResp.Data[0][0].(string)
	assert.NoError(t, testtools.EnsureTokenCreated("test_token_ws_conn"))
	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()
	// connect
	connReq := connRequest{ReqID: 1, BearerToken: token}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp connResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)
	//duplicate connections
	resp, err = doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0xffff, connResp.Code)
	assert.Equal(t, "duplicate connections", connResp.Message)
	assert.Equal(t, version.TaosClientVersion, connResp.Version)

	// with token query param
	ws2, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws?token="+token, nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws2.Close()
		assert.NoError(t, err)
	}()
	resp, err = doWebSocket(ws2, Connect, &connReq)
	assert.NoError(t, err)
	var connResp2 connResponse
	err = json.Unmarshal(resp, &connResp2)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp2.ReqID)
	assert.NotEqual(t, uint64(0), connResp2.Code)
}
