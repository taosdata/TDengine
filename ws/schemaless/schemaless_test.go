package schemaless

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/testenv"
	"github.com/taosdata/driver-go/v3/common/testtool"
	taosErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/ws/client"
)

// @author: xftan
// @date: 2023/10/13 11:35
// @description: test websocket schemaless insert
func TestSchemaless_Insert(t *testing.T) {
	cases := []struct {
		name      string
		protocol  int
		precision string
		data      string
		ttl       int
		code      int
	}{
		{
			name:      "influxdb",
			protocol:  InfluxDBLineProtocol,
			precision: "ms",
			data: "measurement,host=host1 field1=2i,field2=2.0 1577837300000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837400000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837500000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837600000",
			ttl: 1000,
		},
		{
			name:      "opentsdb_telnet",
			protocol:  OpenTSDBTelnetLineProtocol,
			precision: "ms",
			data: "meters.current 1648432611249 10.3 location=California.SanFrancisco group=2\n" +
				"meters.current 1648432611250 12.6 location=California.SanFrancisco group=2\n" +
				"meters.current 1648432611251 10.8 location=California.LosAngeles group=3\n" +
				"meters.current 1648432611252 11.3 location=California.LosAngeles group=3\n",
			ttl: 1000,
		},
		{
			name:      "opentsdb_json",
			protocol:  OpenTSDBJsonFormatProtocol,
			precision: "ms",
			data: "[{\"metric\": \"meters.voltage\", \"timestamp\": 1648432611249, \"value\": 219, \"tags\": " +
				"{\"location\": \"California.LosAngeles\", \"groupid\": 1 } }, {\"metric\": \"meters.voltage\", " +
				"\"timestamp\": 1648432611250, \"value\": 221, \"tags\": {\"location\": \"California.LosAngeles\", " +
				"\"groupid\": 1 } }]",
			ttl: 100,
		},
	}

	if err := before(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = after() }()

	s, err := NewSchemaless(NewConfig("ws://localhost:6041", 1,
		SetDb("test_schemaless_ws"),
		SetReadTimeout(10*time.Second),
		SetWriteTimeout(10*time.Second),
		SetUser("root"),
		SetPassword("taosdata"),
		SetEnableCompression(true),
		SetErrorHandler(func(err error) {
			t.Log(err)
		}),
	))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := s.Insert(c.data, c.protocol, c.precision, c.ttl, 0); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func doRequest(sql string) error {
	req, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1:6041/rest/sql", strings.NewReader(sql))
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http code: %d", resp.StatusCode)
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	iter := client.JsonI.BorrowIterator(data)
	code := int32(0)
	desc := ""
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, s string) bool {
		switch s {
		case "code":
			code = iter.ReadInt32()
		case "desc":
			desc = iter.ReadString()
		default:
			iter.Skip()
		}
		return iter.Error == nil
	})
	client.JsonI.ReturnIterator(iter)
	if code != 0 {
		if code == 0x3d3 {
			time.Sleep(100 * time.Millisecond)
			return doRequest(sql)
		}
		return taosErrors.NewError(int(code), desc)
	}
	return nil
}

func before() error {
	if err := doRequest("drop database if exists test_schemaless_ws"); err != nil {
		return err
	}
	return doRequest("create database if not exists test_schemaless_ws")
}

func after() error {
	return doRequest("drop database  test_schemaless_ws")
}

func newTaosadapter(port string) *exec.Cmd {
	command := "taosadapter"
	if runtime.GOOS == "windows" {
		command = "C:\\TDengine\\taosadapter.exe"

	}
	return exec.Command(command, "--port", port, "--logLevel", "debug")
}

func startTaosadapter(cmd *exec.Cmd, port string) error {
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		return err
	}
	for i := 0; i < 30; i++ {
		time.Sleep(time.Millisecond * 100)
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%s/-/ping", port))
		if err != nil {
			continue
		}
		_ = resp.Body.Close()
		time.Sleep(time.Second)
		return nil
	}
	return errors.New("taosadapter start failed")
}

func stopTaosadapter(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGINT)
	_, _ = cmd.Process.Wait()
	cmd.Process = nil
	time.Sleep(time.Second)
}

func TestSchemalessReconnect(t *testing.T) {
	port := "36041"
	cmd := newTaosadapter(port)
	err := startTaosadapter(cmd, port)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		stopTaosadapter(cmd)
	}()
	err = doRequest("drop database if exists test_schemaless_reconnect")
	if err != nil {
		t.Fatal(err)
	}
	err = doRequest("create database if not exists test_schemaless_reconnect")
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewSchemaless(NewConfig(fmt.Sprintf("ws://localhost:%s", port), 1,
		SetDb("test_schemaless_reconnect"),
		SetReadTimeout(3*time.Second),
		SetWriteTimeout(3*time.Second),
		SetUser("root"),
		SetPassword("taosdata"),
		//SetEnableCompression(true),
		SetErrorHandler(func(err error) {
			t.Log(err)
		}),
		SetAutoReconnect(true),
		SetReconnectIntervalMs(2000),
		SetReconnectRetryCount(3),
	))
	if err != nil {
		t.Fatal(err)
	}
	stopTaosadapter(cmd)
	time.Sleep(time.Second * 3)
	startChan := make(chan struct{})
	go func() {
		time.Sleep(time.Second * 10)
		cmd = newTaosadapter(port)
		err = startTaosadapter(cmd, port)
		startChan <- struct{}{}
		if err != nil {
			t.Error(err)
			return
		}
	}()
	data := "measurement,host=host1 field1=2i,field2=2.0 1577837300000\n" +
		"measurement,host=host1 field1=2i,field2=2.0 1577837400000\n" +
		"measurement,host=host1 field1=2i,field2=2.0 1577837500000\n" +
		"measurement,host=host1 field1=2i,field2=2.0 1577837600000"
	err = s.Insert(data, InfluxDBLineProtocol, "ms", 0, 0)
	assert.Error(t, err)
	<-startChan
	time.Sleep(time.Second)
	err = s.Insert(data, InfluxDBLineProtocol, "ms", 0, 0)
	assert.NoError(t, err)
	err = s.Insert(data, InfluxDBLineProtocol, "ms", 0, 0)
	assert.NoError(t, err)
}

func TestWrongNewSchemaless(t *testing.T) {
	s, err := NewSchemaless(NewConfig("://localhost:6041", 1,
		SetUser("root"),
		SetPassword("taosdata"),
	))
	assert.Error(t, err)
	assert.Nil(t, s)

	s, err = NewSchemaless(NewConfig("wrong://localhost:6041", 1,
		SetUser("root"),
		SetPassword("taosdata"),
	))
	assert.Error(t, err)
	assert.Nil(t, s)

	s, err = NewSchemaless(NewConfig("ws://localhost:6041", 1,
		SetUser("root"),
		SetPassword("wrongpassword"),
	))
	assert.Error(t, err)
	assert.Nil(t, s)

	s, err = NewSchemaless(NewConfig("ws://localhost:9999", 1,
		SetUser("root"),
		SetPassword("taosdata"),
	))
	assert.Error(t, err)
	assert.Nil(t, s)
}

func TestConnectorInfo(t *testing.T) {
	_, ok := os.LookupEnv("TD_3360_TEST")
	if ok {
		t.Skip("Skip 3.3.6.0 test")
	}
	s, err := NewSchemaless(NewConfig("ws://localhost:6041", 1,
		SetReadTimeout(10*time.Second),
		SetWriteTimeout(10*time.Second),
		SetUser("root"),
		SetPassword("taosdata"),
		SetEnableCompression(true),
	))
	require.NoError(t, err)
	defer func() {
		s.Close()
	}()
	app := common.GetProcessName()
	if len(app) > 23 {
		app = app[:23]
	}
	connectorInfo := common.GetConnectorInfo("ws")
	checkSql := fmt.Sprintf("select count(*) from performance_schema.perf_connections where user_app = '%s'  and connector_info = '%s'", app, connectorInfo)
	t.Log(checkSql)
	assert.Eventually(t, func() bool {
		resp, err := testtool.HTTPQuery(checkSql)
		if err != nil {
			return false
		}
		if len(resp.Data) == 0 || len(resp.Data[0]) == 0 {
			return false
		}
		count, ok := resp.Data[0][0].(int64)
		if !ok {
			return false
		}
		return count > 0
	}, 5*time.Second, 500*time.Millisecond)
}

func TestTotpCode(t *testing.T) {
	if !testenv.IsEnterpriseTest() {
		t.Skip("Skip totp test for community edition")
	}
	_, ok := os.LookupEnv("TD_3360_TEST")
	if ok {
		t.Skip("Skip 3.3.6.0 test")
	}
	seed := "Z7Xxoy5E8h9IuVIpTH684cFSzRNVVzgc"
	err := doRequest(fmt.Sprintf("create user totp_user_sml pass 'totp_pass_1' TOTPSEED '%s'", seed))
	require.NoError(t, err)
	defer func() {
		err = doRequest("drop user totp_user_sml")
		require.NoError(t, err)
	}()
	secret := common.GenerateTOTPSecret([]byte(seed))
	code := common.GenerateTOTPCode(secret, uint64(time.Now().Unix()/30), 6)
	s, err := NewSchemaless(NewConfig("ws://localhost:6041", 1,
		SetReadTimeout(10*time.Second),
		SetWriteTimeout(10*time.Second),
		SetUser("totp_user_sml"),
		SetPassword("totp_pass_1"),
		SetEnableCompression(true),
		SetTOTPCode(strconv.Itoa(code)),
	))
	require.NoError(t, err)
	defer func() {
		s.Close()
	}()
	app := common.GetProcessName()
	if len(app) > 23 {
		app = app[:23]
	}
	connectorInfo := common.GetConnectorInfo("ws")
	checkSql := fmt.Sprintf("select count(*) from performance_schema.perf_connections where user_app = '%s'  and connector_info = '%s' and `user` = 'totp_user_sml'", app, connectorInfo)
	t.Log(checkSql)
	assert.Eventually(t, func() bool {
		resp, err := testtool.HTTPQuery(checkSql)
		if err != nil {
			return false
		}
		if len(resp.Data) == 0 || len(resp.Data[0]) == 0 {
			return false
		}
		count, ok := resp.Data[0][0].(int64)
		if !ok {
			return false
		}
		return count > 0
	}, 5*time.Second, 500*time.Millisecond)
}

func TestBearerToken(t *testing.T) {
	if !testenv.IsEnterpriseTest() {
		t.Skip("Skip totp test for community edition")
	}
	_, ok := os.LookupEnv("TD_3360_TEST")
	if ok {
		t.Skip("Skip 3.3.6.0 test")
	}
	result, err := testtool.HTTPQuery("create token test_token_sml_ws from user root")
	require.NoError(t, err)
	token := result.Data[0][0].(string)
	defer func() {
		err = doRequest("drop token test_token_sml_ws")
		require.NoError(t, err)
	}()
	s, err := NewSchemaless(NewConfig("ws://localhost:6041", 1,
		SetReadTimeout(10*time.Second),
		SetWriteTimeout(10*time.Second),
		SetEnableCompression(true),
		SetBearerToken(token),
	))
	require.NoError(t, err)
	defer func() {
		s.Close()
	}()
	app := common.GetProcessName()
	if len(app) > 23 {
		app = app[:23]
	}
	connectorInfo := common.GetConnectorInfo("ws")
	checkSql := fmt.Sprintf("select count(*) from performance_schema.perf_connections where user_app = '%s'  and connector_info = '%s' and `user` = 'root'", app, connectorInfo)
	t.Log(checkSql)
	assert.Eventually(t, func() bool {
		resp, err := testtool.HTTPQuery(checkSql)
		if err != nil {
			return false
		}
		if len(resp.Data) == 0 || len(resp.Data[0]) == 0 {
			return false
		}
		count, ok := resp.Data[0][0].(int64)
		if !ok {
			return false
		}
		return count > 0
	}, 5*time.Second, 500*time.Millisecond)
}
