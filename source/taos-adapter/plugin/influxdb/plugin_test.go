package influxdb

import (
	"database/sql/driver"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
	"github.com/taosdata/taosadapter/v3/tools/testtools/testenv"
)

var router *gin.Engine

func TestMain(m *testing.M) {
	viper.Set("smlAutoCreateDB", true)
	defer viper.Set("smlAutoCreateDB", false)
	viper.Set("influxdb.enable", true)
	config.Init()
	log.ConfigLog()
	db.PrepareConnection()
	router = gin.Default()
	p := Influxdb{}
	err := p.Init(router)
	if err != nil {
		panic(err)
	}
	err = p.Start()
	if err != nil {
		panic(err)
	}
	code := m.Run()
	err = p.Stop()
	if err != nil {
		panic(err)
	}
	os.Exit(code)
}

// @author: xftan
// @date: 2021/12/14 15:07
// @description: test influxdb plugin
func TestInfluxdb(t *testing.T) {
	logger := log.GetLogger("test")
	isDebug := log.IsDebug()
	conn, err := syncinterface.TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		syncinterface.TaosClose(conn, logger, isDebug)
	}()
	err = exec(conn, "drop database if exists test_plugin_influxdb")
	assert.NoError(t, err)
	err = exec(conn, "drop database if exists test_plugin_influxdb_ttl")
	assert.NoError(t, err)
	defer func() {
		err = exec(conn, "drop database if exists test_plugin_influxdb")
		assert.NoError(t, err)
		err = exec(conn, "drop database if exists test_plugin_influxdb_ttl")
		assert.NoError(t, err)
	}()
	number := rand.Int31()
	require.Eventually(t, func() bool {
		w := httptest.NewRecorder()
		reader := strings.NewReader(fmt.Sprintf("measurement,host=host1 field1=%di,field2=2.0,fieldKey=\"Launch 🚀\" %d", number, time.Now().UnixNano()))
		req, _ := http.NewRequest("POST", "/write?u=root&p=taosdata&db=test_plugin_influxdb&app=test_influxdb", reader)
		req.RemoteAddr = testtools.GetRandomRemoteAddr()
		router.ServeHTTP(w, req)
		return w.Code == 204
	}, 10*time.Second, 500*time.Millisecond)
	w := httptest.NewRecorder()
	reader := strings.NewReader("measurement,host=host1 field1=a1")
	req, _ := http.NewRequest("POST", "/write?u=root&p=taosdata&db=test_plugin_influxdb&app=test_influxdb", reader)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	router.ServeHTTP(w, req)
	assert.Equal(t, 500, w.Code)
	w = httptest.NewRecorder()
	reader = strings.NewReader(fmt.Sprintf("measurement,host=host1 field1=%di,field2=2.0,fieldKey=\"Launch 🚀\" %d", number, time.Now().UnixNano()))
	req, _ = http.NewRequest("POST", "/write?u=root&p=taosdata&app=test_influxdb", reader)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	router.ServeHTTP(w, req)
	assert.Equal(t, 400, w.Code)
	time.Sleep(time.Second)

	var values [][]driver.Value
	assert.Eventually(t, func() bool {
		values, err = query(conn, "select * from information_schema.ins_tables where db_name='test_plugin_influxdb' and stable_name='measurement'")
		return err == nil && len(values) == 1
	}, 10*time.Second, 500*time.Millisecond)

	values, err = query(conn, "select * from test_plugin_influxdb.`measurement`")
	assert.NoError(t, err)
	if values[0][3].(string) != "Launch 🚀" {
		t.Errorf("got %s expect %s", values[0][3], "Launch 🚀")
		return
	}
	if int32(values[0][1].(int64)) != number {
		t.Errorf("got %d expect %d", values[0][1].(int64), number)
		return
	}

	w = httptest.NewRecorder()
	reader = strings.NewReader(fmt.Sprintf("measurement_ttl,host=host1 field1=%di,field2=2.0,fieldKey=\"Launch 🚀\" %d", number, time.Now().UnixNano()))
	req, _ = http.NewRequest("POST", "/write?u=root&p=taosdata&db=test_plugin_influxdb_ttl&ttl=1000&app=test_influxdb", reader)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	router.ServeHTTP(w, req)
	for i := 0; i < 10; i++ {
		values, err = query(conn, "select `ttl` from information_schema.ins_tables "+
			" where db_name='test_plugin_influxdb_ttl' and stable_name='measurement_ttl'")
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	assert.NoError(t, err)
	if values[0][0].(int32) != 1000 {
		t.Fatal("ttl miss")
	}
}

func TestInfAndNaN(t *testing.T) {
	logger := log.GetLogger("test")
	isDebug := log.IsDebug()

	conn, err := syncinterface.TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		syncinterface.TaosClose(conn, logger, isDebug)
	}()
	err = exec(conn, "drop database if exists test_plugin_influxdb_inf_nan")
	assert.NoError(t, err)
	defer func() {
		err = exec(conn, "drop database if exists test_plugin_influxdb_inf_nan")
		assert.NoError(t, err)
	}()
	w := httptest.NewRecorder()

	now := time.Now()
	reader := strings.NewReader(fmt.Sprintf(
		"measurement,host=host1 field1=1i,field2=2.0 %d\n"+
			"measurement,host=host1 field1=1i,field2=nan %d\n"+
			"measurement,host=host1 field1=1i,field2=inf %d",
		now.UnixNano(),
		now.Add(time.Second).UnixNano(),
		now.Add(time.Second*2).UnixNano(),
	))
	req, _ := http.NewRequest("POST", "/write?u=root&p=taosdata&db=test_plugin_influxdb_inf_nan&app=test_influxdb", reader)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	router.ServeHTTP(w, req)
	assert.Equal(t, 204, w.Code)
	var values [][]driver.Value
	assert.Eventually(t, func() bool {
		values, err = query(conn, "select * from test_plugin_influxdb_inf_nan.`measurement` order by _ts asc")
		return err == nil && len(values) == 3
	}, 2*time.Second, 200*time.Millisecond)

	assert.Equal(t, float64(2), values[0][2].(float64))
	assert.True(t, math.IsNaN(values[1][2].(float64)))
	assert.True(t, math.IsInf(values[2][2].(float64), 1))
}

func TestToken(t *testing.T) {
	if !testenv.IsEnterpriseTest() {
		t.Skip("totp test only for enterprise edition")
		return
	}
	logger := log.GetLogger("test")
	isDebug := log.IsDebug()
	conn, err := syncinterface.TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		syncinterface.TaosClose(conn, logger, isDebug)
	}()
	err = exec(conn, "drop database if exists test_plugin_influxdb_token")
	assert.NoError(t, err)
	res, err := query(conn, "create token test_influxdb_token from user root")
	assert.NoError(t, err)
	token := res[0][0].(string)
	defer func() {
		err = exec(conn, "drop database if exists test_plugin_influxdb_token")
		assert.NoError(t, err)
		_, err = query(conn, "drop token test_influxdb_token")
		assert.NoError(t, err)
	}()

	require.Eventually(t, func() bool {
		w := httptest.NewRecorder()
		reader := strings.NewReader(fmt.Sprintf("measurement,host=host1 field1=%di,field2=2.0,fieldKey=\"Launch 🚀\" %d", 1, time.Now().UnixNano()))
		req, _ := http.NewRequest("POST", "/write?db=test_plugin_influxdb_token", reader)
		req.RemoteAddr = testtools.GetRandomRemoteAddr()
		req.Header.Set("Authorization", "Bearer "+token)
		router.ServeHTTP(w, req)
		return w.Code == 204
	}, 10*time.Second, 500*time.Millisecond)

	var values [][]driver.Value
	assert.Eventually(t, func() bool {
		values, err = query(conn, "select * from information_schema.ins_tables where db_name='test_plugin_influxdb_token' and stable_name='measurement'")
		return err == nil && len(values) == 1
	}, 10*time.Second, 500*time.Millisecond)

	values, err = query(conn, "select * from test_plugin_influxdb_token.`measurement`")
	assert.NoError(t, err)
	if values[0][3].(string) != "Launch 🚀" {
		t.Errorf("got %s expect %s", values[0][3], "Launch 🚀")
		return
	}
}

func exec(conn unsafe.Pointer, sql string) error {
	logger := log.GetLogger("test")
	logger.Debugf("exec sql %s", sql)
	return testtools.Exec(conn, sql)
}

func query(conn unsafe.Pointer, sql string) ([][]driver.Value, error) {
	logger := log.GetLogger("test")
	logger.Debugf("query sql %s", sql)
	return testtools.Query(conn, sql)
}
