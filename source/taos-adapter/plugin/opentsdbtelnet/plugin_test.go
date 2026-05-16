package opentsdbtelnet_test

import (
	"database/sql/driver"
	"fmt"
	"math/rand"
	"net"
	"testing"
	"time"
	"unsafe"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/plugin/opentsdbtelnet"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
)

// @author: xftan
// @date: 2021/12/14 15:08
// @description: test opentsdb_telnet plugin
func TestPlugin(t *testing.T) {
	//nolint:staticcheck
	rand.Seed(time.Now().UnixNano())
	p := &opentsdbtelnet.Plugin{}
	config.Init()
	log.ConfigLog()
	db.PrepareConnection()
	logger := log.GetLogger("test")
	isDebug := log.IsDebug()
	viper.Set("opentsdb_telnet.enable", true)
	viper.Set("opentsdb_telnet.batchSize", 1)
	viper.Set("opentsdb_telnet.ttl", 1000)
	conn, err := syncinterface.TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		syncinterface.TaosClose(conn, logger, isDebug)
	}()
	err = exec(conn, "create database if not exists opentsdb_telnet")
	assert.NoError(t, err)
	assert.NoError(t, testtools.EnsureDBCreated("opentsdb_telnet"))
	err = p.Init(nil)
	assert.NoError(t, err)
	err = p.Start()
	assert.NoError(t, err)
	defer func() {
		err = p.Stop()
		assert.NoError(t, err)
	}()
	number := rand.Int31()
	c, err := net.Dial("tcp", "127.0.0.1:6046")
	assert.NoError(t, err)
	defer func() {
		err = c.Close()
		assert.NoError(t, err)
	}()
	_, err = fmt.Fprintf(c, "put sys.if.bytes.out 1479496100 %d host=web01 interface=eth0\r\n", number)
	assert.NoError(t, err)

	var values [][]driver.Value
	for i := 0; i < 100; i++ {
		checkSql := "select * from information_schema.ins_tables where db_name='opentsdb_telnet' and stable_name='sys_if_bytes_out'"
		values, err = query(conn, checkSql)
		require.NoError(t, err)
		if len(values) > 0 {
			break
		}
		time.Sleep(time.Millisecond * 500)
	}
	require.Equal(t, 1, len(values))

	defer func() {
		err = exec(conn, "drop database if exists opentsdb_telnet")
		assert.NoError(t, err)
	}()
	values, err = query(conn, "select last(_value) from opentsdb_telnet.`sys_if_bytes_out`")
	assert.NoError(t, err)
	if int32(values[0][0].(float64)) != number {
		t.Errorf("got %f expect %d", values[0], number)
	}
	for i := 0; i < 10; i++ {
		values, err = query(conn, "select `ttl` from information_schema.ins_tables "+
			" where db_name='opentsdb_telnet' and stable_name='sys_if_bytes_out'")
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
