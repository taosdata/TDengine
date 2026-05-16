package statsd

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
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
)

// @author: xftan
// @date: 2021/12/14 15:08
// @description: test statsd plugin
func TestStatsd(t *testing.T) {
	//nolint:staticcheck
	rand.Seed(time.Now().UnixNano())
	p := &Plugin{}
	config.Init()
	log.ConfigLog()
	db.PrepareConnection()
	logger := log.GetLogger("test")
	isDebug := log.IsDebug()

	viper.Set("statsd.gatherInterval", time.Millisecond)
	viper.Set("statsd.enable", true)
	viper.Set("statsd.ttl", 1000)
	conn, err := syncinterface.TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		syncinterface.TaosClose(conn, logger, isDebug)
	}()
	err = exec(conn, "create database if not exists statsd")
	assert.NoError(t, err)
	assert.NoError(t, testtools.EnsureDBCreated("statsd"))
	err = p.Init(nil)
	assert.NoError(t, err)
	err = p.Start()
	assert.NoError(t, err)
	defer func() {
		err = p.Stop()
		assert.NoError(t, err)
	}()
	number := rand.Int31()
	//	echo "foo:1|c" | nc -u -w0 127.0.0.1 8125
	c, err := net.Dial("udp", "127.0.0.1:6044")
	assert.NoError(t, err)
	_, err = fmt.Fprintf(c, "foo:%d|c", number)
	assert.NoError(t, err)
	defer func() {
		err = exec(conn, "drop database if exists statsd")
		assert.NoError(t, err)
	}()
	var values [][]driver.Value
	assert.Eventually(t, func() bool {
		values, err = query(conn, "select * from information_schema.ins_tables where db_name='statsd' and stable_name='foo'")
		return err == nil && len(values) == 1
	}, 10*time.Second, 500*time.Millisecond)
	values, err = query(conn, "select last(`value`) from statsd.`foo`")
	assert.NoError(t, err)
	if int32(values[0][0].(int64)) != number {
		t.Errorf("got %f expect %d", values[0], number)
	}
	for i := 0; i < 10; i++ {
		values, err = query(conn, "select `ttl` from information_schema.ins_tables "+
			" where db_name='statsd' and stable_name='foo'")
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
