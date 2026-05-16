package collectd

import (
	"context"
	"database/sql/driver"
	"math/rand"
	"net"
	"testing"
	"time"
	"unsafe"

	"collectd.org/api"
	"collectd.org/network"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
)

// @author: xftan
// @date: 2021/12/14 15:07
// @description: test collectd plugin
func TestCollectd(t *testing.T) {
	config.Init()
	db.PrepareConnection()
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
	err = exec(conn, "drop database if exists collectd")
	assert.NoError(t, err)
	err = exec(conn, "create database if not exists collectd")
	assert.NoError(t, err)
	assert.NoError(t, testtools.EnsureDBCreated("collectd"))
	//nolint:staticcheck
	rand.Seed(time.Now().UnixNano())
	p := &Plugin{}
	viper.Set("collectd.enable", true)
	viper.Set("collectd.ttl", 1000)
	err = p.Init(nil)
	assert.NoError(t, err)
	err = p.Start()
	assert.NoError(t, err)
	defer func() {
		err = p.Stop()
		assert.NoError(t, err)
	}()
	number := rand.Int31()
	data := api.ValueList{
		Identifier: api.Identifier{
			Host:           "xyzzy",
			Plugin:         "cpu",
			PluginInstance: "0",
			Type:           "cpu",
			TypeInstance:   "user",
		},
		Values: []api.Value{
			api.Derive(number),
		},
		DSNames: []string{"t1", "t2"},
	}
	buffer := network.NewBuffer(0)

	ctx := context.Background()
	err = buffer.Write(ctx, &data)
	assert.NoError(t, err)
	bytes, err := buffer.Bytes()
	assert.NoError(t, err)
	c, err := net.Dial("udp", "127.0.0.1:6045")
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = c.Close()
		assert.NoError(t, err)
	}()
	_, err = c.Write(bytes)
	if err != nil {
		t.Error(err)
		return
	}

	var values [][]driver.Value
	assert.Eventually(t, func() bool {
		values, err = query(conn, "select * from information_schema.ins_tables where db_name='collectd' and stable_name='cpu_value'")
		return err == nil && len(values) == 1
	}, 10*time.Second, 500*time.Millisecond)

	defer func() {
		r := syncinterface.TaosQuery(conn, "drop database if exists collectd", logger, isDebug)
		code := syncinterface.TaosError(r, logger, isDebug)
		if code != 0 {
			errStr := syncinterface.TaosErrorStr(r, logger, isDebug)
			t.Error(errors.NewError(code, errStr))
		}
		syncinterface.TaosSyncQueryFree(r, logger, isDebug)
	}()
	values, err = query(conn, "select last(`value`) from collectd.`cpu_value`")
	assert.NoError(t, err)
	if int32(values[0][0].(float64)) != number {
		t.Errorf("got %f expect %d", values[0], number)
	}
	for i := 0; i < 10; i++ {
		values, err = query(conn, "select `ttl` from information_schema.ins_tables "+
			" where db_name='collectd' and stable_name='cpu_value'")
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
