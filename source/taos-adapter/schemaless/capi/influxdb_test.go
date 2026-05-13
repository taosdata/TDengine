package capi_test

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/schemaless/capi"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
)

// @author: xftan
// @date: 2021/12/14 15:11
// @description: test insert influxdb
func TestInsertInfluxdb(t *testing.T) {
	logger := log.GetLogger("test").WithField("test", "TestInsertInfluxdb")
	isDebug := log.IsDebug()
	conn, err := syncinterface.TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if err != nil {
		t.Error(err)
		return
	}
	defer syncinterface.TaosClose(conn, logger, isDebug)
	defer func() {
		r := syncinterface.TaosQuery(conn, "drop database if exists test_capi_influxdb", logger, isDebug)
		code := syncinterface.TaosError(r, logger, isDebug)
		if code != 0 {
			errStr := syncinterface.TaosErrorStr(r, logger, isDebug)
			t.Error(errors.NewError(code, errStr))
		}
		syncinterface.TaosSyncQueryFree(r, logger, isDebug)
	}()
	r := syncinterface.TaosQuery(conn, "create database if not exists test_capi_influxdb", logger, isDebug)
	code := syncinterface.TaosError(r, logger, isDebug)
	if code != 0 {
		errStr := syncinterface.TaosErrorStr(r, logger, isDebug)
		t.Error(errors.NewError(code, errStr))
	}
	syncinterface.TaosSyncQueryFree(r, logger, isDebug)
	assert.NoError(t, testtools.EnsureDBCreated("test_capi_influxdb"))
	type args struct {
		taosConnect unsafe.Pointer
		data        []byte
		db          string
		precision   string
		ttl         int
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test",
			args: args{
				taosConnect: conn,
				data:        []byte("measurement,host=host1 field1=2i,field2=2.0,fieldKey=\"Launch 🚀\" 1577836800000000001"),
				db:          "test_capi_influxdb",
				ttl:         0,
			},
			wantErr: false,
		}, {
			name: "wrong",
			args: args{
				taosConnect: conn,
				data:        []byte("wrong,host=host1 field1=wrong 1577836800000000001"),
				db:          "test_capi_influxdb",
				ttl:         100,
			},
			wantErr: true,
		}, {
			name: "wrongdb",
			args: args{
				taosConnect: conn,
				data:        []byte("wrong,host=host1 field1=wrong 1577836800000000001"),
				db:          "1'test_capi_influxdb",
				ttl:         1000,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := log.GetLogger("test").WithField("test", "TestInsertInfluxdb").WithField("name", tt.name)
			err := capi.InsertInfluxdb(tt.args.taosConnect, tt.args.data, tt.args.db, tt.args.precision, tt.args.ttl, 0, "", logger)
			if (err != nil) != tt.wantErr {
				t.Errorf("InsertInfluxdb() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
