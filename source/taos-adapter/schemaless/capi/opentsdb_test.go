package capi_test

import (
	"fmt"
	"os"
	"testing"
	"time"
	"unsafe"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/schemaless/capi"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
)

func TestMain(m *testing.M) {
	viper.Set("logLevel", "trace")
	viper.Set("uploadKeeper.enable", false)
	config.Init()
	log.ConfigLog()
	db.PrepareConnection()
	os.Exit(m.Run())
}

// @author: xftan
// @date: 2021/12/14 15:11
// @description: test insert opentsdb telnet
func TestInsertOpentsdbTelnet(t *testing.T) {
	logger := log.GetLogger("test").WithField("test", "TestInsertOpentsdbTelnet")
	isDebug := log.IsDebug()
	conn, err := syncinterface.TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if err != nil {
		t.Error(err)
		return
	}
	defer syncinterface.TaosClose(conn, logger, isDebug)
	defer func() {
		r := syncinterface.TaosQuery(conn, "drop database if exists test_capi_opentsdb", logger, isDebug)
		code := syncinterface.TaosError(r, logger, isDebug)
		if code != 0 {
			errStr := syncinterface.TaosErrorStr(r, logger, isDebug)
			t.Error(errors.NewError(code, errStr))
		}
		syncinterface.TaosSyncQueryFree(r, logger, isDebug)
	}()
	r := syncinterface.TaosQuery(conn, "create database if not exists test_capi_opentsdb", logger, isDebug)
	code := syncinterface.TaosError(r, logger, isDebug)
	if code != 0 {
		errStr := syncinterface.TaosErrorStr(r, logger, isDebug)
		t.Error(errors.NewError(code, errStr))
	}
	syncinterface.TaosSyncQueryFree(r, logger, isDebug)
	assert.NoError(t, testtools.EnsureDBCreated("test_capi_opentsdb"))

	type args struct {
		taosConnect unsafe.Pointer
		data        string
		db          string
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
				data:        "df.data.df_complex.used 1636539620 21393473536 fqdn=vm130  status=production",
				db:          "test_capi_opentsdb",
				ttl:         100,
			},
			wantErr: false,
		}, {
			name: "wrong",
			args: args{
				taosConnect: conn,
				data:        "df.data.df_complex.used 163653962000 21393473536 fqdn=vm130  status=production",
				db:          "test_capi_opentsdb",
			},
			wantErr: true,
		}, {
			name: "wrongdb",
			args: args{
				taosConnect: conn,
				data:        "df.data.df_complex.used 1636539620 21393473536 fqdn=vm130  status=production",
				db:          "1'test_capi_opentsdb",
				ttl:         1000,
			},
			wantErr: true,
		}, {
			name: "nodata",
			args: args{
				taosConnect: conn,
				data:        "",
				db:          "test_capi_opentsdb",
				ttl:         1000,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := capi.InsertOpentsdbTelnet(tt.args.taosConnect, []string{tt.args.data}, tt.args.db, tt.args.ttl, 0, "", logger.WithField("name", tt.name)); (err != nil) != tt.wantErr {
				t.Errorf("InsertOpentsdbTelnet() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func BenchmarkTelnet(b *testing.B) {
	logger := log.GetLogger("test").WithField("test", "BenchmarkTelnet")
	isDebug := log.IsDebug()
	conn, err := syncinterface.TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if err != nil {
		b.Error(err)
		return
	}
	defer syncinterface.TaosClose(conn, logger, isDebug)
	defer func() {
		r := syncinterface.TaosQuery(conn, "drop database if exists test", logger, isDebug)
		code := syncinterface.TaosError(r, logger, isDebug)
		if code != 0 {
			errStr := syncinterface.TaosErrorStr(r, logger, isDebug)
			b.Error(errors.NewError(code, errStr))
		}
		syncinterface.TaosSyncQueryFree(r, logger, isDebug)
	}()
	for i := 0; i < b.N; i++ {
		//`sys.if.bytes.out`,`host`=web01,`interface`=eth0
		//t_98df8453856519710bfc2f1b5f8202cf
		//t_98df8453856519710bfc2f1b5f8202cf
		err := capi.InsertOpentsdbTelnet(conn, []string{`put sys.if.bytes.out 1479496100 1.3E3 host=web01 interface=eth0`}, "test", 0, 0, "", logger)
		if err != nil {
			b.Error(err)
		}
	}
}

// @author: xftan
// @date: 2021/12/14 15:12
// @description: test insert opentsdb json
func TestInsertOpentsdbJson(t *testing.T) {
	logger := log.GetLogger("test").WithField("test", "TestInsertOpentsdbJson")
	isDebug := log.IsDebug()
	conn, err := syncinterface.TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if err != nil {
		t.Error(err)
		return
	}
	now := time.Now().Unix()
	defer syncinterface.TaosClose(conn, logger, isDebug)
	defer func() {
		r := syncinterface.TaosQuery(conn, "drop database if exists test_capi_opentsdb_json", logger, isDebug)
		code := syncinterface.TaosError(r, logger, isDebug)
		if code != 0 {
			errStr := syncinterface.TaosErrorStr(r, logger, isDebug)
			t.Error(errors.NewError(code, errStr))
		}
		syncinterface.TaosSyncQueryFree(r, logger, isDebug)
	}()
	r := syncinterface.TaosQuery(conn, "create database if not exists test_capi_opentsdb_json", logger, isDebug)
	code := syncinterface.TaosError(r, logger, isDebug)
	if code != 0 {
		errStr := syncinterface.TaosErrorStr(r, logger, isDebug)
		t.Error(errors.NewError(code, errStr))
	}
	syncinterface.TaosSyncQueryFree(r, logger, isDebug)
	assert.NoError(t, testtools.EnsureDBCreated("test_capi_opentsdb_json"))
	type args struct {
		taosConnect unsafe.Pointer
		data        []byte
		db          string
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
				data: []byte(fmt.Sprintf(`
{
    "metric": "sys.cpu.nice",
    "timestamp": %d,
    "value": 18,
    "tags": {
       "host": "web01",
       "dc": "lga"
    }
}`, now)),
				db:  "test_capi_opentsdb_json",
				ttl: 100,
			},
			wantErr: false,
		}, {
			name: "wrong",
			args: args{
				taosConnect: conn,
				data: []byte(fmt.Sprintf(`
{
    "metric": "sys.cpu.nice",
    "timestamp": %d,
    "value": 18,
    "tags": {
       "host": "web01",
       "dc": "lga"
    }
}`, now)),
				db:  "test_capi_opentsdb_json",
				ttl: 0,
			},
			wantErr: false,
		}, {
			name: "wrongdb",
			args: args{
				taosConnect: conn,
				data: []byte(`
{
    "metric": "sys.cpu.nice",
    "timestamp": 1346846400,
    "value": 18,
    "tags": {
       "host": "web01",
       "dc": "lga"
    }
}`),
				db: "1'test_capi_opentsdb_json",
			},
			wantErr: true,
		}, {
			name: "nodata",
			args: args{
				taosConnect: conn,
				data:        nil,
				db:          "test_capi_opentsdb_json",
				ttl:         1000,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := capi.InsertOpentsdbJson(tt.args.taosConnect, tt.args.data, tt.args.db, tt.args.ttl, 0, "", logger); (err != nil) != tt.wantErr {
				t.Errorf("InsertOpentsdbJson() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestInsertOpentsdbTelnetBatch(t *testing.T) {
	logger := log.GetLogger("test").WithField("test", "TestInsertOpentsdbTelnetBatch")
	isDebug := log.IsDebug()
	conn, err := syncinterface.TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if err != nil {
		t.Error(err)
		return
	}
	defer syncinterface.TaosClose(conn, logger, isDebug)
	defer func() {
		r := syncinterface.TaosQuery(conn, "drop database if exists test_capi_opentsdb_batch", logger, isDebug)
		code := syncinterface.TaosError(r, logger, isDebug)
		if code != 0 {
			errStr := syncinterface.TaosErrorStr(r, logger, isDebug)
			t.Error(errors.NewError(code, errStr))
		}
		syncinterface.TaosSyncQueryFree(r, logger, isDebug)
	}()
	r := syncinterface.TaosQuery(conn, "create database if not exists test_capi_opentsdb_batch", logger, isDebug)
	code := syncinterface.TaosError(r, logger, isDebug)
	if code != 0 {
		errStr := syncinterface.TaosErrorStr(r, logger, isDebug)
		t.Error(errors.NewError(code, errStr))
	}
	syncinterface.TaosSyncQueryFree(r, logger, isDebug)
	assert.NoError(t, testtools.EnsureDBCreated("test_capi_opentsdb_batch"))
	type args struct {
		taosConnect unsafe.Pointer
		data        []string
		db          string
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
				data: []string{
					"df.data.df_complex.used 1636539620 21393473536 fqdn=vm130  status=production",
					"df.data.df_complex.used 1636539621 21393473536 fqdn=vm129  status=production",
				},
				db:  "test_capi_opentsdb_batch",
				ttl: 100,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := capi.InsertOpentsdbTelnet(tt.args.taosConnect, tt.args.data, tt.args.db, tt.args.ttl, 0, "", logger); (err != nil) != tt.wantErr {
				t.Errorf("InsertOpentsdbTelnet() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
