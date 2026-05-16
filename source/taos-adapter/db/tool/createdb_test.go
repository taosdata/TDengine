package tool

import (
	"fmt"
	"os"
	"testing"
	"unsafe"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	tErrors "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/log"
)

func TestMain(m *testing.M) {
	viper.Set("smlAutoCreateDB", true)
	viper.Set("logLevel", "trace")
	config.Init()
	log.ConfigLog()
	db.PrepareConnection()
	code := m.Run()
	viper.Set("smlAutoCreateDB", false)
	os.Exit(code)
}

// @author: xftan
// @date: 2021/12/14 15:05
// @description: test creat database with connection
func TestCreateDBWithConnection(t *testing.T) {
	logger := log.GetLogger("test").WithField("test", "TestCreateDBWithConnection")
	isDebug := log.IsDebug()
	conn, err := syncinterface.TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if err != nil {
		t.Error(err)
		return
	}
	defer syncinterface.TaosClose(conn, logger, isDebug)
	type args struct {
		connection unsafe.Pointer
		db         string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test",
			args: args{
				connection: conn,
				db:         "test_auto_create_db",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := log.GetLogger("test").WithField("test", "TestCreateDBWithConnection")
			isDebug := log.IsDebug()
			if err := CreateDBWithConnection(tt.args.connection, log.GetLogger("test").WithField("test", "TestCreateDBWithConnection"), false, tt.args.db, 0); (err != nil) != tt.wantErr {
				t.Errorf("CreateDBWithConnection() error = %v, wantErr %v", err, tt.wantErr)
			}
			code := syncinterface.TaosSelectDB(tt.args.connection, tt.args.db, logger, isDebug)
			assert.Equal(t, 0, code)
			r := syncinterface.TaosQuery(conn, "drop database if exists test_auto_create_db", logger, isDebug)
			code = syncinterface.TaosError(r, logger, isDebug)
			if code != 0 {
				errStr := syncinterface.TaosErrorStr(r, logger, isDebug)
				t.Error(tErrors.NewError(code, errStr))
			}
			syncinterface.TaosSyncQueryFree(r, logger, isDebug)
		})
	}
}

// @author: xftan
// @date: 2021/12/14 15:12
// @description:  test selectDB
func TestSchemalessSelectDB(t *testing.T) {
	logger := log.GetLogger("test").WithField("test", "TestSchemalessSelectDB")
	isDebug := log.IsDebug()
	conn, err := syncinterface.TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if err != nil {
		t.Error(err)
		return
	}
	defer syncinterface.TaosClose(conn, logger, isDebug)
	type args struct {
		taosConnect unsafe.Pointer
		db          string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "auto create database",
			args: args{
				taosConnect: conn,
				db:          "auto_create_db_test",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := log.GetLogger("test").WithField("test", "TestSchemalessSelectDB")
			isDebug := log.IsDebug()
			result := syncinterface.TaosQuery(tt.args.taosConnect, "drop database if exists "+tt.args.db, logger, isDebug)
			code := syncinterface.TaosError(result, logger, isDebug)
			if code != 0 {
				errStr := syncinterface.TaosErrorStr(result, logger, isDebug)
				syncinterface.TaosSyncQueryFree(result, logger, isDebug)
				t.Error(tErrors.NewError(code, errStr))
				return
			}
			syncinterface.TaosSyncQueryFree(result, logger, isDebug)
			if err := SchemalessSelectDB(tt.args.taosConnect, log.GetLogger("test").WithField("test", "TestSchemalessSelectDB"), false, tt.args.db, 0); (err != nil) != tt.wantErr {
				t.Errorf("selectDB() error = %v, wantErr %v", err, tt.wantErr)
			}
			r := syncinterface.TaosQuery(tt.args.taosConnect, fmt.Sprintf("drop database if exists %s", tt.args.db), logger, isDebug)
			code = syncinterface.TaosError(r, logger, isDebug)
			if code != 0 {
				errStr := syncinterface.TaosErrorStr(r, logger, isDebug)
				t.Error(tErrors.NewError(code, errStr))
			}
			syncinterface.TaosSyncQueryFree(r, logger, isDebug)
		})
	}
}
