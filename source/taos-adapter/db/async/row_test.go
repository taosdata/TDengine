package async

import (
	"database/sql/driver"
	"fmt"
	"os"
	"testing"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
	"github.com/taosdata/taosadapter/v3/log"
)

func TestMain(m *testing.M) {
	config.Init()
	_ = log.SetLevel("trace")
	os.Exit(m.Run())
}

// @author: xftan
// @date: 2021/12/14 15:02
// @description: test Async Execute
func TestAsync_TaosExec(t *testing.T) {
	logger := log.GetLogger("test").WithField("test", "async_test")
	isDebug := log.IsDebug()
	conn, err := syncinterface.TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if err != nil {
		t.Error(err)
		return
	}
	defer syncinterface.TaosClose(conn, logger, isDebug)
	type fields struct {
		handlerPool *HandlerPool
	}
	type args struct {
		taosConnect unsafe.Pointer
		sql         string
		timeFormat  wrapper.FormatTimeFunc
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *ExecResult
		wantErr bool
	}{
		{
			name:   "select 1",
			fields: fields{NewHandlerPool(10000)},
			args: args{
				taosConnect: conn,
				sql:         "select 1",
				timeFormat: func(ts int64, _ int) driver.Value {
					return ts
				},
			},
			want: &ExecResult{
				FieldCount: 1,
			},
			wantErr: false,
		},
		{
			name:   "create database",
			fields: fields{NewHandlerPool(10000)},
			args: args{
				taosConnect: conn,
				sql:         "create database if not exists test_async_exec",
			},
			want: &ExecResult{
				AffectedRows: 0,
				FieldCount:   0,
				Header:       nil,
				Data:         nil,
			},
			wantErr: false,
		},
		{
			name:   "drop database",
			fields: fields{NewHandlerPool(10000)},
			args: args{
				taosConnect: conn,
				sql:         "drop database if exists test_async_exec",
			},
			want: &ExecResult{
				AffectedRows: 0,
				FieldCount:   0,
				Header:       nil,
				Data:         nil,
			},
			wantErr: false,
		},
		{
			name:   "wrong",
			fields: fields{NewHandlerPool(10000)},
			args: args{
				taosConnect: conn,
				sql:         "wrong sql",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Async{
				HandlerPool: tt.fields.handlerPool,
			}
			got, err := a.TaosExec(tt.args.taosConnect, logger, false, tt.args.sql, tt.args.timeFormat, 0)
			if (err != nil) != tt.wantErr {
				t.Errorf("TaosExec() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == nil {
				if tt.want != nil {
					t.Errorf("TaosExec() expect  %v, get nil", tt.want)
				}
				return
			}
			if got.FieldCount != tt.want.FieldCount {
				t.Errorf("field count expect %d got %d", tt.want.FieldCount, got.FieldCount)
				return
			}
			t.Logf("%#v", got)
		})
	}
}

// @author: xftan
// @date: 2021/12/14 15:03
// @description: test async exec without result
func TestAsync_TaosExecWithoutResult(t *testing.T) {
	var logger = logrus.New().WithField("test", "TaosExecWithoutResult")
	isDebug := log.IsDebug()
	conn, err := syncinterface.TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if err != nil {
		t.Error(err)
		return
	}
	defer syncinterface.TaosClose(conn, logger, isDebug)
	type fields struct {
		handlerPool *HandlerPool
	}
	type args struct {
		taosConnect unsafe.Pointer
		sql         string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name:   "create database",
			fields: fields{NewHandlerPool(10000)},
			args: args{
				taosConnect: conn,
				sql:         "create database if not exists test_async_exec_without_result",
			},
			wantErr: false,
		},
		{
			name:   "drop database",
			fields: fields{NewHandlerPool(10000)},
			args: args{
				taosConnect: conn,
				sql:         "drop database if exists test_async_exec_without_result",
			},
			wantErr: false,
		},
		{
			name: "wrong",
			fields: fields{
				handlerPool: NewHandlerPool(2),
			},
			args: args{
				taosConnect: conn,
				sql:         "wrong sql",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Async{
				HandlerPool: tt.fields.handlerPool,
			}
			if err := a.TaosExecWithoutResult(tt.args.taosConnect, logger, false, tt.args.sql, 0); (err != nil) != tt.wantErr {
				t.Errorf("TaosExecWithoutResult() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAsync_TaosExecWithAffectedRows(t *testing.T) {
	var logger = logrus.New().WithField("test", "TaosExecWithAffectedRows")
	isDebug := log.IsDebug()
	conn, err := syncinterface.TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if err != nil {
		t.Error(err)
		return
	}
	defer syncinterface.TaosClose(conn, logger, isDebug)
	type fields struct {
		HandlerPool *HandlerPool
	}
	type args struct {
		taosConnect unsafe.Pointer
		sql         string
		reqID       int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:   "create database",
			fields: fields{NewHandlerPool(10000)},
			args: args{
				taosConnect: conn,
				sql:         "create database if not exists test_async_exec_with_affected_rows",
			},
			want:    0,
			wantErr: assert.NoError,
		},
		{
			name:   "create table",
			fields: fields{NewHandlerPool(10000)},
			args: args{
				taosConnect: conn,
				sql:         "create table if not exists test_async_exec_with_affected_rows.t1 (ts timestamp, c1 int)",
			},
			want:    0,
			wantErr: assert.NoError,
		},
		{
			name:   "insert data",
			fields: fields{NewHandlerPool(10000)},
			args: args{
				taosConnect: conn,
				sql:         "insert into test_async_exec_with_affected_rows.t1 values (now, 1), (now + 1s, 2), (now + 2s, 3)",
			},
			want:    3,
			wantErr: assert.NoError,
		},
		{
			name:   "drop database",
			fields: fields{NewHandlerPool(10000)},
			args: args{
				taosConnect: conn,
				sql:         "drop database if exists test_async_exec_with_affected_rows",
			},
			want:    0,
			wantErr: assert.NoError,
		},
		{
			name: "wrong",
			fields: fields{
				HandlerPool: NewHandlerPool(2),
			},
			args: args{
				taosConnect: conn,
				sql:         "wrong sql",
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Async{
				HandlerPool: tt.fields.HandlerPool,
			}
			got, err := a.TaosExecWithAffectedRows(tt.args.taosConnect, logger, false, tt.args.sql, tt.args.reqID)
			if !tt.wantErr(t, err, fmt.Sprintf("TaosExecWithAffectedRows(%v, %v, %v, %v, %v)", tt.args.taosConnect, logger, false, tt.args.sql, tt.args.reqID)) {
				return
			}
			assert.Equalf(t, tt.want, got, "TaosExecWithAffectedRows(%v, %v, %v, %v, %v)", tt.args.taosConnect, logger, false, tt.args.sql, tt.args.reqID)
		})
	}
}
