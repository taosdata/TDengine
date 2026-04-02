package log

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/taosadapter/v3/config"
)

func TestMain(m *testing.M) {
	config.Init()
	os.Exit(m.Run())
}

// @author: xftan
// @date: 2021/12/14 15:07
// @description: test config log
func TestConfigLog(_ *testing.T) {
	config.Conf.Log.EnableRecordHttpSql = true
	ConfigLog()
	logger := GetLogger("TST")
	logger.Info("test config log")
	time.Sleep(time.Second * 6)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	Close(ctx)
}

func TestIsDebug(t *testing.T) {
	assert.False(t, IsDebug())
	s := GetLogNow(IsDebug())
	assert.Equal(t, s, zeroTime)
	dur := GetLogDuration(IsDebug(), s)
	assert.Equal(t, dur, zeroDuration)
	err := SetLevel("debug")
	assert.NoError(t, err)
	assert.True(t, IsDebug())
	s = GetLogNow(IsDebug())
	assert.NotEqual(t, s, zeroTime)
	dur = GetLogDuration(IsDebug(), s)
	assert.NotEqual(t, dur, zeroDuration)
}

func TestGetLogSql(t *testing.T) {
	sql := GetLogSql("insert into test values(1)")
	assert.Equal(t, sql, "insert into test values(1)")
	builder := strings.Builder{}
	for i := 0; i < MaxLogSqlLength+100; i++ {
		builder.WriteString("a")
	}
	str := builder.String()
	sql = GetLogSql(str)
	assert.Equal(t, sql, str[:MaxLogSqlLength]+"...(truncated)")
}
func TestTaosLogFormatter_Format(t1 *testing.T) {
	type args struct {
		entry *logrus.Entry
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "common_panic",
			args: args{
				entry: &logrus.Entry{
					Time:    time.Unix(1657084598, 0),
					Message: "select 1\n",
					Data: map[string]interface{}{
						config.ModelKey:     "test",
						config.SessionIDKey: 1,
						config.ReqIDKey:     1,
						"ext":               "111",
					},
					Level: logrus.PanicLevel,
				},
			},
			want: []byte(fmt.Sprintf("%s %s test PANIC SID:0x1, QID:0x1 select 1, ext:111\n", time.Unix(1657084598, 0).Format("01/02 15:04:05.000000"), ServerID)),
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				if err != nil {
					t.Errorf("%s,%v", err.Error(), i)
					return false
				}
				return true
			},
		},
		{
			name: "common_fatal",
			args: args{
				entry: &logrus.Entry{
					Time:    time.Unix(1657084598, 0),
					Message: "select 1\n",
					Data: map[string]interface{}{
						config.ModelKey:     "test",
						config.SessionIDKey: 1,
						config.ReqIDKey:     1,
						"ext":               "111",
					},
					Level: logrus.FatalLevel,
				},
			},
			want: []byte(fmt.Sprintf("%s %s test FATAL SID:0x1, QID:0x1 select 1, ext:111\n", time.Unix(1657084598, 0).Format("01/02 15:04:05.000000"), ServerID)),
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				if err != nil {
					t.Errorf("%s,%v", err.Error(), i)
					return false
				}
				return true
			},
		},
		{
			name: "common_error",
			args: args{
				entry: &logrus.Entry{
					Time:    time.Unix(1657084598, 0),
					Message: "select 1\n",
					Data: map[string]interface{}{
						config.ModelKey:     "test",
						config.SessionIDKey: 1,
						config.ReqIDKey:     1,
						"ext":               "111",
					},
					Level: logrus.ErrorLevel,
				},
			},
			want: []byte(fmt.Sprintf("%s %s test ERROR SID:0x1, QID:0x1 select 1, ext:111\n", time.Unix(1657084598, 0).Format("01/02 15:04:05.000000"), ServerID)),
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				if err != nil {
					t.Errorf("%s,%v", err.Error(), i)
					return false
				}
				return true
			},
		},
		{
			name: "common_warn",
			args: args{
				entry: &logrus.Entry{
					Time:    time.Unix(1657084598, 0),
					Message: "select 1\n",
					Data: map[string]interface{}{
						config.ModelKey:     "test",
						config.SessionIDKey: 1,
						config.ReqIDKey:     1,
						"ext":               "111",
					},
					Level: logrus.WarnLevel,
				},
			},
			want: []byte(fmt.Sprintf("%s %s test WARN  SID:0x1, QID:0x1 select 1, ext:111\n", time.Unix(1657084598, 0).Format("01/02 15:04:05.000000"), ServerID)),
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				if err != nil {
					t.Errorf("%s,%v", err.Error(), i)
					return false
				}
				return true
			},
		},
		{
			name: "common_info",
			args: args{
				entry: &logrus.Entry{
					Time:    time.Unix(1657084598, 0),
					Message: "select 1\n",
					Data: map[string]interface{}{
						config.ModelKey:     "test",
						config.SessionIDKey: 1,
						config.ReqIDKey:     1,
						"ext":               "111",
					},
					Level: logrus.InfoLevel,
				},
			},
			want: []byte(fmt.Sprintf("%s %s test INFO  SID:0x1, QID:0x1 select 1, ext:111\n", time.Unix(1657084598, 0).Format("01/02 15:04:05.000000"), ServerID)),
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				if err != nil {
					t.Errorf("%s,%v", err.Error(), i)
					return false
				}
				return true
			},
		},
		{
			name: "common_debug",
			args: args{
				entry: &logrus.Entry{
					Time:    time.Unix(1657084598, 0),
					Message: "select 1\n",
					Data: map[string]interface{}{
						config.ModelKey:     "test",
						config.SessionIDKey: 1,
						config.ReqIDKey:     1,
						"ext":               "111",
					},
					Level: logrus.DebugLevel,
				},
			},
			want: []byte(fmt.Sprintf("%s %s test DEBUG SID:0x1, QID:0x1 select 1, ext:111\n", time.Unix(1657084598, 0).Format("01/02 15:04:05.000000"), ServerID)),
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				if err != nil {
					t.Errorf("%s,%v", err.Error(), i)
					return false
				}
				return true
			},
		},
		{
			name: "common_debug",
			args: args{
				entry: &logrus.Entry{
					Time:    time.Unix(1657084598, 0),
					Message: "select 1\n",
					Data: map[string]interface{}{
						config.ModelKey:     "test",
						config.SessionIDKey: 1,
						config.ReqIDKey:     1,
						"ext":               "111",
					},
					Level: logrus.TraceLevel,
				},
			},
			want: []byte(fmt.Sprintf("%s %s test TRACE SID:0x1, QID:0x1 select 1, ext:111\n", time.Unix(1657084598, 0).Format("01/02 15:04:05.000000"), ServerID)),
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				if err != nil {
					t.Errorf("%s,%v", err.Error(), i)
					return false
				}
				return true
			},
		},
		{
			name: "common",
			args: args{
				entry: &logrus.Entry{
					Time:    time.Unix(1657084598, 0),
					Message: "select 1",
					Data: map[string]interface{}{
						config.SessionIDKey: 1,
						config.ReqIDKey:     nil,
						"ext":               "111",
					},
					Level:  logrus.InfoLevel,
					Buffer: &bytes.Buffer{},
				},
			},
			want: []byte(fmt.Sprintf("%s %s CLI INFO  SID:0x1, select 1, ext:111\n", time.Unix(1657084598, 0).Format("01/02 15:04:05.000000"), ServerID)),
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				if err != nil {
					t.Errorf("%s,%v", err.Error(), i)
					return false
				}
				return true
			},
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &TaosLogFormatter{}
			got, err := t.Format(tt.args.entry)
			if !tt.wantErr(t1, err, fmt.Sprintf("Format(%v)", tt.args.entry)) {
				return
			}
			assert.Equalf(t1, tt.want, got, "Format(%v)", tt.args.entry)
		})
	}
}

func Test_setLoggerOutPut(t *testing.T) {
	old, exists := os.LookupEnv(DisableDisplayLogEnv)
	if exists {
		defer func() {
			_ = os.Setenv(DisableDisplayLogEnv, old)
		}()
	} else {
		defer func() {
			_ = os.Unsetenv(DisableDisplayLogEnv)
		}()
	}
	l := logrus.New()
	require.NoError(t, os.Unsetenv(DisableDisplayLogEnv))
	setLoggerOutPut(l)
	assert.Equal(t, os.Stdout, l.Out)
	t.Setenv(DisableDisplayLogEnv, "true")
	setLoggerOutPut(l)
	assert.Equal(t, io.Discard, l.Out)
}
