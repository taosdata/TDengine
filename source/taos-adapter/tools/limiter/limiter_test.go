package limiter

import (
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/tools/sqltype"
)

func TestMain(m *testing.M) {
	config.Init()
	config.Conf.Request = &config.Request{
		QueryLimitEnable:                 true,
		ExcludeQueryLimitSql:             []string{"selectserver_version()", "select1"},
		ExcludeQueryLimitSqlMaxByteCount: 22,
		ExcludeQueryLimitSqlMinByteCount: 7,
		ExcludeQueryLimitSqlRegex:        []*regexp.Regexp{regexp.MustCompile(`(?i)^select\s+.*from\s+information_schema.*`)},
		Default: &config.LimitConfig{
			QueryLimit:       2,
			QueryWaitTimeout: 1,
			QueryMaxWait:     1,
		},
		Users: map[string]*config.LimitConfig{
			"test_user": {
				QueryLimit:       3,
				QueryWaitTimeout: 2,
				QueryMaxWait:     2,
			},
		},
	}
	os.Exit(m.Run())
}

func TestNewLimiter(t *testing.T) {
	type args struct {
		limit   int
		timeout time.Duration
		maxWait int32
	}
	tests := []struct {
		name string
		args args
		want *Limiter
	}{
		{
			name: "limit 0",
			args: args{
				limit:   0,
				timeout: time.Second,
				maxWait: 10,
			},
			want: &Limiter{
				limit:   0,
				bucket:  nil,
				timeout: time.Second,
				maxWait: 10,
			},
		},
		{
			name: "limit 5",
			args: args{
				limit:   5,
				timeout: time.Second,
				maxWait: 10,
			},
			want: &Limiter{
				limit:   5,
				bucket:  make(chan struct{}, 5),
				timeout: time.Second,
				maxWait: 10,
			},
		},
		{
			name: "limit -1",
			args: args{
				limit:   -1,
				timeout: time.Second,
				maxWait: 10,
			},
			want: &Limiter{
				limit:   -1,
				bucket:  nil,
				timeout: time.Second,
				maxWait: 10,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewLimiter(tt.args.limit, tt.args.timeout, tt.args.maxWait)

			assert.Equal(t, tt.want.limit, got.limit)
			assert.Equal(t, tt.want.timeout, got.timeout)
			assert.Equal(t, tt.want.maxWait, got.maxWait)

			if tt.want.bucket == nil {
				assert.Nil(t, got.bucket)
			} else {
				assert.NotNil(t, got.bucket)
				assert.Equal(t, cap(tt.want.bucket), cap(got.bucket))
			}
		})
	}
}

func TestLimiter_Acquire(t *testing.T) {
	type fields struct {
		limit   int
		bucket  chan struct{}
		timeout time.Duration
		maxWait int32
	}
	tests := []struct {
		name     string
		fields   fields
		testFunc func(l *Limiter)
	}{
		{
			name: "limit 0",
			fields: fields{
				limit:   0,
				bucket:  nil,
				timeout: time.Second,
				maxWait: 10,
			},
			testFunc: func(l *Limiter) {
				for i := 0; i < 100; i++ {
					err := l.Acquire()
					assert.NoError(t, err)
				}
				for i := 0; i < 100; i++ {
					l.Release()
				}
			},
		},
		{
			name: "limit 2, acquire 2",
			fields: fields{
				limit:   2,
				bucket:  make(chan struct{}, 2),
				timeout: time.Second,
				maxWait: 10,
			},
			testFunc: func(l *Limiter) {
				for i := 0; i < 2; i++ {
					err := l.Acquire()
					assert.NoError(t, err)
				}
			},
		},
		{
			name: "limit 2, maxWait 1, acquire 4, should fail",
			fields: fields{
				limit:   2,
				bucket:  make(chan struct{}, 2),
				timeout: time.Millisecond * 100,
				maxWait: 1,
			},
			testFunc: func(l *Limiter) {
				for i := 0; i < 2; i++ {
					err := l.Acquire()
					assert.NoError(t, err)
				}
				done := make(chan struct{})
				go func() {
					err := l.Acquire()
					assert.Equal(t, "[0xfffe] request wait timeout, please try again later", err.Error())
					close(done)
				}()
				time.Sleep(time.Millisecond * 10)
				err := l.Acquire()
				assert.Equal(t, "[0xfffe] request wait queue is full, please try again later", err.Error())
				<-done
			},
		},
		{
			name: "limit 2, maxWait 2, acquire 4, should fail",
			fields: fields{
				limit:   2,
				bucket:  make(chan struct{}, 2),
				timeout: time.Millisecond * 100,
				maxWait: 1,
			},
			testFunc: func(l *Limiter) {
				for i := 0; i < 2; i++ {
					err := l.Acquire()
					assert.NoError(t, err)
				}
				done := make(chan struct{})
				go func() {
					err := l.Acquire()
					assert.NoError(t, err)
					close(done)
				}()
				time.Sleep(time.Millisecond * 10)
				l.Release()
				<-done
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Limiter{
				limit:   tt.fields.limit,
				bucket:  tt.fields.bucket,
				timeout: tt.fields.timeout,
				maxWait: tt.fields.maxWait,
			}
			tt.testFunc(l)
		})
	}
}

func TestGetLimiter(t *testing.T) {
	type args struct {
		user string
	}
	tests := []struct {
		name string
		args args
		want *Limiter
	}{
		{
			name: "user exists",
			args: args{
				user: "test_user",
			},
			want: NewLimiter(3, time.Second*2, 2),
		},
		{
			name: "user not exists",
			args: args{
				user: "not_exists_user",
			},
			want: NewLimiter(2, time.Second*1, 1),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetLimiter(tt.args.user)
			assert.Equal(t, tt.want.limit, got.limit)
			assert.Equal(t, tt.want.timeout, got.timeout)
			assert.Equal(t, tt.want.maxWait, got.maxWait)
			if tt.want.bucket == nil {
				assert.Nil(t, got.bucket)
			} else {
				assert.NotNil(t, got.bucket)
				assert.Equal(t, cap(tt.want.bucket), cap(got.bucket))
			}
			// second call should return the same limiter
			got2 := GetLimiter(tt.args.user)
			assert.Equal(t, got, got2)
		})
	}
}

func TestCheckShouldLimit(t *testing.T) {
	type args struct {
		sql     string
		sqlType sqltype.SqlType
	}
	tests := []struct {
		name    string
		args    args
		disable bool
		want    bool
	}{
		{
			name: "not select type",
			args: args{
				sql:     "insert into test values (1, 2)",
				sqlType: sqltype.InsertType,
			},
			want: false,
		},
		{
			name: "exclude by exact match",
			args: args{
				sql:     "select server_version()",
				sqlType: sqltype.SelectType,
			},
			want: false,
		},
		{
			name: "exclude by exact match with different case",
			args: args{
				sql:     "SELECT SERVER_VERSION()",
				sqlType: sqltype.SelectType,
			},
			want: false,
		},
		{
			name: "exclude by length too short",
			args: args{
				sql:     "select 1",
				sqlType: sqltype.SelectType,
			},
			want: false,
		},
		{
			name: "exclude by regex match",
			args: args{
				sql:     "SELECT * FROM information_schema.tables",
				sqlType: sqltype.SelectType,
			},
			want: false,
		},
		{
			name: "not exclude, should limit",
			args: args{
				sql:     "select * from test where id = 1",
				sqlType: sqltype.SelectType,
			},
			want: true,
		},
		{
			name: "not exclude, long sql, should limit",
			args: args{
				sql:     "select * from very_long_table_name where some_column = 'some_value' and another_column = 'another_value'",
				sqlType: sqltype.SelectType,
			},
			want: true,
		},
		{
			name: "disable limit",
			args: args{
				sql:     "select * from test where id = 1",
				sqlType: sqltype.SelectType,
			},
			disable: true,
			want:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.disable {
				config.Conf.Request.QueryLimitEnable = false
			}
			assert.Equalf(t, tt.want, CheckShouldLimit(tt.args.sql, tt.args.sqlType), "CheckShouldLimit(%v, %v)", tt.args.sql, tt.args.sqlType)

		})
	}
}

func TestGetLimiterMetrics(t *testing.T) {
	config.Conf.Request.QueryLimitEnable = false
	assert.Nil(t, GetLimiterMetrics())
	config.Conf.Request.QueryLimitEnable = true
	GlobalLimiterMap.Clear()
	limiter := GetLimiter("test_metrics_user")
	for i := 0; i < 2; i++ {
		err := limiter.Acquire()
		assert.NoError(t, err)
	}
	err := limiter.Acquire()
	assert.Error(t, err)
	metrics := GetLimiterMetrics()
	assert.NotNil(t, metrics)
	assert.Equal(t, 1, len(metrics))
	assert.Contains(t, metrics, "test_metrics_user")
	metric := metrics["test_metrics_user"]
	assert.Equal(t, int32(2), metric.Inflight)
	assert.Equal(t, int32(1), metric.FailCount)
	assert.Equal(t, int32(3), metric.TotalCount)
	assert.Equal(t, int32(0), metric.WaitCount)
	assert.Equal(t, int32(0), limiter.totalCount.Load())
	assert.Equal(t, int32(0), limiter.failCount.Load())

	limiter.Release()
	metrics = GetLimiterMetrics()
	assert.NotNil(t, metrics)
	assert.Equal(t, 1, len(metrics))
	assert.Contains(t, metrics, "test_metrics_user")
	metric = metrics["test_metrics_user"]
	assert.Equal(t, int32(1), metric.Inflight)
	assert.Equal(t, int32(0), metric.FailCount)
	assert.Equal(t, int32(0), metric.TotalCount)
	assert.Equal(t, int32(0), metric.WaitCount)
	assert.Equal(t, int32(0), limiter.totalCount.Load())
	assert.Equal(t, int32(0), limiter.failCount.Load())

	err = limiter.Acquire()
	assert.NoError(t, err)
	done := make(chan bool)
	go func() {
		err = limiter.Acquire()
		assert.NoError(t, err)
		close(done)
	}()
	time.Sleep(time.Millisecond * 10)
	metrics = GetLimiterMetrics()
	assert.NotNil(t, metrics)
	assert.Equal(t, 1, len(metrics))
	assert.Contains(t, metrics, "test_metrics_user")
	metric = metrics["test_metrics_user"]
	assert.Equal(t, int32(2), metric.Inflight)
	assert.Equal(t, int32(0), metric.FailCount)
	assert.Equal(t, int32(2), metric.TotalCount)
	assert.Equal(t, int32(1), metric.WaitCount)
	assert.Equal(t, int32(0), limiter.totalCount.Load())
	assert.Equal(t, int32(0), limiter.failCount.Load())

	limiter.Release()
	<-done
	metrics = GetLimiterMetrics()
	assert.NotNil(t, metrics)
	assert.Equal(t, 1, len(metrics))
	assert.Contains(t, metrics, "test_metrics_user")
	metric = metrics["test_metrics_user"]
	assert.Equal(t, int32(2), metric.Inflight)
	assert.Equal(t, int32(0), metric.FailCount)
	assert.Equal(t, int32(0), metric.TotalCount)
	assert.Equal(t, int32(0), metric.WaitCount)
	assert.Equal(t, int32(0), limiter.totalCount.Load())
	assert.Equal(t, int32(0), limiter.failCount.Load())
}
