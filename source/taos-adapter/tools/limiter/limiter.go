package limiter

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/tools/sqltype"
)

var GlobalLimiterMap = sync.Map{}

func GetLimiter(user string) *Limiter {
	limiter, ok := GlobalLimiterMap.Load(user)
	if ok {
		return limiter.(*Limiter)
	}
	limitConfig := config.Conf.Request.GetUserLimitConfig(user)
	newLimiter := NewLimiter(limitConfig.QueryLimit, time.Second*time.Duration(limitConfig.QueryWaitTimeout), int32(limitConfig.QueryMaxWait))
	limiter, _ = GlobalLimiterMap.LoadOrStore(user, newLimiter)
	return limiter.(*Limiter)
}

var empty = struct{}{}

type Limiter struct {
	limit   int
	bucket  chan struct{}
	timeout time.Duration
	maxWait int32
	curWait atomic.Int32
	// metrics
	inflight   atomic.Int32
	totalCount atomic.Int32
	failCount  atomic.Int32
}

func NewLimiter(limit int, timeout time.Duration, maxWait int32) *Limiter {
	var bucket chan struct{}
	if limit > 0 {
		bucket = make(chan struct{}, limit)
	}
	return &Limiter{
		limit:   limit,
		bucket:  bucket,
		timeout: timeout,
		maxWait: maxWait,
	}
}

func (l *Limiter) Acquire() error {
	l.totalCount.Add(1)
	if l.limit <= 0 {
		l.inflight.Add(1)
		return nil
	}
	select {
	case l.bucket <- empty:
		l.inflight.Add(1)
		return nil
	default:
	}

	for {
		old := l.curWait.Load()
		if l.maxWait > 0 && old >= l.maxWait {
			l.failCount.Add(1)
			return errors.NewError(httperror.RequestOverLimit, "request wait queue is full, please try again later")
		}
		if l.curWait.CompareAndSwap(old, old+1) {
			break
		}
	}

	defer l.curWait.Add(-1)
	timer := time.NewTimer(l.timeout)
	defer timer.Stop()
	select {
	case l.bucket <- empty:
		l.inflight.Add(1)
		return nil
	case <-timer.C:
		l.failCount.Add(1)
		return errors.NewError(httperror.RequestOverLimit, "request wait timeout, please try again later")
	}
}

func (l *Limiter) Release() {
	defer l.inflight.Add(-1)
	if l.limit <= 0 {
		return
	}
	<-l.bucket
}

type RequestLimiterMetrics struct {
	QueryLimit int
	MaxWait    int32
	Inflight   int32
	WaitCount  int32
	TotalCount int32
	FailCount  int32
}

func GetLimiterMetrics() map[string]*RequestLimiterMetrics {
	if !config.Conf.Request.QueryLimitEnable {
		return nil
	}
	metrics := make(map[string]*RequestLimiterMetrics)
	GlobalLimiterMap.Range(func(key, value any) bool {
		user := key.(string)
		limiter := value.(*Limiter)
		// reset total and fail count after getting
		total := limiter.totalCount.Swap(0)
		fail := limiter.failCount.Swap(0)

		metrics[user] = &RequestLimiterMetrics{
			QueryLimit: limiter.limit,
			MaxWait:    limiter.maxWait,
			Inflight:   limiter.inflight.Load(),
			WaitCount:  limiter.curWait.Load(),
			TotalCount: total,
			FailCount:  fail,
		}
		return true
	})
	return metrics
}

func CheckShouldLimit(sql string, sqlType sqltype.SqlType) bool {
	if !config.Conf.Request.QueryLimitEnable || sqlType != sqltype.SelectType {
		return false
	}
	// sql length larger than ExcludeQueryLimitSqlMinByteCount, then check exclude list
	if config.Conf.Request.ExcludeQueryLimitSqlMinByteCount > 0 && len(sql) >= config.Conf.Request.ExcludeQueryLimitSqlMinByteCount {
		cleanSql := sqltype.RemoveSpacesAndLowercase(sql, config.Conf.Request.ExcludeQueryLimitSqlMaxByteCount)
		// only when cleanSql length larger than ExcludeQueryLimitSqlMinByteCount, then check exclude list
		if len(cleanSql) >= config.Conf.Request.ExcludeQueryLimitSqlMinByteCount {
			for _, exclude := range config.Conf.Request.ExcludeQueryLimitSql {
				if strings.HasPrefix(cleanSql, exclude) {
					return false
				}
			}
		}
	}
	if len(config.Conf.Request.ExcludeQueryLimitSqlRegex) > 0 {
		for _, reg := range config.Conf.Request.ExcludeQueryLimitSqlRegex {
			if reg.MatchString(sql) {
				return false
			}
		}
	}
	return true
}
