package handler

import (
	"testing"
	"time"
)

func BenchmarkName(b *testing.B) {
	pool := NewHandlerPool(1)
	for i := 0; i < b.N; i++ {
		h := pool.Get()
		pool.Put(h)
	}
}

// @author: xftan
// @date: 2021/12/14 15:00
// @description: test func NewHandlerPool
func TestNewHandlerPool(t *testing.T) {
	type args struct {
		count int
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test",
			args: args{
				count: 100,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewHandlerPool(tt.args.count)
			l := make([]*Handler, tt.args.count)
			for i := 0; i < tt.args.count; i++ {
				l[i] = got.Get()
			}
			for _, handler := range l {
				got.Put(handler)
			}
		})
	}
}

// @author: xftan
// @date: 2021/12/14 15:01
// @description: test func HandlerPool.Get
func TestHandlerPool_Get(t *testing.T) {
	pool := NewHandlerPool(1)
	h := pool.Get()
	go func() {
		time.Sleep(time.Millisecond)
		pool.Put(h)
	}()
	h2 := pool.Get()
	pool.Put(h2)
}

// @author: xftan
// @date: 2023/10/13 11:27
// @description: test caller query
func TestCaller_QueryCall(t *testing.T) {
	caller := NewCaller()
	caller.QueryCall(nil, 0)
}

// @author: xftan
// @date: 2023/10/13 11:27
// @description: test caller fetch
func TestCaller_FetchCall(t *testing.T) {
	caller := NewCaller()
	caller.FetchCall(nil, 0)
}
