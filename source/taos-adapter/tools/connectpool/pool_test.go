package connectpool

import (
	"errors"
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestConnectPool(t *testing.T) {
	var factoryCalled, closeCalled int

	pool, err := NewConnectPool(&Config{
		InitialCap: 2,
		MaxCap:     5,
		Factory: func() (unsafe.Pointer, error) {
			factoryCalled++
			return unsafe.Pointer(&struct{}{}), nil
		},
		Close: func(_ unsafe.Pointer) {
			closeCalled++
		},
	})

	assert.NoError(t, err)
	assert.NotNil(t, pool)

	conn1, err := pool.Get()
	assert.NoError(t, err)
	assert.NotNil(t, conn1)

	conn2, err := pool.Get()
	assert.NoError(t, err)
	assert.NotNil(t, conn2)

	conn3, err := pool.Get()
	assert.NoError(t, err)
	assert.NotNil(t, conn3)

	err = pool.Put(conn1)
	assert.NoError(t, err)
	err = pool.Put(conn2)
	assert.NoError(t, err)
	err = pool.Put(conn3)
	assert.NoError(t, err)

	arr := make([]unsafe.Pointer, 5)
	for i := 0; i < 5; i++ {
		conn, err := pool.Get()
		assert.NoError(t, err)
		assert.NotNil(t, conn)
		arr[i] = conn
	}
	for i := 0; i < 5; i++ {
		err = pool.Put(arr[i])
		assert.NoError(t, err)
	}
	assert.Equal(t, 5, factoryCalled)

	pool.Release()

	assert.Equal(t, 5, closeCalled)
}

func TestTimeout(t *testing.T) {
	pool, err := NewConnectPool(&Config{
		InitialCap:  0,
		MaxCap:      1,
		MaxWait:     0,
		WaitTimeout: time.Second * 1,
		Factory: func() (unsafe.Pointer, error) {
			return nil, nil
		},
		Close: func(_ unsafe.Pointer) {
			t.Log("close")
		},
	})
	assert.NoError(t, err)
	_, err = pool.Get()
	assert.NoError(t, err)
	_, err = pool.Get()
	assert.Equal(t, ErrTimeout, err)
}

func TestGetAfterRelease(t *testing.T) {
	a := 1
	pool, err := NewConnectPool(&Config{
		InitialCap:  0,
		MaxCap:      1,
		MaxWait:     0,
		WaitTimeout: 0,
		Factory: func() (unsafe.Pointer, error) {
			return unsafe.Pointer(&a), nil
		},
		Close: func(_ unsafe.Pointer) {
			t.Log("close")
		},
	})
	assert.NoError(t, err)
	assert.NoError(t, err)
	c, err := pool.Get()
	assert.NoError(t, err)
	go func() {
		time.Sleep(time.Second)
		pool.Release()
		assert.Nil(t, pool.conns)
	}()
	// wait for connection put back, util release
	_, err = pool.Get()
	assert.Equal(t, ErrClosed, err)
	err = pool.Put(nil)
	assert.EqualError(t, err, "connection is nil. rejecting")
	err = pool.Put(c)
	assert.NoError(t, err)

	// get after release
	_, err = pool.Get()
	assert.Equal(t, ErrClosed, err)

	// release after release
	pool.Release()

}

func TestMaxWait(t *testing.T) {
	a := 1
	pool, err := NewConnectPool(&Config{
		InitialCap:  0,
		MaxCap:      1,
		MaxWait:     1,
		WaitTimeout: 0,
		Factory: func() (unsafe.Pointer, error) {
			return unsafe.Pointer(&a), nil
		},
		Close: func(_ unsafe.Pointer) {
			t.Log("close")
		},
	})
	assert.NoError(t, err)
	c, err := pool.Get()
	assert.NoError(t, err)
	go func() {
		time.Sleep(time.Second)
		// over max wait,will return immediately
		_, err = pool.Get()
		assert.Equal(t, ErrMaxWait, err)
		// put back connection
		err = pool.Put(c)
		assert.NoError(t, err)
	}()
	// wait for connection put back
	_, err = pool.Get()
	assert.NoError(t, err)
}

func TestNewConnectPool(t *testing.T) {
	type args struct {
		poolConfig *Config
	}
	tests := []struct {
		name    string
		args    args
		want    *ConnectPool
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "wrong cap",
			args: args{
				poolConfig: &Config{
					MaxWait: 0,
				},
			},
			want: nil,
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.EqualError(t, err, "MaxCap must larger than 0", i...)
			},
		},
		{
			name: "wrong Factory",
			args: args{
				poolConfig: &Config{
					MaxCap:  1,
					Factory: nil,
				},
			},
			want: nil,
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.EqualError(t, err, "invalid factory func settings", i...)
			},
		},
		{
			name: "wrong Close",
			args: args{
				poolConfig: &Config{
					MaxCap: 1,
					Factory: func() (unsafe.Pointer, error) {
						return nil, nil
					},
					Close: nil,
				},
			},
			want: nil,
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.EqualError(t, err, "invalid close func settings", i...)
			},
		},
		{
			name: "factory error",
			args: args{
				poolConfig: &Config{
					MaxCap: 1,
					Factory: func() (unsafe.Pointer, error) {
						return nil, errors.New("connect error")
					},
					Close:      func(pointer unsafe.Pointer) {},
					InitialCap: 1,
				},
			},
			want: nil,
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.EqualError(t, err, "connect error", i...)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewConnectPool(tt.args.poolConfig)
			if !tt.wantErr(t, err, fmt.Sprintf("NewConnectPool(%v)", tt.args.poolConfig)) {
				return
			}
			assert.Equalf(t, tt.want, got, "NewConnectPool(%v)", tt.args.poolConfig)
		})
	}
}
