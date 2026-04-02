package commonpool

import (
	"net"
	"os"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
	"github.com/taosdata/taosadapter/v3/tools/testtools/testenv"
)

func TestMain(m *testing.M) {
	config.Init()
	os.Exit(m.Run())
}

func BenchmarkGetConnection(b *testing.B) {
	for i := 0; i < b.N; i++ {
		conn, err := GetConnection("root", "taosdata", "", net.ParseIP("127.0.0.1"))
		if err != nil {
			b.Error(err)
			return
		}
		err = conn.Put()
		if err != nil {
			b.Error(err)
			return
		}
	}
}

// @author: xftan
// @date: 2021/12/14 15:03
// @description: test connection get connection
func TestGetConnection(t *testing.T) {
	c, err := GetConnection("root", "taosdata", "", net.ParseIP("127.0.0.1"))
	assert.NoError(t, err)
	defer func() {
		err = c.Put()
		assert.NoError(t, err)
	}()
	type args struct {
		user     string
		password string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test",
			args: args{
				user:     "root",
				password: "taosdata",
			},
			wantErr: false,
		},
		{
			name: "wrong",
			args: args{
				user:     "root",
				password: "wrong",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetConnection(tt.args.user, tt.args.password, "", net.ParseIP("127.0.0.1"))
			if (err != nil) != tt.wantErr {
				t.Errorf("GetConnection() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			err = got.Put()
			assert.NoError(t, err)
		})
	}
}

func TestGetConnectionToken(t *testing.T) {
	if !testenv.IsEnterpriseTest() {
		t.Skip("token test only for enterprise edition")
		return
	}
	c, err := GetConnection("root", "taosdata", "", net.ParseIP("127.0.0.1"))
	assert.NoError(t, err)
	defer func() {
		err = c.Put()
		assert.NoError(t, err)
	}()
	res, err := testtools.Query(c.TaosConnection, "create token test_token_pool from user root")
	assert.NoError(t, err)
	assert.NotNil(t, res)
	token := res[0][0].(string)
	//t.Log("got token:", token)
	assert.NoError(t, testtools.EnsureTokenCreated("test_token_pool"))
	defer func() {
		err = testtools.Exec(c.TaosConnection, "drop token test_token_pool")
		assert.NoError(t, err)
	}()
	assert.NotEmpty(t, token)
	got, err := GetConnection("root", "taosdata", "wrong_token", net.ParseIP("127.0.0.1"))
	assert.Error(t, err)
	assert.Nil(t, got)
	got, err = GetConnection("root", "taosdata", token, net.ParseIP("127.0.0.1"))
	assert.NoError(t, err)
	err = got.Put()
	assert.NoError(t, err)
}

// @author: xftan
// @date: 2021/12/14 15:04
// @description: test connection pool re-get connection
func TestReGetConnection(t *testing.T) {
	conn, err := GetConnection("root", "taosdata", "", net.ParseIP("127.0.0.1"))
	assert.NoError(t, err)
	conn2, err := GetConnection("root", "wrong", "", net.ParseIP("127.0.0.1"))
	assert.Error(t, err)
	assert.Nil(t, conn2)
	err = conn.Put()
	assert.NoError(t, err)
}

// @author: xftan
// @date: 2021/12/14 15:05
// @description: test connect pool close
func TestConnectorPool_Close(t *testing.T) {
	pool, err := NewConnectorPool("root", "taosdata", "")
	assert.NoError(t, err)
	conn, err := pool.Get()
	assert.NoError(t, err)
	err = pool.Put(conn)
	assert.NoError(t, err)
	conn2, err := pool.Get()
	assert.NoError(t, err)
	err = pool.Close(conn2)
	assert.NoError(t, err)
	pool.Release()
}

func TestChangePassword(t *testing.T) {
	logger := log.GetLogger("test")
	isDebug := log.IsDebug()
	c, err := GetConnection("root", "taosdata", "", net.ParseIP("127.0.0.1"))
	assert.NoError(t, err)

	defer func() {
		err = c.Put()
		assert.NoError(t, err)
	}()
	result := syncinterface.TaosQuery(c.TaosConnection, "drop user test_change_pass_pool", logger, isDebug)
	assert.NotNil(t, result)
	syncinterface.TaosSyncQueryFree(result, logger, isDebug)

	result = syncinterface.TaosQuery(c.TaosConnection, "create user test_change_pass_pool pass 'test_123'", logger, isDebug)
	assert.NotNil(t, result)
	errNo := syncinterface.TaosError(result, logger, isDebug)
	assert.Equal(t, 0, errNo)
	syncinterface.TaosSyncQueryFree(result, logger, isDebug)

	defer func() {
		r := syncinterface.TaosQuery(c.TaosConnection, "drop user test_change_pass_pool", logger, isDebug)
		syncinterface.TaosSyncQueryFree(r, logger, isDebug)
	}()

	conn, err := GetConnection("test_change_pass_pool", "test_123", "", net.ParseIP("127.0.0.1"))
	assert.NoError(t, err)

	result = syncinterface.TaosQuery(c.TaosConnection, "alter user test_change_pass_pool pass 'test2_123'", logger, isDebug)
	assert.NotNil(t, result)
	errNo = syncinterface.TaosError(result, logger, isDebug)
	assert.Equal(t, 0, errNo, syncinterface.TaosErrorStr(result, logger, isDebug))
	syncinterface.TaosSyncQueryFree(result, logger, isDebug)

	result = syncinterface.TaosQuery(conn.TaosConnection, "show databases", logger, isDebug)
	errNo = syncinterface.TaosError(result, logger, isDebug)
	syncinterface.TaosSyncQueryFree(result, logger, isDebug)
	assert.Equal(t, 0, errNo)
	wc1, err := conn.pool.Get()
	assert.Equal(t, AuthFailureError, err)
	assert.Equal(t, unsafe.Pointer(nil), wc1)
	time.Sleep(time.Second * 2)
	wc, err := conn.pool.Get()
	assert.Equal(t, AuthFailureError, err)
	assert.Equal(t, unsafe.Pointer(nil), wc)
	err = conn.Put()
	assert.NoError(t, err)

	conn2, err := GetConnection("test_change_pass_pool", "test_123", "", net.ParseIP("127.0.0.1"))
	assert.Error(t, err)
	assert.Nil(t, conn2)

	conn3, err := GetConnection("test_change_pass_pool", "test2_123", "", net.ParseIP("127.0.0.1"))
	assert.NoError(t, err)
	assert.NotNil(t, conn3)
	result2 := syncinterface.TaosQuery(conn3.TaosConnection, "show databases", logger, isDebug)
	errNo = syncinterface.TaosError(result2, logger, isDebug)
	syncinterface.TaosSyncQueryFree(result2, logger, isDebug)
	assert.Equal(t, 0, errNo)
	err = conn3.Put()
	assert.NoError(t, err)

	conn4, err := GetConnection("test_change_pass_pool", "test2_123", "", net.ParseIP("127.0.0.1"))
	assert.NoError(t, err)
	assert.NotNil(t, conn4)
	result3 := syncinterface.TaosQuery(conn4.TaosConnection, "show databases", logger, isDebug)
	errNo = syncinterface.TaosError(result3, logger, isDebug)
	syncinterface.TaosSyncQueryFree(result3, logger, isDebug)
	assert.Equal(t, 0, errNo)
	err = conn4.Put()
	assert.NoError(t, err)
}

func TestChangePasswordConcurrent(t *testing.T) {
	logger := log.GetLogger("test")
	isDebug := log.IsDebug()
	c, err := GetConnection("root", "taosdata", "", net.ParseIP("127.0.0.1"))
	assert.NoError(t, err)
	defer func() {
		err = c.Put()
		assert.NoError(t, err)
	}()

	result := syncinterface.TaosQuery(c.TaosConnection, "drop user test_change_pass_con", logger, isDebug)
	assert.NotNil(t, result)
	syncinterface.TaosSyncQueryFree(result, logger, isDebug)

	result = syncinterface.TaosQuery(c.TaosConnection, "create user test_change_pass_con pass 'test_123'", logger, isDebug)
	assert.NotNil(t, result)
	errNo := syncinterface.TaosError(result, logger, isDebug)
	assert.Equal(t, 0, errNo)
	syncinterface.TaosSyncQueryFree(result, logger, isDebug)

	defer func() {
		r := syncinterface.TaosQuery(c.TaosConnection, "drop user test_change_pass_con", logger, isDebug)
		syncinterface.TaosSyncQueryFree(r, logger, isDebug)
	}()
	conn, err := GetConnection("test_change_pass_con", "test_123", "", net.ParseIP("127.0.0.1"))
	assert.NoError(t, err)

	result = syncinterface.TaosQuery(c.TaosConnection, "alter user test_change_pass_con pass 'test2_123'", logger, isDebug)
	assert.NotNil(t, result)
	errNo = syncinterface.TaosError(result, logger, isDebug)
	assert.Equal(t, 0, errNo, syncinterface.TaosErrorStr(result, logger, isDebug))
	syncinterface.TaosSyncQueryFree(result, logger, isDebug)

	result = syncinterface.TaosQuery(conn.TaosConnection, "show databases", logger, isDebug)
	errNo = syncinterface.TaosError(result, logger, isDebug)
	syncinterface.TaosSyncQueryFree(result, logger, isDebug)
	assert.Equal(t, 0, errNo)
	wc1, err := conn.pool.Get()
	assert.Equal(t, AuthFailureError, err)
	assert.Equal(t, unsafe.Pointer(nil), wc1)
	err = conn.Put()
	assert.NoError(t, err)
	wg := sync.WaitGroup{}
	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func() {
			defer wg.Done()
			conn2, err := GetConnection("test_change_pass_con", "test2_123", "", net.ParseIP("127.0.0.1"))
			assert.NoError(t, err)
			assert.NotNil(t, conn2)
			err = conn2.Put()
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
	conn2, err := GetConnection("test_change_pass_con", "test_123", "", net.ParseIP("127.0.0.1"))
	assert.Error(t, err)
	assert.Nil(t, conn2)

	conn3, err := GetConnection("test_change_pass_con", "test2_123", "", net.ParseIP("127.0.0.1"))
	assert.NoError(t, err)
	assert.NotNil(t, conn3)
	result2 := syncinterface.TaosQuery(conn3.TaosConnection, "show databases", logger, isDebug)
	errNo = syncinterface.TaosError(result2, logger, isDebug)
	syncinterface.TaosSyncQueryFree(result2, logger, isDebug)
	assert.Equal(t, 0, errNo)
	err = conn3.Put()
	assert.NoError(t, err)

	conn4, err := GetConnection("test_change_pass_con", "test2_123", "", net.ParseIP("127.0.0.1"))
	assert.NoError(t, err)
	assert.NotNil(t, conn4)
	result3 := syncinterface.TaosQuery(conn4.TaosConnection, "show databases", logger, isDebug)
	errNo = syncinterface.TaosError(result3, logger, isDebug)
	syncinterface.TaosSyncQueryFree(result3, logger, isDebug)
	assert.Equal(t, 0, errNo)
	err = conn4.Put()
	assert.NoError(t, err)
}
