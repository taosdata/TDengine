package wrapper

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/driver/common"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
	"github.com/taosdata/taosadapter/v3/tools/testtools/testenv"
)

// @author: xftan
// @date: 2023/10/13 11:28
// @description: test notify callback
func TestNotify(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	defer TaosClose(conn)
	defer func() {
		_ = exec(conn, "drop user t_notify")
	}()
	_ = exec(conn, "drop user t_notify")
	err = exec(conn, "create user t_notify pass 'notify_123'")
	assert.NoError(t, err)

	conn2, err := TaosConnect("", "t_notify", "notify_123", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	defer TaosClose(conn2)
	notify := make(chan int32, 1)
	handler := cgo.NewHandle(notify)
	errCode := TaosSetNotifyCB(conn2, handler, common.TAOS_NOTIFY_PASSVER)
	if errCode != 0 {
		errStr := TaosErrorStr(nil)
		t.Error(errCode, errStr)
	}
	notifyWhitelist := make(chan int64, 1)
	handlerWhiteList := cgo.NewHandle(notifyWhitelist)
	errCode = TaosSetNotifyCB(conn2, handlerWhiteList, common.TAOS_NOTIFY_WHITELIST_VER)
	if errCode != 0 {
		errStr := TaosErrorStr(nil)
		t.Error(errCode, errStr)
	}

	notifyDropUser := make(chan struct{}, 1)
	handlerDropUser := cgo.NewHandle(notifyDropUser)
	errCode = TaosSetNotifyCB(conn2, handlerDropUser, common.TAOS_NOTIFY_USER_DROPPED)
	if errCode != 0 {
		errStr := TaosErrorStr(nil)
		t.Error(errCode, errStr)
	}

	err = exec(conn, "alter user t_notify pass 'test_123'")
	assert.NoError(t, err)
	timeout, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	now := time.Now()
	select {
	case version := <-notify:
		t.Log(time.Since(now))
		t.Log("password changed", version)
	case <-timeout.Done():
		t.Error("wait for notify callback timeout")
	}

	if testenv.IsEnterpriseTest() {
		err = exec(conn, "ALTER USER t_notify ADD HOST '192.168.1.98/0','192.168.1.98/32'")
		assert.NoError(t, err)
		timeoutWhiteList, cancelWhitelist := context.WithTimeout(context.Background(), time.Second*5)
		defer cancelWhitelist()
		now = time.Now()
		select {
		case version := <-notifyWhitelist:
			t.Log(time.Since(now))
			t.Log("whitelist changed", version)
		case <-timeoutWhiteList.Done():
			t.Error("wait for notifyWhitelist callback timeout")
		}
	}

	err = exec(conn, "drop USER t_notify")
	assert.NoError(t, err)
	timeoutDropUser, cancelDropUser := context.WithTimeout(context.Background(), time.Second*5)
	defer cancelDropUser()
	now = time.Now()
	select {
	case <-notifyDropUser:
		t.Log(time.Since(now))
		t.Log("user dropped")
	case <-timeoutDropUser.Done():
		t.Error("wait for notifyDropUser callback timeoutDropUser")
	}

}
