package tool

import (
	"context"
	"net"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	tErrors "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
	"github.com/taosdata/taosadapter/v3/log"
)

func TestWhiteListHandle(t *testing.T) {
	c, h := getWhiteListHandle()

	assert.NotNil(t, c)
	assert.NotNil(t, h)
	select {
	case <-c:
		t.Errorf("Expected channel to be empty, but it's not")
	default:
		// Channel is empty, which is expected
	}
	putWhiteListHandle(h)

	c2, h2 := getWhiteListHandle()
	assert.NotNil(t, c2)
	assert.NotNil(t, h2)
	assert.Equal(t, c, c2)
	assert.Equal(t, h, h2)

	putWhiteListHandle(h2)

	c2 <- nil

	c3, h3 := getWhiteListHandle()
	assert.NotNil(t, c3)
	assert.NotNil(t, h3)
	assert.Equal(t, c, c3)
	assert.Equal(t, h, h3)

	select {
	case <-c3:
		t.Errorf("Expected channel to be empty, but it's not")
	default:
		// Channel is empty, which is expected
	}
	l := make([]cgo.Handle, 10001)
	for i := 0; i < 10001; i++ {
		_, l[i] = getWhiteListHandle()
	}
	assert.Equal(t, 0, len(whiteListHandleChan))
	for i := 0; i < 10001; i++ {
		putWhiteListHandle(l[i])
	}
	assert.Equal(t, 10000, len(whiteListHandleChan))
	for i := 0; i < 10000; i++ {
		_, h = getWhiteListHandle()
		assert.Equal(t, l[i], h)
	}
	assert.Equal(t, 0, len(whiteListHandleChan))
}

func TestRegisterChangeWhiteListHandle(t *testing.T) {
	c, h := GetRegisterChangeWhiteListHandle()

	assert.NotNil(t, c)
	assert.NotNil(t, h)
	select {
	case <-c:
		t.Errorf("Expected channel to be empty, but it's not")
	default:
		// Channel is empty, which is expected
	}
	PutRegisterChangeWhiteListHandle(h)

	c2, h2 := GetRegisterChangeWhiteListHandle()
	assert.NotNil(t, c2)
	assert.NotNil(t, h2)
	assert.Equal(t, c, c2)
	assert.Equal(t, h, h2)

	PutRegisterChangeWhiteListHandle(h2)

	c2 <- 1

	c3, h3 := GetRegisterChangeWhiteListHandle()
	assert.NotNil(t, c3)
	assert.NotNil(t, h3)
	assert.Equal(t, c, c3)
	assert.Equal(t, h, h3)

	select {
	case <-c3:
		t.Errorf("Expected channel to be empty, but it's not")
	default:
		// Channel is empty, which is expected
	}
	l := make([]cgo.Handle, 10001)
	for i := 0; i < 10001; i++ {
		_, l[i] = GetRegisterChangeWhiteListHandle()
	}
	assert.Equal(t, 0, len(registerChangeWhiteListHandleChan))
	for i := 0; i < 10001; i++ {
		PutRegisterChangeWhiteListHandle(l[i])
	}
	assert.Equal(t, 10000, len(registerChangeWhiteListHandleChan))
	for i := 0; i < 10000; i++ {
		_, h = GetRegisterChangeWhiteListHandle()
		assert.Equal(t, l[i], h)
	}
	assert.Equal(t, 0, len(registerChangeWhiteListHandleChan))
}

func TestRegisterDropUserHandle(t *testing.T) {
	c, h := GetRegisterDropUserHandle()

	assert.NotNil(t, c)
	assert.NotNil(t, h)
	select {
	case <-c:
		t.Errorf("Expected channel to be empty, but it's not")
	default:
		// Channel is empty, which is expected
	}
	PutRegisterDropUserHandle(h)

	c2, h2 := GetRegisterDropUserHandle()
	assert.NotNil(t, c2)
	assert.NotNil(t, h2)
	assert.Equal(t, c, c2)
	assert.Equal(t, h, h2)

	PutRegisterDropUserHandle(h2)

	c2 <- struct{}{}

	c3, h3 := GetRegisterDropUserHandle()
	assert.NotNil(t, c3)
	assert.NotNil(t, h3)
	assert.Equal(t, c, c3)
	assert.Equal(t, h, h3)

	select {
	case <-c3:
		t.Errorf("Expected channel to be empty, but it's not")
	default:
		// Channel is empty, which is expected
	}
	l := make([]cgo.Handle, 10001)
	for i := 0; i < 10001; i++ {
		_, l[i] = GetRegisterDropUserHandle()
	}
	assert.Equal(t, 0, len(registerDropUserHandleChan))
	for i := 0; i < 10001; i++ {
		PutRegisterDropUserHandle(l[i])
	}
	assert.Equal(t, 10000, len(registerDropUserHandleChan))
	for i := 0; i < 10000; i++ {
		_, h = GetRegisterDropUserHandle()
		assert.Equal(t, l[i], h)
	}
	assert.Equal(t, 0, len(registerDropUserHandleChan))
}
func TestRegisterChangePassHandle(t *testing.T) {
	c, h := GetRegisterChangePassHandle()

	assert.NotNil(t, c)
	assert.NotNil(t, h)
	select {
	case <-c:
		t.Errorf("Expected channel to be empty, but it's not")
	default:
		// Channel is empty, which is expected
	}
	PutRegisterChangePassHandle(h)

	c2, h2 := GetRegisterChangePassHandle()
	assert.NotNil(t, c2)
	assert.NotNil(t, h2)
	assert.Equal(t, c, c2)
	assert.Equal(t, h, h2)

	PutRegisterChangePassHandle(h2)

	c2 <- 1

	c3, h3 := GetRegisterChangePassHandle()
	assert.NotNil(t, c3)
	assert.NotNil(t, h3)
	assert.Equal(t, c, c3)
	assert.Equal(t, h, h3)

	select {
	case <-c3:
		t.Errorf("Expected channel to be empty, but it's not")
	default:
		// Channel is empty, which is expected
	}
	l := make([]cgo.Handle, 10001)
	for i := 0; i < 10001; i++ {
		_, l[i] = GetRegisterChangePassHandle()
	}
	assert.Equal(t, 0, len(registerChangePassHandleChan))
	for i := 0; i < 10001; i++ {
		PutRegisterChangePassHandle(l[i])
	}
	assert.Equal(t, 10000, len(registerChangePassHandleChan))
	for i := 0; i < 10000; i++ {
		_, h = GetRegisterChangePassHandle()
		assert.Equal(t, l[i], h)
	}
	assert.Equal(t, 0, len(registerChangePassHandleChan))
}

func TestGetWhitelist(t *testing.T) {
	logger := log.GetLogger("test")
	isDebug := log.IsDebug()
	conn, err := syncinterface.TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	defer syncinterface.TaosClose(conn, logger, isDebug)
	assert.NoError(t, err)
	allowlist, blocklist, err := GetWhitelist(conn, logger, isDebug)
	assert.NoError(t, err)
	assert.NotNil(t, allowlist)
	assert.Nil(t, blocklist)
	t.Log(allowlist)
	_, ipNetV4, _ := net.ParseCIDR("0.0.0.0/0")
	_, ipNetV6, _ := net.ParseCIDR("::/0")
	assert.Equal(t, []*net.IPNet{ipNetV4, ipNetV6}, allowlist)
	allowlist, blocklist, err = GetWhitelist(nil, logger, isDebug)
	assert.Error(t, err)
	assert.Nil(t, allowlist)
	assert.Nil(t, blocklist)
}

func TestCheckWhitelist(t *testing.T) {
	_, ipNet, _ := net.ParseCIDR("127.0.0.1/32")
	ipNets := []*net.IPNet{ipNet}
	allowd := CheckWhitelist(ipNets, nil, net.ParseIP("127.0.0.1"))
	assert.True(t, allowd)
	allowd = CheckWhitelist(ipNets, nil, net.ParseIP("192.168.1.1"))
	assert.False(t, allowd)
	_, ipNet, _ = net.ParseCIDR("0.0.0.0/0")
	ipNets = []*net.IPNet{ipNet}
	allowd = CheckWhitelist(ipNets, nil, net.ParseIP("192.168.1.1"))
	assert.True(t, allowd)
	// invalid ip
	allowd = CheckWhitelist(ipNets, nil, net.ParseIP(""))
	assert.False(t, allowd)
	// ipv6
	_, ipNet, _ = net.ParseCIDR("::1/128")
	ipNets = []*net.IPNet{ipNet}
	allowd = CheckWhitelist(ipNets, nil, net.ParseIP("::1"))
	assert.True(t, allowd)
	allowd = CheckWhitelist(ipNets, nil, net.ParseIP("::2"))
	assert.False(t, allowd)
	_, ipNet, _ = net.ParseCIDR("::/0")
	ipNets = []*net.IPNet{ipNet}
	allowd = CheckWhitelist(ipNets, nil, net.ParseIP("::1"))
	assert.True(t, allowd)
	allowd = CheckWhitelist(ipNets, nil, net.ParseIP("::2"))
	assert.True(t, allowd)
	// invalid ip
	allowd = CheckWhitelist(ipNets, nil, net.ParseIP(""))
	assert.False(t, allowd)
	// with blacklist
	_, ipNetW, _ := net.ParseCIDR("192.168.2.100/32")
	_, ipNet, _ = net.ParseCIDR("192.168.2.0/24")
	allow := []*net.IPNet{ipNet}
	deny := []*net.IPNet{ipNetW}
	allowd = CheckWhitelist(allow, deny, net.ParseIP("192.168.2.1"))
	assert.True(t, allowd)
	allowd = CheckWhitelist(allow, deny, net.ParseIP("192.168.1.1"))
	assert.False(t, allowd)
	allowd = CheckWhitelist(allow, deny, net.ParseIP("192.168.2.100"))
	assert.False(t, allowd)
	allowd = CheckWhitelist(allow, deny, net.ParseIP("::1"))
	assert.False(t, allowd)
	allowd = CheckWhitelist(allow, deny, net.ParseIP(""))
	assert.False(t, allowd)
}

func TestRegisterChangeWhitelist(t *testing.T) {
	logger := log.GetLogger("test")
	isDebug := log.IsDebug()
	c, h := GetRegisterChangeWhiteListHandle()
	defer PutRegisterChangeWhiteListHandle(h)
	conn, err := syncinterface.TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	assert.NoError(t, err)
	defer syncinterface.TaosClose(conn, logger, isDebug)
	done := make(chan struct{})
	go func() {
		select {
		case data := <-c:
			t.Log(data)
		case <-done:
		}
	}()
	err = RegisterChangeWhitelist(conn, h, logger, isDebug)
	assert.NoError(t, err)
	time.Sleep(time.Second * 5)
	close(done)
}

func TestRegisterChangePass(t *testing.T) {
	logger := log.GetLogger("test")
	isDebug := log.IsDebug()
	c, h := GetRegisterChangePassHandle()
	defer PutRegisterChangePassHandle(h)
	conn, err := syncinterface.TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	assert.NoError(t, err)
	defer syncinterface.TaosClose(conn, logger, isDebug)
	err = exec(conn, "create user test_notify pass 'notify_123'")
	assert.NoError(t, err)
	defer func() {
		// ignore error
		_ = exec(conn, "drop user test_notify")
	}()
	conn2, err := syncinterface.TaosConnect("", "test_notify", "notify_123", "", 0, logger, isDebug)
	defer syncinterface.TaosClose(conn2, logger, isDebug)
	assert.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	err = RegisterChangePass(conn2, h, logger, isDebug)
	assert.NoError(t, err)
	select {
	case data := <-c:
		t.Error("unexpected notify callback", data)
	case <-ctx.Done():
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel2()
	err = exec(conn, "alter user test_notify pass 'test_123'")
	assert.NoError(t, err)
	select {
	case data := <-c:
		t.Log(data)
	case <-ctx2.Done():
		t.Error("wait for notify callback timeout")
	}
}

func TestRegisterDropUser(t *testing.T) {
	logger := log.GetLogger("test")
	isDebug := log.IsDebug()
	c, h := GetRegisterDropUserHandle()
	defer PutRegisterDropUserHandle(h)
	conn, err := syncinterface.TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	assert.NoError(t, err)
	defer syncinterface.TaosClose(conn, logger, isDebug)
	err = exec(conn, "create user test_drop_user pass 'notify_123'")
	assert.NoError(t, err)
	defer func() {
		// ignore error
		_ = exec(conn, "drop user test_drop_user")
	}()
	conn2, err := syncinterface.TaosConnect("", "test_drop_user", "notify_123", "", 0, logger, isDebug)
	assert.NoError(t, err)
	defer syncinterface.TaosClose(conn2, logger, isDebug)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	err = RegisterDropUser(conn2, h, logger, isDebug)
	assert.NoError(t, err)
	select {
	case data := <-c:
		t.Error("unexpected notify callback", data)
	case <-ctx.Done():
	}
	err = exec(conn, "drop user test_drop_user")
	assert.NoError(t, err)
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel2()
	select {
	case data := <-c:
		t.Log(data)
	case <-ctx2.Done():
		t.Error("wait for notify callback timeout")
	}
}

func exec(conn unsafe.Pointer, sql string) error {
	logger := log.GetLogger("test")
	isDebug := log.IsDebug()
	result := syncinterface.TaosQuery(conn, sql, logger, isDebug)
	code := syncinterface.TaosError(result, logger, isDebug)
	if code != 0 {
		errStr := syncinterface.TaosErrorStr(result, logger, isDebug)
		syncinterface.TaosSyncQueryFree(result, logger, isDebug)
		return tErrors.NewError(code, errStr)
	}
	syncinterface.TaosSyncQueryFree(result, logger, isDebug)
	return nil
}
