package wstool

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/testtools/testenv"
)

type MockWSHandler struct {
	mutex  sync.Mutex
	exited bool
	ip     net.IP
	ipStr  string
	locked bool

	whitelistChangeHandle cgo.Handle
	dropUserHandle        cgo.Handle
	whitelistChangeChan   chan int64
	dropUserChan          chan struct{}
	exit                  chan struct{}
}

func NewMockWSHandler(conn unsafe.Pointer, ipStr string, logger *logrus.Entry, isDebug bool) *MockWSHandler {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		panic("invalid ip string")
	}
	whitelistChangeChan, whitelistChangeHandle := tool.GetRegisterChangeWhiteListHandle()
	dropUserChan, dropUserHandle := tool.GetRegisterDropUserHandle()
	err := tool.RegisterChangeWhitelist(conn, whitelistChangeHandle, logger, isDebug)
	if err != nil {
		panic(err)
	}
	err = tool.RegisterDropUser(conn, dropUserHandle, logger, isDebug)
	if err != nil {
		panic(err)
	}
	return &MockWSHandler{
		ipStr:                 ipStr,
		ip:                    ip,
		exit:                  make(chan struct{}),
		whitelistChangeChan:   whitelistChangeChan,
		whitelistChangeHandle: whitelistChangeHandle,
		dropUserChan:          dropUserChan,
		dropUserHandle:        dropUserHandle,
	}
}

func (m *MockWSHandler) Lock(_ *logrus.Entry, _ bool) {
	if m.locked {
		panic("already locked")
	}
	m.mutex.Lock()
	m.locked = true
}

func (m *MockWSHandler) Unlock() {
	if !m.locked {
		panic("not locked")
	}
	m.mutex.Unlock()
	m.locked = false
}

func (m *MockWSHandler) UnlockAndExit(_ *logrus.Entry, _ bool) {
	m.mutex.Unlock()
	m.exited = true
	close(m.exit)
}

func (m *MockWSHandler) IsClosed() bool {
	return m.exited == true
}

func TestWaitSignalDropUser(t *testing.T) {
	isDebug := log.IsDebug()
	logger := log.GetLogger("test").WithField("case", "TestWaitSignal")
	rootConn, err := syncinterface.TaosConnect("", "root", "taosdata", "", 0, logger, false)
	assert.NoError(t, err)
	defer func() {
		syncinterface.TaosClose(rootConn, logger, isDebug)
	}()
	err = exec(rootConn, "create user test_signal pass 'signal_pass1'", logger, isDebug)
	assert.NoError(t, err)
	conn, err := syncinterface.TaosConnect("", "test_signal", "signal_pass1", "", 0, logger, isDebug)
	assert.NoError(t, err)
	defer func() {
		syncinterface.TaosClose(conn, logger, isDebug)
	}()
	h := NewMockWSHandler(conn, "192.168.3.100", logger, isDebug)
	h.exited = true
	exitSignal := make(chan struct{})
	h.dropUserChan <- struct{}{}
	go func() {
		WaitSignal(h, conn, h.ip, h.ipStr, h.whitelistChangeHandle, h.dropUserHandle, h.whitelistChangeChan, h.dropUserChan, h.exit, logger)
		exitSignal <- struct{}{}
	}()
	timeout := time.NewTimer(time.Second * 5)
	select {
	case <-exitSignal:
	case <-timeout.C:
		t.Fatal("wait drop user signal timeout")
	}
	t.Log("wait drop user signal success")
	h = NewMockWSHandler(conn, "192.168.3.100", logger, isDebug)
	go func() {
		WaitSignal(h, conn, h.ip, h.ipStr, h.whitelistChangeHandle, h.dropUserHandle, h.whitelistChangeChan, h.dropUserChan, h.exit, logger)
		exitSignal <- struct{}{}
	}()
	// drop user
	err = exec(rootConn, "drop user test_signal", logger, isDebug)
	assert.NoError(t, err)
	// wait for drop user signal processed
	timeout = time.NewTimer(time.Second * 5)
	select {
	case <-h.exit:
	case <-timeout.C:
		t.Fatal("wait drop user signal timeout")
	}
	select {
	case <-exitSignal:
	case <-time.NewTimer(time.Second * 5).C:
		t.Fatal("wait drop user exit timeout")
	}
	assert.True(t, h.IsClosed())

}

func TestWaitSignalChangeWhitelist(t *testing.T) {
	if !testenv.IsEnterpriseTest() {
		t.Skip("skip enterprise only test")
	}
	isDebug := log.IsDebug()
	logger := log.GetLogger("test").WithField("case", "TestWaitSignal")
	rootConn, err := syncinterface.TaosConnect("", "root", "taosdata", "", 0, logger, false)
	assert.NoError(t, err)
	defer func() {
		syncinterface.TaosClose(rootConn, logger, isDebug)
	}()
	err = exec(rootConn, "create user test_signal_w pass 'signal_pass1'", logger, isDebug)
	assert.NoError(t, err)
	defer func() {
		err = exec(rootConn, "drop user test_signal_w", logger, isDebug)
		assert.NoError(t, err)
	}()
	mockGetWhitelist := func(conn unsafe.Pointer, logger *logrus.Entry, isDebug bool) ([]*net.IPNet, []*net.IPNet, error) {
		_, ipNet, err := net.ParseCIDR("192.168.3.100/32")
		assert.NoError(t, err)
		return nil, []*net.IPNet{ipNet}, nil
	}
	conn, err := syncinterface.TaosConnect("", "test_signal_w", "signal_pass1", "", 0, logger, isDebug)
	assert.NoError(t, err)
	defer func() {
		syncinterface.TaosClose(conn, logger, isDebug)
	}()
	h := NewMockWSHandler(conn, "192.168.3.100", logger, isDebug)
	h.whitelistChangeChan <- 1
	go doWaitSignal(h, conn, h.ip, h.ipStr, h.whitelistChangeHandle, h.dropUserHandle, h.whitelistChangeChan, h.dropUserChan, h.exit, logger, mockGetWhitelist)
	timeout := time.NewTimer(time.Second * 5)
	select {
	case <-h.exit:
	case <-timeout.C:
		t.Fatal("wait signal timeout")
	}
	assert.True(t, h.IsClosed())

	h = NewMockWSHandler(conn, "192.168.3.100", logger, isDebug)
	h.exited = true
	signalExit := make(chan struct{})
	h.whitelistChangeChan <- 1
	go func() {
		doWaitSignal(h, conn, h.ip, h.ipStr, h.whitelistChangeHandle, h.dropUserHandle, h.whitelistChangeChan, h.dropUserChan, h.exit, logger, mockGetWhitelist)
		signalExit <- struct{}{}
	}()
	timeout = time.NewTimer(time.Second * 5)
	select {
	case <-signalExit:
	case <-timeout.C:
		t.Fatal("wait signal timeout")
	}
	mockGetWhitelist = func(conn unsafe.Pointer, logger *logrus.Entry, isDebug bool) ([]*net.IPNet, []*net.IPNet, error) {
		return nil, nil, fmt.Errorf("mock get whitelist error")
	}
	h = NewMockWSHandler(conn, "192.168.3.100", logger, isDebug)
	h.whitelistChangeChan <- 1
	go func() {
		doWaitSignal(h, conn, h.ip, h.ipStr, h.whitelistChangeHandle, h.dropUserHandle, h.whitelistChangeChan, h.dropUserChan, h.exit, logger, mockGetWhitelist)
		signalExit <- struct{}{}
	}()
	timeout = time.NewTimer(time.Second)
	select {
	case <-signalExit:
		t.Fatal("should not exit")
	case <-timeout.C:
	}
	close(h.exit)
	select {
	case <-signalExit:
	case <-time.NewTimer(time.Second * 1).C:
		t.Fatal("wait signal exit timeout")
	}

	mockGetWhitelist = func(conn unsafe.Pointer, logger *logrus.Entry, isDebug bool) ([]*net.IPNet, []*net.IPNet, error) {
		_, ipNet, err := net.ParseCIDR("192.168.3.100/32")
		assert.NoError(t, err)
		return []*net.IPNet{ipNet}, nil, nil
	}
	h = NewMockWSHandler(conn, "192.168.3.100", logger, isDebug)
	h.whitelistChangeChan <- 1
	go func() {
		doWaitSignal(h, conn, h.ip, h.ipStr, h.whitelistChangeHandle, h.dropUserHandle, h.whitelistChangeChan, h.dropUserChan, h.exit, logger, mockGetWhitelist)
		signalExit <- struct{}{}
	}()
	timeout = time.NewTimer(time.Second * 1)
	select {
	case <-signalExit:
		t.Fatal("should not exit")
	case <-timeout.C:
		close(h.exit)
	}
	select {
	case <-signalExit:
	case <-time.NewTimer(time.Second * 1).C:
		t.Fatal("wait signal exit timeout")
	}
}

func TestWaitSignalExit(t *testing.T) {
	isDebug := log.IsDebug()
	logger := log.GetLogger("test").WithField("case", "TestWaitSignalExit")
	rootConn, err := syncinterface.TaosConnect("", "root", "taosdata", "", 0, logger, false)
	assert.NoError(t, err)
	defer func() {
		syncinterface.TaosClose(rootConn, logger, isDebug)
	}()
	h := NewMockWSHandler(rootConn, "192.168.3.100", logger, isDebug)
	exitSignal := make(chan struct{})
	close(h.exit)
	go func() {
		WaitSignal(h, nil, h.ip, h.ipStr, h.whitelistChangeHandle, h.dropUserHandle, h.whitelistChangeChan, h.dropUserChan, h.exit, logger)
		exitSignal <- struct{}{}
	}()
	timeout := time.NewTimer(time.Second * 5)
	select {
	case <-exitSignal:
	case <-timeout.C:
		t.Fatal("wait signal exit timeout")
	}
	t.Log("wait signal exit success")
}

func exec(conn unsafe.Pointer, sql string, logger *logrus.Entry, isDebug bool) error {
	res := syncinterface.TaosQuery(conn, sql, logger, isDebug)
	defer syncinterface.TaosSyncQueryFree(res, logger, isDebug)
	code := syncinterface.TaosError(res, logger, isDebug)
	if code != 0 {
		errStr := syncinterface.TaosErrorStr(res, logger, isDebug)
		return errors.NewError(code, errStr)
	}
	return nil
}
