package tool

import (
	"net"
	"strings"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/driver/common"
	"github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/thread"
)

const PoolSize = 10000

var whiteListHandleChan = make(chan cgo.Handle, PoolSize)

func getWhiteListHandle() (chan *wrapper.WhitelistResult, cgo.Handle) {
	select {
	case handle := <-whiteListHandleChan:
		c := handle.Value().(chan *wrapper.WhitelistResult)
		// cleanup channel
		for {
			select {
			case <-c:
			default:
				return c, handle
			}
		}
	default:
		c := make(chan *wrapper.WhitelistResult, 1)
		return c, cgo.NewHandle(c)
	}
}

func putWhiteListHandle(handle cgo.Handle) {
	select {
	case whiteListHandleChan <- handle:
	default:
		handle.Delete()
	}
}

func GetWhitelist(conn unsafe.Pointer, logger *logrus.Entry, isDebug bool) ([]*net.IPNet, []*net.IPNet, error) {
	c, handler := getWhiteListHandle()
	defer putWhiteListHandle(handler)
	taosFetchWhiteListA(conn, handler, logger, isDebug)
	data := <-c
	logger.Debug("get whitelist callback")
	monitor.TaosFetchWhitelistACallBackCounter.Inc()
	if data.Err != nil {
		monitor.TaosFetchWhitelistACallBackFailCounter.Inc()
		return nil, nil, data.Err
	}
	monitor.TaosFetchWhitelistACallBackSuccessCounter.Inc()
	return data.AllowIPNets, data.BlockIPNets, nil
}

func taosFetchWhiteListA(conn unsafe.Pointer, handle cgo.Handle, logger *logrus.Entry, isDebug bool) {
	thread.AsyncSemaphore.Acquire()
	defer func() {
		thread.AsyncSemaphore.Release()
	}()
	syncinterface.TaosFetchWhitelistA(conn, handle, logger, isDebug)
}

func CheckWhitelist(allowlist []*net.IPNet, blocklist []*net.IPNet, ip net.IP) bool {
	for _, ipNet := range blocklist {
		if ipNet.Contains(ip) {
			return false
		}
	}
	for _, ipNet := range allowlist {
		if ipNet.Contains(ip) {
			return true
		}
	}
	return false
}

// whitelist change
var registerChangeWhiteListHandleChan = make(chan cgo.Handle, PoolSize)

func GetRegisterChangeWhiteListHandle() (chan int64, cgo.Handle) {
	select {
	case handle := <-registerChangeWhiteListHandleChan:
		c := handle.Value().(chan int64)
		// cleanup channel
		for {
			select {
			case <-c:
			default:
				return c, handle
			}
		}
	default:
		c := make(chan int64, 1)
		return c, cgo.NewHandle(c)
	}
}

func PutRegisterChangeWhiteListHandle(handle cgo.Handle) {
	select {
	case registerChangeWhiteListHandleChan <- handle:
	default:
		handle.Delete()
	}
}

func RegisterChangeWhitelist(conn unsafe.Pointer, handle cgo.Handle, logger *logrus.Entry, isDebug bool) error {
	errCode := syncinterface.TaosSetNotifyCB(conn, handle, common.TAOS_NOTIFY_WHITELIST_VER, logger, isDebug)
	if errCode != 0 {
		return errors.NewError(int(errCode), syncinterface.TaosErrorStr(nil, logger, isDebug))
	}
	return nil
}

// drop user
var registerDropUserHandleChan = make(chan cgo.Handle, PoolSize)

func GetRegisterDropUserHandle() (chan struct{}, cgo.Handle) {
	select {
	case handle := <-registerDropUserHandleChan:
		c := handle.Value().(chan struct{})
		// cleanup channel
		for {
			select {
			case <-c:
			default:
				return c, handle
			}
		}
	default:
		c := make(chan struct{}, 1)
		return c, cgo.NewHandle(c)
	}
}

func PutRegisterDropUserHandle(handle cgo.Handle) {
	select {
	case registerDropUserHandleChan <- handle:
	default:
		handle.Delete()
	}
}

func RegisterDropUser(conn unsafe.Pointer, handle cgo.Handle, logger *logrus.Entry, isDebug bool) error {
	errCode := syncinterface.TaosSetNotifyCB(conn, handle, common.TAOS_NOTIFY_USER_DROPPED, logger, isDebug)
	if errCode != 0 {
		return errors.NewError(int(errCode), syncinterface.TaosErrorStr(nil, logger, isDebug))
	}
	return nil
}

// change password
var registerChangePassHandleChan = make(chan cgo.Handle, PoolSize)

func GetRegisterChangePassHandle() (chan int32, cgo.Handle) {
	select {
	case handle := <-registerChangePassHandleChan:
		// cleanup channel
		c := handle.Value().(chan int32)
		for {
			select {
			case <-c:
			default:
				return c, handle
			}
		}
	default:
		c := make(chan int32, 1)
		return c, cgo.NewHandle(c)
	}
}

func PutRegisterChangePassHandle(handle cgo.Handle) {
	select {
	case registerChangePassHandleChan <- handle:
	default:
		handle.Delete()
	}
}

func RegisterChangePass(conn unsafe.Pointer, handle cgo.Handle, logger *logrus.Entry, isDebug bool) error {
	errCode := syncinterface.TaosSetNotifyCB(conn, handle, common.TAOS_NOTIFY_PASSVER, logger, isDebug)
	if errCode != 0 {
		return errors.NewError(int(errCode), syncinterface.TaosErrorStr(nil, logger, isDebug))
	}
	return nil
}

func IpNetSliceToString(ipNets []*net.IPNet) string {
	builder := strings.Builder{}
	for i, ipNet := range ipNets {
		builder.WriteString(ipNet.String())
		if i != len(ipNets)-1 {
			builder.WriteString(",")
		}
	}
	return builder.String()
}
