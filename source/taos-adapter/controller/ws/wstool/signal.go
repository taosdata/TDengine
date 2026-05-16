package wstool

import (
	"net"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
	"github.com/taosdata/taosadapter/v3/log"
)

type WSHandler interface {
	Lock(logger *logrus.Entry, isDebug bool)
	Unlock()
	UnlockAndExit(logger *logrus.Entry, isDebug bool)
	IsClosed() bool
}

type getWhitelistFunc func(conn unsafe.Pointer, logger *logrus.Entry, isDebug bool) ([]*net.IPNet, []*net.IPNet, error)

func WaitSignal(
	h WSHandler,
	conn unsafe.Pointer,
	ip net.IP,
	ipStr string,
	whitelistChangeHandle cgo.Handle,
	dropUserHandle cgo.Handle,
	whitelistChangeChan chan int64,
	dropUserChan chan struct{},
	exit chan struct{},
	logger *logrus.Entry,
) {
	doWaitSignal(h, conn, ip, ipStr, whitelistChangeHandle, dropUserHandle, whitelistChangeChan, dropUserChan, exit, logger, tool.GetWhitelist)
}

func doWaitSignal(h WSHandler, conn unsafe.Pointer, ip net.IP, ipStr string, whitelistChangeHandle cgo.Handle, dropUserHandle cgo.Handle, whitelistChangeChan chan int64, dropUserChan chan struct{}, exit chan struct{}, logger *logrus.Entry, whitelist getWhitelistFunc) {
	logger.Debug("enter wait signal")
	defer func() {
		logger.Debug("exit wait signal")
		tool.PutRegisterChangeWhiteListHandle(whitelistChangeHandle)
		tool.PutRegisterDropUserHandle(dropUserHandle)
	}()
	for {
		select {
		case <-dropUserChan:
			logger.Debug("get drop user signal")
			isDebug := log.IsDebug()
			h.Lock(logger, isDebug)
			if h.IsClosed() {
				logger.Debug("server closed")
				h.Unlock()
				return
			}
			logger.Debug("user dropped, close connection")
			h.UnlockAndExit(logger, isDebug)
			return
		case <-whitelistChangeChan:
			logger.Debug("get whitelist change signal")
			isDebug := log.IsDebug()
			h.Lock(logger, isDebug)
			if h.IsClosed() {
				logger.Debug("server closed")
				h.Unlock()
				return
			}
			logger.Debug("get whitelist")
			allowlist, blocklist, err := whitelist(conn, logger, isDebug)
			if err != nil {
				// don't exit, just unlock and continue
				logger.Errorf("get whitelist error, err:%s", err)
			} else {
				allowlistStr := tool.IpNetSliceToString(allowlist)
				blocklistStr := tool.IpNetSliceToString(blocklist)
				logger.Tracef("check whitelist, ip:%s, allowlist:%s, blocklist:%s", ipStr, allowlistStr, blocklistStr)
				valid := tool.CheckWhitelist(allowlist, blocklist, ip)
				if !valid {
					logger.Errorf("ip not in whitelist! close connection, ip:%s, allowlist:%s, blocklist:%s", ipStr, allowlistStr, blocklistStr)
					h.UnlockAndExit(logger, isDebug)
					return
				}
			}
			h.Unlock()
		case <-exit:
			return
		}
	}
}
