package wstool

import (
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
)

type NotifyHandles struct {
	WhitelistChangeChan   chan int64
	WhitelistChangeHandle cgo.Handle
	DropUserChan          chan struct{}
	DropUserHandle        cgo.Handle
}

func (n *NotifyHandles) InitNotifyHandles() {
	if n.WhitelistChangeHandle == 0 {
		n.WhitelistChangeChan, n.WhitelistChangeHandle = tool.GetRegisterChangeWhiteListHandle()
	}
	if n.DropUserHandle == 0 {
		n.DropUserChan, n.DropUserHandle = tool.GetRegisterDropUserHandle()
	}
}

func (n *NotifyHandles) PutNotifyHandles() {
	if n.WhitelistChangeHandle != 0 {
		tool.PutRegisterChangeWhiteListHandle(n.WhitelistChangeHandle)
		n.WhitelistChangeHandle = 0
		n.WhitelistChangeChan = nil
	}
	if n.DropUserHandle != 0 {
		tool.PutRegisterDropUserHandle(n.DropUserHandle)
		n.DropUserHandle = 0
		n.DropUserChan = nil
	}
}
