package wrapper

/*
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
*/
import "C"
import (
	"unsafe"

	"github.com/taosdata/taosadapter/v3/driver/common"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
)

//export NotifyCallback
func NotifyCallback(p unsafe.Pointer, ext unsafe.Pointer, notifyType C.int) {
	defer func() {
		// channel may be closed
		_ = recover()
	}()
	switch int(notifyType) {
	case common.TAOS_NOTIFY_PASSVER:
		version := int32(*(*C.int32_t)(ext))
		c := (*(*cgo.Handle)(p)).Value().(chan int32)
		c <- version
	case common.TAOS_NOTIFY_WHITELIST_VER:
		version := int64(*(*C.int64_t)(ext))
		c := (*(*cgo.Handle)(p)).Value().(chan int64)
		c <- version
	case common.TAOS_NOTIFY_USER_DROPPED:
		c := (*(*cgo.Handle)(p)).Value().(chan struct{})
		c <- struct{}{}
	}
}
