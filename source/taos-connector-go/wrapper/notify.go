package wrapper

/*
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
extern void NotifyCallback(void *param, void *ext, int type);
int taos_set_notify_cb_wrapper(TAOS *taos, void *param, int type){
	return taos_set_notify_cb(taos,NotifyCallback,param,type);
};
*/
import "C"
import (
	"unsafe"

	"github.com/taosdata/driver-go/v3/wrapper/cgo"
)

func TaosSetNotifyCB(taosConnect unsafe.Pointer, caller cgo.Handle, notifyType int) int32 {
	return int32(C.taos_set_notify_cb_wrapper(taosConnect, caller.Pointer(), (C.int)(notifyType)))
}
