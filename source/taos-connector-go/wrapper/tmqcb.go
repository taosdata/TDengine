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

	"github.com/taosdata/driver-go/v3/wrapper/cgo"
)

//typedef void(tmq_commit_cb(tmq_t *, int32_t code, void *param));

//export TMQCommitCB
func TMQCommitCB(consumer unsafe.Pointer, resp C.int32_t, param unsafe.Pointer) {
	c := (*(*cgo.Handle)(param)).Value().(chan *TMQCommitCallbackResult)
	r := GetTMQCommitCallbackResult(int32(resp), consumer)
	defer func() {
		// Avoid panic due to channel closed
		_ = recover()
	}()
	c <- r
}

//export TMQAutoCommitCB
func TMQAutoCommitCB(consumer unsafe.Pointer, resp C.int32_t, param unsafe.Pointer) {
	c := (*(*cgo.Handle)(param)).Value().(chan *TMQCommitCallbackResult)
	r := GetTMQCommitCallbackResult(int32(resp), consumer)
	defer func() {
		// Avoid panic due to channel closed
		_ = recover()
	}()
	c <- r
}

//export TMQCommitOffsetCB
func TMQCommitOffsetCB(consumer unsafe.Pointer, resp C.int32_t, param unsafe.Pointer) {
	c := (*(*cgo.Handle)(param)).Value().(chan *TMQCommitCallbackResult)
	r := GetTMQCommitCallbackResult(int32(resp), consumer)
	defer func() {
		// Avoid panic due to channel closed
		_ = recover()
	}()
	c <- r
}
