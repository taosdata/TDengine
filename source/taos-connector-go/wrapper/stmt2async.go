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

type TaosStmt2CallbackCaller interface {
	ExecCall(res unsafe.Pointer, affected int, code int)
}

//export Stmt2ExecCallback
func Stmt2ExecCallback(p unsafe.Pointer, res *C.TAOS_RES, code C.int) {
	caller := (*(*cgo.Handle)(p)).Value().(TaosStmt2CallbackCaller)
	affectedRows := int(C.taos_affected_rows(unsafe.Pointer(res)))
	caller.ExecCall(unsafe.Pointer(res), affectedRows, int(code))
}
