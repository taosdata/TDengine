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

type Caller interface {
	QueryCall(res unsafe.Pointer, code int)
	FetchCall(res unsafe.Pointer, numOfRows int)
}

//export QueryCallback
func QueryCallback(p unsafe.Pointer, res *C.TAOS_RES, code C.int) {
	caller := (*(*cgo.Handle)(p)).Value().(Caller)
	caller.QueryCall(unsafe.Pointer(res), int(code))
}

//export FetchRowsCallback
func FetchRowsCallback(p unsafe.Pointer, res *C.TAOS_RES, numOfRows C.int) {
	caller := (*(*cgo.Handle)(p)).Value().(Caller)
	caller.FetchCall(unsafe.Pointer(res), int(numOfRows))
}

//export FetchRawBlockCallback
func FetchRawBlockCallback(p unsafe.Pointer, res *C.TAOS_RES, numOfRows C.int) {
	caller := (*(*cgo.Handle)(p)).Value().(Caller)
	caller.FetchCall(unsafe.Pointer(res), int(numOfRows))
}
