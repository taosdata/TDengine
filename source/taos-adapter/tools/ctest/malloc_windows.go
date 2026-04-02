//go:build windows

package ctest

/*
#include <stdlib.h>
#include <string.h>
*/
import "C"
import "unsafe"

func Malloc(size int) unsafe.Pointer {
	p := C.malloc(C.ulonglong(size))
	C.memset(p, 1, C.ulonglong(size))
	return p
}

func Free(p unsafe.Pointer) {
	C.free(p)
}
