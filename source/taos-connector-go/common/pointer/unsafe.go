package pointer

import "unsafe"

func AddUintptr(ptr unsafe.Pointer, len uintptr) unsafe.Pointer {
	return unsafe.Pointer(uintptr(ptr) + len)
}
