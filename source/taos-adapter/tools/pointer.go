package tools

import "unsafe"

func AddPointer(pointer unsafe.Pointer, offset uintptr) unsafe.Pointer {
	return unsafe.Pointer(uintptr(pointer) + offset)
}
