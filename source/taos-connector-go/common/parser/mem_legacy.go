//go:build !go1.22
// +build !go1.22

package parser

import (
	"unsafe"
)

type slice struct {
	Data unsafe.Pointer
	Len  int
	Cap  int
}

func Copy(source unsafe.Pointer, data []byte, index int, length int) {
	src := *(*[]byte)(unsafe.Pointer(&slice{
		Data: source,
		Len:  length,
		Cap:  length,
	}))
	copy(data[index:index+length], src)
}
