//go:build go1.22
// +build go1.22

package parser

import "unsafe"

func Copy(source unsafe.Pointer, data []byte, index int, length int) {
	dst := data[index : index+length]
	src := unsafe.Slice((*byte)(source), length)
	copy(dst, src)
}
