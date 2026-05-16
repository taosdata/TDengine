//go:build go1.22
// +build go1.22

package stmt

import "unsafe"

func copyUint32SliceToBytes(dest []byte, source []uint32) {
	copy(dest, unsafe.Slice((*byte)(unsafe.Pointer(&source[0])), len(source)*4))
}

func copyUint16SliceToBytes(dest []byte, source []uint16) {
	copy(dest, unsafe.Slice((*byte)(unsafe.Pointer(&source[0])), len(source)*2))
}
