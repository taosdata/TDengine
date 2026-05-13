package stmt

import (
	"bytes"
	"encoding/binary"
	"testing"
	"unsafe"
)

func TestCopyUint32SliceToBytes(t *testing.T) {
	order := nativeByteOrder()
	const sentinel byte = 0x7f

	tests := []struct {
		name      string
		source    []uint32
		extraTail int
	}{
		{
			name:      "single value",
			source:    []uint32{0x01020304},
			extraTail: 0,
		},
		{
			name:      "multiple values with tail untouched",
			source:    []uint32{0, 1, 0x11223344, 0x89abcdef, 0xffffffff},
			extraTail: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dest := bytes.Repeat([]byte{sentinel}, len(tt.source)*4+tt.extraTail)
			copyUint32SliceToBytes(dest, tt.source)

			wantPrefix := make([]byte, len(tt.source)*4)
			for i, v := range tt.source {
				order.PutUint32(wantPrefix[i*4:], v)
			}

			if !bytes.Equal(dest[:len(wantPrefix)], wantPrefix) {
				t.Fatalf("copied bytes mismatch, got=%v want=%v", dest[:len(wantPrefix)], wantPrefix)
			}

			for i, b := range dest[len(wantPrefix):] {
				if b != sentinel {
					t.Fatalf("tail byte changed at index %d, got=%d want=%d", i, b, sentinel)
				}
			}
		})
	}
}

func TestCopyUint16SliceToBytes(t *testing.T) {
	order := nativeByteOrder()
	const sentinel byte = 0x5a

	tests := []struct {
		name      string
		source    []uint16
		extraTail int
	}{
		{
			name:      "single value",
			source:    []uint16{0x0102},
			extraTail: 0,
		},
		{
			name:      "multiple values with tail untouched",
			source:    []uint16{0, 1, 0x1234, 0xabcd, 0xffff},
			extraTail: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dest := bytes.Repeat([]byte{sentinel}, len(tt.source)*2+tt.extraTail)
			copyUint16SliceToBytes(dest, tt.source)

			wantPrefix := make([]byte, len(tt.source)*2)
			for i, v := range tt.source {
				order.PutUint16(wantPrefix[i*2:], v)
			}

			if !bytes.Equal(dest[:len(wantPrefix)], wantPrefix) {
				t.Fatalf("copied bytes mismatch, got=%v want=%v", dest[:len(wantPrefix)], wantPrefix)
			}

			for i, b := range dest[len(wantPrefix):] {
				if b != sentinel {
					t.Fatalf("tail byte changed at index %d, got=%d want=%d", i, b, sentinel)
				}
			}
		})
	}
}

func nativeByteOrder() binary.ByteOrder {
	var value uint16 = 0x0102
	if *(*byte)(unsafe.Pointer(&value)) == 0x01 {
		return binary.BigEndian
	}
	return binary.LittleEndian
}
