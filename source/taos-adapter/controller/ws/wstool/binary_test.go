package wstool

import (
	"bytes"
	"testing"
)

func TestWriteUint64(t *testing.T) {
	var buf bytes.Buffer
	var expected = []byte{0xEF, 0xCD, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x01}
	WriteUint64(&buf, 0x0123456789ABCDEF)
	if !bytes.Equal(buf.Bytes(), expected) {
		t.Errorf("WriteUint64 produces incorrect output, got %v, expected %v", buf.Bytes(), expected)
	}
}

func TestWriteUint32(t *testing.T) {
	var buf bytes.Buffer
	var expected = []byte{0x67, 0x45, 0x23, 0x01}
	WriteUint32(&buf, 0x01234567)
	if !bytes.Equal(buf.Bytes(), expected) {
		t.Errorf("WriteUint32 produces incorrect output, got %v, expected %v", buf.Bytes(), expected)
	}
}

func TestWriteUint16(t *testing.T) {
	var buf bytes.Buffer
	var expected = []byte{0x23, 0x01}
	WriteUint16(&buf, 0x0123)
	if !bytes.Equal(buf.Bytes(), expected) {
		t.Errorf("WriteUint16 produces incorrect output, got %v, expected %v", buf.Bytes(), expected)
	}
}
