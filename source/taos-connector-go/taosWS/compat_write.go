package taosWS

import "bytes"

// Deprecated: WriteUint64 is kept only for taosWS source compatibility.
func WriteUint64(buffer *bytes.Buffer, v uint64) {
	buffer.WriteByte(byte(v))
	buffer.WriteByte(byte(v >> 8))
	buffer.WriteByte(byte(v >> 16))
	buffer.WriteByte(byte(v >> 24))
	buffer.WriteByte(byte(v >> 32))
	buffer.WriteByte(byte(v >> 40))
	buffer.WriteByte(byte(v >> 48))
	buffer.WriteByte(byte(v >> 56))
}

// Deprecated: WriteUint32 is kept only for taosWS source compatibility.
func WriteUint32(buffer *bytes.Buffer, v uint32) {
	buffer.WriteByte(byte(v))
	buffer.WriteByte(byte(v >> 8))
	buffer.WriteByte(byte(v >> 16))
	buffer.WriteByte(byte(v >> 24))
}

// Deprecated: WriteUint16 is kept only for taosWS source compatibility.
func WriteUint16(buffer *bytes.Buffer, v uint16) {
	buffer.WriteByte(byte(v))
	buffer.WriteByte(byte(v >> 8))
}
