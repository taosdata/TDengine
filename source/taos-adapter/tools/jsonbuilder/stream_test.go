package jsonbuilder

import (
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var ConfigDefault = &JsonConfig{indentionStep: 0}

func Test_writeByte_should_grow_buffer(t *testing.T) {
	should := require.New(t)
	stream := NewStream(ConfigDefault, nil, 1)
	stream.writeByte('1')
	should.Equal("1", string(stream.Buffer()))
	should.Equal(1, len(stream.buf))
	stream.writeByte('2')
	should.Equal("12", string(stream.Buffer()))
	should.Equal(2, len(stream.buf))
	stream.writeThreeBytes('3', '4', '5')
	should.Equal("12345", string(stream.Buffer()))
}

func Test_writeBytes_should_grow_buffer(t *testing.T) {
	should := require.New(t)
	stream := NewStream(ConfigDefault, nil, 1)
	_, err := stream.Write([]byte{'1', '2'})
	assert.NoError(t, err)
	should.Equal("12", string(stream.Buffer()))
	should.Equal(2, len(stream.buf))
	_, err = stream.Write([]byte{'3', '4', '5', '6', '7'})
	assert.NoError(t, err)
	should.Equal("1234567", string(stream.Buffer()))
	should.Equal(7, len(stream.buf))
}

func Test_writeRaw_should_grow_buffer(t *testing.T) {
	should := require.New(t)
	stream := NewStream(ConfigDefault, nil, 1)
	stream.WriteRaw("123")
	should.Nil(stream.Error)
	should.Equal("123", string(stream.Buffer()))
}

func Test_writeString_should_grow_buffer(t *testing.T) {
	should := require.New(t)
	stream := NewStream(ConfigDefault, nil, 0)
	stream.WriteString("123")
	should.Nil(stream.Error)
	should.Equal(`"123"`, string(stream.Buffer()))
}

type NopWriter struct {
	bufferSize int
}

func (w *NopWriter) Write(p []byte) (n int, err error) {
	w.bufferSize = cap(p)
	return len(p), nil
}

func Test_flush_buffer_should_stop_grow_buffer(t *testing.T) {
	// Stream an array of a zillion zeros.
	writer := new(NopWriter)
	stream := NewStream(ConfigDefault, writer, 512)
	stream.WriteArrayStart()
	for i := 0; i < 10000000; i++ {
		stream.WriteInt(0)
		stream.WriteMore()
		err := stream.Flush()
		assert.NoError(t, err)
	}
	stream.WriteInt(0)
	stream.WriteArrayEnd()

	// Confirm that the buffer didn't have to grow.
	should := require.New(t)

	// 512 is the internal buffer size set in NewEncoder
	//
	// Flush is called after each array element, so only the first 8 bytes of it
	// is ever used, and it is never extended. Capacity remains 512.
	should.Equal(512, writer.bufferSize)
}

func TestStream_Common(t *testing.T) {
	writer := new(NopWriter)
	writer2 := new(NopWriter)
	stream := NewStream(ConfigDefault, writer, 512)
	assert.Equal(t, 512, stream.Available())
	assert.Equal(t, 0, stream.Buffered())
	stream.SetBuffer(make([]byte, 0, 512))
	stream.Reset(writer2)
	_, err := stream.Write([]byte{1})
	assert.NoError(t, err)
	stream.WriteArrayStart()
	stream.AddByte(1)
	err = stream.Flush()
	assert.NoError(t, err)
	stream.WriteNil()
	stream.WriteTrue()
	stream.WriteFalse()
	stream.WriteBool(true)
	stream.WriteBool(false)
	stream.WriteObjectStart()
	stream.WriteObjectField("a")
	stream.WriteObjectEnd()
	stream.WriteEmptyObject()
	stream.WriteEmptyArray()
	stream.WriteFloat32(1)
	stream.WriteFloat32(float32(math.Inf(1)))
	stream.WriteFloat32(float32(math.NaN()))
	stream.WriteFloat32(math.MaxFloat32)
	stream.WriteFloat32Lossy(1)
	stream.WriteFloat32Lossy(float32(math.Inf(1)))
	stream.WriteFloat32Lossy(float32(math.NaN()))
	stream.WriteFloat32Lossy(math.MaxFloat32)
	stream.WriteFloat64(1)
	stream.WriteFloat64(math.Inf(1))
	stream.WriteFloat64(math.MaxFloat64)
	stream.WriteFloat64Lossy(1)
	stream.WriteFloat64Lossy(math.Inf(1))
	stream.WriteFloat64Lossy(math.MaxFloat64)
	stream.WriteInt8(1)
	stream.WriteInt16(1)
	stream.WriteInt32(1)
	stream.WriteInt64(1)
	stream.WriteInt(1)
	stream.WriteInt8(-1)
	stream.WriteInt16(-1)
	stream.WriteInt32(-1)
	stream.WriteInt64(-1)
	stream.WriteInt(-1)
	stream.WriteUint8(1)
	stream.WriteUint16(1)
	stream.WriteUint32(1)
	stream.WriteUint64(1)
	stream.WriteUint(1)
	stream.WriteUint8(math.MaxUint8)
	stream.WriteUint16(math.MaxUint16)
	stream.WriteUint32(math.MaxUint32)
	stream.WriteUint64(math.MaxUint64)
	stream.WriteUint(math.MaxUint64)
	stream.WriteStringByte('"')
	stream.WriteStringByte('/')
	stream.WriteStringByte('a')
	stream.WriteStringByte('\n')
	stream.WriteStringByte('\r')
	stream.WriteStringByte('\t')
	stream.WriteString("\r\n\t/")
	stream.WriteRuneString('"')
	stream.WriteRuneString('/')
	stream.WriteRuneString('a')
	stream.WriteRuneString('\n')
	stream.WriteRuneString('\r')
	stream.WriteRuneString('\t')
	stream.WriteRuneString('A')
	stream.WriteRuneString('é')
	stream.WriteRuneString('你')
	stream.WriteRuneString('𐍈')

	stream.WriteRune('"')
	stream.WriteRune('/')
	stream.WriteRune('a')
	stream.WriteRune('\n')
	stream.WriteRune('\r')
	stream.WriteRune('\t')

	stream.WriteRune('A')
	stream.WriteRune('é')
	stream.WriteRune('你')
	stream.WriteRune('𐍈')

}

func TestStr(t *testing.T) {
	b := &strings.Builder{}
	stream := BorrowStream(b)
	stream.WriteString("a\nb")
	err := stream.Flush()
	assert.NoError(t, err)
	assert.Equal(t, "\"a\\nb\"", b.String())
}

func TestStrByte(t *testing.T) {
	b := &strings.Builder{}
	stream := BorrowStream(b)
	stream.WriteStringByte('a')
	stream.WriteStringByte('\n')
	stream.WriteStringByte('b')
	err := stream.Flush()
	assert.NoError(t, err)
	assert.Equal(t, "a\\nb", b.String())
}

func TestUint64(t *testing.T) {
	b := &strings.Builder{}
	tests := []struct {
		input    uint64
		expected string
	}{
		{0, "0"},
		{9, "9"},
		{10, "10"},
		{999, "999"},
		{1000, "1000"},
		{123456, "123456"},
		{999999, "999999"},
		{1000000, "1000000"},
		{1001001, "1001001"},
		{9876543210, "9876543210"},
		{18446744073709551615, "18446744073709551615"}, // 最大 uint64 值
	}
	for _, test := range tests {
		b.Reset()
		stream := BorrowStream(b)
		stream.WriteUint64(test.input)
		result := string(stream.buf)
		if result != test.expected {
			t.Errorf("WriteUint64(%d) = %s; want %s", test.input, result, test.expected)
		}
		ReturnStream(stream)
	}
}

func TestWriteString(t *testing.T) {
	b := &strings.Builder{}
	tests := []struct {
		input    string
		expected string
	}{
		{"hello", `"hello"`},
		{"he\"llo", `"he\"llo"`},
		{"he\\llo", `"he\\llo"`},
		{"he\nllo", `"he\nllo"`},
		{"he\rllo", `"he\rllo"`},
		{"he\tllo", `"he\tllo"`},
		{"\x01\x02hello", `"\u0001\u0002hello"`},
		{"\x03\x04world", `"\u0003\u0004world"`},
		{"<>&", `"<>&"`},
	}

	for _, test := range tests {
		b.Reset()
		stream := BorrowStream(b)
		stream.WriteString(test.input)

		result := string(stream.buf)
		if result != test.expected {
			t.Errorf("WriteString(%q) = %s; want %s", test.input, result, test.expected)
		}
		ReturnStream(stream)
	}
}

func TestWriteStringByte(t *testing.T) {
	b := &strings.Builder{}
	tests := []struct {
		input    byte
		expected string
	}{
		{'h', "h"},       // 正常字符，无需转义
		{'"', `\"`},      // 引号转义
		{'\\', `\\`},     // 反斜杠转义
		{'\n', `\n`},     // 换行符转义
		{'\r', `\r`},     // 回车符转义
		{'\t', `\t`},     // 制表符转义
		{0x01, `\u0001`}, // 触发 default 分支的控制字符
		{0x04, `\u0004`}, // 触发 default 分支的控制字符
		{'>', ">"},
		{'<', "<"},
		{'&', "&"},
	}

	for _, test := range tests {
		b.Reset()
		stream := BorrowStream(b)
		stream.WriteStringByte(test.input)

		result := string(stream.buf)
		if result != test.expected {
			t.Errorf("WriteStringByte(%q) = %s; want %s", test.input, result, test.expected)
		}
		ReturnStream(stream)
	}
}

func TestWriteInfAndNaN(t *testing.T) {
	b := &strings.Builder{}
	stream := BorrowStream(b)
	defer func() {
		ReturnStream(stream)
	}()
	stream.WriteFloat32(float32(math.Inf(1)))
	stream.writeByte(',')
	stream.WriteFloat32(float32(math.NaN()))
	stream.writeByte(',')
	stream.WriteFloat64(math.Inf(-1))
	stream.writeByte(',')
	stream.WriteFloat64(math.NaN())
	err := stream.Flush()
	assert.NoError(t, err)
	assert.Equal(t, "null,null,null,null", b.String())

	b.Reset()
	stream.WriteFloat32Lossy(float32(math.Inf(1)))
	stream.writeByte(',')
	stream.WriteFloat32Lossy(float32(math.NaN()))
	stream.writeByte(',')
	stream.WriteFloat64Lossy(math.Inf(-1))
	stream.writeByte(',')
	stream.WriteFloat64Lossy(math.NaN())
	err = stream.Flush()
	assert.NoError(t, err)
	assert.Equal(t, "null,null,null,null", b.String())
}
