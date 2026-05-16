package jsonbuilder

// Numbers fundamental to the encoding.
const (
	RuneError = '\uFFFD'     // the "error" Rune or "Unicode replacement character"
	MaxRune   = '\U0010FFFF' // Maximum valid Unicode code point.
)

// Code points in the surrogate range are not valid for UTF-8.
const (
	surrogateMin = 0xD800
	surrogateMax = 0xDFFF
)

const (
	tx = 0b10000000
	t2 = 0b11000000
	t3 = 0b11100000
	t4 = 0b11110000

	maskx = 0b00111111

	rune1Max = 1<<7 - 1
	rune2Max = 1<<11 - 1
	rune3Max = 1<<16 - 1
)

func (stream *Stream) WriteRune(r rune) {
	if uint32(r) <= rune1Max {
		stream.writeByte(byte(r))
		return
	}
	switch i := uint32(r); {
	case i <= rune2Max:
		stream.writeByte(t2 | byte(r>>6))
		stream.writeByte(tx | byte(r)&maskx)
		return
	case i > MaxRune, surrogateMin <= i && i <= surrogateMax:
		r = RuneError
		fallthrough
	case i <= rune3Max:
		stream.writeByte(t3 | byte(r>>12))
		stream.writeByte(tx | byte(r>>6)&maskx)
		stream.writeByte(tx | byte(r)&maskx)
		return
	default:
		stream.writeByte(t4 | byte(r>>18))
		stream.writeByte(tx | byte(r>>12)&maskx)
		stream.writeByte(tx | byte(r>>6)&maskx)
		stream.writeByte(tx | byte(r)&maskx)
		return
	}
}

func (stream *Stream) WriteRuneString(r rune) {
	if uint32(r) <= rune1Max {
		stream.WriteStringByte(byte(r))
		return
	}
	switch i := uint32(r); {
	case i <= rune2Max:
		stream.WriteStringByte(t2 | byte(r>>6))
		stream.WriteStringByte(tx | byte(r)&maskx)
		return
	case i > MaxRune, surrogateMin <= i && i <= surrogateMax:
		r = RuneError
		fallthrough
	case i <= rune3Max:
		stream.WriteStringByte(t3 | byte(r>>12))
		stream.WriteStringByte(tx | byte(r>>6)&maskx)
		stream.WriteStringByte(tx | byte(r)&maskx)
		return
	default:
		stream.WriteStringByte(t4 | byte(r>>18))
		stream.WriteStringByte(tx | byte(r>>12)&maskx)
		stream.WriteStringByte(tx | byte(r>>6)&maskx)
		stream.WriteStringByte(tx | byte(r)&maskx)
		return
	}
}
