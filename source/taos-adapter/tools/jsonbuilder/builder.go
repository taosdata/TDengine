package jsonbuilder

import (
	"io"
	"sync"
)

var defaultConfig = NewJsonConfig(0)
var streamPool = &sync.Pool{
	New: func() interface{} {
		return NewStream(defaultConfig, nil, 512)
	},
}

func BorrowStream(writer io.Writer) *Stream {
	stream := streamPool.Get().(*Stream)
	stream.Reset(writer)
	return stream
}

func ReturnStream(stream *Stream) {
	stream.out = nil
	stream.Error = nil
	stream.Attachment = nil
	streamPool.Put(stream)
}
