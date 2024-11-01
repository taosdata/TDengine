package pool

import (
	"bytes"
	"sync"
)

var bytesBufferPool sync.Pool

func init() {
	bytesBufferPool.New = func() interface{} {
		return &bytes.Buffer{}
	}
}

func BytesPoolGet() *bytes.Buffer {
	return bytesBufferPool.Get().(*bytes.Buffer)
}

func BytesPoolPut(b *bytes.Buffer) {
	b.Reset()
	bytesBufferPool.Put(b)
}
