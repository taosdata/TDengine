package pool

import (
	"strings"
	"sync"
)

var stringBuilderPool sync.Pool

func init() {
	stringBuilderPool.New = func() interface{} {
		return &strings.Builder{}
	}
}

func StringBuilderPoolGet() *strings.Builder {
	return stringBuilderPool.Get().(*strings.Builder)
}

func StringBuilderPoolPut(b *strings.Builder) {
	b.Reset()
	stringBuilderPool.Put(b)
}
