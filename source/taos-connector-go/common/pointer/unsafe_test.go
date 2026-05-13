package pointer

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestAddUintptr(t *testing.T) {
	data := []byte{1, 2, 3, 4, 5}
	p1 := unsafe.Pointer(&data[0])
	p2 := AddUintptr(p1, 1)
	assert.Equal(t, unsafe.Pointer(&data[1]), p2)
	v2 := *(*byte)(p2)
	assert.Equal(t, byte(2), v2)

}
