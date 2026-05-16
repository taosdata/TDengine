package tools

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestAddPointer(t *testing.T) {
	s := []int32{1, 2, 3}
	p0 := unsafe.Pointer(&s[0])
	p1 := AddPointer(p0, 4)
	assert.Equal(t, *(*int32)(p1), s[1])
	p2 := AddPointer(p1, 4)
	assert.Equal(t, *(*int32)(p2), s[2])
}
