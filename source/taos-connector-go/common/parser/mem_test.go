package parser

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestCopy(t *testing.T) {
	data := []byte("World")
	data1 := make([]byte, 10)
	data1[0] = 'H'
	data1[1] = 'e'
	data1[2] = 'l'
	data1[3] = 'l'
	data1[4] = 'o'
	Copy(unsafe.Pointer(&data[0]), data1, 5, 5)
	assert.Equal(t, "HelloWorld", string(data1))
}
