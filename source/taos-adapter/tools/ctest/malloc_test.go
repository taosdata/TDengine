package ctest

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMalloc(t *testing.T) {
	p := Malloc(1024)
	assert.NotNil(t, p)
	Free(p)
}
