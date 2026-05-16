package pool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// @author: xftan
// @date: 2022/1/12 18:20
// @description: test string builder pool get
func TestStringBuilderPoolGet(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "test",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := StringBuilderPoolGet()
			defer StringBuilderPoolPut(b)
			b.WriteString("hello")
			b.WriteByte(',')
			b.WriteString("world")
			assert.Equal(t, "hello,world", b.String())
		})
	}
}
