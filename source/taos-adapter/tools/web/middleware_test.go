package web

import (
	"testing"

	"github.com/gin-gonic/gin"
)

func TestSetTaosErrorCode(t *testing.T) {
	type args struct {
		c    *gin.Context
		code int
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test",
			args: args{
				c:    &gin.Context{},
				code: 0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(_ *testing.T) {
			SetTaosErrorCode(tt.args.c, tt.args.code)
		})
	}
}
