package plugin

import (
	"testing"

	"github.com/gin-gonic/gin"
)

type fakePlugin struct {
}

func (f *fakePlugin) Init(_ gin.IRouter) error {
	return nil
}

func (f *fakePlugin) Start() error {
	return nil
}

func (f *fakePlugin) Stop() error {
	return nil
}

func (f *fakePlugin) String() string {
	return "fake"
}

func (f *fakePlugin) Version() string {
	return "v1"
}

// @author: xftan
// @date: 2021/12/14 15:09
// @description: test plugin register
func TestRegister(_ *testing.T) {
	Register(&fakePlugin{})
	r := gin.Default()
	Init(r)
	Start()
	Stop()
}
