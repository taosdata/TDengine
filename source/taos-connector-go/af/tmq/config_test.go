package tmq

import (
	"testing"

	"github.com/taosdata/driver-go/v3/wrapper"
)

// @author: xftan
// @date: 2023/10/13 11:10
// @description: test config
func TestConfig(t *testing.T) {
	conf := newConfig()
	conf.destroy()
}

// @author: xftan
// @date: 2023/10/13 11:11
// @description: test topic list
func TestList(t *testing.T) {
	topicList := wrapper.TMQListNew()
	wrapper.TMQListAppend(topicList, "123")
	wrapper.TMQListDestroy(topicList)
}
