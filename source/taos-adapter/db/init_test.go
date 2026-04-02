package db

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/taosdata/taosadapter/v3/config"
)

// @author: xftan
// @date: 2021/12/14 15:06
// @description: test database init
func TestPrepareConnection(_ *testing.T) {
	viper.Set("taosConfigDir", "/etc/taos/")
	config.Init()
	PrepareConnection()
}
