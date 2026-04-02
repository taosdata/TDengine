package wstool

import (
	"os"
	"testing"

	"github.com/spf13/viper"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/log"
)

func TestMain(m *testing.M) {
	viper.Set("logLevel", "trace")
	viper.Set("uploadKeeper.enable", false)
	config.Init()
	log.ConfigLog()
	os.Exit(m.Run())
}
