package inputjson

import (
	"os"
	"testing"

	"github.com/spf13/viper"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db"
	"github.com/taosdata/taosadapter/v3/log"
)

func TestMain(m *testing.M) {
	viper.Set("uploadKeeper.enable", true)
	config.Init()
	_ = log.SetLevel("info")
	db.PrepareConnection()
	os.Exit(m.Run())
}
