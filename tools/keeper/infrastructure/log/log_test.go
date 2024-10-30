package log

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taoskeeper/infrastructure/config"
	"testing"
	"time"
)

func TestConfigLog(t *testing.T) {
	config.InitConfig()
	config.Conf.LogLevel = "debug"
	ConfigLog()
	debug, _ := logrus.ParseLevel("debug")
	assert.Equal(t, logger.Level, debug)
	assert.Equal(t, true, IsDebug())
	fmt.Print(GetLogNow(true), GetLogDuration(true, time.Now()))
	Close(context.Background())
}
