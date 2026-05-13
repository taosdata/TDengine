package watch

import (
	"github.com/fsnotify/fsnotify"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/log"
)

func OnConfigChange(file string, _ fsnotify.Op, logger *logrus.Entry) {
	logger.Info("config file changed, reload config")
	v := viper.New()
	v.SetConfigType("toml")
	v.SetConfigFile(file)
	err := v.ReadInConfig()
	if err != nil {
		logger.Errorf("read config failed: %s", err)
		return
	}
	err = config.Conf.Reject.SetValue(v)
	if err != nil {
		logger.Errorf("reject set value failed: %s", err)
		return
	}
	if v.IsSet("log.level") {
		logLevel := v.GetString("log.level")
		logger.Debugf("set log level: %s", logLevel)
		err = log.SetLevel(logLevel)
		if err != nil {
			logger.Errorf("set log level failed: %s, level: %s", err, logLevel)
			return
		}
	}
	logger.Infof("reload config success")
}
