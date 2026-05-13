package config

import (
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type UploadKeeper struct {
	Enable        bool
	Url           string
	Interval      time.Duration
	Timeout       time.Duration
	RetryTimes    uint
	RetryInterval time.Duration
}

func initUploadKeeper() {
	viper.SetDefault("uploadKeeper.enable", true)
	_ = viper.BindEnv("uploadKeeper.enable", "TAOS_ADAPTER_UPLOAD_KEEPER_ENABLE")
	pflag.Bool("uploadKeeper.enable", true, `Whether to enable sending metrics to keeper. Env "TAOS_ADAPTER_UPLOAD_KEEPER_ENABLE"`)

	viper.SetDefault("uploadKeeper.url", "http://127.0.0.1:6043/adapter_report")
	_ = viper.BindEnv("uploadKeeper.url", "TAOS_ADAPTER_UPLOAD_KEEPER_URL")
	pflag.String("uploadKeeper.url", "http://127.0.0.1:6043/adapter_report", `Keeper url. Env "TAOS_ADAPTER_UPLOAD_KEEPER_URL"`)

	viper.SetDefault("uploadKeeper.interval", 15*time.Second)
	_ = viper.BindEnv("uploadKeeper.interval", "TAOS_ADAPTER_UPLOAD_KEEPER_INTERVAL")
	pflag.Duration("uploadKeeper.interval", 15*time.Second, `send to Keeper interval. Env "TAOS_ADAPTER_UPLOAD_KEEPER_INTERVAL"`)

	viper.SetDefault("uploadKeeper.timeout", 5*time.Second)
	_ = viper.BindEnv("uploadKeeper.timeout", "TAOS_ADAPTER_UPLOAD_KEEPER_TIMEOUT")
	pflag.Duration("uploadKeeper.timeout", 5*time.Second, `send to Keeper timeout. Env "TAOS_ADAPTER_UPLOAD_KEEPER_TIMEOUT"`)

	viper.SetDefault("uploadKeeper.retryTimes", 3)
	_ = viper.BindEnv("uploadKeeper.retryTimes", "TAOS_ADAPTER_UPLOAD_KEEPER_RETRY_TIMES")
	pflag.Uint("uploadKeeper.retryTimes", 3, `retry times. Env "TAOS_ADAPTER_UPLOAD_KEEPER_RETRY_TIMES"`)

	viper.SetDefault("uploadKeeper.retryInterval", 5*time.Second)
	_ = viper.BindEnv("uploadKeeper.retryInterval", "TAOS_ADAPTER_UPLOAD_KEEPER_RETRY_INTERVAL")
	pflag.Duration("uploadKeeper.retryInterval", 5*time.Second, `retry interval. Env "TAOS_ADAPTER_UPLOAD_KEEPER_RETRY_INTERVAL"`)

}

func (u *UploadKeeper) setValue() {
	u.Enable = viper.GetBool("uploadKeeper.enable")
	u.Url = viper.GetString("uploadKeeper.url")
	u.Interval = viper.GetDuration("uploadKeeper.interval")
	u.Timeout = viper.GetDuration("uploadKeeper.timeout")
	u.RetryTimes = viper.GetUint("uploadKeeper.retryTimes")
	u.RetryInterval = viper.GetDuration("uploadKeeper.retryInterval")
}
