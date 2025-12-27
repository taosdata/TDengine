package config

import (
	"fmt"
	"runtime"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/taosdata/taoskeeper/version"
)

type Log struct {
	Level            string
	Path             string
	RotationCount    uint
	RotationTime     time.Duration
	RotationSize     uint
	KeepDays         uint
	Compress         bool
	ReservedDiskSize uint
}

func (l *Log) SetValue() {
	l.Level = viper.GetString("log.level")
	l.Path = viper.GetString("log.path")
	l.RotationCount = viper.GetUint("log.rotationCount")
	l.RotationTime = viper.GetDuration("log.rotationTime")
	l.RotationSize = viper.GetSizeInBytes("log.rotationSize")
	l.KeepDays = viper.GetUint("log.keepDays")
	l.Compress = viper.GetBool("log.compress")
	l.ReservedDiskSize = viper.GetSizeInBytes("log.reservedDiskSize")
}

func initLog() {
	switch runtime.GOOS {
	case "windows":
		viper.SetDefault("log.path", fmt.Sprintf("C:\\%s\\log", version.CUS_NAME))
		_ = viper.BindEnv("log.path", "TAOS_KEEPER_LOG_PATH")
		pflag.String("log.path", fmt.Sprintf("C:\\%s\\log", version.CUS_NAME), `log path. Env "TAOS_KEEPER_LOG_PATH"`)
	default:
		viper.SetDefault("log.path", fmt.Sprintf("/var/log/%s", version.CUS_PROMPT))
		_ = viper.BindEnv("log.path", "TAOS_KEEPER_LOG_PATH")
		pflag.String("log.path", fmt.Sprintf("/var/log/%s", version.CUS_PROMPT), `log path. Env "TAOS_KEEPER_LOG_PATH"`)
	}

	_ = viper.BindEnv("log.level", "TAOS_KEEPER_LOG_LEVEL")
	pflag.String("log.level", "info", `log level (trace debug info warning error). Env "TAOS_KEEPER_LOG_LEVEL"`)

	viper.SetDefault("log.rotationCount", 5)
	_ = viper.BindEnv("log.rotationCount", "TAOS_KEEPER_LOG_ROTATION_COUNT")
	pflag.Uint("log.rotationCount", 5, `log rotation count. Env "TAOS_KEEPER_LOG_ROTATION_COUNT"`)

	viper.SetDefault("log.keepDays", 30)
	_ = viper.BindEnv("log.keepDays", "TAOS_KEEPER_LOG_KEEP_DAYS")
	pflag.Uint("log.keepDays", 30, `log retention days, must be a positive integer. Env "TAOS_KEEPER_LOG_KEEP_DAYS"`)

	viper.SetDefault("log.rotationTime", time.Hour*24)
	_ = viper.BindEnv("log.rotationTime", "TAOS_KEEPER_LOG_ROTATION_TIME")
	pflag.Duration("log.rotationTime", time.Hour*24, `deprecated: log rotation time always 24 hours. Env "TAOS_KEEPER_LOG_ROTATION_TIME"`)

	viper.SetDefault("log.rotationSize", "1GB")
	_ = viper.BindEnv("log.rotationSize", "TAOS_KEEPER_LOG_ROTATION_SIZE")
	pflag.String("log.rotationSize", "1GB", `log rotation size(KB MB GB), must be a positive integer. Env "TAOS_KEEPER_LOG_ROTATION_SIZE"`)

	viper.SetDefault("log.compress", false)
	_ = viper.BindEnv("log.compress", "TAOS_KEEPER_LOG_COMPRESS")
	pflag.Bool("log.compress", false, `whether to compress old log. Env "TAOS_KEEPER_LOG_COMPRESS"`)

	viper.SetDefault("log.reservedDiskSize", "1GB")
	_ = viper.BindEnv("log.reservedDiskSize", "TAOS_KEEPER_LOG_RESERVED_DISK_SIZE")
	pflag.String("log.reservedDiskSize", "1GB", `reserved disk size for log dir (KB MB GB), must be a positive integer. Env "TAOS_KEEPER_LOG_RESERVED_DISK_SIZE"`)
}
