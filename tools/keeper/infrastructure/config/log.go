package config

import (
	"time"

	"github.com/spf13/viper"
)

type Log struct {
	Path          string
	RotationCount uint
	RotationTime  time.Duration
	RotationSize  uint
}

func (l *Log) SetValue() {
	l.Path = viper.GetString("log.path")
	l.RotationCount = viper.GetUint("log.rotationCount")
	l.RotationTime = viper.GetDuration("log.rotationTime")
	l.RotationSize = viper.GetUint("log.rotationSize")
}
