package config

import (
	"time"

	"github.com/spf13/viper"
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
