package config

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type Environment struct {
	InCGroup bool `toml:"incgroup"`
}

func initEnvironment() {
	viper.SetDefault("environment.incgroup", false)
	_ = viper.BindEnv("environment.incgroup", "TAOS_KEEPER_ENVIRONMENT_INCGROUP")
	pflag.Bool("environment.incgroup", false, `whether running in cgroup. Env "TAOS_KEEPER_ENVIRONMENT_INCGROUP"`)
}
