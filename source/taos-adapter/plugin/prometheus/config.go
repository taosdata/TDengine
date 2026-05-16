package prometheus

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type Config struct {
	Enable bool
}

func (c *Config) setValue() {
	c.Enable = viper.GetBool("prometheus.enable")
}

func init() {
	_ = viper.BindEnv("prometheus.enable", "TAOS_ADAPTER_PROMETHEUS_ENABLE")
	pflag.Bool("prometheus.enable", true, `enable prometheus. Env "TAOS_ADAPTER_PROMETHEUS_ENABLE"`)
	viper.SetDefault("prometheus.enable", true)
}
