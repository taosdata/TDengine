package opentsdb

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type Config struct {
	Enable bool
}

func (c *Config) setValue() {
	c.Enable = viper.GetBool("opentsdb.enable")
}
func init() {
	_ = viper.BindEnv("opentsdb.enable", "TAOS_ADAPTER_OPENTSDB_ENABLE")
	pflag.Bool("opentsdb.enable", true, `enable opentsdb. Env "TAOS_ADAPTER_OPENTSDB_ENABLE"`)
	viper.SetDefault("opentsdb.enable", true)
}
