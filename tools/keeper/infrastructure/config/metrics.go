package config

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type Metrics struct {
	Cluster  string   `toml:"cluster"`
	Prefix   string   `toml:"prefix"`
	Database Database `toml:"database"`
	Tables   []string `toml:"tables"`
}

type Database struct {
	Name    string                 `toml:"name"`
	Options map[string]interface{} `toml:"options"`
}

func initMetrics() {
	viper.SetDefault("metrics.prefix", "")
	_ = viper.BindEnv("metrics.prefix", "TAOS_KEEPER_METRICS_PREFIX")
	pflag.String("metrics.prefix", "", `prefix in metrics names. Env "TAOS_KEEPER_METRICS_PREFIX"`)

	viper.SetDefault("metrics.database.name", "log")
	_ = viper.BindEnv("metrics.database.name", "TAOS_KEEPER_METRICS_DATABASE")
	pflag.String("metrics.database.name", "log", `database for storing metrics data. Env "TAOS_KEEPER_METRICS_DATABASE"`)

	viper.SetDefault("metrics.database.options.vgroups", 1)
	_ = viper.BindEnv("metrics.database.options.vgroups", "TAOS_KEEPER_METRICS_VGROUPS")
	pflag.Int("metrics.database.options.vgroups", 1, `database option vgroups for audit database. Env "TAOS_KEEPER_METRICS_VGROUPS"`)

	viper.SetDefault("metrics.database.options.buffer", 64)
	_ = viper.BindEnv("metrics.database.options.buffer", "TAOS_KEEPER_METRICS_BUFFER")
	pflag.Int("metrics.database.options.buffer", 64, `database option buffer for audit database. Env "TAOS_KEEPER_METRICS_BUFFER"`)

	viper.SetDefault("metrics.database.options.keep", 90)
	_ = viper.BindEnv("metrics.database.options.keep", "TAOS_KEEPER_METRICS_KEEP")
	pflag.Int("metrics.database.options.keep", 90, `database option buffer for audit database. Env "TAOS_KEEPER_METRICS_KEEP"`)

	viper.SetDefault("metrics.database.options.cachemodel", "both")
	_ = viper.BindEnv("metrics.database.options.cachemodel", "TAOS_KEEPER_METRICS_CACHEMODEL")
	pflag.String("metrics.database.options.cachemodel", "both", `database option cachemodel for audit database. Env "TAOS_KEEPER_METRICS_CACHEMODEL"`)

	viper.SetDefault("metrics.tables", []string{})
	_ = viper.BindEnv("metrics.tables", "TAOS_KEEPER_METRICS_TABLES")
	pflag.StringArray("metrics.tables", []string{}, `export some tables that are not super table, multiple values split with white space. Env "TAOS_KEEPER_METRICS_TABLES"`)
}
