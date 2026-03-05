package config

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Prometheus configuration for v2 memory cache mode
type Prometheus struct {
	IncludeTables []string `toml:"includeTables"` // v2 additional tables to cache (restore from default exclusion list)
	CacheTTL      int      `toml:"cacheTTL"`      // v2 memory cache TTL in seconds (default: 300, minimum: 60)
}

func initPrometheus() {
	// v2 memory cache config: additional tables to cache (restore from default exclusion list)
	viper.SetDefault("prometheus.includeTables", []string{})
	_ = viper.BindEnv("prometheus.includeTables", "TAOS_KEEPER_INCLUDE_TABLES")
	pflag.StringArray("prometheus.includeTables", []string{}, `additional tables to cache in v2 memory mode (e.g., taosd_write_metrics). Env "TAOS_KEEPER_INCLUDE_TABLES"`)

	// v2 memory cache TTL (in seconds)
	viper.SetDefault("prometheus.cacheTTL", 300)
	_ = viper.BindEnv("prometheus.cacheTTL", "TAOS_KEEPER_CACHE_TTL")
	pflag.Int("prometheus.cacheTTL", 300, `memory cache TTL in v2 mode (in seconds, default: 300, minimum: 60). Env "TAOS_KEEPER_CACHE_TTL"`)
}
