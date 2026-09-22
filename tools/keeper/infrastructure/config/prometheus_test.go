package config

import (
	"os"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

// Verify that camelCase TOML keys under [prometheus] actually populate the
// struct fields through viper.Unmarshal (mapstructure), i.e. the toml tags
// are sufficient and explicit values are not silently dropped.
func TestPrometheusUnmarshalCase(t *testing.T) {
	f, err := os.CreateTemp("", "keeper-*.toml")
	assert.NoError(t, err)
	defer os.Remove(f.Name())
	_, err = f.WriteString("[prometheus]\nincludeTables = [\"taos_slow_sql_detail\"]\ncacheTTL = 123\n")
	assert.NoError(t, err)
	assert.NoError(t, f.Close())

	v := viper.New()
	v.SetConfigFile(f.Name())
	v.SetDefault("prometheus.includeTables", []string{})
	v.SetDefault("prometheus.cacheTTL", 300)
	assert.NoError(t, v.ReadInConfig())

	var c Config
	assert.NoError(t, v.Unmarshal(&c))
	t.Logf("prometheus section decoded as: %+v", c.Prometheus)
	assert.Equal(t, []string{"taos_slow_sql_detail"}, c.Prometheus.IncludeTables)
	assert.Equal(t, 123, c.Prometheus.CacheTTL)
}
