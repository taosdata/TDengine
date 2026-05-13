package config

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type Audit struct {
	Enable   bool     `toml:"enable"`
	Database Database `toml:"database"`
}

func initAudit() {
	viper.SetDefault("audit.enable", "true")
	_ = viper.BindEnv("audit.enable", "TAOS_KEEPER_AUDIT_ENABLE")
	pflag.String("audit.enable", "true", `database for enable audit data. Env "TAOS_KEEPER_AUDIT_ENABLE"`)

	viper.SetDefault("audit.database.name", "audit")
	_ = viper.BindEnv("audit.database.name", "TAOS_KEEPER_AUDIT_DATABASE")
	pflag.String("audit.database.name", "audit", `database for storing audit data. Env "TAOS_KEEPER_AUDIT_DATABASE"`)

	viper.SetDefault("audit.database.options.vgroups", 1)
	_ = viper.BindEnv("audit.database.options.vgroups", "TAOS_KEEPER_AUDIT_VGROUPS")
	pflag.Int("audit.database.options.vgroups", 1, `database option vgroups for audit database. Env "TAOS_KEEPER_AUDIT_VGROUPS"`)

	viper.SetDefault("audit.database.options.buffer", 16)
	_ = viper.BindEnv("audit.database.options.buffer", "TAOS_KEEPER_AUDIT_BUFFER")
	pflag.Int("audit.database.options.buffer", 16, `database option buffer for audit database. Env "TAOS_KEEPER_AUDIT_BUFFER"`)

	viper.SetDefault("audit.database.options.cachemodel", "both")
	_ = viper.BindEnv("audit.database.options.cachemodel", "TAOS_KEEPER_AUDIT_CACHEMODEL")
	pflag.String("audit.database.options.cachemodel", "both", `database option cachemodel for audit database. Env "TAOS_KEEPER_AUDIT_CACHEMODEL"`)
}
