package config

type AuditConfig struct {
	Enable   bool     `toml:"enable"`
	Database Database `toml:"database"`
}
