package config

type MetricsConfig struct {
	Cluster  string   `toml:"cluster"`
	Prefix   string   `toml:"prefix"`
	Database Database `toml:"database"`
	Tables   []string `toml:"tables"`
}

type TaosAdapter struct {
	Address []string `toml:"address"`
}

type Metric struct {
	Alias  string            `toml:"alias"`
	Help   string            `toml:"help"`
	Unit   string            `toml:"unit"`
	Type   string            `toml:"type"`
	Labels map[string]string `toml:"labels"`
}

type Environment struct {
	InCGroup bool `toml:"incgroup"`
}

type Database struct {
	Name    string                 `toml:"name"`
	Options map[string]interface{} `toml:"options"`
}
