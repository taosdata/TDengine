package config

import (
	"github.com/BurntSushi/toml"
)

type MQTT struct {
	Address      string `toml:"address"`       // mqtt address eg: tcp://127.0.0.1:1883
	Version      string `toml:"version"`       // mqtt protocol version
	ClientID     string `toml:"client_id"`     // mqtt client id. If not set will use uuid
	Username     string `toml:"username"`      // mqtt username
	Password     string `toml:"password"`      // mqtt password
	KeepAlive    int64  `toml:"keep_alive"`    // mqtt option keepalive
	CleanSession bool   `toml:"clean_session"` // mqtt option clean session
	CA           string `toml:"ca"`            // mqtt ssl ca
	Cert         string `toml:"cert"`          // mqtt ssl client cert
	CertKey      string `toml:"cert_key"`      // mqtt ssl client cert key
}

type Dump struct {
	Enable bool   `toml:"enable"`
	Path   string `toml:"path"`
	Keep   int64  `toml:"keep"`
}

type Batch struct {
	/// Timeout in milliseconds
	BatchTimeout   int `toml:"timeout"`
	BatchSize      int `toml:"size"`
	Worker         int `toml:"worker"`
	ReceiveChanLen int `toml:"receive_chan_len"` // worker receive chan len
}

type Config struct {
	LogLevel string         `toml:"log_level"`
	Remote   string         `toml:"remote"`
	MQTT     *MQTT          `toml:"mqtt"`
	Topics   map[string]int `toml:"topics"` // topic:QOS
	Dump     *Dump          `toml:"dump"`
	Batch    *Batch         `toml:"batch"`
}

func ParseConfig(path string) (*Config, error) {
	var config Config
	_, err := toml.DecodeFile(path, &config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}
