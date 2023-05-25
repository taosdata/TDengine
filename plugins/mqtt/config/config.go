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

type Config struct {
	LogLevel string         `toml:"log_level"`
	Remote   string         `toml:"remote"`
	MQTT     *MQTT          `toml:"mqtt"`
	Topics   map[string]int `toml:"topics"` // topic:QOS
}

func ParseConfig(path string) (*Config, error) {
	var config Config
	_, err := toml.DecodeFile(path, &config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}
