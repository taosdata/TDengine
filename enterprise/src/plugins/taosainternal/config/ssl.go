package config

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type SSl struct {
	Enable   bool
	CertFile string
	KeyFile  string
}

func initSSL() {
	viper.SetDefault("ssl.enable", false)
	_ = viper.BindEnv("ssl.enable", "TAOS_ADAPTER_SSL_ENABLE")
	pflag.Bool("ssl.enable", false, `enable ssl. Env "TAOS_ADAPTER_SSL_ENABLE"`)

	viper.SetDefault("ssl.certFile", "")
	_ = viper.BindEnv("ssl.certFile", "TAOS_ADAPTER_SSL_CERT_FILE")
	pflag.String("ssl.certFile", "", `ssl cert file path. Env "TAOS_ADAPTER_SSL_CERT_FILE"`)

	viper.SetDefault("ssl.keyFile", "")
	_ = viper.BindEnv("ssl.keyFile", "TAOS_ADAPTER_SSL_KEY_FILE")
	pflag.String("ssl.keyFile", "", `ssl key file path. Env "TAOS_ADAPTER_SSL_KEY_FILE"`)
}

func (s *SSl) SetValue() {
	s.Enable = viper.GetBool("ssl.enable")
	s.CertFile = viper.GetString("ssl.certFile")
	s.KeyFile = viper.GetString("ssl.keyFile")
}
