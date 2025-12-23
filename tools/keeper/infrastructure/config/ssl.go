package config

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type SSL struct {
	Enable   bool
	CertFile string
	KeyFile  string
}

func initSSL() {
	viper.SetDefault("ssl.enable", false)
	_ = viper.BindEnv("ssl.enable", "TAOS_KEEPER_SSL_ENABLE")
	pflag.Bool("ssl.enable", false, `enable ssl. Env "TAOS_KEEPER_SSL_ENABLE"`)

	viper.SetDefault("ssl.certFile", "")
	_ = viper.BindEnv("ssl.certFile", "TAOS_KEEPER_SSL_CERT_FILE")
	pflag.String("ssl.certFile", "", `ssl cert file path. Env "TAOS_KEEPER_SSL_CERT_FILE"`)

	viper.SetDefault("ssl.keyFile", "")
	_ = viper.BindEnv("ssl.keyFile", "TAOS_KEEPER_SSL_KEY_FILE")
	pflag.String("ssl.keyFile", "", `ssl key file path. Env "TAOS_KEEPER_SSL_KEY_FILE"`)
}
