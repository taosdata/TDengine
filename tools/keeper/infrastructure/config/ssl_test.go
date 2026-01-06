package config

import (
	"os"
	"strings"
	"testing"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestInitSSL_ParseConfigFile(t *testing.T) {
	viper.Reset()
	pflag.CommandLine = pflag.NewFlagSet("test", pflag.ExitOnError)

	initSSL()

	configContent := `
[ssl]
enable = true
certFile = "test_cert.pem"
keyFile = "test_key.pem"
`
	viper.SetConfigType("toml")
	err := viper.ReadConfig(strings.NewReader(configContent))
	assert.NoError(t, err)

	var conf Config
	err = viper.Unmarshal(&conf)
	assert.NoError(t, err)

	assert.Equal(t, true, conf.SSL.Enable)
	assert.Equal(t, "test_cert.pem", conf.SSL.CertFile)
	assert.Equal(t, "test_key.pem", conf.SSL.KeyFile)
}

func TestInitSSL_WithEnvVars(t *testing.T) {
	os.Unsetenv("TAOS_KEEPER_SSL_ENABLE")
	os.Unsetenv("TAOS_KEEPER_SSL_CERT_FILE")
	os.Unsetenv("TAOS_KEEPER_SSL_KEY_FILE")

	viper.Reset()
	pflag.CommandLine = pflag.NewFlagSet("test", pflag.ExitOnError)

	initSSL()

	assert.Equal(t, false, viper.GetBool("ssl.enable"))
	assert.Equal(t, "", viper.GetString("ssl.certFile"))
	assert.Equal(t, "", viper.GetString("ssl.keyFile"))

	os.Setenv("TAOS_KEEPER_SSL_ENABLE", "true")
	os.Setenv("TAOS_KEEPER_SSL_CERT_FILE", "/tmp/test_cert.pem")
	os.Setenv("TAOS_KEEPER_SSL_KEY_FILE", "/tmp/test_key.pem")

	defer func() {
		os.Unsetenv("TAOS_KEEPER_SSL_ENABLE")
		os.Unsetenv("TAOS_KEEPER_SSL_CERT_FILE")
		os.Unsetenv("TAOS_KEEPER_SSL_KEY_FILE")
	}()

	viper.Reset()
	pflag.CommandLine = pflag.NewFlagSet("test", pflag.ExitOnError)

	initSSL()

	assert.Equal(t, true, viper.GetBool("ssl.enable"))
	assert.Equal(t, "/tmp/test_cert.pem", viper.GetString("ssl.certFile"))
	assert.Equal(t, "/tmp/test_key.pem", viper.GetString("ssl.keyFile"))
}
