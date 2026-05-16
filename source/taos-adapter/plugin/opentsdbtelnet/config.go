package opentsdbtelnet

import (
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/taosdata/taosadapter/v3/driver/common"
)

type Config struct {
	Enable            bool
	PortList          []int
	TCPKeepAlive      bool
	MaxTCPConnections int
	DBList            []string
	User              string
	Password          string
	BatchSize         int
	FlushInterval     time.Duration
	TTL               int
	Token             string
}

func (c *Config) setValue() {
	c.Enable = viper.GetBool("opentsdb_telnet.enable")
	c.PortList = viper.GetIntSlice("opentsdb_telnet.ports")
	c.MaxTCPConnections = viper.GetInt("opentsdb_telnet.maxTCPConnections")
	c.TCPKeepAlive = viper.GetBool("opentsdb_telnet.tcpKeepAlive")
	c.DBList = viper.GetStringSlice("opentsdb_telnet.dbs")
	c.User = viper.GetString("opentsdb_telnet.user")
	c.Password = viper.GetString("opentsdb_telnet.password")
	c.BatchSize = viper.GetInt("opentsdb_telnet.batchSize")
	c.FlushInterval = viper.GetDuration("opentsdb_telnet.flushInterval")
	if c.BatchSize < 0 {
		c.BatchSize = 1
	}
	c.TTL = viper.GetInt("opentsdb_telnet.ttl")
	c.Token = viper.GetString("opentsdb_telnet.token")
}
func init() {
	_ = viper.BindEnv("opentsdb_telnet.enable", "TAOS_ADAPTER_OPENTSDB_TELNET_ENABLE")
	pflag.Bool("opentsdb_telnet.enable", false, `enable opentsdb telnet,warning: without auth info(default false). Env "TAOS_ADAPTER_OPENTSDB_TELNET_ENABLE"`)
	viper.SetDefault("opentsdb_telnet.enable", false)

	_ = viper.BindEnv("opentsdb_telnet.ports", "TAOS_ADAPTER_OPENTSDB_TELNET_PORTS")
	pflag.IntSlice("opentsdb_telnet.ports", []int{6046, 6047, 6048, 6049}, `opentsdb telnet tcp port. Env "TAOS_ADAPTER_OPENTSDB_TELNET_PORTS"`)
	viper.SetDefault("opentsdb_telnet.ports", []int{6046, 6047, 6048, 6049})

	_ = viper.BindEnv("opentsdb_telnet.maxTCPConnections", "TAOS_ADAPTER_OPENTSDB_TELNET_MAX_TCP_CONNECTIONS")
	pflag.Int("opentsdb_telnet.maxTCPConnections", 250, `max tcp connections. Env "TAOS_ADAPTER_OPENTSDB_TELNET_MAX_TCP_CONNECTIONS"`)
	viper.SetDefault("opentsdb_telnet.maxTCPConnections", 250)

	_ = viper.BindEnv("opentsdb_telnet.tcpKeepAlive", "TAOS_ADAPTER_OPENTSDB_TELNET_TCP_KEEP_ALIVE")
	pflag.Bool("opentsdb_telnet.tcpKeepAlive", false, `enable tcp keep alive. Env "TAOS_ADAPTER_OPENTSDB_TELNET_TCP_KEEP_ALIVE"`)
	viper.SetDefault("opentsdb_telnet.tcpKeepAlive", false)

	_ = viper.BindEnv("opentsdb_telnet.dbs", "TAOS_ADAPTER_OPENTSDB_TELNET_DBS")
	pflag.StringSlice("opentsdb_telnet.dbs", []string{"opentsdb_telnet", "collectd_tsdb", "icinga2_tsdb", "tcollector_tsdb"}, `opentsdb_telnet db names. Env "TAOS_ADAPTER_OPENTSDB_TELNET_DBS"`)
	viper.SetDefault("opentsdb_telnet.dbs", []string{"opentsdb_telnet", "collectd_tsdb", "icinga2_tsdb", "tcollector_tsdb"})

	_ = viper.BindEnv("opentsdb_telnet.user", "TAOS_ADAPTER_OPENTSDB_TELNET_USER")
	pflag.String("opentsdb_telnet.user", common.DefaultUser, `opentsdb_telnet user. Env "TAOS_ADAPTER_OPENTSDB_TELNET_USER"`)
	viper.SetDefault("opentsdb_telnet.user", common.DefaultUser)

	_ = viper.BindEnv("opentsdb_telnet.password", "TAOS_ADAPTER_OPENTSDB_TELNET_PASSWORD")
	pflag.String("opentsdb_telnet.password", common.DefaultPassword, `opentsdb_telnet password. Env "TAOS_ADAPTER_OPENTSDB_TELNET_PASSWORD"`)
	viper.SetDefault("opentsdb_telnet.password", common.DefaultPassword)

	_ = viper.BindEnv("opentsdb_telnet.batchSize", "TAOS_ADAPTER_OPENTSDB_TELNET_BATCH_SIZE")
	pflag.Int("opentsdb_telnet.batchSize", 1, `opentsdb_telnet batch size. Env "TAOS_ADAPTER_OPENTSDB_TELNET_BATCH_SIZE"`)
	viper.SetDefault("opentsdb_telnet.batchSize", 1)

	_ = viper.BindEnv("opentsdb_telnet.flushInterval", "TAOS_ADAPTER_OPENTSDB_TELNET_FLUSH_INTERVAL")
	pflag.Duration("opentsdb_telnet.flushInterval", time.Duration(0), `opentsdb_telnet flush interval (0s means not valid) . Env "TAOS_ADAPTER_OPENTSDB_TELNET_FLUSH_INTERVAL"`)
	viper.SetDefault("opentsdb_telnet.flushInterval", time.Duration(0))

	_ = viper.BindEnv("opentsdb_telnet.ttl", "TAOS_ADAPTER_OPENTSDB_TELNET_TTL")
	pflag.Int("opentsdb_telnet.ttl", 0, `opentsdb_telnet data ttl. Env "TAOS_ADAPTER_OPENTSDB_TELNET_TTL"`)
	viper.SetDefault("opentsdb_telnet.ttl", 0)

	_ = viper.BindEnv("opentsdb_telnet.token", "TAOS_ADAPTER_OPENTSDB_TELNET_TOKEN")
	pflag.String("opentsdb_telnet.token", "", `opentsdb_telnet token. Env "TAOS_ADAPTER_OPENTSDB_TELNET_TOKEN"`)
	viper.SetDefault("opentsdb_telnet.token", "")
}
