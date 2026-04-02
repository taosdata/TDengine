package statsd

import (
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/taosdata/taosadapter/v3/driver/common"
)

type Config struct {
	Enable                 bool
	Port                   int
	DB                     string
	User                   string
	Password               string
	Worker                 int
	GatherInterval         time.Duration
	Protocol               string
	MaxTCPConnections      int
	TCPKeepAlive           bool
	AllowedPendingMessages int
	DeleteCounters         bool
	DeleteGauges           bool
	DeleteSets             bool
	DeleteTimings          bool
	TTL                    int
	Token                  string
}

func (c *Config) setValue() {
	c.Enable = viper.GetBool("statsd.enable")
	c.Port = viper.GetInt("statsd.port")
	c.DB = viper.GetString("statsd.db")
	c.User = viper.GetString("statsd.user")
	c.Password = viper.GetString("statsd.password")
	c.Worker = viper.GetInt("statsd.worker")
	c.GatherInterval = viper.GetDuration("statsd.gatherInterval")
	c.Protocol = viper.GetString("statsd.protocol")
	c.MaxTCPConnections = viper.GetInt("statsd.maxTCPConnections")
	c.TCPKeepAlive = viper.GetBool("statsd.tcpKeepAlive")
	c.AllowedPendingMessages = viper.GetInt("statsd.allowPendingMessages")
	c.DeleteCounters = viper.GetBool("statsd.deleteCounters")
	c.DeleteGauges = viper.GetBool("statsd.deleteGauges")
	c.DeleteSets = viper.GetBool("statsd.deleteSets")
	c.DeleteTimings = viper.GetBool("statsd.deleteTimings")
	c.TTL = viper.GetInt("statsd.ttl")
	c.Token = viper.GetString("statsd.token")
}

func init() {
	_ = viper.BindEnv("statsd.enable", "TAOS_ADAPTER_STATSD_ENABLE")
	pflag.Bool("statsd.enable", false, `enable statsd. Env "TAOS_ADAPTER_STATSD_ENABLE"`)
	viper.SetDefault("statsd.enable", false)

	_ = viper.BindEnv("statsd.port", "TAOS_ADAPTER_STATSD_PORT")
	pflag.Int("statsd.port", 6044, `statsd server port. Env "TAOS_ADAPTER_STATSD_PORT"`)
	viper.SetDefault("statsd.port", 6044)

	_ = viper.BindEnv("statsd.db", "TAOS_ADAPTER_STATSD_DB")
	pflag.String("statsd.db", "statsd", `statsd db name. Env "TAOS_ADAPTER_STATSD_DB"`)
	viper.SetDefault("statsd.db", "statsd")

	_ = viper.BindEnv("statsd.user", "TAOS_ADAPTER_STATSD_USER")
	pflag.String("statsd.user", common.DefaultUser, `statsd user. Env "TAOS_ADAPTER_STATSD_USER"`)
	viper.SetDefault("statsd.user", common.DefaultUser)

	_ = viper.BindEnv("statsd.password", "TAOS_ADAPTER_STATSD_PASSWORD")
	pflag.String("statsd.password", common.DefaultPassword, `statsd password. Env "TAOS_ADAPTER_STATSD_PASSWORD"`)
	viper.SetDefault("statsd.password", common.DefaultPassword)

	_ = viper.BindEnv("statsd.worker", "TAOS_ADAPTER_STATSD_WORKER")
	pflag.Int("statsd.worker", 10, `statsd write worker. Env "TAOS_ADAPTER_STATSD_WORKER"`)
	viper.SetDefault("statsd.worker", 10)

	_ = viper.BindEnv("statsd.gatherInterval", "TAOS_ADAPTER_STATSD_GATHER_INTERVAL")
	pflag.Duration("statsd.gatherInterval", time.Second*5, `statsd gather interval. Env "TAOS_ADAPTER_STATSD_GATHER_INTERVAL"`)
	viper.SetDefault("statsd.gatherInterval", "5s")

	_ = viper.BindEnv("statsd.protocol", "TAOS_ADAPTER_STATSD_PROTOCOL")
	pflag.String("statsd.protocol", "udp4", `statsd protocol [tcp or udp]. Env "TAOS_ADAPTER_STATSD_PROTOCOL"`)
	viper.SetDefault("statsd.protocol", "udp4")

	_ = viper.BindEnv("statsd.maxTCPConnections", "TAOS_ADAPTER_STATSD_MAX_TCP_CONNECTIONS")
	pflag.Int("statsd.maxTCPConnections", 250, `statsd max tcp connections. Env "TAOS_ADAPTER_STATSD_MAX_TCP_CONNECTIONS"`)
	viper.SetDefault("statsd.maxTCPConnections", 250)

	_ = viper.BindEnv("statsd.tcpKeepAlive", "TAOS_ADAPTER_STATSD_TCP_KEEP_ALIVE")
	pflag.Bool("statsd.tcpKeepAlive", false, `enable tcp keep alive. Env "TAOS_ADAPTER_STATSD_TCP_KEEP_ALIVE"`)
	viper.SetDefault("statsd.tcpKeepAlive", false)

	_ = viper.BindEnv("statsd.allowPendingMessages", "TAOS_ADAPTER_STATSD_ALLOW_PENDING_MESSAGES")
	pflag.Int("statsd.allowPendingMessages", 50000, `statsd allow pending messages. Env "TAOS_ADAPTER_STATSD_ALLOW_PENDING_MESSAGES"`)
	viper.SetDefault("statsd.allowPendingMessages", 50000)

	_ = viper.BindEnv("statsd.deleteCounters", "TAOS_ADAPTER_STATSD_DELETE_COUNTERS")
	pflag.Bool("statsd.deleteCounters", true, `statsd delete counter cache after gather. Env "TAOS_ADAPTER_STATSD_DELETE_COUNTERS"`)
	viper.SetDefault("statsd.deleteCounters", true)

	_ = viper.BindEnv("statsd.deleteGauges", "TAOS_ADAPTER_STATSD_DELETE_GAUGES")
	pflag.Bool("statsd.deleteGauges", true, `statsd delete gauge cache after gather. Env "TAOS_ADAPTER_STATSD_DELETE_GAUGES"`)
	viper.SetDefault("statsd.deleteGauges", true)

	_ = viper.BindEnv("statsd.deleteSets", "TAOS_ADAPTER_STATSD_DELETE_SETS")
	pflag.Bool("statsd.deleteSets", true, `statsd delete set cache after gather. Env "TAOS_ADAPTER_STATSD_DELETE_SETS"`)
	viper.SetDefault("statsd.deleteSets", true)

	_ = viper.BindEnv("statsd.deleteTimings", "TAOS_ADAPTER_STATSD_DELETE_TIMINGS")
	pflag.Bool("statsd.deleteTimings", true, `statsd delete timing cache after gather. Env "TAOS_ADAPTER_STATSD_DELETE_TIMINGS"`)
	viper.SetDefault("statsd.deleteTimings", true)

	_ = viper.BindEnv("statsd.ttl", "TAOS_ADAPTER_STATSD_TTL")
	pflag.Int("statsd.ttl", 0, `statsd data ttl. Env "TAOS_ADAPTER_STATSD_TTL"`)
	viper.SetDefault("statsd.ttl", 0)

	_ = viper.BindEnv("statsd.token", "TAOS_ADAPTER_STATSD_TOKEN")
	pflag.String("statsd.token", "", `statsd auth token. Env "TAOS_ADAPTER_STATSD_TOKEN"`)
	viper.SetDefault("statsd.token", "")
}
