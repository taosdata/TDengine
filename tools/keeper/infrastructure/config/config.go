package config

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"runtime"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/taosdata/go-utils/web"
	"github.com/taosdata/taoskeeper/util/pool"
	"github.com/taosdata/taoskeeper/version"
)

var Name = fmt.Sprintf("%skeeper", version.CUS_PROMPT)

type Config struct {
	Cors             web.CorsConfig  `toml:"cors"`
	Debug            bool            `toml:"debug"`
	Port             int             `toml:"port"`
	LogLevel         string          `toml:"loglevel"`
	GoPoolSize       int             `toml:"gopoolsize"`
	RotationInterval string          `toml:"RotationInterval"`
	TDengine         TDengineRestful `toml:"tdengine"`
	Metrics          MetricsConfig   `toml:"metrics"`
	Env              Environment     `toml:"environment"`
	Audit            AuditConfig     `toml:"audit"`
	Log              Log             `toml:"log"`

	Transfer string
	FromTime string
	Drop     string
}

type TDengineRestful struct {
	Host     string `toml:"host"`
	Port     int    `toml:"port"`
	Username string `toml:"username"`
	Password string `toml:"password"`
	Usessl   bool   `toml:"usessl"`
}

var Conf *Config

func InitConfig() *Config {
	viper.SetConfigType("toml")
	viper.SetConfigName(Name)
	viper.AddConfigPath("/etc/taos")

	var cp *string
	switch runtime.GOOS {
	case "windows":
		viper.AddConfigPath(fmt.Sprintf("C:\\%s\\cfg", version.CUS_NAME))
		cp = pflag.StringP("config", "c", "", fmt.Sprintf("config path default C:\\%s\\cfg\\%s.toml", version.CUS_NAME, Name))
	default:
		viper.AddConfigPath(fmt.Sprintf("/etc/%s", version.CUS_PROMPT))
		cp = pflag.StringP("config", "c", "", fmt.Sprintf("config path default /etc/%s/%s.toml", version.CUS_PROMPT, Name))
	}

	transfer := pflag.StringP("transfer", "", "", "run "+Name+" in command mode, only support old_taosd_metric. transfer old metrics data to new tables and exit")
	fromTime := pflag.StringP("fromTime", "", "2020-01-01T00:00:00+08:00", "parameter of transfer, example: 2020-01-01T00:00:00+08:00")
	drop := pflag.StringP("drop", "", "", "run "+Name+" in command mode, only support old_taosd_metric_stables. ")

	v := pflag.BoolP("version", "V", false, "Print the version and exit")
	help := pflag.BoolP("help", "h", false, "Print this help message and exit")

	pflag.Parse()

	if *help {
		fmt.Fprintf(os.Stderr, "Usage of %s v%s:\n", Name, version.Version)
		pflag.PrintDefaults()
		os.Exit(0)
	}

	if *v {
		fmt.Printf("%s version: %s\n", Name, version.Version)
		fmt.Printf("git: %s\n", version.Gitinfo)
		fmt.Printf("build: %s\n", version.BuildInfo)
		os.Exit(0)
	}

	if *cp != "" {
		viper.SetConfigFile(*cp)
	}

	viper.SetEnvPrefix(Name)
	err := viper.BindPFlags(pflag.CommandLine)
	if err != nil {
		panic(err)
	}
	viper.AutomaticEnv()

	gotoStep := false
ReadConfig:
	if err := viper.ReadInConfig(); err != nil {
		_, isConfigNotFoundError := err.(viper.ConfigFileNotFoundError)
		_, isPathError := err.(*fs.PathError)
		if isConfigNotFoundError || isPathError {
			fmt.Println("config file not found")

			if !gotoStep {
				fmt.Println("use keeper.toml instead")
				viper.SetConfigName("keeper")
				gotoStep = true
				goto ReadConfig
			}
		} else {
			panic(err)
		}
	}

	// if old format, change to new format
	if !viper.IsSet("metrics.database.name") {
		databaseName := viper.GetString("metrics.database")
		viper.Set("metrics.database.name", databaseName)
		viper.Set("metrics.database.options", viper.Get("metrics.databaseoptions"))
	}

	var conf Config
	if err = viper.Unmarshal(&conf); err != nil {
		panic(err)
	}

	if conf.Debug {
		j, _ := json.Marshal(conf)
		fmt.Println("config file:", string(j))
	}

	conf.Transfer = *transfer
	conf.FromTime = *fromTime
	conf.Drop = *drop

	conf.Cors.Init()
	pool.Init(conf.GoPoolSize)
	conf.Log.SetValue()
	Conf = &conf
	return &conf
}

func init() {
	viper.SetDefault("debug", false)
	_ = viper.BindEnv("debug", "TAOS_KEEPER_DEBUG")
	pflag.Bool("debug", false, `enable debug mode. Env "TAOS_KEEPER_DEBUG"`)

	viper.SetDefault("port", 6043)
	_ = viper.BindEnv("port", "TAOS_KEEPER_PORT")
	pflag.IntP("port", "P", 6043, `http port. Env "TAOS_KEEPER_PORT"`)

	viper.SetDefault("logLevel", "info")
	_ = viper.BindEnv("logLevel", "TAOS_KEEPER_LOG_LEVEL")
	pflag.String("logLevel", "info", `log level (panic fatal error warn warning info debug trace). Env "TAOS_KEEPER_LOG_LEVEL"`)

	viper.SetDefault("gopoolsize", 50000)
	_ = viper.BindEnv("gopoolsize", "TAOS_KEEPER_POOL_SIZE")
	pflag.Int("gopoolsize", 50000, `coroutine size. Env "TAOS_KEEPER_POOL_SIZE"`)

	viper.SetDefault("RotationInterval", "15s")
	_ = viper.BindEnv("RotationInterval", "TAOS_KEEPER_ROTATION_INTERVAL")
	pflag.StringP("RotationInterval", "R", "15s", `interval for refresh metrics, such as "300ms", Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h". Env "TAOS_KEEPER_ROTATION_INTERVAL"`)

	viper.SetDefault("tdengine.host", "127.0.0.1")
	_ = viper.BindEnv("tdengine.host", "TAOS_KEEPER_TDENGINE_HOST")
	pflag.String("tdengine.host", "127.0.0.1", `TDengine server's ip. Env "TAOS_KEEPER_TDENGINE_HOST"`)

	viper.SetDefault("tdengine.port", 6041)
	_ = viper.BindEnv("tdengine.port", "TAOS_KEEPER_TDENGINE_PORT")
	pflag.Int("tdengine.port", 6041, `TDengine REST server(taosAdapter)'s port. Env "TAOS_KEEPER_TDENGINE_PORT"`)

	viper.SetDefault("tdengine.username", "root")
	_ = viper.BindEnv("tdengine.username", "TAOS_KEEPER_TDENGINE_USERNAME")
	pflag.String("tdengine.username", "root", `TDengine server's username. Env "TAOS_KEEPER_TDENGINE_USERNAME"`)

	viper.SetDefault("tdengine.password", "taosdata")
	_ = viper.BindEnv("tdengine.password", "TAOS_KEEPER_TDENGINE_PASSWORD")
	pflag.String("tdengine.password", "taosdata", `TDengine server's password. Env "TAOS_KEEPER_TDENGINE_PASSWORD"`)

	viper.SetDefault("tdengine.usessl", false)
	_ = viper.BindEnv("tdengine.usessl", "TAOS_KEEPER_TDENGINE_USESSL")
	pflag.Bool("tdengine.usessl", false, `TDengine server use ssl or not. Env "TAOS_KEEPER_TDENGINE_USESSL"`)

	viper.SetDefault("metrics.prefix", "")
	_ = viper.BindEnv("metrics.prefix", "TAOS_KEEPER_METRICS_PREFIX")
	pflag.String("metrics.prefix", "", `prefix in metrics names. Env "TAOS_KEEPER_METRICS_PREFIX"`)

	viper.SetDefault("metrics.database.name", "log")
	_ = viper.BindEnv("metrics.database.name", "TAOS_KEEPER_METRICS_DATABASE")
	pflag.String("metrics.database.name", "log", `database for storing metrics data. Env "TAOS_KEEPER_METRICS_DATABASE"`)

	viper.SetDefault("metrics.database.options.vgroups", 1)
	_ = viper.BindEnv("metrics.database.options.vgroups", "TAOS_KEEPER_METRICS_VGROUPS")
	pflag.Int("metrics.database.options.vgroups", 1, `database option vgroups for audit database. Env "TAOS_KEEPER_METRICS_VGROUPS"`)

	viper.SetDefault("metrics.database.options.buffer", 64)
	_ = viper.BindEnv("metrics.database.options.buffer", "TAOS_KEEPER_METRICS_BUFFER")
	pflag.Int("metrics.database.options.buffer", 64, `database option buffer for audit database. Env "TAOS_KEEPER_METRICS_BUFFER"`)

	viper.SetDefault("metrics.database.options.keep", 90)
	_ = viper.BindEnv("metrics.database.options.keep", "TAOS_KEEPER_METRICS_KEEP")
	pflag.Int("metrics.database.options.keep", 90, `database option buffer for audit database. Env "TAOS_KEEPER_METRICS_KEEP"`)

	viper.SetDefault("metrics.database.options.cachemodel", "both")
	_ = viper.BindEnv("metrics.database.options.cachemodel", "TAOS_KEEPER_METRICS_CACHEMODEL")
	pflag.String("metrics.database.options.cachemodel", "both", `database option cachemodel for audit database. Env "TAOS_KEEPER_METRICS_CACHEMODEL"`)

	viper.SetDefault("metrics.tables", []string{})
	_ = viper.BindEnv("metrics.tables", "TAOS_KEEPER_METRICS_TABLES")
	pflag.StringArray("metrics.tables", []string{}, `export some tables that are not super table, multiple values split with white space. Env "TAOS_KEEPER_METRICS_TABLES"`)

	viper.SetDefault("environment.incgroup", false)
	_ = viper.BindEnv("environment.incgroup", "TAOS_KEEPER_ENVIRONMENT_INCGROUP")
	pflag.Bool("environment.incgroup", false, `whether running in cgroup. Env "TAOS_KEEPER_ENVIRONMENT_INCGROUP"`)

	initLog()

	if version.IsEnterprise == "true" {
		initAudit()
	}
}

func initLog() {
	switch runtime.GOOS {
	case "windows":
		viper.SetDefault("log.path", fmt.Sprintf("C:\\%s\\log", version.CUS_NAME))
		_ = viper.BindEnv("log.path", "TAOS_KEEPER_LOG_PATH")
		pflag.String("log.path", fmt.Sprintf("C:\\%s\\log", version.CUS_NAME), `log path. Env "TAOS_KEEPER_LOG_PATH"`)
	default:
		viper.SetDefault("log.path", fmt.Sprintf("/var/log/%s", version.CUS_PROMPT))
		_ = viper.BindEnv("log.path", "TAOS_KEEPER_LOG_PATH")
		pflag.String("log.path", fmt.Sprintf("/var/log/%s", version.CUS_PROMPT), `log path. Env "TAOS_KEEPER_LOG_PATH"`)
	}

	viper.SetDefault("log.rotationCount", 5)
	_ = viper.BindEnv("log.rotationCount", "TAOS_KEEPER_LOG_ROTATION_COUNT")
	pflag.Uint("log.rotationCount", 5, `log rotation count. Env "TAOS_KEEPER_LOG_ROTATION_COUNT"`)

	viper.SetDefault("log.rotationTime", time.Hour*24)
	_ = viper.BindEnv("log.rotationTime", "TAOS_KEEPER_LOG_ROTATION_TIME")
	pflag.Duration("log.rotationTime", time.Hour*24, `log rotation time. Env "TAOS_KEEPER_LOG_ROTATION_TIME"`)

	viper.SetDefault("log.rotationSize", "100000000")
	_ = viper.BindEnv("log.rotationSize", "TAOS_KEEPER_LOG_ROTATION_SIZE")
	pflag.String("log.rotationSize", "100000000", `log rotation size(KB MB GB), must be a positive integer. Env "TAOS_KEEPER_LOG_ROTATION_SIZE"`)
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
