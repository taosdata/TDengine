package config

import (
	"fmt"
	"io/fs"
	"os"
	"runtime"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/taosdata/go-utils/web"
	"github.com/taosdata/taoskeeper/util"
	"github.com/taosdata/taoskeeper/util/pool"
	"github.com/taosdata/taoskeeper/version"
)

var Name = fmt.Sprintf("%skeeper", version.CUS_PROMPT)

const ReqIDKey = "QID"
const ModelKey = "model"

type Config struct {
	InstanceID       uint8
	Cors             web.CorsConfig  `toml:"cors"`
	Host             string          `toml:"host"`
	Port             int             `toml:"port"`
	LogLevel         string          `toml:"loglevel"`
	GoPoolSize       int             `toml:"gopoolsize"`
	RotationInterval string          `toml:"RotationInterval"`
	TDengine         TDengineRestful `toml:"tdengine"`
	Metrics          Metrics         `toml:"metrics"`
	Env              Environment     `toml:"environment"`
	Audit            Audit           `toml:"audit"`
	SSL              SSL             `toml:"ssl"`
	Log              Log             `mapstructure:"-"`

	Transfer string
	FromTime string
	Drop     string
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
		if version.TD_PRODUCT_NAME != "" {
			fmt.Printf("%s\n", version.TD_PRODUCT_NAME)
		} else {
			if version.IsEnterprise == "true" {
				fmt.Printf("%s TSDB-Enterprise\n", version.CUS_NAME)
			} else {
				fmt.Printf("%s TSDB-OSS\n", version.CUS_NAME)
			}
		}
		fmt.Printf("%s version: %s\n", Name, version.Version)
		fmt.Printf("git: %s\n", version.Gitinfo)
		fmt.Printf("build: %s\n", version.BuildInfo)
		os.Exit(0)
	}

	if *cp != "" {
		viper.SetConfigFile(*cp)
	}

	viper.SetEnvPrefix(Name)
	if err := viper.BindPFlags(pflag.CommandLine); err != nil {
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
	if err := viper.Unmarshal(&conf); err != nil {
		panic(err)
	}

	conf.Transfer = *transfer
	conf.FromTime = *fromTime
	conf.Drop = *drop

	conf.Cors.Init()
	pool.Init(conf.GoPoolSize)
	conf.Log.SetValue()

	// set log level default value: info
	if conf.LogLevel == "" {
		conf.LogLevel = "info"
	}
	if viper.IsSet("log.level") {
		conf.LogLevel = conf.Log.Level
	} else {
		viper.Set("log.level", "")
	}

	if !viper.IsSet("logLevel") {
		viper.Set("logLevel", "")
	}

	conf.Host = util.HandleIp(conf.Host)

	Conf = &conf
	return &conf
}

func init() {
	viper.SetDefault("instanceId", 64)
	_ = viper.BindEnv("instanceId", "TAOS_KEEPER_INSTANCE_ID")
	pflag.Int("instanceId", 64, `instance ID. Env "TAOS_KEEPER_INSTANCE_ID"`)

	viper.SetDefault("host", "")
	_ = viper.BindEnv("host", "TAOS_KEEPER_HOST")
	pflag.StringP("host", "H", "", `http host. Env "TAOS_KEEPER_HOST"`)

	viper.SetDefault("port", 6043)
	_ = viper.BindEnv("port", "TAOS_KEEPER_PORT")
	pflag.IntP("port", "P", 6043, `http port. Env "TAOS_KEEPER_PORT"`)

	_ = viper.BindEnv("logLevel", "TAOS_KEEPER_LOG_LEVEL")
	pflag.String("logLevel", "info", `log level (trace debug info warning error). Env "TAOS_KEEPER_LOG_LEVEL"`)

	viper.SetDefault("gopoolsize", 50000)
	_ = viper.BindEnv("gopoolsize", "TAOS_KEEPER_POOL_SIZE")
	pflag.Int("gopoolsize", 50000, `coroutine size. Env "TAOS_KEEPER_POOL_SIZE"`)

	viper.SetDefault("RotationInterval", "15s")
	_ = viper.BindEnv("RotationInterval", "TAOS_KEEPER_ROTATION_INTERVAL")
	pflag.StringP("RotationInterval", "R", "15s", `interval for refresh metrics, such as "300ms", Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h". Env "TAOS_KEEPER_ROTATION_INTERVAL"`)

	initTDengine()
	initMetrics()
	initEnvironment()
	initSSL()
	initLog()
	if version.IsEnterprise == "true" {
		initAudit()
	}
}

func GetCfg() *Config {
	return &Config{
		InstanceID: 64,
		Port:       6043,
		LogLevel:   "trace",
		TDengine: TDengineRestful{
			Host:     "127.0.0.1",
			Port:     6041,
			Username: "root",
			Password: "taosdata",
			Usessl:   false,
		},
		Metrics: Metrics{
			Database: Database{
				Name:    "keeper_test_log",
				Options: map[string]interface{}{},
			},
		},
		Log: Log{
			Level:            "trace",
			Path:             "/var/log/taos",
			RotationCount:    10,
			RotationTime:     24 * time.Hour,
			RotationSize:     1073741824,
			Compress:         true,
			ReservedDiskSize: 1073741824,
		},
	}
}
