package config

import (
	"fmt"
	"runtime"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/taosdata/taosadapter/v3/version"
)

type Log struct {
	Level            string
	Path             string
	RotationCount    uint
	RotationTime     time.Duration
	RotationSize     uint
	KeepDays         uint
	Compress         bool
	ReservedDiskSize uint

	EnableSqlToCsvLogging  bool
	EnableStmtToCsvLogging bool

	EnableRecordHttpSql bool
	SqlRotationCount    uint
	SqlRotationTime     time.Duration
	SqlRotationSize     uint
}

func initLog() {
	switch runtime.GOOS {
	case "windows":
		viper.SetDefault("log.path", fmt.Sprintf("C:\\%s\\log", version.CUS_NAME))
		_ = viper.BindEnv("log.path", "TAOS_ADAPTER_LOG_PATH")
		pflag.String("log.path", fmt.Sprintf("C:\\%s\\log", version.CUS_NAME), `log path. Env "TAOS_ADAPTER_LOG_PATH"`)
	default:
		viper.SetDefault("log.path", fmt.Sprintf("/var/log/%s", version.CUS_PROMPT))
		_ = viper.BindEnv("log.path", "TAOS_ADAPTER_LOG_PATH")
		pflag.String("log.path", fmt.Sprintf("/var/log/%s", version.CUS_PROMPT), `log path. Env "TAOS_ADAPTER_LOG_PATH"`)
	}

	_ = viper.BindEnv("log.level", "TAOS_ADAPTER_LOG_LEVEL")
	pflag.String("log.level", "info", `log level (trace debug info warning error). Env "TAOS_ADAPTER_LOG_LEVEL"`)

	viper.SetDefault("log.rotationCount", 3)
	_ = viper.BindEnv("log.rotationCount", "TAOS_ADAPTER_LOG_ROTATION_COUNT")
	pflag.Uint("log.rotationCount", 3, `log rotation count. Env "TAOS_ADAPTER_LOG_ROTATION_COUNT"`)

	viper.SetDefault("log.rotationTime", time.Hour*24)
	_ = viper.BindEnv("log.rotationTime", "TAOS_ADAPTER_LOG_ROTATION_TIME")
	pflag.Duration("log.rotationTime", time.Hour*24, `deprecated: log rotation time always 24 hours. Env "TAOS_ADAPTER_LOG_ROTATION_TIME"`)

	viper.SetDefault("log.rotationSize", "1GB")
	_ = viper.BindEnv("log.rotationSize", "TAOS_ADAPTER_LOG_ROTATION_SIZE")
	pflag.String("log.rotationSize", "1GB", `log rotation size(KB MB GB), must be a positive integer. Env "TAOS_ADAPTER_LOG_ROTATION_SIZE"`)

	viper.SetDefault("log.keepDays", 3)
	_ = viper.BindEnv("log.keepDays", "TAOS_ADAPTER_LOG_KEEP_DAYS")
	pflag.Uint("log.keepDays", 3, `log retention days, must be a positive integer. Env "TAOS_ADAPTER_LOG_KEEP_DAYS"`)

	viper.SetDefault("log.compress", false)
	_ = viper.BindEnv("log.compress", "TAOS_ADAPTER_LOG_COMPRESS")
	pflag.Bool("log.compress", false, `whether to compress old log. Env "TAOS_ADAPTER_LOG_COMPRESS"`)

	viper.SetDefault("log.reservedDiskSize", "1GB")
	_ = viper.BindEnv("log.reservedDiskSize", "TAOS_ADAPTER_LOG_RESERVED_DISK_SIZE")
	pflag.String("log.reservedDiskSize", "1GB", `reserved disk size for log dir (KB MB GB), must be a positive integer. Env "TAOS_ADAPTER_LOG_RESERVED_DISK_SIZE"`)

	viper.SetDefault("log.enableRecordHttpSql", false)
	_ = viper.BindEnv("log.enableRecordHttpSql", "TAOS_ADAPTER_LOG_ENABLE_RECORD_HTTP_SQL")
	pflag.Bool("log.enableRecordHttpSql", false, `whether to record http sql. Env "TAOS_ADAPTER_LOG_ENABLE_RECORD_HTTP_SQL"`)

	viper.SetDefault("log.sqlRotationCount", 2)
	_ = viper.BindEnv("log.sqlRotationCount", "TAOS_ADAPTER_LOG_SQL_ROTATION_COUNT")
	pflag.Uint("log.sqlRotationCount", 2, `record sql log rotation count. Env "TAOS_ADAPTER_LOG_SQL_ROTATION_COUNT"`)

	viper.SetDefault("log.sqlRotationTime", time.Hour*24)
	_ = viper.BindEnv("log.sqlRotationTime", "TAOS_ADAPTER_LOG_SQL_ROTATION_TIME")
	pflag.Duration("log.sqlRotationTime", time.Hour*24, `record sql log rotation time. Env "TAOS_ADAPTER_LOG_SQL_ROTATION_TIME"`)

	viper.SetDefault("log.sqlRotationSize", "1GB")
	_ = viper.BindEnv("log.sqlRotationSize", "TAOS_ADAPTER_LOG_SQL_ROTATION_SIZE")
	pflag.String("log.sqlRotationSize", "1GB", `record sql log rotation size(KB MB GB), must be a positive integer. Env "TAOS_ADAPTER_LOG_SQL_ROTATION_SIZE"`)

	viper.SetDefault("log.enableSqlToCsvLogging", false)
	_ = viper.BindEnv("log.enableSqlToCsvLogging", "TAOS_ADAPTER_LOG_ENABLE_SQL_TO_CSV_LOGGING")
	pflag.Bool("log.enableSqlToCsvLogging", false, `whether to enable sql to csv logging. Env "TAOS_ADAPTER_LOG_ENABLE_SQL_TO_CSV_LOGGING"`)

	viper.SetDefault("log.enableStmtToCsvLogging", false)
	_ = viper.BindEnv("log.enableStmtToCsvLogging", "TAOS_ADAPTER_LOG_ENABLE_STMT_TO_CSV_LOGGING")
	pflag.Bool("log.enableStmtToCsvLogging", false, `whether to enable stmt to csv logging. Env "TAOS_ADAPTER_LOG_ENABLE_STMT_TO_CSV_LOGGING"`)
}

func (l *Log) setValue() {
	l.Level = viper.GetString("log.level")
	l.Path = viper.GetString("log.path")
	l.RotationCount = viper.GetUint("log.rotationCount")
	l.RotationTime = viper.GetDuration("log.rotationTime")
	l.RotationSize = viper.GetSizeInBytes("log.rotationSize")
	l.KeepDays = viper.GetUint("log.keepDays")
	l.Compress = viper.GetBool("log.compress")
	l.ReservedDiskSize = viper.GetSizeInBytes("log.reservedDiskSize")
	l.EnableRecordHttpSql = viper.GetBool("log.enableRecordHttpSql")
	l.SqlRotationCount = viper.GetUint("log.sqlRotationCount")
	l.SqlRotationTime = viper.GetDuration("log.sqlRotationTime")
	l.SqlRotationSize = viper.GetSizeInBytes("log.sqlRotationSize")
	l.EnableSqlToCsvLogging = viper.GetBool("log.enableSqlToCsvLogging")
	l.EnableStmtToCsvLogging = viper.GetBool("log.enableStmtToCsvLogging")
}
