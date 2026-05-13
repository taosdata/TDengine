package config

import (
	"fmt"
	"os"
	"regexp"
	"runtime"
	"testing"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// @author: xftan
// @date: 2021/12/14 15:00
// @description: test init
func TestInit(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "default",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Init()
			logPath := "/var/log/taos"
			configPath := "/etc/taos/taosadapter.toml"
			if runtime.GOOS == "windows" {
				logPath = "C:\\TDengine\\log"
				configPath = "C:\\TDengine\\cfg\\taosadapter.toml"
			}
			host, err := os.Hostname()
			require.NoError(t, err)
			assert.Equal(t, &Config{
				ConfigFile: configPath,
				Register: &Register{
					Instance:    fmt.Sprintf("%s:%s", host, "6041"),
					Description: "",
					Duration:    10,
					Expire:      30,
				},
				Reject: &Reject{
					rejectQuerySqlRegex: []*regexp.Regexp{},
				},
				InstanceID: 32,
				Cors: &CorsConfig{
					AllowAllOrigins:  true,
					AllowOrigins:     []string{},
					AllowHeaders:     []string{},
					ExposeHeaders:    []string{},
					AllowCredentials: false,
					AllowWebSockets:  false,
				},
				TaosConfigDir:       "",
				MaxSyncMethodLimit:  runtime.GOMAXPROCS(0),
				MaxAsyncMethodLimit: runtime.GOMAXPROCS(0),
				Debug:               true,
				Port:                6041,
				LogLevel:            "info",
				RestfulRowLimit:     -1,
				HttpCodeServerError: false,
				SMLAutoCreateDB:     false,
				Log: &Log{
					Level:                  "info",
					Path:                   logPath,
					RotationCount:          3,
					RotationTime:           time.Hour * 24,
					RotationSize:           1 * 1024 * 1024 * 1024, // 1G
					KeepDays:               3,
					Compress:               false,
					ReservedDiskSize:       1 * 1024 * 1024 * 1024,
					EnableSqlToCsvLogging:  false,
					EnableStmtToCsvLogging: false,
					EnableRecordHttpSql:    false,
					SqlRotationCount:       2,
					SqlRotationTime:        time.Hour * 24,
					SqlRotationSize:        1 * 1024 * 1024 * 1024,
				},
				Pool: &Pool{
					MaxConnect:  runtime.GOMAXPROCS(0) * 2,
					MaxIdle:     0,
					IdleTimeout: 0,
					WaitTimeout: 60,
					MaxWait:     0,
				},
				Monitor: &Monitor{
					Disable:                   true,
					CollectDuration:           3 * time.Second,
					InCGroup:                  false,
					PauseQueryMemoryThreshold: 70,
					PauseAllMemoryThreshold:   80,
					Identity:                  "",
				},
				UploadKeeper: &UploadKeeper{
					Enable:        true,
					Url:           "http://127.0.0.1:6043/adapter_report",
					Interval:      15 * time.Second,
					Timeout:       5 * time.Second,
					RetryTimes:    3,
					RetryInterval: 5 * time.Second,
				},
				Request: &Request{
					QueryLimitEnable:          false,
					ExcludeQueryLimitSql:      []string{},
					ExcludeQueryLimitSqlRegex: nil,
					Default:                   &LimitConfig{QueryLimit: 0, QueryWaitTimeout: 900, QueryMaxWait: 0},
					Users:                     nil,
				},
			}, Conf)
			corsC := Conf.Cors.GetConfig()
			assert.Equal(
				t,
				cors.Config{
					AllowAllOrigins:        true,
					AllowOrigins:           nil,
					AllowOriginFunc:        (func(string) bool)(nil),
					AllowMethods:           []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"},
					AllowHeaders:           []string{"Origin", "Content-Length", "Content-Type", "Authorization"},
					AllowCredentials:       false,
					ExposeHeaders:          []string{"Authorization"},
					MaxAge:                 12 * time.Hour,
					AllowWildcard:          true,
					AllowBrowserExtensions: false,
					AllowWebSockets:        false,
					AllowFiles:             false,
				},
				corsC,
			)
		})
	}
}
