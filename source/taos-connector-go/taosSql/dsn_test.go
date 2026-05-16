package taosSql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// @author: xftan
// @date: 2022/1/27 16:18
// @description: test dsn parse
func TestParseDsn(t *testing.T) {
	shanghaiTimezone, err := time.LoadLocation("Asia/Shanghai")
	assert.NoError(t, err)
	tests := []struct {
		name                    string
		dsn                     string
		errs                    string
		want                    *Config
		user                    string
		passwd                  string
		net                     string
		addr                    string
		port                    int
		dbName                  string
		configPath              string
		cgoThread               int
		cgoAsyncHandlerPoolSize int
	}{
		{
			name: "invalid",
			dsn:  "abcd",
			errs: "invalid DSN: missing the slash separating the database name",
		},
		{
			name: "normal",
			dsn:  "user:passwd@net(fqdn:6030)/dbname",
			want: &Config{
				User:                    "user",
				Passwd:                  "passwd",
				Net:                     "net",
				Addr:                    "fqdn",
				Port:                    6030,
				DbName:                  "dbname",
				Params:                  nil,
				Loc:                     time.UTC,
				InterpolateParams:       true,
				ConfigPath:              "",
				CgoThread:               0,
				CgoAsyncHandlerPoolSize: 0,
			},
		},
		{
			name: "missing closing brace",
			dsn:  "user:passwd@net()/dbname",
			errs: "invalid DSN: network address not terminated (missing closing brace)",
		},
		{
			name: "default addr",
			dsn:  "user:passwd@net(:)/dbname",
			want: &Config{
				User:                    "user",
				Passwd:                  "passwd",
				Net:                     "net",
				Addr:                    "",
				Port:                    0,
				DbName:                  "dbname",
				Params:                  nil,
				Loc:                     time.UTC,
				InterpolateParams:       true,
				ConfigPath:              "",
				CgoThread:               0,
				CgoAsyncHandlerPoolSize: 0,
			},
		},
		{
			name: "0port",
			dsn:  "user:passwd@net(:0)/dbname",
			want: &Config{
				User:                    "user",
				Passwd:                  "passwd",
				Net:                     "net",
				Addr:                    "",
				Port:                    0,
				DbName:                  "dbname",
				Params:                  nil,
				Loc:                     time.UTC,
				InterpolateParams:       true,
				ConfigPath:              "",
				CgoThread:               0,
				CgoAsyncHandlerPoolSize: 0,
			},
		},
		{
			name: "no dbname",
			dsn:  "user:passwd@net(:0)/",
			want: &Config{
				User:                    "user",
				Passwd:                  "passwd",
				Net:                     "net",
				Addr:                    "",
				Port:                    0,
				DbName:                  "",
				Params:                  nil,
				Loc:                     time.UTC,
				InterpolateParams:       true,
				ConfigPath:              "",
				CgoThread:               0,
				CgoAsyncHandlerPoolSize: 0,
			},
		},
		{
			name: "no auth",
			dsn:  "net(:0)/wo",
			want: &Config{
				User:                    "",
				Passwd:                  "",
				Net:                     "net",
				Addr:                    "",
				Port:                    0,
				DbName:                  "wo",
				Params:                  nil,
				Loc:                     time.UTC,
				InterpolateParams:       true,
				ConfigPath:              "",
				CgoThread:               0,
				CgoAsyncHandlerPoolSize: 0,
			},
		},
		{
			name: "cfg",
			dsn:  "user:passwd@cfg(/home/taos)/db",
			want: &Config{
				User:                    "user",
				Passwd:                  "passwd",
				Net:                     "cfg",
				Addr:                    "",
				Port:                    0,
				DbName:                  "db",
				Params:                  nil,
				Loc:                     time.UTC,
				InterpolateParams:       true,
				ConfigPath:              "/home/taos",
				CgoThread:               0,
				CgoAsyncHandlerPoolSize: 0,
			},
		},
		{
			name: "no addr",
			dsn:  "user:passwd@cfg/db",
			want: &Config{
				User:                    "user",
				Passwd:                  "passwd",
				Net:                     "cfg",
				Addr:                    "",
				Port:                    0,
				DbName:                  "db",
				Params:                  nil,
				Loc:                     time.UTC,
				InterpolateParams:       true,
				ConfigPath:              "",
				CgoThread:               0,
				CgoAsyncHandlerPoolSize: 0,
			},
		},
		{
			name: "options",
			dsn:  "net(:0)/wo?firstEp=LAPTOP-NNKFTLTG.localdomain%3A6030&secondEp=LAPTOP-NNKFTLTG.localdomain%3A6030&fqdn=LAPTOP-NNKFTLTG.localdomain&serverPort=6030&configDir=%2Fetc%2Ftaos&logDir=%2Fvar%2Flog%2Ftaos&scriptDir=%2Fetc%2Ftaos&arbitrator=&numOfThreadsPerCore=1.000000&maxNumOfDistinctRes=10000000&rpcTimer=300&rpcForceTcp=0&rpcMaxTime=600&shellActivityTimer=3&compressMsgSize=-1&maxSQLLength=1048576&maxWildCardsLength=100&maxNumOfOrderedRes=100000&keepColumnName=0&timezone=Asia%2FShanghai&locale=C.UTF-8&charset=UTF-8&numOfLogLines=10000000&logKeepDays=0&asyncLog=1&debugFlag=0&rpcDebugFlag=131&tmrDebugFlag=131&cDebugFlag=131&jniDebugFlag=131&odbcDebugFlag=131&uDebugFlag=131&qDebugFlag=131&tsdbDebugFlag=131&gitinfo=TAOS_CFG_VTYPE_STRING&gitinfoOfInternal=TAOS_CFG_VTYPE_STRING&buildinfo=TAOS_CFG_VTYPE_STRING&version=TAOS_CFG_VTYPE_STRING&maxBinaryDisplayWidth=30&tempDir=%2Ftmp%2F",
			want: &Config{
				User:   "",
				Passwd: "",
				Net:    "net",
				Addr:   "",
				Port:   0,
				DbName: "wo",
				Params: map[string]string{
					"firstEp":               "LAPTOP-NNKFTLTG.localdomain:6030",
					"secondEp":              "LAPTOP-NNKFTLTG.localdomain:6030",
					"fqdn":                  "LAPTOP-NNKFTLTG.localdomain",
					"serverPort":            "6030",
					"configDir":             "/etc/taos",
					"logDir":                "/var/log/taos",
					"scriptDir":             "/etc/taos",
					"arbitrator":            "",
					"numOfThreadsPerCore":   "1.000000",
					"maxNumOfDistinctRes":   "10000000",
					"rpcTimer":              "300",
					"rpcForceTcp":           "0",
					"rpcMaxTime":            "600",
					"shellActivityTimer":    "3",
					"compressMsgSize":       "-1",
					"maxSQLLength":          "1048576",
					"maxWildCardsLength":    "100",
					"maxNumOfOrderedRes":    "100000",
					"keepColumnName":        "0",
					"locale":                "C.UTF-8",
					"charset":               "UTF-8",
					"numOfLogLines":         "10000000",
					"logKeepDays":           "0",
					"asyncLog":              "1",
					"debugFlag":             "0",
					"rpcDebugFlag":          "131",
					"tmrDebugFlag":          "131",
					"cDebugFlag":            "131",
					"jniDebugFlag":          "131",
					"odbcDebugFlag":         "131",
					"uDebugFlag":            "131",
					"qDebugFlag":            "131",
					"tsdbDebugFlag":         "131",
					"gitinfo":               "TAOS_CFG_VTYPE_STRING",
					"gitinfoOfInternal":     "TAOS_CFG_VTYPE_STRING",
					"buildinfo":             "TAOS_CFG_VTYPE_STRING",
					"version":               "TAOS_CFG_VTYPE_STRING",
					"maxBinaryDisplayWidth": "30",
					"tempDir":               "/tmp/",
				},
				Loc:                     time.UTC,
				InterpolateParams:       true,
				ConfigPath:              "",
				CgoThread:               0,
				CgoAsyncHandlerPoolSize: 0,
				Timezone:                shanghaiTimezone,
			},
		},
		{
			name: "cgoThread",
			dsn:  "net(:0)/wo?cgoThread=8",
			want: &Config{
				User:                    "",
				Passwd:                  "",
				Net:                     "net",
				Addr:                    "",
				Port:                    0,
				DbName:                  "wo",
				Params:                  nil,
				Loc:                     time.UTC,
				InterpolateParams:       true,
				ConfigPath:              "",
				CgoThread:               8,
				CgoAsyncHandlerPoolSize: 0,
			},
		},
		{
			name: "cgoAsyncHandlerPoolSize",
			dsn:  "net(:0)/wo?cgoThread=8&cgoAsyncHandlerPoolSize=10000",
			want: &Config{
				User:                    "",
				Passwd:                  "",
				Net:                     "net",
				Addr:                    "",
				Port:                    0,
				DbName:                  "wo",
				Params:                  nil,
				Loc:                     time.UTC,
				InterpolateParams:       true,
				ConfigPath:              "",
				CgoThread:               8,
				CgoAsyncHandlerPoolSize: 10000,
			},
		},
		{
			name: "loc",
			dsn:  "net(:0)/wo?cgoThread=8&loc=Asia%2FShanghai",
			want: &Config{
				User:                    "",
				Passwd:                  "",
				Net:                     "net",
				Addr:                    "",
				Port:                    0,
				DbName:                  "wo",
				Params:                  nil,
				Loc:                     shanghaiTimezone,
				InterpolateParams:       true,
				ConfigPath:              "",
				CgoThread:               8,
				CgoAsyncHandlerPoolSize: 0,
			},
		},
		{
			name: "interpolateParams",
			dsn:  "user:passwd@net(:)/dbname?interpolateParams=false",
			want: &Config{
				User:                    "user",
				Passwd:                  "passwd",
				Net:                     "net",
				Addr:                    "",
				Port:                    0,
				DbName:                  "dbname",
				Params:                  nil,
				Loc:                     time.UTC,
				InterpolateParams:       false,
				ConfigPath:              "",
				CgoThread:               0,
				CgoAsyncHandlerPoolSize: 0,
			},
		},
		{
			name: "special char",
			dsn:  "!%40%23%24%25%5E%26*()-_%2B%3D%5B%5D%7B%7D%3A%3B%3E%3C%3F%7C~%2C.:!%40%23%24%25%5E%26*()-_%2B%3D%5B%5D%7B%7D%3A%3B%3E%3C%3F%7C~%2C.@net(:)/dbname",
			want: &Config{
				User:                    "!@#$%^&*()-_+=[]{}:;><?|~,.",
				Passwd:                  "!@#$%^&*()-_+=[]{}:;><?|~,.",
				Net:                     "net",
				Addr:                    "",
				Port:                    0,
				DbName:                  "dbname",
				Params:                  nil,
				Loc:                     time.UTC,
				InterpolateParams:       true,
				ConfigPath:              "",
				CgoThread:               0,
				CgoAsyncHandlerPoolSize: 0,
			},
		},
		//encodeURIComponent('!q@w#a$1%3^&*()-_+=[]{}:;><?|~,.')
		{
			name: "special char2",
			dsn:  "!q%40w%23a%241%253%5E%26*()-_%2B%3D%5B%5D%7B%7D%3A%3B%3E%3C%3F%7C~%2C.:!q%40w%23a%241%253%5E%26*()-_%2B%3D%5B%5D%7B%7D%3A%3B%3E%3C%3F%7C~%2C.@net(:)/dbname",
			want: &Config{
				User:                    "!q@w#a$1%3^&*()-_+=[]{}:;><?|~,.",
				Passwd:                  "!q@w#a$1%3^&*()-_+=[]{}:;><?|~,.",
				Net:                     "net",
				Addr:                    "",
				Port:                    0,
				DbName:                  "dbname",
				Params:                  nil,
				Loc:                     time.UTC,
				InterpolateParams:       true,
				ConfigPath:              "",
				CgoThread:               0,
				CgoAsyncHandlerPoolSize: 0,
			},
		},
		{
			name: "ipv6",
			dsn:  "user:passwd@tcp([ab:cd:ef:ab::cd:ef]:6041)/dbname",
			want: &Config{
				User:                    "user",
				Passwd:                  "passwd",
				Net:                     "tcp",
				Addr:                    "ab:cd:ef:ab::cd:ef",
				Port:                    6041,
				DbName:                  "dbname",
				Params:                  nil,
				Loc:                     time.UTC,
				InterpolateParams:       true,
				ConfigPath:              "",
				CgoThread:               0,
				CgoAsyncHandlerPoolSize: 0,
			},
		},
		{
			name: "timezone",
			dsn:  "user:passwd@net([ab:cd:ef:ab::cd:ef]:6041)/dbname?timezone=Asia%2FShanghai",
			want: &Config{
				User:                    "user",
				Passwd:                  "passwd",
				Net:                     "net",
				Addr:                    "ab:cd:ef:ab::cd:ef",
				Port:                    6041,
				DbName:                  "dbname",
				Params:                  nil,
				Loc:                     time.UTC,
				InterpolateParams:       true,
				ConfigPath:              "",
				CgoThread:               0,
				CgoAsyncHandlerPoolSize: 0,
				Timezone:                shanghaiTimezone,
			},
		},
		{
			name: "timezone with utc",
			dsn:  "user:passwd@net([ab:cd:ef:ab::cd:ef]:6041)/dbname?timezone=UTC",
			want: &Config{
				User:                    "user",
				Passwd:                  "passwd",
				Net:                     "net",
				Addr:                    "ab:cd:ef:ab::cd:ef",
				Port:                    6041,
				DbName:                  "dbname",
				Params:                  nil,
				Loc:                     time.UTC,
				InterpolateParams:       true,
				ConfigPath:              "",
				CgoThread:               0,
				CgoAsyncHandlerPoolSize: 0,
				Timezone:                time.UTC,
			},
		},
		{
			name: "empty timezone",
			dsn:  "user:passwd@net([ab:cd:ef:ab::cd:ef]:6041)/dbname?timezone=",
			errs: "invalid timezone value: , empty string",
		},
		{
			name: "invalid timezone",
			dsn:  "user:passwd@net([ab:cd:ef:ab::cd:ef]:6041)/dbname?timezone=Invalid%2FTimezone",
			errs: "invalid timezone value: Invalid/Timezone, unknown time zone Invalid/Timezone",
		},
		{
			name: "timezone with invalid unescaped characters",
			dsn:  "user:passwd@net([ab:cd:ef:ab::cd:ef]:6041)/dbname?timezone=Asia%2Shanghai",
			errs: "can not unescape timezone value: Asia%2Shanghai, invalid URL escape \"%2S\"",
		},
		{
			name: "timezone Local",
			dsn:  "user:passwd@net([ab:cd:ef:ab::cd:ef]:6041)/dbname?timezone=Local",
			errs: "invalid timezone value: Local, timezone cannot be 'Local'",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := ParseDSN(tc.dsn)
			if err != nil {
				if errs := err.Error(); errs != tc.errs {
					t.Fatal(tc.errs, "\n", errs)
				}
				return
			}
			assert.Equal(t, tc.want, cfg)
		})
	}
}

func TestTryUnescape(t *testing.T) {
	escaped := tryUnescape("%3F") // ?
	assert.Equal(t, "?", escaped)
	escaped = tryUnescape("%3f") // ?
	assert.Equal(t, "?", escaped)
	escaped = tryUnescape("%25") // %
	assert.Equal(t, "%", escaped)
	escaped = tryUnescape("%")
	assert.Equal(t, "%", escaped)
}
