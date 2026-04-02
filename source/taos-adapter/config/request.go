package config

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/taosdata/taosadapter/v3/tools/sqltype"
)

type Request struct {
	QueryLimitEnable                 bool
	ExcludeQueryLimitSql             []string
	ExcludeQueryLimitSqlMaxByteCount int
	ExcludeQueryLimitSqlMinByteCount int
	ExcludeQueryLimitSqlRegex        []*regexp.Regexp
	Default                          *LimitConfig
	Users                            map[string]*LimitConfig
}

type LimitConfig struct {
	QueryLimit       int
	QueryWaitTimeout int
	QueryMaxWait     int
}

func initRequest(v *viper.Viper) {
	v.SetDefault("request.queryLimitEnable", false)
	_ = v.BindEnv("request.queryLimitEnable", "TAOS_ADAPTER_REQUEST_QUERY_LIMIT_ENABLE")

	v.SetDefault("request.excludeQueryLimitSql", nil)
	_ = v.BindEnv("request.excludeQueryLimitSql", "TAOS_ADAPTER_REQUEST_EXCLUDE_QUERY_LIMIT_SQL")

	v.SetDefault("request.excludeQueryLimitSqlRegex", nil)
	_ = v.BindEnv("request.excludeQueryLimitSqlRegex", "TAOS_ADAPTER_REQUEST_EXCLUDE_QUERY_LIMIT_SQL_REGEX")

	v.SetDefault("request.default.queryLimit", 0)
	_ = v.BindEnv("request.default.queryLimit", "TAOS_ADAPTER_REQUEST_DEFAULT_QUERY_LIMIT")

	v.SetDefault("request.default.queryWaitTimeout", 900)
	_ = v.BindEnv("request.default.queryWaitTimeout", "TAOS_ADAPTER_REQUEST_DEFAULT_QUERY_WAIT_TIMEOUT")

	v.SetDefault("request.default.queryMaxWait", 0)
	_ = v.BindEnv("request.default.queryMaxWait", "TAOS_ADAPTER_REQUEST_DEFAULT_QUERY_MAX_WAIT")
}

func registerRequestFlags() {
	pflag.Bool("request.queryLimitEnable", false, `Whether to enable the request limiting function, default false. ENV "TAOS_ADAPTER_REQUEST_QUERY_LIMIT_ENABLE"`)
	pflag.StringArray("request.excludeQueryLimitSql", nil, `The sql that does not limit the request, uses prefix matching (case-insensitive, spaces removed), default []. ENV "TAOS_ADAPTER_REQUEST_EXCLUDE_QUERY_LIMIT_SQL"`)
	pflag.StringArray("request.excludeQueryLimitSqlRegex", nil, `The sql that does not limit the request, support regular expressions, default []. ENV "TAOS_ADAPTER_REQUEST_EXCLUDE_QUERY_LIMIT_SQL_REGEX"`)
	pflag.Int("request.default.queryLimit", 0, `The default maximum number of queries allowed per user, default 0 means no limit. ENV "TAOS_ADAPTER_REQUEST_DEFAULT_QUERY_LIMIT"`)
	pflag.Int("request.default.queryWaitTimeout", 900, `The default maximum wait time for a query to be executed, in seconds, default 900. ENV "TAOS_ADAPTER_REQUEST_DEFAULT_QUERY_WAIT_TIMEOUT"`)
	pflag.Int("request.default.queryMaxWait", 0, `The default maximum number of requests allowed to wait when the request is limited, default 0 means no wait. ENV "TAOS_ADAPTER_REQUEST_DEFAULT_QUERY_MAX_WAIT"`)
}

func (r *Request) setValue(v *viper.Viper) error {
	r.QueryLimitEnable = v.GetBool("request.queryLimitEnable")
	r.ExcludeQueryLimitSql = v.GetStringSlice("request.excludeQueryLimitSql")
	if len(r.ExcludeQueryLimitSql) > 0 {
		for i := 0; i < len(r.ExcludeQueryLimitSql); i++ {
			r.ExcludeQueryLimitSql[i] = sqltype.RemoveSpacesAndLowercase(r.ExcludeQueryLimitSql[i], 0)
			if len(r.ExcludeQueryLimitSql[i]) == 0 {
				return fmt.Errorf("config request.excludeQueryLimitSql has empty sql")
			}
			if !strings.HasPrefix(r.ExcludeQueryLimitSql[i], "select") {
				return fmt.Errorf("config request.excludeQueryLimitSql only support select sql, but got: %s", r.ExcludeQueryLimitSql[i])
			}
			if len(r.ExcludeQueryLimitSql[i]) == len("select") {
				return fmt.Errorf("config request.excludeQueryLimitSql can't be only 'select'")
			}
			if len(r.ExcludeQueryLimitSql[i]) > r.ExcludeQueryLimitSqlMaxByteCount {
				r.ExcludeQueryLimitSqlMaxByteCount = len(r.ExcludeQueryLimitSql[i])
			}
			if r.ExcludeQueryLimitSqlMinByteCount == 0 || len(r.ExcludeQueryLimitSql[i]) < r.ExcludeQueryLimitSqlMinByteCount {
				r.ExcludeQueryLimitSqlMinByteCount = len(r.ExcludeQueryLimitSql[i])
			}
		}
	}
	sqlRegex := v.GetStringSlice("request.excludeQueryLimitSqlRegex")
	if len(sqlRegex) > 0 {
		r.ExcludeQueryLimitSqlRegex = make([]*regexp.Regexp, 0, len(sqlRegex))
		for _, reg := range sqlRegex {
			re, err := regexp.Compile(reg)
			if err != nil {
				return fmt.Errorf("compile excludeQueryLimitSqlRegex %s failed: %w", reg, err)
			}
			r.ExcludeQueryLimitSqlRegex = append(r.ExcludeQueryLimitSqlRegex, re)
		}
	}
	r.Default = &LimitConfig{
		QueryLimit:       v.GetInt("request.default.queryLimit"),
		QueryWaitTimeout: v.GetInt("request.default.queryWaitTimeout"),
		QueryMaxWait:     v.GetInt("request.default.queryMaxWait"),
	}
	userConfig := v.GetStringMap("request.users")
	if len(userConfig) > 0 {
		r.Users = make(map[string]*LimitConfig, len(userConfig))
		for k := range userConfig {
			userViper := v.Sub("request.users." + k)
			if userViper == nil {
				r.Users[k] = r.Default
				continue
			}
			userViper.SetDefault("queryLimit", r.Default.QueryLimit)
			userViper.SetDefault("queryWaitTimeout", r.Default.QueryWaitTimeout)
			userViper.SetDefault("queryMaxWait", r.Default.QueryMaxWait)

			r.Users[k] = &LimitConfig{
				QueryLimit:       userViper.GetInt("queryLimit"),
				QueryWaitTimeout: userViper.GetInt("queryWaitTimeout"),
				QueryMaxWait:     userViper.GetInt("queryMaxWait"),
			}
		}
	}
	return nil
}

func (r *Request) GetUserLimitConfig(user string) *LimitConfig {
	if r.Users == nil {
		return r.Default
	}
	if cfg, ok := r.Users[user]; ok {
		return cfg
	}
	return r.Default
}
