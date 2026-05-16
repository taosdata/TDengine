package unified

import (
	"time"

	"github.com/taosdata/driver-go/v3/common"
)

// ConnectionConfigDefaults defines optional compatibility defaults used by BuildConnectionConfig.
type ConnectionConfigDefaults struct {
	ReconnectRetryCount int
	User                string
	Passwd              string
	Net                 string
	Addr                string
	Port                int
	WriteTimeout        time.Duration
}

// TaosWSConnectionDefaults is the compatibility default set used by taosWS.
var TaosWSConnectionDefaults = ConnectionConfigDefaults{
	ReconnectRetryCount: 3,
	User:                common.DefaultUser,
	Passwd:              common.DefaultPassword,
	Net:                 "ws",
	Addr:                "127.0.0.1",
	Port:                common.DefaultHttpPort,
	WriteTimeout:        common.DefaultWriteWait,
}

// BuildConnectionConfig clones cfg and fills empty fields with unified defaults plus compatibility defaults.
func BuildConnectionConfig(cfg *Config, defaults ConnectionConfigDefaults) *Config {
	if cfg == nil {
		return nil
	}
	base := NewConfig(cfg.Endpoints)
	normalized := *cfg
	normalized.Endpoints = append([]string(nil), cfg.Endpoints...)

	if normalized.ChanLength == 0 {
		normalized.ChanLength = base.ChanLength
	}
	if normalized.ReadTimeout <= 0 {
		normalized.ReadTimeout = base.ReadTimeout
	}
	if normalized.ReconnectIntervalMs <= 0 {
		normalized.ReconnectIntervalMs = base.ReconnectIntervalMs
	}
	if normalized.ReconnectRetryCount <= 0 {
		if defaults.ReconnectRetryCount > 0 {
			normalized.ReconnectRetryCount = defaults.ReconnectRetryCount
		} else {
			normalized.ReconnectRetryCount = base.ReconnectRetryCount
		}
	}
	if len(normalized.User) == 0 && defaults.User != "" {
		normalized.User = defaults.User
	}
	if len(normalized.Passwd) == 0 && defaults.Passwd != "" {
		normalized.Passwd = defaults.Passwd
	}
	if normalized.Port == 0 && defaults.Port > 0 {
		normalized.Port = defaults.Port
	}
	if len(normalized.Net) == 0 && defaults.Net != "" {
		normalized.Net = defaults.Net
	}
	if len(normalized.Addr) == 0 && defaults.Addr != "" {
		normalized.Addr = defaults.Addr
	}
	if normalized.WriteTimeout <= 0 {
		if defaults.WriteTimeout > 0 {
			normalized.WriteTimeout = defaults.WriteTimeout
		} else {
			normalized.WriteTimeout = base.WriteTimeout
		}
	}
	return &normalized
}
