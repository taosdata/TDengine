package taosSql

import (
	"net"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/errors"
)

var (
	ErrInvalidDSNUnescaped = &errors.TaosError{Code: 0xffff, ErrStr: "invalid DSN: did you forget to escape a param value?"}
	ErrInvalidDSNAddr      = &errors.TaosError{Code: 0xffff, ErrStr: "invalid DSN: network address not terminated (missing closing brace)"}
	ErrInvalidDSNPort      = &errors.TaosError{Code: 0xffff, ErrStr: "invalid DSN: network port is not a valid number"}
	ErrInvalidDSNNoSlash   = &errors.TaosError{Code: 0xffff, ErrStr: "invalid DSN: missing the slash separating the database name"}
)

// Config is a configuration parsed from a DSN string.
// If a new Config is created instead of being parsed from a DSN string,
// the NewConfig function should be used, which sets default values.
type Config struct {
	User                    string // Username
	Passwd                  string // Password (requires User)
	Net                     string // Network type
	Addr                    string // Network address (requires Net)
	Port                    int
	DbName                  string            // Database name
	Params                  map[string]string // Connection parameters
	Loc                     *time.Location    // Deprecated: never used
	InterpolateParams       bool              // Interpolate placeholders into query string
	ConfigPath              string
	CgoThread               int
	CgoAsyncHandlerPoolSize int
	Timezone                *time.Location // Timezone for connection, e.g., "Asia%2FShanghai" or "UTC"
}

// NewConfig creates a new Config and sets default values.
func NewConfig() *Config {
	return &Config{
		Loc:               time.UTC,
		InterpolateParams: true,
	}
}

// ParseDSN parses the DSN string to a Config
func ParseDSN(dsn string) (cfg *Config, err error) {
	// New config with some default values
	cfg = NewConfig()

	// [user[:password]@][net[(addr)]]/dbname[?param1=value1&paramN=valueN]
	// Find the last '/' (since the password or the net addr might contain a '/')
	foundSlash := false
	for i := len(dsn) - 1; i >= 0; i-- {
		if dsn[i] == '/' {
			foundSlash = true
			var j, k int

			// left part is empty if i <= 0
			if i > 0 {
				// [username[:password]@][protocol[(address)]]
				// Find the last '@' in dsn[:i]
				for j = i; j >= 0; j-- {
					if dsn[j] == '@' {
						// username[:password]
						// Find the first ':' in dsn[:j]
						for k = 0; k < j; k++ {
							if dsn[k] == ':' {
								cfg.Passwd = tryUnescape(dsn[k+1 : j])
								break
							}
						}
						cfg.User = tryUnescape(dsn[:k])

						break
					}
				}

				// [protocol[(address)]]
				// Find the first '(' in dsn[j+1:i]
				for k = j + 1; k < i; k++ {
					if dsn[k] == '(' {
						// dsn[i-1] must be == ')' if an address is specified
						if dsn[i-1] != ')' {
							if strings.ContainsRune(dsn[k+1:i], ')') {
								return nil, ErrInvalidDSNUnescaped
							}
							//return nil, errInvalidDSNAddr
						}
						if dsn[j+1:k] == "cfg" {
							cfgPath := dsn[k+1 : i-1]
							cfg.ConfigPath, err = filepath.Abs(cfgPath)
							if err != nil {
								return nil, err
							}
							break
						} else {
							// ip:port
							addr := dsn[k+1 : i-1]
							host, port, err := net.SplitHostPort(addr)
							if err != nil {
								return nil, ErrInvalidDSNAddr
							}
							if len(port) != 0 {
								cfg.Port, err = strconv.Atoi(port)
								if err != nil {
									return nil, ErrInvalidDSNPort
								}
							}
							cfg.Addr = host
							break
						}
					}
				}
				cfg.Net = dsn[j+1 : k]
			}

			// dbname[?param1=value1&...&paramN=valueN]
			// Find the first '?' in dsn[i+1:]
			for j = i + 1; j < len(dsn); j++ {
				if dsn[j] == '?' {
					if err = parseDSNParams(cfg, dsn[j+1:]); err != nil {
						return
					}
					break
				}
			}
			cfg.DbName = dsn[i+1 : j]

			break
		}
	}

	if !foundSlash && len(dsn) > 0 {
		return nil, ErrInvalidDSNNoSlash
	}

	return
}

// parseDSNParams parses the DSN "query string"
// Values must be url.QueryEscape'ed
func parseDSNParams(cfg *Config, params string) (err error) {
	for _, v := range strings.Split(params, "&") {
		param := strings.SplitN(v, "=", 2)
		if len(param) != 2 {
			continue
		}

		// cfg params
		switch value := param[1]; param[0] {
		// Enable client side placeholder substitution
		case "interpolateParams":
			cfg.InterpolateParams, err = strconv.ParseBool(value)
			if err != nil {
				return &errors.TaosError{Code: 0xffff, ErrStr: "invalid bool value: " + value}
			}

		// Time Location
		case "loc":
			if value, err = url.QueryUnescape(value); err != nil {
				return
			}
			cfg.Loc, err = time.LoadLocation(value)
			if err != nil {
				return
			}

		case "cgoThread":
			cfg.CgoThread, err = strconv.Atoi(value)
			if err != nil {
				return &errors.TaosError{Code: 0xffff, ErrStr: "invalid cgoThread value: " + value}
			}

		case "cgoAsyncHandlerPoolSize":
			cfg.CgoAsyncHandlerPoolSize, err = strconv.Atoi(value)
			if err != nil {
				return &errors.TaosError{Code: 0xffff, ErrStr: "invalid cgoAsyncHandlerPoolSize value: " + value}
			}
		case "timezone":
			escapedValue, err := url.QueryUnescape(value)
			if err != nil {
				return &errors.TaosError{Code: 0xffff, ErrStr: "can not unescape timezone value: " + value + ", " + err.Error()}
			}
			cfg.Timezone, err = common.ParseTimezone(escapedValue)
			if err != nil {
				return &errors.TaosError{Code: 0xffff, ErrStr: "invalid timezone value: " + escapedValue + ", " + err.Error()}
			}
		default:
			if err = setParam(cfg, param[0], value); err != nil {
				return
			}
		}
	}

	return
}

func setParam(cfg *Config, key, value string) error {
	// lazy init
	var err error
	if cfg.Params == nil {
		cfg.Params = make(map[string]string)
	}

	if cfg.Params[key], err = url.QueryUnescape(value); err != nil {
		return err
	}
	return nil
}
func tryUnescape(s string) string {
	if res, err := url.QueryUnescape(s); err == nil {
		return res
	}
	return s
}
