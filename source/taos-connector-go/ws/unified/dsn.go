package unified

import (
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/taosdata/driver-go/v3/common"
)

type dsnAddress struct {
	host string
	port int
}

// ParseDSN parses DSN using current taos websocket compatibility rules.
func ParseDSN(dsn string) (cfg *Config, err error) {
	cfg = &Config{
		InterpolateParams: true,
	}

	// [user[:password]@][net[(addr)]]/dbname[?param1=value1&paramN=valueN]
	// Find the last '/' (since password or net addr may contain '/').
	foundSlash := false
	for i := len(dsn) - 1; i >= 0; i-- {
		if dsn[i] != '/' {
			continue
		}
		foundSlash = true
		var j, k int

		if i > 0 {
			// Find the last '@' in dsn[:i].
			for j = i; j >= 0; j-- {
				if dsn[j] != '@' {
					continue
				}
				// username[:password]
				for k = 0; k < j; k++ {
					if dsn[k] == ':' {
						cfg.Passwd = tryUnescape(dsn[k+1 : j])
						break
					}
				}
				cfg.User = tryUnescape(dsn[:k])
				break
			}

			var parsedAddrs []dsnAddress
			// protocol[(address[,address...])]
			for k = j + 1; k < i; k++ {
				if dsn[k] != '(' {
					continue
				}
				// dsn[i-1] should be ')' when address is specified.
				if dsn[i-1] != ')' {
					if strings.ContainsRune(dsn[k+1:i], ')') {
						return nil, ErrInvalidDSNUnescaped
					}
				}
				addrListRaw := dsn[k+1 : i-1]
				parsedAddrs, err = parseDSNAddressList(addrListRaw)
				if err != nil {
					return nil, err
				}
				if len(parsedAddrs) > 0 {
					cfg.Addr = parsedAddrs[0].host
					cfg.Port = parsedAddrs[0].port
				}
				break
			}
			cfg.Net = dsn[j+1 : k]
			if len(parsedAddrs) > 1 {
				cfg.Endpoints = make([]string, 0, len(parsedAddrs))
				for idx := 0; idx < len(parsedAddrs); idx++ {
					dsnAddr := parsedAddrs[idx]
					host := dsnAddr.host
					if host == "" {
						host = "127.0.0.1"
					}
					port := dsnAddr.port
					if port == 0 {
						port = common.DefaultHttpPort
					}
					endpoint := &url.URL{
						Scheme: cfg.Net,
						Host:   net.JoinHostPort(host, strconv.Itoa(port)),
					}
					cfg.Endpoints = append(cfg.Endpoints, endpoint.String())
				}
			}
		}

		// dbname[?param1=value1&...]
		for j = i + 1; j < len(dsn); j++ {
			if dsn[j] != '?' {
				continue
			}
			if err = parseDSNParams(cfg, dsn[j+1:]); err != nil {
				return nil, err
			}
			break
		}
		cfg.DbName = dsn[i+1 : j]
		if len(cfg.Endpoints) > 0 && cfg.Token != "" {
			for idx := 0; idx < len(cfg.Endpoints); idx++ {
				u, parseErr := url.Parse(cfg.Endpoints[idx])
				if parseErr != nil {
					return nil, ErrInvalidDSNAddr
				}
				query := u.Query()
				query.Set("token", cfg.Token)
				u.RawQuery = query.Encode()
				cfg.Endpoints[idx] = u.String()
			}
		}
		break
	}

	if !foundSlash && len(dsn) > 0 {
		return nil, ErrInvalidDSNNoSlash
	}

	return cfg, nil
}

// NewConfigFromDSN builds unified client Config from DSN input.
func NewConfigFromDSN(dsn string, defaultPath string) (*Config, error) {
	parsed, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	// Build endpoints from parsed DSN
	endpoints := make([]string, 0, 1)
	if len(parsed.Endpoints) > 0 {
		// Multi-node DSN: ws(host1:port1,host2:port2)/db
		endpoints = append(endpoints, parsed.Endpoints...)
	} else if parsed.Net != "" || parsed.Addr != "" {
		// Single-node DSN: [net](host:port)/db
		netType := parsed.Net
		if netType == "" {
			netType = "ws"
		}
		addr := parsed.Addr
		if addr == "" {
			addr = "127.0.0.1"
		}
		port := parsed.Port
		if port == 0 {
			port = common.DefaultHttpPort
		}
		endpointURL := &url.URL{
			Scheme: netType,
			Host:   net.JoinHostPort(addr, strconv.Itoa(port)),
		}
		if parsed.Token != "" {
			query := endpointURL.Query()
			query.Set("token", parsed.Token)
			endpointURL.RawQuery = query.Encode()
		}
		endpoints = append(endpoints, endpointURL.String())
	}

	cfg := NewConfig(endpoints)
	cfg.EnableCompression = parsed.EnableCompression
	cfg.User = parsed.User
	cfg.Passwd = parsed.Passwd
	cfg.Net = parsed.Net
	cfg.Addr = parsed.Addr
	cfg.Port = parsed.Port
	cfg.DbName = parsed.DbName
	cfg.Params = parsed.Params
	cfg.InterpolateParams = parsed.InterpolateParams
	cfg.Token = parsed.Token
	cfg.ReadTimeout = parsed.ReadTimeout
	cfg.WriteTimeout = parsed.WriteTimeout
	cfg.Timezone = parsed.Timezone
	cfg.TotpCode = parsed.TotpCode
	cfg.BearerToken = parsed.BearerToken
	cfg.AutoReconnect = parsed.AutoReconnect
	if parsed.ChanLength != 0 {
		cfg.ChanLength = parsed.ChanLength
	}
	if parsed.ReconnectIntervalMs != 0 {
		cfg.ReconnectIntervalMs = parsed.ReconnectIntervalMs
	}
	if parsed.ReconnectRetryCount != 0 {
		cfg.ReconnectRetryCount = parsed.ReconnectRetryCount
	}

	if err = cfg.Normalize(defaultPath); err != nil {
		return nil, err
	}
	return cfg, nil
}

// parseDSNParams parses query params and maps known keys into Config fields.
func parseDSNParams(cfg *Config, params string) error {
	for _, v := range strings.Split(params, "&") {
		param := strings.SplitN(v, "=", 2)
		if len(param) != 2 {
			continue
		}

		key := param[0]
		value := param[1]
		switch key {
		case "interpolateParams":
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				return newInvalidDSNErrorf("invalid bool value: %s", value)
			}
			cfg.InterpolateParams = parsed
		case "token":
			cfg.Token = value
		case "enableCompression":
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				return newInvalidDSNErrorf("invalid enableCompression value: %s", value)
			}
			cfg.EnableCompression = parsed
		case "readTimeout":
			parsed, err := time.ParseDuration(value)
			if err != nil {
				return newInvalidDSNErrorf("invalid duration value: %s", value)
			}
			cfg.ReadTimeout = parsed
		case "writeTimeout":
			parsed, err := time.ParseDuration(value)
			if err != nil {
				return newInvalidDSNErrorf("invalid duration value: %s", value)
			}
			cfg.WriteTimeout = parsed
		case "timezone":
			unescapedValue, err := url.QueryUnescape(value)
			if err != nil {
				return newInvalidDSNErrorf("can not unescape timezone value: %s, %s", value, err.Error())
			}
			cfg.Timezone, err = common.ParseTimezone(unescapedValue)
			if err != nil {
				return newInvalidDSNErrorf("invalid timezone value: %s, %s", unescapedValue, err.Error())
			}
		case "bearerToken":
			cfg.BearerToken = value
		case "totpCode":
			cfg.TotpCode = value
		case "autoReconnect":
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				return newInvalidDSNErrorf("invalid autoReconnect value: %s", value)
			}
			cfg.AutoReconnect = parsed
		case "chanLength":
			parsed, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return newInvalidDSNErrorf("invalid chanLength value: %s", value)
			}
			cfg.ChanLength = uint(parsed)
		case "reconnectIntervalMs":
			parsed, err := strconv.Atoi(value)
			if err != nil {
				return newInvalidDSNErrorf("invalid reconnectIntervalMs value: %s", value)
			}
			cfg.ReconnectIntervalMs = parsed
		case "reconnectRetryCount":
			parsed, err := strconv.Atoi(value)
			if err != nil {
				return newInvalidDSNErrorf("invalid reconnectRetryCount value: %s", value)
			}
			cfg.ReconnectRetryCount = parsed
		default:
			if cfg.Params == nil {
				cfg.Params = make(map[string]string)
			}
			unescapedValue, err := url.QueryUnescape(value)
			if err != nil {
				return err
			}
			cfg.Params[key] = unescapedValue
		}
	}
	return nil
}

// parseDSNAddressList parses comma-separated host:port tuples used in DSN net section.
func parseDSNAddressList(raw string) ([]dsnAddress, error) {
	items := strings.Split(raw, ",")
	if len(items) == 0 {
		return nil, ErrInvalidDSNAddr
	}
	addrs := make([]dsnAddress, 0, len(items))
	for i := 0; i < len(items); i++ {
		item := strings.TrimSpace(items[i])
		if item == "" {
			return nil, ErrInvalidDSNAddr
		}
		host, portStr, err := net.SplitHostPort(item)
		if err != nil {
			return nil, ErrInvalidDSNAddr
		}
		port := 0
		if len(portStr) != 0 {
			port, err = strconv.Atoi(portStr)
			if err != nil {
				return nil, ErrInvalidDSNPort
			}
		}
		addrs = append(addrs, dsnAddress{
			host: host,
			port: port,
		})
	}
	return addrs, nil
}

// tryUnescape returns query-unescaped string, or original value on decode failure.
func tryUnescape(s string) string {
	if res, err := url.QueryUnescape(s); err == nil {
		return res
	}
	return s
}
