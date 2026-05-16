package unified

import (
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/taosdata/driver-go/v3/common"
)

// Config is the shared websocket config used by the unified package.
// Existing public packages should adapt their input into this struct.
type Config struct {
	// Unified runtime fields
	Endpoints           []string
	ChanLength          uint
	AutoReconnect       bool
	ReconnectIntervalMs int
	ReconnectRetryCount int

	// Backward compatibility fields (deprecated, use Endpoints instead)
	// These are only used during DSN parsing and converted to Endpoints
	Net               string // "ws" or "wss"
	Addr              string
	Port              int
	DbName            string
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	EnableCompression bool

	User              string
	Passwd            string
	Params            map[string]string
	InterpolateParams bool
	Token             string
	BearerToken       string
	TotpCode          string
	Timezone          *time.Location
}

// NewConfig creates a new Config with default timeout/reconnect behavior and copied endpoints.
func NewConfig(endpoints []string) *Config {
	copyEndpoints := make([]string, len(endpoints))
	copy(copyEndpoints, endpoints)
	conf := &Config{
		InterpolateParams:   true,
		Endpoints:           copyEndpoints,
		ChanLength:          1,
		ReadTimeout:         common.DefaultMessageTimeout,
		WriteTimeout:        common.DefaultWriteWait,
		ReconnectRetryCount: 3,
		ReconnectIntervalMs: 2000,
	}
	return conf
}

// Normalize normalizes endpoints and fills defaults.
// It also handles backward compatibility by converting Addr/Port to Endpoints if needed.
func (c *Config) Normalize(defaultPath string) error {
	// Backward compatibility: convert Addr/Port to Endpoints if Endpoints is empty
	if len(c.Endpoints) == 0 && c.Addr != "" {
		netType := c.Net
		if netType == "" {
			netType = "ws"
		}
		addr := normalizeHostForJoinHostPort(c.Addr)
		port := c.Port
		if port == 0 {
			port = common.DefaultHttpPort
		}
		endpointURL := &url.URL{
			Scheme: netType,
			Host:   net.JoinHostPort(addr, strconv.Itoa(port)),
		}
		if c.Token != "" {
			query := endpointURL.Query()
			query.Set("token", c.Token)
			endpointURL.RawQuery = query.Encode()
		}
		c.Endpoints = []string{endpointURL.String()}
	}

	endpoints, err := NormalizeEndpoints(c.Endpoints, defaultPath)
	if err != nil {
		return err
	}
	c.Endpoints = endpoints
	if c.ReadTimeout <= 0 {
		c.ReadTimeout = common.DefaultMessageTimeout
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = common.DefaultWriteWait
	}
	if c.ReconnectRetryCount <= 0 {
		c.ReconnectRetryCount = 3
	}
	if c.ReconnectIntervalMs <= 0 {
		c.ReconnectIntervalMs = 2000
	}
	return nil
}

func normalizeHostForJoinHostPort(host string) string {
	// net.JoinHostPort expects IPv6 literals without brackets.
	if len(host) >= 2 && strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		return host[1 : len(host)-1]
	}
	return host
}

// NormalizeEndpoints validates ws/wss endpoints, applies default path, and deduplicates.
func NormalizeEndpoints(endpoints []string, defaultPath string) ([]string, error) {
	if len(endpoints) == 0 {
		return nil, ErrNoEndpoints
	}
	if defaultPath == "" {
		defaultPath = "/ws"
	}
	if !strings.HasPrefix(defaultPath, "/") {
		defaultPath = "/" + defaultPath
	}
	seen := make(map[string]bool)
	normalized := make([]string, 0, len(endpoints))
	for i := 0; i < len(endpoints); i++ {
		rawEndpoint := strings.TrimSpace(endpoints[i])
		if rawEndpoint == "" {
			continue
		}
		u, err := url.Parse(rawEndpoint)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return nil, newInvalidConfigErrorf("invalid websocket endpoint: %s", rawEndpoint)
		}
		scheme := strings.ToLower(u.Scheme)
		if scheme != "ws" && scheme != "wss" {
			return nil, newInvalidConfigErrorf("invalid websocket endpoint scheme: %s", rawEndpoint)
		}
		u.Scheme = scheme
		if u.Path == "" || u.Path == "/" {
			u.Path = defaultPath
		}
		normalizedURL := u.String()
		// Deduplicate endpoints
		if !seen[normalizedURL] {
			seen[normalizedURL] = true
			normalized = append(normalized, normalizedURL)
		}
	}
	if len(normalized) == 0 {
		return nil, ErrNoEndpoints
	}
	return normalized, nil
}
