package schemaless

import (
	"time"
)

// Deprecated: use unified.Config from package ws/unified instead.
type Config struct {
	url                 string
	chanLength          uint
	user                string
	password            string
	db                  string
	totpCode            string
	bearerToken         string
	readTimeout         time.Duration
	writeTimeout        time.Duration
	errorHandler        func(error)
	enableCompression   bool
	autoReconnect       bool
	reconnectIntervalMs int
	reconnectRetryCount int
}

// Deprecated: use unified.Config from package ws/unified instead.
func NewConfig(url string, chanLength uint, opts ...func(*Config)) *Config {
	c := Config{url: url, chanLength: chanLength, reconnectRetryCount: 3, reconnectIntervalMs: 2000}
	for _, opt := range opts {
		opt(&c)
	}

	return &c
}

// Deprecated: use unified.Config from package ws/unified instead.
func SetUser(user string) func(*Config) {
	return func(c *Config) {
		c.user = user
	}
}

// Deprecated: use unified.Config from package ws/unified instead.
func SetPassword(password string) func(*Config) {
	return func(c *Config) {
		c.password = password
	}
}

// Deprecated: use unified.Config from package ws/unified instead.
func SetDb(db string) func(*Config) {
	return func(c *Config) {
		c.db = db
	}
}

// Deprecated: use unified.Config from package ws/unified instead.
func SetReadTimeout(readTimeout time.Duration) func(*Config) {
	return func(c *Config) {
		c.readTimeout = readTimeout
	}
}

// Deprecated: use unified.Config from package ws/unified instead.
func SetWriteTimeout(writeTimeout time.Duration) func(*Config) {
	return func(c *Config) {
		c.writeTimeout = writeTimeout
	}
}

// Deprecated: use unified.Config from package ws/unified instead.
func SetErrorHandler(errorHandler func(error)) func(*Config) {
	return func(c *Config) {
		c.errorHandler = errorHandler
	}
}

// Deprecated: use unified.Config from package ws/unified instead.
func SetEnableCompression(enableCompression bool) func(*Config) {
	return func(c *Config) {
		c.enableCompression = enableCompression
	}
}

// Deprecated: use unified.Config from package ws/unified instead.
func SetAutoReconnect(reconnect bool) func(*Config) {
	return func(c *Config) {
		c.autoReconnect = reconnect
	}
}

// Deprecated: use unified.Config from package ws/unified instead.
func SetReconnectIntervalMs(reconnectIntervalMs int) func(*Config) {
	return func(c *Config) {
		c.reconnectIntervalMs = reconnectIntervalMs
	}
}

// Deprecated: use unified.Config from package ws/unified instead.
func SetReconnectRetryCount(reconnectRetryCount int) func(*Config) {
	return func(c *Config) {
		c.reconnectRetryCount = reconnectRetryCount
	}
}

// Deprecated: use unified.Config from package ws/unified instead.
func SetTOTPCode(totpCode string) func(*Config) {
	return func(c *Config) {
		c.totpCode = totpCode
	}
}

// Deprecated: use unified.Config from package ws/unified instead.
func SetBearerToken(bearerToken string) func(*Config) {
	return func(c *Config) {
		c.bearerToken = bearerToken
	}
}
