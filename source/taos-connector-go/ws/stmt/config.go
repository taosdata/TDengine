package stmt

import (
	"errors"
	"strings"
	"time"
)

// Deprecated: use unified.Config from package ws/unified instead.
type Config struct {
	Url                 string
	ChanLength          uint
	MessageTimeout      time.Duration
	WriteWait           time.Duration
	Timezone            *time.Location
	ErrorHandler        func(connector *Connector, err error)
	CloseHandler        func()
	User                string
	Password            string
	DB                  string
	EnableCompression   bool
	AutoReconnect       bool
	ReconnectIntervalMs int
	ReconnectRetryCount int
	TotpCode            string
	BearerToken         string
}

// Deprecated: use unified.Config from package ws/unified instead.
func NewConfig(url string, chanLength uint) *Config {
	return &Config{
		Url:                 url,
		ChanLength:          chanLength,
		ReconnectRetryCount: 3,
		ReconnectIntervalMs: 2000,
	}
}

// Deprecated: use unified.Config from package ws/unified instead.
func (c *Config) SetConnectUser(user string) error {
	c.User = user
	return nil
}

// Deprecated: use unified.Config from package ws/unified instead.
func (c *Config) SetConnectPass(pass string) error {
	c.Password = pass
	return nil
}

// Deprecated: use unified.Config from package ws/unified instead.
func (c *Config) SetConnectDB(db string) error {
	c.DB = db
	return nil
}

// Deprecated: use unified.Config from package ws/unified instead.
func (c *Config) SetConnectionTimezone(timezone string) error {
	if timezone == "" {
		return errors.New("invalid timezone value: empty string")
	}
	if strings.ToLower(timezone) == "local" {
		return errors.New("invalid timezone value: 'local'")
	}
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		return errors.New("invalid timezone value: " + timezone + ", " + err.Error())
	}
	c.Timezone = loc
	return nil
}

// Deprecated: use unified.Config from package ws/unified instead.
func (c *Config) SetMessageTimeout(timeout time.Duration) error {
	if timeout < time.Second {
		return errors.New("message timeout cannot be less than 1 second")
	}
	c.MessageTimeout = timeout
	return nil
}

// Deprecated: use unified.Config from package ws/unified instead.
func (c *Config) SetWriteWait(writeWait time.Duration) error {
	if writeWait < 0 {
		return errors.New("write wait cannot be less than 0")
	}
	c.WriteWait = writeWait
	return nil
}

// Deprecated: use unified.Config from package ws/unified instead.
func (c *Config) SetErrorHandler(f func(connector *Connector, err error)) {
	c.ErrorHandler = f
}

// Deprecated: use unified.Config from package ws/unified instead.
func (c *Config) SetCloseHandler(f func()) {
	c.CloseHandler = f
}

// Deprecated: use unified.Config from package ws/unified instead.
func (c *Config) SetEnableCompression(enableCompression bool) {
	c.EnableCompression = enableCompression
}

// Deprecated: use unified.Config from package ws/unified instead.
func (c *Config) SetAutoReconnect(reconnect bool) {
	c.AutoReconnect = reconnect
}

// Deprecated: use unified.Config from package ws/unified instead.
func (c *Config) SetReconnectIntervalMs(reconnectIntervalMs int) {
	c.ReconnectIntervalMs = reconnectIntervalMs
}

// Deprecated: use unified.Config from package ws/unified instead.
func (c *Config) SetReconnectRetryCount(reconnectRetryCount int) {
	c.ReconnectRetryCount = reconnectRetryCount
}

// Deprecated: use unified.Config from package ws/unified instead.
func (c *Config) SetTotpCode(totpCode string) {
	c.TotpCode = totpCode
}

// Deprecated: use unified.Config from package ws/unified instead.
func (c *Config) SetBearerToken(bearerToken string) {
	c.BearerToken = bearerToken
}
