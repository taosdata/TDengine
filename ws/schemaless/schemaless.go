package schemaless

import (
	"errors"
	"fmt"
	"net/url"
	"sync"

	"github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified"
)

const (
	// Deprecated: use unified.InfluxDBLineProtocol from package ws/unified instead.
	InfluxDBLineProtocol = unified.InfluxDBLineProtocol
	// Deprecated: use unified.OpenTSDBTelnetLineProtocol from package ws/unified instead.
	OpenTSDBTelnetLineProtocol = unified.OpenTSDBTelnetLineProtocol
	// Deprecated: use unified.OpenTSDBJsonFormatProtocol from package ws/unified instead.
	OpenTSDBJsonFormatProtocol = unified.OpenTSDBJsonFormatProtocol
)

// Schemaless provides schemaless insert operations with auto-reconnect.
// It wraps unified.Client to provide backward-compatible API.
// Deprecated: use unified.Client from package ws/unified instead.
type Schemaless struct {
	client *unified.Client
	once   sync.Once
}

// NewSchemaless creates a new Schemaless instance with a single endpoint.
// Deprecated: use unified.NewClient from package ws/unified instead.
func NewSchemaless(config *Config) (*Schemaless, error) {
	if config == nil {
		return nil, errors.New("nil config")
	}

	// Parse and validate URL
	wsUrl, err := url.Parse(config.url)
	if err != nil {
		return nil, fmt.Errorf("config url error: %s", err)
	}
	if wsUrl.Scheme != "ws" && wsUrl.Scheme != "wss" {
		return nil, errors.New("config url scheme error")
	}

	// Build unified config with single endpoint
	unifiedCfg := &unified.Config{
		Endpoints:           []string{config.url},
		DbName:              config.db,
		User:                config.user,
		Passwd:              config.password,
		BearerToken:         config.bearerToken,
		TotpCode:            config.totpCode,
		ReadTimeout:         config.readTimeout,
		WriteTimeout:        config.writeTimeout,
		EnableCompression:   config.enableCompression,
		ChanLength:          config.chanLength,
		AutoReconnect:       config.autoReconnect,
		ReconnectIntervalMs: config.reconnectIntervalMs,
		ReconnectRetryCount: config.reconnectRetryCount,
	}

	// Create unified client
	wsClient, err := unified.NewClient(unifiedCfg, "/ws")
	if err != nil {
		return nil, err
	}

	// Set error handler if provided
	if config.errorHandler != nil {
		wsClient.SetErrorHandler(config.errorHandler)
	}

	// Connect for schemaless
	if err := wsClient.Connect(); err != nil {
		wsClient.Close()
		return nil, mapUnifiedError(err)
	}

	return &Schemaless{
		client: wsClient,
	}, nil
}

// Insert sends a schemaless insert request with auto-reconnect.
// Deprecated: use (*unified.Client).SchemalessInsert instead.
func (s *Schemaless) Insert(lines string, protocol int, precision string, ttl int, reqID int64) error {
	return mapUnifiedError(s.client.SchemalessInsert(reqID, lines, protocol, precision, ttl, ""))
}

// Close closes the schemaless connector and all underlying connections.
// Deprecated: use (*unified.Client).Close instead.
func (s *Schemaless) Close() {
	s.once.Do(func() {
		s.client.Close()
	})
}

var (
	// Deprecated: use unified error types from package ws/unified instead.
	//revive:disable-next-line
	ConnectTimeoutErr = errors.New("schemaless connect timeout")
	// Deprecated: use unified errors from package ws/unified instead.
	//revive:disable-next-line
	SchemalessClosedErr = errors.New("connection closed")
)

func mapUnifiedError(err error) error {
	if err == nil {
		return nil
	}
	if unified.IsErrorType(err, unified.ErrorTypeConnectTimeout) {
		return ConnectTimeoutErr
	}
	if unified.IsConnectionDisconnectedError(err) || errors.Is(err, client.ClosedError) {
		return SchemalessClosedErr
	}
	return err
}
