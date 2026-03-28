package stmt

import (
	"errors"
	"sync"
	"time"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified"
)

// Deprecated: use unified.Client from package ws/unified instead.
type Connector struct {
	unifiedClient      *unified.Client
	config             *Config
	timezone           *time.Location
	customErrorHandler func(*Connector, error)
	customCloseHandler func()
	closed             bool
	sync.Mutex
}

var (
	// Deprecated: use unified error types from package ws/unified instead.
	//revive:disable-next-line
	ConnectTimeoutErr = errors.New("stmt connect timeout")
	// Deprecated: use unified errors from package ws/unified instead.
	ErrConnIsClosed = errors.New("stmt Connector is closed")
	// Deprecated: use unified errors from package ws/unified instead.
	ErrUnifiedClientUninitialized = errors.New("stmt unified client is not initialized")
)

// Deprecated: use unified.NewClient from package ws/unified instead.
func NewConnector(config *Config) (*Connector, error) {
	if config == nil {
		return nil, errors.New("nil config")
	}
	readTimeout := common.DefaultMessageTimeout
	writeTimeout := common.DefaultWriteWait
	if config.MessageTimeout > 0 {
		readTimeout = config.MessageTimeout
	}
	if config.WriteWait > 0 {
		writeTimeout = config.WriteWait
	}

	connector := &Connector{
		config:             config,
		customErrorHandler: config.ErrorHandler,
		customCloseHandler: config.CloseHandler,
		timezone:           config.Timezone,
	}

	unifiedCfg := &unified.Config{
		Endpoints:           []string{config.Url},
		ChanLength:          config.ChanLength,
		AutoReconnect:       config.AutoReconnect,
		ReconnectIntervalMs: config.ReconnectIntervalMs,
		ReconnectRetryCount: config.ReconnectRetryCount,
		DbName:              config.DB,
		ReadTimeout:         readTimeout,
		WriteTimeout:        writeTimeout,
		EnableCompression:   config.EnableCompression,
		User:                config.User,
		Passwd:              config.Password,
		BearerToken:         config.BearerToken,
		TotpCode:            config.TotpCode,
		Timezone:            config.Timezone,
	}
	unifiedClient, err := unified.NewClient(unifiedCfg, "/ws")
	if err != nil {
		return nil, err
	}
	unifiedClient.SetErrorHandler(func(err error) {
		connector.handleError(mapUnifiedError(err))
	})
	if err = unifiedClient.Connect(); err != nil {
		unifiedClient.Close()
		return nil, mapUnifiedError(err)
	}
	connector.unifiedClient = unifiedClient
	return connector, nil
}

func (c *Connector) handleError(err error) {
	if c.customErrorHandler != nil {
		c.customErrorHandler(c, err)
	}
}

func mapUnifiedError(err error) error {
	if err == nil {
		return nil
	}
	if unified.IsErrorType(err, unified.ErrorTypeConnectTimeout) {
		return ConnectTimeoutErr
	}
	if unified.IsConnectionDisconnectedError(err) || errors.Is(err, client.ClosedError) {
		return client.ClosedError
	}
	return err
}

func (c *Connector) isClosed() bool {
	c.Lock()
	defer c.Unlock()
	return c.closed
}

// Deprecated: use (*unified.Client).InitStmt instead.
func (c *Connector) Init() (*Stmt, error) {
	c.Lock()
	defer c.Unlock()

	if c.closed {
		return nil, ErrConnIsClosed
	}
	if c.unifiedClient == nil {
		return nil, ErrUnifiedClientUninitialized
	}
	core, err := c.unifiedClient.InitStmt(0)
	if err != nil {
		return nil, mapUnifiedError(err)
	}
	return &Stmt{
		core:      core,
		connector: c,
	}, nil
}

// Deprecated: use (*unified.Client).Close instead.
func (c *Connector) Close() error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	if c.unifiedClient != nil {
		c.unifiedClient.Close()
	}
	if c.customCloseHandler != nil {
		c.customCloseHandler()
	}
	return nil
}
