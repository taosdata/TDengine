package unified

// Connector builds and connects unified clients from normalized config input.
type Connector struct {
	cfg         Config
	defaultPath string
}

// NewConnector creates a Connector from Config.
func NewConnector(cfg *Config, defaultPath string) (*Connector, error) {
	if cfg == nil {
		return nil, ErrNilConfig
	}
	copyCfg := *cfg
	copyCfg.Endpoints = append([]string(nil), cfg.Endpoints...)
	if err := copyCfg.Normalize(defaultPath); err != nil {
		return nil, err
	}
	return &Connector{
		cfg:         copyCfg,
		defaultPath: defaultPath,
	}, nil
}

// NewConnectorFromDSN creates a Connector from DSN.
func NewConnectorFromDSN(dsn string, defaultPath string) (*Connector, error) {
	cfg, err := NewConfigFromDSN(dsn, defaultPath)
	if err != nil {
		return nil, err
	}
	return NewConnector(cfg, defaultPath)
}

// Config returns connector config snapshot.
func (c *Connector) Config() Config {
	if c == nil {
		return Config{}
	}
	copyCfg := c.cfg
	copyCfg.Endpoints = append([]string(nil), c.cfg.Endpoints...)
	return copyCfg
}

// Connect creates and connects a unified client.
func (c *Connector) Connect() (*Client, error) {
	if c == nil {
		return nil, ErrNilConfig
	}
	cfg := c.cfg
	cfg.Endpoints = append([]string(nil), c.cfg.Endpoints...)
	client, err := NewClient(&cfg, c.defaultPath)
	if err != nil {
		return nil, err
	}
	if err = client.Connect(); err != nil {
		client.Close()
		return nil, err
	}
	return client, nil
}
