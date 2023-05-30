package common

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

const (
	OpcTypeUA          = "opcua"
	OpcTypeDA          = "opcda"
	OpcTypeFake        = "fake"
	OpcUaObserveType   = "observe"
	OPcUaSubscribeType = "subscribe"
)

type Config struct {
	OpcType string        `json:"opc_type,omitempty" yaml:"opc_type" toml:"opc_type"` // metric type, `opcua` or `opcda`(only for windows)
	Debug   bool          `json:"debug,omitempty" yaml:"debug" toml:"debug"`          // debug mode, default is false. only for debug, should be false in production
	Connect ConnectConfig `json:"connect,omitempty" yaml:"connect" toml:"connect"`
	Points  PointsConfig  `json:"points,omitempty" yaml:"points" toml:"points"`
	Collect CollectConfig `json:"collect,omitempty" yaml:"collect" toml:"collect"`
	Report  ReportConfig  `json:"report,omitempty" yaml:"report" toml:"report"`
}

type ConnectConfig struct {
	Ua UaConnectConfig `json:"ua,omitempty" yaml:"ua" toml:"ua"`
	Da DaConnectConfig `json:"da,omitempty" yaml:"da" toml:"da"`
}

type UaConnectConfig struct {
	Endpoint       string `json:"endpoint,omitempty" yaml:"endpoint" toml:"endpoint"`                      // opc endpoint, such as `opc.tcp://localhost:4840`
	ConnectTimeout int64  `json:"connect_timeout,omitempty" yaml:"connect_timeout" toml:"connect_timeout"` // timeout for connect to endpoint in second
	RequestTimeout int64  `json:"request_timeout,omitempty" yaml:"request_timeout" toml:"request_timeout"` // timeout for a request in second
	SecurityPolicy string `json:"security_policy,omitempty" yaml:"security_policy" toml:"security_policy"` // Security policy, one of `None`, `Basic128Rsa15`, `Basic256`, `Basic256Sha256`, `Aes128_Sha256_RsaOaep`, `Aes256_Sha256_RsaPss`
	SecurityMode   string `json:"security_mode,omitempty" yaml:"security_mode" toml:"security_mode"`       // Security mode, one of `None`, `Sign`, `SignAndEncrypt`
	Certificate    string `json:"certificate,omitempty" yaml:"certificate" toml:"certificate"`             // Path to cert.pem. Required when security mode or policy isn't `None`
	PrivateKey     string `json:"private_key,omitempty" yaml:"private_key" toml:"private_key"`             // Path to private key.pem. Required when security mode or policy isn't `None`
	AuthMethod     string `json:"auth_method,omitempty" yaml:"auth_method" toml:"auth_method"`             // authentication Method, one of `Certificate`, `Username`, or `Anonymous`
	Username       string `json:"user_name,omitempty" yaml:"username" toml:"username"`                     // Required for auth_method = "Username"
	Password       string `json:"password,omitempty" yaml:"password" toml:"password"`                      // Required for auth_method = "Username"
}

type DaConnectConfig struct {
	Server string   `json:"server,omitempty" yaml:"server" toml:"server"` // opc server name
	Nodes  []string `json:"nodes,omitempty" yaml:"nodes" toml:"nodes"`    // nodes to collect
}

// PointsConfig is used for collecting points
type PointsConfig struct {
	Limit int    `json:"limit,omitempty" yaml:"limit" toml:"limit"`
	Regex string `json:"regex,omitempty" yaml:"regex" toml:"regex"`
}

type CollectConfig struct {
	Interval    int64           `json:"interval,omitempty" yaml:"interval" toml:"interval"`
	Limit       int             `json:"limit,omitempty" yaml:"limit" toml:"limit"`
	ContainsBad bool            `json:"contains_bad,omitempty" yaml:"contains_bad" toml:"contains_bad"`
	Ua          UaCollectConfig `json:"ua,omitempty" yaml:"ua" toml:"ua"`
	Da          DaCollectConfig `json:"da,omitempty" yaml:"da" toml:"da"`
}

type UaCollectConfig struct {
	CollectMode string       `json:"collect_mode,omitempty" yaml:"collect_mode" toml:"collect_mode"` // collect mode, one of `read` or `subscribe`
	Nodes       []NodeConfig `json:"nodes,omitempty" yaml:"nodes" toml:"nodes"`
}

type DaCollectConfig struct {
	Tags []TagConfig `json:"tags,omitempty" yaml:"tags" toml:"tags"`
}

type NodeConfig struct {
	ID        string `json:"id,omitempty" yaml:"id" toml:"id"` // namespace=?;identifierType=identifier, ns=2;i=2. node_id for ua
	ValueType string `json:"value_type,omitempty" yaml:"value_type" toml:"value_type"`
}

type TagConfig struct {
	Tag       string `json:"tag,omitempty" yaml:"tag" toml:"tag"` // tag for opcda
	ValueType string `json:"value_type,omitempty" yaml:"value_type" toml:"value_type"`
}

type ReportConfig struct {
	Remote       string `json:"remote,omitempty" yaml:"remote" toml:"remote"` //  taosx's address. ip:port
	Concurrent   int    `json:"concurrent,omitempty" yaml:"concurrent" toml:"concurrent"`
	BatchSize    int    `json:"batch_size,omitempty" yaml:"batch_size" toml:"batch_size"`
	BatchTimeout int64  `json:"batch_timeout,omitempty" yaml:"batch_timeout" toml:"batch_timeout"`
}

func ParseConfig(path string) (config Config, err error) {
	if _, err = toml.DecodeFile(path, &config); err != nil {
		err = fmt.Errorf("parse config error %v", err)
	}
	return
}

func (c *UaConnectConfig) Validate() (err error) {
	if err = c.validateEndpoint(); err != nil {
		return err
	}
	if err = c.validateSecurityPolicy(); err != nil {
		return err
	}
	if err = c.validateSecurityMode(); err != nil {
		return err
	}
	if err = c.validateAuthMethod(); err != nil {
		return err
	}

	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = 10
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = 10
	}
	return nil
}

func (c *UaConnectConfig) validateEndpoint() error {
	if c.Endpoint == "" {
		return fmt.Errorf("endpoint url is empty")
	}

	u, err := url.Parse(c.Endpoint)
	if err != nil {
		return fmt.Errorf("endpoint url is invalid")
	}

	if u.Scheme != opcSchema {
		return fmt.Errorf("unsupported scheme %q in endpoint. Expected opc.tcp", u.Scheme)
	}

	return nil
}

var policies = []string{"None", "Basic128Rsa15", "Basic256", "Basic256Sha256", "Aes128_Sha256_RsaOaep",
	"Aes256_Sha256_RsaPss"}

func (c *UaConnectConfig) validateSecurityPolicy() error {
	if !InSlice[string](c.SecurityPolicy, policies) {
		return fmt.Errorf("invalid security policy %q", c.SecurityPolicy)
	}
	if c.SecurityPolicy != "None" && (len(c.Certificate) == 0 || len(c.PrivateKey) == 0) {
		return errors.New("certificate and privateKey is required if security policy is not `None`")
	}
	return nil
}

var modes = []string{"None", "Sign", "SignAndEncrypt"}

func (c *UaConnectConfig) validateSecurityMode() error {
	if !InSlice(c.SecurityMode, modes) {
		return fmt.Errorf("invalid security type %q", c.SecurityMode)
	}
	if c.SecurityMode != "None" && (len(c.Certificate) == 0 || len(c.PrivateKey) == 0) {
		return errors.New("certificate and privateKey is required if security mode is not `None`")
	}
	return nil
}

var authMethods = []string{"certificate", "username", "anonymous"}

func (c *UaConnectConfig) validateAuthMethod() error {
	if !InSlice[string](strings.ToLower(c.AuthMethod), authMethods) {
		return fmt.Errorf("invalid auth method %q", c.AuthMethod)
	}
	if strings.ToLower(c.AuthMethod) == "username" && (len(c.Username) == 0 || len(c.Password) == 0) {
		return errors.New("user name and password is required for `Username` auth method")
	}
	return nil
}

func (d *DaConnectConfig) Validate() error {
	if d.Server == "" {
		return fmt.Errorf("opc server name is null")
	}
	if len(d.Nodes) == 0 {
		return fmt.Errorf("nodes is null")
	}
	return nil
}

func (c *UaCollectConfig) Validate() error {
	if c.CollectMode == "" {
		c.CollectMode = OpcUaObserveType
	}
	if len(c.Nodes) == 0 {
		return fmt.Errorf("nodes is null")
	}

	return nil
}

func (r *ReportConfig) Validate() error {
	if len(r.Remote) == 0 {
		return fmt.Errorf("config error. taosx's address is null")
	}
	if r.Concurrent == 0 {
		r.Concurrent = 1
	}
	if r.BatchSize == 0 {
		r.BatchSize = 1
	}
	if r.BatchTimeout == 0 {
		r.BatchTimeout = 2
	}
	return nil
}

type NodeValue struct {
	Identifier string    `json:"identifier,omitempty"`
	Name       string    `json:"name,omitempty"`
	Timestamp  time.Time `json:"timestamp,omitempty"`
	Now        time.Time `json:"now,omitempty"`
	Value      any       `json:"value,omitempty"`
	ValueType  ValueType `json:"value_type,omitempty"`
	Status     int64     `json:"status,omitempty"`
}

type Point struct {
	ID   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}
