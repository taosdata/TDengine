package config

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"strings"

	"github.com/BurntSushi/toml"
)

const opcSchema = "opc.tcp"

const (
	OpcTypeUA          = "opcua"
	OpcTypeDA          = "opcda"
	OpcUaObserveType   = "observe"
	OpcUaSubscribeType = "subscribe"
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
	Endpoint        string   `json:"endpoint,omitempty" yaml:"endpoint" toml:"endpoint"`                         // opc endpoint, such as `opc.tcp://localhost:4840`
	ConnectTimeout  int64    `json:"connect_timeout,omitempty" yaml:"connect_timeout" toml:"connect_timeout"`    // timeout for connect to endpoint in second
	RequestTimeout  int64    `json:"request_timeout,omitempty" yaml:"request_timeout" toml:"request_timeout"`    // timeout for a request in second
	SecurityPolicy  string   `json:"security_policy,omitempty" yaml:"security_policy" toml:"security_policy"`    // Security policy, one of `None`, `Basic128Rsa15`, `Basic256`, `Basic256Sha256`, `Aes128_Sha256_RsaOaep`, `Aes256_Sha256_RsaPss`
	SecurityMode    string   `json:"security_mode,omitempty" yaml:"security_mode" toml:"security_mode"`          // Security mode, one of `None`, `Sign`, `SignAndEncrypt`
	Certificate     string   `json:"certificate,omitempty" yaml:"certificate" toml:"certificate"`                // Path to cert.pem. Required when security mode or policy isn't `None`
	PrivateKey      string   `json:"private_key,omitempty" yaml:"private_key" toml:"private_key"`                // Path to private key.pem. Required when security mode or policy isn't `None`
	AuthMethod      string   `json:"auth_method,omitempty" yaml:"auth_method" toml:"auth_method"`                // authentication Method, one of `Certificate`, `Username`, or `Anonymous`
	Username        string   `json:"user_name,omitempty" yaml:"username" toml:"username"`                        // Required for auth_method = "Username"
	Password        string   `json:"password,omitempty" yaml:"password" toml:"password"`                         // Required for auth_method = "Username"
	AuthCertificate string   `json:"auth_certificate,omitempty" yaml:"auth_certificate" toml:"auth_certificate"` // Required for auth_method = "Certificate"
	AuthPrivateKey  string   `json:"auth_private_key,omitempty" yaml:"auth_private_key" toml:"auth_private_key"` // Required for auth_method = "Certificate"
	MaxAge          *float64 `json:"max_age,omitempty" yaml:"max_age" toml:"max_age"`                            // MaxAge is the maximum age of the value to be read in milliseconds. If the server has no value within this time, it returns a Bad_Timeout.
}

type DaConnectConfig struct {
	Server string   `json:"server,omitempty" yaml:"server" toml:"server"` // opc server name
	Nodes  []string `json:"nodes,omitempty" yaml:"nodes" toml:"nodes"`    // nodes to collect
}

// PointsConfig is used for collecting points
type PointsConfig struct {
	Limit int            `json:"limit,omitempty" yaml:"limit" toml:"limit"`
	Regex string         `json:"regex,omitempty" yaml:"regex" toml:"regex"`
	Ua    UaPointsConfig `json:"ua,omitempty" yaml:"ua" toml:"ua"`
	Da    DaPointsConfig `json:"da,omitempty" yaml:"da" toml:"da"`
}

type UaPointsConfig struct {
	Root       string   `json:"root,omitempty" yaml:"root" toml:"root"` // root path for points, default is 'i=85'
	Namespaces []uint16 `json:"namespaces,omitempty" yaml:"namespaces" toml:"namespaces"`
}

type DaPointsConfig struct {
	AccessPath []string `json:"access_path,omitempty" yaml:"access_path" toml:"access_path"`
}

type CollectConfig struct {
	Interval    int64           `json:"interval,omitempty" yaml:"interval" toml:"interval"`
	ContainsBad bool            `json:"contains_bad,omitempty" yaml:"contains_bad" toml:"contains_bad"`
	Dump        DumpConfig      `json:"dump,omitempty" yaml:"dump" toml:"dump"`
	Ua          UaCollectConfig `json:"ua,omitempty" yaml:"ua" toml:"ua"`
	Da          DaCollectConfig `json:"da,omitempty" yaml:"da" toml:"da"`
}

type DumpConfig struct {
	Enable bool   `json:"enable,omitempty" yaml:"enable" toml:"enable"`
	Path   string `json:"path,omitempty" yaml:"path" toml:"path"`
	Keep   int64  `json:"keep,omitempty" yaml:"keep" toml:"keep"`
}

type UaCollectConfig struct {
	CollectMode string       `json:"collect_mode,omitempty" yaml:"collect_mode" toml:"collect_mode"` // collect mode, one of `read` or `subscribe`
	Nodes       []NodeConfig `json:"nodes,omitempty" yaml:"nodes" toml:"nodes"`
}

type DaCollectConfig struct {
	Tags []TagConfig `json:"tags,omitempty" yaml:"tags" toml:"tags"`
}

type NodeConfig struct {
	ID string `json:"id,omitempty" yaml:"id" toml:"id"` // namespace=?;identifierType=identifier, ns=2;i=2. node_id for ua
}

type TagConfig struct {
	Tag string `json:"tag,omitempty" yaml:"tag" toml:"tag"` // tag for opcda
}

type ReportConfig struct {
	Remote       string `json:"remote,omitempty" yaml:"remote" toml:"remote"` //  taosx's address. ip:port
	Concurrent   int    `json:"concurrent,omitempty" yaml:"concurrent" toml:"concurrent"`
	BatchSize    int    `json:"batch_size,omitempty" yaml:"batch_size" toml:"batch_size"`
	BatchTimeout int64  `json:"batch_timeout,omitempty" yaml:"batch_timeout" toml:"batch_timeout"`
}

func ParseConfig(file string) (Config, error) {
	bs, err := ioutil.ReadFile(file)
	if err != nil {
		return Config{}, fmt.Errorf("read config error, file:%s, err: %v", file, err)
	}
	return ParseConfigBs(bs)
}
func ParseConfigBs(bs []byte) (Config, error) {
	var config Config
	_, err := toml.NewDecoder(bytes.NewBuffer(bs)).Decode(&config)
	if err != nil {
		return config, fmt.Errorf("parse config error %v", err)
	}
	return config, nil
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
	if !Contains(policies, c.SecurityPolicy) {
		return fmt.Errorf("invalid security policy %q", c.SecurityPolicy)
	}
	return nil
}

var modes = []string{"None", "Sign", "SignAndEncrypt"}

func (c *UaConnectConfig) validateSecurityMode() error {
	if !Contains(modes, c.SecurityMode) {
		return fmt.Errorf("invalid security type %q", c.SecurityMode)
	}
	if c.SecurityMode != "None" && (len(c.Certificate) == 0 || len(c.PrivateKey) == 0) {
		return errors.New("certificate and private_key is required if security mode is not `None`")
	}
	return nil
}

var authMethods = []string{"certificate", "username", "anonymous"}

func (c *UaConnectConfig) validateAuthMethod() error {
	if !Contains(authMethods, strings.ToLower(c.AuthMethod)) {
		return fmt.Errorf("invalid auth method %q", c.AuthMethod)
	}
	if strings.ToLower(c.AuthMethod) == "username" && (len(c.Username) == 0 || len(c.Password) == 0) {
		return errors.New("user name and password is required for `Username` auth method")
	}
	if strings.ToLower(c.AuthMethod) == "certificate" && (len(c.AuthCertificate) == 0 || len(c.AuthCertificate) == 0) {
		return errors.New("auth_certificate and auth_private_key is required for `Certificate` auth method")
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

func (c *DaCollectConfig) Validate() error {
	if len(c.Tags) == 0 {
		return fmt.Errorf("tags is null")
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
		r.BatchSize = 1000
	}
	if r.BatchTimeout == 0 {
		r.BatchTimeout = 2
	}
	return nil
}

func (c *DumpConfig) Validate() error {
	if c.Enable && len(c.Path) == 0 {
		return fmt.Errorf("dump path is null")
	}
	if c.Keep == 0 {
		c.Keep = 7
	}
	return nil
}

func (c *PointsConfig) Validate() {
	if len(c.Ua.Root) == 0 {
		c.Ua.Root = "i=85"
	}
}

func (c *Config) ValidateConnect() error {
	switch c.OpcType {
	case OpcTypeDA:
		return c.Connect.Da.Validate()
	case OpcTypeUA:
		return c.Connect.Ua.Validate()
	default:
		return fmt.Errorf("opc type %s is not support", c.OpcType)
	}
}

func (c *Config) ValidateGetPoints() error {
	switch c.OpcType {
	case OpcTypeDA:
		return c.Connect.Da.Validate()
	case OpcTypeUA:
		c.Points.Validate()
		return c.Connect.Ua.Validate()
	default:
		return fmt.Errorf("opc type %s is not support", c.OpcType)
	}
}

func (c *Config) ValidateCollect() error {
	var errs []error
	switch c.OpcType {
	case OpcTypeDA:
		err := c.Connect.Da.Validate()
		if err != nil {
			errs = append(errs, err)
		}
		err = c.Report.Validate()
		if err != nil {
			errs = append(errs, err)
		}
		err = c.Collect.Da.Validate()
		if err != nil {
			errs = append(errs, err)
		}
	case OpcTypeUA:
		err := c.Collect.Ua.Validate()
		if err != nil {
			errs = append(errs, err)
		}
		err = c.Report.Validate()
		if err != nil {
			errs = append(errs, err)
		}
		err = c.Collect.Ua.Validate()
		if err != nil {
			errs = append(errs, err)
		}
	default:
		return fmt.Errorf("opc type %s is not support", c.OpcType)
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}
