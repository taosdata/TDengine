package openmetrics

import (
	"errors"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/taosdata/taosadapter/v3/driver/common"
)

type Config struct {
	Enable                 bool
	User                   string
	Password               string
	DBs                    []string
	URLs                   []string
	ResponseTimeoutSeconds []int
	HttpUsernames          []string
	HttpPasswords          []string
	HttpBearerTokenStrings []string
	CaCertFiles            []string
	CertFiles              []string
	KeyFiles               []string
	GatherDurationSeconds  []int
	TTL                    []int
	IgnoreTimestamp        bool
	InsecureSkipVerify     bool
	Token                  string
}

func (c *Config) setValue(viper *viper.Viper) {
	c.Enable = viper.GetBool("open_metrics.enable")
	c.User = viper.GetString("open_metrics.user")
	c.Password = viper.GetString("open_metrics.password")
	c.DBs = viper.GetStringSlice("open_metrics.dbs")
	c.URLs = viper.GetStringSlice("open_metrics.urls")
	c.ResponseTimeoutSeconds = viper.GetIntSlice("open_metrics.responseTimeoutSeconds")
	c.HttpUsernames = viper.GetStringSlice("open_metrics.httpUsernames")
	c.HttpPasswords = viper.GetStringSlice("open_metrics.httpPasswords")
	c.HttpBearerTokenStrings = viper.GetStringSlice("open_metrics.httpBearerTokenStrings")
	c.CaCertFiles = viper.GetStringSlice("open_metrics.caCertFiles")
	c.CertFiles = viper.GetStringSlice("open_metrics.certFiles")
	c.KeyFiles = viper.GetStringSlice("open_metrics.keyFiles")
	c.InsecureSkipVerify = viper.GetBool("open_metrics.insecureSkipVerify")
	c.GatherDurationSeconds = viper.GetIntSlice("open_metrics.gatherDurationSeconds")
	c.TTL = viper.GetIntSlice("open_metrics.ttl")
	c.IgnoreTimestamp = viper.GetBool("open_metrics.ignoreTimestamp")
	c.Token = viper.GetString("open_metrics.token")
}

func (c *Config) CheckConfig() error {
	if c.Enable {
		urlLength := len(c.URLs)
		if urlLength == 0 {
			return errors.New("open_metrics.urls is empty")
		}
		if len(c.DBs) != urlLength {
			return errors.New("open_metrics.dbs and open_metrics.urls must have the same length")
		}
		if len(c.ResponseTimeoutSeconds) != urlLength {
			return errors.New("open_metrics.responseTimeoutSeconds and open_metrics.urls must have the same length")
		}
		if len(c.HttpUsernames) != 0 && len(c.HttpUsernames) != urlLength {
			return errors.New("open_metrics.httpUsernames and open_metrics.urls must have the same length")
		}
		if len(c.HttpPasswords) != 0 && len(c.HttpPasswords) != urlLength {
			return errors.New("open_metrics.httpPasswords and open_metrics.urls must have the same length")
		}
		if len(c.HttpUsernames) != 0 && len(c.HttpUsernames) != len(c.HttpPasswords) {
			return errors.New("open_metrics.httpUsernames and open_metrics.httpPasswords must have the same length")
		}
		if len(c.HttpBearerTokenStrings) != 0 && len(c.HttpBearerTokenStrings) != urlLength {
			return errors.New("open_metrics.httpBearerTokenStrings and open_metrics.urls must have the same length")
		}
		if len(c.CaCertFiles) != 0 && len(c.CaCertFiles) != urlLength {
			return errors.New("open_metrics.caCertFiles and open_metrics.urls must have the same length")
		}
		if len(c.CertFiles) != 0 && len(c.CertFiles) != urlLength {
			return errors.New("open_metrics.certFiles and open_metrics.urls must have the same length")
		}
		if len(c.KeyFiles) != 0 && len(c.KeyFiles) != urlLength {
			return errors.New("open_metrics.keyFiles and open_metrics.urls must have the same length")
		}
		if len(c.CertFiles) != 0 && len(c.CertFiles) != len(c.KeyFiles) {
			return errors.New("open_metrics.certFiles and open_metrics.keyFiles must have the same length")
		}
		if len(c.GatherDurationSeconds) != urlLength {
			return errors.New("open_metrics.gatherDurationSeconds and open_metrics.urls must have the same length")
		}
		if len(c.TTL) != 0 && len(c.TTL) != urlLength {
			return errors.New("open_metrics.ttl and open_metrics.urls must have the same length")
		}
	}
	return nil
}

func init() {
	// OpenMetrics feature toggle
	_ = viper.BindEnv("open_metrics.enable", "TAOS_ADAPTER_OPEN_METRICS_ENABLE")
	pflag.Bool("open_metrics.enable", false, `Enable OpenMetrics data collection. Env var: "TAOS_ADAPTER_OPEN_METRICS_ENABLE"`)
	viper.SetDefault("open_metrics.enable", false)

	// TDengine connection credentials
	_ = viper.BindEnv("open_metrics.user", "TAOS_ADAPTER_OPEN_METRICS_USER")
	pflag.String("open_metrics.user", common.DefaultUser, `TDengine username for OpenMetrics connection. Env var: "TAOS_ADAPTER_OPEN_METRICS_USER"`)
	viper.SetDefault("open_metrics.user", common.DefaultUser)

	_ = viper.BindEnv("open_metrics.password", "TAOS_ADAPTER_OPEN_METRICS_PASSWORD")
	pflag.String("open_metrics.password", common.DefaultPassword, `TDengine password for OpenMetrics connection. Env var: "TAOS_ADAPTER_OPEN_METRICS_PASSWORD"`)
	viper.SetDefault("open_metrics.password", common.DefaultPassword)

	// Database configuration
	_ = viper.BindEnv("open_metrics.dbs", "TAOS_ADAPTER_OPEN_METRICS_DBS")
	pflag.StringArray("open_metrics.dbs", []string{"open_metrics"}, `Target database names for OpenMetrics data. Env var: "TAOS_ADAPTER_OPEN_METRICS_DBS"`)
	viper.SetDefault("open_metrics.dbs", []string{"open_metrics"})

	// Endpoint configuration
	_ = viper.BindEnv("open_metrics.urls", "TAOS_ADAPTER_OPEN_METRICS_URLS")
	pflag.StringArray("open_metrics.urls", []string{"http://localhost:9100"}, `OpenMetrics endpoints to scrape (format: http://host:port). Env var: "TAOS_ADAPTER_OPEN_METRICS_URLS"`)
	viper.SetDefault("open_metrics.urls", []string{"http://localhost:9100"})

	// Timeout settings
	_ = viper.BindEnv("open_metrics.responseTimeoutSeconds", "TAOS_ADAPTER_OPEN_METRICS_RESPONSE_TIMEOUT_SECONDS")
	pflag.IntSlice("open_metrics.responseTimeoutSeconds", []int{5}, `HTTP response timeout in seconds for OpenMetrics scraping. Env var: "TAOS_ADAPTER_OPEN_METRICS_RESPONSE_TIMEOUT_SECONDS"`)
	viper.SetDefault("open_metrics.responseTimeoutSeconds", []int{5})

	// Authentication methods
	_ = viper.BindEnv("open_metrics.httpUsernames", "TAOS_ADAPTER_OPEN_METRICS_HTTP_USERNAMES")
	pflag.StringArray("open_metrics.httpUsernames", []string(nil), `Basic auth usernames for protected OpenMetrics endpoints. Env var: "TAOS_ADAPTER_OPEN_METRICS_HTTP_USERNAMES"`)
	viper.SetDefault("open_metrics.httpUsernames", []string(nil))

	_ = viper.BindEnv("open_metrics.httpPasswords", "TAOS_ADAPTER_OPEN_METRICS_HTTP_PASSWORDS")
	pflag.StringArray("open_metrics.httpPasswords", []string(nil), `Basic auth passwords for protected OpenMetrics endpoints. Env var: "TAOS_ADAPTER_OPEN_METRICS_HTTP_PASSWORDS"`)
	viper.SetDefault("open_metrics.httpPasswords", []string(nil))

	_ = viper.BindEnv("open_metrics.httpBearerTokenStrings", "TAOS_ADAPTER_OPEN_METRICS_HTTP_BEARER_TOKEN_STRINGS")
	pflag.StringArray("open_metrics.httpBearerTokenStrings", []string(nil), `Bearer tokens for OpenMetrics endpoint authentication. Env var: "TAOS_ADAPTER_OPEN_METRICS_HTTP_BEARER_TOKEN_STRINGS"`)
	viper.SetDefault("open_metrics.httpBearerTokenStrings", []string(nil))

	// TLS configuration
	_ = viper.BindEnv("open_metrics.caCertFiles", "TAOS_ADAPTER_OPEN_METRICS_CA_CERT_FILES")
	pflag.StringArray("open_metrics.caCertFiles", []string(nil), `Paths to CA certificate files for TLS verification. Env var: "TAOS_ADAPTER_OPEN_METRICS_CA_CERT_FILES"`)
	viper.SetDefault("open_metrics.caCertFiles", []string(nil))

	_ = viper.BindEnv("open_metrics.certFiles", "TAOS_ADAPTER_OPEN_METRICS_CERT_FILES")
	pflag.StringArray("open_metrics.certFiles", []string(nil), `Paths to client certificate files for mTLS. Env var: "TAOS_ADAPTER_OPEN_METRICS_CERT_FILES"`)
	viper.SetDefault("open_metrics.certFiles", []string(nil))

	_ = viper.BindEnv("open_metrics.keyFiles", "TAOS_ADAPTER_OPEN_METRICS_KEY_FILES")
	pflag.StringArray("open_metrics.keyFiles", []string(nil), `Paths to private key files for mTLS. Env var: "TAOS_ADAPTER_OPEN_METRICS_KEY_FILES"`)
	viper.SetDefault("open_metrics.keyFiles", []string(nil))

	_ = viper.BindEnv("open_metrics.insecureSkipVerify", "TAOS_ADAPTER_OPEN_METRICS_INSECURE_SKIP_VERIFY")
	pflag.Bool("open_metrics.insecureSkipVerify", true, `Skip TLS certificate verification (insecure). Env var: "TAOS_ADAPTER_OPEN_METRICS_INSECURE_SKIP_VERIFY"`)
	viper.SetDefault("open_metrics.insecureSkipVerify", true)

	// Collection parameters
	_ = viper.BindEnv("open_metrics.gatherDurationSeconds", "TAOS_ADAPTER_OPEN_METRICS_GATHER_DURATION_SECONDS")
	pflag.IntSlice("open_metrics.gatherDurationSeconds", []int{5}, `Interval in seconds between OpenMetrics scrapes. Env var: "TAOS_ADAPTER_OPEN_METRICS_GATHER_DURATION_SECONDS"`)
	viper.SetDefault("open_metrics.gatherDurationSeconds", []int{5})

	// Data retention
	_ = viper.BindEnv("open_metrics.ttl", "TAOS_ADAPTER_OPEN_METRICS_TTL")
	pflag.IntSlice("open_metrics.ttl", []int{}, `Time-to-live in seconds for OpenMetrics data (0=no expiration). Env var: "TAOS_ADAPTER_OPEN_METRICS_TTL"`)
	viper.SetDefault("open_metrics.ttl", []int{})

	// Timestamp handling
	_ = viper.BindEnv("open_metrics.ignoreTimestamp", "TAOS_ADAPTER_OPEN_METRICS_IGNORE_TIMESTAMP")
	pflag.Bool("open_metrics.ignoreTimestamp", false, `Use server timestamp instead of metrics timestamps. Env var: "TAOS_ADAPTER_OPEN_METRICS_IGNORE_TIMESTAMP"`)
	viper.SetDefault("open_metrics.ignoreTimestamp", false)

	// token for TDengine
	_ = viper.BindEnv("open_metrics.token", "TAOS_ADAPTER_OPEN_METRICS_TOKEN")
	pflag.String("open_metrics.token", "", `TDengine token for OpenMetrics connection. Env var: "TAOS_ADAPTER_OPEN_METRICS_TOKEN"`)
	viper.SetDefault("open_metrics.token", "")
}
