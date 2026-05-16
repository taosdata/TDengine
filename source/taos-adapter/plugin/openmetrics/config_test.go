package openmetrics

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/driver/common"
)

func TestCheckConfig(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		expectError bool
		errMsg      string
	}{
		{
			name:        "disabled config should pass",
			config:      Config{Enable: false},
			expectError: false,
		},
		{
			name: "valid minimal config",
			config: Config{
				Enable:                 true,
				URLs:                   []string{"http://localhost:9100"},
				DBs:                    []string{"metrics"},
				ResponseTimeoutSeconds: []int{5},
				GatherDurationSeconds:  []int{5},
			},
			expectError: false,
		},
		{
			name: "empty URLs should fail",
			config: Config{
				Enable: true,
				URLs:   []string{},
			},
			expectError: true,
			errMsg:      "open_metrics.urls is empty",
		},
		{
			name: "DBs length mismatch should fail",
			config: Config{
				Enable: true,
				URLs:   []string{"http://localhost:9100", "http://localhost:9200"},
				DBs:    []string{"metrics"},
			},
			expectError: true,
			errMsg:      "open_metrics.dbs and open_metrics.urls must have the same length",
		},
		{
			name: "ResponseTimeoutSeconds length mismatch should fail",
			config: Config{
				Enable:                 true,
				URLs:                   []string{"http://localhost:9100"},
				DBs:                    []string{"metrics"},
				ResponseTimeoutSeconds: []int{5, 10},
			},
			expectError: true,
			errMsg:      "open_metrics.responseTimeoutSeconds and open_metrics.urls must have the same length",
		},
		{
			name: "HttpUsernames length mismatch should fail",
			config: Config{
				Enable:                 true,
				URLs:                   []string{"http://localhost:9100"},
				DBs:                    []string{"metrics"},
				HttpUsernames:          []string{"user1", "user2"},
				ResponseTimeoutSeconds: []int{5},
			},
			expectError: true,
			errMsg:      "open_metrics.httpUsernames and open_metrics.urls must have the same length",
		},
		{
			name: "HttpPasswords length mismatch should fail",
			config: Config{
				Enable:                 true,
				URLs:                   []string{"http://localhost:9100"},
				DBs:                    []string{"metrics"},
				HttpPasswords:          []string{"pass1", "pass2"},
				ResponseTimeoutSeconds: []int{5},
			},
			expectError: true,
			errMsg:      "open_metrics.httpPasswords and open_metrics.urls must have the same length",
		},
		{
			name: "HttpUsernames and HttpPasswords length mismatch should fail",
			config: Config{
				Enable:                 true,
				URLs:                   []string{"http://localhost:9100"},
				DBs:                    []string{"metrics"},
				HttpUsernames:          []string{"user1"},
				HttpPasswords:          nil,
				ResponseTimeoutSeconds: []int{5},
			},
			expectError: true,
			errMsg:      "open_metrics.httpUsernames and open_metrics.httpPasswords must have the same length",
		},
		{
			name: "HttpBearerTokenStrings length mismatch should fail",
			config: Config{
				Enable:                 true,
				URLs:                   []string{"http://localhost:9100"},
				DBs:                    []string{"metrics"},
				HttpBearerTokenStrings: []string{"token1", "token2"},
				ResponseTimeoutSeconds: []int{5},
			},
			expectError: true,
			errMsg:      "open_metrics.httpBearerTokenStrings and open_metrics.urls must have the same length",
		},
		{
			name: "CaCertFiles length mismatch should fail",
			config: Config{
				Enable:                 true,
				URLs:                   []string{"http://localhost:9100"},
				DBs:                    []string{"metrics"},
				CaCertFiles:            []string{"ca1.pem", "ca2.pem"},
				ResponseTimeoutSeconds: []int{5},
			},
			expectError: true,
			errMsg:      "open_metrics.caCertFiles and open_metrics.urls must have the same length",
		},
		{
			name: "CertFiles length mismatch should fail",
			config: Config{
				Enable:                 true,
				URLs:                   []string{"http://localhost:9100"},
				DBs:                    []string{"metrics"},
				CertFiles:              []string{"cert1.pem", "cert2.pem"},
				ResponseTimeoutSeconds: []int{5},
			},
			expectError: true,
			errMsg:      "open_metrics.certFiles and open_metrics.urls must have the same length",
		},
		{
			name: "KeyFiles length mismatch should fail",
			config: Config{
				Enable:                 true,
				URLs:                   []string{"http://localhost:9100"},
				DBs:                    []string{"metrics"},
				KeyFiles:               []string{"key1.pem", "key2.pem"},
				ResponseTimeoutSeconds: []int{5},
			},
			expectError: true,
			errMsg:      "open_metrics.keyFiles and open_metrics.urls must have the same length",
		},
		{
			name: "CertFiles and KeyFiles length mismatch should fail",
			config: Config{
				Enable:                 true,
				URLs:                   []string{"http://localhost:9100"},
				DBs:                    []string{"metrics"},
				CertFiles:              []string{"cert1.pem"},
				KeyFiles:               nil,
				ResponseTimeoutSeconds: []int{5},
			},
			expectError: true,
			errMsg:      "open_metrics.certFiles and open_metrics.keyFiles must have the same length",
		},
		{
			name: "GatherDurationSeconds length mismatch should fail",
			config: Config{
				Enable:                 true,
				URLs:                   []string{"http://localhost:9100"},
				DBs:                    []string{"metrics"},
				GatherDurationSeconds:  []int{5, 10},
				ResponseTimeoutSeconds: []int{5},
			},
			expectError: true,
			errMsg:      "open_metrics.gatherDurationSeconds and open_metrics.urls must have the same length",
		},
		{
			name: "TTL length mismatch should fail",
			config: Config{
				Enable:                 true,
				URLs:                   []string{"http://localhost:9100"},
				DBs:                    []string{"metrics"},
				TTL:                    []int{3600, 7200},
				GatherDurationSeconds:  []int{5},
				ResponseTimeoutSeconds: []int{5},
			},
			expectError: true,
			errMsg:      "open_metrics.ttl and open_metrics.urls must have the same length",
		},
		{
			name: "complex valid config should pass",
			config: Config{
				Enable:                 true,
				User:                   common.DefaultUser,
				Password:               common.DefaultPassword,
				URLs:                   []string{"http://localhost:9100", "https://remote:9200"},
				DBs:                    []string{"metrics1", "metrics2"},
				ResponseTimeoutSeconds: []int{5, 10},
				HttpUsernames:          []string{"", "admin"},
				HttpPasswords:          []string{"", "secret"},
				HttpBearerTokenStrings: []string{"", "token"},
				CaCertFiles:            []string{"", "ca.pem"},
				CertFiles:              []string{"", "cert.pem"},
				KeyFiles:               []string{"", "key.pem"},
				GatherDurationSeconds:  []int{5, 10},
				TTL:                    []int{3600, 7200},
				IgnoreTimestamp:        true,
				InsecureSkipVerify:     true,
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.CheckConfig()
			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestExampleConfig(t *testing.T) {
	defaultConfig := `# OpenMetrics Configuration
[open_metrics]
enable = false  # Enable OpenMetrics data collection

## TDengine connection credentials
user = "root"  # TDengine username for OpenMetrics connection
password = "taosdata"  # TDengine password for OpenMetrics connection

## Database configuration
dbs = ["open_metrics"]  # Target database names for OpenMetrics data

## Endpoint configuration
urls = ["http://localhost:9100"]  # OpenMetrics endpoints to scrape

## Timeout settings
responseTimeoutSeconds = [5]  # HTTP response timeout in seconds for OpenMetrics scraping

## Authentication methods
httpUsernames = []  # Basic auth usernames for protected OpenMetrics endpoints
httpPasswords = []  # Basic auth passwords for protected OpenMetrics endpoints
httpBearerTokenStrings = []  # Bearer tokens for OpenMetrics endpoint authentication

## TLS configuration
caCertFiles = []  # Paths to CA certificate files for TLS verification
certFiles = []  # Paths to client certificate files for mTLS
keyFiles = []  # Paths to private key files for mTLS
insecureSkipVerify = true  # Skip TLS certificate verification (insecure)

## Collection parameters
gatherDurationSeconds = [5]  # Interval in seconds between OpenMetrics scrapes

## Data retention
ttl = []  # Time-to-live for OpenMetrics data (0=no expiration)

## Timestamp handling
ignoreTimestamp = false  # Use server timestamp instead of metrics timestamps`
	v := viper.New()
	v.SetConfigType("toml")
	defaultCfg := &Config{}
	defaultCfg.setValue(viper.GetViper())
	t.Log(defaultCfg)
	err := v.ReadConfig(strings.NewReader(defaultConfig))
	assert.NoError(t, err)
	cfg := &Config{}
	cfg.setValue(v)
	t.Log(cfg)
	assert.Equal(t, defaultCfg, cfg)
}
