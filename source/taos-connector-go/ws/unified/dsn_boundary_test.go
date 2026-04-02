package unified

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseDSNParamsAllKnownKeys verifies the expected behavior for this scenario.
func TestParseDSNParamsAllKnownKeys(t *testing.T) {
	cfg := &Config{
		InterpolateParams: true,
	}
	err := parseDSNParams(cfg, "flagOnly&interpolateParams=false&token=tk1&enableCompression=true&readTimeout=1s&writeTimeout=2s&timezone=Asia%2FShanghai&bearerToken=b1&totpCode=123456&autoReconnect=true&chanLength=8&reconnectIntervalMs=5000&reconnectRetryCount=10&custom=a%2Bb")
	require.NoError(t, err)

	assert.False(t, cfg.InterpolateParams)
	assert.Equal(t, "tk1", cfg.Token)
	assert.True(t, cfg.EnableCompression)
	assert.Equal(t, time.Second, cfg.ReadTimeout)
	assert.Equal(t, 2*time.Second, cfg.WriteTimeout)
	require.NotNil(t, cfg.Timezone)
	assert.Equal(t, "Asia/Shanghai", cfg.Timezone.String())
	assert.Equal(t, "b1", cfg.BearerToken)
	assert.Equal(t, "123456", cfg.TotpCode)
	assert.True(t, cfg.AutoReconnect)
	assert.Equal(t, uint(8), cfg.ChanLength)
	assert.Equal(t, 5000, cfg.ReconnectIntervalMs)
	assert.Equal(t, 10, cfg.ReconnectRetryCount)
	require.NotNil(t, cfg.Params)
	assert.Equal(t, "a+b", cfg.Params["custom"])
}

// TestParseDSNParamsErrorBranches verifies the expected behavior for this scenario.
func TestParseDSNParamsErrorBranches(t *testing.T) {
	tests := []struct {
		name       string
		rawParams  string
		wantErrMsg string
	}{
		{
			name:       "invalid interpolateParams",
			rawParams:  "interpolateParams=abc",
			wantErrMsg: "invalid bool value",
		},
		{
			name:       "invalid enableCompression",
			rawParams:  "enableCompression=abc",
			wantErrMsg: "invalid enableCompression value",
		},
		{
			name:       "invalid readTimeout",
			rawParams:  "readTimeout=abc",
			wantErrMsg: "invalid duration value",
		},
		{
			name:       "invalid writeTimeout",
			rawParams:  "writeTimeout=abc",
			wantErrMsg: "invalid duration value",
		},
		{
			name:       "invalid timezone unescape",
			rawParams:  "timezone=Asia%2Shanghai",
			wantErrMsg: "can not unescape timezone value",
		},
		{
			name:       "invalid timezone value",
			rawParams:  "timezone=Invalid%2FTimezone",
			wantErrMsg: "invalid timezone value",
		},
		{
			name:       "invalid custom param unescape",
			rawParams:  "custom=%2S",
			wantErrMsg: "invalid URL escape",
		},
		{
			name:       "invalid autoReconnect",
			rawParams:  "autoReconnect=abc",
			wantErrMsg: "invalid autoReconnect value",
		},
		{
			name:       "invalid chanLength",
			rawParams:  "chanLength=abc",
			wantErrMsg: "invalid chanLength value",
		},
		{
			name:       "invalid reconnectIntervalMs",
			rawParams:  "reconnectIntervalMs=abc",
			wantErrMsg: "invalid reconnectIntervalMs value",
		},
		{
			name:       "invalid reconnectRetryCount",
			rawParams:  "reconnectRetryCount=abc",
			wantErrMsg: "invalid reconnectRetryCount value",
		},
	}
	for i := 0; i < len(tests); i++ {
		tc := tests[i]
		t.Run(tc.name, func(t *testing.T) {
			err := parseDSNParams(&Config{}, tc.rawParams)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErrMsg)
		})
	}
}

// TestParseDSNAddressListBoundaries verifies the expected behavior for this scenario.
func TestParseDSNAddressListBoundaries(t *testing.T) {
	_, err := parseDSNAddressList("")
	require.ErrorIs(t, err, ErrInvalidDSNAddr)

	_, err = parseDSNAddressList("host_without_port")
	require.ErrorIs(t, err, ErrInvalidDSNAddr)

	_, err = parseDSNAddressList("a:b")
	require.ErrorIs(t, err, ErrInvalidDSNPort)

	_, err = parseDSNAddressList("127.0.0.1:6041,,127.0.0.1:6042")
	require.ErrorIs(t, err, ErrInvalidDSNAddr)

	addrs, err := parseDSNAddressList(":,:0,127.0.0.1:,127.0.0.1:6041")
	require.NoError(t, err)
	require.Len(t, addrs, 4)
	assert.Equal(t, "", addrs[0].host)
	assert.Equal(t, 0, addrs[0].port)
	assert.Equal(t, "", addrs[1].host)
	assert.Equal(t, 0, addrs[1].port)
	assert.Equal(t, "127.0.0.1", addrs[2].host)
	assert.Equal(t, 0, addrs[2].port)
	assert.Equal(t, "127.0.0.1", addrs[3].host)
	assert.Equal(t, 6041, addrs[3].port)
}

// TestParseDSNBoundaryPaths verifies the expected behavior for this scenario.
func TestParseDSNBoundaryPaths(t *testing.T) {
	cfg, err := ParseDSN("")
	require.NoError(t, err)
	assert.True(t, cfg.InterpolateParams)
	assert.Equal(t, "", cfg.User)
	assert.Equal(t, "", cfg.Passwd)
	assert.Equal(t, "", cfg.DbName)

	cfg, err = ParseDSN("user@ws(127.0.0.1:6041)/db")
	require.NoError(t, err)
	assert.Equal(t, "user", cfg.User)
	assert.Equal(t, "", cfg.Passwd)

	cfg, err = ParseDSN("%@ws(127.0.0.1:6041)/db")
	require.NoError(t, err)
	// QueryUnescape fails for "%", tryUnescape should keep original value.
	assert.Equal(t, "%", cfg.User)

	_, err = ParseDSN("u:p@ws(127.0.0.1:6041)extra/db")
	require.ErrorIs(t, err, ErrInvalidDSNUnescaped)

	_, err = ParseDSN("u:p@ws()/db")
	require.ErrorIs(t, err, ErrInvalidDSNAddr)

	_, err = ParseDSN("u:p@ws(a:b)/db")
	require.ErrorIs(t, err, ErrInvalidDSNPort)

	cfg, err = ParseDSN("u:p@ws(:,:0,127.0.0.1:)/db?token=tk")
	require.NoError(t, err)
	require.Len(t, cfg.Endpoints, 3)
	assert.Equal(t, "ws://127.0.0.1:6041?token=tk", cfg.Endpoints[0])
	assert.Equal(t, "ws://127.0.0.1:6041?token=tk", cfg.Endpoints[1])
	assert.Equal(t, "ws://127.0.0.1:6041?token=tk", cfg.Endpoints[2])
}

// TestNewConfigFromDSNBoundaryPaths verifies the expected behavior for this scenario.
func TestNewConfigFromDSNBoundaryPaths(t *testing.T) {
	cfg, err := NewConfigFromDSN("u:p@ws(127.0.0.1:6041)/db?token=tk", "/ws")
	require.NoError(t, err)
	require.Len(t, cfg.Endpoints, 1)
	assert.Equal(t, "ws://127.0.0.1:6041/ws?token=tk", cfg.Endpoints[0])

	cfg, err = NewConfigFromDSN("u:p@ws(a:6041,b:6042)/db?token=tk", "/ws")
	require.NoError(t, err)
	require.Len(t, cfg.Endpoints, 2)
	assert.Equal(t, "ws://a:6041/ws?token=tk", cfg.Endpoints[0])
	assert.Equal(t, "ws://b:6042/ws?token=tk", cfg.Endpoints[1])

	_, err = NewConfigFromDSN("/db", "/ws")
	require.ErrorIs(t, err, ErrNoEndpoints)
}

// TestMapDSNErrorHelpers verifies the expected behavior for this scenario.
func TestMapDSNErrorHelpers(t *testing.T) {
	assert.Equal(t, "?", tryUnescape("%3F"))
	assert.Equal(t, "%", tryUnescape("%"))
}

// TestNewConfigFromDSNAllFields verifies that all Config fields round-trip through DSN parsing.
func TestNewConfigFromDSNAllFields(t *testing.T) {
	dsn := "usr:pwd@wss(10.0.0.1:6030,10.0.0.2:6031)/mydb?" +
		"interpolateParams=false&" +
		"token=tok1&" +
		"enableCompression=true&" +
		"readTimeout=10s&" +
		"writeTimeout=5s&" +
		"timezone=Asia%2FShanghai&" +
		"bearerToken=bear1&" +
		"totpCode=654321&" +
		"autoReconnect=true&" +
		"chanLength=16&" +
		"reconnectIntervalMs=3000&" +
		"reconnectRetryCount=5&" +
		"customKey=customVal"

	cfg, err := NewConfigFromDSN(dsn, "/ws")
	require.NoError(t, err)

	// connection fields
	assert.Equal(t, "usr", cfg.User)
	assert.Equal(t, "pwd", cfg.Passwd)
	assert.Equal(t, "wss", cfg.Net)
	assert.Equal(t, "10.0.0.1", cfg.Addr)
	assert.Equal(t, 6030, cfg.Port)
	assert.Equal(t, "mydb", cfg.DbName)

	// endpoints (normalized with path and token)
	require.Len(t, cfg.Endpoints, 2)
	assert.Equal(t, "wss://10.0.0.1:6030/ws?token=tok1", cfg.Endpoints[0])
	assert.Equal(t, "wss://10.0.0.2:6031/ws?token=tok1", cfg.Endpoints[1])

	// params parsed from query string
	assert.False(t, cfg.InterpolateParams)
	assert.Equal(t, "tok1", cfg.Token)
	assert.True(t, cfg.EnableCompression)
	assert.Equal(t, 10*time.Second, cfg.ReadTimeout)
	assert.Equal(t, 5*time.Second, cfg.WriteTimeout)
	require.NotNil(t, cfg.Timezone)
	assert.Equal(t, "Asia/Shanghai", cfg.Timezone.String())
	assert.Equal(t, "bear1", cfg.BearerToken)
	assert.Equal(t, "654321", cfg.TotpCode)

	// runtime fields
	assert.True(t, cfg.AutoReconnect)
	assert.Equal(t, uint(16), cfg.ChanLength)
	assert.Equal(t, 3000, cfg.ReconnectIntervalMs)
	assert.Equal(t, 5, cfg.ReconnectRetryCount)

	// custom params
	require.NotNil(t, cfg.Params)
	assert.Equal(t, "customVal", cfg.Params["customKey"])
}

// TestNewConfigFromDSNRuntimeDefaults verifies that runtime fields keep defaults when omitted from DSN.
func TestNewConfigFromDSNRuntimeDefaults(t *testing.T) {
	cfg, err := NewConfigFromDSN("u:p@ws(127.0.0.1:6041)/db", "/ws")
	require.NoError(t, err)

	assert.False(t, cfg.AutoReconnect)
	assert.Equal(t, uint(1), cfg.ChanLength)
	assert.Equal(t, 2000, cfg.ReconnectIntervalMs)
	assert.Equal(t, 3, cfg.ReconnectRetryCount)
}
