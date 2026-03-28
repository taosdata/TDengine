package unified

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseDSNWithExtendedParams verifies the expected behavior for this scenario.
func TestParseDSNWithExtendedParams(t *testing.T) {
	dsn := "u:p@ws(127.0.0.1:6041)/db1?" +
		"interpolateParams=false&" +
		"token=tk1&" +
		"enableCompression=true&" +
		"readTimeout=2s&" +
		"writeTimeout=3s&" +
		"timezone=Asia%2FShanghai&" +
		"bearerToken=b1&" +
		"totpCode=123456&" +
		"custom=a%2Bb"

	cfg, err := ParseDSN(dsn)
	require.NoError(t, err)

	assert.Equal(t, "u", cfg.User)
	assert.Equal(t, "p", cfg.Passwd)
	assert.Equal(t, "db1", cfg.DbName)
	assert.False(t, cfg.InterpolateParams)
	assert.Equal(t, "tk1", cfg.Token)
	assert.True(t, cfg.EnableCompression)
	assert.Equal(t, 2*time.Second, cfg.ReadTimeout)
	assert.Equal(t, 3*time.Second, cfg.WriteTimeout)
	if assert.NotNil(t, cfg.Timezone) {
		assert.Equal(t, "Asia/Shanghai", cfg.Timezone.String())
	}
	assert.Equal(t, "b1", cfg.BearerToken)
	assert.Equal(t, "123456", cfg.TotpCode)
	if assert.NotNil(t, cfg.Params) {
		assert.Equal(t, "a+b", cfg.Params["custom"])
	}
}

// TestParseDSNInvalidParamTypes verifies the expected behavior for this scenario.
func TestParseDSNInvalidParamTypes(t *testing.T) {
	_, err := ParseDSN("u:p@ws(127.0.0.1:6041)/db1?interpolateParams=not_bool")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid bool value")

	_, err = ParseDSN("u:p@ws(127.0.0.1:6041)/db1?enableCompression=bad")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid enableCompression value")

	_, err = ParseDSN("u:p@ws(127.0.0.1:6041)/db1?readTimeout=bad")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid duration value")

	_, err = ParseDSN("u:p@ws(127.0.0.1:6041)/db1?timezone=No%2FSuch_Zone")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid timezone value")
}
