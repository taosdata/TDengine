package taosWS

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	taosErrors "github.com/taosdata/driver-go/v3/errors"
)

func TestNewConfigDefaultValues(t *testing.T) {
	cfg := NewConfig()
	require.NotNil(t, cfg)
	assert.True(t, cfg.InterpolateParams)
}

func TestParseDSNDelegatedParamsBoundary(t *testing.T) {
	cfg, err := ParseDSN("u:p@ws(127.0.0.1:6041,127.0.0.1:6042)/db?interpolateParams=false&token=tk&enableCompression=true&readTimeout=1s&writeTimeout=2s&timezone=Asia%2FShanghai&bearerToken=b1&totpCode=123456&custom=v%2B1")
	require.NoError(t, err)

	assert.Equal(t, "u", cfg.User)
	assert.Equal(t, "p", cfg.Passwd)
	assert.Equal(t, "ws", cfg.Net)
	assert.Equal(t, "127.0.0.1", cfg.Addr)
	assert.Equal(t, 6041, cfg.Port)
	assert.Equal(t, "db", cfg.DbName)
	assert.False(t, cfg.InterpolateParams)
	assert.Equal(t, "tk", cfg.Token)
	assert.True(t, cfg.EnableCompression)
	assert.Equal(t, time.Second, cfg.ReadTimeout)
	assert.Equal(t, 2*time.Second, cfg.WriteTimeout)
	require.NotNil(t, cfg.Timezone)
	assert.Equal(t, "Asia/Shanghai", cfg.Timezone.String())
	assert.Equal(t, "b1", cfg.BearerToken)
	assert.Equal(t, "123456", cfg.TotpCode)
	require.NotNil(t, cfg.Params)
	assert.Equal(t, "v+1", cfg.Params["custom"])
	require.Len(t, cfg.Endpoints, 2)
	assert.Equal(t, "ws://127.0.0.1:6041?token=tk", cfg.Endpoints[0])
	assert.Equal(t, "ws://127.0.0.1:6042?token=tk", cfg.Endpoints[1])
}

func TestParseDSNDelegatedKnownErrorMappings(t *testing.T) {
	tests := []struct {
		name    string
		dsn     string
		wantErr error
	}{
		{
			name:    "unescaped",
			dsn:     "u:p@ws(127.0.0.1:6041)extra/db",
			wantErr: ErrInvalidDSNUnescaped,
		},
		{
			name:    "addr",
			dsn:     "u:p@ws()/db",
			wantErr: ErrInvalidDSNAddr,
		},
		{
			name:    "port",
			dsn:     "u:p@ws(a:b)/db",
			wantErr: ErrInvalidDSNPort,
		},
		{
			name:    "no slash",
			dsn:     "abcd",
			wantErr: ErrInvalidDSNNoSlash,
		},
	}

	for i := 0; i < len(tests); i++ {
		tc := tests[i]
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseDSN(tc.dsn)
			require.Error(t, err)
			assert.Equal(t, tc.wantErr.Error(), err.Error())
		})
	}
}

func TestMapUnifiedDSNErrorFallbackBranch(t *testing.T) {
	unknown := mapUnifiedDSNError(assert.AnError)
	require.Error(t, unknown)
	var terr *taosErrors.TaosError
	require.ErrorAs(t, unknown, &terr)
	assert.Equal(t, int32(0xffff), terr.Code)
	assert.Equal(t, assert.AnError.Error(), terr.ErrStr)

	require.Nil(t, mapUnifiedDSNError(nil))
}
