package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegister_setValue(t *testing.T) {
	tmpDir := t.TempDir()
	host, err := os.Hostname()
	require.NoError(t, err)
	type fields struct {
		Instance    string
		Description string
		Duration    int
		Expire      int
	}
	tests := []struct {
		name    string
		fields  fields
		content string
		wantErr bool
	}{
		{
			name: "normal case",
			fields: fields{
				Instance:    "http://test_instance:6041",
				Description: "test description",
				Duration:    15,
				Expire:      45,
			},
			content: `
[register]
instance = "http://test_instance:6041"
description = "test description"
duration = 15
expire = 45
`,
			wantErr: false,
		},
		{
			name: "instance too long",
			fields: fields{
				Instance:    string(make([]byte, 256)),
				Description: "test description",
				Duration:    15,
				Expire:      45,
			},
			content: `
[register]
instance = "` + strings.Repeat("0", 256) + `"
description = "test description"
duration = 15
expire = 45
`,
			wantErr: true,
		},
		{
			name: "instance empty, use hostname",
			fields: fields{
				Instance:    fmt.Sprintf("%s:6041", host),
				Description: "test description",
				Duration:    15,
				Expire:      45,
			},
			content: `
port = 6041

[ssl]
enable = false

[register]
instance = ""
description = "test description"
duration = 15
expire = 45
`,
			wantErr: false,
		},
		{
			name:   "description too long",
			fields: fields{},
			content: `
[register]
instance = "http://test_instance:6041"
description = "` + strings.Repeat("0", 512) + `"
duration = 15
expire = 45
`,
			wantErr: true,
		},
		{
			name:   "duration invalid",
			fields: fields{},
			content: `
[register]
instance = "http://test_instance:6041"
description = "test description"
duration = 0
expire = 45
`,
			wantErr: true,
		},
		{
			name:   "expire invalid",
			fields: fields{},
			content: `
[register]
instance = "http://test_instance:6041"
description = "test description"
duration = 15
expire = -10
`,
			wantErr: true,
		},
		{
			name: "use ssl",
			fields: fields{
				Instance:    fmt.Sprintf("https://%s:6041", host),
				Description: "test description",
				Duration:    15,
				Expire:      45,
			},
			content: `
port = 6041

[ssl]
enable = true

[register]
description = "test description"
duration = 15
expire = 45
`,
			wantErr: false,
		},
		{
			name: "duration equal expire",
			fields: fields{
				Instance:    "http://test_instance:6041",
				Description: "test description",
				Duration:    30,
				Expire:      30,
			},
			content: `
[register]
instance = "http://test_instance:6041"
description = "test description"
duration = 30
expire = 30
`,
			wantErr: true,
		},
		{
			name: "expire less than duration",
			fields: fields{
				Instance:    "http://test_instance:6041",
				Description: "test description",
				Duration:    40,
				Expire:      30,
			},
			content: `
[register]
instance = "http://test_instance:6041"
description = "test description"
duration = 40
expire = 30
`,
			wantErr: true,
		},
		{
			name: "use default instance",
			fields: fields{
				Instance:    fmt.Sprintf("%s:6041", host),
				Description: "",
				Duration:    10,
				Expire:      30,
			},
			content: `
port=6041
[register]
# The address of this taosAdapter instance. eg: "192.168.1.98:6041" or "https://192.168.1.98:6041" if ssl enabled.
#instance = "localhost:6041"

# The description of this taosAdapter instance.
#description = ""

# The registration duration (in seconds). After this duration, the instance will be re-register.
#duration = 10

# The expiration duration (in seconds). If the instance is not re-registered within this duration, it will be expired. Could not be less than duration.
#expire = 30
`,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configPath := filepath.Join(tmpDir, "config.toml")
			err := os.WriteFile(configPath, []byte(tt.content), 0644)
			require.NoError(t, err)
			v := viper.New()
			initRegister(v)
			v.SetConfigType("toml")
			v.SetConfigFile(configPath)
			err = v.ReadInConfig()
			require.NoError(t, err)
			r := &Register{
				Instance:    tt.fields.Instance,
				Description: tt.fields.Description,
				Duration:    tt.fields.Duration,
				Expire:      tt.fields.Expire,
			}
			var gotConfig Register
			err = gotConfig.setValue(v)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, r, &gotConfig)
		})
	}
}
