package config

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequest_setValue(t *testing.T) {
	tmpDir := t.TempDir()
	type fields struct {
		QueryLimitEnable                 bool
		ExcludeQueryLimitSql             []string
		ExcludeQueryLimitSqlMaxByteCount int
		ExcludeQueryLimitSqlMinByteCount int
		ExcludeQueryLimitSqlRegex        []*regexp.Regexp
		Default                          *LimitConfig
		Users                            map[string]*LimitConfig
	}
	tests := []struct {
		name    string
		content string
		fields  fields
		wantErr bool
	}{
		{
			name: "normal case",
			content: `
[request]
queryLimitEnable = true
excludeQueryLimitSql = ["select 1","select server_version()"]
excludeQueryLimitSqlRegex = ['(?i)^select\s+.*from\s+information_schema.*']
[request.default]
queryLimit = 1
queryWaitTimeout = 2
queryMaxWait = 3
[request.users.test_user]
queryLimit = 100
queryWaitTimeout = 200
queryMaxWait = 300
[request.users.test_user2]
`,
			fields: fields{
				QueryLimitEnable:                 true,
				ExcludeQueryLimitSql:             []string{"select1", "selectserver_version()"},
				ExcludeQueryLimitSqlMaxByteCount: 22,
				ExcludeQueryLimitSqlMinByteCount: 7,
				ExcludeQueryLimitSqlRegex: []*regexp.Regexp{
					regexp.MustCompile(`(?i)^select\s+.*from\s+information_schema.*`),
				},
				Default: &LimitConfig{
					QueryLimit:       1,
					QueryWaitTimeout: 2,
					QueryMaxWait:     3,
				},
				Users: map[string]*LimitConfig{
					"test_user": {
						QueryLimit:       100,
						QueryWaitTimeout: 200,
						QueryMaxWait:     300,
					},
					"test_user2": {
						QueryLimit:       1,
						QueryWaitTimeout: 2,
						QueryMaxWait:     3,
					},
				},
			},
			wantErr: false,
		},
		{
			name: "error regex",
			content: `
[request]
excludeQueryLimitSqlRegex = ['(?i^select\s+.*from\s+information_schema.*']
`,
			fields:  fields{},
			wantErr: true,
		},
		{
			name: "empty exclude sql",
			content: `
[request]
excludeQueryLimitSql = ['   ']
`,
			fields:  fields{},
			wantErr: true,
		},
		{
			name: "non-select exclude sql",
			content: `
[request]
excludeQueryLimitSql = ['insert into']
`,
			fields:  fields{},
			wantErr: true,
		},
		{
			name: "only select exclude sql",
			content: `
[request]
excludeQueryLimitSql = ['select']
`,
			fields:  fields{},
			wantErr: true,
		},
		{
			name: "default",
			content: `
[request.default]
queryLimit = 0
queryWaitTimeout = 900
queryMaxWait = 0
#[request.users.root]
#queryLimit = 100
#queryWaitTimeout = 200
#queryMaxWait = 10
`,
			fields: fields{
				QueryLimitEnable:                 false,
				ExcludeQueryLimitSql:             nil,
				ExcludeQueryLimitSqlMaxByteCount: 0,
				ExcludeQueryLimitSqlMinByteCount: 0,
				ExcludeQueryLimitSqlRegex:        nil,
				Default: &LimitConfig{
					QueryLimit:       0,
					QueryWaitTimeout: 900,
					QueryMaxWait:     0,
				},
				Users: nil,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configPath := filepath.Join(tmpDir, "config.toml")
			err := os.WriteFile(configPath, []byte(tt.content), 0644)
			require.NoError(t, err)
			v := viper.New()
			initRequest(v)
			v.SetConfigType("toml")
			v.SetConfigFile(configPath)
			err = v.ReadInConfig()
			require.NoError(t, err)
			r := &Request{
				QueryLimitEnable:                 tt.fields.QueryLimitEnable,
				ExcludeQueryLimitSql:             tt.fields.ExcludeQueryLimitSql,
				ExcludeQueryLimitSqlRegex:        tt.fields.ExcludeQueryLimitSqlRegex,
				ExcludeQueryLimitSqlMaxByteCount: tt.fields.ExcludeQueryLimitSqlMaxByteCount,
				ExcludeQueryLimitSqlMinByteCount: tt.fields.ExcludeQueryLimitSqlMinByteCount,
				Default:                          tt.fields.Default,
				Users:                            tt.fields.Users,
			}
			var gotConfig Request
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

func TestRequest_GetUserLimitConfig(t *testing.T) {
	type fields struct {
		Default *LimitConfig
		Users   map[string]*LimitConfig
	}
	type args struct {
		user string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *LimitConfig
	}{
		{
			name: "user exists",
			fields: fields{
				Default: &LimitConfig{
					QueryLimit:       1,
					QueryWaitTimeout: 2,
					QueryMaxWait:     3,
				},
				Users: map[string]*LimitConfig{
					"test_user": {
						QueryLimit:       100,
						QueryWaitTimeout: 200,
						QueryMaxWait:     300,
					},
				},
			},
			args: args{
				user: "test_user",
			},
			want: &LimitConfig{
				QueryLimit:       100,
				QueryWaitTimeout: 200,
				QueryMaxWait:     300,
			},
		},
		{
			name: "user not exists",
			fields: fields{
				Default: &LimitConfig{
					QueryLimit:       1,
					QueryWaitTimeout: 2,
					QueryMaxWait:     3,
				},
				Users: map[string]*LimitConfig{
					"test_user": {
						QueryLimit:       100,
						QueryWaitTimeout: 200,
						QueryMaxWait:     300,
					},
				},
			},
			args: args{
				user: "not_exists_user",
			},
			want: &LimitConfig{
				QueryLimit:       1,
				QueryWaitTimeout: 2,
				QueryMaxWait:     3,
			},
		},
		{
			name: "no users configured",
			fields: fields{
				Default: &LimitConfig{
					QueryLimit:       1,
					QueryWaitTimeout: 2,
					QueryMaxWait:     3,
				},
				Users: nil,
			},
			args: args{
				user: "any_user",
			},
			want: &LimitConfig{
				QueryLimit:       1,
				QueryWaitTimeout: 2,
				QueryMaxWait:     3,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Request{
				Default: tt.fields.Default,
				Users:   tt.fields.Users,
			}
			assert.Equalf(t, tt.want, r.GetUserLimitConfig(tt.args.user), "GetUserLimitConfig(%v)", tt.args.user)
		})
	}
}
