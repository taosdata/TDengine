package inputjson

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_setValue(t *testing.T) {
	tmpDir := t.TempDir()
	type fields struct {
		Enable bool
		Rules  []*Rule
	}
	tests := []struct {
		name    string
		content string
		fields  fields
		wantErr bool
	}{
		{
			name: "valid config",
			content: `
[input_json]
enable = true
# rules1
[[input_json.rules]]
endpoint = "rule1"
db = "inputdb"
superTable = "supertable1"
subTable = "subtable1"
timeKey = "ts"
timeFormat = "datetime"
timezone = "Asia/Shanghai"
timeFieldName = "_ts"
fields = [
    {key = "Key1", optional = false},
    {key = "Key2", optional = true},
    {key = "Key3", optional = false}
]

# rules2
[[input_json.rules]]
endpoint = "rule2"
db = "inputdb2"
superTable = "supertable2"
subTable = "subtable2"
timeKey = "timestamp"
timeFormat = "unix"
Timezone = "UTC"
fields = [
    {key = "TagA", optional = false},
    {key = "TagB", optional = false},
    {key = "ColX", optional = true},
    {key = "ColY", optional = false}
]
`,
			fields: fields{
				Enable: true,
				Rules: []*Rule{
					{
						Endpoint:      "rule1",
						DB:            "inputdb",
						SuperTable:    "supertable1",
						SubTable:      "subtable1",
						TimeKey:       "ts",
						TimeFormat:    "datetime",
						Timezone:      "Asia/Shanghai",
						TimeFieldName: "_ts",
						Fields: []*Field{
							{Key: "Key1", Optional: false},
							{Key: "Key2", Optional: true},
							{Key: "Key3", Optional: false},
						},
					},
					{
						Endpoint:      "rule2",
						DB:            "inputdb2",
						SuperTable:    "supertable2",
						SubTable:      "subtable2",
						TimeKey:       "timestamp",
						TimeFormat:    "unix",
						Timezone:      "UTC",
						TimeFieldName: "ts",
						Fields: []*Field{
							{Key: "TagA", Optional: false},
							{Key: "TagB", Optional: false},
							{Key: "ColX", Optional: true},
							{Key: "ColY", Optional: false},
						},
					},
				},
			},
		},
		{
			name: "disabled config",
			content: `
[input_json]
enable = false
`,
			fields: fields{
				Enable: false,
				Rules:  nil,
			},
		},
		{
			name: "invalid config missing rules",
			content: `
[input_json]
enable = true
`,
			fields: fields{
				Enable: true,
				Rules:  nil,
			},
			wantErr: true,
		},
		{
			name: "invalid config empty rules",
			content: `
[input_json]
enable = true
rules = []
`,
			fields: fields{
				Enable: true,
				Rules:  nil,
			},
			wantErr: true,
		},
		{
			name: "invalid config wrong rules type",
			content: `
[input_json]
enable = true
rules = "not a list"
`,
			fields: fields{
				Enable: true,
				Rules:  nil,
			},
			wantErr: true,
		},
		{
			name: "invalid endpoint type",
			content: `
[input_json]
enable = true
[[input_json.rules]]
endpoint = 123
`,
			fields: fields{
				Enable: true,
				Rules:  nil,
			},
			wantErr: true,
		},
		{
			name: "invalid db type",
			content: `
[input_json]
enable = true
[[input_json.rules]]
endpoint = "rule1"
db = 456
`,
			fields: fields{
				Enable: true,
				Rules:  nil,
			},
			wantErr: true,
		},
		{
			name: "invalid dbKey type",
			content: `
[input_json]
enable = true
[[input_json.rules]]
endpoint = "rule1"
dbKey = 789
`,
			fields: fields{
				Enable: true,
				Rules:  nil,
			},
			wantErr: true,
		},
		{
			name: "invalid superTable type",
			content: `
[input_json]
enable = true
[[input_json.rules]]
endpoint = "rule1"
superTable = 101112
`,
			fields: fields{
				Enable: true,
				Rules:  nil,
			},
			wantErr: true,
		},
		{
			name: "invalid superTableKey type",
			content: `
[input_json]
enable = true
[[input_json.rules]]
endpoint = "rule1"
superTableKey = 131415
`,
			fields: fields{
				Enable: true,
				Rules:  nil,
			},
			wantErr: true,
		},
		{
			name: "invalid subtable type",
			content: `
[input_json]
enable = true
[[input_json.rules]]
endpoint = "rule1"
subTable = 161718
`,
			fields: fields{
				Enable: true,
				Rules:  nil,
			},
			wantErr: true,
		},
		{
			name: "invalid subTableKey type",
			content: `
[input_json]
enable = true
[[input_json.rules]]
endpoint = "rule1"
subTableKey = 192021
`,
			fields: fields{
				Enable: true,
				Rules:  nil,
			},
			wantErr: true,
		},
		{
			name: "invalid timeKey type",
			content: `
[input_json]
enable = true
[[input_json.rules]]
endpoint = "rule1"
timeKey = 222324
`,
			fields: fields{
				Enable: true,
				Rules:  nil,
			},
			wantErr: true,
		},
		{
			name: "invalid timeFormat type",
			content: `
[input_json]
enable = true
[[input_json.rules]]
endpoint = "rule1"
timeFormat = 252627
`,
			fields: fields{
				Enable: true,
				Rules:  nil,
			},
			wantErr: true,
		},
		{
			name: "invalid timezone type",
			content: `
[input_json]
enable = true
[[input_json.rules]]
endpoint = "rule1"
timezone = 282930
`,
			fields: fields{
				Enable: true,
				Rules:  nil,
			},
			wantErr: true,
		},
		{
			name: "invalid transformation type",
			content: `
[input_json]
enable = true
[[input_json.rules]]
endpoint = "rule1"
transformation = 313233
`,
			fields: fields{
				Enable: true,
				Rules:  nil,
			},
			wantErr: true,
		},
		{
			name: "invalid timeFieldName type",
			content: `
[input_json]
enable = true
[[input_json.rules]]
endpoint = "rule1"
timeFieldName = 343536
`,
			fields: fields{
				Enable: true,
				Rules:  nil,
			},
			wantErr: true,
		},
		{
			name: "invalid fields type",
			content: `
[input_json]
enable = true
[[input_json.rules]]
endpoint = "rule1"
fields = "not a list"
`,
			fields: fields{
				Enable: true,
				Rules:  nil,
			},
			wantErr: true,
		},
		{
			name: "no fields",
			content: `
[input_json]
enable = true
[[input_json.rules]]
endpoint = "rule1"
`,
			fields: fields{
				Enable: true,
				Rules:  nil,
			},
			wantErr: true,
		},
		{
			name: "invalid field item type",
			content: `
[input_json]
enable = true
[[input_json.rules]]
endpoint = "rule1"
fields = [ "not a table" ]
`,
			fields: fields{
				Enable: true,
				Rules:  nil,
			},
			wantErr: true,
		},
		{
			name: "invalid field key type",
			content: `
[input_json]
enable = true
[[input_json.rules]]
endpoint = "rule1"
fields = [ { key = 123, optional = false } ]
`,
			fields: fields{
				Enable: true,
				Rules:  nil,
			},
			wantErr: true,
		},
		{
			name: "invalid field optional type",
			content: `
[input_json]
enable = true
[[input_json.rules]]
endpoint = "rule1"
fields = [ { key = "Key1", optional = "not a bool" } ]
`,
			fields: fields{
				Enable: true,
				Rules:  nil,
			},
			wantErr: true,
		},
		{
			name: "rule not a map",
			content: `
[input_json]
enable = true
rules = ["not a map"]
`,
			fields: fields{
				Enable: true,
				Rules:  nil,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configPath := filepath.Join(tmpDir, "config.toml")
			err := os.WriteFile(configPath, []byte(tt.content), 0644)
			require.NoError(t, err)
			v := viper.New()
			v.SetConfigType("toml")
			v.SetConfigFile(configPath)
			err = v.ReadInConfig()
			require.NoError(t, err)
			want := &Config{
				Enable: tt.fields.Enable,
				Rules:  tt.fields.Rules,
			}
			got := &Config{}
			if err := got.setValue(v); (err != nil) != tt.wantErr {
				t.Errorf("setValue() error = %v, wantErr %v", err, tt.wantErr)
			}
			assert.Equal(t, want, got)
		})
	}
}

func TestGetStringConfig(t *testing.T) {
	tests := []struct {
		name        string
		mapData     map[string]interface{}
		key         string
		defaultVal  string
		expected    string
		expectedErr string
	}{
		{
			name: "key exists and is string",
			mapData: map[string]interface{}{
				"host": "localhost",
			},
			key:        "host",
			defaultVal: "127.0.0.1",
			expected:   "localhost",
		},
		{
			name: "key exists with case insensitive",
			mapData: map[string]interface{}{
				"host": "localhost",
			},
			key:        "HOST",
			defaultVal: "127.0.0.1",
			expected:   "localhost",
		},
		{
			name: "key does not exist",
			mapData: map[string]interface{}{
				"port": 8080,
			},
			key:        "host",
			defaultVal: "127.0.0.1",
			expected:   "127.0.0.1",
		},
		{
			name: "key exists but is not string - int",
			mapData: map[string]interface{}{
				"port": 8080,
			},
			key:         "port",
			defaultVal:  "8080",
			expected:    "",
			expectedErr: "port is not string, value: 8080",
		},
		{
			name: "key exists but is not string - bool",
			mapData: map[string]interface{}{
				"enabled": true,
			},
			key:         "enabled",
			defaultVal:  "false",
			expected:    "",
			expectedErr: "enabled is not string, value: true",
		},
		{
			name: "key exists but is not string - float",
			mapData: map[string]interface{}{
				"version": 1.5,
			},
			key:         "version",
			defaultVal:  "1.0",
			expected:    "",
			expectedErr: "version is not string, value: 1.5",
		},
		{
			name: "key exists but is not string - slice",
			mapData: map[string]interface{}{
				"hosts": []string{"host1", "host2"},
			},
			key:         "hosts",
			defaultVal:  "",
			expected:    "",
			expectedErr: "hosts is not string, value: [host1 host2]",
		},
		{
			name: "key exists but is not string - map",
			mapData: map[string]interface{}{
				"config": map[string]interface{}{"key": "value"},
			},
			key:         "config",
			defaultVal:  "",
			expected:    "",
			expectedErr: "config is not string, value: map[key:value]",
		},
		{
			name:       "empty map",
			mapData:    map[string]interface{}{},
			key:        "host",
			defaultVal: "default",
			expected:   "default",
		},
		{
			name:       "nil map",
			mapData:    nil,
			key:        "host",
			defaultVal: "default",
			expected:   "default",
		},
		{
			name: "empty key",
			mapData: map[string]interface{}{
				"": "empty key value",
			},
			key:        "",
			defaultVal: "default",
			expected:   "empty key value",
		},
		{
			name: "key with spaces",
			mapData: map[string]interface{}{
				"server name": "my server",
			},
			key:        "Server Name",
			defaultVal: "default server",
			expected:   "my server",
		},
		{
			name: "key with special characters",
			mapData: map[string]interface{}{
				"host-name": "localhost",
			},
			key:        "HOST-NAME",
			defaultVal: "127.0.0.1",
			expected:   "localhost",
		},
		{
			name: "multiple keys with different cases",
			mapData: map[string]interface{}{
				"host": "lowercase",
				"HOST": "uppercase",
				"Host": "mixedcase",
			},
			key:        "host",
			defaultVal: "default",
			expected:   "lowercase",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := getStringConfig(tt.mapData, tt.key, tt.defaultVal)

			if tt.expectedErr != "" {
				if err == nil {
					t.Errorf("Expected error '%s', but got nil", tt.expectedErr)
				} else if err.Error() != tt.expectedErr {
					t.Errorf("Expected error '%s', but got '%s'", tt.expectedErr, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}

			if result != tt.expected {
				t.Errorf("Expected result '%s', but got '%s'", tt.expected, result)
			}
		})
	}
}

func TestGetStringConfig_ErrorFormat(t *testing.T) {
	mapData := map[string]interface{}{
		"port": 8080,
	}

	_, err := getStringConfig(mapData, "port", "8080")

	if err == nil {
		t.Fatal("Expected error, but got nil")
	}

	expectedError := "port is not string, value: 8080"
	if err.Error() != expectedError {
		t.Errorf("Expected error message '%s', but got '%s'", expectedError, err.Error())
	}

	if !strings.Contains(err.Error(), "port is not string") {
		t.Error("Error message should contain 'port is not string'")
	}
	if !strings.Contains(err.Error(), "8080") {
		t.Error("Error message should contain the actual value '8080'")
	}
}

func TestGetStringConfig_DefaultValueScenarios(t *testing.T) {
	tests := []struct {
		name       string
		mapData    map[string]interface{}
		key        string
		defaultVal string
		expected   string
	}{
		{
			name:       "default value is empty string",
			mapData:    map[string]interface{}{},
			key:        "missing",
			defaultVal: "",
			expected:   "",
		},
		{
			name:       "default value with spaces",
			mapData:    map[string]interface{}{},
			key:        "missing",
			defaultVal: "  default with spaces  ",
			expected:   "  default with spaces  ",
		},
		{
			name:       "default value with special characters",
			mapData:    map[string]interface{}{},
			key:        "missing",
			defaultVal: "default@#$%^&*()",
			expected:   "default@#$%^&*()",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := getStringConfig(tt.mapData, tt.key, tt.defaultVal)

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if result != tt.expected {
				t.Errorf("Expected result '%s', but got '%s'", tt.expected, result)
			}
		})
	}
}

func TestGetBoolConfig(t *testing.T) {
	tests := []struct {
		name        string
		mapData     map[string]interface{}
		key         string
		expected    bool
		expectedErr string
	}{
		{
			name: "key exists and is true",
			mapData: map[string]interface{}{
				"enabled": true,
			},
			key:      "enabled",
			expected: true,
		},
		{
			name: "key exists and is false",
			mapData: map[string]interface{}{
				"enabled": false,
			},
			key:      "enabled",
			expected: false,
		},
		{
			name: "key exists with case insensitive",
			mapData: map[string]interface{}{
				"enabled": true,
			},
			key:      "ENABLED",
			expected: true,
		},
		{
			name: "key does not exist",
			mapData: map[string]interface{}{
				"other": "value",
			},
			key:      "enabled",
			expected: false,
		},
		{
			name: "key exists but is not bool - string true",
			mapData: map[string]interface{}{
				"enabled": "true",
			},
			key:         "enabled",
			expected:    false,
			expectedErr: "enabled is not bool, value: true",
		},
		{
			name: "key exists but is not bool - string false",
			mapData: map[string]interface{}{
				"enabled": "false",
			},
			key:         "enabled",
			expected:    false,
			expectedErr: "enabled is not bool, value: false",
		},
		{
			name: "key exists but is not bool - int 1",
			mapData: map[string]interface{}{
				"enabled": 1,
			},
			key:         "enabled",
			expected:    false,
			expectedErr: "enabled is not bool, value: 1",
		},
		{
			name: "key exists but is not bool - int 0",
			mapData: map[string]interface{}{
				"enabled": 0,
			},
			key:         "enabled",
			expected:    false,
			expectedErr: "enabled is not bool, value: 0",
		},
		{
			name: "key exists but is not bool - string",
			mapData: map[string]interface{}{
				"enabled": "yes",
			},
			key:         "enabled",
			expected:    false,
			expectedErr: "enabled is not bool, value: yes",
		},
		{
			name: "key exists but is not bool - float",
			mapData: map[string]interface{}{
				"enabled": 1.0,
			},
			key:         "enabled",
			expected:    false,
			expectedErr: "enabled is not bool, value: 1",
		},
		{
			name: "key exists but is not bool - slice",
			mapData: map[string]interface{}{
				"enabled": []bool{true, false},
			},
			key:         "enabled",
			expected:    false,
			expectedErr: "enabled is not bool, value: [true false]",
		},
		{
			name: "key exists but is not bool - map",
			mapData: map[string]interface{}{
				"enabled": map[string]bool{"value": true},
			},
			key:         "enabled",
			expected:    false,
			expectedErr: "enabled is not bool, value: map[value:true]",
		},
		{
			name:     "empty map",
			mapData:  map[string]interface{}{},
			key:      "enabled",
			expected: false,
		},
		{
			name:     "nil map",
			mapData:  nil,
			key:      "enabled",
			expected: false,
		},
		{
			name: "empty key",
			mapData: map[string]interface{}{
				"": true,
			},
			key:      "",
			expected: true,
		},
		{
			name: "key with spaces",
			mapData: map[string]interface{}{
				"auto reload": true,
			},
			key:      "Auto Reload",
			expected: true,
		},
		{
			name: "key with special characters",
			mapData: map[string]interface{}{
				"is-enabled": false,
			},
			key:      "IS-ENABLED",
			expected: false,
		},
		{
			name: "multiple boolean keys",
			mapData: map[string]interface{}{
				"feature1": true,
				"feature2": false,
				"feature3": true,
			},
			key:      "feature2",
			expected: false,
		},
		{
			name: "mixed case key matching",
			mapData: map[string]interface{}{
				"debugmode": true,
				"DEBUGMODE": false,
				"DebugMode": true,
			},
			key:      "debugmode",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := getBoolConfig(tt.mapData, tt.key)

			if tt.expectedErr != "" {
				if err == nil {
					t.Errorf("Expected error '%s', but got nil", tt.expectedErr)
				} else if err.Error() != tt.expectedErr {
					t.Errorf("Expected error '%s', but got '%s'", tt.expectedErr, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}

			if result != tt.expected {
				t.Errorf("Expected result %v, but got %v", tt.expected, result)
			}
		})
	}
}

func TestGetBoolConfig_ErrorFormat(t *testing.T) {
	tests := []struct {
		name          string
		mapData       map[string]interface{}
		key           string
		expectedError string
	}{
		{
			name: "string value error",
			mapData: map[string]interface{}{
				"enabled": "true",
			},
			key:           "enabled",
			expectedError: "enabled is not bool, value: true",
		},
		{
			name: "int value error",
			mapData: map[string]interface{}{
				"enabled": 1,
			},
			key:           "enabled",
			expectedError: "enabled is not bool, value: 1",
		},
		{
			name: "complex value error",
			mapData: map[string]interface{}{
				"enabled": map[string]interface{}{"value": true},
			},
			key:           "enabled",
			expectedError: "enabled is not bool, value: map[value:true]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := getBoolConfig(tt.mapData, tt.key)

			if err == nil {
				t.Fatal("Expected error, but got nil")
			}

			if err.Error() != tt.expectedError {
				t.Errorf("Expected error message '%s', but got '%s'", tt.expectedError, err.Error())
			}

			if !strings.Contains(err.Error(), "is not bool") {
				t.Error("Error message should contain 'is not bool'")
			}
			if !strings.Contains(err.Error(), tt.key) {
				t.Errorf("Error message should contain the key '%s'", tt.key)
			}
		})
	}
}

func TestGetBoolConfig_DefaultBehavior(t *testing.T) {
	tests := []struct {
		name    string
		mapData map[string]interface{}
		key     string
	}{
		{
			name:    "missing key in empty map",
			mapData: map[string]interface{}{},
			key:     "missing",
		},
		{
			name:    "missing key in populated map",
			mapData: map[string]interface{}{"other": true},
			key:     "missing",
		},
		{
			name:    "nil map",
			mapData: nil,
			key:     "missing",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := getBoolConfig(tt.mapData, tt.key)

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if result != false {
				t.Errorf("Expected default result false, but got %v", result)
			}
		})
	}
}

func TestIsValidEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		expected bool
	}{
		{
			name:     "lowercase letters",
			endpoint: "abcdefghijklmnopqrstuvwxyz",
			expected: true,
		},
		{
			name:     "uppercase letters",
			endpoint: "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
			expected: true,
		},
		{
			name:     "digits",
			endpoint: "0123456789",
			expected: true,
		},
		{
			name:     "mixed alphanumeric",
			endpoint: "abc123XYZ",
			expected: true,
		},
		{
			name:     "single character lowercase",
			endpoint: "a",
			expected: true,
		},
		{
			name:     "single character uppercase",
			endpoint: "A",
			expected: true,
		},
		{
			name:     "single digit",
			endpoint: "0",
			expected: true,
		},

		{
			name:     "empty string",
			endpoint: "",
			expected: false,
		},
		{
			name:     "contains hyphen",
			endpoint: "api-endpoint",
			expected: true,
		},
		{
			name:     "contains underscore",
			endpoint: "api_endpoint",
			expected: true,
		},
		{
			name:     "contains dot",
			endpoint: "api.endpoint",
			expected: false,
		},
		{
			name:     "contains space",
			endpoint: "api endpoint",
			expected: false,
		},
		{
			name:     "contains special characters",
			endpoint: "api@endpoint",
			expected: false,
		},
		{
			name:     "contains slash",
			endpoint: "api/endpoint",
			expected: false,
		},
		{
			name:     "contains backslash",
			endpoint: "api\\endpoint",
			expected: false,
		},
		{
			name:     "contains parentheses",
			endpoint: "api(endpoint)",
			expected: false,
		},
		{
			name:     "contains brackets",
			endpoint: "api[endpoint]",
			expected: false,
		},
		{
			name:     "contains curly braces",
			endpoint: "api{endpoint}",
			expected: false,
		},
		{
			name:     "starts with number",
			endpoint: "123endpoint",
			expected: true,
		},
		{
			name:     "only special characters",
			endpoint: "!@#$%^&*()",
			expected: false,
		},
		{
			name:     "unicode characters",
			endpoint: "中文端点",
			expected: false,
		},
		{
			name:     "emoji characters",
			endpoint: "🚀endpoint",
			expected: false,
		},
		{
			name:     "whitespace only",
			endpoint: "   ",
			expected: false,
		},
		{
			name:     "newline character",
			endpoint: "endpoint\n",
			expected: false,
		},
		{
			name:     "tab character",
			endpoint: "endpoint\t",
			expected: false,
		},
		{
			name:     "null character",
			endpoint: "endpoint\000",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isValidEndpoint(tt.endpoint)
			if result != tt.expected {
				t.Errorf("isValidEndpoint(%q) = %v, want %v", tt.endpoint, result, tt.expected)
			}
		})
	}
}

func TestIsValidEndpoint_BoundaryValues(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		expected bool
	}{
		{
			name:     "minimum valid character 'a'",
			endpoint: "a",
			expected: true,
		},
		{
			name:     "minimum valid character 'A'",
			endpoint: "A",
			expected: true,
		},
		{
			name:     "minimum valid digit '0'",
			endpoint: "0",
			expected: true,
		},
		{
			name:     "maximum valid character 'z'",
			endpoint: "z",
			expected: true,
		},
		{
			name:     "maximum valid character 'Z'",
			endpoint: "Z",
			expected: true,
		},
		{
			name:     "maximum valid digit '9'",
			endpoint: "9",
			expected: true,
		},
		{
			name:     "character before 'a'",
			endpoint: "`", // ASCII 96, before 'a' (97)
			expected: false,
		},
		{
			name:     "character after 'z'",
			endpoint: "{", // ASCII 123, after 'z' (122)
			expected: false,
		},
		{
			name:     "character before 'A'",
			endpoint: "@", // ASCII 64, before 'A' (65)
			expected: false,
		},
		{
			name:     "character after 'Z'",
			endpoint: "[", // ASCII 91, after 'Z' (90)
			expected: false,
		},
		{
			name:     "character before '0'",
			endpoint: "/", // ASCII 47, before '0' (48)
			expected: false,
		},
		{
			name:     "character after '9'",
			endpoint: ":", // ASCII 58, after '9' (57)
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isValidEndpoint(tt.endpoint)
			if result != tt.expected {
				t.Errorf("isValidEndpoint(%q) = %v, want %v", tt.endpoint, result, tt.expected)
			}
		})
	}
}

func TestIsValidIdentifier(t *testing.T) {
	tests := []struct {
		name       string
		identifier string
		expected   bool
	}{
		{
			name:       "empty string",
			identifier: "",
			expected:   true,
		},
		{
			name:       "alphanumeric",
			identifier: "abc123XYZ",
			expected:   true,
		},
		{
			name:       "with spaces",
			identifier: "hello world",
			expected:   true,
		},
		{
			name:       "with special characters except backtick",
			identifier: "hello-world_123.test",
			expected:   true,
		},
		{
			name:       "with unicode",
			identifier: "中文标识符",
			expected:   true,
		},
		{
			name:       "with emoji",
			identifier: "🚀rocket",
			expected:   true,
		},
		{
			name:       "with hyphen",
			identifier: "user-name",
			expected:   true,
		},
		{
			name:       "with underscore",
			identifier: "user_name",
			expected:   true,
		},
		{
			name:       "with dot",
			identifier: "file.name",
			expected:   true,
		},
		{
			name:       "with at symbol",
			identifier: "user@domain",
			expected:   true,
		},
		{
			name:       "with dollar sign",
			identifier: "variable$name",
			expected:   true,
		},
		{
			name:       "with percent sign",
			identifier: "100%",
			expected:   true,
		},
		{
			name:       "with parentheses",
			identifier: "name(with)parentheses",
			expected:   true,
		},
		{
			name:       "with brackets",
			identifier: "array[index]",
			expected:   true,
		},
		{
			name:       "with curly braces",
			identifier: "template{value}",
			expected:   true,
		},

		{
			name:       "contains single backtick",
			identifier: "name`with`backtick",
			expected:   false,
		},
		{
			name:       "starts with backtick",
			identifier: "`name",
			expected:   false,
		},
		{
			name:       "ends with backtick",
			identifier: "name`",
			expected:   false,
		},
		{
			name:       "only backtick",
			identifier: "`",
			expected:   false,
		},
		{
			name:       "multiple backticks",
			identifier: "``name``",
			expected:   false,
		},
		{
			name:       "backtick in middle",
			identifier: "hello`world",
			expected:   false,
		},
		{
			name:       "mixed invalid characters with backtick",
			identifier: "invalid`name@test",
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isValidIdentifier(tt.identifier)
			if result != tt.expected {
				t.Errorf("isValidIdentifier(%q) = %v, want %v", tt.identifier, result, tt.expected)
			}
		})
	}
}

func TestIsValidIdentifier_EdgeCases(t *testing.T) {
	tests := []struct {
		name       string
		identifier string
		expected   bool
	}{
		{
			name:       "very long valid identifier",
			identifier: strings.Repeat("a", 1000),
			expected:   true,
		},
		{
			name:       "very long invalid identifier with backtick at end",
			identifier: strings.Repeat("a", 999) + "`",
			expected:   false,
		},
		{
			name:       "whitespace only",
			identifier: "   ",
			expected:   true,
		},
		{
			name:       "newline character without backtick",
			identifier: "line1\nline2",
			expected:   true,
		},
		{
			name:       "tab character without backtick",
			identifier: "column1\tcolumn2",
			expected:   true,
		},
		{
			name:       "null character without backtick",
			identifier: "text\000more",
			expected:   true,
		},
		{
			name:       "newline with backtick",
			identifier: "line1`\nline2",
			expected:   false,
		},
		{
			name:       "mixed whitespace with backtick",
			identifier: "  `  ",
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isValidIdentifier(tt.identifier)
			if result != tt.expected {
				t.Errorf("isValidIdentifier(%q) = %v, want %v", tt.identifier, result, tt.expected)
			}
		})
	}
}

func TestConfig_validate(t *testing.T) {
	type fields struct {
		Enable bool
		Rules  []*Rule
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "valid config",
			fields: fields{
				Enable: true,
				Rules: []*Rule{
					{
						Endpoint:      "Rule1",
						DB:            "test_db",
						SuperTable:    "test_super_table",
						SubTable:      "test_sub_table",
						TimeKey:       "ts",
						TimeFormat:    "unix",
						Timezone:      "utc",
						TimeFieldName: "ts",
						Fields: []*Field{
							{Key: "Field1", Optional: false},
						},
					},
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "disabled config",
			fields: fields{
				Enable: false,
				Rules:  nil,
			},
			wantErr: assert.NoError,
		},
		{
			name: "no rules",
			fields: fields{
				Enable: true,
				Rules:  nil,
			},
			wantErr: assert.Error,
		},
		{
			name: "invalid endpoint",
			fields: fields{
				Enable: true,
				Rules: []*Rule{
					{
						Endpoint: "Invalid Endpoint!",
					},
				},
			},
			wantErr: assert.Error,
		},
		{
			name: "no endpoint",
			fields: fields{
				Enable: true,
				Rules: []*Rule{
					{
						Endpoint: "",
					},
				},
			},
			wantErr: assert.Error,
		},
		{
			name: "no db and dbkey",
			fields: fields{
				Enable: true,
				Rules: []*Rule{
					{
						Endpoint: "Rule1",
					},
				},
			},
			wantErr: assert.Error,
		},
		{
			name: "no supertable and supertablekey",
			fields: fields{
				Enable: true,
				Rules: []*Rule{
					{
						Endpoint: "Rule1",
						DB:       "test_db",
					},
				},
			},
			wantErr: assert.Error,
		},
		{
			name: "no subtable and subtablekey",
			fields: fields{
				Enable: true,
				Rules: []*Rule{
					{
						Endpoint:   "Rule1",
						DB:         "test_db",
						SuperTable: "test_super_table",
					},
				},
			},
			wantErr: assert.Error,
		},
		{
			name: "no timeFormat",
			fields: fields{
				Enable: true,
				Rules: []*Rule{
					{
						Endpoint:   "Rule1",
						DB:         "test_db",
						SuperTable: "test_super_table",
						SubTable:   "test_sub_table",
						TimeKey:    "ts",
					},
				},
			},
			wantErr: assert.Error,
		},
		{
			name: "invalid db name",
			fields: fields{
				Enable: true,
				Rules: []*Rule{
					{
						Endpoint:   "Rule1",
						DB:         "db`1",
						SuperTable: "test_super_table",
						SubTable:   "test_sub_table",
						TimeFormat: "unix",
					},
				},
			},
			wantErr: assert.Error,
		},
		{
			name: "invalid supertable name",
			fields: fields{
				Enable: true,
				Rules: []*Rule{
					{
						Endpoint:   "Rule1",
						DB:         "test_db",
						SuperTable: "super`table",
						SubTable:   "test_sub_table",
						TimeFormat: "unix",
					},
				},
			},
			wantErr: assert.Error,
		},
		{
			name: "invalid subtable name",
			fields: fields{
				Enable: true,
				Rules: []*Rule{
					{
						Endpoint:   "Rule1",
						DB:         "test_db",
						SuperTable: "test_super_table",
						SubTable:   "sub`table",
						TimeFormat: "unix",
					},
				},
			},
			wantErr: assert.Error,
		},
		{
			name: "no fields",
			fields: fields{
				Enable: true,
				Rules: []*Rule{
					{
						Endpoint:   "Rule1",
						DB:         "test_db",
						SuperTable: "test_super_table",
						SubTable:   "test_sub_table",
						TimeFormat: "unix",
						Fields:     nil,
					},
				},
			}, wantErr: assert.Error,
		},
		{
			name: "invalid field key",
			fields: fields{
				Enable: true,
				Rules: []*Rule{
					{
						Endpoint:   "Rule1",
						DB:         "test_db",
						SuperTable: "test_super_table",
						SubTable:   "test_sub_table",
						TimeFormat: "unix",
						Fields: []*Field{
							{Key: "Invalid`Key", Optional: false},
						},
					},
				},
			},
			wantErr: assert.Error,
		},
		{
			name: "duplicate field key",
			fields: fields{
				Enable: true,
				Rules: []*Rule{
					{
						Endpoint:   "Rule1",
						DB:         "test_db",
						SuperTable: "test_super_table",
						SubTable:   "test_sub_table",
						TimeFormat: "unix",
						Fields: []*Field{
							{Key: "Field1", Optional: false},
							{Key: "Field1", Optional: true},
						},
					},
				},
			},
			wantErr: assert.Error,
		},
		{
			name: "empty field key",
			fields: fields{
				Enable: true,
				Rules: []*Rule{
					{
						Endpoint:   "Rule1",
						DB:         "test_db",
						SuperTable: "test_super_table",
						SubTable:   "test_sub_table",
						TimeFormat: "unix",
						Fields: []*Field{
							{Key: "", Optional: false},
						},
					},
				},
			},
			wantErr: assert.Error,
		},
		{
			name: "db and dbkey both set",
			fields: fields{
				Enable: true,
				Rules: []*Rule{
					{
						Endpoint: "Rule1",
						DB:       "test_db",
						DBKey:    "db_key",
					},
				},
			},
			wantErr: assert.Error,
		},
		{
			name: "supertable and supertablekey both set",
			fields: fields{
				Enable: true,
				Rules: []*Rule{
					{
						Endpoint:      "Rule1",
						DB:            "test_db",
						SuperTable:    "test_super_table",
						SuperTableKey: "super_table_key",
					},
				},
			},
			wantErr: assert.Error,
		},
		{
			name: "subtable and subtablekey both set",
			fields: fields{
				Enable: true,
				Rules: []*Rule{
					{
						Endpoint:    "Rule1",
						DB:          "test_db",
						SuperTable:  "test_super_table",
						SubTable:    "test_sub_table",
						SubTableKey: "sub_table_key",
					},
				},
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Config{
				Enable: tt.fields.Enable,
				Rules:  tt.fields.Rules,
			}
			tt.wantErr(t, c.validate(), "validate()")
		})
	}
}
