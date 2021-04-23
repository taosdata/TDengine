package dataimport

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/pelletier/go-toml"
)

var (
	cfg  Config
	once sync.Once
)

// Config include all scene import config
type Config struct {
	UserCases map[string]CaseConfig
}

// CaseConfig include the sample data config and tdengine config
type CaseConfig struct {
	Format              string
	FilePath            string
	Separator           string
	StName              string
	SubTableName        string
	Timestamp           string
	TimestampType       string
	TimestampTypeFormat string
	Tags                []FieldInfo
	Fields              []FieldInfo
}

// FieldInfo is field or tag info
type FieldInfo struct {
	Name string
	Type string
}

// LoadConfig will load the specified file config
func LoadConfig(filePath string) Config {
	once.Do(func() {
		filePath, err := filepath.Abs(filePath)
		if err != nil {
			panic(err)
		}
		fmt.Printf("parse toml file once. filePath: %s\n", filePath)
		tree, err := toml.LoadFile(filePath)
		if err != nil {
			panic(err)
		}

		bytes, err := json.Marshal(tree.ToMap())
		if err != nil {
			panic(err)
		}

		err = json.Unmarshal(bytes, &cfg.UserCases)
		if err != nil {
			panic(err)
		}
	})
	return cfg
}
