package utils

import (
	"encoding/json"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Port     uint16 `json:"port,omitempty" yaml:"port,omitempty"`
	Database string `json:"database,omitempty" yaml:"database,omitempty"`
	RuleFile string `json:"ruleFile,omitempty" yaml:"ruleFile,omitempty"`
	Log      struct {
		Level string `json:"level,omitempty" yaml:"level,omitempty"`
		Path  string `json:"path,omitempty" yaml:"path,omitempty"`
	} `json:"log" yaml:"log"`
	TDengine  string `json:"tdengine,omitempty" yaml:"tdengine,omitempty"`
	Receivers struct {
		AlertManager string `json:"alertManager,omitempty" yaml:"alertManager,omitempty"`
		Console      bool   `json:"console"`
	} `json:"receivers" yaml:"receivers"`
}

var Cfg Config

func LoadConfig(path string) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()

	e = yaml.NewDecoder(f).Decode(&Cfg)
	if e != nil {
		f.Seek(0, 0)
		e = json.NewDecoder(f).Decode(&Cfg)
	}

	return e
}
