package util

import (
	"os"
	"strconv"
	"strings"

	"github.com/taosdata/taoskeeper/infrastructure/config"
)

//https://github.com/containerd/cgroups/blob/main/utils.go

func ReadUint(path string) (uint64, error) {
	v, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return ParseUint(strings.TrimSpace(string(v)), 10, 64)
}

func ParseUint(s string, base, bitSize int) (uint64, error) {
	v, err := strconv.ParseUint(s, base, bitSize)
	if err != nil {
		intValue, intErr := strconv.ParseInt(s, base, bitSize)
		// 1. Handle negative values greater than MinInt64 (and)
		// 2. Handle negative values lesser than MinInt64
		if intErr == nil && intValue < 0 {
			return 0, nil
		} else if intErr != nil &&
			intErr.(*strconv.NumError).Err == strconv.ErrRange &&
			intValue < 0 {
			return 0, nil
		}
		return 0, err
	}
	return v, nil
}

func EscapeInfluxProtocol(s string) string {
	s = strings.TrimSuffix(s, "\\")
	s = strings.ReplaceAll(s, ",", "\\,")
	s = strings.ReplaceAll(s, "=", "\\=")
	s = strings.ReplaceAll(s, " ", "\\ ")
	s = strings.ReplaceAll(s, "\"", "\\\"")
	return s
}

func GetCfg() *config.Config {
	c := &config.Config{
		Port: 6043,
		TDengine: config.TDengineRestful{
			Host:     "127.0.0.1",
			Port:     6041,
			Username: "root",
			Password: "taosdata",
			Usessl:   false,
		},
		Metrics: config.MetricsConfig{
			Database: config.Database{
				Name:    "keeper_test_log",
				Options: map[string]interface{}{},
			},
		},
	}
	return c
}

func SafeSubstring(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
