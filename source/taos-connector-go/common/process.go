package common

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/taosdata/driver-go/v3/version"
)

var processName string
var once sync.Once

func GetProcessName() string {
	once.Do(func() {
		processPath := os.Args[0]
		processName = filepath.Base(processPath)
		if processName == "" {
			processName = "go_unknown"
		}
	})
	return processName
}

// GetConnectorInfo returns connector information string
func GetConnectorInfo(connectType string) string {
	return fmt.Sprintf("go-%s-%s-%s", connectType, version.Tag, version.Commit)
}
