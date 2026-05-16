package recordsql

import (
	"os"
	"testing"

	"github.com/taosdata/taosadapter/v3/config"
)

func TestMain(m *testing.M) {
	config.Init()
	code := m.Run()
	os.Exit(code)
}
