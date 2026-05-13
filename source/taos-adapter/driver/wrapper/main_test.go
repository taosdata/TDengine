package wrapper

import (
	"os"
	"testing"

	"github.com/taosdata/taosadapter/v3/driver/common"
)

func TestMain(m *testing.M) {
	TaosOptions(common.TSDB_OPTION_USE_ADAPTER, "true")
	os.Exit(m.Run())
}
