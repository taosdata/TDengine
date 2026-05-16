package unified

import (
	"errors"
	"os"
	"strings"
	"testing"

	taosErrors "github.com/taosdata/driver-go/v3/errors"
)

// TestUnifiedOpenRealAdapter verifies Open wrappers with real adapter.
func TestUnifiedOpenRealAdapter(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	tmqIntegrationSQL(t, "select 1")

	dsn := strings.TrimSpace(os.Getenv("UNIFIED_IT_DSN"))
	if dsn == "" {
		dsn = "root:taosdata@ws(127.0.0.1:6041)/"
	}

	client, err := Open(dsn)
	if err != nil {
		t.Fatalf("integration test requires taosadapter/taosd: unified.Open failed: %v", err)
	}
	_, err = client.Exec(0, "select 1")
	if err != nil {
		client.Close()
		t.Fatalf("select failed after Open: %v", err)
	}
	client.Close()

	driverOpenClient, err := NewDSNDriver("").Open(dsn)
	if err != nil {
		t.Fatalf("integration test requires taosadapter/taosd: DSNDriver.Open failed: %v", err)
	}
	_, err = driverOpenClient.Exec(0, "select 1")
	if err != nil {
		driverOpenClient.Close()
		t.Fatalf("select failed after DSNDriver.Open: %v", err)
	}
	driverOpenClient.Close()
}

// TestUnifiedExecIllegalSQLReturnsTaosError verifies illegal SQL returns driver-go taos error type instead of unified.Error.
func TestUnifiedExecIllegalSQLReturnsTaosError(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	tmqIntegrationSQL(t, "select 1")

	dsn := strings.TrimSpace(os.Getenv("UNIFIED_IT_DSN"))
	if dsn == "" {
		dsn = "root:taosdata@ws(127.0.0.1:6041)/"
	}

	client, err := Open(dsn)
	if err != nil {
		t.Fatalf("integration test requires taosadapter/taosd: unified.Open failed: %v", err)
	}
	defer client.Close()

	_, err = client.Exec(0, "xxxxxxx inot")
	if err == nil {
		t.Fatal("expected illegal SQL to fail")
	}

	var taosErr *taosErrors.TaosError
	if !errors.As(err, &taosErr) {
		t.Fatalf("expected taos error type, got %T: %v", err, err)
	}

	var unifiedErr *Error
	if errors.As(err, &unifiedErr) {
		t.Fatalf("expected non-unified error for illegal SQL, got unified error: %+v", unifiedErr)
	}
}
