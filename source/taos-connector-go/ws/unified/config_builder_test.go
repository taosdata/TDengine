package unified

import (
	"testing"
	"time"

	"github.com/taosdata/driver-go/v3/common"
)

// TestBuildConnectionConfigUsesUnifiedDefaults verifies the expected behavior for this scenario.
func TestBuildConnectionConfigUsesUnifiedDefaults(t *testing.T) {
	out := BuildConnectionConfig(&Config{}, ConnectionConfigDefaults{})
	if out == nil {
		t.Fatal("expected non-nil config")
		return
	}
	if out.ChanLength != 1 {
		t.Fatalf("unexpected channel length: %d", out.ChanLength)
	}
	if out.ReadTimeout != common.DefaultMessageTimeout {
		t.Fatalf("unexpected read timeout: %v", out.ReadTimeout)
	}
	if out.WriteTimeout != common.DefaultWriteWait {
		t.Fatalf("write timeout should default to %v, got %v", common.DefaultWriteWait, out.WriteTimeout)
	}
	if out.ReconnectIntervalMs != 2000 || out.ReconnectRetryCount != 3 {
		t.Fatalf("unexpected reconnect defaults: interval=%d retry=%d", out.ReconnectIntervalMs, out.ReconnectRetryCount)
	}
}

// TestBuildConnectionConfigAppliesCompatibilityDefaults verifies the expected behavior for this scenario.
func TestBuildConnectionConfigAppliesCompatibilityDefaults(t *testing.T) {
	in := &Config{
		Endpoints: []string{"ws://127.0.0.1:6041/ws"},
	}
	out := BuildConnectionConfig(in, TaosWSConnectionDefaults)
	if out == nil {
		t.Fatal("expected non-nil config")
		return
	}
	if out.User != common.DefaultUser || out.Passwd != common.DefaultPassword {
		t.Fatalf("unexpected auth defaults: user=%q passwd=%q", out.User, out.Passwd)
	}
	if out.Net != "ws" || out.Addr != "127.0.0.1" || out.Port != common.DefaultHttpPort {
		t.Fatalf("unexpected endpoint defaults: net=%q addr=%q port=%d", out.Net, out.Addr, out.Port)
	}
	if out.ReconnectRetryCount != 3 {
		t.Fatalf("unexpected reconnect retry count: %d", out.ReconnectRetryCount)
	}
	if out.WriteTimeout != common.DefaultWriteWait {
		t.Fatalf("unexpected write timeout: %v", out.WriteTimeout)
	}
}

// TestBuildConnectionConfigPreservesProvidedValues verifies the expected behavior for this scenario.
func TestBuildConnectionConfigPreservesProvidedValues(t *testing.T) {
	in := &Config{
		Endpoints:           []string{"ws://127.0.0.1:6041/ws"},
		ChanLength:          8,
		ReconnectIntervalMs: 1234,
		ReconnectRetryCount: 9,
		User:                "user",
		Passwd:              "passwd",
		Net:                 "wss",
		Addr:                "cloud.example.com",
		Port:                443,
		ReadTimeout:         3 * time.Second,
		WriteTimeout:        2 * time.Second,
	}
	out := BuildConnectionConfig(in, TaosWSConnectionDefaults)
	if out == nil {
		t.Fatal("expected non-nil config")
		return
	}
	if out.ChanLength != 8 || out.ReconnectIntervalMs != 1234 || out.ReconnectRetryCount != 9 {
		t.Fatalf("unexpected runtime overwrite: %+v", out)
	}
	if out.User != "user" || out.Passwd != "passwd" || out.Net != "wss" || out.Addr != "cloud.example.com" || out.Port != 443 {
		t.Fatalf("unexpected connection overwrite: %+v", out)
	}
	if out.ReadTimeout != 3*time.Second || out.WriteTimeout != 2*time.Second {
		t.Fatalf("unexpected timeout overwrite: read=%v write=%v", out.ReadTimeout, out.WriteTimeout)
	}
	in.Endpoints[0] = "ws://mutated"
	if len(out.Endpoints) != 1 || out.Endpoints[0] != "ws://127.0.0.1:6041/ws" {
		t.Fatalf("expected endpoints copied, got %+v", out.Endpoints)
	}
}
