package unified

import (
	"reflect"
	"testing"
	"time"

	"github.com/taosdata/driver-go/v3/common"
)

// TestNormalizeEndpointsDefaultPath verifies the expected behavior for this scenario.
func TestNormalizeEndpointsDefaultPath(t *testing.T) {
	got, err := NormalizeEndpoints([]string{"ws://127.0.0.1:6041", "wss://cloud:6041/"}, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"ws://127.0.0.1:6041/ws", "wss://cloud:6041/ws"}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

// TestNormalizeEndpointsKeepPath verifies the expected behavior for this scenario.
func TestNormalizeEndpointsKeepPath(t *testing.T) {
	got, err := NormalizeEndpoints([]string{"ws://127.0.0.1:6041/rest/tmq?x=1"}, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"ws://127.0.0.1:6041/rest/tmq?x=1"}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

// TestNormalizeEndpointsInvalidScheme verifies the expected behavior for this scenario.
func TestNormalizeEndpointsInvalidScheme(t *testing.T) {
	_, err := NormalizeEndpoints([]string{"http://127.0.0.1:6041"}, "/ws")
	if err == nil {
		t.Fatal("expect invalid websocket endpoint scheme error")
	}
}

// TestConfigNormalizeDefaultValues verifies the expected behavior for this scenario.
func TestConfigNormalizeDefaultValues(t *testing.T) {
	cfg := NewConfig([]string{"ws://127.0.0.1:6041"})
	cfg.ReadTimeout = 0
	cfg.WriteTimeout = 0
	cfg.ReconnectIntervalMs = 0
	cfg.ReconnectRetryCount = 0
	if err := cfg.Normalize("/ws"); err != nil {
		t.Fatal(err)
	}
	if cfg.Endpoints[0] != "ws://127.0.0.1:6041/ws" {
		t.Fatalf("unexpected endpoint: %s", cfg.Endpoints[0])
	}
	if cfg.ReadTimeout != common.DefaultMessageTimeout {
		t.Fatalf("unexpected read timeout: %v", cfg.ReadTimeout)
	}
	if cfg.WriteTimeout != common.DefaultWriteWait {
		t.Fatalf("unexpected write timeout: %v", cfg.WriteTimeout)
	}
	if cfg.ReconnectIntervalMs != 2000 {
		t.Fatalf("unexpected reconnect interval: %d", cfg.ReconnectIntervalMs)
	}
	if cfg.ReconnectRetryCount != 3 {
		t.Fatalf("unexpected reconnect retry count: %d", cfg.ReconnectRetryCount)
	}
}

// TestConfigNormalizeKeepUserValues verifies the expected behavior for this scenario.
func TestConfigNormalizeKeepUserValues(t *testing.T) {
	cfg := NewConfig([]string{"wss://cluster-a:443/ws"})
	cfg.ReadTimeout = 15 * time.Second
	cfg.WriteTimeout = 30 * time.Second
	cfg.ReconnectIntervalMs = 500
	cfg.ReconnectRetryCount = 5
	if err := cfg.Normalize("/ws"); err != nil {
		t.Fatal(err)
	}
	if cfg.ReadTimeout != 15*time.Second {
		t.Fatalf("unexpected read timeout: %v", cfg.ReadTimeout)
	}
	if cfg.WriteTimeout != 30*time.Second {
		t.Fatalf("unexpected write timeout: %v", cfg.WriteTimeout)
	}
	if cfg.ReconnectIntervalMs != 500 {
		t.Fatalf("unexpected reconnect interval: %d", cfg.ReconnectIntervalMs)
	}
	if cfg.ReconnectRetryCount != 5 {
		t.Fatalf("unexpected reconnect retry count: %d", cfg.ReconnectRetryCount)
	}
}

// TestConfigNormalizeReadTimeoutCustomWriteUsesDefault verifies write timeout
// keeps its own default even when read timeout is customized.
func TestConfigNormalizeReadTimeoutCustomWriteUsesDefault(t *testing.T) {
	cfg := NewConfig([]string{"wss://cluster-a:443/ws"})
	cfg.ReadTimeout = 12 * time.Second
	cfg.WriteTimeout = 0
	if err := cfg.Normalize("/ws"); err != nil {
		t.Fatal(err)
	}
	if cfg.ReadTimeout != 12*time.Second {
		t.Fatalf("unexpected read timeout: %v", cfg.ReadTimeout)
	}
	if cfg.WriteTimeout != common.DefaultWriteWait {
		t.Fatalf("write timeout should default to %v, got: %v", common.DefaultWriteWait, cfg.WriteTimeout)
	}
}

// TestNormalizeEndpointsDeduplication verifies the expected behavior for this scenario.
func TestNormalizeEndpointsDeduplication(t *testing.T) {
	// Test deduplication of identical endpoints
	got, err := NormalizeEndpoints([]string{
		"ws://127.0.0.1:6041",
		"ws://127.0.0.1:6041/",
		"ws://127.0.0.1:6041",
		"ws://host2:6041",
	}, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"ws://127.0.0.1:6041/ws", "ws://host2:6041/ws"}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

// TestConfigNormalizeBackwardCompatibility verifies the expected behavior for this scenario.
func TestConfigNormalizeBackwardCompatibility(t *testing.T) {
	// Test backward compatibility: Addr/Port converted to Endpoints
	cfg := &Config{
		Net:  "ws",
		Addr: "192.168.1.100",
		Port: 6041,
	}
	if err := cfg.Normalize("/ws"); err != nil {
		t.Fatal(err)
	}
	want := []string{"ws://192.168.1.100:6041/ws"}
	if !reflect.DeepEqual(want, cfg.Endpoints) {
		t.Fatalf("want %v, got %v", want, cfg.Endpoints)
	}
}

// TestConfigNormalizeBackwardCompatibilityIPv6 verifies the expected behavior for this scenario.
func TestConfigNormalizeBackwardCompatibilityIPv6(t *testing.T) {
	cfg := &Config{
		Net:  "ws",
		Addr: "::1",
		Port: 6041,
	}
	if err := cfg.Normalize("/ws"); err != nil {
		t.Fatal(err)
	}
	want := []string{"ws://[::1]:6041/ws"}
	if !reflect.DeepEqual(want, cfg.Endpoints) {
		t.Fatalf("want %v, got %v", want, cfg.Endpoints)
	}
}

// TestConfigNormalizeBackwardCompatibilityBracketedIPv6 verifies the expected behavior for this scenario.
func TestConfigNormalizeBackwardCompatibilityBracketedIPv6(t *testing.T) {
	cfg := &Config{
		Net:  "ws",
		Addr: "[::1]",
		Port: 6041,
	}
	if err := cfg.Normalize("/ws"); err != nil {
		t.Fatal(err)
	}
	want := []string{"ws://[::1]:6041/ws"}
	if !reflect.DeepEqual(want, cfg.Endpoints) {
		t.Fatalf("want %v, got %v", want, cfg.Endpoints)
	}
}

// TestConfigNormalizeBackwardCompatibilityWithToken verifies the expected behavior for this scenario.
func TestConfigNormalizeBackwardCompatibilityWithToken(t *testing.T) {
	// Test backward compatibility with token
	cfg := &Config{
		Net:   "wss",
		Addr:  "cloud.example.com",
		Port:  443,
		Token: "mytoken123",
	}
	if err := cfg.Normalize("/ws"); err != nil {
		t.Fatal(err)
	}
	if len(cfg.Endpoints) != 1 {
		t.Fatalf("expected 1 endpoint, got %d", len(cfg.Endpoints))
	}
	// Should contain token in query string
	if cfg.Endpoints[0] != "wss://cloud.example.com:443/ws?token=mytoken123" {
		t.Fatalf("unexpected endpoint: %s", cfg.Endpoints[0])
	}
}
