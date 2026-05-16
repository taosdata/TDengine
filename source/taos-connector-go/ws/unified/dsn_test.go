package unified

import (
	"reflect"
	"testing"
	"time"

	"github.com/taosdata/driver-go/v3/common"
)

// TestParseDSNInvalidNoSlash verifies the expected behavior for this scenario.
func TestParseDSNInvalidNoSlash(t *testing.T) {
	_, err := ParseDSN("abcd")
	if err == nil {
		t.Fatal("expect invalid dsn error")
	}
	if err.Error() != "invalid DSN: missing the slash separating the database name" {
		t.Fatalf("unexpected error: %s", err.Error())
	}
}

// TestParseDSNCommon verifies the expected behavior for this scenario.
func TestParseDSNCommon(t *testing.T) {
	cfg, err := ParseDSN("user:passwd@ws(fqdn:6041)/dbname")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.User != "user" || cfg.Passwd != "passwd" || cfg.Net != "ws" || cfg.Addr != "fqdn" || cfg.Port != 6041 || cfg.DbName != "dbname" {
		t.Fatalf("unexpected cfg: %+v", cfg)
	}
	if !cfg.InterpolateParams {
		t.Fatal("expect interpolate params default true")
	}
}

// TestParseDSNMultiAddrList verifies the expected behavior for this scenario.
func TestParseDSNMultiAddrList(t *testing.T) {
	cfg, err := ParseDSN("user:passwd@ws(a:6041,b:6042)/db")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != "a" || cfg.Port != 6041 {
		t.Fatalf("unexpected first addr: host=%s port=%d", cfg.Addr, cfg.Port)
	}
	want := []string{"ws://a:6041", "ws://b:6042"}
	if !reflect.DeepEqual(want, cfg.Endpoints) {
		t.Fatalf("want %v, got %v", want, cfg.Endpoints)
	}
}

// TestParseDSNMultiAddrListWithToken verifies the expected behavior for this scenario.
func TestParseDSNMultiAddrListWithToken(t *testing.T) {
	cfg, err := ParseDSN("user:passwd@ws(a:6041,b:6042)/db?token=abc")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"ws://a:6041?token=abc", "ws://b:6042?token=abc"}
	if !reflect.DeepEqual(want, cfg.Endpoints) {
		t.Fatalf("want %v, got %v", want, cfg.Endpoints)
	}
}

// TestNewConfigFromDSN verifies the expected behavior for this scenario.
func TestNewConfigFromDSN(t *testing.T) {
	cfg, err := NewConfigFromDSN("user:passwd@ws(127.0.0.1:6041)/db?readTimeout=5s&writeTimeout=2s", "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Endpoints) != 1 || cfg.Endpoints[0] != "ws://127.0.0.1:6041/ws" {
		t.Fatalf("unexpected endpoints: %+v", cfg.Endpoints)
	}
	if cfg.User != "user" || cfg.Passwd != "passwd" || cfg.DbName != "db" {
		t.Fatalf("unexpected auth/db: %+v", cfg)
	}
	if cfg.ReadTimeout != 5*time.Second || cfg.WriteTimeout != 2*time.Second {
		t.Fatalf("unexpected timeouts: read=%v write=%v", cfg.ReadTimeout, cfg.WriteTimeout)
	}
}

// TestNewConfigFromDSNWithoutNetPrefix verifies the expected behavior for this scenario.
func TestNewConfigFromDSNWithoutNetPrefix(t *testing.T) {
	cfg, err := NewConfigFromDSN("user:passwd@(localhost:6041)/db", "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Endpoints) != 1 || cfg.Endpoints[0] != "ws://localhost:6041/ws" {
		t.Fatalf("unexpected endpoints: %+v", cfg.Endpoints)
	}
	if cfg.User != "user" || cfg.Passwd != "passwd" || cfg.DbName != "db" {
		t.Fatalf("unexpected auth/db: %+v", cfg)
	}
}

// TestNewConfigFromDSNWriteTimeoutOnly verifies the expected behavior for this scenario.
func TestNewConfigFromDSNWriteTimeoutOnly(t *testing.T) {
	cfg, err := NewConfigFromDSN("user:passwd@ws(127.0.0.1:6041)/db?writeTimeout=2s", "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ReadTimeout != 5*time.Minute || cfg.WriteTimeout != 2*time.Second {
		t.Fatalf("unexpected timeouts: read=%v write=%v", cfg.ReadTimeout, cfg.WriteTimeout)
	}
}

// TestNewConfigFromDSNReadTimeoutOnly verifies read timeout override keeps
// write timeout at default write wait for compatibility.
func TestNewConfigFromDSNReadTimeoutOnly(t *testing.T) {
	cfg, err := NewConfigFromDSN("user:passwd@ws(127.0.0.1:6041)/db?readTimeout=2s", "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ReadTimeout != 2*time.Second || cfg.WriteTimeout != common.DefaultWriteWait {
		t.Fatalf("unexpected timeouts: read=%v write=%v", cfg.ReadTimeout, cfg.WriteTimeout)
	}
}

// TestNewConfigFromDSNHostOmittedSingleNode verifies the expected behavior for this scenario.
func TestNewConfigFromDSNHostOmittedSingleNode(t *testing.T) {
	cfg, err := NewConfigFromDSN("user:passwd@ws(:6041)/db", "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Endpoints) != 1 || cfg.Endpoints[0] != "ws://127.0.0.1:6041/ws" {
		t.Fatalf("unexpected endpoints: %+v", cfg.Endpoints)
	}
}

// TestNewConfigFromDSNMultiAddrList verifies the expected behavior for this scenario.
func TestNewConfigFromDSNMultiAddrList(t *testing.T) {
	cfg, err := NewConfigFromDSN("user:passwd@ws(a:6041,b:6042)/db", "/ws")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"ws://a:6041/ws", "ws://b:6042/ws"}
	if !reflect.DeepEqual(want, cfg.Endpoints) {
		t.Fatalf("want %v, got %v", want, cfg.Endpoints)
	}
}

// TestParseDSNMultiAddrListIPv6Failover verifies IPv6 multi-node DSN parsing.
func TestParseDSNMultiAddrListIPv6Failover(t *testing.T) {
	cfg, err := ParseDSN("user:passwd@wss([2001:db8::1]:6041,[2001:db8::2]:6042,[2001:db8::3]:6043)/db?token=tk1")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Net != "wss" || cfg.Addr != "2001:db8::1" || cfg.Port != 6041 {
		t.Fatalf("unexpected primary endpoint fields: net=%s addr=%s port=%d", cfg.Net, cfg.Addr, cfg.Port)
	}
	want := []string{
		"wss://[2001:db8::1]:6041?token=tk1",
		"wss://[2001:db8::2]:6042?token=tk1",
		"wss://[2001:db8::3]:6043?token=tk1",
	}
	if !reflect.DeepEqual(want, cfg.Endpoints) {
		t.Fatalf("want %v, got %v", want, cfg.Endpoints)
	}
}

// TestParseDSNMultiParamsCombination verifies multi-param parsing with multi-node failover DSN.
func TestParseDSNMultiParamsCombination(t *testing.T) {
	cfg, err := ParseDSN("u:p@ws([2001:db8::1]:6041,127.0.0.1:6042)/db?interpolateParams=false&token=tk1&enableCompression=true&readTimeout=2s&writeTimeout=3s&timezone=Asia%2FShanghai&bearerToken=b1&totpCode=123456&region=cn-north-1&charset=UTF-8")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != "2001:db8::1" || cfg.Port != 6041 {
		t.Fatalf("unexpected primary endpoint fields: addr=%s port=%d", cfg.Addr, cfg.Port)
	}
	if cfg.InterpolateParams {
		t.Fatal("expect interpolate params false")
	}
	if !cfg.EnableCompression {
		t.Fatal("expect enableCompression true")
	}
	if cfg.ReadTimeout != 2*time.Second || cfg.WriteTimeout != 3*time.Second {
		t.Fatalf("unexpected timeouts: read=%v write=%v", cfg.ReadTimeout, cfg.WriteTimeout)
	}
	if cfg.Timezone == nil || cfg.Timezone.String() != "Asia/Shanghai" {
		t.Fatalf("unexpected timezone: %+v", cfg.Timezone)
	}
	if cfg.BearerToken != "b1" || cfg.TotpCode != "123456" || cfg.Token != "tk1" {
		t.Fatalf("unexpected token fields: token=%s bearer=%s totp=%s", cfg.Token, cfg.BearerToken, cfg.TotpCode)
	}
	wantParams := map[string]string{
		"region":  "cn-north-1",
		"charset": "UTF-8",
	}
	if !reflect.DeepEqual(wantParams, cfg.Params) {
		t.Fatalf("want params %v, got %v", wantParams, cfg.Params)
	}
	wantEndpoints := []string{
		"ws://[2001:db8::1]:6041?token=tk1",
		"ws://127.0.0.1:6042?token=tk1",
	}
	if !reflect.DeepEqual(wantEndpoints, cfg.Endpoints) {
		t.Fatalf("want endpoints %v, got %v", wantEndpoints, cfg.Endpoints)
	}
}

// TestNewConfigFromDSNMultiAddrListIPv6Failover verifies normalized endpoints for IPv6 multi-node DSN.
func TestNewConfigFromDSNMultiAddrListIPv6Failover(t *testing.T) {
	cfg, err := NewConfigFromDSN("user:passwd@wss([2001:db8::1]:6041,[2001:db8::2]:6042)/db?token=tk1", "/ws/v1")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"wss://[2001:db8::1]:6041/ws/v1?token=tk1",
		"wss://[2001:db8::2]:6042/ws/v1?token=tk1",
	}
	if !reflect.DeepEqual(want, cfg.Endpoints) {
		t.Fatalf("want %v, got %v", want, cfg.Endpoints)
	}
}

// TestTryUnescape verifies the expected behavior for this scenario.
func TestTryUnescape(t *testing.T) {
	if got := tryUnescape("%3F"); got != "?" {
		t.Fatalf("unexpected unescape result: %s", got)
	}
	if got := tryUnescape("%"); got != "%" {
		t.Fatalf("unexpected unescape result: %s", got)
	}
}
