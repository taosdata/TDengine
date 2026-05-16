package taosWS

import (
	"testing"
	"time"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/ws/unified"
)

func TestBuildConnectionConfigDefaults(t *testing.T) {
	in := &Config{}
	out := unified.BuildConnectionConfig(in, unified.TaosWSConnectionDefaults)
	if out == nil {
		t.Fatal("expected non-nil config")
		return
	}
	if out.User != common.DefaultUser {
		t.Fatalf("unexpected default user: %q", out.User)
	}
	if out.Passwd != common.DefaultPassword {
		t.Fatalf("unexpected default password: %q", out.Passwd)
	}
	if out.Net != "ws" {
		t.Fatalf("unexpected default net: %q", out.Net)
	}
	if out.Addr != "127.0.0.1" {
		t.Fatalf("unexpected default addr: %q", out.Addr)
	}
	if out.Port != common.DefaultHttpPort {
		t.Fatalf("unexpected default port: %d", out.Port)
	}
	if out.ChanLength != 1 {
		t.Fatalf("unexpected default channel length: %d", out.ChanLength)
	}
	if out.ReadTimeout != common.DefaultMessageTimeout {
		t.Fatalf("unexpected default read timeout: %v", out.ReadTimeout)
	}
	if out.WriteTimeout != common.DefaultWriteWait {
		t.Fatalf("unexpected default write timeout: %v", out.WriteTimeout)
	}
	if out.ReconnectIntervalMs != 2000 {
		t.Fatalf("unexpected default reconnect interval: %d", out.ReconnectIntervalMs)
	}
	if out.ReconnectRetryCount != 3 {
		t.Fatalf("unexpected default reconnect retry count: %d", out.ReconnectRetryCount)
	}
	// Ensure input is not mutated.
	if in.User != "" || in.Passwd != "" || in.Port != 0 {
		t.Fatalf("input config mutated: %+v", in)
	}
}

func TestBuildConnectionConfigPreservesUserValues(t *testing.T) {
	in := &Config{
		Endpoints:           []string{"ws://127.0.0.1:6041/ws"},
		ChanLength:          8,
		ReconnectIntervalMs: 1234,
		ReconnectRetryCount: 9,
		Net:                 "wss",
		Addr:                "cloud.example.com",
		Port:                443,
		ReadTimeout:         3 * time.Second,
		WriteTimeout:        2 * time.Second,
		User:                "user",
		Passwd:              "passwd",
		InterpolateParams:   false,
	}
	out := unified.BuildConnectionConfig(in, unified.TaosWSConnectionDefaults)
	if out == nil {
		t.Fatal("expected non-nil config")
		return
	}
	if out.ChanLength != 8 || out.ReconnectIntervalMs != 1234 || out.ReconnectRetryCount != 9 {
		t.Fatalf("unexpected runtime defaults overwrite: %+v", out)
	}
	if out.Net != "wss" || out.Addr != "cloud.example.com" || out.Port != 443 {
		t.Fatalf("unexpected endpoint defaults overwrite: %+v", out)
	}
	if out.ReadTimeout != 3*time.Second || out.WriteTimeout != 2*time.Second {
		t.Fatalf("unexpected timeout defaults overwrite: read=%v write=%v", out.ReadTimeout, out.WriteTimeout)
	}
	if out.User != "user" || out.Passwd != "passwd" || out.InterpolateParams {
		t.Fatalf("unexpected auth/interpolate overwrite: %+v", out)
	}
	in.Endpoints[0] = "ws://mutated"
	if len(out.Endpoints) != 1 || out.Endpoints[0] != "ws://127.0.0.1:6041/ws" {
		t.Fatalf("expected endpoints copied, got %+v", out.Endpoints)
	}
}

func TestBuildConnectionConfigWriteTimeoutLegacyDefault(t *testing.T) {
	in := &Config{
		ReadTimeout: 30 * time.Second,
	}
	out := unified.BuildConnectionConfig(in, unified.TaosWSConnectionDefaults)
	if out == nil {
		t.Fatal("expected non-nil config")
		return
	}
	if out.WriteTimeout != common.DefaultWriteWait {
		t.Fatalf("expected legacy write timeout default %v, got %v", common.DefaultWriteWait, out.WriteTimeout)
	}
}
