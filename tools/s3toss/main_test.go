package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseAccessStringAcceptsKnownAndUnknownOptions(t *testing.T) {
	resetConfig()

	if err := parseAccessString("endpoint=s3.amazonaws.com;bucket=mybucket;uriStyle=path;protocol=http;accessKeyId=AK;secretAccessKey=SK;region=us-east-2;"); err != nil {
		t.Fatalf("parseAccessString returned error: %v", err)
	}

	if config.Endpoint != "s3.amazonaws.com" {
		t.Fatalf("Endpoint = %q, want s3.amazonaws.com", config.Endpoint)
	}
	if config.Bucket != "mybucket" {
		t.Fatalf("Bucket = %q, want mybucket", config.Bucket)
	}
	if config.Secure {
		t.Fatal("Secure = true, want false for protocol=http")
	}
	if config.AccessKey != "AK" || config.SecretKey != "SK" || config.Region != "us-east-2" {
		t.Fatalf("unexpected credentials or region: access=%q secret=%q region=%q", config.AccessKey, config.SecretKey, config.Region)
	}
}

func TestParseAccessStringRejectsMalformedOption(t *testing.T) {
	resetConfig()

	err := parseAccessString("endpoint")
	if err == nil {
		t.Fatal("parseAccessString succeeded for malformed option")
	}
	if !strings.Contains(err.Error(), "expected key=value") {
		t.Fatalf("error = %q, want expected key=value", err)
	}
}

func TestParseAccessStringRejectsInvalidProtocol(t *testing.T) {
	resetConfig()

	err := parseAccessString("endpoint=s3.amazonaws.com;protocol=ftp")
	if err == nil {
		t.Fatal("parseAccessString succeeded for invalid protocol")
	}
	if !strings.Contains(err.Error(), "expected http or https") {
		t.Fatalf("error = %q, want protocol error", err)
	}
}

func TestParseTaosCfgParsesSsAccessString(t *testing.T) {
	resetConfig()

	cfgPath := writeConfig(t, "dataDir /tmp/taos 0\nssAccessString s3:endpoint=s3.amazonaws.com;bucket=mybucket;uriStyle=path;protocol=http;accessKeyId=AK;secretAccessKey=SK;region=us-east-2\n")

	if err := parseTaosCfg(cfgPath); err != nil {
		t.Fatalf("parseTaosCfg returned error: %v", err)
	}

	if config.Endpoint != "s3.amazonaws.com" {
		t.Fatalf("Endpoint = %q, want s3.amazonaws.com", config.Endpoint)
	}
	if config.Bucket != "mybucket" || config.AccessKey != "AK" || config.SecretKey != "SK" || config.Region != "us-east-2" {
		t.Fatalf("unexpected S3 config: bucket=%q access=%q secret=%q region=%q", config.Bucket, config.AccessKey, config.SecretKey, config.Region)
	}
	if config.Secure {
		t.Fatal("Secure = true, want false for protocol=http")
	}
}

func TestParseTaosCfgRejectsMalformedSsAccessString(t *testing.T) {
	resetConfig()

	cfgPath := writeConfig(t, "dataDir /tmp/taos 0\nssAccessString s3:endpoint\n")

	err := parseTaosCfg(cfgPath)
	if err == nil {
		t.Fatal("parseTaosCfg succeeded for malformed ssAccessString")
	}
	if !strings.Contains(err.Error(), "expected key=value") {
		t.Fatalf("error = %q, want expected key=value", err)
	}
}

func TestParseTaosCfgRejectsInvalidDataDirLevel(t *testing.T) {
	resetConfig()

	cfgPath := writeConfig(t, "dataDir /tmp/taos 3\n")

	err := parseTaosCfg(cfgPath)
	if err == nil {
		t.Fatal("parseTaosCfg succeeded for invalid dataDir level")
	}
	if !strings.Contains(err.Error(), "invalid dataDir level 3") {
		t.Fatalf("error = %q, want invalid dataDir level 3", err)
	}
}

func TestParseTaosCfgParsesLegacyS3Endpoint(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		secure   bool
	}{
		{name: "http", endpoint: "http://localhost:9000", secure: false},
		{name: "https", endpoint: "https://localhost:9000", secure: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetConfig()

			cfgPath := writeConfig(t, "dataDir /tmp/taos 0\ns3endpoint "+tt.endpoint+"\n")

			if err := parseTaosCfg(cfgPath); err != nil {
				t.Fatalf("parseTaosCfg returned error: %v", err)
			}
			if config.Endpoint != "localhost:9000" {
				t.Fatalf("Endpoint = %q, want localhost:9000", config.Endpoint)
			}
			if config.Secure != tt.secure {
				t.Fatalf("Secure = %v, want %v", config.Secure, tt.secure)
			}
		})
	}
}

func TestParseTaosCfgRejectsInvalidS3Endpoint(t *testing.T) {
	resetConfig()

	cfgPath := writeConfig(t, "dataDir /tmp/taos 0\ns3endpoint localhost:9000\n")

	err := parseTaosCfg(cfgPath)
	if err == nil {
		t.Fatal("parseTaosCfg succeeded for invalid s3endpoint")
	}
	if !strings.Contains(err.Error(), "expected http:// or https:// prefix") {
		t.Fatalf("error = %q, want endpoint prefix error", err)
	}
}

func writeConfig(t *testing.T, contents string) string {
	t.Helper()

	cfgPath := filepath.Join(t.TempDir(), "taos.cfg")
	if err := os.WriteFile(cfgPath, []byte(contents), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	return cfgPath
}

func resetConfig() {
	config.BlockSize = 0
	config.DNode = 0
	for i := range config.DataDirs {
		config.DataDirs[i] = nil
	}
	config.Endpoint = ""
	config.Secure = false
	config.AccessKey = ""
	config.SecretKey = ""
	config.Bucket = ""
	config.Region = ""
}
