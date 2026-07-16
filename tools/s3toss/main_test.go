package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseAccessStringAcceptsKnownAndUnknownOptions(t *testing.T) {
	resetConfig()

	if err := parseAccessString("endpoint=s3.amazonaws.com;bucket=mybucket;uriStyle=path;protocol=http;accessKeyId=AK;secretAccessKey=SK;region=us-east-2"); err != nil {
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
	config = struct {
		BlockSize int64
		DNode     uint
		DataDirs  [3][]string
		Endpoint  string
		Secure    bool
		AccessKey string
		SecretKey string
		Bucket    string
		Region    string
	}{}
}
