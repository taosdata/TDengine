package workload

import (
	"os"
	"path/filepath"
	"testing"

	"tdsqlsmith/internal/random"
)

func TestDefaultSampler(t *testing.T) {
	cfg := DefaultConfig()
	s, err := NewSampler(cfg)
	if err != nil {
		t.Fatalf("new sampler failed: %v", err)
	}
	if s.Total() <= 0 {
		t.Fatalf("unexpected total: %d", s.Total())
	}
	r := random.New(1)
	for i := 0; i < 20; i++ {
		p := s.Pick(r)
		if p == "" {
			t.Fatalf("empty pick")
		}
	}
}

func TestLoadConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.toml")
	content := `txn-begin = 1
dml-select = 2
dml-insert = 3`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write file failed: %v", err)
	}
	cfg := DefaultConfig()
	if err := cfg.Load(path); err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if cfg.TxnBegin != 1 || cfg.DMLSelect != 2 || cfg.DMLInsert != 3 {
		t.Fatalf("unexpected cfg: %+v", cfg)
	}
}
