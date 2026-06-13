package workload

import "testing"

func TestDefaultConfigNoTxn(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.TxnBegin != 0 || cfg.TxnCommit != 0 || cfg.TxnRollback != 0 {
		t.Fatalf("transaction weights must be disabled by default for TDengine: %+v", cfg)
	}
	if cfg.DMLSelect <= cfg.DMLInsert || cfg.DMLSelect <= cfg.DMLUpdate {
		t.Fatalf("query-first defaults expected: %+v", cfg)
	}
}
