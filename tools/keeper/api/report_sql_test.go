package api

import (
    "testing"
)

func TestQualifyCreateTableSQL(t *testing.T) {
    in := "create table if not exists keeper_monitor (ts timestamp) tags (identify nchar(50))"
    out := qualifyCreateTableSQL(in, "log")
    wantPrefix := "create table if not exists `log`.keeper_monitor"
    if out[:len(wantPrefix)] != wantPrefix {
        t.Fatalf("expected prefix %q, got %q", wantPrefix, out)
    }
}

func TestQualifyInsertSQL(t *testing.T) {
    in := "insert into d_info_1 using d_info tags (1, 'ep', 'cid') values ('ts','ready')"
    out := qualifyInsertSQL(in, "log")
    if out != "insert into `log`.d_info_1 using `log`.d_info tags (1, 'ep', 'cid') values ('ts','ready')" {
        t.Fatalf("unexpected qualified sql: %s", out)
    }
}


