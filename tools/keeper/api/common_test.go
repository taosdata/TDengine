package api

import (
	"strings"
	"testing"
)

func Test_generateCreateDBSql(t *testing.T) {
	opts := map[string]interface{}{
		"precision": "ms",
		"replica":   2,
		"strict":    true,
	}
	sql := generateCreateDBSql("mydb", opts)

	if !strings.HasPrefix(sql, "create database if not exists mydb") {
		t.Fatalf("prefix mismatch: %q", sql)
	}

	cases := []string{
		" precision 'ms' ",
		" replica 2 ",
		" strict true ",
	}
	for _, sub := range cases {
		if !strings.Contains(sql, sub) {
			t.Fatalf("expected to contain %q, got %q", sub, sql)
		}
	}

	if !strings.HasSuffix(sql, " ") {
		t.Fatalf("expected trailing space, got %q", sql)
	}
}
