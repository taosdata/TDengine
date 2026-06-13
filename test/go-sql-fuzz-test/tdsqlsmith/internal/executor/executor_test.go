package executor

import "testing"

func TestParseUseDatabase(t *testing.T) {
	tests := []struct {
		sql  string
		want string
	}{
		{sql: "use testdb", want: "testdb"},
		{sql: "USE testdb;", want: "testdb"},
		{sql: " use `testdb` ", want: "testdb"},
		{sql: `use "testdb"`, want: "testdb"},
		{sql: "select 1", want: ""},
		{sql: "use", want: ""},
	}
	for _, tc := range tests {
		if got := parseUseDatabase(tc.sql); got != tc.want {
			t.Fatalf("parseUseDatabase(%q)=%q want %q", tc.sql, got, tc.want)
		}
	}
}

func TestShouldRetryWithDatabase(t *testing.T) {
	errDb := mockErr("[0x2616] Database not specified")
	if !shouldRetryWithDatabase(errDb, "db1", "select * from t1") {
		t.Fatalf("expected retry=true for database-not-specified")
	}
	if shouldRetryWithDatabase(errDb, "", "select * from t1") {
		t.Fatalf("expected retry=false when current db missing")
	}
	if shouldRetryWithDatabase(mockErr("other error"), "db1", "select * from t1") {
		t.Fatalf("expected retry=false for non database error")
	}
	if shouldRetryWithDatabase(errDb, "db1", "use db2") {
		t.Fatalf("expected retry=false for use statement")
	}
}

type mockErr string

func (m mockErr) Error() string { return string(m) }
