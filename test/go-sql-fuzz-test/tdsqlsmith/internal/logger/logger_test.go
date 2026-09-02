package logger

import "testing"

func TestSymbolForClass(t *testing.T) {
	cases := map[string]string{
		"syntax":       "S",
		"parse_reject": "S",
		"timeout":      "t",
		"conn_lost":    "C",
		"db_error":     "e",
	}
	for in, want := range cases {
		if got := symbolForClass(in); got != want {
			t.Fatalf("symbol mismatch for %q: got=%q want=%q", in, got, want)
		}
	}
}
