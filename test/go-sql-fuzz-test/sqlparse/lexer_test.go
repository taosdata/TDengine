package sqlparser

import "testing"

type scannedToken struct {
	typ int
	val string
}

func scanAll(sql string) []scannedToken {
	s := NewScanner(sql)
	out := make([]scannedToken, 0, 8)
	for {
		typ, val := s.Scan()
		if typ == 0 {
			break
		}
		out = append(out, scannedToken{typ: typ, val: string(val)})
	}
	return out
}

func TestScanner_KeywordsAndIdentifiers(t *testing.T) {
	got := scanAll("create CREATE test_db true FALSE")
	want := []scannedToken{
		{typ: CREATE, val: "create"},
		{typ: CREATE, val: "CREATE"},
		{typ: NK_ID, val: "test_db"},
		{typ: NK_BOOL, val: "true"},
		{typ: NK_BOOL, val: "false"},
	}
	if len(got) != len(want) {
		t.Fatalf("token count mismatch: got=%d want=%d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("token[%d] mismatch: got=%+v want=%+v", i, got[i], want[i])
		}
	}
}

func TestScanner_SymbolsAndOperators(t *testing.T) {
	got := scanAll("a:b -> 'x'")
	want := []scannedToken{
		{typ: NK_ID, val: "a"},
		{typ: NK_COLON, val: ""},
		{typ: NK_ID, val: "b"},
		{typ: NK_ARROW, val: ""},
		{typ: NK_STRING, val: "x"},
	}
	if len(got) != len(want) {
		t.Fatalf("token count mismatch: got=%d want=%d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("token[%d] mismatch: got=%+v want=%+v", i, got[i], want[i])
		}
	}
}

func TestScanner_HintComment(t *testing.T) {
	got := scanAll("/*+ BATCH_SCAN() */")
	want := []scannedToken{
		{typ: NK_HINT, val: "batch_scan()"},
	}
	if len(got) != len(want) {
		t.Fatalf("token count mismatch: got=%d want=%d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("token[%d] mismatch: got=%+v want=%+v", i, got[i], want[i])
		}
	}
}

func TestScanner_DoubleQuotedAlias(t *testing.T) {
	got := scanAll(`select a "x" from t`)
	want := []scannedToken{
		{typ: SELECT, val: "select"},
		{typ: NK_ID, val: "a"},
		{typ: NK_ALIAS, val: "x"},
		{typ: FROM, val: "from"},
		{typ: NK_ID, val: "t"},
	}
	if len(got) != len(want) {
		t.Fatalf("token count mismatch: got=%d want=%d (%+v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("token[%d] mismatch: got=%+v want=%+v", i, got[i], want[i])
		}
	}
}
