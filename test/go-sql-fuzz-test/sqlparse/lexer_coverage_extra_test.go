package sqlparser

import "testing"

func scanOne(sql string) (int, string) {
	s := NewScanner(sql)
	typ, val := s.Scan()
	return typ, string(val)
}

func TestScanner_OperatorsAndNumbersExtra(t *testing.T) {
	sql := "1 1s 0x1f 0b10 1.2 .3 1e2 <= >= <> << >> != && || ~ ? : / - ->"
	got := scanAll(sql)
	if len(got) == 0 {
		t.Fatalf("expected tokens")
	}
}

func TestScanner_CommentsAndHintExtra(t *testing.T) {
	typ, _ := scanOne("/* hello */")
	if typ != COMMENT {
		t.Fatalf("expected COMMENT, got %d", typ)
	}
	typ2, v2 := scanOne("/*+ HASH_JOIN() */")
	if typ2 != NK_HINT || v2 == "" {
		t.Fatalf("expected NK_HINT with value, got %d %q", typ2, v2)
	}
}

func TestScanner_StringAndLiteralIdentifierExtra(t *testing.T) {
	typ, v := scanOne(`'a\nb'`)
	if typ != NK_STRING || v == "" {
		t.Fatalf("expected NK_STRING, got %d %q", typ, v)
	}
	typ2, v2 := scanOne("`a``b`")
	if typ2 != NK_ID || v2 == "" {
		t.Fatalf("expected NK_ID, got %d %q", typ2, v2)
	}
}

func TestScanner_ErrorPathsExtra(t *testing.T) {
	if typ, _ := scanOne("'unterminated"); typ != LEX_ERROR {
		t.Fatalf("expected LEX_ERROR for unterminated string, got %d", typ)
	}
	if typ, _ := scanOne("/* unterminated"); typ != LEX_ERROR {
		t.Fatalf("expected LEX_ERROR for unterminated comment, got %d", typ)
	}
	if typ, _ := scanOne("`"); typ != LEX_ERROR {
		t.Fatalf("expected LEX_ERROR for empty literal identifier, got %d", typ)
	}
	if typ, _ := scanOne("@"); typ != LEX_ERROR {
		t.Fatalf("expected LEX_ERROR for unknown char, got %d", typ)
	}
}

func TestScanner_OperatorSinglesAndErrorFormatting(t *testing.T) {
	got := scanAll("% ^ & | ! <")
	wantTypes := []int{NK_REM, int('^'), NK_BITAND, NK_BITOR, int('!'), NK_LT}
	if len(got) != len(wantTypes) {
		t.Fatalf("unexpected token count: got=%d tokens=%+v", len(got), got)
	}
	for i := range wantTypes {
		if got[i].typ != wantTypes[i] {
			t.Fatalf("token[%d] mismatch: got=%d want=%d", i, got[i].typ, wantTypes[i])
		}
	}

	s := NewScanner("select")
	s.Error("custom error")
	if s.lastErr == nil || s.lastErr.Error() == "" {
		t.Fatalf("expected formatted error")
	}
}

func TestScanner_NumberAndStringEdgeCases(t *testing.T) {
	if typ, _ := scanOne("0x1g"); typ != LEX_ERROR {
		t.Fatalf("expected hex+letter error, got %d", typ)
	}
	if typ, _ := scanOne("0b1x"); typ != LEX_ERROR {
		t.Fatalf("expected binary+letter error, got %d", typ)
	}
	if typ, _ := scanOne("1q"); typ != LEX_ERROR {
		t.Fatalf("expected integer+letter error, got %d", typ)
	}

	typ, val := scanOne(`'\q'`)
	if typ != NK_STRING || val != "q" {
		t.Fatalf("expected unknown escape passthrough, got %d %q", typ, val)
	}
	if typ, _ := scanOne("'abc\\"); typ != LEX_ERROR {
		t.Fatalf("expected escape-eof lex error, got %d", typ)
	}
}

func TestScanner_CommentAndHintAsteriskPath(t *testing.T) {
	typ, _ := scanOne("/* a* b */")
	if typ != COMMENT {
		t.Fatalf("expected COMMENT, got %d", typ)
	}
	typ, val := scanOne("/*+ a* b */")
	if typ != NK_HINT || val == "" {
		t.Fatalf("expected hint value, got %d %q", typ, val)
	}
}

func TestScanner_UncoveredBranches(t *testing.T) {
	if typ, _ := scanOne("1e+2"); typ != NK_FLOAT {
		t.Fatalf("expected float with exponent sign, got %d", typ)
	}
	if typ, _ := scanOne("/*+ unterminated"); typ != LEX_ERROR {
		t.Fatalf("expected LEX_ERROR for unterminated hint, got %d", typ)
	}
	if typ, _ := scanOne("``x"); typ != LEX_ERROR {
		t.Fatalf("expected LEX_ERROR for empty literal identifier content, got %d", typ)
	}

	s := &Scanner{lastChar: eofChar}
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on consumeNext at EOF")
		}
	}()
	s.consumeNext(nil)
}
