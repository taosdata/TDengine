package sqlparser

import "testing"

func TestSQLCorpusInvalid(t *testing.T) {
	cases := loadInvalidSQLCases(t)
	if len(cases) != 300 {
		t.Fatalf("expected 300 invalid sql cases, got %d", len(cases))
	}

	for _, tc := range cases {
		_, err := Parse(tc.sql)
		if err == nil {
			t.Fatalf("[%s] expected parse failure, sql=%q", tc.id, tc.sql)
		}
		gotType := classifyParseErr(err)
		if gotType != tc.errType {
			t.Fatalf("[%s] invalid err type: got=%q want=%q err=%v sql=%q", tc.id, gotType, tc.errType, err, tc.sql)
		}
	}
}
