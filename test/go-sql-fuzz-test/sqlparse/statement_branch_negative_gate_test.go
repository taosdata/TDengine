package sqlparser

import "testing"

func TestStatementBranchGate_NegativeCoverage(t *testing.T) {
	cases := loadInvalidSQLCases(t)
	if len(cases) == 0 {
		t.Fatalf("no invalid sql cases loaded")
	}

	errTypeHit := map[string]int{}
	for _, tc := range cases {
		_, err := Parse(tc.sql)
		if err == nil {
			t.Fatalf("[%s] expected parse failure, sql=%q", tc.id, tc.sql)
		}
		gotType := classifyParseErr(err)
		errTypeHit[gotType]++
		if gotType != tc.errType {
			t.Fatalf("[%s] invalid err type: got=%q want=%q err=%v sql=%q", tc.id, gotType, tc.errType, err, tc.sql)
		}
	}

	// Gate on essential parser error branches.
	requiredErrTypes := []string{"syntax", "incomplete"}
	for _, et := range requiredErrTypes {
		if errTypeHit[et] == 0 {
			t.Fatalf("statement negative branch gate missing error type branch: %s", et)
		}
	}
}
