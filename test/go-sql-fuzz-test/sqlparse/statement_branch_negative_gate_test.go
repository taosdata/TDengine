// Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This program is free software: you can use, redistribute, and/or modify
// it under the terms of the GNU Affero General Public License, version 3
// or later ("AGPL"), as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

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
