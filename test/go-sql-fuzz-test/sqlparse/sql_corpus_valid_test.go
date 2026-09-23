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

import (
	"fmt"
	"strings"
	"testing"
)

func normalizeStmtTypeName(t string) string {
	t = strings.TrimSpace(t)
	t = strings.TrimPrefix(t, "*")
	t = strings.TrimPrefix(t, "sqlparser.")
	return t
}

func TestSQLCorpusValid(t *testing.T) {
	cases := loadValidSQLCases(t)
	if len(cases) < 300 {
		t.Fatalf("expected at least 300 valid sql cases, got %d", len(cases))
	}

	typeHit := map[string]int{}
	for _, tc := range cases {
		stmt, err := Parse(tc.sql)
		if err != nil {
			t.Fatalf("[%s] parse valid sql failed: %v, sql=%q", tc.id, err, tc.sql)
		}

		gotType := normalizeStmtTypeName(fmt.Sprintf("%T", stmt))
		wantType := normalizeStmtTypeName(tc.stmtType)
		if wantType != "" && gotType != wantType {
			t.Fatalf("[%s] expected %s, got %T", tc.id, wantType, stmt)
		}
		typeHit[fmt.Sprintf("%T", stmt)]++

		assertKeyFields(t, stmt, tc.keyAssert)
	}

	var missing []string
	for _, typ := range expectedStatementTypes() {
		if typeHit[typ] == 0 {
			missing = append(missing, typ)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("valid sql corpus missing statement types: %v", missing)
	}
}
