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
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

func extractSQLLiterals(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s failed: %v", path, err)
	}
	re := regexp.MustCompile(`sql:\s*"([^"]+)"`)
	matches := re.FindAllStringSubmatch(string(b), -1)
	out := make([]string, 0, len(matches))
	for _, m := range matches {
		if len(m) == 2 {
			out = append(out, m[1])
		}
	}
	return out
}

func TestStatementInterfaceRuntimeContracts_FromCommandCases(t *testing.T) {
	files := []string{
		"command_entry_statement_test.go",
		"command_matrix_test.go",
	}

	seen := map[string]struct{}{}
	var allSQL []string
	for _, f := range files {
		sqls := extractSQLLiterals(t, filepath.Clean(f))
		for _, s := range sqls {
			if _, ok := seen[s]; ok {
				continue
			}
			seen[s] = struct{}{}
			allSQL = append(allSQL, s)
		}
	}
	if len(allSQL) == 0 {
		t.Fatalf("no sql literals extracted from command tests")
	}

	for _, sql := range allSQL {
		stmt, err := Parse(sql)
		if err != nil {
			t.Fatalf("parse failed for sql=%q: %v", sql, err)
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Format panic for sql=%q stmt=%T panic=%v", sql, stmt, r)
				}
			}()
			tb := newTB()
			stmt.Format(tb)
		}()
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Walk panic for sql=%q stmt=%T panic=%v", sql, stmt, r)
				}
			}()
			if err := Walk(func(node SQLNode) (bool, error) { return true, nil }, stmt); err != nil {
				t.Fatalf("Walk failed for sql=%q stmt=%T: %v", sql, stmt, err)
			}
		}()
	}
}

func TestStatementInterfaceRuntimeContracts_FromValidCorpus(t *testing.T) {
	cases := loadValidSQLCases(t)
	if len(cases) == 0 {
		t.Fatalf("no valid sql corpus cases")
	}
	for _, tc := range cases {
		stmt, err := Parse(tc.sql)
		if err != nil {
			t.Fatalf("[%s] parse failed: %v sql=%q", tc.id, err, tc.sql)
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("[%s] Format panic sql=%q stmt=%T panic=%v", tc.id, tc.sql, stmt, r)
				}
			}()
			tb := newTB()
			stmt.Format(tb)
		}()
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("[%s] Walk panic sql=%q stmt=%T panic=%v", tc.id, tc.sql, stmt, r)
				}
			}()
			if err := Walk(func(node SQLNode) (bool, error) { return true, nil }, stmt); err != nil {
				t.Fatalf("[%s] Walk failed sql=%q stmt=%T err=%v", tc.id, tc.sql, stmt, err)
			}
		}()
	}
}
