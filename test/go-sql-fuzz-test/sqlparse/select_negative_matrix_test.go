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
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type selectNegativeCase struct {
	id      string
	rule    string
	sql     string
	errType string
}

func loadSelectNegativeCases(t *testing.T) []selectNegativeCase {
	t.Helper()
	path := filepath.Join("testdata", "sql_corpus", "select_branch_negative.tsv")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open select negative matrix failed: %v", err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	line := 0
	var out []selectNegativeCase
	for sc.Scan() {
		line++
		s := sc.Text()
		if line == 1 {
			continue
		}
		cols := strings.Split(s, "\t")
		if len(cols) != 4 {
			t.Fatalf("invalid select negative matrix line %d: %q", line, s)
		}
		out = append(out, selectNegativeCase{
			id:      cols[0],
			rule:    cols[1],
			sql:     cols[2],
			errType: cols[3],
		})
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan select negative matrix failed: %v", err)
	}
	return out
}

func TestSelectBranchNegativeMatrix(t *testing.T) {
	cases := loadSelectNegativeCases(t)
	if len(cases) == 0 {
		t.Fatalf("empty select negative matrix")
	}
	for _, tc := range cases {
		_, err := Parse(tc.sql)
		if err == nil {
			t.Fatalf("[%s] expected parse failure for sql=%q", tc.id, tc.sql)
		}
		if got := classifyParseErr(err); got != tc.errType {
			t.Fatalf("[%s] err type mismatch: got=%q want=%q err=%v", tc.id, got, tc.errType, err)
		}
	}
}
