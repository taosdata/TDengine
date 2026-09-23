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
	"strings"
	"testing"
)

func TestParseRejectsDoubleDashOutsideLiteral(t *testing.T) {
	_, err := Parse("select --v from t1;")
	if err == nil {
		t.Fatalf("expected semantic rejection for double dash")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "semantic") {
		t.Fatalf("expected semantic error, got: %v", err)
	}
}

func TestParseAllowsDoubleDashInsideStringLiteral(t *testing.T) {
	if _, err := Parse("select 'a--b' from t1;"); err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
}

func TestParseRejectsInvalidColsFunction(t *testing.T) {
	cases := []string{
		"select cols(abs(v), v) from t1;",
		"select cols(count(v), v as x) from t1;",
	}
	for _, sql := range cases {
		_, err := Parse(sql)
		if err == nil {
			t.Fatalf("expected semantic rejection for sql: %s", sql)
		}
		if !strings.Contains(strings.ToLower(err.Error()), "semantic") {
			t.Fatalf("expected semantic error, got: %v", err)
		}
	}
}

func TestParseRejectsInvalidFunctionArity(t *testing.T) {
	cases := []string{
		"select rand(1, 2) from t1;",
		"select lower(a, b) from t1;",
	}
	for _, sql := range cases {
		_, err := Parse(sql)
		if err == nil {
			t.Fatalf("expected semantic rejection for sql: %s", sql)
		}
		if !strings.Contains(strings.ToLower(err.Error()), "semantic") {
			t.Fatalf("expected semantic error, got: %v", err)
		}
	}
}

func TestParseRejectsGroupByMismatch(t *testing.T) {
	_, err := Parse("select b, c2 from t1 group by c2;")
	if err == nil {
		t.Fatalf("expected semantic rejection for group by mismatch")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "semantic") {
		t.Fatalf("expected semantic error, got: %v", err)
	}
}

func TestParseAllowsValidGroupedSelect(t *testing.T) {
	if _, err := Parse("select c2, count(*) from t1 group by c2;"); err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
}
