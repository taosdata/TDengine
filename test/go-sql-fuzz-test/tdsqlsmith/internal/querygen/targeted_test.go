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

package querygen

import (
	"strings"
	"testing"

	"tdsqlsmith/internal/parsergate"
	"tdsqlsmith/internal/random"
)

func TestNextForRulesJoinTemplate(t *testing.T) {
	g := New(DefaultConfig())
	r := random.New(11)
	out, ok, err := g.NextForRules(r, []string{"joined_table", "table_reference"})
	if err != nil {
		t.Fatalf("next for rules failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected targeted query")
	}
	sql := strings.ToLower(out.SQL)
	if !strings.Contains(sql, " join ") {
		t.Fatalf("expected join template, got: %s", out.SQL)
	}
	if res := parsergate.Parse(out.SQL); res.Err != nil {
		t.Fatalf("targeted join query parse failed: %v, sql=%s", res.Err, out.SQL)
	}
}

func TestNextForRulesWindowTemplate(t *testing.T) {
	g := New(DefaultConfig())
	r := random.New(12)
	out, ok, err := g.NextForRules(r, []string{"twindow_clause_opt", "fill_opt"})
	if err != nil {
		t.Fatalf("next for rules failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected targeted query")
	}
	sql := strings.ToLower(out.SQL)
	if !strings.Contains(sql, " interval(") || !strings.Contains(sql, " fill(") {
		t.Fatalf("expected window+fill template, got: %s", out.SQL)
	}
	if res := parsergate.Parse(out.SQL); res.Err != nil {
		t.Fatalf("targeted window query parse failed: %v, sql=%s", res.Err, out.SQL)
	}
}

func TestNextForRulesInsertTemplate(t *testing.T) {
	g := New(DefaultConfig())
	r := random.New(13)
	out, ok, err := g.NextForRules(r, []string{"insert_query"})
	if err != nil {
		t.Fatalf("next for rules failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected targeted query")
	}
	sql := strings.ToLower(out.SQL)
	if !strings.HasPrefix(strings.TrimSpace(sql), "insert into") {
		t.Fatalf("expected insert-query template, got: %s", out.SQL)
	}
	if res := parsergate.Parse(out.SQL); res.Err != nil {
		t.Fatalf("targeted insert query parse failed: %v, sql=%s", res.Err, out.SQL)
	}
}

func TestNextForRulesParenthesizedJoinTemplate(t *testing.T) {
	g := New(DefaultConfig())
	r := random.New(21)
	out, ok, err := g.NextForRules(r, []string{"parenthesized_joined_table"})
	if err != nil {
		t.Fatalf("next for rules failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected targeted query")
	}
	sql := strings.ToLower(out.SQL)
	if !strings.Contains(sql, "from (") || !strings.Contains(sql, " join ") {
		t.Fatalf("expected parenthesized join template, got: %s", out.SQL)
	}
	if res := parsergate.Parse(out.SQL); res.Err != nil {
		t.Fatalf("targeted parenthesized join parse failed: %v, sql=%s", res.Err, out.SQL)
	}
}

func TestNextForRulesCountWindowTemplate(t *testing.T) {
	g := New(DefaultConfig())
	r := random.New(22)
	out, ok, err := g.NextForRules(r, []string{"count_window_args", "column_name_list", "trigger_col_name"})
	if err != nil {
		t.Fatalf("next for rules failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected targeted query")
	}
	sql := strings.ToLower(out.SQL)
	if !strings.Contains(sql, "count_window(") {
		t.Fatalf("expected count_window template, got: %s", out.SQL)
	}
	if res := parsergate.Parse(out.SQL); res.Err != nil {
		t.Fatalf("targeted count_window parse failed: %v, sql=%s", res.Err, out.SQL)
	}
}

func TestNextForRulesInterpFillTemplate(t *testing.T) {
	g := New(DefaultConfig())
	r := random.New(23)
	out, ok, err := g.NextForRules(r, []string{"fill_position_mode_extension", "interp_fill_mode"})
	if err != nil {
		t.Fatalf("next for rules failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected targeted query")
	}
	sql := strings.ToLower(out.SQL)
	if !strings.Contains(sql, " fill(") {
		t.Fatalf("expected interp fill template, got: %s", out.SQL)
	}
	if res := parsergate.Parse(out.SQL); res.Err != nil {
		t.Fatalf("targeted interp fill parse failed: %v, sql=%s", res.Err, out.SQL)
	}
}
