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

func TestJsonExpr_ColumnReference(t *testing.T) {
	sel := parseSelect(t, `select v->'x' from t1;`)
	if len(sel.Select) != 1 {
		t.Fatalf("expected 1 select expr, got %d", len(sel.Select))
	}
	j, ok := sel.Select[0].(*RawExpr)
	if !ok || j.Kind != "json" {
		t.Fatalf("expected json RawExpr, got %#v", sel.Select[0])
	}
	left, ok := j.Left.(*RawExpr)
	if !ok || left.Kind != "col" || left.Name != "v" {
		t.Fatalf("expected left column v, got %#v", j.Left)
	}
	path, ok := j.Extra.(Token)
	if !ok || string(path.Bytes) != "x" {
		t.Fatalf("expected json path x, got %#v", j.Extra)
	}
}

func TestJsonExpr_TableQualifiedColumnReference(t *testing.T) {
	sel := parseSelect(t, `select t1.v->'x' from t1;`)
	if len(sel.Select) != 1 {
		t.Fatalf("expected 1 select expr, got %d", len(sel.Select))
	}
	j, ok := sel.Select[0].(*RawExpr)
	if !ok || j.Kind != "json" {
		t.Fatalf("expected json RawExpr, got %#v", sel.Select[0])
	}
	left, ok := j.Left.(*RawExpr)
	if !ok || left.Kind != "col" || left.Name != "v" {
		t.Fatalf("expected left column v, got %#v", j.Left)
	}
	if tbl, _ := left.Extra.(string); tbl != "t1" {
		t.Fatalf("expected table qualifier t1, got %q", tbl)
	}
}

func TestJsonExpr_LeftOperandMustBeColumnReference(t *testing.T) {
	_, err := Parse(`select (v+1)->'x' from t1;`)
	if err == nil {
		t.Fatalf("expected parse error for non-column left operand")
	}
}
