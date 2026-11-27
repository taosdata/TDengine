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

func TestCreateFunctionStatement_ParseAndFields(t *testing.T) {
	stmt, err := Parse("create or replace aggregate function if not exists f1 as 'return 1' outputtype int bufsize 32 language 'python';")
	if err != nil {
		t.Fatalf("parse create function failed: %v", err)
	}
	s, ok := stmt.(*CreateFunctionStmt)
	if !ok {
		t.Fatalf("expected *CreateFunctionStmt, got %T", stmt)
	}
	if s.Name != "f1" || s.Body != "return 1" || s.OutputType != "int" || !s.IgnoreExists || !s.OrReplace || !s.Aggregate || s.Bufsize != 32 || s.Language != "python" {
		t.Fatalf("unexpected create function stmt: %+v", s)
	}
	got := formatStatementForRoundTrip(t, s)
	want := "create or replace aggregate function if not exists f1 as 'return 1' outputtype int bufsize 32 language 'python'"
	if got != want {
		t.Fatalf("unexpected create function format: got=%q want=%q", got, want)
	}
}

func TestCreateFunctionStatement_ErrorPath(t *testing.T) {
	if _, err := Parse("create function f1 as 'x' outputtype;"); err == nil {
		t.Fatalf("expected parse error for incomplete create function")
	}
}
