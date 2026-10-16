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

func TestLiteralAlignment_TimestampBranch(t *testing.T) {
	stmt, err := Parse("select timestamp '2020-01-01 00:00:00';")
	if err != nil {
		t.Fatalf("parse timestamp literal failed: %v", err)
	}
	s, ok := stmt.(*SelectStmt)
	if !ok || len(s.Select) != 1 {
		t.Fatalf("unexpected stmt: %T %+v", stmt, stmt)
	}
	lit, ok := s.Select[0].(Literal)
	if !ok {
		t.Fatalf("expected Literal, got %T", s.Select[0])
	}
	if string(lit.Val.Bytes) == "" {
		t.Fatalf("unexpected empty timestamp literal: %+v", lit)
	}
}

func TestLiteralAlignment_DurationBranch(t *testing.T) {
	stmt, err := Parse("select 1d;")
	if err != nil {
		t.Fatalf("parse duration literal failed: %v", err)
	}
	s, ok := stmt.(*SelectStmt)
	if !ok || len(s.Select) != 1 {
		t.Fatalf("unexpected stmt: %T %+v", stmt, stmt)
	}
	lit, ok := s.Select[0].(Literal)
	if !ok || lit.Type != LiteralDuration {
		t.Fatalf("expected duration literal, got %T %+v", s.Select[0], s.Select[0])
	}
}

func TestLiteralAlignment_PlaceholderBranch(t *testing.T) {
	stmt, err := Parse("select ?;")
	if err != nil {
		t.Fatalf("parse placeholder literal failed: %v", err)
	}
	s, ok := stmt.(*SelectStmt)
	if !ok || len(s.Select) != 1 {
		t.Fatalf("unexpected stmt: %T %+v", stmt, stmt)
	}
	lit, ok := s.Select[0].(Literal)
	if !ok || string(lit.Val.Bytes) != "?" {
		t.Fatalf("expected placeholder literal, got %T %+v", s.Select[0], s.Select[0])
	}
}
