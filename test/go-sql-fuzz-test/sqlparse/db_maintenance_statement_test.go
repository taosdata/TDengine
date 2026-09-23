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

func TestSsMigrateDatabaseStatement_Parse(t *testing.T) {
	stmt, err := Parse("ssmigrate database db1;")
	if err != nil {
		t.Fatalf("parse ssmigrate database failed: %v", err)
	}
	s, ok := stmt.(*SsMigrateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *SsMigrateDatabaseStmt, got %T", stmt)
	}
	if s.DbName != "db1" {
		t.Fatalf("unexpected ssmigrate stmt: %+v", s)
	}
}

func TestTrimDatabaseWalStatement_Parse(t *testing.T) {
	stmt, err := Parse("trim database db1 wal;")
	if err != nil {
		t.Fatalf("parse trim database wal failed: %v", err)
	}
	s, ok := stmt.(*TrimDatabaseWalStmt)
	if !ok {
		t.Fatalf("expected *TrimDatabaseWalStmt, got %T", stmt)
	}
	if s.DbName != "db1" {
		t.Fatalf("unexpected trim wal stmt: %+v", s)
	}
}
