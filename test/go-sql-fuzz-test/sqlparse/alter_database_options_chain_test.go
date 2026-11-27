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

func TestAlterDatabaseOptionChain(t *testing.T) {
	stmt, err := Parse("alter database db1 buffer 10 wal_retention_period -5 ss_keeplocal 2;")
	if err != nil {
		t.Fatalf("parse alter database option chain failed: %v", err)
	}
	s, ok := stmt.(*AlterDatabaseStmt)
	if !ok {
		t.Fatalf("expected *AlterDatabaseStmt, got %T", stmt)
	}
	if s.Name != "db1" || s.Options == nil {
		t.Fatalf("unexpected alter database stmt: %+v", s)
	}
	if s.Options.Buffer != 10 {
		t.Fatalf("unexpected buffer: %+v", s.Options)
	}
	if s.Options.WalRetentionPeriod != -5 || !s.Options.WalRetentionPeriodIsSet {
		t.Fatalf("unexpected wal_retention_period: %+v", s.Options)
	}
	if s.Options.SsKeepLocal != 2 {
		t.Fatalf("unexpected ss_keeplocal: %+v", s.Options)
	}
}
