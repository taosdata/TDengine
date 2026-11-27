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

package executor

import "testing"

func TestParseUseDatabase(t *testing.T) {
	tests := []struct {
		sql  string
		want string
	}{
		{sql: "use testdb", want: "testdb"},
		{sql: "USE testdb;", want: "testdb"},
		{sql: " use `testdb` ", want: "testdb"},
		{sql: `use "testdb"`, want: "testdb"},
		{sql: "select 1", want: ""},
		{sql: "use", want: ""},
	}
	for _, tc := range tests {
		if got := parseUseDatabase(tc.sql); got != tc.want {
			t.Fatalf("parseUseDatabase(%q)=%q want %q", tc.sql, got, tc.want)
		}
	}
}

func TestShouldRetryWithDatabase(t *testing.T) {
	errDb := mockErr("[0x2616] Database not specified")
	if !shouldRetryWithDatabase(errDb, "db1", "select * from t1") {
		t.Fatalf("expected retry=true for database-not-specified")
	}
	if shouldRetryWithDatabase(errDb, "", "select * from t1") {
		t.Fatalf("expected retry=false when current db missing")
	}
	if shouldRetryWithDatabase(mockErr("other error"), "db1", "select * from t1") {
		t.Fatalf("expected retry=false for non database error")
	}
	if shouldRetryWithDatabase(errDb, "db1", "use db2") {
		t.Fatalf("expected retry=false for use statement")
	}
}

type mockErr string

func (m mockErr) Error() string { return string(m) }
