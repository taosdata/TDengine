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

func TestTokenToInt32_Coverage(t *testing.T) {
	if got := tokenToInt32(Token{Bytes: []byte("12")}); got != 12 {
		t.Fatalf("unexpected parse int result: %d", got)
	}
	if got := tokenToInt32(Token{Bytes: []byte("not_int")}); got != -1 {
		t.Fatalf("unexpected parse fallback result: %d", got)
	}
}

func TestMakeDropTableEntryText_Coverage(t *testing.T) {
	if got := makeDropTableEntryText(false, "db.t1"); got != "db.t1" {
		t.Fatalf("unexpected no-exists text: %s", got)
	}
	if got := makeDropTableEntryText(true, "db.t2"); got != "if exists db.t2" {
		t.Fatalf("unexpected exists text: %s", got)
	}
}

func TestTableNameFromFullName_Coverage(t *testing.T) {
	got1 := tableNameFromFullName("db1.t1")
	if got1 == nil || got1.Qualifier.String() != "db1" || got1.Name.String() != "t1" {
		t.Fatalf("unexpected qualified table name: %+v", got1)
	}
	got2 := tableNameFromFullName("t2")
	if got2 == nil || got2.Qualifier.String() != "" || got2.Name.String() != "t2" {
		t.Fatalf("unexpected plain table name: %+v", got2)
	}
}
