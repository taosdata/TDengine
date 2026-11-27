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

func TestCreateEncryptKeyStatement_StringToken(t *testing.T) {
	stmt, err := Parse("create encrypt_key 'k';")
	if err != nil {
		t.Fatalf("parse create encrypt_key failed: %v", err)
	}
	if _, ok := stmt.(*CreateEncryptKeyStmt); !ok {
		t.Fatalf("expected *CreateEncryptKeyStmt, got %T", stmt)
	}
}

func TestAlterEncryptKeyStatement_StringToken(t *testing.T) {
	stmt1, err := Parse("alter system set svr_key 'k';")
	if err != nil {
		t.Fatalf("parse alter svr_key failed: %v", err)
	}
	if _, ok := stmt1.(*AlterEncryptKeyStmt); !ok {
		t.Fatalf("expected *AlterEncryptKeyStmt, got %T", stmt1)
	}
	ak1 := stmt1.(*AlterEncryptKeyStmt)
	if ak1.KeyType != 0 || ak1.NewKey != "k" {
		t.Fatalf("unexpected alter svr_key stmt: %+v", ak1)
	}
	if got := formatStatementForRoundTrip(t, ak1); got != "alter system set svr_key 'k'" {
		t.Fatalf("unexpected format for alter svr_key: %q", got)
	}
	if err := Walk(func(node SQLNode) (bool, error) { return true, nil }, ak1); err != nil {
		t.Fatalf("walk alter svr_key failed: %v", err)
	}

	stmt2, err := Parse("alter system set db_key 'k';")
	if err != nil {
		t.Fatalf("parse alter db_key failed: %v", err)
	}
	if _, ok := stmt2.(*AlterEncryptKeyStmt); !ok {
		t.Fatalf("expected *AlterEncryptKeyStmt, got %T", stmt2)
	}
	ak2 := stmt2.(*AlterEncryptKeyStmt)
	if ak2.KeyType != 1 || ak2.NewKey != "k" {
		t.Fatalf("unexpected alter db_key stmt: %+v", ak2)
	}
	if got := formatStatementForRoundTrip(t, ak2); got != "alter system set db_key 'k'" {
		t.Fatalf("unexpected format for alter db_key: %q", got)
	}
	if err := Walk(func(node SQLNode) (bool, error) { return true, nil }, ak2); err != nil {
		t.Fatalf("walk alter db_key failed: %v", err)
	}
}

func TestEncryptAlgrStatements_StringToken(t *testing.T) {
	stmt1, err := Parse("create encrypt_algr 'a' algr_name 'n' desc 'd' algr_type 't' ossl_algr_name 'o';")
	if err != nil {
		t.Fatalf("parse create encrypt_algr failed: %v", err)
	}
	if _, ok := stmt1.(*CreateAlgrStmt); !ok {
		t.Fatalf("expected *CreateAlgrStmt, got %T", stmt1)
	}

	stmt2, err := Parse("drop encrypt_algr 'a';")
	if err != nil {
		t.Fatalf("parse drop encrypt_algr failed: %v", err)
	}
	if _, ok := stmt2.(*DropAlgrStmt); !ok {
		t.Fatalf("expected *DropAlgrStmt, got %T", stmt2)
	}
}
