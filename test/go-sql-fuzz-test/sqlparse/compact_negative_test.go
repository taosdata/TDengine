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

func TestQuery_EveryRejectsNonDurationLiteral(t *testing.T) {
	if _, err := Parse("select v from t1 every('10s');"); err == nil {
		t.Fatalf("expected parse error for string literal in every()")
	}
	if _, err := Parse("select v from t1 every(10);"); err == nil {
		t.Fatalf("expected parse error for integer literal in every()")
	}
}

func TestRecalculateStreamRangeRequired(t *testing.T) {
	if _, err := Parse("recalculate stream db1.s1;"); err == nil {
		t.Fatalf("expected parse error when recalculate range is missing")
	}
}

func TestUserOption_PasswordReuseUnlimitedRejected(t *testing.T) {
	if _, err := Parse("create user u_time2 pass 'p' password_reuse_time unlimited;"); err == nil {
		t.Fatalf("expected parse error for password_reuse_time unlimited")
	}
	if _, err := Parse("create user u_time3 pass 'p' password_reuse_max unlimited;"); err == nil {
		t.Fatalf("expected parse error for password_reuse_max unlimited")
	}
}

func TestCreateStableRequiresTags(t *testing.T) {
	_, err := Parse("create stable if not exists db1.st_no_tags (ts timestamp, v int);")
	if err == nil {
		t.Fatalf("expected parse error when create stable omits tags")
	}
}
