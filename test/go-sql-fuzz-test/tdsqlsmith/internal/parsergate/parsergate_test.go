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

package parsergate

import "testing"

func TestParseClassifiesRejectTypes(t *testing.T) {
	ok := Parse("select v from t1;")
	if ok.Err != nil {
		t.Fatalf("unexpected parse error: %v", ok.Err)
	}

	bad := Parse("select from t1;")
	if bad.Err == nil {
		t.Fatalf("expected parse error")
	}
	if bad.ErrType == "" {
		t.Fatalf("expected non-empty parse err type")
	}
}

func TestParseQuery(t *testing.T) {
	res := Parse("select v from t1 where v > 1 order by v limit 1;")
	if res.Err != nil {
		t.Fatalf("unexpected parse error: %v", res.Err)
	}
	if len(res.Rules) != 0 {
		t.Fatalf("expected no reduce-rule trace, got: %v", res.Rules)
	}
}

func TestParseWithRulesQuery(t *testing.T) {
	res := ParseWithRules("select v from t1 where v > 1 order by v limit 1;")
	if res.Err != nil {
		t.Fatalf("unexpected parse error: %v", res.Err)
	}
	if len(res.Rules) == 0 {
		t.Fatalf("expected reduce-rule trace, got empty")
	}
}

func TestParseClassifiesSemanticReject(t *testing.T) {
	res := Parse("select rand(1, 2) from t1;")
	if res.Err == nil {
		t.Fatalf("expected semantic parse reject")
	}
	if res.ErrType != "semantic" {
		t.Fatalf("expected semantic err type, got: %s (%v)", res.ErrType, res.Err)
	}
}
