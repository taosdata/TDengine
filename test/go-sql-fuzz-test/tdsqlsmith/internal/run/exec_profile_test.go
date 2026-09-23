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

package run

import (
	"testing"

	"sqlparser"
	"tdsqlsmith/internal/parsergate"
)

func parseSelect(t *testing.T, sqlText string) *sqlparser.SelectStmt {
	t.Helper()
	res := parsergate.Parse(sqlText)
	if res.Err != nil {
		t.Fatalf("parse failed: %v", res.Err)
	}
	sel, ok := res.Stmt.(*sqlparser.SelectStmt)
	if !ok || sel == nil {
		t.Fatalf("expected select stmt, got %T", res.Stmt)
	}
	return sel
}

func TestShouldExecuteProfiles(t *testing.T) {
	simple := parseSelect(t, "select v from t1 where v > 1 order by v limit 5;")
	grouped := parseSelect(t, "select v, count(*) from t1 group by v;")
	joined := parseSelect(t, "select t1.v from t1 join t2 on t1.ts = t2.ts;")

	if !shouldExecuteStatement(simple, "select v from t1 where v > 1 order by v limit 5;", "strict") {
		t.Fatalf("strict should execute simple query")
	}
	if shouldExecuteStatement(grouped, "select v, count(*) from t1 group by v;", "strict") {
		t.Fatalf("strict should skip grouped query")
	}
	if !shouldExecuteStatement(grouped, "select v, count(*) from t1 group by v;", "balanced") {
		t.Fatalf("balanced should execute grouped query")
	}
	if shouldExecuteStatement(joined, "select t1.v from t1 join t2 on t1.ts = t2.ts;", "balanced") {
		t.Fatalf("balanced should skip join query")
	}
	if shouldExecuteStatement(joined, "select t1.v from t1 join t2 on t1.ts = t2.ts;", "aggressive") {
		t.Fatalf("aggressive still skips join query by heuristic")
	}
}

func TestNormalizeExecProfile(t *testing.T) {
	if got := normalizeExecProfile("balanced"); got != "balanced" {
		t.Fatalf("unexpected profile: %s", got)
	}
	if got := normalizeExecProfile("aggressive"); got != "aggressive" {
		t.Fatalf("unexpected profile: %s", got)
	}
	if got := normalizeExecProfile(""); got != "strict" {
		t.Fatalf("unexpected default profile: %s", got)
	}
	if got := normalizeExecProfile("invalid"); got != "strict" {
		t.Fatalf("unexpected fallback profile: %s", got)
	}
}
