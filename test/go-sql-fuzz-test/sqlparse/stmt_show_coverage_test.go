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

import (
	"bytes"
	"testing"
)

func TestShowStmt_FormatAndWalkCoverage(t *testing.T) {
	tb := &TrackedBuffer{Buffer: &bytes.Buffer{}}

	var nilShow *ShowStmt
	nilShow.Format(tb)
	if got := tb.String(); got != "" {
		t.Fatalf("unexpected nil show format output: %q", got)
	}
	if err := nilShow.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("nil show walkSubtree failed: %v", err)
	}

	s := NewShowStmt("role_column_privileges")
	if s.Kind != "role_column_privileges" {
		t.Fatalf("unexpected kind: %+v", s)
	}
	s.iStatement()
	s.Format(tb)
	if got := tb.String(); got != "show role column privileges" {
		t.Fatalf("unexpected show format output: %q", got)
	}
	if err := s.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("show walkSubtree failed: %v", err)
	}
}
