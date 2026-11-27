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
	"strings"
	"testing"
)

func TestErrorHandling_IncompleteSQL(t *testing.T) {
	_, err := Parse("select")
	if err == nil {
		t.Fatalf("expected error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "incomplete sql") {
		t.Fatalf("expected incomplete sql error, got: %q", msg)
	}
	if !strings.Contains(msg, "position") {
		t.Fatalf("expected position in error, got: %q", msg)
	}
}

func TestErrorHandling_UnexpectedTokenNear(t *testing.T) {
	_, err := Parse("select from t")
	if err == nil {
		t.Fatalf("expected error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "syntax error") {
		t.Fatalf("expected syntax error, got: %q", msg)
	}
	if !strings.Contains(msg, "near 'from'") {
		t.Fatalf("expected near token in error, got: %q", msg)
	}
}

func TestErrorHandling_InvalidCharNear(t *testing.T) {
	_, err := Parse("select @")
	if err == nil {
		t.Fatalf("expected error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "near '@'") {
		t.Fatalf("expected invalid-char near token, got: %q", msg)
	}
}
