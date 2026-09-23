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

package logger

import "testing"

func TestSymbolForClass(t *testing.T) {
	cases := map[string]string{
		"syntax":       "S",
		"parse_reject": "S",
		"timeout":      "t",
		"conn_lost":    "C",
		"db_error":     "e",
	}
	for in, want := range cases {
		if got := symbolForClass(in); got != want {
			t.Fatalf("symbol mismatch for %q: got=%q want=%q", in, got, want)
		}
	}
}
