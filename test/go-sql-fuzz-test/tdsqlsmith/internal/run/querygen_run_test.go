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
	"context"
	"testing"
	"time"
)

func TestDryRunRecordsQueryCombos(t *testing.T) {
	res, err := Execute(context.Background(), Config{
		Version:         "test",
		Seed:            20260219,
		Cases:           250,
		StmtTimeout:     2 * time.Second,
		OutDir:          t.TempDir(),
		MutationLevel:   0,
		StopWhenCovered: false,
		DryRun:          true,
		Verbose:         false,
	})
	if err != nil {
		t.Fatalf("run execute failed: %v", err)
	}
	if len(res.QueryRules.Missing) > 0 && res.QueryRules.Hit == 0 {
		t.Fatalf("unexpected empty query rule hits")
	}
}
