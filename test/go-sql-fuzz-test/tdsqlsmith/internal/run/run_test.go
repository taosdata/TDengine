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

func TestDryRunCoversAllQueryRules(t *testing.T) {
	outDir := t.TempDir()
	res, err := Execute(context.Background(), Config{
		Version:         "test",
		Seed:            20260216,
		Cases:           4000,
		StmtTimeout:     2 * time.Second,
		OutDir:          outDir,
		MutationLevel:   0,
		StopWhenCovered: true,
		DryRun:          true,
		Verbose:         false,
	})
	if err != nil {
		t.Fatalf("run execute failed: %v", err)
	}
	if res.QueryRules.Required != 113 {
		t.Fatalf("unexpected query rule set size: %d", res.QueryRules.Required)
	}
	if res.QueryRules.Hit != res.QueryRules.Required {
		t.Fatalf("query rules not fully covered: %d/%d missing=%v", res.QueryRules.Hit, res.QueryRules.Required, res.QueryRules.Missing)
	}
}
