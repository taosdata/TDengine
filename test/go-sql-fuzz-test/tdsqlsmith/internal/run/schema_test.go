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

	"tdsqlsmith/internal/catalog"
)

func TestSchemaFromPrepared(t *testing.T) {
	in := &catalog.Prepared{
		Database: "x",
		Tables: []catalog.Table{
			{
				Name: "t1",
				Columns: []catalog.Column{
					{Name: "ts", Type: "timestamp"},
					{Name: "v", Type: "double"},
				},
			},
			{
				Name: "t2",
				Columns: []catalog.Column{
					{Name: "id", Type: "int"},
				},
			},
		},
	}
	out := schemaFromPrepared(in)
	if len(out.Tables) != 2 {
		t.Fatalf("unexpected table count: %d", len(out.Tables))
	}
	if out.Tables[0].Name != "t1" || len(out.Tables[0].Columns) != 2 {
		t.Fatalf("unexpected first table: %#v", out.Tables[0])
	}
}
