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

package api

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Oversized values are truncated to their column/tag widths. TDengine checks
// the declared width against the unescaped value and rejects the whole insert
// with "Value too long for column/tag" when it overflows, so values that fit
// after truncation keep the report data flowing.
//
// Widths are written as literals on purpose: asserting against the constants
// the code uses would pass even if a constant were wrong. nchar(N) holds N
// characters — measured against a live server, nchar(32) accepts 32 ASCII or 32
// CJK characters and rejects 33 of either — so those are truncated by runes,
// while binary/varchar columns are byte-exact.
func TestInsertSql_TruncatesOversizedValues(t *testing.T) {
	longClusterID := strings.Repeat("c", 300) // cluster_id nchar(32) -> 32 chars
	longEp := strings.Repeat("e", 1200)       // dnode_ep nchar(255) -> 255 chars
	ts := "2026-09-15 00:00:00"

	dnodeSql := insertDnodeSql(DnodeInfo{}, 1, longEp, longClusterID, ts)
	assert.Contains(t, dnodeSql, "tags (1, '"+strings.Repeat("e", 255)+"', '"+strings.Repeat("c", 32)+"')")

	sqls := insertClusterInfoSql(ClusterInfo{
		FirstEp: longEp,
		Version: "3.4.0.0.extra.long",                                                     // version binary(12)
		Dnodes:  []Dnode{{DnodeID: 1, DnodeEp: longEp, Status: "ready-but-way-too-long"}}, // status binary(10)
		Mnodes:  []Mnode{{MnodeID: 1, MnodeEp: longEp, Role: "leader-too-long"}},          // role binary(10)
	}, longClusterID, 1, ts)
	assert.Contains(t, sqls[0], "'ready-but-'")
	assert.Contains(t, sqls[1], "'leader-too'")
	assert.Contains(t, sqls[2], "'3.4.0.0.extr'")
	assert.Contains(t, sqls[2], "'"+strings.Repeat("e", 255)+"'") // first_ep binary(255), byte-exact

	dirSqls := insertDataDirSql(DiskInfo{
		Datadir: []DataDir{{Name: strings.Repeat("d", 900)}},
		Logdir:  LogDir{Name: strings.Repeat("l", 900)},
		Tempdir: TempDir{Name: strings.Repeat("t", 900)},
	}, 1, longEp, longClusterID, ts)
	assert.Contains(t, dirSqls[0], "'"+strings.Repeat("d", 200)+"'") // name nchar(200) -> 200 chars
	assert.Contains(t, dirSqls[1], "'"+strings.Repeat("l", 200)+"'")
	assert.Contains(t, dirSqls[2], "'"+strings.Repeat("t", 200)+"'")

	vgSqls := insertVgroupSql(VgroupInfo{
		VgroupID:     3,
		DatabaseName: strings.Repeat("b", 40),                 // database_name binary(33)
		Status:       strings.Repeat("s", 600),                // status binary(512)
		Vnodes:       []Vnode{{VnodeRole: "leader-too-long"}}, // vnode_role binary(10)
	}, 1, longEp, longClusterID, ts)
	assert.Contains(t, vgSqls[0], "'"+strings.Repeat("b", 33)+"'")
	assert.Contains(t, vgSqls[0], "'"+strings.Repeat("s", 512)+"'")
	assert.Contains(t, vgSqls[1], "'leader-too'")
}

// Multi-byte text must use the whole column: nchar(N) holds N characters, so a
// value of N CJK characters (up to 4N bytes) fits and must reach TDengine
// untruncated. Truncating to N bytes instead would cut it to about a third.
func TestInsertSql_KeepsMultiByteValuesThatFitTheColumn(t *testing.T) {
	cjk255 := strings.Repeat("中", 255) // 765 bytes, 255 chars: fits nchar(255)
	cjk32 := strings.Repeat("汉", 32)   // 96 bytes, 32 chars: fits nchar(32)
	cjk200 := strings.Repeat("字", 200) // 600 bytes, 200 chars: fits nchar(200)
	assert.Greater(t, len(cjk255), 255, "the value must exceed the column's width in bytes")
	assert.Greater(t, len(cjk32), 32)
	assert.Greater(t, len(cjk200), 200)

	dnodeSql := insertDnodeSql(DnodeInfo{}, 1, cjk255, cjk32, "2026-09-15 00:00:00")
	assert.Contains(t, dnodeSql, "'"+cjk255+"'")
	assert.Contains(t, dnodeSql, "'"+cjk32+"'")

	dirSqls := insertDataDirSql(DiskInfo{Datadir: []DataDir{{Name: cjk200}}}, 1, cjk255, cjk32, "2026-09-15 00:00:00")
	assert.Contains(t, dirSqls[0], "'"+cjk200+"'")
}
