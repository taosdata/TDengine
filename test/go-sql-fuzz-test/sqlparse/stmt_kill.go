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

type KillStmt struct {
	Kind   string
	Target string
}

func (k *KillStmt) iStatement() {}

func (k *KillStmt) Format(buf *TrackedBuffer) {
	if k == nil {
		return
	}
	if k.Kind == "query" {
		buf.Myprintf("kill query '%s'", k.Target)
		return
	}
	buf.Myprintf("kill %s %s", k.Kind, k.Target)
}

func (k *KillStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewKillStmt(kind string, target Token) *KillStmt {
	return &KillStmt{
		Kind:   kind,
		Target: string(target.Bytes),
	}
}
