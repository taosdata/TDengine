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

type DropNamedStmt struct {
	Kind     string
	Name     string
	IfExists bool
}

func (*DropNamedStmt) iStatement() {}

func (s *DropNamedStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("drop %s", s.Kind)
	if s.IfExists {
		buf.Myprintf(" if exists")
	}
	buf.Myprintf(" %s", s.Name)
}

func (s *DropNamedStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewDropNamedStmt(kind string, name string, ifExists bool) *DropNamedStmt {
	return &DropNamedStmt{
		Kind:     kind,
		Name:     name,
		IfExists: ifExists,
	}
}
