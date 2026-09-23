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

type DeleteStmt struct {
	Table string
	Where Expr
}

func (*DeleteStmt) iStatement() {}

func (s *DeleteStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("delete from %s", s.Table)
	if s.Where != nil {
		buf.Myprintf(" where %v", s.Where)
	}
}

func (s *DeleteStmt) walkSubtree(visit Visit) error {
	if s == nil {
		return nil
	}
	return Walk(visit, s.Where)
}

func NewDeleteStmt(table string, where Expr) *DeleteStmt {
	return &DeleteStmt{
		Table: table,
		Where: where,
	}
}
