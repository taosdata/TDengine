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

import "strings"

type InsertQueryStmt struct {
	Table   string
	Columns []string
	Query   *SelectStmt
}

func (*InsertQueryStmt) iStatement() {}

func (s *InsertQueryStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("insert into %s", s.Table)
	if len(s.Columns) > 0 {
		buf.Myprintf(" (%s)", strings.Join(s.Columns, ", "))
	}
	if s.Query != nil {
		buf.Myprintf(" %v", s.Query)
	}
}

func (s *InsertQueryStmt) walkSubtree(visit Visit) error {
	if s == nil {
		return nil
	}
	return Walk(visit, s.Query)
}
