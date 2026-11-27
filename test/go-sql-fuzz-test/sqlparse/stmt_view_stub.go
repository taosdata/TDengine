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

type CreateViewStmt struct {
	Replace bool
	Name    string
	Query   *SelectStmt
}

func (*CreateViewStmt) iStatement() {}

func (s *CreateViewStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("create ")
	if s.Replace {
		buf.Myprintf("or replace ")
	}
	buf.Myprintf("view %s", s.Name)
	if s.Query != nil {
		buf.Myprintf(" as %v", s.Query)
	}
}

func (s *CreateViewStmt) walkSubtree(visit Visit) error {
	if s == nil {
		return nil
	}
	return Walk(visit, s.Query)
}

type DropViewStmt struct {
	Name     string
	IfExists bool
}

func (*DropViewStmt) iStatement() {}

func (s *DropViewStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("drop view")
	if s.IfExists {
		buf.Myprintf(" if exists")
	}
	buf.Myprintf(" %s", s.Name)
}

func (s *DropViewStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewDropViewStmt(name string, ifExists bool) *DropViewStmt {
	return &DropViewStmt{Name: name, IfExists: ifExists}
}
