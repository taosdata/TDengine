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

type AlterNamedStmt struct {
	Kind     string
	Name     string
	IfExists bool
	Funcs    []string
}

func (*AlterNamedStmt) iStatement() {}

func (s *AlterNamedStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("alter %s", s.Kind)
	if s.IfExists {
		buf.Myprintf(" if exists")
	}
	buf.Myprintf(" %s", s.Name)
	if len(s.Funcs) > 0 {
		buf.Myprintf(" function(%s)", strings.Join(s.Funcs, ", "))
	}
}

func (s *AlterNamedStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewAlterNamedStmt(kind string, name string, ifExists bool, funcs []string) *AlterNamedStmt {
	return &AlterNamedStmt{
		Kind:     kind,
		Name:     name,
		IfExists: ifExists,
		Funcs:    funcs,
	}
}
