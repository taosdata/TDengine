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

type CompactStmt struct {
	Scope    string
	Name     string
	Start    string
	End      string
	MetaOnly bool
	Force    bool
}

func (*CompactStmt) iStatement() {}

func (s *CompactStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	switch s.Scope {
	case "database":
		buf.Myprintf("compact database %s", s.Name)
	case "vgroups":
		if s.Name != "" {
			buf.Myprintf("compact %s. vgroups in (1)", s.Name)
		} else {
			buf.Myprintf("compact vgroups in (1)")
		}
	default:
		buf.Myprintf("compact %s", s.Scope)
		if s.Name != "" {
			buf.Myprintf(" %s", s.Name)
		}
	}
	if s.Start != "" {
		buf.Myprintf(" start with %s", s.Start)
	}
	if s.End != "" {
		buf.Myprintf(" end with %s", s.End)
	}
	if s.MetaOnly {
		buf.Myprintf(" meta_only")
	}
	if s.Force {
		buf.Myprintf(" force")
	}
}

func (s *CompactStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewCompactStmt(scope string, name string, start string, end string, metaOnly bool, force bool) *CompactStmt {
	return &CompactStmt{
		Scope:    scope,
		Name:     name,
		Start:    start,
		End:      end,
		MetaOnly: metaOnly,
		Force:    force,
	}
}
