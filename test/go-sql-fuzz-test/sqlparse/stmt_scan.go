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

type ScanStmt struct {
	Scope string
	Name  string
	Start string
	End   string
}

func (*ScanStmt) iStatement() {}

func (s *ScanStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	switch s.Scope {
	case "database":
		buf.Myprintf("scan database %s", s.Name)
	case "vgroups":
		if s.Name != "" {
			buf.Myprintf("scan %s. vgroups in (1)", s.Name)
		} else {
			buf.Myprintf("scan vgroups in (1)")
		}
	default:
		buf.Myprintf("scan %s", s.Scope)
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
}

func (s *ScanStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewScanStmt(scope string, name string, start string, end string) *ScanStmt {
	return &ScanStmt{
		Scope: scope,
		Name:  name,
		Start: start,
		End:   end,
	}
}
