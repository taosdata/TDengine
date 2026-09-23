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

import (
	"sqlparser/tool"
	"strconv"
)

type CreateMountStmt struct {
	Name         string
	DnodeID      int32
	Path         string
	IgnoreExists bool
}

func (s *CreateMountStmt) iStatement() {}

func (s *CreateMountStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("create mount")
	if s.IgnoreExists {
		buf.Myprintf(" if not exists")
	}
	buf.Myprintf(" %s on dnode %s from '%s'", s.Name, strconv.FormatInt(int64(s.DnodeID), 10), s.Path)
}

func (s *CreateMountStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewCreateMountStmt(lexer yyLexer, nameTok, dnodeTok, pathTok Token, ignoreExists bool) *CreateMountStmt {
	dnodeID := int32(-1)
	if v, err := tool.ConvertBytesToInt32(dnodeTok.Bytes); err == nil {
		dnodeID = v
	} else {
		lexer.Error("invalid dnode id for create mount")
	}
	return &CreateMountStmt{
		Name:         string(nameTok.Bytes),
		DnodeID:      dnodeID,
		Path:         string(pathTok.Bytes),
		IgnoreExists: ignoreExists,
	}
}

type DropMountStmt struct {
	Name     string
	IfExists bool
}

func (s *DropMountStmt) iStatement() {}

func (s *DropMountStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("drop mount")
	if s.IfExists {
		buf.Myprintf(" if exists")
	}
	buf.Myprintf(" %s", s.Name)
}

func (s *DropMountStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewDropMountStmt(nameTok Token, ifExists bool) *DropMountStmt {
	return &DropMountStmt{
		Name:     string(nameTok.Bytes),
		IfExists: ifExists,
	}
}
