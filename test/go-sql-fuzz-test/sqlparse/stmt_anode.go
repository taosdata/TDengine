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
	"fmt"
	"sqlparser/tool"
	"strconv"
)

type CreateAnodeStmt struct {
	Url string
}

func (s *CreateAnodeStmt) iStatement() {}

func (s *CreateAnodeStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("create anode '%s'", s.Url)
}

func (s *CreateAnodeStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewCreateAnodeStmt(lexer yyLexer, url Token) *CreateAnodeStmt {
	return &CreateAnodeStmt{
		Url: tool.BytesToString(url.Bytes),
	}
}

type UpdateAnodeStmt struct {
	AnodeId int32
}

func (s *UpdateAnodeStmt) iStatement() {}

func (s *UpdateAnodeStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	if s.AnodeId >= 0 {
		buf.Myprintf("update anode %s", strconv.FormatInt(int64(s.AnodeId), 10))
		return
	}
	buf.Myprintf("update all anodes")
}

func (s *UpdateAnodeStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewUpdateAnodeStmt(lexer yyLexer, anodeId *Token) *UpdateAnodeStmt {
	anode := int32(-1)
	var err error
	if anodeId != nil {
		anode, err = tool.ConvertBytesToInt32(anodeId.Bytes)
		if err != nil {
			lexer.Error(fmt.Sprintf("can not parse anode id to int32: %s, err: %v", anodeId.Bytes, err))
			return nil
		}
	}
	return &UpdateAnodeStmt{
		AnodeId: anode,
	}
}

type DropAnodeStmt struct {
	AnodeId int32
}

func (s *DropAnodeStmt) iStatement() {}

func (s *DropAnodeStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("drop anode")
	if s.AnodeId >= 0 {
		buf.Myprintf(" %s", strconv.FormatInt(int64(s.AnodeId), 10))
	}
}

func (s *DropAnodeStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewDropAnodeStmt(lexer yyLexer, anodeId *Token, updateAll bool) *DropAnodeStmt {
	anode := int32(-1)
	var err error
	if anodeId != nil {
		anode, err = tool.ConvertBytesToInt32(anodeId.Bytes)
		if err != nil {
			lexer.Error(fmt.Sprintf("can not parse anode id to int32: %s, err: %v", anodeId.Bytes, err))
			return nil
		}
	}
	return &DropAnodeStmt{
		AnodeId: anode,
	}
}
