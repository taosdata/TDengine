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

func formatInsertIdentifier(raw []byte) string {
	s := string(raw)
	if s == "" {
		return s
	}
	needsQuote := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		isLower := ch >= 'a' && ch <= 'z'
		isUpper := ch >= 'A' && ch <= 'Z'
		isDigit := ch >= '0' && ch <= '9'
		if !(isLower || isUpper || isDigit || ch == '_') {
			needsQuote = true
			break
		}
	}
	if strings.ToLower(s) != s {
		needsQuote = true
	}
	if !needsQuote {
		return s
	}
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

func writeByteList(buf *TrackedBuffer, vals [][]byte) {
	for i, v := range vals {
		if i > 0 {
			buf.Myprintf(", ")
		}
		buf.Myprintf("%s", formatInsertIdentifier(v))
	}
}

func writeSQLValList(buf *TrackedBuffer, vals []*SQLVal) {
	for i, v := range vals {
		if i > 0 {
			buf.Myprintf(", ")
		}
		buf.Myprintf("%v", v)
	}
}

func formatInsertNode(buf *TrackedBuffer, n InsertNode, withInto bool) {
	if withInto {
		buf.Myprintf("insert into ")
	}
	if n.TableName != nil {
		buf.Myprintf("%v", n.TableName)
	}
	if n.Using != nil && n.Using.TableName != nil {
		buf.Myprintf(" using %v", n.Using.TableName)
		if len(n.Using.TagKeys) > 0 {
			buf.Myprintf(" (")
			writeByteList(buf, n.Using.TagKeys)
			buf.Myprintf(")")
		}
		buf.Myprintf(" tags (")
		writeSQLValList(buf, n.Using.TagValues)
		buf.Myprintf(")")
	}
	if len(n.Fields) > 0 {
		buf.Myprintf(" (")
		writeByteList(buf, n.Fields)
		buf.Myprintf(")")
	}
	buf.Myprintf(" values")
	for i, row := range n.Values {
		if i > 0 {
			buf.Myprintf(",")
		}
		buf.Myprintf(" (")
		writeSQLValList(buf, row)
		buf.Myprintf(")")
	}
}

type UsingClause struct {
	TableName *TableName
	TagKeys   [][]byte
	TagValues []*SQLVal
}

type InsertNode struct {
	TableName *TableName
	Using     *UsingClause
	Fields    [][]byte
	Values    [][]*SQLVal
}

type InsertStatement []InsertNode

func (s InsertStatement) iStatement() {}

func (s InsertStatement) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	if len(s) == 0 {
		buf.Myprintf("insert")
		return
	}
	for i := range s {
		if i > 0 {
			buf.Myprintf(" ")
		}
		formatInsertNode(buf, s[i], i == 0)
	}
}

func (s InsertStatement) walkSubtree(visit Visit) error {
	for i := range s {
		if s[i].TableName != nil {
			if err := Walk(visit, *s[i].TableName); err != nil {
				return err
			}
		}
		if s[i].Using != nil && s[i].Using.TableName != nil {
			if err := Walk(visit, *s[i].Using.TableName); err != nil {
				return err
			}
		}
		for _, row := range s[i].Values {
			for _, v := range row {
				if err := Walk(visit, v); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
