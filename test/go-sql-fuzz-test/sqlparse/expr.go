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
	"bytes"
	"fmt"
	"reflect"
)

type SelectExpr interface {
	iSelectExpr()
	SQLNode
}

type SQLNode interface {
	Format(buf *TrackedBuffer)
	// walkSubtree calls visit on all underlying nodes
	// of the subtree, but not the current one. Walking
	// must be interrupted if visit returns an error.
	walkSubtree(visit Visit) error
}

type TrackedBuffer struct {
	*bytes.Buffer
	bindLocations []bindLocation
	nodeFormatter NodeFormatter
}

// Myprintf mimics fmt.Fprintf(buf, ...), but limited to Node(%v),
// Node.Value(%s) and tool.BytesToString(%s). It also allows a %a for a value argument, in
// which case it adds tracking info for future substitutions.
//
// The name must be something other than the usual Printf() to avoid "go vet"
// warnings due to our custom format specifiers.
func (buf *TrackedBuffer) Myprintf(format string, values ...interface{}) {
	end := len(format)
	fieldnum := 0
	for i := 0; i < end; {
		lasti := i
		for i < end && format[i] != '%' {
			i++
		}
		if i > lasti {
			buf.WriteString(format[lasti:i])
		}
		if i >= end {
			break
		}
		i++ // '%'
		switch format[i] {
		case 'c':
			switch v := values[fieldnum].(type) {
			case byte:
				buf.WriteByte(v)
			case rune:
				buf.WriteRune(v)
			default:
				panic(fmt.Sprintf("unexpected TrackedBuffer type %T", v))
			}
		case 's':
			switch v := values[fieldnum].(type) {
			case []byte:
				buf.Write(v)
			case string:
				buf.WriteString(v)
			default:
				panic(fmt.Sprintf("unexpected TrackedBuffer type %T", v))
			}
		case 'v':
			node := values[fieldnum].(SQLNode)
			if buf.nodeFormatter == nil {
				node.Format(buf)
			} else {
				buf.nodeFormatter(buf, node)
			}
		case 'a':
			buf.WriteArg(values[fieldnum].(string))
		default:
			panic("unexpected")
		}
		fieldnum++
		i++
	}
}

// WriteArg writes a value argument into the buffer along with
// tracking information for future substitutions. arg must contain
// the ":" or "::" prefix.
func (buf *TrackedBuffer) WriteArg(arg string) {
	buf.bindLocations = append(buf.bindLocations, bindLocation{
		offset: buf.Len(),
		length: len(arg),
	})
	buf.WriteString(arg)
}

type bindLocation struct {
	offset, length int
}

type NodeFormatter func(buf *TrackedBuffer, node SQLNode)

type Visit func(node SQLNode) (kontinue bool, err error)

// Walk calls visit on every node.
// If visit returns true, the underlying nodes
// are also visited. If it returns an error, walking
// is interrupted, and the error is returned.
func Walk(visit Visit, nodes ...SQLNode) error {
	for _, node := range nodes {
		if isNilSQLNode(node) {
			continue
		}
		kontinue, err := visit(node)
		if err != nil {
			return err
		}
		if kontinue {
			err = node.walkSubtree(visit)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func isNilSQLNode(node SQLNode) bool {
	if node == nil {
		return true
	}
	v := reflect.ValueOf(node)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

type SelectExprs []SelectExpr

type Exprs []Expr

// Expr represents an expression.
type Expr interface {
	iExpr()
	// replace replaces any subexpression that matches
	// from with to. The implementation can use the
	// replaceExprs convenience function.
	replace(from, to Expr) bool
	SQLNode
}
