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

package parsergate

import (
	"fmt"
	"strings"

	"sqlparser"
)

// Result captures the outcome of a parse attempt, including the parsed statement or the error and its classification.
//
// Result 记录一次解析尝试的结果，包括解析得到的语句，或错误及其分类。
type Result struct {
	Stmt    sqlparser.Statement // parsed statement, nil when parsing failed / 解析得到的语句，解析失败时为 nil
	Err     error               // parse error, nil on success / 解析错误，成功时为 nil
	ErrType string              // classified error type (e.g. "syntax", "incomplete", "panic") / 分类后的错误类型（如 "syntax"、"incomplete"、"panic"）
	Rules   []int               // grammar reduction rule ids, populated by ParseWithRules / 文法归约规则 id，由 ParseWithRules 填充
}

// Parse parses sqlText and returns a Result, recovering from parser panics as a "panic" error.
//
// Parse 解析 sqlText 并返回 Result，将解析器的 panic 恢复为 "panic" 错误。
func Parse(sqlText string) (res Result) {
	defer func() {
		if rec := recover(); rec != nil {
			res = Result{Err: fmt.Errorf("parse panic: %v", rec), ErrType: "panic"}
		}
	}()

	stmt, err := sqlparser.Parse(sqlText)
	if err != nil {
		return Result{Err: err, ErrType: classifyParseErr(err)}
	}
	return Result{Stmt: stmt}
}

// ParseWithRules parses sqlText and additionally records the grammar reduction rules applied, recovering from parser panics.
//
// ParseWithRules 解析 sqlText，并额外记录所应用的文法归约规则，同时从解析器 panic 中恢复。
func ParseWithRules(sqlText string) (res Result) {
	defer func() {
		if rec := recover(); rec != nil {
			res = Result{Err: fmt.Errorf("parse panic: %v", rec), ErrType: "panic"}
		}
	}()

	stmt, reductions, err := sqlparser.ParseWithReductions(sqlText)
	if err != nil {
		return Result{Err: err, ErrType: classifyParseErr(err)}
	}
	return Result{Stmt: stmt, Rules: reductions}
}

// classifyParseErr maps a parse error message to a coarse error type string.
//
// classifyParseErr 将解析错误信息映射为粗粒度的错误类型字符串。
func classifyParseErr(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	switch {
	case strings.Contains(msg, "incomplete"):
		return "incomplete"
	case strings.Contains(msg, "syntax"):
		return "syntax"
	case strings.Contains(msg, "semantic"):
		return "semantic"
	default:
		return "parse_error"
	}
}
