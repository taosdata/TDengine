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

// Statement represents a statement.
type Statement interface {
	iStatement()
	SQLNode
}

func Parse(sql string) (Statement, error) {
	if hasDoubleDashOutsideLiteral(sql) {
		return nil, semanticErr("double-dash token is not allowed")
	}
	if insertStmt, err := parseInsertSQL(sql); err == nil {
		if err := validateStatementSemantic(insertStmt); err != nil {
			return nil, err
		}
		return insertStmt, nil
	}
	scanner := NewScanner(sql)
	if yyParse(scanner) != 0 {
		return nil, scanner.lastErr
	}
	if scanner.lastErr != nil {
		return nil, scanner.lastErr
	}
	if err := validateStatementSemantic(scanner.ParseTree); err != nil {
		return nil, err
	}
	return scanner.ParseTree, nil
}

func ParseWithReductions(sql string) (Statement, []int, error) {
	if hasDoubleDashOutsideLiteral(sql) {
		return nil, nil, semanticErr("double-dash token is not allowed")
	}
	if insertStmt, err := parseInsertSQL(sql); err == nil {
		if err := validateStatementSemantic(insertStmt); err != nil {
			return nil, nil, err
		}
		// insert fast-path may bypass goyacc parser; try collect reductions best-effort.
		reductions, _ := collectReductions(sql)
		return insertStmt, reductions, nil
	}
	scanner := NewScanner(sql)
	reductions, code := parseWithReductionTrace(scanner)
	if code != 0 {
		return nil, nil, scanner.lastErr
	}
	if scanner.lastErr != nil {
		return nil, nil, scanner.lastErr
	}
	if err := validateStatementSemantic(scanner.ParseTree); err != nil {
		return nil, nil, err
	}
	return scanner.ParseTree, reductions, nil
}
