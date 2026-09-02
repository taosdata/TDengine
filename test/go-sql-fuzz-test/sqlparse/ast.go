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
