package parsergate

import (
	"fmt"
	"strings"

	"sqlparser"
)

type Result struct {
	Stmt    sqlparser.Statement
	Err     error
	ErrType string
	Rules   []int
}

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
