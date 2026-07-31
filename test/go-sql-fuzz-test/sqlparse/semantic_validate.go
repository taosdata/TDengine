package sqlparser

import (
	"fmt"
	"strconv"
	"strings"
)

func validateStatementSemantic(stmt Statement) error {
	if stmt == nil {
		return nil
	}
	return Walk(func(node SQLNode) (bool, error) {
		switch n := node.(type) {
		case *SelectStmt:
			if err := validateSelectSemantic(n); err != nil {
				return false, err
			}
		case *RawExpr:
			if err := validateRawExprSemantic(n); err != nil {
				return false, err
			}
		}
		return true, nil
	}, stmt)
}

func validateRawExprSemantic(r *RawExpr) error {
	if r == nil {
		return nil
	}
	if r.Kind != "func" {
		return nil
	}
	name := strings.ToLower(strings.TrimSpace(r.Name))
	if name == "" {
		return nil
	}
	if err := validateFunctionArity(name, r.Args); err != nil {
		return err
	}
	if err := validateFunctionSemantics(name, r.Args); err != nil {
		return err
	}
	return nil
}

func validateSelectSemantic(s *SelectStmt) error {
	if s == nil {
		return nil
	}
	if s.GroupBy == nil || len(s.GroupBy.Exprs) == 0 {
		return nil
	}

	groupExprs := make(map[string]struct{}, len(s.GroupBy.Exprs))
	for _, g := range s.GroupBy.Exprs {
		norm := normalizeExprSQL(baseExpr(g))
		if norm != "" {
			groupExprs[norm] = struct{}{}
		}
	}

	selectExprs := make(map[string]struct{}, len(s.Select))
	for _, sel := range s.Select {
		e := baseExpr(sel)
		if e == nil {
			continue
		}
		if _, isStar := e.(*StarExpr); isStar {
			return semanticErr("select * is not allowed with group by")
		}
		norm := normalizeExprSQL(e)
		if norm != "" {
			selectExprs[norm] = struct{}{}
		}
		if exprHasAggregate(e) {
			continue
		}
		if !exprHasColumnRef(e) {
			continue
		}
		if _, ok := groupExprs[norm]; !ok {
			return semanticErr("select expression %q must appear in group by or be aggregated", SQLNodeToString(e))
		}
	}

	for _, ob := range s.OrderBy {
		e := baseExpr(ob.Expr)
		if e == nil {
			continue
		}
		if isPositiveIntLiteralExpr(e) {
			continue
		}
		if exprHasAggregate(e) {
			continue
		}
		if !exprHasColumnRef(e) {
			continue
		}
		norm := normalizeExprSQL(e)
		if _, ok := groupExprs[norm]; ok {
			continue
		}
		if _, ok := selectExprs[norm]; ok {
			continue
		}
		return semanticErr("order by expression %q must appear in group by/select list", SQLNodeToString(e))
	}

	return nil
}

func validateFunctionArity(name string, args []Expr) error {
	argc := len(args)
	switch name {
	case "abs", "ceil", "floor", "length", "lower", "upper", "sum", "avg", "min", "max":
		if argc != 1 {
			return semanticErr("%s expects exactly 1 argument, got %d", name, argc)
		}
	case "pow", "position":
		if argc != 2 {
			return semanticErr("%s expects exactly 2 arguments, got %d", name, argc)
		}
	case "replace":
		if argc != 3 {
			return semanticErr("%s expects exactly 3 arguments, got %d", name, argc)
		}
	case "round":
		if argc < 1 || argc > 2 {
			return semanticErr("%s expects 1..2 arguments, got %d", name, argc)
		}
	case "concat":
		if argc < 2 {
			return semanticErr("%s expects at least 2 arguments, got %d", name, argc)
		}
	case "substr", "substring":
		if argc < 2 || argc > 3 {
			return semanticErr("%s expects 2..3 arguments, got %d", name, argc)
		}
	case "rand":
		if argc > 1 {
			return semanticErr("%s expects 0..1 arguments, got %d", name, argc)
		}
	case "count":
		if argc > 1 {
			return semanticErr("%s expects 0..1 arguments, got %d", name, argc)
		}
	case "cols":
		if argc < 2 {
			return semanticErr("%s expects at least 2 arguments, got %d", name, argc)
		}
	}
	return nil
}

func validateFunctionSemantics(name string, args []Expr) error {
	requireArgKind := func(pos int, want staticKind) error {
		if pos < 0 || pos >= len(args) {
			return nil
		}
		got := inferStaticKind(args[pos])
		if got == kindUnknown || got == want {
			return nil
		}
		return semanticErr("%s argument #%d expects %s type", name, pos+1, want.String())
	}

	requireAllArgKind := func(want staticKind) error {
		for i := range args {
			if err := requireArgKind(i, want); err != nil {
				return err
			}
		}
		return nil
	}

	switch name {
	case "abs", "ceil", "floor", "sum", "avg", "min", "max", "rand":
		return requireAllArgKind(kindNumber)
	case "round":
		if err := requireArgKind(0, kindNumber); err != nil {
			return err
		}
		if len(args) > 1 {
			if err := requireArgKind(1, kindNumber); err != nil {
				return err
			}
		}
	case "pow":
		if err := requireArgKind(0, kindNumber); err != nil {
			return err
		}
		if err := requireArgKind(1, kindNumber); err != nil {
			return err
		}
	case "length", "lower", "upper":
		return requireArgKind(0, kindString)
	case "concat":
		return requireAllArgKind(kindString)
	case "position":
		if err := requireArgKind(0, kindString); err != nil {
			return err
		}
		if err := requireArgKind(1, kindString); err != nil {
			return err
		}
	case "replace":
		if err := requireArgKind(0, kindString); err != nil {
			return err
		}
		if err := requireArgKind(1, kindString); err != nil {
			return err
		}
		if err := requireArgKind(2, kindString); err != nil {
			return err
		}
	case "substr", "substring":
		if err := requireArgKind(0, kindString); err != nil {
			return err
		}
		if err := requireArgKind(1, kindNumber); err != nil {
			return err
		}
		if len(args) > 2 {
			if err := requireArgKind(2, kindNumber); err != nil {
				return err
			}
		}
	case "cols":
		if _, ok := args[0].(*AliasedExpr); ok {
			return semanticErr("cols first argument cannot use alias")
		}
		first := baseExpr(args[0])
		rf, ok := first.(*RawExpr)
		if !ok || rf == nil || rf.Kind != "func" || !isAggregateFunction(rf.Name) {
			return semanticErr("cols first argument must be an aggregate function")
		}
		for i := 1; i < len(args); i++ {
			if _, ok := args[i].(*AliasedExpr); ok {
				return semanticErr("cols argument #%d cannot use alias", i+1)
			}
		}
	}
	return nil
}

func semanticErr(format string, args ...any) error {
	return fmt.Errorf("semantic error: "+format, args...)
}

func normalizeExprSQL(e Expr) string {
	if e == nil {
		return ""
	}
	s := strings.ToLower(strings.TrimSpace(SQLNodeToString(e)))
	if s == "" {
		return ""
	}
	return strings.Join(strings.Fields(s), " ")
}

func baseExpr(e Expr) Expr {
	for {
		if e == nil {
			return nil
		}
		a, ok := e.(*AliasedExpr)
		if !ok || a == nil {
			return e
		}
		e = a.Expr
	}
}

func exprHasAggregate(e Expr) bool {
	e = baseExpr(e)
	if e == nil {
		return false
	}
	hit := false
	_ = Walk(func(node SQLNode) (bool, error) {
		r, ok := node.(*RawExpr)
		if !ok || r == nil || r.Kind != "func" {
			return true, nil
		}
		if isAggregateFunction(r.Name) {
			hit = true
			return false, nil
		}
		return true, nil
	}, e)
	return hit
}

func exprHasColumnRef(e Expr) bool {
	e = baseExpr(e)
	if e == nil {
		return false
	}
	hit := false
	_ = Walk(func(node SQLNode) (bool, error) {
		switch x := node.(type) {
		case *RawExpr:
			if x != nil && (x.Kind == "col" || x.Kind == "pseudo_col") {
				hit = true
				return false, nil
			}
		case *StarExpr:
			hit = true
			return false, nil
		}
		return true, nil
	}, e)
	return hit
}

func isPositiveIntLiteralExpr(e Expr) bool {
	e = baseExpr(e)
	if e == nil {
		return false
	}
	switch x := e.(type) {
	case Literal:
		if x.Type != LiteralInt {
			return false
		}
		n, err := strconv.Atoi(string(x.Val.Bytes))
		return err == nil && n > 0
	case *SQLVal:
		if x == nil || x.Type != IntVal {
			return false
		}
		n, err := strconv.Atoi(string(x.Val))
		return err == nil && n > 0
	}
	txt := strings.TrimSpace(SQLNodeToString(e))
	if txt == "" {
		return false
	}
	n, err := strconv.Atoi(txt)
	return err == nil && n > 0
}

type staticKind int

const (
	kindUnknown staticKind = iota
	kindNumber
	kindString
	kindBool
	kindTime
)

func (k staticKind) String() string {
	switch k {
	case kindNumber:
		return "numeric"
	case kindString:
		return "string"
	case kindBool:
		return "boolean"
	case kindTime:
		return "time"
	default:
		return "unknown"
	}
}

func inferStaticKind(e Expr) staticKind {
	e = baseExpr(e)
	if e == nil {
		return kindUnknown
	}
	switch x := e.(type) {
	case Literal:
		switch x.Type {
		case LiteralInt, LiteralFloat:
			return kindNumber
		case LiteralString:
			return kindString
		case LiteralBool:
			return kindBool
		case LiteralDuration:
			return kindTime
		default:
			return kindUnknown
		}
	case *SQLVal:
		if x == nil {
			return kindUnknown
		}
		switch x.Type {
		case IntVal, FloatVal, HexNum:
			return kindNumber
		case StrVal:
			return kindString
		case TimeVal, DurationVal:
			return kindTime
		default:
			return kindUnknown
		}
	case BoolVal:
		return kindBool
	case *RawExpr:
		if x == nil {
			return kindUnknown
		}
		switch x.Kind {
		case "unary":
			op := strings.ToLower(strings.TrimSpace(string(x.Op.Bytes)))
			if op == "not" {
				return kindBool
			}
			if op == "minus" || op == "uplus" {
				return kindNumber
			}
		case "binary":
			op := strings.ToLower(strings.TrimSpace(string(x.Op.Bytes)))
			switch op {
			case "add", "sub", "mul", "div", "rem", "bit_and", "bit_or":
				return kindNumber
			case "and", "or", "lt", "gt", "le", "ge", "ne", "eq", "like", "not_like", "match", "nmatch", "regexp", "not_regexp", "contains", "in", "not_in":
				return kindBool
			}
		case "cmp", "between", "is_null", "in", "in_subquery":
			return kindBool
		case "cast":
			t := strings.ToLower(strings.TrimSpace(x.Type))
			switch {
			case strings.Contains(t, "char"), strings.Contains(t, "binary"), strings.Contains(t, "text"):
				return kindString
			case strings.Contains(t, "bool"):
				return kindBool
			case strings.Contains(t, "time"), strings.Contains(t, "date"):
				return kindTime
			case strings.Contains(t, "int"), strings.Contains(t, "float"), strings.Contains(t, "double"), strings.Contains(t, "decimal"), strings.Contains(t, "numeric"):
				return kindNumber
			}
		case "func":
			return inferFuncResultKind(x.Name)
		}
	}
	return kindUnknown
}

func inferFuncResultKind(name string) staticKind {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "abs", "ceil", "floor", "round", "pow", "sum", "avg", "min", "max", "count", "length", "position", "rand":
		return kindNumber
	case "lower", "upper", "concat", "replace", "substr", "substring", "trim":
		return kindString
	case "isnull", "isnotnull":
		return kindBool
	case "now", "today":
		return kindTime
	default:
		return kindUnknown
	}
}

func isAggregateFunction(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "count", "sum", "avg", "min", "max":
		return true
	default:
		return false
	}
}

func hasDoubleDashOutsideLiteral(sql string) bool {
	inSingle := false
	inDouble := false
	inBacktick := false
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		if inSingle {
			if ch == '\\' && i+1 < len(sql) {
				i++
				continue
			}
			if ch == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					i++
					continue
				}
				inSingle = false
			}
			continue
		}
		if inDouble {
			if ch == '\\' && i+1 < len(sql) {
				i++
				continue
			}
			if ch == '"' {
				inDouble = false
			}
			continue
		}
		if inBacktick {
			if ch == '`' {
				inBacktick = false
			}
			continue
		}

		if ch == '\'' {
			inSingle = true
			continue
		}
		if ch == '"' {
			inDouble = true
			continue
		}
		if ch == '`' {
			inBacktick = true
			continue
		}
		if ch == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			i += 2
			for i < len(sql)-1 {
				if sql[i] == '*' && sql[i+1] == '/' {
					i++
					break
				}
				i++
			}
			continue
		}
		if ch == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			return true
		}
	}
	return false
}
