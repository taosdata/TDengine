package expr

import (
	"errors"
	"io"
	"math"
	"strconv"
	"strings"
	"text/scanner"
)

var (
	// compile errors
	ErrorExpressionSyntax     = errors.New("expression syntax error")
	ErrorUnrecognizedFunction = errors.New("unrecognized function")
	ErrorArgumentCount        = errors.New("too many/few arguments")
	ErrorInvalidFloat         = errors.New("invalid float")
	ErrorInvalidInteger       = errors.New("invalid integer")

	// eval errors
	ErrorUnsupportedDataType     = errors.New("unsupported data type")
	ErrorInvalidOperationFloat   = errors.New("invalid operation for float")
	ErrorInvalidOperationInteger = errors.New("invalid operation for integer")
	ErrorInvalidOperationBoolean = errors.New("invalid operation for boolean")
	ErrorOnlyIntegerAllowed      = errors.New("only integers is allowed")
	ErrorDataTypeMismatch        = errors.New("data type mismatch")
)

// binary operator precedence
//    5             *   /   %  <<  >>  &
//    4             +   -   |  ^
//    3             ==  !=  <  <=  >   >=
//    2             &&
//    1             ||
const (
	opOr         = -(iota + 1000) // ||
	opAnd                         // &&
	opEqual                       // ==
	opNotEqual                    // !=
	opGTE                         // >=
	opLTE                         // <=
	opLeftShift                   // <<
	opRightShift                  // >>
)

type lexer struct {
	scan scanner.Scanner
	tok  rune
}

func (l *lexer) init(src io.Reader) {
	l.scan.Error = func(s *scanner.Scanner, msg string) {
		panic(errors.New(msg))
	}
	l.scan.Mode = scanner.ScanIdents | scanner.ScanInts | scanner.ScanFloats | scanner.ScanStrings
	l.scan.Init(src)
	l.tok = l.next()
}

func (l *lexer) next() rune {
	l.tok = l.scan.Scan()

	switch l.tok {
	case '|':
		if l.scan.Peek() == '|' {
			l.tok = opOr
			l.scan.Scan()
		}

	case '&':
		if l.scan.Peek() == '&' {
			l.tok = opAnd
			l.scan.Scan()
		}

	case '=':
		if l.scan.Peek() == '=' {
			l.tok = opEqual
			l.scan.Scan()
		} else {
			// TODO: error
		}

	case '!':
		if l.scan.Peek() == '=' {
			l.tok = opNotEqual
			l.scan.Scan()
		} else {
			// TODO: error
		}

	case '<':
		if tok := l.scan.Peek(); tok == '<' {
			l.tok = opLeftShift
			l.scan.Scan()
		} else if tok == '=' {
			l.tok = opLTE
			l.scan.Scan()
		}

	case '>':
		if tok := l.scan.Peek(); tok == '>' {
			l.tok = opRightShift
			l.scan.Scan()
		} else if tok == '=' {
			l.tok = opGTE
			l.scan.Scan()
		}
	}
	return l.tok
}

func (l *lexer) token() rune {
	return l.tok
}

func (l *lexer) text() string {
	switch l.tok {
	case opOr:
		return "||"
	case opAnd:
		return "&&"
	case opEqual:
		return "=="
	case opNotEqual:
		return "!="
	case opLeftShift:
		return "<<"
	case opLTE:
		return "<="
	case opRightShift:
		return ">>"
	case opGTE:
		return ">="
	default:
		return l.scan.TokenText()
	}
}

type Expr interface {
	Eval(env func(string) interface{}) interface{}
}

type unaryExpr struct {
	op      rune
	subExpr Expr
}

func (ue *unaryExpr) Eval(env func(string) interface{}) interface{} {
	val := ue.subExpr.Eval(env)
	switch v := val.(type) {
	case float64:
		if ue.op != '-' {
			panic(ErrorInvalidOperationFloat)
		}
		return -v
	case int64:
		switch ue.op {
		case '-':
			return -v
		case '~':
			return ^v
		default:
			panic(ErrorInvalidOperationInteger)
		}
	case bool:
		if ue.op != '!' {
			panic(ErrorInvalidOperationBoolean)
		}
		return !v
	default:
		panic(ErrorUnsupportedDataType)
	}
}

type binaryExpr struct {
	op  rune
	lhs Expr
	rhs Expr
}

func (be *binaryExpr) Eval(env func(string) interface{}) interface{} {
	lval := be.lhs.Eval(env)
	rval := be.rhs.Eval(env)

	switch be.op {
	case '*':
		switch lv := lval.(type) {
		case float64:
			switch rv := rval.(type) {
			case float64:
				return lv * rv
			case int64:
				return lv * float64(rv)
			case bool:
				panic(ErrorInvalidOperationBoolean)
			}
		case int64:
			switch rv := rval.(type) {
			case float64:
				return float64(lv) * rv
			case int64:
				return lv * rv
			case bool:
				panic(ErrorInvalidOperationBoolean)
			}
		case bool:
			panic(ErrorInvalidOperationBoolean)
		}

	case '/':
		switch lv := lval.(type) {
		case float64:
			switch rv := rval.(type) {
			case float64:
				if rv == 0 {
					return math.Inf(int(lv))
				} else {
					return lv / rv
				}
			case int64:
				if rv == 0 {
					return math.Inf(int(lv))
				} else {
					return lv / float64(rv)
				}
			case bool:
				panic(ErrorInvalidOperationBoolean)
			}
		case int64:
			switch rv := rval.(type) {
			case float64:
				if rv == 0 {
					return math.Inf(int(lv))
				} else {
					return float64(lv) / rv
				}
			case int64:
				if rv == 0 {
					return math.Inf(int(lv))
				} else {
					return lv / rv
				}
			case bool:
				panic(ErrorInvalidOperationBoolean)
			}
		case bool:
			panic(ErrorInvalidOperationBoolean)
		}

	case '%':
		switch lv := lval.(type) {
		case float64:
			switch rv := rval.(type) {
			case float64:
				return math.Mod(lv, rv)
			case int64:
				return math.Mod(lv, float64(rv))
			case bool:
				panic(ErrorInvalidOperationBoolean)
			}
		case int64:
			switch rv := rval.(type) {
			case float64:
				return math.Mod(float64(lv), rv)
			case int64:
				if rv == 0 {
					return math.Inf(int(lv))
				} else {
					return lv % rv
				}
			case bool:
				panic(ErrorInvalidOperationBoolean)
			}
		case bool:
			panic(ErrorInvalidOperationBoolean)
		}

	case opLeftShift:
		switch lv := lval.(type) {
		case int64:
			switch rv := rval.(type) {
			case int64:
				return lv << rv
			default:
				panic(ErrorOnlyIntegerAllowed)
			}
		default:
			panic(ErrorOnlyIntegerAllowed)
		}

	case opRightShift:
		switch lv := lval.(type) {
		case int64:
			switch rv := rval.(type) {
			case int64:
				return lv >> rv
			default:
				panic(ErrorOnlyIntegerAllowed)
			}
		default:
			panic(ErrorOnlyIntegerAllowed)
		}

	case '&':
		switch lv := lval.(type) {
		case int64:
			switch rv := rval.(type) {
			case int64:
				return lv & rv
			default:
				panic(ErrorOnlyIntegerAllowed)
			}
		default:
			panic(ErrorOnlyIntegerAllowed)
		}

	case '+':
		switch lv := lval.(type) {
		case float64:
			switch rv := rval.(type) {
			case float64:
				return lv + rv
			case int64:
				return lv + float64(rv)
			case bool:
				panic(ErrorInvalidOperationBoolean)
			}
		case int64:
			switch rv := rval.(type) {
			case float64:
				return float64(lv) + rv
			case int64:
				return lv + rv
			case bool:
				panic(ErrorInvalidOperationBoolean)
			}
		case bool:
			panic(ErrorInvalidOperationBoolean)
		}

	case '-':
		switch lv := lval.(type) {
		case float64:
			switch rv := rval.(type) {
			case float64:
				return lv - rv
			case int64:
				return lv - float64(rv)
			case bool:
				panic(ErrorInvalidOperationBoolean)
			}
		case int64:
			switch rv := rval.(type) {
			case float64:
				return float64(lv) - rv
			case int64:
				return lv - rv
			case bool:
				panic(ErrorInvalidOperationBoolean)
			}
		case bool:
			panic(ErrorInvalidOperationBoolean)
		}

	case '|':
		switch lv := lval.(type) {
		case int64:
			switch rv := rval.(type) {
			case int64:
				return lv | rv
			default:
				panic(ErrorOnlyIntegerAllowed)
			}
		default:
			panic(ErrorOnlyIntegerAllowed)
		}

	case '^':
		switch lv := lval.(type) {
		case int64:
			switch rv := rval.(type) {
			case int64:
				return lv ^ rv
			default:
				panic(ErrorOnlyIntegerAllowed)
			}
		default:
			panic(ErrorOnlyIntegerAllowed)
		}

	case opEqual:
		switch lv := lval.(type) {
		case float64:
			switch rv := rval.(type) {
			case float64:
				return lv == rv
			case int64:
				return lv == float64(rv)
			case bool:
				panic(ErrorDataTypeMismatch)
			}
		case int64:
			switch rv := rval.(type) {
			case float64:
				return float64(lv) == rv
			case int64:
				return lv == rv
			case bool:
				panic(ErrorDataTypeMismatch)
			}
		case bool:
			switch rv := rval.(type) {
			case float64:
			case int64:
			case bool:
				return lv == rv
			}
		}

	case opNotEqual:
		switch lv := lval.(type) {
		case float64:
			switch rv := rval.(type) {
			case float64:
				return lv != rv
			case int64:
				return lv != float64(rv)
			case bool:
				panic(ErrorDataTypeMismatch)
			}
		case int64:
			switch rv := rval.(type) {
			case float64:
				return float64(lv) != rv
			case int64:
				return lv != rv
			case bool:
				panic(ErrorDataTypeMismatch)
			}
		case bool:
			switch rv := rval.(type) {
			case float64:
			case int64:
			case bool:
				return lv != rv
			}
		}

	case '<':
		switch lv := lval.(type) {
		case float64:
			switch rv := rval.(type) {
			case float64:
				return lv < rv
			case int64:
				return lv < float64(rv)
			case bool:
				panic(ErrorDataTypeMismatch)
			}
		case int64:
			switch rv := rval.(type) {
			case float64:
				return float64(lv) < rv
			case int64:
				return lv < rv
			case bool:
				panic(ErrorDataTypeMismatch)
			}
		case bool:
			panic(ErrorInvalidOperationBoolean)
		}

	case opLTE:
		switch lv := lval.(type) {
		case float64:
			switch rv := rval.(type) {
			case float64:
				return lv <= rv
			case int64:
				return lv <= float64(rv)
			case bool:
				panic(ErrorDataTypeMismatch)
			}
		case int64:
			switch rv := rval.(type) {
			case float64:
				return float64(lv) <= rv
			case int64:
				return lv <= rv
			case bool:
				panic(ErrorDataTypeMismatch)
			}
		case bool:
			panic(ErrorInvalidOperationBoolean)
		}

	case '>':
		switch lv := lval.(type) {
		case float64:
			switch rv := rval.(type) {
			case float64:
				return lv > rv
			case int64:
				return lv > float64(rv)
			case bool:
				panic(ErrorDataTypeMismatch)
			}
		case int64:
			switch rv := rval.(type) {
			case float64:
				return float64(lv) > rv
			case int64:
				return lv > rv
			case bool:
				panic(ErrorDataTypeMismatch)
			}
		case bool:
			panic(ErrorInvalidOperationBoolean)
		}

	case opGTE:
		switch lv := lval.(type) {
		case float64:
			switch rv := rval.(type) {
			case float64:
				return lv >= rv
			case int64:
				return lv >= float64(rv)
			case bool:
				panic(ErrorDataTypeMismatch)
			}
		case int64:
			switch rv := rval.(type) {
			case float64:
				return float64(lv) >= rv
			case int64:
				return lv >= rv
			case bool:
				panic(ErrorDataTypeMismatch)
			}
		case bool:
			panic(ErrorInvalidOperationBoolean)
		}

	case opAnd:
		switch lv := lval.(type) {
		case bool:
			switch rv := rval.(type) {
			case bool:
				return lv && rv
			default:
				panic(ErrorOnlyIntegerAllowed)
			}
		default:
			panic(ErrorOnlyIntegerAllowed)
		}

	case opOr:
		switch lv := lval.(type) {
		case bool:
			switch rv := rval.(type) {
			case bool:
				return lv || rv
			default:
				panic(ErrorOnlyIntegerAllowed)
			}
		default:
			panic(ErrorOnlyIntegerAllowed)
		}
	}

	return nil
}

type funcExpr struct {
	name string
	args []Expr
}

func (fe *funcExpr) Eval(env func(string) interface{}) interface{} {
	argv := make([]interface{}, 0, len(fe.args))
	for _, arg := range fe.args {
		argv = append(argv, arg.Eval(env))
	}
	return funcs[fe.name].call(argv)
}

type floatExpr struct {
	val float64
}

func (fe *floatExpr) Eval(env func(string) interface{}) interface{} {
	return fe.val
}

type intExpr struct {
	val int64
}

func (ie *intExpr) Eval(env func(string) interface{}) interface{} {
	return ie.val
}

type boolExpr struct {
	val bool
}

func (be *boolExpr) Eval(env func(string) interface{}) interface{} {
	return be.val
}

type stringExpr struct {
	val string
}

func (se *stringExpr) Eval(env func(string) interface{}) interface{} {
	return se.val
}

type varExpr struct {
	name string
}

func (ve *varExpr) Eval(env func(string) interface{}) interface{} {
	return env(ve.name)
}

func Compile(src string) (expr Expr, err error) {
	defer func() {
		switch x := recover().(type) {
		case nil:
		case error:
			err = x
		default:
		}
	}()

	lexer := lexer{}
	lexer.init(strings.NewReader(src))
	expr = parseBinary(&lexer, 0)
	if lexer.token() != scanner.EOF {
		panic(ErrorExpressionSyntax)
	}
	return expr, nil
}

func precedence(op rune) int {
	switch op {
	case opOr:
		return 1
	case opAnd:
		return 2
	case opEqual, opNotEqual, '<', '>', opGTE, opLTE:
		return 3
	case '+', '-', '|', '^':
		return 4
	case '*', '/', '%', opLeftShift, opRightShift, '&':
		return 5
	}
	return 0
}

// binary = unary ('+' binary)*
func parseBinary(lexer *lexer, lastPrec int) Expr {
	lhs := parseUnary(lexer)

	for {
		op := lexer.token()
		prec := precedence(op)
		if prec <= lastPrec {
			break
		}
		lexer.next() // consume operator
		rhs := parseBinary(lexer, prec)
		lhs = &binaryExpr{op: op, lhs: lhs, rhs: rhs}
	}

	return lhs
}

// unary = '+|-' expr | primary
func parseUnary(lexer *lexer) Expr {
	flag := false
	for tok := lexer.token(); ; tok = lexer.next() {
		if tok == '-' {
			flag = !flag
		} else if tok != '+' {
			break
		}
	}
	if flag {
		return &unaryExpr{op: '-', subExpr: parsePrimary(lexer)}
	}

	flag = false
	for tok := lexer.token(); tok == '!'; tok = lexer.next() {
		flag = !flag
	}
	if flag {
		return &unaryExpr{op: '!', subExpr: parsePrimary(lexer)}
	}

	flag = false
	for tok := lexer.token(); tok == '~'; tok = lexer.next() {
		flag = !flag
	}
	if flag {
		return &unaryExpr{op: '~', subExpr: parsePrimary(lexer)}
	}

	return parsePrimary(lexer)
}

// primary = id
//         | id '(' expr ',' ... ',' expr ')'
//         | num
//         | '(' expr ')'
func parsePrimary(lexer *lexer) Expr {
	switch lexer.token() {
	case '+', '-', '!', '~':
		return parseUnary(lexer)

	case '(':
		lexer.next() // consume '('
		node := parseBinary(lexer, 0)
		if lexer.token() != ')' {
			panic(ErrorExpressionSyntax)
		}
		lexer.next() // consume ')'
		return node

	case scanner.Ident:
		id := strings.ToLower(lexer.text())
		if lexer.next() != '(' {
			if id == "true" {
				return &boolExpr{val: true}
			} else if id == "false" {
				return &boolExpr{val: false}
			} else {
				return &varExpr{name: id}
			}
		}
		node := funcExpr{name: id}
		for lexer.next() != ')' {
			arg := parseBinary(lexer, 0)
			node.args = append(node.args, arg)
			if lexer.token() != ',' {
				break
			}
		}
		if lexer.token() != ')' {
			panic(ErrorExpressionSyntax)
		}

		if fn, ok := funcs[id]; !ok {
			panic(ErrorUnrecognizedFunction)
		} else if fn.minArgs >= 0 && len(node.args) < fn.minArgs {
			panic(ErrorArgumentCount)
		} else if fn.maxArgs >= 0 && len(node.args) > fn.maxArgs {
			panic(ErrorArgumentCount)
		}

		lexer.next() // consume it
		return &node

	case scanner.Int:
		val, e := strconv.ParseInt(lexer.text(), 0, 64)
		if e != nil {
			panic(ErrorInvalidFloat)
		}
		lexer.next()
		return &intExpr{val: val}

	case scanner.Float:
		val, e := strconv.ParseFloat(lexer.text(), 0)
		if e != nil {
			panic(ErrorInvalidInteger)
		}
		lexer.next()
		return &floatExpr{val: val}

	case scanner.String:
		panic(errors.New("strings are not allowed in expression at present"))
		val := lexer.text()
		lexer.next()
		return &stringExpr{val: val}

	default:
		panic(ErrorExpressionSyntax)
	}
}
