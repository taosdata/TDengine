package sqlparser

import (
	"strings"
)

type CreateEncryptKeyStmt = AlterDnodeStmt
type CreateAlgrStmt = CreateEncryptAlgrStmt
type DropAlgrStmt = DropEncryptAlgrStmt

type TableExpr interface {
	SQLNode
	iTableExpr()
}

type ColumnExpr = string

type JoinType int

const (
	JoinTypeInner JoinType = iota + 1
	JoinTypeLeft
	JoinTypeRight
	JoinTypeFull
	JoinTypeLeftSemi
	JoinTypeRightSemi
	JoinTypeLeftAnti
	JoinTypeRightAnti
	JoinTypeLeftAsof
	JoinTypeRightAsof
	JoinTypeLeftWindow
	JoinTypeRightWindow
)

type OrderByExpr struct {
	Expr       Expr
	Asc        bool
	NullsFirst bool
}

func (o *OrderByExpr) Format(buf *TrackedBuffer) {
	if o == nil {
		return
	}
	if o.Expr != nil {
		buf.Myprintf("%v", o.Expr)
	}
	if o.Asc {
		buf.Myprintf(" asc")
	} else {
		buf.Myprintf(" desc")
	}
	if o.NullsFirst {
		buf.Myprintf(" nulls first")
	} else {
		buf.Myprintf(" nulls last")
	}
}

func (o *OrderByExpr) walkSubtree(visit Visit) error {
	if o == nil {
		return nil
	}
	return Walk(visit, o.Expr)
}

type GroupByExpr struct {
	Exprs []Expr
}

func (g *GroupByExpr) Format(buf *TrackedBuffer) {
	if g == nil {
		return
	}
	for i, expr := range g.Exprs {
		if i > 0 {
			buf.Myprintf(", ")
		}
		buf.Myprintf("%v", expr)
	}
}

func (g *GroupByExpr) walkSubtree(visit Visit) error {
	if g == nil {
		return nil
	}
	for _, expr := range g.Exprs {
		if err := Walk(visit, expr); err != nil {
			return err
		}
	}
	return nil
}

type LimitExpr struct {
	Limit   Token
	Offset  Token
	SLimit  Token
	SOffset Token
}

func (l *LimitExpr) Format(buf *TrackedBuffer) {
	if l == nil {
		return
	}
	if len(l.SLimit.Bytes) > 0 {
		buf.Myprintf("slimit %s", l.SLimit.Bytes)
		if len(l.SOffset.Bytes) > 0 {
			buf.Myprintf(" soffset %s", l.SOffset.Bytes)
		}
		return
	}
	if len(l.Limit.Bytes) > 0 {
		buf.Myprintf("limit %s", l.Limit.Bytes)
	}
	if len(l.Offset.Bytes) > 0 {
		buf.Myprintf(" offset %s", l.Offset.Bytes)
	}
}

func (l *LimitExpr) walkSubtree(visit Visit) error {
	return nil
}

type LiteralType int

const (
	LiteralInt LiteralType = iota + 1
	LiteralFloat
	LiteralString
	LiteralBool
	LiteralHex
	LiteralNull
	LiteralDuration
)

type Literal struct {
	Val  Token
	Type LiteralType
}

type WindowExpr struct {
	Interval         Literal
	Offset           Literal
	Sliding          Literal
	Fill             *FillExpr
	Session          Expr
	SessionGap       Literal
	StateWindow      Expr
	StateWindowOpt   StateWindowOpt
	TrueFor          Literal
	EventWindowStart Expr
	EventWindowEnd   Expr
	CountWindow      Token
	CountWindowSlide Token
	CountWindowCols  []ColumnExpr
	AnomalyWindow    Expr
	AnomalyTag       Token
}

func (w *WindowExpr) isEmpty() bool {
	if w == nil {
		return true
	}
	return len(w.Interval.Val.Bytes) == 0 &&
		w.Session == nil &&
		w.StateWindow == nil &&
		len(w.CountWindow.Bytes) == 0 &&
		w.EventWindowStart == nil &&
		w.EventWindowEnd == nil &&
		w.AnomalyWindow == nil
}

func (w *WindowExpr) Format(buf *TrackedBuffer) {
	if w == nil {
		return
	}
	if w.StateWindow != nil {
		buf.Myprintf("state_window(%v", w.StateWindow)
		if w.StateWindowOpt.HasExtend {
			buf.Myprintf(", %v", w.StateWindowOpt.Extend)
		}
		if w.StateWindowOpt.HasZeroth {
			buf.Myprintf(", %v", w.StateWindowOpt.Zeroth)
		}
		buf.Myprintf(")")
		if len(w.TrueFor.Val.Bytes) > 0 {
			buf.Myprintf(" true_for(%v)", w.TrueFor)
		}
		return
	}
	if w.Session != nil {
		buf.Myprintf("session(%v", w.Session)
		if len(w.SessionGap.Val.Bytes) > 0 {
			buf.Myprintf(", %v", w.SessionGap)
		}
		buf.Myprintf(")")
		return
	}
	if len(w.Interval.Val.Bytes) > 0 {
		buf.Myprintf("interval(%v", w.Interval)
		if len(w.Offset.Val.Bytes) > 0 {
			buf.Myprintf(", %v", w.Offset)
		}
		buf.Myprintf(")")
	}
	if len(w.Sliding.Val.Bytes) > 0 {
		buf.Myprintf(" sliding(%v)", w.Sliding)
	}
	if w.Fill != nil {
		buf.Myprintf(" fill(%v)", w.Fill)
	}
	if w.EventWindowStart != nil || w.EventWindowEnd != nil {
		buf.Myprintf(" event_window")
		if w.EventWindowStart != nil {
			buf.Myprintf(" start with %v", w.EventWindowStart)
		}
		if w.EventWindowEnd != nil {
			buf.Myprintf(" end with %v", w.EventWindowEnd)
		}
		if len(w.TrueFor.Val.Bytes) > 0 {
			buf.Myprintf(" true_for(%v)", w.TrueFor)
		}
	}
	if len(w.CountWindow.Bytes) > 0 {
		buf.Myprintf(" count_window(%s", w.CountWindow.Bytes)
		if len(w.CountWindowSlide.Bytes) > 0 {
			buf.Myprintf(", %s", w.CountWindowSlide.Bytes)
		}
		for _, c := range w.CountWindowCols {
			buf.Myprintf(", %s", c)
		}
		buf.Myprintf(")")
	}
	if w.AnomalyWindow != nil {
		buf.Myprintf(" anomaly_window(%v", w.AnomalyWindow)
		if len(w.AnomalyTag.Bytes) > 0 {
			buf.Myprintf(", '%s'", w.AnomalyTag.Bytes)
		}
		buf.Myprintf(")")
	}
}

func (w *WindowExpr) walkSubtree(visit Visit) error {
	if w == nil {
		return nil
	}
	if err := Walk(
		visit,
		w.Session,
		w.StateWindow,
		w.EventWindowStart,
		w.EventWindowEnd,
		w.AnomalyWindow,
		w.Fill,
	); err != nil {
		return err
	}
	for _, col := range w.CountWindowCols {
		_ = col
	}
	return nil
}

type CountWindowArgs struct {
	Count Token
	Slide Token
	Cols  []ColumnExpr
}

type StateWindowOpt struct {
	HasExtend bool
	Extend    Literal
	HasZeroth bool
	Zeroth    Literal
}

type FillExpr struct {
	Name   string
	Mode   *FillExpr
	Values []Expr
}

func (f *FillExpr) Format(buf *TrackedBuffer) {
	if f == nil {
		return
	}
	if f.Mode != nil && f.Mode.Name != "" {
		buf.Myprintf("%s", f.Mode.Name)
	} else {
		buf.Myprintf("%s", f.Name)
	}
	for i, expr := range f.Values {
		if i == 0 {
			buf.Myprintf(", ")
		} else {
			buf.Myprintf(", ")
		}
		buf.Myprintf("%v", expr)
	}
}

func (f *FillExpr) walkSubtree(visit Visit) error {
	if f == nil {
		return nil
	}
	if err := Walk(visit, f.Mode); err != nil {
		return err
	}
	for _, expr := range f.Values {
		if err := Walk(visit, expr); err != nil {
			return err
		}
	}
	return nil
}

var (
	FILL_MODE_NONE    = &FillExpr{Name: "none"}
	FILL_MODE_NULL    = &FillExpr{Name: "null"}
	FILL_MODE_NULL_F  = &FillExpr{Name: "null_f"}
	FILL_MODE_LINEAR  = &FillExpr{Name: "linear"}
	FILL_MODE_PREV    = &FillExpr{Name: "prev"}
	FILL_MODE_NEXT    = &FillExpr{Name: "next"}
	FILL_MODE_VALUE   = &FillExpr{Name: "value"}
	FILL_MODE_VALUE_F = &FillExpr{Name: "value_f"}
	FILL_MODE_NEAR    = &FillExpr{Name: "near"}
)

type WhenThenExpr struct {
	When Expr
	Then Expr
}

type TableNameExpr struct {
	DBName    string
	TableName string
	Alias     string
}

func (*TableNameExpr) iTableExpr() {}
func (t *TableNameExpr) Format(buf *TrackedBuffer) {
	if t == nil {
		return
	}
	if t.DBName != "" {
		buf.Myprintf("%s.", t.DBName)
	}
	buf.Myprintf("%s", t.TableName)
	if t.Alias != "" {
		buf.Myprintf(" as %s", t.Alias)
	}
}
func (t *TableNameExpr) walkSubtree(visit Visit) error {
	return nil
}

type SubqueryTableExpr struct {
	Query *SelectStmt
	Alias string
}

func (*SubqueryTableExpr) iTableExpr() {}
func (s *SubqueryTableExpr) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("(")
	if s.Query != nil {
		buf.Myprintf("%v", s.Query)
	}
	buf.Myprintf(")")
	if s.Alias != "" {
		buf.Myprintf(" as %s", s.Alias)
	}
}
func (s *SubqueryTableExpr) walkSubtree(visit Visit) error {
	if s == nil {
		return nil
	}
	return Walk(visit, s.Query)
}

type ParenthesizedTableExpr struct {
	Inner TableExpr
}

func (*ParenthesizedTableExpr) iTableExpr() {}
func (p *ParenthesizedTableExpr) Format(buf *TrackedBuffer) {
	if p == nil {
		return
	}
	buf.Myprintf("(")
	if p.Inner != nil {
		buf.Myprintf("%v", p.Inner)
	}
	buf.Myprintf(")")
}
func (p *ParenthesizedTableExpr) walkSubtree(visit Visit) error {
	if p == nil {
		return nil
	}
	return Walk(visit, p.Inner)
}

type JoinTableExpr struct {
	Left         TableExpr
	Right        TableExpr
	JoinType     JoinType
	Condition    interface{}
	WindowOffset Expr
	JLimit       *LimitExpr
}

func (*JoinTableExpr) iTableExpr() {}
func (j *JoinTableExpr) Format(buf *TrackedBuffer) {
	if j == nil {
		return
	}
	if j.Left != nil {
		buf.Myprintf("%v", j.Left)
	}
	switch j.JoinType {
	case JoinTypeLeft:
		buf.Myprintf(" left join ")
	case JoinTypeRight:
		buf.Myprintf(" right join ")
	case JoinTypeFull:
		buf.Myprintf(" full join ")
	case JoinTypeLeftSemi:
		buf.Myprintf(" left semi join ")
	case JoinTypeRightSemi:
		buf.Myprintf(" right semi join ")
	case JoinTypeLeftAnti:
		buf.Myprintf(" left anti join ")
	case JoinTypeRightAnti:
		buf.Myprintf(" right anti join ")
	case JoinTypeLeftAsof:
		buf.Myprintf(" left asof join ")
	case JoinTypeRightAsof:
		buf.Myprintf(" right asof join ")
	case JoinTypeLeftWindow:
		buf.Myprintf(" left window join ")
	case JoinTypeRightWindow:
		buf.Myprintf(" right window join ")
	default:
		buf.Myprintf(" join ")
	}
	if j.Right != nil {
		buf.Myprintf("%v", j.Right)
	}
	if condExpr, ok := j.Condition.(Expr); ok && condExpr != nil {
		buf.Myprintf(" on %v", condExpr)
	}
	if j.WindowOffset != nil {
		buf.Myprintf(" window_offset %v", j.WindowOffset)
	}
	if j.JLimit != nil {
		buf.Myprintf(" j%v", j.JLimit)
	}
}
func (j *JoinTableExpr) walkSubtree(visit Visit) error {
	if j == nil {
		return nil
	}
	if err := Walk(visit, j.Left, j.Right, j.WindowOffset); err != nil {
		return err
	}
	if condExpr, ok := j.Condition.(Expr); ok {
		if err := Walk(visit, condExpr); err != nil {
			return err
		}
	}
	return nil
}

type RawExpr struct {
	Kind  string
	Op    Token
	Name  string
	Type  string
	Left  Expr
	Right Expr
	Args  []Expr
	Extra interface{}
}

type caseWhenExtra struct {
	WhenThen []WhenThenExpr
	ElseExpr Expr
}

type betweenExtra struct {
	From Expr
	To   Expr
	Not  bool
}

func (*RawExpr) iExpr() {}
func (r *RawExpr) replace(from, to Expr) bool {
	if r == nil {
		return false
	}
	if r.Left == from {
		r.Left = to
		return true
	}
	if r.Right == from {
		r.Right = to
		return true
	}
	if r.Left != nil && r.Left.replace(from, to) {
		return true
	}
	if r.Right != nil && r.Right.replace(from, to) {
		return true
	}
	for i := range r.Args {
		if r.Args[i] == from {
			r.Args[i] = to
			return true
		}
		if r.Args[i] != nil && r.Args[i].replace(from, to) {
			return true
		}
	}
	return false
}
func (r *RawExpr) Format(buf *TrackedBuffer) {
	if r == nil {
		return
	}
	writeExpr := func(e Expr) {
		if e == nil {
			return
		}
		if sub, ok := e.(*SelectStmt); ok && sub != nil {
			buf.Myprintf("(%v)", sub)
			return
		}
		buf.Myprintf("%v", e)
	}
	writeExprWithParens := func(e Expr) {
		if e == nil {
			return
		}
		if sub, ok := e.(*SelectStmt); ok && sub != nil {
			buf.Myprintf("(%v)", sub)
			return
		}
		if needsParenForOperatorExpr(e) {
			buf.Myprintf("(%v)", e)
			return
		}
		buf.Myprintf("%v", e)
	}
	switch r.Kind {
	case "col":
		if t, ok := r.Extra.(string); ok && t != "" {
			buf.Myprintf("%s.", t)
		}
		buf.Myprintf("%s", r.Name)
	case "pseudo_col":
		buf.Myprintf("%s", r.Name)
	case "func":
		buf.Myprintf("%s(", r.Name)
		for i, arg := range r.Args {
			if i > 0 {
				buf.Myprintf(", ")
			}
			writeExpr(arg)
		}
		buf.Myprintf(")")
	case "unary":
		buf.Myprintf("%s", unaryOpToSQL(r.Op))
		writeExprWithParens(r.Left)
	case "binary", "cmp":
		if r.Left == nil && r.Right == nil && r.Name != "" {
			buf.Myprintf("%s", r.Name)
			return
		}
		writeExprWithParens(r.Left)
		if op := binaryOpToSQL(r.Op); op != "" {
			buf.Myprintf(" %s", op)
		}
		if r.Right != nil {
			buf.Myprintf(" ")
			writeExprWithParens(r.Right)
		}
	case "json":
		if p, ok := r.Extra.(Token); ok {
			if r.Left != nil {
				writeExprWithParens(r.Left)
			}
			buf.Myprintf("->'%s'", p.Bytes)
		} else {
			if r.Left != nil {
				writeExprWithParens(r.Left)
			}
			switch p := r.Extra.(type) {
			case string:
				buf.Myprintf("->%s", p)
			default:
				buf.Myprintf("->")
			}
		}
	case "cast":
		buf.Myprintf("cast(")
		writeExpr(r.Left)
		if r.Type == "varchar" {
			buf.Myprintf(" as varchar(1))")
		} else {
			buf.Myprintf(" as %s)", r.Type)
		}
	case "trim":
		if spec, ok := r.Extra.(string); ok && spec != "" {
			buf.Myprintf("trim(%s", spec)
			if r.Left != nil {
				buf.Myprintf(" %v", r.Left)
			}
			buf.Myprintf(")")
		} else {
			if r.Left != nil {
				buf.Myprintf("trim(%v)", r.Left)
			} else {
				buf.Myprintf("trim()")
			}
		}
	case "trim_ext":
		spec := ""
		if s, ok := r.Extra.(string); ok {
			spec = s
		}
		if spec != "" {
			buf.Myprintf("trim(%s %v from %v)", spec, r.Left, r.Right)
		} else {
			buf.Myprintf("trim(%v from %v)", r.Left, r.Right)
		}
	case "position":
		buf.Myprintf("position(")
		if r.Left != nil {
			buf.Myprintf("%v", r.Left)
		}
		if r.Right != nil {
			buf.Myprintf(" in %v", r.Right)
		}
		buf.Myprintf(")")
	case "if":
		buf.Myprintf("if(")
		writeExpr(r.Left)
		buf.Myprintf(", ")
		writeExpr(r.Right)
		buf.Myprintf(", ")
		if e, ok := r.Extra.(Expr); ok {
			writeExpr(e)
		} else if n, ok := r.Extra.(SQLNode); ok && n != nil {
			buf.Myprintf("%v", n)
		}
		buf.Myprintf(")")
	case "ifnull":
		buf.Myprintf("ifnull(")
		writeExpr(r.Left)
		buf.Myprintf(", ")
		writeExpr(r.Right)
		buf.Myprintf(")")
	case "nullif":
		buf.Myprintf("nullif(")
		writeExpr(r.Left)
		buf.Myprintf(", ")
		writeExpr(r.Right)
		buf.Myprintf(")")
	case "coalesce":
		buf.Myprintf("coalesce(")
		for i, arg := range r.Args {
			if i > 0 {
				buf.Myprintf(", ")
			}
			writeExpr(arg)
		}
		buf.Myprintf(")")
	case "case_when":
		buf.Myprintf("case")
		if r.Left != nil {
			buf.Myprintf(" ")
			writeExpr(r.Left)
		}
		switch cw := r.Extra.(type) {
		case caseWhenExtra:
			for _, wt := range cw.WhenThen {
				buf.Myprintf(" when ")
				writeExpr(wt.When)
				buf.Myprintf(" then ")
				writeExpr(wt.Then)
			}
			if cw.ElseExpr != nil {
				buf.Myprintf(" else ")
				writeExpr(cw.ElseExpr)
			}
		case *caseWhenExtra:
			if cw != nil {
				for _, wt := range cw.WhenThen {
					buf.Myprintf(" when ")
					writeExpr(wt.When)
					buf.Myprintf(" then ")
					writeExpr(wt.Then)
				}
				if cw.ElseExpr != nil {
					buf.Myprintf(" else ")
					writeExpr(cw.ElseExpr)
				}
			}
		}
		buf.Myprintf(" end")
	case "between":
		writeExprWithParens(r.Left)
		switch be := r.Extra.(type) {
		case betweenExtra:
			buf.Myprintf(" ")
			if be.Not {
				buf.Myprintf("not ")
			}
			buf.Myprintf("between ")
			writeExprWithParens(be.From)
			buf.Myprintf(" and ")
			writeExprWithParens(be.To)
		case *betweenExtra:
			if be == nil {
				return
			}
			buf.Myprintf(" ")
			if be.Not {
				buf.Myprintf("not ")
			}
			buf.Myprintf("between ")
			writeExprWithParens(be.From)
			buf.Myprintf(" and ")
			writeExprWithParens(be.To)
		}
	case "is_null":
		writeExprWithParens(r.Left)
		buf.Myprintf(" is ")
		if not, ok := r.Extra.(bool); ok && not {
			buf.Myprintf("not ")
		}
		buf.Myprintf("null")
	case "in":
		writeExprWithParens(r.Left)
		buf.Myprintf(" %s (", binaryOpToSQL(r.Op))
		for i, arg := range r.Args {
			if i > 0 {
				buf.Myprintf(", ")
			}
			writeExpr(arg)
		}
		buf.Myprintf(")")
	case "in_subquery":
		writeExprWithParens(r.Left)
		buf.Myprintf(" %s (", binaryOpToSQL(r.Op))
		if sub, ok := r.Extra.(*SelectStmt); ok && sub != nil {
			buf.Myprintf("%v", sub)
		}
		buf.Myprintf(")")
	case "window_offset":
		buf.Myprintf("(%v, %v)", r.Left, r.Right)
	case "range_1":
		buf.Myprintf("(%v)", r.Args[0])
	case "range_2":
		buf.Myprintf("(%v, %v)", r.Args[0], r.Args[1])
	case "range_3":
		buf.Myprintf("(%v, %v, %v)", r.Args[0], r.Args[1], r.Args[2])
	case "partition_by":
		for i, arg := range r.Args {
			if i > 0 {
				buf.Myprintf(", ")
			}
			buf.Myprintf("%v", arg)
		}
	case "search_condition_list":
		buf.Myprintf("(")
		for i, arg := range r.Args {
			if i > 0 {
				buf.Myprintf(", ")
			}
			buf.Myprintf("%v", arg)
		}
		buf.Myprintf(")")
	default:
		if r.Name != "" {
			buf.Myprintf("%s", r.Name)
			return
		}
		if len(r.Op.Bytes) > 0 {
			buf.Myprintf("%s", r.Op.Bytes)
			return
		}
		buf.Myprintf("%s", r.Kind)
	}
}

func needsParenForOperatorExpr(e Expr) bool {
	re, ok := e.(*RawExpr)
	if !ok || re == nil {
		return false
	}
	switch re.Kind {
	case "unary", "binary", "cmp", "between", "is_null", "in", "in_subquery", "case_when":
		return true
	default:
		return false
	}
}
func (r *RawExpr) walkSubtree(visit Visit) error {
	if r == nil {
		return nil
	}
	if err := Walk(visit, r.Left, r.Right); err != nil {
		return err
	}
	for _, arg := range r.Args {
		if err := Walk(visit, arg); err != nil {
			return err
		}
	}
	switch x := r.Extra.(type) {
	case Expr:
		return Walk(visit, x)
	case caseWhenExtra:
		for _, wt := range x.WhenThen {
			if err := Walk(visit, wt.When, wt.Then); err != nil {
				return err
			}
		}
		return Walk(visit, x.ElseExpr)
	case *caseWhenExtra:
		if x == nil {
			return nil
		}
		for _, wt := range x.WhenThen {
			if err := Walk(visit, wt.When, wt.Then); err != nil {
				return err
			}
		}
		return Walk(visit, x.ElseExpr)
	case betweenExtra:
		return Walk(visit, x.From, x.To)
	case *betweenExtra:
		if x == nil {
			return nil
		}
		return Walk(visit, x.From, x.To)
	}
	return nil
}

var (
	OP_TYPE_UPLUS         = Token{Bytes: []byte("uplus")}
	OP_TYPE_MINUS         = Token{Bytes: []byte("minus")}
	OP_TYPE_ADD           = Token{Bytes: []byte("add")}
	OP_TYPE_SUB           = Token{Bytes: []byte("sub")}
	OP_TYPE_MULTI         = Token{Bytes: []byte("mul")}
	OP_TYPE_DIV           = Token{Bytes: []byte("div")}
	OP_TYPE_REM           = Token{Bytes: []byte("rem")}
	OP_TYPE_BIT_AND       = Token{Bytes: []byte("bit_and")}
	OP_TYPE_BIT_OR        = Token{Bytes: []byte("bit_or")}
	LOGIC_COND_TYPE_NOT   = Token{Bytes: []byte("not")}
	LOGIC_COND_TYPE_OR    = Token{Bytes: []byte("or")}
	LOGIC_COND_TYPE_AND   = Token{Bytes: []byte("and")}
	OP_TYPE_LOWER_THAN    = Token{Bytes: []byte("lt")}
	OP_TYPE_GREATER_THAN  = Token{Bytes: []byte("gt")}
	OP_TYPE_LOWER_EQUAL   = Token{Bytes: []byte("le")}
	OP_TYPE_GREATER_EQUAL = Token{Bytes: []byte("ge")}
	OP_TYPE_NOT_EQUAL     = Token{Bytes: []byte("ne")}
	OP_TYPE_EQUAL         = Token{Bytes: []byte("eq")}
	OP_TYPE_LIKE          = Token{Bytes: []byte("like")}
	OP_TYPE_NOT_LIKE      = Token{Bytes: []byte("not_like")}
	OP_TYPE_MATCH         = Token{Bytes: []byte("match")}
	OP_TYPE_NMATCH        = Token{Bytes: []byte("nmatch")}
	OP_TYPE_REGEXP        = Token{Bytes: []byte("regexp")}
	OP_TYPE_NOT_REGEXP    = Token{Bytes: []byte("not_regexp")}
	OP_TYPE_JSON_CONTAINS = Token{Bytes: []byte("contains")}
	OP_TYPE_IN            = Token{Bytes: []byte("in")}
	OP_TYPE_NOT_IN        = Token{Bytes: []byte("not_in")}
)

func unaryOpToSQL(op Token) string {
	switch string(op.Bytes) {
	case "uplus":
		return "+"
	case "minus":
		return "-"
	case "not":
		return "not "
	default:
		return ""
	}
}

func binaryOpToSQL(op Token) string {
	switch string(op.Bytes) {
	case "add":
		return "+"
	case "sub":
		return "-"
	case "mul":
		return "*"
	case "div":
		return "/"
	case "rem":
		return "%"
	case "bit_and":
		return "&"
	case "bit_or":
		return "|"
	case "or":
		return "or"
	case "and":
		return "and"
	case "lt":
		return "<"
	case "gt":
		return ">"
	case "le":
		return "<="
	case "ge":
		return ">="
	case "ne":
		return "!="
	case "eq":
		return "="
	case "like":
		return "like"
	case "not_like":
		return "not like"
	case "match":
		return "match"
	case "nmatch":
		return "nmatch"
	case "regexp":
		return "regexp"
	case "not_regexp":
		return "not regexp"
	case "contains":
		return "contains"
	case "in":
		return "in"
	case "not_in":
		return "not in"
	default:
		if len(op.Bytes) > 0 {
			return strings.ToLower(string(op.Bytes))
		}
		return ""
	}
}

func NewSelectStmt(yylex yyLexer, hint *HintOption, distinct bool, tagMode bool, selectList []Expr, from TableExpr, where Expr, partition Expr, rng Expr, every Literal, interpFill *FillExpr, window WindowExpr, group *GroupByExpr, having Expr) *SelectStmt {
	return &SelectStmt{
		Hint:       hint,
		IsDistinct: distinct,
		TagScan:    tagMode,
		Select:     selectList,
		From:       from,
		Where:      where,
		Partition:  partition,
		Range:      rng,
		Every:      every,
		InterpFill: interpFill,
		Window:     window,
		GroupBy:    group,
		Having:     having,
	}
}

func NewHintOptionFromHintToken(tok Token) *HintOption {
	raw := strings.ToLower(strings.TrimSpace(string(tok.Bytes)))
	if hint := hintOptionFromHintName(raw); hint != nil {
		return hint
	}
	raw = strings.ReplaceAll(raw, ",", " ")
	for _, part := range strings.Fields(raw) {
		if hint := hintOptionFromHintName(strings.TrimSpace(part)); hint != nil {
			return hint
		}
	}
	return nil
}

func hintOptionFromHintName(name string) *HintOption {
	switch name {
	case "batch_scan()":
		return NewHintOption(HINT_BATCH_SCAN)
	case "no_batch_scan()":
		return NewHintOption(HINT_NO_BATCH_SCAN)
	case "sort_for_group()":
		return NewHintOption(HINT_SORT_FOR_GROUP)
	case "partition_first()":
		return NewHintOption(HINT_PARTITION_FIRST)
	case "para_tables_sort()":
		return NewHintOption(HINT_PARA_TABLES_SORT)
	case "smalldata_ts_sort()":
		return NewHintOption(HINT_SMALLDATA_TS_SORT)
	case "hash_join()":
		return NewHintOption(HINT_HASH_JOIN)
	case "skip_tsma()":
		return NewHintOption(HINT_SKIP_TSMA)
	case "win_optimize_batch()":
		return NewHintOption(HINT_WIN_OPTIMIZE_BATCH)
	case "win_optimize_single()":
		return NewHintOption(HINT_WIN_OPTIMIZE_SINGLE)
	default:
		return nil
	}
}

func NewSelectStmtWithClauses(yylex yyLexer, base *SelectStmt, orderBy []OrderByExpr, slimit *LimitExpr, limit *LimitExpr) *SelectStmt {
	if base == nil {
		base = &SelectStmt{}
	}
	base.OrderBy = orderBy
	base.SLimit = slimit
	base.Limit = limit
	return base
}

func NewUnionStmt(yylex yyLexer, left *SelectStmt, right *SelectStmt, all bool) *SelectStmt {
	return &SelectStmt{Left: left, Right: right, SetOp: "union", SetAll: all}
}

func NewExceptStmt(yylex yyLexer, left *SelectStmt, right *SelectStmt) *SelectStmt {
	return &SelectStmt{Left: left, Right: right, SetOp: "except"}
}

func NewIntersectStmt(yylex yyLexer, left *SelectStmt, right *SelectStmt) *SelectStmt {
	return &SelectStmt{Left: left, Right: right, SetOp: "intersect"}
}

func NewTableNameExpr(yylex yyLexer, db string, table string, alias string) *TableNameExpr {
	return &TableNameExpr{DBName: db, TableName: table, Alias: alias}
}

func NewSubqueryTableExpr(yylex yyLexer, query *SelectStmt, alias string) *SubqueryTableExpr {
	return &SubqueryTableExpr{Query: query, Alias: alias}
}

func NewJoinTableExpr(yylex yyLexer, left TableExpr, right TableExpr, joinType JoinType, condition interface{}) *JoinTableExpr {
	return &JoinTableExpr{Left: left, Right: right, JoinType: joinType, Condition: condition}
}

func SetJoinWindowOffsetAndLimit(join *JoinTableExpr, windowOffset Expr, jlimit *LimitExpr) *JoinTableExpr {
	if join == nil {
		return nil
	}
	join.WindowOffset = windowOffset
	join.JLimit = jlimit
	return join
}

func NewWindowOffsetExpr(start Expr, end Expr) Expr {
	return &RawExpr{Kind: "window_offset", Left: start, Right: end}
}

func NewUnaryExpr(yylex yyLexer, op Token, expr Expr) Expr {
	return &RawExpr{Kind: "unary", Op: op, Left: expr}
}

func NewBinaryExpr(yylex yyLexer, left Expr, op Token, right Expr) Expr {
	return &RawExpr{Kind: "binary", Op: op, Left: left, Right: right}
}

func NewJsonExpr(yylex yyLexer, left Expr, path Token) Expr {
	return &RawExpr{Kind: "json", Left: left, Extra: path}
}

func NewColNameExpr(yylex yyLexer, table string, col string) Expr {
	return &RawExpr{Kind: "col", Name: col, Extra: table}
}

func NewPseudoColumnExpr(yylex yyLexer, name string) Expr {
	return &RawExpr{Kind: "pseudo_col", Name: name}
}

func NewFuncExpr(yylex yyLexer, name string, args []Expr) Expr {
	return &RawExpr{Kind: "func", Name: name, Args: args}
}

func NewCastExpr(yylex yyLexer, expr Expr, typeName string) Expr {
	return &RawExpr{Kind: "cast", Left: expr, Type: typeName}
}

func NewTrimExpr(yylex yyLexer, expr Expr, spec string) Expr {
	return &RawExpr{Kind: "trim", Left: expr, Extra: spec}
}

func NewTrimExprWithPattern(yylex yyLexer, trimWhat Expr, fromWhat Expr, spec string) Expr {
	return &RawExpr{
		Kind:  "trim_ext",
		Left:  trimWhat,
		Right: fromWhat,
		Extra: spec,
	}
}

func NewPositionExpr(yylex yyLexer, left Expr, right Expr) Expr {
	return &RawExpr{Kind: "position", Left: left, Right: right}
}

func NewIfExpr(yylex yyLexer, cond Expr, ifTrue Expr, ifFalse Expr) Expr {
	return &RawExpr{Kind: "if", Left: cond, Right: ifTrue, Extra: ifFalse}
}

func NewIfNullExpr(yylex yyLexer, left Expr, right Expr) Expr {
	return &RawExpr{Kind: "ifnull", Left: left, Right: right}
}

func NewNullIfExpr(yylex yyLexer, left Expr, right Expr) Expr {
	return &RawExpr{Kind: "nullif", Left: left, Right: right}
}

func NewCoalesceExpr(yylex yyLexer, args []Expr) Expr {
	return &RawExpr{Kind: "coalesce", Args: args}
}

func NewCaseWhenExpr(yylex yyLexer, base Expr, whenThen []WhenThenExpr, elseExpr Expr) Expr {
	return &RawExpr{Kind: "case_when", Left: base, Extra: caseWhenExtra{WhenThen: whenThen, ElseExpr: elseExpr}}
}

func NewLiteralExpr(yylex yyLexer, tok Token, typ LiteralType) Literal {
	return Literal{Val: tok, Type: typ}
}

func (n Literal) iExpr() {}
func (n Literal) replace(from, to Expr) bool {
	return false
}
func (n Literal) Format(buf *TrackedBuffer) {
	if len(n.Val.Bytes) != 0 {
		if n.Type == LiteralString {
			buf.Myprintf("'%s'", n.Val.Bytes)
			return
		}
		buf.Myprintf("%s", n.Val.Bytes)
	}
}
func (n Literal) walkSubtree(visit Visit) error {
	return nil
}

func NewComparisonExpr(yylex yyLexer, left Expr, op Token, right Expr) Expr {
	return &RawExpr{Kind: "cmp", Left: left, Op: op, Right: right}
}

func NewBetweenExpr(yylex yyLexer, target Expr, from Expr, to Expr, not bool) Expr {
	return &RawExpr{Kind: "between", Left: target, Extra: betweenExtra{From: from, To: to, Not: not}}
}

func NewIsNullExpr(yylex yyLexer, target Expr, not bool) Expr {
	return &RawExpr{Kind: "is_null", Left: target, Extra: not}
}

func NewInExpr(yylex yyLexer, target Expr, op Token, list []Expr) Expr {
	return &RawExpr{Kind: "in", Left: target, Op: op, Args: list}
}

func NewInSubqueryExpr(yylex yyLexer, target Expr, op Token, subquery *SelectStmt) Expr {
	return &RawExpr{Kind: "in_subquery", Left: target, Op: op, Extra: subquery}
}

func NewInPredicateExpr(yylex yyLexer, target Expr, op Token, inVal Expr) Expr {
	if raw, ok := inVal.(*RawExpr); ok && raw.Kind == "in_list" {
		return NewInExpr(yylex, target, op, raw.Args)
	}
	if sub, ok := inVal.(*SelectStmt); ok {
		return NewInSubqueryExpr(yylex, target, op, sub)
	}
	return NewInExpr(yylex, target, op, nil)
}

func NewIntervalAutoWindowExpr(yylex yyLexer, interval Literal, autoTok Token, sliding Literal, fill *FillExpr) WindowExpr {
	return WindowExpr{
		Interval: interval,
		Offset:   Literal{Val: autoTok, Type: LiteralDuration},
		Sliding:  sliding,
		Fill:     fill,
	}
}

func NewPartitionByExpr(yylex yyLexer, exprs []Expr) Expr {
	return &RawExpr{Kind: "partition_by", Args: exprs}
}
