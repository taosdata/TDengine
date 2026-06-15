package sqlparser

import "strings"

type ValType int

const (
	NullVal = ValType(iota) + 1
	StrVal
	IntVal
	FloatVal
	HexNum
	TimeVal
	DurationVal
)

// NewStrVal builds a new StrVal.
func NewStrVal(in []byte) *SQLVal {
	return &SQLVal{Type: StrVal, Val: in}
}

// NewIntVal builds a new IntVal.
func NewIntVal(in []byte) *SQLVal {
	return &SQLVal{Type: IntVal, Val: in}
}

// NewFloatVal builds a new FloatVal.
func NewFloatVal(in []byte) *SQLVal {
	return &SQLVal{Type: FloatVal, Val: in}
}

// NewHexNum builds a new HexNum.
func NewHexNum(in []byte) *SQLVal {
	return &SQLVal{Type: HexNum, Val: in}
}

func NewNullVal() *SQLVal {
	return &SQLVal{Type: NullVal, Val: nil}
}

func NewTimeVal(in []byte) *SQLVal {
	return &SQLVal{Type: TimeVal, Val: in}
}

func NewDurationVal(in []byte) *SQLVal {
	return &SQLVal{Type: DurationVal, Val: in}
}

type SQLVal struct {
	Type ValType
	Val  []byte
}

func (node *SQLVal) iExpr() {
}

// Format formats the node.
func (node *SQLVal) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
	switch node.Type {
	case StrVal:
		buf.Myprintf("'%s'", strings.ReplaceAll(string(node.Val), "'", "''"))
	case IntVal, FloatVal, HexNum, TimeVal, DurationVal:
		buf.Myprintf("%s", []byte(node.Val))
	case NullVal:
		buf.Myprintf("null")
	default:
		if len(node.Val) > 0 {
			buf.Myprintf("%s", []byte(node.Val))
		}
	}
}

func (node *SQLVal) walkSubtree(visit Visit) error {
	return nil
}

func (node *SQLVal) replace(from, to Expr) bool {
	return false
}

type BoolVal bool

func (node BoolVal) iExpr() {
}

// Format formats the node.
func (node BoolVal) Format(buf *TrackedBuffer) {
	if node {
		buf.Myprintf("true")
	} else {
		buf.Myprintf("false")
	}
}

func (node BoolVal) walkSubtree(visit Visit) error {
	return nil
}

func (node BoolVal) replace(from, to Expr) bool {
	return false
}
