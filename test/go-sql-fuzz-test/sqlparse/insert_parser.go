package sqlparser

import (
	"fmt"
	"strings"
)

type tokenItem struct {
	typ int
	val []byte
}

type insertTokenParser struct {
	tokens []tokenItem
	pos    int
}

func parseInsertSQL(sql string) (Statement, error) {
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(sql)), "insert") {
		return nil, errNotInsert
	}

	sc := NewScanner(sql)
	toks := make([]tokenItem, 0, 64)
	for {
		t, v := sc.Scan()
		if t == 0 {
			break
		}
		toks = append(toks, tokenItem{typ: t, val: v})
	}

	p := &insertTokenParser{tokens: toks}
	stmt, err := p.parseInsertStatement()
	if err != nil {
		return nil, err
	}
	return stmt, nil
}

var errNotInsert = fmt.Errorf("not insert")

func (p *insertTokenParser) parseInsertStatement() (InsertStatement, error) {
	var out InsertStatement
	if err := p.expect(INSERT); err != nil {
		return nil, err
	}
	if err := p.expect(INTO); err != nil {
		return nil, err
	}

	first, err := p.parseInsertNode()
	if err != nil {
		return nil, err
	}
	out = append(out, first)

	for !p.eof() {
		if p.peekType() == NK_SEMI {
			p.pos++
			break
		}
		n, err := p.parseInsertNode()
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}

	return out, nil
}

func (p *insertTokenParser) parseInsertNode() (InsertNode, error) {
	tbl, err := p.parseTableName()
	if err != nil {
		return InsertNode{}, err
	}

	node := InsertNode{TableName: tbl}

	if p.accept(USING) {
		usingTbl, err := p.parseTableName()
		if err != nil {
			return InsertNode{}, err
		}
		using := &UsingClause{TableName: usingTbl}
		if p.peekType() == NK_LP {
			keys, err := p.parseNameList()
			if err != nil {
				return InsertNode{}, err
			}
			using.TagKeys = keys
		}
		if err := p.expect(TAGS); err != nil {
			return InsertNode{}, err
		}
		tagVals, err := p.parseSQLValList()
		if err != nil {
			return InsertNode{}, err
		}
		using.TagValues = tagVals
		node.Using = using
	}

	if p.peekType() == NK_LP {
		fields, err := p.parseNameList()
		if err != nil {
			return InsertNode{}, err
		}
		node.Fields = fields
	}

	if err := p.expect(VALUES); err != nil {
		return InsertNode{}, err
	}

	rows, err := p.parseValueRows()
	if err != nil {
		return InsertNode{}, err
	}
	node.Values = rows
	return node, nil
}

func (p *insertTokenParser) parseTableName() (*TableName, error) {
	left, err := p.expectToken(NK_ID)
	if err != nil {
		return nil, err
	}
	if p.accept(NK_DOT) {
		right, err := p.expectToken(NK_ID)
		if err != nil {
			return nil, err
		}
		return &TableName{
			Qualifier: NewTableIdent(string(left.val)),
			Name:      NewTableIdent(string(right.val)),
		}, nil
	}
	return &TableName{Name: NewTableIdent(string(left.val))}, nil
}

func (p *insertTokenParser) parseNameList() ([][]byte, error) {
	if err := p.expect(NK_LP); err != nil {
		return nil, err
	}
	out := make([][]byte, 0, 4)
	for {
		tok, err := p.expectToken(NK_ID)
		if err != nil {
			return nil, err
		}
		out = append(out, tok.val)
		if p.accept(NK_COMMA) {
			continue
		}
		break
	}
	if err := p.expect(NK_RP); err != nil {
		return nil, err
	}
	return out, nil
}

func (p *insertTokenParser) parseSQLValList() ([]*SQLVal, error) {
	if err := p.expect(NK_LP); err != nil {
		return nil, err
	}
	out := make([]*SQLVal, 0, 4)
	for {
		v, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		out = append(out, v)
		if p.accept(NK_COMMA) {
			continue
		}
		break
	}
	if err := p.expect(NK_RP); err != nil {
		return nil, err
	}
	return out, nil
}

func (p *insertTokenParser) parseValueRows() ([][]*SQLVal, error) {
	rows := make([][]*SQLVal, 0, 2)
	for {
		if p.peekType() != NK_LP {
			break
		}
		row, err := p.parseSQLValList()
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
		_ = p.accept(NK_COMMA)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("missing values row")
	}
	return rows, nil
}

func (p *insertTokenParser) parseValue() (*SQLVal, error) {
	if p.accept(NOW) {
		if p.accept(NK_LP) {
			if err := p.expect(NK_RP); err != nil {
				return nil, err
			}
		}
		return NewTimeVal([]byte("now")), nil
	}
	if p.accept(TODAY) {
		if p.accept(NK_LP) {
			if err := p.expect(NK_RP); err != nil {
				return nil, err
			}
		}
		return NewTimeVal([]byte("today")), nil
	}

	if p.eof() {
		return nil, fmt.Errorf("unexpected eof")
	}
	t := p.tokens[p.pos]
	p.pos++
	switch t.typ {
	case NK_INTEGER:
		return NewIntVal(t.val), nil
	case NK_FLOAT:
		return NewFloatVal(t.val), nil
	case NK_STRING:
		return NewStrVal(t.val), nil
	case NK_BOOL:
		return &SQLVal{Type: StrVal, Val: t.val}, nil
	default:
		return nil, fmt.Errorf("unexpected value token: %d", t.typ)
	}
}

func (p *insertTokenParser) expect(typ int) error {
	_, err := p.expectToken(typ)
	return err
}

func (p *insertTokenParser) expectToken(typ int) (tokenItem, error) {
	if p.eof() {
		return tokenItem{}, fmt.Errorf("expected %d, got eof", typ)
	}
	got := p.tokens[p.pos]
	if got.typ != typ {
		return tokenItem{}, fmt.Errorf("expected %d, got %d", typ, got.typ)
	}
	p.pos++
	return got, nil
}

func (p *insertTokenParser) accept(typ int) bool {
	if p.eof() {
		return false
	}
	if p.tokens[p.pos].typ != typ {
		return false
	}
	p.pos++
	return true
}

func (p *insertTokenParser) peekType() int {
	if p.eof() {
		return 0
	}
	return p.tokens[p.pos].typ
}

func (p *insertTokenParser) eof() bool {
	return p.pos >= len(p.tokens)
}
