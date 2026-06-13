package sqlparser

import (
	"sqlparser/tool"
	"strconv"
)

type CreateFunctionStmt struct {
	Name         string
	Body         string
	OutputType   string
	IgnoreExists bool
	OrReplace    bool
	Aggregate    bool
	Bufsize      int32
	Language     string
}

func (*CreateFunctionStmt) iStatement() {}

func (s *CreateFunctionStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("create")
	if s.OrReplace {
		buf.Myprintf(" or replace")
	}
	if s.Aggregate {
		buf.Myprintf(" aggregate")
	}
	buf.Myprintf(" function")
	if s.IgnoreExists {
		buf.Myprintf(" if not exists")
	}
	buf.Myprintf(" %s as '%s' outputtype %s", s.Name, s.Body, s.OutputType)
	if s.Bufsize > 0 {
		buf.Myprintf(" bufsize %s", strconv.FormatInt(int64(s.Bufsize), 10))
	}
	if s.Language != "" {
		buf.Myprintf(" language '%s'", s.Language)
	}
}

func (s *CreateFunctionStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewCreateFunctionStmt(orReplace, aggregate, ignoreExists bool, name string, body Token, outputType string, bufsize int32, language Token) *CreateFunctionStmt {
	return &CreateFunctionStmt{
		Name:         name,
		Body:         tool.BytesToString(body.Bytes),
		OutputType:   outputType,
		IgnoreExists: ignoreExists,
		OrReplace:    orReplace,
		Aggregate:    aggregate,
		Bufsize:      bufsize,
		Language:     tool.BytesToString(language.Bytes),
	}
}

type DropFunctionStmt struct {
	Name     string
	IfExists bool
}

func (*DropFunctionStmt) iStatement() {}

func (s *DropFunctionStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("drop function")
	if s.IfExists {
		buf.Myprintf(" if exists")
	}
	buf.Myprintf(" %s", s.Name)
}

func (s *DropFunctionStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewDropFunctionStmt(name string, ifExists bool) *DropFunctionStmt {
	return &DropFunctionStmt{Name: name, IfExists: ifExists}
}
