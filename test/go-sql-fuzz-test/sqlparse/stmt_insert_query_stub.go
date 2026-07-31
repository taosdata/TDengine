package sqlparser

import "strings"

type InsertQueryStmt struct {
	Table   string
	Columns []string
	Query   *SelectStmt
}

func (*InsertQueryStmt) iStatement() {}

func (s *InsertQueryStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("insert into %s", s.Table)
	if len(s.Columns) > 0 {
		buf.Myprintf(" (%s)", strings.Join(s.Columns, ", "))
	}
	if s.Query != nil {
		buf.Myprintf(" %v", s.Query)
	}
}

func (s *InsertQueryStmt) walkSubtree(visit Visit) error {
	if s == nil {
		return nil
	}
	return Walk(visit, s.Query)
}
