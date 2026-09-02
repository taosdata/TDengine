package sqlparser

type DeleteStmt struct {
	Table string
	Where Expr
}

func (*DeleteStmt) iStatement() {}

func (s *DeleteStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("delete from %s", s.Table)
	if s.Where != nil {
		buf.Myprintf(" where %v", s.Where)
	}
}

func (s *DeleteStmt) walkSubtree(visit Visit) error {
	if s == nil {
		return nil
	}
	return Walk(visit, s.Where)
}

func NewDeleteStmt(table string, where Expr) *DeleteStmt {
	return &DeleteStmt{
		Table: table,
		Where: where,
	}
}
