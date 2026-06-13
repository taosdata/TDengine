package sqlparser

type DropNamedStmt struct {
	Kind     string
	Name     string
	IfExists bool
}

func (*DropNamedStmt) iStatement() {}

func (s *DropNamedStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("drop %s", s.Kind)
	if s.IfExists {
		buf.Myprintf(" if exists")
	}
	buf.Myprintf(" %s", s.Name)
}

func (s *DropNamedStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewDropNamedStmt(kind string, name string, ifExists bool) *DropNamedStmt {
	return &DropNamedStmt{
		Kind:     kind,
		Name:     name,
		IfExists: ifExists,
	}
}
