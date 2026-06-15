package sqlparser

type CreateViewStmt struct {
	Replace bool
	Name    string
	Query   *SelectStmt
}

func (*CreateViewStmt) iStatement() {}

func (s *CreateViewStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("create ")
	if s.Replace {
		buf.Myprintf("or replace ")
	}
	buf.Myprintf("view %s", s.Name)
	if s.Query != nil {
		buf.Myprintf(" as %v", s.Query)
	}
}

func (s *CreateViewStmt) walkSubtree(visit Visit) error {
	if s == nil {
		return nil
	}
	return Walk(visit, s.Query)
}

type DropViewStmt struct {
	Name     string
	IfExists bool
}

func (*DropViewStmt) iStatement() {}

func (s *DropViewStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("drop view")
	if s.IfExists {
		buf.Myprintf(" if exists")
	}
	buf.Myprintf(" %s", s.Name)
}

func (s *DropViewStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewDropViewStmt(name string, ifExists bool) *DropViewStmt {
	return &DropViewStmt{Name: name, IfExists: ifExists}
}
