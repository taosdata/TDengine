package sqlparser

import "strings"

type AlterNamedStmt struct {
	Kind     string
	Name     string
	IfExists bool
	Funcs    []string
}

func (*AlterNamedStmt) iStatement() {}

func (s *AlterNamedStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("alter %s", s.Kind)
	if s.IfExists {
		buf.Myprintf(" if exists")
	}
	buf.Myprintf(" %s", s.Name)
	if len(s.Funcs) > 0 {
		buf.Myprintf(" function(%s)", strings.Join(s.Funcs, ", "))
	}
}

func (s *AlterNamedStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewAlterNamedStmt(kind string, name string, ifExists bool, funcs []string) *AlterNamedStmt {
	return &AlterNamedStmt{
		Kind:     kind,
		Name:     name,
		IfExists: ifExists,
		Funcs:    funcs,
	}
}
