package sqlparser

type ScanStmt struct {
	Scope string
	Name  string
	Start string
	End   string
}

func (*ScanStmt) iStatement() {}

func (s *ScanStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	switch s.Scope {
	case "database":
		buf.Myprintf("scan database %s", s.Name)
	case "vgroups":
		if s.Name != "" {
			buf.Myprintf("scan %s. vgroups in (1)", s.Name)
		} else {
			buf.Myprintf("scan vgroups in (1)")
		}
	default:
		buf.Myprintf("scan %s", s.Scope)
		if s.Name != "" {
			buf.Myprintf(" %s", s.Name)
		}
	}
	if s.Start != "" {
		buf.Myprintf(" start with %s", s.Start)
	}
	if s.End != "" {
		buf.Myprintf(" end with %s", s.End)
	}
}

func (s *ScanStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewScanStmt(scope string, name string, start string, end string) *ScanStmt {
	return &ScanStmt{
		Scope: scope,
		Name:  name,
		Start: start,
		End:   end,
	}
}
