package sqlparser

type TopicStmt struct {
	Drop      bool
	DropGroup bool
	Reload    bool
	IfExists  bool
	ExistsOpt bool
	NotExists bool
	Force     bool
	Name      string
	GroupName string
	OnTopic   string
	Query     *SelectStmt
	MetaMode  string
	Database  string
	Stable    string
	Where     Expr
}

func (*TopicStmt) iStatement() {}

func (s *TopicStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	if s.DropGroup {
		buf.Myprintf("drop consumer group")
		if s.ExistsOpt {
			buf.Myprintf(" if exists")
		}
		if s.Force {
			buf.Myprintf(" force")
		}
		buf.Myprintf(" %s on %s", s.GroupName, s.OnTopic)
		return
	}
	if s.Drop {
		buf.Myprintf("drop topic")
		if s.ExistsOpt {
			buf.Myprintf(" if exists")
		}
		if s.Force {
			buf.Myprintf(" force")
		}
		buf.Myprintf(" %s", s.Name)
		return
	}
	if s.Reload {
		buf.Myprintf("reload topic")
		if s.IfExists {
			buf.Myprintf(" if exists")
		}
		buf.Myprintf(" %s", s.Name)
		if s.Query != nil {
			buf.Myprintf(" as %v", s.Query)
		}
		return
	}
	buf.Myprintf("create topic")
	if s.NotExists {
		buf.Myprintf(" if not exists")
	}
	buf.Myprintf(" %s", s.Name)
	if s.Query != nil {
		buf.Myprintf(" as %v", s.Query)
		return
	}
	if s.MetaMode != "" {
		switch s.MetaMode {
		case "with_meta_as":
			buf.Myprintf(" with meta as")
		case "only_meta_as":
			buf.Myprintf(" only meta as")
		default:
			buf.Myprintf(" %s", s.MetaMode)
		}
	}
	if s.Database != "" {
		buf.Myprintf(" database %s", s.Database)
	}
	if s.Stable != "" {
		buf.Myprintf(" stable %s", s.Stable)
	}
	if s.Where != nil {
		buf.Myprintf(" where %v", s.Where)
	}
}

func (s *TopicStmt) walkSubtree(visit Visit) error {
	if s == nil {
		return nil
	}
	return Walk(visit, s.Query, s.Where)
}
