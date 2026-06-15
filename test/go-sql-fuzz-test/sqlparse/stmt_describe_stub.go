package sqlparser

type DescribeStmt struct {
	Table string
}

func (*DescribeStmt) iStatement() {}

func (s *DescribeStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("describe %s", s.Table)
}

func (*DescribeStmt) walkSubtree(visit Visit) error { return nil }
