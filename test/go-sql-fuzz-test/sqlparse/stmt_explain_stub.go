package sqlparser

import "strconv"

type ExplainOptions struct {
	VerboseSet bool
	Verbose    bool
	RatioSet   bool
	Ratio      Literal
}

type ExplainStmt struct {
	Analyze bool
	Options ExplainOptions
	Target  Statement
}

func (*ExplainStmt) iStatement() {}

func (s *ExplainStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("explain")
	if s.Analyze {
		buf.Myprintf(" analyze")
	}
	if s.Options.VerboseSet {
		buf.Myprintf(" verbose %s", strconv.FormatBool(s.Options.Verbose))
	}
	if s.Options.RatioSet {
		buf.Myprintf(" ratio %v", s.Options.Ratio)
	}
	if s.Target != nil {
		buf.Myprintf(" %v", s.Target)
	}
}

func (s *ExplainStmt) walkSubtree(visit Visit) error {
	if s == nil {
		return nil
	}
	if s.Options.RatioSet {
		return Walk(visit, s.Target, s.Options.Ratio)
	}
	return Walk(visit, s.Target)
}
