package sqlparser

import (
	"strconv"
	"strings"
)

type StreamStmt struct {
	Action     string
	Names      []string
	IfExists   bool
	NotExists  bool
	Ignore     bool
	RecalcFrom string
	RecalcTo   string
	Trigger    string
	OutTable   string
	Query      *SelectStmt
}

func (*StreamStmt) iStatement() {}

func (s *StreamStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("%s stream", s.Action)
	if s.NotExists {
		buf.Myprintf(" if not exists")
	}
	if s.IfExists {
		buf.Myprintf(" if exists")
	}
	if s.Ignore {
		buf.Myprintf(" ignore untreated")
	}
	if len(s.Names) > 0 {
		buf.Myprintf(" %s", strings.Join(s.Names, ", "))
	}
	if s.Trigger != "" {
		buf.Myprintf(" %s", s.Trigger)
	}
	if s.OutTable != "" {
		buf.Myprintf(" %s", s.OutTable)
	}
	if s.Query != nil {
		buf.Myprintf(" as %v", s.Query)
	}
	if s.RecalcFrom != "" {
		buf.Myprintf(" from %s", formatStreamTimePoint(s.RecalcFrom))
	}
	if s.RecalcTo != "" {
		buf.Myprintf(" to %s", formatStreamTimePoint(s.RecalcTo))
	}
}

func (s *StreamStmt) walkSubtree(visit Visit) error {
	if s == nil {
		return nil
	}
	return Walk(visit, s.Query)
}

func NewStreamStmt(action string, names []string, ifExists bool, ignore bool) *StreamStmt {
	return &StreamStmt{
		Action:   action,
		Names:    names,
		IfExists: ifExists,
		Ignore:   ignore,
	}
}

type StreamRecalculateRange struct {
	From string
	To   string
}

func NewRecalculateStreamStmt(name string, r StreamRecalculateRange) *StreamStmt {
	return &StreamStmt{
		Action:     "recalculate",
		Names:      []string{name},
		RecalcFrom: r.From,
		RecalcTo:   r.To,
	}
}

func formatStreamTimePoint(v string) string {
	if _, err := strconv.ParseInt(v, 10, 64); err == nil {
		return v
	}
	return "'" + strings.ReplaceAll(v, "'", "''") + "'"
}
