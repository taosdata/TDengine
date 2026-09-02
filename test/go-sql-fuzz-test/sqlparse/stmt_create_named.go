package sqlparser

import "strings"

// CreateNamedStmt models CREATE INDEX/RSMA/TSMA/SMA INDEX forms.
type CreateNamedStmt struct {
	Kind        string
	Name        string
	IfNotExists bool
	OnTable     string

	Columns     []string
	Funcs       []string
	Intervals   []string
	Interval    string
	Options     string
	IsRecursive bool
}

func (*CreateNamedStmt) iStatement() {}

func (s *CreateNamedStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}

	switch s.Kind {
	case "index":
		buf.Myprintf("create index")
	case "sma_index":
		buf.Myprintf("create sma index")
	case "rsma":
		buf.Myprintf("create rsma")
	case "tsma":
		if s.IsRecursive {
			buf.Myprintf("create recursive tsma")
		} else {
			buf.Myprintf("create tsma")
		}
	default:
		buf.Myprintf("create %s", s.Kind)
	}

	if s.IfNotExists {
		buf.Myprintf(" if not exists")
	}
	if s.Name != "" {
		buf.Myprintf(" %s", s.Name)
	}
	if s.OnTable != "" {
		buf.Myprintf(" on %s", s.OnTable)
	}

	switch s.Kind {
	case "index":
		if len(s.Columns) > 0 {
			buf.Myprintf(" (%s)", strings.Join(s.Columns, ", "))
		}
	case "sma_index":
		if s.Options != "" {
			buf.Myprintf(" %s", s.Options)
		}
	case "rsma":
		if len(s.Funcs) > 0 {
			buf.Myprintf(" function(%s)", strings.Join(s.Funcs, ", "))
		}
		if len(s.Intervals) > 0 {
			buf.Myprintf(" interval(%s)", strings.Join(s.Intervals, ", "))
		}
	case "tsma":
		if len(s.Funcs) > 0 {
			buf.Myprintf(" function(%s)", strings.Join(s.Funcs, ", "))
		}
		if s.Interval != "" {
			buf.Myprintf(" interval(%s)", s.Interval)
		}
	}
}

func (s *CreateNamedStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewCreateIndexStmt(ifNotExists bool, name, onTable string, cols []string) *CreateNamedStmt {
	return &CreateNamedStmt{Kind: "index", Name: name, IfNotExists: ifNotExists, OnTable: onTable, Columns: cols}
}

func NewCreateSMAIndexStmt(ifNotExists bool, name, onTable, options string) *CreateNamedStmt {
	return &CreateNamedStmt{Kind: "sma_index", Name: name, IfNotExists: ifNotExists, OnTable: onTable, Options: options}
}

func NewCreateRSMAStmt(ifNotExists bool, name, onTable string, funcs []string, intervals []string) *CreateNamedStmt {
	return &CreateNamedStmt{Kind: "rsma", Name: name, IfNotExists: ifNotExists, OnTable: onTable, Funcs: funcs, Intervals: intervals}
}

func NewCreateTSMAStmt(ifNotExists bool, recursive bool, name, onTable string, funcs []string, interval string) *CreateNamedStmt {
	return &CreateNamedStmt{
		Kind:        "tsma",
		Name:        name,
		IfNotExists: ifNotExists,
		OnTable:     onTable,
		Funcs:       funcs,
		Interval:    interval,
		IsRecursive: recursive,
	}
}
