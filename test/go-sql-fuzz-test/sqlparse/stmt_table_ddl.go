package sqlparser

import (
	"strings"
)

type MultiCreateTableStmt struct {
	Entries []MultiCreateTableEntry
}

type MultiCreateTableEntry struct {
	NotExists    bool
	Target       string
	Using        string
	SpecificCols []string
	TagValues    []string
	Options      *TableOptions
}

func (*MultiCreateTableStmt) iStatement() {}

func (s *MultiCreateTableStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("create table")
	for _, e := range s.Entries {
		buf.Myprintf(" ")
		if e.NotExists {
			buf.Myprintf("if not exists ")
		}
		buf.Myprintf("%s using %s", e.Target, e.Using)
		if len(e.SpecificCols) > 0 {
			buf.Myprintf(" (%s)", strings.Join(e.SpecificCols, ", "))
		}
		if len(e.TagValues) > 0 {
			buf.Myprintf(" tags (%s)", strings.Join(e.TagValues, ", "))
		}
		appendTableOptions(buf, e.Options)
	}
}

func (s *MultiCreateTableStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewMultiCreateTableStmt(target string, using string, notExists bool, specificCols []string, tagValues []string, options *TableOptions) *MultiCreateTableStmt {
	return &MultiCreateTableStmt{
		Entries: []MultiCreateTableEntry{{
			NotExists:    notExists,
			Target:       target,
			Using:        using,
			SpecificCols: specificCols,
			TagValues:    tagValues,
			Options:      options,
		}},
	}
}

func AppendMultiCreateTableStmt(stmt Statement, target string, using string, notExists bool, specificCols []string, tagValues []string, options *TableOptions) *MultiCreateTableStmt {
	out, ok := stmt.(*MultiCreateTableStmt)
	if !ok || out == nil {
		out = &MultiCreateTableStmt{}
	}
	out.Entries = append(out.Entries, MultiCreateTableEntry{
		NotExists:    notExists,
		Target:       target,
		Using:        using,
		SpecificCols: specificCols,
		TagValues:    tagValues,
		Options:      options,
	})
	return out
}

type DropTableStmt struct {
	Kind        string
	WithKeyword bool
	EntriesRaw  string
}

func (*DropTableStmt) iStatement() {}

func (s *DropTableStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("drop %s", s.Kind)
	if s.WithKeyword {
		buf.Myprintf(" with")
	}
	if s.EntriesRaw != "" {
		buf.Myprintf(" %s", s.EntriesRaw)
	}
}

func (s *DropTableStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewDropTableStmt(kind string, withKeyword bool, entriesRaw string) *DropTableStmt {
	return &DropTableStmt{
		Kind:        kind,
		WithKeyword: withKeyword,
		EntriesRaw:  strings.TrimSpace(entriesRaw),
	}
}

type CreateSubTableFromFileStmt struct {
	NotExists bool
	Using     string
	TagItems  []string
	File      string
}

func (*CreateSubTableFromFileStmt) iStatement() {}

func (s *CreateSubTableFromFileStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("create table")
	if s.NotExists {
		buf.Myprintf(" if not exists")
	}
	buf.Myprintf(" using %s", s.Using)
	if len(s.TagItems) > 0 {
		buf.Myprintf(" (%s)", strings.Join(s.TagItems, ", "))
	} else {
		// Grammar requires an explicit empty tag-item list for FROM FILE subtable syntax.
		buf.Myprintf(" ()")
	}
	buf.Myprintf(" file '%s'", s.File)
}

func (s *CreateSubTableFromFileStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewCreateSubTableFromFileStmt(notExists bool, using string, tagItems []string, file string) *CreateSubTableFromFileStmt {
	return &CreateSubTableFromFileStmt{
		NotExists: notExists,
		Using:     strings.TrimSpace(using),
		TagItems:  tagItems,
		File:      file,
	}
}

type CreateVSubTableStmt struct {
	NotExists    bool
	Target       string
	Using        string
	SpecificCols []string
	RefCols      []string
	TagValues    []string
}

func (*CreateVSubTableStmt) iStatement() {}

func (s *CreateVSubTableStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("create vtable")
	if s.NotExists {
		buf.Myprintf(" if not exists")
	}
	buf.Myprintf(" %s using %s", s.Target, s.Using)
	if len(s.SpecificCols) > 0 {
		buf.Myprintf(" (%s)", strings.Join(s.SpecificCols, ", "))
	}
	if len(s.RefCols) > 0 {
		buf.Myprintf(" refs (%s)", strings.Join(s.RefCols, ", "))
	}
	if len(s.TagValues) > 0 {
		buf.Myprintf(" tags (%s)", strings.Join(s.TagValues, ", "))
	}
}

func (s *CreateVSubTableStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewCreateVSubTableStmt(notExists bool, target string, using string, specificCols []string, refCols []string, tagValues []string) *CreateVSubTableStmt {
	return &CreateVSubTableStmt{
		NotExists:    notExists,
		Target:       strings.TrimSpace(target),
		Using:        strings.TrimSpace(using),
		SpecificCols: specificCols,
		RefCols:      refCols,
		TagValues:    tagValues,
	}
}

type AlterTableStmt struct {
	Kind      string
	ClauseRaw string
}

func (*AlterTableStmt) iStatement() {}

func (s *AlterTableStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("alter %s %s", s.Kind, s.ClauseRaw)
}

func (s *AlterTableStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewAlterTableStmt(kind string, clauseRaw string) *AlterTableStmt {
	return &AlterTableStmt{
		Kind:      kind,
		ClauseRaw: strings.TrimSpace(clauseRaw),
	}
}

func formatColumnOptionsForAlter(opts *ColumnOption) string {
	if opts == nil {
		return ""
	}
	parts := make([]string, 0, 5)
	if opts.PrimaryKey {
		parts = append(parts, "primary key")
	}
	if opts.Encode != "" {
		parts = append(parts, "encode '"+opts.Encode+"'")
	}
	if opts.Compress != "" {
		parts = append(parts, "compress '"+opts.Compress+"'")
	}
	if opts.CompressLevel != "" {
		parts = append(parts, "level '"+opts.CompressLevel+"'")
	}
	if opts.HasRef {
		ref := opts.RefColumn
		if opts.RefTable != "" {
			ref = opts.RefTable + "." + ref
		}
		if opts.RefDB != "" {
			ref = opts.RefDB + "." + ref
		}
		if ref != "" {
			parts = append(parts, "from "+ref)
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return " " + strings.Join(parts, " ")
}

type AlterDatabaseStmt struct {
	Name    string
	Options *DatabaseOptions
}

func (*AlterDatabaseStmt) iStatement() {}

func (s *AlterDatabaseStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("alter database %s", s.Name)
	appendDatabaseOptions(buf, s.Options)
}

func (s *AlterDatabaseStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewAlterDatabaseStmt(name string, opts *DatabaseOptions) *AlterDatabaseStmt {
	return &AlterDatabaseStmt{
		Name:    name,
		Options: opts,
	}
}

type RollupStmt struct {
	Scope string
	Name  string
	Start string
	End   string
}

func (*RollupStmt) iStatement() {}

func (s *RollupStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("rollup %s", s.Scope)
	if s.Name != "" {
		buf.Myprintf(" %s", s.Name)
	}
	if s.Start != "" {
		buf.Myprintf(" start with %s", s.Start)
	}
	if s.End != "" {
		buf.Myprintf(" end with %s", s.End)
	}
}

func (s *RollupStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewRollupStmt(scope string, name string, start string, end string) *RollupStmt {
	return &RollupStmt{
		Scope: scope,
		Name:  name,
		Start: start,
		End:   end,
	}
}
