package sqlparser

import "strings"
import "strconv"

type ShowStmt struct {
	Kind      string
	DBKind    string
	DBName    string
	Table     string
	TableKind string
	TagItems  []string
	Object    string
	ID        int32
	HasID     bool
	Pattern   string
}

type ShowTableScope struct {
	TableKind string
	DBName    string
}

func (s *ShowStmt) iStatement() {}

func (s *ShowStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	switch s.Kind {
	case "databases":
		if s.DBKind != "" {
			buf.Myprintf("show %s databases", s.DBKind)
		} else {
			buf.Myprintf("show databases")
		}
	case "users_full":
		buf.Myprintf("show users full")
	case "user_privileges":
		buf.Myprintf("show user privileges")
	case "role_privileges":
		buf.Myprintf("show role privileges")
	case "role_column_privileges":
		buf.Myprintf("show role column privileges")
	case "grants_full":
		buf.Myprintf("show grants full")
	case "grants_logs":
		buf.Myprintf("show grants logs")
	case "cluster_alive":
		buf.Myprintf("show cluster alive")
	case "dnode_variables":
		buf.Myprintf("show dnode %s variables", strconv.FormatInt(int64(s.ID), 10))
		if s.Pattern != "" {
			buf.Myprintf(" like '%s'", s.Pattern)
		}
	case "transaction", "scan", "compact", "retention":
		buf.Myprintf("show %s %s", s.Kind, strconv.FormatInt(int64(s.ID), 10))
	case "vnodes":
		if s.HasID {
			buf.Myprintf("show vnodes on dnode %s", strconv.FormatInt(int64(s.ID), 10))
		} else {
			buf.Myprintf("show vnodes")
		}
	case "streams", "vgroups", "alive", "views", "disk_info", "rsmas", "retentions", "tsmas":
		if s.DBName != "" {
			buf.Myprintf("show %s. %s", s.DBName, s.Kind)
		} else {
			buf.Myprintf("show %s", s.Kind)
		}
		if s.Pattern != "" {
			buf.Myprintf(" like '%s'", s.Pattern)
		}
	case "indexes", "tags":
		buf.Myprintf("show %s from", s.Kind)
		if s.DBName != "" {
			buf.Myprintf(" %s.%s", s.DBName, s.Table)
		} else {
			buf.Myprintf(" %s", s.Table)
		}
	case "table_tags":
		buf.Myprintf("show table tags")
		if len(s.TagItems) > 0 {
			buf.Myprintf(" %s", strings.Join(s.TagItems, ","))
		}
		buf.Myprintf(" from")
		if s.DBName != "" {
			buf.Myprintf(" %s.%s", s.DBName, s.Table)
		} else {
			buf.Myprintf(" %s", s.Table)
		}
	case "table_distributed":
		buf.Myprintf("show table distributed %s", s.Object)
	case "show_create_database":
		buf.Myprintf("show create database %s", s.Object)
	case "show_create_table":
		buf.Myprintf("show create table %s", s.Object)
	case "show_create_vtable":
		buf.Myprintf("show create vtable %s", s.Object)
	case "show_create_stable":
		buf.Myprintf("show create stable %s", s.Object)
	case "show_create_view":
		buf.Myprintf("show create view %s", s.Object)
	case "show_create_rsma":
		buf.Myprintf("show create rsma %s", s.Object)
	case "xnode":
		buf.Myprintf("show xnode %s", s.Object)
	case "tables", "stables", "vtables":
		buf.Myprintf("show")
		if s.TableKind != "" {
			buf.Myprintf(" %s", s.TableKind)
		}
		if s.DBName != "" {
			buf.Myprintf(" %s.", s.DBName)
		}
		buf.Myprintf(" %s", s.Kind)
		if s.Pattern != "" {
			buf.Myprintf(" like '%s'", s.Pattern)
		}
	case "variables":
		buf.Myprintf("show variables")
		if s.Pattern != "" {
			buf.Myprintf(" like '%s'", s.Pattern)
		}
	case "local_variables":
		buf.Myprintf("show local variables")
		if s.Pattern != "" {
			buf.Myprintf(" like '%s'", s.Pattern)
		}
	case "instances":
		buf.Myprintf("show instances")
		if s.Pattern != "" {
			buf.Myprintf(" like '%s'", s.Pattern)
		}
	default:
		buf.Myprintf("show %s", s.Kind)
	}
}

func (s *ShowStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewShowStmt(kind string) *ShowStmt {
	return &ShowStmt{Kind: kind}
}

func NewShowDatabasesStmt(dbKind string) *ShowStmt {
	return &ShowStmt{Kind: "databases", DBKind: dbKind}
}

func NewShowStmtWithPattern(kind string, pattern string) *ShowStmt {
	return &ShowStmt{Kind: kind, Pattern: pattern}
}

func NewShowStmtWithDB(kind string, dbName string) *ShowStmt {
	return &ShowStmt{Kind: kind, DBName: dbName}
}

func NewShowStmtWithDBPattern(kind string, dbName string, pattern string) *ShowStmt {
	return &ShowStmt{Kind: kind, DBName: dbName, Pattern: pattern}
}

func NewShowStmtWithTableDB(kind string, table string, dbName string) *ShowStmt {
	return &ShowStmt{Kind: kind, Table: table, DBName: dbName}
}

func NewShowStmtWithTableScope(kind string, scope ShowTableScope, pattern string) *ShowStmt {
	return &ShowStmt{Kind: kind, TableKind: scope.TableKind, DBName: scope.DBName, Pattern: pattern}
}

func NewShowStmtWithTableDBTags(kind string, table string, dbName string, tags []string) *ShowStmt {
	return &ShowStmt{Kind: kind, Table: table, DBName: dbName, TagItems: tags}
}

func NewShowStmtWithObject(kind string, object string) *ShowStmt {
	return &ShowStmt{Kind: kind, Object: object}
}

func NewShowStmtWithID(kind string, id int32) *ShowStmt {
	return &ShowStmt{Kind: kind, ID: id, HasID: true}
}

func NewShowStmtWithIDPattern(kind string, id int32, pattern string) *ShowStmt {
	return &ShowStmt{Kind: kind, ID: id, HasID: true, Pattern: pattern}
}
