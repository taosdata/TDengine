package sqlparser

const (
	TSDB_ALTER_ROLE_LOCK       = 0x1
	TSDB_ALTER_ROLE_ROLE       = 0x2
	TSDB_ALTER_ROLE_PRIVILEGES = 0x3
	TSDB_ALTER_ROLE_MAX        = 0x4 // increase according to actual use
)

// CreateRoleStmt 表示CREATE ROLE语句
type CreateRoleStmt struct {
	Name         string
	IgnoreExists bool
}

func (c *CreateRoleStmt) iStatement() {}

func (c *CreateRoleStmt) Format(buf *TrackedBuffer) {
	if c == nil {
		return
	}
	buf.Myprintf("create role ")
	if c.IgnoreExists {
		buf.Myprintf("if not exists ")
	}
	buf.Myprintf("%s", c.Name)
}

func (c *CreateRoleStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewCreateRoleStmt(lexer yyLexer, ignoreExists bool, roleName string) *CreateRoleStmt {
	return &CreateRoleStmt{
		Name:         roleName,
		IgnoreExists: ignoreExists,
	}
}

// DropRoleStmt 表示DROP ROLE语句
type DropRoleStmt struct {
	Name         string
	IgnoreExists bool
}

func (d *DropRoleStmt) iStatement() {}

func (d *DropRoleStmt) Format(buf *TrackedBuffer) {
	if d == nil {
		return
	}
	buf.Myprintf("drop role ")
	if d.IgnoreExists {
		buf.Myprintf("if exists ")
	}
	buf.Myprintf("%s", d.Name)
}

func (d *DropRoleStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewDropRoleStmt(lexer yyLexer, ignoreExists bool, roleName string) *DropRoleStmt {
	return &DropRoleStmt{
		Name:         roleName,
		IgnoreExists: ignoreExists,
	}
}

// AlterRoleStmt 表示ALTER ROLE语句
type AlterRoleStmt struct {
	Name   string
	Action int
	Value  Token
}

func (a *AlterRoleStmt) iStatement() {}

func (a *AlterRoleStmt) Format(buf *TrackedBuffer) {
	if a == nil {
		return
	}
	if a.Action == TSDB_ALTER_ROLE_LOCK {
		if a.Value.Type == 0 {
			buf.Myprintf("unlock role %s", a.Name)
		} else {
			buf.Myprintf("lock role %s", a.Name)
		}
		return
	}
	buf.Myprintf("alter role %s", a.Name)
}

func (a *AlterRoleStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewAlterRoleStmt(lexer yyLexer, roleName string, action int, value Token) *AlterRoleStmt {
	return &AlterRoleStmt{
		Name:   roleName,
		Action: action,
		Value:  value,
	}
}

// GrantRoleStmt 表示GRANT ROLE语句
type GrantRoleStmt struct {
	RoleName    string
	GranteeName string
	Action      int
}

func (g *GrantRoleStmt) iStatement() {}

func (g *GrantRoleStmt) Format(buf *TrackedBuffer) {
	if g == nil {
		return
	}
	buf.Myprintf("grant role %s to %s", g.RoleName, g.GranteeName)
}

func (g *GrantRoleStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewGrantRoleStmt(lexer yyLexer, roleName string, granteeName string, action int) *GrantRoleStmt {
	return &GrantRoleStmt{
		RoleName:    roleName,
		GranteeName: granteeName,
		Action:      action,
	}
}

// RevokeRoleStmt 表示REVOKE ROLE语句
type RevokeRoleStmt struct {
	RoleName    string
	RevokeeName string
	Action      int
}

func (r *RevokeRoleStmt) iStatement() {}

func (r *RevokeRoleStmt) Format(buf *TrackedBuffer) {
	if r == nil {
		return
	}
	buf.Myprintf("revoke role %s from %s", r.RoleName, r.RevokeeName)
}

func (r *RevokeRoleStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewRevokeRoleStmt(lexer yyLexer, roleName string, revokeeName string, action int) *RevokeRoleStmt {
	return &RevokeRoleStmt{
		RoleName:    roleName,
		RevokeeName: revokeeName,
		Action:      action,
	}
}
