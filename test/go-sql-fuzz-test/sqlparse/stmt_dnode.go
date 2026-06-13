package sqlparser

import (
	"fmt"
	"sqlparser/tool"
	"strconv"
)

type CreateDnodeStmt struct {
	Fqdn string
	Port int32
}

func (s *CreateDnodeStmt) iStatement() {}

func (s *CreateDnodeStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("create dnode %s", s.Fqdn)
	if s.Port >= 0 {
		buf.Myprintf(" port %s", strconv.FormatInt(int64(s.Port), 10))
	}
}

func (s *CreateDnodeStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewCreateDnodeStmt(lexer yyLexer, fqdn Token, port *Token) *CreateDnodeStmt {
	stmt := &CreateDnodeStmt{
		Fqdn: tool.BytesToString(fqdn.Bytes),
		Port: -1,
	}

	if port != nil {
		p, err := tool.ConvertBytesToInt32(port.Bytes)
		if err != nil {
			lexer.Error(fmt.Sprintf("can not parse port to int32: %s, err: %v", port.Bytes, err))
			return nil
		}
		stmt.Port = p
	}

	return stmt
}

type DropDnodeStmt struct {
	DnodeId int32
	Fqdn    string
	Port    int32
	Force   bool
	Unsafe  bool
}

func (s *DropDnodeStmt) iStatement() {}

func (s *DropDnodeStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("drop dnode ")
	if s.DnodeId >= 0 {
		buf.Myprintf("%s", strconv.FormatInt(int64(s.DnodeId), 10))
	} else {
		buf.Myprintf("%s", s.Fqdn)
	}
	if s.Force {
		buf.Myprintf(" force")
	}
	if s.Unsafe {
		buf.Myprintf(" unsafe")
	}
}

func (s *DropDnodeStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewDropDnodeStmt(lexer yyLexer, dnode Token, force bool, unsafe bool) *DropDnodeStmt {
	stmt := &DropDnodeStmt{
		DnodeId: -1,
		Port:    -1,
		Force:   force,
		Unsafe:  unsafe,
	}

	if dnode.Type == INTEGRALVALUE || dnode.Type == NK_INTEGER {
		// Parse as dnodeId
		dnodeId, err := tool.ConvertBytesToInt32(dnode.Bytes)
		if err != nil {
			lexer.Error(fmt.Sprintf("can not parse dnode id to int32: %s, err: %v", dnode.Bytes, err))
			return nil
		}
		stmt.DnodeId = dnodeId
	} else {
		// Parse as fqdn
		stmt.Fqdn = tool.BytesToString(dnode.Bytes)
		// Note: The original C code uses checkAndSplitEndpoint, but we'll handle this in the parser
	}

	return stmt
}

type AlterDnodeStmt struct {
	DnodeId int32
	Config  string
	Value   string
}

func (s *AlterDnodeStmt) iStatement() {}

func (s *AlterDnodeStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	if s.DnodeId < 0 && s.Config == "\"encrypt_key\"" && s.Value != "" {
		buf.Myprintf("create encrypt_key '%s'", s.Value)
		return
	}
	if s.DnodeId < 0 && s.Config == "dnodes" && s.Value != "" {
		buf.Myprintf("alter all dnodes '%s' ''", s.Value)
		return
	}
	if s.DnodeId >= 0 {
		buf.Myprintf("alter dnode %s", strconv.FormatInt(int64(s.DnodeId), 10))
	} else {
		buf.Myprintf("alter all dnodes")
	}
	if s.Config != "" {
		buf.Myprintf(" '%s'", s.Config)
	}
	if s.Value != "" {
		buf.Myprintf(" '%s'", s.Value)
	}
}

func (s *AlterDnodeStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewAlterDnodeStmt(lexer yyLexer, dnodeId *Token, config Token, value *Token) *AlterDnodeStmt {
	stmt := &AlterDnodeStmt{
		DnodeId: -1,
		Config:  tool.BytesToString(config.Bytes),
		Value:   "",
	}

	if dnodeId != nil {
		id, err := tool.ConvertBytesToInt32(dnodeId.Bytes)
		if err != nil {
			lexer.Error(fmt.Sprintf("can not parse dnode id to int32: %s, err: %v", dnodeId.Bytes, err))
			return nil
		}
		stmt.DnodeId = id
	}

	if value != nil {
		stmt.Value = tool.BytesToString(value.Bytes)
	}

	return stmt
}

type RestoreDnodeStmt struct {
	DnodeId int32
}

func (s *RestoreDnodeStmt) iStatement() {}

func (s *RestoreDnodeStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("restore dnode %s", strconv.FormatInt(int64(s.DnodeId), 10))
}

func (s *RestoreDnodeStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewRestoreDnodeStmt(lexer yyLexer, dnodeId Token) *RestoreDnodeStmt {
	id, err := tool.ConvertBytesToInt32(dnodeId.Bytes)
	if err != nil {
		lexer.Error(fmt.Sprintf("can not parse dnode id to int32: %s, err: %v", dnodeId.Bytes, err))
		return nil
	}

	return &RestoreDnodeStmt{
		DnodeId: id,
	}
}

func NewCreateEncryptKeyStmt(lexer yyLexer, value Token) *CreateEncryptKeyStmt {
	return &CreateEncryptKeyStmt{
		DnodeId: -1,
		Config:  "\"encrypt_key\"",
		Value:   tool.BytesToString(value.Bytes),
	}
}

type AlterClusterStmt struct {
	Config string
	Value  *string
}

func (s *AlterClusterStmt) iStatement() {}

func (s *AlterClusterStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("alter cluster '%s'", s.Config)
	if s.Value != nil {
		buf.Myprintf(" '%s'", *s.Value)
	}
}

func (s *AlterClusterStmt) walkSubtree(visit Visit) error {
	return nil
}

type AlterLocalStmt struct {
	Config string
	Value  *string
}

func (s *AlterLocalStmt) iStatement() {}

func (s *AlterLocalStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("alter local '%s'", s.Config)
	if s.Value != nil {
		buf.Myprintf(" '%s'", *s.Value)
	}
}

func (s *AlterLocalStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewAlterClusterStmt(lexer yyLexer, config Token, value *Token) *AlterClusterStmt {
	stmt := &AlterClusterStmt{
		Config: tool.BytesToString(config.Bytes),
	}
	if value != nil {
		val := tool.BytesToString(value.Bytes)
		stmt.Value = &val
	}
	return stmt
}

func NewAlterLocalStmt(lexer yyLexer, config Token, value *Token) *AlterLocalStmt {
	stmt := &AlterLocalStmt{
		Config: tool.BytesToString(config.Bytes),
	}
	if value != nil {
		val := tool.BytesToString(value.Bytes)
		stmt.Value = &val
	}
	return stmt
}

type AlterDnodesReloadStmt struct {
	Name string
}

func (s *AlterDnodesReloadStmt) iStatement() {}

func (s *AlterDnodesReloadStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("alter dnodes reload %s", s.Name)
}

func (s *AlterDnodesReloadStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewAlterDnodesReloadStmt(lexer yyLexer, name Token) *AlterDnodesReloadStmt {
	return &AlterDnodesReloadStmt{
		Name: tool.BytesToString(name.Bytes),
	}
}
