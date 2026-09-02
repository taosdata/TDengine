package sqlparser

import (
	"fmt"
	"sqlparser/tool"
	"strconv"
)

type TokenOptions struct {
	HasEnable    bool
	HasTTL       bool
	HasProvider  bool
	HasExtraInfo bool
	Enable       int8
	TTL          int32
	Provider     string
	ExtraInfo    string
}

func CreateDefaultTokenOptions() *TokenOptions {
	return &TokenOptions{
		Enable: 1,
		TTL:    0,
	}
}

func MergeTokenOptions(lexer yyLexer, a, b *TokenOptions) *TokenOptions {
	if a == nil && b == nil {
		return CreateDefaultTokenOptions()
	}
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if b.HasEnable {
		a.HasEnable = true
		a.Enable = b.Enable
	}
	if b.HasTTL {
		a.HasTTL = true
		a.TTL = b.TTL
	}
	if b.HasProvider {
		a.HasProvider = true
		a.Provider = b.Provider
	}
	if b.HasExtraInfo {
		a.HasExtraInfo = true
		a.ExtraInfo = b.ExtraInfo
	}
	return a
}

func (opt *TokenOptions) SetProvider(lexer yyLexer, provider Token) {
	opt.HasProvider = true
	opt.Provider = tool.BytesToString(provider.Bytes)
}

func (opt *TokenOptions) SetEnable(lexer yyLexer, enable Token) {
	opt.HasEnable = true
	val, err := tool.ConvertBytesToInt8(enable.Bytes)
	if err != nil {
		lexer.Error(fmt.Sprintf("invalid ENABLE value: %s, error: %v", enable.Bytes, err))
	}
	opt.Enable = val
}

func (opt *TokenOptions) SetTTL(lexer yyLexer, ttl Token) {
	opt.HasTTL = true
	val, err := tool.ConvertBytesToInt32(ttl.Bytes)
	if err != nil {
		lexer.Error(fmt.Sprintf("invalid TTL value: %s, error: %v", ttl.Bytes, err))
	}
	// Lemon stores TTL in seconds; SQL input TTL is in days.
	opt.TTL = val * 86400
}

func (opt *TokenOptions) SetExtraInfo(lexer yyLexer, extraInfo Token) {
	opt.HasExtraInfo = true
	opt.ExtraInfo = tool.BytesToString(extraInfo.Bytes)
}

type CreateTokenStmt struct {
	Name         string
	User         string
	Enable       int8
	IgnoreExists bool
	TTL          int32
	Provider     string
	ExtraInfo    string
}

func (c *CreateTokenStmt) iStatement() {}

func (c *CreateTokenStmt) Format(buf *TrackedBuffer) {
	if c == nil {
		return
	}
	buf.Myprintf("create token ")
	if c.IgnoreExists {
		buf.Myprintf("if not exists ")
	}
	buf.Myprintf("%s from user %s", c.Name, c.User)
	if c.Provider != "" {
		buf.Myprintf(" provider '%s'", c.Provider)
	}
	if c.ExtraInfo != "" {
		buf.Myprintf(" extra_info '%s'", c.ExtraInfo)
	}
	if c.Enable != 1 {
		buf.Myprintf(" enable %s", strconv.FormatInt(int64(c.Enable), 10))
	}
	if c.TTL > 0 {
		buf.Myprintf(" ttl %s", strconv.FormatInt(int64(c.TTL/86400), 10))
	}
}

func (c *CreateTokenStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewCreateTokenStmt(lexer yyLexer, tokenID Token, userName string, opts *TokenOptions, ignoreExists bool) *CreateTokenStmt {
	if opts == nil {
		opts = CreateDefaultTokenOptions()
	}
	return &CreateTokenStmt{
		Name:         tool.BytesToString(tokenID.Bytes),
		User:         userName,
		Enable:       opts.Enable,
		IgnoreExists: ignoreExists,
		TTL:          opts.TTL,
		Provider:     opts.Provider,
		ExtraInfo:    opts.ExtraInfo,
	}
}

type AlterTokenStmt struct {
	Name      string
	Enable    int8
	TTL       int32
	Provider  string
	ExtraInfo string
}

func (a *AlterTokenStmt) iStatement() {}

func (a *AlterTokenStmt) Format(buf *TrackedBuffer) {
	if a == nil {
		return
	}
	buf.Myprintf("alter token %s", a.Name)
	if a.Provider != "" {
		buf.Myprintf(" provider '%s'", a.Provider)
	}
	if a.ExtraInfo != "" {
		buf.Myprintf(" extra_info '%s'", a.ExtraInfo)
	}
	if a.Enable != 1 {
		buf.Myprintf(" enable %s", strconv.FormatInt(int64(a.Enable), 10))
	}
	if a.TTL > 0 {
		buf.Myprintf(" ttl %s", strconv.FormatInt(int64(a.TTL/86400), 10))
	}
	if a.Provider == "" && a.ExtraInfo == "" && a.Enable == 1 && a.TTL == 0 {
		buf.Myprintf(" enable 1")
	}
}

func (a *AlterTokenStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewAlterTokenStmt(lexer yyLexer, tokenID Token, opts *TokenOptions) *AlterTokenStmt {
	if opts == nil {
		opts = CreateDefaultTokenOptions()
	}
	return &AlterTokenStmt{
		Name:      tool.BytesToString(tokenID.Bytes),
		Enable:    opts.Enable,
		TTL:       opts.TTL,
		Provider:  opts.Provider,
		ExtraInfo: opts.ExtraInfo,
	}
}

type DropTokenStmt struct {
	Name         string
	IgnoreExists bool
}

func (d *DropTokenStmt) iStatement() {}

func (d *DropTokenStmt) Format(buf *TrackedBuffer) {
	if d == nil {
		return
	}
	buf.Myprintf("drop token ")
	if d.IgnoreExists {
		buf.Myprintf("if exists ")
	}
	buf.Myprintf("%s", d.Name)
}

func (d *DropTokenStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewDropTokenStmt(lexer yyLexer, tokenID Token, ignoreExists bool) *DropTokenStmt {
	return &DropTokenStmt{
		Name:         tool.BytesToString(tokenID.Bytes),
		IgnoreExists: ignoreExists,
	}
}
