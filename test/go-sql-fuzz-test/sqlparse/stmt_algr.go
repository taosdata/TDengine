package sqlparser

import (
	"sqlparser/tool"
)

type CreateEncryptAlgrStmt struct {
	AlgorithmId  string
	Name         string
	Desc         string
	AlgrType     string
	OsslAlgrName string
}

func (s *CreateEncryptAlgrStmt) iStatement() {}

func (s *CreateEncryptAlgrStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("create encrypt_algr '%s'", s.AlgorithmId)
	if s.Name != "" {
		buf.Myprintf(" algr_name '%s'", s.Name)
	}
	if s.Desc != "" {
		buf.Myprintf(" desc '%s'", s.Desc)
	}
	if s.AlgrType != "" {
		buf.Myprintf(" algr_type '%s'", s.AlgrType)
	}
	if s.OsslAlgrName != "" {
		buf.Myprintf(" ossl_algr_name '%s'", s.OsslAlgrName)
	}
}

func (s *CreateEncryptAlgrStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewCreateAlgrStmt(lexer yyLexer, algorithmId, name, desc, algrType, osslAlgrName Token) *CreateEncryptAlgrStmt {
	return &CreateEncryptAlgrStmt{
		AlgorithmId:  tool.BytesToString(algorithmId.Bytes),
		Name:         tool.BytesToString(name.Bytes),
		Desc:         tool.BytesToString(desc.Bytes),
		AlgrType:     tool.BytesToString(algrType.Bytes),
		OsslAlgrName: tool.BytesToString(osslAlgrName.Bytes),
	}
}

type DropEncryptAlgrStmt struct {
	AlgorithmId string
}

func (s *DropEncryptAlgrStmt) iStatement() {}

func (s *DropEncryptAlgrStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("drop encrypt_algr '%s'", s.AlgorithmId)
}

func (s *DropEncryptAlgrStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewDropAlgrStmt(lexer yyLexer, algorithmId Token) *DropEncryptAlgrStmt {
	return &DropEncryptAlgrStmt{
		AlgorithmId: tool.BytesToString(algorithmId.Bytes),
	}
}

type AlterEncryptKeyStmt struct {
	KeyType int8
	NewKey  string
}

func (s *AlterEncryptKeyStmt) iStatement() {}

func (s *AlterEncryptKeyStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	key := "svr_key"
	if s.KeyType != 0 {
		key = "db_key"
	}
	buf.Myprintf("alter system set %s '%s'", key, s.NewKey)
}

func (s *AlterEncryptKeyStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewAlterEncryptKeyStmt(lexer yyLexer, keyType int8, newKey Token) *AlterEncryptKeyStmt {
	return &AlterEncryptKeyStmt{
		KeyType: keyType,
		NewKey:  tool.BytesToString(newKey.Bytes),
	}
}
