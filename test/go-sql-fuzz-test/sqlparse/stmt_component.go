package sqlparser

import (
	"fmt"
	"sqlparser/tool"
	"strconv"
)

const (
	NodeTypeComponent NodeType = iota
)

type CreateComponentNodeStmt struct {
	Type    NodeType
	DnodeId int32
}

func (s *CreateComponentNodeStmt) iStatement() {}

func (s *CreateComponentNodeStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	node := "qnode"
	switch s.Type {
	case QUERY_NODE_CREATE_SNODE_STMT:
		node = "snode"
	case QUERY_NODE_CREATE_MNODE_STMT:
		node = "mnode"
	case QUERY_NODE_CREATE_QNODE_STMT:
		node = "qnode"
	}
	buf.Myprintf("create %s on dnode %s", node, strconv.FormatInt(int64(s.DnodeId), 10))
}

func (s *CreateComponentNodeStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewCreateComponentNodeStmt(lexer yyLexer, nodeType NodeType, dnodeId Token) *CreateComponentNodeStmt {
	d, err := tool.ConvertBytesToInt32(dnodeId.Bytes)
	if err != nil {
		lexer.Error(fmt.Sprintf("can not parse dnode_id to int32: %s, err: %v", dnodeId.Bytes, err))
		return nil
	}
	stmt := &CreateComponentNodeStmt{
		Type:    nodeType,
		DnodeId: d,
	}
	return stmt
}

type DropComponentNodeStmt struct {
	Type    NodeType
	DnodeId int32
}

func (s *DropComponentNodeStmt) iStatement() {}

func (s *DropComponentNodeStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	node := "qnode"
	switch s.Type {
	case QUERY_NODE_DROP_SNODE_STMT:
		node = "snode"
	case QUERY_NODE_DROP_MNODE_STMT:
		node = "mnode"
	case QUERY_NODE_DROP_QNODE_STMT:
		node = "qnode"
	}
	buf.Myprintf("drop %s on dnode %s", node, strconv.FormatInt(int64(s.DnodeId), 10))
}

func (s *DropComponentNodeStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewDropComponentNodeStmt(lexer yyLexer, nodeType NodeType, dnodeId Token) *DropComponentNodeStmt {
	d, err := tool.ConvertBytesToInt32(dnodeId.Bytes)
	if err != nil {
		lexer.Error(fmt.Sprintf("can not parse dnode_id to int32: %s, err: %v", dnodeId.Bytes, err))
		return nil
	}
	stmt := &DropComponentNodeStmt{
		Type:    nodeType,
		DnodeId: d,
	}
	return stmt
}

type RestoreComponentNodeStmt struct {
	Type    NodeType
	DnodeId int32
}

func (s *RestoreComponentNodeStmt) iStatement() {}

func (s *RestoreComponentNodeStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	node := "qnode"
	switch s.Type {
	case QUERY_NODE_RESTORE_MNODE_STMT:
		node = "mnode"
	case QUERY_NODE_RESTORE_VNODE_STMT:
		node = "vnode"
	case QUERY_NODE_RESTORE_QNODE_STMT:
		node = "qnode"
	}
	buf.Myprintf("restore %s on dnode %s", node, strconv.FormatInt(int64(s.DnodeId), 10))
}

func (s *RestoreComponentNodeStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewRestoreComponentNodeStmt(lexer yyLexer, nodeType NodeType, dnodeId Token) *RestoreComponentNodeStmt {
	d, err := tool.ConvertBytesToInt32(dnodeId.Bytes)
	if err != nil {
		lexer.Error(fmt.Sprintf("can not parse dnode_id to int32: %s, err: %v", dnodeId.Bytes, err))
		return nil
	}
	stmt := &RestoreComponentNodeStmt{
		Type:    nodeType,
		DnodeId: d,
	}
	return stmt
}

type BnodeOptions struct {
	ProtoStr string
	Proto    int8
}

type CreateBnodeStmt struct {
	DnodeId int32
	Options BnodeOptions
}

func (s *CreateBnodeStmt) iStatement() {}

func (s *CreateBnodeStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("create bnode on dnode %s", strconv.FormatInt(int64(s.DnodeId), 10))
	if s.Options.ProtoStr != "" {
		buf.Myprintf(" protocol %s", s.Options.ProtoStr)
	}
}

func (s *CreateBnodeStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewCreateBnodeStmt(lexer yyLexer, dnodeId Token, options *BnodeOptions) *CreateBnodeStmt {
	d, err := tool.ConvertBytesToInt32(dnodeId.Bytes)
	if err != nil {
		lexer.Error(fmt.Sprintf("can not parse dnode_id to int32: %s, err: %v", dnodeId.Bytes, err))
		return nil
	}
	stmt := &CreateBnodeStmt{
		DnodeId: d,
		Options: *options,
	}
	return stmt
}

type DropBnodeStmt struct {
	DnodeId int32
}

func (s *DropBnodeStmt) iStatement() {}

func (s *DropBnodeStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("drop bnode on dnode %s", strconv.FormatInt(int64(s.DnodeId), 10))
}

func (s *DropBnodeStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewDropBnodeStmt(lexer yyLexer, dnodeId Token) *DropBnodeStmt {
	d, err := tool.ConvertBytesToInt32(dnodeId.Bytes)
	if err != nil {
		lexer.Error(fmt.Sprintf("can not parse dnode_id to int32: %s, err: %v", dnodeId.Bytes, err))
		return nil
	}
	stmt := &DropBnodeStmt{
		DnodeId: d,
	}
	return stmt
}

// CreateDefaultBnodeOptions creates default BnodeOptions
func CreateDefaultBnodeOptions() *BnodeOptions {
	return &BnodeOptions{
		ProtoStr: "",
		Proto:    0,
	}
}

// SetBnodeOption sets an option for BnodeOptions
func SetBnodeOption(options *BnodeOptions, optionName string, optionValue string) *BnodeOptions {
	// Create a new options object to avoid modifying the original
	newOptions := &BnodeOptions{
		ProtoStr: options.ProtoStr,
		Proto:    options.Proto,
	}

	// Currently only supporting protocol option
	if optionName == "PROTOCOL" {
		newOptions.ProtoStr = optionValue
		newOptions.Proto = 1 // Assuming 1 is the value for default protocol
	}

	return newOptions
}
