package proto

import "github.com/taosdata/driver-go/v3/common/stmt"

type Stmt2InitRequest struct {
	ReqID               uint64 `json:"req_id"`
	SingleStbInsert     bool   `json:"single_stb_insert"`
	SingleTableBindOnce bool   `json:"single_table_bind_once"`
}

type Stmt2InitResponse struct {
	BaseResp
	StmtID uint64 `json:"stmt_id"`
}

type Stmt2PrepareRequest struct {
	ReqID     uint64 `json:"req_id"`
	StmtID    uint64 `json:"stmt_id"`
	SQL       string `json:"sql"`
	GetFields bool   `json:"get_fields"`
}

type Stmt2PrepareResponse struct {
	BaseResp
	StmtID      uint64                `json:"stmt_id"`
	IsInsert    bool                  `json:"is_insert"`
	Fields      []*stmt.Stmt2AllField `json:"fields"`
	FieldsCount int                   `json:"fields_count"`
}

type Stmt2ExecRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type Stmt2ExecResponse struct {
	BaseResp
	StmtID   uint64 `json:"stmt_id"`
	Affected int    `json:"affected"`
}

type Stmt2UseResultRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type Stmt2UseResultResponse struct {
	BaseResp
	StmtID           uint64   `json:"stmt_id"`
	ID               uint64   `json:"id"`
	FieldsCount      int      `json:"fields_count"`
	FieldsNames      []string `json:"fields_names"`
	FieldsTypes      []uint8  `json:"fields_types"`
	FieldsLengths    []int64  `json:"fields_lengths"`
	Precision        int      `json:"precision"`
	FieldsPrecisions []int64  `json:"fields_precisions"`
	FieldsScales     []int64  `json:"fields_scales"`
}

type Stmt2CloseRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type Stmt2CloseResponse struct {
	BaseResp
	StmtID uint64 `json:"stmt_id"`
}

type Stmt2BindResponse struct {
	BaseResp
	StmtID uint64 `json:"stmt_id"`
}
