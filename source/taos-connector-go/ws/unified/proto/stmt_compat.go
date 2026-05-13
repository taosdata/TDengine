package proto

import stmtCommon "github.com/taosdata/driver-go/v3/common/stmt"

// Legacy stmt compatibility protocol types used by taosWS compatibility aliases.
type StmtPrepareRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
	SQL    string `json:"sql"`
}

type StmtPrepareResponse struct {
	BaseResp
	StmtID   uint64 `json:"stmt_id"`
	IsInsert bool   `json:"is_insert"`
}

type StmtInitReq struct {
	ReqID uint64 `json:"req_id"`
}

type StmtInitResp struct {
	BaseResp
	StmtID uint64 `json:"stmt_id"`
}

type StmtCloseRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type StmtCloseResponse struct {
	BaseResp
	StmtID uint64 `json:"stmt_id,omitempty"`
}

type StmtGetColFieldsRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type StmtGetColFieldsResponse struct {
	BaseResp
	StmtID uint64                  `json:"stmt_id"`
	Fields []*stmtCommon.StmtField `json:"fields"`
}

type StmtBindResponse struct {
	BaseResp
	StmtID uint64 `json:"stmt_id"`
}

type StmtAddBatchRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type StmtAddBatchResponse struct {
	BaseResp
	StmtID uint64 `json:"stmt_id"`
}

type StmtExecRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type StmtExecResponse struct {
	BaseResp
	StmtID   uint64 `json:"stmt_id"`
	Affected int    `json:"affected"`
}

type StmtUseResultRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type StmtUseResultResponse struct {
	BaseResp
	StmtID           uint64   `json:"stmt_id"`
	ResultID         uint64   `json:"result_id"`
	FieldsCount      int      `json:"fields_count"`
	FieldsNames      []string `json:"fields_names"`
	FieldsTypes      []uint8  `json:"fields_types"`
	FieldsLengths    []int64  `json:"fields_lengths"`
	Precision        int      `json:"precision"`
	FieldsPrecisions []int64  `json:"fields_precisions"`
	FieldsScales     []int64  `json:"fields_scales"`
}
