package proto

type RespInterface interface {
	GetReqID() uint64
}

type BaseResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

func (b *BaseResp) GetCode() int {
	if b == nil {
		return 0
	}
	return b.Code
}

func (b *BaseResp) GetMessage() string {
	if b == nil {
		return ""
	}
	return b.Message
}

func (b *BaseResp) GetReqID() uint64 {
	if b == nil {
		return 0
	}
	return b.ReqID
}

type WSConnectReq struct {
	ReqID       uint64 `json:"req_id"`
	User        string `json:"user"`
	Password    string `json:"password"`
	DB          string `json:"db"`
	TZ          string `json:"tz"`
	App         string `json:"app"`
	Connector   string `json:"connector"`
	TOTPCode    string `json:"totp_code"`
	BearerToken string `json:"bearer_token"`
}

type WSConnectResp struct {
	BaseResp
}

type WSQueryReq struct {
	ReqID uint64 `json:"req_id"`
	SQL   string `json:"sql"`
}

type WSQueryResp struct {
	BaseResp
	ID               uint64   `json:"id"`
	IsUpdate         bool     `json:"is_update"`
	AffectedRows     int      `json:"affected_rows"`
	FieldsCount      int      `json:"fields_count"`
	FieldsNames      []string `json:"fields_names"`
	FieldsTypes      []uint8  `json:"fields_types"`
	FieldsLengths    []int64  `json:"fields_lengths"`
	Precision        int      `json:"precision"`
	FieldsPrecisions []int64  `json:"fields_precisions"`
	FieldsScales     []int64  `json:"fields_scales"`
}

type WSFetchReq struct {
	ReqID uint64 `json:"req_id"`
	ID    uint64 `json:"id"`
}

type WSFetchResp struct {
	BaseResp
	ID        uint64 `json:"id"`
	Completed bool   `json:"completed"`
	Lengths   []int  `json:"lengths"`
	Rows      int    `json:"rows"`
}

type WSFetchBlockReq struct {
	ReqID uint64 `json:"req_id"`
	ID    uint64 `json:"id"`
}

type WSFreeResultReq struct {
	ReqID uint64 `json:"req_id"`
	ID    uint64 `json:"id"`
}
