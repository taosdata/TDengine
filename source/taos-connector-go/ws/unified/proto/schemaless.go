package proto

type SchemalessWriteRequest struct {
	ReqID        uint64 `json:"req_id"`
	Protocol     int    `json:"protocol"`
	Precision    string `json:"precision"`
	TTL          int    `json:"ttl"`
	Data         string `json:"data"`
	TableNameKey string `json:"table_name_key"`
}

type SchemalessWriteResponse struct {
	BaseResp
	AffectedRows int   `json:"affected_rows"`
	TotalRows    int32 `json:"total_rows"`
}
