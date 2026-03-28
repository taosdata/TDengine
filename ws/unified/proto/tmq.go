package proto

import (
	"encoding/json"

	commontmq "github.com/taosdata/driver-go/v3/common/tmq"
)

type SubscribeReq struct {
	ReqID                uint64            `json:"req_id"`
	User                 string            `json:"user"`
	Password             string            `json:"password"`
	DB                   string            `json:"db"`
	GroupID              string            `json:"group_id"`
	ClientID             string            `json:"client_id"`
	OffsetRest           string            `json:"offset_rest"`
	Topics               []string          `json:"topics"`
	AutoCommit           string            `json:"auto_commit"`
	AutoCommitIntervalMS string            `json:"auto_commit_interval_ms"`
	SnapshotEnable       string            `json:"snapshot_enable"`
	WithTableName        string            `json:"with_table_name"`
	SessionTimeoutMS     string            `json:"session_timeout_ms"`
	MaxPollIntervalMS    string            `json:"max_poll_interval_ms"`
	App                  string            `json:"app"`
	Connector            string            `json:"connector"`
	Config               map[string]string `json:"config"`
}

type SubscribeResp struct {
	BaseResp
}

type PollReq struct {
	ReqID        uint64 `json:"req_id"`
	BlockingTime int64  `json:"blocking_time"`
	MessageID    uint64 `json:"message_id"`
}

type PollResp struct {
	BaseResp
	HaveMessage bool   `json:"have_message"`
	Topic       string `json:"topic"`
	Database    string `json:"database"`
	VgroupID    int32  `json:"vgroup_id"`
	MessageType int32  `json:"message_type"`
	MessageID   uint64 `json:"message_id"`
	Offset      int64  `json:"offset"`
}

type FetchJSONMetaReq struct {
	ReqID     uint64 `json:"req_id"`
	MessageID uint64 `json:"message_id"`
}

type FetchJSONMetaResp struct {
	BaseResp
	MessageID uint64          `json:"message_id"`
	Data      json.RawMessage `json:"data"`
}

type FetchReq struct {
	ReqID     uint64 `json:"req_id"`
	MessageID uint64 `json:"message_id"`
}

type FetchResp struct {
	BaseResp
	MessageID     uint64   `json:"message_id"`
	Completed     bool     `json:"completed"`
	TableName     string   `json:"table_name"`
	Rows          int      `json:"rows"`
	FieldsCount   int      `json:"fields_count"`
	FieldsNames   []string `json:"fields_names"`
	FieldsTypes   []uint8  `json:"fields_types"`
	FieldsLengths []int64  `json:"fields_lengths"`
	Precision     int      `json:"precision"`
}

type FetchBlockReq struct {
	ReqID     uint64 `json:"req_id"`
	MessageID uint64 `json:"message_id"`
}

type CommitReq struct {
	ReqID     uint64 `json:"req_id"`
	MessageID uint64 `json:"message_id"`
}

type CommitResp struct {
	BaseResp
	MessageID uint64 `json:"message_id"`
}

type UnsubscribeReq struct {
	ReqID uint64 `json:"req_id"`
}

type UnsubscribeResp struct {
	BaseResp
}

type AssignmentReq struct {
	ReqID uint64 `json:"req_id"`
	Topic string `json:"topic"`
}

type AssignmentResp struct {
	BaseResp
	Assignment []commontmq.Assignment `json:"assignment"`
}

type OffsetSeekReq struct {
	ReqID    uint64 `json:"req_id"`
	Topic    string `json:"topic"`
	VgroupID int32  `json:"vgroup_id"`
	Offset   int64  `json:"offset"`
}

type OffsetSeekResp struct {
	BaseResp
}

type CommittedReq struct {
	ReqID          uint64          `json:"req_id"`
	TopicVgroupIDs []TopicVgroupID `json:"topic_vgroup_ids"`
}

type CommittedResp struct {
	BaseResp
	Committed []int64 `json:"committed"`
}

type TopicVgroupID struct {
	Topic    string `json:"topic"`
	VgroupID int32  `json:"vgroup_id"`
}

type CommitOffsetReq struct {
	ReqID    uint64 `json:"req_id"`
	Topic    string `json:"topic"`
	VgroupID int32  `json:"vgroup_id"`
	Offset   int64  `json:"offset"`
}

type CommitOffsetResp struct {
	BaseResp
	Topic    string `json:"topic"`
	VgroupID int32  `json:"vgroup_id"`
	Offset   int64  `json:"offset"`
}

type PositionReq struct {
	ReqID          uint64          `json:"req_id"`
	TopicVgroupIDs []TopicVgroupID `json:"topic_vgroup_ids"`
}

type PositionResp struct {
	BaseResp
	Position []int64 `json:"position"`
}

type FetchRawMetaReq struct {
	ReqID     uint64 `json:"req_id"`
	MessageID uint64 `json:"message_id"`
}
