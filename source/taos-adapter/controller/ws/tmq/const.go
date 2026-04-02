package tmq

const TaosTMQKey = "taos_tmq"
const (
	TMQSubscribe          = "subscribe"
	TMQPoll               = "poll"
	TMQFetch              = "fetch" // fetch_raw_block
	TMQFetchBlock         = "fetch_block"
	TMQFetchRaw           = "fetch_raw" // tmq_get_raw
	TMQFetchJsonMeta      = "fetch_json_meta"
	TMQCommit             = "commit"
	TMQUnsubscribe        = "unsubscribe"
	TMQGetTopicAssignment = "assignment"
	TMQSeek               = "seek"
	TMQCommitOffset       = "commit_offset"
	TMQCommitted          = "committed"
	TMQPosition           = "position"
	TMQListTopics         = "list_topics"
	TMQFetchRawData       = "fetch_raw_data" // tmq_get_raw
)
const (
	TMQRawMessage         = 3
	TMQFetchRawNewMessage = 8
)

const OffsetInvalid = -2147467247

const (
	TmqConfUnknown int32 = -2
	TmqConfInvalid int32 = -1
	TmqConfOk      int32 = 0
)

const TsdbCodeInvalidPara = 0x0118
