package httperror
// Code generated from TDengine. DO NOT EDIT.

const (
	SUCCESS                      = 0x0
	TSDB_CODE_RPC_AUTH_FAILURE   = 0x0003
	HTTP_SERVER_OFFLINE          = 0x1100
	HTTP_UNSUPPORTED_URL         = 0x1101
	HTTP_INVALID_URL             = 0x1102
	HTTP_NO_ENOUGH_MEMORY        = 0x1103
	HTTP_REQUSET_TOO_BIG         = 0x1104
	HTTP_NO_AUTH_INFO            = 0x1105
	HTTP_NO_MSG_INPUT            = 0x1106
	HTTP_NO_SQL_INPUT            = 0x1107
	HTTP_NO_EXEC_USEDB           = 0x1108
	HTTP_SESSION_FULL            = 0x1109
	HTTP_GEN_TAOSD_TOKEN_ERR     = 0x110A
	HTTP_INVALID_MULTI_REQUEST   = 0x110B
	HTTP_CREATE_GZIP_FAILED      = 0x110C
	HTTP_FINISH_GZIP_FAILED      = 0x110D
	HTTP_LOGIN_FAILED            = 0x110E
	HTTP_INVALID_VERSION         = 0x1120
	HTTP_INVALID_CONTENT_LENGTH  = 0x1121
	HTTP_INVALID_AUTH_TYPE       = 0x1122
	HTTP_INVALID_AUTH_FORMAT     = 0x1123
	HTTP_INVALID_BASIC_AUTH      = 0x1124
	HTTP_INVALID_TAOSD_AUTH      = 0x1125
	HTTP_PARSE_METHOD_FAILED     = 0x1126
	HTTP_PARSE_TARGET_FAILED     = 0x1127
	HTTP_PARSE_VERSION_FAILED    = 0x1128
	HTTP_PARSE_SP_FAILED         = 0x1129
	HTTP_PARSE_STATUS_FAILED     = 0x112A
	HTTP_PARSE_PHRASE_FAILED     = 0x112B
	HTTP_PARSE_CRLF_FAILED       = 0x112C
	HTTP_PARSE_HEADER_FAILED     = 0x112D
	HTTP_PARSE_HEADER_KEY_FAILED = 0x112E
	HTTP_PARSE_HEADER_VAL_FAILED = 0x112F
	HTTP_PARSE_CHUNK_SIZE_FAILED = 0x1130
	HTTP_PARSE_CHUNK_FAILED      = 0x1131
	HTTP_PARSE_END_FAILED        = 0x1132
	HTTP_PARSE_INVALID_STATE     = 0x1134
	HTTP_PARSE_ERROR_STATE       = 0x1135
	HTTP_GC_QUERY_NULL           = 0x1150
	HTTP_GC_QUERY_SIZE           = 0x1151
	HTTP_GC_REQ_PARSE_ERROR      = 0x1152
	HTTP_TG_DB_NOT_INPUT         = 0x1160
	HTTP_TG_DB_TOO_LONG          = 0x1161
	HTTP_TG_INVALID_JSON         = 0x1162
	HTTP_TG_METRICS_NULL         = 0x1163
	HTTP_TG_METRICS_SIZE         = 0x1164
	HTTP_TG_METRIC_NULL          = 0x1165
	HTTP_TG_METRIC_TYPE          = 0x1166
	HTTP_TG_METRIC_NAME_NULL     = 0x1167
	HTTP_TG_METRIC_NAME_LONG     = 0x1168
	HTTP_TG_TIMESTAMP_NULL       = 0x1169
	HTTP_TG_TIMESTAMP_TYPE       = 0x116A
	HTTP_TG_TIMESTAMP_VAL_NULL   = 0x116B
	HTTP_TG_TAGS_NULL            = 0x116C
	HTTP_TG_TAGS_SIZE_0          = 0x116D
	HTTP_TG_TAGS_SIZE_LONG       = 0x116E
	HTTP_TG_TAG_NULL             = 0x116F
	HTTP_TG_TAG_NAME_NULL        = 0x1170
	HTTP_TG_TAG_NAME_SIZE        = 0x1171
	HTTP_TG_TAG_VALUE_TYPE       = 0x1172
	HTTP_TG_TAG_VALUE_NULL       = 0x1173
	HTTP_TG_TABLE_NULL           = 0x1174
	HTTP_TG_TABLE_SIZE           = 0x1175
	HTTP_TG_FIELDS_NULL          = 0x1176
	HTTP_TG_FIELDS_SIZE_0        = 0x1177
	HTTP_TG_FIELDS_SIZE_LONG     = 0x1178
	HTTP_TG_FIELD_NULL           = 0x1179
	HTTP_TG_FIELD_NAME_NULL      = 0x117A
	HTTP_TG_FIELD_NAME_SIZE      = 0x117B
	HTTP_TG_FIELD_VALUE_TYPE     = 0x117C
	HTTP_TG_FIELD_VALUE_NULL     = 0x117D
	HTTP_TG_HOST_NOT_STRING      = 0x117E
	HTTP_TG_STABLE_NOT_EXIST     = 0x117F
	HTTP_OP_DB_NOT_INPUT         = 0x1190
	HTTP_OP_DB_TOO_LONG          = 0x1191
	HTTP_OP_INVALID_JSON         = 0x1192
	HTTP_OP_METRICS_NULL         = 0x1193
	HTTP_OP_METRICS_SIZE         = 0x1194
	HTTP_OP_METRIC_NULL          = 0x1195
	HTTP_OP_METRIC_TYPE          = 0x1196
	HTTP_OP_METRIC_NAME_NULL     = 0x1197
	HTTP_OP_METRIC_NAME_LONG     = 0x1198
	HTTP_OP_TIMESTAMP_NULL       = 0x1199
	HTTP_OP_TIMESTAMP_TYPE       = 0x119A
	HTTP_OP_TIMESTAMP_VAL_NULL   = 0x119B
	HTTP_OP_TAGS_NULL            = 0x119C
	HTTP_OP_TAGS_SIZE_0          = 0x119D
	HTTP_OP_TAGS_SIZE_LONG       = 0x119E
	HTTP_OP_TAG_NULL             = 0x119F
	HTTP_OP_TAG_NAME_NULL        = 0x11A0
	HTTP_OP_TAG_NAME_SIZE        = 0x11A1
	HTTP_OP_TAG_VALUE_TYPE       = 0x11A2
	HTTP_OP_TAG_VALUE_NULL       = 0x11A3
	HTTP_OP_TAG_VALUE_TOO_LONG   = 0x11A4
	HTTP_OP_VALUE_NULL           = 0x11A5
	HTTP_OP_VALUE_TYPE           = 0x11A6
	HTTP_REQUEST_JSON_ERROR      = 0x1F00

	TSDB_CODE_MND_DB_NOT_EXIST = 0x0388
	MND_INVALID_TABLE_NAME     = 0x0362
	TSC_INVALID_TABLE_NAME     = 0x0218
	PAR_TABLE_NOT_EXIST        = 0x2603
)

// 401
const (
	TSDB_CODE_MND_USER_ALREADY_EXIST  = 0x0350
	TSDB_CODE_MND_USER_NOT_EXIST      = 0x0351
	TSDB_CODE_MND_INVALID_USER_FORMAT = 0x0352
	TSDB_CODE_MND_INVALID_PASS_FORMAT = 0x0353
	TSDB_CODE_MND_NO_USER_FROM_CONN   = 0x0354
	TSDB_CODE_MND_TOO_MANY_USERS      = 0x0355
	TSDB_CODE_MND_INVALID_ALTER_OPER  = 0x0356
	TSDB_CODE_MND_AUTH_FAILURE        = 0x0357
)

// 400
const (
	TSDB_CODE_TSC_SQL_SYNTAX_ERROR        = 0x0216
	TSDB_CODE_TSC_LINE_SYNTAX_ERROR       = 0x021B
	TSDB_CODE_PAR_SYNTAX_ERROR            = 0x2600
	TSDB_CODE_TDB_TIMESTAMP_OUT_OF_RANGE  = 0x060B
	TSDB_CODE_TSC_VALUE_OUT_OF_RANGE      = 0x0224
	TSDB_CODE_PAR_INVALID_FILL_TIME_RANGE = 0x263B
)

// RPC_NETWORK_UNAVAIL return 502 status code
const (
	RPC_NETWORK_UNAVAIL = 0x000B
)

// ErrorMsgMap is the map of error code and error message.
var ErrorMsgMap = map[int]string{
	TSDB_CODE_RPC_AUTH_FAILURE:   "Authentication failure",
	HTTP_SERVER_OFFLINE:          "http server is not onlin",
	HTTP_UNSUPPORTED_URL:         "url is not support",
	HTTP_INVALID_URL:             "invalid url format",
	HTTP_NO_ENOUGH_MEMORY:        "no enough memory",
	HTTP_REQUSET_TOO_BIG:         "request size is too big",
	HTTP_NO_AUTH_INFO:            "no auth info input",
	HTTP_NO_MSG_INPUT:            "request is empty",
	HTTP_NO_SQL_INPUT:            "no sql input",
	HTTP_NO_EXEC_USEDB:           "no need to execute use db cmd",
	HTTP_SESSION_FULL:            "session list was full",
	HTTP_GEN_TAOSD_TOKEN_ERR:     "generate taosd token error",
	HTTP_INVALID_MULTI_REQUEST:   "size of multi request is 0",
	HTTP_CREATE_GZIP_FAILED:      "failed to create gzip",
	HTTP_FINISH_GZIP_FAILED:      "failed to finish gzip",
	HTTP_LOGIN_FAILED:            "failed to login",
	HTTP_INVALID_VERSION:         "invalid http version",
	HTTP_INVALID_CONTENT_LENGTH:  "invalid content length",
	HTTP_INVALID_AUTH_TYPE:       "invalid type of Authorization",
	HTTP_INVALID_AUTH_FORMAT:     "invalid format of Authorization",
	HTTP_INVALID_BASIC_AUTH:      "invalid basic Authorization",
	HTTP_INVALID_TAOSD_AUTH:      "invalid taosd Authorization",
	HTTP_PARSE_METHOD_FAILED:     "failed to parse method",
	HTTP_PARSE_TARGET_FAILED:     "failed to parse target",
	HTTP_PARSE_VERSION_FAILED:    "failed to parse http version",
	HTTP_PARSE_SP_FAILED:         "failed to parse sp",
	HTTP_PARSE_STATUS_FAILED:     "failed to parse status",
	HTTP_PARSE_PHRASE_FAILED:     "failed to parse phrase",
	HTTP_PARSE_CRLF_FAILED:       "failed to parse crlf",
	HTTP_PARSE_HEADER_FAILED:     "failed to parse header",
	HTTP_PARSE_HEADER_KEY_FAILED: "failed to parse header key",
	HTTP_PARSE_HEADER_VAL_FAILED: "failed to parse header val",
	HTTP_PARSE_CHUNK_SIZE_FAILED: "failed to parse chunk size",
	HTTP_PARSE_CHUNK_FAILED:      "failed to parse chunk",
	HTTP_PARSE_END_FAILED:        "failed to parse end section",
	HTTP_PARSE_INVALID_STATE:     "invalid parse state",
	HTTP_PARSE_ERROR_STATE:       "failed to parse error section",
	HTTP_GC_QUERY_NULL:           "query size is 0",
	HTTP_GC_QUERY_SIZE:           "query size can not more than 100",
	HTTP_GC_REQ_PARSE_ERROR:      "parse grafana json error",
	HTTP_TG_DB_NOT_INPUT:         "database name can not be null",
	HTTP_TG_DB_TOO_LONG:          "database name too long",
	HTTP_TG_INVALID_JSON:         "invalid telegraf json format",
	HTTP_TG_METRICS_NULL:         "metrics size is 0",
	HTTP_TG_METRICS_SIZE:         "metrics size can not more than 1K",
	HTTP_TG_METRIC_NULL:          "metric name not find",
	HTTP_TG_METRIC_TYPE:          "metric name type should be string",
	HTTP_TG_METRIC_NAME_NULL:     "metric name length is 0",
	HTTP_TG_METRIC_NAME_LONG:     "metric name length too long",
	HTTP_TG_TIMESTAMP_NULL:       "timestamp not find",
	HTTP_TG_TIMESTAMP_TYPE:       "timestamp type should be integer",
	HTTP_TG_TIMESTAMP_VAL_NULL:   "timestamp value smaller than 0",
	HTTP_TG_TAGS_NULL:            "tags not find",
	HTTP_TG_TAGS_SIZE_0:          "tags size is 0",
	HTTP_TG_TAGS_SIZE_LONG:       "tags size too long",
	HTTP_TG_TAG_NULL:             "tag is null",
	HTTP_TG_TAG_NAME_NULL:        "tag name is null",
	HTTP_TG_TAG_NAME_SIZE:        "tag name length too long",
	HTTP_TG_TAG_VALUE_TYPE:       "tag value type should be number or string",
	HTTP_TG_TAG_VALUE_NULL:       "tag value is null",
	HTTP_TG_TABLE_NULL:           "table is null",
	HTTP_TG_TABLE_SIZE:           "table name length too long",
	HTTP_TG_FIELDS_NULL:          "fields not find",
	HTTP_TG_FIELDS_SIZE_0:        "fields size is 0",
	HTTP_TG_FIELDS_SIZE_LONG:     "fields size too long",
	HTTP_TG_FIELD_NULL:           "field is null",
	HTTP_TG_FIELD_NAME_NULL:      "field name is null",
	HTTP_TG_FIELD_NAME_SIZE:      "field name length too long",
	HTTP_TG_FIELD_VALUE_TYPE:     "field value type should be number or string",
	HTTP_TG_FIELD_VALUE_NULL:     "field value is null",
	HTTP_TG_HOST_NOT_STRING:      "host type should be string",
	HTTP_TG_STABLE_NOT_EXIST:     "stable not exist",
	HTTP_OP_DB_NOT_INPUT:         "database name can not be null",
	HTTP_OP_DB_TOO_LONG:          "database name too long",
	HTTP_OP_INVALID_JSON:         "invalid opentsdb json format",
	HTTP_OP_METRICS_NULL:         "metrics size is 0",
	HTTP_OP_METRICS_SIZE:         "metrics size can not more than 10K",
	HTTP_OP_METRIC_NULL:          "metric name not find",
	HTTP_OP_METRIC_TYPE:          "metric name type should be string",
	HTTP_OP_METRIC_NAME_NULL:     "metric name length is 0",
	HTTP_OP_METRIC_NAME_LONG:     "metric name length can not more than 22",
	HTTP_OP_TIMESTAMP_NULL:       "timestamp not find",
	HTTP_OP_TIMESTAMP_TYPE:       "timestamp type should be integer",
	HTTP_OP_TIMESTAMP_VAL_NULL:   "timestamp value smaller than 0",
	HTTP_OP_TAGS_NULL:            "tags not find",
	HTTP_OP_TAGS_SIZE_0:          "tags size is 0",
	HTTP_OP_TAGS_SIZE_LONG:       "tags size too long",
	HTTP_OP_TAG_NULL:             "tag is null",
	HTTP_OP_TAG_NAME_NULL:        "tag name is null",
	HTTP_OP_TAG_NAME_SIZE:        "tag name length too long",
	HTTP_OP_TAG_VALUE_TYPE:       "tag value type should be boolean number or string",
	HTTP_OP_TAG_VALUE_NULL:       "tag value is null",
	HTTP_OP_TAG_VALUE_TOO_LONG:   "tag value can not more than 64",
	HTTP_OP_VALUE_NULL:           "value not find",
	HTTP_OP_VALUE_TYPE:           "value type should be boolean number or string",
	HTTP_REQUEST_JSON_ERROR:      "http request json error",
}
