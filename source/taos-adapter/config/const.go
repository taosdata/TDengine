package config

import (
	"net/http"

	"github.com/taosdata/taosadapter/v3/httperror"
)

const ReqIDKey = "QID"
const SessionIDKey = "SID"
const ModelKey = "model"

var ErrorStatusMap = map[int32]int{
	//400
	httperror.TSDB_CODE_TSC_SQL_SYNTAX_ERROR:        http.StatusBadRequest,
	httperror.TSDB_CODE_TSC_LINE_SYNTAX_ERROR:       http.StatusBadRequest,
	httperror.TSDB_CODE_PAR_SYNTAX_ERROR:            http.StatusBadRequest,
	httperror.TSDB_CODE_TDB_TIMESTAMP_OUT_OF_RANGE:  http.StatusBadRequest,
	httperror.TSDB_CODE_TSC_VALUE_OUT_OF_RANGE:      http.StatusBadRequest,
	httperror.TSDB_CODE_PAR_INVALID_FILL_TIME_RANGE: http.StatusBadRequest,
	//401
	httperror.TSDB_CODE_MND_USER_ALREADY_EXIST:  http.StatusUnauthorized,
	httperror.TSDB_CODE_MND_USER_NOT_EXIST:      http.StatusUnauthorized,
	httperror.TSDB_CODE_MND_INVALID_USER_FORMAT: http.StatusUnauthorized,
	httperror.TSDB_CODE_MND_INVALID_PASS_FORMAT: http.StatusUnauthorized,
	httperror.TSDB_CODE_MND_NO_USER_FROM_CONN:   http.StatusUnauthorized,
	httperror.TSDB_CODE_MND_TOO_MANY_USERS:      http.StatusUnauthorized,
	httperror.TSDB_CODE_MND_INVALID_ALTER_OPER:  http.StatusUnauthorized,
	httperror.TSDB_CODE_MND_AUTH_FAILURE:        http.StatusUnauthorized,
	//502
	httperror.RPC_NETWORK_UNAVAIL: http.StatusBadGateway,
}
