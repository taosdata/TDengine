package monitor

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v3/process"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/monitor/metrics"
	"github.com/taosdata/taosadapter/v3/thread"
	"github.com/taosdata/taosadapter/v3/tools/generator"
	"github.com/taosdata/taosadapter/v3/tools/innerjson"
	"github.com/taosdata/taosadapter/v3/tools/limiter"
	"github.com/taosdata/taosadapter/v3/tools/sqltype"
)

// rest
var (
	RestTotal *metrics.Gauge
	RestQuery *metrics.Gauge
	RestWrite *metrics.Gauge
	RestOther *metrics.Gauge

	RestSuccess      *metrics.Gauge
	RestQuerySuccess *metrics.Gauge
	RestWriteSuccess *metrics.Gauge
	RestOtherSuccess *metrics.Gauge

	RestFail      *metrics.Gauge
	RestQueryFail *metrics.Gauge
	RestWriteFail *metrics.Gauge
	RestOtherFail *metrics.Gauge

	RestInProcess      *metrics.Gauge
	RestQueryInProcess *metrics.Gauge
	RestWriteInProcess *metrics.Gauge
	RestOtherInProcess *metrics.Gauge
)

var (
	WSTotal *metrics.Gauge
	WSQuery *metrics.Gauge
	WSWrite *metrics.Gauge
	WSOther *metrics.Gauge

	WSSuccess      *metrics.Gauge
	WSQuerySuccess *metrics.Gauge
	WSWriteSuccess *metrics.Gauge
	WSOtherSuccess *metrics.Gauge

	WSFail      *metrics.Gauge
	WSQueryFail *metrics.Gauge
	WSWriteFail *metrics.Gauge
	WSOtherFail *metrics.Gauge

	WSInProcess      *metrics.Gauge
	WSQueryInProcess *metrics.Gauge
	WSWriteInProcess *metrics.Gauge
	WSOtherInProcess *metrics.Gauge
)

var (
	ConnPoolInUse        sync.Map
	WSQueryConn          *metrics.Gauge
	WSQueryConnIncrement *metrics.Gauge
	WSQueryConnDecrement *metrics.Gauge

	WSSMLConn           *metrics.Gauge
	WSSMLConnIncrement  *metrics.Gauge
	WSSMLConnDecrement  *metrics.Gauge
	WSStmtConn          *metrics.Gauge
	WSStmtConnIncrement *metrics.Gauge
	WSStmtConnDecrement *metrics.Gauge
	WSWSConn            *metrics.Gauge
	WSWSConnIncrement   *metrics.Gauge
	WSWSConnDecrement   *metrics.Gauge
	WSTMQConn           *metrics.Gauge
	WSTMQConnIncrement  *metrics.Gauge
	WSTMQConnDecrement  *metrics.Gauge

	AsyncCInflight *metrics.Gauge
	SyncCInflight  *metrics.Gauge
)

var (
	WSQuerySqlResultCount = metrics.NewGauge("ws_query_sql_result_count")

	WSStmtStmtCount = metrics.NewGauge("ws_stmt_stmt_count")

	WSWSSqlResultCount = metrics.NewGauge("ws_ws_sql_result_count")
	WSWSStmtCount      = metrics.NewGauge("ws_ws_stmt_count")
	WSWSStmt2Count     = metrics.NewGauge("ws_ws_stmt2_count")
)

type InputJsonMetric struct {
	TotalRows    *metrics.Gauge
	SuccessRows  *metrics.Gauge
	FailRows     *metrics.Gauge
	InflightRows *metrics.Gauge
	AffectedRows *metrics.Gauge
}

var (
	InputJsonMetricsMap = make(map[string]*InputJsonMetric)
)

func NewInputJsonMetric(endpoint string) *InputJsonMetric {
	if config.Conf.UploadKeeper.Enable {
		inputJsonMetric := &InputJsonMetric{
			TotalRows:    metrics.NewGauge("total_rows"),
			SuccessRows:  metrics.NewGauge("success_rows"),
			FailRows:     metrics.NewGauge("fail_rows"),
			InflightRows: metrics.NewGauge("inflight_rows"),
			AffectedRows: metrics.NewGauge("affected_rows"),
		}
		InputJsonMetricsMap[endpoint] = inputJsonMetric
		return inputJsonMetric
	}
	return nil
}

var (
	// taos_connect and taos_close
	TaosConnectCounter        = metrics.NewGauge("taos_connect_total")
	TaosConnectSuccessCounter = metrics.NewGauge("taos_connect_success")
	TaosConnectFailCounter    = metrics.NewGauge("taos_connect_fail")

	TaosCloseCounter        = metrics.NewGauge("taos_close_total")
	TaosCloseSuccessCounter = metrics.NewGauge("taos_close_success")

	// sml
	TaosSchemalessInsertCounter            = metrics.NewGauge("taos_schemaless_insert_total")
	TaosSchemalessInsertSuccessCounter     = metrics.NewGauge("taos_schemaless_insert_success")
	TaosSchemalessInsertFailCounter        = metrics.NewGauge("taos_schemaless_insert_fail")
	TaosSchemalessFreeResultCounter        = metrics.NewGauge("taos_schemaless_free_result_total")
	TaosSchemalessFreeResultSuccessCounter = metrics.NewGauge("taos_schemaless_free_result_success")

	// sync query
	TaosQueryCounter                      = metrics.NewGauge("taos_query_total")
	TaosQuerySuccessCounter               = metrics.NewGauge("taos_query_success")
	TaosQueryFailCounter                  = metrics.NewGauge("taos_query_fail")
	TaosSyncQueryFreeResultCounter        = metrics.NewGauge("taos_query_free_result_total")
	TaosSyncQueryFreeResultSuccessCounter = metrics.NewGauge("taos_query_free_result_success")

	// async query
	TaosQueryAWithReqIDCounter        = metrics.NewGauge("taos_query_a_with_reqid_total")
	TaosQueryAWithReqIDSuccessCounter = metrics.NewGauge("taos_query_a_with_reqid_success")

	TaosQueryAWithReqIDCallBackCounter        = metrics.NewGauge("taos_query_a_with_reqid_callback_total")
	TaosQueryAWithReqIDCallBackSuccessCounter = metrics.NewGauge("taos_query_a_with_reqid_callback_success")
	TaosQueryAWithReqIDCallBackFailCounter    = metrics.NewGauge("taos_query_a_with_reqid_callback_fail")

	TaosAsyncQueryFreeResultCounter        = metrics.NewGauge("taos_query_a_free_result_total")
	TaosAsyncQueryFreeResultSuccessCounter = metrics.NewGauge("taos_query_a_free_result_success")

	// tmq poll result
	TMQPollResultCounter        = metrics.NewGauge("tmq_consumer_poll_result_total")
	TMQFreeResultCounter        = metrics.NewGauge("tmq_free_result_total")
	TMQFreeResultSuccessCounter = metrics.NewGauge("tmq_free_result_success")

	// stmt2 init
	TaosStmt2InitCounter        = metrics.NewGauge("taos_stmt2_init_total")
	TaosStmt2InitSuccessCounter = metrics.NewGauge("taos_stmt2_init_success")
	TaosStmt2InitFailCounter    = metrics.NewGauge("taos_stmt2_init_fail")

	TaosStmt2CloseCounter        = metrics.NewGauge("taos_stmt2_close_total")
	TaosStmt2CloseSuccessCounter = metrics.NewGauge("taos_stmt2_close_success")
	TaosStmt2CloseFailCounter    = metrics.NewGauge("taos_stmt2_close_fail")

	// stmt2 get fields
	TaosStmt2GetFieldsCounter        = metrics.NewGauge("taos_stmt2_get_fields_total")
	TaosStmt2GetFieldsSuccessCounter = metrics.NewGauge("taos_stmt2_get_fields_success")
	TaosStmt2GetFieldsFailCounter    = metrics.NewGauge("taos_stmt2_get_fields_fail")

	TaosStmt2FreeFieldsCounter        = metrics.NewGauge("taos_stmt2_free_fields_total")
	TaosStmt2FreeFieldsSuccessCounter = metrics.NewGauge("taos_stmt2_free_fields_success")

	// stmt init
	TaosStmtInitWithReqIDCounter        = metrics.NewGauge("taos_stmt_init_with_reqid_total")
	TaosStmtInitWithReqIDSuccessCounter = metrics.NewGauge("taos_stmt_init_with_reqid_success")
	TaosStmtInitWithReqIDFailCounter    = metrics.NewGauge("taos_stmt_init_with_reqid_fail")

	TaosStmtCloseCounter        = metrics.NewGauge("taos_stmt_close_total")
	TaosStmtCloseSuccessCounter = metrics.NewGauge("taos_stmt_close_success")
	TaosStmtCloseFailCounter    = metrics.NewGauge("taos_stmt_close_fail")

	// stmt get fields
	TaosStmtGetTagFieldsCounter        = metrics.NewGauge("taos_stmt_get_tag_fields_total")
	TaosStmtGetTagFieldsSuccessCounter = metrics.NewGauge("taos_stmt_get_tag_fields_success")
	TaosStmtGetTagFieldsFailCounter    = metrics.NewGauge("taos_stmt_get_tag_fields_fail")

	TaosStmtGetColFieldsCounter        = metrics.NewGauge("taos_stmt_get_col_fields_total")
	TaosStmtGetColFieldsSuccessCounter = metrics.NewGauge("taos_stmt_get_col_fields_success")
	TaosStmtGetColFieldsFailCounter    = metrics.NewGauge("taos_stmt_get_col_fields_fail")

	TaosStmtReclaimFieldsCounter        = metrics.NewGauge("taos_stmt_reclaim_fields_total")
	TaosStmtReclaimFieldsSuccessCounter = metrics.NewGauge("taos_stmt_reclaim_fields_success")

	// tmq get json meta
	TMQGetJsonMetaCounter        = metrics.NewGauge("tmq_get_json_meta_total")
	TMQGetJsonMetaSuccessCounter = metrics.NewGauge("tmq_get_json_meta_success")

	TMQFreeJsonMetaCounter        = metrics.NewGauge("tmq_free_json_meta_total")
	TMQFreeJsonMetaSuccessCounter = metrics.NewGauge("tmq_free_json_meta_success")

	// fetch whitelist
	TaosFetchWhitelistACounter        = metrics.NewGauge("taos_fetch_whitelist_a_total")
	TaosFetchWhitelistASuccessCounter = metrics.NewGauge("taos_fetch_whitelist_a_success")

	TaosFetchWhitelistACallBackCounter        = metrics.NewGauge("taos_fetch_whitelist_a_callback_total")
	TaosFetchWhitelistACallBackSuccessCounter = metrics.NewGauge("taos_fetch_whitelist_a_callback_success")
	TaosFetchWhitelistACallBackFailCounter    = metrics.NewGauge("taos_fetch_whitelist_a_callback_fail")

	// fetch rows
	TaosFetchRowsACounter        = metrics.NewGauge("taos_fetch_rows_a_total")
	TaosFetchRowsASuccessCounter = metrics.NewGauge("taos_fetch_rows_a_success")

	TaosFetchRowsACallBackCounter        = metrics.NewGauge("taos_fetch_rows_a_callback_total")
	TaosFetchRowsACallBackSuccessCounter = metrics.NewGauge("taos_fetch_rows_a_callback_success")
	TaosFetchRowsACallBackFailCounter    = metrics.NewGauge("taos_fetch_rows_a_callback_fail")

	// fetch raw block
	TaosFetchRawBlockACounter        = metrics.NewGauge("taos_fetch_raw_block_a_total")
	TaosFetchRawBlockASuccessCounter = metrics.NewGauge("taos_fetch_raw_block_a_success")

	TaosFetchRawBlockACallBackCounter        = metrics.NewGauge("taos_fetch_raw_block_a_callback_total")
	TaosFetchRawBlockACallBackSuccessCounter = metrics.NewGauge("taos_fetch_raw_block_a_callback_success")
	TaosFetchRawBlockACallBackFailCounter    = metrics.NewGauge("taos_fetch_raw_block_a_callback_fail")

	// tmq get raw
	TMQGetRawCounter        = metrics.NewGauge("tmq_get_raw_total")
	TMQGetRawSuccessCounter = metrics.NewGauge("tmq_get_raw_success")
	TMQGetRawFailCounter    = metrics.NewGauge("tmq_get_raw_fail")

	TMQFreeRawCounter        = metrics.NewGauge("tmq_free_raw_total")
	TMQFreeRawSuccessCounter = metrics.NewGauge("tmq_free_raw_success")

	// tmq new
	TMQConsumerNewCounter        = metrics.NewGauge("tmq_consumer_new_total")
	TMQConsumerNewSuccessCounter = metrics.NewGauge("tmq_consumer_new_success")
	TMQConsumerNewFailCounter    = metrics.NewGauge("tmq_consumer_new_fail")

	TMQConsumerCloseCounter        = metrics.NewGauge("tmq_consumer_close_total")
	TMQConsumerCloseSuccessCounter = metrics.NewGauge("tmq_consumer_close_success")
	TMQConsumerCloseFailCounter    = metrics.NewGauge("tmq_consumer_close_fail")

	// tmq subscribe
	TMQSubscribeCounter        = metrics.NewGauge("tmq_subscribe_total")
	TMQSubscribeSuccessCounter = metrics.NewGauge("tmq_subscribe_success")
	TMQSubscribeFailCounter    = metrics.NewGauge("tmq_subscribe_fail")

	// tmq unsubscribe
	TMQUnsubscribeCounter        = metrics.NewGauge("tmq_unsubscribe_total")
	TMQUnsubscribeSuccessCounter = metrics.NewGauge("tmq_unsubscribe_success")
	TMQUnsubscribeFailCounter    = metrics.NewGauge("tmq_unsubscribe_fail")

	// tmq list
	TMQListNewCounter        = metrics.NewGauge("tmq_list_new_total")
	TMQListNewSuccessCounter = metrics.NewGauge("tmq_list_new_success")
	TMQListNewFailCounter    = metrics.NewGauge("tmq_list_new_fail")

	TMQListDestroyCounter        = metrics.NewGauge("tmq_list_destroy_total")
	TMQListDestroySuccessCounter = metrics.NewGauge("tmq_list_destroy_success")

	// tmq config new
	TMQConfNewCounter        = metrics.NewGauge("tmq_conf_new_total")
	TMQConfNewSuccessCounter = metrics.NewGauge("tmq_conf_new_success")
	TMQConfNewFailCounter    = metrics.NewGauge("tmq_conf_new_fail")

	TMQConfDestroyCounter        = metrics.NewGauge("tmq_conf_destroy_total")
	TMQConfDestroySuccessCounter = metrics.NewGauge("tmq_conf_destroy_success")

	// stmt2 others
	TaosStmt2PrepareCounter        = metrics.NewGauge("taos_stmt2_prepare_total")
	TaosStmt2PrepareSuccessCounter = metrics.NewGauge("taos_stmt2_prepare_success")
	TaosStmt2PrepareFailCounter    = metrics.NewGauge("taos_stmt2_prepare_fail")

	TaosStmt2IsInsertCounter        = metrics.NewGauge("taos_stmt2_is_insert_total")
	TaosStmt2IsInsertSuccessCounter = metrics.NewGauge("taos_stmt2_is_insert_success")
	TaosStmt2IsInsertFailCounter    = metrics.NewGauge("taos_stmt2_is_insert_fail")

	TaosStmt2BindBinaryCounter        = metrics.NewGauge("taos_stmt2_bind_param_total")
	TaosStmt2BindBinarySuccessCounter = metrics.NewGauge("taos_stmt2_bind_param_success")
	TaosStmt2BindBinaryFailCounter    = metrics.NewGauge("taos_stmt2_bind_param_fail")

	TaosStmt2ExecCounter        = metrics.NewGauge("taos_stmt2_exec_total")
	TaosStmt2ExecSuccessCounter = metrics.NewGauge("taos_stmt2_exec_success")
	TaosStmt2ExecFailCounter    = metrics.NewGauge("taos_stmt2_exec_fail")

	TaosStmt2ErrorCounter        = metrics.NewGauge("taos_stmt2_error_total")
	TaosStmt2ErrorSuccessCounter = metrics.NewGauge("taos_stmt2_error_success")

	// query others
	TaosFetchRowCounter        = metrics.NewGauge("taos_fetch_row_total")
	TaosFetchRowSuccessCounter = metrics.NewGauge("taos_fetch_row_success")

	TaosIsUpdateQueryCounter        = metrics.NewGauge("taos_is_update_query_total")
	TaosIsUpdateQuerySuccessCounter = metrics.NewGauge("taos_is_update_query_success")

	TaosAffectedRowsCounter        = metrics.NewGauge("taos_affected_rows_total")
	TaosAffectedRowsSuccessCounter = metrics.NewGauge("taos_affected_rows_success")

	TaosNumFieldsCounter        = metrics.NewGauge("taos_num_fields_total")
	TaosNumFieldsSuccessCounter = metrics.NewGauge("taos_num_fields_success")

	TaosFetchFieldsECounter        = metrics.NewGauge("taos_fetch_fields_e_total")
	TaosFetchFieldsESuccessCounter = metrics.NewGauge("taos_fetch_fields_e_success")
	TaosFetchFieldsEFailCounter    = metrics.NewGauge("taos_fetch_fields_e_fail")

	TaosResultPrecisionCounter        = metrics.NewGauge("taos_result_precision_total")
	TaosResultPrecisionSuccessCounter = metrics.NewGauge("taos_result_precision_success")

	TaosGetRawBlockCounter        = metrics.NewGauge("taos_get_raw_block_total")
	TaosGetRawBlockSuccessCounter = metrics.NewGauge("taos_get_raw_block_success")

	TaosFetchRawBlockCounter        = metrics.NewGauge("taos_fetch_raw_block_total")
	TaosFetchRawBlockSuccessCounter = metrics.NewGauge("taos_fetch_raw_block_success")
	TaosFetchRawBlockFailCounter    = metrics.NewGauge("taos_fetch_raw_block_fail")

	TaosFetchLengthsCounter        = metrics.NewGauge("taos_fetch_lengths_total")
	TaosFetchLengthsSuccessCounter = metrics.NewGauge("taos_fetch_lengths_success")

	// inert others
	TaosWriteRawBlockWithReqIDCounter        = metrics.NewGauge("taos_write_raw_block_with_reqid_total")
	TaosWriteRawBlockWithReqIDSuccessCounter = metrics.NewGauge("taos_write_raw_block_with_reqid_success")
	TaosWriteRawBlockWithReqIDFailCounter    = metrics.NewGauge("taos_write_raw_block_with_reqid_fail")

	TaosWriteRawBlockWithFieldsWithReqIDCounter        = metrics.NewGauge("taos_write_raw_block_with_fields_with_reqid_total")
	TaosWriteRawBlockWithFieldsWithReqIDSuccessCounter = metrics.NewGauge("taos_write_raw_block_with_fields_with_reqid_success")
	TaosWriteRawBlockWithFieldsWithReqIDFailCounter    = metrics.NewGauge("taos_write_raw_block_with_fields_with_reqid_fail")
	TMQWriteRawCounter                                 = metrics.NewGauge("tmq_write_raw_total")
	TMQWriteRawSuccessCounter                          = metrics.NewGauge("tmq_write_raw_success")
	TMQWriteRawFailCounter                             = metrics.NewGauge("tmq_write_raw_fail")

	// stmt others
	TaosStmtPrepareCounter        = metrics.NewGauge("taos_stmt_prepare_total")
	TaosStmtPrepareSuccessCounter = metrics.NewGauge("taos_stmt_prepare_success")
	TaosStmtPrepareFailCounter    = metrics.NewGauge("taos_stmt_prepare_fail")

	TaosStmtIsInsertCounter        = metrics.NewGauge("taos_stmt_is_insert_total")
	TaosStmtIsInsertSuccessCounter = metrics.NewGauge("taos_stmt_is_insert_success")
	TaosStmtIsInsertFailCounter    = metrics.NewGauge("taos_stmt_is_insert_fail")

	TaosStmtSetTBNameCounter        = metrics.NewGauge("taos_stmt_set_tbname_total")
	TaosStmtSetTBNameSuccessCounter = metrics.NewGauge("taos_stmt_set_tbname_success")
	TaosStmtSetTBNameFailCounter    = metrics.NewGauge("taos_stmt_set_tbname_fail")

	TaosStmtSetTagsCounter        = metrics.NewGauge("taos_stmt_set_tags_total")
	TaosStmtSetTagsSuccessCounter = metrics.NewGauge("taos_stmt_set_tags_success")
	TaosStmtSetTagsFailCounter    = metrics.NewGauge("taos_stmt_set_tags_fail")

	TaosStmtBindParamBatchCounter        = metrics.NewGauge("taos_stmt_bind_param_batch_total")
	TaosStmtBindParamBatchSuccessCounter = metrics.NewGauge("taos_stmt_bind_param_batch_success")
	TaosStmtBindParamBatchFailCounter    = metrics.NewGauge("taos_stmt_bind_param_batch_fail")

	TaosStmtAddBatchCounter        = metrics.NewGauge("taos_stmt_add_batch_total")
	TaosStmtAddBatchSuccessCounter = metrics.NewGauge("taos_stmt_add_batch_success")
	TaosStmtAddBatchFailCounter    = metrics.NewGauge("taos_stmt_add_batch_fail")

	TaosStmtExecuteCounter        = metrics.NewGauge("taos_stmt_execute_total")
	TaosStmtExecuteSuccessCounter = metrics.NewGauge("taos_stmt_execute_success")
	TaosStmtExecuteFailCounter    = metrics.NewGauge("taos_stmt_execute_fail")

	TaosStmtNumParamsCounter        = metrics.NewGauge("taos_stmt_num_params_total")
	TaosStmtNumParamsSuccessCounter = metrics.NewGauge("taos_stmt_num_params_success")
	TaosStmtNumParamsFailCounter    = metrics.NewGauge("taos_stmt_num_params_fail")

	TaosStmtGetParamCounter        = metrics.NewGauge("taos_stmt_get_param_total")
	TaosStmtGetParamSuccessCounter = metrics.NewGauge("taos_stmt_get_param_success")
	TaosStmtGetParamFailCounter    = metrics.NewGauge("taos_stmt_get_param_fail")

	TaosStmtErrStrCounter        = metrics.NewGauge("taos_stmt_errstr_total")
	TaosStmtErrStrSuccessCounter = metrics.NewGauge("taos_stmt_errstr_success")

	TaosStmtAffectedRowsOnceCounter        = metrics.NewGauge("taos_stmt_affected_rows_once_total")
	TaosStmtAffectedRowsOnceSuccessCounter = metrics.NewGauge("taos_stmt_affected_rows_once_success")

	TaosStmtUseResultCounter        = metrics.NewGauge("taos_stmt_use_result_total")
	TaosStmtUseResultSuccessCounter = metrics.NewGauge("taos_stmt_use_result_success")
	TaosStmtUseResultFailCounter    = metrics.NewGauge("taos_stmt_use_result_fail")

	// others
	TaosSelectDBCounter        = metrics.NewGauge("taos_select_db_total")
	TaosSelectDBSuccessCounter = metrics.NewGauge("taos_select_db_success")
	TaosSelectDBFailCounter    = metrics.NewGauge("taos_select_db_fail")

	TaosGetTablesVgIDCounter        = metrics.NewGauge("taos_get_tables_vgId_total")
	TaosGetTablesVgIDSuccessCounter = metrics.NewGauge("taos_get_tables_vgId_success")
	TaosGetTablesVgIDFailCounter    = metrics.NewGauge("taos_get_tables_vgId_fail")

	TaosOptionsConnectionCounter        = metrics.NewGauge("taos_options_connection_total")
	TaosOptionsConnectionSuccessCounter = metrics.NewGauge("taos_options_connection_success")
	TaosOptionsConnectionFailCounter    = metrics.NewGauge("taos_options_connection_fail")

	TaosValidateSqlCounter        = metrics.NewGauge("taos_validate_sql_total")
	TaosValidateSqlSuccessCounter = metrics.NewGauge("taos_validate_sql_success")
	TaosValidateSqlFailCounter    = metrics.NewGauge("taos_validate_sql_fail")

	TaosCheckServerStatusCounter        = metrics.NewGauge("taos_check_server_status_total")
	TaosCheckServerStatusSuccessCounter = metrics.NewGauge("taos_check_server_status_success")

	TaosGetCurrentDBCounter        = metrics.NewGauge("taos_get_current_db_total")
	TaosGetCurrentDBSuccessCounter = metrics.NewGauge("taos_get_current_db_success")
	TaosGetCurrentDBFailCounter    = metrics.NewGauge("taos_get_current_db_fail")

	TaosGetServerInfoCounter        = metrics.NewGauge("taos_get_server_info_total")
	TaosGetServerInfoSuccessCounter = metrics.NewGauge("taos_get_server_info_success")

	TaosOptionsCounter        = metrics.NewGauge("taos_options_total")
	TaosOptionsSuccessCounter = metrics.NewGauge("taos_options_success")
	TaosOptionsFailCounter    = metrics.NewGauge("taos_options_fail")

	TaosSetConnModeCounter        = metrics.NewGauge("taos_set_conn_mode_total")
	TaosSetConnModeSuccessCounter = metrics.NewGauge("taos_set_conn_mode_success")
	TaosSetConnModeFailCounter    = metrics.NewGauge("taos_set_conn_mode_fail")

	TaosResetCurrentDBCounter        = metrics.NewGauge("taos_reset_current_db_total")
	TaosResetCurrentDBSuccessCounter = metrics.NewGauge("taos_reset_current_db_success")

	// notify
	TaosSetNotifyCBCounter        = metrics.NewGauge("taos_set_notify_cb_total")
	TaosSetNotifyCBSuccessCounter = metrics.NewGauge("taos_set_notify_cb_success")
	TaosSetNotifyCBFailCounter    = metrics.NewGauge("taos_set_notify_cb_fail")

	// taos error
	TaosErrorCounter        = metrics.NewGauge("taos_errno_total")
	TaosErrorSuccessCounter = metrics.NewGauge("taos_errno_success")

	TaosErrorStrCounter        = metrics.NewGauge("taos_errstr_total")
	TaosErrorStrSuccessCounter = metrics.NewGauge("taos_errstr_success")

	// tmq others
	TMQPollCounter        = metrics.NewGauge("tmq_consumer_poll_total")
	TMQPollSuccessCounter = metrics.NewGauge("tmq_consumer_poll_success")
	TMQPollFailCounter    = metrics.NewGauge("tmq_consumer_poll_fail")

	TMQSubscriptionCounter        = metrics.NewGauge("tmq_subscription_total")
	TMQSubscriptionSuccessCounter = metrics.NewGauge("tmq_subscription_success")
	TMQSubscriptionFailCounter    = metrics.NewGauge("tmq_subscription_fail")

	TMQListAppendCounter        = metrics.NewGauge("tmq_list_append_total")
	TMQListAppendSuccessCounter = metrics.NewGauge("tmq_list_append_success")
	TMQListAppendFailCounter    = metrics.NewGauge("tmq_list_append_fail")

	TMQListGetSizeCounter        = metrics.NewGauge("tmq_list_get_size_total")
	TMQListGetSizeSuccessCounter = metrics.NewGauge("tmq_list_get_size_success")

	TMQErr2StrCounter        = metrics.NewGauge("tmq_err2str_total")
	TMQErr2StrSuccessCounter = metrics.NewGauge("tmq_err2str_success")

	TMQConfSetCounter        = metrics.NewGauge("tmq_conf_set_total")
	TMQConfSetSuccessCounter = metrics.NewGauge("tmq_conf_set_success")
	TMQConfSetFailCounter    = metrics.NewGauge("tmq_conf_set_fail")

	TMQGetResTypeCounter        = metrics.NewGauge("tmq_get_res_type_total")
	TMQGetResTypeSuccessCounter = metrics.NewGauge("tmq_get_res_type_success")

	TMQGetTopicNameCounter        = metrics.NewGauge("tmq_get_topic_name_total")
	TMQGetTopicNameSuccessCounter = metrics.NewGauge("tmq_get_topic_name_success")

	TMQGetVgroupIDCounter        = metrics.NewGauge("tmq_get_vgroup_id_total")
	TMQGetVgroupIDSuccessCounter = metrics.NewGauge("tmq_get_vgroup_id_success")

	TMQGetVgroupOffsetCounter        = metrics.NewGauge("tmq_get_vgroup_offset_total")
	TMQGetVgroupOffsetSuccessCounter = metrics.NewGauge("tmq_get_vgroup_offset_success")

	TMQGetDBNameCounter        = metrics.NewGauge("tmq_get_db_name_total")
	TMQGetDBNameSuccessCounter = metrics.NewGauge("tmq_get_db_name_success")

	TMQGetTableNameCounter        = metrics.NewGauge("tmq_get_table_name_total")
	TMQGetTableNameSuccessCounter = metrics.NewGauge("tmq_get_table_name_success")

	TMQGetConnectCounter        = metrics.NewGauge("tmq_get_connect_total")
	TMQGetConnectSuccessCounter = metrics.NewGauge("tmq_get_connect_success")

	TMQCommitSyncCounter        = metrics.NewGauge("tmq_commit_sync_total")
	TMQCommitSyncSuccessCounter = metrics.NewGauge("tmq_commit_sync_success")
	TMQCommitSyncFailCounter    = metrics.NewGauge("tmq_commit_sync_fail")

	TMQFetchRawBlockCounter        = metrics.NewGauge("tmq_fetch_raw_block_total")
	TMQFetchRawBlockSuccessCounter = metrics.NewGauge("tmq_fetch_raw_block_success")
	TMQFetchRawBlockFailCounter    = metrics.NewGauge("tmq_fetch_raw_block_fail")

	TMQGetTopicAssignmentCounter        = metrics.NewGauge("tmq_get_topic_assignment_total")
	TMQGetTopicAssignmentSuccessCounter = metrics.NewGauge("tmq_get_topic_assignment_success")
	TMQGetTopicAssignmentFailCounter    = metrics.NewGauge("tmq_get_topic_assignment_fail")

	TMQOffsetSeekCounter        = metrics.NewGauge("tmq_offset_seek_total")
	TMQOffsetSeekSuccessCounter = metrics.NewGauge("tmq_offset_seek_success")
	TMQOffsetSeekFailCounter    = metrics.NewGauge("tmq_offset_seek_fail")

	TMQCommittedCounter        = metrics.NewGauge("tmq_committed_total")
	TMQCommittedSuccessCounter = metrics.NewGauge("tmq_committed_success")
	TMQCommitOffsetFailCounter = metrics.NewGauge("tmq_commit_offset_sync_fail")

	TMQPositionCounter        = metrics.NewGauge("tmq_position_total")
	TMQPositionSuccessCounter = metrics.NewGauge("tmq_position_success")

	TMQCommitOffsetCounter        = metrics.NewGauge("tmq_commit_offset_sync_total")
	TMQCommitOffsetSuccessCounter = metrics.NewGauge("tmq_commit_offset_sync_success")

	// 3.4.0.0
	TaosConnectTOTPCounter        = metrics.NewGauge("taos_connect_totp_total")
	TaosConnectTOTPSuccessCounter = metrics.NewGauge("taos_connect_totp_success")
	TaosConnectTOTPFailCounter    = metrics.NewGauge("taos_connect_totp_fail")

	TaosConnectTokenCounter        = metrics.NewGauge("taos_connect_token_total")
	TaosConnectTokenSuccessCounter = metrics.NewGauge("taos_connect_token_success")
	TaosConnectTokenFailCounter    = metrics.NewGauge("taos_connect_token_fail")

	TaosGetConnectionInfoCounter        = metrics.NewGauge("taos_get_connection_info_total")
	TaosGetConnectionInfoSuccessCounter = metrics.NewGauge("taos_get_connection_info_success")
	TaosGetConnectInfoFailCounter       = metrics.NewGauge("taos_get_connection_info_fail")
)

func InitKeeper() {
	if config.Conf.UploadKeeper.Enable {
		// rest
		RestTotal = metrics.NewGauge("rest_total")
		RestQuery = metrics.NewGauge("rest_query")
		RestWrite = metrics.NewGauge("rest_write")
		RestOther = metrics.NewGauge("rest_other")

		RestSuccess = metrics.NewGauge("rest_success")
		RestQuerySuccess = metrics.NewGauge("rest_query_success")
		RestWriteSuccess = metrics.NewGauge("rest_write_success")
		RestOtherSuccess = metrics.NewGauge("rest_other_success")

		RestFail = metrics.NewGauge("rest_fail")
		RestQueryFail = metrics.NewGauge("rest_query_fail")
		RestWriteFail = metrics.NewGauge("rest_write_fail")
		RestOtherFail = metrics.NewGauge("rest_other_fail")

		RestInProcess = metrics.NewGauge("rest_in_process")
		RestQueryInProcess = metrics.NewGauge("rest_query_in_process")
		RestWriteInProcess = metrics.NewGauge("rest_write_in_process")
		RestOtherInProcess = metrics.NewGauge("rest_other_in_process")

		// ws
		WSTotal = metrics.NewGauge("ws_total")
		WSQuery = metrics.NewGauge("ws_query")
		WSWrite = metrics.NewGauge("ws_write")
		WSOther = metrics.NewGauge("ws_other")

		WSSuccess = metrics.NewGauge("ws_success")
		WSQuerySuccess = metrics.NewGauge("ws_query_success")
		WSWriteSuccess = metrics.NewGauge("ws_write_success")
		WSOtherSuccess = metrics.NewGauge("ws_other_success")

		WSFail = metrics.NewGauge("ws_fail")
		WSQueryFail = metrics.NewGauge("ws_query_fail")
		WSWriteFail = metrics.NewGauge("ws_write_fail")
		WSOtherFail = metrics.NewGauge("ws_other_fail")

		WSInProcess = metrics.NewGauge("ws_in_process")
		WSQueryInProcess = metrics.NewGauge("ws_query_in_process")
		WSWriteInProcess = metrics.NewGauge("ws_write_in_process")
		WSOtherInProcess = metrics.NewGauge("ws_other_in_process")

		recordMetrics = append(recordMetrics,
			RestTotal,
			RestQuery,
			RestWrite,
			RestOther,

			RestSuccess,
			RestQuerySuccess,
			RestWriteSuccess,
			RestOtherSuccess,

			RestFail,
			RestQueryFail,
			RestWriteFail,
			RestOtherFail,

			WSTotal,
			WSQuery,
			WSWrite,
			WSOther,

			WSSuccess,
			WSQuerySuccess,
			WSWriteSuccess,
			WSOtherSuccess,

			WSFail,
			WSQueryFail,
			WSWriteFail,
			WSOtherFail,
		)
		inflightMetrics = append(inflightMetrics,
			RestInProcess,
			RestQueryInProcess,
			RestWriteInProcess,
			RestOtherInProcess,

			WSInProcess,
			WSQueryInProcess,
			WSWriteInProcess,
			WSOtherInProcess,
		)

		WSQueryConn = metrics.NewGauge("ws_query_conn")
		WSQueryConnIncrement = metrics.NewGauge("ws_query_conn_inc")
		WSQueryConnDecrement = metrics.NewGauge("ws_query_conn_dec")

		WSSMLConn = metrics.NewGauge("ws_sml_conn")
		WSSMLConnIncrement = metrics.NewGauge("ws_sml_conn_inc")
		WSSMLConnDecrement = metrics.NewGauge("ws_sml_conn_dec")

		WSStmtConn = metrics.NewGauge("ws_stmt_conn")
		WSStmtConnIncrement = metrics.NewGauge("ws_stmt_conn_inc")
		WSStmtConnDecrement = metrics.NewGauge("ws_stmt_conn_dec")

		WSWSConn = metrics.NewGauge("ws_ws_conn")
		WSWSConnIncrement = metrics.NewGauge("ws_ws_conn_inc")
		WSWSConnDecrement = metrics.NewGauge("ws_ws_conn_dec")

		WSTMQConn = metrics.NewGauge("ws_tmq_conn")
		WSTMQConnIncrement = metrics.NewGauge("ws_tmq_conn_inc")
		WSTMQConnDecrement = metrics.NewGauge("ws_tmq_conn_dec")

		AsyncCInflight = metrics.NewGauge("async_c_inflight")
		SyncCInflight = metrics.NewGauge("sync_c_inflight")

		thread.AsyncSemaphore.SetGauge(AsyncCInflight)
		thread.SyncSemaphore.SetGauge(SyncCInflight)
	}
}

var recordMetrics []*metrics.Gauge
var inflightMetrics []*metrics.Gauge

var cInterfaceCountMetrics = []*metrics.Gauge{
	TaosConnectCounter,
	TaosConnectSuccessCounter,
	TaosConnectFailCounter,
	TaosCloseCounter,
	TaosCloseSuccessCounter,
	TaosSchemalessInsertCounter,
	TaosSchemalessInsertSuccessCounter,
	TaosSchemalessInsertFailCounter,
	TaosSchemalessFreeResultCounter,
	TaosSchemalessFreeResultSuccessCounter,
	TaosQueryCounter,
	TaosQuerySuccessCounter,
	TaosQueryFailCounter,
	TaosSyncQueryFreeResultCounter,
	TaosSyncQueryFreeResultSuccessCounter,
	TaosQueryAWithReqIDCounter,
	TaosQueryAWithReqIDSuccessCounter,
	TaosQueryAWithReqIDCallBackCounter,
	TaosQueryAWithReqIDCallBackSuccessCounter,
	TaosQueryAWithReqIDCallBackFailCounter,
	TaosAsyncQueryFreeResultCounter,
	TaosAsyncQueryFreeResultSuccessCounter,
	TMQPollResultCounter,
	TMQFreeResultCounter,
	TMQFreeResultSuccessCounter,
	TaosStmt2InitCounter,
	TaosStmt2InitSuccessCounter,
	TaosStmt2InitFailCounter,
	TaosStmt2CloseCounter,
	TaosStmt2CloseSuccessCounter,
	TaosStmt2CloseFailCounter,
	TaosStmt2GetFieldsCounter,
	TaosStmt2GetFieldsSuccessCounter,
	TaosStmt2GetFieldsFailCounter,
	TaosStmt2FreeFieldsCounter,
	TaosStmt2FreeFieldsSuccessCounter,
	TaosStmtInitWithReqIDCounter,
	TaosStmtInitWithReqIDSuccessCounter,
	TaosStmtInitWithReqIDFailCounter,
	TaosStmtCloseCounter,
	TaosStmtCloseSuccessCounter,
	TaosStmtCloseFailCounter,
	TaosStmtGetTagFieldsCounter,
	TaosStmtGetTagFieldsSuccessCounter,
	TaosStmtGetTagFieldsFailCounter,
	TaosStmtGetColFieldsCounter,
	TaosStmtGetColFieldsSuccessCounter,
	TaosStmtGetColFieldsFailCounter,
	TaosStmtReclaimFieldsCounter,
	TaosStmtReclaimFieldsSuccessCounter,
	TMQGetJsonMetaCounter,
	TMQGetJsonMetaSuccessCounter,
	TMQFreeJsonMetaCounter,
	TMQFreeJsonMetaSuccessCounter,
	TaosFetchWhitelistACounter,
	TaosFetchWhitelistASuccessCounter,
	TaosFetchWhitelistACallBackCounter,
	TaosFetchWhitelistACallBackSuccessCounter,
	TaosFetchWhitelistACallBackFailCounter,
	TaosFetchRowsACounter,
	TaosFetchRowsASuccessCounter,
	TaosFetchRowsACallBackCounter,
	TaosFetchRowsACallBackSuccessCounter,
	TaosFetchRowsACallBackFailCounter,
	TaosFetchRawBlockACounter,
	TaosFetchRawBlockASuccessCounter,
	TaosFetchRawBlockACallBackCounter,
	TaosFetchRawBlockACallBackSuccessCounter,
	TaosFetchRawBlockACallBackFailCounter,
	TMQGetRawCounter,
	TMQGetRawSuccessCounter,
	TMQGetRawFailCounter,
	TMQFreeRawCounter,
	TMQFreeRawSuccessCounter,
	TMQConsumerNewCounter,
	TMQConsumerNewSuccessCounter,
	TMQConsumerNewFailCounter,
	TMQConsumerCloseCounter,
	TMQConsumerCloseSuccessCounter,
	TMQConsumerCloseFailCounter,
	TMQSubscribeCounter,
	TMQSubscribeSuccessCounter,
	TMQSubscribeFailCounter,
	TMQUnsubscribeCounter,
	TMQUnsubscribeSuccessCounter,
	TMQUnsubscribeFailCounter,
	TMQListNewCounter,
	TMQListNewSuccessCounter,
	TMQListNewFailCounter,
	TMQListDestroyCounter,
	TMQListDestroySuccessCounter,
	TMQConfNewCounter,
	TMQConfNewSuccessCounter,
	TMQConfNewFailCounter,
	TMQConfDestroyCounter,
	TMQConfDestroySuccessCounter,
	TaosStmt2PrepareCounter,
	TaosStmt2PrepareSuccessCounter,
	TaosStmt2PrepareFailCounter,
	TaosStmt2IsInsertCounter,
	TaosStmt2IsInsertSuccessCounter,
	TaosStmt2IsInsertFailCounter,
	TaosStmt2BindBinaryCounter,
	TaosStmt2BindBinarySuccessCounter,
	TaosStmt2BindBinaryFailCounter,
	TaosStmt2ExecCounter,
	TaosStmt2ExecSuccessCounter,
	TaosStmt2ExecFailCounter,
	TaosStmt2ErrorCounter,
	TaosStmt2ErrorSuccessCounter,
	TaosFetchRowCounter,
	TaosFetchRowSuccessCounter,
	TaosIsUpdateQueryCounter,
	TaosIsUpdateQuerySuccessCounter,
	TaosAffectedRowsCounter,
	TaosAffectedRowsSuccessCounter,
	TaosNumFieldsCounter,
	TaosNumFieldsSuccessCounter,
	TaosFetchFieldsECounter,
	TaosFetchFieldsESuccessCounter,
	TaosFetchFieldsEFailCounter,
	TaosResultPrecisionCounter,
	TaosResultPrecisionSuccessCounter,
	TaosGetRawBlockCounter,
	TaosGetRawBlockSuccessCounter,
	TaosFetchRawBlockCounter,
	TaosFetchRawBlockSuccessCounter,
	TaosFetchRawBlockFailCounter,
	TaosFetchLengthsCounter,
	TaosFetchLengthsSuccessCounter,
	TaosWriteRawBlockWithReqIDCounter,
	TaosWriteRawBlockWithReqIDSuccessCounter,
	TaosWriteRawBlockWithReqIDFailCounter,
	TaosWriteRawBlockWithFieldsWithReqIDCounter,
	TaosWriteRawBlockWithFieldsWithReqIDSuccessCounter,
	TaosWriteRawBlockWithFieldsWithReqIDFailCounter,
	TMQWriteRawCounter,
	TMQWriteRawSuccessCounter,
	TMQWriteRawFailCounter,
	TaosStmtPrepareCounter,
	TaosStmtPrepareSuccessCounter,
	TaosStmtPrepareFailCounter,
	TaosStmtIsInsertCounter,
	TaosStmtIsInsertSuccessCounter,
	TaosStmtIsInsertFailCounter,
	TaosStmtSetTBNameCounter,
	TaosStmtSetTBNameSuccessCounter,
	TaosStmtSetTBNameFailCounter,
	TaosStmtSetTagsCounter,
	TaosStmtSetTagsSuccessCounter,
	TaosStmtSetTagsFailCounter,
	TaosStmtBindParamBatchCounter,
	TaosStmtBindParamBatchSuccessCounter,
	TaosStmtBindParamBatchFailCounter,
	TaosStmtAddBatchCounter,
	TaosStmtAddBatchSuccessCounter,
	TaosStmtAddBatchFailCounter,
	TaosStmtExecuteCounter,
	TaosStmtExecuteSuccessCounter,
	TaosStmtExecuteFailCounter,
	TaosStmtNumParamsCounter,
	TaosStmtNumParamsSuccessCounter,
	TaosStmtNumParamsFailCounter,
	TaosStmtGetParamCounter,
	TaosStmtGetParamSuccessCounter,
	TaosStmtGetParamFailCounter,
	TaosStmtErrStrCounter,
	TaosStmtErrStrSuccessCounter,
	TaosStmtAffectedRowsOnceCounter,
	TaosStmtAffectedRowsOnceSuccessCounter,
	TaosStmtUseResultCounter,
	TaosStmtUseResultSuccessCounter,
	TaosStmtUseResultFailCounter,
	TaosSelectDBCounter,
	TaosSelectDBSuccessCounter,
	TaosSelectDBFailCounter,
	TaosGetTablesVgIDCounter,
	TaosGetTablesVgIDSuccessCounter,
	TaosGetTablesVgIDFailCounter,
	TaosOptionsConnectionCounter,
	TaosOptionsConnectionSuccessCounter,
	TaosOptionsConnectionFailCounter,
	TaosValidateSqlCounter,
	TaosValidateSqlSuccessCounter,
	TaosValidateSqlFailCounter,
	TaosCheckServerStatusCounter,
	TaosCheckServerStatusSuccessCounter,
	TaosGetCurrentDBCounter,
	TaosGetCurrentDBSuccessCounter,
	TaosGetCurrentDBFailCounter,
	TaosGetServerInfoCounter,
	TaosGetServerInfoSuccessCounter,
	TaosOptionsCounter,
	TaosOptionsSuccessCounter,
	TaosOptionsFailCounter,
	TaosSetConnModeCounter,
	TaosSetConnModeSuccessCounter,
	TaosSetConnModeFailCounter,
	TaosResetCurrentDBCounter,
	TaosResetCurrentDBSuccessCounter,
	TaosSetNotifyCBCounter,
	TaosSetNotifyCBSuccessCounter,
	TaosSetNotifyCBFailCounter,
	TaosErrorCounter,
	TaosErrorSuccessCounter,
	TaosErrorStrCounter,
	TaosErrorStrSuccessCounter,
	TMQPollCounter,
	TMQPollSuccessCounter,
	TMQPollFailCounter,
	TMQSubscriptionCounter,
	TMQSubscriptionSuccessCounter,
	TMQSubscriptionFailCounter,
	TMQListAppendCounter,
	TMQListAppendSuccessCounter,
	TMQListAppendFailCounter,
	TMQListGetSizeCounter,
	TMQListGetSizeSuccessCounter,
	TMQErr2StrCounter,
	TMQErr2StrSuccessCounter,
	TMQConfSetCounter,
	TMQConfSetSuccessCounter,
	TMQConfSetFailCounter,
	TMQGetResTypeCounter,
	TMQGetResTypeSuccessCounter,
	TMQGetTopicNameCounter,
	TMQGetTopicNameSuccessCounter,
	TMQGetVgroupIDCounter,
	TMQGetVgroupIDSuccessCounter,
	TMQGetVgroupOffsetCounter,
	TMQGetVgroupOffsetSuccessCounter,
	TMQGetDBNameCounter,
	TMQGetDBNameSuccessCounter,
	TMQGetTableNameCounter,
	TMQGetTableNameSuccessCounter,
	TMQGetConnectCounter,
	TMQGetConnectSuccessCounter,
	TMQCommitSyncCounter,
	TMQCommitSyncSuccessCounter,
	TMQCommitSyncFailCounter,
	TMQFetchRawBlockCounter,
	TMQFetchRawBlockSuccessCounter,
	TMQFetchRawBlockFailCounter,
	TMQGetTopicAssignmentCounter,
	TMQGetTopicAssignmentSuccessCounter,
	TMQGetTopicAssignmentFailCounter,
	TMQOffsetSeekCounter,
	TMQOffsetSeekSuccessCounter,
	TMQOffsetSeekFailCounter,
	TMQCommittedCounter,
	TMQCommittedSuccessCounter,
	TMQCommitOffsetFailCounter,
	TMQPositionCounter,
	TMQPositionSuccessCounter,
	TMQCommitOffsetCounter,
	TMQCommitOffsetSuccessCounter,

	// 3.4.0.0
	TaosConnectTOTPCounter,
	TaosConnectTOTPSuccessCounter,
	TaosConnectTOTPFailCounter,

	TaosConnectTokenCounter,
	TaosConnectTokenSuccessCounter,
	TaosConnectTokenFailCounter,

	TaosGetConnectionInfoCounter,
	TaosGetConnectionInfoSuccessCounter,
	TaosGetConnectInfoFailCounter,
}

// RestRecordRequest records a REST request and returns the SQL type.
// whatever the UploadKeeper is enabled or not, it will return the sql type.
func RestRecordRequest(sql string) sqltype.SqlType {
	sqlType := sqltype.GetSqlType(sql)
	if config.Conf.UploadKeeper.Enable {
		RestTotal.Inc()
		RestInProcess.Inc()
		switch sqlType {
		case sqltype.InsertType:
			RestWrite.Inc()
			RestWriteInProcess.Inc()
		case sqltype.SelectType:
			RestQuery.Inc()
			RestQueryInProcess.Inc()
		default:
			RestOther.Inc()
			RestOtherInProcess.Inc()
		}
	}
	return sqlType
}

func RestRecordResult(sqlType sqltype.SqlType, success bool) {
	if config.Conf.UploadKeeper.Enable {
		RestInProcess.Dec()
		if success {
			RestSuccess.Inc()
		} else {
			RestFail.Inc()
		}
		switch sqlType {
		case sqltype.InsertType:
			RestWriteInProcess.Dec()
			if success {
				RestWriteSuccess.Inc()
			} else {
				RestWriteFail.Inc()
			}
		case sqltype.SelectType:
			RestQueryInProcess.Dec()
			if success {
				RestQuerySuccess.Inc()
			} else {
				RestQueryFail.Inc()
			}
		default:
			RestOtherInProcess.Dec()
			if success {
				RestOtherSuccess.Inc()
			} else {
				RestOtherFail.Inc()
			}
		}
	}
}

// WSRecordRequest records a WebSocket request and returns the SQL type.
// whatever the UploadKeeper is enabled or not, it will return the sql type.
func WSRecordRequest(sql string) sqltype.SqlType {
	sqlType := sqltype.GetSqlType(sql)
	if config.Conf.UploadKeeper.Enable {
		WSTotal.Inc()
		WSInProcess.Inc()
		switch sqlType {
		case sqltype.InsertType:
			WSWrite.Inc()
			WSWriteInProcess.Inc()
		case sqltype.SelectType:
			WSQuery.Inc()
			WSQueryInProcess.Inc()
		default:
			WSOther.Inc()
			WSOtherInProcess.Inc()
		}
		return sqlType
	}
	return sqlType
}

func WSRecordResult(sqlType sqltype.SqlType, success bool) {
	if config.Conf.UploadKeeper.Enable {
		WSInProcess.Dec()
		if success {
			WSSuccess.Inc()
		} else {
			WSFail.Inc()
		}
		switch sqlType {
		case sqltype.InsertType:
			WSWriteInProcess.Dec()
			if success {
				WSWriteSuccess.Inc()
			} else {
				WSWriteFail.Inc()
			}
		case sqltype.SelectType:
			WSQueryInProcess.Dec()
			if success {
				WSQuerySuccess.Inc()
			} else {
				WSQueryFail.Inc()
			}
		default:
			WSOtherInProcess.Dec()
			if success {
				WSOtherSuccess.Inc()
			} else {
				WSOtherFail.Inc()
			}
		}
	}
}

func RecordNewConnectionPool(userName string) *metrics.Gauge {
	if config.Conf.UploadKeeper.Enable {
		gauge := metrics.NewGauge(fmt.Sprintf("conn_pool_in_use_%s", userName))
		ConnPoolInUse.Store(userName, gauge)
		return gauge
	}
	return nil
}

func RecordWSQueryConn() {
	if config.Conf.UploadKeeper.Enable {
		WSQueryConn.Inc()
		WSQueryConnIncrement.Inc()
	}
}

func RecordWSQueryDisconnect() {
	if config.Conf.UploadKeeper.Enable {
		WSQueryConn.Dec()
		WSQueryConnDecrement.Inc()
	}
}

func RecordWSSMLConn() {
	if config.Conf.UploadKeeper.Enable {
		WSSMLConn.Inc()
		WSSMLConnIncrement.Inc()
	}
}

func RecordWSSMLDisconnect() {
	if config.Conf.UploadKeeper.Enable {
		WSSMLConn.Dec()
		WSSMLConnDecrement.Inc()
	}
}

func RecordWSStmtConn() {
	if config.Conf.UploadKeeper.Enable {
		WSStmtConn.Inc()
		WSStmtConnIncrement.Inc()
	}
}

func RecordWSStmtDisconnect() {
	if config.Conf.UploadKeeper.Enable {
		WSStmtConn.Dec()
		WSStmtConnDecrement.Inc()
	}
}

func RecordWSWSConn() {
	if config.Conf.UploadKeeper.Enable {
		WSWSConn.Inc()
		WSWSConnIncrement.Inc()
	}
}

func RecordWSWSDisconnect() {
	if config.Conf.UploadKeeper.Enable {
		WSWSConn.Dec()
		WSWSConnDecrement.Inc()
	}
}

func RecordWSTMQConn() {
	if config.Conf.UploadKeeper.Enable {
		WSTMQConn.Inc()
		WSTMQConnIncrement.Inc()
	}
}

func RecordWSTMQDisconnect() {
	if config.Conf.UploadKeeper.Enable {
		WSTMQConn.Dec()
		WSTMQConnDecrement.Inc()
	}
}

var closeChan = make(chan struct{}, 1)

// just for test
func StopUpload() {
	closeChan <- struct{}{}
}

func StartUpload() {
	if config.Conf.UploadKeeper.Enable {
		client := &http.Client{
			Timeout: config.Conf.UploadKeeper.Timeout,
		}
		p, err := process.NewProcess(int32(os.Getpid()))
		if err != nil {
			logger.Panicf("get process error, err:%s", err)
		}
		// init cpu record
		_, err = p.Percent(0)
		if err != nil {
			logger.Panicf("get process cpu percent error, err")
		}
		go func() {
			nextUploadTime := getNextUploadTime()
			logger.Debugf("start upload keeper when %s", nextUploadTime.Format("2006-01-02 15:04:05.000000000"))
			startTimer := time.NewTimer(time.Until(nextUploadTime))
			<-startTimer.C
			startTimer.Stop()
			go func() {
				reqID := generator.GetUploadKeeperReqID()
				err := upload(p, client, reqID)
				if err != nil {
					logger.Errorf("upload_id:0x%x, upload to keeper error, err:%s", reqID, err)
				}
			}()
			ticker := time.NewTicker(config.Conf.UploadKeeper.Interval)
			for {
				select {
				case <-closeChan:
					ticker.Stop()
					return
				case <-ticker.C:
					go func() {
						reqID := generator.GetUploadKeeperReqID()
						err := upload(p, client, reqID)
						if err != nil {
							logger.Errorf("upload_id:0x%x, upload to keeper error, err:%s", reqID, err)
						}
					}()
				}
			}
		}()
	}
}

func getNextUploadTime() time.Time {
	now := time.Now()
	next := now.Round(config.Conf.UploadKeeper.Interval)
	if next.Before(now) {
		next = next.Add(config.Conf.UploadKeeper.Interval)
	}
	return next
}

type UploadData struct {
	Ts           int64          `json:"ts"`
	Metrics      map[string]int `json:"metrics"`
	Endpoint     string         `json:"endpoint"`
	ExtraMetrics []*ExtraMetric `json:"extra_metrics"`
}

type ExtraMetric struct {
	Ts       string   `json:"ts"`
	Protocol int      `json:"protocol"`
	Tables   []*Table `json:"tables"`
}

type Table struct {
	Name         string         `json:"name"`
	MetricGroups []*MetricGroup `json:"metric_groups"`
}

type MetricGroup struct {
	Tags    []*Tag    `json:"tags"`
	Metrics []*Metric `json:"metrics"`
}

type Tag struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type Metric struct {
	Name  string      `json:"name"`
	Value interface{} `json:"value"`
}

func getLimiterMetrics() *Table {
	limiterMetrics := limiter.GetLimiterMetrics()
	if len(limiterMetrics) == 0 {
		return nil
	}
	table := &Table{
		Name: "adapter_request_limit",
	}
	table.MetricGroups = make([]*MetricGroup, 0, len(limiterMetrics))
	for user, requestLimiterMetrics := range limiterMetrics {
		table.MetricGroups = append(table.MetricGroups, &MetricGroup{
			Tags: []*Tag{
				{
					Name:  "endpoint",
					Value: identity,
				},
				{
					Name:  "user",
					Value: user,
				},
			},
			Metrics: []*Metric{
				{
					Name:  "query_limit",
					Value: requestLimiterMetrics.QueryLimit,
				},
				{
					Name:  "query_max_wait",
					Value: requestLimiterMetrics.MaxWait,
				},
				{
					Name:  "query_inflight",
					Value: requestLimiterMetrics.Inflight,
				},
				{
					Name:  "query_wait_count",
					Value: requestLimiterMetrics.WaitCount,
				},
				{
					Name:  "query_count",
					Value: requestLimiterMetrics.TotalCount,
				},
				{
					Name:  "query_wait_fail_count",
					Value: requestLimiterMetrics.FailCount,
				},
			},
		})
	}
	return table
}

func getInputJsonMetrics() *Table {
	if len(InputJsonMetricsMap) == 0 {
		return nil
	}
	table := &Table{
		Name: "adapter_input_json",
	}
	table.MetricGroups = make([]*MetricGroup, 0, len(InputJsonMetricsMap))
	for key, value := range InputJsonMetricsMap {
		totalRows := value.TotalRows.Value()
		value.TotalRows.Sub(totalRows)

		successRows := value.SuccessRows.Value()
		value.SuccessRows.Sub(successRows)

		failRows := value.FailRows.Value()
		value.FailRows.Sub(failRows)

		affectedRows := value.AffectedRows.Value()
		value.AffectedRows.Sub(affectedRows)

		inflightRows := value.InflightRows.Value()
		table.MetricGroups = append(table.MetricGroups, &MetricGroup{
			Tags: []*Tag{
				{
					Name:  "url_endpoint",
					Value: key,
				},
				{
					Name:  "endpoint",
					Value: identity,
				},
			},

			Metrics: []*Metric{
				{
					Name:  "total_rows",
					Value: totalRows,
				},
				{
					Name:  "success_rows",
					Value: successRows,
				},
				{
					Name:  "fail_rows",
					Value: failRows,
				},
				{
					Name:  "inflight_rows",
					Value: inflightRows,
				},
				{
					Name:  "affected_rows",
					Value: affectedRows,
				},
			},
		})
	}
	return table
}

func upload(p *process.Process, client *http.Client, reqID int64) error {
	ts := time.Now().Round(config.Conf.UploadKeeper.Interval)
	extraMetric, err := generateExtraMetrics(ts, p)
	if err != nil {
		logger.Errorf("generate extra metrics error, err:%s", err)
		return err
	}
	data := UploadData{
		Ts:           ts.Unix(),
		Metrics:      make(map[string]int, len(recordMetrics)+len(inflightMetrics)),
		Endpoint:     identity,
		ExtraMetrics: extraMetric,
	}
	for _, metric := range recordMetrics {
		value := metric.Value()
		data.Metrics[metric.MetricName()] = int(value)
		metric.Sub(value)
	}
	for _, metric := range inflightMetrics {
		data.Metrics[metric.MetricName()] = int(metric.Value())
	}
	jsonData, err := innerjson.Marshal(data)
	if err != nil {
		return err
	}
	err = doRequest(client, jsonData, reqID)
	if err != nil {
		for i := 0; i < int(config.Conf.UploadKeeper.RetryTimes); i++ {
			logger.Debugf("upload_id:0x%x, upload to keeper error, will retry in %s", reqID, config.Conf.UploadKeeper.RetryInterval)
			time.Sleep(config.Conf.UploadKeeper.RetryInterval)
			logger.Debugf("upload_id:0x%x, retry upload to keeper, retry times:%d", reqID, i+1)
			err = doRequest(client, jsonData, reqID)
			if err == nil {
				return nil
			}
		}
	}
	return err
}

func generateExtraMetrics(ts time.Time, p *process.Process) ([]*ExtraMetric, error) {
	memState, err := p.MemoryInfo()
	if err != nil {
		return nil, fmt.Errorf("get memory info error, err:%s", err.Error())
	}
	cpu, err := p.Percent(0)
	if err != nil {
		return nil, fmt.Errorf("get cpu percent error, err:%s", err.Error())
	}
	cpu = cpu / float64(runtime.NumCPU())
	memStats := new(runtime.MemStats)
	runtime.ReadMemStats(memStats)
	queryConnInc := WSQueryConnIncrement.Value()
	WSQueryConnIncrement.Sub(queryConnInc)

	queryConnDec := WSQueryConnDecrement.Value()
	WSQueryConnDecrement.Sub(queryConnDec)

	stmtConnInc := WSStmtConnIncrement.Value()
	WSStmtConnIncrement.Sub(stmtConnInc)

	stmtConnDec := WSStmtConnDecrement.Value()
	WSStmtConnDecrement.Sub(stmtConnDec)

	smlConnInc := WSSMLConnIncrement.Value()
	WSSMLConnIncrement.Sub(smlConnInc)

	smlConnDec := WSSMLConnDecrement.Value()
	WSSMLConnDecrement.Sub(smlConnDec)

	wsConnInc := WSWSConnIncrement.Value()
	WSWSConnIncrement.Sub(wsConnInc)

	wsConnDec := WSWSConnDecrement.Value()
	WSWSConnDecrement.Sub(wsConnDec)

	tmqConnInc := WSTMQConnIncrement.Value()
	WSTMQConnIncrement.Sub(tmqConnInc)

	tmqConnDec := WSTMQConnDecrement.Value()
	WSTMQConnDecrement.Sub(tmqConnDec)

	statusTable := &Table{
		Name: "adapter_status",
		MetricGroups: []*MetricGroup{
			{
				Tags: []*Tag{
					{
						Name:  "endpoint",
						Value: identity,
					},
				},
				Metrics: []*Metric{
					{
						Name:  "go_heap_sys",
						Value: memStats.HeapSys,
					},
					{
						Name:  "go_heap_inuse",
						Value: memStats.HeapInuse,
					},
					{
						Name:  "go_stack_sys",
						Value: memStats.StackSys,
					},
					{
						Name:  "go_stack_inuse",
						Value: memStats.StackInuse,
					},
					{
						Name:  "rss",
						Value: memState.RSS,
					},
					{
						Name:  "ws_query_conn",
						Value: WSQueryConn.Value(),
					},
					{
						Name:  "ws_stmt_conn",
						Value: WSStmtConn.Value(),
					},
					{
						Name:  "ws_sml_conn",
						Value: WSSMLConn.Value(),
					},
					{
						Name:  "ws_ws_conn",
						Value: WSWSConn.Value(),
					},
					{
						Name:  "ws_tmq_conn",
						Value: WSTMQConn.Value(),
					},
					{
						Name:  "async_c_limit",
						Value: config.Conf.MaxAsyncMethodLimit,
					},
					{
						Name:  "async_c_inflight",
						Value: AsyncCInflight.Value(),
					},
					{
						Name:  "sync_c_limit",
						Value: config.Conf.MaxSyncMethodLimit,
					},
					{
						Name:  "sync_c_inflight",
						Value: SyncCInflight.Value(),
					},
					{
						Name:  "ws_query_conn_inc",
						Value: queryConnInc,
					},
					{
						Name:  "ws_query_conn_dec",
						Value: queryConnDec,
					},
					{
						Name:  "ws_stmt_conn_inc",
						Value: stmtConnInc,
					},
					{
						Name:  "ws_stmt_conn_dec",
						Value: stmtConnDec,
					},
					{
						Name:  "ws_sml_conn_inc",
						Value: smlConnInc,
					},
					{
						Name:  "ws_sml_conn_dec",
						Value: smlConnDec,
					},
					{
						Name:  "ws_ws_conn_inc",
						Value: wsConnInc,
					},
					{
						Name:  "ws_ws_conn_dec",
						Value: wsConnDec,
					},
					{
						Name:  "ws_tmq_conn_inc",
						Value: tmqConnInc,
					},
					{
						Name:  "ws_tmq_conn_dec",
						Value: tmqConnDec,
					},
					{
						Name:  "ws_query_sql_result_count",
						Value: WSQuerySqlResultCount.Value(),
					},
					{
						Name:  "ws_stmt_stmt_count",
						Value: WSStmtStmtCount.Value(),
					},
					{
						Name:  "ws_ws_sql_result_count",
						Value: WSWSSqlResultCount.Value(),
					},
					{
						Name:  "ws_ws_stmt_count",
						Value: WSWSStmtCount.Value(),
					},
					{
						Name:  "ws_ws_stmt2_count",
						Value: WSWSStmt2Count.Value(),
					},
					{
						Name:  "cpu_percent",
						Value: cpu,
					},
				},
			},
		},
	}
	connTable := &Table{
		Name: "adapter_conn_pool",
	}
	ConnPoolInUse.Range(func(k, v interface{}) bool {
		connTable.MetricGroups = append(connTable.MetricGroups, &MetricGroup{
			Tags: []*Tag{
				{
					Name:  "endpoint",
					Value: identity,
				},
				{
					Name:  "user",
					Value: k.(string),
				},
			},
			Metrics: []*Metric{
				{
					Name:  "conn_pool_total",
					Value: config.Conf.Pool.MaxConnect,
				},
				{
					Name:  "conn_pool_in_use",
					Value: v.(*metrics.Gauge).Value(),
				},
			},
		})
		return true
	})
	cInterfaceMetrics := make([]*Metric, len(cInterfaceCountMetrics))
	for i := 0; i < len(cInterfaceCountMetrics); i++ {
		cInterfaceMetrics[i] = &Metric{
			Name:  cInterfaceCountMetrics[i].MetricName(),
			Value: cInterfaceCountMetrics[i].Value(),
		}
	}
	cInterfaceTable := &Table{
		Name: "adapter_c_interface",
		MetricGroups: []*MetricGroup{
			{
				Tags: []*Tag{
					{
						Name:  "endpoint",
						Value: identity,
					},
				},
				Metrics: cInterfaceMetrics,
			},
		},
	}
	metric := &ExtraMetric{
		Ts:       strconv.FormatInt(ts.UnixMilli(), 10),
		Protocol: 2,
		Tables: []*Table{
			statusTable,
		},
	}
	if len(connTable.MetricGroups) > 0 {
		metric.Tables = append(metric.Tables, connTable)
	}
	metric.Tables = append(metric.Tables, cInterfaceTable)
	limitTable := getLimiterMetrics()
	if limitTable != nil {
		metric.Tables = append(metric.Tables, limitTable)
	}
	inputJsonTable := getInputJsonMetrics()
	if inputJsonTable != nil {
		metric.Tables = append(metric.Tables, inputJsonTable)
	}
	return []*ExtraMetric{metric}, nil
}
func doRequest(client *http.Client, data []byte, reqID int64) error {
	req, err := http.NewRequest(http.MethodPost, config.Conf.UploadKeeper.Url, bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("create new request error: %s", err)
	}
	req.Header.Set("X-QID", fmt.Sprintf("0x%x", reqID))
	logger.Tracef("upload_id:0x%x, upload to keeper, url:%s, data:%s", reqID, config.Conf.UploadKeeper.Url, data)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	logger.Debugf("upload_id:0x%x, upload to keeper success", reqID)
	if resp.StatusCode != http.StatusOK {
		logger.Errorf("upload_id:0x%x, upload keeper error, code: %d", reqID, resp.StatusCode)
		return fmt.Errorf("upload keeper error, code: %d", resp.StatusCode)
	}
	return nil
}
