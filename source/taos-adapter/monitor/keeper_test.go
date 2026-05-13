package monitor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/shirou/gopsutil/v3/process"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/thread"
	"github.com/taosdata/taosadapter/v3/tools/limiter"
	"github.com/taosdata/taosadapter/v3/tools/sqltype"
)

func TestRecordRequest(t *testing.T) {
	// Mock the configuration to enable monitoring
	config.Conf.UploadKeeper.Enable = true
	InitKeeper()

	// Test with an INSERT SQL statement
	sql := "INSERT INTO table_name VALUES (1, 'data')"
	result := RestRecordRequest(sql)
	if result != sqltype.InsertType {
		t.Errorf("Expected InsertType, got %v", result)
	}
	if RestTotal.Value() != 1.0 {
		t.Errorf("Expected RestTotal to be 1.0, got %f", RestTotal.Value())
	}
	if RestInProcess.Value() != 1.0 {
		t.Errorf("Expected RestInProcess to be 1.0, got %f", RestInProcess.Value())
	}
	if RestWrite.Value() != 1.0 {
		t.Errorf("Expected RestWrite to be 1.0, got %f", RestWrite.Value())
	}
	if RestWriteInProcess.Value() != 1.0 {
		t.Errorf("Expected RestWriteInProcess to be 1.0, got %f", RestWriteInProcess.Value())
	}

	// Test with a SELECT SQL statement
	sql = "SELECT * FROM table_name"
	result = RestRecordRequest(sql)
	if result != sqltype.SelectType {
		t.Errorf("Expected SelectType, got %v", result)
	}
	if RestTotal.Value() != 2.0 {
		t.Errorf("Expected RestTotal to be 2.0, got %f", RestTotal.Value())
	}
	if RestInProcess.Value() != 2.0 {
		t.Errorf("Expected RestInProcess to be 2.0, got %f", RestInProcess.Value())
	}
	if RestQuery.Value() != 1.0 {
		t.Errorf("Expected RestQuery to be 1.0, got %f", RestQuery.Value())
	}
	if RestQueryInProcess.Value() != 1.0 {
		t.Errorf("Expected RestQueryInProcess to be 1.0, got %f", RestQueryInProcess.Value())
	}

	// Test with an unsupported SQL statement
	sql = "UPDATE table_name SET column = 'value'"
	result = RestRecordRequest(sql)
	if result != sqltype.OtherType {
		t.Errorf("Expected OtherType, got %v", result)
	}
	if RestTotal.Value() != 3.0 {
		t.Errorf("Expected RestTotal to be 3.0, got %f", RestTotal.Value())
	}
	if RestInProcess.Value() != 3.0 {
		t.Errorf("Expected RestInProcess to be 3.0, got %f", RestInProcess.Value())
	}
	if RestOther.Value() != 1.0 {
		t.Errorf("Expected RestOther to be 1.0, got %f", RestOther.Value())
	}
	if RestOtherInProcess.Value() != 1.0 {
		t.Errorf("Expected RestOtherInProcess to be 1.0, got %f", RestOtherInProcess.Value())
	}

	// Reset the configuration
	config.Conf.UploadKeeper.Enable = false
}

func TestRecordResult(t *testing.T) {
	// Mock the configuration to enable monitoring
	config.Conf.UploadKeeper.Enable = true
	InitKeeper()
	// Test a successful result
	sqlType := RestRecordRequest("INSERT INTO table_name VALUES (1, 'data')")
	success := true
	RestRecordResult(sqlType, success)
	if RestSuccess.Value() != 1.0 {
		t.Errorf("Expected RestSuccess to be 1.0, got %f", RestSuccess.Value())
	}
	if RestInProcess.Value() != 0.0 {
		t.Errorf("Expected RestInProcess to be 0.0, got %f", RestInProcess.Value())
	}
	if RestWriteInProcess.Value() != 0.0 {
		t.Errorf("Expected RestWriteInProcess to be 0.0, got %f", RestWriteInProcess.Value())
	}

	// Test a failed result
	sqlType = RestRecordRequest("SELECT * FROM table_name")
	success = false
	RestRecordResult(sqlType, success)
	if RestFail.Value() != 1.0 {
		t.Errorf("Expected RestFail to be 1.0, got %f", RestFail.Value())
	}
	if RestInProcess.Value() != 0.0 {
		t.Errorf("Expected RestInProcess to be 0.0, got %f", RestInProcess.Value())
	}
	if RestQueryInProcess.Value() != 0.0 {
		t.Errorf("Expected RestQueryInProcess to be 0.0, got %f", RestQueryInProcess.Value())
	}

	// Reset the configuration
	config.Conf.UploadKeeper.Enable = false
}

func TestWSRecordRequest(t *testing.T) {
	// Mock the configuration to enable monitoring
	config.Conf.UploadKeeper.Enable = true
	InitKeeper()

	// Test with an INSERT SQL statement
	sql := "INSERT INTO table_name VALUES (1, 'data')"
	result := WSRecordRequest(sql)
	if result != sqltype.InsertType {
		t.Errorf("Expected InsertType, got %v", result)
	}
	if WSTotal.Value() != 1.0 {
		t.Errorf("Expected WSTotal to be 1.0, got %f", WSTotal.Value())
	}
	if WSInProcess.Value() != 1.0 {
		t.Errorf("Expected WSInProcess to be 1.0, got %f", WSInProcess.Value())
	}
	if WSWrite.Value() != 1.0 {
		t.Errorf("Expected WSWrite to be 1.0, got %f", WSWrite.Value())
	}
	if WSWriteInProcess.Value() != 1.0 {
		t.Errorf("Expected WSWriteInProcess to be 1.0, got %f", WSWriteInProcess.Value())
	}

	// Test with a SELECT SQL statement
	sql = "SELECT * FROM table_name"
	result = WSRecordRequest(sql)
	if result != sqltype.SelectType {
		t.Errorf("Expected SelectType, got %v", result)
	}
	if WSTotal.Value() != 2.0 {
		t.Errorf("Expected WSTotal to be 2.0, got %f", WSTotal.Value())
	}
	if WSInProcess.Value() != 2.0 {
		t.Errorf("Expected WSInProcess to be 2.0, got %f", WSInProcess.Value())
	}
	if WSQuery.Value() != 1.0 {
		t.Errorf("Expected WSQuery to be 1.0, got %f", WSQuery.Value())
	}
	if WSQueryInProcess.Value() != 1.0 {
		t.Errorf("Expected WSQueryInProcess to be 1.0, got %f", WSQueryInProcess.Value())
	}

	// Test with an unsupported SQL statement
	sql = "UPDATE table_name SET column = 'value'"
	result = WSRecordRequest(sql)
	if result != sqltype.OtherType {
		t.Errorf("Expected OtherType, got %v", result)
	}
	if WSTotal.Value() != 3.0 {
		t.Errorf("Expected WSTotal to be 3.0, got %f", WSTotal.Value())
	}
	if WSInProcess.Value() != 3.0 {
		t.Errorf("Expected WSInProcess to be 3.0, got %f", WSInProcess.Value())
	}
	if WSOther.Value() != 1.0 {
		t.Errorf("Expected WSOther to be 1.0, got %f", WSOther.Value())
	}
	if WSOtherInProcess.Value() != 1.0 {
		t.Errorf("Expected WSOtherInProcess to be 1.0, got %f", WSOtherInProcess.Value())
	}

	// Reset the configuration
	config.Conf.UploadKeeper.Enable = false
}

func TestWSRecordResult(t *testing.T) {
	// Mock the configuration to enable monitoring
	config.Conf.UploadKeeper.Enable = true
	InitKeeper()
	// Test a successful result
	sqlType := WSRecordRequest("INSERT INTO table_name VALUES (1, 'data')")
	success := true
	WSRecordResult(sqlType, success)
	if WSSuccess.Value() != 1.0 {
		t.Errorf("Expected WSSuccess to be 1.0, got %f", WSSuccess.Value())
	}
	if WSInProcess.Value() != 0.0 {
		t.Errorf("Expected WSInProcess to be 0.0, got %f", WSInProcess.Value())
	}
	if WSWriteInProcess.Value() != 0.0 {
		t.Errorf("Expected WSWriteInProcess to be 0.0, got %f", WSWriteInProcess.Value())
	}

	// Test a failed result
	sqlType = WSRecordRequest("SELECT * FROM table_name")
	success = false
	WSRecordResult(sqlType, success)
	if WSFail.Value() != 1.0 {
		t.Errorf("Expected WSFail to be 1.0, got %f", WSFail.Value())
	}
	if WSInProcess.Value() != 0.0 {
		t.Errorf("Expected WSInProcess to be 0.0, got %f", WSInProcess.Value())
	}
	if WSQueryInProcess.Value() != 0.0 {
		t.Errorf("Expected WSQueryInProcess to be 0.0, got %f", WSQueryInProcess.Value())
	}

	// Reset the configuration
	config.Conf.UploadKeeper.Enable = false
}

func TestUpload(t *testing.T) {
	config.Conf.UploadKeeper.Enable = true
	InitKeeper()
	times := 0
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/upload" {
			//b, err := io.ReadAll(r.Body)
			//if err != nil {
			//	t.Error(err)
			//	return
			//}
			//t.Log(string(b))
			times += 1
			switch times {
			case 1:
				w.WriteHeader(http.StatusRequestTimeout)
				return
			case 5:
				close(done)
				fallthrough
			default:
				w.WriteHeader(http.StatusOK)
				return
			}
		}
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()
	config.Conf.UploadKeeper.Url = server.URL + "/upload"
	config.Conf.UploadKeeper.Interval = time.Second
	config.Conf.UploadKeeper.RetryTimes = 1
	config.Conf.UploadKeeper.RetryInterval = time.Millisecond * 100
	StartUpload()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*8)
	defer cancel()
	select {
	case <-ctx.Done():
		t.Errorf("upload failed")
	case <-done:
	}
	StopUpload()
	time.Sleep(time.Second * 2)
	config.Conf.UploadKeeper.Enable = false
	config.Conf.UploadKeeper.Interval = time.Second * 5
}

func TestRecordWSConn(t *testing.T) {
	config.Conf.UploadKeeper.Enable = true
	InitKeeper()
	RecordWSQueryConn()
	assert.Equal(t, float64(1), WSQueryConn.Value())
	RecordWSQueryDisconnect()
	assert.Equal(t, float64(0), WSQueryConn.Value())
	RecordWSSMLConn()
	assert.Equal(t, float64(1), WSSMLConn.Value())
	RecordWSSMLDisconnect()
	assert.Equal(t, float64(0), WSSMLConn.Value())
	RecordWSStmtConn()
	assert.Equal(t, float64(1), WSStmtConn.Value())
	RecordWSStmtDisconnect()
	assert.Equal(t, float64(0), WSStmtConn.Value())
	RecordWSWSConn()
	assert.Equal(t, float64(1), WSWSConn.Value())
	RecordWSWSDisconnect()
	assert.Equal(t, float64(0), WSWSConn.Value())
	RecordWSTMQConn()
	assert.Equal(t, float64(1), WSTMQConn.Value())
	RecordWSTMQDisconnect()
	assert.Equal(t, float64(0), WSTMQConn.Value())
	config.Conf.UploadKeeper.Enable = false
}

func TestGenerateExtraMetrics(t *testing.T) {
	config.Conf.UploadKeeper.Enable = true
	InitKeeper()
	ts := time.Now()
	p, err := process.NewProcess(int32(os.Getpid()))
	assert.NoError(t, err)
	_, err = p.Percent(0)
	require.NoError(t, err)
	go func() {
		x := 0
		for {
			x += 1
			if x > 1000 {
				x = 1
			}
		}
	}()
	inputJsonMetrics := NewInputJsonMetric("test_metrics")
	require.NotNil(t, inputJsonMetrics)
	inputJsonMetrics.TotalRows.Add(5)
	inputJsonMetrics.SuccessRows.Add(1)
	inputJsonMetrics.FailRows.Add(1)
	inputJsonMetrics.AffectedRows.Add(10)
	inputJsonMetrics.InflightRows.Add(4)
	RecordWSQueryConn()
	RecordWSSMLConn()
	RecordWSStmtConn()
	RecordWSWSConn()
	RecordWSTMQConn()
	WSQueryConnIncrement.Inc()
	WSQueryConnDecrement.Inc()
	WSStmtConnIncrement.Inc()
	WSStmtConnDecrement.Inc()
	WSSMLConnIncrement.Inc()
	WSSMLConnDecrement.Inc()
	WSWSConnIncrement.Inc()
	WSWSConnDecrement.Inc()
	WSTMQConnIncrement.Inc()
	WSTMQConnDecrement.Inc()

	WSQuerySqlResultCount.Inc()
	WSStmtStmtCount.Inc()
	WSWSSqlResultCount.Inc()
	WSWSStmtCount.Inc()
	WSWSStmt2Count.Inc()
	thread.AsyncSemaphore.Acquire()
	thread.SyncSemaphore.Acquire()
	g := RecordNewConnectionPool("test")
	g.Inc()
	time.Sleep(time.Second)
	metrics, err := generateExtraMetrics(ts, p)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(metrics))
	expectCMetricNames := []string{
		"taos_connect_total",
		"taos_connect_success",
		"taos_connect_fail",
		"taos_close_total",
		"taos_close_success",
		"taos_schemaless_insert_total",
		"taos_schemaless_insert_success",
		"taos_schemaless_insert_fail",
		"taos_schemaless_free_result_total",
		"taos_schemaless_free_result_success",
		"taos_query_total",
		"taos_query_success",
		"taos_query_fail",
		"taos_query_free_result_total",
		"taos_query_free_result_success",
		"taos_query_a_with_reqid_total",
		"taos_query_a_with_reqid_success",
		"taos_query_a_with_reqid_callback_total",
		"taos_query_a_with_reqid_callback_success",
		"taos_query_a_with_reqid_callback_fail",
		"taos_query_a_free_result_total",
		"taos_query_a_free_result_success",
		"tmq_consumer_poll_result_total",
		"tmq_free_result_total",
		"tmq_free_result_success",
		"taos_stmt2_init_total",
		"taos_stmt2_init_success",
		"taos_stmt2_init_fail",
		"taos_stmt2_close_total",
		"taos_stmt2_close_success",
		"taos_stmt2_close_fail",
		"taos_stmt2_get_fields_total",
		"taos_stmt2_get_fields_success",
		"taos_stmt2_get_fields_fail",
		"taos_stmt2_free_fields_total",
		"taos_stmt2_free_fields_success",
		"taos_stmt_init_with_reqid_total",
		"taos_stmt_init_with_reqid_success",
		"taos_stmt_init_with_reqid_fail",
		"taos_stmt_close_total",
		"taos_stmt_close_success",
		"taos_stmt_close_fail",
		"taos_stmt_get_tag_fields_total",
		"taos_stmt_get_tag_fields_success",
		"taos_stmt_get_tag_fields_fail",
		"taos_stmt_get_col_fields_total",
		"taos_stmt_get_col_fields_success",
		"taos_stmt_get_col_fields_fail",
		"taos_stmt_reclaim_fields_total",
		"taos_stmt_reclaim_fields_success",
		"tmq_get_json_meta_total",
		"tmq_get_json_meta_success",
		"tmq_free_json_meta_total",
		"tmq_free_json_meta_success",
		"taos_fetch_whitelist_a_total",
		"taos_fetch_whitelist_a_success",
		"taos_fetch_whitelist_a_callback_total",
		"taos_fetch_whitelist_a_callback_success",
		"taos_fetch_whitelist_a_callback_fail",
		"taos_fetch_rows_a_total",
		"taos_fetch_rows_a_success",
		"taos_fetch_rows_a_callback_total",
		"taos_fetch_rows_a_callback_success",
		"taos_fetch_rows_a_callback_fail",
		"taos_fetch_raw_block_a_total",
		"taos_fetch_raw_block_a_success",
		"taos_fetch_raw_block_a_callback_total",
		"taos_fetch_raw_block_a_callback_success",
		"taos_fetch_raw_block_a_callback_fail",
		"tmq_get_raw_total",
		"tmq_get_raw_success",
		"tmq_get_raw_fail",
		"tmq_free_raw_total",
		"tmq_free_raw_success",
		"tmq_consumer_new_total",
		"tmq_consumer_new_success",
		"tmq_consumer_new_fail",
		"tmq_consumer_close_total",
		"tmq_consumer_close_success",
		"tmq_consumer_close_fail",
		"tmq_subscribe_total",
		"tmq_subscribe_success",
		"tmq_subscribe_fail",
		"tmq_unsubscribe_total",
		"tmq_unsubscribe_success",
		"tmq_unsubscribe_fail",
		"tmq_list_new_total",
		"tmq_list_new_success",
		"tmq_list_new_fail",
		"tmq_list_destroy_total",
		"tmq_list_destroy_success",
		"tmq_conf_new_total",
		"tmq_conf_new_success",
		"tmq_conf_new_fail",
		"tmq_conf_destroy_total",
		"tmq_conf_destroy_success",
		"taos_stmt2_prepare_total",
		"taos_stmt2_prepare_success",
		"taos_stmt2_prepare_fail",
		"taos_stmt2_is_insert_total",
		"taos_stmt2_is_insert_success",
		"taos_stmt2_is_insert_fail",
		"taos_stmt2_bind_param_total",
		"taos_stmt2_bind_param_success",
		"taos_stmt2_bind_param_fail",
		"taos_stmt2_exec_total",
		"taos_stmt2_exec_success",
		"taos_stmt2_exec_fail",
		"taos_stmt2_error_total",
		"taos_stmt2_error_success",
		"taos_fetch_row_total",
		"taos_fetch_row_success",
		"taos_is_update_query_total",
		"taos_is_update_query_success",
		"taos_affected_rows_total",
		"taos_affected_rows_success",
		"taos_num_fields_total",
		"taos_num_fields_success",
		"taos_fetch_fields_e_total",
		"taos_fetch_fields_e_success",
		"taos_fetch_fields_e_fail",
		"taos_result_precision_total",
		"taos_result_precision_success",
		"taos_get_raw_block_total",
		"taos_get_raw_block_success",
		"taos_fetch_raw_block_total",
		"taos_fetch_raw_block_success",
		"taos_fetch_raw_block_fail",
		"taos_fetch_lengths_total",
		"taos_fetch_lengths_success",
		"taos_write_raw_block_with_reqid_total",
		"taos_write_raw_block_with_reqid_success",
		"taos_write_raw_block_with_reqid_fail",
		"taos_write_raw_block_with_fields_with_reqid_total",
		"taos_write_raw_block_with_fields_with_reqid_success",
		"taos_write_raw_block_with_fields_with_reqid_fail",
		"tmq_write_raw_total",
		"tmq_write_raw_success",
		"tmq_write_raw_fail",
		"taos_stmt_prepare_total",
		"taos_stmt_prepare_success",
		"taos_stmt_prepare_fail",
		"taos_stmt_is_insert_total",
		"taos_stmt_is_insert_success",
		"taos_stmt_is_insert_fail",
		"taos_stmt_set_tbname_total",
		"taos_stmt_set_tbname_success",
		"taos_stmt_set_tbname_fail",
		"taos_stmt_set_tags_total",
		"taos_stmt_set_tags_success",
		"taos_stmt_set_tags_fail",
		"taos_stmt_bind_param_batch_total",
		"taos_stmt_bind_param_batch_success",
		"taos_stmt_bind_param_batch_fail",
		"taos_stmt_add_batch_total",
		"taos_stmt_add_batch_success",
		"taos_stmt_add_batch_fail",
		"taos_stmt_execute_total",
		"taos_stmt_execute_success",
		"taos_stmt_execute_fail",
		"taos_stmt_num_params_total",
		"taos_stmt_num_params_success",
		"taos_stmt_num_params_fail",
		"taos_stmt_get_param_total",
		"taos_stmt_get_param_success",
		"taos_stmt_get_param_fail",
		"taos_stmt_errstr_total",
		"taos_stmt_errstr_success",
		"taos_stmt_affected_rows_once_total",
		"taos_stmt_affected_rows_once_success",
		"taos_stmt_use_result_total",
		"taos_stmt_use_result_success",
		"taos_stmt_use_result_fail",
		"taos_select_db_total",
		"taos_select_db_success",
		"taos_select_db_fail",
		"taos_get_tables_vgId_total",
		"taos_get_tables_vgId_success",
		"taos_get_tables_vgId_fail",
		"taos_options_connection_total",
		"taos_options_connection_success",
		"taos_options_connection_fail",
		"taos_validate_sql_total",
		"taos_validate_sql_success",
		"taos_validate_sql_fail",
		"taos_check_server_status_total",
		"taos_check_server_status_success",
		"taos_get_current_db_total",
		"taos_get_current_db_success",
		"taos_get_current_db_fail",
		"taos_get_server_info_total",
		"taos_get_server_info_success",
		"taos_options_total",
		"taos_options_success",
		"taos_options_fail",
		"taos_set_conn_mode_total",
		"taos_set_conn_mode_success",
		"taos_set_conn_mode_fail",
		"taos_reset_current_db_total",
		"taos_reset_current_db_success",
		"taos_set_notify_cb_total",
		"taos_set_notify_cb_success",
		"taos_set_notify_cb_fail",
		"taos_errno_total",
		"taos_errno_success",
		"taos_errstr_total",
		"taos_errstr_success",
		"tmq_consumer_poll_total",
		"tmq_consumer_poll_success",
		"tmq_consumer_poll_fail",
		"tmq_subscription_total",
		"tmq_subscription_success",
		"tmq_subscription_fail",
		"tmq_list_append_total",
		"tmq_list_append_success",
		"tmq_list_append_fail",
		"tmq_list_get_size_total",
		"tmq_list_get_size_success",
		"tmq_err2str_total",
		"tmq_err2str_success",
		"tmq_conf_set_total",
		"tmq_conf_set_success",
		"tmq_conf_set_fail",
		"tmq_get_res_type_total",
		"tmq_get_res_type_success",
		"tmq_get_topic_name_total",
		"tmq_get_topic_name_success",
		"tmq_get_vgroup_id_total",
		"tmq_get_vgroup_id_success",
		"tmq_get_vgroup_offset_total",
		"tmq_get_vgroup_offset_success",
		"tmq_get_db_name_total",
		"tmq_get_db_name_success",
		"tmq_get_table_name_total",
		"tmq_get_table_name_success",
		"tmq_get_connect_total",
		"tmq_get_connect_success",
		"tmq_commit_sync_total",
		"tmq_commit_sync_success",
		"tmq_commit_sync_fail",
		"tmq_fetch_raw_block_total",
		"tmq_fetch_raw_block_success",
		"tmq_fetch_raw_block_fail",
		"tmq_get_topic_assignment_total",
		"tmq_get_topic_assignment_success",
		"tmq_get_topic_assignment_fail",
		"tmq_offset_seek_total",
		"tmq_offset_seek_success",
		"tmq_offset_seek_fail",
		"tmq_committed_total",
		"tmq_committed_success",
		"tmq_commit_offset_sync_fail",
		"tmq_position_total",
		"tmq_position_success",
		"tmq_commit_offset_sync_total",
		"tmq_commit_offset_sync_success",

		"taos_connect_totp_total",
		"taos_connect_totp_success",
		"taos_connect_totp_fail",
		"taos_connect_token_total",
		"taos_connect_token_success",
		"taos_connect_token_fail",
		"taos_get_connection_info_total",
		"taos_get_connection_info_success",
		"taos_get_connection_info_fail",
	}
	metric := metrics[0]
	assert.Equal(t, strconv.FormatInt(ts.UnixMilli(), 10), metric.Ts)
	assert.Equal(t, 2, metric.Protocol)
	assert.Equal(t, 4, len(metric.Tables))
	statusTable := metric.Tables[0]
	assert.Equal(t, "adapter_status", statusTable.Name)
	assert.Equal(t, 1, len(statusTable.MetricGroups))
	statusMetricGroup := statusTable.MetricGroups[0]
	assert.Equal(t, 1, len(statusMetricGroup.Tags))
	assert.Equal(t, "endpoint", statusMetricGroup.Tags[0].Name)
	assert.Equal(t, identity, statusMetricGroup.Tags[0].Value)
	statusMetric := statusMetricGroup.Metrics
	assert.Equal(t, 30, len(statusMetric))
	assert.Equal(t, "go_heap_sys", statusMetric[0].Name)
	assert.Greater(t, statusMetric[0].Value, uint64(0))
	assert.Equal(t, "go_heap_inuse", statusMetric[1].Name)
	assert.Less(t, statusMetric[1].Value, statusMetric[0].Value)
	assert.Equal(t, "go_stack_sys", statusMetric[2].Name)
	assert.Greater(t, statusMetric[2].Value, uint64(0))
	assert.Equal(t, "go_stack_inuse", statusMetric[3].Name)
	assert.LessOrEqual(t, statusMetric[3].Value, statusMetric[2].Value)
	assert.Equal(t, "rss", statusMetric[4].Name)
	assert.Greater(t, statusMetric[4].Value, statusMetric[0].Value.(uint64)+statusMetric[2].Value.(uint64))
	assert.Equal(t, "ws_query_conn", statusMetric[5].Name)
	assert.Equal(t, float64(1), statusMetric[5].Value)
	assert.Equal(t, "ws_stmt_conn", statusMetric[6].Name)
	assert.Equal(t, float64(1), statusMetric[6].Value)
	assert.Equal(t, "ws_sml_conn", statusMetric[7].Name)
	assert.Equal(t, float64(1), statusMetric[7].Value)
	assert.Equal(t, "ws_ws_conn", statusMetric[8].Name)
	assert.Equal(t, float64(1), statusMetric[8].Value)
	assert.Equal(t, "ws_tmq_conn", statusMetric[9].Name)
	assert.Equal(t, float64(1), statusMetric[9].Value)
	assert.Equal(t, "async_c_limit", statusMetric[10].Name)
	assert.Equal(t, config.Conf.MaxAsyncMethodLimit, statusMetric[10].Value)
	assert.Equal(t, "async_c_inflight", statusMetric[11].Name)
	assert.Equal(t, float64(1), statusMetric[11].Value)
	assert.Equal(t, "sync_c_limit", statusMetric[12].Name)
	assert.Equal(t, config.Conf.MaxSyncMethodLimit, statusMetric[12].Value)
	assert.Equal(t, "sync_c_inflight", statusMetric[13].Name)
	assert.Equal(t, float64(1), statusMetric[13].Value)
	assert.Equal(t, "ws_query_conn_inc", statusMetric[14].Name)
	assert.Equal(t, float64(2), statusMetric[14].Value)
	assert.Equal(t, "ws_query_conn_dec", statusMetric[15].Name)
	assert.Equal(t, float64(1), statusMetric[15].Value)
	assert.Equal(t, "ws_stmt_conn_inc", statusMetric[16].Name)
	assert.Equal(t, float64(2), statusMetric[16].Value)
	assert.Equal(t, "ws_stmt_conn_dec", statusMetric[17].Name)
	assert.Equal(t, float64(1), statusMetric[17].Value)
	assert.Equal(t, "ws_sml_conn_inc", statusMetric[18].Name)
	assert.Equal(t, float64(2), statusMetric[18].Value)
	assert.Equal(t, "ws_sml_conn_dec", statusMetric[19].Name)
	assert.Equal(t, float64(1), statusMetric[19].Value)
	assert.Equal(t, "ws_ws_conn_inc", statusMetric[20].Name)
	assert.Equal(t, float64(2), statusMetric[20].Value)
	assert.Equal(t, "ws_ws_conn_dec", statusMetric[21].Name)
	assert.Equal(t, float64(1), statusMetric[21].Value)
	assert.Equal(t, "ws_tmq_conn_inc", statusMetric[22].Name)
	assert.Equal(t, float64(2), statusMetric[22].Value)
	assert.Equal(t, "ws_tmq_conn_dec", statusMetric[23].Name)
	assert.Equal(t, float64(1), statusMetric[23].Value)
	assert.Equal(t, "ws_query_sql_result_count", statusMetric[24].Name)
	assert.Equal(t, float64(1), statusMetric[24].Value)
	assert.Equal(t, "ws_stmt_stmt_count", statusMetric[25].Name)
	assert.Equal(t, float64(1), statusMetric[25].Value)
	assert.Equal(t, "ws_ws_sql_result_count", statusMetric[26].Name)
	assert.Equal(t, float64(1), statusMetric[26].Value)
	assert.Equal(t, "ws_ws_stmt_count", statusMetric[27].Name)
	assert.Equal(t, float64(1), statusMetric[27].Value)
	assert.Equal(t, "ws_ws_stmt2_count", statusMetric[28].Name)
	assert.Equal(t, float64(1), statusMetric[28].Value)
	assert.Equal(t, "cpu_percent", statusMetric[29].Name)
	assert.Greater(t, statusMetric[29].Value, float64(0))
	t.Log(statusMetric[29].Value)

	connPoolTable := metric.Tables[1]
	assert.Equal(t, "adapter_conn_pool", connPoolTable.Name)
	assert.Equal(t, 1, len(connPoolTable.MetricGroups))
	assert.Equal(t, 2, len(connPoolTable.MetricGroups[0].Tags))
	assert.Equal(t, "endpoint", connPoolTable.MetricGroups[0].Tags[0].Name)
	assert.Equal(t, identity, connPoolTable.MetricGroups[0].Tags[0].Value)
	assert.Equal(t, "user", connPoolTable.MetricGroups[0].Tags[1].Name)
	assert.Equal(t, "test", connPoolTable.MetricGroups[0].Tags[1].Value)
	assert.Equal(t, 2, len(connPoolTable.MetricGroups[0].Metrics))
	assert.Equal(t, "conn_pool_total", connPoolTable.MetricGroups[0].Metrics[0].Name)
	assert.Equal(t, config.Conf.Pool.MaxConnect, connPoolTable.MetricGroups[0].Metrics[0].Value)
	assert.Equal(t, "conn_pool_in_use", connPoolTable.MetricGroups[0].Metrics[1].Name)
	assert.Equal(t, float64(1), connPoolTable.MetricGroups[0].Metrics[1].Value)

	inputJsonTable := metric.Tables[3]
	assert.Equal(t, "adapter_input_json", inputJsonTable.Name)
	assert.Equal(t, 1, len(inputJsonTable.MetricGroups))
	assert.Equal(t, 2, len(inputJsonTable.MetricGroups[0].Tags))
	assert.Equal(t, "url_endpoint", inputJsonTable.MetricGroups[0].Tags[0].Name)
	assert.Equal(t, "test_metrics", inputJsonTable.MetricGroups[0].Tags[0].Value)
	assert.Equal(t, "endpoint", inputJsonTable.MetricGroups[0].Tags[1].Name)
	assert.Equal(t, identity, inputJsonTable.MetricGroups[0].Tags[1].Value)
	assert.Equal(t, 5, len(inputJsonTable.MetricGroups[0].Metrics))
	assert.Equal(t, "total_rows", inputJsonTable.MetricGroups[0].Metrics[0].Name)
	assert.Equal(t, float64(5), inputJsonTable.MetricGroups[0].Metrics[0].Value)
	assert.Equal(t, "success_rows", inputJsonTable.MetricGroups[0].Metrics[1].Name)
	assert.Equal(t, float64(1), inputJsonTable.MetricGroups[0].Metrics[1].Value)
	assert.Equal(t, "fail_rows", inputJsonTable.MetricGroups[0].Metrics[2].Name)
	assert.Equal(t, float64(1), inputJsonTable.MetricGroups[0].Metrics[2].Value)
	assert.Equal(t, "inflight_rows", inputJsonTable.MetricGroups[0].Metrics[3].Name)
	assert.Equal(t, float64(4), inputJsonTable.MetricGroups[0].Metrics[3].Value)
	assert.Equal(t, "affected_rows", inputJsonTable.MetricGroups[0].Metrics[4].Name)
	assert.Equal(t, float64(10), inputJsonTable.MetricGroups[0].Metrics[4].Value)

	assert.Equal(t, float64(0), inputJsonMetrics.TotalRows.Value())
	assert.Equal(t, float64(0), inputJsonMetrics.SuccessRows.Value())
	assert.Equal(t, float64(0), inputJsonMetrics.FailRows.Value())
	assert.Equal(t, float64(0), inputJsonMetrics.AffectedRows.Value())
	assert.Equal(t, float64(4), inputJsonMetrics.InflightRows.Value())

	inputJsonMetrics.TotalRows.Add(20)
	inputJsonMetrics.SuccessRows.Add(12)
	inputJsonMetrics.FailRows.Add(1)
	inputJsonMetrics.AffectedRows.Add(200)
	inputJsonMetrics.InflightRows.Add(7)

	config.Conf.Request.QueryLimitEnable = true
	testUserName := "test_metrics"
	l := limiter.GetLimiter(testUserName)
	err = l.Acquire()
	require.NoError(t, err)

	RecordWSQueryDisconnect()
	RecordWSSMLDisconnect()
	RecordWSStmtDisconnect()
	RecordWSWSDisconnect()
	RecordWSTMQDisconnect()
	thread.AsyncSemaphore.Release()
	thread.SyncSemaphore.Release()
	WSQueryConnIncrement.Inc()
	WSQueryConnDecrement.Inc()
	WSStmtConnIncrement.Inc()
	WSStmtConnDecrement.Inc()
	WSSMLConnIncrement.Inc()
	WSSMLConnDecrement.Inc()
	WSWSConnIncrement.Inc()
	WSWSConnDecrement.Inc()
	WSTMQConnIncrement.Inc()
	WSTMQConnDecrement.Inc()

	WSQuerySqlResultCount.Inc()
	WSStmtStmtCount.Inc()
	WSWSSqlResultCount.Inc()
	WSWSStmtCount.Inc()
	WSWSStmt2Count.Inc()
	g = RecordNewConnectionPool("test")
	g.Inc()
	g.Inc()
	for i := 0; i < len(cInterfaceCountMetrics); i++ {
		cInterfaceCountMetrics[i].Inc()
	}
	time.Sleep(time.Second)
	metrics, err = generateExtraMetrics(ts, p)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(metrics))
	metric = metrics[0]
	assert.Equal(t, strconv.FormatInt(ts.UnixMilli(), 10), metric.Ts)
	assert.Equal(t, 2, metric.Protocol)
	assert.Equal(t, 5, len(metric.Tables))
	statusTable = metric.Tables[0]
	assert.Equal(t, "adapter_status", statusTable.Name)
	assert.Equal(t, 1, len(statusTable.MetricGroups))
	statusMetricGroup = statusTable.MetricGroups[0]
	assert.Equal(t, 1, len(statusMetricGroup.Tags))
	assert.Equal(t, "endpoint", statusMetricGroup.Tags[0].Name)
	assert.Equal(t, identity, statusMetricGroup.Tags[0].Value)
	statusMetric = statusMetricGroup.Metrics
	assert.Equal(t, 30, len(statusMetric))
	assert.Equal(t, "go_heap_sys", statusMetric[0].Name)
	assert.Greater(t, statusMetric[0].Value, uint64(0))
	assert.Equal(t, "go_heap_inuse", statusMetric[1].Name)
	assert.Less(t, statusMetric[1].Value, statusMetric[0].Value)
	assert.Equal(t, "go_stack_sys", statusMetric[2].Name)
	assert.Greater(t, statusMetric[2].Value, uint64(0))
	assert.Equal(t, "go_stack_inuse", statusMetric[3].Name)
	assert.LessOrEqual(t, statusMetric[3].Value, statusMetric[2].Value)
	assert.Equal(t, "rss", statusMetric[4].Name)
	assert.Greater(t, statusMetric[4].Value, statusMetric[0].Value.(uint64)+statusMetric[2].Value.(uint64))
	assert.Equal(t, "ws_query_conn", statusMetric[5].Name)
	assert.Equal(t, float64(0), statusMetric[5].Value)
	assert.Equal(t, "ws_stmt_conn", statusMetric[6].Name)
	assert.Equal(t, float64(0), statusMetric[6].Value)
	assert.Equal(t, "ws_sml_conn", statusMetric[7].Name)
	assert.Equal(t, float64(0), statusMetric[7].Value)
	assert.Equal(t, "ws_ws_conn", statusMetric[8].Name)
	assert.Equal(t, float64(0), statusMetric[8].Value)
	assert.Equal(t, "ws_tmq_conn", statusMetric[9].Name)
	assert.Equal(t, float64(0), statusMetric[9].Value)
	assert.Equal(t, "async_c_limit", statusMetric[10].Name)
	assert.Equal(t, config.Conf.MaxAsyncMethodLimit, statusMetric[10].Value)
	assert.Equal(t, "async_c_inflight", statusMetric[11].Name)
	assert.Equal(t, float64(0), statusMetric[11].Value)
	assert.Equal(t, "sync_c_limit", statusMetric[12].Name)
	assert.Equal(t, config.Conf.MaxSyncMethodLimit, statusMetric[12].Value)
	assert.Equal(t, "sync_c_inflight", statusMetric[13].Name)
	assert.Equal(t, float64(0), statusMetric[13].Value)
	assert.Equal(t, "ws_query_conn_inc", statusMetric[14].Name)
	assert.Equal(t, float64(1), statusMetric[14].Value)
	assert.Equal(t, "ws_query_conn_dec", statusMetric[15].Name)
	assert.Equal(t, float64(2), statusMetric[15].Value)
	assert.Equal(t, "ws_stmt_conn_inc", statusMetric[16].Name)
	assert.Equal(t, float64(1), statusMetric[16].Value)
	assert.Equal(t, "ws_stmt_conn_dec", statusMetric[17].Name)
	assert.Equal(t, float64(2), statusMetric[17].Value)
	assert.Equal(t, "ws_sml_conn_inc", statusMetric[18].Name)
	assert.Equal(t, float64(1), statusMetric[18].Value)
	assert.Equal(t, "ws_sml_conn_dec", statusMetric[19].Name)
	assert.Equal(t, float64(2), statusMetric[19].Value)
	assert.Equal(t, "ws_ws_conn_inc", statusMetric[20].Name)
	assert.Equal(t, float64(1), statusMetric[20].Value)
	assert.Equal(t, "ws_ws_conn_dec", statusMetric[21].Name)
	assert.Equal(t, float64(2), statusMetric[21].Value)
	assert.Equal(t, "ws_tmq_conn_inc", statusMetric[22].Name)
	assert.Equal(t, float64(1), statusMetric[22].Value)
	assert.Equal(t, "ws_tmq_conn_dec", statusMetric[23].Name)
	assert.Equal(t, float64(2), statusMetric[23].Value)
	assert.Equal(t, "ws_query_sql_result_count", statusMetric[24].Name)
	assert.Equal(t, float64(2), statusMetric[24].Value)
	assert.Equal(t, "ws_stmt_stmt_count", statusMetric[25].Name)
	assert.Equal(t, float64(2), statusMetric[25].Value)
	assert.Equal(t, "ws_ws_sql_result_count", statusMetric[26].Name)
	assert.Equal(t, float64(2), statusMetric[26].Value)
	assert.Equal(t, "ws_ws_stmt_count", statusMetric[27].Name)
	assert.Equal(t, float64(2), statusMetric[27].Value)
	assert.Equal(t, "ws_ws_stmt2_count", statusMetric[28].Name)
	assert.Equal(t, float64(2), statusMetric[28].Value)
	assert.Equal(t, "cpu_percent", statusMetric[29].Name)
	assert.Greater(t, statusMetric[29].Value, float64(0))
	t.Log(statusMetric[29].Value)
	connPoolTable = metric.Tables[1]
	assert.Equal(t, "adapter_conn_pool", connPoolTable.Name)
	assert.Equal(t, 1, len(connPoolTable.MetricGroups))
	assert.Equal(t, 2, len(connPoolTable.MetricGroups[0].Tags))
	assert.Equal(t, "endpoint", connPoolTable.MetricGroups[0].Tags[0].Name)
	assert.Equal(t, identity, connPoolTable.MetricGroups[0].Tags[0].Value)
	assert.Equal(t, "user", connPoolTable.MetricGroups[0].Tags[1].Name)
	assert.Equal(t, "test", connPoolTable.MetricGroups[0].Tags[1].Value)
	assert.Equal(t, 2, len(connPoolTable.MetricGroups[0].Metrics))
	assert.Equal(t, "conn_pool_total", connPoolTable.MetricGroups[0].Metrics[0].Name)
	assert.Equal(t, config.Conf.Pool.MaxConnect, connPoolTable.MetricGroups[0].Metrics[0].Value)
	assert.Equal(t, "conn_pool_in_use", connPoolTable.MetricGroups[0].Metrics[1].Name)
	assert.Equal(t, float64(2), connPoolTable.MetricGroups[0].Metrics[1].Value)
	cInterfaceTable := metric.Tables[2]
	assert.Equal(t, "adapter_c_interface", cInterfaceTable.Name)
	assert.Equal(t, 1, len(cInterfaceTable.MetricGroups))
	cInterfaceMetricGroup := cInterfaceTable.MetricGroups[0]
	assert.Equal(t, len(expectCMetricNames), len(cInterfaceMetricGroup.Metrics))
	assert.Equal(t, 1, len(cInterfaceMetricGroup.Tags))
	assert.Equal(t, "endpoint", cInterfaceMetricGroup.Tags[0].Name)
	assert.Equal(t, identity, cInterfaceMetricGroup.Tags[0].Value)
	cInterfaceMetric := cInterfaceMetricGroup.Metrics
	assert.Equal(t, len(expectCMetricNames), len(cInterfaceMetric))
	for i := 0; i < len(cInterfaceMetric); i++ {
		assert.Equal(t, expectCMetricNames[i], cInterfaceMetric[i].Name)
		assert.Greater(t, cInterfaceMetric[i].Value, float64(0))
	}
	expectLimiterMetricNames := []string{
		"query_limit",
		"query_max_wait",
		"query_inflight",
		"query_wait_count",
		"query_count",
		"query_wait_fail_count",
	}
	expectLimiterMetricValues := []interface{}{
		config.Conf.Request.Default.QueryLimit,
		int32(config.Conf.Request.Default.QueryMaxWait),
		int32(1),
		int32(0),
		int32(1),
		int32(0),
	}
	limiterTable := metric.Tables[3]
	assert.Equal(t, "adapter_request_limit", limiterTable.Name)
	assert.Equal(t, 1, len(limiterTable.MetricGroups))
	limiterMetricGroup := limiterTable.MetricGroups[0]
	assert.Equal(t, 2, len(limiterMetricGroup.Tags))
	assert.Equal(t, "endpoint", limiterMetricGroup.Tags[0].Name)
	assert.Equal(t, identity, connPoolTable.MetricGroups[0].Tags[0].Value)
	assert.Equal(t, "user", limiterMetricGroup.Tags[1].Name)
	assert.Equal(t, testUserName, limiterMetricGroup.Tags[1].Value)

	assert.Equal(t, identity, limiterMetricGroup.Tags[0].Value)
	limiterMetric := limiterMetricGroup.Metrics
	assert.Equal(t, len(expectLimiterMetricNames), len(limiterMetric))
	for i := 0; i < len(limiterMetric); i++ {
		assert.Equal(t, expectLimiterMetricNames[i], limiterMetric[i].Name)
		assert.Equal(t, expectLimiterMetricValues[i], limiterMetric[i].Value)
	}

	inputJsonTable = metric.Tables[4]
	assert.Equal(t, "adapter_input_json", inputJsonTable.Name)
	assert.Equal(t, 1, len(inputJsonTable.MetricGroups))
	assert.Equal(t, 2, len(inputJsonTable.MetricGroups[0].Tags))
	assert.Equal(t, "url_endpoint", inputJsonTable.MetricGroups[0].Tags[0].Name)
	assert.Equal(t, "test_metrics", inputJsonTable.MetricGroups[0].Tags[0].Value)
	assert.Equal(t, "endpoint", inputJsonTable.MetricGroups[0].Tags[1].Name)
	assert.Equal(t, identity, inputJsonTable.MetricGroups[0].Tags[1].Value)
	assert.Equal(t, 5, len(inputJsonTable.MetricGroups[0].Metrics))
	assert.Equal(t, "total_rows", inputJsonTable.MetricGroups[0].Metrics[0].Name)
	assert.Equal(t, float64(20), inputJsonTable.MetricGroups[0].Metrics[0].Value)
	assert.Equal(t, "success_rows", inputJsonTable.MetricGroups[0].Metrics[1].Name)
	assert.Equal(t, float64(12), inputJsonTable.MetricGroups[0].Metrics[1].Value)
	assert.Equal(t, "fail_rows", inputJsonTable.MetricGroups[0].Metrics[2].Name)
	assert.Equal(t, float64(1), inputJsonTable.MetricGroups[0].Metrics[2].Value)
	assert.Equal(t, "inflight_rows", inputJsonTable.MetricGroups[0].Metrics[3].Name)
	assert.Equal(t, float64(11), inputJsonTable.MetricGroups[0].Metrics[3].Value)
	assert.Equal(t, "affected_rows", inputJsonTable.MetricGroups[0].Metrics[4].Name)
	assert.Equal(t, float64(200), inputJsonTable.MetricGroups[0].Metrics[4].Value)

	assert.Equal(t, float64(0), inputJsonMetrics.TotalRows.Value())
	assert.Equal(t, float64(0), inputJsonMetrics.SuccessRows.Value())
	assert.Equal(t, float64(0), inputJsonMetrics.FailRows.Value())
	assert.Equal(t, float64(0), inputJsonMetrics.AffectedRows.Value())
	assert.Equal(t, float64(11), inputJsonMetrics.InflightRows.Value())

	config.Conf.UploadKeeper.Enable = false
	config.Conf.Request.QueryLimitEnable = false
}

func TestNewInputJsonMetric(t *testing.T) {
	config.Conf.UploadKeeper.Enable = false
	metrics := NewInputJsonMetric("disable_upload_keeper")
	require.Nil(t, metrics)
}
