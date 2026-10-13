/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "version.h"
#include "wrapper.h"
/* uWarn: the entry-point report belongs in the client log, not on the caller's
 * terminal (see taosDriverReportMissing()). */
#include "tlog.h"


/*
 * One fallback per public entry point, carrying that entry point's exact
 * signature so the slot below can name it without a cast.
 *
 * A slot is written by taosDriverInit() only when the lookup succeeded, so a
 * slot still holding its fallback means the driver is not loaded
 * (TSDB_CODE_DLL_NOT_LOAD) or is loaded without that entry point
 * (TSDB_CODE_DLL_FUNC_NOT_LOAD) -- which is what tells wrapperFunc.c it can
 * drop the per-call `if (slot == NULL)` check.
 */
static int tdNotProvided(void) {
  return tsDriver == NULL ? TSDB_CODE_DLL_NOT_LOAD : TSDB_CODE_DLL_FUNC_NOT_LOAD;
}

static setConfRet td_taos_set_config_not_provided(const char *config) {
  terrno = tdNotProvided();
  setConfRet ret = {.retCode = -1};
  return ret;
}

static int td_taos_init_not_provided(void) {
  /* taos_init_wrapper() used to set this unconditionally. */
  terrno = TSDB_CODE_DLL_FUNC_NOT_LOAD;
  return -1;
}

static void td_taos_cleanup_not_provided(void) {
  terrno = tdNotProvided();
}

static int td_taos_options_not_provided(TSDB_OPTION option, const void *arg, ...) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_options_connection_not_provided(TAOS *taos, TSDB_OPTION_CONNECTION option, const void *arg, ...) {
  terrno = tdNotProvided();
  return -1;
}

static void td_taos_set_option_not_provided(OPTIONS *options, const char *key, const char *value) {
  terrno = tdNotProvided();
}

static TAOS * td_taos_connect_not_provided(const char *ip, const char *user, const char *pass, const char *db, uint16_t port) {
  terrno = tdNotProvided();
  return NULL;
}

static TAOS * td_taos_connect_totp_not_provided(const char *ip, const char *user, const char *pass, const char* totp, const char *db, uint16_t port) {
  terrno = tdNotProvided();
  return NULL;
}

static int td_taos_connect_test_not_provided(const char *ip, const char *user, const char *pass, const char* totp, const char *db, uint16_t port) {
  /* Returns the code itself and leaves terrno alone. */
  return tsDriver == NULL ? TSDB_CODE_DLL_NOT_LOAD : TSDB_CODE_DLL_FUNC_NOT_LOAD;
}

static TAOS * td_taos_connect_token_not_provided(const char *ip, const char *token, const char *db, uint16_t port) {
  terrno = tdNotProvided();
  return NULL;
}

static TAOS * td_taos_connect_auth_not_provided(const char *ip, const char *user, const char *auth, const char *db, uint16_t port) {
  terrno = tdNotProvided();
  return NULL;
}

static TAOS * td_taos_connect_with_not_provided(const OPTIONS *options) {
  terrno = tdNotProvided();
  return NULL;
}

static TAOS * td_taos_connect_with_dsn_not_provided(const char *dsn) {
  terrno = tdNotProvided();
  return NULL;
}

static void td_taos_close_not_provided(TAOS *taos) {
  terrno = tdNotProvided();
}

static int td_taos_txn_begin_not_provided(TAOS *taos) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_txn_commit_not_provided(TAOS *taos) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_txn_rollback_not_provided(TAOS *taos) {
  terrno = tdNotProvided();
  return -1;
}

static const char * td_taos_data_type_not_provided(int type) {
  terrno = tdNotProvided();
  return NULL;
}

static TAOS_STMT * td_taos_stmt_init_not_provided(TAOS *taos) {
  terrno = tdNotProvided();
  return NULL;
}

static TAOS_STMT * td_taos_stmt_init_with_reqid_not_provided(TAOS *taos, int64_t reqid) {
  terrno = tdNotProvided();
  return NULL;
}

static TAOS_STMT * td_taos_stmt_init_with_options_not_provided(TAOS *taos, TAOS_STMT_OPTIONS *options) {
  terrno = tdNotProvided();
  return NULL;
}

static int td_taos_stmt_prepare_not_provided(TAOS_STMT *stmt, const char *sql, unsigned long length) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_stmt_set_tbname_tags_not_provided(TAOS_STMT *stmt, const char *name, TAOS_MULTI_BIND *tags) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_stmt_set_tbname_not_provided(TAOS_STMT *stmt, const char *name) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_stmt_set_tags_not_provided(TAOS_STMT *stmt, TAOS_MULTI_BIND *tags) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_stmt_set_sub_tbname_not_provided(TAOS_STMT *stmt, const char *name) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_stmt_get_tag_fields_not_provided(TAOS_STMT *stmt, int *fieldNum, TAOS_FIELD_E **fields) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_stmt_get_col_fields_not_provided(TAOS_STMT *stmt, int *fieldNum, TAOS_FIELD_E **fields) {
  terrno = tdNotProvided();
  return -1;
}

static void td_taos_stmt_reclaim_fields_not_provided(TAOS_STMT *stmt, TAOS_FIELD_E *fields) {
  terrno = tdNotProvided();
}

static int td_taos_stmt_is_insert_not_provided(TAOS_STMT *stmt, int *insert) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_stmt_num_params_not_provided(TAOS_STMT *stmt, int *nums) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_stmt_get_param_not_provided(TAOS_STMT *stmt, int idx, int *type, int *bytes) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_stmt_bind_param_not_provided(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_stmt_bind_param_batch_not_provided(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_stmt_bind_single_param_batch_not_provided(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind, int colIdx) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_stmt_add_batch_not_provided(TAOS_STMT *stmt) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_stmt_execute_not_provided(TAOS_STMT *stmt) {
  terrno = tdNotProvided();
  return -1;
}

static TAOS_RES * td_taos_stmt_use_result_not_provided(TAOS_STMT *stmt) {
  terrno = tdNotProvided();
  return NULL;
}

static int td_taos_stmt_close_not_provided(TAOS_STMT *stmt) {
  terrno = tdNotProvided();
  return -1;
}

static char * td_taos_stmt_errstr_not_provided(TAOS_STMT *stmt) {
  terrno = tdNotProvided();
  return NULL;
}

static int td_taos_stmt_affected_rows_not_provided(TAOS_STMT *stmt) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_stmt_affected_rows_once_not_provided(TAOS_STMT *stmt) {
  terrno = tdNotProvided();
  return -1;
}

static TAOS_STMT2 * td_taos_stmt2_init_not_provided(TAOS *taos, TAOS_STMT2_OPTION *option) {
  terrno = tdNotProvided();
  return NULL;
}

static int td_taos_stmt2_prepare_not_provided(TAOS_STMT2 *stmt, const char *sql, unsigned long length) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_stmt2_bind_param_not_provided(TAOS_STMT2 *stmt, TAOS_STMT2_BINDV *bindv, int32_t col_idx) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_stmt2_bind_param_a_not_provided(TAOS_STMT2 *stmt, TAOS_STMT2_BINDV *bindv, int32_t col_idx, __taos_async_fn_t fp,
                                  void *param) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_stmt2_bind_param_column_not_provided(TAOS_STMT2 *stmt, TAOS_STMT2_COLUMN_BINDV *bindv) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_stmt2_bind_param_column_a_not_provided(TAOS_STMT2 *stmt, TAOS_STMT2_COLUMN_BINDV *bindv, __taos_async_fn_t fp,
                                         void *param) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_stmt2_exec_not_provided(TAOS_STMT2 *stmt, int *affected_rows) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_stmt2_close_not_provided(TAOS_STMT2 *stmt) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_stmt2_is_insert_not_provided(TAOS_STMT2 *stmt, int *insert) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_stmt2_get_fields_not_provided(TAOS_STMT2 *stmt, int *count, TAOS_FIELD_ALL **fields) {
  terrno = tdNotProvided();
  return -1;
}

static void td_taos_stmt2_free_fields_not_provided(TAOS_STMT2 *stmt, TAOS_FIELD_ALL *fields) {
  terrno = tdNotProvided();
}

static TAOS_RES * td_taos_stmt2_result_not_provided(TAOS_STMT2 *stmt) {
  terrno = tdNotProvided();
  return NULL;
}

static char * td_taos_stmt2_error_not_provided(TAOS_STMT2 *stmt) {
  terrno = tdNotProvided();
  return NULL;
}

static TAOS_RES * td_taos_query_not_provided(TAOS *taos, const char *sql) {
  terrno = tdNotProvided();
  return NULL;
}

static TAOS_RES * td_taos_query_with_reqid_not_provided(TAOS *taos, const char *sql, int64_t reqId) {
  terrno = tdNotProvided();
  return NULL;
}

static TAOS_ROW td_taos_fetch_row_not_provided(TAOS_RES *res) {
  terrno = tdNotProvided();
  return NULL;
}

static int td_taos_result_precision_not_provided(TAOS_RES *res) {
  terrno = tdNotProvided();
  return -1;
}

static void td_taos_free_result_not_provided(TAOS_RES *res) {
  terrno = tdNotProvided();
}

static void td_taos_kill_query_not_provided(TAOS *taos) {
  terrno = tdNotProvided();
}

static int td_taos_field_count_not_provided(TAOS_RES *res) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_num_fields_not_provided(TAOS_RES *res) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_affected_rows_not_provided(TAOS_RES *res) {
  terrno = tdNotProvided();
  return -1;
}

static int64_t td_taos_affected_rows64_not_provided(TAOS_RES *res) {
  terrno = tdNotProvided();
  return -1;
}

static TAOS_FIELD * td_taos_fetch_fields_not_provided(TAOS_RES *res) {
  terrno = tdNotProvided();
  return NULL;
}

static TAOS_FIELD_E * td_taos_fetch_fields_e_not_provided(TAOS_RES *res) {
  terrno = tdNotProvided();
  return NULL;
}

static int td_taos_select_db_not_provided(TAOS *taos, const char *db) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_print_row_not_provided(char *str, TAOS_ROW row, TAOS_FIELD *fields, int num_fields) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_print_row_with_size_not_provided(char *str, uint32_t size, TAOS_ROW row, TAOS_FIELD *fields, int num_fields) {
  terrno = tdNotProvided();
  return -1;
}

static void td_taos_stop_query_not_provided(TAOS_RES *res) {
  terrno = tdNotProvided();
}

static bool td_taos_is_null_not_provided(TAOS_RES *res, int32_t row, int32_t col) {
  terrno = tdNotProvided();
  return false;
}

static int td_taos_is_null_by_column_not_provided(TAOS_RES *res, int columnIndex, bool result[], int *rows) {
  terrno = tdNotProvided();
  return -1;
}

static bool td_taos_is_update_query_not_provided(TAOS_RES *res) {
  terrno = tdNotProvided();
  return false;
}

static int td_taos_fetch_block_not_provided(TAOS_RES *res, TAOS_ROW *rows) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_fetch_block_s_not_provided(TAOS_RES *res, int *numOfRows, TAOS_ROW *rows) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_fetch_raw_block_not_provided(TAOS_RES *res, int *numOfRows, void **pData) {
  terrno = tdNotProvided();
  return -1;
}

static int * td_taos_get_column_data_offset_not_provided(TAOS_RES *res, int columnIndex) {
  terrno = tdNotProvided();
  return NULL;
}

static int td_taos_validate_sql_not_provided(TAOS *taos, const char *sql) {
  terrno = tdNotProvided();
  return -1;
}

static void td_taos_reset_current_db_not_provided(TAOS *taos) {
  terrno = tdNotProvided();
}

static int * td_taos_fetch_lengths_not_provided(TAOS_RES *res) {
  terrno = tdNotProvided();
  return NULL;
}

static TAOS_ROW * td_taos_result_block_not_provided(TAOS_RES *res) {
  terrno = tdNotProvided();
  return NULL;
}

static const char * td_taos_get_server_info_not_provided(TAOS *taos) {
  terrno = tdNotProvided();
  return NULL;
}

static const char *td_taos_get_client_info_not_provided() {
  return td_version;
}

static int td_taos_get_current_db_not_provided(TAOS *taos, char *database, int len, int *required) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_get_connection_info_not_provided(TAOS *taos, TSDB_CONNECTION_INFO info, char *buffer, int *len) {
  terrno = tdNotProvided();
  return -1;
}

static void *td_taos_get_result_tz_not_provided(TAOS_RES *res) {
  return NULL;
}

static const char *td_taos_errstr_not_provided(TAOS_RES *res) {
  return tstrerror(terrno);
}

static int td_taos_errno_not_provided(TAOS_RES *res) {
  /* Reports the last error instead of a fixed code. */
  return terrno;
}

static void td_taos_query_a_not_provided(TAOS *taos, const char *sql, __taos_async_fn_t fp, void *param) {
  terrno = tdNotProvided();
}

static void td_taos_query_a_with_reqid_not_provided(TAOS *taos, const char *sql, __taos_async_fn_t fp, void *param,
                                   int64_t reqid) {
  terrno = tdNotProvided();
}

static void td_taos_fetch_rows_a_not_provided(TAOS_RES *res, __taos_async_fn_t fp, void *param) {
  terrno = tdNotProvided();
}

static void td_taos_fetch_raw_block_a_not_provided(TAOS_RES *res, __taos_async_fn_t fp, void *param) {
  terrno = tdNotProvided();
}

static const void * td_taos_get_raw_block_not_provided(TAOS_RES *res) {
  terrno = tdNotProvided();
  return NULL;
}

static int td_taos_get_db_route_info_not_provided(TAOS *taos, const char *db, TAOS_DB_ROUTE_INFO *dbInfo) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_get_table_vgId_not_provided(TAOS *taos, const char *db, const char *table, int *vgId) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_get_tables_vgId_not_provided(TAOS *taos, const char *db, const char *table[], int tableNum, int *vgId) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_load_table_info_not_provided(TAOS *taos, const char *tableNameList) {
  terrno = tdNotProvided();
  return -1;
}

static void td_taos_set_hb_quit_not_provided(int8_t quitByKill) {
  terrno = tdNotProvided();
}

static int td_taos_set_notify_cb_not_provided(TAOS *taos, __taos_notify_fn_t fp, void *param, int type) {
  terrno = tdNotProvided();
  return -1;
}

static void td_taos_fetch_whitelist_a_not_provided(TAOS *taos, __taos_async_whitelist_fn_t fp, void *param) {
  terrno = tdNotProvided();
}

static void td_taos_fetch_whitelist_dual_stack_a_not_provided(TAOS *taos, __taos_async_whitelist_dual_stack_fn_t fp, void *param) {
  terrno = tdNotProvided();
}

static void td_taos_fetch_ip_whitelist_a_not_provided(TAOS *taos, __taos_async_ip_whitelist_fn_t fp, void *param) {
  terrno = tdNotProvided();
}

static void td_taos_fetch_datetime_whitelist_a_not_provided(TAOS *taos, __taos_async_datetime_whitelist_fn_t fp, void *param) {
  terrno = tdNotProvided();
}

static int td_taos_set_conn_mode_not_provided(TAOS *taos, int mode, int value) {
  terrno = tdNotProvided();
  return -1;
}

static TAOS_RES * td_taos_schemaless_insert_not_provided(TAOS *taos, char *lines[], int numLines, int protocol, int precision) {
  terrno = tdNotProvided();
  return NULL;
}

static TAOS_RES * td_taos_schemaless_insert_with_reqid_not_provided(TAOS *taos, char *lines[], int numLines, int protocol, int precision,
                                                  int64_t reqid) {
  terrno = tdNotProvided();
  return NULL;
}

static TAOS_RES * td_taos_schemaless_insert_raw_not_provided(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol,
                                           int precision) {
  terrno = tdNotProvided();
  return NULL;
}

static TAOS_RES * td_taos_schemaless_insert_raw_with_reqid_not_provided(TAOS *taos, char *lines, int len, int32_t *totalRows,
                                                      int protocol, int precision, int64_t reqid) {
  terrno = tdNotProvided();
  return NULL;
}

static TAOS_RES * td_taos_schemaless_insert_ttl_not_provided(TAOS *taos, char *lines[], int numLines, int protocol, int precision,
                                           int32_t ttl) {
  terrno = tdNotProvided();
  return NULL;
}

static TAOS_RES * td_taos_schemaless_insert_ttl_with_reqid_not_provided(TAOS *taos, char *lines[], int numLines, int protocol,
                                                      int precision, int32_t ttl, int64_t reqid) {
  terrno = tdNotProvided();
  return NULL;
}

static TAOS_RES * td_taos_schemaless_insert_raw_ttl_not_provided(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol,
                                               int precision, int32_t ttl) {
  terrno = tdNotProvided();
  return NULL;
}

static TAOS_RES * td_taos_schemaless_insert_raw_ttl_with_reqid_not_provided(TAOS *taos, char *lines, int len, int32_t *totalRows,
                                                          int protocol, int precision, int32_t ttl,
                                                          int64_t reqid) {
  terrno = tdNotProvided();
  return NULL;
}

static TAOS_RES * td_taos_schemaless_insert_raw_ttl_with_reqid_tbname_key_not_provided(TAOS *taos, char *lines, int len,
                                                                     int32_t *totalRows, int protocol, int precision,
                                                                     int32_t ttl, int64_t reqid,
                                                                     char *tbnameKey) {
  terrno = tdNotProvided();
  return NULL;
}

static TAOS_RES * td_taos_schemaless_insert_ttl_with_reqid_tbname_key_not_provided(TAOS *taos, char *lines[], int numLines, int protocol,
                                                                 int precision, int32_t ttl, int64_t reqid,
                                                                 char *tbnameKey) {
  terrno = tdNotProvided();
  return NULL;
}

static tmq_conf_t * td_tmq_conf_new_not_provided() {
  terrno = tdNotProvided();
  return NULL;
}

static tmq_conf_res_t td_tmq_conf_set_not_provided(tmq_conf_t *conf, const char *key, const char *value) {
  terrno = tdNotProvided();
  return -1;
}

static void td_tmq_conf_destroy_not_provided(tmq_conf_t *conf) {
  terrno = tdNotProvided();
}

static void td_tmq_conf_set_auto_commit_cb_not_provided(tmq_conf_t *conf, tmq_commit_cb *cb, void *param) {
  terrno = tdNotProvided();
}

static tmq_list_t * td_tmq_list_new_not_provided() {
  terrno = tdNotProvided();
  return NULL;
}

static int32_t td_tmq_list_append_not_provided(tmq_list_t *list, const char *val) {
  terrno = tdNotProvided();
  return -1;
}

static void td_tmq_list_destroy_not_provided(tmq_list_t *list) {
  terrno = tdNotProvided();
}

static int32_t td_tmq_list_get_size_not_provided(const tmq_list_t *list) {
  terrno = tdNotProvided();
  return -1;
}

static char ** td_tmq_list_to_c_array_not_provided(const tmq_list_t *list) {
  terrno = tdNotProvided();
  return NULL;
}

static tmq_t * td_tmq_consumer_new_not_provided(tmq_conf_t *conf, char *errstr, int32_t errstrLen) {
  terrno = tdNotProvided();
  return NULL;
}

static int32_t td_tmq_subscribe_not_provided(tmq_t *tmq, const tmq_list_t *topic_list) {
  terrno = tdNotProvided();
  return -1;
}

static int32_t td_tmq_unsubscribe_not_provided(tmq_t *tmq) {
  terrno = tdNotProvided();
  return -1;
}

static int32_t td_tmq_subscription_not_provided(tmq_t *tmq, tmq_list_t **topics) {
  terrno = tdNotProvided();
  return -1;
}

static TAOS_RES * td_tmq_consumer_poll_not_provided(tmq_t *tmq, int64_t timeout) {
  terrno = tdNotProvided();
  return NULL;
}

static int32_t td_tmq_consumer_close_not_provided(tmq_t *tmq) {
  terrno = tdNotProvided();
  return -1;
}

static int32_t td_tmq_commit_sync_not_provided(tmq_t *tmq, const TAOS_RES *msg) {
  terrno = tdNotProvided();
  return -1;
}

static void td_tmq_commit_async_not_provided(tmq_t *tmq, const TAOS_RES *msg, tmq_commit_cb *cb, void *param) {
  terrno = tdNotProvided();
}

static int32_t td_tmq_commit_offset_sync_not_provided(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset) {
  terrno = tdNotProvided();
  return -1;
}

static void td_tmq_commit_offset_async_not_provided(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset, tmq_commit_cb *cb,
                                   void *param) {
  terrno = tdNotProvided();
}

static int32_t td_tmq_get_topic_assignment_not_provided(tmq_t *tmq, const char *pTopicName, tmq_topic_assignment **assignment,
                                       int32_t *numOfAssignment) {
  terrno = tdNotProvided();
  return -1;
}

static void td_tmq_free_assignment_not_provided(tmq_topic_assignment *pAssignment) {
  terrno = tdNotProvided();
}

static int32_t td_tmq_offset_seek_not_provided(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset) {
  terrno = tdNotProvided();
  return -1;
}

static int64_t td_tmq_position_not_provided(tmq_t *tmq, const char *pTopicName, int32_t vgId) {
  terrno = tdNotProvided();
  return -1;
}

static int64_t td_tmq_committed_not_provided(tmq_t *tmq, const char *pTopicName, int32_t vgId) {
  terrno = tdNotProvided();
  return -1;
}

static TAOS * td_tmq_get_connect_not_provided(tmq_t *tmq) {
  terrno = tdNotProvided();
  return NULL;
}

static const char * td_tmq_get_table_name_not_provided(TAOS_RES *res) {
  terrno = tdNotProvided();
  return NULL;
}

static tmq_res_t td_tmq_get_res_type_not_provided(TAOS_RES *res) {
  terrno = tdNotProvided();
  return -1;
}

static const char * td_tmq_get_topic_name_not_provided(TAOS_RES *res) {
  terrno = tdNotProvided();
  return NULL;
}

static const char * td_tmq_get_db_name_not_provided(TAOS_RES *res) {
  terrno = tdNotProvided();
  return NULL;
}

static int32_t td_tmq_get_vgroup_id_not_provided(TAOS_RES *res) {
  terrno = tdNotProvided();
  return -1;
}

static int64_t td_tmq_get_vgroup_offset_not_provided(TAOS_RES *res) {
  terrno = tdNotProvided();
  return -1;
}

static const char * td_tmq_err2str_not_provided(int32_t code) {
  terrno = tdNotProvided();
  return NULL;
}

static int32_t td_tmq_get_raw_not_provided(TAOS_RES *res, tmq_raw_data *raw) {
  terrno = tdNotProvided();
  return -1;
}

static int32_t td_tmq_write_raw_not_provided(TAOS *taos, tmq_raw_data raw) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_write_raw_block_not_provided(TAOS *taos, int numOfRows, char *pData, const char *tbname) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_write_raw_block_with_reqid_not_provided(TAOS *taos, int numOfRows, char *pData, const char *tbname,
                                          int64_t reqid) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_write_raw_block_with_fields_not_provided(TAOS *taos, int rows, char *pData, const char *tbname, TAOS_FIELD *fields,
                                           int numFields) {
  terrno = tdNotProvided();
  return -1;
}

static int td_taos_write_raw_block_with_fields_with_reqid_not_provided(TAOS *taos, int rows, char *pData, const char *tbname,
                                                      TAOS_FIELD *fields, int numFields, int64_t reqid) {
  terrno = tdNotProvided();
  return -1;
}

static void td_tmq_free_raw_not_provided(tmq_raw_data raw) {
  terrno = tdNotProvided();
}

static char * td_tmq_get_json_meta_not_provided(TAOS_RES *res) {
  terrno = tdNotProvided();
  return NULL;
}

static void td_tmq_free_json_meta_not_provided(char *jsonMeta) {
  terrno = tdNotProvided();
}

static TSDB_SERVER_STATUS td_taos_check_server_status_not_provided(const char *fqdn, int port, char *details, int maxlen) {
  terrno = tdNotProvided();
  return -1;
}

static void td_taos_write_crashinfo_not_provided(int signum, void *sigInfo, void *context) {
  terrno = tdNotProvided();
}

static char * td_getBuildInfo_not_provided() {
  terrno = tdNotProvided();
  return NULL;
}

static int32_t td_taos_connect_is_alive_not_provided(TAOS *taos) {
  terrno = tdNotProvided();
  return -1;
}

static int32_t td_taos_register_instance_not_provided(const char *id, const char *type, const char *desc, int32_t expire) {
  terrno = tdNotProvided();
  return -1;
}

static int32_t td_taos_list_instances_not_provided(const char *filter_type, char ***pList, int32_t *pCount) {
  terrno = tdNotProvided();
  return -1;
}

static void td_taos_free_instances_not_provided(char ***pList, int32_t pCount) {
  terrno = tdNotProvided();
}

setConfRet (*fp_taos_set_config)(const char *config) = td_taos_set_config_not_provided;

int (*fp_taos_init)(void) = td_taos_init_not_provided;
void (*fp_taos_cleanup)(void) = td_taos_cleanup_not_provided;
int (*fp_taos_options)(TSDB_OPTION option, const void *arg, ...) = td_taos_options_not_provided;
int (*fp_taos_options_connection)(TAOS *taos, TSDB_OPTION_CONNECTION option, const void *arg, ...) = td_taos_options_connection_not_provided;
void (*fp_taos_set_option)(OPTIONS *options, const char *key, const char *value) = td_taos_set_option_not_provided;
TAOS *(*fp_taos_connect)(const char *ip, const char *user, const char *pass, const char *db, uint16_t port) = td_taos_connect_not_provided;
TAOS *(*fp_taos_connect_totp)(const char *ip, const char *user, const char *pass, const char* totp, const char *db, uint16_t port) = td_taos_connect_totp_not_provided;
int (*fp_taos_connect_test)(const char *ip, const char *user, const char *pass, const char* totp, const char *db, uint16_t port) = td_taos_connect_test_not_provided;
TAOS *(*fp_taos_connect_token)(const char *ip, const char *token, const char *db, uint16_t port) = td_taos_connect_token_not_provided;
TAOS *(*fp_taos_connect_auth)(const char *ip, const char *user, const char *auth, const char *db, uint16_t port) = td_taos_connect_auth_not_provided;
TAOS *(*fp_taos_connect_with)(const OPTIONS *options) = td_taos_connect_with_not_provided;
TAOS *(*fp_taos_connect_with_dsn)(const char *dsn) = td_taos_connect_with_dsn_not_provided;
void (*fp_taos_close)(TAOS *taos) = td_taos_close_not_provided;
int (*fp_taos_txn_begin)(TAOS *taos) = td_taos_txn_begin_not_provided;
int (*fp_taos_txn_commit)(TAOS *taos) = td_taos_txn_commit_not_provided;
int (*fp_taos_txn_rollback)(TAOS *taos) = td_taos_txn_rollback_not_provided;

const char *(*fp_taos_data_type)(int type) = td_taos_data_type_not_provided;

TAOS_STMT *(*fp_taos_stmt_init)(TAOS *taos) = td_taos_stmt_init_not_provided;
TAOS_STMT *(*fp_taos_stmt_init_with_reqid)(TAOS *taos, int64_t reqid) = td_taos_stmt_init_with_reqid_not_provided;
TAOS_STMT *(*fp_taos_stmt_init_with_options)(TAOS *taos, TAOS_STMT_OPTIONS *options) = td_taos_stmt_init_with_options_not_provided;
int (*fp_taos_stmt_prepare)(TAOS_STMT *stmt, const char *sql, unsigned long length) = td_taos_stmt_prepare_not_provided;
int (*fp_taos_stmt_set_tbname_tags)(TAOS_STMT *stmt, const char *name, TAOS_MULTI_BIND *tags) = td_taos_stmt_set_tbname_tags_not_provided;
int (*fp_taos_stmt_set_tbname)(TAOS_STMT *stmt, const char *name) = td_taos_stmt_set_tbname_not_provided;
int (*fp_taos_stmt_set_tags)(TAOS_STMT *stmt, TAOS_MULTI_BIND *tags) = td_taos_stmt_set_tags_not_provided;
int (*fp_taos_stmt_set_sub_tbname)(TAOS_STMT *stmt, const char *name) = td_taos_stmt_set_sub_tbname_not_provided;
int (*fp_taos_stmt_get_tag_fields)(TAOS_STMT *stmt, int *fieldNum, TAOS_FIELD_E **fields) = td_taos_stmt_get_tag_fields_not_provided;
int (*fp_taos_stmt_get_col_fields)(TAOS_STMT *stmt, int *fieldNum, TAOS_FIELD_E **fields) = td_taos_stmt_get_col_fields_not_provided;
void (*fp_taos_stmt_reclaim_fields)(TAOS_STMT *stmt, TAOS_FIELD_E *fields) = td_taos_stmt_reclaim_fields_not_provided;

int (*fp_taos_stmt_is_insert)(TAOS_STMT *stmt, int *insert) = td_taos_stmt_is_insert_not_provided;
int (*fp_taos_stmt_num_params)(TAOS_STMT *stmt, int *nums) = td_taos_stmt_num_params_not_provided;
int (*fp_taos_stmt_get_param)(TAOS_STMT *stmt, int idx, int *type, int *bytes) = td_taos_stmt_get_param_not_provided;
int (*fp_taos_stmt_bind_param)(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind) = td_taos_stmt_bind_param_not_provided;
int (*fp_taos_stmt_bind_param_batch)(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind) = td_taos_stmt_bind_param_batch_not_provided;
int (*fp_taos_stmt_bind_single_param_batch)(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind, int colIdx) = td_taos_stmt_bind_single_param_batch_not_provided;
int (*fp_taos_stmt_add_batch)(TAOS_STMT *stmt) = td_taos_stmt_add_batch_not_provided;
int (*fp_taos_stmt_execute)(TAOS_STMT *stmt) = td_taos_stmt_execute_not_provided;
TAOS_RES *(*fp_taos_stmt_use_result)(TAOS_STMT *stmt) = td_taos_stmt_use_result_not_provided;
int (*fp_taos_stmt_close)(TAOS_STMT *stmt) = td_taos_stmt_close_not_provided;
char *(*fp_taos_stmt_errstr)(TAOS_STMT *stmt) = td_taos_stmt_errstr_not_provided;
int (*fp_taos_stmt_affected_rows)(TAOS_STMT *stmt) = td_taos_stmt_affected_rows_not_provided;
int (*fp_taos_stmt_affected_rows_once)(TAOS_STMT *stmt) = td_taos_stmt_affected_rows_once_not_provided;

TAOS_STMT2 *(*fp_taos_stmt2_init)(TAOS *taos, TAOS_STMT2_OPTION *option) = td_taos_stmt2_init_not_provided;
int (*fp_taos_stmt2_prepare)(TAOS_STMT2 *stmt, const char *sql, unsigned long length) = td_taos_stmt2_prepare_not_provided;
int (*fp_taos_stmt2_bind_param)(TAOS_STMT2 *stmt, TAOS_STMT2_BINDV *bindv, int32_t col_idx) = td_taos_stmt2_bind_param_not_provided;
int (*fp_taos_stmt2_bind_param_a)(TAOS_STMT2 *stmt, TAOS_STMT2_BINDV *bindv, int32_t col_idx, __taos_async_fn_t fp,
                                  void *param) = td_taos_stmt2_bind_param_a_not_provided;
int (*fp_taos_stmt2_bind_param_column)(TAOS_STMT2 *stmt, TAOS_STMT2_COLUMN_BINDV *bindv) = td_taos_stmt2_bind_param_column_not_provided;
int (*fp_taos_stmt2_bind_param_column_a)(TAOS_STMT2 *stmt, TAOS_STMT2_COLUMN_BINDV *bindv, __taos_async_fn_t fp,
                                         void *param) = td_taos_stmt2_bind_param_column_a_not_provided;
int (*fp_taos_stmt2_exec)(TAOS_STMT2 *stmt, int *affected_rows) = td_taos_stmt2_exec_not_provided;
int (*fp_taos_stmt2_close)(TAOS_STMT2 *stmt) = td_taos_stmt2_close_not_provided;
int (*fp_taos_stmt2_is_insert)(TAOS_STMT2 *stmt, int *insert) = td_taos_stmt2_is_insert_not_provided;
int (*fp_taos_stmt2_get_fields)(TAOS_STMT2 *stmt, int *count, TAOS_FIELD_ALL **fields) = td_taos_stmt2_get_fields_not_provided;
void (*fp_taos_stmt2_free_fields)(TAOS_STMT2 *stmt, TAOS_FIELD_ALL *fields) = td_taos_stmt2_free_fields_not_provided;
TAOS_RES *(*fp_taos_stmt2_result)(TAOS_STMT2 *stmt) = td_taos_stmt2_result_not_provided;
char *(*fp_taos_stmt2_error)(TAOS_STMT2 *stmt) = td_taos_stmt2_error_not_provided;

TAOS_RES *(*fp_taos_query)(TAOS *taos, const char *sql) = td_taos_query_not_provided;
TAOS_RES *(*fp_taos_query_with_reqid)(TAOS *taos, const char *sql, int64_t reqId) = td_taos_query_with_reqid_not_provided;

TAOS_ROW (*fp_taos_fetch_row)(TAOS_RES *res) = td_taos_fetch_row_not_provided;
int (*fp_taos_result_precision)(TAOS_RES *res) = td_taos_result_precision_not_provided;  // get the time precision of result
void (*fp_taos_free_result)(TAOS_RES *res) = td_taos_free_result_not_provided;
void (*fp_taos_kill_query)(TAOS *taos) = td_taos_kill_query_not_provided;
int (*fp_taos_field_count)(TAOS_RES *res) = td_taos_field_count_not_provided;
int (*fp_taos_num_fields)(TAOS_RES *res) = td_taos_num_fields_not_provided;
int (*fp_taos_affected_rows)(TAOS_RES *res) = td_taos_affected_rows_not_provided;
int64_t (*fp_taos_affected_rows64)(TAOS_RES *res) = td_taos_affected_rows64_not_provided;

TAOS_FIELD *(*fp_taos_fetch_fields)(TAOS_RES *res) = td_taos_fetch_fields_not_provided;
TAOS_FIELD_E *(*fp_taos_fetch_fields_e)(TAOS_RES *res) = td_taos_fetch_fields_e_not_provided;
int (*fp_taos_select_db)(TAOS *taos, const char *db) = td_taos_select_db_not_provided;
int (*fp_taos_print_row)(char *str, TAOS_ROW row, TAOS_FIELD *fields, int num_fields) = td_taos_print_row_not_provided;
int (*fp_taos_print_row_with_size)(char *str, uint32_t size, TAOS_ROW row, TAOS_FIELD *fields, int num_fields) = td_taos_print_row_with_size_not_provided;
void (*fp_taos_stop_query)(TAOS_RES *res) = td_taos_stop_query_not_provided;
bool (*fp_taos_is_null)(TAOS_RES *res, int32_t row, int32_t col) = td_taos_is_null_not_provided;
int (*fp_taos_is_null_by_column)(TAOS_RES *res, int columnIndex, bool result[], int *rows) = td_taos_is_null_by_column_not_provided;
bool (*fp_taos_is_update_query)(TAOS_RES *res) = td_taos_is_update_query_not_provided;
int (*fp_taos_fetch_block)(TAOS_RES *res, TAOS_ROW *rows) = td_taos_fetch_block_not_provided;
int (*fp_taos_fetch_block_s)(TAOS_RES *res, int *numOfRows, TAOS_ROW *rows) = td_taos_fetch_block_s_not_provided;
int (*fp_taos_fetch_raw_block)(TAOS_RES *res, int *numOfRows, void **pData) = td_taos_fetch_raw_block_not_provided;
int *(*fp_taos_get_column_data_offset)(TAOS_RES *res, int columnIndex) = td_taos_get_column_data_offset_not_provided;
int (*fp_taos_validate_sql)(TAOS *taos, const char *sql) = td_taos_validate_sql_not_provided;
void (*fp_taos_reset_current_db)(TAOS *taos) = td_taos_reset_current_db_not_provided;

int *(*fp_taos_fetch_lengths)(TAOS_RES *res) = td_taos_fetch_lengths_not_provided;
TAOS_ROW *(*fp_taos_result_block)(TAOS_RES *res) = td_taos_result_block_not_provided;

const char *(*fp_taos_get_server_info)(TAOS *taos) = td_taos_get_server_info_not_provided;
const char *(*fp_taos_get_client_info)() = td_taos_get_client_info_not_provided;
int (*fp_taos_get_current_db)(TAOS *taos, char *database, int len, int *required) = td_taos_get_current_db_not_provided;
int (*fp_taos_get_connection_info)(TAOS *taos, TSDB_CONNECTION_INFO info, char *buffer, int *len) = td_taos_get_connection_info_not_provided;
void *(*fp_taos_get_result_tz)(TAOS_RES *res) = td_taos_get_result_tz_not_provided;

const char *(*fp_taos_errstr)(TAOS_RES *res) = td_taos_errstr_not_provided;
int (*fp_taos_errno)(TAOS_RES *res) = td_taos_errno_not_provided;

void (*fp_taos_query_a)(TAOS *taos, const char *sql, __taos_async_fn_t fp, void *param) = td_taos_query_a_not_provided;
void (*fp_taos_query_a_with_reqid)(TAOS *taos, const char *sql, __taos_async_fn_t fp, void *param,
                                   int64_t reqid) = td_taos_query_a_with_reqid_not_provided;
void (*fp_taos_fetch_rows_a)(TAOS_RES *res, __taos_async_fn_t fp, void *param) = td_taos_fetch_rows_a_not_provided;
void (*fp_taos_fetch_raw_block_a)(TAOS_RES *res, __taos_async_fn_t fp, void *param) = td_taos_fetch_raw_block_a_not_provided;
const void *(*fp_taos_get_raw_block)(TAOS_RES *res) = td_taos_get_raw_block_not_provided;

int (*fp_taos_get_db_route_info)(TAOS *taos, const char *db, TAOS_DB_ROUTE_INFO *dbInfo) = td_taos_get_db_route_info_not_provided;
int (*fp_taos_get_table_vgId)(TAOS *taos, const char *db, const char *table, int *vgId) = td_taos_get_table_vgId_not_provided;
int (*fp_taos_get_tables_vgId)(TAOS *taos, const char *db, const char *table[], int tableNum, int *vgId) = td_taos_get_tables_vgId_not_provided;

int (*fp_taos_load_table_info)(TAOS *taos, const char *tableNameList) = td_taos_load_table_info_not_provided;

void (*fp_taos_set_hb_quit)(int8_t quitByKill) = td_taos_set_hb_quit_not_provided;

int (*fp_taos_set_notify_cb)(TAOS *taos, __taos_notify_fn_t fp, void *param, int type) = td_taos_set_notify_cb_not_provided;

void (*fp_taos_fetch_whitelist_a)(TAOS *taos, __taos_async_whitelist_fn_t fp, void *param) = td_taos_fetch_whitelist_a_not_provided;

void (*fp_taos_fetch_whitelist_dual_stack_a)(TAOS *taos, __taos_async_whitelist_dual_stack_fn_t fp, void *param) = td_taos_fetch_whitelist_dual_stack_a_not_provided;

void (*fp_taos_fetch_ip_whitelist_a)(TAOS *taos, __taos_async_ip_whitelist_fn_t fp, void *param) = td_taos_fetch_ip_whitelist_a_not_provided;

void (*fp_taos_fetch_datetime_whitelist_a)(TAOS *taos, __taos_async_datetime_whitelist_fn_t fp, void *param) = td_taos_fetch_datetime_whitelist_a_not_provided;

int (*fp_taos_set_conn_mode)(TAOS *taos, int mode, int value) = td_taos_set_conn_mode_not_provided;

TAOS_RES *(*fp_taos_schemaless_insert)(TAOS *taos, char *lines[], int numLines, int protocol, int precision) = td_taos_schemaless_insert_not_provided;
TAOS_RES *(*fp_taos_schemaless_insert_with_reqid)(TAOS *taos, char *lines[], int numLines, int protocol, int precision,
                                                  int64_t reqid) = td_taos_schemaless_insert_with_reqid_not_provided;
TAOS_RES *(*fp_taos_schemaless_insert_raw)(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol,
                                           int precision) = td_taos_schemaless_insert_raw_not_provided;
TAOS_RES *(*fp_taos_schemaless_insert_raw_with_reqid)(TAOS *taos, char *lines, int len, int32_t *totalRows,
                                                      int protocol, int precision, int64_t reqid) = td_taos_schemaless_insert_raw_with_reqid_not_provided;
TAOS_RES *(*fp_taos_schemaless_insert_ttl)(TAOS *taos, char *lines[], int numLines, int protocol, int precision,
                                           int32_t ttl) = td_taos_schemaless_insert_ttl_not_provided;
TAOS_RES *(*fp_taos_schemaless_insert_ttl_with_reqid)(TAOS *taos, char *lines[], int numLines, int protocol,
                                                      int precision, int32_t ttl, int64_t reqid) = td_taos_schemaless_insert_ttl_with_reqid_not_provided;
TAOS_RES *(*fp_taos_schemaless_insert_raw_ttl)(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol,
                                               int precision, int32_t ttl) = td_taos_schemaless_insert_raw_ttl_not_provided;
TAOS_RES *(*fp_taos_schemaless_insert_raw_ttl_with_reqid)(TAOS *taos, char *lines, int len, int32_t *totalRows,
                                                          int protocol, int precision, int32_t ttl,
                                                          int64_t reqid) = td_taos_schemaless_insert_raw_ttl_with_reqid_not_provided;
TAOS_RES *(*fp_taos_schemaless_insert_raw_ttl_with_reqid_tbname_key)(TAOS *taos, char *lines, int len,
                                                                     int32_t *totalRows, int protocol, int precision,
                                                                     int32_t ttl, int64_t reqid,
                                                                     char *tbnameKey) = td_taos_schemaless_insert_raw_ttl_with_reqid_tbname_key_not_provided;
TAOS_RES *(*fp_taos_schemaless_insert_ttl_with_reqid_tbname_key)(TAOS *taos, char *lines[], int numLines, int protocol,
                                                                 int precision, int32_t ttl, int64_t reqid,
                                                                 char *tbnameKey) = td_taos_schemaless_insert_ttl_with_reqid_tbname_key_not_provided;

tmq_conf_t *(*fp_tmq_conf_new)() = td_tmq_conf_new_not_provided;
tmq_conf_res_t (*fp_tmq_conf_set)(tmq_conf_t *conf, const char *key, const char *value) = td_tmq_conf_set_not_provided;
void (*fp_tmq_conf_destroy)(tmq_conf_t *conf) = td_tmq_conf_destroy_not_provided;
void (*fp_tmq_conf_set_auto_commit_cb)(tmq_conf_t *conf, tmq_commit_cb *cb, void *param) = td_tmq_conf_set_auto_commit_cb_not_provided;

tmq_list_t *(*fp_tmq_list_new)() = td_tmq_list_new_not_provided;
int32_t (*fp_tmq_list_append)(tmq_list_t *, const char *) = td_tmq_list_append_not_provided;
void (*fp_tmq_list_destroy)(tmq_list_t *) = td_tmq_list_destroy_not_provided;
int32_t (*fp_tmq_list_get_size)(const tmq_list_t *) = td_tmq_list_get_size_not_provided;
char **(*fp_tmq_list_to_c_array)(const tmq_list_t *) = td_tmq_list_to_c_array_not_provided;

tmq_t *(*fp_tmq_consumer_new)(tmq_conf_t *conf, char *errstr, int32_t errstrLen) = td_tmq_consumer_new_not_provided;
int32_t (*fp_tmq_subscribe)(tmq_t *tmq, const tmq_list_t *topic_list) = td_tmq_subscribe_not_provided;
int32_t (*fp_tmq_unsubscribe)(tmq_t *tmq) = td_tmq_unsubscribe_not_provided;
int32_t (*fp_tmq_subscription)(tmq_t *tmq, tmq_list_t **topics) = td_tmq_subscription_not_provided;
TAOS_RES *(*fp_tmq_consumer_poll)(tmq_t *tmq, int64_t timeout) = td_tmq_consumer_poll_not_provided;
int32_t (*fp_tmq_consumer_close)(tmq_t *tmq) = td_tmq_consumer_close_not_provided;
int32_t (*fp_tmq_commit_sync)(tmq_t *tmq, const TAOS_RES *msg) = td_tmq_commit_sync_not_provided;
void (*fp_tmq_commit_async)(tmq_t *tmq, const TAOS_RES *msg, tmq_commit_cb *cb, void *param) = td_tmq_commit_async_not_provided;
int32_t (*fp_tmq_commit_offset_sync)(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset) = td_tmq_commit_offset_sync_not_provided;
void (*fp_tmq_commit_offset_async)(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset, tmq_commit_cb *cb,
                                   void *param) = td_tmq_commit_offset_async_not_provided;
int32_t (*fp_tmq_get_topic_assignment)(tmq_t *tmq, const char *pTopicName, tmq_topic_assignment **assignment,
                                       int32_t *numOfAssignment) = td_tmq_get_topic_assignment_not_provided;
void (*fp_tmq_free_assignment)(tmq_topic_assignment *pAssignment) = td_tmq_free_assignment_not_provided;
int32_t (*fp_tmq_offset_seek)(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset) = td_tmq_offset_seek_not_provided;
int64_t (*fp_tmq_position)(tmq_t *tmq, const char *pTopicName, int32_t vgId) = td_tmq_position_not_provided;
int64_t (*fp_tmq_committed)(tmq_t *tmq, const char *pTopicName, int32_t vgId) = td_tmq_committed_not_provided;

TAOS *(*fp_tmq_get_connect)(tmq_t *tmq) = td_tmq_get_connect_not_provided;
const char *(*fp_tmq_get_table_name)(TAOS_RES *res) = td_tmq_get_table_name_not_provided;
tmq_res_t (*fp_tmq_get_res_type)(TAOS_RES *res) = td_tmq_get_res_type_not_provided;
const char *(*fp_tmq_get_topic_name)(TAOS_RES *res) = td_tmq_get_topic_name_not_provided;
const char *(*fp_tmq_get_db_name)(TAOS_RES *res) = td_tmq_get_db_name_not_provided;
int32_t (*fp_tmq_get_vgroup_id)(TAOS_RES *res) = td_tmq_get_vgroup_id_not_provided;
int64_t (*fp_tmq_get_vgroup_offset)(TAOS_RES *res) = td_tmq_get_vgroup_offset_not_provided;
const char *(*fp_tmq_err2str)(int32_t code) = td_tmq_err2str_not_provided;

int32_t (*fp_tmq_get_raw)(TAOS_RES *res, tmq_raw_data *raw) = td_tmq_get_raw_not_provided;
int32_t (*fp_tmq_write_raw)(TAOS *taos, tmq_raw_data raw) = td_tmq_write_raw_not_provided;
int (*fp_taos_write_raw_block)(TAOS *taos, int numOfRows, char *pData, const char *tbname) = td_taos_write_raw_block_not_provided;
int (*fp_taos_write_raw_block_with_reqid)(TAOS *taos, int numOfRows, char *pData, const char *tbname,
                                          int64_t reqid) = td_taos_write_raw_block_with_reqid_not_provided;
int (*fp_taos_write_raw_block_with_fields)(TAOS *taos, int rows, char *pData, const char *tbname, TAOS_FIELD *fields,
                                           int numFields) = td_taos_write_raw_block_with_fields_not_provided;
int (*fp_taos_write_raw_block_with_fields_with_reqid)(TAOS *taos, int rows, char *pData, const char *tbname,
                                                      TAOS_FIELD *fields, int numFields, int64_t reqid) = td_taos_write_raw_block_with_fields_with_reqid_not_provided;
void (*fp_tmq_free_raw)(tmq_raw_data raw) = td_tmq_free_raw_not_provided;
char *(*fp_tmq_get_json_meta)(TAOS_RES *res) = td_tmq_get_json_meta_not_provided;
void (*fp_tmq_free_json_meta)(char *jsonMeta) = td_tmq_free_json_meta_not_provided;

TSDB_SERVER_STATUS (*fp_taos_check_server_status)(const char *fqdn, int port, char *details, int maxlen) = td_taos_check_server_status_not_provided;
void (*fp_taos_write_crashinfo)(int signum, void *sigInfo, void *context) = td_taos_write_crashinfo_not_provided;
char *(*fp_getBuildInfo)() = td_getBuildInfo_not_provided;

int32_t (*fp_taos_connect_is_alive)(TAOS *taos) = td_taos_connect_is_alive_not_provided;
int32_t (*fp_taos_register_instance)(const char *id, const char *type, const char *desc, int32_t expire) = td_taos_register_instance_not_provided;
int32_t (*fp_taos_list_instances)(const char *filter_type, char ***pList, int32_t *pCount) = td_taos_list_instances_not_provided;
void    (*fp_taos_free_instances)(char ***pList, int32_t pCount) = td_taos_free_instances_not_provided;

/*
 * The same slots, for the two places that have to walk all of them:
 * taosDriverCleanup() must not leave an unloaded library reachable through a
 * slot, and the post-init report names the optional entry points the driver
 * does not provide.
 */
const TdWrapperSlot tdWrapperSlots[] = {
    { "taos_set_config", (void **)&fp_taos_set_config, (void *)td_taos_set_config_not_provided },
    { "taos_init", (void **)&fp_taos_init, (void *)td_taos_init_not_provided },
    { "taos_cleanup", (void **)&fp_taos_cleanup, (void *)td_taos_cleanup_not_provided },
    { "taos_options", (void **)&fp_taos_options, (void *)td_taos_options_not_provided },
    { "taos_options_connection", (void **)&fp_taos_options_connection, (void *)td_taos_options_connection_not_provided },
    { "taos_set_option", (void **)&fp_taos_set_option, (void *)td_taos_set_option_not_provided },
    { "taos_connect", (void **)&fp_taos_connect, (void *)td_taos_connect_not_provided },
    { "taos_connect_totp", (void **)&fp_taos_connect_totp, (void *)td_taos_connect_totp_not_provided },
    { "taos_connect_test", (void **)&fp_taos_connect_test, (void *)td_taos_connect_test_not_provided },
    { "taos_connect_token", (void **)&fp_taos_connect_token, (void *)td_taos_connect_token_not_provided },
    { "taos_connect_auth", (void **)&fp_taos_connect_auth, (void *)td_taos_connect_auth_not_provided },
    { "taos_connect_with", (void **)&fp_taos_connect_with, (void *)td_taos_connect_with_not_provided },
    { "taos_connect_with_dsn", (void **)&fp_taos_connect_with_dsn, (void *)td_taos_connect_with_dsn_not_provided },
    { "taos_close", (void **)&fp_taos_close, (void *)td_taos_close_not_provided },
    { "taos_txn_begin", (void **)&fp_taos_txn_begin, (void *)td_taos_txn_begin_not_provided },
    { "taos_txn_commit", (void **)&fp_taos_txn_commit, (void *)td_taos_txn_commit_not_provided },
    { "taos_txn_rollback", (void **)&fp_taos_txn_rollback, (void *)td_taos_txn_rollback_not_provided },
    { "taos_data_type", (void **)&fp_taos_data_type, (void *)td_taos_data_type_not_provided },
    { "taos_stmt_init", (void **)&fp_taos_stmt_init, (void *)td_taos_stmt_init_not_provided },
    { "taos_stmt_init_with_reqid", (void **)&fp_taos_stmt_init_with_reqid, (void *)td_taos_stmt_init_with_reqid_not_provided },
    { "taos_stmt_init_with_options", (void **)&fp_taos_stmt_init_with_options, (void *)td_taos_stmt_init_with_options_not_provided },
    { "taos_stmt_prepare", (void **)&fp_taos_stmt_prepare, (void *)td_taos_stmt_prepare_not_provided },
    { "taos_stmt_set_tbname_tags", (void **)&fp_taos_stmt_set_tbname_tags, (void *)td_taos_stmt_set_tbname_tags_not_provided },
    { "taos_stmt_set_tbname", (void **)&fp_taos_stmt_set_tbname, (void *)td_taos_stmt_set_tbname_not_provided },
    { "taos_stmt_set_tags", (void **)&fp_taos_stmt_set_tags, (void *)td_taos_stmt_set_tags_not_provided },
    { "taos_stmt_set_sub_tbname", (void **)&fp_taos_stmt_set_sub_tbname, (void *)td_taos_stmt_set_sub_tbname_not_provided },
    { "taos_stmt_get_tag_fields", (void **)&fp_taos_stmt_get_tag_fields, (void *)td_taos_stmt_get_tag_fields_not_provided },
    { "taos_stmt_get_col_fields", (void **)&fp_taos_stmt_get_col_fields, (void *)td_taos_stmt_get_col_fields_not_provided },
    { "taos_stmt_reclaim_fields", (void **)&fp_taos_stmt_reclaim_fields, (void *)td_taos_stmt_reclaim_fields_not_provided },
    { "taos_stmt_is_insert", (void **)&fp_taos_stmt_is_insert, (void *)td_taos_stmt_is_insert_not_provided },
    { "taos_stmt_num_params", (void **)&fp_taos_stmt_num_params, (void *)td_taos_stmt_num_params_not_provided },
    { "taos_stmt_get_param", (void **)&fp_taos_stmt_get_param, (void *)td_taos_stmt_get_param_not_provided },
    { "taos_stmt_bind_param", (void **)&fp_taos_stmt_bind_param, (void *)td_taos_stmt_bind_param_not_provided },
    { "taos_stmt_bind_param_batch", (void **)&fp_taos_stmt_bind_param_batch, (void *)td_taos_stmt_bind_param_batch_not_provided },
    { "taos_stmt_bind_single_param_batch", (void **)&fp_taos_stmt_bind_single_param_batch, (void *)td_taos_stmt_bind_single_param_batch_not_provided },
    { "taos_stmt_add_batch", (void **)&fp_taos_stmt_add_batch, (void *)td_taos_stmt_add_batch_not_provided },
    { "taos_stmt_execute", (void **)&fp_taos_stmt_execute, (void *)td_taos_stmt_execute_not_provided },
    { "taos_stmt_use_result", (void **)&fp_taos_stmt_use_result, (void *)td_taos_stmt_use_result_not_provided },
    { "taos_stmt_close", (void **)&fp_taos_stmt_close, (void *)td_taos_stmt_close_not_provided },
    { "taos_stmt_errstr", (void **)&fp_taos_stmt_errstr, (void *)td_taos_stmt_errstr_not_provided },
    { "taos_stmt_affected_rows", (void **)&fp_taos_stmt_affected_rows, (void *)td_taos_stmt_affected_rows_not_provided },
    { "taos_stmt_affected_rows_once", (void **)&fp_taos_stmt_affected_rows_once, (void *)td_taos_stmt_affected_rows_once_not_provided },
    { "taos_stmt2_init", (void **)&fp_taos_stmt2_init, (void *)td_taos_stmt2_init_not_provided },
    { "taos_stmt2_prepare", (void **)&fp_taos_stmt2_prepare, (void *)td_taos_stmt2_prepare_not_provided },
    { "taos_stmt2_bind_param", (void **)&fp_taos_stmt2_bind_param, (void *)td_taos_stmt2_bind_param_not_provided },
    { "taos_stmt2_bind_param_a", (void **)&fp_taos_stmt2_bind_param_a, (void *)td_taos_stmt2_bind_param_a_not_provided },
    { "taos_stmt2_bind_param_column", (void **)&fp_taos_stmt2_bind_param_column, (void *)td_taos_stmt2_bind_param_column_not_provided },
    { "taos_stmt2_bind_param_column_a", (void **)&fp_taos_stmt2_bind_param_column_a, (void *)td_taos_stmt2_bind_param_column_a_not_provided },
    { "taos_stmt2_exec", (void **)&fp_taos_stmt2_exec, (void *)td_taos_stmt2_exec_not_provided },
    { "taos_stmt2_close", (void **)&fp_taos_stmt2_close, (void *)td_taos_stmt2_close_not_provided },
    { "taos_stmt2_is_insert", (void **)&fp_taos_stmt2_is_insert, (void *)td_taos_stmt2_is_insert_not_provided },
    { "taos_stmt2_get_fields", (void **)&fp_taos_stmt2_get_fields, (void *)td_taos_stmt2_get_fields_not_provided },
    { "taos_stmt2_free_fields", (void **)&fp_taos_stmt2_free_fields, (void *)td_taos_stmt2_free_fields_not_provided },
    { "taos_stmt2_result", (void **)&fp_taos_stmt2_result, (void *)td_taos_stmt2_result_not_provided },
    { "taos_stmt2_error", (void **)&fp_taos_stmt2_error, (void *)td_taos_stmt2_error_not_provided },
    { "taos_query", (void **)&fp_taos_query, (void *)td_taos_query_not_provided },
    { "taos_query_with_reqid", (void **)&fp_taos_query_with_reqid, (void *)td_taos_query_with_reqid_not_provided },
    { "taos_fetch_row", (void **)&fp_taos_fetch_row, (void *)td_taos_fetch_row_not_provided },
    { "taos_result_precision", (void **)&fp_taos_result_precision, (void *)td_taos_result_precision_not_provided },
    { "taos_free_result", (void **)&fp_taos_free_result, (void *)td_taos_free_result_not_provided },
    { "taos_kill_query", (void **)&fp_taos_kill_query, (void *)td_taos_kill_query_not_provided },
    { "taos_field_count", (void **)&fp_taos_field_count, (void *)td_taos_field_count_not_provided },
    { "taos_num_fields", (void **)&fp_taos_num_fields, (void *)td_taos_num_fields_not_provided },
    { "taos_affected_rows", (void **)&fp_taos_affected_rows, (void *)td_taos_affected_rows_not_provided },
    { "taos_affected_rows64", (void **)&fp_taos_affected_rows64, (void *)td_taos_affected_rows64_not_provided },
    { "taos_fetch_fields", (void **)&fp_taos_fetch_fields, (void *)td_taos_fetch_fields_not_provided },
    { "taos_fetch_fields_e", (void **)&fp_taos_fetch_fields_e, (void *)td_taos_fetch_fields_e_not_provided },
    { "taos_select_db", (void **)&fp_taos_select_db, (void *)td_taos_select_db_not_provided },
    { "taos_print_row", (void **)&fp_taos_print_row, (void *)td_taos_print_row_not_provided },
    { "taos_print_row_with_size", (void **)&fp_taos_print_row_with_size, (void *)td_taos_print_row_with_size_not_provided },
    { "taos_stop_query", (void **)&fp_taos_stop_query, (void *)td_taos_stop_query_not_provided },
    { "taos_is_null", (void **)&fp_taos_is_null, (void *)td_taos_is_null_not_provided },
    { "taos_is_null_by_column", (void **)&fp_taos_is_null_by_column, (void *)td_taos_is_null_by_column_not_provided },
    { "taos_is_update_query", (void **)&fp_taos_is_update_query, (void *)td_taos_is_update_query_not_provided },
    { "taos_fetch_block", (void **)&fp_taos_fetch_block, (void *)td_taos_fetch_block_not_provided },
    { "taos_fetch_block_s", (void **)&fp_taos_fetch_block_s, (void *)td_taos_fetch_block_s_not_provided },
    { "taos_fetch_raw_block", (void **)&fp_taos_fetch_raw_block, (void *)td_taos_fetch_raw_block_not_provided },
    { "taos_get_column_data_offset", (void **)&fp_taos_get_column_data_offset, (void *)td_taos_get_column_data_offset_not_provided },
    { "taos_validate_sql", (void **)&fp_taos_validate_sql, (void *)td_taos_validate_sql_not_provided },
    { "taos_reset_current_db", (void **)&fp_taos_reset_current_db, (void *)td_taos_reset_current_db_not_provided },
    { "taos_fetch_lengths", (void **)&fp_taos_fetch_lengths, (void *)td_taos_fetch_lengths_not_provided },
    { "taos_result_block", (void **)&fp_taos_result_block, (void *)td_taos_result_block_not_provided },
    { "taos_get_server_info", (void **)&fp_taos_get_server_info, (void *)td_taos_get_server_info_not_provided },
    { "taos_get_client_info", (void **)&fp_taos_get_client_info, (void *)td_taos_get_client_info_not_provided },
    { "taos_get_current_db", (void **)&fp_taos_get_current_db, (void *)td_taos_get_current_db_not_provided },
    { "taos_get_connection_info", (void **)&fp_taos_get_connection_info, (void *)td_taos_get_connection_info_not_provided },
    { "taos_get_result_tz", (void **)&fp_taos_get_result_tz, (void *)td_taos_get_result_tz_not_provided },
    { "taos_errstr", (void **)&fp_taos_errstr, (void *)td_taos_errstr_not_provided },
    { "taos_errno", (void **)&fp_taos_errno, (void *)td_taos_errno_not_provided },
    { "taos_query_a", (void **)&fp_taos_query_a, (void *)td_taos_query_a_not_provided },
    { "taos_query_a_with_reqid", (void **)&fp_taos_query_a_with_reqid, (void *)td_taos_query_a_with_reqid_not_provided },
    { "taos_fetch_rows_a", (void **)&fp_taos_fetch_rows_a, (void *)td_taos_fetch_rows_a_not_provided },
    { "taos_fetch_raw_block_a", (void **)&fp_taos_fetch_raw_block_a, (void *)td_taos_fetch_raw_block_a_not_provided },
    { "taos_get_raw_block", (void **)&fp_taos_get_raw_block, (void *)td_taos_get_raw_block_not_provided },
    { "taos_get_db_route_info", (void **)&fp_taos_get_db_route_info, (void *)td_taos_get_db_route_info_not_provided },
    { "taos_get_table_vgId", (void **)&fp_taos_get_table_vgId, (void *)td_taos_get_table_vgId_not_provided },
    { "taos_get_tables_vgId", (void **)&fp_taos_get_tables_vgId, (void *)td_taos_get_tables_vgId_not_provided },
    { "taos_load_table_info", (void **)&fp_taos_load_table_info, (void *)td_taos_load_table_info_not_provided },
    { "taos_set_hb_quit", (void **)&fp_taos_set_hb_quit, (void *)td_taos_set_hb_quit_not_provided },
    { "taos_set_notify_cb", (void **)&fp_taos_set_notify_cb, (void *)td_taos_set_notify_cb_not_provided },
    { "taos_fetch_whitelist_a", (void **)&fp_taos_fetch_whitelist_a, (void *)td_taos_fetch_whitelist_a_not_provided },
    { "taos_fetch_whitelist_dual_stack_a", (void **)&fp_taos_fetch_whitelist_dual_stack_a, (void *)td_taos_fetch_whitelist_dual_stack_a_not_provided },
    { "taos_fetch_ip_whitelist_a", (void **)&fp_taos_fetch_ip_whitelist_a, (void *)td_taos_fetch_ip_whitelist_a_not_provided },
    { "taos_fetch_datetime_whitelist_a", (void **)&fp_taos_fetch_datetime_whitelist_a, (void *)td_taos_fetch_datetime_whitelist_a_not_provided },
    { "taos_set_conn_mode", (void **)&fp_taos_set_conn_mode, (void *)td_taos_set_conn_mode_not_provided },
    { "taos_schemaless_insert", (void **)&fp_taos_schemaless_insert, (void *)td_taos_schemaless_insert_not_provided },
    { "taos_schemaless_insert_with_reqid", (void **)&fp_taos_schemaless_insert_with_reqid, (void *)td_taos_schemaless_insert_with_reqid_not_provided },
    { "taos_schemaless_insert_raw", (void **)&fp_taos_schemaless_insert_raw, (void *)td_taos_schemaless_insert_raw_not_provided },
    { "taos_schemaless_insert_raw_with_reqid", (void **)&fp_taos_schemaless_insert_raw_with_reqid, (void *)td_taos_schemaless_insert_raw_with_reqid_not_provided },
    { "taos_schemaless_insert_ttl", (void **)&fp_taos_schemaless_insert_ttl, (void *)td_taos_schemaless_insert_ttl_not_provided },
    { "taos_schemaless_insert_ttl_with_reqid", (void **)&fp_taos_schemaless_insert_ttl_with_reqid, (void *)td_taos_schemaless_insert_ttl_with_reqid_not_provided },
    { "taos_schemaless_insert_raw_ttl", (void **)&fp_taos_schemaless_insert_raw_ttl, (void *)td_taos_schemaless_insert_raw_ttl_not_provided },
    { "taos_schemaless_insert_raw_ttl_with_reqid", (void **)&fp_taos_schemaless_insert_raw_ttl_with_reqid, (void *)td_taos_schemaless_insert_raw_ttl_with_reqid_not_provided },
    { "taos_schemaless_insert_raw_ttl_with_reqid_tbname_key", (void **)&fp_taos_schemaless_insert_raw_ttl_with_reqid_tbname_key, (void *)td_taos_schemaless_insert_raw_ttl_with_reqid_tbname_key_not_provided },
    { "taos_schemaless_insert_ttl_with_reqid_tbname_key", (void **)&fp_taos_schemaless_insert_ttl_with_reqid_tbname_key, (void *)td_taos_schemaless_insert_ttl_with_reqid_tbname_key_not_provided },
    { "tmq_conf_new", (void **)&fp_tmq_conf_new, (void *)td_tmq_conf_new_not_provided },
    { "tmq_conf_set", (void **)&fp_tmq_conf_set, (void *)td_tmq_conf_set_not_provided },
    { "tmq_conf_destroy", (void **)&fp_tmq_conf_destroy, (void *)td_tmq_conf_destroy_not_provided },
    { "tmq_conf_set_auto_commit_cb", (void **)&fp_tmq_conf_set_auto_commit_cb, (void *)td_tmq_conf_set_auto_commit_cb_not_provided },
    { "tmq_list_new", (void **)&fp_tmq_list_new, (void *)td_tmq_list_new_not_provided },
    { "tmq_list_append", (void **)&fp_tmq_list_append, (void *)td_tmq_list_append_not_provided },
    { "tmq_list_destroy", (void **)&fp_tmq_list_destroy, (void *)td_tmq_list_destroy_not_provided },
    { "tmq_list_get_size", (void **)&fp_tmq_list_get_size, (void *)td_tmq_list_get_size_not_provided },
    { "tmq_list_to_c_array", (void **)&fp_tmq_list_to_c_array, (void *)td_tmq_list_to_c_array_not_provided },
    { "tmq_consumer_new", (void **)&fp_tmq_consumer_new, (void *)td_tmq_consumer_new_not_provided },
    { "tmq_subscribe", (void **)&fp_tmq_subscribe, (void *)td_tmq_subscribe_not_provided },
    { "tmq_unsubscribe", (void **)&fp_tmq_unsubscribe, (void *)td_tmq_unsubscribe_not_provided },
    { "tmq_subscription", (void **)&fp_tmq_subscription, (void *)td_tmq_subscription_not_provided },
    { "tmq_consumer_poll", (void **)&fp_tmq_consumer_poll, (void *)td_tmq_consumer_poll_not_provided },
    { "tmq_consumer_close", (void **)&fp_tmq_consumer_close, (void *)td_tmq_consumer_close_not_provided },
    { "tmq_commit_sync", (void **)&fp_tmq_commit_sync, (void *)td_tmq_commit_sync_not_provided },
    { "tmq_commit_async", (void **)&fp_tmq_commit_async, (void *)td_tmq_commit_async_not_provided },
    { "tmq_commit_offset_sync", (void **)&fp_tmq_commit_offset_sync, (void *)td_tmq_commit_offset_sync_not_provided },
    { "tmq_commit_offset_async", (void **)&fp_tmq_commit_offset_async, (void *)td_tmq_commit_offset_async_not_provided },
    { "tmq_get_topic_assignment", (void **)&fp_tmq_get_topic_assignment, (void *)td_tmq_get_topic_assignment_not_provided },
    { "tmq_free_assignment", (void **)&fp_tmq_free_assignment, (void *)td_tmq_free_assignment_not_provided },
    { "tmq_offset_seek", (void **)&fp_tmq_offset_seek, (void *)td_tmq_offset_seek_not_provided },
    { "tmq_position", (void **)&fp_tmq_position, (void *)td_tmq_position_not_provided },
    { "tmq_committed", (void **)&fp_tmq_committed, (void *)td_tmq_committed_not_provided },
    { "tmq_get_connect", (void **)&fp_tmq_get_connect, (void *)td_tmq_get_connect_not_provided },
    { "tmq_get_table_name", (void **)&fp_tmq_get_table_name, (void *)td_tmq_get_table_name_not_provided },
    { "tmq_get_res_type", (void **)&fp_tmq_get_res_type, (void *)td_tmq_get_res_type_not_provided },
    { "tmq_get_topic_name", (void **)&fp_tmq_get_topic_name, (void *)td_tmq_get_topic_name_not_provided },
    { "tmq_get_db_name", (void **)&fp_tmq_get_db_name, (void *)td_tmq_get_db_name_not_provided },
    { "tmq_get_vgroup_id", (void **)&fp_tmq_get_vgroup_id, (void *)td_tmq_get_vgroup_id_not_provided },
    { "tmq_get_vgroup_offset", (void **)&fp_tmq_get_vgroup_offset, (void *)td_tmq_get_vgroup_offset_not_provided },
    { "tmq_err2str", (void **)&fp_tmq_err2str, (void *)td_tmq_err2str_not_provided },
    { "tmq_get_raw", (void **)&fp_tmq_get_raw, (void *)td_tmq_get_raw_not_provided },
    { "tmq_write_raw", (void **)&fp_tmq_write_raw, (void *)td_tmq_write_raw_not_provided },
    { "taos_write_raw_block", (void **)&fp_taos_write_raw_block, (void *)td_taos_write_raw_block_not_provided },
    { "taos_write_raw_block_with_reqid", (void **)&fp_taos_write_raw_block_with_reqid, (void *)td_taos_write_raw_block_with_reqid_not_provided },
    { "taos_write_raw_block_with_fields", (void **)&fp_taos_write_raw_block_with_fields, (void *)td_taos_write_raw_block_with_fields_not_provided },
    { "taos_write_raw_block_with_fields_with_reqid", (void **)&fp_taos_write_raw_block_with_fields_with_reqid, (void *)td_taos_write_raw_block_with_fields_with_reqid_not_provided },
    { "tmq_free_raw", (void **)&fp_tmq_free_raw, (void *)td_tmq_free_raw_not_provided },
    { "tmq_get_json_meta", (void **)&fp_tmq_get_json_meta, (void *)td_tmq_get_json_meta_not_provided },
    { "tmq_free_json_meta", (void **)&fp_tmq_free_json_meta, (void *)td_tmq_free_json_meta_not_provided },
    { "taos_check_server_status", (void **)&fp_taos_check_server_status, (void *)td_taos_check_server_status_not_provided },
    { "taos_write_crashinfo", (void **)&fp_taos_write_crashinfo, (void *)td_taos_write_crashinfo_not_provided },
    { "getBuildInfo", (void **)&fp_getBuildInfo, (void *)td_getBuildInfo_not_provided },
    { "taos_connect_is_alive", (void **)&fp_taos_connect_is_alive, (void *)td_taos_connect_is_alive_not_provided },
    { "taos_register_instance", (void **)&fp_taos_register_instance, (void *)td_taos_register_instance_not_provided },
    { "taos_list_instances", (void **)&fp_taos_list_instances, (void *)td_taos_list_instances_not_provided },
    { "taos_free_instances", (void **)&fp_taos_free_instances, (void *)td_taos_free_instances_not_provided },
    { NULL, NULL, NULL },
};

/* Back to "not provided": after the driver is unloaded, a call has to report an
 * error instead of jumping into the image that is gone. */
void taosDriverResetSlots(void) {
  for (const TdWrapperSlot *entry = tdWrapperSlots; entry->name != NULL; ++entry) {
    *entry->slot = entry->def;
  }
}

/* Said once, after init, instead of on every call.  Called from taos_init(),
 * once the driver's own init has brought the log up: an entry point the driver
 * does not provide is a diagnostic to record, not a line to push onto the
 * caller's terminal (taos -Z 1 prints it on every start otherwise). */
void taosDriverReportMissing(void) {
  const char *missing[8] = {0};
  int32_t     maxMissing = (int32_t)(sizeof(missing) / sizeof(missing[0]));
  int32_t     missingCount = 0;
  int32_t     total = 0;

  for (const TdWrapperSlot *entry = tdWrapperSlots; entry->name != NULL; ++entry) {
    ++total;
    if (*entry->slot != entry->def) {
      continue;
    }
    if (missingCount < maxMissing) {
      missing[missingCount] = entry->name;
    }
    ++missingCount;
  }

  if (missingCount == 0) {
    return;
  }

  char    msg[512];
  int32_t len = snprintf(msg, sizeof(msg), "taos: the loaded driver does not provide %d of %d entry points:",
                         missingCount, total);
  if (len < 0) {
    return;
  }
  if (len > (int32_t)sizeof(msg) - 1) {
    len = (int32_t)sizeof(msg) - 1;
  }

  for (int32_t i = 0; i < missingCount && i < maxMissing; ++i) {
    int32_t n = snprintf(msg + len, sizeof(msg) - len, "%s%s", i == 0 ? " " : ", ", missing[i]);
    if (n < 0) {
      break;
    }
    if (n > (int32_t)sizeof(msg) - len - 1) {
      len = (int32_t)sizeof(msg) - 1;
      break;
    }
    len += n;
  }

  if (missingCount > maxMissing && len < (int32_t)sizeof(msg) - 1) {
    (void)snprintf(msg + len, sizeof(msg) - len, ", ...");
  }

  uWarn("%s", msg);
}
