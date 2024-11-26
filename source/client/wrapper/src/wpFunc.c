/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

#include "os.h"
#include "wrapper.h"

ETaosDriverType tsDriverType = TAOS_DRIVER_NATIVE;

static void *test = NULL;
#define CHECK_VOID_FUNC   \
  if (wpHandle == NULL) { \
    return;               \
  }

#define CHECK_INT_FUNC    \
  if (wpHandle == NULL) { \
    return -1;            \
  }

#define CHECK_PTR_FUNC    \
  if (wpHandle == NULL) { \
    return NULL;          \
  }

#define CHECK_CONFRET_FUNC \
  if (wpHandle == NULL) {  \
    setConfRet ret = {0};  \
    ret.retCode = -1;      \
    return ret;            \
  }

#define CHECK_BOOL_FUNC   \
  if (wpHandle == NULL) { \
    setConfRet ret = {0}; \
    return false;         \
  }

void taos_cleanup(void) {
  CHECK_VOID_FUNC;
  return (*fptr_taos_cleanup)();
}

int taos_options(TSDB_OPTION option, const void *arg, ...) {
  CHECK_INT_FUNC;
  (*fptr_taos_options)();
}

setConfRet taos_set_config(const char *config) {
  CHECK_CONFRET_FUNC;
  return (*fptr_taos_set_config)(config);
}

int taos_init(void) {
  CHECK_INT_FUNC;
  return (*fptr_taos_init)();
}

TAOS *taos_connect(const char *ip, const char *user, const char *pass, const char *db, uint16_t port) {
  CHECK_PTR_FUNC;
  return (*fptr_taos_connect)(ip, user, pass, db, port);
}

TAOS *taos_connect_auth(const char *ip, const char *user, const char *auth, const char *db, uint16_t port) {
  CHECK_PTR_FUNC;
  return (*fptr_taos_connect_auth)(ip, user, auth, db, port);
}

void taos_close(TAOS *taos) {
  CHECK_VOID_FUNC;
  (*fptr_taos_close)(taos);
}

const char *taos_data_type(int type) {
  CHECK_PTR_FUNC;
  return (*fptr_taos_data_type)(type);
}

TAOS_STMT *taos_stmt_init(TAOS *taos) {
  CHECK_PTR_FUNC;
  return (*fptr_taos_stmt_init)(taos);
}

TAOS_STMT *taos_stmt_init_with_reqid(TAOS *taos, int64_t reqid) {
  CHECK_PTR_FUNC;
  return (*fptr_taos_stmt_init_with_reqid)(taos, reqid);
}

TAOS_STMT *taos_stmt_init_with_options(TAOS *taos, TAOS_STMT_OPTIONS *options) {
  CHECK_PTR_FUNC;
  return (*fptr_taos_stmt_init_with_options)(taos, options);
}

int taos_stmt_prepare(TAOS_STMT *stmt, const char *sql, unsigned long length) {
  CHECK_INT_FUNC;
  return (*fptr_taos_stmt_prepare)(stmt, sql, length);
}

int taos_stmt_set_tbname_tags(TAOS_STMT *stmt, const char *name, TAOS_MULTI_BIND *tags) {
  CHECK_INT_FUNC;
  return (*fptr_taos_stmt_set_tbname_tags)(stmt, name, tags);
}

int taos_stmt_set_tbname(TAOS_STMT *stmt, const char *name) {
  CHECK_INT_FUNC;
  return (*fptr_taos_stmt_set_tbname)(stmt, name);
}

int taos_stmt_set_tags(TAOS_STMT *stmt, TAOS_MULTI_BIND *tags) {
  CHECK_INT_FUNC;
  return (*fptr_taos_stmt_set_tags)(stmt, tags);
}

int taos_stmt_set_sub_tbname(TAOS_STMT *stmt, const char *name) {
  CHECK_INT_FUNC;
  return (*fptr_taos_stmt_set_sub_tbname)(stmt, name);
}

int taos_stmt_get_tag_fields(TAOS_STMT *stmt, int *fieldNum, TAOS_FIELD_E **fields) {
  CHECK_INT_FUNC;
  return (*fptr_taos_stmt_get_tag_fields)(stmt, fieldNum, fields);
}

int taos_stmt_get_col_fields(TAOS_STMT *stmt, int *fieldNum, TAOS_FIELD_E **fields) {
  CHECK_INT_FUNC;
  return (*fptr_taos_stmt_get_col_fields)(stmt, fieldNum, fields);
}

void taos_stmt_reclaim_fields(TAOS_STMT *stmt, TAOS_FIELD_E *fields) {
  CHECK_VOID_FUNC;
  (*fptr_taos_stmt_reclaim_fields)(stmt, fields);
}

int taos_stmt_is_insert(TAOS_STMT *stmt, int *insert) {
  CHECK_INT_FUNC;
  return (*fptr_taos_stmt_is_insert)(stmt, insert);
}

int taos_stmt_num_params(TAOS_STMT *stmt, int *nums) {
  CHECK_INT_FUNC;
  return (*fptr_taos_stmt_num_params)(stmt, nums);
}

int taos_stmt_get_param(TAOS_STMT *stmt, int idx, int *type, int *bytes) {
  CHECK_INT_FUNC;
  return (*fptr_taos_stmt_get_param)(stmt, idx, type, bytes);
}

int taos_stmt_bind_param(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind) {
  CHECK_INT_FUNC;
  return (*fptr_taos_stmt_bind_param)(stmt, bind);
}

int taos_stmt_bind_param_batch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind) {
  CHECK_INT_FUNC;
  return (*fptr_taos_stmt_bind_param_batch)(stmt, bind);
}

int taos_stmt_bind_single_param_batch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind, int colIdx) {
  CHECK_INT_FUNC;
  return (*fptr_taos_stmt_bind_single_param_batch)(stmt, bind, colIdx);
}

int taos_stmt_add_batch(TAOS_STMT *stmt) {
  CHECK_INT_FUNC;
  return (*fptr_taos_stmt_add_batch)(stmt);
}

int taos_stmt_execute(TAOS_STMT *stmt) {
  CHECK_INT_FUNC;
  return (*fptr_taos_stmt_execute)(stmt);
}

TAOS_RES *taos_stmt_use_result(TAOS_STMT *stmt) {
  CHECK_PTR_FUNC;
  return (*fptr_taos_stmt_use_result)(stmt);
}

int taos_stmt_close(TAOS_STMT *stmt) {
  CHECK_INT_FUNC;
  return (*fptr_taos_stmt_close)(stmt);
}

char *taos_stmt_errstr(TAOS_STMT *stmt) {
  CHECK_PTR_FUNC;
  return (*fptr_taos_stmt_errstr)(stmt);
}

int taos_stmt_affected_rows(TAOS_STMT *stmt) {
  CHECK_INT_FUNC;
  return (*fptr_taos_stmt_affected_rows)(stmt);
}

int taos_stmt_affected_rows_once(TAOS_STMT *stmt) {
  CHECK_INT_FUNC;
  return (*fptr_taos_stmt_affected_rows_once)(stmt);
}

TAOS_STMT2 *taos_stmt2_init(TAOS *taos, TAOS_STMT2_OPTION *option) {
  CHECK_PTR_FUNC;
  return (*fptr_taos_stmt2_init)(taos, option);
}

int taos_stmt2_prepare(TAOS_STMT2 *stmt, const char *sql, unsigned long length) {
  CHECK_INT_FUNC;
  return (*fptr_taos_cleanup)(stmt, sql, length);
}

int taos_stmt2_bind_param(TAOS_STMT2 *stmt, TAOS_STMT2_BINDV *bindv, int32_t col_idx) {
  CHECK_INT_FUNC;
  return (*fptr_taos_stmt2_bind_param)(stmt, bindv, col_idx);
}

int taos_stmt2_exec(TAOS_STMT2 *stmt, int *affected_rows) {
  CHECK_INT_FUNC;
  return (*fptr_taos_stmt2_exec)(stmt, affected_rows);
}

int taos_stmt2_close(TAOS_STMT2 *stmt) {
  CHECK_INT_FUNC;
  return (*fptr_taos_stmt2_close)(stmt);
}

int taos_stmt2_is_insert(TAOS_STMT2 *stmt, int *insert) {
  CHECK_INT_FUNC;
  return (*fptr_taos_stmt2_is_insert)(stmt, insert);
}

int taos_stmt2_get_fields(TAOS_STMT2 *stmt, TAOS_FIELD_T field_type, int *count, TAOS_FIELD_E **fields) {
  CHECK_INT_FUNC;
  return (*fptr_taos_stmt2_get_fields)(stmt, field_type, count, fields);
}

int taos_stmt2_get_stb_fields(TAOS_STMT2 *stmt, int *count, TAOS_FIELD_STB **fields) {
  CHECK_INT_FUNC;
  return (*fptr_taos_stmt2_get_stb_fields)(stmt, count, fields);
}

void taos_stmt2_free_fields(TAOS_STMT2 *stmt, TAOS_FIELD_E *fields) {
  CHECK_VOID_FUNC;
  (*fptr_taos_stmt2_free_fields)(stmt, fields);
}

void taos_stmt2_free_stb_fields(TAOS_STMT2 *stmt, TAOS_FIELD_STB *fields) {
  CHECK_VOID_FUNC;
  (*fptr_taos_stmt2_free_stb_fields)(stmt, fields);
}

TAOS_RES *taos_stmt2_result(TAOS_STMT2 *stmt) {
  CHECK_PTR_FUNC;
  return (*fptr_taos_stmt2_result)(stmt);
}

char *taos_stmt2_error(TAOS_STMT2 *stmt) {
  CHECK_PTR_FUNC;
  return (*fptr_taos_stmt2_error)(stmt);
}

TAOS_RES *taos_query(TAOS *taos, const char *sql) {
  CHECK_PTR_FUNC;
  return (*fptr_taos_query)(taos, sql);
}

TAOS_RES *taos_query_with_reqid(TAOS *taos, const char *sql, int64_t reqId) {
  CHECK_PTR_FUNC;
  return (*fptr_taos_query_with_reqid)(taos, sql, reqId);
}

TAOS_ROW taos_fetch_row(TAOS_RES *res) {
  CHECK_PTR_FUNC;
  return (*taos_fetch_row)(res);
}

int taos_result_precision(TAOS_RES *res) {
  CHECK_INT_FUNC;
  return (*fptr_taos_result_precision)(res);
}

void taos_free_result(TAOS_RES *res) {
  CHECK_VOID_FUNC;
  return (*fptr_taos_free_result)(res);
}

void taos_kill_query(TAOS *taos) {
  CHECK_VOID_FUNC;
  return (*fptr_taos_kill_query)(taos);
}

int taos_field_count(TAOS_RES *res) {
  CHECK_INT_FUNC;
  return (*fptr_taos_field_count)(res);
}

int taos_num_fields(TAOS_RES *res) {
  CHECK_INT_FUNC;
  return (*fptr_taos_num_fields)(res);
}

int taos_affected_rows(TAOS_RES *res) {
  CHECK_INT_FUNC;
  return (*fptr_taos_affected_rows)(res);
}

int64_t taos_affected_rows64(TAOS_RES *res) {
  CHECK_INT_FUNC;
  return (*fptr_taos_affected_rows64)(res);
}

TAOS_FIELD *taos_fetch_fields(TAOS_RES *res) {
  CHECK_PTR_FUNC;
  return (*fptr_taos_fetch_fields)(res);
}

int taos_select_db(TAOS *taos, const char *db) {
  CHECK_INT_FUNC;
  return (*fptr_taos_select_db)(taos, db);
}

int taos_print_row(char *str, TAOS_ROW row, TAOS_FIELD *fields, int num_fields) {
  CHECK_INT_FUNC;
  return (*fptr_taos_print_row)(str, row, fields, num_fields);
}

int taos_print_row_with_size(char *str, uint32_t size, TAOS_ROW row, TAOS_FIELD *fields, int num_fields) {
  (*fptr_taos_print_row_with_size)(str, size, row, fields, num_fields);
}

void taos_stop_query(TAOS_RES *res) {
  CHECK_VOID_FUNC;
  (*fptr_taos_stop_query)(res);
}

bool taos_is_null(TAOS_RES *res, int32_t row, int32_t col) {
  CHECK_BOOL_FUNC;
  return (*fptr_taos_is_null)(res, row, col);
}

int taos_is_null_by_column(TAOS_RES *res, int columnIndex, bool result[], int *rows) {
  CHECK_INT_FUNC;
  return (*fptr_taos_is_null_by_column)(res, columnIndex, result, rows);
}

bool taos_is_update_query(TAOS_RES *res) {
  CHECK_BOOL_FUNC;
  return (*fptr_taos_is_update_query)(res);
}

int taos_fetch_block(TAOS_RES *res, TAOS_ROW *rows) {
  CHECK_INT_FUNC;
  return (*fptr_taos_fetch_block)(res, rows);
}

int taos_fetch_block_s(TAOS_RES *res, int *numOfRows, TAOS_ROW *rows) {
  CHECK_INT_FUNC;
  return (*fptr_taos_fetch_block_s)(res, numOfRows, rows);
}

int taos_fetch_raw_block(TAOS_RES *res, int *numOfRows, void **pData) {
  CHECK_INT_FUNC;
  return (*fptr_taos_fetch_raw_block)(res, numOfRows, pData);
}

int *taos_get_column_data_offset(TAOS_RES *res, int columnIndex) {
  CHECK_PTR_FUNC;
  return (*fptr_taos_get_column_data_offset)(res, columnIndex);
}

int taos_validate_sql(TAOS *taos, const char *sql) {
  CHECK_INT_FUNC;
  return (*fptr_taos_validate_sql)(taos, sql);
}

void taos_reset_current_db(TAOS *taos) {
  CHECK_VOID_FUNC;
  (*fptr_taos_reset_current_db)(taos);
}

int *taos_fetch_lengths(TAOS_RES *res) {
  CHECK_INT_FUNC;
  return (*fptr_taos_fetch_lengths)(res);
}

TAOS_ROW *taos_result_block(TAOS_RES *res) {
  CHECK_PTR_FUNC;
  return (*fptr_taos_result_block)(res);
}

const char *taos_get_server_info(TAOS *taos) {
  CHECK_PTR_FUNC;
  return (*fptr_taos_get_server_info)(taos);
}

const char *taos_get_client_info() {
  CHECK_PTR_FUNC;
  return (*fptr_taos_get_client_info)();
}

int taos_get_current_db(TAOS *taos, char *database, int len, int *required) {
  CHECK_INT_FUNC;
  return (*fptr_taos_get_current_db)(taos, database, required);
}

const char *taos_errstr(TAOS_RES *res) {
  CHECK_PTR_FUNC;
  return (*fptr_taos_errstr)(res);
}

int taos_errno(TAOS_RES *res) {
  CHECK_INT_FUNC;
  return (*fptr_taos_errno)(res);
}

void taos_query_a(TAOS *taos, const char *sql, __taos_async_fn_t fp, void *param) {
  CHECK_VOID_FUNC;
  (*fptr_taos_query_a)(taos, sql, fp, param);
}

void taos_query_a_with_reqid(TAOS *taos, const char *sql, __taos_async_fn_t fp, void *param, int64_t reqid) {
  CHECK_VOID_FUNC;
  (*fptr_taos_query_a_with_reqid)(taos, sql, fp, reqid);
}

void taos_fetch_rows_a(TAOS_RES *res, __taos_async_fn_t fp, void *param) {
  CHECK_VOID_FUNC;
  (*fptr_taos_fetch_rows_a)(res, fp, param);
}

void taos_fetch_raw_block_a(TAOS_RES *res, __taos_async_fn_t fp, void *param) {
  CHECK_VOID_FUNC;
  (*fptr_taos_fetch_raw_block_a)(res, fp, param);
}

const void *taos_get_raw_block(TAOS_RES *res) {
  CHECK_PTR_FUNC;
  return (*fptr_taos_get_raw_block)(res);
}

int taos_get_db_route_info(TAOS *taos, const char *db, TAOS_DB_ROUTE_INFO *dbInfo) {
  CHECK_INT_FUNC;
  return (*fptr_taos_get_db_route_info)(db, dbInfo);
}

int taos_get_table_vgId(TAOS *taos, const char *db, const char *table, int *vgId) {
  CHECK_INT_FUNC;
  return (*fptr_taos_get_table_vgId)(taos, db, table, vgId);
}

int taos_get_tables_vgId(TAOS *taos, const char *db, const char *table[], int tableNum, int *vgId) {
  CHECK_INT_FUNC;
  return (*fptr_taos_get_tables_vgId)(taos, db, table, tableNum, vgId);
}

int taos_load_table_info(TAOS *taos, const char *tableNameList) {
  CHECK_INT_FUNC;
  return (*fptr_taos_load_table_info)(taos, tableNameList);
}

void taos_set_hb_quit(int8_t quitByKill) {
  CHECK_INT_FUNC;
  return (*fptr_taos_set_hb_quit)(quitByKill);
}

int taos_set_notify_cb(TAOS *taos, __taos_notify_fn_t fp, void *param, int type) {
  CHECK_INT_FUNC;
  return (*fptr_taos_set_notify_cb)(taos, fp, type);
}

void taos_fetch_whitelist_a(TAOS *taos, __taos_async_whitelist_fn_t fp, void *param) {
  CHECK_VOID_FUNC;
  (*fptr_taos_fetch_whitelist_a)(taos, fp, param);
}

int taos_set_conn_mode(TAOS *taos, int mode, int value) {
  CHECK_INT_FUNC;
  return (*fptr_taos_set_conn_mode)(taos, mode, value);
}

TAOS_RES *taos_schemaless_insert(TAOS *taos, char *lines[], int numLines, int protocol, int precision) {
  CHECK_PTR_FUNC;
  return (*fptr_taos_schemaless_insert)(taos, lines, numLines, protocol, precision);
}

TAOS_RES *taos_schemaless_insert_with_reqid(TAOS *taos, char *lines[], int numLines, int protocol, int precision,
                                            int64_t reqid) {
  CHECK_PTR_FUNC;
  return (*fptr_taos_schemaless_insert_with_reqid)(taos, lines, numLines, protocol, precision, reqid);
}

TAOS_RES *taos_schemaless_insert_raw(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol,
                                     int precision) {
  CHECK_PTR_FUNC;
  return (*fptr_taos_schemaless_insert_raw)(taos, lines, len, totalRows, protocol, precision);
}

TAOS_RES *taos_schemaless_insert_raw_with_reqid(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol,
                                                int precision, int64_t reqid) {
  CHECK_PTR_FUNC;
  return (*fptr_taos_schemaless_insert_raw_with_reqid)(taos, lines, len, totalRows, protocol, precision, reqid);
}

TAOS_RES *taos_schemaless_insert_ttl(TAOS *taos, char *lines[], int numLines, int protocol, int precision,
                                     int32_t ttl) {
  CHECK_PTR_FUNC;
  return (*fptr_taos_schemaless_insert_ttl)(taos, lines, numLines, protocol, precision, ttl);
}

TAOS_RES *taos_schemaless_insert_ttl_with_reqid(TAOS *taos, char *lines[], int numLines, int protocol, int precision,
                                                int32_t ttl, int64_t reqid) {
  CHECK_PTR_FUNC;
  return (*fptr_taos_schemaless_insert_ttl_with_reqid)(taos, lines, numLines, protocol, precision, ttl, reqid);
}

TAOS_RES *taos_schemaless_insert_raw_ttl(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol,
                                         int precision, int32_t ttl) {
  CHECK_PTR_FUNC;
  return (*fptr_taos_schemaless_insert_raw_ttl)(taos, lines, len, totalRows, protocol, precision, ttl);
}

TAOS_RES *taos_schemaless_insert_raw_ttl_with_reqid(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol,
                                                    int precision, int32_t ttl, int64_t reqid) {
  CHECK_PTR_FUNC;
  return (*fptr_taos_schemaless_insert_raw_ttl_with_reqid)(taos, lines, len, totalRows, protocol, precision, ttl,
                                                           reqid);
}

TAOS_RES *taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(TAOS *taos, char *lines, int len, int32_t *totalRows,
                                                               int protocol, int precision, int32_t ttl, int64_t reqid,
                                                               char *tbnameKey) {
  CHECK_PTR_FUNC;
  return (*fptr_taos_schemaless_insert_raw_ttl_with_reqid_tbname_key)(taos, lines, len, totalRows, protocol, precision,
                                                                      ttl, reqid, tbnameKey);
}

TAOS_RES *taos_schemaless_insert_ttl_with_reqid_tbname_key(TAOS *taos, char *lines[], int numLines, int protocol,
                                                           int precision, int32_t ttl, int64_t reqid, char *tbnameKey) {
  CHECK_PTR_FUNC;
  return (*fptr_taos_schemaless_insert_ttl_with_reqid_tbname_key)(taos, lines, numLines, protocol, precision, ttl,
                                                                  reqid, tbnameKey);
}

tmq_conf_t *tmq_conf_new() {
  CHECK_PTR_FUNC;
  return (*fptr_tmq_conf_new)();
}

tmq_conf_res_t tmq_conf_set(tmq_conf_t *conf, const char *key, const char *value) {
  CHECK_INT_FUNC;
  return (*fptr_tmq_conf_set)(conf, key, value);
}

void tmq_conf_destroy(tmq_conf_t *conf) {
  CHECK_VOID_FUNC;
  (*fptr_tmq_conf_destroy)(conf);
}

void tmq_conf_set_auto_commit_cb(tmq_conf_t *conf, tmq_commit_cb *cb, void *param) {
  CHECK_VOID_FUNC;
  (*fptr_tmq_conf_set_auto_commit_cb)(conf, cb, param);
}

tmq_list_t *tmq_list_new() {
  CHECK_VOID_FUNC;
  (*fptr_tmq_list_new)();
}

int32_t tmq_list_append(tmq_list_t *tlist, const char *val) {
  CHECK_INT_FUNC;
  return (*fptr_tmq_list_append)(tlist, val);
}

void tmq_list_destroy(tmq_list_t *tlist) {
  CHECK_VOID_FUNC;
  (*fptr_tmq_list_destroy)(tlist);
}

int32_t tmq_list_get_size(const tmq_list_t *tlist) {
  CHECK_INT_FUNC;
  return (*fptr_tmq_list_get_size)(tlist);
}

char **tmq_list_to_c_array(const tmq_list_t *tlist) {
  CHECK_PTR_FUNC;
  return (*fptr_tmq_list_to_c_array)(tlist);
}

tmq_t *tmq_consumer_new(tmq_conf_t *conf, char *errstr, int32_t errstrLen) {
  CHECK_PTR_FUNC;
  return (*fptr_tmq_consumer_new)(conf, errstr, errstrLen);
}

int32_t tmq_subscribe(tmq_t *tmq, const tmq_list_t *topic_list) {
  CHECK_INT_FUNC;
  return (*fptr_tmq_subscribe)(tmq, topic_list);
}

int32_t tmq_unsubscribe(tmq_t *tmq) {
  CHECK_INT_FUNC;
  return (*fptr_tmq_unsubscribe)(tmq);
}

int32_t tmq_subscription(tmq_t *tmq, tmq_list_t **topics) {
  CHECK_INT_FUNC;
  return (*fptr_tmq_subscription)(tmq, topics);
}

TAOS_RES *tmq_consumer_poll(tmq_t *tmq, int64_t timeout) {
  CHECK_PTR_FUNC;
  return (*fptr_tmq_consumer_poll)(tmq, timeout);
}

int32_t tmq_consumer_close(tmq_t *tmq) {
  CHECK_INT_FUNC;
  return (*fptr_tmq_consumer_close)(tmq);
}

int32_t tmq_commit_sync(tmq_t *tmq, const TAOS_RES *msg) {
  CHECK_INT_FUNC;
  return (*fptr_tmq_commit_sync)(tmq, msg);
}

void tmq_commit_async(tmq_t *tmq, const TAOS_RES *msg, tmq_commit_cb *cb, void *param) {
  CHECK_VOID_FUNC;
  (*fptr_tmq_commit_async)(tmq, msg, cb, param);
}

int32_t tmq_commit_offset_sync(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset) {
  CHECK_INT_FUNC;
  return (*fptr_tmq_commit_offset_sync)(tmq, pTopicName, vgId, offset);
}

void tmq_commit_offset_async(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset, tmq_commit_cb *cb,
                             void *param) {
  CHECK_VOID_FUNC;
  (*fptr_tmq_commit_offset_async)(tmq, pTopicName, vgId, offset, cb, param);
}

int32_t tmq_get_topic_assignment(tmq_t *tmq, const char *pTopicName, tmq_topic_assignment **assignment,
                                 int32_t *numOfAssignment) {
  CHECK_INT_FUNC;
  return (*fptr_tmq_get_topic_assignment)(tmq, pTopicName, assignment, numOfAssignment);
}

void tmq_free_assignment(tmq_topic_assignment *pAssignment) {
  CHECK_VOID_FUNC;
  (*fptr_tmq_free_assignment)(pAssignment);
}

int32_t tmq_offset_seek(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset) {
  CHECK_INT_FUNC;
  return (*fptr_tmq_offset_seek)(tmq pTopicName, vgId, offset);
}

int64_t tmq_position(tmq_t *tmq, const char *pTopicName, int32_t vgId) {
  CHECK_INT_FUNC;
  return (*fptr_tmq_position)(tmq, pTopicName, vgId);
}

int64_t tmq_committed(tmq_t *tmq, const char *pTopicName, int32_t vgId) {
  CHECK_INT_FUNC;
  return (*fptr_tmq_committed)(tmq, pTopicName, vgId);
}

TAOS *tmq_get_connect(tmq_t *tmq) {
  CHECK_PTR_FUNC;
  return (*fptr_tmq_get_connect)(tmq);
}

const char *tmq_get_table_name(TAOS_RES *res) {
  CHECK_PTR_FUNC;
  return (*fptr_tmq_get_table_name)(res);
}

tmq_res_t tmq_get_res_type(TAOS_RES *res) {
  CHECK_INT_FUNC;
  return (*fptr_tmq_get_res_type)(res);
}

const char *tmq_get_topic_name(TAOS_RES *res) {
  CHECK_PTR_FUNC;
  return (*fptr_tmq_get_topic_name)(res);
}

const char *tmq_get_db_name(TAOS_RES *res) {
  CHECK_PTR_FUNC;
  return (*fptr_tmq_get_db_name)(res);
}

int32_t tmq_get_vgroup_id(TAOS_RES *res) {
  CHECK_INT_FUNC;
  return (*fptr_tmq_get_vgroup_id)(res);
}

int64_t tmq_get_vgroup_offset(TAOS_RES *res) {
  CHECK_INT_FUNC;
  return (*fptr_tmq_get_vgroup_offset)(res);
}

const char *tmq_err2str(int32_t code) {
  CHECK_PTR_FUNC;
  return (*fptr_tmq_err2str)(code);
}

int32_t tmq_get_raw(TAOS_RES *res, tmq_raw_data *raw) {
  CHECK_INT_FUNC;
  return (*fptr_tmq_get_raw)(res, raw);
}

int32_t tmq_write_raw(TAOS *taos, tmq_raw_data raw) {
  CHECK_INT_FUNC;
  return (*fptr_tmq_write_raw)(taos, raw);
}

int taos_write_raw_block(TAOS *taos, int numOfRows, char *pData, const char *tbname) {
  CHECK_INT_FUNC;
  return (*fptr_taos_write_raw_block)(taos, numOfRows, pData, tbname);
}

int taos_write_raw_block_with_reqid(TAOS *taos, int numOfRows, char *pData, const char *tbname, int64_t reqid) {
  CHECK_INT_FUNC;
  return (*fptr_taos_write_raw_block_with_reqid)(taos, numOfRows, pData, reqid);
}

int taos_write_raw_block_with_fields(TAOS *taos, int rows, char *pData, const char *tbname, TAOS_FIELD *fields,
                                     int numFields) {
  CHECK_INT_FUNC;
  return (*fptr_taos_write_raw_block_with_fields)(taos, rows, pData, tbname, fields, numFields);
}

int taos_write_raw_block_with_fields_with_reqid(TAOS *taos, int rows, char *pData, const char *tbname,
                                                TAOS_FIELD *fields, int numFields, int64_t reqid) {
  CHECK_INT_FUNC;
  return (*fptr_taos_write_raw_block_with_fields_with_reqid)(taos, rows, pData, tbname, fields, numFields, reqid);
}

void tmq_free_raw(tmq_raw_data raw) {
  CHECK_VOID_FUNC;
  (*fptr_tmq_free_raw)(raw);
}

char *tmq_get_json_meta(TAOS_RES *res) {
  CHECK_PTR_FUNC;
  return (*fptr_tmq_get_json_meta)(res);
}

void tmq_free_json_meta(char *jsonMeta) {
  CHECK_VOID_FUNC;
  return (*fptr_tmq_free_json_meta)(jsonMeta);
}

TSDB_SERVER_STATUS taos_check_server_status(const char *fqdn, int port, char *details, int maxlen) {
  CHECK_INT_FUNC;
  return (*fptr_taos_check_server_status)(fqdn, port, details, maxlen);
}

char *getBuildInfo() {
  CHECK_PTR_FUNC;
  return (*fptr_getBuildInfo)();
}
