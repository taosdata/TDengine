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

#ifndef TDENGINE_WRAPPER_H
#define TDENGINE_WRAPPER_H

#include "os.h"
#include "taos.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  DRIVER_NATIVE = 0,
  DRIVER_WEBSOCKET = 1,
  DRIVER_MAX = 2,
} EDriverType;

extern EDriverType tsDriverType;
extern void       *tsDriver;

extern int32_t taosDriverInit(EDriverType driverType);
extern void    taosDriverCleanup();

extern setConfRet (*fp_taos_set_config)(const char *config);

extern int (*fp_taos_init)(void);
extern void (*fp_taos_cleanup)(void);
extern int (*fp_taos_options)(TSDB_OPTION option, const void *arg, ...);
extern int (*fp_taos_options_connection)(TAOS *taos, TSDB_OPTION_CONNECTION option, const void *arg, ...);
extern TAOS *(*fp_taos_connect)(const char *ip, const char *user, const char *pass, const char *db, uint16_t port);
extern TAOS *(*fp_taos_connect_auth)(const char *ip, const char *user, const char *auth, const char *db, uint16_t port);
extern void (*fp_taos_close)(TAOS *taos);

extern const char *(*fp_taos_data_type)(int type);

extern TAOS_STMT *(*fp_taos_stmt_init)(TAOS *taos);
extern TAOS_STMT *(*fp_taos_stmt_init_with_reqid)(TAOS *taos, int64_t reqid);
extern TAOS_STMT *(*fp_taos_stmt_init_with_options)(TAOS *taos, TAOS_STMT_OPTIONS *options);
extern int (*fp_taos_stmt_prepare)(TAOS_STMT *stmt, const char *sql, unsigned long length);
extern int (*fp_taos_stmt_set_tbname_tags)(TAOS_STMT *stmt, const char *name, TAOS_MULTI_BIND *tags);
extern int (*fp_taos_stmt_set_tbname)(TAOS_STMT *stmt, const char *name);
extern int (*fp_taos_stmt_set_tags)(TAOS_STMT *stmt, TAOS_MULTI_BIND *tags);
extern int (*fp_taos_stmt_set_sub_tbname)(TAOS_STMT *stmt, const char *name);
extern int (*fp_taos_stmt_get_tag_fields)(TAOS_STMT *stmt, int *fieldNum, TAOS_FIELD_E **fields);
extern int (*fp_taos_stmt_get_col_fields)(TAOS_STMT *stmt, int *fieldNum, TAOS_FIELD_E **fields);
extern void (*fp_taos_stmt_reclaim_fields)(TAOS_STMT *stmt, TAOS_FIELD_E *fields);

extern int (*fp_taos_stmt_is_insert)(TAOS_STMT *stmt, int *insert);
extern int (*fp_taos_stmt_num_params)(TAOS_STMT *stmt, int *nums);
extern int (*fp_taos_stmt_get_param)(TAOS_STMT *stmt, int idx, int *type, int *bytes);
extern int (*fp_taos_stmt_bind_param)(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind);
extern int (*fp_taos_stmt_bind_param_batch)(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind);
extern int (*fp_taos_stmt_bind_single_param_batch)(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind, int colIdx);
extern int (*fp_taos_stmt_add_batch)(TAOS_STMT *stmt);
extern int (*fp_taos_stmt_execute)(TAOS_STMT *stmt);
extern TAOS_RES *(*fp_taos_stmt_use_result)(TAOS_STMT *stmt);
extern int (*fp_taos_stmt_close)(TAOS_STMT *stmt);
extern char *(*fp_taos_stmt_errstr)(TAOS_STMT *stmt);
extern int (*fp_taos_stmt_affected_rows)(TAOS_STMT *stmt);
extern int (*fp_taos_stmt_affected_rows_once)(TAOS_STMT *stmt);

extern TAOS_STMT2 *(*fp_taos_stmt2_init)(TAOS *taos, TAOS_STMT2_OPTION *option);
extern int (*fp_taos_stmt2_prepare)(TAOS_STMT2 *stmt, const char *sql, unsigned long length);
extern int (*fp_taos_stmt2_bind_param)(TAOS_STMT2 *stmt, TAOS_STMT2_BINDV *bindv, int32_t col_idx);
extern int (*fp_taos_stmt2_bind_param_a)(TAOS_STMT2 *stmt, TAOS_STMT2_BINDV *bindv, int32_t col_idx,
                                         __taos_async_fn_t fp, void *param);
extern int (*fp_taos_stmt2_exec)(TAOS_STMT2 *stmt, int *affected_rows);
extern int (*fp_taos_stmt2_close)(TAOS_STMT2 *stmt);
extern int (*fp_taos_stmt2_is_insert)(TAOS_STMT2 *stmt, int *insert);
extern int (*fp_taos_stmt2_get_fields)(TAOS_STMT2 *stmt, int *count, TAOS_FIELD_ALL **fields);
extern void (*fp_taos_stmt2_free_fields)(TAOS_STMT2 *stmt, TAOS_FIELD_ALL *fields);
extern TAOS_RES *(*fp_taos_stmt2_result)(TAOS_STMT2 *stmt);
extern char *(*fp_taos_stmt2_error)(TAOS_STMT2 *stmt);

extern TAOS_RES *(*fp_taos_query)(TAOS *taos, const char *sql);
extern TAOS_RES *(*fp_taos_query_with_reqid)(TAOS *taos, const char *sql, int64_t reqId);

extern TAOS_ROW (*fp_taos_fetch_row)(TAOS_RES *res);
extern int (*fp_taos_result_precision)(TAOS_RES *res);  // get the time precision of result
extern void (*fp_taos_free_result)(TAOS_RES *res);
extern void (*fp_taos_kill_query)(TAOS *taos);
extern int (*fp_taos_field_count)(TAOS_RES *res);
extern int (*fp_taos_num_fields)(TAOS_RES *res);
extern int (*fp_taos_affected_rows)(TAOS_RES *res);
extern int64_t (*fp_taos_affected_rows64)(TAOS_RES *res);

extern TAOS_FIELD *(*fp_taos_fetch_fields)(TAOS_RES *res);
extern TAOS_FIELD_E *(*fp_taos_fetch_fields_e)(TAOS_RES *res);
extern int (*fp_taos_select_db)(TAOS *taos, const char *db);
extern int (*fp_taos_print_row)(char *str, TAOS_ROW row, TAOS_FIELD *fields, int num_fields);
extern int (*fp_taos_print_row_with_size)(char *str, uint32_t size, TAOS_ROW row, TAOS_FIELD *fields, int num_fields);
extern void (*fp_taos_stop_query)(TAOS_RES *res);
extern bool (*fp_taos_is_null)(TAOS_RES *res, int32_t row, int32_t col);
extern int (*fp_taos_is_null_by_column)(TAOS_RES *res, int columnIndex, bool result[], int *rows);
extern bool (*fp_taos_is_update_query)(TAOS_RES *res);
extern int (*fp_taos_fetch_block)(TAOS_RES *res, TAOS_ROW *rows);
extern int (*fp_taos_fetch_block_s)(TAOS_RES *res, int *numOfRows, TAOS_ROW *rows);
extern int (*fp_taos_fetch_raw_block)(TAOS_RES *res, int *numOfRows, void **pData);
extern int *(*fp_taos_get_column_data_offset)(TAOS_RES *res, int columnIndex);
extern int (*fp_taos_validate_sql)(TAOS *taos, const char *sql);
extern void (*fp_taos_reset_current_db)(TAOS *taos);

extern int *(*fp_taos_fetch_lengths)(TAOS_RES *res);
extern TAOS_ROW *(*fp_taos_result_block)(TAOS_RES *res);

extern const char *(*fp_taos_get_server_info)(TAOS *taos);
extern const char *(*fp_taos_get_client_info)();
extern int (*fp_taos_get_current_db)(TAOS *taos, char *database, int len, int *required);

extern const char *(*fp_taos_errstr)(TAOS_RES *res);
extern int (*fp_taos_errno)(TAOS_RES *res);

extern void (*fp_taos_query_a)(TAOS *taos, const char *sql, __taos_async_fn_t fp, void *param);
extern void (*fp_taos_query_a_with_reqid)(TAOS *taos, const char *sql, __taos_async_fn_t fp, void *param,
                                          int64_t reqid);
extern void (*fp_taos_fetch_rows_a)(TAOS_RES *res, __taos_async_fn_t fp, void *param);
extern void (*fp_taos_fetch_raw_block_a)(TAOS_RES *res, __taos_async_fn_t fp, void *param);
extern const void *(*fp_taos_get_raw_block)(TAOS_RES *res);

extern int (*fp_taos_get_db_route_info)(TAOS *taos, const char *db, TAOS_DB_ROUTE_INFO *dbInfo);
extern int (*fp_taos_get_table_vgId)(TAOS *taos, const char *db, const char *table, int *vgId);
extern int (*fp_taos_get_tables_vgId)(TAOS *taos, const char *db, const char *table[], int tableNum, int *vgId);

extern int (*fp_taos_load_table_info)(TAOS *taos, const char *tableNameList);

extern void (*fp_taos_set_hb_quit)(int8_t quitByKill);

extern int (*fp_taos_set_notify_cb)(TAOS *taos, __taos_notify_fn_t fp, void *param, int type);

extern void (*fp_taos_fetch_whitelist_a)(TAOS *taos, __taos_async_whitelist_fn_t fp, void *param);

extern void (*fp_taos_fetch_whitelist_dual_stack_a)(TAOS *taos, __taos_async_whitelist_dual_stack_fn_t fp, void *param);

extern int (*fp_taos_set_conn_mode)(TAOS *taos, int mode, int value);

extern TAOS_RES *(*fp_taos_schemaless_insert)(TAOS *taos, char *lines[], int numLines, int protocol, int precision);
extern TAOS_RES *(*fp_taos_schemaless_insert_with_reqid)(TAOS *taos, char *lines[], int numLines, int protocol,
                                                         int precision, int64_t reqid);
extern TAOS_RES *(*fp_taos_schemaless_insert_raw)(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol,
                                                  int precision);
extern TAOS_RES *(*fp_taos_schemaless_insert_raw_with_reqid)(TAOS *taos, char *lines, int len, int32_t *totalRows,
                                                             int protocol, int precision, int64_t reqid);
extern TAOS_RES *(*fp_taos_schemaless_insert_ttl)(TAOS *taos, char *lines[], int numLines, int protocol, int precision,
                                                  int32_t ttl);
extern TAOS_RES *(*fp_taos_schemaless_insert_ttl_with_reqid)(TAOS *taos, char *lines[], int numLines, int protocol,
                                                             int precision, int32_t ttl, int64_t reqid);
extern TAOS_RES *(*fp_taos_schemaless_insert_raw_ttl)(TAOS *taos, char *lines, int len, int32_t *totalRows,
                                                      int protocol, int precision, int32_t ttl);
extern TAOS_RES *(*fp_taos_schemaless_insert_raw_ttl_with_reqid)(TAOS *taos, char *lines, int len, int32_t *totalRows,
                                                                 int protocol, int precision, int32_t ttl,
                                                                 int64_t reqid);
extern TAOS_RES *(*fp_taos_schemaless_insert_raw_ttl_with_reqid_tbname_key)(TAOS *taos, char *lines, int len,
                                                                            int32_t *totalRows, int protocol,
                                                                            int precision, int32_t ttl, int64_t reqid,
                                                                            char *tbnameKey);
extern TAOS_RES *(*fp_taos_schemaless_insert_ttl_with_reqid_tbname_key)(TAOS *taos, char *lines[], int numLines,
                                                                        int protocol, int precision, int32_t ttl,
                                                                        int64_t reqid, char *tbnameKey);

extern tmq_conf_t *(*fp_tmq_conf_new)();
extern tmq_conf_res_t (*fp_tmq_conf_set)(tmq_conf_t *conf, const char *key, const char *value);
extern void (*fp_tmq_conf_destroy)(tmq_conf_t *conf);
extern void (*fp_tmq_conf_set_auto_commit_cb)(tmq_conf_t *conf, tmq_commit_cb *cb, void *param);

extern tmq_list_t *(*fp_tmq_list_new)();
extern int32_t (*fp_tmq_list_append)(tmq_list_t *, const char *);
extern void (*fp_tmq_list_destroy)(tmq_list_t *);
extern int32_t (*fp_tmq_list_get_size)(const tmq_list_t *);
extern char **(*fp_tmq_list_to_c_array)(const tmq_list_t *);

extern tmq_t *(*fp_tmq_consumer_new)(tmq_conf_t *conf, char *errstr, int32_t errstrLen);
extern int32_t (*fp_tmq_subscribe)(tmq_t *tmq, const tmq_list_t *topic_list);
extern int32_t (*fp_tmq_unsubscribe)(tmq_t *tmq);
extern int32_t (*fp_tmq_subscription)(tmq_t *tmq, tmq_list_t **topics);
extern TAOS_RES *(*fp_tmq_consumer_poll)(tmq_t *tmq, int64_t timeout);
extern int32_t (*fp_tmq_consumer_close)(tmq_t *tmq);
extern int32_t (*fp_tmq_commit_sync)(tmq_t *tmq, const TAOS_RES *msg);
extern void (*fp_tmq_commit_async)(tmq_t *tmq, const TAOS_RES *msg, tmq_commit_cb *cb, void *param);
extern int32_t (*fp_tmq_commit_offset_sync)(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset);
extern void (*fp_tmq_commit_offset_async)(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset,
                                          tmq_commit_cb *cb, void *param);
extern int32_t (*fp_tmq_get_topic_assignment)(tmq_t *tmq, const char *pTopicName, tmq_topic_assignment **assignment,
                                              int32_t *numOfAssignment);
extern void (*fp_tmq_free_assignment)(tmq_topic_assignment *pAssignment);
extern int32_t (*fp_tmq_offset_seek)(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset);
extern int64_t (*fp_tmq_position)(tmq_t *tmq, const char *pTopicName, int32_t vgId);
extern int64_t (*fp_tmq_committed)(tmq_t *tmq, const char *pTopicName, int32_t vgId);

extern TAOS *(*fp_tmq_get_connect)(tmq_t *tmq);
extern const char *(*fp_tmq_get_table_name)(TAOS_RES *res);
extern tmq_res_t (*fp_tmq_get_res_type)(TAOS_RES *res);
extern const char *(*fp_tmq_get_topic_name)(TAOS_RES *res);
extern const char *(*fp_tmq_get_db_name)(TAOS_RES *res);
extern int32_t (*fp_tmq_get_vgroup_id)(TAOS_RES *res);
extern int64_t (*fp_tmq_get_vgroup_offset)(TAOS_RES *res);
extern const char *(*fp_tmq_err2str)(int32_t code);

extern int32_t (*fp_tmq_get_raw)(TAOS_RES *res, tmq_raw_data *raw);
extern int32_t (*fp_tmq_write_raw)(TAOS *taos, tmq_raw_data raw);
extern int (*fp_taos_write_raw_block)(TAOS *taos, int numOfRows, char *pData, const char *tbname);
extern int (*fp_taos_write_raw_block_with_reqid)(TAOS *taos, int numOfRows, char *pData, const char *tbname,
                                                 int64_t reqid);
extern int (*fp_taos_write_raw_block_with_fields)(TAOS *taos, int rows, char *pData, const char *tbname,
                                                  TAOS_FIELD *fields, int numFields);
extern int (*fp_taos_write_raw_block_with_fields_with_reqid)(TAOS *taos, int rows, char *pData, const char *tbname,
                                                             TAOS_FIELD *fields, int numFields, int64_t reqid);
extern void (*fp_tmq_free_raw)(tmq_raw_data raw);

extern char *(*fp_tmq_get_json_meta)(TAOS_RES *res);
extern void (*fp_tmq_free_json_meta)(char *jsonMeta);

extern TSDB_SERVER_STATUS (*fp_taos_check_server_status)(const char *fqdn, int port, char *details, int maxlen);
extern void (*fp_taos_write_crashinfo)(int signum, void *sigInfo, void *context);
extern char *(*fp_getBuildInfo)();

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_CLIENT_WRAPPER_H
