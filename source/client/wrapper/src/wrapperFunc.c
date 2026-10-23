/*
 * Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

/* Last include on purpose: re-points the memory macros at the local libc shims. */
#include "wrapperLiteMem.h"

static TdThreadOnce tsDriverEnvOnce = PTHREAD_ONCE_INIT;
static TdThreadOnce tsDriverOnce = PTHREAD_ONCE_INIT;
volatile int32_t    tsDriverOnceRet = 0;

static TdThreadOnce tsInitOnce = PTHREAD_ONCE_INIT;
volatile int32_t    tsInitOnceRet = 0;

static TdThreadOnce tsReportOnce = PTHREAD_ONCE_INIT;

#define ERR_VOID(code) \
  terrno = code;       \
  return;

#define ERR_PTR(code) \
  terrno = code;      \
  return NULL;

#define ERR_INT(code) \
  terrno = code;      \
  return -1;

#define ERR_BOOL(code) \
  terrno = code;       \
  return false;

#define ERR_CONFRET(code)           \
  terrno = code;                    \
  setConfRet ret = {.retCode = -1}; \
  return ret;

/* There is no CHECK_* here on purpose.  Every slot starts out holding a
 * fallback (see wrapperVariable.c) and taosDriverInit() only ever overwrites a
 * slot whose lookup succeeded, so an entry point the driver does not provide
 * answers through that fallback -- with the same error code these macros used
 * to return.  Checking on the way in would cost a load and a branch per call
 * for an answer that is fixed at init time. */

setConfRet taos_set_config(const char *config) {
  if (taos_init() != 0) {
    ERR_CONFRET(TSDB_CODE_DLL_NOT_LOAD)
  }

  return (*fp_taos_set_config)(config);
}

static void taos_init_driver_env(void) {
  /* Reads taos.cfg (logDir, jniDebugFlag) and opens the wrapper's own log file
   * before the driver is loaded, so driver-loading diagnostics land in it. */
  tdLiteLogInit();
  taosDriverEnvInit();
}

static void taos_init_driver(void) {
  tsDriverOnceRet = taosDriverInit(tsDriverType);
  if (tsDriverOnceRet != 0) return;

  tsDriverOnceRet = 0;
}

static void taos_init_wrapper(void) {
  /* fp_taos_init holds its fallback when the driver has no such entry point,
   * and that fallback reports the same code the check here used to. */
  tsInitOnceRet = (*fp_taos_init)();
}

/* "The driver does not provide N of M entry points" is a diagnostic for the
 * log, not a line for the caller's terminal, and the log only exists once the
 * driver's own taos_init() has run -- so report it here rather than from
 * taosDriverInit(), which runs before the log is created. */
static void taos_init_report_missing(void) {
  if (tsDriverOnceRet == 0) {
    taosDriverReportMissing();
  }
}

int taos_init(void) {
  (void)taosThreadOnce(&tsDriverEnvOnce, taos_init_driver_env);
  (void)taosThreadOnce(&tsDriverOnce, taos_init_driver);
  (void)taosThreadOnce(&tsInitOnce, taos_init_wrapper);
  (void)taosThreadOnce(&tsReportOnce, taos_init_report_missing);
  return tsInitOnceRet;
}

void taos_cleanup(void) {
  (*fp_taos_cleanup)();
  tdLiteLogClose();
}

int taos_options(TSDB_OPTION option, const void *arg, ...) {
  /* The JNI layer writes the directory it was given straight into the wrapper's own
   * configDir (Java_com_taosdata_jdbc_TSDBJNIConnector_initImp does the same copy).
   * A caller that uses the public option -- the Python/Go/C connectors do -- has to
   * land in the same place, because the wrapper reads its logDir and jniDebugFlag out
   * of <configDir>/taos.cfg, and before the thin dispatcher that directory was the
   * engine's single global.  It has to happen before the once below opens the log and
   * before the driver is loaded, which is when configDir is handed over. */
  if (option == TSDB_OPTION_CONFIGDIR) {
    if (arg == NULL) {
      /* The engine's public taos_options() answers TSDB_CODE_INVALID_PARA for a NULL
       * value (taos_options_imp() would strlen(NULL)); this dispatcher reports failure
       * as -1 + terrno everywhere else, so it does that here too.  Forwarding the NULL
       * to the driver instead would make the two entry points disagree. */
      terrno = TSDB_CODE_INVALID_PARA;
      return -1;
    }
    /* tdLiteSetConfigDir() is the engine's own rule for this option
     * (clientEnv.c's taos_options_imp: a path that needs quoting is stored quoted,
     * because the engine parses the value with wordexp()) in the buffer that
     * wrapperDriver.c hands to the engine when the driver is loaded.  Copying the raw
     * string here -- what this path used to do -- made a directory with a space
     * behave differently depending on whether it was set before or after
     * taos_init(): it is the value the engine receives that gets parsed. */
    int32_t code = tdLiteSetConfigDir((const char *)arg);
    if (code == 0 && tsDriver != NULL) {
      /* Keep a loaded driver in step.  The caller's own string is passed on, not the
       * canonical one -- the WebSocket driver takes it as a plain path and the engine
       * normalizes it itself -- and its answer wins, since it is what will be in use. */
      int32_t drvCode = (*fp_taos_options)(option, arg);
      if (drvCode != 0) {
        return drvCode;
      }
    }
    return code;
  }

  (void)taosThreadOnce(&tsDriverEnvOnce, taos_init_driver_env);
  if (option == TSDB_OPTION_DRIVER) {
    if (tsDriver == NULL) {
      if (strcasecmp((const char *)arg, STR_NATIVE) == 0) {
        tsDriverType = DRIVER_NATIVE;
        return 0;
      }
      if (strcasecmp((const char *)arg, STR_WEBSOCKET) == 0) {
        tsDriverType = DRIVER_WEBSOCKET;
        return 0;
      }
    }
    terrno = TSDB_CODE_REPEAT_INIT;
    return -1;
  }
  (void)taosThreadOnce(&tsDriverOnce, taos_init_driver);

  int code = (*fp_taos_options)(option, arg);
  if (code == 0 && option == TSDB_OPTION_CHARSET && arg != NULL) {
    /* The engine's tsCharset is not reachable from here (it exports no data symbol on
     * Windows, and the WebSocket driver has none), so record what it accepted: this is
     * the value getTsCharset() has to report, and the only place the wrapper sees it. */
    tdLiteSetCharset((const char *)arg);
  }
  return code;
}

int taos_options_connection(TAOS *taos, TSDB_OPTION_CONNECTION option, const void *arg, ...) {
  return (*fp_taos_options_connection)(taos, option, (const char *)arg);
}

TAOS *taos_connect(const char *ip, const char *user, const char *pass, const char *db, uint16_t port) {
  if (taos_init() != 0) {
    //terrno = TSDB_CODE_DLL_NOT_LOAD;
    return NULL;
  }

  return (*fp_taos_connect)(ip, user, pass, db, port);
}

TAOS *taos_connect_totp(const char *ip, const char *user, const char *pass, const char* totp, const char *db, uint16_t port) {
  if (taos_init() != 0) {
    terrno = TSDB_CODE_DLL_NOT_LOAD;
    return NULL;
  }

  return (*fp_taos_connect_totp)(ip, user, pass, totp, db, port);
}

int taos_connect_test(const char *ip, const char *user, const char *pass, const char* totp, const char *db, uint16_t port) {
  if (taos_init() != 0) {
    return TSDB_CODE_DLL_NOT_LOAD;
  }
  return (*fp_taos_connect_test)(ip, user, pass, totp, db, port);
}

TAOS *taos_connect_token(const char *ip, const char *token, const char *db, uint16_t port) {
  if (taos_init() != 0) {
    terrno = TSDB_CODE_DLL_NOT_LOAD;
    return NULL;
  }

  return (*fp_taos_connect_token)(ip, token, db, port);
}

TAOS *taos_connect_auth(const char *ip, const char *user, const char *auth, const char *db, uint16_t port) {
  if (taos_init() != 0) {
    terrno = TSDB_CODE_DLL_NOT_LOAD;
    return NULL;
  }

  return (*fp_taos_connect_auth)(ip, user, auth, db, port);
}

void taos_set_option(OPTIONS *options, const char *key, const char *value) {
  if (taos_init() != 0) {
    return;
  }

  (*fp_taos_set_option)(options, key, value);
}

TAOS *taos_connect_with(const OPTIONS *options) {
  if (taos_init() != 0) {
    return NULL;
  }

  return (*fp_taos_connect_with)(options);
}

TAOS *taos_connect_with_dsn(const char *dsn) {
  if (taos_init() != 0) {
    return NULL;
  }

  return (*fp_taos_connect_with_dsn)(dsn);
}

void taos_close(TAOS *taos) {
  (*fp_taos_close)(taos);
}

int taos_txn_begin(TAOS *taos) {
  return (*fp_taos_txn_begin)(taos);
}

int taos_txn_commit(TAOS *taos) {
  return (*fp_taos_txn_commit)(taos);
}

int taos_txn_rollback(TAOS *taos) {
  return (*fp_taos_txn_rollback)(taos);
}

const char *taos_data_type(int type) {
  (void)taos_init();
  return (*fp_taos_data_type)(type);
}

TAOS_STMT *taos_stmt_init(TAOS *taos) {
  return (*fp_taos_stmt_init)(taos);
}

TAOS_STMT *taos_stmt_init_with_reqid(TAOS *taos, int64_t reqid) {
  return (*fp_taos_stmt_init_with_reqid)(taos, reqid);
}

TAOS_STMT *taos_stmt_init_with_options(TAOS *taos, TAOS_STMT_OPTIONS *options) {
  return (*fp_taos_stmt_init_with_options)(taos, options);
}

int taos_stmt_prepare(TAOS_STMT *stmt, const char *sql, unsigned long length) {
  return (*fp_taos_stmt_prepare)(stmt, sql, length);
}

int taos_stmt_set_tbname_tags(TAOS_STMT *stmt, const char *name, TAOS_MULTI_BIND *tags) {
  return (*fp_taos_stmt_set_tbname_tags)(stmt, name, tags);
}

int taos_stmt_set_tbname(TAOS_STMT *stmt, const char *name) {
  return (*fp_taos_stmt_set_tbname)(stmt, name);
}

int taos_stmt_set_tags(TAOS_STMT *stmt, TAOS_MULTI_BIND *tags) {
  return (*fp_taos_stmt_set_tags)(stmt, tags);
}

int taos_stmt_set_sub_tbname(TAOS_STMT *stmt, const char *name) {
  return (*fp_taos_stmt_set_sub_tbname)(stmt, name);
}

int taos_stmt_get_tag_fields(TAOS_STMT *stmt, int *fieldNum, TAOS_FIELD_E **fields) {
  return (*fp_taos_stmt_get_tag_fields)(stmt, fieldNum, fields);
}

int taos_stmt_get_col_fields(TAOS_STMT *stmt, int *fieldNum, TAOS_FIELD_E **fields) {
  return (*fp_taos_stmt_get_col_fields)(stmt, fieldNum, fields);
}

void taos_stmt_reclaim_fields(TAOS_STMT *stmt, TAOS_FIELD_E *fields) {
  (*fp_taos_stmt_reclaim_fields)(stmt, fields);
}

int taos_stmt_is_insert(TAOS_STMT *stmt, int *insert) {
  return (*fp_taos_stmt_is_insert)(stmt, insert);
}

int taos_stmt_num_params(TAOS_STMT *stmt, int *nums) {
  return (*fp_taos_stmt_num_params)(stmt, nums);
}

int taos_stmt_get_param(TAOS_STMT *stmt, int idx, int *type, int *bytes) {
  return (*fp_taos_stmt_get_param)(stmt, idx, type, bytes);
}

int taos_stmt_bind_param(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind) {
  return (*fp_taos_stmt_bind_param)(stmt, bind);
}

int taos_stmt_bind_param_batch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind) {
  return (*fp_taos_stmt_bind_param_batch)(stmt, bind);
}

int taos_stmt_bind_single_param_batch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind, int colIdx) {
  return (*fp_taos_stmt_bind_single_param_batch)(stmt, bind, colIdx);
}

int taos_stmt_add_batch(TAOS_STMT *stmt) {
  return (*fp_taos_stmt_add_batch)(stmt);
}

int taos_stmt_execute(TAOS_STMT *stmt) {
  return (*fp_taos_stmt_execute)(stmt);
}

TAOS_RES *taos_stmt_use_result(TAOS_STMT *stmt) {
  return (*fp_taos_stmt_use_result)(stmt);
}

int taos_stmt_close(TAOS_STMT *stmt) {
  return (*fp_taos_stmt_close)(stmt);
}

char *taos_stmt_errstr(TAOS_STMT *stmt) {
  return (*fp_taos_stmt_errstr)(stmt);
}

int taos_stmt_affected_rows(TAOS_STMT *stmt) {
  return (*fp_taos_stmt_affected_rows)(stmt);
}

int taos_stmt_affected_rows_once(TAOS_STMT *stmt) {
  return (*fp_taos_stmt_affected_rows_once)(stmt);
}

TAOS_STMT2 *taos_stmt2_init(TAOS *taos, TAOS_STMT2_OPTION *option) {
  return (*fp_taos_stmt2_init)(taos, option);
}

int taos_stmt2_prepare(TAOS_STMT2 *stmt, const char *sql, unsigned long length) {
  return (*fp_taos_stmt2_prepare)(stmt, sql, length);
}

int taos_stmt2_bind_param(TAOS_STMT2 *stmt, TAOS_STMT2_BINDV *bindv, int32_t col_idx) {
  return (*fp_taos_stmt2_bind_param)(stmt, bindv, col_idx);
}

int taos_stmt2_bind_param_a(TAOS_STMT2 *stmt, TAOS_STMT2_BINDV *bindv, int32_t col_idx, __taos_async_fn_t fp,
                            void *param) {
  return (*fp_taos_stmt2_bind_param_a)(stmt, bindv, col_idx, fp, param);
}

int taos_stmt2_bind_param_column(TAOS_STMT2 *stmt, TAOS_STMT2_COLUMN_BINDV *bindv) {
  return (*fp_taos_stmt2_bind_param_column)(stmt, bindv);
}

int taos_stmt2_bind_param_column_a(TAOS_STMT2 *stmt, TAOS_STMT2_COLUMN_BINDV *bindv, __taos_async_fn_t fp,
                                   void *param) {
  return (*fp_taos_stmt2_bind_param_column_a)(stmt, bindv, fp, param);
}

int taos_stmt2_exec(TAOS_STMT2 *stmt, int *affected_rows) {
  return (*fp_taos_stmt2_exec)(stmt, affected_rows);
}

int taos_stmt2_close(TAOS_STMT2 *stmt) {
  return (*fp_taos_stmt2_close)(stmt);
}

int taos_stmt2_is_insert(TAOS_STMT2 *stmt, int *insert) {
  return (*fp_taos_stmt2_is_insert)(stmt, insert);
}

int taos_stmt2_get_fields(TAOS_STMT2 *stmt, int *count, TAOS_FIELD_ALL **fields) {
  return (*fp_taos_stmt2_get_fields)(stmt, count, fields);
}

void taos_stmt2_free_fields(TAOS_STMT2 *stmt, TAOS_FIELD_ALL *fields) {
  (*fp_taos_stmt2_free_fields)(stmt, fields);
}

TAOS_RES *taos_stmt2_result(TAOS_STMT2 *stmt) {
  return (*fp_taos_stmt2_result)(stmt);
}

char *taos_stmt2_error(TAOS_STMT2 *stmt) {
  return (*fp_taos_stmt2_error)(stmt);
}

TAOS_RES *taos_query(TAOS *taos, const char *sql) {
  return (*fp_taos_query)(taos, sql);
}

TAOS_RES *taos_query_with_reqid(TAOS *taos, const char *sql, int64_t reqId) {
  return (*fp_taos_query_with_reqid)(taos, sql, reqId);
}

TAOS_ROW taos_fetch_row(TAOS_RES *res) {
  return (*fp_taos_fetch_row)(res);
}

int taos_result_precision(TAOS_RES *res) {
  return (*fp_taos_result_precision)(res);
}

void taos_free_result(TAOS_RES *res) {
  return (*fp_taos_free_result)(res);
}

void taos_kill_query(TAOS *taos) {
  return (*fp_taos_kill_query)(taos);
}

int taos_field_count(TAOS_RES *res) {
  return (*fp_taos_field_count)(res);
}

int taos_num_fields(TAOS_RES *res) {
  return (*fp_taos_num_fields)(res);
}

int taos_affected_rows(TAOS_RES *res) {
  return (*fp_taos_affected_rows)(res);
}

int64_t taos_affected_rows64(TAOS_RES *res) {
  return (*fp_taos_affected_rows64)(res);
}

TAOS_FIELD *taos_fetch_fields(TAOS_RES *res) {
  return (*fp_taos_fetch_fields)(res);
}

TAOS_FIELD_E *taos_fetch_fields_e(TAOS_RES *res) {
  return (*fp_taos_fetch_fields_e)(res);
}

int taos_select_db(TAOS *taos, const char *db) {
  return (*fp_taos_select_db)(taos, db);
}

int taos_print_row(char *str, TAOS_ROW row, TAOS_FIELD *fields, int num_fields) {
  return (*fp_taos_print_row)(str, row, fields, num_fields);
}

int taos_print_row_with_size(char *str, uint32_t size, TAOS_ROW row, TAOS_FIELD *fields, int num_fields) {
  return (*fp_taos_print_row_with_size)(str, size, row, fields, num_fields);
}

void taos_stop_query(TAOS_RES *res) {
  (*fp_taos_stop_query)(res);
}

bool taos_is_null(TAOS_RES *res, int32_t row, int32_t col) {
  return (*fp_taos_is_null)(res, row, col);
}

int taos_is_null_by_column(TAOS_RES *res, int columnIndex, bool result[], int *rows) {
  return (*fp_taos_is_null_by_column)(res, columnIndex, result, rows);
}

bool taos_is_update_query(TAOS_RES *res) {
  return (*fp_taos_is_update_query)(res);
}

int taos_fetch_block(TAOS_RES *res, TAOS_ROW *rows) {
  return (*fp_taos_fetch_block)(res, rows);
}

int taos_fetch_block_s(TAOS_RES *res, int *numOfRows, TAOS_ROW *rows) {
  return (*fp_taos_fetch_block_s)(res, numOfRows, rows);
}

int taos_fetch_raw_block(TAOS_RES *res, int *numOfRows, void **pData) {
  return (*fp_taos_fetch_raw_block)(res, numOfRows, pData);
}

int *taos_get_column_data_offset(TAOS_RES *res, int columnIndex) {
  return (*fp_taos_get_column_data_offset)(res, columnIndex);
}

int taos_validate_sql(TAOS *taos, const char *sql) {
  return (*fp_taos_validate_sql)(taos, sql);
}

void taos_reset_current_db(TAOS *taos) {
  (*fp_taos_reset_current_db)(taos);
}

int *taos_fetch_lengths(TAOS_RES *res) {
  return (*fp_taos_fetch_lengths)(res);
}

TAOS_ROW *taos_result_block(TAOS_RES *res) {
  return (*fp_taos_result_block)(res);
}

const char *taos_get_server_info(TAOS *taos) {
  return (*fp_taos_get_server_info)(taos);
}

const char *taos_get_client_info() {
  return (*fp_taos_get_client_info)();
}

int taos_get_current_db(TAOS *taos, char *database, int len, int *required) {
  return (*fp_taos_get_current_db)(taos, database, len, required);
}

int taos_get_connection_info(TAOS *taos, TSDB_CONNECTION_INFO info, char *buffer, int* len) {
  return (*fp_taos_get_connection_info)(taos, info, buffer, len);
}

void *taos_get_result_tz(TAOS_RES *res) {
  return (*fp_taos_get_result_tz)(res);
}

const char *taos_errstr(TAOS_RES *res) {
  (void)taos_init();
  return (*fp_taos_errstr)(res);
}

int taos_errno(TAOS_RES *res) {
  (void)taos_init();
  return (*fp_taos_errno)(res);
}

void taos_query_a(TAOS *taos, const char *sql, __taos_async_fn_t fp, void *param) {
  (*fp_taos_query_a)(taos, sql, fp, param);
}

void taos_query_a_with_reqid(TAOS *taos, const char *sql, __taos_async_fn_t fp, void *param, int64_t reqid) {
  (*fp_taos_query_a_with_reqid)(taos, sql, fp, param, reqid);
}

void taos_fetch_rows_a(TAOS_RES *res, __taos_async_fn_t fp, void *param) {
  (*fp_taos_fetch_rows_a)(res, fp, param);
}

void taos_fetch_raw_block_a(TAOS_RES *res, __taos_async_fn_t fp, void *param) {
  (*fp_taos_fetch_raw_block_a)(res, fp, param);
}

const void *taos_get_raw_block(TAOS_RES *res) {
  return (*fp_taos_get_raw_block)(res);
}

int taos_get_db_route_info(TAOS *taos, const char *db, TAOS_DB_ROUTE_INFO *dbInfo) {
  return (*fp_taos_get_db_route_info)(taos, db, dbInfo);
}

int taos_get_table_vgId(TAOS *taos, const char *db, const char *table, int *vgId) {
  return (*fp_taos_get_table_vgId)(taos, db, table, vgId);
}

int taos_get_tables_vgId(TAOS *taos, const char *db, const char *table[], int tableNum, int *vgId) {
  return (*fp_taos_get_tables_vgId)(taos, db, table, tableNum, vgId);
}

int taos_load_table_info(TAOS *taos, const char *tableNameList) {
  return (*fp_taos_load_table_info)(taos, tableNameList);
}

void taos_set_hb_quit(int8_t quitByKill) {
  if (taos_init() != 0) {
    return;
  }

  return (*fp_taos_set_hb_quit)(quitByKill);
}

int taos_set_notify_cb(TAOS *taos, __taos_notify_fn_t fp, void *param, int type) {
  return (*fp_taos_set_notify_cb)(taos, fp, param, type);
}

void taos_fetch_whitelist_a(TAOS *taos, __taos_async_whitelist_fn_t fp, void *param) {
  return (*fp_taos_fetch_whitelist_a)(taos, fp, param);
}

void taos_fetch_whitelist_dual_stack_a(TAOS *taos, __taos_async_whitelist_dual_stack_fn_t fp, void *param) {
  return (*fp_taos_fetch_whitelist_dual_stack_a)(taos, fp, param);
}

void taos_fetch_ip_whitelist_a(TAOS *taos, __taos_async_ip_whitelist_fn_t fp, void *param) {
  return (*fp_taos_fetch_ip_whitelist_a)(taos, fp, param);
}

void taos_fetch_datetime_whitelist_a(TAOS *taos, __taos_async_datetime_whitelist_fn_t fp, void *param) {
  return (*fp_taos_fetch_datetime_whitelist_a)(taos, fp, param);
}

int taos_set_conn_mode(TAOS *taos, int mode, int value) {
  return (*fp_taos_set_conn_mode)(taos, mode, value);
}

TAOS_RES *taos_schemaless_insert(TAOS *taos, char *lines[], int numLines, int protocol, int precision) {
  return (*fp_taos_schemaless_insert)(taos, lines, numLines, protocol, precision);
}

TAOS_RES *taos_schemaless_insert_with_reqid(TAOS *taos, char *lines[], int numLines, int protocol, int precision,
                                            int64_t reqid) {
  return (*fp_taos_schemaless_insert_with_reqid)(taos, lines, numLines, protocol, precision, reqid);
}

TAOS_RES *taos_schemaless_insert_raw(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol,
                                     int precision) {
  return (*fp_taos_schemaless_insert_raw)(taos, lines, len, totalRows, protocol, precision);
}

TAOS_RES *taos_schemaless_insert_raw_with_reqid(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol,
                                                int precision, int64_t reqid) {
  return (*fp_taos_schemaless_insert_raw_with_reqid)(taos, lines, len, totalRows, protocol, precision, reqid);
}

TAOS_RES *taos_schemaless_insert_ttl(TAOS *taos, char *lines[], int numLines, int protocol, int precision,
                                     int32_t ttl) {
  return (*fp_taos_schemaless_insert_ttl)(taos, lines, numLines, protocol, precision, ttl);
}

TAOS_RES *taos_schemaless_insert_ttl_with_reqid(TAOS *taos, char *lines[], int numLines, int protocol, int precision,
                                                int32_t ttl, int64_t reqid) {
  return (*fp_taos_schemaless_insert_ttl_with_reqid)(taos, lines, numLines, protocol, precision, ttl, reqid);
}

TAOS_RES *taos_schemaless_insert_raw_ttl(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol,
                                         int precision, int32_t ttl) {
  return (*fp_taos_schemaless_insert_raw_ttl)(taos, lines, len, totalRows, protocol, precision, ttl);
}

TAOS_RES *taos_schemaless_insert_raw_ttl_with_reqid(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol,
                                                    int precision, int32_t ttl, int64_t reqid) {
  return (*fp_taos_schemaless_insert_raw_ttl_with_reqid)(taos, lines, len, totalRows, protocol, precision, ttl, reqid);
}

TAOS_RES *taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(TAOS *taos, char *lines, int len, int32_t *totalRows,
                                                               int protocol, int precision, int32_t ttl, int64_t reqid,
                                                               char *tbnameKey) {
  return (*fp_taos_schemaless_insert_raw_ttl_with_reqid_tbname_key)(taos, lines, len, totalRows, protocol, precision,
                                                                    ttl, reqid, tbnameKey);
}

TAOS_RES *taos_schemaless_insert_ttl_with_reqid_tbname_key(TAOS *taos, char *lines[], int numLines, int protocol,
                                                           int precision, int32_t ttl, int64_t reqid, char *tbnameKey) {
  return (*fp_taos_schemaless_insert_ttl_with_reqid_tbname_key)(taos, lines, numLines, protocol, precision, ttl, reqid,
                                                                tbnameKey);
}

tmq_conf_t *tmq_conf_new() {
  (void)taos_init();
  return (*fp_tmq_conf_new)();
}

tmq_conf_res_t tmq_conf_set(tmq_conf_t *conf, const char *key, const char *value) {
  return (*fp_tmq_conf_set)(conf, key, value);
}

void tmq_conf_destroy(tmq_conf_t *conf) {
  (*fp_tmq_conf_destroy)(conf);
}

void tmq_conf_set_auto_commit_cb(tmq_conf_t *conf, tmq_commit_cb *cb, void *param) {
  (*fp_tmq_conf_set_auto_commit_cb)(conf, cb, param);
}

tmq_list_t *tmq_list_new() {
  (void)taos_init();
  return (*fp_tmq_list_new)();
}

int32_t tmq_list_append(tmq_list_t *tlist, const char *val) {
  return (*fp_tmq_list_append)(tlist, val);
}

void tmq_list_destroy(tmq_list_t *tlist) {
  (*fp_tmq_list_destroy)(tlist);
}

int32_t tmq_list_get_size(const tmq_list_t *tlist) {
  return (*fp_tmq_list_get_size)(tlist);
}

char **tmq_list_to_c_array(const tmq_list_t *tlist) {
  return (*fp_tmq_list_to_c_array)(tlist);
}

tmq_t *tmq_consumer_new(tmq_conf_t *conf, char *errstr, int32_t errstrLen) {
  (void)taos_init();
  return (*fp_tmq_consumer_new)(conf, errstr, errstrLen);
}

int32_t tmq_subscribe(tmq_t *tmq, const tmq_list_t *topic_list) {
  return (*fp_tmq_subscribe)(tmq, topic_list);
}

int32_t tmq_unsubscribe(tmq_t *tmq) {
  return (*fp_tmq_unsubscribe)(tmq);
}

int32_t tmq_subscription(tmq_t *tmq, tmq_list_t **topics) {
  return (*fp_tmq_subscription)(tmq, topics);
}

TAOS_RES *tmq_consumer_poll(tmq_t *tmq, int64_t timeout) {
  return (*fp_tmq_consumer_poll)(tmq, timeout);
}

int32_t tmq_consumer_close(tmq_t *tmq) {
  return (*fp_tmq_consumer_close)(tmq);
}

int32_t tmq_commit_sync(tmq_t *tmq, const TAOS_RES *msg) {
  return (*fp_tmq_commit_sync)(tmq, msg);
}

void tmq_commit_async(tmq_t *tmq, const TAOS_RES *msg, tmq_commit_cb *cb, void *param) {
  (*fp_tmq_commit_async)(tmq, msg, cb, param);
}

int32_t tmq_commit_offset_sync(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset) {
  return (*fp_tmq_commit_offset_sync)(tmq, pTopicName, vgId, offset);
}

void tmq_commit_offset_async(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset, tmq_commit_cb *cb,
                             void *param) {
  (*fp_tmq_commit_offset_async)(tmq, pTopicName, vgId, offset, cb, param);
}

int32_t tmq_get_topic_assignment(tmq_t *tmq, const char *pTopicName, tmq_topic_assignment **assignment,
                                 int32_t *numOfAssignment) {
  return (*fp_tmq_get_topic_assignment)(tmq, pTopicName, assignment, numOfAssignment);
}

void tmq_free_assignment(tmq_topic_assignment *pAssignment) {
  (*fp_tmq_free_assignment)(pAssignment);
}

int32_t tmq_offset_seek(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset) {
  return (*fp_tmq_offset_seek)(tmq, pTopicName, vgId, offset);
}

int64_t tmq_position(tmq_t *tmq, const char *pTopicName, int32_t vgId) {
  return (*fp_tmq_position)(tmq, pTopicName, vgId);
}

int64_t tmq_committed(tmq_t *tmq, const char *pTopicName, int32_t vgId) {
  return (*fp_tmq_committed)(tmq, pTopicName, vgId);
}

TAOS *tmq_get_connect(tmq_t *tmq) {
  return (*fp_tmq_get_connect)(tmq);
}

const char *tmq_get_table_name(TAOS_RES *res) {
  return (*fp_tmq_get_table_name)(res);
}

tmq_res_t tmq_get_res_type(TAOS_RES *res) {
  return (*fp_tmq_get_res_type)(res);
}

const char *tmq_get_topic_name(TAOS_RES *res) {
  return (*fp_tmq_get_topic_name)(res);
}

const char *tmq_get_db_name(TAOS_RES *res) {
  return (*fp_tmq_get_db_name)(res);
}

int32_t tmq_get_vgroup_id(TAOS_RES *res) {
  return (*fp_tmq_get_vgroup_id)(res);
}

int64_t tmq_get_vgroup_offset(TAOS_RES *res) {
  return (*fp_tmq_get_vgroup_offset)(res);
}

const char *tmq_err2str(int32_t code) {
  return (*fp_tmq_err2str)(code);
}

int32_t tmq_get_raw(TAOS_RES *res, tmq_raw_data *raw) {
  return (*fp_tmq_get_raw)(res, raw);
}

int32_t tmq_write_raw(TAOS *taos, tmq_raw_data raw) {
  return (*fp_tmq_write_raw)(taos, raw);
}

int taos_write_raw_block(TAOS *taos, int numOfRows, char *pData, const char *tbname) {
  return (*fp_taos_write_raw_block)(taos, numOfRows, pData, tbname);
}

int taos_write_raw_block_with_reqid(TAOS *taos, int numOfRows, char *pData, const char *tbname, int64_t reqid) {
  return (*fp_taos_write_raw_block_with_reqid)(taos, numOfRows, pData, tbname, reqid);
}

int taos_write_raw_block_with_fields(TAOS *taos, int rows, char *pData, const char *tbname, TAOS_FIELD *fields,
                                     int numFields) {
  return (*fp_taos_write_raw_block_with_fields)(taos, rows, pData, tbname, fields, numFields);
}

int taos_write_raw_block_with_fields_with_reqid(TAOS *taos, int rows, char *pData, const char *tbname,
                                                TAOS_FIELD *fields, int numFields, int64_t reqid) {
  return (*fp_taos_write_raw_block_with_fields_with_reqid)(taos, rows, pData, tbname, fields, numFields, reqid);
}

/* Instance management APIs.  They are declared in taos.h but were not forwarded
 * here, so libtaos.so did not export them and consumers that link only the
 * dispatcher could not resolve them (taosadapter did).  Appended so the symbols
 * already exported keep their place. */
int32_t taos_register_instance(const char *id, const char *type, const char *desc, int32_t expire) {
  (void)taos_init(); /* loading the driver is what fills the slot below */
  return (*fp_taos_register_instance)(id, type, desc, expire);
}

int32_t taos_list_instances(const char *filter_type, char ***pList, int32_t *pCount) {
  (void)taos_init();
  return (*fp_taos_list_instances)(filter_type, pList, pCount);
}

void taos_free_instances(char ***pList, int32_t pCount) {
  (void)taos_init();
  (*fp_taos_free_instances)(pList, pCount);
}

void tmq_free_raw(tmq_raw_data raw) {
  (*fp_tmq_free_raw)(raw);
}

char *tmq_get_json_meta(TAOS_RES *res) {
  return (*fp_tmq_get_json_meta)(res);
}

void tmq_free_json_meta(char *jsonMeta) {
  return (*fp_tmq_free_json_meta)(jsonMeta);
}

TSDB_SERVER_STATUS taos_check_server_status(const char *fqdn, int port, char *details, int maxlen) {
  return (*fp_taos_check_server_status)(fqdn, port, details, maxlen);
}

void taos_write_crashinfo(int signum, void *sigInfo, void *context) {
  (void)taos_init();
  (*fp_taos_write_crashinfo)(signum, sigInfo, context);
}

char *getBuildInfo() {
  (void)taos_init();
  return (*fp_getBuildInfo)();
}

int32_t taos_connect_is_alive(TAOS *taos) {
  return (*fp_taos_connect_is_alive)(taos);
}
