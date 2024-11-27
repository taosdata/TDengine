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

#include "wrapper.h"

static TdThreadOnce tsDriverOnce = PTHREAD_ONCE_INIT;
volatile int32_t    tsDriverOnceRet = 0;

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

#define CHECK_VOID(fp)                \
  if (tsDriver == NULL) {             \
    ERR_VOID(TSDB_CODE_DLL_NOT_FOUND) \
  }                                   \
  if (fp == NULL) {                   \
    ERR_VOID(TSDB_CODE_DLL_NOT_FOUND) \
  }

#define CHECK_PTR(fp)                \
  if (tsDriver == NULL) {            \
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND) \
  }                                  \
  if (fp == NULL) {                  \
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND) \
  }

#define CHECK_INT(fp)                \
  if (tsDriver == NULL) {            \
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND) \
  }                                  \
  if (fp == NULL) {                  \
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND) \
  }

#define CHECK_BOOL(fp)                \
  if (tsDriver == NULL) {             \
    ERR_BOOL(TSDB_CODE_DLL_NOT_FOUND) \
  }                                   \
  if (fp == NULL) {                   \
    ERR_BOOL(TSDB_CODE_DLL_NOT_FOUND) \
  }

#define CHECK_CONFRET(fp)                \
  if (tsDriver == NULL) {                \
    ERR_CONFRET(TSDB_CODE_DLL_NOT_FOUND) \
  }                                      \
  if (fp == NULL) {                      \
    ERR_CONFRET(TSDB_CODE_DLL_NOT_FOUND) \
  }

#define IsInternal() (tsDriverType == DRIVER_INTERNAL)

void taos_cleanup(void) {
  if (IsInternal()) {
    CHECK_VOID(fp_taos_cleanup);
    (*fp_taos_cleanup)();
  } else {
    ERR_VOID(TSDB_CODE_DLL_NOT_FOUND)  // todo
  }
}

int taos_options(TSDB_OPTION option, const void *arg, ...) {
  if (option == TSDB_OPTION_DRIVER) {
    if (tsDriver == NULL) {
      if (strcmp((const char *)arg, "internal") == 0) {
        tsDriverType = DRIVER_INTERNAL;
      } else {
        tsDriverType = DRIVER_WEBSOCKET;
      }
      return 0;
    } else {
      terrno = TSDB_CODE_REPEAT_INIT;
      return -1;
    }
  }

  if (IsInternal()) {
    CHECK_INT(fp_taos_options);
    return (*fp_taos_options)(option, arg);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

setConfRet taos_set_config(const char *config) {
  if (IsInternal()) {
    CHECK_CONFRET(fp_taos_set_config);
    return (*fp_taos_set_config)(config);
  } else {
    ERR_CONFRET(TSDB_CODE_DLL_NOT_FOUND)
  }
}

static void taos_init_wrapper(void) {
  tsDriverOnceRet = taosDriverInit(tsDriverType);
  if (tsDriverOnceRet != 0) return;

  if (IsInternal()) {
    if (fp_taos_init == NULL) {
      terrno = TSDB_CODE_DLL_FUNC_NOT_FOUND;
      tsDriverOnceRet = -1;
    } else {
      tsDriverOnceRet = (*fp_taos_init)();
    }
  } else {
    terrno = TSDB_CODE_DLL_NOT_FOUND;
    tsDriverOnceRet = -1;  // todo
  }
}

int taos_init(void) {
  (void)taosThreadOnce(&tsDriverOnce, taos_init_wrapper);
  return tsDriverOnceRet;
}

TAOS *taos_connect(const char *ip, const char *user, const char *pass, const char *db, uint16_t port) {
  if (taos_init() != 0) {
    terrno = TSDB_CODE_DLL_NOT_FOUND;
    return NULL;
  }

  if (IsInternal()) {
    CHECK_PTR(fp_taos_connect);
    return (*fp_taos_connect)(ip, user, pass, db, port);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)  // todo
  }
}

TAOS *taos_connect_auth(const char *ip, const char *user, const char *auth, const char *db, uint16_t port) {
  if (taos_init() != 0) {
    terrno = TSDB_CODE_DLL_NOT_FOUND;
    return NULL;
  }

  if (IsInternal()) {
    CHECK_PTR(fp_taos_connect_auth);
    return (*fp_taos_connect_auth)(ip, user, auth, db, port);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)  // todo
  }
}

void taos_close(TAOS *taos) {
  if (IsInternal()) {
    CHECK_VOID(fp_taos_close);
    (*fp_taos_close)(taos);
  } else {
    ERR_VOID(TSDB_CODE_DLL_NOT_FOUND)  // todo
  }
}

const char *taos_data_type(int type) {
  if (IsInternal()) {
    CHECK_PTR(fp_taos_data_type);
    return (*fp_taos_data_type)(type);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

TAOS_STMT *taos_stmt_init(TAOS *taos) {
  if (IsInternal()) {
    CHECK_PTR(fp_taos_stmt_init);
    return (*fp_taos_stmt_init)(taos);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

TAOS_STMT *taos_stmt_init_with_reqid(TAOS *taos, int64_t reqid) {
  if (IsInternal()) {
    CHECK_PTR(fp_taos_stmt_init_with_reqid);
    return (*fp_taos_stmt_init_with_reqid)(taos, reqid);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

TAOS_STMT *taos_stmt_init_with_options(TAOS *taos, TAOS_STMT_OPTIONS *options) {
  if (IsInternal()) {
    CHECK_PTR(fp_taos_stmt_init_with_options);
    return (*fp_taos_stmt_init_with_options)(taos, options);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_stmt_prepare(TAOS_STMT *stmt, const char *sql, unsigned long length) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_stmt_prepare);
    return (*fp_taos_stmt_prepare)(stmt, sql, length);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_stmt_set_tbname_tags(TAOS_STMT *stmt, const char *name, TAOS_MULTI_BIND *tags) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_stmt_set_tbname_tags);
    return (*fp_taos_stmt_set_tbname_tags)(stmt, name, tags);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_stmt_set_tbname(TAOS_STMT *stmt, const char *name) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_stmt_set_tbname);
    return (*fp_taos_stmt_set_tbname)(stmt, name);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_stmt_set_tags(TAOS_STMT *stmt, TAOS_MULTI_BIND *tags) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_stmt_set_tags);
    return (*fp_taos_stmt_set_tags)(stmt, tags);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_stmt_set_sub_tbname(TAOS_STMT *stmt, const char *name) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_stmt_set_sub_tbname);
    return (*fp_taos_stmt_set_sub_tbname)(stmt, name);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_stmt_get_tag_fields(TAOS_STMT *stmt, int *fieldNum, TAOS_FIELD_E **fields) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_stmt_get_tag_fields);
    return (*fp_taos_stmt_get_tag_fields)(stmt, fieldNum, fields);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_stmt_get_col_fields(TAOS_STMT *stmt, int *fieldNum, TAOS_FIELD_E **fields) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_stmt_get_col_fields);
    return (*fp_taos_stmt_get_col_fields)(stmt, fieldNum, fields);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

void taos_stmt_reclaim_fields(TAOS_STMT *stmt, TAOS_FIELD_E *fields) {
  if (IsInternal()) {
    CHECK_VOID(fp_taos_stmt_reclaim_fields);
    (*fp_taos_stmt_reclaim_fields)(stmt, fields);
  } else {
    ERR_VOID(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_stmt_is_insert(TAOS_STMT *stmt, int *insert) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_stmt_is_insert);
    return (*fp_taos_stmt_is_insert)(stmt, insert);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_stmt_num_params(TAOS_STMT *stmt, int *nums) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_stmt_num_params);
    return (*fp_taos_stmt_num_params)(stmt, nums);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_stmt_get_param(TAOS_STMT *stmt, int idx, int *type, int *bytes) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_stmt_get_param);
    return (*fp_taos_stmt_get_param)(stmt, idx, type, bytes);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_stmt_bind_param(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_stmt_bind_param);
    return (*fp_taos_stmt_bind_param)(stmt, bind);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_stmt_bind_param_batch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_stmt_bind_param_batch);
    return (*fp_taos_stmt_bind_param_batch)(stmt, bind);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_stmt_bind_single_param_batch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind, int colIdx) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_stmt_bind_single_param_batch);
    return (*fp_taos_stmt_bind_single_param_batch)(stmt, bind, colIdx);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_stmt_add_batch(TAOS_STMT *stmt) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_stmt_add_batch);
    return (*fp_taos_stmt_add_batch)(stmt);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_stmt_execute(TAOS_STMT *stmt) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_stmt_execute);
    return (*fp_taos_stmt_execute)(stmt);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

TAOS_RES *taos_stmt_use_result(TAOS_STMT *stmt) {
  if (IsInternal()) {
    CHECK_PTR(fp_taos_stmt_use_result);
    return (*fp_taos_stmt_use_result)(stmt);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_stmt_close(TAOS_STMT *stmt) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_stmt_close);
    return (*fp_taos_stmt_close)(stmt);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

char *taos_stmt_errstr(TAOS_STMT *stmt) {
  if (IsInternal()) {
    CHECK_PTR(fp_taos_stmt_errstr);
    return (*fp_taos_stmt_errstr)(stmt);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_stmt_affected_rows(TAOS_STMT *stmt) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_stmt_affected_rows);
    return (*fp_taos_stmt_affected_rows)(stmt);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_stmt_affected_rows_once(TAOS_STMT *stmt) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_stmt_affected_rows_once);
    return (*fp_taos_stmt_affected_rows_once)(stmt);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

TAOS_STMT2 *taos_stmt2_init(TAOS *taos, TAOS_STMT2_OPTION *option) {
  if (IsInternal()) {
    CHECK_PTR(fp_taos_stmt2_init);
    return (*fp_taos_stmt2_init)(taos, option);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_stmt2_prepare(TAOS_STMT2 *stmt, const char *sql, unsigned long length) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_stmt2_prepare);
    return (*fp_taos_stmt2_prepare)(stmt, sql, length);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_stmt2_bind_param(TAOS_STMT2 *stmt, TAOS_STMT2_BINDV *bindv, int32_t col_idx) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_stmt2_bind_param);
    return (*fp_taos_stmt2_bind_param)(stmt, bindv, col_idx);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_stmt2_exec(TAOS_STMT2 *stmt, int *affected_rows) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_stmt2_exec);
    return (*fp_taos_stmt2_exec)(stmt, affected_rows);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_stmt2_close(TAOS_STMT2 *stmt) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_stmt2_close);
    return (*fp_taos_stmt2_close)(stmt);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_stmt2_is_insert(TAOS_STMT2 *stmt, int *insert) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_stmt2_is_insert);
    return (*fp_taos_stmt2_is_insert)(stmt, insert);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_stmt2_get_fields(TAOS_STMT2 *stmt, TAOS_FIELD_T field_type, int *count, TAOS_FIELD_E **fields) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_stmt2_get_fields);
    return (*fp_taos_stmt2_get_fields)(stmt, field_type, count, fields);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_stmt2_get_stb_fields(TAOS_STMT2 *stmt, int *count, TAOS_FIELD_STB **fields) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_stmt2_get_stb_fields);
    return (*fp_taos_stmt2_get_stb_fields)(stmt, count, fields);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

void taos_stmt2_free_fields(TAOS_STMT2 *stmt, TAOS_FIELD_E *fields) {
  if (IsInternal()) {
    CHECK_VOID(fp_taos_stmt2_free_fields);
    (*fp_taos_stmt2_free_fields)(stmt, fields);
  } else {
    ERR_VOID(TSDB_CODE_DLL_NOT_FOUND)
  }
}

void taos_stmt2_free_stb_fields(TAOS_STMT2 *stmt, TAOS_FIELD_STB *fields) {
  if (IsInternal()) {
    CHECK_VOID(fp_taos_stmt2_free_stb_fields);
    (*fp_taos_stmt2_free_stb_fields)(stmt, fields);
  } else {
    ERR_VOID(TSDB_CODE_DLL_NOT_FOUND)
  }
}

TAOS_RES *taos_stmt2_result(TAOS_STMT2 *stmt) {
  if (IsInternal()) {
    CHECK_PTR(fp_taos_stmt2_result);
    return (*fp_taos_stmt2_result)(stmt);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

char *taos_stmt2_error(TAOS_STMT2 *stmt) {
  if (IsInternal()) {
    CHECK_PTR(fp_taos_stmt2_error);
    return (*fp_taos_stmt2_error)(stmt);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

TAOS_RES *taos_query(TAOS *taos, const char *sql) {
  if (IsInternal()) {
    CHECK_PTR(fp_taos_query);
    return (*fp_taos_query)(taos, sql);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)  // todo
  }
}

TAOS_RES *taos_query_with_reqid(TAOS *taos, const char *sql, int64_t reqId) {
  if (IsInternal()) {
    CHECK_PTR(fp_taos_query_with_reqid);
    return (*fp_taos_query_with_reqid)(taos, sql, reqId);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

TAOS_ROW taos_fetch_row(TAOS_RES *res) {
  if (IsInternal()) {
    CHECK_PTR(fp_taos_fetch_row);
    return (*fp_taos_fetch_row)(res);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)  // todo
  }
}

int taos_result_precision(TAOS_RES *res) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_result_precision);
    return (*fp_taos_result_precision)(res);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)  // todo
  }
}

void taos_free_result(TAOS_RES *res) {
  if (IsInternal()) {
    CHECK_VOID(fp_taos_free_result);
    return (*fp_taos_free_result)(res);
  } else {
    ERR_VOID(TSDB_CODE_DLL_NOT_FOUND)  // todo
  }
}

void taos_kill_query(TAOS *taos) {
  if (IsInternal()) {
    CHECK_VOID(fp_taos_kill_query);
    return (*fp_taos_kill_query)(taos);
  } else {
    ERR_VOID(TSDB_CODE_DLL_NOT_FOUND)  // todo
  }
}

int taos_field_count(TAOS_RES *res) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_field_count);
    return (*fp_taos_field_count)(res);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)  // todo
  }
}

int taos_num_fields(TAOS_RES *res) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_num_fields);
    return (*fp_taos_num_fields)(res);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)  // todo
  }
}

int taos_affected_rows(TAOS_RES *res) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_affected_rows);
    return (*fp_taos_affected_rows)(res);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int64_t taos_affected_rows64(TAOS_RES *res) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_affected_rows64);
    return (*fp_taos_affected_rows64)(res);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)  // todo
  }
}

TAOS_FIELD *taos_fetch_fields(TAOS_RES *res) {
  if (IsInternal()) {
    CHECK_PTR(fp_taos_fetch_fields);
    return (*fp_taos_fetch_fields)(res);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)  // todo
  }
}

int taos_select_db(TAOS *taos, const char *db) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_select_db);
    return (*fp_taos_select_db)(taos, db);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)  // todo
  }
}

int taos_print_row(char *str, TAOS_ROW row, TAOS_FIELD *fields, int num_fields) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_print_row);
    return (*fp_taos_print_row)(str, row, fields, num_fields);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)  // todo
  }
}

int taos_print_row_with_size(char *str, uint32_t size, TAOS_ROW row, TAOS_FIELD *fields, int num_fields) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_print_row_with_size);
    return (*fp_taos_print_row_with_size)(str, size, row, fields, num_fields);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

void taos_stop_query(TAOS_RES *res) {
  if (IsInternal()) {
    CHECK_VOID(fp_taos_stop_query);
    (*fp_taos_stop_query)(res);
  } else {
    ERR_VOID(TSDB_CODE_DLL_NOT_FOUND)  // todo
  }
}

bool taos_is_null(TAOS_RES *res, int32_t row, int32_t col) {
  if (IsInternal()) {
    CHECK_BOOL(fp_taos_is_null);
    return (*fp_taos_is_null)(res, row, col);
  } else {
    ERR_BOOL(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_is_null_by_column(TAOS_RES *res, int columnIndex, bool result[], int *rows) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_is_null_by_column);
    return (*fp_taos_is_null_by_column)(res, columnIndex, result, rows);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

bool taos_is_update_query(TAOS_RES *res) {
  if (IsInternal()) {
    CHECK_BOOL(fp_taos_is_update_query);
    return (*fp_taos_is_update_query)(res);
  } else {
    ERR_BOOL(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_fetch_block(TAOS_RES *res, TAOS_ROW *rows) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_fetch_block);
    return (*fp_taos_fetch_block)(res, rows);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_fetch_block_s(TAOS_RES *res, int *numOfRows, TAOS_ROW *rows) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_fetch_block_s);
    return (*fp_taos_fetch_block_s)(res, numOfRows, rows);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_fetch_raw_block(TAOS_RES *res, int *numOfRows, void **pData) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_fetch_raw_block);
    return (*fp_taos_fetch_raw_block)(res, numOfRows, pData);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int *taos_get_column_data_offset(TAOS_RES *res, int columnIndex) {
  if (IsInternal()) {
    CHECK_PTR(fp_taos_get_column_data_offset);
    return (*fp_taos_get_column_data_offset)(res, columnIndex);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_validate_sql(TAOS *taos, const char *sql) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_validate_sql);
    return (*fp_taos_validate_sql)(taos, sql);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

void taos_reset_current_db(TAOS *taos) {
  if (IsInternal()) {
    CHECK_VOID(fp_taos_reset_current_db);
    (*fp_taos_reset_current_db)(taos);
  } else {
    ERR_VOID(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int *taos_fetch_lengths(TAOS_RES *res) {
  if (IsInternal()) {
    CHECK_PTR(fp_taos_fetch_lengths);
    return (*fp_taos_fetch_lengths)(res);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)  // todo
  }
}

TAOS_ROW *taos_result_block(TAOS_RES *res) {
  if (IsInternal()) {
    CHECK_PTR(fp_taos_result_block);
    return (*fp_taos_result_block)(res);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

const char *taos_get_server_info(TAOS *taos) {
  if (IsInternal()) {
    CHECK_PTR(fp_taos_get_server_info);
    return (*fp_taos_get_server_info)(taos);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)  // todo
  }
}

const char *taos_get_client_info() {
  if (IsInternal()) {
    CHECK_PTR(fp_taos_get_client_info);
    return (*fp_taos_get_client_info)();
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)  // todo
  }
}

int taos_get_current_db(TAOS *taos, char *database, int len, int *required) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_get_current_db);
    return (*fp_taos_get_current_db)(taos, database, len, required);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)  // todo
  }
}

const char *taos_errstr(TAOS_RES *res) {
  if (res == NULL) {
    return (const char *)tstrerror(terrno);
  }

  if (IsInternal()) {
    CHECK_PTR(fp_taos_errstr);
    return (*fp_taos_errstr)(res);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)  // todo
  }
}

int taos_errno(TAOS_RES *res) {
  if (res == NULL) {
    return terrno;
  }

  if (IsInternal()) {
    CHECK_INT(fp_taos_errno);
    return (*fp_taos_errno)(res);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)  // todo
  }
}

void taos_query_a(TAOS *taos, const char *sql, __taos_async_fn_t fp, void *param) {
  if (IsInternal()) {
    CHECK_VOID(fp_taos_query_a);
    (*fp_taos_query_a)(taos, sql, fp, param);
  } else {
    ERR_VOID(TSDB_CODE_DLL_NOT_FOUND)
  }
}

void taos_query_a_with_reqid(TAOS *taos, const char *sql, __taos_async_fn_t fp, void *param, int64_t reqid) {
  if (IsInternal()) {
    CHECK_VOID(fp_taos_query_a_with_reqid);
    (*fp_taos_query_a_with_reqid)(taos, sql, fp, param, reqid);
  } else {
    ERR_VOID(TSDB_CODE_DLL_NOT_FOUND)
  }
}

void taos_fetch_rows_a(TAOS_RES *res, __taos_async_fn_t fp, void *param) {
  if (IsInternal()) {
    CHECK_VOID(fp_taos_fetch_rows_a);
    (*fp_taos_fetch_rows_a)(res, fp, param);
  } else {
    ERR_VOID(TSDB_CODE_DLL_NOT_FOUND)  // todo
  }
}

void taos_fetch_raw_block_a(TAOS_RES *res, __taos_async_fn_t fp, void *param) {
  if (IsInternal()) {
    CHECK_VOID(fp_taos_fetch_raw_block_a);
    (*fp_taos_fetch_raw_block_a)(res, fp, param);
  } else {
    ERR_VOID(TSDB_CODE_DLL_NOT_FOUND)
  }
}

const void *taos_get_raw_block(TAOS_RES *res) {
  if (IsInternal()) {
    CHECK_PTR(fp_taos_get_raw_block);
    return (*fp_taos_get_raw_block)(res);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_get_db_route_info(TAOS *taos, const char *db, TAOS_DB_ROUTE_INFO *dbInfo) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_get_db_route_info);
    return (*fp_taos_get_db_route_info)(taos, db, dbInfo);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_get_table_vgId(TAOS *taos, const char *db, const char *table, int *vgId) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_get_table_vgId);
    return (*fp_taos_get_table_vgId)(taos, db, table, vgId);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_get_tables_vgId(TAOS *taos, const char *db, const char *table[], int tableNum, int *vgId) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_get_tables_vgId);
    return (*fp_taos_get_tables_vgId)(taos, db, table, tableNum, vgId);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_load_table_info(TAOS *taos, const char *tableNameList) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_load_table_info);
    return (*fp_taos_load_table_info)(taos, tableNameList);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

void taos_set_hb_quit(int8_t quitByKill) {
  if (taos_init() != 0) {
    return;
  }

  if (IsInternal()) {
    CHECK_VOID(fp_taos_set_hb_quit);
    return (*fp_taos_set_hb_quit)(quitByKill);
  } else {
    ERR_VOID(TSDB_CODE_DLL_NOT_FOUND)  // todo
  }
}

int taos_set_notify_cb(TAOS *taos, __taos_notify_fn_t fp, void *param, int type) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_set_notify_cb);
    return (*fp_taos_set_notify_cb)(taos, fp, param, type);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

void taos_fetch_whitelist_a(TAOS *taos, __taos_async_whitelist_fn_t fp, void *param) {
  if (IsInternal()) {
    CHECK_VOID(fp_taos_fetch_whitelist_a);
    (*fp_taos_fetch_whitelist_a)(taos, fp, param);
  } else {
    ERR_VOID(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_set_conn_mode(TAOS *taos, int mode, int value) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_set_conn_mode);
    return (*fp_taos_set_conn_mode)(taos, mode, value);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)  // todo
  }
}

TAOS_RES *taos_schemaless_insert(TAOS *taos, char *lines[], int numLines, int protocol, int precision) {
  if (IsInternal()) {
    CHECK_PTR(fp_taos_schemaless_insert);
    return (*fp_taos_schemaless_insert)(taos, lines, numLines, protocol, precision);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

TAOS_RES *taos_schemaless_insert_with_reqid(TAOS *taos, char *lines[], int numLines, int protocol, int precision,
                                            int64_t reqid) {
  if (IsInternal()) {
    CHECK_PTR(fp_taos_schemaless_insert_with_reqid);
    return (*fp_taos_schemaless_insert_with_reqid)(taos, lines, numLines, protocol, precision, reqid);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

TAOS_RES *taos_schemaless_insert_raw(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol,
                                     int precision) {
  if (IsInternal()) {
    CHECK_PTR(fp_taos_schemaless_insert_raw);
    return (*fp_taos_schemaless_insert_raw)(taos, lines, len, totalRows, protocol, precision);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

TAOS_RES *taos_schemaless_insert_raw_with_reqid(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol,
                                                int precision, int64_t reqid) {
  if (IsInternal()) {
    CHECK_PTR(fp_taos_schemaless_insert_raw_with_reqid);
    return (*fp_taos_schemaless_insert_raw_with_reqid)(taos, lines, len, totalRows, protocol, precision, reqid);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

TAOS_RES *taos_schemaless_insert_ttl(TAOS *taos, char *lines[], int numLines, int protocol, int precision,
                                     int32_t ttl) {
  if (IsInternal()) {
    CHECK_PTR(fp_taos_schemaless_insert_ttl);
    return (*fp_taos_schemaless_insert_ttl)(taos, lines, numLines, protocol, precision, ttl);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

TAOS_RES *taos_schemaless_insert_ttl_with_reqid(TAOS *taos, char *lines[], int numLines, int protocol, int precision,
                                                int32_t ttl, int64_t reqid) {
  if (IsInternal()) {
    CHECK_PTR(fp_taos_schemaless_insert_ttl_with_reqid);
    return (*fp_taos_schemaless_insert_ttl_with_reqid)(taos, lines, numLines, protocol, precision, ttl, reqid);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

TAOS_RES *taos_schemaless_insert_raw_ttl(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol,
                                         int precision, int32_t ttl) {
  if (IsInternal()) {
    CHECK_PTR(fp_taos_schemaless_insert_raw_ttl);
    return (*fp_taos_schemaless_insert_raw_ttl)(taos, lines, len, totalRows, protocol, precision, ttl);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

TAOS_RES *taos_schemaless_insert_raw_ttl_with_reqid(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol,
                                                    int precision, int32_t ttl, int64_t reqid) {
  if (IsInternal()) {
    CHECK_PTR(fp_taos_schemaless_insert_raw_ttl_with_reqid);
    return (*fp_taos_schemaless_insert_raw_ttl_with_reqid)(taos, lines, len, totalRows, protocol, precision, ttl,
                                                           reqid);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

TAOS_RES *taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(TAOS *taos, char *lines, int len, int32_t *totalRows,
                                                               int protocol, int precision, int32_t ttl, int64_t reqid,
                                                               char *tbnameKey) {
  if (IsInternal()) {
    CHECK_PTR(fp_taos_schemaless_insert_raw_ttl_with_reqid_tbname_key);
    return (*fp_taos_schemaless_insert_raw_ttl_with_reqid_tbname_key)(taos, lines, len, totalRows, protocol, precision,
                                                                      ttl, reqid, tbnameKey);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

TAOS_RES *taos_schemaless_insert_ttl_with_reqid_tbname_key(TAOS *taos, char *lines[], int numLines, int protocol,
                                                           int precision, int32_t ttl, int64_t reqid, char *tbnameKey) {
  if (IsInternal()) {
    CHECK_PTR(fp_taos_schemaless_insert_ttl_with_reqid_tbname_key);
    return (*fp_taos_schemaless_insert_ttl_with_reqid_tbname_key)(taos, lines, numLines, protocol, precision, ttl,
                                                                  reqid, tbnameKey);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

tmq_conf_t *tmq_conf_new() {
  if (IsInternal()) {
    CHECK_PTR(fp_tmq_conf_new);
    return (*fp_tmq_conf_new)();
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

tmq_conf_res_t tmq_conf_set(tmq_conf_t *conf, const char *key, const char *value) {
  if (IsInternal()) {
    CHECK_INT(fp_tmq_conf_set);
    return (*fp_tmq_conf_set)(conf, key, value);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

void tmq_conf_destroy(tmq_conf_t *conf) {
  if (IsInternal()) {
    CHECK_VOID(fp_tmq_conf_destroy);
    (*fp_tmq_conf_destroy)(conf);
  } else {
    ERR_VOID(TSDB_CODE_DLL_NOT_FOUND)
  }
}

void tmq_conf_set_auto_commit_cb(tmq_conf_t *conf, tmq_commit_cb *cb, void *param) {
  if (IsInternal()) {
    CHECK_VOID(fp_tmq_conf_set_auto_commit_cb);
    (*fp_tmq_conf_set_auto_commit_cb)(conf, cb, param);
  } else {
    ERR_VOID(TSDB_CODE_DLL_NOT_FOUND)
  }
}

tmq_list_t *tmq_list_new() {
  if (IsInternal()) {
    CHECK_PTR(fp_tmq_list_new);
    return (*fp_tmq_list_new)();
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int32_t tmq_list_append(tmq_list_t *tlist, const char *val) {
  if (IsInternal()) {
    CHECK_INT(fp_tmq_list_append);
    return (*fp_tmq_list_append)(tlist, val);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

void tmq_list_destroy(tmq_list_t *tlist) {
  if (IsInternal()) {
    CHECK_VOID(fp_tmq_list_destroy);
    (*fp_tmq_list_destroy)(tlist);
  } else {
    ERR_VOID(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int32_t tmq_list_get_size(const tmq_list_t *tlist) {
  if (IsInternal()) {
    CHECK_INT(fp_tmq_list_get_size);
    return (*fp_tmq_list_get_size)(tlist);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

char **tmq_list_to_c_array(const tmq_list_t *tlist) {
  if (IsInternal()) {
    CHECK_PTR(fp_tmq_list_to_c_array);
    return (*fp_tmq_list_to_c_array)(tlist);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

tmq_t *tmq_consumer_new(tmq_conf_t *conf, char *errstr, int32_t errstrLen) {
  if (IsInternal()) {
    CHECK_PTR(fp_tmq_consumer_new);
    return (*fp_tmq_consumer_new)(conf, errstr, errstrLen);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int32_t tmq_subscribe(tmq_t *tmq, const tmq_list_t *topic_list) {
  if (IsInternal()) {
    CHECK_INT(fp_tmq_subscribe);
    return (*fp_tmq_subscribe)(tmq, topic_list);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int32_t tmq_unsubscribe(tmq_t *tmq) {
  if (IsInternal()) {
    CHECK_INT(fp_tmq_unsubscribe);
    return (*fp_tmq_unsubscribe)(tmq);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int32_t tmq_subscription(tmq_t *tmq, tmq_list_t **topics) {
  if (IsInternal()) {
    CHECK_INT(fp_tmq_subscription);
    return (*fp_tmq_subscription)(tmq, topics);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

TAOS_RES *tmq_consumer_poll(tmq_t *tmq, int64_t timeout) {
  if (IsInternal()) {
    CHECK_PTR(fp_tmq_consumer_poll);
    return (*fp_tmq_consumer_poll)(tmq, timeout);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int32_t tmq_consumer_close(tmq_t *tmq) {
  if (IsInternal()) {
    CHECK_INT(fp_tmq_consumer_close);
    return (*fp_tmq_consumer_close)(tmq);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int32_t tmq_commit_sync(tmq_t *tmq, const TAOS_RES *msg) {
  if (IsInternal()) {
    CHECK_INT(fp_tmq_commit_sync);
    return (*fp_tmq_commit_sync)(tmq, msg);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

void tmq_commit_async(tmq_t *tmq, const TAOS_RES *msg, tmq_commit_cb *cb, void *param) {
  if (IsInternal()) {
    CHECK_VOID(fp_tmq_commit_async);
    (*fp_tmq_commit_async)(tmq, msg, cb, param);
  } else {
    ERR_VOID(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int32_t tmq_commit_offset_sync(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset) {
  if (IsInternal()) {
    CHECK_INT(fp_tmq_commit_offset_sync);
    return (*fp_tmq_commit_offset_sync)(tmq, pTopicName, vgId, offset);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

void tmq_commit_offset_async(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset, tmq_commit_cb *cb,
                             void *param) {
  if (IsInternal()) {
    CHECK_VOID(fp_tmq_commit_offset_async);
    (*fp_tmq_commit_offset_async)(tmq, pTopicName, vgId, offset, cb, param);
  } else {
    ERR_VOID(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int32_t tmq_get_topic_assignment(tmq_t *tmq, const char *pTopicName, tmq_topic_assignment **assignment,
                                 int32_t *numOfAssignment) {
  if (IsInternal()) {
    CHECK_INT(fp_tmq_get_topic_assignment);
    return (*fp_tmq_get_topic_assignment)(tmq, pTopicName, assignment, numOfAssignment);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

void tmq_free_assignment(tmq_topic_assignment *pAssignment) {
  if (IsInternal()) {
    CHECK_VOID(fp_tmq_free_assignment);
    (*fp_tmq_free_assignment)(pAssignment);
  } else {
    ERR_VOID(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int32_t tmq_offset_seek(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset) {
  if (IsInternal()) {
    CHECK_INT(fp_tmq_offset_seek);
    return (*fp_tmq_offset_seek)(tmq, pTopicName, vgId, offset);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int64_t tmq_position(tmq_t *tmq, const char *pTopicName, int32_t vgId) {
  if (IsInternal()) {
    CHECK_INT(fp_tmq_position);
    return (*fp_tmq_position)(tmq, pTopicName, vgId);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int64_t tmq_committed(tmq_t *tmq, const char *pTopicName, int32_t vgId) {
  if (IsInternal()) {
    CHECK_INT(fp_tmq_committed);
    return (*fp_tmq_committed)(tmq, pTopicName, vgId);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

TAOS *tmq_get_connect(tmq_t *tmq) {
  if (IsInternal()) {
    CHECK_PTR(fp_tmq_get_connect);
    return (*fp_tmq_get_connect)(tmq);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

const char *tmq_get_table_name(TAOS_RES *res) {
  if (IsInternal()) {
    CHECK_PTR(fp_tmq_get_table_name);
    return (*fp_tmq_get_table_name)(res);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

tmq_res_t tmq_get_res_type(TAOS_RES *res) {
  if (IsInternal()) {
    CHECK_INT(fp_tmq_get_res_type);
    return (*fp_tmq_get_res_type)(res);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

const char *tmq_get_topic_name(TAOS_RES *res) {
  if (IsInternal()) {
    CHECK_PTR(fp_tmq_get_topic_name);
    return (*fp_tmq_get_topic_name)(res);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

const char *tmq_get_db_name(TAOS_RES *res) {
  if (IsInternal()) {
    CHECK_PTR(fp_tmq_get_db_name);
    return (*fp_tmq_get_db_name)(res);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int32_t tmq_get_vgroup_id(TAOS_RES *res) {
  if (IsInternal()) {
    CHECK_INT(fp_tmq_get_vgroup_id);
    return (*fp_tmq_get_vgroup_id)(res);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int64_t tmq_get_vgroup_offset(TAOS_RES *res) {
  if (IsInternal()) {
    CHECK_INT(fp_tmq_get_vgroup_offset);
    return (*fp_tmq_get_vgroup_offset)(res);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

const char *tmq_err2str(int32_t code) {
  if (IsInternal()) {
    CHECK_PTR(fp_tmq_err2str);
    return (*fp_tmq_err2str)(code);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int32_t tmq_get_raw(TAOS_RES *res, tmq_raw_data *raw) {
  if (IsInternal()) {
    CHECK_INT(fp_tmq_get_raw);
    return (*fp_tmq_get_raw)(res, raw);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int32_t tmq_write_raw(TAOS *taos, tmq_raw_data raw) {
  if (IsInternal()) {
    CHECK_INT(fp_tmq_write_raw);
    return (*fp_tmq_write_raw)(taos, raw);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_write_raw_block(TAOS *taos, int numOfRows, char *pData, const char *tbname) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_write_raw_block);
    return (*fp_taos_write_raw_block)(taos, numOfRows, pData, tbname);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_write_raw_block_with_reqid(TAOS *taos, int numOfRows, char *pData, const char *tbname, int64_t reqid) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_write_raw_block_with_reqid);
    return (*fp_taos_write_raw_block_with_reqid)(taos, numOfRows, pData, tbname, reqid);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_write_raw_block_with_fields(TAOS *taos, int rows, char *pData, const char *tbname, TAOS_FIELD *fields,
                                     int numFields) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_write_raw_block_with_fields);
    return (*fp_taos_write_raw_block_with_fields)(taos, rows, pData, tbname, fields, numFields);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

int taos_write_raw_block_with_fields_with_reqid(TAOS *taos, int rows, char *pData, const char *tbname,
                                                TAOS_FIELD *fields, int numFields, int64_t reqid) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_write_raw_block_with_fields_with_reqid);
    return (*fp_taos_write_raw_block_with_fields_with_reqid)(taos, rows, pData, tbname, fields, numFields, reqid);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)
  }
}

void tmq_free_raw(tmq_raw_data raw) {
  if (IsInternal()) {
    CHECK_VOID(fp_tmq_free_raw);
    (*fp_tmq_free_raw)(raw);
  } else {
    ERR_VOID(TSDB_CODE_DLL_NOT_FOUND)
  }
}

char *tmq_get_json_meta(TAOS_RES *res) {
  if (IsInternal()) {
    CHECK_PTR(fp_tmq_get_json_meta);
    return (*fp_tmq_get_json_meta)(res);
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}

void tmq_free_json_meta(char *jsonMeta) {
  if (IsInternal()) {
    CHECK_VOID(fp_tmq_free_json_meta);
    return (*fp_tmq_free_json_meta)(jsonMeta);
  } else {
    ERR_VOID(TSDB_CODE_DLL_NOT_FOUND)
  }
}

TSDB_SERVER_STATUS taos_check_server_status(const char *fqdn, int port, char *details, int maxlen) {
  if (IsInternal()) {
    CHECK_INT(fp_taos_check_server_status);
    return (*fp_taos_check_server_status)(fqdn, port, details, maxlen);
  } else {
    ERR_INT(TSDB_CODE_DLL_NOT_FOUND)  // todo
  }
}

char *getBuildInfo() {
  if (IsInternal()) {
    CHECK_PTR(fp_getBuildInfo);
    return (*fp_getBuildInfo)();
  } else {
    ERR_PTR(TSDB_CODE_DLL_NOT_FOUND)
  }
}
