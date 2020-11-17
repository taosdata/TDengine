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

#ifndef TDENGINE_TAOS_H
#define TDENGINE_TAOS_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void   TAOS;
typedef void   TAOS_STMT;
typedef void   TAOS_RES;
typedef void   TAOS_STREAM;
typedef void   TAOS_SUB;
typedef void **TAOS_ROW;

// Data type definition
#define TSDB_DATA_TYPE_NULL       0     // 1 bytes
#define TSDB_DATA_TYPE_BOOL       1     // 1 bytes
#define TSDB_DATA_TYPE_TINYINT    2     // 1 byte
#define TSDB_DATA_TYPE_SMALLINT   3     // 2 bytes
#define TSDB_DATA_TYPE_INT        4     // 4 bytes
#define TSDB_DATA_TYPE_BIGINT     5     // 8 bytes
#define TSDB_DATA_TYPE_FLOAT      6     // 4 bytes
#define TSDB_DATA_TYPE_DOUBLE     7     // 8 bytes
#define TSDB_DATA_TYPE_BINARY     8     // string
#define TSDB_DATA_TYPE_TIMESTAMP  9     // 8 bytes
#define TSDB_DATA_TYPE_NCHAR      10    // unicode string

typedef enum {
  TSDB_OPTION_LOCALE,
  TSDB_OPTION_CHARSET,
  TSDB_OPTION_TIMEZONE,
  TSDB_OPTION_CONFIGDIR,
  TSDB_OPTION_SHELL_ACTIVITY_TIMER,
  TSDB_MAX_OPTIONS
} TSDB_OPTION;

typedef struct taosField {
  char     name[65];
  uint8_t  type;
  int16_t  bytes;
} TAOS_FIELD;

#ifdef _TD_GO_DLL_
  #define DLL_EXPORT    __declspec(dllexport)
#else
  #define DLL_EXPORT 
#endif

DLL_EXPORT void  taos_init();
DLL_EXPORT void  taos_cleanup(void);
DLL_EXPORT int   taos_options(TSDB_OPTION option, const void *arg, ...);
DLL_EXPORT TAOS *taos_connect(const char *ip, const char *user, const char *pass, const char *db, uint16_t port);
DLL_EXPORT void  taos_close(TAOS *taos);

const char *taos_data_type(int type);

typedef struct TAOS_BIND {
  int            buffer_type;
  void *         buffer;
  uintptr_t      buffer_length;  // unused
  uintptr_t      *length;
  int *          is_null;
  int            is_unsigned;  // unused
  int *          error;        // unused
  union {
    int64_t        ts;
    int8_t         b;
    int8_t         v1;
    int16_t        v2;
    int32_t        v4;
    int64_t        v8;
    float          f4;
    double         f8;
    unsigned char *bin;
    char          *nchar;
  } u;
  unsigned int     allocated;
} TAOS_BIND;

TAOS_STMT *taos_stmt_init(TAOS *taos);
int        taos_stmt_prepare(TAOS_STMT *stmt, const char *sql, unsigned long length);
int        taos_stmt_is_insert(TAOS_STMT *stmt, int *insert);
int        taos_stmt_num_params(TAOS_STMT *stmt, int *nums);
int        taos_stmt_get_param(TAOS_STMT *stmt, int idx, int *type, int *bytes);
int        taos_stmt_bind_param(TAOS_STMT *stmt, TAOS_BIND *bind);
int        taos_stmt_add_batch(TAOS_STMT *stmt);
int        taos_stmt_execute(TAOS_STMT *stmt);
TAOS_RES * taos_stmt_use_result(TAOS_STMT *stmt);
int        taos_stmt_close(TAOS_STMT *stmt);

DLL_EXPORT TAOS_RES *taos_query(TAOS *taos, const char *sql);
DLL_EXPORT TAOS_ROW taos_fetch_row(TAOS_RES *res);
DLL_EXPORT int taos_result_precision(TAOS_RES *res);  // get the time precision of result
DLL_EXPORT void taos_free_result(TAOS_RES *res);
DLL_EXPORT int taos_field_count(TAOS_RES *res);
DLL_EXPORT int taos_num_fields(TAOS_RES *res);
DLL_EXPORT int taos_affected_rows(TAOS_RES *res);
DLL_EXPORT TAOS_FIELD *taos_fetch_fields(TAOS_RES *res);
DLL_EXPORT int taos_select_db(TAOS *taos, const char *db);
DLL_EXPORT int taos_print_row(char *str, TAOS_ROW row, TAOS_FIELD *fields, int num_fields);
DLL_EXPORT void taos_stop_query(TAOS_RES *res);
DLL_EXPORT bool taos_is_null(TAOS_RES *res, int32_t row, int32_t col);

int taos_fetch_block(TAOS_RES *res, TAOS_ROW *rows);
int taos_validate_sql(TAOS *taos, const char *sql);

int* taos_fetch_lengths(TAOS_RES *res);

// TAOS_RES   *taos_list_tables(TAOS *mysql, const char *wild);
// TAOS_RES   *taos_list_dbs(TAOS *mysql, const char *wild);

// TODO: the return value should be `const`
DLL_EXPORT char *taos_get_server_info(TAOS *taos);
DLL_EXPORT char *taos_get_client_info();
DLL_EXPORT char *taos_errstr(TAOS_RES *tres);

DLL_EXPORT int taos_errno(TAOS_RES *tres);

DLL_EXPORT void taos_query_a(TAOS *taos, const char *sql, void (*fp)(void *param, TAOS_RES *, int code), void *param);
DLL_EXPORT void taos_fetch_rows_a(TAOS_RES *res, void (*fp)(void *param, TAOS_RES *, int numOfRows), void *param);
DLL_EXPORT void taos_fetch_row_a(TAOS_RES *res, void (*fp)(void *param, TAOS_RES *, TAOS_ROW row), void *param);

typedef void (*TAOS_SUBSCRIBE_CALLBACK)(TAOS_SUB* tsub, TAOS_RES *res, void* param, int code);
DLL_EXPORT TAOS_SUB *taos_subscribe(TAOS* taos, int restart, const char* topic, const char *sql, TAOS_SUBSCRIBE_CALLBACK fp, void *param, int interval);
DLL_EXPORT TAOS_RES *taos_consume(TAOS_SUB *tsub);
DLL_EXPORT void      taos_unsubscribe(TAOS_SUB *tsub, int keepProgress);

DLL_EXPORT TAOS_STREAM *taos_open_stream(TAOS *taos, const char *sql, void (*fp)(void *param, TAOS_RES *, TAOS_ROW row),
                              int64_t stime, void *param, void (*callback)(void *));
DLL_EXPORT void taos_close_stream(TAOS_STREAM *tstr);

DLL_EXPORT int taos_load_table_info(TAOS *taos, const char* tableNameList);

#ifdef __cplusplus
}
#endif

#endif
