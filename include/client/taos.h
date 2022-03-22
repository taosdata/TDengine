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

#include <stdbool.h>
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
#define TSDB_DATA_TYPE_NULL       0   // 1 bytes
#define TSDB_DATA_TYPE_BOOL       1   // 1 bytes
#define TSDB_DATA_TYPE_TINYINT    2   // 1 byte
#define TSDB_DATA_TYPE_SMALLINT   3   // 2 bytes
#define TSDB_DATA_TYPE_INT        4   // 4 bytes
#define TSDB_DATA_TYPE_BIGINT     5   // 8 bytes
#define TSDB_DATA_TYPE_FLOAT      6   // 4 bytes
#define TSDB_DATA_TYPE_DOUBLE     7   // 8 bytes
#define TSDB_DATA_TYPE_VARCHAR    8   // string, alias for varchar
#define TSDB_DATA_TYPE_TIMESTAMP  9   // 8 bytes
#define TSDB_DATA_TYPE_NCHAR      10  // unicode string
#define TSDB_DATA_TYPE_UTINYINT   11  // 1 byte
#define TSDB_DATA_TYPE_USMALLINT  12  // 2 bytes
#define TSDB_DATA_TYPE_UINT       13  // 4 bytes
#define TSDB_DATA_TYPE_UBIGINT    14  // 8 bytes
#define TSDB_DATA_TYPE_JSON       15  // json string
#define TSDB_DATA_TYPE_VARBINARY  16  // binary
#define TSDB_DATA_TYPE_DECIMAL    17  // decimal
#define TSDB_DATA_TYPE_BLOB       18  // binary
#define TSDB_DATA_TYPE_MEDIUMBLOB 19
#define TSDB_DATA_TYPE_BINARY     TSDB_DATA_TYPE_VARCHAR  // string

typedef enum {
  TSDB_OPTION_LOCALE,
  TSDB_OPTION_CHARSET,
  TSDB_OPTION_TIMEZONE,
  TSDB_OPTION_CONFIGDIR,
  TSDB_OPTION_SHELL_ACTIVITY_TIMER,
  TSDB_MAX_OPTIONS
} TSDB_OPTION;

typedef enum {
  TSDB_SML_UNKNOWN_PROTOCOL = 0,
  TSDB_SML_LINE_PROTOCOL = 1,
  TSDB_SML_TELNET_PROTOCOL = 2,
  TSDB_SML_JSON_PROTOCOL = 3,
} TSDB_SML_PROTOCOL_TYPE;

typedef enum {
  TSDB_SML_TIMESTAMP_NOT_CONFIGURED = 0,
  TSDB_SML_TIMESTAMP_HOURS,
  TSDB_SML_TIMESTAMP_MINUTES,
  TSDB_SML_TIMESTAMP_SECONDS,
  TSDB_SML_TIMESTAMP_MILLI_SECONDS,
  TSDB_SML_TIMESTAMP_MICRO_SECONDS,
  TSDB_SML_TIMESTAMP_NANO_SECONDS,
} TSDB_SML_TIMESTAMP_TYPE;

typedef struct taosField {
  char    name[65];
  int8_t  type;
  int32_t bytes;
} TAOS_FIELD;

#define DLL_EXPORT

typedef void (*__taos_async_fn_t)(void *param, TAOS_RES *, int code);

typedef struct TAOS_BIND {
  int        buffer_type;
  void      *buffer;
  uintptr_t  buffer_length;  // unused
  uintptr_t *length;
  int       *is_null;

  int  is_unsigned;  // unused
  int *error;        // unused
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
  unsigned int allocated;
} TAOS_BIND;

typedef struct TAOS_MULTI_BIND {
  int       buffer_type;
  void     *buffer;
  uintptr_t buffer_length;
  int32_t  *length;
  char     *is_null;
  int       num;
} TAOS_MULTI_BIND;

DLL_EXPORT void  taos_cleanup(void);
DLL_EXPORT int   taos_options(TSDB_OPTION option, const void *arg, ...);
DLL_EXPORT TAOS *taos_connect(const char *ip, const char *user, const char *pass, const char *db, uint16_t port);
DLL_EXPORT TAOS *taos_connect_l(const char *ip, int ipLen, const char *user, int userLen, const char *pass, int passLen,
                                const char *db, int dbLen, uint16_t port);
DLL_EXPORT TAOS *taos_connect_auth(const char *ip, const char *user, const char *auth, const char *db, uint16_t port);
DLL_EXPORT void  taos_close(TAOS *taos);

const char *taos_data_type(int type);

DLL_EXPORT TAOS_STMT *taos_stmt_init(TAOS *taos);
DLL_EXPORT int        taos_stmt_prepare(TAOS_STMT *stmt, const char *sql, unsigned long length);
DLL_EXPORT int        taos_stmt_set_tbname_tags(TAOS_STMT *stmt, const char *name, TAOS_BIND *tags);
DLL_EXPORT int        taos_stmt_set_tbname(TAOS_STMT *stmt, const char *name);
DLL_EXPORT int        taos_stmt_set_sub_tbname(TAOS_STMT *stmt, const char *name);

DLL_EXPORT int       taos_stmt_is_insert(TAOS_STMT *stmt, int *insert);
DLL_EXPORT int       taos_stmt_num_params(TAOS_STMT *stmt, int *nums);
DLL_EXPORT int       taos_stmt_get_param(TAOS_STMT *stmt, int idx, int *type, int *bytes);
DLL_EXPORT int       taos_stmt_bind_param(TAOS_STMT *stmt, TAOS_BIND *bind);
DLL_EXPORT int       taos_stmt_bind_param_batch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind);
DLL_EXPORT int       taos_stmt_bind_single_param_batch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind, int colIdx);
DLL_EXPORT int       taos_stmt_add_batch(TAOS_STMT *stmt);
DLL_EXPORT int       taos_stmt_execute(TAOS_STMT *stmt);
DLL_EXPORT TAOS_RES *taos_stmt_use_result(TAOS_STMT *stmt);
DLL_EXPORT int       taos_stmt_close(TAOS_STMT *stmt);
DLL_EXPORT char     *taos_stmt_errstr(TAOS_STMT *stmt);

DLL_EXPORT TAOS_RES *taos_query(TAOS *taos, const char *sql);
DLL_EXPORT TAOS_RES *taos_query_l(TAOS *taos, const char *sql, int sqlLen);

DLL_EXPORT TAOS_ROW taos_fetch_row(TAOS_RES *res);
DLL_EXPORT int      taos_result_precision(TAOS_RES *res);  // get the time precision of result
DLL_EXPORT void     taos_free_result(TAOS_RES *res);
DLL_EXPORT int      taos_field_count(TAOS_RES *res);
DLL_EXPORT int      taos_num_fields(TAOS_RES *res);
DLL_EXPORT int      taos_affected_rows(TAOS_RES *res);

DLL_EXPORT TAOS_FIELD *taos_fetch_fields(TAOS_RES *res);
DLL_EXPORT int         taos_select_db(TAOS *taos, const char *db);
DLL_EXPORT int         taos_print_row(char *str, TAOS_ROW row, TAOS_FIELD *fields, int num_fields);
DLL_EXPORT void        taos_stop_query(TAOS_RES *res);
DLL_EXPORT bool        taos_is_null(TAOS_RES *res, int32_t row, int32_t col);
DLL_EXPORT int         taos_fetch_block(TAOS_RES *res, TAOS_ROW *rows);
DLL_EXPORT int         taos_validate_sql(TAOS *taos, const char *sql);

DLL_EXPORT int *taos_fetch_lengths(TAOS_RES *res);

DLL_EXPORT const char *taos_get_server_info(TAOS *taos);
DLL_EXPORT const char *taos_get_client_info();

DLL_EXPORT const char *taos_errstr(TAOS_RES *tres);
DLL_EXPORT int         taos_errno(TAOS_RES *tres);

DLL_EXPORT void taos_query_a(TAOS *taos, const char *sql, __taos_async_fn_t fp, void *param);
DLL_EXPORT void taos_fetch_rows_a(TAOS_RES *res, __taos_async_fn_t fp, void *param);

typedef void (*__taos_sub_fn_t)(TAOS_SUB *tsub, TAOS_RES *res, void *param, int code);
DLL_EXPORT TAOS_SUB *taos_subscribe(TAOS *taos, int restart, const char *topic, const char *sql, __taos_sub_fn_t fp,
                                    void *param, int interval);
DLL_EXPORT TAOS_RES *taos_consume(TAOS_SUB *tsub);
DLL_EXPORT void      taos_unsubscribe(TAOS_SUB *tsub, int keepProgress);

DLL_EXPORT TAOS_STREAM *taos_open_stream(TAOS *taos, const char *sql, void (*fp)(void *param, TAOS_RES *, TAOS_ROW row),
                                         int64_t stime, void *param, void (*callback)(void *));
DLL_EXPORT void         taos_close_stream(TAOS_STREAM *tstr);

DLL_EXPORT int       taos_load_table_info(TAOS *taos, const char *tableNameList);
DLL_EXPORT TAOS_RES *taos_schemaless_insert(TAOS *taos, char *lines[], int numLines, int protocol, int precision);

/* --------------------------TMQ INTERFACE------------------------------- */

enum tmq_resp_err_t {
  TMQ_RESP_ERR__FAIL = -1,
  TMQ_RESP_ERR__SUCCESS = 0,
};

typedef enum tmq_resp_err_t tmq_resp_err_t;

typedef struct tmq_t                   tmq_t;
typedef struct tmq_topic_vgroup_t      tmq_topic_vgroup_t;
typedef struct tmq_topic_vgroup_list_t tmq_topic_vgroup_list_t;

typedef struct tmq_conf_t    tmq_conf_t;
typedef struct tmq_list_t    tmq_list_t;
typedef struct tmq_message_t tmq_message_t;

typedef void(tmq_commit_cb(tmq_t *, tmq_resp_err_t, tmq_topic_vgroup_list_t *, void *param));

DLL_EXPORT tmq_list_t *tmq_list_new();
DLL_EXPORT int32_t     tmq_list_append(tmq_list_t *, const char *);

DLL_EXPORT TAOS_RES   *tmq_create_topic(TAOS *taos, const char *name, const char *sql, int sqlLen);
DLL_EXPORT tmq_t      *tmq_consumer_new(void *conn, tmq_conf_t *conf, char *errstr, int32_t errstrLen);
DLL_EXPORT void        tmq_message_destroy(tmq_message_t *tmq_message);
DLL_EXPORT const char *tmq_err2str(tmq_resp_err_t);

/* ------------------------TMQ CONSUMER INTERFACE------------------------ */
DLL_EXPORT tmq_resp_err_t tmq_subscribe(tmq_t *tmq, tmq_list_t *topic_list);
DLL_EXPORT tmq_resp_err_t tmq_unsubscribe(tmq_t *tmq);
DLL_EXPORT tmq_resp_err_t tmq_subscription(tmq_t *tmq, tmq_list_t **topics);
DLL_EXPORT tmq_message_t *tmq_consumer_poll(tmq_t *tmq, int64_t blocking_time);
DLL_EXPORT tmq_resp_err_t tmq_consumer_close(tmq_t *tmq);
#if 0
DLL_EXPORT tmq_resp_err_t tmq_assign(tmq_t* tmq, const tmq_topic_vgroup_list_t* vgroups);
DLL_EXPORT tmq_resp_err_t tmq_assignment(tmq_t* tmq, tmq_topic_vgroup_list_t** vgroups);
#endif
DLL_EXPORT tmq_resp_err_t tmq_commit(tmq_t *tmq, const tmq_topic_vgroup_list_t *offsets, int32_t async);
#if 0
DLL_EXPORT tmq_resp_err_t tmq_commit_message(tmq_t* tmq, const tmq_message_t* tmqmessage, int32_t async);
#endif
DLL_EXPORT tmq_resp_err_t tmq_seek(tmq_t *tmq, const tmq_topic_vgroup_t *offset);
/* ----------------------TMQ CONFIGURATION INTERFACE---------------------- */

enum tmq_conf_res_t {
  TMQ_CONF_UNKNOWN = -2,
  TMQ_CONF_INVALID = -1,
  TMQ_CONF_OK = 0,
};

typedef enum tmq_conf_res_t tmq_conf_res_t;

DLL_EXPORT tmq_conf_t    *tmq_conf_new();
DLL_EXPORT void           tmq_conf_destroy(tmq_conf_t *conf);
DLL_EXPORT tmq_conf_res_t tmq_conf_set(tmq_conf_t *conf, const char *key, const char *value);
DLL_EXPORT void           tmq_conf_set_offset_commit_cb(tmq_conf_t *conf, tmq_commit_cb *cb);

// temporary used function for demo only
void    tmqShowMsg(tmq_message_t *tmq_message);
int32_t tmqGetSkipLogNum(tmq_message_t *tmq_message);

/* -------------------------TMQ MSG HANDLE INTERFACE---------------------- */

DLL_EXPORT TAOS_ROW tmq_get_row(tmq_message_t *message);
DLL_EXPORT char    *tmq_get_topic_name(tmq_message_t *message);

/* ---------------------- OTHER ---------------------------- */
typedef void (*TAOS_SUBSCRIBE_CALLBACK)(TAOS_SUB *tsub, TAOS_RES *res, void *param, int code);

DLL_EXPORT int taos_stmt_affected_rows(TAOS_STMT *stmt);

#ifdef __cplusplus
}
#endif

#endif
