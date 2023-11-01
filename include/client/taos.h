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
typedef void **TAOS_ROW;
typedef void   TAOS_SUB;

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
#define TSDB_DATA_TYPE_GEOMETRY   20  // geometry
#define TSDB_DATA_TYPE_MAX        21

typedef enum {
  TSDB_OPTION_LOCALE,
  TSDB_OPTION_CHARSET,
  TSDB_OPTION_TIMEZONE,
  TSDB_OPTION_CONFIGDIR,
  TSDB_OPTION_SHELL_ACTIVITY_TIMER,
  TSDB_OPTION_USE_ADAPTER,
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

typedef struct TAOS_FIELD_E {
  char    name[65];
  int8_t  type;
  uint8_t precision;
  uint8_t scale;
  int32_t bytes;
} TAOS_FIELD_E;

#ifdef WINDOWS
#define DLL_EXPORT __declspec(dllexport)
#else
#define DLL_EXPORT
#endif

typedef void (*__taos_async_fn_t)(void *param, TAOS_RES *res, int code);
typedef void (*__taos_notify_fn_t)(void *param, void *ext, int type);

typedef struct TAOS_MULTI_BIND {
  int       buffer_type;
  void     *buffer;
  uintptr_t buffer_length;
  int32_t  *length;
  char     *is_null;
  int       num;
} TAOS_MULTI_BIND;

typedef enum {
  SET_CONF_RET_SUCC = 0,
  SET_CONF_RET_ERR_PART = -1,
  SET_CONF_RET_ERR_INNER = -2,
  SET_CONF_RET_ERR_JSON_INVALID = -3,
  SET_CONF_RET_ERR_JSON_PARSE = -4,
  SET_CONF_RET_ERR_ONLY_ONCE = -5,
  SET_CONF_RET_ERR_TOO_LONG = -6
} SET_CONF_RET_CODE;

typedef enum {
  TAOS_NOTIFY_PASSVER = 0,
  TAOS_NOTIFY_WHITELIST_VER = 1,
  TAOS_NOTIFY_USER_DROPPED = 2,
} TAOS_NOTIFY_TYPE;

#define RET_MSG_LENGTH 1024
typedef struct setConfRet {
  SET_CONF_RET_CODE retCode;
  char              retMsg[RET_MSG_LENGTH];
} setConfRet;

typedef struct TAOS_VGROUP_HASH_INFO {
  int32_t  vgId;
  uint32_t hashBegin;
  uint32_t hashEnd;
} TAOS_VGROUP_HASH_INFO;

typedef struct TAOS_DB_ROUTE_INFO {
  int32_t                routeVersion;
  int16_t                hashPrefix;
  int16_t                hashSuffix;
  int8_t                 hashMethod;
  int32_t                vgNum;
  TAOS_VGROUP_HASH_INFO *vgHash;
} TAOS_DB_ROUTE_INFO;

DLL_EXPORT void       taos_cleanup(void);
DLL_EXPORT int        taos_options(TSDB_OPTION option, const void *arg, ...);
DLL_EXPORT setConfRet taos_set_config(const char *config);
DLL_EXPORT int        taos_init(void);
DLL_EXPORT TAOS      *taos_connect(const char *ip, const char *user, const char *pass, const char *db, uint16_t port);
DLL_EXPORT TAOS *taos_connect_auth(const char *ip, const char *user, const char *auth, const char *db, uint16_t port);
DLL_EXPORT void  taos_close(TAOS *taos);

DLL_EXPORT const char *taos_data_type(int type);

DLL_EXPORT TAOS_STMT *taos_stmt_init(TAOS *taos);
DLL_EXPORT TAOS_STMT *taos_stmt_init_with_reqid(TAOS *taos, int64_t reqid);
DLL_EXPORT int        taos_stmt_prepare(TAOS_STMT *stmt, const char *sql, unsigned long length);
DLL_EXPORT int        taos_stmt_set_tbname_tags(TAOS_STMT *stmt, const char *name, TAOS_MULTI_BIND *tags);
DLL_EXPORT int        taos_stmt_set_tbname(TAOS_STMT *stmt, const char *name);
DLL_EXPORT int        taos_stmt_set_tags(TAOS_STMT *stmt, TAOS_MULTI_BIND *tags);
DLL_EXPORT int        taos_stmt_set_sub_tbname(TAOS_STMT *stmt, const char *name);
DLL_EXPORT int        taos_stmt_get_tag_fields(TAOS_STMT *stmt, int *fieldNum, TAOS_FIELD_E **fields);
DLL_EXPORT int        taos_stmt_get_col_fields(TAOS_STMT *stmt, int *fieldNum, TAOS_FIELD_E **fields);
// let stmt to reclaim TAOS_FIELD_E that was allocated by `taos_stmt_get_tag_fields`/`taos_stmt_get_col_fields`
DLL_EXPORT void taos_stmt_reclaim_fields(TAOS_STMT *stmt, TAOS_FIELD_E *fields);

DLL_EXPORT int       taos_stmt_is_insert(TAOS_STMT *stmt, int *insert);
DLL_EXPORT int       taos_stmt_num_params(TAOS_STMT *stmt, int *nums);
DLL_EXPORT int       taos_stmt_get_param(TAOS_STMT *stmt, int idx, int *type, int *bytes);
DLL_EXPORT int       taos_stmt_bind_param(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind);
DLL_EXPORT int       taos_stmt_bind_param_batch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind);
DLL_EXPORT int       taos_stmt_bind_single_param_batch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind, int colIdx);
DLL_EXPORT int       taos_stmt_add_batch(TAOS_STMT *stmt);
DLL_EXPORT int       taos_stmt_execute(TAOS_STMT *stmt);
DLL_EXPORT TAOS_RES *taos_stmt_use_result(TAOS_STMT *stmt);
DLL_EXPORT int       taos_stmt_close(TAOS_STMT *stmt);
DLL_EXPORT char     *taos_stmt_errstr(TAOS_STMT *stmt);
DLL_EXPORT int       taos_stmt_affected_rows(TAOS_STMT *stmt);
DLL_EXPORT int       taos_stmt_affected_rows_once(TAOS_STMT *stmt);

DLL_EXPORT TAOS_RES *taos_query(TAOS *taos, const char *sql);
DLL_EXPORT TAOS_RES *taos_query_with_reqid(TAOS *taos, const char *sql, int64_t reqId);

DLL_EXPORT TAOS_ROW taos_fetch_row(TAOS_RES *res);
DLL_EXPORT int      taos_result_precision(TAOS_RES *res);  // get the time precision of result
DLL_EXPORT void     taos_free_result(TAOS_RES *res);
DLL_EXPORT void     taos_kill_query(TAOS *taos);
DLL_EXPORT int      taos_field_count(TAOS_RES *res);
DLL_EXPORT int      taos_num_fields(TAOS_RES *res);
DLL_EXPORT int      taos_affected_rows(TAOS_RES *res);
DLL_EXPORT int64_t  taos_affected_rows64(TAOS_RES *res);

DLL_EXPORT TAOS_FIELD *taos_fetch_fields(TAOS_RES *res);
DLL_EXPORT int         taos_select_db(TAOS *taos, const char *db);
DLL_EXPORT int         taos_print_row(char *str, TAOS_ROW row, TAOS_FIELD *fields, int num_fields);
DLL_EXPORT void        taos_stop_query(TAOS_RES *res);
DLL_EXPORT bool        taos_is_null(TAOS_RES *res, int32_t row, int32_t col);
DLL_EXPORT bool        taos_is_update_query(TAOS_RES *res);
DLL_EXPORT int         taos_fetch_block(TAOS_RES *res, TAOS_ROW *rows);
DLL_EXPORT int         taos_fetch_block_s(TAOS_RES *res, int *numOfRows, TAOS_ROW *rows);
DLL_EXPORT int         taos_fetch_raw_block(TAOS_RES *res, int *numOfRows, void **pData);
DLL_EXPORT int        *taos_get_column_data_offset(TAOS_RES *res, int columnIndex);
DLL_EXPORT int         taos_validate_sql(TAOS *taos, const char *sql);
DLL_EXPORT void        taos_reset_current_db(TAOS *taos);

DLL_EXPORT int      *taos_fetch_lengths(TAOS_RES *res);
DLL_EXPORT TAOS_ROW *taos_result_block(TAOS_RES *res);

DLL_EXPORT const char *taos_get_server_info(TAOS *taos);
DLL_EXPORT const char *taos_get_client_info();
DLL_EXPORT int         taos_get_current_db(TAOS *taos, char *database, int len, int *required);

DLL_EXPORT const char *taos_errstr(TAOS_RES *res);
DLL_EXPORT int         taos_errno(TAOS_RES *res);

DLL_EXPORT void taos_query_a(TAOS *taos, const char *sql, __taos_async_fn_t fp, void *param);
DLL_EXPORT void taos_query_a_with_reqid(TAOS *taos, const char *sql, __taos_async_fn_t fp, void *param, int64_t reqid);
DLL_EXPORT void taos_fetch_rows_a(TAOS_RES *res, __taos_async_fn_t fp, void *param);
DLL_EXPORT void taos_fetch_raw_block_a(TAOS_RES *res, __taos_async_fn_t fp, void *param);
DLL_EXPORT const void *taos_get_raw_block(TAOS_RES *res);

DLL_EXPORT int taos_get_db_route_info(TAOS *taos, const char *db, TAOS_DB_ROUTE_INFO *dbInfo);
DLL_EXPORT int taos_get_table_vgId(TAOS *taos, const char *db, const char *table, int *vgId);
DLL_EXPORT int taos_get_tables_vgId(TAOS *taos, const char *db, const char *table[], int tableNum, int *vgId);

DLL_EXPORT int taos_load_table_info(TAOS *taos, const char *tableNameList);

// set heart beat thread quit mode , if quicByKill 1 then kill thread else quit from inner
DLL_EXPORT void taos_set_hb_quit(int8_t quitByKill);

DLL_EXPORT int taos_set_notify_cb(TAOS *taos, __taos_notify_fn_t fp, void *param, int type);

typedef void (*__taos_async_whitelist_fn_t)(void *param, int code, TAOS *taos, int numOfWhiteLists, uint64_t* pWhiteLists);
DLL_EXPORT void taos_fetch_whitelist_a(TAOS *taos, __taos_async_whitelist_fn_t fp, void *param);

typedef enum {
  TAOS_CONN_MODE_BI = 0,
} TAOS_CONN_MODE;

DLL_EXPORT int taos_set_conn_mode(TAOS* taos, int mode, int value);
/*  --------------------------schemaless INTERFACE------------------------------- */

DLL_EXPORT TAOS_RES *taos_schemaless_insert(TAOS *taos, char *lines[], int numLines, int protocol, int precision);
DLL_EXPORT TAOS_RES *taos_schemaless_insert_with_reqid(TAOS *taos, char *lines[], int numLines, int protocol,
                                                       int precision, int64_t reqid);
DLL_EXPORT TAOS_RES *taos_schemaless_insert_raw(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol,
                                                int precision);
DLL_EXPORT TAOS_RES *taos_schemaless_insert_raw_with_reqid(TAOS *taos, char *lines, int len, int32_t *totalRows,
                                                           int protocol, int precision, int64_t reqid);
DLL_EXPORT TAOS_RES *taos_schemaless_insert_ttl(TAOS *taos, char *lines[], int numLines, int protocol, int precision,
                                                int32_t ttl);
DLL_EXPORT TAOS_RES *taos_schemaless_insert_ttl_with_reqid(TAOS *taos, char *lines[], int numLines, int protocol,
                                                           int precision, int32_t ttl, int64_t reqid);
DLL_EXPORT TAOS_RES *taos_schemaless_insert_raw_ttl(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol,
                                                    int precision, int32_t ttl);
DLL_EXPORT TAOS_RES *taos_schemaless_insert_raw_ttl_with_reqid(TAOS *taos, char *lines, int len, int32_t *totalRows,
                                                               int protocol, int precision, int32_t ttl, int64_t reqid);

/* --------------------------TMQ INTERFACE------------------------------- */

typedef struct tmq_t      tmq_t;
typedef struct tmq_conf_t tmq_conf_t;
typedef struct tmq_list_t tmq_list_t;

typedef void(tmq_commit_cb(tmq_t *tmq, int32_t code, void *param));

typedef enum tmq_conf_res_t {
  TMQ_CONF_UNKNOWN = -2,
  TMQ_CONF_INVALID = -1,
  TMQ_CONF_OK = 0,
} tmq_conf_res_t;

typedef enum tmq_res_t {
  TMQ_RES_INVALID = -1,
  TMQ_RES_DATA = 1,
  TMQ_RES_TABLE_META = 2,
  TMQ_RES_METADATA = 3,
} tmq_res_t;

typedef struct tmq_topic_assignment {
  int32_t vgId;
  int64_t currentOffset;
  int64_t begin;
  int64_t end;
} tmq_topic_assignment;

DLL_EXPORT tmq_conf_t    *tmq_conf_new();
DLL_EXPORT tmq_conf_res_t tmq_conf_set(tmq_conf_t *conf, const char *key, const char *value);
DLL_EXPORT void           tmq_conf_destroy(tmq_conf_t *conf);
DLL_EXPORT void           tmq_conf_set_auto_commit_cb(tmq_conf_t *conf, tmq_commit_cb *cb, void *param);

DLL_EXPORT tmq_list_t *tmq_list_new();
DLL_EXPORT int32_t     tmq_list_append(tmq_list_t *, const char *);
DLL_EXPORT void        tmq_list_destroy(tmq_list_t *);
DLL_EXPORT int32_t     tmq_list_get_size(const tmq_list_t *);
DLL_EXPORT char      **tmq_list_to_c_array(const tmq_list_t *);

DLL_EXPORT tmq_t    *tmq_consumer_new(tmq_conf_t *conf, char *errstr, int32_t errstrLen);
DLL_EXPORT int32_t   tmq_subscribe(tmq_t *tmq, const tmq_list_t *topic_list);
DLL_EXPORT int32_t   tmq_unsubscribe(tmq_t *tmq);
DLL_EXPORT int32_t   tmq_subscription(tmq_t *tmq, tmq_list_t **topics);
DLL_EXPORT TAOS_RES *tmq_consumer_poll(tmq_t *tmq, int64_t timeout);
DLL_EXPORT int32_t   tmq_consumer_close(tmq_t *tmq);
DLL_EXPORT int32_t   tmq_commit_sync(tmq_t *tmq, const TAOS_RES *msg); //Commit the msg’s offset + 1
DLL_EXPORT void      tmq_commit_async(tmq_t *tmq, const TAOS_RES *msg, tmq_commit_cb *cb, void *param);
DLL_EXPORT int32_t   tmq_commit_offset_sync(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset);
DLL_EXPORT void      tmq_commit_offset_async(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset, tmq_commit_cb *cb, void *param);
DLL_EXPORT int32_t   tmq_get_topic_assignment(tmq_t *tmq, const char *pTopicName, tmq_topic_assignment **assignment,int32_t *numOfAssignment);
DLL_EXPORT void      tmq_free_assignment(tmq_topic_assignment* pAssignment);
DLL_EXPORT int32_t   tmq_offset_seek(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset);
DLL_EXPORT int64_t   tmq_position(tmq_t *tmq, const char *pTopicName, int32_t vgId);  // The current offset is the offset of the last consumed message + 1
DLL_EXPORT int64_t   tmq_committed(tmq_t *tmq, const char *pTopicName, int32_t vgId);

DLL_EXPORT TAOS       *tmq_get_connect(tmq_t *tmq);
DLL_EXPORT const char *tmq_get_table_name(TAOS_RES *res);
DLL_EXPORT tmq_res_t   tmq_get_res_type(TAOS_RES *res);
DLL_EXPORT const char *tmq_get_topic_name(TAOS_RES *res);
DLL_EXPORT const char *tmq_get_db_name(TAOS_RES *res);
DLL_EXPORT int32_t     tmq_get_vgroup_id(TAOS_RES *res);
DLL_EXPORT int64_t     tmq_get_vgroup_offset(TAOS_RES* res);
DLL_EXPORT const char *tmq_err2str(int32_t code);

/* ------------------------------ TAOSX -----------------------------------*/
typedef struct tmq_raw_data {
  void    *raw;
  uint32_t raw_len;
  uint16_t raw_type;
} tmq_raw_data;

DLL_EXPORT int32_t     tmq_get_raw(TAOS_RES *res, tmq_raw_data *raw);
DLL_EXPORT int32_t     tmq_write_raw(TAOS *taos, tmq_raw_data raw);
DLL_EXPORT int         taos_write_raw_block(TAOS *taos, int numOfRows, char *pData, const char *tbname);
DLL_EXPORT int         taos_write_raw_block_with_reqid(TAOS *taos, int numOfRows, char *pData, const char *tbname, int64_t reqid);
DLL_EXPORT int         taos_write_raw_block_with_fields(TAOS *taos, int rows, char *pData, const char *tbname,
                                                        TAOS_FIELD *fields, int numFields);
DLL_EXPORT int         taos_write_raw_block_with_fields_with_reqid(TAOS *taos, int rows, char *pData, const char *tbname,
                                                        TAOS_FIELD *fields, int numFields, int64_t reqid);
DLL_EXPORT void        tmq_free_raw(tmq_raw_data raw);

// Returning null means error. Returned result need to be freed by tmq_free_json_meta
DLL_EXPORT char *tmq_get_json_meta(TAOS_RES *res);
DLL_EXPORT void  tmq_free_json_meta(char *jsonMeta);
/* ---------------------------- TAOSX END -------------------------------- */

typedef enum {
  TSDB_SRV_STATUS_UNAVAILABLE = 0,
  TSDB_SRV_STATUS_NETWORK_OK = 1,
  TSDB_SRV_STATUS_SERVICE_OK = 2,
  TSDB_SRV_STATUS_SERVICE_DEGRADED = 3,
  TSDB_SRV_STATUS_EXTING = 4,
} TSDB_SERVER_STATUS;

DLL_EXPORT TSDB_SERVER_STATUS taos_check_server_status(const char *fqdn, int port, char *details, int maxlen);

#ifdef __cplusplus
}
#endif

#endif
