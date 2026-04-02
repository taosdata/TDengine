/*
 * MIT License
 *
 * Copyright (c) 2022-2023 freemine <freemine@yeah.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#ifndef _internal_h_
#define _internal_h_

#include "taos_odbc_config.h"

#include "os_port.h"
#include "enums.h"

#include "list.h"
#include "utils.h"

#include "typedefs.h"

#include "parser.h"

#include "taos_helpers.h"
#ifdef HAVE_TAOSWS           /* { */
#include "taosws_helpers.h"
#endif                       /* } */

EXTERN_C_BEGIN

// <TDengine/ include/util/tdef.h
// ...
// #define TSDB_NODE_NAME_LEN  64
// #define TSDB_TABLE_NAME_LEN 193  // it is a null-terminated string
// #define TSDB_TOPIC_NAME_LEN 193  // it is a null-terminated string
// #define TSDB_CGROUP_LEN     193  // it is a null-terminated string
// #define TSDB_DB_NAME_LEN    65
// #define TSDB_DB_FNAME_LEN   (TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN + TSDB_NAME_DELIMITER_LEN)
// ...
// #define TSDB_COL_NAME_LEN        65
// ...

#define MAX_CATALOG_NAME_LEN        64
#define MAX_SCHEMA_NAME_LEN         64
#define MAX_TABLE_NAME_LEN         192
#define MAX_COLUMN_NAME_LEN         64

struct err_s {
  int                         err;
  const char                 *estr;
  SQLCHAR                     sql_state[6];

  char                        buf[1024];

  struct tod_list_head        node;
};

struct errs_s {
  struct tod_list_head        errs;
  struct tod_list_head        frees;

  size_t                      count;
  conn_t                     *connected_conn; // NOTE: no ownership
};

#define conn_data_source(_conn) _conn->cfg.dsn ? _conn->cfg.dsn : (_conn->cfg.driver ? _conn->cfg.driver : "")

#define env_append_err(_env, _sql_state, _e, _estr) errs_append(&_env->errs, _sql_state, _e, _estr)

#define env_append_err_format(_env, _sql_state, _e, _fmt, ...) errs_append_format(&_env->errs, _sql_state, _e, _fmt, ##__VA_ARGS__)

#define env_oom(_env) errs_oom(&_env->errs)

#define conn_append_err(_conn, _sql_state, _e, _estr) errs_append(&_conn->errs, _sql_state, _e, _estr)

#define conn_append_err_format(_conn, _sql_state, _e, _fmt, ...) errs_append_format(&_conn->errs, _sql_state, _e, _fmt, ##__VA_ARGS__)

#define conn_oom(_conn) errs_oom(&_conn->errs)

#define conn_niy(_conn) errs_niy(&_conn->errs)

#define conn_nsy(_conn) errs_nsy(&_conn->errs)

#define stmt_append_err(_stmt, _sql_state, _e, _estr) errs_append(&_stmt->errs, _sql_state, _e, _estr)

#define stmt_append_err_format(_stmt, _sql_state, _e, _fmt, ...) errs_append_format(&_stmt->errs, _sql_state, _e, _fmt, ##__VA_ARGS__)

#define stmt_oom(_stmt) errs_oom(&_stmt->errs)

#define stmt_niy(_stmt) errs_niy(&_stmt->errs)

#define stmt_nsy(_stmt) errs_nsy(&_stmt->errs)

#define desc_append_err(_desc, _sql_state, _e, _estr) errs_append(&_desc->errs, _sql_state, _e, _estr)

#define desc_append_err_format(_desc, _sql_state, _e, _fmt, ...) errs_append_format(&_desc->errs, _sql_state, _e, _fmt, ##__VA_ARGS__)

#define desc_oom(_desc) errs_oom(&_desc->errs)

#define desc_niy(_desc) errs_niy(&_desc->errs)

#define desc_nsy(_desc) errs_nsy(&_desc->errs)


static inline int sql_succeeded(SQLRETURN sr)
{
  return sr == SQL_SUCCESS || sr == SQL_SUCCESS_WITH_INFO;
}

enum var_e {
  VAR_NULL,
  VAR_BOOL,
  VAR_INT8,
  VAR_INT16,
  VAR_INT32,
  VAR_INT64,
  VAR_UINT8,
  VAR_UINT16,
  VAR_UINT32,
  VAR_UINT64,
  VAR_FLOAT,
  VAR_DOUBLE,
  VAR_ID,
  VAR_STRING,
  VAR_PARAM,
  VAR_ARR,
  VAR_EVAL,
};

typedef var_t* (*var_eval_f)(var_t *args);

enum var_quote_e {
  QUOTE_DQ,   // double quote, quotation mark
  QUOTE_SQ,   // single quote, apostrophe
  QUOTE_BQ,   // back quote, back tick
};

struct var_s {
  var_e                 type;
  union {
    bool                b;
    int8_t              i8;
    int16_t             i16;
    int32_t             i32;
    int64_t             i64;
    uint8_t             u8;
    uint16_t            u16;
    uint32_t            u32;
    uint64_t            u64;
    struct {
      float             v;
      str_t             s;
    } flt;
    struct {
      double            v;
      str_t             s;
    } dbl;
    struct {
      str_t             s;
      var_quote_e       q;
    } str;
    struct {
      var_t           **vals;
      size_t            cap;
      size_t            nr;
    } arr;
    struct {
      var_eval_f        eval;
      var_t            *args;
    } exp;
  };
};

struct sqlc_tsdb_s {
  const char           *sqlc;              // NOTE: no ownership
  size_t                sqlc_bytes;
  const char           *tsdb;
  size_t                tsdb_bytes;        // NOTE: no ownership

  int32_t               qms;
};

struct sqlc_data_s {
  // https://learn.microsoft.com/en-us/sql/odbc/reference/appendixes/c-data-types?view=sql-server-ver16
  SQLSMALLINT           type;
  union {
    uint8_t             b;
    int8_t              i8;
    uint8_t             u8;
    int16_t             i16;
    uint16_t            u16;
    int32_t             i32;
    uint32_t            u32;
    int64_t             i64;
    uint64_t            u64;
    float               flt;
    double              dbl;
    struct {
      const char *str;
      size_t      len;
    }                   str;
    struct {
      // UCS-2LE
      const char *wstr;
      size_t      wlen; // in characters
    }                   wstr;
    struct {
      const unsigned char *bin;
      size_t               len;
    }                   bin;
    int64_t             ts;
  };

  mem_t          mem;
  size_t         len;
  size_t         cur;

  uint8_t               is_null:1;
};

struct sql_data_s {
  SQLSMALLINT         type;
  union {
    uint8_t             b;
    int8_t              i8;
    uint8_t             u8;
    int16_t             i16;
    uint16_t            u16;
    int32_t             i32;
    uint32_t            u32;
    int64_t             i64;
    uint64_t            u64;
    float               flt;
    double              dbl;
    struct {
      const char *str;
      size_t      len;
    }                   str;
    struct {
      // UCS-2LE
      const char *wstr;
      size_t      wlen; // in characters
    }                   wstr;
    struct {
      int64_t           sec;
      int64_t           nsec;
      int64_t           i64;
      uint8_t           is_i64:1;
    }                   ts;
    struct {
      const unsigned char *bin;
      size_t               len;
    }                   bin;
  };

  mem_t          mem;
  size_t         len;
  size_t         cur;

  uint8_t               is_null:1;
  uint8_t               unsigned_:1;
};

struct env_s {
  atomic_int          refc;

  atomic_int          conns;

  errs_t              errs;

  mem_t               mem;

  unsigned int        debug_flex:1;
  unsigned int        debug_bison:1;
};

// https://github.com/MicrosoftDocs/sql-docs/blob/live/docs/odbc/reference/develop-app/descriptor-field-conformance.md

struct desc_header_s {
  // header fields settable by SQLSetStmtAttr
  SQLULEN             DESC_ARRAY_SIZE;
  SQLUSMALLINT       *DESC_ARRAY_STATUS_PTR;
  SQLULEN            *DESC_BIND_OFFSET_PTR;
  SQLULEN             DESC_BIND_TYPE;
  SQLULEN            *DESC_ROWS_PROCESSED_PTR;

  // header fields else
  SQLUSMALLINT        DESC_COUNT;
  // SQL_DESC_ALLOC_TYPE
};

struct get_data_ctx_s {
  SQLUSMALLINT   Col_or_Param_Num;
  SQLSMALLINT    TargetType;

  tsdb_data_t    tsdb;
  sqlc_data_t    sqlc;

  char           residual[64];
  size_t         curr;
  size_t         end;

  //
  char           buf[64];
  mem_t          mem;

  const char    *pos;
  size_t         nr;
};

typedef struct tsdb_param_column_s         tsdb_param_column_t;
struct tsdb_param_column_s {
  mem_t           mem;
  mem_t           mem_length;
  mem_t           mem_is_null;

  TAOS_FIELD_E    tsdb_field;
};

typedef struct tsdb_paramset_s             tsdb_paramset_t;
struct tsdb_paramset_s {
  int                   cap;
  int                   nr;
  tsdb_param_column_t  *params;
};

struct tsdb_binds_s {
  int                        cap;
  int                        nr;

  TAOS_MULTI_BIND           *mbs;
};

struct desc_record_s {
  SQLLEN                       *DESC_INDICATOR_PTR;
  SQLLEN                       *DESC_OCTET_LENGTH_PTR;
  SQLLEN                        DESC_PARAMETER_TYPE;

  SQLLEN                        DESC_AUTO_UNIQUE_VALUE;
  SQLCHAR                       DESC_BASE_COLUMN_NAME[192+1];
  SQLCHAR                       DESC_BASE_TABLE_NAME[192+1];
  SQLLEN                        DESC_CASE_SENSITIVE;
  SQLCHAR                       DESC_CATALOG_NAME[192+1];
  SQLLEN                        DESC_CONCISE_TYPE;
  SQLPOINTER                    DESC_DATA_PTR;
  SQLLEN                        DESC_COUNT;
  SQLLEN                        DESC_DISPLAY_SIZE;
  SQLLEN                        DESC_FIXED_PREC_SCALE;
  SQLCHAR                       DESC_LABEL[192+1];
  SQLLEN                        DESC_LENGTH;
  SQLCHAR                       DESC_LITERAL_PREFIX[128+1];
  SQLCHAR                       DESC_LITERAL_SUFFIX[128+1];
  SQLCHAR                       DESC_LOCAL_TYPE_NAME[128+1];
  SQLCHAR                       DESC_NAME[192+1];
  SQLLEN                        DESC_NULLABLE;
  SQLLEN                        DESC_NUM_PREC_RADIX;
  SQLLEN                        DESC_OCTET_LENGTH;
  SQLLEN                        DESC_PRECISION;
  SQLLEN                        DESC_SCALE;
  SQLCHAR                       DESC_SCHEMA_NAME[192+1];
  SQLLEN                        DESC_SEARCHABLE;
  SQLCHAR                       DESC_TABLE_NAME[192+1];
  SQLLEN                        DESC_TYPE;
  SQLCHAR                       DESC_TYPE_NAME[64+1];
  SQLLEN                        DESC_UNNAMED;
  SQLLEN                        DESC_UNSIGNED;
  SQLLEN                        DESC_UPDATABLE;


  // SQL_DESC_DATETIME_INTERVAL_CODE
  // SQL_DESC_DATETIME_INTERVAL_PRECISION


  int                           tsdb_type;



  unsigned int                  bound:1;
};

struct descriptor_s {
  desc_header_t                 header;

  desc_record_t                *records;
  size_t                        cap;
};

struct desc_s {
  atomic_int                    refc;

  descriptor_t                  descriptor;

  conn_t                       *conn;

  // https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/types-of-descriptors?view=sql-server-ver16
  // `A row descriptor in one statement can serve as a parameter descriptor in another statement.`
  struct tod_list_head          associated_stmts_as_ARD; // struct stmt_s*
  struct tod_list_head          associated_stmts_as_APD; // struct stmt_s*

  errs_t                        errs;
};

struct charset_conv_s {
  charset_name_t      from;
  charset_name_t      to;
  iconv_t             cnv;
};

struct charset_conv_mgr_s {
  hash_table_t               *convs;
};

typedef enum backend_e                backend_e;
enum backend_e {
  BACKEND_TAOS,
  BACKEND_TAOSWS,
};

enum custprod_type_e {
  CUSTP_GENERAL           = 0,
  CUSTP_KINGSCADA         = 1,
  CUSTP_KEPWARE           = 2,
  CUSTP_ADO               = 3,
};

struct custprod_item_s {
  const char *name;
  custprod_type_e custprod_type;
};

struct conn_cfg_s {
  char                  *driver;
  char                  *dsn;

  backend_e              backend;

  char                  *url;

  char                  *uid;
  char                  *pwd;
  char                  *ip;
  char                  *db;

  char                  *charset_for_col_bind;
  char                  *charset_for_param_bind;
  int                    port;

  char                  *customproduct_name;
  custprod_type_e        customproduct;

  // NOTE: 1.this is to hack node.odbc, which maps SQL_TINYINT to SQL_C_UTINYINT
  //       2.node.odbc does not call SQLGetInfo/SQLColAttribute to get signess of integers
  unsigned int           unsigned_promotion:1;
  // NOTE: this is to hack PowerBI, which seems not displace seconds-fractional,
  //       thus, if timestamp_as_is is not set, TSDB_DATA_TYPE_TIMESTAMP would map to SQL_WVARCHAR
  unsigned int           timestamp_as_is:1;

  // NOTE: default is 0, BI mode : 1
  unsigned int           conn_mode:1;
};

struct sqls_parser_nterm_s {
  size_t           start;
  size_t           end;

  int32_t          qms;
};

struct sqls_s {
  sqls_parser_nterm_t   *sqls;
  size_t                 cap;
  size_t                 nr;

  size_t                 pos; // 1-based

  uint8_t                failed:1;
};

struct url_s {
  char                  *scheme;     // NOTE: without tail ':'

  char                  *user;       // NOTE: decoded-form
  char                  *pass;       // NOTE: decoded-form

  char                  *host;       // NOTE: decoded-form
  uint16_t               port;

  // NOTE: undecoded yet
  char                  *path;
  char                  *query;      // NOTE: without head '?'
  char                  *fragment;   // NOTE: without head '#'
};

struct topic_cfg_s {
  char                 **names;
  size_t                 names_cap;
  size_t                 names_nr;

  kvs_t                  kvs;
};

struct conn_parser_param_s {
  conn_cfg_t            *conn_cfg;
  int (*init)(conn_cfg_t *conn_cfg, char *buf, size_t len);

  parser_ctx_t           ctx;
};

struct insert_eval_s {
  var_t             *table_db;
  var_t             *table_tbl;
  var_t             *super_db;
  var_t             *super_tbl;
  var_t             *tag_names;    // NOTE: nr_tags == tag_names ? tag_names->arr.nr : 0
  var_t             *tag_vals;
  var_t             *col_names;    // NOTE: nr_cols == col_names ? col_names->arr.nr : 0
  var_t             *col_vals;

  int8_t             nr_params;    // NOTE: count of '?'
  uint8_t            tbl_param:1;  // NOTE: if table_name is '?'
};

struct ext_parser_param_s {
  union {
    topic_cfg_t          topic_cfg;
    insert_eval_t        insert_eval;
  };

  parser_ctx_t           ctx;
  uint8_t                is_topic:1;
};

struct sqls_parser_param_s {
  int (*sql_found)(sqls_parser_param_t *param, size_t start, size_t end, int32_t qms, void *arg);
  void                  *arg;

  parser_ctx_t           ctx;
};

struct url_parser_param_s {
  parser_ctx_t           ctx;
  url_t                  url;
};

struct ts_parser_param_s {
  parser_ctx_t           ctx;

  time_t                 tm_utc0;

  // breakdown
  unsigned long long     frac_nano;
  int64_t                tz_seconds;
  uint8_t                decimal_digits;
};


struct charset_convs_s {
  charset_conv_t            *cnv_from_sqlc_charset_for_param_bind_to_wchar;
  charset_conv_t            *cnv_from_wchar_to_wchar;
  charset_conv_t            *cnv_from_wchar_to_sqlc;
  charset_conv_t            *cnv_from_sqlc_charset_for_param_bind_to_tsdb;
  charset_conv_t            *cnv_from_wchar_to_tsdb;
};

struct ds_err_s {
  int                err;
  char               str[1024];
};

struct ds_conn_s {
  conn_t                *conn;
  void                  *taos;
  int         (*query)           (ds_conn_t *ds_conn, const char *sql, ds_res_t *ds_res);
  const char* (*get_server_info) (ds_conn_t *ds_conn);
  const char* (*get_client_info) (ds_conn_t *ds_conn);
  int         (*get_current_db)  (ds_conn_t *ds_conn, char *db, size_t len, ds_err_t *ds_err);
  void        (*close)           (ds_conn_t *ds_conn);

  int         (*stmt_init)       (ds_conn_t *ds_conn, ds_stmt_t *ds_stmt);
};

struct ds_fields_s {
  ds_res_t              *ds_res;
  const void            *fields;

  int8_t  (*field_type)   (ds_fields_t *ds_fields, int i_col);

  size_t                 nr_fields;
};

struct ds_block_s {
  ds_res_t              *ds_res;

  int  (*get_into_tsdb)(ds_block_t *ds_block, int i_row, int i_col, tsdb_data_t *tsdb, ds_err_t *ds_err);

  int                    nr_rows_in_block;
  const void            *block;
};

struct ds_res_s {
  ds_conn_t             *ds_conn;
  void                  *res;

  void        (*close)               (ds_res_t *ds_res);
  int         (*xerrno)              (ds_res_t *ds_res);
  const char* (*errstr)              (ds_res_t *ds_res);
  int         (*fetch_block)         (ds_res_t *ds_res);

  int                    result_precision;

  ds_fields_t            fields;
  ds_block_t             block;
};

struct ds_stmt_s {
  ds_conn_t              *ds_conn;
  void                   *stmt;

  void        (*close)       (ds_stmt_t *ds_stmt);
  int         (*prepare)     (ds_stmt_t *ds_stmt, const char *sql);
};

struct conn_s {
  atomic_int          refc;
  atomic_int          descs;
  atomic_int          outstandings;

  size_t                   nr_stmts;
  struct tod_list_head     stmts;

  env_t              *env;

  conn_cfg_t          cfg;

  // server info
  const char         *svr_info;
  // client-side-timezone, which is set via `taos.cfg`
  // we use 'select to_iso8601(0)' to get the timezone info per connection
  // currently, we just get this info but not use it in anyway
  // all timestamp would be converted into SQL_C_CHAR/WCHAR according to local-timezone of your machine, via system-call `localtime_r`
  // which is the ODBC convention we believe
  // if you really wanna map timestamp to timezone that is different, you might SQLGetData(...SQL_C_BIGINT...) to get the raw int64_t of timestamp,
  // whose main part (excluding seconds fraction) represents the time in seconds since the Epoch (00:00:00 UTC, January 1, 1970)
  int64_t             tz;         // +0800 for Asia/Shanghai
  int64_t             tz_seconds; // +28800 for Asia/Shanghai

  // config from information_schema.ins_configs
  char               *s_statusInterval;
  char               *s_timezone; // this is server-side timezone
  char               *s_locale;
  char               *s_charset;

  // NOTE: big enough?
  charset_name_t      sqlc_charset;
  charset_name_t      tsdb_charset;

  errs_t              errs;

  ds_conn_t           ds_conn;

#ifdef _WIN32           /* { */
  HWND                win_handle;
#endif                  /* } */
  int32_t             txn_isolation;
  SQLUINTEGER         login_timeout;

  unsigned int        fmt_time:1;
  unsigned int        dead:1;
};

struct stmt_get_data_args_s {
  SQLUSMALLINT   Col_or_Param_Num;
  SQLSMALLINT    TargetType;
  SQLPOINTER     TargetValuePtr;
  SQLLEN         BufferLength;
  SQLLEN        *StrLenPtr;
  SQLLEN        *IndPtr;
};

struct stmt_base_s {
  SQLRETURN (*prepare)(stmt_base_t *base, const sqlc_tsdb_t *sqlc_tsdb);
  SQLRETURN (*execute)(stmt_base_t *base);
  SQLRETURN (*get_col_fields)(stmt_base_t *base, TAOS_FIELD **fields, size_t *nr);
  SQLRETURN (*fetch_row)(stmt_base_t *base);
  SQLRETURN (*more_results)(stmt_base_t *base);
  SQLRETURN (*describe_param)(stmt_base_t *base,
      SQLUSMALLINT    ParameterNumber,
      SQLSMALLINT    *DataTypePtr,
      SQLULEN        *ParameterSizePtr,
      SQLSMALLINT    *DecimalDigitsPtr,
      SQLSMALLINT    *NullablePtr);
  SQLRETURN (*get_num_params)(stmt_base_t *base, SQLSMALLINT *ParameterCountPtr);
  SQLRETURN (*tsdb_field_by_param)(stmt_base_t *base, int i_param, TAOS_FIELD_E **field);
  SQLRETURN (*row_count)(stmt_base_t *base, SQLLEN *row_count_ptr);
  SQLRETURN (*get_num_cols)(stmt_base_t *base, SQLSMALLINT *ColumnCountPtr);
  SQLRETURN (*get_data)(stmt_base_t *base, SQLUSMALLINT Col_or_Param_Num, tsdb_data_t *tsdb);
};

struct tsdb_fields_s {
  TAOS_FIELD                *fields;
  size_t                     nr;
};

struct tsdb_rows_block_s {
  TAOS_ROW            rows;
  const void         *ws_ptr;        // NOTE: libtaosws, ugly for the moment!!!
  size_t              nr;
  size_t              pos;           // 1-based
};

struct tsdb_res_s {
  tsdb_stmt_t               *owner;
  TAOS_RES                  *res;
  size_t                     affected_row_count;
  int                        time_precision;
  tsdb_fields_t              fields;
  tsdb_rows_block_t          rows_block;

  unsigned int               res_is_from_taos_query:1;
  unsigned int               eof:1;
};

struct tsdb_params_s {
  tsdb_stmt_t                        *owner;
  char                               *subtbl;
  TAOS_FIELD_E                       *tag_fields;
  int                                 nr_tag_fields;
  TAOS_FIELD_E                       *col_fields;
  int                                 nr_col_fields;

  int                                 nr_params;

  int32_t                             qms; // NOTE: qms_from_sql_parsed_by_taos_odbc;

  unsigned int                        subtbl_required:1;
};

struct tsdb_stmt_s {
  stmt_base_t                base;

  stmt_t                    *owner;

  const sqlc_tsdb_t         *current_sql;

  TAOS_STMT                 *stmt;
  // for insert-parameterized-statement
  tsdb_params_t              params;

  tsdb_binds_t               binds;

  tsdb_res_t                 res;

  unsigned int               prepared:1;
  unsigned int               is_ext:1;
  unsigned int               is_insert_stmt:1;
};

struct topic_s {
  stmt_base_t                base;
  stmt_t                    *owner;

  char                       name[193];
  topic_cfg_t                cfg;

  int64_t                    records_max;
  int64_t                    seconds_max;

  int64_t                    records_count;
  time_t                     t0;

  tmq_conf_t                *conf;
  tmq_t                     *tmq;

  TAOS_RES                  *res;
  mem_t                      res_topic_name;
  mem_t                      res_db_name;
  int32_t                    res_vgroup_id;

  TAOS_FIELD                *fields;
  size_t                     fields_cap;
  size_t                     fields_nr;

  TAOS_ROW                   row;

  uint8_t                    subscribed:1;
  uint8_t                    do_not_commit:1;
};

struct tables_args_s {
  wildex_t        *catalog_pattern;
  wildex_t        *schema_pattern;
  wildex_t        *table_pattern;
  wildex_t        *type_pattern;
  char             db[1024];    // already big enough! see TSDB_DB_NAME_LEN(65) in tdef.h
  uint8_t          select_current_db:1;
};

enum tables_type_e {
  TABLES_FOR_GENERIC,
  TABLES_FOR_CATALOGS,
  TABLES_FOR_SCHEMAS,
  TABLES_FOR_TABLETYPES,
};

struct tables_s {
  stmt_base_t                base;
  stmt_t                    *owner;

  tables_args_t              tables_args;

  mem_t                      tsdb_stmt;
  tsdb_stmt_t                stmt;

  const unsigned char       *catalog;
  const unsigned char       *schema;
  const unsigned char       *table;
  const unsigned char       *type;

  mem_t                      catalog_cache;
  mem_t                      schema_cache;
  mem_t                      table_cache;
  mem_t                      type_cache;

  mem_t                      table_types;

  tables_type_e              tables_type;
};

struct columns_args_s {
  wildex_t        *catalog_pattern;
  wildex_t        *schema_pattern;
  wildex_t        *table_pattern;
  wildex_t        *column_pattern;
};

struct columns_s {
  stmt_base_t                base;
  stmt_t                    *owner;

  columns_args_t             columns_args;

  tables_t                   tables;

  tsdb_data_t                current_catalog;
  tsdb_data_t                current_schema;
  tsdb_data_t                current_table;
  tsdb_data_t                current_table_type;

  tsdb_data_t                current_col_name;
  tsdb_data_t                current_col_type;
  tsdb_data_t                current_col_length;
  tsdb_data_t                current_col_note;

  mem_t                      tsdb_desc;
  tsdb_stmt_t                desc;      // desc <catalog>.<table_name>

  mem_t                      tsdb_query;
  tsdb_stmt_t                query;     // select * from <catalog>.<table_name>

  int                        ordinal_order;

  mem_t                      column_cache;
};

struct primarykeys_args_s {
  wildex_t        *catalog_pattern;
  wildex_t        *schema_pattern;
  wildex_t        *table_pattern;
};

struct primarykeys_s {
  stmt_base_t                base;
  stmt_t                    *owner;

  primarykeys_args_t         primarykeys_args;

  tables_t                   tables;

  tsdb_data_t                current_catalog;
  tsdb_data_t                current_schema;
  tsdb_data_t                current_table;
  tsdb_data_t                current_table_type;

  tsdb_data_t                current_col_name;
  tsdb_data_t                current_col_type;
  tsdb_data_t                current_col_length;
  tsdb_data_t                current_col_note;

  mem_t                      tsdb_desc;
  tsdb_stmt_t                desc;

  int                        ordinal_order;
};

struct typesinfo_s {
  stmt_base_t                base;
  stmt_t                    *owner;

  SQLSMALLINT                data_type;

  size_t                     pos; // 1-based
};

struct param_state_s {
  int                        nr_batch_size;
  size_t                     i_batch_offset;

  SQLSMALLINT                nr_tsdb_fields;

  int                        i_row;
  int                        i_param;
  desc_record_t             *APD_record;
  desc_record_t             *IPD_record;

  TAOS_FIELD_E              *tsdb_field;

  tsdb_param_column_t       *param_column;
  TAOS_MULTI_BIND           *tsdb_bind;

  const char                *sqlc_base;
  size_t                     sqlc_len;

  sqlc_data_t                sqlc_data;
  sql_data_t                 sql_data;

  mem_t                      tmp;

  charset_convs_t            charset_convs;

  uint8_t                    is_subtbl:1;
  uint8_t                    row_with_info:1;
  uint8_t                    row_err:1;
};

struct col_bind_map_s {
  int                    tsdb_type;

  const char            *type_name;

  int                    sql_type;
  int                    sql_promoted;

  const char            *prefix;
  const char            *suffix;

  int                    length;
  int                    octet_length;
  int                    precision;
  int                    scale;
  int                    display_size;
  int                    num_prec_radix;
  int                    unsigned_;

  int                    searchable;
};

typedef SQLRETURN (*param_f)(stmt_t *stmt, param_state_t *param_state);

struct param_bind_meta_s {
  param_f                     check;     // check sqlc_data against sql_data and convert into sql_data
  param_f                     guess;     // guess tsdb_type by sqlc_type
  param_f                     get_sqlc;  // get sqlc
  param_f                     adjust;    // adjust tsdb_array
  param_f                     conv;      // conv sqlc to tsdb
};

struct params_bind_meta_s {
  param_bind_meta_t         *base;
  size_t                     cap;
  size_t                     nr;
};

typedef SQLRETURN (*param_bind_set_APD_record_f)(stmt_t* stmt,
    desc_record_t  *APD_record,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     ValueType,
    SQLPOINTER      ParameterValuePtr,
    SQLLEN          BufferLength,
    SQLLEN         *StrLen_or_IndPtr);

typedef SQLRETURN (*param_bind_set_IPD_record_f)(stmt_t* stmt,
    desc_record_t  *IPD_record,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     InputOutputType,
    SQLSMALLINT     ParameterType,
    SQLULEN         ColumnSize,
    SQLSMALLINT     DecimalDigits);

typedef SQLRETURN (*param_bind_f)(stmt_t* stmt,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     ValueType,
    SQLSMALLINT     ParameterType,
    SQLULEN         ColumnSize,
    SQLSMALLINT     DecimalDigits,
    SQLPOINTER      ParameterValuePtr,
    SQLLEN          BufferLength,
    SQLLEN         *StrLen_or_IndPtr);

struct sqlc_sql_map_s {
  SQLSMALLINT    ValueType;
  SQLSMALLINT    ParameterType;
  param_bind_set_APD_record_f       set_APD_record;
  param_bind_set_IPD_record_f       set_IPD_record;
  param_f        get_sqlc;  // get sqlc
  param_f        check;     // check sqlc_data against sql_data and convert into sql_data
  param_f        guess;     // guess tsdb_type by sqlc_type
};

struct param_bind_map_s {
  SQLSMALLINT    ValueType;
  SQLSMALLINT    ParameterType;
  int8_t         tsdb_type;
  param_f        adjust;    // adjust tsdb_array
  param_f        conv;      // conv sqlc to tsdb
};


// NOTE: do not use this unless you know what it does!!!
#ifdef USE_TICK_TO_DEBUG                 /* { */
typedef struct tick_s                   tick_t;
typedef struct ticks_s                  ticks_t;

struct tick_s {
#ifdef _WIN32               /* { */
  LARGE_INTEGER              enter;
  LARGE_INTEGER              leave;
#else                       /* }{ */
  struct timespec            enter;
  struct timespec            leave;
#endif                      /* } */
};

struct ticks_s {
  tick_t                    *ticks;
  size_t                     sz;
  size_t                     idx;

  int64_t                    max_delta;
  int64_t                    min_delta;
  int64_t                    total_delta;
  int64_t                    max_interval;
  int64_t                    min_interval;
  int64_t                    total_interval;

#ifdef _WIN32               /* { */
  LARGE_INTEGER              freq;
#endif                      /* } */
};
#endif                                   /* } */

struct stmt_s {
  atomic_int                 refc;

  struct tod_list_head       node;

#ifdef USE_TICK_TO_DEBUG                 /* { */
  ticks_t                    ticks_for_SQLGetData;
  ticks_t                    ticks_for_fetch_block;
#endif                                   /* } */

  conn_t                    *conn;

  errs_t                     errs;

  struct tod_list_head       associated_APD_node;
  desc_t                    *associated_APD;

  struct tod_list_head       associated_ARD_node;
  desc_t                    *associated_ARD;

  descriptor_t               APD, IPD;
  descriptor_t               ARD, IRD;

  params_bind_meta_t         params_bind_meta;

  descriptor_t              *current_APD;
  descriptor_t              *current_ARD;

  get_data_ctx_t             get_data_ctx;
  param_state_t              param_state;

  mem_t                      raw;
  sqls_t                     sqls;

  mem_t                      tsdb_sql;
  sqlc_tsdb_t                current_sql;

  tsdb_paramset_t            tsdb_paramset;

  tsdb_binds_t               tsdb_binds;

  tsdb_stmt_t                tsdb_stmt;
  tables_t                   tables;
  columns_t                  columns;
  typesinfo_t                typesinfo;
  primarykeys_t              primarykeys;
  topic_t                    topic;

  mem_t                      mem;

  stmt_base_t               *base;

  unsigned int               strict:1; // 1: param-truncation as failure
  unsigned int               no_total:1;
  SQLULEN                    concurrency_attr;
  SQLULEN                    cursor_type;
};

struct tls_s {
  mem_t                      intermediate;
  charset_conv_mgr_t        *mgr;
  // debug leakage only
  char                      *leakage;
};

#ifdef USE_TICK_TO_DEBUG                 /* { */
int stmt_enter_fetch_block(stmt_t *stmt) FA_HIDDEN;
void stmt_leave_fetch_block(stmt_t *stmt) FA_HIDDEN;
#endif                                   /* } */

EXTERN_C_END

#endif // _internal_h_

