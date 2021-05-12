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

#include "tsdb_impl.h"

#include "env.h"
#include "../todbc_tls.h"
#include "../todbc_util.h"

#include "ttype.h"


#include <odbcinst.h>

// tsdb_conn

typedef struct tsdb_conn_s          tsdb_conn_t;
typedef struct tsdb_stmt_s          tsdb_stmt_t;
typedef struct tsdb_param_s         tsdb_param_t;
typedef struct tsdb_param_val_s     tsdb_param_val_t;
typedef struct tsdb_param_conv_arg_s         tsdb_param_conv_arg_t;

struct tsdb_conn_s {
  conn_t            *conn;

  char               svr_info[64];
  char               cli_info[64];

  TAOS              *taos;
};

struct tsdb_param_s {
  int         tsdb_type;          // TSDB_DATA_TYPE_xxx
  int         tsdb_bytes;

  SQLRETURN (*conv)(stmt_t *stmt, tsdb_param_conv_arg_t *arg);
};

struct tsdb_param_val_s {
  SQLUSMALLINT   ParameterNumber;
  int            is_null;
};

struct tsdb_stmt_s {
  stmt_t                  *stmt;

  TAOS_STMT               *tsdb_stmt;      // prepared-statement
  TAOS_RES                *tsdb_res;

  tsdb_param_t            *tsdb_params;

  todbc_buf_t             *tsdb_param_vals_cache;
  tsdb_param_val_t        *tsdb_param_vals;
  TAOS_BIND               *taos_binds;

  TAOS_FIELD              *tsdb_fields;
  TAOS_ROW                 tsdb_curr;

  unsigned int             by_query:1;
};

struct tsdb_param_conv_arg_s {
  conn_t                 *conn;
  todbc_buf_t            *cache;
  int                     idx;
  SQLPOINTER              val;
  SQLLEN                  soi;
  tsdb_param_t           *tsdb_param;
  tsdb_param_val_t       *tsdb_param_val;
  TAOS_BIND              *taos_bind;
};

static void tsdb_stmt_init_param_vals_cache(tsdb_stmt_t *tsdb_stmt) {
  OILE(tsdb_stmt, "");
  if (tsdb_stmt->tsdb_param_vals_cache) return;
  tsdb_stmt->tsdb_param_vals_cache = todbc_buf_create();
}

static void tsdb_stmt_reclaim_param_vals(tsdb_stmt_t *tsdb_stmt) {
  if (!tsdb_stmt) return;
  if (tsdb_stmt->tsdb_param_vals_cache) {
    tsdb_stmt->tsdb_param_vals = NULL;
    tsdb_stmt->taos_binds = NULL;
    todbc_buf_reclaim(tsdb_stmt->tsdb_param_vals_cache);
  }
  OILE(tsdb_stmt->tsdb_param_vals==NULL, "");
  OILE(tsdb_stmt->taos_binds==NULL, "");
}

static void tsdb_stmt_cleanup_param_vals(tsdb_stmt_t *tsdb_stmt) {
  if (!tsdb_stmt) return;
  tsdb_stmt_reclaim_param_vals(tsdb_stmt);

  if (tsdb_stmt->tsdb_param_vals_cache) {
    todbc_buf_free(tsdb_stmt->tsdb_param_vals_cache);
  }
}

static void tsdb_stmt_calloc_param_vals(tsdb_stmt_t *tsdb_stmt) {
  OILE(tsdb_stmt, "");
  stmt_t *stmt = tsdb_stmt->stmt;
  OILE(stmt, "");
  paramset_t *paramset = &stmt->paramset;
  int n_params = paramset->n_params;
  OILE(n_params>0, "");
  todbc_buf_t *cache = tsdb_stmt->tsdb_param_vals_cache;
  OILE(cache, "");
  OILE(tsdb_stmt->tsdb_param_vals==NULL, "");
  OILE(tsdb_stmt->taos_binds==NULL, "");
  tsdb_stmt->tsdb_param_vals = (tsdb_param_val_t*)todbc_buf_calloc(cache, (size_t)n_params, sizeof(*tsdb_stmt->tsdb_param_vals));
  tsdb_stmt->taos_binds = (TAOS_BIND*)todbc_buf_calloc(cache, (size_t)n_params, sizeof(*tsdb_stmt->taos_binds));
}

static SQLRETURN tsdb_stmt_init_stmt(tsdb_stmt_t *tsdb_stmt) {
  OILE(tsdb_stmt && tsdb_stmt->stmt, "");
  errs_t *errs = &tsdb_stmt->stmt->errs;

  if (tsdb_stmt->tsdb_stmt) return SQL_SUCCESS;
  OILE(tsdb_stmt->stmt->owner, "");
  conn_t *conn = tsdb_stmt->stmt->owner->conn;
  OILE(conn, "");
  tsdb_conn_t *tsdb_conn = (tsdb_conn_t*)conn->ext.ext;
  OILE(tsdb_conn && tsdb_conn->taos, "");
  tsdb_stmt->tsdb_stmt = taos_stmt_init(tsdb_conn->taos);
  if (!tsdb_stmt->tsdb_stmt) {
    SET_GENERAL(errs, "failed to init taos stmt");
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static void tsdb_stmt_close_stmt(tsdb_stmt_t *tsdb_stmt) {
  if (!tsdb_stmt) return;
  if (!tsdb_stmt->tsdb_stmt) return;

  int r = taos_stmt_close(tsdb_stmt->tsdb_stmt);
  tsdb_stmt->tsdb_stmt= NULL;
  tsdb_stmt->stmt->prepared = 0;
  if (r) OD("[%d]%s", r, tstrerror(r));
}

static void tsdb_stmt_close_rs(tsdb_stmt_t *tsdb_stmt) {
  if (!tsdb_stmt) return;
  if (!tsdb_stmt->tsdb_res) return;

  tsdb_stmt->tsdb_curr = NULL;

  if (tsdb_stmt->by_query) {
    taos_stop_query(tsdb_stmt->tsdb_res);
  } else {
    OILE(tsdb_stmt->tsdb_stmt==NULL, "");
    taos_free_result(tsdb_stmt->tsdb_res);
  }
  tsdb_stmt->tsdb_res = NULL;
}

static void tsdb_conn_free(conn_t *conn) {
  OILE(conn, "");

  tsdb_conn_t *tsdb_conn = (tsdb_conn_t*)conn->ext.ext;
  OILE(tsdb_conn, "");
  OILE(tsdb_conn->taos==NULL, "");

  conn->ext.ext       = NULL;
  conn->ext.free_conn = NULL;

  free(tsdb_conn);
}

static SQLRETURN tsdb_conn_connect(conn_t *conn) {
  OILE(conn, "");
  OILE(CONN_IS_NORM(conn), "");
  errs_t *errs = &conn->errs;
  tsdb_conn_t *tsdb_conn = (tsdb_conn_t*)conn->ext.ext;
  OILE(tsdb_conn, "");
  OILE(tsdb_conn->conn==conn, "");

  conn_val_t *val = &conn->val;
  const char *dsn = val->dsn;
  const char *uid = val->uid;
  const char *pwd = val->pwd;
  const char *db  = val->db;
  const char *svr = val->server;

  OILE(dsn, "");

  int use_default = 0;
  char server[4096]; server[0] = '\0';
  if (!svr || !svr[0]) {
    int n = SQLGetPrivateProfileString(dsn, "Server", "", server, sizeof(server)-1, "Odbc.ini");
    if (n<=0) {
      snprintf(server, sizeof(server), DEFAULT_SERVER);
      n = (int)strlen(server);
      use_default = 1;
    } else {
      server[n] = '\0';
    }
    svr = server;

    if (!svr || !svr[0]) {
      SET_GENERAL(errs, "please specify Server entry in connection string or odbc.ini or windows registry for DSN:[%s]", dsn);
      return SQL_ERROR;
    }
  }

  char *ip         = NULL;
  int   port       = 0;
  char *p = strchr(svr, ':');
  if (p) {
    ip = todbc_tls_strndup(svr, (size_t)(p-svr));
    port = atoi(p+1);
  }

  tsdb_conn->taos = taos_connect(ip, uid, pwd, db, (uint16_t)port);
  if (!tsdb_conn->taos) {
    int          e  = terrno;
    const char * es = tstrerror(e);
    if (use_default) {
      SET_GENERAL(errs, "no Server entry in odbc.ini or windows registry for DSN[%s], fallback to svr[%s:%d] db[%s]", dsn, ip, port, db);
    }
    SET_GENERAL(errs, "connect to DSN[%s] svr[%s:%d] db[%s] failed", dsn, ip, port, db);
    SET_GENERAL(errs, "[%x]%s", e, es);
    return SQL_ERROR;
  }

  const char *svr_info = taos_get_server_info(tsdb_conn->taos);
  const char *cli_info = taos_get_client_info(tsdb_conn->taos);
  snprintf(tsdb_conn->svr_info, sizeof(tsdb_conn->svr_info), "%s", svr_info);
  snprintf(tsdb_conn->cli_info, sizeof(tsdb_conn->cli_info), "%s", cli_info);

  return SQL_SUCCESS;
}

static void tsdb_conn_disconnect(conn_t *conn) {
  OILE(conn, "");
  OILE(CONN_IS_CONNECTED(conn), "");

  tsdb_conn_t *tsdb_conn = (tsdb_conn_t*)conn->ext.ext;
  OILE(tsdb_conn, "");
  OILE(tsdb_conn->conn==conn, "");

  TAOS *taos = tsdb_conn->taos;

  if (!taos) return;
  taos_close(taos);
  taos = NULL;
  tsdb_conn->taos = NULL;
}

static void tsdb_conn_free_stmt(stmt_t *stmt) {
  OILE(stmt, "");
  tsdb_stmt_t *tsdb_stmt = (tsdb_stmt_t*)stmt->ext.ext;
  OILE(tsdb_stmt && tsdb_stmt->stmt==stmt, "");

  tsdb_stmt_close_rs(tsdb_stmt);
  tsdb_stmt_close_stmt(tsdb_stmt);
  tsdb_stmt_cleanup_param_vals(tsdb_stmt);

  tsdb_stmt->tsdb_params = NULL;

  stmt_ext_t ext = {0};
  stmt->ext = ext;

  free(tsdb_stmt);
}

static void tsdb_conn_clear_param_vals(stmt_t *stmt) {
  OILE(stmt, "");
  tsdb_stmt_t *tsdb_stmt = (tsdb_stmt_t*)stmt->ext.ext;
  OILE(tsdb_stmt && tsdb_stmt->stmt==stmt, "");

  tsdb_stmt_reclaim_param_vals(tsdb_stmt);
}

static SQLRETURN tsdb_conn_exec_direct(stmt_t *stmt) {
  OILE(stmt, "");
  OILE(stmt->owner, "");
  OILE(!STMT_IS_EXECUTED(stmt), "");

  conn_t *conn = stmt->owner->conn;
  OILE(conn, "");
  tsdb_conn_t *tsdb_conn = (tsdb_conn_t*)conn->ext.ext;
  OILE(tsdb_conn && tsdb_conn->conn==conn, "");
  TAOS *taos = tsdb_conn->taos;
  OILE(taos, "");

  tsdb_stmt_t *tsdb_stmt = (tsdb_stmt_t*)stmt->ext.ext;
  OILE(tsdb_stmt && tsdb_stmt->stmt==stmt, "");
  OILE(tsdb_stmt->tsdb_stmt==NULL, "");
  OILE(tsdb_stmt->tsdb_res==NULL, "");

  errs_t *errs = &stmt->errs;

  tsdb_stmt_reclaim_param_vals(tsdb_stmt);

  const char *txt = (const char*)stmt->sql.txt.buf;
  TAOS_RES *tsdb_res = taos_query(taos, txt);
  OILE(tsdb_res, "");
  int r = taos_errno(tsdb_res);
  if (r) {
    SET_GENERAL(errs, "taos query failed:[%d]%s", r, tstrerror(r));
    taos_stop_query(tsdb_res);
    STMT_SET_NORM(stmt);
    return SQL_ERROR;
  }

  tsdb_stmt->tsdb_res = tsdb_res;
  tsdb_stmt->by_query = 1;

  STMT_SET_EXECUTED(stmt);
  return SQL_SUCCESS;
}

static SQLRETURN do_conv_int64_to_tsdb_timestamp(stmt_t *stmt, const int64_t v, tsdb_param_conv_arg_t *arg) {
  tsdb_param_val_t    *tsdb_param_val = arg->tsdb_param_val;
  TAOS_BIND           *taos_bind      = arg->taos_bind;

  taos_bind->buffer_type   = TSDB_DATA_TYPE_TIMESTAMP;
  taos_bind->u.ts          = v;
  taos_bind->buffer_length = sizeof(taos_bind->u.ts);
  taos_bind->buffer        = &taos_bind->u.ts;
  taos_bind->length        = &taos_bind->buffer_length;
  taos_bind->is_null       = &tsdb_param_val->is_null;

  return SQL_SUCCESS;
}

static SQLRETURN do_conv_int64_to_tsdb_tinyint(stmt_t *stmt, const int64_t v, tsdb_param_conv_arg_t *arg) {
  errs_t *errs = &stmt->errs;

  if (v<INT8_MIN || v>INT8_MAX) {
    SET_GENERAL(errs, "integer overflow for param [%d]", arg->idx+1);
    return SQL_ERROR;
  }

  tsdb_param_val_t    *tsdb_param_val = arg->tsdb_param_val;
  TAOS_BIND           *taos_bind      = arg->taos_bind;

  taos_bind->buffer_type   = TSDB_DATA_TYPE_TINYINT;
  taos_bind->u.v1          = (int8_t)v;
  taos_bind->buffer_length = sizeof(taos_bind->u.v1);
  taos_bind->buffer        = &taos_bind->u.v1;
  taos_bind->length        = &taos_bind->buffer_length;
  taos_bind->is_null       = &tsdb_param_val->is_null;

  return SQL_SUCCESS;
}

static SQLRETURN do_conv_int64_to_tsdb_smallint(stmt_t *stmt, const int64_t v, tsdb_param_conv_arg_t *arg) {
  errs_t *errs = &stmt->errs;

  if (v<INT16_MIN || v>INT16_MAX) {
    SET_GENERAL(errs, "integer overflow for param [%d]", arg->idx+1);
    return SQL_ERROR;
  }

  tsdb_param_val_t    *tsdb_param_val = arg->tsdb_param_val;
  TAOS_BIND           *taos_bind      = arg->taos_bind;

  taos_bind->buffer_type   = TSDB_DATA_TYPE_SMALLINT;
  taos_bind->u.v2          = (int16_t)v;
  taos_bind->buffer_length = sizeof(taos_bind->u.v2);
  taos_bind->buffer        = &taos_bind->u.v2;
  taos_bind->length        = &taos_bind->buffer_length;
  taos_bind->is_null       = &tsdb_param_val->is_null;

  return SQL_SUCCESS;
}

static SQLRETURN do_conv_int64_to_tsdb_int(stmt_t *stmt, const int64_t v, tsdb_param_conv_arg_t *arg) {
  errs_t *errs = &stmt->errs;

  if (v<INT32_MIN || v>INT32_MAX) {
    SET_GENERAL(errs, "integer overflow for param [%d]", arg->idx+1);
    return SQL_ERROR;
  }

  tsdb_param_val_t    *tsdb_param_val = arg->tsdb_param_val;
  TAOS_BIND           *taos_bind      = arg->taos_bind;

  taos_bind->buffer_type   = TSDB_DATA_TYPE_INT;
  taos_bind->u.v4          = (int32_t)v;
  taos_bind->buffer_length = sizeof(taos_bind->u.v4);
  taos_bind->buffer        = &taos_bind->u.v4;
  taos_bind->length        = &taos_bind->buffer_length;
  taos_bind->is_null       = &tsdb_param_val->is_null;

  return SQL_SUCCESS;
}

static SQLRETURN do_conv_int64_to_tsdb_bigint(stmt_t *stmt, const int64_t v, tsdb_param_conv_arg_t *arg) {
  tsdb_param_val_t    *tsdb_param_val = arg->tsdb_param_val;
  TAOS_BIND           *taos_bind      = arg->taos_bind;

  taos_bind->buffer_type   = TSDB_DATA_TYPE_BIGINT;
  taos_bind->u.v8          = v;
  taos_bind->buffer_length = sizeof(taos_bind->u.v8);
  taos_bind->buffer        = &taos_bind->u.v8;
  taos_bind->length        = &taos_bind->buffer_length;
  taos_bind->is_null       = &tsdb_param_val->is_null;

  return SQL_SUCCESS;
}

static SQLRETURN do_conv_double_to_tsdb_float(stmt_t *stmt, const double v, tsdb_param_conv_arg_t *arg) {
  tsdb_param_val_t    *tsdb_param_val = arg->tsdb_param_val;
  TAOS_BIND           *taos_bind      = arg->taos_bind;

  taos_bind->buffer_type   = TSDB_DATA_TYPE_FLOAT;
  taos_bind->u.f4          = (float)v;
  taos_bind->buffer_length = sizeof(taos_bind->u.f4);
  taos_bind->buffer        = &taos_bind->u.f4;
  taos_bind->length        = &taos_bind->buffer_length;
  taos_bind->is_null       = &tsdb_param_val->is_null;

  return SQL_SUCCESS;
}

static SQLRETURN do_conv_double_to_tsdb_double(stmt_t *stmt, const double v, tsdb_param_conv_arg_t *arg) {
  tsdb_param_val_t    *tsdb_param_val = arg->tsdb_param_val;
  TAOS_BIND           *taos_bind      = arg->taos_bind;

  taos_bind->buffer_type   = TSDB_DATA_TYPE_DOUBLE;
  taos_bind->u.f8          = v;
  taos_bind->buffer_length = sizeof(taos_bind->u.f8);
  taos_bind->buffer        = &taos_bind->u.f8;
  taos_bind->length        = &taos_bind->buffer_length;
  taos_bind->is_null       = &tsdb_param_val->is_null;

  return SQL_SUCCESS;
}


static SQLRETURN do_conv_sql_string_to_int64(stmt_t *stmt, const char *enc_from, tsdb_param_conv_arg_t *arg, int64_t *v) {
  todbc_buf_t         *cache       = arg->cache;
  const char          *enc_to      = arg->conn->enc_src;      // ?locale or src?, windows iconv!!!
  const unsigned char *src         = (const unsigned char*)arg->val;
  size_t               slen        = (size_t)arg->soi;

  errs_t *errs = &stmt->errs;

  todbc_string_t txt = todbc_tls_conv(cache, enc_to, enc_from, src, &slen);
  if (!txt.buf) {
    SET_OOM(errs, "failed to alloc space to convert from [%s->%s] for param [%d]", enc_from, enc_to, arg->idx+1);
    return SQL_ERROR;
  }
  if (txt.bytes<txt.total_bytes) {
    SET_OOM(errs, "failed to convert from [%s->%s] for param [%d]", enc_from, enc_to, arg->idx+1);
    return SQL_ERROR;
  }

  const char *buf   = (const char*)txt.buf;
  int         bytes = 0;
  int64_t     i64   = 0;
  sscanf(buf, "%" PRId64 " %n", &i64, &bytes);
  if (strlen(buf)!=bytes) {
    SET_GENERAL(errs, "failed to convert to integer for param [%d]", arg->idx+1);
    return SQL_ERROR;
  }

  *v = i64;

  return SQL_SUCCESS;
}

static SQLRETURN do_conv_sql_string_to_double(stmt_t *stmt, const char *enc_from, tsdb_param_conv_arg_t *arg, double *v) {
  todbc_buf_t         *cache       = arg->cache;
  const char          *enc_to      = arg->conn->enc_src;      // ?locale or src?, windows iconv!!!
  const unsigned char *src         = (const unsigned char*)arg->val;
  size_t               slen        = (size_t)arg->soi;

  errs_t *errs = &stmt->errs;

  todbc_string_t txt = todbc_tls_conv(cache, enc_to, enc_from, src, &slen);
  if (!txt.buf) {
    SET_OOM(errs, "failed to alloc space to convert from [%s->%s] for param [%d]", enc_from, enc_to, arg->idx+1);
    return SQL_ERROR;
  }
  if (txt.bytes<txt.total_bytes) {
    SET_OOM(errs, "failed to convert from [%s->%s] for param [%d]", enc_from, enc_to, arg->idx+1);
    return SQL_ERROR;
  }

  const char *buf   = (const char*)txt.buf;
  int         bytes = 0;
  double      dbl   = 0.0;
  sscanf(buf, "%lf%n", &dbl, &bytes);
  if (strlen(buf)!=bytes) {
    SET_GENERAL(errs, "failed to convert to double for param [%d]", arg->idx+1);
    return SQL_ERROR;
  }

  *v = dbl;

  return SQL_SUCCESS;
}

static SQLRETURN do_conv_sql_string_to_tsdb_timestamp(stmt_t *stmt, const char *enc_from, tsdb_param_conv_arg_t *arg) {
  todbc_buf_t         *cache       = arg->cache;
  const char          *enc_to      = arg->conn->enc_src;      // ?locale or src?, windows iconv!!!
  const unsigned char *src         = (const unsigned char*)arg->val;
  size_t               slen        = (size_t)arg->soi;

  errs_t *errs = &stmt->errs;

  todbc_string_t txt = todbc_tls_conv(cache, enc_to, enc_from, src, &slen);
  if (!txt.buf) {
    SET_OOM(errs, "failed to alloc space to convert from [%s->%s] for param [%d]", enc_from, enc_to, arg->idx+1);
    return SQL_ERROR;
  }
  if (txt.bytes<txt.total_bytes) {
    SET_OOM(errs, "failed to convert from [%s->%s] for param [%d]", enc_from, enc_to, arg->idx+1);
    return SQL_ERROR;
  }

  char       *buf   = (char*)txt.buf;
  int32_t     bytes = (int32_t)txt.bytes;

  int64_t ts = 0;
  int r = taosParseTime(buf, &ts, bytes, TSDB_TIME_PRECISION_MILLI, 0);
  if (r) {
    SET_GENERAL(errs, "failed to parse as timestamp for param [%d]", arg->idx+1);
    return SQL_ERROR;
  }

  tsdb_param_val_t    *tsdb_param_val = arg->tsdb_param_val;
  TAOS_BIND           *taos_bind      = arg->taos_bind;

  taos_bind->buffer_type   = TSDB_DATA_TYPE_TIMESTAMP;
  taos_bind->u.ts          = ts;
  taos_bind->buffer_length = sizeof(taos_bind->u.ts);
  taos_bind->buffer        = &taos_bind->u.ts;
  taos_bind->length        = &taos_bind->buffer_length;
  taos_bind->is_null       = &tsdb_param_val->is_null;

  return SQL_SUCCESS;
}

static SQLRETURN do_conv_sql_string_to_tsdb_nchar(stmt_t *stmt, const char *enc_from, tsdb_param_conv_arg_t *arg) {
  todbc_buf_t         *cache       = arg->cache;
  const char          *enc_to      = arg->conn->enc_db;
  const unsigned char *src         = (const unsigned char*)arg->val;
  size_t               slen        = (size_t)arg->soi;

  errs_t *errs = &stmt->errs;

  todbc_string_t txt_db = todbc_tls_conv(cache, enc_to, enc_from, src, &slen);
  if (!txt_db.buf) {
    SET_OOM(errs, "failed to alloc space to convert from [%s->%s] for param [%d]", enc_from, enc_to, arg->idx+1);
    return SQL_ERROR;
  }
  if (txt_db.bytes<txt_db.total_bytes) {
    SET_OOM(errs, "failed to convert from [%s->%s] for param [%d]", enc_from, enc_to, arg->idx+1);
    return SQL_ERROR;
  }

  char       *buf   = (char*)txt_db.buf;
  int32_t     bytes = (int32_t)txt_db.bytes;
  if (bytes > arg->tsdb_param->tsdb_bytes) {
    SET_OOM(errs, "failed to convert from [%s->%s] for param [%d], string too long [%d/%d]",
                  enc_from, enc_to, arg->idx+1, bytes, arg->tsdb_param->tsdb_bytes);
    return SQL_ERROR;
  }

  tsdb_param_val_t    *tsdb_param_val = arg->tsdb_param_val;
  TAOS_BIND           *taos_bind      = arg->taos_bind;

  taos_bind->buffer_type   = TSDB_DATA_TYPE_NCHAR;
  taos_bind->u.nchar       = buf;
  taos_bind->buffer_length = (uintptr_t)((size_t)bytes);
  taos_bind->buffer        = taos_bind->u.nchar;
  taos_bind->length        = &taos_bind->buffer_length;
  taos_bind->is_null       = &tsdb_param_val->is_null;

  return SQL_SUCCESS;
}

static SQLRETURN do_conv_sql_string_to_tsdb_bool(stmt_t *stmt, const char *enc_from, tsdb_param_conv_arg_t *arg) {
  int64_t     v     = 0;

  todbc_buf_t         *cache       = arg->cache;
  const char          *enc_to      = arg->conn->enc_src;      // ?locale or src?, windows iconv!!!
  const unsigned char *src         = (const unsigned char*)arg->val;
  size_t               slen        = (size_t)arg->soi;

  errs_t *errs = &stmt->errs;

  todbc_string_t txt_db = todbc_tls_conv(cache, enc_to, enc_from, src, &slen);
  if (!txt_db.buf) {
    SET_OOM(errs, "failed to alloc space to convert from [%s->%s] for param [%d]", enc_from, enc_to, arg->idx+1);
    return SQL_ERROR;
  }
  if (txt_db.bytes<txt_db.total_bytes) {
    SET_OOM(errs, "failed to convert from [%s->%s] for param [%d]", enc_from, enc_to, arg->idx+1);
    return SQL_ERROR;
  }

  const char *buf   = (const char*)txt_db.buf;
  int         bytes = 0;
  if (strcasecmp(buf, "true")==0) {
    v = 1;
  } else if (strcasecmp(buf, "false")==0) {
    v = 0;
  } else {
    sscanf(buf, "%" PRId64 " %n", &v, &bytes);
    if (strlen(buf)!=bytes) {
      SET_GENERAL(errs, "failed to convert to integer for param [%d]", arg->idx+1);
      return SQL_ERROR;
    }
  }

  tsdb_param_val_t    *tsdb_param_val = arg->tsdb_param_val;
  TAOS_BIND           *taos_bind      = arg->taos_bind;

  taos_bind->buffer_type   = TSDB_DATA_TYPE_BOOL;
  taos_bind->u.b           = v ? 1 : 0;
  taos_bind->buffer_length = sizeof(taos_bind->u.b);
  taos_bind->buffer        = &taos_bind->u.b;
  taos_bind->length        = &taos_bind->buffer_length;
  taos_bind->is_null       = &tsdb_param_val->is_null;

  return SQL_SUCCESS;
}

static SQLRETURN do_conv_sql_string_to_tsdb_tinyint(stmt_t *stmt, const char *enc_from, tsdb_param_conv_arg_t *arg) {
  int64_t     v     = 0;

  todbc_buf_t         *cache       = arg->cache;
  const char          *enc_to      = arg->conn->enc_src;      // ?locale or src?, windows iconv!!!
  const unsigned char *src         = (const unsigned char*)arg->val;
  size_t               slen        = (size_t)arg->soi;

  errs_t *errs = &stmt->errs;

  todbc_string_t txt_db = todbc_tls_conv(cache, enc_to, enc_from, src, &slen);
  if (!txt_db.buf) {
    SET_OOM(errs, "failed to alloc space to convert from [%s->%s] for param [%d]", enc_from, enc_to, arg->idx+1);
    return SQL_ERROR;
  }
  if (txt_db.bytes<txt_db.total_bytes) {
    SET_OOM(errs, "failed to convert from [%s->%s] for param [%d]", enc_from, enc_to, arg->idx+1);
    return SQL_ERROR;
  }

  const char *buf   = (const char*)txt_db.buf;
  int         bytes = 0;
  sscanf(buf, "%" PRId64 " %n", &v, &bytes);
  if (strlen(buf)!=bytes) {
    SET_GENERAL(errs, "failed to convert to integer for param [%d]", arg->idx+1);
    return SQL_ERROR;
  }
  if (v<INT8_MIN || v>INT8_MAX) {
    SET_GENERAL(errs, "failed to convert to tinyint for param [%d]", arg->idx+1);
    return SQL_ERROR;
  }

  tsdb_param_val_t    *tsdb_param_val = arg->tsdb_param_val;
  TAOS_BIND           *taos_bind      = arg->taos_bind;

  taos_bind->buffer_type   = TSDB_DATA_TYPE_TINYINT;
  taos_bind->u.v1          = (int8_t)v;
  taos_bind->buffer_length = sizeof(taos_bind->u.v1);
  taos_bind->buffer        = &taos_bind->u.v1;
  taos_bind->length        = &taos_bind->buffer_length;
  taos_bind->is_null       = &tsdb_param_val->is_null;

  return SQL_SUCCESS;
}

static SQLRETURN do_conv_sql_string_to_tsdb_smallint(stmt_t *stmt, const char *enc_from, tsdb_param_conv_arg_t *arg) {
  int64_t     v     = 0;

  todbc_buf_t         *cache       = arg->cache;
  const char          *enc_to      = arg->conn->enc_src;      // ?locale or src?, windows iconv!!!
  const unsigned char *src         = (const unsigned char*)arg->val;
  size_t               slen        = (size_t)arg->soi;

  errs_t *errs = &stmt->errs;

  todbc_string_t txt_db = todbc_tls_conv(cache, enc_to, enc_from, src, &slen);
  if (!txt_db.buf) {
    SET_OOM(errs, "failed to alloc space to convert from [%s->%s] for param [%d]", enc_from, enc_to, arg->idx+1);
    return SQL_ERROR;
  }
  if (txt_db.bytes<txt_db.total_bytes) {
    SET_OOM(errs, "failed to convert from [%s->%s] for param [%d]", enc_from, enc_to, arg->idx+1);
    return SQL_ERROR;
  }

  const char *buf   = (const char*)txt_db.buf;
  int         bytes = 0;
  sscanf(buf, "%" PRId64 " %n", &v, &bytes);
  if (strlen(buf)!=bytes) {
    SET_GENERAL(errs, "failed to convert to integer for param [%d]", arg->idx+1);
    return SQL_ERROR;
  }
  if (v<INT16_MIN || v>INT16_MAX) {
    SET_GENERAL(errs, "failed to convert to smallint for param [%d]", arg->idx+1);
    return SQL_ERROR;
  }

  tsdb_param_val_t    *tsdb_param_val = arg->tsdb_param_val;
  TAOS_BIND           *taos_bind      = arg->taos_bind;

  taos_bind->buffer_type   = TSDB_DATA_TYPE_SMALLINT;
  taos_bind->u.v2          = (int16_t)v;
  taos_bind->buffer_length = sizeof(taos_bind->u.v2);
  taos_bind->buffer        = &taos_bind->u.v2;
  taos_bind->length        = &taos_bind->buffer_length;
  taos_bind->is_null       = &tsdb_param_val->is_null;

  return SQL_SUCCESS;
}

static SQLRETURN do_conv_sql_string_to_tsdb_int(stmt_t *stmt, const char *enc_from, tsdb_param_conv_arg_t *arg) {
  int64_t     v     = 0;

  todbc_buf_t         *cache       = arg->cache;
  const char          *enc_to      = arg->conn->enc_src;      // ?locale or src?, windows iconv!!!
  const unsigned char *src         = (const unsigned char*)arg->val;
  size_t               slen        = (size_t)arg->soi;

  errs_t *errs = &stmt->errs;

  todbc_string_t txt_db = todbc_tls_conv(cache, enc_to, enc_from, src, &slen);
  if (!txt_db.buf) {
    SET_OOM(errs, "failed to alloc space to convert from [%s->%s] for param [%d]", enc_from, enc_to, arg->idx+1);
    return SQL_ERROR;
  }
  if (txt_db.bytes<txt_db.total_bytes) {
    SET_OOM(errs, "failed to convert from [%s->%s] for param [%d]", enc_from, enc_to, arg->idx+1);
    return SQL_ERROR;
  }

  const char *buf   = (const char*)txt_db.buf;
  int         bytes = 0;
  sscanf(buf, "%" PRId64 " %n", &v, &bytes);
  if (strlen(buf)!=bytes) {
    SET_GENERAL(errs, "failed to convert to integer for param [%d]", arg->idx+1);
    return SQL_ERROR;
  }
  if (v<INT32_MIN || v>INT32_MAX) {
    SET_GENERAL(errs, "failed to convert to int for param [%d]", arg->idx+1);
    return SQL_ERROR;
  }

  tsdb_param_val_t    *tsdb_param_val = arg->tsdb_param_val;
  TAOS_BIND           *taos_bind      = arg->taos_bind;

  taos_bind->buffer_type   = TSDB_DATA_TYPE_INT;
  taos_bind->u.v4          = (int32_t)v;
  taos_bind->buffer_length = sizeof(taos_bind->u.v4);
  taos_bind->buffer        = &taos_bind->u.v4;
  taos_bind->length        = &taos_bind->buffer_length;
  taos_bind->is_null       = &tsdb_param_val->is_null;

  return SQL_SUCCESS;
}

static SQLRETURN do_conv_sql_string_to_tsdb_bigint(stmt_t *stmt, const char *enc_from, tsdb_param_conv_arg_t *arg) {
  int64_t     v     = 0;

  todbc_buf_t         *cache       = arg->cache;
  const char          *enc_to      = arg->conn->enc_src;      // ?locale or src?, windows iconv!!!
  const unsigned char *src         = (const unsigned char*)arg->val;
  size_t               slen        = (size_t)arg->soi;

  errs_t *errs = &stmt->errs;

  todbc_string_t txt_db = todbc_tls_conv(cache, enc_to, enc_from, src, &slen);
  if (!txt_db.buf) {
    SET_OOM(errs, "failed to alloc space to convert from [%s->%s] for param [%d]", enc_from, enc_to, arg->idx+1);
    return SQL_ERROR;
  }
  if (txt_db.bytes<txt_db.total_bytes) {
    SET_OOM(errs, "failed to convert from [%s->%s] for param [%d]", enc_from, enc_to, arg->idx+1);
    return SQL_ERROR;
  }

  const char *buf   = (const char*)txt_db.buf;
  int         bytes = 0;
  sscanf(buf, "%" PRId64 " %n", &v, &bytes);
  if (strlen(buf)!=bytes) {
    SET_GENERAL(errs, "failed to convert to integer for param [%d]", arg->idx+1);
    return SQL_ERROR;
  }

  tsdb_param_val_t    *tsdb_param_val = arg->tsdb_param_val;
  TAOS_BIND           *taos_bind      = arg->taos_bind;

  taos_bind->buffer_type   = TSDB_DATA_TYPE_BIGINT;
  taos_bind->u.v8          = (int64_t)v;
  taos_bind->buffer_length = sizeof(taos_bind->u.v8);
  taos_bind->buffer        = &taos_bind->u.v8;
  taos_bind->length        = &taos_bind->buffer_length;
  taos_bind->is_null       = &tsdb_param_val->is_null;

  return SQL_SUCCESS;
}

static SQLRETURN do_conv_sql_string_to_tsdb_float(stmt_t *stmt, const char *enc_from, tsdb_param_conv_arg_t *arg) {
  double      v     = 0;

  todbc_buf_t         *cache       = arg->cache;
  const char          *enc_to      = arg->conn->enc_src;      // ?locale or src?, windows iconv!!!
  const unsigned char *src         = (const unsigned char*)arg->val;
  size_t               slen        = (size_t)arg->soi;

  errs_t *errs = &stmt->errs;

  todbc_string_t txt_db = todbc_tls_conv(cache, enc_to, enc_from, src, &slen);
  if (!txt_db.buf) {
    SET_OOM(errs, "failed to alloc space to convert from [%s->%s] for param [%d]", enc_from, enc_to, arg->idx+1);
    return SQL_ERROR;
  }
  if (txt_db.bytes<txt_db.total_bytes) {
    SET_OOM(errs, "failed to convert from [%s->%s] for param [%d]", enc_from, enc_to, arg->idx+1);
    return SQL_ERROR;
  }

  const char *buf   = (const char*)txt_db.buf;
  int         bytes = 0;
  sscanf(buf, "%lf %n", &v, &bytes);
  if (strlen(buf)!=bytes) {
    SET_GENERAL(errs, "failed to convert to integer for param [%d]", arg->idx+1);
    return SQL_ERROR;
  }

  tsdb_param_val_t    *tsdb_param_val = arg->tsdb_param_val;
  TAOS_BIND           *taos_bind      = arg->taos_bind;

  taos_bind->buffer_type   = TSDB_DATA_TYPE_FLOAT;
  taos_bind->u.f4          = (float)v;
  taos_bind->buffer_length = sizeof(taos_bind->u.f4);
  taos_bind->buffer        = &taos_bind->u.f4;
  taos_bind->length        = &taos_bind->buffer_length;
  taos_bind->is_null       = &tsdb_param_val->is_null;

  return SQL_SUCCESS;
}

static SQLRETURN do_conv_sql_string_to_tsdb_double(stmt_t *stmt, const char *enc_from, tsdb_param_conv_arg_t *arg) {
  double      v     = 0;

  todbc_buf_t         *cache       = arg->cache;
  const char          *enc_to      = arg->conn->enc_src;      // ?locale or src?, windows iconv!!!
  const unsigned char *src         = (const unsigned char*)arg->val;
  size_t               slen        = (size_t)arg->soi;

  errs_t *errs = &stmt->errs;

  todbc_string_t txt_db = todbc_tls_conv(cache, enc_to, enc_from, src, &slen);
  if (!txt_db.buf) {
    SET_OOM(errs, "failed to alloc space to convert from [%s->%s] for param [%d]", enc_from, enc_to, arg->idx+1);
    return SQL_ERROR;
  }
  if (txt_db.bytes<txt_db.total_bytes) {
    SET_OOM(errs, "failed to convert from [%s->%s] for param [%d]", enc_from, enc_to, arg->idx+1);
    return SQL_ERROR;
  }

  const char *buf   = (const char*)txt_db.buf;
  int         bytes = 0;
  sscanf(buf, "%lf %n", &v, &bytes);
  if (strlen(buf)!=bytes) {
    SET_GENERAL(errs, "failed to convert to integer for param [%d]", arg->idx+1);
    return SQL_ERROR;
  }

  tsdb_param_val_t    *tsdb_param_val = arg->tsdb_param_val;
  TAOS_BIND           *taos_bind      = arg->taos_bind;

  taos_bind->buffer_type   = TSDB_DATA_TYPE_DOUBLE;
  taos_bind->u.f8          = v;
  taos_bind->buffer_length = sizeof(taos_bind->u.f8);
  taos_bind->buffer        = &taos_bind->u.f8;
  taos_bind->length        = &taos_bind->buffer_length;
  taos_bind->is_null       = &tsdb_param_val->is_null;

  return SQL_SUCCESS;
}

static SQLRETURN do_conv_sql_wchar_to_tsdb_timestamp(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  const char          *enc_from    = arg->conn->enc_wchar;
  return do_conv_sql_string_to_tsdb_timestamp(stmt, enc_from, arg);
}

static SQLRETURN do_conv_sql_wchar_to_tsdb_nchar(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  const char          *enc_from    = arg->conn->enc_wchar;
  return do_conv_sql_string_to_tsdb_nchar(stmt, enc_from, arg);
}

static SQLRETURN do_conv_sql_wchar_to_tsdb_bool(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  const char          *enc_from    = arg->conn->enc_wchar;
  return do_conv_sql_string_to_tsdb_bool(stmt, enc_from, arg);
}

static SQLRETURN do_conv_sql_wchar_to_tsdb_tinyint(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  const char          *enc_from    = arg->conn->enc_wchar;
  int64_t v = 0;
  SQLRETURN r = do_conv_sql_string_to_int64(stmt, enc_from, arg, &v);
  if (r!=SQL_SUCCESS) return r;
  return do_conv_int64_to_tsdb_tinyint(stmt, v, arg);
}

static SQLRETURN do_conv_sql_wchar_to_tsdb_smallint(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  const char          *enc_from    = arg->conn->enc_wchar;
  int64_t v = 0;
  SQLRETURN r = do_conv_sql_string_to_int64(stmt, enc_from, arg, &v);
  if (r!=SQL_SUCCESS) return r;
  return do_conv_int64_to_tsdb_smallint(stmt, v, arg);
}

static SQLRETURN do_conv_sql_wchar_to_tsdb_int(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  const char          *enc_from    = arg->conn->enc_wchar;
  int64_t v = 0;
  SQLRETURN r = do_conv_sql_string_to_int64(stmt, enc_from, arg, &v);
  if (r!=SQL_SUCCESS) return r;
  return do_conv_int64_to_tsdb_int(stmt, v, arg);
}

static SQLRETURN do_conv_sql_wchar_to_tsdb_bigint(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  const char          *enc_from    = arg->conn->enc_wchar;
  int64_t v = 0;
  SQLRETURN r = do_conv_sql_string_to_int64(stmt, enc_from, arg, &v);
  if (r!=SQL_SUCCESS) return r;
  return do_conv_int64_to_tsdb_bigint(stmt, v, arg);
}

static SQLRETURN do_conv_sql_wchar_to_tsdb_float(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  const char          *enc_from    = arg->conn->enc_wchar;
  double v = 0;
  SQLRETURN r = do_conv_sql_string_to_double(stmt, enc_from, arg, &v);
  if (r!=SQL_SUCCESS) return r;
  return do_conv_double_to_tsdb_float(stmt, v, arg);
}

static SQLRETURN do_conv_sql_wchar_to_tsdb_double(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  const char          *enc_from    = arg->conn->enc_wchar;
  double v = 0;
  SQLRETURN r = do_conv_sql_string_to_double(stmt, enc_from, arg, &v);
  if (r!=SQL_SUCCESS) return r;
  return do_conv_double_to_tsdb_double(stmt, v, arg);
}

static SQLRETURN do_conv_sql_char_to_tsdb_timestamp(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  const char          *enc_from    = arg->conn->enc_char;
  return do_conv_sql_string_to_tsdb_timestamp(stmt, enc_from, arg);
}

static SQLRETURN do_conv_sql_char_to_tsdb_nchar(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  const char          *enc_from    = arg->conn->enc_char;
  return do_conv_sql_string_to_tsdb_nchar(stmt, enc_from, arg);
}

static SQLRETURN do_conv_sql_char_to_tsdb_bool(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  const char          *enc_from    = arg->conn->enc_char;
  return do_conv_sql_string_to_tsdb_bool(stmt, enc_from, arg);
}

static SQLRETURN do_conv_sql_char_to_tsdb_tinyint(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  const char          *enc_from    = arg->conn->enc_char;
  return do_conv_sql_string_to_tsdb_tinyint(stmt, enc_from, arg);
}

static SQLRETURN do_conv_sql_char_to_tsdb_smallint(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  const char          *enc_from    = arg->conn->enc_char;
  return do_conv_sql_string_to_tsdb_smallint(stmt, enc_from, arg);
}

static SQLRETURN do_conv_sql_char_to_tsdb_int(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  const char          *enc_from    = arg->conn->enc_char;
  return do_conv_sql_string_to_tsdb_int(stmt, enc_from, arg);
}

static SQLRETURN do_conv_sql_char_to_tsdb_bigint(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  const char          *enc_from    = arg->conn->enc_char;
  return do_conv_sql_string_to_tsdb_bigint(stmt, enc_from, arg);
}

static SQLRETURN do_conv_sql_char_to_tsdb_float(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  const char          *enc_from    = arg->conn->enc_char;
  return do_conv_sql_string_to_tsdb_float(stmt, enc_from, arg);
}

static SQLRETURN do_conv_sql_char_to_tsdb_double(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  const char          *enc_from    = arg->conn->enc_char;
  return do_conv_sql_string_to_tsdb_double(stmt, enc_from, arg);
}

static SQLRETURN do_conv_int64_to_tsdb_bool(stmt_t *stmt, const int64_t v, tsdb_param_conv_arg_t *arg) {
  errs_t *errs = &stmt->errs;

  if (v!=1 && v!=0) {
    SET_GENERAL(errs, "integer overflow for param [%d]", arg->idx+1);
    return SQL_ERROR;
  }

  tsdb_param_val_t    *tsdb_param_val = arg->tsdb_param_val;
  TAOS_BIND           *taos_bind      = arg->taos_bind;

  taos_bind->buffer_type   = TSDB_DATA_TYPE_BOOL;
  taos_bind->u.b           = v ? 1 : 0;
  taos_bind->buffer_length = sizeof(taos_bind->u.b);
  taos_bind->buffer        = &taos_bind->u.b;
  taos_bind->length        = &taos_bind->buffer_length;
  taos_bind->is_null       = &tsdb_param_val->is_null;

  return SQL_SUCCESS;
}

static SQLRETURN do_conv_sql_sbigint_to_tsdb_bigint(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  int64_t v = *(int64_t*)arg->val;

  return do_conv_int64_to_tsdb_bigint(stmt, v, arg);
}

static SQLRETURN do_conv_sql_sbigint_to_tsdb_tinyint(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  int64_t v = *(int64_t*)arg->val;

  return do_conv_int64_to_tsdb_tinyint(stmt, v, arg);
}

static SQLRETURN do_conv_sql_sbigint_to_tsdb_int(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  int64_t v = *(int64_t*)arg->val;

  return do_conv_int64_to_tsdb_int(stmt, v, arg);
}

static SQLRETURN do_conv_sql_sbigint_to_tsdb_timestamp(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  int64_t v = *(int64_t*)arg->val;

  return do_conv_int64_to_tsdb_timestamp(stmt, v, arg);
}

static SQLRETURN do_conv_sql_long_to_tsdb_bool(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  int32_t v = *(int32_t*)arg->val;

  return do_conv_int64_to_tsdb_bool(stmt, v, arg);
}

static SQLRETURN do_conv_sql_long_to_tsdb_tinyint(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  int32_t v = *(int32_t*)arg->val;

  return do_conv_int64_to_tsdb_tinyint(stmt, v, arg);
}

static SQLRETURN do_conv_sql_long_to_tsdb_smallint(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  int32_t v = *(int32_t*)arg->val;

  return do_conv_int64_to_tsdb_smallint(stmt, v, arg);
}

static SQLRETURN do_conv_sql_long_to_tsdb_int(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  int32_t v = *(int32_t*)arg->val;

  return do_conv_int64_to_tsdb_int(stmt, v, arg);
}

static SQLRETURN do_conv_sql_long_to_tsdb_bigint(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  int32_t v = *(int32_t*)arg->val;

  return do_conv_int64_to_tsdb_bigint(stmt, v, arg);
}

static SQLRETURN do_conv_sql_tinyint_to_tsdb_bool(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  int8_t v = *(int8_t*)arg->val;

  return do_conv_int64_to_tsdb_bool(stmt, v, arg);
}

static SQLRETURN do_conv_sql_tinyint_to_tsdb_tinyint(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  int8_t v = *(int8_t*)arg->val;

  return do_conv_int64_to_tsdb_tinyint(stmt, v, arg);
}

static SQLRETURN do_conv_sql_tinyint_to_tsdb_smallint(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  int8_t v = *(int8_t*)arg->val;

  return do_conv_int64_to_tsdb_smallint(stmt, v, arg);
}

static SQLRETURN do_conv_sql_tinyint_to_tsdb_int(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  int8_t v = *(int8_t*)arg->val;

  return do_conv_int64_to_tsdb_int(stmt, v, arg);
}

static SQLRETURN do_conv_sql_tinyint_to_tsdb_bigint(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  int8_t v = *(int8_t*)arg->val;

  return do_conv_int64_to_tsdb_bigint(stmt, v, arg);
}

static SQLRETURN do_conv_sql_short_to_tsdb_bool(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  int16_t v = *(int16_t*)arg->val;

  return do_conv_int64_to_tsdb_bool(stmt, v, arg);
}

static SQLRETURN do_conv_sql_short_to_tsdb_tinyint(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  int16_t v = *(int16_t*)arg->val;

  return do_conv_int64_to_tsdb_tinyint(stmt, v, arg);
}

static SQLRETURN do_conv_sql_short_to_tsdb_smallint(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  int16_t v = *(int16_t*)arg->val;

  return do_conv_int64_to_tsdb_smallint(stmt, v, arg);
}

static SQLRETURN do_conv_sql_short_to_tsdb_int(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  int16_t v = *(int16_t*)arg->val;

  return do_conv_int64_to_tsdb_int(stmt, v, arg);
}

static SQLRETURN do_conv_sql_short_to_tsdb_bigint(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  int16_t v = *(int16_t*)arg->val;

  return do_conv_int64_to_tsdb_bigint(stmt, v, arg);
}

static SQLRETURN do_conv_sql_double_to_tsdb_float(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  double v = *(double*)arg->val;

  return do_conv_double_to_tsdb_float(stmt, v, arg);
}

static SQLRETURN do_conv_sql_double_to_tsdb_double(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  double v = *(double*)arg->val;

  return do_conv_double_to_tsdb_double(stmt, v, arg);
}

static SQLRETURN do_conv_sql_binary_to_tsdb_binary(stmt_t *stmt, tsdb_param_conv_arg_t *arg) {
  unsigned char       *buf             = (unsigned char*)arg->val;
  size_t               len             = (size_t)arg->soi;
  OILE(len>0, "");

  errs_t *errs = &stmt->errs;

  if (len > arg->tsdb_param->tsdb_bytes) {
    SET_OOM(errs, "failed to convert binary for param [%d], binary too long [%zd/%d]", arg->idx+1, len, arg->tsdb_param->tsdb_bytes);
    return SQL_ERROR;
  }

  tsdb_param_val_t    *tsdb_param_val = arg->tsdb_param_val;
  TAOS_BIND           *taos_bind      = arg->taos_bind;

  taos_bind->buffer_type   = TSDB_DATA_TYPE_BINARY;
  taos_bind->u.bin         = buf;
  taos_bind->buffer_length = len;
  taos_bind->buffer        = taos_bind->u.bin;
  taos_bind->length        = &taos_bind->buffer_length;
  taos_bind->is_null       = &tsdb_param_val->is_null;

  return SQL_SUCCESS;
}

static SQLRETURN do_set_param_wchar_conv_func(stmt_t *stmt, int idx, param_binding_t *binding, tsdb_param_t *tsdb_param) {
  errs_t *errs = &stmt->errs;

  SQLSMALLINT     valueType = binding->ValueType;
  int             tsdb_type = tsdb_param->tsdb_type;

  switch (tsdb_type)
  {
    case TSDB_DATA_TYPE_TIMESTAMP:
      {
        tsdb_param->conv = do_conv_sql_wchar_to_tsdb_timestamp;
      } break;
    case TSDB_DATA_TYPE_NCHAR:
      {
        tsdb_param->conv = do_conv_sql_wchar_to_tsdb_nchar;
      } break;
    case TSDB_DATA_TYPE_BOOL:
      {
        tsdb_param->conv = do_conv_sql_wchar_to_tsdb_bool;
      } break;
    case TSDB_DATA_TYPE_TINYINT:
      {
        tsdb_param->conv = do_conv_sql_wchar_to_tsdb_tinyint;
      } break;
    case TSDB_DATA_TYPE_SMALLINT:
      {
        tsdb_param->conv = do_conv_sql_wchar_to_tsdb_smallint;
      } break;
    case TSDB_DATA_TYPE_INT:
      {
        tsdb_param->conv = do_conv_sql_wchar_to_tsdb_int;
      } break;
    case TSDB_DATA_TYPE_BIGINT:
      {
        tsdb_param->conv = do_conv_sql_wchar_to_tsdb_bigint;
      } break;
    case TSDB_DATA_TYPE_FLOAT:
      {
        tsdb_param->conv = do_conv_sql_wchar_to_tsdb_float;
      } break;
    case TSDB_DATA_TYPE_DOUBLE:
      {
        tsdb_param->conv = do_conv_sql_wchar_to_tsdb_double;
      } break;
    default:
      {
        SET_GENERAL(errs, "not convertion from [%d]%s to [%d] %s for param [%d]",
            valueType, sql_c_type(valueType),
            tsdb_type, taos_data_type(tsdb_type), idx+1);
        return SQL_ERROR;
      } break;
  }

  return SQL_SUCCESS;
}

static SQLRETURN do_set_param_char_conv_func(stmt_t *stmt, int idx, param_binding_t *binding, tsdb_param_t *tsdb_param) {
  errs_t *errs = &stmt->errs;

  SQLSMALLINT     valueType = binding->ValueType;
  int             tsdb_type = tsdb_param->tsdb_type;

  switch (tsdb_type)
  {
    case TSDB_DATA_TYPE_TIMESTAMP:
      {
        tsdb_param->conv = do_conv_sql_char_to_tsdb_timestamp;
      } break;
    case TSDB_DATA_TYPE_NCHAR:
      {
        tsdb_param->conv = do_conv_sql_char_to_tsdb_nchar;
      } break;
    case TSDB_DATA_TYPE_BOOL:
      {
        tsdb_param->conv = do_conv_sql_char_to_tsdb_bool;
      } break;
    case TSDB_DATA_TYPE_TINYINT:
      {
        tsdb_param->conv = do_conv_sql_char_to_tsdb_tinyint;
      } break;
    case TSDB_DATA_TYPE_SMALLINT:
      {
        tsdb_param->conv = do_conv_sql_char_to_tsdb_smallint;
      } break;
    case TSDB_DATA_TYPE_INT:
      {
        tsdb_param->conv = do_conv_sql_char_to_tsdb_int;
      } break;
    case TSDB_DATA_TYPE_BIGINT:
      {
        tsdb_param->conv = do_conv_sql_char_to_tsdb_bigint;
      } break;
    case TSDB_DATA_TYPE_FLOAT:
      {
        tsdb_param->conv = do_conv_sql_char_to_tsdb_float;
      } break;
    case TSDB_DATA_TYPE_DOUBLE:
      {
        tsdb_param->conv = do_conv_sql_char_to_tsdb_double;
      } break;
    default:
      {
        SET_GENERAL(errs, "not convertion from [%d]%s to [%d] %s for param [%d]",
            valueType, sql_c_type(valueType),
            tsdb_type, taos_data_type(tsdb_type), idx+1);
        return SQL_ERROR;
      } break;
  }

  return SQL_SUCCESS;
}

static SQLRETURN do_set_param_sbigint_conv_func(stmt_t *stmt, int idx, param_binding_t *binding, tsdb_param_t *tsdb_param) {
  errs_t *errs = &stmt->errs;

  SQLSMALLINT     valueType = binding->ValueType;
  int             tsdb_type = tsdb_param->tsdb_type;

  switch (tsdb_type)
  {
    case TSDB_DATA_TYPE_BIGINT:
      {
        tsdb_param->conv = do_conv_sql_sbigint_to_tsdb_bigint;
      } break;
    case TSDB_DATA_TYPE_TINYINT:
      {
        tsdb_param->conv = do_conv_sql_sbigint_to_tsdb_tinyint;
      } break;
    case TSDB_DATA_TYPE_INT:
      {
        tsdb_param->conv = do_conv_sql_sbigint_to_tsdb_int;
      } break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      {
        tsdb_param->conv = do_conv_sql_sbigint_to_tsdb_timestamp;
      } break;
    default:
      {
        SET_GENERAL(errs, "not convertion from [%d]%s to [%d] %s for param [%d]",
            valueType, sql_c_type(valueType),
            tsdb_type, taos_data_type(tsdb_type), idx+1);
        return SQL_ERROR;
      } break;
  }

  return SQL_SUCCESS;
}

static SQLRETURN do_set_param_long_conv_func(stmt_t *stmt, int idx, param_binding_t *binding, tsdb_param_t *tsdb_param) {
  errs_t *errs = &stmt->errs;

  SQLSMALLINT     valueType = binding->ValueType;
  int             tsdb_type = tsdb_param->tsdb_type;

  switch (tsdb_type)
  {
    case TSDB_DATA_TYPE_BOOL:
      {
        tsdb_param->conv = do_conv_sql_long_to_tsdb_bool;
      } break;
    case TSDB_DATA_TYPE_TINYINT:
      {
        tsdb_param->conv = do_conv_sql_long_to_tsdb_tinyint;
      } break;
    case TSDB_DATA_TYPE_SMALLINT:
      {
        tsdb_param->conv = do_conv_sql_long_to_tsdb_smallint;
      } break;
    case TSDB_DATA_TYPE_INT:
      {
        tsdb_param->conv = do_conv_sql_long_to_tsdb_int;
      } break;
    case TSDB_DATA_TYPE_BIGINT:
      {
        tsdb_param->conv = do_conv_sql_long_to_tsdb_bigint;
      } break;
    default:
      {
        SET_GENERAL(errs, "not convertion from [%d]%s to [%d] %s for param [%d]",
            valueType, sql_c_type(valueType),
            tsdb_type, taos_data_type(tsdb_type), idx+1);
        return SQL_ERROR;
      } break;
  }

  return SQL_SUCCESS;
}

static SQLRETURN do_set_param_tinyint_conv_func(stmt_t *stmt, int idx, param_binding_t *binding, tsdb_param_t *tsdb_param) {
  errs_t *errs = &stmt->errs;

  SQLSMALLINT     valueType = binding->ValueType;
  int             tsdb_type = tsdb_param->tsdb_type;

  switch (tsdb_type)
  {
    case TSDB_DATA_TYPE_BOOL:
      {
        tsdb_param->conv = do_conv_sql_tinyint_to_tsdb_bool;
      } break;
    case TSDB_DATA_TYPE_TINYINT:
      {
        tsdb_param->conv = do_conv_sql_tinyint_to_tsdb_tinyint;
      } break;
    case TSDB_DATA_TYPE_SMALLINT:
      {
        tsdb_param->conv = do_conv_sql_tinyint_to_tsdb_smallint;
      } break;
    case TSDB_DATA_TYPE_INT:
      {
        tsdb_param->conv = do_conv_sql_tinyint_to_tsdb_int;
      } break;
    case TSDB_DATA_TYPE_BIGINT:
      {
        tsdb_param->conv = do_conv_sql_tinyint_to_tsdb_bigint;
      } break;
    default:
      {
        SET_GENERAL(errs, "not convertion from [%d]%s to [%d] %s for param [%d]",
            valueType, sql_c_type(valueType),
            tsdb_type, taos_data_type(tsdb_type), idx+1);
        return SQL_ERROR;
      } break;
  }

  return SQL_SUCCESS;
}

static SQLRETURN do_set_param_short_conv_func(stmt_t *stmt, int idx, param_binding_t *binding, tsdb_param_t *tsdb_param) {
  errs_t *errs = &stmt->errs;

  SQLSMALLINT     valueType = binding->ValueType;
  int             tsdb_type = tsdb_param->tsdb_type;

  switch (tsdb_type)
  {
    case TSDB_DATA_TYPE_BOOL:
      {
        tsdb_param->conv = do_conv_sql_short_to_tsdb_bool;
      } break;
    case TSDB_DATA_TYPE_TINYINT:
      {
        tsdb_param->conv = do_conv_sql_short_to_tsdb_tinyint;
      } break;
    case TSDB_DATA_TYPE_SMALLINT:
      {
        tsdb_param->conv = do_conv_sql_short_to_tsdb_smallint;
      } break;
    case TSDB_DATA_TYPE_INT:
      {
        tsdb_param->conv = do_conv_sql_short_to_tsdb_int;
      } break;
    case TSDB_DATA_TYPE_BIGINT:
      {
        tsdb_param->conv = do_conv_sql_short_to_tsdb_bigint;
      } break;
    default:
      {
        SET_GENERAL(errs, "not convertion from [%d]%s to [%d] %s for param [%d]",
            valueType, sql_c_type(valueType),
            tsdb_type, taos_data_type(tsdb_type), idx+1);
        return SQL_ERROR;
      } break;
  }

  return SQL_SUCCESS;
}

static SQLRETURN do_set_param_double_conv_func(stmt_t *stmt, int idx, param_binding_t *binding, tsdb_param_t *tsdb_param) {
  errs_t *errs = &stmt->errs;

  SQLSMALLINT     valueType = binding->ValueType;
  int             tsdb_type = tsdb_param->tsdb_type;

  switch (tsdb_type)
  {
    case TSDB_DATA_TYPE_FLOAT:
      {
        tsdb_param->conv = do_conv_sql_double_to_tsdb_float;
      } break;
    case TSDB_DATA_TYPE_DOUBLE:
      {
        tsdb_param->conv = do_conv_sql_double_to_tsdb_double;
      } break;
    default:
      {
        SET_GENERAL(errs, "not convertion from [%d]%s to [%d] %s for param [%d]",
            valueType, sql_c_type(valueType),
            tsdb_type, taos_data_type(tsdb_type), idx+1);
        return SQL_ERROR;
      } break;
  }

  return SQL_SUCCESS;
}

static SQLRETURN do_set_param_binary_conv_func(stmt_t *stmt, int idx, param_binding_t *binding, tsdb_param_t *tsdb_param) {
  errs_t *errs = &stmt->errs;

  SQLSMALLINT     valueType = binding->ValueType;
  int             tsdb_type = tsdb_param->tsdb_type;

  switch (tsdb_type)
  {
    case TSDB_DATA_TYPE_BINARY:
      {
        tsdb_param->conv = do_conv_sql_binary_to_tsdb_binary;
      } break;
    default:
      {
        SET_GENERAL(errs, "not convertion from [%d]%s to [%d] %s for param [%d]",
            valueType, sql_c_type(valueType),
            tsdb_type, taos_data_type(tsdb_type), idx+1);
        return SQL_ERROR;
      } break;
  }

  return SQL_SUCCESS;
}


static SQLRETURN do_set_param_conv_func(stmt_t *stmt, int idx, param_binding_t *binding) {
  tsdb_stmt_t  *tsdb_stmt   = (tsdb_stmt_t*)stmt->ext.ext;
  tsdb_param_t *tsdb_params = tsdb_stmt->tsdb_params;
  tsdb_param_t *tsdb_param  = tsdb_params + idx;

  errs_t *errs = &stmt->errs;

  SQLSMALLINT     valueType = binding->ValueType;

  switch (valueType)
  {
    case SQL_C_CHAR:
      {
        return do_set_param_char_conv_func(stmt, idx, binding, tsdb_param);
      } break;
    case SQL_C_WCHAR:
      {
        return do_set_param_wchar_conv_func(stmt, idx, binding, tsdb_param);
      } break;
    case SQL_C_SBIGINT:
      {
        return do_set_param_sbigint_conv_func(stmt, idx, binding, tsdb_param);
      } break;
    case SQL_C_LONG:
      {
        return do_set_param_long_conv_func(stmt, idx, binding, tsdb_param);
      } break;
    case SQL_C_TINYINT:
      {
        return do_set_param_tinyint_conv_func(stmt, idx, binding, tsdb_param);
      } break;
    case SQL_C_SHORT:
      {
        return do_set_param_short_conv_func(stmt, idx, binding, tsdb_param);
      } break;
    case SQL_C_DOUBLE:
      {
        return do_set_param_double_conv_func(stmt, idx, binding, tsdb_param);
      } break;
    case SQL_C_BINARY:
      {
        return do_set_param_binary_conv_func(stmt, idx, binding, tsdb_param);
      } break;
    default:
      {
        SET_GENERAL(errs, "not convertion from [%d]%s for param [%d]",
            valueType, sql_c_type(valueType), idx+1);
        return SQL_ERROR;
      } break;
  }

  return SQL_SUCCESS;
}

static SQLRETURN tsdb_conn_set_param_conv(stmt_t *stmt, int idx) {
  OILE(stmt, "");
  OILE(stmt->owner, "");
  tsdb_stmt_t *tsdb_stmt = (tsdb_stmt_t*)stmt->ext.ext;
  OILE(tsdb_stmt && tsdb_stmt->stmt==stmt, "");
  OILE(tsdb_stmt->tsdb_res==NULL, "");
  OILE(idx>=0, "");

  paramset_t *paramset = &stmt->paramset;
  param_t         *params      = paramset->params;
  if (!params || idx>=paramset->n_params) return SQL_SUCCESS;
  param_binding_t *bindings    = paramset->bindings;
  if (!bindings || idx>=paramset->n_bindings) return SQL_SUCCESS;
  tsdb_param_t    *tsdb_params = tsdb_stmt->tsdb_params;
  OILE(tsdb_params, "");

  param_binding_t *binding = bindings + idx;

  return do_set_param_conv_func(stmt, idx, binding);
}

static SQLRETURN do_fill_param(stmt_t *stmt, int idx) {
  OILE(stmt, "");
  OILE(stmt->owner, "");
  tsdb_stmt_t *tsdb_stmt = (tsdb_stmt_t*)stmt->ext.ext;
  OILE(tsdb_stmt && tsdb_stmt->stmt==stmt, "");
  OILE(tsdb_stmt->tsdb_res==NULL, "");

  paramset_t *paramset = &stmt->paramset;
  param_t         *params      = paramset->params;
  tsdb_param_t    *tsdb_params = tsdb_stmt->tsdb_params;
  OILE(params, "");
  OILE(tsdb_params, "");
  OILE(idx>=0, "");
  OILE(idx<paramset->n_params, "");
  param_t         *param       = params + idx;
  tsdb_param_t    *tsdb_param  = tsdb_params + idx;

  errs_t *errs = &stmt->errs;

  int tsdb_type  = 0;
  int tsdb_bytes = 0;
  int r = taos_stmt_get_param(tsdb_stmt->tsdb_stmt, idx, &tsdb_type, &tsdb_bytes);
  if (r) {
    SET_GENERAL(errs, "failed to get param[%d]", idx+1);
    return SQL_ERROR;
  }

  // https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/column-size-decimal-digits-transfer-octet-length-and-display-size?view=sql-server-ver15
  param->DecimalDigits     = 0;
  param->Nullable          = SQL_NULLABLE;
  tsdb_param->tsdb_type  = tsdb_type;
  tsdb_param->tsdb_bytes = tsdb_bytes;
  switch (tsdb_type)
  {
    case TSDB_DATA_TYPE_TIMESTAMP:
      {
        param->DataType          = SQL_CHAR;
        param->ParameterSize     = 23;
      } break;
    case TSDB_DATA_TYPE_NCHAR:
      {
        size_t bytes             = ((size_t)tsdb_bytes - VARSTR_HEADER_SIZE);
        size_t chars             = bytes / TSDB_NCHAR_SIZE;
        tsdb_param->tsdb_bytes   = (int)bytes;
        param->DataType          = SQL_WCHAR;
        param->ParameterSize     = (SQLULEN)chars;
      } break;
    case TSDB_DATA_TYPE_BINARY:
      {
        size_t bytes             = ((size_t)tsdb_bytes - VARSTR_HEADER_SIZE);
        tsdb_param->tsdb_bytes   = (int)bytes;
        param->DataType          = SQL_BINARY;
        param->ParameterSize     = (SQLULEN)bytes;
      } break;
    case TSDB_DATA_TYPE_BOOL:
      {
        param->DataType          = SQL_BIT;
        param->ParameterSize     = 1;
      } break;
    case TSDB_DATA_TYPE_TINYINT:
      {
        param->DataType          = SQL_TINYINT;
        param->ParameterSize     = 3;
      } break;
    case TSDB_DATA_TYPE_SMALLINT:
      {
        param->DataType          = SQL_SMALLINT;
        param->ParameterSize     = 5;
      } break;
    case TSDB_DATA_TYPE_INT:
      {
        param->DataType          = SQL_INTEGER;
        param->ParameterSize     = 10;
      } break;
    case TSDB_DATA_TYPE_BIGINT:
      {
        param->DataType          = SQL_BIGINT;
        param->ParameterSize     = 20;
      } break;
    case TSDB_DATA_TYPE_FLOAT:
      {
        param->DataType          = SQL_FLOAT;
        param->ParameterSize     = 15;
      } break;
    case TSDB_DATA_TYPE_DOUBLE:
      {
        param->DataType          = SQL_DOUBLE;
        param->ParameterSize     = 15;
      } break;
    default:
      {
        SET_GENERAL(errs, "failed to map param[%d] type[%d]%s to SQL DATA TYPE",
            idx+1, tsdb_type, taos_data_type(tsdb_type));
        return SQL_ERROR;
      } break;
  }

  param->ParameterNumber = (SQLUSMALLINT)(idx + 1);

  param_binding_t *bindings    = paramset->bindings;
  if (!bindings) return SQL_SUCCESS;
  if (idx>=paramset->n_bindings) return SQL_SUCCESS;

  param_binding_t *binding     = bindings + idx;

  return do_set_param_conv_func(stmt, idx, binding);
}

static SQLRETURN tsdb_conn_prepare(stmt_t *stmt) {
  OILE(stmt, "");
  OILE(stmt->owner, "");
  OILE(stmt->prepared==0, "");
  OILE(STMT_IS_NORM(stmt), "");
  errs_t *errs = &stmt->errs;

  conn_t *conn = stmt->owner->conn;
  OILE(conn, "");
  tsdb_conn_t *tsdb_conn = (tsdb_conn_t*)conn->ext.ext;
  OILE(tsdb_conn && tsdb_conn->conn==conn, "");
  TAOS *taos = tsdb_conn->taos;
  OILE(taos, "");

  tsdb_stmt_t *tsdb_stmt = (tsdb_stmt_t*)stmt->ext.ext;
  OILE(tsdb_stmt && tsdb_stmt->stmt==stmt, "");
  OILE(tsdb_stmt->tsdb_res==NULL, "");
  // OILE(tsdb_stmt->tsdb_stmt==NULL, "");

  tsdb_stmt->tsdb_params = NULL;

  if (!tsdb_stmt->tsdb_stmt) {
    SQLRETURN r = tsdb_stmt_init_stmt(tsdb_stmt);
    if (r!=SQL_SUCCESS) return r;
  }
  OILE(tsdb_stmt->tsdb_stmt, "");

  tsdb_stmt_reclaim_param_vals(tsdb_stmt);

  do {
    const char *txt = (const char*)stmt->sql.txt.buf;
    size_t      len = stmt->sql.txt.bytes;
    OILE(txt, "");
    OILE(len>0, "");
    int r = taos_stmt_prepare(tsdb_stmt->tsdb_stmt, txt, (unsigned int)len);
    if (r) {
      SET_GENERAL(errs, "failed to prepare taos stmt:[%d]%s", r, tstrerror(r));
      break;
    }

    int nums = 0;
    r = taos_stmt_num_params(tsdb_stmt->tsdb_stmt, &nums);
    if (r) {
      SET_GENERAL(errs, "failed to prepare taos stmt:[%d]%s", r, tstrerror(r));
      break;
    }

    paramset_t *paramset = &stmt->paramset;
    OILE(paramset->params==NULL, "");
    OILE(paramset->n_params==0, "");
    OILE(tsdb_stmt->tsdb_params==NULL, "");

    if (nums>0) {
      paramset_init_params_cache(paramset);
      tsdb_stmt_init_param_vals_cache(tsdb_stmt);

      if (!tsdb_stmt->tsdb_param_vals_cache) {
        SET_OOM(errs, "failed to alloc val cache for params");
        return SQL_ERROR;
      }

      todbc_buf_t *cache = stmt->paramset.params_cache;
      if (!cache) {
        SET_OOM(errs, "failed to alloc cache buffer for params");
        return SQL_ERROR;
      }
      OILE(cache, "");

      param_t *params = (param_t*)todbc_buf_calloc(cache, (size_t)nums, sizeof(*params));
      if (!params) {
        SET_OOM(errs, "failed to alloc buffer for params");
        return SQL_ERROR;
      }

      tsdb_param_t *tsdb_params = (tsdb_param_t*)todbc_buf_calloc(cache, (size_t)nums, sizeof(*tsdb_params));
      if (!tsdb_params) {
        SET_OOM(errs, "failed to alloc buffer for tsdb params");
        return SQL_ERROR;
      }

      paramset->params           = params;
      paramset->n_params         = nums;
      tsdb_stmt->tsdb_params     = tsdb_params;

      for (int i=0; i<nums; ++i) {
        SQLRETURN r = do_fill_param(stmt, i);
        if (r) return r;
      }
    }

    return SQL_SUCCESS;
  } while (0);

  tsdb_stmt_close_stmt(tsdb_stmt);
  return SQL_ERROR;
}

static SQLRETURN do_conv_utf8_to_sql_c_char(stmt_t *stmt, const char *buf, col_binding_t *col) {
  OILE(stmt, "");
  OILE(stmt->owner, "");

  errs_t *errs = &stmt->errs;

  conn_t *conn = stmt->owner->conn;
  OILE(conn, "");

  const char          *enc_to          = conn->enc_char;
  const char          *enc_from        = conn->enc_src; // UTF8
  const unsigned char *src             = (const unsigned char*)buf;
  size_t               slen            = strlen(buf);
  unsigned char       *dst             = (unsigned char*)col->TargetValue;
  size_t               dlen            = (size_t)col->BufferLength;
  todbc_string_t s = todbc_tls_write(enc_to, enc_from, src, &slen, dst, dlen);
  if (!s.buf) {
    SET_OOM(errs, "failed to convert timestamp");
    return SQL_ERROR;
  }
  OILE(s.bytes==s.total_bytes, "");
  if (col->StrLen_or_IndPtr) {
    *col->StrLen_or_IndPtr = (SQLLEN)s.bytes;
  }
  for(size_t i=s.bytes; i<dlen; ++i) {
    dst[i] = '\0'; // in case, the app forgets to check StrLen_orInd
  }

  return SQL_SUCCESS;
}

static SQLRETURN do_conv_binary_to_sql_c(stmt_t *stmt, TAOS_FIELD *field, void *val, size_t bytes, col_binding_t *binding) {
  errs_t *errs = &stmt->errs;

  switch (binding->TargetType)
  {
    case SQL_C_CHAR:
      {
        size_t len = (size_t)binding->BufferLength;
        OILE(len>0, "");
        if (bytes<len) {
          len = bytes;
        }
        if (binding->TargetValue) {
          memcpy(binding->TargetValue, val, len);
        }
        if (binding->StrLen_or_IndPtr) {
          *binding->StrLen_or_IndPtr = (SQLLEN)len;
        }
        // do we really need this?
        size_t dlen = (size_t)binding->BufferLength;
        unsigned char *dst = (unsigned char*)binding->TargetValue;
        for(size_t i=len; i<dlen; ++i) {
          dst[i] = '\0'; // in case, the app forgets to check StrLen_orInd
        }
      } break;
    case SQL_C_BINARY:
      {
        size_t len = (size_t)binding->BufferLength;
        OILE(len>0, "");
        if (bytes<len) {
          len = bytes;
        }
        if (binding->TargetValue) {
          memcpy(binding->TargetValue, val, len);
        }
        if (binding->StrLen_or_IndPtr) {
          *binding->StrLen_or_IndPtr = (SQLLEN)len;
        }
      } break;
    default:
      {
        SET_GENERAL(errs, "not convertion from [%d]%s to [%d] %s for binding[%d]",
            field->type, taos_data_type(field->type),
            binding->TargetType, sql_c_type(binding->TargetType),
            binding->ColumnNumber);
        return SQL_ERROR;
      } break;
  }

  return SQL_SUCCESS;
}

static SQLRETURN do_conv_nchar_to_sql_c(stmt_t *stmt, TAOS_FIELD *field, void *val, size_t bytes, col_binding_t *binding) {
  errs_t *errs = &stmt->errs;

  OILE(stmt->owner, "");
  conn_t *conn = stmt->owner->conn;
  OILE(conn, "");

  switch (binding->TargetType)
  {
    case SQL_C_CHAR:
      {
        const char          *enc_to          = conn->enc_char;
        const char          *enc_from        = conn->enc_db;
        const unsigned char *src             = (const unsigned char*)val;
        size_t               slen            = bytes;
        unsigned char       *dst             = (unsigned char*)binding->TargetValue;
        size_t               dlen            = (size_t)binding->BufferLength;
        todbc_string_t s = todbc_tls_write(enc_to, enc_from, src, &slen, dst, dlen);
        if (!s.buf) {
          SET_OOM(errs, "failed to convert nchar");
          return SQL_ERROR;
        }
        OILE(s.bytes==s.total_bytes, "");
        if (binding->StrLen_or_IndPtr) {
          *binding->StrLen_or_IndPtr = (SQLLEN)s.bytes;  // com-on, it's NOT character-size
        }
        for(size_t i=s.bytes; i<dlen; ++i) {
          dst[i] = '\0'; // in case, the app forgets to check StrLen_orInd
        }
      } break;
    case SQL_C_WCHAR:
      {
        const char          *enc_to          = conn->enc_wchar;
        const char          *enc_from        = conn->enc_db;
        const unsigned char *src             = (const unsigned char*)val;
        size_t               slen            = bytes;
        unsigned char       *dst             = (unsigned char*)binding->TargetValue;
        size_t               dlen            = (size_t)binding->BufferLength;
        todbc_string_t s = todbc_tls_write(enc_to, enc_from, src, &slen, dst, dlen);
        if (!s.buf) {
          SET_OOM(errs, "failed to convert nchar");
          return SQL_ERROR;
        }
        OILE(s.bytes==s.total_bytes, "");
        if (binding->StrLen_or_IndPtr) {
          *binding->StrLen_or_IndPtr = (SQLLEN)s.bytes;  // com-on, it's NOT character-size
        }
        for(size_t i=s.bytes; i<dlen; ++i) {
          dst[i] = '\0'; // in case, the app forgets to check StrLen_orInd
        }
      } break;
    default:
      {
        SET_GENERAL(errs, "not convertion from [%d]%s to [%d] %s for binding[%d]",
            field->type, taos_data_type(field->type),
            binding->TargetType, sql_c_type(binding->TargetType),
            binding->ColumnNumber);
        return SQL_ERROR;
      } break;
  }

  return SQL_SUCCESS;
}

static SQLRETURN do_conv_int64_to_sql_c(stmt_t *stmt, TAOS_FIELD *field, int64_t val, col_binding_t *binding) {
  errs_t *errs = &stmt->errs;

  char buf[128];

  switch (binding->TargetType)
  {
    case SQL_C_CHAR:
      {
        snprintf(buf, sizeof(buf), "%" PRId64 "", val);
        return do_conv_utf8_to_sql_c_char(stmt, buf, binding);
      } break;
    case SQL_C_UTINYINT:
      {
        if (val>UINT8_MAX || val<0) {
          SET_GENERAL(errs, "");
          return SQL_ERROR;
        }
        if (binding->TargetValue) {
          *(uint8_t*)binding->TargetValue = (uint8_t)val;
        }
      } break;
    case SQL_C_USHORT:
      {
        if (val>UINT16_MAX || val<0) {
          SET_GENERAL(errs, "");
          return SQL_ERROR;
        }
        if (binding->TargetValue) {
          *(uint16_t*)binding->TargetValue = (uint16_t)val;
        }
      } break;
    case SQL_C_SLONG:
      {
        if (val>INT32_MAX || val<INT32_MIN) {
          SET_GENERAL(errs, "");
          return SQL_ERROR;
        }
        if (binding->TargetValue) {
          *(int32_t*)binding->TargetValue = (int32_t)val;
        }
      } break;
    case SQL_C_UBIGINT:
      {
        if (binding->TargetValue) {
          *(uint64_t*)binding->TargetValue = (uint64_t)val;
        }
      } break;
    default:
      {
        SET_GENERAL(errs, "not convertion from [%d]%s to [%d] %s for binding[%d]",
            field->type, taos_data_type(field->type),
            binding->TargetType, sql_c_type(binding->TargetType),
            binding->ColumnNumber);
        return SQL_ERROR;
      } break;
  }

  return SQL_SUCCESS;
}

static SQLRETURN do_conv_timestamp_to_utf8(stmt_t *stmt, int64_t v, char *buf, size_t len) {
  errs_t *errs = &stmt->errs;

  // microsecond precision, based on test
  time_t secs   = v / 1000;
  int    msecs  = (int)(v % 1000);

  struct tm vtm = {0};
  if (&vtm!=localtime_r(&secs, &vtm)) {
    SET_ERR(errs, "22007", "invalid timestamp");
    return SQL_ERROR; // ? SQL_SUCCESS_WITH_INFO
  }

  char  *p     = buf;
  size_t bytes = len;

  OILE(bytes>0, "");
  size_t n = strftime(p, bytes, "%Y-%m-%d %H:%M:%S", &vtm);
  if (n==0) {
    SET_GENERAL(errs, "failed to convert timestamp");
    return SQL_ERROR; // ? SQL_SUCCESS_WITH_INFO
  }
  p     += n;
  bytes -= n;

  OILE(bytes>0, "");
  int m = snprintf(p, bytes, ".%03d", msecs);
  if (m>=bytes) {
    SET_GENERAL(errs, "failed to convert timestamp");
    return SQL_ERROR; // ? SQL_SUCCESS_WITH_INFO
  }
  p     += (size_t)m;
  bytes -= (size_t)m;

  OILE(bytes>=0, "");

  return SQL_SUCCESS;
}

static SQLRETURN do_conv_timestamp_to_sql_c(stmt_t *stmt, TAOS_FIELD *field, int64_t val, col_binding_t *col) {
  errs_t *errs = &stmt->errs;

  OILE(stmt, "");
  OILE(stmt->owner, "");

  conn_t *conn = stmt->owner->conn;
  OILE(conn, "");

  SQLRETURN r;
  char buf[128];

  switch (col->TargetType)
  {
    case SQL_C_CHAR:
      {
        r = do_conv_timestamp_to_utf8(stmt, val, buf, sizeof(buf));
        if (r!=SQL_SUCCESS) return r;
        return do_conv_utf8_to_sql_c_char(stmt, buf, col);
      } break;
    default:
      {
        SET_GENERAL(errs, "not convertion from [%d]%s to [%d] %s for col[%d]",
            field->type, taos_data_type(field->type),
            col->TargetType, sql_c_type(col->TargetType),
            col->ColumnNumber);
        return SQL_ERROR;
      } break;
  }

  return SQL_SUCCESS;
}

static SQLRETURN do_conv_double_to_sql_c(stmt_t *stmt, TAOS_FIELD *field, double val, col_binding_t *binding) {
  errs_t *errs = &stmt->errs;

  char buf[256];

  switch (binding->TargetType)
  {
    case SQL_C_DOUBLE:
      {
        if (binding->TargetValue) {
          *(double*)binding->TargetValue = val;
        }
      } break;
    case SQL_C_FLOAT:
      {
        // shall we check overflow/underflow here?
        if (binding->TargetValue) {
          *(float*)binding->TargetValue = (float)val;
        }
      } break;
    case SQL_C_CHAR:
      {
        snprintf(buf, sizeof(buf), "%lf", val);
        return do_conv_utf8_to_sql_c_char(stmt, buf, binding);
      } break;
    default:
      {
        SET_GENERAL(errs, "not convertion from [%d]%s to [%d] %s for binding[%d]",
            field->type, taos_data_type(field->type),
            binding->TargetType, sql_c_type(binding->TargetType),
            binding->ColumnNumber);
        return SQL_ERROR;
      } break;
  }

  return SQL_SUCCESS;
}

static SQLRETURN tsdb_conn_proc_param(stmt_t *stmt) {
  OILE(stmt, "");
  OILE(stmt->owner, "");
  tsdb_stmt_t     *tsdb_stmt   = (tsdb_stmt_t*)stmt->ext.ext;
  OILE(tsdb_stmt && tsdb_stmt->stmt==stmt, "");

  paramset_t      *paramset    = &stmt->paramset;
  param_t         *params      = paramset->params;
  int              n_params    = paramset->n_params;
  param_binding_t *bindings    = paramset->bindings;
  int              n_bindings  = paramset->n_bindings;
  tsdb_param_t    *tsdb_params = tsdb_stmt->tsdb_params;
  int              i_row       = paramset->i_row;
  int              i_col       = paramset->i_col;

  OILE(params && n_params>0, "");
  OILE(bindings && n_bindings>=0, "");
  OILE(tsdb_params, "");
  OILE(n_params==n_bindings, "");
  OILE(i_row>=0, "");
  OILE(i_col>=0 && i_col<n_params, "");

  // SQL_ATTR_PARAM_BIND_TYPE: SQL_PARAM_BIND_BY_COLUMN or row size
  SQLULEN   bind_type         = stmt->attr.bind_type;             // default: SQL_PARAM_BIND_BY_COLUMN
  // SQL_ATTR_PARAM_BIND_OFFSET_PTR
  SQLULEN  *bind_offset_ptr   = stmt->attr.bind_offset_ptr;       // default: NULL
  // SQL_ATTR_PARAMSET_SIZE
  SQLULEN   paramset_size     = stmt->attr.paramset_size;         // default: 0

  // OILE(bind_type && bind_type!=SQL_PARAM_BIND_BY_COLUMN, "[%ld]", bind_type);
  // OILE(bind_offset_ptr, "");
  OILE(paramset_size>0, "");

  OILE(i_row<paramset_size, "");

  param_t *param = params + (size_t)i_col;
  OILE(param->ParameterNumber>0 && param->ParameterNumber<=n_params, "");
  tsdb_param_t *tsdb_param = tsdb_params + i_col;
  OILE(tsdb_param->conv, "");

  tsdb_param_val_t *tsdb_param_vals = tsdb_stmt->tsdb_param_vals;
  if (!tsdb_param_vals) {
    errs_t *errs = &stmt->errs;
    tsdb_stmt_calloc_param_vals(tsdb_stmt);
    if (tsdb_stmt->tsdb_param_vals==NULL) {
      SET_OOM(errs, "failed to alloc tsdb param vals for tsdb params");
      return SQL_ERROR;
    }
    if (tsdb_stmt->taos_binds==NULL) {
      SET_OOM(errs, "failed to alloc taos binds for tsdb params");
      return SQL_ERROR;
    }
    tsdb_param_vals = tsdb_stmt->tsdb_param_vals;
  }
  OILE(tsdb_param_vals, "");
  TAOS_BIND        *taos_binds      = tsdb_stmt->taos_binds;
  OILE(taos_binds, "");


  tsdb_param_val_t *tsdb_param_val = tsdb_param_vals + i_col;
  tsdb_param_val->ParameterNumber  = (SQLUSMALLINT)(i_col + 1);
  tsdb_param_val->is_null          = 1;
  TAOS_BIND        *taos_bind      = taos_binds + i_col;

  param_binding_t *binding = bindings + (size_t)i_col;
  OILE(binding->ParameterNumber==i_col+1, "");
  if (binding->ParameterValuePtr==NULL) return SQL_SUCCESS;

  SQLPOINTER val    = binding->ParameterValuePtr;
  SQLLEN    *soip   = binding->StrLen_or_IndPtr;
  OILE(soip, "");

  size_t offset = (size_t)i_row * (size_t)bind_type;
  size_t bind_offset = 0;
  if (bind_offset_ptr) bind_offset = *bind_offset_ptr;

  val   = (SQLPOINTER)(((char*)val) + offset + bind_offset);
  soip  = (SQLLEN*)(((char*)soip) + offset + bind_offset);

  SQLLEN soi = *soip;

  if (soi == SQL_NULL_DATA) return SQL_SUCCESS;

  OILE(soi>=0 || soi==SQL_NTS, "");

  tsdb_param_val->is_null = 0;

  conn_t *conn = stmt->owner->conn;
  OILE(conn, "");
  todbc_buf_t *cache = tsdb_stmt->tsdb_param_vals_cache;
  OILE(cache, "");

  tsdb_param_conv_arg_t arg = {
    .conn               = conn,
    .cache              = cache,
    .idx                = i_col,
    .val                = val,
    .soi                = soi,
    .tsdb_param         = tsdb_param,
    .tsdb_param_val     = tsdb_param_val,
    .taos_bind          = taos_bind
  };
  return tsdb_param->conv(stmt, &arg);
}

static SQLRETURN tsdb_conn_param_row_processed(stmt_t *stmt) {
  paramset_t *paramset = &stmt->paramset;
  OILE(paramset->n_params>0, "");
  tsdb_stmt_t *tsdb_stmt = (tsdb_stmt_t*)stmt->ext.ext;
  OILE(tsdb_stmt, "");
  TAOS_BIND *taos_binds = tsdb_stmt->taos_binds;
  OILE(taos_binds, "");
  OILE(tsdb_stmt->tsdb_stmt, "");

  errs_t *errs = &stmt->errs;

  if (1) {
    int r = taos_stmt_bind_param(tsdb_stmt->tsdb_stmt, taos_binds);
    if (r) {
      SET_GENERAL(errs, "failed to bind params:[%d]%s", r, tstrerror(r));
      // keep executing/executed state unchanged
      return SQL_ERROR;
    }

    r = taos_stmt_add_batch(tsdb_stmt->tsdb_stmt);
    if (r) {
      SET_GENERAL(errs, "failed to add batch params:[%d]%s", r, tstrerror(r));
      // keep executing/executed state unchanged
      return SQL_ERROR;
    }
  }

  return SQL_SUCCESS;
}

static SQLRETURN tsdb_conn_execute(stmt_t *stmt) {
  OILE(stmt, "");
  OILE(stmt->owner, "");
  OILE(!STMT_IS_EXECUTED(stmt), "");

  tsdb_stmt_t *tsdb_stmt  = (tsdb_stmt_t*)stmt->ext.ext;
  OILE(tsdb_stmt, "");
  if (!tsdb_stmt->tsdb_stmt) {
    SQLRETURN r = tsdb_stmt_init_stmt(tsdb_stmt);
    if (r!=SQL_SUCCESS) return r;
    OILE(0, "");
  }
  OILE(tsdb_stmt->tsdb_stmt, "");

  errs_t * errs = &stmt->errs;

  if (1) {
    int r = 0;

    r = taos_stmt_execute(tsdb_stmt->tsdb_stmt);
    if (r) {
      SET_GENERAL(errs, "failed to execute:[%d]%s", r, tstrerror(r));
      // keep executing/executed state unchanged
      return SQL_ERROR;
    }

    tsdb_stmt->by_query = 0;
  }

  STMT_SET_EXECUTED(stmt);
  return SQL_SUCCESS;
}

static void do_fetch_tsdb_res(tsdb_stmt_t *tsdb_stmt) {
  if (!tsdb_stmt->tsdb_res) {
    OILE(tsdb_stmt->by_query==0, "");
    OILE(tsdb_stmt->tsdb_stmt, "");
    tsdb_stmt->tsdb_res = taos_stmt_use_result(tsdb_stmt->tsdb_stmt);
    OILE(tsdb_stmt->tsdb_res, "");
    // currently, TAOS_STMT does NOT co-exist with TAOS_RES
    tsdb_stmt_close_stmt(tsdb_stmt);
  }
}

SQLRETURN tsdb_conn_get_affected_rows(stmt_t *stmt, SQLLEN *RowCount) {
  OILE(stmt, "");
  OILE(stmt->owner, "");

  tsdb_stmt_t *tsdb_stmt = (tsdb_stmt_t*)stmt->ext.ext;
  OILE(tsdb_stmt, "");

  do_fetch_tsdb_res(tsdb_stmt);

  int rows = taos_affected_rows(tsdb_stmt->tsdb_res);
  OILE(RowCount, "");
  *RowCount = rows;

  return SQL_SUCCESS;
}

static void do_fetch_tsdb_fields(tsdb_stmt_t *tsdb_stmt) {
  if (!tsdb_stmt->tsdb_fields) {
    tsdb_stmt->tsdb_fields = taos_fetch_fields(tsdb_stmt->tsdb_res);
  }
}

SQLRETURN tsdb_conn_get_fields_count(stmt_t *stmt, SQLSMALLINT *ColumnCount) {
  OILE(stmt, "");
  OILE(stmt->owner, "");

  tsdb_stmt_t *tsdb_stmt = (tsdb_stmt_t*)stmt->ext.ext;
  OILE(tsdb_stmt, "");

  do_fetch_tsdb_res(tsdb_stmt);
  OILE(tsdb_stmt->tsdb_res, "");

  int n_fields = taos_num_fields(tsdb_stmt->tsdb_res);
  OILE(ColumnCount, "");
  *ColumnCount = (SQLSMALLINT)n_fields;

  return SQL_SUCCESS;
}

SQLRETURN tsdb_conn_get_field(stmt_t *stmt, field_arg_t *arg) {
  OILE(stmt, "");
  OILE(stmt->owner, "");

  errs_t *errs = &stmt->errs;

  tsdb_stmt_t *tsdb_stmt = (tsdb_stmt_t*)stmt->ext.ext;
  OILE(tsdb_stmt, "");

  do_fetch_tsdb_res(tsdb_stmt);
  OILE(tsdb_stmt->tsdb_res, "");

  int n_fields = taos_num_fields(tsdb_stmt->tsdb_res);
  OILE(arg->ColumnNumber>0, "");
  OILE(arg->ColumnNumber<=n_fields, "");
  do_fetch_tsdb_fields(tsdb_stmt);
  OILE(tsdb_stmt->tsdb_fields, "");

  TAOS_FIELD *field = tsdb_stmt->tsdb_fields + (arg->ColumnNumber-1);
  int len = 0;
  // charset ?
  len = snprintf((char*)arg->ColumnName, (size_t)arg->BufferLength, "%s", field->name); 
  if (arg->NameLength)      *arg->NameLength    = (SQLSMALLINT)len;
  if (arg->DecimalDigits)   *arg->DecimalDigits = 0;
  if (arg->Nullable)        *arg->Nullable      = SQL_NULLABLE;

  SQLSMALLINT DataType;
  SQLULEN ColumnSize;

  // https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/column-size-decimal-digits-transfer-octet-length-and-display-size?view=sql-server-ver15
  switch (field->type) {
    case TSDB_DATA_TYPE_TIMESTAMP:
      {
        DataType       = SQL_CHAR;
        ColumnSize     = 23;
      } break;
    case TSDB_DATA_TYPE_NCHAR:
      {
        DataType          = SQL_WCHAR;
        ColumnSize        = (SQLULEN)field->bytes;
      } break;
    case TSDB_DATA_TYPE_BINARY:
      {
        DataType          = SQL_BINARY;
        ColumnSize        = (SQLULEN)field->bytes;
      } break;
    case TSDB_DATA_TYPE_BOOL:
      {
        DataType          = SQL_BIT;
        ColumnSize        = 1;
      } break;
    case TSDB_DATA_TYPE_TINYINT:
      {
        DataType          = SQL_TINYINT;
        ColumnSize        = 3;
      } break;
    case TSDB_DATA_TYPE_SMALLINT:
      {
        DataType          = SQL_SMALLINT;
        ColumnSize        = 5;
      } break;
    case TSDB_DATA_TYPE_INT:
      {
        DataType          = SQL_INTEGER;
        ColumnSize        = 10;
      } break;
    case TSDB_DATA_TYPE_BIGINT:
      {
        DataType          = SQL_BIGINT;
        ColumnSize        = 20;
      } break;
    case TSDB_DATA_TYPE_FLOAT:
      {
        DataType          = SQL_FLOAT;
        ColumnSize        = 15;
      } break;
    case TSDB_DATA_TYPE_DOUBLE:
      {
        DataType          = SQL_DOUBLE;
        ColumnSize        = 15;
      } break;
    default:
      {
        SET_GENERAL(errs, "failed to map field[%d] type[%d]%s to SQL DATA TYPE",
            arg->ColumnNumber, field->type, taos_data_type(field->type));
        return SQL_ERROR;
      } break;
  }

  if (arg->DataType)       *arg->DataType       = DataType;
  if (arg->ColumnSize)     *arg->ColumnSize     = ColumnSize;

  return SQL_SUCCESS;
}

SQLRETURN tsdb_conn_fetch(stmt_t *stmt) {
  OILE(stmt, "");
  OILE(stmt->owner, "");

  tsdb_stmt_t *tsdb_stmt = (tsdb_stmt_t*)stmt->ext.ext;
  OILE(tsdb_stmt, "");

  OILE(stmt->eof==0, "");

  do_fetch_tsdb_res(tsdb_stmt);
  OILE(tsdb_stmt->tsdb_res, "");

  tsdb_stmt->tsdb_curr = taos_fetch_row(tsdb_stmt->tsdb_res);
  if (!tsdb_stmt->tsdb_curr) {
    stmt->eof = 1;
    return SQL_NO_DATA;
  }

  return SQL_SUCCESS;
}

static SQLRETURN tsdb_conn_get_data(stmt_t *stmt, col_binding_t *col) {
  OILE(stmt, "");
  OILE(stmt->owner, "");

  tsdb_stmt_t *tsdb_stmt = (tsdb_stmt_t*)stmt->ext.ext;
  OILE(tsdb_stmt, "");

  OILE(stmt->eof==0, "");
  OILE(tsdb_stmt->tsdb_curr, "");
  OILE(tsdb_stmt->tsdb_res, "");

  int n_fields = taos_num_fields(tsdb_stmt->tsdb_res);
  int idx = (int)(col->ColumnNumber-1);
  OILE(idx>=0, "");
  OILE(idx<n_fields, "");
  do_fetch_tsdb_fields(tsdb_stmt);
  OILE(tsdb_stmt->tsdb_fields, "");

  TAOS_FIELD *field = tsdb_stmt->tsdb_fields + idx;

  OILE(col->StrLen_or_IndPtr, "");
  void *val = tsdb_stmt->tsdb_curr[idx];
  if (!val) {
    *col->StrLen_or_IndPtr = SQL_NULL_DATA;
    return SQL_SUCCESS;
  }

  errs_t *errs = &stmt->errs;

  int64_t i64;
  double  dbl;
  int     is_dbl = 0;

  switch (field->type)
  {
    case TSDB_DATA_TYPE_TINYINT:
      {
        i64 = *(int8_t*)val;
      } break;
    case TSDB_DATA_TYPE_SMALLINT:
      {
        i64 = *(int16_t*)val;
      } break;
    case TSDB_DATA_TYPE_INT:
      {
        i64 = *(int32_t*)val;
      } break;
    case TSDB_DATA_TYPE_BIGINT:
      {
        i64 = *(int64_t*)val;
      }  break;
    case TSDB_DATA_TYPE_FLOAT:
      {
        dbl = GET_FLOAT_VAL(val);
        is_dbl = 1;
      } break;
    case TSDB_DATA_TYPE_DOUBLE:
      {
        dbl = GET_DOUBLE_VAL(val);
        is_dbl = 1;
      } break;
    case TSDB_DATA_TYPE_BINARY:
      {
        size_t bytes = (size_t)varDataLen((char*)val - VARSTR_HEADER_SIZE);
        return do_conv_binary_to_sql_c(stmt, field, val, bytes, col);
      } break;
    case TSDB_DATA_TYPE_NCHAR:
      {
        size_t bytes = (size_t)varDataLen((char*)val - VARSTR_HEADER_SIZE);
        return do_conv_nchar_to_sql_c(stmt, field, val, bytes, col);
      } break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      {
        i64 = *(int64_t*)val;
        return do_conv_timestamp_to_sql_c(stmt, field, i64, col);
        break;
      }
    case TSDB_DATA_TYPE_BOOL:
      {
        i64 = *(int8_t*)val;
      } break;
    default:
      {
        SET_GENERAL(errs, "not convertion from [%d]%s for col[%d]",
            field->type, taos_data_type(field->type), col->ColumnNumber);
        return SQL_ERROR;
      }  break;
  }
  if (is_dbl) return do_conv_double_to_sql_c(stmt, field, dbl, col);
  else        return do_conv_int64_to_sql_c(stmt, field, i64, col);
}

static void tsdb_conn_close_rs(stmt_t *stmt) {
  OILE(stmt, "");
  OILE(stmt->owner, "");
  tsdb_stmt_t *tsdb_stmt = (tsdb_stmt_t*)stmt->ext.ext;
  OILE(tsdb_stmt && tsdb_stmt->stmt==stmt, "");

  tsdb_stmt_close_rs(tsdb_stmt);
}

static int tsdb_conn_init_stmt(stmt_t *stmt) {
  OILE(stmt, "");
  OILE(stmt->ext.ext==NULL, "");
  OILE(stmt->ext.free_stmt==NULL, "");

  OILE(stmt->owner==NULL, "");

  tsdb_stmt_t *tsdb_stmt = (tsdb_stmt_t*)calloc(1, sizeof(*tsdb_stmt));
  if (!tsdb_stmt) return -1;

  stmt_ext_t *ext = &stmt->ext;

  tsdb_stmt->stmt          = stmt;
  ext->ext                 = tsdb_stmt;
  ext->free_stmt           = tsdb_conn_free_stmt;
  ext->clear_param_vals    = tsdb_conn_clear_param_vals;
  ext->exec_direct         = tsdb_conn_exec_direct;
  ext->prepare             = tsdb_conn_prepare;
  ext->set_param_conv      = tsdb_conn_set_param_conv;
  ext->proc_param          = tsdb_conn_proc_param;
  ext->param_row_processed = tsdb_conn_param_row_processed;
  ext->execute             = tsdb_conn_execute;
  ext->get_affected_rows   = tsdb_conn_get_affected_rows;
  ext->get_fields_count    = tsdb_conn_get_fields_count;
  ext->get_field           = tsdb_conn_get_field;
  ext->fetch               = tsdb_conn_fetch;
  ext->get_data            = tsdb_conn_get_data;
  ext->close_rs            = tsdb_conn_close_rs;

  return 0;
}

static pthread_once_t          init_once           = PTHREAD_ONCE_INIT;
static int                     inited              = 0;
// static char                    tsdb_svr_info[128]  = "";
// static char                    tsdb_cli_info[128]  = "";

static void init_routine(void) {
  int r = taos_init();
  if (r) {
    OW("taos init failed: [%d]%s", r, tstrerror(r));
    return;
  }
  OI("taos inited");
  inited = 1;
}

int conn_init_tsdb_conn(conn_t *conn) {
  OILE(conn, "");
  OILE(conn->ext.ext==NULL, "");
  OILE(conn->ext.free_conn==NULL, "");

  pthread_once(&init_once, init_routine);
  if (!inited) return -1;

  tsdb_conn_t *tsdb_conn = (tsdb_conn_t*)calloc(1, sizeof(*tsdb_conn));
  if (!tsdb_conn) return -1;

  tsdb_conn->conn       = conn;
  conn->ext.ext         = tsdb_conn;
  conn->ext.free_conn   = tsdb_conn_free;
  conn->ext.connect     = tsdb_conn_connect;
  conn->ext.disconnect  = tsdb_conn_disconnect;
  conn->ext.init_stmt   = tsdb_conn_init_stmt;

  return 0;
}


