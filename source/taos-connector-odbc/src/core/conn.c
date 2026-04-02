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

#include "internal.h"

#include "charset.h"
#include "conn.h"
#include "desc.h"
#include "ds.h"
#include "env.h"
#include "errs.h"
#include "log.h"
#include "conn_parser.h"
#include "stmt.h"
#include "taos_helpers.h"
#ifdef HAVE_TAOSWS           /* { */
#include "taosws_helpers.h"
#endif                       /* } */
#include "tls.h"
#include "ts_parser.h"
#include "url_parser.h"

#include <odbcinst.h>
#include <string.h>

void conn_cfg_release(conn_cfg_t *conn_cfg)
{
  if (!conn_cfg) return;

  TOD_SAFE_FREE(conn_cfg->driver);
  TOD_SAFE_FREE(conn_cfg->url);
  TOD_SAFE_FREE(conn_cfg->dsn);
  TOD_SAFE_FREE(conn_cfg->uid);
  TOD_SAFE_FREE(conn_cfg->pwd);
  TOD_SAFE_FREE(conn_cfg->ip);
  TOD_SAFE_FREE(conn_cfg->db);
  TOD_SAFE_FREE(conn_cfg->charset_for_col_bind);
  TOD_SAFE_FREE(conn_cfg->charset_for_param_bind);
  TOD_SAFE_FREE(conn_cfg->customproduct_name);

  memset(conn_cfg, 0, sizeof(*conn_cfg));
}

const custprod_item_t custprod_array[] = {
  {"general", CUSTP_GENERAL},
  {"kingscada", CUSTP_KINGSCADA},
  {"kepware", CUSTP_KEPWARE},
  {"ado", CUSTP_ADO},
};

const custprod_item_t *conn_get_custprod_by_index(size_t index)
{
  if (index < sizeof(custprod_array)/sizeof(custprod_array[0]))
    return &custprod_array[index];
  else
    return NULL;
}

const custprod_item_t *conn_get_custprod_by_name(const char *s, size_t n)
{
  for (size_t i=0; i<sizeof(custprod_array)/sizeof(custprod_array[0]); ++i) {
    const char       *name = custprod_array[i].name;
    size_t            len  = strlen(name);
    if (len == n && tod_strncasecmp(s, name, len) == 0) {
      return &custprod_array[i];
    }
  }
  return NULL;
}

int conn_cfg_set_custom_product(conn_cfg_t *conn_cfg, const char *s, size_t n)
{
  const custprod_item_t *custprod_case = conn_get_custprod_by_name(s, n);
  if (!custprod_case) {
    custprod_case = conn_get_custprod_by_index(0);
    if (!custprod_case)
      return -1;
  }

  TOD_SAFE_FREE(conn_cfg->customproduct_name);
  conn_cfg->customproduct_name = strndup(s, n);
  if (!conn_cfg->customproduct_name) return -1;
  conn_cfg->customproduct = custprod_case->custprod_type;
  return 0;
}

static void _conn_init(conn_t *conn, env_t *env)
{
  conn->ds_conn.conn = conn;

  INIT_TOD_LIST_HEAD(&conn->stmts);

  conn->env = env_ref(env);
  int prev = atomic_fetch_add(&env->conns, 1);
  OA_ILE(prev >= 0);

  errs_init(&conn->errs);

  conn->refc = 1;
}

static void _conn_release_information_schema_ins_configs(conn_t *conn)
{
  TOD_SAFE_FREE(conn->s_statusInterval);
  TOD_SAFE_FREE(conn->s_timezone);
  TOD_SAFE_FREE(conn->s_locale);
  TOD_SAFE_FREE(conn->s_charset);
  conn->sqlc_charset[0] = '\0';
  conn->tsdb_charset[0] = '\0';
}

static void _conn_release(conn_t *conn)
{
  OA_ILE(conn->ds_conn.taos == NULL);

  int prev = atomic_fetch_sub(&conn->env->conns, 1);
  OA_ILE(prev >= 1);
  size_t stmts = conn->nr_stmts;
  OA_ILE(stmts == 0);
  env_unref(conn->env);
  conn->env = NULL;

  conn_cfg_release(&conn->cfg);
  _conn_release_information_schema_ins_configs(conn);

  errs_release(&conn->errs);

  return;
}

conn_t* conn_create(env_t *env)
{
  conn_t *conn = (conn_t*)calloc(1, sizeof(*conn));
  if (!conn) {
    env_oom(env);
    return NULL;
  }

  _conn_init(conn, env);

  return conn;
}

conn_t* conn_ref(conn_t *conn)
{
  int prev = atomic_fetch_add(&conn->refc, 1);
  OA_ILE(prev>0);
  return conn;
}

conn_t* conn_unref(conn_t *conn)
{
  int prev = atomic_fetch_sub(&conn->refc, 1);
  if (prev>1) return conn;
  OA_ILE(prev==1);

  _conn_release(conn);
  free(conn);

  return NULL;
}

static int _conn_is_connected(conn_t *conn)
{
  return !!conn->ds_conn.taos;
}

SQLRETURN conn_free(conn_t *conn)
{
  int r = 0;

  int outstandings = atomic_load(&conn->outstandings);
  if (outstandings) {
    conn_append_err_format(conn, "HY000", 0, "General error:%d outstandings still active", outstandings);
    return SQL_ERROR;
  }

  r = _conn_is_connected(conn);
  if (r) {
    conn_append_err(conn, "HY000", 0, "General error:SQLDisconnect not called yet");
    return SQL_ERROR;
  }

  size_t stmts = conn->nr_stmts;
  if (stmts) {
    conn_append_err_format(conn, "HY000", 0, "General error:%zd statements are still outstanding", stmts);
    return SQL_ERROR;
  }

  int descs = atomic_load(&conn->descs);
  if (descs) {
    conn_append_err_format(conn, "HY000", 0, "General error:%d descriptors are still outstanding", descs);
    return SQL_ERROR;
  }

  conn_unref(conn);
  return SQL_SUCCESS;
}

static int _conn_save_from_information_schema_ins_configs(conn_t *conn, int name_len, const char *name, int value_len, const char *value)
{
#ifdef _LOCAL_
#error choose another name for local usage
#endif

#define _LOCAL_(x) do {                                                        \
  if (name_len == sizeof(#x) - 1 && strncmp(#x, name, name_len) == 0) {        \
    TOD_SAFE_FREE(conn->s_##x);                                                \
    conn->s_##x = strndup(value, value_len);                                   \
    if (!conn->s_##x) return -1;                                               \
    return 0;                                                                  \
  }                                                                            \
} while (0)

  _LOCAL_(statusInterval);
  _LOCAL_(timezone);
  _LOCAL_(locale);
  _LOCAL_(charset);

#undef _LOCAL_
  return 0;
}

static SQLRETURN _conn_get_configs_from_information_schema_ins_configs_with_res(conn_t *conn, ds_res_t *ds_res)
{
  int nr_fields                = 0;

  int r = 0;
  nr_fields = (int)ds_res->fields.nr_fields;
  if (nr_fields != 2) {
    conn_append_err_format(conn, "HY000", 0,
        "General error:# of fields from `information_schema.ins_configs` is expected 2 but got ==%d==", nr_fields);
    return SQL_ERROR;
  }

  for (size_t i=0; i<ds_res->fields.nr_fields; ++i) {
    int8_t fld_type = ds_fields_field_type(&ds_res->fields, (int)i);
    if (fld_type != TSDB_DATA_TYPE_VARCHAR) {
      conn_append_err_format(conn, "HY000", 0,
          "General error:field[#%zd] from `information_schema.ins_configs` is expected `TSDB_DATA_TYPE_VARCHAR` but got ==%s==",
          i + 1, taos_data_type(fld_type));
      return SQL_ERROR;
    }
  }

  r = ds_res_fetch_block(ds_res);
  if (r || !ds_res->block.block) {
    int err = ds_res_errno(ds_res);
    const char *s = ds_res_errstr(ds_res);
    conn_append_err_format(conn, "HY000", 0,
        "General error:failed to fetch block from `information_schema.ins_configs`:[%d]%s", err, s);
    return SQL_ERROR;
  }
  ds_err_t ds_err; ds_err.err = 0; ds_err.str[0] = '\0';
  for (int i=0; i<ds_res->block.nr_rows_in_block; ++i) {

    tsdb_data_t name = {0};
    r = ds_block_get_into_tsdb(&ds_res->block, i, 0, &name, &ds_err);
    if (r) {
      conn_append_err_format(conn, "HY000", 0, "General error:%.*s", (int)strlen(ds_err.str), ds_err.str);
      return SQL_ERROR;
    }
    if (name.type != TSDB_DATA_TYPE_VARCHAR || name.is_null) {
      conn_append_err(conn, "HY000", 0, "General error:internal logic error or taosc conformance issue");
      return SQL_ERROR;
    }

    tsdb_data_t value = {0};
    r = ds_block_get_into_tsdb(&ds_res->block, i, 1, &value, &ds_err);
    if (r) {
      conn_append_err_format(conn, "HY000", 0, "General error:%.*s", (int)strlen(ds_err.str), ds_err.str);
      return SQL_ERROR;
    }
    if (value.type != TSDB_DATA_TYPE_VARCHAR || value.is_null) {
      conn_append_err(conn, "HY000", 0, "General error:internal logic error or taosc conformance issue");
      return SQL_ERROR;
    }

    r = _conn_save_from_information_schema_ins_configs(conn, (int)name.str.len, name.str.str, (int)value.str.len, value.str.str);
    if (r) {
      conn_append_err_format(conn, "HY000", 0,
          "General error: save `%.*s:%.*s` failed", (int)name.str.len, name.str.str, (int)value.str.len, value.str.str);
      return SQL_ERROR;
    }
  }
  return SQL_SUCCESS;
}

static SQLRETURN _conn_get_configs_from_information_schema_ins_configs(conn_t *conn)
{
  int r = 0;

#ifdef FAKE_TAOS            /* { */
  if (1) return SQL_SUCCESS;
#endif                      /* } */

  ds_res_t ds_res = {0};
  const char *sql = "select * from information_schema.ins_configs";
  r = ds_conn_query(&conn->ds_conn, sql, &ds_res);
  if (r) {
    int e = ds_res_errno(&ds_res);
    const char *estr = ds_res_errstr(&ds_res);
    conn_append_err_format(conn, "HY000", 0,
        "General error:failed to query `%s`:[%d]%s", sql, e, estr);
    ds_res_close(&ds_res);
    return SQL_ERROR;
  }

  OA_ILE(ds_res.res);
  SQLRETURN sr = _conn_get_configs_from_information_schema_ins_configs_with_res(conn, &ds_res);
  ds_res_close(&ds_res);
  return sr;
}

static int _conn_setup_iconvs(conn_t *conn)
{
  // FIXME: we know conn->s_charset is actually server-side config rather than client-side
  const char *tsdb_charset = conn->s_charset;
  const char *sqlc_charset = tod_get_sqlc_charset();
  if (!sqlc_charset) {
    conn_append_err_format(conn, "HY000", 0, "General error:current locale_or_ACP [%s]:not implemented yet", tod_get_locale_or_ACP());
    return -1;
  }

#ifdef HAVE_TAOSWS           /* { */
  if (conn->cfg.url) {
    tsdb_charset = "UTF-8";  // NOTE: as required by taosws.h?
  }
#endif                       /* } */

#ifdef FAKE_TAOS            /* { */
  sqlc_charset = "GB18030";
  tsdb_charset = "UTF-8";
#endif                      /* } */

  snprintf(conn->sqlc_charset, sizeof(conn->sqlc_charset), "%s", sqlc_charset);
  snprintf(conn->tsdb_charset, sizeof(conn->tsdb_charset), "%s", tsdb_charset);

  return 0;
}

static int _conn_get_timezone_from_tsdb(conn_t *conn, const tsdb_data_t *ts)
{
  int r = 0;

  if (ts->type != TSDB_DATA_TYPE_VARCHAR || ts->is_null) {
    conn_append_err(conn, "HY000", 0, "General error:internal logic error or taosc conformance issue");
    return -1;
  }

  time_t tz_default = tod_get_local_timezone();

  ts_parser_param_t param = {0};
  // param.ctx.debug_flex = 1;
  // param.ctx.debug_bison = 1;
  // NOTE: actually speaking, `tz_default` shall not be used, because `to_iso8601` shall return timezone-d timestamp
  r = ts_parser_parse(ts->str.str, ts->str.len, &param, tz_default);
  if (r) {
    conn_append_err_format(conn, "HY000", 0,
        "General error:format for `%.*s` not implemented yet",
        (int)ts->str.len, ts->str.str);
  } else {
    int64_t tz = param.tz_seconds;
    conn->tz         = tz / 3600 * 100 + (tz % 3600) / 60;
    conn->tz_seconds = tz;
  }

  ts_parser_param_release(&param);

  return r;
}

static int _conn_get_timezone_from_res(conn_t *conn, const char *sql, ds_res_t *ds_res)
{
  int nr_fields                = 0;

  int r = 0;
  nr_fields = (int)ds_res->fields.nr_fields;
  if (nr_fields != 1) {
    conn_append_err_format(conn, "HY000", 0,
        "General error:# of fields from `%s` is expected 1 but got ==%d==", sql, nr_fields);
    return -1;
  }

  int8_t fld_type = ds_fields_field_type(&ds_res->fields, 0);
  if (fld_type != TSDB_DATA_TYPE_VARCHAR) {
    conn_append_err_format(conn, "HY000", 0,
        "General error:field[#%d] from `%s` is expected `TSDB_DATA_TYPE_VARCHAR` but got ==%s==",
        1, sql, taos_data_type(fld_type));
    return -1;
  }

  r = ds_res_fetch_block(ds_res);
  if (r || !ds_res->block.block) {
    int err = ds_res_errno(ds_res);
    const char *s = ds_res_errstr(ds_res);
    conn_append_err_format(conn, "HY000", 0,
        "General error:failed to fetch block from `%s`:[%d]%s", sql, err, s);
    return -1;
  }

  ds_err_t ds_err; ds_err.err = 0; ds_err.str[0] = '\0';

  tsdb_data_t ts = {0};
  r = ds_block_get_into_tsdb(&ds_res->block, 0, 0, &ts, &ds_err);
  if (r) {
    conn_append_err_format(conn, "HY000", 0, "General error:%.*s", (int)strlen(ds_err.str), ds_err.str);
    return -1;
  }

  return _conn_get_timezone_from_tsdb(conn, &ts);
}

static int _conn_get_timezone(conn_t *conn)
{
  int r = 0;

// #ifdef FAKE_TAOS            /* { */
//   conn->tz = 800;
//   conn->tz_seconds = 28800;
//   return 0;
// #endif                      /* } */

  const char *sql = "select to_iso8601(0) as ts";
  ds_res_t ds_res = {0};
  r = ds_conn_query(&conn->ds_conn, sql, &ds_res);
  if (r) {
    int e = ds_res_errno(&ds_res);
    const char *estr = ds_res_errstr(&ds_res);
    conn_append_err_format(conn, "HY000", 0,
        "General error:failed to query `%s`:[%d]%s", sql, e, estr);
    ds_res_close(&ds_res);
    return -1;
  }

  OA_ILE(ds_res.res);

  r = _conn_get_timezone_from_res(conn, sql, &ds_res);
  ds_res_close(&ds_res);
  return r;
}

static SQLRETURN _conn_get_server_info(conn_t *conn)
{
  conn->svr_info = ds_conn_get_server_info(&conn->ds_conn);
  return SQL_SUCCESS;
}

static SQLRETURN _conn_post_connected(conn_t *conn)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  sr = _conn_get_server_info(conn);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  sr = _conn_get_configs_from_information_schema_ins_configs(conn);
  if (sr == SQL_ERROR) return SQL_ERROR;
  r = _conn_setup_iconvs(conn);
  if (r) return SQL_ERROR;
  r = _conn_get_timezone(conn);
  if (r) return SQL_ERROR;
  if (0) {
    r = tls_leakage_potential();
    if (r) {
      env_oom(conn->env);
      return SQL_ERROR;
    }
  }
  if (0) {
    struct {
      const char *from;
      const char *to;
    } convs[] = {
      {"UTF-8",          "GB18030"},
      {"UTF-8",          "GB18030"},
      {"UTF-8",          "GB18030"},
      {"GB18030",        "UTF-8"},
      {"GB18030",        "UTF-8"},
      {"GB18030",        "UTF-8"},
      {"GB18030",        "UTF-8"},
      {"UTF-8",          "UCS-2LE"},
      {"UTF-8",          "UCS-2LE"},
      {"UTF-8",          "UCS-2LE"},
      {"UTF-8",          "UCS-2LE"},
      {"UTF-8",          "UCS-2LE"},
    };
    int failed = 0;
    for (size_t i=0; i<sizeof(convs)/sizeof(convs[0]); ++i) {
      const char *from = convs[i].from;
      const char *to   = convs[i].to;
      charset_conv_t *cnv = tls_get_charset_conv(from, to);
      if (!cnv) {
        env_oom(conn->env);
        failed = 1;
        break;
      }
    }
    if (failed) return SQL_ERROR;
  }

  conn->errs.connected_conn = conn;

  return SQL_SUCCESS;
}

static SQLRETURN _do_conn_connect(conn_t *conn)
{
  SQLRETURN sr;

  ds_conn_setup(&conn->ds_conn);

  const conn_cfg_t *cfg = &conn->cfg;
  const char *db = cfg->db;
  if (conn->cfg.url) {
#ifdef HAVE_TAOSWS           /* { */
    char *url = NULL;
    url_parser_param_t param = {0};
    int r = url_parse_and_encode(&conn->cfg, &url, &param);
    if (r) {
      conn_append_err_format(conn, "HY000", 0, "General error:assembling url failed:[%s]/[%s:%d]/[%s]:cause:[%s]", conn->cfg.url, conn->cfg.ip, conn->cfg.port, conn->cfg.db, param.ctx.err_msg);
      url_parser_param_release(&param);
      return SQL_ERROR;
    }
    url_parser_param_release(&param);
    conn->ds_conn.taos = CALL_ws_connect(url);
    if (!conn->ds_conn.taos) {
      conn_append_err_format(conn, "08001", ws_errno(NULL), "Client unable to establish connection:[%s][%s]", url, ws_errstr(NULL));
      TOD_SAFE_FREE(url);
      return SQL_ERROR;
    }
    TOD_SAFE_FREE(url);
#else                        /* }{ */
    conn_append_err_format(conn, "08001", 0, "Client unable to establish connection:websocket backend not supported yet");
    return SQL_ERROR;
#endif                       /* } */
  } else {
    if (db && (tod_strcasecmp(db, "information_schema")==0 || tod_strcasecmp(db, "performance_schema")==0)) {
      db = NULL;
    }

    conn->ds_conn.taos = CALL_taos_connect(cfg->ip, cfg->uid, cfg->pwd, db, cfg->port);
    if (!conn->ds_conn.taos) {
      char buf[1024];
      fixed_buf_t buffer = {0};
      buffer.buf = buf;
      buffer.cap = sizeof(buf);
      buffer.nr  = 0;
      int n = 0;
      fixed_buf_sprintf(n, &buffer, "taos_odbc://");
      if (cfg->uid) fixed_buf_sprintf(n, &buffer, "%s:*@", cfg->uid);
      if (cfg->ip) {
        if (cfg->port) {
          fixed_buf_sprintf(n, &buffer, "%s:%d", cfg->ip, cfg->port);
        } else {
          fixed_buf_sprintf(n, &buffer, "%s", cfg->ip);
        }
      } else {
        fixed_buf_sprintf(n, &buffer, "localhost");
      }
      if (cfg->db) fixed_buf_sprintf(n, &buffer, "/%s", cfg->db);

      conn_append_err_format(conn, "08001", taos_errno(NULL), "Client unable to establish connection:[%s][%s]", buffer.buf, taos_errstr(NULL));
      return SQL_ERROR;
    }
    if (conn->cfg.db && db == NULL) {
      // FIXME: vulnerability!!!
      int e = CALL_taos_select_db(conn->ds_conn.taos, conn->cfg.db);
      if (e) {
        const char *estr = taos_errstr(NULL);
        conn_append_err_format(conn, "HY000", e, "General error:[taosc]%s, selecting db:%s", estr, cfg->db);
        conn_disconnect(conn);
        return SQL_ERROR;
      }
    }
  }

  sr = _conn_post_connected(conn);
  if (sr != SQL_SUCCESS) {
    conn_disconnect(conn);
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static void _conn_fill_out_connection_str(
    conn_t         *conn,
    SQLCHAR        *OutConnectionString,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLength2Ptr)
{
  char *p = (char*)OutConnectionString;
  if (BufferLength > 0) *p = '\0';
  size_t count = 0;
  int n = 0;

  fixed_buf_t buffer = {0};
  buffer.buf = p;
  buffer.cap = BufferLength;
  buffer.nr = 0;
  if (conn->cfg.driver) {
    fixed_buf_sprintf(n, &buffer, "Driver={%s};", conn->cfg.driver);
  } else if (conn->cfg.dsn) {
    fixed_buf_sprintf(n, &buffer, "DSN=%s;", conn->cfg.dsn);
  }
  if (n>0) count += n;

  if (conn->cfg.backend == BACKEND_TAOSWS) {
    OA_NIY(conn->cfg.url);
    fixed_buf_sprintf(n, &buffer, "URL={%s};", conn->cfg.url);
    if (n>0) count += n;
  }

  if (conn->cfg.ip) {
    if (conn->cfg.port) {
      fixed_buf_sprintf(n, &buffer, "SERVER=%s:%d;", conn->cfg.ip, conn->cfg.port);
    } else {
      fixed_buf_sprintf(n, &buffer, "SERVER=%s;", conn->cfg.ip);
    }
  } else {
    OA_NIY(conn->cfg.port == 0);
    fixed_buf_sprintf(n, &buffer, "SERVER=;");
  }
  if (n>0) count += n;

  if (conn->cfg.db) {
    fixed_buf_sprintf(n, &buffer, "DB=%s;", conn->cfg.db);
  } else {
    fixed_buf_sprintf(n, &buffer, "DB=;");
  }
  if (n>0) count += n;

  if (conn->cfg.charset_for_col_bind) {
    fixed_buf_sprintf(n, &buffer, "CHARSET_ENCODER_FOR_COL_BIND=%s;", conn->cfg.charset_for_col_bind);
  }
  if (n>0) count += n;

  if (conn->cfg.charset_for_param_bind) {
    fixed_buf_sprintf(n, &buffer, "CHARSET_ENCODER_FOR_PARAM_BIND=%s;", conn->cfg.charset_for_param_bind);
  }
  if (n>0) count += n;

  if (conn->cfg.unsigned_promotion) {
    fixed_buf_sprintf(n, &buffer, "UNSIGNED_PROMOTION=1;");
  } else {
    fixed_buf_sprintf(n, &buffer, "UNSIGNED_PROMOTION=0;");
  }
  if (n>0) count += n;

  if (conn->cfg.timestamp_as_is) {
    fixed_buf_sprintf(n, &buffer, "TIMESTAMP_AS_IS=1;");
  } else {
    fixed_buf_sprintf(n, &buffer, "TIMESTAMP_AS_IS=0;");
  }
  if (n>0) count += n;

  if (conn->cfg.conn_mode) {
    fixed_buf_sprintf(n, &buffer, "CONN_MODE=%u;", conn->cfg.conn_mode);
  } else {
    fixed_buf_sprintf(n, &buffer, "CONN_MODE=0;");
  }
  if (n>0) count += n;

  if (conn->cfg.customproduct) {
    fixed_buf_sprintf(n, &buffer, "CUSTOMPRODUCT=%s;", conn->cfg.customproduct_name);
  }
  if (n>0) count += n;

  if (buffer.nr+1 == buffer.cap) {
    char *x = buffer.buf + buffer.nr;
    for (int i=0; i<3 && x>buffer.buf; ++i, --x) x[-1] = '.';
  }

  if (StringLength2Ptr) {
    *StringLength2Ptr = (SQLSMALLINT)count;
  }
}

static int _conn_cfg_init_by_dsn(conn_cfg_t *cfg, char *ebuf, size_t elen)
{
  char buf[1024];     buf[0]     = '\0';

  int r = 0;

  buf[0] = '\0';
  r = SQLGetPrivateProfileString((LPCSTR)cfg->dsn, "URL", (LPCSTR)"", (LPSTR)buf, sizeof(buf), "Odbc.ini");
  if (buf[0]) {
    cfg->url = strdup(buf);
    if (!cfg->url) {
      snprintf(ebuf, elen, "@%d:%s():out of memory", __LINE__, __func__);
      return -1;
    }
    cfg->backend = BACKEND_TAOSWS;
  } else {
    cfg->backend = BACKEND_TAOS;
  }

  if (cfg->backend == BACKEND_TAOSWS) {
#ifndef HAVE_TAOSWS          /* { */
    snprintf(ebuf, elen, "@%d:%s():`URL=%s`, but the driver not built with websocket functionality", __LINE__, __func__, cfg->url);
    return -1;
#endif                       /* } */
  }

  buf[0] = '\0';
  r = SQLGetPrivateProfileString((LPCSTR)cfg->dsn, "SERVER", (LPCSTR)"", (LPSTR)buf, sizeof(buf), "Odbc.ini");
  if (buf[0]) {
    // TODO:
    const char *p = strchr(buf, ':'); // FIXME: trim or not?
    int port = 0;
    if (p) {
      int len = 0;
      int n = sscanf(p+1, "%d%n", &port, &len);
      if (n != 1 || port < 0 || port > UINT16_MAX || len < 0 || (size_t)len != strlen(p+1)) {
        snprintf(ebuf, elen, "@%d:%s():`SERVER=%s` not valid", __LINE__, __func__, buf);
        return -1;
      }
    }

    char *ip = p ? strndup(buf, p-buf) : strdup(buf);
    if (!ip) {
      snprintf(ebuf, elen, "out of memory");
      return -1;
    }
    TOD_SAFE_FREE(cfg->ip);
    cfg->ip = ip;
    cfg->port = port;
  }

  buf[0] = '\0';
  r = SQLGetPrivateProfileString((LPCSTR)cfg->dsn, "DB", (LPCSTR)"", (LPSTR)buf, sizeof(buf), "Odbc.ini");
  if (buf[0]) {
    cfg->db = strdup(buf);
    if (!cfg->db) {
      snprintf(ebuf, elen, "@%d:%s():out of memory", __LINE__, __func__);
      return -1;
    }
  }

  buf[0] = '\0';
  r = SQLGetPrivateProfileString((LPCSTR)cfg->dsn, "UNSIGNED_PROMOTION", (LPCSTR)"0", (LPSTR)buf, sizeof(buf), "Odbc.ini");
  if (r == 1) cfg->unsigned_promotion = !!atoi(buf);

  r = 0;
  buf[0] = '\0';
  r = SQLGetPrivateProfileString((LPCSTR)cfg->dsn, "TIMESTAMP_AS_IS", (LPCSTR)"0", (LPSTR)buf, sizeof(buf), "Odbc.ini");
  if (r == 1) cfg->timestamp_as_is = !!atoi(buf);

  buf[0] = '\0';
  r = SQLGetPrivateProfileString((LPCSTR)cfg->dsn, "CONN_MODE", (LPCSTR)"0", (LPSTR)buf, sizeof(buf), "Odbc.ini");
  if (r == 1) cfg->conn_mode = !!atoi(buf);

  buf[0] = '\0';
  r = SQLGetPrivateProfileString((LPCSTR)cfg->dsn, "CUSTOMPRODUCT", (LPCSTR)"", (LPSTR)buf, sizeof(buf), "Odbc.ini");
  if (r > 0) {
    size_t len = strnlen(buf, sizeof(buf));
    if (len && conn_cfg_set_custom_product(cfg, buf, len)) {
      snprintf(ebuf, elen, "@%d:%s():out of memory or not valid customproduct:[%.*s]", __LINE__, __func__, (int)len, buf);
      return -1;
    }
  }

  buf[0] = '\0';
  r = SQLGetPrivateProfileString((LPCSTR)cfg->dsn, "CHARSET_ENCODER_FOR_COL_BIND", (LPCSTR)"", (LPSTR)buf, sizeof(buf), "Odbc.ini");
  if (buf[0]) {
    cfg->charset_for_col_bind = strdup(buf);
    if (!cfg->charset_for_col_bind) {
      snprintf(ebuf, elen, "@%d:%s():out of memory", __LINE__, __func__);
      return -1;
    }
  }

  buf[0] = '\0';
  r = SQLGetPrivateProfileString((LPCSTR)cfg->dsn, "CHARSET_ENCODER_FOR_PARAM_BIND", (LPCSTR)"", (LPSTR)buf, sizeof(buf), "Odbc.ini");
  if (buf[0]) {
    cfg->charset_for_param_bind = strdup(buf);
    if (!cfg->charset_for_param_bind) {
      snprintf(ebuf, elen, "@%d:%s():out of memory", __LINE__, __func__);
      return -1;
    }
  }

  return 0;
}

SQLRETURN conn_driver_connect(
    conn_t         *conn,
    SQLHWND         WindowHandle,
    SQLCHAR        *InConnectionString,
    SQLSMALLINT     StringLength1,
    SQLCHAR        *OutConnectionString,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLength2Ptr,
    SQLUSMALLINT    DriverCompletion)
{
  (void)WindowHandle;
  SQLRETURN sr = SQL_SUCCESS;

#ifdef USE_WIN_ICONV  /* { */
  OI("win-iconv is used");
#else                 /* }{ */
  OI("taos_odbc's implementation of iconv-api is used");
#endif                /* } */

  conn_cfg_release(&conn->cfg);

  if (StringLength1 == SQL_NTS) StringLength1 = (SQLSMALLINT)strlen((const char*)InConnectionString);

  switch (DriverCompletion) {
    case SQL_DRIVER_NOPROMPT:
      break;
    case SQL_DRIVER_COMPLETE:
      break;
    case SQL_DRIVER_COMPLETE_REQUIRED:
      break;
    case SQL_DRIVER_PROMPT:
      break;
    default:
      conn_append_err_format(conn, "HY000", 0,
          "General error:`%s[%d/0x%x]` not supported yet",
          sql_driver_completion(DriverCompletion), DriverCompletion, DriverCompletion);
      return SQL_ERROR;
  }

  conn_parser_param_t param = {0};
  param.conn_cfg        = &conn->cfg;
  param.init            = _conn_cfg_init_by_dsn;
  param.ctx.debug_flex  = env_get_debug_flex(conn->env);
  param.ctx.debug_bison = env_get_debug_bison(conn->env);

  int r = conn_parser_parse((const char*)InConnectionString, StringLength1, &param);

  do {
    if (r) {
      parser_loc_t *loc = &param.ctx.bad_token;
      conn_append_err_format(conn, "HY000", 0, "General error:parsing:%.*s", StringLength1, (const char*)InConnectionString);
      conn_append_err_format(conn, "HY000", 0, "General error:location:(%d,%d)->(%d,%d)",
          loc->first_line, loc->first_column, loc->last_line, loc->last_column);
      conn_append_err_format(conn, "HY000", 0, "General error:failed:%.*s", (int)strlen(param.ctx.err_msg), param.ctx.err_msg);
      conn_append_err(conn, "HY000", 0, "General error:supported connection string syntax:[<key[=<val>]>]+");
      break;
    }

    if (r) {
      conn_oom(conn);
      break;
    }

    sr = _do_conn_connect(conn);
    if (!sql_succeeded(sr)) break;

    if (DriverCompletion == SQL_DRIVER_COMPLETE || DriverCompletion == SQL_DRIVER_COMPLETE_REQUIRED) {
      _conn_fill_out_connection_str(conn, OutConnectionString, BufferLength, StringLength2Ptr);
    }
    if (DriverCompletion == SQL_DRIVER_NOPROMPT) {
      int n = snprintf((char*)OutConnectionString, BufferLength, "%s", InConnectionString);
      if (StringLength2Ptr) *StringLength2Ptr = n;
    }

    conn_parser_param_release(&param);
    return SQL_SUCCESS;
  } while (0);

  conn_parser_param_release(&param);
  return SQL_ERROR;
}

void conn_disconnect(conn_t *conn)
{
  stmt_t *p, *n;
  tod_list_for_each_entry_safe(p, n, &conn->stmts, stmt_t, node) {
    OW("statement [%p] is still associated with connection [%p], but have to be freed by SQLDisconnect, "
       "and it's application's responsibility not use the statment anymore", p, conn);
    OA_NIY(p->refc == 1);
    stmt_unref(p);
  }
  conn->nr_stmts = 0;

  ds_conn_close(&conn->ds_conn);
  conn_cfg_release(&conn->cfg);
}

static SQLRETURN _conn_commit(conn_t *conn)
{
  int outstandings = atomic_load(&conn->outstandings);
  if (outstandings == 0) {
    conn_append_err_format(conn, "01000", 0, "General warning:no outstanding transactions to commit");
    return SQL_SUCCESS_WITH_INFO;
  }

  conn_append_err_format(conn, "25S02", 0, "Transaction is still active");
  return SQL_ERROR;
}

static int _conn_rollback(conn_t *conn)
{
  int outstandings = atomic_load(&conn->outstandings);
  if (outstandings == 0) {
    conn_append_err_format(conn, "01000", 0, "General warning:no outstanding transactions to rollback");
    return SQL_SUCCESS_WITH_INFO;
  }

  conn_append_err_format(conn, "25S01", 0, "Transaction state unknown");
  return SQL_ERROR;
}

SQLRETURN conn_get_diag_rec(
    conn_t         *conn,
    SQLSMALLINT     RecNumber,
    SQLCHAR        *SQLState,
    SQLINTEGER     *NativeErrorPtr,
    SQLCHAR        *MessageText,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *TextLengthPtr)
{
  return errs_get_diag_rec(&conn->errs, RecNumber, SQLState, NativeErrorPtr, MessageText, BufferLength, TextLengthPtr);
}

SQLRETURN conn_alloc_stmt(conn_t *conn, SQLHANDLE *OutputHandle)
{
  *OutputHandle = SQL_NULL_HANDLE;

  // if (conn->cfg.url) {
  //   conn_append_err(conn, "HY000", 0, "General error:websocket backend not implemented yet");
  //   return SQL_ERROR;
  // }

  stmt_t *stmt = stmt_create(conn);
  if (stmt == NULL) return SQL_ERROR;

  *OutputHandle = (SQLHANDLE)stmt;
  return SQL_SUCCESS;
}

SQLRETURN conn_alloc_desc(conn_t *conn, SQLHANDLE *OutputHandle)
{
  // if (conn->cfg.url) {
  //   conn_append_err(conn, "HY000", 0, "General error: websocket backend not implemented yet");
  //   return SQL_ERROR;
  // }

  desc_t *desc = desc_create(conn);
  if (!desc) return SQL_ERROR;

  *OutputHandle = (SQLHANDLE)desc;
  return SQL_SUCCESS;
}

SQLRETURN conn_connect(
    conn_t        *conn,
    SQLCHAR       *ServerName,
    SQLSMALLINT    NameLength1,
    SQLCHAR       *UserName,
    SQLSMALLINT    NameLength2,
    SQLCHAR       *Authentication,
    SQLSMALLINT    NameLength3)
{
  int r = 0;

#ifdef USE_WIN_ICONV  /* { */
  OI("win-iconv is used");
#else                 /* }{ */
  OI("taos_odbc's implementation of iconv-api is used");
#endif                /* } */

  conn_cfg_release(&conn->cfg);

  if (NameLength1 == SQL_NTS) NameLength1 = ServerName ? (SQLSMALLINT)strlen((const char*)ServerName) : 0;
  if (NameLength2 == SQL_NTS) NameLength2 = UserName ? (SQLSMALLINT)strlen((const char*)UserName) : 0;
  if (NameLength3 == SQL_NTS) NameLength3 = Authentication ? (SQLSMALLINT)strlen((const char*)Authentication) : 0;

  if (ServerName) {
    conn->cfg.dsn = strndup((const char*)ServerName, NameLength1);
    if (!conn->cfg.dsn) {
      conn_oom(conn);
      return SQL_ERROR;
    }
  }

  char buf[1024]; buf[0] = '\0';
  r = _conn_cfg_init_by_dsn(&conn->cfg, buf, sizeof(buf));
  if (r) {
    conn_append_err_format(conn, "HY000", taos_errno(NULL), "General error:%s", buf);
    return SQL_ERROR;
  }

  if (UserName) {
    conn->cfg.uid = strndup((const char*)UserName, NameLength2);
    if (!conn->cfg.uid) {
      conn_oom(conn);
      return SQL_ERROR;
    }
  }
  if (Authentication) {
    conn->cfg.pwd = strndup((const char*)Authentication, NameLength3);
    if (!conn->cfg.pwd) {
      conn_oom(conn);
      return SQL_ERROR;
    }
  }

  return _do_conn_connect(conn);
}

static SQLRETURN _conn_set_string(
    conn_t         *conn,
    const char     *value,
    SQLUSMALLINT    InfoType,
    SQLPOINTER      InfoValuePtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  int n = snprintf((char *)InfoValuePtr, InfoValuePtr ? BufferLength : 0, "%s", value);
  if (StringLengthPtr) *StringLengthPtr = n;

  if (n >= BufferLength) {
    conn_append_err_format(conn, "01004", 0, "String data, right truncated:`%s[%d/0x%x]`", sql_info_type(InfoType), InfoType, InfoType);
    return SQL_SUCCESS_WITH_INFO;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _conn_get_info_dbms_ver(
    conn_t         *conn,
    SQLUSMALLINT    InfoType,
    SQLPOINTER      InfoValuePtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  const char *server = ds_conn_get_server_info(&conn->ds_conn);
  if (!server) {
    conn_append_err_format(conn, "HY000", 0, "General error:`%s[%d/0x%x]` internal logic error", sql_info_type(InfoType), InfoType, InfoType);
    return SQL_ERROR;
  }

  int v1=0, v2=0, v3=0, v4=0;
  sscanf(server, "ver:%d.%d.%d.%d", &v1, &v2, &v3, &v4);

  const char *client = ds_conn_get_client_info(&conn->ds_conn);
  if (!client) {
    conn_append_err_format(conn, "HY000", 0, "General error:`%s[%d/0x%x]` internal logic error", sql_info_type(InfoType), InfoType, InfoType);
    return SQL_ERROR;
  }

  int n = snprintf((char*)InfoValuePtr, BufferLength,
    "%02d.%02d.%04d\n"
    "taos_odbc-0.1@taosc:%s@taosd:%s",
    v1 % 100, v2 %100, v3 % 100 * 100 + v4 % 100,
    client, server);
  if (StringLengthPtr) *StringLengthPtr = n;

  if (n >= BufferLength) {
    conn_append_err_format(conn, "01004", 0, "String data, right truncated:`%s[%d/0x%x]`", sql_info_type(InfoType), InfoType, InfoType);
    return SQL_SUCCESS_WITH_INFO;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _conn_get_info_driver_name(
    conn_t         *conn,
    SQLUSMALLINT    InfoType,
    SQLPOINTER      InfoValuePtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  const char *driver_name = tod_get_image_name();

  if (1) {
    int n = snprintf((char*)InfoValuePtr, BufferLength, "%s", driver_name);
    if (StringLengthPtr) *StringLengthPtr = n;

    if (n >= BufferLength) {
      conn_append_err_format(conn, "01004", 0, "String data, right truncated:`%s[%d/0x%x]`", sql_info_type(InfoType), InfoType, InfoType);
      return SQL_SUCCESS_WITH_INFO;
    }

    return SQL_SUCCESS;
  }
}

static SQLRETURN _conn_get_current_db(conn_t *conn, char *db, size_t len)
{
  // ref: https://dev.mysql.com/doc/connector-odbc/en/connector-odbc-usagenotes-functionality-catalog-schema.html
  // TODO:
  int r = 0;
  ds_err_t ds_err; ds_err.err = 0; ds_err.str[0] = '\0';
  r = ds_conn_get_current_db(&conn->ds_conn, db, len, &ds_err);
  if (r) {
    conn_append_err_format(conn, "HY000", ds_err.err, "General error:%s", ds_err.str);
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _conn_get_info_database_name(
    conn_t         *conn,
    SQLUSMALLINT    InfoType,
    SQLPOINTER      InfoValuePtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  // FIXME: `database` or current database selected?
  int r = 0;
  char dbName[128] = {0};     // #define TSDB_DB_NAME_LEN  65


  r = _conn_get_current_db(conn, (char*)dbName, sizeof(dbName));
  if (r) return SQL_ERROR;

  int n = (int)strlen((const char*)dbName);
  if (StringLengthPtr) *StringLengthPtr = n;


  if (InfoValuePtr) {
    if (n >= BufferLength) {
      conn_append_err_format(conn, "01004", 0, "String data, right truncated:`%s[%d/0x%x]`", sql_info_type(InfoType), InfoType, InfoType);
      // FIXME: or SQL_ERROR?
      return SQL_SUCCESS_WITH_INFO;
    }

    snprintf(InfoValuePtr, BufferLength, "%s", dbName);
  }

  return SQL_SUCCESS;
}

static const char* ds_conn_get_uid(ds_conn_t *ds_conn)
{
  OA_NIY(ds_conn->conn);

  conn_t *conn = ds_conn->conn;
#ifdef HAVE_TAOSWS           /* { */
  if (conn->cfg.url) {
    conn_append_err(conn, "HY000", 0, "General error:websocket backend not implemented yet");
    return NULL;
  }
#endif                       /* } */

  // NOTE: [taosc] has no user-name bounded to database, thus we choose login name to return
  return conn->cfg.uid;
}

static SQLRETURN _conn_get_info_user_name(
    conn_t         *conn,
    SQLUSMALLINT    InfoType,
    SQLPOINTER      InfoValuePtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  const char *uid = ds_conn_get_uid(&conn->ds_conn);
  if (!uid) return SQL_ERROR;
  int n = snprintf((char*)InfoValuePtr, BufferLength, "%s", uid);
  if (StringLengthPtr) *StringLengthPtr = n;

  if (n >= BufferLength) {
    conn_append_err_format(conn, "01004", 0, "String data, right truncated:`%s[%d/0x%x]`", sql_info_type(InfoType), InfoType, InfoType);
    // FIXME: or SQL_ERROR?
    return SQL_SUCCESS_WITH_INFO;
  }

  return SQL_SUCCESS;
}

SQLRETURN conn_get_info(
    conn_t         *conn,
    SQLUSMALLINT    InfoType,
    SQLPOINTER      InfoValuePtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  // https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetinfo-function?view=sql-server-ver16
  switch (InfoType) {
    // Driver Information
    case SQL_ACTIVE_ENVIRONMENTS:
      break;
#if (ODBCVER >= 0x0380)      /* { */
    case SQL_ASYNC_DBC_FUNCTIONS:
      *(SQLUINTEGER*)InfoValuePtr = SQL_ASYNC_DBC_NOT_CAPABLE;
      return SQL_SUCCESS;
#endif                       /* } */
    case SQL_ASYNC_MODE:
      break;
#if (ODBCVER >= 0x0380)      /* { */
    case SQL_ASYNC_NOTIFICATION:
      *(SQLUINTEGER*)InfoValuePtr = SQL_ASYNC_NOTIFICATION_NOT_CAPABLE;
      return SQL_SUCCESS;
#endif                       /* } */
    case SQL_BATCH_ROW_COUNT:
      break;
    case SQL_BATCH_SUPPORT:
      *(SQLUINTEGER*)InfoValuePtr = (SQL_BS_SELECT_EXPLICIT | SQL_BS_ROW_COUNT_EXPLICIT);
      return SQL_SUCCESS;
    case SQL_DATA_SOURCE_NAME:
      break;
    case SQL_DRIVER_AWARE_POOLING_SUPPORTED:
      break;
    case SQL_DRIVER_HDBC:
      // https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetinfo-function?view=sql-server-ver16
      // These information types are implemented by the Driver Manager alone.
      break;
    case SQL_DRIVER_HDESC:
      break;
    case SQL_DRIVER_HENV:
      // https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetinfo-function?view=sql-server-ver16
      // These information types are implemented by the Driver Manager alone.
      break;
    case SQL_DRIVER_HLIB:
      break;
    case SQL_DRIVER_HSTMT:
      break;
    case SQL_DRIVER_NAME:
      return _conn_get_info_driver_name(conn, InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
    case SQL_DRIVER_ODBC_VER: {
      // https://learn.microsoft.com/en-us/sql/odbc/reference/install/driver-specification-subkeys?view=sql-server-ver16
      // `DriverODBCVer`: This must be the same as the value returned for the SQL_DRIVER_ODBC_VER option in SQLGetInfo.
#if (ODBCVER == 0x0351)              /* { */
      return _conn_set_string(conn, "03.51", InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
#else                                /* }{ */
#error `ODBCVER` must be equal to 0x0351
#endif                               /* } */
    } break;
    case SQL_DRIVER_VER:
      return _conn_set_string(conn, "01.01.0001", InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
    case SQL_DYNAMIC_CURSOR_ATTRIBUTES1:
      // https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetinfo-function?view=sql-server-ver16
      *(SQLUINTEGER*)InfoValuePtr = SQL_CA1_NEXT | SQL_CA1_ABSOLUTE | SQL_CA1_RELATIVE;
      return SQL_SUCCESS;
    case SQL_DYNAMIC_CURSOR_ATTRIBUTES2:
      // https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetinfo-function?view=sql-server-ver16
      *(SQLUINTEGER*)InfoValuePtr = SQL_CA2_READ_ONLY_CONCURRENCY;
      return SQL_SUCCESS;
    case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1:
      *(SQLUINTEGER*)InfoValuePtr = SQL_CA1_NEXT;
      return SQL_SUCCESS;
    case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2:
      break;
    case SQL_FILE_USAGE:
      break;
    case SQL_GETDATA_EXTENSIONS:
      *(SQLUINTEGER*)InfoValuePtr = SQL_GD_ANY_COLUMN | SQL_GD_ANY_ORDER /* | SQL_GD_BLOCK */ | SQL_GD_BOUND /* | SQL_GD_OUTPUT_PARAMS */;
      return SQL_SUCCESS;
    case SQL_INFO_SCHEMA_VIEWS:
      break;
    case SQL_KEYSET_CURSOR_ATTRIBUTES1:
      *(SQLUINTEGER*)InfoValuePtr = SQL_CA1_NEXT | SQL_CA1_ABSOLUTE | SQL_CA1_RELATIVE;
      return SQL_SUCCESS;
    case SQL_KEYSET_CURSOR_ATTRIBUTES2:
      *(SQLUINTEGER*)InfoValuePtr = 0;
      return SQL_SUCCESS;
    case SQL_MAX_ASYNC_CONCURRENT_STATEMENTS:
      break;
    case SQL_MAX_CONCURRENT_ACTIVITIES:
      *(SQLUSMALLINT*)InfoValuePtr = 0;
      return SQL_SUCCESS;
    case SQL_MAX_DRIVER_CONNECTIONS:
      break;
    case SQL_ODBC_INTERFACE_CONFORMANCE:
      *(SQLUSMALLINT*)InfoValuePtr = 0; // FIXME:
      return SQL_SUCCESS;
#if 0         /* { */
    case SQL_ODBC_STANDARD_CLI_CONFORMANCE:
      break;
#endif        /* } */
    case SQL_ODBC_VER:
      break;
    case SQL_PARAM_ARRAY_ROW_COUNTS:
      *(SQLUINTEGER*)InfoValuePtr = SQL_PARC_NO_BATCH; // FIXME: more test cases
      return SQL_SUCCESS;
    case SQL_PARAM_ARRAY_SELECTS:
      break;
    case SQL_ROW_UPDATES:
      break;
    case SQL_SEARCH_PATTERN_ESCAPE:
      return _conn_set_string(conn, "\\", InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
    case SQL_SERVER_NAME:
      return _conn_set_string(conn, "", InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
    case SQL_STATIC_CURSOR_ATTRIBUTES1:
      *(SQLUINTEGER*)InfoValuePtr = SQL_CA1_NEXT | SQL_CA1_ABSOLUTE | SQL_CA1_RELATIVE;
      return SQL_SUCCESS;
    case SQL_STATIC_CURSOR_ATTRIBUTES2:
      *(SQLUINTEGER*)InfoValuePtr = 0;
      return SQL_SUCCESS;

    // DBMS Product Information
    case SQL_DATABASE_NAME:
      return _conn_get_info_database_name(conn, InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
    case SQL_DBMS_NAME:
      return _conn_set_string(conn, "tdengine", InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
    case SQL_DBMS_VER:
      return _conn_get_info_dbms_ver(conn, InfoType, InfoValuePtr, BufferLength, StringLengthPtr);

    // Data Source Information
    case SQL_ACCESSIBLE_PROCEDURES:
      break;
    case SQL_ACCESSIBLE_TABLES:
      break;
    case SQL_BOOKMARK_PERSISTENCE:
      *(SQLUINTEGER*)InfoValuePtr = 0;
      return SQL_SUCCESS;
    case SQL_CATALOG_TERM:
      return _conn_set_string(conn, "database", InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
    case SQL_COLLATION_SEQ:
      break;
    case SQL_CONCAT_NULL_BEHAVIOR:
      break;
    case SQL_CURSOR_COMMIT_BEHAVIOR:
      *(SQLUSMALLINT*)InfoValuePtr = SQL_CB_DELETE; // NOTE: refer to msdn listed above
      return SQL_SUCCESS;
    case SQL_CURSOR_ROLLBACK_BEHAVIOR: 
      *(SQLUSMALLINT*)InfoValuePtr = SQL_CB_DELETE; // NOTE: refer to msdn listed above
      return SQL_SUCCESS;
    case SQL_CURSOR_SENSITIVITY:
      break;
    case SQL_DATA_SOURCE_READ_ONLY:
      return _conn_set_string(conn, "N", InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
    case SQL_DEFAULT_TXN_ISOLATION:
      *(SQLUINTEGER*)InfoValuePtr = 0;
      return SQL_SUCCESS;;
    case SQL_DESCRIBE_PARAMETER:
      break;
    case SQL_MULT_RESULT_SETS:
      return _conn_set_string(conn, "N", InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
    case SQL_MULTIPLE_ACTIVE_TXN:
      break;
    case SQL_NEED_LONG_DATA_LEN:
      return _conn_set_string(conn, "N", InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
    case SQL_NULL_COLLATION:
      break;
    case SQL_PROCEDURE_TERM:
      break;
    case SQL_SCHEMA_TERM: // SQL_OWNER_TERM
      return _conn_set_string(conn, "schema", InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
    case SQL_SCROLL_OPTIONS:
      *(SQLUINTEGER*)InfoValuePtr = 0;
      return SQL_SUCCESS;;
    case SQL_TABLE_TERM:
      break;
    case SQL_TXN_CAPABLE:
      // https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetinfo-function?view=sql-server-ver16
      *(SQLUSMALLINT*)InfoValuePtr = SQL_TC_NONE; // FIXME:
      return SQL_SUCCESS;
    case SQL_TXN_ISOLATION_OPTION:
      *(SQLUINTEGER*)InfoValuePtr = 0; // TODO:
      return SQL_SUCCESS;
    case SQL_USER_NAME:
      return _conn_get_info_user_name(conn, InfoType, InfoValuePtr, BufferLength, StringLengthPtr);

    // Supported SQL
    case SQL_AGGREGATE_FUNCTIONS:
      // NOTE: need to fine-tune [taosc]
      break;
    case SQL_ALTER_DOMAIN:
      break;
#if 0                       /* { */
    case SQL_ALTER_SCHEMA:
      break;
#endif                      /* } */
    case SQL_ALTER_TABLE:
      // NOTE: https://learn.microsoft.com/en-us/sql/odbc/reference/appendixes/sqlgetinfo-support?view=sql-server-ver16
      *(SQLINTEGER*)InfoValuePtr = SQL_AT_DROP_COLUMN | SQL_AT_ADD_COLUMN; // FIXME:
      return SQL_SUCCESS;
#if 0                       /* { */
    case SQL_ANSI_SQL_DATETIME_LITERALS:
      break;
#endif                      /* } */
    case SQL_CATALOG_LOCATION: // SQL_QUALIFIER_LOCATION
      *(SQLUSMALLINT*)InfoValuePtr = SQL_CL_START;
      return SQL_SUCCESS;
    case SQL_CATALOG_NAME:
      return _conn_set_string(conn, "Y", InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
    case SQL_CATALOG_NAME_SEPARATOR:
      return _conn_set_string(conn, ".", InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
    case SQL_CATALOG_USAGE:
      *(SQLUINTEGER*)InfoValuePtr = SQL_CU_DML_STATEMENTS | SQL_CU_PROCEDURE_INVOCATION | SQL_CU_TABLE_DEFINITION | SQL_CU_INDEX_DEFINITION | SQL_CU_PRIVILEGE_DEFINITION;
      return SQL_SUCCESS;
    case SQL_COLUMN_ALIAS:
      return _conn_set_string(conn, "Y", InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
    case SQL_CORRELATION_NAME:
      break;
    case SQL_CREATE_ASSERTION:
      break;
    case SQL_CREATE_CHARACTER_SET:
      break;
    case SQL_CREATE_COLLATION:
      break;
    case SQL_CREATE_DOMAIN:
      break;
    case SQL_CREATE_SCHEMA:
      break;
    case SQL_CREATE_TABLE:
      break;
    case SQL_CREATE_TRANSLATION:
      break;
    case SQL_DDL_INDEX:
      break;
    case SQL_DROP_ASSERTION:
      break;
    case SQL_DROP_CHARACTER_SET:
      break;
    case SQL_DROP_COLLATION:
      break;
    case SQL_DROP_DOMAIN:
      break;
    case SQL_DROP_SCHEMA:
      break;
    case SQL_DROP_TABLE:
      break;
    case SQL_DROP_TRANSLATION:
      break;
    case SQL_DROP_VIEW:
      break;
    case SQL_EXPRESSIONS_IN_ORDERBY:
      break;
    case SQL_GROUP_BY:
      *(SQLUSMALLINT*)InfoValuePtr = SQL_GB_GROUP_BY_EQUALS_SELECT;
      return SQL_SUCCESS;
    case SQL_IDENTIFIER_CASE:
      *(SQLUSMALLINT*)InfoValuePtr = SQL_IC_UPPER; // FIXME:
      return SQL_SUCCESS;
    case SQL_IDENTIFIER_QUOTE_CHAR:
      return _conn_set_string(conn, "`", InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
    case SQL_INDEX_KEYWORDS:
      break;
    case SQL_INSERT_STATEMENT:
      break;
    case SQL_INTEGRITY:
      break;
    case SQL_KEYWORDS:
      break;
    case SQL_LIKE_ESCAPE_CLAUSE:
      break;
    case SQL_NON_NULLABLE_COLUMNS:
      break;
    case SQL_OJ_CAPABILITIES:
      // *(SQLUINTEGER*)InfoValuePtr = SQL_OJ_LEFT | SQL_OJ_RIGHT | SQL_OJ_FULL | SQL_OJ_NESTED | SQL_OJ_NOT_ORDERED | SQL_OJ_INNER | SQL_OJ_ALL_COMPARISON_OPS;
      *(SQLUINTEGER*)InfoValuePtr = 0;
      return SQL_SUCCESS;
    case SQL_ORDER_BY_COLUMNS_IN_SELECT:
      return _conn_set_string(conn, "N", InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
    case SQL_OUTER_JOINS:
      break;
    case SQL_PROCEDURES:
      break;
    case SQL_QUOTED_IDENTIFIER_CASE:
      *(SQLUSMALLINT*)InfoValuePtr = SQL_IC_SENSITIVE;
      return SQL_SUCCESS;
    case SQL_SCHEMA_USAGE: // SQL_OWNER_USAGE
      *(SQLUINTEGER*)InfoValuePtr = 0;
      return SQL_SUCCESS;
    case SQL_SPECIAL_CHARACTERS:
      // NOTE: check with [taosc]
      return _conn_set_string(conn, "", InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
    case SQL_SQL_CONFORMANCE:
      *(SQLUINTEGER*)InfoValuePtr = SQL_SC_SQL92_ENTRY;
      return SQL_SUCCESS;
    case SQL_SUBQUERIES:
      break;
    case SQL_UNION:
      break;

    // SQL Limits
    case SQL_MAX_BINARY_LITERAL_LEN:
      break;
    case SQL_MAX_CATALOG_NAME_LEN:
      *(SQLUSMALLINT*)InfoValuePtr = MAX_CATALOG_NAME_LEN;
      return SQL_SUCCESS;
    case SQL_MAX_CHAR_LITERAL_LEN:
      break;
    case SQL_MAX_COLUMN_NAME_LEN:
      *(SQLUSMALLINT*)InfoValuePtr = MAX_COLUMN_NAME_LEN;
      return SQL_SUCCESS;
    case SQL_MAX_COLUMNS_IN_GROUP_BY:
      *(SQLUSMALLINT*)InfoValuePtr = 0;
      return SQL_SUCCESS;
    case SQL_MAX_COLUMNS_IN_INDEX:
      break;
    case SQL_MAX_COLUMNS_IN_ORDER_BY:
      *(SQLUSMALLINT*)InfoValuePtr = 0;
      return SQL_SUCCESS;
    case SQL_MAX_COLUMNS_IN_SELECT:
      *(SQLUSMALLINT*)InfoValuePtr = 4096;
      return SQL_SUCCESS;
    case SQL_MAX_COLUMNS_IN_TABLE:
      break;
    case SQL_MAX_CURSOR_NAME_LEN:
      break;
    case SQL_MAX_IDENTIFIER_LEN:
      *(SQLUSMALLINT*)InfoValuePtr = 192;
      return SQL_SUCCESS;
    case SQL_MAX_INDEX_SIZE:
      break;
    case SQL_MAX_PROCEDURE_NAME_LEN:
      break;
    case SQL_MAX_ROW_SIZE:
      break;
    case SQL_MAX_ROW_SIZE_INCLUDES_LONG:
      break;
    case SQL_MAX_SCHEMA_NAME_LEN:
      *(SQLUSMALLINT*)InfoValuePtr = MAX_SCHEMA_NAME_LEN;
      return SQL_SUCCESS;
    case SQL_MAX_STATEMENT_LEN:
      break;
    case SQL_MAX_TABLE_NAME_LEN:
      *(SQLUSMALLINT*)InfoValuePtr = MAX_TABLE_NAME_LEN;
      return SQL_SUCCESS;
    case SQL_MAX_TABLES_IN_SELECT:
      break;
    case SQL_MAX_USER_NAME_LEN:
      break;

    // Scalar Function Information
    case SQL_CONVERT_FUNCTIONS:
      // NOTE: need to fine-tune [taosc]
      break;
    case SQL_NUMERIC_FUNCTIONS:
      // NOTE: need to fine-tune [taosc]
      break;
    case SQL_STRING_FUNCTIONS:
      // NOTE: need to fine-tune [taosc]
      break;
    case SQL_SYSTEM_FUNCTIONS:
      // NOTE: need to fine-tune [taosc]
      break;
    case SQL_TIMEDATE_ADD_INTERVALS:
      // NOTE: need to fine-tune [taosc]
      break;
    case SQL_TIMEDATE_DIFF_INTERVALS:
      // NOTE: need to fine-tune [taosc]
      break;
    case SQL_TIMEDATE_FUNCTIONS:
      // NOTE: need to fine-tune [taosc]
      break;

    // Conversion Information
    case SQL_CONVERT_BIGINT:
      break;
    case SQL_CONVERT_BINARY:
      break;
    case SQL_CONVERT_BIT:
      break;
    case SQL_CONVERT_CHAR:
      break;
    case SQL_CONVERT_DATE:
      break;
    case SQL_CONVERT_DECIMAL:
      break;
    case SQL_CONVERT_DOUBLE:
      break;
    case SQL_CONVERT_FLOAT:
      break;
    case SQL_CONVERT_INTEGER:
      break;
    case SQL_CONVERT_INTERVAL_DAY_TIME:
      break;
    case SQL_CONVERT_INTERVAL_YEAR_MONTH:
      break;
    case SQL_CONVERT_LONGVARBINARY:
      break;
    case SQL_CONVERT_LONGVARCHAR:
      break;
    case SQL_CONVERT_NUMERIC:
      break;
    case SQL_CONVERT_REAL:
      break;
    case SQL_CONVERT_SMALLINT:
      break;
    case SQL_CONVERT_TIME:
      break;
    case SQL_CONVERT_TIMESTAMP:
      break;
    case SQL_CONVERT_TINYINT:
      break;
    case SQL_CONVERT_VARBINARY:
      break;
    case SQL_CONVERT_VARCHAR:
      break;

    // Information Types Added for ODBC 3.x
    case SQL_DM_VER:
      // TODO:
      break;
    case SQL_XOPEN_CLI_YEAR:
      break;



    // Information Types Deprecated in ODBC 3.x
    // NOTE: https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetinfo-function?view=sql-server-ver16#information-types-deprecated-in-odbc-3x
    // NOTE: https://learn.microsoft.com/en-us/sql/odbc/reference/appendixes/sqlgetinfo-support?view=sql-server-ver16
    case SQL_FETCH_DIRECTION:
      *(SQLINTEGER*)InfoValuePtr = SQL_FD_FETCH_NEXT; // FIXME:
      return SQL_SUCCESS;
    case SQL_LOCK_TYPES:
      *(SQLINTEGER*)InfoValuePtr = SQL_LCK_NO_CHANGE; // FIXME:
      return SQL_SUCCESS;
    case SQL_ODBC_API_CONFORMANCE:
      *(SQLSMALLINT*)InfoValuePtr = SQL_OAC_LEVEL2; // FIXME:
      return SQL_SUCCESS;
    case SQL_ODBC_SQL_CONFORMANCE:
      *(SQLSMALLINT*)InfoValuePtr = SQL_OSC_MINIMUM; // FIXME:
      return SQL_SUCCESS;
    case SQL_POS_OPERATIONS:
      *(SQLINTEGER*)InfoValuePtr = 0; // FIXME:
      return SQL_SUCCESS;
    case SQL_POSITIONED_STATEMENTS:
      *(SQLINTEGER*)InfoValuePtr = 0; // FIXME:
      return SQL_SUCCESS;
    case SQL_SCROLL_CONCURRENCY:
      *(SQLINTEGER*)InfoValuePtr = SQL_SCCO_READ_ONLY; // FIXME:
      return SQL_SUCCESS;
    case SQL_STATIC_SENSITIVITY:
      *(SQLINTEGER*)InfoValuePtr = 0; // FIXME:
      return SQL_SUCCESS;


    // Information Type Descriptions that's not listed above
    case SQL_CONVERT_GUID:
      break;
    case SQL_CREATE_VIEW:
      break;
    case SQL_DATETIME_LITERALS:
      // NOTE: need to fine-tune [taosc]
      break;
    case SQL_SQL92_DATETIME_FUNCTIONS:
      // NOTE: need to fine-tune [taosc]
      break;
    case SQL_SQL92_FOREIGN_KEY_UPDATE_RULE:
      break;
    case SQL_SQL92_GRANT:
      break;
    case SQL_SQL92_NUMERIC_VALUE_FUNCTIONS:
      // NOTE: need to fine-tune [taosc]
      break;
    case SQL_SQL92_PREDICATES:
      *(SQLUINTEGER*)InfoValuePtr = 0;
      return SQL_SUCCESS;
    case SQL_SQL92_RELATIONAL_JOIN_OPERATORS:
      // NOTE: need to fine-tune [taosc]
      break;
    case SQL_SQL92_REVOKE:
      break;
    case SQL_SQL92_ROW_VALUE_CONSTRUCTOR:
      break;
    case SQL_SQL92_STRING_FUNCTIONS:
      // NOTE: need to fine-tune [taosc]
      break;
    case SQL_SQL92_VALUE_EXPRESSIONS:
      // NOTE: need to fine-tune [taosc]
      break;
    case SQL_STANDARD_CLI_CONFORMANCE:
      break;

#if 0         /* { */
    case SQL_DTC_TRANSITION_COST:
      *(SQLUINTEGER*)InfoValuePtr = 0;
      return SQL_SUCCESS;
#endif        /* } */

    default:
      break;
  }

  conn_append_err_format(conn, "HY000", 0, "General error:`%s[%d/0x%x]` not implemented yet", sql_info_type(InfoType), InfoType, InfoType);
  return SQL_ERROR;
}

SQLRETURN conn_end_tran(
    conn_t       *conn,
    SQLSMALLINT   CompletionType)
{
  switch (CompletionType) {
    case SQL_COMMIT:
      return _conn_commit(conn);
    case SQL_ROLLBACK:
      return _conn_rollback(conn);
    default:
      conn_append_err_format(conn, "HY000", 0, "General error:[DM]logic error");
      return SQL_ERROR;
  }
}

SQLRETURN conn_set_attr(
    conn_t       *conn,
    SQLINTEGER    Attribute,
    SQLPOINTER    ValuePtr,
    SQLINTEGER    StringLength)
{
  int r = 0;

  /*
  if (conn->cfg.url) {
    conn_append_err(conn, "HY000", 0, "General error:websocket backend not implemented yet");
    return SQL_ERROR;
  }
  */

  (void)StringLength;

  switch (Attribute) {
    case SQL_ATTR_ACCESS_MODE:
      if ((SQLUINTEGER)(uintptr_t)ValuePtr == SQL_MODE_READ_WRITE) return SQL_SUCCESS;
      conn_append_err_format(conn, "01S02", 0,
          "Option value changed:`%u` for `SQL_ATTR_ACCESS_MODE` is substituted by `SQL_MODE_READ_WRITE`",
          (SQLUINTEGER)(uintptr_t)ValuePtr);
      return SQL_SUCCESS_WITH_INFO;
#if (ODBCVER >= 0x0380)      /* { */
    case SQL_ATTR_ASYNC_DBC_EVENT:
      break;
    case SQL_ATTR_ASYNC_DBC_FUNCTIONS_ENABLE:
      break;
    case SQL_ATTR_ASYNC_DBC_PCALLBACK:
      break;
    case SQL_ATTR_ASYNC_DBC_PCONTEXT:
      break;
#endif                       /* } */
    case SQL_ATTR_ASYNC_ENABLE:
      if ((SQLULEN)(uintptr_t)ValuePtr == SQL_ASYNC_ENABLE_OFF) return SQL_SUCCESS;
      conn_append_err_format(conn, "01S02", 0,
          "Option value changed:`%u` for `SQL_ATTR_ASYNC_ENABLE` is substituted by `SQL_ASYNC_ENABLE_OFF`",
          (SQLUINTEGER)(uintptr_t)ValuePtr);
      return SQL_SUCCESS_WITH_INFO;
    case SQL_ATTR_AUTO_IPD:
      break;
    case SQL_ATTR_AUTOCOMMIT:
      if ((SQLUINTEGER)(uintptr_t)ValuePtr == SQL_AUTOCOMMIT_ON) return SQL_SUCCESS;
      conn_append_err_format(conn, "01S02", 0,
          "Option value changed:`%u` for `SQL_ATTR_AUTOCOMMIT` is substituted by `SQL_AUTOCOMMIT_ON`",
          (SQLUINTEGER)(uintptr_t)ValuePtr);
      return SQL_SUCCESS_WITH_INFO;
    case SQL_ATTR_CONNECTION_DEAD:
      break;
    case SQL_ATTR_CONNECTION_TIMEOUT:
      if (0 == (SQLUINTEGER)(uintptr_t)ValuePtr) return SQL_SUCCESS;
      conn_append_err_format(conn, "01S02", 0,
          "Option value changed:`%u` for `SQL_ATTR_CONNECTION_TIMEOUT` is substituted by `0`",
          (SQLUINTEGER)(uintptr_t)ValuePtr);
      return SQL_SUCCESS_WITH_INFO;
    case SQL_ATTR_CURRENT_CATALOG:
      r = CALL_taos_select_db(conn->ds_conn.taos, (const char*)ValuePtr);
      if (r == 0) return SQL_SUCCESS;
      conn_append_err_format(conn, "HY000", r, "General error:[taosc]%s, failed to select db:%s", taos_errstr(NULL), (const char*)ValuePtr);
      return SQL_ERROR;
#if (ODBCVER >= 0x0380)      /* { */
    case SQL_ATTR_DBC_INFO_TOKEN:
      break;
#endif                       /* } */
    case SQL_ATTR_ENLIST_IN_DTC:
      break;
    case SQL_ATTR_LOGIN_TIMEOUT:
      if ((SQLUINTEGER)(uintptr_t)ValuePtr == 0) return SQL_SUCCESS;
      switch (conn->cfg.customproduct) {
        case CUSTP_KINGSCADA:
          // FIXME: add for king scada joint debugging
          conn->login_timeout = (SQLUINTEGER)(uintptr_t)ValuePtr;
          return SQL_SUCCESS;
        default:
          break;
      }

      conn_append_err_format(conn, "01S02", 0,
          "Option value changed:`%u` for `SQL_ATTR_LOGIN_TIMEOUT` is substituted by `0`",
          (SQLUINTEGER)(uintptr_t)ValuePtr);
      return SQL_SUCCESS_WITH_INFO;
    case SQL_ATTR_METADATA_ID:
      // FIXME:
      if ((SQLUINTEGER)(uintptr_t)ValuePtr == SQL_FALSE) return SQL_SUCCESS;
      break;
    case SQL_ATTR_ODBC_CURSORS:
      // FIXME:
      if ((SQLULEN)(uintptr_t)ValuePtr == SQL_CUR_USE_DRIVER) return SQL_SUCCESS;
      break;
    case SQL_ATTR_PACKET_SIZE:
      break;
#ifdef _WIN32      /* { */
    case SQL_ATTR_QUIET_MODE:
      conn->win_handle = (HWND)ValuePtr;
      return SQL_SUCCESS;
#endif             /* } */

    case SQL_ATTR_TRACE:
      if ((SQLUINTEGER)(uintptr_t)ValuePtr == SQL_OPT_TRACE_OFF) return SQL_SUCCESS;
      break;
    case SQL_ATTR_TRACEFILE:
      break;
    case SQL_ATTR_TRANSLATE_LIB:
      break;
    case SQL_ATTR_TRANSLATE_OPTION:
      break;
    case SQL_ATTR_TXN_ISOLATION:
      conn->txn_isolation = (SQLINTEGER)(SQLLEN)ValuePtr;
      return SQL_SUCCESS;
    case SQL_ATTR_MAX_ROWS:
      if ((SQLULEN)ValuePtr == 0) return SQL_SUCCESS;
      break;
    case SQL_ATTR_QUERY_TIMEOUT:
      if ((SQLULEN)ValuePtr == 0) return SQL_SUCCESS;
      break;

    default:
      break;
  }
  conn_append_err_format(conn, "HY000", 0, "General error:`%s[0x%x/%d]` not supported yet", sql_conn_attr(Attribute), Attribute, Attribute);
  return SQL_ERROR;
}

#if (ODBCVER >= 0x0300)           /* { */
static SQLRETURN _conn_get_attr_current_qualifier(
    conn_t       *conn,
    SQLPOINTER    Value,
    SQLINTEGER    BufferLength,
    SQLINTEGER   *StringLengthPtr)
{
  int r = 0;

  r = _conn_get_current_db(conn, (char*)Value, BufferLength);
  if (r) return SQL_ERROR;

  int n = (int)strlen((const char*)Value);
  if (StringLengthPtr) *StringLengthPtr = n;

  if (n >= BufferLength) {
    conn_append_err(conn, "01004", 0, "String data, right truncated`");
    // FIXME: or SQL_ERROR?
    return SQL_SUCCESS_WITH_INFO;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _conn_check_alive(conn_t *conn, SQLPOINTER Value)
{
  int r = 0;

  *(SQLUINTEGER*)Value = SQL_CD_TRUE;

  if (conn->dead) return SQL_SUCCESS;
  if (conn->ds_conn.taos == NULL) return SQL_SUCCESS;

  // FIXME: check [taosc] for better performance!!!
  const char *sql = "select 1";
  ds_res_t ds_res = {0};

  r = ds_conn_query(&conn->ds_conn, sql, &ds_res);
  if (r) {
    conn->dead = 1;
    int e = ds_res_errno(&ds_res);
    const char *estr = ds_res_errstr(&ds_res);
    OW("connection[%p] is dead because of query `%s` failure:[%d]%s", conn, sql, e, estr);
  } else {
    *(SQLUINTEGER*)Value = SQL_CD_FALSE;
  }

  ds_res_close(&ds_res);

  return SQL_SUCCESS;
}

SQLRETURN conn_get_attr(
    conn_t       *conn,
    SQLINTEGER    Attribute,
    SQLPOINTER    Value,
    SQLINTEGER    BufferLength,
    SQLINTEGER   *StringLengthPtr)
{
  if (conn->cfg.url) {
    // conn_append_err(conn, "HY000", 0, "General error:websocket backend not implemented yet");
    // return SQL_ERROR;
  }

  switch (Attribute) {
    case SQL_ATTR_ACCESS_MODE:
      break;
#if (ODBCVER >= 0x0380)      /* { */
    case SQL_ATTR_ASYNC_DBC_EVENT:
      break;
    case SQL_ATTR_ASYNC_DBC_FUNCTIONS_ENABLE:
      break;
    case SQL_ATTR_ASYNC_DBC_PCALLBACK:
      break;
    case SQL_ATTR_ASYNC_DBC_PCONTEXT:
      break;
#endif                       /* } */
    case SQL_ATTR_ASYNC_ENABLE:
      break;
    case SQL_ATTR_AUTO_IPD:
      *(SQLUINTEGER*)Value = SQL_FALSE;
      break;
    case SQL_ATTR_AUTOCOMMIT:
      break;
    case SQL_ATTR_CONNECTION_DEAD:
      return _conn_check_alive(conn, Value);
    case SQL_ATTR_CONNECTION_TIMEOUT:
      break;
    case SQL_ATTR_CURRENT_CATALOG: /* SQL_CURRENT_QUALIFIER */
      return _conn_get_attr_current_qualifier(conn, Value, BufferLength, StringLengthPtr);
#if (ODBCVER >= 0x0380)      /* { */
    case SQL_ATTR_DBC_INFO_TOKEN:
      break;
#endif                       /* } */
    case SQL_ATTR_ENLIST_IN_DTC:
      break;
    case SQL_ATTR_LOGIN_TIMEOUT:
      switch (conn->cfg.customproduct) {
        case CUSTP_KINGSCADA:
          // FIXME: add for king scada joint debugging
          *(SQLUINTEGER*)Value = conn->login_timeout;
          return SQL_SUCCESS;
        default:
          *(SQLUINTEGER*)Value = 0;
          break;
      }
      return SQL_SUCCESS;
    case SQL_ATTR_METADATA_ID:
      break;
    case SQL_ATTR_ODBC_CURSORS:
      break;
    case SQL_ATTR_PACKET_SIZE:
      break;
    case SQL_ATTR_QUIET_MODE:
      break;
    case SQL_ATTR_TRACE:
      break;
    case SQL_ATTR_TRACEFILE:
      break;
    case SQL_ATTR_TRANSLATE_LIB:
      break;
    case SQL_ATTR_TRANSLATE_OPTION:
      break;
    case SQL_ATTR_TXN_ISOLATION:
      *(SQLUINTEGER*)Value = conn->txn_isolation;
      return SQL_SUCCESS;
    default:
      break;
  }
  conn_append_err_format(conn, "HY000", 0, "General error:`%s[0x%x/%d]` not supported yet", sql_conn_attr(Attribute), Attribute, Attribute);
  return SQL_ERROR;
}
#endif                            /* } */

void conn_clr_errs(conn_t *conn)
{
  errs_clr(&conn->errs);
}

SQLRETURN conn_get_diag_field(
    conn_t         *conn,
    SQLSMALLINT     RecNumber,
    SQLSMALLINT     DiagIdentifier,
    SQLPOINTER      DiagInfoPtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  switch (DiagIdentifier) {
    case SQL_DIAG_CLASS_ORIGIN:
      return errs_get_diag_field_class_origin(&conn->errs, RecNumber, DiagIdentifier, DiagInfoPtr, BufferLength, StringLengthPtr);
    case SQL_DIAG_SUBCLASS_ORIGIN:
      return errs_get_diag_field_subclass_origin(&conn->errs, RecNumber, DiagIdentifier, DiagInfoPtr, BufferLength, StringLengthPtr);
    case SQL_DIAG_SQLSTATE:
      return errs_get_diag_field_sqlstate(&conn->errs, RecNumber, DiagIdentifier, DiagInfoPtr, BufferLength, StringLengthPtr);
    case SQL_DIAG_CONNECTION_NAME:
      // TODO:
      *(char*)DiagInfoPtr = '\0';
      if (StringLengthPtr) *StringLengthPtr = 0;
      return SQL_SUCCESS;
    case SQL_DIAG_SERVER_NAME:
      // TODO:
      *(char*)DiagInfoPtr = '\0';
      if (StringLengthPtr) *StringLengthPtr = 0;
      return SQL_SUCCESS;
    default:
      conn_append_err_format(conn, "HY000", 0, "General error:RecNumber:[%d]; DiagIdentifier:[%d]%s", RecNumber, DiagIdentifier, sql_diag_identifier(DiagIdentifier));
      return SQL_ERROR;
  }
}

SQLRETURN conn_native_sql(
    conn_t        *conn,
    SQLCHAR       *InStatementText,
    SQLINTEGER     TextLength1,
    SQLCHAR       *OutStatementText,
    SQLINTEGER     BufferLength,
    SQLINTEGER    *TextLength2Ptr)
{
  (void)InStatementText;
  (void)TextLength1;
  (void)OutStatementText;
  (void)BufferLength;
  (void)TextLength2Ptr;
  conn_append_err(conn, "HY000", 0, "General error:not supported yet");
  return SQL_ERROR;
}

SQLRETURN conn_browse_connect(
    conn_t        *conn,
    SQLCHAR       *InConnectionString,
    SQLSMALLINT    StringLength1,
    SQLCHAR       *OutConnectionString,
    SQLSMALLINT    BufferLength,
    SQLSMALLINT   *StringLength2Ptr)
{
  (void)InConnectionString;
  (void)StringLength1;
  (void)OutConnectionString;
  (void)BufferLength;
  (void)StringLength2Ptr;
  conn_append_err(conn, "HY000", 0, "General error:not supported yet");
  return SQL_ERROR;
}

SQLRETURN conn_complete_async(
    conn_t      *conn,
    RETCODE     *AsyncRetCodePtr)
{
  (void)AsyncRetCodePtr;
  conn_append_err(conn, "HY000", 0, "General error:not supported yet");
  return SQL_ERROR;
}

const char* conn_get_sqlc_charset(conn_t *conn)
{
  return conn->sqlc_charset;
}

const char* conn_get_tsdb_charset(conn_t *conn)
{
  return conn->tsdb_charset;
}

const char* conn_get_sqlc_charset_for_col_bind(conn_t *conn)
{
  const char *charset = conn->cfg.charset_for_col_bind;
  if (charset) return charset;
  return conn->sqlc_charset;
}

const char* conn_get_sqlc_charset_for_param_bind(conn_t *conn)
{
  const char *charset = conn->cfg.charset_for_param_bind;
  if (charset) return charset;
  return conn->sqlc_charset;
}

int conn_is_ws_backended(conn_t *conn)
{
  return !!conn->cfg.url;
}

