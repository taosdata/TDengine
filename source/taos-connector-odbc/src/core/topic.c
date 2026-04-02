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

#include "topic.h"

#include "desc.h"
#include "errs.h"
#include "log.h"
#include "conn_parser.h"
#include "stmt.h"
#include "taos_helpers.h"
#ifdef HAVE_TAOSWS           /* { */
#include "taosws_helpers.h"
#endif                       /* } */

#include <errno.h>

static void _topic_cfg_reset_names(topic_cfg_t *cfg)
{
  if (!cfg) return;
  for (size_t i=0; i<cfg->names_nr; ++i) {
    TOD_SAFE_FREE(cfg->names[i]);
  }
  cfg->names_nr = 0;
}

static void _topic_cfg_release_names(topic_cfg_t *cfg)
{
  if (!cfg) return;
  _topic_cfg_reset_names(cfg);
  TOD_SAFE_FREE(cfg->names);
  cfg->names_cap = 0;
}

void topic_cfg_release(topic_cfg_t *cfg)
{
  if (!cfg) return;
  _topic_cfg_release_names(cfg);
  kvs_release(&cfg->kvs);
}

void topic_cfg_transfer(topic_cfg_t *from, topic_cfg_t *to)
{
  if (from == to) return;
  topic_cfg_release(to);

  memcpy(to, from, sizeof(*from));
  memset(from, 0, sizeof(*from));
}

int topic_cfg_append_kv(topic_cfg_t *cfg, const char *k, size_t kn, const char *v, size_t vn)
{
  kvs_t *kvs = &cfg->kvs;
  return kvs_append(kvs, k, kn, v, vn);
}

static void _topic_release_tripple(topic_t *topic)
{
  mem_release(&topic->res_topic_name);
  mem_release(&topic->res_db_name);
  topic->res_vgroup_id     = 0;
}

static void _topic_reset_res(topic_t *topic, SQLRETURN *psr)
{
  SQLRETURN sr = SQL_SUCCESS;
  if (topic->res && !topic->do_not_commit) {
    int r = CALL_tmq_commit_sync(topic->tmq, topic->res);
    if (r) {
      stmt_append_err_format(topic->owner, "HY000", 0, "General error:[taosc]tmq_commit_sync failed:[%d/0x%x]%s",
          r, r, tmq_err2str(r));
      topic->do_not_commit = 1;
      sr = SQL_ERROR;
    }
  }
  if (topic->res) {
#ifdef HAVE_TAOSWS           /* [ */
    if (topic->owner->conn->cfg.url) {
      CALL_ws_free_result((WS_RES*)topic->res);
    } else {
#endif                       /* ] */
      CALL_taos_free_result(topic->res);
#ifdef HAVE_TAOSWS           /* [ */
    }
#endif                       /* ] */
    topic->res = NULL;
  }
  if (psr) *psr = sr;
}

static void _topic_reset_tmq(topic_t *topic)
{
  if (!topic->tmq) return;
  if (topic->subscribed) {
    CALL_tmq_unsubscribe(topic->tmq);
    topic->subscribed = 0;
  }
  CALL_tmq_consumer_close(topic->tmq);
  topic->tmq = NULL;
}

void topic_reset(topic_t *topic)
{
  if (!topic) return;
  topic->row = NULL;
  _topic_reset_res(topic, NULL);
  _topic_reset_tmq(topic);
  topic->fields_nr = 0;
}

static void _topic_release_conf(topic_t *topic)
{
  if (!topic->conf) return;
  CALL_tmq_conf_destroy(topic->conf);
  topic->conf = NULL;
}

void topic_release(topic_t *topic)
{
  if (!topic) return;
  topic_reset(topic);
  topic_cfg_release(&topic->cfg);
  TOD_SAFE_FREE(topic->fields);
  topic->fields_cap = 0;
  _topic_release_conf(topic);
  _topic_release_tripple(topic);
}

static SQLRETURN _prepare(stmt_base_t *base, const sqlc_tsdb_t *sqlc_tsdb)
{
  (void)sqlc_tsdb;

  topic_t *topic = (topic_t*)base;
  (void)topic;
  stmt_append_err(topic->owner, "HY000", 0, "General error:internal logic error");
  return SQL_ERROR;
}

static SQLRETURN _get_col_fields(stmt_base_t *base, TAOS_FIELD **fields, size_t *nr)
{
  (void)fields;
  (void)nr;
  topic_t *topic = (topic_t*)base;
  (void)topic;
  if (!topic->res) {
    if (fields) *fields = NULL;
    if (nr) *nr = 0;
    return SQL_SUCCESS;
  }

  if (fields) *fields = topic->fields;
  if (nr) *nr = topic->fields_nr;

  return SQL_SUCCESS;
}

static SQLRETURN _topic_desc_tripple(topic_t *topic)
{
  int r = 0;
  int topic_change = 0;

  const char *res_topic_name = CALL_tmq_get_topic_name(topic->res);
  if (!res_topic_name || !*res_topic_name) {
    stmt_append_err(topic->owner, "HY000", 0, "General error:tmq_get_topic_name failed");
    return SQL_ERROR;
  }
  if (topic->res_topic_name.base && strcmp((const char*)topic->res_topic_name.base, res_topic_name)) {
    topic_change = 1;
  }
  r = mem_copy(&topic->res_topic_name, res_topic_name);
  if (r) {
    stmt_oom(topic->owner);
    return SQL_ERROR;
  }

  const char *res_db_name    = CALL_tmq_get_db_name(topic->res);
  if (!res_db_name || !*res_db_name) {
    stmt_append_err(topic->owner, "HY000", 0, "General error:tmq_get_db_name failed");
    return SQL_ERROR;
  }
  r = mem_copy(&topic->res_db_name, res_db_name);
  if (r) {
    stmt_oom(topic->owner);
    return SQL_ERROR;
  }

  topic->res_vgroup_id  = CALL_tmq_get_vgroup_id(topic->res);

  TAOS_FIELD *fields = NULL;
  size_t nr = 0;

#ifdef HAVE_TAOSWS           /* [ */
  if (topic->owner->conn->cfg.url) {
    DLL_EXPORT int         taos_fetch_raw_block(TAOS_RES *res, int *numOfRows, void **pData);
    fields = (TAOS_FIELD*)ws_fetch_fields((WS_RES*)topic->res);
    if (!fields) {
      stmt_append_err_format(topic->owner, "HY000", 0,
          "General error:taos_fetch_fields failed:[%d]%s",
          ws_errno(topic->res), ws_errstr(topic->res));
      return SQL_ERROR;
    }
    nr = CALL_ws_field_count(topic->res);
  } else {
#endif                       /* ] */
    fields = CALL_taos_fetch_fields(topic->res);
    if (!fields) {
      stmt_append_err_format(topic->owner, "HY000", 0,
          "General error:taos_fetch_fields failed:[%d]%s",
          taos_errno(topic->res), taos_errstr(topic->res));
      return SQL_ERROR;
    }
    nr = CALL_taos_field_count(topic->res);
#ifdef HAVE_TAOSWS           /* [ */
  }
#endif                       /* ] */

  const char *pseudos[] = {
    "_topic_name",
    "_db_name",
    "_vgroup_id",
  };
  for (size_t i=0; i<nr; ++i) {
    TAOS_FIELD *field = fields + i;
    for (size_t j=0; j<sizeof(pseudos)/sizeof(pseudos[0]); ++j) {
      if (tod_strncasecmp(field->name, pseudos[i], sizeof(field->name)) == 0) {
        stmt_append_err_format(topic->owner, "HY000", 0,
            "General error:column[%zd]`%.*s` in select list conflicts with the pseudo-name:[%s]",
            i+1, (int)sizeof(field->name), field->name, pseudos[i]);
        return SQL_ERROR;
      }
    }
  }

  if (nr + 3 > topic->fields_cap) {
    size_t cap = (nr + 3 + 15) / 16 * 16;
    TAOS_FIELD *p = (TAOS_FIELD*)realloc(topic->fields, sizeof(*p) * cap);
    if (!p) {
      stmt_oom(topic->owner);
      return SQL_ERROR;
    }
    topic->fields        = p;
    topic->fields_cap    = cap;
  }
  topic->fields_nr = nr + 3;

  memcpy(topic->fields + 3, fields, nr * sizeof(*fields));
  snprintf(topic->fields[0].name, sizeof(topic->fields[0].name), "_topic_name");
  snprintf(topic->fields[1].name, sizeof(topic->fields[1].name), "_db_name");
  snprintf(topic->fields[2].name, sizeof(topic->fields[2].name), "_vgroup_id");
  topic->fields[0].type = TSDB_DATA_TYPE_VARCHAR;
  topic->fields[0].bytes = (int32_t)topic->res_topic_name.nr;
  topic->fields[1].type = TSDB_DATA_TYPE_VARCHAR;
  topic->fields[1].bytes = (int32_t)topic->res_db_name.nr;
  topic->fields[2].type = TSDB_DATA_TYPE_INT;
  topic->fields[2].bytes = sizeof(int32_t);

  if (topic_change) return SQL_NO_DATA;

  return SQL_SUCCESS;
}

static SQLRETURN _poll(topic_t *topic)
{
  SQLRETURN sr = SQL_SUCCESS;

  while (topic->res == NULL) {
    if (topic->records_max >= 0 && topic->records_count >= topic->records_max) {
      stmt_append_err_format(topic->owner, "HY000", 0, "General error:taos_odbc.limit.records[%" PRId64 "] has been reached", topic->records_max);
      return SQL_NO_DATA;
    }
    time_t t1 = time(NULL);
    if (topic->seconds_max >= 0 && difftime(t1, topic->t0) > topic->seconds_max) {
      stmt_append_err_format(topic->owner, "HY000", 0, "General error:taos_odbc.limit.seconds[%" PRId64 "] has been reached", topic->seconds_max);
      return SQL_NO_DATA;
    }

    int32_t timeout = 100;
    topic->res = CALL_tmq_consumer_poll(topic->tmq, timeout);
    if (topic->res) {
      sr = _topic_desc_tripple(topic);
      if (sr == SQL_NO_DATA) return SQL_NO_DATA;
      if (sr != SQL_SUCCESS) {
        topic->do_not_commit = 1;
        return SQL_ERROR;
      }
    }
  }

  return SQL_SUCCESS;
}

static SQLRETURN _execute(stmt_base_t *base)
{
  SQLRETURN sr = SQL_SUCCESS;

  topic_t *topic = (topic_t*)base;
  (void)topic;

  sr = _poll(topic);
  if (sr == SQL_NO_DATA) return SQL_NO_DATA;
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  return SQL_SUCCESS;
}

static SQLRETURN _fetch_row(stmt_base_t *base)
{
  SQLRETURN sr = SQL_SUCCESS;

  topic_t *topic = (topic_t*)base;
  (void)topic;

  TAOS_ROW row = NULL;

again:

  sr = _poll(topic);
  if (sr == SQL_NO_DATA) return SQL_NO_DATA;
  if (sr != SQL_SUCCESS) return SQL_ERROR;

#ifdef HAVE_TAOSWS           /* [ */
  if (topic->owner->conn->cfg.url) {
    stmt_append_err(topic->owner, "HY000", 0, "General error:not implemented yet");
    return SQL_ERROR;
  } else {
#endif                       /* ] */
    row = CALL_taos_fetch_row(topic->res);
    if (row == NULL) {
      // NOTE: once no row is available, which implicitly means that user has traversed all rows within current res
      //       this seems the right time to call tmq_commit_sync
      _topic_reset_res(topic, &sr);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
      goto again;
    }
    topic->row = row;
#ifdef HAVE_TAOSWS           /* [ */
  }
#endif                       /* ] */

  ++topic->records_count;
  return SQL_SUCCESS;
}

static SQLRETURN _more_results(stmt_base_t *base)
{
  topic_t *topic = (topic_t*)base;
  (void)topic;
  if (!topic->res) return SQL_NO_DATA;
  return SQL_SUCCESS;
}

static SQLRETURN _describe_param(stmt_base_t *base,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT    *DataTypePtr,
    SQLULEN        *ParameterSizePtr,
    SQLSMALLINT    *DecimalDigitsPtr,
    SQLSMALLINT    *NullablePtr)
{
  (void)ParameterNumber;
  (void)DataTypePtr;
  (void)ParameterSizePtr;
  (void)DecimalDigitsPtr;
  (void)NullablePtr;

  topic_t *topic = (topic_t*)base;
  (void)topic;
  stmt_append_err(topic->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _get_num_params(stmt_base_t *base, SQLSMALLINT *ParameterCountPtr)
{
  (void)ParameterCountPtr;

  topic_t *topic = (topic_t*)base;
  (void)topic;
  stmt_append_err(topic->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _tsdb_field_by_param(stmt_base_t *base, int i_param, TAOS_FIELD_E **field)
{
  (void)i_param;
  (void)field;

  topic_t *topic = (topic_t*)base;
  (void)topic;
  stmt_append_err(topic->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _row_count(stmt_base_t *base, SQLLEN *row_count_ptr)
{
  (void)row_count_ptr;
  topic_t *topic = (topic_t*)base;
  (void)topic;
  stmt_append_err(topic->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _get_num_cols(stmt_base_t *base, SQLSMALLINT *ColumnCountPtr)
{
  (void)ColumnCountPtr;
  topic_t *topic = (topic_t*)base;
  SQLSMALLINT n =0;
  if (topic->res) n = (SQLSMALLINT)topic->fields_nr;
  if (ColumnCountPtr) *ColumnCountPtr = n;
  return SQL_SUCCESS;
}

static SQLRETURN _get_data(stmt_base_t *base, SQLUSMALLINT Col_or_Param_Num, tsdb_data_t *tsdb)
{
  topic_t *topic = (topic_t*)base;
  TAOS_ROW row = topic->row;
  if (!row) {
    stmt_append_err(topic->owner, "HY000", 0, "General error:not implemented yet");
    return SQL_ERROR;
  }

  TAOS_FIELD *fields = topic->fields;

  int i = Col_or_Param_Num - 1;

  TAOS_FIELD *field = fields + i;

  if (i == 0) {
    tsdb->is_null                = 0;
    tsdb->type                   = TSDB_DATA_TYPE_VARCHAR;
    tsdb->str.str                = (const char*)topic->res_topic_name.base;
    tsdb->str.len                = topic->res_topic_name.nr;
    return SQL_SUCCESS;
  }

  if (i == 1) {
    tsdb->is_null                = 0;
    tsdb->type                   = TSDB_DATA_TYPE_VARCHAR;
    tsdb->str.str                = (const char*)topic->res_db_name.base;
    tsdb->str.len                = topic->res_db_name.nr;
    return SQL_SUCCESS;
  }

  if (i == 2) {
    tsdb->is_null                = 0;
    tsdb->type                   = TSDB_DATA_TYPE_INT;
    tsdb->i32                    = topic->res_vgroup_id;
    return SQL_SUCCESS;
  }

  int *offsets = NULL;

#ifdef HAVE_TAOSWS           /* [ */
  if (topic->owner->conn->cfg.url) {
    stmt_append_err(topic->owner, "HY000", 0, "General error:not implemented yet");
  } else {
#endif                       /* ] */
    offsets = CALL_taos_get_column_data_offset(topic->res, i-3);
#ifdef HAVE_TAOSWS           /* [ */
  }
#endif                       /* ] */

  char *col = row[i-3];
  if (col) col += offsets ? *offsets : 0;

  tsdb->type = field->type;
  if (col == NULL) {
    tsdb->is_null = 1;
    return SQL_SUCCESS;
  }
  tsdb->is_null = 0;
  switch (tsdb->type) {
    case TSDB_DATA_TYPE_TINYINT:
      tsdb->i8 = *(int8_t*)col;
      break;

    case TSDB_DATA_TYPE_UTINYINT:
      tsdb->u8 = *(uint8_t*)col;
      break;

    case TSDB_DATA_TYPE_SMALLINT:
      tsdb->i16 = *(int16_t*)col;
      break;

    case TSDB_DATA_TYPE_USMALLINT:
      tsdb->u16 = *(uint16_t*)col;
      break;

    case TSDB_DATA_TYPE_INT:
      tsdb->i32 = *(int32_t*)col;
      break;

    case TSDB_DATA_TYPE_UINT:
      tsdb->u32 = *(uint32_t*)col;
      break;

    case TSDB_DATA_TYPE_BIGINT:
      tsdb->i64 = *(int64_t*)col;
      break;

    case TSDB_DATA_TYPE_UBIGINT:
      tsdb->u64 = *(uint64_t*)col;
      break;

    case TSDB_DATA_TYPE_FLOAT:
      tsdb->flt = *(float*)col;
      break;

    case TSDB_DATA_TYPE_DOUBLE:
      tsdb->dbl = *(double*)col;
      break;

    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_NCHAR:
      tsdb->str.len = *(int16_t*)(col - sizeof(int16_t));
      tsdb->str.str = col;
      break;

    case TSDB_DATA_TYPE_TIMESTAMP:
      tsdb->ts.ts = *(int64_t*)col;
#ifdef HAVE_TAOSWS           /* [ */
      if (topic->owner->conn->cfg.url) {
        tsdb->ts.precision = CALL_ws_result_precision((WS_RES*)topic->res);
      } else {
#endif                       /* ] */
        tsdb->ts.precision = CALL_taos_result_precision(topic->res);
#ifdef HAVE_TAOSWS           /* [ */
      }
#endif                       /* ] */
      break;

    case TSDB_DATA_TYPE_BOOL:
      tsdb->b = !!*(int8_t*)col;
      break;

    default:
      stmt_append_err_format(topic->owner, "HY000", 0, "General error:`%s` not supported yet", taos_data_type(tsdb->type));
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

void topic_init(topic_t *topic, stmt_t *stmt)
{
  topic->owner = stmt;

  stmt_base_t *base = &topic->base;

  base->prepare                      = _prepare;
  base->execute                      = _execute;
  base->get_col_fields               = _get_col_fields;
  base->fetch_row                    = _fetch_row;
  base->more_results                 = _more_results;
  base->describe_param               = _describe_param;
  base->get_num_params               = _get_num_params;
  base->tsdb_field_by_param          = _tsdb_field_by_param;
  base->row_count                    = _row_count;
  base->get_num_cols                 = _get_num_cols;
  base->get_data                     = _get_data;
}

static void _tmq_commit_cb_print(tmq_t* tmq, int32_t code, void* param)
{
  if (0) fprintf(stderr, "%s(): code: %d, tmq: %p, param: %p\n", __func__, code, tmq, param);
}

static SQLRETURN _build_consumer(topic_t *topic)
{
  // https://github.com/taosdata/TDengine/blob/main/docs/en/07-develop/07-tmq.mdx#create-a-consumer
  // td.connect.ip
  // td.connect.user
  // td.connect.pass
  // td.connect.port
  // group.id
  // client.id
  // auto.offset.reset
  // enable.auto.commit
  // auto.commit.interval.ms
  // experimental.snapshot.enable
  // msg.with.table.name
  // taos_odbc.limit.records        /* return SQL_NO_DATA once # of records has been reached */
  // taos_odbc.limit.seconds        /* return SQL_NO_DATA once # of seconds has passed */
  stmt_t     *stmt          = topic->owner;
  conn_t     *conn          = stmt->conn;
  conn_cfg_t *cfg           = &conn->cfg;
  const char *ip            = cfg->ip;
  uint16_t    port          = cfg->port;

  _topic_release_conf(topic);

  tmq_conf_res_t code = TMQ_CONF_OK;

  topic->conf = CALL_tmq_conf_new();
  if (!topic->conf) {
    stmt_oom(topic->owner);
    return SQL_ERROR;
  }

  topic->records_max = -1;
  topic->seconds_max = -1;

  tmq_conf_t *conf = topic->conf;
  if (ip) {
    const char *k = "td.connect.ip";
    code = CALL_tmq_conf_set(conf, k, ip);
    if (code != TMQ_CONF_OK) {
      stmt_append_err_format(topic->owner, "HY000", 0,
          "General error:[taosc]tmq_conf_set(%s=%s) failed",
          k, ip);
      return SQL_ERROR;
    }
  }
  if (port) {
    char buf[64]; buf[0] = '\0';
    snprintf(buf, sizeof(buf), "%d", port);
    const char *k = "td.connect.port";
    code = CALL_tmq_conf_set(conf, k, buf);
    if (code != TMQ_CONF_OK) {
      stmt_append_err_format(topic->owner, "HY000", 0,
          "General error:[taosc]tmq_conf_set(%s=%d) failed",
          k, port);
      return SQL_ERROR;
    }
  }

  kvs_t *kvs = &topic->cfg.kvs;
  for (size_t i=0; i<kvs->nr; ++i) {
    kv_t *kv = kvs->kvs + i;
    if (!kv->val) continue;
    if (tod_strcasecmp(kv->key, "taos_odbc.limit.records") == 0) {
      topic->records_max = atoi(kv->val);
      continue;
    }
    if (tod_strcasecmp(kv->key, "taos_odbc.limit.seconds") == 0) {
      topic->seconds_max = atoi(kv->val);
      continue;
    }
    code = CALL_tmq_conf_set(conf, kv->key, kv->val);
    if (code != TMQ_CONF_OK) {
      stmt_append_err_format(topic->owner, "HY000", 0,
          "General error:[taosc]tmq_conf_set(%s=%s) failed",
          kv->key, kv->val);
      return SQL_ERROR;
    }
  }

  if (0) CALL_tmq_conf_set_auto_commit_cb(conf, _tmq_commit_cb_print, NULL);

  _topic_reset_tmq(topic);
  topic->tmq = CALL_tmq_consumer_new(conf, NULL, 0);
  if (!topic->tmq) {
    stmt_append_err(topic->owner, "HY000", 0, "General error:[taosc]tmq_consumer_new failed:reason unknown, but don't forget to specify `group.id`");
    return SQL_ERROR;
  }
  return SQL_SUCCESS;
}

static SQLRETURN _topic_open(
    topic_t       *topic,
    tmq_list_t    *topicList)
{
  int r = 0;

  for (size_t i=0; i<topic->cfg.names_nr; ++i) {
    int32_t code = CALL_tmq_list_append(topicList, topic->cfg.names[i]);
    if (code) {
      stmt_append_err_format(topic->owner, "HY000", 0, "General error:[taosc]tmq_list_append failed:[%d]%s",
          taos_errno(NULL), taos_errstr(NULL)); // FIXME: taos_errstr?
      return SQL_ERROR;
    }
  }

  r = CALL_tmq_subscribe(topic->tmq, topicList);
  topic->subscribed = (r == 0);

  if (r) {
    stmt_append_err_format(topic->owner, "HY000", 0, "General error:[taosc]:tmq_subscribe failed:[%d/0x%x]%s",
        r, r, tmq_err2str(r));
    return SQL_ERROR;
  }

  topic->records_count = 0;
  topic->t0 = time(NULL);

  return SQL_SUCCESS;
}

SQLRETURN topic_open(
    topic_t             *topic,
    const sqlc_tsdb_t   *sql,
    topic_cfg_t         *cfg)
{
  (void)sql;
  OA_ILE(topic->tmq == NULL);
  SQLRETURN sr = SQL_SUCCESS;

  topic_cfg_transfer(cfg, &topic->cfg);
  cfg = &topic->cfg;
  if (!cfg->names || cfg->names_nr == 0) {
    stmt_append_err(topic->owner, "HY000", 0, "General error:topic name not specified");
    return SQL_ERROR;
  }

  sr = _build_consumer(topic);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  tmq_list_t* topicList = CALL_tmq_list_new();
  if (!topicList) {
    stmt_oom(topic->owner);
    return SQL_ERROR;
  }
  sr = _topic_open(topic, topicList);
  CALL_tmq_list_destroy(topicList);
  if (sr == SQL_NO_DATA) return SQL_NO_DATA;
  if (sr != SQL_SUCCESS) {
    topic_reset(topic);
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

