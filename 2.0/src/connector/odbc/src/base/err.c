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

#include "err.h"

#include "env.h"
#include "conn.h"
#include "stmt.h"

struct err_s {
  char                sql_state[6];
  char               *err_str;         // no ownership
};

static void errs_clr(errs_t *errs) {
  if (!errs) return;

  errs->count = 0;
  errs->errs  = NULL;
}

int errs_init(errs_t *errs) {
  OILE(errs && errs->cache==NULL, "");
  OILE(errs->count==0 && errs->errs==NULL, "");

  errs->cache = todbc_buf_create();

  return errs->cache ? 0 : -1;
}

void errs_reclaim(errs_t *errs) {
  if (!errs) return;
  if (!errs->cache) return;

  errs_clr(errs);

  todbc_buf_reclaim(errs->cache);
}

void errs_release(errs_t *errs) {
  if (!errs) return;
  if (!errs->cache) return;

  errs_clr(errs);

  todbc_buf_free(errs->cache);
  errs->cache = NULL;
}

// public
#ifdef __GNUC__
 __attribute__((format(printf, 6, 7)))
#endif
SQLRETURN errs_append(errs_t *errs, const char sql_state[6], const char *file, int line, const char *func, const char *fmt, ...)
{
  OILE(errs, "");
  OILE(errs->cache, "");
  todbc_buf_t *cache = errs->cache;

  const char *name = basename((char*)file);

  char      *buf   = NULL;
  size_t     blen  = 0;
  while (1) {
    char   *p     = buf;
    size_t  bytes = blen;

    int count = 0;
    int n = 0;

    va_list        ap;
    va_start(ap, fmt);
    if (bytes<0) bytes = 0;
    n = vsnprintf(p, bytes, fmt, ap);
    va_end(ap);

    OILE(n>=0, "");

    count += n;
    if (p)       p     += n;
    if (bytes)   bytes -= (size_t)n;

    if (bytes<0) bytes = 0;
    n = snprintf(p, bytes, "@%s[%d]%s()\n", name, line, func);

    OILE(n>=0, "");
    count += n;

    if (p) break;

    buf = todbc_buf_alloc(cache, (size_t)count + 1);
    if (!buf) return SQL_ERROR;
    blen = (size_t)count;
  }

  size_t bytes = (size_t)(errs->count + 1) * sizeof(err_t);

  err_t *es = (err_t*)todbc_buf_realloc(cache, errs->errs, bytes);
  if (!es) return SQL_ERROR;
  errs->errs   = es;
  errs->count += 1;

  err_t *err = errs->errs + errs->count - 1;
  snprintf(err->sql_state, sizeof(err->sql_state), "%s", sql_state);
  err->err_str = buf;


  return SQL_SUCCESS;
}

int errs_count(errs_t *errs) {
  OILE(errs, "");
  OILE(errs->cache, "");

  return errs->count;
}

// 0/-1: ok/no-error
int errs_fetch(errs_t *errs, int idx, const char **sql_state, const char **err_str) {
  OILE(errs, "");
  OILE(errs->cache, "");

  if (errs->count<=0) return -1;
  if (idx<0 || idx>=errs->count) return -1;

  err_t *err = errs->errs + idx;

  if (sql_state) *sql_state = err->sql_state;
  if (err_str)   *err_str   = err->err_str;

  return 0;
}

void errs_clear(SQLSMALLINT HandleType, SQLHANDLE InputHandle) {
  errs_t *errs = NULL;

  if (InputHandle==NULL) return;

  switch (HandleType)
  {
    case SQL_HANDLE_ENV:
      {
        env_t *env = (env_t*)InputHandle;
        errs = &env->errs;
      } break;
    case SQL_HANDLE_DBC:
      {
        conn_t *conn = (conn_t*)InputHandle;
        errs = &conn->errs;
      } break;
    case SQL_HANDLE_STMT:
      {
        stmt_t *stmt = (stmt_t*)InputHandle;
        errs = &stmt->errs;
      } break;
    default:
      {
        ONIY(0, "");
      } break;
  }

  if (!errs) return;
  errs_reclaim(errs);
}

