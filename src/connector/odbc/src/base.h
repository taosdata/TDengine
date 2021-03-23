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

#ifndef _base_h_
#define _base_h_

#include "todbc_buf.h"
#include "todbc_iconv.h"
#include "todbc_log.h"

#include "taos.h"
#include "taoserror.h"

#include <sql.h>
#include <sqlext.h>

typedef struct errs_s               errs_t;
typedef struct env_s                env_t;
typedef struct conn_s               conn_t;
typedef struct stmt_s               stmt_t;
typedef struct param_s              param_t;
typedef struct field_s              field_t;
typedef struct rs_s                 rs_t;
typedef struct col_s                col_t;

#define GET_REF(obj) atomic_load_64(&obj->refcount)
#define INC_REF(obj) atomic_add_fetch_64(&obj->refcount, 1)
#define DEC_REF(obj) atomic_sub_fetch_64(&obj->refcount, 1)

// public
#ifdef __GNUC__
 __attribute__((format(printf, 6, 7)))
#endif
SQLRETURN errs_append(errs_t *errs, const char sql_state[6], const char *file, int line, const char *func, const char *fmt, ...);
int       errs_count(errs_t *errs);
// 0/-1: ok/no-error
int       errs_fetch(errs_t *errs, int idx, const char **sql_state, const char **err_str);
void      errs_clear(SQLSMALLINT HandleType, SQLHANDLE InputHandle);

// err: if <>0, will generate strerror
#define SET_ERR(errs, sql_state, fmt, ...)      \
        errs_append(errs, sql_state, __FILE__, __LINE__, __func__, "%s" fmt "", "", ##__VA_ARGS__)

#define SET_OOM(errs, fmt, ...) SET_ERR(errs, "TD001", "OOM:" fmt, ##__VA_ARGS__)
#define SET_NIY(errs, fmt, ...) SET_ERR(errs, "TDC00", "NIY:" fmt, ##__VA_ARGS__)
#define SET_GENERAL(errs, fmt, ...) SET_ERR(errs, "TD000", "GEN:" fmt, ##__VA_ARGS__)


// public
errs_t*   env_get_errs(env_t *env);
void      env_clr_errs(env_t *env);
void      env_inc_ref(env_t *env);
void      env_dec_ref(env_t *env);

// public
errs_t*   conn_get_errs(conn_t *conn);
void      conn_clr_errs(conn_t *conn);

// public
errs_t*   stmt_get_errs(stmt_t *stmt);
void      stmt_clr_errs(stmt_t *stmt);

#endif // _base_h_


