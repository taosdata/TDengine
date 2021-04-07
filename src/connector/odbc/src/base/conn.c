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

#include "conn.h"

#include "env.h"

#include "../todbc_tls.h"

static void init_encodes(conn_t *conn, env_t *env) {
  OILE(conn, "");
  OILE(env, "");
  const char *enc_charset = env->enc_charset;

  snprintf(conn->enc_src,    sizeof(conn->enc_src),    "%s", UTF8_ENC);    // compile-time constant
  snprintf(conn->enc_wchar,  sizeof(conn->enc_wchar),  "%s", UTF16_ENC);   // compile-time constant
  snprintf(conn->enc_char,   sizeof(conn->enc_char),   "%s", enc_charset); // runtime default
  snprintf(conn->enc_db,     sizeof(conn->enc_db),     "%s", enc_charset); // runtime default
  snprintf(conn->enc_locale, sizeof(conn->enc_locale), "%s", enc_charset); // runtime default

  OD("enc_src:[%s]; enc_wchar:[%s]; enc_char:[%s]; enc_db:[%s]; enc_locale:[%s]", 
      conn->enc_src, conn->enc_wchar, conn->enc_char, conn->enc_db, conn->enc_locale);
}

static int conn_init(conn_t *conn, env_t *env) {
  OILE(conn, "");
  OILE(env, "");
  errs_t *errs = &env->errs;

  OILE(conn->env==NULL, "");

  int r = errs_init(&conn->errs);
  if (r) return -1;

  init_encodes(conn, env);
  if (SQL_SUCCESS!=conn_check_charset(conn, errs)) {
    return -1;
  }

  conn->env   = env;
  env_inc_ref(env);

  return 0;
};

static void conn_release(conn_t *conn) {
  if (!conn) return;
  env_t *env = conn->env;
  if (!env) return;

  conn->env = NULL;
  env_dec_ref(env);
}

conn_t* conn_new(env_t *env) {
  conn_t *conn = (conn_t*)calloc(1, sizeof(*conn));
  if (!conn) return NULL;

  if (conn_init(conn, env)) {
    OILE(conn->env==NULL, "");
    free(conn);
    return NULL;
  }

  return conn;
}

void conn_free(conn_t *conn) {
  if (!conn) return;

  // clean ext stuff
  if (conn->ext.free_conn) {
    conn->ext.free_conn(conn);
  }

  // clean conn stuffs
  conn_release(conn);

  free(conn);
}

static SQLRETURN do_check_charset(errs_t *errs, const char *enc_charset, todbc_enc_t *enc) {
  *enc = todbc_tls_iconv_enc(enc_charset);
  if (enc->enc[0]=='\0') {
    if (errs) {
      SET_GENERAL(errs, "unknown charset [%s]", enc_charset);
    }
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

SQLRETURN conn_check_charset(conn_t *conn, errs_t *errs) {
  OILE(conn, "");

  todbc_enc_t enc;

  SQLRETURN r;
  r = do_check_charset(errs, conn->enc_char, &enc);
  if (r!=SQL_SUCCESS) return r;
  r = do_check_charset(errs, conn->enc_db, &enc);
  if (r!=SQL_SUCCESS) return r;
  r = do_check_charset(errs, conn->enc_locale, &enc);
  if (r!=SQL_SUCCESS) return r;
  r = do_check_charset(errs, conn->enc_src, &enc);
  if (r!=SQL_SUCCESS) return r;

  r = do_check_charset(errs, conn->enc_wchar, &enc);
  if (r!=SQL_SUCCESS) return r;

  if (enc.variable_char_size!=-1) {
    OE("does not support [%s] for WCHAR", conn->enc_wchar);
    if (errs) {
      SET_GENERAL(errs, "does not support [%s] for WCHAR", conn->enc_wchar);
    }
    return SQL_ERROR;
  }
  if (enc.char_size<=0) {
    if (errs) {
      SET_GENERAL(errs, "unknown [%s] for WCHAR", conn->enc_wchar);
    }
    return SQL_ERROR;
  }

  conn->wchar_size = (size_t)enc.char_size;

  return SQL_SUCCESS;
}

int conn_add_stmt(conn_t *conn, stmt_t *stmt) {
  OILE(conn, "");
  OILE(stmt, "");
  OILE(stmt->owner==NULL, "");
  OILE(stmt->next==NULL, "");
  OILE(stmt->prev==NULL, "");
  OILE(conn->ext.init_stmt, "");

  if (conn->ext.init_stmt(stmt)) {
    return -1;
  }

  stmts_t *owner = &conn->stmts;

  stmt->owner = owner;

  stmt->prev = owner->tail;
  if (owner->tail) owner->tail->next = stmt;
  else             owner->head       = stmt;
  owner->tail = stmt;

  ++owner->count;
  owner->conn = conn;

  return 0;
}

void conn_del_stmt(conn_t *conn, stmt_t *stmt) {
  OILE(conn, "");
  OILE(stmt, "");
  OILE(stmt->owner, "");
  OILE(stmt->owner==&conn->stmts, "");
  OILE(stmt->owner->conn==conn, "");

  stmts_t *owner = stmt->owner;

  stmt_t *next = stmt->next;
  stmt_t *prev = stmt->prev;

  if (next) next->prev   = prev;
  else      owner->tail  = prev;

  if (prev) prev->next   = next;
  else      owner->head  = next;

  --owner->count;

  stmt->next  = NULL;
  stmt->prev  = NULL;
  stmt->owner = NULL;
}

SQLRETURN conn_connect(conn_t *conn) {
  OILE(conn, "");
  OILE(conn->ext.connect, "");
  return conn->ext.connect(conn);
}

void conn_disconnect(conn_t *conn) {
  OILE(conn, "");
  OILE(conn->ext.disconnect, "");
  conn->ext.disconnect(conn);
}

// public
errs_t* conn_get_errs(conn_t *conn) {
  OILE(conn, "");

  return &conn->errs;
}

void conn_clr_errs(conn_t *conn) {
  if (!conn) return;

  errs_reclaim(&conn->errs);
}





