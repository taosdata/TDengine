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

#include "env.h"
#include "conn.h"
#include "errs.h"
#include "log.h"
#include "taos_helpers.h"

static unsigned int         _taos_init_failed      = 0;

static void _exit_routine(void)
{
#ifdef _WIN32
  // NOTE: taos_cleanup would be hung-up under windows-ODBC
  //       need to check later
#else
  CALL_taos_cleanup();
#endif
}

static void _init_once(void)
{
#ifdef _WIN32
  // NOTE: taos_cleanup would be hung-up under windows-ODBC
  //       need to check later
#else
  if (CALL_taos_init()) {
    _taos_init_failed = 1;
    return;
  }
  atexit(_exit_routine);
#endif
}

static int _env_init(env_t *env)
{
  static pthread_once_t          once;
  pthread_once(&once, _init_once);

  env->debug_flex  = tod_get_debug_flex();
  env->debug_bison = tod_get_debug_bison();

  errs_init(&env->errs);

  if (_taos_init_failed) return -1;

  env->refc = 1;

  return 0;
}

int env_get_debug_flex(env_t *env)
{
  return !!env->debug_flex;
}

int env_get_debug_bison(env_t *env)
{
  return !!env->debug_bison;
}

static void _env_release(env_t *env)
{
  int conns = atomic_load(&env->conns);
  OA_ILE(conns == 0);
  errs_release(&env->errs);
  mem_release(&env->mem);
}

env_t* env_create(void)
{
  env_t *env = (env_t*)calloc(1, sizeof(*env));
  if (!env) return NULL;

  int r = _env_init(env);
  if (r) {
    _env_release(env);
    free(env);
    return NULL;
  }

  return env;
}

env_t* env_ref(env_t *env)
{
  int prev = atomic_fetch_add(&env->refc, 1);
  OA_ILE(prev>0);
  return env;
}

env_t* env_unref(env_t *env)
{
  int prev = atomic_fetch_sub(&env->refc, 1);
  if (prev>1) return env;
  OA_ILE(prev==1);

  _env_release(env);
  free(env);

  return NULL;
}

SQLRETURN env_free(env_t *env)
{
  int conns = atomic_load(&env->conns);
  if (conns) {
    env_append_err_format(env, "HY000", 0, "General error:%d connections are still connected or allocated", conns);
    return SQL_ERROR;
  }

  env_unref(env);
  return SQL_SUCCESS;
}

static SQLRETURN _env_commit(env_t *env)
{
  int conns = atomic_load(&env->conns);
  if (conns == 0) {
    env_append_err_format(env, "01000", 0, "General warning:no outstanding connection");
    return SQL_SUCCESS_WITH_INFO;
  }

  env_append_err_format(env, "25S02", 0, "Transaction is still active");
  return SQL_ERROR;
}

static SQLRETURN _env_rollback(env_t *env)
{
  int conns = atomic_load(&env->conns);
  if (conns == 0) {
    env_append_err_format(env, "01000", 0, "General warning:no outstanding connection");
    return SQL_SUCCESS_WITH_INFO;
  }

  env_append_err_format(env, "25S01", 0, "Transaction state unknown");
  return SQL_ERROR;
}

SQLRETURN env_get_diag_rec(
    env_t          *env,
    SQLSMALLINT     RecNumber,
    SQLCHAR        *SQLState,
    SQLINTEGER     *NativeErrorPtr,
    SQLCHAR        *MessageText,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *TextLengthPtr)
{
  return errs_get_diag_rec(&env->errs, RecNumber, SQLState, NativeErrorPtr, MessageText, BufferLength, TextLengthPtr);
}

static SQLRETURN _env_set_odbc_version(env_t *env, SQLINTEGER odbc_version)
{
  switch (odbc_version) {
    case SQL_OV_ODBC3:
      return SQL_SUCCESS;
    case SQL_OV_ODBC2:
      return SQL_SUCCESS;
    default:
      env_append_err_format(env, "01S02", 0,
          "Option value changed:`%s[0x%x/%d]` is substituted by `SQL_OV_ODBC3`", sql_odbc_version(odbc_version), odbc_version, odbc_version);
      OA_NIY(0);
      return SQL_SUCCESS_WITH_INFO;
  }
}

SQLRETURN env_set_attr(
    env_t       *env,
    SQLINTEGER   Attribute,
    SQLPOINTER   ValuePtr,
    SQLINTEGER   StringLength)
{
  (void)StringLength;

  switch (Attribute) {
#if (ODBCVER >= 0x0380)      /* { */
    case SQL_ATTR_CONNECTION_POOLING:
      // NOTE: SQL_ATTR_CONNECTION_POOLING is implemented inside the Driver Manager
      break;
#endif                       /* } */
    case SQL_ATTR_CP_MATCH:
      if (*(SQLUINTEGER*)ValuePtr == SQL_CP_STRICT_MATCH) return SQL_SUCCESS;
      env_append_err_format(env, "HYC00", 0,
          "Optional feature not implemented:`%s[0x%x/%d]` shall be set to SQL_CP_STRICT_MATCH",
          sql_env_attr(Attribute), Attribute, Attribute);
      return SQL_ERROR;
    case SQL_ATTR_ODBC_VERSION:
      return _env_set_odbc_version(env, (SQLINTEGER)(size_t)ValuePtr);
    case SQL_ATTR_OUTPUT_NTS:
      if (*(int32_t*)ValuePtr == SQL_TRUE) return SQL_SUCCESS;
      env_append_err_format(env, "HYC00", 0, "Optional feature not implemented:`%s[0x%x/%d]` shall be set to SQL_TRUE", sql_env_attr(Attribute), Attribute, Attribute);
      return SQL_ERROR;
    default:
      break;
  }

  env_append_err_format(env, "01S02", 0, "Optional value changed:`%s[0x%x/%d]` is substituted by default", sql_env_attr(Attribute), Attribute, Attribute);
  OA_NIY(0);
  return SQL_SUCCESS_WITH_INFO;
}

SQLRETURN env_get_attr(
    env_t       *env,
    SQLINTEGER   Attribute,
    SQLPOINTER   Value,
    SQLINTEGER   BufferLength,
    SQLINTEGER  *StringLength)
{
  (void)Value;
  (void)BufferLength;
  (void)StringLength;

  switch (Attribute) {
#if (ODBCVER >= 0x0380)      /* { */
    case SQL_ATTR_CONNECTION_POOLING:
      // NOTE: SQL_ATTR_CONNECTION_POOLING is implemented inside the Driver Manager
      break;
#endif                       /* } */
    case SQL_ATTR_CP_MATCH:
      *(SQLUINTEGER*)Value = SQL_CP_STRICT_MATCH;
      return SQL_SUCCESS;
    case SQL_ATTR_ODBC_VERSION:
      *(SQLINTEGER*)Value = (SQLINTEGER)SQL_OV_ODBC3;
      return SQL_SUCCESS;
    case SQL_ATTR_OUTPUT_NTS:
      *(int32_t*)Value = SQL_TRUE;
      return SQL_SUCCESS;
    default:
      break;
  }

  OE("General error:`%s[0x%x/%d]` not supported yet", sql_env_attr(Attribute), Attribute, Attribute);
  env_append_err_format(env, "HY000", 0, "General error:`%s[0x%x/%d]` not supported yet", sql_env_attr(Attribute), Attribute, Attribute);
  OA_NIY(0);
  return SQL_ERROR;
}

SQLRETURN env_end_tran(env_t *env, SQLSMALLINT CompletionType)
{
  switch (CompletionType) {
    case SQL_COMMIT:
      return _env_commit(env);
    case SQL_ROLLBACK:
      return _env_rollback(env);
    default:
      env_append_err_format(env, "HY000", 0, "General error:[DM]logic error");
      return SQL_ERROR;
  }
}

SQLRETURN env_alloc_conn(env_t *env, SQLHANDLE *OutputHandle)
{
  *OutputHandle = SQL_NULL_HANDLE;

  conn_t *conn = conn_create(env);
  if (conn == NULL) return SQL_ERROR;

  *OutputHandle = (SQLHANDLE)conn;
  return SQL_SUCCESS;
}

SQLRETURN env_get_diag_field(
    env_t          *env,
    SQLSMALLINT     RecNumber,
    SQLSMALLINT     DiagIdentifier,
    SQLPOINTER      DiagInfoPtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  switch (DiagIdentifier) {
    case SQL_DIAG_CLASS_ORIGIN:
      return errs_get_diag_field_class_origin(&env->errs, RecNumber, DiagIdentifier, DiagInfoPtr, BufferLength, StringLengthPtr);
    case SQL_DIAG_SUBCLASS_ORIGIN:
      return errs_get_diag_field_subclass_origin(&env->errs, RecNumber, DiagIdentifier, DiagInfoPtr, BufferLength, StringLengthPtr);
    case SQL_DIAG_SQLSTATE:
      return errs_get_diag_field_sqlstate(&env->errs, RecNumber, DiagIdentifier, DiagInfoPtr, BufferLength, StringLengthPtr);
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
      OA(0, "RecNumber:[%d]; DiagIdentifier:[%d]%s", RecNumber, DiagIdentifier, sql_diag_identifier(DiagIdentifier));
      return SQL_ERROR;
  }
}

void env_clr_errs(env_t *env)
{
  errs_clr(&env->errs);
}

size_t env_conv(env_t *env, iconv_t cnv, mem_t *mem, const char *src, char **pdst)
{
  (void)env;

  size_t nr = strlen(src);
  size_t sz = (nr + 1) * 3;

  int rr = 0;
  rr = mem_keep(mem, sz);
  if (rr) return (size_t)-1;

  unsigned char *p = mem->base;
  *pdst = (char*)p;

  char *inbuf = (char*)src;
  size_t inbytesleft = nr;
  char *outbuf = (char*)p;
  size_t outbytesleft = sz;
  size_t r = iconv(cnv, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
  iconv(cnv, NULL, NULL, NULL, NULL); // back to initial state
  if (r == (size_t)-1) return (size_t)-1;
  OA_NIY(outbytesleft > 2);
  outbuf[0] = '\0';
  outbuf[1] = '\0';
  return sz - outbytesleft;
}
