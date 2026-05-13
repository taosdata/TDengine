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

#include "errs.h"
#include "log.h"

void errs_init(errs_t *errs)
{
  INIT_TOD_LIST_HEAD(&errs->errs);
  INIT_TOD_LIST_HEAD(&errs->frees);
  errs->count = 0;
}

static void err_set_x(err_t *err, const char *file, int line, const char *func, const char *data_source, const char *sql_state, int e, const char *estr)
{
  const char *vendor = "freemine@yeah.net,yjshe@taosdata.com";
  const char *odbc_component = "TDengine ODBC Driver";
  err->err = e;
  char buf[PATH_MAX + 1];
  char *p = tod_basename(file, buf, sizeof(buf));
  snprintf(err->buf, sizeof(err->buf),
      "[%s][%s][%s]%s[%d]:%s(): %s",
      vendor, odbc_component, data_source,
      p, line, func,
      estr);
  err->estr = err->buf;
  tod_strncpy((char*)err->sql_state, sql_state, sizeof(err->sql_state));
}

void errs_append_x(errs_t *errs, const char *file, int line, const char *func, const char *sql_state, int e, const char *estr)
{
  conn_t *conn = errs->connected_conn;
  const char *data_source = conn ? conn_data_source(conn) : "";

  if (tod_list_empty(&errs->frees)) {
    err_t *err = (err_t*)calloc(1, sizeof(*err));
    if (!err) {
      OD("out of memory");
      return;
    }

    err->err           = 0;
    err->estr          = "";
    err->sql_state[0]  = '\0';
    err->buf[0]        = '\0';

    tod_list_add_tail(&err->node, &errs->frees);
  }

  err_t *err = NULL;
  tod_list_first_entry_or_null(err, &errs->frees, err_t, node);
  tod_list_del(&err->node);
  err_set_x(err, file, line, func, data_source, sql_state, e, estr);
  tod_list_add_tail(&err->node, &errs->errs);
  errs->count += 1;
}

void errs_clr_x(errs_t *errs)
{
  if (tod_list_empty(&errs->errs)) return;

  err_t *p, *n;
  tod_list_for_each_entry_safe(p, n, &errs->errs, err_t, node) {
    tod_list_del(&p->node);
    tod_list_add_tail(&p->node, &errs->frees);
  }

  errs->count = 0;
}

void errs_release_x(errs_t *errs)
{
  err_t *p, *n;
  tod_list_for_each_entry_safe(p, n, &errs->errs, err_t, node) {
    tod_list_del(&p->node);
    free(p);
  }

  tod_list_for_each_entry_safe(p, n, &errs->frees, err_t, node) {
    tod_list_del(&p->node);
    free(p);
  }

  errs->count = 0;
  errs->connected_conn = NULL;
}

SQLRETURN errs_get_diag_rec_x(
    errs_t         *errs,
    SQLSMALLINT     RecNumber,
    SQLCHAR        *SQLState,
    SQLINTEGER     *NativeErrorPtr,
    SQLCHAR        *MessageText,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *TextLengthPtr)
{
  if (RecNumber == 0) return SQL_NO_DATA;
  if (tod_list_empty(&errs->errs)) return SQL_NO_DATA;

  int i = 1;

  int found = 0;
  err_t *p = NULL;
  tod_list_for_each_entry(p, &errs->errs, err_t, node) {
    if (i == RecNumber) {
      found = 1;
      break;
    }
    ++i;
  }

  if (!found) return SQL_NO_DATA;

  if (NativeErrorPtr) *NativeErrorPtr = p->err;
  if (SQLState) strncpy((char*)SQLState, (const char*)p->sql_state, 6);
  int n = snprintf((char*)MessageText, BufferLength, "%s", p->estr);
  if (TextLengthPtr) *TextLengthPtr = n;

  return SQL_SUCCESS;
}

SQLRETURN errs_get_diag_field_sqlstate_x(
    errs_t         *errs,
    SQLSMALLINT     RecNumber,
    SQLSMALLINT     DiagIdentifier,
    SQLPOINTER      DiagInfoPtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  (void)DiagIdentifier;
  if (RecNumber == 0) return SQL_NO_DATA;
  if (tod_list_empty(&errs->errs)) return SQL_NO_DATA;

  int i = 1;

  int found = 0;
  err_t *p = NULL;
  tod_list_for_each_entry(p, &errs->errs, err_t, node) {
    if (i == RecNumber) {
      found = 1;
      break;
    }
    ++i;
  }

  if (!found) return SQL_NO_DATA;

  SQLCHAR *SQLState = (SQLCHAR*)DiagInfoPtr;
  int n = snprintf((char*)SQLState, BufferLength, "%.*s", 6, (const char*)p->sql_state);
  if (StringLengthPtr) *StringLengthPtr = n;

  return SQL_SUCCESS;
}

SQLRETURN errs_get_diag_field_class_origin_x(
    errs_t         *errs,
    SQLSMALLINT     RecNumber,
    SQLSMALLINT     DiagIdentifier,
    SQLPOINTER      DiagInfoPtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  (void)DiagIdentifier;
  if (RecNumber == 0) return SQL_NO_DATA;
  if (tod_list_empty(&errs->errs)) return SQL_NO_DATA;

  int i = 1;

  int found = 0;
  err_t *p = NULL;
  tod_list_for_each_entry(p, &errs->errs, err_t, node) {
    if (i == RecNumber) {
      found = 1;
      break;
    }
    ++i;
  }

  if (!found) return SQL_NO_DATA;

  SQLCHAR *ClassOrigin = (SQLCHAR*)DiagInfoPtr;
  const char *class_origin = "ISO 9075";
  if (p->sql_state[0] == 'I' && p->sql_state[1] == 'M') { 
    class_origin = "ODBC 3.0";
  }
  int n = snprintf((char*)ClassOrigin, BufferLength, "%s", class_origin);
  if (StringLengthPtr) *StringLengthPtr = n;

  return SQL_SUCCESS;
}

SQLRETURN errs_get_diag_field_subclass_origin_x(
    errs_t         *errs,
    SQLSMALLINT     RecNumber,
    SQLSMALLINT     DiagIdentifier,
    SQLPOINTER      DiagInfoPtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  // TODO:
  return errs_get_diag_field_class_origin_x(errs, RecNumber, DiagIdentifier, DiagInfoPtr, BufferLength, StringLengthPtr);
}


