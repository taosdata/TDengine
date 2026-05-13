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

#ifndef _errs_h_
#define _errs_h_

#include "macros.h"
#include "typedefs.h"

#include <sqlext.h>

EXTERN_C_BEGIN

void errs_init(errs_t *errs) FA_HIDDEN;
void errs_append_x(errs_t *errs, const char *file, int line, const char *func, const char *sql_state, int e, const char *estr) FA_HIDDEN;
void errs_clr_x(errs_t *errs) FA_HIDDEN;
void errs_release_x(errs_t *errs) FA_HIDDEN;

SQLRETURN errs_get_diag_rec_x(
    errs_t         *errs,
    SQLSMALLINT     RecNumber,
    SQLCHAR        *SQLState,
    SQLINTEGER     *NativeErrorPtr,
    SQLCHAR        *MessageText,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *TextLengthPtr) FA_HIDDEN;

SQLRETURN errs_get_diag_field_sqlstate_x(
    errs_t         *errs,
    SQLSMALLINT     RecNumber,
    SQLSMALLINT     DiagIdentifier,
    SQLPOINTER      DiagInfoPtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr) FA_HIDDEN;

SQLRETURN errs_get_diag_field_class_origin_x(
    errs_t         *errs,
    SQLSMALLINT     RecNumber,
    SQLSMALLINT     DiagIdentifier,
    SQLPOINTER      DiagInfoPtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr) FA_HIDDEN;

SQLRETURN errs_get_diag_field_subclass_origin_x(
    errs_t         *errs,
    SQLSMALLINT     RecNumber,
    SQLSMALLINT     DiagIdentifier,
    SQLPOINTER      DiagInfoPtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr) FA_HIDDEN;

#define errs_append(_errs, _sql_state, _e, _estr) errs_append_x(_errs, __FILE__, __LINE__, __func__, _sql_state, _e, _estr)

#define errs_append_format(_errs, _sql_state, _e, _fmt, ...)                  \
  do {                                                                        \
    char _buf[4096];                                                          \
    snprintf(_buf, sizeof(_buf), "" _fmt "", ##__VA_ARGS__);                  \
    errs_append(_errs, _sql_state, _e, _buf);                                 \
  } while(0)

#define errs_oom(_errs) errs_append(_errs, "HY001", 0, "Memory allocation error")
#define errs_niy(_errs) errs_append(_errs, "HY000", 0, "General error:Not implemented yet")

#define errs_clr(_errs) errs_clr_x(_errs)

#define errs_release(_errs) errs_release_x(_errs)

#define errs_get_diag_rec(_errs, _RecNumber, _SQLSTATE, _NativeErrorPtr, _MessageText, _BufferLength, _TextLengthPtr) \
  errs_get_diag_rec_x(_errs, _RecNumber, _SQLSTATE, _NativeErrorPtr, _MessageText, _BufferLength, _TextLengthPtr)

#define errs_get_diag_field_sqlstate(_errs, _RecNumber, _DiagIdentifier, _DiagInfoPtr, _BufferLength, _StringLengthPtr) \
  errs_get_diag_field_sqlstate_x(_errs, _RecNumber, _DiagIdentifier, _DiagInfoPtr, _BufferLength, _StringLengthPtr)

#define errs_get_diag_field_class_origin(_errs, _RecNumber, _DiagIdentifier, _DiagInfoPtr, _BufferLength, _StringLengthPtr) \
  errs_get_diag_field_class_origin_x(_errs, _RecNumber, _DiagIdentifier, _DiagInfoPtr, _BufferLength, _StringLengthPtr)

#define errs_get_diag_field_subclass_origin(_errs, _RecNumber, _DiagIdentifier, _DiagInfoPtr, _BufferLength, _StringLengthPtr) \
  errs_get_diag_field_subclass_origin_x(_errs, _RecNumber, _DiagIdentifier, _DiagInfoPtr, _BufferLength, _StringLengthPtr)

EXTERN_C_END

#endif // _errs_h_

