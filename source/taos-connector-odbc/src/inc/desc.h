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

#ifndef _desc_h_
#define _desc_h_

#include "macros.h"
#include "typedefs.h"

EXTERN_C_BEGIN

desc_t* desc_create(conn_t *conn) FA_HIDDEN;
desc_t* desc_ref(desc_t *desc) FA_HIDDEN;
desc_t* desc_unref(desc_t *desc) FA_HIDDEN;

SQLRETURN desc_free(desc_t *desc) FA_HIDDEN;

void descriptor_init(descriptor_t *descriptor) FA_HIDDEN;
void descriptor_release(descriptor_t *descriptor) FA_HIDDEN;

void descriptor_reclaim_buffers(descriptor_t *APD) FA_HIDDEN;

SQLRETURN descriptor_keep(descriptor_t *descriptor, stmt_t *owner, size_t nr) FA_HIDDEN;

SQLRETURN descriptor_bind_col(descriptor_t *ARD,
    stmt_t        *owner,
    SQLUSMALLINT   ColumnNumber,
    SQLSMALLINT    TargetType,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr) FA_HIDDEN;

void desc_clr_errs(desc_t *desc) FA_HIDDEN;

SQLRETURN desc_get_diag_rec(
    desc_t         *desc,
    SQLSMALLINT     RecNumber,
    SQLCHAR        *SQLState,
    SQLINTEGER     *NativeErrorPtr,
    SQLCHAR        *MessageText,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *TextLengthPtr) FA_HIDDEN;

SQLRETURN desc_get_diag_field(
    desc_t         *desc,
    SQLSMALLINT     RecNumber,
    SQLSMALLINT     DiagIdentifier,
    SQLPOINTER      DiagInfoPtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr) FA_HIDDEN;

#if (ODBCVER >= 0x0300)       /* { */
SQLRETURN desc_copy(
    desc_t *src,
    desc_t *tgt) FA_HIDDEN;

SQLRETURN desc_get_field(
    desc_t         *desc,
    SQLSMALLINT RecNumber, SQLSMALLINT FieldIdentifier,
    SQLPOINTER Value, SQLINTEGER BufferLength,
    SQLINTEGER *StringLength) FA_HIDDEN;

SQLRETURN desc_get_rec(
    desc_t         *desc,
    SQLSMALLINT RecNumber, SQLCHAR *Name,
    SQLSMALLINT BufferLength, SQLSMALLINT *StringLengthPtr,
    SQLSMALLINT *TypePtr, SQLSMALLINT *SubTypePtr,
    SQLLEN     *LengthPtr, SQLSMALLINT *PrecisionPtr,
    SQLSMALLINT *ScalePtr, SQLSMALLINT *NullablePtr) FA_HIDDEN;

SQLRETURN desc_set_field(
    desc_t         *desc,
    SQLSMALLINT RecNumber, SQLSMALLINT FieldIdentifier,
    SQLPOINTER Value, SQLINTEGER BufferLength) FA_HIDDEN;

SQLRETURN desc_set_rec(
    desc_t         *desc,
    SQLSMALLINT RecNumber, SQLSMALLINT Type,
    SQLSMALLINT SubType, SQLLEN Length,
    SQLSMALLINT Precision, SQLSMALLINT Scale,
    SQLPOINTER Data, SQLLEN *StringLength,
    SQLLEN *Indicator) FA_HIDDEN;
#endif                        /* }*/

EXTERN_C_END

#endif //  _desc_h_

