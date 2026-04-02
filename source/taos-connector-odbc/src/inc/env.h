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

#ifndef _env_h_
#define _env_h_

#include "macros.h"

#include "logger.h"
#include "utils.h"

#include "typedefs.h"

#include <sqlext.h>
#include "iconv_wrapper.h"

EXTERN_C_BEGIN

env_t* env_create(void) FA_HIDDEN;
env_t* env_ref(env_t *env) FA_HIDDEN;
env_t* env_unref(env_t *env) FA_HIDDEN;
SQLRETURN env_free(env_t *env) FA_HIDDEN;

void env_clr_errs(env_t *env) FA_HIDDEN;

int env_get_debug(env_t *env) FA_HIDDEN;
int env_get_debug_flex(env_t *env) FA_HIDDEN;
int env_get_debug_bison(env_t *env) FA_HIDDEN;

SQLRETURN env_get_diag_rec(
    env_t          *env,
    SQLSMALLINT     RecNumber,
    SQLCHAR        *SQLState,
    SQLINTEGER     *NativeErrorPtr,
    SQLCHAR        *MessageText,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *TextLengthPtr) FA_HIDDEN;

SQLRETURN env_set_attr(
    env_t       *env,
    SQLINTEGER   Attribute,
    SQLPOINTER   ValuePtr,
    SQLINTEGER   StringLength) FA_HIDDEN;

SQLRETURN env_get_attr(
    env_t       *env,
    SQLINTEGER Attribute,
    SQLPOINTER Value,
    SQLINTEGER BufferLength,
    SQLINTEGER *StringLength) FA_HIDDEN;

SQLRETURN env_end_tran(env_t *env, SQLSMALLINT CompletionType) FA_HIDDEN;

SQLRETURN env_alloc_conn(env_t *env, SQLHANDLE *OutputHandle) FA_HIDDEN;

SQLRETURN env_get_diag_field(
    env_t          *env,
    SQLSMALLINT     RecNumber,
    SQLSMALLINT     DiagIdentifier,
    SQLPOINTER      DiagInfoPtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr) FA_HIDDEN;

size_t env_conv(env_t *env, iconv_t cnv, mem_t *mem, const char *src, char **pdst) FA_HIDDEN;

int tod_get_debug_flex(void) FA_HIDDEN;
int tod_get_debug_bison(void) FA_HIDDEN;

const char* tod_get_sqlc_charset(void) FA_HIDDEN;
const char* tod_get_locale_or_ACP(void) FA_HIDDEN;

const char* tod_get_image_name(void) FA_HIDDEN;

EXTERN_C_END

#endif // _env_h_

