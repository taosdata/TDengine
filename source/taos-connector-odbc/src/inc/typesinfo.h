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

#ifndef _typesinfo_h_
#define _typesinfo_h_

#include "macros.h"
#include "typedefs.h"

EXTERN_C_BEGIN

void typesinfo_reset(typesinfo_t *typesinfo) FA_HIDDEN;
void typesinfo_release(typesinfo_t *typesinfo) FA_HIDDEN;

void typesinfo_init(typesinfo_t *typesinfo, stmt_t *stmt) FA_HIDDEN;

SQLRETURN typesinfo_open(
    typesinfo_t     *typesinfo,
    SQLSMALLINT      DataType) FA_HIDDEN;

EXTERN_C_END

#endif //  _typesinfo_h_
