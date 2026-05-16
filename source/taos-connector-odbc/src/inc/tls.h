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
 * OUT OF OR IN tlsECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#ifndef _tls_h_
#define _tls_h_

#include "macros.h"
#include "typedefs.h"

#include "utils.h"

EXTERN_C_BEGIN

size_t tls_size(void) FA_HIDDEN;
tls_t* tls_get(void) FA_HIDDEN;
void tls_release(tls_t *tls) FA_HIDDEN;
mem_t* tls_get_mem_intermediate(void) FA_HIDDEN;
static inline int tls_iconv(const char *fromcode, const char *tocode, const char *src, size_t len)
{
    mem_t *mem = tls_get_mem_intermediate();
    if (!mem) return -1;
    return mem_iconv(mem, fromcode, tocode, src, len);
}

charset_conv_t* tls_get_charset_conv(const char *fromcode, const char *tocode) FA_HIDDEN;


// debug leakage only
int tls_leakage_potential(void) FA_HIDDEN;

EXTERN_C_END

#endif //  _tls_h_


