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

#ifndef _charset_h_
#define _charset_h_

#include "macros.h"
#include "typedefs.h"

#include "iconv_wrapper.h"

EXTERN_C_BEGIN

void charset_conv_release(charset_conv_t *cnv) FA_HIDDEN;
int charset_conv_reset(charset_conv_t *cnv, const char *from, const char *to) FA_HIDDEN;
iconv_t charset_conv_get(charset_conv_t *cnv) FA_HIDDEN;

void charset_conv_mgr_release(charset_conv_mgr_t *mgr) FA_HIDDEN;
charset_conv_t* charset_conv_mgr_get_charset_conv(charset_conv_mgr_t *mgr, const char *fromcode, const char *tocode) FA_HIDDEN;

size_t iconv_x(const char *file, int line, const char *func,
    iconv_t cd, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft) FA_HIDDEN;

#define CALL_iconv(...)   iconv_x(__FILE__, __LINE__, __func__, ##__VA_ARGS__)

EXTERN_C_END

#endif //  _charset_h_

