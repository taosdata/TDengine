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

#ifndef _ts_parser_h_
#define _ts_parser_h_

#include "macros.h"
#include "typedefs.h"

#include <stddef.h>
#include <stdint.h>

EXTERN_C_BEGIN

void ts_parser_param_release(ts_parser_param_t *param) FA_HIDDEN;

// support ISO-8601 and RFC-3339, referenced by `man date`
// tz_default: used when timezone field not found in `input`
//             eg.: 28800 for Beijing as +0800/+08:00 == [8 * 60 * 60 + 0 * 60]
int ts_parser_parse(const char *input, size_t len, ts_parser_param_t *param,
    int64_t tz_default) FA_HIDDEN;

EXTERN_C_END

#endif // _ts_parser_h_

