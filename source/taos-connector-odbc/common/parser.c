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

#include "parser.h"

#include "os_port.h"

#include <stdarg.h>

void parser_yyerror(
    const char *file, int line, const char *func,
    parser_loc_t *yylloc,
    void *arg,
    parser_ctx_t *ctx,
    const char *errmsg)
{
  (void)arg;

  char bn[512]; bn[0] = '\0';
  const char *fn = tod_basename(file, bn, sizeof(bn));
  if (!ctx) {
    fprintf(stderr, "%s[%d]:%s():(%d,%d)->(%d,%d):%s\n",
        fn, line, func,
        yylloc->first_line, yylloc->first_column,
        yylloc->last_line, yylloc->last_column,
        errmsg);

    return;
  }

  ctx->bad_token = *yylloc;
  ctx->err_msg[0] = '\0';
  snprintf(ctx->err_msg, sizeof(ctx->err_msg), "%s[%d]:%s():near `%.*s`:%s",
      fn, line, func,
      (int)(yylloc->pres + 10 - yylloc->prev), ctx->input + yylloc->prev,
      errmsg);
}

int parser_ylogv(const char *file, int line, const char *func,
    void *arg,
    parser_ctx_t *ctx,
    parser_loc_t *yylloc,
    const char *fmt,
    ...)
{
  char buf[4096]; buf[0] = '\0';
  va_list ap;
  va_start(ap, fmt);
  int n = vsnprintf(buf, sizeof(buf), fmt, ap);
  va_end(ap);

  parser_yyerror(file, line, func, yylloc, arg, ctx, buf);

  return n;
}

