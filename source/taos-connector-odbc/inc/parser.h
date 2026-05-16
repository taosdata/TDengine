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

#ifndef _parser_h_
#define _parser_h_

#include "macros.h"

#include <stdint.h>
#include <stdio.h>

EXTERN_C_BEGIN

typedef struct parser_ctx_s             parser_ctx_t;
typedef struct parser_token_s           parser_token_t;
typedef struct parser_loc_s             parser_loc_t;

struct parser_token_s {
  const char      *text;
  size_t           leng;
};

struct parser_loc_s {
  int first_line;
  int first_column;
  int last_line;
  int last_column;

  int prev;
  int pres;
};

struct parser_ctx_s {
  parser_loc_t           bad_token;       // NOTE: unknown or unexpected
  char                   err_msg[1024];

  const char            *input; // NOTE: no owner ship
  size_t                 len;
  // globally 0-based
  size_t                 prev;
  size_t                 pres;

  unsigned int           debug_flex:1;
  unsigned int           debug_bison:1;
  unsigned int           oom:1;
};

int parser_ylogv(const char *file, int line, const char *func,
    void *arg,
    parser_ctx_t *ctx,
    parser_loc_t *yylloc,
    const char *fmt,
    ...) __attribute__ ((format (printf, 7, 8))) FA_HIDDEN;

# define YYLLOC_DEFAULT(Cur, Rhs, N) do {                     \
  if (N) {                                                    \
    (Cur).first_line   = YYRHSLOC(Rhs, 1).first_line;         \
    (Cur).first_column = YYRHSLOC(Rhs, 1).first_column;       \
    (Cur).last_line    = YYRHSLOC(Rhs, N).last_line;          \
    (Cur).last_column  = YYRHSLOC(Rhs, N).last_column;        \
    (Cur).prev         = YYRHSLOC(Rhs, 1).prev;               \
    (Cur).pres         = YYRHSLOC(Rhs, N).pres;               \
  } else {                                                    \
    (Cur).first_line   = (Cur).last_line   =                  \
      YYRHSLOC(Rhs, 0).last_line;                             \
    (Cur).first_column = (Cur).last_column =                  \
      YYRHSLOC(Rhs, 0).last_column;                           \
    (Cur).prev         = (Cur).pres        =                  \
      YYRHSLOC(Rhs, 0).pres;                                  \
  }                                                           \
} while (0)

#define LOG_ARGS const char *file, int line, const char *func, yyscan_t arg, parser_ctx_t *ctx, YYLTYPE *yylloc
#define LOG_VALS file, line, func, arg, ctx
#define LOG_MALS __FILE__, __LINE__, __func__, arg, (param ? &param->ctx : NULL)
#define LOG_FLF __FILE__, __LINE__, __func__

#ifdef _WIN32               /* { */
#define YLOG(args, yylloc, fmt, ...) \
  (0 ? fprintf(stderr, fmt, ##__VA_ARGS__) : parser_ylogv(args, yylloc, fmt, ##__VA_ARGS__))
#else                       /* }{ */
#define YLOG parser_ylogv
#endif                      /* } */

void parser_yyerror(
    const char *file, int line, const char *func,
    parser_loc_t *yylloc,
    void *arg,
    parser_ctx_t *ctx,
    const char *errmsg) FA_HIDDEN;

EXTERN_C_END

#endif // _parser_h_

