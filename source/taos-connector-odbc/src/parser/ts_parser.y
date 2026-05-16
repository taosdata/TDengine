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

%code top {
}

%code top {
    // here to include header files required for generated code
}

%code requires {
    #define YYSTYPE       TS_PARSER_YYSTYPE
    #define YYLTYPE       TS_PARSER_YYLTYPE
    #ifndef YY_TYPEDEF_YY_SCANNER_T
    #define YY_TYPEDEF_YY_SCANNER_T
    typedef void* yyscan_t;
    #endif
}

%code provides {
}

%code {
    // generated header from flex
    // introduce yylex decl for later use
    static void yyerror(
        YYLTYPE *yylloc,                   // match %define locations
        yyscan_t arg,                      // match %param
        ts_parser_param_t *param,          // match %parse-param
        const char *errsg
    );

    static int digits_to_ull(const char *s, size_t len, unsigned long long *v)
    {
      char *end = NULL;
      *v = strtoull(s, &end, 10);
      if (s + len != end) return -1;
      return 0;
    }

    #define SET_DT(_loc, _fmt) do {                                            \
      const char *next;                                                        \
      size_t _f = _loc.first_column;                                           \
      size_t _l = _loc.last_column;                                            \
      const char *_dt = param->ctx.input + _f;                                 \
      size_t _n       = _l - _f;                                               \
      struct tm tm = {0};                                                      \
      next = tod_strptime_with_len(_dt, _n, _fmt, &tm);                        \
      if (next == NULL) {                                                      \
        YLOG(LOG_MALS, &_loc, "bad datetime:[%.*s]", (int)_n, _dt);            \
        YYABORT;                                                               \
      }                                                                        \
      param->tm_utc0 = mktime(&tm) + tod_get_local_timezone();                 \
    } while (0)

    #define SET_FRAC_NANO(_loc) do {                                           \
      size_t _f = _loc.first_column;                                           \
      size_t _l = _loc.last_column;                                            \
      size_t _n = _l - _f;                                                     \
      const char *_nano  = param->ctx.input + _f;                              \
      if (_n > 9 || digits_to_ull(_nano, _n, &param->frac_nano)) {             \
        YLOG(LOG_MALS, &_loc, "bad timefraction:[.%.*s]", (int)_n, _nano);     \
        YYABORT;                                                               \
      }                                                                        \
      param->decimal_digits = (uint8_t)_n;                                     \
      while (_n++<9)                                                           \
        param->frac_nano *= 10;                                                \
    } while (0)

    #define SET_TZ(_loc, _sign) do {                                           \
      size_t _f = _loc.first_column;                                           \
      size_t _l = _loc.last_column;                                            \
      size_t _n = _l - _f;                                                     \
      const char *_tz = param->ctx.input + _f;                                 \
      unsigned long long _v;                                                   \
      if (_n != 4 || digits_to_ull(_tz, _n, &_v)) {                            \
        YLOG(LOG_MALS, &_loc, "bad timezone:[.%.*s]", (int)_n, _tz);           \
        YYABORT;                                                               \
      }                                                                        \
      unsigned long long _h = _v / 100;                                        \
      unsigned long long _m = _v % 100;                                        \
      if (_h >= 24 || _m >= 60) {                                              \
        YLOG(LOG_MALS, &_loc, "bad timezone:[.%.*s]", (int)_n, _tz);           \
        YYABORT;                                                               \
      }                                                                        \
      param->tz_seconds  = _h * 3600 + _m * 60;                                \
      param->tz_seconds *= _sign;                                              \
    } while (0)

    #define SET_TZ2(_loc1, _loc2, _sign) do {                                  \
      size_t _f = _loc1.first_column;                                          \
      size_t _l = _loc1.last_column;                                           \
      size_t _n = _l - _f;                                                     \
      const char *_tz = param->ctx.input + _f;                                 \
      unsigned long long _v;                                                   \
      if (_n != 2 || digits_to_ull(_tz, _n, &_v) || _v >= 24) {                \
        YLOG(LOG_MALS, &_loc1, "bad timezone:[.%.*s]", (int)_n, _tz);          \
        YYABORT;                                                               \
      }                                                                        \
      param->tz_seconds = _v * 3600;                                           \
      _f = _loc2.first_column;                                                 \
      _l = _loc2.last_column;                                                  \
      _n = _l - _f;                                                            \
      _tz = param->ctx.input + _f;                                             \
      if (_n != 2 || digits_to_ull(_tz, _n, &_v) || _v >= 60) {                \
        YLOG(LOG_MALS, &_loc2, "bad timezone:[.%.*s]", (int)_n, _tz);          \
        YYABORT;                                                               \
      }                                                                        \
      param->tz_seconds += _v * 60;                                            \
      param->tz_seconds *= _sign;                                              \
    } while (0)

    #define POST_SET() do {                                                    \
      param->tm_utc0 -= param->tz_seconds;                                     \
    } while (0)
}

/* Bison declarations. */
%require "3.0.4"
%define api.pure full
%define api.token.prefix {TOK_}
%define locations
%define api.location.type {parser_loc_t}
%define parse.error verbose
%define parse.lac full
%define parse.trace true
%defines
%verbose

%param { yyscan_t arg }
%parse-param { ts_parser_param_t *param }

// union members
%union { parser_token_t token; }
%union { char c; }

%token <token> DIGITS

 /* %nterm <str>   args */ // non-terminal `input` use `str` to store
                           // token value as well
 /* %destructor { free($$); } <str> */

%% /* The grammar follows. */

input:
  %empty   { YYABORT; }
| iso_8601 { POST_SET(); }
| rfc_3339 { POST_SET(); }
;

iso_8601:
  iso_8601_dt
| iso_8601_dt fraction
| iso_8601_dt fraction tz
| iso_8601_dt tz
;

rfc_3339:
  rfc_3339_dt
| rfc_3339_dt fraction
| rfc_3339_dt fraction tz
| rfc_3339_dt tz
;

iso_8601_dt:
  iso_8601_dt_T { SET_DT(@1, "%Y-%m-%dT%H:%M:%S"); }
;

iso_8601_dt_T:
  date 'T' time
;

rfc_3339_dt:
  rfc_3339_dt_  { SET_DT(@1, "%Y-%m-%d %H:%M:%S"); }
;

rfc_3339_dt_:
  date ' ' time
;

date:
  DIGITS '-' DIGITS '-' DIGITS
;

time:
  DIGITS ':' DIGITS ':' DIGITS
;

fraction:
  '.' DIGITS    { SET_FRAC_NANO(@2); }
;

tz:
  '+' DIGITS                 { SET_TZ(@2, 1); }
| '-' DIGITS                 { SET_TZ(@2, -1); }
| '+' DIGITS ':' DIGITS      { SET_TZ2(@2, @4, 1); }
| '-' DIGITS ':' DIGITS      { SET_TZ2(@2, @4, -1); }
| 'Z'                        { param->tz_seconds = 0; }
;

%%

/* Called by yyparse on error. */
static void yyerror(
    YYLTYPE *yylloc,                   // match %define locations
    yyscan_t arg,                      // match %param
    ts_parser_param_t *param,         // match %parse-param
    const char *errmsg
)
{
  parser_ctx_t *ctx = param ? &param->ctx : NULL;
  parser_yyerror(__FILE__, __LINE__, __func__, yylloc, arg, ctx, errmsg);
}

int ts_parser_parse(const char *input, size_t len, ts_parser_param_t *param,
    int64_t tz_default)
{
  yyscan_t arg = {0};
  yylex_init(&arg);
  // yyset_in(in, arg);
  int debug_flex = param ? param->ctx.debug_flex : 0;
  int debug_bison = param ? param->ctx.debug_bison: 0;
  yyset_debug(debug_flex, arg);
  yydebug = debug_bison;
  yyset_extra(&param->ctx, arg);
  param->ctx.input = input;
  param->ctx.len   = len;
  param->ctx.prev  = 0;
  param->ctx.pres  = 0;

  param->tz_seconds = tz_default;

  yy_scan_bytes(input ? input : "", input ? (int)len : 0, arg);
  int ret =yyparse(arg, param);
  yylex_destroy(arg);
  return ret ? -1 : 0;
}

