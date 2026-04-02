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
    #define YYSTYPE       URL_PARSER_YYSTYPE
    #define YYLTYPE       URL_PARSER_YYLTYPE
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
        url_parser_param_t *param,         // match %parse-param
        const char *errsg
    );

    #define SET_SCHEME(_v, _loc) do {                                                           \
      char *s = strndup(_v.text, _v.leng);                                                      \
      if (!s) {                                                                                 \
        YLOG(LOG_MALS, &_loc, "runtime error:out of memory");                                   \
        YYABORT;                                                                                \
      }                                                                                         \
      param->url.scheme = s;                                                                    \
    } while (0)

    #define SET_USERINFO(_v, _loc) {                                                            \
      TOD_SAFE_FREE(param->url.user);                                                           \
      TOD_SAFE_FREE(param->url.pass);                                                           \
      for (size_t i=0; i<_v.leng; ++i) {                                                        \
        if (_v.text[i] == ':') {                                                                \
          param->url.user = url_decode(_v.text, i);                                             \
          param->url.pass = url_decode(_v.text + i + 1,  _v.leng - i - 1);                      \
          if (!param->url.user || !param->url.pass) {                                           \
            YLOG(LOG_MALS, &_loc, "runtime error:out of memory");                               \
            YYABORT;                                                                            \
          }                                                                                     \
          break;                                                                                \
        }                                                                                       \
      }                                                                                         \
    } while (0)

    #define SET_PORT(_v, _loc) do {                                                             \
      char buf[1024]; buf[0] = '\0';                                                            \
      int n = snprintf(buf, sizeof(buf), "%.*s", (int)_v.leng, _v.text);                        \
      if (n <= 0) {                                                                             \
        YLOG(LOG_MALS, &_loc, "runtime error:snprintf failed");                                 \
        YYABORT;                                                                                \
      }                                                                                         \
      if ((size_t)n >= sizeof(buf)) {                                                           \
        YLOG(LOG_MALS, &_loc, "runtime error:buffer too small");                                \
        YYABORT;                                                                                \
      }                                                                                         \
      int nn = 0;                                                                               \
      int port = 0;                                                                             \
      n = sscanf(buf, "%d%n", &port, &nn);                                                      \
      if (n != 1 || (size_t)nn != strlen(buf) || port < 0 || port >UINT16_MAX) {                \
        YLOG(LOG_MALS, &_loc, "runtime error:not valid port");                                  \
        YYABORT;                                                                                \
      }                                                                                         \
      param->url.port = (uint16_t)port;                                                         \
    } while (0)

    #define SET_HOST(_v, _loc) do {                                                             \
      TOD_SAFE_FREE(param->url.host);                                                           \
      param->url.host = strndup(_v.text, _v.leng);                                              \
      if (!param->url.host) {                                                                   \
        YLOG(LOG_MALS, &_loc, "runtime error:out of memory");                                   \
        YYABORT;                                                                                \
      }                                                                                         \
    } while (0)

    #define SET_HOST_BY_REG_NAME(_v, _loc) do {                                                 \
      TOD_SAFE_FREE(param->url.host);                                                           \
      param->url.host = url_decode(_v.text, _v.leng);                                           \
      if (!param->url.host) {                                                                   \
        YLOG(LOG_MALS, &_loc, "runtime error:out of memory");                                   \
        YYABORT;                                                                                \
      }                                                                                         \
    } while (0)

    #define SET_PATH_BY_STRS(_strs, _loc) do {                                                  \
      TOD_SAFE_FREE(param->url.path);                                                           \
      char *s = url_strs_join(&_strs);                                                          \
      url_strs_release(&_strs);                                                                 \
      if (!s) {                                                                                 \
        YLOG(LOG_MALS, &_loc, "runtime error:out of memory");                                   \
        YYABORT;                                                                                \
      }                                                                                         \
      param->url.path = s;                                                                      \
    } while (0)

    #define SET_PATH_BY_STR(_str, _loc) do {                                                    \
      TOD_SAFE_FREE(param->url.path);                                                           \
      char *s = strndup(_str.str, _str.nr);                                                     \
      url_str_release(&_str);                                                                   \
      if (!s) {                                                                                 \
        YLOG(LOG_MALS, &_loc, "runtime error:out of memory");                                   \
        YYABORT;                                                                                \
      }                                                                                         \
      param->url.path = s;                                                                      \
    } while (0)

    #define SET_PATH_BY_STR_STRS(_str, _strs, _loc) do {                                        \
      TOD_SAFE_FREE(param->url.path);                                                           \
      char *s = url_strs_join_with_head(&_strs, _str.str, _str.nr);                             \
      url_str_release(&_str);                                                                   \
      url_strs_release(&_strs);                                                                 \
      if (!s) {                                                                                 \
        YLOG(LOG_MALS, &_loc, "runtime error:out of memory");                                   \
        YYABORT;                                                                                \
      }                                                                                         \
      param->url.path = s;                                                                      \
    } while (0)

    #define SET_PATH_BY_ROOT(_loc) do {                                                         \
      TOD_SAFE_FREE(param->url.path);                                                           \
      char *s = strdup("/");                                                                    \
      if (!s) {                                                                                 \
        YLOG(LOG_MALS, &_loc, "runtime error:out of memory");                                   \
        YYABORT;                                                                                \
      }                                                                                         \
      param->url.path = s;                                                                      \
    } while (0)

    #define STR_SET(_str, _v, _loc) do {                                                        \
      memset(&_str, 0, sizeof(_str));                                                           \
      int r = url_str_set(&_str, _v.text, _v.leng);                                             \
      if (r) {                                                                                  \
        YLOG(LOG_MALS, &_loc, "runtime error:out of memory");                                   \
        YYABORT;                                                                                \
      }                                                                                         \
    } while (0)

    #define STR_SET_BY_PCT(_str, _v, _loc) do {                                                 \
      memset(&_str, 0, sizeof(_str));                                                           \
      int r = url_str_set_by_pct(&_str, _v.text, _v.leng);                                      \
      if (r) {                                                                                  \
        YLOG(LOG_MALS, &_loc, "runtime error:out of memory");                                   \
        YYABORT;                                                                                \
      }                                                                                         \
    } while (0)

    #define STR_APPEND(_r, _str, _v, _loc) do {                                                 \
      int r = url_str_append(&_str, _v.text, _v.leng);                                          \
      if (r) {                                                                                  \
        url_str_release(&_str);                                                                 \
        YLOG(LOG_MALS, &_loc, "runtime error:out of memory");                                   \
        YYABORT;                                                                                \
      }                                                                                         \
      _r = _str;                                                                                \
    } while (0)

    #define STR_APPEND_PCT(_r, _str, _v, _loc) do {                                             \
      int r = url_str_append_pct(&_str, _v.text, _v.leng);                                      \
      if (r) {                                                                                  \
        url_str_release(&_str);                                                                 \
        YLOG(LOG_MALS, &_loc, "runtime error:out of memory");                                   \
        YYABORT;                                                                                \
      }                                                                                         \
      _r = _str;                                                                                \
    } while (0)

    #define STRS_APPEND(_r, _v, _s, _loc) do {                                                  \
      int r = url_strs_append(&_v, &_s);                                                        \
      url_strs_release(&_s);                                                                    \
      if (r) {                                                                                  \
        url_strs_release(&_v);                                                                  \
        YLOG(LOG_MALS, &_loc, "runtime error:out of memory");                                   \
        YYABORT;                                                                                \
      }                                                                                         \
      _r = _v;                                                                                  \
    } while (0)

    #define SET_STRS_BY_CHR_STR(_r, _c, _s, _loc) do {                                          \
      memset(&_r, 0, sizeof(_r));                                                               \
      char c = _c;                                                                              \
      int r = url_strs_append_str(&_r, &c, 1);                                                  \
      if (r == 0) {                                                                             \
        r = url_strs_append_str(&_r, _s.str, _s.nr);                                            \
      }                                                                                         \
      url_str_release(&_s);                                                                     \
      if (r) {                                                                                  \
        url_strs_release(&_r);                                                                  \
        YLOG(LOG_MALS, &_loc, "runtime error:out of memory");                                   \
        YYABORT;                                                                                \
      }                                                                                         \
    } while (0)

    #define SET_QUERY(_v, _loc) do {                                                            \
      TOD_SAFE_FREE(param->url.query);                                                          \
      param->url.query = _v.str;                                                                \
      _v.str = NULL;                                                                            \
      _v.nr = 0;                                                                                \
      _v.cap = 0;                                                                               \
    } while (0)

    #define SET_FRAGMENT(_v, _loc) do {                                                         \
      TOD_SAFE_FREE(param->url.fragment);                                                       \
      param->url.fragment = _v.str;                                                             \
      _v.str = NULL;                                                                            \
      _v.nr = 0;                                                                                \
      _v.cap = 0;                                                                               \
    } while (0)

    #define SET_QUERY_EMPTY(_loc) do {                                                          \
      TOD_SAFE_FREE(param->url.query);                                                          \
      param->url.query = strdup("");                                                            \
    } while (0)

    #define SET_FRAGMENT_EMPTY(_loc) do {                                                       \
      TOD_SAFE_FREE(param->url.fragment);                                                       \
      param->url.fragment = strdup("");                                                         \
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
%parse-param { url_parser_param_t *param }

// union members
%union { parser_token_t token; }
%union { char c; }
%union { url_str_t  str; }
%union { url_strs_t strs; }
%destructor { url_str_release(&$$); }  <str>
%destructor { url_strs_release(&$$); } <strs>

%token <token> SCHEME USERINFO PCHAR
%token FILE_SCHEME_NOT_SUPPORTED_YET
%token <token> IPV4 IPVFUTURE IPV6
%token <token> REG_NAME
%token <token> PORT

%nterm <strs> slashsegment slashpath
%nterm <str> segment
%nterm <str> pchars_slash_qm

// %nterm <nterm> dq sq aa qm sc
// %nterm <nterm> token str any_token sql quoted strs delimit url

 /* %nterm <str>   args */ // non-terminal `input` use `str` to store
                           // token value as well
 /* %destructor { free($$); } <str> */

%% /* The grammar follows. */

input:
  url                      { (void)yynerrs; }
;

url:
  scheme ':' '/' '/' auth
| scheme ':' '/' '/' auth '/'                            { SET_PATH_BY_ROOT(@6); }
| scheme ':' '/' '/' auth '/' query_fragment             { SET_PATH_BY_ROOT(@6); }
| scheme ':' '/' '/' auth slashpath                      { SET_PATH_BY_STRS($6, @6); }
| scheme ':' '/' '/' auth slashpath query_fragment       { SET_PATH_BY_STRS($6, @6); }
| scheme ':' '/' '/' auth query_fragment
| scheme ':' path
| scheme ':'
;

scheme:
  SCHEME                              { SET_SCHEME($1, @1); }
;

path:
  slashpath                           { SET_PATH_BY_STRS($1, @1); }
| slashpath query_fragment            { SET_PATH_BY_STRS($1, @1); }
| segment                             { SET_PATH_BY_STR($1, @1); }
| segment query_fragment              { SET_PATH_BY_STR($1, @1); }
| segment slashpath                   { SET_PATH_BY_STR_STRS($1, $2, @$); }
| segment slashpath query_fragment    { SET_PATH_BY_STR_STRS($1, $2, @$); }
;

slashpath:
  slashsegment                { $$ = $1; }
| slashpath slashsegment      { STRS_APPEND($$, $1, $2, @$); }
;

slashsegment:
  '/' segment                 { SET_STRS_BY_CHR_STR($$, '/', $2, @$); } 
;

segment:
  PCHAR                       { STR_SET($$, $1, @1); }
| segment PCHAR               { STR_APPEND($$, $1, $2, @$); }
;

auth:
  addr
| USERINFO '@' addr           { SET_USERINFO($1, @1); }
;

addr:
  host
| host ':' PORT               { SET_PORT($3, @3); }
;

host:
  '[' IPV6 ']'                { SET_HOST($2, @2); }
| '[' IPVFUTURE ']'           { SET_HOST($2, @2); }
| IPV4                        { SET_HOST($1, @1); }
| REG_NAME                    { SET_HOST_BY_REG_NAME($1, @1); }
;

query_fragment:
  query
| fragment
| query fragment
;

query:
  '?'                         { SET_QUERY_EMPTY(@1); }
| '?' pchars_slash_qm         { SET_QUERY($2, @2); }
;

fragment:
  '#'                         { SET_FRAGMENT_EMPTY(@1); }
| '#' pchars_slash_qm         { SET_FRAGMENT($2, @2); }
;

pchars_slash_qm:
  PCHAR                       { STR_SET($$, $1, @1); }
| pchars_slash_qm PCHAR       { STR_APPEND($$, $1, $2, @$); }
;


%%

/* Called by yyparse on error. */
static void yyerror(
    YYLTYPE *yylloc,                   // match %define locations
    yyscan_t arg,                      // match %param
    url_parser_param_t *param,         // match %parse-param
    const char *errmsg
)
{
  parser_ctx_t *ctx = param ? &param->ctx : NULL;
  parser_yyerror(__FILE__, __LINE__, __func__, yylloc, arg, ctx, errmsg);
}

int url_parser_parse(const char *input, size_t len, url_parser_param_t *param)
{
  OA_ILE(param);
  yyscan_t arg = {0};
  yylex_init_extra(&param->ctx, &arg);
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
  yy_scan_bytes(input ? input : "", input ? (int)len : 0, arg);
  int ret =yyparse(arg, param);
  yylex_destroy(arg);
  return ret ? -1 : 0;
}

