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
    #define YYSTYPE       EJSON_PARSER_YYSTYPE
    #define YYLTYPE       EJSON_PARSER_YYLTYPE
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
    static void _yyerror_impl(
        YYLTYPE *yylloc,                   // match %define locations
        yyscan_t arg,                      // match %param
        ejson_parser_param_t *param,       // match %parse-param
        const char *errmsg
    );
    static void yyerror(
        YYLTYPE *yylloc,                   // match %define locations
        yyscan_t arg,                      // match %param
        ejson_parser_param_t *param,       // match %parse-param
        const char *errsg
    );

    #define EPE(_loc, fmt, ...) do {                                 \
      YLOG(LOG_MALS, &_loc, fmt, ##__VA_ARGS__);                     \
      YYABORT;                                                       \
    } while (0)
    #define EEE(_loc, fmt, ...) do {                                 \
      EPE(_loc, "[%d]%s" fmt "",                                     \
          errno, strerror(errno), ##__VA_ARGS__);                    \
    } while (0)

    #define SET_LOC(_dst, _src) do {                                 \
      _dst = _src;                                                   \
    } while (0)

    #define EJSON_NEW_WITH_ID(_ejson, _token, _loc) do {             \
      const char *s = _token.text;                                   \
      size_t n = _token.leng;                                        \
      _ejson = ejson_new_str(s, n);                                  \
      if (!_ejson) EEE(_loc, ":invalid id:[%.*s]", (int)n, s);       \
    } while (0)

    #define EJSON_NEW_WITH_NUM(_ejson, _token, _loc) do {            \
      const char *s = _token.text;                                   \
      size_t n = _token.leng;                                        \
      _ejson = _ejson_new_num(s, n);                                 \
      if (!_ejson) {                                                 \
        EEE(_loc, ":OOM or invalid number:[%.*s]", (int)n, s);       \
      }                                                              \
    } while (0)

    #define EJSON_NEW_WITH_STR(_ejson, _str, _loc) do {              \
      _ejson = _ejson_new_str(&_str);                                \
      if (!_ejson) EEE(_loc, "%s", "");                              \
    } while (0)

    #define EJSON_NEW_WITH_BIN(_ejson, _bin, _loc) do {              \
      _ejson = _ejson_new_bin(&_bin);                                \
      if (!_ejson) EEE(_loc, "%s", "");                              \
    } while (0)


    #define EJSON_NEW_ARR(_ejson, _loc) do {                         \
      _ejson = ejson_new_arr();                                      \
      if (!_ejson) EEE(_loc, "%s", "");                              \
    } while (0)

    #define EJSON_DEC_REF(_v) do {                                   \
      if (_v) { ejson_dec_ref(_v); _v = NULL; }                      \
    } while (0)

    #define EJSON_NEW_ARR_WITH_VAL(_ejson, _val, _loc) do {          \
      _ejson = ejson_new_arr();                                      \
      if (!_ejson) {                                                 \
        EJSON_DEC_REF(_val);                                         \
        EEE(_loc, "%s", "");                                         \
      }                                                              \
      int r = ejson_arr_append(_ejson, _val);                        \
      EJSON_DEC_REF(_val);                                           \
      if (r) {                                                       \
        EJSON_DEC_REF(_ejson);                                       \
        EEE(_loc, "%s", "");                                         \
      }                                                              \
    } while (0)

    #define EJSON_ARR_APPEND(_ejson, _vals, _val, _loc) do {         \
      int r = ejson_arr_append(_vals, _val);                         \
      EJSON_DEC_REF(_val);                                           \
      if (r) {                                                       \
        EJSON_DEC_REF(_vals);                                        \
        EEE(_loc, "%s", "");                                         \
      }                                                              \
      _ejson = _vals;                                                \
    } while (0)

    #define EJSON_NEW_OBJ(_ejson, _loc) do {                         \
      _ejson = ejson_new_obj();                                      \
      if (!_ejson) EEE(_loc, "%s", "");                              \
    } while (0)

    #define EJSON_NEW_OBJ_WITH_KV(_ejson, _kv, _loc) do {            \
      _ejson = _ejson_new_obj(&_kv);                                 \
      _ejson_kv_release(&_kv);                                       \
      if (!_ejson) EEE(_loc, "%s", "");                              \
    } while (0)

    #define EJSON_OBJ_APPEND(_ejson, _kvs, _kv, _loc) do {           \
      int r = _ejson_obj_set(_kvs, &_kv);                            \
      _ejson_kv_release(&_kv);                                       \
      if (r) {                                                       \
        EJSON_DEC_REF(_kvs);                                         \
        EEE(_loc, "%s", "");                                         \
      }                                                              \
      _ejson = _kvs;                                                 \
    } while (0)

    #define STR_INIT(_str, _loc) do {                                \
      _ejson_str_init(&_str);                                        \
    } while (0)

    #define BIN_INIT(_bin, _loc) do {                                \
      _ejson_bin_init(&_bin);                                        \
    } while (0)

    #define STR_APPEND(_str, _v, _loc) do {                          \
      if (_ejson_str_append(&_str, _v.text, _v.leng)) {              \
        _ejson_str_release(&_str);                                   \
        EEE(_loc, "%s", "");                                         \
      }                                                              \
    } while (0)

    #define BIN_APPEND_HEX(_bin, _v, _loc) do {                      \
      const char *s = _v.text;                                       \
      size_t      n = _v.leng;                                       \
      if (n & 1) {                                                   \
        _ejson_bin_release(&_bin);                                   \
        errno = EINVAL;                                              \
        EEE(_loc, ":[%.*s] not even", (int)n, s);                    \
      }                                                              \
      if (_ejson_bin_append_hex(&_bin, s+2, n-2)) {                  \
        _ejson_bin_release(&_bin);                                   \
        EEE(_loc, "%s", "");                                         \
      }                                                              \
    } while (0)

    #define BIN_APPEND_STR(_bin, _v, _loc) do {                      \
      if (_ejson_bin_append_str(&_bin, _v.text, _v.leng)) {          \
        _ejson_bin_release(&_bin);                                   \
        EEE(_loc, "%s", "");                                         \
      }                                                              \
    } while (0)

    #define STR_APPEND_CHR(_str, _c, _loc) do {                      \
      if (_ejson_str_append(&_str, &_c, 1)) {                        \
        _ejson_str_release(&_str);                                   \
        EEE(_loc, "%s", "");                                         \
      }                                                              \
    } while (0)

    #define STR_APPEND_UNI(_str, _u, _loc) do {                      \
      const char *s = _u.text + 2;                                   \
      if (strncmp(s, "0000", 4) == 0) {                              \
        _ejson_str_release(&_str);                                   \
        errno = EINVAL;                                              \
        EEE(_loc, ":[\\u%.4s] not allowd", s);                       \
      }                                                              \
      char utf16[64]; *utf16 = '\0';                                 \
      snprintf(utf16, sizeof(utf16), "%.4s", s);                     \
      if (_u.leng == 12) {                                           \
        snprintf(utf16+4, sizeof(utf16)-4, "%.4s", s + 6);           \
      }                                                              \
      char utf8[64]; *utf8 = '\0';                                   \
      iconv_t cnv = param->cnv;                                      \
      int r = _ejson_parser_iconv_char_unsafe(utf16,                 \
          utf8, sizeof(utf8), cnv);                                  \
      if (r) {                                                       \
        _ejson_str_release(&_str);                                   \
        EEE(_loc, ":invalid %s:[%.*s]",                              \
          EJSON_PARSER_FROM, (int)_u.leng, _u.text);                 \
      }                                                              \
      if (_ejson_str_append_uni(&_str, utf8, strlen(utf8))) {        \
        _ejson_str_release(&_str);                                   \
        EEE(_loc, "%s", "");                                         \
      }                                                              \
    } while (0)

    #define KV_INIT_WITH_STR(_kv, _str, _loc) do {                   \
      _kv.val = ejson_new_null();                                    \
      _kv.key = _str;                                                \
    } while (0)

    #define KV_INIT_WITH_ID(_kv, _token, _loc) do {                  \
      const char *s = _token.text;                                   \
      size_t n = _token.leng;                                        \
      memset(&_kv, 0, sizeof(_kv));                                  \
      _ejson_str_init(&_kv.key);                                     \
      if (_ejson_str_append(&_kv.key, s, n)) EEE(_loc, "%s", "");    \
      _kv.val = ejson_new_null();                                    \
    } while (0)

    #define KV_INIT(_kv, _token, _val, _loc) do {                    \
      const char *s = _token.text;                                   \
      size_t n = _token.leng;                                        \
      _kv.val = _val;                                                \
      _ejson_str_init(&_kv.key);                                     \
      if (_ejson_str_append(&_kv.key, s, n)) {                       \
        _ejson_kv_release(&_kv);                                     \
        EEE(_loc, "%s", "");                                         \
      }                                                              \
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
%parse-param { ejson_parser_param_t *param }

// union members
%union { ejson_parser_token_t  token; }
%union { ejson_t              *ejson; }
%union { _ejson_str_t          str; }
%union { _ejson_bin_t          bin; }
%union { _ejson_kv_t           kv; }
%union { char                  c; }
%union { uint32_t              uni; }

%token V_TRUE V_FALSE V_NULL BQ
%token <token> ID NUMBER STR CHR EUNI HEX
%nterm <ejson> json obj arr kvs jsons
%nterm <str> str strings
%nterm <bin> bin bins
%nterm <kv> kv

%destructor { ejson_dec_ref($$); $$ = NULL; }           <ejson>
%destructor { _ejson_str_release(&$$); }     <str>
%destructor { _ejson_bin_release(&$$); }     <bin>
%destructor { _ejson_kv_release(&$$);  }     <kv>

 /* %nterm <str>   args */ // non-terminal `input` use `str` to store
                           // token value as well
 /* %destructor { free($$); } <str> */

%% /* The grammar follows. */

input:
  %empty            { (void)yynerrs; }
| json              { param->ejson = $1; SET_LOC(param->ejson->loc, @$); }
;

json:
  ID                { EJSON_NEW_WITH_ID($$, $1, @$);  SET_LOC($$->loc, @$); }
| NUMBER            { EJSON_NEW_WITH_NUM($$, $1, @$); SET_LOC($$->loc, @$); }
| V_TRUE            { $$ = ejson_new_true();          SET_LOC($$->loc, @$); }
| V_FALSE           { $$ = ejson_new_false();         SET_LOC($$->loc, @$); }
| V_NULL            { $$ = ejson_new_null();          SET_LOC($$->loc, @$); }
| str               { EJSON_NEW_WITH_STR($$, $1, @$); SET_LOC($$->loc, @$); }
| bin               { EJSON_NEW_WITH_BIN($$, $1, @$); SET_LOC($$->loc, @$); }
| obj               { $$ = $1; SET_LOC($$->loc, @$); }
| arr               { $$ = $1; SET_LOC($$->loc, @$); }
;

bin:
  BQ BQ             { BIN_INIT($$, @$); SET_LOC($$.loc, @$); }
| BQ bins BQ        { $$ = $2; SET_LOC($$.loc, @$); }
;

bins:
  HEX               { BIN_INIT($$, @$); BIN_APPEND_HEX($$, $1, @$);  SET_LOC($$.loc, @$); }
| STR               { BIN_INIT($$, @$); BIN_APPEND_STR($$, $1, @$);  SET_LOC($$.loc, @$); }
| CHR               { BIN_INIT($$, @$); BIN_APPEND_STR($$, $1, @$);  SET_LOC($$.loc, @$); }
| bins HEX          { $$ = $1; BIN_APPEND_HEX($$, $2, @$);    SET_LOC($$.loc, @$); }
| bins STR          { $$ = $1; BIN_APPEND_STR($$, $2, @$);    SET_LOC($$.loc, @$); }
| bins CHR          { $$ = $1; BIN_APPEND_STR($$, $2, @$);    SET_LOC($$.loc, @$); }
;

str:
  '"' strings '"'      { $$ = $2; SET_LOC($$.loc, @$); }
| '\'' strings '\''    { $$ = $2; SET_LOC($$.loc, @$); }
| '`' strings '`'      { $$ = $2; SET_LOC($$.loc, @$); }
| '"' '"'              { STR_INIT($$, @$); SET_LOC($$.loc, @$); }
| '\'' '\''            { STR_INIT($$, @$); SET_LOC($$.loc, @$); }
| '`' '`'              { STR_INIT($$, @$); SET_LOC($$.loc, @$); }
;

strings:
  STR                  { STR_INIT($$, @$); STR_APPEND($$, $1, @$);         SET_LOC($$.loc, @$); }
| CHR                  { STR_INIT($$, @$); STR_APPEND($$, $1, @$);         SET_LOC($$.loc, @$); }
| EUNI                 { STR_INIT($$, @$); STR_APPEND_UNI($$, $1, @$);     SET_LOC($$.loc, @$); }
| strings STR          { $$ = $1; STR_APPEND($$, $2, @$);     SET_LOC($$.loc, @$); }
| strings CHR          { $$ = $1; STR_APPEND($$, $2, @$);     SET_LOC($$.loc, @$); }
| strings EUNI         { $$ = $1; STR_APPEND_UNI($$, $2, @$); SET_LOC($$.loc, @$); }
;

arr:
  '[' ']'              { EJSON_NEW_ARR($$, @$); SET_LOC($$->loc, @$); }
| '[' jsons ']'        { $$ = $2;               SET_LOC($$->loc, @$); }
;

jsons:
  json                 { EJSON_NEW_ARR_WITH_VAL($$, $1, @$); SET_LOC($$->loc, @$); }
| ','                  { EJSON_NEW_ARR($$, @$);              SET_LOC($$->loc, @$); }
| jsons ',' json       { EJSON_ARR_APPEND($$, $1, $3, @$);   SET_LOC($$->loc, @$); }
| jsons ','            { $$ = $1;                            SET_LOC($$->loc, @$); }
;

obj:
  '{' '}'              { EJSON_NEW_OBJ($$, @$); SET_LOC($$->loc, @$); }
| '{' kvs '}'          { $$ = $2;               SET_LOC($$->loc, @$); }
;

kvs:
  kv                   { EJSON_NEW_OBJ_WITH_KV($$, $1, @$); SET_LOC($$->loc, @$); }
| ','                  { EJSON_NEW_OBJ($$, @$);             SET_LOC($$->loc, @$); }
| kvs ',' kv           { EJSON_OBJ_APPEND($$, $1, $3, @$);  SET_LOC($$->loc, @$); }
| kvs ','              { $$ = $1;                           SET_LOC($$->loc, @$); }
;

kv:
  str                  { KV_INIT_WITH_STR($$, $1, @$); SET_LOC($$.loc, @$); }
| str ':'              { KV_INIT_WITH_STR($$, $1, @$); SET_LOC($$.loc, @$); }
| str ':' json         { $$.key = $1; $$.val = $3;     SET_LOC($$.loc, @$); }
| ID                   { KV_INIT_WITH_ID($$, $1, @$);  SET_LOC($$.loc, @$); }
| ID ':'               { KV_INIT_WITH_ID($$, $1, @$);  SET_LOC($$.loc, @$); }
| ID ':' json          { KV_INIT($$, $1, $3, @$);      SET_LOC($$.loc, @$); }
;


%%

static void _yyerror_impl(
    YYLTYPE *yylloc,                   // match %define locations
    yyscan_t arg,                      // match %param
    ejson_parser_param_t *param,       // match %parse-param
    const char *errmsg
)
{
  // to implement it here
  (void)yylloc;
  (void)arg;
  (void)param;
  (void)errmsg;

  if (!param) {
    fprintf(stderr, "(%d,%d)->(%d,%d):%s\n",
        yylloc->first_line, yylloc->first_column,
        yylloc->last_line, yylloc->last_column,
        errmsg);

    return;
  }

  param->ctx.bad_token.first_line   = yylloc->first_line;
  param->ctx.bad_token.first_column = yylloc->first_column;
  param->ctx.bad_token.last_line    = yylloc->last_line;
  param->ctx.bad_token.last_column  = yylloc->last_column;
  param->ctx.err_msg[0] = '\0';
  snprintf(param->ctx.err_msg, sizeof(param->ctx.err_msg), "%s", errmsg);
}

/* Called by yyparse on error. */
static void yyerror(
    YYLTYPE *yylloc,                   // match %define locations
    yyscan_t arg,                      // match %param
    ejson_parser_param_t *param,       // match %parse-param
    const char *errmsg
)
{
  _yyerror_impl(yylloc, arg, param, errmsg);
}

int ejson_parser_parse(const char *input, size_t len,
    ejson_parser_param_t *param, iconv_t cnv)
{
  yyscan_t arg = {0};
  yylex_init(&arg);
  // yyset_in(in, arg);
  int debug_flex = param ? param->ctx.debug_flex : 0;
  int debug_bison = param ? param->ctx.debug_bison: 0;
  yyset_debug(debug_flex, arg);
  yydebug = debug_bison;
  yyset_extra(param, arg);
  param->ctx.input = input;
  param->ctx.len   = len;
  param->ctx.prev  = 0;
  param->ctx.pres  = 0;
  param->internal  = NULL;
  param->cnv       = cnv;

  yy_scan_bytes(input ? input : "", input ? (int)len : 0, arg);
  int ret =yyparse(arg, param);
  yylex_destroy(arg);
  return ret ? -1 : 0;
}

