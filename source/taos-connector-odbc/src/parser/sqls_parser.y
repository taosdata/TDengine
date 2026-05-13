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
    #define YYSTYPE       SQLS_PARSER_YYSTYPE
    #define YYLTYPE       SQLS_PARSER_YYLTYPE
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
        sqls_parser_param_t *param,        // match %parse-param
        const char *errsg
    );

    #define SET_TOKEN_BOUND(_rr, _qms) do {    \
      _rr.qms  = _qms;                         \
    } while (0)

    #define FOUND(_start, _end, _qms) do {                                    \
      if (param->sql_found) {                                                 \
        int r = param->sql_found(param, _start, _end, _qms, param->arg);      \
        if (r) YYABORT;                                                       \
      }                                                                       \
    } while (0)

    void sqls_parser_param_reset(sqls_parser_param_t *param)
    {
      if (!param) return;
      param->ctx.err_msg[0] = '\0';
    }

    void sqls_parser_param_release(sqls_parser_param_t *param)
    {
      if (!param) return;
      param->ctx.err_msg[0] = '\0';
    }
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
%parse-param { sqls_parser_param_t *param }

// union members
%union { parser_token_t token; }
%union { char c; }
%union { sqls_parser_nterm_t nterm; }

%token TOKEN ERROR STR
%token LP RP LC RC LB RB
%token DQ SQ AA
%token LN CH

%nterm <nterm> qm
%nterm <nterm> any_token sql sqls

 /* %nterm <str>   args */ // non-terminal `input` use `str` to store
                           // token value as well
 /* %destructor { free($$); } <str> */

%% /* The grammar follows. */

input:
  sqls                      { (void)yynerrs; }
| sqls delimit
;

sqls:
  %empty                    { SET_TOKEN_BOUND($$, 0); }
| sql                       { SET_TOKEN_BOUND($$, $1.qms); FOUND(@1.prev, @1.pres+1, $$.qms); }
| sqls delimit sql          { SET_TOKEN_BOUND($$, $3.qms); FOUND(@3.prev, @3.pres+1, $$.qms); }
;

sql:
  any_token            { SET_TOKEN_BOUND($$, $1.qms); }
| sql any_token        { SET_TOKEN_BOUND($$, ($1.qms + $2.qms)); }
;

any_token:
  token                { SET_TOKEN_BOUND($$, 0); }
| quoted               { SET_TOKEN_BOUND($$, 0); }
| lc rc                { SET_TOKEN_BOUND($$, 0); }
| lp rp                { SET_TOKEN_BOUND($$, 0); }
| lb rb                { SET_TOKEN_BOUND($$, 0); }
| lc sql rc            { SET_TOKEN_BOUND($$, $2.qms); }
| lp sql rp            { SET_TOKEN_BOUND($$, $2.qms); }
| lb sql rb            { SET_TOKEN_BOUND($$, $2.qms); }
| qm                   { SET_TOKEN_BOUND($$, $1.qms); }
;

token:
  TOKEN
;

lc:
  '('
;

rc:
  ')'
;

lp:
  '{'
;

rp:
  '}'
;

lb:
  '['
;

rb:
  ']'
;

quoted:
  dq dq
| sq sq
| aa aa
| dq dqss dq
| sq sqss sq
| aa aass aa
;

dq:
  '"'
;

sq:
  '\''
;

aa:
  '`'
;

qm:
  '?'          { SET_TOKEN_BOUND($$, 1); }
;

dqss:
  dqs
| dqss dqs
;

dqs:
  STR
| DQ DQ
| esc
;

sqss:
  sqs
| sqss sqs
;

sqs:
  STR
| SQ SQ
| esc
;

aass:
  aas
| aass aas
;

aas:
  STR
| AA AA
| esc
;

esc:
  '\\' CH
;

delimit:
  sc
| delimit sc
;

sc:
  ';'
;

%%

/* Called by yyparse on error. */
static void yyerror(
    YYLTYPE *yylloc,                   // match %define locations
    yyscan_t arg,                      // match %param
    sqls_parser_param_t *param,         // match %parse-param
    const char *errmsg
)
{
  parser_ctx_t *ctx = param ? &param->ctx : NULL;
  parser_yyerror(__FILE__, __LINE__, __func__, yylloc, arg, ctx, errmsg);
}

int sqls_parser_parse(const char *input, size_t len, sqls_parser_param_t *param)
{
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

