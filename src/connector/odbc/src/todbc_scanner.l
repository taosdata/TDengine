%{
#include "todbc_util.h"

#include <stdio.h>

typedef struct params_s              params_t;
struct params_s {
    char                *key;
    char                *dsn;
    char                *uid;
    char                *pwd;
    char                *host;
};

#define PUSH_STATE(state)      yy_push_state(state, yyscanner)
#define POP_STATE()            yy_pop_state(yyscanner)

#define CHG_STATE(state)                \
do {                                    \
    yy_pop_state(yyscanner);            \
    yy_push_state(state, yyscanner);    \
} while (0)

#define TOP_STATE(top)                  \
do {                                    \
    yy_push_state(INITIAL, yyscanner);  \
    top = yy_top_state(yyscanner);      \
    yy_pop_state(yyscanner);            \
} while (0)

#define UNPUT()                                 \
do {                                            \
    while (yyleng) unput(yytext[yyleng-1]);     \
} while (0)

#define set_key()                       \
do {                                    \
    free(yyextra->key);                 \
    yyextra->key = strdup(yytext);      \
} while (0)

#define set_val()                                    \
do {                                                 \
    if (!yyextra->key) break;                        \
    if (strcasecmp(yyextra->key, "DSN")==0) {        \
        free(yyextra->dsn);                          \
        yyextra->dsn = strdup(yytext);               \
        break;                                       \
    }                                                \
    if (strcasecmp(yyextra->key, "UID")==0) {        \
        free(yyextra->uid);                          \
        yyextra->uid = strdup(yytext);               \
        break;                                       \
    }                                                \
    if (strcasecmp(yyextra->key, "PWD")==0) {        \
        free(yyextra->pwd);                          \
        yyextra->pwd = strdup(yytext);               \
        break;                                       \
    }                                                \
    if (strcasecmp(yyextra->key, "HOST")==0) {       \
        free(yyextra->host);                         \
        yyextra->host = strdup(yytext);              \
        break;                                       \
    }                                                \
} while (0)

%}

%option prefix="todbc_yy"
%option extra-type="struct params_s *"
%option reentrant
%option noyywrap
%option noinput nounput
%option debug verbose
%option stack
%option nodefault
%option warn
%option perf-report
%option 8bit

%x KEY EQ BRACE1 BRACE2 VAL

%%
<<EOF>> { int state; TOP_STATE(state);
          if (state == INITIAL) yyterminate();
          if (state == VAL)     yyterminate();
          return -1; }
[[:space:]]+    { }
[[:alnum:]]+    { set_key(); PUSH_STATE(KEY); }
.|\n            { return -1; }

<KEY>[[:space:]]+   { }
<KEY>[=]            { CHG_STATE(EQ); }
<KEY>.|\n           { return -1; }

<EQ>[[:space:]]+               { }
<EQ>[^][{}(),;?*=!@/\\\n[:space:]]+     { set_val(); CHG_STATE(VAL); }
<EQ>[{]                        { CHG_STATE(BRACE1); }
<EQ>.|\n                       { return -1; }

<BRACE1>[[:space:]]+       { }
<BRACE1>[^{}\n[:space:]]+  { set_val(); CHG_STATE(BRACE2); }
<BRACE1>.|\n      { return -1; }

<BRACE2>[[:space:]]+       { }
<BRACE2>[}]       { CHG_STATE(VAL); }
<BRACE2>.|\n      { return -1; }

<VAL>[;]          { POP_STATE(); }
<VAL>.|\n         { return -1; }
%%

int todbc_parse_conn_string(const char *conn, char **dsn, char **uid, char **pwd, char **host) {
  yyscan_t arg = {0};
  params_t params = {0};
  yylex_init(&arg);
  yyset_debug(0, arg);
  yyset_extra(&params, arg);
  yy_scan_string(conn, arg);
  int ret =yylex(arg);
  yylex_destroy(arg);
  *dsn = params.dsn;
  *uid = params.uid;
  *pwd = params.pwd;
  *host = params.host;
  return ret ? -1 : 0;
}


