%{
#include "todbc_flex.h"
#include <stdio.h>

#ifdef _MSC_VER
#define strncasecmp _strnicmp
#define strcasecmp _stricmp
#endif

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
    if (strcasecmp(yyextra->key, "DB")==0) {         \
        free(yyextra->db);                           \
        yyextra->pwd = strdup(yytext);               \
        break;                                       \
    }                                                \
    if (strcasecmp(yyextra->key, "Server")==0) {     \
        free(yyextra->server);                       \
        yyextra->server = strdup(yytext);            \
        break;                                       \
    }                                                \
    if (strcasecmp(yyextra->key, "SERVER_ENC")==0) { \
        free(yyextra->svr_enc);                      \
        yyextra->svr_enc = strdup(yytext);           \
        break;                                       \
    }                                                \
    if (strcasecmp(yyextra->key, "CLIENT_ENC")==0) { \
        free(yyextra->cli_enc);                      \
        yyextra->cli_enc = strdup(yytext);           \
        break;                                       \
    }                                                \
} while (0)

%}

%option prefix="todbc_yy"
%option extra-type="conn_val_t *"
%option nounistd
%option never-interactive
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
[[:alnum:]_]+   { set_key(); PUSH_STATE(KEY); }
.|\n            { return -1; }

<KEY>[[:space:]]+   { }
<KEY>[=]            { CHG_STATE(EQ); }
<KEY>.|\n           { return -1; }

<EQ>[[:space:]]+               { }
<EQ>[^][{}(),;?*=!@/\\\n[:space:]]+     { set_val(); CHG_STATE(VAL); }
<EQ>[{]                        { CHG_STATE(BRACE1); }
<EQ>.|\n                       { return -1; }

<BRACE1>[^{}\n]+  { set_val(); CHG_STATE(BRACE2); }
<BRACE1>.|\n      { return -1; }

<BRACE2>[[:space:]]+       { }
<BRACE2>[}]       { CHG_STATE(VAL); }
<BRACE2>.|\n      { return -1; }

<VAL>[;]          { POP_STATE(); }
<VAL>.|\n         { return -1; }
%%

int todbc_parse_conn_string(const char *conn, conn_val_t *val) {
  yyscan_t arg = {0};
  yylex_init(&arg);
  yyset_debug(0, arg);
  yyset_extra(val, arg);
  yy_scan_string(conn, arg);
  int ret =yylex(arg);
  yylex_destroy(arg);
  if (val->key) free(val->key); val->key = NULL;
  if (ret) {
    conn_val_reset(val);
  }
  return ret ? -1 : 0;
}

void conn_val_reset(conn_val_t *val) {
  if (val->key) {
    free(val->key); val->key = NULL;
  }
  if (val->dsn) {
    free(val->dsn); val->dsn = NULL;
  }
  if (val->uid) {
    free(val->uid); val->uid = NULL;
  }
  if (val->pwd) {
    free(val->pwd); val->pwd = NULL;
  }
  if (val->db) {
    free(val->db); val->db = NULL;
  }
  if (val->server) {
    free(val->server); val->server = NULL;
  }
  if (val->svr_enc) {
    free(val->svr_enc); val->svr_enc = NULL;
  }
  if (val->cli_enc) {
    free(val->cli_enc); val->cli_enc = NULL;
  }
}

