%{
#ifdef _MSC_VER
#include <windows.h>
#define strncasecmp  _strnicmp
#define strcasecmp   _stricmp
#define basename     PathFindFileNameA
#else
#include <libgen.h>
#endif

#include "todbc_flex.h"
#include <stdio.h>
#include <odbcinst.h>

static int   process_map(const char *cfg, map_tsdb_type_t *tsdb_map, int type);

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

#define set_val()                                                      \
do {                                                                   \
  int r = 0;                                                           \
  int curr; TOP_STATE(curr);                                           \
  POP_STATE();                                                         \
  int state; TOP_STATE(state);                                         \
  switch(state) {                                                      \
    case DSN: {                                                        \
      free(yyextra->dsn);                                              \
      yyextra->dsn = strdup(yytext);                                   \
    } break;                                                           \
    case UID: {                                                        \
      free(yyextra->uid);                                              \
      yyextra->uid = strdup(yytext);                                   \
    } break;                                                           \
    case PWD: {                                                        \
      free(yyextra->pwd);                                              \
      yyextra->pwd = strdup(yytext);                                   \
    } break;                                                           \
    case SERVER: {                                                     \
      free(yyextra->server);                                           \
      yyextra->server = strdup(yytext);                                \
    } break;                                                           \
    case DB: {                                                         \
      free(yyextra->db);                                               \
      yyextra->db = strdup(yytext);                                    \
    } break;                                                           \
    case ENC_CHAR: {                                                   \
      free(yyextra->enc_char);                                         \
      yyextra->enc_char = strdup(yytext);                              \
    } break;                                                           \
    case ENC_WCHAR: {                                                  \
      free(yyextra->enc_wchar);                                        \
      yyextra->enc_wchar = strdup(yytext);                             \
    } break;                                                           \
    case ENC_DB: {                                                     \
      free(yyextra->enc_db);                                           \
      yyextra->enc_db = strdup(yytext);                                \
    } break;                                                           \
    case ENC_LOCAL: {                                                  \
      free(yyextra->enc_local);                                        \
      yyextra->enc_local = strdup(yytext);                             \
    } break;                                                           \
    case TSDB_FLOAT: {                                                 \
      if (process_map(yytext, &yyextra->tsdb_map, TSDB_FLOAT)) {       \
        r = -1;                                                        \
      }                                                                \
    } break;                                                           \
    case TSDB_BIGINT: {                                                \
      if (process_map(yytext, &yyextra->tsdb_map, TSDB_BIGINT)) {      \
        r = -1;                                                        \
      }                                                                \
    } break;                                                           \
    case KEY: {                                                        \
    } break;                                                           \
    default: {                                                         \
      r = -1;                                                          \
    } break;                                                           \
  }                                                                    \
  PUSH_STATE(curr);                                                    \
  if (r) return r;                                                     \
} while (0)

#define FAIL()                                                                       \
do {                                                                                 \
  /*fprintf(stderr, "==%s[%d]%s()==\n", basename(__FILE__), __LINE__, __func__);*/   \
  return -1;                                                                         \
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
%option case-insensitive

%x DSN UID PWD SERVER DB
%x ENC_CHAR ENC_WCHAR ENC_DB ENC_LOCAL
%x TSDB_FLOAT TSDB_BIGINT
%x KEY EQ BRACE1 BRACE2 VAL

%%
<<EOF>> { int state; TOP_STATE(state);
          if (state == INITIAL) yyterminate();
          if (state == VAL)     yyterminate();
          FAIL(); }
[[:space:]]+    { }
"DSN"           { PUSH_STATE(DSN); }
"UID"           { PUSH_STATE(UID); }
"PWD"           { PUSH_STATE(PWD); }
"Server"        { PUSH_STATE(SERVER); }
"DB"            { PUSH_STATE(DB); }
"ENC_CHAR"      { PUSH_STATE(ENC_CHAR); }
"ENC_WCHAR"     { PUSH_STATE(ENC_WCHAR); }
"ENC_DB"        { PUSH_STATE(ENC_DB); }
"ENC_LOCAL"     { PUSH_STATE(ENC_LOCAL); }
"map.float"     { PUSH_STATE(TSDB_FLOAT); }
"map.bigint"    { PUSH_STATE(TSDB_BIGINT); }
[[:alnum:]_]+   { PUSH_STATE(KEY); }
.|\n            { FAIL(); }

<DSN,UID,PWD,SERVER,DB,ENC_CHAR,ENC_WCHAR,ENC_DB,ENC_LOCAL,TSDB_FLOAT,TSDB_BIGINT,KEY>[[:space:]]+   { }
<DSN,UID,PWD,SERVER,DB,ENC_CHAR,ENC_WCHAR,ENC_DB,ENC_LOCAL,TSDB_FLOAT,TSDB_BIGINT,KEY>[=]            { PUSH_STATE(EQ); }
<DSN,UID,PWD,SERVER,DB,ENC_CHAR,ENC_WCHAR,ENC_DB,ENC_LOCAL,TSDB_FLOAT,TSDB_BIGINT,KEY>.|\n           { FAIL(); }

<EQ>[[:space:]]+               { }
<EQ>[{]                        { CHG_STATE(BRACE1); }
<EQ>[^][{}(),;?*=!@/\\\n[:space:]]+     { set_val(); POP_STATE(); CHG_STATE(VAL); }
<EQ>.|\n                       { FAIL(); }

<BRACE1>[^{}\n]+  { set_val(); CHG_STATE(BRACE2); }
<BRACE1>.|\n      { FAIL(); }

<BRACE2>[[:space:]]+       { }
<BRACE2>[}]       { POP_STATE(); CHG_STATE(VAL); }
<BRACE2>.|\n      { FAIL(); }

<VAL>[;]          { POP_STATE(); }
<VAL>.|\n         { FAIL(); }
%%

static char* get_val_by_key_from_odbc_ini(const char *dsn, const char *key);

static void conn_val_init(conn_val_t *val);

int todbc_parse_conn_string(const char *conn, conn_val_t *val) {
  yyscan_t arg = {0};
  yylex_init(&arg);
  yyset_debug(0, arg);
  yyset_extra(val, arg);

  conn_val_init(val);

  yy_scan_string(conn, arg);
  int ret =yylex(arg);
  yylex_destroy(arg);
  if (ret || !val->dsn) {
    conn_val_reset(val);
  } else {
    if (!val->uid) {
      val->uid = get_val_by_key_from_odbc_ini(val->dsn, "UID");
    }
    if (!val->pwd) {
      val->pwd = get_val_by_key_from_odbc_ini(val->dsn, "PWD");
    }
    if (!val->server) {
      val->server = get_val_by_key_from_odbc_ini(val->dsn, "Server");
    }
  }
  return ret ? -1 : 0;
}

void conn_val_reset(conn_val_t *val) {
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
  if (val->enc_local) {
    free(val->enc_local); val->enc_local = NULL;
  }
  if (val->enc_db) {
    free(val->enc_db); val->enc_db = NULL;
  }
  if (val->enc_char) {
    free(val->enc_char); val->enc_char = NULL;
  }
  if (val->enc_wchar) {
    free(val->enc_wchar); val->enc_wchar = NULL;
  }
}

static char* get_val_by_key_from_odbc_ini(const char *dsn, const char *key) {
  char Val[4096];
  Val[0] = '\0';
  int n = SQLGetPrivateProfileString(dsn, key, "", Val, sizeof(Val), "odbc.ini");
  if (n<=0) return NULL;
  if (Val[0]=='\0') return NULL;
  return strdup(Val);
}

static int process_map(const char *cfg, map_tsdb_type_t *tsdb_map, int type) {
  switch (type) {
    case TSDB_FLOAT: {
      if (strcmp(cfg, "SQL_DOUBLE")==0) {
        tsdb_map->tsdb_float = SQL_DOUBLE;
        return 0;
      }
    } break;
    case TSDB_BIGINT: {
      if (strcmp(cfg, "SQL_C_SBIGINT")==0) {
        tsdb_map->tsdb_bigint = SQL_C_SBIGINT;
        return 0;
      }
      if (strcmp(cfg, "SQL_C_UBIGINT")==0) {
        tsdb_map->tsdb_bigint = SQL_C_UBIGINT;
        return 0;
      }
      if (strcmp(cfg, "SQL_CHAR")==0) {
        tsdb_map->tsdb_bigint = SQL_CHAR;
        return 0;
      }
    } break;
    default: {
    } break;
  }
  return -1;
}

static void conn_val_init(conn_val_t *val) {
  if (!val) return;
  val->tsdb_map.tsdb_tinyint          = SQL_TINYINT;
  val->tsdb_map.tsdb_smallint         = SQL_SMALLINT;
  val->tsdb_map.tsdb_int              = SQL_INTEGER;
  val->tsdb_map.tsdb_bigint           = SQL_BIGINT;
  val->tsdb_map.tsdb_float            = SQL_REAL;
  val->tsdb_map.tsdb_double           = SQL_DOUBLE;
  val->tsdb_map.tsdb_bool             = SQL_TINYINT;
  val->tsdb_map.tsdb_timestamp        = SQL_CHAR;
  val->tsdb_map.tsdb_binary           = SQL_BINARY;
  val->tsdb_map.tsdb_nchar            = SQL_WCHAR;
}

