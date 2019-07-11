/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include <ctype.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>

#include "shash.h"
#include "tsql.h"
#include "tutil.h"

//All the keywords of the SQL language are stored in a hash table
typedef struct SKeyword {
  const char* name; // The keyword name
  uint8_t     type; // type
  uint8_t     len;  // length
} SKeyword;

static SKeyword keywordTable[] = {
    {"ID",          TK_ID},
    {"BOOL",        TK_BOOL},
    {"TINYINT",     TK_TINYINT},
    {"SMALLINT",    TK_SMALLINT},
    {"INTEGER",     TK_INTEGER},
    {"INT",         TK_INTEGER},
    {"BIGINT",      TK_BIGINT},
    {"FLOAT",       TK_FLOAT},
    {"DOUBLE",      TK_DOUBLE},
    {"STRING",      TK_STRING},
    {"TIMESTAMP",   TK_TIMESTAMP},
    {"BINARY",      TK_BINARY},
    {"NCHAR",       TK_NCHAR},
    {"OR",          TK_OR},
    {"AND",         TK_AND},
    {"NOT",         TK_NOT},
    {"EQ",          TK_EQ},
    {"NE",          TK_NE},
    {"ISNULL",      TK_ISNULL},
    {"NOTNULL",     TK_NOTNULL},
    {"IS",          TK_IS},
    {"LIKE",        TK_LIKE},
    {"GLOB",        TK_GLOB},
    {"BETWEEN",     TK_BETWEEN},
    {"IN",          TK_IN},
    {"GT",          TK_GT},
    {"GE",          TK_GE},
    {"LT",          TK_LT},
    {"LE",          TK_LE},
    {"BITAND",      TK_BITAND},
    {"BITOR",       TK_BITOR},
    {"LSHIFT",      TK_LSHIFT},
    {"RSHIFT",      TK_RSHIFT},
    {"PLUS",        TK_PLUS},
    {"MINUS",       TK_MINUS},
    {"DIVIDE",      TK_DIVIDE},
    {"TIMES",       TK_TIMES},
    {"STAR",        TK_STAR},
    {"SLASH",       TK_SLASH},
    {"REM ",        TK_REM},
    {"CONCAT",      TK_CONCAT},
    {"UMINUS",      TK_UMINUS},
    {"UPLUS",       TK_UPLUS},
    {"BITNOT",      TK_BITNOT},
    {"SHOW",        TK_SHOW},
    {"DATABASES",   TK_DATABASES},
    {"MNODES",      TK_MNODES},
    {"DNODES",      TK_DNODES},
    {"USERS",       TK_USERS},
    {"MODULES",     TK_MODULES},
    {"QUERIES",     TK_QUERIES},
    {"CONNECTIONS", TK_CONNECTIONS},
    {"STREAMS",     TK_STREAMS},
    {"CONFIGS",     TK_CONFIGS},
    {"SCORES",      TK_SCORES},
    {"GRANTS",      TK_GRANTS},
    {"DOT",         TK_DOT},
    {"TABLES",      TK_TABLES},
    {"STABLES",     TK_STABLES},
    {"VGROUPS",     TK_VGROUPS},
    {"DROP",        TK_DROP},
    {"TABLE",       TK_TABLE},
    {"DATABASE",    TK_DATABASE},
    {"USER",        TK_USER},
    {"USE",         TK_USE},
    {"DESCRIBE",    TK_DESCRIBE},
    {"ALTER",       TK_ALTER},
    {"PASS",        TK_PASS},
    {"PRIVILEGE",   TK_PRIVILEGE},
    {"DNODE",       TK_DNODE},
    {"IP",          TK_IP},
    {"LOCAL",       TK_LOCAL},
    {"IF",          TK_IF},
    {"EXISTS",      TK_EXISTS},
    {"CREATE",      TK_CREATE},
    {"KEEP",        TK_KEEP},
    {"REPLICA",     TK_REPLICA},
    {"DAYS",        TK_DAYS},
    {"ROWS",        TK_ROWS},
    {"CACHE",       TK_CACHE},
    {"ABLOCKS",     TK_ABLOCKS},
    {"TBLOCKS",     TK_TBLOCKS},
    {"CTIME",       TK_CTIME},
    {"CLOG",        TK_CLOG},
    {"COMP",        TK_COMP},
    {"PRECISION",   TK_PRECISION},
    {"LP",          TK_LP},
    {"RP",          TK_RP},
    {"TAGS",        TK_TAGS},
    {"USING",       TK_USING},
    {"AS",          TK_AS},
    {"COMMA",       TK_COMMA},
    {"NULL",        TK_NULL},
    {"SELECT",      TK_SELECT},
    {"FROM",        TK_FROM},
    {"VARIABLE",    TK_VARIABLE},
    {"INTERVAL",    TK_INTERVAL},
    {"FILL",        TK_FILL},
    {"SLIDING",     TK_SLIDING},
    {"ORDER",       TK_ORDER},
    {"BY",          TK_BY},
    {"ASC",         TK_ASC},
    {"DESC",        TK_DESC},
    {"GROUP",       TK_GROUP},
    {"HAVING",      TK_HAVING},
    {"LIMIT",       TK_LIMIT},
    {"OFFSET",      TK_OFFSET},
    {"SLIMIT",      TK_SLIMIT},
    {"SOFFSET",     TK_SOFFSET},
    {"WHERE",       TK_WHERE},
    {"NOW",         TK_NOW},
    {"INSERT",      TK_INSERT},
    {"INTO",        TK_INTO},
    {"VALUES",      TK_VALUES},
    {"RESET",       TK_RESET},
    {"QUERY",       TK_QUERY},
    {"ADD",         TK_ADD},
    {"COLUMN",      TK_COLUMN},
    {"TAG",         TK_TAG},
    {"CHANGE",      TK_CHANGE},
    {"SET",         TK_SET},
    {"KILL",        TK_KILL},
    {"CONNECTION",  TK_CONNECTION},
    {"COLON",       TK_COLON},
    {"STREAM",      TK_STREAM},
    {"ABORT",       TK_ABORT},
    {"AFTER",       TK_AFTER},
    {"ATTACH",      TK_ATTACH},
    {"BEFORE",      TK_BEFORE},
    {"BEGIN",       TK_BEGIN},
    {"CASCADE",     TK_CASCADE},
    {"CLUSTER",     TK_CLUSTER},
    {"CONFLICT",    TK_CONFLICT},
    {"COPY",        TK_COPY},
    {"DEFERRED",    TK_DEFERRED},
    {"DELIMITERS",  TK_DELIMITERS},
    {"DETACH",      TK_DETACH},
    {"EACH",        TK_EACH},
    {"END",         TK_END},
    {"EXPLAIN",     TK_EXPLAIN},
    {"FAIL",        TK_FAIL},
    {"FOR",         TK_FOR},
    {"IGNORE",      TK_IGNORE},
    {"IMMEDIATE",   TK_IMMEDIATE},
    {"INITIALLY",   TK_INITIALLY},
    {"INSTEAD",     TK_INSTEAD},
    {"MATCH",       TK_MATCH},
    {"KEY",         TK_KEY},
    {"OF",          TK_OF},
    {"RAISE",       TK_RAISE},
    {"REPLACE",     TK_REPLACE},
    {"RESTRICT",    TK_RESTRICT},
    {"ROW",         TK_ROW},
    {"STATEMENT",   TK_STATEMENT},
    {"TRIGGER",     TK_TRIGGER},
    {"VIEW",        TK_VIEW},
    {"ALL",         TK_ALL},
    {"COUNT",       TK_COUNT},
    {"SUM",         TK_SUM},
    {"AVG",         TK_AVG},
    {"MIN",         TK_MIN},
    {"MAX",         TK_MAX},
    {"FIRST",       TK_FIRST},
    {"LAST",        TK_LAST},
    {"TOP",         TK_TOP},
    {"BOTTOM",      TK_BOTTOM},
    {"STDDEV",      TK_STDDEV},
    {"PERCENTILE",  TK_PERCENTILE},
    {"APERCENTILE", TK_APERCENTILE},
    {"LEASTSQUARES",TK_LEASTSQUARES},
    {"HISTOGRAM",   TK_HISTOGRAM},
    {"DIFF",        TK_DIFF},
    {"SPREAD",      TK_SPREAD},
    {"WAVG",        TK_WAVG},
    {"INTERP",      TK_INTERP},
    {"LAST_ROW",    TK_LAST_ROW},
    {"SEMI",        TK_SEMI,},
    {"NONE",        TK_NONE,},
    {"PREV",        TK_PREV,},
    {"LINEAR",      TK_LINEAR,},
    {"IMPORT",      TK_IMPORT},
    {"METRIC",      TK_METRIC},
    {"TBNAME",      TK_TBNAME},
    {"JOIN",        TK_JOIN},
    {"METRICS",     TK_METRICS},
    {"STABLE",      TK_STABLE}
};

static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

static const char isIdChar[] = {
    /* x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 xA xB xC xD xE xF */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /* 0x */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /* 1x */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /* 2x */
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, /* 3x */
    0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, /* 4x */
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 1, /* 5x */
    0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, /* 6x */
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, /* 7x */
};

static void* KeywordHashTable = NULL;
int tSQLKeywordCode(const char* z, int n) {
  int         i;
  static char needInit = 1;
  if (needInit) {
    /* Initialize the keyword hash table */
    pthread_mutex_lock(&mutex);

    // double check
    if (needInit) {
      int nk = tListLen(keywordTable);

      KeywordHashTable = taosInitStrHash(nk, POINTER_BYTES, taosHashStringStep1);
      for (i = 0; i < nk; i++) {
        keywordTable[i].len = strlen(keywordTable[i].name);
        void* ptr = &keywordTable[i];
        taosAddStrHash(KeywordHashTable, (char*)keywordTable[i].name, (void*)&ptr);
      }
      needInit = 0;
    }
    pthread_mutex_unlock(&mutex);
  }

  char key[128] = {0};
  for (int32_t j = 0; j < n; ++j) {
    if (z[j] >= 'a' && z[j] <= 'z') {
      key[j] = (char)(z[j] & 0xDF);  // touppercase and set the null-terminated
    } else {
      key[j] = z[j];
    }
  }

  SKeyword** pKey = (SKeyword**)taosGetStrHashData(KeywordHashTable, key);
  if (pKey != NULL) {
    return (*pKey)->type;
  } else {
    return TK_ID;
  }
}

uint32_t tSQLGetToken(char* z, uint32_t* tokenType) {
  int i;
  switch (*z) {
    case ' ':
    case '\t':
    case '\n':
    case '\f':
    case '\r': {
      for (i = 1; isspace(z[i]); i++) {
      }
      *tokenType = TK_SPACE;
      return i;
    }
    case ':': {
      *tokenType = TK_COLON;
      return 1;
    }
    case '-': {
      if (z[1] == '-') {
        for (i = 2; z[i] && z[i] != '\n'; i++) {
        }
        *tokenType = TK_COMMENT;
        return i;
      }
      *tokenType = TK_MINUS;
      return 1;
    }
    case '(': {
      *tokenType = TK_LP;
      return 1;
    }
    case ')': {
      *tokenType = TK_RP;
      return 1;
    }
    case ';': {
      *tokenType = TK_SEMI;
      return 1;
    }
    case '+': {
      *tokenType = TK_PLUS;
      return 1;
    }
    case '*': {
      *tokenType = TK_STAR;
      return 1;
    }
    case '/': {
      if (z[1] != '*' || z[2] == 0) {
        *tokenType = TK_SLASH;
        return 1;
      }
      for (i = 3; z[i] && (z[i] != '/' || z[i - 1] != '*'); i++) {
      }
      if (z[i]) i++;
      *tokenType = TK_COMMENT;
      return i;
    }
    case '%': {
      *tokenType = TK_REM;
      return 1;
    }
    case '=': {
      *tokenType = TK_EQ;
      return 1 + (z[1] == '=');
    }
    case '<': {
      if (z[1] == '=') {
        *tokenType = TK_LE;
        return 2;
      } else if (z[1] == '>') {
        *tokenType = TK_NE;
        return 2;
      } else if (z[1] == '<') {
        *tokenType = TK_LSHIFT;
        return 2;
      } else {
        *tokenType = TK_LT;
        return 1;
      }
    }
    case '>': {
      if (z[1] == '=') {
        *tokenType = TK_GE;
        return 2;
      } else if (z[1] == '>') {
        *tokenType = TK_RSHIFT;
        return 2;
      } else {
        *tokenType = TK_GT;
        return 1;
      }
    }
    case '!': {
      if (z[1] != '=') {
        *tokenType = TK_ILLEGAL;
        return 2;
      } else {
        *tokenType = TK_NE;
        return 2;
      }
    }
    case '|': {
      if (z[1] != '|') {
        *tokenType = TK_BITOR;
        return 1;
      } else {
        *tokenType = TK_CONCAT;
        return 2;
      }
    }
    case ',': {
      *tokenType = TK_COMMA;
      return 1;
    }
    case '&': {
      *tokenType = TK_BITAND;
      return 1;
    }
    case '~': {
      *tokenType = TK_BITNOT;
      return 1;
    }
    case '\'':
    case '"': {
      int delim = z[0];
      for (i = 1; z[i]; i++) {
        if (z[i] == delim) {
          if (z[i + 1] == delim) {
            i++;
          } else {
            break;
          }
        }
      }
      if (z[i]) i++;
      *tokenType = TK_STRING;
      return i;
    }
    case '.': {
      *tokenType = TK_DOT;
      return 1;
    }
    case '0':
    case '1':
    case '2':
    case '3':
    case '4':
    case '5':
    case '6':
    case '7':
    case '8':
    case '9': {
      *tokenType = TK_INTEGER;
      for (i = 1; isdigit(z[i]); i++) {
      }

      /* here is the 1a/2s/3m/9y */
      if ((z[i] == 'a' || z[i] == 's' || z[i] == 'm' || z[i] == 'h' || z[i] == 'd' || z[i] == 'n' || z[i] == 'y' ||
           z[i] == 'w' || z[i] == 'A' || z[i] == 'S' || z[i] == 'M' || z[i] == 'H' || z[i] == 'D' || z[i] == 'N' ||
           z[i] == 'Y' || z[i] == 'W') &&
          (isIdChar[z[i + 1]] == 0)) {
        *tokenType = TK_VARIABLE;
        i += 1;
        return i;
      }

      int32_t seg = 1;
      while (z[i] == '.' && isdigit(z[i + 1])) {
        i += 2;
        while (isdigit(z[i])) {
          i++;
        }
        *tokenType = TK_FLOAT;
        seg++;
      }

      if (seg == 4) {  // ip address
        *tokenType = TK_IP;
        return i;
      }

      if ((z[i] == 'e' || z[i] == 'E') &&
          (isdigit(z[i + 1]) || ((z[i + 1] == '+' || z[i + 1] == '-') && isdigit(z[i + 2])))) {
        i += 2;
        while (isdigit(z[i])) {
          i++;
        }
        *tokenType = TK_FLOAT;
      }
      return i;
    }
    case '[': {
      for (i = 1; z[i] && z[i - 1] != ']'; i++) {
      }
      *tokenType = TK_ID;
      return i;
    }
    case 'T':
    case 't':
    case 'F':
    case 'f': {
      for (i = 1; ((z[i] & 0x80) == 0) && isIdChar[z[i]]; i++) {
      }

      if ((i == 4 && strncasecmp(z, "true", 4) == 0) || (i == 5 && strncasecmp(z, "false", 5) == 0)) {
        *tokenType = TK_BOOL;
        return i;
      }
    }
    default: {
      if (((*z & 0x80) != 0) || !isIdChar[*z]) {
        break;
      }
      for (i = 1; ((z[i] & 0x80) == 0) && isIdChar[z[i]]; i++) {
      }
      *tokenType = tSQLKeywordCode(z, i);
      return i;
    }
  }

  *tokenType = TK_ILLEGAL;
  return 0;
}

void tStrGetToken(char* str, int32_t* i, SSQLToken* t0, bool isPrevOptr) {
  // here we reach the end of sql string, null-terminated string
  if (str[*i] == 0) {
    t0->n = 0;
    return;
  }

  t0->n = tSQLGetToken(&str[*i], &t0->type);

  /* IGNORE all space between valid tokens */
  while (t0->type == TK_SPACE) {
    *i += t0->n;
    t0->n = tSQLGetToken(&str[*i], &t0->type);
  }

  /* support parse the -/+number format */
  if ((isPrevOptr) && (t0->type == TK_MINUS || t0->type == TK_PLUS)) {
    uint32_t type = 0;
    int32_t  len = tSQLGetToken(&str[*i + t0->n], &type);
    if (type == TK_INTEGER || type == TK_FLOAT) {
      t0->type = type;
      t0->n += len;
    }
  }

  t0->z = str + (*i);
  *i += t0->n;
}

bool isKeyWord(const char* z, int32_t len) {
  int32_t tokenType = tSQLKeywordCode((char*)z, len);
  return (tokenType != TK_ID);
}

bool isNumber(const SSQLToken* pToken) { return (pToken->type == TK_INTEGER || pToken->type == TK_FLOAT); }
