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

#include "os.h"

#include "hash.h"
#include "hashfunc.h"
#include "taosdef.h"
#include "tstoken.h"
#include "ttokendef.h"
#include "tutil.h"

// All the keywords of the SQL language are stored in a hash table
typedef struct SKeyword {
  const char* name;  // The keyword name
  uint16_t    type;  // type
  uint8_t     len;   // length
} SKeyword;

// keywords in sql string
static SKeyword keywordTable[] = {
    {"ID",           TK_ID},
    {"BOOL",         TK_BOOL},
    {"TINYINT",      TK_TINYINT},
    {"SMALLINT",     TK_SMALLINT},
    {"INTEGER",      TK_INTEGER},
    {"INT",          TK_INTEGER},
    {"BIGINT",       TK_BIGINT},
    {"FLOAT",        TK_FLOAT},
    {"DOUBLE",       TK_DOUBLE},
    {"STRING",       TK_STRING},
    {"TIMESTAMP",    TK_TIMESTAMP},
    {"BINARY",       TK_BINARY},
    {"NCHAR",        TK_NCHAR},
    {"OR",           TK_OR},
    {"AND",          TK_AND},
    {"NOT",          TK_NOT},
    {"EQ",           TK_EQ},
    {"NE",           TK_NE},
    {"ISNULL",       TK_ISNULL},
    {"NOTNULL",      TK_NOTNULL},
    {"IS",           TK_IS},
    {"LIKE",         TK_LIKE},
    {"GLOB",         TK_GLOB},
    {"BETWEEN",      TK_BETWEEN},
    {"IN",           TK_IN},
    {"GT",           TK_GT},
    {"GE",           TK_GE},
    {"LT",           TK_LT},
    {"LE",           TK_LE},
    {"BITAND",       TK_BITAND},
    {"BITOR",        TK_BITOR},
    {"LSHIFT",       TK_LSHIFT},
    {"RSHIFT",       TK_RSHIFT},
    {"PLUS",         TK_PLUS},
    {"MINUS",        TK_MINUS},
    {"DIVIDE",       TK_DIVIDE},
    {"TIMES",        TK_TIMES},
    {"STAR",         TK_STAR},
    {"SLASH",        TK_SLASH},
    {"REM ",         TK_REM},
    {"CONCAT",       TK_CONCAT},
    {"UMINUS",       TK_UMINUS},
    {"UPLUS",        TK_UPLUS},
    {"BITNOT",       TK_BITNOT},
    {"SHOW",         TK_SHOW},
    {"DATABASES",    TK_DATABASES},
    {"MNODES",       TK_MNODES},
    {"DNODES",       TK_DNODES},
    {"ACCOUNTS",     TK_ACCOUNTS},
    {"USERS",        TK_USERS},
    {"MODULES",      TK_MODULES},
    {"QUERIES",      TK_QUERIES},
    {"CONNECTIONS",  TK_CONNECTIONS},
    {"STREAMS",      TK_STREAMS},
    {"VARIABLES",    TK_VARIABLES},
    {"SCORES",       TK_SCORES},
    {"GRANTS",       TK_GRANTS},
    {"DOT",          TK_DOT},
    {"TABLES",       TK_TABLES},
    {"STABLES",      TK_STABLES},
    {"VGROUPS",      TK_VGROUPS},
    {"DROP",         TK_DROP},
    {"TABLE",        TK_TABLE},
    {"DATABASE",     TK_DATABASE},
    {"DNODE",        TK_DNODE},
    {"USER",         TK_USER},
    {"ACCOUNT",      TK_ACCOUNT},
    {"USE",          TK_USE},
    {"DESCRIBE",     TK_DESCRIBE},
    {"ALTER",        TK_ALTER},
    {"PASS",         TK_PASS},
    {"PRIVILEGE",    TK_PRIVILEGE},
    {"LOCAL",        TK_LOCAL},
    {"IF",           TK_IF},
    {"EXISTS",       TK_EXISTS},
    {"CREATE",       TK_CREATE},
    {"PPS",          TK_PPS},
    {"TSERIES",      TK_TSERIES},
    {"DBS",          TK_DBS},
    {"STORAGE",      TK_STORAGE},
    {"QTIME",        TK_QTIME},
    {"CONNS",        TK_CONNS},
    {"STATE",        TK_STATE},
    {"KEEP",         TK_KEEP},
    {"REPLICA",      TK_REPLICA},
    {"QUORUM",       TK_QUORUM},
    {"DAYS",         TK_DAYS},
    {"MINROWS",      TK_MINROWS},
    {"MAXROWS",      TK_MAXROWS},
    {"BLOCKS",       TK_BLOCKS},
    {"CACHE",        TK_CACHE},
    {"CTIME",        TK_CTIME},
    {"WAL",          TK_WAL},
    {"FSYNC",        TK_FSYNC},
    {"COMP",         TK_COMP},
    {"PRECISION",    TK_PRECISION},
    {"LP",           TK_LP},
    {"RP",           TK_RP},
    {"UNSIGNED",     TK_UNSIGNED},
    {"TAGS",         TK_TAGS},
    {"USING",        TK_USING},
    {"AS",           TK_AS},
    {"COMMA",        TK_COMMA},
    {"NULL",         TK_NULL},
    {"SELECT",       TK_SELECT},
    {"FROM",         TK_FROM},
    {"VARIABLE",     TK_VARIABLE},
    {"INTERVAL",     TK_INTERVAL},
    {"SESSION",      TK_SESSION},
    {"FILL",         TK_FILL},
    {"SLIDING",      TK_SLIDING},
    {"ORDER",        TK_ORDER},
    {"BY",           TK_BY},
    {"ASC",          TK_ASC},
    {"DESC",         TK_DESC},
    {"GROUP",        TK_GROUP},
    {"HAVING",       TK_HAVING},
    {"LIMIT",        TK_LIMIT},
    {"OFFSET",       TK_OFFSET},
    {"SLIMIT",       TK_SLIMIT},
    {"SOFFSET",      TK_SOFFSET},
    {"WHERE",        TK_WHERE},
    {"NOW",          TK_NOW},
    {"INSERT",       TK_INSERT},
    {"INTO",         TK_INTO},
    {"VALUES",       TK_VALUES},
    {"UPDATE",       TK_UPDATE},
    {"RESET",        TK_RESET},
    {"QUERY",        TK_QUERY},
    {"ADD",          TK_ADD},
    {"COLUMN",       TK_COLUMN},
    {"TAG",          TK_TAG},
    {"CHANGE",       TK_CHANGE},
    {"SET",          TK_SET},
    {"KILL",         TK_KILL},
    {"CONNECTION",   TK_CONNECTION},
    {"COLON",        TK_COLON},
    {"STREAM",       TK_STREAM},
    {"ABORT",        TK_ABORT},
    {"AFTER",        TK_AFTER},
    {"ATTACH",       TK_ATTACH},
    {"BEFORE",       TK_BEFORE},
    {"BEGIN",        TK_BEGIN},
    {"CASCADE",      TK_CASCADE},
    {"CLUSTER",      TK_CLUSTER},
    {"CONFLICT",     TK_CONFLICT},
    {"COPY",         TK_COPY},
    {"DEFERRED",     TK_DEFERRED},
    {"DELIMITERS",   TK_DELIMITERS},
    {"DETACH",       TK_DETACH},
    {"EACH",         TK_EACH},
    {"END",          TK_END},
    {"EXPLAIN",      TK_EXPLAIN},
    {"FAIL",         TK_FAIL},
    {"FOR",          TK_FOR},
    {"IGNORE",       TK_IGNORE},
    {"IMMEDIATE",    TK_IMMEDIATE},
    {"INITIALLY",    TK_INITIALLY},
    {"INSTEAD",      TK_INSTEAD},
    {"MATCH",        TK_MATCH},
    {"KEY",          TK_KEY},
    {"OF",           TK_OF},
    {"RAISE",        TK_RAISE},
    {"REPLACE",      TK_REPLACE},
    {"RESTRICT",     TK_RESTRICT},
    {"ROW",          TK_ROW},
    {"STATEMENT",    TK_STATEMENT},
    {"TRIGGER",      TK_TRIGGER},
    {"VIEW",         TK_VIEW},
    {"ALL",          TK_ALL},
    {"SEMI",         TK_SEMI},
    {"NONE",         TK_NONE},
    {"PREV",         TK_PREV},
    {"LINEAR",       TK_LINEAR},
    {"IMPORT",       TK_IMPORT},
    {"METRIC",       TK_METRIC},
    {"TBNAME",       TK_TBNAME},
    {"JOIN",         TK_JOIN},
    {"METRICS",      TK_METRICS},
    {"STABLE",       TK_STABLE},
    {"FILE",         TK_FILE},
    {"VNODES",       TK_VNODES},
    {"UNION",        TK_UNION},
    {"CACHELAST",    TK_CACHELAST},
    {"DISTINCT",     TK_DISTINCT},
    {"PARTITIONS",   TK_PARTITIONS},
    {"TOPIC",        TK_TOPIC},
    {"TOPICS",       TK_TOPICS}
};

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

static void* keywordHashTable = NULL;

static void doInitKeywordsTable(void) {
  int numOfEntries = tListLen(keywordTable);
  
  keywordHashTable = taosHashInit(numOfEntries, MurmurHash3_32, true, false);
  for (int32_t i = 0; i < numOfEntries; i++) {
    keywordTable[i].len = (uint8_t)strlen(keywordTable[i].name);
    void* ptr = &keywordTable[i];
    taosHashPut(keywordHashTable, keywordTable[i].name, keywordTable[i].len, (void*)&ptr, POINTER_BYTES);
  }
}

static pthread_once_t keywordsHashTableInit = PTHREAD_ONCE_INIT;

int tSQLKeywordCode(const char* z, int n) {
  pthread_once(&keywordsHashTableInit, doInitKeywordsTable);
  
  char key[512] = {0};
  if (n > tListLen(key)) { // too long token, can not be any other token type
    return TK_ID;
  }
  
  for (int32_t j = 0; j < n; ++j) {
    if (z[j] >= 'a' && z[j] <= 'z') {
      key[j] = (char)(z[j] & 0xDF);  // to uppercase and set the null-terminated
    } else {
      key[j] = z[j];
    }
  }

  SKeyword** pKey = (SKeyword**)taosHashGet(keywordHashTable, key, n);
  return (pKey != NULL)? (*pKey)->type:TK_ID;
}

/*
 * Return the length of the token that begins at z[0].
 * Store the token type in *type before returning.
 */
uint32_t tSQLGetToken(char* z, uint32_t* tokenId) {
  uint32_t i;
  switch (*z) {
    case ' ':
    case '\t':
    case '\n':
    case '\f':
    case '\r': {
      for (i = 1; isspace(z[i]); i++) {
      }
      *tokenId = TK_SPACE;
      return i;
    }
    case ':': {
      *tokenId = TK_COLON;
      return 1;
    }
    case '-': {
      if (z[1] == '-') {
        for (i = 2; z[i] && z[i] != '\n'; i++) {
        }
        *tokenId = TK_COMMENT;
        return i;
      }
      *tokenId = TK_MINUS;
      return 1;
    }
    case '(': {
      *tokenId = TK_LP;
      return 1;
    }
    case ')': {
      *tokenId = TK_RP;
      return 1;
    }
    case ';': {
      *tokenId = TK_SEMI;
      return 1;
    }
    case '+': {
      *tokenId = TK_PLUS;
      return 1;
    }
    case '*': {
      *tokenId = TK_STAR;
      return 1;
    }
    case '/': {
      if (z[1] != '*' || z[2] == 0) {
        *tokenId = TK_SLASH;
        return 1;
      }
      for (i = 3; z[i] && (z[i] != '/' || z[i - 1] != '*'); i++) {
      }
      if (z[i]) i++;
      *tokenId = TK_COMMENT;
      return i;
    }
    case '%': {
      *tokenId = TK_REM;
      return 1;
    }
    case '=': {
      *tokenId = TK_EQ;
      return 1 + (z[1] == '=');
    }
    case '<': {
      if (z[1] == '=') {
        *tokenId = TK_LE;
        return 2;
      } else if (z[1] == '>') {
        *tokenId = TK_NE;
        return 2;
      } else if (z[1] == '<') {
        *tokenId = TK_LSHIFT;
        return 2;
      } else {
        *tokenId = TK_LT;
        return 1;
      }
    }
    case '>': {
      if (z[1] == '=') {
        *tokenId = TK_GE;
        return 2;
      } else if (z[1] == '>') {
        *tokenId = TK_RSHIFT;
        return 2;
      } else {
        *tokenId = TK_GT;
        return 1;
      }
    }
    case '!': {
      if (z[1] != '=') {
        *tokenId = TK_ILLEGAL;
        return 2;
      } else {
        *tokenId = TK_NE;
        return 2;
      }
    }
    case '|': {
      if (z[1] != '|') {
        *tokenId = TK_BITOR;
        return 1;
      } else {
        *tokenId = TK_CONCAT;
        return 2;
      }
    }
    case ',': {
      *tokenId = TK_COMMA;
      return 1;
    }
    case '&': {
      *tokenId = TK_BITAND;
      return 1;
    }
    case '~': {
      *tokenId = TK_BITNOT;
      return 1;
    }
    case '?': {
      *tokenId = TK_QUESTION;
      return 1;
    }
    case '\'':
    case '"': {
      int  delim = z[0];
      bool strEnd = false;
      for (i = 1; z[i]; i++) {
        if (z[i] == '\\') { 
          i++;
          continue;
        }
        
        if (z[i] == delim ) {
          if (z[i + 1] == delim) {
            i++;
          } else {
            strEnd = true;
            break;
          }
        }
      }
      
      if (z[i]) i++;

      if (strEnd) {
        *tokenId = TK_STRING;
        return i;
      }

      break;
    }
    case '.': {
      /*
       * handle the the float number with out integer part
       * .123
       * .123e4
       */
      if (isdigit(z[1])) {
        for (i = 2; isdigit(z[i]); i++) {
        }

        if ((z[i] == 'e' || z[i] == 'E') &&
            (isdigit(z[i + 1]) || ((z[i + 1] == '+' || z[i + 1] == '-') && isdigit(z[i + 2])))) {
          i += 2;
          while (isdigit(z[i])) {
            i++;
          }
        }

        *tokenId = TK_FLOAT;
        return i;
      } else {
        *tokenId = TK_DOT;
        return 1;
      }
    }

    case '0': {
      char next = z[1];

      if (next == 'b') { // bin number
        *tokenId = TK_BIN;
        for (i = 2; (z[i] == '0' || z[i] == '1'); ++i) {
        }

        if (i == 2) {
          break;
        }

        return i;
      } else if (next == 'x') {  //hex number
        *tokenId = TK_HEX;
        for (i = 2; isdigit(z[i]) || (z[i] >= 'a' && z[i] <= 'f') || (z[i] >= 'A' && z[i] <= 'F'); ++i) {
        }

        if (i == 2) {
          break;
        }

        return i;
      }
    }
    case '1':
    case '2':
    case '3':
    case '4':
    case '5':
    case '6':
    case '7':
    case '8':
    case '9': {
      *tokenId = TK_INTEGER;
      for (i = 1; isdigit(z[i]); i++) {
      }

      /* here is the 1u/1a/2s/3m/9y */
      if ((z[i] == 'u' || z[i] == 'a' || z[i] == 's' || z[i] == 'm' || z[i] == 'h' || z[i] == 'd' || z[i] == 'n' ||
           z[i] == 'y' || z[i] == 'w' ||
           z[i] == 'U' || z[i] == 'A' || z[i] == 'S' || z[i] == 'M' || z[i] == 'H' || z[i] == 'D' || z[i] == 'N' ||
           z[i] == 'Y' || z[i] == 'W') &&
          (isIdChar[(uint8_t)z[i + 1]] == 0)) {
        *tokenId = TK_VARIABLE;
        i += 1;
        return i;
      }

      int32_t seg = 1;
      while (z[i] == '.' && isdigit(z[i + 1])) {
        i += 2;
        while (isdigit(z[i])) {
          i++;
        }
        *tokenId = TK_FLOAT;
        seg++;
      }

      if (seg == 4) {  // ip address
        *tokenId = TK_IPTOKEN;
        return i;
      }

      if ((z[i] == 'e' || z[i] == 'E') &&
          (isdigit(z[i + 1]) || ((z[i + 1] == '+' || z[i + 1] == '-') && isdigit(z[i + 2])))) {
        i += 2;
        while (isdigit(z[i])) {
          i++;
        }
        *tokenId = TK_FLOAT;
      }
      return i;
    }
    case '[': {
      for (i = 1; z[i] && z[i - 1] != ']'; i++) {
      }
      *tokenId = TK_ID;
      return i;
    }
    case 'T':
    case 't':
    case 'F':
    case 'f': {
      for (i = 1; ((z[i] & 0x80) == 0) && isIdChar[(uint8_t) z[i]]; i++) {
      }

      if ((i == 4 && strncasecmp(z, "true", 4) == 0) || (i == 5 && strncasecmp(z, "false", 5) == 0)) {
        *tokenId = TK_BOOL;
        return i;
      }
    }
    default: {
      if (((*z & 0x80) != 0) || !isIdChar[(uint8_t) *z]) {
        break;
      }
      for (i = 1; ((z[i] & 0x80) == 0) && isIdChar[(uint8_t) z[i]]; i++) {
      }
      *tokenId = tSQLKeywordCode(z, i);
      return i;
    }
  }

  *tokenId = TK_ILLEGAL;
  return 0;
}

SStrToken tStrGetToken(char* str, int32_t* i, bool isPrevOptr, uint32_t numOfIgnoreToken, uint32_t* ignoreTokenTypes) {
  SStrToken t0 = {0};

  // here we reach the end of sql string, null-terminated string
  if (str[*i] == 0) {
    t0.n = 0;
    return t0;
  }

  // IGNORE TK_SPACE, TK_COMMA, and specified tokens
  while (1) {
    *i += t0.n;

    int32_t numOfComma = 0;
    char t = str[*i];
    while (t == ' ' || t == '\n' || t == '\r' || t == '\t' || t == '\f' || t == ',') {
      if (t == ',' && (++numOfComma > 1)) {  // comma only allowed once
        t0.n = 0;
        return t0;
      }
    
      t = str[++(*i)];
    }

    t0.n = tSQLGetToken(&str[*i], &t0.type);

    bool ignore = false;
    for (uint32_t k = 0; k < numOfIgnoreToken; k++) {
      if (t0.type == ignoreTokenTypes[k]) {
        ignore = true;
        break;
      }
    }

    if (!ignore) {
      break;
    }
  }

  if (t0.type == TK_SEMI) {
    t0.n = 0;
    return t0;
  }

  uint32_t type = 0;
  int32_t  len;

  // support parse the 'db.tbl' format, notes: There should be no space on either side of the dot!
  if ('.' == str[*i + t0.n]) {
    len = tSQLGetToken(&str[*i + t0.n + 1], &type);

    // only id and string are valid
    if ((TK_STRING != t0.type) && (TK_ID != t0.type)) {
      t0.type = TK_ILLEGAL;
      t0.n = 0;

      return t0;
    }

    t0.n += len + 1;

  } else {
    // support parse the -/+number format
    if ((isPrevOptr) && (t0.type == TK_MINUS || t0.type == TK_PLUS)) {
      len = tSQLGetToken(&str[*i + t0.n], &type);
      if (type == TK_INTEGER || type == TK_FLOAT) {
        t0.type = type;
        t0.n += len;
      }
    }
  }

  t0.z = str + (*i);
  *i += t0.n;

  return t0;
}

bool isKeyWord(const char* z, int32_t len) { return (tSQLKeywordCode((char*)z, len) != TK_ID); }

void taosCleanupKeywordsTable() {
  void* m = keywordHashTable;
  if (m != NULL && atomic_val_compare_exchange_ptr(&keywordHashTable, m, 0) == m) {
    taosHashCleanup(m);
  }
}
