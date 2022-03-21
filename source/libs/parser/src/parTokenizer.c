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
#include "parToken.h"
#include "thash.h"
#include "taosdef.h"
#include "ttokendef.h"

// All the keywords of the SQL language are stored in a hash table
typedef struct SKeyword {
  const char* name;  // The keyword name
  uint16_t    type;  // type
  uint8_t     len;   // length
} SKeyword;

// keywords in sql string
static SKeyword keywordTable[] = {
    {"ACCOUNT",       TK_ACCOUNT},
    {"ALL",           TK_ALL},
    {"ALTER",         TK_ALTER},
    {"AND",           TK_AND},
    {"AS",            TK_AS},
    {"ASC",           TK_ASC},
    {"BETWEEN",       TK_BETWEEN},
    {"BINARY",        TK_BINARY},
    {"BIGINT",        TK_BIGINT},
    {"BLOCKS",        TK_BLOCKS},
    {"BOOL",          TK_BOOL},
    {"BY",            TK_BY},
    {"CACHE",         TK_CACHE},
    {"CACHELAST",     TK_CACHELAST},
    {"COMMENT",       TK_COMMENT},
    {"COMP",          TK_COMP},
    {"CREATE",        TK_CREATE},
    {"DATABASE",      TK_DATABASE},
    {"DATABASES",     TK_DATABASES},
    {"DAYS",          TK_DAYS},
    {"DESC",          TK_DESC},
    {"DISTINCT",      TK_DISTINCT},
    {"DNODE",         TK_DNODE},
    {"DNODES",        TK_DNODES},
    {"DOUBLE",        TK_DOUBLE},
    {"DROP",          TK_DROP},
    {"EXISTS",        TK_EXISTS},
    // {"FILE",          TK_FILE},
    {"FILL",          TK_FILL},
    {"FLOAT",         TK_FLOAT},
    {"FROM",          TK_FROM},
    {"FSYNC",         TK_FSYNC},
    {"FUNCTION",      TK_FUNCTION},
    {"GROUP",         TK_GROUP},
    {"HAVING",        TK_HAVING},
    {"IF",            TK_IF},
    {"IMPORT",        TK_IMPORT},
    {"IN",            TK_IN},
    {"INDEX",         TK_INDEX},
    {"INNER",         TK_INNER},
    {"INT",           TK_INT},
    {"INSERT",        TK_INSERT},
    {"INTEGER",       TK_INTEGER},
    {"INTERVAL",      TK_INTERVAL},
    {"INTO",          TK_INTO},
    {"IS",            TK_IS},
    {"JOIN",          TK_JOIN},
    {"JSON",          TK_JSON},
    {"KEEP",          TK_KEEP},
    {"LIKE",          TK_LIKE},
    {"LIMIT",         TK_LIMIT},
    {"LINEAR",        TK_LINEAR},
    {"MATCH",         TK_MATCH},
    {"MAXROWS",       TK_MAXROWS},
    {"MINROWS",       TK_MINROWS},
    {"MINUS",         TK_MINUS},
    {"MNODES",        TK_MNODES},
    {"NCHAR",         TK_NCHAR},
    {"NMATCH",        TK_NMATCH},
    {"NONE",          TK_NONE},
    {"NOT",           TK_NOT},
    {"NOW",           TK_NOW},
    {"NULL",          TK_NULL},
    {"OFFSET",        TK_OFFSET},
    {"ON",            TK_ON},
    {"OR",            TK_OR},
    {"ORDER",         TK_ORDER},
    {"PASS",          TK_PASS},
    {"PORT",          TK_PORT},
    {"PRECISION",     TK_PRECISION},
    {"PRIVILEGE",     TK_PRIVILEGE},
    {"PREV",          TK_PREV},
    {"QNODE",         TK_QNODE},
    {"QNODES",        TK_QNODES},
    {"QUORUM",        TK_QUORUM},
    {"REPLICA",       TK_REPLICA},
    {"SELECT",        TK_SELECT},
    {"SESSION",       TK_SESSION},
    {"SHOW",          TK_SHOW},
    {"SINGLE_STABLE", TK_SINGLE_STABLE},
    {"SLIDING",       TK_SLIDING},
    {"SLIMIT",        TK_SLIMIT},
    {"SMA",           TK_SMA},
    {"SMALLINT",      TK_SMALLINT},
    {"SOFFSET",       TK_SOFFSET},
    {"STABLE",        TK_STABLE},
    {"STABLES",       TK_STABLES},
    {"STATE_WINDOW",  TK_STATE_WINDOW},
    {"STREAM_MODE",   TK_STREAM_MODE},
    {"TABLE",         TK_TABLE},
    {"TABLES",        TK_TABLES},
    {"TAGS",          TK_TAGS},
    {"TIMESTAMP",     TK_TIMESTAMP},
    {"TINYINT",       TK_TINYINT},
    {"TOPIC",         TK_TOPIC},
    {"TTL",           TK_TTL},
    {"UNION",         TK_UNION},
    {"UNSIGNED",      TK_UNSIGNED},
    {"USE",           TK_USE},
    {"USER",          TK_USER},
    {"USERS",         TK_USERS},
    {"USING",         TK_USING},
    {"VALUES",        TK_VALUES},
    {"VARCHAR",       TK_VARCHAR},
    {"VGROUPS",       TK_VGROUPS},
    {"WAL",           TK_WAL},
    {"WHERE",         TK_WHERE},
    // {"ID",           TK_ID},
    // {"STRING",       TK_STRING},
    // {"EQ",           TK_EQ},
    // {"NE",           TK_NE},
    // {"ISNULL",       TK_ISNULL},
    // {"NOTNULL",      TK_NOTNULL},
    // {"GLOB",         TK_GLOB},
    // {"GT",           TK_GT},
    // {"GE",           TK_GE},
    // {"LT",           TK_LT},
    // {"LE",           TK_LE},
    // {"BITAND",       TK_BITAND},
    // {"BITOR",        TK_BITOR},
    // {"LSHIFT",       TK_LSHIFT},
    // {"RSHIFT",       TK_RSHIFT},
    // {"PLUS",         TK_PLUS},
    // {"DIVIDE",       TK_DIVIDE},
    // {"TIMES",        TK_TIMES},
    // {"STAR",         TK_STAR},
    // {"SLASH",        TK_SLASH},
    // {"REM ",         TK_REM},
    // {"||",           TK_CONCAT},
    // {"UMINUS",       TK_UMINUS},
    // {"UPLUS",        TK_UPLUS},
    // {"BITNOT",       TK_BITNOT},
    // {"ACCOUNTS",     TK_ACCOUNTS},
    // {"MODULES",      TK_MODULES},
    // {"QUERIES",      TK_QUERIES},
    // {"CONNECTIONS",  TK_CONNECTIONS},
    // {"STREAMS",      TK_STREAMS},
    // {"VARIABLES",    TK_VARIABLES},
    // {"SCORES",       TK_SCORES},
    // {"GRANTS",       TK_GRANTS},
    // {"DOT",          TK_DOT},
    // {"DESCRIBE",     TK_DESCRIBE},
    // {"SYNCDB",       TK_SYNCDB},
    // {"LOCAL",        TK_LOCAL},
    // {"PPS",          TK_PPS},
    // {"TSERIES",      TK_TSERIES},
    // {"DBS",          TK_DBS},
    // {"STORAGE",      TK_STORAGE},
    // {"QTIME",        TK_QTIME},
    // {"CONNS",        TK_CONNS},
    // {"STATE",        TK_STATE},
    // {"CTIME",        TK_CTIME},
    // {"LP",           TK_LP},
    // {"RP",           TK_RP},
    // {"COMMA",        TK_COMMA},
    // {"EVERY",        TK_EVERY},
    // {"VARIABLE",     TK_VARIABLE},
    // {"UPDATE",       TK_UPDATE},
    // {"RESET",        TK_RESET},
    // {"QUERY",        TK_QUERY},
    // {"ADD",          TK_ADD},
    // {"COLUMN",       TK_COLUMN},
    // {"TAG",          TK_TAG},
    // {"CHANGE",       TK_CHANGE},
    // {"SET",          TK_SET},
    // {"KILL",         TK_KILL},
    // {"CONNECTION",   TK_CONNECTION},
    // {"COLON",        TK_COLON},
    // {"STREAM",       TK_STREAM},
    // {"ABORT",        TK_ABORT},
    // {"AFTER",        TK_AFTER},
    // {"ATTACH",       TK_ATTACH},
    // {"BEFORE",       TK_BEFORE},
    // {"BEGIN",        TK_BEGIN},
    // {"CASCADE",      TK_CASCADE},
    // {"CLUSTER",      TK_CLUSTER},
    // {"CONFLICT",     TK_CONFLICT},
    // {"COPY",         TK_COPY},
    // {"DEFERRED",     TK_DEFERRED},
    // {"DELIMITERS",   TK_DELIMITERS},
    // {"DETACH",       TK_DETACH},
    // {"EACH",         TK_EACH},
    // {"END",          TK_END},
    // {"EXPLAIN",      TK_EXPLAIN},
    // {"FAIL",         TK_FAIL},
    // {"FOR",          TK_FOR},
    // {"IGNORE",       TK_IGNORE},
    // {"IMMEDIATE",    TK_IMMEDIATE},
    // {"INITIALLY",    TK_INITIALLY},
    // {"INSTEAD",      TK_INSTEAD},
    // {"KEY",          TK_KEY},
    // {"OF",           TK_OF},
    // {"RAISE",        TK_RAISE},
    // {"REPLACE",      TK_REPLACE},
    // {"RESTRICT",     TK_RESTRICT},
    // {"ROW",          TK_ROW},
    // {"STATEMENT",    TK_STATEMENT},
    // {"TRIGGER",      TK_TRIGGER},
    // {"VIEW",         TK_VIEW},
    // {"SEMI",         TK_SEMI},
    // {"TBNAME",       TK_TBNAME},
    // {"VNODES",       TK_VNODES},
//    {"PARTITIONS",   TK_PARTITIONS},
    // {"TOPICS",       TK_TOPICS},
    // {"COMPACT",      TK_COMPACT},
    // {"MODIFY",       TK_MODIFY},
    // {"FUNCTIONS",    TK_FUNCTIONS},
    // {"OUTPUTTYPE",   TK_OUTPUTTYPE},
    // {"AGGREGATE",    TK_AGGREGATE},
    // {"BUFSIZE",      TK_BUFSIZE},
    // {"MODE",         TK_MODE},
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

static TdThreadOnce keywordsHashTableInit = PTHREAD_ONCE_INIT;

static int32_t tKeywordCode(const char* z, int n) {
  taosThreadOnce(&keywordsHashTableInit, doInitKeywordsTable);
  
  char key[512] = {0};
  if (n > tListLen(key)) { // too long token, can not be any other token type
    return TK_NK_ID;
  }
  
  for (int32_t j = 0; j < n; ++j) {
    if (z[j] >= 'a' && z[j] <= 'z') {
      key[j] = (char)(z[j] & 0xDF);  // to uppercase and set the null-terminated
    } else {
      key[j] = z[j];
    }
  }

  if (keywordHashTable == NULL) {
    return TK_NK_ILLEGAL;
  }

  SKeyword** pKey = (SKeyword**)taosHashGet(keywordHashTable, key, n);
  return (pKey != NULL)? (*pKey)->type:TK_NK_ID;
}

/*
 * Return the length of the token that begins at z[0].
 * Store the token type in *type before returning.
 */
uint32_t tGetToken(const char* z, uint32_t* tokenId) {
  uint32_t i;
  switch (*z) {
    case ' ':
    case '\t':
    case '\n':
    case '\f':
    case '\r': {
      for (i = 1; isspace(z[i]); i++) {
      }
      *tokenId = TK_NK_SPACE;
      return i;
    }
    case ':': {
      *tokenId = TK_NK_COLON;
      return 1;
    }
    case '-': {
      if (z[1] == '-') {
        for (i = 2; z[i] && z[i] != '\n'; i++) {
        }
        *tokenId = TK_NK_COMMENT;
        return i;
      }
      *tokenId = TK_MINUS;
      return 1;
    }
    case '(': {
      *tokenId = TK_NK_LP;
      return 1;
    }
    case ')': {
      *tokenId = TK_NK_RP;
      return 1;
    }
    case ';': {
      *tokenId = TK_NK_SEMI;
      return 1;
    }
    case '+': {
      *tokenId = TK_NK_PLUS;
      return 1;
    }
    case '*': {
      *tokenId = TK_NK_STAR;
      return 1;
    }
    case '/': {
      if (z[1] != '*' || z[2] == 0) {
        *tokenId = TK_NK_SLASH;
        return 1;
      }
      for (i = 3; z[i] && (z[i] != '/' || z[i - 1] != '*'); i++) {
      }
      if (z[i]) i++;
      *tokenId = TK_NK_COMMENT;
      return i;
    }
    case '%': {
      *tokenId = TK_NK_REM;
      return 1;
    }
    case '=': {
      *tokenId = TK_NK_EQ;
      return 1 + (z[1] == '=');
    }
    case '<': {
      if (z[1] == '=') {
        *tokenId = TK_NK_LE;
        return 2;
      } else if (z[1] == '>') {
        *tokenId = TK_NK_NE;
        return 2;
      } else if (z[1] == '<') {
        *tokenId = TK_NK_LSHIFT;
        return 2;
      } else {
        *tokenId = TK_NK_LT;
        return 1;
      }
    }
    case '>': {
      if (z[1] == '=') {
        *tokenId = TK_NK_GE;
        return 2;
      } else if (z[1] == '>') {
        *tokenId = TK_NK_RSHIFT;
        return 2;
      } else {
        *tokenId = TK_NK_GT;
        return 1;
      }
    }
    case '!': {
      if (z[1] != '=') {
        *tokenId = TK_NK_ILLEGAL;
        return 2;
      } else {
        *tokenId = TK_NK_NE;
        return 2;
      }
    }
    case '|': {
      if (z[1] != '|') {
        *tokenId = TK_NK_BITOR;
        return 1;
      } else {
        *tokenId = TK_NK_CONCAT;
        return 2;
      }
    }
    case ',': {
      *tokenId = TK_NK_COMMA;
      return 1;
    }
    case '&': {
      *tokenId = TK_NK_BITAND;
      return 1;
    }
    case '~': {
      *tokenId = TK_NK_BITNOT;
      return 1;
    }
    case '?': {
      *tokenId = TK_NK_QUESTION;
      return 1;
    }
    case '`':
    case '\'':
    case '"': {
      int  delim = z[0];
      bool strEnd = false;
      for (i = 1; z[i]; i++) {
        if (z[i] == '\\') {   // ignore the escaped character that follows this backslash
          i++;
          continue;
        }
        
        if (z[i] == delim) {
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
        *tokenId = (delim == '`')? TK_NK_ID:TK_NK_STRING;
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

        *tokenId = TK_NK_FLOAT;
        return i;
      } else {
        *tokenId = TK_NK_DOT;
        return 1;
      }
    }

    case '0': {
      char next = z[1];

      if (next == 'b') { // bin number
        *tokenId = TK_NK_BIN;
        for (i = 2; (z[i] == '0' || z[i] == '1'); ++i) {
        }

        if (i == 2) {
          break;
        }

        return i;
      } else if (next == 'x') {  //hex number
        *tokenId = TK_NK_HEX;
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
      *tokenId = TK_NK_INTEGER;
      for (i = 1; isdigit(z[i]); i++) {
      }

      /* here is the 1u/1a/2s/3m/9y */
      if ((z[i] == 'b' || z[i] == 'u' || z[i] == 'a' || z[i] == 's' || z[i] == 'm' || z[i] == 'h' || z[i] == 'd' || z[i] == 'n' ||
           z[i] == 'y' || z[i] == 'w' ||
           z[i] == 'B' || z[i] == 'U' || z[i] == 'A' || z[i] == 'S' || z[i] == 'M' || z[i] == 'H' || z[i] == 'D' || z[i] == 'N' ||
           z[i] == 'Y' || z[i] == 'W') &&
          (isIdChar[(uint8_t)z[i + 1]] == 0)) {
        *tokenId = TK_NK_VARIABLE;
        i += 1;
        return i;
      }

      int32_t seg = 1;
      while (z[i] == '.' && isdigit(z[i + 1])) {
        i += 2;
        while (isdigit(z[i])) {
          i++;
        }
        *tokenId = TK_NK_FLOAT;
        seg++;
      }

      if (seg == 4) {  // ip address
        *tokenId = TK_NK_IPTOKEN;
        return i;
      }

      if ((z[i] == 'e' || z[i] == 'E') &&
          (isdigit(z[i + 1]) || ((z[i + 1] == '+' || z[i + 1] == '-') && isdigit(z[i + 2])))) {
        i += 2;
        while (isdigit(z[i])) {
          i++;
        }
        *tokenId = TK_NK_FLOAT;
      }
      return i;
    }
    case '[': {
      for (i = 1; z[i] && z[i - 1] != ']'; i++) {
      }
      *tokenId = TK_NK_ID;
      return i;
    }
    case 'T':
    case 't':
    case 'F':
    case 'f': {
      for (i = 1; ((z[i] & 0x80) == 0) && isIdChar[(uint8_t) z[i]]; i++) {
      }

      if ((i == 4 && strncasecmp(z, "true", 4) == 0) || (i == 5 && strncasecmp(z, "false", 5) == 0)) {
        *tokenId = TK_NK_BOOL;
        return i;
      }
    }
    default: {
      if (((*z & 0x80) != 0) || !isIdChar[(uint8_t) *z]) {
        break;
      }
      for (i = 1; ((z[i] & 0x80) == 0) && isIdChar[(uint8_t) z[i]]; i++) {
      }
      *tokenId = tKeywordCode(z, i);
      return i;
    }
  }

  *tokenId = TK_NK_ILLEGAL;
  return 0;
}

SToken tscReplaceStrToken(char **str, SToken *token, const char* newToken) {
  char *src = *str;
  size_t nsize = strlen(newToken);
  int32_t size = (int32_t)strlen(*str) - token->n + (int32_t)nsize + 1;
  int32_t bsize = (int32_t)((uint64_t)token->z - (uint64_t)src);
  SToken ntoken;

  *str = calloc(1, size);

  strncpy(*str, src, bsize);
  strcat(*str, newToken);
  strcat(*str, token->z + token->n);

  ntoken.n = (uint32_t)nsize;
  ntoken.z = *str + bsize;

  tfree(src);

  return ntoken;
}

SToken tStrGetToken(const char* str, int32_t* i, bool isPrevOptr) {
  SToken t0 = {0};

  // here we reach the end of sql string, null-terminated string
  if (str[*i] == 0) {
    t0.n = 0;
    return t0;
  }

  // IGNORE TK_NK_SPACE, TK_NK_COMMA, and specified tokens
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

    t0.n = tGetToken(&str[*i], &t0.type);
    break;

    // not support user specfied ignored symbol list
#if 0
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
#endif
  }

  if (t0.type == TK_NK_SEMI) {
    t0.n = 0;
    return t0;
  }

  uint32_t type = 0;
  int32_t  len;

  // support parse the 'db.tbl' format, notes: There should be no space on either side of the dot!
  if ('.' == str[*i + t0.n]) {
    len = tGetToken(&str[*i + t0.n + 1], &type);

    // only id and string are valid
    if ((TK_NK_STRING != t0.type) && (TK_NK_ID != t0.type)) {
      t0.type = TK_NK_ILLEGAL;
      t0.n = 0;

      return t0;
    }

    t0.n += len + 1;

  } else {
    // support parse the -/+number format
    if ((isPrevOptr) && (t0.type == TK_MINUS || t0.type == TK_NK_PLUS)) {
      len = tGetToken(&str[*i + t0.n], &type);
      if (type == TK_NK_INTEGER || type == TK_NK_FLOAT) {
        t0.type = type;
        t0.n += len;
      }
    }
  }

  t0.z = (char*) str + (*i);
  *i += t0.n;

  return t0;
}

bool taosIsKeyWordToken(const char* z, int32_t len) {
  return (tKeywordCode((char*)z, len) != TK_NK_ID);
}

void taosCleanupKeywordsTable() {
  void* m = keywordHashTable;
  if (m != NULL && atomic_val_compare_exchange_ptr(&keywordHashTable, m, 0) == m) {
    taosHashCleanup(m);
  }
}

SToken taosTokenDup(SToken* pToken, char* buf, int32_t len) {
  assert(pToken != NULL && buf != NULL && len > pToken->n);
  
  strncpy(buf, pToken->z, pToken->n);
  buf[pToken->n] = 0;

  SToken token = *pToken;
  token.z = buf;
  return token;
}
