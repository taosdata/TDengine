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
#include "taosdef.h"
#include "thash.h"
#include "ttokendef.h"

// All the keywords of the SQL language are stored in a hash table
typedef struct SKeyword {
  const char* name;  // The keyword name
  uint16_t    type;  // type
  uint8_t     len;   // length
} SKeyword;

// clang-format off
// keywords in sql string
static SKeyword keywordTable[] = {
    {"ACCOUNT",              TK_ACCOUNT},
    {"ACCOUNTS",             TK_ACCOUNTS},
    {"ADD",                  TK_ADD},
    {"AGGREGATE",            TK_AGGREGATE},
    {"ALL",                  TK_ALL},
    {"ALTER",                TK_ALTER},
    {"ANALYZE",              TK_ANALYZE},
    {"AND",                  TK_AND},
    {"APPS",                 TK_APPS},
    {"AS",                   TK_AS},
    {"ASC",                  TK_ASC},
    {"AT_ONCE",              TK_AT_ONCE},
    {"BALANCE",              TK_BALANCE},
    {"BATCH_SCAN",           TK_BATCH_SCAN},
    {"BETWEEN",              TK_BETWEEN},
    {"BIGINT",               TK_BIGINT},
    {"BINARY",               TK_BINARY},
    {"BNODE",                TK_BNODE},
    {"BNODES",               TK_BNODES},
    {"BOOL",                 TK_BOOL},
    {"BUFFER",               TK_BUFFER},
    {"BUFSIZE",              TK_BUFSIZE},
    {"BY",                   TK_BY},
    {"CACHE",                TK_CACHE},
    {"CACHEMODEL",           TK_CACHEMODEL},
    {"CACHESIZE",            TK_CACHESIZE},
    {"CASE",                 TK_CASE},
    {"CAST",                 TK_CAST},
    {"CHILD",                TK_CHILD},
    {"CLIENT_VERSION",       TK_CLIENT_VERSION},
    {"CLUSTER",              TK_CLUSTER},
    {"COLUMN",               TK_COLUMN},
    {"COMMENT",              TK_COMMENT},
    {"COMP",                 TK_COMP},
    {"COMPACT",              TK_COMPACT},
    {"COMPACTS",             TK_COMPACTS},
    {"CONNECTION",           TK_CONNECTION},
    {"CONNECTIONS",          TK_CONNECTIONS},
    {"CONNS",                TK_CONNS},
    {"CONSUMER",             TK_CONSUMER},
    {"CONSUMERS",            TK_CONSUMERS},
    {"CONTAINS",             TK_CONTAINS},
    {"COUNT",                TK_COUNT},
    {"COUNT_WINDOW",         TK_COUNT_WINDOW},
    {"CREATE",               TK_CREATE},
    {"CURRENT_USER",         TK_CURRENT_USER},
    {"DATABASE",             TK_DATABASE},
    {"DATABASES",            TK_DATABASES},
    {"DBS",                  TK_DBS},
    {"DELETE",               TK_DELETE},
    {"DELETE_MARK",          TK_DELETE_MARK},
    {"DESC",                 TK_DESC},
    {"DESCRIBE",             TK_DESCRIBE},
    {"DISTINCT",             TK_DISTINCT},
    {"DISTRIBUTED",          TK_DISTRIBUTED},
    {"DNODE",                TK_DNODE},
    {"DNODES",               TK_DNODES},
    {"DOUBLE",               TK_DOUBLE},
    {"DROP",                 TK_DROP},
    {"DURATION",             TK_DURATION},
    {"ELSE",                 TK_ELSE},
    {"ENABLE",               TK_ENABLE},
    {"END",                  TK_END},
    {"EXISTS",               TK_EXISTS},
    {"EXPIRED",              TK_EXPIRED},
    {"EXPLAIN",              TK_EXPLAIN},
    {"EVENT_WINDOW",         TK_EVENT_WINDOW},
    {"EVERY",                TK_EVERY},
    {"FILE",                 TK_FILE},
    {"FILL",                 TK_FILL},
    {"FILL_HISTORY",         TK_FILL_HISTORY},
    {"FIRST",                TK_FIRST},
    {"FLOAT",                TK_FLOAT},
    {"FLUSH",                TK_FLUSH},
    {"FROM",                 TK_FROM},
    {"FORCE",                TK_FORCE},
    {"FUNCTION",             TK_FUNCTION},
    {"FUNCTIONS",            TK_FUNCTIONS},
    {"GEOMETRY",             TK_GEOMETRY},
    {"GRANT",                TK_GRANT},
    {"GRANTS",               TK_GRANTS},
    {"FULL",                 TK_FULL},
    {"LOGS",                 TK_LOGS},
    {"MACHINES",             TK_MACHINES},
    {"GROUP",                TK_GROUP},
    {"HAVING",               TK_HAVING},
    {"HOST",                 TK_HOST},
    {"IF",                   TK_IF},
    {"IGNORE",               TK_IGNORE},
    {"IMPORT",               TK_IMPORT},
    {"IN",                   TK_IN},
    {"INDEX",                TK_INDEX},
    {"INDEXES",              TK_INDEXES},
    {"INNER",                TK_INNER},
    {"INSERT",               TK_INSERT},
    {"INT",                  TK_INT},
    {"INTEGER",              TK_INTEGER},
    {"INTERVAL",             TK_INTERVAL},
    {"INTO",                 TK_INTO},
    {"IS",                   TK_IS},
    {"JOIN",                 TK_JOIN},
    {"JSON",                 TK_JSON},
    {"KEEP",                 TK_KEEP},
    {"KILL",                 TK_KILL},
    {"LANGUAGE",             TK_LANGUAGE},
    {"LAST",                 TK_LAST},
    {"LAST_ROW",             TK_LAST_ROW},
    {"LEADER",               TK_LEADER},
    {"LICENCES",             TK_LICENCES},
    {"LIKE",                 TK_LIKE},
    {"LIMIT",                TK_LIMIT},
    {"LINEAR",               TK_LINEAR},
    {"LOCAL",                TK_LOCAL},
    {"MATCH",                TK_MATCH},
    {"MAXROWS",              TK_MAXROWS},
    {"MAX_DELAY",            TK_MAX_DELAY},
    {"BWLIMIT",              TK_BWLIMIT},
    {"MERGE",                TK_MERGE},
    {"META",                 TK_META},
    {"ONLY",                 TK_ONLY},
    {"MINROWS",              TK_MINROWS},
    {"MINUS",                TK_MINUS},
    {"MNODE",                TK_MNODE},
    {"MNODES",               TK_MNODES},
    {"MODIFY",               TK_MODIFY},
    {"MODULES",              TK_MODULES},
    {"NORMAL",               TK_NORMAL},
    {"NCHAR",                TK_NCHAR},
    {"NEXT",                 TK_NEXT},
    {"NMATCH",               TK_NMATCH},
    {"NONE",                 TK_NONE},
    {"NOT",                  TK_NOT},
    {"NOW",                  TK_NOW},
    {"NO_BATCH_SCAN",        TK_NO_BATCH_SCAN},
    {"NULL",                 TK_NULL},
    {"NULL_F",               TK_NULL_F},
    {"NULLS",                TK_NULLS},
    {"OFFSET",               TK_OFFSET},
    {"ON",                   TK_ON},
    {"OR",                   TK_OR},
    {"ORDER",                TK_ORDER},
    {"OUTPUTTYPE",           TK_OUTPUTTYPE},
    {"PAGES",                TK_PAGES},
    {"PAGESIZE",             TK_PAGESIZE},
    {"PARA_TABLES_SORT",     TK_PARA_TABLES_SORT},
    {"PARTITION",            TK_PARTITION},
    {"PARTITION_FIRST",      TK_PARTITION_FIRST},
    {"PASS",                 TK_PASS},
    {"PORT",                 TK_PORT},
    {"PPS",                  TK_PPS},
    {"PRECISION",            TK_PRECISION},
    {"PREV",                 TK_PREV},
    {"PRIVILEGES",           TK_PRIVILEGES},
    {"QNODE",                TK_QNODE},
    {"QNODES",               TK_QNODES},
    {"QTIME",                TK_QTIME},
    {"QUERIES",              TK_QUERIES},
    {"QUERY",                TK_QUERY},
    {"RANGE",                TK_RANGE},
    {"RATIO",                TK_RATIO},
    {"PAUSE",                TK_PAUSE},
    {"READ",                 TK_READ},
    {"REDISTRIBUTE",         TK_REDISTRIBUTE},
    {"RENAME",               TK_RENAME},
    {"REPLACE",              TK_REPLACE},
    {"REPLICA",              TK_REPLICA},
    {"RESET",                TK_RESET},
    {"RESUME",               TK_RESUME},
    {"RESTORE",              TK_RESTORE},
    {"RETENTIONS",           TK_RETENTIONS},
    {"REVOKE",               TK_REVOKE},
    {"ROLLUP",               TK_ROLLUP},
    {"SCHEMALESS",           TK_SCHEMALESS},
    {"SCORES",               TK_SCORES},
    {"SELECT",               TK_SELECT},
    {"SERVER_STATUS",        TK_SERVER_STATUS},
    {"SERVER_VERSION",       TK_SERVER_VERSION},
    {"SESSION",              TK_SESSION},
    {"SET",                  TK_SET},
    {"SHOW",                 TK_SHOW},
    {"SINGLE_STABLE",        TK_SINGLE_STABLE},
    {"SLIDING",              TK_SLIDING},
    {"SLIMIT",               TK_SLIMIT},
    {"SMA",                  TK_SMA},
    {"SMALLINT",             TK_SMALLINT},
    {"SNODE",                TK_SNODE},
    {"SNODES",               TK_SNODES},
    {"SORT_FOR_GROUP",       TK_SORT_FOR_GROUP},
    {"SOFFSET",              TK_SOFFSET},
    {"SPLIT",                TK_SPLIT},
    {"STABLE",               TK_STABLE},
    {"STABLES",              TK_STABLES},
    {"START",                TK_START},
    {"STATE",                TK_STATE},
    {"STATE_WINDOW",         TK_STATE_WINDOW},
    {"STORAGE",              TK_STORAGE},
    {"STREAM",               TK_STREAM},
    {"STREAMS",              TK_STREAMS},
    {"STRICT",               TK_STRICT},
    {"STT_TRIGGER",          TK_STT_TRIGGER},
    {"SUBSCRIBE",            TK_SUBSCRIBE},
    {"SUBSCRIPTIONS",        TK_SUBSCRIPTIONS},
    {"SUBTABLE",             TK_SUBTABLE},
    {"SYSINFO",              TK_SYSINFO},
    {"SYSTEM",               TK_SYSTEM},
    {"TABLE",                TK_TABLE},
    {"TABLES",               TK_TABLES},
    {"TABLE_PREFIX",         TK_TABLE_PREFIX},
    {"TABLE_SUFFIX",         TK_TABLE_SUFFIX},
    {"TAG",                  TK_TAG},
    {"TAGS",                 TK_TAGS},
    {"TBNAME",               TK_TBNAME},
    {"THEN",                 TK_THEN},
    {"TIMESTAMP",            TK_TIMESTAMP},
    {"TIMEZONE",             TK_TIMEZONE},
    {"TINYINT",              TK_TINYINT},
    {"TO",                   TK_TO},
    {"TODAY",                TK_TODAY},
    {"TOPIC",                TK_TOPIC},
    {"TOPICS",               TK_TOPICS},
    {"TRANSACTION",          TK_TRANSACTION},
    {"TRANSACTIONS",         TK_TRANSACTIONS},
    {"TRIGGER",              TK_TRIGGER},
    {"TRIM",                 TK_TRIM},
    {"TSDB_PAGESIZE",        TK_TSDB_PAGESIZE},
    {"TSERIES",              TK_TSERIES},
    {"TTL",                  TK_TTL},
    {"UNION",                TK_UNION},
    {"UNSAFE",               TK_UNSAFE},
    {"UNSIGNED",             TK_UNSIGNED},
    {"UNTREATED",            TK_UNTREATED},
    {"UPDATE",               TK_UPDATE},
    {"USE",                  TK_USE},
    {"USER",                 TK_USER},
    {"USERS",                TK_USERS},
    {"USING",                TK_USING},
    {"VALUE",                TK_VALUE},
    {"VALUE_F",              TK_VALUE_F},
    {"VALUES",               TK_VALUES},
    {"VARCHAR",              TK_VARCHAR},
    {"VARIABLES",            TK_VARIABLES},
    {"VERBOSE",              TK_VERBOSE},
    {"VGROUP",               TK_VGROUP},
    {"VGROUPS",              TK_VGROUPS},
    {"VIEW",                 TK_VIEW},
    {"VIEWS",                TK_VIEWS},
    {"VNODE",                TK_VNODE},
    {"VNODES",               TK_VNODES},
    {"WAL_FSYNC_PERIOD",     TK_WAL_FSYNC_PERIOD},
    {"WAL_LEVEL",            TK_WAL_LEVEL},
    {"WAL_RETENTION_PERIOD", TK_WAL_RETENTION_PERIOD},
    {"WAL_RETENTION_SIZE",   TK_WAL_RETENTION_SIZE},
    {"WAL_ROLL_PERIOD",      TK_WAL_ROLL_PERIOD},
    {"WAL_SEGMENT_SIZE",     TK_WAL_SEGMENT_SIZE},
    {"WATERMARK",            TK_WATERMARK},
    {"WHEN",                 TK_WHEN},
    {"WHERE",                TK_WHERE},
    {"WINDOW_CLOSE",         TK_WINDOW_CLOSE},
    {"WITH",                 TK_WITH},
    {"WRITE",                TK_WRITE},
    {"_C0",                  TK_ROWTS},
    {"_IROWTS",              TK_IROWTS},
    {"_ISFILLED",            TK_ISFILLED},
    {"_QDURATION",           TK_QDURATION},
    {"_QEND",                TK_QEND},
    {"_QSTART",              TK_QSTART},
    {"_ROWTS",               TK_ROWTS},
    {"_TAGS",                TK_QTAGS},
    {"_WDURATION",           TK_WDURATION},
    {"_WEND",                TK_WEND},
    {"_WSTART",              TK_WSTART},
    {"ALIVE",                TK_ALIVE},
    {"VARBINARY",            TK_VARBINARY},
    {"KEEP_TIME_OFFSET",     TK_KEEP_TIME_OFFSET},
};
// clang-format on

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
  if (n > tListLen(key)) {  // too long token, can not be any other token type
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
  return (pKey != NULL) ? (*pKey)->type : TK_NK_ID;
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
      } else if (z[1] == '>') {
        *tokenId = TK_NK_ARROW;
        return 2;
      }
      *tokenId = TK_NK_MINUS;
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
      bool isHint = false;
      if (z[2] == '+') {
        isHint = true;
      }
      for (i = 3; z[i] && (z[i] != '/' || z[i - 1] != '*'); i++) {
      }
      if (z[i]) i++;
      *tokenId = isHint ? TK_NK_HINT : TK_NK_COMMENT;
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
        if (delim != '`' && z[i] == '\\') {  // ignore the escaped character that follows this backslash
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
        *tokenId = (delim == '`') ? TK_NK_ID : TK_NK_STRING;
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

      if (next == 'b') {  // bin number
        *tokenId = TK_NK_BIN;
        for (i = 2; (z[i] == '0' || z[i] == '1'); ++i) {
        }

        if (i == 2) {
          break;
        }

        return i;
      } else if (next == 'x') {  // hex number
        *tokenId = TK_NK_HEX;
        for (i = 2; isxdigit(z[i]) != 0; ++i) {
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
      if ((z[i] == 'b' || z[i] == 'u' || z[i] == 'a' || z[i] == 's' || z[i] == 'm' || z[i] == 'h' || z[i] == 'd' ||
           z[i] == 'n' || z[i] == 'y' || z[i] == 'w' || z[i] == 'B' || z[i] == 'U' || z[i] == 'A' || z[i] == 'S' ||
           z[i] == 'M' || z[i] == 'H' || z[i] == 'D' || z[i] == 'N' || z[i] == 'Y' || z[i] == 'W') &&
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
      } else if (seg > 2) {
        break;
      }

      // support float with no decimal part after the decimal point
      if (z[i] == '.' && seg == 1) {
        *tokenId = TK_NK_FLOAT;
        i++;
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
    // case '[': {
    //   for (i = 1; z[i] && z[i - 1] != ']'; i++) {
    //   }
    //   *tokenId = TK_NK_ID;
    //   return i;
    // }
    case 'T':
    case 't':
    case 'F':
    case 'f': {
      bool hasNonAsciiChars = false;
      for (i = 1;; i++) {
        if ((z[i] & 0x80) != 0) {
          // utf-8 characters
          // currently, we support using utf-8 characters only in alias
          hasNonAsciiChars = true;
        } else if (isIdChar[(uint8_t)z[i]]) {
        } else {
          break;
        }
      }
      if (hasNonAsciiChars) {
        *tokenId = TK_NK_ALIAS; // must be alias
        return i;
      }
      if ((i == 4 && strncasecmp(z, "true", 4) == 0) || (i == 5 && strncasecmp(z, "false", 5) == 0)) {
        *tokenId = TK_NK_BOOL;
        return i;
      }
      *tokenId = tKeywordCode(z, i);
      return i;
    }
    default: {
      if ((*z & 0x80) == 0 && !isIdChar[(uint8_t)*z]) {
        break;
      }
      bool hasNonAsciiChars = false;
      for (i = 1; ; i++) {
        if ((z[i] & 0x80) != 0) {
          hasNonAsciiChars = true;
        } else if (isIdChar[(uint8_t)z[i]]){
        } else {
          break;
        }
      }
      if (hasNonAsciiChars) {
        *tokenId = TK_NK_ALIAS;
        return i;
      }
      *tokenId = tKeywordCode(z, i);
      return i;
    }
  }

  *tokenId = TK_NK_ILLEGAL;
  return 0;
}

SToken tStrGetToken(const char* str, int32_t* i, bool isPrevOptr, bool* pIgnoreComma) {
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
    char    t = str[*i];
    while (t == ' ' || t == '\n' || t == '\r' || t == '\t' || t == '\f' || t == ',') {
      if (t == ',' && (++numOfComma > 1)) {  // comma only allowed once
        t0.n = 0;
        return t0;
      }

      if (NULL != pIgnoreComma && t == ',') {
        *pIgnoreComma = true;
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
    t0.type = 0;
    return t0;
  }

  uint32_t type = 0;
  int32_t  len;

  // support parse the 'db.tbl' format, notes: There should be no space on either side of the dot!
  if ('.' == str[*i + t0.n]) {
    len = tGetToken(&str[*i + t0.n + 1], &type);

    // only id and string are valid
    if (((TK_NK_STRING != t0.type) && (TK_NK_ID != t0.type)) || ((TK_NK_STRING != type) && (TK_NK_ID != type))) {
      t0.type = TK_NK_ILLEGAL;
      t0.n = 0;

      return t0;
    }

    t0.n += len + 1;

  } else {
    // support parse the -/+number format
    if ((isPrevOptr) && (t0.type == TK_NK_MINUS || t0.type == TK_NK_PLUS)) {
      len = tGetToken(&str[*i + t0.n], &type);
      if (type == TK_NK_INTEGER || type == TK_NK_FLOAT || type == TK_NK_BIN || type == TK_NK_HEX) {
        t0.type = type;
        t0.n += len;
      }
    }
  }

  t0.z = (char*)str + (*i);
  *i += t0.n;

  return t0;
}

bool taosIsKeyWordToken(const char* z, int32_t len) { return (tKeywordCode((char*)z, len) != TK_NK_ID); }

void taosCleanupKeywordsTable() {
  void* m = keywordHashTable;
  if (m != NULL && atomic_val_compare_exchange_ptr(&keywordHashTable, m, 0) == m) {
    taosHashCleanup(m);
  }
}
