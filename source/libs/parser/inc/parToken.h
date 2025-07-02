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

#ifndef TDENGINE_TTOKEN_H
#define TDENGINE_TTOKEN_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "tname.h"

#include "ttokendef.h"

#define IS_TRUE_STR(s, n)                                                                \
  (n == 4 && (*(s) == 't' || *(s) == 'T') && (*((s) + 1) == 'r' || *((s) + 1) == 'R') && \
   (*((s) + 2) == 'u' || *((s) + 2) == 'U') && (*((s) + 3) == 'e' || *((s) + 3) == 'E'))

#define IS_FALSE_STR(s, n)                                                                 \
  (n == 5 && (*(s) == 'f' || *(s) == 'F') && (*((s) + 1) == 'a' || *((s) + 1) == 'A') &&   \
   (*((s) + 2) == 'l' || *((s) + 2) == 'L') && (*((s) + 3) == 's' || *((s) + 3) == 'S') && \
   (*((s) + 4) == 'e' || *((s) + 4) == 'E'))

// used to denote the minimum unite in sql parsing
typedef struct SToken {
  uint32_t n;
  uint32_t type;
  char    *z;
} SToken;

// used to denote VALUES section in INSERT statement
typedef struct {
  const char *z;  // Pointer to VALUES content
  int32_t     n;  // Length of VALUES content
} SValuesToken;

// used to denote bound columns section in INSERT statement
typedef struct {
  const char *z;  // Pointer to bound columns content
  int32_t     n;  // Length of bound columns content
} SBoundColumnsToken;

// used to denote INSERT SQL components (VALUES and bound columns)
typedef struct {
  SValuesToken       values;     // VALUES section token
  SBoundColumnsToken boundCols;  // Bound columns token
  SName             *pName;      // Table name (target table or using table)
  bool               isStb;      // Whether this is a super table (using table)
} SInsertTokens;

/**
 * check if it is a number or not
 * @param pToken
 * @return
 */
#define isNumber(tk) \
  ((tk)->type == TK_NK_INTEGER || (tk)->type == TK_NK_FLOAT || (tk)->type == TK_NK_HEX || (tk)->type == TK_NK_BIN)

/**
 * tokenizer for sql string
 * @param z
 * @param tokenType
 * @param dupQuoteChar duplicated quote char contained in the quoted string
 * @return
 */
uint32_t tGetToken(const char *z, uint32_t *tokenType, char* dupQuoteChar);

/**
 * enhanced tokenizer for sql string.
 *
 * @param str
 * @param i
 * @param isPrevOptr
 * @return
 */
SToken tStrGetToken(const char *str, int32_t *i, bool isPrevOptr, bool *pIgnoreComma);

/**
 * Parse VALUES section in INSERT statement
 * Each call reads complete VALUES content until next table name or statement end
 * Example: insert into t1 values (now,1),(now,2) t2 values ...
 * First call returns: (now,1),(now,2)
 *
 * @param str input string
 * @param i current position pointer
 * @return SValuesToken containing pointer and length of VALUES content
 */
SValuesToken tStrGetValues(const char *str);

/**
 * Parse bound columns section in INSERT statement
 * Extracts column names list from INSERT statement
 * Example: insert into t1 (col1, col2, col3) values ...
 * Returns: (col1, col2, col3)
 *
 * @param str input string
 * @return SBoundColumnsToken containing pointer and length of bound columns content
 */
SBoundColumnsToken tStrGetBoundColumns(const char *str);

/**
 * check if it is a keyword or not
 * @param z
 * @param len
 * @return
 */
bool taosIsKeyWordToken(const char *z, int32_t len);

/**
 * check if it is a token or not
 * @param   pToken
 * @return  token type, if it is not a number, TK_NK_ILLEGAL will return
 */
static FORCE_INLINE int32_t tGetNumericStringType(const SToken *pToken) {
  const char *z = pToken->z;
  int32_t     type = TK_NK_ILLEGAL;

  uint32_t i = 0;
  for (; i < pToken->n; ++i) {
    switch (z[i]) {
      case '+':
      case '-': {
        break;
      }

      case '.': {
        /*
         * handle the the float number with out integer part
         * .123
         * .123e4
         */
        if (!isdigit(z[i + 1])) {
          return TK_NK_ILLEGAL;
        }

        for (i += 2; isdigit(z[i]); i++) {
        }

        if ((z[i] == 'e' || z[i] == 'E') &&
            (isdigit(z[i + 1]) || ((z[i + 1] == '+' || z[i + 1] == '-') && isdigit(z[i + 2])))) {
          i += 2;
          while (isdigit(z[i])) {
            i++;
          }
        }

        type = TK_NK_FLOAT;
        goto _end;
      }

      case '0': {
        char next = z[i + 1];
        if (next == 'b') {  // bin number
          type = TK_NK_BIN;
          for (i += 2; (z[i] == '0' || z[i] == '1'); ++i) {
          }

          goto _end;
        } else if (next == 'x') {  // hex number
          type = TK_NK_HEX;
          for (i += 2; isdigit(z[i]) || (z[i] >= 'a' && z[i] <= 'f') || (z[i] >= 'A' && z[i] <= 'F'); ++i) {
          }

          goto _end;
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
        type = TK_NK_INTEGER;
        for (; isdigit(z[i]); i++) {
        }

        int32_t seg = 0;
        while (z[i] == '.' && isdigit(z[i + 1])) {
          i += 2;

          while (isdigit(z[i])) {
            i++;
          }

          seg++;
          type = TK_NK_FLOAT;
        }

        if (seg > 1) {
          return TK_NK_ILLEGAL;
        }

        if ((z[i] == 'e' || z[i] == 'E') &&
            (isdigit(z[i + 1]) || ((z[i + 1] == '+' || z[i + 1] == '-') && isdigit(z[i + 2])))) {
          i += 2;
          while (isdigit(z[i])) {
            i++;
          }

          type = TK_NK_FLOAT;
        }

        goto _end;
      }
      default:
        return TK_NK_ILLEGAL;
    }
  }

_end:
  return (i < pToken->n) ? TK_NK_ILLEGAL : type;
}

int32_t taosInitKeywordsTable();
void    taosCleanupKeywordsTable();

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TTOKEN_H
