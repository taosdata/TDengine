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
#include "tutil.h"
#include "ttokendef.h"

#define TSQL_TBNAME   "TBNAME"
#define TSQL_TBNAME_L "tbname"

// used to denote the minimum unite in sql parsing
typedef struct SStrToken {
  uint32_t n;
  uint32_t type;
  char    *z;
} SStrToken;

/**
 * tokenizer for sql string
 * @param z
 * @param tokenType
 * @return
 */
uint32_t tSQLGetToken(char *z, uint32_t *tokenType);

/**
 * enhanced tokenizer for sql string.
 *
 * @param str
 * @param i
 * @param isPrevOptr
 * @param numOfIgnoreToken
 * @param ignoreTokenTypes
 * @return
 */
SStrToken tStrGetToken(char *str, int32_t *i, bool isPrevOptr, uint32_t numOfIgnoreToken, uint32_t *ignoreTokenTypes);

/**
 * check if it is a keyword or not
 * @param z
 * @param len
 * @return
 */
bool isKeyWord(const char *z, int32_t len);

/**
 * check if it is a number or not
 * @param pToken
 * @return
 */
#define isNumber(tk) \
((tk)->type == TK_INTEGER || (tk)->type == TK_FLOAT || (tk)->type == TK_HEX || (tk)->type == TK_BIN)


/**
 * check if it is a token or not
 * @param pToken
 * @return        token type, if it is not a number, TK_ILLEGAL will return
 */
static FORCE_INLINE int32_t tGetNumericStringType(const SStrToken* pToken) {
  const char* z = pToken->z;
  int32_t type = TK_ILLEGAL;

  uint32_t i = 0;
  for(; i < pToken->n; ++i) {
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
        if (!isdigit(z[i+1])) {
          return TK_ILLEGAL;
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

        type = TK_FLOAT;
        goto _end;
      }

      case '0': {
        char next = z[i + 1];
        if (next == 'b') { // bin number
          type = TK_BIN;
          for (i += 2; (z[i] == '0' || z[i] == '1'); ++i) {
          }

          goto _end;
        } else if (next == 'x') {  //hex number
          type = TK_HEX;
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
        type = TK_INTEGER;
        for (; isdigit(z[i]); i++) {
        }

        int32_t seg = 0;
        while (z[i] == '.' && isdigit(z[i + 1])) {
          i += 2;

          while (isdigit(z[i])) {
            i++;
          }

          seg++;
          type = TK_FLOAT;
        }

        if (seg > 1) {
          return TK_ILLEGAL;
        }

        if ((z[i] == 'e' || z[i] == 'E') &&
            (isdigit(z[i + 1]) || ((z[i + 1] == '+' || z[i + 1] == '-') && isdigit(z[i + 2])))) {
          i += 2;
          while (isdigit(z[i])) {
            i++;
          }

          type = TK_FLOAT;
        }

        goto _end;
      }
      default:
        return TK_ILLEGAL;
    }
  }

  _end:
  return (i < pToken->n)? TK_ILLEGAL:type;
}

void taosCleanupKeywordsTable();

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TTOKEN_H
