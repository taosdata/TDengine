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
 * @return
 */
uint32_t tGetToken(const char *z, uint32_t *tokenType);

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
 * check if it is a keyword or not
 * @param z
 * @param len
 * @return
 */
bool taosIsKeyWordToken(const char *z, int32_t len);

void taosCleanupKeywordsTable();

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TTOKEN_H
