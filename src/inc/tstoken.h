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

#include <stdbool.h>

// used to denote the minimum unite in sql parsing
typedef struct SSQLToken {
  uint32_t n;
  uint32_t type;
  char *   z;
} SSQLToken;

#if 0
char *tscGetToken(char *string, char **token, int *tokenLen);
#endif

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
SSQLToken tStrGetToken(char *str, int32_t *i, bool isPrevOptr, uint32_t numOfIgnoreToken, uint32_t *ignoreTokenTypes);

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
bool isNumber(const SSQLToken *pToken);

/**
 * check if it is a token or not
 * @param pToken
 * @return       token type, if it is not a number, TK_ILLEGAL will return
 */
int32_t isValidNumber(const SSQLToken* pToken);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TTOKEN_H
