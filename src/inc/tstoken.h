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

char *tscGetToken(char *string, char **token, int *tokenLen);
char *tscGetTokenDelimiter(char *string, char **token, int *tokenLen, char *delimiters);

/**
 * tokenizer for sql string
 * @param z
 * @param tokenType
 * @return
 */
uint32_t tSQLGetToken(char *z, uint32_t *tokenType);

void tStrGetToken(char *str, int32_t *i, SSQLToken *t0, bool isPrevOptr);

bool isKeyWord(const char *z, int32_t len);
bool isNumber(const SSQLToken *pToken);

void shiftStr(char *dst, char *src);

uint64_t changeToTimestampWithDynPrec(SSQLToken *pToken);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TTOKEN_H
