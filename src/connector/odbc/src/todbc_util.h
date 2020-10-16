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

#ifndef _TODBC_UTIL_H_
#define _TODBC_UTIL_H_

#include <libgen.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <sql.h>

#define D(fmt, ...)                                              \
  fprintf(stderr,                                                \
          "%s[%d]:%s() " fmt "\n",                               \
          basename((char*)__FILE__), __LINE__, __func__,         \
          ##__VA_ARGS__)

#define DASSERT(statement)                                       \
do {                                                             \
  if (statement) break;                                          \
  D("Assertion failure: %s", #statement);                        \
  abort();                                                       \
} while (0)

#define DASSERTX(statement, fmt, ...)                                \
do {                                                                 \
  if (statement) break;                                              \
  D("Assertion failure: %s, " fmt "", #statement, ##__VA_ARGS__);    \
  abort();                                                           \
} while (0)



const char* sql_sql_type(int type);
const char* sql_c_type(int type);

int is_valid_sql_c_type(int type);
int is_valid_sql_sql_type(int type);

int string_conv(const char *fromcode, const char *tocode,
                const unsigned char *src, size_t sbytes,
                unsigned char *dst, size_t dbytes,
                size_t *consumed, size_t *generated);
int utf8_chars(const char *src);

unsigned char* utf8_to_ucs4le(const char *utf8, size_t *chars);
char*          ucs4le_to_utf8(const unsigned char *ucs4le, size_t slen, size_t *chars);
SQLCHAR*       wchars_to_chars(const SQLWCHAR *wchars, size_t chs, size_t *bytes);

#endif // _TODBC_UTIL_H_
