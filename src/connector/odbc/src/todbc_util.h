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

#include "todbc_log.h"

#include <stddef.h>
#include <sql.h>

const char* sql_sql_type(int type);
const char* sql_c_type(int type);

int is_valid_sql_c_type(int type);
int is_valid_sql_sql_type(int type);

int todbc_parse_conn_string(const char *conn, char **dsn, char **uid, char **pwd, char **host);

int string_conv(const char *fromcode, const char *tocode,
                const unsigned char *src, size_t sbytes,
                unsigned char *dst, size_t dbytes,
                size_t *consumed, size_t *generated);
int utf8_chars(const char *src);

unsigned char* utf8_to_ucs4le(const char *utf8, size_t *chars);
char*          ucs4le_to_utf8(const unsigned char *ucs4le, size_t slen, size_t *chars);
SQLCHAR*       wchars_to_chars(const SQLWCHAR *wchars, size_t chs, size_t *bytes);

size_t         wchars_to_chars2(const SQLWCHAR *src, size_t slen, SQLCHAR *dst, size_t dlen);
size_t         chars_to_wchars2(const SQLCHAR *src, size_t slen, SQLWCHAR *dst, size_t dlen);

#endif // _TODBC_UTIL_H_

