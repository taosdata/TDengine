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

#include "todbc_util.h"

#include "iconv.h"

#include <sql.h>
#include <sqltypes.h>
#include <sqlext.h>
#include <stdlib.h>
#include <string.h>

const char* sql_sql_type(int type) {
  switch (type) {
    case SQL_BIT:                               return "SQL_BIT";
    case SQL_TINYINT:                           return "SQL_TINYINT";
    case SQL_SMALLINT:                          return "SQL_SMALLINT";
    case SQL_INTEGER:                           return "SQL_INTEGER";
    case SQL_BIGINT:                            return "SQL_BIGINT";
    case SQL_FLOAT:                             return "SQL_FLOAT";
    case SQL_DOUBLE:                            return "SQL_DOUBLE";
    case SQL_DECIMAL:                           return "SQL_DECIMAL";
    case SQL_NUMERIC:                           return "SQL_NUMERIC";
    case SQL_REAL:                              return "SQL_REAL";
    case SQL_CHAR:                              return "SQL_CHAR";
    case SQL_VARCHAR:                           return "SQL_VARCHAR";
    case SQL_LONGVARCHAR:                       return "SQL_LONGVARCHAR";
    case SQL_WCHAR:                             return "SQL_WCHAR";
    case SQL_WVARCHAR:                          return "SQL_WVARCHAR";
    case SQL_WLONGVARCHAR:                      return "SQL_WLONGVARCHAR";
    case SQL_BINARY:                            return "SQL_BINARY";
    case SQL_VARBINARY:                         return "SQL_VARBINARY";
    case SQL_LONGVARBINARY:                     return "SQL_LONGVARBINARY";
    case SQL_DATE:                              return "SQL_DATE";
    case SQL_TIME:                              return "SQL_TIME";
    case SQL_TIMESTAMP:                         return "SQL_TIMESTAMP";
    case SQL_TYPE_DATE:                         return "SQL_TYPE_DATE";
    case SQL_TYPE_TIME:                         return "SQL_TYPE_TIME";
    case SQL_TYPE_TIMESTAMP:                    return "SQL_TYPE_TIMESTAMP";
    case SQL_INTERVAL_MONTH:                    return "SQL_INTERVAL_MONTH";
    case SQL_INTERVAL_YEAR:                     return "SQL_INTERVAL_YEAR";
    case SQL_INTERVAL_YEAR_TO_MONTH:            return "SQL_INTERVAL_YEAR_TO_MONTH";
    case SQL_INTERVAL_DAY:                      return "SQL_INTERVAL_DAY";
    case SQL_INTERVAL_HOUR:                     return "SQL_INTERVAL_HOUR";
    case SQL_INTERVAL_MINUTE:                   return "SQL_INTERVAL_MINUTE";
    case SQL_INTERVAL_SECOND:                   return "SQL_INTERVAL_SECOND";
    case SQL_INTERVAL_DAY_TO_HOUR:              return "SQL_INTERVAL_DAY_TO_HOUR";
    case SQL_INTERVAL_DAY_TO_MINUTE:            return "SQL_INTERVAL_DAY_TO_MINUTE";
    case SQL_INTERVAL_DAY_TO_SECOND:            return "SQL_INTERVAL_DAY_TO_SECOND";
    case SQL_INTERVAL_HOUR_TO_MINUTE:           return "SQL_INTERVAL_HOUR_TO_MINUTE";
    case SQL_INTERVAL_HOUR_TO_SECOND:           return "SQL_INTERVAL_HOUR_TO_SECOND";
    case SQL_INTERVAL_MINUTE_TO_SECOND:         return "SQL_INTERVAL_MINUTE_TO_SECOND";
    case SQL_GUID:                              return "SQL_GUID";
    default: return "UNKNOWN";
  }
}

const char* sql_c_type(int type) {
  switch (type) {
    case SQL_C_CHAR:                  return "SQL_C_CHAR";
    case SQL_C_WCHAR:                 return "SQL_C_WCHAR";
    case SQL_C_SHORT:                 return "SQL_C_SHORT";
    case SQL_C_SSHORT:                return "SQL_C_SSHORT";
    case SQL_C_USHORT:                return "SQL_C_USHORT";
    case SQL_C_LONG:                  return "SQL_C_LONG";
    case SQL_C_SLONG:                 return "SQL_C_SLONG";
    case SQL_C_ULONG:                 return "SQL_C_ULONG";
    case SQL_C_FLOAT:                 return "SQL_C_FLOAT";
    case SQL_C_DOUBLE:                return "SQL_C_DOUBLE";
    case SQL_C_BIT:                   return "SQL_C_BIT";
    case SQL_C_TINYINT:               return "SQL_C_TINYINT";
    case SQL_C_STINYINT:              return "SQL_C_STINYINT";
    case SQL_C_UTINYINT:              return "SQL_C_UTINYINT";
    case SQL_C_SBIGINT:               return "SQL_C_SBIGINT";
    case SQL_C_UBIGINT:               return "SQL_C_UBIGINT";
    case SQL_C_BINARY:                return "SQL_C_BINARY";
    case SQL_C_DATE:                  return "SQL_C_DATE";
    case SQL_C_TIME:                  return "SQL_C_TIME";
    case SQL_C_TIMESTAMP:             return "SQL_C_TIMESTAMP";
    case SQL_C_TYPE_DATE:             return "SQL_C_TYPE_DATE";
    case SQL_C_TYPE_TIME:             return "SQL_C_TYPE_TIME";
    case SQL_C_TYPE_TIMESTAMP:        return "SQL_C_TYPE_TIMESTAMP";
    case SQL_C_NUMERIC:               return "SQL_C_NUMERIC";
    case SQL_C_GUID:                  return "SQL_C_GUID";
    default: return "UNKNOWN";
  }
}

int is_valid_sql_c_type(int type) {
  const char *ctype = sql_c_type(type);
  if (strcmp(ctype, "UNKNOWN")==0) return 0;
  return 1;
}

int is_valid_sql_sql_type(int type) {
  const char *sqltype = sql_sql_type(type);
  if (strcmp(sqltype, "UNKNOWN")==0) return 0;
  return 1;
}

int string_conv(const char *fromcode, const char *tocode,
                const unsigned char *src, size_t sbytes,
                unsigned char *dst, size_t dbytes,
                size_t *consumed, size_t *generated)
{
  if (consumed) *consumed = 0;
  if (generated) *generated = 0;

  if (dbytes <= 0) return -1;
  dst[0] = '\0';

  iconv_t conv = iconv_open(tocode, fromcode);
  if (!conv) return -1;

  int r = 0;
  do {
    char *s = (char*)src;
    char *d = (char*)dst;
    size_t sl = sbytes;
    size_t dl = dbytes;

    r = iconv(conv, &s, &sl, &d, &dl);
    *d = '\0';

    if (consumed) *consumed = sbytes - sl;
    if (generated) *generated = dbytes - dl;

  } while (0);

  iconv_close(conv);
  return r;
}

int utf8_chars(const char *src)
{
  const char *fromcode = "UTF-8";
  const char *tocode = "UCS-2LE";
  iconv_t conv = iconv_open(tocode, fromcode);
  if (!conv) return -1;

  size_t slen = strlen(src);
  char buf[4096];
  size_t dlen = sizeof(buf);
  char *ps = (char*)src;
  char *pd = buf;
  iconv(conv, &ps, &slen, &pd, &dlen);
  DASSERT(slen==0);

  size_t chars = (sizeof(buf) - dlen) / 2;
  iconv_close(conv);
  return chars;
}

unsigned char* utf8_to_ucs4le(const char *utf8, size_t *chars)
{
  const char *tocode = "UCS-4LE";
  const char *fromcode = "UTF-8";

  iconv_t conv = iconv_open(tocode, fromcode);
  if (!conv) return NULL;

  unsigned char *ucs4le = NULL;

  do {
    size_t slen = strlen(utf8);
    size_t dlen = slen * 4;

    ucs4le = (unsigned char*)malloc(dlen+1);
    if (!ucs4le) break;

    char *src = (char*)utf8;
    char *dst = (char*)ucs4le;
    size_t s = slen;
    size_t d = dlen;
    iconv(conv, &src, &s, &dst, &d);
    dst[0] = '\0';

    if (chars) *chars = (dlen - d) / 4;
  } while (0);

  iconv_close(conv);
  return ucs4le;
}

char* ucs4le_to_utf8(const unsigned char *ucs4le, size_t slen, size_t *chars)
{
  const char *fromcode = "UCS-4LE";
  const char *tocode = "UTF-8";

  iconv_t conv = iconv_open(tocode, fromcode);
  if (!conv) return NULL;

  char *utf8 = NULL;

  do {
    size_t dlen = slen;

    utf8 = (char*)malloc(dlen+1);
    if (!utf8) break;

    char *dst = utf8;
    char *src = (char*)ucs4le;
    size_t s = slen;
    size_t d = dlen;
    iconv(conv, &src, &s, &dst, &d);
    dst[0] = '\0';

    if (chars) *chars = (slen - s) / 4;
  } while (0);

  iconv_close(conv);
  return utf8;
}

SQLCHAR* wchars_to_chars(const SQLWCHAR *wchars, size_t chs, size_t *bytes)
{
  size_t dlen = chs * 4;
  SQLCHAR *dst = (SQLCHAR*)malloc(dlen + 1);
  if (!dst) return NULL;

  string_conv("UCS-2LE", "UTF-8", (const unsigned char*)wchars, chs * sizeof(*wchars), dst, dlen + 1, NULL, bytes);

  return dst;
}

size_t wchars_to_chars2(const SQLWCHAR *src, size_t slen, SQLCHAR *dst, size_t dlen)
{
  size_t consumed=0, generated=0;
  int n = string_conv("UCS-2LE", "UTF-8", (const unsigned char*)src, slen, dst, dlen, &consumed, &generated);
  if (n) return -1;
  return generated;
}

size_t chars_to_wchars2(const SQLCHAR *src, size_t slen, SQLWCHAR *dst, size_t dlen)
{
  size_t consumed=0, generated=0;
  int n = string_conv("UTF-8", "UCS-2LE", (const unsigned char*)src, slen, (unsigned char*)dst, dlen, &consumed, &generated);
  if (n) return -1;
  return generated;
}

