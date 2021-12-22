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

#ifndef TDENGINE_TUTIL_H
#define TDENGINE_TUTIL_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "tmd5.h"
#include "tcrc32c.h"
#include "taosdef.h"

int32_t strdequote(char *src);
int32_t strRmquote(char *z, int32_t len);
size_t  strtrim(char *src);
char *  strnchr(char *haystack, char needle, int32_t len, bool skipquote);
char ** strsplit(char *src, const char *delim, int32_t *num);
char *  strtolower(char *dst, const char *src);
char *  strntolower(char *dst, const char *src, int32_t n);
char *  strntolower_s(char *dst, const char *src, int32_t n);
int64_t strnatoi(char *num, int32_t len);
char *  strbetween(char *string, char *begin, char *end);
char *  paGetToken(char *src, char **token, int32_t *tokenLen);

int32_t taosByteArrayToHexStr(char bytes[], int32_t len, char hexstr[]);
int32_t taosHexStrToByteArray(char hexstr[], char bytes[]);

bool taosGetVersionNumber(char *versionStr, int *versionNubmer);
int  taosCheckVersion(char *input_client_version, char *input_server_version, int compared_segments);

char *   taosIpStr(uint32_t ipInt);
uint32_t ip2uint(const char *const ip_addr);

static FORCE_INLINE void taosEncryptPass(uint8_t *inBuf, size_t inLen, char *target) {
  MD5_CTX context;
  MD5Init(&context);
  MD5Update(&context, inBuf, (unsigned int)inLen);
  MD5Final(&context);
  memcpy(target, context.digest, TSDB_KEY_LEN);
}

//
// TSKEY util
//

// if time area(s1,e1) intersect with time area(s2,e2) then return true else return false
bool timeIntersect(TSKEY s1, TSKEY e1, TSKEY s2, TSKEY e2);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TUTIL_H
