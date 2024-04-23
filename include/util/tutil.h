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

#ifndef _TD_UTIL_UTIL_H_
#define _TD_UTIL_UTIL_H_

#include "os.h"
#include "tcrc32c.h"
#include "tdef.h"
#include "thash.h"
#include "tmd5.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t strdequote(char *src);
size_t  strtrim(char *src);
char   *strnchr(const char *haystack, char needle, int32_t len, bool skipquote);
TdUcs4 *wcsnchr(const TdUcs4 *haystack, TdUcs4 needle, size_t len);

char  **strsplit(char *src, const char *delim, int32_t *num);
char   *strtolower(char *dst, const char *src);
char   *strntolower(char *dst, const char *src, int32_t n);
char   *strntolower_s(char *dst, const char *src, int32_t n);
int64_t strnatoi(char *num, int32_t len);

size_t tstrncspn(const char *str, size_t ssize, const char *reject, size_t rsize);
size_t twcsncspn(const TdUcs4 *wcs, size_t size, const TdUcs4 *reject, size_t rsize);

char *strbetween(char *string, char *begin, char *end);
char *paGetToken(char *src, char **token, int32_t *tokenLen);

int32_t taosByteArrayToHexStr(char bytes[], int32_t len, char hexstr[]);
int32_t taosHexStrToByteArray(char hexstr[], char bytes[]);

int32_t tintToHex(uint64_t val, char hex[]);
int32_t titoa(uint64_t val, size_t radix, char str[]);

char    *taosIpStr(uint32_t ipInt);
uint32_t ip2uint(const char *const ip_addr);
void     taosIp2String(uint32_t ip, char *str);
void     taosIpPort2String(uint32_t ip, uint16_t port, char *str);

void *tmemmem(const char *haystack, int hlen, const char *needle, int nlen);

static FORCE_INLINE void taosEncryptPass(uint8_t *inBuf, size_t inLen, char *target) {
  T_MD5_CTX context;
  tMD5Init(&context);
  tMD5Update(&context, inBuf, (uint32_t)inLen);
  tMD5Final(&context);
  memcpy(target, context.digest, tListLen(context.digest));
}

static FORCE_INLINE void taosEncryptPass_c(uint8_t *inBuf, size_t len, char *target) {
  T_MD5_CTX context;
  tMD5Init(&context);
  tMD5Update(&context, inBuf, (uint32_t)len);
  tMD5Final(&context);
  char buf[TSDB_PASSWORD_LEN + 1];

  buf[TSDB_PASSWORD_LEN] = 0;
  sprintf(buf, "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x", context.digest[0], context.digest[1],
          context.digest[2], context.digest[3], context.digest[4], context.digest[5], context.digest[6],
          context.digest[7], context.digest[8], context.digest[9], context.digest[10], context.digest[11],
          context.digest[12], context.digest[13], context.digest[14], context.digest[15]);
  memcpy(target, buf, TSDB_PASSWORD_LEN);
}

static FORCE_INLINE int32_t taosCreateMD5Hash(char *pBuf, int32_t len) {
  T_MD5_CTX ctx;
  tMD5Init(&ctx);
  tMD5Update(&ctx, (uint8_t*)pBuf, len);
  tMD5Final(&ctx);
  char* p = pBuf;
  int32_t resLen = 0;
  for (uint8_t i = 0; i < tListLen(ctx.digest); ++i) {
    resLen += snprintf(p, 3, "%02x", ctx.digest[i]);
    p += 2;
  }
  return resLen;
}

static FORCE_INLINE int32_t taosGetTbHashVal(const char *tbname, int32_t tblen, int32_t method, int32_t prefix,
                                             int32_t suffix) {
  if ((prefix == 0 && suffix == 0) || (tblen <= (prefix + suffix)) || (tblen <= -1 * (prefix + suffix)) ||
      prefix * suffix < 0) {
    return MurmurHash3_32(tbname, tblen);
  } else if (prefix > 0 || suffix > 0) {
    return MurmurHash3_32(tbname + prefix, tblen - prefix - suffix);
  } else {
    char    tbName[TSDB_TABLE_FNAME_LEN];
    int32_t offset = 0;
    if (prefix < 0) {
      offset = -1 * prefix;
      strncpy(tbName, tbname, offset);
    }
    if (suffix < 0) {
      strncpy(tbName + offset, tbname + tblen + suffix, -1 * suffix);
      offset += -1 * suffix;
    }
    return MurmurHash3_32(tbName, offset);
  }
}

#define TSDB_CHECK_CODE(CODE, LINO, LABEL) \
  do {                                     \
    if ((CODE)) {                          \
      LINO = __LINE__;                     \
      goto LABEL;                          \
    }                                      \
  } while (0)

#define TSDB_CHECK_NULL(ptr, CODE, LINO, LABEL, ERRNO) \
  if ((ptr) == NULL) {                                 \
    (CODE) = (ERRNO);                                  \
    (LINO) = __LINE__;                                 \
    goto LABEL;                                        \
  }

#define ARRAY_SIZE(a) (sizeof(a) / sizeof(a[0]))

#define VND_CHECK_CODE(CODE, LINO, LABEL) TSDB_CHECK_CODE(CODE, LINO, LABEL)

#define TCONTAINER_OF(ptr, type, member) ((type *)((char *)(ptr)-offsetof(type, member)))

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_UTIL_H_*/
