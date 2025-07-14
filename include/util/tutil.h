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
int64_t strnatoi(const char *num, int32_t len);

size_t tstrncspn(const char *str, size_t ssize, const char *reject, size_t rsize);
size_t twcsncspn(const TdUcs4 *wcs, size_t size, const TdUcs4 *reject, size_t rsize);

char *strbetween(char *string, char *begin, char *end);
char *paGetToken(char *src, char **token, int32_t *tokenLen);

int32_t taosByteArrayToHexStr(char bytes[], int32_t len, char hexstr[]);
int32_t taosHexStrToByteArray(char hexstr[], char bytes[]);

int32_t tintToHex(uint64_t val, char hex[]);
int32_t titoa(uint64_t val, size_t radix, char str[]);

void *tmemmem(const char *haystack, int hlen, const char *needle, int nlen);

int32_t parseCfgReal(const char *str, float *out);
bool    tIsValidFileName(const char *fileName, const char *pattern);
bool    tIsValidFilePath(const char *filePath, const char *pattern);
void    tTrimMountPrefix(char *fullName);

#ifdef TD_ASTRA
static FORCE_INLINE int32_t taosStrcasecmp(const char *s1, const char *s2) {
  if (s1[0] == 0 && s2[0] == 0) return 0;
  return strcasecmp(s1, s2);
}

static FORCE_INLINE int32_t taosStrncasecmp(const char *s1, const char *s2, size_t n) {
  if (s1[0] == 0 && s2[0] == 0) return 0;
  return strncasecmp(s1, s2, n);
}
#else
#define taosStrcasecmp  strcasecmp
#define taosStrncasecmp strncasecmp
#endif

#ifdef NO_UNALIGNED_ACCESS
#define CHECK_ALIGNMENT
static FORCE_INLINE int64_t taosGetInt64Aligned(int64_t *pVal) {
#ifdef CHECK_ALIGNMENT
  if ((((uintptr_t)pVal) & 7) == 0) return *pVal;
#endif
  int64_t val;
  memcpy(&val, pVal, sizeof(int64_t));
  return val;
}

static FORCE_INLINE uint64_t taosGetUInt64Aligned(uint64_t *pVal) {
#ifdef CHECK_ALIGNMENT
  if ((((uintptr_t)pVal) & 7) == 0) return *pVal;
#endif
  uint64_t val;
  memcpy(&val, pVal, sizeof(uint64_t));
  return val;
}

static FORCE_INLINE float taosGetFloatAligned(float *pVal) {
#ifdef CHECK_ALIGNMENT
  if ((((uintptr_t)pVal) & 7) == 0) return *pVal;
#endif
  float val;
  memcpy(&val, pVal, sizeof(float));
  return val;
}

static FORCE_INLINE double taosGetDoubleAligned(double *pVal) {
#ifdef CHECK_ALIGNMENT
  if ((((uintptr_t)pVal) & 7) == 0) return *pVal;
#endif
  double val;
  memcpy(&val, pVal, sizeof(double));
  return val;
}

static FORCE_INLINE void taosSetInt64Aligned(int64_t *p, int64_t val) {
#ifdef CHECK_ALIGNMENT
  if ((((uintptr_t)p) & 7) == 0) {
    *p = val;
    return;
  }
#endif
  memcpy(p, &val, sizeof(int64_t));
}

static FORCE_INLINE void taosSetUInt64Aligned(uint64_t *p, uint64_t val) {
#ifdef CHECK_ALIGNMENT
  if ((((uintptr_t)p) & 7) == 0) {
    *p = val;
    return;
  }
#endif
  memcpy(p, &val, sizeof(uint64_t));
}

static FORCE_INLINE void taosSetPInt64Aligned(int64_t *to, int64_t *from) {
#ifdef CHECK_ALIGNMENT
  if ((((uintptr_t)from) & 7) == 0 && ((uintptr_t)to & 7) == 0) {
    *to = *from;
    return;
  }
#endif
  memcpy(to, from, sizeof(int64_t));
}

static FORCE_INLINE void taosSetPFloatAligned(float *to, float *from) {
#ifdef CHECK_ALIGNMENT
  if ((((uintptr_t)from) & 7) == 0 && ((uintptr_t)to & 7) == 0) {
    *to = *from;
    return;
  }
#endif
  memcpy(to, from, sizeof(float));
}

static FORCE_INLINE void taosSetPDoubleAligned(double *to, double *from) {
#ifdef CHECK_ALIGNMENT
  if ((((uintptr_t)from) & 7) == 0 && ((uintptr_t)to & 7) == 0) {
    *to = *from;
    return;
  }
#endif
  memcpy(to, from, sizeof(double));
}

static FORCE_INLINE void taosSetPUInt64Aligned(uint64_t *to, uint64_t *from) {
#ifdef CHECK_ALIGNMENT
  if ((((uintptr_t)from) & 7) == 0 && ((uintptr_t)to & 7) == 0) {
    *to = *from;
    return;
  }
#endif
  memcpy(to, from, sizeof(uint64_t));
}

#define TAOS_SET_OBJ_ALIGNED(pTo, vFrom)  memcpy((pTo), &(vFrom), sizeof(*(pTo)))
#define TAOS_SET_POBJ_ALIGNED(pTo, pFrom) memcpy((pTo), (pFrom), sizeof(*(pTo)))
#else
static FORCE_INLINE int64_t  taosGetInt64Aligned(int64_t *pVal) { return *pVal; }
static FORCE_INLINE uint64_t taosGetUInt64Aligned(uint64_t *pVal) { return *pVal; }
static FORCE_INLINE float    taosGetFloatAligned(float *pVal) { return *pVal; }
static FORCE_INLINE double   taosGetDoubleAligned(double *pVal) { return *pVal; }
static FORCE_INLINE void     taosSetInt64Aligned(int64_t *p, int64_t val) { *p = val; }
static FORCE_INLINE void     taosSetUInt64Aligned(uint64_t *p, uint64_t val) { *p = val; }
static FORCE_INLINE void     taosSetPInt64Aligned(int64_t *to, int64_t *from) { *to = *from; }
static FORCE_INLINE void     taosSetPFloatAligned(float *to, float *from) { *to = *from; }
static FORCE_INLINE void     taosSetPDoubleAligned(double *to, double *from) { *to = *from; }
static FORCE_INLINE void     taosSetPUInt64Aligned(uint64_t *to, uint64_t *from) { *to = *from; }
#define TAOS_SET_OBJ_ALIGNED(pTo, vFrom)  *(pTo) = (vFrom)
#define TAOS_SET_POBJ_ALIGNED(pTo, pFrom) *(pTo) = *(pFrom)
#endif

static FORCE_INLINE void taosEncryptPass(uint8_t *inBuf, size_t inLen, char *target) {
  T_MD5_CTX context;
  tMD5Init(&context);
  tMD5Update(&context, inBuf, (uint32_t)inLen);
  tMD5Final(&context);
  (void)memcpy(target, context.digest, tListLen(context.digest));
}

static FORCE_INLINE void taosEncryptPass_c(uint8_t *inBuf, size_t len, char *target) {
  T_MD5_CTX context;
  tMD5Init(&context);
  tMD5Update(&context, inBuf, (uint32_t)len);
  tMD5Final(&context);
  char buf[TSDB_PASSWORD_LEN + 1];

  buf[TSDB_PASSWORD_LEN] = 0;
  (void)sprintf(buf, "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x", context.digest[0],
                context.digest[1], context.digest[2], context.digest[3], context.digest[4], context.digest[5],
                context.digest[6], context.digest[7], context.digest[8], context.digest[9], context.digest[10],
                context.digest[11], context.digest[12], context.digest[13], context.digest[14], context.digest[15]);
  (void)memcpy(target, buf, TSDB_PASSWORD_LEN);
}

static FORCE_INLINE int32_t taosHashBinary(char *pBuf, int32_t len) {
  uint64_t hashVal = MurmurHash3_64(pBuf, len);
  return sprintf(pBuf, "%" PRIu64, hashVal);
}

static FORCE_INLINE int32_t taosCreateMD5Hash(char *pBuf, int32_t len) {
  T_MD5_CTX ctx;
  tMD5Init(&ctx);
  tMD5Update(&ctx, (uint8_t *)pBuf, len);
  tMD5Final(&ctx);
  char   *p = pBuf;
  int32_t resLen = 0;
  return sprintf(pBuf, "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x", ctx.digest[0], ctx.digest[1],
                 ctx.digest[2], ctx.digest[3], ctx.digest[4], ctx.digest[5], ctx.digest[6], ctx.digest[7],
                 ctx.digest[8], ctx.digest[9], ctx.digest[10], ctx.digest[11], ctx.digest[12], ctx.digest[13],
                 ctx.digest[14], ctx.digest[15]);
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
      (void)strncpy(tbName, tbname, offset);
    }
    if (suffix < 0) {
      (void)strncpy(tbName + offset, tbname + tblen + suffix, -1 * suffix);
      offset += -1 * suffix;
    }
    return MurmurHash3_32(tbName, offset);
  }
}

/*
 * LIKELY and UNLIKELY macros for branch predication hints. Use them judiciously
 * only in very hot code paths. Misuse or abuse can lead to performance degradation.
 */
#if __GNUC__ >= 3
#define LIKELY(x)   __builtin_expect((x) != 0, 1)
#define UNLIKELY(x) __builtin_expect((x) != 0, 0)
#else
#define LIKELY(x)   ((x) != 0)
#define UNLIKELY(x) ((x) != 0)
#endif

#define TAOS_CHECK_ERRNO(CODE)         \
  do {                                 \
    terrno = (CODE);                   \
    if (terrno != TSDB_CODE_SUCCESS) { \
      terrln = __LINE__;               \
      goto _exit;                      \
    }                                  \
  } while (0)

#define TSDB_CHECK_CODE(CODE, LINO, LABEL)       \
  do {                                           \
    if (UNLIKELY(TSDB_CODE_SUCCESS != (CODE))) { \
      LINO = __LINE__;                           \
      goto LABEL;                                \
    }                                            \
  } while (0)

#define QUERY_CHECK_CODE TSDB_CHECK_CODE

#define TSDB_CHECK_CONDITION(condition, CODE, LINO, LABEL, _ERRNO) \
  if (UNLIKELY(!(condition))) {                                    \
    (CODE) = (_ERRNO);                                             \
    (LINO) = __LINE__;                                             \
    goto LABEL;                                                    \
  }

#define QUERY_CHECK_CONDITION TSDB_CHECK_CONDITION

#define TSDB_CHECK_NULL(ptr, CODE, LINO, LABEL, _ERRNO) \
  if (UNLIKELY((ptr) == NULL)) {                        \
    (CODE) = (_ERRNO);                                  \
    (LINO) = __LINE__;                                  \
    goto LABEL;                                         \
  }

#define QUERY_CHECK_NULL TSDB_CHECK_NULL

#define ARRAY_SIZE(a) (sizeof(a) / sizeof(a[0]))

#define VND_CHECK_CODE(CODE, LINO, LABEL) TSDB_CHECK_CODE(CODE, LINO, LABEL)

#define TCONTAINER_OF(ptr, type, member) ((type *)((char *)(ptr) - offsetof(type, member)))

#define TAOS_GET_TERRNO(code) (terrno == 0 ? code : terrno)

#define TAOS_RETURN(CODE)     \
  do {                        \
    return (terrno = (CODE)); \
  } while (0)

#define TAOS_CHECK_RETURN(CMD)      \
  do {                              \
    int32_t __c = (CMD);            \
    if (__c != TSDB_CODE_SUCCESS) { \
      TAOS_RETURN(__c);             \
    }                               \
  } while (0)

#define TAOS_CHECK_RETURN_SET_CODE(CMD, CODE, _ERRNO) \
  do {                                                \
    int32_t __c = (CMD);                              \
    if (__c != TSDB_CODE_SUCCESS) {                   \
      (CODE) = (_ERRNO);                              \
      TAOS_RETURN(__c);                               \
    }                                                 \
  } while (0)

#define TAOS_CHECK_RETURN_WITH_RELEASE(CMD, PTR1, PTR2) \
  do {                                                  \
    int32_t __c = (CMD);                                \
    if (__c != TSDB_CODE_SUCCESS) {                     \
      sdbRelease(PTR1, PTR2);                           \
      TAOS_RETURN(__c);                                 \
    }                                                   \
  } while (0)

#define TAOS_CHECK_RETURN_WITH_FREE(CMD, PTR) \
  do {                                        \
    int32_t __c = (CMD);                      \
    if (__c != TSDB_CODE_SUCCESS) {           \
      taosMemoryFree(PTR);                    \
      TAOS_RETURN(__c);                       \
    }                                         \
  } while (0)

#define TAOS_CHECK_GOTO(CMD, LINO, LABEL) \
  do {                                    \
    code = (CMD);                         \
    if (code != TSDB_CODE_SUCCESS) {      \
      if (LINO) {                         \
        *((int32_t *)(LINO)) = __LINE__;  \
      }                                   \
      goto LABEL;                         \
    }                                     \
  } while (0)

#define TAOS_CHECK_EXIT(CMD)        \
  do {                              \
    code = (CMD);                   \
    if (code < TSDB_CODE_SUCCESS) { \
      lino = __LINE__;              \
      goto _exit;                   \
    }                               \
  } while (0)

#define TAOS_CHECK_EXIT_SET_CODE(CMD, CODE, _ERRNO) \
  do {                                              \
    code = (CMD);                                   \
    if (code < TSDB_CODE_SUCCESS) {                 \
      (CODE) = (_ERRNO);                            \
      lino = __LINE__;                              \
      goto _exit;                                   \
    }                                               \
  } while (0)

#define TAOS_UNUSED(expr) (void)(expr)

bool taosIsBigChar(char c);
bool taosIsSmallChar(char c);
bool taosIsNumberChar(char c);
bool taosIsSpecialChar(char c);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_UTIL_H_*/
