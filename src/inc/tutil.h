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

#include "os.h"
#include "tmd5.h"

#ifdef __cplusplus
extern "C" {
#endif

#include <assert.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <wchar.h>

#include "tcrc32c.h"
#include "tsdb.h"

#define VALIDFD(x) ((x) > 2)

#define WCHAR wchar_t
#define tfree(x) \
  {              \
    if (x) {     \
      free(x);   \
      x = NULL;  \
    }            \
  }

#ifdef WINDOWS
#define taosCloseSocket(fd) closesocket(fd)
#define taosWriteSocket(fd, buf, len) send(fd, buf, len, 0)
#define taosReadSocket(fd, buf, len) recv(fd, buf, len, 0)
#else
#define taosCloseSocket(x) \
  {                        \
    if (VALIDFD(x)) {      \
      close(x);            \
      x = -1;              \
    }                      \
  }
#define taosWriteSocket(fd, buf, len) write(fd, buf, len)
#define taosReadSocket(fd, buf, len) read(fd, buf, len)
#endif

#define tclose(x) taosCloseSocket(x)

#ifdef ASSERTION
#define ASSERT(x) assert(x)
#else
#define ASSERT(x)
#endif

#ifdef UNUSED
#undefine UNUSED
#endif
#define UNUSED(x) ((void)(x))

#ifdef UNUSED_FUNC
#undefine UNUSED_FUNC
#endif

#ifdef UNUSED_PARAM
#undef UNUSED_PARAM
#endif

#if defined(__GNUC__)
#define UNUSED_PARAM(x) _UNUSED##x __attribute__((unused))
#define UNUSED_FUNC __attribute__((unused))
#else
#define UNUSED_PARAM(x) x
#define UNUSED_FUNC
#endif

#ifdef tListLen
#undefine tListLen
#endif
#define tListLen(x) (sizeof(x) / sizeof((x)[0]))

#if defined(__GNUC__)
#define FORCE_INLINE inline __attribute__((always_inline))
#else
#define FORCE_INLINE
#endif

#define DEFAULT_UNICODE_ENCODEC "UCS-4LE"
  
#ifdef LINUX
#define SWAP(a, b, c)      \
  do {                     \
    typeof(a) __tmp = (a); \
    (a) = (b);             \
    (b) = __tmp;           \
  } while (0)

#define MAX(a, b)            \
  ({                         \
    typeof(a) __a = (a);     \
    typeof(b) __b = (b);     \
    (__a > __b) ? __a : __b; \
  })

#define MIN(a, b)            \
  ({                         \
    typeof(a) __a = (a);     \
    typeof(b) __b = (b);     \
    (__a < __b) ? __a : __b; \
  })

#else

#define SWAP(a, b, c)      \
  do {                     \
    c __tmp = (c)(a);      \
    (a) = (c)(b);          \
    (b) = __tmp;           \
  } while (0)

#define MAX(a,b)  (((a)>(b))?(a):(b))
#define MIN(a,b)  (((a)<(b))?(a):(b))

#endif

#define DEFAULT_COMP(x, y)       \
  do {                           \
    if ((x) == (y)) {            \
      return 0;                  \
    } else {                     \
      return (x) < (y) ? -1 : 1; \
    }                            \
  } while (0);

#define GET_INT8_VAL(x)   (*(int8_t *)(x))
#define GET_INT16_VAL(x)  (*(int16_t *)(x))
#define GET_INT32_VAL(x)  (*(int32_t *)(x))
#define GET_INT64_VAL(x)  (*(int64_t *)(x))
#define GET_FLOAT_VAL(x)  (*(float *)(x))
#define GET_DOUBLE_VAL(x) (*(double *)(x))

#define ALIGN_NUM(n, align) (((n) + ((align)-1)) & (~((align)-1)))

// align to 8bytes
#define ALIGN8(n) ALIGN_NUM(n, 8)

#ifdef WINDOWS
#define MILLISECOND_PER_SECOND (1000i64)
#else
#define MILLISECOND_PER_SECOND (1000L)
#endif

#define MILLISECOND_PER_MINUTE (MILLISECOND_PER_SECOND * 60)
#define MILLISECOND_PER_HOUR   (MILLISECOND_PER_MINUTE * 60)
#define MILLISECOND_PER_DAY    (MILLISECOND_PER_HOUR * 24)
#define MILLISECOND_PER_WEEK   (MILLISECOND_PER_DAY * 7)
#define MILLISECOND_PER_MONTH  (MILLISECOND_PER_DAY * 30)
#define MILLISECOND_PER_YEAR   (MILLISECOND_PER_DAY * 365)

#define POW2(x) ((x) * (x))

int32_t strdequote(char *src);

void strtrim(char *src);

char *strnchr(char *haystack, char needle, int32_t len, bool skipquote);

char **strsplit(char *src, const char *delim, int32_t *num);

void strtolower(char *dst, const char *src);

int64_t strnatoi(char *num, int32_t len);

char* strreplace(const char* str, const char* pattern, const char* rep);

char *paGetToken(char *src, char **token, int32_t *tokenLen);

void taosMsleep(int32_t mseconds);

int32_t taosByteArrayToHexStr(char bytes[], int32_t len, char hexstr[]);

int32_t taosHexStrToByteArray(char hexstr[], char bytes[]);

int64_t str2int64(char *str);

int32_t taosFileRename(char *fullPath, char *suffix, char delimiter, char **dstPath);

int32_t taosInitTimer(void *(*callback)(void *), int32_t ms);

/**
 * murmur hash algorithm
 * @key  usually string
 * @len  key length
 * @seed hash seed
 * @out  an int32 value
 */
uint32_t MurmurHash3_32(const void *key, int32_t len);

bool taosCheckDbName(char *db, char *monitordb);

bool taosMbsToUcs4(char *mbs, int32_t mbs_len, char *ucs4, int32_t ucs4_max_len);

bool taosUcs4ToMbs(void *ucs4, int32_t ucs4_max_len, char *mbs);

bool taosValidateEncodec(char *encodec);

static FORCE_INLINE void taosEncryptPass(uint8_t *inBuf, unsigned int inLen, char *target) {
  MD5_CTX context;
  MD5Init(&context);
  MD5Update(&context, inBuf, inLen);
  MD5Final(&context);
  memcpy(target, context.digest, TSDB_KEY_LEN);
}

#ifdef WINDOWS
int32_t __sync_val_compare_and_swap_32(int32_t *ptr, int32_t oldval, int32_t newval);
int32_t __sync_add_and_fetch_32(int32_t *ptr, int32_t val);
int64_t __sync_val_compare_and_swap_64(int64_t *ptr, int64_t oldval, int64_t newval);
int64_t __sync_add_and_fetch_64(int64_t *ptr, int64_t val);
#ifndef PATH_MAX
#define PATH_MAX 256
#endif
#else
#define __sync_val_compare_and_swap_64 __sync_val_compare_and_swap
#define __sync_val_compare_and_swap_32 __sync_val_compare_and_swap
#define __sync_add_and_fetch_64 __sync_add_and_fetch
#define __sync_add_and_fetch_32 __sync_add_and_fetch
#endif

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TUTIL_H
