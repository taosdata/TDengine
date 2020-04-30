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

#ifndef STDERR_FILENO
#define STDERR_FILENO (2)
#endif

#define FD_VALID(x) ((x) > STDERR_FILENO)
#define FD_INITIALIZER  ((int32_t)-1)

#define WCHAR wchar_t

#define tfree(x) \
  {              \
    if (x) {     \
      free((void*)(x));   \
      x = 0;  \
    }            \
  }

#define tclose(x) taosCloseSocket(x)

// Pointer p drift right by b bytes
#define POINTER_DRIFT(p, b) ((void *)((char *)(p) + (b)))

#ifndef NDEBUG
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
  
#define DEFAULT_COMP(x, y)       \
  do {                           \
    if ((x) == (y)) {            \
      return 0;                  \
    } else {                     \
      return (x) < (y) ? -1 : 1; \
    }                            \
  } while (0)

#define ALIGN_NUM(n, align) (((n) + ((align)-1)) & (~((align)-1)))

// align to 8bytes
#define ALIGN8(n) ALIGN_NUM(n, 8)

#define MILLISECOND_PER_MINUTE (MILLISECOND_PER_SECOND * 60)
#define MILLISECOND_PER_HOUR   (MILLISECOND_PER_MINUTE * 60)
#define MILLISECOND_PER_DAY    (MILLISECOND_PER_HOUR * 24)
#define MILLISECOND_PER_WEEK   (MILLISECOND_PER_DAY * 7)
#define MILLISECOND_PER_MONTH  (MILLISECOND_PER_DAY * 30)
#define MILLISECOND_PER_YEAR   (MILLISECOND_PER_DAY * 365)

#define POW2(x) ((x) * (x))

typedef struct SPair {
  void* first;
  void* sec;
} SPair;

int32_t strdequote(char *src);

void strtrim(char *src);

char *strnchr(char *haystack, char needle, int32_t len, bool skipquote);

char **strsplit(char *src, const char *delim, int32_t *num);

char* strtolower(char *dst, const char *src);

int64_t strnatoi(char *num, int32_t len);

char* strreplace(const char* str, const char* pattern, const char* rep);

char *paGetToken(char *src, char **token, int32_t *tokenLen);

void taosMsleep(int32_t mseconds);

int32_t taosByteArrayToHexStr(char bytes[], int32_t len, char hexstr[]);

int32_t taosHexStrToByteArray(char hexstr[], char bytes[]);

int32_t taosFileRename(char *fullPath, char *suffix, char delimiter, char **dstPath);

/**
 *
 * @param fileNamePattern
 * @param dstPath
 */
void getTmpfilePath(const char *fileNamePattern, char *dstPath);

int32_t taosInitTimer(void (*callback)(int), int32_t ms);
void taosUninitTimer();

bool taosMbsToUcs4(char *mbs, int32_t mbs_len, char *ucs4, int32_t ucs4_max_len);

int tasoUcs4Compare(void* f1_ucs4, void *f2_ucs4, int bytes);

bool taosUcs4ToMbs(void *ucs4, int32_t ucs4_max_len, char *mbs);

bool taosValidateEncodec(const char *encodec);

bool taosGetVersionNumber(char *versionStr, int *versionNubmer);

static FORCE_INLINE void taosEncryptPass(uint8_t *inBuf, unsigned int inLen, char *target) {
  MD5_CTX context;
  MD5Init(&context);
  MD5Update(&context, inBuf, inLen);
  MD5Final(&context);
  memcpy(target, context.digest, TSDB_KEY_LEN);
}

int taosCheckVersion(char *input_client_version, char *input_server_version, int compared_segments);

char *taosIpStr(uint32_t ipInt);

uint32_t ip2uint(const char *const ip_addr);

void taosRemoveDir(char *rootDir);

#define TAOS_ALLOC_MODE_DEFAULT 0
#define TAOS_ALLOC_MODE_RANDOM_FAIL 1
#define TAOS_ALLOC_MODE_DETECT_LEAK 2
void taosSetAllocMode(int mode, const char* path, bool autoDump);
void taosDumpMemoryLeak();

void * tmalloc(size_t size);
void * tcalloc(size_t nmemb, size_t size);
size_t tsizeof(void *ptr);
void   tmemset(void *ptr, int c);
void * trealloc(void *ptr, size_t size);
void   tzfree(void *ptr);

#ifdef TAOS_MEM_CHECK

void *  taos_malloc(size_t size, const char *file, uint32_t line);
void *  taos_calloc(size_t num, size_t size, const char *file, uint32_t line);
void *  taos_realloc(void *ptr, size_t size, const char *file, uint32_t line);
void    taos_free(void *ptr, const char *file, uint32_t line);
char *  taos_strdup(const char *str, const char *file, uint32_t line);
char *  taos_strndup(const char *str, size_t size, const char *file, uint32_t line);
ssize_t taos_getline(char **lineptr, size_t *n, FILE *stream, const char *file, uint32_t line);

#ifndef TAOS_MEM_CHECK_IMPL

#define malloc(size) taos_malloc(size, __FILE__, __LINE__)
#define calloc(num, size) taos_calloc(num, size, __FILE__, __LINE__)
#define realloc(ptr, size) taos_realloc(ptr, size, __FILE__, __LINE__)
#define free(ptr) taos_free(ptr, __FILE__, __LINE__)
#define strdup(str) taos_strdup(str, __FILE__, __LINE__)
#define strndup(str, size) taos_strndup(str, size, __FILE__, __LINE__)
#define getline(lineptr, n, stream) taos_getline(lineptr, n, stream, __FILE__, __LINE__)

#endif  // TAOS_MEM_CHECK_IMPL

#endif // TAOS_MEM_CHECK

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TUTIL_H
