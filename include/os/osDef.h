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

#ifndef _TD_OS_DEF_H_
#define _TD_OS_DEF_H_

#ifdef __cplusplus
extern "C" {
#endif

#if defined(_TD_DARWIN_64)
  // specific
  typedef int(*__compar_fn_t)(const void *, const void *);

  // for send function in tsocket.c
  #if defined(MSG_NOSIGNAL)
    #undef MSG_NOSIGNAL
  #endif

  #define MSG_NOSIGNAL             0

  #define SO_NO_CHECK              0x1234
  #define SOL_TCP                  0x1234
  #define TCP_KEEPIDLE             0x1234

  #ifndef PTHREAD_MUTEX_RECURSIVE_NP
    #define PTHREAD_MUTEX_RECURSIVE_NP PTHREAD_MUTEX_RECURSIVE
  #endif
#endif

#if defined(_ALPINE)
  typedef int(*__compar_fn_t)(const void *, const void *);
  void  error (int, int, const char *);
  #ifndef PTHREAD_MUTEX_RECURSIVE_NP
    #define  PTHREAD_MUTEX_RECURSIVE_NP PTHREAD_MUTEX_RECURSIVE
  #endif
#endif


#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  char *stpcpy (char *dest, const char *src);
  char *stpncpy (char *dest, const char *src, size_t n);

  // specific
  typedef int (*__compar_fn_t)(const void *, const void *);
  #define ssize_t int
  #define bzero(ptr, size) memset((ptr), 0, (size))
  #define strcasecmp  _stricmp
  #define strncasecmp _strnicmp
  #define wcsncasecmp _wcsnicmp
  #define strtok_r strtok_s
  #define snprintf _snprintf
  #define in_addr_t unsigned long
  #define socklen_t int

  struct tm *localtime_r(const time_t *timep, struct tm *result);
  char *     strptime(const char *buf, const char *fmt, struct tm *tm);
  char *     strsep(char **stringp, const char *delim);
  char *     getpass(const char *prefix);
  char *     strndup(const char *s, size_t n);
  
#endif

#define POINTER_SHIFT(p, b) ((void *)((char *)(p) + (b)))
#define POINTER_DISTANCE(p1, p2) ((char *)(p1) - (char *)(p2)) 

#ifndef NDEBUG
#define ASSERT(x) assert(x)
#else
#define ASSERT(x)
#endif

#ifndef UNUSED
#define UNUSED(x) ((void)(x))
#endif

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

// TODO: replace and remove code below
#define CHAR_BYTES    sizeof(char)
#define SHORT_BYTES   sizeof(int16_t)
#define INT_BYTES     sizeof(int32_t)
#define LONG_BYTES    sizeof(int64_t)
#define FLOAT_BYTES   sizeof(float)
#define DOUBLE_BYTES  sizeof(double)
#define POINTER_BYTES sizeof(void *)  // 8 by default  assert(sizeof(ptrdiff_t) == sizseof(void*)

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

#define DEFAULT_DOUBLE_COMP(x, y)           \
  do {                                      \
    if (isnan(x) && isnan(y)) { return 0; } \
    if (isnan(x)) { return -1; }            \
    if (isnan(y)) { return 1; }             \
    if ((x) == (y)) {                       \
      return 0;                             \
    } else {                                \
      return (x) < (y) ? -1 : 1;            \
    }                                       \
  } while (0)

#define DEFAULT_FLOAT_COMP(x, y) DEFAULT_DOUBLE_COMP(x, y)

#define ALIGN_NUM(n, align) (((n) + ((align)-1)) & (~((align)-1)))

// align to 8bytes
#define ALIGN8(n) ALIGN_NUM(n, 8)

#undef threadlocal
#ifdef _ISOC11_SOURCE
  #define threadlocal _Thread_local
#elif defined(__APPLE__)
  #define threadlocal __thread
#elif defined(__GNUC__) && !defined(threadlocal)
  #define threadlocal __thread
#else
  #define threadlocal __declspec( thread )
#endif

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  #define PRIzu "ld"  
#else
  #define PRIzu "zu"  
#endif

#if defined(_TD_LINUX_64) || defined(_TD_LINUX_32) || defined(_TD_MIPS_64)  || defined(_TD_ARM_32) || defined(_TD_ARM_64)  || defined(_TD_DARWIN_64)
  #if defined(_TD_DARWIN_64)
    // MacOS
    #if !defined(_GNU_SOURCE)
      #define setThreadName(name) do { pthread_setname_np((name)); } while (0)
    #else
      // pthread_setname_np not defined
      #define setThreadName(name)
    #endif
  #else
    // Linux, length of name must <= 16 (the last '\0' included)
    #define setThreadName(name) do { prctl(PR_SET_NAME, (name)); } while (0)
  #endif
#else
  // Windows
  #define setThreadName(name)
#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_DEF_H_*/