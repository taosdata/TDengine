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

#ifndef TDENGINE_OS_DEF_H
#define TDENGINE_OS_DEF_H

#ifdef __cplusplus
extern "C" {
#endif

#ifndef WINDOWS
  #ifndef O_BINARY
    #define O_BINARY 0
  #endif
#endif

#define FD_VALID(x) ((x) > STDERR_FILENO)
#define FD_INITIALIZER  ((int32_t)-1)

// #define WCHAR wchar_t

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

// setThreadName
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

#endif
