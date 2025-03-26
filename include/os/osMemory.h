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

#ifndef _TD_OS_MEMORY_H_
#define _TD_OS_MEMORY_H_

#ifdef __cplusplus
extern "C" {
#endif

// If the error is in a third-party library, place this header file under the third-party library header file.
// When you want to use this feature, you should find or add the same function in the following sectio
#if !defined(WINDOWS)

#ifndef ALLOW_FORBID_FUNC
#ifdef malloc
#undef malloc
#endif
#define malloc MALLOC_FUNC_TAOS_FORBID
#ifdef calloc
#undef calloc
#endif
#define calloc CALLOC_FUNC_TAOS_FORBID
#ifdef realloc
#undef realloc
#endif
#define realloc REALLOC_FUNC_TAOS_FORBID
#ifdef free
#undef free
#endif
#define free FREE_FUNC_TAOS_FORBID
#ifdef strdup
#undef strdup
#define strdup STRDUP_FUNC_TAOS_FORBID
#endif
#endif  // ifndef ALLOW_FORBID_FUNC
#endif  // if !defined(WINDOWS)

int32_t taosMemoryDbgInit();
int32_t taosMemoryDbgInitRestore();
void   *taosMemMalloc(int64_t size);
void   *taosMemCalloc(int64_t num, int64_t size);
void   *taosMemRealloc(void *ptr, int64_t size);
char   *taosStrdupi(const char *ptr);
char   *taosStrndupi(const char *ptr, int64_t size);
void    taosMemFree(void *ptr);
int64_t taosMemSize(void *ptr);
void    taosPrintBackTrace();
int32_t taosMemTrim(int32_t size, bool* trimed);
void   *taosMemMallocAlign(uint32_t alignment, int64_t size);

#define TAOS_MEMSET(_s, _c, _n) ((void)memset(_s, _c, _n))
#define TAOS_MEMCPY(_d, _s, _n) ((void)memcpy(_d, _s, _n))
#define TAOS_MEMMOVE(_d, _s, _n) ((void)memmove(_d, _s, _n))

#define taosMemFreeClear(ptr)      \
  do {                             \
    if (ptr) {                     \
      taosMemFree((void *)ptr);    \
      (ptr) = NULL;                \
    }                              \
  } while (0)

#include "osMemPool.h"  
#define TAOS_MEMORY_REALLOC(ptr, len)          \
  do {                                         \
    void *tmp = taosMemoryRealloc(ptr, (len)); \
    if (tmp) {                                 \
      (ptr) = tmp;                             \
    } else {                                   \
      taosMemoryFreeClear(ptr);                \
    }                                          \
  } while (0)

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_MEMORY_H_*/
