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
#define malloc  MALLOC_FUNC_TAOS_FORBID
#define calloc  CALLOC_FUNC_TAOS_FORBID
#define realloc REALLOC_FUNC_TAOS_FORBID
#define free    FREE_FUNC_TAOS_FORBID
#ifdef strdup
#undef strdup
#define strdup STRDUP_FUNC_TAOS_FORBID
#endif
#endif  // ifndef ALLOW_FORBID_FUNC
#endif  // if !defined(WINDOWS)

// #define taosMemoryMalloc  malloc
// #define taosMemoryCalloc  calloc
// #define taosMemoryRealloc realloc
// #define taosMemoryFree    free

int32_t taosMemoryDbgInit();
int32_t taosMemoryDbgInitRestore();
void   *taosMemoryMalloc(int64_t size);
void   *taosMemoryCalloc(int64_t num, int64_t size);
void   *taosMemoryRealloc(void *ptr, int64_t size);
char   *taosStrdup(const char *ptr);
void    taosMemoryFree(void *ptr);
int64_t taosMemorySize(void *ptr);
void    taosPrintBackTrace();
void    taosMemoryTrim(int32_t size);
void   *taosMemoryMallocAlign(uint32_t alignment, int64_t size);

#define taosMemoryFreeClear(ptr)   \
  do {                             \
    if (ptr) {                     \
      taosMemoryFree((void *)ptr); \
      (ptr) = NULL;                \
    }                              \
  } while (0)

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_MEMORY_H_*/
