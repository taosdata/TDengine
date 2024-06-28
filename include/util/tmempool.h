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
#ifndef _TD_UTIL_MEMPOOL_H_
#define _TD_UTIL_MEMPOOL_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

#define MEMPOOL_MAX_CHUNK_SIZE (1 << 30)
#define MEMPOOL_MIN_CHUNK_SIZE (1 << 20)

typedef enum MemPoolEvictPolicy{
  E_EVICT_ALL = 1,
  E_EVICT_NONE,
  E_EVICT_AUTO,
  E_EVICT_MAX_VALUE, // no used
} MemPoolEvictPolicy;

typedef struct SMemPoolCfg {
  int64_t            maxSize;
  int32_t            chunkSize;
  int32_t            threadNum;
  MemPoolEvictPolicy evicPolicy;
} SMemPoolCfg;

void    taosMemPoolModInit(void);
int32_t taosMemPoolOpen(char* poolName, SMemPoolCfg* cfg, void** poolHandle);
void   *taosMemPoolMalloc(void* poolHandle, void* session, int64_t size, char* fileName, int32_t lineNo);
void   *taosMemPoolCalloc(void* poolHandle, void* session, int64_t num, int64_t size, char* fileName, int32_t lineNo);
void   *taosMemPoolRealloc(void* poolHandle, void* session, void *ptr, int64_t size, char* fileName, int32_t lineNo);
char   *taosMemPoolStrdup(void* poolHandle, void* session, const char *ptr, char* fileName, int32_t lineNo);
void    taosMemPoolFree(void* poolHandle, void* session, void *ptr, char* fileName, int32_t lineNo);
int64_t taosMemPoolGetMemorySize(void* poolHandle, void* session, void *ptr, char* fileName, int32_t lineNo);
void    taosMemPoolTrim(void* poolHandle, void* session, int32_t size, char* fileName, int32_t lineNo);
void   *taosMemPoolMallocAlign(void* poolHandle, void* session, uint32_t alignment, int64_t size, char* fileName, int32_t lineNo);
void    taosMemPoolClose(void* poolHandle);
void    taosMemPoolModDestroy(void);

#define taosEnableMemoryPoolUsage(_pool, _session) do { threadPoolHandle = _pool; threadPoolSession = _session; } while (0) 
#define taosDisableMemoryPoolUsage() (threadPoolHandle = NULL) 

#define taosMemoryMalloc(_size) ((NULL != threadPoolHandle) ? (taosMemPoolMalloc(threadPoolHandle, threadPoolSession, _size, __FILE__, __LINE__)) : (taosMemMalloc(_size)))
#define taosMemoryCalloc(_num, _size) ((NULL != threadPoolHandle) ? (taosMemPoolCalloc(threadPoolHandle, threadPoolSession, _num, _size, __FILE__, __LINE__)) : (taosMemCalloc(_num, _size)))
#define taosMemoryRealloc(_ptr, _size) ((NULL != threadPoolHandle) ? (taosMemPoolRealloc(threadPoolHandle, threadPoolSession, _ptr, _size, __FILE__, __LINE__)) : (taosMemRealloc(_ptr, _size)))
#define taosStrdup(_ptr) ((NULL != threadPoolHandle) ? (taosMemPoolStrdup(threadPoolHandle, threadPoolSession, _ptr, __FILE__, __LINE__)) : (taosStrdupi(_ptr)))
#define taosMemoryFree(_ptr) ((NULL != threadPoolHandle) ? (taosMemPoolFree(threadPoolHandle, threadPoolSession, _ptr, __FILE__, __LINE__)) : (taosMemFree(_ptr)))
#define taosMemorySize(_ptr) ((NULL != threadPoolHandle) ? (taosMemPoolGetMemorySize(threadPoolHandle, threadPoolSession, _ptr, __FILE__, __LINE__)) : (taosMemSize(_ptr)))
#define taosMemoryTrim(_size) ((NULL != threadPoolHandle) ? (taosMemPoolTrim(threadPoolHandle, threadPoolSession, _size, __FILE__, __LINE__)) : (taosMemTrim(_size)))
#define taosMemoryMallocAlign(_alignment, _size) ((NULL != threadPoolHandle) ? (taosMemPoolMallocAlign(threadPoolHandle, threadPoolSession, _alignment, _size, __FILE__, __LINE__)) : (taosMemMallocAlign(_alignment, _size)))

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

#endif /*_TD_UTIL_MEMPOOL_H_*/
