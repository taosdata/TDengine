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
#ifndef _TD_OS_MEMPOOL_H_
#define _TD_OS_MEMPOOL_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

#define MEMPOOL_MAX_CHUNK_SIZE (1 << 30)
#define MEMPOOL_MIN_CHUNK_SIZE (1 << 20)

typedef enum MemPoolEvictPolicy {
  E_EVICT_ALL = 1,
  E_EVICT_NONE,
  E_EVICT_AUTO,
  E_EVICT_MAX_VALUE, // no used
} MemPoolEvictPolicy;

typedef struct SMemPoolJob {
  uint64_t           jobId;
  int64_t            allocMemSize;
  int64_t            maxAllocMemSize;
} SMemPoolJob;

typedef struct SMPStatItem {
  int64_t  inErr;
  int64_t  exec;
  int64_t  succ;
  int64_t  fail;
} SMPStatItem;

typedef struct SMPStatItemExt {
  int64_t  inErr;
  int64_t  exec;
  int64_t  succ;
  int64_t  fail;
  int64_t  origExec;
  int64_t  origSucc;
  int64_t  origFail;
} SMPStatItemExt;

typedef struct SMPMemoryStat {
  SMPStatItem    memMalloc;
  SMPStatItem    memCalloc;
  SMPStatItemExt memRealloc;
  SMPStatItem    strdup;
  SMPStatItem    strndup;
  SMPStatItem    memFree;
  SMPStatItem    memTrim;

  SMPStatItem    chunkMalloc;
  SMPStatItem    chunkRecycle;
  SMPStatItem    chunkReUse;
  SMPStatItem    chunkFree;
} SMPMemoryStat;

typedef struct SMPStatDetail {
  SMPMemoryStat times;
  SMPMemoryStat bytes;
} SMPStatDetail;


typedef void (*mpDecConcSessionNum)(void);
typedef void (*mpIncConcSessionNum)(void);
typedef void (*mpSetConcSessionNum)(int32_t);
typedef void (*mpRetireJobs)(void*, int64_t, bool, int32_t);
typedef void (*mpRetireJob)(SMemPoolJob*, int32_t);
typedef void (*mpCfgUpdate)(void*, void*);

typedef struct SMemPoolCallBack {
  mpDecConcSessionNum  decSessFp;
  mpIncConcSessionNum  incSessFp;
  mpSetConcSessionNum  setSessFp;
  mpRetireJobs         retireJobsFp;
  mpRetireJob          retireJobFp;
  mpCfgUpdate          cfgUpdateFp;
} SMemPoolCallBack;


typedef struct SMemPoolCfg {
  bool               autoMaxSize;
  int64_t            reserveSize;
  int64_t            retireUnitSize;
  int64_t            freeSize;
  int64_t            jobQuota;
  int32_t            chunkSize;
  int32_t            threadNum;
  MemPoolEvictPolicy evicPolicy;
  SMemPoolCallBack   cb;
} SMemPoolCfg;

#define MEMPOOL_GET_ALLOC_SIZE(_dstat) ((_dstat)->bytes.memMalloc.succ + (_dstat)->bytes.memCalloc.succ + (_dstat)->bytes.memRealloc.succ + (_dstat)->bytes.strdup.succ + (_dstat)->bytes.strndup.succ)
#define MEMPOOL_GET_FREE_SIZE(_dstat) ((_dstat)->bytes.memRealloc.origSucc + (_dstat)->bytes.memFree.succ)
#define MEMPOOL_GET_USED_SIZE(_dstat) (MEMPOOL_GET_ALLOC_SIZE(_dstat) - MEMPOOL_GET_FREE_SIZE(_dstat))


int32_t taosMemPoolOpen(char* poolName, SMemPoolCfg* cfg, void** poolHandle);
void   *taosMemPoolMalloc(void* poolHandle, void* session, int64_t size, char* fileName, int32_t lineNo);
void   *taosMemPoolCalloc(void* poolHandle, void* session, int64_t num, int64_t size, char* fileName, int32_t lineNo);
void   *taosMemPoolRealloc(void* poolHandle, void* session, void *ptr, int64_t size, char* fileName, int32_t lineNo);
char   *taosMemPoolStrdup(void* poolHandle, void* session, const char *ptr, char* fileName, int32_t lineNo);
char   *taosMemPoolStrndup(void* poolHandle, void* session, const char *ptr, int64_t size, char* fileName, int32_t lineNo);
void    taosMemPoolFree(void* poolHandle, void* session, void *ptr, char* fileName, int32_t lineNo);
int64_t taosMemPoolGetMemorySize(void* poolHandle, void* session, void *ptr, char* fileName, int32_t lineNo);
int32_t taosMemPoolTrim(void* poolHandle, void* session, int32_t size, char* fileName, int32_t lineNo, bool* trimed);
void   *taosMemPoolMallocAlign(void* poolHandle, void* session, uint32_t alignment, int64_t size, char* fileName, int32_t lineNo);
void    taosMemPoolClose(void* poolHandle);
void    taosMemPoolModDestroy(void);
void    taosAutoMemoryFree(void *ptr);
int32_t taosMemPoolInitSession(void* poolHandle, void** ppSession, void* pJob);
void    taosMemPoolDestroySession(void* poolHandle, void* session);
int32_t taosMemPoolCallocJob(uint64_t jobId, void** ppJob);
void    taosMemPoolCfgUpdate(void* poolHandle, SMemPoolCfg* pCfg);
void    taosMemPoolPrintStat(void* poolHandle, void* session, char* procName);
void    taosMemPoolGetUsedSizeBegin(void* poolHandle, int64_t* usedSize, bool* needEnd);
void    taosMemPoolGetUsedSizeEnd(void* poolHandle);
bool    taosMemPoolNeedRetireJob(void* poolHandle);
int32_t taosMemPoolGetSessionStat(void* session, SMPStatDetail** ppStat, int64_t* allocSize, int64_t* maxAllocSize);


#define taosMemPoolFreeClear(ptr)   \
  do {                             \
    if (ptr) {                     \
      taosMemPoolFree((void *)ptr); \
      (ptr) = NULL;                \
    }                              \
  } while (0)


#ifndef BUILD_TEST
extern threadlocal void* threadPoolHandle;
extern threadlocal void* threadPoolSession;


#define taosEnableMemoryPoolUsage(_pool, _session) do { threadPoolHandle = _pool; threadPoolSession = _session; tsEnableRandErr = true;} while (0) 
#define taosDisableMemoryPoolUsage() (threadPoolHandle = NULL, tsEnableRandErr = false) 
#define taosSaveDisableMemoryPoolUsage(_handle) do { (_handle) = threadPoolHandle; threadPoolHandle = NULL; } while (0)
#define taosRestoreEnableMemoryPoolUsage(_handle) (threadPoolHandle = (_handle))

#define taosMemoryMalloc(_size) ((NULL != threadPoolHandle) ? (taosMemPoolMalloc(threadPoolHandle, threadPoolSession, _size, (char*)__FILE__, __LINE__)) : (taosMemMalloc(_size)))
#define taosMemoryCalloc(_num, _size) ((NULL != threadPoolHandle) ? (taosMemPoolCalloc(threadPoolHandle, threadPoolSession, _num, _size, (char*)__FILE__, __LINE__)) : (taosMemCalloc(_num, _size)))
#define taosMemoryRealloc(_ptr, _size) ((NULL != threadPoolHandle) ? (taosMemPoolRealloc(threadPoolHandle, threadPoolSession, _ptr, _size, (char*)__FILE__, __LINE__)) : (taosMemRealloc(_ptr, _size)))
#define taosStrdup(_ptr) ((NULL != threadPoolHandle) ? (taosMemPoolStrdup(threadPoolHandle, threadPoolSession, _ptr, (char*)__FILE__, __LINE__)) : (taosStrdupi(_ptr)))
#define taosStrndup(_ptr, _size) ((NULL != threadPoolHandle) ? (taosMemPoolStrndup(threadPoolHandle, threadPoolSession, _ptr, _size, (char*)__FILE__, __LINE__)) : (taosStrndupi(_ptr, _size)))
#define taosMemoryFree(_ptr) ((NULL != threadPoolHandle) ? (taosMemPoolFree(threadPoolHandle, threadPoolSession, _ptr, (char*)__FILE__, __LINE__)) : (taosMemFree(_ptr)))
#define taosMemorySize(_ptr) ((NULL != threadPoolHandle) ? (taosMemPoolGetMemorySize(threadPoolHandle, threadPoolSession, _ptr, (char*)__FILE__, __LINE__)) : (taosMemSize(_ptr)))
#define taosMemoryTrim(_size, _trimed) ((NULL != threadPoolHandle) ? (taosMemPoolTrim(threadPoolHandle, threadPoolSession, _size, (char*)__FILE__, __LINE__, _trimed)) : (taosMemTrim(_size, _trimed)))
#define taosMemoryMallocAlign(_alignment, _size) ((NULL != threadPoolHandle) ? (taosMemPoolMallocAlign(threadPoolHandle, threadPoolSession, _alignment, _size, (char*)__FILE__, __LINE__)) : (taosMemMallocAlign(_alignment, _size)))
#else
#define taosEnableMemoryPoolUsage(_pool, _session) 
#define taosDisableMemoryPoolUsage() 
#define taosSaveDisableMemoryPoolUsage(_handle) 
#define taosRestoreEnableMemoryPoolUsage(_handle) 

#define taosMemoryMalloc(_size) taosMemMalloc(_size)
#define taosMemoryCalloc(_num, _size) taosMemCalloc(_num, _size)
#define taosMemoryRealloc(_ptr, _size) taosMemRealloc(_ptr, _size)
#define taosStrdup(_ptr) taosStrdupi(_ptr)
#define taosStrndup(_ptr, _size) taosStrndupi(_ptr, _size)
#define taosMemoryFree(_ptr) taosMemFree(_ptr)
#define taosMemorySize(_ptr) taosMemSize(_ptr)
#define taosMemoryTrim(_size, _trimed) taosMemTrim(_size, _trimed)
#define taosMemoryMallocAlign(_alignment, _size) taosMemMallocAlign(_alignment, _size)

#endif

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

#endif /*_TD_OS_MEMPOOL_H_*/
