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
  uint64_t           clientId;

  int32_t            remainSession;
  
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
  SMPStatItem    memStrdup;
  SMPStatItem    memStrndup;
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
typedef void (*mpReserveFailFp)(int64_t, int32_t);
typedef void (*mpReserveReachFp)(uint64_t, uint64_t, int32_t);
typedef void (*mpCfgUpdate)(void*, void*);

typedef struct SMemPoolCallBack {
  //mpDecConcSessionNum  decSessFp;
  //mpIncConcSessionNum  incSessFp;
  //mpSetConcSessionNum  setSessFp;
  mpReserveFailFp        failFp;
  mpReserveReachFp       reachFp;
  //mpCfgUpdate          cfgUpdateFp;
} SMemPoolCallBack;


typedef struct SMemPoolCfg {
  //bool               reserveMode;
  int64_t            reserveSize;
  //int32_t           *upperLimitSize; //MB
  //int64_t            retireUnitSize;
  int32_t           *jobQuota;       //MB
  int32_t            chunkSize;
  int32_t            threadNum;
  MemPoolEvictPolicy evicPolicy;
  SMemPoolCallBack   cb;
} SMemPoolCfg;

#define MEMPOOL_GET_ALLOC_SIZE(_dstat) ((_dstat)->bytes.memMalloc.succ + (_dstat)->bytes.memCalloc.succ + (_dstat)->bytes.memRealloc.succ + (_dstat)->bytes.memStrdup.succ + (_dstat)->bytes.memStrndup.succ)
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
int32_t taosMemPoolInitSession(void* poolHandle, void** ppSession, void* pJob, char *sessionId);
void    taosMemPoolDestroySession(void* poolHandle, void* session);
int32_t taosMemPoolCallocJob(uint64_t jobId, uint64_t cId, void** ppJob);
void    taosMemPoolCfgUpdate(void* poolHandle, SMemPoolCfg* pCfg);
void    taosMemPoolPrintStat(void* poolHandle, void* session, char* procName);
int32_t taosMemPoolTryLockPool(void* poolHandle, bool readLock);
void    taosMemPoolUnLockPool(void* poolHandle, bool readLock);
void    taosMemPoolGetUsedSizeBegin(void* poolHandle, int64_t* usedSize, bool* needEnd);
void    taosMemPoolGetUsedSizeEnd(void* poolHandle);
int32_t taosMemPoolGetSessionStat(void* session, SMPStatDetail** ppStat, int64_t* allocSize, int64_t* maxAllocSize);
void    taosMemPoolSchedTrim(void);
int32_t taosMemoryPoolInit(mpReserveFailFp, mpReserveReachFp);
int32_t taosMemoryPoolCfgUpdateReservedSize(int32_t newReservedSizeMB);


#define taosMemPoolFreeClear(ptr)   \
  do {                             \
    if (ptr) {                     \
      taosMemPoolFree((void *)ptr); \
      (ptr) = NULL;                \
    }                              \
  } while (0)


extern void* gMemPoolHandle;

#if !defined(BUILD_TEST) && !defined(TD_ASTRA)
extern threadlocal void* threadPoolSession;
extern threadlocal bool  threadPoolEnabled;
extern int8_t tsMemPoolFullFunc;


#define taosEnableMemPoolUsage(_session) do { threadPoolSession = _session; tsEnableRandErr = true;} while (0) 
#define taosDisableMemPoolUsage() do { threadPoolSession = NULL; tsEnableRandErr = false;} while (0) 

#define taosSaveDisableMemPoolUsage(_enable, _randErr) do { (_enable) = threadPoolEnabled; (_randErr) = tsEnableRandErr; threadPoolEnabled = false; tsEnableRandErr = false;} while (0) 
#define taosRestoreEnableMemPoolUsage(_enable, _randErr) do { threadPoolEnabled = (_enable); tsEnableRandErr = (_randErr);} while (0) 


#define taosMemoryMalloc(_size) ((threadPoolEnabled && threadPoolSession) ? (taosMemPoolMalloc(gMemPoolHandle, threadPoolSession, _size, (char*)__FILE__, __LINE__)) : (taosMemMalloc(_size)))
#define taosMemoryCalloc(_num, _size) ((threadPoolEnabled && threadPoolSession) ? (taosMemPoolCalloc(gMemPoolHandle, threadPoolSession, _num, _size, (char*)__FILE__, __LINE__)) : (taosMemCalloc(_num, _size)))
#define taosMemoryRealloc(_ptr, _size) ((threadPoolEnabled && threadPoolSession) ? (taosMemPoolRealloc(gMemPoolHandle, threadPoolSession, _ptr, _size, (char*)__FILE__, __LINE__)) : (taosMemRealloc(_ptr, _size)))
#define taosStrdup(_ptr) ((threadPoolEnabled && threadPoolSession) ? (taosMemPoolStrdup(gMemPoolHandle, threadPoolSession, _ptr, (char*)__FILE__, __LINE__)) : (taosStrdupi(_ptr)))
#define taosStrndup(_ptr, _size) ((threadPoolEnabled && threadPoolSession) ? (taosMemPoolStrndup(gMemPoolHandle, threadPoolSession, _ptr, _size, (char*)__FILE__, __LINE__)) : (taosStrndupi(_ptr, _size)))
#define taosMemoryFree(_ptr) ((threadPoolEnabled && threadPoolSession) ? (taosMemPoolFree(gMemPoolHandle, threadPoolSession, _ptr, (char*)__FILE__, __LINE__)) : (taosMemFree(_ptr)))
#define taosMemorySize(_ptr) ((threadPoolEnabled && threadPoolSession) ? (taosMemPoolGetMemorySize(gMemPoolHandle, threadPoolSession, _ptr, (char*)__FILE__, __LINE__)) : (taosMemSize(_ptr)))
#define taosMemoryTrim(_size, _trimed) ((threadPoolEnabled && threadPoolSession) ? (taosMemPoolTrim(gMemPoolHandle, threadPoolSession, _size, (char*)__FILE__, __LINE__, _trimed)) : (taosMemTrim(_size, _trimed)))
#define taosMemoryMallocAlign(_alignment, _size) ((threadPoolEnabled && threadPoolSession) ? (taosMemPoolMallocAlign(gMemPoolHandle, threadPoolSession, _alignment, _size, (char*)__FILE__, __LINE__)) : (taosMemMallocAlign(_alignment, _size)))
#else
#define taosEnableMemPoolUsage(_session) 
#define taosDisableMemPoolUsage() 
#define taosSaveDisableMemPoolUsage(_enable, _randErr) 
#define taosRestoreEnableMemPoolUsage(_enable, _randErr) 

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
