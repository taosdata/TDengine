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

#ifndef _TD_UTIL_MEMPOOL_INT_H_
#define _TD_UTIL_MEMPOOL_INT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"

#define MP_CHUNKPOOL_MIN_BATCH_SIZE 1000
#define MP_MAX_KEEP_FREE_CHUNK_NUM  1000

#define MP_CHUNK_FLAG_IN_USE   (1 << 0)
#define MP_CHUNK_FLAG_NS_CHUNK (1 << 1)

typedef struct SMPChunk {
  void    *pNext;
  void    *pMemStart;
  int32_t  flags;
  uint32_t offset;
} SMPChunk;

typedef struct SMPNSChunk {
  void    *pNext;
  void    *pMemStart;
  int32_t  flags;
  uint32_t offset;

  uint32_t memBytes;
} SMPNSChunk;


typedef struct SMPChunkCache {
  int32_t        chunkNum;
  int32_t        idleOffset;
  SMPChunk      *pChunks;
  void*          pNext;
} SMPChunkCache;

typedef struct SMPMemoryStat {
  int64_t  chunkAlloc;
  int64_t  chunkFree;
  int64_t  memMalloc;
  int64_t  memCalloc;
  int64_t  memRealloc;
  int64_t  strdup;
  int64_t  memFree;
} SMPMemoryStat;

typedef struct SMPStat {
  SMPMemoryStat times;
  SMPMemoryStat bytes;
} SMPStat;

typedef struct SMPSession {
  int64_t            allocChunkNum;
  int64_t            allocChunkMemSize;
  int64_t            allocMemSize;
  int64_t            reUseChunkNum;
  
  int32_t            srcChunkNum;
  SMPChunk          *srcChunkHead;
  SMPChunk          *srcChunkTail;

  SMPChunk          *inUseChunkHead;
  SMPChunk          *inUseChunkTail;

  SMPNSChunk        *inUseNSChunkHead;
  SMPNSChunk        *inUseNSChunkTail;

  SMPChunk          *reUseChunkHead;
  SMPChunk          *reUseChunkTail;

  SMPNSChunk        *reUseNSChunkHead;
  SMPNSChunk        *reUseNSChunkTail;

  SMPStat            stat;
} SMPSession; 

typedef struct SMemPool {
  char              *name;
  int16_t            slotId;
  SMemPoolCfg        cfg;
  int32_t            maxChunkNum;

  double             threadChunkReserveNum;
  int64_t            allocChunkNum;
  int64_t            allocMemSize;
  int64_t            usedMemSize;

  int64_t            allocChunkCacheNum;
  int32_t            chunkCacheUnitNum;
  SMPChunkCache     *pChunkCacheHead;
  SMPChunkCache     *pChunkCacheTail;
  SMPChunk          *pIdleChunkList;
  
  int32_t            readyChunkNum;
  int32_t            readyChunkReserveNum;
  int32_t            readyChunkLowNum;
  int32_t            readyChunkGotNum;
  SRWLatch           readyChunkLock;
  SMPChunk          *readyChunkHead;
  SMPChunk          *readyChunkTail;

  int64_t              readyNSChunkNum;
  SMPChunk            *readyNSChunkHead;
  SMPChunk            *readyNSChunkTail;

  SMPStat            stat;
} SMemPool;

#define MP_CHUNK_GET_FLAG(st, f) ((st) & (f))
#define MP_CHUNK_SET_FLAG(st, f) (st) |= (f)
#define MP_CHUNK_CLR_FLAG(st, f) (st) &= (~f)

enum {
  MP_READ = 1,
  MP_WRITE,
};


#define MP_LOCK(type, _lock)                                                                                \
  do {                                                                                                       \
    if (MP_READ == (type)) {                                                                                \
      ASSERTS(atomic_load_32((_lock)) >= 0, "invalid lock value before read lock");                          \
      uDebug("MP RLOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);         \
      taosRLockLatch(_lock);                                                                                 \
      uDebug("MP RLOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);         \
      ASSERTS(atomic_load_32((_lock)) > 0, "invalid lock value after read lock");                            \
    } else {                                                                                                 \
      ASSERTS(atomic_load_32((_lock)) >= 0, "invalid lock value before write lock");                         \
      uDebug("MP WLOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);         \
      taosWLockLatch(_lock);                                                                                 \
      uDebug("MP WLOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);         \
      ASSERTS(atomic_load_32((_lock)) == TD_RWLATCH_WRITE_FLAG_COPY, "invalid lock value after write lock"); \
    }                                                                                                        \
  } while (0)

#define MP_UNLOCK(type, _lock)                                                                                 \
  do {                                                                                                          \
    if (MP_READ == (type)) {                                                                                   \
      ASSERTS(atomic_load_32((_lock)) > 0, "invalid lock value before read unlock");                            \
      uDebug("MP RULOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);           \
      taosRUnLockLatch(_lock);                                                                                  \
      uDebug("MP RULOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);           \
      ASSERTS(atomic_load_32((_lock)) >= 0, "invalid lock value after read unlock");                            \
    } else {                                                                                                    \
      ASSERTS(atomic_load_32((_lock)) == TD_RWLATCH_WRITE_FLAG_COPY, "invalid lock value before write unlock"); \
      uDebug("MP WULOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);           \
      taosWUnLockLatch(_lock);                                                                                  \
      uDebug("MP WULOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);           \
      ASSERTS(atomic_load_32((_lock)) >= 0, "invalid lock value after write unlock");                           \
    }                                                                                                           \
  } while (0)


#define MP_ERR_RET(c)                 \
  do {                                \
    int32_t _code = c;                \
    if (_code != TSDB_CODE_SUCCESS) { \
      terrno = _code;                 \
      return _code;                   \
    }                                 \
  } while (0)
  
#define MP_RET(c)                     \
  do {                                \
    int32_t _code = c;                \
    if (_code != TSDB_CODE_SUCCESS) { \
      terrno = _code;                 \
    }                                 \
    return _code;                     \
  } while (0)
  
#define MP_ERR_JRET(c)               \
  do {                               \
    code = c;                        \
    if (code != TSDB_CODE_SUCCESS) { \
      terrno = code;                 \
      goto _return;                  \
    }                                \
  } while (0)



#ifdef __cplusplus
}
#endif

#endif /* _TD_UTIL_MEMPOOL_INT_H_ */
