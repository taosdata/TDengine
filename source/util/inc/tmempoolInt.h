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

#define MP_CHUNK_CACHE_ALLOC_BATCH_SIZE 1000
#define MP_NSCHUNK_CACHE_ALLOC_BATCH_SIZE 500
#define MP_SESSION_CACHE_ALLOC_BATCH_SIZE 100

#define MP_MAX_KEEP_FREE_CHUNK_NUM  1000
#define MP_MAX_MALLOC_MEM_SIZE      0xFFFFFFFFFF


// FLAGS AREA
#define MP_CHUNK_FLAG_IN_USE   (1 << 0)
#define MP_CHUNK_FLAG_NS_CHUNK (1 << 1)

#define MP_DBG_FLAG_LOG_MALLOC_FREE (1 << 0)

#define MP_MEM_HEADER_FLAG_NS_CHUNK (1 << 0)

typedef struct SMPMemHeader {
  uint64_t flags:3;
  uint64_t size:5;
} SMPMemHeader;

typedef struct SMPMemTailer {

} SMPMemTailer;

typedef struct SMPListNode {
  void    *pNext;
} SMPListNode;

typedef struct SMPChunk {
  SMPListNode list;
  char    *pMemStart;
  int32_t  flags;
  /* KEEP ABOVE SAME WITH SMPNSChunk */

  uint32_t offset;
} SMPChunk;

typedef struct SMPNSChunk {
  SMPListNode list;
  char    *pMemStart;
  int32_t  flags;
  /* KEEP ABOVE SAME WITH SMPChunk */

  uint64_t offset;
  uint64_t memBytes;
} SMPNSChunk;


typedef struct SMPCacheGroup {
  int32_t        nodesNum;
  int32_t        idleOffset;
  void          *pNodes;
  void*          pNext;
} SMPCacheGroup;

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
  SMPListNode        list;
  int64_t            allocChunkNum;
  int64_t            allocChunkMemSize;
  int64_t            allocMemSize;
  int64_t            reUseChunkNum;
  
  int32_t            srcChunkNum;
  SMPChunk          *srcChunkHead;
  SMPChunk          *srcChunkTail;

  int32_t            inUseChunkNum;
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

typedef struct SMPCacheGroupInfo {
  int16_t            nodeSize;
  int64_t            allocNum;
  int32_t            groupNum;
  SMPCacheGroup     *pGrpHead;
  SMPCacheGroup     *pGrpTail;
  void              *pIdleList;
} SMPCacheGroupInfo;

typedef struct SMPDebugInfo {
  int64_t  flags;
} SMPDebugInfo;

typedef struct SMemPool {
  char              *name;
  int16_t            slotId;
  SMemPoolCfg        cfg;
  int32_t            maxChunkNum;
  SMPDebugInfo       dbgInfo;

  int16_t            maxDiscardSize;
  double             threadChunkReserveNum;
  int64_t            allocChunkNum;
  int64_t            allocChunkSize;
  int64_t            allocNSChunkNum;
  int64_t            allocNSChunkSize;
  int64_t            allocMemSize;

  SMPCacheGroupInfo  chunkCache;
  SMPCacheGroupInfo  NSChunkCache;
  SMPCacheGroupInfo  sessionCache;
  
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

#define MP_GET_FLAG(st, f) ((st) & (f))
#define MP_SET_FLAG(st, f) (st) |= (f)
#define MP_CLR_FLAG(st, f) (st) &= (~f)

enum {
  MP_READ = 1,
  MP_WRITE,
};

#define MP_INIT_MEM_HEADER(_header, _size, _nsChunk)                      \
  do {                                                                    \
    (_header)->size = _size;                                              \
    if (_nsChunk) {                                                       \
      MP_SET_FLAG((_header)->flags, MP_MEM_HEADER_FLAG_NS_CHUNK);         \
    }                                                                     \
  } while (0)

#define MP_ADD_TO_CHUNK_LIST(_chunkHead, _chunkTail, _chunkNum, _chunk)       \
  do {                                                                        \
    if (NULL == _chunkHead) {                                                 \
      _chunkHead = _chunk;                                                    \
      _chunkTail = _chunk;                                                    \
    } else {                                                                  \
      (_chunkTail)->list.pNext = _chunk;                                           \
    }                                                                         \
    (_chunkNum)++;                                                            \
  } while (0)

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
