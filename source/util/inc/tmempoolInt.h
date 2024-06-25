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

#define MEMPOOL_CHUNKPOOL_MIN_BATCH_SIZE 1000
#define MEMPOOL_MAX_KEEP_FREE_CHUNK_NUM  1000

typedef struct SChunk {
  void    *pNext;
  int64_t  flags;
  uint32_t memSize; 
  uint32_t offset;
  void    *pMemStart;
} SChunk;

typedef struct SChunkCache {
  int32_t        chunkNum;
  int32_t        idleOffset;
  SChunk        *pChunks;
} SChunkCache;

typedef struct SMemoryStat {
  int64_t  chunkAlloc;
  int64_t  chunkFree;
  int64_t  memMalloc;
  int64_t  memCalloc;
  int64_t  memRealloc;
  int64_t  strdup;
  int64_t  memFree;
} SMemoryStat;

typedef struct SMemPoolStat {
  SMemPoolMemoryStat times;
  SMemPoolMemoryStat bytes;
} SMemPoolStat;

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
  SArray            *pChunkCache;    // SArray<SChunkCache>
  
  int32_t            freeChunkNum;
  int32_t            freeChunkReserveNum;
  SChunk            *freeChunkHead;
  SChunk            *freeChunkTail;

  int64_t            freeNChunkNum;
  SChunk            *freeNChunkHead;
  SChunk            *freeNChunkTail;

  SMemPoolStat       stat;
} SMemPool;

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
