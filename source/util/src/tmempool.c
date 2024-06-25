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

#define _DEFAULT_SOURCE
#include "tmempool.h"
#include "tmempoolInt.h"
#include "tlog.h"
#include "tutil.h"

static SArray* gMPoolList;
static TdThreadOnce  gMPoolInit = PTHREAD_ONCE_INIT;
static TdThreadMutex gMPoolMutex;

int32_t memPoolCheckCfg(SMemPoolCfg* cfg) {
  if (cfg->chunkSize < MEMPOOL_MIN_CHUNK_SIZE || cfg->chunkSize > MEMPOOL_MAX_CHUNK_SIZE) {
    uError("invalid memory pool chunkSize:%d", cfg->chunkSize);
    return TSDB_CODE_INVALID_MEM_POOL_PARAM;
  }

  if (cfg->evicPolicy <= 0 || cfg->evicPolicy >= E_EVICT_MAX_VALUE) {
    uError("invalid memory pool evicPolicy:%d", cfg->evicPolicy);
    return TSDB_CODE_INVALID_MEM_POOL_PARAM;
  }

  if (cfg->threadNum <= 0) {
    uError("invalid memory pool threadNum:%d", cfg->threadNum);
    return TSDB_CODE_INVALID_MEM_POOL_PARAM;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t memPoolAddChunkCache(SMemPool* pPool) {
  if (0 == pPool->chunkCacheUnitNum) {
    pPool->chunkCacheUnitNum = TMAX(pPool->maxChunkNum / 10, MEMPOOL_CHUNKPOOL_MIN_BATCH_SIZE);
  }

  if (NULL == pPool->pChunkCache) {
    pPool->pChunkCache = taosArrayInit(10, sizeof(SChunkCache));
    if (NULL == pPool->pChunkCache) {
      uError("taosArrayInit chunkPool failed");
      MP_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  SChunkCache* pChunkCache = taosArrayReserve(pPool->pChunkCache, 1);
  pChunkCache->chunkNum = pPool->chunkCacheUnitNum;
  pChunkCache->pChunks = taosMemoryCalloc(pChunkCache->chunkNum, sizeof(*pChunkCache->pChunks));
  if (NULL == pChunkCache->pChunks) {
    uError("calloc %d chunks in pool failed", pChunkCache->chunkNum);
    MP_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  atomic_add_fetch_64(&pPool->allocChunkCacheNum, pChunkCache->chunkNum);

  return TSDB_CODE_SUCCESS;
}

int32_t memPoolGetChunkCache(SMemPool* pPool, SChunkCache** pCache) {
  while (true) {
    SChunkCache* cache = (SChunkCache*)taosArrayGetLast(pPool->pChunkCache);
    if (NULL != cache && cache->idleOffset < cache->chunkNum) {
      *pCache = cache;
      break;
    }

    MP_ERR_RET(memPoolAddChunkCache(pPool));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t memPoolGetIdleChunkFromCache() {

}

int32_t memPoolEnsureMinFreeChunk(SMemPool* pPool) {
  if (E_EVICT_ALL == pPool->cfg.evicPolicy) {
    return TSDB_CODE_SUCCESS;
  }

  if (pPool->freeChunkNum >= pPool->freeChunkReserveNum) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t toAddNum = pPool->freeChunkReserveNum - pPool->freeChunkNum;
  for (int32_t i = 0; i < toAddNum; ++i) {
    memPoolGetIdleChunk(pPool);
  }
  
}

int32_t memPoolInit(SMemPool* pPool, char* poolName, SMemPoolCfg* cfg) {
  MP_ERR_RET(memPoolCheckCfg(cfg));
  
  memcpy(&pPool->cfg, &cfg, sizeof(cfg));
  
  pPool->name = taosStrdup(poolName);
  if (NULL == pPool->name) {
    uError("calloc memory pool name %s failed", poolName);
    MP_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  pPool->maxChunkNum = cfg->maxSize / cfg->chunkSize;
  if (pPool->maxChunkNum <= 0) {
    uError("invalid memory pool max chunk num, maxSize:%" PRId64 ", chunkSize:%d", cfg->maxSize, cfg->chunkSize);
    return TSDB_CODE_INVALID_MEM_POOL_PARAM;
  }

  pPool->threadChunkReserveNum = 1;
  pPool->freeChunkReserveNum = TMIN(cfg->threadNum * pPool->threadChunkReserveNum, pPool->maxChunkNum);

  MP_ERR_RET(memPoolAddChunkCache(pPool));

  MP_ERR_RET(memPoolEnsureMinFreeChunk(pPool));
}


void taosMemPoolModInit(void) {
  taosThreadMutexInit(&gMPoolMutex, NULL);

  gMPoolList = taosArrayInit(10, POINTER_BYTES);
}

int32_t taosMemPoolOpen(char* poolName, SMemPoolCfg cfg, void** poolHandle) {
  int32_t code = TSDB_CODE_SUCCESS;
  SMemPool* pPool = NULL;
  
  taosThreadOnce(&gMPoolInit, taosMemPoolModInit);
  if (NULL == gMPoolList) {
    uError("init memory pool failed");
    MP_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }

  SMemPool* pPool = taosMemoryCalloc(1, sizeof(SMemPool));
  if (NULL == pPool) {
    uError("calloc memory pool failed");
    MP_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }

  pPool->slotId = -1;
  MP_ERR_JRET(memPoolInit(pPool, poolName, &cfg))

  

_return:

  if (TSDB_CODE_SUCCESS != code) {
    taosMemPoolClose(pPool);
  }

}

void   *taosMemPoolMalloc(void* poolHandle, int64_t size, char* fileName, int32_t lineNo) {

}

void   *taosMemPoolCalloc(void* poolHandle, int64_t num, int64_t size, char* fileName, int32_t lineNo) {

}

void   *taosMemPoolRealloc(void* poolHandle, void *ptr, int64_t size) {

}

char   *taosMemPoolStrdup(void* poolHandle, const char *ptr) {

}

void    taosMemPoolFree(void* poolHandle, void *ptr) {

}

int32_t taosMemPoolGetMemorySize(void* poolHandle, void *ptr, int64_t* size) {

}

void   *taosMemPoolMallocAlign(void* poolHandle, uint32_t alignment, int64_t size) {

}

void    taosMemPoolClose(void* poolHandle) {

}

void    taosMemPoolModDestroy(void) {

}


