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

static SArray* gMPoolList = NULL;
static TdThreadOnce  gMPoolInit = PTHREAD_ONCE_INIT;
static TdThreadMutex gMPoolMutex;
static threadlocal void* threadPoolHandle = NULL;
static threadlocal void* threadPoolSession = NULL;


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

void memPoolFreeChunkCache(SMPChunkCache* pCache) {
  //TODO
}

int32_t memPoolAddChunkCache(SMemPool* pPool, SMPChunkCache* pTail) {
  if (0 == pPool->chunkCacheUnitNum) {
    pPool->chunkCacheUnitNum = TMAX(pPool->maxChunkNum / 10, MP_CHUNKPOOL_MIN_BATCH_SIZE);
  }

  SMPChunkCache* pCache = NULL;
  if (NULL == pPool->pChunkCacheHead) {
    pPool->pChunkCacheHead = taosMemCalloc(1, sizeof(*pPool->pChunkCacheHead));
    if (NULL == pPool->pChunkCacheHead) {
      uError("malloc chunkCache failed");
      MP_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }

    pCache = pPool->pChunkCacheHead;
  } else {
    pCache = (SMPChunkCache*)taosMemCalloc(1, sizeof(SMPChunkCache));
  }

  pCache->chunkNum = pPool->chunkCacheUnitNum;
  pCache->pChunks = taosMemoryCalloc(pCache->chunkNum, sizeof(*pCache->pChunks));
  if (NULL == pCache->pChunks) {
    uError("calloc %d chunks in cache failed", pCache->chunkNum);
    MP_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  if (atomic_val_compare_exchange_ptr(&pPool->pChunkCacheTail, pTail, pCache) != pTail) {
    memPoolFreeChunkCache(pCache);
    return TSDB_CODE_SUCCESS;
  }

  atomic_add_fetch_64(&pPool->allocChunkCacheNum, pCache->chunkNum);

  return TSDB_CODE_SUCCESS;
}

int32_t memPoolGetIdleChunk(SMemPool* pPool, SMPChunkCache** ppCache, SMPChunk** ppChunk) {
  SMPChunkCache* pCache = NULL;
  SMPChunk* pChunk = NULL;
  
  while (true) {
    pChunk = (SMPChunk*)atomic_load_ptr(&pPool->pIdleChunkList);
    if (NULL == pChunk) {
      break;
    }

    if (atomic_val_compare_exchange_ptr(&pPool->pIdleChunkList, pChunk, pChunk->pNext) != pChunk) {
      continue;
    }

    pChunk->pNext = NULL;
    goto _return;
  }

  while (true) {
    pCache = atomic_load_ptr(&pPool->pChunkCacheTail);
    int32_t offset = atomic_fetch_add_32(&pCache->idleOffset, 1);
    if (offset < pCache->chunkNum) {
      pChunk = pCache->pChunks + offset;
      break;
    } else {
      atomic_sub_fetch_32(&pCache->idleOffset, 1);
    }
    
    MP_ERR_RET(memPoolAddChunkCache(pPool, pCache));
  }

_return:

  if (NULL != ppCache) {
    *ppCache = pCache;
  }

  *ppChunk = pChunk;

  return TSDB_CODE_SUCCESS;
}

int32_t memPoolNewReadyChunk(SMemPool* pPool, SMPChunkCache** ppCache, SMPChunk** ppChunk) {
  SMPChunk* pChunk = NULL;
  MP_ERR_RET(memPoolGetIdleChunk(pPool, ppCache, &pChunk));
  
  pChunk->pMemStart = taosMemMalloc(pPool->cfg.chunkSize);
  if (NULL == pChunk->pMemStart) {
    uError("add new chunk, memory malloc %d failed", pPool->cfg.chunkSize);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t memPoolAddReadyChunk(SMemPool* pPool, int32_t num) {
  SMPChunkCache* pCache = NULL;
  SMPChunk* pChunk = NULL;
  for (int32_t i = 0; i < num; ++i) {
    MP_ERR_RET(memPoolNewReadyChunk(pPool, &pCache, &pChunk));

    if (NULL == pPool->readyChunkTail) {
      pPool->readyChunkHead = pChunk;
      pPool->readyChunkTail = pChunk;
    } else {
      pPool->readyChunkTail->pNext = pChunk;
    }

    atomic_add_fetch_32(&pPool->readyChunkNum, 1);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t memPoolEnsureReadyChunks(SMemPool* pPool) {
  if (E_EVICT_ALL == pPool->cfg.evicPolicy) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t readyMissNum = pPool->readyChunkReserveNum - atomic_load_32(&pPool->readyChunkNum);
  if (readyMissNum <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  MP_ERR_RET(memPoolAddReadyChunk(pPool, readyMissNum));

  return TSDB_CODE_SUCCESS;
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
  pPool->readyChunkReserveNum = TMIN(cfg->threadNum * pPool->threadChunkReserveNum, pPool->maxChunkNum);

  MP_ERR_RET(memPoolAddChunkCache(pPool));

  MP_ERR_RET(memPoolGetIdleChunk(pPool, NULL, &pPool->readyChunkHead));
  pPool->readyChunkTail = pPool->readyChunkHead;

  MP_ERR_RET(memPoolEnsureReadyChunks(pPool));

  return TSDB_CODE_SUCCESS;
}

void memPoolNotifyLowReadyChunk(SMemPool* pPool) {

}

int32_t memPoolGetReadyChunk(SMemPool* pPool, SMPChunk** ppChunk) {
  SMPChunkCache* pCache = NULL;
  SMPChunk* pChunk = NULL;
  int32_t readyChunkNum = atomic_sub_fetch_32(&pPool->readyChunkNum, 1);
  if (readyChunkNum >= 0) {
    if (atomic_add_fetch_32(&pPool->readyChunkGotNum, 1) == pPool->readyChunkLowNum) {
      memPoolNotifyLowReadyChunk(pPool);
    }

    pChunk = (SMPChunk*)atomic_load_ptr(&pPool->readyChunkHead->pNext);
    while (atomic_val_compare_exchange_ptr(&pPool->readyChunkHead->pNext, pChunk, pChunk->pNext) != pChunk) {
      pChunk = (SMPChunk*)atomic_load_ptr(&pPool->readyChunkHead->pNext);
    }

    *ppChunk = pChunk;

    return TSDB_CODE_SUCCESS;
  }

  MP_RET(memPoolNewReadyChunk(pPool, NULL, ppChunk));
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

  MP_ERR_JRET(memPoolInit(pPool, poolName, &cfg));

  taosThreadMutexLock(&gMPoolMutex);
  
  taosArrayPush(gMPoolList, &pPool);
  pPool->slotId = taosArrayGetSize(gMPoolList) - 1;
  
  taosThreadMutexUnlock(&gMPoolMutex);

_return:

  if (TSDB_CODE_SUCCESS != code) {
    taosMemPoolClose(pPool);
  }

  return code;
}

void taosMemPoolDestroySession(void* session) {
  SMPSession* pSession = (SMPSession*)session;
  //TODO;
}

int32_t taosMemPoolInitSession(void* poolHandle, void** ppSession) {
  int32_t code = TSDB_CODE_SUCCESS;
  SMemPool* pPool = (SMemPool*)poolHandle;
  SMPSession* pSession = (SMPSession*)taosMemCalloc(1, sizeof(SMPSession));
  if (NULL == pSession) {
    uError("calloc memory pool session failed");
    MP_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }

  MP_ERR_JRET(memPoolGetReadyChunk(pPool, &pSession->srcChunkHead));

  pSession->srcChunkTail = pSession->srcChunkHead;
  pSession->srcChunkNum = 1;
  pSession->allocChunkNum = 1;
  pSession->allocChunkMemSize = pPool->cfg.chunkSize;

_return:

  if (TSDB_CODE_SUCCESS != code) {
    taosMemPoolDestroySession(pSession);
    pSession = NULL;
  }

  *ppSession = pSession;

  return code;
}

void   *taosMemPoolMalloc(void* poolHandle, void* session, int64_t size, char* fileName, int32_t lineNo) {
  SMemPool* pPool = (SMemPool*)poolHandle;
  SMPSession* pSession = (SMPSession*)session;

}

void   *taosMemPoolCalloc(void* poolHandle, void* session, int64_t num, int64_t size, char* fileName, int32_t lineNo) {

}

void   *taosMemPoolRealloc(void* poolHandle, void* session, void *ptr, int64_t size) {

}

char   *taosMemPoolStrdup(void* poolHandle, void* session, const char *ptr) {

}

void    taosMemPoolFree(void* poolHandle, void* session, void *ptr) {

}

int32_t taosMemPoolGetMemorySize(void* poolHandle, void* session, void *ptr, int64_t* size) {

}

void   *taosMemPoolMallocAlign(void* poolHandle, void* session, uint32_t alignment, int64_t size) {

}

void    taosMemPoolClose(void* poolHandle) {

}

void    taosMemPoolModDestroy(void) {

}


