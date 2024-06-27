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

void memPoolFreeChunkGroup(SMPChunkGroup* pGrp) {
  //TODO
}

int32_t memPoolAddChunkGroup(SMemPool* pPool, SMPChunkGroupInfo* pInfo, SMPChunkGroup* pTail) {
  SMPChunkGroup* pGrp = NULL;
  if (NULL == pInfo->pChunkGrpHead) {
    pInfo->pChunkGrpHead = taosMemCalloc(1, sizeof(*pInfo->pChunkGrpHead));
    if (NULL == pInfo->pChunkGrpHead) {
      uError("malloc chunkCache failed");
      MP_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }

    pGrp = pInfo->pChunkGrpHead;
  } else {
    pGrp = (SMPChunkGroup*)taosMemCalloc(1, sizeof(SMPChunkGroup));
  }

  pGrp->chunkNum = pInfo->chunkGroupNum;
  pGrp->pChunks = taosMemoryCalloc(pGrp->chunkNum, pInfo->chunkNodeSize);
  if (NULL == pGrp->pChunks) {
    uError("calloc %d %d chunks in chunk group failed", pGrp->chunkNum, pInfo->chunkNodeSize);
    MP_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  if (atomic_val_compare_exchange_ptr(&pInfo->pChunkGrpTail, pTail, pGrp) != pTail) {
    memPoolFreeChunkGroup(pGrp);
    return TSDB_CODE_SUCCESS;
  }

  atomic_add_fetch_64(&pInfo->allocChunkNum, pGrp->chunkNum);

  return TSDB_CODE_SUCCESS;
}

int32_t memPoolGetIdleChunk(SMemPool* pPool, SMPChunkGroupInfo* pInfo, void** ppChunk) {
  SMPChunkGroup* pGrp = NULL;
  SMPChunk* pChunk = NULL;
  
  while (true) {
    pChunk = (SMPChunk*)atomic_load_ptr(&pInfo->pIdleChunkList);
    if (NULL == pChunk) {
      break;
    }

    if (atomic_val_compare_exchange_ptr(&pInfo->pIdleChunkList, pChunk, pChunk->pNext) != pChunk) {
      continue;
    }

    pChunk->pNext = NULL;
    goto _return;
  }

  while (true) {
    pGrp = atomic_load_ptr(&pInfo->pChunkGrpTail);
    int32_t offset = atomic_fetch_add_32(&pGrp->idleOffset, 1);
    if (offset < pGrp->chunkNum) {
      pChunk = (SMPChunk*)((char*)pGrp->pChunks + offset * pInfo->chunkNodeSize);
      break;
    } else {
      atomic_sub_fetch_32(&pGrp->idleOffset, 1);
    }
    
    MP_ERR_RET(memPoolAddChunkGroup(pPool, pInfo, pGrp));
  }

_return:

  *ppChunk = pChunk;

  return TSDB_CODE_SUCCESS;
}

int32_t memPoolNewChunk(SMemPool* pPool, SMPChunk** ppChunk) {
  SMPChunk* pChunk = NULL;
  MP_ERR_RET(memPoolGetIdleChunk(pPool, &pPool->chunkGrpInfo, &pChunk));
  
  pChunk->pMemStart = taosMemMalloc(pPool->cfg.chunkSize);
  if (NULL == pChunk->pMemStart) {
    uError("add new chunk, memory malloc %d failed", pPool->cfg.chunkSize);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return TSDB_CODE_SUCCESS;
}


int32_t memPoolNewNSChunk(SMemPool* pPool, SMPNSChunk** ppChunk, int64_t chunkSize) {
  SMPNSChunk* pChunk = NULL;
  MP_ERR_RET(memPoolGetIdleChunk(pPool, &pPool->NSChunkGrpInfo, &pChunk));
  
  pChunk->pMemStart = taosMemMalloc(chunkSize);
  if (NULL == pChunk->pMemStart) {
    uError("add new chunk, memory malloc %" PRId64 " failed", chunkSize);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pChunk->memBytes = chunkSize;
  MP_SET_FLAG(pChunk->flags, MP_CHUNK_FLAG_NS_CHUNK);

  return TSDB_CODE_SUCCESS;
}


int32_t memPoolPrepareChunks(SMemPool* pPool, int32_t num) {
  SMPChunkGroup* pGrp = NULL;
  SMPChunk* pChunk = NULL;
  for (int32_t i = 0; i < num; ++i) {
    MP_ERR_RET(memPoolNewChunk(pPool, &pGrp, &pChunk));

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


int32_t memPoolEnsureChunks(SMemPool* pPool) {
  if (E_EVICT_ALL == pPool->cfg.evicPolicy) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t readyMissNum = pPool->readyChunkReserveNum - atomic_load_32(&pPool->readyChunkNum);
  if (readyMissNum <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  MP_ERR_RET(memPoolPrepareChunks(pPool, readyMissNum));

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

  pPool->chunkGrpInfo.chunkGroupNum = TMAX(pPool->maxChunkNum / 10, MP_CHUNKGRP_ALLOC_BATCH_SIZE);
  pPool->chunkGrpInfo.chunkNodeSize = sizeof(SMPChunk);
  pPool->NSChunkGrpInfo.chunkGroupNum = MP_NSCHUNKGRP_ALLOC_BATCH_SIZE;
  pPool->NSChunkGrpInfo.chunkNodeSize = sizeof(SMPNSChunk);

  MP_ERR_RET(memPoolAddChunkGroup(pPool, &pPool->chunkGrpInfo, NULL));

  MP_ERR_RET(memPoolGetIdleChunk(pPool, &pPool->chunkGrpInfo, &pPool->readyChunkHead));
  pPool->readyChunkTail = pPool->readyChunkHead;

  MP_ERR_RET(memPoolEnsureChunks(pPool));

  return TSDB_CODE_SUCCESS;
}

void memPoolNotifyLowChunkNum(SMemPool* pPool) {

}

int32_t memPoolGetChunk(SMemPool* pPool, SMPChunk** ppChunk) {
  SMPChunkGroup* pCache = NULL;
  SMPChunk* pChunk = NULL;
  int32_t readyChunkNum = atomic_sub_fetch_32(&pPool->readyChunkNum, 1);
  if (readyChunkNum >= 0) {
    if (atomic_add_fetch_32(&pPool->readyChunkGotNum, 1) == pPool->readyChunkLowNum) {
      memPoolNotifyLowChunkNum(pPool);
    }

    pChunk = (SMPChunk*)atomic_load_ptr(&pPool->readyChunkHead->pNext);
    while (atomic_val_compare_exchange_ptr(&pPool->readyChunkHead->pNext, pChunk, pChunk->pNext) != pChunk) {
      pChunk = (SMPChunk*)atomic_load_ptr(&pPool->readyChunkHead->pNext);
    }

    *ppChunk = pChunk;

    return TSDB_CODE_SUCCESS;
  }

  MP_RET(memPoolNewChunk(pPool, NULL, ppChunk));
}

int32_t memPoolGetChunkFromSession(SMemPool* pPool, SMPSession* pSession, int64_t size, SMPChunk** ppChunk, SMPChunk** ppPreChunk) {
  SMPChunk* pChunk = pSession->srcChunkHead;
  while (NULL != pChunk) {
    if ((pChunk->offset + size) <= pPool->cfg.chunkSize) {
      *ppChunk = pChunk;
      break;
    }

    *ppPreChunk = pChunk;
    pChunk = (SMPChunk*)pChunk->pNext;
  }

  if (NULL == *ppChunk) {
    *ppPreChunk = NULL;
  }

  return TSDB_CODE_SUCCESS;
}

void* memPoolAllocFromChunk(SMemPool* pPool, SMPSession* pSession, int64_t size) {
  SMPChunk* pChunk = NULL, *preSrcChunk = NULL;
  void* pRes = NULL;
  if (pSession->srcChunkNum > 0) {
    MP_ERR_JRET(memPoolGetChunkFromSession(pPool, pSession, size, &pChunk, &preSrcChunk));
  }

  if (NULL == pChunk) {
    MP_ERR_JRET(memPoolNewChunk(pPool, &pChunk));
    
    pSession->allocChunkNum++;
    pSession->allocChunkMemSize += pPool->cfg.chunkSize;
    pSession->allocMemSize += size;

    MP_ADD_TO_CHUNK_LIST(pSession->srcChunkHead, pSession->srcChunkTail, pSession->srcChunkNum, pChunk);
    MP_ADD_TO_CHUNK_LIST(pSession->inUseChunkHead, pSession->inUseChunkTail, pSession->inUseChunkNum, pChunk);
  }

  pRes = pChunk->pMemStart + pChunk->offset;
  pChunk->offset += size;

  if (pChunk->offset >= (pPool->cfg.chunkSize - pPool->maxDiscardSize)) {
    if (NULL == preSrcChunk) {
      pSession->srcChunkHead = NULL;
      pSession->srcChunkTail = NULL;
    } else {
      preSrcChunk->pNext = pChunk->pNext;
    }

    pSession->srcChunkNum--;
  }
  
  
_return:

  return pRes;
}

void* memPoolAllocFromNSChunk(SMemPool* pPool, SMPSession* pSession, int64_t size) {
  SMPNSChunk* pChunk = NULL;
  MP_ERR_JRET(memPoolNewNSChunk(pPool, &pChunk, size));

  pSession->allocChunkNum++;
  pSession->allocChunkMemSize += size;
  pSession->allocMemSize += size;
  
  if (NULL == pSession->inUseNSChunkHead) {
    pSession->inUseNSChunkHead = pChunk;
    pSession->inUseNSChunkTail = pChunk;
  } else {
    pSession->inUseNSChunkTail->pNext = pChunk;
  }
  
_return:

  return pChunk ? pChunk->pMemStart : NULL;
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

  MP_ERR_JRET(memPoolGetChunk(pPool, &pSession->srcChunkHead));

  pSession->allocChunkNum = 1;
  pSession->allocChunkMemSize = pPool->cfg.chunkSize;

  MP_ADD_TO_CHUNK_LIST(pSession->srcChunkHead, pSession->srcChunkTail, pSession->srcChunkNum, pSession->srcChunkHead);
  MP_ADD_TO_CHUNK_LIST(pSession->inUseChunkHead, pSession->inUseChunkTail, pSession->inUseChunkNum, pSession->srcChunkHead);

_return:

  if (TSDB_CODE_SUCCESS != code) {
    taosMemPoolDestroySession(pSession);
    pSession = NULL;
  }

  *ppSession = pSession;

  return code;
}


void   *taosMemPoolMalloc(void* poolHandle, void* session, int64_t size, char* fileName, int32_t lineNo) {
  void *res = NULL;
  
  if (NULL == poolHandle || NULL == session || NULL == fileName || size < 0) {
    uError("%s invalid input param, handle:%p, session:%p, fileName:%p, size:%" PRId64, __FUNC__, poolHandle, session, fileName, size);
    MP_ERR_JRET(TSDB_CODE_INVALID_MEM_POOL_PARAM);
  }

  SMemPool* pPool = (SMemPool*)poolHandle;
  SMPSession* pSession = (SMPSession*)session;
  res = (size > pPool->cfg.chunkSize) ? memPoolAllocFromNSChunk(pPool, pSession, size) : memPoolAllocFromChunk(pPool, pSession, size);

  if (MP_GET_FLAG(pPool->dbgInfo.flags, MP_DBG_FLAG_LOG_MALLOC_FREE)) {
    //TODO
  }

_return:

  return res;
}

void   *taosMemPoolCalloc(void* poolHandle, void* session, int64_t num, int64_t size, char* fileName, int32_t lineNo) {
  void *res = NULL;
  
  if (NULL == poolHandle || NULL == session || NULL == fileName || num < 0 || size < 0) {
    uError("%s invalid input param, handle:%p, session:%p, fileName:%p, num:%" PRId64 ", size:%" PRId64, 
      __FUNC__, poolHandle, session, fileName, num, size);
    MP_ERR_JRET(TSDB_CODE_INVALID_MEM_POOL_PARAM);
  }

  SMemPool* pPool = (SMemPool*)poolHandle;
  SMPSession* pSession = (SMPSession*)session;
  res = (size > pPool->cfg.chunkSize) ? memPoolAllocFromNSChunk(pPool, pSession, size) : memPoolAllocFromChunk(pPool, pSession, size);

  memset(res, 0, num * size);

  if (MP_GET_FLAG(pPool->dbgInfo.flags, MP_DBG_FLAG_LOG_MALLOC_FREE)) {
    //TODO
  }

_return:

  return res;
}

void   *taosMemPoolRealloc(void* poolHandle, void* session, void *ptr, int64_t size, char* fileName, int32_t lineNo) {

}

char   *taosMemPoolStrdup(void* poolHandle, void* session, const char *ptr, char* fileName, int32_t lineNo) {

}

void    taosMemPoolFree(void* poolHandle, void* session, void *ptr, char* fileName, int32_t lineNo) {

}

int32_t taosMemPoolGetMemorySize(void* poolHandle, void* session, void *ptr, int64_t* size, char* fileName, int32_t lineNo) {

}

void   *taosMemPoolMallocAlign(void* poolHandle, void* session, uint32_t alignment, int64_t size, char* fileName, int32_t lineNo) {

}

void    taosMemPoolClose(void* poolHandle) {

}

void    taosMemPoolModDestroy(void) {

}


