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
#include "osMemPool.h"
#include "tmempoolInt.h"
#include "tlog.h"
#include "tutil.h"

static SArray* gMPoolList = NULL;
static TdThreadOnce  gMPoolInit = PTHREAD_ONCE_INIT;
static TdThreadMutex gMPoolMutex;
threadlocal void* threadPoolHandle = NULL;
threadlocal void* threadPoolSession = NULL;


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

void memPoolFreeChunkGroup(SMPCacheGroup* pGrp) {
  //TODO
}

int32_t memPoolAddCacheGroup(SMemPool* pPool, SMPCacheGroupInfo* pInfo, SMPCacheGroup* pTail) {
  SMPCacheGroup* pGrp = NULL;
  if (NULL == pInfo->pGrpHead) {
    pInfo->pGrpHead = taosMemCalloc(1, sizeof(*pInfo->pGrpHead));
    if (NULL == pInfo->pGrpHead) {
      uError("malloc chunkCache failed");
      MP_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }

    pGrp = pInfo->pGrpHead;
  } else {
    pGrp = (SMPCacheGroup*)taosMemCalloc(1, sizeof(SMPCacheGroup));
  }

  pGrp->nodesNum = pInfo->groupNum;
  pGrp->pNodes = taosMemoryCalloc(pGrp->nodesNum, pInfo->nodeSize);
  if (NULL == pGrp->pNodes) {
    uError("calloc %d %d nodes in cache group failed", pGrp->nodesNum, pInfo->nodeSize);
    MP_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  if (atomic_val_compare_exchange_ptr(&pInfo->pGrpTail, pTail, pGrp) != pTail) {
    memPoolFreeChunkGroup(pGrp);
    return TSDB_CODE_SUCCESS;
  }

  atomic_add_fetch_64(&pInfo->allocNum, pGrp->nodesNum);

  return TSDB_CODE_SUCCESS;
}

int32_t memPoolGetIdleNode(SMemPool* pPool, SMPCacheGroupInfo* pInfo, void** ppRes) {
  SMPCacheGroup* pGrp = NULL;
  SMPListNode* pList = NULL;
  
  while (true) {
    pList = (SMPListNode*)atomic_load_ptr(&pInfo->pIdleList);
    if (NULL == pList) {
      break;
    }

    if (atomic_val_compare_exchange_ptr(&pInfo->pIdleList, pList, pList->pNext) != pList) {
      continue;
    }

    pList->pNext = NULL;
    goto _return;
  }

  while (true) {
    pGrp = atomic_load_ptr(&pInfo->pGrpTail);
    int32_t offset = atomic_fetch_add_32(&pGrp->idleOffset, 1);
    if (offset < pGrp->nodesNum) {
      pList = (SMPListNode*)((char*)pGrp->pNodes + offset * pInfo->nodeSize);
      break;
    } else {
      atomic_sub_fetch_32(&pGrp->idleOffset, 1);
    }
    
    MP_ERR_RET(memPoolAddCacheGroup(pPool, pInfo, pGrp));
  }

_return:

  *ppRes = pList;

  return TSDB_CODE_SUCCESS;
}

int32_t memPoolNewChunk(SMemPool* pPool, SMPChunk** ppChunk) {
  SMPChunk* pChunk = NULL;
  MP_ERR_RET(memPoolGetIdleNode(pPool, &pPool->chunkCache, (void**)&pChunk));
  
  pChunk->pMemStart = taosMemMalloc(pPool->cfg.chunkSize);
  if (NULL == pChunk->pMemStart) {
    uError("add new chunk, memory malloc %d failed", pPool->cfg.chunkSize);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pPool->allocChunkNum++;
  pPool->allocChunkSize += pPool->cfg.chunkSize;

  return TSDB_CODE_SUCCESS;
}


int32_t memPoolNewNSChunk(SMemPool* pPool, SMPNSChunk** ppChunk, int64_t chunkSize) {
  SMPNSChunk* pChunk = NULL;
  MP_ERR_RET(memPoolGetIdleNode(pPool, &pPool->NSChunkCache, (void**)&pChunk));
  
  pChunk->pMemStart = taosMemMalloc(chunkSize);
  if (NULL == pChunk->pMemStart) {
    uError("add new chunk, memory malloc %" PRId64 " failed", chunkSize);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pChunk->memBytes = chunkSize;
  MP_SET_FLAG(pChunk->flags, MP_CHUNK_FLAG_NS_CHUNK);

  pPool->allocNSChunkNum++;
  pPool->allocNSChunkSize += pPool->cfg.chunkSize;

  return TSDB_CODE_SUCCESS;
}


int32_t memPoolPrepareChunks(SMemPool* pPool, int32_t num) {
  SMPChunk* pChunk = NULL;
  for (int32_t i = 0; i < num; ++i) {
    MP_ERR_RET(memPoolNewChunk(pPool, &pChunk));

    if (NULL == pPool->readyChunkTail) {
      pPool->readyChunkHead = pChunk;
      pPool->readyChunkTail = pChunk;
    } else {
      pPool->readyChunkTail->list.pNext = pChunk;
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

  pPool->chunkCache.groupNum = TMAX(pPool->maxChunkNum / 10, MP_CHUNK_CACHE_ALLOC_BATCH_SIZE);
  pPool->chunkCache.nodeSize = sizeof(SMPChunk);
  pPool->NSChunkCache.groupNum = MP_NSCHUNK_CACHE_ALLOC_BATCH_SIZE;
  pPool->NSChunkCache.nodeSize = sizeof(SMPNSChunk);
  pPool->sessionCache.groupNum = MP_SESSION_CACHE_ALLOC_BATCH_SIZE;
  pPool->sessionCache.nodeSize = sizeof(SMPSession);

  MP_ERR_RET(memPoolAddCacheGroup(pPool, &pPool->chunkCache, NULL));
  MP_ERR_RET(memPoolAddCacheGroup(pPool, &pPool->NSChunkCache, NULL));
  MP_ERR_RET(memPoolAddCacheGroup(pPool, &pPool->sessionCache, NULL));

  MP_ERR_RET(memPoolGetIdleNode(pPool, &pPool->chunkCache, (void**)&pPool->readyChunkHead));
  pPool->readyChunkTail = pPool->readyChunkHead;

  MP_ERR_RET(memPoolEnsureChunks(pPool));

  return TSDB_CODE_SUCCESS;
}

void memPoolNotifyLowChunkNum(SMemPool* pPool) {

}

int32_t memPoolGetChunk(SMemPool* pPool, SMPChunk** ppChunk) {
  SMPCacheGroup* pCache = NULL;
  SMPChunk* pChunk = NULL;
  int32_t readyChunkNum = atomic_sub_fetch_32(&pPool->readyChunkNum, 1);
  if (readyChunkNum >= 0) {
    if (atomic_add_fetch_32(&pPool->readyChunkGotNum, 1) == pPool->readyChunkLowNum) {
      memPoolNotifyLowChunkNum(pPool);
    }

    pChunk = (SMPChunk*)atomic_load_ptr(&pPool->readyChunkHead->list.pNext);
    while (atomic_val_compare_exchange_ptr(&pPool->readyChunkHead->list.pNext, pChunk, pChunk->list.pNext) != pChunk) {
      pChunk = (SMPChunk*)atomic_load_ptr(&pPool->readyChunkHead->list.pNext);
    }

    *ppChunk = pChunk;

    return TSDB_CODE_SUCCESS;
  }

  MP_RET(memPoolNewChunk(pPool, ppChunk));
}

int32_t memPoolGetChunkFromSession(SMemPool* pPool, SMPSession* pSession, int64_t size, SMPChunk** ppChunk, SMPChunk** ppPreChunk) {
  SMPChunk* pChunk = pSession->srcChunkHead;
  while (NULL != pChunk) {
    if ((pChunk->offset + size) <= pPool->cfg.chunkSize) {
      *ppChunk = pChunk;
      break;
    }

    *ppPreChunk = pChunk;
    pChunk = (SMPChunk*)pChunk->list.pNext;
  }

  if (NULL == *ppChunk) {
    *ppPreChunk = NULL;
  }

  return TSDB_CODE_SUCCESS;
}

void* memPoolAllocFromChunk(SMemPool* pPool, SMPSession* pSession, int64_t size) {
  int32_t code = TSDB_CODE_SUCCESS;
  SMPChunk* pChunk = NULL, *preSrcChunk = NULL;
  void* pRes = NULL;
  int64_t totalSize = size + sizeof(SMPMemHeader) + sizeof(SMPMemTailer);
  
  if (pSession->srcChunkNum > 0) {
    MP_ERR_JRET(memPoolGetChunkFromSession(pPool, pSession, totalSize, &pChunk, &preSrcChunk));
  }

  if (NULL == pChunk) {
    MP_ERR_JRET(memPoolNewChunk(pPool, &pChunk));
    
    pSession->allocChunkNum++;
    pSession->allocChunkMemSize += pPool->cfg.chunkSize;
    pSession->allocMemSize += totalSize;

    MP_ADD_TO_CHUNK_LIST(pSession->srcChunkHead, pSession->srcChunkTail, pSession->srcChunkNum, pChunk);
    MP_ADD_TO_CHUNK_LIST(pSession->inUseChunkHead, pSession->inUseChunkTail, pSession->inUseChunkNum, pChunk);
  }

  SMPMemHeader* pHeader = (SMPMemHeader*)(pChunk->pMemStart + pChunk->offset);
  MP_INIT_MEM_HEADER(pHeader, size, false);

  pRes = (void*)(pHeader + 1);
  pChunk->offset += totalSize;

  if (pChunk->offset >= (pPool->cfg.chunkSize - pPool->maxDiscardSize)) {
    if (NULL == preSrcChunk) {
      pSession->srcChunkHead = NULL;
      pSession->srcChunkTail = NULL;
    } else {
      preSrcChunk->list.pNext = pChunk->list.pNext;
    }

    pSession->srcChunkNum--;
  }
  
  
_return:

  return pRes;
}

void* memPoolAllocFromNSChunk(SMemPool* pPool, SMPSession* pSession, int64_t size) {
  int32_t code = TSDB_CODE_SUCCESS;
  SMPNSChunk* pChunk = NULL;
  void* pRes = NULL;
  int64_t totalSize = size + sizeof(SMPMemHeader) + sizeof(SMPMemTailer);
  
  MP_ERR_JRET(memPoolNewNSChunk(pPool, &pChunk, totalSize));
  SMPMemHeader* pHeader = (SMPMemHeader*)pChunk->pMemStart;
  MP_INIT_MEM_HEADER(pHeader, size, false);

  pRes = (void*)(pHeader + 1);
  
  pSession->allocChunkNum++;
  pSession->allocChunkMemSize += totalSize;
  pSession->allocMemSize += totalSize;
  
  if (NULL == pSession->inUseNSChunkHead) {
    pSession->inUseNSChunkHead = pChunk;
    pSession->inUseNSChunkTail = pChunk;
  } else {
    pSession->inUseNSChunkTail->list.pNext = pChunk;
  }
  
_return:

  return pRes;
}

void *memPoolMallocImpl(SMemPool* pPool, SMPSession* pSession, int64_t size, char* fileName, int32_t lineNo) {
  int32_t code = TSDB_CODE_SUCCESS;
  void *res = NULL;
  
  res = (size > pPool->cfg.chunkSize) ? memPoolAllocFromNSChunk(pPool, pSession, size) : memPoolAllocFromChunk(pPool, pSession, size);

  if (MP_GET_FLAG(pPool->dbgInfo.flags, MP_DBG_FLAG_LOG_MALLOC_FREE)) {
    //TODO
  }

_return:

  return res;
}

void *memPoolCallocImpl(SMemPool* pPool, SMPSession* pSession, int64_t num, int64_t size, char* fileName, int32_t lineNo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int64_t totalSize = num * size;
  void *res = memPoolMallocImpl(pPool, pSession, totalSize, fileName, lineNo);

  if (NULL != res) {
    memset(res, 0, totalSize);
  }

  if (MP_GET_FLAG(pPool->dbgInfo.flags, MP_DBG_FLAG_LOG_MALLOC_FREE)) {
    //TODO
  }

_return:

  return res;
}


void memPoolFreeImpl(SMemPool* pPool, SMPSession* pSession, void *ptr, char* fileName, int32_t lineNo) {
  //TODO
  
  if (MP_GET_FLAG(pPool->dbgInfo.flags, MP_DBG_FLAG_LOG_MALLOC_FREE)) {
    //TODO
  }
}

int64_t memPoolGetMemorySizeImpl(SMemPool* pPool, SMPSession* pSession, void *ptr, char* fileName, int32_t lineNo) {
  SMPMemHeader* pHeader = (SMPMemHeader*)ptr - 1;

  return pHeader->size;
}



void taosMemPoolModInit(void) {
  taosThreadMutexInit(&gMPoolMutex, NULL);

  gMPoolList = taosArrayInit(10, POINTER_BYTES);
}

int32_t taosMemPoolOpen(char* poolName, SMemPoolCfg* cfg, void** poolHandle) {
  int32_t code = TSDB_CODE_SUCCESS;
  SMemPool* pPool = NULL;
  
  taosThreadOnce(&gMPoolInit, taosMemPoolModInit);
  if (NULL == gMPoolList) {
    uError("init memory pool failed");
    MP_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }

  pPool = (SMemPool*)taosMemoryCalloc(1, sizeof(SMemPool));
  if (NULL == pPool) {
    uError("calloc memory pool failed");
    MP_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }

  MP_ERR_JRET(memPoolInit(pPool, poolName, cfg));

  taosThreadMutexLock(&gMPoolMutex);
  
  taosArrayPush(gMPoolList, &pPool);
  pPool->slotId = taosArrayGetSize(gMPoolList) - 1;
  
  taosThreadMutexUnlock(&gMPoolMutex);

_return:

  if (TSDB_CODE_SUCCESS != code) {
    taosMemPoolClose(pPool);
    pPool = NULL;
  }

  *poolHandle = pPool;

  return code;
}

void taosMemPoolDestroySession(void* session) {
  SMPSession* pSession = (SMPSession*)session;
  //TODO;
}

int32_t taosMemPoolInitSession(void* poolHandle, void** ppSession) {
  int32_t code = TSDB_CODE_SUCCESS;
  SMemPool* pPool = (SMemPool*)poolHandle;
  SMPSession* pSession = NULL;

  MP_ERR_JRET(memPoolGetIdleNode(pPool, &pPool->sessionCache, (void**)&pSession));

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
  int32_t code = TSDB_CODE_SUCCESS;
  
  if (NULL == poolHandle || NULL == session || NULL == fileName || size < 0) {
    uError("%s invalid input param, handle:%p, session:%p, fileName:%p, size:%" PRId64, __FUNCTION__, poolHandle, session, fileName, size);
    MP_ERR_JRET(TSDB_CODE_INVALID_MEM_POOL_PARAM);
  }

  SMemPool* pPool = (SMemPool*)poolHandle;
  SMPSession* pSession = (SMPSession*)session;
  return memPoolMallocImpl(pPool, pSession, size, fileName, lineNo);

_return:

  return NULL;
}

void   *taosMemPoolCalloc(void* poolHandle, void* session, int64_t num, int64_t size, char* fileName, int32_t lineNo) {
  int32_t code = TSDB_CODE_SUCCESS;
  void *res = NULL;
  
  if (NULL == poolHandle || NULL == session || NULL == fileName || num < 0 || size < 0) {
    uError("%s invalid input param, handle:%p, session:%p, fileName:%p, num:%" PRId64 ", size:%" PRId64, 
      __FUNCTION__, poolHandle, session, fileName, num, size);
    MP_ERR_JRET(TSDB_CODE_INVALID_MEM_POOL_PARAM);
  }

  SMemPool* pPool = (SMemPool*)poolHandle;
  SMPSession* pSession = (SMPSession*)session;
  res = memPoolMallocImpl(pPool, pSession, num * size, fileName, lineNo);

  if (NULL != res) {
    memset(res, 0, num * size);
  }

  if (MP_GET_FLAG(pPool->dbgInfo.flags, MP_DBG_FLAG_LOG_MALLOC_FREE)) {
    //TODO
  }

_return:

  return res;
}

void   *taosMemPoolRealloc(void* poolHandle, void* session, void *ptr, int64_t size, char* fileName, int32_t lineNo) {
  int32_t code = TSDB_CODE_SUCCESS;
  void *res = NULL;
  
  if (NULL == poolHandle || NULL == session || NULL == fileName || size < 0) {
    uError("%s invalid input param, handle:%p, session:%p, fileName:%p, size:%" PRId64, 
      __FUNCTION__, poolHandle, session, fileName, size);
    MP_ERR_JRET(TSDB_CODE_INVALID_MEM_POOL_PARAM);
  }

  SMemPool* pPool = (SMemPool*)poolHandle;
  SMPSession* pSession = (SMPSession*)session;
  if (NULL == ptr) {
    res = (size > pPool->cfg.chunkSize) ? memPoolAllocFromNSChunk(pPool, pSession, size) : memPoolAllocFromChunk(pPool, pSession, size);
  } else if (0 == size) {
    memPoolFreeImpl(pPool, pSession, ptr, fileName, lineNo);
  } else {
    int64_t origSize = memPoolGetMemorySizeImpl(pPool, pSession, ptr, fileName, lineNo);
    if (origSize >= size) {
      SMPMemHeader* pHeader = (SMPMemHeader*)((char*)ptr - sizeof(SMPMemHeader));
      pHeader->size = size;
    } else {
      res = (size > pPool->cfg.chunkSize) ? memPoolAllocFromNSChunk(pPool, pSession, size) : memPoolAllocFromChunk(pPool, pSession, size);
      SMPMemHeader* pOrigHeader = (SMPMemHeader*)((char*)ptr - sizeof(SMPMemHeader));
      SMPMemHeader* pNewHeader = (SMPMemHeader*)((char*)res - sizeof(SMPMemHeader));

      memcpy(res, ptr, origSize);
      memset((char*)res + origSize, 0, size - origSize);
    }
  }

  if (MP_GET_FLAG(pPool->dbgInfo.flags, MP_DBG_FLAG_LOG_MALLOC_FREE)) {
    //TODO
  }

_return:

  return res;
}

char   *taosMemPoolStrdup(void* poolHandle, void* session, const char *ptr, char* fileName, int32_t lineNo) {
  int32_t code = TSDB_CODE_SUCCESS;
  void *res = NULL;
  
  if (NULL == poolHandle || NULL == session || NULL == fileName || NULL == ptr) {
    uError("%s invalid input param, handle:%p, session:%p, fileName:%p, ptr:%p", 
      __FUNCTION__, poolHandle, session, fileName, ptr);
    MP_ERR_JRET(TSDB_CODE_INVALID_MEM_POOL_PARAM);
  }

  SMemPool* pPool = (SMemPool*)poolHandle;
  SMPSession* pSession = (SMPSession*)session;
  int64_t size = strlen(ptr) + 1;
  res = (size > pPool->cfg.chunkSize) ? memPoolAllocFromNSChunk(pPool, pSession, size) : memPoolAllocFromChunk(pPool, pSession, size);
  if (NULL != res) {
    strcpy(res, ptr);
  }

  if (MP_GET_FLAG(pPool->dbgInfo.flags, MP_DBG_FLAG_LOG_MALLOC_FREE)) {
    //TODO
  }

_return:

  return res;
}

void taosMemPoolFree(void* poolHandle, void* session, void *ptr, char* fileName, int32_t lineNo) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (NULL == poolHandle || NULL == session || NULL == fileName || NULL == ptr) {
    uError("%s invalid input param, handle:%p, session:%p, fileName:%p, ptr:%p", 
      __FUNCTION__, poolHandle, session, fileName, ptr);
    MP_ERR_JRET(TSDB_CODE_INVALID_MEM_POOL_PARAM);
  }

  SMemPool* pPool = (SMemPool*)poolHandle;
  SMPSession* pSession = (SMPSession*)session;
  memPoolFreeImpl(pPool, pSession, ptr, fileName, lineNo);

_return:

  return;
}

int64_t taosMemPoolGetMemorySize(void* poolHandle, void* session, void *ptr, char* fileName, int32_t lineNo) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (NULL == poolHandle || NULL == session || NULL == fileName) {
    uError("%s invalid input param, handle:%p, session:%p, fileName:%p", 
      __FUNCTION__, poolHandle, session, fileName);
    MP_ERR_JRET(TSDB_CODE_INVALID_MEM_POOL_PARAM);
  }

  if (NULL == ptr) {
    return 0;
  }

  SMemPool* pPool = (SMemPool*)poolHandle;
  SMPSession* pSession = (SMPSession*)session;
  return memPoolGetMemorySizeImpl(pPool, pSession, ptr, fileName, lineNo);

_return:

  return -1;
}

void* taosMemPoolMallocAlign(void* poolHandle, void* session, uint32_t alignment, int64_t size, char* fileName, int32_t lineNo) {
  int32_t code = TSDB_CODE_SUCCESS;
  
  if (NULL == poolHandle || NULL == session || NULL == fileName || size < 0 || alignment < POINTER_BYTES || alignment % POINTER_BYTES) {
    uError("%s invalid input param, handle:%p, session:%p, fileName:%p, alignment:%u, size:%" PRId64, 
      __FUNCTION__, poolHandle, session, fileName, alignment, size);
    MP_ERR_JRET(TSDB_CODE_INVALID_MEM_POOL_PARAM);
  }

  SMemPool* pPool = (SMemPool*)poolHandle;
  SMPSession* pSession = (SMPSession*)session;
  return memPoolMallocImpl(pPool, pSession, size, fileName, lineNo);

_return:

  return NULL;
}

void taosMemPoolClose(void* poolHandle) {

}

void    taosMemPoolModDestroy(void) {

}

void taosAutoMemoryFree(void *ptr) {
  if (NULL != threadPoolHandle) {
    taosMemPoolFree(threadPoolHandle, threadPoolSession, ptr, __FILE__, __LINE__);
  } else {
    taosMemFree(ptr);
  }
}

void    taosMemPoolTrim(void* poolHandle, void* session, int32_t size, char* fileName, int32_t lineNo) {

}



