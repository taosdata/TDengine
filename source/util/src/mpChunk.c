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


int32_t mpChunkNew(SMemPool* pPool, SMPChunk** ppChunk) {
  SMPChunk* pChunk = NULL;
  MP_ERR_RET(mpPopIdleNode(pPool, &pPool->chunk.chunkCache, (void**)&pChunk));
  
  pChunk->pMemStart = taosMemMalloc(pPool->cfg.chunkSize);
  if (NULL == pChunk->pMemStart) {
    uError("add new chunk, memory malloc %d failed, code: 0x%x", pPool->cfg.chunkSize, terrno);
    return terrno;
  }

  pPool->chunk.allocChunkNum++;
  pPool->chunk.allocChunkSize += pPool->cfg.chunkSize;

  *ppChunk = pChunk;

  return TSDB_CODE_SUCCESS;
}


int32_t mpChunkNewNS(SMemPool* pPool, SMPNSChunk** ppChunk, int64_t chunkSize) {
  SMPNSChunk* pChunk = NULL;
  MP_ERR_RET(mpPopIdleNode(pPool, &pPool->chunk.NSChunkCache, (void**)&pChunk));
  
  pChunk->pMemStart = taosMemMalloc(chunkSize);
  if (NULL == pChunk->pMemStart) {
    uError("add new NS chunk, memory malloc %" PRId64 " failed, code: 0x%x", chunkSize, terrno);
    return terrno;
  }

  pChunk->memBytes = chunkSize;
  MP_SET_FLAG(pChunk->flags, MP_CHUNK_FLAG_NS_CHUNK);

  pPool->chunk.allocNSChunkNum++;
  pPool->chunk.allocNSChunkSize += pPool->cfg.chunkSize;

  *ppChunk = pChunk;

  return TSDB_CODE_SUCCESS;
}


int32_t mpChunkPrepare(SMemPool* pPool, int32_t num) {
  SMPChunk* pChunk = NULL;
  for (int32_t i = 0; i < num; ++i) {
    MP_ERR_RET(mpChunkNew(pPool, &pChunk));

    pPool->chunk.readyChunkTail->list.pNext = pChunk;
    pPool->chunk.readyChunkTail = pChunk;

    atomic_add_fetch_32(&pPool->chunk.readyChunkNum, 1);
  }

  return TSDB_CODE_SUCCESS;
}


int32_t mpChunkEnsureCapacity(SMemPool* pPool, SMPChunkMgmt* pChunk) {
  if (E_EVICT_ALL == pPool->cfg.evicPolicy) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t readyMissNum = pChunk->readyChunkReserveNum - atomic_load_32(&pChunk->readyChunkNum);
  if (readyMissNum <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  MP_ERR_RET(mpChunkPrepare(pPool, readyMissNum));

  return TSDB_CODE_SUCCESS;
}

void mpChunkNotifyLowNum(SMemPool* pPool) {

}

int32_t mpChunkRetrieve(SMemPool* pPool, SMPChunk** ppChunk) {
  SMPCacheGroup* pCache = NULL;
  SMPChunk* pChunk = NULL;
  int32_t readyChunkNum = atomic_sub_fetch_32(&pPool->chunk.readyChunkNum, 1);
  if (readyChunkNum >= 0) {
    if (atomic_add_fetch_32(&pPool->chunk.readyChunkGotNum, 1) == pPool->chunk.readyChunkLowNum) {
      mpChunkNotifyLowNum(pPool);
    }

    pChunk = (SMPChunk*)atomic_load_ptr(&pPool->chunk.readyChunkHead->list.pNext);
    while (atomic_val_compare_exchange_ptr(&pPool->chunk.readyChunkHead->list.pNext, pChunk, pChunk->list.pNext) != pChunk) {
      pChunk = (SMPChunk*)atomic_load_ptr(&pPool->chunk.readyChunkHead->list.pNext);
    }

    *ppChunk = pChunk;

    return TSDB_CODE_SUCCESS;
  } else {
    atomic_add_fetch_32(&pPool->chunk.readyChunkNum, 1);
  }

  MP_RET(mpChunkNew(pPool, ppChunk));
}

int32_t mpChunkRetrieveFromSession(SMemPool* pPool, SMPSession* pSession, int64_t size, SMPChunk** ppChunk, SMPChunk** ppPreChunk) {
  SMPChunk* pChunk = pSession->chunk.srcChunkHead;
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

int32_t mpChunkAllocMem(SMemPool* pPool, SMPSession* pSession, int64_t size, uint32_t alignment, void** ppRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  SMPChunk* pChunk = NULL, *preSrcChunk = NULL;
  void* pRes = NULL;
  int64_t totalSize = size + sizeof(SMPMemHeader) + sizeof(SMPMemTailer);
  
  if (pSession->chunk.srcChunkNum > 0) {
    MP_ERR_JRET(mpChunkRetrieveFromSession(pPool, pSession, totalSize, &pChunk, &preSrcChunk));
  }

  if (NULL == pChunk) {
    MP_ERR_JRET(mpChunkNew(pPool, &pChunk));
    
    pSession->chunk.allocChunkNum++;
    pSession->chunk.allocChunkMemSize += pPool->cfg.chunkSize;
    mpUpdateAllocSize(pPool, pSession, totalSize, 0);

    MP_ADD_TO_CHUNK_LIST(pSession->chunk.srcChunkHead, pSession->chunk.srcChunkTail, pSession->chunk.srcChunkNum, pChunk);
    MP_ADD_TO_CHUNK_LIST(pSession->chunk.inUseChunkHead, pSession->chunk.inUseChunkTail, pSession->chunk.inUseChunkNum, pChunk);
  }

  SMPMemHeader* pHeader = (SMPMemHeader*)(pChunk->pMemStart + pChunk->offset);
  MP_INIT_MEM_HEADER(pHeader, size, false);

  pRes = (void*)(pHeader + 1);
  pChunk->offset += totalSize;

  if (pChunk->offset >= (pPool->cfg.chunkSize - pPool->chunk.maxDiscardSize)) {
    if (NULL == preSrcChunk) {
      pSession->chunk.srcChunkHead = NULL;
      pSession->chunk.srcChunkTail = NULL;
    } else {
      preSrcChunk->list.pNext = pChunk->list.pNext;
    }

    pSession->chunk.srcChunkNum--;
  }
  
  
_return:

  *ppRes = pRes;

  return code;
}

int32_t mpChunkNSAllocMem(SMemPool* pPool, SMPSession* pSession, int64_t size, uint32_t alignment, void** ppRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  SMPNSChunk* pChunk = NULL;
  void* pRes = NULL;
  int64_t totalSize = size + sizeof(SMPMemHeader) + sizeof(SMPMemTailer) + alignment;
  
  MP_ERR_JRET(mpChunkNewNS(pPool, &pChunk, totalSize));
  SMPMemHeader* pHeader = (SMPMemHeader*)pChunk->pMemStart;
  MP_INIT_MEM_HEADER(pHeader, size, false);

  pRes = (void*)(pHeader + 1);
  
  pSession->chunk.allocChunkNum++;
  pSession->chunk.allocChunkMemSize += totalSize;
  mpUpdateAllocSize(pPool, pSession, totalSize, 0);
  
  if (NULL == pSession->chunk.inUseNSChunkHead) {
    pSession->chunk.inUseNSChunkHead = pChunk;
    pSession->chunk.inUseNSChunkTail = pChunk;
  } else {
    pSession->chunk.inUseNSChunkTail->list.pNext = pChunk;
  }
  
_return:

  *ppRes = pRes;
  
  return code;
}


int32_t mpChunkInit(SMemPool* pPool, char* poolName, SMemPoolCfg* cfg) {
  SMPChunkMgmt* pChunk = &pPool->chunk;
  pChunk->threadChunkReserveNum = 1;

  pChunk->chunkCache.nodeSize = sizeof(SMPChunk);
  pChunk->NSChunkCache.groupNum = MP_NSCHUNK_CACHE_ALLOC_BATCH_SIZE;
  pChunk->NSChunkCache.nodeSize = sizeof(SMPNSChunk);

  MP_ERR_RET(mpAddCacheGroup(pPool, &pChunk->chunkCache, NULL));
  MP_ERR_RET(mpAddCacheGroup(pPool, &pChunk->NSChunkCache, NULL));

  MP_ERR_RET(mpPopIdleNode(pPool, &pChunk->chunkCache, (void**)&pChunk->readyChunkHead));
  pChunk->readyChunkTail = pChunk->readyChunkHead;

  MP_ERR_RET(mpChunkEnsureCapacity(pPool, pChunk));

  return TSDB_CODE_SUCCESS;
}


int64_t mpChunkGetMemSize(SMemPool* pPool, SMPSession* pSession, void *ptr) {
  SMPMemHeader* pHeader = (SMPMemHeader*)ptr - 1;
  return pHeader->size;
}

int32_t mpChunkAlloc(SMemPool* pPool, SMPSession* pSession, int64_t* size, uint32_t alignment, void** ppRes) {
  MP_RET((*size > pPool->cfg.chunkSize) ? mpChunkNSAllocMem(pPool, pSession, *size, alignment, ppRes) : mpChunkAllocMem(pPool, pSession, *size, alignment, ppRes));
}


void mpChunkFree(SMemPool* pPool, SMPSession* pSession, void *ptr, int64_t* origSize) {
  int64_t oSize = mpChunkGetMemSize(pPool, pSession, ptr);
  if (origSize) {
    *origSize = oSize;
  }
  
  // TODO
  
  atomic_sub_fetch_64(&pSession->allocMemSize, oSize);
  atomic_sub_fetch_64(&pPool->allocMemSize, oSize);
}


int32_t mpChunkRealloc(SMemPool* pPool, SMPSession* pSession, void **pPtr, int64_t* size, int64_t* origSize) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (*origSize >= *size) {
    SMPMemHeader* pHeader = (SMPMemHeader*)((char*)*pPtr - sizeof(SMPMemHeader));
    pHeader->size = *size;
    return TSDB_CODE_SUCCESS;
  }

  void* res = NULL;
  MP_ERR_JRET(mpMalloc(pPool, pSession, size, 0, &res));
  SMPMemHeader* pOrigHeader = (SMPMemHeader*)((char*)*pPtr - sizeof(SMPMemHeader));
  SMPMemHeader* pNewHeader = (SMPMemHeader*)((char*)res - sizeof(SMPMemHeader));
  
  TAOS_MEMCPY(res, *pPtr, *origSize);
  mpChunkFree(pPool, pSession, *pPtr, NULL);
  *pPtr = res;
  
  return TSDB_CODE_SUCCESS;

_return:

  mpChunkFree(pPool, pSession, *pPtr, NULL);
  
  return code;
}

int32_t mpChunkInitSession(SMemPool* pPool, SMPSession* pSession) {
  int32_t code = TSDB_CODE_SUCCESS;
  SMPChunk* pChunk = NULL;

  MP_ERR_RET(mpChunkRetrieve(pPool, &pChunk));

  pSession->chunk.allocChunkNum = 1;
  pSession->chunk.allocChunkMemSize = pPool->cfg.chunkSize;

  MP_ADD_TO_CHUNK_LIST(pSession->chunk.srcChunkHead, pSession->chunk.srcChunkTail, pSession->chunk.srcChunkNum, pChunk);
  MP_ADD_TO_CHUNK_LIST(pSession->chunk.inUseChunkHead, pSession->chunk.inUseChunkTail, pSession->chunk.inUseChunkNum, pChunk);

  return code;
}

int32_t mpChunkUpdateCfg(SMemPool* pPool) {
  pPool->chunk.maxChunkNum = pPool->cfg.freeSize / pPool->cfg.chunkSize;
  if (pPool->chunk.maxChunkNum <= 0) {
    uError("invalid memory pool max chunk num, freeSize:%" PRId64 ", chunkSize:%d", pPool->cfg.freeSize, pPool->cfg.chunkSize);
    return TSDB_CODE_INVALID_MEM_POOL_PARAM;
  }

  pPool->chunk.readyChunkReserveNum = TMIN(pPool->cfg.threadNum * pPool->chunk.threadChunkReserveNum, pPool->chunk.maxChunkNum);

  pPool->chunk.chunkCache.groupNum = TMAX(pPool->chunk.maxChunkNum / 10, MP_CHUNK_CACHE_ALLOC_BATCH_SIZE);

  return TSDB_CODE_SUCCESS;
}


