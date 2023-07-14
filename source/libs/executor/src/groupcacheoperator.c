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

#include "executorInt.h"
#include "filter.h"
#include "function.h"
#include "operator.h"
#include "os.h"
#include "querynodes.h"
#include "querytask.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "thash.h"
#include "tmsg.h"
#include "ttypes.h"
#include "groupcache.h"

static int32_t initGroupColsInfo(SGroupColsInfo* pCols, bool grpColsMayBeNull, SNodeList* pList) {
  pCols->colNum = LIST_LENGTH(pList);
  pCols->withNull = grpColsMayBeNull;  
  pCols->pColsInfo = taosMemoryMalloc(pCols->colNum * sizeof(SGroupColInfo));
  if (NULL == pCols->pColsInfo) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t i = 0;
  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    SColumnNode* pColNode = (SColumnNode*)pNode;
    pCols->pColsInfo[i].slot = pColNode->slotId;
    pCols->pColsInfo[i].vardata = IS_VAR_DATA_TYPE(pColNode->node.resType.type);
    pCols->pColsInfo[i].bytes = pColNode->node.resType.bytes;
    pCols->bufSize += pColNode->node.resType.bytes;
    ++i;
  }  

  if (pCols->withNull) {
    pCols->bitMapSize = pCols->colNum / sizeof(int8_t) + ((pCols->colNum % sizeof(int8_t)) ? 1 : 0);
    pCols->bufSize += pCols->bitMapSize;
  }

  if (pCols->colNum > 1) {
    pCols->pBuf = taosMemoryMalloc(pCols->bufSize);
    if (NULL == pCols->pBuf) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static void logGroupCacheExecInfo(SGroupCacheOperatorInfo* pGrpCacheOperator) {
  char* buf = taosMemoryMalloc(pGrpCacheOperator->execInfo.downstreamNum * 32 + 100);
  if (NULL == buf) {
    return;
  }
  int32_t offset = sprintf(buf, "groupCache exec info, downstreamBlkNum:");
  for (int32_t i = 0; i < pGrpCacheOperator->execInfo.downstreamNum; ++i) {
    offset += sprintf(buf + offset, " %" PRId64 , pGrpCacheOperator->execInfo.pDownstreamBlkNum[i]);
  }
  qDebug("%s", buf);
}

static void destroyGroupCacheOperator(void* param) {
  SGroupCacheOperatorInfo* pGrpCacheOperator = (SGroupCacheOperatorInfo*)param;

  logGroupCacheExecInfo(pGrpCacheOperator);
  
  taosMemoryFree(pGrpCacheOperator->groupColsInfo.pColsInfo);
  taosMemoryFree(pGrpCacheOperator->groupColsInfo.pBuf);
  taosHashCleanup(pGrpCacheOperator->pSessionHash);
  taosHashCleanup(pGrpCacheOperator->pGrpHash);
  
  taosMemoryFreeClear(param);
}

static int32_t addBlkToDirtyBufList(SGroupCacheOperatorInfo* pGCache, SGcBlkCacheInfo* pCache, SGcBlkBufInfo* pBufInfo) {
  if (0 != taosHashPut(pCache->pDirtyBlk, &pBufInfo->blkId, sizeof(pBufInfo->blkId), pBufInfo, sizeof(*pBufInfo))) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pBufInfo = taosHashGet(pCache->pDirtyBlk, &pBufInfo->blkId, sizeof(pBufInfo->blkId));

  taosWLockLatch(&pCache->dirtyLock);
  if (NULL == pCache->pDirtyHead) {
    pCache->pDirtyHead = pBufInfo;
  } else {
    pBufInfo->prev = pCache->pDirtyTail;
    pCache->pDirtyTail->next = pBufInfo;
  }
  pCache->pDirtyTail = pBufInfo;
  taosWUnLockLatch(&pCache->dirtyLock);
  
  int64_t blkCacheSize = atomic_add_fetch_64(&pCache->blkCacheSize, pBufInfo->bufSize);
  qDebug("group cache block cache num:%d size:%" PRId64 , taosHashGetSize(pCache->pDirtyBlk), blkCacheSize);
  
  if (pGCache->maxCacheSize > 0 && blkCacheSize > pGCache->maxCacheSize) {
    //TODO
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t addBlkToBufCache(struct SOperatorInfo* pOperator, SSDataBlock* pBlock, SGcDownstreamCtx* pCtx, SGroupCacheData* pGroup, SGcBlkBufInfo* pBufInfo) {
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  int64_t bufSize = blockDataGetSize(pBlock) + sizeof(int32_t) + taosArrayGetSize(pBlock->pDataBlock) * sizeof(int32_t);
  pBufInfo->pBuf = taosMemoryMalloc(bufSize);
  if (NULL == pBufInfo->pBuf) {
    qError("group cache add block to cache failed, size:%" PRId64, bufSize);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  blockDataToBuf(pBufInfo->pBuf, pBlock);

  pBufInfo->prev = NULL;
  pBufInfo->next = NULL;
  pBufInfo->blkId = atomic_add_fetch_64(&pGCache->currentBlkId, 1);
  pBufInfo->fileId = pGroup->fileId;
  pBufInfo->offset = pGroup->pVgCtx->fileSize;
  pBufInfo->bufSize = bufSize;

  pGroup->pVgCtx->fileSize += bufSize;

  int32_t code = addBlkToDirtyBufList(pGCache, &pGCache->blkCache, pBufInfo);

  return code;
}


static int32_t buildGroupCacheBaseBlock(SSDataBlock** ppDst, SSDataBlock* pSrc) {
  *ppDst = taosMemoryMalloc(sizeof(*pSrc));
  if (NULL == *ppDst) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  (*ppDst)->pBlockAgg = NULL;
  (*ppDst)->pDataBlock = taosArrayDup(pSrc->pDataBlock, NULL);
  if (NULL == (*ppDst)->pDataBlock) {
    taosMemoryFree(*ppDst);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  memcpy(&(*ppDst)->info, &pSrc->info, sizeof(pSrc->info));
  return TSDB_CODE_SUCCESS;
}

static int32_t acquireBaseBlockFromList(SGcDownstreamCtx* pCtx, SSDataBlock** ppRes) {
  taosWLockLatch(&pCtx->blkLock);
  if (taosArrayGetSize(pCtx->pFreeBlock) <= 0) {
    taosWUnLockLatch(&pCtx->blkLock);
    return buildGroupCacheBaseBlock(ppRes, pCtx->pBaseBlock);
  }
  *ppRes = taosArrayPop(pCtx->pFreeBlock);
  taosWUnLockLatch(&pCtx->blkLock);

  return TSDB_CODE_SUCCESS;  
}

static int32_t releaseBaseBlockFromList(SGcDownstreamCtx* pCtx, SSDataBlock* pBlock) {
  taosWLockLatch(&pCtx->blkLock);
  taosArrayPush(pCtx->pFreeBlock, &pBlock);
  taosWUnLockLatch(&pCtx->blkLock);

  return TSDB_CODE_SUCCESS;  
}


static int32_t buildGroupCacheResultBlock(SGroupCacheOperatorInfo* pGCache, int32_t downstreamIdx, SGcBlkBufInfo* pBufInfo, SSDataBlock** ppRes) {
  int32_t code = acquireBaseBlockFromList(&pGCache->pDownstreams[downstreamIdx], ppRes);
  if (code) {
    return code;
  }
  //TODO OPTIMIZE PERF
  return blockDataFromBuf(*ppRes, pBufInfo->pBuf);
}

static int32_t retrieveBlkFromBufCache(SGroupCacheOperatorInfo* pGCache, SGroupCacheData* pGrp, int64_t blkId, int64_t* nextOffset, SSDataBlock** ppRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  SGcBlkCacheInfo* pCache = &pGCache->blkCache;
  SGcReadBlkInfo* pReadBlk = taosHashAcquire(pCache->pReadBlk, &blkId, sizeof(blkId));
  if (pReadBlk) {
    *ppRes = pReadBlk->pBlock;
    *nextOffset = pReadBlk->nextOffset;
    return TSDB_CODE_SUCCESS;
  }

  taosRLockLatch(&pCache->dirtyLock);
  SGcBlkBufInfo* pBufInfo = taosHashAcquire(pCache->pDirtyBlk, &blkId, sizeof(blkId));
  if (pBufInfo) {
    code = buildGroupCacheResultBlock(pGCache, pGrp->downstreamIdx, pBufInfo, ppRes);
    taosRUnLockLatch(&pCache->dirtyLock);
    if (code) {
      return code;
    }

    *nextOffset = pBufInfo->offset + pBufInfo->bufSize;
    SGcReadBlkInfo readBlk = {.pBlock = *ppRes, .nextOffset = *nextOffset};
    taosHashPut(pCache->pReadBlk, &blkId, sizeof(blkId), &readBlk, sizeof(readBlk));
    return TSDB_CODE_SUCCESS;
  }
  taosRUnLockLatch(&pCache->dirtyLock);

  //TODO READ FROM FILE
  code = TSDB_CODE_INVALID_PARA;
  return code;
}

static int32_t addNewGroupToVgHash(SSHashObj* pHash, SGcNewGroupInfo* pNew) {
  SGcVgroupCtx* pVgCtx = pNew->pGroup->pVgCtx;
  if (NULL == pVgCtx) {
    SArray* pList = taosArrayInit(10, sizeof(*pNew));
    if (NULL == pList) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    taosArrayPush(pList, pNew);
    SGcVgroupCtx vgCtx = {.pTbList = pList, .lastUid = 0, .fileSize = 0, .fileId = 0};
    tSimpleHashPut(pHash, &pNew->vgId, sizeof(pNew->vgId), &vgCtx, sizeof(vgCtx));
    pNew->pGroup->pVgCtx = tSimpleHashGet(pHash, &pNew->vgId, sizeof(pNew->vgId));
    return TSDB_CODE_SUCCESS;
  }

  taosArrayPush(pVgCtx->pTbList, pNew);
  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE int32_t appendNewGroupToDownstream(struct SOperatorInfo* pOperator, int32_t downstreamIdx, SOperatorParam** ppParam) {
  int32_t code = TSDB_CODE_SUCCESS;
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  SGcDownstreamCtx* pCtx = &pGCache->pDownstreams[downstreamIdx];  
  SOperatorParam* pDst = NULL;
  
  taosWLockLatch(&pCtx->grpLock);
  int32_t num = taosArrayGetSize(pCtx->pNewGrpList);
  if (num <= 0) {
    goto _return;
  }

  for (int32_t i = 0; i < num; ++i) {
    SGcNewGroupInfo* pNew = taosArrayGet(pCtx->pNewGrpList, i);
    code = addNewGroupToVgHash(pCtx->pVgTbHash, pNew);
    if (code) {
      goto _return;
    }

    if (num > 1) {
      if (0 == i) {
        pDst = pNew->pParam;
      } else {
        code = mergeOperatorParams(pDst, pNew->pParam);
        if (code) {
          goto _return;
        }
      }
    } else {
      pDst = pNew->pParam;
    }
  }

  taosArrayClear(pCtx->pNewGrpList);
  
_return:

  taosWUnLockLatch(&pCtx->grpLock);
  *ppParam = pDst;
  
  return code;
}

static FORCE_INLINE int32_t getBlkFromDownstreamOperator(struct SOperatorInfo* pOperator, int32_t downstreamIdx, SSDataBlock** ppRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  SOperatorParam* pDownstreamParam = NULL;
  SSDataBlock* pBlock = NULL;
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  code = appendNewGroupToDownstream(pOperator, downstreamIdx, &pDownstreamParam);
  if (code) {
    return code;
  }

  if (pDownstreamParam) {
    pBlock = pOperator->pDownstream[downstreamIdx]->fpSet.getNextExtFn(pOperator->pDownstream[downstreamIdx], pDownstreamParam);
  } else {
    pBlock = pOperator->pDownstream[downstreamIdx]->fpSet.getNextFn(pOperator->pDownstream[downstreamIdx]);
  }

  if (pBlock) {
    pGCache->execInfo.pDownstreamBlkNum[downstreamIdx]++;
    if (NULL == pGCache->pDownstreams[downstreamIdx].pBaseBlock) {
      code = buildGroupCacheBaseBlock(&pGCache->pDownstreams[downstreamIdx].pBaseBlock, pBlock);
      if (code) {
        return code;
      }
      taosArrayPush(pGCache->pDownstreams[downstreamIdx].pFreeBlock, &pGCache->pDownstreams[downstreamIdx].pBaseBlock);
    }
  }

  *ppRes = pBlock;
  
  return code;
}

static void notifyWaitingSessions(SArray* pWaitQueue) {
  if (NULL == pWaitQueue || taosArrayGetSize(pWaitQueue) <= 0) {
    return;
  }
  
  int32_t n = taosArrayGetSize(pWaitQueue);
  for (int32_t i = 0; i < n; ++i) {
    SGcSessionCtx* pSession = taosArrayGetP(pWaitQueue, i);
    tsem_post(&pSession->waitSem);
  }
}

static FORCE_INLINE void handleGroupFetchDone(SGroupCacheData* pGroup) {
  pGroup->pBlock = NULL;
  pGroup->fetchDone = true;
  
  taosThreadMutexLock(&pGroup->mutex);
  notifyWaitingSessions(pGroup->waitQueue);
  taosArrayClear(pGroup->waitQueue);
  taosThreadMutexUnlock(&pGroup->mutex);
}

static void handleVgroupTableFetchDone(SGcDownstreamCtx* pCtx, SGroupCacheData* pGroup, uint64_t uid) {
  if (pCtx->lastBlkUid == uid || pGroup->pVgCtx->lastUid == uid) {
    return;
  }
  pCtx->lastBlkUid = uid;
  pGroup->pVgCtx->lastUid = uid;
  
  int32_t i = 0;
  while (true) {
    SGcNewGroupInfo* pNew = taosArrayGet(pGroup->pVgCtx->pTbList, i++);
    if (NULL == pNew || pNew->uid == uid) {
      break;
    }
    handleGroupFetchDone(pNew->pGroup);
  }

  if (pGroup->pVgCtx->fileSize >= GROUP_CACHE_DEFAULT_MAX_FILE_SIZE) {
    pGroup->pVgCtx->fileId++;
    pGroup->pVgCtx->fileSize = 0;
  }

  pGroup->fileId = pGroup->pVgCtx->fileId;
  pGroup->startOffset = pGroup->pVgCtx->fileSize;
}

static int32_t handleGroupCacheRetrievedBlk(struct SOperatorInfo* pOperator, SSDataBlock* pBlock, SGcSessionCtx* pSession, bool* continueFetch) {
  int32_t code = TSDB_CODE_SUCCESS;
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  SGcDownstreamCtx* pCtx = &pGCache->pDownstreams[pSession->downstreamIdx];
  SHashObj* pGrpHash = pGCache->globalGrp ? pGCache->pGrpHash : pCtx->pGrpHash;

  if (pGCache->grpByUid) {
    SGroupCacheData* pGroup = taosHashGet(pGrpHash, &pBlock->info.id.uid, sizeof(pBlock->info.id.uid));
    if (NULL == pGroup) {
      qError("table uid:%" PRIu64 " not found in group hash", pBlock->info.id.uid);
      return TSDB_CODE_INVALID_PARA;
    }
    
    handleVgroupTableFetchDone(pCtx, pGroup, pBlock->info.id.uid);
    
    pGroup->pBlock = pBlock;
    
    if (pGroup->needCache) {
      SGcBlkBufInfo newBlkBuf;
      code = addBlkToBufCache(pOperator, pBlock, pCtx, pGroup, &newBlkBuf);
      if (code) {
        return code;
      }
      
      if (pGroup->endBlkId > 0) {
        pGroup->endBlkId = newBlkBuf.blkId;
      } else {
        pGroup->startBlkId = newBlkBuf.blkId;
        pGroup->endBlkId = newBlkBuf.blkId;
      }
    }

    notifyWaitingSessions(pGroup->waitQueue);
    if (pGroup == pSession->pGroupData) {
      *continueFetch = false;
    }

    return TSDB_CODE_SUCCESS;
  }

  return TSDB_CODE_INVALID_PARA;
}

static int32_t handleDownstreamFetchDone(struct SOperatorInfo* pOperator, SGcSessionCtx* pSession) {
  int32_t code = TSDB_CODE_SUCCESS;
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  SGcDownstreamCtx* pCtx = &pGCache->pDownstreams[pSession->downstreamIdx];  
  int32_t uidNum = 0;
  SHashObj* pGrpHash = pGCache->globalGrp ? pGCache->pGrpHash : pCtx->pGrpHash;
  SGcVgroupCtx* pVgCtx = NULL;
  int32_t iter = 0;
  while (pVgCtx = tSimpleHashIterate(pCtx->pVgTbHash, pVgCtx, &iter)) {
    uidNum = taosArrayGetSize(pVgCtx->pTbList);
    for (int32_t i = 0; i < uidNum; ++i) {
      SGcNewGroupInfo* pNew = taosArrayGet(pVgCtx->pTbList, i);
      handleGroupFetchDone(pNew->pGroup);
    }
    taosArrayClear(pVgCtx->pTbList);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t getCacheBlkFromDownstreamOperator(struct SOperatorInfo* pOperator, SGcSessionCtx* pSession, SSDataBlock** ppRes) {
  bool continueFetch = true;
  int32_t code = TSDB_CODE_SUCCESS;
  
  while (continueFetch && TSDB_CODE_SUCCESS == code) {
    int32_t code = getBlkFromDownstreamOperator(pOperator, pSession->downstreamIdx, ppRes);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    
    if (NULL == *ppRes) {
      code = handleDownstreamFetchDone(pOperator, pSession);
      break;
    } else {
      code = handleGroupCacheRetrievedBlk(pOperator, *ppRes, pSession, &continueFetch);
    }
  }

  return code;
}

static int32_t groupCacheSessionWait(SGroupCacheOperatorInfo* pGCache, SGroupCacheData* pGroup, SGcSessionCtx* pSession, SSDataBlock** ppRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (NULL == pGroup->waitQueue) {
    pGroup->waitQueue = taosArrayInit(1, POINTER_BYTES);
    if (NULL == pGroup->waitQueue) {
      taosThreadMutexUnlock(&pSession->pGroupData->mutex);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  
  taosArrayPush(pGroup->waitQueue, &pSession);

  if (!pSession->semInit) {
    tsem_init(&pSession->waitSem, 0, 0);
    pSession->semInit = true;
  }

  taosThreadMutexUnlock(&pSession->pGroupData->mutex);

  tsem_wait(&pSession->waitSem);

  if (pSession->pGroupData->needCache) {
    if (pSession->lastBlkId < 0) {
      int64_t startBlkId = atomic_load_64(&pSession->pGroupData->startBlkId);
      if (startBlkId > 0) {
        code = retrieveBlkFromBufCache(pGCache, pSession->pGroupData, startBlkId, &pSession->nextOffset, ppRes);
        pSession->lastBlkId = startBlkId;
        return code;
      } else if (pGroup->fetchDone) {
        *ppRes = NULL;
        return TSDB_CODE_SUCCESS;
      }
    } else if (pSession->lastBlkId < atomic_load_64(&pSession->pGroupData->endBlkId)) {
      code = retrieveBlkFromBufCache(pGCache, pSession->pGroupData, pSession->lastBlkId + 1, &pSession->nextOffset, ppRes);
      pSession->lastBlkId++;
      return code;
    } else if (pGroup->fetchDone) {
      *ppRes = NULL;
      return TSDB_CODE_SUCCESS;
    }
  } else {
    *ppRes = pSession->pGroupData->pBlock;
    return TSDB_CODE_SUCCESS;
  }

  qError("no block retrieved from downstream and waked up");
  return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
}

static int32_t getBlkFromSessionCacheImpl(struct SOperatorInfo* pOperator, int64_t sessionId, SGcSessionCtx* pSession, SSDataBlock** ppRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  bool locked = false;
  
  while (true) {
    if (pSession->pGroupData->needCache) {
      if (pSession->lastBlkId < 0) {
        int64_t startBlkId = atomic_load_64(&pSession->pGroupData->startBlkId);
        if (startBlkId > 0) {
          code = retrieveBlkFromBufCache(pGCache, pSession->pGroupData, startBlkId, &pSession->nextOffset, ppRes);
          pSession->lastBlkId = startBlkId;
          goto _return;
        }
      } else if (pSession->lastBlkId < atomic_load_64(&pSession->pGroupData->endBlkId)) {
        code = retrieveBlkFromBufCache(pGCache, pSession->pGroupData, pSession->lastBlkId + 1, &pSession->nextOffset, ppRes);
        pSession->lastBlkId++;
        goto _return;
      } else if (atomic_load_8((int8_t*)&pSession->pGroupData->fetchDone)) {
        *ppRes = NULL;
        goto _return;
      }
    } else if (pSession->pGroupData->pBlock || atomic_load_8((int8_t*)&pSession->pGroupData->fetchDone)) {
      *ppRes = pSession->pGroupData->pBlock;
      pSession->pGroupData->pBlock = NULL;
      goto _return;
    }

    if ((atomic_load_64(&pGCache->pDownstreams[pSession->downstreamIdx].fetchSessionId) == sessionId)
      || (-1 == atomic_val_compare_exchange_64(&pGCache->pDownstreams[pSession->downstreamIdx].fetchSessionId, -1, sessionId))) {
      if (locked) {
        taosThreadMutexUnlock(&pSession->pGroupData->mutex);
        locked = false;
      }
      
      code = getCacheBlkFromDownstreamOperator(pOperator, pSession, ppRes);
      goto _return;
    }

    if (locked) {
      code = groupCacheSessionWait(pGCache, pSession->pGroupData, pSession, ppRes);
      locked = false;
      if (TSDB_CODE_SUCCESS != code) {
        goto _return;
      }
      
      break;
    }
    
    taosThreadMutexLock(&pSession->pGroupData->mutex);
    locked = true;
  };


_return:

  if (locked) {
    taosThreadMutexUnlock(&pSession->pGroupData->mutex);
  }

  return code;
}


static int32_t initGroupCacheBlockCache(SGroupCacheOperatorInfo* pInfo) {
  SGcBlkCacheInfo* pCache = &pInfo->blkCache;
  pCache->pCacheFile = tSimpleHashInit(10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  if (NULL == pCache->pCacheFile) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCache->pDirtyBlk = taosHashInit(10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == pCache->pDirtyBlk) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCache->pReadBlk = taosHashInit(10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == pCache->pReadBlk) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE void initNewGroupData(SGcDownstreamCtx* pCtx, SGroupCacheData* pGroup, SGcOperatorParam* pGcParam) {
  taosThreadMutexInit(&pGroup->mutex, NULL);
  pGroup->needCache = pGcParam->needCache;
  pGroup->downstreamIdx = pGcParam->downstreamIdx;
  pGroup->vgId = pGcParam->vgId;
  pGroup->fileId = -1;
  pGroup->startBlkId = -1;
  pGroup->endBlkId = -1;
  pGroup->startOffset = -1;
  pGroup->pVgCtx = tSimpleHashGet(pCtx->pVgTbHash, &pGroup->vgId, sizeof(pGroup->vgId));
}

static int32_t addNewGroupData(struct SOperatorInfo* pOperator, SOperatorParam* pParam, SGroupCacheData** ppGrp) {
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  SGcOperatorParam* pGcParam = pParam->value;  
  SGcDownstreamCtx* pCtx = &pGCache->pDownstreams[pParam->downstreamIdx];
  SHashObj* pGrpHash = pGCache->globalGrp ? pGCache->pGrpHash : pCtx->pGrpHash;
  SGroupCacheData grpData = {0};
  initNewGroupData(pCtx, &grpData, pGcParam);
  
  while (true) {
    if (0 != taosHashPut(pGrpHash, &pGcParam->tbUid, sizeof(pGcParam->tbUid), &grpData, sizeof(grpData))) {
      if (terrno == TSDB_CODE_DUP_KEY) {
        *ppGrp = taosHashAcquire(pGrpHash, &pGcParam->tbUid, sizeof(pGcParam->tbUid));
        if (*ppGrp) {
          break;
        }
      } else {
        return terrno;
      }
    }

    *ppGrp = taosHashAcquire(pGrpHash, &pGcParam->tbUid, sizeof(pGcParam->tbUid));
    if (*ppGrp) {
      SGcNewGroupInfo newGroup;
      newGroup.pGroup = *ppGrp;
      newGroup.vgId = pGcParam->vgId;
      newGroup.uid = pGcParam->tbUid;
      newGroup.pParam = taosArrayGet(pParam->pChildren, 0);
      
      taosWLockLatch(&pCtx->grpLock);
      if (NULL == taosArrayPush(pCtx->pNewGrpList, &newGroup)) {
        taosWUnLockLatch(&pCtx->grpLock);
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      taosWUnLockLatch(&pCtx->grpLock);
      
      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t initGroupCacheSession(struct SOperatorInfo* pOperator, SOperatorParam* pParam, SGcSessionCtx** ppSession) {
  int32_t code = TSDB_CODE_SUCCESS;
  SGcSessionCtx ctx = {0};
  SGcOperatorParam* pGcParam = pParam->value;  
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  SGcDownstreamCtx* pCtx = &pGCache->pDownstreams[pParam->downstreamIdx];
  SHashObj* pGrpHash = pGCache->globalGrp ? pGCache->pGrpHash : pCtx->pGrpHash;

  SGroupCacheData* pGroup = taosHashAcquire(pGrpHash, &pGcParam->tbUid, sizeof(pGcParam->tbUid));
  if (pGroup) {
    ctx.pGroupData = pGroup;
  } else {
    code = addNewGroupData(pOperator, pParam, &ctx.pGroupData);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }
  
  ctx.pParam = pGcParam;

  code = taosHashPut(pGCache->pSessionHash, &pGcParam->sessionId, sizeof(pGcParam->sessionId), &ctx, sizeof(ctx));
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  *ppSession = taosHashGet(pGCache->pSessionHash, &pGcParam->sessionId, sizeof(pGcParam->sessionId));
  
  return TSDB_CODE_SUCCESS;
}

static int32_t getBlkFromGroupCache(struct SOperatorInfo* pOperator, SSDataBlock** ppRes, SOperatorParam* pParam) {
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  SGcOperatorParam* pGcParam = pParam->value;
  SGcSessionCtx* pSession = taosHashGet(pGCache->pSessionHash, &pGcParam->sessionId, sizeof(pGcParam->sessionId));
  if (NULL == pSession) {
    int32_t code = initGroupCacheSession(pOperator, pParam, &pSession);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }
  
  return getBlkFromSessionCacheImpl(pOperator, pGcParam->sessionId, pSession, ppRes);
}

static FORCE_INLINE void destroyCurrentGroupCacheSession(SGroupCacheOperatorInfo* pGCache, SGcSessionCtx** ppCurrent, int64_t* pCurrentId) {
  if (NULL == *ppCurrent) {
    return;
  }
  if (taosHashRemove(pGCache->pSessionHash, pCurrentId, sizeof(*pCurrentId))) {
    qError("remove session %" PRIx64 " failed", *pCurrentId);
  }
  
  *ppCurrent = NULL;
  *pCurrentId = 0;
}

static int32_t initGroupCacheExecInfo(SOperatorInfo*        pOperator) {
  SGroupCacheOperatorInfo* pInfo = pOperator->info;
  pInfo->execInfo.downstreamNum = pOperator->numOfDownstream;
  pInfo->execInfo.pDownstreamBlkNum = taosMemoryCalloc(pOperator->numOfDownstream, sizeof(int64_t));
  if (NULL == pInfo->execInfo.pDownstreamBlkNum) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t initGroupCacheDownstreamCtx(SOperatorInfo*          pOperator) {
  SGroupCacheOperatorInfo* pInfo = pOperator->info;
  pInfo->pDownstreams = taosMemoryMalloc(pOperator->numOfDownstream * sizeof(*pInfo->pDownstreams));
  if (NULL == pInfo->pDownstreams) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
    pInfo->pDownstreams[i].fetchSessionId = -1;
    pInfo->pDownstreams[i].lastBlkUid = 0;
    pInfo->pDownstreams[i].pVgTbHash = tSimpleHashInit(10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
    if (NULL == pInfo->pDownstreams[i].pVgTbHash) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    
    pInfo->pDownstreams[i].pNewGrpList = taosArrayInit(10, sizeof(SGcNewGroupInfo));
    if (NULL == pInfo->pDownstreams[i].pNewGrpList) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    if (!pInfo->globalGrp) {
      pInfo->pDownstreams[i].pGrpHash = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
      if (pInfo->pDownstreams[i].pGrpHash == NULL) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
    }
    
    pInfo->pDownstreams[i].pFreeBlock = taosArrayInit(10, POINTER_BYTES);
    if (NULL == pInfo->pDownstreams[i].pFreeBlock) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return TSDB_CODE_SUCCESS;
}

SSDataBlock* groupCacheGetNext(struct SOperatorInfo* pOperator, SOperatorParam* pParam) {
  SSDataBlock* pBlock = NULL;

  int32_t code = getBlkFromGroupCache(pOperator, &pBlock, pParam);
  if (TSDB_CODE_SUCCESS != code) {
    pOperator->pTaskInfo->code = code;
    T_LONG_JMP(pOperator->pTaskInfo->env, pOperator->pTaskInfo->code);
  }

  return pBlock;
}


SOperatorInfo* createGroupCacheOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream,
                                           SGroupCachePhysiNode* pPhyciNode, SExecTaskInfo* pTaskInfo) {
  SGroupCacheOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SGroupCacheOperatorInfo));
  SOperatorInfo*     pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));

  int32_t code = TSDB_CODE_SUCCESS;
  if (pOperator == NULL || pInfo == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  pOperator->transparent = true;
  
  setOperatorInfo(pOperator, "GroupCacheOperator", QUERY_NODE_PHYSICAL_PLAN_GROUP_CACHE, false, OP_NOT_OPENED, pInfo, pTaskInfo);

  pInfo->maxCacheSize = -1;
  pInfo->grpByUid = pPhyciNode->grpByUid;
  pInfo->globalGrp = pPhyciNode->globalGrp;
  
  if (!pInfo->grpByUid) {
    qError("only group cache by uid is supported now");
    code = TSDB_CODE_INVALID_PARA;
    goto _error;
  }
  
  if (pPhyciNode->pGroupCols) {
    code = initGroupColsInfo(&pInfo->groupColsInfo, pPhyciNode->grpColsMayBeNull, pPhyciNode->pGroupCols);
    if (code) {
      goto _error;
    }
  }

  code = initGroupCacheBlockCache(pInfo);
  if (code) {
    goto _error;
  }

  if (pInfo->globalGrp) {
    pInfo->pGrpHash = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
    if (pInfo->pGrpHash == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _error;
    }
  }

  pInfo->pSessionHash = taosHashInit(20, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (pInfo->pSessionHash == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  code = appendDownstream(pOperator, pDownstream, numOfDownstream);
  if (TSDB_CODE_SUCCESS != code) {
    goto _error;
  }

  code = initGroupCacheDownstreamCtx(pOperator);
  if (TSDB_CODE_SUCCESS != code) {
    goto _error;
  }

  code = initGroupCacheExecInfo(pOperator);
  if (TSDB_CODE_SUCCESS != code) {
    goto _error;
  }

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, NULL, NULL, destroyGroupCacheOperator, optrDefaultBufFn, NULL, groupCacheGetNext, NULL);

  return pOperator;

_error:
  if (pInfo != NULL) {
    destroyGroupCacheOperator(pInfo);
  }

  taosMemoryFree(pOperator);
  pTaskInfo->code = code;
  return NULL;
}


