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
  taosHashCleanup(pGrpCacheOperator->pGrpHash);
  
  taosMemoryFreeClear(param);
}

static FORCE_INLINE int32_t initOpenCacheFile(SGcVgroupFileFd* pFileFd, char* filename) {
  TdFilePtr newFd = taosOpenFile(filename, TD_FILE_CREATE|TD_FILE_READ|TD_FILE_WRITE|TD_FILE_AUTO_DEL);
  if (NULL == newFd) {
    return TAOS_SYSTEM_ERROR(errno);
  }
  pFileFd->fd = newFd;
  taosThreadMutexInit(&pFileFd->mutex, NULL);
  return TSDB_CODE_SUCCESS;
}

static int32_t acquireVgroupFileFd(SGroupCacheOperatorInfo* pGCache, SGcDownstreamCtx* pCtx, SGcVgroupCtx* pVgCtx, uint32_t fileId, SGcVgroupFileFd** ppFd) {
  int32_t code = TSDB_CODE_SUCCESS;
  SGcVgroupFileFd* pTmp = tSimpleHashGet(pVgCtx->pCacheFile, &fileId, sizeof(fileId));
  if (NULL == pTmp) {
    sprintf(pVgCtx->baseFilename[pVgCtx->baseNameLen], "_%u", fileId);

    SGcVgroupFileFd newVgFd = {0};
    tSimpleHashPut(pVgCtx->pCacheFile, &fileId, sizeof(fileId), &newVgFd, sizeof(newVgFd));
    pTmp = tSimpleHashGet(pVgCtx->pCacheFile, &fileId, sizeof(fileId));

    code = initOpenCacheFile(pTmp, pVgCtx->baseFilename);
    if (code) {
      return code;
    }
  }

  taosThreadMutexLock(&pTmp->mutex);
  *ppFd = pTmp;
  
  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE void releaseVgroupFileFd(SGcVgroupFileFd* pFd) {
  taosThreadMutexUnlock(&pFd->mutex);
}

static int32_t saveBatchBlocksToDisk(SGroupCacheOperatorInfo* pGCache, SGcBlkBufInfo* pHead) {
  int32_t code = TSDB_CODE_SUCCESS;
  SGcBlkBufBasic blkBasic;
  
  while (NULL != pHead) {
    if (NULL == pHead->pCtx->cacheFileFd[0]) {
      pHead->pCtx->cacheFileFd[0] = taosOpenFile(pHead->pCtx->baseFilename, TD_FILE_CREATE|TD_FILE_READ|TD_FILE_WRITE|TD_FILE_AUTO_DEL);
      if (NULL == pHead->pCtx->cacheFileFd[0]) {
        return TAOS_SYSTEM_ERROR(errno);
      }
      pHead->pCtx->cacheFileFdNum = 1;
    }

    int32_t ret = taosLSeekFile(pHead->pCtx->cacheFileFd[0], pHead->offset, SEEK_SET);
    if (ret == -1) {
      return TAOS_SYSTEM_ERROR(errno);
    }
    
    ret = (int32_t)taosWriteFile(pHead->pCtx->cacheFileFd[0], pHead->pBuf, pHead->bufSize);
    if (ret != pHead->bufSize) {
      return TAOS_SYSTEM_ERROR(errno);
    }

    taos

    int64_t blkId = pHead->blkId;
    pHead = pHead->next;

    taosHashRemove(pGCache->blkCache.pDirtyBlk, &blkId, sizeof(blkId));
  }

  return code;
}


static int32_t saveSeqBlocksToDisk(SGroupCacheOperatorInfo* pGCache, SGcBlkBufInfo* pHead) {
  int32_t code = TSDB_CODE_SUCCESS;

  SGcVgroupFileFd* pFd = NULL;
  while (NULL != pHead) {
    code = acquireVgroupFileFd(pGCache, pHead->pCtx, pHead->pGroup->pVgCtx, pHead->fileId, &pFd);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }

    int32_t ret = taosLSeekFile(pFd->fd, pHead->offset, SEEK_SET);
    if (ret == -1) {
      code= TAOS_SYSTEM_ERROR(errno);
      return code;
    }
    
    code = (int32_t)taosWriteFile(pFd->fd, pHead->pBuf, pHead->bufSize);
    releaseVgroupFileFd(pFd);
    if (code != pHead->bufSize) {
      code= TAOS_SYSTEM_ERROR(errno);
      return code;
    }
    
    int64_t blkId = pHead->blkId;
    pHead = pHead->next;
    
    taosHashRemove(pGCache->blkCache.pDirtyBlk, &blkId, sizeof(blkId));
  }

  return code;
}


static int32_t saveBlocksToDisk(SGroupCacheOperatorInfo* pGCache, SGcDownstreamCtx* pCtx, SGcBlkBufInfo* pHead) {
  int32_t code = TSDB_CODE_SUCCESS;
  
  if (pGCache->batchFetch) {
    code = saveBatchBlocksToDisk(pGCache, pHead);
  } else {
    code = saveSeqBlocksToDisk(pGCache, pHead);
  }

  atomic_val_compare_exchange_32(&pGCache->blkCache.writeDownstreamId, pCtx->id, -1);

  return code;
}

static int32_t addBlkToDirtyBufList(SGroupCacheOperatorInfo* pGCache, SGcDownstreamCtx* pCtx, SGcVgroupCtx* pVgCtx, SGcBlkCacheInfo* pCache, SGcBlkBufInfo* pBufInfo) {
  if (0 != taosHashPut(pCache->pDirtyBlk, &pBufInfo->blkId, sizeof(pBufInfo->blkId), pBufInfo, sizeof(*pBufInfo))) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pBufInfo = taosHashGet(pCache->pDirtyBlk, &pBufInfo->blkId, sizeof(pBufInfo->blkId));

  int32_t code = TSDB_CODE_SUCCESS;
  SGcBlkBufInfo* pWriteHead = NULL;
  
  taosWLockLatch(&pCache->dirtyLock);
  pCache->blkCacheSize += pBufInfo->bufSize;
  qError("group cache block cache num:%d size:%" PRId64 , taosHashGetSize(pCache->pDirtyBlk), pCache->blkCacheSize);

  if (NULL == pCache->pDirtyHead) {
    pCache->pDirtyHead = pBufInfo;
  } else {
    pCache->pDirtyTail->next = pBufInfo;
  }
  pCache->pDirtyTail = pBufInfo;
    
  if (pGCache->maxCacheSize > 0 && pCache->blkCacheSize > pGCache->maxCacheSize) {
    if (-1 == atomic_val_compare_exchange_32(&pCache->writeDownstreamId, -1, pCtx->id)) {
      pWriteHead = pCache->pDirtyHead;
      SGcBlkBufInfo* pTmp = pCache->pDirtyHead;
      while (NULL != pTmp) {
        pCache->blkCacheSize -= pTmp->bufSize;
        if (pCache->blkCacheSize <= pGCache->maxCacheSize) {
          pCache->pDirtyHead = pTmp->next;
          pTmp->next = NULL;
          break;
        }
        pTmp = pTmp->next;
      }
    }
  }
  taosWUnLockLatch(&pCache->dirtyLock);

  if (NULL != pWriteHead) {
    code = saveBlocksToDisk(pGCache, pCtx, pWriteHead);
  }

  return code;
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

  pBufInfo->next = NULL;
  pBufInfo->blkId = atomic_add_fetch_64(&pGCache->currentBlkId, 1);
  pBufInfo->fileId = pGroup->fileId;
  pBufInfo->bufSize = bufSize;
  pBufInfo->offset = atomic_fetch_add_64(&pGroup->pVgCtx->fileSize, bufSize);
  pBufInfo->pCtx = pCtx;
  pBufInfo->pGroup = pGroup;

  int32_t code = addBlkToDirtyBufList(pGCache, pCtx, pGroup->pVgCtx, &pGCache->blkCache, pBufInfo);

  return code;
}

void blockDataDeepCleanup(SSDataBlock* pDataBlock) {
  size_t numOfCols = taosArrayGetSize(pDataBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* p = taosArrayGet(pDataBlock->pDataBlock, i);
    p->pData = NULL;
    if (IS_VAR_DATA_TYPE(p->info.type)) {
      p->varmeta.offset = NULL;
      p->varmeta.length = 0;
      p->varmeta.allocLen = 0;
    } else {
      p->nullbitmap = NULL;
    }
  }
  pDataBlock->info.capacity = 0;
  pDataBlock->info.rows = 0;
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
  blockDataDeepCleanup(*ppDst);
  return TSDB_CODE_SUCCESS;
}

static int32_t acquireBaseBlockFromList(SGcDownstreamCtx* pCtx, SSDataBlock** ppRes) {
  taosWLockLatch(&pCtx->blkLock);
  if (taosArrayGetSize(pCtx->pFreeBlock) <= 0) {
    taosWUnLockLatch(&pCtx->blkLock);
    return buildGroupCacheBaseBlock(ppRes, pCtx->pBaseBlock);
  }
  *ppRes = *(SSDataBlock**)taosArrayPop(pCtx->pFreeBlock);
  taosWUnLockLatch(&pCtx->blkLock);

  return TSDB_CODE_SUCCESS;  
}

static void releaseBaseBlockToList(SGcDownstreamCtx* pCtx, SSDataBlock* pBlock) {
  blockDataDeepCleanup(pBlock);
  taosWLockLatch(&pCtx->blkLock);
  taosArrayPush(pCtx->pFreeBlock, &pBlock);
  taosWUnLockLatch(&pCtx->blkLock);
}


static int32_t buildGroupCacheResultBlock(SGroupCacheOperatorInfo* pGCache, int32_t downstreamIdx, void* pBuf, SSDataBlock** ppRes) {
  int32_t code = acquireBaseBlockFromList(&pGCache->pDownstreams[downstreamIdx], ppRes);
  if (code) {
    return code;
  }
  //TODO OPTIMIZE PERF
  return blockDataFromBuf(*ppRes, pBuf);
}

static int32_t retrieveBlkFromBufCache(SGroupCacheOperatorInfo* pGCache, SGroupCacheData* pGrp, int64_t sessionId, int64_t blkId, int64_t* nextOffset, SSDataBlock** ppRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  SGcBlkCacheInfo* pCache = &pGCache->blkCache;
  SGcBlkBufInfo bufInfo;
  SGcBlkBufBasic* pBlkBasic = NULL;
  void* pBuf = NULL;

  taosRLockLatch(&pCache->dirtyLock);
  SGcBlkBufInfo* pBufInfo = taosHashAcquire(pCache->pDirtyBlk, &blkId, sizeof(blkId));
  if (NULL == pBufInfo) {
    code = readBlockFromDisk(pGCache, pGrp, blkId, nextOffset, &bufInfo.basic, &pBuf);
    if (code) {
      return code;
    }
    pBlkBasic = &bufInfo.basic;
  } else {
    pBlkBasic = &pBufInfo->basic;
    pBuf = pBufInfo->pBuf;
  }
  
  code = buildGroupCacheResultBlock(pGCache, pGrp->downstreamIdx, pBuf, ppRes);
  taosRUnLockLatch(&pCache->dirtyLock);
  if (code) {
    return code;
  }

  *nextOffset = pBlkBasic->offset + pBlkBasic->bufSize;

  taosHashPut(pCache->pReadBlk, &sessionId, sizeof(sessionId), ppRes, POINTER_BYTES);
  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE void initGcVgroupCtx(SGcVgroupCtx* pVgCtx, int32_t downstreamId, int32_t vgId, SArray* pTbList) {
  pVgCtx->pTbList = pTbList;
  
  snprintf(pVgCtx->baseFilename, sizeof(pVgCtx->baseFilename) - 1, "%s/gc_%d_%s_%d_%d", tsTempDir, getpid(), GET_TASKID(pOperator->pTaskInfo), downstreamId, vgId);
  pVgCtx->baseFilename[sizeof(pVgCtx->baseFilename) - 1] = 0;

  pVgCtx->baseNameLen = strlen(pVgCtx->baseFilename);
}

static int32_t addNewGroupToVgHash(SSHashObj* pHash, SGcNewGroupInfo* pNew) {
  SGcVgroupCtx* pVgCtx = pNew->pGroup->pVgCtx;
  if (NULL == pVgCtx) {
    SArray* pList = taosArrayInit(10, sizeof(*pNew));
    if (NULL == pList) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    taosArrayPush(pList, pNew);
    SGcVgroupCtx vgCtx = {0};
    initGcVgroupCtx(&vgCtx, pNew->pGroup->downstreamIdx, pNew->vgId, pList);
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
    if (!pGCache->batchFetch) {
      code = addNewGroupToVgHash(pCtx->pVgTbHash, pNew);
      if (code) {
        goto _return;
      }
    }

    if (NULL == pDst) {
      pDst = pNew->pParam;
    } else if (pNew->pParam) {
      code = mergeOperatorParams(pDst, pNew->pParam);
      if (code) {
        goto _return;
      }
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
    qError("%s group cache retrieved block with groupId: %" PRIu64, GET_TASKID(pOperator->pTaskInfo), pBlock->info.id.groupId);
    
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
  atomic_store_8((int8_t*)&pGroup->fetchDone, true);
  
  taosThreadMutexLock(&pGroup->mutex);
  notifyWaitingSessions(pGroup->waitQueue);
  taosArrayClear(pGroup->waitQueue);
  taosThreadMutexUnlock(&pGroup->mutex);
}

static int32_t vgroupSwitchNewFile(SGcVgroupCtx* pVgCtx) {
  if (NULL != pVgCtx->cacheFileFd) {
    if (NULL == pVgCtx->pCacheFile) {
      pVgCtx->pCacheFile = tSimpleHashInit(10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
      if (NULL == pVgCtx->pCacheFile) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
    }
    tSimpleHashPut(pVgCtx->pCacheFile, &pVgCtx->fileId, sizeof(pVgCtx->fileId), &pVgCtx->cacheFileFd, sizeof(pVgCtx->cacheFileFd));
  }
  
  pVgCtx->fileId++;
  pVgCtx->fileSize = 0;
  return TSDB_CODE_SUCCESS;
}

static int32_t handleVgroupTableFetchDone(SGcDownstreamCtx* pCtx, SGroupCacheData* pGroup, uint64_t uid) {
  if (pCtx->lastBlkUid == uid || pGroup->pVgCtx->lastUid == uid) {
    return TSDB_CODE_SUCCESS;
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
    int32_t code = vgroupSwitchNewFile(pGroup->pVgCtx);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  pGroup->fileId = pGroup->pVgCtx->fileId;
  pGroup->startOffset = pGroup->pVgCtx->fileSize;

  return TSDB_CODE_SUCCESS;
}


static FORCE_INLINE void initNewGroupData(SGcDownstreamCtx* pCtx, SGroupCacheData* pGroup, int32_t downstreamIdx, int32_t vgId, bool batchFetch) {
  taosThreadMutexInit(&pGroup->mutex, NULL);
  pGroup->downstreamIdx = downstreamIdx;
  pGroup->vgId = vgId;
  if (batchFetch) {
    pGroup->fileId = 0;
    pGroup->batchList.pList = taosArrayInit(10, POINTER_BYTES);
  } else {
    pGroup->fileId = -1;
    pGroup->seqList.startBlkId = -1;
    pGroup->seqList.endBlkId = -1;

  }
  pGroup->startOffset = -1;
  pGroup->pVgCtx = tSimpleHashGet(pCtx->pVgTbHash, &pGroup->vgId, sizeof(pGroup->vgId));
}

static int32_t addNewGroupData(struct SOperatorInfo* pOperator, SOperatorParam* pParam, SGroupCacheData** ppGrp, int32_t vgId, int64_t uid) {
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  SGcDownstreamCtx* pCtx = &pGCache->pDownstreams[pParam->downstreamIdx];
  SHashObj* pGrpHash = pGCache->globalGrp ? pGCache->pGrpHash : pCtx->pGrpHash;
  SGroupCacheData grpData = {0};
  initNewGroupData(pCtx, &grpData, pParam->downstreamIdx, vgId, pGCache->batchFetch);
  
  while (true) {
    if (0 != taosHashPut(pGrpHash, &uid, sizeof(uid), &grpData, sizeof(grpData))) {
      if (terrno == TSDB_CODE_DUP_KEY) {
        *ppGrp = taosHashAcquire(pGrpHash, &uid, sizeof(uid));
        if (*ppGrp) {
          break;
        }
      } else {
        return terrno;
      }
    }

    *ppGrp = taosHashAcquire(pGrpHash, &uid, sizeof(uid));
    if (*ppGrp && pParam->pChildren) {
      SGcNewGroupInfo newGroup;
      newGroup.pGroup = *ppGrp;
      newGroup.vgId = vgId;
      newGroup.uid = uid;
      newGroup.pParam = taosArrayGetP(pParam->pChildren, 0);
      
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

static int32_t addBlkToGroupCache(bool batchFetch, SGroupCacheData* pGroup, SGcBlkBufInfo* pNewBlk) {
  if (batchFetch) {
    taosWLockLatch(&pGroup->batchList.lock);
    taosArrayPush(pGroup->batchList.pList, &pNewBlk->basic);
    qError("block added to group cache, total block num:%" PRId64, (int64_t)taosArrayGetSize(pGroup->batchList.pList));
    taosWUnLockLatch(&pGroup->batchList.lock);
    return TSDB_CODE_SUCCESS;
  }

  taosWLockLatch(&pGroup->batchList.lock);
  
  if (pGroup->seqList.endBlkId > 0) {
    atomic_store_64(&pGroup->seqList.endBlkId, pNewBlk->blkId);
  } else {
    atomic_store_64(&pGroup->seqList.startBlkId, pNewBlk->blkId);
    atomic_store_64(&pGroup->seqList.endBlkId, pNewBlk->blkId);
  }

  qError("block added to group cache, total block num:%" PRId64, pGroup->seqList.endBlkId - pGroup->seqList.startBlkId + 1);

  return TSDB_CODE_SUCCESS;
}

static int32_t handleGroupCacheRetrievedBlk(struct SOperatorInfo* pOperator, SSDataBlock* pBlock, SGcSessionCtx* pSession, bool* continueFetch) {
  int32_t code = TSDB_CODE_SUCCESS;
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  SGcDownstreamCtx* pCtx = &pGCache->pDownstreams[pSession->downstreamIdx];
  SHashObj* pGrpHash = pGCache->globalGrp ? pGCache->pGrpHash : pCtx->pGrpHash;

  SGroupCacheData* pGroup = taosHashGet(pGrpHash, &pBlock->info.id.groupId, sizeof(pBlock->info.id.groupId));
  if (NULL == pGroup) {
    if (pGCache->batchFetch) {
      SOperatorParam fakeParam = {0};
      fakeParam.downstreamIdx = pSession->downstreamIdx;
      code = addNewGroupData(pOperator, &fakeParam, &pGroup, -1, pBlock->info.id.groupId);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
    } else {
      qError("table uid:%" PRIu64 " not found in group hash", pBlock->info.id.groupId);
      return TSDB_CODE_INVALID_PARA;
    }
  }

  if (!pGCache->batchFetch) {
    code = handleVgroupTableFetchDone(pCtx, pGroup, pBlock->info.id.groupId);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }
  
  SGcBlkBufInfo newBlkBuf;    
  code = addBlkToBufCache(pOperator, pBlock, pCtx, pGroup, &newBlkBuf);
  if (code) {
    return code;
  }

  code = addBlkToGroupCache(pGCache->batchFetch, pGroup, &newBlkBuf);
  if (code) {
    return code;
  }

  notifyWaitingSessions(pGroup->waitQueue);
  if (pGroup == pSession->pGroupData) {
    pSession->lastBlkId = newBlkBuf.blkId;
    pSession->nextOffset = newBlkBuf.offset + newBlkBuf.bufSize;
    
    *continueFetch = false;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t handleDownstreamFetchDone(struct SOperatorInfo* pOperator, SGcSessionCtx* pSession) {
  int32_t code = TSDB_CODE_SUCCESS;
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  SGcDownstreamCtx* pCtx = &pGCache->pDownstreams[pSession->downstreamIdx];  
  if (pGCache->batchFetch) {
    SHashObj* pGrpHash = pGCache->globalGrp ? pGCache->pGrpHash : pCtx->pGrpHash;
    SGroupCacheData* pGroup = NULL;
    while (pGroup = taosHashIterate(pGrpHash, pGroup)) {
      handleGroupFetchDone(pGroup);
    }
  } else {
    int32_t uidNum = 0;
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
  }

  taosHashClear(pCtx->pWaitSessions);

  return TSDB_CODE_SUCCESS;
}

static int32_t getCacheBlkFromDownstreamOperator(struct SOperatorInfo* pOperator, SGcDownstreamCtx* pCtx, int64_t sessionId, SGcSessionCtx* pSession, SSDataBlock** ppRes) {
  bool continueFetch = true;
  int32_t code = TSDB_CODE_SUCCESS;
  SGroupCacheOperatorInfo* pGCache = pOperator->info;

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

  if (!continueFetch) {
    SGcSessionCtx** ppWaitCtx = taosHashIterate(pCtx->pWaitSessions, NULL);
    if (ppWaitCtx) {
      taosHashCancelIterate(pCtx->pWaitSessions, ppWaitCtx);
      int64_t* pSessionId = taosHashGetKey(ppWaitCtx, NULL);
      if (sessionId != atomic_val_compare_exchange_64(&pCtx->fetchSessionId, sessionId, *pSessionId)) {
        qError("wrong fetch sessionId: %" PRIu64 " expected: %" PRIu64 , pCtx->fetchSessionId, sessionId);
        return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      }
      SGcSessionCtx* pWaitCtx = *ppWaitCtx;
      pWaitCtx->newFetch = true;
      taosHashRemove(pCtx->pWaitSessions, pSessionId, sizeof(*pSessionId));
      tsem_post(&pWaitCtx->waitSem);

      return code;
    }
  }

  if (sessionId != atomic_val_compare_exchange_64(&pCtx->fetchSessionId, sessionId, -1)) {
    qError("wrong fetch sessionId: %" PRIu64 " expected: %" PRIu64 , pCtx->fetchSessionId, sessionId);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  
  return code;
}

static int32_t getBlkFromSessionCacheImpl(struct SOperatorInfo* pOperator, int64_t sessionId, SGcSessionCtx* pSession, SSDataBlock** ppRes, bool* got) {
  int32_t code = TSDB_CODE_SUCCESS;
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  *got = true;
  
  if (pGCache->batchFetch) {
    SGroupBatchBlkList* pBatchList = &pSession->pGroupData->batchList;
    taosRLockLatch(&pBatchList->lock);
    int64_t blkNum = taosArrayGetSize(pBatchList->pList);
    if (pSession->lastBlkId < 0) {
      if (blkNum > 0) {
        SGcBlkBufBasic* pBasic = taosArrayGet(pBatchList->pList, 0);
        taosRUnLockLatch(&pBatchList->lock);
        code = retrieveBlkFromBufCache(pGCache, pSession->pGroupData, sessionId, pBasic->blkId, &pSession->nextOffset, ppRes);
        pSession->lastBlkId = 0;
        return code;
      }
    } else if ((pSession->lastBlkId + 1) < blkNum) {
      SGcBlkBufBasic* pBasic = taosArrayGet(pBatchList->pList, pSession->lastBlkId + 1);
      taosRUnLockLatch(&pBatchList->lock);
      code = retrieveBlkFromBufCache(pGCache, pSession->pGroupData, sessionId, pBasic->blkId, &pSession->nextOffset, ppRes);
      pSession->lastBlkId++;
      return code;
    }
    taosRUnLockLatch(&pBatchList->lock);
  } else {
    if (pSession->lastBlkId < 0) {
      int64_t startBlkId = atomic_load_64(&pSession->pGroupData->seqList.startBlkId);
      if (startBlkId > 0) {
        code = retrieveBlkFromBufCache(pGCache, pSession->pGroupData, sessionId, startBlkId, &pSession->nextOffset, ppRes);
        pSession->lastBlkId = startBlkId;
        return code;
      }
    } else if (pSession->lastBlkId < atomic_load_64(&pSession->pGroupData->seqList.endBlkId)) {
      code = retrieveBlkFromBufCache(pGCache, pSession->pGroupData, sessionId, pSession->lastBlkId + 1, &pSession->nextOffset, ppRes);
      pSession->lastBlkId++;
      return code;
    }
  }

  if (atomic_load_8((int8_t*)&pSession->pGroupData->fetchDone)) {
    *ppRes = NULL;
    return code;
  }

  *got = false;
  return code;
}


static int32_t groupCacheSessionWait(struct SOperatorInfo* pOperator, SGcDownstreamCtx* pCtx, int64_t sessionId, SGcSessionCtx* pSession, SSDataBlock** ppRes) {
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  SGroupCacheData* pGroup = pSession->pGroupData;
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

  taosHashPut(pCtx->pWaitSessions, &sessionId, sizeof(sessionId), &pSession, POINTER_BYTES);

  tsem_wait(&pSession->waitSem);

  if (pSession->newFetch) {
    pSession->newFetch = false;
    return getCacheBlkFromDownstreamOperator(pOperator, pCtx, sessionId, pSession, ppRes);
  }

  taosHashRemove(pCtx->pWaitSessions, &sessionId, sizeof(sessionId));

  bool got = false;
  return getBlkFromSessionCacheImpl(pOperator, sessionId, pSession, ppRes, &got);
}


static int32_t getBlkFromSessionCache(struct SOperatorInfo* pOperator, int64_t sessionId, SGcSessionCtx* pSession, SSDataBlock** ppRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  bool locked = false;
  SGcDownstreamCtx* pCtx = &pGCache->pDownstreams[pSession->downstreamIdx];
  
  while (true) {
    bool got = false;
    code = getBlkFromSessionCacheImpl(pOperator, sessionId, pSession, ppRes, &got);
    if (TSDB_CODE_SUCCESS != code || got) {
      goto _return;
    }
    
    if ((atomic_load_64(&pCtx->fetchSessionId) == sessionId)
      || (-1 == atomic_val_compare_exchange_64(&pCtx->fetchSessionId, -1, sessionId))) {
      if (locked) {
        taosThreadMutexUnlock(&pSession->pGroupData->mutex);
        locked = false;
      }
      
      code = getCacheBlkFromDownstreamOperator(pOperator, pCtx, sessionId, pSession, ppRes);
      goto _return;
    }

    if (locked) {
      code = groupCacheSessionWait(pOperator, pCtx, sessionId, pSession, ppRes);
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
  pCache->pDirtyBlk = taosHashInit(10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == pCache->pDirtyBlk) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  taosHashSetFreeFp(pCache->pDirtyBlk,,,,,,,,);
  pCache->pReadBlk = taosHashInit(10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == pCache->pReadBlk) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE void initGroupCacheSessionCtx(SGcSessionCtx* pSession, SGcOperatorParam* pGcParam, SGroupCacheData* pGroup) {
  pSession->pParam = pGcParam;
  pSession->downstreamIdx = pGcParam->downstreamIdx;
  pSession->pGroupData = pGroup;
  pSession->lastBlkId = -1;
  pSession->nextOffset = 0;
}

static int32_t initGroupCacheSession(struct SOperatorInfo* pOperator, SOperatorParam* pParam, SGcSessionCtx** ppSession) {
  int32_t code = TSDB_CODE_SUCCESS;
  SGcSessionCtx ctx = {0};
  SGcOperatorParam* pGcParam = pParam->value;  
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  SGcDownstreamCtx* pCtx = &pGCache->pDownstreams[pParam->downstreamIdx];
  SHashObj* pGrpHash = pGCache->globalGrp ? pGCache->pGrpHash : pCtx->pGrpHash;

  SGroupCacheData* pGroup = taosHashAcquire(pGrpHash, &pGcParam->tbUid, sizeof(pGcParam->tbUid));
  if (NULL == pGroup) {
    code = addNewGroupData(pOperator, pParam, &pGroup, pGCache->batchFetch ? -1 : pGcParam->vgId, pGcParam->tbUid);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  initGroupCacheSessionCtx(&ctx, pGcParam, pGroup);

  code = taosHashPut(pCtx->pSessions, &pGcParam->sessionId, sizeof(pGcParam->sessionId), &ctx, sizeof(ctx));
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  *ppSession = taosHashGet(pCtx->pSessions, &pGcParam->sessionId, sizeof(pGcParam->sessionId));
  
  return TSDB_CODE_SUCCESS;
}

static int32_t getBlkFromGroupCache(struct SOperatorInfo* pOperator, SSDataBlock** ppRes, SOperatorParam* pParam) {
  int32_t code = TSDB_CODE_SUCCESS;
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  SGcOperatorParam* pGcParam = pParam->value;
  SGcDownstreamCtx* pCtx = &pGCache->pDownstreams[pParam->downstreamIdx];
  SGcSessionCtx* pSession = taosHashGet(pCtx->pSessions, &pGcParam->sessionId, sizeof(pGcParam->sessionId));
  if (NULL == pSession) {
    int32_t code = initGroupCacheSession(pOperator, pParam, &pSession);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  } else {
    SSDataBlock** ppBlock = taosHashGet(pGCache->blkCache.pReadBlk, &pGcParam->sessionId, sizeof(pGcParam->sessionId));
    if (ppBlock) {
      releaseBaseBlockToList(pCtx, *ppBlock);
      taosHashRemove(pGCache->blkCache.pReadBlk, &pGcParam->sessionId, sizeof(pGcParam->sessionId));
    }
  }
  
  code = getBlkFromSessionCache(pOperator, pGcParam->sessionId, pSession, ppRes);
  if (NULL == ppRes) {
    taosHashRemove(pCtx->pSessions, &pGcParam->sessionId, sizeof(pGcParam->sessionId));
  }

  return code;
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
  pInfo->pDownstreams = taosMemoryCalloc(pOperator->numOfDownstream, sizeof(*pInfo->pDownstreams));
  if (NULL == pInfo->pDownstreams) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
    SGcDownstreamCtx* pCtx = &pInfo->pDownstreams[i];
    pCtx->id = i;
    pCtx->fetchSessionId = -1;
    pCtx->lastBlkUid = 0;
    pCtx->pVgTbHash = tSimpleHashInit(10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
    if (NULL == pCtx->pVgTbHash) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    if (pInfo->batchFetch) {
      int32_t defaultVg = 0;
      SGcVgroupCtx vgCtx = {0};
      initGcVgroupCtx(&vgCtx, pCtx->id, defaultVg, NULL);      
      tSimpleHashPut(pCtx->pVgTbHash, &defaultVg, sizeof(defaultVg), &vgCtx, sizeof(vgCtx));
    }
    
    pCtx->pNewGrpList = taosArrayInit(10, sizeof(SGcNewGroupInfo));
    if (NULL == pCtx->pNewGrpList) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    if (!pInfo->globalGrp) {
      pCtx->pGrpHash = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
      if (pCtx->pGrpHash == NULL) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
    }

    pCtx->pSessions = taosHashInit(20, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
    if (pCtx->pSessions == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  
    pCtx->pFreeBlock = taosArrayInit(10, POINTER_BYTES);
    if (NULL == pCtx->pFreeBlock) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    
    pCtx->pWaitSessions = taosHashInit(20, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
    if (pCtx->pWaitSessions == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    snprintf(pCtx->baseFilename, sizeof(pCtx->baseFilename) - 1, "%s/gc_%d_%s_%d", tsTempDir, getpid(), GET_TASKID(pOperator->pTaskInfo), pCtx->id);
    pCtx->baseFilename[sizeof(pCtx->baseFilename) - 1] = 0;
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

  pInfo->maxCacheSize = 104857600;
  pInfo->grpByUid = pPhyciNode->grpByUid;
  pInfo->globalGrp = pPhyciNode->globalGrp;
  pInfo->batchFetch = pPhyciNode->batchFetch;
  
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


