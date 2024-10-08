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


static void removeGroupCacheFile(SGroupCacheFileInfo* pFileInfo) {
  if (pFileInfo->fd.fd) {
    taosCloseFile(&pFileInfo->fd.fd);
    pFileInfo->fd.fd = NULL;
  }
  pFileInfo->deleted = true;
}


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
  char* buf = taosMemoryMalloc(pGrpCacheOperator->downstreamNum * 32 + 100);
  if (NULL == buf) {
    return;
  }
  int32_t offset = sprintf(buf, "groupCache exec info, downstreamBlkNum:");
  for (int32_t i = 0; i < pGrpCacheOperator->downstreamNum; ++i) {
    offset += sprintf(buf + offset, " %" PRId64 , pGrpCacheOperator->execInfo.pDownstreamBlkNum[i]);
  }
  qDebug("%s", buf);
  taosMemoryFree(buf);
}

static void freeSGroupCacheFileInfo(void* p) {
  SGroupCacheFileInfo* pFileInfo = p;
  if (pFileInfo->deleted) {
    return;
  }

  removeGroupCacheFile(pFileInfo);
}

static void freeSGcFileCacheCtx(SGcFileCacheCtx* pFileCtx) {
  taosHashCleanup(pFileCtx->pCacheFile);
}

static void freeSGcDownstreamCtx(SGcDownstreamCtx* pCtx) {
  taosHashCleanup(pCtx->pGrpHash);

  taosHashCleanup(pCtx->pSessions);
  freeSGcFileCacheCtx(&pCtx->fileCtx);
}

static void destroyGroupCacheDownstreamCtx(SGroupCacheOperatorInfo* pGrpCacheOperator) {
  if (NULL == pGrpCacheOperator->pDownstreams) {
    return;
  }
  
  for (int32_t i = 0; i < pGrpCacheOperator->downstreamNum; ++i) {
    SGcDownstreamCtx* pCtx = &pGrpCacheOperator->pDownstreams[i];
    freeSGcDownstreamCtx(pCtx);
  }

  taosMemoryFree(pGrpCacheOperator->pDownstreams);
}


void blockDataDeepCleanup(SSDataBlock* pDataBlock) {
  size_t numOfCols = taosArrayGetSize(pDataBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* p = taosArrayGet(pDataBlock->pDataBlock, i);
    taosMemoryFreeClear(p->pData);
    if (IS_VAR_DATA_TYPE(p->info.type)) {
      taosMemoryFreeClear(p->varmeta.offset);
      p->varmeta.length = 0;
      p->varmeta.allocLen = 0;
    } else {
      taosMemoryFreeClear(p->nullbitmap);
    }
  }
  pDataBlock->info.capacity = 0;
  pDataBlock->info.rows = 0;
}



static void destroySGcBlkCacheInfo(SGcBlkCacheInfo* pBlkCache) {
  taosHashCleanup(pBlkCache->pDirtyBlk);
}

static void destroyGroupCacheOperator(void* param) {
  SGroupCacheOperatorInfo* pGrpCacheOperator = (SGroupCacheOperatorInfo*)param;

  logGroupCacheExecInfo(pGrpCacheOperator);
  
  taosMemoryFree(pGrpCacheOperator->groupColsInfo.pColsInfo);
  taosMemoryFree(pGrpCacheOperator->groupColsInfo.pBuf);

  destroyGroupCacheDownstreamCtx(pGrpCacheOperator);
  destroySGcBlkCacheInfo(&pGrpCacheOperator->blkCache);
  taosHashCleanup(pGrpCacheOperator->pGrpHash);

  taosMemoryFree(pGrpCacheOperator->execInfo.pDownstreamBlkNum);
  
  taosMemoryFreeClear(param);
}

static FORCE_INLINE int32_t initOpenCacheFile(SGroupCacheFileFd* pFileFd, char* filename) {
  TdFilePtr newFd = taosOpenFile(filename, TD_FILE_CREATE|TD_FILE_READ|TD_FILE_WRITE|TD_FILE_AUTO_DEL);
  //TdFilePtr newFd = taosOpenFile(filename, TD_FILE_CREATE|TD_FILE_READ|TD_FILE_WRITE);
  if (NULL == newFd) {
    return TAOS_SYSTEM_ERROR(errno);
  }
  pFileFd->fd = newFd;

  qTrace("file path %s created", filename);
  
  return TSDB_CODE_SUCCESS;
}

static int32_t acquireFdFromFileCtx(SGcFileCacheCtx* pFileCtx, int32_t fileId, SGroupCacheFileFd** ppFd, bool* pDeleted) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (NULL == pFileCtx->pCacheFile) {
    pFileCtx->pCacheFile = taosHashInit(10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
    if (NULL == pFileCtx->pCacheFile) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    taosHashSetFreeFp(pFileCtx->pCacheFile, freeSGroupCacheFileInfo);
  }
  
  SGroupCacheFileInfo* pTmp = taosHashGet(pFileCtx->pCacheFile, &fileId, sizeof(fileId));
  if (NULL == pTmp) {
    sprintf(&pFileCtx->baseFilename[pFileCtx->baseNameLen], "_%d", fileId);

    SGroupCacheFileInfo newFile = {0};
    taosHashPut(pFileCtx->pCacheFile, &fileId, sizeof(fileId), &newFile, sizeof(newFile));
    pTmp = taosHashGet(pFileCtx->pCacheFile, &fileId, sizeof(fileId));
    if (NULL == pTmp) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  if (pTmp->deleted) {
    *pDeleted = true;
    return TSDB_CODE_SUCCESS;
  }

  if (NULL == pTmp->fd.fd) {
    code = initOpenCacheFile(&pTmp->fd, pFileCtx->baseFilename);
    if (code) {
      return code;
    }
  }

  *ppFd = &pTmp->fd;
  
  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE void releaseFdToFileCtx(SGroupCacheFileFd* pFd) {
}

static int32_t saveBlocksToDisk(SGroupCacheOperatorInfo* pGCache, SGcDownstreamCtx* pCtx, SGcBlkBufInfo* pHead) {
  int32_t code = TSDB_CODE_SUCCESS;
  SGroupCacheFileFd *pFd = NULL;
  SGcFileCacheCtx* pFileCtx = NULL;
  SHashObj* pGrpHash = pGCache->globalGrp ? pGCache->pGrpHash : pCtx->pGrpHash;
  int64_t lastGroupId = 0;
  SGroupCacheData* pGroup = NULL;
  
  while (NULL != pHead) {
    pFileCtx = &pHead->pCtx->fileCtx;
    bool deleted = false;
    code = acquireFdFromFileCtx(pFileCtx, pHead->basic.fileId, &pFd, &deleted);
    if (code) {
      goto _return;
    }

    if (deleted) {
      qTrace("FileId:%d-%d-%d already be deleted, skip write", 
          pCtx->id, pGroup ? pGroup->vgId : GROUP_CACHE_DEFAULT_VGID, pHead->basic.fileId);
      
      int64_t blkId = pHead->basic.blkId;
      pHead = pHead->next;
      
      taosHashRemove(pGCache->blkCache.pDirtyBlk, &blkId, sizeof(blkId));
      continue;
    }
    
    int32_t ret = taosLSeekFile(pFd->fd, pHead->basic.offset, SEEK_SET);
    if (ret == -1) {
      releaseFdToFileCtx(pFd);
      code = TAOS_SYSTEM_ERROR(errno);
      goto _return;
    }
    
    ret = (int32_t)taosWriteFile(pFd->fd, pHead->pBuf, pHead->basic.bufSize);
    if (ret != pHead->basic.bufSize) {
      releaseFdToFileCtx(pFd);
      code = TAOS_SYSTEM_ERROR(errno);
      goto _return;
    }
    
    releaseFdToFileCtx(pFd);

    qTrace("FileId:%d-%d blk %" PRIu64 " in group %" PRIu64 " size %" PRIu64 " written to offset %" PRIu64, 
        pCtx->id, pHead->basic.fileId, pHead->basic.blkId, pHead->groupId, pHead->basic.bufSize, pHead->basic.offset);
    
    int64_t blkId = pHead->basic.blkId;
    pHead = pHead->next;

    taosHashRemove(pGCache->blkCache.pDirtyBlk, &blkId, sizeof(blkId));
  }

_return:

  atomic_val_compare_exchange_32(&pGCache->blkCache.writeDownstreamId, pCtx->id, -1);

  return code;
}

static int32_t addBlkToDirtyBufList(SGroupCacheOperatorInfo* pGCache, SGcDownstreamCtx* pCtx, SGcBlkCacheInfo* pCache, SGcBlkBufInfo* pBufInfo) {
  if (0 != taosHashPut(pCache->pDirtyBlk, &pBufInfo->basic.blkId, sizeof(pBufInfo->basic.blkId), pBufInfo, sizeof(*pBufInfo))) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pBufInfo = taosHashGet(pCache->pDirtyBlk, &pBufInfo->basic.blkId, sizeof(pBufInfo->basic.blkId));
  if (NULL == pBufInfo) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  int32_t code = TSDB_CODE_SUCCESS;
  SGcBlkBufInfo* pWriteHead = NULL;
  
  pCache->blkCacheSize += pBufInfo->basic.bufSize;
  qError("group cache total dirty block num:%d size:%" PRId64 , taosHashGetSize(pCache->pDirtyBlk), pCache->blkCacheSize);

  if (NULL == pCache->pDirtyHead) {
    pCache->pDirtyHead = pBufInfo;
  } else {
    pCache->pDirtyTail->next = pBufInfo;
  }
  pCache->pDirtyTail = pBufInfo;
    
  if (pGCache->maxCacheSize >= 0 && pCache->blkCacheSize > pGCache->maxCacheSize) {
    if (-1 == atomic_val_compare_exchange_32(&pCache->writeDownstreamId, -1, pCtx->id)) {
      pWriteHead = pCache->pDirtyHead;
      SGcBlkBufInfo* pTmp = pCache->pDirtyHead;
      while (NULL != pTmp) {
        pCache->blkCacheSize -= pTmp->basic.bufSize;
        if (pCache->blkCacheSize <= pGCache->maxCacheSize) {
          pCache->pDirtyHead = pTmp->next;
          pTmp->next = NULL;
          break;
        }
        pTmp = pTmp->next;
      }
    }
  }

  if (NULL != pWriteHead) {
    code = saveBlocksToDisk(pGCache, pCtx, pWriteHead);
  }

  return code;
}

static FORCE_INLINE void chkRemoveVgroupCurrFile(SGcFileCacheCtx* pFileCtx, int32_t downstreamIdx) {
  SGroupCacheFileInfo* pFileInfo = taosHashGet(pFileCtx->pCacheFile, &pFileCtx->fileId, sizeof(pFileCtx->fileId));
  if (NULL == pFileInfo) {
    return;
  }
  
  if (0 == pFileInfo->groupNum) {
    removeGroupCacheFile(pFileInfo);

#if 0  
    /* debug only */
    sprintf(&pFileCtx->baseFilename[pFileCtx->baseNameLen], "_%d", pFileCtx->fileId);
    taosRemoveFile(pFileCtx->baseFilename);
    /* debug only */
#endif

    qTrace("FileId:%d-%d removed", downstreamIdx, pFileCtx->fileId);
    //taosHashRemove(pFileCtx->pCacheFile, &pGroup->fileId, sizeof(pGroup->fileId));
  }
}

static FORCE_INLINE void groupCacheSwitchNewFile(SGcFileCacheCtx* pFileCtx, int32_t downstreamIdx, bool removeCheck) {
  if (pFileCtx->fileSize < GROUP_CACHE_DEFAULT_MAX_FILE_SIZE) {
    return;
  }

  if (removeCheck) {
    chkRemoveVgroupCurrFile(pFileCtx, downstreamIdx);
  }
      
  pFileCtx->fileId++;
  pFileCtx->fileSize = 0;
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

  SGcFileCacheCtx* pFileCtx = &pCtx->fileCtx;

  pBufInfo->next = NULL;
  pBufInfo->basic.blkId = atomic_add_fetch_64(&pGCache->currentBlkId, 1);
  pBufInfo->basic.fileId = pGCache->batchFetch ? pFileCtx->fileId : pGroup->fileId;
  pBufInfo->basic.bufSize = bufSize;
  pBufInfo->basic.offset = atomic_fetch_add_64(&pFileCtx->fileSize, bufSize);
  pBufInfo->pCtx = pCtx;
  pBufInfo->groupId = pBlock->info.id.groupId;

  if (pGCache->batchFetch) {    
    groupCacheSwitchNewFile(pFileCtx, pCtx->id, false);
  }

  int32_t code = addBlkToDirtyBufList(pGCache, pCtx, &pGCache->blkCache, pBufInfo);

  return code;
}

static int32_t buildGroupCacheResultBlock(SGroupCacheOperatorInfo* pGCache, int32_t downstreamIdx, void* pBuf, SSDataBlock** ppRes) {
  *ppRes = pGCache->pDownstreams[downstreamIdx].pBaseBlock;
  
  //TODO OPTIMIZE PERF
  return blockDataFromBuf(*ppRes, pBuf);
}

static int32_t readBlockFromDisk(SGroupCacheOperatorInfo* pGCache, SGroupCacheData* pGrp, SGcBlkBufBasic* pBasic, void** ppBuf) {
  SGroupCacheFileFd *pFileFd = NULL;
  SGcFileCacheCtx* pFileCtx = &pGrp->pDownstreamCtx->fileCtx;
  bool deleted = false;
  int32_t code = acquireFdFromFileCtx(pFileCtx, pBasic->fileId, &pFileFd, &deleted);
  if (code) {
    return code;
  }
  if (deleted) {
    qError("FileId:%d-%d already be deleted, skip read", pGrp->pDownstreamCtx->id, pBasic->fileId);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  
  int32_t ret = taosLSeekFile(pFileFd->fd, pBasic->offset, SEEK_SET);
  if (ret == -1) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _return;
  }

  *ppBuf = taosMemoryMalloc(pBasic->bufSize);
  if (NULL == *ppBuf) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _return;
  }
  
  ret = (int32_t)taosReadFile(pFileFd->fd, *ppBuf, pBasic->bufSize);
  if (ret != pBasic->bufSize) {
    taosMemoryFreeClear(*ppBuf);
    code = TAOS_SYSTEM_ERROR(errno);
    goto _return;
  }

  qTrace("FileId:%d-%d blk %" PRIu64 " size %" PRIu64 " read from offset %" PRIu64, 
      pGrp->pDownstreamCtx->id, pBasic->fileId, pBasic->blkId, pBasic->bufSize, pBasic->offset);

_return:

  releaseFdToFileCtx(pFileFd);
  return code;
}

static int32_t retrieveBlkFromBufCache(SGroupCacheOperatorInfo* pGCache, SGroupCacheData* pGrp, int64_t sessionId, SGcBlkBufBasic* pBasic, SSDataBlock** ppRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  SGcBlkCacheInfo* pCache = &pGCache->blkCache;
  void* pBuf = NULL;

  SGcBlkBufInfo* pBufInfo = taosHashAcquire(pCache->pDirtyBlk, &pBasic->blkId, sizeof(pBasic->blkId));
  if (NULL == pBufInfo) {
    code = readBlockFromDisk(pGCache, pGrp, pBasic, &pBuf);
    if (code) {
      return code;
    }
  } else {
    pBuf = pBufInfo->pBuf;
  }
  
  code = buildGroupCacheResultBlock(pGCache, pGrp->pDownstreamCtx->id, pBuf, ppRes);
  taosHashRelease(pCache->pDirtyBlk, pBufInfo);
  if (NULL == pBufInfo) {
    taosMemoryFree(pBuf);
  }
  
  if (code) {
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE int32_t getBlkFromDownstreamOperator(struct SOperatorInfo* pOperator, int32_t downstreamIdx, SOperatorParam* pParam, SSDataBlock** ppRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  SOperatorParam* pDownstreamParam = pParam->pChildren ? taosArrayGetP(pParam->pChildren, 0) : NULL;
  SSDataBlock* pBlock = NULL;
  SGroupCacheOperatorInfo* pGCache = pOperator->info;

  taosArrayDestroy(pParam->pChildren);
  pParam->pChildren = NULL;

  if (pDownstreamParam) {
    pBlock = pOperator->pDownstream[downstreamIdx]->fpSet.getNextExtFn(pOperator->pDownstream[downstreamIdx], pDownstreamParam);
  } else {
    pBlock = pOperator->pDownstream[downstreamIdx]->fpSet.getNextFn(pOperator->pDownstream[downstreamIdx]);
  }

  if (pBlock) {
    qError("%s blk retrieved from group %" PRIu64, GET_TASKID(pOperator->pTaskInfo), pBlock->info.id.groupId);
    
    pGCache->execInfo.pDownstreamBlkNum[downstreamIdx]++;
    if (NULL == pGCache->pDownstreams[downstreamIdx].pBaseBlock) {
      code = cloneEmptyBlockFromBlock(&pGCache->pDownstreams[downstreamIdx].pBaseBlock, pBlock);
      if (code) {
        return code;
      }
    }
  }

  *ppRes = pBlock;
  
  return code;
}

static FORCE_INLINE void handleGroupFetchDone(SGroupCacheData* pGroup) {
  atomic_store_8((int8_t*)&pGroup->fetchDone, true);
}

static int32_t addFileRefTableNum(SGcFileCacheCtx* pFileCtx, int32_t fileId, int32_t downstreamId) {
  if (NULL == pFileCtx->pCacheFile) {
    pFileCtx->pCacheFile = taosHashInit(10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
    if (NULL == pFileCtx->pCacheFile) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    taosHashSetFreeFp(pFileCtx->pCacheFile, freeSGroupCacheFileInfo);
  }
  
  SGroupCacheFileInfo* pTmp = taosHashGet(pFileCtx->pCacheFile, &fileId, sizeof(fileId));
  if (NULL == pTmp) {
    sprintf(&pFileCtx->baseFilename[pFileCtx->baseNameLen], "_%u", fileId);

    SGroupCacheFileInfo newFile = {0};
    newFile.groupNum = 1;
    taosHashPut(pFileCtx->pCacheFile, &fileId, sizeof(fileId), &newFile, sizeof(newFile));
    pTmp = taosHashGet(pFileCtx->pCacheFile, &fileId, sizeof(fileId));
    if (NULL == pTmp) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  } else {
    pTmp->groupNum++;
  }

  qTrace("FileId:%d-%d add groupNum to %u", downstreamId, fileId, pTmp->groupNum);

  return TSDB_CODE_SUCCESS;
}

static int32_t handleGroupFirstBlock(SGcDownstreamCtx* pCtx, SGroupCacheData* pGroup, uint64_t uid) {
  pCtx->lastBlkUid = uid;

  groupCacheSwitchNewFile(&pCtx->fileCtx, pGroup->pDownstreamCtx->id, true);

  pGroup->fileId = pCtx->fileCtx.fileId;
  pGroup->startOffset = pCtx->fileCtx.fileSize;

  qTrace("FileId:%d-%d add groupNum for group %" PRIu64, pGroup->pDownstreamCtx->id, pCtx->fileCtx.fileId, uid);

  if (pGroup->needCache) {
    return addFileRefTableNum(&pCtx->fileCtx, pCtx->fileCtx.fileId, pGroup->pDownstreamCtx->id);
  }

  return TSDB_CODE_SUCCESS;
}


static FORCE_INLINE void initNewGroupData(SGcDownstreamCtx* pCtx, SGroupCacheData* pGroup, bool needCache) {
  pGroup->pDownstreamCtx = pCtx;
  pGroup->fileId = -1;
  pGroup->blkList.pList = taosArrayInit(10, sizeof(SGcBlkBufBasic));
  pGroup->startOffset = -1;
  pGroup->needCache = needCache;
}

static int32_t addNewGroupData(struct SOperatorInfo* pOperator, SOperatorParam* pParam, SGroupCacheData** ppGrp, int32_t vgId, int64_t uid) {
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  SGcDownstreamCtx* pCtx = &pGCache->pDownstreams[pParam->downstreamIdx];
  SGcOperatorParam* pGcParam = pParam->value;  
  SHashObj* pGrpHash = pGCache->globalGrp ? pGCache->pGrpHash : pCtx->pGrpHash;
  SGroupCacheData grpData = {0};
  
  if (0 != taosHashPut(pGrpHash, &uid, sizeof(uid), &grpData, sizeof(grpData))) {
    return terrno;
  }

  *ppGrp = taosHashGet(pGrpHash, &uid, sizeof(uid));
  initNewGroupData(pCtx, *ppGrp, pGcParam->needCache);
  if (NULL == *ppGrp) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  initNewGroupData(pCtx, *ppGrp, pGcParam->needCache);

  qError("new group %" PRIu64 " initialized, downstreamIdx:%d, vgId:%d, needCache:%d", uid, pParam->downstreamIdx, vgId, pGcParam->needCache);

  return TSDB_CODE_SUCCESS;
}

static int32_t addBlkToGroupCache(bool batchFetch, SGroupCacheData* pGroup, SGcBlkBufInfo* pNewBlk, int64_t* pIdx) {
  taosArrayPush(pGroup->blkList.pList, &pNewBlk->basic);
  *pIdx = taosArrayGetSize(pGroup->blkList.pList) - 1;

  qError("block added to group cache, total block num:%" PRId64, *pIdx + 1);
  
  return TSDB_CODE_SUCCESS;
}

static int32_t handleGroupCacheRetrievedBlk(struct SOperatorInfo* pOperator, SSDataBlock* pBlock, SGcSessionCtx* pSession, bool* continueFetch) {
  int32_t code = TSDB_CODE_SUCCESS;
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  SGcDownstreamCtx* pCtx = &pGCache->pDownstreams[pSession->downstreamIdx];
  SHashObj* pGrpHash = pGCache->globalGrp ? pGCache->pGrpHash : pCtx->pGrpHash;
  int64_t newBlkIdx = 0;

  SGroupCacheData* pGroup = taosHashGet(pGrpHash, &pBlock->info.id.groupId, sizeof(pBlock->info.id.groupId));
  if (NULL == pGroup) {
    if (pGCache->batchFetch) {
      SGcOperatorParam fakeGcParam = {0};
      SOperatorParam fakeParam = {0};
      fakeGcParam.needCache = true;
      fakeParam.downstreamIdx = pSession->downstreamIdx;
      fakeParam.value = &fakeGcParam;
      code = addNewGroupData(pOperator, &fakeParam, &pGroup, GROUP_CACHE_DEFAULT_VGID, pBlock->info.id.groupId);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
    } else {
      qError("group %" PRIu64 " not found in group hash", pBlock->info.id.groupId);
      return TSDB_CODE_INVALID_PARA;
    }
  }

  if (!pGCache->batchFetch && pCtx->lastBlkUid != pBlock->info.id.groupId) {
    code = handleGroupFirstBlock(pCtx, pGroup, pBlock->info.id.groupId);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  if (pGroup->needCache) {
    SGcBlkBufInfo newBlkBuf;    
    code = addBlkToBufCache(pOperator, pBlock, pCtx, pGroup, &newBlkBuf);
    if (code) {
      return code;
    }

    code = addBlkToGroupCache(pGCache->batchFetch, pGroup, &newBlkBuf, &newBlkIdx);
    if (code) {
      return code;
    }
  }

  if (pGroup == pSession->pGroupData) {
    if (pGroup->needCache) {
      pSession->lastBlkId = newBlkIdx;
    }
    
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
    while (NULL != (pGroup = taosHashIterate(pGrpHash, pGroup))) {
      handleGroupFetchDone(pGroup);
    }
  } else {
    handleGroupFetchDone(pSession->pGroupData);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t getCacheBlkFromDownstreamOperator(struct SOperatorInfo* pOperator, SGcDownstreamCtx* pCtx, int64_t sessionId, SGcSessionCtx* pSession, SSDataBlock** ppRes) {
  bool continueFetch = true;
  int32_t code = TSDB_CODE_SUCCESS;
  SGroupCacheOperatorInfo* pGCache = pOperator->info;

  while (continueFetch && TSDB_CODE_SUCCESS == code) {
    int32_t code = getBlkFromDownstreamOperator(pOperator, pSession->downstreamIdx, pSession->pParam, ppRes);
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

static int32_t getBlkFromSessionCacheImpl(struct SOperatorInfo* pOperator, int64_t sessionId, SGcSessionCtx* pSession, SSDataBlock** ppRes, bool* got) {
  int32_t code = TSDB_CODE_SUCCESS;
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  *got = true;

  if (pSession->pGroupData->needCache) {
    SGcBlkList* pBlkList = &pSession->pGroupData->blkList;
    int64_t blkNum = taosArrayGetSize(pBlkList->pList);
    if (pSession->lastBlkId < 0) {
      if (blkNum > 0) {
        SGcBlkBufBasic* pBasic = taosArrayGet(pBlkList->pList, 0);
        code = retrieveBlkFromBufCache(pGCache, pSession->pGroupData, sessionId, pBasic, ppRes);
        pSession->lastBlkId = 0;
        return code;
      }
    } else if ((pSession->lastBlkId + 1) < blkNum) {
      SGcBlkBufBasic* pBasic = taosArrayGet(pBlkList->pList, pSession->lastBlkId + 1);
      code = retrieveBlkFromBufCache(pGCache, pSession->pGroupData, sessionId, pBasic, ppRes);
      pSession->lastBlkId++;
      return code;
    }
  }

  if (pSession->pGroupData->fetchDone) {
    *ppRes = NULL;
    return code;
  }

  *got = false;
  return code;
}


static int32_t getBlkFromSessionCache(struct SOperatorInfo* pOperator, int64_t sessionId, SGcSessionCtx* pSession, SSDataBlock** ppRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  SGcDownstreamCtx* pCtx = &pGCache->pDownstreams[pSession->downstreamIdx];
  bool got = false;
  code = getBlkFromSessionCacheImpl(pOperator, sessionId, pSession, ppRes, &got);
  if (TSDB_CODE_SUCCESS != code || got) {
    return code;
  }
   
  return getCacheBlkFromDownstreamOperator(pOperator, pCtx, sessionId, pSession, ppRes);
}

void freeGcBlkBufInfo(void* ptr) {
  SGcBlkBufInfo* pBlk = (SGcBlkBufInfo*)ptr;
  taosMemoryFree(pBlk->pBuf);
}

static int32_t initGroupCacheBlockCache(SGroupCacheOperatorInfo* pInfo) {
  SGcBlkCacheInfo* pCache = &pInfo->blkCache;
  pCache->pDirtyBlk = taosHashInit(10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == pCache->pDirtyBlk) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  taosHashSetFreeFp(pCache->pDirtyBlk, freeGcBlkBufInfo);

  pCache->writeDownstreamId = -1;

  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE void initGroupCacheSessionCtx(SGcSessionCtx* pSession, SOperatorParam* pParam, int32_t downstreamIdx, SGroupCacheData* pGroup) {
  pSession->pParam = pParam;
  pSession->downstreamIdx = downstreamIdx;
  pSession->pGroupData = pGroup;
  pSession->lastBlkId = -1;
}

static int32_t initGroupCacheSession(struct SOperatorInfo* pOperator, SOperatorParam* pParam, SGcSessionCtx** ppSession) {
  int32_t code = TSDB_CODE_SUCCESS;
  SGcSessionCtx ctx = {0};
  SGcOperatorParam* pGcParam = pParam->value;  
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  SGcDownstreamCtx* pCtx = &pGCache->pDownstreams[pParam->downstreamIdx];
  SHashObj* pGrpHash = pGCache->globalGrp ? pGCache->pGrpHash : pCtx->pGrpHash;

  SGroupCacheData* pGroup = taosHashGet(pGrpHash, &pGcParam->tbUid, sizeof(pGcParam->tbUid));
  if (NULL == pGroup) {
    code = addNewGroupData(pOperator, pParam, &pGroup, pGCache->batchFetch ? GROUP_CACHE_DEFAULT_VGID : pGcParam->vgId, pGcParam->tbUid);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  initGroupCacheSessionCtx(&ctx, pParam, pGcParam->downstreamIdx, pGroup);

  code = taosHashPut(pCtx->pSessions, &pGcParam->sessionId, sizeof(pGcParam->sessionId), &ctx, sizeof(ctx));
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  *ppSession = taosHashGet(pCtx->pSessions, &pGcParam->sessionId, sizeof(pGcParam->sessionId));
  
  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE int32_t removeGroupCache(SHashObj* pGrpHash, uint64_t uid, int32_t downstreamIdx) {
  qTrace("try to remove group %" PRIu64, uid);
  if (taosHashRemove(pGrpHash, &uid, sizeof(uid))) {
    qError("failed to remove group %" PRIu64 " downstreamIdx %d", uid, downstreamIdx);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  return TSDB_CODE_SUCCESS;
}

static void handleSessionFetchDone(SGroupCacheOperatorInfo* pGCache, SGcDownstreamCtx* pCtx, SGcSessionCtx* pSession, int64_t sessionId, uint64_t uid) {
  if (pSession->pGroupData && !pSession->pGroupData->needCache) {
    SHashObj* pGrpHash = pGCache->globalGrp ? pGCache->pGrpHash : pCtx->pGrpHash;
    (void)removeGroupCache(pGrpHash, uid, pCtx->id);
  }
  
  taosHashRemove(pCtx->pSessions, &sessionId, sizeof(sessionId));
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
  } else if (pSession->pGroupData->needCache) {
    blockDataDeepCleanup(pGCache->pDownstreams[pSession->downstreamIdx].pBaseBlock);
  }
  
  code = getBlkFromSessionCache(pOperator, pGcParam->sessionId, pSession, ppRes);
  if (NULL == *ppRes) {
    qError("session %" PRId64 " in downstream %d total got %" PRId64 " rows", pGcParam->sessionId, pCtx->id, pSession->resRows);
    handleSessionFetchDone(pGCache, pCtx, pSession, pGcParam->sessionId, pGcParam->tbUid);
  } else {
    pSession->resRows += (*ppRes)->info.rows;
    qError("session %" PRId64 " in downstream %d got %" PRId64 " rows in one block", pGcParam->sessionId, pCtx->id, (*ppRes)->info.rows);
  }

  return code;
}

static int32_t initGroupCacheExecInfo(SOperatorInfo*        pOperator) {
  SGroupCacheOperatorInfo* pInfo = pOperator->info;
  pInfo->execInfo.pDownstreamBlkNum = taosMemoryCalloc(pOperator->numOfDownstream, sizeof(int64_t));
  if (NULL == pInfo->execInfo.pDownstreamBlkNum) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return TSDB_CODE_SUCCESS;
}

static void freeRemoveGroupCacheData(void* p) {
  SGroupCacheData* pGroup = p;
  if (pGroup->needCache) {
    SGcFileCacheCtx* pFileCtx = &pGroup->pDownstreamCtx->fileCtx;
    if (pGroup->fileId >= 0) {
      SGroupCacheFileInfo* pFileInfo = taosHashGet(pFileCtx->pCacheFile, &pGroup->fileId, sizeof(pGroup->fileId));
      uint32_t remainNum = atomic_sub_fetch_32(&pFileInfo->groupNum, 1);

      qTrace("FileId:%d-%d sub group num to %u", pGroup->pDownstreamCtx->id, pFileCtx->fileId, remainNum);

      if (0 == remainNum && pGroup->fileId != pFileCtx->fileId) {
        removeGroupCacheFile(pFileInfo);

#if 0
        /* debug only */
        sprintf(&pFileCtx->baseFilename[pFileCtx->baseNameLen], "_%d", pGroup->fileId);
        taosRemoveFile(pFileCtx->baseFilename);
        /* debug only */
#endif

        qTrace("FileId:%d-%d removed", pGroup->pDownstreamCtx->id, pFileCtx->fileId);
        //taosHashRemove(pFileCtx->pCacheFile, &pGroup->fileId, sizeof(pGroup->fileId));
      }
    }
  }

  taosArrayDestroy(pGroup->blkList.pList);

  qTrace("group removed");
}



static int32_t initGroupCacheDownstreamCtx(SOperatorInfo*          pOperator) {
  SGroupCacheOperatorInfo* pInfo = pOperator->info;
  pInfo->pDownstreams = taosMemoryCalloc(pOperator->numOfDownstream, sizeof(*pInfo->pDownstreams));
  if (NULL == pInfo->pDownstreams) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pInfo->downstreamNum = pOperator->numOfDownstream;

  for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
    SGcDownstreamCtx* pCtx = &pInfo->pDownstreams[i];
    pCtx->id = i;
    pCtx->lastBlkUid = 0;
    
    if (!pInfo->globalGrp) {
      pCtx->pGrpHash = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
      if (pCtx->pGrpHash == NULL) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      taosHashSetFreeFp(pCtx->pGrpHash, freeRemoveGroupCacheData);      
    }

    pCtx->pSessions = taosHashInit(20, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
    if (pCtx->pSessions == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    
    snprintf(pCtx->fileCtx.baseFilename, sizeof(pCtx->fileCtx.baseFilename) - 1, "%s/gc_%d_%" PRIx64 "_%" PRIu64 "_%d", 
      tsTempDir, getpid(), pOperator->pTaskInfo->id.queryId, pOperator->pTaskInfo->id.taskId, pCtx->id);
    pCtx->fileCtx.baseFilename[sizeof(pCtx->fileCtx.baseFilename) - 1] = 0;
    pCtx->fileCtx.baseNameLen = strlen(pCtx->fileCtx.baseFilename);
  }

  return TSDB_CODE_SUCCESS;
}

static SSDataBlock* groupCacheGetNext(struct SOperatorInfo* pOperator, SOperatorParam* pParam) {
  SSDataBlock* pBlock = NULL;
  int64_t st = 0;

  if (pOperator->cost.openCost == 0) {
    st = taosGetTimestampUs();
  }

  int32_t code = getBlkFromGroupCache(pOperator, &pBlock, pParam);
  if (TSDB_CODE_SUCCESS != code) {
    pOperator->pTaskInfo->code = code;
    T_LONG_JMP(pOperator->pTaskInfo->env, pOperator->pTaskInfo->code);
  }

  if (pOperator->cost.openCost == 0) {
    pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;
  }

  return pBlock;
}

static int32_t groupCacheTableCacheEnd(SOperatorInfo* pOperator, SOperatorParam* pParam) {
  SGcNotifyOperatorParam* pGcParam = pParam->value;
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  SGcDownstreamCtx* pCtx = &pGCache->pDownstreams[pGcParam->downstreamIdx];
  SHashObj* pGrpHash = pGCache->globalGrp ? pGCache->pGrpHash : pCtx->pGrpHash;

  return removeGroupCache(pGrpHash, pGcParam->tbUid, pGcParam->downstreamIdx);
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

  pInfo->maxCacheSize = 0;
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
    taosHashSetFreeFp(pInfo->pGrpHash, freeRemoveGroupCacheData);
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

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, NULL, NULL, destroyGroupCacheOperator, optrDefaultBufFn, NULL, groupCacheGetNext, groupCacheTableCacheEnd);

  qTrace("new group cache operator, maxCacheSize:%" PRId64 ", globalGrp:%d, batchFetch:%d", pInfo->maxCacheSize, pInfo->globalGrp, pInfo->batchFetch);

  return pOperator;

_error:
  if (pInfo != NULL) {
    destroyGroupCacheOperator(pInfo);
  }

  taosMemoryFree(pOperator);
  pTaskInfo->code = code;
  return NULL;
}


