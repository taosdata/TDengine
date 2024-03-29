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
    taosThreadMutexDestroy(&pFileInfo->fd.mutex);
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

static void freeSGcSessionCtx(void* p) {
  SGcSessionCtx* pSession = p;
  if (pSession->semInit) {
    tsem_destroy(&pSession->waitSem);
  }
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

static void freeSGcVgroupCtx(void* p) {
  SGcVgroupCtx* pVgCtx = p;
  taosArrayDestroy(pVgCtx->pTbList);
  freeSGcFileCacheCtx(&pVgCtx->fileCtx);
}

static void freeGcBlockInList(void* p) {
  SSDataBlock** ppBlock = p;
  if (*ppBlock) {
    taosArrayDestroy((*ppBlock)->pDataBlock);
    taosMemoryFree(*ppBlock);
  }
}

static void freeSGcDownstreamCtx(SGcDownstreamCtx* pCtx) {
  taosArrayDestroy(pCtx->pNewGrpList);
  taosHashCleanup(pCtx->pGrpHash);
  tSimpleHashCleanup(pCtx->pVgTbHash);

  taosArrayDestroyEx(pCtx->pFreeBlock, freeGcBlockInList);
  taosHashCleanup(pCtx->pSessions);
  taosHashCleanup(pCtx->pWaitSessions);
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

  void* p = NULL;
  while (NULL != (p = taosHashIterate(pBlkCache->pReadBlk, p))) {
    blockDataDeepCleanup(*(SSDataBlock**)p);
    freeGcBlockInList(p);
  }

  taosHashCleanup(pBlkCache->pReadBlk);
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
  taosThreadMutexInit(&pFileFd->mutex, NULL);

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

  taosThreadMutexLock(&pTmp->fd.mutex);
  *ppFd = &pTmp->fd;
  
  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE void releaseFdToFileCtx(SGroupCacheFileFd* pFd) {
  taosThreadMutexUnlock(&pFd->mutex);
}

static int32_t saveBlocksToDisk(SGroupCacheOperatorInfo* pGCache, SGcDownstreamCtx* pCtx, SGcBlkBufInfo* pHead) {
  int32_t code = TSDB_CODE_SUCCESS;
  SGroupCacheFileFd *pFd = NULL;
  SGcFileCacheCtx* pFileCtx = NULL;
  SHashObj* pGrpHash = pGCache->globalGrp ? pGCache->pGrpHash : pCtx->pGrpHash;
  int64_t lastGroupId = 0;
  SGroupCacheData* pGroup = NULL;
  
  while (NULL != pHead) {
    if (pGCache->batchFetch) {
      pFileCtx = &pHead->pCtx->fileCtx;
    } else {
      if (pHead->groupId != lastGroupId) {
        if (NULL != pGroup) {
          taosHashRelease(pGrpHash, pGroup);
        }
        pGroup = taosHashAcquire(pGrpHash, &pHead->groupId, sizeof(pHead->groupId));      
        lastGroupId = pHead->groupId;
      }
    
      if (NULL == pGroup) {
        qTrace("group %" PRIu64 " in downstream %d may already be deleted, skip write", pHead->groupId, pHead->pCtx->id);

        int64_t blkId = pHead->basic.blkId;
        pHead = pHead->next;
        taosHashRemove(pGCache->blkCache.pDirtyBlk, &blkId, sizeof(blkId));
        continue;
      }
      
      pFileCtx = &pGroup->pVgCtx->fileCtx;
    }

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

    qTrace("FileId:%d-%d-%d blk %" PRIu64 " in group %" PRIu64 " size %" PRIu64 " written to offset %" PRIu64, 
        pCtx->id, pGroup ? pGroup->vgId : GROUP_CACHE_DEFAULT_VGID, pHead->basic.fileId, pHead->basic.blkId, pHead->groupId, pHead->basic.bufSize, pHead->basic.offset);
    
    int64_t blkId = pHead->basic.blkId;
    pHead = pHead->next;

    taosHashRemove(pGCache->blkCache.pDirtyBlk, &blkId, sizeof(blkId));
  }

_return:

  if (NULL != pGroup) {
    taosHashRelease(pGrpHash, pGroup);
  }

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
  
  taosWLockLatch(&pCache->dirtyLock);
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
  taosWUnLockLatch(&pCache->dirtyLock);

  if (NULL != pWriteHead) {
    code = saveBlocksToDisk(pGCache, pCtx, pWriteHead);
  }

  return code;
}

static FORCE_INLINE void chkRemoveVgroupCurrFile(SGcFileCacheCtx* pFileCtx, int32_t downstreamIdx, int32_t vgId) {
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

    qTrace("FileId:%d-%d-%d removed", downstreamIdx, vgId, pFileCtx->fileId);
    //taosHashRemove(pFileCtx->pCacheFile, &pGroup->fileId, sizeof(pGroup->fileId));
  }
}

static FORCE_INLINE void groupCacheSwitchNewFile(SGcFileCacheCtx* pFileCtx, int32_t downstreamIdx, int32_t vgId, bool removeCheck) {
  if (pFileCtx->fileSize < GROUP_CACHE_DEFAULT_MAX_FILE_SIZE) {
    return;
  }

  if (removeCheck) {
    chkRemoveVgroupCurrFile(pFileCtx, downstreamIdx, vgId);
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

  SGcFileCacheCtx* pFileCtx = pGCache->batchFetch ? &pCtx->fileCtx : &pGroup->pVgCtx->fileCtx;

  pBufInfo->next = NULL;
  pBufInfo->basic.blkId = atomic_add_fetch_64(&pGCache->currentBlkId, 1);
  pBufInfo->basic.fileId = pGCache->batchFetch ? pFileCtx->fileId : pGroup->fileId;
  pBufInfo->basic.bufSize = bufSize;
  pBufInfo->basic.offset = atomic_fetch_add_64(&pFileCtx->fileSize, bufSize);
  pBufInfo->pCtx = pCtx;
  pBufInfo->groupId = pBlock->info.id.groupId;

  if (pGCache->batchFetch) {    
    groupCacheSwitchNewFile(pFileCtx, pCtx->id, pGroup->vgId, false);
  }

  int32_t code = addBlkToDirtyBufList(pGCache, pCtx, &pGCache->blkCache, pBufInfo);

  return code;
}

void blockDataDeepClear(SSDataBlock* pDataBlock) {
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
  blockDataDeepClear(*ppDst);
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

static int32_t readBlockFromDisk(SGroupCacheOperatorInfo* pGCache, SGroupCacheData* pGrp, SGcBlkBufBasic* pBasic, void** ppBuf) {
  SGroupCacheFileFd *pFileFd = NULL;
  SGcFileCacheCtx* pFileCtx = pGCache->batchFetch ? &pGCache->pDownstreams[pGrp->downstreamIdx].fileCtx : &pGrp->pVgCtx->fileCtx;
  bool deleted = false;
  int32_t code = acquireFdFromFileCtx(pFileCtx, pBasic->fileId, &pFileFd, &deleted);
  if (code) {
    return code;
  }
  if (deleted) {
    qError("FileId:%d-%d-%d already be deleted, skip read", pGrp->downstreamIdx, pGrp->vgId, pBasic->fileId);
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

  qTrace("FileId:%d-%d-%d blk %" PRIu64 " size %" PRIu64 " read from offset %" PRIu64, 
      pGrp->downstreamIdx, pGrp->vgId, pBasic->fileId, pBasic->blkId, pBasic->bufSize, pBasic->offset);

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
  
  code = buildGroupCacheResultBlock(pGCache, pGrp->downstreamIdx, pBuf, ppRes);
  taosHashRelease(pCache->pDirtyBlk, pBufInfo);
  if (NULL == pBufInfo) {
    taosMemoryFree(pBuf);
  }
  
  if (code) {
    return code;
  }

  taosHashPut(pCache->pReadBlk, &sessionId, sizeof(sessionId), ppRes, POINTER_BYTES);
  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE void initGcVgroupCtx(SOperatorInfo* pOperator, SGcVgroupCtx* pVgCtx, int32_t downstreamId, int32_t vgId, SArray* pTbList) {
  pVgCtx->pTbList = pTbList;
  pVgCtx->id = vgId;
  snprintf(pVgCtx->fileCtx.baseFilename, sizeof(pVgCtx->fileCtx.baseFilename) - 1, "%s/gc_%d_%" PRIx64 "_%" PRIu64 "_%d_%d", 
     tsTempDir, getpid(), pOperator->pTaskInfo->id.queryId, pOperator->pTaskInfo->id.taskId, downstreamId, vgId);
  pVgCtx->fileCtx.baseFilename[sizeof(pVgCtx->fileCtx.baseFilename) - 1] = 0;

  pVgCtx->fileCtx.baseNameLen = strlen(pVgCtx->fileCtx.baseFilename);
}

static int32_t addNewGroupToVgHash(SOperatorInfo* pOperator, SSHashObj* pHash, SGcNewGroupInfo* pNew) {
  SGcVgroupCtx* pVgCtx = pNew->pGroup->pVgCtx;
  if (NULL == pVgCtx) {
    SArray* pList = taosArrayInit(10, sizeof(*pNew));
    if (NULL == pList) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    taosArrayPush(pList, pNew);
    SGcVgroupCtx vgCtx = {0};
    initGcVgroupCtx(pOperator, &vgCtx, pNew->pGroup->downstreamIdx, pNew->vgId, pList);
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
      code = addNewGroupToVgHash(pOperator, pCtx->pVgTbHash, pNew);
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
    qError("%s blk retrieved from group %" PRIu64, GET_TASKID(pOperator->pTaskInfo), pBlock->info.id.groupId);
    
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

static int32_t addFileRefTableNum(SGcFileCacheCtx* pFileCtx, int32_t fileId, int32_t downstreamId, int32_t vgId) {
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

  qTrace("FileId:%d-%d-%d add groupNum to %u", downstreamId, vgId, fileId, pTmp->groupNum);

  return TSDB_CODE_SUCCESS;
}

static int32_t handleVgroupTableFetchDone(SGcDownstreamCtx* pCtx, SGroupCacheData* pGroup, uint64_t uid) {
  if (pCtx->lastBlkUid == uid || pGroup->pVgCtx->lastBlkUid == uid) {
    return TSDB_CODE_SUCCESS;
  }
  
  pCtx->lastBlkUid = uid;
  pGroup->pVgCtx->lastBlkUid = uid;
  
  int32_t i = 0;
  while (true) {
    SGcNewGroupInfo* pNew = taosArrayGet(pGroup->pVgCtx->pTbList, i++);
    if (NULL == pNew || pNew->uid == uid) {
      break;
    }
    handleGroupFetchDone(pNew->pGroup);
  }

  groupCacheSwitchNewFile(&pGroup->pVgCtx->fileCtx, pGroup->downstreamIdx, pGroup->vgId, true);

  pGroup->fileId = pGroup->pVgCtx->fileCtx.fileId;
  pGroup->startOffset = pGroup->pVgCtx->fileCtx.fileSize;

  qTrace("FileId:%d-%d-%d add groupNum for group %" PRIu64, pGroup->downstreamIdx, pGroup->vgId, pGroup->pVgCtx->fileCtx.fileId, uid);

  if (pGroup->needCache) {
    return addFileRefTableNum(&pGroup->pVgCtx->fileCtx, pGroup->pVgCtx->fileCtx.fileId, pGroup->downstreamIdx, pGroup->vgId);
  }

  return TSDB_CODE_SUCCESS;
}


static FORCE_INLINE void initNewGroupData(SGcDownstreamCtx* pCtx, SGroupCacheData* pGroup, int32_t downstreamIdx, int32_t vgId, bool batchFetch, bool needCache) {
  taosThreadMutexInit(&pGroup->mutex, NULL);
  pGroup->downstreamIdx = downstreamIdx;
  pGroup->vgId = vgId;
  pGroup->fileId = -1;
  pGroup->blkList.pList = taosArrayInit(10, sizeof(SGcBlkBufBasic));
  pGroup->startOffset = -1;
  pGroup->needCache = needCache;
  pGroup->pVgCtx = tSimpleHashGet(pCtx->pVgTbHash, &pGroup->vgId, sizeof(pGroup->vgId));
}

static int32_t addNewGroupData(struct SOperatorInfo* pOperator, SOperatorParam* pParam, SGroupCacheData** ppGrp, int32_t vgId, int64_t uid) {
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  SGcDownstreamCtx* pCtx = &pGCache->pDownstreams[pParam->downstreamIdx];
  SGcOperatorParam* pGcParam = pParam->value;  
  SHashObj* pGrpHash = pGCache->globalGrp ? pGCache->pGrpHash : pCtx->pGrpHash;
  SGroupCacheData grpData = {0};
  
  while (true) {
    if (0 != taosHashPut(pGrpHash, &uid, sizeof(uid), &grpData, sizeof(grpData))) {
      if (terrno == TSDB_CODE_DUP_KEY) {
        *ppGrp = taosHashGet(pGrpHash, &uid, sizeof(uid));
        if (*ppGrp) {
          break;
        }
      } else {
        return terrno;
      }
    }

    break;
  }

  *ppGrp = taosHashGet(pGrpHash, &uid, sizeof(uid));
  if (NULL == *ppGrp) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  initNewGroupData(pCtx, *ppGrp, pParam->downstreamIdx, vgId, pGCache->batchFetch, pGcParam->needCache);

  qError("new group %" PRIu64 " initialized, downstreamIdx:%d, vgId:%d, needCache:%d", uid, pParam->downstreamIdx, vgId, pGcParam->needCache);

  if (pParam->pChildren) {
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
    
    taosArrayDestroy(pParam->pChildren);
    pParam->pChildren = NULL;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t addBlkToGroupCache(bool batchFetch, SGroupCacheData* pGroup, SGcBlkBufInfo* pNewBlk, int64_t* pIdx) {
  taosWLockLatch(&pGroup->blkList.lock);
  taosArrayPush(pGroup->blkList.pList, &pNewBlk->basic);
  *pIdx = taosArrayGetSize(pGroup->blkList.pList) - 1;
  taosWUnLockLatch(&pGroup->blkList.lock);

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

  if (!pGCache->batchFetch) {
    code = handleVgroupTableFetchDone(pCtx, pGroup, pBlock->info.id.groupId);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  if (pGroup->needCache) {
    qError("add block to group cache");
    
    SGcBlkBufInfo newBlkBuf;    
    code = addBlkToBufCache(pOperator, pBlock, pCtx, pGroup, &newBlkBuf);
    if (code) {
      return code;
    }

    code = addBlkToGroupCache(pGCache->batchFetch, pGroup, &newBlkBuf, &newBlkIdx);
    if (code) {
      return code;
    }
  } else {
    qError("no need to add block to group cache");
    
    pGroup->pBlock = pBlock;
  }

  notifyWaitingSessions(pGroup->waitQueue);
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
    int32_t uidNum = 0;
    SGcVgroupCtx* pVgCtx = NULL;
    int32_t iter = 0;
    while (NULL != (pVgCtx = tSimpleHashIterate(pCtx->pVgTbHash, pVgCtx, &iter))) {
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

  if (pSession->pGroupData->needCache) {
    SGcBlkList* pBlkList = &pSession->pGroupData->blkList;
    taosRLockLatch(&pBlkList->lock);
    int64_t blkNum = taosArrayGetSize(pBlkList->pList);
    if (pSession->lastBlkId < 0) {
      if (blkNum > 0) {
        SGcBlkBufBasic* pBasic = taosArrayGet(pBlkList->pList, 0);
        taosRUnLockLatch(&pBlkList->lock);
        code = retrieveBlkFromBufCache(pGCache, pSession->pGroupData, sessionId, pBasic, ppRes);
        pSession->lastBlkId = 0;
        return code;
      }
    } else if ((pSession->lastBlkId + 1) < blkNum) {
      SGcBlkBufBasic* pBasic = taosArrayGet(pBlkList->pList, pSession->lastBlkId + 1);
      taosRUnLockLatch(&pBlkList->lock);
      code = retrieveBlkFromBufCache(pGCache, pSession->pGroupData, sessionId, pBasic, ppRes);
      pSession->lastBlkId++;
      return code;
    }
    taosRUnLockLatch(&pBlkList->lock);
  } else if (pSession->pGroupData->pBlock) {
    *ppRes = pSession->pGroupData->pBlock;
    pSession->pGroupData->pBlock = NULL;
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
  pCache->pReadBlk = taosHashInit(10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == pCache->pReadBlk) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCache->writeDownstreamId = -1;

  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE void initGroupCacheSessionCtx(SGcSessionCtx* pSession, SGcOperatorParam* pGcParam, SGroupCacheData* pGroup) {
  pSession->pParam = pGcParam;
  pSession->downstreamIdx = pGcParam->downstreamIdx;
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
  } else if (pSession->pGroupData->needCache) {
    SSDataBlock** ppBlock = taosHashGet(pGCache->blkCache.pReadBlk, &pGcParam->sessionId, sizeof(pGcParam->sessionId));
    if (ppBlock) {
      releaseBaseBlockToList(pCtx, *ppBlock);
      taosHashRemove(pGCache->blkCache.pReadBlk, &pGcParam->sessionId, sizeof(pGcParam->sessionId));
    }
  }
  
  code = getBlkFromSessionCache(pOperator, pGcParam->sessionId, pSession, ppRes);
  if (NULL == *ppRes) {
    qError("session %" PRId64 " in downstream %d total got %" PRId64 " rows", pGcParam->sessionId, pCtx->id, pSession->resRows);
    taosHashRemove(pCtx->pSessions, &pGcParam->sessionId, sizeof(pGcParam->sessionId));
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
  if (pGroup->vgId > 0 && pGroup->needCache) {
    SGcFileCacheCtx* pFileCtx = &pGroup->pVgCtx->fileCtx;
    if (pGroup->fileId >= 0) {
      SGroupCacheFileInfo* pFileInfo = taosHashGet(pFileCtx->pCacheFile, &pGroup->fileId, sizeof(pGroup->fileId));
      uint32_t remainNum = atomic_sub_fetch_32(&pFileInfo->groupNum, 1);

      qTrace("FileId:%d-%d-%d sub group num to %u", pGroup->downstreamIdx, pGroup->vgId, pFileCtx->fileId, remainNum);

      if (0 == remainNum && pGroup->fileId != pFileCtx->fileId) {
        removeGroupCacheFile(pFileInfo);

#if 0
        /* debug only */
        sprintf(&pFileCtx->baseFilename[pFileCtx->baseNameLen], "_%d", pGroup->fileId);
        taosRemoveFile(pFileCtx->baseFilename);
        /* debug only */
#endif

        qTrace("FileId:%d-%d-%d removed", pGroup->downstreamIdx, pGroup->vgId, pFileCtx->fileId);
        //taosHashRemove(pFileCtx->pCacheFile, &pGroup->fileId, sizeof(pGroup->fileId));
      }
    }
  }

  taosArrayDestroy(pGroup->waitQueue);
  taosArrayDestroy(pGroup->blkList.pList);
  taosThreadMutexDestroy(&pGroup->mutex);

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
    pCtx->fetchSessionId = -1;
    pCtx->lastBlkUid = 0;
    pCtx->pVgTbHash = tSimpleHashInit(10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
    if (NULL == pCtx->pVgTbHash) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    tSimpleHashSetFreeFp(pCtx->pVgTbHash, freeSGcVgroupCtx);      

    if (pInfo->batchFetch) {
      int32_t defaultVg = 0;
      SGcVgroupCtx vgCtx = {0};
      initGcVgroupCtx(pOperator, &vgCtx, pCtx->id, defaultVg, NULL);      
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
      taosHashSetFreeFp(pCtx->pGrpHash, freeRemoveGroupCacheData);      
    }

    pCtx->pSessions = taosHashInit(20, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
    if (pCtx->pSessions == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    taosHashSetFreeFp(pCtx->pSessions, freeSGcSessionCtx);
  
    pCtx->pFreeBlock = taosArrayInit(10, POINTER_BYTES);
    if (NULL == pCtx->pFreeBlock) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    
    pCtx->pWaitSessions = taosHashInit(20, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
    if (pCtx->pWaitSessions == NULL) {
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

  qTrace("try to remove group %" PRIu64, pGcParam->tbUid);
  if (taosHashRemove(pGrpHash, &pGcParam->tbUid, sizeof(pGcParam->tbUid))) {
    qError("failed to remove group %" PRIu64 " in vgId %d downstreamIdx %d", pGcParam->tbUid, pGcParam->vgId, pGcParam->downstreamIdx);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  return TSDB_CODE_SUCCESS;
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


