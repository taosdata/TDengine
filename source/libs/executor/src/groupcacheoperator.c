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

static void freeGroupCacheBufPage(void* param) {
  SGcBufPageInfo* pInfo = (SGcBufPageInfo*)param;
  taosMemoryFree(pInfo->data);
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
  taosArrayDestroyEx(pGrpCacheOperator->pBlkBufs, freeGroupCacheBufPage);
  tSimpleHashCleanup(pGrpCacheOperator->pSessionHash);
  taosHashCleanup(pGrpCacheOperator->pBlkHash);
  
  taosMemoryFreeClear(param);
}

static FORCE_INLINE int32_t addPageToGroupCacheBuf(SArray* pBlkBufs) {
  SGcBufPageInfo page;
  page.pageSize = GROUP_CACHE_DEFAULT_PAGE_SIZE;
  page.offset = 0;
  page.data = taosMemoryMalloc(page.pageSize);
  if (NULL == page.data) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  taosArrayPush(pBlkBufs, &page);
  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE char* retrieveBlkFromBlkBufs(SArray* pBlkBufs, SGcBlkBufInfo* pBlkInfo) {
  SGcBufPageInfo *pPage = taosArrayGet(pBlkBufs, pBlkInfo->pageId);
  return pPage->data + pBlkInfo->offset;
}

static FORCE_INLINE int32_t appendNewGroupToDownstream(struct SOperatorInfo* pOperator, int32_t downstreamIdx) {
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  SGcDownstreamCtx* pCtx = &pGCache->pDownstreams[downstreamIdx];  

  taosWLockLatch(&pGCache->pDownstreams[pParam->downstreamIdx].lock);

  if (NULL == taosArrayPush(pCtx->pGrpUidList, &pGCache->newGroup.uid)) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  
  pGCache->newGroup.uid = 0;
  taosThreadMutexUnlock(&pGCache->sessionMutex);
  
  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE int32_t getBlkFromDownstreamOperator(struct SOperatorInfo* pOperator, int32_t downstreamIdx, SSDataBlock** ppRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  if (pGCache->pDownstreams[downstreamIdx].pNewGrpList) {
    code = appendNewGroupToDownstream(pOperator, downstreamIdx, &pGCache->newGroup.uid);
    if (code) {
      return code;
    }
  }
  
  SSDataBlock* pBlock = getNextBlockFromDownstreamOnce(pOperator, downstreamIdx);
  if (pBlock) {
    pGCache->execInfo.pDownstreamBlkNum[downstreamIdx]++;
  }
  
  *ppRes = pBlock;
  
  return TSDB_CODE_SUCCESS;
}

static void addBlkToGroupCache(struct SOperatorInfo* pOperator, SSDataBlock* pBlock, SSDataBlock** ppRes) {
  *ppRes = pBlock;
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

int32_t handleGroupCacheRetrievedBlk(struct SOperatorInfo* pOperator, SSDataBlock* pBlock, SGcSessionCtx* pSession, bool* continueFetch) {
  int32_t code = TSDB_CODE_SUCCESS;
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  if (pGCache->grpByUid) {
    SGroupCacheData* pGroup = taosHashGet(pGCache->pBlkHash, &pBlock->info.id.uid, sizeof(pBlock->info.id.uid));
    pGroup->pBlock = pBlock;
    
    if (pGroup->needCache) {
      SGcBlkBufInfo* pNewBlk = NULL;
      code = addBlkToGroupCache(pOperator, pBlock, &pNewBlk);
      if (code) {
        return code;
      }
      
      if (pGroup->pLastBlk) {
        pGroup->pLastBlk->next = pNewBlk;
        pGroup->pLastBlk = pNewBlk;
      } else {
        pGroup->pFirstBlk = pNewBlk;
        pGroup->pLastBlk = pNewBlk;
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
  notifyWaitingSessions();
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
    if (NULL == pSession->pLastBlk) {
      if (pSession->pGroupData->pFirstBlk) {
        *ppRes = retrieveBlkFromBlkBufs(pGCache->pBlkBufs, pSession->pGroupData->pFirstBlk);
        pSession->pLastBlk = pSession->pGroupData->pFirstBlk;
        return TSDB_CODE_SUCCESS;
      } else if (pGroup->fetchDone) {
        *ppRes = NULL;
        return TSDB_CODE_SUCCESS;
      }
    } else if (pSession->pLastBlk->next) {
      *ppRes = retrieveBlkFromBlkBufs(pGCache->pBlkBufs, pSession->pLastBlk->next);
      pSession->pLastBlk = pSession->pLastBlk->next;
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
      if (NULL == pSession->pLastBlk) {
        if (pSession->pGroupData->pFirstBlk) {
          *ppRes = retrieveBlkFromBlkBufs(pGCache->pBlkBufs, pSession->pGroupData->pFirstBlk);
          pSession->pLastBlk = pSession->pGroupData->pFirstBlk;
          goto _return;
        }
      } else if (pSession->pLastBlk->next) {
        *ppRes = retrieveBlkFromBlkBufs(pGCache->pBlkBufs, pSession->pLastBlk->next);
        pSession->pLastBlk = pSession->pLastBlk->next;
        goto _return;
      }
    } else if (pSession->pGroupData->pBlock || pSession->pGroupData->fetchDone) {
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


static int32_t initGroupCacheBufPages(SGroupCacheOperatorInfo* pInfo) {
  pInfo->pBlkBufs = taosArrayInit(32, sizeof(SGcBufPageInfo));
  if (NULL == pInfo->pBlkBufs) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return addPageToGroupCacheBuf(pInfo->pBlkBufs);
}

static int32_t initGroupCacheGroupData(struct SOperatorInfo* pOperator, SGcOperatorParam* pParam, SGroupCacheData** ppGrp) {
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  SGroupCacheData grpData = {0};
  grpData.needCache = pParam->needCache;
  
  while (true) {
    if (0 != taosHashPut(pGCache->pBlkHash, pParam->pGroupValue, pParam->groupValueSize, &grpData, sizeof(grpData))) {
      if (terrno == TSDB_CODE_DUP_KEY) {
        *ppGrp = taosHashAcquire(pGCache->pBlkHash, pParam->pGroupValue, pParam->groupValueSize);
        if (*ppGrp) {
          break;
        }
      } else {
        return terrno;
      }
    }

    *ppGrp = taosHashAcquire(pGCache->pBlkHash, pParam->pGroupValue, pParam->groupValueSize);
    if (*ppGrp) {
      SGcNewGroupInfo newGroup;
      newGroup.uid = *(int64_t*)pParam->pGroupValue;
      newGroup.pParam = pOperator->pDownstreamParams[pParam->downstreamIdx];
      taosWLockLatch(&pGCache->pDownstreams[pParam->downstreamIdx].lock);
      if (NULL == taosArrayPush(pGCache->pDownstreams[pParam->downstreamIdx].pNewGrpList, &newGroup)) {
        taosWUnLockLatch(&pGCache->pDownstreams[pParam->downstreamIdx].lock);
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      taosWUnLockLatch(&pGCache->pDownstreams[pParam->downstreamIdx].lock);
      
      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t initGroupCacheSession(struct SOperatorInfo* pOperator, SGcOperatorParam* pParam, SGcSessionCtx** ppSession) {
  SGcSessionCtx ctx = {0};
  int32_t code = 0;
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  SGroupCacheData* pGroup = taosHashAcquire(pGCache->pBlkHash, pParam->pGroupValue, pParam->groupValueSize);
  if (pGroup) {
    ctx.pGroupData = pGroup;
  } else {
    code = initGroupCacheGroupData(pOperator, pParam, &ctx.pGroupData);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  taosThreadMutexUnlock(&pGCache->sessionMutex);
  
  ctx.pParam = pParam;

  int32_t code = tSimpleHashPut(pGCache->pSessionHash, &pParam->sessionId, sizeof(pParam->sessionId), &ctx, sizeof(ctx));
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  *ppSession = tSimpleHashGet(pGCache->pSessionHash, &pParam->sessionId, sizeof(pParam->sessionId));
  
  return TSDB_CODE_SUCCESS;
}

static int32_t getBlkFromSessionCache(struct SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  SGcOperatorParam* pGcParam = pOperator->pOperatorParam->value;
  SGcSessionCtx* pSession = tSimpleHashGet(pGCache->pSessionHash, &pGcParam->sessionId, sizeof(pGcParam->sessionId));
  if (NULL == pSession) {
    int32_t code = initGroupCacheSession(pOperator, pGcParam, &pSession);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  } else {
    taosThreadMutexUnlock(&pGCache->sessionMutex);
  }
  
  return getBlkFromSessionCacheImpl(pOperator, pGcParam->sessionId, pSession, ppRes);
}

static FORCE_INLINE void destroyCurrentGroupCacheSession(SGroupCacheOperatorInfo* pGCache, SGcSessionCtx** ppCurrent, int64_t* pCurrentId) {
  if (NULL == *ppCurrent) {
    return;
  }
  if (tSimpleHashRemove(pGCache->pSessionHash, pCurrentId, sizeof(*pCurrentId))) {
    qError("remove session %" PRIx64 " failed", *pCurrentId);
  }
  
  *ppCurrent = NULL;
  *pCurrentId = 0;
}

SSDataBlock* getBlkFromGroupCache(struct SOperatorInfo* pOperator) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SSDataBlock* pRes = NULL;
  
  int32_t code = getBlkFromSessionCache(pOperator, &pRes);
  if (TSDB_CODE_SUCCESS != code) {
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
  }
  
  return pRes;
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
    pInfo->pDownstreams[i].pGrpUidList = taosArrayInit(10, sizeof(int64_t));
    if (NULL == pInfo->pDownstreams[i].pGrpUidList) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return TSDB_CODE_SUCCESS;
}

SSDataBlock* groupCacheGetNext(struct SOperatorInfo* pOperator, SOperatorParam* pParam) {
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  
  taosThreadMutexLock(&pGCache->sessionMutex);

  int32_t code = setOperatorParams(pOperator, pParam);
  if (TSDB_CODE_SUCCESS != code) {
    pOperator->pTaskInfo->code = code;
    T_LONG_JMP(pOperator->pTaskInfo->env, pOperator->pTaskInfo->code);
  }

  return getBlkFromGroupCache(pOperator);
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

  pInfo->grpByUid = pPhyciNode->grpByUid;
  if (!pInfo->grpByUid) {
    qError("only group cache by uid is supported now");
    return TSDB_CODE_INVALID_PARA;
  }
  
  if (pPhyciNode->pGroupCols) {
    code = initGroupColsInfo(&pInfo->groupColsInfo, pPhyciNode->grpColsMayBeNull, pPhyciNode->pGroupCols);
    if (code) {
      goto _error;
    }
  }

  code = initGroupCacheBufPages(pInfo);
  if (code) {
    goto _error;
  }

  pInfo->pBlkHash = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (pInfo->pBlkHash == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  pInfo->pSessionHash = tSimpleHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
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

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, getBlkFromGroupCache, NULL, destroyGroupCacheOperator, optrDefaultBufFn, NULL, groupCacheGetNext, NULL);

  return pOperator;

_error:
  if (pInfo != NULL) {
    destroyGroupCacheOperator(pInfo);
  }

  taosMemoryFree(pOperator);
  pTaskInfo->code = code;
  return NULL;
}


