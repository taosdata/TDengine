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

static void destroyGroupCacheOperator(void* param) {
  SGroupCacheOperatorInfo* pGrpCacheOperator = (SGroupCacheOperatorInfo*)param;

  taosMemoryFree(pGrpCacheOperator->groupColsInfo.pColsInfo);
  taosMemoryFree(pGrpCacheOperator->groupColsInfo.pBuf);
  taosArrayDestroyEx(pGrpCacheOperator->pBlkBufs, freeGroupCacheBufPage);
  tSimpleHashCleanup(pGrpCacheOperator->pSessionHash);
  tSimpleHashCleanup(pGrpCacheOperator->pBlkHash);
  tSimpleHashCleanup(pGrpCacheOperator->downstreamInfo.pKey2Idx);
  taosMemoryFree(pGrpCacheOperator->downstreamInfo.ppDownStream);
  
  taosMemoryFreeClear(param);
}

static FORCE_INLINE int32_t addPageToGroupCacheBuf(SArray* pBlkBufs) {
  SBufPageInfo page;
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
  SBufPageInfo *pPage = taosArrayGet(pBlkBufs, pBlkInfo->pageId);
  return pPage->data + pBlkInfo->offset;
}

static FORCE_INLINE char* moveRetrieveBlkFromBlkBufs(SArray* pBlkBufs, SGcBlkBufInfo** ppLastBlk) {
  if (NULL == *ppLastBlk) {
    return NULL;
  }
  SGcBlkBufInfo* pCurr = (*ppLastBlk)->next;
  *ppLastBlk = pCurr;
  if (pCurr) {
    SBufPageInfo *pPage = taosArrayGet(pBlkBufs, pCurr->pageId);
    return pPage->data + pCurr->offset;
  }

  return NULL;
}


static int32_t initGroupCacheBufPages(SGroupCacheOperatorInfo* pInfo) {
  pInfo->pBlkBufs = taosArrayInit(32, sizeof(SBufPageInfo));
  if (NULL == pInfo->pBlkBufs) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return addPageToGroupCacheBuf(pInfo->pBlkBufs);
}

static int32_t initGroupCacheDownstreamInfo(SGroupCachePhysiNode* pPhyciNode, SOperatorInfo** pDownstream, int32_t numOfDownstream, SGcDownstreamInfo* pInfo) {
  pInfo->ppDownStream = taosMemoryMalloc(numOfDownstream * POINTER_BYTES);
  if (NULL == pInfo->ppDownStream) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  memcpy(pInfo->ppDownStream, pDownstream, numOfDownstream * POINTER_BYTES);
  pInfo->pKey2Idx = tSimpleHashInit(numOfDownstream, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  if (NULL == pInfo->pKey2Idx) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  
  for (int32_t i = 0; i < numOfDownstream; ++i) {
    int32_t keyValue = taosArrayGet(pPhyciNode, i);
    tSimpleHashPut(pInfo->pKey2Idx, &keyValue, sizeof(keyValue), &i, sizeof(i));
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t initGroupCacheSession(SGroupCacheOperatorInfo* pGCache, SGcOperatorParam* pParam, SGcSessionCtx** ppSession) {
  SGcSessionCtx ctx = {0};
  SGroupData* pGroup = tSimpleHashGet(pGCache->pBlkHash, pParam->pGroupValue, pParam->groupValueSize);
  if (pGroup) {
    ctx.cacheHit = true;
    ctx.pLastBlk = pGroup->blks;
  } else {
    int32_t* pIdx = tSimpleHashGet(pGCache->downstreamInfo.pKey2Idx, &pParam->downstreamKey, sizeof(pParam->downstreamKey));
    if (NULL == pIdx) {
      qError("Invalid downstream key value: %d", pParam->downstreamKey);
      return TSDB_CODE_INVALID_PARA;
    }
    ctx.pDownstream = pGCache->downstreamInfo.ppDownStream[*pIdx];
    ctx.needCache = pParam->needCache;
  }
  
  return TSDB_CODE_SUCCESS;
}

static void getFromSessionCache(SExecTaskInfo* pTaskInfo, SGroupCacheOperatorInfo* pGCache, SGcOperatorParam* pParam, SSDataBlock** ppRes, SGcSessionCtx** ppSession) {
  if (pParam->basic.newExec) {
    int32_t code = initGroupCacheSession(pGCache, pParam, ppSession);
    if (TSDB_CODE_SUCCESS != code) {
      pTaskInfo->code = code;
      T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
    }
    if ((*ppSession)->pLastBlk) {
      *ppRes = retrieveBlkFromBlkBufs(pGCache->pBlkBufs, (*ppSession)->pLastBlk);
    } else {
      *ppRes = NULL;
    }
    return;
  }
  
  SGcSessionCtx* pCtx = tSimpleHashGet(pGCache->pSessionHash, &pParam->sessionId, sizeof(pParam->sessionId));
  if (NULL == pCtx) {
    qError("session %" PRIx64 " not exists", pParam->sessionId);
    pTaskInfo->code = TSDB_CODE_INVALID_PARA;
    T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
  }
  *ppSession = pCtx;
  
  if (pCtx->cacheHit) {
    *ppRes = moveRetrieveBlkFromBlkBufs(pGCache->pBlkBufs, &pCtx->pLastBlk);
    return;
  }

  *ppRes = NULL;
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

static void setCurrentGroupCacheDone(struct SOperatorInfo* pOperator) {
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  destroyCurrentGroupCacheSession(pGCache, &pGCache->pCurrent, &pGCache->pCurrentId);
}

static void addBlkToGroupCache(struct SOperatorInfo* pOperator, SSDataBlock* pBlock, SSDataBlock** ppRes) {
  *ppRes = pBlock;
}

static SSDataBlock* getFromGroupCache(struct SOperatorInfo* pOperator, SOperatorParam* param) {
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SGcSessionCtx* pSession = NULL;
  SSDataBlock* pRes = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  SGcOperatorParam* pParam = getOperatorParam(pOperator->operatorType, param);
  if (NULL == pParam) {
    return NULL;
  }
  
  getFromSessionCache(pTaskInfo, pGCache, pParam, &pRes, &pSession);
  pGCache->pCurrent = pSession;
  pGCache->pCurrentId = pParam->sessionId;
  
  if (pRes) {
    return pRes;
  }

  while (true) {
    SSDataBlock* pBlock = pSession->pDownstream->fpSet.getNextExtFn(pSession->pDownstream, param);
    if (NULL == pBlock) {
      setCurrentGroupCacheDone(pOperator);
      break;
    }

    if (pGCache->pCurrent->needCache) {
      addBlkToGroupCache(pOperator, pBlock, &pRes);
    }
    break;
  }
  
  return pRes;
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

  setOperatorInfo(pOperator, "GroupCacheOperator", QUERY_NODE_PHYSICAL_PLAN_GROUP_CACHE, false, OP_NOT_OPENED, pInfo, pTaskInfo);

  code = initGroupColsInfo(&pInfo->groupColsInfo, pPhyciNode->grpColsMayBeNull, pPhyciNode->pGroupCols);
  if (code) {
    goto _error;
  }

  code = initGroupCacheBufPages(pInfo);
  if (code) {
    goto _error;
  }

  pInfo->pBlkHash = tSimpleHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
  if (pInfo->pBlkHash == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  pInfo->pSessionHash = tSimpleHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  if (pInfo->pSessionHash == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  code = initGroupCacheDownstreamInfo(pPhyciNode, pDownstream, numOfDownstream, &pInfo->downstreamInfo);
  if (code) {
    goto _error;
  }

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, NULL, NULL, destroyGroupCacheOperator, optrDefaultBufFn, NULL, getFromGroupCache, NULL);

  return pOperator;

_error:
  if (pInfo != NULL) {
    destroyGroupCacheOperator(pInfo);
  }

  taosMemoryFree(pOperator);
  pTaskInfo->code = code;
  return NULL;
}


