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
  taosMemoryFree(pGrpCacheOperator->ppDownStream);
  
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

static int32_t initGroupCacheBufPages(SGroupCacheOperatorInfo* pInfo) {
  pInfo->pBlkBufs = taosArrayInit(32, sizeof(SBufPageInfo));
  if (NULL == pInfo->pBlkBufs) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return addPageToGroupCacheBuf(pInfo->pBlkBufs);
}

static int32_t getFromSessionCache(SGroupCacheOperatorInfo* pGCache, SGcOperatorParam* pParam, SSDataBlock** ppRes) {

}

static SSDataBlock* getFromGroupCache(struct SOperatorInfo* pOperator, SOperatorParam* param) {
  SGroupCacheOperatorInfo* pGCache = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  int32_t code = TSDB_CODE_SUCCESS;
  SSDataBlock* pRes = NULL;
  SGcOperatorParam* pParam = getOperatorParam(pOperator->operatorType, param)
  if (NULL == pParam || pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }
  
  code = getFromSessionCache(pGCache, pParam, &pRes, );

  while (true) {
    SSDataBlock* pBlock = pJoin->pProbe->downStream->fpSet.getNextFn(pJoin->pProbe->downStream);
    if (NULL == pBlock) {
      setHJoinDone(pOperator);
      break;
    }
    
    code = launchBlockHashJoin(pOperator, pBlock);
    if (code) {
      pTaskInfo->code = code;
      T_LONG_JMP(pTaskInfo->env, code);
    }

    if (pRes->info.rows < pOperator->resultInfo.threshold) {
      continue;
    }
    
    if (pOperator->exprSupp.pFilterInfo != NULL) {
      doFilter(pRes, pOperator->exprSupp.pFilterInfo, NULL);
    }
    if (pRes->info.rows > 0) {
      break;
    }
  }
  
  return (pRes->info.rows > 0) ? pRes : NULL;
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

  pInfo->ppDownStream = taosMemoryMalloc(numOfDownstream * POINTER_BYTES);
  if (NULL == pInfo->ppDownStream) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }
  memcpy(pInfo->ppDownStream, pDownstream, numOfDownstream * POINTER_BYTES);

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


