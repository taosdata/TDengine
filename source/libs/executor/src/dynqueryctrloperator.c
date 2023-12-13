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
#include "dynqueryctrl.h"

int64_t gSessionId = 0;

void freeVgTableList(void* ptr) { 
  taosArrayDestroy(*(SArray**)ptr); 
}

static void destroyStbJoinTableList(SStbJoinTableList* pListHead) {
  SStbJoinTableList* pNext = NULL;
  
  while (pListHead) {
    taosMemoryFree(pListHead->pLeftVg);
    taosMemoryFree(pListHead->pLeftUid);
    taosMemoryFree(pListHead->pRightVg);
    taosMemoryFree(pListHead->pRightUid);
    pNext = pListHead->pNext;
    taosMemoryFree(pListHead);
    pListHead = pNext;
  }
}

static void destroyStbJoinDynCtrlInfo(SStbJoinDynCtrlInfo* pStbJoin) {
  qError("dynQueryCtrl exec info, prevBlk:%" PRId64 ", prevRows:%" PRId64 ", postBlk:%" PRId64 ", postRows:%" PRId64 ", leftCacheNum:%" PRId64 ", rightCacheNum:%" PRId64, 
         pStbJoin->execInfo.prevBlkNum, pStbJoin->execInfo.prevBlkRows, pStbJoin->execInfo.postBlkNum, 
         pStbJoin->execInfo.postBlkRows, pStbJoin->execInfo.leftCacheNum, pStbJoin->execInfo.rightCacheNum);

  if (pStbJoin->basic.batchFetch) {
    if (pStbJoin->ctx.prev.leftHash) {
      tSimpleHashSetFreeFp(pStbJoin->ctx.prev.leftHash, freeVgTableList);
      tSimpleHashCleanup(pStbJoin->ctx.prev.leftHash);
    }
    if (pStbJoin->ctx.prev.rightHash) {
      tSimpleHashSetFreeFp(pStbJoin->ctx.prev.rightHash, freeVgTableList);
      tSimpleHashCleanup(pStbJoin->ctx.prev.rightHash);
    }
  } else {
    if (pStbJoin->ctx.prev.leftCache) {
      tSimpleHashCleanup(pStbJoin->ctx.prev.leftCache);
    }
    if (pStbJoin->ctx.prev.rightCache) {
      tSimpleHashCleanup(pStbJoin->ctx.prev.rightCache);
    }
    if (pStbJoin->ctx.prev.onceTable) {
      tSimpleHashCleanup(pStbJoin->ctx.prev.onceTable);
    }
  }

  destroyStbJoinTableList(pStbJoin->ctx.prev.pListHead);
}

static void destroyDynQueryCtrlOperator(void* param) {
  SDynQueryCtrlOperatorInfo* pDyn = (SDynQueryCtrlOperatorInfo*)param;

  switch (pDyn->qType) {
    case DYN_QTYPE_STB_HASH:
      destroyStbJoinDynCtrlInfo(&pDyn->stbJoin);
      break;
    default:
      qError("unsupported dynamic query ctrl type: %d", pDyn->qType);
      break;
  }

  taosMemoryFreeClear(param);
}

static FORCE_INLINE bool tableNeedCache(int64_t uid, SStbJoinPrevJoinCtx* pPrev, SStbJoinPostJoinCtx* pPost, bool rightTable, bool batchFetch) {
  if (batchFetch) {
    return true;
  }
  
  if (rightTable) {
    return pPost->rightCurrUid == pPost->rightNextUid;
  }

  uint32_t* num = tSimpleHashGet(pPrev->leftCache, &uid, sizeof(uid));

  return (NULL == num) ? false : true;
}

static void updatePostJoinCurrTableInfo(SStbJoinDynCtrlInfo*          pStbJoin) {
  SStbJoinPrevJoinCtx*       pPrev = &pStbJoin->ctx.prev;
  SStbJoinPostJoinCtx*       pPost = &pStbJoin->ctx.post;
  SStbJoinTableList*         pNode = pPrev->pListHead;
  int32_t*                   leftVgId = pNode->pLeftVg + pNode->readIdx;
  int32_t*                   rightVgId = pNode->pRightVg + pNode->readIdx;
  int64_t*                   leftUid = pNode->pLeftUid + pNode->readIdx;
  int64_t*                   rightUid = pNode->pRightUid + pNode->readIdx;
  int64_t                    readIdx = pNode->readIdx + 1;
  int64_t                    rightPrevUid = pPost->rightCurrUid;

  pPost->leftCurrUid = *leftUid;
  pPost->rightCurrUid = *rightUid;

  pPost->leftVgId = *leftVgId;
  pPost->rightVgId = *rightVgId;

  while (true) {
    if (readIdx < pNode->uidNum) {
      pPost->rightNextUid = *(pNode->pRightUid + readIdx);
      break;
    }
    
    pNode = pNode->pNext;
    if (NULL == pNode) {
      pPost->rightNextUid = 0;
      break;
    }
    
    rightUid = pNode->pRightUid;
    readIdx = 0;
  }

  pPost->leftNeedCache = tableNeedCache(*leftUid, pPrev, pPost, false, pStbJoin->basic.batchFetch);
  pPost->rightNeedCache = tableNeedCache(*rightUid, pPrev, pPost, true, pStbJoin->basic.batchFetch);

  if (pPost->rightNeedCache && rightPrevUid != pPost->rightCurrUid) {
    tSimpleHashPut(pPrev->rightCache, &pPost->rightCurrUid, sizeof(pPost->rightCurrUid), NULL, 0);
    pStbJoin->execInfo.rightCacheNum++;
  }  
}


static int32_t buildGroupCacheOperatorParam(SOperatorParam** ppRes, int32_t downstreamIdx, int32_t vgId, int64_t tbUid, bool needCache, SOperatorParam* pChild) {
  *ppRes = taosMemoryMalloc(sizeof(SOperatorParam));
  if (NULL == *ppRes) {
    freeOperatorParam(pChild, OP_GET_PARAM);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (pChild) {
    (*ppRes)->pChildren = taosArrayInit(1, POINTER_BYTES);
    if (NULL == (*ppRes)->pChildren) {
      freeOperatorParam(pChild, OP_GET_PARAM);
      freeOperatorParam(*ppRes, OP_GET_PARAM);
      *ppRes = NULL;
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    if (NULL == taosArrayPush((*ppRes)->pChildren, &pChild)) {
      freeOperatorParam(pChild, OP_GET_PARAM);
      freeOperatorParam(*ppRes, OP_GET_PARAM);
      *ppRes = NULL;
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  } else {
    (*ppRes)->pChildren = NULL;
  }

  SGcOperatorParam* pGc = taosMemoryMalloc(sizeof(SGcOperatorParam));
  if (NULL == pGc) {
    freeOperatorParam(*ppRes, OP_GET_PARAM);
    *ppRes = NULL;
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pGc->sessionId = atomic_add_fetch_64(&gSessionId, 1);
  pGc->downstreamIdx = downstreamIdx;
  pGc->vgId = vgId;
  pGc->tbUid = tbUid;
  pGc->needCache = needCache;

  (*ppRes)->opType = QUERY_NODE_PHYSICAL_PLAN_GROUP_CACHE;
  (*ppRes)->downstreamIdx = downstreamIdx;
  (*ppRes)->value = pGc;

  return TSDB_CODE_SUCCESS;
}


static int32_t buildGroupCacheNotifyOperatorParam(SOperatorParam** ppRes, int32_t downstreamIdx, int32_t vgId, int64_t tbUid) {
  *ppRes = taosMemoryMalloc(sizeof(SOperatorParam));
  if (NULL == *ppRes) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  (*ppRes)->pChildren = NULL;

  SGcNotifyOperatorParam* pGc = taosMemoryMalloc(sizeof(SGcNotifyOperatorParam));
  if (NULL == pGc) {
    freeOperatorParam(*ppRes, OP_NOTIFY_PARAM);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pGc->downstreamIdx = downstreamIdx;
  pGc->vgId = vgId;
  pGc->tbUid = tbUid;

  (*ppRes)->opType = QUERY_NODE_PHYSICAL_PLAN_GROUP_CACHE;
  (*ppRes)->downstreamIdx = downstreamIdx;
  (*ppRes)->value = pGc;

  return TSDB_CODE_SUCCESS;
}


static int32_t buildExchangeOperatorParam(SOperatorParam** ppRes, int32_t downstreamIdx, int32_t* pVgId, int64_t* pUid) {
  *ppRes = taosMemoryMalloc(sizeof(SOperatorParam));
  if (NULL == *ppRes) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  (*ppRes)->pChildren = NULL;
  
  SExchangeOperatorParam* pExc = taosMemoryMalloc(sizeof(SExchangeOperatorParam));
  if (NULL == pExc) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pExc->multiParams = false;
  pExc->basic.vgId = *pVgId;
  pExc->basic.tableSeq = true;
  pExc->basic.srcOpType = QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN;
  pExc->basic.uidList = taosArrayInit(1, sizeof(int64_t));
  if (NULL == pExc->basic.uidList) {
    taosMemoryFree(pExc);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  taosArrayPush(pExc->basic.uidList, pUid);

  (*ppRes)->opType = QUERY_NODE_PHYSICAL_PLAN_EXCHANGE;
  (*ppRes)->downstreamIdx = downstreamIdx;
  (*ppRes)->value = pExc;
  
  return TSDB_CODE_SUCCESS;
}


static int32_t buildBatchExchangeOperatorParam(SOperatorParam** ppRes, int32_t downstreamIdx, SSHashObj* pVg) {
  *ppRes = taosMemoryMalloc(sizeof(SOperatorParam));
  if (NULL == *ppRes) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  (*ppRes)->pChildren = NULL;
  
  SExchangeOperatorBatchParam* pExc = taosMemoryMalloc(sizeof(SExchangeOperatorBatchParam));
  if (NULL == pExc) {
    taosMemoryFreeClear(*ppRes);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pExc->multiParams = true;
  pExc->pBatchs = tSimpleHashInit(tSimpleHashGetSize(pVg), taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  if (NULL == pExc->pBatchs) {
    taosMemoryFree(pExc);
    taosMemoryFreeClear(*ppRes);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  tSimpleHashSetFreeFp(pExc->pBatchs, freeExchangeGetBasicOperatorParam);
  
  SExchangeOperatorBasicParam basic;
  basic.srcOpType = QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN;

  int32_t iter = 0;
  void* p = NULL;
  while (NULL != (p = tSimpleHashIterate(pVg, p, &iter))) {
    int32_t* pVgId = tSimpleHashGetKey(p, NULL);
    SArray* pUidList = *(SArray**)p;
    basic.vgId = *pVgId;
    basic.uidList = pUidList;
    basic.tableSeq = false;
    
    tSimpleHashPut(pExc->pBatchs, pVgId, sizeof(*pVgId), &basic, sizeof(basic));   

    qTrace("build downstreamIdx %d batch scan, vgId:%d, uidNum:%" PRId64, downstreamIdx, *pVgId, (int64_t)taosArrayGetSize(pUidList));
    *(SArray**)p = NULL;
  }

  (*ppRes)->opType = QUERY_NODE_PHYSICAL_PLAN_EXCHANGE;
  (*ppRes)->downstreamIdx = downstreamIdx;
  (*ppRes)->value = pExc;
  
  return TSDB_CODE_SUCCESS;
}


static int32_t buildMergeJoinOperatorParam(SOperatorParam** ppRes, bool initParam, SOperatorParam* pChild0, SOperatorParam* pChild1) {
  *ppRes = taosMemoryMalloc(sizeof(SOperatorParam));
  if (NULL == *ppRes) {
    freeOperatorParam(pChild0, OP_GET_PARAM);
    freeOperatorParam(pChild1, OP_GET_PARAM);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  (*ppRes)->pChildren = taosArrayInit(2, POINTER_BYTES);
  if (NULL == *ppRes) {
    freeOperatorParam(pChild0, OP_GET_PARAM);
    freeOperatorParam(pChild1, OP_GET_PARAM);
    freeOperatorParam(*ppRes, OP_GET_PARAM);
    *ppRes = NULL;
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (NULL == taosArrayPush((*ppRes)->pChildren, &pChild0)) {
    freeOperatorParam(pChild0, OP_GET_PARAM);
    freeOperatorParam(pChild1, OP_GET_PARAM);
    freeOperatorParam(*ppRes, OP_GET_PARAM);
    *ppRes = NULL;
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (NULL == taosArrayPush((*ppRes)->pChildren, &pChild1)) {
    freeOperatorParam(pChild1, OP_GET_PARAM);
    freeOperatorParam(*ppRes, OP_GET_PARAM);
    *ppRes = NULL;
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  
  SSortMergeJoinOperatorParam* pJoin = taosMemoryMalloc(sizeof(SSortMergeJoinOperatorParam));
  if (NULL == pJoin) {
    freeOperatorParam(*ppRes, OP_GET_PARAM);
    *ppRes = NULL;
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pJoin->initDownstream = initParam;
  
  (*ppRes)->opType = QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN;
  (*ppRes)->value = pJoin;

  return TSDB_CODE_SUCCESS;
}


static int32_t buildMergeJoinNotifyOperatorParam(SOperatorParam** ppRes, SOperatorParam* pChild0, SOperatorParam* pChild1) {
  *ppRes = taosMemoryMalloc(sizeof(SOperatorParam));
  if (NULL == *ppRes) {
    freeOperatorParam(pChild0, OP_NOTIFY_PARAM);
    freeOperatorParam(pChild1, OP_NOTIFY_PARAM);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  (*ppRes)->pChildren = taosArrayInit(2, POINTER_BYTES);
  if (NULL == *ppRes) {
    taosMemoryFreeClear(*ppRes);
    freeOperatorParam(pChild0, OP_NOTIFY_PARAM);
    freeOperatorParam(pChild1, OP_NOTIFY_PARAM);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (pChild0 && NULL == taosArrayPush((*ppRes)->pChildren, &pChild0)) {
    freeOperatorParam(*ppRes, OP_NOTIFY_PARAM);
    freeOperatorParam(pChild0, OP_NOTIFY_PARAM);
    freeOperatorParam(pChild1, OP_NOTIFY_PARAM);
    *ppRes = NULL;
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (pChild1 && NULL == taosArrayPush((*ppRes)->pChildren, &pChild1)) {
    freeOperatorParam(*ppRes, OP_NOTIFY_PARAM);
    freeOperatorParam(pChild1, OP_NOTIFY_PARAM);
    *ppRes = NULL;
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  
  (*ppRes)->opType = QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN;
  (*ppRes)->value = NULL;

  return TSDB_CODE_SUCCESS;
}



static int32_t buildBatchTableScanOperatorParam(SOperatorParam** ppRes, int32_t downstreamIdx, SSHashObj* pVg) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t vgNum = tSimpleHashGetSize(pVg);
  if (vgNum <= 0 || vgNum > 1) {
    qError("Invalid vgroup num %d to build table scan operator param", vgNum);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  int32_t iter = 0;
  void* p = NULL;
  while (NULL != (p = tSimpleHashIterate(pVg, p, &iter))) {
    int32_t* pVgId = tSimpleHashGetKey(p, NULL);
    SArray* pUidList = *(SArray**)p;

    code = buildTableScanOperatorParam(ppRes, pUidList, QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, false);
    if (code) {
      return code;
    }
    taosArrayDestroy(pUidList);
    *(SArray**)p = NULL;
  }
  
  return TSDB_CODE_SUCCESS;
}


static int32_t buildSingleTableScanOperatorParam(SOperatorParam** ppRes, int32_t downstreamIdx, int32_t* pVgId, int64_t* pUid) {
  SArray* pUidList = taosArrayInit(1, sizeof(int64_t));
  if (NULL == pUidList) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  taosArrayPush(pUidList, pUid);

  int32_t code = buildTableScanOperatorParam(ppRes, pUidList, QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, true);
  taosArrayDestroy(pUidList);
  if (code) {
    return code;
  }
  
  return TSDB_CODE_SUCCESS;
}

static int32_t buildSeqStbJoinOperatorParam(SDynQueryCtrlOperatorInfo* pInfo, SStbJoinPrevJoinCtx* pPrev, SStbJoinPostJoinCtx* pPost, SOperatorParam** ppParam) {
  int64_t                     rowIdx = pPrev->pListHead->readIdx;
  SOperatorParam*             pSrcParam0 = NULL;
  SOperatorParam*             pSrcParam1 = NULL;
  SOperatorParam*             pGcParam0 = NULL;
  SOperatorParam*             pGcParam1 = NULL;  
  int32_t*                    leftVg = pPrev->pListHead->pLeftVg + rowIdx;
  int64_t*                    leftUid = pPrev->pListHead->pLeftUid + rowIdx;
  int32_t*                    rightVg = pPrev->pListHead->pRightVg + rowIdx;
  int64_t*                    rightUid = pPrev->pListHead->pRightUid + rowIdx;
  int32_t                     code = TSDB_CODE_SUCCESS;

  qError("start %" PRId64 ":%" PRId64 "th stbJoin, left:%d,%" PRIu64 " - right:%d,%" PRIu64, 
      rowIdx, pPrev->tableNum, *leftVg, *leftUid, *rightVg, *rightUid);

  updatePostJoinCurrTableInfo(&pInfo->stbJoin);
  
  if (pInfo->stbJoin.basic.batchFetch) {
    if (pPrev->leftHash) {
      code = pInfo->stbJoin.basic.srcScan[0] ? buildBatchTableScanOperatorParam(&pSrcParam0, 0, pPrev->leftHash) : buildBatchExchangeOperatorParam(&pSrcParam0, 0, pPrev->leftHash);
      if (TSDB_CODE_SUCCESS == code) {
        code = pInfo->stbJoin.basic.srcScan[1] ? buildBatchTableScanOperatorParam(&pSrcParam1, 1, pPrev->rightHash) : buildBatchExchangeOperatorParam(&pSrcParam1, 1, pPrev->rightHash);
      }
      if (TSDB_CODE_SUCCESS == code) {
        tSimpleHashCleanup(pPrev->leftHash);
        tSimpleHashCleanup(pPrev->rightHash);
        pPrev->leftHash = NULL;
        pPrev->rightHash = NULL;
      }
    }
  } else {
    code = pInfo->stbJoin.basic.srcScan[0] ? buildSingleTableScanOperatorParam(&pSrcParam0, 0, leftVg, leftUid) : buildExchangeOperatorParam(&pSrcParam0, 0, leftVg, leftUid);
    if (TSDB_CODE_SUCCESS == code) {
      code = pInfo->stbJoin.basic.srcScan[1] ? buildSingleTableScanOperatorParam(&pSrcParam1, 1, rightVg, rightUid) : buildExchangeOperatorParam(&pSrcParam1, 1, rightVg, rightUid);
    }
  }
  
  if (TSDB_CODE_SUCCESS == code) {
    code = buildGroupCacheOperatorParam(&pGcParam0, 0, *leftVg, *leftUid, pPost->leftNeedCache, pSrcParam0);
    pSrcParam0 = NULL;
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildGroupCacheOperatorParam(&pGcParam1, 1, *rightVg, *rightUid, pPost->rightNeedCache, pSrcParam1);
    pSrcParam1 = NULL;
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildMergeJoinOperatorParam(ppParam, pSrcParam0 ? true : false, pGcParam0, pGcParam1);
  }
  if (TSDB_CODE_SUCCESS != code) {
    if (pSrcParam0) {
      freeOperatorParam(pSrcParam0, OP_GET_PARAM);
    }
    if (pSrcParam1) {
      freeOperatorParam(pSrcParam1, OP_GET_PARAM);
    }
    if (pGcParam0) {
      freeOperatorParam(pGcParam0, OP_GET_PARAM);
    }
    if (pGcParam1) {
      freeOperatorParam(pGcParam1, OP_GET_PARAM);
    }
    if (*ppParam) {
      freeOperatorParam(*ppParam, OP_GET_PARAM);
      *ppParam = NULL;
    }
  }
  
  return code;
}


static void seqJoinLaunchNewRetrieveImpl(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SStbJoinDynCtrlInfo*       pStbJoin = (SStbJoinDynCtrlInfo*)&pInfo->stbJoin;
  SStbJoinPrevJoinCtx*       pPrev = &pStbJoin->ctx.prev;
  SStbJoinPostJoinCtx*       pPost = &pStbJoin->ctx.post;
  SOperatorParam*            pParam = NULL;
  int32_t                    code  = buildSeqStbJoinOperatorParam(pInfo, pPrev, pPost, &pParam);
  if (TSDB_CODE_SUCCESS != code) {
    pOperator->pTaskInfo->code = code;
    T_LONG_JMP(pOperator->pTaskInfo->env, pOperator->pTaskInfo->code);
  }

  qDebug("%s dynamic post task begin", GET_TASKID(pOperator->pTaskInfo));
  *ppRes = pOperator->pDownstream[1]->fpSet.getNextExtFn(pOperator->pDownstream[1], pParam);
  if (*ppRes) {
    pPost->isStarted = true;
    pStbJoin->execInfo.postBlkNum++;
    pStbJoin->execInfo.postBlkRows += (*ppRes)->info.rows;
    qDebug("%s join res block retrieved", GET_TASKID(pOperator->pTaskInfo));
  } else {
    qDebug("%s Empty join res block retrieved", GET_TASKID(pOperator->pTaskInfo));
  }
}


static int32_t notifySeqJoinTableCacheEnd(SOperatorInfo* pOperator, SStbJoinPostJoinCtx* pPost, bool leftTable) {
  SOperatorParam* pGcParam = NULL;
  SOperatorParam* pMergeJoinParam = NULL;
  int32_t         downstreamId = leftTable ? 0 : 1;
  int32_t         vgId = leftTable ? pPost->leftVgId : pPost->rightVgId;
  int64_t         uid = leftTable ? pPost->leftCurrUid : pPost->rightCurrUid;

  qError("notify table %" PRIu64 " in vgId %d downstreamId %d cache end", uid, vgId, downstreamId);

  int32_t code = buildGroupCacheNotifyOperatorParam(&pGcParam, downstreamId, vgId, uid);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  code = buildMergeJoinNotifyOperatorParam(&pMergeJoinParam, pGcParam, NULL);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  return optrDefaultNotifyFn(pOperator->pDownstream[1], pMergeJoinParam);
}

static void handleSeqJoinCurrRetrieveEnd(SOperatorInfo* pOperator, SStbJoinDynCtrlInfo*          pStbJoin) {
  SStbJoinPostJoinCtx* pPost = &pStbJoin->ctx.post;

  pPost->isStarted = false;
  
  if (pStbJoin->basic.batchFetch) {
    return;
  }
  
  if (pPost->leftNeedCache) {
    uint32_t* num = tSimpleHashGet(pStbJoin->ctx.prev.leftCache, &pPost->leftCurrUid, sizeof(pPost->leftCurrUid));
    if (num && --(*num) <= 0) {
      tSimpleHashRemove(pStbJoin->ctx.prev.leftCache, &pPost->leftCurrUid, sizeof(pPost->leftCurrUid));
      notifySeqJoinTableCacheEnd(pOperator, pPost, true);
    }
  }
  
  if (!pPost->rightNeedCache) {
    void* v = tSimpleHashGet(pStbJoin->ctx.prev.rightCache, &pPost->rightCurrUid, sizeof(pPost->rightCurrUid));
    if (NULL != v) {
      tSimpleHashRemove(pStbJoin->ctx.prev.rightCache, &pPost->rightCurrUid, sizeof(pPost->rightCurrUid));
      notifySeqJoinTableCacheEnd(pOperator, pPost, false);
    }
  }
}


static FORCE_INLINE void seqJoinContinueCurrRetrieve(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SStbJoinPostJoinCtx*       pPost = &pInfo->stbJoin.ctx.post;
  SStbJoinPrevJoinCtx*       pPrev = &pInfo->stbJoin.ctx.prev;

  if (!pPost->isStarted) {
    return;
  }
  
  qDebug("%s dynQueryCtrl continue to retrieve block from post op", GET_TASKID(pOperator->pTaskInfo));
  
  *ppRes = getNextBlockFromDownstream(pOperator, 1);
  if (NULL == *ppRes) {
    handleSeqJoinCurrRetrieveEnd(pOperator, &pInfo->stbJoin);
    pPrev->pListHead->readIdx++;
  } else {
    pInfo->stbJoin.execInfo.postBlkNum++;
    pInfo->stbJoin.execInfo.postBlkRows += (*ppRes)->info.rows;
    return;
  }
}

static FORCE_INLINE int32_t addToJoinVgroupHash(SSHashObj* pHash, void* pKey, int32_t keySize, void* pVal, int32_t valSize) {
  SArray** ppArray = tSimpleHashGet(pHash, pKey, keySize);
  if (NULL == ppArray) {
    SArray* pArray = taosArrayInit(10, valSize);
    if (NULL == pArray) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    if (NULL == taosArrayPush(pArray, pVal)) {
      taosArrayDestroy(pArray);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    if (tSimpleHashPut(pHash, pKey, keySize, &pArray, POINTER_BYTES)) {
      taosArrayDestroy(pArray);      
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    return TSDB_CODE_SUCCESS;
  }

  if (NULL == taosArrayPush(*ppArray, pVal)) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  
  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE int32_t addToJoinTableHash(SSHashObj* pHash, SSHashObj* pOnceHash, void* pKey, int32_t keySize) {
  uint32_t* pNum = tSimpleHashGet(pHash, pKey, keySize);
  if (NULL == pNum) {
    uint32_t n = 1;
    if (tSimpleHashPut(pHash, pKey, keySize, &n, sizeof(n))) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    if (tSimpleHashPut(pOnceHash, pKey, keySize, NULL, 0)) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    return TSDB_CODE_SUCCESS;
  }

  switch (*pNum) {
    case 0:
      break;
    case UINT32_MAX:
      *pNum = 0;
      break;
    default:
      if (1 == (*pNum)) {
        tSimpleHashRemove(pOnceHash, pKey, keySize);
      }
      (*pNum)++;
      break;
  }
  
  return TSDB_CODE_SUCCESS;
}


static void freeStbJoinTableList(SStbJoinTableList* pList) {
  if (NULL == pList) {
    return;
  }
  taosMemoryFree(pList->pLeftVg);
  taosMemoryFree(pList->pLeftUid);
  taosMemoryFree(pList->pRightVg);
  taosMemoryFree(pList->pRightUid);
  taosMemoryFree(pList);
}

static int32_t appendStbJoinTableList(SStbJoinPrevJoinCtx* pCtx, int64_t rows, int32_t* pLeftVg, int64_t* pLeftUid, int32_t* pRightVg, int64_t* pRightUid) {
  SStbJoinTableList* pNew = taosMemoryMalloc(sizeof(SStbJoinTableList));
  if (NULL == pNew) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pNew->pLeftVg = taosMemoryMalloc(rows * sizeof(*pLeftVg));
  pNew->pLeftUid = taosMemoryMalloc(rows * sizeof(*pLeftUid));
  pNew->pRightVg = taosMemoryMalloc(rows * sizeof(*pRightVg));
  pNew->pRightUid = taosMemoryMalloc(rows * sizeof(*pRightUid));
  if (NULL == pNew->pLeftVg || NULL == pNew->pLeftUid || NULL == pNew->pRightVg || NULL == pNew->pRightUid) {
    freeStbJoinTableList(pNew);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  memcpy(pNew->pLeftVg, pLeftVg, rows * sizeof(*pLeftVg));
  memcpy(pNew->pLeftUid, pLeftUid, rows * sizeof(*pLeftUid));
  memcpy(pNew->pRightVg, pRightVg, rows * sizeof(*pRightVg));
  memcpy(pNew->pRightUid, pRightUid, rows * sizeof(*pRightUid));

  pNew->readIdx = 0;
  pNew->uidNum = rows;
  pNew->pNext = NULL;
  
  if (pCtx->pListTail) {
    pCtx->pListTail->pNext = pNew;
    pCtx->pListTail = pNew;
  } else {
    pCtx->pListHead = pNew;
    pCtx->pListTail= pNew;
  }

  return TSDB_CODE_SUCCESS;
}

static void doBuildStbJoinTableHash(SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SStbJoinDynCtrlInfo*       pStbJoin = (SStbJoinDynCtrlInfo*)&pInfo->stbJoin;
  SColumnInfoData*           pVg0 = taosArrayGet(pBlock->pDataBlock, pStbJoin->basic.vgSlot[0]);
  SColumnInfoData*           pVg1 = taosArrayGet(pBlock->pDataBlock, pStbJoin->basic.vgSlot[1]);
  SColumnInfoData*           pUid0 = taosArrayGet(pBlock->pDataBlock, pStbJoin->basic.uidSlot[0]);
  SColumnInfoData*           pUid1 = taosArrayGet(pBlock->pDataBlock, pStbJoin->basic.uidSlot[1]);

  if (pStbJoin->basic.batchFetch) {
    for (int32_t i = 0; i < pBlock->info.rows; ++i) {
      int32_t* leftVg = (int32_t*)(pVg0->pData + pVg0->info.bytes * i);
      int64_t* leftUid = (int64_t*)(pUid0->pData + pUid0->info.bytes * i);
      int32_t* rightVg = (int32_t*)(pVg1->pData + pVg1->info.bytes * i);
      int64_t* rightUid = (int64_t*)(pUid1->pData + pUid1->info.bytes * i);

      code = addToJoinVgroupHash(pStbJoin->ctx.prev.leftHash, leftVg, sizeof(*leftVg), leftUid, sizeof(*leftUid));
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
      code = addToJoinVgroupHash(pStbJoin->ctx.prev.rightHash, rightVg, sizeof(*rightVg), rightUid, sizeof(*rightUid));
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    }
  } else {
    for (int32_t i = 0; i < pBlock->info.rows; ++i) {
      int64_t* leftUid = (int64_t*)(pUid0->pData + pUid0->info.bytes * i);
    
      code = addToJoinTableHash(pStbJoin->ctx.prev.leftCache, pStbJoin->ctx.prev.onceTable, leftUid, sizeof(*leftUid));
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = appendStbJoinTableList(&pStbJoin->ctx.prev, pBlock->info.rows, (int32_t*)pVg0->pData, (int64_t*)pUid0->pData, (int32_t*)pVg1->pData, (int64_t*)pUid1->pData);
    if (TSDB_CODE_SUCCESS == code) {
      pStbJoin->ctx.prev.tableNum += pBlock->info.rows;
    }
  }

  if (TSDB_CODE_SUCCESS != code) {
    pOperator->pTaskInfo->code = code;
    T_LONG_JMP(pOperator->pTaskInfo->env, pOperator->pTaskInfo->code);
  }
}


static void postProcessStbJoinTableHash(SOperatorInfo* pOperator) {
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SStbJoinDynCtrlInfo*       pStbJoin = (SStbJoinDynCtrlInfo*)&pInfo->stbJoin;

  if (pStbJoin->basic.batchFetch) {
    return;
  }

  if (tSimpleHashGetSize(pStbJoin->ctx.prev.leftCache) == tSimpleHashGetSize(pStbJoin->ctx.prev.onceTable)) {
    tSimpleHashClear(pStbJoin->ctx.prev.leftCache);
    return;
  }

  uint64_t* pUid = NULL;
  int32_t iter = 0;
  while (NULL != (pUid = tSimpleHashIterate(pStbJoin->ctx.prev.onceTable, pUid, &iter))) {
    tSimpleHashRemove(pStbJoin->ctx.prev.leftCache, pUid, sizeof(*pUid));
  }

  pStbJoin->execInfo.leftCacheNum = tSimpleHashGetSize(pStbJoin->ctx.prev.leftCache);
  qError("more than 1 ref build table num %" PRId64, (int64_t)tSimpleHashGetSize(pStbJoin->ctx.prev.leftCache));

  // debug only
  iter = 0;
  uint32_t* num = NULL;
  while (NULL != (num = tSimpleHashIterate(pStbJoin->ctx.prev.leftCache, num, &iter))) {
    ASSERT(*num > 1);
  }
}

static void buildStbJoinTableList(SOperatorInfo* pOperator) {
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SStbJoinDynCtrlInfo*       pStbJoin = (SStbJoinDynCtrlInfo*)&pInfo->stbJoin;

  while (true) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (NULL == pBlock) {
      break;
    }

    pStbJoin->execInfo.prevBlkNum++;
    pStbJoin->execInfo.prevBlkRows += pBlock->info.rows;
    
    doBuildStbJoinTableHash(pOperator, pBlock);
  }

  postProcessStbJoinTableHash(pOperator);

  pStbJoin->ctx.prev.joinBuild = true;
}

static void seqJoinLaunchNewRetrieve(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SStbJoinDynCtrlInfo*       pStbJoin = (SStbJoinDynCtrlInfo*)&pInfo->stbJoin;
  SStbJoinPrevJoinCtx*       pPrev = &pStbJoin->ctx.prev;
  SStbJoinTableList*         pNode = pPrev->pListHead;

  while (pNode) {
    if (pNode->readIdx >= pNode->uidNum) {
      pPrev->pListHead = pNode->pNext;
      freeStbJoinTableList(pNode);
      pNode = pPrev->pListHead;
      continue;
    }
    
    seqJoinLaunchNewRetrieveImpl(pOperator, ppRes);
    if (*ppRes) {
      return;
    }

    handleSeqJoinCurrRetrieveEnd(pOperator, pStbJoin);
    pPrev->pListHead->readIdx++;
  }

  *ppRes = NULL;
  setOperatorCompleted(pOperator);

  return;
}

static FORCE_INLINE SSDataBlock* seqStableJoinComposeRes(SStbJoinDynCtrlInfo*        pStbJoin, SSDataBlock* pBlock) {
  pBlock->info.id.blockId = pStbJoin->outputBlkId;
  return pBlock;
}


SSDataBlock* seqStableJoin(SOperatorInfo* pOperator) {
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SStbJoinDynCtrlInfo*       pStbJoin = (SStbJoinDynCtrlInfo*)&pInfo->stbJoin;
  SSDataBlock* pRes = NULL;

  if (pOperator->status == OP_EXEC_DONE) {
    return pRes;
  }

  int64_t st = 0;
  if (pOperator->cost.openCost == 0) {
    st = taosGetTimestampUs();
  }

  if (!pStbJoin->ctx.prev.joinBuild) {
    buildStbJoinTableList(pOperator);
    if (pStbJoin->execInfo.prevBlkRows <= 0) {
      setOperatorCompleted(pOperator);
      goto _return;
    }
  }

  seqJoinContinueCurrRetrieve(pOperator, &pRes);
  if (pRes) {
    goto _return;
  }
  
  seqJoinLaunchNewRetrieve(pOperator, &pRes);

_return:

  if (pOperator->cost.openCost == 0) {
    pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;
  }
  
  return pRes ? seqStableJoinComposeRes(pStbJoin, pRes) : NULL;
}

int32_t initSeqStbJoinTableHash(SStbJoinPrevJoinCtx* pPrev, bool batchFetch) {
  if (batchFetch) {
    pPrev->leftHash = tSimpleHashInit(20, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
    if (NULL == pPrev->leftHash) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    pPrev->rightHash = tSimpleHashInit(20, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
    if (NULL == pPrev->rightHash) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  } else {
    pPrev->leftCache = tSimpleHashInit(20, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    if (NULL == pPrev->leftCache) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    pPrev->rightCache = tSimpleHashInit(20, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    if (NULL == pPrev->rightCache) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    pPrev->onceTable = tSimpleHashInit(20, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    if (NULL == pPrev->onceTable) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return TSDB_CODE_SUCCESS;
}


SOperatorInfo* createDynQueryCtrlOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream,
                                           SDynQueryCtrlPhysiNode* pPhyciNode, SExecTaskInfo* pTaskInfo) {
  SDynQueryCtrlOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SDynQueryCtrlOperatorInfo));
  SOperatorInfo*     pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  __optr_fn_t        nextFp = NULL;

  int32_t code = TSDB_CODE_SUCCESS;
  if (pOperator == NULL || pInfo == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  pTaskInfo->dynamicTask = pPhyciNode->node.dynamicOp;

  code = appendDownstream(pOperator, pDownstream, numOfDownstream);
  if (TSDB_CODE_SUCCESS != code) {
    goto _error;
  }

  pInfo->qType = pPhyciNode->qType;
  switch (pInfo->qType) {
    case DYN_QTYPE_STB_HASH:
      memcpy(&pInfo->stbJoin.basic, &pPhyciNode->stbJoin, sizeof(pPhyciNode->stbJoin));
      pInfo->stbJoin.outputBlkId = pPhyciNode->node.pOutputDataBlockDesc->dataBlockId;
      code = initSeqStbJoinTableHash(&pInfo->stbJoin.ctx.prev, pInfo->stbJoin.basic.batchFetch);
      if (TSDB_CODE_SUCCESS != code) {
        goto _error;
      }
      nextFp = seqStableJoin;
      break;
    default:
      qError("unsupported dynamic query ctrl type: %d", pInfo->qType);
      code = TSDB_CODE_INVALID_PARA;
      goto _error;
  }
  
  setOperatorInfo(pOperator, "DynQueryCtrlOperator", QUERY_NODE_PHYSICAL_PLAN_DYN_QUERY_CTRL, false, OP_NOT_OPENED, pInfo, pTaskInfo);

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, nextFp, NULL, destroyDynQueryCtrlOperator, optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  return pOperator;

_error:
  if (pInfo != NULL) {
    destroyDynQueryCtrlOperator(pInfo);
  }

  taosMemoryFree(pOperator);
  pTaskInfo->code = code;
  return NULL;
}


