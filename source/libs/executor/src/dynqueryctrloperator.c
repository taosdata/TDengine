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


static void destroyDynQueryCtrlOperator(void* param) {
  SDynQueryCtrlOperatorInfo* pDyn = (SDynQueryCtrlOperatorInfo*)param;
  qError("dynQueryCtrl exec info, prevBlk:%" PRId64 ", prevRows:%" PRId64 ", postBlk:%" PRId64 ", postRows:%" PRId64, 
         pDyn->execInfo.prevBlkNum, pDyn->execInfo.prevBlkRows, pDyn->execInfo.postBlkNum, pDyn->execInfo.postBlkRows);

  if (pDyn->stbJoin.basic.batchFetch) {
    if (pDyn->stbJoin.ctx.prev.leftHash) {
      tSimpleHashSetFreeFp(pDyn->stbJoin.ctx.prev.leftHash, freeVgTableList);
      tSimpleHashCleanup(pDyn->stbJoin.ctx.prev.leftHash);
    }
    if (pDyn->stbJoin.ctx.prev.rightHash) {
      tSimpleHashSetFreeFp(pDyn->stbJoin.ctx.prev.rightHash, freeVgTableList);
      tSimpleHashCleanup(pDyn->stbJoin.ctx.prev.rightHash);
    }
  }
  
  if (pDyn->stbJoin.ctx.prev.tableTimes) {
    tSimpleHashCleanup(pDyn->stbJoin.ctx.prev.tableTimes);
  }
  if (pDyn->stbJoin.ctx.prev.onceTable) {
    tSimpleHashCleanup(pDyn->stbJoin.ctx.prev.onceTable);
  }

  taosMemoryFreeClear(param);
}

static FORCE_INLINE int32_t buildGroupCacheOperatorParam(SOperatorParam** ppRes, int32_t downstreamIdx, int32_t vgId, int64_t tbUid, bool needCache, SOperatorParam* pChild) {
  *ppRes = taosMemoryMalloc(sizeof(SOperatorParam));
  if (NULL == *ppRes) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (pChild) {
    (*ppRes)->pChildren = taosArrayInit(1, POINTER_BYTES);
    if (NULL == *ppRes) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    if (NULL == taosArrayPush((*ppRes)->pChildren, &pChild)) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  } else {
    (*ppRes)->pChildren = NULL;
  }

  SGcOperatorParam* pGc = taosMemoryMalloc(sizeof(SGcOperatorParam));
  if (NULL == pGc) {
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

static FORCE_INLINE int32_t buildExchangeOperatorParam(SOperatorParam** ppRes, int32_t downstreamIdx, int32_t* pVgId, int64_t* pUid) {
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


static FORCE_INLINE int32_t buildBatchExchangeOperatorParam(SOperatorParam** ppRes, int32_t downstreamIdx, SSHashObj* pVg) {
  *ppRes = taosMemoryMalloc(sizeof(SOperatorParam));
  if (NULL == *ppRes) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  (*ppRes)->pChildren = NULL;
  
  SExchangeOperatorBatchParam* pExc = taosMemoryMalloc(sizeof(SExchangeOperatorBatchParam));
  if (NULL == pExc) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pExc->multiParams = true;
  pExc->pBatchs = tSimpleHashInit(tSimpleHashGetSize(pVg), taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  if (NULL == pExc->pBatchs) {
    taosMemoryFree(pExc);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  
  SExchangeOperatorBasicParam basic;
  basic.srcOpType = QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN;

  int32_t iter = 0;
  void* p = NULL;
  while (p = tSimpleHashIterate(pVg, p, &iter)) {
    int32_t* pVgId = tSimpleHashGetKey(p, NULL);
    SArray* pUidList = *(SArray**)p;
    basic.vgId = *pVgId;
    basic.uidList = pUidList;
    basic.tableSeq = false;
    
    tSimpleHashPut(pExc->pBatchs, pVgId, sizeof(*pVgId), &basic, sizeof(basic));   

    qTrace("build downstreamIdx %d batch scan, vgId:%d, uidNum:%" PRId64, downstreamIdx, *pVgId, (int64_t)taosArrayGetSize(pUidList));
  }

  (*ppRes)->opType = QUERY_NODE_PHYSICAL_PLAN_EXCHANGE;
  (*ppRes)->downstreamIdx = downstreamIdx;
  (*ppRes)->value = pExc;
  
  return TSDB_CODE_SUCCESS;
}


static FORCE_INLINE int32_t buildMergeJoinOperatorParam(SOperatorParam** ppRes, bool initParam, SOperatorParam* pChild0, SOperatorParam* pChild1) {
  *ppRes = taosMemoryMalloc(sizeof(SOperatorParam));
  if (NULL == *ppRes) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  (*ppRes)->pChildren = taosArrayInit(2, POINTER_BYTES);
  if (NULL == *ppRes) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (NULL == taosArrayPush((*ppRes)->pChildren, &pChild0)) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (NULL == taosArrayPush((*ppRes)->pChildren, &pChild1)) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  
  SSortMergeJoinOperatorParam* pJoin = taosMemoryMalloc(sizeof(SSortMergeJoinOperatorParam));
  if (NULL == pJoin) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pJoin->initParam = initParam;
  
  (*ppRes)->opType = QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN;
  (*ppRes)->value = pJoin;

  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE bool tableNeedCache(int64_t uid, SStbJoinPrevJoinCtx* pPrev, SStbJoinPostJoinCtx* pPost, bool rightTable, bool batchFetch) {
  if (batchFetch) {
    return true;
  }
  
  if (rightTable) {
    return pPost->rightCurrUid == pPost->rightNextUid;
  }

  uint32_t* num = tSimpleHashGet(pPrev->tableTimes, &uid, sizeof(uid));

  return (NULL == num) ? false : true;
}

static int32_t buildSeqStbJoinOperatorParam(SDynQueryCtrlOperatorInfo* pInfo, SStbJoinPrevJoinCtx* pPrev, SStbJoinPostJoinCtx* pPost, SOperatorParam** ppParam) {
  int64_t                     rowIdx = pPrev->pListHead->readIdx;
  SOperatorParam*             pExcParam0 = NULL;
  SOperatorParam*             pExcParam1 = NULL;
  SOperatorParam*             pGcParam0 = NULL;
  SOperatorParam*             pGcParam1 = NULL;  
  int32_t*                    leftVg = pPrev->pListHead->pLeftVg + rowIdx;
  int64_t*                    leftUid = pPrev->pListHead->pLeftUid + rowIdx;
  int32_t*                    rightVg = pPrev->pListHead->pRightVg + rowIdx;
  int64_t*                    rightUid = pPrev->pListHead->pRightUid + rowIdx;
  int32_t                     code = TSDB_CODE_SUCCESS;

  qError("start %" PRId64 ":%" PRId64 "th stbJoin, left:%d,%" PRIu64 " - right:%d,%" PRIu64, 
      rowIdx, pPrev->tableNum, *leftVg, *leftUid, *rightVg, *rightUid);

  if (pInfo->stbJoin.basic.batchFetch) {
    if (pPrev->leftHash) {
      code = buildBatchExchangeOperatorParam(&pExcParam0, 0, pPrev->leftHash);
      if (TSDB_CODE_SUCCESS == code) {
        code = buildBatchExchangeOperatorParam(&pExcParam1, 1, pPrev->rightHash);
      }
      if (TSDB_CODE_SUCCESS == code) {
        tSimpleHashCleanup(pPrev->leftHash);
        tSimpleHashCleanup(pPrev->rightHash);
        pPrev->leftHash = NULL;
        pPrev->rightHash = NULL;
      }
    }
  } else {
    code = buildExchangeOperatorParam(&pExcParam0, 0, leftVg, leftUid);
    if (TSDB_CODE_SUCCESS == code) {
      code = buildExchangeOperatorParam(&pExcParam1, 1, rightVg, rightUid);
    }
  }
  
  if (TSDB_CODE_SUCCESS == code) {
    code = buildGroupCacheOperatorParam(&pGcParam0, 0, *leftVg, *leftUid, tableNeedCache(*leftUid, pPrev, pPost, false, pInfo->stbJoin.basic.batchFetch), pExcParam0);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildGroupCacheOperatorParam(&pGcParam1, 1, *rightVg, *rightUid, tableNeedCache(*rightUid, pPrev, pPost, true, pInfo->stbJoin.basic.batchFetch), pExcParam1);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildMergeJoinOperatorParam(ppParam, pExcParam0 ? true : false, pGcParam0, pGcParam1);
  }
  return code;
}


static void seqJoinLaunchPostJoin(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
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

  qError("dynamic post task begin");
  *ppRes = pOperator->pDownstream[1]->fpSet.getNextExtFn(pOperator->pDownstream[1], pParam);
  if (*ppRes) {
    pPost->isStarted = true;
    pInfo->execInfo.postBlkNum++;
    pInfo->execInfo.postBlkRows += (*ppRes)->info.rows;
    qError("join res block retrieved");
  } else {
    qError("Empty join res block retrieved");
  }
}

static FORCE_INLINE void seqJoinContinueRetrieve(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SStbJoinPostJoinCtx*       pPost = &pInfo->stbJoin.ctx.post;

  if (!pPost->isStarted) {
    return;
  }
  
  qDebug("%s dynQueryCtrl continue to retrieve block from post op", GET_TASKID(pOperator->pTaskInfo));
  
  *ppRes = getNextBlockFromDownstream(pOperator, 1);
  if (NULL == *ppRes) {
    pPost->isStarted = false;
  } else {
    pInfo->execInfo.postBlkNum++;
    pInfo->execInfo.postBlkRows += (*ppRes)->info.rows;
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
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    if (tSimpleHashPut(pHash, pKey, keySize, &pArray, POINTER_BYTES)) {
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

static int32_t appendStbJoinTableList(SStbJoinTableList** ppHead, int64_t rows, int32_t* pLeftVg, int64_t* pLeftUid, int32_t* pRightVg, int64_t* pRightUid) {
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
  
  if (*ppHead) {
    pNew->pNext = *ppHead;
  } else {
    pNew->pNext = NULL;
  }

  *ppHead = pNew;

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
  }
  
  for (int32_t i = 0; i < pBlock->info.rows; ++i) {
    int64_t* leftUid = (int64_t*)(pUid0->pData + pUid0->info.bytes * i);
  
    code = addToJoinTableHash(pStbJoin->ctx.prev.tableTimes, pStbJoin->ctx.prev.onceTable, leftUid, sizeof(*leftUid));
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = appendStbJoinTableList(&pStbJoin->ctx.prev.pListHead, pBlock->info.rows, (int32_t*)pVg0->pData, (int64_t*)pUid0->pData, (int32_t*)pVg1->pData, (int64_t*)pUid1->pData);
    if (TSDB_CODE_SUCCESS == code) {
      pStbJoin->ctx.prev.tableNum += pBlock->info.rows;
    }
  }

  if (TSDB_CODE_SUCCESS != code) {
    pOperator->pTaskInfo->code = code;
    T_LONG_JMP(pOperator->pTaskInfo->env, pOperator->pTaskInfo->code);
  }
}

static void updatePostJoinRightTableUid(SStbJoinDynCtrlInfo*         pStbJoin) {
  SStbJoinPrevJoinCtx*       pPrev = &pStbJoin->ctx.prev;
  SStbJoinPostJoinCtx*       pPost = &pStbJoin->ctx.post;
  SStbJoinTableList*         pNode = pPrev->pListHead;
  int64_t*                   rightUid = pNode->pRightUid;
  int64_t                    readIdx = pNode->readIdx + 1;

  if (pPost->rightNextUid) {
    pPost->rightCurrUid = pPost->rightNextUid;
  } else {
    pPost->rightCurrUid = *rightUid;
  }

  while (true) {
    if (readIdx < pNode->uidNum) {
      pPost->rightNextUid = *(rightUid + readIdx);
      return;
    }
    
    pNode = pNode->pNext;
    if (NULL == pNode) {
      pPost->rightNextUid = 0;
      return;
    }
    
    rightUid = pNode->pRightUid;
    readIdx = 0;
  }
}

static void postProcessStbJoinTableHash(SOperatorInfo* pOperator) {
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SStbJoinDynCtrlInfo*       pStbJoin = (SStbJoinDynCtrlInfo*)&pInfo->stbJoin;

  updatePostJoinRightTableUid(pStbJoin);

  if (tSimpleHashGetSize(pStbJoin->ctx.prev.tableTimes) == tSimpleHashGetSize(pStbJoin->ctx.prev.onceTable)) {
    tSimpleHashClear(pStbJoin->ctx.prev.tableTimes);
    return;
  }

  uint64_t* pUid = NULL;
  int32_t iter = 0;
  while (pUid = tSimpleHashIterate(pStbJoin->ctx.prev.onceTable, pUid, &iter)) {
    tSimpleHashRemove(pStbJoin->ctx.prev.tableTimes, pUid, sizeof(*pUid));
  }

  qError("more than 1 ref build table num %" PRId64, (int64_t)tSimpleHashGetSize(pStbJoin->ctx.prev.tableTimes));

  // debug only
  iter = 0;
  uint32_t* num = NULL;
  while (num = tSimpleHashIterate(pStbJoin->ctx.prev.tableTimes, num, &iter)) {
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

    pInfo->execInfo.prevBlkNum++;
    pInfo->execInfo.prevBlkRows += pBlock->info.rows;
    
    doBuildStbJoinTableHash(pOperator, pBlock);
  }

  postProcessStbJoinTableHash(pOperator);

  pStbJoin->ctx.prev.joinBuild = true;
}

static void seqJoinLaunchRetrieve(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
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
    
    seqJoinLaunchPostJoin(pOperator, ppRes);
    pPrev->pListHead->readIdx++;
    
    if (*ppRes) {
      return;
    }
  }

  *ppRes = NULL;
  return;
}

SSDataBlock* seqStableJoin(SOperatorInfo* pOperator) {
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SStbJoinDynCtrlInfo*       pStbJoin = (SStbJoinDynCtrlInfo*)&pInfo->stbJoin;
  SSDataBlock* pRes = NULL;

  if (pOperator->status == OP_EXEC_DONE) {
    return pRes;
  }

  if (!pStbJoin->ctx.prev.joinBuild) {
    buildStbJoinTableList(pOperator);
    if (pInfo->execInfo.prevBlkRows <= 0) {
      pOperator->status = OP_EXEC_DONE;
      return NULL;
    }
  }

  seqJoinContinueRetrieve(pOperator, &pRes);
  if (pRes) {
    return pRes;
  }
  
  seqJoinLaunchRetrieve(pOperator, &pRes);
  return pRes;
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
  }
  
  pPrev->tableTimes = tSimpleHashInit(20, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  if (NULL == pPrev->tableTimes) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pPrev->onceTable = tSimpleHashInit(20, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  if (NULL == pPrev->onceTable) {
    return TSDB_CODE_OUT_OF_MEMORY;
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

  code = appendDownstream(pOperator, pDownstream, numOfDownstream);
  if (TSDB_CODE_SUCCESS != code) {
    goto _error;
  }

  pInfo->qType = pPhyciNode->qType;
  switch (pInfo->qType) {
    case DYN_QTYPE_STB_HASH:
      memcpy(&pInfo->stbJoin.basic, &pPhyciNode->stbJoin, sizeof(pPhyciNode->stbJoin));
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


