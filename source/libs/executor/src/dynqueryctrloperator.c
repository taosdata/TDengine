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
#include "nodes.h"
#include "operator.h"
#include "os.h"
#include "plannodes.h"
#include "query.h"
#include "querynodes.h"
#include "querytask.h"
#include "tarray.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "thash.h"
#include "tmsg.h"
#include "trpc.h"
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
  qDebug("dynQueryCtrl exec info, prevBlk:%" PRId64 ", prevRows:%" PRId64 ", postBlk:%" PRId64 ", postRows:%" PRId64 ", leftCacheNum:%" PRId64 ", rightCacheNum:%" PRId64, 
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

void destroyOrgTbInfo(void *info) {
  SOrgTbInfo *pOrgTbInfo = (SOrgTbInfo *)info;
  if (pOrgTbInfo) {
    taosArrayDestroy(pOrgTbInfo->colMap);
  }
}

void freeUseDbOutput(void* pOutput) {
  SUseDbOutput *pOut = *(SUseDbOutput**)pOutput;
  if (NULL == pOutput) {
    return;
  }

  if (pOut->dbVgroup) {
    freeVgInfo(pOut->dbVgroup);
  }
  taosMemFree(pOut);
}

static void destroyVtbScanDynCtrlInfo(SVtbScanDynCtrlInfo* pVtbScan) {
  if (pVtbScan->dbName) {
    taosMemoryFreeClear(pVtbScan->dbName);
  }
  if (pVtbScan->childTableList) {
    taosArrayDestroy(pVtbScan->childTableList);
  }
  if (pVtbScan->readColList) {
    taosArrayDestroy(pVtbScan->readColList);
  }
  if (pVtbScan->dbVgInfoMap) {
    taosHashSetFreeFp(pVtbScan->dbVgInfoMap, freeUseDbOutput);
    taosHashCleanup(pVtbScan->dbVgInfoMap);
  }
  if (pVtbScan->orgTbVgColMap) {
    taosHashSetFreeFp(pVtbScan->orgTbVgColMap, destroyOrgTbInfo);
    taosHashCleanup(pVtbScan->orgTbVgColMap);
  }
  if (pVtbScan->pRsp) {
    tFreeSUsedbRsp(pVtbScan->pRsp);
    taosMemoryFreeClear(pVtbScan->pRsp);
  }
}

static void destroyDynQueryCtrlOperator(void* param) {
  SDynQueryCtrlOperatorInfo* pDyn = (SDynQueryCtrlOperatorInfo*)param;

  switch (pDyn->qType) {
    case DYN_QTYPE_STB_HASH:
      destroyStbJoinDynCtrlInfo(&pDyn->stbJoin);
      break;
    case DYN_QTYPE_VTB_SCAN:
      destroyVtbScanDynCtrlInfo(&pDyn->vtbScan);
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

static int32_t updatePostJoinCurrTableInfo(SStbJoinDynCtrlInfo*          pStbJoin) {
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

  if (!pStbJoin->basic.batchFetch && pPost->rightNeedCache && rightPrevUid != pPost->rightCurrUid) {
    QRY_ERR_RET(tSimpleHashPut(pPrev->rightCache, &pPost->rightCurrUid, sizeof(pPost->rightCurrUid), NULL, 0));
    pStbJoin->execInfo.rightCacheNum++;
  }  

  return TSDB_CODE_SUCCESS;
}


static int32_t buildGroupCacheOperatorParam(SOperatorParam** ppRes, int32_t downstreamIdx, int32_t vgId, int64_t tbUid, bool needCache, SOperatorParam* pChild) {
  int32_t code = TSDB_CODE_SUCCESS;
  *ppRes = taosMemoryMalloc(sizeof(SOperatorParam));
  if (NULL == *ppRes) {
    code = terrno;
    freeOperatorParam(pChild, OP_GET_PARAM);
    return code;
  }
  if (pChild) {
    (*ppRes)->pChildren = taosArrayInit(1, POINTER_BYTES);
    if (NULL == (*ppRes)->pChildren) {
      code = terrno;
      freeOperatorParam(pChild, OP_GET_PARAM);
      freeOperatorParam(*ppRes, OP_GET_PARAM);
      *ppRes = NULL;
      return code;
    }
    if (NULL == taosArrayPush((*ppRes)->pChildren, &pChild)) {
      code = terrno;
      freeOperatorParam(pChild, OP_GET_PARAM);
      freeOperatorParam(*ppRes, OP_GET_PARAM);
      *ppRes = NULL;
      return code;
    }
  } else {
    (*ppRes)->pChildren = NULL;
  }

  SGcOperatorParam* pGc = taosMemoryMalloc(sizeof(SGcOperatorParam));
  if (NULL == pGc) {
    code = terrno;
    freeOperatorParam(*ppRes, OP_GET_PARAM);
    *ppRes = NULL;
    return code;
  }

  pGc->sessionId = atomic_add_fetch_64(&gSessionId, 1);
  pGc->downstreamIdx = downstreamIdx;
  pGc->vgId = vgId;
  pGc->tbUid = tbUid;
  pGc->needCache = needCache;

  (*ppRes)->opType = QUERY_NODE_PHYSICAL_PLAN_GROUP_CACHE;
  (*ppRes)->downstreamIdx = downstreamIdx;
  (*ppRes)->value = pGc;
  (*ppRes)->reUse = false;

  return TSDB_CODE_SUCCESS;
}


static int32_t buildGroupCacheNotifyOperatorParam(SOperatorParam** ppRes, int32_t downstreamIdx, int32_t vgId, int64_t tbUid) {
  int32_t code = TSDB_CODE_SUCCESS;
  *ppRes = taosMemoryMalloc(sizeof(SOperatorParam));
  if (NULL == *ppRes) {
    return terrno;
  }
  (*ppRes)->pChildren = NULL;

  SGcNotifyOperatorParam* pGc = taosMemoryMalloc(sizeof(SGcNotifyOperatorParam));
  if (NULL == pGc) {
    code = terrno;
    freeOperatorParam(*ppRes, OP_NOTIFY_PARAM);
    return code;
  }

  pGc->downstreamIdx = downstreamIdx;
  pGc->vgId = vgId;
  pGc->tbUid = tbUid;

  (*ppRes)->opType = QUERY_NODE_PHYSICAL_PLAN_GROUP_CACHE;
  (*ppRes)->downstreamIdx = downstreamIdx;
  (*ppRes)->value = pGc;
  (*ppRes)->reUse = false;

  return TSDB_CODE_SUCCESS;
}


static int32_t buildExchangeOperatorParam(SOperatorParam** ppRes, int32_t downstreamIdx, const int32_t* pVgId, int64_t* pUid) {
  *ppRes = taosMemoryMalloc(sizeof(SOperatorParam));
  if (NULL == *ppRes) {
    return terrno;
  }
  (*ppRes)->pChildren = NULL;
  
  SExchangeOperatorParam* pExc = taosMemoryMalloc(sizeof(SExchangeOperatorParam));
  if (NULL == pExc) {
    return terrno;
  }

  pExc->multiParams = false;
  pExc->basic.vgId = *pVgId;
  pExc->basic.tableSeq = true;
  pExc->basic.isVtbRefScan = false;
  pExc->basic.srcOpType = QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN;
  pExc->basic.colMap = NULL;
  pExc->basic.uidList = taosArrayInit(1, sizeof(int64_t));
  if (NULL == pExc->basic.uidList) {
    taosMemoryFree(pExc);
    return terrno;
  }
  if (NULL == taosArrayPush(pExc->basic.uidList, pUid)) {
    taosArrayDestroy(pExc->basic.uidList);
    taosMemoryFree(pExc);
    return terrno;
  }

  (*ppRes)->opType = QUERY_NODE_PHYSICAL_PLAN_EXCHANGE;
  (*ppRes)->downstreamIdx = downstreamIdx;
  (*ppRes)->value = pExc;
  (*ppRes)->reUse = false;

  return TSDB_CODE_SUCCESS;
}

static int32_t buildBatchExchangeOperatorParam(SOperatorParam** ppRes, int32_t downstreamIdx, SSHashObj* pVg) {
  *ppRes = taosMemoryMalloc(sizeof(SOperatorParam));
  if (NULL == *ppRes) {
    return terrno;
  }
  (*ppRes)->pChildren = NULL;
  
  SExchangeOperatorBatchParam* pExc = taosMemoryMalloc(sizeof(SExchangeOperatorBatchParam));
  if (NULL == pExc) {
    taosMemoryFreeClear(*ppRes);
    return terrno;
  }

  pExc->multiParams = true;
  pExc->pBatchs = tSimpleHashInit(tSimpleHashGetSize(pVg), taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  if (NULL == pExc->pBatchs) {
    taosMemoryFree(pExc);
    taosMemoryFreeClear(*ppRes);
    return terrno;
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
    basic.colMap = NULL;
    basic.tableSeq = false;
    basic.isVtbRefScan = false;
    
    QRY_ERR_RET(tSimpleHashPut(pExc->pBatchs, pVgId, sizeof(*pVgId), &basic, sizeof(basic)));   

    qTrace("build downstreamIdx %d batch scan, vgId:%d, uidNum:%" PRId64, downstreamIdx, *pVgId, (int64_t)taosArrayGetSize(pUidList));
    *(SArray**)p = NULL;
  }

  (*ppRes)->opType = QUERY_NODE_PHYSICAL_PLAN_EXCHANGE;
  (*ppRes)->downstreamIdx = downstreamIdx;
  (*ppRes)->value = pExc;
  (*ppRes)->reUse = false;

  return TSDB_CODE_SUCCESS;
}

static int32_t buildExchangeOperatorParamForVScan(SOperatorParam** ppRes, int32_t downstreamIdx, SOrgTbInfo* pMap) {
  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      lino = 0;
  SExchangeOperatorParam*      pExc = NULL;
  SExchangeOperatorBasicParam* basic = NULL;

  *ppRes = taosMemoryMalloc(sizeof(SOperatorParam));
  QUERY_CHECK_NULL(*ppRes, code, lino, _return, terrno);
  (*ppRes)->pChildren = NULL;

  pExc = taosMemoryMalloc(sizeof(SExchangeOperatorParam));
  QUERY_CHECK_NULL(pExc, code, lino, _return, terrno);

  pExc->multiParams = false;

  basic = &pExc->basic;
  basic->srcOpType = QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN;

  basic->vgId = pMap->vgId;
  basic->tableSeq = false;
  basic->isVtbRefScan = true;
  basic->colMap = taosMemoryMalloc(sizeof(SOrgTbInfo));
  QUERY_CHECK_NULL(basic->colMap, code, lino, _return, terrno);
  basic->colMap->vgId = pMap->vgId;
  tstrncpy(basic->colMap->tbName, pMap->tbName, TSDB_TABLE_FNAME_LEN);
  basic->colMap->colMap = taosArrayDup(pMap->colMap, NULL);
  QUERY_CHECK_NULL(basic->colMap->colMap, code, lino, _return, terrno);

  basic->uidList = taosArrayInit(1, sizeof(int64_t));
  QUERY_CHECK_NULL(basic->uidList, code, lino, _return, terrno);

  (*ppRes)->opType = QUERY_NODE_PHYSICAL_PLAN_EXCHANGE;
  (*ppRes)->downstreamIdx = downstreamIdx;
  (*ppRes)->value = pExc;
  (*ppRes)->reUse = true;

  return TSDB_CODE_SUCCESS;

_return:
  qError("failed to build exchange operator param for vscan, code:%d", code);
  taosMemoryFreeClear(*ppRes);
  if (basic) {
    if (basic->colMap) {
      taosArrayDestroy(basic->colMap->colMap);
      taosMemoryFreeClear(basic->colMap);
    }
    if (basic->uidList) {
      taosArrayDestroy(basic->uidList);
    }
    taosMemoryFreeClear(basic);
  }
  taosMemoryFreeClear(pExc);
  return code;
}

static int32_t buildMergeJoinOperatorParam(SOperatorParam** ppRes, bool initParam, SOperatorParam** ppChild0, SOperatorParam** ppChild1) {
  int32_t code = TSDB_CODE_SUCCESS;
  *ppRes = taosMemoryMalloc(sizeof(SOperatorParam));
  if (NULL == *ppRes) {
    code = terrno;
    return code;
  }
  (*ppRes)->pChildren = taosArrayInit(2, POINTER_BYTES);
  if (NULL == (*ppRes)->pChildren) {
    code = terrno;
    freeOperatorParam(*ppRes, OP_GET_PARAM);
    *ppRes = NULL;
    return code;
  }
  if (NULL == taosArrayPush((*ppRes)->pChildren, ppChild0)) {
    code = terrno;
    freeOperatorParam(*ppRes, OP_GET_PARAM);
    *ppRes = NULL;
    return code;
  }
  *ppChild0 = NULL;
  if (NULL == taosArrayPush((*ppRes)->pChildren, ppChild1)) {
    code = terrno;
    freeOperatorParam(*ppRes, OP_GET_PARAM);
    *ppRes = NULL;
    return code;
  }
  *ppChild1 = NULL;
  
  SSortMergeJoinOperatorParam* pJoin = taosMemoryMalloc(sizeof(SSortMergeJoinOperatorParam));
  if (NULL == pJoin) {
    code = terrno;
    freeOperatorParam(*ppRes, OP_GET_PARAM);
    *ppRes = NULL;
    return code;
  }

  pJoin->initDownstream = initParam;
  
  (*ppRes)->opType = QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN;
  (*ppRes)->value = pJoin;
  (*ppRes)->reUse = false;

  return TSDB_CODE_SUCCESS;
}

static int32_t buildMergeJoinNotifyOperatorParam(SOperatorParam** ppRes, SOperatorParam* pChild0, SOperatorParam* pChild1) {
  int32_t code = TSDB_CODE_SUCCESS;
  *ppRes = taosMemoryMalloc(sizeof(SOperatorParam));
  if (NULL == *ppRes) {
    code = terrno;
    freeOperatorParam(pChild0, OP_NOTIFY_PARAM);
    freeOperatorParam(pChild1, OP_NOTIFY_PARAM);
    return code;
  }
  (*ppRes)->pChildren = taosArrayInit(2, POINTER_BYTES);
  if (NULL == *ppRes) {
    code = terrno;
    taosMemoryFreeClear(*ppRes);
    freeOperatorParam(pChild0, OP_NOTIFY_PARAM);
    freeOperatorParam(pChild1, OP_NOTIFY_PARAM);
    return code;
  }
  if (pChild0 && NULL == taosArrayPush((*ppRes)->pChildren, &pChild0)) {
    code = terrno;
    freeOperatorParam(*ppRes, OP_NOTIFY_PARAM);
    freeOperatorParam(pChild0, OP_NOTIFY_PARAM);
    freeOperatorParam(pChild1, OP_NOTIFY_PARAM);
    *ppRes = NULL;
    return code;
  }
  if (pChild1 && NULL == taosArrayPush((*ppRes)->pChildren, &pChild1)) {
    code = terrno;
    freeOperatorParam(*ppRes, OP_NOTIFY_PARAM);
    freeOperatorParam(pChild1, OP_NOTIFY_PARAM);
    *ppRes = NULL;
    return code;
  }
  
  (*ppRes)->opType = QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN;
  (*ppRes)->value = NULL;
  (*ppRes)->reUse = false;

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
    return terrno;
  }
  if (NULL == taosArrayPush(pUidList, pUid)) {
    return terrno;
  }

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

  qDebug("start %" PRId64 ":%" PRId64 "th stbJoin, left:%d,%" PRIu64 " - right:%d,%" PRIu64, 
      rowIdx, pPrev->tableNum, *leftVg, *leftUid, *rightVg, *rightUid);

  QRY_ERR_RET(updatePostJoinCurrTableInfo(&pInfo->stbJoin));
  
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

  bool initParam = pSrcParam0 ? true : false;
  if (TSDB_CODE_SUCCESS == code) {
    code = buildGroupCacheOperatorParam(&pGcParam0, 0, *leftVg, *leftUid, pPost->leftNeedCache, pSrcParam0);
    pSrcParam0 = NULL;
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildGroupCacheOperatorParam(&pGcParam1, 1, *rightVg, *rightUid, pPost->rightNeedCache, pSrcParam1);
    pSrcParam1 = NULL;
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildMergeJoinOperatorParam(ppParam, initParam, &pGcParam0, &pGcParam1);
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
  code = pOperator->pDownstream[1]->fpSet.getNextExtFn(pOperator->pDownstream[1], pParam, ppRes);
  if (*ppRes && (code == 0)) {
    code = blockDataCheck(*ppRes);
    if (code) {
      qError("Invalid block data, blockDataCheck failed, error:%s", tstrerror(code));
      pOperator->pTaskInfo->code = code;
      T_LONG_JMP(pOperator->pTaskInfo->env, pOperator->pTaskInfo->code);
    }
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

  qDebug("notify table %" PRIu64 " in vgId %d downstreamId %d cache end", uid, vgId, downstreamId);

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

static int32_t handleSeqJoinCurrRetrieveEnd(SOperatorInfo* pOperator, SStbJoinDynCtrlInfo*          pStbJoin) {
  SStbJoinPostJoinCtx* pPost = &pStbJoin->ctx.post;
  int32_t code = 0;
  
  pPost->isStarted = false;
  
  if (pStbJoin->basic.batchFetch) {
    return TSDB_CODE_SUCCESS;
  }
  
  if (pPost->leftNeedCache) {
    uint32_t* num = tSimpleHashGet(pStbJoin->ctx.prev.leftCache, &pPost->leftCurrUid, sizeof(pPost->leftCurrUid));
    if (num && --(*num) <= 0) {
      code = tSimpleHashRemove(pStbJoin->ctx.prev.leftCache, &pPost->leftCurrUid, sizeof(pPost->leftCurrUid));
      if (code) {
        qError("tSimpleHashRemove leftCurrUid %" PRId64 " from leftCache failed, error:%s", pPost->leftCurrUid, tstrerror(code));
        QRY_ERR_RET(code);
      }
      QRY_ERR_RET(notifySeqJoinTableCacheEnd(pOperator, pPost, true));
    }
  }
  
  if (!pPost->rightNeedCache) {
    void* v = tSimpleHashGet(pStbJoin->ctx.prev.rightCache, &pPost->rightCurrUid, sizeof(pPost->rightCurrUid));
    if (NULL != v) {
      code = tSimpleHashRemove(pStbJoin->ctx.prev.rightCache, &pPost->rightCurrUid, sizeof(pPost->rightCurrUid));
      if (code) {
        qError("tSimpleHashRemove rightCurrUid %" PRId64 " from rightCache failed, error:%s", pPost->rightCurrUid, tstrerror(code));
        QRY_ERR_RET(code);
      }
      QRY_ERR_RET(notifySeqJoinTableCacheEnd(pOperator, pPost, false));
    }
  }

  return TSDB_CODE_SUCCESS;
}


static FORCE_INLINE int32_t seqJoinContinueCurrRetrieve(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SStbJoinPostJoinCtx*       pPost = &pInfo->stbJoin.ctx.post;
  SStbJoinPrevJoinCtx*       pPrev = &pInfo->stbJoin.ctx.prev;

  if (!pPost->isStarted) {
    return TSDB_CODE_SUCCESS;
  }
  
  qDebug("%s dynQueryCtrl continue to retrieve block from post op", GET_TASKID(pOperator->pTaskInfo));
  
  *ppRes = getNextBlockFromDownstream(pOperator, 1);
  if (NULL == *ppRes) {
    QRY_ERR_RET(handleSeqJoinCurrRetrieveEnd(pOperator, &pInfo->stbJoin));
    pPrev->pListHead->readIdx++;
  } else {
    pInfo->stbJoin.execInfo.postBlkNum++;
    pInfo->stbJoin.execInfo.postBlkRows += (*ppRes)->info.rows;
  }

  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE int32_t addToJoinVgroupHash(SSHashObj* pHash, void* pKey, int32_t keySize, void* pVal, int32_t valSize) {
  SArray** ppArray = tSimpleHashGet(pHash, pKey, keySize);
  if (NULL == ppArray) {
    SArray* pArray = taosArrayInit(10, valSize);
    if (NULL == pArray) {
      return terrno;
    }
    if (NULL == taosArrayPush(pArray, pVal)) {
      taosArrayDestroy(pArray);
      return terrno;
    }
    if (tSimpleHashPut(pHash, pKey, keySize, &pArray, POINTER_BYTES)) {
      taosArrayDestroy(pArray);      
      return terrno;
    }
    return TSDB_CODE_SUCCESS;
  }

  if (NULL == taosArrayPush(*ppArray, pVal)) {
    return terrno;
  }
  
  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE int32_t addToJoinTableHash(SSHashObj* pHash, SSHashObj* pOnceHash, void* pKey, int32_t keySize) {
  int32_t code = TSDB_CODE_SUCCESS;
  uint32_t* pNum = tSimpleHashGet(pHash, pKey, keySize);
  if (NULL == pNum) {
    uint32_t n = 1;
    code = tSimpleHashPut(pHash, pKey, keySize, &n, sizeof(n));
    if (code) {
      return code;
    }
    code = tSimpleHashPut(pOnceHash, pKey, keySize, NULL, 0);
    if (code) {
      return code;
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
        code = tSimpleHashRemove(pOnceHash, pKey, keySize);
        if (code) {
          qError("tSimpleHashRemove failed in addToJoinTableHash, error:%s", tstrerror(code));
          QRY_ERR_RET(code);
        }
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
  int32_t code = TSDB_CODE_SUCCESS;
  SStbJoinTableList* pNew = taosMemoryCalloc(1, sizeof(SStbJoinTableList));
  if (NULL == pNew) {
    return terrno;
  }
  pNew->pLeftVg = taosMemoryMalloc(rows * sizeof(*pLeftVg));
  if (NULL == pNew->pLeftVg) {
    code = terrno;
    freeStbJoinTableList(pNew);
    return code;
  }
  pNew->pLeftUid = taosMemoryMalloc(rows * sizeof(*pLeftUid));
  if (NULL == pNew->pLeftUid) {
    code = terrno;
    freeStbJoinTableList(pNew);
    return code;
  }
  pNew->pRightVg = taosMemoryMalloc(rows * sizeof(*pRightVg));
  if (NULL == pNew->pRightVg) {
    code = terrno;
    freeStbJoinTableList(pNew);
    return code;
  }
  pNew->pRightUid = taosMemoryMalloc(rows * sizeof(*pRightUid));
  if (NULL == pNew->pRightUid) {
    code = terrno;
    freeStbJoinTableList(pNew);
    return code;
  }

  TAOS_MEMCPY(pNew->pLeftVg, pLeftVg, rows * sizeof(*pLeftVg));
  TAOS_MEMCPY(pNew->pLeftUid, pLeftUid, rows * sizeof(*pLeftUid));
  TAOS_MEMCPY(pNew->pRightVg, pRightVg, rows * sizeof(*pRightVg));
  TAOS_MEMCPY(pNew->pRightUid, pRightUid, rows * sizeof(*pRightUid));

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
  if (NULL == pVg0) {
    QRY_ERR_JRET(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  }
  SColumnInfoData*           pVg1 = taosArrayGet(pBlock->pDataBlock, pStbJoin->basic.vgSlot[1]);
  if (NULL == pVg1) {
    QRY_ERR_JRET(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  }
  SColumnInfoData*           pUid0 = taosArrayGet(pBlock->pDataBlock, pStbJoin->basic.uidSlot[0]);
  if (NULL == pUid0) {
    QRY_ERR_JRET(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  }
  SColumnInfoData*           pUid1 = taosArrayGet(pBlock->pDataBlock, pStbJoin->basic.uidSlot[1]);
  if (NULL == pUid1) {
    QRY_ERR_JRET(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  }

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

_return:

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
  int32_t code = 0;
  while (NULL != (pUid = tSimpleHashIterate(pStbJoin->ctx.prev.onceTable, pUid, &iter))) {
    code = tSimpleHashRemove(pStbJoin->ctx.prev.leftCache, pUid, sizeof(*pUid));
    if (code) {
      qError("tSimpleHashRemove failed in postProcessStbJoinTableHash, error:%s", tstrerror(code));
    }
  }

  pStbJoin->execInfo.leftCacheNum = tSimpleHashGetSize(pStbJoin->ctx.prev.leftCache);
  qDebug("more than 1 ref build table num %" PRId64, (int64_t)tSimpleHashGetSize(pStbJoin->ctx.prev.leftCache));

/*
  // debug only
  iter = 0;
  uint32_t* num = NULL;
  while (NULL != (num = tSimpleHashIterate(pStbJoin->ctx.prev.leftCache, num, &iter))) {
    A S S E R T(*num > 1);
  }
*/  
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

static int32_t seqJoinLaunchNewRetrieve(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
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
      return TSDB_CODE_SUCCESS;
    }

    QRY_ERR_RET(handleSeqJoinCurrRetrieveEnd(pOperator, pStbJoin));
    pPrev->pListHead->readIdx++;
  }

  *ppRes = NULL;
  setOperatorCompleted(pOperator);

  return TSDB_CODE_SUCCESS;
}

static int32_t seqStableJoinComposeRes(SStbJoinDynCtrlInfo* pStbJoin, SSDataBlock* pBlock) {
  if (pBlock) {
    if (pStbJoin && pStbJoin->pOutputDataBlockDesc) {
      pBlock->info.id.blockId = pStbJoin->pOutputDataBlockDesc->dataBlockId;
      if (!pBlock->pDataBlock) return TSDB_CODE_SUCCESS;

      for (int i = pBlock->pDataBlock->size; i < pStbJoin->pOutputDataBlockDesc->pSlots->length; i++) {
        SSlotDescNode* pSlot = (SSlotDescNode*)nodesListGetNode(pStbJoin->pOutputDataBlockDesc->pSlots, i);
        if (pSlot == NULL) {
          qError("seqStableJoinComposeRes: pSlot is NULL, i:%d", i);
          return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        }
        SColumnInfoData colInfo = createColumnInfoData(pSlot->dataType.type, pSlot->dataType.bytes, pSlot->slotId);
        int32_t code = colInfoDataEnsureCapacity(&colInfo, pBlock->info.rows, true);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
        code = blockDataAppendColInfo(pBlock, &colInfo);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
    } else {
      qError("seqStableJoinComposeRes: pBlock or pStbJoin is NULL");
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t seqStableJoin(SOperatorInfo* pOperator, SSDataBlock** pRes) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SStbJoinDynCtrlInfo*       pStbJoin = (SStbJoinDynCtrlInfo*)&pInfo->stbJoin;

  QRY_PARAM_CHECK(pRes);
  if (pOperator->status == OP_EXEC_DONE) {
    return code;
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

  QRY_ERR_JRET(seqJoinContinueCurrRetrieve(pOperator, pRes));
  if (*pRes) {
    goto _return;
  }

  QRY_ERR_JRET(seqJoinLaunchNewRetrieve(pOperator, pRes));

_return:
  if (pOperator->cost.openCost == 0) {
    pOperator->cost.openCost = (double)(taosGetTimestampUs() - st) / 1000.0;
  }

  if (code) {
    qError("%s failed since %s", __func__, tstrerror(code));
    pOperator->pTaskInfo->code = code;
    T_LONG_JMP(pOperator->pTaskInfo->env, code);
  } else {
    code = seqStableJoinComposeRes(pStbJoin, *pRes);
  }
  return code;
}

static int32_t buildVtbScanOperatorParam(SDynQueryCtrlOperatorInfo* pInfo, SOperatorParam** ppRes, uint64_t uid) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SVTableScanOperatorParam* pVScan = NULL;
  *ppRes = taosMemoryMalloc(sizeof(SOperatorParam));
  QUERY_CHECK_NULL(*ppRes, code, lino, _return, terrno);

  (*ppRes)->pChildren = taosArrayInit(1, POINTER_BYTES);
  QUERY_CHECK_NULL((*ppRes)->pChildren, code, lino, _return, terrno);

  pVScan = taosMemoryMalloc(sizeof(SVTableScanOperatorParam));
  QUERY_CHECK_NULL(pVScan, code, lino, _return, terrno);
  pVScan->pOpParamArray = taosArrayInit(1, POINTER_BYTES);
  QUERY_CHECK_NULL(pVScan->pOpParamArray, code, lino, _return, terrno);
  pVScan->uid = uid;

  (*ppRes)->opType = QUERY_NODE_PHYSICAL_PLAN_VIRTUAL_TABLE_SCAN;
  (*ppRes)->downstreamIdx = 0;
  (*ppRes)->value = pVScan;
  (*ppRes)->reUse = false;

  return TSDB_CODE_SUCCESS;
_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  if (pVScan) {
    taosArrayDestroy(pVScan->pOpParamArray);
    taosMemoryFreeClear(pVScan);
  }
  if (*ppRes) {
    taosArrayDestroy((*ppRes)->pChildren);
    taosMemoryFreeClear(*ppRes);
  }
  return code;
}

int32_t dynProcessUseDbRsp(void* param, SDataBuf* pMsg, int32_t code) {
  int32_t                    lino = 0;
  SOperatorInfo*             operator=(SOperatorInfo*) param;
  SDynQueryCtrlOperatorInfo* pScanResInfo = (SDynQueryCtrlOperatorInfo*)operator->info;

  if (TSDB_CODE_SUCCESS != code) {
    operator->pTaskInfo->code = rpcCvtErrCode(code);
    if (operator->pTaskInfo->code != code) {
      qError("load systable rsp received, error:%s, cvted error:%s", tstrerror(code),
             tstrerror(operator->pTaskInfo->code));
    } else {
      qError("load systable rsp received, error:%s", tstrerror(code));
    }
    goto _return;
  }

  pScanResInfo->vtbScan.pRsp = taosMemoryMalloc(sizeof(SUseDbRsp));
  QUERY_CHECK_NULL(pScanResInfo->vtbScan.pRsp, code, lino, _return, terrno);

  code = tDeserializeSUseDbRsp(pMsg->pData, (int32_t)pMsg->len, pScanResInfo->vtbScan.pRsp);
  QUERY_CHECK_CODE(code, lino, _return);

  taosMemoryFreeClear(pMsg->pData);

  code = tsem_post(&pScanResInfo->vtbScan.ready);
  QUERY_CHECK_CODE(code, lino, _return);

  return code;
_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

static int32_t buildDbVgInfoMap(SOperatorInfo* pOperator, SReadHandle* pHandle, SName* name, SExecTaskInfo* pTaskInfo, SUseDbOutput* output) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    lino = 0;
  char*                      buf1 = NULL;
  SUseDbReq*                 pReq = NULL;
  SDynQueryCtrlOperatorInfo* pScanResInfo = (SDynQueryCtrlOperatorInfo*)pOperator->info;

  pReq = taosMemoryMalloc(sizeof(SUseDbReq));
  QUERY_CHECK_NULL(pReq, code, lino, _return, terrno);
  code = tNameGetFullDbName(name, pReq->db);
  QUERY_CHECK_CODE(code, lino, _return);
  int32_t contLen = tSerializeSUseDbReq(NULL, 0, pReq);
  buf1 = taosMemoryCalloc(1, contLen);
  QUERY_CHECK_NULL(buf1, code, lino, _return, terrno);
  int32_t tempRes = tSerializeSUseDbReq(buf1, contLen, pReq);
  if (tempRes < 0) {
    QUERY_CHECK_CODE(terrno, lino, _return);
  }

  // send the fetch remote task result request
  SMsgSendInfo* pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  QUERY_CHECK_NULL(pMsgSendInfo, code, lino, _return, terrno);

  pMsgSendInfo->param = pOperator;
  pMsgSendInfo->msgInfo.pData = buf1;
  pMsgSendInfo->msgInfo.len = contLen;
  pMsgSendInfo->msgType = TDMT_MND_GET_DB_INFO;
  pMsgSendInfo->fp = dynProcessUseDbRsp;
  pMsgSendInfo->requestId = pTaskInfo->id.queryId;

  code = asyncSendMsgToServer(pHandle->pMsgCb->clientRpc, &pScanResInfo->vtbScan.epSet, NULL, pMsgSendInfo);
  QUERY_CHECK_CODE(code, lino, _return);

  code = tsem_wait(&pScanResInfo->vtbScan.ready);
  QUERY_CHECK_CODE(code, lino, _return);

  code = queryBuildUseDbOutput(output, pScanResInfo->vtbScan.pRsp);
  QUERY_CHECK_CODE(code, lino, _return);

_return:
  if (code) {
     qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
     taosMemoryFree(buf1);
  }
  taosMemoryFree(pReq);
  tFreeSUsedbRsp(pScanResInfo->vtbScan.pRsp);
  taosMemoryFreeClear(pScanResInfo->vtbScan.pRsp);
  return code;
}

int dynVgInfoComp(const void* lp, const void* rp) {
  SVgroupInfo* pLeft = (SVgroupInfo*)lp;
  SVgroupInfo* pRight = (SVgroupInfo*)rp;
  if (pLeft->hashBegin < pRight->hashBegin) {
    return -1;
  } else if (pLeft->hashBegin > pRight->hashBegin) {
    return 1;
  }

  return 0;
}

int32_t dynMakeVgArraySortBy(SDBVgInfo* dbInfo, __compar_fn_t sort_func) {
  if (NULL == dbInfo) {
    return TSDB_CODE_SUCCESS;
  }

  if (dbInfo->vgHash && NULL == dbInfo->vgArray) {
    int32_t vgSize = taosHashGetSize(dbInfo->vgHash);
    dbInfo->vgArray = taosArrayInit(vgSize, sizeof(SVgroupInfo));
    if (NULL == dbInfo->vgArray) {
      return terrno;
    }

    void* pIter = taosHashIterate(dbInfo->vgHash, NULL);
    while (pIter) {
      if (NULL == taosArrayPush(dbInfo->vgArray, pIter)) {
        taosHashCancelIterate(dbInfo->vgHash, pIter);
        return terrno;
      }

      pIter = taosHashIterate(dbInfo->vgHash, pIter);
    }

    taosArraySort(dbInfo->vgArray, sort_func);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t dynHashValueComp(void const* lp, void const* rp) {
  uint32_t*    key = (uint32_t*)lp;
  SVgroupInfo* pVg = (SVgroupInfo*)rp;

  if (*key < pVg->hashBegin) {
    return -1;
  } else if (*key > pVg->hashEnd) {
    return 1;
  }

  return 0;
}

int32_t getVgId(SDBVgInfo* dbInfo, char* dbFName, int32_t* vgId, char *tbName) {
  int32_t code = 0;
  int32_t lino = 0;
  code = dynMakeVgArraySortBy(dbInfo, dynVgInfoComp);
  QUERY_CHECK_CODE(code, lino, _return);

  int32_t vgNum = (int32_t)taosArrayGetSize(dbInfo->vgArray);
  if (vgNum <= 0) {
    qError("db vgroup cache invalid, db:%s, vgroup number:%d", dbFName, vgNum);
    QUERY_CHECK_CODE(code = TSDB_CODE_TSC_DB_NOT_SELECTED, lino, _return);
  }

  SVgroupInfo* vgInfo = NULL;
  char         tbFullName[TSDB_TABLE_FNAME_LEN];
  (void)snprintf(tbFullName, sizeof(tbFullName), "%s.", dbFName);
  int32_t offset = (int32_t)strlen(tbFullName);

  (void)snprintf(tbFullName + offset, sizeof(tbFullName) - offset, "%s", tbName);
  uint32_t hashValue = taosGetTbHashVal(tbFullName, (int32_t)strlen(tbFullName), dbInfo->hashMethod,
                                        dbInfo->hashPrefix, dbInfo->hashSuffix);

  vgInfo = taosArraySearch(dbInfo->vgArray, &hashValue, dynHashValueComp, TD_EQ);
  if (NULL == vgInfo) {
    qError("no hash range found for hash value [%u], db:%s, numOfVgId:%d", hashValue, dbFName,
           (int32_t)taosArrayGetSize(dbInfo->vgArray));
    return TSDB_CODE_CTG_INTERNAL_ERROR;
  }

  *vgId = vgInfo->vgId;

_return:
  return code;
}

bool colNeedScan(SOperatorInfo* pOperator, col_id_t colId) {
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = (SVtbScanDynCtrlInfo*)&pInfo->vtbScan;
  SArray *                   pColList = pVtbScan->readColList;
  if (pVtbScan->scanAllCols) {
    return true;
  }
  for (int32_t i = 0; i < taosArrayGetSize(pColList); i++) {
    if (colId == *(col_id_t*)taosArrayGet(pColList, i)) {
      return true;
    }
  }
  return false;
}

int32_t getDbVgInfo(SOperatorInfo* pOperator, SName *name, SDBVgInfo **dbVgInfo) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = (SVtbScanDynCtrlInfo*)&pInfo->vtbScan;
  SExecTaskInfo*             pTaskInfo = pOperator->pTaskInfo;
  SReadHandle*               pHandle = &pVtbScan->readHandle;
  SUseDbOutput*              output = NULL;
  SUseDbOutput**             find = (SUseDbOutput**)taosHashGet(pInfo->vtbScan.dbVgInfoMap, name->dbname, strlen(name->dbname));

  QRY_PARAM_CHECK(dbVgInfo);

  if (find == NULL) {
    output = taosMemoryMalloc(sizeof(SUseDbOutput));
    code = buildDbVgInfoMap(pOperator, pHandle, name, pTaskInfo, output);
    QUERY_CHECK_CODE(code, line, _return);
    code = taosHashPut(pInfo->vtbScan.dbVgInfoMap, name->dbname, strlen(name->dbname), &output, POINTER_BYTES);
    QUERY_CHECK_CODE(code, line, _return);
  } else {
    output = *find;
  }

  *dbVgInfo = output->dbVgroup;
  return code;
_return:
  qError("%s failed at line %d since %s", __func__, line, tstrerror(code));
  freeUseDbOutput(output);
  return code;
}

int32_t vtbScan(SOperatorInfo* pOperator, SSDataBlock** pRes) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = (SVtbScanDynCtrlInfo*)&pInfo->vtbScan;
  SExecTaskInfo*             pTaskInfo = pOperator->pTaskInfo;
  SReadHandle*               pHandle = &pVtbScan->readHandle;
  SMetaReader                mr = {0};
  SDBVgInfo*                 dbVgInfo = NULL;
  bool                       readerInit = false;

  QRY_PARAM_CHECK(pRes);
  if (pOperator->status == OP_EXEC_DONE) {
    return code;
  }

  int64_t st = 0;
  if (pOperator->cost.openCost == 0) {
    st = taosGetTimestampUs();
  }

  size_t num = taosArrayGetSize(pVtbScan->childTableList);

  // no child table, return
  if (num == 0) {
    setOperatorCompleted(pOperator);
    return code;
  }

  pVtbScan->orgTbVgColMap = taosHashInit(num * 64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  QUERY_CHECK_NULL(pVtbScan->orgTbVgColMap, code, line, _return, terrno);
  taosHashSetFreeFp(pVtbScan->orgTbVgColMap, destroyOrgTbInfo);

  while (true) {
    if (pVtbScan->curTableIdx == pVtbScan->lastTableIdx) {
      code = pOperator->pDownstream[0]->fpSet.getNextFn(pOperator->pDownstream[0], pRes);
      QUERY_CHECK_CODE(code, line, _return);
    } else {
      uint64_t* id = taosArrayGet(pVtbScan->childTableList, pVtbScan->curTableIdx);
      QUERY_CHECK_NULL(id, code, line, _return, terrno);
      pHandle->api.metaReaderFn.initReader(&mr, pHandle->vnode, META_READER_LOCK, &pHandle->api.metaFn);
      readerInit = true;
      code = pHandle->api.metaReaderFn.getTableEntryByUid(&mr, *id);
      QUERY_CHECK_CODE(code, line, _return);

      for (int32_t j = 0; j < mr.me.colRef.nCols; j++) {
        if (mr.me.colRef.pColRef[j].hasRef && colNeedScan(pOperator, mr.me.colRef.pColRef[j].id)) {
          SName   name = {0};
          char    dbFname[TSDB_DB_FNAME_LEN] = {0};
          char    orgTbFName[TSDB_TABLE_FNAME_LEN] = {0};
          toName(pInfo->vtbScan.acctId, mr.me.colRef.pColRef[j].refDbName, mr.me.colRef.pColRef[j].refTableName, &name);
          code = getDbVgInfo(pOperator, &name, &dbVgInfo);
          QUERY_CHECK_CODE(code, line, _return);
          tNameGetFullDbName(&name, dbFname);
          QUERY_CHECK_CODE(code, line, _return);
          tNameGetFullTableName(&name, orgTbFName);
          QUERY_CHECK_CODE(code, line, _return);

          void *pVal = taosHashGet(pVtbScan->orgTbVgColMap, orgTbFName, sizeof(orgTbFName));
          if (!pVal) {
            SOrgTbInfo map = {0};
            code = getVgId(dbVgInfo, dbFname, &map.vgId, name.tname);
            QUERY_CHECK_CODE(code, line, _return);
            tstrncpy(map.tbName, orgTbFName, sizeof(map.tbName));
            map.colMap = taosArrayInit(10, sizeof(SColIdNameKV));
            QUERY_CHECK_NULL(map.colMap, code, line, _return, terrno);
            SColIdNameKV colIdNameKV = {0};
            colIdNameKV.colId = mr.me.colRef.pColRef[j].id;
            tstrncpy(colIdNameKV.colName, mr.me.colRef.pColRef[j].refColName, sizeof(colIdNameKV.colName));
            QUERY_CHECK_NULL(taosArrayPush(map.colMap, &colIdNameKV), code, line, _return, terrno);
            code = taosHashPut(pVtbScan->orgTbVgColMap, orgTbFName, sizeof(orgTbFName), &map, sizeof(map));
            QUERY_CHECK_CODE(code, line, _return);
          } else {
            SOrgTbInfo *tbInfo = (SOrgTbInfo *)pVal;
            SColIdNameKV colIdNameKV = {0};
            colIdNameKV.colId = mr.me.colRef.pColRef[j].id;
            tstrncpy(colIdNameKV.colName, mr.me.colRef.pColRef[j].refColName, sizeof(colIdNameKV.colName));
            QUERY_CHECK_NULL(taosArrayPush(tbInfo->colMap, &colIdNameKV), code, line, _return, terrno);
          }
        }
      }

      pVtbScan->vtbScanParam = NULL;
      code = buildVtbScanOperatorParam(pInfo, &pVtbScan->vtbScanParam, *id);
      QUERY_CHECK_CODE(code, line, _return);

      void* pIter = taosHashIterate(pVtbScan->orgTbVgColMap, NULL);
      while (pIter != NULL) {
        SOrgTbInfo*      pMap = (SOrgTbInfo*)pIter;
        SOperatorParam*  pExchangeParam = NULL;
        code = buildExchangeOperatorParamForVScan(&pExchangeParam, 0, pMap);
        QUERY_CHECK_CODE(code, line, _return);
        QUERY_CHECK_NULL(taosArrayPush(((SVTableScanOperatorParam*)pVtbScan->vtbScanParam->value)->pOpParamArray, &pExchangeParam), code, line, _return, terrno);
        pIter = taosHashIterate(pVtbScan->orgTbVgColMap, pIter);
      }
      pHandle->api.metaReaderFn.clearReader(&mr);
      readerInit = false;

      // reset downstream operator's status
      pOperator->pDownstream[0]->status = OP_NOT_OPENED;
      code = pOperator->pDownstream[0]->fpSet.getNextExtFn(pOperator->pDownstream[0], pVtbScan->vtbScanParam, pRes);
      QUERY_CHECK_CODE(code, line, _return);
    }

    if (*pRes) {
      // has result, still read data from this table.
      pVtbScan->lastTableIdx = pVtbScan->curTableIdx;
      break;
    } else {
      // no result, read next table.
      pVtbScan->curTableIdx++;
      if (pVtbScan->curTableIdx >= taosArrayGetSize(pVtbScan->childTableList)) {
        setOperatorCompleted(pOperator);
        break;
      }
    }
  }

_return:
  if (readerInit) {
    pHandle->api.metaReaderFn.clearReader(&mr);
  }
  taosHashCleanup(pVtbScan->orgTbVgColMap);
  pVtbScan->orgTbVgColMap = NULL;
  if (pOperator->cost.openCost == 0) {
    pOperator->cost.openCost = (double)(taosGetTimestampUs() - st) / 1000.0;
  }

  if (code) {
    qError("%s failed since %s", __func__, tstrerror(code));
    pOperator->pTaskInfo->code = code;
    T_LONG_JMP(pOperator->pTaskInfo->env, code);
  }

  return code;
}

int32_t initSeqStbJoinTableHash(SStbJoinPrevJoinCtx* pPrev, bool batchFetch) {
  if (batchFetch) {
    pPrev->leftHash = tSimpleHashInit(20, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
    if (NULL == pPrev->leftHash) {
      return terrno;
    }
    pPrev->rightHash = tSimpleHashInit(20, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
    if (NULL == pPrev->rightHash) {
      return terrno;
    }
  } else {
    pPrev->leftCache = tSimpleHashInit(20, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    if (NULL == pPrev->leftCache) {
      return terrno;
    }
    pPrev->rightCache = tSimpleHashInit(20, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    if (NULL == pPrev->rightCache) {
      return terrno;
    }
    pPrev->onceTable = tSimpleHashInit(20, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    if (NULL == pPrev->onceTable) {
      return terrno;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t initVtbScanInfo(SOperatorInfo* pOperator, SDynQueryCtrlOperatorInfo* pInfo, SReadHandle* pHandle,
                               SDynQueryCtrlPhysiNode* pPhyciNode, SExecTaskInfo* pTaskInfo) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      line = 0;

  code = tsem_init(&pInfo->vtbScan.ready, 0, 0);
  QUERY_CHECK_CODE(code, line, _return);

  pInfo->vtbScan.scanAllCols = pPhyciNode->vtbScan.scanAllCols;
  pInfo->vtbScan.suid = pPhyciNode->vtbScan.suid;
  pInfo->vtbScan.epSet = pPhyciNode->vtbScan.mgmtEpSet;
  pInfo->vtbScan.acctId = pPhyciNode->vtbScan.accountId;
  pInfo->vtbScan.readHandle = *pHandle;
  pInfo->vtbScan.curTableIdx = 0;
  pInfo->vtbScan.lastTableIdx = -1;
  pInfo->vtbScan.dbName = taosStrdup(pPhyciNode->vtbScan.dbName);
  QUERY_CHECK_NULL(pInfo->vtbScan.dbName, code, line, _return, terrno);

  pInfo->vtbScan.readColList = taosArrayInit(LIST_LENGTH(pPhyciNode->vtbScan.pScanCols), sizeof(col_id_t));
  QUERY_CHECK_NULL(pInfo->vtbScan.readColList, code, line, _return, terrno);

  for (int32_t i = 0; i < LIST_LENGTH(pPhyciNode->vtbScan.pScanCols); ++i) {
    SColumnNode* pNode = (SColumnNode*)nodesListGetNode(pPhyciNode->vtbScan.pScanCols, i);
    QUERY_CHECK_NULL(pNode, code, line, _return, terrno);
    QUERY_CHECK_NULL(taosArrayPush(pInfo->vtbScan.readColList, &pNode->colId), code, line, _return, terrno);
  }

  pInfo->vtbScan.childTableList = taosArrayInit(10, sizeof(uint64_t));
  QUERY_CHECK_NULL(pInfo->vtbScan.childTableList, code, line, _return, terrno);
  code = pHandle->api.metaFn.getChildTableList(pHandle->vnode, pInfo->vtbScan.suid, pInfo->vtbScan.childTableList);
  QUERY_CHECK_CODE(code, line, _return);

  pInfo->vtbScan.dbVgInfoMap = taosHashInit(taosArrayGetSize(pInfo->vtbScan.childTableList), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  QUERY_CHECK_NULL(pInfo->vtbScan.dbVgInfoMap, code, line, _return, terrno);

  return code;
_return:
  // no need to destroy array and hashmap allocated in this function,
  // since the operator's destroy function will take care of it
  qError("%s failed at line %d since %s", __func__, line, tstrerror(code));
  return code;
}

int32_t createDynQueryCtrlOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream,
                                       SDynQueryCtrlPhysiNode* pPhyciNode, SExecTaskInfo* pTaskInfo,
                                       SReadHandle* pHandle, SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  __optr_fn_t                nextFp = NULL;
  SOperatorInfo*             pOperator = NULL;
  SDynQueryCtrlOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SDynQueryCtrlOperatorInfo));
  if (pInfo == NULL) {
    code = terrno;
    goto _error;
  }

  pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pOperator == NULL) {
    code = terrno;
    goto _error;
  }

  pTaskInfo->dynamicTask = (int8_t)pPhyciNode->node.dynamicOp;

  code = appendDownstream(pOperator, pDownstream, numOfDownstream);
  if (TSDB_CODE_SUCCESS != code) {
    goto _error;
  }

  setOperatorInfo(pOperator, "DynQueryCtrlOperator", QUERY_NODE_PHYSICAL_PLAN_DYN_QUERY_CTRL, false, OP_NOT_OPENED,
                  pInfo, pTaskInfo);

  pInfo->qType = pPhyciNode->qType;
  switch (pInfo->qType) {
    case DYN_QTYPE_STB_HASH:
      TAOS_MEMCPY(&pInfo->stbJoin.basic, &pPhyciNode->stbJoin, sizeof(pPhyciNode->stbJoin));
      pInfo->stbJoin.pOutputDataBlockDesc = pPhyciNode->node.pOutputDataBlockDesc;
      code = initSeqStbJoinTableHash(&pInfo->stbJoin.ctx.prev, pInfo->stbJoin.basic.batchFetch);
      if (TSDB_CODE_SUCCESS != code) {
        goto _error;
      }
      nextFp = seqStableJoin;
      break;
    case DYN_QTYPE_VTB_SCAN:
      code = initVtbScanInfo(pOperator, pInfo, pHandle, pPhyciNode, pTaskInfo);
      QUERY_CHECK_CODE(code, line, _error);
      nextFp = vtbScan;
      break;
    default:
      qError("unsupported dynamic query ctrl type: %d", pInfo->qType);
      code = TSDB_CODE_INVALID_PARA;
      goto _error;
  }

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, nextFp, NULL, destroyDynQueryCtrlOperator, optrDefaultBufFn,
                                         NULL, optrDefaultGetNextExtFn, NULL);

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  if (pInfo != NULL) {
    destroyDynQueryCtrlOperator(pInfo);
  }

  destroyOperatorAndDownstreams(pOperator, pDownstream, numOfDownstream);
  pTaskInfo->code = code;
  return code;
}
