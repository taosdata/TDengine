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
#include "tdataformat.h"
#include "dynqueryctrl.h"

#define DYN_VTB_REF_MAX_DEPTH 32

typedef struct SDynResolvedColRef {
  char    dbName[TSDB_DB_NAME_LEN];
  char    tbName[TSDB_TABLE_NAME_LEN];
  char    colName[TSDB_COL_NAME_LEN];
  char    fullColRef[TSDB_DB_NAME_LEN + TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN + 3];
  int32_t vgId;
} SDynResolvedColRef;

typedef struct SDynRefKey {
  char colRef[TSDB_DB_NAME_LEN + TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN + 3];
} SDynRefKey;

typedef struct SColRefNameView {
  const char* dbName;     // start of database-name segment in original ref string
  const char* tbName;     // start of table-name segment in original ref string
  const char* colName;    // start of column-name segment in original ref string
  size_t      dbNameLen;  // database-name bytes, excluding terminator
  size_t      tbNameLen;  // table-name bytes, excluding terminator
  size_t      colNameLen; // column-name bytes, excluding terminator
} SColRefNameView;

/*
 * Resolve vgroup id and optional vnode epSet for a table.
 *
 * @param dbInfo   database vgroup info cache
 * @param dbFName  full database name
 * @param vgId     output vgroup id
 * @param tbName   table name inside database
 * @param pEpSet   optional output vnode epSet
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t getVgIdAndEpSet(SDBVgInfo* dbInfo, char* dbFName, int32_t* vgId, char* tbName, SEpSet* pEpSet);
static int32_t getDbVgInfo(SOperatorInfo* pOperator, SName* name, SDBVgInfo** dbVgInfo);
static int32_t parseColRefNameView(const char* colRef, SColRefNameView* pView);
static int32_t getVgIdFromColref(SOperatorInfo* pOperator, const char* colRef, int32_t* vgId);

int64_t gSessionId = 0;

void freeVgTableList(void* ptr) { 
  taosArrayDestroy(*(SArray**)ptr); 
}

/*
 * Release cached final referenced-column information.
 *
 * @param info  hash value that stores SDynResolvedColRef pointer
 *
 * @return none
 */
static void destroyResolvedColRef(void* info) {
  SDynResolvedColRef* pResolved = *(SDynResolvedColRef**)info;
  if (pResolved) {
    taosMemoryFree(pResolved);
  }
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

void destroyColRefInfo(void *info) {
  SColRefInfo *pColRefInfo = (SColRefInfo *)info;
  if (pColRefInfo) {
    taosMemoryFree(pColRefInfo->colName);
    taosMemoryFree(pColRefInfo->colrefName);
  }
}

void destroyColRefArray(void *info) {
  SArray *pColRefArray = *(SArray **)info;
  if (pColRefArray) {
    taosArrayDestroyEx(pColRefArray, destroyColRefInfo);
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

void destroyOtbInfoArray(void *info) {
  SArray *pOtbInfoArray = *(SArray **)info;
  if (pOtbInfoArray) {
    taosArrayDestroyEx(pOtbInfoArray, destroySOrgTbInfo);
  }
}

void destroyOtbVgIdToOtbInfoArrayMap(void *info) {
  SHashObj* pOtbVgIdToOtbInfoArrayMap = *(SHashObj **)info;
  if (pOtbVgIdToOtbInfoArrayMap) {
    taosHashSetFreeFp(pOtbVgIdToOtbInfoArrayMap, destroyOtbInfoArray);
    taosHashCleanup(pOtbVgIdToOtbInfoArrayMap);
  }
}

void destroyTagList(void *info) {
  SArray *pTagList = *(SArray **)info;
  if (pTagList) {
    taosArrayDestroyEx(pTagList, destroyTagVal);
  }
}

static void destroyRefColIdGroup(void *info) {
  SRefColIdGroup *pGroup = (SRefColIdGroup *)info;
  if (pGroup && pGroup->pSlotIdList) {
    taosArrayDestroy(pGroup->pSlotIdList);
    pGroup->pSlotIdList = NULL;
  }
}

void destroyVtbUidTagListMap(void *info) {
  SHashObj* pVtbUidTagListMap = *(SHashObj **)info;
  if (pVtbUidTagListMap) {
    taosHashSetFreeFp(pVtbUidTagListMap, destroyTagList);
    taosHashCleanup(pVtbUidTagListMap);
  }
}

static void destroyVtbScanDynCtrlInfo(SVtbScanDynCtrlInfo* pVtbScan) {
  if (pVtbScan->dbName) {
    taosMemoryFreeClear(pVtbScan->dbName);
  }
  if (pVtbScan->tbName) {
    taosMemoryFreeClear(pVtbScan->tbName);
  }
  if (pVtbScan->refColGroups) {
    taosArrayDestroyEx(pVtbScan->refColGroups, destroyRefColIdGroup);
    pVtbScan->refColGroups = NULL;
  }
  if (pVtbScan->childTableList) {
    taosArrayDestroyEx(pVtbScan->childTableList, destroyColRefArray);
  }
  if (pVtbScan->colRefInfo) {
    taosArrayDestroyEx(pVtbScan->colRefInfo, destroyColRefInfo);
    pVtbScan->colRefInfo = NULL;
  }
  if (pVtbScan->childTableMap) {
    taosHashCleanup(pVtbScan->childTableMap);
  }
  if (pVtbScan->readColList) {
    taosArrayDestroy(pVtbScan->readColList);
  }
  if (pVtbScan->readColSet) {
    taosHashCleanup(pVtbScan->readColSet);
  }
  if (pVtbScan->dbVgInfoMap) {
    taosHashSetFreeFp(pVtbScan->dbVgInfoMap, freeUseDbOutput);
    taosHashCleanup(pVtbScan->dbVgInfoMap);
  }
  if (pVtbScan->resolvedColRefMap) {
    taosHashSetFreeFp(pVtbScan->resolvedColRefMap, destroyResolvedColRef);
    taosHashCleanup(pVtbScan->resolvedColRefMap);
  }
  if (pVtbScan->otbNameToOtbInfoMap) {
    taosHashSetFreeFp(pVtbScan->otbNameToOtbInfoMap, destroySOrgTbInfo);
    taosHashCleanup(pVtbScan->otbNameToOtbInfoMap);
  }
  if (pVtbScan->pRsp) {
    tFreeSUsedbRsp(pVtbScan->pRsp);
    taosMemoryFreeClear(pVtbScan->pRsp);
  }
  if (pVtbScan->existOrgTbVg) {
    taosHashCleanup(pVtbScan->existOrgTbVg);
  }
  if (pVtbScan->curOrgTbVg) {
    taosHashCleanup(pVtbScan->curOrgTbVg);
  }
  if (pVtbScan->newAddedVgInfo) {
    taosHashCleanup(pVtbScan->newAddedVgInfo);
  }
  if (pVtbScan->otbVgIdToOtbInfoArrayMap) {
    taosHashSetFreeFp(pVtbScan->otbVgIdToOtbInfoArrayMap, destroyOtbInfoArray);
    taosHashCleanup(pVtbScan->otbVgIdToOtbInfoArrayMap);
  }
  if (pVtbScan->vtbUidToVgIdMapMap) {
    taosHashSetFreeFp(pVtbScan->vtbUidToVgIdMapMap, destroyOtbVgIdToOtbInfoArrayMap);
    taosHashCleanup(pVtbScan->vtbUidToVgIdMapMap);
  }
  if (pVtbScan->vtbGroupIdToVgIdMapMap) {
    taosHashSetFreeFp(pVtbScan->vtbGroupIdToVgIdMapMap, destroyOtbVgIdToOtbInfoArrayMap);
    taosHashCleanup(pVtbScan->vtbGroupIdToVgIdMapMap);
  }
  if (pVtbScan->vtbUidTagListMap) {
    taosHashSetFreeFp(pVtbScan->vtbUidTagListMap, destroyTagList);
    taosHashCleanup(pVtbScan->vtbUidTagListMap);
  }
  if (pVtbScan->vtbGroupIdTagListMap) {
    taosHashCleanup(pVtbScan->vtbGroupIdTagListMap);
  }
  if (pVtbScan->vtbUidToGroupIdMap) {
    taosHashCleanup(pVtbScan->vtbUidToGroupIdMap);
  }
}

void destroyWinArray(void *info) {
  SArray *pWinArray = *(SArray **)info;
  if (pWinArray) {
    taosArrayDestroy(pWinArray);
  }
}

static void destroyVtbWindowDynCtrlInfo(SVtbWindowDynCtrlInfo* pVtbWindow) {
  if (pVtbWindow->pRes) {
    blockDataDestroy(pVtbWindow->pRes);
  }
  if (pVtbWindow->pWins) {
    taosArrayDestroyEx(pVtbWindow->pWins, destroyWinArray);
  }
}

static void destroyDynQueryCtrlOperator(void* param) {
  SDynQueryCtrlOperatorInfo* pDyn = (SDynQueryCtrlOperatorInfo*)param;

  switch (pDyn->qType) {
    case DYN_QTYPE_STB_HASH:
      destroyStbJoinDynCtrlInfo(&pDyn->stbJoin);
      break;
    case DYN_QTYPE_VTB_WINDOW:
      destroyVtbWindowDynCtrlInfo(&pDyn->vtbWindow);
      destroyVtbScanDynCtrlInfo(&pDyn->vtbScan);
      break;
    case DYN_QTYPE_VTB_INTERVAL:
    case DYN_QTYPE_VTB_AGG:
    case DYN_QTYPE_VTB_SCAN:
    case DYN_QTYPE_VTB_TS_SCAN:
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

static int32_t copyOrgTbInfo(SOrgTbInfo* pSrc, SOrgTbInfo** ppDst) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  SOrgTbInfo* pTbInfo = NULL;

  qDebug("start to copy org table info, vgId:%d, tbName:%s", pSrc->vgId, pSrc->tbName);

  pTbInfo = taosMemoryMalloc(sizeof(SOrgTbInfo));
  QUERY_CHECK_NULL(pTbInfo, code, lino, _return, terrno)

  pTbInfo->vgId = pSrc->vgId;
  tstrncpy(pTbInfo->tbName, pSrc->tbName, TSDB_TABLE_FNAME_LEN);

  pTbInfo->colMap = taosArrayDup(pSrc->colMap, NULL);
  QUERY_CHECK_NULL(pTbInfo->colMap, code, lino, _return, terrno)

  *ppDst = pTbInfo;

  return code;
_return:
  qError("failed to copy org table info, code:%d, line:%d", code, lino);
  if (pTbInfo) {
    if (pTbInfo->colMap) {
      taosArrayDestroy(pTbInfo->colMap);
    }
    taosMemoryFreeClear(pTbInfo);
  }
  return code;
}

static int32_t buildTagListForExchangeBasicParam(SExchangeOperatorBasicParam* pBasic, SArray* pTagList) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  STagVal  tmpTag;

  pBasic->tagList = taosArrayInit(1, sizeof(STagVal));
  QUERY_CHECK_NULL(pBasic->tagList, code, lino, _return, terrno)

  for (int32_t i = 0; i < taosArrayGetSize(pTagList); ++i) {
    STagVal* pSrcTag = (STagVal*)taosArrayGet(pTagList, i);
    QUERY_CHECK_NULL(pSrcTag, code, lino, _return, terrno)
    tmpTag.type = pSrcTag->type;
    tmpTag.cid = pSrcTag->cid;
    if (IS_VAR_DATA_TYPE(pSrcTag->type)) {
      tmpTag.nData = pSrcTag->nData;
      tmpTag.pData = taosMemoryMalloc(tmpTag.nData);
      QUERY_CHECK_NULL(tmpTag.pData, code, lino, _return, terrno)
      memcpy(tmpTag.pData, pSrcTag->pData, tmpTag.nData);
    } else {
      tmpTag.i64 = pSrcTag->i64;
    }

    QUERY_CHECK_NULL(taosArrayPush(pBasic->tagList, &tmpTag), code, lino, _return, terrno)
    tmpTag = (STagVal){0};
  }

  return code;
_return:
  if (pBasic->tagList) {
    taosArrayDestroyEx(pBasic->tagList, destroyTagVal);
    pBasic->tagList = NULL;
  }
  if (tmpTag.pData) {
    taosMemoryFree(tmpTag.pData);
  }
  qError("%s failed at line: %d, code: %d", __func__, lino, code);
  return code;
}

static int32_t buildBatchOrgTbInfoForExchangeBasicParam(SExchangeOperatorBasicParam* pBasic, SArray* pOrgTbInfoArray) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  SOrgTbInfo  batchInfo;

  pBasic->batchOrgTbInfo = taosArrayInit(1, sizeof(SOrgTbInfo));
  QUERY_CHECK_NULL(pBasic->batchOrgTbInfo, code, lino, _return, terrno)

  for (int32_t i = 0; i < taosArrayGetSize(pOrgTbInfoArray); ++i) {
    SOrgTbInfo* pSrc = (SOrgTbInfo*)taosArrayGet(pOrgTbInfoArray, i);
    QUERY_CHECK_NULL(pSrc, code, lino, _return, terrno)
    batchInfo.vgId = pSrc->vgId;
    tstrncpy(batchInfo.tbName, pSrc->tbName, TSDB_TABLE_FNAME_LEN);
    batchInfo.colMap = taosArrayDup(pSrc->colMap, NULL);
    QUERY_CHECK_NULL(batchInfo.colMap, code, lino, _return, terrno)
    QUERY_CHECK_NULL(taosArrayPush(pBasic->batchOrgTbInfo, &batchInfo), code, lino, _return, terrno)
    batchInfo = (SOrgTbInfo){0};
  }

  return code;
_return:
  qError("%s failed at line: %d, code: %d", __func__, lino, code);
  if (pBasic->batchOrgTbInfo) {
    taosArrayDestroyEx(pBasic->batchOrgTbInfo, destroySOrgTbInfo);
    pBasic->batchOrgTbInfo = NULL;
  }
  if (batchInfo.colMap) {
    taosArrayDestroy(batchInfo.colMap);
    batchInfo.colMap = NULL;
  }
  return code;
}

static int32_t buildGroupCacheOperatorParam(SOperatorParam** ppRes, int32_t downstreamIdx, int32_t vgId, int64_t tbUid, bool needCache, SOperatorParam* pChild) {
  int32_t code = TSDB_CODE_SUCCESS;
  *ppRes = taosMemoryCalloc(1, sizeof(SOperatorParam));
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
  *ppRes = taosMemoryCalloc(1, sizeof(SOperatorParam));
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

static int32_t buildExchangeOperatorBasicParam(SExchangeOperatorBasicParam* pBasic, ENodeType srcOpType,
                                               EExchangeSourceType exchangeType, int32_t vgId, uint64_t groupId,
                                               SArray* pUidList, SOrgTbInfo* pOrgTbInfo, SArray* pTagList,
                                               SArray* pSysScanReqs,
                                               SArray* pOrgTbInfoArray, STimeWindow window,
                                               SDownstreamSourceNode* pDownstreamSourceNode,
                                               bool tableSeq, bool isNewParam, bool isNewDeployed) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  qDebug("buildExchangeOperatorBasicParam, srcOpType:%d, exchangeType:%d, vgId:%d, groupId:%" PRIu64 ", tableSeq:%d, "
         "isNewParam:%d, isNewDeployed:%d", srcOpType, exchangeType, vgId, groupId, tableSeq, isNewParam, isNewDeployed);

  pBasic->paramType = DYN_TYPE_EXCHANGE_PARAM;
  pBasic->srcOpType = srcOpType;
  pBasic->vgId = vgId;
  pBasic->groupid = groupId;
  pBasic->window = window;
  pBasic->tableSeq = tableSeq;
  pBasic->type = exchangeType;
  pBasic->isNewParam = isNewParam;

  if (pDownstreamSourceNode) {
    pBasic->isNewDeployed = true;
    pBasic->newDeployedSrc.type = QUERY_NODE_DOWNSTREAM_SOURCE;
    pBasic->newDeployedSrc.clientId = pDownstreamSourceNode->clientId;// current task's taskid
    pBasic->newDeployedSrc.taskId = pDownstreamSourceNode->taskId;
    pBasic->newDeployedSrc.fetchMsgType = TDMT_STREAM_FETCH;
    pBasic->newDeployedSrc.localExec = false;
    pBasic->newDeployedSrc.addr.nodeId = pDownstreamSourceNode->addr.nodeId;
    memcpy(&pBasic->newDeployedSrc.addr.epSet, &pDownstreamSourceNode->addr.epSet, sizeof(SEpSet));
  } else {
    pBasic->isNewDeployed = false;
    pBasic->newDeployedSrc = (SDownstreamSourceNode){0};
  }

  if (pUidList) {
    pBasic->uidList = taosArrayDup(pUidList, NULL);
    QUERY_CHECK_NULL(pBasic->uidList, code, lino, _return, terrno)
  } else {
    pBasic->uidList = taosArrayInit(1, sizeof(int64_t));
    QUERY_CHECK_NULL(pBasic->uidList, code, lino, _return, terrno)
  }

  if (pOrgTbInfo) {
    code = copyOrgTbInfo(pOrgTbInfo, &pBasic->orgTbInfo);
    QUERY_CHECK_CODE(code, lino, _return);
  } else {
    pBasic->orgTbInfo = NULL;
  }

  if (pTagList) {
    code = buildTagListForExchangeBasicParam(pBasic, pTagList);
    QUERY_CHECK_CODE(code, lino, _return);
  } else {
    pBasic->tagList = NULL;
  }

  if (pSysScanReqs) {
    pBasic->sysScanReqs = taosArrayDup(pSysScanReqs, NULL);
    QUERY_CHECK_NULL(pBasic->sysScanReqs, code, lino, _return, terrno)
  } else {
    pBasic->sysScanReqs = NULL;
  }

  if (pOrgTbInfoArray) {
    code = buildBatchOrgTbInfoForExchangeBasicParam(pBasic, pOrgTbInfoArray);
    QUERY_CHECK_CODE(code, lino, _return);
  } else {
    pBasic->batchOrgTbInfo = NULL;
  }
  return code;

_return:
  qError("%s failed at line: %d, code: %d", __func__, lino, code);
  freeExchangeGetBasicOperatorParam(pBasic);
  return code;
}

static int32_t buildExchangeOperatorParamImpl(SOperatorParam** ppRes, int32_t downstreamIdx, ENodeType srcOpType,
                                              EExchangeSourceType exchangeType, int32_t vgId, uint64_t groupId,
                                              SArray* pUidList, SOrgTbInfo* pOrgTbInfo, SArray* pTagList,
                                              SArray* pOrgTbInfoArray, STimeWindow window,
                                              SDownstreamSourceNode* pDownstreamSourceNode,
                                              bool tableSeq, bool isNewParam, bool reUse, bool isNewDeployed) {

  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      lino = 0;
  SOperatorParam*              pParam = NULL;
  SExchangeOperatorParam*      pExc = NULL;

  *ppRes = NULL;

  pParam = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(pParam, code, lino, _return, terrno)

  pParam->opType = QUERY_NODE_PHYSICAL_PLAN_EXCHANGE;
  pParam->downstreamIdx = downstreamIdx;
  pParam->reUse = reUse;
  pParam->pChildren = NULL;
  pParam->value = taosMemoryMalloc(sizeof(SExchangeOperatorParam));
  QUERY_CHECK_NULL(pParam->value, code, lino, _return, terrno)

  pExc = (SExchangeOperatorParam*)pParam->value;
  pExc->multiParams = false;

  code = buildExchangeOperatorBasicParam(&pExc->basic, srcOpType, exchangeType, vgId, groupId,
                                         pUidList, pOrgTbInfo, pTagList, NULL, pOrgTbInfoArray,
                                         window, pDownstreamSourceNode, tableSeq, isNewParam, isNewDeployed);
  QUERY_CHECK_CODE(code, lino, _return);

  *ppRes = pParam;
  return code;
_return:
  qError("%s failed at line: %d, code: %d", __func__, lino, code);
  if (pParam) {
    freeOperatorParam(pParam, OP_GET_PARAM);
  }
  return code;
}

static int32_t buildExchangeOperatorParam(SOperatorParam** ppRes, int32_t downstreamIdx, const int32_t* pVgId, int64_t* pUid) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  SArray* pUidList = taosArrayInit(1, sizeof(int64_t));
  QUERY_CHECK_NULL(pUidList, code, lino, _return, terrno)

  QUERY_CHECK_NULL(taosArrayPush(pUidList, pUid), code, lino, _return, terrno);

  code = buildExchangeOperatorParamImpl(ppRes, downstreamIdx, QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, EX_SRC_TYPE_STB_JOIN_SCAN,
                                        *pVgId, 0, pUidList, NULL, NULL, NULL, (STimeWindow){0}, NULL, true, false, false, false);
  QUERY_CHECK_CODE(code, lino, _return);

_return:
  if (code) {
    qError("failed to build exchange operator param, code:%d", code);
  }
  taosArrayDestroy(pUidList);
  return code;
}

static int32_t buildExchangeOperatorParamForExternalWindow(SOperatorParam** ppRes, int32_t downstreamIdx, STimeWindow win) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;

  code = buildExchangeOperatorParamImpl(ppRes, downstreamIdx, QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, EX_SRC_TYPE_VTB_WIN_SCAN,
                                        0, 0, NULL, NULL, NULL, NULL, win, NULL, true, true, true, false);
  QUERY_CHECK_CODE(code, lino, _return);

  return code;
_return:
  qError("failed to build exchange operator param for external window, code:%d", code);
  return code;
}

static int32_t buildExchangeOperatorParamForVTagScan(SOperatorParam** ppRes, int32_t downstreamIdx, int32_t vgId, tb_uid_t uid) {
  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      lino = 0;
  SArray*                      pUidList = NULL;

  pUidList = taosArrayInit(1, sizeof(int64_t));
  QUERY_CHECK_NULL(pUidList, code, lino, _return, terrno)

  QUERY_CHECK_NULL(taosArrayPush(pUidList, &uid), code, lino, _return, terrno)

  code = buildExchangeOperatorParamImpl(ppRes, downstreamIdx, QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN, EX_SRC_TYPE_VSTB_TAG_SCAN,
                                        vgId, 0, pUidList, NULL, NULL, NULL, (STimeWindow){0}, NULL, false, false, true, false);
  QUERY_CHECK_CODE(code, lino, _return);

_return:
  if (code) {
    qError("failed to build exchange operator param for tag scan, code:%d", code);
  }
  taosArrayDestroy(pUidList);
  return code;
}

static int32_t buildExchangeOperatorParamForVScan(SOperatorParam** ppRes, int32_t downstreamIdx, SOrgTbInfo* pOrgTbInfo,
                                                  SDownstreamSourceNode* pNewSource) {
  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      lino = 0;

  code = buildExchangeOperatorParamImpl(ppRes, downstreamIdx, QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, EX_SRC_TYPE_VSTB_SCAN,
                                        pOrgTbInfo->vgId, 0, NULL, pOrgTbInfo, NULL, NULL, (STimeWindow){0}, pNewSource, false, true, true, true);
  QUERY_CHECK_CODE(code, lino, _return);

  return code;
_return:
  qError("failed to build exchange operator param for vscan, code:%d", code);
  return code;
}

static int32_t buildBatchExchangeOperatorParam(SOperatorParam** ppRes, int32_t downstreamIdx, SSHashObj* pVg) {
  int32_t                       code = TSDB_CODE_SUCCESS;
  int32_t                       line = 0;
  SOperatorParam*               pParam = NULL;
  SExchangeOperatorBatchParam*  pExc = NULL;
  SExchangeOperatorBasicParam   basic = {0};

  pParam = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(pParam, code, line, _return, terrno);

  pParam->opType = QUERY_NODE_PHYSICAL_PLAN_EXCHANGE;
  pParam->downstreamIdx = downstreamIdx;
  pParam->reUse = false;
  pParam->pChildren = NULL;
  pParam->value = taosMemoryMalloc(sizeof(SExchangeOperatorParam));
  QUERY_CHECK_NULL(pParam->value, code, line, _return, terrno);

  pExc = pParam->value;
  pExc->multiParams = true;
  pExc->pBatchs = tSimpleHashInit(tSimpleHashGetSize(pVg), taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  QUERY_CHECK_NULL(pExc->pBatchs, code, line, _return, terrno)

  tSimpleHashSetFreeFp(pExc->pBatchs, freeExchangeGetBasicOperatorParam);

  int32_t iter = 0;
  void*   p = NULL;
  while (NULL != (p = tSimpleHashIterate(pVg, p, &iter))) {
    int32_t* pVgId = tSimpleHashGetKey(p, NULL);
    SArray*  pUidList = *(SArray**)p;

    code = buildExchangeOperatorBasicParam(&basic, QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN,
                                           EX_SRC_TYPE_STB_JOIN_SCAN, *pVgId, 0,
                                           pUidList, NULL, NULL, NULL, NULL,
                                           (STimeWindow){0}, NULL, false, false, false);
    QUERY_CHECK_CODE(code, line, _return);

    QRY_ERR_RET(tSimpleHashPut(pExc->pBatchs, pVgId, sizeof(*pVgId), &basic, sizeof(basic)));

    basic = (SExchangeOperatorBasicParam){0};
    qTrace("build downstreamIdx %d batch scan, vgId:%d, uidNum:%" PRId64, downstreamIdx, *pVgId, (int64_t)taosArrayGetSize(pUidList));

    // already transferred to batch param, can free here
    taosArrayDestroy(pUidList);

    *(SArray**)p = NULL;
  }
  *ppRes = pParam;

  return code;
  
_return:
  qError("failed to build batch exchange operator param, code:%d", code);
  freeOperatorParam(pParam, OP_GET_PARAM);
  freeExchangeGetBasicOperatorParam(&basic);
  return code;
}

/*
 * Build one batch-exchange get-param for virtual-table dynamic execution.
 *
 * @param ppRes Output operator param.
 * @param downstreamIdx Downstream operator index to fetch from.
 * @param pTagList Optional tag values bound to each source table.
 * @param groupid Group id used by downstream operators.
 * @param pBatchMaps Hash map from vgroup id to source-table metadata array.
 * @param window Time window forwarded to downstream scan.
 * @param type Exchange source type.
 * @param srcOpType Physical scan operator type for each batch source.
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise error code.
 */
static int32_t buildBatchExchangeOperatorParamForVirtual(SOperatorParam** ppRes, int32_t downstreamIdx,
                                                         SArray* pTagList, uint64_t groupid, SHashObj* pBatchMaps,
                                                         STimeWindow window, EExchangeSourceType type,
                                                         ENodeType srcOpType) {
  int32_t                       code = TSDB_CODE_SUCCESS;
  int32_t                       lino = 0;
  SOperatorParam*               pParam = NULL;
  SExchangeOperatorBatchParam*  pExc = NULL;
  SExchangeOperatorBasicParam   basic = {0};

  pParam = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(pParam, code, lino, _return, terrno)

  pParam->value = taosMemoryMalloc(sizeof(SExchangeOperatorParam));
  QUERY_CHECK_NULL(pParam->value, code, lino, _return, terrno)

  pExc = pParam->value;
  pExc->multiParams = true;

  pExc->pBatchs = tSimpleHashInit(taosHashGetSize(pBatchMaps), taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  QUERY_CHECK_NULL(pExc->pBatchs, code, lino, _return, terrno)
  tSimpleHashSetFreeFp(pExc->pBatchs, freeExchangeGetBasicOperatorParam);

  size_t keyLen = 0;
  void*  pIter = taosHashIterate(pBatchMaps, NULL);
  while (pIter != NULL) {
    SArray*          pOrgTbInfoArray = *(SArray**)pIter;
    int32_t*         vgId = (int32_t*)taosHashGetKey(pIter, &keyLen);

    code = buildExchangeOperatorBasicParam(&basic, srcOpType,
                                           type, *vgId, groupid,
                                           NULL, NULL, pTagList, NULL, pOrgTbInfoArray,
                                           window, NULL, false, true, false);
    QUERY_CHECK_CODE(code, lino, _return);

    code = tSimpleHashPut(pExc->pBatchs, vgId, sizeof(*vgId), &basic, sizeof(basic));
    QUERY_CHECK_CODE(code, lino, _return);

    basic = (SExchangeOperatorBasicParam){0};
    pIter = taosHashIterate(pBatchMaps, pIter);
  }

  pParam->pChildren = NULL;
  pParam->opType = QUERY_NODE_PHYSICAL_PLAN_EXCHANGE;
  pParam->downstreamIdx = downstreamIdx;
  pParam->reUse = false;

  *ppRes = pParam;
  return code;

_return:
  qError("failed to build exchange operator param for vscan, code:%d", code);
  freeOperatorParam(pParam, OP_GET_PARAM);
  freeExchangeGetBasicOperatorParam(&basic);
  return code;
}

static int32_t buildMergeJoinOperatorParam(SOperatorParam** ppRes, bool initParam, SOperatorParam** ppChild0, SOperatorParam** ppChild1) {
  int32_t code = TSDB_CODE_SUCCESS;
  *ppRes = taosMemoryCalloc(1, sizeof(SOperatorParam));
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
  *ppRes = taosMemoryCalloc(1, sizeof(SOperatorParam));
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

static int32_t buildVtbScanOperatorParam(SDynQueryCtrlOperatorInfo* pInfo, SOperatorParam** ppRes, uint64_t uid) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SVTableScanOperatorParam* pVScan = NULL;
  *ppRes = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(*ppRes, code, lino, _return, terrno)

  (*ppRes)->pChildren = taosArrayInit(1, POINTER_BYTES);
  QUERY_CHECK_NULL((*ppRes)->pChildren, code, lino, _return, terrno)

  pVScan = taosMemoryMalloc(sizeof(SVTableScanOperatorParam));
  QUERY_CHECK_NULL(pVScan, code, lino, _return, terrno)
  pVScan->pOpParamArray = taosArrayInit(1, POINTER_BYTES);
  QUERY_CHECK_NULL(pVScan->pOpParamArray, code, lino, _return, terrno)
  pVScan->uid = uid;
  pVScan->window = pInfo->vtbScan.window;
  if (pInfo->vtbScan.refColGroups) {
    pVScan->pRefColGroups = taosArrayInit(taosArrayGetSize(pInfo->vtbScan.refColGroups), sizeof(SRefColIdGroup));
    QUERY_CHECK_NULL(pVScan->pRefColGroups, code, lino, _return, terrno)
    for (int32_t i = 0; i < taosArrayGetSize(pInfo->vtbScan.refColGroups); i++) {
      SRefColIdGroup* pSrc = (SRefColIdGroup*)taosArrayGet(pInfo->vtbScan.refColGroups, i);
      SRefColIdGroup  dst = {0};
      QUERY_CHECK_NULL(pSrc, code, lino, _return, terrno)
      dst.pSlotIdList = taosArrayDup(pSrc->pSlotIdList, NULL);
      QUERY_CHECK_NULL(dst.pSlotIdList, code, lino, _return, terrno)
      void* px = taosArrayPush(pVScan->pRefColGroups, &dst);
      if (NULL == px) {
        taosArrayDestroy(dst.pSlotIdList);
        dst.pSlotIdList = NULL;
      }
      QUERY_CHECK_NULL(px, code, lino, _return, terrno)
    }
  } else {
    pVScan->pRefColGroups = NULL;
  }

  (*ppRes)->opType = QUERY_NODE_PHYSICAL_PLAN_VIRTUAL_TABLE_SCAN;
  (*ppRes)->downstreamIdx = 0;
  (*ppRes)->value = pVScan;
  (*ppRes)->reUse = false;

  return TSDB_CODE_SUCCESS;
_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  if (pVScan) {
    if (pVScan->pRefColGroups) {
      taosArrayDestroyEx(pVScan->pRefColGroups, destroyRefColIdGroup);
      pVScan->pRefColGroups = NULL;
    }
    taosArrayDestroy(pVScan->pOpParamArray);
    taosMemoryFreeClear(pVScan);
  }
  if (*ppRes) {
    taosArrayDestroy((*ppRes)->pChildren);
    taosMemoryFreeClear(*ppRes);
  }
  return code;
}

static int32_t addRefColIdToRefMap(SHashObj* refMap, const char* colrefName, col_id_t colId) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  line = 0;
  SArray** sameRefColIdList = NULL;

  if (colrefName == NULL || colrefName[0] == '\0') {
    return code;
  }

  sameRefColIdList = (SArray**)taosHashGet(refMap, colrefName, strlen(colrefName));
  if (sameRefColIdList == NULL) {
    SArray* list = taosArrayInit(2, sizeof(col_id_t));
    QUERY_CHECK_NULL(list, code, line, _return, terrno)
    QUERY_CHECK_CODE(taosHashPut(refMap, colrefName, strlen(colrefName), &list, POINTER_BYTES), line, _return);
    sameRefColIdList = (SArray**)taosHashGet(refMap, colrefName, strlen(colrefName));
    QUERY_CHECK_NULL(sameRefColIdList, code, line, _return, terrno)
  }

  for (int32_t i = 0; i < taosArrayGetSize(*sameRefColIdList); i++) {
    col_id_t existing = *(col_id_t*)taosArrayGet(*sameRefColIdList, i);
    if (existing == colId) {
      return code;
    }
  }
  QUERY_CHECK_NULL(taosArrayPush(*sameRefColIdList, &colId), code, line, _return, terrno)
  return code;

_return:
  qError("%s failed at line %d since %s", __func__, line, tstrerror(code));
  return code;
}

static int32_t buildRefSlotGroupsFromRefMap(SHashObj* refMap, SArray* readColList, SArray** ppGroups) {
  int32_t   code = TSDB_CODE_SUCCESS;
  int32_t   line = 0;
  SArray*   groups = NULL;
  SHashObj* colIdToSlot = NULL;

  if (refMap == NULL || readColList == NULL) {
    return code;
  }

  if (*ppGroups) {
    taosArrayDestroyEx(*ppGroups, destroyRefColIdGroup);
    *ppGroups = NULL;
  }

  colIdToSlot = taosHashInit(taosArrayGetSize(readColList), taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT), false,
                             HASH_NO_LOCK);
  QUERY_CHECK_NULL(colIdToSlot, code, line, _return, terrno)

  // Build a quick colId -> slotId lookup for columns actually read.
  for (int32_t i = 0; i < taosArrayGetSize(readColList); i++) {
    col_id_t colId = *(col_id_t*)taosArrayGet(readColList, i);
    int32_t  slotId = i;
    code = taosHashPut(colIdToSlot, &colId, sizeof(colId), &slotId, sizeof(slotId));
    QUERY_CHECK_CODE(code, line, _return);
  }

  groups = taosArrayInit(1, sizeof(SRefColIdGroup));
  QUERY_CHECK_NULL(groups, code, line, _return, terrno)

  // Group columns that share the same ref name into slotId lists.
  void* pIter = taosHashIterate(refMap, NULL);
  while (pIter != NULL) {
    SArray* pList = *(SArray**)pIter;  // colId list
    if (pList && taosArrayGetSize(pList) > 1) {
      SArray* slotList = taosArrayInit(taosArrayGetSize(pList), sizeof(int32_t));
      QUERY_CHECK_NULL(slotList, code, line, _return, terrno)
      for (int32_t i = 0; i < taosArrayGetSize(pList); i++) {
        col_id_t colId = *(col_id_t*)taosArrayGet(pList, i);
        int32_t* slotId = taosHashGet(colIdToSlot, &colId, sizeof(colId));
        if (slotId) {
          QUERY_CHECK_NULL(taosArrayPush(slotList, slotId), code, line, _return, terrno)
        }
      }
      if (taosArrayGetSize(slotList) > 1) {
        SRefColIdGroup g = {.pSlotIdList = slotList};
        QUERY_CHECK_NULL(taosArrayPush(groups, &g), code, line, _return, terrno)
      } else {
        taosArrayDestroy(slotList);
      }
    }
    if (pList) {
      taosArrayDestroy(pList);
    }
    pIter = taosHashIterate(refMap, pIter);
  }

  if (taosArrayGetSize(groups) == 0) {
    taosArrayDestroy(groups);
    groups = NULL;
  }

_return:
  if (code) {
    qError("%s failed at line %d since %s", __func__, line, tstrerror(code));
    if (groups) {
      taosArrayDestroyEx(groups, destroyRefColIdGroup);
    }
  }
  if (refMap) {
    taosHashCleanup(refMap);
  }
  if (colIdToSlot) {
    taosHashCleanup(colIdToSlot);
  }
  *ppGroups = groups;
  return code;
}

bool colNeedScan(SOperatorInfo* pOperator, col_id_t colId) {
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = (SVtbScanDynCtrlInfo*)&pInfo->vtbScan;

  if (pVtbScan->scanAllCols) {
    return true;
  }

  // if readColSet exists, use it to check whether colId is needed, otherwise use readColList
  if (pVtbScan->readColSet) {
    return taosHashGet(pVtbScan->readColSet, &colId, sizeof(colId)) != NULL;
  }

  for (int32_t i = 0; i < taosArrayGetSize(pVtbScan->readColList); i++) {
    if (colId == *(col_id_t*)taosArrayGet(pVtbScan->readColList, i)) {
      return true;
    }
  }
  return false;
}

static int32_t buildExternalWindowOperatorParam(SDynQueryCtrlOperatorInfo* pInfo, SOperatorParam** ppRes, SArray* pWins, int32_t idx) {
  int32_t                       code = TSDB_CODE_SUCCESS;
  int32_t                       lino = 0;
  SExternalWindowOperatorParam* pExtWinOp = NULL;

  *ppRes = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(*ppRes, code, lino, _return, terrno)

  pExtWinOp = taosMemoryMalloc(sizeof(SExternalWindowOperatorParam));
  QUERY_CHECK_NULL(pExtWinOp, code, lino, _return, terrno)

  pExtWinOp->ExtWins = taosArrayDup(pWins, NULL);
  QUERY_CHECK_NULL(pExtWinOp->ExtWins, code, lino, _return, terrno)

  SExtWinTimeWindow *firstWin = (SExtWinTimeWindow *)taosArrayGet(pWins, 0);
  SExtWinTimeWindow *lastWin = (SExtWinTimeWindow *)taosArrayGet(pWins, taosArrayGetSize(pWins) - 1);

  QUERY_CHECK_NULL(firstWin, code, lino, _return, terrno)
  QUERY_CHECK_NULL(lastWin, code, lino, _return, terrno)

  (*ppRes)->pChildren = taosArrayInit(1, POINTER_BYTES);
  QUERY_CHECK_NULL((*ppRes)->pChildren, code, lino, _return, terrno)

  SOperatorParam* pExchangeOperator = NULL;
  STimeWindow     twin = {.skey = firstWin->tw.skey, .ekey = lastWin->tw.ekey};
  code = buildExchangeOperatorParamForExternalWindow(&pExchangeOperator, 0, twin);
  QUERY_CHECK_CODE(code, lino, _return);
  QUERY_CHECK_NULL(taosArrayPush((*ppRes)->pChildren, &pExchangeOperator), code, lino, _return, terrno)

  (*ppRes)->opType = QUERY_NODE_PHYSICAL_PLAN_EXTERNAL_WINDOW;
  (*ppRes)->downstreamIdx = idx;
  (*ppRes)->value = pExtWinOp;
  (*ppRes)->reUse = false;

  return code;
_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  if (pExtWinOp) {
    if (pExtWinOp->ExtWins) {
      taosArrayDestroy(pExtWinOp->ExtWins);
    }
    taosMemoryFree(pExtWinOp);
  }
  if (*ppRes) {
    if ((*ppRes)->pChildren) {
      for (int32_t i = 0; i < taosArrayGetSize((*ppRes)->pChildren); ++i) {
        SOperatorParam* pChildParam = taosArrayGetP((*ppRes)->pChildren, i);
        if (pChildParam) {
          freeOperatorParam(pChildParam, OP_GET_PARAM);
        }
      }
      taosArrayDestroy((*ppRes)->pChildren);
    }
    taosMemoryFree(*ppRes);
    *ppRes = NULL;
  }
  return code;
}

static int32_t buildMergeOperatorParam(SDynQueryCtrlOperatorInfo* pInfo, SOperatorParam** ppRes, SArray* pWins,
                                       int32_t numOfDownstream, int32_t numOfWins) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SMergeOperatorParam*      pMergeOp = NULL;

  *ppRes = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(*ppRes, code, lino, _return, terrno)

  (*ppRes)->pChildren = taosArrayInit(numOfDownstream, POINTER_BYTES);
  QUERY_CHECK_NULL((*ppRes)->pChildren, code, lino, _return, terrno)

  pMergeOp = taosMemoryMalloc(sizeof(SMergeOperatorParam));
  QUERY_CHECK_NULL(pMergeOp, code, lino, _return, terrno)

  pMergeOp->winNum = numOfWins;

  for (int32_t i = 0; i < numOfDownstream; i++) {
    SOperatorParam* pExternalWinParam = NULL;
    code = buildExternalWindowOperatorParam(pInfo, &pExternalWinParam, pWins, i);
    QUERY_CHECK_CODE(code, lino, _return);
    QUERY_CHECK_NULL(taosArrayPush((*ppRes)->pChildren, &pExternalWinParam), code, lino, _return, terrno)
  }

  (*ppRes)->opType = QUERY_NODE_PHYSICAL_PLAN_MERGE;
  (*ppRes)->downstreamIdx = 0;
  (*ppRes)->value = pMergeOp;
  (*ppRes)->reUse = false;

  return TSDB_CODE_SUCCESS;
_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  if (pMergeOp) {
    taosMemoryFree(pMergeOp);
  }
  if (*ppRes) {
    if ((*ppRes)->pChildren) {
      for (int32_t i = 0; i < taosArrayGetSize((*ppRes)->pChildren); i++) {
        SOperatorParam* pChildParam = taosArrayGetP((*ppRes)->pChildren, i);
        if (pChildParam) {
          freeOperatorParam(pChildParam, OP_GET_PARAM);
        }
      }
      taosArrayDestroy((*ppRes)->pChildren);
    }
    taosMemoryFree(*ppRes);
    *ppRes = NULL;
  }
  return code;
}

/*
 * Build merge operator params for vtable ts-scan mode.
 *
 * @param pInfo Dynamic-query control operator runtime info.
 * @param ppRes Output merge operator param.
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise error code.
 */
static int32_t buildMergeOperatorParamForTsScan(SDynQueryCtrlOperatorInfo* pInfo, int32_t numOfDownstream,
                                                SOperatorParam** ppRes) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SOperatorParam*           pParam = NULL;
  SOperatorParam*           pExchangeParam = NULL;
  SVtbScanDynCtrlInfo*      pVtbScan = (SVtbScanDynCtrlInfo*)&pInfo->vtbScan;

  pParam = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(pParam, code, lino, _return, terrno)

  pParam->opType = QUERY_NODE_PHYSICAL_PLAN_MERGE;
  pParam->downstreamIdx = 0;
  pParam->reUse = false;
  pParam->pChildren = taosArrayInit(numOfDownstream, POINTER_BYTES);
  QUERY_CHECK_NULL(pParam->pChildren, code, lino, _return, terrno)

  pParam->value = taosMemoryMalloc(sizeof(SMergeOperatorParam));
  QUERY_CHECK_NULL(pParam->value, code, lino, _return, terrno)

  for (int32_t i = 0; i < numOfDownstream; i++) {
    code = buildBatchExchangeOperatorParamForVirtual(&pExchangeParam, i, NULL, 0, pVtbScan->otbVgIdToOtbInfoArrayMap,
                                                     (STimeWindow){.skey = INT64_MAX, .ekey = INT64_MIN},
                                                     EX_SRC_TYPE_VSTB_TS_SCAN,
                                                     QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN);
    QUERY_CHECK_CODE(code, lino, _return);
    QUERY_CHECK_NULL(taosArrayPush(pParam->pChildren, &pExchangeParam), code, lino, _return, terrno)
    pExchangeParam = NULL;
  }

  *ppRes = pParam;

  return code;
_return:
  if (pExchangeParam) {
    freeOperatorParam(pExchangeParam, OP_GET_PARAM);
  }
  if (pParam) {
    freeOperatorParam(pParam, OP_GET_PARAM);
  }
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

static int32_t buildAggOperatorParam(SDynQueryCtrlOperatorInfo* pInfo, SOperatorParam** ppRes) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SOperatorParam*           pParam = NULL;
  SOperatorParam*           pExchangeParam = NULL;
  SVtbScanDynCtrlInfo*      pVtbScan = (SVtbScanDynCtrlInfo*)&pInfo->vtbScan;
  bool                      freeExchange = false;

  pParam = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(pParam, code, lino, _return, terrno)

  pParam->pChildren = taosArrayInit(1, POINTER_BYTES);
  QUERY_CHECK_NULL(pParam->pChildren, code, lino, _return, terrno)

  pParam->value = taosMemoryMalloc(sizeof(SAggOperatorParam));
  QUERY_CHECK_NULL(pParam->value, code, lino, _return, terrno)

  code = buildBatchExchangeOperatorParamForVirtual(
      &pExchangeParam, 0, NULL, 0, pVtbScan->otbVgIdToOtbInfoArrayMap,
      (STimeWindow){.skey = INT64_MAX, .ekey = INT64_MIN}, EX_SRC_TYPE_VSTB_AGG_SCAN,
      QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN);
  QUERY_CHECK_CODE(code, lino, _return);

  freeExchange = true;

  QUERY_CHECK_NULL(taosArrayPush(pParam->pChildren, &pExchangeParam), code, lino, _return, terrno)

  freeExchange = false;

  pParam->opType = QUERY_NODE_PHYSICAL_PLAN_HASH_AGG;
  pParam->downstreamIdx = 0;
  pParam->reUse = false;

  *ppRes = pParam;

  return code;
_return:
  if (freeExchange) {
    freeOperatorParam(pExchangeParam, OP_GET_PARAM);
  }
  if (pParam) {
    freeOperatorParam(pParam, OP_GET_PARAM);
  }
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

static int32_t buildAggOperatorParamWithGroupId(SDynQueryCtrlOperatorInfo* pInfo, uint64_t groupid,
                                                SOperatorParam** ppRes) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SVtbScanDynCtrlInfo*      pVtbScan = (SVtbScanDynCtrlInfo*)&pInfo->vtbScan;
  SOperatorParam*           pParam = NULL;
  SOperatorParam*           pExchangeParam = NULL;
  SHashObj*                 otbVgIdToOtbInfoArrayMap = NULL;
  bool                      freeExchange = false;
  void*                     pIter = taosHashGet(pVtbScan->vtbGroupIdToVgIdMapMap, &groupid, sizeof(groupid));

  if (!pIter) {
    *ppRes = NULL;
    return code;
  }

  otbVgIdToOtbInfoArrayMap = *(SHashObj**)pIter;

  pParam = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(pParam, code, lino, _return, terrno)

  pParam->pChildren = taosArrayInit(1, POINTER_BYTES);
  QUERY_CHECK_NULL(pParam->pChildren, code, lino, _return, terrno)

  code = buildBatchExchangeOperatorParamForVirtual(
      &pExchangeParam, 0, NULL, groupid, otbVgIdToOtbInfoArrayMap,
      (STimeWindow){.skey = INT64_MAX, .ekey = INT64_MIN}, EX_SRC_TYPE_VSTB_AGG_SCAN,
      QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN);
  QUERY_CHECK_CODE(code, lino, _return);

  freeExchange = true;

  QUERY_CHECK_NULL(taosArrayPush(pParam->pChildren, &pExchangeParam), code, lino, _return, terrno)

  freeExchange = false;

  pParam->opType = QUERY_NODE_PHYSICAL_PLAN_HASH_AGG;
  pParam->downstreamIdx = 0;
  pParam->value = NULL;
  pParam->reUse = false;

  *ppRes = pParam;

  return code;
_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  if (freeExchange) {
    freeOperatorParam(pExchangeParam, OP_GET_PARAM);
  }
  if (pParam) {
    freeOperatorParam(pParam, OP_GET_PARAM);
  }
  return code;
}

static int32_t buildAggOperatorParamForSingleChild(SDynQueryCtrlOperatorInfo* pInfo, tb_uid_t uid,
                                                   uint64_t groupid, SArray* pTagList, SOperatorParam** ppRes) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SVtbScanDynCtrlInfo*      pVtbScan = (SVtbScanDynCtrlInfo*)&pInfo->vtbScan;
  SOperatorParam*           pParam = NULL;
  SHashObj*                 pOtbVgIdToOtbInfoArrayMap = NULL;
  void*                     pIter = taosHashGet(pVtbScan->vtbUidToVgIdMapMap, &uid, sizeof(uid));

  if (pIter) {
    pOtbVgIdToOtbInfoArrayMap = *(SHashObj**)taosHashGet(pVtbScan->vtbUidToVgIdMapMap, &uid, sizeof(uid));

    code = buildBatchExchangeOperatorParamForVirtual(
        &pParam, 0, pTagList, groupid, pOtbVgIdToOtbInfoArrayMap,
        (STimeWindow){.skey = INT64_MAX, .ekey = INT64_MIN}, EX_SRC_TYPE_VSTB_AGG_SCAN,
        QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN);
    QUERY_CHECK_CODE(code, lino, _return);

    *ppRes = pParam;
  } else {
    *ppRes = NULL;
  }

  return code;
_return:
  if (pParam) {
    freeOperatorParam(pParam, OP_GET_PARAM);
  }
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
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

      for (int i = (int)pBlock->pDataBlock->size; i < pStbJoin->pOutputDataBlockDesc->pSlots->length; i++) {
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
  if (code) {
    qError("%s failed since %s", __func__, tstrerror(code));
    pOperator->pTaskInfo->code = code;
    T_LONG_JMP(pOperator->pTaskInfo->env, code);
  } else {
    code = seqStableJoinComposeRes(pStbJoin, *pRes);
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
  QUERY_CHECK_NULL(pScanResInfo->vtbScan.pRsp, code, lino, _return, terrno)

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

static int32_t buildDbVgInfoMap(SOperatorInfo* pOperator, SMsgCb* pMsgCb, SName* name, SExecTaskInfo* pTaskInfo, SUseDbOutput* output) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    lino = 0;
  char*                      buf1 = NULL;
  SUseDbReq*                 pReq = NULL;
  SDynQueryCtrlOperatorInfo* pScanResInfo = (SDynQueryCtrlOperatorInfo*)pOperator->info;

  pReq = taosMemoryMalloc(sizeof(SUseDbReq));
  QUERY_CHECK_NULL(pReq, code, lino, _return, terrno)
  code = tNameGetFullDbName(name, pReq->db);
  QUERY_CHECK_CODE(code, lino, _return);
  int32_t contLen = tSerializeSUseDbReq(NULL, 0, pReq);
  buf1 = taosMemoryCalloc(1, contLen);
  QUERY_CHECK_NULL(buf1, code, lino, _return, terrno)
  int32_t tempRes = tSerializeSUseDbReq(buf1, contLen, pReq);
  if (tempRes < 0) {
    QUERY_CHECK_CODE(terrno, lino, _return);
  }

  // send the fetch remote task result request
  SMsgSendInfo* pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  QUERY_CHECK_NULL(pMsgSendInfo, code, lino, _return, terrno)

  pMsgSendInfo->param = pOperator;
  pMsgSendInfo->msgInfo.pData = buf1;
  pMsgSendInfo->msgInfo.len = contLen;
  pMsgSendInfo->msgType = TDMT_MND_GET_DB_INFO;
  pMsgSendInfo->fp = dynProcessUseDbRsp;
  pMsgSendInfo->requestId = pTaskInfo->id.queryId;

  code = asyncSendMsgToServer(pMsgCb->clientRpc, &pScanResInfo->vtbScan.epSet, NULL, pMsgSendInfo);
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
  return getVgIdAndEpSet(dbInfo, dbFName, vgId, tbName, NULL);
}

/*
 * Resolve vgroup id and optional vnode epSet for a table.
 *
 * @param dbInfo   database vgroup info cache
 * @param dbFName  full database name
 * @param vgId     output vgroup id
 * @param tbName   table name inside database
 * @param pEpSet   optional output vnode epSet
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t getVgIdAndEpSet(SDBVgInfo* dbInfo, char* dbFName, int32_t* vgId, char *tbName, SEpSet* pEpSet) {
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
  if (pEpSet != NULL) {
    *pEpSet = vgInfo->epSet;
  }

_return:
  return code;
}

/*
 * Release one array of layered systable-scan requests.
 *
 * @param info  hash value that stores SArray<SSysTableScanVtbRefReq>*
 *
 * @return none
 */
static void destroySysScanReqArray(void* info) {
  SArray* pReqs = *(SArray**)info;
  if (pReqs != NULL) {
    taosArrayDestroy(pReqs);
  }
}

/*
 * Resolve vgroup id for one referenced table.
 *
 * @param pOperator dyn operator context
 * @param colRef    referenced column name in db.table.col format
 * @param pVgId     output vgroup id
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t dynGetRefVgId(SOperatorInfo* pOperator, const char* colRef, int32_t* pVgId) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         line = 0;
  SDBVgInfo*      dbVgInfo = NULL;
  SName           name = {0};
  SColRefNameView refView = {0};
  char            dbName[TSDB_DB_NAME_LEN] = {0};
  char            tbName[TSDB_TABLE_NAME_LEN] = {0};
  char            dbFName[TSDB_DB_FNAME_LEN] = {0};

  code = parseColRefNameView(colRef, &refView);
  QUERY_CHECK_CODE(code, line, _return);

  memcpy(dbName, refView.dbName, refView.dbNameLen);
  memcpy(tbName, refView.tbName, refView.tbNameLen);
  toName(((SDynQueryCtrlOperatorInfo*)pOperator->info)->vtbScan.acctId, dbName, tbName, &name);

  code = getDbVgInfo(pOperator, &name, &dbVgInfo);
  QUERY_CHECK_CODE(code, line, _return);
  code = tNameGetFullDbName(&name, dbFName);
  QUERY_CHECK_CODE(code, line, _return);
  code = getVgId(dbVgInfo, dbFName, pVgId, name.tname);
  QUERY_CHECK_CODE(code, line, _return);

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed since %s, line %d, ref:%s", __func__, tstrerror(code), line, colRef ? colRef : "<null>");
  }
  return code;
}

/*
 * Build one local systable-scan get-param for layered reference lookup.
 *
 * @param ppRes   output operator param
 * @param pReqByVg request map keyed by vgId
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t buildSysTableScanOperatorParam(SOperatorParam** ppRes, SHashObj* pReqByVg) {
  int32_t                     code = TSDB_CODE_SUCCESS;
  int32_t                     line = 0;
  SOperatorParam*             pParam = NULL;
  SSysTableScanOperatorParam* pSysScan = NULL;
  SArray*                     pReqs = NULL;
  void*                       pIter = NULL;

  pParam = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(pParam, code, line, _return, terrno);
  pSysScan = taosMemoryCalloc(1, sizeof(SSysTableScanOperatorParam));
  QUERY_CHECK_NULL(pSysScan, code, line, _return, terrno);

  pReqs = taosArrayInit(taosHashGetSize(pReqByVg), sizeof(SSysTableScanVtbRefReq));
  QUERY_CHECK_NULL(pReqs, code, line, _return, terrno);

  while ((pIter = taosHashIterate(pReqByVg, pIter)) != NULL) {
    SArray* pLocalReqs = *(SArray**)pIter;
    for (int32_t i = 0; i < taosArrayGetSize(pLocalReqs); ++i) {
      SSysTableScanVtbRefReq* pReq = taosArrayGet(pLocalReqs, i);
      QUERY_CHECK_NULL(taosArrayPush(pReqs, pReq), code, line, _return, terrno);
    }
  }

  pSysScan->pVtbRefReqs = pReqs;
  pReqs = NULL;

  pParam->opType = QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN;
  pParam->downstreamIdx = 0;
  pParam->value = pSysScan;
  pParam->pChildren = NULL;
  pParam->reUse = false;

  *ppRes = pParam;
  return code;

_return:
  taosArrayDestroy(pReqs);
  if (pSysScan != NULL) {
    taosArrayDestroy(pSysScan->pVtbRefReqs);
    pSysScan->pVtbRefReqs = NULL;
    taosMemoryFree(pSysScan);
  }
  taosMemoryFree(pParam);
  qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
  return code;
}

/*
 * Build one batch exchange get-param that forwards layered systable-scan requests.
 *
 * @param ppRes         output operator param
 * @param downstreamIdx downstream operator index
 * @param pReqByVg      request map keyed by target vgId
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t buildBatchExchangeOperatorParamForSysScan(SOperatorParam** ppRes, int32_t downstreamIdx,
                                                         SHashObj* pReqByVg) {
  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      line = 0;
  SOperatorParam*              pParam = NULL;
  SExchangeOperatorBatchParam* pExc = NULL;
  SExchangeOperatorBasicParam  basic = {0};
  void*                        pIter = NULL;

  pParam = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(pParam, code, line, _return, terrno);

  pParam->opType = QUERY_NODE_PHYSICAL_PLAN_EXCHANGE;
  pParam->downstreamIdx = downstreamIdx;
  pParam->reUse = false;
  pParam->pChildren = NULL;
  pParam->value = taosMemoryMalloc(sizeof(SExchangeOperatorParam));
  QUERY_CHECK_NULL(pParam->value, code, line, _return, terrno);

  pExc = (SExchangeOperatorBatchParam*)pParam->value;
  pExc->multiParams = true;
  pExc->pBatchs = tSimpleHashInit(taosHashGetSize(pReqByVg), taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  QUERY_CHECK_NULL(pExc->pBatchs, code, line, _return, terrno);
  tSimpleHashSetFreeFp(pExc->pBatchs, freeExchangeGetBasicOperatorParam);

  while ((pIter = taosHashIterate(pReqByVg, pIter)) != NULL) {
    int32_t* vgId = (int32_t*)taosHashGetKey(pIter, NULL);
    SArray*  pReqs = *(SArray**)pIter;

    code = buildExchangeOperatorBasicParam(&basic, QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN,
                                           EX_SRC_TYPE_VSTB_SYS_SCAN, *vgId, 0, NULL, NULL, NULL,
                                           pReqs, NULL, (STimeWindow){0}, NULL, false, true, false);
    QUERY_CHECK_CODE(code, line, _return);

    code = tSimpleHashPut(pExc->pBatchs, vgId, sizeof(*vgId), &basic, sizeof(basic));
    QUERY_CHECK_CODE(code, line, _return);
    basic = (SExchangeOperatorBasicParam){0};
  }

  *ppRes = pParam;
  return code;

_return:
  freeExchangeGetBasicOperatorParam(&basic);
  freeOperatorParam(pParam, OP_GET_PARAM);
  qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
  return code;
}

/*
 * Build a merge get-param whose children forward layered systable-scan requests.
 *
 * @param pOperator     merge operator
 * @param pReqByVg      request map keyed by target vgId
 * @param ppRes         output operator param
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t buildMergeOperatorParamForSysScan(SOperatorInfo* pOperator, SHashObj* pReqByVg, SOperatorParam** ppRes) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         line = 0;
  SOperatorParam* pChild = NULL;

  *ppRes = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(*ppRes, code, line, _return, terrno);

  (*ppRes)->opType = QUERY_NODE_PHYSICAL_PLAN_MERGE;
  (*ppRes)->downstreamIdx = 0;
  (*ppRes)->reUse = false;
  (*ppRes)->value = taosMemoryCalloc(1, sizeof(SMergeOperatorParam));
  QUERY_CHECK_NULL((*ppRes)->value, code, line, _return, terrno);
  ((SMergeOperatorParam*)(*ppRes)->value)->winNum = 1;

  (*ppRes)->pChildren = taosArrayInit(pOperator->numOfDownstream, POINTER_BYTES);
  QUERY_CHECK_NULL((*ppRes)->pChildren, code, line, _return, terrno);

  for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
    code = buildBatchExchangeOperatorParamForSysScan(&pChild, i, pReqByVg);
    QUERY_CHECK_CODE(code, line, _return);
    QUERY_CHECK_NULL(taosArrayPush((*ppRes)->pChildren, &pChild), code, line, _return, terrno);
    pChild = NULL;
  }

  return code;

_return:
  if (pChild != NULL) {
    freeOperatorParam(pChild, OP_GET_PARAM);
  }
  freeOperatorParam(*ppRes, OP_GET_PARAM);
  *ppRes = NULL;
  qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
  return code;
}

/*
 * Build a layered systable-scan get-param that matches the current operator tree.
 *
 * @param pTargetOp     operator used to read `ins_vc_cols`
 * @param downstreamIdx downstream index when the target is attached to a merge parent
 * @param pReqByVg      request map keyed by target vgId
 * @param ppRes         output operator param
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t buildSysScanLayerOperatorParam(SOperatorInfo* pTargetOp, int32_t downstreamIdx, SHashObj* pReqByVg,
                                              SOperatorParam** ppRes) {
  if (pTargetOp->operatorType == QUERY_NODE_PHYSICAL_PLAN_MERGE) {
    return buildMergeOperatorParamForSysScan(pTargetOp, pReqByVg, ppRes);
  }

  if (pTargetOp->operatorType == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE) {
    return buildBatchExchangeOperatorParamForSysScan(ppRes, downstreamIdx, pReqByVg);
  }

  return buildSysTableScanOperatorParam(ppRes, pReqByVg);
}

/*
 * Build the first bootstrap get-param for a dynamic `ins_vc_cols` exchange chain.
 *
 * @param ppRes         output operator param
 * @param pExchangeOp   target exchange operator
 * @param downstreamIdx downstream index used by merge parent
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t buildBootstrapSysScanExchangeParam(SOperatorParam** ppRes, SOperatorInfo* pExchangeOp,
                                                  int32_t downstreamIdx) {
  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      line = 0;
  SOperatorParam*              pParam = NULL;
  SExchangeOperatorBatchParam* pExc = NULL;
  SExchangeOperatorBasicParam  basic = {0};
  SExchangeInfo*               pExchangeInfo = pExchangeOp->info;

  pParam = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(pParam, code, line, _return, terrno);

  pParam->opType = QUERY_NODE_PHYSICAL_PLAN_EXCHANGE;
  pParam->downstreamIdx = downstreamIdx;
  pParam->reUse = false;
  pParam->pChildren = NULL;
  pParam->value = taosMemoryMalloc(sizeof(SExchangeOperatorParam));
  QUERY_CHECK_NULL(pParam->value, code, line, _return, terrno);

  pExc = (SExchangeOperatorBatchParam*)pParam->value;
  pExc->multiParams = true;
  pExc->pBatchs =
      tSimpleHashInit(TMAX((int32_t)taosArrayGetSize(pExchangeInfo->pSources), 1), taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  QUERY_CHECK_NULL(pExc->pBatchs, code, line, _return, terrno);
  tSimpleHashSetFreeFp(pExc->pBatchs, freeExchangeGetBasicOperatorParam);

  for (int32_t i = 0; i < taosArrayGetSize(pExchangeInfo->pSources); ++i) {
    SDownstreamSourceNode* pSrc = taosArrayGet(pExchangeInfo->pSources, i);
    QUERY_CHECK_NULL(pSrc, code, line, _return, terrno);

    SArray* pFakeReqs = taosArrayInit(1, sizeof(int32_t));
    QUERY_CHECK_NULL(pFakeReqs, code, line, _return, terrno);
    code = buildExchangeOperatorBasicParam(&basic, QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN, EX_SRC_TYPE_VSTB_SYS_SCAN,
                                           pSrc->addr.nodeId, 0, NULL, NULL, NULL, pFakeReqs, NULL, (STimeWindow){0},
                                           NULL, false, true, false);
    taosArrayDestroy(pFakeReqs);
    pFakeReqs = NULL;
    QUERY_CHECK_CODE(code, line, _return);

    code = tSimpleHashPut(pExc->pBatchs, &pSrc->addr.nodeId, sizeof(pSrc->addr.nodeId), &basic, sizeof(basic));
    QUERY_CHECK_CODE(code, line, _return);
    basic = (SExchangeOperatorBasicParam){0};
  }

  *ppRes = pParam;
  return code;

_return:
  freeExchangeGetBasicOperatorParam(&basic);
  freeOperatorParam(pParam, OP_GET_PARAM);
  qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
  return code;
}

/*
 * Build the first bootstrap get-param for a dynamic `ins_vc_cols` merge chain.
 *
 * @param pMergeOp merge operator on top of exchange children
 * @param ppRes    output operator param
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t buildBootstrapSysScanMergeParam(SOperatorInfo* pMergeOp, SOperatorParam** ppRes) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         line = 0;
  SOperatorParam* pChild = NULL;

  *ppRes = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(*ppRes, code, line, _return, terrno);

  (*ppRes)->opType = QUERY_NODE_PHYSICAL_PLAN_MERGE;
  (*ppRes)->downstreamIdx = 0;
  (*ppRes)->reUse = false;
  (*ppRes)->value = taosMemoryCalloc(1, sizeof(SMergeOperatorParam));
  QUERY_CHECK_NULL((*ppRes)->value, code, line, _return, terrno);
  ((SMergeOperatorParam*)(*ppRes)->value)->winNum = 1;

  (*ppRes)->pChildren = taosArrayInit(pMergeOp->numOfDownstream, POINTER_BYTES);
  QUERY_CHECK_NULL((*ppRes)->pChildren, code, line, _return, terrno);

  for (int32_t i = 0; i < pMergeOp->numOfDownstream; ++i) {
    code = buildBootstrapSysScanExchangeParam(&pChild, pMergeOp->pDownstream[i], i);
    QUERY_CHECK_CODE(code, line, _return);
    QUERY_CHECK_NULL(taosArrayPush((*ppRes)->pChildren, &pChild), code, line, _return, terrno);
    pChild = NULL;
  }

  return code;

_return:
  if (pChild != NULL) {
    freeOperatorParam(pChild, OP_GET_PARAM);
  }
  freeOperatorParam(*ppRes, OP_GET_PARAM);
  *ppRes = NULL;
  qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
  return code;
}

/*
 * Build the first bootstrap get-param for the initial dynamic `ins_vc_cols` read.
 *
 * @param pTargetOp target operator used to read `ins_vc_cols`
 * @param ppRes     output operator param
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t buildBootstrapSysScanOperatorParam(SOperatorInfo* pTargetOp, SOperatorParam** ppRes) {
  int32_t                     code = TSDB_CODE_SUCCESS;
  int32_t                     line = 0;
  SOperatorParam*             pParam = NULL;
  SSysTableScanOperatorParam* pSysScan = NULL;

  *ppRes = NULL;

  if (pTargetOp->operatorType == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE) {
    return buildBootstrapSysScanExchangeParam(ppRes, pTargetOp, 0);
  }

  pParam = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(pParam, code, line, _return, terrno);
  pSysScan = taosMemoryCalloc(1, sizeof(SSysTableScanOperatorParam));
  QUERY_CHECK_NULL(pSysScan, code, line, _return, terrno);

  pParam->opType = QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN;
  pParam->downstreamIdx = 0;
  pParam->value = pSysScan;
  pParam->pChildren = NULL;
  pParam->reUse = false;

  *ppRes = pParam;
  return code;

_return:
  taosMemoryFree(pSysScan);
  taosMemoryFree(pParam);
  qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
  return code;
}

/*
 * Fetch one `ins_vc_cols` block and bootstrap the first dynamic systable read with an empty get-param.
 *
 * @param pTargetOp     systable scan operator used to read `ins_vc_cols`
 * @param pBootstrapped whether the initial bootstrap fetch has been issued
 * @param ppBlock       output block pointer
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t dynFetchInitialSysScanBlock(SOperatorInfo* pTargetOp, bool* pBootstrapped, SSDataBlock** ppBlock) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         line = 0;
  SOperatorParam* pParam = NULL;

  if (!(*pBootstrapped)) {
    code = buildBootstrapSysScanOperatorParam(pTargetOp, &pParam);
    QUERY_CHECK_CODE(code, line, _return);
    code = pTargetOp->fpSet.getNextExtFn(pTargetOp, pParam, ppBlock);
    QUERY_CHECK_CODE(code, line, _return);
    *pBootstrapped = true;
    return code;
  }

  code = pTargetOp->fpSet.getNextFn(pTargetOp, ppBlock);
  QUERY_CHECK_CODE(code, line, _return);
  return code;

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
  }
  return code;
}

/*
 * Cache one final resolved column reference.
 *
 * @param pVtbScan  virtual-table dyn runtime info
 * @param rootRef   root reference key
 * @param fullColRef final raw column ref in db.table.col format
 * @param vgId      final raw vgroup id
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t dynPutResolvedColRef(SVtbScanDynCtrlInfo* pVtbScan, const char* rootRef, const char* fullColRef,
                                    int32_t vgId) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             line = 0;
  SDynResolvedColRef* pResolved = NULL;
  SColRefNameView     refView = {0};

  pResolved = taosMemoryCalloc(1, sizeof(SDynResolvedColRef));
  QUERY_CHECK_NULL(pResolved, code, line, _return, terrno);

  code = parseColRefNameView(fullColRef, &refView);
  QUERY_CHECK_CODE(code, line, _return);

  memcpy(pResolved->dbName, refView.dbName, refView.dbNameLen);
  memcpy(pResolved->tbName, refView.tbName, refView.tbNameLen);
  memcpy(pResolved->colName, refView.colName, refView.colNameLen);
  tstrncpy(pResolved->fullColRef, fullColRef, sizeof(pResolved->fullColRef));
  pResolved->vgId = vgId;

  code = taosHashPut(pVtbScan->resolvedColRefMap, rootRef, strlen(rootRef), &pResolved, POINTER_BYTES);
  QUERY_CHECK_CODE(code, line, _return);

  return code;
_return:
  taosMemoryFree(pResolved);
  qError("%s failed since %s, line %d, ref:%s", __func__, tstrerror(code), line, rootRef);
  return code;
}

/*
 * Append one layered sysscan request into the vg-keyed request map.
 *
 * @param pReqByVg hash keyed by vgId, value SArray<SSysTableScanVtbRefReq>*
 * @param vgId     target vgroup id
 * @param colRef   referenced column name in db.table.col format
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t dynAddSysScanReqToMap(SHashObj* pReqByVg, int32_t vgId, const char* colRef) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  line = 0;
  SArray**                 ppReqs = (SArray**)taosHashGet(pReqByVg, &vgId, sizeof(vgId));
  SArray*                  pReqs = NULL;
  SSysTableScanVtbRefReq   req = {.vgId = vgId};
  SColRefNameView          refView = {0};

  code = parseColRefNameView(colRef, &refView);
  QUERY_CHECK_CODE(code, line, _return);

  if (ppReqs == NULL) {
    pReqs = taosArrayInit(1, sizeof(SSysTableScanVtbRefReq));
    QUERY_CHECK_NULL(pReqs, code, line, _return, terrno);
    code = taosHashPut(pReqByVg, &vgId, sizeof(vgId), &pReqs, POINTER_BYTES);
    QUERY_CHECK_CODE(code, line, _return);
    ppReqs = (SArray**)taosHashGet(pReqByVg, &vgId, sizeof(vgId));
    QUERY_CHECK_NULL(ppReqs, code, line, _return, terrno);
  }

  memcpy(req.dbName, refView.dbName, refView.dbNameLen);
  memcpy(req.tbName, refView.tbName, refView.tbNameLen);
  memcpy(req.colName, refView.colName, refView.colNameLen);
  QUERY_CHECK_NULL(taosArrayPush(*ppReqs, &req), code, line, _return, terrno);
  return code;

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed since %s, line %d, vgId:%d, ref:%s", __func__, tstrerror(code), line, vgId,
           colRef ? colRef : "<null>");
  }
  return code;
}

/*
 * Collect current-layer request maps from unresolved refs.
 *
 * @param pOperator   dyn operator context
 * @param pPendingMap unresolved refs keyed by root ref, value current ref
 * @param ppReqByVg   output request map keyed by vgId
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t dynBuildSysScanLayerReqs(SOperatorInfo* pOperator, SHashObj* pPendingMap, SHashObj** ppReqByVg) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            line = 0;
  SHashObj*          pReqByVg = NULL;       // key: vgId, value: SArray<SSysTableScanVtbRefReq>*
  SHashObj*          pSeenRef = NULL;       // key: currentRef, value: uint8_t marker for dedup inside this layer
  void*              pIter = NULL;

  pReqByVg = taosHashInit(taosHashGetSize(pPendingMap), taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  QUERY_CHECK_NULL(pReqByVg, code, line, _return, terrno);
  taosHashSetFreeFp(pReqByVg, destroySysScanReqArray);

  pSeenRef = taosHashInit(taosHashGetSize(pPendingMap), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  QUERY_CHECK_NULL(pSeenRef, code, line, _return, terrno);

  while ((pIter = taosHashIterate(pPendingMap, pIter)) != NULL) {
    SDynRefKey* pCurrentRef = (SDynRefKey*)pIter;
    uint8_t     seen = 1;

    if (taosHashGet(pSeenRef, pCurrentRef->colRef, strlen(pCurrentRef->colRef)) == NULL) {
      int32_t vgId = 0;

      code = taosHashPut(pSeenRef, pCurrentRef->colRef, strlen(pCurrentRef->colRef), &seen, sizeof(seen));
      QUERY_CHECK_CODE(code, line, _return);
      code = dynGetRefVgId(pOperator, pCurrentRef->colRef, &vgId);
      QUERY_CHECK_CODE(code, line, _return);
      code = dynAddSysScanReqToMap(pReqByVg, vgId, pCurrentRef->colRef);
      QUERY_CHECK_CODE(code, line, _return);
    }
  }

  *ppReqByVg = pReqByVg;
  pReqByVg = NULL;

_return:
  taosHashCleanup(pSeenRef);
  taosHashCleanup(pReqByVg);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
  }
  return code;
}

/*
 * Read one systable-scan layer and collect hit rows and next-level refs.
 *
 * @param pTargetOp   operator used to read `ins_vc_cols`
 * @param pReqByVg    request map keyed by vgId
 * @param pHitMap     output current-ref hit markers
 * @param pNextRefMap output current-ref to next-ref map
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t dynCollectSysScanNextRefs(SOperatorInfo* pTargetOp, SHashObj* pReqByVg, SHashObj* pHitMap,
                                         SHashObj* pNextRefMap) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         line = 0;
  size_t          len = 0;
  SOperatorParam* pParam = NULL;
  SSDataBlock*    pBlock = NULL;

  code = buildSysScanLayerOperatorParam(pTargetOp, 0, pReqByVg, &pParam);
  QUERY_CHECK_CODE(code, line, _return);

  pTargetOp->status = OP_NOT_OPENED;
  code = pTargetOp->fpSet.getNextExtFn(pTargetOp, pParam, &pBlock);
  QUERY_CHECK_CODE(code, line, _return);
  pParam = NULL;

  while (pBlock != NULL) {
    SColumnInfoData* pTableNameCol = taosArrayGet(pBlock->pDataBlock, 0);
    SColumnInfoData* pDbNameCol = taosArrayGet(pBlock->pDataBlock, 2);
    SColumnInfoData* pColNameCol = taosArrayGet(pBlock->pDataBlock, 3);
    SColumnInfoData* pRefCol = taosArrayGet(pBlock->pDataBlock, 6);

    QUERY_CHECK_NULL(pTableNameCol, code, line, _return, terrno);
    QUERY_CHECK_NULL(pDbNameCol, code, line, _return, terrno);
    QUERY_CHECK_NULL(pColNameCol, code, line, _return, terrno);
    QUERY_CHECK_NULL(pRefCol, code, line, _return, terrno);

    for (int32_t i = 0; i < pBlock->info.rows; ++i) {
      char      dbName[TSDB_DB_NAME_LEN] = {0};
      char      tbName[TSDB_TABLE_NAME_LEN] = {0};
      char      colName[TSDB_COL_NAME_LEN] = {0};
      char      currentRef[sizeof(((SDynRefKey*)0)->colRef)] = {0};
      SDynRefKey nextRef = {0};
      uint8_t   hit = 1;
      char*     pDb = colDataGetData(pDbNameCol, i);
      char*     pTb = colDataGetData(pTableNameCol, i);
      char*     pCol = colDataGetData(pColNameCol, i);

      memcpy(dbName, varDataVal(pDb), varDataLen(pDb));
      memcpy(tbName, varDataVal(pTb), varDataLen(pTb));
      memcpy(colName, varDataVal(pCol), varDataLen(pCol));

      len = tsnprintf(currentRef, sizeof(currentRef), "%s.%s.%s", dbName, tbName, colName);
      if (len < 0 || len >= sizeof(currentRef)) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        QUERY_CHECK_CODE(code, line, _return);
      }

      code = taosHashPut(pHitMap, currentRef, strlen(currentRef), &hit, sizeof(hit));
      QUERY_CHECK_CODE(code, line, _return);

      if (colDataIsNull_s(pRefCol, i)) {
        continue;
      }

      memcpy(nextRef.colRef, varDataVal(colDataGetData(pRefCol, i)), varDataLen(colDataGetData(pRefCol, i)));
      code = taosHashPut(pNextRefMap, currentRef, strlen(currentRef), &nextRef, sizeof(nextRef));
      QUERY_CHECK_CODE(code, line, _return);
    }

    code = pTargetOp->fpSet.getNextFn(pTargetOp, &pBlock);
    QUERY_CHECK_CODE(code, line, _return);
  }

_return:
  freeOperatorParam(pParam, OP_GET_PARAM);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
  }
  return code;
}

/*
 * Walk the discovered ref graph from every root ref and fill the final raw-ref cache.
 *
 * @param pOperator dyn operator context
 * @param pEdgeMap  discovered ref graph, key currentRef, value nextRef or empty ref for terminal
 * @param pRootRefs all root refs that must be resolved
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t dynBuildResolvedColRefsFromEdgeMap(SOperatorInfo* pOperator, SHashObj* pEdgeMap, SArray* pRootRefs) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = &pInfo->vtbScan;

  for (int32_t i = 0; i < taosArrayGetSize(pRootRefs); ++i) {
    SDynRefKey* pRootRef = taosArrayGet(pRootRefs, i);
    const char* currentRef = NULL;

    QUERY_CHECK_NULL(pRootRef, code, line, _return, terrno);
    currentRef = pRootRef->colRef;

    for (int32_t depth = 0; depth < DYN_VTB_REF_MAX_DEPTH; ++depth) {
      SDynRefKey* pNextRef = (SDynRefKey*)taosHashGet(pEdgeMap, currentRef, strlen(currentRef));
      int32_t     vgId = 0;

      QUERY_CHECK_NULL(pNextRef, code, line, _return, terrno);
      if (pNextRef->colRef[0] != 0) {
        currentRef = pNextRef->colRef;
        continue;
      }

      code = dynGetRefVgId(pOperator, currentRef, &vgId);
      QUERY_CHECK_CODE(code, line, _return);
      code = dynPutResolvedColRef(pVtbScan, pRootRef->colRef, currentRef, vgId);
      QUERY_CHECK_CODE(code, line, _return);
      break;
    }

    if (taosHashGet(pVtbScan->resolvedColRefMap, pRootRef->colRef, strlen(pRootRef->colRef)) == NULL) {
      code = TSDB_CODE_VTABLE_INVALID_REF_COLUMN;
      QUERY_CHECK_CODE(code, line, _return);
    }
  }

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
  }
  return code;
}

/*
 * Resolve one pending-ref layer through systable scan and advance the frontier.
 *
 * @param pOperator   dyn operator context
 * @param pTargetOp   operator used to read `ins_vc_cols`
 * @param pPendingMap unresolved refs keyed by root ref, value current ref
 * @param pEdgeMap    discovered ref graph, key current ref, value next ref or terminal
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t dynResolvePendingColRefsLayerBySysScan(SOperatorInfo* pOperator, SOperatorInfo* pTargetOp,
                                                      SHashObj* pPendingMap, SHashObj* pEdgeMap) {
  int32_t   code = TSDB_CODE_SUCCESS;
  int32_t   line = 0;
  SHashObj* pReqByVg = NULL;
  SHashObj* pHitMap = NULL;
  SHashObj* pNextRefMap = NULL;
  SHashObj* pNextPending = NULL;
  void*     pIter = NULL;
  size_t    pendingSize = taosHashGetSize(pPendingMap);

  code = dynBuildSysScanLayerReqs(pOperator, pPendingMap, &pReqByVg);
  QUERY_CHECK_CODE(code, line, _return);

  pHitMap = taosHashInit(pendingSize, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  QUERY_CHECK_NULL(pHitMap, code, line, _return, terrno);

  pNextRefMap = taosHashInit(pendingSize, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  QUERY_CHECK_NULL(pNextRefMap, code, line, _return, terrno);

  pNextPending = taosHashInit(pendingSize, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  QUERY_CHECK_NULL(pNextPending, code, line, _return, terrno);

  code = dynCollectSysScanNextRefs(pTargetOp, pReqByVg, pHitMap, pNextRefMap);
  QUERY_CHECK_CODE(code, line, _return);

  while ((pIter = taosHashIterate(pPendingMap, pIter)) != NULL) {
    size_t      keyLen = 0;
    char*       rootRef = taosHashGetKey(pIter, &keyLen);
    SDynRefKey* pCurrentRef = (SDynRefKey*)pIter;
    SDynRefKey* pNextRef = NULL;
    SDynRefKey  terminalRef = {0};

    pNextRef = (SDynRefKey*)taosHashGet(pNextRefMap, pCurrentRef->colRef, strlen(pCurrentRef->colRef));
    if (pNextRef != NULL) {
      code = taosHashPut(pEdgeMap, pCurrentRef->colRef, strlen(pCurrentRef->colRef), pNextRef, sizeof(*pNextRef));
      QUERY_CHECK_CODE(code, line, _return);

      code = taosHashPut(pNextPending, rootRef, keyLen, pNextRef, sizeof(*pNextRef));
      QUERY_CHECK_CODE(code, line, _return);
    } else if (taosHashGet(pHitMap, pCurrentRef->colRef, strlen(pCurrentRef->colRef)) != NULL) {
      code = TSDB_CODE_VTABLE_INVALID_REF_COLUMN;
      QUERY_CHECK_CODE(code, line, _return);
    } else {
      code = taosHashPut(pEdgeMap, pCurrentRef->colRef, strlen(pCurrentRef->colRef), &terminalRef,
                         sizeof(terminalRef));
      QUERY_CHECK_CODE(code, line, _return);
    }
  }

  taosHashClear(pPendingMap);

  pIter = taosHashIterate(pNextPending, NULL);
  while (pIter != NULL) {
    size_t keyLen = 0;
    char*  key = taosHashGetKey(pIter, &keyLen);
    code = taosHashPut(pPendingMap, key, keyLen, pIter, sizeof(SDynRefKey));
    QUERY_CHECK_CODE(code, line, _return);
    pIter = taosHashIterate(pNextPending, pIter);
  }

_return:
  taosHashCleanup(pReqByVg);
  taosHashCleanup(pHitMap);
  taosHashCleanup(pNextRefMap);
  taosHashCleanup(pNextPending);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
  }
  return code;
}

/*
 * Resolve all pending refs layer by layer through repeated systable scans.
 *
 * @param pOperator   dyn operator context
 * @param pTargetOp   operator used to read `ins_vc_cols`
 * @param pPendingMap unresolved refs keyed by root ref, value current ref
 * @param pRootRefs   all root refs that must be resolved
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t dynResolvePendingColRefsBySysScan(SOperatorInfo* pOperator, SOperatorInfo* pTargetOp,
                                                 SHashObj* pPendingMap, SArray* pRootRefs) {
  int32_t   code = TSDB_CODE_SUCCESS;
  int32_t   line = 0;
  SHashObj* pEdgeMap = NULL;  // key: currentRef, value: nextRef; empty nextRef means terminal raw ref

  pEdgeMap = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  QUERY_CHECK_NULL(pEdgeMap, code, line, _return, terrno);

  for (int32_t depth = 0; taosHashGetSize(pPendingMap) > 0; ++depth) {
    if (depth >= DYN_VTB_REF_MAX_DEPTH) {
      code = TSDB_CODE_VTABLE_REF_DEPTH_EXCEEDED;
      QUERY_CHECK_CODE(code, line, _return);
    }

    code = dynResolvePendingColRefsLayerBySysScan(pOperator, pTargetOp, pPendingMap, pEdgeMap);
    QUERY_CHECK_CODE(code, line, _return);
  }

  code = dynBuildResolvedColRefsFromEdgeMap(pOperator, pEdgeMap, pRootRefs);
  QUERY_CHECK_CODE(code, line, _return);

_return:
  taosHashCleanup(pEdgeMap);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
  }
  return code;
}

/*
 * Resolve one root column reference from the already prepared cache.
 *
 * @param pOperator   dyn operator context
 * @param colRef      root reference in db.table.col format
 * @param ppResolved  output cached final referenced-column info
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t dynResolveFinalColRef(SOperatorInfo* pOperator, const char* colRef, SDynResolvedColRef** ppResolved) {
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SDynResolvedColRef**       ppCached =
      (SDynResolvedColRef**)taosHashGet(pInfo->vtbScan.resolvedColRefMap, colRef, strlen(colRef));

  if (ppCached == NULL) {
    qError("%s failed since unresolved ref cache miss, ref:%s", __func__, colRef);
    return TSDB_CODE_VTABLE_INVALID_REF_COLUMN;
  }

  *ppResolved = *ppCached;
  return TSDB_CODE_SUCCESS;
}

/*
 * Add one root ref into the layered systable-scan pending map.
 *
 * @param pOperator   dyn operator context
 * @param pPendingMap pending refs keyed by root ref, value current ref
 * @param pRootRefs   all root refs to be resolved
 * @param rootRef     root ref in db.table.col format
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t dynAddPendingRootRef(SOperatorInfo* pOperator, SHashObj* pPendingMap, SArray* pRootRefs,
                                    const char* rootRef) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SDynRefKey                 pending = {0};

  if (rootRef == NULL) {
    return code;
  }

  if (taosHashGet(pInfo->vtbScan.resolvedColRefMap, rootRef, strlen(rootRef)) != NULL ||
      taosHashGet(pPendingMap, rootRef, strlen(rootRef)) != NULL) {
    return code;
  }

  tstrncpy(pending.colRef, rootRef, sizeof(pending.colRef));

  code = taosHashPut(pPendingMap, rootRef, strlen(rootRef), &pending, sizeof(pending));
  QUERY_CHECK_CODE(code, line, _return);
  QUERY_CHECK_NULL(taosArrayPush(pRootRefs, &pending), code, line, _return, terrno);

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed since %s, line %d, ref:%s", __func__, tstrerror(code), line,
           rootRef ? rootRef : "<null>");
  }
  return code;
}

/*
 * Resolve all root refs stored in one column-ref array through layered systable scans.
 *
 * @param pOperator   dyn operator context
 * @param pTargetOp   operator used to read `ins_vc_cols`
 * @param pColRefInfo source column-ref array
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t dynResolveColRefArrayBySysScan(SOperatorInfo* pOperator, SOperatorInfo* pTargetOp, SArray* pColRefInfo) {
  int32_t   code = TSDB_CODE_SUCCESS;
  int32_t   line = 0;
  SHashObj* pPendingMap =
      taosHashInit(TMAX(taosArrayGetSize(pColRefInfo), 8), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true,
                   HASH_NO_LOCK);
  SArray*   pRootRefs = NULL;
  QUERY_CHECK_NULL(pPendingMap, code, line, _return, terrno);
  pRootRefs = taosArrayInit(TMAX(taosArrayGetSize(pColRefInfo), 1), sizeof(SDynRefKey));
  QUERY_CHECK_NULL(pRootRefs, code, line, _return, terrno);

  for (int32_t i = 0; i < taosArrayGetSize(pColRefInfo); ++i) {
    SColRefInfo* pColRef = taosArrayGet(pColRefInfo, i);
    QUERY_CHECK_NULL(pColRef, code, line, _return, terrno);

    code = dynAddPendingRootRef(pOperator, pPendingMap, pRootRefs, pColRef->colrefName);
    QUERY_CHECK_CODE(code, line, _return);
  }

  if (taosHashGetSize(pPendingMap) > 0) {
    code = dynResolvePendingColRefsBySysScan(pOperator, pTargetOp, pPendingMap, pRootRefs);
    QUERY_CHECK_CODE(code, line, _return);
  }

_return:
  taosArrayDestroy(pRootRefs);
  taosHashCleanup(pPendingMap);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
  }
  return code;
}

/*
 * Resolve all root refs stored in virtual-super-table child arrays.
 *
 * @param pOperator dyn operator context
 * @param pTargetOp operator used to read `ins_vc_cols`
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t dynResolveChildTableRefsBySysScan(SOperatorInfo* pOperator, SOperatorInfo* pTargetOp) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SHashObj*                  pPendingMap = NULL;
  SArray*                    pRootRefs = NULL;

  if (taosArrayGetSize(pInfo->vtbScan.childTableList) == 0) {
    return TSDB_CODE_SUCCESS;
  }

  pPendingMap = taosHashInit(taosArrayGetSize(pInfo->vtbScan.childTableList),
                             taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  QUERY_CHECK_NULL(pPendingMap, code, line, _return, terrno);

  pRootRefs = taosArrayInit(TMAX((int32_t)taosArrayGetSize(pInfo->vtbScan.childTableList), 1), sizeof(SDynRefKey));
  QUERY_CHECK_NULL(pRootRefs, code, line, _return, terrno);

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->vtbScan.childTableList); ++i) {
    SArray* pColRefArray = taosArrayGetP(pInfo->vtbScan.childTableList, i);
    QUERY_CHECK_NULL(pColRefArray, code, line, _return, terrno);

    for (int32_t j = 0; j < taosArrayGetSize(pColRefArray); ++j) {
      SColRefInfo* pColRef = taosArrayGet(pColRefArray, j);
      QUERY_CHECK_NULL(pColRef, code, line, _return, terrno);

      code = dynAddPendingRootRef(pOperator, pPendingMap, pRootRefs, pColRef->colrefName);
      QUERY_CHECK_CODE(code, line, _return);
    }
  }

  if (taosHashGetSize(pPendingMap) > 0) {
    code = dynResolvePendingColRefsBySysScan(pOperator, pTargetOp, pPendingMap, pRootRefs);
    QUERY_CHECK_CODE(code, line, _return);
  }

_return:
  taosArrayDestroy(pRootRefs);
  taosHashCleanup(pPendingMap);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
  }
  return code;
}

static int32_t getDbVgInfo(SOperatorInfo* pOperator, SName *name, SDBVgInfo **dbVgInfo) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = (SVtbScanDynCtrlInfo*)&pInfo->vtbScan;
  SExecTaskInfo*             pTaskInfo = pOperator->pTaskInfo;
  SMsgCb*                    pMsgCb = pVtbScan->pMsgCb;
  SUseDbOutput*              output = NULL;
  SUseDbOutput**             find = (SUseDbOutput**)taosHashGet(pInfo->vtbScan.dbVgInfoMap, name->dbname, strlen(name->dbname));

  QRY_PARAM_CHECK(dbVgInfo);

  if (find == NULL) {
    output = taosMemoryMalloc(sizeof(SUseDbOutput));
    code = buildDbVgInfoMap(pOperator, pMsgCb, name, pTaskInfo, output);
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

/*
 * Parse one full column-ref string into non-owning name slices.
 *
 * @param colRef full column ref in db.table.col format
 * @param pView  output view into the original string
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t parseColRefNameView(const char* colRef, SColRefNameView* pView) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     line = 0;
  const char* firstDot = NULL;
  const char* secondDot = NULL;

  QUERY_CHECK_NULL(colRef, code, line, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pView, code, line, _return, TSDB_CODE_INVALID_PARA);

  firstDot = strchr(colRef, '.');
  secondDot = firstDot == NULL ? NULL : strchr(firstDot + 1, '.');
  if (firstDot == NULL || secondDot == NULL || firstDot == colRef || secondDot == firstDot + 1 || secondDot[1] == 0) {
    code = TSDB_CODE_VTABLE_INVALID_REF_COLUMN;
    QUERY_CHECK_CODE(code, line, _return);
  }

  pView->dbName = colRef;
  pView->tbName = firstDot + 1;
  pView->colName = secondDot + 1;
  pView->dbNameLen = firstDot - colRef;
  pView->tbNameLen = secondDot - firstDot - 1;
  pView->colNameLen = strlen(secondDot + 1);

  // check name length valid
  if (pView->dbNameLen <= 0 || pView->dbNameLen >= TSDB_DB_NAME_LEN || pView->tbNameLen <= 0 ||
      pView->tbNameLen >= TSDB_TABLE_NAME_LEN || pView->colNameLen <= 0 || pView->colNameLen >= TSDB_COL_NAME_LEN) {
    qError("%s failed since invalid name length in ref, dbNameLen:%zu, tbNameLen:%zu, colNameLen:%zu",
           __func__, pView->dbNameLen, pView->tbNameLen, pView->colNameLen);
    code = TSDB_CODE_VTABLE_INVALID_REF_COLUMN;
    QUERY_CHECK_CODE(code, line, _return);
  }
  return code;

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s, ref:%s", __func__, line, tstrerror(code), colRef ? colRef : "<null>");
  }
  return code;
}

bool tableInfoNeedCollect(char *dbName, char *tbName, char *expectDbName, char *expectTbName) {
  if (strncmp(varDataVal(tbName), expectTbName, varDataLen(tbName)) == 0 &&
      strlen(expectTbName) == varDataLen(tbName) &&
      strncmp(varDataVal(dbName), expectDbName, varDataLen(dbName)) == 0 &&
      strlen(expectDbName) == varDataLen(dbName)) {
    return true;
  }
  return false;
}

int32_t getColRefInfo(SColRefInfo *pInfo, SArray* pDataBlock, int32_t index) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          line = 0;

  SColumnInfoData *pColNameCol = taosArrayGet(pDataBlock, 3);
  SColumnInfoData *pUidCol = taosArrayGet(pDataBlock, 4);
  SColumnInfoData *pColIdCol = taosArrayGet(pDataBlock, 5);
  SColumnInfoData *pRefCol = taosArrayGet(pDataBlock, 6);
  SColumnInfoData *pVgIdCol = taosArrayGet(pDataBlock, 7);
  SColumnInfoData *pRefVerCol = taosArrayGet(pDataBlock, 8);

  QUERY_CHECK_NULL(pColNameCol, code, line, _return, terrno)
  QUERY_CHECK_NULL(pUidCol, code, line, _return, terrno)
  QUERY_CHECK_NULL(pColIdCol, code, line, _return, terrno)
  QUERY_CHECK_NULL(pRefCol, code, line, _return, terrno)
  QUERY_CHECK_NULL(pVgIdCol, code, line, _return, terrno)
  QUERY_CHECK_NULL(pRefVerCol, code, line, _return, terrno)

  if (colDataIsNull_s(pRefCol, index)) {
    pInfo->colrefName = NULL;
  } else {
    pInfo->colrefName = taosMemoryCalloc(varDataTLen(colDataGetData(pRefCol, index)), 1);
    QUERY_CHECK_NULL(pInfo->colrefName, code, line, _return, terrno)
    memcpy(pInfo->colrefName, varDataVal(colDataGetData(pRefCol, index)), varDataLen(colDataGetData(pRefCol, index)));
    pInfo->colrefName[varDataLen(colDataGetData(pRefCol, index))] = 0;
  }

  pInfo->colName = taosMemoryCalloc(varDataTLen(colDataGetData(pColNameCol, index)), 1);
  QUERY_CHECK_NULL(pInfo->colName, code, line, _return, terrno)
  memcpy(pInfo->colName, varDataVal(colDataGetData(pColNameCol, index)), varDataLen(colDataGetData(pColNameCol, index)));
  pInfo->colName[varDataLen(colDataGetData(pColNameCol, index))] = 0;

  if (!colDataIsNull_s(pUidCol, index)) {
    GET_TYPED_DATA(pInfo->uid, int64_t, TSDB_DATA_TYPE_BIGINT, colDataGetNumData(pUidCol, index), 0);
  }
  if (!colDataIsNull_s(pColIdCol, index)) {
    GET_TYPED_DATA(pInfo->colId, int32_t, TSDB_DATA_TYPE_INT, colDataGetNumData(pColIdCol, index), 0);
  }
  if (!colDataIsNull_s(pVgIdCol, index)) {
    GET_TYPED_DATA(pInfo->vgId, int32_t, TSDB_DATA_TYPE_INT, colDataGetNumData(pVgIdCol, index), 0);
  }

_return:
  return code;
}

/*
 * Collect resolved raw-table vgroups from one column-ref array.
 *
 * @param pOperator   dyn operator context
 * @param pColRefInfo source column-ref array
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t dynCollectResolvedOrgTbVgFromArray(SOperatorInfo* pOperator, SArray* pColRefInfo) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = &pInfo->vtbScan;

  if (pVtbScan->curOrgTbVg == NULL) {
    pVtbScan->curOrgTbVg = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
    QUERY_CHECK_NULL(pVtbScan->curOrgTbVg, code, line, _return, terrno)
  } else {
    taosHashClear(pVtbScan->curOrgTbVg);
  }

  for (int32_t i = 0; i < taosArrayGetSize(pColRefInfo); ++i) {
    SColRefInfo* pColRef = taosArrayGet(pColRefInfo, i);
    int32_t      vgId = 0;

    QUERY_CHECK_NULL(pColRef, code, line, _return, terrno)
    if (pColRef->colrefName == NULL) {
      continue;
    }

    code = getVgIdFromColref(pOperator, pColRef->colrefName, &vgId);
    QUERY_CHECK_CODE(code, line, _return);
    code = taosHashPut(pVtbScan->curOrgTbVg, &vgId, sizeof(vgId), NULL, 0);
    QUERY_CHECK_CODE(code, line, _return);
  }

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
  }
  return code;
}

/*
 * Collect resolved raw-table vgroups from all child column-ref arrays.
 *
 * @param pOperator dyn operator context
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t dynCollectResolvedOrgTbVgFromChildTables(SOperatorInfo* pOperator) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = &pInfo->vtbScan;

  if (pVtbScan->curOrgTbVg == NULL) {
    pVtbScan->curOrgTbVg = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
    QUERY_CHECK_NULL(pVtbScan->curOrgTbVg, code, line, _return, terrno)
  } else {
    taosHashClear(pVtbScan->curOrgTbVg);
  }

  for (int32_t i = 0; i < taosArrayGetSize(pVtbScan->childTableList); ++i) {
    SArray* pColRefInfo = taosArrayGetP(pVtbScan->childTableList, i);
    QUERY_CHECK_NULL(pColRefInfo, code, line, _return, terrno)

    for (int32_t j = 0; j < taosArrayGetSize(pColRefInfo); ++j) {
      SColRefInfo* pColRef = taosArrayGet(pColRefInfo, j);
      int32_t      vgId = 0;

      QUERY_CHECK_NULL(pColRef, code, line, _return, terrno)
      if (pColRef->colrefName == NULL) {
        continue;
      }

      code = getVgIdFromColref(pOperator, pColRef->colrefName, &vgId);
      QUERY_CHECK_CODE(code, line, _return);
      code = taosHashPut(pVtbScan->curOrgTbVg, &vgId, sizeof(vgId), NULL, 0);
      QUERY_CHECK_CODE(code, line, _return);
    }
  }

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
  }
  return code;
}

int32_t processOrgTbVg(SVtbScanDynCtrlInfo* pVtbScan, SExecTaskInfo* pTaskInfo, int32_t rversion) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;

  if (pTaskInfo->pStreamRuntimeInfo == NULL) {
    return code;
  }

  if (pVtbScan->existOrgTbVg == NULL) {
    pVtbScan->existOrgTbVg = pVtbScan->curOrgTbVg;
    pVtbScan->curOrgTbVg = NULL;
  }

  if (pVtbScan->curOrgTbVg != NULL) {
    // which means rversion has changed
    void*   pCurIter = NULL;
    SArray* tmpArray = NULL;
    while ((pCurIter = taosHashIterate(pVtbScan->curOrgTbVg, pCurIter))) {
      int32_t* vgId = (int32_t*)taosHashGetKey(pCurIter, NULL);
      if (taosHashGet(pVtbScan->existOrgTbVg, vgId, sizeof(int32_t)) == NULL) {
        if (tmpArray == NULL) {
          tmpArray = taosArrayInit(1, sizeof(int32_t));
          QUERY_CHECK_NULL(tmpArray, code, line, _return, terrno)
        }
        QUERY_CHECK_NULL(taosArrayPush(tmpArray, vgId), code, line, _return, terrno)
      }
    }
    if (tmpArray == NULL) {
      return TSDB_CODE_SUCCESS;
    }
    if (tmpArray != NULL && pTaskInfo->pStreamRuntimeInfo->vtableDeployInfo.addVgIds == NULL) {
      SArray* expiredInfo = atomic_load_ptr(&pTaskInfo->pStreamRuntimeInfo->vtableDeployInfo.addedVgInfo);
      if (expiredInfo && expiredInfo == atomic_val_compare_exchange_ptr(&pTaskInfo->pStreamRuntimeInfo->vtableDeployInfo.addedVgInfo, expiredInfo, NULL)) {
        for (int32_t i = 0; i < taosArrayGetSize(expiredInfo); i++) {
          SStreamTaskAddr* vgInfo = (SStreamTaskAddr*)taosArrayGet(expiredInfo, i);
          QUERY_CHECK_NULL(taosArrayPush(tmpArray, &vgInfo->nodeId), code, line, _return, terrno)
        }
        taosArrayDestroy(expiredInfo);
      }
      if (atomic_val_compare_exchange_ptr(&pTaskInfo->pStreamRuntimeInfo->vtableDeployInfo.addVgIds, NULL, tmpArray)) {
        taosArrayDestroy(tmpArray);
      }
    }
    atomic_store_64(&pTaskInfo->pStreamRuntimeInfo->vtableDeployInfo.uid, (int64_t)(pVtbScan->isSuperTable ? pVtbScan->suid : pVtbScan->uid));
    (void)atomic_val_compare_exchange_8(pTaskInfo->pStreamRuntimeInfo->vtableDeployGot, 0, 1);
    taosHashClear(pVtbScan->curOrgTbVg);
    pVtbScan->needRedeploy = true;
    pVtbScan->rversion = rversion;
    return TSDB_CODE_STREAM_VTABLE_NEED_REDEPLOY;
  }
  return code;
_return:
  qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
  return code;
}

static int32_t getVgIdFromColref(SOperatorInfo* pOperator, const char* colRef, int32_t* vgId) {
  int32_t             code =TSDB_CODE_SUCCESS;
  int32_t             line = 0;
  SDynResolvedColRef* pResolved = NULL;

  code = dynResolveFinalColRef(pOperator, colRef, &pResolved);
  QUERY_CHECK_CODE(code, line, _return);
  *vgId = pResolved->vgId;

_return:
  if (code) {
    qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
  }
  return code;
}

static int32_t generateTagArrayByTagBlockAndSave(SHashObj* vtbUidTagListMap, tb_uid_t uid, SSDataBlock *pTagVal, int32_t rowIdx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t line = 0;
  STagVal tagVal = {0};
  // last col is uid

  SArray* pTagList = taosArrayInit(1, sizeof(STagVal));
  QUERY_CHECK_NULL(pTagList, code, line, _return, terrno)

  for (int32_t k = 0; k < taosArrayGetSize(pTagVal->pDataBlock) - 1; k++) {
    SColumnInfoData *pTagCol = taosArrayGet(pTagVal->pDataBlock, k);
    QUERY_CHECK_NULL(pTagCol, code, line, _return, terrno)
    tagVal.type = pTagCol->info.type;
    tagVal.cid = pTagCol->info.colId;
    if (!colDataIsNull_s(pTagCol, rowIdx)) {
      char*   pData = colDataGetData(pTagCol, rowIdx);
      if (IS_VAR_DATA_TYPE(pTagCol->info.type)) {
        tagVal.nData = varDataLen(pData);
        tagVal.pData = taosMemoryMalloc(tagVal.nData);
        QUERY_CHECK_NULL(tagVal.pData, code, line, _return, terrno)
        memcpy(tagVal.pData, varDataVal(pData), varDataLen(pData));
        QUERY_CHECK_NULL(taosArrayPush(pTagList, &tagVal), code, line, _return, terrno)
      } else {
        memcpy(&tagVal.i64, pData, tDataTypes[pTagCol->info.type].bytes);
        QUERY_CHECK_NULL(taosArrayPush(pTagList, &tagVal), code, line, _return, terrno)
      }
    } else {
      tagVal.pData = NULL;
      tagVal.nData = 0;
      QUERY_CHECK_NULL(taosArrayPush(pTagList, &tagVal), code, line, _return, terrno)
    }
    tagVal = (STagVal){0};
  }
  code = taosHashPut(vtbUidTagListMap, &uid, sizeof(uid), &pTagList, POINTER_BYTES);
  QUERY_CHECK_CODE(code, line, _return);

  return code;
_return:
  if (tagVal.pData) {
    taosMemoryFreeClear(tagVal.pData);
  }
  if (pTagList) {
    taosArrayDestroyEx(pTagList, destroyTagVal);
  }
  qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
  return code;
}

int32_t virtualTableScanProcessColRefInfo(SOperatorInfo* pOperator, SArray* pColRefInfo, tb_uid_t* uid, int32_t* vgId,
                                          SHashObj** ppRefMap) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = (SVtbScanDynCtrlInfo*)&pInfo->vtbScan;
  SHashObj*                  refMap = NULL;

  for (int32_t j = 0; j < taosArrayGetSize(pColRefInfo); j++) {
    SColRefInfo *pKV = (SColRefInfo*)taosArrayGet(pColRefInfo, j);
    *uid = pKV->uid;
    *vgId = pKV->vgId;
    if (pKV->colrefName != NULL && colNeedScan(pOperator, pKV->colId)) {
      SDynResolvedColRef* pResolved = NULL;
      SName               name = {0};
      char                orgTbFName[TSDB_TABLE_FNAME_LEN] = {0};

      code = dynResolveFinalColRef(pOperator, pKV->colrefName, &pResolved);
      QUERY_CHECK_CODE(code, line, _return);

      if (ppRefMap != NULL) {
        if (refMap == NULL) {
          refMap = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR), true, HASH_NO_LOCK);
          QUERY_CHECK_NULL(refMap, code, line, _return, terrno)
        }
        code = addRefColIdToRefMap(refMap, pResolved->fullColRef, pKV->colId);
        QUERY_CHECK_CODE(code, line, _return);
      }

      toName(pInfo->vtbScan.acctId, pResolved->dbName, pResolved->tbName, &name);
      code = tNameGetFullTableName(&name, orgTbFName);
      QUERY_CHECK_CODE(code, line, _return);

      void *pVal = taosHashGet(pVtbScan->otbNameToOtbInfoMap, orgTbFName, sizeof(orgTbFName));
      if (!pVal) {
        SOrgTbInfo orgTbInfo = {0};
        orgTbInfo.vgId = pResolved->vgId;
        tstrncpy(orgTbInfo.tbName, orgTbFName, sizeof(orgTbInfo.tbName));
        orgTbInfo.colMap = taosArrayInit(10, sizeof(SColIdNameKV));
        QUERY_CHECK_NULL(orgTbInfo.colMap, code, line, _return, terrno)
        SColIdNameKV colIdNameKV = {0};
        colIdNameKV.colId = pKV->colId;
        tstrncpy(colIdNameKV.colName, pResolved->colName, sizeof(colIdNameKV.colName));
        QUERY_CHECK_NULL(taosArrayPush(orgTbInfo.colMap, &colIdNameKV), code, line, _return, terrno)
        code = taosHashPut(pVtbScan->otbNameToOtbInfoMap, orgTbFName, sizeof(orgTbFName), &orgTbInfo, sizeof(orgTbInfo));
        QUERY_CHECK_CODE(code, line, _return);
      } else {
        SOrgTbInfo *tbInfo = (SOrgTbInfo *)pVal;
        SColIdNameKV colIdNameKV = {0};
        colIdNameKV.colId = pKV->colId;
        tstrncpy(colIdNameKV.colName, pResolved->colName, sizeof(colIdNameKV.colName));
        QUERY_CHECK_NULL(taosArrayPush(tbInfo->colMap, &colIdNameKV), code, line, _return, terrno)
      }
    }
  }

  if (ppRefMap != NULL) {
    *ppRefMap = refMap;
  }

  return code;

_return:
  qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
  if (refMap) {
    taosHashCleanup(refMap);
  }
  return code;
}

static int32_t getTagBlockAndProcess(SOperatorInfo* pOperator, bool hasPartition) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = (SVtbScanDynCtrlInfo*)&pInfo->vtbScan;
  SExecTaskInfo*             pTaskInfo = pOperator->pTaskInfo;
  SArray*                    pColRefArray = NULL;
  SOperatorInfo*             pSystableScanOp = pOperator->pDownstream[0];
  SOperatorInfo*             pTagScanOp = pOperator->pDownstream[1];

  pVtbScan->vtbUidTagListMap = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  QUERY_CHECK_NULL(pVtbScan->vtbUidTagListMap, code, line, _return, terrno)
  taosHashSetFreeFp(pVtbScan->vtbUidTagListMap, destroyTagList);
  if (hasPartition) {
    pVtbScan->vtbUidToGroupIdMap = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
    pVtbScan->vtbGroupIdTagListMap = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_ENTRY_LOCK);
    QUERY_CHECK_NULL(pVtbScan->vtbUidToGroupIdMap, code, line, _return, terrno)
    QUERY_CHECK_NULL(pVtbScan->vtbGroupIdTagListMap, code, line, _return, terrno)
    taosHashSetFreeFp(pVtbScan->vtbGroupIdTagListMap, destroyVtbUidTagListMap);
  }

  while (true) {
    SSDataBlock *pTagVal = NULL;
    code = pTagScanOp->fpSet.getNextFn(pTagScanOp, &pTagVal);
    QUERY_CHECK_CODE(code, line, _return);
    if (pTagVal == NULL) {
      break;
    }
    SHashObj *vtbUidTagListMap = NULL;
    if (hasPartition) {
      void* pIter = taosHashGet(pVtbScan->vtbGroupIdTagListMap, &pTagVal->info.id.groupId, sizeof(pTagVal->info.id.groupId));
      if (pIter) {
        vtbUidTagListMap = *(SHashObj**)pIter;
      } else {
        vtbUidTagListMap = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
        QUERY_CHECK_NULL(vtbUidTagListMap, code, line, _return, terrno)
        taosHashSetFreeFp(vtbUidTagListMap, destroyTagList);

        code = taosHashPut(pVtbScan->vtbGroupIdTagListMap, &pTagVal->info.id.groupId, sizeof(pTagVal->info.id.groupId), &vtbUidTagListMap, POINTER_BYTES);
        QUERY_CHECK_CODE(code, line, _return);
      }
    } else {
      vtbUidTagListMap = pVtbScan->vtbUidTagListMap;
    }

    SColumnInfoData *pUidCol = taosArrayGetLast(pTagVal->pDataBlock);
    QUERY_CHECK_NULL(pUidCol, code, line, _return, terrno)
    for (int32_t i = 0; i < pTagVal->info.rows; i++) {
      tb_uid_t uid = 0;
      if (!colDataIsNull_s(pUidCol, i)) {
        GET_TYPED_DATA(uid, int64_t, TSDB_DATA_TYPE_BIGINT, colDataGetNumData(pUidCol, i), 0);
        QUERY_CHECK_CODE(code, line, _return);
      }

      code = generateTagArrayByTagBlockAndSave(vtbUidTagListMap, uid, pTagVal, i);
      QUERY_CHECK_CODE(code, line, _return);

      if (hasPartition) {
        code = taosHashPut(pVtbScan->vtbUidToGroupIdMap, &uid, sizeof(uid), &pTagVal->info.id.groupId, sizeof(pTagVal->info.id.groupId));
        QUERY_CHECK_CODE(code, line, _return);
      }
    }
  }

  return code;

_return:
  qError("%s failed at line %d since %s", __func__, line, tstrerror(code));
  return code;
}

static int32_t processChildTableListAndGenerateOrgTbInfoMap(SOperatorInfo* pOperator) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = (SVtbScanDynCtrlInfo*)&pInfo->vtbScan;
  SExecTaskInfo*             pTaskInfo = pOperator->pTaskInfo;
  SArray*                    pColRefArray = NULL;
  SOperatorInfo*             pSystableScanOp = pOperator->pDownstream[0];
  SOperatorInfo*             pTagScanOp = pOperator->pDownstream[1];

  pVtbScan->vtbUidToVgIdMapMap = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  QUERY_CHECK_NULL(pVtbScan->vtbUidToVgIdMapMap, code, line, _return, terrno)

  for (int32_t i = 0; i < taosArrayGetSize(pVtbScan->childTableList); i++) {
    SHashObj* otbVgIdToOtbInfoArrayMap = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
    QUERY_CHECK_NULL(otbVgIdToOtbInfoArrayMap, code, line, _return, terrno)

    SArray* pColRefInfo = (SArray*)taosArrayGetP(pVtbScan->childTableList, i);
    QUERY_CHECK_NULL(pColRefInfo, code, line, _return, terrno)

    tb_uid_t uid = 0;
    int32_t  vgId = 0;
    code = virtualTableScanProcessColRefInfo(pOperator, pColRefInfo, &uid, &vgId, NULL);
    QUERY_CHECK_CODE(code, line, _return);

    size_t len = 0;
    void*  pOrgTbInfo = taosHashIterate(pVtbScan->otbNameToOtbInfoMap, NULL);
    while (pOrgTbInfo != NULL) {
      char*       key = taosHashGetKey(pOrgTbInfo, &len);
      SOrgTbInfo* orgTbInfo = (SOrgTbInfo*)pOrgTbInfo;

      void* pIter = taosHashGet(otbVgIdToOtbInfoArrayMap, &orgTbInfo->vgId, sizeof(orgTbInfo->vgId));
      if (!pIter) {
        SArray* pOrgTbInfoArray = taosArrayInit(1, sizeof(SOrgTbInfo));
        QUERY_CHECK_NULL(pOrgTbInfoArray, code, line, _return, terrno)
        QUERY_CHECK_NULL(taosArrayPush(pOrgTbInfoArray, orgTbInfo), code, line, _return, terrno)
        code = taosHashPut(otbVgIdToOtbInfoArrayMap, &orgTbInfo->vgId, sizeof(orgTbInfo->vgId), &pOrgTbInfoArray, POINTER_BYTES);
        QUERY_CHECK_CODE(code, line, _return);
      } else {
        SArray* pOrgTbInfoArray = *(SArray**)pIter;
        QUERY_CHECK_NULL(pOrgTbInfoArray, code, line, _return, terrno)
        QUERY_CHECK_NULL(taosArrayPush(pOrgTbInfoArray, orgTbInfo), code, line, _return, terrno)
      }

      pOrgTbInfo = taosHashIterate(pVtbScan->otbNameToOtbInfoMap, pOrgTbInfo);

      code = taosHashRemove(pVtbScan->otbNameToOtbInfoMap, key, len);
      QUERY_CHECK_CODE(code, line, _return);
    }

    code = taosHashPut(pVtbScan->vtbUidToVgIdMapMap, &uid, sizeof(uid), &otbVgIdToOtbInfoArrayMap, POINTER_BYTES);
    QUERY_CHECK_CODE(code, line, _return);
  }

  return code;
_return:
  qError("%s failed at line %d since %s", __func__, line, tstrerror(code));
  return code;
}

static int32_t buildOrgTbInfoSingle(SOperatorInfo* pOperator, bool hasPartition) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;

  code = processChildTableListAndGenerateOrgTbInfoMap(pOperator);
  QUERY_CHECK_CODE(code, line, _return);

  // process tag
  code = getTagBlockAndProcess(pOperator, hasPartition);
  QUERY_CHECK_CODE(code, line, _return);

  return code;
_return:
  qError("%s failed at line %d since %s", __func__, line, tstrerror(code));
  return code;
}

static int32_t buildOrgTbInfoBatch(SOperatorInfo* pOperator, bool hasPartition) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = (SVtbScanDynCtrlInfo*)&pInfo->vtbScan;
  SExecTaskInfo*             pTaskInfo = pOperator->pTaskInfo;
  SArray*                    pColRefArray = NULL;
  SOperatorInfo*             pSystableScanOp = pOperator->pDownstream[0];
  SOperatorInfo*             pTagScanOp = pOperator->pDownstream[1];

  if (hasPartition) {
    pVtbScan->vtbUidToGroupIdMap = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
    pVtbScan->vtbGroupIdTagListMap = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_ENTRY_LOCK);
    pVtbScan->vtbGroupIdToVgIdMapMap = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_ENTRY_LOCK);

    QUERY_CHECK_NULL(pVtbScan->vtbGroupIdToVgIdMapMap, code, line, _return, terrno)
    QUERY_CHECK_NULL(pVtbScan->vtbUidToGroupIdMap, code, line, _return, terrno)
    QUERY_CHECK_NULL(pVtbScan->vtbGroupIdTagListMap, code, line, _return, terrno)
    taosHashSetFreeFp(pVtbScan->vtbGroupIdToVgIdMapMap, destroyOtbVgIdToOtbInfoArrayMap);
  } else {
    pVtbScan->otbVgIdToOtbInfoArrayMap = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
    QUERY_CHECK_NULL(pVtbScan->otbVgIdToOtbInfoArrayMap, code, line, _return, terrno)
  }

  while (true && hasPartition) {
    SSDataBlock* pTagVal = NULL;
    code = pTagScanOp->fpSet.getNextFn(pTagScanOp, &pTagVal);
    QUERY_CHECK_CODE(code, line, _return);
    if (pTagVal == NULL) {
      break;
    }

    SColumnInfoData *pUidCol = taosArrayGetLast(pTagVal->pDataBlock);
    QUERY_CHECK_NULL(pUidCol, code, line, _return, terrno)
    for (int32_t i = 0; i < pTagVal->info.rows; i++) {
      tb_uid_t uid = 0;
      if (!colDataIsNull_s(pUidCol, i)) {
        GET_TYPED_DATA(uid, int64_t, TSDB_DATA_TYPE_BIGINT, colDataGetNumData(pUidCol, i), 0);
        QUERY_CHECK_CODE(code, line, _return);
      }
      code = taosHashPut(pVtbScan->vtbUidToGroupIdMap, &uid, sizeof(uid), &pTagVal->info.id.groupId, sizeof(pTagVal->info.id.groupId));
      QUERY_CHECK_CODE(code, line, _return);
    }
    code = taosHashPut(pVtbScan->vtbGroupIdTagListMap, &pTagVal->info.id.groupId, sizeof(pTagVal->info.id.groupId), NULL, 0);
    QUERY_CHECK_CODE(code, line, _return);
  }

  for (int32_t i = 0; i < taosArrayGetSize(pVtbScan->childTableList); i++) {
    SArray* pColRefInfo = (SArray*)taosArrayGetP(pVtbScan->childTableList, i);
    QUERY_CHECK_NULL(pColRefInfo, code, line, _return, terrno)
    tb_uid_t uid = 0;
    int32_t  vgId = 0;
    code = virtualTableScanProcessColRefInfo(pOperator, pColRefInfo, &uid, &vgId, NULL);
    QUERY_CHECK_CODE(code, line, _return);

    SHashObj* otbVgIdToOtbInfoArrayMap = NULL;
    if (hasPartition) {
      uint64_t* groupId = (uint64_t *)taosHashGet(pVtbScan->vtbUidToGroupIdMap, &uid, sizeof(uid));
      QUERY_CHECK_NULL(groupId, code, line, _return, terrno)

      void* pHashIter = taosHashGet(pVtbScan->vtbGroupIdToVgIdMapMap, groupId, sizeof(*groupId));
      if (pHashIter) {
        otbVgIdToOtbInfoArrayMap = *(SHashObj**)pHashIter;
      } else {
        otbVgIdToOtbInfoArrayMap = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
        QUERY_CHECK_NULL(otbVgIdToOtbInfoArrayMap, code, line, _return, terrno)
        code = taosHashPut(pVtbScan->vtbGroupIdToVgIdMapMap, groupId, sizeof(*groupId), &otbVgIdToOtbInfoArrayMap, POINTER_BYTES);
        QUERY_CHECK_CODE(code, line, _return);
      }
    } else {
      otbVgIdToOtbInfoArrayMap = pVtbScan->otbVgIdToOtbInfoArrayMap;
    }

    size_t len = 0;
    void*  pOrgTbInfo = taosHashIterate(pVtbScan->otbNameToOtbInfoMap, NULL);
    while (pOrgTbInfo != NULL) {
      char*       key = taosHashGetKey(pOrgTbInfo, &len);
      SOrgTbInfo* orgTbInfo = (SOrgTbInfo*)pOrgTbInfo;
      void* pIter = taosHashGet(otbVgIdToOtbInfoArrayMap, &orgTbInfo->vgId, sizeof(orgTbInfo->vgId));
      if (!pIter) {
        SArray* pOrgTbInfoArray = taosArrayInit(1, sizeof(SOrgTbInfo));
        QUERY_CHECK_NULL(pOrgTbInfoArray, code, line, _return, terrno)
        QUERY_CHECK_NULL(taosArrayPush(pOrgTbInfoArray, orgTbInfo), code, line, _return, terrno)
        code = taosHashPut(otbVgIdToOtbInfoArrayMap, &orgTbInfo->vgId, sizeof(orgTbInfo->vgId), &pOrgTbInfoArray, POINTER_BYTES);
        QUERY_CHECK_CODE(code, line, _return);
      } else {
        SArray* pOrgTbInfoArray = *(SArray**)pIter;
        QUERY_CHECK_NULL(pOrgTbInfoArray, code, line, _return, terrno)
        QUERY_CHECK_NULL(taosArrayPush(pOrgTbInfoArray, orgTbInfo), code, line, _return, terrno)
      }

      pOrgTbInfo = taosHashIterate(pVtbScan->otbNameToOtbInfoMap, pOrgTbInfo);

      code = taosHashRemove(pVtbScan->otbNameToOtbInfoMap, key, len);
      QUERY_CHECK_CODE(code, line, _return);
    }
  }
  return code;
_return:
  qError("%s failed at line %d since %s", __func__, line, tstrerror(code));
  return code;
}

int32_t buildVirtualSuperTableScanChildTableMap(SOperatorInfo* pOperator) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = (SVtbScanDynCtrlInfo*)&pInfo->vtbScan;
  SExecTaskInfo*             pTaskInfo = pOperator->pTaskInfo;
  SArray*                    pColRefArray = NULL;
  SOperatorInfo*             pSystableScanOp = NULL;
  bool                       sysScanBootstrapped = false;

  taosHashClear(pVtbScan->resolvedColRefMap);
  pVtbScan->childTableMap = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  QUERY_CHECK_NULL(pVtbScan->childTableMap, code, line, _return, terrno)

  if (pInfo->qType == DYN_QTYPE_VTB_AGG || pInfo->qType == DYN_QTYPE_VTB_INTERVAL) {
    pVtbScan->otbNameToOtbInfoMap = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
    QUERY_CHECK_NULL(pVtbScan->otbNameToOtbInfoMap, code, line, _return, terrno)
    pSystableScanOp = pOperator->pDownstream[0];
  } else if (pInfo->qType == DYN_QTYPE_VTB_WINDOW || pInfo->qType == DYN_QTYPE_VTB_TS_SCAN) {
    pVtbScan->otbNameToOtbInfoMap = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
    QUERY_CHECK_NULL(pVtbScan->otbNameToOtbInfoMap, code, line, _return, terrno)
    pSystableScanOp = pOperator->pDownstream[1];
  } else {
    pSystableScanOp = pOperator->pDownstream[1];
  }

  while (true) {
    SSDataBlock *pChildInfo = NULL;
    code = dynFetchInitialSysScanBlock(pSystableScanOp, &sysScanBootstrapped, &pChildInfo);
    QUERY_CHECK_CODE(code, line, _return);
    if (pChildInfo == NULL) {
      break;
    }
    SColumnInfoData *pTableNameCol = taosArrayGet(pChildInfo->pDataBlock, 0);
    SColumnInfoData *pStbNameCol = taosArrayGet(pChildInfo->pDataBlock, 1);
    SColumnInfoData *pDbNameCol = taosArrayGet(pChildInfo->pDataBlock, 2);

    QUERY_CHECK_NULL(pTableNameCol, code, line, _return, terrno)
    QUERY_CHECK_NULL(pStbNameCol, code, line, _return, terrno)
    QUERY_CHECK_NULL(pDbNameCol, code, line, _return, terrno)

    for (int32_t i = 0; i < pChildInfo->info.rows; i++) {
      if (!colDataIsNull_s(pStbNameCol, i)) {
        char* stbrawname = colDataGetData(pStbNameCol, i);
        char* dbrawname = colDataGetData(pDbNameCol, i);
        char *ctbName = colDataGetData(pTableNameCol, i);

        if (tableInfoNeedCollect(dbrawname, stbrawname, pInfo->vtbScan.dbName, pInfo->vtbScan.tbName)) {
          SColRefInfo info = {0};
          code = getColRefInfo(&info, pChildInfo->pDataBlock, i);
          QUERY_CHECK_CODE(code, line, _return);

          if (pInfo->qType == DYN_QTYPE_VTB_SCAN) {
            if (pInfo->vtbScan.dynTbUid != 0 && info.uid != pInfo->vtbScan.dynTbUid) {
              qTrace("dynQueryCtrl tb uid filter, info uid:%" PRIu64 ", dyn tb uid:%" PRIu64, info.uid,
                     pInfo->vtbScan.dynTbUid);
              destroyColRefInfo(&info);
              continue;
            }
          }

          if (taosHashGet(pVtbScan->childTableMap, varDataVal(ctbName), varDataLen(ctbName)) == NULL) {
            pColRefArray = taosArrayInit(1, sizeof(SColRefInfo));
            QUERY_CHECK_NULL(pColRefArray, code, line, _return, terrno)
            QUERY_CHECK_NULL(taosArrayPush(pColRefArray, &info), code, line, _return, terrno)
            int32_t tableIdx = (int32_t)taosArrayGetSize(pVtbScan->childTableList);
            QUERY_CHECK_NULL(taosArrayPush(pVtbScan->childTableList, &pColRefArray), code, line, _return, terrno)
            code = taosHashPut(pVtbScan->childTableMap, varDataVal(ctbName), varDataLen(ctbName), &tableIdx, sizeof(tableIdx));
            QUERY_CHECK_CODE(code, line, _return);
          } else {
            int32_t *tableIdx = (int32_t*)taosHashGet(pVtbScan->childTableMap, varDataVal(ctbName), varDataLen(ctbName));
            QUERY_CHECK_NULL(tableIdx, code, line, _return, terrno)
            pColRefArray = (SArray *)taosArrayGetP(pVtbScan->childTableList, *tableIdx);
            QUERY_CHECK_NULL(pColRefArray, code, line, _return, terrno)
            QUERY_CHECK_NULL(taosArrayPush(pColRefArray, &info), code, line, _return, terrno)
          }
        }
      }
    }
  }

  code = dynResolveChildTableRefsBySysScan(pOperator, pSystableScanOp);
  QUERY_CHECK_CODE(code, line, _return);

  if (pInfo->qType == DYN_QTYPE_VTB_SCAN && pTaskInfo->pStreamRuntimeInfo != NULL) {
    code = dynCollectResolvedOrgTbVgFromChildTables(pOperator);
    QUERY_CHECK_CODE(code, line, _return);
  }

  switch (pInfo->qType) {
    case DYN_QTYPE_VTB_TS_SCAN:
    case DYN_QTYPE_VTB_WINDOW:
    case DYN_QTYPE_VTB_INTERVAL: {
      code = buildOrgTbInfoBatch(pOperator, false);
      break;
    }
    case DYN_QTYPE_VTB_AGG: {
      if (pVtbScan->batchProcessChild) {
        code = buildOrgTbInfoBatch(pOperator, pVtbScan->hasPartition);
      } else {
        code = buildOrgTbInfoSingle(pOperator, pVtbScan->hasPartition);
      }
      break;
    }
    case DYN_QTYPE_VTB_SCAN: {
      code = processOrgTbVg(pVtbScan, pTaskInfo, 1);
      break;
    }
    default: {
      code = TSDB_CODE_PLAN_INVALID_DYN_CTRL_TYPE;
      break;
    }
  }

  QUERY_CHECK_CODE(code, line, _return);

_return:
  if (code) {
    qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
  }
  return code;
}

int32_t buildVirtualNormalChildTableScanChildTableMap(SOperatorInfo* pOperator) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = (SVtbScanDynCtrlInfo*)&pInfo->vtbScan;
  SExecTaskInfo*             pTaskInfo = pOperator->pTaskInfo;
  SOperatorInfo*             pSystableScanOp = pOperator->pDownstream[1];
  int32_t                    rversion = 0;
  bool                       sysScanBootstrapped = false;

  taosHashClear(pVtbScan->resolvedColRefMap);
  pInfo->vtbScan.colRefInfo = taosArrayInit(1, sizeof(SColRefInfo));
  QUERY_CHECK_NULL(pInfo->vtbScan.colRefInfo, code, line, _return, terrno)

  while (true) {
    SSDataBlock *pTableInfo = NULL;
    code = dynFetchInitialSysScanBlock(pSystableScanOp, &sysScanBootstrapped, &pTableInfo);
    QUERY_CHECK_CODE(code, line, _return);
    if (pTableInfo == NULL) {
      break;
    }

    SColumnInfoData *pTableNameCol = taosArrayGet(pTableInfo->pDataBlock, 0);
    SColumnInfoData *pDbNameCol = taosArrayGet(pTableInfo->pDataBlock, 2);
    SColumnInfoData *pRefVerCol = taosArrayGet(pTableInfo->pDataBlock, 8);

    QUERY_CHECK_NULL(pTableNameCol, code, line, _return, terrno)
    QUERY_CHECK_NULL(pDbNameCol, code, line, _return, terrno)
    QUERY_CHECK_NULL(pRefVerCol, code, line, _return, terrno)

    for (int32_t i = 0; i < pTableInfo->info.rows; i++) {
      if (!colDataIsNull_s(pRefVerCol, i)) {
        GET_TYPED_DATA(rversion, int32_t, TSDB_DATA_TYPE_INT, colDataGetNumData(pRefVerCol, i), 0);
      }

      if (!colDataIsNull_s(pTableNameCol, i)) {
        char* tbrawname = colDataGetData(pTableNameCol, i);
        char* dbrawname = colDataGetData(pDbNameCol, i);
        QUERY_CHECK_NULL(tbrawname, code, line, _return, terrno)
        QUERY_CHECK_NULL(dbrawname, code, line, _return, terrno)

        if (tableInfoNeedCollect(dbrawname, tbrawname, pInfo->vtbScan.dbName, pInfo->vtbScan.tbName)) {
          SColRefInfo info = {0};
          code = getColRefInfo(&info, pTableInfo->pDataBlock, i);
          QUERY_CHECK_CODE(code, line, _return);

          QUERY_CHECK_NULL(taosArrayPush(pInfo->vtbScan.colRefInfo, &info), code, line, _return, terrno)
        }
      }
    }
  }

  code = dynResolveColRefArrayBySysScan(pOperator, pSystableScanOp, pInfo->vtbScan.colRefInfo);
  QUERY_CHECK_CODE(code, line, _return);

  if (rversion != pVtbScan->rversion || pVtbScan->existOrgTbVg == NULL) {
    code = dynCollectResolvedOrgTbVgFromArray(pOperator, pInfo->vtbScan.colRefInfo);
    QUERY_CHECK_CODE(code, line, _return);
  }
  code = processOrgTbVg(pVtbScan, pTaskInfo, rversion);
  QUERY_CHECK_CODE(code, line, _return);

_return:
  if (code) {
    qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
  }
  return code;
}

int32_t virtualTableScanCheckNeedRedeploy(SOperatorInfo* pOperator) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = (SVtbScanDynCtrlInfo*)&pInfo->vtbScan;
  SExecTaskInfo*             pTaskInfo = pOperator->pTaskInfo;

  SArray *tmpArray = NULL;
  tmpArray = atomic_load_ptr(&pTaskInfo->pStreamRuntimeInfo->vtableDeployInfo.addedVgInfo);
  if (tmpArray && tmpArray == atomic_val_compare_exchange_ptr(&pTaskInfo->pStreamRuntimeInfo->vtableDeployInfo.addedVgInfo, tmpArray, NULL)) {
    for (int32_t i = 0; i < taosArrayGetSize(tmpArray); i++) {
      SStreamTaskAddr* pTaskAddr = (SStreamTaskAddr*)taosArrayGet(tmpArray, i);
      code = taosHashPut(pVtbScan->existOrgTbVg, &pTaskAddr->nodeId, sizeof(pTaskAddr->nodeId), NULL, 0);
      QUERY_CHECK_CODE(code, line, _return);
      if (pVtbScan->newAddedVgInfo == NULL) {
        pVtbScan->newAddedVgInfo = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
        QUERY_CHECK_NULL(pVtbScan->newAddedVgInfo, code, line, _return, terrno)
      }
      code = taosHashPut(pVtbScan->newAddedVgInfo, &pTaskAddr->nodeId, sizeof(pTaskAddr->nodeId), pTaskAddr, sizeof(SStreamTaskAddr));
      QUERY_CHECK_CODE(code, line, _return);
    }
    pVtbScan->needRedeploy = false;
  } else {
    code = TSDB_CODE_STREAM_VTABLE_NEED_REDEPLOY;
    QUERY_CHECK_CODE(code, line, _return);
  }

_return:
  taosArrayClear(tmpArray);
  taosArrayDestroy(tmpArray);
  if (code) {
    qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
  }
  return code;
}

int32_t virtualTableScanBuildDownStreamOpParam(SOperatorInfo* pOperator, tb_uid_t uid, int32_t vgId) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = (SVtbScanDynCtrlInfo*)&pInfo->vtbScan;
  SExecTaskInfo*             pTaskInfo = pOperator->pTaskInfo;

  pVtbScan->vtbScanParam = NULL;
  code = buildVtbScanOperatorParam(pInfo, &pVtbScan->vtbScanParam, uid);
  QUERY_CHECK_CODE(code, line, _return);

  void* pIter = taosHashIterate(pVtbScan->otbNameToOtbInfoMap, NULL);
  while (pIter != NULL) {
    SOrgTbInfo*      pMap = (SOrgTbInfo*)pIter;
    SOperatorParam*  pExchangeParam = NULL;
    SStreamTaskAddr* addr = taosHashGet(pVtbScan->newAddedVgInfo, &pMap->vgId, sizeof(pMap->vgId));
    if (addr != NULL) {
      SDownstreamSourceNode newSource = {0};
      newSource.type = QUERY_NODE_DOWNSTREAM_SOURCE;
      newSource.clientId = pTaskInfo->id.taskId;// current task's taskid
      newSource.taskId = addr->taskId;
      newSource.fetchMsgType = TDMT_STREAM_FETCH;
      newSource.localExec = false;
      newSource.addr.nodeId = addr->nodeId;
      memcpy(&newSource.addr.epSet, &addr->epset, sizeof(SEpSet));

      code = buildExchangeOperatorParamForVScan(&pExchangeParam, 0, pMap, &newSource);
      QUERY_CHECK_CODE(code, line, _return);
      code = taosHashRemove(pVtbScan->newAddedVgInfo, &pMap->vgId, sizeof(pMap->vgId));
      QUERY_CHECK_CODE(code, line, _return);
    } else {
      code = buildExchangeOperatorParamForVScan(&pExchangeParam, 0, pMap, NULL);
      QUERY_CHECK_CODE(code, line, _return);
    }
    QUERY_CHECK_NULL(taosArrayPush(((SVTableScanOperatorParam*)pVtbScan->vtbScanParam->value)->pOpParamArray, &pExchangeParam), code, line, _return, terrno)
    pIter = taosHashIterate(pVtbScan->otbNameToOtbInfoMap, pIter);
  }

  SOperatorParam*  pExchangeParam = NULL;
  code = buildExchangeOperatorParamForVTagScan(&pExchangeParam, 0, vgId, uid);
  QUERY_CHECK_CODE(code, line, _return);
  ((SVTableScanOperatorParam*)pVtbScan->vtbScanParam->value)->pTagScanOp = pExchangeParam;

_return:
  if (code) {
    qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
  }
  return code;
}

int32_t virtualTableScanGetNext(SOperatorInfo* pOperator, SSDataBlock** pRes) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = (SVtbScanDynCtrlInfo*)&pInfo->vtbScan;
  SOperatorInfo*             pVtbScanOp = pOperator->pDownstream[0];

  pVtbScan->otbNameToOtbInfoMap = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  QUERY_CHECK_NULL(pVtbScan->otbNameToOtbInfoMap, code, line, _return, terrno)
  taosHashSetFreeFp(pVtbScan->otbNameToOtbInfoMap, destroySOrgTbInfo);

  while (true) {
    if (pVtbScan->curTableIdx == pVtbScan->lastTableIdx) {
      code = pVtbScanOp->fpSet.getNextFn(pVtbScanOp, pRes);
      QUERY_CHECK_CODE(code, line, _return);
    } else {
      taosHashClear(pVtbScan->otbNameToOtbInfoMap);
      SArray* pColRefInfo = NULL;
      if (pVtbScan->isSuperTable) {
        pColRefInfo = (SArray*)taosArrayGetP(pVtbScan->childTableList, pVtbScan->curTableIdx);
      } else {
        pColRefInfo = pInfo->vtbScan.colRefInfo;
      }
      QUERY_CHECK_NULL(pColRefInfo, code, line, _return, terrno)

      tb_uid_t  uid = 0;
      int32_t   vgId = 0;
      SHashObj* refMap = NULL;
      code = virtualTableScanProcessColRefInfo(pOperator, pColRefInfo, &uid, &vgId, &refMap);
      QUERY_CHECK_CODE(code, line, _return);

      qDebug("virtual table scan process subtable idx:%d uid:%" PRIu64 " vgId:%d", pVtbScan->curTableIdx, uid, vgId);

      code = buildRefSlotGroupsFromRefMap(refMap, pVtbScan->readColList, &pVtbScan->refColGroups);
      QUERY_CHECK_CODE(code, line, _return);

      code = virtualTableScanBuildDownStreamOpParam(pOperator, uid, vgId);
      QUERY_CHECK_CODE(code, line, _return);

      // reset downstream operator's status
      pVtbScanOp->status = OP_NOT_OPENED;
      code = pVtbScanOp->fpSet.getNextExtFn(pVtbScanOp, pVtbScan->vtbScanParam, pRes);
      QUERY_CHECK_CODE(code, line, _return);
    }

    if (*pRes) {
      // has result, still read data from this table.
      pVtbScan->lastTableIdx = pVtbScan->curTableIdx;
      break;
    } else {
      // no result, read next table.
      pVtbScan->curTableIdx++;
      if (pVtbScan->isSuperTable) {
        if (pVtbScan->curTableIdx >= taosArrayGetSize(pVtbScan->childTableList)) {
          setOperatorCompleted(pOperator);
          break;
        }
      } else {
        setOperatorCompleted(pOperator);
        break;
      }
    }
  }

_return:
  taosHashCleanup(pVtbScan->otbNameToOtbInfoMap);
  pVtbScan->otbNameToOtbInfoMap = NULL;
  if (code) {
    qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
  }
  return code;
}

int32_t vtbScanOpen(SOperatorInfo* pOperator) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = (SVtbScanDynCtrlInfo*)&pInfo->vtbScan;

  if (OPTR_IS_OPENED(pOperator)) {
    return code;
  }

  if (pVtbScan->isSuperTable) {
    code = buildVirtualSuperTableScanChildTableMap(pOperator);
    QUERY_CHECK_CODE(code, line, _return);
  } else {
    code = buildVirtualNormalChildTableScanChildTableMap(pOperator);
    QUERY_CHECK_CODE(code, line, _return);
  }

  OPTR_SET_OPENED(pOperator);

_return:
  if (code) {
    qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
    pOperator->pTaskInfo->code = code;
    T_LONG_JMP(pOperator->pTaskInfo->env, code);
  }
  return code;
}

int32_t vtbScanNext(SOperatorInfo* pOperator, SSDataBlock** pRes) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = (SVtbScanDynCtrlInfo*)&pInfo->vtbScan;

  QRY_PARAM_CHECK(pRes);
  if (pOperator->status == OP_EXEC_DONE && !pOperator->pOperatorGetParam) {
    return code;
  }
  if (pOperator->pOperatorGetParam) {
    if (pOperator->status == OP_EXEC_DONE) {
      pOperator->status = OP_OPENED;
    }
    pVtbScan->curTableIdx = 0;
    pVtbScan->lastTableIdx = -1;
    pVtbScan->window = ((SDynQueryCtrlOperatorParam *)(pOperator->pOperatorGetParam)->value)->window;
    freeOperatorParam(pOperator->pOperatorGetParam, OP_GET_PARAM);
    pOperator->pOperatorGetParam = NULL;
  } else {
    pVtbScan->window.skey = INT64_MAX;
    pVtbScan->window.ekey = INT64_MIN;
  }

  if (pVtbScan->needRedeploy) {
    code = virtualTableScanCheckNeedRedeploy(pOperator);
    QUERY_CHECK_CODE(code, line, _return);
  }

  code = pOperator->fpSet._openFn(pOperator);
  QUERY_CHECK_CODE(code, line, _return);

  if (pVtbScan->isSuperTable && taosArrayGetSize(pVtbScan->childTableList) == 0) {
    setOperatorCompleted(pOperator);
    return code;
  }

  code = virtualTableScanGetNext(pOperator, pRes);
  QUERY_CHECK_CODE(code, line, _return);

  return code;

_return:
  if (code) {
    qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
    pOperator->pTaskInfo->code = code;
    T_LONG_JMP(pOperator->pTaskInfo->env, code);
  }
  return code;
}

/*
 * Open dynamic vtable operator and build child-table mapping once.
 *
 * @param pOperator Dynamic-query control operator.
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise error code.
 */
static int32_t vtbDefaultOpen(SOperatorInfo* pOperator) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = (SVtbScanDynCtrlInfo*)&pInfo->vtbScan;

  if (OPTR_IS_OPENED(pOperator)) {
    return code;
  }

  code = buildVirtualSuperTableScanChildTableMap(pOperator);
  QUERY_CHECK_CODE(code, line, _return);

  OPTR_SET_OPENED(pOperator);

_return:
  if (code) {
    qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
    pOperator->pTaskInfo->code = code;
    T_LONG_JMP(pOperator->pTaskInfo->env, code);
  }
  return code;
}

/*
 * Get next result block for virtual-table ts-scan workflow.
 *
 * @param pOperator Dynamic-query control operator.
 * @param pRes Output result data block pointer.
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise error code.
 */
int32_t vtbTsScanNext(SOperatorInfo* pOperator, SSDataBlock** pRes) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = (SVtbScanDynCtrlInfo*)&pInfo->vtbScan;
  SOperatorInfo*             pMergeOp = pOperator->pDownstream[0];
  SOperatorParam*            pMergeParam = NULL;
  SSDataBlock*               pResult = NULL;

  QRY_PARAM_CHECK(pRes);
  if (pOperator->status == OP_EXEC_DONE) {
    return code;
  }

  code = pOperator->fpSet._openFn(pOperator);
  QUERY_CHECK_CODE(code, line, _return);

  if (pVtbScan->genNewParam) {
    code = buildMergeOperatorParamForTsScan(pInfo, pMergeOp->numOfDownstream, &pMergeParam);
    QUERY_CHECK_CODE(code, line, _return);

    code = pMergeOp->fpSet.getNextExtFn(pMergeOp, pMergeParam, &pResult);
    QUERY_CHECK_CODE(code, line, _return);
    pVtbScan->genNewParam = false;
  } else {
    code = pMergeOp->fpSet.getNextFn(pMergeOp, &pResult);
    QUERY_CHECK_CODE(code, line, _return);
  }

  if (pResult) {
    *pRes = pResult;
  } else {
    *pRes = NULL;
    setOperatorCompleted(pOperator);
  }

_return:
  if (code) {
    qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
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

static void updateDynTbUidIfNeeded(SVtbScanDynCtrlInfo* pVtbScan, SStreamRuntimeInfo* pStreamRuntimeInfo) {
  if (pStreamRuntimeInfo == NULL) {
    return;
  }

  SArray* vals = pStreamRuntimeInfo->funcInfo.pStreamPartColVals;
  for (int32_t i = 0; i < taosArrayGetSize(vals); ++i) {
    SStreamGroupValue* pValue = taosArrayGet(vals, i);
    if (pValue != NULL && pValue->isTbname && pValue->uid != pVtbScan->dynTbUid) {
      qTrace("dynQueryCtrl dyn tb uid:%" PRIu64 " reset to:%" PRIu64, pVtbScan->dynTbUid, pValue->uid);

      pVtbScan->dynTbUid = pValue->uid;
      break;
    }
  }
}

static int32_t initVtbScanInfo(SDynQueryCtrlOperatorInfo* pInfo, SMsgCb* pMsgCb,
                               SDynQueryCtrlPhysiNode* pPhyciNode, SExecTaskInfo* pTaskInfo) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      line = 0;

  code = tsem_init(&pInfo->vtbScan.ready, 0, 0);
  QUERY_CHECK_CODE(code, line, _return);

  pInfo->vtbScan.genNewParam = true;
  pInfo->vtbScan.batchProcessChild = pPhyciNode->vtbScan.batchProcessChild;
  pInfo->vtbScan.hasPartition = pPhyciNode->vtbScan.hasPartition;
  pInfo->vtbScan.scanAllCols = pPhyciNode->vtbScan.scanAllCols;
  pInfo->vtbScan.isSuperTable = pPhyciNode->vtbScan.isSuperTable;
  pInfo->vtbScan.rversion = pPhyciNode->vtbScan.rversion;
  pInfo->vtbScan.uid = pPhyciNode->vtbScan.uid;
  pInfo->vtbScan.suid = pPhyciNode->vtbScan.suid;
  pInfo->vtbScan.epSet = pPhyciNode->vtbScan.mgmtEpSet;
  pInfo->vtbScan.acctId = pPhyciNode->vtbScan.accountId;
  pInfo->vtbScan.needRedeploy = false;
  pInfo->vtbScan.pMsgCb = pMsgCb;
  pInfo->vtbScan.curTableIdx = 0;
  pInfo->vtbScan.lastTableIdx = -1;
  pInfo->vtbScan.dynTbUid = 0;
  pInfo->vtbScan.dbName = taosStrdup(pPhyciNode->vtbScan.dbName);
  pInfo->vtbScan.tbName = taosStrdup(pPhyciNode->vtbScan.tbName);
  QUERY_CHECK_NULL(pInfo->vtbScan.dbName, code, line, _return, terrno)
  QUERY_CHECK_NULL(pInfo->vtbScan.tbName, code, line, _return, terrno)
  pInfo->vtbScan.existOrgTbVg = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  QUERY_CHECK_NULL(pInfo->vtbScan.existOrgTbVg, code, line, _return, terrno)
  SNode* node = NULL;
  FOREACH(node, pPhyciNode->vtbScan.pOrgVgIds) {
    SValueNode* valueNode = (SValueNode*)node;
    int32_t vgId = (int32_t)valueNode->datum.i;
    code = taosHashPut(pInfo->vtbScan.existOrgTbVg, &vgId, sizeof(vgId), NULL, 0);
    QUERY_CHECK_CODE(code, line, _return);
  }

  if (pPhyciNode->dynTbname && pTaskInfo) {
    updateDynTbUidIfNeeded(&pInfo->vtbScan, pTaskInfo->pStreamRuntimeInfo);
  }

  pInfo->vtbScan.readColList = taosArrayInit(LIST_LENGTH(pPhyciNode->vtbScan.pScanCols), sizeof(col_id_t));
  QUERY_CHECK_NULL(pInfo->vtbScan.readColList, code, line, _return, terrno)

  SNode* colNode = NULL;
  FOREACH(colNode, pPhyciNode->vtbScan.pScanCols) {
    SColumnNode* pNode = (SColumnNode*)colNode;
    QUERY_CHECK_NULL(pNode, code, line, _return, terrno)
    QUERY_CHECK_NULL(taosArrayPush(pInfo->vtbScan.readColList, &pNode->colId), code, line, _return, terrno)
  }

  pInfo->vtbScan.readColSet =
      taosHashInit(taosArrayGetSize(pInfo->vtbScan.readColList) > 0 ? taosArrayGetSize(pInfo->vtbScan.readColList) : 1,
                   taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT), true, HASH_NO_LOCK);
  QUERY_CHECK_NULL(pInfo->vtbScan.readColSet, code, line, _return, terrno)
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->vtbScan.readColList); i++) {
    col_id_t colId = *(col_id_t*)taosArrayGet(pInfo->vtbScan.readColList, i);
    code = taosHashPut(pInfo->vtbScan.readColSet, &colId, sizeof(colId), NULL, 0);
    QUERY_CHECK_CODE(code, line, _return);
  }

  pInfo->vtbScan.refColGroups = NULL;

  pInfo->vtbScan.childTableList = taosArrayInit(10, POINTER_BYTES);
  QUERY_CHECK_NULL(pInfo->vtbScan.childTableList, code, line, _return, terrno)

  pInfo->vtbScan.dbVgInfoMap = taosHashInit(taosArrayGetSize(pInfo->vtbScan.childTableList), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  QUERY_CHECK_NULL(pInfo->vtbScan.dbVgInfoMap, code, line, _return, terrno)
  pInfo->vtbScan.resolvedColRefMap = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  QUERY_CHECK_NULL(pInfo->vtbScan.resolvedColRefMap, code, line, _return, terrno)

  pInfo->vtbScan.otbNameToOtbInfoMap = NULL;
  pInfo->vtbScan.otbVgIdToOtbInfoArrayMap = NULL;
  pInfo->vtbScan.vtbUidToVgIdMapMap = NULL;
  pInfo->vtbScan.vtbGroupIdToVgIdMapMap = NULL;
  pInfo->vtbScan.vtbUidTagListMap = NULL;
  pInfo->vtbScan.vtbGroupIdTagListMap = NULL;
  pInfo->vtbScan.vtbUidToGroupIdMap = NULL;

  return code;
_return:
  // no need to destroy array and hashmap allocated in this function,
  // since the operator's destroy function will take care of it
  qError("%s failed at line %d since %s", __func__, line, tstrerror(code));
  return code;
}

static int32_t initVtbWindowInfo(SDynQueryCtrlOperatorInfo* pInfo, SDynQueryCtrlPhysiNode* pPhyciNode,
                                 SExecTaskInfo* pTaskInfo, SOperatorInfo* pOperator) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              line = 0;
  SDataBlockDescNode*  pDescNode = pPhyciNode->node.pOutputDataBlockDesc;

  pInfo->vtbWindow.wstartSlotId = pPhyciNode->vtbWindow.wstartSlotId;
  pInfo->vtbWindow.wendSlotId = pPhyciNode->vtbWindow.wendSlotId;
  pInfo->vtbWindow.wdurationSlotId = pPhyciNode->vtbWindow.wdurationSlotId;
  pInfo->vtbWindow.pTargets = pPhyciNode->vtbWindow.pTargets;
  pInfo->vtbWindow.isVstb = pPhyciNode->vtbWindow.isVstb;
  pInfo->vtbWindow.extendOption = pPhyciNode->vtbWindow.extendOption;

  pInfo->vtbWindow.pRes = createDataBlockFromDescNode(pDescNode);
  QUERY_CHECK_NULL(pInfo->vtbWindow.pRes, code, line, _return, terrno)

  pInfo->vtbWindow.pWins = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
  QUERY_CHECK_NULL(pInfo->vtbWindow.pWins, code, line, _return, terrno)

  pInfo->vtbWindow.outputWstartSlotId = -1;
  pInfo->vtbWindow.outputWendSlotId = -1;
  pInfo->vtbWindow.outputWdurationSlotId = -1;
  pInfo->vtbWindow.curWinBatchIdx = 0;

  initResultSizeInfo(&pOperator->resultInfo, 1);
  code = blockDataEnsureCapacity(pInfo->vtbWindow.pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, line, _return);

  return code;
_return:
  qError("%s failed at line %d since %s", __func__, line, tstrerror(code));
  return code;
}

static int32_t extractTsCol(SSDataBlock* pBlock, int32_t slotId, TSKEY** ppTsCols) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (pBlock->pDataBlock != NULL && pBlock->info.dataLoad) {
    SColumnInfoData* pColDataInfo = taosArrayGet(pBlock->pDataBlock, slotId);
    QUERY_CHECK_NULL(pColDataInfo, code, lino, _return, terrno)

    *ppTsCols = (int64_t*)pColDataInfo->pData;

    if ((*ppTsCols)[0] != 0 && (pBlock->info.window.skey == 0 && pBlock->info.window.ekey == 0)) {
      code = blockDataUpdateTsWindow(pBlock, slotId);
      QUERY_CHECK_CODE(code, lino, _return);
    }
  }

  return code;
_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

int32_t vtbWindowOpen(SOperatorInfo* pOperator) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    lino = 0;
  SDynQueryCtrlOperatorInfo* pDynInfo = pOperator->info;
  SExecTaskInfo*             pTaskInfo = pOperator->pTaskInfo;
  SVtbWindowDynCtrlInfo*     pInfo = &pDynInfo->vtbWindow;

  if (OPTR_IS_OPENED(pOperator)) {
    return code;
  }

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      break;
    }

    if (pInfo->outputWendSlotId == -1 && pInfo->outputWstartSlotId == -1 && pInfo->outputWdurationSlotId == -1) {
      for (int32_t i = 0; i < LIST_LENGTH(pInfo->pTargets); ++i) {
        STargetNode* pNode = (STargetNode*)nodesListGetNode(pInfo->pTargets, i);
        if (nodeType(pNode->pExpr) == QUERY_NODE_COLUMN && ((SColumnNode*)pNode->pExpr)->dataBlockId == pBlock->info.id.blockId) {
          if (((SColumnNode*)pNode->pExpr)->slotId == pDynInfo->vtbWindow.wstartSlotId) {
            pInfo->outputWstartSlotId = i;
          } else if (((SColumnNode*)pNode->pExpr)->slotId == pDynInfo->vtbWindow.wendSlotId) {
            pInfo->outputWendSlotId = i;
          } else if (((SColumnNode*)pNode->pExpr)->slotId == pDynInfo->vtbWindow.wdurationSlotId) {
            pInfo->outputWdurationSlotId = i;
          }
        }
      }
    }

    TSKEY* wstartCol = NULL;
    TSKEY* wendCol = NULL;

    code = extractTsCol(pBlock, pDynInfo->vtbWindow.wstartSlotId, &wstartCol);
    QUERY_CHECK_CODE(code, lino, _return);
    code = extractTsCol(pBlock, pDynInfo->vtbWindow.wendSlotId, &wendCol);
    QUERY_CHECK_CODE(code, lino, _return);

    SArray* pWin = taosArrayInit(pBlock->info.rows, sizeof(SExtWinTimeWindow));
    QUERY_CHECK_NULL(pWin, code, lino, _return, terrno)

    QUERY_CHECK_NULL(taosArrayReserve(pWin, pBlock->info.rows), code, lino, _return, terrno);

    for (int32_t i = 0; i < pBlock->info.rows; i++) {
      SExtWinTimeWindow* pWindow = taosArrayGet(pWin, i);
      QUERY_CHECK_NULL(pWindow, code, lino, _return, terrno)
      pWindow->tw.skey = wstartCol[i];
      pWindow->tw.ekey = wendCol[i] + 1;
      pWindow->winOutIdx = -1;
    }

    QUERY_CHECK_NULL(taosArrayPush(pDynInfo->vtbWindow.pWins, &pWin), code, lino, _return, terrno);
  }

  // handle first window's start key and last window's end key
  int32_t winBatchNum = (int32_t)taosArrayGetSize(pDynInfo->vtbWindow.pWins);
  if (winBatchNum > 0) {
    SArray* firstBatch = (SArray*)taosArrayGetP(pDynInfo->vtbWindow.pWins, 0);
    SArray* lastBatch = (SArray*)taosArrayGetP(pDynInfo->vtbWindow.pWins, winBatchNum - 1);

    QUERY_CHECK_NULL(firstBatch, code, lino, _return, terrno)
    QUERY_CHECK_NULL(lastBatch, code, lino, _return, terrno)

    SExtWinTimeWindow* firstWin = (SExtWinTimeWindow*)taosArrayGet(firstBatch, 0);
    SExtWinTimeWindow* lastWin = (SExtWinTimeWindow*)taosArrayGetLast(lastBatch);

    QUERY_CHECK_NULL(firstWin, code, lino, _return, terrno)
    QUERY_CHECK_NULL(lastWin, code, lino, _return, terrno)

    if (pInfo->extendOption == STATE_WIN_EXTEND_OPTION_BACKWARD) {
      lastWin->tw.ekey = INT64_MAX;
    }
    if (pInfo->extendOption == STATE_WIN_EXTEND_OPTION_FORWARD) {
      firstWin->tw.skey = INT64_MIN;
    }
  }

  if (pInfo->isVstb) {
    code = buildVirtualSuperTableScanChildTableMap(pOperator);
    QUERY_CHECK_CODE(code, lino, _return);
  }

  OPTR_SET_OPENED(pOperator);
_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return code;
}

static int32_t buildExternalWindowOperatorParamEx(SDynQueryCtrlOperatorInfo* pInfo, SOperatorParam** ppRes, SArray* pWins, int32_t idx) {
  int32_t                       code = TSDB_CODE_SUCCESS;
  int32_t                       lino = 0;
  SExternalWindowOperatorParam* pExtWinOp = NULL;

  *ppRes = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(*ppRes, code, lino, _return, terrno)

  pExtWinOp = taosMemoryMalloc(sizeof(SExternalWindowOperatorParam));
  QUERY_CHECK_NULL(pExtWinOp, code, lino, _return, terrno)

  pExtWinOp->ExtWins = taosArrayDup(pWins, NULL);
  QUERY_CHECK_NULL(pExtWinOp->ExtWins, code, lino, _return, terrno)

  SExtWinTimeWindow* firstWin = (SExtWinTimeWindow*)taosArrayGet(pWins, 0);
  SExtWinTimeWindow* lastWin = (SExtWinTimeWindow*)taosArrayGetLast(pWins);

  QUERY_CHECK_NULL(firstWin, code, lino, _return, terrno);
  QUERY_CHECK_NULL(lastWin, code, lino, _return, terrno);

  (*ppRes)->pChildren = taosArrayInit(1, POINTER_BYTES);
  QUERY_CHECK_NULL((*ppRes)->pChildren, code, lino, _return, terrno)

  SOperatorParam* pExchangeParam = NULL;
  code = buildBatchExchangeOperatorParamForVirtual(
      &pExchangeParam, 0, NULL, 0, pInfo->vtbScan.otbVgIdToOtbInfoArrayMap,
      (STimeWindow){.skey = firstWin->tw.skey, .ekey = lastWin->tw.ekey}, EX_SRC_TYPE_VSTB_WIN_SCAN,
      QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN);
  QUERY_CHECK_CODE(code, lino, _return);

  QUERY_CHECK_NULL(taosArrayPush((*ppRes)->pChildren, &pExchangeParam), code, lino, _return, terrno)

  (*ppRes)->opType = QUERY_NODE_PHYSICAL_PLAN_EXTERNAL_WINDOW;
  (*ppRes)->downstreamIdx = idx;
  (*ppRes)->value = pExtWinOp;
  (*ppRes)->reUse = false;

  return code;
_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  if (pExtWinOp) {
    if (pExtWinOp->ExtWins) {
      taosArrayDestroy(pExtWinOp->ExtWins);
    }
    taosMemoryFree(pExtWinOp);
  }
  if (*ppRes) {
    if ((*ppRes)->pChildren) {
      for (int32_t i = 0; i < taosArrayGetSize((*ppRes)->pChildren); i++) {
        SOperatorParam* pChildParam = taosArrayGetP((*ppRes)->pChildren, i);
        if (pChildParam) {
          freeOperatorParam(pChildParam, OP_GET_PARAM);
        }
      }
      taosArrayDestroy((*ppRes)->pChildren);
    }
    taosMemoryFree(*ppRes);
    *ppRes = NULL;
  }
  return code;
}

int32_t vtbWindowNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    lino = 0;
  SDynQueryCtrlOperatorInfo* pDynInfo = pOperator->info;
  SExecTaskInfo*             pTaskInfo = pOperator->pTaskInfo;
  int32_t                    numOfWins = 0;
  SOperatorInfo*             mergeOp = NULL;
  SOperatorInfo*             extWinOp = NULL;
  SOperatorParam*            pMergeParam = NULL;
  SOperatorParam*            pExtWinParam = NULL;
  SVtbWindowDynCtrlInfo*     pInfo = &pDynInfo->vtbWindow;
  SSDataBlock*               pRes = pInfo->pRes;

  code = pOperator->fpSet._openFn(pOperator);
  QUERY_CHECK_CODE(code, lino, _return);

  if (pInfo->curWinBatchIdx >= taosArrayGetSize(pInfo->pWins)) {
    *ppRes = NULL;
    return code;
  }

  SArray* pWinArray = (SArray*)taosArrayGetP(pInfo->pWins, pInfo->curWinBatchIdx);
  QUERY_CHECK_NULL(pWinArray, code, lino, _return, terrno)

  numOfWins = (int32_t)taosArrayGetSize(pWinArray);

  if (pInfo->isVstb) {
    extWinOp = pOperator->pDownstream[2];
    code = buildExternalWindowOperatorParamEx(pDynInfo, &pExtWinParam, pWinArray, extWinOp->numOfDownstream);
    QUERY_CHECK_CODE(code, lino, _return);

    SSDataBlock* pExtWinBlock = NULL;
    code = extWinOp->fpSet.getNextExtFn(extWinOp, pExtWinParam, &pExtWinBlock);
    QUERY_CHECK_CODE(code, lino, _return);
    setOperatorCompleted(extWinOp);
    // Free the parameter after operator completes, as it's been saved to the operator
    if (extWinOp->pOperatorGetParam) {
      freeOperatorParam(extWinOp->pOperatorGetParam, OP_GET_PARAM);
      extWinOp->pOperatorGetParam = NULL;
    }
    // Also free downstream params if any
    if (extWinOp->pDownstreamGetParams) {
      for (int32_t i = 0; i < extWinOp->numOfDownstream; i++) {
        if (extWinOp->pDownstreamGetParams[i]) {
          freeOperatorParam(extWinOp->pDownstreamGetParams[i], OP_GET_PARAM);
          extWinOp->pDownstreamGetParams[i] = NULL;
        }
      }
    }

    blockDataCleanup(pRes);
    code = blockDataEnsureCapacity(pRes, numOfWins);
    QUERY_CHECK_CODE(code, lino, _return);

    if (pExtWinBlock) {
      code = copyColumnsValue(pInfo->pTargets, pExtWinBlock->info.id.blockId, pRes, pExtWinBlock, numOfWins);
      QUERY_CHECK_CODE(code, lino, _return);

      if (pInfo->curWinBatchIdx == 0) {
        // first batch, bound _wstart by upstream window range
        SExtWinTimeWindow* firstWin = (SExtWinTimeWindow*)taosArrayGet(taosArrayGetP(pInfo->pWins, 0), 0);
        QUERY_CHECK_NULL(firstWin, code, lino, _return, terrno)

        firstWin->tw.skey = TMAX(firstWin->tw.skey, pExtWinBlock->info.window.skey);
      }
      if (pInfo->curWinBatchIdx == taosArrayGetSize(pInfo->pWins) - 1) {
        // last batch, bound _wend by upstream window range
        SExtWinTimeWindow* lastWin = (SExtWinTimeWindow*)taosArrayGetLast(taosArrayGetP(pInfo->pWins, taosArrayGetSize(pInfo->pWins) - 1));
        QUERY_CHECK_NULL(lastWin, code, lino, _return, terrno)

        lastWin->tw.ekey = TMIN(lastWin->tw.ekey, pExtWinBlock->info.window.ekey + 1);
      }
    }
  } else {
    mergeOp = pOperator->pDownstream[1];
    code = buildMergeOperatorParam(pDynInfo, &pMergeParam, pWinArray, mergeOp->numOfDownstream, numOfWins);
    QUERY_CHECK_CODE(code, lino, _return);

    SSDataBlock* pMergedBlock = NULL;
    code = mergeOp->fpSet.getNextExtFn(mergeOp, pMergeParam, &pMergedBlock);
    QUERY_CHECK_CODE(code, lino, _return);
    // Free the parameter after operator completes, as it's been saved to the operator
    if (mergeOp->pOperatorGetParam) {
      freeOperatorParam(mergeOp->pOperatorGetParam, OP_GET_PARAM);
      mergeOp->pOperatorGetParam = NULL;
    }
    // Also free downstream params if any
    if (mergeOp->pDownstreamGetParams) {
      for (int32_t i = 0; i < mergeOp->numOfDownstream; i++) {
        if (mergeOp->pDownstreamGetParams[i]) {
          freeOperatorParam(mergeOp->pDownstreamGetParams[i], OP_GET_PARAM);
          mergeOp->pDownstreamGetParams[i] = NULL;
        }
      }
    }

    blockDataCleanup(pRes);
    code = blockDataEnsureCapacity(pRes, numOfWins);
    QUERY_CHECK_CODE(code, lino, _return);

    if (pMergedBlock) {
      code = copyColumnsValue(pInfo->pTargets, pMergedBlock->info.id.blockId, pRes, pMergedBlock, numOfWins);
      QUERY_CHECK_CODE(code, lino, _return);

      if (pInfo->curWinBatchIdx == 0) {
        // first batch, bound _wstart by upstream window range
        SExtWinTimeWindow* firstWin = (SExtWinTimeWindow*)taosArrayGet(taosArrayGetP(pInfo->pWins, 0), 0);
        QUERY_CHECK_NULL(firstWin, code, lino, _return, terrno)

        firstWin->tw.skey = TMAX(firstWin->tw.skey, pMergedBlock->info.window.skey);
      }
      if (pInfo->curWinBatchIdx == taosArrayGetSize(pInfo->pWins) - 1) {
        // last batch, bound _wend by upstream window range
        SExtWinTimeWindow* lastWin = (SExtWinTimeWindow*)taosArrayGetLast(taosArrayGetP(pInfo->pWins, taosArrayGetSize(pInfo->pWins) - 1));
        QUERY_CHECK_NULL(lastWin, code, lino, _return, terrno)

        lastWin->tw.ekey = TMIN(lastWin->tw.ekey, pMergedBlock->info.window.ekey + 1);
      }
    }
  }


  if (pInfo->outputWstartSlotId != -1) {
    SColumnInfoData* pWstartCol = taosArrayGet(pRes->pDataBlock, pInfo->outputWstartSlotId);
    QUERY_CHECK_NULL(pWstartCol, code, lino, _return, terrno)

    for (int32_t i = 0; i < numOfWins; i++) {
      SExtWinTimeWindow* pWindow = (SExtWinTimeWindow*)taosArrayGet(pWinArray, i);
      QUERY_CHECK_NULL(pWindow, code, lino, _return, terrno)
      code = colDataSetVal(pWstartCol, i, (const char*)&pWindow->tw.skey, false);
      QUERY_CHECK_CODE(code, lino, _return);
    }
  }
  if (pInfo->outputWendSlotId != -1) {
    SColumnInfoData* pWendCol = taosArrayGet(pRes->pDataBlock, pInfo->outputWendSlotId);
    QUERY_CHECK_NULL(pWendCol, code, lino, _return, terrno)

    for (int32_t i = 0; i < numOfWins; i++) {
      SExtWinTimeWindow* pWindow = (SExtWinTimeWindow*)taosArrayGet(pWinArray, i);
      QUERY_CHECK_NULL(pWindow, code, lino, _return, terrno)
      TSKEY ekey = pWindow->tw.ekey - 1;
      code = colDataSetVal(pWendCol, i, (const char*)&ekey, false);
      QUERY_CHECK_CODE(code, lino, _return);
    }
  }
  if (pInfo->outputWdurationSlotId != -1) {
    SColumnInfoData* pWdurationCol = taosArrayGet(pRes->pDataBlock, pInfo->outputWdurationSlotId);
    QUERY_CHECK_NULL(pWdurationCol, code, lino, _return, terrno)

    for (int32_t i = 0; i < numOfWins; i++) {
      SExtWinTimeWindow* pWindow = (SExtWinTimeWindow*)taosArrayGet(pWinArray, i);
      QUERY_CHECK_NULL(pWindow, code, lino, _return, terrno)
      int64_t duration = pWindow->tw.ekey - 1 - pWindow->tw.skey;
      code = colDataSetVal(pWdurationCol, i, (const char*)&duration, false);
      QUERY_CHECK_CODE(code, lino, _return);
    }
  }

  pRes->info.rows = numOfWins;
  *ppRes = pRes;
  pInfo->curWinBatchIdx++;

  return code;

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return code;
}

static int32_t resetDynQueryCtrlOperState(SOperatorInfo* pOper) {
  SDynQueryCtrlOperatorInfo*    pDyn = pOper->info;
  SDynQueryCtrlPhysiNode const* pPhyciNode = pOper->pPhyNode;
  SExecTaskInfo*                pTaskInfo = pOper->pTaskInfo;

  pOper->status = OP_NOT_OPENED;

  switch (pDyn->qType) {
    case DYN_QTYPE_STB_HASH:{
      pDyn->stbJoin.execInfo = (SDynQueryCtrlExecInfo){0};
      SStbJoinDynCtrlInfo* pStbJoin = &pDyn->stbJoin;
      destroyStbJoinDynCtrlInfo(&pDyn->stbJoin);
      
      int32_t code = initSeqStbJoinTableHash(&pDyn->stbJoin.ctx.prev, pDyn->stbJoin.basic.batchFetch);
      if (TSDB_CODE_SUCCESS != code) {
        qError("initSeqStbJoinTableHash failed since %s", tstrerror(code));
        return code;
      }
      pStbJoin->ctx.prev.pListHead = NULL;
      pStbJoin->ctx.prev.joinBuild = false;
      pStbJoin->ctx.prev.pListTail = NULL;
      pStbJoin->ctx.prev.tableNum = 0;

      pStbJoin->ctx.post = (SStbJoinPostJoinCtx){0};
      break; 
    }
    case DYN_QTYPE_VTB_SCAN:
    case DYN_QTYPE_VTB_TS_SCAN:
    case DYN_QTYPE_VTB_AGG:
    case DYN_QTYPE_VTB_INTERVAL: {
      SVtbScanDynCtrlInfo* pVtbScan = &pDyn->vtbScan;
      
      if (pVtbScan->otbNameToOtbInfoMap) {
        taosHashSetFreeFp(pVtbScan->otbNameToOtbInfoMap, destroySOrgTbInfo);
        taosHashCleanup(pVtbScan->otbNameToOtbInfoMap);
        pVtbScan->otbNameToOtbInfoMap = NULL;
      }
      if (pVtbScan->pRsp) {
        tFreeSUsedbRsp(pVtbScan->pRsp);
        taosMemoryFreeClear(pVtbScan->pRsp);
      }
      if (pVtbScan->colRefInfo) {
        taosArrayDestroyEx(pVtbScan->colRefInfo, destroyColRefInfo);
        pVtbScan->colRefInfo = NULL;
      }
      if (pVtbScan->childTableMap) {
        taosHashCleanup(pVtbScan->childTableMap);
        pVtbScan->childTableMap = NULL;
      }
      if (pVtbScan->childTableList) {
        taosArrayClearEx(pVtbScan->childTableList, destroyColRefArray);
      }
      if (pVtbScan->resolvedColRefMap) {
        taosHashClear(pVtbScan->resolvedColRefMap);
      }
      if (pPhyciNode->dynTbname && pTaskInfo) {
        updateDynTbUidIfNeeded(pVtbScan, pTaskInfo->pStreamRuntimeInfo);
      }
      pVtbScan->curTableIdx = 0;
      pVtbScan->lastTableIdx = -1;
      break;
    }
    case DYN_QTYPE_VTB_WINDOW: {
      SVtbWindowDynCtrlInfo* pVtbWindow = &pDyn->vtbWindow;
      if (pVtbWindow->pRes) {
        blockDataDestroy(pVtbWindow->pRes);
        pVtbWindow->pRes = NULL;
      }
      if (pVtbWindow->pWins) {
        taosArrayDestroyEx(pVtbWindow->pWins, destroyWinArray);
        pVtbWindow->pWins = NULL;
      }
      pVtbWindow->outputWdurationSlotId = -1;
      pVtbWindow->outputWendSlotId = -1;
      pVtbWindow->outputWstartSlotId = -1;
      pVtbWindow->curWinBatchIdx = 0;
      break;
    }
    default:
      qError("unsupported dynamic query ctrl type: %d", pDyn->qType);
      break;
  }
  return 0;
}

int32_t virtualTableAggGetNext(SOperatorInfo* pOperator, SSDataBlock** pRes) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = (SVtbScanDynCtrlInfo*)&pInfo->vtbScan;
  SOperatorInfo*             pAggOp = pOperator->pDownstream[pOperator->numOfDownstream - 1];
  SOperatorInfo*             pTagScanOp = pOperator->pDownstream[1];
  SOperatorParam*            pAggParam = NULL;

  if (pInfo->vtbScan.hasPartition) {
    if (pInfo->vtbScan.batchProcessChild) {
      void* pIter = taosHashIterate(pVtbScan->vtbGroupIdTagListMap, NULL);
      while (pIter) {
        size_t     keyLen = 0;
        uint64_t   groupid = *(uint64_t*)taosHashGetKey(pIter, &keyLen);

        code = buildAggOperatorParamWithGroupId(pInfo, groupid, &pAggParam);
        QUERY_CHECK_CODE(code, line, _return);

        if (pAggParam) {
          code = pAggOp->fpSet.getNextExtFn(pAggOp, pAggParam, pRes);
          QUERY_CHECK_CODE(code, line, _return);
        } else {
          *pRes = NULL;
        }

        pIter = taosHashIterate(pVtbScan->vtbGroupIdTagListMap, pIter);

        if (*pRes) {
          (*pRes)->info.id.groupId = groupid;
          code = taosHashRemove(pVtbScan->vtbGroupIdTagListMap, &groupid, keyLen);
          QUERY_CHECK_CODE(code, line, _return);
          break;
        }
      }
    } else {
      void *pIter = taosHashIterate(pVtbScan->vtbGroupIdTagListMap, NULL);
      while (pIter) {
        size_t     keyLen = 0;
        uint64_t*  groupid = (uint64_t*)taosHashGetKey(pIter, &keyLen);
        SHashObj*  vtbUidTagListMap = *(SHashObj**)pIter;

        void* pIter2 = taosHashIterate(vtbUidTagListMap, NULL);
        while (pIter2) {
          size_t   keyLen2 = 0;
          tb_uid_t uid = *(tb_uid_t*)taosHashGetKey(pIter2, &keyLen2);
          SArray*  pTagList = *(SArray**)pIter2;

          if (pVtbScan->genNewParam) {
            code = buildAggOperatorParamForSingleChild(pInfo, uid, *groupid, pTagList, &pAggParam);
            QUERY_CHECK_CODE(code, line, _return);
            if (pAggParam) {
              code = pAggOp->fpSet.getNextExtFn(pAggOp, pAggParam, pRes);
              QUERY_CHECK_CODE(code, line, _return);
            } else {
              *pRes = NULL;
            }
          } else {
            code = pAggOp->fpSet.getNextFn(pAggOp, pRes);
            QUERY_CHECK_CODE(code, line, _return);
          }

          if (*pRes) {
            pVtbScan->genNewParam = false;
            (*pRes)->info.id.groupId = *groupid;
            break;
          }
          pVtbScan->genNewParam = true;
          pIter2 = taosHashIterate(vtbUidTagListMap, pIter2);
          code = taosHashRemove(vtbUidTagListMap, &uid, keyLen);
          QUERY_CHECK_CODE(code, line, _return);
        }
        if (*pRes) {
          break;
        }
        pIter = taosHashIterate(pVtbScan->vtbGroupIdTagListMap, pIter);
        code = taosHashRemove(pVtbScan->vtbGroupIdTagListMap, groupid, keyLen);
        QUERY_CHECK_CODE(code, line, _return);
      }
    }

  } else {
    if (pInfo->vtbScan.batchProcessChild) {
      code = buildAggOperatorParam(pInfo, &pAggParam);
      QUERY_CHECK_CODE(code, line, _return);

      code = pAggOp->fpSet.getNextExtFn(pAggOp, pAggParam, pRes);
      QUERY_CHECK_CODE(code, line, _return);
      setOperatorCompleted(pOperator);
    } else {
      void* pIter = taosHashIterate(pVtbScan->vtbUidTagListMap, NULL);
      while (pIter) {
        size_t   keyLen = 0;
        tb_uid_t uid = *(tb_uid_t*)taosHashGetKey(pIter, &keyLen);
        SArray*  pTagList = *(SArray**)pIter;

        if (pVtbScan->genNewParam) {
          code = buildAggOperatorParamForSingleChild(pInfo, uid, 0, pTagList, &pAggParam);
          QUERY_CHECK_CODE(code, line, _return);

          if (pAggParam) {
            code = pAggOp->fpSet.getNextExtFn(pAggOp, pAggParam, pRes);
            QUERY_CHECK_CODE(code, line, _return);
          } else {
            *pRes = NULL;
          }
        } else {
          code = pAggOp->fpSet.getNextFn(pAggOp, pRes);
          QUERY_CHECK_CODE(code, line, _return);
        }

        if (*pRes) {
          pVtbScan->genNewParam = false;
          break;
        }
        pVtbScan->genNewParam = true;
        pIter = taosHashIterate(pVtbScan->vtbUidTagListMap, pIter);
        code = taosHashRemove(pVtbScan->vtbUidTagListMap, &uid, keyLen);
        QUERY_CHECK_CODE(code, line, _return);
      }
    }
  }
_return:
  if (code) {
    qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
  }
  return code;
}

int32_t vtbAggNext(SOperatorInfo* pOperator, SSDataBlock** pRes) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = (SVtbScanDynCtrlInfo*)&pInfo->vtbScan;

  QRY_PARAM_CHECK(pRes);
  if (pOperator->status == OP_EXEC_DONE) {
    return code;
  }

  code = pOperator->fpSet._openFn(pOperator);
  QUERY_CHECK_CODE(code, line, _return);

  if (pVtbScan->isSuperTable && taosArrayGetSize(pVtbScan->childTableList) == 0) {
    setOperatorCompleted(pOperator);
    return code;
  }

  code = virtualTableAggGetNext(pOperator, pRes);
  QUERY_CHECK_CODE(code, line, _return);

  return code;

_return:
  if (code) {
    qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
    pOperator->pTaskInfo->code = code;
    T_LONG_JMP(pOperator->pTaskInfo->env, code);
  }
  return code;
}

/*
 * Build hash-interval operator params for interval dynamic query.
 *
 * @param pInfo Dynamic-query control operator runtime info.
 * @param ppRes Output interval operator param.
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise error code.
 */
static int32_t buildHashIntervalOperatorParam(SDynQueryCtrlOperatorInfo* pInfo, SOperatorParam** ppRes) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SOperatorParam*           pParam = NULL;
  SOperatorParam*           pExchangeParam = NULL;
  SVtbScanDynCtrlInfo*      pVtbScan = (SVtbScanDynCtrlInfo*)&pInfo->vtbScan;

  pParam = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(pParam, code, lino, _return, terrno)

  pParam->opType = QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL;
  pParam->downstreamIdx = 0;
  pParam->reUse = false;
  pParam->pChildren = taosArrayInit(1, POINTER_BYTES);
  QUERY_CHECK_NULL(pParam->pChildren, code, lino, _return, terrno)

  pParam->value = taosMemoryMalloc(sizeof(SAggOperatorParam));
  QUERY_CHECK_NULL(pParam->value, code, lino, _return, terrno)

  code = buildBatchExchangeOperatorParamForVirtual(
      &pExchangeParam, 0, NULL, 0, pVtbScan->otbVgIdToOtbInfoArrayMap,
      (STimeWindow){.skey = INT64_MAX, .ekey = INT64_MIN}, EX_SRC_TYPE_VSTB_INTERVAL_SCAN,
      QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN);
  QUERY_CHECK_CODE(code, lino, _return);

  QUERY_CHECK_NULL(taosArrayPush(pParam->pChildren, &pExchangeParam), code, lino, _return, terrno)
  pExchangeParam = NULL;

  *ppRes = pParam;

  return code;
_return:
  if (pExchangeParam) {
    freeOperatorParam(pExchangeParam, OP_GET_PARAM);
  }
  if (pParam) {
    freeOperatorParam(pParam, OP_GET_PARAM);
  }
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

/*
 * Build merge operator params for split-interval execution.
 *
 * @param pInfo Dynamic-query control operator runtime info.
 * @param pMergeOp Merge operator used by interval execution.
 * @param ppRes Output merge operator param.
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise error code.
 */
static int32_t buildSplitIntervalMergeOperatorParam(SDynQueryCtrlOperatorInfo* pInfo, SOperatorInfo* pMergeOp,
                                                    SOperatorParam** ppRes) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  SVtbScanDynCtrlInfo* pVtbScan = (SVtbScanDynCtrlInfo*)&pInfo->vtbScan;
  SOperatorParam*      pExchangeParam = NULL;
  *ppRes = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(*ppRes, code, lino, _return, terrno)

  (*ppRes)->opType = QUERY_NODE_PHYSICAL_PLAN_MERGE;
  (*ppRes)->downstreamIdx = 0;
  (*ppRes)->reUse = false;
  (*ppRes)->value = taosMemoryCalloc(1, sizeof(SMergeOperatorParam));
  QUERY_CHECK_NULL((*ppRes)->value, code, lino, _return, terrno)

  (*ppRes)->pChildren = taosArrayInit(pMergeOp->numOfDownstream, POINTER_BYTES);
  QUERY_CHECK_NULL((*ppRes)->pChildren, code, lino, _return, terrno)

  for (int32_t i = 0; i < pMergeOp->numOfDownstream; ++i) {
    code = buildBatchExchangeOperatorParamForVirtual(
        &pExchangeParam, i, NULL, 0, pVtbScan->otbVgIdToOtbInfoArrayMap,
        (STimeWindow){.skey = INT64_MAX, .ekey = INT64_MIN}, EX_SRC_TYPE_VSTB_PART_INTERVAL_SCAN,
        QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN);
    QUERY_CHECK_CODE(code, lino, _return);
    QUERY_CHECK_NULL(taosArrayPush((*ppRes)->pChildren, &pExchangeParam), code, lino, _return, terrno)
    pExchangeParam = NULL;
  }

  return code;

_return:
  if (pExchangeParam) {
    freeOperatorParam(pExchangeParam, OP_GET_PARAM);
  }
  freeOperatorParam(*ppRes, OP_GET_PARAM);
  *ppRes = NULL;
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

/*
 * Build split-interval root operator params with one merge child.
 *
 * @param pInfo Dynamic-query control operator runtime info.
 * @param pIntervalOp Interval operator whose downstream contains merge node.
 * @param ppRes Output split-interval operator param.
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise error code.
 */
static int32_t buildSplitIntervalOperatorParam(SDynQueryCtrlOperatorInfo* pInfo, SOperatorInfo* pIntervalOp,
                                               SOperatorParam** ppRes) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SOperatorInfo*  pMergeOp = NULL;
  SOperatorParam* pMergeParam = NULL;

  QUERY_CHECK_NULL(pIntervalOp->pDownstream, code, lino, _return, TSDB_CODE_INVALID_PARA)
  pMergeOp = pIntervalOp->pDownstream[0];
  QUERY_CHECK_NULL(pMergeOp, code, lino, _return, TSDB_CODE_INVALID_PARA)
  if (QUERY_NODE_PHYSICAL_PLAN_MERGE != pMergeOp->operatorType) {
    qError("%s invalid downstream operator type %d for split interval", __func__, pMergeOp->operatorType);
    return TSDB_CODE_INVALID_PARA;
  }

  *ppRes = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(*ppRes, code, lino, _return, terrno)

  (*ppRes)->opType = QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL;
  (*ppRes)->downstreamIdx = 0;
  (*ppRes)->reUse = false;
  (*ppRes)->value = taosMemoryCalloc(1, sizeof(SAggOperatorParam));
  QUERY_CHECK_NULL((*ppRes)->value, code, lino, _return, terrno)

  (*ppRes)->pChildren = taosArrayInit(1, POINTER_BYTES);
  QUERY_CHECK_NULL((*ppRes)->pChildren, code, lino, _return, terrno)

  code = buildSplitIntervalMergeOperatorParam(pInfo, pMergeOp, &pMergeParam);
  QUERY_CHECK_CODE(code, lino, _return);
  QUERY_CHECK_NULL(taosArrayPush((*ppRes)->pChildren, &pMergeParam), code, lino, _return, terrno)
  pMergeParam = NULL;

  return code;

_return:
  if (pMergeParam) {
    freeOperatorParam(pMergeParam, OP_GET_PARAM);
  }
  freeOperatorParam(*ppRes, OP_GET_PARAM);
  *ppRes = NULL;
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

/*
 * Dispatch interval-param builder by interval operator physical type.
 *
 * @param pInfo Dynamic-query control operator runtime info.
 * @param pIntervalOp Interval operator to build params for.
 * @param ppRes Output interval operator param.
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise error code.
 */
static int32_t buildIntervalOperatorParam(SDynQueryCtrlOperatorInfo* pInfo, SOperatorInfo* pIntervalOp,
                                          SOperatorParam** ppRes) {
  if (NULL == pIntervalOp) {
    return TSDB_CODE_INVALID_PARA;
  }

  switch (pIntervalOp->operatorType) {
    case QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL:
      return buildHashIntervalOperatorParam(pInfo, ppRes);
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL:
      return buildSplitIntervalOperatorParam(pInfo, pIntervalOp, ppRes);
    default:
      qError("%s unsupported interval operator type %d", __func__, pIntervalOp->operatorType);
      return TSDB_CODE_INVALID_PARA;
  }
}

/*
 * Get next result block for virtual-table interval workflow.
 *
 * @param pOperator Dynamic-query control operator.
 * @param pRes Output result data block pointer.
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise error code.
 */
int32_t vtbIntervalNext(SOperatorInfo* pOperator, SSDataBlock** pRes) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = (SVtbScanDynCtrlInfo*)&pInfo->vtbScan;
  SOperatorInfo*             pInterval = pOperator->pDownstream[pOperator->numOfDownstream - 1];
  SOperatorParam*            pIntervalParam = NULL;
  SSDataBlock*               pResult = NULL;

  QRY_PARAM_CHECK(pRes);
  if (pOperator->status == OP_EXEC_DONE) {
    return code;
  }

  code = pOperator->fpSet._openFn(pOperator);
  QUERY_CHECK_CODE(code, line, _return);

  if (pVtbScan->genNewParam) {
    qDebug("%s vtb interval split start, intervalOp:%s type:%d", GET_TASKID(pOperator->pTaskInfo), pInterval->name,
           pInterval->operatorType);
    code = buildIntervalOperatorParam(pInfo, pInterval, &pIntervalParam);
    QUERY_CHECK_CODE(code, line, _return);

    code = pInterval->fpSet.getNextExtFn(pInterval, pIntervalParam, &pResult);
    QUERY_CHECK_CODE(code, line, _return);

    pVtbScan->genNewParam = false;
  } else {
    code = pInterval->fpSet.getNextFn(pInterval, &pResult);
    QUERY_CHECK_CODE(code, line, _return);
  }

  if (pResult) {
    qDebug("%s vtb interval got result rows:%" PRId64 " status:%d", GET_TASKID(pOperator->pTaskInfo),
           pResult->info.rows, pInterval->status);
    *pRes = pResult;
  } else {
    qDebug("%s vtb interval got empty result, interval status:%d", GET_TASKID(pOperator->pTaskInfo), pInterval->status);
    *pRes = NULL;
    setOperatorCompleted(pOperator);
  }

  return code;

_return:
  if (code) {
    qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
    pOperator->pTaskInfo->code = code;
    T_LONG_JMP(pOperator->pTaskInfo->env, code);
  }
  return code;
}

int32_t createDynQueryCtrlOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream,
                                       SDynQueryCtrlPhysiNode* pPhyciNode, SExecTaskInfo* pTaskInfo,
                                       SMsgCb* pMsgCb, SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  __optr_fn_t                nextFp = NULL;
  __optr_open_fn_t           openFp = NULL;
  SOperatorInfo*             pOperator = NULL;
  SDynQueryCtrlOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SDynQueryCtrlOperatorInfo));
  QUERY_CHECK_NULL(pInfo, code, line, _error, terrno)

  pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  QUERY_CHECK_NULL(pOperator, code, line, _error, terrno)
  initOperatorCostInfo(pOperator);

  pOperator->pPhyNode = pPhyciNode;
  pTaskInfo->dynamicTask = (int8_t)pPhyciNode->node.dynamicOp;

  code = appendDownstream(pOperator, pDownstream, numOfDownstream);
  QUERY_CHECK_CODE(code, line, _error);

  setOperatorInfo(pOperator, "DynQueryCtrlOperator", QUERY_NODE_PHYSICAL_PLAN_DYN_QUERY_CTRL, false, OP_NOT_OPENED,
                  pInfo, pTaskInfo);

  pInfo->qType = pPhyciNode->qType;
  switch (pInfo->qType) {
    case DYN_QTYPE_STB_HASH:
      TAOS_MEMCPY(&pInfo->stbJoin.basic, &pPhyciNode->stbJoin, sizeof(pPhyciNode->stbJoin));
      pInfo->stbJoin.pOutputDataBlockDesc = pPhyciNode->node.pOutputDataBlockDesc;
      code = initSeqStbJoinTableHash(&pInfo->stbJoin.ctx.prev, pInfo->stbJoin.basic.batchFetch);
      QUERY_CHECK_CODE(code, line, _error);
      nextFp = seqStableJoin;
      openFp = optrDummyOpenFn;
      break;
    case DYN_QTYPE_VTB_SCAN:
      code = initVtbScanInfo(pInfo, pMsgCb, pPhyciNode, pTaskInfo);
      QUERY_CHECK_CODE(code, line, _error);
      nextFp = vtbScanNext;
      openFp = vtbScanOpen;
      break;
    case DYN_QTYPE_VTB_TS_SCAN:
      code = initVtbScanInfo(pInfo, pMsgCb, pPhyciNode, pTaskInfo);
      QUERY_CHECK_CODE(code, line, _error);
      nextFp = vtbTsScanNext;
      openFp = vtbDefaultOpen;
      break;
    case DYN_QTYPE_VTB_WINDOW:
      code = initVtbScanInfo(pInfo, pMsgCb, pPhyciNode, pTaskInfo);
      QUERY_CHECK_CODE(code, line, _error);
      code = initVtbWindowInfo(pInfo, pPhyciNode, pTaskInfo, pOperator);
      QUERY_CHECK_CODE(code, line, _error);
      nextFp = vtbWindowNext;
      openFp = vtbWindowOpen;
      break;
    case DYN_QTYPE_VTB_AGG:
      code = initVtbScanInfo(pInfo, pMsgCb, pPhyciNode, pTaskInfo);
      QUERY_CHECK_CODE(code, line, _error);
      nextFp = vtbAggNext;
      openFp = vtbDefaultOpen;
      break;
    case DYN_QTYPE_VTB_INTERVAL:
      code = initVtbScanInfo(pInfo, pMsgCb, pPhyciNode, pTaskInfo);
      QUERY_CHECK_CODE(code, line, _error);
      nextFp = vtbIntervalNext;
      openFp = vtbDefaultOpen;
      break;
    default:
      qError("unsupported dynamic query ctrl type: %d", pInfo->qType);
      code = TSDB_CODE_INVALID_PARA;
      goto _error;
  }

  pOperator->fpSet = createOperatorFpSet(openFp, nextFp, NULL, destroyDynQueryCtrlOperator, optrDefaultBufFn,
                                         NULL, optrDefaultGetNextExtFn, NULL);

  setOperatorResetStateFn(pOperator, resetDynQueryCtrlOperState);
  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  if (pInfo != NULL) {
    destroyDynQueryCtrlOperator(pInfo);
  }
  qError("failed to create dyn query ctrl operator, %s code:%s, line:%d", __func__, tstrerror(code), line);
  destroyOperatorAndDownstreams(pOperator, pDownstream, numOfDownstream);
  pTaskInfo->code = code;
  return code;
}
