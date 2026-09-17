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
#include "extConnector.h"
#include "extTypeMap.h"
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
#include "ttime.h"

#define DYN_COL_REF_NAME_LEN (TSDB_EXT_SOURCE_NAME_LEN + TSDB_DB_NAME_LEN + TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN + 4)

typedef struct SDynResolvedColRef {
  char    sourceName[TSDB_EXT_SOURCE_NAME_LEN];
  char    dbName[TSDB_DB_NAME_LEN];
  char    tbName[TSDB_TABLE_NAME_LEN];
  char    colName[TSDB_COL_NAME_LEN];
  char    fullColRef[DYN_COL_REF_NAME_LEN];
  int32_t  vgId;
  int8_t   extRefType;
  tb_uid_t leafUid;
  int32_t  leafVgId;
  col_id_t leafColId;
} SDynResolvedColRef;

typedef struct SDynRefKey {
  char colRef[DYN_COL_REF_NAME_LEN];
} SDynRefKey;

typedef struct SDynRefEdge {
  SDynRefKey nextRef;
  tb_uid_t   uid;
  int32_t    vgId;
  col_id_t   colId;
  bool       isNullRef;
} SDynRefEdge;

typedef struct SDynLeafVtbKey {
  tb_uid_t uid;
  int32_t  vgId;
  int32_t  reserved;
} SDynLeafVtbKey;

typedef struct SDynLeafCondTarget {
  SColRefInfo* pColRef;
  col_id_t     leafColId;
} SDynLeafCondTarget;

typedef struct SColRefNameView {
  const char* dbName;     // start of database-name segment in original ref string
  const char* tbName;     // start of table-name segment in original ref string
  const char* colName;    // start of column-name segment in original ref string
  size_t      dbNameLen;  // database-name bytes, excluding terminator
  size_t      tbNameLen;  // table-name bytes, excluding terminator
  size_t      colNameLen; // column-name bytes, excluding terminator
} SColRefNameView;

// Column indices for ins_virtual_child_columns (userVctbColsSchema in systable.c)
enum {
  VC_COL_TABLE_NAME  = 0,
  VC_COL_STABLE_NAME = 1,
  VC_COL_DB_NAME     = 2,
  VC_COL_COL_NAME    = 3,
  VC_COL_UID         = 4,
  VC_COL_COL_ID      = 5,
  VC_COL_COL_SOURCE  = 6,
  VC_COL_VGROUP_ID   = 7,
  VC_COL_REF_VERSION = 8,
  VC_COL_COL_TYPE    = 9,
  VC_COL_COUNT       = 10
};

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
static int32_t getDbVgInfo(SOperatorInfo* pOperator, SName* name, SDBVgInfo** dbVgInfo);
static int32_t getVgId(SDBVgInfo* dbInfo, char* dbFName, int32_t* vgId, char* tbName);
static int32_t parseColRefNameView(const char* colRef, SColRefNameView* pView);
static bool    dynIsExternalColRefName(const char* colRef);
static int32_t getVgIdFromColref(SOperatorInfo* pOperator, const char* colRef, int32_t* vgId);
static int32_t dynReplaceCondJson(char** ppDst, int32_t* pDstLen, const char* pSrc, int32_t srcLen);
int32_t extractExtColRefName(const char* colref, char** refSource, char** refDb, char** refTb, char** refCol);

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
    taosMemoryFree(pColRefInfo->tagCondJson);
    taosMemoryFree(pColRefInfo->filterCondJson);
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

static void destroyForeignSourceInfoInExecutor(void *info) {
  SForeignSourceInfo *pFsInfo = (SForeignSourceInfo *)info;
  if (pFsInfo != NULL) {
    nodesDestroyNode(pFsInfo->pPushedCond);
    pFsInfo->pPushedCond = NULL;
  }
  destroySForeignSourceInfo(info);
}

void destroyVtbUidTagListMap(void *info) {
  SHashObj* pVtbUidTagListMap = *(SHashObj **)info;
  if (pVtbUidTagListMap) {
    taosHashSetFreeFp(pVtbUidTagListMap, destroyTagList);
    taosHashCleanup(pVtbUidTagListMap);
  }
}

static void destroyVtbScanDynCtrlInfo(SVtbScanDynCtrlInfo* pVtbScan) {
  taosHashCleanup(pVtbScan->extTableMetaCache);
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
  taosArrayDestroy(pVtbScan->aggTagColIds);
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
  if (pVtbScan->foreignSourceMap) {
    taosHashSetFreeFp(pVtbScan->foreignSourceMap, destroyForeignSourceInfoInExecutor);
    taosHashCleanup(pVtbScan->foreignSourceMap);
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
    taosHashSetFreeFp(pVtbScan->vtbGroupIdTagListMap, destroyVtbUidTagListMap);
    taosHashCleanup(pVtbScan->vtbGroupIdTagListMap);
  }
  if (pVtbScan->vtbUidToGroupIdMap) {
    taosHashCleanup(pVtbScan->vtbUidToGroupIdMap);
  }
  if (pVtbScan->pExcludedSourceNames) {
    taosHashCleanup(pVtbScan->pExcludedSourceNames);
  }
  if (pVtbScan->tableCfgCache) {
    taosHashCleanup(pVtbScan->tableCfgCache);
  }
  if (pVtbScan->tagRefFilterColName) {
    taosMemoryFreeClear(pVtbScan->tagRefFilterColName);
  }
  if (pVtbScan->pTagRefFilterCond) {
    nodesDestroyNode(pVtbScan->pTagRefFilterCond);
    pVtbScan->pTagRefFilterCond = NULL;
  }
  if (pVtbScan->pTagRefFilterColInfos) {
    taosArrayDestroy(pVtbScan->pTagRefFilterColInfos);
    pVtbScan->pTagRefFilterColInfos = NULL;
  }
  if (pVtbScan->vtbScanParam) {
    freeOperatorParam(pVtbScan->vtbScanParam, OP_GET_PARAM);
    pVtbScan->vtbScanParam = NULL;
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
  pTbInfo->srcPrecision = pSrc->srcPrecision;
  pTbInfo->dstPrecision = pSrc->dstPrecision;

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

static int32_t cloneTagList(SArray* pTagList, SArray** ppCloned) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  STagVal  tmpTag = {0};

  *ppCloned = taosArrayInit(1, sizeof(STagVal));
  QUERY_CHECK_NULL(*ppCloned, code, lino, _return, terrno)

  for (int32_t i = 0; i < taosArrayGetSize(pTagList); ++i) {
    STagVal* pSrcTag = (STagVal*)taosArrayGet(pTagList, i);
    QUERY_CHECK_NULL(pSrcTag, code, lino, _return, terrno)
    tmpTag.type = pSrcTag->type;
    tmpTag.cid = pSrcTag->cid;
    if (IS_VAR_DATA_TYPE(pSrcTag->type)) {
      tmpTag.nData = pSrcTag->nData;
      if (NULL != pSrcTag->pData) {
        tmpTag.pData = taosMemoryMalloc(TMAX(1, tmpTag.nData));
        QUERY_CHECK_NULL(tmpTag.pData, code, lino, _return, terrno)
        if (tmpTag.nData > 0) {
          memcpy(tmpTag.pData, pSrcTag->pData, tmpTag.nData);
        }
      } else {
        tmpTag.pData = NULL;
      }
    } else {
      tmpTag.i64 = pSrcTag->i64;
    }

    QUERY_CHECK_NULL(taosArrayPush(*ppCloned, &tmpTag), code, lino, _return, terrno)
    tmpTag = (STagVal){0};
  }

  return code;
_return:
  if (*ppCloned) {
    taosArrayDestroyEx(*ppCloned, destroyTagVal);
    *ppCloned = NULL;
  }
  if (tmpTag.pData) {
    taosMemoryFree(tmpTag.pData);
  }
  qError("%s failed at line: %d, code: %d", __func__, lino, code);
  return code;
}

int32_t appendResolvedTagVal(SArray* pResolvedTags, col_id_t dstColId, const SSchema* pSchema, const STag* pTagData) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  STagVal srcVal = {.cid = pSchema->colId};
  STagVal dstVal = {.cid = dstColId, .type = pSchema->type};

  // JSON tag: the value is the whole STag blob (same convention as metaGetTableTagVal), so it
  // cannot be looked up with tTagGet  -  that binary-searches a JSON blob by key and would
  // dereference the unset srcVal.pKey. Copy the blob outright; a JSON-null blob means NULL.
  if (pSchema->type == TSDB_DATA_TYPE_JSON) {
    if (pTagData == NULL || !tTagIsJson(pTagData) || tTagIsJsonNull((void*)pTagData)) {
      return TSDB_CODE_SUCCESS;  // no JSON value: leave the tag NULL
    }
    dstVal.nData = pTagData->len;
    dstVal.pData = taosMemoryMalloc(dstVal.nData);
    QUERY_CHECK_NULL(dstVal.pData, code, lino, _return, terrno)
    memcpy(dstVal.pData, pTagData, dstVal.nData);
  } else {
    if (!tTagGet(pTagData, &srcVal)) {
      return TSDB_CODE_SUCCESS;
    }

    if (IS_VAR_DATA_TYPE(pSchema->type)) {
      if (srcVal.pData != NULL && srcVal.nData > 0) {
        dstVal.nData = srcVal.nData;
        dstVal.pData = taosMemoryMalloc(dstVal.nData);
        QUERY_CHECK_NULL(dstVal.pData, code, lino, _return, terrno)
        memcpy(dstVal.pData, srcVal.pData, dstVal.nData);
      }
    } else {
      dstVal.i64 = srcVal.i64;
    }
  }

  // Replace existing entry with same cid if present
  for (int32_t i = 0; i < taosArrayGetSize(pResolvedTags); ++i) {
    STagVal* pExisting = taosArrayGet(pResolvedTags, i);
    if (pExisting->cid == dstColId) {
      if (IS_VAR_DATA_TYPE(pExisting->type)) {
        taosMemoryFree(pExisting->pData);
      }
      *pExisting = dstVal;
      return TSDB_CODE_SUCCESS;
    }
  }

  QUERY_CHECK_NULL(taosArrayPush(pResolvedTags, &dstVal), code, lino, _return, terrno)
  return TSDB_CODE_SUCCESS;

_return:
  if (dstVal.pData != NULL) {
    taosMemoryFreeClear(dstVal.pData);
  }
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
    batchInfo.srcPrecision = pSrc->srcPrecision;
    batchInfo.dstPrecision = pSrc->dstPrecision;
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
    code = cloneTagList(pTagList, &pBasic->tagList);
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

static int32_t buildFederatedScanOperatorParamForExternalWindow(SOperatorParam** ppRes, STimeWindow win) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SOperatorParam* pParam = NULL;

  pParam = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(pParam, code, lino, _return, terrno)

  pParam->value = taosMemoryCalloc(1, sizeof(SDynQueryCtrlOperatorParam));
  QUERY_CHECK_NULL(pParam->value, code, lino, _return, terrno)

  pParam->opType = QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN;
  pParam->pChildren = taosArrayInit(0, POINTER_BYTES);
  QUERY_CHECK_NULL(pParam->pChildren, code, lino, _return, terrno)
  ((SDynQueryCtrlOperatorParam*)pParam->value)->window = win;

  *ppRes = pParam;
  return code;

_return:
  qError("failed to build federated scan operator param for external window, code:%d", code);
  if (pParam) {
    taosArrayDestroy(pParam->pChildren);
    taosMemoryFree(pParam->value);
    taosMemoryFree(pParam);
  }
  return code;
}

static int32_t buildDynQueryCtrlOperatorParamForExternalWindow(SOperatorParam** ppRes, STimeWindow win) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SOperatorParam* pParam = NULL;

  pParam = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(pParam, code, lino, _return, terrno)

  pParam->value = taosMemoryCalloc(1, sizeof(SDynQueryCtrlOperatorParam));
  QUERY_CHECK_NULL(pParam->value, code, lino, _return, terrno)

  pParam->opType = QUERY_NODE_PHYSICAL_PLAN_DYN_QUERY_CTRL;
  pParam->downstreamIdx = 0;
  pParam->pChildren = taosArrayInit(0, POINTER_BYTES);
  QUERY_CHECK_NULL(pParam->pChildren, code, lino, _return, terrno)
  ((SDynQueryCtrlOperatorParam*)pParam->value)->window = win;

  *ppRes = pParam;
  return code;

_return:
  qError("failed to build dynamic query control param for external window, code:%d", code);
  if (pParam) {
    taosArrayDestroy(pParam->pChildren);
    taosMemoryFree(pParam->value);
    taosMemoryFree(pParam);
  }
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

static int32_t buildFederatedScanOperatorParam(SOperatorParam** ppRes, int32_t downstreamIdx,
                                               SForeignSourceInfo* pForeignInfo, int8_t dstPrecision) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  SOperatorParam* pParam = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(pParam, code, lino, _return, terrno);

  pParam->opType = QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN;
  pParam->downstreamIdx = downstreamIdx;
  pParam->reUse = false;
  pParam->pChildren = NULL;

  SForeignScanOperatorParam* pFsParam = taosMemoryCalloc(1, sizeof(SForeignScanOperatorParam));
  QUERY_CHECK_NULL(pFsParam, code, lino, _return, terrno);
  pParam->value = pFsParam;
  pFsParam->paramType = FOREIGN_SCAN_PARAM_SINGLE;

  tstrncpy(pFsParam->sourceName, pForeignInfo->sourceName, sizeof(pFsParam->sourceName));
  tstrncpy(pFsParam->dbName, pForeignInfo->dbName, sizeof(pFsParam->dbName));
  tstrncpy(pFsParam->tableName, pForeignInfo->tableName, sizeof(pFsParam->tableName));
  pFsParam->colMap = taosArrayDup(pForeignInfo->colMap, NULL);
  QUERY_CHECK_NULL(pFsParam->colMap, code, lino, _return, terrno);
  pFsParam->rowLimit = pForeignInfo->rowLimit;

  if (pForeignInfo->tagCond != NULL && pForeignInfo->tagCondLen > 0) {
    pFsParam->tagCond = taosStrdup(pForeignInfo->tagCond);
    QUERY_CHECK_NULL(pFsParam->tagCond, code, lino, _return, terrno);
    pFsParam->tagCondLen = pForeignInfo->tagCondLen;
  }
  pFsParam->scanParamId = pForeignInfo->scanParamId;
  code = dynReplaceCondJson(&pFsParam->filterCondJson, &pFsParam->filterCondLen,
                            pForeignInfo->filterCondJson, pForeignInfo->filterCondLen);
  QUERY_CHECK_CODE(code, lino, _return);

  if (pForeignInfo->pPushedCond != NULL) {
    code = nodesCloneNode(pForeignInfo->pPushedCond, &pFsParam->pPushedCond);
    QUERY_CHECK_CODE(code, lino, _return);
  }

  pFsParam->dstPrecision = dstPrecision;

  *ppRes = pParam;
  return code;

_return:
  qError("failed to build federated scan operator param, code:%d", code);
  if (pParam) {
    SForeignScanOperatorParam* pFs = (SForeignScanOperatorParam*)pParam->value;
    if (pFs) {
      taosArrayDestroy(pFs->colMap);
      taosMemoryFree(pFs->tagCond);
      taosMemoryFree(pFs->filterCondJson);
      nodesDestroyNode(pFs->pPushedCond);
    }
    taosMemoryFree(pParam->value);
    taosMemoryFree(pParam);
  }
  return code;
}

static void destroyForeignTableInfo(void* pData) {
  SForeignTableInfo* pInfo = pData;
  if (pInfo == NULL) return;
  taosArrayDestroy(pInfo->colMap);
  taosMemoryFreeClear(pInfo->tagCond);
  taosMemoryFreeClear(pInfo->filterCondJson);
  taosMemoryFreeClear(pInfo->pPushedCondJson);
  nodesDestroyNode(pInfo->pPushedCond);
  taosArrayDestroyEx(pInfo->pVtbTagList, destroyTagVal);
}

static bool colIdArrayContains(const SArray* pColIds, col_id_t colId) {
  for (int32_t i = 0; pColIds != NULL && i < taosArrayGetSize(pColIds); ++i) {
    col_id_t* pRequired = taosArrayGet(pColIds, i);
    if (pRequired != NULL && *pRequired == colId) {
      return true;
    }
  }
  return false;
}

static bool colMapIntersectsRequiredColumns(const SArray* pColMap, const SArray* pRequiredColIds) {
  if (pRequiredColIds == NULL) {
    return true;
  }
  // The primary timestamp is implicit in every foreign-table mapping.  A
  // timestamp-only virtual scan (for example count(*) or session-boundary
  // generation) must therefore keep every remote table even though colMap
  // contains only mapped value columns.
  if (colIdArrayContains(pRequiredColIds, PRIMARYKEY_TIMESTAMP_COL_ID)) {
    return true;
  }
  for (int32_t i = 0; pColMap != NULL && i < taosArrayGetSize(pColMap); ++i) {
    SColIdNameKV* pCol = taosArrayGet(pColMap, i);
    if (pCol != NULL && colIdArrayContains(pRequiredColIds, pCol->colId)) {
      return true;
    }
  }
  return false;
}

static SArray* cloneRequiredForeignColMap(const SArray* pColMap, const SArray* pRequiredColIds) {
  if (pRequiredColIds == NULL) {
    return taosArrayDup(pColMap, NULL);
  }

  SArray* pResult = taosArrayInit(TMAX(1, taosArrayGetSize(pRequiredColIds)), sizeof(SColIdNameKV));
  if (pResult == NULL) {
    return NULL;
  }
  for (int32_t i = 0; pColMap != NULL && i < taosArrayGetSize(pColMap); ++i) {
    SColIdNameKV* pCol = taosArrayGet(pColMap, i);
    if (pCol != NULL && colIdArrayContains(pRequiredColIds, pCol->colId) && taosArrayPush(pResult, pCol) == NULL) {
      taosArrayDestroy(pResult);
      return NULL;
    }
  }
  return pResult;
}

static int32_t appendForeignTableInfo(SArray* pTables, const SForeignSourceInfo* pSource, int8_t dstPrecision,
                                      bool timestampOnly, const SArray* pRequiredColIds,
                                      SArray* pTagList) {
  int32_t           code = TSDB_CODE_SUCCESS;
  int32_t           lino = 0;
  SForeignTableInfo info = {0};

  tstrncpy(info.dbName, pSource->dbName, sizeof(info.dbName));
  tstrncpy(info.tableName, pSource->tableName, sizeof(info.tableName));
  info.colMap = timestampOnly ? taosArrayInit(1, sizeof(SColIdNameKV))
                              : cloneRequiredForeignColMap(pSource->colMap, pRequiredColIds);
  QUERY_CHECK_NULL(info.colMap, code, lino, _return, terrno);
  info.dstPrecision = dstPrecision;
  info.rowLimit = pSource->rowLimit;
  info.virtualChildUid = pSource->virtualChildUid;
  info.scanParamId = pSource->scanParamId;
  code = dynReplaceCondJson(&info.filterCondJson, &info.filterCondLen,
                            pSource->filterCondJson, pSource->filterCondLen);
  QUERY_CHECK_CODE(code, lino, _return);
  if (NULL != pTagList) {
    code = cloneTagList(pTagList, &info.pVtbTagList);
    QUERY_CHECK_CODE(code, lino, _return);
  }
  if (pSource->tagCond != NULL && pSource->tagCondLen > 0) {
    info.tagCond = taosStrdup(pSource->tagCond);
    QUERY_CHECK_NULL(info.tagCond, code, lino, _return, terrno)
    info.tagCondLen = pSource->tagCondLen;
  }
  if (pSource->pPushedCond != NULL) {
    code = nodesNodeToString(pSource->pPushedCond, false, &info.pPushedCondJson, NULL);
    QUERY_CHECK_CODE(code, lino, _return);
  }
  QUERY_CHECK_NULL(taosArrayPush(pTables, &info), code, lino, _return, terrno)
  return code;
_return:
  destroyForeignTableInfo(&info);
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

static int32_t compareForeignTableInfoByVirtualChildUid(const void* pLeft,
                                                        const void* pRight) {
  const SForeignTableInfo* pLeftInfo = pLeft;
  const SForeignTableInfo* pRightInfo = pRight;
  if (pLeftInfo->virtualChildUid < pRightInfo->virtualChildUid) {
    return -1;
  }
  if (pLeftInfo->virtualChildUid > pRightInfo->virtualChildUid) {
    return 1;
  }
  if (pLeftInfo->scanParamId < pRightInfo->scanParamId) {
    return -1;
  }
  return pLeftInfo->scanParamId > pRightInfo->scanParamId;
}

static int32_t createBatchFederatedScanOperatorParam(SOperatorParam** ppRes, int32_t downstreamIdx,
                                                     int32_t tableCapacity, const char* pSourceName,
                                                     int8_t dstPrecision, int32_t groupId,
                                                     STimeWindow window) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    lino = 0;
  SOperatorParam*            pParam = NULL;
  SForeignScanOperatorParam* pBatch = NULL;

  QUERY_CHECK_NULL(ppRes, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pSourceName, code, lino, _return, TSDB_CODE_INVALID_PARA);
  *ppRes = NULL;
  pParam = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(pParam, code, lino, _return, terrno);
  pBatch = taosMemoryCalloc(1, sizeof(SForeignScanOperatorParam));
  QUERY_CHECK_NULL(pBatch, code, lino, _return, terrno);

  pBatch->paramType = FOREIGN_SCAN_PARAM_BATCH;
  pBatch->dstPrecision = dstPrecision;
  pBatch->groupId = groupId;
  pBatch->window = window;
  pBatch->isNewParam = true;
  tstrncpy(pBatch->sourceName, pSourceName, sizeof(pBatch->sourceName));
  pBatch->pBatchTables = taosArrayInit(TMAX(1, tableCapacity), sizeof(SForeignTableInfo));
  QUERY_CHECK_NULL(pBatch->pBatchTables, code, lino, _return, terrno);

  pParam->opType = QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN;
  pParam->downstreamIdx = downstreamIdx;
  pParam->value = pBatch;
  *ppRes = pParam;
  return code;

_return:
  if (NULL != pBatch) {
    taosArrayDestroyEx(pBatch->pBatchTables, destroyForeignTableInfo);
    taosMemoryFree(pBatch);
  }
  taosMemoryFree(pParam);
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

// Build the only batch shape currently proven safe: every foreign table belongs
// to one registered external source and is consumed by one FederatedScan.
static int32_t buildBatchFederatedScanOperatorParamForVirtual(
    SOperatorParam** ppRes, int32_t downstreamIdx, SHashObj* pForeignSourceMap,
    const char* pSourceName, int8_t dstPrecision, int32_t groupId,
    STimeWindow window, bool timestampOnly, const SArray* pRequiredColIds) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  *ppRes = NULL;
  QUERY_CHECK_NULL(pForeignSourceMap, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pSourceName, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(0 < taosHashGetSize(pForeignSourceMap) && '\0' != pSourceName[0],
                        code, lino, _return, TSDB_CODE_INVALID_PARA);

  code = createBatchFederatedScanOperatorParam(ppRes, downstreamIdx,
                                               taosHashGetSize(pForeignSourceMap), pSourceName,
                                               dstPrecision, groupId, window);
  QUERY_CHECK_CODE(code, lino, _return);
  SForeignScanOperatorParam* pBatch = (*ppRes)->value;

  void* pIter = taosHashIterate(pForeignSourceMap, NULL);
  while (NULL != pIter) {
    SForeignSourceInfo* pSource = pIter;
    if (0 == strcmp(pSourceName, pSource->sourceName) &&
        (timestampOnly || colMapIntersectsRequiredColumns(pSource->colMap, pRequiredColIds))) {
      code = appendForeignTableInfo(pBatch->pBatchTables, pSource, dstPrecision, timestampOnly,
                                    pRequiredColIds, NULL);
      QUERY_CHECK_CODE(code, lino, _return);
    }
    pIter = taosHashIterate(pForeignSourceMap, pIter);
  }

  QUERY_CHECK_CONDITION(0 < taosArrayGetSize(pBatch->pBatchTables), code, lino, _return,
                        TSDB_CODE_INVALID_PARA);
  return code;

_return:
  freeOperatorParam(*ppRes, OP_GET_PARAM);
  *ppRes = NULL;
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

static int32_t buildScopedBatchFederatedScanOperatorParamForVirtual(
    SOperatorParam** ppRes, int32_t downstreamIdx, SVtbScanDynCtrlInfo* pVtbScan,
    const char* pSourceName, const SVtbAggDispatchScope* pScope) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  *ppRes = NULL;
  QUERY_CHECK_NULL(pVtbScan, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pScope, code, lino, _return, TSDB_CODE_INVALID_PARA);
  SHashObj* pForeignSourceMap = pVtbScan->foreignSourceMap;
  if (NULL == pForeignSourceMap || 0 == taosHashGetSize(pForeignSourceMap) ||
      NULL == pSourceName || '\0' == pSourceName[0]) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t legacyGroupId = (pScope->hasGroup && pScope->groupId <= INT32_MAX) ? (int32_t)pScope->groupId : 0;
  code = createBatchFederatedScanOperatorParam(ppRes, downstreamIdx, taosHashGetSize(pForeignSourceMap), pSourceName,
                                               pVtbScan->dstPrecision, legacyGroupId,
                                               (STimeWindow){.skey = INT64_MAX, .ekey = INT64_MIN});
  QUERY_CHECK_CODE(code, lino, _return);
  SForeignScanOperatorParam* pBatch = (*ppRes)->value;
  if (pScope->hasGroup) {
    pBatch->hasVtbGroupId = true;
    pBatch->vtbGroupId = pScope->groupId;
  }

  void* pIter = taosHashIterate(pForeignSourceMap, NULL);
  while (NULL != pIter) {
    SForeignSourceInfo* pSource = pIter;
    bool scopeMatches = !pScope->hasUid || pSource->virtualChildUid == pScope->uid;
    if (scopeMatches && pScope->hasGroup) {
      uint64_t* pGroupId =
          NULL != pVtbScan->vtbUidToGroupIdMap
              ? taosHashGet(pVtbScan->vtbUidToGroupIdMap, &pSource->virtualChildUid, sizeof(pSource->virtualChildUid))
              : NULL;
      scopeMatches = (NULL != pGroupId && *pGroupId == pScope->groupId);
    }
    if (scopeMatches && 0 == strcmp(pSourceName, pSource->sourceName) &&
        colMapIntersectsRequiredColumns(pSource->colMap, pVtbScan->readColList)) {
      SArray* pTagList = pScope->hasUid ? pScope->pTagList : NULL;
      code = appendForeignTableInfo(pBatch->pBatchTables, pSource, pVtbScan->dstPrecision, false,
                                    pVtbScan->readColList, pTagList);
      QUERY_CHECK_CODE(code, lino, _return);
    }
    pIter = taosHashIterate(pForeignSourceMap, pIter);
  }

  taosArraySort(pBatch->pBatchTables, compareForeignTableInfoByVirtualChildUid);

  if (0 == taosArrayGetSize(pBatch->pBatchTables)) {
    freeOperatorParam(*ppRes, OP_GET_PARAM);
    *ppRes = NULL;
  }
  return code;

_return:
  freeOperatorParam(*ppRes, OP_GET_PARAM);
  *ppRes = NULL;
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

static void* vtbAggFindMaxUidValue(SHashObj* pUidMap, tb_uid_t* pUid) {
  void* pMaxValue = NULL;
  void* pIter = taosHashIterate(pUidMap, NULL);
  while (NULL != pIter) {
    size_t    keyLen = 0;
    tb_uid_t* pCurrentUid = taosHashGetKey(pIter, &keyLen);
    if (NULL != pCurrentUid && sizeof(*pCurrentUid) == keyLen &&
        (NULL == pMaxValue || *pCurrentUid > *pUid)) {
      *pUid = *pCurrentUid;
      pMaxValue = pIter;
    }
    pIter = taosHashIterate(pUidMap, pIter);
  }
  return pMaxValue;
}

static bool foreignSourceMapHasSource(SHashObj* pForeignSourceMap, const char* pSourceName) {
  if (NULL == pForeignSourceMap || NULL == pSourceName || '\0' == pSourceName[0]) {
    return false;
  }
  void* pIter = taosHashIterate(pForeignSourceMap, NULL);
  while (NULL != pIter) {
    if (0 == strcmp(((SForeignSourceInfo*)pIter)->sourceName, pSourceName)) {
      return true;
    }
    pIter = taosHashIterate(pForeignSourceMap, pIter);
  }
  return false;
}

static bool colMapHasVirtualColumn(const SArray* pColMap, col_id_t colId) {
  for (int32_t i = 0; NULL != pColMap && i < taosArrayGetSize(pColMap); ++i) {
    SColIdNameKV* pCol = taosArrayGet(pColMap, i);
    if (NULL != pCol && pCol->colId == colId) {
      return true;
    }
  }
  return false;
}

static bool foreignSourceMapHasColumn(SHashObj* pForeignSourceMap, const char* pSourceName, col_id_t colId) {
  void* pIter = taosHashIterate(pForeignSourceMap, NULL);
  while (NULL != pIter) {
    SForeignSourceInfo* pInfo = pIter;
    if (0 == strcmp(pInfo->sourceName, pSourceName) && colMapHasVirtualColumn(pInfo->colMap, colId)) {
      taosHashCancelIterate(pForeignSourceMap, pIter);
      return true;
    }
    pIter = taosHashIterate(pForeignSourceMap, pIter);
  }
  return false;
}

static bool localSourceMapHasColumn(SHashObj* pVgMap, col_id_t colId) {
  void* pIter = taosHashIterate(pVgMap, NULL);
  while (NULL != pIter) {
    SArray* pTables = *(SArray**)pIter;
    for (int32_t i = 0; NULL != pTables && i < taosArrayGetSize(pTables); ++i) {
      SOrgTbInfo* pInfo = taosArrayGet(pTables, i);
      if (NULL != pInfo && colMapHasVirtualColumn(pInfo->colMap, colId)) {
        taosHashCancelIterate(pVgMap, pIter);
        return true;
      }
    }
    pIter = taosHashIterate(pVgMap, pIter);
  }
  return false;
}

static SOperatorInfo* findFederatedScanInUnaryBranch(SOperatorInfo* pOperator) {
  while (pOperator != NULL) {
    if (QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN == pOperator->operatorType) {
      return pOperator;
    }
    if (1 != pOperator->numOfDownstream) {
      return NULL;
    }
    pOperator = pOperator->pDownstream[0];
  }
  return NULL;
}

typedef struct {
  SArray* pColIds;  // SArray<col_id_t>
  int32_t code;
} SVstbFuncColumnCollectCtx;

static EDealRes collectVstbFuncColumn(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN != nodeType(pNode)) {
    return DEAL_RES_CONTINUE;
  }

  SColumnNode* pCol = (SColumnNode*)pNode;
  if (pCol->isPrimTs || PRIMARYKEY_TIMESTAMP_COL_ID == pCol->colId || COLUMN_TYPE_COLUMN != pCol->colType) {
    return DEAL_RES_CONTINUE;
  }

  SVstbFuncColumnCollectCtx* pCtx = pContext;
  if (!colIdArrayContains(pCtx->pColIds, pCol->colId) && taosArrayPush(pCtx->pColIds, &pCol->colId) == NULL) {
    pCtx->code = terrno;
    return DEAL_RES_END;
  }
  return DEAL_RES_CONTINUE;
}

static bool externalWindowUnitHasColumn(SDynQueryCtrlOperatorInfo* pInfo, SOperatorInfo* pExtWinOperator,
                                        col_id_t colId) {
  SOperatorInfo* pScanOp = pExtWinOperator != NULL && pExtWinOperator->numOfDownstream == 1
                              ? pExtWinOperator->pDownstream[0]
                              : NULL;
  SOperatorInfo* pFedScanOp = findFederatedScanInUnaryBranch(pScanOp);
  if (pFedScanOp != NULL) {
    SFederatedScanOperatorInfo* pFedInfo = pFedScanOp->info;
    SExtTableNode* pExtTable = pFedInfo != NULL && pFedInfo->pFedScanNode != NULL
                                  ? (SExtTableNode*)pFedInfo->pFedScanNode->pExtTable
                                  : NULL;
    return pExtTable != NULL &&
           foreignSourceMapHasColumn(pInfo->vtbScan.foreignSourceMap, pExtTable->sourceName, colId);
  }
  return localSourceMapHasColumn(pInfo->vtbScan.otbVgIdToOtbInfoArrayMap, colId);
}

// Physical templates have the same output shape on every execution unit.  At
// runtime the virtual-column maps decide which aggregate belongs to this unit.
// Non-owned aggregate slots are later nulled before Merge(colsMerge=true).
static int32_t analyzeExternalWindowUnit(SDynQueryCtrlOperatorInfo* pInfo, SOperatorInfo* pExtWinOperator,
                                         SArray** ppRequiredColIds, SArray** ppNullOutputSlots, bool* pActive) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SArray* pRequiredColIds = NULL;
  SArray* pNullOutputSlots = NULL;
  bool    hasMappedFunc = false;
  bool    ownsMappedFunc = false;

  QUERY_CHECK_NULL(pInfo, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(ppRequiredColIds, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(ppNullOutputSlots, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pActive, code, lino, _return, TSDB_CODE_INVALID_PARA);
  *ppRequiredColIds = NULL;
  *ppNullOutputSlots = NULL;
  *pActive = false;
  QUERY_CHECK_NULL(pExtWinOperator, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pExtWinOperator->pPhyNode, code, lino, _return, TSDB_CODE_INVALID_PARA);

  SExternalWindowPhysiNode* pPhyNode = (SExternalWindowPhysiNode*)pExtWinOperator->pPhyNode;
  pRequiredColIds = taosArrayInit(TMAX(1, LIST_LENGTH(pPhyNode->window.pFuncs)), sizeof(col_id_t));
  QUERY_CHECK_NULL(pRequiredColIds, code, lino, _return, terrno);
  pNullOutputSlots = taosArrayInit(TMAX(1, LIST_LENGTH(pPhyNode->window.pFuncs)), sizeof(int32_t));
  QUERY_CHECK_NULL(pNullOutputSlots, code, lino, _return, terrno);

  SOperatorInfo* pCalcSource = pExtWinOperator->numOfDownstream == 1 ? pExtWinOperator->pDownstream[0] : NULL;
  if (pCalcSource != NULL && pCalcSource->operatorType == QUERY_NODE_PHYSICAL_PLAN_DYN_QUERY_CTRL) {
    *pActive = true;
    *ppRequiredColIds = pRequiredColIds;
    *ppNullOutputSlots = pNullOutputSlots;
    return TSDB_CODE_SUCCESS;
  }

  SNode* pFuncNode = NULL;
  FOREACH(pFuncNode, pPhyNode->window.pFuncs) {
    if (QUERY_NODE_TARGET != nodeType(pFuncNode)) {
      continue;
    }
    STargetNode* pTarget = (STargetNode*)pFuncNode;
    SArray* pFuncColIds = taosArrayInit(1, sizeof(col_id_t));
    QUERY_CHECK_NULL(pFuncColIds, code, lino, _return, terrno);
    SVstbFuncColumnCollectCtx ctx = {.pColIds = pFuncColIds, .code = TSDB_CODE_SUCCESS};
    nodesWalkExpr(pTarget->pExpr, collectVstbFuncColumn, &ctx);
    if (ctx.code != TSDB_CODE_SUCCESS) {
      code = ctx.code;
      taosArrayDestroy(pFuncColIds);
      QUERY_CHECK_CODE(code, lino, _return);
    }

    if (taosArrayGetSize(pFuncColIds) > 0) {
      hasMappedFunc = true;
      bool ownsFunc = true;
      for (int32_t i = 0; i < taosArrayGetSize(pFuncColIds); ++i) {
        col_id_t* pColId = taosArrayGet(pFuncColIds, i);
        if (pColId == NULL || !externalWindowUnitHasColumn(pInfo, pExtWinOperator, *pColId)) {
          ownsFunc = false;
          break;
        }
      }
      if (ownsFunc) {
        ownsMappedFunc = true;
        for (int32_t i = 0; i < taosArrayGetSize(pFuncColIds); ++i) {
          col_id_t* pColId = taosArrayGet(pFuncColIds, i);
          if (pColId != NULL && !colIdArrayContains(pRequiredColIds, *pColId) &&
              taosArrayPush(pRequiredColIds, pColId) == NULL) {
            code = terrno;
            taosArrayDestroy(pFuncColIds);
            QUERY_CHECK_CODE(code, lino, _return);
          }
        }
      } else if (taosArrayPush(pNullOutputSlots, &pTarget->slotId) == NULL) {
        code = terrno;
        taosArrayDestroy(pFuncColIds);
        QUERY_CHECK_CODE(code, lino, _return);
      }
    }
    taosArrayDestroy(pFuncColIds);
  }

  *pActive = !hasMappedFunc || ownsMappedFunc;
  *ppRequiredColIds = pRequiredColIds;
  *ppNullOutputSlots = pNullOutputSlots;
  return TSDB_CODE_SUCCESS;

_return:
  taosArrayDestroy(pRequiredColIds);
  taosArrayDestroy(pNullOutputSlots);
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
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
                                                         ENodeType srcOpType, SHashObj* newAddedVgInfo,
                                                         uint64_t clientId) {
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

    // When an original-table vgroup was deployed at runtime (stream redeploy), the merge
    // exchange has no static source endpoint for it. Thread the freshly-deployed reader's
    // SDownstreamSourceNode so addSingleExchangeSource can dynamically register it (isNewDeployed).
    SDownstreamSourceNode  newSource = {0};
    SDownstreamSourceNode* pNewSource = NULL;
    if (newAddedVgInfo != NULL) {
      SStreamTaskAddr* addr = (SStreamTaskAddr*)taosHashGet(newAddedVgInfo, vgId, sizeof(*vgId));
      if (addr != NULL) {
        newSource.type = QUERY_NODE_DOWNSTREAM_SOURCE;
        newSource.clientId = clientId;
        newSource.taskId = addr->taskId;
        newSource.fetchMsgType = TDMT_STREAM_FETCH;
        newSource.localExec = false;
        newSource.addr.nodeId = addr->nodeId;
        memcpy(&newSource.addr.epSet, &addr->epset, sizeof(SEpSet));
        pNewSource = &newSource;
      }
    }

    code = buildExchangeOperatorBasicParam(&basic, srcOpType,
                                           type, *vgId, groupid,
                                           NULL, NULL, pTagList, NULL, pOrgTbInfoArray,
                                           window, pNewSource, false, true, pNewSource != NULL);
    QUERY_CHECK_CODE(code, lino, _return);

    code = tSimpleHashPut(pExc->pBatchs, vgId, sizeof(*vgId), &basic, sizeof(basic));
    QUERY_CHECK_CODE(code, lino, _return);

    // The new source is registered into the exchange only once; subsequent fires find it
    // in the exchange's source set, so drop it from the pending runtime-added map.
    if (pNewSource != NULL) {
      code = taosHashRemove(newAddedVgInfo, vgId, sizeof(*vgId));
      QUERY_CHECK_CODE(code, lino, _return);
    }

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

  pVScan = taosMemoryCalloc(1, sizeof(SVTableScanOperatorParam));
  QUERY_CHECK_NULL(pVScan, code, lino, _return, terrno)
  pVScan->pOpParamArray = taosArrayInit(1, POINTER_BYTES);
  QUERY_CHECK_NULL(pVScan->pOpParamArray, code, lino, _return, terrno)
  pVScan->pForeignParamArray = NULL;
  pVScan->uid = uid;
  pVScan->window = pInfo->vtbScan.window;
  pVScan->tagDownStreamId = -1;
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

static SNode* getVirtualScanFilterCond(SOperatorInfo* pVtbScanOp) {
  if (pVtbScanOp == NULL || pVtbScanOp->pPhyNode == NULL ||
      nodeType(pVtbScanOp->pPhyNode) != QUERY_NODE_PHYSICAL_PLAN_VIRTUAL_TABLE_SCAN) {
    return NULL;
  }

  SVirtualScanPhysiNode* pVScan = (SVirtualScanPhysiNode*)pVtbScanOp->pPhyNode;
  return pVScan->scan.node.pConditions;
}

static int64_t getLimitRowsFromPhysiNode(SPhysiNode* pNode) {
  if (pNode == NULL || pNode->pLimit == NULL || nodeType(pNode->pLimit) != QUERY_NODE_LIMIT) {
    return -1;
  }

  SLimitNode* pLimit = (SLimitNode*)pNode->pLimit;
  if (pLimit->limit == NULL || pLimit->limit->isNull) {
    return -1;
  }

  int64_t rows = pLimit->limit->datum.i;
  if (pLimit->offset != NULL && !pLimit->offset->isNull && pLimit->offset->datum.i > 0) {
    rows += pLimit->offset->datum.i;
  }
  return rows > 0 ? rows : -1;
}

static int64_t getVirtualScanRemoteRowLimit(SOperatorInfo* pDynOp, SOperatorInfo* pVtbScanOp) {
  int64_t rowLimit = getLimitRowsFromPhysiNode(pDynOp != NULL ? (SPhysiNode*)pDynOp->pPhyNode : NULL);
  if (rowLimit > 0) {
    return rowLimit;
  }
  return getLimitRowsFromPhysiNode(pVtbScanOp != NULL ? (SPhysiNode*)pVtbScanOp->pPhyNode : NULL);
}

static void setVirtualScanRemoteRowLimit(SHashObj* pForeignSourceMap, int64_t rowLimit) {
  if (pForeignSourceMap == NULL) {
    return;
  }

  void* pIter = taosHashIterate(pForeignSourceMap, NULL);
  while (pIter != NULL) {
    SForeignSourceInfo* pFsInfo = (SForeignSourceInfo*)pIter;
    pFsInfo->rowLimit = rowLimit;
    nodesDestroyNode(pFsInfo->pPushedCond);
    pFsInfo->pPushedCond = NULL;
    pIter = taosHashIterate(pForeignSourceMap, pIter);
  }
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

static int32_t buildExternalWindowOperatorParam(uint16_t downstreamType, SOperatorParam** ppRes, SArray* pWins,
                                                int32_t idx) {
  int32_t                       code = TSDB_CODE_SUCCESS;
  int32_t                       lino = 0;
  SExternalWindowOperatorParam* pExtWinOp = NULL;

  *ppRes = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(*ppRes, code, lino, _return, terrno)

  pExtWinOp = taosMemoryCalloc(1, sizeof(SExternalWindowOperatorParam));
  QUERY_CHECK_NULL(pExtWinOp, code, lino, _return, terrno)

  pExtWinOp->ExtWins = taosArrayDup(pWins, NULL);
  QUERY_CHECK_NULL(pExtWinOp->ExtWins, code, lino, _return, terrno)

  SExtWinTimeWindow *firstWin = (SExtWinTimeWindow *)taosArrayGet(pWins, 0);
  SExtWinTimeWindow *lastWin = (SExtWinTimeWindow *)taosArrayGet(pWins, taosArrayGetSize(pWins) - 1);

  QUERY_CHECK_NULL(firstWin, code, lino, _return, terrno)
  QUERY_CHECK_NULL(lastWin, code, lino, _return, terrno)

  (*ppRes)->pChildren = taosArrayInit(1, POINTER_BYTES);
  QUERY_CHECK_NULL((*ppRes)->pChildren, code, lino, _return, terrno)

  SOperatorParam* pDownstreamParam = NULL;
  STimeWindow     twin = {.skey = firstWin->tw.skey, .ekey = lastWin->tw.ekey};
  if (QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN == downstreamType) {
    code = buildFederatedScanOperatorParamForExternalWindow(&pDownstreamParam, twin);
    QUERY_CHECK_CODE(code, lino, _return);
  } else {
    code = buildExchangeOperatorParamForExternalWindow(&pDownstreamParam, 0, twin);
    QUERY_CHECK_CODE(code, lino, _return);
  }
  QUERY_CHECK_NULL(taosArrayPush((*ppRes)->pChildren, &pDownstreamParam), code, lino, _return, terrno)

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

static int32_t buildMergeOperatorParam(SOperatorInfo* pMergeOpInfo, SOperatorParam** ppRes, SArray* pWins,
                                       int32_t numOfDownstream, int32_t numOfWins) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SMergeOperatorParam*      pMergeOp = NULL;

  *ppRes = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(*ppRes, code, lino, _return, terrno)

  (*ppRes)->pChildren = taosArrayInit(numOfDownstream, POINTER_BYTES);
  QUERY_CHECK_NULL((*ppRes)->pChildren, code, lino, _return, terrno)

  pMergeOp = taosMemoryCalloc(1, sizeof(SMergeOperatorParam));
  QUERY_CHECK_NULL(pMergeOp, code, lino, _return, terrno)

  pMergeOp->winNum = numOfWins;
  pMergeOp->mergeSameCols = false;

  for (int32_t i = 0; i < numOfDownstream; i++) {
    SOperatorParam* pExternalWinParam = NULL;
    SOperatorInfo* pExtWinOp =
        (pMergeOpInfo != NULL && i < pMergeOpInfo->numOfDownstream) ? pMergeOpInfo->pDownstream[i] : NULL;
    QUERY_CHECK_NULL(pExtWinOp, code, lino, _return, TSDB_CODE_INVALID_PARA)
    QUERY_CHECK_CONDITION(1 == pExtWinOp->numOfDownstream, code, lino, _return, TSDB_CODE_INVALID_PARA)
    QUERY_CHECK_NULL(pExtWinOp->pDownstream[0], code, lino, _return, TSDB_CODE_INVALID_PARA)
    code = buildExternalWindowOperatorParam(pExtWinOp->pDownstream[0]->operatorType, &pExternalWinParam, pWins, i);
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
static int32_t buildMergeOperatorParamForTsScan(SDynQueryCtrlOperatorInfo* pInfo, SOperatorInfo* pMergeOp,
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
  QUERY_CHECK_NULL(pMergeOp, code, lino, _return, TSDB_CODE_INVALID_PARA)
  pParam->pChildren = taosArrayInit(pMergeOp->numOfDownstream, POINTER_BYTES);
  QUERY_CHECK_NULL(pParam->pChildren, code, lino, _return, terrno)

  pParam->value = taosMemoryCalloc(1, sizeof(SMergeOperatorParam));
  QUERY_CHECK_NULL(pParam->value, code, lino, _return, terrno)

  bool timestampOnly = true;
  col_id_t requiredColId = PRIMARYKEY_TIMESTAMP_COL_ID;
  if (pMergeOp->pPhyNode != NULL &&
      QUERY_NODE_PHYSICAL_PLAN_MERGE == nodeType((SNode*)pMergeOp->pPhyNode)) {
    SNodeList* pTargets = ((SMergePhysiNode*)pMergeOp->pPhyNode)->pTargets;
    timestampOnly = LIST_LENGTH(pTargets) <= 1;
    if (!timestampOnly) {
      SNode* pTargetNode = NULL;
      FOREACH(pTargetNode, pTargets) {
        STargetNode* pTarget = (STargetNode*)pTargetNode;
        if (QUERY_NODE_COLUMN == nodeType(pTarget->pExpr)) {
          SColumnNode* pCol = (SColumnNode*)pTarget->pExpr;
          if (!pCol->isPrimTs && PRIMARYKEY_TIMESTAMP_COL_ID != pCol->colId) {
            requiredColId = pCol->colId;
            break;
          }
        }
      }
    }
  }

  for (int32_t i = 0; i < pMergeOp->numOfDownstream; i++) {
    SOperatorInfo* pScanOp = pMergeOp->pDownstream[i];
    while (pScanOp != NULL && pScanOp->operatorType != QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN &&
           pScanOp->numOfDownstream == 1) {
      pScanOp = pScanOp->pDownstream[0];
    }
    if (pScanOp != NULL && pScanOp->operatorType == QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN) {
      SFederatedScanOperatorInfo* pFedInfo = pScanOp->info;
      SExtTableNode* pExtTable =
          pFedInfo != NULL && pFedInfo->pFedScanNode != NULL
              ? (SExtTableNode*)pFedInfo->pFedScanNode->pExtTable
              : NULL;
      QUERY_CHECK_NULL(pExtTable, code, lino, _return, TSDB_CODE_INVALID_PARA)
      if (!foreignSourceMapHasSource(pVtbScan->foreignSourceMap, pExtTable->sourceName)) {
        continue;
      }
      if (!timestampOnly &&
          !foreignSourceMapHasColumn(pVtbScan->foreignSourceMap, pExtTable->sourceName, requiredColId)) {
        continue;
      }
      code = buildBatchFederatedScanOperatorParamForVirtual(
          &pExchangeParam, i, pVtbScan->foreignSourceMap, pExtTable->sourceName,
          pVtbScan->dstPrecision, 0,
          (STimeWindow){.skey = INT64_MAX, .ekey = INT64_MIN}, timestampOnly, NULL);
      QUERY_CHECK_CODE(code, lino, _return);
      ((SForeignScanOperatorParam*)pExchangeParam->value)->sourceMetaVersion =
          pFedInfo->pFedScanNode->metaVersion;
    } else {
      if (NULL == pVtbScan->otbVgIdToOtbInfoArrayMap ||
          0 == taosHashGetSize(pVtbScan->otbVgIdToOtbInfoArrayMap)) {
        continue;
      }
      if (!timestampOnly &&
          !localSourceMapHasColumn(pVtbScan->otbVgIdToOtbInfoArrayMap, requiredColId)) {
        continue;
      }
      code = buildBatchExchangeOperatorParamForVirtual(
          &pExchangeParam, i, NULL, 0, pVtbScan->otbVgIdToOtbInfoArrayMap,
          (STimeWindow){.skey = INT64_MAX, .ekey = INT64_MIN}, EX_SRC_TYPE_VSTB_TS_SCAN,
          QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN, NULL, 0);
    }
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
      QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, pVtbScan->newAddedVgInfo, pVtbScan->clientId);
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
      QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, pVtbScan->newAddedVgInfo, pVtbScan->clientId);
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
        QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, pVtbScan->newAddedVgInfo, pVtbScan->clientId);
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

static SHashObj* vtbAggScopeLocalMap(SVtbScanDynCtrlInfo* pVtbScan,
                                     const SVtbAggDispatchScope* pScope) {
  if (pScope->hasUid) {
    SHashObj** ppMap =
        taosHashGet(pVtbScan->vtbUidToVgIdMapMap, &pScope->uid, sizeof(pScope->uid));
    return NULL != ppMap ? *ppMap : NULL;
  }
  if (pScope->hasGroup) {
    SHashObj** ppMap = taosHashGet(pVtbScan->vtbGroupIdToVgIdMapMap,
                                   &pScope->groupId, sizeof(pScope->groupId));
    return NULL != ppMap ? *ppMap : NULL;
  }
  return pVtbScan->otbVgIdToOtbInfoArrayMap;
}

static int32_t buildVtbAggHashParam(SOperatorParam** ppRes, int32_t downstreamIdx,
                                    SOperatorParam* pChild) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SOperatorParam* pParam = NULL;

  *ppRes = NULL;
  pParam = taosMemoryCalloc(1, sizeof(*pParam));
  QUERY_CHECK_NULL(pParam, code, lino, _return, terrno);
  pParam->opType = QUERY_NODE_PHYSICAL_PLAN_HASH_AGG;
  pParam->downstreamIdx = downstreamIdx;
  pParam->reUse = false;
  pParam->value = taosMemoryCalloc(1, sizeof(SAggOperatorParam));
  QUERY_CHECK_NULL(pParam->value, code, lino, _return, terrno);
  pParam->pChildren = taosArrayInit(1, POINTER_BYTES);
  QUERY_CHECK_NULL(pParam->pChildren, code, lino, _return, terrno);
  if (NULL != pChild) {
    QUERY_CHECK_NULL(taosArrayPush(pParam->pChildren, &pChild), code, lino, _return, terrno);
  }
  *ppRes = pParam;
  return code;

_return:
  freeOperatorParam(pParam, OP_GET_PARAM);
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

static int32_t buildVtbAggMergeScopeParam(SDynQueryCtrlOperatorInfo* pInfo,
                                          SOperatorInfo* pMergeOp,
                                          const SVtbAggDispatchScope* pScope,
                                          SOperatorParam** ppRes) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  SVtbScanDynCtrlInfo* pVtbScan = &pInfo->vtbScan;
  SOperatorParam*      pChildParam = NULL;
  SOperatorParam*      pBranchParam = NULL;
  SHashObj*            pLocalMap = vtbAggScopeLocalMap(pVtbScan, pScope);

  *ppRes = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(*ppRes, code, lino, _return, terrno);
  (*ppRes)->opType = QUERY_NODE_PHYSICAL_PLAN_MERGE;
  (*ppRes)->downstreamIdx = 0;
  (*ppRes)->reUse = false;
  (*ppRes)->value = taosMemoryCalloc(1, sizeof(SMergeOperatorParam));
  QUERY_CHECK_NULL((*ppRes)->value, code, lino, _return, terrno);
  ((SMergeOperatorParam*)(*ppRes)->value)->activeSourcesOnly = true;
  (*ppRes)->pChildren = taosArrayInit(TMAX(1, pMergeOp->numOfDownstream), POINTER_BYTES);
  QUERY_CHECK_NULL((*ppRes)->pChildren, code, lino, _return, terrno);

  for (int32_t i = 0; i < pMergeOp->numOfDownstream; ++i) {
    SOperatorInfo* pBranch = pMergeOp->pDownstream[i];
    SOperatorInfo* pFedScan = findFederatedScanInUnaryBranch(pBranch);
    if (NULL != pFedScan) {
      SFederatedScanOperatorInfo* pFedInfo = pFedScan->info;
      SExtTableNode* pExtTable =
          NULL != pFedInfo && NULL != pFedInfo->pFedScanNode
              ? (SExtTableNode*)pFedInfo->pFedScanNode->pExtTable
              : NULL;
      QUERY_CHECK_NULL(pExtTable, code, lino, _return, TSDB_CODE_INVALID_PARA);
      SVtbAggDispatchScope branchScope = *pScope;
      code = buildScopedBatchFederatedScanOperatorParamForVirtual(
          &pChildParam, 0, pVtbScan, pExtTable->sourceName, &branchScope);
      QUERY_CHECK_CODE(code, lino, _return);
      if (NULL == pChildParam) {
        continue;
      }
      ((SForeignScanOperatorParam*)pChildParam->value)->sourceMetaVersion =
          pFedInfo->pFedScanNode->metaVersion;
      QUERY_CHECK_CONDITION(QUERY_NODE_PHYSICAL_PLAN_HASH_AGG == pBranch->operatorType,
                            code, lino, _return, TSDB_CODE_INVALID_PARA);
      code = buildVtbAggHashParam(&pBranchParam, i, pChildParam);
      QUERY_CHECK_CODE(code, lino, _return);
      pChildParam = NULL;
    } else {
      if (NULL == pLocalMap || 0 == taosHashGetSize(pLocalMap)) {
        continue;
      }
      code = buildBatchExchangeOperatorParamForVirtual(
          &pBranchParam, i, pScope->hasUid ? pScope->pTagList : NULL,
          pScope->hasGroup ? pScope->groupId : 0, pLocalMap,
          (STimeWindow){.skey = INT64_MAX, .ekey = INT64_MIN},
          EX_SRC_TYPE_VSTB_AGG_SCAN, QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN,
          pVtbScan->newAddedVgInfo, pVtbScan->clientId);
      QUERY_CHECK_CODE(code, lino, _return);
    }
    QUERY_CHECK_NULL(taosArrayPush((*ppRes)->pChildren, &pBranchParam),
                     code, lino, _return, terrno);
    pBranchParam = NULL;
  }
  return code;

_return:
  freeOperatorParam(pChildParam, OP_GET_PARAM);
  freeOperatorParam(pBranchParam, OP_GET_PARAM);
  freeOperatorParam(*ppRes, OP_GET_PARAM);
  *ppRes = NULL;
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

static int32_t buildVtbAggScopeParam(SDynQueryCtrlOperatorInfo* pInfo,
                                     SOperatorInfo* pRootOp,
                                     const SVtbAggDispatchScope* pScope,
                                     SOperatorParam** ppRes) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SOperatorParam* pMergeParam = NULL;

  *ppRes = NULL;
  QUERY_CHECK_NULL(pRootOp, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pScope, code, lino, _return, TSDB_CODE_INVALID_PARA);
  switch (pRootOp->operatorType) {
    case QUERY_NODE_PHYSICAL_PLAN_EXCHANGE:
      if (pScope->hasUid) {
        code = buildAggOperatorParamForSingleChild(
            pInfo, pScope->uid, pScope->hasGroup ? pScope->groupId : 0,
            pScope->pTagList, ppRes);
      } else {
        code = TSDB_CODE_INVALID_PARA;
      }
      break;
    case QUERY_NODE_PHYSICAL_PLAN_MERGE:
      code = buildVtbAggMergeScopeParam(pInfo, pRootOp, pScope, ppRes);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_HASH_AGG: {
      bool hasMergeChild =
          1 == pRootOp->numOfDownstream &&
          NULL != pRootOp->pDownstream && NULL != pRootOp->pDownstream[0] &&
          QUERY_NODE_PHYSICAL_PLAN_MERGE == pRootOp->pDownstream[0]->operatorType;
      if (hasMergeChild) {
        code = buildVtbAggMergeScopeParam(pInfo, pRootOp->pDownstream[0], pScope, &pMergeParam);
        QUERY_CHECK_CODE(code, lino, _return);
        code = buildVtbAggHashParam(ppRes, 0, pMergeParam);
        QUERY_CHECK_CODE(code, lino, _return);
        pMergeParam = NULL;
      } else if (pScope->allChildren) {
        code = buildAggOperatorParam(pInfo, ppRes);
      } else if (pScope->hasUid) {
        code = buildAggOperatorParamForSingleChild(
            pInfo, pScope->uid, pScope->hasGroup ? pScope->groupId : 0,
            pScope->pTagList, ppRes);
      } else if (pScope->hasGroup) {
        code = buildAggOperatorParamWithGroupId(pInfo, pScope->groupId, ppRes);
      } else {
        code = TSDB_CODE_INVALID_PARA;
      }
      break;
    }
    default:
      code = TSDB_CODE_INVALID_PARA;
      break;
  }
  QUERY_CHECK_CODE(code, lino, _return);
  return code;

_return:
  freeOperatorParam(pMergeParam, OP_GET_PARAM);
  freeOperatorParam(*ppRes, OP_GET_PARAM);
  *ppRes = NULL;
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
        colInfo.info.precision = pSlot->dataType.precision;
        colInfo.info.scale = pSlot->dataType.scale;
        int32_t code = colInfoDataEnsureCapacity(&colInfo, pBlock->info.capacity, true);
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

int32_t extractColRefName(const char *colref, char **refDb, char** refTb, char** refCol) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     line = 0;

  const char *first_dot = strchr(colref, '.');
  QUERY_CHECK_NULL(first_dot, code, line, _return, terrno)

  const char *second_dot = strchr(first_dot + 1, '.');
  QUERY_CHECK_NULL(second_dot, code, line, _return, terrno)

  size_t db_len = first_dot - colref;
  size_t table_len = second_dot - first_dot - 1;
  size_t col_len = strlen(second_dot + 1);

  *refDb = taosMemoryMalloc(db_len + 1);
  *refTb = taosMemoryMalloc(table_len + 1);
  *refCol = taosMemoryMalloc(col_len + 1);
  QUERY_CHECK_NULL(*refDb, code, line, _return, terrno)
  QUERY_CHECK_NULL(*refTb, code, line, _return, terrno)
  QUERY_CHECK_NULL(*refCol, code, line, _return, terrno)

  tstrncpy(*refDb, colref, db_len + 1);
  tstrncpy(*refTb, first_dot + 1, table_len + 1);
  tstrncpy(*refCol, second_dot + 1, col_len + 1);

  return TSDB_CODE_SUCCESS;
_return:
  qError("%s failed at line %d since %s", __func__, line, tstrerror(code));
  if (*refDb) {
    taosMemoryFree(*refDb);
    *refDb = NULL;
  }
  if (*refTb) {
    taosMemoryFree(*refTb);
    *refTb = NULL;
  }
  if (*refCol) {
    taosMemoryFree(*refCol);
    *refCol = NULL;
  }
  return code;
}

// Forward declaration for fetchRemoteTableCfg (defined below)
static int32_t fetchRemoteTableCfg(SOperatorInfo* pOperator, SDBVgInfo* dbVgInfo, const char* dbFName,
                                   const char* tbName, int32_t vgId, STableCfgRsp* pCfgRsp);

// Free function for tableCfgCache hash entries
static void destroyTableCfgCacheEntry(void* data) {
  STableCfgRsp* pRsp = (STableCfgRsp*)data;
  if (pRsp) {
    tFreeSTableCfgRsp(pRsp);
  }
}

// Initialize per-query table config cache (lazy)
static void ensureTableCfgCache(SVtbScanDynCtrlInfo* pVtbScan) {
  if (pVtbScan->tableCfgCache == NULL) {
    pVtbScan->tableCfgCache =
        taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
    if (pVtbScan->tableCfgCache != NULL) {
      taosHashSetFreeFp(pVtbScan->tableCfgCache, destroyTableCfgCacheEntry);
    }
  }
}

// Fetch table config with per-query caching.
// Returns a pointer to cached STableCfgRsp (caller must NOT free it).
static int32_t fetchRemoteTableCfgCached(SOperatorInfo* pOperator, SDBVgInfo* dbVgInfo, const char* dbFName,
                                          const char* tbName, int32_t vgId, const STableCfgRsp** ppCfgRsp) {
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = &pInfo->vtbScan;

  // Build cache key: "dbFName.tbName"
  char cacheKey[TSDB_DB_FNAME_LEN + TSDB_TABLE_NAME_LEN + 2];
  snprintf(cacheKey, sizeof(cacheKey), "%s.%s", dbFName, tbName);

  ensureTableCfgCache(pVtbScan);

  // Check cache first
  if (pVtbScan->tableCfgCache != NULL) {
    STableCfgRsp* pCached = taosHashGet(pVtbScan->tableCfgCache, cacheKey, strlen(cacheKey));
    if (pCached != NULL) {
      *ppCfgRsp = pCached;
      return TSDB_CODE_SUCCESS;
    }
  }

  // Cache miss: fetch via RPC
  STableCfgRsp cfgRsp = {0};
  int32_t code = fetchRemoteTableCfg(pOperator, dbVgInfo, dbFName, tbName, vgId, &cfgRsp);
  if (code != TSDB_CODE_SUCCESS) {
    tFreeSTableCfgRsp(&cfgRsp);
    *ppCfgRsp = NULL;
    return code;
  }

  // Store in cache (hash takes ownership of the value copy)
  if (pVtbScan->tableCfgCache != NULL) {
    code = taosHashPut(pVtbScan->tableCfgCache, cacheKey, strlen(cacheKey), &cfgRsp, sizeof(STableCfgRsp));
    if (code == TSDB_CODE_SUCCESS) {
      *ppCfgRsp = taosHashGet(pVtbScan->tableCfgCache, cacheKey, strlen(cacheKey));
      return TSDB_CODE_SUCCESS;
    }
  }

  // Fallback: cache put failed, free and report error
  tFreeSTableCfgRsp(&cfgRsp);
  *ppCfgRsp = NULL;
  return TSDB_CODE_OUT_OF_MEMORY;
}

static int32_t fetchRemoteTableCfg(SOperatorInfo* pOperator, SDBVgInfo* dbVgInfo, const char* dbFName,
                                   const char* tbName, int32_t vgId, STableCfgRsp* pCfgRsp) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    lino = 0;
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = &pInfo->vtbScan;
  SExecTaskInfo*             pTaskInfo = pOperator->pTaskInfo;
  STableCfgReq               req = {0};

  SVgroupInfo* pVgInfo = taosHashGet(dbVgInfo->vgHash, &vgId, sizeof(vgId));
  if (!pVgInfo) {
    qError("fetchRemoteTableCfg: vgId %d not found in db %s", vgId, dbFName);
    return TSDB_CODE_FAILED;
  }

  req.header.vgId = vgId;
  tstrncpy(req.dbFName, dbFName, sizeof(req.dbFName));
  tstrncpy(req.tbName, tbName, sizeof(req.tbName));
  req.txnId = pTaskInfo->txnId;

  int32_t contLen = tSerializeSTableCfgReq(NULL, 0, &req);
  char* buf = rpcMallocCont(contLen);
  QUERY_CHECK_NULL(buf, code, lino, _return, terrno);
  if (tSerializeSTableCfgReq(buf, contLen, &req) < 0) {
    rpcFreeCont(buf);
    QUERY_CHECK_CODE(terrno, lino, _return);
  }

  SRpcMsg rpcMsg = {
    .msgType = TDMT_VND_TABLE_CFG,
    .pCont = buf,
    .contLen = contLen,
    .info.ahandle = (void*)0x9526,
    .info.notFreeAhandle = 1,
  };
  TRACE_SET_ROOTID(&rpcMsg.info.traceId, pTaskInfo->id.queryId);

  SRpcMsg rpcRsp = {0};
  code = rpcSendRecv(pVtbScan->pMsgCb->clientRpc, &pVgInfo->epSet, &rpcMsg, &rpcRsp);
  if (code != TSDB_CODE_SUCCESS) {
    qDebug("fetchRemoteTableCfg: rpcSendRecv failed for %s.%s on vgId %d, code=%s",
           dbFName, tbName, vgId, tstrerror(code));
    return code;
  }

  if (rpcRsp.code != TSDB_CODE_SUCCESS || rpcRsp.pCont == NULL || rpcRsp.contLen <= 0) {
    qDebug("fetchRemoteTableCfg: table %s.%s on vgId %d returned %s",
           dbFName, tbName, vgId, tstrerror(rpcRsp.code));
    rpcFreeCont(rpcRsp.pCont);
    return rpcRsp.code;
  }

  code = tDeserializeSTableCfgRsp(rpcRsp.pCont, rpcRsp.contLen, pCfgRsp);
  rpcFreeCont(rpcRsp.pCont);
  if (code != TSDB_CODE_SUCCESS) {
    qDebug("fetchRemoteTableCfg: deserialize failed for %s.%s on vgId %d", dbFName, tbName, vgId);
    return code;
  }

  qDebug("fetchRemoteTableCfg: got tags for %s.%s from vgId %d", dbFName, tbName, vgId);
  return TSDB_CODE_SUCCESS;

_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

/*
 * ============================================================================
 * Resolve tag values for a virtual child table.
 *
 * This function only processes TagRef column references (refType == 1).
 * Non-TagRef references are ignored  -  they are handled by data-scan operators.
 *
 * TagRef projection output is handled separately by TagRefSource planner nodes
 * (see STagRefSourceLogicNode/PhysiNode). This function serves the DynQueryCtrl
 * path where tag values are resolved per-child-table for filter push-down.
 * ============================================================================
 */

// Look up cached tag list from partition or uid maps.
static SArray* lookupCachedTagList(SVtbScanDynCtrlInfo* pVtbScan, uint64_t uid) {
  if (pVtbScan->hasPartition) {
    uint64_t* pGroupId = taosHashGet(pVtbScan->vtbUidToGroupIdMap, &uid, sizeof(uid));
    if (pGroupId == NULL) {
      return NULL;
    }
    SHashObj** ppUidTagListMap = taosHashGet(pVtbScan->vtbGroupIdTagListMap, pGroupId, sizeof(*pGroupId));
    if (ppUidTagListMap == NULL || *ppUidTagListMap == NULL) {
      return NULL;
    }
    SArray** ppTagList = taosHashGet(*ppUidTagListMap, &uid, sizeof(uid));
    return (ppTagList != NULL) ? *ppTagList : NULL;
  }

  if (pVtbScan->vtbUidTagListMap != NULL) {
    SArray** ppTagList = taosHashGet(pVtbScan->vtbUidTagListMap, &uid, sizeof(uid));
    return (ppTagList != NULL) ? *ppTagList : NULL;
  }
  return NULL;
}

// Fetch table config and extract all tag values into ppResolvedTags.
// For STB mode, tags with active refs are skipped (resolved by tag-ref chain later).
static int32_t fetchAndExtractTags(SOperatorInfo* pOperator, SVtbScanDynCtrlInfo* pVtbScan,
                                   const char* dbName, const char* tbName,
                                   bool skipActiveRefs, SArray** ppResolvedTags) {
  int32_t        code = TSDB_CODE_SUCCESS;
  SName          tblName = {0};
  char           dbFName[TSDB_DB_FNAME_LEN] = {0};
  SDBVgInfo*     dbVgInfo = NULL;
  int32_t        vgId = 0;
  STableCfgRsp   cfgRsp = {0};

  toName(pVtbScan->acctId, dbName, tbName, &tblName);
  code = getDbVgInfo(pOperator, &tblName, &dbVgInfo);
  if (code != TSDB_CODE_SUCCESS) return code;
  code = tNameGetFullDbName(&tblName, dbFName);
  if (code != TSDB_CODE_SUCCESS) return code;
  code = getVgId(dbVgInfo, dbFName, &vgId, tblName.tname);
  if (code != TSDB_CODE_SUCCESS) return code;

  code = fetchRemoteTableCfg(pOperator, dbVgInfo, dbFName, tblName.tname, vgId, &cfgRsp);
  if (code != TSDB_CODE_SUCCESS || cfgRsp.pTags == NULL || cfgRsp.pSchemas == NULL) {
    tFreeSTableCfgRsp(&cfgRsp);
    return code;
  }

  *ppResolvedTags = taosArrayInit(cfgRsp.numOfTags, sizeof(STagVal));
  if (*ppResolvedTags == NULL) {
    tFreeSTableCfgRsp(&cfgRsp);
    return terrno;
  }

  for (int32_t tagIdx = 0; tagIdx < cfgRsp.numOfTags; ++tagIdx) {
    SSchema* pSchema = &cfgRsp.pSchemas[cfgRsp.numOfColumns + tagIdx];
    if (skipActiveRefs && cfgRsp.pTagRefs != NULL) {
      bool hasActiveRef = false;
      for (int32_t r = 0; r < cfgRsp.numOfTagRefs; ++r) {
        if (cfgRsp.pTagRefs[r].hasRef && cfgRsp.pTagRefs[r].id == pSchema->colId) {
          hasActiveRef = true;
          break;
        }
      }
      if (hasActiveRef) continue;
    }
    code = appendResolvedTagVal(*ppResolvedTags, pSchema->colId, pSchema, (const STag*)cfgRsp.pTags);
    if (code != TSDB_CODE_SUCCESS) break;
  }

  tFreeSTableCfgRsp(&cfgRsp);
  return code;
}

// Cache resolved tags into vtbUidTagListMap for subsequent calls.
static void cacheResolvedTags(SVtbScanDynCtrlInfo* pVtbScan, uint64_t uid, SArray* pResolvedTags) {
  if (pVtbScan->vtbUidTagListMap == NULL) {
    pVtbScan->vtbUidTagListMap =
        taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
    if (pVtbScan->vtbUidTagListMap != NULL) {
      taosHashSetFreeFp(pVtbScan->vtbUidTagListMap, destroyTagList);
    }
  }
  if (pVtbScan->vtbUidTagListMap == NULL || pResolvedTags == NULL) return;

  SArray* pCachedCopy = NULL;
  int32_t code = cloneTagList(pResolvedTags, &pCachedCopy);
  if (code == TSDB_CODE_SUCCESS && pCachedCopy != NULL) {
    if (taosHashPut(pVtbScan->vtbUidTagListMap, &uid, sizeof(uid), &pCachedCopy, POINTER_BYTES) != TSDB_CODE_SUCCESS) {
      destroyTagList(&pCachedCopy);
    }
  }
}

// Follow the tag-ref chain for a single TagRef column until reaching a physical table,
// then extract the terminal tag value into ppResolvedTags.
static int32_t resolveOneTagRefChain(SOperatorInfo* pOperator, SVtbScanDynCtrlInfo* pVtbScan,
                                     SColRefInfo* pKV, SArray* pColRefInfo, SArray** ppResolvedTags) {
  int32_t  code = TSDB_CODE_SUCCESS;
  char*    refDbName = NULL;
  char*    refTbName = NULL;
  char*    refColName = NULL;

  code = extractColRefName(pKV->colrefName, &refDbName, &refTbName, &refColName);
  if (code != TSDB_CODE_SUCCESS) goto _cleanup;

  for (int32_t depth = 0; depth < TSDB_MAX_VTABLE_REF_DEPTH; ++depth) {
    SName          name = {0};
    char           dbFName[TSDB_DB_FNAME_LEN] = {0};
    int32_t        vgId = 0;
    SDBVgInfo*     dbVgInfo = NULL;

    toName(pVtbScan->acctId, refDbName, refTbName, &name);
    code = getDbVgInfo(pOperator, &name, &dbVgInfo);
    if (code != TSDB_CODE_SUCCESS) goto _cleanup;
    code = tNameGetFullDbName(&name, dbFName);
    if (code != TSDB_CODE_SUCCESS) goto _cleanup;
    code = getVgId(dbVgInfo, dbFName, &vgId, name.tname);
    if (code != TSDB_CODE_SUCCESS) goto _cleanup;

    // Optimization 2: Use per-query cached table config to avoid redundant RPCs
    // across children sharing the same tag-ref chain intermediate nodes.
    const STableCfgRsp* pCfgRsp = NULL;
    code = fetchRemoteTableCfgCached(pOperator, dbVgInfo, dbFName, name.tname, vgId, &pCfgRsp);
    if (code != TSDB_CODE_SUCCESS || pCfgRsp == NULL) {
      goto _cleanup;
    }

    // Check if the referenced tag is itself a tag-ref (chain continues)
    bool chainContinues = false;
    if (pCfgRsp->pTagRefs != NULL && pCfgRsp->numOfTagRefs > 0 && pCfgRsp->pSchemas != NULL) {
      col_id_t refTagColId = -1;
      for (int32_t tagIdx = 0; tagIdx < pCfgRsp->numOfTags; ++tagIdx) {
        SSchema* pSchema = &pCfgRsp->pSchemas[pCfgRsp->numOfColumns + tagIdx];
        if (0 == strcmp(pSchema->name, refColName)) {
          refTagColId = pSchema->colId;
          break;
        }
      }
      if (refTagColId >= 0) {
        for (int32_t t = 0; t < pCfgRsp->numOfTagRefs; ++t) {
          SColRef* pChainRef = &pCfgRsp->pTagRefs[t];
          if (pChainRef->hasRef && pChainRef->id == refTagColId) {
            taosMemoryFree(refDbName);
            taosMemoryFree(refTbName);
            taosMemoryFree(refColName);
            refDbName = taosStrdup(pChainRef->refDbName);
            refTbName = taosStrdup(pChainRef->refTableName);
            refColName = taosStrdup(pChainRef->refColName);
            if (!refDbName || !refTbName || !refColName) {
              code = terrno;
              goto _cleanup;
            }
            chainContinues = true;
            break;
          }
        }
      }
    }

    if (chainContinues) {
      continue;
    }

    // Terminal: extract the tag value
    if (pCfgRsp->pTags != NULL && pCfgRsp->pSchemas != NULL) {
      const SSchema* pTagSchema = NULL;
      for (int32_t tagIdx = 0; tagIdx < pCfgRsp->numOfTags; ++tagIdx) {
        SSchema* pSchema = &pCfgRsp->pSchemas[pCfgRsp->numOfColumns + tagIdx];
        if (0 == strcmp(pSchema->name, refColName)) {
          pTagSchema = pSchema;
          break;
        }
      }

      if (pTagSchema != NULL) {
        if (*ppResolvedTags == NULL) {
          *ppResolvedTags = taosArrayInit(taosArrayGetSize(pColRefInfo), sizeof(STagVal));
          if (*ppResolvedTags == NULL) {
            code = terrno;
            goto _cleanup;
          }
        }
        code = appendResolvedTagVal(*ppResolvedTags, pKV->colId, pTagSchema, (const STag*)pCfgRsp->pTags);
      }
    }

    break;
  }

_cleanup:
  taosMemoryFree(refDbName);
  taosMemoryFree(refTbName);
  taosMemoryFree(refColName);
  return code;
}

static int32_t resolveTagValsForVtbChild(SOperatorInfo* pOperator, SArray* pColRefInfo,
                                         const char* vtbName, SArray** ppResolvedTags) {
  int32_t code = TSDB_CODE_SUCCESS;
  *ppResolvedTags = NULL;

  if (pOperator == NULL || pOperator->info == NULL || pColRefInfo == NULL || taosArrayGetSize(pColRefInfo) == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = &pInfo->vtbScan;
  SColRefInfo*               pRefInfo = taosArrayGet(pColRefInfo, 0);
  if (pRefInfo == NULL) return terrno;

  // Step 1: Try to get tags from cache
  SArray* pTagList = lookupCachedTagList(pVtbScan, pRefInfo->uid);
  if (pVtbScan->hasPartition && pTagList == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  // Step 2: If not cached, fetch tag metadata on-demand
  if (pTagList == NULL && !pVtbScan->isSuperTable) {
    code = fetchAndExtractTags(pOperator, pVtbScan, pVtbScan->dbName, pVtbScan->tbName,
                               false, ppResolvedTags);
    if (code != TSDB_CODE_SUCCESS) goto _error;
    cacheResolvedTags(pVtbScan, pRefInfo->uid, *ppResolvedTags);
  } else if (pTagList == NULL && pVtbScan->isSuperTable && vtbName != NULL) {
    // STB scans: fetch child's tag metadata, skip tags with active refs
    code = fetchAndExtractTags(pOperator, pVtbScan, pVtbScan->dbName, vtbName,
                               true, ppResolvedTags);
    if (code != TSDB_CODE_SUCCESS) goto _error;
    // Optimization 1: Cache resolved tags for STB path to avoid redundant resolution
    // when the same child table is visited again within the same query.
    if (*ppResolvedTags != NULL) {
      cacheResolvedTags(pVtbScan, pRefInfo->uid, *ppResolvedTags);
    }
  } else if (pTagList != NULL) {
    code = cloneTagList(pTagList, ppResolvedTags);
    if (code != TSDB_CODE_SUCCESS) goto _error;
  }
  // else: pTagList == NULL for dynamic VSTB scan  -  resolved by tag-ref chain below

  // Step 3: Resolve TagRef columns by following the ref chain to physical tables
  for (int32_t i = 0; i < taosArrayGetSize(pColRefInfo); ++i) {
    SColRefInfo* pKV = taosArrayGet(pColRefInfo, i);
    if (pKV == NULL) { code = terrno; goto _error; }
    if (pKV->refType != 1 || pKV->colrefName == NULL || !colNeedScan(pOperator, pKV->colId)) {
      continue;
    }
    code = resolveOneTagRefChain(pOperator, pVtbScan, pKV, pColRefInfo, ppResolvedTags);
    if (code != TSDB_CODE_SUCCESS) {
      taosArrayDestroy(*ppResolvedTags);
      *ppResolvedTags = NULL;
      goto _error;
    }
  }

  return TSDB_CODE_SUCCESS;

_error:
  taosArrayDestroy(*ppResolvedTags);
  *ppResolvedTags = NULL;
  qError("%s failed for %s since %s", __func__, vtbName, tstrerror(code));
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

/*
 * Async callback for the TDMT_VND_VTB_TAG_COND response. Decodes the per-column
 * stored conditions into the scratch field pTagCondRsp for fetchVtbColConds to
 * consume. Posts the ready semaphore on both success and failure so the waiter
 * is never left blocked.
 *
 * @param param SOperatorInfo of the dyn query ctrl operator.
 * @param pMsg Response payload buffer.
 * @param code RPC level result code.
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise error code.
 */
int32_t dynProcessVTagCondRsp(void* param, SDataBuf* pMsg, int32_t code) {
  int32_t                    lino = 0;
  SOperatorInfo*             operator=(SOperatorInfo*) param;
  SDynQueryCtrlOperatorInfo* pScanResInfo = (SDynQueryCtrlOperatorInfo*)operator->info;

  if (TSDB_CODE_SUCCESS != code) {
    operator->pTaskInfo->code = rpcCvtErrCode(code);
    qError("fetch vtb tag cond rsp received, error:%s", tstrerror(operator->pTaskInfo->code));
    goto _return;
  }

  pScanResInfo->vtbScan.pTagCondRsp = taosMemoryCalloc(1, sizeof(SVTagCondRsp));
  QUERY_CHECK_NULL(pScanResInfo->vtbScan.pTagCondRsp, code, lino, _return, terrno)

  code = tDeserializeSVTagCondRsp(pMsg->pData, (int32_t)pMsg->len, pScanResInfo->vtbScan.pTagCondRsp);
  QUERY_CHECK_CODE(code, lino, _return);

_return:
  taosMemoryFreeClear(pMsg->pData);
  if (lino != 0) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    if (operator->pTaskInfo->code == TSDB_CODE_SUCCESS) {
      operator->pTaskInfo->code = code;
    }
  }
  int32_t postCode = tsem_post(&pScanResInfo->vtbScan.ready);
  if (postCode != TSDB_CODE_SUCCESS) {
    qError("%s failed to post vtb scan ready semaphore since %s", __func__, tstrerror(postCode));
    if (operator->pTaskInfo->code == TSDB_CODE_SUCCESS) {
      operator->pTaskInfo->code = postCode;
    }
    if (code == TSDB_CODE_SUCCESS) {
      code = postCode;
    }
  }
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

static int32_t getVgId(SDBVgInfo* dbInfo, char* dbFName, int32_t* vgId, char *tbName) {
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
    pParam = NULL;
    *pBootstrapped = true;
    return code;
  }

  code = pTargetOp->fpSet.getNextFn(pTargetOp, ppBlock);
  QUERY_CHECK_CODE(code, line, _return);
  return code;

_return:
  freeOperatorParam(pParam, OP_GET_PARAM);
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
 * @param fullColRef final raw column ref in db.table.col format, or empty for an unbound virtual column
 * @param vgId      final raw vgroup id
 * @param pLeafEdge virtual-table column that directly references an external column
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t dynPutResolvedColRef(SVtbScanDynCtrlInfo* pVtbScan, const char* rootRef, const char* fullColRef,
                                    int32_t vgId, const SDynRefEdge* pLeafEdge) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             line = 0;
  SDynResolvedColRef* pResolved = NULL;
  SColRefNameView     refView = {0};
  char*               refSourceName = NULL;
  char*               refDbName = NULL;
  char*               refTbName = NULL;
  char*               refColName = NULL;

  pResolved = taosMemoryCalloc(1, sizeof(SDynResolvedColRef));
  QUERY_CHECK_NULL(pResolved, code, line, _return, terrno);

  if (dynIsExternalColRefName(fullColRef)) {
    code = extractExtColRefName(fullColRef, &refSourceName, &refDbName, &refTbName, &refColName);
    QUERY_CHECK_CODE(code, line, _return);

    tstrncpy(pResolved->sourceName, refSourceName, sizeof(pResolved->sourceName));
    tstrncpy(pResolved->dbName, refDbName, sizeof(pResolved->dbName));
    tstrncpy(pResolved->tbName, refTbName, sizeof(pResolved->tbName));
    tstrncpy(pResolved->colName, refColName, sizeof(pResolved->colName));
    pResolved->extRefType = 1;
    pResolved->vgId = 0;
    QUERY_CHECK_NULL(pLeafEdge, code, line, _return, TSDB_CODE_VTABLE_INVALID_REF_COLUMN);
    pResolved->leafUid = pLeafEdge->uid;
    pResolved->leafVgId = pLeafEdge->vgId;
    pResolved->leafColId = pLeafEdge->colId;
  } else if ('\0' != fullColRef[0]) {
    code = parseColRefNameView(fullColRef, &refView);
    QUERY_CHECK_CODE(code, line, _return);

    memcpy(pResolved->dbName, refView.dbName, refView.dbNameLen);
    memcpy(pResolved->tbName, refView.tbName, refView.tbNameLen);
    memcpy(pResolved->colName, refView.colName, refView.colNameLen);
    pResolved->vgId = vgId;
  }
  tstrncpy(pResolved->fullColRef, fullColRef, sizeof(pResolved->fullColRef));

  qDebug("vtable resolved final colref, root:%s -> final:%s vgId:%d ext:%d leafUid:%" PRIu64
         " leafVgId:%d leafColId:%d",
         rootRef ? rootRef : "<null>", fullColRef ? fullColRef : "<null>", vgId, pResolved->extRefType,
         pResolved->leafUid, pResolved->leafVgId, pResolved->leafColId);

  code = taosHashPut(pVtbScan->resolvedColRefMap, rootRef, strlen(rootRef), &pResolved, POINTER_BYTES);
  QUERY_CHECK_CODE(code, line, _return);

  taosMemoryFree(refSourceName);
  taosMemoryFree(refDbName);
  taosMemoryFree(refTbName);
  taosMemoryFree(refColName);
  return code;
_return:
  taosMemoryFree(refSourceName);
  taosMemoryFree(refDbName);
  taosMemoryFree(refTbName);
  taosMemoryFree(refColName);
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

      if (dynIsExternalColRefName(pCurrentRef->colRef)) {
        continue;
      }

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
  taosHashCancelIterate(pPendingMap, pIter);
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
    SColumnInfoData* pTableNameCol = taosArrayGet(pBlock->pDataBlock, VC_COL_TABLE_NAME);
    SColumnInfoData* pDbNameCol = taosArrayGet(pBlock->pDataBlock, VC_COL_DB_NAME);
    SColumnInfoData* pColNameCol = taosArrayGet(pBlock->pDataBlock, VC_COL_COL_NAME);
    SColumnInfoData* pUidCol = taosArrayGet(pBlock->pDataBlock, VC_COL_UID);
    SColumnInfoData* pColIdCol = taosArrayGet(pBlock->pDataBlock, VC_COL_COL_ID);
    SColumnInfoData* pRefCol = taosArrayGet(pBlock->pDataBlock, VC_COL_COL_SOURCE);
    SColumnInfoData* pVgIdCol = taosArrayGet(pBlock->pDataBlock, VC_COL_VGROUP_ID);

    QUERY_CHECK_NULL(pTableNameCol, code, line, _return, terrno);
    QUERY_CHECK_NULL(pDbNameCol, code, line, _return, terrno);
    QUERY_CHECK_NULL(pColNameCol, code, line, _return, terrno);
    QUERY_CHECK_NULL(pUidCol, code, line, _return, terrno);
    QUERY_CHECK_NULL(pColIdCol, code, line, _return, terrno);
    QUERY_CHECK_NULL(pRefCol, code, line, _return, terrno);
    QUERY_CHECK_NULL(pVgIdCol, code, line, _return, terrno);

    for (int32_t i = 0; i < pBlock->info.rows; ++i) {
      char      dbName[TSDB_DB_NAME_LEN] = {0};
      char      tbName[TSDB_TABLE_NAME_LEN] = {0};
      char      colName[TSDB_COL_NAME_LEN] = {0};
      char      currentRef[sizeof(((SDynRefKey*)0)->colRef)] = {0};
      SDynRefEdge edge = {0};
      uint8_t   hit = 1;
      char*     pDb = colDataGetData(pDbNameCol, i);
      char*     pTb = colDataGetData(pTableNameCol, i);
      char*     pCol = colDataGetData(pColNameCol, i);

      memcpy(dbName, varDataVal(pDb), varDataLen(pDb));
      memcpy(tbName, varDataVal(pTb), varDataLen(pTb));
      memcpy(colName, varDataVal(pCol), varDataLen(pCol));

      len = tsnprintf(currentRef, sizeof(currentRef), "%s.%s.%s", dbName, tbName, colName);
      if (len < 0 || len >= sizeof(currentRef)) {
        code = TSDB_CODE_INVALID_PARA;
        QUERY_CHECK_CODE(code, line, _return);
      }

      code = taosHashPut(pHitMap, currentRef, strlen(currentRef), &hit, sizeof(hit));
      QUERY_CHECK_CODE(code, line, _return);

      if (colDataIsNull_s(pRefCol, i)) {
        qDebug("vtable resolve layer terminal ref, current:%s", currentRef);
        continue;
      }

      QUERY_CHECK_CONDITION(!colDataIsNull_s(pUidCol, i), code, line, _return,
                            TSDB_CODE_VTABLE_INVALID_REF_COLUMN);
      QUERY_CHECK_CONDITION(!colDataIsNull_s(pColIdCol, i), code, line, _return,
                            TSDB_CODE_VTABLE_INVALID_REF_COLUMN);
      QUERY_CHECK_CONDITION(!colDataIsNull_s(pVgIdCol, i), code, line, _return,
                            TSDB_CODE_VTABLE_INVALID_REF_COLUMN);
      GET_TYPED_DATA(edge.uid, int64_t, TSDB_DATA_TYPE_BIGINT, colDataGetNumData(pUidCol, i), 0);
      GET_TYPED_DATA(edge.colId, int32_t, TSDB_DATA_TYPE_INT, colDataGetNumData(pColIdCol, i), 0);
      GET_TYPED_DATA(edge.vgId, int32_t, TSDB_DATA_TYPE_INT, colDataGetNumData(pVgIdCol, i), 0);

      int32_t refLen = varDataLen(colDataGetData(pRefCol, i));
      if (refLen >= sizeof(edge.nextRef.colRef)) {
        qError("colRef length %d exceeds buffer size %d", refLen, (int32_t)sizeof(edge.nextRef.colRef));
        code = TSDB_CODE_INVALID_PARA;
        QUERY_CHECK_CODE(code, line, _return);
      }
      memcpy(edge.nextRef.colRef, varDataVal(colDataGetData(pRefCol, i)), refLen);
      qDebug("vtable resolve layer edge, current:%s -> next:%s uid:%" PRIu64 " vgId:%d colId:%d", currentRef,
             edge.nextRef.colRef, edge.uid, edge.vgId, edge.colId);
      code = taosHashPut(pNextRefMap, currentRef, strlen(currentRef), &edge, sizeof(edge));
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
 * @param pEdgeMap  discovered ref graph, key currentRef, value SDynRefEdge; empty nextRef means terminal
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

    for (int32_t depth = 0; depth < TSDB_MAX_VTABLE_REF_DEPTH; ++depth) {
      SDynRefEdge* pEdge = (SDynRefEdge*)taosHashGet(pEdgeMap, currentRef, strlen(currentRef));
      int32_t      vgId = 0;

      QUERY_CHECK_NULL(pEdge, code, line, _return, terrno);
      if (pEdge->isNullRef) {
        code = dynPutResolvedColRef(pVtbScan, pRootRef->colRef, "", 0, NULL);
        QUERY_CHECK_CODE(code, line, _return);
        break;
      }
      if (pEdge->nextRef.colRef[0] != 0) {
        if (dynIsExternalColRefName(pEdge->nextRef.colRef)) {
          code = dynPutResolvedColRef(pVtbScan, pRootRef->colRef, pEdge->nextRef.colRef, 0, pEdge);
          QUERY_CHECK_CODE(code, line, _return);
          break;
        }
        currentRef = pEdge->nextRef.colRef;
        continue;
      }

      code = dynGetRefVgId(pOperator, currentRef, &vgId);
      QUERY_CHECK_CODE(code, line, _return);
      code = dynPutResolvedColRef(pVtbScan, pRootRef->colRef, currentRef, vgId, NULL);
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
  SHashObj* pIterHash = NULL;
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

  pIterHash = pPendingMap;
  while ((pIter = taosHashIterate(pPendingMap, pIter)) != NULL) {
    size_t      keyLen = 0;
    char*       rootRef = taosHashGetKey(pIter, &keyLen);
    SDynRefKey*  pCurrentRef = (SDynRefKey*)pIter;
    SDynRefEdge* pEdge = NULL;
    SDynRefEdge  terminalEdge = {0};

    pEdge = (SDynRefEdge*)taosHashGet(pNextRefMap, pCurrentRef->colRef, strlen(pCurrentRef->colRef));
    if (pEdge != NULL) {
      code = taosHashPut(pEdgeMap, pCurrentRef->colRef, strlen(pCurrentRef->colRef), pEdge, sizeof(*pEdge));
      QUERY_CHECK_CODE(code, line, _return);

      if (!dynIsExternalColRefName(pEdge->nextRef.colRef)) {
        code = taosHashPut(pNextPending, rootRef, keyLen, &pEdge->nextRef, sizeof(pEdge->nextRef));
        QUERY_CHECK_CODE(code, line, _return);
      }
    } else {
      // A returned virtual column with a NULL source is an unbound leaf, not a raw table reference.
      terminalEdge.isNullRef = taosHashGet(pHitMap, pCurrentRef->colRef, strlen(pCurrentRef->colRef)) != NULL;
      QUERY_CHECK_CONDITION(!terminalEdge.isNullRef || NULL == pOperator->pTaskInfo->pStreamRuntimeInfo,
                            code, line, _return, TSDB_CODE_VTABLE_INVALID_REF_COLUMN);
      code = taosHashPut(pEdgeMap, pCurrentRef->colRef, strlen(pCurrentRef->colRef), &terminalEdge,
                         sizeof(terminalEdge));
      QUERY_CHECK_CODE(code, line, _return);
    }
  }

  taosHashClear(pPendingMap);

  pIterHash = pNextPending;
  pIter = taosHashIterate(pNextPending, NULL);
  while (pIter != NULL) {
    size_t keyLen = 0;
    char*  key = taosHashGetKey(pIter, &keyLen);
    code = taosHashPut(pPendingMap, key, keyLen, pIter, sizeof(SDynRefKey));
    QUERY_CHECK_CODE(code, line, _return);
    pIter = taosHashIterate(pNextPending, pIter);
  }

_return:
  taosHashCancelIterate(pIterHash, pIter);
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
  SHashObj* pEdgeMap = NULL;  // key: currentRef, value: SDynRefEdge; empty nextRef means terminal raw ref

  pEdgeMap = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  QUERY_CHECK_NULL(pEdgeMap, code, line, _return, terrno);

  for (int32_t depth = 0; taosHashGetSize(pPendingMap) > 0; ++depth) {
    if (depth >= TSDB_MAX_VTABLE_REF_DEPTH) {
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

  if (dynIsExternalColRefName(rootRef)) {
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
    QUERY_CHECK_NULL(output, code, line, _return, terrno);
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

static int32_t getDbPrecision(SOperatorInfo* pOperator, const char* dbName, uint8_t* pPrecision) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    lino = 0;
  SDynQueryCtrlOperatorInfo* pInfo = NULL;
  SName                      name = {0};
  SDBVgInfo*                 dbVgInfo = NULL;

  QUERY_CHECK_NULL(pOperator, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(dbName, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pPrecision, code, lino, _return, TSDB_CODE_INVALID_PARA);
  pInfo = pOperator->info;
  QUERY_CHECK_NULL(pInfo, code, lino, _return, TSDB_CODE_INVALID_PARA);
  tNameSetDbName(&name, pInfo->vtbScan.acctId, dbName, strlen(dbName));
  code = getDbVgInfo(pOperator, &name, &dbVgInfo);
  QUERY_CHECK_CODE(code, lino, _return);

  SUseDbOutput** pOutput =
      (SUseDbOutput**)taosHashGet(pInfo->vtbScan.dbVgInfoMap, dbName, strlen(dbName));
  QUERY_CHECK_NULL(pOutput, code, lino, _return, TSDB_CODE_FAILED);
  QUERY_CHECK_NULL(*pOutput, code, lino, _return, TSDB_CODE_FAILED);
  *pPrecision = (uint8_t)(*pOutput)->precision;
  return code;

_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
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
  secondDot = (firstDot == NULL) ? NULL : strchr(firstDot + 1, '.');
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
  if (pView->dbNameLen == 0 || pView->dbNameLen >= TSDB_DB_NAME_LEN || pView->tbNameLen == 0 ||
      pView->tbNameLen >= TSDB_TABLE_NAME_LEN || pView->colNameLen == 0 || pView->colNameLen >= TSDB_COL_NAME_LEN) {
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

static bool dynIsExternalColRefName(const char* colRef) {
  if (colRef == NULL) {
    return false;
  }

  int32_t dotCount = 0;
  for (const char* p = colRef; *p; ++p) {
    if (*p == '.') {
      ++dotCount;
      if (dotCount >= 3) {
        return true;
      }
    }
  }

  return false;
}

/*
 * Resolve the endpoint and database name of the vnode hosting a virtual table
 * from its vgId. Reference chains may cross databases, so all database-vgroup
 * mappings loaded while resolving the chain are searched.
 *
 * @param pOperator Dyn query ctrl operator.
 * @param vgId Target vnode group id.
 * @param pEpSet    output epSet
 * @param dbFName   output full database name
 * @param dbNameLen dbFName buffer size
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise error code.
 */
static int32_t getEpSetByVgId(SOperatorInfo* pOperator, int32_t vgId, SEpSet* pEpSet, char* dbFName,
                              int32_t dbNameLen) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  SDBVgInfo*                 dbVgInfo = NULL;
  SName                      name = {0};
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = (SVtbScanDynCtrlInfo*)&pInfo->vtbScan;
  void*                      pIter = NULL;

  QUERY_CHECK_NULL(pEpSet, code, line, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(dbFName, code, line, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(dbNameLen > 0, code, line, _return, TSDB_CODE_INVALID_PARA);

  toName(pVtbScan->acctId, pVtbScan->dbName, "", &name);
  code = getDbVgInfo(pOperator, &name, &dbVgInfo);
  QUERY_CHECK_CODE(code, line, _return);

  SVgroupInfo* pCurrentVg = taosHashGet(dbVgInfo->vgHash, &vgId, sizeof(vgId));
  if (pCurrentVg != NULL) {
    size_t keyLen = strlen(name.dbname);
    QUERY_CHECK_CONDITION(keyLen < (size_t)dbNameLen, code, line, _return, TSDB_CODE_INVALID_PARA);
    memcpy(dbFName, name.dbname, keyLen + 1);
    *pEpSet = pCurrentVg->epSet;
    goto _return;
  }

  while ((pIter = taosHashIterate(pVtbScan->dbVgInfoMap, pIter)) != NULL) {
    size_t         keyLen = 0;
    const char*    pDbFName = taosHashGetKey(pIter, &keyLen);
    SUseDbOutput*  pOutput = *(SUseDbOutput**)pIter;
    SVgroupInfo*   pVg = NULL;

    QUERY_CHECK_NULL(pDbFName, code, line, _return, TSDB_CODE_APP_ERROR);
    QUERY_CHECK_NULL(pOutput, code, line, _return, TSDB_CODE_APP_ERROR);
    QUERY_CHECK_NULL(pOutput->dbVgroup, code, line, _return, TSDB_CODE_APP_ERROR);
    pVg = taosHashGet(pOutput->dbVgroup->vgHash, &vgId, sizeof(vgId));
    if (pVg == NULL) {
      continue;
    }

    QUERY_CHECK_CONDITION(keyLen < (size_t)dbNameLen, code, line, _return, TSDB_CODE_INVALID_PARA);
    memcpy(dbFName, pDbFName, keyLen);
    dbFName[keyLen] = '\0';
    *pEpSet = pVg->epSet;
    goto _return;
  }

  QUERY_CHECK_CODE(code = TSDB_CODE_TSC_INVALID_INPUT, line, _return);

_return:
  taosHashCancelIterate(pVtbScan->dbVgInfoMap, pIter);
  if (code) {
    qError("%s failed at line %d since %s", __func__, line, tstrerror(code));
  }
  return code;
}

static void destroyDynLeafCondTargets(void* pValue) {
  SArray* pTargets = *(SArray**)pValue;
  taosArrayDestroy(pTargets);
}

static int32_t dynGetLeafVtbKey(SOperatorInfo* pOperator, SColRefInfo* pColRef, SDynLeafVtbKey* pKey,
                                col_id_t* pLeafColId, bool* pIsExternal) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             line = 0;
  SDynResolvedColRef* pResolved = NULL;

  QUERY_CHECK_NULL(pColRef, code, line, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pKey, code, line, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pLeafColId, code, line, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pIsExternal, code, line, _return, TSDB_CODE_INVALID_PARA);

  *pKey = (SDynLeafVtbKey){0};
  *pLeafColId = 0;
  *pIsExternal = false;
  if (pColRef->colrefName == NULL) {
    return code;
  }

  if (pColRef->extRefType == 1) {
    pKey->uid = pColRef->uid;
    pKey->vgId = pColRef->vgId;
    *pLeafColId = pColRef->colId;
  } else {
    code = dynResolveFinalColRef(pOperator, pColRef->colrefName, &pResolved);
    QUERY_CHECK_CODE(code, line, _return);
    if (pResolved->extRefType != 1) {
      return code;
    }
    pKey->uid = pResolved->leafUid;
    pKey->vgId = pResolved->leafVgId;
    *pLeafColId = pResolved->leafColId;
  }

  QUERY_CHECK_CONDITION(pKey->uid != 0 && pKey->vgId > 0, code, line, _return,
                        TSDB_CODE_VTABLE_INVALID_REF_COLUMN);
  *pIsExternal = true;
  return code;

_return:
  qError("%s failed at line %d since %s", __func__, line, tstrerror(code));
  return code;
}

static int32_t dynAddLeafCondTarget(SHashObj* pGroups, const SDynLeafVtbKey* pKey,
                                    const SDynLeafCondTarget* pTarget) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  line = 0;
  SArray*  pNewTargets = NULL;
  SArray** ppTargets = (SArray**)taosHashGet(pGroups, pKey, sizeof(*pKey));

  if (ppTargets == NULL) {
    pNewTargets = taosArrayInit(1, sizeof(SDynLeafCondTarget));
    QUERY_CHECK_NULL(pNewTargets, code, line, _return, terrno);
    code = taosHashPut(pGroups, pKey, sizeof(*pKey), &pNewTargets, POINTER_BYTES);
    QUERY_CHECK_CODE(code, line, _return);
    pNewTargets = NULL;
    ppTargets = (SArray**)taosHashGet(pGroups, pKey, sizeof(*pKey));
    QUERY_CHECK_NULL(ppTargets, code, line, _return, TSDB_CODE_APP_ERROR);
  }

  QUERY_CHECK_NULL(taosArrayPush(*ppTargets, pTarget), code, line, _return, terrno);
  return code;

_return:
  taosArrayDestroy(pNewTargets);
  qError("%s failed at line %d since %s", __func__, line, tstrerror(code));
  return code;
}

static int32_t dynBuildLeafCondGroups(SOperatorInfo* pOperator, SHashObj** ppGroups) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SHashObj*                  pGroups = NULL;

  QUERY_CHECK_NULL(ppGroups, code, line, _return, TSDB_CODE_INVALID_PARA);
  *ppGroups = NULL;
  pGroups = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  QUERY_CHECK_NULL(pGroups, code, line, _return, terrno);
  taosHashSetFreeFp(pGroups, destroyDynLeafCondTargets);

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->vtbScan.childTableList); ++i) {
    SArray* pColRefs = taosArrayGetP(pInfo->vtbScan.childTableList, i);
    QUERY_CHECK_NULL(pColRefs, code, line, _return, TSDB_CODE_APP_ERROR);

    for (int32_t j = 0; j < taosArrayGetSize(pColRefs); ++j) {
      SColRefInfo*       pColRef = taosArrayGet(pColRefs, j);
      SDynLeafVtbKey     key = {0};
      SDynLeafCondTarget target = {0};
      bool               isExternal = false;

      QUERY_CHECK_NULL(pColRef, code, line, _return, TSDB_CODE_APP_ERROR);
      if (!colNeedScan(pOperator, pColRef->colId) || pColRef->colrefName == NULL) {
        continue;
      }

      code = dynGetLeafVtbKey(pOperator, pColRef, &key, &target.leafColId, &isExternal);
      QUERY_CHECK_CODE(code, line, _return);
      if (!isExternal) {
        continue;
      }

      target.pColRef = pColRef;
      code = dynAddLeafCondTarget(pGroups, &key, &target);
      QUERY_CHECK_CODE(code, line, _return);
    }
  }

  *ppGroups = pGroups;
  pGroups = NULL;

_return:
  taosHashCleanup(pGroups);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, line, tstrerror(code));
  }
  return code;
}

static int32_t dynReplaceCondJson(char** ppDst, int32_t* pDstLen, const char* pSrc, int32_t srcLen) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t line = 0;
  char*   pCopy = NULL;

  QUERY_CHECK_NULL(ppDst, code, line, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pDstLen, code, line, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(srcLen >= 0 && (srcLen == 0 || pSrc != NULL), code, line, _return,
                        TSDB_CODE_INVALID_MSG);

  if (srcLen > 0) {
    pCopy = taosMemoryMalloc((size_t)srcLen + 1);
    QUERY_CHECK_NULL(pCopy, code, line, _return, terrno);
    memcpy(pCopy, pSrc, srcLen);
    pCopy[srcLen] = '\0';
  }

  taosMemoryFreeClear(*ppDst);
  *ppDst = pCopy;
  *pDstLen = srcLen;
  return code;

_return:
  taosMemoryFree(pCopy);
  qError("%s failed at line %d since %s", __func__, line, tstrerror(code));
  return code;
}

static int32_t dynBackfillLeafConds(const SVTagCondRsp* pRsp, SArray* pTargets) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t line = 0;

  QUERY_CHECK_NULL(pRsp, code, line, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pTargets, code, line, _return, TSDB_CODE_INVALID_PARA);

  for (int32_t i = 0; i < taosArrayGetSize(pRsp->pEntries); ++i) {
    SVTagCondEntry* pEntry = taosArrayGet(pRsp->pEntries, i);
    QUERY_CHECK_NULL(pEntry, code, line, _return, TSDB_CODE_INVALID_MSG);
    if (pEntry->tagCondLen <= 0 || pEntry->tagCondJson == NULL) {
      continue;
    }
    for (int32_t j = 0; j < taosArrayGetSize(pTargets); ++j) {
      SDynLeafCondTarget* pTarget = taosArrayGet(pTargets, j);
      QUERY_CHECK_NULL(pTarget, code, line, _return, TSDB_CODE_APP_ERROR);
      if (pTarget->leafColId == pEntry->colId) {
        code = dynReplaceCondJson(&pTarget->pColRef->tagCondJson, &pTarget->pColRef->tagCondLen,
                                  pEntry->tagCondJson, pEntry->tagCondLen);
        QUERY_CHECK_CODE(code, line, _return);
      }
    }
  }

  for (int32_t i = 0; i < taosArrayGetSize(pRsp->pFilterConds); ++i) {
    SVtbColFilterCond* pCond = taosArrayGet(pRsp->pFilterConds, i);
    QUERY_CHECK_NULL(pCond, code, line, _return, TSDB_CODE_INVALID_MSG);
    if (pCond->filterCondLen <= 0 || pCond->filterCondJson == NULL) {
      continue;
    }
    for (int32_t j = 0; j < taosArrayGetSize(pTargets); ++j) {
      SDynLeafCondTarget* pTarget = taosArrayGet(pTargets, j);
      QUERY_CHECK_NULL(pTarget, code, line, _return, TSDB_CODE_APP_ERROR);
      if (pTarget->leafColId == pCond->colId) {
        code = dynReplaceCondJson(&pTarget->pColRef->filterCondJson, &pTarget->pColRef->filterCondLen,
                                  pCond->filterCondJson, pCond->filterCondLen);
        QUERY_CHECK_CODE(code, line, _return);
      }
    }
  }

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, line, tstrerror(code));
  }
  return code;
}

static int32_t fetchVtbCondGroup(SOperatorInfo* pOperator, const SDynLeafVtbKey* pKey, SArray* pTargets) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  void*                      pBuf = NULL;
  SMsgSendInfo*              pMsgSendInfo = NULL;
  SEpSet                     epSet = {0};
  SVTagCondReq               req = {0};
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = &pInfo->vtbScan;
  SExecTaskInfo*             pTaskInfo = pOperator->pTaskInfo;

  QUERY_CHECK_NULL(pKey, code, line, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pTargets, code, line, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(pVtbScan->pTagCondRsp == NULL, code, line, _return, TSDB_CODE_APP_ERROR);

  code = getEpSetByVgId(pOperator, pKey->vgId, &epSet, req.dbFName, sizeof(req.dbFName));
  QUERY_CHECK_CODE(code, line, _return);

  req.uid = pKey->uid;
  req.header.vgId = pKey->vgId;

  int32_t contLen = tSerializeSVTagCondReq(NULL, 0, &req);
  if (contLen < 0) {
    QUERY_CHECK_CODE(code = contLen, line, _return);
  }
  pBuf = taosMemoryCalloc(1, contLen);
  QUERY_CHECK_NULL(pBuf, code, line, _return, terrno);
  int32_t encodedLen = tSerializeSVTagCondReq(pBuf, contLen, &req);
  if (encodedLen < 0) {
    QUERY_CHECK_CODE(code = encodedLen, line, _return);
  }
  QUERY_CHECK_CONDITION(encodedLen == contLen, code, line, _return, TSDB_CODE_INVALID_MSG);

  pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  QUERY_CHECK_NULL(pMsgSendInfo, code, line, _return, terrno);
  pMsgSendInfo->param = pOperator;
  pMsgSendInfo->msgInfo.pData = pBuf;
  pMsgSendInfo->msgInfo.len = contLen;
  pMsgSendInfo->msgType = TDMT_VND_VTB_TAG_COND;
  pMsgSendInfo->fp = dynProcessVTagCondRsp;
  pMsgSendInfo->requestId = pTaskInfo->id.queryId;
  pBuf = NULL;

  code = asyncSendMsgToServer(pVtbScan->pMsgCb->clientRpc, &epSet, NULL, pMsgSendInfo);
  QUERY_CHECK_CODE(code, line, _return);
  code = tsem_wait(&pVtbScan->ready);
  QUERY_CHECK_CODE(code, line, _return);
  if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
    QUERY_CHECK_CODE(code = pTaskInfo->code, line, _return);
  }

  QUERY_CHECK_NULL(pVtbScan->pTagCondRsp, code, line, _return, TSDB_CODE_INVALID_MSG);
  code = dynBackfillLeafConds(pVtbScan->pTagCondRsp, pTargets);
  QUERY_CHECK_CODE(code, line, _return);

_return:
  taosMemoryFree(pBuf);
  if (pVtbScan->pTagCondRsp != NULL) {
    tDestroySVTagCondRsp(pVtbScan->pTagCondRsp);
    taosMemoryFreeClear(pVtbScan->pTagCondRsp);
  }
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, line, tstrerror(code));
  }
  return code;
}

/* Fetch each distinct leaf virtual table once and back-fill its stored conditions into outer columns. */
static int32_t fetchVtbColConds(SOperatorInfo* pOperator) {
  int32_t   code = TSDB_CODE_SUCCESS;
  int32_t   line = 0;
  SHashObj* pGroups = NULL;
  void*     pIter = NULL;

  code = dynBuildLeafCondGroups(pOperator, &pGroups);
  QUERY_CHECK_CODE(code, line, _return);

  while ((pIter = taosHashIterate(pGroups, pIter)) != NULL) {
    size_t              keyLen = 0;
    SDynLeafVtbKey*     pKey = taosHashGetKey(pIter, &keyLen);
    SArray*             pTargets = *(SArray**)pIter;
    QUERY_CHECK_NULL(pKey, code, line, _return, TSDB_CODE_APP_ERROR);
    QUERY_CHECK_CONDITION(keyLen == sizeof(*pKey), code, line, _return, TSDB_CODE_APP_ERROR);
    QUERY_CHECK_NULL(pTargets, code, line, _return, TSDB_CODE_APP_ERROR);

    code = fetchVtbCondGroup(pOperator, pKey, pTargets);
    QUERY_CHECK_CODE(code, line, _return);
  }

_return:
  taosHashCancelIterate(pGroups, pIter);
  taosHashCleanup(pGroups);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, line, tstrerror(code));
  }
  return code;
}

int32_t extractExtColRefName(const char *colref, char **refSource, char **refDb, char** refTb, char** refCol) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     line = 0;

  *refSource = NULL;

  const char *dot1 = strchr(colref, '.');
  QUERY_CHECK_NULL(dot1, code, line, _return, terrno)

  const char *dot2 = strchr(dot1 + 1, '.');
  QUERY_CHECK_NULL(dot2, code, line, _return, terrno)

  const char *dot3 = strchr(dot2 + 1, '.');

  if (dot3) {
    // 4-part: source.db.table.col
    size_t src_len = dot1 - colref;
    size_t db_len = dot2 - dot1 - 1;
    size_t tb_len = dot3 - dot2 - 1;
    size_t col_len = strlen(dot3 + 1);

    *refSource = taosMemoryMalloc(src_len + 1);
    *refDb = taosMemoryMalloc(db_len + 1);
    *refTb = taosMemoryMalloc(tb_len + 1);
    *refCol = taosMemoryMalloc(col_len + 1);
    QUERY_CHECK_NULL(*refSource, code, line, _return, terrno)
    QUERY_CHECK_NULL(*refDb, code, line, _return, terrno)
    QUERY_CHECK_NULL(*refTb, code, line, _return, terrno)
    QUERY_CHECK_NULL(*refCol, code, line, _return, terrno)

    tstrncpy(*refSource, colref, src_len + 1);
    tstrncpy(*refDb, dot1 + 1, db_len + 1);
    tstrncpy(*refTb, dot2 + 1, tb_len + 1);
    tstrncpy(*refCol, dot3 + 1, col_len + 1);
  } else {
    // 3-part: db.table.col
    size_t db_len = dot1 - colref;
    size_t table_len = dot2 - dot1 - 1;
    size_t col_len = strlen(dot2 + 1);

    *refDb = taosMemoryMalloc(db_len + 1);
    *refTb = taosMemoryMalloc(table_len + 1);
    *refCol = taosMemoryMalloc(col_len + 1);
    QUERY_CHECK_NULL(*refDb, code, line, _return, terrno)
    QUERY_CHECK_NULL(*refTb, code, line, _return, terrno)
    QUERY_CHECK_NULL(*refCol, code, line, _return, terrno)

    tstrncpy(*refDb, colref, db_len + 1);
    tstrncpy(*refTb, dot1 + 1, table_len + 1);
    tstrncpy(*refCol, dot2 + 1, col_len + 1);
  }

  return TSDB_CODE_SUCCESS;
_return:
  qError("%s failed at line %d since %s", __func__, line, tstrerror(code));
  if (*refSource) {
    taosMemoryFree(*refSource);
    *refSource = NULL;
  }
  if (*refDb) {
    taosMemoryFree(*refDb);
    *refDb = NULL;
  }
  if (*refTb) {
    taosMemoryFree(*refTb);
    *refTb = NULL;
  }
  if (*refCol) {
    taosMemoryFree(*refCol);
    *refCol = NULL;
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

  SColumnInfoData *pColNameCol = taosArrayGet(pDataBlock, VC_COL_COL_NAME);
  SColumnInfoData *pUidCol = taosArrayGet(pDataBlock, VC_COL_UID);
  SColumnInfoData *pColIdCol = taosArrayGet(pDataBlock, VC_COL_COL_ID);
  SColumnInfoData *pRefCol = taosArrayGet(pDataBlock, VC_COL_COL_SOURCE);
  SColumnInfoData *pVgIdCol = taosArrayGet(pDataBlock, VC_COL_VGROUP_ID);
  SColumnInfoData *pRefVerCol = taosArrayGet(pDataBlock, VC_COL_REF_VERSION);
  SColumnInfoData *pRefTypeCol = taosArrayGetSize(pDataBlock) > VC_COL_COL_TYPE ? taosArrayGet(pDataBlock, VC_COL_COL_TYPE) : NULL;

  QUERY_CHECK_NULL(pColNameCol, code, line, _return, terrno)
  QUERY_CHECK_NULL(pUidCol, code, line, _return, terrno)
  QUERY_CHECK_NULL(pColIdCol, code, line, _return, terrno)
  QUERY_CHECK_NULL(pRefCol, code, line, _return, terrno)
  QUERY_CHECK_NULL(pVgIdCol, code, line, _return, terrno)
  QUERY_CHECK_NULL(pRefVerCol, code, line, _return, terrno)

  if (colDataIsNull_s(pRefCol, index)) {
    pInfo->colrefName = NULL;
    pInfo->refType = 0;
    pInfo->extRefType = 0;
  } else {
    void* data = colDataGetData(pRefCol, index);
    pInfo->colrefName = taosMemoryCalloc(varDataTLen(data), 1);
    QUERY_CHECK_NULL(pInfo->colrefName, code, line, _return, terrno)
    memcpy(pInfo->colrefName, varDataVal(data), varDataLen(data));
    pInfo->colrefName[varDataLen(data)] = 0;
    int dotCount = 0;
    for (const char *p = pInfo->colrefName; *p; p++) {
      if (*p == '.') dotCount++;
    }
    pInfo->extRefType = (dotCount >= 3) ? 1 : 0;
  }

  if (colDataIsNull_s(pColNameCol, index)) {
    pInfo->colName = NULL;
  } else {
    void* data = colDataGetData(pColNameCol, index);
    pInfo->colName = taosMemoryCalloc(varDataTLen(data), 1);
    QUERY_CHECK_NULL(pInfo->colName, code, line, _return, terrno)
    memcpy(pInfo->colName, varDataVal(data), varDataLen(data));
    pInfo->colName[varDataLen(data)] = 0;
  }

  if (!colDataIsNull_s(pUidCol, index)) {
    GET_TYPED_DATA(pInfo->uid, int64_t, TSDB_DATA_TYPE_BIGINT, colDataGetNumData(pUidCol, index), 0);
  }
  if (!colDataIsNull_s(pColIdCol, index)) {
    GET_TYPED_DATA(pInfo->colId, int32_t, TSDB_DATA_TYPE_INT, colDataGetNumData(pColIdCol, index), 0);
  }
  if (!colDataIsNull_s(pVgIdCol, index)) {
    GET_TYPED_DATA(pInfo->vgId, int32_t, TSDB_DATA_TYPE_INT, colDataGetNumData(pVgIdCol, index), 0);
  }
  pInfo->refType = 0;
  if (pRefTypeCol != NULL && !colDataIsNull_s(pRefTypeCol, index)) {
    int32_t refType = 0;
    GET_TYPED_DATA(refType, int32_t, TSDB_DATA_TYPE_INT, colDataGetNumData(pRefTypeCol, index), 0);
    pInfo->refType = (int8_t)refType;
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
    if (pColRef->extRefType == 1) {
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
      if (pColRef->extRefType == 1) {
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
    uint64_t deployUid = pVtbScan->uid;
    if (pVtbScan->isSuperTable) {
      deployUid = pVtbScan->dynTbUid != 0 ? pVtbScan->dynTbUid : pVtbScan->suid;
    }
    atomic_store_64(&pTaskInfo->pStreamRuntimeInfo->vtableDeployInfo.uid, (int64_t)deployUid);
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

static int32_t generateTagArrayByTagBlockAndSave(SHashObj* vtbUidTagListMap, tb_uid_t uid, SSDataBlock* pTagVal,
                                                 int32_t rowIdx, const SArray* pLogicalTagColIds) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  STagVal tagVal = {0};
  SArray* pTagList = NULL;
  // last col is uid

  QUERY_CHECK_NULL(vtbUidTagListMap, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pTagVal, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pTagVal->pDataBlock, code, lino, _return, TSDB_CODE_INVALID_PARA);

  pTagList = taosArrayInit(1, sizeof(STagVal));
  QUERY_CHECK_NULL(pTagList, code, lino, _return, terrno)

  int32_t numTagCols = taosArrayGetSize(pTagVal->pDataBlock) - 1;
  QUERY_CHECK_CONDITION(NULL == pLogicalTagColIds || taosArrayGetSize(pLogicalTagColIds) == numTagCols,
                        code, lino, _return, TSDB_CODE_PLAN_SLOT_NOT_FOUND);

  for (int32_t k = 0; k < numTagCols; k++) {
    SColumnInfoData* pTagCol = taosArrayGet(pTagVal->pDataBlock, k);
    QUERY_CHECK_NULL(pTagCol, code, lino, _return, terrno)
    tagVal.type = pTagCol->info.type;
    if (NULL != pLogicalTagColIds) {
      col_id_t* pTagColId = taosArrayGet(pLogicalTagColIds, k);
      QUERY_CHECK_NULL(pTagColId, code, lino, _return, terrno);
      tagVal.cid = *pTagColId;
    } else {
      tagVal.cid = pTagCol->info.colId;
    }
    if (!colDataIsNull_s(pTagCol, rowIdx)) {
      char*   pData = colDataGetData(pTagCol, rowIdx);
      if (IS_VAR_DATA_TYPE(pTagCol->info.type)) {
        tagVal.nData = varDataLen(pData);
        tagVal.pData = taosMemoryMalloc(TMAX(1, tagVal.nData));
        QUERY_CHECK_NULL(tagVal.pData, code, lino, _return, terrno)
        if (tagVal.nData > 0) {
          memcpy(tagVal.pData, varDataVal(pData), tagVal.nData);
        }
        QUERY_CHECK_NULL(taosArrayPush(pTagList, &tagVal), code, lino, _return, terrno)
      } else {
        memcpy(&tagVal.i64, pData, tDataTypes[pTagCol->info.type].bytes);
        QUERY_CHECK_NULL(taosArrayPush(pTagList, &tagVal), code, lino, _return, terrno)
      }
    } else {
      tagVal.pData = NULL;
      // Keep the fixed-width NULL marker used by setTagValFromTagList().
      // Variable-width NULLs are identified by pData == NULL.
      tagVal.nData = (uint32_t)-1;
      QUERY_CHECK_NULL(taosArrayPush(pTagList, &tagVal), code, lino, _return, terrno)
    }
    tagVal = (STagVal){0};
  }
  code = taosHashPut(vtbUidTagListMap, &uid, sizeof(uid), &pTagList, POINTER_BYTES);
  QUERY_CHECK_CODE(code, lino, _return);

  return code;
_return:
  if (tagVal.pData) {
    taosMemoryFreeClear(tagVal.pData);
  }
  if (pTagList) {
    taosArrayDestroyEx(pTagList, destroyTagVal);
  }
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

static int32_t buildForeignSourceKey(const SColRefInfo* pColRef, const char* sourceName,
                                     const char* dbName, const char* tableName,
                                     uint8_t** ppKey, int32_t* pKeyLen) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  line = 0;
  uint8_t* pKey = NULL;

  QUERY_CHECK_NULL(pColRef, code, line, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(sourceName, code, line, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(dbName, code, line, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(tableName, code, line, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(ppKey, code, line, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pKeyLen, code, line, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(pColRef->filterCondLen >= 0 &&
                            (pColRef->filterCondLen == 0 || pColRef->filterCondJson != NULL) &&
                            pColRef->tagCondLen >= 0 &&
                            (pColRef->tagCondLen == 0 || pColRef->tagCondJson != NULL),
                        code, line, _return, TSDB_CODE_INVALID_MSG);
  *ppKey = NULL;
  *pKeyLen = 0;

  uint32_t sourceNameLen = (uint32_t)strlen(sourceName);
  uint32_t dbNameLen = (uint32_t)strlen(dbName);
  uint32_t tableNameLen = (uint32_t)strlen(tableName);
  size_t keySize = sizeof(pColRef->uid) + sizeof(sourceNameLen) + sourceNameLen +
                   sizeof(dbNameLen) + dbNameLen + sizeof(tableNameLen) + tableNameLen +
                   sizeof(pColRef->filterCondLen) + (size_t)pColRef->filterCondLen +
                   sizeof(pColRef->tagCondLen) + (size_t)pColRef->tagCondLen;
  QUERY_CHECK_CONDITION(keySize <= INT32_MAX, code, line, _return, TSDB_CODE_INVALID_PARA);

  pKey = taosMemoryMalloc(keySize);
  QUERY_CHECK_NULL(pKey, code, line, _return, terrno);

  size_t offset = 0;
  TAOS_MEMCPY(pKey + offset, &pColRef->uid, sizeof(pColRef->uid));
  offset += sizeof(pColRef->uid);
  TAOS_MEMCPY(pKey + offset, &sourceNameLen, sizeof(sourceNameLen));
  offset += sizeof(sourceNameLen);
  TAOS_MEMCPY(pKey + offset, sourceName, sourceNameLen);
  offset += sourceNameLen;
  TAOS_MEMCPY(pKey + offset, &dbNameLen, sizeof(dbNameLen));
  offset += sizeof(dbNameLen);
  TAOS_MEMCPY(pKey + offset, dbName, dbNameLen);
  offset += dbNameLen;
  TAOS_MEMCPY(pKey + offset, &tableNameLen, sizeof(tableNameLen));
  offset += sizeof(tableNameLen);
  TAOS_MEMCPY(pKey + offset, tableName, tableNameLen);
  offset += tableNameLen;
  TAOS_MEMCPY(pKey + offset, &pColRef->filterCondLen, sizeof(pColRef->filterCondLen));
  offset += sizeof(pColRef->filterCondLen);
  if (pColRef->filterCondLen > 0) {
    TAOS_MEMCPY(pKey + offset, pColRef->filterCondJson, pColRef->filterCondLen);
    offset += pColRef->filterCondLen;
  }
  TAOS_MEMCPY(pKey + offset, &pColRef->tagCondLen, sizeof(pColRef->tagCondLen));
  offset += sizeof(pColRef->tagCondLen);
  if (pColRef->tagCondLen > 0) {
    TAOS_MEMCPY(pKey + offset, pColRef->tagCondJson, pColRef->tagCondLen);
    offset += pColRef->tagCondLen;
  }
  QUERY_CHECK_CONDITION(offset == keySize, code, line, _return, TSDB_CODE_APP_ERROR);

  *ppKey = pKey;
  *pKeyLen = (int32_t)keySize;
  return code;

_return:
  taosMemoryFree(pKey);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
  }
  return code;
}

static const SFederatedScanPhysiNode* dynFindForeignScan(const SPhysiNode* pNode, const char* sourceName) {
  if (NULL == pNode) {
    return NULL;
  }
  if (QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN == nodeType(pNode)) {
    const SFederatedScanPhysiNode* pScan = (const SFederatedScanPhysiNode*)pNode;
    const SExtTableNode*           pTable = (const SExtTableNode*)pScan->pExtTable;
    if (NULL != pTable && 0 == strcasecmp(pTable->sourceName, sourceName)) {
      return pScan;
    }
  }
  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    const SFederatedScanPhysiNode* pScan = dynFindForeignScan((const SPhysiNode*)pChild, sourceName);
    if (NULL != pScan) {
      return pScan;
    }
  }
  return NULL;
}

static void dynDestroyExtTableMeta(void* pValue) { extConnectorFreeTableSchema(*(SExtTableMeta**)pValue); }

static int32_t dynGetForeignTableMeta(SVtbScanDynCtrlInfo* pVtbScan, const SFederatedScanPhysiNode* pScan,
                                      const char* dbName, const char* tableName, SExtTableMeta** ppMeta) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  SExtConnectorHandle* pHandle = NULL;
  SExtTableMeta*       pMeta = NULL;
  SExtTableNode        table = *(const SExtTableNode*)pScan->pExtTable;
  SExtSourceCfg        cfg = {0};
  char key[TSDB_EXT_SOURCE_NAME_LEN + TSDB_DB_NAME_LEN + TSDB_TABLE_NAME_LEN] = {0};
  strntolower(key, table.sourceName, strlen(table.sourceName));
  strntolower(key + TSDB_EXT_SOURCE_NAME_LEN, dbName, strlen(dbName));
  strntolower(key + TSDB_EXT_SOURCE_NAME_LEN + TSDB_DB_NAME_LEN, tableName, strlen(tableName));

  if (NULL == pVtbScan->extTableMetaCache) {
    pVtbScan->extTableMetaCache =
        taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
    QUERY_CHECK_NULL(pVtbScan->extTableMetaCache, code, lino, _return, terrno);
    taosHashSetFreeFp(pVtbScan->extTableMetaCache, dynDestroyExtTableMeta);
  }
  SExtTableMeta** ppCached = taosHashGet(pVtbScan->extTableMetaCache, key, sizeof(key));
  if (NULL != ppCached) {
    *ppMeta = *ppCached;
    return code;
  }

  cfg.source_type = (EExtSourceType)pScan->sourceType;
  cfg.port = pScan->srcPort;
  cfg.meta_version = pScan->metaVersion;
  tstrncpy(cfg.source_name, table.sourceName, sizeof(cfg.source_name));
  tstrncpy(cfg.host, pScan->srcHost, sizeof(cfg.host));
  tstrncpy(cfg.user, pScan->srcUser, sizeof(cfg.user));
  tstrncpy(cfg.password, pScan->srcPassword, sizeof(cfg.password));
  tstrncpy(cfg.default_database, pScan->srcDatabase, sizeof(cfg.default_database));
  tstrncpy(cfg.default_schema, pScan->srcSchema, sizeof(cfg.default_schema));
  tstrncpy(cfg.options, pScan->srcOptions, sizeof(cfg.options));
  tstrncpy(table.table.tableName, tableName, sizeof(table.table.tableName));
  tstrncpy(table.table.dbName, dbName, sizeof(table.table.dbName));
  if (EXT_SOURCE_POSTGRESQL == cfg.source_type) {
    tstrncpy(table.schemaName, pScan->srcSchema, sizeof(table.schemaName));
  }
  code = extConnectorOpen(&cfg, &pHandle);
  QUERY_CHECK_CODE(code, lino, _return);
  code = extConnectorGetTableSchema(pHandle, &table, &pMeta);
  QUERY_CHECK_CODE(code, lino, _return);
  QUERY_CHECK_NULL(pMeta, code, lino, _return, TSDB_CODE_EXT_TABLE_NOT_EXIST);
  code = taosHashPut(pVtbScan->extTableMetaCache, key, sizeof(key), &pMeta, sizeof(pMeta));
  QUERY_CHECK_CODE(code, lino, _return);
  *ppMeta = pMeta;
  pMeta = NULL;

_return:
  extConnectorFreeTableSchema(pMeta);
  extConnectorClose(pHandle);
  if (TSDB_CODE_SUCCESS != code) {
    qError("failed to fetch external schema, source:%s, table:%s.%s, code:%s", table.sourceName, dbName, tableName,
           tstrerror(code));
  }
  return code;
}

static int32_t dynValidateForeignColRef(SOperatorInfo* pOperator, const SColRefInfo* pRef, const char* sourceName,
                                        const char* dbName, const char* tableName, const char* colName) {
  int32_t                        code = TSDB_CODE_SUCCESS;
  int32_t                        lino = 0;
  int32_t                        tsIdx = -1;
  char                           reason[256] = {0};
  timezone_t                     timezone = NULL;
  SExtTableMeta*                 pMeta = NULL;
  SDynQueryCtrlOperatorInfo*     pInfo = pOperator->info;
  const SFederatedScanPhysiNode* pScan = dynFindForeignScan((const SPhysiNode*)pOperator->pPhyNode, sourceName);
  QUERY_CHECK_NULL(pScan, code, lino, _return, TSDB_CODE_EXT_SOURCE_NOT_FOUND);
  EExtSourceType sourceType = (EExtSourceType)pScan->sourceType;
  if (EXT_SOURCE_MYSQL != sourceType && EXT_SOURCE_POSTGRESQL != sourceType) {
    QUERY_CHECK_CONDITION(0 == pRef->filterCondLen, code, lino, _return, TSDB_CODE_VTABLE_EXT_FILTER_NOT_SUPPORTED);
    return code;
  }
  code = dynGetForeignTableMeta(&pInfo->vtbScan, pScan, dbName, tableName, &pMeta);
  QUERY_CHECK_CODE(code, lino, _return);
  if (pRef->filterCondLen > 0) {
    timezone = tzalloc('\0' != pScan->timezone[0] ? pScan->timezone : NULL);
    QUERY_CHECK_NULL(timezone, code, lino, _return, terrno);
  }
  code = nodesValidateExternalFilterCurrentMeta(sourceType, pMeta, pRef->filterCondJson, pRef->filterCondLen, timezone,
                                                NULL, reason, sizeof(reason), &tsIdx);
  QUERY_CHECK_CODE(code, lino, _return);
  QUERY_CHECK_CONDITION(tsIdx >= 0 && tsIdx < pMeta->numOfCols, code, lino, _return, TSDB_CODE_EXT_NO_TS_PRIMARY_KEY);
  QUERY_CHECK_CONDITION(extTimestampTypePrecision(sourceType, pMeta->pCols[tsIdx].extTypeName, pMeta->tsPrecision) <=
                            pInfo->vtbScan.dstPrecision,
                        code, lino, _return, TSDB_CODE_EXT_SCHEMA_CHANGED);

  const SExtColumnDef* pExtCol = NULL;
  for (int32_t i = 0; i < pMeta->numOfCols; ++i) {
    if (0 == strcasecmp(pMeta->pCols[i].colName, colName)) {
      pExtCol = &pMeta->pCols[i];
      break;
    }
  }
  QUERY_CHECK_NULL(pExtCol, code, lino, _return, TSDB_CODE_EXT_SCHEMA_CHANGED);
  SDataType remoteType = {0};
  code = extTypeNameToTDengineType(sourceType, pExtCol->extTypeName, pExtCol->extCharsetName, &remoteType);
  if (TSDB_CODE_EXT_TYPE_NOT_MAPPABLE == code) {
    code = TSDB_CODE_EXT_SCHEMA_CHANGED;
  }
  QUERY_CHECK_CODE(code, lino, _return);
  const SDynQueryCtrlPhysiNode* pDynNode = (const SDynQueryCtrlPhysiNode*)pOperator->pPhyNode;
  SNode*                        pNode = NULL;
  FOREACH(pNode, pDynNode->vtbScan.pScanCols) {
    const SColumnNode* pCol = (const SColumnNode*)pNode;
    if (QUERY_NODE_COLUMN == nodeType(pNode) && pCol->colId == pRef->colId) {
      QUERY_CHECK_CONDITION(isSameRefDataType(&pCol->node.resType, &remoteType), code, lino, _return,
                            TSDB_CODE_EXT_SCHEMA_CHANGED);
      break;
    }
  }

_return:
  if (NULL != timezone) {
    tzfree(timezone);
  }
  if (TSDB_CODE_SUCCESS != code) {
    qError("QID:0x%" PRIx64 " external binding invalid, source:%s, table:%s.%s, column:%s, code:%s, reason:%s",
           pOperator->pTaskInfo->id.queryId, sourceName, dbName, tableName, colName, tstrerror(code), reason);
  }
  return code;
}

static int32_t addForeignSourceColRef(SOperatorInfo* pOperator, const SColRefInfo* pKV,
                                      const char* refSourceName, const char* refDbName, const char* refTbName,
                                      const char* refColName) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            line = 0;
  uint8_t*           foreignKey = NULL;
  int32_t            keyLen = 0;
  SForeignSourceInfo fsInfo = {0};
  SVtbScanDynCtrlInfo* pVtbScan = &((SDynQueryCtrlOperatorInfo*)pOperator->info)->vtbScan;

  if (NULL == pOperator->pTaskInfo->pStreamRuntimeInfo) {
    code = dynValidateForeignColRef(pOperator, pKV, refSourceName, refDbName, refTbName, refColName);
    QUERY_CHECK_CODE(code, line, _return);
  }

  code = buildForeignSourceKey(pKV, refSourceName, refDbName, refTbName, &foreignKey, &keyLen);
  QUERY_CHECK_CODE(code, line, _return);

  void* pVal = taosHashGet(pVtbScan->foreignSourceMap, foreignKey, keyLen);
  if (pVal == NULL) {
    tstrncpy(fsInfo.sourceName, refSourceName, sizeof(fsInfo.sourceName));
    tstrncpy(fsInfo.dbName, refDbName, sizeof(fsInfo.dbName));
    tstrncpy(fsInfo.tableName, refTbName, sizeof(fsInfo.tableName));
    fsInfo.virtualChildUid = pKV->uid;
    fsInfo.scanParamId = MurmurHash3_64((const char *)foreignKey, (uint32_t)keyLen);
    code = dynReplaceCondJson(&fsInfo.tagCond, &fsInfo.tagCondLen,
                              pKV->tagCondJson, pKV->tagCondLen);
    QUERY_CHECK_CODE(code, line, _return);
    code = dynReplaceCondJson(&fsInfo.filterCondJson, &fsInfo.filterCondLen,
                              pKV->filterCondJson, pKV->filterCondLen);
    QUERY_CHECK_CODE(code, line, _return);

    fsInfo.colMap = taosArrayInit(10, sizeof(SColIdNameKV));
    QUERY_CHECK_NULL(fsInfo.colMap, code, line, _return, terrno);

    SColIdNameKV colIdNameKV = {0};
    colIdNameKV.colId = pKV->colId;
    tstrncpy(colIdNameKV.colName, refColName, sizeof(colIdNameKV.colName));
    QUERY_CHECK_NULL(taosArrayPush(fsInfo.colMap, &colIdNameKV), code, line, _return, terrno);

    code = taosHashPut(pVtbScan->foreignSourceMap, foreignKey, keyLen, &fsInfo, sizeof(fsInfo));
    QUERY_CHECK_CODE(code, line, _return);
    fsInfo.colMap = NULL;
    fsInfo.tagCond = NULL;
    fsInfo.filterCondJson = NULL;
  } else {
    SForeignSourceInfo* pFsInfo = (SForeignSourceInfo*)pVal;
    SColIdNameKV        colIdNameKV = {0};
    colIdNameKV.colId = pKV->colId;
    tstrncpy(colIdNameKV.colName, refColName, sizeof(colIdNameKV.colName));
    QUERY_CHECK_NULL(taosArrayPush(pFsInfo->colMap, &colIdNameKV), code, line, _return, terrno);
  }

_return:
  destroyForeignSourceInfoInExecutor(&fsInfo);
  taosMemoryFree(foreignKey);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed since %s, line %d", __func__, tstrerror(code), line);
  }
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
    bool needResolve = colNeedScan(pOperator, pKV->colId);
    if (needResolve && pKV->refType == 1) {
      pVtbScan->useTagScan = true;
    }

    if (pKV->colrefName != NULL && needResolve) {
      SDynResolvedColRef* pResolved = NULL;
      SName               name = {0};
      char                orgTbFName[TSDB_TABLE_FNAME_LEN] = {0};

      // External column ref (source.db.table.col)  ->  federated foreign source map.
      // External refs cannot be resolved as internal col-refs, so handle them first.
      if (pKV->extRefType == 1) {
        char* refSourceName = NULL;
        char* refDbName = NULL;
        char* refTbName = NULL;
        char* refColName = NULL;
        code = extractExtColRefName(pKV->colrefName, &refSourceName, &refDbName, &refTbName, &refColName);
        QUERY_CHECK_CODE(code, line, _return);

        code = addForeignSourceColRef(pOperator, pKV, refSourceName, refDbName, refTbName, refColName);
        taosMemoryFree(refSourceName);
        taosMemoryFree(refDbName);
        taosMemoryFree(refTbName);
        taosMemoryFree(refColName);
        QUERY_CHECK_CODE(code, line, _return);
        continue;
      }

      code = dynResolveFinalColRef(pOperator, pKV->colrefName, &pResolved);
      QUERY_CHECK_CODE(code, line, _return);
      if ('\0' == pResolved->fullColRef[0]) {
        continue;
      }

      if (pResolved->extRefType == 1) {
        code = addForeignSourceColRef(pOperator, pKV, pResolved->sourceName, pResolved->dbName, pResolved->tbName,
                                      pResolved->colName);
        QUERY_CHECK_CODE(code, line, _return);
        continue;
      }

      if (ppRefMap != NULL && pKV->refType != 1) {
        // Only ordinary col-ref slots participate in virtual-scan ref-slot grouping.
        // TagRef is handled by TagRefSource operators and must not be merged/copied as row data.
        if (refMap == NULL) {
          refMap = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR), true, HASH_NO_LOCK);
          QUERY_CHECK_NULL(refMap, code, line, _return, terrno)
        }
        code = addRefColIdToRefMap(refMap, pResolved->fullColRef, pKV->colId);
        QUERY_CHECK_CODE(code, line, _return);
      }

      // TagRef columns get their values from the Tag Scan path (useTagScan=true set above),
      // not from the Table Scan exchange. Do not add tag-ref source tables to otbNameToOtbInfoMap
      // because that would cause the Table Scan to scan the tag-ref source's data rows, producing
      // extra NULL rows in the result.
      if (pKV->refType == 1) {
        continue;
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
        orgTbInfo.srcPrecision = UINT8_MAX;
        orgTbInfo.dstPrecision = UINT8_MAX;
        code = getDbPrecision(pOperator, pResolved->dbName, &orgTbInfo.srcPrecision);
        QUERY_CHECK_CODE(code, line, _return);
        if (NULL != pOperator->pPhyNode &&
            NULL != ((SPhysiNode*)pOperator->pPhyNode)->pOutputDataBlockDesc) {
          orgTbInfo.dstPrecision =
              ((SPhysiNode*)pOperator->pPhyNode)->pOutputDataBlockDesc->precision;
        }
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

  qDebug("vtable process col refs, db:%s tb:%s super:%d colRefs:%" PRId64 " uid:%" PRIu64 " vgId:%d otbSources:%d foreignSources:%d refGroups:%d",
         pVtbScan->dbName ? pVtbScan->dbName : "<null>", pVtbScan->tbName ? pVtbScan->tbName : "<null>",
         pVtbScan->isSuperTable, (int64_t)taosArrayGetSize(pColRefInfo), *uid, *vgId,
         pVtbScan->otbNameToOtbInfoMap ? taosHashGetSize(pVtbScan->otbNameToOtbInfoMap) : 0,
         pVtbScan->foreignSourceMap ? taosHashGetSize(pVtbScan->foreignSourceMap) : 0,
         refMap ? taosHashGetSize(refMap) : 0);

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

      code = generateTagArrayByTagBlockAndSave(
          vtbUidTagListMap, uid, pTagVal, i,
          DYN_QTYPE_VTB_AGG == pInfo->qType ? pVtbScan->aggTagColIds : NULL);
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

static int32_t filterChildTableListByTagRef(SVtbScanDynCtrlInfo* pVtbScan);
static int32_t evaluateTagRefFilter(SVtbScanDynCtrlInfo* pVtbScan, SArray* pResolvedTags, bool* pPasses);

int32_t buildVirtualSuperTableScanChildTableMap(SOperatorInfo* pOperator) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  SDynQueryCtrlOperatorInfo* pInfo = pOperator->info;
  SVtbScanDynCtrlInfo*       pVtbScan = (SVtbScanDynCtrlInfo*)&pInfo->vtbScan;
  SExecTaskInfo*             pTaskInfo = pOperator->pTaskInfo;
  SArray*                    pColRefArray = NULL;
  SOperatorInfo*             pSystableScanOp = NULL;
  bool                       sysScanBootstrapped = false;
  int32_t                    scannedRows = 0;
  int32_t                    matchedRows = 0;
  int32_t                    sampleRows = 0;
  char                       sampleBuf[512] = {0};

  taosHashClear(pVtbScan->resolvedColRefMap);
  pVtbScan->childTableMap = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  QUERY_CHECK_NULL(pVtbScan->childTableMap, code, line, _return, terrno)

  if (pInfo->qType == DYN_QTYPE_VTB_AGG || pInfo->qType == DYN_QTYPE_VTB_INTERVAL) {
    pVtbScan->otbNameToOtbInfoMap = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
    QUERY_CHECK_NULL(pVtbScan->otbNameToOtbInfoMap, code, line, _return, terrno)
    pVtbScan->foreignSourceMap = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
    QUERY_CHECK_NULL(pVtbScan->foreignSourceMap, code, line, _return, terrno)
    pSystableScanOp = pOperator->pDownstream[0];
  } else if (pInfo->qType == DYN_QTYPE_VTB_WINDOW || pInfo->qType == DYN_QTYPE_VTB_TS_SCAN) {
    pVtbScan->otbNameToOtbInfoMap = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
    QUERY_CHECK_NULL(pVtbScan->otbNameToOtbInfoMap, code, line, _return, terrno)
    pVtbScan->foreignSourceMap = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
    QUERY_CHECK_NULL(pVtbScan->foreignSourceMap, code, line, _return, terrno)
    pSystableScanOp = pOperator->pDownstream[1];
  } else {
    pSystableScanOp = pOperator->pDownstream[1];
  }

  qDebug("vtable build child map enter, db:%s tb:%s qType:%d super:%d systableOpType:%d downstreams:%d",
         pInfo->vtbScan.dbName, pInfo->vtbScan.tbName, pInfo->qType, pVtbScan->isSuperTable,
         pSystableScanOp ? pSystableScanOp->operatorType : -1, pOperator->numOfDownstream);

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
      ++scannedRows;
      // A virtual super table groups its child rows by stable name (column 1), so the
      // scan is filtered against that. A single (non-super) virtual table has no such
      // grouping: the scan returns the vtable's own column-ref rows whose stable_name
      // differs from the queried table name, so match on the table name (column 0)
      // instead. Without this, the non-super VTB_AGG/VTB_INTERVAL path collects no
      // child tables and the downstream agg exchange ends up with zero sources.
      SColumnInfoData* pMatchNameCol = pVtbScan->isSuperTable ? pStbNameCol : pTableNameCol;
      if (!colDataIsNull_s(pMatchNameCol, i)) {
        char* matchrawname = colDataGetData(pMatchNameCol, i);
        char* dbrawname = colDataGetData(pDbNameCol, i);
        char *ctbName = colDataGetData(pTableNameCol, i);
        bool matched = tableInfoNeedCollect(dbrawname, matchrawname, pInfo->vtbScan.dbName, pInfo->vtbScan.tbName);

        if (!matched && sampleRows < 4) {
          int32_t used = (int32_t)strlen(sampleBuf);
          if (used < (int32_t)sizeof(sampleBuf) - 1) {
            tsnprintf(sampleBuf + used, sizeof(sampleBuf) - used, "[db=%.*s match=%.*s table=%.*s] ",
                      varDataLen(dbrawname), varDataVal(dbrawname), varDataLen(matchrawname), varDataVal(matchrawname),
                      varDataLen(ctbName), varDataVal(ctbName));
            ++sampleRows;
          }
        }

        if (matched) {
          ++matchedRows;
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

  if (0 == taosArrayGetSize(pVtbScan->childTableList) && scannedRows > 0) {
    qDebug("vtable sysscan matched no child table, expect=%s.%s qType=%d super=%d scannedRows=%d samples=%s",
           pInfo->vtbScan.dbName, pInfo->vtbScan.tbName, pInfo->qType, pVtbScan->isSuperTable, scannedRows,
           sampleBuf[0] ? sampleBuf : "<empty>");
  } else {
    qDebug("vtable sysscan child table matched, expect=%s.%s qType=%d super=%d scannedRows=%d matchedRows=%d childTables=%" PRId64,
           pInfo->vtbScan.dbName, pInfo->vtbScan.tbName, pInfo->qType, pVtbScan->isSuperTable, scannedRows,
           matchedRows, (int64_t)taosArrayGetSize(pVtbScan->childTableList));
  }

  // Apply tag-ref filter optimization: remove virtual children that don't match
  if (pInfo->qType == DYN_QTYPE_VTB_SCAN) {
    code = filterChildTableListByTagRef(&pInfo->vtbScan);
    QUERY_CHECK_CODE(code, line, _return);
  }

  code = dynResolveChildTableRefsBySysScan(pOperator, pSystableScanOp);
  QUERY_CHECK_CODE(code, line, _return);

  code = fetchVtbColConds(pOperator);
  QUERY_CHECK_CODE(code, line, _return);

  if ((pInfo->qType == DYN_QTYPE_VTB_SCAN || pInfo->qType == DYN_QTYPE_VTB_AGG) &&
      pTaskInfo->pStreamRuntimeInfo != NULL) {
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
      // In a stream, the resolved original-table vgroups may change over time. Mirror the
      // VTB_SCAN path and check for redeploy *before* building org-table info, so an aborted
      // open does not leave the org-table maps half-populated for the subsequent re-open.
      if (pTaskInfo->pStreamRuntimeInfo != NULL) {
        code = processOrgTbVg(pVtbScan, pTaskInfo, 1);
        QUERY_CHECK_CODE(code, line, _return);
      }
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

  // Rebuild curOrgTbVg from runtime chain resolution so that processOrgTbVg
  // can diff it against the plan-time baseline existOrgTbVg and trigger
  // redeploy when the chain terminal vg differs from what the planner
  // recorded (e.g. a multi-hop cross-vgroup vtable chain whose terminal
  // physical table lives on a different vgroup than the planner assumed).
  //
  // Use curOrgTbVg == NULL as the "not yet runtime-verified" sentinel:
  // existOrgTbVg is always pre-populated from plan-time pOrgVgIds at
  // operator init, so it can never serve as a first-run indicator.
  // curOrgTbVg starts NULL and becomes non-NULL after the first call to
  // dynCollectResolvedOrgTbVgFromArray, making it a reliable once-per-open
  // trigger that avoids repeated full chain resolution on steady-state calls.
  if (rversion != pVtbScan->rversion || pVtbScan->curOrgTbVg == NULL) {
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
  SOperatorInfo*             pVtbScanOp = pOperator->pDownstream[0];

  // Clear reUse on the old vtbScanParam so that the next setOperatorParams call
  // (inside getNextExtFn) can free it via freeResetOperatorParams.
  if (pVtbScanOp->pOperatorGetParam) {
    pVtbScanOp->pOperatorGetParam->reUse = false;
  }

  // Null out downstream exchange/tag-scan operators' stale pOperatorGetParam.
  // freeVirtualTableScanGetOperatorParam will free the exchange params owned by
  // pOpParamArray; clearing these references prevents UAF in downstream operators.
  for (int32_t d = 0; d < pVtbScanOp->numOfDownstream; ++d) {
    if (pVtbScanOp->pDownstream[d]) {
      pVtbScanOp->pDownstream[d]->pOperatorGetParam = NULL;
    }
  }

  pVtbScan->vtbScanParam = NULL;
  code = buildVtbScanOperatorParam(pInfo, &pVtbScan->vtbScanParam, uid);
  QUERY_CHECK_CODE(code, line, _return);

  SNode* pResidualCond = getVirtualScanFilterCond(pVtbScanOp);
  setVirtualScanRemoteRowLimit(pVtbScan->foreignSourceMap,
                               pResidualCond == NULL ? getVirtualScanRemoteRowLimit(pOperator, pVtbScanOp) : -1);

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

  // Step 3: foreign source params for external column refs
  if (pVtbScan->foreignSourceMap && taosHashGetSize(pVtbScan->foreignSourceMap) > 0) {
    SVTableScanOperatorParam* pVScan = (SVTableScanOperatorParam*)pVtbScan->vtbScanParam->value;
    if (pVScan->pForeignParamArray == NULL) {
      pVScan->pForeignParamArray = taosArrayInit(pVtbScanOp->numOfDownstream, POINTER_BYTES);
      QUERY_CHECK_NULL(pVScan->pForeignParamArray, code, line, _return, terrno)
    }
    int8_t dstPrecision = -1;
    SPhysiNode* pDynNode = (SPhysiNode*)pOperator->pPhyNode;
    if (pDynNode != NULL && pDynNode->pOutputDataBlockDesc != NULL) {
      dstPrecision = pDynNode->pOutputDataBlockDesc->precision;
    }

    // The physical VirtualScan contains one FederatedScan template for every
    // registered external source.  Build one batch parameter for that template
    // instead of one parameter per remote table.  Apart from satisfying the
    // sourceName execution-unit contract, this also prevents multiple sort
    // callbacks from concurrently resetting and reusing the same stateful
    // FederatedScan operator.
    for (int32_t d = 0; d < pVtbScanOp->numOfDownstream; ++d) {
      SOperatorInfo* pDownstream = pVtbScanOp->pDownstream[d];
      if (NULL == pDownstream ||
          QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN != pDownstream->operatorType) {
        continue;
      }

      SFederatedScanOperatorInfo* pFedInfo = pDownstream->info;
      SExtTableNode* pExtTable =
          NULL != pFedInfo && NULL != pFedInfo->pFedScanNode
              ? (SExtTableNode*)pFedInfo->pFedScanNode->pExtTable
              : NULL;
      QUERY_CHECK_NULL(pExtTable, code, line, _return, TSDB_CODE_INVALID_PARA)
      if (!foreignSourceMapHasSource(pVtbScan->foreignSourceMap, pExtTable->sourceName)) {
        continue;
      }

      SOperatorParam* pForeignParam = NULL;
      code = buildBatchFederatedScanOperatorParamForVirtual(
          &pForeignParam, d, pVtbScan->foreignSourceMap, pExtTable->sourceName,
          dstPrecision, 0, pVtbScan->window, false, pVtbScan->readColList);
      QUERY_CHECK_CODE(code, line, _return);
      ((SForeignScanOperatorParam*)pForeignParam->value)->sourceMetaVersion =
          pFedInfo->pFedScanNode->metaVersion;
      if (taosArrayPush(pVScan->pForeignParamArray, &pForeignParam) == NULL) {
        freeOperatorParam(pForeignParam, OP_GET_PARAM);
        QUERY_CHECK_CODE(code = terrno, line, _return);
      }
    }
  }

  SVTableScanOperatorParam* pParam = pVtbScan->vtbScanParam->value;
  pParam->pTagScanOp = NULL;
  pParam->tagDownStreamId = -1;
  if (pVtbScan->isSuperTable && pVtbScan->useTagScan && pVtbScanOp != NULL && pVtbScanOp->numOfDownstream > 1) {
    // Tag scan looks up the virtual child table's own metadata (e.g. tbname),
    // which resides on the virtual child table's own vgroup, not the tag-ref source's vgroup.
    code = buildExchangeOperatorParamForVTagScan(&pParam->pTagScanOp, 0, vgId, uid);
    QUERY_CHECK_CODE(code, line, _return);
    pParam->tagDownStreamId = 0;
  }

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
  pVtbScan->foreignSourceMap = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  QUERY_CHECK_NULL(pVtbScan->foreignSourceMap, code, line, _return, terrno)
  taosHashSetFreeFp(pVtbScan->foreignSourceMap, destroyForeignSourceInfoInExecutor);

  while (true) {
    if (pVtbScan->curTableIdx == pVtbScan->lastTableIdx) {
      code = pVtbScanOp->fpSet.getNextFn(pVtbScanOp, pRes);
      QUERY_CHECK_CODE(code, line, _return);
    } else {
      taosHashClear(pVtbScan->otbNameToOtbInfoMap);
      taosHashClear(pVtbScan->foreignSourceMap);
      SArray* pColRefInfo = NULL;
      if (pVtbScan->isSuperTable) {
        pColRefInfo = (SArray*)taosArrayGetP(pVtbScan->childTableList, pVtbScan->curTableIdx);
      } else {
        pColRefInfo = pInfo->vtbScan.colRefInfo;
      }
      QUERY_CHECK_NULL(pColRefInfo, code, line, _return, terrno)

      pVtbScan->useTagScan = false;
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

      const char* vtbChildName = NULL;
      size_t      vtbChildNameLen = 0;
      char        vtbChildNameBuf[TSDB_TABLE_NAME_LEN] = {0};
      if (pVtbScan->isSuperTable && pVtbScan->childTableMap) {
        void* iter = taosHashIterate(pVtbScan->childTableMap, NULL);
        while (iter) {
          int32_t* pIdx = (int32_t*)iter;
          if (*pIdx == pVtbScan->curTableIdx) {
            vtbChildName = taosHashGetKey(iter, &vtbChildNameLen);
            taosHashCancelIterate(pVtbScan->childTableMap, iter);
            break;
          }
          iter = taosHashIterate(pVtbScan->childTableMap, iter);
        }
      } else if (!pVtbScan->isSuperTable && pVtbScan->tbName) {
        vtbChildName = pVtbScan->tbName;
        vtbChildNameLen = strlen(pVtbScan->tbName);
      }

      if (vtbChildName != NULL) {
        size_t copyLen = TMIN(vtbChildNameLen, sizeof(vtbChildNameBuf) - 1);
        if (copyLen > 0) {
          memcpy(vtbChildNameBuf, vtbChildName, copyLen);
        }
        vtbChildName = vtbChildNameBuf;
      }

      SArray* pResolvedTags = NULL;
      code = resolveTagValsForVtbChild(pOperator, pColRefInfo, vtbChildName, &pResolvedTags);
      QUERY_CHECK_CODE(code, line, _return);
      if (pResolvedTags && pVtbScan->vtbScanParam) {
        ((SVTableScanOperatorParam*)pVtbScan->vtbScanParam->value)->pResolvedTags = pResolvedTags;
      } else if (pResolvedTags) {
        taosArrayDestroy(pResolvedTags);
      }
      if (vtbChildName != NULL && pVtbScan->vtbScanParam != NULL) {
        tstrncpy(((SVTableScanOperatorParam*)pVtbScan->vtbScanParam->value)->tbName, vtbChildName,
                 TSDB_TABLE_NAME_LEN);
      }

      // Layer 2: Evaluate tag-ref filter against resolved tag values before data scan
      if (pVtbScan->pTagRefFilterCond && pVtbScan->vtbScanParam) {
        SArray* pTags = ((SVTableScanOperatorParam*)pVtbScan->vtbScanParam->value)->pResolvedTags;
        if (pTags) {
          bool passes = true;
          code = evaluateTagRefFilter(pVtbScan, pTags, &passes);
          QUERY_CHECK_CODE(code, line, _return);
          if (!passes) {
            qDebug("tag-ref filter pushdown: child idx=%d skipped (filter not matched)", pVtbScan->curTableIdx);
            freeOperatorParam(pVtbScan->vtbScanParam, OP_GET_PARAM);
            pVtbScan->vtbScanParam = NULL;
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
            continue;
          }
        }
      }

      // reset downstream operator's status
      pVtbScanOp->status = OP_NOT_OPENED;
      // Transfer ownership before getNextExtFn: setOperatorParams stores the param
      // in pVtbScanOp->pOperatorGetParam immediately. If getNextExtFn triggers
      // T_LONG_JMP, the param is safely owned by the downstream operator and
      // will be freed during operator destruction. Clearing here avoids dual
      // ownership (pVtbScan->vtbScanParam AND pOperatorGetParam pointing to same memory).
      SOperatorParam* pParamToSend = pVtbScan->vtbScanParam;
      pVtbScan->vtbScanParam = NULL;
      code = pVtbScanOp->fpSet.getNextExtFn(pVtbScanOp, pParamToSend, pRes);
      QUERY_CHECK_CODE(code, line, _return);
    }

    if (*pRes) {
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
  if (pVtbScan->vtbScanParam) {
    freeOperatorParam(pVtbScan->vtbScanParam, OP_GET_PARAM);
    pVtbScan->vtbScanParam = NULL;
  }
  taosHashCleanup(pVtbScan->otbNameToOtbInfoMap);
  pVtbScan->otbNameToOtbInfoMap = NULL;
  taosHashCleanup(pVtbScan->foreignSourceMap);
  pVtbScan->foreignSourceMap = NULL;
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
    code = buildMergeOperatorParamForTsScan(pInfo, pMergeOp, &pMergeParam);
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

// Filter childTableList using tag-ref source exclusion set.
// Removes virtual children whose tag-ref source table is in the exclusion set (local non-matching).
// Entries referencing source tables NOT in the exclusion set are kept (either matching or in other vnodes).
static int32_t filterChildTableListByTagRef(SVtbScanDynCtrlInfo* pVtbScan) {
  if (!pVtbScan->pExcludedSourceNames || !pVtbScan->tagRefFilterColName) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t hashSize = (int32_t)taosHashGetSize(pVtbScan->pExcludedSourceNames);
  if (hashSize == 0) return TSDB_CODE_SUCCESS;

  int32_t total = (int32_t)taosArrayGetSize(pVtbScan->childTableList);
  if (total == 0) return TSDB_CODE_SUCCESS;

  // Use a bitmap to mark which entries to keep
  bool* keepBits = taosMemoryCalloc(total, sizeof(bool));
  if (!keepBits) return terrno;

  int32_t excludeCount = 0;
  for (int32_t i = 0; i < total; ++i) {
    SArray* pColRefArray = (SArray*)taosArrayGetP(pVtbScan->childTableList, i);
    if (!pColRefArray) {
      keepBits[i] = true;  // keep entries we can't inspect
      qError("tag-ref filter: i=%d pColRefArray=NULL", i);
      continue;
    }

    // Default: keep the entry (safe for entries without this tag-ref column)
    keepBits[i] = true;
    int32_t numRefs = (int32_t)taosArrayGetSize(pColRefArray);
    for (int32_t j = 0; j < numRefs; ++j) {
      SColRefInfo* pRef = (SColRefInfo*)taosArrayGet(pColRefArray, j);
      if (!pRef) continue;
      if (pRef->refType == 1 && pRef->colrefName) {
        // colrefName format: "db.table.column"  -  extract column name (last part after second dot)
        const char* firstDot = strchr(pRef->colrefName, '.');
        if (!firstDot) continue;
        const char* tbStart = firstDot + 1;
        const char* secondDot = strchr(tbStart, '.');
        if (!secondDot) continue;
        const char* colStart = secondDot + 1;
        // Match the column name from colrefName against the filter column name
        if (strcmp(colStart, pVtbScan->tagRefFilterColName) == 0) {
          // Extract the table name (middle part) to check against exclusion set
          size_t tbLen = (size_t)(secondDot - tbStart);
          if (taosHashGet(pVtbScan->pExcludedSourceNames, tbStart, tbLen) != NULL) {
            keepBits[i] = false;
            excludeCount++;
          }
          break;
        }
      }
    }
  }

  if (excludeCount == 0) {
    taosMemoryFree(keepBits);
    return TSDB_CODE_SUCCESS;
  }

  // Compact childTableList in-place and build old -> new index mapping
  int32_t* indexMap = taosMemoryCalloc(total, sizeof(int32_t));
  if (!indexMap) {
    taosMemoryFree(keepBits);
    return terrno;
  }

  int32_t writeIdx = 0;
  for (int32_t i = 0; i < total; ++i) {
    if (keepBits[i]) {
      indexMap[i] = writeIdx;
      if (writeIdx != i) {
        SArray* pEntry = (SArray*)taosArrayGetP(pVtbScan->childTableList, i);
        taosArraySet(pVtbScan->childTableList, writeIdx, &pEntry);
      }
      writeIdx++;
    } else {
      indexMap[i] = -1;
      SArray* pEntry = (SArray*)taosArrayGetP(pVtbScan->childTableList, i);
      if (pEntry) {
        destroyColRefArray(&pEntry);
      }
    }
  }

  taosArrayPopTailBatch(pVtbScan->childTableList, total - writeIdx);
  qDebug("tag-ref filter: reduced childTableList from %d to %d (excluded %d)", total, writeIdx, excludeCount);

  // Update childTableMap indices: two passes for safe hash modification
  typedef struct { char key[TSDB_TABLE_NAME_LEN]; size_t keyLen; } SRemoveKey;
  int32_t code = TSDB_CODE_SUCCESS;
  SArray* pRemoveKeys = taosArrayInit(excludeCount, sizeof(SRemoveKey));
  if (!pRemoveKeys) {
    taosMemoryFree(keepBits);
    taosMemoryFree(indexMap);
    return terrno;
  }

  void* iter = taosHashIterate(pVtbScan->childTableMap, NULL);
  while (iter) {
    int32_t* pOldIdx = (int32_t*)iter;
    int32_t  oldIdx = *pOldIdx;
    if (oldIdx >= 0 && oldIdx < total) {
      int32_t newIdx = indexMap[oldIdx];
      if (newIdx < 0) {
        SRemoveKey rk = {0};
        rk.keyLen = 0;
        void* key = taosHashGetKey(iter, &rk.keyLen);
        size_t copyLen = TMIN(rk.keyLen, sizeof(rk.key));
        memcpy(rk.key, key, copyLen);
        rk.keyLen = copyLen;
        if (NULL == taosArrayPush(pRemoveKeys, &rk)) {
          taosHashCancelIterate(pVtbScan->childTableMap, iter);
          code = terrno;
          goto _cleanup;
        }
      } else {
        *pOldIdx = newIdx;
      }
    }
    iter = taosHashIterate(pVtbScan->childTableMap, iter);
  }

  for (int32_t r = 0; r < (int32_t)taosArrayGetSize(pRemoveKeys); ++r) {
    SRemoveKey* pRk = (SRemoveKey*)taosArrayGet(pRemoveKeys, r);
    if (pRk) {
      code = taosHashRemove(pVtbScan->childTableMap, pRk->key, pRk->keyLen);
      if (code != TSDB_CODE_SUCCESS) {
        goto _cleanup;
      }
    }
  }

_cleanup:
  taosArrayDestroy(pRemoveKeys);
  taosMemoryFree(keepBits);
  taosMemoryFree(indexMap);
  return code;
}

// Context for rewriting tag filter column slotIds
typedef struct STagRefFilterRewriteCtx {
  int32_t  nextSlotId;
  SHashObj* colIdHash;   // colId -> slotId mapping (to dedup)
  SArray*  pColInfos;    // SArray<SColumnInfo>
} STagRefFilterRewriteCtx;

// Rewriter callback: assigns sequential slotIds to SColumnNode in the filter tree
static EDealRes tagRefFilterRewriteColumn(SNode** pNode, void* pContext) {
  if (QUERY_NODE_COLUMN != nodeType(*pNode)) {
    return DEAL_RES_CONTINUE;
  }

  SColumnNode* pCol = (SColumnNode*)*pNode;
  STagRefFilterRewriteCtx* pCtx = (STagRefFilterRewriteCtx*)pContext;

  int32_t* pExistingSlot = taosHashGet(pCtx->colIdHash, &pCol->colId, sizeof(pCol->colId));
  if (pExistingSlot) {
    pCol->slotId = *pExistingSlot;
  } else {
    int32_t slot = pCtx->nextSlotId++;
    pCol->slotId = slot;
    if (taosHashPut(pCtx->colIdHash, &pCol->colId, sizeof(pCol->colId), &slot, sizeof(slot)) != TSDB_CODE_SUCCESS) {
      return DEAL_RES_ERROR;
    }
    SColumnInfo cInfo = {.colId = pCol->colId,
                         .type = pCol->node.resType.type,
                         .bytes = pCol->node.resType.bytes};
    if (NULL == taosArrayPush(pCtx->pColInfos, &cInfo)) {
      return DEAL_RES_ERROR;
    }
  }

  return DEAL_RES_CONTINUE;
}

// Initialize general tag-ref filter context for per-child evaluation (Layer 2).
// Clones pTagFilterCond, rewrites column slotIds to sequential layout, builds cInfoList.
static int32_t initTagRefFilterContext(SVtbScanDynCtrlInfo* pVtbScan, SDynQueryCtrlPhysiNode* pPhyciNode) {
  SNode* pOrigCond = pPhyciNode->vtbScan.pTagFilterCond;
  if (!pOrigCond) {
    return TSDB_CODE_SUCCESS;
  }

  // Clone the filter condition (we own this copy and will rewrite it)
  SNode* pCloned = NULL;
  int32_t code = nodesCloneNode(pOrigCond, &pCloned);
  if (code != TSDB_CODE_SUCCESS || !pCloned) {
    return (code != TSDB_CODE_SUCCESS) ? code : terrno;
  }

  // Rewrite column slotIds and collect column info
  STagRefFilterRewriteCtx ctx = {0};
  ctx.nextSlotId = 0;
  ctx.colIdHash = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT), true, HASH_NO_LOCK);
  ctx.pColInfos = taosArrayInit(4, sizeof(SColumnInfo));
  if (!ctx.colIdHash || !ctx.pColInfos) {
    nodesDestroyNode(pCloned);
    taosHashCleanup(ctx.colIdHash);
    taosArrayDestroy(ctx.pColInfos);
    return terrno;
  }

  nodesRewriteExpr(&pCloned, tagRefFilterRewriteColumn, &ctx);
  taosHashCleanup(ctx.colIdHash);

  if (taosArrayGetSize(ctx.pColInfos) == 0) {
    nodesDestroyNode(pCloned);
    taosArrayDestroy(ctx.pColInfos);
    return TSDB_CODE_SUCCESS;
  }

  pVtbScan->pTagRefFilterCond = pCloned;
  pVtbScan->pTagRefFilterColInfos = ctx.pColInfos;

  // Ensure all filter columns are in readColSet so resolveTagValsForVtbChild resolves them
  for (int32_t i = 0; i < taosArrayGetSize(ctx.pColInfos); ++i) {
    SColumnInfo* pCI = taosArrayGet(ctx.pColInfos, i);
    if (pCI && pVtbScan->readColSet) {
      if (!taosHashGet(pVtbScan->readColSet, &pCI->colId, sizeof(pCI->colId))) {
        int32_t rc = taosHashPut(pVtbScan->readColSet, &pCI->colId, sizeof(pCI->colId), NULL, 0);
        if (rc != TSDB_CODE_SUCCESS) {
          return rc;
        }
        if (NULL == taosArrayPush(pVtbScan->readColList, &pCI->colId)) {
          return terrno;
        }
      }
    }
  }

  qDebug("tag-ref filter ctx: initialized with %d columns for general pushdown",
         (int32_t)taosArrayGetSize(ctx.pColInfos));
  return TSDB_CODE_SUCCESS;
}

// Evaluate the tag-ref filter condition against resolved tag values for a single child.
// Returns true in *pPasses if the child matches the filter (or if evaluation fails conservatively).
static int32_t evaluateTagRefFilter(SVtbScanDynCtrlInfo* pVtbScan, SArray* pResolvedTags, bool* pPasses) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  *pPasses = true;  // default: pass (conservative on error)

  if (!pVtbScan->pTagRefFilterCond || !pVtbScan->pTagRefFilterColInfos || !pResolvedTags) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t numCols = (int32_t)taosArrayGetSize(pVtbScan->pTagRefFilterColInfos);
  if (numCols == 0) {
    return TSDB_CODE_SUCCESS;
  }

  // Create a 1-row data block with the filter columns
  SSDataBlock* pBlock = NULL;
  code = createDataBlock(&pBlock);
  QUERY_CHECK_CODE(code, lino, _end);

  for (int32_t i = 0; i < numCols; ++i) {
    SColumnInfoData colInfoData = {0};
    SColumnInfo* pCI = taosArrayGet(pVtbScan->pTagRefFilterColInfos, i);
    QUERY_CHECK_NULL(pCI, code, lino, _end, terrno);
    colInfoData.info = *pCI;
    code = blockDataAppendColInfo(pBlock, &colInfoData);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  code = blockDataEnsureCapacity(pBlock, 1);
  QUERY_CHECK_CODE(code, lino, _end);
  pBlock->info.rows = 1;

  // Fill in tag values from pResolvedTags, matched by colId
  for (int32_t i = 0; i < numCols; ++i) {
    SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, i);
    QUERY_CHECK_NULL(pColData, code, lino, _end, terrno);

    // Find matching tag value in pResolvedTags by colId
    bool found = false;
    int32_t numTags = (int32_t)taosArrayGetSize(pResolvedTags);
    for (int32_t t = 0; t < numTags; ++t) {
      STagVal* pTag = taosArrayGet(pResolvedTags, t);
      if (pTag && pTag->cid == pColData->info.colId) {
        if (IS_VAR_DATA_TYPE(pColData->info.type)) {
          if (pTag->pData && pTag->nData > 0) {
            char* buf = taosMemoryMalloc(pTag->nData + VARSTR_HEADER_SIZE);
            QUERY_CHECK_NULL(buf, code, lino, _end, terrno);
            varDataSetLen(buf, pTag->nData);
            memcpy(varDataVal(buf), pTag->pData, pTag->nData);
            code = colDataSetVal(pColData, 0, buf, false);
            taosMemoryFree(buf);
            QUERY_CHECK_CODE(code, lino, _end);
          } else {
            colDataSetNULL(pColData, 0);
          }
        } else {
          code = colDataSetVal(pColData, 0, (const char*)&pTag->i64, false);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        found = true;
        break;
      }
    }
    if (!found) {
      colDataSetNULL(pColData, 0);
    }
  }

  // Evaluate the filter condition
  SArray* pBlockList = taosArrayInit(1, POINTER_BYTES);
  QUERY_CHECK_NULL(pBlockList, code, lino, _end, terrno);
  QUERY_CHECK_NULL(taosArrayPush(pBlockList, &pBlock), code, lino, _end2, terrno);

  SDataType type = {.type = TSDB_DATA_TYPE_BOOL, .bytes = sizeof(bool)};
  SScalarParam output = {0};
  SColumnInfoData* pOutputCol = taosMemoryCalloc(1, sizeof(SColumnInfoData));
  QUERY_CHECK_NULL(pOutputCol, code, lino, _end2, terrno);
  pOutputCol->info.type = TSDB_DATA_TYPE_BOOL;
  pOutputCol->info.bytes = sizeof(bool);
  code = colInfoDataEnsureCapacity(pOutputCol, 1, true);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pOutputCol);
    goto _end2;
  }
  output.columnData = pOutputCol;
  output.numOfRows = 1;
  output.colAlloced = true;

  gTaskScalarExtra.pStreamInfo = NULL;
  gTaskScalarExtra.pStreamRange = NULL;
  code = scalarCalculate(pVtbScan->pTagRefFilterCond, pBlockList, &output, &gTaskScalarExtra);
  if (code != TSDB_CODE_SUCCESS) {
    // Evaluation failed  -  conservatively pass
    qWarn("tag-ref filter eval failed: %s, passing child through", tstrerror(code));
    code = TSDB_CODE_SUCCESS;
    *pPasses = true;
  } else {
    if (colDataIsNull_f(output.columnData, 0)) {
      *pPasses = false;  // NULL result means filter doesn't match (SQL semantics)
    } else {
      bool* pResult = (bool*)output.columnData->pData;
      *pPasses = pResult[0];
    }
  }

  colDataDestroy(output.columnData);
  taosMemoryFree(output.columnData);

_end2:
  taosArrayDestroy(pBlockList);
_end:
  blockDataDestroy(pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    *pPasses = true;  // conservative on error
    code = TSDB_CODE_SUCCESS;
  }
  return code;
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
  pInfo->vtbScan.useTagScan = false;
  pInfo->vtbScan.isSuperTable = pPhyciNode->vtbScan.isSuperTable;
  pInfo->vtbScan.hasLocalTag = pPhyciNode->vtbScan.hasLocalTag;
  pInfo->vtbScan.rversion = pPhyciNode->vtbScan.rversion;
  pInfo->vtbScan.uid = pPhyciNode->vtbScan.uid;
  pInfo->vtbScan.suid = pPhyciNode->vtbScan.suid;
  pInfo->vtbScan.epSet = pPhyciNode->vtbScan.mgmtEpSet;
  pInfo->vtbScan.acctId = pPhyciNode->vtbScan.accountId;
  pInfo->vtbScan.dstPrecision = pPhyciNode->node.pOutputDataBlockDesc != NULL
                                    ? pPhyciNode->node.pOutputDataBlockDesc->precision
                                    : -1;
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
  if (DYN_QTYPE_VTB_AGG == pInfo->qType) {
    pInfo->vtbScan.aggTagColIds = taosArrayInit(1, sizeof(col_id_t));
    QUERY_CHECK_NULL(pInfo->vtbScan.aggTagColIds, code, line, _return, terrno)
  }

  SNode* colNode = NULL;
  FOREACH(colNode, pPhyciNode->vtbScan.pScanCols) {
    SColumnNode* pNode = (SColumnNode*)colNode;
    QUERY_CHECK_NULL(pNode, code, line, _return, terrno)
    QUERY_CHECK_NULL(taosArrayPush(pInfo->vtbScan.readColList, &pNode->colId), code, line, _return, terrno)
    // A rewritten TBNAME can carry a generated alias and an unset column type,
    // but its logical column id remains zero.
    if (NULL != pInfo->vtbScan.aggTagColIds &&
        (COLUMN_TYPE_TAG == pNode->colType || COLUMN_TYPE_TBNAME == pNode->colType ||
         0 == strcasecmp(pNode->colName, "tbname") || (0 == pNode->colType && 0 == pNode->colId))) {
      bool duplicate = false;
      for (int32_t i = 0; i < taosArrayGetSize(pInfo->vtbScan.aggTagColIds); ++i) {
        col_id_t* pColId = taosArrayGet(pInfo->vtbScan.aggTagColIds, i);
        if (NULL != pColId && *pColId == pNode->colId) {
          duplicate = true;
          break;
        }
      }
      if (!duplicate) {
        QUERY_CHECK_NULL(taosArrayPush(pInfo->vtbScan.aggTagColIds, &pNode->colId), code, line, _return, terrno)
      }
    }
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

  qDebug("vtable init scan info, qType:%d db:%s tb:%s super:%d suid:%" PRIu64 " uid:%" PRIu64 " orgVgNum:%d readCols:%" PRId64,
         pInfo->qType, pInfo->vtbScan.dbName, pInfo->vtbScan.tbName, pInfo->vtbScan.isSuperTable,
         pInfo->vtbScan.suid, pInfo->vtbScan.uid, taosHashGetSize(pInfo->vtbScan.existOrgTbVg),
         (int64_t)taosArrayGetSize(pInfo->vtbScan.readColList));

  pInfo->vtbScan.refColGroups = NULL;

  pInfo->vtbScan.childTableList = taosArrayInit(10, POINTER_BYTES);
  QUERY_CHECK_NULL(pInfo->vtbScan.childTableList, code, line, _return, terrno)

  pInfo->vtbScan.dbVgInfoMap = taosHashInit(taosArrayGetSize(pInfo->vtbScan.childTableList), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  QUERY_CHECK_NULL(pInfo->vtbScan.dbVgInfoMap, code, line, _return, terrno)
  pInfo->vtbScan.resolvedColRefMap = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  QUERY_CHECK_NULL(pInfo->vtbScan.resolvedColRefMap, code, line, _return, terrno)
  taosHashSetFreeFp(pInfo->vtbScan.resolvedColRefMap, destroyResolvedColRef);

  pInfo->vtbScan.otbNameToOtbInfoMap = NULL;
  pInfo->vtbScan.otbVgIdToOtbInfoArrayMap = NULL;
  pInfo->vtbScan.vtbUidToVgIdMapMap = NULL;
  pInfo->vtbScan.vtbGroupIdToVgIdMapMap = NULL;
  pInfo->vtbScan.vtbUidTagListMap = NULL;
  pInfo->vtbScan.vtbGroupIdTagListMap = NULL;
  pInfo->vtbScan.vtbUidToGroupIdMap = NULL;
  pInfo->vtbScan.pExcludedSourceNames = NULL;
  pInfo->vtbScan.tagRefFilterColName = NULL;
  pInfo->vtbScan.tagRefFilterColId = 0;
  tstrncpy(pInfo->vtbScan.tagRefTerminalColName, pPhyciNode->vtbScan.tagRefTerminalColName, TSDB_COL_NAME_LEN);
  pInfo->vtbScan.pTagRefFilterCond = NULL;
  pInfo->vtbScan.pTagRefFilterColInfos = NULL;

  code = initTagRefFilterContext(&pInfo->vtbScan, pPhyciNode);
  QUERY_CHECK_CODE(code, line, _return);

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
      pWindow->resWinIdx = -1;
    }

    QUERY_CHECK_NULL(taosArrayPush(pDynInfo->vtbWindow.pWins, &pWin), code, lino, _return, terrno);
  }

  // handle first window's start key and last window's end key
  int32_t winBatchNum = (int32_t)taosArrayGetSize(pDynInfo->vtbWindow.pWins);
  bool    needExtendScan = pInfo->isVstb || pOperator->numOfDownstream > 1;
  if (winBatchNum > 0 && needExtendScan) {
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

static int32_t buildMergeOperatorParamEx(SDynQueryCtrlOperatorInfo* pInfo, SOperatorInfo* pMergeOp,
                                         SOperatorParam** ppRes, SArray* pWins, int32_t numOfWins);

static int32_t buildExternalWindowOperatorParamEx(SDynQueryCtrlOperatorInfo* pInfo, SOperatorInfo* pExtWinOperator,
                                                  SOperatorParam** ppRes, SArray* pWins, int32_t idx) {
  int32_t                       code = TSDB_CODE_SUCCESS;
  int32_t                       lino = 0;
  SExternalWindowOperatorParam* pExtWinOp = NULL;
  SArray*                       pRequiredColIds = NULL;
  SArray*                       pNullOutputSlots = NULL;
  SOperatorParam*               pExchangeParam = NULL;
  bool                          active = false;
  SOperatorInfo*                pScanOp =
      pExtWinOperator != NULL && pExtWinOperator->numOfDownstream == 1
          ? pExtWinOperator->pDownstream[0]
          : NULL;

  *ppRes = NULL;
  if (pScanOp != NULL && QUERY_NODE_PHYSICAL_PLAN_MERGE == pScanOp->operatorType) {
    pRequiredColIds = taosArrayInit(1, sizeof(col_id_t));
    pNullOutputSlots = taosArrayInit(1, sizeof(int32_t));
    QUERY_CHECK_NULL(pRequiredColIds, code, lino, _return, terrno);
    QUERY_CHECK_NULL(pNullOutputSlots, code, lino, _return, terrno);
    active = true;
  } else {
    code = analyzeExternalWindowUnit(pInfo, pExtWinOperator, &pRequiredColIds,
                                     &pNullOutputSlots, &active);
    QUERY_CHECK_CODE(code, lino, _return);
  }
  if (!active) {
    taosArrayDestroy(pRequiredColIds);
    taosArrayDestroy(pNullOutputSlots);
    return TSDB_CODE_SUCCESS;
  }

  *ppRes = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(*ppRes, code, lino, _return, terrno)

  pExtWinOp = taosMemoryCalloc(1, sizeof(SExternalWindowOperatorParam));
  QUERY_CHECK_NULL(pExtWinOp, code, lino, _return, terrno)
  pExtWinOp->pNullOutputSlots = pNullOutputSlots;
  pNullOutputSlots = NULL;

  pExtWinOp->ExtWins = taosArrayDup(pWins, NULL);
  QUERY_CHECK_NULL(pExtWinOp->ExtWins, code, lino, _return, terrno)

  SExtWinTimeWindow* firstWin = (SExtWinTimeWindow*)taosArrayGet(pWins, 0);
  SExtWinTimeWindow* lastWin = (SExtWinTimeWindow*)taosArrayGetLast(pWins);

  QUERY_CHECK_NULL(firstWin, code, lino, _return, terrno);
  QUERY_CHECK_NULL(lastWin, code, lino, _return, terrno);

  (*ppRes)->pChildren = taosArrayInit(1, POINTER_BYTES);
  QUERY_CHECK_NULL((*ppRes)->pChildren, code, lino, _return, terrno)

  SOperatorInfo* pFedScanOp = findFederatedScanInUnaryBranch(pScanOp);
  if (pScanOp != NULL && QUERY_NODE_PHYSICAL_PLAN_MERGE == pScanOp->operatorType) {
    code = buildMergeOperatorParamEx(pInfo, pScanOp, &pExchangeParam, pWins,
                                     (int32_t)taosArrayGetSize(pWins));
    QUERY_CHECK_CODE(code, lino, _return);
  } else if (pScanOp != NULL && pScanOp->operatorType == QUERY_NODE_PHYSICAL_PLAN_DYN_QUERY_CTRL) {
    code = buildDynQueryCtrlOperatorParamForExternalWindow(
        &pExchangeParam, (STimeWindow){.skey = firstWin->tw.skey, .ekey = lastWin->tw.ekey});
    QUERY_CHECK_CODE(code, lino, _return);
  } else if (pFedScanOp != NULL) {
    SFederatedScanOperatorInfo* pFedInfo = pFedScanOp->info;
    SExtTableNode* pExtTable =
        pFedInfo != NULL && pFedInfo->pFedScanNode != NULL
            ? (SExtTableNode*)pFedInfo->pFedScanNode->pExtTable
            : NULL;
    QUERY_CHECK_NULL(pExtTable, code, lino, _return, TSDB_CODE_INVALID_PARA)
    code = buildBatchFederatedScanOperatorParamForVirtual(
        &pExchangeParam, 0, pInfo->vtbScan.foreignSourceMap, pExtTable->sourceName,
        pInfo->vtbScan.dstPrecision, 0,
        (STimeWindow){.skey = firstWin->tw.skey, .ekey = lastWin->tw.ekey}, false,
        taosArrayGetSize(pRequiredColIds) > 0 ? pRequiredColIds : NULL);
    QUERY_CHECK_CODE(code, lino, _return);
    ((SForeignScanOperatorParam*)pExchangeParam->value)->sourceMetaVersion =
        pFedInfo->pFedScanNode->metaVersion;
  } else {
    code = buildBatchExchangeOperatorParamForVirtual(
        &pExchangeParam, 0, NULL, 0, pInfo->vtbScan.otbVgIdToOtbInfoArrayMap,
        (STimeWindow){.skey = firstWin->tw.skey, .ekey = lastWin->tw.ekey}, EX_SRC_TYPE_VSTB_WIN_SCAN,
        QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, NULL, 0);
  }
  QUERY_CHECK_CODE(code, lino, _return);

  QUERY_CHECK_NULL(taosArrayPush((*ppRes)->pChildren, &pExchangeParam), code, lino, _return, terrno)
  pExchangeParam = NULL;

  (*ppRes)->opType = QUERY_NODE_PHYSICAL_PLAN_EXTERNAL_WINDOW;
  (*ppRes)->downstreamIdx = idx;
  (*ppRes)->value = pExtWinOp;
  (*ppRes)->reUse = false;

  taosArrayDestroy(pRequiredColIds);
  return code;
_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  taosArrayDestroy(pRequiredColIds);
  taosArrayDestroy(pNullOutputSlots);
  if (pExtWinOp) {
    taosArrayDestroy(pExtWinOp->ExtWins);
    taosArrayDestroy(pExtWinOp->pNullOutputSlots);
    taosMemoryFree(pExtWinOp);
  }
  if (pExchangeParam) {
    freeOperatorParam(pExchangeParam, OP_GET_PARAM);
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

static int32_t buildMergeOperatorParamEx(SDynQueryCtrlOperatorInfo* pInfo, SOperatorInfo* pMergeOp,
                                         SOperatorParam** ppRes, SArray* pWins, int32_t numOfWins) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SOperatorParam* pChildParam = NULL;

  *ppRes = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(*ppRes, code, lino, _return, terrno);
  (*ppRes)->opType = QUERY_NODE_PHYSICAL_PLAN_MERGE;
  (*ppRes)->downstreamIdx = 0;
  (*ppRes)->value = taosMemoryCalloc(1, sizeof(SMergeOperatorParam));
  QUERY_CHECK_NULL((*ppRes)->value, code, lino, _return, terrno);
  ((SMergeOperatorParam*)(*ppRes)->value)->winNum = numOfWins;
  ((SMergeOperatorParam*)(*ppRes)->value)->mergeSameCols = true;
  (*ppRes)->pChildren = taosArrayInit(pMergeOp->numOfDownstream, POINTER_BYTES);
  QUERY_CHECK_NULL((*ppRes)->pChildren, code, lino, _return, terrno);

  for (int32_t i = 0; i < pMergeOp->numOfDownstream; ++i) {
    SOperatorInfo* pExtWin = pMergeOp->pDownstream[i];
    QUERY_CHECK_NULL(pExtWin, code, lino, _return, TSDB_CODE_INVALID_PARA);
    QUERY_CHECK_CONDITION(QUERY_NODE_PHYSICAL_PLAN_EXTERNAL_WINDOW == pExtWin->operatorType ||
                              QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL == pExtWin->operatorType,
                          code, lino, _return, TSDB_CODE_INVALID_PARA);
    SOperatorInfo* pScanOp = pExtWin->numOfDownstream == 1 ? pExtWin->pDownstream[0] : NULL;
    SOperatorInfo* pFedScanOp = findFederatedScanInUnaryBranch(pScanOp);
    if (pFedScanOp != NULL) {
      SFederatedScanOperatorInfo* pFedInfo = pFedScanOp->info;
      SExtTableNode* pExtTable =
          pFedInfo != NULL && pFedInfo->pFedScanNode != NULL
              ? (SExtTableNode*)pFedInfo->pFedScanNode->pExtTable
              : NULL;
      if (pExtTable == NULL ||
          !foreignSourceMapHasSource(pInfo->vtbScan.foreignSourceMap, pExtTable->sourceName)) {
        continue;
      }
    } else if (pInfo->vtbScan.otbVgIdToOtbInfoArrayMap == NULL ||
               taosHashGetSize(pInfo->vtbScan.otbVgIdToOtbInfoArrayMap) == 0) {
      continue;
    }
    code = buildExternalWindowOperatorParamEx(pInfo, pExtWin, &pChildParam, pWins, i);
    QUERY_CHECK_CODE(code, lino, _return);
    if (pChildParam == NULL) {
      continue;
    }
    QUERY_CHECK_NULL(taosArrayPush((*ppRes)->pChildren, &pChildParam), code, lino, _return, terrno);
    pChildParam = NULL;
  }
  return TSDB_CODE_SUCCESS;

_return:
  freeOperatorParam(pChildParam, OP_GET_PARAM);
  freeOperatorParam(*ppRes, OP_GET_PARAM);
  *ppRes = NULL;
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
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
    bool calcMerge = QUERY_NODE_PHYSICAL_PLAN_MERGE == extWinOp->operatorType;
    if (calcMerge) {
      code = buildMergeOperatorParamEx(pDynInfo, extWinOp, &pExtWinParam, pWinArray, numOfWins);
    } else {
      code = buildExternalWindowOperatorParamEx(pDynInfo, extWinOp, &pExtWinParam, pWinArray,
                                                extWinOp->numOfDownstream);
    }
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

      if (pInfo->curWinBatchIdx == 0 &&
          pExtWinBlock->info.window.skey <= pExtWinBlock->info.window.ekey) {
        // first batch, bound _wstart by upstream window range
        SExtWinTimeWindow* firstWin = (SExtWinTimeWindow*)taosArrayGet(taosArrayGetP(pInfo->pWins, 0), 0);
        QUERY_CHECK_NULL(firstWin, code, lino, _return, terrno)

        firstWin->tw.skey = TMAX(firstWin->tw.skey, pExtWinBlock->info.window.skey);
      }
      if (pInfo->curWinBatchIdx == taosArrayGetSize(pInfo->pWins) - 1 &&
          pExtWinBlock->info.window.skey <= pExtWinBlock->info.window.ekey) {
        // last batch, bound _wend by upstream window range
        SExtWinTimeWindow* lastWin = (SExtWinTimeWindow*)taosArrayGetLast(taosArrayGetP(pInfo->pWins, taosArrayGetSize(pInfo->pWins) - 1));
        QUERY_CHECK_NULL(lastWin, code, lino, _return, terrno)

        lastWin->tw.ekey = TMIN(lastWin->tw.ekey, pExtWinBlock->info.window.ekey + 1);
      }
    }
  } else {
    if (pOperator->numOfDownstream > 1) {
      mergeOp = pOperator->pDownstream[1];
      code = buildMergeOperatorParam(mergeOp, &pMergeParam, pWinArray, mergeOp->numOfDownstream, numOfWins);
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

        if (pInfo->curWinBatchIdx == 0 &&
            pMergedBlock->info.window.skey <= pMergedBlock->info.window.ekey) {
          // first batch, bound _wstart by upstream window range
          SExtWinTimeWindow* firstWin = (SExtWinTimeWindow*)taosArrayGet(taosArrayGetP(pInfo->pWins, 0), 0);
          QUERY_CHECK_NULL(firstWin, code, lino, _return, terrno)

          firstWin->tw.skey = TMAX(firstWin->tw.skey, pMergedBlock->info.window.skey);
        }
        if (pInfo->curWinBatchIdx == taosArrayGetSize(pInfo->pWins) - 1 &&
            pMergedBlock->info.window.skey <= pMergedBlock->info.window.ekey) {
          // last batch, bound _wend by upstream window range
          SExtWinTimeWindow* lastWin =
              (SExtWinTimeWindow*)taosArrayGetLast(taosArrayGetP(pInfo->pWins, taosArrayGetSize(pInfo->pWins) - 1));
          QUERY_CHECK_NULL(lastWin, code, lino, _return, terrno)

          lastWin->tw.ekey = TMIN(lastWin->tw.ekey, pMergedBlock->info.window.ekey + 1);
        }
      }
    } else {
      blockDataCleanup(pRes);
      code = blockDataEnsureCapacity(pRes, numOfWins);
      QUERY_CHECK_CODE(code, lino, _return);
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
      taosHashClear(pVtbScan->extTableMetaCache);
      
      if (pVtbScan->otbNameToOtbInfoMap) {
        taosHashSetFreeFp(pVtbScan->otbNameToOtbInfoMap, destroySOrgTbInfo);
        taosHashCleanup(pVtbScan->otbNameToOtbInfoMap);
        pVtbScan->otbNameToOtbInfoMap = NULL;
      }
      if (pVtbScan->foreignSourceMap) {
        taosHashSetFreeFp(pVtbScan->foreignSourceMap, destroyForeignSourceInfoInExecutor);
        taosHashCleanup(pVtbScan->foreignSourceMap);
        pVtbScan->foreignSourceMap = NULL;
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
      // Table configurations include terminal tag values used by tag-ref chains.
      // They must be refetched for every stream trigger so ALTER TABLE tag changes
      // are visible to the next calculation cycle.
      if (pVtbScan->tableCfgCache) {
        taosHashSetFreeFp(pVtbScan->tableCfgCache, destroyTableCfgCacheEntry);
        taosHashCleanup(pVtbScan->tableCfgCache);
        pVtbScan->tableCfgCache = NULL;
        qDebug("reset dynamic vtable scan: cleared table config cache for tag-ref refresh");
      }
      // Agg/interval org-table maps are rebuilt from scratch on every (re)open by
      // buildOrgTbInfoBatch/Single. In a stream the operator is reset and reopened per
      // trigger, so release the previous instances here to avoid leaking / reusing stale
      // mappings. existOrgTbVg is intentionally preserved across resets because it tracks
      // which original-table vgroups are already deployed for this stream task.
      if (pVtbScan->otbVgIdToOtbInfoArrayMap) {
        taosHashSetFreeFp(pVtbScan->otbVgIdToOtbInfoArrayMap, destroyOtbInfoArray);
        taosHashCleanup(pVtbScan->otbVgIdToOtbInfoArrayMap);
        pVtbScan->otbVgIdToOtbInfoArrayMap = NULL;
      }
      if (pVtbScan->vtbUidToVgIdMapMap) {
        taosHashSetFreeFp(pVtbScan->vtbUidToVgIdMapMap, destroyOtbVgIdToOtbInfoArrayMap);
        taosHashCleanup(pVtbScan->vtbUidToVgIdMapMap);
        pVtbScan->vtbUidToVgIdMapMap = NULL;
      }
      if (pVtbScan->vtbGroupIdToVgIdMapMap) {
        taosHashSetFreeFp(pVtbScan->vtbGroupIdToVgIdMapMap, destroyOtbVgIdToOtbInfoArrayMap);
        taosHashCleanup(pVtbScan->vtbGroupIdToVgIdMapMap);
        pVtbScan->vtbGroupIdToVgIdMapMap = NULL;
      }
      if (pVtbScan->vtbUidTagListMap) {
        taosHashSetFreeFp(pVtbScan->vtbUidTagListMap, destroyTagList);
        taosHashCleanup(pVtbScan->vtbUidTagListMap);
        pVtbScan->vtbUidTagListMap = NULL;
      }
      if (pVtbScan->vtbGroupIdTagListMap) {
        taosHashSetFreeFp(pVtbScan->vtbGroupIdTagListMap, destroyVtbUidTagListMap);
        taosHashCleanup(pVtbScan->vtbGroupIdTagListMap);
        pVtbScan->vtbGroupIdTagListMap = NULL;
      }
      if (pVtbScan->vtbUidToGroupIdMap) {
        taosHashCleanup(pVtbScan->vtbUidToGroupIdMap);
        pVtbScan->vtbUidToGroupIdMap = NULL;
      }
      if (pPhyciNode->dynTbname && pTaskInfo) {
        updateDynTbUidIfNeeded(pVtbScan, pTaskInfo->pStreamRuntimeInfo);
      }
      pVtbScan->curTableIdx = 0;
      pVtbScan->lastTableIdx = -1;
      pVtbScan->genNewParam = true;
      pVtbScan->activeScopeValid = false;
      pVtbScan->activeAggScope = (SVtbAggDispatchScope){0};
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
  SVtbScanDynCtrlInfo*       pVtbScan = &pInfo->vtbScan;
  SOperatorInfo*             pAggRoot =
      pOperator->pDownstream[pOperator->numOfDownstream - 1];

  *pRes = NULL;
  while (OP_EXEC_DONE != pOperator->status) {
    if (!pVtbScan->activeScopeValid) {
      SVtbAggDispatchScope scope = {0};
      if (!pVtbScan->hasPartition && pVtbScan->batchProcessChild) {
        scope.allChildren = true;
      } else if (pVtbScan->hasPartition) {
        void* pGroupIter = taosHashIterate(pVtbScan->vtbGroupIdTagListMap, NULL);
        if (NULL == pGroupIter) {
          setOperatorCompleted(pOperator);
          break;
        }
        size_t keyLen = 0;
        scope.hasGroup = true;
        scope.groupId = *(uint64_t*)taosHashGetKey(pGroupIter, &keyLen);
        if (!pVtbScan->batchProcessChild) {
          SHashObj* pUidMap = *(SHashObj**)pGroupIter;
          void* pUidIter = vtbAggFindMaxUidValue(pUidMap, &scope.uid);
          if (NULL == pUidIter) {
            code = taosHashRemove(pVtbScan->vtbGroupIdTagListMap,
                                  &scope.groupId, sizeof(scope.groupId));
            QUERY_CHECK_CODE(code, line, _return);
            continue;
          }
          scope.hasUid = true;
          scope.pTagList = *(SArray**)pUidIter;
        }
      } else {
        void* pUidIter =
            vtbAggFindMaxUidValue(pVtbScan->vtbUidTagListMap, &scope.uid);
        if (NULL == pUidIter) {
          setOperatorCompleted(pOperator);
          break;
        }
        scope.hasUid = true;
        scope.pTagList = *(SArray**)pUidIter;
      }

      SOperatorParam* pAggParam = NULL;
      code = buildVtbAggScopeParam(pInfo, pAggRoot, &scope, &pAggParam);
      QUERY_CHECK_CODE(code, line, _return);
      pVtbScan->activeAggScope = scope;
      pVtbScan->activeScopeValid = true;
      pVtbScan->genNewParam = false;
      code = pAggRoot->fpSet.getNextExtFn(pAggRoot, pAggParam, pRes);
      QUERY_CHECK_CODE(code, line, _return);
    } else {
      code = pAggRoot->fpSet.getNextFn(pAggRoot, pRes);
      QUERY_CHECK_CODE(code, line, _return);
    }

    if (NULL != *pRes) {
      if (pVtbScan->activeAggScope.hasGroup) {
        (*pRes)->info.id.groupId = pVtbScan->activeAggScope.groupId;
      }
      return code;
    }

    SVtbAggDispatchScope completed = pVtbScan->activeAggScope;
    pVtbScan->activeScopeValid = false;
    pVtbScan->activeAggScope = (SVtbAggDispatchScope){0};
    pVtbScan->genNewParam = true;

    if (completed.allChildren) {
      setOperatorCompleted(pOperator);
      break;
    }
    if (completed.hasGroup && completed.hasUid) {
      SHashObj** ppUidMap =
          taosHashGet(pVtbScan->vtbGroupIdTagListMap, &completed.groupId,
                      sizeof(completed.groupId));
      if (NULL != ppUidMap) {
        code = taosHashRemove(*ppUidMap, &completed.uid, sizeof(completed.uid));
        QUERY_CHECK_CODE(code, line, _return);
        if (0 == taosHashGetSize(*ppUidMap)) {
          code = taosHashRemove(pVtbScan->vtbGroupIdTagListMap,
                                &completed.groupId, sizeof(completed.groupId));
          QUERY_CHECK_CODE(code, line, _return);
        }
      }
    } else if (completed.hasGroup) {
      code = taosHashRemove(pVtbScan->vtbGroupIdTagListMap,
                            &completed.groupId, sizeof(completed.groupId));
      QUERY_CHECK_CODE(code, line, _return);
    } else if (completed.hasUid) {
      code = taosHashRemove(pVtbScan->vtbUidTagListMap,
                            &completed.uid, sizeof(completed.uid));
      QUERY_CHECK_CODE(code, line, _return);
    }
  }

_return:
  // cross-precision conversion for VSTB AGG happens at the source Table Scan
  // (scan-level, via outputTsPrecision), so the partial Aggregate above runs in
  // vtable precision and the merged result here is already correct. No post-agg
  // conversion needed.
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

  pVtbScan->clientId = pOperator->pTaskInfo->id.taskId;

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
static int32_t buildHashIntervalOperatorParam(SDynQueryCtrlOperatorInfo* pInfo, SOperatorInfo* pIntervalOp,
                                              SOperatorParam** ppRes) {
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

  SOperatorInfo* pScanOp =
      pIntervalOp != NULL && pIntervalOp->numOfDownstream == 1 ? pIntervalOp->pDownstream[0] : NULL;
  if (pScanOp != NULL && pScanOp->operatorType == QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN) {
    SFederatedScanOperatorInfo* pFedInfo = pScanOp->info;
    SExtTableNode* pExtTable =
        pFedInfo != NULL && pFedInfo->pFedScanNode != NULL ? (SExtTableNode*)pFedInfo->pFedScanNode->pExtTable : NULL;
    QUERY_CHECK_NULL(pExtTable, code, lino, _return, TSDB_CODE_INVALID_PARA);
    code = buildBatchFederatedScanOperatorParamForVirtual(
        &pExchangeParam, 0, pVtbScan->foreignSourceMap, pExtTable->sourceName,
        pVtbScan->dstPrecision,
        0, (STimeWindow){.skey = INT64_MAX, .ekey = INT64_MIN}, false, pVtbScan->readColList);
    QUERY_CHECK_CODE(code, lino, _return);
    SForeignScanOperatorParam* pFedParam = pExchangeParam->value;
    pFedParam->sourceMetaVersion = pFedInfo->pFedScanNode->metaVersion;
    pFedParam->allowUnorderedBatch = true;
  } else {
    code = buildBatchExchangeOperatorParamForVirtual(
        &pExchangeParam, 0, NULL, 0, pVtbScan->otbVgIdToOtbInfoArrayMap,
        (STimeWindow){.skey = INT64_MAX, .ekey = INT64_MIN}, EX_SRC_TYPE_VSTB_INTERVAL_SCAN,
        QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, NULL, 0);
  }
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

  bool hasFederatedBranch = false;
  for (int32_t i = 0; i < pMergeOp->numOfDownstream; ++i) {
    if (findFederatedScanInUnaryBranch(pMergeOp->pDownstream[i]) != NULL) {
      hasFederatedBranch = true;
      break;
    }
  }

  for (int32_t i = 0; i < pMergeOp->numOfDownstream; ++i) {
    SOperatorInfo* pFedScan = findFederatedScanInUnaryBranch(pMergeOp->pDownstream[i]);

    if (pFedScan != NULL) {
      SFederatedScanOperatorInfo* pFedInfo = pFedScan->info;
      SExtTableNode* pExtTable =
          pFedInfo != NULL && pFedInfo->pFedScanNode != NULL
              ? (SExtTableNode*)pFedInfo->pFedScanNode->pExtTable
              : NULL;
      QUERY_CHECK_NULL(pExtTable, code, lino, _return, TSDB_CODE_INVALID_PARA);
      if (!foreignSourceMapHasSource(pVtbScan->foreignSourceMap, pExtTable->sourceName)) {
        continue;
      }
      code = buildBatchFederatedScanOperatorParamForVirtual(
          &pExchangeParam, i, pVtbScan->foreignSourceMap, pExtTable->sourceName,
          pVtbScan->dstPrecision, 0,
          (STimeWindow){.skey = INT64_MAX, .ekey = INT64_MIN}, false, pVtbScan->readColList);
      QUERY_CHECK_CODE(code, lino, _return);
      SForeignScanOperatorParam* pFedParam = pExchangeParam->value;
      pFedParam->sourceMetaVersion = pFedInfo->pFedScanNode->metaVersion;
      pFedParam->allowUnorderedBatch = true;
    } else {
      if (NULL == pVtbScan->otbVgIdToOtbInfoArrayMap ||
          0 == taosHashGetSize(pVtbScan->otbVgIdToOtbInfoArrayMap)) {
        continue;
      }
      EExchangeSourceType sourceType =
          hasFederatedBranch ? EX_SRC_TYPE_VSTB_INTERVAL_SCAN : EX_SRC_TYPE_VSTB_PART_INTERVAL_SCAN;
      code = buildBatchExchangeOperatorParamForVirtual(
          &pExchangeParam, i, NULL, 0, pVtbScan->otbVgIdToOtbInfoArrayMap,
          (STimeWindow){.skey = INT64_MAX, .ekey = INT64_MIN}, sourceType,
          QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, NULL, 0);
    }
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
      return buildHashIntervalOperatorParam(pInfo, pIntervalOp, ppRes);
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
                                       SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  __optr_fn_t                nextFp = NULL;
  __optr_open_fn_t           openFp = NULL;
  SMsgCb*                    pMsgCb = pTaskInfo->pMsgCb;
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
