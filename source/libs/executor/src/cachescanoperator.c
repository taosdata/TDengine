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

#include "function.h"
#include "os.h"
#include "tname.h"

#include "tdatablock.h"
#include "tmsg.h"

#include "executorInt.h"
#include "functionMgt.h"
#include "operator.h"
#include "querytask.h"
#include "tcompare.h"
#include "thash.h"
#include "ttypes.h"

#include "storageapi.h"

typedef struct SCacheRowsScanInfo {
  SSDataBlock*    pRes;
  SReadHandle     readHandle;
  void*           pLastrowReader;
  SColMatchInfo   matchInfo;
  int32_t*        pSlotIds;
  int32_t*        pDstSlotIds;
  SExprSupp       pseudoExprSup;
  int32_t         retrieveType;
  int32_t         currentGroupIndex;
  SSDataBlock*    pBufferedRes;
  SArray*         pUidList;
  SArray*         pCidList;
  int32_t         indexOfBufferedRes;
  STableListInfo* pTableList;
  SArray*         pFuncTypeList;
  int32_t         numOfPks;
  SColumnInfo     pkCol;
} SCacheRowsScanInfo;

static int32_t doScanCacheNext(SOperatorInfo* pOperator, SSDataBlock** ppRes);
static void    destroyCacheScanOperator(void* param);
static int32_t extractCacheScanSlotId(const SArray* pColMatchInfo, SExecTaskInfo* pTaskInfo, int32_t** pSlotIds,
                                      int32_t** pDstSlotIds);
static int32_t removeRedundantTsCol(SLastRowScanPhysiNode* pScanNode, SColMatchInfo* pColMatchInfo);

#define SCAN_ROW_TYPE(_t) ((_t) ? CACHESCAN_RETRIEVE_LAST : CACHESCAN_RETRIEVE_LAST_ROW)

static int32_t setColIdForCacheReadBlock(SSDataBlock* pBlock, SLastRowScanPhysiNode* pScan) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SNode*  pNode;
  int32_t idx = 0;
  FOREACH(pNode, pScan->pTargets) {
    if (nodeType(pNode) == QUERY_NODE_COLUMN) {
      SColumnNode*     pCol = (SColumnNode*)pNode;
      SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, idx);
      QUERY_CHECK_NULL(pColInfo, code, lino, _end, terrno);
      pColInfo->info.colId = pCol->colId;
    }
    idx++;
  }

  for (; idx < pBlock->pDataBlock->size; ++idx) {
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, idx);
    QUERY_CHECK_NULL(pColInfo, code, lino, _end, terrno);
    if (pScan->scan.pScanPseudoCols) {
      FOREACH(pNode, pScan->scan.pScanPseudoCols) {
        STargetNode* pTarget = (STargetNode*)pNode;
        if (pColInfo->info.slotId == pTarget->slotId) {
          pColInfo->info.colId = 0;
          break;
        }
      }
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t createCacherowsScanOperator(SLastRowScanPhysiNode* pScanNode, SReadHandle* readHandle,
                                    STableListInfo* pTableListInfo, SExecTaskInfo* pTaskInfo,
                                    SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  int32_t             numOfCols = 0;
  SNodeList*          pScanCols = pScanNode->scan.pScanCols;
  SCacheRowsScanInfo* pInfo = taosMemoryCalloc(1, sizeof(SCacheRowsScanInfo));
  SOperatorInfo*      pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));

  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }

  pInfo->pTableList = pTableListInfo;
  pInfo->readHandle = *readHandle;

  SDataBlockDescNode* pDescNode = pScanNode->scan.node.pOutputDataBlockDesc;
  pInfo->pRes = createDataBlockFromDescNode(pDescNode);
  QUERY_CHECK_NULL(pInfo->pRes, code, lino, _error, terrno);

  code = extractColMatchInfo(pScanCols, pDescNode, &numOfCols, COL_MATCH_FROM_COL_ID, &pInfo->matchInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  // todo: the pk information should comes from the physical plan
  // pk info may not in pScanCols, so extract primary key from pInfo->matchInfo may failed
  SSchemaInfo* pSchemaInfo = taosArrayGet(pTaskInfo->schemaInfos, 0);
  if (pSchemaInfo != NULL) {
    if (pSchemaInfo->sw->pSchema[1].flags & COL_IS_KEY) {  // is primary key
      SSchema* pColSchema = &pSchemaInfo->sw->pSchema[1];
      pInfo->numOfPks = 1;
      pInfo->pkCol.type = pColSchema->type;
      pInfo->pkCol.bytes = pColSchema->bytes;
      pInfo->pkCol.pk = 1;
    }
  } else {
    for (int32_t i = 0; i < taosArrayGetSize(pInfo->matchInfo.pList); ++i) {
      SColMatchItem* pItem = taosArrayGet(pInfo->matchInfo.pList, i);
      QUERY_CHECK_NULL(pItem, code, lino, _error, terrno);
      if (pItem->isPk) {
        pInfo->numOfPks += 1;
        pInfo->pkCol.type = pItem->dataType.type;    // only record one primary key
        pInfo->pkCol.bytes = pItem->dataType.bytes;  // only record one primary key
        pInfo->pkCol.pk = 1;
      }
    }
  }

  SArray* pCidList = taosArrayInit(numOfCols, sizeof(int16_t));
  QUERY_CHECK_NULL(pCidList, code, lino, _error, terrno);

  pInfo->pFuncTypeList = taosArrayInit(taosArrayGetSize(pScanNode->pFuncTypes), sizeof(int32_t));
  QUERY_CHECK_NULL(pInfo->pFuncTypeList, code, lino, _error, terrno);

  void* tmp = taosArrayAddAll(pInfo->pFuncTypeList, pScanNode->pFuncTypes);
  if (!tmp && taosArrayGetSize(pScanNode->pFuncTypes) > 0) {
    QUERY_CHECK_NULL(tmp, code, lino, _error, terrno);
  }

  for (int i = 0; i < TARRAY_SIZE(pInfo->matchInfo.pList); ++i) {
    SColMatchItem* pColInfo = taosArrayGet(pInfo->matchInfo.pList, i);
    QUERY_CHECK_NULL(pColInfo, code, lino, _error, terrno);

    void*          tmp = taosArrayPush(pCidList, &pColInfo->colId);
    QUERY_CHECK_NULL(tmp, code, lino, _error, terrno);
    if (pInfo->pFuncTypeList != NULL && taosArrayGetSize(pInfo->pFuncTypeList) > i) {
      void* pFuncType = taosArrayGet(pInfo->pFuncTypeList, i);
      QUERY_CHECK_NULL(pFuncType, code, lino, _error, terrno);
      pColInfo->funcType = *(int32_t*)pFuncType;
    }
  }
  pInfo->pCidList = pCidList;

  code = removeRedundantTsCol(pScanNode, &pInfo->matchInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  code = extractCacheScanSlotId(pInfo->matchInfo.pList, pTaskInfo, &pInfo->pSlotIds, &pInfo->pDstSlotIds);
  QUERY_CHECK_CODE(code, lino, _error);

  int32_t totalTables = 0;
  code = tableListGetSize(pTableListInfo, &totalTables);
  QUERY_CHECK_CODE(code, lino, _error);

  int32_t capacity = 0;

  pInfo->pUidList = taosArrayInit(4, sizeof(int64_t));
  QUERY_CHECK_NULL(pInfo->pUidList, code, lino, _error, terrno);

  // partition by tbname
  if (oneTableForEachGroup(pTableListInfo) || (totalTables == 1)) {
    pInfo->retrieveType = CACHESCAN_RETRIEVE_TYPE_ALL | SCAN_ROW_TYPE(pScanNode->ignoreNull);

    STableKeyInfo* pList = tableListGetInfo(pTableListInfo, 0);
    if (totalTables) QUERY_CHECK_NULL(pList, code, lino, _error, terrno);

    uint64_t suid = tableListGetSuid(pTableListInfo);
    code = pInfo->readHandle.api.cacheFn.openReader(pInfo->readHandle.vnode, pInfo->retrieveType, pList, totalTables,
                                                    taosArrayGetSize(pInfo->matchInfo.pList), pCidList, pInfo->pSlotIds,
                                                    suid, &pInfo->pLastrowReader, pTaskInfo->id.str,
                                                    pScanNode->pFuncTypes, &pInfo->pkCol, pInfo->numOfPks);
    QUERY_CHECK_CODE(code, lino, _error);

    capacity = TMIN(totalTables, 4096);

    pInfo->pBufferedRes = NULL;
    code = createOneDataBlock(pInfo->pRes, false, &pInfo->pBufferedRes);
    QUERY_CHECK_CODE(code, lino, _error);

    code = setColIdForCacheReadBlock(pInfo->pBufferedRes, pScanNode);
    QUERY_CHECK_CODE(code, lino, _error);

    code = blockDataEnsureCapacity(pInfo->pBufferedRes, capacity);
    QUERY_CHECK_CODE(code, lino, _error);
  } else {  // by tags
    pInfo->retrieveType = CACHESCAN_RETRIEVE_TYPE_SINGLE | SCAN_ROW_TYPE(pScanNode->ignoreNull);
    capacity = 1;  // only one row output
    code = setColIdForCacheReadBlock(pInfo->pRes, pScanNode);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  initResultSizeInfo(&pOperator->resultInfo, capacity);
  code = blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _error);

  if (pScanNode->scan.pScanPseudoCols != NULL) {
    SExprSupp* p = &pInfo->pseudoExprSup;
    code = createExprInfo(pScanNode->scan.pScanPseudoCols, NULL, &p->pExprInfo, &p->numOfExprs);
    TSDB_CHECK_CODE(code, lino, _error);

    p->pCtx =
        createSqlFunctionCtx(p->pExprInfo, p->numOfExprs, &p->rowEntryInfoOffset, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_NULL(p->pCtx, code, lino, _error, terrno);
  }

  setOperatorInfo(pOperator, "CachedRowScanOperator", QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN, false, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  pOperator->exprSupp.numOfExprs = taosArrayGetSize(pInfo->pRes->pDataBlock);

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doScanCacheNext, NULL, destroyCacheScanOperator, optrDefaultBufFn,
                                         NULL, optrDefaultGetNextExtFn, NULL);

  pOperator->cost.openCost = 0;

  *pOptrInfo = pOperator;
  return code;

_error:
  pTaskInfo->code = code;
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (pInfo != NULL) {
    pInfo->pTableList = NULL;
    destroyCacheScanOperator(pInfo);
  }
  if (pOperator != NULL) {
    pOperator->info = NULL;
    destroyOperator(pOperator);
  }
  return code;
}

static int32_t doScanCacheNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return code;
  }

  SCacheRowsScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*      pTaskInfo = pOperator->pTaskInfo;
  STableListInfo*     pTableList = pInfo->pTableList;
  SStoreCacheReader*  pReaderFn = &pInfo->readHandle.api.cacheFn;
  SSDataBlock*        pBufRes = pInfo->pBufferedRes;

  uint64_t suid = tableListGetSuid(pTableList);
  int32_t  size = 0;
  code = tableListGetSize(pTableList, &size);
  QUERY_CHECK_CODE(code, lino, _end);

  if (size == 0) {
    setOperatorCompleted(pOperator);
    (*ppRes) = NULL;
    return code;
  }

  blockDataCleanup(pInfo->pRes);

  // check if it is a group by tbname
  if ((pInfo->retrieveType & CACHESCAN_RETRIEVE_TYPE_ALL) == CACHESCAN_RETRIEVE_TYPE_ALL) {
    if (isTaskKilled(pTaskInfo)) {
      T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
    }

    if (pInfo->indexOfBufferedRes >= pBufRes->info.rows) {
      blockDataCleanup(pBufRes);
      taosArrayClear(pInfo->pUidList);

      code =
          pReaderFn->retrieveRows(pInfo->pLastrowReader, pBufRes, pInfo->pSlotIds, pInfo->pDstSlotIds, pInfo->pUidList);
      QUERY_CHECK_CODE(code, lino, _end);

      // check for tag values
      int32_t resultRows = pBufRes->info.rows;

      // the results may be null, if last values are all null
      if (resultRows != 0 && resultRows != taosArrayGetSize(pInfo->pUidList)) {
        pTaskInfo->code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(pTaskInfo->code));
        T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
      }
      pInfo->indexOfBufferedRes = 0;
    }

    SSDataBlock* pRes = pInfo->pRes;

    if (pInfo->indexOfBufferedRes < pBufRes->info.rows) {
      for (int32_t i = 0; i < taosArrayGetSize(pBufRes->pDataBlock); ++i) {
        SColumnInfoData* pCol = taosArrayGet(pRes->pDataBlock, i);
        QUERY_CHECK_NULL(pCol, code, lino, _end, terrno);
        int32_t          slotId = pCol->info.slotId;

        SColumnInfoData* pSrc = taosArrayGet(pBufRes->pDataBlock, slotId);
        QUERY_CHECK_NULL(pSrc, code, lino, _end, terrno);
        SColumnInfoData* pDst = taosArrayGet(pRes->pDataBlock, slotId);
        QUERY_CHECK_NULL(pDst, code, lino, _end, terrno);

        if (colDataIsNull_s(pSrc, pInfo->indexOfBufferedRes)) {
          colDataSetNULL(pDst, 0);
        } else {
          if (pSrc->pData) {
            char* p = colDataGetData(pSrc, pInfo->indexOfBufferedRes);
            code = colDataSetVal(pDst, 0, p, false);
            QUERY_CHECK_CODE(code, lino, _end);
          }
        }
      }

      void* pUid = taosArrayGet(pInfo->pUidList, pInfo->indexOfBufferedRes);
      QUERY_CHECK_NULL(pUid, code, lino, _end, terrno);

      pRes->info.id.uid = *(tb_uid_t*)pUid;
      pRes->info.rows = 1;
      pRes->info.scanFlag = MAIN_SCAN;

      SExprSupp* pSup = &pInfo->pseudoExprSup;
      code = addTagPseudoColumnData(&pInfo->readHandle, pSup->pExprInfo, pSup->numOfExprs, pRes, pRes->info.rows,
                                    pTaskInfo, NULL);
      if (code != TSDB_CODE_SUCCESS) {
        pTaskInfo->code = code;
        (*ppRes) = NULL;
        return code;
      }

      pRes->info.id.groupId = tableListGetTableGroupId(pTableList, pRes->info.id.uid);
      pInfo->indexOfBufferedRes += 1;
      (*ppRes) = pRes;
      return code;
    } else {
      setOperatorCompleted(pOperator);
      (*ppRes) = NULL;
      return code;
    }
  } else {
    size_t totalGroups = tableListGetOutputGroups(pTableList);

    while (pInfo->currentGroupIndex < totalGroups) {
      if (isTaskKilled(pTaskInfo)) {
        T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
      }

      STableKeyInfo* pList = NULL;
      int32_t        num = 0;

      code = tableListGetGroupList(pTableList, pInfo->currentGroupIndex, &pList, &num);
      QUERY_CHECK_CODE(code, lino, _end);

      if (NULL == pInfo->pLastrowReader) {
        int32_t tmpRes = pReaderFn->openReader(pInfo->readHandle.vnode, pInfo->retrieveType, pList, num,
                                               taosArrayGetSize(pInfo->matchInfo.pList), pInfo->pCidList,
                                               pInfo->pSlotIds, suid, &pInfo->pLastrowReader, pTaskInfo->id.str,
                                               pInfo->pFuncTypeList, &pInfo->pkCol, pInfo->numOfPks);

        if (tmpRes != TSDB_CODE_SUCCESS) {
          pInfo->currentGroupIndex += 1;
          taosArrayClear(pInfo->pUidList);
          continue;
        }
      } else {
        code = pReaderFn->reuseReader(pInfo->pLastrowReader, pList, num);
        QUERY_CHECK_CODE(code, lino, _end);
      }

      taosArrayClear(pInfo->pUidList);

      code = pReaderFn->retrieveRows(pInfo->pLastrowReader, pInfo->pRes, pInfo->pSlotIds, pInfo->pDstSlotIds,
                                     pInfo->pUidList);
      QUERY_CHECK_CODE(code, lino, _end);

      pInfo->currentGroupIndex += 1;

      // check for tag values
      if (pInfo->pRes->info.rows > 0) {
        if (pInfo->pseudoExprSup.numOfExprs > 0) {
          SExprSupp* pSup = &pInfo->pseudoExprSup;

          STableKeyInfo* pKeyInfo = &((STableKeyInfo*)pList)[0];
          pInfo->pRes->info.id.groupId = pKeyInfo->groupId;

          if (taosArrayGetSize(pInfo->pUidList) > 0) {
            void* pUid = taosArrayGet(pInfo->pUidList, 0);
            QUERY_CHECK_NULL(pUid, code, lino, _end, terrno);
            pInfo->pRes->info.id.uid = *(tb_uid_t*)pUid;
            code = addTagPseudoColumnData(&pInfo->readHandle, pSup->pExprInfo, pSup->numOfExprs, pInfo->pRes,
                                          pInfo->pRes->info.rows, pTaskInfo, NULL);
            if (code != TSDB_CODE_SUCCESS) {
              pTaskInfo->code = code;
              (*ppRes) = NULL;
              return code;
            }
          }
        }

        // pInfo->pLastrowReader = tsdbCacherowsReaderClose(pInfo->pLastrowReader);
        (*ppRes) = pInfo->pRes;
        return code;
      } else {
        // pInfo->pLastrowReader = tsdbCacherowsReaderClose(pInfo->pLastrowReader);
      }
    }

    pReaderFn->closeReader(pInfo->pLastrowReader);
    pInfo->pLastrowReader = NULL;
    setOperatorCompleted(pOperator);
    (*ppRes) = NULL;
    return code;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return code;
}

void destroyCacheScanOperator(void* param) {
  SCacheRowsScanInfo* pInfo = (SCacheRowsScanInfo*)param;
  blockDataDestroy(pInfo->pRes);
  blockDataDestroy(pInfo->pBufferedRes);
  taosMemoryFreeClear(pInfo->pSlotIds);
  taosMemoryFreeClear(pInfo->pDstSlotIds);
  taosArrayDestroy(pInfo->pCidList);
  taosArrayDestroy(pInfo->pFuncTypeList);
  taosArrayDestroy(pInfo->pUidList);
  taosArrayDestroy(pInfo->matchInfo.pList);
  tableListDestroy(pInfo->pTableList);

  if (pInfo->pLastrowReader != NULL && pInfo->readHandle.api.cacheFn.closeReader != NULL) {
    pInfo->readHandle.api.cacheFn.closeReader(pInfo->pLastrowReader);
    pInfo->pLastrowReader = NULL;
  }

  cleanupExprSupp(&pInfo->pseudoExprSup);
  taosMemoryFreeClear(param);
}

int32_t extractCacheScanSlotId(const SArray* pColMatchInfo, SExecTaskInfo* pTaskInfo, int32_t** pSlotIds,
                               int32_t** pDstSlotIds) {
  size_t numOfCols = taosArrayGetSize(pColMatchInfo);

  *pSlotIds = taosMemoryMalloc(numOfCols * sizeof(int32_t));
  if (*pSlotIds == NULL) {
    return terrno;
  }

  *pDstSlotIds = taosMemoryMalloc(numOfCols * sizeof(int32_t));
  if (*pDstSlotIds == NULL) {
    taosMemoryFreeClear(*pSlotIds);
    return terrno;
  }

  SSchemaInfo*    pSchemaInfo = taosArrayGetLast(pTaskInfo->schemaInfos);
  SSchemaWrapper* pWrapper = pSchemaInfo->sw;

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColMatchItem* pColMatch = taosArrayGet(pColMatchInfo, i);
    if (!pColMatch) {
      return terrno;
    }
    bool           found = false;
    for (int32_t j = 0; j < pWrapper->nCols; ++j) {
      /*      if (pColMatch->colId == pWrapper->pSchema[j].colId && pColMatch->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
        (*pSlotIds)[pColMatch->dstSlotId] = -1;
        break;
        }*/

      if (pColMatch->colId == pWrapper->pSchema[j].colId) {
        (*pSlotIds)[i] = j;
        (*pDstSlotIds)[i] = pColMatch->dstSlotId;
        found = true;
        break;
      }
    }
    if (!found) {
      (*pSlotIds)[i] = -1;
      (*pDstSlotIds)[i] = pColMatch->dstSlotId;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t removeRedundantTsCol(SLastRowScanPhysiNode* pScanNode, SColMatchInfo* pColMatchInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (!pScanNode->ignoreNull) {  // retrieve cached last value
    return TSDB_CODE_SUCCESS;
  }

  size_t  size = taosArrayGetSize(pColMatchInfo->pList);
  SArray* pMatchInfo = taosArrayInit(size, sizeof(SColMatchItem));
  QUERY_CHECK_NULL(pMatchInfo, code, lino, _end, terrno);

  for (int32_t i = 0; i < size; ++i) {
    SColMatchItem* pColInfo = taosArrayGet(pColMatchInfo->pList, i);
    if (!pColInfo) {
      return terrno;
    }

    int32_t    slotId = pColInfo->dstSlotId;
    SNodeList* pList = pScanNode->scan.node.pOutputDataBlockDesc->pSlots;

    SSlotDescNode* pDesc = (SSlotDescNode*)nodesListGetNode(pList, slotId);
    QUERY_CHECK_NULL(pDesc, code, lino, _end, terrno);

    if (pDesc->dataType.type != TSDB_DATA_TYPE_TIMESTAMP) {
      void* tmp = taosArrayPush(pMatchInfo, pColInfo);
      QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
    } else if (FUNCTION_TYPE_CACHE_LAST_ROW == pColInfo->funcType) {
      void* tmp = taosArrayPush(pMatchInfo, pColInfo);
      QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
    }
  }

  taosArrayDestroy(pColMatchInfo->pList);
  pColMatchInfo->pList = pMatchInfo;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}
