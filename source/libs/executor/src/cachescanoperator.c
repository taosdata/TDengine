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

#include "os.h"
#include "function.h"
#include "tname.h"

#include "tdatablock.h"
#include "tmsg.h"

#include "executorimpl.h"
#include "tcompare.h"
#include "thash.h"
#include "ttypes.h"
#include "executorInt.h"

static SSDataBlock* doScanLastrow(SOperatorInfo* pOperator);
static void destroyLastrowScanOperator(void* param, int32_t numOfOutput);
static int32_t extractTargetSlotId(const SArray* pColMatchInfo, SExecTaskInfo* pTaskInfo, int32_t** pSlotIds);

SOperatorInfo* createLastrowScanOperator(SLastRowScanPhysiNode* pScanNode, SReadHandle* readHandle, SExecTaskInfo* pTaskInfo) {
  SLastrowScanInfo* pInfo = taosMemoryCalloc(1, sizeof(SLastrowScanInfo));
  SOperatorInfo*    pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pInfo->readHandle = *readHandle;
  pInfo->pRes = createResDataBlock(pScanNode->scan.node.pOutputDataBlockDesc);

  int32_t numOfCols = 0;
  pInfo->pColMatchInfo = extractColMatchInfo(pScanNode->scan.pScanCols, pScanNode->scan.node.pOutputDataBlockDesc, &numOfCols,
                                             COL_MATCH_FROM_COL_ID);
  int32_t code = extractTargetSlotId(pInfo->pColMatchInfo, pTaskInfo, &pInfo->pSlotIds);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  STableListInfo* pTableList = &pTaskInfo->tableqinfoList;

  initResultSizeInfo(pOperator, 1024);
  blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);
  pInfo->pUidList = taosArrayInit(4, sizeof(int64_t));

  // partition by tbname
  if (taosArrayGetSize(pTableList->pGroupList) == taosArrayGetSize(pTableList->pTableList)) {
    pInfo->retrieveType = LASTROW_RETRIEVE_TYPE_ALL;
    tsdbLastRowReaderOpen(pInfo->readHandle.vnode, pInfo->retrieveType, pTableList->pTableList,
                          taosArrayGetSize(pInfo->pColMatchInfo), &pInfo->pLastrowReader);
    pInfo->pBufferredRes = createOneDataBlock(pInfo->pRes, false);
    blockDataEnsureCapacity(pInfo->pBufferredRes, pOperator->resultInfo.capacity);
  } else { // by tags
    pInfo->retrieveType = LASTROW_RETRIEVE_TYPE_SINGLE;
  }

  if (pScanNode->scan.pScanPseudoCols != NULL) {
    SExprSupp* pPseudoExpr = &pInfo->pseudoExprSup;

    pPseudoExpr->pExprInfo = createExprInfo(pScanNode->scan.pScanPseudoCols, NULL, &pPseudoExpr->numOfExprs);
    pPseudoExpr->pCtx = createSqlFunctionCtx(pPseudoExpr->pExprInfo, pPseudoExpr->numOfExprs, &pPseudoExpr->rowEntryInfoOffset);
  }

  pOperator->name         = "LastrowScanOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN;
  pOperator->blocking     = false;
  pOperator->status       = OP_NOT_OPENED;
  pOperator->info         = pInfo;
  pOperator->pTaskInfo    = pTaskInfo;
  pOperator->exprSupp.numOfExprs = taosArrayGetSize(pInfo->pRes->pDataBlock);

  pOperator->fpSet =
      createOperatorFpSet(operatorDummyOpenFn, doScanLastrow, NULL, NULL, destroyLastrowScanOperator, NULL, NULL, NULL);

  pOperator->cost.openCost = 0;
  return pOperator;

  _error:
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  taosMemoryFree(pInfo);
  taosMemoryFree(pOperator);
  return NULL;
}

SSDataBlock* doScanLastrow(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SLastrowScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*    pTaskInfo = pOperator->pTaskInfo;
  STableListInfo*   pTableList = &pTaskInfo->tableqinfoList;
  int32_t           size = taosArrayGetSize(pTableList->pTableList);
  if (size == 0) {
    doSetOperatorCompleted(pOperator);
    return NULL;
  }

  blockDataCleanup(pInfo->pRes);

  // check if it is a group by tbname
  if (pInfo->retrieveType == LASTROW_RETRIEVE_TYPE_ALL) {
    if (pInfo->indexOfBufferedRes >= pInfo->pBufferredRes->info.rows) {
      blockDataCleanup(pInfo->pBufferredRes);
      taosArrayClear(pInfo->pUidList);

      int32_t code = tsdbRetrieveLastRow(pInfo->pLastrowReader, pInfo->pBufferredRes, pInfo->pSlotIds, pInfo->pUidList);
      if (code != TSDB_CODE_SUCCESS) {
        longjmp(pTaskInfo->env, code);
      }

      // check for tag values
      int32_t resultRows = pInfo->pBufferredRes->info.rows;
      ASSERT(resultRows == taosArrayGetSize(pInfo->pUidList));
      pInfo->indexOfBufferedRes = 0;
    }

    if (pInfo->indexOfBufferedRes < pInfo->pBufferredRes->info.rows) {
      for(int32_t i = 0; i < taosArrayGetSize(pInfo->pColMatchInfo); ++i) {
        SColMatchInfo* pMatchInfo = taosArrayGet(pInfo->pColMatchInfo, i);
        int32_t slotId = pMatchInfo->targetSlotId;

        SColumnInfoData* pSrc = taosArrayGet(pInfo->pBufferredRes->pDataBlock, slotId);
        SColumnInfoData* pDst = taosArrayGet(pInfo->pRes->pDataBlock, slotId);

        char* p = colDataGetData(pSrc, pInfo->indexOfBufferedRes);
        bool isNull = colDataIsNull_s(pSrc, pInfo->indexOfBufferedRes);
        colDataAppend(pDst, 0, p, isNull);
      }

      pInfo->pRes->info.uid = *(tb_uid_t*)taosArrayGet(pInfo->pUidList, pInfo->indexOfBufferedRes);
      pInfo->pRes->info.rows = 1;

      if (pInfo->pseudoExprSup.numOfExprs > 0) {
        SExprSupp* pSup = &pInfo->pseudoExprSup;
        int32_t code = addTagPseudoColumnData(&pInfo->readHandle, pSup->pExprInfo, pSup->numOfExprs, pInfo->pRes,
                               GET_TASKID(pTaskInfo));
        if (code != TSDB_CODE_SUCCESS) {
          pTaskInfo->code = code;
          return NULL;
        }
      }

      if (pTableList->map != NULL) {
        int64_t* groupId = taosHashGet(pTableList->map, &pInfo->pRes->info.uid, sizeof(int64_t));
        pInfo->pRes->info.groupId = *groupId;
      } else {
        ASSERT(taosArrayGetSize(pTableList->pTableList) == 1);
        STableKeyInfo* pKeyInfo = taosArrayGet(pTableList->pTableList, 0);
        pInfo->pRes->info.groupId = pKeyInfo->groupId;
      }

      pInfo->indexOfBufferedRes += 1;
      return pInfo->pRes;
    } else {
      doSetOperatorCompleted(pOperator);
      return NULL;
    }
  } else {
    size_t totalGroups = taosArrayGetSize(pTableList->pGroupList);

    while (pInfo->currentGroupIndex < totalGroups) {
      SArray* pGroupTableList = taosArrayGetP(pTableList->pGroupList, pInfo->currentGroupIndex);

      tsdbLastRowReaderOpen(pInfo->readHandle.vnode, pInfo->retrieveType, pGroupTableList,
                            taosArrayGetSize(pInfo->pColMatchInfo), &pInfo->pLastrowReader);
      taosArrayClear(pInfo->pUidList);

      int32_t code = tsdbRetrieveLastRow(pInfo->pLastrowReader, pInfo->pRes, pInfo->pSlotIds, pInfo->pUidList);
      if (code != TSDB_CODE_SUCCESS) {
        longjmp(pTaskInfo->env, code);
      }

      pInfo->currentGroupIndex += 1;

      // check for tag values
      if (pInfo->pRes->info.rows > 0) {
        if (pInfo->pseudoExprSup.numOfExprs > 0) {
          SExprSupp* pSup = &pInfo->pseudoExprSup;
          pInfo->pRes->info.uid = *(tb_uid_t*)taosArrayGet(pInfo->pUidList, 0);

          STableKeyInfo* pKeyInfo = taosArrayGet(pGroupTableList, 0);
          pInfo->pRes->info.groupId = pKeyInfo->groupId;

          code = addTagPseudoColumnData(&pInfo->readHandle, pSup->pExprInfo, pSup->numOfExprs, pInfo->pRes,
                                 GET_TASKID(pTaskInfo));
          if  (code != TSDB_CODE_SUCCESS) {
            pTaskInfo->code = code;
            return NULL;
          }
        }

        tsdbLastrowReaderClose(pInfo->pLastrowReader);
        return pInfo->pRes;
      }
    }

    doSetOperatorCompleted(pOperator);
    return NULL;
  }
}

void destroyLastrowScanOperator(void* param, int32_t numOfOutput) {
  SLastrowScanInfo* pInfo = (SLastrowScanInfo*)param;
  blockDataDestroy(pInfo->pRes);
  taosMemoryFreeClear(param);
}

int32_t extractTargetSlotId(const SArray* pColMatchInfo, SExecTaskInfo* pTaskInfo, int32_t** pSlotIds) {
  size_t   numOfCols = taosArrayGetSize(pColMatchInfo);

  *pSlotIds = taosMemoryMalloc(numOfCols * sizeof(int32_t));
  if (*pSlotIds == NULL)  {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColMatchInfo* pColMatch = taosArrayGet(pColMatchInfo, i);
    for (int32_t j = 0; j < pTaskInfo->schemaInfo.sw->nCols; ++j) {
      if (pColMatch->colId == pTaskInfo->schemaInfo.sw->pSchema[j].colId &&
          pColMatch->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
        (*pSlotIds)[pColMatch->targetSlotId] = -1;
        break;
      }

      if (pColMatch->colId == pTaskInfo->schemaInfo.sw->pSchema[j].colId) {
        (*pSlotIds)[pColMatch->targetSlotId] = j;
        break;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}