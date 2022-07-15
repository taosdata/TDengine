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

SOperatorInfo* createLastrowScanOperator(SLastRowScanPhysiNode* pScanNode, SReadHandle* readHandle, SArray* pTableList,
                                         SExecTaskInfo* pTaskInfo) {
  SLastrowScanInfo* pInfo = taosMemoryCalloc(1, sizeof(SLastrowScanInfo));
  SOperatorInfo*    pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pInfo->pTableList = pTableList;
  pInfo->readHandle = *readHandle;
  pInfo->pRes = createResDataBlock(pScanNode->scan.node.pOutputDataBlockDesc);

  int32_t numOfCols = 0;
  pInfo->pColMatchInfo = extractColMatchInfo(pScanNode->scan.pScanCols, pScanNode->scan.node.pOutputDataBlockDesc, &numOfCols,
                                             COL_MATCH_FROM_COL_ID);
  int32_t code = extractTargetSlotId(pInfo->pColMatchInfo, pTaskInfo, &pInfo->pSlotIds);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  tsdbLastRowReaderOpen(readHandle->vnode, LASTROW_RETRIEVE_TYPE_SINGLE, pTableList, taosArrayGetSize(pInfo->pColMatchInfo),
                        &pInfo->pLastrowReader);

  if (pScanNode->scan.pScanPseudoCols != NULL) {
    SExprSupp* pPseudoExpr = &pInfo->pseudoExprSup;

    pPseudoExpr->pExprInfo = createExprInfo(pScanNode->scan.pScanPseudoCols, NULL, &pPseudoExpr->numOfExprs);
    pPseudoExpr->pCtx = createSqlFunctionCtx(pPseudoExpr->pExprInfo, pPseudoExpr->numOfExprs, &pPseudoExpr->rowEntryInfoOffset);
  }

  pOperator->name = "LastrowScanOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN;
  pOperator->blocking = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->pTaskInfo = pTaskInfo;
  pOperator->exprSupp.numOfExprs = taosArrayGetSize(pInfo->pRes->pDataBlock);

  initResultSizeInfo(pOperator, 1024);
  blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);

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

  int32_t size = taosArrayGetSize(pInfo->pTableList);
  if (size == 0) {
    setTaskStatus(pTaskInfo, TASK_COMPLETED);
    return NULL;
  }

  // check if it is a group by tbname
  if (size == taosArrayGetSize(pInfo->pTableList)) {
    blockDataCleanup(pInfo->pRes);
    SArray* pUidList = taosArrayInit(1, sizeof(tb_uid_t));
    int32_t code = tsdbRetrieveLastRow(pInfo->pLastrowReader, pInfo->pRes, pInfo->pSlotIds, pUidList);
    if (code != TSDB_CODE_SUCCESS)  {
      longjmp(pTaskInfo->env, code);
    }

    // check for tag values
    if (pInfo->pRes->info.rows > 0 && pInfo->pseudoExprSup.numOfExprs > 0) {
      SExprSupp* pSup = &pInfo->pseudoExprSup;
      pInfo->pRes->info.uid = *(tb_uid_t*) taosArrayGet(pUidList, 0);
      addTagPseudoColumnData(&pInfo->readHandle, pSup->pExprInfo, pSup->numOfExprs, pInfo->pRes, GET_TASKID(pTaskInfo));
    }

    doSetOperatorCompleted(pOperator);
    return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
  } else {
    // todo fetch the result for each group
  }

  return pInfo->pRes->info.rows == 0 ? NULL : pInfo->pRes;
}

void destroyLastrowScanOperator(void* param, int32_t numOfOutput) {
  SLastrowScanInfo* pInfo = (SLastrowScanInfo*)param;
  blockDataDestroy(pInfo->pRes);
  tsdbLastrowReaderClose(pInfo->pLastrowReader);

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
    for (int32_t j = 0; j < pTaskInfo->schemaVer.sw->nCols; ++j) {
      if (pColMatch->colId == pTaskInfo->schemaVer.sw->pSchema[j].colId &&
          pColMatch->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
        (*pSlotIds)[pColMatch->targetSlotId] = -1;
        break;
      }

      if (pColMatch->colId == pTaskInfo->schemaVer.sw->pSchema[j].colId) {
        (*pSlotIds)[pColMatch->targetSlotId] = j;
        break;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}