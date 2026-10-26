/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
#include "functionMgt.h"
#include "operator.h"
#include "querytask.h"
#include "taoserror.h"
#include "tdatablock.h"

typedef struct SProjectProcessByRowState {
  SqlFunctionCtx* pCtx;
  uint64_t        baseGId;
  int32_t         winOutIdx;
} SProjectProcessByRowState;

typedef struct SProjectOperatorInfo {
  SOptrBasicInfo binfo;
  SAggSupporter  aggSup;
  SArray*        pPseudoColInfo;
  SLimitInfo     limitInfo;
  bool           mergeDataBlocks;
  SSDataBlock*   pFinalRes;
  bool           inputIgnoreGroup;
  bool           outputIgnoreGroup;
  SArray*                 pInputWinIdx;
  SArray*                 pOutputWinIdx;
  SStreamRuntimeFuncInfo* pPublishedStream;
  SArray*                 pProcessByRowStates;
  bool                    pendingEog;
  uint64_t                pendingEogGroupId;
  bool                    drainingZeroRows;
  uint64_t                drainGroupId;
  uint64_t                bufferedGroupId;
} SProjectOperatorInfo;

typedef struct SIndefOperatorInfo {
  SOptrBasicInfo binfo;
  SAggSupporter  aggSup;
  SArray*        pPseudoColInfo;
  SExprSupp      scalarSup;
  uint64_t       groupId;
  SSDataBlock*   pNextGroupRes;
} SIndefOperatorInfo;

static int32_t      doGenerateSourceData(SOperatorInfo* pOperator);
static int32_t      doProjectOperation(SOperatorInfo* pOperator, SSDataBlock** pResBlock);
static int32_t      doApplyIndefinitFunction(SOperatorInfo* pOperator, SSDataBlock** pResBlock);
static int32_t      resetProcessByRowCtx(SqlFunctionCtx* pCtx);
static int32_t      projectResetProcessByRowStates(SProjectOperatorInfo* pInfo);
static int32_t      projectApplyFunctionsWithLineage(SExprInfo* pExpr, SSDataBlock* pResult, SSDataBlock* pSrcBlock,
                                                     SqlFunctionCtx* pCtx, int32_t numOfOutput, SArray* pPseudoList,
                                                     const void* pExtraParams, bool doSelectFunc, bool hasIndefRowsFunc,
                                                     SExecTaskInfo* pTaskInfo, const SArray* pInputWinIdx,
                                                     SArray* pOutputWinIdx, SProjectOperatorInfo* pProjectInfo);
int32_t projectApplyOperator(SExprInfo* pExpr, SSDataBlock* pResult, SSDataBlock* pSrcBlock, int32_t outputSlotId,
                             int64_t* numOfRows, bool createNewColModel, const void* pExtraParams,
                             const SScalarExtraInfo* pScalarExtra);

static bool isMainFirstScanOperator(const SOperatorInfo* pOperator) {
  if (pOperator == NULL || pOperator->pPhyNode == NULL) {
    return false;
  }
  if (pOperator->operatorType != QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN &&
      pOperator->operatorType != QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN) {
    return false;
  }

  const STableScanPhysiNode* pScanNode =
      (const STableScanPhysiNode*)pOperator->pPhyNode;
  return pScanNode->interpMode == INTERP_MODE_MAIN_FIRST ||
         pScanNode->interpMode == INTERP_MODE_GROUP_FIRST;
}

static bool hasLagLeadFunc(const SExprSupp* pSup) {
  if (pSup == NULL || pSup->pCtx == NULL) {
    return false;
  }

  for (int32_t i = 0; i < pSup->numOfExprs; ++i) {
    EFunctionType type = fmGetFuncTypeFromId(pSup->pCtx[i].functionId);
    if (type == FUNCTION_TYPE_LAG || type == FUNCTION_TYPE_LEAD) {
      return true;
    }
  }

  return false;
}

static bool projectUsesStrictMultiGroupProtocol(const SOperatorInfo* pOperator) {
  const SStreamRuntimeFuncInfo* pStream =
      pOperator == NULL || pOperator->pTaskInfo == NULL ? NULL : GET_STM_RTINFO(pOperator->pTaskInfo);
  return pOperator != NULL && pOperator->pTaskInfo != NULL &&
         pOperator->pTaskInfo->execModel == OPTR_EXEC_MODEL_STREAM && isStaticMultiGroupJoinQualified(pOperator) &&
         pStream != NULL && pStream->isMultiGroupCalc;
}

static int32_t projectValidateMultiGroupProtocolMode(const SOperatorInfo* pOperator) {
  if (pOperator == NULL || pOperator->pTaskInfo == NULL || pOperator->pTaskInfo->execModel != OPTR_EXEC_MODEL_STREAM ||
      !isStaticMultiGroupJoinQualified(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  const SStreamRuntimeFuncInfo* pStream = GET_STM_RTINFO(pOperator->pTaskInfo);
  if (pStream == NULL || !pStream->isMultiGroupCalc) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t projectEnsureLineageArray(SArray** ppLineage) {
  if (*ppLineage == NULL) {
    *ppLineage = taosArrayInit(4, sizeof(int64_t));
    if (*ppLineage == NULL) {
      return terrno;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static void projectDecodeLineageSegment(const int64_t* pValue, int32_t* pWinOutIdx, int32_t* pRowStartIdx) {
  const int32_t* pPair = (const int32_t*)pValue;
  *pWinOutIdx = pPair[0];
  *pRowStartIdx = pPair[1];
}

static int32_t projectAppendLineageSegment(SArray* pLineage, int32_t winOutIdx, int32_t rowStartIdx) {
  int32_t size = taosArrayGetSize(pLineage);
  if (size > 0) {
    int64_t* pLast = taosArrayGet(pLineage, size - 1);
    if (pLast == NULL) {
      return TSDB_CODE_STREAM_INTERNAL_ERROR;
    }
    int32_t lastWinOutIdx = 0;
    int32_t lastRowStartIdx = 0;
    projectDecodeLineageSegment(pLast, &lastWinOutIdx, &lastRowStartIdx);
    if (lastWinOutIdx == winOutIdx) {
      return TSDB_CODE_SUCCESS;
    }
    if (rowStartIdx <= lastRowStartIdx) {
      return TSDB_CODE_STREAM_INTERNAL_ERROR;
    }
  }

  int64_t  value = 0;
  int32_t* pPair = (int32_t*)&value;
  pPair[0] = winOutIdx;
  pPair[1] = rowStartIdx;
  return taosArrayPush(pLineage, &value) == NULL ? terrno : TSDB_CODE_SUCCESS;
}

static int32_t projectValidateLineage(const SArray* pLineage, int32_t rows) {
  if (rows < 0 || (rows == 0 && pLineage != NULL && taosArrayGetSize(pLineage) != 0) ||
      (rows > 0 && (pLineage == NULL || taosArrayGetSize(pLineage) == 0))) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  if (rows == 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t previousStart = -1;
  int32_t size = taosArrayGetSize(pLineage);
  for (int32_t i = 0; i < size; ++i) {
    int64_t* pValue = taosArrayGet(pLineage, i);
    if (pValue == NULL) {
      return TSDB_CODE_STREAM_INTERNAL_ERROR;
    }
    int32_t winOutIdx = 0;
    int32_t rowStartIdx = 0;
    projectDecodeLineageSegment(pValue, &winOutIdx, &rowStartIdx);
    if ((i == 0 && rowStartIdx != 0) || rowStartIdx <= previousStart || rowStartIdx >= rows) {
      return TSDB_CODE_STREAM_INTERNAL_ERROR;
    }
    previousStart = rowStartIdx;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t projectCopyLineage(SArray** ppOutput, const SArray* pInput, int32_t rows) {
  int32_t code = projectValidateLineage(pInput, rows);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  code = projectEnsureLineageArray(ppOutput);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  taosArrayClear(*ppOutput);
  for (int32_t i = 0; i < taosArrayGetSize(pInput); ++i) {
    int64_t* pValue = taosArrayGet(pInput, i);
    int32_t  winOutIdx = 0;
    int32_t  rowStartIdx = 0;
    projectDecodeLineageSegment(pValue, &winOutIdx, &rowStartIdx);
    code = projectAppendLineageSegment(*ppOutput, winOutIdx, rowStartIdx);
    if (code != TSDB_CODE_SUCCESS) {
      taosArrayClear(*ppOutput);
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t projectSnapshotInputLineage(SProjectOperatorInfo* pInfo, const SArray* pChildIdx, int32_t rows) {
  int32_t code = projectCopyLineage(&pInfo->pInputWinIdx, pChildIdx, rows);
  if (code != TSDB_CODE_SUCCESS && pInfo->pInputWinIdx != NULL) {
    taosArrayClear(pInfo->pInputWinIdx);
  }
  return code;
}

static int32_t projectReplaceLineage(SArray* pDest, SArray* pSource) {
  taosArrayClear(pDest);
  int32_t size = taosArrayGetSize(pSource);
  if (size > 0 && taosArrayAddBatch(pDest, TARRAY_DATA(pSource), size) == NULL) {
    taosArrayClear(pDest);
    return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t projectRebuildLineageByFilter(SProjectOperatorInfo* pInfo, int32_t rowsBefore,
                                             const SColumnInfoData* pFilterResult) {
  if (pInfo == NULL || pInfo->pOutputWinIdx == NULL || pFilterResult == NULL || rowsBefore <= 0) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  int32_t code = projectValidateLineage(pInfo->pOutputWinIdx, rowsBefore);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SArray* pRebuilt = taosArrayInit(taosArrayGetSize(pInfo->pOutputWinIdx), sizeof(int64_t));
  if (pRebuilt == NULL) {
    return terrno;
  }

  const int8_t* pIndicator = (const int8_t*)pFilterResult->pData;
  int32_t       outputRow = 0;
  int32_t       segmentCount = taosArrayGetSize(pInfo->pOutputWinIdx);
  for (int32_t i = 0; i < segmentCount && code == TSDB_CODE_SUCCESS; ++i) {
    int64_t* pCurrent = taosArrayGet(pInfo->pOutputWinIdx, i);
    int32_t  winOutIdx = 0;
    int32_t  rowStartIdx = 0;
    projectDecodeLineageSegment(pCurrent, &winOutIdx, &rowStartIdx);
    int32_t rowEndIdx = rowsBefore;
    if (i + 1 < segmentCount) {
      int64_t* pNext = taosArrayGet(pInfo->pOutputWinIdx, i + 1);
      int32_t  nextWinOutIdx = 0;
      projectDecodeLineageSegment(pNext, &nextWinOutIdx, &rowEndIdx);
    }
    for (int32_t row = rowStartIdx; row < rowEndIdx; ++row) {
      if (pIndicator[row]) {
        code = projectAppendLineageSegment(pRebuilt, winOutIdx, outputRow++);
        if (code != TSDB_CODE_SUCCESS) {
          break;
        }
      }
    }
  }

  if (code == TSDB_CODE_SUCCESS) {
    code = projectReplaceLineage(pInfo->pOutputWinIdx, pRebuilt);
  }
  taosArrayDestroy(pRebuilt);
  return code;
}

static int32_t projectSliceLineageArray(SArray* pLineage, int32_t startRow, int32_t rows) {
  if (pLineage == NULL || startRow < 0 || rows < 0) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  if (rows == 0) {
    taosArrayClear(pLineage);
    return TSDB_CODE_SUCCESS;
  }

  int32_t segmentCount = taosArrayGetSize(pLineage);
  SArray* pSliced = taosArrayInit(segmentCount, sizeof(int64_t));
  if (pSliced == NULL) {
    return terrno;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  int32_t sliceEnd = startRow + rows;
  int32_t previousStart = -1;
  for (int32_t i = 0; i < segmentCount && code == TSDB_CODE_SUCCESS; ++i) {
    int64_t* pCurrent = taosArrayGet(pLineage, i);
    if (pCurrent == NULL) {
      code = TSDB_CODE_STREAM_INTERNAL_ERROR;
      break;
    }
    int32_t winOutIdx = 0;
    int32_t rowStartIdx = 0;
    projectDecodeLineageSegment(pCurrent, &winOutIdx, &rowStartIdx);
    if ((i == 0 && rowStartIdx != 0) || rowStartIdx <= previousStart) {
      code = TSDB_CODE_STREAM_INTERNAL_ERROR;
      break;
    }
    previousStart = rowStartIdx;
    int32_t rowEndIdx = sliceEnd;
    if (i + 1 < segmentCount) {
      int64_t* pNext = taosArrayGet(pLineage, i + 1);
      if (pNext == NULL) {
        code = TSDB_CODE_STREAM_INTERNAL_ERROR;
        break;
      }
      int32_t nextWinOutIdx = 0;
      projectDecodeLineageSegment(pNext, &nextWinOutIdx, &rowEndIdx);
    }
    int32_t retainedStart = TMAX(rowStartIdx, startRow);
    int32_t retainedEnd = TMIN(rowEndIdx, sliceEnd);
    if (retainedEnd > retainedStart) {
      code = projectAppendLineageSegment(pSliced, winOutIdx, retainedStart - startRow);
    }
  }

  if (code == TSDB_CODE_SUCCESS) {
    code = projectReplaceLineage(pLineage, pSliced);
  }
  taosArrayDestroy(pSliced);
  return code;
}

static int32_t projectSliceLineage(SProjectOperatorInfo* pInfo, int32_t startRow, int32_t rows) {
  return pInfo == NULL ? TSDB_CODE_STREAM_INTERNAL_ERROR
                       : projectSliceLineageArray(pInfo->pOutputWinIdx, startRow, rows);
}

static int32_t projectAppendMergedLineage(SProjectOperatorInfo* pInfo, const SArray* pSource, int32_t sourceRows,
                                          int32_t outputOffset) {
  if (pInfo == NULL || outputOffset < 0) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  int32_t code = projectEnsureLineageArray(&pInfo->pOutputWinIdx);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  code = projectValidateLineage(pSource, sourceRows);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  for (int32_t i = 0; i < taosArrayGetSize(pSource); ++i) {
    int64_t* pValue = taosArrayGet(pSource, i);
    int32_t  winOutIdx = 0;
    int32_t  rowStartIdx = 0;
    projectDecodeLineageSegment(pValue, &winOutIdx, &rowStartIdx);
    code = projectAppendLineageSegment(pInfo->pOutputWinIdx, winOutIdx, outputOffset + rowStartIdx);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t projectPublishLineage(SProjectOperatorInfo* pInfo, SStreamRuntimeFuncInfo* pStream, int32_t outputRows) {
  if (pInfo == NULL || pStream == NULL || pInfo->pOutputWinIdx == NULL) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  int32_t code = projectValidateLineage(pInfo->pOutputWinIdx, outputRows);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  pStream->pStreamBlkWinIdx = pInfo->pOutputWinIdx;
  pInfo->pPublishedStream = pStream;
  return TSDB_CODE_SUCCESS;
}

static int32_t projectPrepareEog(SProjectOperatorInfo* pInfo, SStreamRuntimeFuncInfo* pStream, SSDataBlock* pResult,
                                 uint64_t baseGId) {
  int32_t code = projectResetProcessByRowStates(pInfo);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  blockDataCleanup(pResult);
  code = projectEnsureLineageArray(&pInfo->pOutputWinIdx);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  code = qSetMultiGroupEogBlock(pResult, baseGId);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  taosArrayClear(pInfo->pOutputWinIdx);
  return projectPublishLineage(pInfo, pStream, 0);
}

static void projectClearZeroRowDrain(SProjectOperatorInfo* pInfo) {
  pInfo->drainingZeroRows = false;
  pInfo->drainGroupId = 0;
}

static int32_t projectStartZeroRowDrain(SProjectOperatorInfo* pInfo, uint64_t baseGId) {
  if (pInfo == NULL || baseGId == 0 || (pInfo->drainingZeroRows && pInfo->drainGroupId != baseGId)) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  pInfo->drainingZeroRows = true;
  pInfo->drainGroupId = baseGId;
  return TSDB_CODE_SUCCESS;
}

static int32_t projectValidateZeroRowDrainInput(const SProjectOperatorInfo* pInfo, const SSDataBlock* pBlock) {
  if (pInfo == NULL || pBlock == NULL || !pInfo->drainingZeroRows) {
    return TSDB_CODE_SUCCESS;
  }
  return pBlock->info.id.baseGId == pInfo->drainGroupId && pBlock->info.id.groupId == pInfo->drainGroupId
             ? TSDB_CODE_SUCCESS
             : TSDB_CODE_STREAM_INTERNAL_ERROR;
}

static int32_t projectStartGroupBuffer(SProjectOperatorInfo* pInfo, const SSDataBlock* pBlock) {
  if (pInfo == NULL || pBlock == NULL || pBlock->info.id.baseGId == 0 ||
      pBlock->info.id.groupId != pBlock->info.id.baseGId ||
      (pInfo->bufferedGroupId != 0 && pInfo->bufferedGroupId != pBlock->info.id.baseGId)) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  pInfo->bufferedGroupId = pBlock->info.id.baseGId;
  return TSDB_CODE_SUCCESS;
}

static int32_t projectValidateGroupBufferInput(const SProjectOperatorInfo* pInfo, const SSDataBlock* pBlock) {
  if (pInfo == NULL || pBlock == NULL || pInfo->bufferedGroupId == 0) {
    return TSDB_CODE_SUCCESS;
  }
  return pBlock->info.id.baseGId == pInfo->bufferedGroupId && pBlock->info.id.groupId == pInfo->bufferedGroupId
             ? TSDB_CODE_SUCCESS
             : TSDB_CODE_STREAM_INTERNAL_ERROR;
}

static void destroyProjectOperatorInfo(void* param) {
  if (NULL == param) {
    return;
  }

  SProjectOperatorInfo* pInfo = (SProjectOperatorInfo*)param;
  if (pInfo->pPublishedStream != NULL && (pInfo->pPublishedStream->pStreamBlkWinIdx == pInfo->pInputWinIdx ||
                                          pInfo->pPublishedStream->pStreamBlkWinIdx == pInfo->pOutputWinIdx)) {
    pInfo->pPublishedStream->pStreamBlkWinIdx = NULL;
  }
  cleanupBasicInfo(&pInfo->binfo);
  cleanupAggSup(&pInfo->aggSup);
  taosArrayDestroy(pInfo->pPseudoColInfo);
  taosArrayDestroy(pInfo->pInputWinIdx);
  taosArrayDestroy(pInfo->pOutputWinIdx);
  taosArrayDestroy(pInfo->pProcessByRowStates);

  blockDataDestroy(pInfo->pFinalRes);
  taosMemoryFreeClear(param);
}

static void destroyIndefinitOperatorInfo(void* param) {
  SIndefOperatorInfo* pInfo = (SIndefOperatorInfo*)param;
  if (pInfo == NULL) {
    return;
  }

  cleanupBasicInfo(&pInfo->binfo);
  taosArrayDestroy(pInfo->pPseudoColInfo);
  cleanupAggSup(&pInfo->aggSup);
  cleanupExprSupp(&pInfo->scalarSup);

  taosMemoryFreeClear(param);
}

static int32_t resetProcessByRowCtx(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  char*                pOutput = pCtx->pOutput;

  if (pResInfo->initialized && pCtx->fpSet.cleanup != NULL) {
    pCtx->fpSet.cleanup(pCtx);
  }

  funcInputRowIterCleanup(&pCtx->rowIter);
  pResInfo->initialized = false;
  pResInfo->numOfRes = 0;
  pCtx->bInputFinished = false;

  pCtx->pOutput = NULL;
  int32_t code = pCtx->fpSet.init(pCtx, pResInfo);
  pCtx->pOutput = pOutput;
  return code;
}

static int32_t projectPrepareProcessByRowCtx(SProjectOperatorInfo* pInfo, SqlFunctionCtx* pCtx, uint64_t baseGId,
                                             int32_t winOutIdx) {
  if (pInfo == NULL || pCtx == NULL || baseGId == 0) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  if (pInfo->pProcessByRowStates == NULL) {
    pInfo->pProcessByRowStates = taosArrayInit(4, sizeof(SProjectProcessByRowState));
    if (pInfo->pProcessByRowStates == NULL) {
      return terrno;
    }
  }

  SProjectProcessByRowState* pState = NULL;
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pProcessByRowStates); ++i) {
    SProjectProcessByRowState* pCurrent = taosArrayGet(pInfo->pProcessByRowStates, i);
    if (pCurrent != NULL && pCurrent->pCtx == pCtx) {
      pState = pCurrent;
      break;
    }
  }
  if (pState != NULL && pState->baseGId == baseGId && pState->winOutIdx == winOutIdx) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = resetProcessByRowCtx(pCtx);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (pState != NULL) {
    pState->baseGId = baseGId;
    pState->winOutIdx = winOutIdx;
    return TSDB_CODE_SUCCESS;
  }

  SProjectProcessByRowState state = {.pCtx = pCtx, .baseGId = baseGId, .winOutIdx = winOutIdx};
  return taosArrayPush(pInfo->pProcessByRowStates, &state) == NULL ? terrno : TSDB_CODE_SUCCESS;
}

static int32_t projectResetProcessByRowStates(SProjectOperatorInfo* pInfo) {
  if (pInfo == NULL || pInfo->pProcessByRowStates == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pProcessByRowStates); ++i) {
    SProjectProcessByRowState* pState = taosArrayGet(pInfo->pProcessByRowStates, i);
    if (pState == NULL || pState->pCtx == NULL) {
      return TSDB_CODE_STREAM_INTERNAL_ERROR;
    }
    code = resetProcessByRowCtx(pState->pCtx);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  taosArrayClear(pInfo->pProcessByRowStates);
  return TSDB_CODE_SUCCESS;
}

static bool allProcessByRowCtxSameFuncId(SArray* pProcessByRowFunctionCtx) {
  if (pProcessByRowFunctionCtx == NULL || taosArrayGetSize(pProcessByRowFunctionCtx) <= 1) {
    return true;
  }

  SqlFunctionCtx** ppFirstCtx = taosArrayGet(pProcessByRowFunctionCtx, 0);
  if (ppFirstCtx == NULL || *ppFirstCtx == NULL) {
    return false;
  }

  int32_t funcId = (*ppFirstCtx)->functionId;
  for (int32_t i = 1; i < taosArrayGetSize(pProcessByRowFunctionCtx); ++i) {
    SqlFunctionCtx** ppCtx = taosArrayGet(pProcessByRowFunctionCtx, i);
    if (ppCtx == NULL || *ppCtx == NULL || (*ppCtx)->functionId != funcId) {
      return false;
    }
  }
  return true;
}

static int32_t projectValidateProcessByRowResultRows(SArray* pGroupedCtxArray, uint64_t inputRows,
                                                     uint64_t* pOutputRows) {
  if (pGroupedCtxArray == NULL || pOutputRows == NULL || taosArrayGetSize(pGroupedCtxArray) <= 0) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  uint64_t rows = 0;
  for (int32_t i = 0; i < taosArrayGetSize(pGroupedCtxArray); ++i) {
    SqlFunctionCtx** ppCtx = taosArrayGet(pGroupedCtxArray, i);
    if (ppCtx == NULL || *ppCtx == NULL || (*ppCtx)->resultInfo == NULL) {
      return TSDB_CODE_STREAM_INTERNAL_ERROR;
    }

    uint64_t currentRows = (*ppCtx)->resultInfo->numOfRes;
    if (currentRows > inputRows || (i > 0 && currentRows != rows)) {
      qError("strict Project process-by-row contract violated, functionId:%d, inputRows:%" PRIu64
             ", outputRows:%" PRIu64,
             (*ppCtx)->functionId, inputRows, currentRows);
      return TSDB_CODE_STREAM_INTERNAL_ERROR;
    }
    rows = currentRows;
  }

  *pOutputRows = rows;
  return TSDB_CODE_SUCCESS;
}

static bool projectLineageEqual(const SArray* pLeft, const SArray* pRight) {
  if (pLeft == NULL || pRight == NULL || taosArrayGetSize(pLeft) != taosArrayGetSize(pRight)) {
    return false;
  }
  for (int32_t i = 0; i < taosArrayGetSize(pLeft); ++i) {
    const int64_t* pLeftValue = taosArrayGet(pLeft, i);
    const int64_t* pRightValue = taosArrayGet(pRight, i);
    if (pLeftValue == NULL || pRightValue == NULL || *pLeftValue != *pRightValue) {
      return false;
    }
  }
  return true;
}

static int32_t processByRowInExternalWindows(SArray* pGroupedCtxArray, SSDataBlock* pSrcBlock,
                                             const SArray* pSourceWinIdx, SArray* pOutputWinIdx,
                                             SProjectOperatorInfo* pProjectInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  int32_t  ctxNum = taosArrayGetSize(pGroupedCtxArray);
  int32_t  idxNum = taosArrayGetSize(pSourceWinIdx);
  int32_t  totalRows = 0;
  SArray*  pInputWinIdx = NULL;
  int32_t* pStartRows = NULL;
  int64_t* pNumRows = NULL;
  int32_t* pOffsets = NULL;

  pInputWinIdx = taosArrayInit(idxNum, sizeof(int64_t));
  TSDB_CHECK_NULL(pInputWinIdx, code, lino, _exit, terrno);
  TSDB_CHECK_NULL(taosArrayAddBatch(pInputWinIdx, TARRAY_DATA(pSourceWinIdx), idxNum), code, lino, _exit, terrno);

  pStartRows = taosMemoryCalloc(ctxNum, sizeof(int32_t));
  TSDB_CHECK_NULL(pStartRows, code, lino, _exit, terrno);
  pNumRows = taosMemoryCalloc(ctxNum, sizeof(int64_t));
  TSDB_CHECK_NULL(pNumRows, code, lino, _exit, terrno);
  pOffsets = taosMemoryCalloc(ctxNum, sizeof(int32_t));
  TSDB_CHECK_NULL(pOffsets, code, lino, _exit, terrno);

  for (int32_t i = 0; i < ctxNum; ++i) {
    SqlFunctionCtx** ppCtx = taosArrayGet(pGroupedCtxArray, i);
    TSDB_CHECK_NULL(ppCtx, code, lino, _exit, terrno);
    TSDB_CHECK_NULL(*ppCtx, code, lino, _exit, terrno);
    pStartRows[i] = (*ppCtx)->input.startRowIndex;
    pNumRows[i] = (*ppCtx)->input.numOfRows;
    pOffsets[i] = (*ppCtx)->offset;
  }

  taosArrayClear(pOutputWinIdx);

  for (int32_t i = 0; i < idxNum; ++i) {
    int64_t* pCurr = taosArrayGet(pInputWinIdx, i);
    int32_t* pCurrPair = (int32_t*)pCurr;
    int32_t  winIdx = pCurrPair[0];
    int32_t  rowStart = pCurrPair[1];
    int32_t  rowEnd = pSrcBlock->info.rows;

    if (i + 1 < idxNum) {
      int64_t* pNext = taosArrayGet(pInputWinIdx, i + 1);
      rowEnd = ((int32_t*)pNext)[1];
    }

    if (rowEnd <= rowStart) {
      continue;
    }

    for (int32_t j = 0; j < ctxNum; ++j) {
      SqlFunctionCtx** ppCtx = taosArrayGet(pGroupedCtxArray, j);
      SqlFunctionCtx*  pCtx = *ppCtx;

      pCtx->input.startRowIndex = rowStart;
      pCtx->input.numOfRows = rowEnd - rowStart;
      pCtx->offset = pOffsets[j] + totalRows;
      if (pProjectInfo != NULL) {
        TAOS_CHECK_EXIT(projectPrepareProcessByRowCtx(pProjectInfo, pCtx, pSrcBlock->info.id.baseGId, winIdx));
      } else {
        TAOS_CHECK_EXIT(resetProcessByRowCtx(pCtx));
      }
    }

    SqlFunctionCtx** ppFirstCtx = taosArrayGet(pGroupedCtxArray, 0);
    TAOS_CHECK_EXIT((*ppFirstCtx)->fpSet.processFuncByRow(pGroupedCtxArray));

    // Strict Project reserves one result row per logical input row. Every registered process-by-row callback must
    // honor that bound before writing because the callback interface has no capacity preflight hook. Keep this
    // executable postcondition before publishing row counts or lineage.
    uint64_t winRows = 0;
    if (pProjectInfo != NULL) {
      TAOS_CHECK_EXIT(projectValidateProcessByRowResultRows(pGroupedCtxArray, (uint64_t)(rowEnd - rowStart), &winRows));
    } else {
      winRows = (*ppFirstCtx)->resultInfo->numOfRes;
    }
    if (winRows > 0) {
      if (winRows > INT32_MAX - totalRows) {
        qError("external window process-by-row result rows overflow, totalRows:%d, winRows:%" PRIu64, totalRows,
               winRows);
        code = TSDB_CODE_OUT_OF_RANGE;
        goto _exit;
      }

      int64_t  val = 0;
      int32_t* pOutPair = (int32_t*)&val;
      pOutPair[0] = winIdx;
      pOutPair[1] = totalRows;
      TSDB_CHECK_NULL(taosArrayPush(pOutputWinIdx, &val), code, lino, _exit, terrno);
      totalRows += (int32_t)winRows;
    }
  }

  for (int32_t i = 0; i < ctxNum; ++i) {
    SqlFunctionCtx** ppCtx = taosArrayGet(pGroupedCtxArray, i);
    SqlFunctionCtx*  pCtx = *ppCtx;
    pCtx->input.startRowIndex = pStartRows[i];
    pCtx->input.numOfRows = pNumRows[i];
    pCtx->offset = pOffsets[i];
    pCtx->resultInfo->numOfRes = totalRows;
  }

_exit:
  if (pInputWinIdx != NULL) {
    taosArrayDestroy(pInputWinIdx);
  }
  taosMemoryFreeClear(pStartRows);
  taosMemoryFreeClear(pNumRows);
  taosMemoryFreeClear(pOffsets);
  return code;
}

static int32_t assignPlaceHolderInExternalWindows(SColumnInfoData* pResColData, int64_t offset, int64_t rows,
                                                  int16_t funcId, SStreamRuntimeFuncInfo* pInfo,
                                                  const SArray* pSourceWinIdx, SNode* pParamNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  int32_t originIdx = pInfo->curIdx;
  int32_t idxNum = taosArrayGetSize(pSourceWinIdx);
  SArray* pInputWinIdx = taosArrayInit(idxNum, sizeof(int64_t));
  TSDB_CHECK_NULL(pInputWinIdx, code, lino, _exit, terrno);
  TSDB_CHECK_NULL(taosArrayAddBatch(pInputWinIdx, TARRAY_DATA(pSourceWinIdx), idxNum), code, lino, _exit, terrno);

  for (int32_t i = 0; i < idxNum; ++i) {
    int64_t* pCurr = taosArrayGet(pInputWinIdx, i);
    int32_t* pCurrPair = (int32_t*)pCurr;
    int32_t  winIdx = pCurrPair[0];
    int32_t  rowStart = pCurrPair[1];
    int32_t  rowEnd = rows;

    if (i + 1 < idxNum) {
      int64_t* pNext = taosArrayGet(pInputWinIdx, i + 1);
      rowEnd = ((int32_t*)pNext)[1];
    }

    if (rowEnd <= rowStart) {
      continue;
    }

    pInfo->curIdx = winIdx;
    TAOS_CHECK_EXIT(scalarAssignPlaceHolderRes(pResColData, offset + rowStart, rowEnd - rowStart, funcId, pInfo,
                                               pParamNode));
  }

_exit:
  pInfo->curIdx = originIdx;
  taosArrayDestroy(pInputWinIdx);
  return code;
}

void streamOperatorReleaseState(SOperatorInfo* pOperator) {
  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (downstream->fpSet.releaseStreamStateFn) {
    downstream->fpSet.releaseStreamStateFn(downstream);
  }
}

void streamOperatorReloadState(SOperatorInfo* pOperator) {
  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (downstream->fpSet.reloadStreamStateFn) {
    downstream->fpSet.reloadStreamStateFn(downstream);
  }
}

static int32_t resetProjectOperState(SOperatorInfo* pOper) {
  SProjectOperatorInfo* pProject = pOper->info;
  SExecTaskInfo*           pTaskInfo = pOper->pTaskInfo;
  pOper->status = OP_NOT_OPENED;

  resetBasicOperatorState(&pProject->binfo);
  SProjectPhysiNode* pPhynode = (SProjectPhysiNode*)pOper->pPhyNode;

  pProject->limitInfo = (SLimitInfo){0};
  initLimitInfo(pPhynode->node.pLimit, pPhynode->node.pSlimit, &pProject->limitInfo);

  blockDataCleanup(pProject->pFinalRes);
  int32_t code = projectResetProcessByRowStates(pProject);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  taosArrayClear(pProject->pInputWinIdx);
  taosArrayClear(pProject->pOutputWinIdx);
  pProject->pendingEog = false;
  pProject->pendingEogGroupId = 0;
  projectClearZeroRowDrain(pProject);
  pProject->bufferedGroupId = 0;
  if (pProject->pPublishedStream != NULL && (pProject->pPublishedStream->pStreamBlkWinIdx == pProject->pInputWinIdx ||
                                             pProject->pPublishedStream->pStreamBlkWinIdx == pProject->pOutputWinIdx)) {
    pProject->pPublishedStream->pStreamBlkWinIdx = NULL;
  }
  pProject->pPublishedStream = NULL;

  code =
      resetAggSup(&pOper->exprSupp, &pProject->aggSup, pTaskInfo, pPhynode->pProjections, NULL,
                  sizeof(int64_t) * 2 + POINTER_BYTES, pTaskInfo->id.str, NULL, &pTaskInfo->storageAPI.functionStore);
  if (code == 0) {
    code = setFunctionResultOutput(pOper, &pProject->binfo, &pProject->aggSup, MAIN_SCAN, pOper->exprSupp.numOfExprs);
  }
  return code;
}

int32_t createProjectOperatorInfo(SOperatorInfo* downstream, SProjectPhysiNode* pProjPhyNode, SExecTaskInfo* pTaskInfo,
                                  SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t code = TSDB_CODE_SUCCESS;
  SProjectOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SProjectOperatorInfo));
  SOperatorInfo*        pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }

  pOperator->pPhyNode = pProjPhyNode;
  pOperator->exprSupp.hasWindowOrGroup = false;
  pOperator->pTaskInfo = pTaskInfo;
  initOperatorCostInfo(pOperator);

  int32_t    lino = 0;

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pProjPhyNode->node.pOutputDataBlockDesc);
  TSDB_CHECK_NULL(pResBlock, code, lino, _error, terrno);

  initLimitInfo(pProjPhyNode->node.pLimit, pProjPhyNode->node.pSlimit, &pInfo->limitInfo);

  pInfo->binfo.pRes = pResBlock;
  pInfo->pFinalRes = NULL;

  code = createOneDataBlock(pResBlock, false, &pInfo->pFinalRes);
  TSDB_CHECK_CODE(code, lino, _error);

  setOptrBasicInfoOrder(&pInfo->binfo, &pProjPhyNode->node);
  pInfo->inputIgnoreGroup = pProjPhyNode->inputIgnoreGroup;
  pInfo->outputIgnoreGroup = pProjPhyNode->ignoreGroupId;

  if (pTaskInfo->execModel == OPTR_EXEC_MODEL_QUEUE) {
    pInfo->mergeDataBlocks = false;
  } else {
    if (!pProjPhyNode->ignoreGroupId) {
      pInfo->mergeDataBlocks = false;
    } else {
      pInfo->mergeDataBlocks = pProjPhyNode->mergeDataBlock;
    }
  }
  if (isMainFirstScanOperator(downstream)) {
    // scanPhase is a block boundary. Combining phases would relabel earlier
    // rows with the last input block's phase before TimeSlice sees them.
    pInfo->mergeDataBlocks = false;
  }

  int32_t numOfRows = 4096;
  size_t  keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;

  // Make sure the size of SSDataBlock will never exceed the size of 2MB.
  int32_t TWOMB = 2 * 1024 * 1024;
  if (numOfRows * pResBlock->info.rowSize > TWOMB) {
    numOfRows = TWOMB / pResBlock->info.rowSize;
  }

  initResultSizeInfo(&pOperator->resultInfo, numOfRows);
  
  int32_t    numOfCols = 0;
  SExprInfo* pExprInfo = NULL;
  code = createExprInfo(pProjPhyNode->pProjections, NULL, &pExprInfo, &numOfCols);
  TSDB_CHECK_CODE(code, lino, _error);
  code = initAggSup(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, numOfCols, keyBufSize, pTaskInfo->id.str,
                    NULL, &pTaskInfo->storageAPI.functionStore);
  TSDB_CHECK_CODE(code, lino, _error);

  initBasicInfo(&pInfo->binfo, pResBlock);
  code = setFunctionResultOutput(pOperator, &pInfo->binfo, &pInfo->aggSup, MAIN_SCAN, numOfCols);
  TSDB_CHECK_CODE(code, lino, _error);

  code = filterInitFromNode((SNode*)pProjPhyNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                            pTaskInfo->pStreamRuntimeInfo);
  TSDB_CHECK_CODE(code, lino, _error);

  code = setRowTsColumnOutputInfo(pOperator->exprSupp.pCtx, numOfCols, &pInfo->pPseudoColInfo);
  TSDB_CHECK_CODE(code, lino, _error);

  setOperatorInfo(pOperator, "ProjectOperator", QUERY_NODE_PHYSICAL_PLAN_PROJECT, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doProjectOperation, NULL, destroyProjectOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorStreamStateFn(pOperator, streamOperatorReleaseState, streamOperatorReloadState);
  setOperatorResetStateFn(pOperator, resetProjectOperState);

  if (NULL != downstream) {
    code = appendDownstream(pOperator, &downstream, 1);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  if (pInfo != NULL) destroyProjectOperatorInfo(pInfo);
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  return code;
}

static int32_t discardGroupDataBlock(SSDataBlock* pBlock, SLimitInfo* pLimitInfo) {
  if (pLimitInfo->remainGroupOffset > 0) {
    // it is the first group
    if (pLimitInfo->currentGroupId == 0 || pLimitInfo->currentGroupId == pBlock->info.id.groupId) {
      pLimitInfo->currentGroupId = pBlock->info.id.groupId;
      return PROJECT_RETRIEVE_CONTINUE;
    } else if (pLimitInfo->currentGroupId != pBlock->info.id.groupId) {
      // now it is the data from a new group
      pLimitInfo->remainGroupOffset -= 1;
      pLimitInfo->currentGroupId = pBlock->info.id.groupId;

      // ignore data block in current group
      if (pLimitInfo->remainGroupOffset > 0) {
        return PROJECT_RETRIEVE_CONTINUE;
      }

      pLimitInfo->currentGroupId = 0;
    }
  }

  return PROJECT_RETRIEVE_DONE;
}

static int32_t setInfoForNewGroup(SSDataBlock* pBlock, SLimitInfo* pLimitInfo, SOperatorInfo* pOperator) {
  // remainGroupOffset == 0
  // here check for a new group data, we need to handle the data of the previous group.
  if (!(pLimitInfo->remainGroupOffset == 0 || pLimitInfo->remainGroupOffset == -1)) {
    qError("project failed at: %s:%d", __func__, __LINE__);
    return TSDB_CODE_INVALID_PARA;
  }

  bool newGroup = false;
  if (0 == pBlock->info.id.groupId) {
    pLimitInfo->numOfOutputGroups = 1;
  } else if (pLimitInfo->currentGroupId != pBlock->info.id.groupId) {
    pLimitInfo->currentGroupId = pBlock->info.id.groupId;
    pLimitInfo->numOfOutputGroups += 1;
    newGroup = true;
  } else {
    return PROJECT_RETRIEVE_CONTINUE;
  }

  if ((pLimitInfo->slimit.limit >= 0) && (pLimitInfo->slimit.limit < pLimitInfo->numOfOutputGroups)) {
    setOperatorCompleted(pOperator);
    return PROJECT_RETRIEVE_DONE;
  }

  // reset the value for a new group data
  // existing rows that belongs to previous group.
  if (newGroup) {
    resetLimitInfoForNextGroup(pLimitInfo);
  }

  return PROJECT_RETRIEVE_CONTINUE;
}

// todo refactor
static int32_t doIngroupLimitOffset(SLimitInfo* pLimitInfo, uint64_t groupId, SSDataBlock* pBlock,
                                    SOperatorInfo* pOperator) {
  // set current group id
  pLimitInfo->currentGroupId = groupId;
  bool limitReached = applyLimitOffset(pLimitInfo, pBlock, pOperator->pTaskInfo);
  if (pBlock->info.rows == 0 && 0 != pLimitInfo->limit.limit) {
    return PROJECT_RETRIEVE_CONTINUE;
  } else {
    if (limitReached && (pLimitInfo->slimit.limit >= 0 && pLimitInfo->slimit.limit <= pLimitInfo->numOfOutputGroups)) {
      setOperatorCompleted(pOperator);
    } else if (limitReached && groupId == 0) {
      setOperatorCompleted(pOperator);
    }
  }

  return PROJECT_RETRIEVE_DONE;
}

int32_t doProjectOperation(SOperatorInfo* pOperator, SSDataBlock** pResBlock) {
  QRY_PARAM_CHECK(pResBlock);

  SProjectOperatorInfo* pProjectInfo = pOperator->info;
  SOptrBasicInfo*       pInfo = &pProjectInfo->binfo;
  SExprSupp*            pSup = &pOperator->exprSupp;
  SSDataBlock*          pRes = pInfo->pRes;
  SSDataBlock*          pFinalRes = pProjectInfo->pFinalRes;
  int32_t               code = 0;
  int32_t               lino = 0;
  int32_t               order = pInfo->inputTsOrder;
  int32_t               scanFlag = 0;
  SArray*               pCurrentWinIdx = NULL;
  SColumnInfoData*      pFilterRes = NULL;

  *pResBlock = NULL;
  detachOperatorRoutingParam(pOperator);
  if (pOperator->numOfDownstream > 0 &&
      pOperator->pDownstreamGetParams != NULL &&
      pOperator->pDownstreamGetParams[0] != NULL) {
    pOperator->pDownstreamGetParams[0]->reUse = true;
  }
  blockDataCleanup(pFinalRes);
  SExecTaskInfo*          pTaskInfo = pOperator->pTaskInfo;
  SStreamRuntimeFuncInfo* pStream = GET_STM_RTINFO(pTaskInfo);
  bool withExternalWindow = pStream != NULL && pStream->withExternalWindow;
  bool multiGroupProject = projectUsesStrictMultiGroupProtocol(pOperator);
  bool groupCompleteProject = multiGroupProject && hasLagLeadFunc(pSup);
  bool mergeProjectOutput = pProjectInfo->mergeDataBlocks && !groupCompleteProject;
  bool finalizeBufferedGroup = false;

  code = projectValidateMultiGroupProtocolMode(pOperator);
  QUERY_CHECK_CODE(code, lino, _end);
  if (multiGroupProject) {
    taosArrayClear(pProjectInfo->pInputWinIdx);
    taosArrayClear(pProjectInfo->pOutputWinIdx);
  }

  if (multiGroupProject && pProjectInfo->pendingEog) {
    code = projectPrepareEog(pProjectInfo, pStream, pRes, pProjectInfo->pendingEogGroupId);
    QUERY_CHECK_CODE(code, lino, _end);
    pProjectInfo->pendingEog = false;
    pProjectInfo->pendingEogGroupId = 0;
    projectClearZeroRowDrain(pProjectInfo);
    pProjectInfo->bufferedGroupId = 0;
    *pResBlock = pRes;
    goto _end;
  }

  if (pOperator->status == OP_EXEC_DONE && !(multiGroupProject && pProjectInfo->drainingZeroRows)) {
    goto _end;
  }

  SOperatorInfo* downstream = pOperator->numOfDownstream > 0 ? pOperator->pDownstream[0] : NULL;
  SLimitInfo*    pLimitInfo = &pProjectInfo->limitInfo;

  if (downstream == NULL) {
    code = doGenerateSourceData(pOperator);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pProjectInfo->outputIgnoreGroup) {
      pRes->info.id.groupId = 0;
    }

    *pResBlock = (pRes->info.rows > 0)? pRes:NULL;
    goto _end;
  }

  while (1) {
    while (1) {
      if (!groupCompleteProject || pProjectInfo->bufferedGroupId == 0) {
        blockDataCleanup(pRes);
      }

      // The downstream exec may change the value of the newgroup, so use a local variable instead.
      SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
      if (pBlock == NULL) {
        if (multiGroupProject && (pProjectInfo->drainingZeroRows || pProjectInfo->bufferedGroupId != 0)) {
          code = TSDB_CODE_STREAM_INTERNAL_ERROR;
          QUERY_CHECK_CODE(code, lino, _end);
        }
        qDebug("set op close, exec %d, status %d rows %" PRId64 , pTaskInfo->execModel, pOperator->status, pFinalRes->info.rows);
        setOperatorCompleted(pOperator);
        break;
      }

      if (multiGroupProject) {
        code = projectValidateZeroRowDrainInput(pProjectInfo, pBlock);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      if (groupCompleteProject) {
        code = projectValidateGroupBufferInput(pProjectInfo, pBlock);
        QUERY_CHECK_CODE(code, lino, _end);
      }

      if (multiGroupProject && qIsMultiGroupEogBlock(pBlock)) {
        if (groupCompleteProject && pProjectInfo->bufferedGroupId != 0) {
          pProjectInfo->pendingEog = true;
          pProjectInfo->pendingEogGroupId = pBlock->info.id.baseGId;
          pProjectInfo->bufferedGroupId = 0;
          finalizeBufferedGroup = true;
          break;
        }
        if (mergeProjectOutput && pFinalRes->info.rows > 0) {
          if (pFinalRes->info.id.baseGId != pBlock->info.id.baseGId ||
              pFinalRes->info.id.groupId != pBlock->info.id.baseGId) {
            code = TSDB_CODE_STREAM_INTERNAL_ERROR;
            QUERY_CHECK_CODE(code, lino, _end);
          }
          pProjectInfo->pendingEog = true;
          pProjectInfo->pendingEogGroupId = pBlock->info.id.baseGId;
          break;
        }
        code = projectPrepareEog(pProjectInfo, pStream, pRes, pBlock->info.id.baseGId);
        QUERY_CHECK_CODE(code, lino, _end);
        projectClearZeroRowDrain(pProjectInfo);
        *pResBlock = pRes;
        goto _end;
      }

      // Every strict DATA starts a group protocol, even if it is discarded or later filtered to zero rows.
      if (multiGroupProject) {
        if (pBlock->info.id.baseGId == 0 || pBlock->info.id.groupId != pBlock->info.id.baseGId) {
          code = TSDB_CODE_STREAM_INTERNAL_ERROR;
          QUERY_CHECK_CODE(code, lino, _end);
        }
        code = projectStartZeroRowDrain(pProjectInfo, pBlock->info.id.baseGId);
        QUERY_CHECK_CODE(code, lino, _end);
      }

      if (!multiGroupProject && pStream != NULL && pStream->isMultiGroupCalc && withExternalWindow &&
          pStream->curGrpCalc != NULL && pBlock->info.rows == 0) {
        *pResBlock = pBlock;
        goto _end;
      }
//      if (pTaskInfo->execModel == OPTR_EXEC_MODEL_QUEUE) {
//        qDebug("set status recv");
//        pOperator->status = OP_EXEC_RECV;
//      }

      if (pProjectInfo->inputIgnoreGroup && !multiGroupProject) {
        pBlock->info.id.groupId = 0;
      }

      int32_t status = discardGroupDataBlock(pBlock, pLimitInfo);
      if (status == PROJECT_RETRIEVE_CONTINUE) {
        continue;
      }

      (void) setInfoForNewGroup(pBlock, pLimitInfo, pOperator);
      if (pOperator->status == OP_EXEC_DONE) {
        if (multiGroupProject && pProjectInfo->drainingZeroRows) {
          continue;
        }
        break;
      }

      if (mergeProjectOutput) {
        pFinalRes->info.scanFlag = scanFlag = pBlock->info.scanFlag;
        pFinalRes->info.scanPhase = pBlock->info.scanPhase;
      } else {
        pRes->info.scanFlag = scanFlag = pBlock->info.scanFlag;
        pRes->info.scanPhase = pBlock->info.scanPhase;
      }

      SArray* pProjectedWinIdx = NULL;
      if (multiGroupProject && pBlock->info.rows > 0) {
        code = projectSnapshotInputLineage(pProjectInfo, pStream->pStreamBlkWinIdx, pBlock->info.rows);
        QUERY_CHECK_CODE(code, lino, _end);
        if (groupCompleteProject) {
          code = projectStartGroupBuffer(pProjectInfo, pBlock);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        if (mergeProjectOutput || groupCompleteProject) {
          code = projectCopyLineage(&pCurrentWinIdx, pProjectInfo->pInputWinIdx, pBlock->info.rows);
          QUERY_CHECK_CODE(code, lino, _end);
          pProjectedWinIdx = pCurrentWinIdx;
        } else {
          code = projectCopyLineage(&pProjectInfo->pOutputWinIdx, pProjectInfo->pInputWinIdx, pBlock->info.rows);
          QUERY_CHECK_CODE(code, lino, _end);
          pProjectedWinIdx = pProjectInfo->pOutputWinIdx;
        }
      }

      code = setInputDataBlock(pSup, pBlock, order, scanFlag, false);
      QUERY_CHECK_CODE(code, lino, _end);

      code = blockDataEnsureCapacity(pInfo->pRes, pInfo->pRes->info.rows + pBlock->info.rows);
      QUERY_CHECK_CODE(code, lino, _end);

      int32_t outputOffset = pInfo->pRes->info.rows;
      if (multiGroupProject && pBlock->info.rows > 0) {
        code = projectApplyFunctionsWithLineage(pSup->pExprInfo, pInfo->pRes, pBlock, pSup->pCtx, pSup->numOfExprs,
                                                pProjectInfo->pPseudoColInfo, GET_STM_RTINFO(pOperator->pTaskInfo),
                                                false, true, pOperator->pTaskInfo, pProjectInfo->pInputWinIdx,
                                                pProjectedWinIdx, pProjectInfo);
      } else {
        code = projectApplyFunctions(pSup->pExprInfo, pInfo->pRes, pBlock, pSup->pCtx, pSup->numOfExprs,
                                     pProjectInfo->pPseudoColInfo, GET_STM_RTINFO(pOperator->pTaskInfo),
                                     pOperator->pTaskInfo);
      }
      QUERY_CHECK_CODE(code, lino, _end);

      if (groupCompleteProject) {
        int32_t appendedRows = pInfo->pRes->info.rows - outputOffset;
        if (appendedRows < 0) {
          code = TSDB_CODE_STREAM_INTERNAL_ERROR;
          QUERY_CHECK_CODE(code, lino, _end);
        }
        code = projectAppendMergedLineage(pProjectInfo, pCurrentWinIdx, appendedRows, outputOffset);
        QUERY_CHECK_CODE(code, lino, _end);
        continue;
      }

      int32_t rowsBeforeLimit = pInfo->pRes->info.rows;
      int64_t offsetBeforeLimit = pLimitInfo->remainOffset;
      status = doIngroupLimitOffset(pLimitInfo, pBlock->info.id.groupId, pInfo->pRes, pOperator);
      if (multiGroupProject && rowsBeforeLimit > 0) {
        int32_t startRow = offsetBeforeLimit > 0 ? (int32_t)TMIN(offsetBeforeLimit, rowsBeforeLimit) : 0;
        code = projectValidateLineage(pProjectedWinIdx, rowsBeforeLimit);
        QUERY_CHECK_CODE(code, lino, _end);
        if (startRow > 0 || pInfo->pRes->info.rows != rowsBeforeLimit) {
          code = mergeProjectOutput ? projectSliceLineageArray(pProjectedWinIdx, startRow, pInfo->pRes->info.rows)
                                    : projectSliceLineage(pProjectInfo, startRow, pInfo->pRes->info.rows);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
      if (multiGroupProject && pInfo->pRes->info.rows == 0) {
        code = projectStartZeroRowDrain(pProjectInfo, pBlock->info.id.baseGId);
        QUERY_CHECK_CODE(code, lino, _end);
        continue;
      }
      if (status == PROJECT_RETRIEVE_CONTINUE) {
        continue;
      }

      break;
    }

    if (mergeProjectOutput) {
      if (pRes->info.rows > 0) {
        int32_t outputOffset = pFinalRes->info.rows;
        if (multiGroupProject) {
          if (pRes->info.id.baseGId == 0 || pRes->info.id.groupId != pRes->info.id.baseGId ||
              (outputOffset > 0 && pFinalRes->info.id.baseGId != pRes->info.id.baseGId)) {
            code = TSDB_CODE_STREAM_INTERNAL_ERROR;
            QUERY_CHECK_CODE(code, lino, _end);
          }
          if (outputOffset == 0) {
            pFinalRes->info.id = pRes->info.id;
          }
        } else {
          pFinalRes->info.id.groupId = 0;  // clear groupId
        }
        pFinalRes->info.version = pRes->info.version;
        // keep baseGId from current upstream block; already set above for this merge round

        // continue merge data, ignore the group id
        code = blockDataMerge(pFinalRes, pRes);
        QUERY_CHECK_CODE(code, lino, _end);
        if (multiGroupProject) {
          code = projectAppendMergedLineage(pProjectInfo, pCurrentWinIdx, pRes->info.rows, outputOffset);
          QUERY_CHECK_CODE(code, lino, _end);
        }

        // Bound the accumulation by bytes as well: the row-count threshold alone is no bound
        // at all for large values (a 16MB BLOB fills the per-block byte budget in 16 rows and
        // overflows the int32 range of the per-row offsets in 128).
        //
        // This runs AFTER the merge above, so pFinalRes already contains pRes and adding
        // pRes's length again deliberately over-counts.  What it actually asks is "would one
        // more block of this size still fit?", which stops the accumulation slightly earlier
        // than strictly necessary.  That is intentional and harmless: it only moves the block
        // boundaries, the returned rows are unaffected.  It also has to be this way, because
        // pRes comes from getNextBlockFromDownstream() and is already consumed here - it could
        // not be left for the next call without a pending-block mechanism.
        if (!withExternalWindow && pFinalRes->info.rows + pRes->info.rows <= pOperator->resultInfo.threshold &&
            (pOperator->status != OP_EXEC_DONE) && !blockDataMergeExceedLimit(pFinalRes, pRes)) {
          continue;
        }
      }

      // do apply filter
      if (pOperator->exprSupp.pFilterInfo != NULL) {
        filterSetExecContext(pOperator->exprSupp.pFilterInfo, pOperator->pTaskInfo, isTaskKilled);
      }
      int32_t rowsBeforeFilter = pFinalRes->info.rows;
      code = doFilterWithTaskCorrelation(pFinalRes, pOperator->exprSupp.pFilterInfo, NULL,
                                         multiGroupProject ? &pFilterRes : NULL, pOperator->pTaskInfo);
      QUERY_CHECK_CODE(code, lino, _end);
      if (multiGroupProject && pFinalRes->info.rows < rowsBeforeFilter) {
        if (pFilterRes == NULL) {
          code = TSDB_CODE_STREAM_INTERNAL_ERROR;
          QUERY_CHECK_CODE(code, lino, _end);
        }
        code = projectRebuildLineageByFilter(pProjectInfo, rowsBeforeFilter, pFilterRes);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      colDataDestroy(pFilterRes);
      taosMemoryFreeClear(pFilterRes);

      if (multiGroupProject && pProjectInfo->pendingEog && pFinalRes->info.rows == 0) {
        code = projectStartZeroRowDrain(pProjectInfo, pProjectInfo->pendingEogGroupId);
        QUERY_CHECK_CODE(code, lino, _end);
        code = projectPrepareEog(pProjectInfo, pStream, pRes, pProjectInfo->pendingEogGroupId);
        QUERY_CHECK_CODE(code, lino, _end);
        pProjectInfo->pendingEog = false;
        pProjectInfo->pendingEogGroupId = 0;
        projectClearZeroRowDrain(pProjectInfo);
        *pResBlock = pRes;
        goto _end;
      }

      // when apply the limit/offset for each group, pRes->info.rows may be 0, due to limit constraint.
      if (pFinalRes->info.rows > 0 || (pOperator->status == OP_EXEC_DONE)) {
        qDebug("project return %" PRId64 " rows, status %d", pFinalRes->info.rows, pOperator->status);
        break;
      }
    } else {
      if (groupCompleteProject && finalizeBufferedGroup) {
        int32_t rowsBeforeLimit = pRes->info.rows;
        int64_t offsetBeforeLimit = pLimitInfo->remainOffset;
        (void)doIngroupLimitOffset(pLimitInfo, pProjectInfo->pendingEogGroupId, pRes, pOperator);
        if (rowsBeforeLimit > 0) {
          int32_t startRow = offsetBeforeLimit > 0 ? (int32_t)TMIN(offsetBeforeLimit, rowsBeforeLimit) : 0;
          code = projectValidateLineage(pProjectInfo->pOutputWinIdx, rowsBeforeLimit);
          QUERY_CHECK_CODE(code, lino, _end);
          if (startRow > 0 || pRes->info.rows != rowsBeforeLimit) {
            code = projectSliceLineage(pProjectInfo, startRow, pRes->info.rows);
            QUERY_CHECK_CODE(code, lino, _end);
          }
        }
        if (pRes->info.rows == 0 && pProjectInfo->pendingEog) {
          taosArrayClear(pProjectInfo->pOutputWinIdx);
          code = projectPrepareEog(pProjectInfo, pStream, pRes, pProjectInfo->pendingEogGroupId);
          QUERY_CHECK_CODE(code, lino, _end);
          pProjectInfo->pendingEog = false;
          pProjectInfo->pendingEogGroupId = 0;
          projectClearZeroRowDrain(pProjectInfo);
          *pResBlock = pRes;
          goto _end;
        }
      }

      // do apply filter
      if (pRes->info.rows > 0) {
        int32_t rowsBeforeFilter = pRes->info.rows;
        code = doFilterWithTaskCorrelation(pRes, pOperator->exprSupp.pFilterInfo, NULL,
                                           multiGroupProject ? &pFilterRes : NULL, pOperator->pTaskInfo);
        QUERY_CHECK_CODE(code, lino, _end);
        if (multiGroupProject && pRes->info.rows < rowsBeforeFilter) {
          if (pFilterRes == NULL) {
            code = TSDB_CODE_STREAM_INTERNAL_ERROR;
            QUERY_CHECK_CODE(code, lino, _end);
          }
          code = projectRebuildLineageByFilter(pProjectInfo, rowsBeforeFilter, pFilterRes);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        colDataDestroy(pFilterRes);
        taosMemoryFreeClear(pFilterRes);

        if (pRes->info.rows == 0) {
          if (multiGroupProject) {
            taosArrayClear(pProjectInfo->pOutputWinIdx);
            if (groupCompleteProject && pProjectInfo->pendingEog) {
              code = projectPrepareEog(pProjectInfo, pStream, pRes, pProjectInfo->pendingEogGroupId);
              QUERY_CHECK_CODE(code, lino, _end);
              pProjectInfo->pendingEog = false;
              pProjectInfo->pendingEogGroupId = 0;
              projectClearZeroRowDrain(pProjectInfo);
              *pResBlock = pRes;
              goto _end;
            }
            code = projectStartZeroRowDrain(pProjectInfo, pRes->info.id.baseGId);
            QUERY_CHECK_CODE(code, lino, _end);
          }
          continue;
        }
      }

      // no results generated
      break;
    }
  }

  SSDataBlock* p = mergeProjectOutput ? pFinalRes : pRes;
  p->info.dataLoad = 1;

  if (pProjectInfo->outputIgnoreGroup && !multiGroupProject) {
    p->info.id.groupId = 0;
  }

  if (multiGroupProject && p->info.rows > 0) {
    if (p->info.id.baseGId == 0 || p->info.id.groupId != p->info.id.baseGId) {
      code = TSDB_CODE_STREAM_INTERNAL_ERROR;
      QUERY_CHECK_CODE(code, lino, _end);
    }
    code = projectPublishLineage(pProjectInfo, pStream, p->info.rows);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (pTaskInfo->execModel == OPTR_EXEC_MODEL_STREAM) {
    printDataBlock(p, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo), pTaskInfo->id.queryId);
  }

  *pResBlock = (p->info.rows > 0)? p:NULL;

_end:
  colDataDestroy(pFilterRes);
  taosMemoryFree(pFilterRes);
  taosArrayDestroy(pCurrentWinIdx);
  if (code != TSDB_CODE_SUCCESS) {
    pProjectInfo->bufferedGroupId = 0;
    pProjectInfo->pendingEog = false;
    pProjectInfo->pendingEogGroupId = 0;
    projectClearZeroRowDrain(pProjectInfo);
    blockDataCleanup(pRes);
    taosArrayClear(pProjectInfo->pInputWinIdx);
    taosArrayClear(pProjectInfo->pOutputWinIdx);
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return code;
}

static int32_t resetIndefinitOutputOperState(SOperatorInfo* pOper) {
  SIndefOperatorInfo* pInfo = pOper->info;
  SExecTaskInfo*           pTaskInfo = pOper->pTaskInfo;
  SIndefRowsFuncPhysiNode* pPhynode = (SIndefRowsFuncPhysiNode*)pOper->pPhyNode;
  pOper->status = OP_NOT_OPENED;

  resetBasicOperatorState(&pInfo->binfo);

  pInfo->groupId = 0;
  pInfo->pNextGroupRes = NULL;
  int32_t code = resetAggSup(&pOper->exprSupp, &pInfo->aggSup, pTaskInfo, pPhynode->pFuncs, NULL,
    sizeof(int64_t) * 2 + POINTER_BYTES, pTaskInfo->id.str, NULL,
    &pTaskInfo->storageAPI.functionStore);
  if (code == 0){
    code = setFunctionResultOutput(pOper, &pInfo->binfo, &pInfo->aggSup, MAIN_SCAN, pOper->exprSupp.numOfExprs);
  }

  if (code == 0) {
    code = resetExprSupp(&pInfo->scalarSup, pTaskInfo, pPhynode->pExprs, NULL,
                         &pTaskInfo->storageAPI.functionStore);
  }
  return 0;
}

int32_t createIndefinitOutputOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pNode,
                                                 SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);
  int32_t code = 0;
  int32_t lino = 0;
  int32_t numOfRows = 4096;
  size_t  keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;

  SIndefOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SIndefOperatorInfo));
  SOperatorInfo*      pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }

  pOperator->pPhyNode = pNode;
  pOperator->pTaskInfo = pTaskInfo;
  initOperatorCostInfo(pOperator);

  SExprSupp* pSup = &pOperator->exprSupp;
  pSup->hasWindowOrGroup = false;

  SIndefRowsFuncPhysiNode* pPhyNode = (SIndefRowsFuncPhysiNode*)pNode;

  if (pPhyNode->pExprs != NULL) {
    int32_t    num = 0;
    SExprInfo* pSExpr = NULL;
    code = createExprInfo(pPhyNode->pExprs, NULL, &pSExpr, &num);
    QUERY_CHECK_CODE(code, lino, _error);

    code = initExprSupp(&pInfo->scalarSup, pSExpr, num, &pTaskInfo->storageAPI.functionStore);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pPhyNode->node.pOutputDataBlockDesc);
  TSDB_CHECK_NULL(pResBlock, code, lino, _error, terrno);

  // Make sure the size of SSDataBlock will never exceed the size of 2MB.
  int32_t TWOMB = 2 * 1024 * 1024;
  if (numOfRows * pResBlock->info.rowSize > TWOMB) {
    numOfRows = TWOMB / pResBlock->info.rowSize;
  }

  initBasicInfo(&pInfo->binfo, pResBlock);
  initResultSizeInfo(&pOperator->resultInfo, numOfRows);
  code = blockDataEnsureCapacity(pResBlock, numOfRows);
  TSDB_CHECK_CODE(code, lino, _error);

  int32_t    numOfExpr = 0;
  SExprInfo* pExprInfo = NULL;
  code = createExprInfo(pPhyNode->pFuncs, NULL, &pExprInfo, &numOfExpr);
  TSDB_CHECK_CODE(code, lino, _error);

  code = initAggSup(pSup, &pInfo->aggSup, pExprInfo, numOfExpr, keyBufSize, pTaskInfo->id.str,
                            NULL, &pTaskInfo->storageAPI.functionStore);
  TSDB_CHECK_CODE(code, lino, _error);

  code = setFunctionResultOutput(pOperator, &pInfo->binfo, &pInfo->aggSup, MAIN_SCAN, numOfExpr);
  TSDB_CHECK_CODE(code, lino, _error);

  code = filterInitFromNode((SNode*)pPhyNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                            pTaskInfo->pStreamRuntimeInfo);
  TSDB_CHECK_CODE(code, lino, _error);

  pInfo->binfo.pRes = pResBlock;
  setOptrBasicInfoOrder(&pInfo->binfo, pNode);
  code = setRowTsColumnOutputInfo(pSup->pCtx, numOfExpr, &pInfo->pPseudoColInfo);
  TSDB_CHECK_CODE(code, lino, _error);

  setOperatorInfo(pOperator, "IndefinitOperator", QUERY_NODE_PHYSICAL_PLAN_INDEF_ROWS_FUNC, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doApplyIndefinitFunction, NULL, destroyIndefinitOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
                                         
  setOperatorResetStateFn(pOperator, resetIndefinitOutputOperState);
  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  if (pInfo != NULL) destroyIndefinitOperatorInfo(pInfo);
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  return code;
}

static void doHandleDataBlock(SOperatorInfo* pOperator, SSDataBlock* pBlock, SOperatorInfo* downstream,
                              SExecTaskInfo* pTaskInfo) {
  SIndefOperatorInfo* pIndefInfo = pOperator->info;
  SOptrBasicInfo*     pInfo = &pIndefInfo->binfo;
  SExprSupp*          pSup = &pOperator->exprSupp;

  int32_t order = pInfo->inputTsOrder;
  int32_t scanFlag = pBlock->info.scanFlag;
  int32_t code = TSDB_CODE_SUCCESS;

  // there is an scalar expression that needs to be calculated before apply the group aggregation.
  SExprSupp* pScalarSup = &pIndefInfo->scalarSup;
  if (pScalarSup->pExprInfo != NULL) {
    code = projectApplyFunctions(pScalarSup->pExprInfo, pBlock, pBlock, pScalarSup->pCtx, pScalarSup->numOfExprs,
                                 pIndefInfo->pPseudoColInfo, GET_STM_RTINFO(pOperator->pTaskInfo),
                                 pOperator->pTaskInfo);
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, code);
    }
  }

  code = setInputDataBlock(pSup, pBlock, order, scanFlag, false);
  if (code) {
    T_LONG_JMP(pTaskInfo->env, code);
  }

  code = blockDataEnsureCapacity(pInfo->pRes, pInfo->pRes->info.rows + pBlock->info.rows);
  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, code);
  }

  code = projectApplyFunctions(pSup->pExprInfo, pInfo->pRes, pBlock, pSup->pCtx, pSup->numOfExprs,
                               pIndefInfo->pPseudoColInfo, GET_STM_RTINFO(pOperator->pTaskInfo),
                               pOperator->pTaskInfo);
  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, code);
  }
}

int32_t doApplyIndefinitFunction(SOperatorInfo* pOperator, SSDataBlock** pResBlock) {
  QRY_PARAM_CHECK(pResBlock);
  SIndefOperatorInfo* pIndefInfo = pOperator->info;
  SOptrBasicInfo*     pInfo = &pIndefInfo->binfo;
  SExprSupp*          pSup = &pOperator->exprSupp;
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SSDataBlock*        pRes = pInfo->pRes;

  blockDataCleanup(pRes);

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  if (pOperator->status == OP_EXEC_DONE) {
    return code;
  }

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  bool           noSplitOutput = hasLagLeadFunc(pSup);

  while (1) {
    // here we need to handle the existsed group results
    if (pIndefInfo->pNextGroupRes != NULL) {  // todo extract method
      for (int32_t k = 0; k < pSup->numOfExprs; ++k) {
        SqlFunctionCtx* pCtx = &pSup->pCtx[k];

        SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
        if (pResInfo->initialized && pCtx->fpSet.cleanup != NULL) {
          pCtx->fpSet.cleanup(pCtx);
        }
        pResInfo->initialized = false;
        pCtx->pOutput = NULL;
      }

      doHandleDataBlock(pOperator, pIndefInfo->pNextGroupRes, downstream, pTaskInfo);
      pIndefInfo->pNextGroupRes = NULL;
    }

    if (noSplitOutput || pInfo->pRes->info.rows < pOperator->resultInfo.threshold) {
      while (1) {
        // The downstream exec may change the value of the newgroup, so use a local variable instead.
        SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
        if (pBlock == NULL) {
          setOperatorCompleted(pOperator);
          break;
        }
        pInfo->pRes->info.scanFlag = pBlock->info.scanFlag;

        if (pIndefInfo->groupId == 0 && pBlock->info.id.groupId != 0) {
          pIndefInfo->groupId = pBlock->info.id.groupId;  // this is the initial group result
        } else {
          if (pIndefInfo->groupId != pBlock->info.id.groupId) {  // reset output buffer and computing status
            pIndefInfo->groupId = pBlock->info.id.groupId;
            pIndefInfo->pNextGroupRes = pBlock;
            break;
          }
        }

        doHandleDataBlock(pOperator, pBlock, downstream, pTaskInfo);
        // External-window outputs carry per-window row ranges in stream runtime state.
        // Return as soon as this operator has a result block so the downstream state
        // still matches the block we are about to hand back to the runner.
        if (pTaskInfo->pStreamRuntimeInfo != NULL && pTaskInfo->pStreamRuntimeInfo->funcInfo.withExternalWindow &&
            pInfo->pRes->info.rows > 0) {
          break;
        }
        if (!noSplitOutput && pInfo->pRes->info.rows >= pOperator->resultInfo.threshold) {
          break;
        }
      }
    }

    code = doFilterWithTaskCorrelation(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL,
                                       pOperator->pTaskInfo);
    QUERY_CHECK_CODE(code, lino, _end);

    size_t rows = pInfo->pRes->info.rows;
    if (rows > 0 || pOperator->status == OP_EXEC_DONE) {
      break;
    } else {
      blockDataCleanup(pInfo->pRes);
    }
  }

  *pResBlock = (pInfo->pRes->info.rows> 0) ? pInfo->pRes : NULL;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return code;
}

int32_t initCtxOutputBuffer(SqlFunctionCtx* pCtx, int32_t size) {
  int32_t code = TSDB_CODE_SUCCESS;
  for (int32_t j = 0; j < size; ++j) {
    struct SResultRowEntryInfo* pResInfo = GET_RES_INFO(&pCtx[j]);
    if (isRowEntryInitialized(pResInfo) || fmIsPseudoColumnFunc(pCtx[j].functionId) || pCtx[j].functionId == -1 ||
        fmIsScalarFunc(pCtx[j].functionId)) {
      continue;
    }

    code = pCtx[j].fpSet.init(&pCtx[j], pCtx[j].resultInfo);
    if (code) {
      return code;
    }
  }

  return 0;
}

/*
 * The start of each column SResultRowEntryInfo is denote by RowCellInfoOffset.
 * Note that in case of top/bottom query, the whole multiple rows of result is treated as only one row of results.
 * +------------+-----------------result column 1------------+------------------result column 2-----------+
 * | SResultRow | SResultRowEntryInfo | intermediate buffer1 | SResultRowEntryInfo | intermediate buffer 2|
 * +------------+--------------------------------------------+--------------------------------------------+
 *           offset[0]                                  offset[1]                                   offset[2]
 */
// TODO refactor: some function move away
int32_t setFunctionResultOutput(struct SOperatorInfo* pOperator, SOptrBasicInfo* pInfo, SAggSupporter* pSup, int32_t stage,
                             int32_t numOfExprs) {
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SqlFunctionCtx* pCtx = pOperator->exprSupp.pCtx;
  int32_t*        rowEntryInfoOffset = pOperator->exprSupp.rowEntryInfoOffset;

  SResultRowInfo* pResultRowInfo = &pInfo->resultRowInfo;
  initResultRowInfo(pResultRowInfo);

  int64_t     tid = 0;
  int64_t     groupId = 0;
  SResultRow* pRow = doSetResultOutBufByKey(pSup->pResultBuf, pResultRowInfo, (char*)&tid, sizeof(tid), true, groupId,
                                            pTaskInfo, false, pSup, true);
  if (pRow == NULL || pTaskInfo->code != 0) {
    return pTaskInfo->code;
  }

  for (int32_t i = 0; i < numOfExprs; ++i) {
    struct SResultRowEntryInfo* pEntry = getResultEntryInfo(pRow, i, rowEntryInfoOffset);
    cleanupResultRowEntry(pEntry);

    pCtx[i].resultInfo = pEntry;
    pCtx[i].scanFlag = stage;
  }

  return initCtxOutputBuffer(pCtx, numOfExprs);
}

int32_t setRowTsColumnOutputInfo(SqlFunctionCtx* pCtx, int32_t numOfCols, SArray** pResList) {
  QRY_PARAM_CHECK(pResList);
  SArray* pList = taosArrayInit(4, sizeof(int32_t));
  if (pList == NULL) {
    return terrno;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    if (fmIsPseudoColumnFunc(pCtx[i].functionId) && !fmIsPlaceHolderFunc(pCtx[i].functionId)) {
      void* px = taosArrayPush(pList, &i);
      if (px == NULL) {
        return terrno;
      }
    }
  }

  *pResList = pList;
  return 0;
}

int32_t doGenerateSourceData(SOperatorInfo* pOperator) {
  SProjectOperatorInfo* pProjectInfo = pOperator->info;

  SExprSupp*   pSup = &pOperator->exprSupp;
  SSDataBlock* pRes = pProjectInfo->binfo.pRes;
  SExprInfo*   pExpr = pSup->pExprInfo;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SCorrelationEvalContext correlation = {0};
  SCorrelationWaitHooks wait = {0};
  const SCorrelationEvalContext* pCorrelation = initTaskCorrelationEvalContext(pTaskInfo, &correlation, &wait);

  int32_t code = blockDataEnsureCapacity(pRes, pOperator->resultInfo.capacity);
  if (code) {
    return code;
  }

  for (int32_t k = 0; k < pSup->numOfExprs; ++k) {
    int32_t outputSlotId = pExpr[k].base.resSchema.slotId;

    if (pExpr[k].pExpr->nodeType == QUERY_NODE_VALUE) {
      SColumnInfoData* pColInfoData = taosArrayGet(pRes->pDataBlock, outputSlotId);
      if (pColInfoData == NULL) {
        return terrno;
      }

      int32_t type = pExpr[k].base.pParam[0].param.nType;
      if (TSDB_DATA_TYPE_NULL == type) {
        colDataSetNNULL(pColInfoData, 0, 1);
      } else {
        code = colDataSetVal(pColInfoData, 0, taosVariantGet(&pExpr[k].base.pParam[0].param, type), false);
        if (code) {
          return code;
        }
      }
    } else if (pExpr[k].pExpr->nodeType == QUERY_NODE_FUNCTION) {
      SqlFunctionCtx* pfCtx = &pSup->pCtx[k];

      // UDF scalar functions will be calculated here, for example, select foo(n) from (select 1 n).
      // UDF aggregate functions will be handled in agg operator.
      if (fmIsScalarFunc(pfCtx->functionId)) {
        SArray* pBlockList = taosArrayInit(4, POINTER_BYTES);
        if (pBlockList == NULL) {
          return terrno;
        }

        SNode* pFunction = (SNode*)pExpr[k].pExpr->_function.pFunctNode;
        bool hasCorrelation = NULL != pCorrelation && nodesExprHasCorrelation(pFunction);
        SSDataBlock virtualInput = {0};
        SSDataBlock* pInput = pRes;
        if (hasCorrelation) {
          virtualInput.info.rows = 1;
          pInput = &virtualInput;
        }
        void* px = taosArrayPush(pBlockList, &pInput);
        if (px == NULL) {
          return terrno;
        }

        SColumnInfoData* pResColData = taosArrayGet(pRes->pDataBlock, outputSlotId);
        if (pResColData == NULL) {
          return terrno;
        }

        SColumnInfoData  idata = {.info = pResColData->info, .hasNull = true};

        SScalarParam dest = {.columnData = &idata};
        gTaskScalarExtra.pStreamInfo  = GET_STM_RTINFO(pOperator->pTaskInfo);
        gTaskScalarExtra.pStreamRange = NULL;
        gTaskScalarExtra.pTaskInfo    = pOperator->pTaskInfo;
        gTaskScalarExtra.isTaskKilled = isTaskKilled;
        SScalarExtraInfo scalarExtra = gTaskScalarExtra;
        scalarExtra.pCorrelation = pCorrelation;
        code = scalarCalculate(pFunction, pBlockList, &dest,
                               hasCorrelation ? &scalarExtra : &gTaskScalarExtra);
        if (code != TSDB_CODE_SUCCESS) {
          taosArrayDestroy(pBlockList);
          return code;
        }

        int32_t startOffset = pRes->info.rows;
        if (pRes->info.capacity <= 0) {
          qError("project failed at: %s:%d", __func__, __LINE__);
          return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        }
        code = colDataAssign(pResColData, &idata, dest.numOfRows, &pRes->info);
        if (code) {
          return code;
        }

        colDataDestroy(&idata);
        taosArrayDestroy(pBlockList);
      } else {
        qError("%s: unsupported scalar expression node type at line %d, since %s", __func__, __LINE__, tstrerror(TSDB_CODE_OPS_NOT_SUPPORT));
        return TSDB_CODE_OPS_NOT_SUPPORT;
      }
    } else if (pExpr[k].pExpr->nodeType == QUERY_NODE_OPERATOR) {
      SScalarExtraInfo scalarExtra = gTaskScalarExtra;
      scalarExtra.pCorrelation = pCorrelation;
      TAOS_CHECK_RETURN(projectApplyOperator(&pExpr[k], pRes, NULL, outputSlotId, NULL, false,
                                             &gTaskScalarExtra, NULL == pCorrelation ? NULL : &scalarExtra));
    } else {
      qError("%s: unsupported expression node type at line %d, since %s", __func__, __LINE__, tstrerror(TSDB_CODE_OPS_NOT_SUPPORT));
      return TSDB_CODE_OPS_NOT_SUPPORT;
    }
  }

  pRes->info.rows = 1;
  code = doFilterWithTaskCorrelation(pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL, pTaskInfo);
  if (code) {
    pTaskInfo->code = code;
    return code;
  }

  (void) doIngroupLimitOffset(&pProjectInfo->limitInfo, 0, pRes, pOperator);

  setOperatorCompleted(pOperator);

  return code;
}

static void setPseudoOutputColInfo(SSDataBlock* pResult, SqlFunctionCtx* pCtx, SArray* pPseudoList) {
  size_t num = (pPseudoList != NULL) ? taosArrayGetSize(pPseudoList) : 0;
  for (int32_t i = 0; i < num; ++i) {
    pCtx[i].pOutput = taosArrayGet(pResult->pDataBlock, i);
    if (pCtx[i].pOutput == NULL) {
      qError("failed to get the output buf, ptr is null");
    }
  }
}

static int32_t projectAssignColumn(SColumnInfoData* pDst, const SColumnInfoData* pSrc, int32_t rows,
                                   const SDataBlockInfo* pBlockInfo) {
  uint8_t precision = pDst->info.precision;
  uint8_t scale = pDst->info.scale;
  if (IS_DECIMAL_TYPE(pDst->info.type) && precision != 0 &&
      (pSrc->info.precision != 0 || pSrc->info.scale != 0) &&
      (precision != pSrc->info.precision || scale != pSrc->info.scale)) {
    return TSDB_CODE_INVALID_MSG;
  }

  int32_t code = colDataAssign(pDst, pSrc, rows, pBlockInfo);
  if (code == TSDB_CODE_SUCCESS && IS_DECIMAL_TYPE(pDst->info.type) && precision != 0) {
    // colDataAssign copies source metadata; Project's declared decimal type owns the output schema.
    pDst->info.precision = precision;
    pDst->info.scale = scale;
  }
  return code;
}

int32_t projectApplyColumn(SSDataBlock* pResult, SSDataBlock* pSrcBlock, int32_t outputSlotId,
                           SqlFunctionCtx* pfCtx, int64_t* numOfRows, bool createNewColModel) {
  int32_t code = 0, lino = 0;
  SInputColumnInfoData* pInputData = &pfCtx->input;
  SColumnInfoData* pColInfoData = taosArrayGet(pResult->pDataBlock, outputSlotId);
  TSDB_CHECK_NULL(pColInfoData, code, lino, _exit, terrno);

  if (pResult->info.rows > 0 && !createNewColModel) {
    if (pInputData->pData[0] == NULL) {
      int32_t slotId = pfCtx->param[0].pCol->slotId;

      SColumnInfoData* pInput = taosArrayGet(pSrcBlock->pDataBlock, slotId);
      TSDB_CHECK_NULL(pInput, code, lino, _exit, terrno);

      TAOS_CHECK_EXIT(colDataMergeCol(pColInfoData, pResult->info.rows, (int32_t*)&pResult->info.capacity, pInput,
                            pSrcBlock->info.rows));
      *numOfRows = pSrcBlock->info.rows;
      return code;
    }
    
    TAOS_CHECK_EXIT(colDataMergeCol(pColInfoData, pResult->info.rows, (int32_t*)&pResult->info.capacity,
                          pInputData->pData[0], pInputData->numOfRows));
    *numOfRows = pInputData->numOfRows;
    return code;
  } 
  
  if (pInputData->pData[0] == NULL) {
    int32_t slotId = pfCtx->param[0].pCol->slotId;

    SColumnInfoData* pInput = taosArrayGet(pSrcBlock->pDataBlock, slotId);
    TSDB_CHECK_NULL(pInput, code, lino, _exit, terrno);

    TAOS_CHECK_EXIT(projectAssignColumn(pColInfoData, pInput, pSrcBlock->info.rows, &pResult->info));
    *numOfRows = pSrcBlock->info.rows;

    return code;
  }
  
  TAOS_CHECK_EXIT(projectAssignColumn(pColInfoData, pInputData->pData[0], pInputData->numOfRows, &pResult->info));
  *numOfRows = pInputData->numOfRows;

_exit:

  if (code) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  
  return code;
}


int32_t projectApplyValue(SExprInfo* pExpr, SSDataBlock* pResult, SSDataBlock* pSrcBlock, int32_t outputSlotId,
                          int64_t* numOfRows, bool createNewColModel) {
  int32_t code = 0, lino = 0;
  SColumnInfoData* pColInfoData = taosArrayGet(pResult->pDataBlock, outputSlotId);
  TSDB_CHECK_NULL(pColInfoData, code, lino, _exit, terrno);

  int32_t offset = createNewColModel ? 0 : pResult->info.rows;
  int32_t type = pExpr->base.pParam[0].param.nType;
  if (TSDB_DATA_TYPE_NULL == type) {
    colDataSetNNULL(pColInfoData, offset, pSrcBlock->info.rows);
  } else {
    char* p = taosVariantGet(&pExpr->base.pParam[0].param, type);
    for (int32_t i = 0; i < pSrcBlock->info.rows; ++i) {
      TAOS_CHECK_EXIT(colDataSetVal(pColInfoData, i + offset, p, false));
    }
  }

  *numOfRows = pSrcBlock->info.rows;

_exit:

  if (code) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  
  return code;
}



int32_t projectApplyOperator(SExprInfo* pExpr, SSDataBlock* pResult, SSDataBlock* pSrcBlock, int32_t outputSlotId,
                             int64_t* numOfRows, bool createNewColModel, const void* pExtraParams,
                             const SScalarExtraInfo* pScalarExtra) {
  int32_t     code = 0, lino = 0;
  SSDataBlock scalarBlock = {0};
  SArray*     pBlockList = taosArrayInit(4, POINTER_BYTES);
  TSDB_CHECK_NULL(pBlockList, code, lino, _exit, terrno);

  if (NULL == pSrcBlock) {
    // Source-less SELECT still emits one row, so scalar expressions must not
    // be evaluated against an empty input range.
    scalarBlock.info.rows = 1;
    pSrcBlock = &scalarBlock;
  }

  void* px = taosArrayPush(pBlockList, &pSrcBlock);
  TSDB_CHECK_NULL(px, code, lino, _exit, terrno);
  
  SColumnInfoData* pResColData = taosArrayGet(pResult->pDataBlock, outputSlotId);
  TSDB_CHECK_NULL(pResColData, code, lino, _exit, terrno);

  SColumnInfoData idata = {.info = pResColData->info, .hasNull = true};
  SScalarParam dest = {.columnData = &idata};
  SScalarExtraInfo extra = pScalarExtra ? *pScalarExtra : gTaskScalarExtra;
  SScalarExtraInfo* pExtra = pScalarExtra ? &extra : &gTaskScalarExtra;
  pExtra->pStreamInfo = (void*)pExtraParams;
  pExtra->pStreamRange = NULL;
  TAOS_CHECK_EXIT(scalarCalculate(pExpr->pExpr->_optrRoot.pRootNode, pBlockList, &dest, pExtra));

  if (pResult->info.rows > 0 && !createNewColModel) {
    code = colDataMergeCol(pResColData, pResult->info.rows, (int32_t*)&pResult->info.capacity, &idata, dest.numOfRows);
  } else {
    code = colDataAssign(pResColData, &idata, dest.numOfRows, &pResult->info);
  }

  colDataDestroy(&idata);
  TAOS_CHECK_EXIT(code);

  if (numOfRows) {
    *numOfRows = dest.numOfRows;
  }
  
_exit:

  if (code < 0) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  taosArrayDestroy(pBlockList);
  
  return code;
}

static int32_t projectApplyFunctionWithLineage(SqlFunctionCtx* pCtx, SqlFunctionCtx* pfCtx, SExprInfo* pExpr,
                                               SSDataBlock* pResult, SSDataBlock* pSrcBlock, int32_t outputSlotId,
                                               int64_t* numOfRows, bool createNewColModel, const void* pExtraParams,
                                               SArray* pPseudoList, SArray** processByRowFunctionCtx, bool doSelectFunc,
                                               const SArray* pInputWinIdx, const SScalarExtraInfo* pScalarExtra) {
  int32_t code = 0, lino = 0;
  SArray* pBlockList = NULL;
  SColumnInfoData* pResColData = taosArrayGet(pResult->pDataBlock, outputSlotId);
  TSDB_CHECK_NULL(pResColData, code, lino, _exit, terrno);

  if (fmIsPlaceHolderFunc(pfCtx->functionId) && pExtraParams && pfCtx->pExpr->base.pParamList && 1 == pfCtx->pExpr->base.pParamList->length) {
    SNode* pParamNode = nodesListGetNode(pfCtx->pExpr->base.pParamList, 0);
    SStreamRuntimeFuncInfo* pStreamInfo = (SStreamRuntimeFuncInfo*)pExtraParams;
    const SArray*           pSourceWinIdx =
        pInputWinIdx != NULL ? pInputWinIdx : (pStreamInfo == NULL ? NULL : pStreamInfo->pStreamBlkWinIdx);
    if (pStreamInfo != NULL && pStreamInfo->withExternalWindow && pSourceWinIdx != NULL &&
        taosArrayGetSize(pSourceWinIdx) > 1) {
      TAOS_CHECK_EXIT(assignPlaceHolderInExternalWindows(pResColData, pResult->info.rows, pSrcBlock->info.rows,
                                                         pfCtx->functionId, pStreamInfo, pSourceWinIdx, pParamNode));
    } else {
      TAOS_CHECK_EXIT(scalarAssignPlaceHolderRes(pResColData, pResult->info.rows, pSrcBlock->info.rows,
                                                 pfCtx->functionId, pExtraParams, pParamNode));
    }
    *numOfRows = pSrcBlock->info.rows;

    return code;
  }

  if (fmIsScalarFunc(pfCtx->functionId) || fmIsPlaceHolderFunc(pfCtx->functionId)) {
    pBlockList = taosArrayInit(4, POINTER_BYTES);
    TSDB_CHECK_NULL(pBlockList, code, lino, _exit, terrno);

    void* px = taosArrayPush(pBlockList, &pSrcBlock);
    TSDB_CHECK_NULL(px, code, lino, _exit, terrno);

    SColumnInfoData idata = {.info = pResColData->info, .hasNull = true};
    SScalarParam dest = {.columnData = &idata};
    SScalarExtraInfo extra = pScalarExtra ? *pScalarExtra : gTaskScalarExtra;
    SScalarExtraInfo* pExtra = pScalarExtra ? &extra : &gTaskScalarExtra;
    pExtra->pStreamInfo = (void*)pExtraParams;
    pExtra->pStreamRange = NULL;
    TAOS_CHECK_EXIT(scalarCalculate((SNode*)pExpr->pExpr->_function.pFunctNode, pBlockList, &dest, pExtra));

    if (pResult->info.rows > 0 && !createNewColModel) {
      code = colDataMergeCol(pResColData, pResult->info.rows, (int32_t*)&pResult->info.capacity, &idata, dest.numOfRows);
    } else {
      SColumnInfo oriInfo = pResColData->info;
      code = colDataAssign(pResColData, &idata, dest.numOfRows, &pResult->info);
      // restore the original column info to satisfy the output column schema
      pResColData->info = oriInfo;
    }

    colDataDestroy(&idata);
    taosArrayDestroy(pBlockList);
    pBlockList = NULL;
    TAOS_CHECK_EXIT(code);

    *numOfRows = dest.numOfRows;

    return code;
  }

  if (fmIsIndefiniteRowsFunc(pfCtx->functionId)) {
    SResultRowEntryInfo* pResInfo = GET_RES_INFO(pfCtx);
    TAOS_CHECK_EXIT(pfCtx->fpSet.init(pfCtx, pResInfo));


    pfCtx->pOutput = (char*)pResColData;
    TSDB_CHECK_NULL(pfCtx->pOutput, code, lino, _exit, terrno);

    pfCtx->offset = createNewColModel ? 0 : pResult->info.rows;  // set the start offset

    // set the timestamp(_rowts) output buffer
    if (taosArrayGetSize(pPseudoList) > 0) {
      int32_t* outputColIndex = taosArrayGet(pPseudoList, 0);
      TSDB_CHECK_NULL(outputColIndex, code, lino, _exit, terrno);

      pfCtx->pTsOutput = (SColumnInfoData*)pCtx[*outputColIndex].pOutput;
    }

    // link pDstBlock to set selectivity value
    if (pfCtx->subsidiaries.num > 0) {
      pfCtx->pDstBlock = pResult;
    }

    code = pfCtx->fpSet.process(pfCtx);
    if (code != TSDB_CODE_SUCCESS) {
      if (pfCtx->fpSet.cleanup != NULL) {
        pfCtx->fpSet.cleanup(pfCtx);
      }
      TAOS_CHECK_EXIT(code);
    }

    *numOfRows = pResInfo->numOfRes;

    if (fmIsProcessByRowFunc(pfCtx->functionId)) {
      if (NULL == *processByRowFunctionCtx) {
        *processByRowFunctionCtx = taosArrayInit(1, sizeof(SqlFunctionCtx*));
        TSDB_CHECK_NULL(*processByRowFunctionCtx, code, lino, _exit, terrno);
      }

      void* px = taosArrayPush(*processByRowFunctionCtx, &pfCtx);
      TSDB_CHECK_NULL(px, code, lino, _exit, terrno);
    }

    return code;
  } 

  if (fmIsAggFunc(pfCtx->functionId)) {
    // selective value output should be set during corresponding function execution
    if (!doSelectFunc && fmIsSelectValueFunc(pfCtx->functionId)) {
      return code;
    }
    
    // _group_key function for "partition by tbname" + csum(col_name) query
    int32_t slotId = pfCtx->param[0].pCol->slotId;

    // todo handle the json tag
    SColumnInfoData* pInput = taosArrayGet(pSrcBlock->pDataBlock, slotId);
    TSDB_CHECK_NULL(pInput, code, lino, _exit, terrno);

    for (int32_t f = 0; f < pSrcBlock->info.rows; ++f) {
      bool isNull = colDataIsNull_s(pInput, f);
      if (isNull) {
        colDataSetNULL(pResColData, pResult->info.rows + f);
      } else {
        char* data = colDataGetData(pInput, f);
        TAOS_CHECK_EXIT(colDataSetVal(pResColData, pResult->info.rows + f, data, isNull));
      }
    }

    *numOfRows = pSrcBlock->info.rows;

    return code;
  } 
  
  if (fmIsGroupIdFunc(pfCtx->functionId)) {
    for (int32_t f = 0; f < pSrcBlock->info.rows; ++f) {
      TAOS_CHECK_EXIT(colDataSetVal(pResColData, pResult->info.rows + f, (const char*)&pSrcBlock->info.id.groupId, false));
    }

    *numOfRows = pSrcBlock->info.rows;
    return code;
  }
  
_exit:

  if (code) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  taosArrayDestroy(pBlockList);
  
  return code;
}

int32_t projectApplyFunction(SqlFunctionCtx* pCtx, SqlFunctionCtx* pfCtx, SExprInfo* pExpr, SSDataBlock* pResult,
                             SSDataBlock* pSrcBlock, int32_t outputSlotId, int64_t* numOfRows, bool createNewColModel,
                             const void* pExtraParams, SArray* pPseudoList, SArray** processByRowFunctionCtx,
                             bool doSelectFunc) {
  return projectApplyFunctionWithLineage(pCtx, pfCtx, pExpr, pResult, pSrcBlock, outputSlotId, numOfRows,
                                         createNewColModel, pExtraParams, pPseudoList, processByRowFunctionCtx,
                                         doSelectFunc, NULL, NULL);
}

static int32_t projectApplyFunctionsWithLineage(SExprInfo* pExpr, SSDataBlock* pResult, SSDataBlock* pSrcBlock,
                                                SqlFunctionCtx* pCtx, int32_t numOfOutput, SArray* pPseudoList,
                                                const void* pExtraParams, bool doSelectFunc, bool hasIndefRowsFunc,
                                                SExecTaskInfo* pTaskInfo, const SArray* pInputWinIdx,
                                                SArray* pOutputWinIdx, SProjectOperatorInfo* pProjectInfo) {
  int32_t lino = 0;
  int32_t code = TSDB_CODE_SUCCESS;

  SExecTaskInfo* savedTaskInfo = gTaskScalarExtra.pTaskInfo;
  sclIsTaskKilled savedIsTaskKilled = gTaskScalarExtra.isTaskKilled;

  if (pTaskInfo != NULL) {
    gTaskScalarExtra.pTaskInfo    = pTaskInfo;
    gTaskScalarExtra.isTaskKilled = isTaskKilled;
  }

  SCorrelationEvalContext correlation = {0};
  SCorrelationWaitHooks wait = {0};
  const SCorrelationEvalContext* pCorrelation = initTaskCorrelationEvalContext(pTaskInfo, &correlation, &wait);
  SScalarExtraInfo scalarExtra = gTaskScalarExtra;
  scalarExtra.pCorrelation = pCorrelation;

  if (hasIndefRowsFunc) {
    setPseudoOutputColInfo(pResult, pCtx, pPseudoList);
  }
  pResult->info.dataLoad = 1;

  SArray* processByRowFunctionCtx = NULL;
  SArray* pProcessedFuncIds = NULL;
  SArray* pGroupedCtxArray = NULL;
  SArray* pGroupOutputWinIdx = NULL;
  if (pSrcBlock == NULL) {
    for (int32_t k = 0; k < numOfOutput; ++k) {
      int32_t outputSlotId = pExpr[k].base.resSchema.slotId;

      if (pExpr[k].pExpr->nodeType != QUERY_NODE_VALUE) {
        qError("project failed at: %s:%d", __func__, __LINE__);
        TAOS_CHECK_EXIT(TSDB_CODE_INVALID_PARA);
      }
      SColumnInfoData* pColInfoData = taosArrayGet(pResult->pDataBlock, outputSlotId);
      TSDB_CHECK_NULL(pColInfoData, code, lino, _exit, terrno);

      int32_t type = pExpr[k].base.pParam[0].param.nType;
      if (TSDB_DATA_TYPE_NULL == type) {
        colDataSetNNULL(pColInfoData, 0, 1);
      } else {
        TAOS_CHECK_EXIT(colDataSetVal(pColInfoData, 0, taosVariantGet(&pExpr[k].base.pParam[0].param, type), false));
      }
    }

    pResult->info.rows = 1;
    goto _exit;
  }

  if (pResult != pSrcBlock) {
    pResult->info.id.groupId = pSrcBlock->info.id.groupId;
    pResult->info.id.baseGId = pSrcBlock->info.id.baseGId;
    if (pSrcBlock->info.parTbName[0]) {
      tstrncpy(pResult->info.parTbName, pSrcBlock->info.parTbName, TSDB_TABLE_NAME_LEN);
    }
    qTrace("%s, parName:%s,groupId:%" PRIu64, __FUNCTION__, pSrcBlock->info.parTbName, pResult->info.id.groupId);
  }

  // if the source equals to the destination, it is to create a new column as the result of scalar
  // function or some operators.
  bool createNewColModel = (pResult == pSrcBlock);
  if (createNewColModel) {
    TAOS_CHECK_EXIT(blockDataEnsureCapacity(pResult, pResult->info.rows));
  }

  int64_t numOfRows = 0;

  for (int32_t k = 0; k < numOfOutput; ++k) {
    int32_t         outputSlotId = pExpr[k].base.resSchema.slotId;
    SqlFunctionCtx* pfCtx = &pCtx[k];
    switch (pExpr[k].pExpr->nodeType) {
      case QUERY_NODE_COLUMN: {
        TAOS_CHECK_EXIT(projectApplyColumn(pResult, pSrcBlock, outputSlotId, pfCtx, &numOfRows, createNewColModel));
        break;
      } 
      case QUERY_NODE_VALUE: {
        TAOS_CHECK_EXIT(projectApplyValue(&pExpr[k], pResult, pSrcBlock, outputSlotId, &numOfRows, createNewColModel));
        break;
      } 
      case QUERY_NODE_OPERATOR: {
        SNode* pRoot = pExpr[k].pExpr->_optrRoot.pRootNode;
        const SScalarExtraInfo* pScalarExtra =
            NULL != pCorrelation && nodesExprHasCorrelation(pRoot) ? &scalarExtra : NULL;
        TAOS_CHECK_EXIT(projectApplyOperator(&pExpr[k], pResult, pSrcBlock, outputSlotId, &numOfRows,
                                             createNewColModel, pExtraParams, pScalarExtra));
        break;
      } 
      case QUERY_NODE_FUNCTION: {
        SNode* pFunction = (SNode*)pExpr[k].pExpr->_function.pFunctNode;
        const SScalarExtraInfo* pScalarExtra =
            NULL != pCorrelation && nodesExprHasCorrelation(pFunction) ? &scalarExtra : NULL;
        TAOS_CHECK_EXIT(projectApplyFunctionWithLineage(pCtx, pfCtx, &pExpr[k], pResult, pSrcBlock, outputSlotId,
                                                        &numOfRows, createNewColModel, pExtraParams, pPseudoList,
                                                        &processByRowFunctionCtx, doSelectFunc, pInputWinIdx,
                                                        pScalarExtra));
        break;
      }
      default: {
        qError("invalid project expr nodeType:%d", pExpr[k].pExpr->nodeType);
        TAOS_CHECK_EXIT(TSDB_CODE_OPS_NOT_SUPPORT);
      }
    }
  }

  if (processByRowFunctionCtx && taosArrayGetSize(processByRowFunctionCtx) > 0) {
    int32_t processByRowSize = taosArrayGetSize(processByRowFunctionCtx);
    SStreamRuntimeFuncInfo* pStreamInfo = (SStreamRuntimeFuncInfo*)pExtraParams;
    bool                    hasOwnedLineage = pInputWinIdx != NULL && pOutputWinIdx != NULL;
    const SArray*           pSourceWinIdx =
        hasOwnedLineage ? pInputWinIdx : (pStreamInfo == NULL ? NULL : pStreamInfo->pStreamBlkWinIdx);
    SArray* pTargetWinIdx =
        hasOwnedLineage ? pOutputWinIdx : (pStreamInfo == NULL ? NULL : pStreamInfo->pStreamBlkWinIdx);
    bool splitByExternalWindow = pSrcBlock != NULL && pStreamInfo != NULL && pStreamInfo->withExternalWindow &&
                                 pSourceWinIdx != NULL && pTargetWinIdx != NULL &&
                                 taosArrayGetSize(pSourceWinIdx) > (hasOwnedLineage ? 0 : 1) &&
                                 (hasOwnedLineage || allProcessByRowCtxSameFuncId(processByRowFunctionCtx));
    pProcessedFuncIds = taosArrayInit(4, sizeof(int32_t));
    TSDB_CHECK_NULL(pProcessedFuncIds, code, lino, _exit, terrno);
    int64_t expectedProcessByRowRows = -1;

    for (int32_t i = 0; i < processByRowSize; ++i) {
      SqlFunctionCtx** ppCurrCtx = taosArrayGet(processByRowFunctionCtx, i);
      TSDB_CHECK_NULL(ppCurrCtx, code, lino, _exit, terrno);
      TSDB_CHECK_NULL(*ppCurrCtx, code, lino, _exit, terrno);

      bool    processed = false;
      int32_t processedNum = taosArrayGetSize(pProcessedFuncIds);
      for (int32_t j = 0; j < processedNum; ++j) {
        int32_t* pFuncId = taosArrayGet(pProcessedFuncIds, j);
        TSDB_CHECK_NULL(pFuncId, code, lino, _exit, terrno);
        if (*pFuncId == (*ppCurrCtx)->functionId) {
          processed = true;
          break;
        }
      }

      if (processed) {
        continue;
      }

      pGroupedCtxArray = taosArrayInit(2, sizeof(SqlFunctionCtx*));
      TSDB_CHECK_NULL(pGroupedCtxArray, code, lino, _exit, terrno);

      for (int32_t j = i; j < processByRowSize; ++j) {
        SqlFunctionCtx** ppTmpCtx = taosArrayGet(processByRowFunctionCtx, j);
        TSDB_CHECK_NULL(ppTmpCtx, code, lino, _exit, terrno);
        TSDB_CHECK_NULL(*ppTmpCtx, code, lino, _exit, terrno);

        if ((*ppTmpCtx)->functionId == (*ppCurrCtx)->functionId) {
          void* px = taosArrayPush(pGroupedCtxArray, ppTmpCtx);
          TSDB_CHECK_NULL(px, code, lino, _exit, terrno);
        }
      }

      SArray* pGroupTargetWinIdx = pTargetWinIdx;
      if (splitByExternalWindow && expectedProcessByRowRows >= 0) {
        pGroupOutputWinIdx = taosArrayInit(TMAX(taosArrayGetSize(pSourceWinIdx), 1), sizeof(int64_t));
        TSDB_CHECK_NULL(pGroupOutputWinIdx, code, lino, _exit, terrno);
        pGroupTargetWinIdx = pGroupOutputWinIdx;
      }

      uint64_t groupOutputRows = 0;
      if (splitByExternalWindow) {
        TAOS_CHECK_EXIT(processByRowInExternalWindows(pGroupedCtxArray, pSrcBlock, pSourceWinIdx, pGroupTargetWinIdx,
                                                      pProjectInfo));
        groupOutputRows = (*ppCurrCtx)->resultInfo->numOfRes;
      } else {
        TAOS_CHECK_EXIT((*ppCurrCtx)->fpSet.processFuncByRow(pGroupedCtxArray));
        if (pProjectInfo != NULL) {
          TAOS_CHECK_EXIT(
              projectValidateProcessByRowResultRows(pGroupedCtxArray, pSrcBlock->info.rows, &groupOutputRows));
        } else {
          groupOutputRows = (*ppCurrCtx)->resultInfo->numOfRes;
        }
      }

      if (pProjectInfo == NULL) {
        expectedProcessByRowRows = (int64_t)groupOutputRows;
      } else if (expectedProcessByRowRows < 0) {
        expectedProcessByRowRows = (int64_t)groupOutputRows;
      } else if ((uint64_t)expectedProcessByRowRows != groupOutputRows ||
                 (splitByExternalWindow && !projectLineageEqual(pTargetWinIdx, pGroupTargetWinIdx))) {
        qError("strict Project process-by-row function groups produced inconsistent rows, expected:%" PRId64
               ", actual:%" PRIu64,
               expectedProcessByRowRows, groupOutputRows);
        code = TSDB_CODE_STREAM_INTERNAL_ERROR;
        goto _exit;
      }
      taosArrayDestroy(pGroupOutputWinIdx);
      pGroupOutputWinIdx = NULL;
      taosArrayDestroy(pGroupedCtxArray);
      pGroupedCtxArray = NULL;

      void* px = taosArrayPush(pProcessedFuncIds, &(*ppCurrCtx)->functionId);
      TSDB_CHECK_NULL(px, code, lino, _exit, terrno);

      numOfRows = expectedProcessByRowRows;
    }

    taosArrayDestroy(pProcessedFuncIds);
    pProcessedFuncIds = NULL;
  }

  if (!createNewColModel) {
    if (numOfRows > INT32_MAX - pResult->info.rows) {
      code = TSDB_CODE_OUT_OF_RANGE;
      qError("project output rows overflow, currentRows:%" PRId64 ", appendRows:%" PRId64,
             (int64_t)pResult->info.rows, numOfRows);
      goto _exit;
    }

    pResult->info.rows += (int32_t)numOfRows;
  }

_exit:
  gTaskScalarExtra.pTaskInfo    = savedTaskInfo;
  gTaskScalarExtra.isTaskKilled = savedIsTaskKilled;

  if (pGroupedCtxArray) {
    taosArrayDestroy(pGroupedCtxArray);
  }
  taosArrayDestroy(pGroupOutputWinIdx);
  if (pProcessedFuncIds) {
    taosArrayDestroy(pProcessedFuncIds);
  }
  if (processByRowFunctionCtx) {
    taosArrayDestroy(processByRowFunctionCtx);
  }
  if (code) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t projectApplyFunctionsWithSelect(SExprInfo* pExpr, SSDataBlock* pResult, SSDataBlock* pSrcBlock,
                                        SqlFunctionCtx* pCtx, int32_t numOfOutput, SArray* pPseudoList,
                                        const void* pExtraParams, bool doSelectFunc, bool hasIndefRowsFunc,
                                        SExecTaskInfo* pTaskInfo) {
  return projectApplyFunctionsWithLineage(pExpr, pResult, pSrcBlock, pCtx, numOfOutput, pPseudoList, pExtraParams,
                                          doSelectFunc, hasIndefRowsFunc, pTaskInfo, NULL, NULL, NULL);
}

int32_t projectApplyFunctions(SExprInfo* pExpr, SSDataBlock* pResult, SSDataBlock* pSrcBlock, SqlFunctionCtx* pCtx,
                              int32_t numOfOutput, SArray* pPseudoList, const void* pExtraParams,
                              SExecTaskInfo* pTaskInfo) {
  return projectApplyFunctionsWithSelect(pExpr, pResult, pSrcBlock, pCtx, numOfOutput, pPseudoList, pExtraParams,
                                         false, true, pTaskInfo);
}

int32_t projectResolveStreamJoinFilter(SOperatorInfo* pOperator, SStreamJoinFilterSpec* pSpec, bool* pEligible) {
  if (pOperator == NULL || pSpec == NULL || pEligible == NULL || pSpec->pColumns == NULL ||
      pSpec->pColumns->elemSize != sizeof(SStreamJoinFilterColumn))
    return TSDB_CODE_INVALID_PARA;
  *pEligible = false;
  const SProjectPhysiNode* pNode = pOperator->pPhyNode;
  if (pNode == NULL || pNode->node.pConditions != NULL || pOperator->exprSupp.pFilterInfo != NULL ||
      pNode->node.pLimit != NULL || pNode->node.pSlimit != NULL || pNode->isUnionDedup ||
      !projectUsesStrictMultiGroupProtocol(pOperator) || pOperator->numOfDownstream != 1 ||
      pOperator->pDownstream == NULL || pOperator->pDownstream[0] == NULL)
    return TSDB_CODE_SUCCESS;
  const SPhysiNode* pChild = pOperator->pDownstream[0]->pPhyNode;
  if (pChild == NULL || pChild->pOutputDataBlockDesc == NULL || pNode->node.pOutputDataBlockDesc == NULL)
    return TSDB_CODE_SUCCESS;
  if (pSpec->inputBlockId != pNode->node.pOutputDataBlockDesc->dataBlockId)
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  SNode* pProj = NULL;
  FOREACH(pProj, pNode->pProjections) {
    if (nodeType(pProj) != QUERY_NODE_TARGET || ((STargetNode*)pProj)->pExpr == NULL ||
        nodeType(((STargetNode*)pProj)->pExpr) != QUERY_NODE_COLUMN)
      return TSDB_CODE_SUCCESS;
  }
  for (int32_t i = 0; i < taosArrayGetSize(pSpec->pColumns);) {
    SStreamJoinFilterColumn* pCol = taosArrayGet(pSpec->pColumns, i);
    const SColumnNode*       pSource = NULL;
    FOREACH(pProj, pNode->pProjections) {
      const STargetNode* pTarget = (const STargetNode*)pProj;
      if (pTarget->slotId == pCol->inputSlot) {
        if (pTarget->dataBlockId != pSpec->inputBlockId || pSource != NULL)
          return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        pSource = (const SColumnNode*)pTarget->pExpr;
      }
    }
    if (pSource == NULL || pSource->dataBlockId != pChild->pOutputDataBlockDesc->dataBlockId)
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    const SSlotDescNode* pInputSlot = NULL;
    const SSlotDescNode* pOutputSlot = NULL;
    SNode*               pEntry = NULL;
    FOREACH(pEntry, pChild->pOutputDataBlockDesc->pSlots) {
      if (((SSlotDescNode*)pEntry)->slotId == pSource->slotId) pInputSlot = (const SSlotDescNode*)pEntry;
    }
    FOREACH(pEntry, pNode->node.pOutputDataBlockDesc->pSlots) {
      if (((SSlotDescNode*)pEntry)->slotId == pCol->inputSlot) pOutputSlot = (const SSlotDescNode*)pEntry;
    }
    if (pInputSlot == NULL || pOutputSlot == NULL || pSource->slotId < 0 || pCol->inputSlot < 0)
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    const SDataType* pTypes[] = {&pSource->node.resType, &pInputSlot->dataType, &pOutputSlot->dataType};
    bool             compatible = true;
    for (int32_t j = 0; j < 3; ++j) {
      const SDataType* pType = pTypes[j];
      if (pType->type != pCol->probeType.type || pType->bytes != pCol->probeType.bytes ||
          pType->scale != pCol->probeType.scale ||
          (pType->type == TSDB_DATA_TYPE_TIMESTAMP && pType->precision != pCol->probeType.precision))
        compatible = false;
    }
    if (!compatible) {
      taosArrayRemove(pSpec->pColumns, i);
      continue;
    }
    pCol->inputSlot = pSource->slotId;
    ++i;
  }
  if (taosArrayGetSize(pSpec->pColumns) == 0) return TSDB_CODE_SUCCESS;
  pSpec->inputBlockId = pChild->pOutputDataBlockDesc->dataBlockId;
  SOperatorInfo* pDownstream = pOperator->pDownstream[0];
  switch (pDownstream->operatorType) {
    case QUERY_NODE_PHYSICAL_PLAN_PROJECT:
      return projectResolveStreamJoinFilter(pDownstream, pSpec, pEligible);
    case QUERY_NODE_PHYSICAL_PLAN_EXTERNAL_WINDOW:
      return extWinResolveStreamJoinFilter(pDownstream, pSpec, pEligible);
    default:
      return TSDB_CODE_SUCCESS;
  }
}

int32_t projectInstallStreamJoinFilter(SOperatorInfo* pOperator, SStreamJoinFilter** ppFilter) {
  if (pOperator == NULL || ppFilter == NULL || *ppFilter == NULL) return TSDB_CODE_INVALID_PARA;
  if (pOperator->numOfDownstream != 1 || pOperator->pDownstream == NULL) return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  return optrInstallStreamJoinFilter(pOperator->pDownstream[0], ppFilter);
}
