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
#include "functionMgt.h"
#include "operator.h"
#include "querytask.h"
#include "streamexecutorInt.h"
#include "tchecksum.h"
#include "tcommon.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tfill.h"
#include "tglobal.h"
#include "tlog.h"
#include "ttime.h"

// static int32_t buildWinResultKey(SRowBuffPos* pPos, SSHashObj* pUpdatedMap, SSHashObj* pResultCache, bool savePos) {
//   int32_t      code = TSDB_CODE_SUCCESS;
//   int32_t      lino = 0;
//   SWinKey*     pKey = pPos->pKey;
//   SRowBuffPos* pPrevResPos = tSimpleHashGet(pResultCache, &pKey->groupId, sizeof(uint64_t));
//   if (pPrevResPos != NULL) {
//     SWinKey* pPrevResKey = (SWinKey*)pPrevResPos->pKey;
//     if (savePos) {
//       code = tSimpleHashPut(pUpdatedMap, pPrevResKey, sizeof(SWinKey), &pPrevResPos, POINTER_BYTES);
//     } else {
//       code = tSimpleHashPut(pUpdatedMap, pPrevResKey, sizeof(SWinKey), NULL, 0);
//     }
//     QUERY_CHECK_CODE(code, lino, _end);
//   }
//   code = tSimpleHashPut(pResultCache, &pKey->groupId, sizeof(uint64_t), &pPos, POINTER_BYTES);
//   QUERY_CHECK_CODE(code, lino, _end);

// _end:
//   if (code != TSDB_CODE_SUCCESS) {
//     qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
//   }
//   return code;
// }

static int32_t doStreamIntervalNonblockAggImpl(SOperatorInfo* pOperator, SSDataBlock* pBlock, SArray* pUpdated) {
  int32_t                           code = TSDB_CODE_SUCCESS;
  int32_t                           lino = 0;
  SStreamIntervalSliceOperatorInfo* pInfo = (SStreamIntervalSliceOperatorInfo*)pOperator->info;
  SResultRowInfo*                   pResultRowInfo = &(pInfo->binfo.resultRowInfo);
  SExecTaskInfo*                    pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*                        pSup = &pOperator->exprSupp;
  int32_t                           numOfOutput = pSup->numOfExprs;
  TSKEY*                            tsCols = NULL;
  int64_t                           groupId = pBlock->info.id.groupId;
  SResultRow*                       pResult = NULL;
  int32_t                           forwardRows = 0;

  SColumnInfoData* pColDataInfo = taosArrayGet(pBlock->pDataBlock, pInfo->primaryTsIndex);
  tsCols = (int64_t*)pColDataInfo->pData;

  int32_t     startPos = 0;
  TSKEY       curTs = getStartTsKey(&pBlock->info.window, tsCols);
  SInervalSlicePoint curPoint = {0};
  SInervalSlicePoint prevPoint = {0};
  STimeWindow curWin =
      getActiveTimeWindow(NULL, pResultRowInfo, curTs, &pInfo->interval, TSDB_ORDER_ASC);
  while (1) {
    int32_t winCode = TSDB_CODE_SUCCESS;
    code = getIntervalSliceCurStateBuf(&pInfo->streamAggSup, &pInfo->interval, pInfo->hasInterpoFunc, &curWin, groupId, &curPoint, &prevPoint, &winCode);
    QUERY_CHECK_CODE(code, lino, _end);
    ASSERT(curPoint.pResPos->pRowBuff != NULL);

    if(winCode != TSDB_CODE_SUCCESS && pInfo->hasInterpoFunc == false) {
      SWinKey curKey = {.ts = curPoint.winKey.win.skey, .groupId = groupId};
      code = getIntervalSlicePrevStateBuf(&pInfo->streamAggSup, &pInfo->interval, &curKey, &prevPoint);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (IS_VALID_WIN_KEY(prevPoint.winKey.win.skey) && winCode != TSDB_CODE_SUCCESS) {
      if (pInfo->hasInterpoFunc && isInterpoWindowFinished(&prevPoint) == false) {
        code = setIntervalSliceOutputBuf(&pInfo->streamAggSup, &prevPoint, pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset);
        QUERY_CHECK_CODE(code, lino, _end);

        resetIntervalSliceFunctionKey(pSup->pCtx, numOfOutput);
        doSetElapsedEndKey(prevPoint.winKey.win.ekey, &pOperator->exprSupp);
        doStreamSliceInterpolation(prevPoint.pLastRow, prevPoint.winKey.win.ekey, curTs, pBlock, startPos,
                                   &pOperator->exprSupp, INTERVAL_SLICE_END, pInfo->pOffsetInfo);
        updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &prevPoint.winKey.win, 1);
        code = applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, startPos, 0,
                                               pBlock->info.rows, numOfOutput);
        QUERY_CHECK_CODE(code, lino, _end);
        setInterpoWindowFinished(&prevPoint);
      }

      void* pResPtr = taosArrayPush(pUpdated, &prevPoint.pResPos);
      QUERY_CHECK_NULL(pResPtr, code, lino, _end, terrno);
    }

    code = setIntervalSliceOutputBuf(&pInfo->streamAggSup, &curPoint, pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset);
    QUERY_CHECK_CODE(code, lino, _end);

    resetIntervalSliceFunctionKey(pSup->pCtx, numOfOutput);
    if (pInfo->hasInterpoFunc && IS_VALID_WIN_KEY(prevPoint.winKey.win.skey) && curPoint.winKey.win.skey != curTs) {
      doStreamSliceInterpolation(prevPoint.pLastRow, curPoint.winKey.win.skey, curTs, pBlock, startPos, &pOperator->exprSupp, INTERVAL_SLICE_START, pInfo->pOffsetInfo);
    }
    forwardRows = getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, curWin.ekey, binarySearchForKey, NULL,
                                           TSDB_ORDER_ASC);
    int32_t prevEndPos = (forwardRows - 1) + startPos;
    if (pInfo->hasInterpoFunc) {
      int32_t endRowId = getQualifiedRowNumDesc(pSup, pBlock, tsCols, prevEndPos, false);
      TSKEY endRowTs = tsCols[endRowId];
      transBlockToSliceResultRow(pBlock, endRowId, endRowTs, curPoint.pLastRow, 0, NULL, NULL, pInfo->pOffsetInfo);
    }

    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &curPoint.winKey.win, 1);
    code = applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, startPos,
                                           forwardRows, pBlock->info.rows, numOfOutput);
    QUERY_CHECK_CODE(code, lino, _end);

    if (curPoint.pLastRow->key == curPoint.winKey.win.ekey) {
      setInterpoWindowFinished(&curPoint);
    }
    releaseOutputBuf(pInfo->streamAggSup.pState, curPoint.pResPos, &pInfo->streamAggSup.stateStore);

    startPos = getNextQualifiedWindow(&pInfo->interval, &curWin, &pBlock->info, tsCols, prevEndPos, TSDB_ORDER_ASC);
    if (startPos < 0) {
      break;
    }
    curTs = tsCols[startPos];
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

int32_t doStreamIntervalNonblockAggNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                           code = TSDB_CODE_SUCCESS;
  int32_t                           lino = 0;
  SStreamIntervalSliceOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*                    pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*                      pAPI = &pOperator->pTaskInfo->storageAPI;
  SExprSupp*                        pSup = &pOperator->exprSupp;
  SStreamAggSupporter*              pAggSup = &pInfo->streamAggSup;

  qDebug("stask:%s  %s status: %d", GET_TASKID(pTaskInfo), getStreamOpName(pOperator->operatorType), pOperator->status);

  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return code;
  }

  doBuildStreamIntervalResult(pOperator, pInfo->streamAggSup.pState, pInfo->binfo.pRes, &pInfo->groupResInfo);
  if (pInfo->binfo.pRes->info.rows != 0) {
    printDataBlock(pInfo->binfo.pRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
    (*ppRes) = pInfo->binfo.pRes;
    return code;
  }

  if (pOperator->status == OP_RES_TO_RETURN) {
    if (pInfo->recvCkBlock) {
      pInfo->recvCkBlock = false;
      printDataBlock(pInfo->pCheckpointRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
      (*ppRes) = pInfo->pCheckpointRes;
      return code;
    }

    pAggSup->stateStore.streamStateClearExpiredState(pAggSup->pState);
    setStreamOperatorCompleted(pOperator);
    (*ppRes) = NULL;
    return code;
  }

  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while (1) {
    SSDataBlock* pBlock = NULL;
    code = downstream->fpSet.getNextFn(downstream, &pBlock);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pBlock == NULL) {
      qDebug("===stream===return data:%s.", getStreamOpName(pOperator->operatorType));
      pOperator->status = OP_RES_TO_RETURN;
      break;
    }

    printSpecDataBlock(pBlock, getStreamOpName(pOperator->operatorType), "recv", GET_TASKID(pTaskInfo));
    setStreamOperatorState(&pInfo->basic, pBlock->info.type);

    switch (pBlock->info.type) {
      case STREAM_NORMAL:
      case STREAM_INVALID: {
        SExprSupp* pExprSup = &pInfo->scalarSup;
        if (pExprSup->pExprInfo != NULL) {
          code = projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      } break;
      case STREAM_CHECKPOINT: {
        pInfo->recvCkBlock = true;
        pAggSup->stateStore.streamStateCommit(pAggSup->pState);
        code = copyDataBlock(pInfo->pCheckpointRes, pBlock);
        QUERY_CHECK_CODE(code, lino, _end);
        continue;
      } break;
      case STREAM_CREATE_CHILD_TABLE: 
      case STREAM_DROP_CHILD_TABLE: {
        (*ppRes) = pBlock;
        return code;
      } break;
      case STREAM_DELETE_DATA:
      case STREAM_DELETE_RESULT:
      case STREAM_CLEAR: {
        continue;
      } break;
      default:
        code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        QUERY_CHECK_CODE(code, lino, _end);
    }

    if (pBlock->info.type == STREAM_NORMAL && pBlock->info.version != 0) {
      // set input version
      pTaskInfo->version = pBlock->info.version;
    }

    code = setInputDataBlock(pSup, pBlock, TSDB_ORDER_ASC, MAIN_SCAN, true);
    QUERY_CHECK_CODE(code, lino, _end);

    code = doStreamIntervalNonblockAggImpl(pOperator, pBlock, pInfo->pUpdated);
    if (code == TSDB_CODE_STREAM_INTERNAL_ERROR) {
      pOperator->status = OP_RES_TO_RETURN;
      code = TSDB_CODE_SUCCESS;
      break;
    }
    QUERY_CHECK_CODE(code, lino, _end);

    if (taosArrayGetSize(pInfo->pUpdated) > 0) {
      break;
    }
  }

  taosArraySort(pInfo->pUpdated, winPosCmprImpl);

  initMultiResInfoFromArrayList(&pInfo->groupResInfo, pInfo->pUpdated);
  pInfo->pUpdated = taosArrayInit(1024, POINTER_BYTES);
  QUERY_CHECK_NULL(pInfo->pUpdated, code, lino, _end, terrno);

  code = blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  doBuildStreamIntervalResult(pOperator, pInfo->streamAggSup.pState, pInfo->binfo.pRes, &pInfo->groupResInfo);
  if (pInfo->binfo.pRes->info.rows != 0) {
    printDataBlock(pInfo->binfo.pRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
    (*ppRes) = pInfo->binfo.pRes;
    return code;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
    pTaskInfo->code = code;
  }
  pAggSup->stateStore.streamStateClearExpiredState(pAggSup->pState);
  setStreamOperatorCompleted(pOperator);
  (*ppRes) = NULL;
  return code;
}
