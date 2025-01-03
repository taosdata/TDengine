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

#define STREAM_INTERVAL_NONBLOCK_OP_STATE_NAME      "StreamIntervalNonblockHistoryState"

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

void streamIntervalNonblockReleaseState(SOperatorInfo* pOperator) {
  SStreamIntervalSliceOperatorInfo* pInfo = pOperator->info;
  SStreamAggSupporter*              pAggSup = &pInfo->streamAggSup;
  pAggSup->stateStore.streamStateClearExpiredState(pAggSup->pState, 1);
  pAggSup->stateStore.streamStateCommit(pAggSup->pState);
  int32_t resSize = sizeof(TSKEY);
  pAggSup->stateStore.streamStateSaveInfo(pAggSup->pState, STREAM_INTERVAL_NONBLOCK_OP_STATE_NAME,
                                          strlen(STREAM_INTERVAL_NONBLOCK_OP_STATE_NAME), &pInfo->twAggSup.maxTs,
                                          resSize);

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (downstream->fpSet.releaseStreamStateFn) {
    downstream->fpSet.releaseStreamStateFn(downstream);
  }
  qDebug("%s===stream===streamIntervalNonblockReleaseState:%" PRId64, GET_TASKID(pOperator->pTaskInfo), pInfo->twAggSup.maxTs);
}

void streamIntervalNonblockReloadState(SOperatorInfo* pOperator) {
  int32_t                           code = TSDB_CODE_SUCCESS;
  int32_t                           lino = 0;
  SStreamIntervalSliceOperatorInfo* pInfo = pOperator->info;
  SStreamAggSupporter*              pAggSup = &pInfo->streamAggSup;
  SExecTaskInfo*                    pTaskInfo = pOperator->pTaskInfo;
  int32_t                           size = 0;
  void*                             pBuf = NULL;
  code = pAggSup->stateStore.streamStateGetInfo(pAggSup->pState, STREAM_INTERVAL_NONBLOCK_OP_STATE_NAME,
                                                strlen(STREAM_INTERVAL_NONBLOCK_OP_STATE_NAME), &pBuf, &size);
  QUERY_CHECK_CODE(code, lino, _end);

  TSKEY ts = *(TSKEY*)pBuf;
  taosMemoryFreeClear(pBuf);
  pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, ts);
  pAggSup->stateStore.streamStateReloadInfo(pAggSup->pState, ts);

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (downstream->fpSet.reloadStreamStateFn) {
    downstream->fpSet.reloadStreamStateFn(downstream);
  }
  qDebug("%s===stream===streamIntervalNonblockReloadState:%" PRId64, GET_TASKID(pOperator->pTaskInfo), ts);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
}

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

    if(winCode != TSDB_CODE_SUCCESS && pInfo->hasInterpoFunc == false && pInfo->numOfKeep == 1) {
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

      if (pInfo->numOfKeep == 1) {
        void* pResPtr = taosArrayPush(pUpdated, &prevPoint.pResPos);
        QUERY_CHECK_NULL(pResPtr, code, lino, _end, terrno);
      } else {
        SWinKey curKey = {.groupId = groupId};
        curKey.ts = taosTimeAdd(curTs, -pInfo->interval.interval, pInfo->interval.intervalUnit,
                                pInfo->interval.precision, NULL) + 1;
        code = pInfo->streamAggSup.stateStore.streamStateGetAllPrev(pInfo->streamAggSup.pState, &curKey, pUpdated,
                                                                    pInfo->numOfKeep);
        QUERY_CHECK_CODE(code, lino, _end);
      }
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
    curPoint.pResPos->beUpdated = true;

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

int32_t getHistoryRemainResultInfo(SStreamAggSupporter* pAggSup, SArray* pUpdated, int32_t capacity) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (pAggSup->pCur == NULL) {
    pAggSup->pCur = pAggSup->stateStore.streamStateGetLastStateCur(pAggSup->pState);
  }
  int32_t      num = capacity - taosArrayGetSize(pUpdated);
  SRowBuffPos* pPos = NULL;
  for (int32_t i = 0; i < num; i++) {
    int32_t winCode = pAggSup->stateStore.streamStateLastStateGetKVByCur(pAggSup->pCur, (void**)&pPos);
    if (winCode == TSDB_CODE_FAILED) {
      pAggSup->stateStore.streamStateFreeCur(pAggSup->pCur);
      pAggSup->pCur = NULL;
      break;
    }
    if (pPos->beUpdated == false) {
      pAggSup->stateStore.streamStateLastStateCurNext(pAggSup->pCur);
      continue;
    }
    pPos->beUpdated = false;
    void* tmpPtr = taosArrayPush(pUpdated, &pPos);
    QUERY_CHECK_NULL(tmpPtr, code, lino, _end, terrno);

    pAggSup->stateStore.streamStateLastStateCurNext(pAggSup->pCur);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s.", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t buildIntervalHistoryResult(SOperatorInfo* pOperator) {
  int32_t                           code = TSDB_CODE_SUCCESS;
  int32_t                           lino = 0;
  SStreamIntervalSliceOperatorInfo* pInfo = pOperator->info;
  SStreamAggSupporter*              pAggSup = &pInfo->streamAggSup;
  code = getHistoryRemainResultInfo(pAggSup, pInfo->pUpdated, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);
  if (taosArrayGetSize(pInfo->pUpdated) > 0) {
    taosArraySort(pInfo->pUpdated, winPosCmprImpl);
    initMultiResInfoFromArrayList(&pInfo->groupResInfo, pInfo->pUpdated);
    pInfo->pUpdated = taosArrayInit(1024, POINTER_BYTES);
    QUERY_CHECK_NULL(pInfo->pUpdated, code, lino, _end, terrno);

    doBuildStreamIntervalResult(pOperator, pInfo->streamAggSup.pState, pInfo->binfo.pRes, &pInfo->groupResInfo);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s.", __func__, lino, tstrerror(code));
  }
  return code;
}

void releaseFlusedPos(void* pRes) {
  SRowBuffPos* pPos = *(SRowBuffPos**)pRes;
  if (pPos != NULL && pPos->needFree) {
    pPos->beUsed = false;
  }
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
    if (isFillHistoryOperator(&pInfo->basic)) {
      code = buildIntervalHistoryResult(pOperator);
      QUERY_CHECK_CODE(code, lino, _end);
      if (pInfo->binfo.pRes->info.rows != 0) {
        printDataBlock(pInfo->binfo.pRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
        (*ppRes) = pInfo->binfo.pRes;
        return code;
      }
    }

    if (pInfo->recvCkBlock) {
      pInfo->recvCkBlock = false;
      printDataBlock(pInfo->pCheckpointRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
      (*ppRes) = pInfo->pCheckpointRes;
      return code;
    }

    pAggSup->stateStore.streamStateClearExpiredState(pAggSup->pState, pInfo->numOfKeep);
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
      case STREAM_RECALCULATE_DATA:// todo(liuyao) for debug
      case STREAM_RECALCULATE_DELETE: // todo(liuyao) for debug
      case STREAM_DELETE_DATA: {
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
    }
    QUERY_CHECK_CODE(code, lino, _end);
    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.window.ekey);

    if (taosArrayGetSize(pInfo->pUpdated) > 0) {
      break;
    }
  }

  if (pOperator->status == OP_RES_TO_RETURN && isFillHistoryOperator(&pInfo->basic)) {
    getHistoryRemainResultInfo(pAggSup, pInfo->pUpdated, pOperator->resultInfo.capacity);
  }

  taosArraySort(pInfo->pUpdated, winPosCmprImpl);
  if (pInfo->numOfKeep > 1) {
    taosArrayRemoveDuplicate(pInfo->pUpdated, winPosCmprImpl, releaseFlusedPos);
  }

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

  pAggSup->stateStore.streamStateClearExpiredState(pAggSup->pState, pInfo->numOfKeep);
  setStreamOperatorCompleted(pOperator);
  (*ppRes) = NULL;
  return code;
}
