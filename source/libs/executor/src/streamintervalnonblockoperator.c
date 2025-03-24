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
#include "streaminterval.h"
#include "tchecksum.h"
#include "tcommon.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tfill.h"
#include "tglobal.h"
#include "tlog.h"
#include "ttime.h"

#define STREAM_INTERVAL_NONBLOCK_OP_STATE_NAME "StreamIntervalNonblockHistoryState"

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

void releaseFlusedPos(void* pRes) {
  SRowBuffPos* pPos = *(SRowBuffPos**)pRes;
  if (pPos != NULL && pPos->needFree) {
    pPos->beUsed = false;
  }
}

void getStateKeepInfo(SNonBlockAggSupporter* pNbSup, bool isRecOp, int32_t* pNumRes, TSKEY* pTsRes) {
  if (isRecOp) {
    (*pNumRes) = 0;
    (*pTsRes) = INT64_MIN;
  } else {
    (*pNumRes) = pNbSup->numOfKeep;
    (*pTsRes) = pNbSup->tsOfKeep;
  }
}

void streamIntervalNonblockReleaseState(SOperatorInfo* pOperator) {
  SStreamIntervalSliceOperatorInfo* pInfo = pOperator->info;
  SStreamAggSupporter*              pAggSup = &pInfo->streamAggSup;
  pAggSup->stateStore.streamStateClearExpiredState(pAggSup->pState, pInfo->nbSup.numOfKeep, pInfo->nbSup.tsOfKeep);
  pAggSup->stateStore.streamStateCommit(pAggSup->pState);
  int32_t resSize = sizeof(TSKEY);
  pAggSup->stateStore.streamStateSaveInfo(pAggSup->pState, STREAM_INTERVAL_NONBLOCK_OP_STATE_NAME,
                                          strlen(STREAM_INTERVAL_NONBLOCK_OP_STATE_NAME), &pInfo->twAggSup.maxTs,
                                          resSize);

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (downstream->fpSet.releaseStreamStateFn) {
    downstream->fpSet.releaseStreamStateFn(downstream);
  }
  qDebug("%s===stream===streamIntervalNonblockReleaseState:%" PRId64, GET_TASKID(pOperator->pTaskInfo),
         pInfo->twAggSup.maxTs);
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

int32_t saveRecWindowToDisc(SSessionKey* pWinKey, uint64_t uid, EStreamType mode, STableTsDataState* pTsDataState,
                            SStreamAggSupporter* pAggSup) {
  int32_t len = copyRecDataToBuff(pWinKey->win.skey, pWinKey->win.ekey, uid, -1, mode, NULL, 0,
                                  pTsDataState->pRecValueBuff, pTsDataState->recValueLen);
  return pAggSup->stateStore.streamStateSessionSaveToDisk(pTsDataState, pWinKey, pTsDataState->pRecValueBuff, len);
}

static int32_t checkAndSaveWinStateToDisc(int32_t startIndex, SArray* pUpdated, uint64_t uid,
                                          STableTsDataState* pTsDataState, SStreamAggSupporter* pAggSup,
                                          SInterval* pInterval) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t mode = 0;
  int32_t size = taosArrayGetSize(pUpdated);
  for (int32_t i = startIndex; i < size; i++) {
    SRowBuffPos* pWinPos = taosArrayGetP(pUpdated, i);
    SWinKey*     pKey = pWinPos->pKey;
    int32_t      winRes = pAggSup->stateStore.streamStateGetRecFlag(pAggSup->pState, pKey, sizeof(SWinKey), &mode);
    if (winRes == TSDB_CODE_SUCCESS) {
      SSessionKey winKey = {
          .win.skey = pKey->ts, .win.ekey = taosTimeGetIntervalEnd(pKey->ts, pInterval), .groupId = pKey->groupId};
      code = saveRecWindowToDisc(&winKey, uid, mode, pTsDataState, pAggSup);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s.", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t doStreamIntervalNonblockAggImpl(SOperatorInfo* pOperator, SSDataBlock* pBlock) {
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

  int32_t            startPos = 0;
  TSKEY              curTs = getStartTsKey(&pBlock->info.window, tsCols);
  SInervalSlicePoint curPoint = {0};
  SInervalSlicePoint prevPoint = {0};
  STimeWindow        curWin = getActiveTimeWindow(NULL, pResultRowInfo, curTs, &pInfo->interval, TSDB_ORDER_ASC);
  while (1) {
    int32_t winCode = TSDB_CODE_SUCCESS;
    code = getIntervalSliceCurStateBuf(&pInfo->streamAggSup, &pInfo->interval, pInfo->hasInterpoFunc, &curWin, groupId,
                                       &curPoint, &prevPoint, &winCode);
    QUERY_CHECK_CODE(code, lino, _end);

    if (winCode != TSDB_CODE_SUCCESS && pInfo->hasInterpoFunc == false && pInfo->nbSup.numOfKeep == 1) {
      SWinKey curKey = {.ts = curPoint.winKey.win.skey, .groupId = groupId};
      code = getIntervalSlicePrevStateBuf(&pInfo->streamAggSup, &pInfo->interval, &curKey, &prevPoint);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (IS_VALID_WIN_KEY(prevPoint.winKey.win.skey) && winCode != TSDB_CODE_SUCCESS) {
      if (pInfo->hasInterpoFunc && isInterpoWindowFinished(&prevPoint) == false) {
        code = setIntervalSliceOutputBuf(&pInfo->streamAggSup, &prevPoint, pSup->pCtx, numOfOutput,
                                         pSup->rowEntryInfoOffset);
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

      if (pInfo->nbSup.numOfKeep == 1) {
        void* pResPtr = taosArrayPush(pInfo->pUpdated, &prevPoint.pResPos);
        QUERY_CHECK_NULL(pResPtr, code, lino, _end, terrno);
        int32_t  mode = 0;
        SWinKey* pKey = prevPoint.pResPos->pKey;
        int32_t  winRes = pInfo->streamAggSup.stateStore.streamStateGetRecFlag(pInfo->streamAggSup.pState, pKey,
                                                                               sizeof(SWinKey), &mode);
        if (winRes == TSDB_CODE_SUCCESS) {
          code = saveRecWindowToDisc(&prevPoint.winKey, pBlock->info.id.uid, mode, pInfo->basic.pTsDataState,
                                     &pInfo->streamAggSup);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      } else {
        SWinKey curKey = {.groupId = groupId};
        curKey.ts = taosTimeAdd(curTs, -pInfo->interval.interval, pInfo->interval.intervalUnit,
                                pInfo->interval.precision, NULL) +
                    1;
        int32_t startIndex = taosArrayGetSize(pInfo->pUpdated);
        code = pInfo->streamAggSup.stateStore.streamStateGetAllPrev(pInfo->streamAggSup.pState, &curKey,
                                                                    pInfo->pUpdated, pInfo->nbSup.numOfKeep);
        QUERY_CHECK_CODE(code, lino, _end);
        if (!isRecalculateOperator(&pInfo->basic)) {
          code = checkAndSaveWinStateToDisc(startIndex, pInfo->pUpdated, 0, pInfo->basic.pTsDataState, &pInfo->streamAggSup, &pInfo->interval);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
    }

    code =
        setIntervalSliceOutputBuf(&pInfo->streamAggSup, &curPoint, pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset);
    QUERY_CHECK_CODE(code, lino, _end);

    resetIntervalSliceFunctionKey(pSup->pCtx, numOfOutput);
    if (pInfo->hasInterpoFunc && IS_VALID_WIN_KEY(prevPoint.winKey.win.skey) && curPoint.winKey.win.skey != curTs) {
      doStreamSliceInterpolation(prevPoint.pLastRow, curPoint.winKey.win.skey, curTs, pBlock, startPos,
                                 &pOperator->exprSupp, INTERVAL_SLICE_START, pInfo->pOffsetInfo);
    }
    forwardRows = getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, curWin.ekey, binarySearchForKey, NULL,
                                           TSDB_ORDER_ASC);
    int32_t prevEndPos = (forwardRows - 1) + startPos;
    if (pInfo->hasInterpoFunc) {
      int32_t endRowId = getQualifiedRowNumDesc(pSup, pBlock, tsCols, prevEndPos, false);
      TSKEY   endRowTs = tsCols[endRowId];
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

int32_t getHistoryRemainResultInfo(SStreamAggSupporter* pAggSup, int32_t numOfState, SArray* pUpdated,
                                   int32_t capacity) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (pAggSup->pCur == NULL) {
    goto _end;
  }

  int32_t num = capacity - taosArrayGetSize(pUpdated);
  for (int32_t i = 0; i < num; i++) {
    int32_t winCode = pAggSup->stateStore.streamStateNLastStateGetKVByCur(pAggSup->pCur, numOfState, pUpdated);
    if (winCode == TSDB_CODE_FAILED) {
      pAggSup->stateStore.streamStateFreeCur(pAggSup->pCur);
      pAggSup->pCur = NULL;
      break;
    }

    pAggSup->stateStore.streamStateLastStateCurNext(pAggSup->pCur);
    num = capacity - taosArrayGetSize(pUpdated);
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
  SStreamNotifyEventSupp*           pNotifySup = &pInfo->basic.notifyEventSup;
  bool                              addNotifyEvent = false;

  code = getHistoryRemainResultInfo(pAggSup, pInfo->nbSup.numOfKeep, pInfo->pUpdated, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);
  if (taosArrayGetSize(pInfo->pUpdated) > 0) {
    taosArraySort(pInfo->pUpdated, winPosCmprImpl);
    if (pInfo->nbSup.numOfKeep > 1) {
      taosArrayRemoveDuplicate(pInfo->pUpdated, winPosCmprImpl, releaseFlusedPos);
    }
    initMultiResInfoFromArrayList(&pInfo->groupResInfo, pInfo->pUpdated);
    pInfo->pUpdated = taosArrayInit(1024, POINTER_BYTES);
    QUERY_CHECK_NULL(pInfo->pUpdated, code, lino, _end, terrno);

    doBuildStreamIntervalResult(pOperator, pInfo->streamAggSup.pState, pInfo->binfo.pRes, &pInfo->groupResInfo, addNotifyEvent ? pNotifySup->pSessionKeys : NULL);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s.", __func__, lino, tstrerror(code));
  }
  return code;
}

static void removeDataDeleteResults(SArray* pUpdatedWins, SArray* pDelWins) {
  int32_t size = taosArrayGetSize(pUpdatedWins);
  int32_t delSize = taosArrayGetSize(pDelWins);
  if (delSize == 0 || size == 0) {
    return;
  }
  taosArraySort(pDelWins, winKeyCmprImpl);
  taosArrayRemoveDuplicate(pDelWins, winKeyCmprImpl, NULL);
  delSize = taosArrayGetSize(pDelWins);

  for (int32_t i = 0; i < size; i++) {
    SRowBuffPos* pPos = (SRowBuffPos*) taosArrayGetP(pUpdatedWins, i);
    SWinKey*     pResKey = (SWinKey*)pPos->pKey;
    int32_t      index = binarySearchCom(pDelWins, delSize, pResKey, TSDB_ORDER_DESC, compareWinKey);
    if (index >= 0 && 0 == compareWinKey(pResKey, pDelWins, index)) {
      taosArrayRemove(pDelWins, index);
      delSize = taosArrayGetSize(pDelWins);
    }
  }
}

static int32_t doTransformRecalculateWindows(SExecTaskInfo* pTaskInfo, SInterval* pInterval, SSDataBlock* pBlock,
                                             SArray* pUpWins) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SColumnInfoData* pStartTsCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  TSKEY*           startTsCols = (TSKEY*)pStartTsCol->pData;
  SColumnInfoData* pEndTsCol = taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
  TSKEY*           endTsCols = (TSKEY*)pEndTsCol->pData;
  SColumnInfoData* pCalStTsCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX);
  TSKEY*           calStTsCols = (TSKEY*)pCalStTsCol->pData;
  SColumnInfoData* pCalEnTsCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX);
  TSKEY*           calEnTsCols = (TSKEY*)pCalEnTsCol->pData;
  SColumnInfoData* pGpCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  uint64_t*        pGpDatas = (uint64_t*)pGpCol->pData;
  for (int32_t i = 0; i < pBlock->info.rows; i++) {
    STimeWindow win = {.skey = startTsCols[i], .ekey = endTsCols[i]};
    do {
      if (!inCalSlidingWindow(pInterval, &win, calStTsCols[i], calEnTsCols[i], pBlock->info.type)) {
        getNextTimeWindow(pInterval, &win, TSDB_ORDER_ASC);
        continue;
      }

      uint64_t winGpId = pGpDatas[i];
      SWinKey  winRes = {.ts = win.skey, .groupId = winGpId};
      void*    pTmp = taosArrayPush(pUpWins, &winRes);
      QUERY_CHECK_NULL(pTmp, code, lino, _end, terrno);
      getNextTimeWindow(pInterval, &win, TSDB_ORDER_ASC);
    } while (win.ekey <= endTsCols[i]);
  }
_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

static int32_t buildOtherResult(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                           code = TSDB_CODE_SUCCESS;
  int32_t                           lino = 0;
  SStreamIntervalSliceOperatorInfo* pInfo = pOperator->info;
  SStreamAggSupporter*              pAggSup = &pInfo->streamAggSup;
  SExecTaskInfo*                    pTaskInfo = pOperator->pTaskInfo;
  if (isHistoryOperator(&pInfo->basic) && isSingleOperator(&pInfo->basic)) {
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

  if (pInfo->twAggSup.minTs != INT64_MAX) {
    pInfo->nbSup.tsOfKeep = pInfo->twAggSup.minTs;
  }

  if ( !(isFinalOperator(&pInfo->basic) && (isRecalculateOperator(&pInfo->basic) || isHistoryOperator(&pInfo->basic))) ) {
    int32_t numOfKeep = 0;
    TSKEY tsOfKeep = INT64_MAX;
    getStateKeepInfo(&pInfo->nbSup, isRecalculateOperator(&pInfo->basic), &numOfKeep, &tsOfKeep);
    pAggSup->stateStore.streamStateClearExpiredState(pAggSup->pState, numOfKeep, tsOfKeep);
  }

  pInfo->twAggSup.minTs = INT64_MAX;
  pInfo->basic.numOfRecv = 0;
  setStreamOperatorCompleted(pOperator);
  if (isFinalOperator(&pInfo->basic) && isRecalculateOperator(&pInfo->basic) && tSimpleHashGetSize(pInfo->nbSup.pPullDataMap) == 0) {
    qInfo("===stream===%s recalculate is finished.", GET_TASKID(pTaskInfo));
    pAggSup->stateStore.streamStateClearExpiredState(pAggSup->pState, 0, INT64_MAX);
    pTaskInfo->streamInfo.recoverScanFinished = true;
  }
  (*ppRes) = NULL;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

int32_t copyNewResult(SSHashObj** ppWinUpdated, SArray* pUpdated, __compar_fn_t compar) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(*ppWinUpdated, pIte, &iter)) != NULL) {
    void* tmp = taosArrayPush(pUpdated, pIte);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
  }
  taosArraySort(pUpdated, compar);
  if (tSimpleHashGetSize(*ppWinUpdated) < 4096) {
    tSimpleHashClear(*ppWinUpdated);
  } else {
    tSimpleHashClear(*ppWinUpdated);
    tSimpleHashCleanup(*ppWinUpdated);
    _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
    (*ppWinUpdated) = tSimpleHashInit(1024, hashFn);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t closeNonblockIntervalWindow(SSHashObj* pHashMap, STimeWindowAggSupp* pTwSup, SInterval* pInterval,
                                           SArray* pUpdated, SExecTaskInfo* pTaskInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pHashMap, pIte, &iter)) != NULL) {
    void*    key = tSimpleHashGetKey(pIte, NULL);
    SWinKey* pWinKey = (SWinKey*)key;

    STimeWindow win = {
        .skey = pWinKey->ts,
        .ekey = taosTimeAdd(win.skey, pInterval->interval, pInterval->intervalUnit, pInterval->precision, NULL) - 1,
    };

    if (isCloseWindow(&win, pTwSup)) {
      void* pTemp = taosArrayPush(pUpdated, pIte);
      QUERY_CHECK_NULL(pTemp, code, lino, _end, terrno);

      int32_t tmpRes = tSimpleHashIterateRemove(pHashMap, pWinKey, sizeof(SWinKey), &pIte, &iter);
      qTrace("%s at line %d res:%d", __func__, __LINE__, tmpRes);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

static int32_t doProcessRecalculateReq(SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  int32_t                           code = TSDB_CODE_SUCCESS;
  int32_t                           lino = 0;
  SStreamIntervalSliceOperatorInfo* pInfo = pOperator->info;
  SInterval*                        pInterval = &pInfo->interval;
  SExecTaskInfo*                    pTaskInfo = pOperator->pTaskInfo;
  SColumnInfoData*                  pStartTsCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  TSKEY*                            startTsCols = (TSKEY*)pStartTsCol->pData;
  SColumnInfoData*                  pEndTsCol = taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
  TSKEY*                            endTsCols = (TSKEY*)pEndTsCol->pData;
  SColumnInfoData*                  pCalStTsCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX);
  TSKEY*                            calStTsCols = (TSKEY*)pCalStTsCol->pData;
  SColumnInfoData*                  pCalEnTsCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX);
  TSKEY*                            calEnTsCols = (TSKEY*)pCalEnTsCol->pData;
  SColumnInfoData*                  pGpCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  uint64_t*                         pGpDatas = (uint64_t*)pGpCol->pData;
  SColumnInfoData*                  pUidCol = taosArrayGet(pBlock->pDataBlock, UID_COLUMN_INDEX);
  uint64_t*                         pUidDatas = (uint64_t*)pUidCol->pData;

  for (int32_t i = 0; i < pBlock->info.rows; i++) {
    SResultRowInfo dumyInfo = {0};
    dumyInfo.cur.pageId = -1;

    STimeWindow win = getActiveTimeWindow(NULL, &dumyInfo, startTsCols[i], pInterval, TSDB_ORDER_ASC);

    do {
      if (!inCalSlidingWindow(pInterval, &win, calStTsCols[i], calEnTsCols[i], pBlock->info.type)) {
        getNextTimeWindow(pInterval, &win, TSDB_ORDER_ASC);
        continue;
      }

      SWinKey key = {.ts = win.skey, .groupId = pGpDatas[i]};
      bool isLastWin = false;
      if (pInfo->streamAggSup.stateStore.streamStateCheck(pInfo->streamAggSup.pState, &key,
                                                          isFinalOperator(&pInfo->basic), &isLastWin)) {
        qDebug("===stream===%s set recalculate flag ts:%" PRId64 ",group id:%" PRIu64, GET_TASKID(pTaskInfo), key.ts,
               key.groupId);
        pInfo->streamAggSup.stateStore.streamStateSetRecFlag(pInfo->streamAggSup.pState, &key, sizeof(SWinKey),
                                                             pBlock->info.type);
        if ((isFinalOperator(&pInfo->basic) && isCloseWindow(&win, &pInfo->twAggSup)) || (isSingleOperator(&pInfo->basic) && isLastWin == false) ) {
          SSessionKey winKey = {.win = win, .groupId = key.groupId};
          code = saveRecWindowToDisc(&winKey, pUidDatas[i], pBlock->info.type, pInfo->basic.pTsDataState,
                                     &pInfo->streamAggSup);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      } else {
        SSessionKey winKey = {.win = win, .groupId = key.groupId};
        code = saveRecWindowToDisc(&winKey, pUidDatas[i], pBlock->info.type, pInfo->basic.pTsDataState,
                                   &pInfo->streamAggSup);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      getNextTimeWindow(pInterval, &win, TSDB_ORDER_ASC);
    } while (win.ekey <= endTsCols[i]);
  }
_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

static int32_t addDataPullWindowInfo(SSHashObj* pPullMap, SPullWindowInfo* pPullKey, int32_t numOfChild, SExecTaskInfo* pTaskInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SArray* childIds = taosArrayInit(numOfChild, sizeof(int32_t));
  QUERY_CHECK_NULL(childIds, code, lino, _end, terrno);
  for (int32_t i = 0; i < numOfChild; i++) {
    void* pTemp = taosArrayPush(childIds, &i);
    QUERY_CHECK_NULL(pTemp, code, lino, _end, terrno);
  }
  code = tSimpleHashPut(pPullMap, pPullKey, sizeof(SPullWindowInfo), &childIds, POINTER_BYTES);
  QUERY_CHECK_CODE(code, lino, _end);
_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s %s failed at line %d since %s", GET_TASKID(pTaskInfo), __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t buildRetriveRequest(SExecTaskInfo* pTaskInfo, SStreamAggSupporter* pAggSup, STableTsDataState* pTsDataState,
                            SNonBlockAggSupporter* pNbSup) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  code = pAggSup->stateStore.streamStateMergeAllScanRange(pTsDataState);
  QUERY_CHECK_CODE(code, lino, _end);

  while (1) {
    SScanRange range = {0};
    code = pAggSup->stateStore.streamStatePopScanRange(pTsDataState, &range);
    QUERY_CHECK_CODE(code, lino, _end);
    if (IS_INVALID_RANGE(range)) {
      break;
    }
    void*   pIte = NULL;
    int32_t iter = 0;
    while ((pIte = tSimpleHashIterate(range.pGroupIds, pIte, &iter)) != NULL) {
      uint64_t        groupId = *(uint64_t*)tSimpleHashGetKey(pIte, NULL);
      SPullWindowInfo pullReq = {.window = range.win, .groupId = groupId, .calWin = range.calWin};
      code = addDataPullWindowInfo(pNbSup->pPullDataMap, &pullReq, pNbSup->numOfChild, pTaskInfo);
      QUERY_CHECK_CODE(code, lino, _end);
      void*           pTemp = taosArrayPush(pNbSup->pPullWins, &pullReq);
      QUERY_CHECK_NULL(pTemp, code, lino, _end, terrno);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
    pTaskInfo->code = code;
  }
  return code;
}

int32_t processDataPullOver(SSDataBlock* pBlock, SSHashObj* pPullMap, SExecTaskInfo* pTaskInfo) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SColumnInfoData* pStartCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  TSKEY*           pTsStartData = (TSKEY*)pStartCol->pData;
  SColumnInfoData* pEndCol = taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
  TSKEY*           pTsEndData = (TSKEY*)pEndCol->pData;
  SColumnInfoData* pCalStartCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX);
  TSKEY*           pCalTsStartData = (TSKEY*)pCalStartCol->pData;
  SColumnInfoData* pCalEndCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX);
  TSKEY*           pCalTsEndData = (TSKEY*)pCalEndCol->pData;
  SColumnInfoData* pGroupCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  uint64_t*        pGroupIdData = (uint64_t*)pGroupCol->pData;
  int32_t          chId = getChildIndex(pBlock);
  for (int32_t i = 0; i < pBlock->info.rows; i++) {
    SPullWindowInfo pull = {.window.skey = pTsStartData[i],
                            .window.ekey = pTsEndData[i],
                            .groupId = pGroupIdData[i],
                            .calWin.skey = pCalTsStartData[i],
                            .calWin.ekey = pCalTsEndData[i]};
    void*           pChIds = tSimpleHashGet(pPullMap, &pull, sizeof(SPullWindowInfo));
    if (pChIds == NULL) {
      qInfo("===stream===%s did not find retrive window. ts:%" PRId64 ",groupId:%" PRIu64 ",child id %d",
            GET_TASKID(pTaskInfo), pull.window.skey, pull.groupId, chId);
      continue;
    }
    SArray* chArray = *(SArray**)pChIds;
    int32_t index = taosArraySearchIdx(chArray, &chId, compareInt32Val, TD_EQ);
    if (index == -1) {
      qInfo("===stream===%s did not find child id. retrive window ts:%" PRId64 ",groupId:%" PRIu64 ",child id %d",
            GET_TASKID(pTaskInfo), pull.window.skey, pull.groupId, chId);
      continue;
    }
    qDebug("===stream===%s retrive window %" PRId64 " delete child id %d", GET_TASKID(pTaskInfo), pull.window.skey,
           chId);
    taosArrayRemove(chArray, index);
    if (taosArrayGetSize(chArray) == 0) {
      // pull data is over
      taosArrayDestroy(chArray);
      int32_t tmpRes = tSimpleHashRemove(pPullMap, &pull, sizeof(SPullWindowInfo));
       qDebug("===stream===%s retrive pull data over. ts:%" PRId64 ",groupId:%" PRIu64 , GET_TASKID(pTaskInfo), pull.window.skey,
              pull.groupId);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
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

  code = buildIntervalSliceResult(pOperator, ppRes);
  QUERY_CHECK_CODE(code, lino, _end);
  if ((*ppRes) != NULL) {
    return code;
  }

  if (isHistoryOperator(&pInfo->basic) && !isFinalOperator(&pInfo->basic)) {
    pAggSup->stateStore.streamStateClearExpiredState(pAggSup->pState, pInfo->nbSup.numOfKeep, pInfo->nbSup.tsOfKeep);
  }

  if (pOperator->status == OP_RES_TO_RETURN) {
    return buildOtherResult(pOperator, ppRes);
  }

  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while (1) {
    if (isTaskKilled(pTaskInfo)) {
      qInfo("===stream===%s task is killed, code %s", GET_TASKID(pTaskInfo), tstrerror(pTaskInfo->code));
      (*ppRes) = NULL;
      return code;
    }
    SSDataBlock* pBlock = NULL;
    code = downstream->fpSet.getNextFn(downstream, &pBlock);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pBlock == NULL) {
      qDebug("===stream===%s return data:%s. rev rows:%d", GET_TASKID(pTaskInfo),
             getStreamOpName(pOperator->operatorType), pInfo->basic.numOfRecv);
      if (isFinalOperator(&pInfo->basic)) {
        if (isRecalculateOperator(&pInfo->basic)) {
          code = buildRetriveRequest(pTaskInfo, pAggSup, pInfo->basic.pTsDataState, &pInfo->nbSup);
          QUERY_CHECK_CODE(code, lino, _end);
        } else {
          code = pAggSup->stateStore.streamStateFlushReaminInfoToDisk(pInfo->basic.pTsDataState);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
      pOperator->status = OP_RES_TO_RETURN;
      break;
    }
    pInfo->basic.numOfRecv += pBlock->info.rows;

    printSpecDataBlock(pBlock, getStreamOpName(pOperator->operatorType), "recv", GET_TASKID(pTaskInfo));
    setStreamOperatorState(&pInfo->basic, pBlock->info.type);

    switch (pBlock->info.type) {
      case STREAM_NORMAL:
      case STREAM_INVALID:
      case STREAM_PULL_DATA: {
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
        printDataBlock(pBlock, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
        (*ppRes) = pBlock;
        return code;
      } break;
      case STREAM_RECALCULATE_DATA:
      case STREAM_RECALCULATE_DELETE: {
        if (isRecalculateOperator(&pInfo->basic)) {
          if (!isSemiOperator(&pInfo->basic)) {
            code = doTransformRecalculateWindows(pTaskInfo, &pInfo->interval, pBlock, pInfo->pDelWins);
            QUERY_CHECK_CODE(code, lino, _end);
            if (isFinalOperator(&pInfo->basic)) {
              saveRecalculateData(&pAggSup->stateStore, pInfo->basic.pTsDataState, pBlock, pBlock->info.type);
            }
            continue;
          }
        }

        if (isSemiOperator(&pInfo->basic)) {
          (*ppRes) = pBlock;
          return code;
        } else {
          code = doProcessRecalculateReq(pOperator, pBlock);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        continue;
      } break;
      case STREAM_PULL_OVER: {
        code = processDataPullOver(pBlock, pInfo->nbSup.pPullDataMap, pTaskInfo);
        QUERY_CHECK_CODE(code, lino, _end);
        continue;
      }      
      default: {
        qDebug("===stream===%s ignore recv block. type:%d", GET_TASKID(pTaskInfo), pBlock->info.type);
        continue;
      } break;
    }

    if (pBlock->info.type == STREAM_NORMAL && pBlock->info.version != 0) {
      // set input version
      pTaskInfo->version = pBlock->info.version;
    }

    code = setInputDataBlock(pSup, pBlock, TSDB_ORDER_ASC, MAIN_SCAN, true);
    QUERY_CHECK_CODE(code, lino, _end);

    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.window.ekey);
    code = pInfo->nbSup.pWindowAggFn(pOperator, pBlock);
    if (code == TSDB_CODE_STREAM_INTERNAL_ERROR) {
      pOperator->status = OP_RES_TO_RETURN;
      code = TSDB_CODE_SUCCESS;
    }
    QUERY_CHECK_CODE(code, lino, _end);

    if (pAggSup->pScanBlock->info.rows > 0) {
      (*ppRes) = pAggSup->pScanBlock;
      printDataBlock(pAggSup->pScanBlock, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
      return code;
    }

    if (taosArrayGetSize(pInfo->pUpdated) > 0) {
      break;
    }
  }

  if (pOperator->status == OP_RES_TO_RETURN &&
      (isHistoryOperator(&pInfo->basic) || isRecalculateOperator(&pInfo->basic) || isSemiOperator(&pInfo->basic))) {
    code = copyNewResult(&pAggSup->pResultRows, pInfo->pUpdated, winPosCmprImpl);
    QUERY_CHECK_CODE(code, lino, _end);

    if (isSingleOperator(&pInfo->basic)) {
      if (pAggSup->pCur == NULL) {
        pAggSup->pCur = pAggSup->stateStore.streamStateGetLastStateCur(pAggSup->pState);
      }
      code =
          getHistoryRemainResultInfo(pAggSup, pInfo->nbSup.numOfKeep, pInfo->pUpdated, pOperator->resultInfo.capacity);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  if (pOperator->status == OP_RES_TO_RETURN && pInfo->destHasPrimaryKey && isFinalOperator(&pInfo->basic)) {
    code = closeNonblockIntervalWindow(pAggSup->pResultRows, &pInfo->twAggSup, &pInfo->interval, pInfo->pUpdated,
                                       pTaskInfo);
    QUERY_CHECK_CODE(code, lino, _end);
    if (!isHistoryOperator(&pInfo->basic) && !isRecalculateOperator(&pInfo->basic)) {
      code = checkAndSaveWinStateToDisc(0, pInfo->pUpdated, 0, pInfo->basic.pTsDataState, &pInfo->streamAggSup, &pInfo->interval);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  taosArraySort(pInfo->pUpdated, winPosCmprImpl);
  if (pInfo->nbSup.numOfKeep > 1) {
    taosArrayRemoveDuplicate(pInfo->pUpdated, winPosCmprImpl, releaseFlusedPos);
  }
  if (!isSemiOperator(&pInfo->basic) && !pInfo->destHasPrimaryKey) {
    removeDataDeleteResults(pInfo->pUpdated, pInfo->pDelWins);
  }

  initMultiResInfoFromArrayList(&pInfo->groupResInfo, pInfo->pUpdated);
  pInfo->pUpdated = taosArrayInit(1024, POINTER_BYTES);
  QUERY_CHECK_NULL(pInfo->pUpdated, code, lino, _end, terrno);

  code = blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  code = buildIntervalSliceResult(pOperator, ppRes);
  QUERY_CHECK_CODE(code, lino, _end);
  if ((*ppRes) != NULL) {
    return code;
  }

  return buildOtherResult(pOperator, ppRes);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
    pTaskInfo->code = code;
  }
  return code;
}

int32_t doStreamSemiIntervalNonblockAggImpl(SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  int32_t                           code = TSDB_CODE_SUCCESS;
  int32_t                           lino = 0;
  SStreamIntervalSliceOperatorInfo* pInfo = (SStreamIntervalSliceOperatorInfo*)pOperator->info;
  SStreamAggSupporter*              pAggSup = &pInfo->streamAggSup;
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

  int32_t            startPos = 0;
  TSKEY              curTs = getStartTsKey(&pBlock->info.window, tsCols);
  SInervalSlicePoint curPoint = {0};
  SInervalSlicePoint prevPoint = {0};
  STimeWindow        curWin = getActiveTimeWindow(NULL, pResultRowInfo, curTs, &pInfo->interval, TSDB_ORDER_ASC);
  while (1) {
    int32_t winCode = TSDB_CODE_SUCCESS;
    code = getIntervalSliceCurStateBuf(&pInfo->streamAggSup, &pInfo->interval, pInfo->hasInterpoFunc, &curWin, groupId,
                                       &curPoint, &prevPoint, &winCode);
    QUERY_CHECK_CODE(code, lino, _end);

    if (winCode != TSDB_CODE_SUCCESS) {
      SWinKey key = {.ts = curPoint.winKey.win.skey, .groupId = groupId};
      code = tSimpleHashPut(pAggSup->pResultRows, &key, sizeof(SWinKey), &curPoint.pResPos, POINTER_BYTES);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    code =
        setIntervalSliceOutputBuf(&pInfo->streamAggSup, &curPoint, pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset);
    QUERY_CHECK_CODE(code, lino, _end);

    forwardRows = getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, curWin.ekey, binarySearchForKey, NULL,
                                           TSDB_ORDER_ASC);
    int32_t prevEndPos = (forwardRows - 1) + startPos;

    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &curPoint.winKey.win, 1);
    code = applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, startPos,
                                           forwardRows, pBlock->info.rows, numOfOutput);
    QUERY_CHECK_CODE(code, lino, _end);
    curPoint.pResPos->beUpdated = true;

    if (curPoint.pLastRow->key == curPoint.winKey.win.ekey) {
      setInterpoWindowFinished(&curPoint);
    }

    startPos = getNextQualifiedWindow(&pInfo->interval, &curWin, &pBlock->info, tsCols, prevEndPos, TSDB_ORDER_ASC);
    if (startPos < 0) {
      break;
    }
    curTs = tsCols[startPos];
  }

  if (isHistoryOperator(&pInfo->basic) &&
      tSimpleHashGetSize(pAggSup->pResultRows) > pOperator->resultInfo.capacity * 10) {
    code = copyNewResult(&pAggSup->pResultRows, pInfo->pUpdated, winPosCmprImpl);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

void adjustDownstreamBasicInfo(SOperatorInfo* downstream, struct SSteamOpBasicInfo* pBasic) {
  SExecTaskInfo* pTaskInfo = downstream->pTaskInfo;
  if (downstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_PARTITION) {
    SStreamPartitionOperatorInfo* pPartionInfo = downstream->info;
    pPartionInfo->basic.operatorFlag = pBasic->operatorFlag;
  }
  if (downstream->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    adjustDownstreamBasicInfo(downstream->pDownstream[0], pBasic);
  }
  SStreamScanInfo* pScanInfo = downstream->info;
  pScanInfo->basic.operatorFlag = pBasic->operatorFlag;
}

int32_t createSemiIntervalSliceOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo,
                                            SReadHandle* pHandle, SOperatorInfo** ppOptInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  code = createStreamIntervalSliceOperatorInfo(downstream, pPhyNode, pTaskInfo, pHandle, ppOptInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  SStreamIntervalSliceOperatorInfo* pInfo = (SStreamIntervalSliceOperatorInfo*)(*ppOptInfo)->info;
  pInfo->nbSup.numOfKeep = 0;
  pInfo->nbSup.pWindowAggFn = doStreamSemiIntervalNonblockAggImpl;
  setSemiOperatorFlag(&pInfo->basic);
  adjustDownstreamBasicInfo(downstream, &pInfo->basic);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

bool isDataDeletedStreamWindow(SStreamIntervalSliceOperatorInfo* pInfo, STimeWindow* pWin, uint64_t groupId) {
  SStreamAggSupporter* pAggSup = &pInfo->streamAggSup;
  if (pWin->skey < pInfo->nbSup.tsOfKeep) {
    SWinKey key = {.ts = pWin->skey, .groupId = groupId};
    return !(pAggSup->stateStore.streamStateCheck(pAggSup->pState, &key, isFinalOperator(&pInfo->basic), NULL));
  }
  return false;
}

static int32_t doStreamFinalntervalNonblockAggImpl(SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  int32_t                           code = TSDB_CODE_SUCCESS;
  int32_t                           lino = 0;
  SStreamIntervalSliceOperatorInfo* pInfo = (SStreamIntervalSliceOperatorInfo*)pOperator->info;
  SStreamAggSupporter*              pAggSup = &pInfo->streamAggSup;
  SResultRowInfo*                   pResultRowInfo = &(pInfo->binfo.resultRowInfo);
  SExecTaskInfo*                    pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*                        pSup = &pOperator->exprSupp;
  int32_t                           numOfOutput = pSup->numOfExprs;
  SResultRow*                       pResult = NULL;
  int32_t                           forwardRows = 1;
  SColumnInfoData*                  pColDataInfo = taosArrayGet(pBlock->pDataBlock, pInfo->primaryTsIndex);
  TSKEY*                            tsCols = (int64_t*)pColDataInfo->pData;
  int32_t                           startPos = 0;
  uint64_t                          groupId = pBlock->info.id.groupId;
  SInervalSlicePoint                curPoint = {0};
  SInervalSlicePoint                prevPoint = {0};

  if (pAggSup->pScanBlock->info.rows > 0) {
    blockDataCleanup(pAggSup->pScanBlock);
  }
  pInfo->twAggSup.minTs = TMIN(pInfo->twAggSup.minTs, pBlock->info.window.ekey);

  blockDataEnsureCapacity(pAggSup->pScanBlock, pBlock->info.rows);
  TSKEY       ts = getStartTsKey(&pBlock->info.window, tsCols);
  STimeWindow curWin = getFinalTimeWindow(ts, &pInfo->interval);
  while (startPos >= 0) {
    if (!isHistoryOperator(&pInfo->basic) && isDataDeletedStreamWindow(pInfo, &curWin, groupId)) {
      uint64_t uid = 0;
      code = appendOneRowToSpecialBlockImpl(pAggSup->pScanBlock, &curWin.skey, &curWin.ekey, &curWin.skey, &curWin.skey,
                                            &uid, &groupId, NULL, NULL);
      QUERY_CHECK_CODE(code, lino, _end);

      startPos = getNextQualifiedFinalWindow(&pInfo->interval, &curWin, &pBlock->info, tsCols, startPos);
    } else if (isRecalculateOperator(&pInfo->basic) && !inSlidingWindow(&pInfo->interval, &curWin, &pBlock->info)) {
      startPos = getNextQualifiedFinalWindow(&pInfo->interval, &curWin, &pBlock->info, tsCols, startPos);
    } else {
      break;
    }
  }

  while (startPos >= 0) {
    int32_t winCode = TSDB_CODE_SUCCESS;
    code = getIntervalSliceCurStateBuf(&pInfo->streamAggSup, &pInfo->interval, pInfo->hasInterpoFunc, &curWin, groupId,
                                       &curPoint, &prevPoint, &winCode);
    QUERY_CHECK_CODE(code, lino, _end);

    SWinKey key = {.ts = curPoint.winKey.win.skey, .groupId = groupId};
    if (pInfo->destHasPrimaryKey && winCode == TSDB_CODE_SUCCESS) {
      if (tSimpleHashGet(pAggSup->pResultRows, &key, sizeof(SWinKey)) == NULL) {
        void* pTmp = taosArrayPush(pInfo->pDelWins, &key);
        QUERY_CHECK_NULL(pTmp, code, lino, _end, terrno);
      }
    }

    curPoint.pResPos->beUpdated = true;
    code = tSimpleHashPut(pAggSup->pResultRows, &key, sizeof(SWinKey), &curPoint.pResPos, POINTER_BYTES);
    QUERY_CHECK_CODE(code, lino, _end);

    code =
        setIntervalSliceOutputBuf(&pInfo->streamAggSup, &curPoint, pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset);
    QUERY_CHECK_CODE(code, lino, _end);
    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &curWin, 1);
    code = applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, startPos,
                                           forwardRows, pBlock->info.rows, numOfOutput);
    QUERY_CHECK_CODE(code, lino, _end);

    int32_t prevEndPos = startPos;
    startPos = getNextQualifiedFinalWindow(&pInfo->interval, &curWin, &pBlock->info, tsCols, prevEndPos);
  }

  if (!pInfo->destHasPrimaryKey && !isHistoryOperator(&pInfo->basic)) {
    code = closeNonblockIntervalWindow(pAggSup->pResultRows, &pInfo->twAggSup, &pInfo->interval, pInfo->pUpdated,
                                       pTaskInfo);
    QUERY_CHECK_CODE(code, lino, _end);
  } else if ((isHistoryOperator(&pInfo->basic) || isRecalculateOperator(&pInfo->basic)) &&
             tSimpleHashGetSize(pAggSup->pResultRows) > pOperator->resultInfo.capacity * 10) {
    code = copyNewResult(&pAggSup->pResultRows, pInfo->pUpdated, winPosCmprImpl);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (!isHistoryOperator(&pInfo->basic) && !isRecalculateOperator(&pInfo->basic)) {
    code = checkAndSaveWinStateToDisc(0, pInfo->pUpdated, 0, pInfo->basic.pTsDataState, &pInfo->streamAggSup, &pInfo->interval);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

int32_t createFinalIntervalSliceOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo,
                                             SReadHandle* pHandle, SOperatorInfo** ppOptInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  code = createStreamIntervalSliceOperatorInfo(downstream, pPhyNode, pTaskInfo, pHandle, ppOptInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  SStreamIntervalSliceOperatorInfo* pInfo = (SStreamIntervalSliceOperatorInfo*)(*ppOptInfo)->info;
  pInfo->nbSup.pWindowAggFn = doStreamFinalntervalNonblockAggImpl;
  pInfo->nbSup.numOfChild = pHandle->numOfVgroups;
  pInfo->streamAggSup.pScanBlock->info.type = STREAM_RETRIEVE;
  pInfo->nbSup.tsOfKeep = INT64_MIN;
  pInfo->twAggSup.waterMark = 0;
  setFinalOperatorFlag(&pInfo->basic);
  adjustDownstreamBasicInfo(downstream, &pInfo->basic);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}
