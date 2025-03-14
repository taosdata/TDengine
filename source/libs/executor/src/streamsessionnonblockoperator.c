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
#include "streamsession.h"
#include "tchecksum.h"
#include "tcommon.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "tlog.h"
#include "ttime.h"

#define STREAM_SESSION_NONBLOCK_OP_STATE_NAME "StreamSessionNonblockHistoryState"

void streamSessionNonblockReleaseState(SOperatorInfo* pOperator) {
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  SStreamAggSupporter*           pAggSup = &pInfo->streamAggSup;
  pAggSup->stateStore.streamStateClearExpiredSessionState(pAggSup->pState, pInfo->nbSup.numOfKeep,
                                                          pInfo->nbSup.tsOfKeep, NULL);
  streamSessionReleaseState(pOperator);
  qDebug("%s===stream===streamSessionNonblockReleaseState:%" PRId64, GET_TASKID(pOperator->pTaskInfo),
         pInfo->twAggSup.maxTs);
}

void streamSessionNonblockReloadState(SOperatorInfo* pOperator) {
  int32_t                        code = TSDB_CODE_SUCCESS;
  int32_t                        lino = 0;
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  SStreamAggSupporter*           pAggSup = &pInfo->streamAggSup;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  int32_t                        size = 0;
  void*                          pBuf = NULL;

  resetWinRange(&pAggSup->winRange);
  code = pAggSup->stateStore.streamStateGetInfo(pAggSup->pState, STREAM_SESSION_NONBLOCK_OP_STATE_NAME,
                                                strlen(STREAM_SESSION_NONBLOCK_OP_STATE_NAME), &pBuf, &size);
  QUERY_CHECK_CODE(code, lino, _end);

  int32_t      num = (size - sizeof(TSKEY)) / sizeof(SSessionKey);
  SSessionKey* pSeKeyBuf = (SSessionKey*)pBuf;

  TSKEY ts = *(TSKEY*)((char*)pBuf + size - sizeof(TSKEY));
  pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, ts);
  pAggSup->stateStore.streamStateReloadInfo(pAggSup->pState, ts);

  for (int32_t i = 0; i < num; i++) {
    SResultWindowInfo winInfo = {0};
    code = getSessionWindowInfoByKey(pAggSup, pSeKeyBuf + i, &winInfo);
    QUERY_CHECK_CODE(code, lino, _end);
    if (!IS_VALID_SESSION_WIN(winInfo)) {
      continue;
    }

    int32_t winNum = 0;
    bool    isEnd = false;
    code = compactSessionWindow(pOperator, &winInfo, pInfo->pStUpdated, pInfo->basic.pSeDeleted, true, &winNum, &isEnd);
    QUERY_CHECK_CODE(code, lino, _end);

    if (winNum > 0) {
      if (isEnd) {
        code = saveDeleteRes(pInfo->basic.pSeDeleted, winInfo.sessionWin);
        QUERY_CHECK_CODE(code, lino, _end);
        qDebug("===stream=== reload state. save delete result %" PRId64 ", %" PRIu64, winInfo.sessionWin.win.skey,
               winInfo.sessionWin.groupId);
      } else {
        void* pResPtr = taosArrayPush(pInfo->basic.pUpdated, &winInfo);
        QUERY_CHECK_NULL(pResPtr, code, lino, _end, terrno);
        reuseOutputBuf(pAggSup->pState, winInfo.pStatePos, &pAggSup->stateStore);
        qDebug("===stream=== reload state. save result %" PRId64 ", %" PRIu64, winInfo.sessionWin.win.skey,
               winInfo.sessionWin.groupId);
      }
    }
    code = saveSessionOutputBuf(pAggSup, &winInfo);
    QUERY_CHECK_CODE(code, lino, _end);
  }
  taosMemoryFree(pBuf);

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (downstream->fpSet.reloadStreamStateFn) {
    downstream->fpSet.reloadStreamStateFn(downstream);
  }
  qDebug("%s===stream===streamSessionNonblockReloadState", GET_TASKID(pOperator->pTaskInfo));

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
}

int32_t doStreamSessionNonblockAggImpl(SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  int32_t                        code = TSDB_CODE_SUCCESS;
  int32_t                        lino = 0;
  SStreamSessionAggOperatorInfo* pInfo = (SStreamSessionAggOperatorInfo*)pOperator->info;
  SResultRowInfo*                pResultRowInfo = &(pInfo->binfo.resultRowInfo);
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  SStreamAggSupporter*           pAggSup = &pInfo->streamAggSup;
  SResultRow*                    pResult = NULL;
  int64_t                        groupId = pBlock->info.id.groupId;
  int64_t                        rows = pBlock->info.rows;
  int32_t                        winRows = 0;
  int32_t                        numOfOutput = pOperator->exprSupp.numOfExprs;
  SColumnInfoData*               pStartTsCol = taosArrayGet(pBlock->pDataBlock, pInfo->primaryTsIndex);
  TSKEY*                         startTsCols = (int64_t*)pStartTsCol->pData;

  pAggSup->winRange = pTaskInfo->streamInfo.fillHistoryWindow;
  if (pAggSup->winRange.ekey <= 0) {
    pAggSup->winRange.ekey = INT64_MAX;
  }

  if (pAggSup->winRange.skey != INT64_MIN && pInfo->nbSup.pHistoryGroup == NULL) {
    _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
    pInfo->nbSup.pHistoryGroup = tSimpleHashInit(1024, hashFn);
  }

  for (int32_t i = 0; i < rows;) {
    SResultWindowInfo curWinInfo = {0};
    int32_t           winCode = TSDB_CODE_SUCCESS;
    code = setSessionOutputBuf(pAggSup, startTsCols[i], startTsCols[i], groupId, &curWinInfo, &winCode);
    QUERY_CHECK_CODE(code, lino, _end);

    if (winCode != TSDB_CODE_SUCCESS) {
      SStreamStateCur* pCur =
          pAggSup->stateStore.streamStateSessionSeekKeyPrev(pAggSup->pState, &curWinInfo.sessionWin);
      int32_t           size = 0;
      SResultWindowInfo prevWinInfo = {.sessionWin.groupId = groupId};
      int32_t           tmpWinCode = pAggSup->stateStore.streamStateSessionGetKVByCur(pCur, &prevWinInfo.sessionWin,
                                                                                      (void**)&prevWinInfo.pStatePos, &size);
      if (tmpWinCode == TSDB_CODE_SUCCESS) {
        void* pResPtr = taosArrayPush(pInfo->basic.pUpdated, &prevWinInfo);
        QUERY_CHECK_NULL(pResPtr, code, lino, _end, terrno);
        reuseOutputBuf(pAggSup->pState, prevWinInfo.pStatePos, &pAggSup->stateStore);
        int32_t mode = 0;
        int32_t winRes = pAggSup->stateStore.streamStateGetRecFlag(pAggSup->pState, &prevWinInfo.sessionWin,
                                                                   sizeof(SSessionKey), &mode);
        if (winRes == TSDB_CODE_SUCCESS) {
          code = saveRecWindowToDisc(&prevWinInfo.sessionWin, pBlock->info.id.uid, mode, pInfo->basic.pTsDataState,
                                     pAggSup);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
    }

    code = updateSessionWindowInfo(pAggSup, &curWinInfo, startTsCols, startTsCols, groupId, rows, i, pAggSup->gap,
                                   pAggSup->pResultRows, NULL, pInfo->basic.pSeDeleted, &winRows);
    QUERY_CHECK_CODE(code, lino, _end);

    code = doOneWindowAggImpl(&pInfo->twAggSup.timeWindowData, &curWinInfo, &pResult, i, winRows, rows, numOfOutput,
                              pOperator, pAggSup->gap);
    QUERY_CHECK_CODE(code, lino, _end);

    code = saveSessionOutputBuf(pAggSup, &curWinInfo);
    QUERY_CHECK_CODE(code, lino, _end);
    releaseOutputBuf(pAggSup->pState, curWinInfo.pStatePos, &pAggSup->stateStore);

    i += winRows;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

int32_t getSessionHistoryRemainResultInfo(SStreamAggSupporter* pAggSup, int32_t numOfState, SArray* pUpdated,
                                          int32_t capacity) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t winCode = TSDB_CODE_SUCCESS;

  if (pAggSup->pCur == NULL) {
    goto _end;
  }

  int32_t num = capacity - taosArrayGetSize(pUpdated);
  for (int32_t i = 0; i < num; i++) {
    winCode = pAggSup->stateStore.streamStateNLastSessionStateGetKVByCur(pAggSup->pCur, numOfState, pUpdated);
    if (winCode == TSDB_CODE_FAILED) {
      pAggSup->stateStore.streamStateFreeCur(pAggSup->pCur);
      pAggSup->pCur = NULL;
      break;
    }

    pAggSup->stateStore.streamStateLastSessionStateCurNext(pAggSup->pCur);
    num = capacity - taosArrayGetSize(pUpdated);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s.", __func__, lino, tstrerror(code));
  }
  return code;
}

void releaseSessionFlusedPos(void* pRes) {
  SResultWindowInfo* pWinInfo = (SResultWindowInfo*)pRes;
  SRowBuffPos* pPos = pWinInfo->pStatePos;
  if (pPos != NULL && pPos->needFree) {
    pPos->beUsed = false;
  }
}

int32_t buildSessionHistoryResult(SOperatorInfo* pOperator, SOptrBasicInfo* pBinfo, SSteamOpBasicInfo* pBasic,
                                  SStreamAggSupporter* pAggSup, SNonBlockAggSupporter* pNbSup,
                                  SGroupResInfo* pGroupResInfo) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;
  SStreamNotifyEventSupp* pNotifySup = &pBasic->notifyEventSup;
  bool                    addNotifyEvent = false;

  code =
      getSessionHistoryRemainResultInfo(pAggSup, pNbSup->numOfKeep, pBasic->pUpdated, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);
  if (taosArrayGetSize(pBasic->pUpdated) > 0) {
    taosArraySort(pBasic->pUpdated, sessionKeyCompareAsc);
    if (pNbSup->numOfKeep > 1) {
      taosArrayRemoveDuplicate(pBasic->pUpdated, sessionKeyCompareAsc, releaseSessionFlusedPos);
    }
    initGroupResInfoFromArrayList(pGroupResInfo, pBasic->pUpdated);
    pBasic->pUpdated = taosArrayInit(1024, sizeof(SResultWindowInfo));
    QUERY_CHECK_NULL(pBasic->pUpdated, code, lino, _end, terrno);

    doBuildSessionResult(pOperator, pAggSup->pState, pGroupResInfo, pBinfo->pRes, addNotifyEvent ? pNotifySup->pSessionKeys : NULL);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s.", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doDeleteSessionRecalculateWindows(SExecTaskInfo* pTaskInfo, SSDataBlock* pBlock, SSHashObj* pDeleteMap) {
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
    SSessionKey winRes = {.win.skey = startTsCols[i], .win.ekey = endTsCols[i], .groupId = pGpDatas[i]};
    code = tSimpleHashPut(pDeleteMap, &winRes, sizeof(SSessionKey), NULL, 0);
    QUERY_CHECK_CODE(code, lino, _end);
  }
_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

static int32_t buildOtherResult(SOperatorInfo* pOperator, SOptrBasicInfo* pBinfo, SSteamOpBasicInfo* pBasic,
                                SStreamAggSupporter* pAggSup, STimeWindowAggSupp* pTwAggSup,
                                SNonBlockAggSupporter* pNbSup, SGroupResInfo* pGroupResInfo, SSDataBlock** ppRes) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  if (isHistoryOperator(pBasic) && isSingleOperator(pBasic)) {
    code = buildSessionHistoryResult(pOperator, pBinfo, pBasic, pAggSup, pNbSup, pGroupResInfo);
    QUERY_CHECK_CODE(code, lino, _end);
    if (pBinfo->pRes->info.rows != 0) {
      printDataBlock(pBinfo->pRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
      (*ppRes) = pBinfo->pRes;
      return code;
    }
  }

  if (pBasic->recvCkBlock) {
    pBasic->recvCkBlock = false;
    printDataBlock(pBasic->pCheckpointRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
    (*ppRes) = pBasic->pCheckpointRes;
    return code;
  }

  if (pTwAggSup->minTs != INT64_MAX) {
    pNbSup->tsOfKeep = pTwAggSup->minTs;
  }

  if (!isHistoryOperator(pBasic) || !isFinalOperator(pBasic)) {
    int32_t numOfKeep = 0;
    TSKEY tsOfKeep = INT64_MAX;
    getStateKeepInfo(pNbSup, isRecalculateOperator(pBasic), &numOfKeep, &tsOfKeep);
    pAggSup->stateStore.streamStateClearExpiredSessionState(pAggSup->pState, numOfKeep, tsOfKeep,
                                                            pNbSup->pHistoryGroup);
  }
  pTwAggSup->minTs = INT64_MAX;
  setStreamOperatorCompleted(pOperator);
  if (isFinalOperator(pBasic) && isRecalculateOperator(pBasic) && tSimpleHashGetSize(pNbSup->pPullDataMap) == 0) {
    qDebug("===stream===%s recalculate is finished.", GET_TASKID(pTaskInfo));
    pTaskInfo->streamInfo.recoverScanFinished = true;
  }
  (*ppRes) = NULL;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

static int32_t closeNonBlockSessionWindow(SSHashObj* pHashMap, STimeWindowAggSupp* pTwSup, SArray* pUpdated,
                                          SExecTaskInfo* pTaskInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pHashMap, pIte, &iter)) != NULL) {
    void*        key = tSimpleHashGetKey(pIte, NULL);
    SSessionKey* pWinKey = (SSessionKey*)key;

    if (isCloseWindow(&pWinKey->win, pTwSup)) {
      void* pTemp = taosArrayPush(pUpdated, pIte);
      QUERY_CHECK_NULL(pTemp, code, lino, _end, terrno);

      int32_t tmpRes = tSimpleHashIterateRemove(pHashMap, pWinKey, sizeof(SSessionKey), &pIte, &iter);
      qTrace("%s at line %d res:%d", __func__, __LINE__, tmpRes);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

int32_t buildNonBlockSessionResult(SOperatorInfo* pOperator, SStreamAggSupporter* pAggSup, SOptrBasicInfo* pBInfo,
                                   SSteamOpBasicInfo* pBasic, SGroupResInfo* pGroupResInfo, SNonBlockAggSupporter* pNbSup, SSDataBlock** ppRes) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  SExecTaskInfo*          pTaskInfo = pOperator->pTaskInfo;
  SStreamNotifyEventSupp* pNotifySup = &pBasic->notifyEventSup;
  bool                    addNotifyEvent = false;
  addNotifyEvent = BIT_FLAG_TEST_MASK(pTaskInfo->streamInfo.eventTypes, SNOTIFY_EVENT_WINDOW_CLOSE);
  if (isFinalOperator(pBasic)) {
    doBuildPullDataBlock(pNbSup->pPullWins, &pNbSup->pullIndex, pNbSup->pPullDataRes);
    if (pNbSup->pPullDataRes->info.rows != 0) {
      printDataBlock(pNbSup->pPullDataRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
      (*ppRes) = pNbSup->pPullDataRes;
      return code;
    }
  }

  SSDataBlock*   pDelRes = pBasic->pDelRes;
  doBuildDeleteDataBlock(pOperator, pBasic->pSeDeleted, pDelRes, &pBasic->pDelIterator, pGroupResInfo);
  if (pDelRes->info.rows > 0) {
    printDataBlock(pDelRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
    (*ppRes) = pDelRes;
    return code;
  }

  doBuildSessionResult(pOperator, pAggSup->pState, pGroupResInfo, pBInfo->pRes, addNotifyEvent ? pNotifySup->pSessionKeys : NULL);
  if (pBInfo->pRes->info.rows > 0) {
    printDataBlock(pBInfo->pRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
    (*ppRes) = pBInfo->pRes;
    return code;
  }
  (*ppRes) = NULL;
  return code;
}

static int32_t doSetSessionWindowRecFlag(SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  int32_t                        code = TSDB_CODE_SUCCESS;
  int32_t                        lino = 0;
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  SStreamAggSupporter*           pAggSup = &pInfo->streamAggSup;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  SColumnInfoData*               pStartTsCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  TSKEY*                         startTsCols = (TSKEY*)pStartTsCol->pData;
  SColumnInfoData*               pEndTsCol = taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
  TSKEY*                         endTsCols = (TSKEY*)pEndTsCol->pData;
  SColumnInfoData*               pGpCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  uint64_t*                      pGpDatas = (uint64_t*)pGpCol->pData;
  SColumnInfoData*               pUidCol = taosArrayGet(pBlock->pDataBlock, UID_COLUMN_INDEX);
  uint64_t*                      pUidDatas = (uint64_t*)pUidCol->pData;
  for (int32_t i = 0; i < pBlock->info.rows; i++) {
    SSessionKey key = {.win.skey = startTsCols[i], .win.ekey = endTsCols[i], .groupId = pGpDatas[i]};
    bool isLastWin = false;
    if (pAggSup->stateStore.streamStateCheckSessionState(pAggSup->pState, &key, pAggSup->gap, &isLastWin)) {
      qDebug("===stream===%s set recalculate flag start ts:%" PRId64 ",end ts:%" PRId64 ", group id:%" PRIu64,
             GET_TASKID(pTaskInfo), key.win.skey, key.win.ekey, key.groupId);
      pAggSup->stateStore.streamStateSetRecFlag(pAggSup->pState, &key, sizeof(SSessionKey), pBlock->info.type);
      if ((isFinalOperator(&pInfo->basic) && isCloseWindow(&key.win, &pInfo->twAggSup)) ||
          (isSingleOperator(&pInfo->basic) && isLastWin == false)) {
        code = saveRecWindowToDisc(&key, pUidDatas[i], pBlock->info.type, pInfo->basic.pTsDataState,
                                   &pInfo->streamAggSup);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    } else {
      code = saveRecWindowToDisc(&key, pUidDatas[i], pBlock->info.type, pInfo->basic.pTsDataState,
                                 &pInfo->streamAggSup);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }
_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

static int32_t checkAndSaveSessionStateToDisc(int32_t startIndex, SArray* pUpdated, uint64_t uid, STableTsDataState* pTsDataState,
                                              SStreamAggSupporter* pAggSup) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t mode = 0;
  int32_t size = taosArrayGetSize(pUpdated);
  for (int32_t i = startIndex; i < size; i++) {
    SResultWindowInfo* pWinInfo = taosArrayGet(pUpdated, i);
    SSessionKey*       pKey = &pWinInfo->sessionWin;
    int32_t      winRes = pAggSup->stateStore.streamStateGetRecFlag(pAggSup->pState, pKey, sizeof(SSessionKey), &mode);
    if (winRes == TSDB_CODE_SUCCESS) {
      code = saveRecWindowToDisc(pKey, uid, mode, pTsDataState, pAggSup);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s.", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t doStreamSessionNonblockAggNextImpl(SOperatorInfo* pOperator, SOptrBasicInfo* pBInfo, SSteamOpBasicInfo* pBasic,
                                           SStreamAggSupporter* pAggSup, STimeWindowAggSupp* pTwAggSup,
                                           SGroupResInfo* pGroupResInfo, SNonBlockAggSupporter* pNbSup,
                                           SExprSupp* pScalarSupp, SArray* pHistoryWins, SSDataBlock** ppRes) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*   pAPI = &pOperator->pTaskInfo->storageAPI;
  SExprSupp*     pSup = &pOperator->exprSupp;

  qDebug("stask:%s  %s status: %d", GET_TASKID(pTaskInfo), getStreamOpName(pOperator->operatorType), pOperator->status);

  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return code;
  }

  code = buildNonBlockSessionResult(pOperator, pAggSup, pBInfo, pBasic, pGroupResInfo, pNbSup, ppRes);
  QUERY_CHECK_CODE(code, lino, _end);
  if ((*ppRes) != NULL) {
    return code;
  }

  if (isHistoryOperator(pBasic) && !isFinalOperator(pBasic)) {
    int32_t numOfKeep = 0;
    TSKEY tsOfKeep = INT64_MAX;
    getStateKeepInfo(pNbSup, isRecalculateOperator(pBasic), &numOfKeep, &tsOfKeep);
    pAggSup->stateStore.streamStateClearExpiredSessionState(pAggSup->pState, pNbSup->numOfKeep, pNbSup->tsOfKeep,
                                                            pNbSup->pHistoryGroup);
  }

  if (pOperator->status == OP_RES_TO_RETURN) {
    return buildOtherResult(pOperator, pBInfo, pBasic, pAggSup, pTwAggSup, pNbSup, pGroupResInfo, ppRes);
  }

  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while (1) {
    if (isTaskKilled(pTaskInfo)) {
      qInfo("===stream=== %s task is killed, code %s", GET_TASKID(pTaskInfo), tstrerror(pTaskInfo->code));
      (*ppRes) = NULL;
      return code;
    }
    SSDataBlock* pBlock = NULL;
    code = downstream->fpSet.getNextFn(downstream, &pBlock);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pBlock == NULL) {
      qDebug("===stream===return data:%s.", getStreamOpName(pOperator->operatorType));
      if (isFinalOperator(pBasic) && isRecalculateOperator(pBasic)) {
        code = buildRetriveRequest(pTaskInfo, pAggSup, pBasic->pTsDataState, pNbSup);
      }
      pOperator->status = OP_RES_TO_RETURN;
      break;
    }

    printSpecDataBlock(pBlock, getStreamOpName(pOperator->operatorType), "recv", GET_TASKID(pTaskInfo));
    setStreamOperatorState(pBasic, pBlock->info.type);

    switch (pBlock->info.type) {
      case STREAM_NORMAL:
      case STREAM_INVALID: 
      case STREAM_PULL_DATA: {
        SExprSupp* pExprSup = pScalarSupp;
        if (pExprSup->pExprInfo != NULL) {
          code = projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      } break;
      case STREAM_CHECKPOINT: {
        pBasic->recvCkBlock = true;
        pAggSup->stateStore.streamStateCommit(pAggSup->pState);
        code = copyDataBlock(pBasic->pCheckpointRes, pBlock);
        QUERY_CHECK_CODE(code, lino, _end);
        continue;
      } break;
      case STREAM_CREATE_CHILD_TABLE:
      case STREAM_DROP_CHILD_TABLE: {
        (*ppRes) = pBlock;
        return code;
      } break;
      case STREAM_RECALCULATE_DATA:
      case STREAM_RECALCULATE_DELETE: {
        if (isRecalculateOperator(pBasic)) {
          if (!isSemiOperator(pBasic)) {
            code = doDeleteSessionRecalculateWindows(pTaskInfo, pBlock, pBasic->pSeDeleted);
            QUERY_CHECK_CODE(code, lino, _end);
            if (isFinalOperator(pBasic)) {
              saveRecalculateData(&pAggSup->stateStore, pBasic->pTsDataState, pBlock, pBlock->info.type);
            }
            continue;
          }
        }
        
        if (isSemiOperator(pBasic)) {
          (*ppRes) = pBlock;
          return code;
        } else {
          code = doSetSessionWindowRecFlag(pOperator, pBlock);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        continue;
      } break;
      case STREAM_PULL_OVER: {
        code = processDataPullOver(pBlock, pNbSup->pPullDataMap, pTaskInfo);
        QUERY_CHECK_CODE(code, lino, _end);
        continue;
      } break; 
      default:
        qDebug("===stream===%s ignore recv block. type:%d", GET_TASKID(pTaskInfo), pBlock->info.type);
        continue;
    }

    if (pBlock->info.type == STREAM_NORMAL && pBlock->info.version != 0) {
      // set input version
      pTaskInfo->version = pBlock->info.version;
    }

    code = setInputDataBlock(pSup, pBlock, TSDB_ORDER_ASC, MAIN_SCAN, true);
    QUERY_CHECK_CODE(code, lino, _end);

    pTwAggSup->maxTs = TMAX(pTwAggSup->maxTs, pBlock->info.window.ekey);
    code = pNbSup->pWindowAggFn(pOperator, pBlock);
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

    if (taosArrayGetSize(pBasic->pUpdated) > 0) {
      break;
    }
  }

  if (pOperator->status == OP_RES_TO_RETURN &&
      (isHistoryOperator(pBasic) || isRecalculateOperator(pBasic) || isSemiOperator(pBasic))) {
    code = copyNewResult(&pAggSup->pResultRows, pBasic->pUpdated, sessionKeyCompareAsc);
    QUERY_CHECK_CODE(code, lino, _end);

    if (isSingleOperator(pBasic)) {
      if (pAggSup->pCur == NULL) {
        pAggSup->pCur = pAggSup->stateStore.streamStateGetLastSessionStateCur(pAggSup->pState);
      }
      code = getSessionHistoryRemainResultInfo(pAggSup, pNbSup->numOfKeep, pBasic->pUpdated,
                                               pOperator->resultInfo.capacity);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  if (pOperator->status == OP_RES_TO_RETURN && pBasic->destHasPrimaryKey && isFinalOperator(pBasic)) {
    code = closeNonBlockSessionWindow(pAggSup->pResultRows, pTwAggSup, pBasic->pUpdated, pTaskInfo);
    QUERY_CHECK_CODE(code, lino, _end);
    if (!isHistoryOperator(pBasic)) {
      checkAndSaveSessionStateToDisc(0, pBasic->pUpdated, 0, pBasic->pTsDataState, pAggSup);
    }
  }

  taosArraySort(pBasic->pUpdated, sessionKeyCompareAsc);
  if (pNbSup->numOfKeep > 1) {
    taosArrayRemoveDuplicate(pBasic->pUpdated, sessionKeyCompareAsc, releaseSessionFlusedPos);
  }
  if (!isSemiOperator(pBasic) && !pBasic->destHasPrimaryKey) {
    removeSessionDeleteResults(pBasic->pSeDeleted, pBasic->pUpdated);
  }

  if (isHistoryOperator(pBasic)) {
    code = getMaxTsWins(pBasic->pUpdated, pHistoryWins);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  initGroupResInfoFromArrayList(pGroupResInfo, pBasic->pUpdated);
  pBasic->pUpdated = taosArrayInit(1024, sizeof(SResultWindowInfo));
  QUERY_CHECK_NULL(pBasic->pUpdated, code, lino, _end, terrno);

  code = blockDataEnsureCapacity(pBInfo->pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  code = buildNonBlockSessionResult(pOperator, pAggSup, pBInfo, pBasic, pGroupResInfo, pNbSup, ppRes);
  QUERY_CHECK_CODE(code, lino, _end);
  if ((*ppRes) != NULL) {
    return code;
  }

  return buildOtherResult(pOperator, pBInfo, pBasic, pAggSup, pTwAggSup, pNbSup, pGroupResInfo, ppRes);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
    pTaskInfo->code = code;
  }
  return code;
}

int32_t doStreamSessionNonblockAggNext(SOperatorInfo* pOperator, SSDataBlock** ppBlock) {
  SStreamSessionAggOperatorInfo* pInfo = (SStreamSessionAggOperatorInfo*)pOperator->info;
  return doStreamSessionNonblockAggNextImpl(pOperator, &pInfo->binfo, &pInfo->basic, &pInfo->streamAggSup,
                                            &pInfo->twAggSup, &pInfo->groupResInfo, &pInfo->nbSup, &pInfo->scalarSupp,
                                            pInfo->historyWins, ppBlock);
}

int32_t doStreamSemiSessionNonblockAggImpl(SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  int32_t                        code = TSDB_CODE_SUCCESS;
  int32_t                        lino = 0;
  SStreamSessionAggOperatorInfo* pInfo = (SStreamSessionAggOperatorInfo*)pOperator->info;
  SStreamAggSupporter*           pAggSup = &pInfo->streamAggSup;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*                     pSup = &pOperator->exprSupp;
  int32_t                        numOfOutput = pSup->numOfExprs;
  int64_t                        groupId = pBlock->info.id.groupId;
  SResultRow*                    pResult = NULL;
  int64_t                        rows = pBlock->info.rows;
  int32_t                        winRows = 0;
  SColumnInfoData*               pStartTsCol = taosArrayGet(pBlock->pDataBlock, pInfo->primaryTsIndex);
  TSKEY*                         startTsCols = (int64_t*)pStartTsCol->pData;

  pAggSup->winRange = pTaskInfo->streamInfo.fillHistoryWindow;
  if (pAggSup->winRange.ekey <= 0) {
    pAggSup->winRange.ekey = INT64_MAX;
  }

  for (int64_t i = 0; i < rows;) {
    SResultWindowInfo curWinInfo = {0};
    int32_t           winCode = TSDB_CODE_SUCCESS;
    code = setSessionOutputBuf(pAggSup, startTsCols[i], startTsCols[i], groupId, &curWinInfo, &winCode);
    QUERY_CHECK_CODE(code, lino, _end);

    if (winCode != TSDB_CODE_SUCCESS) {
      code = tSimpleHashPut(pAggSup->pResultRows, &curWinInfo.sessionWin, sizeof(SSessionKey), &curWinInfo,
                            sizeof(SResultWindowInfo));
      QUERY_CHECK_CODE(code, lino, _end);
    }

    code = updateSessionWindowInfo(pAggSup, &curWinInfo, startTsCols, startTsCols, groupId, rows, i, pAggSup->gap,
                                   pAggSup->pResultRows, NULL, NULL, &winRows);
    QUERY_CHECK_CODE(code, lino, _end);

    code = doOneWindowAggImpl(&pInfo->twAggSup.timeWindowData, &curWinInfo, &pResult, i, winRows, rows, numOfOutput,
                              pOperator, 0);
    QUERY_CHECK_CODE(code, lino, _end);

    code = saveSessionOutputBuf(pAggSup, &curWinInfo);
    QUERY_CHECK_CODE(code, lino, _end);

    i += winRows;
  }

  if (isHistoryOperator(&pInfo->basic) &&
      tSimpleHashGetSize(pAggSup->pResultRows) > pOperator->resultInfo.capacity * 10) {
    code = copyNewResult(&pAggSup->pResultRows, pInfo->basic.pUpdated, sessionKeyCompareAsc);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

int32_t createSessionNonblockOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo,
                                          SReadHandle* pHandle, SOperatorInfo** ppOptInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  code = createStreamSessionAggOperatorInfo(downstream, pPhyNode, pTaskInfo, pHandle, ppOptInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  SStreamSessionAggOperatorInfo* pInfo = (SStreamSessionAggOperatorInfo*)(*ppOptInfo)->info;
  pInfo->nbSup.numOfKeep = 1;
  pInfo->nbSup.pWindowAggFn = doStreamSessionNonblockAggImpl;
  setSingleOperatorFlag(&pInfo->basic);
  adjustDownstreamBasicInfo(downstream, &pInfo->basic);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

int32_t createSemiSessionNonblockOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo,
                                              SReadHandle* pHandle, SOperatorInfo** ppOptInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  code = createStreamSessionAggOperatorInfo(downstream, pPhyNode, pTaskInfo, pHandle, ppOptInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  SStreamSessionAggOperatorInfo* pInfo = (SStreamSessionAggOperatorInfo*)(*ppOptInfo)->info;
  pInfo->nbSup.numOfKeep = 0;
  pInfo->nbSup.pWindowAggFn = doStreamSemiSessionNonblockAggImpl;
  setSemiOperatorFlag(&pInfo->basic);
  adjustDownstreamBasicInfo(downstream, &pInfo->basic);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

bool isDataDeletedSessionWindow(SStreamAggSupporter* pAggSup, SNonBlockAggSupporter* pNbSup, TSKEY startTs, TSKEY endTs,
                                uint64_t groupId, TSKEY gap) {
  if (startTs < pNbSup->tsOfKeep) {
    SSessionKey key = {.win.skey = startTs, .win.ekey = endTs, .groupId = groupId};
    return !(pAggSup->stateStore.streamStateCheckSessionState(pAggSup->pState, &key, gap, NULL));
  }
  return false;
}

static int32_t doStreamFinalSessionNonblockAggImpl(SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  int32_t                        code = TSDB_CODE_SUCCESS;
  int32_t                        lino = 0;
  SStreamSessionAggOperatorInfo* pInfo = (SStreamSessionAggOperatorInfo*)pOperator->info;
  SStreamAggSupporter*           pAggSup = &pInfo->streamAggSup;
  SResultRowInfo*                pResultRowInfo = &(pInfo->binfo.resultRowInfo);
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*                     pSup = &pOperator->exprSupp;
  int32_t                        numOfOutput = pSup->numOfExprs;
  SResultRow*                    pResult = NULL;
  uint64_t                       groupId = pBlock->info.id.groupId;
  int64_t                        rows = pBlock->info.rows;
  int32_t                        winRows = 0;
  SColumnInfoData*               pStartTsCol = taosArrayGet(pBlock->pDataBlock, pInfo->primaryTsIndex);
  TSKEY*                         startTsCols = (int64_t*)pStartTsCol->pData;
  SColumnInfoData*               pEndTsCol = taosArrayGet(pBlock->pDataBlock, pInfo->endTsIndex);
  TSKEY*                         endTsCols = (int64_t*)pEndTsCol->pData;
  int32_t                        startPos = 0;

  if (pAggSup->pScanBlock->info.rows > 0) {
    blockDataCleanup(pAggSup->pScanBlock);
  }
  pInfo->twAggSup.minTs = TMIN(pInfo->twAggSup.minTs, pBlock->info.window.ekey);

  pAggSup->winRange = pTaskInfo->streamInfo.fillHistoryWindow;
  if (pAggSup->winRange.ekey <= 0) {
    pAggSup->winRange.ekey = INT64_MAX;
  }

  for (int32_t i = 0; i < rows; i++) {
    if (!isHistoryOperator(&pInfo->basic) &&
        isDataDeletedSessionWindow(pAggSup, &pInfo->nbSup, startTsCols[i], endTsCols[i], groupId, pAggSup->gap)) {
      uint64_t uid = 0;
      code = appendOneRowToSpecialBlockImpl(pAggSup->pScanBlock, startTsCols + i, endTsCols + i, startTsCols + i, startTsCols + i,
                                            &uid, &groupId, NULL, NULL);
      QUERY_CHECK_CODE(code, lino, _end);
      continue;
    }
    startPos = i;
    break;
  }

  for (int32_t i = startPos; i < rows;) {
    SResultWindowInfo curWinInfo = {0};
    int32_t           winCode = TSDB_CODE_SUCCESS;
    code = setSessionOutputBuf(pAggSup, startTsCols[i], endTsCols[i], groupId, &curWinInfo, &winCode);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pInfo->destHasPrimaryKey && winCode == TSDB_CODE_SUCCESS) {
      if (tSimpleHashGet(pAggSup->pResultRows, &curWinInfo.sessionWin, sizeof(SSessionKey)) == NULL) {
        code = saveDeleteRes(pInfo->basic.pSeDeleted, curWinInfo.sessionWin);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }

    curWinInfo.pStatePos->beUpdated = true;
    code = tSimpleHashPut(pAggSup->pResultRows, &curWinInfo.sessionWin, sizeof(SSessionKey), &curWinInfo,
                          sizeof(SResultWindowInfo));
    QUERY_CHECK_CODE(code, lino, _end);

    code = updateSessionWindowInfo(pAggSup, &curWinInfo, startTsCols, endTsCols, groupId, rows, i, pAggSup->gap,
                                   pAggSup->pResultRows, NULL, pInfo->basic.pSeDeleted, &winRows);
    QUERY_CHECK_CODE(code, lino, _end);

    code = doOneWindowAggImpl(&pInfo->twAggSup.timeWindowData, &curWinInfo, &pResult, i, winRows, rows, numOfOutput,
                              pOperator, pAggSup->gap);
    QUERY_CHECK_CODE(code, lino, _end);

    code = saveSessionOutputBuf(pAggSup, &curWinInfo);
    QUERY_CHECK_CODE(code, lino, _end);

    i += winRows;
  }

  if (!pInfo->destHasPrimaryKey && !isHistoryOperator(&pInfo->basic)) {
    code = closeNonBlockSessionWindow(pAggSup->pResultRows, &pInfo->twAggSup, pInfo->basic.pUpdated, pTaskInfo);
    QUERY_CHECK_CODE(code, lino, _end);
  } else if ((isHistoryOperator(&pInfo->basic) || isRecalculateOperator(&pInfo->basic)) &&
             tSimpleHashGetSize(pAggSup->pResultRows) > pOperator->resultInfo.capacity * 10) {
    code = copyNewResult(&pAggSup->pResultRows, pInfo->basic.pUpdated, sessionKeyCompareAsc);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (!isHistoryOperator(&pInfo->basic)) {
    checkAndSaveSessionStateToDisc(0, pInfo->basic.pUpdated, 0, pInfo->basic.pTsDataState, &pInfo->streamAggSup);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

int32_t createFinalSessionNonblockOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode,
                                               SExecTaskInfo* pTaskInfo, SReadHandle* pHandle,
                                               SOperatorInfo** ppOptInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  code = createStreamSessionAggOperatorInfo(downstream, pPhyNode, pTaskInfo, pHandle, ppOptInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  SStreamSessionAggOperatorInfo* pInfo = (SStreamSessionAggOperatorInfo*)(*ppOptInfo)->info;
  pInfo->nbSup.pWindowAggFn = doStreamFinalSessionNonblockAggImpl;
  pInfo->streamAggSup.pScanBlock->info.type = STREAM_RETRIEVE;
  pInfo->nbSup.tsOfKeep = INT64_MIN;
  pInfo->nbSup.numOfChild = pHandle->numOfVgroups;
  pInfo->twAggSup.waterMark = 0;
  setFinalOperatorFlag(&pInfo->basic);
  adjustDownstreamBasicInfo(downstream, &pInfo->basic);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}
