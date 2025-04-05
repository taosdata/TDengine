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

#define STREAM_STATE_NONBLOCK_OP_STATE_NAME "StreamStateNonblockHistoryState"

void streamStateNonblockReleaseState(SOperatorInfo* pOperator) {
  SStreamStateAggOperatorInfo* pInfo = pOperator->info;
  SStreamAggSupporter*         pAggSup = &pInfo->streamAggSup;
  pAggSup->stateStore.streamStateClearExpiredSessionState(pAggSup->pState, pInfo->nbSup.numOfKeep,
                                                          pInfo->nbSup.tsOfKeep, NULL);

  streamStateReleaseState(pOperator);
  qDebug("===stream===%s streamStateNonblockReleaseState:%" PRId64, GET_TASKID(pOperator->pTaskInfo),
         pInfo->twAggSup.maxTs);
}

void streamStateNonblockReloadState(SOperatorInfo* pOperator) {
  int32_t                        code = TSDB_CODE_SUCCESS;
  int32_t                        lino = 0;
  SStreamStateAggOperatorInfo* pInfo = pOperator->info;
  SStreamAggSupporter*           pAggSup = &pInfo->streamAggSup;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  int32_t                        size = 0;
  void*                          pBuf = NULL;

  resetWinRange(&pAggSup->winRange);
  code = pAggSup->stateStore.streamStateGetInfo(pAggSup->pState, STREAM_STATE_NONBLOCK_OP_STATE_NAME,
                                                strlen(STREAM_STATE_NONBLOCK_OP_STATE_NAME), &pBuf, &size);
  QUERY_CHECK_CODE(code, lino, _end);

  int32_t      num = (size - sizeof(TSKEY)) / sizeof(SSessionKey);
  SSessionKey* pSeKeyBuf = (SSessionKey*)pBuf;

  TSKEY ts = *(TSKEY*)((char*)pBuf + size - sizeof(TSKEY));
  pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, ts);
  pAggSup->stateStore.streamStateReloadInfo(pAggSup->pState, ts);

  for (int32_t i = 0; i < num; i++) {
    SStateWindowInfo curInfo = {0};
    SStateWindowInfo nextInfo = {0};
    qDebug("===stream===%s reload state. try process result %" PRId64 ", %" PRIu64 ", index:%d", GET_TASKID(pTaskInfo), pSeKeyBuf[i].win.skey,
           pSeKeyBuf[i].groupId, i);
    code = getStateWindowInfoByKey(pAggSup, pSeKeyBuf + i, &curInfo, &nextInfo);
    QUERY_CHECK_CODE(code, lino, _end);

    bool cpRes = compareWinStateKey(curInfo.pStateKey, nextInfo.pStateKey);
    qDebug("===stream=== reload state. next window info %" PRId64 ", %" PRIu64 ", compare:%d",
           nextInfo.winInfo.sessionWin.win.skey, nextInfo.winInfo.sessionWin.groupId, cpRes);
    if (cpRes) {
      code = compactStateWindow(pOperator, &curInfo.winInfo, &nextInfo.winInfo, pInfo->pSeUpdated, pInfo->pSeDeleted);
      qDebug("===stream=== reload state. save result %" PRId64 ", %" PRIu64, curInfo.winInfo.sessionWin.win.skey,
             curInfo.winInfo.sessionWin.groupId);
      QUERY_CHECK_CODE(code, lino, _end);
      bool isEnd = true;
      SStateWindowInfo nextNextInfo = nextInfo;
      getNextStateWin(pAggSup, &nextNextInfo, true);
      if (IS_VALID_SESSION_WIN(nextNextInfo.winInfo)) {
        isEnd = false;
      }

      if (isEnd) {
        code = saveDeleteRes(pInfo->basic.pSeDeleted, curInfo.winInfo.sessionWin);
        QUERY_CHECK_CODE(code, lino, _end);
        qDebug("===stream=== reload state. save delete result %" PRId64 ", %" PRIu64, curInfo.winInfo.sessionWin.win.skey,
               curInfo.winInfo.sessionWin.groupId);
      } else {
        void* pResPtr = taosArrayPush(pInfo->basic.pUpdated, &curInfo.winInfo);
        QUERY_CHECK_NULL(pResPtr, code, lino, _end, terrno);
        reuseOutputBuf(pAggSup->pState, curInfo.winInfo.pStatePos, &pAggSup->stateStore);
        qDebug("===stream=== reload state. save result %" PRId64 ", %" PRIu64, curInfo.winInfo.sessionWin.win.skey,
               curInfo.winInfo.sessionWin.groupId);
      }
    } else if (IS_VALID_SESSION_WIN(nextInfo.winInfo)) {
      releaseOutputBuf(pAggSup->pState, nextInfo.winInfo.pStatePos, &pAggSup->pSessionAPI->stateStore);
    }

    if (IS_VALID_SESSION_WIN(curInfo.winInfo)) {
      code = saveSessionOutputBuf(pAggSup, &curInfo.winInfo);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }
  taosMemoryFreeClear(pBuf);

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (downstream->fpSet.reloadStreamStateFn) {
    downstream->fpSet.reloadStreamStateFn(downstream);
  }
  qDebug("===stream===%s streamStateNonblockReloadState", GET_TASKID(pOperator->pTaskInfo));

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
}

int32_t doStreamStateNonblockAggImpl(SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  int32_t                        code = TSDB_CODE_SUCCESS;
  int32_t                        lino = 0;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  SStreamStateAggOperatorInfo*   pInfo = (SStreamStateAggOperatorInfo*)pOperator->info;
  int32_t                        numOfOutput = pOperator->exprSupp.numOfExprs;
  SResultRow*                    pResult = NULL;
  int64_t                        groupId = pBlock->info.id.groupId;
  SStreamAggSupporter*           pAggSup = &pInfo->streamAggSup;
  int64_t                        rows = pBlock->info.rows;
  int32_t                        winRows = 0;
  
  pAggSup->winRange = pTaskInfo->streamInfo.fillHistoryWindow;
  if (pAggSup->winRange.ekey <= 0) {
    pAggSup->winRange.ekey = INT64_MAX;
  }
  if (pAggSup->winRange.skey != INT64_MIN && pInfo->nbSup.pHistoryGroup == NULL) {
    _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
    pInfo->nbSup.pHistoryGroup = tSimpleHashInit(1024, hashFn);
  }

  SColumnInfoData* pStartTsCol = taosArrayGet(pBlock->pDataBlock, pInfo->primaryTsIndex);
  TSKEY*           tsCols = (int64_t*)pStartTsCol->pData;
  SColumnInfoData* pKeyColInfo = taosArrayGet(pBlock->pDataBlock, pInfo->stateCol.slotId);

  for (int32_t i = 0; i < rows; i += winRows) {
    char*            pKeyData = colDataGetData(pKeyColInfo, i);
    int32_t          winIndex = 0;
    bool             allEqual = true;
    SStateWindowInfo curWin = {0};
    SStateWindowInfo nextWin = {0};
    int32_t          winCode = TSDB_CODE_SUCCESS;
    code = setStateOutputBuf(pAggSup, tsCols[i], groupId, pKeyData, &curWin, &nextWin, &winCode);
    QUERY_CHECK_CODE(code, lino, _end);

    if (winCode != TSDB_CODE_SUCCESS) {
      SStreamStateCur* pCur =
          pAggSup->stateStore.streamStateSessionSeekKeyPrev(pAggSup->pState, &curWin.winInfo.sessionWin);
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

    code = updateStateWindowInfo(pAggSup, &curWin, &nextWin, tsCols, groupId, pKeyColInfo, rows, i, &allEqual,
                                 pAggSup->pResultRows, NULL, NULL, &winRows);
    QUERY_CHECK_CODE(code, lino, _end);

    code = doOneWindowAggImpl(&pInfo->twAggSup.timeWindowData, &curWin.winInfo, &pResult, i, winRows, rows, numOfOutput,
                              pOperator, 0);
    QUERY_CHECK_CODE(code, lino, _end);
    code = saveSessionOutputBuf(pAggSup, &curWin.winInfo);
    QUERY_CHECK_CODE(code, lino, _end);
    releaseOutputBuf(pAggSup->pState, curWin.winInfo.pStatePos, &pAggSup->stateStore);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

int32_t doStreamStateNonblockAggNext(SOperatorInfo* pOperator, SSDataBlock** ppBlock) {
  SStreamStateAggOperatorInfo* pInfo = (SStreamStateAggOperatorInfo*)pOperator->info;
  return doStreamSessionNonblockAggNextImpl(pOperator, &pInfo->binfo, &pInfo->basic, &pInfo->streamAggSup,
                                            &pInfo->twAggSup, &pInfo->groupResInfo, &pInfo->nbSup, &pInfo->scalarSupp,
                                            pInfo->historyWins, ppBlock);
}

int32_t createStateNonblockOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo,
                                        SReadHandle* pHandle, SOperatorInfo** ppOptInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  code = createStreamStateAggOperatorInfo(downstream, pPhyNode, pTaskInfo, pHandle, ppOptInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  SStreamStateAggOperatorInfo* pInfo = (SStreamStateAggOperatorInfo*)(*ppOptInfo)->info;
  pInfo->nbSup.numOfKeep = 1;
  pInfo->nbSup.pWindowAggFn = doStreamStateNonblockAggImpl;
  pInfo->nbSup.tsOfKeep = INT64_MIN;
  setSingleOperatorFlag(&pInfo->basic);
  adjustDownstreamBasicInfo(downstream, &pInfo->basic);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}
