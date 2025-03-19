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

#define STREAM_EVENT_NONBLOCK_OP_STATE_NAME "StreamEventNonblockHistoryState"

void streamEventNonblockReleaseState(SOperatorInfo* pOperator) {
  SStreamEventAggOperatorInfo* pInfo = pOperator->info;
  SStreamAggSupporter*         pAggSup = &pInfo->streamAggSup;
  pAggSup->stateStore.streamStateClearExpiredSessionState(pAggSup->pState, pInfo->nbSup.numOfKeep,
                                                          pInfo->nbSup.tsOfKeep, NULL);

  streamEventReleaseState(pOperator);
  qDebug("===stream===%s streamEventNonblockReleaseState:%" PRId64, GET_TASKID(pOperator->pTaskInfo),
         pInfo->twAggSup.maxTs);
}

void streamEventNonblockReloadState(SOperatorInfo* pOperator) {
  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      lino = 0;
  SStreamEventAggOperatorInfo* pInfo = pOperator->info;
  SStreamAggSupporter*         pAggSup = &pInfo->streamAggSup;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  int32_t                      size = 0;
  void*                        pBuf = NULL;

  resetWinRange(&pAggSup->winRange);
  code = pAggSup->stateStore.streamStateGetInfo(pAggSup->pState, STREAM_EVENT_NONBLOCK_OP_STATE_NAME,
                                                strlen(STREAM_EVENT_NONBLOCK_OP_STATE_NAME), &pBuf, &size);
  QUERY_CHECK_CODE(code, lino, _end);

  int32_t num = (size - sizeof(TSKEY)) / sizeof(SSessionKey);
  qDebug("===stream===%s event window operator reload state. get result count:%d", GET_TASKID(pOperator->pTaskInfo),
         num);
  SSessionKey* pSeKeyBuf = (SSessionKey*)pBuf;

  TSKEY ts = *(TSKEY*)((char*)pBuf + size - sizeof(TSKEY));
  pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, ts);
  pAggSup->stateStore.streamStateReloadInfo(pAggSup->pState, ts);

  for (int32_t i = 0; i < num; i++) {
    SEventWindowInfo curInfo = {0};
    qDebug("===stream=== reload state. try process result %" PRId64 ", %" PRIu64 ", index:%d", pSeKeyBuf[i].win.skey,
           pSeKeyBuf[i].groupId, i);
    code = getSessionWindowInfoByKey(pAggSup, pSeKeyBuf + i, &curInfo.winInfo);
    QUERY_CHECK_CODE(code, lino, _end);

    // event window has been deleted
    if (!IS_VALID_SESSION_WIN(curInfo.winInfo)) {
      continue;
    }
    setEventWindowFlag(pAggSup, &curInfo);
    if (!curInfo.pWinFlag->startFlag || curInfo.pWinFlag->endFlag) {
      code = saveSessionOutputBuf(pAggSup, &curInfo.winInfo);
      QUERY_CHECK_CODE(code, lino, _end);
      continue;
    }

    bool isEnd = true;
    code = compactEventWindow(pOperator, &curInfo, pInfo->pSeUpdated, pInfo->pSeDeleted, &isEnd);
    QUERY_CHECK_CODE(code, lino, _end);
    qDebug("===stream=== reload state. save result %" PRId64 ", %" PRIu64, curInfo.winInfo.sessionWin.win.skey,
           curInfo.winInfo.sessionWin.groupId);

    if (IS_VALID_SESSION_WIN(curInfo.winInfo)) {
      code = saveSessionOutputBuf(pAggSup, &curInfo.winInfo);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (!curInfo.pWinFlag->endFlag) {
      continue;
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
  }
  taosMemoryFreeClear(pBuf);

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (downstream->fpSet.reloadStreamStateFn) {
    downstream->fpSet.reloadStreamStateFn(downstream);
  }
  qDebug("===stream===%s streamEventNonblockReloadState", GET_TASKID(pOperator->pTaskInfo));

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
}

int32_t doStreamEventNonblockAggImpl(SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      lino = 0;
  SStreamEventAggOperatorInfo* pInfo = (SStreamEventAggOperatorInfo*)pOperator->info;
  SResultRowInfo*              pResultRowInfo = &(pInfo->binfo.resultRowInfo);
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*                   pSup = &pOperator->exprSupp;
  int32_t                      numOfOutput = pSup->numOfExprs;
  int64_t                      groupId = pBlock->info.id.groupId;
  SColumnInfoData*             pColStart = NULL;
  SColumnInfoData*             pColEnd = NULL;
  SStreamAggSupporter*         pAggSup = &pInfo->streamAggSup;
  SResultRow*                  pResult = NULL;

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

  SFilterColumnParam paramStart = {.numOfCols = taosArrayGetSize(pBlock->pDataBlock), .pDataBlock = pBlock->pDataBlock};
  code = filterSetDataFromSlotId(pInfo->pStartCondInfo, &paramStart);
  if (code != TSDB_CODE_SUCCESS) {
    qError("set data from start slotId error.");
    goto _end;
  }
  int32_t statusStart = 0;
  code = filterExecute(pInfo->pStartCondInfo, pBlock, &pColStart, NULL, paramStart.numOfCols, &statusStart);
  QUERY_CHECK_CODE(code, lino, _end);

  SFilterColumnParam paramEnd = {.numOfCols = taosArrayGetSize(pBlock->pDataBlock), .pDataBlock = pBlock->pDataBlock};
  code = filterSetDataFromSlotId(pInfo->pEndCondInfo, &paramEnd);
  if (code != TSDB_CODE_SUCCESS) {
    qError("set data from end slotId error.");
    goto _end;
  }

  int32_t statusEnd = 0;
  code = filterExecute(pInfo->pEndCondInfo, pBlock, &pColEnd, NULL, paramEnd.numOfCols, &statusEnd);
  QUERY_CHECK_CODE(code, lino, _end);

  int32_t rows = pBlock->info.rows;
  int32_t winRows = 0;
  for (int32_t i = 0; i < rows; i += winRows) {
    int32_t          winIndex = 0;
    bool             allEqual = true;
    SEventWindowInfo curWin = {0};
    SSessionKey      nextWinKey = {0};
    int32_t          winCode = TSDB_CODE_SUCCESS;
    code = setEventOutputBuf(pAggSup, tsCols, groupId, (bool*)pColStart->pData, (bool*)pColEnd->pData, i, rows, &curWin,
                             &nextWinKey, &winCode);
    QUERY_CHECK_CODE(code, lino, _end);

    if (winCode != TSDB_CODE_SUCCESS) {
      SStreamStateCur* pCur =
          pAggSup->stateStore.streamStateSessionSeekKeyPrev(pAggSup->pState, &curWin.winInfo.sessionWin);
      int32_t           size = 0;
      SResultWindowInfo prevWinInfo = {.sessionWin.groupId = groupId};
      int32_t           tmpWinCode = pAggSup->stateStore.streamStateSessionGetKVByCur(pCur, &prevWinInfo.sessionWin,
                                                                                      (void**)&prevWinInfo.pStatePos, &size);
      if (tmpWinCode == TSDB_CODE_SUCCESS) {
        SEventWindowInfo prevWin = {0};
        setEventWindowInfo(pAggSup, &prevWinInfo.sessionWin, prevWinInfo.pStatePos, &prevWin);
        if (isWindowIncomplete(&prevWin)) {
          continue;
        }
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
    bool rebuild = false;
    code = updateEventWindowInfo(pAggSup, &curWin, &nextWinKey, tsCols, (bool*)pColStart->pData, (bool*)pColEnd->pData,
                                 rows, i, pAggSup->pResultRows, NULL, NULL, &rebuild, &winRows);
    QUERY_CHECK_CODE(code, lino, _end);
    code = doOneWindowAggImpl(&pInfo->twAggSup.timeWindowData, &curWin.winInfo, &pResult, i, winRows, rows, numOfOutput,
                              pOperator, 0);
    QUERY_CHECK_CODE(code, lino, _end);
    code = saveSessionOutputBuf(pAggSup, &curWin.winInfo);
    QUERY_CHECK_CODE(code, lino, _end);
    releaseOutputBuf(pAggSup->pState, curWin.winInfo.pStatePos, &pAggSup->stateStore);
  }

_end:
  colDataDestroy(pColStart);
  taosMemoryFree(pColStart);
  colDataDestroy(pColEnd);
  taosMemoryFree(pColEnd);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

int32_t doStreamEventNonblockAggNext(SOperatorInfo* pOperator, SSDataBlock** ppBlock) {
  SStreamEventAggOperatorInfo* pInfo = (SStreamEventAggOperatorInfo*)pOperator->info;
  return doStreamSessionNonblockAggNextImpl(pOperator, &pInfo->binfo, &pInfo->basic, &pInfo->streamAggSup,
                                            &pInfo->twAggSup, &pInfo->groupResInfo, &pInfo->nbSup, &pInfo->scalarSupp,
                                            pInfo->historyWins, ppBlock);
}

int32_t createEventNonblockOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo,
                                        SReadHandle* pHandle, SOperatorInfo** ppOptInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  code = createStreamEventAggOperatorInfo(downstream, pPhyNode, pTaskInfo, pHandle, ppOptInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  SStreamEventAggOperatorInfo* pInfo = (SStreamEventAggOperatorInfo*)(*ppOptInfo)->info;
  pInfo->nbSup.pWindowAggFn = doStreamEventNonblockAggImpl;
  pInfo->nbSup.numOfKeep = 1;
  pInfo->nbSup.tsOfKeep = INT64_MIN;
  setSingleOperatorFlag(&pInfo->basic);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}
