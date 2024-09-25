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

#define IS_NORMAL_EVENT_OP(op)          ((op)->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_EVENT)
#define STREAM_EVENT_OP_STATE_NAME      "StreamEventHistoryState"
#define STREAM_EVENT_OP_CHECKPOINT_NAME "StreamEventOperator_Checkpoint"

typedef struct SEventWinfowFlag {
  bool startFlag;
  bool endFlag;
} SEventWinfowFlag;

typedef struct SEventWindowInfo {
  SResultWindowInfo winInfo;
  SEventWinfowFlag* pWinFlag;
} SEventWindowInfo;

void destroyStreamEventOperatorInfo(void* param) {
  if (param == NULL) {
    return;
  }
  SStreamEventAggOperatorInfo* pInfo = (SStreamEventAggOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);
  cleanupResultInfoInStream(pInfo->pOperator->pTaskInfo, pInfo->streamAggSup.pState, &pInfo->pOperator->exprSupp,
                            &pInfo->groupResInfo);
  pInfo->pOperator = NULL;
  destroyStreamAggSupporter(&pInfo->streamAggSup);
  clearGroupResInfo(&pInfo->groupResInfo);
  taosArrayDestroyP(pInfo->pUpdated, destroyFlusedPos);
  pInfo->pUpdated = NULL;

  cleanupExprSupp(&pInfo->scalarSupp);
  if (pInfo->pChildren != NULL) {
    int32_t size = taosArrayGetSize(pInfo->pChildren);
    for (int32_t i = 0; i < size; i++) {
      SOperatorInfo* pChild = taosArrayGetP(pInfo->pChildren, i);
      destroyOperator(pChild);
    }
    taosArrayDestroy(pInfo->pChildren);
  }
  colDataDestroy(&pInfo->twAggSup.timeWindowData);
  blockDataDestroy(pInfo->pDelRes);
  tSimpleHashCleanup(pInfo->pSeUpdated);
  tSimpleHashCleanup(pInfo->pAllUpdated);
  tSimpleHashCleanup(pInfo->pSeDeleted);
  cleanupGroupResInfo(&pInfo->groupResInfo);

  taosArrayDestroy(pInfo->historyWins);
  blockDataDestroy(pInfo->pCheckpointRes);

  tSimpleHashCleanup(pInfo->pPkDeleted);

  if (pInfo->pStartCondInfo != NULL) {
    filterFreeInfo(pInfo->pStartCondInfo);
    pInfo->pStartCondInfo = NULL;
  }

  if (pInfo->pEndCondInfo != NULL) {
    filterFreeInfo(pInfo->pEndCondInfo);
    pInfo->pEndCondInfo = NULL;
  }

  taosMemoryFreeClear(param);
}

void setEventWindowFlag(SStreamAggSupporter* pAggSup, SEventWindowInfo* pWinInfo) {
  char* pFlagInfo = (char*)pWinInfo->winInfo.pStatePos->pRowBuff + (pAggSup->resultRowSize - pAggSup->stateKeySize);
  pWinInfo->pWinFlag = (SEventWinfowFlag*)pFlagInfo;
}

void setEventWindowInfo(SStreamAggSupporter* pAggSup, SSessionKey* pKey, SRowBuffPos* pPos,
                        SEventWindowInfo* pWinInfo) {
  pWinInfo->winInfo.sessionWin = *pKey;
  pWinInfo->winInfo.pStatePos = pPos;
  setEventWindowFlag(pAggSup, pWinInfo);
}

int32_t getEndCondIndex(bool* pEnd, int32_t start, int32_t rows) {
  for (int32_t i = start; i < rows; i++) {
    if (pEnd[i]) {
      return i;
    }
  }
  return -1;
}

static bool isWindowIncomplete(SEventWindowInfo* pWinInfo) {
  return !(pWinInfo->pWinFlag->startFlag && pWinInfo->pWinFlag->endFlag);
}
void reuseOutputBuf(void* pState, SRowBuffPos* pPos, SStateStore* pAPI) {
  pAPI->streamStateReleaseBuf(pState, pPos, true);
}

int32_t setEventOutputBuf(SStreamAggSupporter* pAggSup, TSKEY* pTs, uint64_t groupId, bool* pStart, bool* pEnd,
                          int32_t index, int32_t rows, SEventWindowInfo* pCurWin, SSessionKey* pNextWinKey) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t winCode = TSDB_CODE_SUCCESS;
  int32_t size = pAggSup->resultRowSize;
  TSKEY   ts = pTs[index];
  bool    start = pStart[index];
  bool    end = pEnd[index];
  pCurWin->winInfo.sessionWin.groupId = groupId;
  pCurWin->winInfo.sessionWin.win.skey = ts;
  pCurWin->winInfo.sessionWin.win.ekey = ts;
  SStreamStateCur* pCur =
      pAggSup->stateStore.streamStateSessionSeekKeyCurrentPrev(pAggSup->pState, &pCurWin->winInfo.sessionWin);
  SSessionKey leftWinKey = {.groupId = groupId};
  void*       pVal = NULL;
  int32_t     len = 0;
  winCode = pAggSup->stateStore.streamStateSessionGetKVByCur(pCur, &leftWinKey, &pVal, &len);
  if (winCode == TSDB_CODE_SUCCESS && inWinRange(&pAggSup->winRange, &leftWinKey.win)) {
    bool inWin = isInTimeWindow(&leftWinKey.win, ts, 0);
    setEventWindowInfo(pAggSup, &leftWinKey, pVal, pCurWin);
    if (inWin || (pCurWin->pWinFlag->startFlag && !pCurWin->pWinFlag->endFlag)) {
      pCurWin->winInfo.isOutput = !isWindowIncomplete(pCurWin);
      goto _end;
    }
  }
  pAggSup->stateStore.streamStateFreeCur(pCur);
  pCur = pAggSup->stateStore.streamStateSessionSeekKeyNext(pAggSup->pState, &pCurWin->winInfo.sessionWin);
  SSessionKey rightWinKey = {.groupId = groupId};
  winCode = pAggSup->stateStore.streamStateSessionGetKVByCur(pCur, &rightWinKey, &pVal, &len);
  bool inWin = isInTimeWindow(&rightWinKey.win, ts, 0);
  if (winCode == TSDB_CODE_SUCCESS && inWinRange(&pAggSup->winRange, &rightWinKey.win) && (inWin || (start && !end))) {
    int32_t endi = getEndCondIndex(pEnd, index, rows);
    if (endi < 0 || pTs[endi] >= rightWinKey.win.skey) {
      setEventWindowInfo(pAggSup, &rightWinKey, pVal, pCurWin);
      pCurWin->winInfo.isOutput = !isWindowIncomplete(pCurWin);
      goto _end;
    }
  }

  SSessionKey winKey = {.win.skey = ts, .win.ekey = ts, .groupId = groupId};
  code = pAggSup->stateStore.streamStateSessionAllocWinBuffByNextPosition(pAggSup->pState, pCur, &winKey, &pVal, &len);
  QUERY_CHECK_CODE(code, lino, _error);

  setEventWindowInfo(pAggSup, &winKey, pVal, pCurWin);
  pCurWin->pWinFlag->startFlag = start;
  pCurWin->pWinFlag->endFlag = end;
  pCurWin->winInfo.isOutput = false;

_end:
  reuseOutputBuf(pAggSup->pState, pCurWin->winInfo.pStatePos, &pAggSup->stateStore);
  pAggSup->stateStore.streamStateCurNext(pAggSup->pState, pCur);
  pNextWinKey->groupId = groupId;
  winCode = pAggSup->stateStore.streamStateSessionGetKVByCur(pCur, pNextWinKey, NULL, 0);
  if (winCode != TSDB_CODE_SUCCESS) {
    SET_SESSION_WIN_KEY_INVALID(pNextWinKey);
  }
  if (pCurWin->winInfo.pStatePos->needFree) {
    pAggSup->stateStore.streamStateSessionDel(pAggSup->pState, &pCurWin->winInfo.sessionWin);
  }
  pAggSup->stateStore.streamStateFreeCur(pCur);
  qDebug("===stream===set event next win buff. skey:%" PRId64 ", endkey:%" PRId64, pCurWin->winInfo.sessionWin.win.skey,
         pCurWin->winInfo.sessionWin.win.ekey);

_error:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t updateEventWindowInfo(SStreamAggSupporter* pAggSup, SEventWindowInfo* pWinInfo, SSessionKey* pNextWinKey,
                              TSKEY* pTsData, bool* starts, bool* ends, int32_t rows, int32_t start,
                              SSHashObj* pResultRows, SSHashObj* pStUpdated, SSHashObj* pStDeleted, bool* pRebuild,
                              int32_t* pWinRow) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  *pRebuild = false;
  if (!pWinInfo->pWinFlag->startFlag && !(starts[start])) {
    (*pWinRow) = 1;
    goto _end;
  }

  TSKEY        maxTs = INT64_MAX;
  STimeWindow* pWin = &pWinInfo->winInfo.sessionWin.win;
  if (pWinInfo->pWinFlag->endFlag) {
    maxTs = pWin->ekey + 1;
  }
  if (!IS_INVALID_SESSION_WIN_KEY(*pNextWinKey)) {
    maxTs = TMIN(maxTs, pNextWinKey->win.skey);
  }

  for (int32_t i = start; i < rows; ++i) {
    if (pTsData[i] >= maxTs) {
      (*pWinRow) = i - start;
      goto _end;
    }

    if (pWin->skey > pTsData[i]) {
      if (pStDeleted && pWinInfo->winInfo.isOutput) {
        code = saveDeleteRes(pStDeleted, pWinInfo->winInfo.sessionWin);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      removeSessionResult(pAggSup, pStUpdated, pResultRows, &pWinInfo->winInfo.sessionWin);
      pWin->skey = pTsData[i];
      pWinInfo->pWinFlag->startFlag = starts[i];
    } else if (pWin->skey == pTsData[i]) {
      pWinInfo->pWinFlag->startFlag |= starts[i];
    }

    if (pWin->ekey < pTsData[i]) {
      pWin->ekey = pTsData[i];
      pWinInfo->pWinFlag->endFlag = ends[i];
    } else if (pWin->ekey == pTsData[i]) {
      pWinInfo->pWinFlag->endFlag |= ends[i];
    } else {
      *pRebuild = true;
      pWinInfo->pWinFlag->endFlag |= ends[i];
      (*pWinRow) = i + 1 - start;
      goto _end;
    }

    memcpy(pWinInfo->winInfo.pStatePos->pKey, &pWinInfo->winInfo.sessionWin, sizeof(SSessionKey));

    if (ends[i]) {
      if (pWinInfo->pWinFlag->endFlag && pWin->skey <= pTsData[i] && pTsData[i] < pWin->ekey) {
        *pRebuild = true;
      }
      (*pWinRow) = i + 1 - start;
      goto _end;
    }
  }
  (*pWinRow) = rows - start;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t compactEventWindow(SOperatorInfo* pOperator, SEventWindowInfo* pCurWin, SSHashObj* pStUpdated,
                                  SSHashObj* pStDeleted, bool addGap) {
  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      lino = 0;
  SExprSupp*                   pSup = &pOperator->exprSupp;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*                 pAPI = &pOperator->pTaskInfo->storageAPI;
  SStreamEventAggOperatorInfo* pInfo = pOperator->info;
  SResultRow*                  pCurResult = NULL;
  int32_t                      numOfOutput = pOperator->exprSupp.numOfExprs;
  SStreamAggSupporter*         pAggSup = &pInfo->streamAggSup;
  while (1) {
    if (!pCurWin->pWinFlag->startFlag || pCurWin->pWinFlag->endFlag) {
      break;
    }
    SEventWindowInfo nextWinInfo = {0};
    getNextSessionWinInfo(pAggSup, pStUpdated, &pCurWin->winInfo, &nextWinInfo.winInfo);
    if (!IS_VALID_SESSION_WIN(nextWinInfo.winInfo) ||
        !inWinRange(&pAggSup->winRange, &nextWinInfo.winInfo.sessionWin.win)) {
      releaseOutputBuf(pAggSup->pState, nextWinInfo.winInfo.pStatePos, &pAggSup->pSessionAPI->stateStore);
      break;
    }
    setEventWindowFlag(pAggSup, &nextWinInfo);
    code = compactTimeWindow(pSup, pAggSup, &pInfo->twAggSup, pTaskInfo, &pCurWin->winInfo, &nextWinInfo.winInfo,
                             pStUpdated, pStDeleted, false);
    QUERY_CHECK_CODE(code, lino, _end);

    pCurWin->pWinFlag->endFlag = nextWinInfo.pWinFlag->endFlag;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

void doDeleteEventWindow(SStreamAggSupporter* pAggSup, SSHashObj* pSeUpdated, SSessionKey* pKey) {
  pAggSup->stateStore.streamStateSessionDel(pAggSup->pState, pKey);
  removeSessionResult(pAggSup, pSeUpdated, pAggSup->pResultRows, pKey);
}

static void doStreamEventAggImpl(SOperatorInfo* pOperator, SSDataBlock* pSDataBlock, SSHashObj* pSeUpdated,
                                 SSHashObj* pStDeleted) {
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*                 pAPI = &pOperator->pTaskInfo->storageAPI;
  SStreamEventAggOperatorInfo* pInfo = pOperator->info;
  int32_t                      numOfOutput = pOperator->exprSupp.numOfExprs;
  uint64_t                     groupId = pSDataBlock->info.id.groupId;
  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      lino = 0;
  TSKEY*                       tsCols = NULL;
  SResultRow*                  pResult = NULL;
  int32_t                      winRows = 0;
  SStreamAggSupporter*         pAggSup = &pInfo->streamAggSup;
  SColumnInfoData*             pColStart = NULL;
  SColumnInfoData*             pColEnd = NULL;

  pInfo->dataVersion = TMAX(pInfo->dataVersion, pSDataBlock->info.version);
  pAggSup->winRange = pTaskInfo->streamInfo.fillHistoryWindow;
  if (pAggSup->winRange.ekey <= 0) {
    pAggSup->winRange.ekey = INT64_MAX;
  }

  if (pSDataBlock->pDataBlock != NULL) {
    SColumnInfoData* pColDataInfo = taosArrayGet(pSDataBlock->pDataBlock, pInfo->primaryTsIndex);
    if (!pColDataInfo) {
      code = TSDB_CODE_FAILED;
      QUERY_CHECK_CODE(code, lino, _end);
    }
    tsCols = (int64_t*)pColDataInfo->pData;
  } else {
    return;
  }

  SFilterColumnParam paramStart = {.numOfCols = taosArrayGetSize(pSDataBlock->pDataBlock),
                                   .pDataBlock = pSDataBlock->pDataBlock};
  code = filterSetDataFromSlotId(pInfo->pStartCondInfo, &paramStart);
  if (code != TSDB_CODE_SUCCESS) {
    qError("set data from start slotId error.");
    goto _end;
  }
  int32_t statusStart = 0;
  code = filterExecute(pInfo->pStartCondInfo, pSDataBlock, &pColStart, NULL, paramStart.numOfCols, &statusStart);
  QUERY_CHECK_CODE(code, lino, _end);

  SFilterColumnParam paramEnd = {.numOfCols = taosArrayGetSize(pSDataBlock->pDataBlock),
                                 .pDataBlock = pSDataBlock->pDataBlock};
  code = filterSetDataFromSlotId(pInfo->pEndCondInfo, &paramEnd);
  if (code != TSDB_CODE_SUCCESS) {
    qError("set data from end slotId error.");
    goto _end;
  }

  int32_t statusEnd = 0;
  code = filterExecute(pInfo->pEndCondInfo, pSDataBlock, &pColEnd, NULL, paramEnd.numOfCols, &statusEnd);
  QUERY_CHECK_CODE(code, lino, _end);

  int32_t rows = pSDataBlock->info.rows;
  code = blockDataEnsureCapacity(pAggSup->pScanBlock, rows);
  QUERY_CHECK_CODE(code, lino, _end);

  for (int32_t i = 0; i < rows; i += winRows) {
    if (pInfo->ignoreExpiredData && checkExpiredData(&pInfo->streamAggSup.stateStore, pInfo->streamAggSup.pUpdateInfo,
                                                     &pInfo->twAggSup, pSDataBlock->info.id.uid, tsCols[i], NULL, 0)) {
      i++;
      continue;
    }
    int32_t          winIndex = 0;
    bool             allEqual = true;
    SEventWindowInfo curWin = {0};
    SSessionKey      nextWinKey = {0};
    code = setEventOutputBuf(pAggSup, tsCols, groupId, (bool*)pColStart->pData, (bool*)pColEnd->pData, i, rows, &curWin,
                             &nextWinKey);
    QUERY_CHECK_CODE(code, lino, _end);

    setSessionWinOutputInfo(pSeUpdated, &curWin.winInfo);
    bool rebuild = false;
    code = updateEventWindowInfo(pAggSup, &curWin, &nextWinKey, tsCols, (bool*)pColStart->pData, (bool*)pColEnd->pData,
                                 rows, i, pAggSup->pResultRows, pSeUpdated, pStDeleted, &rebuild, &winRows);
    QUERY_CHECK_CODE(code, lino, _end);

    if (rebuild) {
      uint64_t uid = 0;
      code = appendDataToSpecialBlock(pAggSup->pScanBlock, &curWin.winInfo.sessionWin.win.skey,
                                      &curWin.winInfo.sessionWin.win.ekey, &uid, &groupId, NULL);
      QUERY_CHECK_CODE(code, lino, _end);

      int32_t tmpRes = tSimpleHashRemove(pSeUpdated, &curWin.winInfo.sessionWin, sizeof(SSessionKey));
      qTrace("%s at line %d res:%d", __func__, __LINE__, tmpRes);

      doDeleteEventWindow(pAggSup, pSeUpdated, &curWin.winInfo.sessionWin);
      if (pInfo->destHasPrimaryKey && curWin.winInfo.isOutput && IS_NORMAL_EVENT_OP(pOperator) &&
          !isWindowIncomplete(&curWin)) {
        code = saveDeleteRes(pInfo->pPkDeleted, curWin.winInfo.sessionWin);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      releaseOutputBuf(pAggSup->pState, curWin.winInfo.pStatePos, &pAPI->stateStore);
      SSessionKey tmpSeInfo = {0};
      getSessionHashKey(&curWin.winInfo.sessionWin, &tmpSeInfo);
      code = tSimpleHashPut(pStDeleted, &tmpSeInfo, sizeof(SSessionKey), NULL, 0);
      QUERY_CHECK_CODE(code, lino, _end);
      continue;
    }
    code = doOneWindowAggImpl(&pInfo->twAggSup.timeWindowData, &curWin.winInfo, &pResult, i, winRows, rows, numOfOutput,
                              pOperator, 0);
    QUERY_CHECK_CODE(code, lino, _end);

    code = compactEventWindow(pOperator, &curWin, pInfo->pSeUpdated, pInfo->pSeDeleted, false);
    QUERY_CHECK_CODE(code, lino, _end);

    code = saveSessionOutputBuf(pAggSup, &curWin.winInfo);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pInfo->isHistoryOp) {
      code = saveResult(curWin.winInfo, pInfo->pAllUpdated);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (isWindowIncomplete(&curWin)) {
      releaseOutputBuf(pAggSup->pState, curWin.winInfo.pStatePos, &pAggSup->stateStore);
      continue;
    }

    if (pInfo->destHasPrimaryKey && curWin.winInfo.isOutput && IS_NORMAL_EVENT_OP(pOperator)) {
      code = saveDeleteRes(pInfo->pPkDeleted, curWin.winInfo.sessionWin);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE) {
      code = saveResult(curWin.winInfo, pSeUpdated);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_WINDOW_CLOSE) {
      curWin.winInfo.pStatePos->beUpdated = true;
      SSessionKey key = {0};
      getSessionHashKey(&curWin.winInfo.sessionWin, &key);
      code =
          tSimpleHashPut(pAggSup->pResultRows, &key, sizeof(SSessionKey), &curWin.winInfo, sizeof(SResultWindowInfo));
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  colDataDestroy(pColStart);
  taosMemoryFree(pColStart);
  colDataDestroy(pColEnd);
  taosMemoryFree(pColEnd);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
}

int32_t doStreamEventEncodeOpState(void** buf, int32_t len, SOperatorInfo* pOperator) {
  SStreamEventAggOperatorInfo* pInfo = pOperator->info;
  if (!pInfo) {
    return 0;
  }

  void* pData = (buf == NULL) ? NULL : *buf;

  // 1.streamAggSup.pResultRows
  int32_t tlen = 0;
  int32_t mapSize = tSimpleHashGetSize(pInfo->streamAggSup.pResultRows);
  tlen += taosEncodeFixedI32(buf, mapSize);
  void*   pIte = NULL;
  size_t  keyLen = 0;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pInfo->streamAggSup.pResultRows, pIte, &iter)) != NULL) {
    void* key = tSimpleHashGetKey(pIte, &keyLen);
    tlen += encodeSSessionKey(buf, key);
    tlen += encodeSResultWindowInfo(buf, pIte, pInfo->streamAggSup.resultRowSize);
  }

  // 2.twAggSup
  tlen += encodeSTimeWindowAggSupp(buf, &pInfo->twAggSup);

  // 3.dataVersion
  tlen += taosEncodeFixedI32(buf, pInfo->dataVersion);

  // 4.checksum
  if (buf) {
    uint32_t cksum = taosCalcChecksum(0, pData, len - sizeof(uint32_t));
    tlen += taosEncodeFixedU32(buf, cksum);
  } else {
    tlen += sizeof(uint32_t);
  }

  return tlen;
}

int32_t doStreamEventDecodeOpState(void* buf, int32_t len, SOperatorInfo* pOperator) {
  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      lino = 0;
  SStreamEventAggOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  if (!pInfo) {
    code = TSDB_CODE_FAILED;
    QUERY_CHECK_CODE(code, lino, _end);
  }
  SStreamAggSupporter* pAggSup = &pInfo->streamAggSup;

  // 4.checksum
  int32_t dataLen = len - sizeof(uint32_t);
  void*   pCksum = POINTER_SHIFT(buf, dataLen);
  if (taosCheckChecksum(buf, dataLen, *(uint32_t*)pCksum) != TSDB_CODE_SUCCESS) {
    qError("stream event state is invalid");
    code = TSDB_CODE_FAILED;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  // 1.streamAggSup.pResultRows
  int32_t mapSize = 0;
  buf = taosDecodeFixedI32(buf, &mapSize);
  for (int32_t i = 0; i < mapSize; i++) {
    SResultWindowInfo winfo = {0};
    buf = decodeSSessionKey(buf, &winfo.sessionWin);
    int32_t winCode = TSDB_CODE_SUCCESS;
    code = pAggSup->stateStore.streamStateSessionAddIfNotExist(
        pAggSup->pState, &winfo.sessionWin, pAggSup->gap, (void**)&winfo.pStatePos, &pAggSup->resultRowSize, &winCode);
    QUERY_CHECK_CODE(code, lino, _end);

    buf = decodeSResultWindowInfo(buf, &winfo, pInfo->streamAggSup.resultRowSize);
    code =
        tSimpleHashPut(pInfo->streamAggSup.pResultRows, &winfo.sessionWin, sizeof(SSessionKey), &winfo, sizeof(SResultWindowInfo));
    QUERY_CHECK_CODE(code, lino, _end);
  }

  // 2.twAggSup
  buf = decodeSTimeWindowAggSupp(buf, &pInfo->twAggSup);

  // 3.dataVersion
  buf = taosDecodeFixedI64(buf, &pInfo->dataVersion);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

void doStreamEventSaveCheckpoint(SOperatorInfo* pOperator) {
  SStreamEventAggOperatorInfo* pInfo = pOperator->info;
  if (needSaveStreamOperatorInfo(&pInfo->basic)) {
    int32_t len = doStreamEventEncodeOpState(NULL, 0, pOperator);
    void*   buf = taosMemoryCalloc(1, len);
    if (!buf) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(TSDB_CODE_OUT_OF_MEMORY));
      return;
    }
    void* pBuf = buf;
    len = doStreamEventEncodeOpState(&pBuf, len, pOperator);
    pInfo->streamAggSup.stateStore.streamStateSaveInfo(pInfo->streamAggSup.pState, STREAM_EVENT_OP_CHECKPOINT_NAME,
                                                       strlen(STREAM_EVENT_OP_CHECKPOINT_NAME), buf, len);
    taosMemoryFree(buf);
    saveStreamOperatorStateComplete(&pInfo->basic);
  }
}

static int32_t buildEventResult(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                      code = TSDB_CODE_SUCCESS;
  SStreamEventAggOperatorInfo* pInfo = pOperator->info;
  SOptrBasicInfo*              pBInfo = &pInfo->binfo;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;

  doBuildDeleteDataBlock(pOperator, pInfo->pSeDeleted, pInfo->pDelRes, &pInfo->pDelIterator);
  if (pInfo->pDelRes->info.rows > 0) {
    printDataBlock(pInfo->pDelRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
    (*ppRes) = pInfo->pDelRes;
    return code;
  }

  doBuildSessionResult(pOperator, pInfo->streamAggSup.pState, &pInfo->groupResInfo, pBInfo->pRes);
  if (pBInfo->pRes->info.rows > 0) {
    printDataBlock(pBInfo->pRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
    (*ppRes) = pBInfo->pRes;
    return code;
  }
  (*ppRes) = NULL;
  return code;
}

static int32_t doStreamEventAggNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return TSDB_CODE_SUCCESS;
  }

  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      lino = 0;
  SExprSupp*                   pSup = &pOperator->exprSupp;
  SStreamEventAggOperatorInfo* pInfo = pOperator->info;
  SOptrBasicInfo*              pBInfo = &pInfo->binfo;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  qDebug("===stream=== stream event agg. history task:%d, taskId:%s", pInfo->isHistoryOp, GET_TASKID(pTaskInfo));
  if (pOperator->status == OP_RES_TO_RETURN) {
    SSDataBlock* resBlock = NULL;
    code = buildEventResult(pOperator, &resBlock);
    QUERY_CHECK_CODE(code, lino, _end);
    if (resBlock != NULL) {
      (*ppRes) = resBlock;
      return code;
    }

    if (pInfo->recvGetAll) {
      pInfo->recvGetAll = false;
      resetUnCloseSessionWinInfo(pInfo->streamAggSup.pResultRows);
    }

    if (pInfo->reCkBlock) {
      pInfo->reCkBlock = false;
      printDataBlock(pInfo->pCheckpointRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
      (*ppRes) = pInfo->pCheckpointRes;
      return code;
    }

    setStreamOperatorCompleted(pOperator);
    (*ppRes) = NULL;
    return code;
  }

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (!pInfo->pUpdated) {
    pInfo->pUpdated = taosArrayInit(16, sizeof(SEventWindowInfo));
    QUERY_CHECK_NULL(pInfo->pUpdated, code, lino, _end, terrno);
  }
  if (!pInfo->pSeUpdated) {
    _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
    pInfo->pSeUpdated = tSimpleHashInit(64, hashFn);
    QUERY_CHECK_NULL(pInfo->pSeUpdated, code, lino, _end, terrno);
  }
  while (1) {
    SSDataBlock* pBlock = NULL;

    code = downstream->fpSet.getNextFn(downstream, &pBlock);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pBlock == NULL) {
      break;
    }

    printSpecDataBlock(pBlock, getStreamOpName(pOperator->operatorType), "recv", GET_TASKID(pTaskInfo));
    setStreamOperatorState(&pInfo->basic, pBlock->info.type);

    if (pBlock->info.type == STREAM_DELETE_DATA || pBlock->info.type == STREAM_DELETE_RESULT ||
        pBlock->info.type == STREAM_CLEAR) {
      bool add = pInfo->destHasPrimaryKey && IS_NORMAL_EVENT_OP(pOperator);
      code = deleteSessionWinState(&pInfo->streamAggSup, pBlock, pInfo->pSeUpdated, pInfo->pSeDeleted,
                                   pInfo->pPkDeleted, add);
      QUERY_CHECK_CODE(code, lino, _end);
      continue;
    } else if (pBlock->info.type == STREAM_GET_ALL) {
      pInfo->recvGetAll = true;
      code = getAllSessionWindow(pInfo->streamAggSup.pResultRows, pInfo->pSeUpdated);
      QUERY_CHECK_CODE(code, lino, _end);
      continue;
    } else if (pBlock->info.type == STREAM_CREATE_CHILD_TABLE) {
      (*ppRes) = pBlock;
      return code;
    } else if (pBlock->info.type == STREAM_CHECKPOINT) {
      pInfo->streamAggSup.stateStore.streamStateCommit(pInfo->streamAggSup.pState);
      doStreamEventSaveCheckpoint(pOperator);
      pInfo->reCkBlock = true;
      code = copyDataBlock(pInfo->pCheckpointRes, pBlock);
      QUERY_CHECK_CODE(code, lino, _end);
      continue;
    } else {
      if (pBlock->info.type != STREAM_NORMAL && pBlock->info.type != STREAM_INVALID) {
        code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }

    if (pInfo->scalarSupp.pExprInfo != NULL) {
      SExprSupp* pExprSup = &pInfo->scalarSupp;
      code = projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    // the pDataBlock are always the same one, no need to call this again
    code = setInputDataBlock(pSup, pBlock, TSDB_ORDER_ASC, MAIN_SCAN, true);
    QUERY_CHECK_CODE(code, lino, _end);
    doStreamEventAggImpl(pOperator, pBlock, pInfo->pSeUpdated, pInfo->pSeDeleted);
    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.window.ekey);
  }
  // restore the value
  pOperator->status = OP_RES_TO_RETURN;

  code = closeSessionWindow(pInfo->streamAggSup.pResultRows, &pInfo->twAggSup, pInfo->pSeUpdated);
  QUERY_CHECK_CODE(code, lino, _end);

  code = copyUpdateResult(&pInfo->pSeUpdated, pInfo->pUpdated, sessionKeyCompareAsc);
  QUERY_CHECK_CODE(code, lino, _end);

  removeSessionDeleteResults(pInfo->pSeDeleted, pInfo->pUpdated);

  if (pInfo->isHistoryOp) {
    SArray* pHisWins = taosArrayInit(16, sizeof(SEventWindowInfo));
    if (!pHisWins) {
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _end);
    }

    code = copyUpdateResult(&pInfo->pAllUpdated, pHisWins, sessionKeyCompareAsc);
    QUERY_CHECK_CODE(code, lino, _end);

    _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
    pInfo->pAllUpdated = tSimpleHashInit(64, hashFn);
    QUERY_CHECK_NULL(pInfo->pAllUpdated, code, lino, _end, terrno);

    code = getMaxTsWins(pHisWins, pInfo->historyWins);
    QUERY_CHECK_CODE(code, lino, _end);

    taosArrayDestroy(pHisWins);
  }
  if (pInfo->destHasPrimaryKey && IS_NORMAL_EVENT_OP(pOperator)) {
    code = copyDeleteSessionKey(pInfo->pPkDeleted, pInfo->pSeDeleted);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  initGroupResInfoFromArrayList(&pInfo->groupResInfo, pInfo->pUpdated);
  pInfo->pUpdated = NULL;
  code = blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  SSDataBlock* resBlock = NULL;
  code = buildEventResult(pOperator, &resBlock);
  QUERY_CHECK_CODE(code, lino, _end);
  if (resBlock != NULL) {
    (*ppRes) = resBlock;
    return code;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  setStreamOperatorCompleted(pOperator);
  (*ppRes) = NULL;
  return code;
}

void streamEventReleaseState(SOperatorInfo* pOperator) {
  SStreamEventAggOperatorInfo* pInfo = pOperator->info;
  int32_t                      winSize = taosArrayGetSize(pInfo->historyWins) * sizeof(SSessionKey);
  int32_t                      resSize = winSize + sizeof(TSKEY);
  char*                        pBuff = taosMemoryCalloc(1, resSize);
  if (!pBuff) {
    return ;
  }
  memcpy(pBuff, pInfo->historyWins->pData, winSize);
  memcpy(pBuff + winSize, &pInfo->twAggSup.maxTs, sizeof(TSKEY));
  qDebug("===stream=== event window operator relase state. save result count:%d",
         (int32_t)taosArrayGetSize(pInfo->historyWins));
  pInfo->streamAggSup.stateStore.streamStateSaveInfo(pInfo->streamAggSup.pState, STREAM_EVENT_OP_STATE_NAME,
                                                     strlen(STREAM_EVENT_OP_STATE_NAME), pBuff, resSize);
  pInfo->streamAggSup.stateStore.streamStateCommit(pInfo->streamAggSup.pState);
  taosMemoryFreeClear(pBuff);

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (downstream->fpSet.releaseStreamStateFn) {
    downstream->fpSet.releaseStreamStateFn(downstream);
  }
}

void streamEventReloadState(SOperatorInfo* pOperator) {
  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      lino = 0;
  SStreamEventAggOperatorInfo* pInfo = pOperator->info;
  SStreamAggSupporter*         pAggSup = &pInfo->streamAggSup;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  resetWinRange(&pAggSup->winRange);

  SSessionKey seKey = {.win.skey = INT64_MIN, .win.ekey = INT64_MIN, .groupId = 0};
  int32_t     size = 0;
  void*       pBuf = NULL;
  code = pAggSup->stateStore.streamStateGetInfo(pAggSup->pState, STREAM_EVENT_OP_STATE_NAME,
                                                strlen(STREAM_EVENT_OP_STATE_NAME), &pBuf, &size);
  QUERY_CHECK_CODE(code, lino, _end);

  int32_t num = (size - sizeof(TSKEY)) / sizeof(SSessionKey);
  qDebug("===stream=== event window operator reload state. get result count:%d", num);
  SSessionKey* pSeKeyBuf = (SSessionKey*)pBuf;

  TSKEY ts = *(TSKEY*)((char*)pBuf + size - sizeof(TSKEY));
  pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, ts);
  pAggSup->stateStore.streamStateReloadInfo(pAggSup->pState, ts);

  if (!pInfo->pSeUpdated && num > 0) {
    _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
    pInfo->pSeUpdated = tSimpleHashInit(64, hashFn);
    QUERY_CHECK_NULL(pInfo->pSeUpdated, code, lino, _end, terrno);
  }
  if (!pInfo->pSeDeleted && num > 0) {
    _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
    pInfo->pSeDeleted = tSimpleHashInit(64, hashFn);
    QUERY_CHECK_NULL(pInfo->pSeDeleted, code, lino, _end, terrno);
  }
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

    code = compactEventWindow(pOperator, &curInfo, pInfo->pSeUpdated, pInfo->pSeDeleted, false);
    qDebug("===stream=== reload state. save result %" PRId64 ", %" PRIu64, curInfo.winInfo.sessionWin.win.skey,
           curInfo.winInfo.sessionWin.groupId);
    QUERY_CHECK_CODE(code, lino, _end);

    if (IS_VALID_SESSION_WIN(curInfo.winInfo)) {
      code = saveSessionOutputBuf(pAggSup, &curInfo.winInfo);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (!curInfo.pWinFlag->endFlag) {
      continue;
    }

    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE) {
      code = saveResult(curInfo.winInfo, pInfo->pSeUpdated);
      QUERY_CHECK_CODE(code, lino, _end);
    } else if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_WINDOW_CLOSE) {
      if (!isCloseWindow(&curInfo.winInfo.sessionWin.win, &pInfo->twAggSup)) {
        code = saveDeleteRes(pInfo->pSeDeleted, curInfo.winInfo.sessionWin);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      SSessionKey key = {0};
      getSessionHashKey(&curInfo.winInfo.sessionWin, &key);
      code =
          tSimpleHashPut(pAggSup->pResultRows, &key, sizeof(SSessionKey), &curInfo.winInfo, sizeof(SResultWindowInfo));
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }
  taosMemoryFree(pBuf);

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (downstream->fpSet.reloadStreamStateFn) {
    downstream->fpSet.reloadStreamStateFn(downstream);
  }
  reloadAggSupFromDownStream(downstream, &pInfo->streamAggSup);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
}

int32_t createStreamEventAggOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo,
                                         SReadHandle* pHandle, SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  SStreamEventWinodwPhysiNode* pEventNode = (SStreamEventWinodwPhysiNode*)pPhyNode;
  int32_t                      tsSlotId = ((SColumnNode*)pEventNode->window.pTspk)->slotId;
  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      lino = 0;
  SStreamEventAggOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamEventAggOperatorInfo));
  SOperatorInfo*               pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }

  initResultSizeInfo(&pOperator->resultInfo, 4096);
  if (pEventNode->window.pExprs != NULL) {
    int32_t    numOfScalar = 0;
    SExprInfo* pScalarExprInfo = NULL;
    code = createExprInfo(pEventNode->window.pExprs, NULL, &pScalarExprInfo, &numOfScalar);
    QUERY_CHECK_CODE(code, lino, _error);

    code = initExprSupp(&pInfo->scalarSupp, pScalarExprInfo, numOfScalar, &pTaskInfo->storageAPI.functionStore);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  pInfo->twAggSup = (STimeWindowAggSupp){
      .waterMark = pEventNode->window.watermark,
      .calTrigger = pEventNode->window.triggerType,
      .maxTs = INT64_MIN,
      .minTs = INT64_MAX,
      .deleteMark = getDeleteMark(&pEventNode->window, 0),
  };

  code = initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);
  QUERY_CHECK_CODE(code, lino, _error);

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pPhyNode->pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pResBlock, code, lino, _error, terrno);
  pInfo->binfo.pRes = pResBlock;

  SExprSupp*   pExpSup = &pOperator->exprSupp;
  int32_t      numOfCols = 0;
  SExprInfo*   pExprInfo = NULL;
  code = createExprInfo(pEventNode->window.pFuncs, NULL, &pExprInfo, &numOfCols);
  QUERY_CHECK_CODE(code, lino, _error);

  code = initBasicInfoEx(&pInfo->binfo, pExpSup, pExprInfo, numOfCols, pResBlock, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->primaryTsIndex = tsSlotId;
  code = initStreamAggSupporter(&pInfo->streamAggSup, pExpSup, numOfCols, 0, pTaskInfo->streamInfo.pState,
                                sizeof(bool) + sizeof(bool), 0, &pTaskInfo->storageAPI.stateStore, pHandle,
                                &pInfo->twAggSup, GET_TASKID(pTaskInfo), &pTaskInfo->storageAPI, pInfo->primaryTsIndex);
  QUERY_CHECK_CODE(code, lino, _error);

  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pInfo->pSeDeleted = tSimpleHashInit(64, hashFn);
  QUERY_CHECK_NULL(pInfo->pSeDeleted, code, lino, _error, terrno);
  pInfo->pDelIterator = NULL;
  code = createSpecialDataBlock(STREAM_DELETE_RESULT, &pInfo->pDelRes);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->pChildren = NULL;
  pInfo->ignoreExpiredData = pEventNode->window.igExpired;
  pInfo->ignoreExpiredDataSaved = false;
  pInfo->pUpdated = NULL;
  pInfo->pSeUpdated = NULL;
  pInfo->dataVersion = 0;
  pInfo->historyWins = taosArrayInit(4, sizeof(SSessionKey));
  if (!pInfo->historyWins) {
    goto _error;
  }
  if (pHandle) {
    pInfo->isHistoryOp = pHandle->fillHistory;
  }

  if (pInfo->isHistoryOp) {
    pInfo->pAllUpdated = tSimpleHashInit(64, hashFn);
    QUERY_CHECK_NULL(pInfo->pAllUpdated, code, lino, _error, terrno);
  } else {
    pInfo->pAllUpdated = NULL;
  }

  code = createSpecialDataBlock(STREAM_CHECKPOINT, &pInfo->pCheckpointRes);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->reCkBlock = false;
  pInfo->recvGetAll = false;
  pInfo->pPkDeleted = tSimpleHashInit(64, hashFn);
  QUERY_CHECK_NULL(pInfo->pPkDeleted, code, lino, _error, terrno);
  pInfo->destHasPrimaryKey = pEventNode->window.destHasPrimayKey;

  pInfo->pOperator = pOperator;
  setOperatorInfo(pOperator, "StreamEventAggOperator", QUERY_NODE_PHYSICAL_PLAN_STREAM_EVENT, true, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  // for stream
  void*   buff = NULL;
  int32_t len = 0;
  int32_t res =
      pInfo->streamAggSup.stateStore.streamStateGetInfo(pInfo->streamAggSup.pState, STREAM_EVENT_OP_CHECKPOINT_NAME,
                                                        strlen(STREAM_EVENT_OP_CHECKPOINT_NAME), &buff, &len);
  if (res == TSDB_CODE_SUCCESS) {
    code = doStreamEventDecodeOpState(buff, len, pOperator);
    taosMemoryFree(buff);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doStreamEventAggNext, NULL, destroyStreamEventOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorStreamStateFn(pOperator, streamEventReleaseState, streamEventReloadState);
  code = initDownStream(downstream, &pInfo->streamAggSup, pOperator->operatorType, pInfo->primaryTsIndex,
                        &pInfo->twAggSup, &pInfo->basic);
  QUERY_CHECK_CODE(code, lino, _error);

  code = appendDownstream(pOperator, &downstream, 1);
  QUERY_CHECK_CODE(code, lino, _error);

  code = filterInitFromNode((SNode*)pEventNode->pStartCond, &pInfo->pStartCondInfo, 0);
  QUERY_CHECK_CODE(code, lino, _error);

  code = filterInitFromNode((SNode*)pEventNode->pEndCond, &pInfo->pEndCondInfo, 0);
  QUERY_CHECK_CODE(code, lino, _error);

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  if (pInfo != NULL) destroyStreamEventOperatorInfo(pInfo);
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}
