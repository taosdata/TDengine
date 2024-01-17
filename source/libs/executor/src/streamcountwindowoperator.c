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
#include "function.h"
#include "functionMgt.h"
#include "operator.h"
#include "querytask.h"
#include "tchecksum.h"
#include "tcommon.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "tlog.h"
#include "ttime.h"

#define IS_FINAL_COUNT_OP(op)           ((op)->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_COUNT)
#define STREAM_COUNT_OP_STATE_NAME      "StreamCountHistoryState"
#define STREAM_COUNT_OP_CHECKPOINT_NAME "StreamCountOperator_Checkpoint"

typedef struct SCountWindowInfo {
  SResultWindowInfo winInfo;
  COUNT_TYPE*       pWindowCount;
} SCountWindowInfo;

void destroyStreamCountAggOperatorInfo(void* param) {
  SStreamCountAggOperatorInfo* pInfo = (SStreamCountAggOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);
  destroyStreamAggSupporter(&pInfo->streamAggSup);
  cleanupExprSupp(&pInfo->scalarSupp);
  clearGroupResInfo(&pInfo->groupResInfo);

  colDataDestroy(&pInfo->twAggSup.timeWindowData);
  blockDataDestroy(pInfo->pDelRes);
  tSimpleHashCleanup(pInfo->pStUpdated);
  tSimpleHashCleanup(pInfo->pStDeleted);
  pInfo->pUpdated = taosArrayDestroy(pInfo->pUpdated);
  cleanupGroupResInfo(&pInfo->groupResInfo);

  taosArrayDestroy(pInfo->historyWins);
  blockDataDestroy(pInfo->pCheckpointRes);

  taosMemoryFreeClear(param);
}

void setCountOutputBuf(SStreamAggSupporter* pAggSup, TSKEY ts, uint64_t groupId, SCountWindowInfo* pCurWin,
                       bool* pRebuild) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t size = pAggSup->resultRowSize;
  pCurWin->winInfo.sessionWin.groupId = groupId;
  pCurWin->winInfo.sessionWin.win.skey = ts;
  pCurWin->winInfo.sessionWin.win.ekey = ts;

  code = pAggSup->stateStore.streamStateCountWinAddIfNotExist(
      pAggSup->pState, &pCurWin->winInfo.sessionWin, pAggSup->windowCount, (void**)&pCurWin->winInfo.pStatePos, &size);
  // if (code == TSDB_CODE_SUCCESS && inWinRange(&pAggSup->winRange, &pCurWin->winInfo.sessionWin.win)) {
  //   pCurWin->pWindowCount=
  //       (COUNT_TYPE*) ((char*)pCurWin->winInfo.pStatePos->pRowBuff + (pAggSup->resultRowSize - sizeof(COUNT_TYPE)));
  // }
  if (code == TSDB_CODE_SUCCESS) {
    pCurWin->winInfo.isOutput = true;
  }
  pCurWin->pWindowCount=
    (COUNT_TYPE*) ((char*)pCurWin->winInfo.pStatePos->pRowBuff + (pAggSup->resultRowSize - sizeof(COUNT_TYPE)));

  if (*pCurWin->pWindowCount + 1 > pAggSup->windowCount) {
    *pRebuild = true;
  }
}

int32_t updateCountWindowInfo(SStreamAggSupporter* pAggSup, SCountWindowInfo* pWinInfo, TSKEY* pTs, int32_t start, int32_t rows, int32_t maxRows,
                              SSHashObj* pStDeleted, bool* pRebuild) {
  int32_t num = 0;
  for (int32_t i = start; i < rows; i++) {
    if (pTs[i] < pWinInfo->winInfo.sessionWin.win.ekey) {
      num++;
    } else {
      break;
    }
  }
  int32_t maxNum = TMIN(maxRows - *pWinInfo->pWindowCount, rows - start);
  if (num > maxNum) {
    *pRebuild = true;
  }
  *pWinInfo->pWindowCount += maxNum;
  bool needDelState = false;
  if (pWinInfo->winInfo.sessionWin.win.skey > pTs[start]) {
    needDelState = true;
    if (pStDeleted && pWinInfo->winInfo.isOutput) {
      saveDeleteRes(pStDeleted, pWinInfo->winInfo.sessionWin);
    }

    pWinInfo->winInfo.sessionWin.win.skey = pTs[start];
  }

  if (pWinInfo->winInfo.sessionWin.win.ekey < pTs[maxNum + start - 1]) {
    needDelState = true;
    pWinInfo->winInfo.sessionWin.win.ekey = pTs[maxNum + start - 1];
  }

  if (needDelState) {
    memcpy(pWinInfo->winInfo.pStatePos->pKey, &pWinInfo->winInfo.sessionWin, sizeof(SSessionKey));
    if (pWinInfo->winInfo.pStatePos->needFree) {
      pAggSup->stateStore.streamStateSessionDel(pAggSup->pState, &pWinInfo->winInfo.sessionWin);
    }
  }

  return maxNum;
}

void getCountWinRange(SStreamAggSupporter* pAggSup, const SSessionKey* pKey, EStreamType mode, SSessionKey* pDelRange) {
  *pDelRange = *pKey;
  SStreamStateCur* pCur = pAggSup->stateStore.streamStateSessionSeekKeyCurrentNext(pAggSup->pState, pKey);
  SSessionKey tmpKey = {0};
  int32_t code = pAggSup->stateStore.streamStateSessionGetKVByCur(pCur, &tmpKey, NULL, 0);
  if (code != TSDB_CODE_SUCCESS) {
    pAggSup->stateStore.streamStateFreeCur(pCur);
    return;
  }
  pDelRange->win = tmpKey.win;
  while (mode == STREAM_DELETE_DATA) {
    pAggSup->stateStore.streamStateCurNext(pAggSup->pState, pCur);
    code = pAggSup->stateStore.streamStateSessionGetKVByCur(pCur, &tmpKey, NULL, 0);
    if (code != TSDB_CODE_SUCCESS) {
      break;
    }
    pDelRange->win.ekey = TMAX(pDelRange->win.ekey, tmpKey.win.ekey);
  }
  pAggSup->stateStore.streamStateFreeCur(pCur);
}

void getCurSessionWindowByKey(SStreamAggSupporter* pAggSup, const SSessionKey* pRange, SSessionKey* pKey) {
  int32_t code = pAggSup->stateStore.streamStateSessionGetKeyByRange(pAggSup->pState, pRange, pKey);
  if (code != TSDB_CODE_SUCCESS) {
    SET_SESSION_WIN_KEY_INVALID(pKey);
  }
}

static void doStreamCountAggImpl(SOperatorInfo* pOperator, SSDataBlock* pSDataBlock, SSHashObj* pStUpdated,
                                 SSHashObj* pStDeleted) {
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  SStreamCountAggOperatorInfo* pInfo = pOperator->info;
  int32_t                      numOfOutput = pOperator->exprSupp.numOfExprs;
  uint64_t                     groupId = pSDataBlock->info.id.groupId;
  int64_t                      code = TSDB_CODE_SUCCESS;
  SResultRow*                  pResult = NULL;
  int32_t                      rows = pSDataBlock->info.rows;
  int32_t                      winRows = 0;
  SStreamAggSupporter*         pAggSup = &pInfo->streamAggSup;

  pInfo->dataVersion = TMAX(pInfo->dataVersion, pSDataBlock->info.version);
  pAggSup->winRange = pTaskInfo->streamInfo.fillHistoryWindow;
  if (pAggSup->winRange.ekey <= 0) {
    pAggSup->winRange.ekey = INT64_MAX;
  }

  SColumnInfoData* pStartTsCol = taosArrayGet(pSDataBlock->pDataBlock, pInfo->primaryTsIndex);
  TSKEY*           startTsCols = (int64_t*)pStartTsCol->pData;
  blockDataEnsureCapacity(pAggSup->pScanBlock, rows);

  for (int32_t i = 0; i < rows;) {
    if (pInfo->ignoreExpiredData &&
        checkExpiredData(&pInfo->streamAggSup.stateStore, pInfo->streamAggSup.pUpdateInfo, &pInfo->twAggSup,
                         pSDataBlock->info.id.uid, startTsCols[i])) {
      i++;
      continue;
    }
    SCountWindowInfo curWin = {0};
    bool rebuild = false;
    setCountOutputBuf(pAggSup, startTsCols[i], groupId, &curWin, &rebuild);
    setSessionWinOutputInfo(pStUpdated, &curWin.winInfo);
    if (!rebuild) {
      winRows = updateCountWindowInfo(pAggSup, &curWin, startTsCols, i, rows, pAggSup->windowCount, pStDeleted, &rebuild);
    }
    if (rebuild) {
      SSessionKey range = {0};
      getCountWinRange(pAggSup, &curWin.winInfo.sessionWin, STREAM_DELETE_DATA, &range);
      range.win.skey = TMIN(startTsCols[i], range.win.skey);
      range.win.ekey = TMAX(startTsCols[rows-1], range.win.ekey);
      uint64_t uid = 0;
      appendOneRowToStreamSpecialBlock(pAggSup->pScanBlock, &range.win.skey, &range.win.ekey, &uid, &range.groupId,
                                       NULL);
      break;
    }
    code = doOneWindowAggImpl(&pInfo->twAggSup.timeWindowData, &curWin.winInfo, &pResult, i, winRows, rows, numOfOutput,
                              pOperator, 0);
    if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
      qError("%s do stream count aggregate impl error, code %s", GET_TASKID(pTaskInfo), tstrerror(code));
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_OUT_OF_MEMORY);
    }
    saveSessionOutputBuf(pAggSup, &curWin.winInfo);

    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE && pStUpdated) {
      code = saveResult(curWin.winInfo, pStUpdated);
      if (code != TSDB_CODE_SUCCESS) {
        qError("%s do stream count aggregate impl, set result error, code %s", GET_TASKID(pTaskInfo),
               tstrerror(code));
        T_LONG_JMP(pTaskInfo->env, TSDB_CODE_OUT_OF_MEMORY);
      }
    }
    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_WINDOW_CLOSE) {
      SSessionKey key = {0};
      getSessionHashKey(&curWin.winInfo.sessionWin, &key);
      tSimpleHashPut(pAggSup->pResultRows, &key, sizeof(SSessionKey), &curWin.winInfo, sizeof(SResultWindowInfo));
    }

    i += winRows;
  }
}

static SSDataBlock* buildCountResult(SOperatorInfo* pOperator) {
  SStreamCountAggOperatorInfo* pInfo = pOperator->info;
  SStreamAggSupporter*         pAggSup = &pInfo->streamAggSup;
  SOptrBasicInfo*              pBInfo = &pInfo->binfo;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  doBuildDeleteDataBlock(pOperator, pInfo->pStDeleted, pInfo->pDelRes, &pInfo->pDelIterator);
  if (pInfo->pDelRes->info.rows > 0) {
    printDataBlock(pInfo->pDelRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
    return pInfo->pDelRes;
  }

  doBuildSessionResult(pOperator, pAggSup->pState, &pInfo->groupResInfo, pBInfo->pRes);
  if (pBInfo->pRes->info.rows > 0) {
    printDataBlock(pBInfo->pRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
    return pBInfo->pRes;
  }
  return NULL;
}

int32_t doStreamCountEncodeOpState(void** buf, int32_t len, SOperatorInfo* pOperator, bool isParent) {
  SStreamCountAggOperatorInfo* pInfo = pOperator->info;
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
    void* key = taosHashGetKey(pIte, &keyLen);
    tlen += encodeSSessionKey(buf, key);
    tlen += encodeSResultWindowInfo(buf, pIte, pInfo->streamAggSup.resultRowSize);
  }

  // 2.twAggSup
  tlen += encodeSTimeWindowAggSupp(buf, &pInfo->twAggSup);

  // 3.dataVersion
  tlen += taosEncodeFixedI32(buf, pInfo->dataVersion);

  // 4.checksum
  if (isParent) {
    if (buf) {
      uint32_t cksum = taosCalcChecksum(0, pData, len - sizeof(uint32_t));
      tlen += taosEncodeFixedU32(buf, cksum);
    } else {
      tlen += sizeof(uint32_t);
    }
  }

  return tlen;
}

void* doStreamCountDecodeOpState(void* buf, int32_t len, SOperatorInfo* pOperator, bool isParent) {
  SStreamCountAggOperatorInfo* pInfo = pOperator->info;
  if (!pInfo) {
    return buf;
  }

  // 4.checksum
  if (isParent) {
    int32_t dataLen = len - sizeof(uint32_t);
    void*   pCksum = POINTER_SHIFT(buf, dataLen);
    if (taosCheckChecksum(buf, dataLen, *(uint32_t*)pCksum) != TSDB_CODE_SUCCESS) {
      qError("stream count state is invalid");
      return buf;
    }
  }

  // 1.streamAggSup.pResultRows
  int32_t mapSize = 0;
  buf = taosDecodeFixedI32(buf, &mapSize);
  for (int32_t i = 0; i < mapSize; i++) {
    SSessionKey       key = {0};
    SResultWindowInfo winfo = {0};
    buf = decodeSSessionKey(buf, &key);
    buf = decodeSResultWindowInfo(buf, &winfo, pInfo->streamAggSup.resultRowSize);
    tSimpleHashPut(pInfo->streamAggSup.pResultRows, &key, sizeof(SSessionKey), &winfo, sizeof(SResultWindowInfo));
  }

  // 2.twAggSup
  buf = decodeSTimeWindowAggSupp(buf, &pInfo->twAggSup);

  // 3.dataVersion
  buf = taosDecodeFixedI64(buf, &pInfo->dataVersion);
  return buf;
}

void doStreamCountSaveCheckpoint(SOperatorInfo* pOperator) {
  SStreamCountAggOperatorInfo* pInfo = pOperator->info;
  int32_t                        len = doStreamCountEncodeOpState(NULL, 0, pOperator, true);
  void*                          buf = taosMemoryCalloc(1, len);
  void*                          pBuf = buf;
  len = doStreamCountEncodeOpState(&pBuf, len, pOperator, true);
  pInfo->streamAggSup.stateStore.streamStateSaveInfo(pInfo->streamAggSup.pState, STREAM_COUNT_OP_CHECKPOINT_NAME,
                                                     strlen(STREAM_COUNT_OP_CHECKPOINT_NAME), buf, len);
  taosMemoryFree(buf);
}

void doResetCountWindows(SStreamAggSupporter* pAggSup, SSDataBlock* pBlock) {
  SColumnInfoData* pStartTsCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  TSKEY*           startDatas = (TSKEY*)pStartTsCol->pData;
  SColumnInfoData* pEndTsCol = taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
  TSKEY*           endDatas = (TSKEY*)pEndTsCol->pData;
  SColumnInfoData* pGroupCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  uint64_t*        gpDatas = (uint64_t*)pGroupCol->pData;

  SRowBuffPos* pPos = NULL;
  int32_t size = 0;
  for (int32_t i = 0; i < pBlock->info.rows; i++) {
    SSessionKey key = {.groupId = gpDatas[i], .win.skey = startDatas[i], .win.ekey = endDatas[i]};
    SStreamStateCur* pCur = pAggSup->stateStore.streamStateSessionSeekKeyCurrentNext(pAggSup->pState, &key);
    while (1) {
      SSessionKey tmpKey = {0};
      int32_t code = pAggSup->stateStore.streamStateSessionGetKVByCur(pCur, &tmpKey, (void **)&pPos, &size);
      if (code != TSDB_CODE_SUCCESS || tmpKey.win.skey > endDatas[i]) {
        pAggSup->stateStore.streamStateFreeCur(pCur);
        break;
      }
      pAggSup->stateStore.streamStateSessionReset(pAggSup->pState, pPos->pRowBuff);
      pAggSup->stateStore.streamStateCurNext(pAggSup->pState, pCur);
    }
  }
}

static SSDataBlock* doStreamCountAgg(SOperatorInfo* pOperator) {
  SExprSupp*                     pSup = &pOperator->exprSupp;
  SStreamCountAggOperatorInfo*   pInfo = pOperator->info;
  SOptrBasicInfo*                pBInfo = &pInfo->binfo;
  SStreamAggSupporter*           pAggSup = &pInfo->streamAggSup;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  qDebug("stask:%s  %s status: %d", GET_TASKID(pTaskInfo), getStreamOpName(pOperator->operatorType), pOperator->status);
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  } else if (pOperator->status == OP_RES_TO_RETURN) {
    SSDataBlock* opRes = buildCountResult(pOperator);
    if (opRes) {
      return opRes;
    }

    if (pInfo->recvGetAll) {
      pInfo->recvGetAll = false;
      resetUnCloseSessionWinInfo(pInfo->streamAggSup.pResultRows);
    }

    if (pInfo->reCkBlock) {
      pInfo->reCkBlock = false;
      printDataBlock(pInfo->pCheckpointRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
      return pInfo->pCheckpointRes;
    }

    setStreamOperatorCompleted(pOperator);
    return NULL;
  }

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (!pInfo->pUpdated) {
    pInfo->pUpdated = taosArrayInit(16, sizeof(SResultWindowInfo));
  }
  if (!pInfo->pStUpdated) {
    _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
    pInfo->pStUpdated = tSimpleHashInit(64, hashFn);
  }
  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      break;
    }
    printSpecDataBlock(pBlock, getStreamOpName(pOperator->operatorType), "recv", GET_TASKID(pTaskInfo));

    if (pBlock->info.type == STREAM_DELETE_DATA || pBlock->info.type == STREAM_DELETE_RESULT) {
      deleteSessionWinState(&pInfo->streamAggSup, pBlock, pInfo->pStUpdated, pInfo->pStDeleted);
      continue;
    } else if (pBlock->info.type == STREAM_CLEAR) {
      doResetCountWindows(&pInfo->streamAggSup, pBlock);
      continue;
    } else if (pBlock->info.type == STREAM_GET_ALL) {
      pInfo->recvGetAll = true;
      getAllSessionWindow(pAggSup->pResultRows, pInfo->pStUpdated);
      continue;
    } else if (pBlock->info.type == STREAM_CREATE_CHILD_TABLE) {
      return pBlock;
    } else if (pBlock->info.type == STREAM_CHECKPOINT) {
      pAggSup->stateStore.streamStateCommit(pAggSup->pState);
      doStreamCountSaveCheckpoint(pOperator);
      copyDataBlock(pInfo->pCheckpointRes, pBlock);
      continue;
    } else {
      ASSERTS(pBlock->info.type == STREAM_NORMAL || pBlock->info.type == STREAM_INVALID, "invalid SSDataBlock type");
    }

    if (pInfo->scalarSupp.pExprInfo != NULL) {
      SExprSupp* pExprSup = &pInfo->scalarSupp;
      projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
    }
    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pSup, pBlock, TSDB_ORDER_ASC, MAIN_SCAN, true);
    doStreamCountAggImpl(pOperator, pBlock, pInfo->pStUpdated, pInfo->pStDeleted);
    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.window.ekey);
    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.watermark);
  }
  // restore the value
  pOperator->status = OP_RES_TO_RETURN;

  closeSessionWindow(pAggSup->pResultRows, &pInfo->twAggSup, pInfo->pStUpdated);
  copyUpdateResult(&pInfo->pStUpdated, pInfo->pUpdated, sessionKeyCompareAsc);
  removeSessionDeleteResults(pInfo->pStDeleted, pInfo->pUpdated);
  initGroupResInfoFromArrayList(&pInfo->groupResInfo, pInfo->pUpdated);
  pInfo->pUpdated = NULL;
  blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);

  SSDataBlock* opRes = buildCountResult(pOperator);
  if (opRes) {
    return opRes;
  }

  setStreamOperatorCompleted(pOperator);
  return NULL;
}

void streamCountReleaseState(SOperatorInfo* pOperator) {
  //nothing
  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (downstream->fpSet.releaseStreamStateFn) {
    downstream->fpSet.releaseStreamStateFn(downstream);
  }
}

void streamCountReloadState(SOperatorInfo* pOperator) {
  // nothing
  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (downstream->fpSet.reloadStreamStateFn) {
    downstream->fpSet.reloadStreamStateFn(downstream);
  }
}

SOperatorInfo* createStreamCountAggOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode,
                                                SExecTaskInfo* pTaskInfo, SReadHandle* pHandle) {
  SCountWinodwPhysiNode*       pCountNode = (SCountWinodwPhysiNode*)pPhyNode;
  int32_t                      numOfCols = 0;
  int32_t                      code = TSDB_CODE_OUT_OF_MEMORY;
  SStreamCountAggOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamCountAggOperatorInfo));
  SOperatorInfo*               pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pOperator->pTaskInfo = pTaskInfo;

  initResultSizeInfo(&pOperator->resultInfo, 4096);
  if (pCountNode->window.pExprs != NULL) {
    int32_t    numOfScalar = 0;
    SExprInfo* pScalarExprInfo = createExprInfo(pCountNode->window.pExprs, NULL, &numOfScalar);
    code = initExprSupp(&pInfo->scalarSupp, pScalarExprInfo, numOfScalar, &pTaskInfo->storageAPI.functionStore);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }
  SExprSupp* pExpSup = &pOperator->exprSupp;

  SExprInfo*   pExprInfo = createExprInfo(pCountNode->window.pFuncs, NULL, &numOfCols);
  SSDataBlock* pResBlock = createDataBlockFromDescNode(pPhyNode->pOutputDataBlockDesc);
  code = initBasicInfoEx(&pInfo->binfo, pExpSup, pExprInfo, numOfCols, pResBlock, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  code = initStreamAggSupporter(&pInfo->streamAggSup, pExpSup, numOfCols, 0,
                                pTaskInfo->streamInfo.pState, sizeof(COUNT_TYPE), 0, &pTaskInfo->storageAPI.stateStore, pHandle,
                                &pInfo->twAggSup, GET_TASKID(pTaskInfo), &pTaskInfo->storageAPI);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pInfo->streamAggSup.windowCount = pCountNode->windowCount;

  pInfo->twAggSup = (STimeWindowAggSupp){
      .waterMark = pCountNode->window.watermark,
      .calTrigger = pCountNode->window.triggerType,
      .maxTs = INT64_MIN,
      .minTs = INT64_MAX,
  };

  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);

  pInfo->primaryTsIndex = ((SColumnNode*)pCountNode->window.pTspk)->slotId;

  pInfo->binfo.pRes = pResBlock;
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pInfo->pStDeleted = tSimpleHashInit(64, hashFn);
  pInfo->pDelIterator = NULL;
  pInfo->pDelRes = createSpecialDataBlock(STREAM_DELETE_RESULT);
  pInfo->ignoreExpiredData = pCountNode->window.igExpired;
  pInfo->ignoreExpiredDataSaved = false;
  pInfo->pUpdated = NULL;
  pInfo->pStUpdated = NULL;
  pInfo->dataVersion = 0;
  pInfo->historyWins = taosArrayInit(4, sizeof(SSessionKey));
  if (!pInfo->historyWins) {
    goto _error;
  }

  pInfo->pCheckpointRes = createSpecialDataBlock(STREAM_CHECKPOINT);
  pInfo->recvGetAll = false;

  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_STREAM_COUNT;
  // for stream
  void*   buff = NULL;
  int32_t len = 0;
  int32_t res =
      pInfo->streamAggSup.stateStore.streamStateGetInfo(pInfo->streamAggSup.pState, STREAM_COUNT_OP_CHECKPOINT_NAME,
                                                        strlen(STREAM_COUNT_OP_CHECKPOINT_NAME), &buff, &len);
  if (res == TSDB_CODE_SUCCESS) {
    doStreamCountDecodeOpState(buff, len, pOperator, true);
    taosMemoryFree(buff);
  }
  setOperatorInfo(pOperator, getStreamOpName(pOperator->operatorType), QUERY_NODE_PHYSICAL_PLAN_STREAM_COUNT, true,
                  OP_NOT_OPENED, pInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doStreamCountAgg, NULL, destroyStreamCountAggOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorStreamStateFn(pOperator, streamCountReleaseState, streamCountReloadState);

  if (downstream) {
    initDownStream(downstream, &pInfo->streamAggSup, pOperator->operatorType, pInfo->primaryTsIndex, &pInfo->twAggSup);
    code = appendDownstream(pOperator, &downstream, 1);
  }
  return pOperator;

_error:
  if (pInfo != NULL) {
    destroyStreamCountAggOperatorInfo(pInfo);
  }

  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

