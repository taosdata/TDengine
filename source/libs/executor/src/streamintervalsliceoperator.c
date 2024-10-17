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
#include "functionMgt.h"
#include "operator.h"
#include "querytask.h"
#include "storageapi.h"
#include "streamexecutorInt.h"
#include "tcommon.h"
#include "tdatablock.h"
#include "ttime.h"

#define STREAM_INTERVAL_SLICE_OP_CHECKPOINT_NAME "StreamIntervalSliceOperator_Checkpoint"

typedef struct SInervalSlicePoint {
  SSessionKey      winKey;
  bool             *pFinished;
  SSliceRowData*   pLastRow;
  SRowBuffPos*     pResPos;
} SInervalSlicePoint;

typedef enum SIntervalSliceType {
  INTERVAL_SLICE_START = 1,
  INTERVAL_SLICE_END = 2,
} SIntervalSliceType;

void streamIntervalSliceReleaseState(SOperatorInfo* pOperator) {
}

void streamIntervalSliceReloadState(SOperatorInfo* pOperator) {
}

void destroyStreamIntervalSliceOperatorInfo(void* param) {
  SStreamIntervalSliceOperatorInfo* pInfo = (SStreamIntervalSliceOperatorInfo*)param;
  if (param == NULL) {
    return;
  }
  cleanupBasicInfo(&pInfo->binfo);
  if (pInfo->pOperator) {
    cleanupResultInfoInStream(pInfo->pOperator->pTaskInfo, pInfo->streamAggSup.pState, &pInfo->pOperator->exprSupp,
                              &pInfo->groupResInfo);
    pInfo->pOperator = NULL;
  }

  clearGroupResInfo(&pInfo->groupResInfo);
  taosArrayDestroyP(pInfo->pUpdated, destroyFlusedPos);
  pInfo->pUpdated = NULL;

  if (pInfo->pUpdatedMap != NULL) {
    tSimpleHashSetFreeFp(pInfo->pUpdatedMap, destroyFlusedppPos);
    tSimpleHashCleanup(pInfo->pUpdatedMap);
    pInfo->pUpdatedMap = NULL;
  }
  destroyStreamAggSupporter(&pInfo->streamAggSup);

  colDataDestroy(&pInfo->twAggSup.timeWindowData);
  cleanupExprSupp(&pInfo->scalarSup);

  tSimpleHashCleanup(pInfo->pDeletedMap);
  taosArrayDestroy(pInfo->pDelWins);
  blockDataDestroy(pInfo->pDelRes);

  blockDataDestroy(pInfo->pCheckpointRes);

  taosMemoryFreeClear(param);
}

static int32_t buildIntervalSliceResult(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                           code = TSDB_CODE_SUCCESS;
  int32_t                           lino = 0;
  SStreamIntervalSliceOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*                    pTaskInfo = pOperator->pTaskInfo;
  uint16_t                          opType = pOperator->operatorType;
  SStreamAggSupporter*              pAggSup = &pInfo->streamAggSup;

  doBuildDeleteResultImpl(&pInfo->streamAggSup.stateStore, pInfo->streamAggSup.pState, pInfo->pDelWins, &pInfo->delIndex,
                          pInfo->pDelRes);
  if (pInfo->pDelRes->info.rows != 0) {
    // process the rest of the data
    printDataBlock(pInfo->pDelRes, getStreamOpName(opType), GET_TASKID(pTaskInfo));
    (*ppRes) = pInfo->pDelRes;
    return code;
  }

  doBuildStreamIntervalResult(pOperator, pInfo->streamAggSup.pState, pInfo->binfo.pRes, &pInfo->groupResInfo);
  if (pInfo->binfo.pRes->info.rows != 0) {
    printDataBlock(pInfo->binfo.pRes, getStreamOpName(opType), GET_TASKID(pTaskInfo));
    (*ppRes) = pInfo->binfo.pRes;
    goto _end;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

// static void doStreamIntervalSliceSaveCheckpoint(SOperatorInfo* pOperator) {
// }

void initIntervalSlicePoint(SStreamAggSupporter* pAggSup, STimeWindow* pTWin, int64_t groupId, SInervalSlicePoint* pPoint) {
  pPoint->winKey.groupId = groupId;
  pPoint->winKey.win = *pTWin;
  pPoint->pFinished = POINTER_SHIFT(pPoint->pResPos->pRowBuff, pAggSup->resultRowSize - pAggSup->stateKeySize);
  pPoint->pLastRow = POINTER_SHIFT(pPoint->pFinished, sizeof(bool));
}

static int32_t getIntervalSliceCurStateBuf(SStreamAggSupporter* pAggSup, SInterval* pInterval, bool needPrev, STimeWindow* pTWin, int64_t groupId,
                                           SInervalSlicePoint* pCurPoint, SInervalSlicePoint* pPrevPoint, int32_t* pWinCode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SWinKey curKey = {.ts = pTWin->skey, .groupId = groupId};
  int32_t curVLen = 0;
  code = pAggSup->stateStore.streamStateAddIfNotExist(pAggSup->pState, &curKey, (void**)&pCurPoint->pResPos,
                                                      &curVLen, pWinCode);
  QUERY_CHECK_CODE(code, lino, _end);

  qDebug("===stream=== set stream twa next point buf.ts:%" PRId64 ", groupId:%" PRIu64 ", res:%d",
         curKey.ts, curKey.groupId, *pWinCode);

  initIntervalSlicePoint(pAggSup, pTWin, groupId, pCurPoint);

  if (needPrev) {
    SWinKey prevKey = {.groupId = groupId};
    SET_WIN_KEY_INVALID(prevKey.ts);
    int32_t prevVLen = 0;
    int32_t prevWinCode = TSDB_CODE_SUCCESS;
    code = pAggSup->stateStore.streamStateGetPrev(pAggSup->pState, &curKey, &prevKey, (void**)&pPrevPoint->pResPos,
                                                  &prevVLen, &prevWinCode);
    QUERY_CHECK_CODE(code, lino, _end);

    if (prevWinCode == TSDB_CODE_SUCCESS) {
      STimeWindow prevSTW = {.skey = prevKey.ts};
      prevSTW.ekey = taosTimeGetIntervalEnd(prevSTW.skey, pInterval);
      initIntervalSlicePoint(pAggSup, &prevSTW, groupId, pPrevPoint);
    } else {
      SET_WIN_KEY_INVALID(pPrevPoint->winKey.win.skey);
      SET_WIN_KEY_INVALID(pPrevPoint->winKey.win.ekey);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

void doStreamSliceInterpolation(SSliceRowData* pPrevWinVal, TSKEY winKey, TSKEY curTs, SSDataBlock* pDataBlock,
                                int32_t curRowIndex, SExprSupp* pSup, SIntervalSliceType type) {
  SqlFunctionCtx* pCtx = pSup->pCtx;
  for (int32_t k = 0; k < pSup->numOfExprs; ++k) {
    if (!fmIsIntervalInterpoFunc(pCtx[k].functionId)) {
      pCtx[k].start.key = INT64_MIN;
      continue;
    }

    SFunctParam*     pParam = &pCtx[k].param[0];
    SColumnInfoData* pColInfo = taosArrayGet(pDataBlock->pDataBlock, pParam->pCol->slotId);

    double           prevVal = 0, curVal = 0, winVal = 0;
    SResultCellData* pCell = getSliceResultCell((SResultCellData*)pPrevWinVal->pRowVal, pParam->pCol->slotId);
    GET_TYPED_DATA(prevVal, double, pCell->type, pCell->pData);
    GET_TYPED_DATA(curVal, double, pColInfo->info.type, colDataGetData(pColInfo, curRowIndex));

    SPoint point1 = (SPoint){.key = pPrevWinVal->key, .val = &prevVal};
    SPoint point2 = (SPoint){.key = curTs, .val = &curVal};
    SPoint point = (SPoint){.key = winKey, .val = &winVal};

    if (!fmIsElapsedFunc(pCtx[k].functionId)) {
      taosGetLinearInterpolationVal(&point, TSDB_DATA_TYPE_DOUBLE, &point1, &point2, TSDB_DATA_TYPE_DOUBLE);
    }

    if (type == INTERVAL_SLICE_START) {
      pCtx[k].start.key = point.key;
      pCtx[k].start.val = winVal;
    } else {
      pCtx[k].end.key = point.key;
      pCtx[k].end.val = winVal;
    }
  }
}

static void resetIntervalSliceFunctionKey(SqlFunctionCtx* pCtx, int32_t numOfOutput) {
  for (int32_t k = 0; k < numOfOutput; ++k) {
    pCtx[k].start.key = INT64_MIN;
    pCtx[k].end.key = INT64_MIN;
  }
}

int32_t setIntervalSliceOutputBuf(SInervalSlicePoint* pPoint, SqlFunctionCtx* pCtx, int32_t numOfOutput,
                                  int32_t* rowEntryInfoOffset) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  SResultRow* res = pPoint->pResPos->pRowBuff;

  // set time window for current result
  res->win = pPoint->winKey.win;
  code = setResultRowInitCtx(res, pCtx, numOfOutput, rowEntryInfoOffset);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void setInterpoWindowFinished(SInervalSlicePoint* pPoint) {
  (*pPoint->pFinished) = true;
}

static bool isInterpoWindowFinished(SInervalSlicePoint* pPoint) {
  return *pPoint->pFinished;
}

static int32_t doStreamIntervalSliceAggImpl(SOperatorInfo* pOperator, SSDataBlock* pBlock, SSHashObj* pUpdatedMap,
                                            SSHashObj* pDeletedMap) {
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
    if (curTs > pInfo->endTs) {
      break;
    }

    int32_t winCode = TSDB_CODE_SUCCESS;
    code = getIntervalSliceCurStateBuf(&pInfo->streamAggSup, &pInfo->interval, pInfo->hasInterpoFunc, &curWin, groupId, &curPoint, &prevPoint, &winCode);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pInfo->hasInterpoFunc && IS_VALID_WIN_KEY(prevPoint.winKey.win.skey) && isInterpoWindowFinished(&prevPoint) == false) {
      code = setIntervalSliceOutputBuf(&prevPoint, pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset);
      QUERY_CHECK_CODE(code, lino, _end);

      resetIntervalSliceFunctionKey(pSup->pCtx, numOfOutput);
      doStreamSliceInterpolation(prevPoint.pLastRow, prevPoint.winKey.win.ekey, curTs, pBlock, startPos, &pOperator->exprSupp, INTERVAL_SLICE_END);
      updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &prevPoint.winKey.win, 1);
      code = applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, startPos,
                                             0, pBlock->info.rows, numOfOutput);
      QUERY_CHECK_CODE(code, lino, _end);
      SWinKey prevKey = {.ts = prevPoint.winKey.win.skey, .groupId = prevPoint.winKey.groupId};
      code = saveWinResult(&prevKey, prevPoint.pResPos, pInfo->pUpdatedMap);
      QUERY_CHECK_CODE(code, lino, _end);
      setInterpoWindowFinished(&prevPoint);
    }

    code = setIntervalSliceOutputBuf(&curPoint, pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset);
    QUERY_CHECK_CODE(code, lino, _end);

    resetIntervalSliceFunctionKey(pSup->pCtx, numOfOutput);
    if (pInfo->hasInterpoFunc && IS_VALID_WIN_KEY(prevPoint.winKey.win.skey) && curPoint.winKey.win.skey != curTs) {
      doStreamSliceInterpolation(prevPoint.pLastRow, curPoint.winKey.win.skey, curTs, pBlock, startPos, &pOperator->exprSupp, INTERVAL_SLICE_START);
    }
    forwardRows = getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, curWin.ekey, binarySearchForKey, NULL,
                                           TSDB_ORDER_ASC);
    int32_t prevEndPos = (forwardRows - 1) + startPos;
    if (pInfo->hasInterpoFunc && winCode != TSDB_CODE_SUCCESS) {
      int32_t endRowId = getQualifiedRowNumDesc(pSup, pBlock, tsCols, prevEndPos, false);
      TSKEY endRowTs = tsCols[endRowId];
      transBlockToSliceResultRow(pBlock, endRowId, endRowTs, curPoint.pLastRow, 0, NULL, NULL);
    }
    SWinKey curKey = {.ts = curPoint.winKey.win.skey, .groupId = curPoint.winKey.groupId};
    if (pInfo->destHasPrimaryKey && winCode == TSDB_CODE_SUCCESS) {
      code = tSimpleHashPut(pDeletedMap, &curKey, sizeof(SWinKey), NULL, 0);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    code = saveWinResult(&curKey, curPoint.pResPos, pInfo->pUpdatedMap);
    QUERY_CHECK_CODE(code, lino, _end);

    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &curPoint.winKey.win, 1);
    code = applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, startPos,
                                           forwardRows, pBlock->info.rows, numOfOutput);
    QUERY_CHECK_CODE(code, lino, _end);

    if (curPoint.pLastRow->key == curPoint.winKey.win.ekey) {
      setInterpoWindowFinished(&curPoint);
    }

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

static int32_t doStreamIntervalSliceNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                           code = TSDB_CODE_SUCCESS;
  int32_t                           lino = 0;
  SStreamIntervalSliceOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*                    pTaskInfo = pOperator->pTaskInfo;
  SStreamAggSupporter*              pAggSup = &pInfo->streamAggSup;

  qDebug("stask:%s  %s status: %d", GET_TASKID(pTaskInfo), getStreamOpName(pOperator->operatorType), pOperator->status);

  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    goto _end;
  }

  if (pOperator->status == OP_RES_TO_RETURN) {
    SSDataBlock* resBlock = NULL;
    code = buildIntervalSliceResult(pOperator, &resBlock);
    QUERY_CHECK_CODE(code, lino, _end);
    if (resBlock != NULL) {
      (*ppRes) = resBlock;
      return code;
    }

    if (pInfo->hasFill == false) {
      pAggSup->stateStore.streamStateClearExpiredState(pAggSup->pState);
    }
    setStreamOperatorCompleted(pOperator);
    (*ppRes) = NULL;
    return code;
  }

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  int32_t numOfDatapack = 0;

  while (1) {
    SSDataBlock* pBlock = NULL;
    code = downstream->fpSet.getNextFn(downstream, &pBlock);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pBlock == NULL) {
      pOperator->status = OP_RES_TO_RETURN;
      break;
    }

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
        // doStreamIntervalSliceSaveCheckpoint(pOperator);
        pInfo->recvCkBlock = true;
        code = copyDataBlock(pInfo->pCheckpointRes, pBlock);
        QUERY_CHECK_CODE(code, lino, _end);
        continue;
      } break;
      case STREAM_CREATE_CHILD_TABLE: {
        (*ppRes) = pBlock;
        goto _end;
      } break;
      case STREAM_GET_RESULT: {
        pInfo->endTs = taosTimeGetIntervalEnd(pBlock->info.window.skey, &pInfo->interval);
        if (pInfo->hasFill) {
          (*ppRes) = pBlock;
          goto _end;
        } else {
          continue;
        }
      }
      default:
        code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        QUERY_CHECK_CODE(code, lino, _end);
    }

    code = setInputDataBlock(&pOperator->exprSupp, pBlock, TSDB_ORDER_ASC, MAIN_SCAN, true);
    QUERY_CHECK_CODE(code, lino, _end);
    code = doStreamIntervalSliceAggImpl(pOperator, pBlock, pInfo->pUpdatedMap, NULL);
    QUERY_CHECK_CODE(code, lino, _end);
    
  }

  if (!pInfo->destHasPrimaryKey) {
    removeDeleteResults(pInfo->pUpdatedMap, pInfo->pDelWins);
  }

  if (pInfo->destHasPrimaryKey) {
    code = copyIntervalDeleteKey(pInfo->pDeletedMap, pInfo->pDelWins);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  code = copyUpdateResult(&pInfo->pUpdatedMap, pInfo->pUpdated, winPosCmprImpl);
  QUERY_CHECK_CODE(code, lino, _end);

  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pInfo->pUpdatedMap = tSimpleHashInit(1024, hashFn);
  QUERY_CHECK_NULL(pInfo->pUpdatedMap, code, lino, _end, terrno);

  initMultiResInfoFromArrayList(&pInfo->groupResInfo, pInfo->pUpdated);
  pInfo->pUpdated = taosArrayInit(1024, POINTER_BYTES);
  QUERY_CHECK_NULL(pInfo->pUpdated, code, lino, _end, terrno);

  code = blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  (*ppRes) = NULL;
  code = buildIntervalSliceResult(pOperator, ppRes);
  QUERY_CHECK_CODE(code, lino, _end);

  if ((*ppRes) == NULL) {
    if (pInfo->hasFill == false) {
      pAggSup->stateStore.streamStateClearExpiredState(pAggSup->pState);
    }
    setStreamOperatorCompleted(pOperator);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t initIntervalSliceDownStream(SOperatorInfo* downstream, SStreamAggSupporter* pAggSup, uint16_t type,
                                    int32_t tsColIndex, STimeWindowAggSupp* pTwSup, struct SSteamOpBasicInfo* pBasic,
                                    SInterval* pInterval, bool hasInterpoFunc) {
  SExecTaskInfo* pTaskInfo = downstream->pTaskInfo;
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  if (downstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_PARTITION) {
    SStreamPartitionOperatorInfo* pPartionInfo = downstream->info;
    pPartionInfo->tsColIndex = tsColIndex;
    pBasic->primaryPkIndex = pPartionInfo->basic.primaryPkIndex;
  }

  if (downstream->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    code =
        initIntervalSliceDownStream(downstream->pDownstream[0], pAggSup, type, tsColIndex, pTwSup, pBasic, pInterval, hasInterpoFunc);
    return code;
  }
  SStreamScanInfo* pScanInfo = downstream->info;
  pScanInfo->useGetResultRange = hasInterpoFunc;
  pScanInfo->igCheckUpdate = true;
  pScanInfo->windowSup = (SWindowSupporter){.pStreamAggSup = pAggSup, .gap = pAggSup->gap, .parentType = type};
  pScanInfo->pState = pAggSup->pState;
  if (!pScanInfo->pUpdateInfo) {
    code = pAggSup->stateStore.updateInfoInit(60000, TSDB_TIME_PRECISION_MILLI, pTwSup->waterMark,
                                              pScanInfo->igCheckUpdate, pScanInfo->pkColType, pScanInfo->pkColLen,
                                              &pScanInfo->pUpdateInfo);
    QUERY_CHECK_CODE(code, lino, _end);
  }
  pScanInfo->twAggSup = *pTwSup;
  pScanInfo->interval = *pInterval;
  pAggSup->pUpdateInfo = pScanInfo->pUpdateInfo;
  if (!hasSrcPrimaryKeyCol(pBasic)) {
    pBasic->primaryPkIndex = pScanInfo->basic.primaryPkIndex;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

static bool windowinterpNeeded(SqlFunctionCtx* pCtx, int32_t numOfCols) {
  bool  needed = false;
  for (int32_t i = 0; i < numOfCols; ++i) {
    SExprInfo* pExpr = pCtx[i].pExpr;
    if (fmIsIntervalInterpoFunc(pCtx[i].functionId)) {
      needed = true;
      break;
    }
  }
  return needed;
}

int32_t createStreamIntervalSliceOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo,
                                              SReadHandle* pHandle, SOperatorInfo** ppOptInfo) {
  int32_t                           code = TSDB_CODE_SUCCESS;
  int32_t                           lino = 0;
  SStreamIntervalSliceOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamIntervalSliceOperatorInfo));
  QUERY_CHECK_NULL(pInfo, code, lino, _error, terrno);

  SOperatorInfo* pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  QUERY_CHECK_NULL(pOperator, code, lino, _error, terrno)

  pInfo->pUpdated = taosArrayInit(1024, POINTER_BYTES);
  QUERY_CHECK_NULL(pInfo->pUpdated, code, lino, _error, terrno);

  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pInfo->pUpdatedMap = tSimpleHashInit(1024, hashFn);
  QUERY_CHECK_NULL(pInfo->pUpdatedMap, code, lino, _error, terrno);

  pInfo->pDeletedMap = tSimpleHashInit(1024, hashFn);
  QUERY_CHECK_NULL(pInfo->pDeletedMap, code, lino, _error, terrno);

  pInfo->delIndex = 0;
  pInfo->pDelWins = taosArrayInit(4, sizeof(SWinKey));
  QUERY_CHECK_NULL(pInfo->pDelWins, code, lino, _error, terrno);

  code = createSpecialDataBlock(STREAM_DELETE_RESULT, &pInfo->pDelRes);
  QUERY_CHECK_CODE(code, lino, _error);

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pPhyNode->pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pResBlock, code, lino, _error, terrno);
  initBasicInfo(&pInfo->binfo, pResBlock);

  code = createSpecialDataBlock(STREAM_CHECKPOINT, &pInfo->pCheckpointRes);
  QUERY_CHECK_CODE(code, lino, _error);
  pInfo->recvCkBlock = false;

  SStreamIntervalPhysiNode* pIntervalPhyNode = (SStreamIntervalPhysiNode*)pPhyNode;
  pOperator->pTaskInfo = pTaskInfo;
  initResultSizeInfo(&pOperator->resultInfo, 4096);
  SExprSupp* pExpSup = &pOperator->exprSupp;
  int32_t    numOfExprs = 0;
  SExprInfo* pExprInfo = NULL;
  code = createExprInfo(pIntervalPhyNode->window.pFuncs, NULL, &pExprInfo, &numOfExprs);
  QUERY_CHECK_CODE(code, lino, _error);

  code = initExprSupp(pExpSup, pExprInfo, numOfExprs, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->interval = (SInterval){.interval = pIntervalPhyNode->interval,
                                .sliding = pIntervalPhyNode->sliding,
                                .intervalUnit = pIntervalPhyNode->intervalUnit,
                                .slidingUnit = pIntervalPhyNode->slidingUnit,
                                .offset = pIntervalPhyNode->offset,
                                .precision = ((SColumnNode*)pIntervalPhyNode->window.pTspk)->node.resType.precision};

  pInfo->twAggSup =
      (STimeWindowAggSupp){.waterMark = pIntervalPhyNode->window.watermark,
                           .calTrigger = pIntervalPhyNode->window.triggerType,
                           .maxTs = INT64_MIN,
                           .minTs = INT64_MAX,
                           .deleteMark = getDeleteMark(&pIntervalPhyNode->window, pIntervalPhyNode->interval)};
  code = initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);
  QUERY_CHECK_CODE(code, lino, _error);

  if (pIntervalPhyNode->window.pExprs != NULL) {
    int32_t    numOfScalar = 0;
    SExprInfo* pScalarExprInfo = NULL;
    code = createExprInfo(pIntervalPhyNode->window.pExprs, NULL, &pScalarExprInfo, &numOfScalar);
    QUERY_CHECK_CODE(code, lino, _error);

    code = initExprSupp(&pInfo->scalarSup, pScalarExprInfo, numOfScalar, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  SSDataBlock* pDownRes = NULL;
  SColumnInfo* pPkCol = NULL;
  code = getDownstreamRes(downstream, &pDownRes, &pPkCol);
  QUERY_CHECK_CODE(code, lino, _error);

  int32_t keyBytes = sizeof(TSKEY);
  keyBytes += blockDataGetRowSize(pDownRes) + sizeof(SResultCellData) * taosArrayGetSize(pDownRes->pDataBlock) + sizeof(bool);
  if (pPkCol) {
    keyBytes += pPkCol->bytes;
  }
  code = initStreamAggSupporter(&pInfo->streamAggSup, pExpSup, numOfExprs, 0, pTaskInfo->streamInfo.pState, keyBytes, 0,
                                &pTaskInfo->storageAPI.stateStore, pHandle, &pInfo->twAggSup, GET_TASKID(pTaskInfo),
                                &pTaskInfo->storageAPI, pInfo->primaryTsIndex, STREAM_STATE_BUFF_HASH_SEARCH, 1);

  pInfo->destHasPrimaryKey = pIntervalPhyNode->window.destHasPrimaryKey;
  pInfo->pOperator = pOperator;
  pInfo->hasFill = false;
  pInfo->hasInterpoFunc = windowinterpNeeded(pExpSup->pCtx, numOfExprs);

  setOperatorInfo(pOperator, "StreamIntervalSliceOperator", QUERY_NODE_PHYSICAL_PLAN_STREAM_CONTINUE_INTERVAL, true, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doStreamIntervalSliceNext, NULL, destroyStreamIntervalSliceOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorStreamStateFn(pOperator, streamIntervalSliceReleaseState, streamIntervalSliceReloadState);

  initStreamBasicInfo(&pInfo->basic);
  if (downstream) {
    code = initIntervalSliceDownStream(downstream, &pInfo->streamAggSup, pPhyNode->type, pInfo->primaryTsIndex,
                                       &pInfo->twAggSup, &pInfo->basic, &pInfo->interval, pInfo->hasInterpoFunc);
    QUERY_CHECK_CODE(code, lino, _error);

    code = appendDownstream(pOperator, &downstream, 1);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  (*ppOptInfo) = pOperator;
  return code;

_error:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (pInfo != NULL) {
    destroyStreamIntervalSliceOperatorInfo(pInfo);
  }
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  (*ppOptInfo) = NULL;
  return code;
}
