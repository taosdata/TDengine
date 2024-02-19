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
#include "tchecksum.h"
#include "tcommon.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tfill.h"
#include "tglobal.h"
#include "tlog.h"
#include "ttime.h"

#define IS_FINAL_INTERVAL_OP(op)           ((op)->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL)
#define IS_MID_INTERVAL_OP(op)             ((op)->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_MID_INTERVAL)
#define IS_FINAL_SESSION_OP(op)            ((op)->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION)
#define DEAULT_DELETE_MARK                 INT64_MAX
#define STREAM_INTERVAL_OP_STATE_NAME      "StreamIntervalHistoryState"
#define STREAM_SESSION_OP_STATE_NAME       "StreamSessionHistoryState"
#define STREAM_STATE_OP_STATE_NAME         "StreamStateHistoryState"
#define STREAM_INTERVAL_OP_CHECKPOINT_NAME "StreamIntervalOperator_Checkpoint"
#define STREAM_SESSION_OP_CHECKPOINT_NAME  "StreamSessionOperator_Checkpoint"
#define STREAM_STATE_OP_CHECKPOINT_NAME    "StreamStateOperator_Checkpoint"

typedef struct SStateWindowInfo {
  SResultWindowInfo winInfo;
  SStateKeys*       pStateKey;
} SStateWindowInfo;

typedef struct SPullWindowInfo {
  STimeWindow window;
  uint64_t    groupId;
  STimeWindow calWin;
} SPullWindowInfo;

static SSDataBlock* doStreamMidIntervalAgg(SOperatorInfo* pOperator);

typedef int32_t (*__compare_fn_t)(void* pKey, void* data, int32_t index);

static int32_t binarySearchCom(void* keyList, int num, void* pKey, int order, __compare_fn_t comparefn) {
  int firstPos = 0, lastPos = num - 1, midPos = -1;
  int numOfRows = 0;

  if (num <= 0) return -1;
  if (order == TSDB_ORDER_DESC) {
    // find the first position which is smaller or equal than the key
    while (1) {
      if (comparefn(pKey, keyList, lastPos) >= 0) return lastPos;
      if (comparefn(pKey, keyList, firstPos) == 0) return firstPos;
      if (comparefn(pKey, keyList, firstPos) < 0) return firstPos - 1;

      numOfRows = lastPos - firstPos + 1;
      midPos = (numOfRows >> 1) + firstPos;

      if (comparefn(pKey, keyList, midPos) < 0) {
        lastPos = midPos - 1;
      } else if (comparefn(pKey, keyList, midPos) > 0) {
        firstPos = midPos + 1;
      } else {
        break;
      }
    }

  } else {
    // find the first position which is bigger or equal than the key
    while (1) {
      if (comparefn(pKey, keyList, firstPos) <= 0) return firstPos;
      if (comparefn(pKey, keyList, lastPos) == 0) return lastPos;

      if (comparefn(pKey, keyList, lastPos) > 0) {
        lastPos = lastPos + 1;
        if (lastPos >= num)
          return -1;
        else
          return lastPos;
      }

      numOfRows = lastPos - firstPos + 1;
      midPos = (numOfRows >> 1) + firstPos;

      if (comparefn(pKey, keyList, midPos) < 0) {
        lastPos = midPos - 1;
      } else if (comparefn(pKey, keyList, midPos) > 0) {
        firstPos = midPos + 1;
      } else {
        break;
      }
    }
  }

  return midPos;
}

static int32_t comparePullWinKey(void* pKey, void* data, int32_t index) {
  SArray*          res = (SArray*)data;
  SPullWindowInfo* pos = taosArrayGet(res, index);
  SPullWindowInfo* pData = (SPullWindowInfo*)pKey;
  if (pData->groupId > pos->groupId) {
    return 1;
  } else if (pData->groupId < pos->groupId) {
    return -1;
  }

  if (pData->window.skey > pos->window.ekey) {
    return 1;
  } else if (pData->window.ekey < pos->window.skey) {
    return -1;
  }
  return 0;
}

static int32_t savePullWindow(SPullWindowInfo* pPullInfo, SArray* pPullWins) {
  int32_t size = taosArrayGetSize(pPullWins);
  int32_t index = binarySearchCom(pPullWins, size, pPullInfo, TSDB_ORDER_DESC, comparePullWinKey);
  if (index == -1) {
    index = 0;
  } else {
    int32_t code = comparePullWinKey(pPullInfo, pPullWins, index);
    if (code == 0) {
      SPullWindowInfo* pos = taosArrayGet(pPullWins, index);
      pos->window.skey = TMIN(pos->window.skey, pPullInfo->window.skey);
      pos->window.ekey = TMAX(pos->window.ekey, pPullInfo->window.ekey);
      pos->calWin.skey = TMIN(pos->calWin.skey, pPullInfo->calWin.skey);
      pos->calWin.ekey = TMAX(pos->calWin.ekey, pPullInfo->calWin.ekey);
      return TSDB_CODE_SUCCESS;
    } else if (code > 0) {
      index++;
    }
  }
  if (taosArrayInsert(pPullWins, index, pPullInfo) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t saveResult(SResultWindowInfo winInfo, SSHashObj* pStUpdated) {
  winInfo.sessionWin.win.ekey = winInfo.sessionWin.win.skey;
  return tSimpleHashPut(pStUpdated, &winInfo.sessionWin, sizeof(SSessionKey), &winInfo, sizeof(SResultWindowInfo));
}

static int32_t saveWinResult(SWinKey* pKey, SRowBuffPos* pPos, SSHashObj* pUpdatedMap) {
  return tSimpleHashPut(pUpdatedMap, pKey, sizeof(SWinKey), &pPos, POINTER_BYTES);
}

static int32_t saveWinResultInfo(TSKEY ts, uint64_t groupId, SRowBuffPos* pPos, SSHashObj* pUpdatedMap) {
  SWinKey key = {.ts = ts, .groupId = groupId};
  saveWinResult(&key, pPos, pUpdatedMap);
  return TSDB_CODE_SUCCESS;
}

static void removeResults(SArray* pWins, SSHashObj* pUpdatedMap) {
  int32_t size = taosArrayGetSize(pWins);
  for (int32_t i = 0; i < size; i++) {
    SWinKey* pW = taosArrayGet(pWins, i);
    void*    tmp = tSimpleHashGet(pUpdatedMap, pW, sizeof(SWinKey));
    if (tmp) {
      void* value = *(void**)tmp;
      taosMemoryFree(value);
      tSimpleHashRemove(pUpdatedMap, pW, sizeof(SWinKey));
    }
  }
}

static int32_t compareWinKey(void* pKey, void* data, int32_t index) {
  void* pDataPos = taosArrayGet((SArray*)data, index);
  return winKeyCmprImpl(pKey, pDataPos);
}

static void removeDeleteResults(SSHashObj* pUpdatedMap, SArray* pDelWins) {
  taosArraySort(pDelWins, winKeyCmprImpl);
  taosArrayRemoveDuplicate(pDelWins, winKeyCmprImpl, NULL);
  int32_t delSize = taosArrayGetSize(pDelWins);
  if (tSimpleHashGetSize(pUpdatedMap) == 0 || delSize == 0) {
    return;
  }
  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pUpdatedMap, pIte, &iter)) != NULL) {
    SWinKey* pResKey = tSimpleHashGetKey(pIte, NULL);
    int32_t  index = binarySearchCom(pDelWins, delSize, pResKey, TSDB_ORDER_DESC, compareWinKey);
    if (index >= 0 && 0 == compareWinKey(pResKey, pDelWins, index)) {
      taosArrayRemove(pDelWins, index);
      delSize = taosArrayGetSize(pDelWins);
    }
  }
}

bool isOverdue(TSKEY ekey, STimeWindowAggSupp* pTwSup) {
  ASSERTS(pTwSup->maxTs == INT64_MIN || pTwSup->maxTs > 0, "maxts should greater than 0");
  return pTwSup->maxTs != INT64_MIN && ekey < pTwSup->maxTs - pTwSup->waterMark;
}

bool isCloseWindow(STimeWindow* pWin, STimeWindowAggSupp* pTwSup) { return isOverdue(pWin->ekey, pTwSup); }

static bool doDeleteWindow(SOperatorInfo* pOperator, TSKEY ts, uint64_t groupId) {
  SStorageAPI* pAPI = &pOperator->pTaskInfo->storageAPI;

  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  SWinKey                      key = {.ts = ts, .groupId = groupId};
  tSimpleHashRemove(pInfo->aggSup.pResultRowHashTable, &key, sizeof(SWinKey));
  pAPI->stateStore.streamStateDel(pInfo->pState, &key);
  return true;
}

static int32_t getChildIndex(SSDataBlock* pBlock) { return pBlock->info.childId; }

static void doDeleteWindows(SOperatorInfo* pOperator, SInterval* pInterval, SSDataBlock* pBlock, SArray* pUpWins,
                            SSHashObj* pUpdatedMap) {
  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  SColumnInfoData*             pStartTsCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  TSKEY*                       startTsCols = (TSKEY*)pStartTsCol->pData;
  SColumnInfoData*             pEndTsCol = taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
  TSKEY*                       endTsCols = (TSKEY*)pEndTsCol->pData;
  SColumnInfoData*             pCalStTsCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX);
  TSKEY*                       calStTsCols = (TSKEY*)pCalStTsCol->pData;
  SColumnInfoData*             pCalEnTsCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX);
  TSKEY*                       calEnTsCols = (TSKEY*)pCalEnTsCol->pData;
  SColumnInfoData*             pGpCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  uint64_t*                    pGpDatas = (uint64_t*)pGpCol->pData;
  for (int32_t i = 0; i < pBlock->info.rows; i++) {
    SResultRowInfo dumyInfo = {0};
    dumyInfo.cur.pageId = -1;

    STimeWindow win = {0};
    if (IS_FINAL_INTERVAL_OP(pOperator) || IS_MID_INTERVAL_OP(pOperator)) {
      win.skey = startTsCols[i];
      win.ekey = endTsCols[i];
    } else {
      win = getActiveTimeWindow(NULL, &dumyInfo, startTsCols[i], pInterval, TSDB_ORDER_ASC);
    }

    do {
      if (!inCalSlidingWindow(pInterval, &win, calStTsCols[i], calEnTsCols[i], pBlock->info.type)) {
        getNextTimeWindow(pInterval, &win, TSDB_ORDER_ASC);
        continue;
      }
      uint64_t winGpId = pGpDatas[i];
      SWinKey  winRes = {.ts = win.skey, .groupId = winGpId};
      void*    chIds = taosHashGet(pInfo->pPullDataMap, &winRes, sizeof(SWinKey));
      if (chIds) {
        int32_t childId = getChildIndex(pBlock);
        SArray* chArray = *(void**)chIds;
        int32_t index = taosArraySearchIdx(chArray, &childId, compareInt32Val, TD_EQ);
        if (index != -1) {
          qDebug("===stream===try push delete window%" PRId64 "chId:%d ,continue", win.skey, childId);
          getNextTimeWindow(pInterval, &win, TSDB_ORDER_ASC);
          continue;
        }
      }
      bool res = doDeleteWindow(pOperator, win.skey, winGpId);
      if (pUpWins && res) {
        taosArrayPush(pUpWins, &winRes);
      }
      if (pUpdatedMap) {
        tSimpleHashRemove(pUpdatedMap, &winRes, sizeof(SWinKey));
      }
      getNextTimeWindow(pInterval, &win, TSDB_ORDER_ASC);
    } while (win.ekey <= endTsCols[i]);
  }
}

static int32_t getAllIntervalWindow(SSHashObj* pHashMap, SSHashObj* resWins) {
  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pHashMap, pIte, &iter)) != NULL) {
    SWinKey*     pKey = tSimpleHashGetKey(pIte, NULL);
    uint64_t     groupId = pKey->groupId;
    TSKEY        ts = pKey->ts;
    SRowBuffPos* pPos = *(SRowBuffPos**)pIte;
    if (!pPos->beUpdated) {
      continue;
    }
    pPos->beUpdated = false;
    int32_t code = saveWinResultInfo(ts, groupId, pPos, resWins);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t closeStreamIntervalWindow(SSHashObj* pHashMap, STimeWindowAggSupp* pTwSup, SInterval* pInterval,
                                         SHashObj* pPullDataMap, SSHashObj* closeWins, SArray* pDelWins,
                                         SOperatorInfo* pOperator) {
  qDebug("===stream===close interval window");
  void*                        pIte = NULL;
  int32_t                      iter = 0;
  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  int32_t                      delSize = taosArrayGetSize(pDelWins);
  while ((pIte = tSimpleHashIterate(pHashMap, pIte, &iter)) != NULL) {
    void*    key = tSimpleHashGetKey(pIte, NULL);
    SWinKey* pWinKey = (SWinKey*)key;
    if (delSize > 0) {
      int32_t index = binarySearchCom(pDelWins, delSize, pWinKey, TSDB_ORDER_DESC, compareWinKey);
      if (index >= 0 && 0 == compareWinKey(pWinKey, pDelWins, index)) {
        taosArrayRemove(pDelWins, index);
        delSize = taosArrayGetSize(pDelWins);
      }
    }

    void*       chIds = taosHashGet(pPullDataMap, pWinKey, sizeof(SWinKey));
    STimeWindow win = {
        .skey = pWinKey->ts,
        .ekey = taosTimeAdd(win.skey, pInterval->interval, pInterval->intervalUnit, pInterval->precision) - 1,
    };
    if (isCloseWindow(&win, pTwSup)) {
      if (chIds && pPullDataMap) {
        SArray* chAy = *(SArray**)chIds;
        int32_t size = taosArrayGetSize(chAy);
        qDebug("===stream===window %" PRId64 " wait child size:%d", pWinKey->ts, size);
        for (int32_t i = 0; i < size; i++) {
          qDebug("===stream===window %" PRId64 " wait child id:%d", pWinKey->ts, *(int32_t*)taosArrayGet(chAy, i));
        }
        continue;
      } else if (pPullDataMap) {
        qDebug("===stream===close window %" PRId64, pWinKey->ts);
      }

      if (pTwSup->calTrigger == STREAM_TRIGGER_WINDOW_CLOSE) {
        int32_t code = saveWinResult(pWinKey, *(SRowBuffPos**)pIte, closeWins);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
      tSimpleHashIterateRemove(pHashMap, pWinKey, sizeof(SWinKey), &pIte, &iter);
    }
  }
  return TSDB_CODE_SUCCESS;
}

STimeWindow getFinalTimeWindow(int64_t ts, SInterval* pInterval) {
  STimeWindow w = {.skey = ts, .ekey = INT64_MAX};
  w.ekey = taosTimeAdd(w.skey, pInterval->interval, pInterval->intervalUnit, pInterval->precision) - 1;
  return w;
}

static void doBuildDeleteResult(SStreamIntervalOperatorInfo* pInfo, SArray* pWins, int32_t* index,
                                SSDataBlock* pBlock) {
  blockDataCleanup(pBlock);
  int32_t size = taosArrayGetSize(pWins);
  if (*index == size) {
    *index = 0;
    taosArrayClear(pWins);
    return;
  }
  blockDataEnsureCapacity(pBlock, size - *index);
  uint64_t uid = 0;
  for (int32_t i = *index; i < size; i++) {
    SWinKey* pWin = taosArrayGet(pWins, i);
    void*    tbname = NULL;
    pInfo->stateStore.streamStateGetParName(pInfo->pState, pWin->groupId, &tbname);
    if (tbname == NULL) {
      appendOneRowToStreamSpecialBlock(pBlock, &pWin->ts, &pWin->ts, &uid, &pWin->groupId, NULL);
    } else {
      char parTbName[VARSTR_HEADER_SIZE + TSDB_TABLE_NAME_LEN];
      STR_WITH_MAXSIZE_TO_VARSTR(parTbName, tbname, sizeof(parTbName));
      appendOneRowToStreamSpecialBlock(pBlock, &pWin->ts, &pWin->ts, &uid, &pWin->groupId, parTbName);
    }
    pInfo->stateStore.streamStateFreeVal(tbname);
    (*index)++;
  }
}

void clearGroupResInfo(SGroupResInfo* pGroupResInfo) {
  if (pGroupResInfo->freeItem) {
    int32_t size = taosArrayGetSize(pGroupResInfo->pRows);
    for (int32_t i = pGroupResInfo->index; i < size; i++) {
      SRowBuffPos* pPos = taosArrayGetP(pGroupResInfo->pRows, i);
      if (!pPos->needFree && !pPos->pRowBuff) {
        taosMemoryFreeClear(pPos->pKey);
        taosMemoryFree(pPos);
      }
    }
    pGroupResInfo->freeItem = false;
  }
  pGroupResInfo->pRows = taosArrayDestroy(pGroupResInfo->pRows);
  pGroupResInfo->index = 0;
}

void destroyStreamFinalIntervalOperatorInfo(void* param) {
  SStreamIntervalOperatorInfo* pInfo = (SStreamIntervalOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);
  cleanupAggSup(&pInfo->aggSup);
  clearGroupResInfo(&pInfo->groupResInfo);

  // it should be empty.
  void* pIte = NULL;
  while ((pIte = taosHashIterate(pInfo->pPullDataMap, pIte)) != NULL) {
    taosArrayDestroy(*(void**)pIte);
  }
  taosHashCleanup(pInfo->pPullDataMap);
  taosHashCleanup(pInfo->pFinalPullDataMap);
  taosArrayDestroy(pInfo->pPullWins);
  blockDataDestroy(pInfo->pPullDataRes);
  taosArrayDestroy(pInfo->pDelWins);
  blockDataDestroy(pInfo->pDelRes);
  blockDataDestroy(pInfo->pMidRetriveRes);
  blockDataDestroy(pInfo->pMidPulloverRes);
  pInfo->stateStore.streamFileStateDestroy(pInfo->pState->pFileState);

  if (pInfo->pState->dump == 1) {
    taosMemoryFreeClear(pInfo->pState->pTdbState->pOwner);
    taosMemoryFreeClear(pInfo->pState->pTdbState);
  }
  taosMemoryFreeClear(pInfo->pState);

  nodesDestroyNode((SNode*)pInfo->pPhyNode);
  colDataDestroy(&pInfo->twAggSup.timeWindowData);
  cleanupExprSupp(&pInfo->scalarSupp);
  tSimpleHashCleanup(pInfo->pUpdatedMap);
  pInfo->pUpdatedMap = NULL;
  pInfo->pUpdated = taosArrayDestroy(pInfo->pUpdated);

  blockDataDestroy(pInfo->pCheckpointRes);

  taosMemoryFreeClear(param);
}

#ifdef BUILD_NO_CALL
static bool allInvertible(SqlFunctionCtx* pFCtx, int32_t numOfCols) {
  for (int32_t i = 0; i < numOfCols; i++) {
    if (fmIsUserDefinedFunc(pFCtx[i].functionId) || !fmIsInvertible(pFCtx[i].functionId)) {
      return false;
    }
  }
  return true;
}
#endif

void reloadFromDownStream(SOperatorInfo* downstream, SStreamIntervalOperatorInfo* pInfo) {
  SStateStore* pAPI = &downstream->pTaskInfo->storageAPI.stateStore;

  if (downstream->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    reloadFromDownStream(downstream->pDownstream[0], pInfo);
    return;
  }

  SStreamScanInfo* pScanInfo = downstream->info;
  pInfo->pUpdateInfo = pScanInfo->pUpdateInfo;
}

void initIntervalDownStream(SOperatorInfo* downstream, uint16_t type, SStreamIntervalOperatorInfo* pInfo) {
  SStateStore* pAPI = &downstream->pTaskInfo->storageAPI.stateStore;

  if (downstream->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    initIntervalDownStream(downstream->pDownstream[0], type, pInfo);
    return;
  }

  SStreamScanInfo* pScanInfo = downstream->info;
  pScanInfo->windowSup.parentType = type;
  pScanInfo->windowSup.pIntervalAggSup = &pInfo->aggSup;
  if (!pScanInfo->pUpdateInfo) {
    pScanInfo->pUpdateInfo =
        pAPI->updateInfoInitP(&pInfo->interval, pInfo->twAggSup.waterMark, pScanInfo->igCheckUpdate);
  }

  pScanInfo->interval = pInfo->interval;
  pScanInfo->twAggSup = pInfo->twAggSup;
  pScanInfo->pState = pInfo->pState;
  pInfo->pUpdateInfo = pScanInfo->pUpdateInfo;
}

void compactFunctions(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx, int32_t numOfOutput,
                      SExecTaskInfo* pTaskInfo, SColumnInfoData* pTimeWindowData) {
  for (int32_t k = 0; k < numOfOutput; ++k) {
    if (fmIsWindowPseudoColumnFunc(pDestCtx[k].functionId)) {
      if (!pTimeWindowData) {
        continue;
      }

      SResultRowEntryInfo* pEntryInfo = GET_RES_INFO(&pDestCtx[k]);
      char*                p = GET_ROWCELL_INTERBUF(pEntryInfo);
      SColumnInfoData      idata = {0};
      idata.info.type = TSDB_DATA_TYPE_BIGINT;
      idata.info.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
      idata.pData = p;

      SScalarParam out = {.columnData = &idata};
      SScalarParam tw = {.numOfRows = 5, .columnData = pTimeWindowData};
      pDestCtx[k].sfp.process(&tw, 1, &out);
      pEntryInfo->numOfRes = 1;
    } else if (functionNeedToExecute(&pDestCtx[k]) && pDestCtx[k].fpSet.combine != NULL) {
      int32_t code = pDestCtx[k].fpSet.combine(&pDestCtx[k], &pSourceCtx[k]);
      if (code != TSDB_CODE_SUCCESS) {
        qError("%s apply combine functions error, code: %s", GET_TASKID(pTaskInfo), tstrerror(code));
      }
    } else if (pDestCtx[k].fpSet.combine == NULL) {
      char* funName = fmGetFuncName(pDestCtx[k].functionId);
      qError("%s error, combine funcion for %s is not implemented", GET_TASKID(pTaskInfo), funName);
      taosMemoryFreeClear(funName);
    }
  }
}

bool hasIntervalWindow(void* pState, SWinKey* pKey, SStateStore* pStore) {
  return pStore->streamStateCheck(pState, pKey);
}

int32_t setIntervalOutputBuf(void* pState, STimeWindow* win, SRowBuffPos** pResult, int64_t groupId,
                             SqlFunctionCtx* pCtx, int32_t numOfOutput, int32_t* rowEntryInfoOffset,
                             SAggSupporter* pAggSup, SStateStore* pStore) {
  SWinKey key = {.ts = win->skey, .groupId = groupId};
  char*   value = NULL;
  int32_t size = pAggSup->resultRowSize;

  if (pStore->streamStateAddIfNotExist(pState, &key, (void**)&value, &size) < 0) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  *pResult = (SRowBuffPos*)value;
  SResultRow* res = (SResultRow*)((*pResult)->pRowBuff);

  // set time window for current result
  res->win = (*win);
  setResultRowInitCtx(res, pCtx, numOfOutput, rowEntryInfoOffset);
  return TSDB_CODE_SUCCESS;
}

bool isDeletedStreamWindow(STimeWindow* pWin, uint64_t groupId, void* pState, STimeWindowAggSupp* pTwSup,
                           SStateStore* pStore) {
  if (pTwSup->maxTs != INT64_MIN && pWin->ekey < pTwSup->maxTs - pTwSup->deleteMark) {
    SWinKey key = {.ts = pWin->skey, .groupId = groupId};
    if (!hasIntervalWindow(pState, &key, pStore)) {
      return true;
    }
    return false;
  }
  return false;
}

int32_t getNexWindowPos(SInterval* pInterval, SDataBlockInfo* pBlockInfo, TSKEY* tsCols, int32_t startPos, TSKEY eKey,
                        STimeWindow* pNextWin) {
  int32_t forwardRows =
      getNumOfRowsInTimeWindow(pBlockInfo, tsCols, startPos, eKey, binarySearchForKey, NULL, TSDB_ORDER_ASC);
  int32_t prevEndPos = forwardRows - 1 + startPos;
  return getNextQualifiedWindow(pInterval, pNextWin, pBlockInfo, tsCols, prevEndPos, TSDB_ORDER_ASC);
}

void addPullWindow(SHashObj* pMap, SWinKey* pWinRes, int32_t size) {
  SArray* childIds = taosArrayInit(8, sizeof(int32_t));
  for (int32_t i = 0; i < size; i++) {
    taosArrayPush(childIds, &i);
  }
  taosHashPut(pMap, pWinRes, sizeof(SWinKey), &childIds, sizeof(void*));
}

static void clearStreamIntervalOperator(SStreamIntervalOperatorInfo* pInfo) {
  tSimpleHashClear(pInfo->aggSup.pResultRowHashTable);
  clearDiskbasedBuf(pInfo->aggSup.pResultBuf);
  initResultRowInfo(&pInfo->binfo.resultRowInfo);
  pInfo->aggSup.currentPageId = -1;
  pInfo->stateStore.streamStateClear(pInfo->pState);
}

static void clearSpecialDataBlock(SSDataBlock* pBlock) {
  if (pBlock->info.rows <= 0) {
    return;
  }
  blockDataCleanup(pBlock);
}

static void doBuildPullDataBlock(SArray* array, int32_t* pIndex, SSDataBlock* pBlock) {
  clearSpecialDataBlock(pBlock);
  int32_t size = taosArrayGetSize(array);
  if (size - (*pIndex) == 0) {
    return;
  }
  blockDataEnsureCapacity(pBlock, size - (*pIndex));
  SColumnInfoData* pStartTs = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  SColumnInfoData* pEndTs = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
  SColumnInfoData* pGroupId = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  SColumnInfoData* pCalStartTs = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX);
  SColumnInfoData* pCalEndTs = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX);
  for (; (*pIndex) < size; (*pIndex)++) {
    SPullWindowInfo* pWin = taosArrayGet(array, (*pIndex));
    colDataSetVal(pStartTs, pBlock->info.rows, (const char*)&pWin->window.skey, false);
    colDataSetVal(pEndTs, pBlock->info.rows, (const char*)&pWin->window.ekey, false);
    colDataSetVal(pGroupId, pBlock->info.rows, (const char*)&pWin->groupId, false);
    colDataSetVal(pCalStartTs, pBlock->info.rows, (const char*)&pWin->calWin.skey, false);
    colDataSetVal(pCalEndTs, pBlock->info.rows, (const char*)&pWin->calWin.ekey, false);
    pBlock->info.rows++;
  }
  if ((*pIndex) == size) {
    *pIndex = 0;
    taosArrayClear(array);
  }
  blockDataUpdateTsWindow(pBlock, 0);
}

static bool processPullOver(SSDataBlock* pBlock, SHashObj* pMap, SHashObj* pFinalMap, SInterval* pInterval, SArray* pPullWins,
                     int32_t numOfCh, SOperatorInfo* pOperator) {
  SColumnInfoData* pStartCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX);
  TSKEY*           tsData = (TSKEY*)pStartCol->pData;
  SColumnInfoData* pEndCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX);
  TSKEY*           tsEndData = (TSKEY*)pEndCol->pData;
  SColumnInfoData* pGroupCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  uint64_t*        groupIdData = (uint64_t*)pGroupCol->pData;
  int32_t          chId = getChildIndex(pBlock);
  bool             res = false;
  for (int32_t i = 0; i < pBlock->info.rows; i++) {
    TSKEY winTs = tsData[i];
    while (winTs <= tsEndData[i]) {
      SWinKey winRes = {.ts = winTs, .groupId = groupIdData[i]};
      void*   chIds = taosHashGet(pMap, &winRes, sizeof(SWinKey));
      if (chIds) {
        SArray* chArray = *(SArray**)chIds;
        int32_t index = taosArraySearchIdx(chArray, &chId, compareInt32Val, TD_EQ);
        if (index != -1) {
          qDebug("===stream===retrive window %" PRId64 " delete child id %d", winRes.ts, chId);
          taosArrayRemove(chArray, index);
          if (taosArrayGetSize(chArray) == 0) {
            // pull data is over
            taosArrayDestroy(chArray);
            taosHashRemove(pMap, &winRes, sizeof(SWinKey));
            res =true;
            qDebug("===stream===retrive pull data over.window %" PRId64, winRes.ts);

            void* pFinalCh = taosHashGet(pFinalMap, &winRes, sizeof(SWinKey));
            if (pFinalCh) {
              taosHashRemove(pFinalMap, &winRes, sizeof(SWinKey));
              doDeleteWindow(pOperator, winRes.ts, winRes.groupId);
              STimeWindow     nextWin = getFinalTimeWindow(winRes.ts, pInterval);
              SPullWindowInfo pull = {.window = nextWin,
                                      .groupId = winRes.groupId,
                                      .calWin.skey = nextWin.skey,
                                      .calWin.ekey = nextWin.skey};
              // add pull data request
              if (savePullWindow(&pull, pPullWins) == TSDB_CODE_SUCCESS) {
                addPullWindow(pMap, &winRes, numOfCh);
                qDebug("===stream===prepare final retrive for delete %" PRId64 ", size:%d", winRes.ts, numOfCh);
              }
            }
          }
        }
      }
      winTs = taosTimeAdd(winTs, pInterval->sliding, pInterval->slidingUnit, pInterval->precision);
    }
  }
  return res;
}

static void addRetriveWindow(SArray* wins, SStreamIntervalOperatorInfo* pInfo, int32_t childId) {
  int32_t size = taosArrayGetSize(wins);
  for (int32_t i = 0; i < size; i++) {
    SWinKey*    winKey = taosArrayGet(wins, i);
    STimeWindow nextWin = getFinalTimeWindow(winKey->ts, &pInfo->interval);
    void* chIds = taosHashGet(pInfo->pPullDataMap, winKey, sizeof(SWinKey));
    if (!chIds) {
      SPullWindowInfo pull = {
          .window = nextWin, .groupId = winKey->groupId, .calWin.skey = nextWin.skey, .calWin.ekey = nextWin.skey};
      // add pull data request
      if (savePullWindow(&pull, pInfo->pPullWins) == TSDB_CODE_SUCCESS) {
        addPullWindow(pInfo->pPullDataMap, winKey, pInfo->numOfChild);
        qDebug("===stream===prepare retrive for delete %" PRId64 ", size:%d", winKey->ts, pInfo->numOfChild);
      }
    } else {
      SArray* chArray = *(void**)chIds;
      int32_t index = taosArraySearchIdx(chArray, &childId, compareInt32Val, TD_EQ);
      qDebug("===stream===check final retrive %" PRId64 ",chid:%d", winKey->ts, index);
      if (index == -1) {
        qDebug("===stream===add final retrive %" PRId64, winKey->ts);
        taosHashPut(pInfo->pFinalPullDataMap, winKey, sizeof(SWinKey), NULL, 0);
      }
    }
  }
}

static void clearFunctionContext(SExprSupp* pSup) {
  for (int32_t i = 0; i < pSup->numOfExprs; i++) {
    pSup->pCtx[i].saveHandle.currentPage = -1;
  }
}

int32_t getOutputBuf(void* pState, SRowBuffPos* pPos, SResultRow** pResult, SStateStore* pStore) {
  return pStore->streamStateGetByPos(pState, pPos, (void**)pResult);
}

int32_t buildDataBlockFromGroupRes(SOperatorInfo* pOperator, void* pState, SSDataBlock* pBlock, SExprSupp* pSup,
                                   SGroupResInfo* pGroupResInfo) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*   pAPI = &pOperator->pTaskInfo->storageAPI;

  SExprInfo*      pExprInfo = pSup->pExprInfo;
  int32_t         numOfExprs = pSup->numOfExprs;
  int32_t*        rowEntryOffset = pSup->rowEntryInfoOffset;
  SqlFunctionCtx* pCtx = pSup->pCtx;

  int32_t numOfRows = getNumOfTotalRes(pGroupResInfo);

  for (int32_t i = pGroupResInfo->index; i < numOfRows; i += 1) {
    SRowBuffPos* pPos = *(SRowBuffPos**)taosArrayGet(pGroupResInfo->pRows, i);
    SResultRow*  pRow = NULL;
    int32_t      code = getOutputBuf(pState, pPos, &pRow, &pAPI->stateStore);
    uint64_t     groupId = ((SWinKey*)pPos->pKey)->groupId;
    ASSERT(code == 0);
    doUpdateNumOfRows(pCtx, pRow, numOfExprs, rowEntryOffset);
    // no results, continue to check the next one
    if (pRow->numOfRows == 0) {
      pGroupResInfo->index += 1;
      continue;
    }
    if (pBlock->info.id.groupId == 0) {
      pBlock->info.id.groupId = groupId;
      void* tbname = NULL;
      if (pAPI->stateStore.streamStateGetParName(pTaskInfo->streamInfo.pState, pBlock->info.id.groupId, &tbname) < 0) {
        pBlock->info.parTbName[0] = 0;
      } else {
        memcpy(pBlock->info.parTbName, tbname, TSDB_TABLE_NAME_LEN);
      }
      pAPI->stateStore.streamStateFreeVal(tbname);
    } else {
      // current value belongs to different group, it can't be packed into one datablock
      if (pBlock->info.id.groupId != groupId) {
        break;
      }
    }

    if (pBlock->info.rows + pRow->numOfRows > pBlock->info.capacity) {
      ASSERT(pBlock->info.rows > 0);
      break;
    }
    pGroupResInfo->index += 1;

    for (int32_t j = 0; j < numOfExprs; ++j) {
      int32_t slotId = pExprInfo[j].base.resSchema.slotId;

      pCtx[j].resultInfo = getResultEntryInfo(pRow, j, rowEntryOffset);

      if (pCtx[j].fpSet.finalize) {
        int32_t code1 = pCtx[j].fpSet.finalize(&pCtx[j], pBlock);
        if (TAOS_FAILED(code1)) {
          qError("%s build result data block error, code %s", GET_TASKID(pTaskInfo), tstrerror(code1));
          T_LONG_JMP(pTaskInfo->env, code1);
        }
      } else if (strcmp(pCtx[j].pExpr->pExpr->_function.functionName, "_select_value") == 0) {
        // do nothing, todo refactor
      } else {
        // expand the result into multiple rows. E.g., _wstart, top(k, 20)
        // the _wstart needs to copy to 20 following rows, since the results of top-k expands to 20 different rows.
        SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, slotId);
        char*            in = GET_ROWCELL_INTERBUF(pCtx[j].resultInfo);
        for (int32_t k = 0; k < pRow->numOfRows; ++k) {
          colDataSetVal(pColInfoData, pBlock->info.rows + k, in, pCtx[j].resultInfo->isNullRes);
        }
      }
    }

    pBlock->info.rows += pRow->numOfRows;
  }

  pBlock->info.dataLoad = 1;
  blockDataUpdateTsWindow(pBlock, 0);
  return TSDB_CODE_SUCCESS;
}

void doBuildStreamIntervalResult(SOperatorInfo* pOperator, void* pState, SSDataBlock* pBlock,
                                 SGroupResInfo* pGroupResInfo) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  // set output datablock version
  pBlock->info.version = pTaskInfo->version;

  blockDataCleanup(pBlock);
  if (!hasRemainResults(pGroupResInfo)) {
    return;
  }

  // clear the existed group id
  pBlock->info.id.groupId = 0;
  buildDataBlockFromGroupRes(pOperator, pState, pBlock, &pOperator->exprSupp, pGroupResInfo);
}

static int32_t getNextQualifiedFinalWindow(SInterval* pInterval, STimeWindow* pNext, SDataBlockInfo* pDataBlockInfo,
                                           TSKEY* primaryKeys, int32_t prevPosition) {
  int32_t startPos = prevPosition + 1;
  if (startPos == pDataBlockInfo->rows) {
    startPos = -1;
  } else {
    *pNext = getFinalTimeWindow(primaryKeys[startPos], pInterval);
  }
  return startPos;
}

static void doStreamIntervalAggImpl(SOperatorInfo* pOperator, SSDataBlock* pSDataBlock, uint64_t groupId,
                                    SSHashObj* pUpdatedMap) {
  SStreamIntervalOperatorInfo* pInfo = (SStreamIntervalOperatorInfo*)pOperator->info;
  pInfo->dataVersion = TMAX(pInfo->dataVersion, pSDataBlock->info.version);

  SResultRowInfo* pResultRowInfo = &(pInfo->binfo.resultRowInfo);
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*      pSup = &pOperator->exprSupp;
  int32_t         numOfOutput = pSup->numOfExprs;
  int32_t         step = 1;
  TSKEY*          tsCols = NULL;
  SRowBuffPos*    pResPos = NULL;
  SResultRow*     pResult = NULL;
  int32_t         forwardRows = 0;
  int32_t         endRowId = pSDataBlock->info.rows - 1;

  SColumnInfoData* pColDataInfo = taosArrayGet(pSDataBlock->pDataBlock, pInfo->primaryTsIndex);
  tsCols = (int64_t*)pColDataInfo->pData;

  if (pSDataBlock->info.window.skey != tsCols[0] || pSDataBlock->info.window.ekey != tsCols[endRowId]) {
    qError("table uid %" PRIu64 " data block timestamp range may not be calculated! minKey %" PRId64
            ",maxKey %" PRId64,
            pSDataBlock->info.id.uid, pSDataBlock->info.window.skey, pSDataBlock->info.window.ekey);
    blockDataUpdateTsWindow(pSDataBlock, pInfo->primaryTsIndex);

    // timestamp of the data is incorrect
    if (pSDataBlock->info.window.skey <= 0 || pSDataBlock->info.window.ekey <= 0) {
      qError("table uid %" PRIu64 " data block timestamp is out of range! minKey %" PRId64 ",maxKey %" PRId64,
              pSDataBlock->info.id.uid, pSDataBlock->info.window.skey, pSDataBlock->info.window.ekey);
    }
  }

  int32_t     startPos = 0;
  TSKEY       ts = getStartTsKey(&pSDataBlock->info.window, tsCols);
  STimeWindow nextWin = {0};
  if (IS_FINAL_INTERVAL_OP(pOperator)) {
    nextWin = getFinalTimeWindow(ts, &pInfo->interval);
  } else {
    nextWin = getActiveTimeWindow(pInfo->aggSup.pResultBuf, pResultRowInfo, ts, &pInfo->interval, TSDB_ORDER_ASC);
  }
  while (1) {
    bool isClosed = isCloseWindow(&nextWin, &pInfo->twAggSup);
    if ((!IS_FINAL_INTERVAL_OP(pOperator) && pInfo->ignoreExpiredData && pSDataBlock->info.type != STREAM_PULL_DATA &&
         checkExpiredData(&pInfo->stateStore, pInfo->pUpdateInfo, &pInfo->twAggSup, pSDataBlock->info.id.uid,
                          nextWin.ekey)) ||
        !inSlidingWindow(&pInfo->interval, &nextWin, &pSDataBlock->info)) {
      startPos = getNexWindowPos(&pInfo->interval, &pSDataBlock->info, tsCols, startPos, nextWin.ekey, &nextWin);
      if (startPos < 0) {
        break;
      }
      qDebug("===stream===ignore expired data, window end ts:%" PRId64 ", maxts - wartermak:%" PRId64, nextWin.ekey,
             pInfo->twAggSup.maxTs - pInfo->twAggSup.waterMark);
      continue;
    }

    if (IS_FINAL_INTERVAL_OP(pOperator) && pInfo->numOfChild > 0) {
      bool    ignore = true;
      SWinKey winRes = {
          .ts = nextWin.skey,
          .groupId = groupId,
      };
      void* chIds = taosHashGet(pInfo->pPullDataMap, &winRes, sizeof(SWinKey));
      if (isDeletedStreamWindow(&nextWin, groupId, pInfo->pState, &pInfo->twAggSup, &pInfo->stateStore) && isClosed &&
          !chIds) {
        SPullWindowInfo pull = {
            .window = nextWin, .groupId = groupId, .calWin.skey = nextWin.skey, .calWin.ekey = nextWin.skey};
        // add pull data request
        if (savePullWindow(&pull, pInfo->pPullWins) == TSDB_CODE_SUCCESS) {
          addPullWindow(pInfo->pPullDataMap, &winRes, pInfo->numOfChild);
        }
      } else {
        int32_t index = -1;
        SArray* chArray = NULL;
        int32_t chId = 0;
        if (chIds) {
          chArray = *(void**)chIds;
          chId = getChildIndex(pSDataBlock);
          index = taosArraySearchIdx(chArray, &chId, compareInt32Val, TD_EQ);
        }
        if (index == -1 || pSDataBlock->info.type == STREAM_PULL_DATA) {
          ignore = false;
        }
      }

      if (ignore) {
        startPos = getNextQualifiedFinalWindow(&pInfo->interval, &nextWin, &pSDataBlock->info, tsCols, startPos);
        if (startPos < 0) {
          break;
        }
        continue;
      }
    }

    int32_t code = setIntervalOutputBuf(pInfo->pState, &nextWin, &pResPos, groupId, pSup->pCtx, numOfOutput,
                                        pSup->rowEntryInfoOffset, &pInfo->aggSup, &pInfo->stateStore);
    pResult = (SResultRow*)pResPos->pRowBuff;
    if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
      qError("%s set interval output buff error, code %s", GET_TASKID(pTaskInfo), tstrerror(code));
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_OUT_OF_MEMORY);
    }
    if (IS_FINAL_INTERVAL_OP(pOperator)) {
      forwardRows = 1;
    } else {
      forwardRows = getNumOfRowsInTimeWindow(&pSDataBlock->info, tsCols, startPos, nextWin.ekey, binarySearchForKey,
                                             NULL, TSDB_ORDER_ASC);
    }

    SWinKey key = {
        .ts = pResult->win.skey,
        .groupId = groupId,
    };
    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE && pUpdatedMap) {
      saveWinResult(&key, pResPos, pUpdatedMap);
    }

    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_WINDOW_CLOSE) {
      pResPos->beUpdated = true;
      tSimpleHashPut(pInfo->aggSup.pResultRowHashTable, &key, sizeof(SWinKey), &pResPos, POINTER_BYTES);
    }

    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &nextWin, 1);
    applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, startPos, forwardRows,
                                    pSDataBlock->info.rows, numOfOutput);
    key.ts = nextWin.skey;

    if (pInfo->delKey.ts > key.ts) {
      pInfo->delKey = key;
    }
    int32_t prevEndPos = (forwardRows - 1) * step + startPos;
    if (IS_FINAL_INTERVAL_OP(pOperator)) {
      startPos = getNextQualifiedFinalWindow(&pInfo->interval, &nextWin, &pSDataBlock->info, tsCols, prevEndPos);
    } else {
      startPos =
          getNextQualifiedWindow(&pInfo->interval, &nextWin, &pSDataBlock->info, tsCols, prevEndPos, TSDB_ORDER_ASC);
    }
    if (startPos < 0) {
      break;
    }
  }
}

static inline int winPosCmprImpl(const void* pKey1, const void* pKey2) {
  SRowBuffPos* pos1 = *(SRowBuffPos**)pKey1;
  SRowBuffPos* pos2 = *(SRowBuffPos**)pKey2;
  SWinKey*     pWin1 = (SWinKey*)pos1->pKey;
  SWinKey*     pWin2 = (SWinKey*)pos2->pKey;

  if (pWin1->groupId > pWin2->groupId) {
    return 1;
  } else if (pWin1->groupId < pWin2->groupId) {
    return -1;
  }

  if (pWin1->ts > pWin2->ts) {
    return 1;
  } else if (pWin1->ts < pWin2->ts) {
    return -1;
  }

  return 0;
}

static void resetUnCloseWinInfo(SSHashObj* winMap) {
  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(winMap, pIte, &iter)) != NULL) {
    SRowBuffPos* pPos = *(SRowBuffPos**)pIte;
    pPos->beUsed = true;
  }
}

int32_t encodeSWinKey(void** buf, SWinKey* key) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI64(buf, key->ts);
  tlen += taosEncodeFixedU64(buf, key->groupId);
  return tlen;
}

void* decodeSWinKey(void* buf, SWinKey* key) {
  buf = taosDecodeFixedI64(buf, &key->ts);
  buf = taosDecodeFixedU64(buf, &key->groupId);
  return buf;
}

int32_t encodeSTimeWindowAggSupp(void** buf, STimeWindowAggSupp* pTwAggSup) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI64(buf, pTwAggSup->minTs);
  tlen += taosEncodeFixedI64(buf, pTwAggSup->maxTs);
  return tlen;
}

void* decodeSTimeWindowAggSupp(void* buf, STimeWindowAggSupp* pTwAggSup) {
  buf = taosDecodeFixedI64(buf, &pTwAggSup->minTs);
  buf = taosDecodeFixedI64(buf, &pTwAggSup->maxTs);
  return buf;
}

int32_t encodeSTimeWindow(void** buf, STimeWindow* pWin) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI64(buf, pWin->skey);
  tlen += taosEncodeFixedI64(buf, pWin->ekey);
  return tlen;
}

void* decodeSTimeWindow(void* buf, STimeWindow* pWin) {
  buf = taosDecodeFixedI64(buf, &pWin->skey);
  buf = taosDecodeFixedI64(buf, &pWin->ekey);
  return buf;
}

int32_t encodeSPullWindowInfo(void** buf, SPullWindowInfo* pPullInfo) {
  int32_t tlen = 0;
  tlen += encodeSTimeWindow(buf, &pPullInfo->calWin);
  tlen += taosEncodeFixedU64(buf, pPullInfo->groupId);
  tlen += encodeSTimeWindow(buf, &pPullInfo->window);
  return tlen;
}

void* decodeSPullWindowInfo(void* buf, SPullWindowInfo* pPullInfo) {
  buf = decodeSTimeWindow(buf, &pPullInfo->calWin);
  buf = taosDecodeFixedU64(buf, &pPullInfo->groupId);
  buf = decodeSTimeWindow(buf, &pPullInfo->window);
  return buf;
}

int32_t encodeSPullWindowInfoArray(void** buf, SArray* pPullInfos) {
  int32_t tlen = 0;
  int32_t size = taosArrayGetSize(pPullInfos);
  tlen += taosEncodeFixedI32(buf, size);
  for (int32_t i = 0; i < size; i++) {
    void* pItem = taosArrayGet(pPullInfos, i);
    tlen += encodeSPullWindowInfo(buf, pItem);
  }
  return tlen;
}

void* decodeSPullWindowInfoArray(void* buf, SArray* pPullInfos) {
  int32_t size = 0;
  buf = taosDecodeFixedI32(buf, &size);
  for (int32_t i = 0; i < size; i++) {
    SPullWindowInfo item = {0};
    buf = decodeSPullWindowInfo(buf, &item);
    taosArrayPush(pPullInfos, &item);
  }
  return buf;
}

int32_t doStreamIntervalEncodeOpState(void** buf, int32_t len, SOperatorInfo* pOperator) {
  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  if (!pInfo) {
    return 0;
  }

  void* pData = (buf == NULL) ? NULL : *buf;

  // 1.pResultRowHashTable
  int32_t tlen = 0;
  int32_t mapSize = tSimpleHashGetSize(pInfo->aggSup.pResultRowHashTable);
  tlen += taosEncodeFixedI32(buf, mapSize);
  void*   pIte = NULL;
  size_t  keyLen = 0;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pInfo->aggSup.pResultRowHashTable, pIte, &iter)) != NULL) {
    void* key = tSimpleHashGetKey(pIte, &keyLen);
    tlen += encodeSWinKey(buf, key);
  }

  // 2.twAggSup
  tlen += encodeSTimeWindowAggSupp(buf, &pInfo->twAggSup);

  // 3.pPullDataMap
  int32_t size = taosHashGetSize(pInfo->pPullDataMap);
  tlen += taosEncodeFixedI32(buf, size);
  pIte = NULL;
  keyLen = 0;
  while ((pIte = taosHashIterate(pInfo->pPullDataMap, pIte)) != NULL) {
    void* key = taosHashGetKey(pIte, &keyLen);
    tlen += encodeSWinKey(buf, key);
    SArray* pArray = *(SArray**)pIte;
    int32_t chSize = taosArrayGetSize(pArray);
    tlen += taosEncodeFixedI32(buf, chSize);
    for (int32_t i = 0; i < chSize; i++) {
      void* pChItem = taosArrayGet(pArray, i);
      tlen += taosEncodeFixedI32(buf, *(int32_t*)pChItem);
    }
  }

  // 4.pPullWins
  tlen += encodeSPullWindowInfoArray(buf, pInfo->pPullWins);

  // 5.dataVersion
  tlen += taosEncodeFixedI64(buf, pInfo->dataVersion);

  // 6.checksum
  if (buf) {
    uint32_t cksum = taosCalcChecksum(0, pData, len - sizeof(uint32_t));
    tlen += taosEncodeFixedU32(buf, cksum);
  } else {
    tlen += sizeof(uint32_t);
  }

  return tlen;
}

void doStreamIntervalDecodeOpState(void* buf, int32_t len, SOperatorInfo* pOperator) {
  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  if (!pInfo) {
    return;
  }

  // 6.checksum
  int32_t dataLen = len - sizeof(uint32_t);
  void*   pCksum = POINTER_SHIFT(buf, dataLen);
  if (taosCheckChecksum(buf, dataLen, *(uint32_t*)pCksum) != TSDB_CODE_SUCCESS) {
    qError("stream interval state is invalid");
    return;
  }

  // 1.pResultRowHashTable
  int32_t mapSize = 0;
  buf = taosDecodeFixedI32(buf, &mapSize);
  for (int32_t i = 0; i < mapSize; i++) {
    SWinKey key = {0};
    buf = decodeSWinKey(buf, &key);
    SRowBuffPos* pPos = NULL;
    int32_t      resSize = pInfo->aggSup.resultRowSize;
    pInfo->stateStore.streamStateAddIfNotExist(pInfo->pState, &key, (void**)&pPos, &resSize);
    tSimpleHashPut(pInfo->aggSup.pResultRowHashTable, &key, sizeof(SWinKey), &pPos, POINTER_BYTES);
  }

  // 2.twAggSup
  buf = decodeSTimeWindowAggSupp(buf, &pInfo->twAggSup);

  // 3.pPullDataMap
  int32_t size = 0;
  buf = taosDecodeFixedI32(buf, &size);
  for (int32_t i = 0; i < size; i++) {
    SWinKey key = {0};
    SArray* pArray = taosArrayInit(0, sizeof(int32_t));
    buf = decodeSWinKey(buf, &key);
    int32_t chSize = 0;
    buf = taosDecodeFixedI32(buf, &chSize);
    for (int32_t i = 0; i < chSize; i++) {
      int32_t chId = 0;
      buf = taosDecodeFixedI32(buf, &chId);
      taosArrayPush(pArray, &chId);
    }
    taosHashPut(pInfo->pPullDataMap, &key, sizeof(SWinKey), &pArray, POINTER_BYTES);
  }

  // 4.pPullWins
  buf = decodeSPullWindowInfoArray(buf, pInfo->pPullWins);

  // 5.dataVersion
  buf = taosDecodeFixedI64(buf, &pInfo->dataVersion);
}

void doStreamIntervalSaveCheckpoint(SOperatorInfo* pOperator) {
  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  int32_t                      len = doStreamIntervalEncodeOpState(NULL, 0, pOperator);
  void*                        buf = taosMemoryCalloc(1, len);
  void*                        pBuf = buf;
  len = doStreamIntervalEncodeOpState(&pBuf, len, pOperator);
  pInfo->stateStore.streamStateSaveInfo(pInfo->pState, STREAM_INTERVAL_OP_CHECKPOINT_NAME,
                                        strlen(STREAM_INTERVAL_OP_CHECKPOINT_NAME), buf, len);
  taosMemoryFree(buf);
}

static SSDataBlock* buildIntervalResult(SOperatorInfo* pOperator) {
  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  uint16_t                     opType = pOperator->operatorType;
  if (IS_FINAL_INTERVAL_OP(pOperator)) {
    doBuildPullDataBlock(pInfo->pPullWins, &pInfo->pullIndex, pInfo->pPullDataRes);
    if (pInfo->pPullDataRes->info.rows != 0) {
      // process the rest of the data
      printDataBlock(pInfo->pPullDataRes, getStreamOpName(opType), GET_TASKID(pTaskInfo));
      return pInfo->pPullDataRes;
    }
  }

  doBuildDeleteResult(pInfo, pInfo->pDelWins, &pInfo->delIndex, pInfo->pDelRes);
  if (pInfo->pDelRes->info.rows != 0) {
    // process the rest of the data
    printDataBlock(pInfo->pDelRes, getStreamOpName(opType), GET_TASKID(pTaskInfo));
    return pInfo->pDelRes;
  }

  doBuildStreamIntervalResult(pOperator, pInfo->pState, pInfo->binfo.pRes, &pInfo->groupResInfo);
  if (pInfo->binfo.pRes->info.rows != 0) {
    printDataBlock(pInfo->binfo.pRes, getStreamOpName(opType), GET_TASKID(pTaskInfo));
    return pInfo->binfo.pRes;
  }

  if (pInfo->recvPullover) {
    pInfo->recvPullover = false;
    printDataBlock(pInfo->pMidPulloverRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
    return pInfo->pMidPulloverRes;
  }
  return NULL;
}

int32_t copyUpdateResult(SSHashObj** ppWinUpdated, SArray* pUpdated, __compar_fn_t compar) {
  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(*ppWinUpdated, pIte, &iter)) != NULL) {
    taosArrayPush(pUpdated, pIte);
  }
  taosArraySort(pUpdated, compar);
  tSimpleHashCleanup(*ppWinUpdated);
  *ppWinUpdated = NULL;
  return TSDB_CODE_SUCCESS;
}

static SSDataBlock* doStreamFinalIntervalAgg(SOperatorInfo* pOperator) {
  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*                 pAPI = &pOperator->pTaskInfo->storageAPI;

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  SExprSupp*     pSup = &pOperator->exprSupp;

  qDebug("stask:%s  %s status: %d", GET_TASKID(pTaskInfo), getStreamOpName(pOperator->operatorType), pOperator->status);

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  } else if (pOperator->status == OP_RES_TO_RETURN) {
    SSDataBlock* resBlock = buildIntervalResult(pOperator);
    if (resBlock != NULL) {
      return resBlock;
    }

    if (pInfo->recvGetAll) {
      pInfo->recvGetAll = false;
      resetUnCloseWinInfo(pInfo->aggSup.pResultRowHashTable);
    }

    if (pInfo->reCkBlock) {
      pInfo->reCkBlock = false;
      printDataBlock(pInfo->pCheckpointRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
      return pInfo->pCheckpointRes;
    }

    setStreamOperatorCompleted(pOperator);
    if (!IS_FINAL_INTERVAL_OP(pOperator)) {
      clearFunctionContext(&pOperator->exprSupp);
      // semi interval operator clear disk buffer
      clearStreamIntervalOperator(pInfo);
      qDebug("===stream===clear semi operator");
    }
    return NULL;
  } else {
    if (!IS_FINAL_INTERVAL_OP(pOperator)) {
      SSDataBlock* resBlock = buildIntervalResult(pOperator);
      if (resBlock != NULL) {
        return resBlock;
      }

      if (pInfo->recvRetrive) {
        pInfo->recvRetrive = false;
        printDataBlock(pInfo->pMidRetriveRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
        return pInfo->pMidRetriveRes;
      }
    }
  }

  if (!pInfo->pUpdated) {
    pInfo->pUpdated = taosArrayInit(4096, POINTER_BYTES);
  }
  if (!pInfo->pUpdatedMap) {
    _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
    pInfo->pUpdatedMap = tSimpleHashInit(4096, hashFn);
  }

  while (1) {
    if (isTaskKilled(pTaskInfo)) {
      if (pInfo->pUpdated != NULL) {
        pInfo->pUpdated = taosArrayDestroy(pInfo->pUpdated);
      }

      if (pInfo->pUpdatedMap != NULL) {
        tSimpleHashCleanup(pInfo->pUpdatedMap);
        pInfo->pUpdatedMap = NULL;
      }
      qInfo("%s task is killed, code %s", GET_TASKID(pTaskInfo), tstrerror(pTaskInfo->code));
      T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
    }

    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      pOperator->status = OP_RES_TO_RETURN;
      qDebug("===stream===return data:%s. recv datablock num:%" PRIu64, getStreamOpName(pOperator->operatorType),
             pInfo->numOfDatapack);
      pInfo->numOfDatapack = 0;
      break;
    }
    pInfo->numOfDatapack++;
    printSpecDataBlock(pBlock, getStreamOpName(pOperator->operatorType), "recv", GET_TASKID(pTaskInfo));

    if (pBlock->info.type == STREAM_NORMAL || pBlock->info.type == STREAM_PULL_DATA) {
      pInfo->binfo.pRes->info.type = pBlock->info.type;
    } else if (pBlock->info.type == STREAM_DELETE_DATA || pBlock->info.type == STREAM_DELETE_RESULT ||
               pBlock->info.type == STREAM_CLEAR) {
      SArray* delWins = taosArrayInit(8, sizeof(SWinKey));
      doDeleteWindows(pOperator, &pInfo->interval, pBlock, delWins, pInfo->pUpdatedMap);
      if (IS_FINAL_INTERVAL_OP(pOperator)) {
        int32_t chId = getChildIndex(pBlock);
        addRetriveWindow(delWins, pInfo, chId);
        if (pBlock->info.type != STREAM_CLEAR) {
          taosArrayAddAll(pInfo->pDelWins, delWins);
        }
        taosArrayDestroy(delWins);
        continue;
      }
      removeResults(delWins, pInfo->pUpdatedMap);
      taosArrayAddAll(pInfo->pDelWins, delWins);
      taosArrayDestroy(delWins);

      doBuildDeleteResult(pInfo, pInfo->pDelWins, &pInfo->delIndex, pInfo->pDelRes);
      if (pInfo->pDelRes->info.rows != 0) {
        // process the rest of the data
        printDataBlock(pInfo->pDelRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
        if (pBlock->info.type == STREAM_CLEAR) {
          pInfo->pDelRes->info.type = STREAM_CLEAR;
        } else {
          pInfo->pDelRes->info.type = STREAM_DELETE_RESULT;
        }
        return pInfo->pDelRes;
      }

      break;
    } else if (pBlock->info.type == STREAM_GET_ALL && IS_FINAL_INTERVAL_OP(pOperator)) {
      pInfo->recvGetAll = true;
      getAllIntervalWindow(pInfo->aggSup.pResultRowHashTable, pInfo->pUpdatedMap);
      continue;
    } else if (pBlock->info.type == STREAM_RETRIEVE) {
      if(!IS_FINAL_INTERVAL_OP(pOperator)) {
        pInfo->recvRetrive = true;
        copyDataBlock(pInfo->pMidRetriveRes, pBlock);
        pInfo->pMidRetriveRes->info.type = STREAM_MID_RETRIEVE;
        doDeleteWindows(pOperator, &pInfo->interval, pBlock, NULL, pInfo->pUpdatedMap);
        break;
      }
      continue;
    } else if (pBlock->info.type == STREAM_PULL_OVER && IS_FINAL_INTERVAL_OP(pOperator)) {
      processPullOver(pBlock, pInfo->pPullDataMap, pInfo->pFinalPullDataMap, &pInfo->interval, pInfo->pPullWins,
                      pInfo->numOfChild, pOperator);
      continue;
    } else if (pBlock->info.type == STREAM_CREATE_CHILD_TABLE) {
      return pBlock;
    } else if (pBlock->info.type == STREAM_CHECKPOINT) {
      pAPI->stateStore.streamStateCommit(pInfo->pState);
      doStreamIntervalSaveCheckpoint(pOperator);
      copyDataBlock(pInfo->pCheckpointRes, pBlock);
      continue;
    } else if (IS_FINAL_INTERVAL_OP(pOperator) && pBlock->info.type == STREAM_MID_RETRIEVE) {
      continue;
    } else {
      ASSERTS(pBlock->info.type == STREAM_INVALID, "invalid SSDataBlock type");
    }

    if (pInfo->scalarSupp.pExprInfo != NULL) {
      SExprSupp* pExprSup = &pInfo->scalarSupp;
      projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
    }
    setInputDataBlock(pSup, pBlock, TSDB_ORDER_ASC, MAIN_SCAN, true);
    doStreamIntervalAggImpl(pOperator, pBlock, pBlock->info.id.groupId, pInfo->pUpdatedMap);
    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.window.ekey);
    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.watermark);
    pInfo->twAggSup.minTs = TMIN(pInfo->twAggSup.minTs, pBlock->info.window.skey);
  }

  removeDeleteResults(pInfo->pUpdatedMap, pInfo->pDelWins);
  if (IS_FINAL_INTERVAL_OP(pOperator)) {
    closeStreamIntervalWindow(pInfo->aggSup.pResultRowHashTable, &pInfo->twAggSup, &pInfo->interval,
                              pInfo->pPullDataMap, pInfo->pUpdatedMap, pInfo->pDelWins, pOperator);
  }
  pInfo->binfo.pRes->info.watermark = pInfo->twAggSup.maxTs;

  copyUpdateResult(&pInfo->pUpdatedMap, pInfo->pUpdated, winPosCmprImpl);

  initMultiResInfoFromArrayList(&pInfo->groupResInfo, pInfo->pUpdated);
  pInfo->pUpdated = NULL;
  blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);

  SSDataBlock* resBlock = buildIntervalResult(pOperator);
  if (resBlock != NULL) {
    return resBlock;
  }

  if (pInfo->recvRetrive) {
    pInfo->recvRetrive = false;
    printDataBlock(pInfo->pMidRetriveRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
    return pInfo->pMidRetriveRes;
  }

  return NULL;
}

int64_t getDeleteMark(SWindowPhysiNode* pWinPhyNode, int64_t interval) {
  if (pWinPhyNode->deleteMark <= 0) {
    return DEAULT_DELETE_MARK;
  }
  int64_t deleteMark = TMAX(pWinPhyNode->deleteMark, pWinPhyNode->watermark);
  deleteMark = TMAX(deleteMark, interval);
  return deleteMark;
}

static TSKEY compareTs(void* pKey) {
  SWinKey* pWinKey = (SWinKey*)pKey;
  return pWinKey->ts;
}

static int32_t getSelectivityBufSize(SqlFunctionCtx* pCtx) {
  if (pCtx->subsidiaries.rowLen == 0) {
    int32_t rowLen = 0;
    for (int32_t j = 0; j < pCtx->subsidiaries.num; ++j) {
      SqlFunctionCtx* pc = pCtx->subsidiaries.pCtx[j];
      rowLen += pc->pExpr->base.resSchema.bytes;
    }

    return rowLen + pCtx->subsidiaries.num * sizeof(bool);
  } else {
    return pCtx->subsidiaries.rowLen;
  }
}

static int32_t getMaxFunResSize(SExprSupp* pSup, int32_t numOfCols) {
  int32_t size = 0;
  for (int32_t i = 0; i < numOfCols; ++i) {
    int32_t resSize = getSelectivityBufSize(pSup->pCtx + i);
    size = TMAX(size, resSize);
  }
  return size;
}

static void streamIntervalReleaseState(SOperatorInfo* pOperator) {
  if (pOperator->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL &&
      pOperator->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_MID_INTERVAL) {
    SStreamIntervalOperatorInfo* pInfo = pOperator->info;
    int32_t                      resSize = sizeof(TSKEY);
    pInfo->stateStore.streamStateSaveInfo(pInfo->pState, STREAM_INTERVAL_OP_STATE_NAME,
                                          strlen(STREAM_INTERVAL_OP_STATE_NAME), &pInfo->twAggSup.maxTs, resSize);
  }
  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  SStorageAPI*                 pAPI = &pOperator->pTaskInfo->storageAPI;
  pAPI->stateStore.streamStateCommit(pInfo->pState);
  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (downstream->fpSet.releaseStreamStateFn) {
    downstream->fpSet.releaseStreamStateFn(downstream);
  }
}

void streamIntervalReloadState(SOperatorInfo* pOperator) {
  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  if (pOperator->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL &&
      pOperator->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_MID_INTERVAL) {
    int32_t                      size = 0;
    void*                        pBuf = NULL;
    int32_t code = pInfo->stateStore.streamStateGetInfo(pInfo->pState, STREAM_INTERVAL_OP_STATE_NAME,
                                                        strlen(STREAM_INTERVAL_OP_STATE_NAME), &pBuf, &size);
    if (code == 0) {
      TSKEY ts = *(TSKEY*)pBuf;
      taosMemoryFree(pBuf);
      pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, ts);
      pInfo->stateStore.streamStateReloadInfo(pInfo->pState, ts);
    }
  }
  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (downstream->fpSet.reloadStreamStateFn) {
    downstream->fpSet.reloadStreamStateFn(downstream);
  }
  reloadFromDownStream(downstream, pInfo);
}

SOperatorInfo* createStreamFinalIntervalOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode,
                                                     SExecTaskInfo* pTaskInfo, int32_t numOfChild,
                                                     SReadHandle* pHandle) {
  SIntervalPhysiNode*          pIntervalPhyNode = (SIntervalPhysiNode*)pPhyNode;
  SStreamIntervalOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamIntervalOperatorInfo));
  SOperatorInfo*               pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  int32_t                      code = 0;
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pOperator->pTaskInfo = pTaskInfo;
  SStorageAPI* pAPI = &pTaskInfo->storageAPI;

  pInfo->interval = (SInterval){.interval = pIntervalPhyNode->interval,
                                .sliding = pIntervalPhyNode->sliding,
                                .intervalUnit = pIntervalPhyNode->intervalUnit,
                                .slidingUnit = pIntervalPhyNode->slidingUnit,
                                .offset = pIntervalPhyNode->offset,
                                .precision = ((SColumnNode*)pIntervalPhyNode->window.pTspk)->node.resType.precision};
  pInfo->twAggSup = (STimeWindowAggSupp){
      .waterMark = pIntervalPhyNode->window.watermark,
      .calTrigger = pIntervalPhyNode->window.triggerType,
      .maxTs = INT64_MIN,
      .minTs = INT64_MAX,
      .deleteMark = getDeleteMark(&pIntervalPhyNode->window, pIntervalPhyNode->interval),
      .deleteMarkSaved = 0,
      .calTriggerSaved = 0,
  };
  ASSERTS(pInfo->twAggSup.calTrigger != STREAM_TRIGGER_MAX_DELAY, "trigger type should not be max delay");
  pInfo->primaryTsIndex = ((SColumnNode*)pIntervalPhyNode->window.pTspk)->slotId;
  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  initResultSizeInfo(&pOperator->resultInfo, 4096);
  if (pIntervalPhyNode->window.pExprs != NULL) {
    int32_t    numOfScalar = 0;
    SExprInfo* pScalarExprInfo = createExprInfo(pIntervalPhyNode->window.pExprs, NULL, &numOfScalar);
    code = initExprSupp(&pInfo->scalarSupp, pScalarExprInfo, numOfScalar, &pTaskInfo->storageAPI.functionStore);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  int32_t      numOfCols = 0;
  SExprInfo*   pExprInfo = createExprInfo(pIntervalPhyNode->window.pFuncs, NULL, &numOfCols);
  SSDataBlock* pResBlock = createDataBlockFromDescNode(pPhyNode->pOutputDataBlockDesc);
  initBasicInfo(&pInfo->binfo, pResBlock);

  pInfo->pState = taosMemoryCalloc(1, sizeof(SStreamState));
  qInfo("open state %p", pInfo->pState);
  pAPI->stateStore.streamStateCopyBackend(pTaskInfo->streamInfo.pState, pInfo->pState);
  //*(pInfo->pState) = *(pTaskInfo->streamInfo.pState);

  qInfo("copy state %p to %p", pTaskInfo->streamInfo.pState, pInfo->pState);

  pAPI->stateStore.streamStateSetNumber(pInfo->pState, -1);
  code = initAggSup(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, numOfCols, keyBufSize, pTaskInfo->id.str,
                            pInfo->pState, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);
  initResultRowInfo(&pInfo->binfo.resultRowInfo);

  pInfo->numOfChild = numOfChild;
  pInfo->pPhyNode = (SPhysiNode*)nodesCloneNode((SNode*)pPhyNode);

  pInfo->pPullWins = taosArrayInit(8, sizeof(SPullWindowInfo));
  pInfo->pullIndex = 0;
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pInfo->pPullDataMap = taosHashInit(64, hashFn, false, HASH_NO_LOCK);
  pInfo->pFinalPullDataMap = taosHashInit(64, hashFn, false, HASH_NO_LOCK);
  pInfo->pPullDataRes = createSpecialDataBlock(STREAM_RETRIEVE);
  pInfo->ignoreExpiredData = pIntervalPhyNode->window.igExpired;
  pInfo->ignoreExpiredDataSaved = false;
  pInfo->pDelRes = createSpecialDataBlock(STREAM_DELETE_RESULT);
  pInfo->delIndex = 0;
  pInfo->pDelWins = taosArrayInit(4, sizeof(SWinKey));
  pInfo->delKey.ts = INT64_MAX;
  pInfo->delKey.groupId = 0;
  pInfo->numOfDatapack = 0;
  pInfo->pUpdated = NULL;
  pInfo->pUpdatedMap = NULL;
  int32_t funResSize = getMaxFunResSize(&pOperator->exprSupp, numOfCols);
  pInfo->pState->pFileState = pAPI->stateStore.streamFileStateInit(
      tsStreamBufferSize, sizeof(SWinKey), pInfo->aggSup.resultRowSize, funResSize, compareTs, pInfo->pState,
      pInfo->twAggSup.deleteMark, GET_TASKID(pTaskInfo), pHandle->checkpointId, STREAM_STATE_BUFF_HASH);
  pInfo->dataVersion = 0;
  pInfo->stateStore = pTaskInfo->storageAPI.stateStore;
  pInfo->recvGetAll = false;
  pInfo->pCheckpointRes = createSpecialDataBlock(STREAM_CHECKPOINT);
  pInfo->recvRetrive = false;
  pInfo->pMidRetriveRes = createSpecialDataBlock(STREAM_MID_RETRIEVE);
  pInfo->pMidPulloverRes = createSpecialDataBlock(STREAM_MID_RETRIEVE);
  pInfo->clearState = false;

  pOperator->operatorType = pPhyNode->type;
  if (!IS_FINAL_INTERVAL_OP(pOperator) || numOfChild == 0) {
    pInfo->twAggSup.calTrigger = STREAM_TRIGGER_AT_ONCE;
  }
  pOperator->name = getStreamOpName(pOperator->operatorType);
  pOperator->blocking = true;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;

  if (pPhyNode->type == QUERY_NODE_PHYSICAL_PLAN_STREAM_MID_INTERVAL) {
    pOperator->fpSet = createOperatorFpSet(NULL, doStreamMidIntervalAgg, NULL, destroyStreamFinalIntervalOperatorInfo,
                                           optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  } else {
    pOperator->fpSet = createOperatorFpSet(NULL, doStreamFinalIntervalAgg, NULL, destroyStreamFinalIntervalOperatorInfo,
                                           optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  }
  setOperatorStreamStateFn(pOperator, streamIntervalReleaseState, streamIntervalReloadState);
  if (pPhyNode->type == QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL ||
      pPhyNode->type == QUERY_NODE_PHYSICAL_PLAN_STREAM_MID_INTERVAL) {
    initIntervalDownStream(downstream, pPhyNode->type, pInfo);
  }
  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  // for stream
  void*   buff = NULL;
  int32_t len = 0;
  int32_t res = pAPI->stateStore.streamStateGetInfo(pInfo->pState, STREAM_INTERVAL_OP_CHECKPOINT_NAME,
                                                    strlen(STREAM_INTERVAL_OP_CHECKPOINT_NAME), &buff, &len);
  if (res == TSDB_CODE_SUCCESS) {
    doStreamIntervalDecodeOpState(buff, len, pOperator);
    taosMemoryFree(buff);
  }

  return pOperator;

_error:
  destroyStreamFinalIntervalOperatorInfo(pInfo);
  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

void destroyStreamAggSupporter(SStreamAggSupporter* pSup) {
  tSimpleHashCleanup(pSup->pResultRows);
  destroyDiskbasedBuf(pSup->pResultBuf);
  blockDataDestroy(pSup->pScanBlock);
  pSup->stateStore.streamFileStateDestroy(pSup->pState->pFileState);
  taosMemoryFreeClear(pSup->pState);
  taosMemoryFreeClear(pSup->pDummyCtx);
}

void destroyStreamSessionAggOperatorInfo(void* param) {
  SStreamSessionAggOperatorInfo* pInfo = (SStreamSessionAggOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);
  destroyStreamAggSupporter(&pInfo->streamAggSup);
  cleanupExprSupp(&pInfo->scalarSupp);
  clearGroupResInfo(&pInfo->groupResInfo);

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
  blockDataDestroy(pInfo->pWinBlock);
  tSimpleHashCleanup(pInfo->pStUpdated);
  tSimpleHashCleanup(pInfo->pStDeleted);
  pInfo->pUpdated = taosArrayDestroy(pInfo->pUpdated);
  cleanupGroupResInfo(&pInfo->groupResInfo);

  taosArrayDestroy(pInfo->historyWins);
  blockDataDestroy(pInfo->pCheckpointRes);

  taosMemoryFreeClear(param);
}

int32_t initBasicInfoEx(SOptrBasicInfo* pBasicInfo, SExprSupp* pSup, SExprInfo* pExprInfo, int32_t numOfCols,
                        SSDataBlock* pResultBlock, SFunctionStateStore* pStore) {
  initBasicInfo(pBasicInfo, pResultBlock);
  int32_t code = initExprSupp(pSup, pExprInfo, numOfCols, pStore);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    pSup->pCtx[i].saveHandle.pBuf = NULL;
  }

  ASSERT(numOfCols > 0);
  return TSDB_CODE_SUCCESS;
}

void initDummyFunction(SqlFunctionCtx* pDummy, SqlFunctionCtx* pCtx, int32_t nums) {
  for (int i = 0; i < nums; i++) {
    pDummy[i].functionId = pCtx[i].functionId;
    pDummy[i].isNotNullFunc = pCtx[i].isNotNullFunc;
    pDummy[i].isPseudoFunc = pCtx[i].isPseudoFunc;
    pDummy[i].fpSet.init = pCtx[i].fpSet.init;
  }
}

void initDownStream(SOperatorInfo* downstream, SStreamAggSupporter* pAggSup, uint16_t type, int32_t tsColIndex,
                    STimeWindowAggSupp* pTwSup) {
  if (downstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_PARTITION) {
    SStreamPartitionOperatorInfo* pScanInfo = downstream->info;
    pScanInfo->tsColIndex = tsColIndex;
  }

  if (downstream->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    initDownStream(downstream->pDownstream[0], pAggSup, type, tsColIndex, pTwSup);
    return;
  }
  SStreamScanInfo* pScanInfo = downstream->info;
  pScanInfo->windowSup = (SWindowSupporter){.pStreamAggSup = pAggSup, .gap = pAggSup->gap, .parentType = type};
  pScanInfo->pState = pAggSup->pState;
  if (!pScanInfo->pUpdateInfo) {
    pScanInfo->pUpdateInfo = pAggSup->stateStore.updateInfoInit(60000, TSDB_TIME_PRECISION_MILLI, pTwSup->waterMark,
                                                                pScanInfo->igCheckUpdate);
  }
  pScanInfo->twAggSup = *pTwSup;
  pAggSup->pUpdateInfo = pScanInfo->pUpdateInfo;
}

static TSKEY sesionTs(void* pKey) {
  SSessionKey* pWinKey = (SSessionKey*)pKey;
  return pWinKey->win.skey;
}

int32_t initStreamAggSupporter(SStreamAggSupporter* pSup, SExprSupp* pExpSup, int32_t numOfOutput, int64_t gap,
                               SStreamState* pState, int32_t keySize, int16_t keyType, SStateStore* pStore,
                               SReadHandle* pHandle, STimeWindowAggSupp* pTwAggSup, const char* taskIdStr,
                               SStorageAPI* pApi) {
  pSup->resultRowSize = keySize + getResultRowSize(pExpSup->pCtx, numOfOutput);
  pSup->pScanBlock = createSpecialDataBlock(STREAM_CLEAR);
  pSup->gap = gap;
  pSup->stateKeySize = keySize;
  pSup->stateKeyType = keyType;
  pSup->pDummyCtx = (SqlFunctionCtx*)taosMemoryCalloc(numOfOutput, sizeof(SqlFunctionCtx));
  if (pSup->pDummyCtx == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pSup->stateStore = *pStore;

  initDummyFunction(pSup->pDummyCtx, pExpSup->pCtx, numOfOutput);
  pSup->pState = taosMemoryCalloc(1, sizeof(SStreamState));
  *(pSup->pState) = *pState;
  pSup->stateStore.streamStateSetNumber(pSup->pState, -1);
  int32_t funResSize = getMaxFunResSize(pExpSup, numOfOutput);
  pSup->pState->pFileState = pSup->stateStore.streamFileStateInit(
      tsStreamBufferSize, sizeof(SSessionKey), pSup->resultRowSize, funResSize, sesionTs, pSup->pState,
      pTwAggSup->deleteMark, taskIdStr, pHandle->checkpointId, STREAM_STATE_BUFF_SORT);

  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pSup->pResultRows = tSimpleHashInit(32, hashFn);

  int32_t pageSize = 4096;
  while (pageSize < pSup->resultRowSize * 4) {
    pageSize <<= 1u;
  }
  // at least four pages need to be in buffer
  int32_t bufSize = 4096 * 256;
  if (bufSize <= pageSize) {
    bufSize = pageSize * 4;
  }

  if (!osTempSpaceAvailable()) {
    terrno = TSDB_CODE_NO_DISKSPACE;
    qError("Init stream agg supporter failed since %s, tempDir:%s", terrstr(), tsTempDir);
    return terrno;
  }

  int32_t code = createDiskbasedBuf(&pSup->pResultBuf, pageSize, bufSize, "function", tsTempDir);
  for (int32_t i = 0; i < numOfOutput; ++i) {
    pExpSup->pCtx[i].saveHandle.pBuf = pSup->pResultBuf;
  }

  pSup->pSessionAPI = pApi;

  return TSDB_CODE_SUCCESS;
}

bool isInTimeWindow(STimeWindow* pWin, TSKEY ts, int64_t gap) {
  if (ts + gap >= pWin->skey && ts - gap <= pWin->ekey) {
    return true;
  }
  return false;
}

bool isInWindow(SResultWindowInfo* pWinInfo, TSKEY ts, int64_t gap) {
  return isInTimeWindow(&pWinInfo->sessionWin.win, ts, gap);
}

void getCurSessionWindow(SStreamAggSupporter* pAggSup, TSKEY startTs, TSKEY endTs, uint64_t groupId,
                         SSessionKey* pKey) {
  pKey->win.skey = startTs;
  pKey->win.ekey = endTs;
  pKey->groupId = groupId;
  int32_t code = pAggSup->stateStore.streamStateSessionGetKeyByRange(pAggSup->pState, pKey, pKey);
  if (code != TSDB_CODE_SUCCESS) {
    SET_SESSION_WIN_KEY_INVALID(pKey);
  }
}

bool isInvalidSessionWin(SResultWindowInfo* pWinInfo) { return pWinInfo->sessionWin.win.skey == 0; }

bool inWinRange(STimeWindow* range, STimeWindow* cur) {
  if (cur->skey >= range->skey && cur->ekey <= range->ekey) {
    return true;
  }
  return false;
}

int32_t clearOutputBuf(void* pState, SRowBuffPos* pPos, SStateStore* pAPI) {
  return pAPI->streamStateClearBuff(pState, pPos);
}

void setSessionOutputBuf(SStreamAggSupporter* pAggSup, TSKEY startTs, TSKEY endTs, uint64_t groupId,
                         SResultWindowInfo* pCurWin) {
  pCurWin->sessionWin.groupId = groupId;
  pCurWin->sessionWin.win.skey = startTs;
  pCurWin->sessionWin.win.ekey = endTs;
  int32_t size = pAggSup->resultRowSize;
  int32_t code = pAggSup->stateStore.streamStateSessionAddIfNotExist(pAggSup->pState, &pCurWin->sessionWin,
                                                                     pAggSup->gap, (void**)&pCurWin->pStatePos, &size);
  if (code == TSDB_CODE_SUCCESS && !inWinRange(&pAggSup->winRange, &pCurWin->sessionWin.win)) {
    code = TSDB_CODE_FAILED;
    clearOutputBuf(pAggSup->pState, pCurWin->pStatePos, &pAggSup->pSessionAPI->stateStore);
  }

  if (code == TSDB_CODE_SUCCESS) {
    pCurWin->isOutput = true;
    if (pCurWin->pStatePos->needFree) {
      pAggSup->stateStore.streamStateSessionDel(pAggSup->pState, &pCurWin->sessionWin);
    }
  } else {
    pCurWin->sessionWin.win.skey = startTs;
    pCurWin->sessionWin.win.ekey = endTs;
  }
  qDebug("===stream===set session window buff .start:%" PRId64 ",end:%" PRId64 ",groupid:%" PRIu64,
         pCurWin->sessionWin.win.skey, pCurWin->sessionWin.win.ekey, pCurWin->sessionWin.groupId);
}

int32_t getSessionWinBuf(SStreamAggSupporter* pAggSup, SStreamStateCur* pCur, SResultWindowInfo* pWinInfo) {
  int32_t size = 0;
  int32_t code = pAggSup->stateStore.streamStateSessionGetKVByCur(pCur, &pWinInfo->sessionWin,
                                                                  (void**)&pWinInfo->pStatePos, &size);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pAggSup->stateStore.streamStateCurNext(pAggSup->pState, pCur);
  return TSDB_CODE_SUCCESS;
}
void saveDeleteInfo(SArray* pWins, SSessionKey key) {
  // key.win.ekey = key.win.skey;
  taosArrayPush(pWins, &key);
}

void saveDeleteRes(SSHashObj* pStDelete, SSessionKey key) {
  key.win.ekey = key.win.skey;
  tSimpleHashPut(pStDelete, &key, sizeof(SSessionKey), NULL, 0);
}

int32_t releaseOutputBuf(void* pState, SRowBuffPos* pPos, SStateStore* pAPI) {
  pAPI->streamStateReleaseBuf(pState, pPos, false);
  return TSDB_CODE_SUCCESS;
}

int32_t reuseOutputBuf(void* pState, SRowBuffPos* pPos, SStateStore* pAPI) {
  pAPI->streamStateReleaseBuf(pState, pPos, true);
  return TSDB_CODE_SUCCESS;
}

void removeSessionResult(SStreamAggSupporter* pAggSup, SSHashObj* pHashMap, SSHashObj* pResMap, SSessionKey* pKey) {
  SSessionKey key = {0};
  getSessionHashKey(pKey, &key);
  void* pVal = tSimpleHashGet(pHashMap, &key, sizeof(SSessionKey));
  if (pVal) {
    releaseOutputBuf(pAggSup->pState, *(void**)pVal, &pAggSup->pSessionAPI->stateStore);
    tSimpleHashRemove(pHashMap, &key, sizeof(SSessionKey));
  }
  tSimpleHashRemove(pResMap, &key, sizeof(SSessionKey));
}

void getSessionHashKey(const SSessionKey* pKey, SSessionKey* pHashKey) {
  *pHashKey = *pKey;
  pHashKey->win.ekey = pKey->win.skey;
}

void removeSessionDeleteResults(SSHashObj* pHashMap, SArray* pWins) {
  if (tSimpleHashGetSize(pHashMap) == 0) {
    return;
  }
  int32_t size = taosArrayGetSize(pWins);
  for (int32_t i = 0; i < size; i++) {
    SSessionKey* pWin = taosArrayGet(pWins, i);
    if (!pWin) continue;
    SSessionKey key = {0};
    getSessionHashKey(pWin, &key);
    tSimpleHashRemove(pHashMap, &key, sizeof(SSessionKey));
  }
}

void removeSessionResults(SStreamAggSupporter* pAggSup, SSHashObj* pHashMap, SArray* pWins) {
  if (tSimpleHashGetSize(pHashMap) == 0) {
    return;
  }
  int32_t size = taosArrayGetSize(pWins);
  for (int32_t i = 0; i < size; i++) {
    SSessionKey* pWin = taosArrayGet(pWins, i);
    if (!pWin) continue;
    SSessionKey key = {0};
    getSessionHashKey(pWin, &key);
    void* pVal = tSimpleHashGet(pHashMap, &key, sizeof(SSessionKey));
    if (pVal) {
      releaseOutputBuf(pAggSup->pState, *(void**)pVal, &pAggSup->pSessionAPI->stateStore);
      tSimpleHashRemove(pHashMap, &key, sizeof(SSessionKey));
    }
  }
}

int32_t updateSessionWindowInfo(SStreamAggSupporter* pAggSup, SResultWindowInfo* pWinInfo, TSKEY* pStartTs,
                                TSKEY* pEndTs, uint64_t groupId, int32_t rows, int32_t start, int64_t gap,
                                SSHashObj* pResultRows, SSHashObj* pStUpdated, SSHashObj* pStDeleted) {
  for (int32_t i = start; i < rows; ++i) {
    if (!isInWindow(pWinInfo, pStartTs[i], gap) && (!pEndTs || !isInWindow(pWinInfo, pEndTs[i], gap))) {
      return i - start;
    }
    if (pWinInfo->sessionWin.win.skey > pStartTs[i]) {
      if (pStDeleted && pWinInfo->isOutput) {
        saveDeleteRes(pStDeleted, pWinInfo->sessionWin);
      }
      removeSessionResult(pAggSup, pStUpdated, pResultRows, &pWinInfo->sessionWin);
      pWinInfo->sessionWin.win.skey = pStartTs[i];
    }
    pWinInfo->sessionWin.win.ekey = TMAX(pWinInfo->sessionWin.win.ekey, pStartTs[i]);
    if (pEndTs) {
      pWinInfo->sessionWin.win.ekey = TMAX(pWinInfo->sessionWin.win.ekey, pEndTs[i]);
    }
    memcpy(pWinInfo->pStatePos->pKey, &pWinInfo->sessionWin, sizeof(SSessionKey));
  }
  return rows - start;
}

static int32_t initSessionOutputBuf(SResultWindowInfo* pWinInfo, SResultRow** pResult, SqlFunctionCtx* pCtx,
                                    int32_t numOfOutput, int32_t* rowEntryInfoOffset) {
  ASSERT(pWinInfo->sessionWin.win.skey <= pWinInfo->sessionWin.win.ekey);
  *pResult = (SResultRow*)pWinInfo->pStatePos->pRowBuff;
  // set time window for current result
  (*pResult)->win = pWinInfo->sessionWin.win;
  setResultRowInitCtx(*pResult, pCtx, numOfOutput, rowEntryInfoOffset);
  return TSDB_CODE_SUCCESS;
}

int32_t doOneWindowAggImpl(SColumnInfoData* pTimeWindowData, SResultWindowInfo* pCurWin, SResultRow** pResult,
                           int32_t startIndex, int32_t winRows, int32_t rows, int32_t numOutput,
                           SOperatorInfo* pOperator, int64_t winDelta) {
  SExprSupp*     pSup = &pOperator->exprSupp;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  int32_t        code = initSessionOutputBuf(pCurWin, pResult, pSup->pCtx, numOutput, pSup->rowEntryInfoOffset);
  if (code != TSDB_CODE_SUCCESS || (*pResult) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  updateTimeWindowInfo(pTimeWindowData, &pCurWin->sessionWin.win, winDelta);
  applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, pTimeWindowData, startIndex, winRows, rows, numOutput);
  return TSDB_CODE_SUCCESS;
}

bool doDeleteSessionWindow(SStreamAggSupporter* pAggSup, SSessionKey* pKey) {
  pAggSup->stateStore.streamStateSessionDel(pAggSup->pState, pKey);
  SSessionKey hashKey = {0};
  getSessionHashKey(pKey, &hashKey);
  tSimpleHashRemove(pAggSup->pResultRows, &hashKey, sizeof(SSessionKey));
  return true;
}

int32_t setSessionWinOutputInfo(SSHashObj* pStUpdated, SResultWindowInfo* pWinInfo) {
  void* pVal = tSimpleHashGet(pStUpdated, &pWinInfo->sessionWin, sizeof(SSessionKey));
  if (pVal) {
    SResultWindowInfo* pWin = pVal;
    pWinInfo->isOutput = pWin->isOutput;
  }
  return TSDB_CODE_SUCCESS;
}

void getNextSessionWinInfo(SStreamAggSupporter* pAggSup, SSHashObj* pStUpdated, SResultWindowInfo* pCurWin,
                           SResultWindowInfo* pNextWin) {
  SStreamStateCur* pCur = pAggSup->stateStore.streamStateSessionSeekKeyNext(pAggSup->pState, &pCurWin->sessionWin);
  pNextWin->isOutput = true;
  setSessionWinOutputInfo(pStUpdated, pNextWin);
  int32_t size = 0;
  pNextWin->sessionWin = pCurWin->sessionWin;
  int32_t code = pAggSup->stateStore.streamStateSessionGetKVByCur(pCur, &pNextWin->sessionWin,
                                                                  (void**)&pNextWin->pStatePos, &size);
  if (code != TSDB_CODE_SUCCESS) {
    SET_SESSION_WIN_INVALID(*pNextWin);
  }
  pAggSup->stateStore.streamStateFreeCur(pCur);
}

void compactTimeWindow(SExprSupp* pSup, SStreamAggSupporter* pAggSup, STimeWindowAggSupp* pTwAggSup,
                       SExecTaskInfo* pTaskInfo, SResultWindowInfo* pCurWin, SResultWindowInfo* pNextWin,
                       SSHashObj* pStUpdated, SSHashObj* pStDeleted, bool addGap) {
  SResultRow* pCurResult = NULL;
  int32_t     numOfOutput = pSup->numOfExprs;
  initSessionOutputBuf(pCurWin, &pCurResult, pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset);
  SResultRow* pWinResult = NULL;
  initSessionOutputBuf(pNextWin, &pWinResult, pAggSup->pDummyCtx, numOfOutput, pSup->rowEntryInfoOffset);
  pCurWin->sessionWin.win.ekey = TMAX(pCurWin->sessionWin.win.ekey, pNextWin->sessionWin.win.ekey);
  memcpy(pCurWin->pStatePos->pKey, &pCurWin->sessionWin, sizeof(SSessionKey));

  int64_t winDelta = 0;
  if (addGap) {
    winDelta = pAggSup->gap;
  }
  updateTimeWindowInfo(&pTwAggSup->timeWindowData, &pCurWin->sessionWin.win, winDelta);
  compactFunctions(pSup->pCtx, pAggSup->pDummyCtx, numOfOutput, pTaskInfo, &pTwAggSup->timeWindowData);
  tSimpleHashRemove(pStUpdated, &pNextWin->sessionWin, sizeof(SSessionKey));
  if (pNextWin->isOutput && pStDeleted) {
    qDebug("===stream=== save delete window info %" PRId64 ", %" PRIu64, pNextWin->sessionWin.win.skey,
           pNextWin->sessionWin.groupId);
    saveDeleteRes(pStDeleted, pNextWin->sessionWin);
  }
  removeSessionResult(pAggSup, pStUpdated, pAggSup->pResultRows, &pNextWin->sessionWin);
  doDeleteSessionWindow(pAggSup, &pNextWin->sessionWin);
  releaseOutputBuf(pAggSup->pState, pNextWin->pStatePos, &pAggSup->pSessionAPI->stateStore);
}

static int32_t compactSessionWindow(SOperatorInfo* pOperator, SResultWindowInfo* pCurWin, SSHashObj* pStUpdated,
                                    SSHashObj* pStDeleted, bool addGap) {
  SExprSupp*     pSup = &pOperator->exprSupp;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*   pAPI = &pOperator->pTaskInfo->storageAPI;
  int32_t        winNum = 0;

  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  SResultRow*                    pCurResult = NULL;
  int32_t                        numOfOutput = pOperator->exprSupp.numOfExprs;
  SStreamAggSupporter*           pAggSup = &pInfo->streamAggSup;
  // initSessionOutputBuf(pCurWin, &pCurResult, pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset);
  // Just look for the window behind StartIndex
  while (1) {
    SResultWindowInfo winInfo = {0};
    getNextSessionWinInfo(pAggSup, pStUpdated, pCurWin, &winInfo);
    if (!IS_VALID_SESSION_WIN(winInfo) || !isInWindow(pCurWin, winInfo.sessionWin.win.skey, pAggSup->gap) ||
        !inWinRange(&pAggSup->winRange, &winInfo.sessionWin.win)) {
      releaseOutputBuf(pAggSup->pState, winInfo.pStatePos, &pAggSup->pSessionAPI->stateStore);
      break;
    }
    compactTimeWindow(pSup, pAggSup, &pInfo->twAggSup, pTaskInfo, pCurWin, &winInfo, pStUpdated, pStDeleted, true);
    winNum++;
  }
  return winNum;
}

static void compactSessionSemiWindow(SOperatorInfo* pOperator, SResultWindowInfo* pCurWin) {
  SExprSupp*                     pSup = &pOperator->exprSupp;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*                   pAPI = &pOperator->pTaskInfo->storageAPI;
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  SResultRow*                    pCurResult = NULL;
  int32_t                        numOfOutput = pOperator->exprSupp.numOfExprs;
  SStreamAggSupporter*           pAggSup = &pInfo->streamAggSup;
  // Just look for the window behind StartIndex
  while (1) {
    SResultWindowInfo winInfo = {0};
    getNextSessionWinInfo(pAggSup, NULL, pCurWin, &winInfo);
    if (!IS_VALID_SESSION_WIN(winInfo) || !isInWindow(pCurWin, winInfo.sessionWin.win.skey, pAggSup->gap) ||
        !inWinRange(&pAggSup->winRange, &winInfo.sessionWin.win)) {
      releaseOutputBuf(pAggSup->pState, winInfo.pStatePos, &pAggSup->pSessionAPI->stateStore);
      break;
    }
    pCurWin->sessionWin.win.ekey = TMAX(pCurWin->sessionWin.win.ekey, winInfo.sessionWin.win.ekey);
    memcpy(pCurWin->pStatePos->pKey, &pCurWin->sessionWin, sizeof(SSessionKey));
    doDeleteSessionWindow(pAggSup, &winInfo.sessionWin);
    releaseOutputBuf(pAggSup->pState, winInfo.pStatePos, &pAggSup->pSessionAPI->stateStore);
  }
}

int32_t saveSessionOutputBuf(SStreamAggSupporter* pAggSup, SResultWindowInfo* pWinInfo) {
  qDebug("===stream===try save session result skey:%" PRId64 ", ekey:%" PRId64 ".pos%d", pWinInfo->sessionWin.win.skey,
         pWinInfo->sessionWin.win.ekey, pWinInfo->pStatePos->needFree);
  return pAggSup->stateStore.streamStateSessionPut(pAggSup->pState, &pWinInfo->sessionWin, pWinInfo->pStatePos,
                                                   pAggSup->resultRowSize);
}

static void doStreamSessionAggImpl(SOperatorInfo* pOperator, SSDataBlock* pSDataBlock, SSHashObj* pStUpdated,
                                   SSHashObj* pStDeleted, bool hasEndTs, bool addGap) {
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  int32_t                        numOfOutput = pOperator->exprSupp.numOfExprs;
  uint64_t                       groupId = pSDataBlock->info.id.groupId;
  int64_t                        code = TSDB_CODE_SUCCESS;
  SResultRow*                    pResult = NULL;
  int32_t                        rows = pSDataBlock->info.rows;
  int32_t                        winRows = 0;
  SStreamAggSupporter*           pAggSup = &pInfo->streamAggSup;

  pInfo->dataVersion = TMAX(pInfo->dataVersion, pSDataBlock->info.version);
  pAggSup->winRange = pTaskInfo->streamInfo.fillHistoryWindow;
  if (pAggSup->winRange.ekey <= 0) {
    pAggSup->winRange.ekey = INT64_MAX;
  }

  SColumnInfoData* pStartTsCol = taosArrayGet(pSDataBlock->pDataBlock, pInfo->primaryTsIndex);
  TSKEY*           startTsCols = (int64_t*)pStartTsCol->pData;
  SColumnInfoData* pEndTsCol = NULL;
  if (hasEndTs) {
    pEndTsCol = taosArrayGet(pSDataBlock->pDataBlock, pInfo->endTsIndex);
  } else {
    pEndTsCol = taosArrayGet(pSDataBlock->pDataBlock, pInfo->primaryTsIndex);
  }

  TSKEY* endTsCols = (int64_t*)pEndTsCol->pData;
  for (int32_t i = 0; i < rows;) {
    if (!IS_FINAL_SESSION_OP(pOperator) && pInfo->ignoreExpiredData &&
        checkExpiredData(&pInfo->streamAggSup.stateStore, pInfo->streamAggSup.pUpdateInfo, &pInfo->twAggSup,
                         pSDataBlock->info.id.uid, endTsCols[i])) {
      i++;
      continue;
    }
    SResultWindowInfo winInfo = {0};
    setSessionOutputBuf(pAggSup, startTsCols[i], endTsCols[i], groupId, &winInfo);
    // coverity scan error
    if (!winInfo.pStatePos) {
      continue;
    }
    setSessionWinOutputInfo(pStUpdated, &winInfo);
    winRows = updateSessionWindowInfo(pAggSup, &winInfo, startTsCols, endTsCols, groupId, rows, i, pAggSup->gap,
                                      pAggSup->pResultRows, pStUpdated, pStDeleted);

    int64_t winDelta = 0;
    if (addGap) {
      winDelta = pAggSup->gap;
    }
    code = doOneWindowAggImpl(&pInfo->twAggSup.timeWindowData, &winInfo, &pResult, i, winRows, rows, numOfOutput,
                              pOperator, winDelta);
    if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
      qError("%s do stream session aggregate impl error, code %s", GET_TASKID(pTaskInfo), tstrerror(code));
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_OUT_OF_MEMORY);
    }
    compactSessionWindow(pOperator, &winInfo, pStUpdated, pStDeleted, addGap);
    saveSessionOutputBuf(pAggSup, &winInfo);

    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE && pStUpdated) {
      code = saveResult(winInfo, pStUpdated);
      if (code != TSDB_CODE_SUCCESS) {
        qError("%s do stream session aggregate impl, set result error, code %s", GET_TASKID(pTaskInfo),
               tstrerror(code));
        T_LONG_JMP(pTaskInfo->env, TSDB_CODE_OUT_OF_MEMORY);
      }
    }
    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_WINDOW_CLOSE) {
      winInfo.pStatePos->beUpdated = true;
      SSessionKey key = {0};
      getSessionHashKey(&winInfo.sessionWin, &key);
      tSimpleHashPut(pAggSup->pResultRows, &key, sizeof(SSessionKey), &winInfo, sizeof(SResultWindowInfo));
    }

    i += winRows;
  }
}

void doDeleteTimeWindows(SStreamAggSupporter* pAggSup, SSDataBlock* pBlock, SArray* result) {
  SColumnInfoData* pStartTsCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  TSKEY*           startDatas = (TSKEY*)pStartTsCol->pData;
  SColumnInfoData* pEndTsCol = taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
  TSKEY*           endDatas = (TSKEY*)pEndTsCol->pData;
  SColumnInfoData* pGroupCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  uint64_t*        gpDatas = (uint64_t*)pGroupCol->pData;
  for (int32_t i = 0; i < pBlock->info.rows; i++) {
    while (1) {
      SSessionKey curWin = {0};
      getCurSessionWindow(pAggSup, startDatas[i], endDatas[i], gpDatas[i], &curWin);
      if (IS_INVALID_SESSION_WIN_KEY(curWin)) {
        break;
      }
      doDeleteSessionWindow(pAggSup, &curWin);
      if (result) {
        saveDeleteInfo(result, curWin);
      }
    }
  }
}

inline int32_t sessionKeyCompareAsc(const void* pKey1, const void* pKey2) {
  SResultWindowInfo* pWinInfo1 = (SResultWindowInfo*)pKey1;
  SResultWindowInfo* pWinInfo2 = (SResultWindowInfo*)pKey2;
  SSessionKey*       pWin1 = &pWinInfo1->sessionWin;
  SSessionKey*       pWin2 = &pWinInfo2->sessionWin;

  if (pWin1->groupId > pWin2->groupId) {
    return 1;
  } else if (pWin1->groupId < pWin2->groupId) {
    return -1;
  }

  if (pWin1->win.skey > pWin2->win.skey) {
    return 1;
  } else if (pWin1->win.skey < pWin2->win.skey) {
    return -1;
  }

  return 0;
}

void doBuildDeleteDataBlock(SOperatorInfo* pOp, SSHashObj* pStDeleted, SSDataBlock* pBlock, void** Ite) {
  SStorageAPI* pAPI = &pOp->pTaskInfo->storageAPI;

  blockDataCleanup(pBlock);
  int32_t size = tSimpleHashGetSize(pStDeleted);
  if (size == 0) {
    return;
  }
  blockDataEnsureCapacity(pBlock, size);
  int32_t iter = 0;
  while (((*Ite) = tSimpleHashIterate(pStDeleted, *Ite, &iter)) != NULL) {
    if (pBlock->info.rows + 1 > pBlock->info.capacity) {
      break;
    }
    SSessionKey*     res = tSimpleHashGetKey(*Ite, NULL);
    SColumnInfoData* pStartTsCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
    colDataSetVal(pStartTsCol, pBlock->info.rows, (const char*)&res->win.skey, false);
    SColumnInfoData* pEndTsCol = taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
    colDataSetVal(pEndTsCol, pBlock->info.rows, (const char*)&res->win.skey, false);
    SColumnInfoData* pUidCol = taosArrayGet(pBlock->pDataBlock, UID_COLUMN_INDEX);
    colDataSetNULL(pUidCol, pBlock->info.rows);
    SColumnInfoData* pGpCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
    colDataSetVal(pGpCol, pBlock->info.rows, (const char*)&res->groupId, false);
    SColumnInfoData* pCalStCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX);
    colDataSetNULL(pCalStCol, pBlock->info.rows);
    SColumnInfoData* pCalEdCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX);
    colDataSetNULL(pCalEdCol, pBlock->info.rows);

    SColumnInfoData* pTableCol = taosArrayGet(pBlock->pDataBlock, TABLE_NAME_COLUMN_INDEX);

    void* tbname = NULL;
    pAPI->stateStore.streamStateGetParName(pOp->pTaskInfo->streamInfo.pState, res->groupId, &tbname);
    if (tbname == NULL) {
      colDataSetNULL(pTableCol, pBlock->info.rows);
    } else {
      char parTbName[VARSTR_HEADER_SIZE + TSDB_TABLE_NAME_LEN];
      STR_WITH_MAXSIZE_TO_VARSTR(parTbName, tbname, sizeof(parTbName));
      colDataSetVal(pTableCol, pBlock->info.rows, (const char*)parTbName, false);
      pAPI->stateStore.streamStateFreeVal(tbname);
    }
    pBlock->info.rows += 1;
  }
  if ((*Ite) == NULL) {
    tSimpleHashClear(pStDeleted);
  }
}

static void rebuildSessionWindow(SOperatorInfo* pOperator, SArray* pWinArray, SSHashObj* pStUpdated) {
  SExprSupp*     pSup = &pOperator->exprSupp;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*   pAPI = &pOperator->pTaskInfo->storageAPI;

  int32_t                        size = taosArrayGetSize(pWinArray);
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  SStreamAggSupporter*           pAggSup = &pInfo->streamAggSup;
  int32_t                        numOfOutput = pSup->numOfExprs;
  int32_t                        numOfChild = taosArrayGetSize(pInfo->pChildren);

  for (int32_t i = 0; i < size; i++) {
    SSessionKey*      pWinKey = taosArrayGet(pWinArray, i);
    int32_t           num = 0;
    SResultWindowInfo parentWin = {0};
    for (int32_t j = 0; j < numOfChild; j++) {
      SOperatorInfo*                 pChild = taosArrayGetP(pInfo->pChildren, j);
      SStreamSessionAggOperatorInfo* pChInfo = pChild->info;
      SStreamAggSupporter*           pChAggSup = &pChInfo->streamAggSup;
      SSessionKey                    chWinKey = {0};
      getSessionHashKey(pWinKey, &chWinKey);
      SStreamStateCur* pCur = pAggSup->stateStore.streamStateSessionSeekKeyCurrentNext(pChAggSup->pState, &chWinKey);
      SResultRow*      pResult = NULL;
      SResultRow*      pChResult = NULL;
      while (1) {
        SResultWindowInfo childWin = {0};
        childWin.sessionWin = *pWinKey;
        int32_t code = getSessionWinBuf(pChAggSup, pCur, &childWin);

        if (code == TSDB_CODE_SUCCESS && !inWinRange(&pAggSup->winRange, &childWin.sessionWin.win)) {
          releaseOutputBuf(pAggSup->pState, childWin.pStatePos, &pAggSup->stateStore);
          continue;
        }

        if (code == TSDB_CODE_SUCCESS && inWinRange(&pWinKey->win, &childWin.sessionWin.win)) {
          if (num == 0) {
            setSessionOutputBuf(pAggSup, pWinKey->win.skey, pWinKey->win.ekey, pWinKey->groupId, &parentWin);
            parentWin.sessionWin = childWin.sessionWin;
            memcpy(parentWin.pStatePos->pKey, &parentWin.sessionWin, sizeof(SSessionKey));
            code = initSessionOutputBuf(&parentWin, &pResult, pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset);
            if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
              releaseOutputBuf(pAggSup->pState, childWin.pStatePos, &pAggSup->stateStore);
              break;
            }
          }
          num++;
          parentWin.sessionWin.win.skey = TMIN(parentWin.sessionWin.win.skey, childWin.sessionWin.win.skey);
          parentWin.sessionWin.win.ekey = TMAX(parentWin.sessionWin.win.ekey, childWin.sessionWin.win.ekey);
          memcpy(parentWin.pStatePos->pKey, &parentWin.sessionWin, sizeof(SSessionKey));

          updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &parentWin.sessionWin.win, pAggSup->gap);
          initSessionOutputBuf(&childWin, &pChResult, pChild->exprSupp.pCtx, numOfOutput,
                               pChild->exprSupp.rowEntryInfoOffset);
          compactFunctions(pSup->pCtx, pChild->exprSupp.pCtx, numOfOutput, pTaskInfo, &pInfo->twAggSup.timeWindowData);
          compactSessionWindow(pOperator, &parentWin, pStUpdated, NULL, true);
          releaseOutputBuf(pAggSup->pState, childWin.pStatePos, &pAggSup->stateStore);
        } else {
          releaseOutputBuf(pAggSup->pState, childWin.pStatePos, &pAggSup->stateStore);
          break;
        }
      }
      pAPI->stateStore.streamStateFreeCur(pCur);
    }
    if (num > 0) {
      saveResult(parentWin, pStUpdated);
      saveSessionOutputBuf(pAggSup, &parentWin);
    }
  }
}

int32_t closeSessionWindow(SSHashObj* pHashMap, STimeWindowAggSupp* pTwSup, SSHashObj* pClosed) {
  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pHashMap, pIte, &iter)) != NULL) {
    SResultWindowInfo* pWinInfo = pIte;
    if (isCloseWindow(&pWinInfo->sessionWin.win, pTwSup)) {
      if (pTwSup->calTrigger == STREAM_TRIGGER_WINDOW_CLOSE && pClosed) {
        int32_t code = saveResult(*pWinInfo, pClosed);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
      SSessionKey* pKey = tSimpleHashGetKey(pIte, NULL);
      tSimpleHashIterateRemove(pHashMap, pKey, sizeof(SSessionKey), &pIte, &iter);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static void closeChildSessionWindow(SArray* pChildren, TSKEY maxTs) {
  int32_t size = taosArrayGetSize(pChildren);
  for (int32_t i = 0; i < size; i++) {
    SOperatorInfo*                 pChildOp = taosArrayGetP(pChildren, i);
    SStreamSessionAggOperatorInfo* pChInfo = pChildOp->info;
    pChInfo->twAggSup.maxTs = TMAX(pChInfo->twAggSup.maxTs, maxTs);
    closeSessionWindow(pChInfo->streamAggSup.pResultRows, &pChInfo->twAggSup, NULL);
  }
}

int32_t getAllSessionWindow(SSHashObj* pHashMap, SSHashObj* pStUpdated) {
  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pHashMap, pIte, &iter)) != NULL) {
    SResultWindowInfo* pWinInfo = pIte;
    if (!pWinInfo->pStatePos->beUpdated) {
      continue;
    }
    pWinInfo->pStatePos->beUpdated = false;
    saveResult(*pWinInfo, pStUpdated);
  }
  return TSDB_CODE_SUCCESS;
}

void copyDeleteWindowInfo(SArray* pResWins, SSHashObj* pStDeleted) {
  int32_t size = taosArrayGetSize(pResWins);
  for (int32_t i = 0; i < size; i++) {
    SSessionKey* pWinKey = taosArrayGet(pResWins, i);
    if (!pWinKey) continue;
    SSessionKey winInfo = {0};
    getSessionHashKey(pWinKey, &winInfo);
    tSimpleHashPut(pStDeleted, &winInfo, sizeof(SSessionKey), NULL, 0);
  }
}

// the allocated memory comes from outer function.
void initGroupResInfoFromArrayList(SGroupResInfo* pGroupResInfo, SArray* pArrayList) {
  pGroupResInfo->pRows = pArrayList;
  pGroupResInfo->index = 0;
  pGroupResInfo->pBuf = NULL;
  pGroupResInfo->freeItem = false;
}

int32_t buildSessionResultDataBlock(SOperatorInfo* pOperator, void* pState, SSDataBlock* pBlock, SExprSupp* pSup,
                                    SGroupResInfo* pGroupResInfo) {
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*    pAPI = &pTaskInfo->storageAPI;
  SExprInfo*      pExprInfo = pSup->pExprInfo;
  int32_t         numOfExprs = pSup->numOfExprs;
  int32_t*        rowEntryOffset = pSup->rowEntryInfoOffset;
  SqlFunctionCtx* pCtx = pSup->pCtx;

  int32_t numOfRows = getNumOfTotalRes(pGroupResInfo);

  for (int32_t i = pGroupResInfo->index; i < numOfRows; i += 1) {
    SResultWindowInfo* pWinInfo = taosArrayGet(pGroupResInfo->pRows, i);
    SRowBuffPos*       pPos = pWinInfo->pStatePos;
    SResultRow*        pRow = NULL;
    SSessionKey*       pKey = (SSessionKey*)pPos->pKey;

    if (pBlock->info.id.groupId == 0) {
      pBlock->info.id.groupId = pKey->groupId;

      void* tbname = NULL;
      if (pAPI->stateStore.streamStateGetParName((void*)pTaskInfo->streamInfo.pState, pBlock->info.id.groupId,
                                                 &tbname) < 0) {
        pBlock->info.parTbName[0] = 0;
      } else {
        memcpy(pBlock->info.parTbName, tbname, TSDB_TABLE_NAME_LEN);
      }
      pAPI->stateStore.streamStateFreeVal(tbname);
    } else {
      // current value belongs to different group, it can't be packed into one datablock
      if (pBlock->info.id.groupId != pKey->groupId) {
        break;
      }
    }

    int32_t code = pAPI->stateStore.streamStateGetByPos(pState, pPos, (void**)&pRow);

    if (code == -1) {
      // for history
      qWarn("===stream===not found session result key:%" PRId64 ", ekey:%" PRId64 ", groupId:%" PRIu64 "",
            pKey->win.skey, pKey->win.ekey, pKey->groupId);
      pGroupResInfo->index += 1;
      continue;
    }

    doUpdateNumOfRows(pCtx, pRow, numOfExprs, rowEntryOffset);
    // no results, continue to check the next one
    if (pRow->numOfRows == 0) {
      pGroupResInfo->index += 1;
      continue;
    }

    if (pBlock->info.rows + pRow->numOfRows > pBlock->info.capacity) {
      ASSERT(pBlock->info.rows > 0);
      break;
    }

    pGroupResInfo->index += 1;

    for (int32_t j = 0; j < numOfExprs; ++j) {
      int32_t slotId = pExprInfo[j].base.resSchema.slotId;

      pCtx[j].resultInfo = getResultEntryInfo(pRow, j, rowEntryOffset);
      if (pCtx[j].fpSet.finalize) {
        int32_t code1 = pCtx[j].fpSet.finalize(&pCtx[j], pBlock);
        if (TAOS_FAILED(code1)) {
          qError("%s build result data block error, code %s", GET_TASKID(pTaskInfo), tstrerror(code1));
          T_LONG_JMP(pTaskInfo->env, code1);
        }
      } else if (strcmp(pCtx[j].pExpr->pExpr->_function.functionName, "_select_value") == 0) {
        // do nothing, todo refactor
      } else {
        // expand the result into multiple rows. E.g., _wstart, top(k, 20)
        // the _wstart needs to copy to 20 following rows, since the results of top-k expands to 20 different rows.
        SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, slotId);
        char*            in = GET_ROWCELL_INTERBUF(pCtx[j].resultInfo);
        for (int32_t k = 0; k < pRow->numOfRows; ++k) {
          colDataSetVal(pColInfoData, pBlock->info.rows + k, in, pCtx[j].resultInfo->isNullRes);
        }
      }
    }

    pBlock->info.dataLoad = 1;
    pBlock->info.rows += pRow->numOfRows;
  }
  blockDataUpdateTsWindow(pBlock, 0);
  return TSDB_CODE_SUCCESS;
}

void doBuildSessionResult(SOperatorInfo* pOperator, void* pState, SGroupResInfo* pGroupResInfo, SSDataBlock* pBlock) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  // set output datablock version
  pBlock->info.version = pTaskInfo->version;

  blockDataCleanup(pBlock);
  if (!hasRemainResults(pGroupResInfo)) {
    cleanupGroupResInfo(pGroupResInfo);
    return;
  }

  // clear the existed group id
  pBlock->info.id.groupId = 0;
  buildSessionResultDataBlock(pOperator, pState, pBlock, &pOperator->exprSupp, pGroupResInfo);
  if (pBlock->info.rows == 0) {
    cleanupGroupResInfo(pGroupResInfo);
  }
}

static SSDataBlock* buildSessionResult(SOperatorInfo* pOperator) {
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  SStreamAggSupporter*           pAggSup = &pInfo->streamAggSup;
  SOptrBasicInfo*                pBInfo = &pInfo->binfo;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
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

void getMaxTsWins(const SArray* pAllWins, SArray* pMaxWins) {
  int32_t size = taosArrayGetSize(pAllWins);
  if (size == 0) {
    return;
  }
  SResultWindowInfo* pWinInfo = taosArrayGet(pAllWins, size - 1);
  SSessionKey*       pSeKey = pWinInfo->pStatePos->pKey;
  taosArrayPush(pMaxWins, pSeKey);
  if (pSeKey->groupId == 0) {
    return;
  }
  uint64_t preGpId = pSeKey->groupId;
  for (int32_t i = size - 2; i >= 0; i--) {
    pWinInfo = taosArrayGet(pAllWins, i);
    pSeKey = pWinInfo->pStatePos->pKey;
    if (preGpId != pSeKey->groupId) {
      taosArrayPush(pMaxWins, pSeKey);
      preGpId = pSeKey->groupId;
    }
  }
}

int32_t encodeSSessionKey(void** buf, SSessionKey* key) {
  int32_t tlen = 0;
  tlen += encodeSTimeWindow(buf, &key->win);
  tlen += taosEncodeFixedU64(buf, key->groupId);
  return tlen;
}

void* decodeSSessionKey(void* buf, SSessionKey* key) {
  buf = decodeSTimeWindow(buf, &key->win);
  buf = taosDecodeFixedU64(buf, &key->groupId);
  return buf;
}

int32_t encodeSResultWindowInfo(void** buf, SResultWindowInfo* key, int32_t outLen) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedBool(buf, key->isOutput);
  tlen += encodeSSessionKey(buf, &key->sessionWin);
  return tlen;
}

void* decodeSResultWindowInfo(void* buf, SResultWindowInfo* key, int32_t outLen) {
  buf = taosDecodeFixedBool(buf, &key->isOutput);
  key->pStatePos->pRowBuff = NULL;
  buf = decodeSSessionKey(buf, &key->sessionWin);
  return buf;
}

int32_t doStreamSessionEncodeOpState(void** buf, int32_t len, SOperatorInfo* pOperator, bool isParent) {
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
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

  // 3.pChildren
  int32_t size = taosArrayGetSize(pInfo->pChildren);
  tlen += taosEncodeFixedI32(buf, size);
  for (int32_t i = 0; i < size; i++) {
    SOperatorInfo* pChOp = taosArrayGetP(pInfo->pChildren, i);
    tlen += doStreamSessionEncodeOpState(buf, 0, pChOp, false);
  }

  // 4.dataVersion
  tlen += taosEncodeFixedI32(buf, pInfo->dataVersion);

  // 5.checksum
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

void* doStreamSessionDecodeOpState(void* buf, int32_t len, SOperatorInfo* pOperator, bool isParent) {
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  if (!pInfo) {
    return buf;
  }

  // 5.checksum
  if (isParent) {
    int32_t dataLen = len - sizeof(uint32_t);
    void*   pCksum = POINTER_SHIFT(buf, dataLen);
    if (taosCheckChecksum(buf, dataLen, *(uint32_t*)pCksum) != TSDB_CODE_SUCCESS) {
      qError("stream session state is invalid");
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

  // 3.pChildren
  int32_t size = 0;
  buf = taosDecodeFixedI32(buf, &size);
  ASSERT(size <= taosArrayGetSize(pInfo->pChildren));
  for (int32_t i = 0; i < size; i++) {
    SOperatorInfo* pChOp = taosArrayGetP(pInfo->pChildren, i);
    buf = doStreamSessionDecodeOpState(buf, 0, pChOp, false);
  }

  // 4.dataVersion
  buf = taosDecodeFixedI64(buf, &pInfo->dataVersion);
  return buf;
}

void doStreamSessionSaveCheckpoint(SOperatorInfo* pOperator) {
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  int32_t                        len = doStreamSessionEncodeOpState(NULL, 0, pOperator, true);
  void*                          buf = taosMemoryCalloc(1, len);
  void*                          pBuf = buf;
  len = doStreamSessionEncodeOpState(&pBuf, len, pOperator, true);
  pInfo->streamAggSup.stateStore.streamStateSaveInfo(pInfo->streamAggSup.pState, STREAM_SESSION_OP_CHECKPOINT_NAME,
                                                     strlen(STREAM_SESSION_OP_CHECKPOINT_NAME), buf, len);
  taosMemoryFree(buf);
}

void resetUnCloseSessionWinInfo(SSHashObj* winMap) {
  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(winMap, pIte, &iter)) != NULL) {
    SResultWindowInfo* pResInfo = pIte;
    pResInfo->pStatePos->beUsed = true;
  }
}

static SSDataBlock* doStreamSessionAgg(SOperatorInfo* pOperator) {
  SExprSupp*                     pSup = &pOperator->exprSupp;
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  SOptrBasicInfo*                pBInfo = &pInfo->binfo;
  SStreamAggSupporter*           pAggSup = &pInfo->streamAggSup;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  qDebug("stask:%s  %s status: %d", GET_TASKID(pTaskInfo), getStreamOpName(pOperator->operatorType), pOperator->status);
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  } else if (pOperator->status == OP_RES_TO_RETURN) {
    SSDataBlock* opRes = buildSessionResult(pOperator);
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

    if (pBlock->info.type == STREAM_DELETE_DATA || pBlock->info.type == STREAM_DELETE_RESULT ||
        pBlock->info.type == STREAM_CLEAR) {
      SArray* pWins = taosArrayInit(16, sizeof(SSessionKey));
      // gap must be 0
      doDeleteTimeWindows(pAggSup, pBlock, pWins);
      removeSessionResults(pAggSup, pInfo->pStUpdated, pWins);
      if (IS_FINAL_SESSION_OP(pOperator)) {
        int32_t                        childIndex = getChildIndex(pBlock);
        SOperatorInfo*                 pChildOp = taosArrayGetP(pInfo->pChildren, childIndex);
        SStreamSessionAggOperatorInfo* pChildInfo = pChildOp->info;
        // gap must be 0
        doDeleteTimeWindows(&pChildInfo->streamAggSup, pBlock, NULL);
        rebuildSessionWindow(pOperator, pWins, pInfo->pStUpdated);
      }
      copyDeleteWindowInfo(pWins, pInfo->pStDeleted);
      taosArrayDestroy(pWins);
      continue;
    } else if (pBlock->info.type == STREAM_GET_ALL) {
      pInfo->recvGetAll = true;
      getAllSessionWindow(pAggSup->pResultRows, pInfo->pStUpdated);
      continue;
    } else if (pBlock->info.type == STREAM_CREATE_CHILD_TABLE) {
      return pBlock;
    } else if (pBlock->info.type == STREAM_CHECKPOINT) {
      pAggSup->stateStore.streamStateCommit(pAggSup->pState);
      doStreamSessionSaveCheckpoint(pOperator);
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
    doStreamSessionAggImpl(pOperator, pBlock, pInfo->pStUpdated, pInfo->pStDeleted, IS_FINAL_SESSION_OP(pOperator),
                           true);
    if (IS_FINAL_SESSION_OP(pOperator)) {
      int32_t chIndex = getChildIndex(pBlock);
      int32_t size = taosArrayGetSize(pInfo->pChildren);
      // if chIndex + 1 - size > 0, add new child
      for (int32_t i = 0; i < chIndex + 1 - size; i++) {
        SOperatorInfo* pChildOp =
            createStreamFinalSessionAggOperatorInfo(NULL, pInfo->pPhyNode, pOperator->pTaskInfo, 0, NULL);
        if (!pChildOp) {
          qError("%s create stream child of final session error", GET_TASKID(pTaskInfo));
          T_LONG_JMP(pOperator->pTaskInfo->env, TSDB_CODE_OUT_OF_MEMORY);
        }
        taosArrayPush(pInfo->pChildren, &pChildOp);
      }
      SOperatorInfo* pChildOp = taosArrayGetP(pInfo->pChildren, chIndex);
      setInputDataBlock(&pChildOp->exprSupp, pBlock, TSDB_ORDER_ASC, MAIN_SCAN, true);
      doStreamSessionAggImpl(pChildOp, pBlock, NULL, NULL, true, false);
    }
    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.window.ekey);
    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.watermark);
  }
  // restore the value
  pOperator->status = OP_RES_TO_RETURN;

  closeSessionWindow(pAggSup->pResultRows, &pInfo->twAggSup, pInfo->pStUpdated);
  closeChildSessionWindow(pInfo->pChildren, pInfo->twAggSup.maxTs);
  copyUpdateResult(&pInfo->pStUpdated, pInfo->pUpdated, sessionKeyCompareAsc);
  removeSessionDeleteResults(pInfo->pStDeleted, pInfo->pUpdated);
  if (pInfo->isHistoryOp) {
    getMaxTsWins(pInfo->pUpdated, pInfo->historyWins);
  }
  initGroupResInfoFromArrayList(&pInfo->groupResInfo, pInfo->pUpdated);
  pInfo->pUpdated = NULL;
  blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);

  SSDataBlock* opRes = buildSessionResult(pOperator);
  if (opRes) {
    return opRes;
  }

  setStreamOperatorCompleted(pOperator);
  return NULL;
}

void streamSessionReleaseState(SOperatorInfo* pOperator) {
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  int32_t                        winSize = taosArrayGetSize(pInfo->historyWins) * sizeof(SSessionKey);
  int32_t                        resSize = winSize + sizeof(TSKEY);
  char*                          pBuff = taosMemoryCalloc(1, resSize);
  memcpy(pBuff, pInfo->historyWins->pData, winSize);
  memcpy(pBuff + winSize, &pInfo->twAggSup.maxTs, sizeof(TSKEY));
  pInfo->streamAggSup.stateStore.streamStateSaveInfo(pInfo->streamAggSup.pState, STREAM_SESSION_OP_STATE_NAME,
                                                     strlen(STREAM_SESSION_OP_STATE_NAME), pBuff, resSize);
  pInfo->streamAggSup.stateStore.streamStateCommit(pInfo->streamAggSup.pState);
  taosMemoryFreeClear(pBuff);
  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (downstream->fpSet.releaseStreamStateFn) {
    downstream->fpSet.releaseStreamStateFn(downstream);
  }
}

void resetWinRange(STimeWindow* winRange) {
  winRange->skey = INT64_MIN;
  winRange->ekey = INT64_MAX;
}

void getSessionWindowInfoByKey(SStreamAggSupporter* pAggSup, SSessionKey* pKey, SResultWindowInfo* pWinInfo) {
  int32_t rowSize = pAggSup->resultRowSize;
  int32_t code =
      pAggSup->stateStore.streamStateSessionGet(pAggSup->pState, pKey, (void**)&pWinInfo->pStatePos, &rowSize);
  if (code == TSDB_CODE_SUCCESS) {
    pWinInfo->sessionWin = *pKey;
    pWinInfo->isOutput = true;
    if (pWinInfo->pStatePos->needFree) {
      pAggSup->stateStore.streamStateSessionDel(pAggSup->pState, &pWinInfo->sessionWin);
    }
  } else {
    SET_SESSION_WIN_INVALID((*pWinInfo));
  }
}

void reloadAggSupFromDownStream(SOperatorInfo* downstream, SStreamAggSupporter* pAggSup) {
  SStateStore* pAPI = &downstream->pTaskInfo->storageAPI.stateStore;

  if (downstream->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    reloadAggSupFromDownStream(downstream->pDownstream[0], pAggSup);
    return;
  }

  SStreamScanInfo* pScanInfo = downstream->info;
  pAggSup->pUpdateInfo = pScanInfo->pUpdateInfo;
}

void streamSessionSemiReloadState(SOperatorInfo* pOperator) {
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  SStreamAggSupporter*           pAggSup = &pInfo->streamAggSup;
  resetWinRange(&pAggSup->winRange);

  SResultWindowInfo winInfo = {0};
  int32_t           size = 0;
  void*             pBuf = NULL;
  int32_t           code = pAggSup->stateStore.streamStateGetInfo(pAggSup->pState, STREAM_SESSION_OP_STATE_NAME,
                                                        strlen(STREAM_SESSION_OP_STATE_NAME), &pBuf, &size);
  int32_t           num = (size - sizeof(TSKEY)) / sizeof(SSessionKey);
  SSessionKey*      pSeKeyBuf = (SSessionKey*)pBuf;
  ASSERT(size == num * sizeof(SSessionKey) + sizeof(TSKEY));
  for (int32_t i = 0; i < num; i++) {
    SResultWindowInfo winInfo = {0};
    getSessionWindowInfoByKey(pAggSup, pSeKeyBuf + i, &winInfo);
    if (!IS_VALID_SESSION_WIN(winInfo)) {
      continue;
    }
    compactSessionSemiWindow(pOperator, &winInfo);
    saveSessionOutputBuf(pAggSup, &winInfo);
  }
  TSKEY ts = *(TSKEY*)((char*)pBuf + size - sizeof(TSKEY));
  taosMemoryFree(pBuf);
  pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, ts);
  pAggSup->stateStore.streamStateReloadInfo(pAggSup->pState, ts);

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (downstream->fpSet.reloadStreamStateFn) {
    downstream->fpSet.reloadStreamStateFn(downstream);
  }
  reloadAggSupFromDownStream(downstream, &pInfo->streamAggSup);
}

void streamSessionReloadState(SOperatorInfo* pOperator) {
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  SStreamAggSupporter*           pAggSup = &pInfo->streamAggSup;
  resetWinRange(&pAggSup->winRange);

  int32_t      size = 0;
  void*        pBuf = NULL;
  int32_t      code = pAggSup->stateStore.streamStateGetInfo(pAggSup->pState, STREAM_SESSION_OP_STATE_NAME,
                                                        strlen(STREAM_SESSION_OP_STATE_NAME), &pBuf, &size);
  int32_t      num = (size - sizeof(TSKEY)) / sizeof(SSessionKey);
  SSessionKey* pSeKeyBuf = (SSessionKey*)pBuf;
  ASSERT(size == num * sizeof(SSessionKey) + sizeof(TSKEY));

  TSKEY ts = *(TSKEY*)((char*)pBuf + size - sizeof(TSKEY));
  pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, ts);
  pAggSup->stateStore.streamStateReloadInfo(pAggSup->pState, ts);

  if (!pInfo->pStUpdated && num > 0) {
    _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
    pInfo->pStUpdated = tSimpleHashInit(64, hashFn);
  }
  for (int32_t i = 0; i < num; i++) {
    SResultWindowInfo winInfo = {0};
    getSessionWindowInfoByKey(pAggSup, pSeKeyBuf + i, &winInfo);
    if (!IS_VALID_SESSION_WIN(winInfo)) {
      continue;
    }
    int32_t winNum = compactSessionWindow(pOperator, &winInfo, pInfo->pStUpdated, pInfo->pStDeleted, true);
    if (winNum > 0) {
      qDebug("===stream=== reload state. save result %" PRId64 ", %" PRIu64, winInfo.sessionWin.win.skey,
             winInfo.sessionWin.groupId);
      if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE) {
        saveResult(winInfo, pInfo->pStUpdated);
      } else if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_WINDOW_CLOSE) {
        if (!isCloseWindow(&winInfo.sessionWin.win, &pInfo->twAggSup)) {
          saveDeleteRes(pInfo->pStDeleted, winInfo.sessionWin);
        }
        SSessionKey key = {0};
        getSessionHashKey(&winInfo.sessionWin, &key);
        tSimpleHashPut(pAggSup->pResultRows, &key, sizeof(SSessionKey), &winInfo, sizeof(SResultWindowInfo));
      }
    }
    saveSessionOutputBuf(pAggSup, &winInfo);
  }
  taosMemoryFree(pBuf);

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (downstream->fpSet.reloadStreamStateFn) {
    downstream->fpSet.reloadStreamStateFn(downstream);
  }
  reloadAggSupFromDownStream(downstream, &pInfo->streamAggSup);
}

SOperatorInfo* createStreamSessionAggOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode,
                                                  SExecTaskInfo* pTaskInfo, SReadHandle* pHandle) {
  SSessionWinodwPhysiNode*       pSessionNode = (SSessionWinodwPhysiNode*)pPhyNode;
  int32_t                        numOfCols = 0;
  int32_t                        code = TSDB_CODE_OUT_OF_MEMORY;
  SStreamSessionAggOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamSessionAggOperatorInfo));
  SOperatorInfo*                 pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pOperator->pTaskInfo = pTaskInfo;

  initResultSizeInfo(&pOperator->resultInfo, 4096);
  if (pSessionNode->window.pExprs != NULL) {
    int32_t    numOfScalar = 0;
    SExprInfo* pScalarExprInfo = createExprInfo(pSessionNode->window.pExprs, NULL, &numOfScalar);
    code = initExprSupp(&pInfo->scalarSupp, pScalarExprInfo, numOfScalar, &pTaskInfo->storageAPI.functionStore);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }
  SExprSupp* pExpSup = &pOperator->exprSupp;

  SExprInfo*   pExprInfo = createExprInfo(pSessionNode->window.pFuncs, NULL, &numOfCols);
  SSDataBlock* pResBlock = createDataBlockFromDescNode(pPhyNode->pOutputDataBlockDesc);
  code = initBasicInfoEx(&pInfo->binfo, pExpSup, pExprInfo, numOfCols, pResBlock, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->twAggSup = (STimeWindowAggSupp){
      .waterMark = pSessionNode->window.watermark,
      .calTrigger = pSessionNode->window.triggerType,
      .maxTs = INT64_MIN,
      .minTs = INT64_MAX,
      .deleteMark = getDeleteMark(&pSessionNode->window, 0),
  };

  code = initStreamAggSupporter(&pInfo->streamAggSup, pExpSup, numOfCols, pSessionNode->gap,
                                pTaskInfo->streamInfo.pState, 0, 0, &pTaskInfo->storageAPI.stateStore, pHandle,
                                &pInfo->twAggSup, GET_TASKID(pTaskInfo), &pTaskInfo->storageAPI);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);

  pInfo->primaryTsIndex = ((SColumnNode*)pSessionNode->window.pTspk)->slotId;
  if (pSessionNode->window.pTsEnd) {
    pInfo->endTsIndex = ((SColumnNode*)pSessionNode->window.pTsEnd)->slotId;
  }
  pInfo->binfo.pRes = pResBlock;
  pInfo->order = TSDB_ORDER_ASC;
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pInfo->pStDeleted = tSimpleHashInit(64, hashFn);
  pInfo->pDelIterator = NULL;
  pInfo->pDelRes = createSpecialDataBlock(STREAM_DELETE_RESULT);
  pInfo->pChildren = NULL;
  pInfo->pPhyNode = pPhyNode;
  pInfo->ignoreExpiredData = pSessionNode->window.igExpired;
  pInfo->ignoreExpiredDataSaved = false;
  pInfo->pUpdated = NULL;
  pInfo->pStUpdated = NULL;
  pInfo->dataVersion = 0;
  pInfo->historyWins = taosArrayInit(4, sizeof(SSessionKey));
  if (!pInfo->historyWins) {
    goto _error;
  }
  if (pHandle) {
    pInfo->isHistoryOp = pHandle->fillHistory;
  }

  pInfo->pCheckpointRes = createSpecialDataBlock(STREAM_CHECKPOINT);
  pInfo->clearState = false;
  pInfo->recvGetAll = false;

  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION;
  // for stream
  void*   buff = NULL;
  int32_t len = 0;
  int32_t res =
      pInfo->streamAggSup.stateStore.streamStateGetInfo(pInfo->streamAggSup.pState, STREAM_SESSION_OP_CHECKPOINT_NAME,
                                                        strlen(STREAM_SESSION_OP_CHECKPOINT_NAME), &buff, &len);
  if (res == TSDB_CODE_SUCCESS) {
    doStreamSessionDecodeOpState(buff, len, pOperator, true);
    taosMemoryFree(buff);
  }
  setOperatorInfo(pOperator, getStreamOpName(pOperator->operatorType), QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION, true,
                  OP_NOT_OPENED, pInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doStreamSessionAgg, NULL, destroyStreamSessionAggOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorStreamStateFn(pOperator, streamSessionReleaseState, streamSessionReloadState);

  if (downstream) {
    initDownStream(downstream, &pInfo->streamAggSup, pOperator->operatorType, pInfo->primaryTsIndex, &pInfo->twAggSup);
    code = appendDownstream(pOperator, &downstream, 1);
  }
  return pOperator;

_error:
  if (pInfo != NULL) {
    destroyStreamSessionAggOperatorInfo(pInfo);
  }

  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

static void clearStreamSessionOperator(SStreamSessionAggOperatorInfo* pInfo) {
  tSimpleHashClear(pInfo->streamAggSup.pResultRows);
  pInfo->streamAggSup.stateStore.streamStateSessionClear(pInfo->streamAggSup.pState);
}

void deleteSessionWinState(SStreamAggSupporter* pAggSup, SSDataBlock* pBlock, SSHashObj* pMapUpdate,
                           SSHashObj* pMapDelete) {
  SArray* pWins = taosArrayInit(16, sizeof(SSessionKey));
  doDeleteTimeWindows(pAggSup, pBlock, pWins);
  removeSessionResults(pAggSup, pMapUpdate, pWins);
  copyDeleteWindowInfo(pWins, pMapDelete);
  taosArrayDestroy(pWins);
}

static SSDataBlock* doStreamSessionSemiAgg(SOperatorInfo* pOperator) {
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  SOptrBasicInfo*                pBInfo = &pInfo->binfo;
  TSKEY                          maxTs = INT64_MIN;
  SExprSupp*                     pSup = &pOperator->exprSupp;
  SStreamAggSupporter*           pAggSup = &pInfo->streamAggSup;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;

  qDebug("stask:%s  %s status: %d", GET_TASKID(pTaskInfo), getStreamOpName(pOperator->operatorType), pOperator->status);
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  {
    SSDataBlock* opRes = buildSessionResult(pOperator);
    if (opRes) {
      return opRes;
    }

    if (pInfo->clearState) {
      clearFunctionContext(&pOperator->exprSupp);
      // semi session operator clear disk buffer
      clearStreamSessionOperator(pInfo);
    }

    if (pOperator->status == OP_RES_TO_RETURN) {
      if (pInfo->reCkBlock) {
        pInfo->reCkBlock = false;
        printDataBlock(pInfo->pCheckpointRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
        return pInfo->pCheckpointRes;
      }
      clearFunctionContext(&pOperator->exprSupp);
      // semi session operator clear disk buffer
      clearStreamSessionOperator(pInfo);
      setStreamOperatorCompleted(pOperator);
      pInfo->clearState = false;
      return NULL;
    }
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
      pOperator->status = OP_RES_TO_RETURN;
      break;
    }
    printSpecDataBlock(pBlock, getStreamOpName(pOperator->operatorType), "recv", GET_TASKID(pTaskInfo));

    if (pBlock->info.type == STREAM_DELETE_DATA || pBlock->info.type == STREAM_DELETE_RESULT ||
        pBlock->info.type == STREAM_CLEAR) {
      // gap must be 0
      deleteSessionWinState(pAggSup, pBlock, pInfo->pStUpdated, pInfo->pStDeleted);
      pInfo->clearState = true;
      break;
    } else if (pBlock->info.type == STREAM_GET_ALL) {
      getAllSessionWindow(pInfo->streamAggSup.pResultRows, pInfo->pStUpdated);
      continue;
    } else if (pBlock->info.type == STREAM_CREATE_CHILD_TABLE) {
      return pBlock;
    } else if (pBlock->info.type == STREAM_CHECKPOINT) {
      pAggSup->stateStore.streamStateCommit(pAggSup->pState);
      doStreamSessionSaveCheckpoint(pOperator);
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
    doStreamSessionAggImpl(pOperator, pBlock, pInfo->pStUpdated, NULL, false, false);
    maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.window.ekey);
  }

  pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, maxTs);
  pBInfo->pRes->info.watermark = pInfo->twAggSup.maxTs;

  copyUpdateResult(&pInfo->pStUpdated, pInfo->pUpdated, sessionKeyCompareAsc);
  removeSessionDeleteResults(pInfo->pStDeleted, pInfo->pUpdated);

  if (pInfo->isHistoryOp) {
    getMaxTsWins(pInfo->pUpdated, pInfo->historyWins);
  }

  initGroupResInfoFromArrayList(&pInfo->groupResInfo, pInfo->pUpdated);
  pInfo->pUpdated = NULL;
  blockDataEnsureCapacity(pBInfo->pRes, pOperator->resultInfo.capacity);

  SSDataBlock* opRes = buildSessionResult(pOperator);
  if (opRes) {
    return opRes;
  }

  clearFunctionContext(&pOperator->exprSupp);
  // semi session operator clear disk buffer
  clearStreamSessionOperator(pInfo);
  setStreamOperatorCompleted(pOperator);
  return NULL;
}

SOperatorInfo* createStreamFinalSessionAggOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode,
                                                       SExecTaskInfo* pTaskInfo, int32_t numOfChild,
                                                       SReadHandle* pHandle) {
  int32_t        code = TSDB_CODE_OUT_OF_MEMORY;
  SOperatorInfo* pOperator = createStreamSessionAggOperatorInfo(downstream, pPhyNode, pTaskInfo, pHandle);
  if (pOperator == NULL) {
    goto _error;
  }

  SStorageAPI*                   pAPI = &pTaskInfo->storageAPI;
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  pOperator->operatorType = pPhyNode->type;

  if (pPhyNode->type != QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION) {
    pOperator->fpSet =
        createOperatorFpSet(optrDummyOpenFn, doStreamSessionSemiAgg, NULL, destroyStreamSessionAggOperatorInfo,
                            optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
    setOperatorStreamStateFn(pOperator, streamSessionReleaseState, streamSessionSemiReloadState);
  }
  setOperatorInfo(pOperator, getStreamOpName(pOperator->operatorType), pPhyNode->type, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);

  if (numOfChild > 0) {
    pInfo->pChildren = taosArrayInit(numOfChild, sizeof(void*));
    for (int32_t i = 0; i < numOfChild; i++) {
      SOperatorInfo* pChildOp = createStreamFinalSessionAggOperatorInfo(NULL, pPhyNode, pTaskInfo, 0, pHandle);
      if (pChildOp == NULL) {
        goto _error;
      }
      SStreamSessionAggOperatorInfo* pChInfo = pChildOp->info;
      pChInfo->twAggSup.calTrigger = STREAM_TRIGGER_AT_ONCE;
      pAPI->stateStore.streamStateSetNumber(pChInfo->streamAggSup.pState, i);
      taosArrayPush(pInfo->pChildren, &pChildOp);
    }
  }

  if (!IS_FINAL_SESSION_OP(pOperator) || numOfChild == 0) {
    pInfo->twAggSup.calTrigger = STREAM_TRIGGER_AT_ONCE;
  }

  return pOperator;

_error:
  if (pInfo != NULL) {
    destroyStreamSessionAggOperatorInfo(pInfo);
  }
  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

void destroyStreamStateOperatorInfo(void* param) {
  SStreamStateAggOperatorInfo* pInfo = (SStreamStateAggOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);
  destroyStreamAggSupporter(&pInfo->streamAggSup);
  clearGroupResInfo(&pInfo->groupResInfo);
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
  tSimpleHashCleanup(pInfo->pSeDeleted);
  pInfo->pUpdated = taosArrayDestroy(pInfo->pUpdated);
  cleanupGroupResInfo(&pInfo->groupResInfo);

  taosArrayDestroy(pInfo->historyWins);
  blockDataDestroy(pInfo->pCheckpointRes);

  taosMemoryFreeClear(param);
}

bool isTsInWindow(SStateWindowInfo* pWin, TSKEY ts) {
  if (pWin->winInfo.sessionWin.win.skey <= ts && ts <= pWin->winInfo.sessionWin.win.ekey) {
    return true;
  }
  return false;
}

bool isEqualStateKey(SStateWindowInfo* pWin, char* pKeyData) {
  return pKeyData && compareVal(pKeyData, pWin->pStateKey);
}

bool compareStateKey(void* data, void* key) {
  if (!data || !key) {
    return true;
  }
  SStateKeys* stateKey = (SStateKeys*)key;
  stateKey->pData = (char*)key + sizeof(SStateKeys);
  return compareVal(data, stateKey);
}

bool compareWinStateKey(SStateKeys* left, SStateKeys* right) {
  if (!left || !right) {
    return false;
  }
  return compareVal(left->pData, right);
}

void getStateWindowInfoByKey(SStreamAggSupporter* pAggSup, SSessionKey* pKey, SStateWindowInfo* pCurWin,
                             SStateWindowInfo* pNextWin) {
  int32_t size = pAggSup->resultRowSize;
  pCurWin->winInfo.sessionWin.groupId = pKey->groupId;
  pCurWin->winInfo.sessionWin.win.skey = pKey->win.skey;
  pCurWin->winInfo.sessionWin.win.ekey = pKey->win.ekey;
  getSessionWindowInfoByKey(pAggSup, pKey, &pCurWin->winInfo);
  ASSERT(IS_VALID_SESSION_WIN(pCurWin->winInfo));
  pCurWin->pStateKey =
      (SStateKeys*)((char*)pCurWin->winInfo.pStatePos->pRowBuff + (pAggSup->resultRowSize - pAggSup->stateKeySize));
  pCurWin->pStateKey->bytes = pAggSup->stateKeySize - sizeof(SStateKeys);
  pCurWin->pStateKey->type = pAggSup->stateKeyType;
  pCurWin->pStateKey->pData = (char*)pCurWin->pStateKey + sizeof(SStateKeys);
  pCurWin->pStateKey->isNull = false;
  pCurWin->winInfo.isOutput = true;
  if (pCurWin->winInfo.pStatePos->needFree) {
    pAggSup->stateStore.streamStateSessionDel(pAggSup->pState, &pCurWin->winInfo.sessionWin);
  }

  qDebug("===stream===get state cur win buff. skey:%" PRId64 ", endkey:%" PRId64, pCurWin->winInfo.sessionWin.win.skey,
         pCurWin->winInfo.sessionWin.win.ekey);

  pNextWin->winInfo.sessionWin = pCurWin->winInfo.sessionWin;
  SStreamStateCur* pCur =
      pAggSup->stateStore.streamStateSessionSeekKeyNext(pAggSup->pState, &pNextWin->winInfo.sessionWin);
  int32_t nextSize = pAggSup->resultRowSize;
  int32_t code = pAggSup->stateStore.streamStateSessionGetKVByCur(pCur, &pNextWin->winInfo.sessionWin,
                                                                  (void**)&pNextWin->winInfo.pStatePos, &nextSize);
  if (code != TSDB_CODE_SUCCESS) {
    SET_SESSION_WIN_INVALID(pNextWin->winInfo);
  } else {
    pNextWin->pStateKey =
        (SStateKeys*)((char*)pNextWin->winInfo.pStatePos->pRowBuff + (pAggSup->resultRowSize - pAggSup->stateKeySize));
    pNextWin->pStateKey->bytes = pAggSup->stateKeySize - sizeof(SStateKeys);
    pNextWin->pStateKey->type = pAggSup->stateKeyType;
    pNextWin->pStateKey->pData = (char*)pNextWin->pStateKey + sizeof(SStateKeys);
    pNextWin->pStateKey->isNull = false;
    pNextWin->winInfo.isOutput = true;
  }
  pAggSup->stateStore.streamStateFreeCur(pCur);
  qDebug("===stream===get state next win buff. skey:%" PRId64 ", endkey:%" PRId64,
         pNextWin->winInfo.sessionWin.win.skey, pNextWin->winInfo.sessionWin.win.ekey);
}

void setStateOutputBuf(SStreamAggSupporter* pAggSup, TSKEY ts, uint64_t groupId, char* pKeyData,
                       SStateWindowInfo* pCurWin, SStateWindowInfo* pNextWin) {
  int32_t size = pAggSup->resultRowSize;
  pCurWin->winInfo.sessionWin.groupId = groupId;
  pCurWin->winInfo.sessionWin.win.skey = ts;
  pCurWin->winInfo.sessionWin.win.ekey = ts;
  int32_t code = pAggSup->stateStore.streamStateStateAddIfNotExist(pAggSup->pState, &pCurWin->winInfo.sessionWin,
                                                                   pKeyData, pAggSup->stateKeySize, compareStateKey,
                                                                   (void**)&pCurWin->winInfo.pStatePos, &size);
  pCurWin->pStateKey =
      (SStateKeys*)((char*)pCurWin->winInfo.pStatePos->pRowBuff + (pAggSup->resultRowSize - pAggSup->stateKeySize));
  pCurWin->pStateKey->bytes = pAggSup->stateKeySize - sizeof(SStateKeys);
  pCurWin->pStateKey->type = pAggSup->stateKeyType;
  pCurWin->pStateKey->pData = (char*)pCurWin->pStateKey + sizeof(SStateKeys);
  pCurWin->pStateKey->isNull = false;

  if (code == TSDB_CODE_SUCCESS && !inWinRange(&pAggSup->winRange, &pCurWin->winInfo.sessionWin.win)) {
    code = TSDB_CODE_FAILED;
    clearOutputBuf(pAggSup->pState, pCurWin->winInfo.pStatePos, &pAggSup->pSessionAPI->stateStore);
    pCurWin->pStateKey =
        (SStateKeys*)((char*)pCurWin->winInfo.pStatePos->pRowBuff + (pAggSup->resultRowSize - pAggSup->stateKeySize));
    pCurWin->pStateKey->bytes = pAggSup->stateKeySize - sizeof(SStateKeys);
    pCurWin->pStateKey->type = pAggSup->stateKeyType;
    pCurWin->pStateKey->pData = (char*)pCurWin->pStateKey + sizeof(SStateKeys);
    pCurWin->pStateKey->isNull = false;
    pCurWin->winInfo.sessionWin.groupId = groupId;
    pCurWin->winInfo.sessionWin.win.skey = ts;
    pCurWin->winInfo.sessionWin.win.ekey = ts;
    qDebug("===stream===reset state win key. skey:%" PRId64 ", endkey:%" PRId64, pCurWin->winInfo.sessionWin.win.skey,
           pCurWin->winInfo.sessionWin.win.ekey);
  }

  if (code == TSDB_CODE_SUCCESS) {
    pCurWin->winInfo.isOutput = true;
    if (pCurWin->winInfo.pStatePos->needFree) {
      pAggSup->stateStore.streamStateSessionDel(pAggSup->pState, &pCurWin->winInfo.sessionWin);
    }
  } else if (pKeyData) {
    if (IS_VAR_DATA_TYPE(pAggSup->stateKeyType)) {
      varDataCopy(pCurWin->pStateKey->pData, pKeyData);
    } else {
      memcpy(pCurWin->pStateKey->pData, pKeyData, pCurWin->pStateKey->bytes);
    }
  }

  qDebug("===stream===set state cur win buff. skey:%" PRId64 ", endkey:%" PRId64, pCurWin->winInfo.sessionWin.win.skey,
         pCurWin->winInfo.sessionWin.win.ekey);

  pNextWin->winInfo.sessionWin = pCurWin->winInfo.sessionWin;
  SStreamStateCur* pCur =
      pAggSup->stateStore.streamStateSessionSeekKeyNext(pAggSup->pState, &pNextWin->winInfo.sessionWin);
  int32_t nextSize = pAggSup->resultRowSize;
  code = pAggSup->stateStore.streamStateSessionGetKVByCur(pCur, &pNextWin->winInfo.sessionWin,
                                                          (void**)&pNextWin->winInfo.pStatePos, &nextSize);
  if (code != TSDB_CODE_SUCCESS) {
    SET_SESSION_WIN_INVALID(pNextWin->winInfo);
  } else {
    pNextWin->pStateKey =
        (SStateKeys*)((char*)pNextWin->winInfo.pStatePos->pRowBuff + (pAggSup->resultRowSize - pAggSup->stateKeySize));
    pNextWin->pStateKey->bytes = pAggSup->stateKeySize - sizeof(SStateKeys);
    pNextWin->pStateKey->type = pAggSup->stateKeyType;
    pNextWin->pStateKey->pData = (char*)pNextWin->pStateKey + sizeof(SStateKeys);
    pNextWin->pStateKey->isNull = false;
    pNextWin->winInfo.isOutput = true;
  }
  pAggSup->stateStore.streamStateFreeCur(pCur);
  qDebug("===stream===set state next win buff. skey:%" PRId64 ", endkey:%" PRId64,
         pNextWin->winInfo.sessionWin.win.skey, pNextWin->winInfo.sessionWin.win.ekey);
}

int32_t updateStateWindowInfo(SStreamAggSupporter* pAggSup, SStateWindowInfo* pWinInfo, SStateWindowInfo* pNextWin,
                              TSKEY* pTs, uint64_t groupId, SColumnInfoData* pKeyCol, int32_t rows, int32_t start,
                              bool* allEqual, SSHashObj* pResultRows, SSHashObj* pSeUpdated, SSHashObj* pSeDeleted) {
  *allEqual = true;
  for (int32_t i = start; i < rows; ++i) {
    char* pKeyData = colDataGetData(pKeyCol, i);
    if (!isTsInWindow(pWinInfo, pTs[i])) {
      if (isEqualStateKey(pWinInfo, pKeyData)) {
        if (IS_VALID_SESSION_WIN(pNextWin->winInfo)) {
          // ts belongs to the next window
          if (pTs[i] >= pNextWin->winInfo.sessionWin.win.skey) {
            return i - start;
          }
        }
      } else {
        return i - start;
      }
    }

    if (pWinInfo->winInfo.sessionWin.win.skey > pTs[i]) {
      if (pSeDeleted && pWinInfo->winInfo.isOutput) {
        saveDeleteRes(pSeDeleted, pWinInfo->winInfo.sessionWin);
      }
      removeSessionResult(pAggSup, pSeUpdated, pResultRows, &pWinInfo->winInfo.sessionWin);
      pWinInfo->winInfo.sessionWin.win.skey = pTs[i];
    }
    pWinInfo->winInfo.sessionWin.win.ekey = TMAX(pWinInfo->winInfo.sessionWin.win.ekey, pTs[i]);
    memcpy(pWinInfo->winInfo.pStatePos->pKey, &pWinInfo->winInfo.sessionWin, sizeof(SSessionKey));
    if (!isEqualStateKey(pWinInfo, pKeyData)) {
      *allEqual = false;
    }
  }
  return rows - start;
}

static void doStreamStateAggImpl(SOperatorInfo* pOperator, SSDataBlock* pSDataBlock, SSHashObj* pSeUpdated,
                                 SSHashObj* pStDeleted) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*   pAPI = &pOperator->pTaskInfo->storageAPI;

  SStreamStateAggOperatorInfo* pInfo = pOperator->info;
  int32_t                      numOfOutput = pOperator->exprSupp.numOfExprs;
  uint64_t                     groupId = pSDataBlock->info.id.groupId;
  int64_t                      code = TSDB_CODE_SUCCESS;
  TSKEY*                       tsCols = NULL;
  SResultRow*                  pResult = NULL;
  int32_t                      winRows = 0;
  SStreamAggSupporter*         pAggSup = &pInfo->streamAggSup;

  pInfo->dataVersion = TMAX(pInfo->dataVersion, pSDataBlock->info.version);
  pAggSup->winRange = pTaskInfo->streamInfo.fillHistoryWindow;
  if (pAggSup->winRange.ekey <= 0) {
    pAggSup->winRange.ekey = INT64_MAX;
  }

  if (pSDataBlock->pDataBlock != NULL) {
    SColumnInfoData* pColDataInfo = taosArrayGet(pSDataBlock->pDataBlock, pInfo->primaryTsIndex);
    tsCols = (int64_t*)pColDataInfo->pData;
  } else {
    return;
  }

  int32_t rows = pSDataBlock->info.rows;
  blockDataEnsureCapacity(pAggSup->pScanBlock, rows);
  SColumnInfoData* pKeyColInfo = taosArrayGet(pSDataBlock->pDataBlock, pInfo->stateCol.slotId);
  for (int32_t i = 0; i < rows; i += winRows) {
    if (pInfo->ignoreExpiredData && checkExpiredData(&pInfo->streamAggSup.stateStore, pInfo->streamAggSup.pUpdateInfo,
                                                     &pInfo->twAggSup, pSDataBlock->info.id.uid, tsCols[i]) ||
        colDataIsNull_s(pKeyColInfo, i)) {
      i++;
      continue;
    }
    char*            pKeyData = colDataGetData(pKeyColInfo, i);
    int32_t          winIndex = 0;
    bool             allEqual = true;
    SStateWindowInfo curWin = {0};
    SStateWindowInfo nextWin = {0};
    setStateOutputBuf(pAggSup, tsCols[i], groupId, pKeyData, &curWin, &nextWin);
    releaseOutputBuf(pAggSup->pState, nextWin.winInfo.pStatePos, &pAPI->stateStore);

    setSessionWinOutputInfo(pSeUpdated, &curWin.winInfo);
    winRows = updateStateWindowInfo(pAggSup, &curWin, &nextWin, tsCols, groupId, pKeyColInfo, rows, i, &allEqual,
                                    pAggSup->pResultRows, pSeUpdated, pStDeleted);
    if (!allEqual) {
      uint64_t uid = 0;
      appendOneRowToStreamSpecialBlock(pAggSup->pScanBlock, &curWin.winInfo.sessionWin.win.skey,
                                       &curWin.winInfo.sessionWin.win.ekey, &uid, &groupId, NULL);
      tSimpleHashRemove(pSeUpdated, &curWin.winInfo.sessionWin, sizeof(SSessionKey));
      doDeleteSessionWindow(pAggSup, &curWin.winInfo.sessionWin);
      releaseOutputBuf(pAggSup->pState, curWin.winInfo.pStatePos, &pAPI->stateStore);
      continue;
    }
    code = doOneWindowAggImpl(&pInfo->twAggSup.timeWindowData, &curWin.winInfo, &pResult, i, winRows, rows, numOfOutput,
                              pOperator, 0);
    if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
      qError("%s do one window aggregate impl error, code %s", GET_TASKID(pTaskInfo), tstrerror(code));
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_OUT_OF_MEMORY);
    }
    saveSessionOutputBuf(pAggSup, &curWin.winInfo);

    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE) {
      code = saveResult(curWin.winInfo, pSeUpdated);
      if (code != TSDB_CODE_SUCCESS) {
        qError("%s do stream state aggregate impl, set result error, code %s", GET_TASKID(pTaskInfo), tstrerror(code));
        T_LONG_JMP(pTaskInfo->env, TSDB_CODE_OUT_OF_MEMORY);
      }
    }

    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_WINDOW_CLOSE) {
      curWin.winInfo.pStatePos->beUpdated = true;
      SSessionKey key = {0};
      getSessionHashKey(&curWin.winInfo.sessionWin, &key);
      tSimpleHashPut(pAggSup->pResultRows, &key, sizeof(SSessionKey), &curWin.winInfo, sizeof(SResultWindowInfo));
    }
  }
}

int32_t doStreamStateEncodeOpState(void** buf, int32_t len, SOperatorInfo* pOperator, bool isParent) {
  SStreamStateAggOperatorInfo* pInfo = pOperator->info;
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

  // 3.pChildren
  int32_t size = taosArrayGetSize(pInfo->pChildren);
  tlen += taosEncodeFixedI32(buf, size);
  for (int32_t i = 0; i < size; i++) {
    SOperatorInfo* pChOp = taosArrayGetP(pInfo->pChildren, i);
    tlen += doStreamStateEncodeOpState(buf, 0, pChOp, false);
  }

  // 4.dataVersion
  tlen += taosEncodeFixedI32(buf, pInfo->dataVersion);

  // 5.checksum
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

void* doStreamStateDecodeOpState(void* buf, int32_t len, SOperatorInfo* pOperator, bool isParent) {
  SStreamStateAggOperatorInfo* pInfo = pOperator->info;
  if (!pInfo) {
    return buf;
  }

  // 5.checksum
  if (isParent) {
    int32_t dataLen = len - sizeof(uint32_t);
    void*   pCksum = POINTER_SHIFT(buf, dataLen);
    if (taosCheckChecksum(buf, dataLen, *(uint32_t*)pCksum) != TSDB_CODE_SUCCESS) {
      qError("stream state_window state is invalid");
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

  // 3.pChildren
  int32_t size = 0;
  buf = taosDecodeFixedI32(buf, &size);
  ASSERT(size <= taosArrayGetSize(pInfo->pChildren));
  for (int32_t i = 0; i < size; i++) {
    SOperatorInfo* pChOp = taosArrayGetP(pInfo->pChildren, i);
    buf = doStreamStateDecodeOpState(buf, 0, pChOp, false);
  }

  // 4.dataVersion
  buf = taosDecodeFixedI64(buf, &pInfo->dataVersion);
  return buf;
}

void doStreamStateSaveCheckpoint(SOperatorInfo* pOperator) {
  SStreamStateAggOperatorInfo* pInfo = pOperator->info;
  int32_t                      len = doStreamStateEncodeOpState(NULL, 0, pOperator, true);
  void*                        buf = taosMemoryCalloc(1, len);
  void*                        pBuf = buf;
  len = doStreamStateEncodeOpState(&pBuf, len, pOperator, true);
  pInfo->streamAggSup.stateStore.streamStateSaveInfo(pInfo->streamAggSup.pState, STREAM_STATE_OP_CHECKPOINT_NAME,
                                                     strlen(STREAM_STATE_OP_CHECKPOINT_NAME), buf, len);
  taosMemoryFree(buf);
}

static SSDataBlock* buildStateResult(SOperatorInfo* pOperator) {
  SStreamStateAggOperatorInfo* pInfo = pOperator->info;
  SOptrBasicInfo*              pBInfo = &pInfo->binfo;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;

  doBuildDeleteDataBlock(pOperator, pInfo->pSeDeleted, pInfo->pDelRes, &pInfo->pDelIterator);
  if (pInfo->pDelRes->info.rows > 0) {
    printDataBlock(pInfo->pDelRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
    return pInfo->pDelRes;
  }

  doBuildSessionResult(pOperator, pInfo->streamAggSup.pState, &pInfo->groupResInfo, pBInfo->pRes);
  if (pBInfo->pRes->info.rows > 0) {
    printDataBlock(pBInfo->pRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
    return pBInfo->pRes;
  }
  return NULL;
}

static SSDataBlock* doStreamStateAgg(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExprSupp*                   pSup = &pOperator->exprSupp;
  SStreamStateAggOperatorInfo* pInfo = pOperator->info;
  SOptrBasicInfo*              pBInfo = &pInfo->binfo;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  qDebug("===stream=== stream state agg");
  if (pOperator->status == OP_RES_TO_RETURN) {
    SSDataBlock* resBlock = buildStateResult(pOperator);
    if (resBlock != NULL) {
      return resBlock;
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
  if (!pInfo->pSeUpdated) {
    _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
    pInfo->pSeUpdated = tSimpleHashInit(64, hashFn);
  }
  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      break;
    }
    printSpecDataBlock(pBlock, getStreamOpName(pOperator->operatorType), "recv", GET_TASKID(pTaskInfo));

    if (pBlock->info.type == STREAM_DELETE_DATA || pBlock->info.type == STREAM_DELETE_RESULT ||
        pBlock->info.type == STREAM_CLEAR) {
      deleteSessionWinState(&pInfo->streamAggSup, pBlock, pInfo->pSeUpdated, pInfo->pSeDeleted);
      continue;
    } else if (pBlock->info.type == STREAM_GET_ALL) {
      pInfo->recvGetAll = true;
      getAllSessionWindow(pInfo->streamAggSup.pResultRows, pInfo->pSeUpdated);
      continue;
    } else if (pBlock->info.type == STREAM_CREATE_CHILD_TABLE) {
      return pBlock;
    } else if (pBlock->info.type == STREAM_CHECKPOINT) {
      pInfo->streamAggSup.stateStore.streamStateCommit(pInfo->streamAggSup.pState);
      doStreamStateSaveCheckpoint(pOperator);
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
    doStreamStateAggImpl(pOperator, pBlock, pInfo->pSeUpdated, pInfo->pSeDeleted);
    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.window.ekey);
  }
  // restore the value
  pOperator->status = OP_RES_TO_RETURN;

  closeSessionWindow(pInfo->streamAggSup.pResultRows, &pInfo->twAggSup, pInfo->pSeUpdated);
  copyUpdateResult(&pInfo->pSeUpdated, pInfo->pUpdated, sessionKeyCompareAsc);
  removeSessionDeleteResults(pInfo->pSeDeleted, pInfo->pUpdated);

  if (pInfo->isHistoryOp) {
    getMaxTsWins(pInfo->pUpdated, pInfo->historyWins);
  }

  initGroupResInfoFromArrayList(&pInfo->groupResInfo, pInfo->pUpdated);
  pInfo->pUpdated = NULL;
  blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);

  SSDataBlock* resBlock = buildStateResult(pOperator);
  if (resBlock != NULL) {
    return resBlock;
  }
  setStreamOperatorCompleted(pOperator);
  return NULL;
}

void streamStateReleaseState(SOperatorInfo* pOperator) {
  SStreamStateAggOperatorInfo* pInfo = pOperator->info;
  int32_t                      winSize = taosArrayGetSize(pInfo->historyWins) * sizeof(SSessionKey);
  int32_t                      resSize = winSize + sizeof(TSKEY);
  char*                        pBuff = taosMemoryCalloc(1, resSize);
  memcpy(pBuff, pInfo->historyWins->pData, winSize);
  memcpy(pBuff + winSize, &pInfo->twAggSup.maxTs, sizeof(TSKEY));
  qDebug("===stream=== relase state. save result count:%d", (int32_t)taosArrayGetSize(pInfo->historyWins));
  pInfo->streamAggSup.stateStore.streamStateSaveInfo(pInfo->streamAggSup.pState, STREAM_STATE_OP_STATE_NAME,
                                                     strlen(STREAM_STATE_OP_STATE_NAME), pBuff, resSize);
  pInfo->streamAggSup.stateStore.streamStateCommit(pInfo->streamAggSup.pState);
  taosMemoryFreeClear(pBuff);

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (downstream->fpSet.releaseStreamStateFn) {
    downstream->fpSet.releaseStreamStateFn(downstream);
  }
}

static void compactStateWindow(SOperatorInfo* pOperator, SResultWindowInfo* pCurWin, SResultWindowInfo* pNextWin,
                               SSHashObj* pStUpdated, SSHashObj* pStDeleted) {
  SExprSupp*                   pSup = &pOperator->exprSupp;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  SStreamStateAggOperatorInfo* pInfo = pOperator->info;
  compactTimeWindow(pSup, &pInfo->streamAggSup, &pInfo->twAggSup, pTaskInfo, pCurWin, pNextWin, pStUpdated, pStDeleted,
                    false);
}

void streamStateReloadState(SOperatorInfo* pOperator) {
  SStreamStateAggOperatorInfo* pInfo = pOperator->info;
  SStreamAggSupporter*         pAggSup = &pInfo->streamAggSup;
  resetWinRange(&pAggSup->winRange);

  SSessionKey seKey = {.win.skey = INT64_MIN, .win.ekey = INT64_MIN, .groupId = 0};
  int32_t     size = 0;
  void*       pBuf = NULL;
  int32_t     code = pAggSup->stateStore.streamStateGetInfo(pAggSup->pState, STREAM_STATE_OP_STATE_NAME,
                                                        strlen(STREAM_STATE_OP_STATE_NAME), &pBuf, &size);
  int32_t     num = (size - sizeof(TSKEY)) / sizeof(SSessionKey);
  qDebug("===stream=== reload state. get result count:%d", num);
  SSessionKey* pSeKeyBuf = (SSessionKey*)pBuf;
  ASSERT(size == num * sizeof(SSessionKey) + sizeof(TSKEY));

  TSKEY ts = *(TSKEY*)((char*)pBuf + size - sizeof(TSKEY));
  pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, ts);
  pAggSup->stateStore.streamStateReloadInfo(pAggSup->pState, ts);

  if (!pInfo->pSeUpdated && num > 0) {
    _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
    pInfo->pSeUpdated = tSimpleHashInit(64, hashFn);
  }
  if (!pInfo->pSeDeleted && num > 0) {
    _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
    pInfo->pSeDeleted = tSimpleHashInit(64, hashFn);
  }
  for (int32_t i = 0; i < num; i++) {
    SStateWindowInfo curInfo = {0};
    SStateWindowInfo nextInfo = {0};
    qDebug("===stream=== reload state. try process result %" PRId64 ", %" PRIu64 ", index:%d", pSeKeyBuf[i].win.skey,
           pSeKeyBuf[i].groupId, i);
    getStateWindowInfoByKey(pAggSup, pSeKeyBuf + i, &curInfo, &nextInfo);
    bool cpRes = compareWinStateKey(curInfo.pStateKey, nextInfo.pStateKey);
    qDebug("===stream=== reload state. next window info %" PRId64 ", %" PRIu64 ", compare:%d",
           nextInfo.winInfo.sessionWin.win.skey, nextInfo.winInfo.sessionWin.groupId, cpRes);
    if (cpRes) {
      compactStateWindow(pOperator, &curInfo.winInfo, &nextInfo.winInfo, pInfo->pSeUpdated, pInfo->pSeDeleted);
      qDebug("===stream=== reload state. save result %" PRId64 ", %" PRIu64, curInfo.winInfo.sessionWin.win.skey,
             curInfo.winInfo.sessionWin.groupId);
      if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE) {
        saveResult(curInfo.winInfo, pInfo->pSeUpdated);
      } else if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_WINDOW_CLOSE) {
        if (!isCloseWindow(&curInfo.winInfo.sessionWin.win, &pInfo->twAggSup)) {
          saveDeleteRes(pInfo->pSeDeleted, curInfo.winInfo.sessionWin);
        }
        SSessionKey key = {0};
        getSessionHashKey(&curInfo.winInfo.sessionWin, &key);
        tSimpleHashPut(pAggSup->pResultRows, &key, sizeof(SSessionKey), &curInfo.winInfo, sizeof(SResultWindowInfo));
      }
    } else if (IS_VALID_SESSION_WIN(nextInfo.winInfo)) {
      releaseOutputBuf(pAggSup->pState, nextInfo.winInfo.pStatePos, &pAggSup->pSessionAPI->stateStore);
    }

    if (IS_VALID_SESSION_WIN(curInfo.winInfo)) {
      saveSessionOutputBuf(pAggSup, &curInfo.winInfo);
    }
  }
  taosMemoryFree(pBuf);

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (downstream->fpSet.reloadStreamStateFn) {
    downstream->fpSet.reloadStreamStateFn(downstream);
  }
  reloadAggSupFromDownStream(downstream, &pInfo->streamAggSup);
}

SOperatorInfo* createStreamStateAggOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode,
                                                SExecTaskInfo* pTaskInfo, SReadHandle* pHandle) {
  SStreamStateWinodwPhysiNode* pStateNode = (SStreamStateWinodwPhysiNode*)pPhyNode;
  int32_t                      tsSlotId = ((SColumnNode*)pStateNode->window.pTspk)->slotId;
  SColumnNode*                 pColNode = (SColumnNode*)(pStateNode->pStateKey);
  int32_t                      code = TSDB_CODE_SUCCESS;

  SStreamStateAggOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamStateAggOperatorInfo));
  SOperatorInfo*               pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  pInfo->stateCol = extractColumnFromColumnNode(pColNode);
  initResultSizeInfo(&pOperator->resultInfo, 4096);
  if (pStateNode->window.pExprs != NULL) {
    int32_t    numOfScalar = 0;
    SExprInfo* pScalarExprInfo = createExprInfo(pStateNode->window.pExprs, NULL, &numOfScalar);
    code = initExprSupp(&pInfo->scalarSupp, pScalarExprInfo, numOfScalar, &pTaskInfo->storageAPI.functionStore);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  pInfo->twAggSup = (STimeWindowAggSupp){
      .waterMark = pStateNode->window.watermark,
      .calTrigger = pStateNode->window.triggerType,
      .maxTs = INT64_MIN,
      .minTs = INT64_MAX,
      .deleteMark = getDeleteMark(&pStateNode->window, 0),
  };

  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);

  SExprSupp*   pExpSup = &pOperator->exprSupp;
  int32_t      numOfCols = 0;
  SExprInfo*   pExprInfo = createExprInfo(pStateNode->window.pFuncs, NULL, &numOfCols);
  SSDataBlock* pResBlock = createDataBlockFromDescNode(pPhyNode->pOutputDataBlockDesc);
  code = initBasicInfoEx(&pInfo->binfo, pExpSup, pExprInfo, numOfCols, pResBlock, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  int32_t keySize = sizeof(SStateKeys) + pColNode->node.resType.bytes;
  int16_t type = pColNode->node.resType.type;
  code = initStreamAggSupporter(&pInfo->streamAggSup, pExpSup, numOfCols, 0, pTaskInfo->streamInfo.pState, keySize,
                                type, &pTaskInfo->storageAPI.stateStore, pHandle, &pInfo->twAggSup,
                                GET_TASKID(pTaskInfo), &pTaskInfo->storageAPI);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->primaryTsIndex = tsSlotId;
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pInfo->pSeDeleted = tSimpleHashInit(64, hashFn);
  pInfo->pDelIterator = NULL;
  pInfo->pDelRes = createSpecialDataBlock(STREAM_DELETE_RESULT);
  pInfo->pChildren = NULL;
  pInfo->ignoreExpiredData = pStateNode->window.igExpired;
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

  pInfo->pCheckpointRes = createSpecialDataBlock(STREAM_CHECKPOINT);
  pInfo->recvGetAll = false;

  // for stream
  void*   buff = NULL;
  int32_t len = 0;
  int32_t res =
      pInfo->streamAggSup.stateStore.streamStateGetInfo(pInfo->streamAggSup.pState, STREAM_STATE_OP_CHECKPOINT_NAME,
                                                        strlen(STREAM_STATE_OP_CHECKPOINT_NAME), &buff, &len);
  if (res == TSDB_CODE_SUCCESS) {
    doStreamStateDecodeOpState(buff, len, pOperator, true);
    taosMemoryFree(buff);
  }

  setOperatorInfo(pOperator, "StreamStateAggOperator", QUERY_NODE_PHYSICAL_PLAN_STREAM_STATE, true, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doStreamStateAgg, NULL, destroyStreamStateOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorStreamStateFn(pOperator, streamStateReleaseState, streamStateReloadState);
  initDownStream(downstream, &pInfo->streamAggSup, pOperator->operatorType, pInfo->primaryTsIndex, &pInfo->twAggSup);
  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  return pOperator;

_error:
  destroyStreamStateOperatorInfo(pInfo);
  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

#ifdef BUILD_NO_CALL
static void setInverFunction(SqlFunctionCtx* pCtx, int32_t num, EStreamType type) {
  for (int i = 0; i < num; i++) {
    if (type == STREAM_INVERT) {
      fmSetInvertFunc(pCtx[i].functionId, &(pCtx[i].fpSet));
    } else if (type == STREAM_NORMAL) {
      fmSetNormalFunc(pCtx[i].functionId, &(pCtx[i].fpSet));
    }
  }
}
#endif

static SSDataBlock* doStreamIntervalAgg(SOperatorInfo* pOperator) {
  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*                 pAPI = &pOperator->pTaskInfo->storageAPI;
  SExprSupp*                   pSup = &pOperator->exprSupp;

  qDebug("stask:%s  %s status: %d", GET_TASKID(pTaskInfo), getStreamOpName(pOperator->operatorType), pOperator->status);

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  if (pOperator->status == OP_RES_TO_RETURN) {
    SSDataBlock* resBlock = buildIntervalResult(pOperator);
    if (resBlock != NULL) {
      return resBlock;
    }

    if (pInfo->recvGetAll) {
      pInfo->recvGetAll = false;
      resetUnCloseWinInfo(pInfo->aggSup.pResultRowHashTable);
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
    pInfo->pUpdated = taosArrayInit(4096, POINTER_BYTES);
  }

  if (!pInfo->pUpdatedMap) {
    _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
    pInfo->pUpdatedMap = tSimpleHashInit(4096, hashFn);
  }

  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      qDebug("===stream===return data:%s. recv datablock num:%" PRIu64, getStreamOpName(pOperator->operatorType),
             pInfo->numOfDatapack);
      pInfo->numOfDatapack = 0;
      break;
    }

    pInfo->numOfDatapack++;
    printSpecDataBlock(pBlock, getStreamOpName(pOperator->operatorType), "recv", GET_TASKID(pTaskInfo));

    if (pBlock->info.type == STREAM_DELETE_DATA || pBlock->info.type == STREAM_DELETE_RESULT ||
        pBlock->info.type == STREAM_CLEAR) {
      doDeleteWindows(pOperator, &pInfo->interval, pBlock, pInfo->pDelWins, pInfo->pUpdatedMap);
      continue;
    } else if (pBlock->info.type == STREAM_GET_ALL) {
      pInfo->recvGetAll = true;
      getAllIntervalWindow(pInfo->aggSup.pResultRowHashTable, pInfo->pUpdatedMap);
      continue;
    } else if (pBlock->info.type == STREAM_CREATE_CHILD_TABLE) {
      printDataBlock(pBlock, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
      return pBlock;
    } else if (pBlock->info.type == STREAM_CHECKPOINT) {
      pAPI->stateStore.streamStateCommit(pInfo->pState);
      doStreamIntervalSaveCheckpoint(pOperator);
      pInfo->reCkBlock = true;
      copyDataBlock(pInfo->pCheckpointRes, pBlock);
      continue;
    } else {
      ASSERTS(pBlock->info.type == STREAM_NORMAL || pBlock->info.type == STREAM_INVALID, "invalid SSDataBlock type");
    }

    if (pBlock->info.type == STREAM_NORMAL && pBlock->info.version != 0) {
      // set input version
      pTaskInfo->version = pBlock->info.version;
    }

    if (pInfo->scalarSupp.pExprInfo != NULL) {
      SExprSupp* pExprSup = &pInfo->scalarSupp;
      projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
    }

    // The timewindow that overlaps the timestamps of the input pBlock need to be recalculated and return to the
    // caller. Note that all the time window are not close till now.
    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pSup, pBlock, TSDB_ORDER_ASC, MAIN_SCAN, true);
#ifdef BUILD_NO_CALL
    if (pInfo->invertible) {
      setInverFunction(pSup->pCtx, pOperator->exprSupp.numOfExprs, pBlock->info.type);
    }
#endif

    doStreamIntervalAggImpl(pOperator, pBlock, pBlock->info.id.groupId, pInfo->pUpdatedMap);
    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.window.ekey);
    pInfo->twAggSup.minTs = TMIN(pInfo->twAggSup.minTs, pBlock->info.window.skey);
  }
  pOperator->status = OP_RES_TO_RETURN;
  removeDeleteResults(pInfo->pUpdatedMap, pInfo->pDelWins);
  closeStreamIntervalWindow(pInfo->aggSup.pResultRowHashTable, &pInfo->twAggSup, &pInfo->interval, NULL,
                            pInfo->pUpdatedMap, pInfo->pDelWins, pOperator);

  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pInfo->pUpdatedMap, pIte, &iter)) != NULL) {
    taosArrayPush(pInfo->pUpdated, pIte);
  }
  taosArraySort(pInfo->pUpdated, winPosCmprImpl);

  initMultiResInfoFromArrayList(&pInfo->groupResInfo, pInfo->pUpdated);
  pInfo->pUpdated = NULL;
  blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);
  tSimpleHashCleanup(pInfo->pUpdatedMap);
  pInfo->pUpdatedMap = NULL;

  return buildIntervalResult(pOperator);
}

SOperatorInfo* createStreamIntervalOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode,
                                                SExecTaskInfo* pTaskInfo, SReadHandle* pHandle) {
  SStreamIntervalOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamIntervalOperatorInfo));
  SOperatorInfo*               pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }
  SStreamIntervalPhysiNode* pIntervalPhyNode = (SStreamIntervalPhysiNode*)pPhyNode;

  int32_t    code = TSDB_CODE_SUCCESS;
  int32_t    numOfCols = 0;
  SExprInfo* pExprInfo = createExprInfo(pIntervalPhyNode->window.pFuncs, NULL, &numOfCols);

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pPhyNode->pOutputDataBlockDesc);
  pInfo->interval = (SInterval){
      .interval = pIntervalPhyNode->interval,
      .sliding = pIntervalPhyNode->sliding,
      .intervalUnit = pIntervalPhyNode->intervalUnit,
      .slidingUnit = pIntervalPhyNode->slidingUnit,
      .offset = pIntervalPhyNode->offset,
      .precision = ((SColumnNode*)pIntervalPhyNode->window.pTspk)->node.resType.precision,
  };

  pInfo->twAggSup = (STimeWindowAggSupp){.waterMark = pIntervalPhyNode->window.watermark,
                                         .calTrigger = pIntervalPhyNode->window.triggerType,
                                         .maxTs = INT64_MIN,
                                         .minTs = INT64_MAX,
                                         .deleteMark = getDeleteMark(&pIntervalPhyNode->window, pIntervalPhyNode->interval)};

  ASSERTS(pInfo->twAggSup.calTrigger != STREAM_TRIGGER_MAX_DELAY, "trigger type should not be max delay");

  pOperator->pTaskInfo = pTaskInfo;
  SStorageAPI* pAPI = &pOperator->pTaskInfo->storageAPI;

  pInfo->ignoreExpiredData = pIntervalPhyNode->window.igExpired;
  pInfo->ignoreExpiredDataSaved = false;

  SExprSupp* pSup = &pOperator->exprSupp;
  initBasicInfo(&pInfo->binfo, pResBlock);
  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);

  pInfo->primaryTsIndex = ((SColumnNode*)pIntervalPhyNode->window.pTspk)->slotId;
  initResultSizeInfo(&pOperator->resultInfo, 4096);

  pInfo->pState = taosMemoryCalloc(1, sizeof(SStreamState));
  *(pInfo->pState) = *(pTaskInfo->streamInfo.pState);
  pAPI->stateStore.streamStateSetNumber(pInfo->pState, -1);

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  code = initAggSup(pSup, &pInfo->aggSup, pExprInfo, numOfCols, keyBufSize, pTaskInfo->id.str, pInfo->pState,
                    &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  if (pIntervalPhyNode->window.pExprs != NULL) {
    int32_t    numOfScalar = 0;
    SExprInfo* pScalarExprInfo = createExprInfo(pIntervalPhyNode->window.pExprs, NULL, &numOfScalar);
    code = initExprSupp(&pInfo->scalarSupp, pScalarExprInfo, numOfScalar, &pTaskInfo->storageAPI.functionStore);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  pInfo->invertible = false;
  pInfo->pDelWins = taosArrayInit(4, sizeof(SWinKey));
  pInfo->delIndex = 0;
  pInfo->pDelRes = createSpecialDataBlock(STREAM_DELETE_RESULT);
  initResultRowInfo(&pInfo->binfo.resultRowInfo);

  pInfo->pPhyNode = NULL;  // create new child
  pInfo->pPullDataMap = NULL;
  pInfo->pFinalPullDataMap = NULL;
  pInfo->pPullWins = NULL;  // SPullWindowInfo
  pInfo->pullIndex = 0;
  pInfo->pPullDataRes = NULL;
  pInfo->numOfChild = 0;
  pInfo->delKey.ts = INT64_MAX;
  pInfo->delKey.groupId = 0;
  pInfo->numOfDatapack = 0;
  pInfo->pUpdated = NULL;
  pInfo->pUpdatedMap = NULL;
  int32_t funResSize = getMaxFunResSize(pSup, numOfCols);

  pInfo->pState->pFileState = pTaskInfo->storageAPI.stateStore.streamFileStateInit(
      tsStreamBufferSize, sizeof(SWinKey), pInfo->aggSup.resultRowSize, funResSize, compareTs, pInfo->pState,
      pInfo->twAggSup.deleteMark, GET_TASKID(pTaskInfo), pHandle->checkpointId, STREAM_STATE_BUFF_HASH);

  setOperatorInfo(pOperator, "StreamIntervalOperator", QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL, true, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  pOperator->fpSet =
      createOperatorFpSet(optrDummyOpenFn, doStreamIntervalAgg, NULL, destroyStreamFinalIntervalOperatorInfo,
                          optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorStreamStateFn(pOperator, streamIntervalReleaseState, streamIntervalReloadState);

  pInfo->stateStore = pTaskInfo->storageAPI.stateStore;
  pInfo->recvGetAll = false;
  pInfo->pCheckpointRes = createSpecialDataBlock(STREAM_CHECKPOINT);

  // for stream
  void*   buff = NULL;
  int32_t len = 0;
  int32_t res = pAPI->stateStore.streamStateGetInfo(pInfo->pState, STREAM_INTERVAL_OP_CHECKPOINT_NAME,
                                                    strlen(STREAM_INTERVAL_OP_CHECKPOINT_NAME), &buff, &len);
  if (res == TSDB_CODE_SUCCESS) {
    doStreamIntervalDecodeOpState(buff, len, pOperator);
    taosMemoryFree(buff);
  }

  initIntervalDownStream(downstream, pPhyNode->type, pInfo);
  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  destroyStreamFinalIntervalOperatorInfo(pInfo);
  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

static void doStreamMidIntervalAggImpl(SOperatorInfo* pOperator, SSDataBlock* pSDataBlock, SSHashObj* pUpdatedMap) {
  SStreamIntervalOperatorInfo* pInfo = (SStreamIntervalOperatorInfo*)pOperator->info;
  pInfo->dataVersion = TMAX(pInfo->dataVersion, pSDataBlock->info.version);

  SResultRowInfo*  pResultRowInfo = &(pInfo->binfo.resultRowInfo);
  SExecTaskInfo*   pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*       pSup = &pOperator->exprSupp;
  int32_t          numOfOutput = pSup->numOfExprs;
  int32_t          step = 1;
  SRowBuffPos*     pResPos = NULL;
  SResultRow*      pResult = NULL;
  int32_t          forwardRows = 1;
  uint64_t         groupId = pSDataBlock->info.id.groupId;
  SColumnInfoData* pColDataInfo = taosArrayGet(pSDataBlock->pDataBlock, pInfo->primaryTsIndex);
  TSKEY*           tsCol = (int64_t*)pColDataInfo->pData;

  int32_t     startPos = 0;
  TSKEY       ts = getStartTsKey(&pSDataBlock->info.window, tsCol);
  STimeWindow nextWin = getFinalTimeWindow(ts, &pInfo->interval);

  while (1) {
    SWinKey key = {
        .ts = nextWin.skey,
        .groupId = groupId,
    };
    void* chIds = taosHashGet(pInfo->pPullDataMap, &key, sizeof(SWinKey));
    int32_t index = -1;
    SArray* chArray = NULL;
    int32_t chId = 0;
    if (chIds) {
      chArray = *(void**)chIds;
      chId = getChildIndex(pSDataBlock);
      index = taosArraySearchIdx(chArray, &chId, compareInt32Val, TD_EQ);
    }
    if (!(index == -1 || pSDataBlock->info.type == STREAM_PULL_DATA)) {
      startPos = getNextQualifiedFinalWindow(&pInfo->interval, &nextWin, &pSDataBlock->info, tsCol, startPos);
      if (startPos < 0) {
        break;
      }
      continue;
    }

    if (!inSlidingWindow(&pInfo->interval, &nextWin, &pSDataBlock->info)) {
      startPos = getNexWindowPos(&pInfo->interval, &pSDataBlock->info, tsCol, startPos, nextWin.ekey, &nextWin);
      if (startPos < 0) {
        break;
      }
      continue;
    }

    int32_t code = setIntervalOutputBuf(pInfo->pState, &nextWin, &pResPos, groupId, pSup->pCtx, numOfOutput,
                                        pSup->rowEntryInfoOffset, &pInfo->aggSup, &pInfo->stateStore);
    pResult = (SResultRow*)pResPos->pRowBuff;
    if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_OUT_OF_MEMORY);
    }

    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE) {
      saveWinResult(&key, pResPos, pUpdatedMap);
    }

    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_WINDOW_CLOSE) {
      tSimpleHashPut(pInfo->aggSup.pResultRowHashTable, &key, sizeof(SWinKey), &pResPos, POINTER_BYTES);
    }

    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &nextWin, 1);
    applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, startPos, forwardRows,
                                    pSDataBlock->info.rows, numOfOutput);
    key.ts = nextWin.skey;

    if (pInfo->delKey.ts > key.ts) {
      pInfo->delKey = key;
    }
    int32_t prevEndPos = (forwardRows - 1) * step + startPos;
    if (pSDataBlock->info.window.skey <= 0 || pSDataBlock->info.window.ekey <= 0) {
      qError("table uid %" PRIu64 " data block timestamp range may not be calculated! minKey %" PRId64
             ",maxKey %" PRId64,
             pSDataBlock->info.id.uid, pSDataBlock->info.window.skey, pSDataBlock->info.window.ekey);
      blockDataUpdateTsWindow(pSDataBlock, 0);

      // timestamp of the data is incorrect
      if (pSDataBlock->info.window.skey <= 0 || pSDataBlock->info.window.ekey <= 0) {
        qError("table uid %" PRIu64 " data block timestamp is out of range! minKey %" PRId64 ",maxKey %" PRId64,
               pSDataBlock->info.id.uid, pSDataBlock->info.window.skey, pSDataBlock->info.window.ekey);
      }
    }
    startPos = getNextQualifiedFinalWindow(&pInfo->interval, &nextWin, &pSDataBlock->info, tsCol, prevEndPos);
    if (startPos < 0) {
      break;
    }
  }
}

static void addMidRetriveWindow(SArray* wins, SHashObj* pMidPullMap, int32_t numOfChild) {
  int32_t size = taosArrayGetSize(wins);
  for (int32_t i = 0; i < size; i++) {
    SWinKey*    winKey = taosArrayGet(wins, i);
    void* chIds = taosHashGet(pMidPullMap, winKey, sizeof(SWinKey));
    if (!chIds) {
      addPullWindow(pMidPullMap, winKey, numOfChild);
      qDebug("===stream===prepare mid operator retrive for delete %" PRId64 ", size:%d", winKey->ts, numOfChild);
    }
  }
}

static SSDataBlock* doStreamMidIntervalAgg(SOperatorInfo* pOperator) {
  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*                 pAPI = &pOperator->pTaskInfo->storageAPI;
  SOperatorInfo*               downstream = pOperator->pDownstream[0];
  SExprSupp*                   pSup = &pOperator->exprSupp;

  qDebug("stask:%s  %s status: %d", GET_TASKID(pTaskInfo), getStreamOpName(pOperator->operatorType), pOperator->status);

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  } else if (pOperator->status == OP_RES_TO_RETURN) {
    SSDataBlock* resBlock = buildIntervalResult(pOperator);
    if (resBlock != NULL) {
      return resBlock;
    }

    setOperatorCompleted(pOperator);
    clearFunctionContext(&pOperator->exprSupp);
    clearStreamIntervalOperator(pInfo);
    qDebug("stask:%s  ===stream===%s clear", GET_TASKID(pTaskInfo), getStreamOpName(pOperator->operatorType));
    return NULL;
  } else {
    SSDataBlock* resBlock = buildIntervalResult(pOperator);
    if (resBlock != NULL) {
      return resBlock;
    }

    if (pInfo->recvRetrive) {
      pInfo->recvRetrive = false;
      printDataBlock(pInfo->pMidRetriveRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
      return pInfo->pMidRetriveRes;
    }

    if (pInfo->clearState) {
      pInfo->clearState = false;
      clearFunctionContext(&pOperator->exprSupp);
      clearStreamIntervalOperator(pInfo);
    }
  }

  if (!pInfo->pUpdated) {
    pInfo->pUpdated = taosArrayInit(4096, POINTER_BYTES);
  }
  if (!pInfo->pUpdatedMap) {
    _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
    pInfo->pUpdatedMap = tSimpleHashInit(4096, hashFn);
  }

  while (1) {
    if (isTaskKilled(pTaskInfo)) {
      if (pInfo->pUpdated != NULL) {
        pInfo->pUpdated = taosArrayDestroy(pInfo->pUpdated);
      }

      if (pInfo->pUpdatedMap != NULL) {
        tSimpleHashCleanup(pInfo->pUpdatedMap);
        pInfo->pUpdatedMap = NULL;
      }

      T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
    }

    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      pOperator->status = OP_RES_TO_RETURN;
      qDebug("===stream===return data:%s. recv datablock num:%" PRIu64, getStreamOpName(pOperator->operatorType),
             pInfo->numOfDatapack);
      pInfo->numOfDatapack = 0;
      break;
    }
    pInfo->numOfDatapack++;
    printSpecDataBlock(pBlock, getStreamOpName(pOperator->operatorType), "recv", GET_TASKID(pTaskInfo));

    if (pBlock->info.type == STREAM_NORMAL || pBlock->info.type == STREAM_PULL_DATA) {
      pInfo->binfo.pRes->info.type = pBlock->info.type;
    } else if (pBlock->info.type == STREAM_DELETE_DATA || pBlock->info.type == STREAM_DELETE_RESULT ||
               pBlock->info.type == STREAM_CLEAR) {
      SArray* delWins = taosArrayInit(8, sizeof(SWinKey));
      doDeleteWindows(pOperator, &pInfo->interval, pBlock, delWins, pInfo->pUpdatedMap);
      removeResults(delWins, pInfo->pUpdatedMap);
      taosArrayAddAll(pInfo->pDelWins, delWins);
      taosArrayDestroy(delWins);

      doBuildDeleteResult(pInfo, pInfo->pDelWins, &pInfo->delIndex, pInfo->pDelRes);
      if (pInfo->pDelRes->info.rows != 0) {
        // process the rest of the data
        printDataBlock(pInfo->pDelRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
        if (pBlock->info.type == STREAM_CLEAR) {
          pInfo->pDelRes->info.type = STREAM_CLEAR;
        } else {
          pInfo->pDelRes->info.type = STREAM_DELETE_RESULT;
        }
        ASSERT(taosArrayGetSize(pInfo->pUpdated) == 0);
        return pInfo->pDelRes;
      }
      continue;
    } else if (pBlock->info.type == STREAM_CREATE_CHILD_TABLE) {
      return pBlock;
    } else if (pBlock->info.type == STREAM_PULL_OVER) {
      pInfo->recvPullover = processPullOver(pBlock, pInfo->pPullDataMap, pInfo->pFinalPullDataMap, &pInfo->interval,
                                            pInfo->pPullWins, pInfo->numOfChild, pOperator);
      if (pInfo->recvPullover) {
        copyDataBlock(pInfo->pMidPulloverRes, pBlock);
        pInfo->clearState = true;
        break;
      }
      continue;
    } else if (pBlock->info.type == STREAM_CHECKPOINT) {
      pAPI->stateStore.streamStateCommit(pInfo->pState);
      doStreamIntervalSaveCheckpoint(pOperator);
      copyDataBlock(pInfo->pCheckpointRes, pBlock);
      continue;
    } else if (pBlock->info.type == STREAM_MID_RETRIEVE) {
      SArray* delWins = taosArrayInit(8, sizeof(SWinKey));
      doDeleteWindows(pOperator, &pInfo->interval, pBlock, delWins, pInfo->pUpdatedMap);
      addMidRetriveWindow(delWins, pInfo->pPullDataMap, pInfo->numOfChild);
      taosArrayDestroy(delWins);
      pInfo->recvRetrive = true;
      copyDataBlock(pInfo->pMidRetriveRes, pBlock);
      pInfo->pMidRetriveRes->info.type = STREAM_MID_RETRIEVE;
      pInfo->clearState = true;
      break;
    } else {
      ASSERTS(pBlock->info.type == STREAM_INVALID, "invalid SSDataBlock type");
    }

    if (pInfo->scalarSupp.pExprInfo != NULL) {
      SExprSupp* pExprSup = &pInfo->scalarSupp;
      projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
    }
    setInputDataBlock(pSup, pBlock, TSDB_ORDER_ASC, MAIN_SCAN, true);
    doStreamMidIntervalAggImpl(pOperator, pBlock, pInfo->pUpdatedMap);
    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.window.ekey);
    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.watermark);
    pInfo->twAggSup.minTs = TMIN(pInfo->twAggSup.minTs, pBlock->info.window.skey);
  }

  removeDeleteResults(pInfo->pUpdatedMap, pInfo->pDelWins);
  pInfo->binfo.pRes->info.watermark = pInfo->twAggSup.maxTs;

  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pInfo->pUpdatedMap, pIte, &iter)) != NULL) {
    taosArrayPush(pInfo->pUpdated, pIte);
  }

  tSimpleHashCleanup(pInfo->pUpdatedMap);
  pInfo->pUpdatedMap = NULL;
  taosArraySort(pInfo->pUpdated, winPosCmprImpl);

  initMultiResInfoFromArrayList(&pInfo->groupResInfo, pInfo->pUpdated);
  pInfo->pUpdated = NULL;
  blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);

  SSDataBlock* resBlock = buildIntervalResult(pOperator);
  if (resBlock != NULL) {
    return resBlock;
  }

  if (pInfo->recvRetrive) {
    pInfo->recvRetrive = false;
    printDataBlock(pInfo->pMidRetriveRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
    return pInfo->pMidRetriveRes;
  }

  if (pInfo->clearState) {
    pInfo->clearState = false;
    clearFunctionContext(&pOperator->exprSupp);
    clearStreamIntervalOperator(pInfo);
  }
  return NULL;
}

void setStreamOperatorCompleted(SOperatorInfo* pOperator) {
  setOperatorCompleted(pOperator);
  qDebug("stask:%s  %s status: %d. set completed", GET_TASKID(pOperator->pTaskInfo), getStreamOpName(pOperator->operatorType), pOperator->status);
}
