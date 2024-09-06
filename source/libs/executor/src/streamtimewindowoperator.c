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

#define IS_FINAL_INTERVAL_OP(op) ((op)->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL)
#define IS_MID_INTERVAL_OP(op)   ((op)->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_MID_INTERVAL)
#define IS_NORMAL_INTERVAL_OP(op)                                    \
  ((op)->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL || \
   (op)->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL)

#define IS_FINAL_SESSION_OP(op) ((op)->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION)
#define IS_NORMAL_SESSION_OP(op)                                    \
  ((op)->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION || \
   (op)->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION)

#define IS_NORMAL_STATE_OP(op) ((op)->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_STATE)

#define DEAULT_DELETE_MARK                 INT64_MAX
#define STREAM_INTERVAL_OP_STATE_NAME      "StreamIntervalHistoryState"
#define STREAM_SESSION_OP_STATE_NAME       "StreamSessionHistoryState"
#define STREAM_STATE_OP_STATE_NAME         "StreamStateHistoryState"
#define STREAM_INTERVAL_OP_CHECKPOINT_NAME "StreamIntervalOperator_Checkpoint"
#define STREAM_SESSION_OP_CHECKPOINT_NAME  "StreamSessionOperator_Checkpoint"
#define STREAM_STATE_OP_CHECKPOINT_NAME    "StreamStateOperator_Checkpoint"

#define MAX_STREAM_HISTORY_RESULT          100000000

typedef struct SStateWindowInfo {
  SResultWindowInfo winInfo;
  SStateKeys*       pStateKey;
} SStateWindowInfo;

typedef struct SPullWindowInfo {
  STimeWindow window;
  uint64_t    groupId;
  STimeWindow calWin;
} SPullWindowInfo;

static int32_t doStreamMidIntervalAggNext(SOperatorInfo* pOperator, SSDataBlock** ppRes);

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
  if (tSimpleHashGetSize(pStUpdated) > MAX_STREAM_HISTORY_RESULT) {
    qError("%s failed at line %d since too many history result. ", __func__, __LINE__);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  winInfo.sessionWin.win.ekey = winInfo.sessionWin.win.skey;
  return tSimpleHashPut(pStUpdated, &winInfo.sessionWin, sizeof(SSessionKey), &winInfo, sizeof(SResultWindowInfo));
}

static int32_t saveWinResult(SWinKey* pKey, SRowBuffPos* pPos, SSHashObj* pUpdatedMap) {
  if (tSimpleHashGetSize(pUpdatedMap) > MAX_STREAM_HISTORY_RESULT) {
    qError("%s failed at line %d since too many history result. ", __func__, __LINE__);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  return tSimpleHashPut(pUpdatedMap, pKey, sizeof(SWinKey), &pPos, POINTER_BYTES);
}

static int32_t saveWinResultInfo(TSKEY ts, uint64_t groupId, SRowBuffPos* pPos, SSHashObj* pUpdatedMap) {
  SWinKey key = {.ts = ts, .groupId = groupId};
  return saveWinResult(&key, pPos, pUpdatedMap);
}

static void removeResults(SArray* pWins, SSHashObj* pUpdatedMap) {
  int32_t size = taosArrayGetSize(pWins);
  for (int32_t i = 0; i < size; i++) {
    SWinKey* pW = taosArrayGet(pWins, i);
    void*    tmp = tSimpleHashGet(pUpdatedMap, pW, sizeof(SWinKey));
    if (tmp) {
      void* value = *(void**)tmp;
      taosMemoryFree(value);
      int32_t tmpRes = tSimpleHashRemove(pUpdatedMap, pW, sizeof(SWinKey));
      qTrace("%s at line %d res:%d", __func__, __LINE__, tmpRes);
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
  return pTwSup->maxTs != INT64_MIN && ekey < pTwSup->maxTs - pTwSup->waterMark;
}

bool isCloseWindow(STimeWindow* pWin, STimeWindowAggSupp* pTwSup) { return isOverdue(pWin->ekey, pTwSup); }

static void doDeleteWindow(SOperatorInfo* pOperator, TSKEY ts, uint64_t groupId) {
  SStorageAPI* pAPI = &pOperator->pTaskInfo->storageAPI;

  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  SWinKey                      key = {.ts = ts, .groupId = groupId};
  int32_t                      tmpRes = tSimpleHashRemove(pInfo->aggSup.pResultRowHashTable, &key, sizeof(SWinKey));
  qTrace("%s at line %d res:%d", __func__, __LINE__, tmpRes);
  pAPI->stateStore.streamStateDel(pInfo->pState, &key);
}

static int32_t getChildIndex(SSDataBlock* pBlock) { return pBlock->info.childId; }

static int32_t doDeleteWindows(SOperatorInfo* pOperator, SInterval* pInterval, SSDataBlock* pBlock, SArray* pUpWins,
                               SSHashObj* pUpdatedMap, SHashObj* pInvalidWins) {
  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      lino = 0;
  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
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
        if (pInvalidWins) {
          qDebug("===stream===save invalid delete window:%" PRId64 ",groupId:%" PRId64 ",chId:%d", winRes.ts,
                 winRes.groupId, childId);
          code = taosHashPut(pInvalidWins, &winRes, sizeof(SWinKey), NULL, 0);
          QUERY_CHECK_CODE(code, lino, _end);
        }

        SArray* chArray = *(void**)chIds;
        int32_t index = taosArraySearchIdx(chArray, &childId, compareInt32Val, TD_EQ);
        if (index != -1) {
          qDebug("===stream===try push delete window:%" PRId64 ",groupId:%" PRId64 ",chId:%d ,continue", win.skey,
                 winGpId, childId);
          getNextTimeWindow(pInterval, &win, TSDB_ORDER_ASC);
          continue;
        }
      }
      doDeleteWindow(pOperator, win.skey, winGpId);
      if (pUpWins) {
        void* tmp = taosArrayPush(pUpWins, &winRes);
        if (!tmp) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
      if (pUpdatedMap) {
        int32_t tmpRes = tSimpleHashRemove(pUpdatedMap, &winRes, sizeof(SWinKey));
        qTrace("%s at line %d res:%d", __func__, __LINE__, tmpRes);
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
  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      lino = 0;
  void*                        pIte = NULL;
  int32_t                      iter = 0;
  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
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
        code = saveWinResult(pWinKey, *(SRowBuffPos**)pIte, closeWins);
        QUERY_CHECK_CODE(code, lino, _end);
      }
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

STimeWindow getFinalTimeWindow(int64_t ts, SInterval* pInterval) {
  STimeWindow w = {.skey = ts, .ekey = INT64_MAX};
  w.ekey = taosTimeAdd(w.skey, pInterval->interval, pInterval->intervalUnit, pInterval->precision) - 1;
  return w;
}

static void doBuildDeleteResult(SStreamIntervalOperatorInfo* pInfo, SArray* pWins, int32_t* index,
                                SSDataBlock* pBlock) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  blockDataCleanup(pBlock);
  int32_t size = taosArrayGetSize(pWins);
  if (*index == size) {
    *index = 0;
    taosArrayClear(pWins);
    goto _end;
  }
  code = blockDataEnsureCapacity(pBlock, size - *index);
  QUERY_CHECK_CODE(code, lino, _end);

  uint64_t uid = 0;
  for (int32_t i = *index; i < size; i++) {
    SWinKey* pWin = taosArrayGet(pWins, i);
    void*    tbname = NULL;
    int32_t  winCode = TSDB_CODE_SUCCESS;
    code = pInfo->stateStore.streamStateGetParName(pInfo->pState, pWin->groupId, &tbname, false, &winCode);
    QUERY_CHECK_CODE(code, lino, _end);

    if (winCode != TSDB_CODE_SUCCESS) {
      code = appendDataToSpecialBlock(pBlock, &pWin->ts, &pWin->ts, &uid, &pWin->groupId, NULL);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      QUERY_CHECK_CONDITION((tbname), code, lino, _end, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
      char parTbName[VARSTR_HEADER_SIZE + TSDB_TABLE_NAME_LEN];
      STR_WITH_MAXSIZE_TO_VARSTR(parTbName, tbname, sizeof(parTbName));
      code = appendDataToSpecialBlock(pBlock, &pWin->ts, &pWin->ts, &uid, &pWin->groupId, parTbName);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    pInfo->stateStore.streamStateFreeVal(tbname);
    (*index)++;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
}

void destroyFlusedPos(void* pRes) {
  SRowBuffPos* pPos = (SRowBuffPos*)pRes;
  if (!pPos->needFree && !pPos->pRowBuff) {
    taosMemoryFreeClear(pPos->pKey);
    taosMemoryFree(pPos);
  }
}

void destroyFlusedppPos(void* ppRes) {
  void *pRes = *(void **)ppRes;
  destroyFlusedPos(pRes);
}

void clearGroupResInfo(SGroupResInfo* pGroupResInfo) {
  if (pGroupResInfo->freeItem) {
    int32_t size = taosArrayGetSize(pGroupResInfo->pRows);
    for (int32_t i = pGroupResInfo->index; i < size; i++) {
      void* pPos = taosArrayGetP(pGroupResInfo->pRows, i);
      destroyFlusedPos(pPos);
    }
    pGroupResInfo->freeItem = false;
  }
  taosArrayDestroy(pGroupResInfo->pRows);
  pGroupResInfo->pRows = NULL;
  pGroupResInfo->index = 0;
}

void destroyStreamFinalIntervalOperatorInfo(void* param) {
  if (param == NULL) {
    return;
  }
  SStreamIntervalOperatorInfo* pInfo = (SStreamIntervalOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);
  cleanupAggSup(&pInfo->aggSup);
  clearGroupResInfo(&pInfo->groupResInfo);
  taosArrayDestroyP(pInfo->pUpdated, destroyFlusedPos);
  pInfo->pUpdated = NULL;

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
  if (pInfo->pUpdatedMap != NULL) {
    tSimpleHashSetFreeFp(pInfo->pUpdatedMap, destroyFlusedppPos);
    tSimpleHashCleanup(pInfo->pUpdatedMap);
    pInfo->pUpdatedMap = NULL;
  }

  if (pInfo->stateStore.streamFileStateDestroy != NULL) {
    pInfo->stateStore.streamFileStateDestroy(pInfo->pState->pFileState);
  }
  taosArrayDestroy(pInfo->pMidPullDatas);

  if (pInfo->pState !=NULL && pInfo->pState->dump == 1) {
    taosMemoryFreeClear(pInfo->pState->pTdbState->pOwner);
    taosMemoryFreeClear(pInfo->pState->pTdbState);
  }
  taosMemoryFreeClear(pInfo->pState);

  nodesDestroyNode((SNode*)pInfo->pPhyNode);
  colDataDestroy(&pInfo->twAggSup.timeWindowData);
  cleanupExprSupp(&pInfo->scalarSupp);
  tSimpleHashCleanup(pInfo->pDeletedMap);

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

int32_t initIntervalDownStream(SOperatorInfo* downstream, uint16_t type, SStreamIntervalOperatorInfo* pInfo) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SStateStore*   pAPI = &downstream->pTaskInfo->storageAPI.stateStore;
  SExecTaskInfo* pTaskInfo = downstream->pTaskInfo;

  if (downstream->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    return initIntervalDownStream(downstream->pDownstream[0], type, pInfo);
  }

  SStreamScanInfo* pScanInfo = downstream->info;
  pScanInfo->windowSup.parentType = type;
  pScanInfo->windowSup.pIntervalAggSup = &pInfo->aggSup;
  if (!pScanInfo->pUpdateInfo) {
    code = pAPI->updateInfoInitP(&pInfo->interval, pInfo->twAggSup.waterMark, pScanInfo->igCheckUpdate,
                                 pScanInfo->pkColType, pScanInfo->pkColLen, &pScanInfo->pUpdateInfo);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  pScanInfo->interval = pInfo->interval;
  pScanInfo->twAggSup = pInfo->twAggSup;
  pScanInfo->pState = pInfo->pState;
  pInfo->pUpdateInfo = pScanInfo->pUpdateInfo;
  pInfo->basic.primaryPkIndex = pScanInfo->primaryKeyIndex;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

int32_t compactFunctions(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx, int32_t numOfOutput,
                         SExecTaskInfo* pTaskInfo, SColumnInfoData* pTimeWindowData) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
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
      code = pDestCtx[k].sfp.process(&tw, 1, &out);
      QUERY_CHECK_CODE(code, lino, _end);

      pEntryInfo->numOfRes = 1;
    } else if (functionNeedToExecute(&pDestCtx[k]) && pDestCtx[k].fpSet.combine != NULL) {
      code = pDestCtx[k].fpSet.combine(&pDestCtx[k], &pSourceCtx[k]);
      QUERY_CHECK_CODE(code, lino, _end);
    } else if (pDestCtx[k].fpSet.combine == NULL) {
      char* funName = fmGetFuncName(pDestCtx[k].functionId);
      qError("%s error, combine funcion for %s is not implemented", GET_TASKID(pTaskInfo), funName);
      taosMemoryFreeClear(funName);
      code = TSDB_CODE_FAILED;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

bool hasIntervalWindow(void* pState, SWinKey* pKey, SStateStore* pStore) {
  return pStore->streamStateCheck(pState, pKey);
}

int32_t setIntervalOutputBuf(void* pState, STimeWindow* win, SRowBuffPos** pResult, int64_t groupId,
                             SqlFunctionCtx* pCtx, int32_t numOfOutput, int32_t* rowEntryInfoOffset,
                             SAggSupporter* pAggSup, SStateStore* pStore, int32_t* pWinCode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SWinKey key = {.ts = win->skey, .groupId = groupId};
  char*   value = NULL;
  int32_t size = pAggSup->resultRowSize;

  code = pStore->streamStateAddIfNotExist(pState, &key, (void**)&value, &size, pWinCode);
  QUERY_CHECK_CODE(code, lino, _end);

  *pResult = (SRowBuffPos*)value;
  SResultRow* res = (SResultRow*)((*pResult)->pRowBuff);

  // set time window for current result
  res->win = (*win);
  code = setResultRowInitCtx(res, pCtx, numOfOutput, rowEntryInfoOffset);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
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

int32_t addPullWindow(SHashObj* pMap, SWinKey* pWinRes, int32_t size) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SArray* childIds = taosArrayInit(8, sizeof(int32_t));
  QUERY_CHECK_NULL(childIds, code, lino, _end, terrno);
  for (int32_t i = 0; i < size; i++) {
    void* tmp = taosArrayPush(childIds, &i);
    if (!tmp) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }
  code = taosHashPut(pMap, pWinRes, sizeof(SWinKey), &childIds, sizeof(void*));
  QUERY_CHECK_CODE(code, lino, _end);
_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
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
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  clearSpecialDataBlock(pBlock);
  int32_t size = taosArrayGetSize(array);
  if (size - (*pIndex) == 0) {
    goto _end;
  }
  code = blockDataEnsureCapacity(pBlock, size - (*pIndex));
  QUERY_CHECK_CODE(code, lino, _end);

  SColumnInfoData* pStartTs = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  SColumnInfoData* pEndTs = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
  SColumnInfoData* pGroupId = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  SColumnInfoData* pCalStartTs = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX);
  SColumnInfoData* pCalEndTs = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX);
  SColumnInfoData* pTbName = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, TABLE_NAME_COLUMN_INDEX);
  SColumnInfoData* pPrimaryKey = NULL;
  if (taosArrayGetSize(pBlock->pDataBlock) > PRIMARY_KEY_COLUMN_INDEX) {
    pPrimaryKey = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, PRIMARY_KEY_COLUMN_INDEX);
  }
  for (; (*pIndex) < size; (*pIndex)++) {
    SPullWindowInfo* pWin = taosArrayGet(array, (*pIndex));
    code = colDataSetVal(pStartTs, pBlock->info.rows, (const char*)&pWin->window.skey, false);
    QUERY_CHECK_CODE(code, lino, _end);

    code = colDataSetVal(pEndTs, pBlock->info.rows, (const char*)&pWin->window.ekey, false);
    QUERY_CHECK_CODE(code, lino, _end);

    code = colDataSetVal(pGroupId, pBlock->info.rows, (const char*)&pWin->groupId, false);
    QUERY_CHECK_CODE(code, lino, _end);

    code = colDataSetVal(pCalStartTs, pBlock->info.rows, (const char*)&pWin->calWin.skey, false);
    QUERY_CHECK_CODE(code, lino, _end);

    code = colDataSetVal(pCalEndTs, pBlock->info.rows, (const char*)&pWin->calWin.ekey, false);
    QUERY_CHECK_CODE(code, lino, _end);

    colDataSetNULL(pTbName, pBlock->info.rows);
    if (pPrimaryKey != NULL) {
      colDataSetNULL(pPrimaryKey, pBlock->info.rows);
    }

    pBlock->info.rows++;
  }
  if ((*pIndex) == size) {
    *pIndex = 0;
    taosArrayClear(array);
  }
  code = blockDataUpdateTsWindow(pBlock, 0);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
}

static int32_t processPullOver(SSDataBlock* pBlock, SHashObj* pMap, SHashObj* pFinalMap, SInterval* pInterval,
                               SArray* pPullWins, int32_t numOfCh, SOperatorInfo* pOperator, bool* pBeOver) {
  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      lino = 0;
  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  SColumnInfoData*             pStartCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX);
  TSKEY*                       tsData = (TSKEY*)pStartCol->pData;
  SColumnInfoData*             pEndCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX);
  TSKEY*                       tsEndData = (TSKEY*)pEndCol->pData;
  SColumnInfoData*             pGroupCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  uint64_t*                    groupIdData = (uint64_t*)pGroupCol->pData;
  int32_t                      chId = getChildIndex(pBlock);
  bool                         res = false;
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
            int32_t tmpRes = taosHashRemove(pMap, &winRes, sizeof(SWinKey));
            qTrace("%s at line %d res:%d", __func__, __LINE__, tmpRes);
            res = true;
            qDebug("===stream===retrive pull data over.window %" PRId64, winRes.ts);

            void* pFinalCh = taosHashGet(pFinalMap, &winRes, sizeof(SWinKey));
            if (pFinalCh) {
              int32_t tmpRes = taosHashRemove(pFinalMap, &winRes, sizeof(SWinKey));
              qTrace("%s at line %d res:%d", __func__, __LINE__, tmpRes);
              doDeleteWindow(pOperator, winRes.ts, winRes.groupId);
              STimeWindow     nextWin = getFinalTimeWindow(winRes.ts, pInterval);
              SPullWindowInfo pull = {.window = nextWin,
                                      .groupId = winRes.groupId,
                                      .calWin.skey = nextWin.skey,
                                      .calWin.ekey = nextWin.skey};
              // add pull data request
              qDebug("===stream===prepare final retrive for delete window:%" PRId64 ",groupId:%" PRId64 ", size:%d",
                     winRes.ts, winRes.groupId, numOfCh);
              if (IS_MID_INTERVAL_OP(pOperator)) {
                SStreamIntervalOperatorInfo* pInfo = (SStreamIntervalOperatorInfo*)pOperator->info;

                void* tmp = taosArrayPush(pInfo->pMidPullDatas, &winRes);
                if (!tmp) {
                  code = TSDB_CODE_OUT_OF_MEMORY;
                  QUERY_CHECK_CODE(code, lino, _end);
                }
              } else if (savePullWindow(&pull, pPullWins) == TSDB_CODE_SUCCESS) {
                void* tmp = taosArrayPush(pInfo->pDelWins, &winRes);
                if (!tmp) {
                  code = TSDB_CODE_OUT_OF_MEMORY;
                  QUERY_CHECK_CODE(code, lino, _end);
                }

                code = addPullWindow(pMap, &winRes, numOfCh);
                QUERY_CHECK_CODE(code, lino, _end);

                if (pInfo->destHasPrimaryKey) {
                  code = tSimpleHashPut(pInfo->pDeletedMap, &winRes, sizeof(SWinKey), NULL, 0);
                  QUERY_CHECK_CODE(code, lino, _end);
                }
                qDebug("===stream===prepare final retrive for delete %" PRId64 ", size:%d", winRes.ts, numOfCh);
              }
            }
          }
        }
      }
      winTs = taosTimeAdd(winTs, pInterval->sliding, pInterval->slidingUnit, pInterval->precision);
    }
  }
  if (pBeOver) {
    *pBeOver = res;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t addRetriveWindow(SArray* wins, SStreamIntervalOperatorInfo* pInfo, int32_t childId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t size = taosArrayGetSize(wins);
  for (int32_t i = 0; i < size; i++) {
    SWinKey*    winKey = taosArrayGet(wins, i);
    STimeWindow nextWin = getFinalTimeWindow(winKey->ts, &pInfo->interval);
    void*       chIds = taosHashGet(pInfo->pPullDataMap, winKey, sizeof(SWinKey));
    if (!chIds) {
      SPullWindowInfo pull = {
          .window = nextWin, .groupId = winKey->groupId, .calWin.skey = nextWin.skey, .calWin.ekey = nextWin.skey};
      // add pull data request
      if (savePullWindow(&pull, pInfo->pPullWins) == TSDB_CODE_SUCCESS) {
        code = addPullWindow(pInfo->pPullDataMap, winKey, pInfo->numOfChild);
        QUERY_CHECK_CODE(code, lino, _end);

        if (pInfo->destHasPrimaryKey) {
          code = tSimpleHashPut(pInfo->pDeletedMap, winKey, sizeof(SWinKey), NULL, 0);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        qDebug("===stream===prepare retrive for delete %" PRId64 ", size:%d", winKey->ts, pInfo->numOfChild);
      }
    } else {
      SArray* chArray = *(void**)chIds;
      int32_t index = taosArraySearchIdx(chArray, &childId, compareInt32Val, TD_EQ);
      qDebug("===stream===check final retrive %" PRId64 ",chid:%d", winKey->ts, index);
      if (index == -1) {
        qDebug("===stream===add final retrive %" PRId64, winKey->ts);
        code = taosHashPut(pInfo->pFinalPullDataMap, winKey, sizeof(SWinKey), NULL, 0);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void clearFunctionContext(SExprSupp* pSup) {
  for (int32_t i = 0; i < pSup->numOfExprs; i++) {
    pSup->pCtx[i].saveHandle.currentPage = -1;
  }
}

int32_t getOutputBuf(void* pState, SRowBuffPos* pPos, SResultRow** pResult, SStateStore* pStore) {
  return pStore->streamStateGetByPos(pState, pPos, (void**)pResult);
}

void buildDataBlockFromGroupRes(SOperatorInfo* pOperator, void* pState, SSDataBlock* pBlock, SExprSupp* pSup,
                                SGroupResInfo* pGroupResInfo) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*    pAPI = &pOperator->pTaskInfo->storageAPI;
  SExprInfo*      pExprInfo = pSup->pExprInfo;
  int32_t         numOfExprs = pSup->numOfExprs;
  int32_t*        rowEntryOffset = pSup->rowEntryInfoOffset;
  SqlFunctionCtx* pCtx = pSup->pCtx;

  int32_t numOfRows = getNumOfTotalRes(pGroupResInfo);

  for (int32_t i = pGroupResInfo->index; i < numOfRows; i += 1) {
    SRowBuffPos* pPos = *(SRowBuffPos**)taosArrayGet(pGroupResInfo->pRows, i);
    SResultRow*  pRow = NULL;
    code = getOutputBuf(pState, pPos, &pRow, &pAPI->stateStore);
    QUERY_CHECK_CODE(code, lino, _end);
    uint64_t groupId = ((SWinKey*)pPos->pKey)->groupId;
    doUpdateNumOfRows(pCtx, pRow, numOfExprs, rowEntryOffset);
    // no results, continue to check the next one
    if (pRow->numOfRows == 0) {
      pGroupResInfo->index += 1;
      continue;
    }
    if (pBlock->info.id.groupId == 0) {
      pBlock->info.id.groupId = groupId;
      void*   tbname = NULL;
      int32_t winCode = TSDB_CODE_SUCCESS;
      code = pAPI->stateStore.streamStateGetParName(pTaskInfo->streamInfo.pState, pBlock->info.id.groupId, &tbname,
                                                    false, &winCode);
      QUERY_CHECK_CODE(code, lino, _end);
      if (winCode != TSDB_CODE_SUCCESS) {
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
      break;
    }
    pGroupResInfo->index += 1;

    for (int32_t j = 0; j < numOfExprs; ++j) {
      int32_t slotId = pExprInfo[j].base.resSchema.slotId;

      pCtx[j].resultInfo = getResultEntryInfo(pRow, j, rowEntryOffset);

      if (pCtx[j].fpSet.finalize) {
        int32_t tmpRes = pCtx[j].fpSet.finalize(&pCtx[j], pBlock);
        if (TAOS_FAILED(tmpRes)) {
          qError("%s build result data block error, code %s", GET_TASKID(pTaskInfo), tstrerror(tmpRes));
          QUERY_CHECK_CODE(code, lino, _end);
        }
      } else if (strcmp(pCtx[j].pExpr->pExpr->_function.functionName, "_select_value") == 0) {
        // do nothing, todo refactor
      } else {
        // expand the result into multiple rows. E.g., _wstart, top(k, 20)
        // the _wstart needs to copy to 20 following rows, since the results of top-k expands to 20 different rows.
        SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, slotId);
        char*            in = GET_ROWCELL_INTERBUF(pCtx[j].resultInfo);
        for (int32_t k = 0; k < pRow->numOfRows; ++k) {
          code = colDataSetVal(pColInfoData, pBlock->info.rows + k, in, pCtx[j].resultInfo->isNullRes);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
    }

    pBlock->info.rows += pRow->numOfRows;
  }

  pBlock->info.dataLoad = 1;
  code = blockDataUpdateTsWindow(pBlock, 0);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
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

bool hasSrcPrimaryKeyCol(SSteamOpBasicInfo* pInfo) { return pInfo->primaryPkIndex != -1; }

static int32_t doStreamIntervalAggImpl(SOperatorInfo* pOperator, SSDataBlock* pSDataBlock, uint64_t groupId,
                                    SSHashObj* pUpdatedMap, SSHashObj* pDeletedMap) {
  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      lino = 0;
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

  void*            pPkVal = NULL;
  int32_t          pkLen = 0;
  SColumnInfoData* pPkColDataInfo = NULL;
  if (hasSrcPrimaryKeyCol(&pInfo->basic)) {
    pPkColDataInfo = taosArrayGet(pSDataBlock->pDataBlock, pInfo->basic.primaryPkIndex);
  }

  if (pSDataBlock->info.window.skey != tsCols[0] || pSDataBlock->info.window.ekey != tsCols[endRowId]) {
    qError("table uid %" PRIu64 " data block timestamp range may not be calculated! minKey %" PRId64 ",maxKey %" PRId64,
           pSDataBlock->info.id.uid, pSDataBlock->info.window.skey, pSDataBlock->info.window.ekey);
    code = blockDataUpdateTsWindow(pSDataBlock, pInfo->primaryTsIndex);
    QUERY_CHECK_CODE(code, lino, _end);

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
    if (hasSrcPrimaryKeyCol(&pInfo->basic) && !IS_FINAL_INTERVAL_OP(pOperator) && pInfo->ignoreExpiredData &&
        pSDataBlock->info.type != STREAM_PULL_DATA) {
      pPkVal = colDataGetData(pPkColDataInfo, startPos);
      pkLen = colDataGetRowLength(pPkColDataInfo, startPos);
    }

    if ((!IS_FINAL_INTERVAL_OP(pOperator) && pInfo->ignoreExpiredData && pSDataBlock->info.type != STREAM_PULL_DATA &&
         checkExpiredData(&pInfo->stateStore, pInfo->pUpdateInfo, &pInfo->twAggSup, pSDataBlock->info.id.uid,
                          nextWin.ekey, pPkVal, pkLen)) ||
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
          code = addPullWindow(pInfo->pPullDataMap, &winRes, pInfo->numOfChild);
          QUERY_CHECK_CODE(code, lino, _end);

          if (pInfo->destHasPrimaryKey) {
            code = tSimpleHashPut(pInfo->pDeletedMap, &winRes, sizeof(SWinKey), NULL, 0);
            QUERY_CHECK_CODE(code, lino, _end);
          }
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

    int32_t winCode = TSDB_CODE_SUCCESS;
    code = setIntervalOutputBuf(pInfo->pState, &nextWin, &pResPos, groupId, pSup->pCtx, numOfOutput,
                                pSup->rowEntryInfoOffset, &pInfo->aggSup, &pInfo->stateStore, &winCode);
    QUERY_CHECK_CODE(code, lino, _end);

    pResult = (SResultRow*)pResPos->pRowBuff;

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

    if (pInfo->destHasPrimaryKey && winCode == TSDB_CODE_SUCCESS && IS_NORMAL_INTERVAL_OP(pOperator)) {
      code = tSimpleHashPut(pDeletedMap, &key, sizeof(SWinKey), NULL, 0);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE && pUpdatedMap) {
      code = saveWinResult(&key, pResPos, pUpdatedMap);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_WINDOW_CLOSE) {
      pResPos->beUpdated = true;
      code = tSimpleHashPut(pInfo->aggSup.pResultRowHashTable, &key, sizeof(SWinKey), &pResPos, POINTER_BYTES);
      QUERY_CHECK_CODE(code, lino, _end);
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
_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
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

int32_t decodeSPullWindowInfoArray(void* buf, SArray* pPullInfos, void** ppBuf) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t size = 0;
  buf = taosDecodeFixedI32(buf, &size);
  for (int32_t i = 0; i < size; i++) {
    SPullWindowInfo item = {0};
    buf = decodeSPullWindowInfo(buf, &item);
    void* tmp = taosArrayPush(pPullInfos, &item);
    if (!tmp) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }
  (*ppBuf) = buf;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
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
  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      lino = 0;
  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  if (!pInfo) {
    code = TSDB_CODE_FAILED;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  // 6.checksum
  int32_t dataLen = len - sizeof(uint32_t);
  void*   pCksum = POINTER_SHIFT(buf, dataLen);
  if (taosCheckChecksum(buf, dataLen, *(uint32_t*)pCksum) != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_FAILED;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  // 1.pResultRowHashTable
  int32_t mapSize = 0;
  buf = taosDecodeFixedI32(buf, &mapSize);
  for (int32_t i = 0; i < mapSize; i++) {
    SWinKey key = {0};
    buf = decodeSWinKey(buf, &key);
    SRowBuffPos* pPos = NULL;
    int32_t      resSize = pInfo->aggSup.resultRowSize;
    int32_t      winCode = TSDB_CODE_SUCCESS;
    code = pInfo->stateStore.streamStateAddIfNotExist(pInfo->pState, &key, (void**)&pPos, &resSize, &winCode);
    QUERY_CHECK_CODE(code, lino, _end);
    QUERY_CHECK_CONDITION((winCode == TSDB_CODE_SUCCESS), code, lino, _end, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);

    code = tSimpleHashPut(pInfo->aggSup.pResultRowHashTable, &key, sizeof(SWinKey), &pPos, POINTER_BYTES);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  // 2.twAggSup
  buf = decodeSTimeWindowAggSupp(buf, &pInfo->twAggSup);

  // 3.pPullDataMap
  int32_t size = 0;
  buf = taosDecodeFixedI32(buf, &size);
  for (int32_t i = 0; i < size; i++) {
    SWinKey key = {0};
    SArray* pArray = taosArrayInit(0, sizeof(int32_t));
    if (!pArray) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      QUERY_CHECK_CODE(code, lino, _end);
    }

    buf = decodeSWinKey(buf, &key);
    int32_t chSize = 0;
    buf = taosDecodeFixedI32(buf, &chSize);
    for (int32_t i = 0; i < chSize; i++) {
      int32_t chId = 0;
      buf = taosDecodeFixedI32(buf, &chId);
      void* tmp = taosArrayPush(pArray, &chId);
      if (!tmp) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
    code = taosHashPut(pInfo->pPullDataMap, &key, sizeof(SWinKey), &pArray, POINTER_BYTES);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  // 4.pPullWins
  code = decodeSPullWindowInfoArray(buf, pInfo->pPullWins, &buf);
  QUERY_CHECK_CODE(code, lino, _end);

  // 5.dataVersion
  buf = taosDecodeFixedI64(buf, &pInfo->dataVersion);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
}

void doStreamIntervalSaveCheckpoint(SOperatorInfo* pOperator) {
  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  if (needSaveStreamOperatorInfo(&pInfo->basic)) {
    int32_t len = doStreamIntervalEncodeOpState(NULL, 0, pOperator);
    void*   buf = taosMemoryCalloc(1, len);
    if (!buf) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(TSDB_CODE_OUT_OF_MEMORY));
      return;
    }
    void* pBuf = buf;
    len = doStreamIntervalEncodeOpState(&pBuf, len, pOperator);
    pInfo->stateStore.streamStateSaveInfo(pInfo->pState, STREAM_INTERVAL_OP_CHECKPOINT_NAME,
                                          strlen(STREAM_INTERVAL_OP_CHECKPOINT_NAME), buf, len);
    taosMemoryFree(buf);
    saveStreamOperatorStateComplete(&pInfo->basic);
  }
}

static int32_t copyIntervalDeleteKey(SSHashObj* pMap, SArray* pWins) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pMap, pIte, &iter)) != NULL) {
    void* pKey = tSimpleHashGetKey(pIte, NULL);
    void* tmp = taosArrayPush(pWins, pKey);
    if (!tmp) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }
  tSimpleHashClear(pMap);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t buildIntervalResult(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  int32_t                      code = TSDB_CODE_SUCCESS;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  uint16_t                     opType = pOperator->operatorType;

  // check if query task is closed or not
  if (isTaskKilled(pTaskInfo)) {
    (*ppRes) = NULL;
    return code;
  }

  if (IS_FINAL_INTERVAL_OP(pOperator)) {
    doBuildPullDataBlock(pInfo->pPullWins, &pInfo->pullIndex, pInfo->pPullDataRes);
    if (pInfo->pPullDataRes->info.rows != 0) {
      // process the rest of the data
      printDataBlock(pInfo->pPullDataRes, getStreamOpName(opType), GET_TASKID(pTaskInfo));
      (*ppRes) = pInfo->pPullDataRes;
      return code;
    }
  }

  doBuildDeleteResult(pInfo, pInfo->pDelWins, &pInfo->delIndex, pInfo->pDelRes);
  if (pInfo->pDelRes->info.rows != 0) {
    // process the rest of the data
    printDataBlock(pInfo->pDelRes, getStreamOpName(opType), GET_TASKID(pTaskInfo));
    (*ppRes) = pInfo->pDelRes;
    return code;
  }

  doBuildStreamIntervalResult(pOperator, pInfo->pState, pInfo->binfo.pRes, &pInfo->groupResInfo);
  if (pInfo->binfo.pRes->info.rows != 0) {
    printDataBlock(pInfo->binfo.pRes, getStreamOpName(opType), GET_TASKID(pTaskInfo));
    (*ppRes) = pInfo->binfo.pRes;
    return code;
  }

  (*ppRes) = NULL;
  return code;
}

int32_t copyUpdateResult(SSHashObj** ppWinUpdated, SArray* pUpdated, __compar_fn_t compar) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(*ppWinUpdated, pIte, &iter)) != NULL) {
    void* tmp = taosArrayPush(pUpdated, pIte);
    if (!tmp) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }
  taosArraySort(pUpdated, compar);
  tSimpleHashCleanup(*ppWinUpdated);
  *ppWinUpdated = NULL;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doStreamFinalIntervalAggNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      lino = 0;
  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*                 pAPI = &pOperator->pTaskInfo->storageAPI;

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  SExprSupp*     pSup = &pOperator->exprSupp;

  qDebug("stask:%s  %s status: %d", GET_TASKID(pTaskInfo), getStreamOpName(pOperator->operatorType), pOperator->status);

  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return code;
  } else if (pOperator->status == OP_RES_TO_RETURN) {
    SSDataBlock* resBlock = NULL;
    code = buildIntervalResult(pOperator, &resBlock);
    QUERY_CHECK_CODE(code, lino, _end);
    if (resBlock != NULL) {
      (*ppRes) = resBlock;
      return code;
    }

    if (pInfo->recvGetAll) {
      pInfo->recvGetAll = false;
      resetUnCloseWinInfo(pInfo->aggSup.pResultRowHashTable);
    }

    if (pInfo->reCkBlock) {
      pInfo->reCkBlock = false;
      printDataBlock(pInfo->pCheckpointRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
      (*ppRes) = pInfo->pCheckpointRes;
      return code;
    }

    setStreamOperatorCompleted(pOperator);
    if (!IS_FINAL_INTERVAL_OP(pOperator)) {
      clearFunctionContext(&pOperator->exprSupp);
      // semi interval operator clear disk buffer
      clearStreamIntervalOperator(pInfo);
      qDebug("===stream===clear semi operator");
    }
    (*ppRes) = NULL;
    return code;
  } else {
    if (!IS_FINAL_INTERVAL_OP(pOperator)) {
      SSDataBlock* resBlock = NULL;
      code = buildIntervalResult(pOperator, &resBlock);
      QUERY_CHECK_CODE(code, lino, _end);
      if (resBlock != NULL) {
        (*ppRes) = resBlock;
        return code;
      }

      if (pInfo->recvRetrive) {
        pInfo->recvRetrive = false;
        printDataBlock(pInfo->pMidRetriveRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
        (*ppRes) = pInfo->pMidRetriveRes;
        return code;
      }
    }
  }

  if (!pInfo->pUpdated) {
    pInfo->pUpdated = taosArrayInit(4096, POINTER_BYTES);
    QUERY_CHECK_NULL(pInfo->pUpdated, code, lino, _end, terrno);
  }
  if (!pInfo->pUpdatedMap) {
    _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
    pInfo->pUpdatedMap = tSimpleHashInit(4096, hashFn);
    QUERY_CHECK_NULL(pInfo->pUpdatedMap, code, lino, _end, terrno);
  }

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
      pOperator->status = OP_RES_TO_RETURN;
      qDebug("===stream===return data:%s. recv datablock num:%" PRIu64, getStreamOpName(pOperator->operatorType),
             pInfo->numOfDatapack);
      pInfo->numOfDatapack = 0;
      break;
    }

    pInfo->numOfDatapack++;
    printSpecDataBlock(pBlock, getStreamOpName(pOperator->operatorType), "recv", GET_TASKID(pTaskInfo));
    setStreamOperatorState(&pInfo->basic, pBlock->info.type);

    if (pBlock->info.type == STREAM_NORMAL || pBlock->info.type == STREAM_PULL_DATA) {
      pInfo->binfo.pRes->info.type = pBlock->info.type;
    } else if (pBlock->info.type == STREAM_DELETE_DATA || pBlock->info.type == STREAM_DELETE_RESULT ||
               pBlock->info.type == STREAM_CLEAR) {
      SArray*   delWins = taosArrayInit(8, sizeof(SWinKey));
      QUERY_CHECK_NULL(delWins, code, lino, _end, terrno);
      SHashObj* finalMap = IS_FINAL_INTERVAL_OP(pOperator) ? pInfo->pFinalPullDataMap : NULL;
      code = doDeleteWindows(pOperator, &pInfo->interval, pBlock, delWins, pInfo->pUpdatedMap, finalMap);
      QUERY_CHECK_CODE(code, lino, _end);

      if (IS_FINAL_INTERVAL_OP(pOperator)) {
        int32_t chId = getChildIndex(pBlock);
        code = addRetriveWindow(delWins, pInfo, chId);
        QUERY_CHECK_CODE(code, lino, _end);

        if (pBlock->info.type != STREAM_CLEAR) {
          void* tmp = taosArrayAddAll(pInfo->pDelWins, delWins);
          if (!tmp && taosArrayGetSize(delWins) > 0) {
            code = TSDB_CODE_OUT_OF_MEMORY;
            QUERY_CHECK_CODE(code, lino, _end);
          }
        }
        taosArrayDestroy(delWins);
        continue;
      }
      removeResults(delWins, pInfo->pUpdatedMap);
      void* tmp = taosArrayAddAll(pInfo->pDelWins, delWins);
      if (!tmp && taosArrayGetSize(delWins) > 0) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        QUERY_CHECK_CODE(code, lino, _end);
      }
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
        (*ppRes) = pInfo->pDelRes;
        return code;
      }

      break;
    } else if (pBlock->info.type == STREAM_GET_ALL && IS_FINAL_INTERVAL_OP(pOperator)) {
      pInfo->recvGetAll = true;
      code = getAllIntervalWindow(pInfo->aggSup.pResultRowHashTable, pInfo->pUpdatedMap);
      QUERY_CHECK_CODE(code, lino, _end);
      continue;
    } else if (pBlock->info.type == STREAM_RETRIEVE) {
      if (!IS_FINAL_INTERVAL_OP(pOperator)) {
        pInfo->recvRetrive = true;
        code = copyDataBlock(pInfo->pMidRetriveRes, pBlock);
        QUERY_CHECK_CODE(code, lino, _end);

        pInfo->pMidRetriveRes->info.type = STREAM_MID_RETRIEVE;
        code = doDeleteWindows(pOperator, &pInfo->interval, pBlock, NULL, pInfo->pUpdatedMap, NULL);
        QUERY_CHECK_CODE(code, lino, _end);
        break;
      }
      continue;
    } else if (pBlock->info.type == STREAM_PULL_OVER && IS_FINAL_INTERVAL_OP(pOperator)) {
      code = processPullOver(pBlock, pInfo->pPullDataMap, pInfo->pFinalPullDataMap, &pInfo->interval, pInfo->pPullWins,
                             pInfo->numOfChild, pOperator, NULL);
      QUERY_CHECK_CODE(code, lino, _end);
      continue;
    } else if (pBlock->info.type == STREAM_CREATE_CHILD_TABLE) {
      (*ppRes) = pBlock;
      return code;
    } else if (pBlock->info.type == STREAM_CHECKPOINT) {
      pAPI->stateStore.streamStateCommit(pInfo->pState);
      doStreamIntervalSaveCheckpoint(pOperator);
      code = copyDataBlock(pInfo->pCheckpointRes, pBlock);
      QUERY_CHECK_CODE(code, lino, _end);

      continue;
    } else if (IS_FINAL_INTERVAL_OP(pOperator) && pBlock->info.type == STREAM_MID_RETRIEVE) {
      continue;
    } else {
      if (pBlock->info.type != STREAM_INVALID) {
        code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }

    if (pInfo->scalarSupp.pExprInfo != NULL) {
      SExprSupp* pExprSup = &pInfo->scalarSupp;
      code = projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    code = setInputDataBlock(pSup, pBlock, TSDB_ORDER_ASC, MAIN_SCAN, true);
    QUERY_CHECK_CODE(code, lino, _end);

    code = doStreamIntervalAggImpl(pOperator, pBlock, pBlock->info.id.groupId, pInfo->pUpdatedMap, pInfo->pDeletedMap);
    if (code == TSDB_CODE_STREAM_INTERNAL_ERROR) {
      code = TSDB_CODE_SUCCESS;
      pOperator->status = OP_RES_TO_RETURN;
      break;
    }
    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.window.ekey);
    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.watermark);
    pInfo->twAggSup.minTs = TMIN(pInfo->twAggSup.minTs, pBlock->info.window.skey);
  }

  if (IS_FINAL_INTERVAL_OP(pOperator) && !pInfo->destHasPrimaryKey) {
    removeDeleteResults(pInfo->pUpdatedMap, pInfo->pDelWins);
  }
  if (IS_FINAL_INTERVAL_OP(pOperator)) {
    code = closeStreamIntervalWindow(pInfo->aggSup.pResultRowHashTable, &pInfo->twAggSup, &pInfo->interval,
                                     pInfo->pPullDataMap, pInfo->pUpdatedMap, pInfo->pDelWins, pOperator);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pInfo->destHasPrimaryKey) {
      code = copyIntervalDeleteKey(pInfo->pDeletedMap, pInfo->pDelWins);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }
  pInfo->binfo.pRes->info.watermark = pInfo->twAggSup.maxTs;

  code = copyUpdateResult(&pInfo->pUpdatedMap, pInfo->pUpdated, winPosCmprImpl);
  QUERY_CHECK_CODE(code, lino, _end);

  initMultiResInfoFromArrayList(&pInfo->groupResInfo, pInfo->pUpdated);
  pInfo->pUpdated = NULL;
  code = blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  SSDataBlock* resBlock = NULL;
  code = buildIntervalResult(pOperator, &resBlock);
  QUERY_CHECK_CODE(code, lino, _end);
  if (resBlock != NULL) {
    (*ppRes) = resBlock;
    return code;
  }

  if (pInfo->recvRetrive) {
    pInfo->recvRetrive = false;
    printDataBlock(pInfo->pMidRetriveRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
    (*ppRes) = pInfo->pMidRetriveRes;
    return code;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    pTaskInfo->code = code;
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  setStreamOperatorCompleted(pOperator);
  (*ppRes) = NULL;
  return code;
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
  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      lino = 0;
  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  if (pOperator->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL &&
      pOperator->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_MID_INTERVAL) {
    int32_t size = 0;
    void*   pBuf = NULL;
    code = pInfo->stateStore.streamStateGetInfo(pInfo->pState, STREAM_INTERVAL_OP_STATE_NAME,
                                                strlen(STREAM_INTERVAL_OP_STATE_NAME), &pBuf, &size);
    QUERY_CHECK_CODE(code, lino, _end);

    TSKEY ts = *(TSKEY*)pBuf;
    taosMemoryFree(pBuf);
    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, ts);
    pInfo->stateStore.streamStateReloadInfo(pInfo->pState, ts);
  }
  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (downstream->fpSet.reloadStreamStateFn) {
    downstream->fpSet.reloadStreamStateFn(downstream);
  }
  reloadFromDownStream(downstream, pInfo);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
}

int32_t createStreamFinalIntervalOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode,
                                                     SExecTaskInfo* pTaskInfo, int32_t numOfChild,
                                                     SReadHandle* pHandle, SOperatorInfo** pOptrInfo) {
  QRY_OPTR_CHECK(pOptrInfo);

  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      lino = 0;
  SIntervalPhysiNode*          pIntervalPhyNode = (SIntervalPhysiNode*)pPhyNode;
  SStreamIntervalOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamIntervalOperatorInfo));
  SOperatorInfo*               pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _error);
  }

  pOperator->exprSupp.hasWindowOrGroup = true;
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
  pInfo->primaryTsIndex = ((SColumnNode*)pIntervalPhyNode->window.pTspk)->slotId;
  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  initResultSizeInfo(&pOperator->resultInfo, 4096);
  if (pIntervalPhyNode->window.pExprs != NULL) {
    int32_t    numOfScalar = 0;
    SExprInfo* pScalarExprInfo = NULL;

    code = createExprInfo(pIntervalPhyNode->window.pExprs, NULL, &pScalarExprInfo, &numOfScalar);
    QUERY_CHECK_CODE(code, lino, _error);

    code = initExprSupp(&pInfo->scalarSupp, pScalarExprInfo, numOfScalar, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pPhyNode->pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pResBlock, code, lino, _error, terrno);
  initBasicInfo(&pInfo->binfo, pResBlock);

  pInfo->pState = taosMemoryCalloc(1, sizeof(SStreamState));
  QUERY_CHECK_NULL(pInfo->pState, code, lino, _error, terrno);
  qInfo("open state %p", pInfo->pState);
  pAPI->stateStore.streamStateCopyBackend(pTaskInfo->streamInfo.pState, pInfo->pState);
  //*(pInfo->pState) = *(pTaskInfo->streamInfo.pState);

  qInfo("copy state %p to %p", pTaskInfo->streamInfo.pState, pInfo->pState);

  pAPI->stateStore.streamStateSetNumber(pInfo->pState, -1, pInfo->primaryTsIndex);

  int32_t      numOfCols = 0;
  SExprInfo*   pExprInfo = NULL;
  code = createExprInfo(pIntervalPhyNode->window.pFuncs, NULL, &pExprInfo, &numOfCols);
  QUERY_CHECK_CODE(code, lino, _error);

  code = initAggSup(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, numOfCols, keyBufSize, pTaskInfo->id.str,
                    pInfo->pState, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);
  tSimpleHashSetFreeFp(pInfo->aggSup.pResultRowHashTable, destroyFlusedppPos);

  code = initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);
  QUERY_CHECK_CODE(code, lino, _error);
  initResultRowInfo(&pInfo->binfo.resultRowInfo);

  pInfo->numOfChild = numOfChild;
  pInfo->pPhyNode = NULL;
  code = nodesCloneNode((SNode*)pPhyNode, (SNode**)&pInfo->pPhyNode);
  if (TSDB_CODE_SUCCESS != code) {
    goto _error;
  }

  pInfo->pPullWins = taosArrayInit(8, sizeof(SPullWindowInfo));
  QUERY_CHECK_NULL(pInfo->pPullWins, code, lino, _error, terrno);
  pInfo->pullIndex = 0;
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pInfo->pPullDataMap = taosHashInit(64, hashFn, true, HASH_NO_LOCK);
  pInfo->pFinalPullDataMap = taosHashInit(64, hashFn, true, HASH_NO_LOCK);

  code = createSpecialDataBlock(STREAM_RETRIEVE, &pInfo->pPullDataRes);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->ignoreExpiredData = pIntervalPhyNode->window.igExpired;
  pInfo->ignoreExpiredDataSaved = false;
  code = createSpecialDataBlock(STREAM_DELETE_RESULT, &pInfo->pDelRes);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->delIndex = 0;
  pInfo->pDelWins = taosArrayInit(4, sizeof(SWinKey));
  QUERY_CHECK_NULL(pInfo->pDelWins, code, lino, _error, terrno);
  pInfo->delKey.ts = INT64_MAX;
  pInfo->delKey.groupId = 0;
  pInfo->numOfDatapack = 0;
  pInfo->pUpdated = NULL;
  pInfo->pUpdatedMap = NULL;
  pInfo->stateStore = pTaskInfo->storageAPI.stateStore;
  int32_t funResSize = getMaxFunResSize(&pOperator->exprSupp, numOfCols);
  pInfo->pState->pFileState = pAPI->stateStore.streamFileStateInit(
      tsStreamBufferSize, sizeof(SWinKey), pInfo->aggSup.resultRowSize, funResSize, compareTs, pInfo->pState,
      pInfo->twAggSup.deleteMark, GET_TASKID(pTaskInfo), pHandle->checkpointId, STREAM_STATE_BUFF_HASH);
  QUERY_CHECK_NULL(pInfo->pState->pFileState, code, lino, _error, terrno);

  pInfo->dataVersion = 0;
  pInfo->recvGetAll = false;
  pInfo->recvPullover = false;
  pInfo->recvRetrive = false;

  code = createSpecialDataBlock(STREAM_CHECKPOINT, &pInfo->pCheckpointRes);
  QUERY_CHECK_CODE(code, lino, _error);
  code = createSpecialDataBlock(STREAM_MID_RETRIEVE, &pInfo->pMidRetriveRes);
  QUERY_CHECK_CODE(code, lino, _error);
  code = createSpecialDataBlock(STREAM_MID_RETRIEVE, &pInfo->pMidPulloverRes);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->clearState = false;
  pInfo->pMidPullDatas = taosArrayInit(4, sizeof(SWinKey));
  QUERY_CHECK_NULL(pInfo->pMidPullDatas, code, lino, _error, terrno);
  pInfo->pDeletedMap = tSimpleHashInit(4096, hashFn);
  QUERY_CHECK_NULL(pInfo->pDeletedMap, code, lino, _error, terrno);
  pInfo->destHasPrimaryKey = pIntervalPhyNode->window.destHasPrimayKey;

  pOperator->operatorType = pPhyNode->type;
  if (!IS_FINAL_INTERVAL_OP(pOperator) || numOfChild == 0) {
    pInfo->twAggSup.calTrigger = STREAM_TRIGGER_AT_ONCE;
  }
  pOperator->name = getStreamOpName(pOperator->operatorType);
  pOperator->blocking = true;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;

  if (pPhyNode->type == QUERY_NODE_PHYSICAL_PLAN_STREAM_MID_INTERVAL) {
    pOperator->fpSet = createOperatorFpSet(NULL, doStreamMidIntervalAggNext, NULL, destroyStreamFinalIntervalOperatorInfo,
                                           optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  } else {
    pOperator->fpSet = createOperatorFpSet(NULL, doStreamFinalIntervalAggNext, NULL, destroyStreamFinalIntervalOperatorInfo,
                                           optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  }
  setOperatorStreamStateFn(pOperator, streamIntervalReleaseState, streamIntervalReloadState);
  if (pPhyNode->type == QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL ||
      pPhyNode->type == QUERY_NODE_PHYSICAL_PLAN_STREAM_MID_INTERVAL) {
    code = initIntervalDownStream(downstream, pPhyNode->type, pInfo);
    QUERY_CHECK_CODE(code, lino, _error);
  }
  code = appendDownstream(pOperator, &downstream, 1);
  QUERY_CHECK_CODE(code, lino, _error);

  // for stream
  void*   buff = NULL;
  int32_t len = 0;
  int32_t res = pAPI->stateStore.streamStateGetInfo(pInfo->pState, STREAM_INTERVAL_OP_CHECKPOINT_NAME,
                                                    strlen(STREAM_INTERVAL_OP_CHECKPOINT_NAME), &buff, &len);
  if (res == TSDB_CODE_SUCCESS) {
    doStreamIntervalDecodeOpState(buff, len, pOperator);
    taosMemoryFree(buff);
  }

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  if (pInfo != NULL) destroyStreamFinalIntervalOperatorInfo(pInfo);
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  return code;
}

void destroyStreamAggSupporter(SStreamAggSupporter* pSup) {
  tSimpleHashCleanup(pSup->pResultRows);
  destroyDiskbasedBuf(pSup->pResultBuf);
  blockDataDestroy(pSup->pScanBlock);
  if (pSup->stateStore.streamFileStateDestroy != NULL) {
    pSup->stateStore.streamFileStateDestroy(pSup->pState->pFileState);
  }
  taosMemoryFreeClear(pSup->pState);
  taosMemoryFreeClear(pSup->pDummyCtx);
}

void destroyStreamSessionAggOperatorInfo(void* param) {
  if (param == NULL) {
    return;
  }
  SStreamSessionAggOperatorInfo* pInfo = (SStreamSessionAggOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);
  destroyStreamAggSupporter(&pInfo->streamAggSup);
  cleanupExprSupp(&pInfo->scalarSupp);
  clearGroupResInfo(&pInfo->groupResInfo);
  taosArrayDestroyP(pInfo->pUpdated, destroyFlusedPos);
  pInfo->pUpdated = NULL;

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
  cleanupGroupResInfo(&pInfo->groupResInfo);

  taosArrayDestroy(pInfo->historyWins);
  blockDataDestroy(pInfo->pCheckpointRes);
  tSimpleHashCleanup(pInfo->pPkDeleted);

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

int32_t initDownStream(SOperatorInfo* downstream, SStreamAggSupporter* pAggSup, uint16_t type, int32_t tsColIndex,
                       STimeWindowAggSupp* pTwSup, struct SSteamOpBasicInfo* pBasic) {
  SExecTaskInfo* pTaskInfo = downstream->pTaskInfo;
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  if (downstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_PARTITION) {
    SStreamPartitionOperatorInfo* pScanInfo = downstream->info;
    pScanInfo->tsColIndex = tsColIndex;
  }

  if (downstream->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    code = initDownStream(downstream->pDownstream[0], pAggSup, type, tsColIndex, pTwSup, pBasic);
    return code;
  }
  SStreamScanInfo* pScanInfo = downstream->info;
  pScanInfo->windowSup = (SWindowSupporter){.pStreamAggSup = pAggSup, .gap = pAggSup->gap, .parentType = type};
  pScanInfo->pState = pAggSup->pState;
  if (!pScanInfo->pUpdateInfo) {
    code = pAggSup->stateStore.updateInfoInit(60000, TSDB_TIME_PRECISION_MILLI, pTwSup->waterMark,
                                              pScanInfo->igCheckUpdate, pScanInfo->pkColType, pScanInfo->pkColLen,
                                              &pScanInfo->pUpdateInfo);
    QUERY_CHECK_CODE(code, lino, _end);
  }
  pScanInfo->twAggSup = *pTwSup;
  pAggSup->pUpdateInfo = pScanInfo->pUpdateInfo;
  pBasic->primaryPkIndex = pScanInfo->primaryKeyIndex;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

static TSKEY sesionTs(void* pKey) {
  SSessionKey* pWinKey = (SSessionKey*)pKey;
  return pWinKey->win.skey;
}

int32_t initStreamAggSupporter(SStreamAggSupporter* pSup, SExprSupp* pExpSup, int32_t numOfOutput, int64_t gap,
                               SStreamState* pState, int32_t keySize, int16_t keyType, SStateStore* pStore,
                               SReadHandle* pHandle, STimeWindowAggSupp* pTwAggSup, const char* taskIdStr,
                               SStorageAPI* pApi, int32_t tsIndex) {
  pSup->resultRowSize = keySize + getResultRowSize(pExpSup->pCtx, numOfOutput);
  int32_t lino = 0;
  int32_t code = createSpecialDataBlock(STREAM_CLEAR, &pSup->pScanBlock);
  if (code) {
    return code;
  }

  pSup->gap = gap;
  pSup->stateKeySize = keySize;
  pSup->stateKeyType = keyType;
  pSup->pDummyCtx = (SqlFunctionCtx*)taosMemoryCalloc(numOfOutput, sizeof(SqlFunctionCtx));
  if (pSup->pDummyCtx == NULL) {
    return terrno;
  }

  pSup->stateStore = *pStore;
  pSup->pSessionAPI = pApi;

  initDummyFunction(pSup->pDummyCtx, pExpSup->pCtx, numOfOutput);
  pSup->pState = taosMemoryCalloc(1, sizeof(SStreamState));
  if (!pSup->pState) {
    return terrno;
  }
  *(pSup->pState) = *pState;
  pSup->stateStore.streamStateSetNumber(pSup->pState, -1, tsIndex);
  int32_t funResSize = getMaxFunResSize(pExpSup, numOfOutput);
  pSup->pState->pFileState = pSup->stateStore.streamFileStateInit(
      tsStreamBufferSize, sizeof(SSessionKey), pSup->resultRowSize, funResSize, sesionTs, pSup->pState,
      pTwAggSup->deleteMark, taskIdStr, pHandle->checkpointId, STREAM_STATE_BUFF_SORT);
  QUERY_CHECK_NULL(pSup->pState->pFileState, code, lino, _end, terrno);

  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pSup->pResultRows = tSimpleHashInit(32, hashFn);
  if (!pSup->pResultRows) {
    return terrno;
  }

  for (int32_t i = 0; i < numOfOutput; ++i) {
    pExpSup->pCtx[i].saveHandle.pState = pSup->pState;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
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

void clearOutputBuf(void* pState, SRowBuffPos* pPos, SStateStore* pAPI) { pAPI->streamStateClearBuff(pState, pPos); }

int32_t setSessionOutputBuf(SStreamAggSupporter* pAggSup, TSKEY startTs, TSKEY endTs, uint64_t groupId,
                            SResultWindowInfo* pCurWin) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  pCurWin->sessionWin.groupId = groupId;
  pCurWin->sessionWin.win.skey = startTs;
  pCurWin->sessionWin.win.ekey = endTs;
  int32_t size = pAggSup->resultRowSize;
  int32_t winCode = TSDB_CODE_SUCCESS;
  code = pAggSup->stateStore.streamStateSessionAddIfNotExist(pAggSup->pState, &pCurWin->sessionWin, pAggSup->gap,
                                                             (void**)&pCurWin->pStatePos, &size, &winCode);
  QUERY_CHECK_CODE(code, lino, _end);

  if (winCode == TSDB_CODE_SUCCESS && !inWinRange(&pAggSup->winRange, &pCurWin->sessionWin.win)) {
    winCode = TSDB_CODE_FAILED;
    clearOutputBuf(pAggSup->pState, pCurWin->pStatePos, &pAggSup->pSessionAPI->stateStore);
  }

  if (winCode == TSDB_CODE_SUCCESS) {
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

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

void getSessionWinBuf(SStreamAggSupporter* pAggSup, SStreamStateCur* pCur, SResultWindowInfo* pWinInfo,
                      int32_t* pWinCode) {
  int32_t size = 0;
  (*pWinCode) = pAggSup->stateStore.streamStateSessionGetKVByCur(pCur, &pWinInfo->sessionWin,
                                                                 (void**)&pWinInfo->pStatePos, &size);
  if ((*pWinCode) != TSDB_CODE_SUCCESS) {
    return;
  }

  pAggSup->stateStore.streamStateCurNext(pAggSup->pState, pCur);
}

int32_t saveDeleteInfo(SArray* pWins, SSessionKey key) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  void*   res = taosArrayPush(pWins, &key);
  if (!res) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t saveDeleteRes(SSHashObj* pStDelete, SSessionKey key) {
  key.win.ekey = key.win.skey;
  return tSimpleHashPut(pStDelete, &key, sizeof(SSessionKey), NULL, 0);
}

void releaseOutputBuf(void* pState, SRowBuffPos* pPos, SStateStore* pAPI) {
  pAPI->streamStateReleaseBuf(pState, pPos, false);
}

void removeSessionResult(SStreamAggSupporter* pAggSup, SSHashObj* pHashMap, SSHashObj* pResMap, SSessionKey* pKey) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  SSessionKey key = {0};
  getSessionHashKey(pKey, &key);
  void* pVal = tSimpleHashGet(pHashMap, &key, sizeof(SSessionKey));
  if (pVal) {
    releaseOutputBuf(pAggSup->pState, *(void**)pVal, &pAggSup->pSessionAPI->stateStore);
    int32_t tmpRes = tSimpleHashRemove(pHashMap, &key, sizeof(SSessionKey));
    qTrace("%s at line %d res:%d", __func__, __LINE__, tmpRes);
  }
  int32_t tmpRes = tSimpleHashRemove(pResMap, &key, sizeof(SSessionKey));
  qTrace("%s at line %d res:%d", __func__, __LINE__, tmpRes);
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
    SResultWindowInfo* pWin = taosArrayGet(pWins, i);
    if (!pWin) continue;
    SSessionKey key = {0};
    getSessionHashKey(&pWin->sessionWin, &key);
    int32_t tmpRes = tSimpleHashRemove(pHashMap, &key, sizeof(SSessionKey));
    qTrace("%s at line %d res:%d", __func__, __LINE__, tmpRes);
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
      int32_t tmpRes = tSimpleHashRemove(pHashMap, &key, sizeof(SSessionKey));
      qTrace("%s at line %d res:%d", __func__, __LINE__, tmpRes);
    }
  }
}

int32_t updateSessionWindowInfo(SStreamAggSupporter* pAggSup, SResultWindowInfo* pWinInfo, TSKEY* pStartTs,
                                TSKEY* pEndTs, uint64_t groupId, int32_t rows, int32_t start, int64_t gap,
                                SSHashObj* pResultRows, SSHashObj* pStUpdated, SSHashObj* pStDeleted,
                                int32_t* pWinRos) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  for (int32_t i = start; i < rows; ++i) {
    if (!isInWindow(pWinInfo, pStartTs[i], gap) && (!pEndTs || !isInWindow(pWinInfo, pEndTs[i], gap))) {
      (*pWinRos) = i - start;
      goto _end;
    }
    if (pWinInfo->sessionWin.win.skey > pStartTs[i]) {
      if (pStDeleted && pWinInfo->isOutput) {
        code = saveDeleteRes(pStDeleted, pWinInfo->sessionWin);
        QUERY_CHECK_CODE(code, lino, _end);
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
  (*pWinRos) = rows - start;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t initSessionOutputBuf(SResultWindowInfo* pWinInfo, SResultRow** pResult, SqlFunctionCtx* pCtx,
                                    int32_t numOfOutput, int32_t* rowEntryInfoOffset) {
  *pResult = (SResultRow*)pWinInfo->pStatePos->pRowBuff;
  // set time window for current result
  (*pResult)->win = pWinInfo->sessionWin.win;
  return setResultRowInitCtx(*pResult, pCtx, numOfOutput, rowEntryInfoOffset);
}

int32_t doOneWindowAggImpl(SColumnInfoData* pTimeWindowData, SResultWindowInfo* pCurWin, SResultRow** pResult,
                           int32_t startIndex, int32_t winRows, int32_t rows, int32_t numOutput,
                           SOperatorInfo* pOperator, int64_t winDelta) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExprSupp*     pSup = &pOperator->exprSupp;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  code = initSessionOutputBuf(pCurWin, pResult, pSup->pCtx, numOutput, pSup->rowEntryInfoOffset);
  QUERY_CHECK_CODE(code, lino, _end);

  updateTimeWindowInfo(pTimeWindowData, &pCurWin->sessionWin.win, winDelta);
  applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, pTimeWindowData, startIndex, winRows, rows, numOutput);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

void doDeleteSessionWindow(SStreamAggSupporter* pAggSup, SSessionKey* pKey) {
  pAggSup->stateStore.streamStateSessionDel(pAggSup->pState, pKey);
  SSessionKey hashKey = {0};
  getSessionHashKey(pKey, &hashKey);
  int32_t tmpRes = tSimpleHashRemove(pAggSup->pResultRows, &hashKey, sizeof(SSessionKey));
  qTrace("%s at line %d res:%d", __func__, __LINE__, tmpRes);
}

void setSessionWinOutputInfo(SSHashObj* pStUpdated, SResultWindowInfo* pWinInfo) {
  void* pVal = tSimpleHashGet(pStUpdated, &pWinInfo->sessionWin, sizeof(SSessionKey));
  if (pVal) {
    SResultWindowInfo* pWin = pVal;
    pWinInfo->isOutput = pWin->isOutput;
  }
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

int32_t compactTimeWindow(SExprSupp* pSup, SStreamAggSupporter* pAggSup, STimeWindowAggSupp* pTwAggSup,
                          SExecTaskInfo* pTaskInfo, SResultWindowInfo* pCurWin, SResultWindowInfo* pNextWin,
                          SSHashObj* pStUpdated, SSHashObj* pStDeleted, bool addGap) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  SResultRow* pCurResult = NULL;
  int32_t     numOfOutput = pSup->numOfExprs;
  code = initSessionOutputBuf(pCurWin, &pCurResult, pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset);
  QUERY_CHECK_CODE(code, lino, _end);

  SResultRow* pWinResult = NULL;
  code = initSessionOutputBuf(pNextWin, &pWinResult, pAggSup->pDummyCtx, numOfOutput, pSup->rowEntryInfoOffset);
  QUERY_CHECK_CODE(code, lino, _end);

  pCurWin->sessionWin.win.ekey = TMAX(pCurWin->sessionWin.win.ekey, pNextWin->sessionWin.win.ekey);
  memcpy(pCurWin->pStatePos->pKey, &pCurWin->sessionWin, sizeof(SSessionKey));

  int64_t winDelta = 0;
  if (addGap) {
    winDelta = pAggSup->gap;
  }
  updateTimeWindowInfo(&pTwAggSup->timeWindowData, &pCurWin->sessionWin.win, winDelta);
  code = compactFunctions(pSup->pCtx, pAggSup->pDummyCtx, numOfOutput, pTaskInfo, &pTwAggSup->timeWindowData);
  QUERY_CHECK_CODE(code, lino, _end);

  int32_t tmpRes = tSimpleHashRemove(pStUpdated, &pNextWin->sessionWin, sizeof(SSessionKey));
  qTrace("%s at line %d res:%d", __func__, __LINE__, tmpRes);

  if (pNextWin->isOutput && pStDeleted) {
    qDebug("===stream=== save delete window info %" PRId64 ", %" PRIu64, pNextWin->sessionWin.win.skey,
           pNextWin->sessionWin.groupId);
    code = saveDeleteRes(pStDeleted, pNextWin->sessionWin);
    QUERY_CHECK_CODE(code, lino, _end);
  }
  removeSessionResult(pAggSup, pStUpdated, pAggSup->pResultRows, &pNextWin->sessionWin);
  doDeleteSessionWindow(pAggSup, &pNextWin->sessionWin);
  releaseOutputBuf(pAggSup->pState, pNextWin->pStatePos, &pAggSup->pSessionAPI->stateStore);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

static int32_t compactSessionWindow(SOperatorInfo* pOperator, SResultWindowInfo* pCurWin, SSHashObj* pStUpdated,
                                    SSHashObj* pStDeleted, bool addGap, int32_t* pWinNum) {
  int32_t                        code = TSDB_CODE_SUCCESS;
  int32_t                        lino = 0;
  SExprSupp*                     pSup = &pOperator->exprSupp;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*                   pAPI = &pOperator->pTaskInfo->storageAPI;
  int32_t                        winNum = 0;
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  SResultRow*                    pCurResult = NULL;
  int32_t                        numOfOutput = pOperator->exprSupp.numOfExprs;
  SStreamAggSupporter*           pAggSup = &pInfo->streamAggSup;

  // Just look for the window behind StartIndex
  while (1) {
    SResultWindowInfo winInfo = {0};
    getNextSessionWinInfo(pAggSup, pStUpdated, pCurWin, &winInfo);
    if (!IS_VALID_SESSION_WIN(winInfo) || !isInWindow(pCurWin, winInfo.sessionWin.win.skey, pAggSup->gap) ||
        !inWinRange(&pAggSup->winRange, &winInfo.sessionWin.win)) {
      releaseOutputBuf(pAggSup->pState, winInfo.pStatePos, &pAggSup->pSessionAPI->stateStore);
      break;
    }
    code =
        compactTimeWindow(pSup, pAggSup, &pInfo->twAggSup, pTaskInfo, pCurWin, &winInfo, pStUpdated, pStDeleted, true);
    QUERY_CHECK_CODE(code, lino, _end);
    winNum++;
  }
  if (pWinNum) {
    (*pWinNum) = winNum;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
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
  int32_t                        code = TSDB_CODE_SUCCESS;
  int32_t                        lino = 0;
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

  void*            pPkVal = NULL;
  int32_t          pkLen = 0;
  SColumnInfoData* pPkColDataInfo = NULL;
  if (hasSrcPrimaryKeyCol(&pInfo->basic)) {
    pPkColDataInfo = taosArrayGet(pSDataBlock->pDataBlock, pInfo->basic.primaryPkIndex);
  }

  for (int32_t i = 0; i < rows;) {
    if (hasSrcPrimaryKeyCol(&pInfo->basic) && !IS_FINAL_SESSION_OP(pOperator) && pInfo->ignoreExpiredData) {
      pPkVal = colDataGetData(pPkColDataInfo, i);
      pkLen = colDataGetRowLength(pPkColDataInfo, i);
    }
    if (!IS_FINAL_SESSION_OP(pOperator) && pInfo->ignoreExpiredData &&
        checkExpiredData(&pInfo->streamAggSup.stateStore, pInfo->streamAggSup.pUpdateInfo, &pInfo->twAggSup,
                         pSDataBlock->info.id.uid, endTsCols[i], pPkVal, pkLen)) {
      i++;
      continue;
    }
    SResultWindowInfo winInfo = {0};
    code = setSessionOutputBuf(pAggSup, startTsCols[i], endTsCols[i], groupId, &winInfo);
    QUERY_CHECK_CODE(code, lino, _end);

    // coverity scan error
    if (!winInfo.pStatePos) {
      continue;
    }
    setSessionWinOutputInfo(pStUpdated, &winInfo);
    code = updateSessionWindowInfo(pAggSup, &winInfo, startTsCols, endTsCols, groupId, rows, i, pAggSup->gap,
                                   pAggSup->pResultRows, pStUpdated, pStDeleted, &winRows);
    QUERY_CHECK_CODE(code, lino, _end);

    int64_t winDelta = 0;
    if (addGap) {
      winDelta = pAggSup->gap;
    }
    code = doOneWindowAggImpl(&pInfo->twAggSup.timeWindowData, &winInfo, &pResult, i, winRows, rows, numOfOutput,
                              pOperator, winDelta);
    QUERY_CHECK_CODE(code, lino, _end);

    code = compactSessionWindow(pOperator, &winInfo, pStUpdated, pStDeleted, addGap, NULL);
    QUERY_CHECK_CODE(code, lino, _end);

    code = saveSessionOutputBuf(pAggSup, &winInfo);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pInfo->destHasPrimaryKey && winInfo.isOutput && IS_NORMAL_SESSION_OP(pOperator)) {
      code = saveDeleteRes(pInfo->pPkDeleted, winInfo.sessionWin);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE && pStUpdated) {
      code = saveResult(winInfo, pStUpdated);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_WINDOW_CLOSE) {
      winInfo.pStatePos->beUpdated = true;
      SSessionKey key = {0};
      getSessionHashKey(&winInfo.sessionWin, &key);
      code = tSimpleHashPut(pAggSup->pResultRows, &key, sizeof(SSessionKey), &winInfo, sizeof(SResultWindowInfo));
      QUERY_CHECK_CODE(code, lino, _end);
    }

    i += winRows;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
}

int32_t doDeleteTimeWindows(SStreamAggSupporter* pAggSup, SSDataBlock* pBlock, SArray* result) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
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
        code = saveDeleteInfo(result, curWin);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
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
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SStorageAPI*   pAPI = &pOp->pTaskInfo->storageAPI;
  SExecTaskInfo* pTaskInfo = pOp->pTaskInfo;

  blockDataCleanup(pBlock);
  int32_t size = tSimpleHashGetSize(pStDeleted);
  if (size == 0) {
    return;
  }
  code = blockDataEnsureCapacity(pBlock, size);
  QUERY_CHECK_CODE(code, lino, _end);

  int32_t iter = 0;
  while (((*Ite) = tSimpleHashIterate(pStDeleted, *Ite, &iter)) != NULL) {
    if (pBlock->info.rows + 1 > pBlock->info.capacity) {
      break;
    }
    SSessionKey*     res = tSimpleHashGetKey(*Ite, NULL);
    SColumnInfoData* pStartTsCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
    code = colDataSetVal(pStartTsCol, pBlock->info.rows, (const char*)&res->win.skey, false);
    QUERY_CHECK_CODE(code, lino, _end);

    SColumnInfoData* pEndTsCol = taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
    code = colDataSetVal(pEndTsCol, pBlock->info.rows, (const char*)&res->win.skey, false);
    QUERY_CHECK_CODE(code, lino, _end);

    SColumnInfoData* pUidCol = taosArrayGet(pBlock->pDataBlock, UID_COLUMN_INDEX);
    colDataSetNULL(pUidCol, pBlock->info.rows);

    SColumnInfoData* pGpCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
    code = colDataSetVal(pGpCol, pBlock->info.rows, (const char*)&res->groupId, false);
    QUERY_CHECK_CODE(code, lino, _end);

    SColumnInfoData* pCalStCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX);
    colDataSetNULL(pCalStCol, pBlock->info.rows);

    SColumnInfoData* pCalEdCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX);
    colDataSetNULL(pCalEdCol, pBlock->info.rows);

    SColumnInfoData* pTableCol = taosArrayGet(pBlock->pDataBlock, TABLE_NAME_COLUMN_INDEX);
    if (!pTableCol) {
      QUERY_CHECK_CODE(code, lino, _end);
    }

    void*   tbname = NULL;
    int32_t winCode = TSDB_CODE_SUCCESS;
    code = pAPI->stateStore.streamStateGetParName(pOp->pTaskInfo->streamInfo.pState, res->groupId, &tbname, false,
                                                  &winCode);
    QUERY_CHECK_CODE(code, lino, _end);

    if (winCode != TSDB_CODE_SUCCESS) {
      colDataSetNULL(pTableCol, pBlock->info.rows);
    } else {
      char parTbName[VARSTR_HEADER_SIZE + TSDB_TABLE_NAME_LEN];
      STR_WITH_MAXSIZE_TO_VARSTR(parTbName, tbname, sizeof(parTbName));
      code = colDataSetVal(pTableCol, pBlock->info.rows, (const char*)parTbName, false);
      QUERY_CHECK_CODE(code, lino, _end);
      pAPI->stateStore.streamStateFreeVal(tbname);
    }
    pBlock->info.rows += 1;
  }

_end:
  if ((*Ite) == NULL) {
    tSimpleHashClear(pStDeleted);
  }

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
}

static int32_t rebuildSessionWindow(SOperatorInfo* pOperator, SArray* pWinArray, SSHashObj* pStUpdated) {
  int32_t        winCode = TSDB_CODE_SUCCESS;
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
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
        getSessionWinBuf(pChAggSup, pCur, &childWin, &winCode);

        if (winCode == TSDB_CODE_SUCCESS && !inWinRange(&pAggSup->winRange, &childWin.sessionWin.win)) {
          releaseOutputBuf(pAggSup->pState, childWin.pStatePos, &pAggSup->stateStore);
          continue;
        }

        if (winCode == TSDB_CODE_SUCCESS && inWinRange(&pWinKey->win, &childWin.sessionWin.win)) {
          if (num == 0) {
            code = setSessionOutputBuf(pAggSup, pWinKey->win.skey, pWinKey->win.ekey, pWinKey->groupId, &parentWin);
            QUERY_CHECK_CODE(code, lino, _end);

            parentWin.sessionWin = childWin.sessionWin;
            memcpy(parentWin.pStatePos->pKey, &parentWin.sessionWin, sizeof(SSessionKey));
            code = initSessionOutputBuf(&parentWin, &pResult, pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset);
            QUERY_CHECK_CODE(code, lino, _end);
          }
          num++;
          parentWin.sessionWin.win.skey = TMIN(parentWin.sessionWin.win.skey, childWin.sessionWin.win.skey);
          parentWin.sessionWin.win.ekey = TMAX(parentWin.sessionWin.win.ekey, childWin.sessionWin.win.ekey);
          memcpy(parentWin.pStatePos->pKey, &parentWin.sessionWin, sizeof(SSessionKey));

          updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &parentWin.sessionWin.win, pAggSup->gap);
          code = initSessionOutputBuf(&childWin, &pChResult, pChild->exprSupp.pCtx, numOfOutput,
                                      pChild->exprSupp.rowEntryInfoOffset);
          QUERY_CHECK_CODE(code, lino, _end);

          code = compactFunctions(pSup->pCtx, pChild->exprSupp.pCtx, numOfOutput, pTaskInfo,
                                  &pInfo->twAggSup.timeWindowData);
          QUERY_CHECK_CODE(code, lino, _end);

          code = compactSessionWindow(pOperator, &parentWin, pStUpdated, NULL, true, NULL);
          QUERY_CHECK_CODE(code, lino, _end);

          releaseOutputBuf(pAggSup->pState, childWin.pStatePos, &pAggSup->stateStore);
        } else {
          releaseOutputBuf(pAggSup->pState, childWin.pStatePos, &pAggSup->stateStore);
          break;
        }
      }
      pAPI->stateStore.streamStateFreeCur(pCur);
    }
    if (num > 0) {
      code = saveResult(parentWin, pStUpdated);
      QUERY_CHECK_CODE(code, lino, _end);

      code = saveSessionOutputBuf(pAggSup, &parentWin);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

int32_t closeSessionWindow(SSHashObj* pHashMap, STimeWindowAggSupp* pTwSup, SSHashObj* pClosed) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pHashMap, pIte, &iter)) != NULL) {
    SResultWindowInfo* pWinInfo = pIte;
    if (isCloseWindow(&pWinInfo->sessionWin.win, pTwSup)) {
      if (pTwSup->calTrigger == STREAM_TRIGGER_WINDOW_CLOSE && pClosed) {
        code = saveResult(*pWinInfo, pClosed);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      SSessionKey* pKey = tSimpleHashGetKey(pIte, NULL);
      code = tSimpleHashIterateRemove(pHashMap, pKey, sizeof(SSessionKey), &pIte, &iter);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }
_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t closeChildSessionWindow(SArray* pChildren, TSKEY maxTs) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  int32_t size = taosArrayGetSize(pChildren);
  for (int32_t i = 0; i < size; i++) {
    SOperatorInfo*                 pChildOp = taosArrayGetP(pChildren, i);
    SStreamSessionAggOperatorInfo* pChInfo = pChildOp->info;
    pChInfo->twAggSup.maxTs = TMAX(pChInfo->twAggSup.maxTs, maxTs);
    code = closeSessionWindow(pChInfo->streamAggSup.pResultRows, &pChInfo->twAggSup, NULL);
    QUERY_CHECK_CODE(code, lino, _end);
  }
_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t getAllSessionWindow(SSHashObj* pHashMap, SSHashObj* pStUpdated) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pHashMap, pIte, &iter)) != NULL) {
    SResultWindowInfo* pWinInfo = pIte;
    if (!pWinInfo->pStatePos->beUpdated) {
      continue;
    }
    pWinInfo->pStatePos->beUpdated = false;
    code = saveResult(*pWinInfo, pStUpdated);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t copyDeleteWindowInfo(SArray* pResWins, SSHashObj* pStDeleted) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t size = taosArrayGetSize(pResWins);
  for (int32_t i = 0; i < size; i++) {
    SSessionKey* pWinKey = taosArrayGet(pResWins, i);
    if (!pWinKey) continue;
    SSessionKey winInfo = {0};
    getSessionHashKey(pWinKey, &winInfo);
    code = tSimpleHashPut(pStDeleted, &winInfo, sizeof(SSessionKey), NULL, 0);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
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
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
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

      void*   tbname = NULL;
      int32_t winCode = TSDB_CODE_SUCCESS;
      code = pAPI->stateStore.streamStateGetParName((void*)pTaskInfo->streamInfo.pState, pBlock->info.id.groupId,
                                                    &tbname, false, &winCode);
      QUERY_CHECK_CODE(code, lino, _end);

      if (winCode != TSDB_CODE_SUCCESS) {
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

    code = pAPI->stateStore.streamStateGetByPos(pState, pPos, (void**)&pRow);
    QUERY_CHECK_CODE(code, lino, _end);

    doUpdateNumOfRows(pCtx, pRow, numOfExprs, rowEntryOffset);
    // no results, continue to check the next one
    if (pRow->numOfRows == 0) {
      pGroupResInfo->index += 1;
      continue;
    }

    if (pBlock->info.rows + pRow->numOfRows > pBlock->info.capacity) {
      break;
    }

    pGroupResInfo->index += 1;

    for (int32_t j = 0; j < numOfExprs; ++j) {
      int32_t slotId = pExprInfo[j].base.resSchema.slotId;

      pCtx[j].resultInfo = getResultEntryInfo(pRow, j, rowEntryOffset);
      if (pCtx[j].fpSet.finalize) {
        int32_t tmpRes = pCtx[j].fpSet.finalize(&pCtx[j], pBlock);
        if (TAOS_FAILED(tmpRes)) {
          qError("%s build result data block error, code %s", GET_TASKID(pTaskInfo), tstrerror(tmpRes));
          QUERY_CHECK_CODE(code, lino, _end);
        }
      } else if (strcmp(pCtx[j].pExpr->pExpr->_function.functionName, "_select_value") == 0) {
        // do nothing, todo refactor
      } else {
        // expand the result into multiple rows. E.g., _wstart, top(k, 20)
        // the _wstart needs to copy to 20 following rows, since the results of top-k expands to 20 different rows.
        SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, slotId);
        char*            in = GET_ROWCELL_INTERBUF(pCtx[j].resultInfo);
        for (int32_t k = 0; k < pRow->numOfRows; ++k) {
          code = colDataSetVal(pColInfoData, pBlock->info.rows + k, in, pCtx[j].resultInfo->isNullRes);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
    }

    pBlock->info.dataLoad = 1;
    pBlock->info.rows += pRow->numOfRows;
  }
  code = blockDataUpdateTsWindow(pBlock, 0);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

void doBuildSessionResult(SOperatorInfo* pOperator, void* pState, SGroupResInfo* pGroupResInfo, SSDataBlock* pBlock) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  // set output datablock version
  pBlock->info.version = pTaskInfo->version;

  blockDataCleanup(pBlock);
  if (!hasRemainResults(pGroupResInfo)) {
    cleanupGroupResInfo(pGroupResInfo);
    goto _end;
  }

  // clear the existed group id
  pBlock->info.id.groupId = 0;
  code = buildSessionResultDataBlock(pOperator, pState, pBlock, &pOperator->exprSupp, pGroupResInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  if (pBlock->info.rows == 0) {
    cleanupGroupResInfo(pGroupResInfo);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
}

static int32_t buildSessionResult(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                        code = TSDB_CODE_SUCCESS;
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  SStreamAggSupporter*           pAggSup = &pInfo->streamAggSup;
  SOptrBasicInfo*                pBInfo = &pInfo->binfo;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  doBuildDeleteDataBlock(pOperator, pInfo->pStDeleted, pInfo->pDelRes, &pInfo->pDelIterator);
  if (pInfo->pDelRes->info.rows > 0) {
    printDataBlock(pInfo->pDelRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
    (*ppRes) = pInfo->pDelRes;
    return code;
  }

  doBuildSessionResult(pOperator, pAggSup->pState, &pInfo->groupResInfo, pBInfo->pRes);
  if (pBInfo->pRes->info.rows > 0) {
    printDataBlock(pBInfo->pRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
    (*ppRes) = pBInfo->pRes;
    return code;
  }
  (*ppRes) = NULL;
  return code;
}

int32_t getMaxTsWins(const SArray* pAllWins, SArray* pMaxWins) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t size = taosArrayGetSize(pAllWins);
  if (size == 0) {
    goto _end;
  }
  SResultWindowInfo* pWinInfo = taosArrayGet(pAllWins, size - 1);
  SSessionKey*       pSeKey = &pWinInfo->sessionWin;
  void*              tmp = taosArrayPush(pMaxWins, pSeKey);
  if (!tmp) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (pSeKey->groupId == 0) {
    goto _end;
  }
  uint64_t preGpId = pSeKey->groupId;
  for (int32_t i = size - 2; i >= 0; i--) {
    pWinInfo = taosArrayGet(pAllWins, i);
    pSeKey = &pWinInfo->sessionWin;
    if (preGpId != pSeKey->groupId) {
      void* tmp = taosArrayPush(pMaxWins, pSeKey);
      if (!tmp) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        QUERY_CHECK_CODE(code, lino, _end);
      }
      preGpId = pSeKey->groupId;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
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
  tlen += taosEncodeFixedI64(buf, pInfo->dataVersion);

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

int32_t doStreamSessionDecodeOpState(void* buf, int32_t len, SOperatorInfo* pOperator, bool isParent, void** ppBuf) {
  int32_t                        code = TSDB_CODE_SUCCESS;
  int32_t                        lino = 0;
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  if (!pInfo) {
    code = TSDB_CODE_FAILED;
    QUERY_CHECK_CODE(code, lino, _end);
  }
  SStreamAggSupporter* pAggSup = &pInfo->streamAggSup;

  // 5.checksum
  if (isParent) {
    int32_t dataLen = len - sizeof(uint32_t);
    void*   pCksum = POINTER_SHIFT(buf, dataLen);
    if (taosCheckChecksum(buf, dataLen, *(uint32_t*)pCksum) != TSDB_CODE_SUCCESS) {
      qError("stream session state is invalid");
      code = TSDB_CODE_FAILED;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  // 1.streamAggSup.pResultRows
  int32_t mapSize = 0;
  buf = taosDecodeFixedI32(buf, &mapSize);
  for (int32_t i = 0; i < mapSize; i++) {
    SSessionKey       key = {0};
    SResultWindowInfo winfo = {0};
    buf = decodeSSessionKey(buf, &key);
    int32_t winCode = TSDB_CODE_SUCCESS;
    code = pAggSup->stateStore.streamStateSessionAddIfNotExist(
        pAggSup->pState, &winfo.sessionWin, pAggSup->gap, (void**)&winfo.pStatePos, &pAggSup->resultRowSize, &winCode);
    QUERY_CHECK_CODE(code, lino, _end);
    QUERY_CHECK_CONDITION((winCode == TSDB_CODE_SUCCESS), code, lino, _end, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);

    buf = decodeSResultWindowInfo(buf, &winfo, pInfo->streamAggSup.resultRowSize);
    code =
        tSimpleHashPut(pInfo->streamAggSup.pResultRows, &key, sizeof(SSessionKey), &winfo, sizeof(SResultWindowInfo));
    QUERY_CHECK_CODE(code, lino, _end);
  }

  // 2.twAggSup
  buf = decodeSTimeWindowAggSupp(buf, &pInfo->twAggSup);

  // 3.pChildren
  int32_t size = 0;
  buf = taosDecodeFixedI32(buf, &size);
  for (int32_t i = 0; i < size; i++) {
    SOperatorInfo* pChOp = taosArrayGetP(pInfo->pChildren, i);
    code = doStreamSessionDecodeOpState(buf, 0, pChOp, false, &buf);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  // 4.dataVersion
  buf = taosDecodeFixedI64(buf, &pInfo->dataVersion);
  if (ppBuf) {
    (*ppBuf) = buf;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

void doStreamSessionSaveCheckpoint(SOperatorInfo* pOperator) {
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  if (needSaveStreamOperatorInfo(&pInfo->basic)) {
    int32_t len = doStreamSessionEncodeOpState(NULL, 0, pOperator, true);
    void*   buf = taosMemoryCalloc(1, len);
    if (!buf) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(TSDB_CODE_OUT_OF_MEMORY));
      return;
    }
    void* pBuf = buf;
    len = doStreamSessionEncodeOpState(&pBuf, len, pOperator, true);
    pInfo->streamAggSup.stateStore.streamStateSaveInfo(pInfo->streamAggSup.pState, STREAM_SESSION_OP_CHECKPOINT_NAME,
                                                       strlen(STREAM_SESSION_OP_CHECKPOINT_NAME), buf, len);
    taosMemoryFree(buf);
    saveStreamOperatorStateComplete(&pInfo->basic);
  }
}

void resetUnCloseSessionWinInfo(SSHashObj* winMap) {
  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(winMap, pIte, &iter)) != NULL) {
    SResultWindowInfo* pResInfo = pIte;
    pResInfo->pStatePos->beUsed = true;
  }
}

int32_t copyDeleteSessionKey(SSHashObj* source, SSHashObj* dest) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (tSimpleHashGetSize(source) == 0) {
    goto _end;
  }
  void*   pIte = NULL;
  int32_t iter = 0;
  size_t  keyLen = 0;
  while ((pIte = tSimpleHashIterate(source, pIte, &iter)) != NULL) {
    SSessionKey* pKey = tSimpleHashGetKey(pIte, &keyLen);
    code = saveDeleteRes(dest, *pKey);
    QUERY_CHECK_CODE(code, lino, _end);
  }
  tSimpleHashClear(source);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doStreamSessionAggNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                        code = TSDB_CODE_SUCCESS;
  int32_t                        lino = 0;
  SExprSupp*                     pSup = &pOperator->exprSupp;
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  SOptrBasicInfo*                pBInfo = &pInfo->binfo;
  SStreamAggSupporter*           pAggSup = &pInfo->streamAggSup;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  qDebug("stask:%s  %s status: %d", GET_TASKID(pTaskInfo), getStreamOpName(pOperator->operatorType), pOperator->status);
  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return code;
  } else if (pOperator->status == OP_RES_TO_RETURN) {
    SSDataBlock* opRes = NULL;
    code = buildSessionResult(pOperator, &opRes);
    QUERY_CHECK_CODE(code, lino, _end);
    if (opRes) {
      (*ppRes) = opRes;
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
    pInfo->pUpdated = taosArrayInit(16, sizeof(SResultWindowInfo));
    QUERY_CHECK_NULL(pInfo->pUpdated, code, lino, _end, terrno);
  }
  if (!pInfo->pStUpdated) {
    _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
    pInfo->pStUpdated = tSimpleHashInit(64, hashFn);
    QUERY_CHECK_NULL(pInfo->pStUpdated, code, lino, _end, terrno);
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
      SArray* pWins = taosArrayInit(16, sizeof(SSessionKey));
      QUERY_CHECK_NULL(pWins, code, lino, _end, terrno);
      // gap must be 0
      code = doDeleteTimeWindows(pAggSup, pBlock, pWins);
      QUERY_CHECK_CODE(code, lino, _end);

      removeSessionResults(pAggSup, pInfo->pStUpdated, pWins);
      if (IS_FINAL_SESSION_OP(pOperator)) {
        int32_t                        childIndex = getChildIndex(pBlock);
        SOperatorInfo*                 pChildOp = taosArrayGetP(pInfo->pChildren, childIndex);
        SStreamSessionAggOperatorInfo* pChildInfo = pChildOp->info;
        // gap must be 0
        code = doDeleteTimeWindows(&pChildInfo->streamAggSup, pBlock, NULL);
        QUERY_CHECK_CODE(code, lino, _end);

        code = rebuildSessionWindow(pOperator, pWins, pInfo->pStUpdated);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      code = copyDeleteWindowInfo(pWins, pInfo->pStDeleted);
      QUERY_CHECK_CODE(code, lino, _end);

      if (pInfo->destHasPrimaryKey && IS_NORMAL_SESSION_OP(pOperator)) {
        code = copyDeleteWindowInfo(pWins, pInfo->pPkDeleted);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      taosArrayDestroy(pWins);
      continue;
    } else if (pBlock->info.type == STREAM_GET_ALL) {
      pInfo->recvGetAll = true;
      code = getAllSessionWindow(pAggSup->pResultRows, pInfo->pStUpdated);
      QUERY_CHECK_CODE(code, lino, _end);
      continue;
    } else if (pBlock->info.type == STREAM_CREATE_CHILD_TABLE) {
      (*ppRes) = pBlock;
      return code;
    } else if (pBlock->info.type == STREAM_CHECKPOINT) {
      pAggSup->stateStore.streamStateCommit(pAggSup->pState);
      doStreamSessionSaveCheckpoint(pOperator);
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

    doStreamSessionAggImpl(pOperator, pBlock, pInfo->pStUpdated, pInfo->pStDeleted, IS_FINAL_SESSION_OP(pOperator),
                           true);
    if (IS_FINAL_SESSION_OP(pOperator)) {
      int32_t chIndex = getChildIndex(pBlock);
      int32_t size = taosArrayGetSize(pInfo->pChildren);
      // if chIndex + 1 - size > 0, add new child
      for (int32_t i = 0; i < chIndex + 1 - size; i++) {
        SOperatorInfo* pChildOp = NULL;
        code = createStreamFinalSessionAggOperatorInfo(NULL, pInfo->pPhyNode, pOperator->pTaskInfo, 0, NULL, &pChildOp);
        if (pChildOp == NULL || code != 0) {
          qError("%s create stream child of final session error", GET_TASKID(pTaskInfo));
          code = TSDB_CODE_FAILED;
          QUERY_CHECK_CODE(code, lino, _end);
        }

        void* tmp = taosArrayPush(pInfo->pChildren, &pChildOp);
        if (!tmp) {
          code = terrno;
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }

      SOperatorInfo* pChildOp = taosArrayGetP(pInfo->pChildren, chIndex);
      code = setInputDataBlock(&pChildOp->exprSupp, pBlock, TSDB_ORDER_ASC, MAIN_SCAN, true);
      QUERY_CHECK_CODE(code, lino, _end);
      doStreamSessionAggImpl(pChildOp, pBlock, NULL, NULL, true, false);
    }
    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.window.ekey);
    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.watermark);
  }
  // restore the value
  pOperator->status = OP_RES_TO_RETURN;

  code = closeSessionWindow(pAggSup->pResultRows, &pInfo->twAggSup, pInfo->pStUpdated);
  QUERY_CHECK_CODE(code, lino, _end);

  code = closeChildSessionWindow(pInfo->pChildren, pInfo->twAggSup.maxTs);
  QUERY_CHECK_CODE(code, lino, _end);

  code = copyUpdateResult(&pInfo->pStUpdated, pInfo->pUpdated, sessionKeyCompareAsc);
  QUERY_CHECK_CODE(code, lino, _end);

  if (!pInfo->destHasPrimaryKey) {
    removeSessionDeleteResults(pInfo->pStDeleted, pInfo->pUpdated);
  }
  if (pInfo->isHistoryOp) {
    code = getMaxTsWins(pInfo->pUpdated, pInfo->historyWins);
    QUERY_CHECK_CODE(code, lino, _end);
  }
  if (pInfo->destHasPrimaryKey && IS_NORMAL_SESSION_OP(pOperator)) {
    code = copyDeleteSessionKey(pInfo->pPkDeleted, pInfo->pStDeleted);
    QUERY_CHECK_CODE(code, lino, _end);
  }
  initGroupResInfoFromArrayList(&pInfo->groupResInfo, pInfo->pUpdated);
  pInfo->pUpdated = NULL;
  code = blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  SSDataBlock* opRes = NULL;
  code = buildSessionResult(pOperator, &opRes);
  QUERY_CHECK_CODE(code, lino, _end);
  if (opRes) {
    (*ppRes) = opRes;
    return code;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    pTaskInfo->code = code;
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  setStreamOperatorCompleted(pOperator);
  (*ppRes) = NULL;
  return code;
}

static SSDataBlock* doStreamSessionAgg(SOperatorInfo* pOperator) {
  SSDataBlock* pRes = NULL;
  int32_t      code = doStreamSessionAggNext(pOperator, &pRes);
  return pRes;
}

void streamSessionReleaseState(SOperatorInfo* pOperator) {
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  int32_t                        winSize = taosArrayGetSize(pInfo->historyWins) * sizeof(SSessionKey);
  int32_t                        resSize = winSize + sizeof(TSKEY);
  char*                          pBuff = taosMemoryCalloc(1, resSize);
  if (!pBuff) {
    return;
  }
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

int32_t getSessionWindowInfoByKey(SStreamAggSupporter* pAggSup, SSessionKey* pKey, SResultWindowInfo* pWinInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t rowSize = pAggSup->resultRowSize;
  int32_t winCode = TSDB_CODE_SUCCESS;
  code = pAggSup->stateStore.streamStateSessionGet(pAggSup->pState, pKey, (void**)&pWinInfo->pStatePos, &rowSize,
                                                   &winCode);
  QUERY_CHECK_CODE(code, lino, _end);

  if (winCode == TSDB_CODE_SUCCESS) {
    pWinInfo->sessionWin = *pKey;
    pWinInfo->isOutput = true;
    if (pWinInfo->pStatePos->needFree) {
      pAggSup->stateStore.streamStateSessionDel(pAggSup->pState, &pWinInfo->sessionWin);
    }
  } else {
    SET_SESSION_WIN_INVALID((*pWinInfo));
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
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
  int32_t                        code = TSDB_CODE_SUCCESS;
  int32_t                        lino = 0;
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  SStreamAggSupporter*           pAggSup = &pInfo->streamAggSup;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  resetWinRange(&pAggSup->winRange);

  SResultWindowInfo winInfo = {0};
  int32_t           size = 0;
  void*             pBuf = NULL;
  code = pAggSup->stateStore.streamStateGetInfo(pAggSup->pState, STREAM_SESSION_OP_STATE_NAME,
                                                strlen(STREAM_SESSION_OP_STATE_NAME), &pBuf, &size);
  QUERY_CHECK_CODE(code, lino, _end);

  int32_t      num = (size - sizeof(TSKEY)) / sizeof(SSessionKey);
  SSessionKey* pSeKeyBuf = (SSessionKey*)pBuf;
  for (int32_t i = 0; i < num; i++) {
    SResultWindowInfo winInfo = {0};
    code = getSessionWindowInfoByKey(pAggSup, pSeKeyBuf + i, &winInfo);
    QUERY_CHECK_CODE(code, lino, _end);
    if (!IS_VALID_SESSION_WIN(winInfo)) {
      continue;
    }
    compactSessionSemiWindow(pOperator, &winInfo);
    code = saveSessionOutputBuf(pAggSup, &winInfo);
    QUERY_CHECK_CODE(code, lino, _end);
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

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
}

void streamSessionReloadState(SOperatorInfo* pOperator) {
  int32_t                        code = TSDB_CODE_SUCCESS;
  int32_t                        lino = 0;
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  SStreamAggSupporter*           pAggSup = &pInfo->streamAggSup;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  resetWinRange(&pAggSup->winRange);

  int32_t size = 0;
  void*   pBuf = NULL;
  code = pAggSup->stateStore.streamStateGetInfo(pAggSup->pState, STREAM_SESSION_OP_STATE_NAME,
                                                strlen(STREAM_SESSION_OP_STATE_NAME), &pBuf, &size);

  QUERY_CHECK_CODE(code, lino, _end);

  int32_t      num = (size - sizeof(TSKEY)) / sizeof(SSessionKey);
  SSessionKey* pSeKeyBuf = (SSessionKey*)pBuf;

  TSKEY ts = *(TSKEY*)((char*)pBuf + size - sizeof(TSKEY));
  pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, ts);
  pAggSup->stateStore.streamStateReloadInfo(pAggSup->pState, ts);

  if (!pInfo->pStUpdated && num > 0) {
    _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
    pInfo->pStUpdated = tSimpleHashInit(64, hashFn);
    QUERY_CHECK_NULL(pInfo->pStUpdated, code, lino, _end, terrno);
  }
  for (int32_t i = 0; i < num; i++) {
    SResultWindowInfo winInfo = {0};
    code = getSessionWindowInfoByKey(pAggSup, pSeKeyBuf + i, &winInfo);
    QUERY_CHECK_CODE(code, lino, _end);
    if (!IS_VALID_SESSION_WIN(winInfo)) {
      continue;
    }

    int32_t winNum = 0;
    code = compactSessionWindow(pOperator, &winInfo, pInfo->pStUpdated, pInfo->pStDeleted, true, &winNum);
    QUERY_CHECK_CODE(code, lino, _end);

    if (winNum > 0) {
      qDebug("===stream=== reload state. save result %" PRId64 ", %" PRIu64, winInfo.sessionWin.win.skey,
             winInfo.sessionWin.groupId);
      if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE) {
        code = saveResult(winInfo, pInfo->pStUpdated);
        QUERY_CHECK_CODE(code, lino, _end);
      } else if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_WINDOW_CLOSE) {
        if (!isCloseWindow(&winInfo.sessionWin.win, &pInfo->twAggSup)) {
          code = saveDeleteRes(pInfo->pStDeleted, winInfo.sessionWin);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        SSessionKey key = {0};
        getSessionHashKey(&winInfo.sessionWin, &key);
        code = tSimpleHashPut(pAggSup->pResultRows, &key, sizeof(SSessionKey), &winInfo, sizeof(SResultWindowInfo));
        QUERY_CHECK_CODE(code, lino, _end);
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
  reloadAggSupFromDownStream(downstream, &pInfo->streamAggSup);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
}

int32_t createStreamSessionAggOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode,
                                                  SExecTaskInfo* pTaskInfo, SReadHandle* pHandle, SOperatorInfo** pOptrInfo) {
  QRY_OPTR_CHECK(pOptrInfo);

  SSessionWinodwPhysiNode*       pSessionNode = (SSessionWinodwPhysiNode*)pPhyNode;
  int32_t                        numOfCols = 0;
  int32_t                        code = TSDB_CODE_OUT_OF_MEMORY;
  int32_t                        lino = 0;
  SStreamSessionAggOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamSessionAggOperatorInfo));
  SOperatorInfo*                 pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }

  pOperator->pTaskInfo = pTaskInfo;

  initResultSizeInfo(&pOperator->resultInfo, 4096);
  if (pSessionNode->window.pExprs != NULL) {
    int32_t    numOfScalar = 0;
    SExprInfo* pScalarExprInfo = NULL;
    code = createExprInfo(pSessionNode->window.pExprs, NULL, &pScalarExprInfo, &numOfScalar);
    QUERY_CHECK_CODE(code, lino, _error);

    code = initExprSupp(&pInfo->scalarSupp, pScalarExprInfo, numOfScalar, &pTaskInfo->storageAPI.functionStore);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }
  SExprSupp* pExpSup = &pOperator->exprSupp;
  
  SSDataBlock* pResBlock = createDataBlockFromDescNode(pPhyNode->pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pResBlock, code, lino, _error, terrno);
  pInfo->binfo.pRes = pResBlock;

  SExprInfo*   pExprInfo = NULL;
  code = createExprInfo(pSessionNode->window.pFuncs, NULL, &pExprInfo, &numOfCols);
  QUERY_CHECK_CODE(code, lino, _error);

  code = initBasicInfoEx(&pInfo->binfo, pExpSup, pExprInfo, numOfCols, pResBlock, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->twAggSup = (STimeWindowAggSupp){
      .waterMark = pSessionNode->window.watermark,
      .calTrigger = pSessionNode->window.triggerType,
      .maxTs = INT64_MIN,
      .minTs = INT64_MAX,
      .deleteMark = getDeleteMark(&pSessionNode->window, 0),
  };

  pInfo->primaryTsIndex = ((SColumnNode*)pSessionNode->window.pTspk)->slotId;
  code = initStreamAggSupporter(&pInfo->streamAggSup, pExpSup, numOfCols, pSessionNode->gap,
                                pTaskInfo->streamInfo.pState, 0, 0, &pTaskInfo->storageAPI.stateStore, pHandle,
                                &pInfo->twAggSup, GET_TASKID(pTaskInfo), &pTaskInfo->storageAPI, pInfo->primaryTsIndex);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  code = initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);
  QUERY_CHECK_CODE(code, lino, _error);

  if (pSessionNode->window.pTsEnd) {
    pInfo->endTsIndex = ((SColumnNode*)pSessionNode->window.pTsEnd)->slotId;
  }

  pInfo->order = TSDB_ORDER_ASC;
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pInfo->pStDeleted = tSimpleHashInit(64, hashFn);
  QUERY_CHECK_NULL(pInfo->pStDeleted, code, lino, _error, terrno);
  pInfo->pDelIterator = NULL;
  code = createSpecialDataBlock(STREAM_DELETE_RESULT, &pInfo->pDelRes);
  QUERY_CHECK_CODE(code, lino, _error);

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

  code = createSpecialDataBlock(STREAM_CHECKPOINT, &pInfo->pCheckpointRes);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->clearState = false;
  pInfo->recvGetAll = false;
  pInfo->destHasPrimaryKey = pSessionNode->window.destHasPrimayKey;
  pInfo->pPkDeleted = tSimpleHashInit(64, hashFn);
  QUERY_CHECK_NULL(pInfo->pPkDeleted, code, lino, _error, terrno);

  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION;
  setOperatorInfo(pOperator, getStreamOpName(pOperator->operatorType), QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION, true,
                  OP_NOT_OPENED, pInfo, pTaskInfo);
  if (pPhyNode->type != QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION) {
    // for stream
    void*   buff = NULL;
    int32_t len = 0;
    int32_t res =
        pInfo->streamAggSup.stateStore.streamStateGetInfo(pInfo->streamAggSup.pState, STREAM_SESSION_OP_CHECKPOINT_NAME,
                                                          strlen(STREAM_SESSION_OP_CHECKPOINT_NAME), &buff, &len);
    if (res == TSDB_CODE_SUCCESS) {
      code = doStreamSessionDecodeOpState(buff, len, pOperator, true, NULL);
      taosMemoryFree(buff);
      QUERY_CHECK_CODE(code, lino, _error);
    }
  }
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doStreamSessionAggNext, NULL, destroyStreamSessionAggOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorStreamStateFn(pOperator, streamSessionReleaseState, streamSessionReloadState);

  if (downstream) {
    code = initDownStream(downstream, &pInfo->streamAggSup, pOperator->operatorType, pInfo->primaryTsIndex,
                          &pInfo->twAggSup, &pInfo->basic);
    QUERY_CHECK_CODE(code, lino, _error);

    code = appendDownstream(pOperator, &downstream, 1);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  if (pInfo != NULL) {
    destroyStreamSessionAggOperatorInfo(pInfo);
  }
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

static void clearStreamSessionOperator(SStreamSessionAggOperatorInfo* pInfo) {
  tSimpleHashClear(pInfo->streamAggSup.pResultRows);
  pInfo->streamAggSup.stateStore.streamStateSessionClear(pInfo->streamAggSup.pState);
  pInfo->clearState = false;
}

int32_t deleteSessionWinState(SStreamAggSupporter* pAggSup, SSDataBlock* pBlock, SSHashObj* pMapUpdate,
                              SSHashObj* pMapDelete, SSHashObj* pPkDelete, bool needAdd) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SArray* pWins = taosArrayInit(16, sizeof(SSessionKey));
  if (!pWins) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    QUERY_CHECK_CODE(code, lino, _end);
  }
  code = doDeleteTimeWindows(pAggSup, pBlock, pWins);
  QUERY_CHECK_CODE(code, lino, _end);

  removeSessionResults(pAggSup, pMapUpdate, pWins);
  code = copyDeleteWindowInfo(pWins, pMapDelete);
  QUERY_CHECK_CODE(code, lino, _end);

  if (needAdd) {
    code = copyDeleteWindowInfo(pWins, pPkDelete);
    QUERY_CHECK_CODE(code, lino, _end);
  }
  taosArrayDestroy(pWins);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doStreamSessionSemiAggNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                        code = TSDB_CODE_SUCCESS;
  int32_t                        lino = 0;
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  SOptrBasicInfo*                pBInfo = &pInfo->binfo;
  TSKEY                          maxTs = INT64_MIN;
  SExprSupp*                     pSup = &pOperator->exprSupp;
  SStreamAggSupporter*           pAggSup = &pInfo->streamAggSup;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;

  qDebug("stask:%s  %s status: %d", GET_TASKID(pTaskInfo), getStreamOpName(pOperator->operatorType), pOperator->status);
  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return code;
  }

  {
    SSDataBlock* opRes = NULL;
    code = buildSessionResult(pOperator, &opRes);
    QUERY_CHECK_CODE(code, lino, _end);
    if (opRes) {
      (*ppRes) = opRes;
      return code;
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
        (*ppRes) = pInfo->pCheckpointRes;
        return code;
      }
      clearFunctionContext(&pOperator->exprSupp);
      // semi session operator clear disk buffer
      clearStreamSessionOperator(pInfo);
      setStreamOperatorCompleted(pOperator);
      (*ppRes) = NULL;
      return code;
    }
  }

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (!pInfo->pUpdated) {
    pInfo->pUpdated = taosArrayInit(16, sizeof(SResultWindowInfo));
    QUERY_CHECK_NULL(pInfo->pUpdated, code, lino, _end, terrno);
  }
  if (!pInfo->pStUpdated) {
    _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
    pInfo->pStUpdated = tSimpleHashInit(64, hashFn);
    QUERY_CHECK_NULL(pInfo->pStUpdated, code, lino, _end, terrno);
  }
  while (1) {
    SSDataBlock* pBlock = NULL;
    code = downstream->fpSet.getNextFn(downstream, &pBlock);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pBlock == NULL) {
      pOperator->status = OP_RES_TO_RETURN;
      break;
    }
    printSpecDataBlock(pBlock, getStreamOpName(pOperator->operatorType), "recv", GET_TASKID(pTaskInfo));
    setStreamOperatorState(&pInfo->basic, pBlock->info.type);

    if (pBlock->info.type == STREAM_DELETE_DATA || pBlock->info.type == STREAM_DELETE_RESULT ||
        pBlock->info.type == STREAM_CLEAR) {
      // gap must be 0
      code = deleteSessionWinState(pAggSup, pBlock, pInfo->pStUpdated, pInfo->pStDeleted, NULL, false);
      QUERY_CHECK_CODE(code, lino, _end);
      pInfo->clearState = true;
      break;
    } else if (pBlock->info.type == STREAM_GET_ALL) {
      code = getAllSessionWindow(pInfo->streamAggSup.pResultRows, pInfo->pStUpdated);
      QUERY_CHECK_CODE(code, lino, _end);
      continue;
    } else if (pBlock->info.type == STREAM_CREATE_CHILD_TABLE) {
      (*ppRes) = pBlock;
      return code;
    } else if (pBlock->info.type == STREAM_CHECKPOINT) {
      pAggSup->stateStore.streamStateCommit(pAggSup->pState);
      doStreamSessionSaveCheckpoint(pOperator);
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
    doStreamSessionAggImpl(pOperator, pBlock, pInfo->pStUpdated, NULL, false, false);
    maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.window.ekey);
  }

  pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, maxTs);
  pBInfo->pRes->info.watermark = pInfo->twAggSup.maxTs;

  code = copyUpdateResult(&pInfo->pStUpdated, pInfo->pUpdated, sessionKeyCompareAsc);
  QUERY_CHECK_CODE(code, lino, _end);

  removeSessionDeleteResults(pInfo->pStDeleted, pInfo->pUpdated);

  if (pInfo->isHistoryOp) {
    code = getMaxTsWins(pInfo->pUpdated, pInfo->historyWins);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  initGroupResInfoFromArrayList(&pInfo->groupResInfo, pInfo->pUpdated);
  pInfo->pUpdated = NULL;
  code = blockDataEnsureCapacity(pBInfo->pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  SSDataBlock* opRes = NULL;
  code = buildSessionResult(pOperator, &opRes);
  QUERY_CHECK_CODE(code, lino, _end);
  if (opRes) {
    (*ppRes) = opRes;
    return code;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    pTaskInfo->code = code;
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }

  clearFunctionContext(&pOperator->exprSupp);
  // semi session operator clear disk buffer
  clearStreamSessionOperator(pInfo);
  setStreamOperatorCompleted(pOperator);
  (*ppRes) = NULL;
  return code;
}

static SSDataBlock* doStreamSessionSemiAgg(SOperatorInfo* pOperator) {
  SSDataBlock* pRes = NULL;
  int32_t      code = doStreamSessionSemiAggNext(pOperator, &pRes);
  return pRes;
}

int32_t createStreamFinalSessionAggOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode,
                                                       SExecTaskInfo* pTaskInfo, int32_t numOfChild,
                                                       SReadHandle* pHandle, SOperatorInfo** pOptrInfo) {
  QRY_OPTR_CHECK(pOptrInfo);

  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SOperatorInfo* pOperator = NULL;
  code = createStreamSessionAggOperatorInfo(downstream, pPhyNode, pTaskInfo, pHandle, &pOperator);
  if (pOperator == NULL || code != 0) {
    downstream =  NULL;
    QUERY_CHECK_CODE(code, lino, _error);
  }

  SStorageAPI*                   pAPI = &pTaskInfo->storageAPI;
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  pOperator->operatorType = pPhyNode->type;

  if (pPhyNode->type != QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION) {
    pOperator->fpSet =
        createOperatorFpSet(optrDummyOpenFn, doStreamSessionSemiAggNext, NULL, destroyStreamSessionAggOperatorInfo,
                            optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
    setOperatorStreamStateFn(pOperator, streamSessionReleaseState, streamSessionSemiReloadState);
  }
  setOperatorInfo(pOperator, getStreamOpName(pOperator->operatorType), pPhyNode->type, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);

  if (numOfChild > 0) {
    pInfo->pChildren = taosArrayInit(numOfChild, sizeof(void*));
    QUERY_CHECK_NULL(pInfo->pChildren, code, lino, _error, terrno);
    for (int32_t i = 0; i < numOfChild; i++) {
      SOperatorInfo* pChildOp = NULL;
      code = createStreamFinalSessionAggOperatorInfo(NULL, pPhyNode, pTaskInfo, 0, pHandle, &pChildOp);
      if (pChildOp == NULL || code != 0) {
        QUERY_CHECK_CODE(code, lino, _error);
      }

      SStreamSessionAggOperatorInfo* pChInfo = pChildOp->info;
      pChInfo->twAggSup.calTrigger = STREAM_TRIGGER_AT_ONCE;
      pAPI->stateStore.streamStateSetNumber(pChInfo->streamAggSup.pState, i, pInfo->primaryTsIndex);
      void* tmp = taosArrayPush(pInfo->pChildren, &pChildOp);
      if (!tmp) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        QUERY_CHECK_CODE(code, lino, _error);
      }
    }

    void*   buff = NULL;
    int32_t len = 0;
    int32_t res =
        pInfo->streamAggSup.stateStore.streamStateGetInfo(pInfo->streamAggSup.pState, STREAM_SESSION_OP_CHECKPOINT_NAME,
                                                          strlen(STREAM_SESSION_OP_CHECKPOINT_NAME), &buff, &len);
    if (res == TSDB_CODE_SUCCESS) {
      code = doStreamSessionDecodeOpState(buff, len, pOperator, true, NULL);
      taosMemoryFree(buff);
      QUERY_CHECK_CODE(code, lino, _error);
    }
  }

  if (!IS_FINAL_SESSION_OP(pOperator) || numOfChild == 0) {
    pInfo->twAggSup.calTrigger = STREAM_TRIGGER_AT_ONCE;
  }

  *pOptrInfo = pOperator;
  return code;

_error:
  if (pInfo != NULL) {
    destroyStreamSessionAggOperatorInfo(pInfo);
  }
  if (pOperator != NULL) {
    pOperator->info = NULL;
    destroyOperator(pOperator);
  }
  pTaskInfo->code = code;
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

void destroyStreamStateOperatorInfo(void* param) {
  if (param == NULL) {
    return;
  }
  SStreamStateAggOperatorInfo* pInfo = (SStreamStateAggOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);
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
  tSimpleHashCleanup(pInfo->pSeDeleted);
  cleanupGroupResInfo(&pInfo->groupResInfo);

  taosArrayDestroy(pInfo->historyWins);
  blockDataDestroy(pInfo->pCheckpointRes);
  tSimpleHashCleanup(pInfo->pPkDeleted);

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

int32_t getStateWindowInfoByKey(SStreamAggSupporter* pAggSup, SSessionKey* pKey, SStateWindowInfo* pCurWin,
                                SStateWindowInfo* pNextWin) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SStreamStateCur* pCur = NULL;
  int32_t          size = pAggSup->resultRowSize;
  pCurWin->winInfo.sessionWin.groupId = pKey->groupId;
  pCurWin->winInfo.sessionWin.win.skey = pKey->win.skey;
  pCurWin->winInfo.sessionWin.win.ekey = pKey->win.ekey;
  code = getSessionWindowInfoByKey(pAggSup, pKey, &pCurWin->winInfo);
  QUERY_CHECK_CODE(code, lino, _end);
  QUERY_CHECK_CONDITION((IS_VALID_SESSION_WIN(pCurWin->winInfo)), code, lino, _end,
                        TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);

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
  pCur = pAggSup->stateStore.streamStateSessionSeekKeyNext(pAggSup->pState, &pNextWin->winInfo.sessionWin);
  int32_t nextSize = pAggSup->resultRowSize;
  int32_t winCode = pAggSup->stateStore.streamStateSessionGetKVByCur(pCur, &pNextWin->winInfo.sessionWin,
                                                                     (void**)&pNextWin->winInfo.pStatePos, &nextSize);
  if (winCode != TSDB_CODE_SUCCESS) {
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

_end:
  pAggSup->stateStore.streamStateFreeCur(pCur);
  qDebug("===stream===get state next win buff. skey:%" PRId64 ", endkey:%" PRId64,
         pNextWin->winInfo.sessionWin.win.skey, pNextWin->winInfo.sessionWin.win.ekey);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t setStateOutputBuf(SStreamAggSupporter* pAggSup, TSKEY ts, uint64_t groupId, char* pKeyData,
                          SStateWindowInfo* pCurWin, SStateWindowInfo* pNextWin) {
  int32_t          size = pAggSup->resultRowSize;
  SStreamStateCur* pCur = NULL;
  pCurWin->winInfo.sessionWin.groupId = groupId;
  pCurWin->winInfo.sessionWin.win.skey = ts;
  pCurWin->winInfo.sessionWin.win.ekey = ts;
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t winCode = TSDB_CODE_SUCCESS;
  code = pAggSup->stateStore.streamStateStateAddIfNotExist(pAggSup->pState, &pCurWin->winInfo.sessionWin, pKeyData,
                                                           pAggSup->stateKeySize, compareStateKey,
                                                           (void**)&pCurWin->winInfo.pStatePos, &size, &winCode);
  QUERY_CHECK_CODE(code, lino, _end);

  pCurWin->pStateKey =
      (SStateKeys*)((char*)pCurWin->winInfo.pStatePos->pRowBuff + (pAggSup->resultRowSize - pAggSup->stateKeySize));
  pCurWin->pStateKey->bytes = pAggSup->stateKeySize - sizeof(SStateKeys);
  pCurWin->pStateKey->type = pAggSup->stateKeyType;
  pCurWin->pStateKey->pData = (char*)pCurWin->pStateKey + sizeof(SStateKeys);
  pCurWin->pStateKey->isNull = false;

  if (winCode == TSDB_CODE_SUCCESS && !inWinRange(&pAggSup->winRange, &pCurWin->winInfo.sessionWin.win)) {
    winCode = TSDB_CODE_FAILED;
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

  if (winCode == TSDB_CODE_SUCCESS) {
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
  pCur = pAggSup->stateStore.streamStateSessionSeekKeyNext(pAggSup->pState, &pNextWin->winInfo.sessionWin);
  int32_t nextSize = pAggSup->resultRowSize;
  winCode = pAggSup->stateStore.streamStateSessionGetKVByCur(pCur, &pNextWin->winInfo.sessionWin,
                                                             (void**)&pNextWin->winInfo.pStatePos, &nextSize);
  if (winCode != TSDB_CODE_SUCCESS) {
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
  qDebug("===stream===set state next win buff. skey:%" PRId64 ", endkey:%" PRId64,
         pNextWin->winInfo.sessionWin.win.skey, pNextWin->winInfo.sessionWin.win.ekey);
_end:
  pAggSup->stateStore.streamStateFreeCur(pCur);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t updateStateWindowInfo(SStreamAggSupporter* pAggSup, SStateWindowInfo* pWinInfo, SStateWindowInfo* pNextWin,
                              TSKEY* pTs, uint64_t groupId, SColumnInfoData* pKeyCol, int32_t rows, int32_t start,
                              bool* allEqual, SSHashObj* pResultRows, SSHashObj* pSeUpdated, SSHashObj* pSeDeleted,
                              int32_t* pWinRows) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  *allEqual = true;
  for (int32_t i = start; i < rows; ++i) {
    char* pKeyData = colDataGetData(pKeyCol, i);
    if (!isTsInWindow(pWinInfo, pTs[i])) {
      if (isEqualStateKey(pWinInfo, pKeyData)) {
        if (IS_VALID_SESSION_WIN(pNextWin->winInfo)) {
          // ts belongs to the next window
          if (pTs[i] >= pNextWin->winInfo.sessionWin.win.skey) {
            (*pWinRows) = i - start;
            goto _end;
          }
        }
      } else {
        (*pWinRows) = i - start;
        goto _end;
      }
    }

    if (pWinInfo->winInfo.sessionWin.win.skey > pTs[i]) {
      if (pSeDeleted && pWinInfo->winInfo.isOutput) {
        code = saveDeleteRes(pSeDeleted, pWinInfo->winInfo.sessionWin);
        QUERY_CHECK_CODE(code, lino, _end);
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
  (*pWinRows) = rows - start;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void doStreamStateAggImpl(SOperatorInfo* pOperator, SSDataBlock* pSDataBlock, SSHashObj* pSeUpdated,
                                 SSHashObj* pStDeleted) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*   pAPI = &pOperator->pTaskInfo->storageAPI;

  SStreamStateAggOperatorInfo* pInfo = pOperator->info;
  int32_t                      numOfOutput = pOperator->exprSupp.numOfExprs;
  uint64_t                     groupId = pSDataBlock->info.id.groupId;
  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      lino = 0;
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
    if (!pColDataInfo) {
      code = TSDB_CODE_FAILED;
      QUERY_CHECK_CODE(code, lino, _end);
    }
    tsCols = (int64_t*)pColDataInfo->pData;
  } else {
    return;
  }

  int32_t rows = pSDataBlock->info.rows;
  code = blockDataEnsureCapacity(pAggSup->pScanBlock, rows);
  QUERY_CHECK_CODE(code, lino, _end);

  SColumnInfoData* pKeyColInfo = taosArrayGet(pSDataBlock->pDataBlock, pInfo->stateCol.slotId);
  for (int32_t i = 0; i < rows; i += winRows) {
    if (pInfo->ignoreExpiredData && checkExpiredData(&pInfo->streamAggSup.stateStore, pInfo->streamAggSup.pUpdateInfo,
                                                     &pInfo->twAggSup, pSDataBlock->info.id.uid, tsCols[i], NULL, 0) ||
        colDataIsNull_s(pKeyColInfo, i)) {
      i++;
      continue;
    }
    char*            pKeyData = colDataGetData(pKeyColInfo, i);
    int32_t          winIndex = 0;
    bool             allEqual = true;
    SStateWindowInfo curWin = {0};
    SStateWindowInfo nextWin = {0};
    code = setStateOutputBuf(pAggSup, tsCols[i], groupId, pKeyData, &curWin, &nextWin);
    QUERY_CHECK_CODE(code, lino, _end);

    releaseOutputBuf(pAggSup->pState, nextWin.winInfo.pStatePos, &pAPI->stateStore);

    setSessionWinOutputInfo(pSeUpdated, &curWin.winInfo);
    code = updateStateWindowInfo(pAggSup, &curWin, &nextWin, tsCols, groupId, pKeyColInfo, rows, i, &allEqual,
                                 pAggSup->pResultRows, pSeUpdated, pStDeleted, &winRows);
    QUERY_CHECK_CODE(code, lino, _end);

    if (!allEqual) {
      uint64_t uid = 0;
      code = appendDataToSpecialBlock(pAggSup->pScanBlock, &curWin.winInfo.sessionWin.win.skey,
                                      &curWin.winInfo.sessionWin.win.ekey, &uid, &groupId, NULL);
      QUERY_CHECK_CODE(code, lino, _end);
      int32_t tmpRes = tSimpleHashRemove(pSeUpdated, &curWin.winInfo.sessionWin, sizeof(SSessionKey));
      qTrace("%s at line %d res:%d", __func__, __LINE__, tmpRes);

      doDeleteSessionWindow(pAggSup, &curWin.winInfo.sessionWin);
      releaseOutputBuf(pAggSup->pState, curWin.winInfo.pStatePos, &pAPI->stateStore);
      continue;
    }

    code = doOneWindowAggImpl(&pInfo->twAggSup.timeWindowData, &curWin.winInfo, &pResult, i, winRows, rows, numOfOutput,
                              pOperator, 0);
    QUERY_CHECK_CODE(code, lino, _end);

    code = saveSessionOutputBuf(pAggSup, &curWin.winInfo);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pInfo->destHasPrimaryKey && curWin.winInfo.isOutput && IS_NORMAL_STATE_OP(pOperator)) {
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
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
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
  tlen += taosEncodeFixedI64(buf, pInfo->dataVersion);

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

int32_t doStreamStateDecodeOpState(void* buf, int32_t len, SOperatorInfo* pOperator, bool isParent, void** ppBuf) {
  SStreamStateAggOperatorInfo* pInfo = pOperator->info;
  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      lino = 0;
  SStreamAggSupporter*         pAggSup = &pInfo->streamAggSup;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  if (!pInfo) {
    code = TSDB_CODE_FAILED;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  // 5.checksum
  if (isParent) {
    int32_t dataLen = len - sizeof(uint32_t);
    void*   pCksum = POINTER_SHIFT(buf, dataLen);
    if (taosCheckChecksum(buf, dataLen, *(uint32_t*)pCksum) != TSDB_CODE_SUCCESS) {
      qError("stream state_window state is invalid");
      code = TSDB_CODE_FAILED;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  // 1.streamAggSup.pResultRows
  int32_t mapSize = 0;
  buf = taosDecodeFixedI32(buf, &mapSize);
  for (int32_t i = 0; i < mapSize; i++) {
    SSessionKey       key = {0};
    SResultWindowInfo winfo = {0};
    buf = decodeSSessionKey(buf, &key);
    int32_t winCode = TSDB_CODE_SUCCESS;
    code = pAggSup->stateStore.streamStateStateAddIfNotExist(
        pAggSup->pState, &winfo.sessionWin, NULL, pAggSup->stateKeySize, compareStateKey, (void**)&winfo.pStatePos,
        &pAggSup->resultRowSize, &winCode);
    QUERY_CHECK_CODE(code, lino, _end);

    buf = decodeSResultWindowInfo(buf, &winfo, pInfo->streamAggSup.resultRowSize);
    code =
        tSimpleHashPut(pInfo->streamAggSup.pResultRows, &key, sizeof(SSessionKey), &winfo, sizeof(SResultWindowInfo));
    QUERY_CHECK_CODE(code, lino, _end);
  }

  // 2.twAggSup
  buf = decodeSTimeWindowAggSupp(buf, &pInfo->twAggSup);

  // 3.pChildren
  int32_t size = 0;
  buf = taosDecodeFixedI32(buf, &size);
  for (int32_t i = 0; i < size; i++) {
    SOperatorInfo* pChOp = taosArrayGetP(pInfo->pChildren, i);
    code = doStreamStateDecodeOpState(buf, 0, pChOp, false, &buf);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  // 4.dataVersion
  buf = taosDecodeFixedI64(buf, &pInfo->dataVersion);

  if (ppBuf) {
    (*ppBuf) = buf;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

void doStreamStateSaveCheckpoint(SOperatorInfo* pOperator) {
  SStreamStateAggOperatorInfo* pInfo = pOperator->info;
  if (needSaveStreamOperatorInfo(&pInfo->basic)) {
    int32_t len = doStreamStateEncodeOpState(NULL, 0, pOperator, true);
    void*   buf = taosMemoryCalloc(1, len);
    if (!buf) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(TSDB_CODE_OUT_OF_MEMORY));
      return;
    }
    void* pBuf = buf;
    len = doStreamStateEncodeOpState(&pBuf, len, pOperator, true);
    pInfo->streamAggSup.stateStore.streamStateSaveInfo(pInfo->streamAggSup.pState, STREAM_STATE_OP_CHECKPOINT_NAME,
                                                       strlen(STREAM_STATE_OP_CHECKPOINT_NAME), buf, len);
    taosMemoryFree(buf);
    saveStreamOperatorStateComplete(&pInfo->basic);
  }
}

static int32_t buildStateResult(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                      code = TSDB_CODE_SUCCESS;
  SStreamStateAggOperatorInfo* pInfo = pOperator->info;
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

static int32_t doStreamStateAggNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return TSDB_CODE_SUCCESS;
  }

  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      lino = 0;
  SExprSupp*                   pSup = &pOperator->exprSupp;
  SStreamStateAggOperatorInfo* pInfo = pOperator->info;
  SOptrBasicInfo*              pBInfo = &pInfo->binfo;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  qDebug("===stream=== stream state agg");
  if (pOperator->status == OP_RES_TO_RETURN) {
    SSDataBlock* resBlock = NULL;
    code = buildStateResult(pOperator, &resBlock);
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
    pInfo->pUpdated = taosArrayInit(16, sizeof(SResultWindowInfo));
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
      bool add = pInfo->destHasPrimaryKey && IS_NORMAL_STATE_OP(pOperator);
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
      doStreamStateSaveCheckpoint(pOperator);
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
    doStreamStateAggImpl(pOperator, pBlock, pInfo->pSeUpdated, pInfo->pSeDeleted);
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
    code = getMaxTsWins(pInfo->pUpdated, pInfo->historyWins);
    QUERY_CHECK_CODE(code, lino, _end);
  }
  if (pInfo->destHasPrimaryKey && IS_NORMAL_STATE_OP(pOperator)) {
    code = copyDeleteSessionKey(pInfo->pPkDeleted, pInfo->pSeDeleted);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  initGroupResInfoFromArrayList(&pInfo->groupResInfo, pInfo->pUpdated);
  pInfo->pUpdated = NULL;
  code = blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  SSDataBlock* resBlock = NULL;
  code = buildStateResult(pOperator, &resBlock);
  QUERY_CHECK_CODE(code, lino, _end);
  if (resBlock != NULL) {
    (*ppRes) = resBlock;
    return code;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    pTaskInfo->code = code;
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  setStreamOperatorCompleted(pOperator);
  (*ppRes) = NULL;
  return code;
}

static SSDataBlock* doStreamStateAgg(SOperatorInfo* pOperator) {
  SSDataBlock* pRes = NULL;
  int32_t      code = doStreamStateAggNext(pOperator, &pRes);
  return pRes;
}

void streamStateReleaseState(SOperatorInfo* pOperator) {
  SStreamStateAggOperatorInfo* pInfo = pOperator->info;
  int32_t                      winSize = taosArrayGetSize(pInfo->historyWins) * sizeof(SSessionKey);
  int32_t                      resSize = winSize + sizeof(TSKEY);
  char*                        pBuff = taosMemoryCalloc(1, resSize);
  if (!pBuff) {
    return ;
  }
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

static int32_t compactStateWindow(SOperatorInfo* pOperator, SResultWindowInfo* pCurWin, SResultWindowInfo* pNextWin,
                                  SSHashObj* pStUpdated, SSHashObj* pStDeleted) {
  SExprSupp*                   pSup = &pOperator->exprSupp;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  SStreamStateAggOperatorInfo* pInfo = pOperator->info;
  return compactTimeWindow(pSup, &pInfo->streamAggSup, &pInfo->twAggSup, pTaskInfo, pCurWin, pNextWin, pStUpdated,
                           pStDeleted, false);
}

void streamStateReloadState(SOperatorInfo* pOperator) {
  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      lino = 0;
  SStreamStateAggOperatorInfo* pInfo = pOperator->info;
  SStreamAggSupporter*         pAggSup = &pInfo->streamAggSup;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  resetWinRange(&pAggSup->winRange);

  SSessionKey seKey = {.win.skey = INT64_MIN, .win.ekey = INT64_MIN, .groupId = 0};
  int32_t     size = 0;
  void*       pBuf = NULL;
  code = pAggSup->stateStore.streamStateGetInfo(pAggSup->pState, STREAM_STATE_OP_STATE_NAME,
                                                strlen(STREAM_STATE_OP_STATE_NAME), &pBuf, &size);
  QUERY_CHECK_CODE(code, lino, _end);

  int32_t num = (size - sizeof(TSKEY)) / sizeof(SSessionKey);
  qDebug("===stream=== reload state. get result count:%d", num);
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
    SStateWindowInfo curInfo = {0};
    SStateWindowInfo nextInfo = {0};
    qDebug("===stream=== reload state. try process result %" PRId64 ", %" PRIu64 ", index:%d", pSeKeyBuf[i].win.skey,
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
        code = tSimpleHashPut(pAggSup->pResultRows, &key, sizeof(SSessionKey), &curInfo.winInfo,
                              sizeof(SResultWindowInfo));
        QUERY_CHECK_CODE(code, lino, _end);
      }
    } else if (IS_VALID_SESSION_WIN(nextInfo.winInfo)) {
      releaseOutputBuf(pAggSup->pState, nextInfo.winInfo.pStatePos, &pAggSup->pSessionAPI->stateStore);
    }

    if (IS_VALID_SESSION_WIN(curInfo.winInfo)) {
      code = saveSessionOutputBuf(pAggSup, &curInfo.winInfo);
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

int32_t createStreamStateAggOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo,
                                         SReadHandle* pHandle, SOperatorInfo** pOptrInfo) {
  QRY_OPTR_CHECK(pOptrInfo);
  int32_t code = 0;
  int32_t lino = 0;

  SStreamStateWinodwPhysiNode* pStateNode = (SStreamStateWinodwPhysiNode*)pPhyNode;
  int32_t                      tsSlotId = ((SColumnNode*)pStateNode->window.pTspk)->slotId;
  SColumnNode*                 pColNode = (SColumnNode*)(pStateNode->pStateKey);
  SStreamStateAggOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamStateAggOperatorInfo));
  SOperatorInfo*               pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _error);
  }

  pInfo->stateCol = extractColumnFromColumnNode(pColNode);
  initResultSizeInfo(&pOperator->resultInfo, 4096);
  if (pStateNode->window.pExprs != NULL) {
    int32_t    numOfScalar = 0;
    SExprInfo* pScalarExprInfo = NULL;
    code = createExprInfo(pStateNode->window.pExprs, NULL, &pScalarExprInfo, &numOfScalar);
    QUERY_CHECK_CODE(code, lino, _error);

    code = initExprSupp(&pInfo->scalarSupp, pScalarExprInfo, numOfScalar, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  pInfo->twAggSup = (STimeWindowAggSupp){
      .waterMark = pStateNode->window.watermark,
      .calTrigger = pStateNode->window.triggerType,
      .maxTs = INT64_MIN,
      .minTs = INT64_MAX,
      .deleteMark = getDeleteMark(&pStateNode->window, 0),
  };

  code = initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);
  QUERY_CHECK_CODE(code, lino, _error);

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pPhyNode->pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pResBlock, code, lino, _error, terrno);
  pInfo->binfo.pRes = pResBlock;

  SExprSupp*   pExpSup = &pOperator->exprSupp;
  int32_t      numOfCols = 0;
  SExprInfo*   pExprInfo = NULL;
  code = createExprInfo(pStateNode->window.pFuncs, NULL, &pExprInfo, &numOfCols);
  QUERY_CHECK_CODE(code, lino, _error);

  code = initBasicInfoEx(&pInfo->binfo, pExpSup, pExprInfo, numOfCols, pResBlock, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  int32_t keySize = sizeof(SStateKeys) + pColNode->node.resType.bytes;
  int16_t type = pColNode->node.resType.type;
  pInfo->primaryTsIndex = tsSlotId;
  code = initStreamAggSupporter(&pInfo->streamAggSup, pExpSup, numOfCols, 0, pTaskInfo->streamInfo.pState, keySize,
                                type, &pTaskInfo->storageAPI.stateStore, pHandle, &pInfo->twAggSup,
                                GET_TASKID(pTaskInfo), &pTaskInfo->storageAPI, pInfo->primaryTsIndex);
  QUERY_CHECK_CODE(code, lino, _error);

  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pInfo->pSeDeleted = tSimpleHashInit(64, hashFn);
  QUERY_CHECK_NULL(pInfo->pSeDeleted, code, lino, _error, terrno);
  pInfo->pDelIterator = NULL;

  code = createSpecialDataBlock(STREAM_DELETE_RESULT, &pInfo->pDelRes);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->pChildren = NULL;
  pInfo->ignoreExpiredData = pStateNode->window.igExpired;
  pInfo->ignoreExpiredDataSaved = false;
  pInfo->pUpdated = NULL;
  pInfo->pSeUpdated = NULL;
  pInfo->dataVersion = 0;
  pInfo->historyWins = taosArrayInit(4, sizeof(SSessionKey));
  if (!pInfo->historyWins) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _error);
  }

  if (pHandle) {
    pInfo->isHistoryOp = pHandle->fillHistory;
  }

  code = createSpecialDataBlock(STREAM_CHECKPOINT, &pInfo->pCheckpointRes);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->recvGetAll = false;
  pInfo->pPkDeleted = tSimpleHashInit(64, hashFn);
  QUERY_CHECK_NULL(pInfo->pPkDeleted, code, lino, _error, terrno);
  pInfo->destHasPrimaryKey = pStateNode->window.destHasPrimayKey;

  setOperatorInfo(pOperator, "StreamStateAggOperator", QUERY_NODE_PHYSICAL_PLAN_STREAM_STATE, true, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  // for stream
  void*   buff = NULL;
  int32_t len = 0;
  int32_t res =
      pInfo->streamAggSup.stateStore.streamStateGetInfo(pInfo->streamAggSup.pState, STREAM_STATE_OP_CHECKPOINT_NAME,
                                                        strlen(STREAM_STATE_OP_CHECKPOINT_NAME), &buff, &len);
  if (res == TSDB_CODE_SUCCESS) {
    code = doStreamStateDecodeOpState(buff, len, pOperator, true, NULL);
    taosMemoryFree(buff);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doStreamStateAggNext, NULL, destroyStreamStateOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorStreamStateFn(pOperator, streamStateReleaseState, streamStateReloadState);
  code = initDownStream(downstream, &pInfo->streamAggSup, pOperator->operatorType, pInfo->primaryTsIndex,
                        &pInfo->twAggSup, &pInfo->basic);
  QUERY_CHECK_CODE(code, lino, _error);

  code = appendDownstream(pOperator, &downstream, 1);
  QUERY_CHECK_CODE(code, lino, _error);

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  if (pInfo != NULL) destroyStreamStateOperatorInfo(pInfo);
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
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

static int32_t doStreamIntervalAggNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      lino = 0;
  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*                 pAPI = &pOperator->pTaskInfo->storageAPI;
  SExprSupp*                   pSup = &pOperator->exprSupp;

  qDebug("stask:%s  %s status: %d", GET_TASKID(pTaskInfo), getStreamOpName(pOperator->operatorType), pOperator->status);

  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return code;
  }

  if (pOperator->status == OP_RES_TO_RETURN) {
    SSDataBlock* resBlock = NULL;
    code = buildIntervalResult(pOperator, &resBlock);
    QUERY_CHECK_CODE(code, lino, _end);
    if (resBlock != NULL) {
      (*ppRes) = resBlock;
      return code;
    }

    if (pInfo->recvGetAll) {
      pInfo->recvGetAll = false;
      resetUnCloseWinInfo(pInfo->aggSup.pResultRowHashTable);
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
    pInfo->pUpdated = taosArrayInit(4096, POINTER_BYTES);
    QUERY_CHECK_NULL(pInfo->pUpdated, code, lino, _end, terrno);
  }

  if (!pInfo->pUpdatedMap) {
    _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
    pInfo->pUpdatedMap = tSimpleHashInit(4096, hashFn);
    QUERY_CHECK_NULL(pInfo->pUpdatedMap, code, lino, _end, terrno);
  }

  while (1) {
    SSDataBlock* pBlock = NULL;
    code = downstream->fpSet.getNextFn(downstream, &pBlock);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pBlock == NULL) {
      qDebug("===stream===return data:%s. recv datablock num:%" PRIu64, getStreamOpName(pOperator->operatorType),
             pInfo->numOfDatapack);
      pInfo->numOfDatapack = 0;
      break;
    }

    pInfo->numOfDatapack++;
    printSpecDataBlock(pBlock, getStreamOpName(pOperator->operatorType), "recv", GET_TASKID(pTaskInfo));
    setStreamOperatorState(&pInfo->basic, pBlock->info.type);

    if (pBlock->info.type == STREAM_DELETE_DATA || pBlock->info.type == STREAM_DELETE_RESULT ||
        pBlock->info.type == STREAM_CLEAR) {
      code = doDeleteWindows(pOperator, &pInfo->interval, pBlock, pInfo->pDelWins, pInfo->pUpdatedMap, NULL);
      QUERY_CHECK_CODE(code, lino, _end);
      continue;
    } else if (pBlock->info.type == STREAM_GET_ALL) {
      pInfo->recvGetAll = true;
      code = getAllIntervalWindow(pInfo->aggSup.pResultRowHashTable, pInfo->pUpdatedMap);
      QUERY_CHECK_CODE(code, lino, _end);
      continue;
    } else if (pBlock->info.type == STREAM_CREATE_CHILD_TABLE) {
      printDataBlock(pBlock, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
      (*ppRes) = pBlock;
      return code;
    } else if (pBlock->info.type == STREAM_CHECKPOINT) {
      pAPI->stateStore.streamStateCommit(pInfo->pState);
      doStreamIntervalSaveCheckpoint(pOperator);
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

    if (pBlock->info.type == STREAM_NORMAL && pBlock->info.version != 0) {
      // set input version
      pTaskInfo->version = pBlock->info.version;
    }

    if (pInfo->scalarSupp.pExprInfo != NULL) {
      SExprSupp* pExprSup = &pInfo->scalarSupp;
      code = projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    // The timewindow that overlaps the timestamps of the input pBlock need to be recalculated and return to the
    // caller. Note that all the time window are not close till now.
    // the pDataBlock are always the same one, no need to call this again
    code = setInputDataBlock(pSup, pBlock, TSDB_ORDER_ASC, MAIN_SCAN, true);
    QUERY_CHECK_CODE(code, lino, _end);
#ifdef BUILD_NO_CALL
    if (pInfo->invertible) {
      setInverFunction(pSup->pCtx, pOperator->exprSupp.numOfExprs, pBlock->info.type);
    }
#endif

    code = doStreamIntervalAggImpl(pOperator, pBlock, pBlock->info.id.groupId, pInfo->pUpdatedMap, pInfo->pDeletedMap);
    if (code == TSDB_CODE_STREAM_INTERNAL_ERROR) {
      pOperator->status = OP_RES_TO_RETURN;
      code = TSDB_CODE_SUCCESS;
      break;
    }
    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.window.ekey);
    pInfo->twAggSup.minTs = TMIN(pInfo->twAggSup.minTs, pBlock->info.window.skey);
  }
  pOperator->status = OP_RES_TO_RETURN;
  if (!pInfo->destHasPrimaryKey) {
    removeDeleteResults(pInfo->pUpdatedMap, pInfo->pDelWins);
  }
  code = closeStreamIntervalWindow(pInfo->aggSup.pResultRowHashTable, &pInfo->twAggSup, &pInfo->interval, NULL,
                                   pInfo->pUpdatedMap, pInfo->pDelWins, pOperator);
  QUERY_CHECK_CODE(code, lino, _end);

  if (pInfo->destHasPrimaryKey && IS_NORMAL_INTERVAL_OP(pOperator)) {
    code = copyIntervalDeleteKey(pInfo->pDeletedMap, pInfo->pDelWins);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pInfo->pUpdatedMap, pIte, &iter)) != NULL) {
    void* tmp = taosArrayPush(pInfo->pUpdated, pIte);
    if (!tmp) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }
  taosArraySort(pInfo->pUpdated, winPosCmprImpl);

  initMultiResInfoFromArrayList(&pInfo->groupResInfo, pInfo->pUpdated);
  pInfo->pUpdated = NULL;
  code = blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  tSimpleHashCleanup(pInfo->pUpdatedMap);
  pInfo->pUpdatedMap = NULL;

  code = buildIntervalResult(pOperator, ppRes);
  QUERY_CHECK_CODE(code, lino, _end);

  return code;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    pTaskInfo->code = code;
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  setStreamOperatorCompleted(pOperator);
  (*ppRes) = NULL;
  return code;
}

int32_t createStreamIntervalOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo,
                                         SReadHandle* pHandle, SOperatorInfo** pOptrInfo) {
  QRY_OPTR_CHECK(pOptrInfo);

  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t numOfCols = 0;

  SStreamIntervalOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamIntervalOperatorInfo));
  SOperatorInfo*               pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _error);
  }

  SStreamIntervalPhysiNode* pIntervalPhyNode = (SStreamIntervalPhysiNode*)pPhyNode;

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pPhyNode->pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pResBlock, code, lino, _error, terrno);
  initBasicInfo(&pInfo->binfo, pResBlock);

  pInfo->interval = (SInterval){
      .interval = pIntervalPhyNode->interval,
      .sliding = pIntervalPhyNode->sliding,
      .intervalUnit = pIntervalPhyNode->intervalUnit,
      .slidingUnit = pIntervalPhyNode->slidingUnit,
      .offset = pIntervalPhyNode->offset,
      .precision = ((SColumnNode*)pIntervalPhyNode->window.pTspk)->node.resType.precision,
  };

  pInfo->twAggSup =
      (STimeWindowAggSupp){.waterMark = pIntervalPhyNode->window.watermark,
                           .calTrigger = pIntervalPhyNode->window.triggerType,
                           .maxTs = INT64_MIN,
                           .minTs = INT64_MAX,
                           .deleteMark = getDeleteMark(&pIntervalPhyNode->window, pIntervalPhyNode->interval)};

  pOperator->pTaskInfo = pTaskInfo;
  SStorageAPI* pAPI = &pOperator->pTaskInfo->storageAPI;

  pInfo->ignoreExpiredData = pIntervalPhyNode->window.igExpired;
  pInfo->ignoreExpiredDataSaved = false;

  SExprSupp* pSup = &pOperator->exprSupp;
  pSup->hasWindowOrGroup = true;

  code = initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->primaryTsIndex = ((SColumnNode*)pIntervalPhyNode->window.pTspk)->slotId;
  initResultSizeInfo(&pOperator->resultInfo, 4096);

  pInfo->pState = taosMemoryCalloc(1, sizeof(SStreamState));
  QUERY_CHECK_NULL(pInfo->pState, code, lino, _error, terrno);
  *(pInfo->pState) = *(pTaskInfo->streamInfo.pState);
  pAPI->stateStore.streamStateSetNumber(pInfo->pState, -1, pInfo->primaryTsIndex);

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  SExprInfo* pExprInfo = NULL;
  code = createExprInfo(pIntervalPhyNode->window.pFuncs, NULL, &pExprInfo, &numOfCols);
  QUERY_CHECK_CODE(code, lino, _error);
  code = initAggSup(pSup, &pInfo->aggSup, pExprInfo, numOfCols, keyBufSize, pTaskInfo->id.str, pInfo->pState,
                    &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);
  tSimpleHashSetFreeFp(pInfo->aggSup.pResultRowHashTable, destroyFlusedppPos);

  if (pIntervalPhyNode->window.pExprs != NULL) {
    int32_t    numOfScalar = 0;
    SExprInfo* pScalarExprInfo = NULL;

    code = createExprInfo(pIntervalPhyNode->window.pExprs, NULL, &pScalarExprInfo, &numOfScalar);
    QUERY_CHECK_CODE(code, lino, _error);

    code = initExprSupp(&pInfo->scalarSupp, pScalarExprInfo, numOfScalar, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  pInfo->invertible = false;
  pInfo->pDelWins = taosArrayInit(4, sizeof(SWinKey));
  QUERY_CHECK_NULL(pInfo->pDelWins, code, lino, _error, terrno);
  pInfo->delIndex = 0;

  code = createSpecialDataBlock(STREAM_DELETE_RESULT, &pInfo->pDelRes);
  QUERY_CHECK_CODE(code, lino, _error);

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

  pInfo->stateStore = pTaskInfo->storageAPI.stateStore;
  pInfo->pState->pFileState = pTaskInfo->storageAPI.stateStore.streamFileStateInit(
      tsStreamBufferSize, sizeof(SWinKey), pInfo->aggSup.resultRowSize, funResSize, compareTs, pInfo->pState,
      pInfo->twAggSup.deleteMark, GET_TASKID(pTaskInfo), pHandle->checkpointId, STREAM_STATE_BUFF_HASH);
  QUERY_CHECK_NULL(pInfo->pState->pFileState, code, lino, _error, terrno);

  setOperatorInfo(pOperator, "StreamIntervalOperator", QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL, true, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  pOperator->fpSet =
      createOperatorFpSet(optrDummyOpenFn, doStreamIntervalAggNext, NULL, destroyStreamFinalIntervalOperatorInfo,
                          optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorStreamStateFn(pOperator, streamIntervalReleaseState, streamIntervalReloadState);

  pInfo->recvGetAll = false;

  code = createSpecialDataBlock(STREAM_CHECKPOINT, &pInfo->pCheckpointRes);
      QUERY_CHECK_CODE(code, lino, _error);

  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pInfo->pDeletedMap = tSimpleHashInit(4096, hashFn);
  QUERY_CHECK_NULL(pInfo->pDeletedMap, code, lino, _error, terrno);
  pInfo->destHasPrimaryKey = pIntervalPhyNode->window.destHasPrimayKey;

  // for stream
  void*   buff = NULL;
  int32_t len = 0;
  int32_t res = pAPI->stateStore.streamStateGetInfo(pInfo->pState, STREAM_INTERVAL_OP_CHECKPOINT_NAME,
                                                    strlen(STREAM_INTERVAL_OP_CHECKPOINT_NAME), &buff, &len);
  if (res == TSDB_CODE_SUCCESS) {
    doStreamIntervalDecodeOpState(buff, len, pOperator);
    taosMemoryFree(buff);
  }

  code = initIntervalDownStream(downstream, pPhyNode->type, pInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  code = appendDownstream(pOperator, &downstream, 1);
  QUERY_CHECK_CODE(code, lino, _error);

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  if (pInfo != NULL) destroyStreamFinalIntervalOperatorInfo(pInfo);
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  return code;
}

static void doStreamMidIntervalAggImpl(SOperatorInfo* pOperator, SSDataBlock* pSDataBlock, SSHashObj* pUpdatedMap) {
  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      lino = 0;
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
    void*   chIds = taosHashGet(pInfo->pPullDataMap, &key, sizeof(SWinKey));
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

    int32_t winCode = TSDB_CODE_SUCCESS;
    code = setIntervalOutputBuf(pInfo->pState, &nextWin, &pResPos, groupId, pSup->pCtx, numOfOutput,
                                pSup->rowEntryInfoOffset, &pInfo->aggSup, &pInfo->stateStore, &winCode);
    QUERY_CHECK_CODE(code, lino, _end);

    pResult = (SResultRow*)pResPos->pRowBuff;

    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE) {
      code = saveWinResult(&key, pResPos, pUpdatedMap);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_WINDOW_CLOSE) {
      code = tSimpleHashPut(pInfo->aggSup.pResultRowHashTable, &key, sizeof(SWinKey), &pResPos, POINTER_BYTES);
      QUERY_CHECK_CODE(code, lino, _end);
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
      code = blockDataUpdateTsWindow(pSDataBlock, 0);
      QUERY_CHECK_CODE(code, lino, _end);

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

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
}

static int32_t addMidRetriveWindow(SArray* wins, SHashObj* pMidPullMap, int32_t numOfChild) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t size = taosArrayGetSize(wins);
  for (int32_t i = 0; i < size; i++) {
    SWinKey* winKey = taosArrayGet(wins, i);
    void*    chIds = taosHashGet(pMidPullMap, winKey, sizeof(SWinKey));
    if (!chIds) {
      code = addPullWindow(pMidPullMap, winKey, numOfChild);
      qDebug("===stream===prepare mid operator retrive for delete %" PRId64 ", size:%d", winKey->ts, numOfChild);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }
_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static SSDataBlock* buildMidIntervalResult(SOperatorInfo* pOperator) {
  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  uint16_t                     opType = pOperator->operatorType;

  if (pInfo->recvPullover) {
    pInfo->recvPullover = false;
    printDataBlock(pInfo->pMidPulloverRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
    return pInfo->pMidPulloverRes;
  }

  qDebug("===stream=== build mid interval result");
  doBuildDeleteResult(pInfo, pInfo->pMidPullDatas, &pInfo->midDelIndex, pInfo->pDelRes);
  if (pInfo->pDelRes->info.rows != 0) {
    // process the rest of the data
    printDataBlock(pInfo->pDelRes, getStreamOpName(opType), GET_TASKID(pTaskInfo));
    return pInfo->pDelRes;
  }

  if (pInfo->recvRetrive) {
    pInfo->recvRetrive = false;
    printDataBlock(pInfo->pMidRetriveRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
    return pInfo->pMidRetriveRes;
  }

  return NULL;
}

static int32_t doStreamMidIntervalAggNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      lino = 0;
  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*                 pAPI = &pOperator->pTaskInfo->storageAPI;
  SOperatorInfo*               downstream = pOperator->pDownstream[0];
  SExprSupp*                   pSup = &pOperator->exprSupp;

  qDebug("stask:%s  %s status: %d", GET_TASKID(pTaskInfo), getStreamOpName(pOperator->operatorType), pOperator->status);

  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return code;
  } else if (pOperator->status == OP_RES_TO_RETURN) {
    SSDataBlock* resBlock = NULL;
    code = buildIntervalResult(pOperator, &resBlock);
    QUERY_CHECK_CODE(code, lino, _end);
    if (resBlock != NULL) {
      (*ppRes) = resBlock;
      return code;
    }

    setOperatorCompleted(pOperator);
    clearFunctionContext(&pOperator->exprSupp);
    clearStreamIntervalOperator(pInfo);
    qDebug("stask:%s  ===stream===%s clear", GET_TASKID(pTaskInfo), getStreamOpName(pOperator->operatorType));
    (*ppRes) = NULL;
    return code;
  } else {
    SSDataBlock* resBlock = NULL;
    code = buildIntervalResult(pOperator, &resBlock);
    QUERY_CHECK_CODE(code, lino, _end);
    if (resBlock != NULL) {
      (*ppRes) = resBlock;
      return code;
    }

    resBlock = buildMidIntervalResult(pOperator);
    if (resBlock != NULL) {
      (*ppRes) = resBlock;
      return code;
    }

    if (pInfo->clearState) {
      pInfo->clearState = false;
      clearFunctionContext(&pOperator->exprSupp);
      clearStreamIntervalOperator(pInfo);
    }
  }

  if (!pInfo->pUpdated) {
    pInfo->pUpdated = taosArrayInit(4096, POINTER_BYTES);
    QUERY_CHECK_NULL(pInfo->pUpdated, code, lino, _end, terrno);
  }
  if (!pInfo->pUpdatedMap) {
    _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
    pInfo->pUpdatedMap = tSimpleHashInit(4096, hashFn);
    QUERY_CHECK_NULL(pInfo->pUpdatedMap, code, lino, _end, terrno);
  }

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
      pOperator->status = OP_RES_TO_RETURN;
      qDebug("===stream===return data:%s. recv datablock num:%" PRIu64, getStreamOpName(pOperator->operatorType),
             pInfo->numOfDatapack);
      pInfo->numOfDatapack = 0;
      break;
    }
    pInfo->numOfDatapack++;
    printSpecDataBlock(pBlock, getStreamOpName(pOperator->operatorType), "recv", GET_TASKID(pTaskInfo));
    setStreamOperatorState(&pInfo->basic, pBlock->info.type);

    if (pBlock->info.type == STREAM_NORMAL || pBlock->info.type == STREAM_PULL_DATA) {
      pInfo->binfo.pRes->info.type = pBlock->info.type;
    } else if (pBlock->info.type == STREAM_DELETE_DATA || pBlock->info.type == STREAM_DELETE_RESULT ||
               pBlock->info.type == STREAM_CLEAR) {
      SArray* delWins = taosArrayInit(8, sizeof(SWinKey));
      if (!delWins) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        QUERY_CHECK_CODE(code, lino, _end);
      }
      code =
          doDeleteWindows(pOperator, &pInfo->interval, pBlock, delWins, pInfo->pUpdatedMap, pInfo->pFinalPullDataMap);
      QUERY_CHECK_CODE(code, lino, _end);

      removeResults(delWins, pInfo->pUpdatedMap);
      void* tmp = taosArrayAddAll(pInfo->pDelWins, delWins);
      if (!tmp && taosArrayGetSize(delWins) > 0) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        QUERY_CHECK_CODE(code, lino, _end);
      }
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
        (*ppRes) = pInfo->pDelRes;
        return code;
      }
      continue;
    } else if (pBlock->info.type == STREAM_CREATE_CHILD_TABLE) {
      (*ppRes) = pBlock;
      return code;
    } else if (pBlock->info.type == STREAM_PULL_OVER) {
      code = processPullOver(pBlock, pInfo->pPullDataMap, pInfo->pFinalPullDataMap, &pInfo->interval, pInfo->pPullWins,
                             pInfo->numOfChild, pOperator, &pInfo->recvPullover);
      QUERY_CHECK_CODE(code, lino, _end);

      if (pInfo->recvPullover) {
        code = copyDataBlock(pInfo->pMidPulloverRes, pBlock);
        QUERY_CHECK_CODE(code, lino, _end);

        pInfo->clearState = true;
        break;
      }
      continue;
    } else if (pBlock->info.type == STREAM_CHECKPOINT) {
      pAPI->stateStore.streamStateCommit(pInfo->pState);
      doStreamIntervalSaveCheckpoint(pOperator);
      code = copyDataBlock(pInfo->pCheckpointRes, pBlock);
      QUERY_CHECK_CODE(code, lino, _end);

      continue;
    } else if (pBlock->info.type == STREAM_MID_RETRIEVE) {
      SArray* delWins = taosArrayInit(8, sizeof(SWinKey));
      if (!delWins) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        QUERY_CHECK_CODE(code, lino, _end);
      }
      code = doDeleteWindows(pOperator, &pInfo->interval, pBlock, delWins, pInfo->pUpdatedMap, NULL);
      QUERY_CHECK_CODE(code, lino, _end);

      code = addMidRetriveWindow(delWins, pInfo->pPullDataMap, pInfo->numOfChild);
      QUERY_CHECK_CODE(code, lino, _end);

      taosArrayDestroy(delWins);
      pInfo->recvRetrive = true;
      code = copyDataBlock(pInfo->pMidRetriveRes, pBlock);
      QUERY_CHECK_CODE(code, lino, _end);

      pInfo->pMidRetriveRes->info.type = STREAM_MID_RETRIEVE;
      pInfo->clearState = true;
      break;
    } else {
      if (pBlock->info.type != STREAM_INVALID) {
        code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }

    if (pInfo->scalarSupp.pExprInfo != NULL) {
      SExprSupp* pExprSup = &pInfo->scalarSupp;
      code = projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    code = setInputDataBlock(pSup, pBlock, TSDB_ORDER_ASC, MAIN_SCAN, true);
    QUERY_CHECK_CODE(code, lino, _end);
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
    void* tmp = taosArrayPush(pInfo->pUpdated, pIte);
    if (!tmp) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  tSimpleHashCleanup(pInfo->pUpdatedMap);
  pInfo->pUpdatedMap = NULL;
  taosArraySort(pInfo->pUpdated, winPosCmprImpl);

  initMultiResInfoFromArrayList(&pInfo->groupResInfo, pInfo->pUpdated);
  pInfo->pUpdated = NULL;
  code = blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  SSDataBlock* resBlock = NULL;
  code = buildIntervalResult(pOperator, &resBlock);
  QUERY_CHECK_CODE(code, lino, _end);
  if (resBlock != NULL) {
    (*ppRes) = resBlock;
    return code;
  }

  resBlock = buildMidIntervalResult(pOperator);
  if (resBlock != NULL) {
    (*ppRes) = resBlock;
    return code;
  }

  if (pInfo->clearState) {
    pInfo->clearState = false;
    clearFunctionContext(&pOperator->exprSupp);
    clearStreamIntervalOperator(pInfo);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    pTaskInfo->code = code;
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  (*ppRes) = NULL;
  return code;
}

static SSDataBlock* doStreamMidIntervalAgg(SOperatorInfo* pOperator) {
  SSDataBlock* pRes = NULL;
  int32_t      code = doStreamMidIntervalAggNext(pOperator, &pRes);
  return pRes;
}

void setStreamOperatorCompleted(SOperatorInfo* pOperator) {
  setOperatorCompleted(pOperator);
  qDebug("stask:%s  %s status: %d. set completed", GET_TASKID(pOperator->pTaskInfo),
         getStreamOpName(pOperator->operatorType), pOperator->status);
}
