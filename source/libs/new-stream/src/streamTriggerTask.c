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

#include "streamTriggerTask.h"

#include "dataSink.h"
#include "plannodes.h"
#include "streamInt.h"
#include "streamReader.h"
#include "tdatablock.h"
#include "ttime.h"

static int32_t strtcResumeCheck(SSTriggerRealtimeContext *pContext);
static int32_t strtcSendPullReq(SSTriggerRealtimeContext *pContext, ESTriggerPullType type, void *param);
static int32_t sthcSendPullReq(SSTriggerHistoryContext *pContext, ESTriggerPullType type, void *param);

static TdThreadOnce     gStreamTriggerModuleInit = PTHREAD_ONCE_INIT;
static volatile int32_t gStreamTriggerInitRes = TSDB_CODE_SUCCESS;
static volatile bool    gStreamTriggerToStop = false;
// The number of calculation requests that can be sent by stream triggers on the entire dnode is limited
static TdThreadMutex    gStreamTriggerCalcMutex;
static TdThreadCond     gStreamTriggerCalcCond;
static volatile int32_t gStreamTriggerCalcLimit = 10000;  // todo(kjq): adjust dynamically
// When the trigger task's real-time calculation catches up with the latest WAL
// progress, it will wait and be awakened later by a timer.
static SRWLatch gStreamTriggerWaitLatch;
static SList    gStreamTriggerWaitList;
static tmr_h    gStreamTriggerTimerId = NULL;

#define STREAM_TRIGGER_CHECK_INTERVAL_MS 1000  // 1s
#define STREAM_TRIGGER_WAIT_TIME_MS      1000  // 1s

typedef struct StreamTriggerWaitInfo {
  SStreamTriggerTask *pTask;
  int64_t             resumeTime;
} StreamTriggerWaitInfo;

static int32_t streamTriggerAddWaitTask(SStreamTriggerTask *pTask, int64_t resumeTime) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  StreamTriggerWaitInfo info = {
      .pTask = pTask,
      .resumeTime = resumeTime,
  };
  taosWLockLatch(&gStreamTriggerWaitLatch);
  code = tdListAppend(&gStreamTriggerWaitList, &info);
  taosWUnLockLatch(&gStreamTriggerWaitLatch);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("failed to add stream trigger task %" PRIx64 "-%" PRIx64 " to wait list since %s",
                 pTask->task.streamId, pTask->task.taskId, tstrerror(code));
  } else {
    ST_TASK_DLOG("add stream trigger task %" PRIx64 "-%" PRIx64 " to wait list, resumeTime:%" PRId64,
                 pTask->task.streamId, pTask->task.taskId, resumeTime);
  }
  return code;
}

static void streamTriggerCheckWaitList(void *param, void *tmrId) {
  int64_t now = taosGetMonotonicMs();
  SList   readylist = {0};
  taosWLockLatch(&gStreamTriggerWaitLatch);
  SListNode *pNode = TD_DLIST_HEAD(&gStreamTriggerWaitList);
  while (pNode != NULL) {
    SListNode *pCurNode = pNode;
    pNode = TD_DLIST_NODE_NEXT(pCurNode);
    StreamTriggerWaitInfo *pInfo = (StreamTriggerWaitInfo *)pCurNode->data;
    if (pInfo == NULL || pInfo->pTask == NULL) {
      stWarn("unexpected null stream trigger wait info");
      TD_DLIST_POP(&gStreamTriggerWaitList, pCurNode);
      taosMemoryFreeClear(pCurNode);
    } else if (pInfo->resumeTime <= now) {
      TD_DLIST_POP(&gStreamTriggerWaitList, pCurNode);
      TD_DLIST_APPEND(&readylist, pCurNode);
    }
  }
  taosWUnLockLatch(&gStreamTriggerWaitLatch);

  pNode = TD_DLIST_HEAD(&readylist);
  while (pNode != NULL) {
    SListNode *pCurNode = pNode;
    pNode = TD_DLIST_NODE_NEXT(pCurNode);
    StreamTriggerWaitInfo *pInfo = (StreamTriggerWaitInfo *)pCurNode->data;
    // resume the task
    SStreamTriggerTask *pTask = pInfo->pTask;
    pTask->pRealtimeCtx->curReaderIdx = -1;
    int32_t code = strtcResumeCheck(pTask->pRealtimeCtx);
    if (code != TSDB_CODE_SUCCESS) {
      stError("failed to resume stream trigger task %" PRIx64 "-%" PRIx64 " since %s", pTask->task.streamId,
              pTask->task.taskId, tstrerror(code));
      continue;
    }
    stDebug("resume stream trigger task %" PRIx64 "-%" PRIx64 " since now:%" PRId64 ", resumeTime:%" PRId64,
            pInfo->pTask->task.streamId, pInfo->pTask->task.taskId, now, pInfo->resumeTime);
    TD_DLIST_POP(&readylist, pCurNode);
    taosMemoryFreeClear(pCurNode);
  }

  taosWLockLatch(&gStreamTriggerWaitLatch);
  pNode = TD_DLIST_HEAD(&readylist);
  while (pNode != NULL) {
    SListNode *pCurNode = pNode;
    pNode = TD_DLIST_NODE_NEXT(pCurNode);
    TD_DLIST_POP(&readylist, pCurNode);
    TD_DLIST_APPEND(&gStreamTriggerWaitList, pCurNode);
  }
  taosWUnLockLatch(&gStreamTriggerWaitLatch);

  streamTmrStart(streamTriggerCheckWaitList, STREAM_TRIGGER_CHECK_INTERVAL_MS, NULL, gStreamMgmt.timer,
                 &gStreamTriggerTimerId, "stream-trigger");
  return;
}

static void streamTriggerEnvDoInit() {
  gStreamTriggerInitRes = taosThreadMutexInit(&gStreamTriggerCalcMutex, NULL);
  if (gStreamTriggerInitRes != TSDB_CODE_SUCCESS) {
    stError("failed to init gStreamTriggerCalcMutex since %s", tstrerror(gStreamTriggerInitRes));
    return;
  }
  gStreamTriggerInitRes = taosThreadCondInit(&gStreamTriggerCalcCond, NULL);
  if (gStreamTriggerInitRes != TSDB_CODE_SUCCESS) {
    stError("failed to init gStreamTriggerCalcCond since %s", tstrerror(gStreamTriggerInitRes));
    return;
  }
  taosInitRWLatch(&gStreamTriggerWaitLatch);
  tdListInit(&gStreamTriggerWaitList, sizeof(StreamTriggerWaitInfo));
  streamTmrStart(streamTriggerCheckWaitList, STREAM_TRIGGER_CHECK_INTERVAL_MS, NULL, gStreamMgmt.timer,
                 &gStreamTriggerTimerId, "stream-trigger");
}

int32_t streamTriggerEnvInit() {
  int32_t code = taosThreadOnce(&gStreamTriggerModuleInit, streamTriggerEnvDoInit);
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to init stream trigger module since %s", tstrerror(code));
    return code;
  }
  return gStreamTriggerInitRes;
}

void streamTriggerEnvStop() {
  int32_t code = taosThreadMutexLock(&gStreamTriggerCalcMutex);
  if (code == TSDB_CODE_SUCCESS) {
    gStreamTriggerToStop = true;
    code = taosThreadCondBroadcast(&gStreamTriggerCalcCond);
    if (code != TSDB_CODE_SUCCESS) {
      stWarn("failed to broadcast gStreamTriggerCalcCond since %s", tstrerror(code));
    }
    code = taosThreadMutexUnlock(&gStreamTriggerCalcMutex);
  } else {
    stWarn("failed to lock gStreamTriggerCalcMutex since %s", tstrerror(code));
  }

  taosWLockLatch(&gStreamTriggerWaitLatch);
  tdListEmpty(&gStreamTriggerWaitList);
  taosWUnLockLatch(&gStreamTriggerWaitLatch);
}

void streamTriggerEnvCleanup() {
  int32_t code = taosThreadMutexDestroy(&gStreamTriggerCalcMutex);
  if (code != TSDB_CODE_SUCCESS) {
    stWarn("failed to destroy gStreamTriggerCalcMutex since %s", tstrerror(code));
  }
  code = taosThreadCondDestroy(&gStreamTriggerCalcCond);
  if (code != TSDB_CODE_SUCCESS) {
    stWarn("failed to destroy gStreamTriggerCalcCond since %s", tstrerror(code));
  }
}

int32_t streamTriggerKickCalc() {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  bool    locked = false;

  code = taosThreadMutexLock(&gStreamTriggerCalcMutex);
  QUERY_CHECK_CODE(code, lino, _end);
  locked = true;

  ++gStreamTriggerCalcLimit;
  stTrace("receive calc resp and increase gStreamTriggerCalcLimit to %d", gStreamTriggerCalcLimit);
  code = taosThreadCondSignal(&gStreamTriggerCalcCond);
  QUERY_CHECK_CODE(code, lino, _end);

  code = taosThreadMutexUnlock(&gStreamTriggerCalcMutex);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    if (locked) {
      code = taosThreadMutexUnlock(&gStreamTriggerCalcMutex);
    }
  }
  return code;
}

static int32_t streamTriggerAcquireCalcReq() {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  bool    locked = false;

  stTrace("try to wait for gStreamTriggerCalcLimit");
  code = taosThreadMutexLock(&gStreamTriggerCalcMutex);
  QUERY_CHECK_CODE(code, lino, _end);
  locked = true;

  while (gStreamTriggerCalcLimit == 0) {
    if (gStreamTriggerToStop) {
      code = TSDB_CODE_STREAM_EXEC_CANCELLED;
      QUERY_CHECK_CODE(code, lino, _end);
    }
    code = taosThreadCondWait(&gStreamTriggerCalcCond, &gStreamTriggerCalcMutex);
    QUERY_CHECK_CODE(code, lino, _end);
  }
  gStreamTriggerCalcLimit--;
  stTrace("acquire calc req and decrease gStreamTriggerCalcLimit to %d", gStreamTriggerCalcLimit);

  code = taosThreadMutexUnlock(&gStreamTriggerCalcMutex);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    if (locked) {
      code = taosThreadMutexUnlock(&gStreamTriggerCalcMutex);
    }
  }
  return code;
}

#define TRIGGER_META_SKEY_INACCURATE_MASK (1 << 0)
#define TRIGGER_META_EKEY_INACCURATE_MASK (1 << 1)
#define TRIGGER_META_DATA_EMPTY_MASK      (1 << 2)

#define SET_TRIGGER_META_NROW_INACCURATE(pMeta) \
  do {                                          \
    if ((pMeta)->nrows >= 0) {                  \
      (pMeta)->nrows = INT64_MIN;               \
    }                                           \
  } while (0)
#define IS_TRIGGER_META_NROW_INACCURATE(pMeta) ((pMeta)->nrows < 0)

#define SET_TRIGGER_META_SKEY_INACCURATE(pMeta)          \
  do {                                                   \
    SET_TRIGGER_META_NROW_INACCURATE(pMeta);             \
    (pMeta)->nrows |= TRIGGER_META_SKEY_INACCURATE_MASK; \
  } while (0)
#define IS_TRIGGER_META_SKEY_INACCURATE(pMeta) \
  (IS_TRIGGER_META_NROW_INACCURATE(pMeta) && ((pMeta)->nrows & TRIGGER_META_SKEY_INACCURATE_MASK))

#define SET_TRIGGER_META_EKEY_INACCURATE(pMeta)          \
  do {                                                   \
    SET_TRIGGER_META_NROW_INACCURATE(pMeta);             \
    (pMeta)->nrows |= TRIGGER_META_EKEY_INACCURATE_MASK; \
  } while (0)
#define IS_TRIGGER_META_EKEY_INACCURATE(pMeta) \
  (IS_TRIGGER_META_NROW_INACCURATE(pMeta) && ((pMeta)->nrows & TRIGGER_META_EKEY_INACCURATE_MASK))

#define SET_TRIGGER_META_DATA_EMPTY(pMeta)          \
  do {                                              \
    SET_TRIGGER_META_NROW_INACCURATE(pMeta);        \
    (pMeta)->nrows |= TRIGGER_META_DATA_EMPTY_MASK; \
  } while (0)
#define IS_TRIGGER_META_DATA_EMPTY(pMeta) \
  (IS_TRIGGER_META_NROW_INACCURATE(pMeta) && ((pMeta)->nrows & TRIGGER_META_DATA_EMPTY_MASK))

#define IS_TRIGGER_WAL_META_MERGER_EMPTY(pMerger) (taosArrayGetSize((pMerger)->pMetaNodeBuf) == 0)
#define SET_TRIGGER_WAL_META_SESS_MERGER_INVALID(pMerger) \
  pMerger->sessRange = (STimeWindow) { .skey = INT64_MIN, .ekey = INT64_MIN }
#define IS_TRIGGER_WAL_META_SESS_MERGER_INVALID(pMerger) ((pMerger)->sessRange.ekey == INT64_MIN)
#define SET_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger) \
  pMerger->dataReadRange = (STimeWindow) { .skey = INT64_MIN, .ekey = INT64_MIN }
#define IS_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger) ((pMerger)->dataReadRange.ekey == INT64_MIN)

static int32_t stwmInit(SSTriggerWalMetaMerger *pMerger, SSTriggerRealtimeContext *pContext) {
  pMerger->pContext = pContext;
  pMerger->useMetaAccel = true;  // todo(kjq): set it to false if stream trigger has prediction filter
  SET_TRIGGER_WAL_META_SESS_MERGER_INVALID(pMerger);
  SET_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger);
  return TSDB_CODE_SUCCESS;
}

static void stwmClear(SSTriggerWalMetaMerger *pMerger) {
  if (pMerger->pMetaNodeBuf != NULL) {
    taosArrayClear(pMerger->pMetaNodeBuf);
  }
  if (pMerger->pMetaLists != NULL) {
    for (int32_t i = 0; i < taosArrayGetSize(pMerger->pMetaLists); ++i) {
      SSTriggerWalMetaList *pList = taosArrayGet(pMerger->pMetaLists, i);
      if (pList->pDataBlock != NULL) {
        blockDataDestroy(pList->pDataBlock);
        pList->pDataBlock = NULL;
      }
    }
    taosArrayClear(pMerger->pMetaLists);
  }
  SET_TRIGGER_WAL_META_SESS_MERGER_INVALID(pMerger);
  SET_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger);
}

static void stwmDestroy(void *ptr) {
  SSTriggerWalMetaMerger **ppMerger = ptr;
  if (ppMerger == NULL || *ppMerger == NULL) {
    return;
  }

  SSTriggerWalMetaMerger *pMerger = *ppMerger;
  if (pMerger->pMetaNodeBuf != NULL) {
    taosArrayDestroy(pMerger->pMetaNodeBuf);
    pMerger->pMetaNodeBuf = NULL;
  }
  if (pMerger->pMetaLists != NULL) {
    for (int32_t i = 0; i < taosArrayGetSize(pMerger->pMetaLists); ++i) {
      SSTriggerWalMetaList *pList = taosArrayGet(pMerger->pMetaLists, i);
      if (pList->pDataBlock != NULL) {
        blockDataDestroy(pList->pDataBlock);
        pList->pDataBlock = NULL;
      }
    }
    taosArrayDestroy(pMerger->pMetaLists);
    pMerger->pMetaLists = NULL;
  }
  if (pMerger->pSessMerger != NULL) {
    tMergeTreeDestroy(&pMerger->pSessMerger);
  }
  if (pMerger->pDataMerger != NULL) {
    tMergeTreeDestroy(&pMerger->pDataMerger);
  }
  taosMemoryFreeClear(*ppMerger);
}

static int32_t stwmSetWalMetas(SSTriggerWalMetaMerger *pMerger, SSTriggerWalMeta *pMetas, int32_t nMetas,
                               int32_t tsSlotId) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pMerger->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  QUERY_CHECK_CONDITION(IS_TRIGGER_WAL_META_MERGER_EMPTY(pMerger), code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(IS_TRIGGER_WAL_META_SESS_MERGER_INVALID(pMerger), code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(IS_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger), code, lino, _end, TSDB_CODE_INVALID_PARA);

  pMerger->tsSlotId = tsSlotId;
  if (pMerger->pMetaNodeBuf == NULL) {
    pMerger->pMetaNodeBuf = taosArrayInit(nMetas, sizeof(SSTriggerWalMetaNode));
    QUERY_CHECK_NULL(pMerger->pMetaNodeBuf, code, lino, _end, terrno);
  } else {
    code = taosArrayEnsureCap(pMerger->pMetaNodeBuf, nMetas);
    QUERY_CHECK_CODE(code, lino, _end);
  }
  TARRAY_SIZE(pMerger->pMetaNodeBuf) = nMetas;
  if (pMerger->pMetaLists == NULL) {
    pMerger->pMetaLists = taosArrayInit(0, sizeof(SSTriggerWalMetaList));
    QUERY_CHECK_NULL(pMerger->pMetaLists, code, lino, _end, terrno);
  }
  TARRAY_SIZE(pMerger->pMetaLists) = 0;

  for (int32_t i = 0; i < nMetas; ++i) {
    SSTriggerWalMetaNode *pNode = taosArrayGet(pMerger->pMetaNodeBuf, i);
    pNode->pMeta = &pMetas[i];
    pNode->next = NULL;
    int32_t j = 0;
    while (j < taosArrayGetSize(pMerger->pMetaLists)) {
      SSTriggerWalMetaList *pList = taosArrayGet(pMerger->pMetaLists, j);
      if (pNode->pMeta->skey > pList->tail->pMeta->ekey) {
        pList->tail->next = pNode;
        pList->tail = pNode;
        break;
      }
      ++j;
    }
    if (j >= taosArrayGetSize(pMerger->pMetaLists)) {
      SSTriggerWalMetaList list = {.head = pNode, .tail = pNode};
      void                *px = taosArrayPush(pMerger->pMetaLists, &list);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stwmMetaListCompareSess(const void *pLeft, const void *pRight, void *param) {
  int32_t left = *(const int32_t *)pLeft;
  int32_t right = *(const int32_t *)pRight;
  SArray *pMetaLists = (SArray *)param;

  if (left < taosArrayGetSize(pMetaLists) && right < taosArrayGetSize(pMetaLists)) {
    SSTriggerWalMetaList *pLeft = taosArrayGet(pMetaLists, left);
    SSTriggerWalMetaList *pRight = taosArrayGet(pMetaLists, right);
    if (pLeft->curSessWin.skey < pRight->curSessWin.skey) {
      return -1;
    } else if (pLeft->curSessWin.skey > pRight->curSessWin.skey) {
      return 1;
    } else if (pLeft->curSessWin.ekey != pRight->curSessWin.ekey) {
      return pLeft->curSessWin.ekey - pRight->curSessWin.ekey;
    }
  }
  return left - right;
}

static int32_t stwmMetaListNextSess(SSTriggerWalMetaMerger *pMerger, SSTriggerWalMetaList *pList, int64_t gap) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pMerger->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;
  int64_t                   globalEnd = pMerger->sessRange.ekey;

  int64_t               start = pList->curSessWin.ekey + 1;
  SSTriggerWalMetaNode *pSessNode = pList->nextSessNode;
  while (pSessNode != NULL) {
    if (pSessNode->pMeta->ekey < start) {
      pSessNode = pSessNode->next;
      continue;
    }
    if (pSessNode->pMeta->skey > globalEnd) {
      pSessNode = NULL;
      break;
    }
    if (pSessNode->pMeta->skey >= start && !IS_TRIGGER_META_SKEY_INACCURATE(pSessNode->pMeta)) {
      start = pSessNode->pMeta->skey;
      break;
    }
    if (pSessNode->pMeta->ekey <= globalEnd && !IS_TRIGGER_META_EKEY_INACCURATE(pSessNode->pMeta)) {
      start = pSessNode->pMeta->ekey;
      break;
    }
    pSessNode = pSessNode->next;
  }

  if (pSessNode == NULL) {
    pList->nextSessNode = NULL;
    pList->curSessWin = (STimeWindow){.skey = INT64_MAX, .ekey = INT64_MAX};
    goto _end;
  }

  int64_t end = start;
  while (pSessNode != NULL) {
    if (pSessNode->pMeta->ekey <= end) {
      pSessNode = pSessNode->next;
      continue;
    }
    if (pSessNode->pMeta->skey > globalEnd) {
      pSessNode = NULL;
      break;
    }
    if (pSessNode->pMeta->skey > end && !IS_TRIGGER_META_SKEY_INACCURATE(pSessNode->pMeta)) {
      if (pSessNode->pMeta->skey <= end + gap) {
        end = pSessNode->pMeta->skey;
      } else {
        break;
      }
    }
    if (pSessNode->pMeta->ekey <= globalEnd && !IS_TRIGGER_META_EKEY_INACCURATE(pSessNode->pMeta)) {
      if (pSessNode->pMeta->ekey <= end + gap) {
        end = pSessNode->pMeta->ekey;
      } else {
        break;
      }
    }
    pSessNode = pSessNode->next;
  }

  pList->nextSessNode = pSessNode;
  pList->curSessWin = (STimeWindow){.skey = start, .ekey = end};

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stwmBuildSessMerger(SSTriggerWalMetaMerger *pMerger, int64_t skey, int64_t ekey, int64_t gap) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pMerger->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  QUERY_CHECK_CONDITION(IS_TRIGGER_WAL_META_SESS_MERGER_INVALID(pMerger), code, lino, _end, TSDB_CODE_INVALID_PARA);

  if (IS_TRIGGER_WAL_META_MERGER_EMPTY(pMerger) || skey > ekey || !pMerger->useMetaAccel) {
    SET_TRIGGER_WAL_META_SESS_MERGER_INVALID(pMerger);
    goto _end;
  }

  pMerger->sessRange = (STimeWindow){.skey = skey, .ekey = ekey};

  int32_t numList = taosArrayGetSize(pMerger->pMetaLists);
  for (int32_t i = 0; i < numList; ++i) {
    SSTriggerWalMetaList *pList = taosArrayGet(pMerger->pMetaLists, i);
    pList->nextSessNode = pList->head;
    pList->curSessWin = (STimeWindow){.skey = INT64_MIN, .ekey = INT64_MIN};
    code = stwmMetaListNextSess(pMerger, pList, gap);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (pMerger->pSessMerger && pMerger->pSessMerger->numOfSources < numList) {
    tMergeTreeDestroy(&pMerger->pSessMerger);
  }
  if (pMerger->pSessMerger == NULL) {
    int capacity = (numList + 7) / 8 * 8;
    code = tMergeTreeCreate(&pMerger->pSessMerger, capacity, pMerger->pMetaLists, stwmMetaListCompareSess);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    code = tMergeTreeRebuild(pMerger->pSessMerger);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static SSTriggerWalMetaList *stwmGetSessWinner(SSTriggerWalMetaMerger *pMerger) {
  if (IS_TRIGGER_WAL_META_SESS_MERGER_INVALID(pMerger)) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

  int32_t idx = tMergeTreeGetChosenIndex(pMerger->pSessMerger);
  return taosArrayGet(pMerger->pMetaLists, idx);
}

static int32_t stwmAdjustSessWinner(SSTriggerWalMetaMerger *pMerger) {
  if (IS_TRIGGER_WAL_META_SESS_MERGER_INVALID(pMerger)) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t idx = tMergeTreeGetAdjustIndex(pMerger->pSessMerger);
  return tMergeTreeAdjust(pMerger->pSessMerger, idx);
}

static int32_t stwmMetaListCompareData(const void *pLeft, const void *pRight, void *param) {
  int32_t left = *(const int32_t *)pLeft;
  int32_t right = *(const int32_t *)pRight;
  SArray *pMetaLists = (SArray *)param;

  if (left < taosArrayGetSize(pMetaLists) && right < taosArrayGetSize(pMetaLists)) {
    SSTriggerWalMetaList *pLeft = taosArrayGet(pMetaLists, left);
    SSTriggerWalMetaList *pRight = taosArrayGet(pMetaLists, right);
    if (pLeft->nextTs < pRight->nextTs) {
      return -1;
    } else if (pLeft->nextTs > pRight->nextTs) {
      return 1;
    } else if (pLeft->nextTs != INT64_MAX) {
      int64_t verLeft = pLeft->head->pMeta->ver;
      int64_t verRight = pRight->head->pMeta->ver;
      return verRight - verLeft;
    }
  }
  return left - right;
}

static int32_t stwmMetaListSkip2Ts(SSTriggerWalMetaMerger *pMerger, SSTriggerWalMetaList *pList, int64_t start) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pMerger->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  while (pList->head != NULL) {
    if (pList->head->pMeta->ekey >= start) {
      break;
    }
    pList->head = pList->head->next;
    pList->nextIdx = -1;
    if (pList->pDataBlock != NULL) {
      blockDataDestroy(pList->pDataBlock);
      pList->pDataBlock = NULL;
    }
  }

  if (pList->head == NULL) {
    pList->nextTs = INT64_MAX;
    goto _end;
  }

  if (pList->pDataBlock == NULL) {
    pList->nextTs = TMAX(pList->head->pMeta->skey, start);
  } else {
    int32_t          nrows = blockDataGetNumOfRows(pList->pDataBlock);
    SColumnInfoData *pTsCol = taosArrayGet(pList->pDataBlock->pDataBlock, pMerger->tsSlotId);
    while (pList->nextIdx < nrows) {
      int64_t ts = *(int64_t *)colDataGetNumData(pTsCol, pList->nextIdx);
      if (ts >= start) {
        pList->nextTs = ts;
        break;
      }
      ++pList->nextIdx;
    }
    QUERY_CHECK_CONDITION(pList->nextIdx < nrows, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stwmBuildDataMerger(SSTriggerWalMetaMerger *pMerger, int64_t skey, int64_t ekey) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pMerger->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  QUERY_CHECK_CONDITION(IS_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger), code, lino, _end, TSDB_CODE_INVALID_PARA);

  if (IS_TRIGGER_WAL_META_MERGER_EMPTY(pMerger) || skey > ekey) {
    SET_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger);
    goto _end;
  }

  pMerger->dataReadRange = (STimeWindow){.skey = skey, .ekey = ekey};

  int32_t numList = taosArrayGetSize(pMerger->pMetaLists);
  for (int32_t i = 0; i < numList; ++i) {
    SSTriggerWalMetaList *pList = taosArrayGet(pMerger->pMetaLists, i);
    code = stwmMetaListSkip2Ts(pMerger, pList, skey);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (pMerger->pDataMerger && pMerger->pDataMerger->numOfSources < numList) {
    tMergeTreeDestroy(&pMerger->pDataMerger);
  }
  if (pMerger->pDataMerger == NULL) {
    int capacity = (numList + 7) / 8 * 8;
    code = tMergeTreeCreate(&pMerger->pDataMerger, capacity, pMerger->pMetaLists, stwmMetaListCompareData);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    code = tMergeTreeRebuild(pMerger->pDataMerger);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static SSTriggerWalMetaList *stwmGetDataWinner(SSTriggerWalMetaMerger *pMerger) {
  if (IS_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger)) {
    return NULL;
  }

  int32_t               idx = tMergeTreeGetChosenIndex(pMerger->pDataMerger);
  SSTriggerWalMetaList *pList = taosArrayGet(pMerger->pMetaLists, idx);

  if (pList->nextTs > pMerger->dataReadRange.ekey) {
    SET_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger);
    return NULL;
  }

  // set the reading range for winner to avoid row-by-row comparison
  if (taosArrayGetSize(pMerger->pMetaLists) == 1) {
    pMerger->stepReadRange = pMerger->dataReadRange;
  } else {
    int32_t               idx2 = pMerger->pDataMerger->pNode[1].index;
    SSTriggerWalMetaList *pList2 = taosArrayGet(pMerger->pMetaLists, idx2);
    pMerger->stepReadRange.skey = pMerger->dataReadRange.skey;
    if (pList->nextTs == pList2->nextTs) {
      pMerger->stepReadRange.ekey = pList->nextTs;
    } else {
      pMerger->stepReadRange.ekey = TMIN(pList2->nextTs - 1, pMerger->dataReadRange.ekey);
    }
  }
  return pList;
}

static int32_t stwmAdjustDataWinner(SSTriggerWalMetaMerger *pMerger) {
  if (IS_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger)) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t idx = tMergeTreeGetAdjustIndex(pMerger->pDataMerger);
  return tMergeTreeAdjust(pMerger->pDataMerger, idx);
}

static int32_t stwmMetaListNextData(SSTriggerWalMetaMerger *pMerger, SSTriggerWalMetaList *pList,
                                    SSDataBlock **ppDataBlock, int32_t *pStartIdx, int32_t *pEndIdx, bool *pNeedFetch,
                                    bool *pNeedFree) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pMerger->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  QUERY_CHECK_CONDITION(!IS_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger), code, lino, _end, TSDB_CODE_INVALID_PARA);

  *ppDataBlock = NULL;
  *pNeedFetch = false;
  *pNeedFree = false;

  if (pList->nextTs < pMerger->stepReadRange.skey) {
    code = stwmMetaListSkip2Ts(pMerger, pList, pMerger->stepReadRange.skey);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (pList->nextTs > pMerger->stepReadRange.ekey) {
    goto _end;
  }

  if (pList->pDataBlock == NULL) {
    *pNeedFetch = true;
    goto _end;
  }

  QUERY_CHECK_CONDITION(pList->nextIdx >= 0, code, lino, _end, TSDB_CODE_INVALID_PARA);

  int32_t          nrows = blockDataGetNumOfRows(pList->pDataBlock);
  SColumnInfoData *pTsCol = taosArrayGet(pList->pDataBlock->pDataBlock, pMerger->tsSlotId);
  *ppDataBlock = pList->pDataBlock;
  *pStartIdx = pList->nextIdx;
  *pEndIdx = (*pStartIdx) + 1;
  while (*pEndIdx < nrows) {
    int64_t ts = *(int64_t *)colDataGetNumData(pTsCol, *pEndIdx);
    if (ts > pMerger->stepReadRange.ekey) {
      break;
    }
    ++(*pEndIdx);
  }

  int64_t lastTs = *(int64_t *)colDataGetNumData(pTsCol, (*pEndIdx) - 1);
  pMerger->dataReadRange.skey = lastTs + 1;

  if (*pEndIdx >= nrows) {
    pList->head = pList->head->next;
    pList->nextTs = (pList->head == NULL) ? INT64_MAX : pList->head->pMeta->skey;
    pList->nextIdx = -1;
    pList->pDataBlock = NULL;
    *pNeedFree = true;
  } else {
    pList->nextTs = *(int64_t *)colDataGetNumData(pTsCol, *pEndIdx);
    pList->nextIdx = *pEndIdx;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stwmBindDataBlock(SSTriggerWalMetaMerger *pMerger, SSDataBlock *pDataBlock) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pMerger->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  QUERY_CHECK_CONDITION(!IS_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger), code, lino, _end, TSDB_CODE_INVALID_PARA);

  int32_t               idx = tMergeTreeGetChosenIndex(pMerger->pDataMerger);
  SSTriggerWalMetaList *pList = taosArrayGet(pMerger->pMetaLists, idx);
  QUERY_CHECK_CONDITION(pList->pDataBlock == NULL, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pList->head, code, lino, _end, TSDB_CODE_INVALID_PARA);

  int32_t nrows = blockDataGetNumOfRows(pDataBlock);
  if (nrows <= 0) {
    SET_TRIGGER_META_DATA_EMPTY(pList->head->pMeta);

    pList->head = pList->head->next;
    pList->nextTs = (pList->head == NULL) ? INT64_MAX : pList->head->pMeta->skey;
    pList->nextIdx = -1;
    pList->pDataBlock = NULL;
    blockDataDestroy(pDataBlock);
  } else {
    SColumnInfoData  *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pMerger->tsSlotId);
    SSTriggerWalMeta *pMeta = pList->head->pMeta;
    if (IS_TRIGGER_META_NROW_INACCURATE(pMeta)) {
      // update accurate meta info, which helps with subsequent data merging
      int64_t skey = *(int64_t *)colDataGetNumData(pTsCol, 0);
      int64_t ekey = *(int64_t *)colDataGetNumData(pTsCol, nrows - 1);
      QUERY_CHECK_CONDITION(skey >= pMeta->skey, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      QUERY_CHECK_CONDITION(ekey <= pMeta->ekey, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pMeta->skey = skey;
      pMeta->ekey = ekey;
      pMeta->nrows = nrows;
    }

    for (pList->nextIdx = 0; pList->nextIdx < nrows; ++pList->nextIdx) {
      int64_t ts = *(int64_t *)colDataGetNumData(pTsCol, pList->nextIdx);
      if (ts >= pList->nextTs) {
        pList->nextTs = ts;
        break;
      }
    }
    QUERY_CHECK_CONDITION(pList->nextIdx < nrows, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    pList->pDataBlock = pDataBlock;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stwmMetaListSkipNrow(SSTriggerWalMetaMerger *pMerger, SSTriggerWalMetaList *pList, int32_t nrowsToSkip,
                                    int32_t *pSkipped, int64_t *pLastTs, bool *pNeedFetch) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pMerger->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  QUERY_CHECK_CONDITION(!IS_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger), code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(nrowsToSkip > 0, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *pSkipped = 0;
  *pNeedFetch = false;
  *pLastTs = INT64_MIN;

  if (pList->nextTs < pMerger->stepReadRange.skey) {
    code = stwmMetaListSkip2Ts(pMerger, pList, pMerger->stepReadRange.skey);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (pList->nextTs > pMerger->stepReadRange.ekey) {
    goto _end;
  }

  if (pList->pDataBlock == NULL) {
    SSTriggerWalMeta *pMeta = pList->head->pMeta;
    if (!IS_TRIGGER_META_NROW_INACCURATE(pMeta) && pMeta->ekey <= pMerger->stepReadRange.ekey &&
        pMeta->nrows <= nrowsToSkip && pMerger->useMetaAccel) {
      *pSkipped = pMeta->nrows;

      int64_t lastTs = pMeta->ekey;
      pMerger->dataReadRange.skey = lastTs + 1;
      *pLastTs = lastTs;

      pList->head = pList->head->next;
      pList->nextTs = (pList->head == NULL) ? INT64_MAX : pList->head->pMeta->skey;
      pList->nextIdx = -1;
      pList->pDataBlock = NULL;
    } else {
      *pNeedFetch = true;
    }
    goto _end;
  }

  int32_t          nrows = blockDataGetNumOfRows(pList->pDataBlock);
  SColumnInfoData *pTsCol = taosArrayGet(pList->pDataBlock->pDataBlock, pMerger->tsSlotId);
  int32_t          nextIdx = pList->nextIdx;
  while (nextIdx < nrows && *pSkipped < nrowsToSkip) {
    int64_t ts = *(int64_t *)colDataGetNumData(pTsCol, nextIdx);
    if (ts > pMerger->stepReadRange.ekey) {
      break;
    }
    ++nextIdx;
    ++(*pSkipped);
  }

  int64_t lastTs = *(int64_t *)colDataGetNumData(pTsCol, nextIdx - 1);
  pMerger->dataReadRange.skey = lastTs + 1;
  *pLastTs = lastTs;

  if (nextIdx >= nrows) {
    pList->head = pList->head->next;
    pList->nextTs = (pList->head == NULL) ? INT64_MAX : pList->head->pMeta->skey;
    pList->nextIdx = -1;
    blockDataDestroy(pList->pDataBlock);
    pList->pDataBlock = NULL;
  } else {
    pList->nextTs = *(int64_t *)colDataGetNumData(pTsCol, nextIdx);
    pList->nextIdx = nextIdx;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t strtgInit(SSTriggerRealtimeGroup *pGroup, SSTriggerRealtimeContext *pContext, int64_t groupId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  pGroup->pContext = pContext;
  pGroup->groupId = groupId;
  pGroup->maxMetaDelta = 0;      // todo(kjq): adjust dynamically
  pGroup->minMetaThreshold = 1;  // todo(kjq): adjust dynamically
  pGroup->oldThreshold = INT64_MIN;
  pGroup->newThreshold = INT64_MIN;
  pGroup->pMetas = taosArrayInit(0, sizeof(SSTriggerWalMeta));
  QUERY_CHECK_NULL(pGroup->pMetas, code, lino, _end, terrno);
  pGroup->curWindow = (STimeWindow){.skey = INT64_MIN, .ekey = INT64_MIN};
  if (pContext->pTask->triggerType == STREAM_TRIGGER_COUNT) {
    TRINGBUF_INIT(&pGroup->wstartBuf);
    int32_t cap = (pContext->pTask->windowCount / pContext->pTask->windowSliding) + 1;
    code = TRINGBUF_RESERVE(&pGroup->wstartBuf, cap);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  return code;
}

static void strtgDestroy(void *ptr) {
  SSTriggerRealtimeGroup **ppGroup = ptr;
  if (ppGroup == NULL || *ppGroup == NULL) {
    return;
  }

  SSTriggerRealtimeGroup *pGroup = *ppGroup;
  if (pGroup->pContext->pTask->singleVnodePerGroup) {
    taosMemFreeClear(pGroup->pMetaStat);
  } else if (pGroup->pMetaStats != NULL) {
    tSimpleHashCleanup(pGroup->pMetaStats);
    pGroup->pMetaStats = NULL;
  }
  if (pGroup->pMetas) {
    taosArrayDestroy(pGroup->pMetas);
    pGroup->pMetas = NULL;
  }
  if (pGroup->pContext->pTask->triggerType == STREAM_TRIGGER_COUNT) {
    TRINGBUF_DESTROY(&pGroup->wstartBuf);
  } else if (pGroup->pContext->pTask->triggerType == STREAM_TRIGGER_STATE) {
    if (IS_VAR_DATA_TYPE(pGroup->stateVal.type)) {
      taosMemoryFreeClear(pGroup->stateVal.pData);
    }
  }

  taosMemFreeClear(*ppGroup);
}

static SSTriggerWalMetaStat *strtgGetMetaStat(SSTriggerRealtimeGroup *pGroup, int64_t vgroupId) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;
  SSTriggerWalMetaStat     *pStat = NULL;

  if (pTask->singleVnodePerGroup) {
    if (pGroup->pMetaStat == NULL) {
      pGroup->pMetaStat = taosMemCalloc(1, sizeof(SSTriggerWalMetaStat));
      QUERY_CHECK_NULL(pGroup->pMetaStat, code, lino, _end, terrno);
      pGroup->pMetaStat->vgroupId = vgroupId;
      pGroup->pMetaStat->threshold = INT64_MIN;
    }
    pStat = pGroup->pMetaStat;
  } else {
    if (pGroup->pMetaStats == NULL) {
      pGroup->pMetaStats = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
      QUERY_CHECK_NULL(pGroup->pMetaStats, code, lino, _end, terrno);
    }
    pStat = tSimpleHashGet(pGroup->pMetaStats, &vgroupId, sizeof(int64_t));
    if (pStat == NULL) {
      SSTriggerWalMetaStat metaStat = {.vgroupId = vgroupId, .threshold = INT64_MIN};
      code = tSimpleHashPut(pGroup->pMetaStats, &vgroupId, sizeof(int64_t), &metaStat, sizeof(metaStat));
      QUERY_CHECK_CODE(code, lino, _end);
      pStat = tSimpleHashGet(pGroup->pMetaStats, &vgroupId, sizeof(int64_t));
      QUERY_CHECK_NULL(pStat, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    }
  }
  QUERY_CHECK_CONDITION(pStat->vgroupId == vgroupId, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    terrno = code;
    pStat = NULL;
  }
  return pStat;
}

static int32_t strtgDelMetaInRange(SSTriggerRealtimeGroup *pGroup, int64_t uid, int64_t skey, int64_t ekey,
                                   int32_t *pNumAdded) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  *pNumAdded = 0;

  int32_t origSize = taosArrayGetSize(pGroup->pMetas);
  if (pGroup->metaIdx >= origSize) {
    goto _end;
  }

  for (int32_t i = pGroup->metaIdx; i < taosArrayGetSize(pGroup->pMetas); ++i) {
    SSTriggerWalMeta *pMeta = taosArrayGet(pGroup->pMetas, i);
    if (pMeta->uid != uid || pMeta->skey > ekey || pMeta->ekey < skey) {
      continue;
    } else if (pMeta->skey >= skey) {
      pMeta->skey = ekey + 1;
      SET_TRIGGER_META_SKEY_INACCURATE(pMeta);
    } else if (pMeta->ekey <= ekey) {
      pMeta->ekey = skey - 1;
      SET_TRIGGER_META_EKEY_INACCURATE(pMeta);
    } else {
      SSTriggerWalMeta *px = taosArrayPush(pGroup->pMetas, pMeta);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      pMeta->ekey = skey - 1;
      SET_TRIGGER_META_EKEY_INACCURATE(pMeta);
      px->skey = ekey + 1;
      SET_TRIGGER_META_SKEY_INACCURATE(px);
      (*pNumAdded)++;
    }
  }

  int32_t j = pGroup->metaIdx;
  for (int32_t i = pGroup->metaIdx; i < taosArrayGetSize(pGroup->pMetas); ++i) {
    SSTriggerWalMeta *pMeta = taosArrayGet(pGroup->pMetas, i);
    if (pMeta->skey > pMeta->ekey) {
      continue;
    }
    if (i != j) {
      SSTriggerWalMeta *px = taosArrayGet(pGroup->pMetas, j);
      *px = *pMeta;
    }
    ++j;
  }
  TARRAY_SIZE(pGroup->pMetas) = j;
  *pNumAdded = j - origSize;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t strtgAddNewMeta(SSTriggerRealtimeGroup *pGroup) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;
  SSDataBlock              *pWalMetaData = pGroup->pWalMetaData;

  QUERY_CHECK_NULL(pWalMetaData, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(blockDataGetNumOfCols(pWalMetaData) == 7, code, lino, _end, TSDB_CODE_INVALID_PARA);

  SStreamTaskAddr *pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
  QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);

  pGroup->vgId = pReader->nodeId;

  SSTriggerWalMetaStat *pStat = strtgGetMetaStat(pGroup, pReader->nodeId);
  QUERY_CHECK_NULL(pStat, code, lino, _end, terrno);

  int32_t          numNewMeta = blockDataGetNumOfRows(pWalMetaData);
  int32_t          iCol = 0;
  SColumnInfoData *pTypeCol = taosArrayGet(pWalMetaData->pDataBlock, iCol++);
  SColumnInfoData *pGidCol = taosArrayGet(pWalMetaData->pDataBlock, iCol++);
  SColumnInfoData *pUidCol = taosArrayGet(pWalMetaData->pDataBlock, iCol++);
  SColumnInfoData *pSkeyCol = taosArrayGet(pWalMetaData->pDataBlock, iCol++);
  SColumnInfoData *pEkeyCol = taosArrayGet(pWalMetaData->pDataBlock, iCol++);
  SColumnInfoData *pVerCol = taosArrayGet(pWalMetaData->pDataBlock, iCol++);
  SColumnInfoData *pNrowsCol = taosArrayGet(pWalMetaData->pDataBlock, iCol++);

  for (int32_t i = 0; i < numNewMeta; ++i) {
    // todo(kjq): fix the check condition for virtual tables
    int64_t gid = *(int64_t *)colDataGetNumData(pGidCol, i);
    bool    isValid = (gid == pGroup->groupId);
    if (isValid) {
      int64_t skey = *(int64_t *)colDataGetNumData(pSkeyCol, i);
      int64_t ekey = *(int64_t *)colDataGetNumData(pEkeyCol, i);
      if (skey <= pStat->threshold) {
        code = stTriggerTaskMarkRecalc(pTask, pGroup->groupId, skey, TMIN(ekey, pStat->threshold));
        QUERY_CHECK_CODE(code, lino, _end);
      }
      if (ekey > pStat->threshold) {
        skey = TMAX(skey, pStat->threshold + 1);
        int64_t uid = *(int64_t *)colDataGetNumData(pUidCol, i);
        int8_t  type = *(int8_t *)colDataGetNumData(pTypeCol, i);
        if (type == WAL_DELETE_DATA) {
          int32_t numAdded = 0;
          code = strtgDelMetaInRange(pGroup, uid, skey, ekey, &numAdded);
          QUERY_CHECK_CODE(code, lino, _end);
          pStat->numHoldMetas += numAdded;
        } else if (type == WAL_SUBMIT_DATA) {
          SSTriggerWalMeta *pMeta = taosArrayReserve(pGroup->pMetas, 1);
          QUERY_CHECK_NULL(pMeta, code, lino, _end, terrno);
          pMeta->vgId = pReader->nodeId;
          pMeta->uid = uid;
          pMeta->skey = skey;
          pMeta->ekey = ekey;
          pMeta->ver = *(int64_t *)colDataGetNumData(pVerCol, i);
          if (skey != *(int64_t *)colDataGetNumData(pSkeyCol, i)) {
            SET_TRIGGER_META_SKEY_INACCURATE(pMeta);
          } else {
            pMeta->nrows = *(int64_t *)colDataGetNumData(pNrowsCol, i);
          }
          pStat->threshold = TMAX(pStat->threshold, ekey);
          pStat->numHoldMetas++;
        }
      }
    }
  }

  pGroup->pWalMetaData = NULL;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t strtgUpdateThreshold(SSTriggerRealtimeGroup *pGroup) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  if (pTask->singleVnodePerGroup) {
    QUERY_CHECK_NULL(pGroup->pMetaStat, code, lino, _end, TSDB_CODE_INVALID_PARA);
    if (pGroup->pMetaStat->numHoldMetas > 0) {
      pGroup->newThreshold = pGroup->pMetaStat->threshold;
    }
  } else {
    QUERY_CHECK_NULL(pGroup->pMetaStats, code, lino, _end, TSDB_CODE_INVALID_PARA);
    int32_t               iter = 0;
    SSTriggerWalMetaStat *pStat = tSimpleHashIterate(pGroup->pMetaStats, NULL, &iter);
    int32_t               maxNumHold = 0;
    while (pStat != NULL) {
      maxNumHold = TMAX(maxNumHold, pStat->numHoldMetas);
      pStat = tSimpleHashIterate(pGroup->pMetaStats, pStat, &iter);
    }
    if (maxNumHold == 0) {
      goto _end;
    }
    int32_t numHoldThreshold = maxNumHold - pGroup->maxMetaDelta;
    bool    holdAllVnodes = (tSimpleHashGetSize(pGroup->pMetaStats) == taosArrayGetSize(pTask->readerList));
    if (numHoldThreshold > 0 || holdAllVnodes) {
      pGroup->newThreshold = INT64_MAX;
      iter = 0;
      pStat = tSimpleHashIterate(pGroup->pMetaStats, NULL, &iter);
      while (pStat != NULL) {
        if (pStat->numHoldMetas >= numHoldThreshold) {
          pGroup->newThreshold = TMIN(pGroup->newThreshold, pStat->threshold);
        }
        pStat = tSimpleHashIterate(pGroup->pMetaStats, pStat, &iter);
      }
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t strtgCompareMeta(const void *pLeft, const void *pRight) {
  const SSTriggerWalMeta *left = pLeft;
  const SSTriggerWalMeta *right = pRight;
  if (left->ekey < right->ekey) {
    return -1;
  } else if (left->ekey > right->ekey) {
    return 1;
  } else {
    return 0;
  }
}

static int32_t strtgSearchMeta(const void *pLeft, const void *pRight) {
  int64_t                 watermark = *(int64_t *)pLeft;
  const SSTriggerWalMeta *right = pRight;
  if (watermark < right->ekey) {
    return -1;
  } else if (watermark > right->ekey) {
    return 1;
  } else {
    return 0;
  }
}

static STimeWindow strtgGetIntervalWindow(const SInterval *pInterval, int64_t ts) {
  STimeWindow win;
  win.skey = taosTimeTruncate(ts, pInterval);
  win.ekey = taosTimeGetIntervalEnd(win.skey, pInterval);
  if (win.ekey < win.skey) {
    win.ekey = INT64_MAX;
  }
  return win;
}

static void strtgNextIntervalWindow(const SInterval *pInterval, STimeWindow *pWindow) {
  TSKEY nextStart =
      taosTimeAdd(pWindow->skey, -1 * pInterval->offset, pInterval->offsetUnit, pInterval->precision, NULL);
  nextStart = taosTimeAdd(nextStart, pInterval->sliding, pInterval->slidingUnit, pInterval->precision, NULL);
  nextStart = taosTimeAdd(nextStart, pInterval->offset, pInterval->offsetUnit, pInterval->precision, NULL);
  pWindow->skey = nextStart;
  pWindow->ekey = taosTimeGetIntervalEnd(nextStart, pInterval);
}

static STimeWindow strtgGetPeriodWindow(const SInterval *pInterval, int64_t ts) {
  int64_t day = convertTimePrecision(24 * 60 * 60 * 1000, TSDB_TIME_PRECISION_MILLI, pInterval->precision);
  // truncate to the start of day
  SInterval interval = {.intervalUnit = 'd',
                        .slidingUnit = 'd',
                        .offsetUnit = pInterval->offsetUnit,
                        .precision = pInterval->precision,
                        .offset = 0};
  interval.interval = day;
  interval.sliding = day;
  int64_t     first = taosTimeTruncate(ts, &interval) + pInterval->offset;
  STimeWindow win;
  if (pInterval->sliding > day) {
    if (first > ts) {
      win.skey = first - pInterval->sliding;
      win.ekey = first - 1;
    } else {
      win.skey = first;
      win.ekey = first + pInterval->sliding - 1;
    }
  } else {
    if (first > ts) {
      int64_t prev = first - day;
      win.skey = (ts - prev) / pInterval->sliding * pInterval->sliding + prev;
      win.ekey = first - 1;
    } else {
      win.skey = (ts - first) / pInterval->sliding * pInterval->sliding + first;
      win.ekey = win.skey + pInterval->sliding - 1;
    }
  }
  return win;
}

static void strtgNextPeriodWindow(const SInterval *pInterval, STimeWindow *pWindow) {
  int64_t day = convertTimePrecision(24 * 60 * 60 * 1000, TSDB_TIME_PRECISION_MILLI, pInterval->precision);
  if (pInterval->sliding > day) {
    pWindow->skey += pInterval->sliding;
    pWindow->ekey += pInterval->sliding;
  } else {
    pWindow->skey = pWindow->ekey + 1;
    pWindow->ekey += pInterval->sliding;
    // truncate to the start of day
    SInterval interval = {.intervalUnit = 'd',
                          .slidingUnit = 'd',
                          .offsetUnit = pInterval->offsetUnit,
                          .precision = pInterval->precision,
                          .offset = 0};
    interval.interval = day;
    interval.sliding = day;
    int64_t first = taosTimeTruncate(pWindow->ekey + 1, &interval) + pInterval->offset;
    if (first > pWindow->skey && first <= pWindow->ekey) {
      pWindow->ekey = first - 1;
    }
  }
}

static int32_t strtgOpenNewWindow(SSTriggerRealtimeGroup *pGroup, int64_t ts, char *pExtraNotifyContent) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;
  STimeWindow              *pWindow = &pGroup->curWindow;

  QUERY_CHECK_CONDITION(pGroup->winStatus != STRIGGER_WINDOW_OPENED, code, lino, _end, TSDB_CODE_INVALID_PARA);

  switch (pTask->triggerType) {
    case STREAM_TRIGGER_PERIOD: {
      SInterval *pInterval = &pTask->interval;
      if (pGroup->winStatus == STRIGGER_WINDOW_INITIALIZED) {
        *pWindow = strtgGetPeriodWindow(pInterval, ts);
      } else {
        QUERY_CHECK_CONDITION(pWindow->ekey < ts, code, lino, _end, TSDB_CODE_INVALID_PARA);
        strtgNextPeriodWindow(pInterval, pWindow);
      }
      break;
    }
    case STREAM_TRIGGER_SLIDING: {
      SInterval *pInterval = &pTask->interval;
      if (pGroup->winStatus == STRIGGER_WINDOW_INITIALIZED) {
        if (pInterval->interval > 0) {
          *pWindow = strtgGetIntervalWindow(pInterval, ts);
        } else {
          *pWindow = strtgGetPeriodWindow(pInterval, ts);
        }
      } else {
        QUERY_CHECK_CONDITION(pWindow->ekey < ts, code, lino, _end, TSDB_CODE_INVALID_PARA);
        if (pInterval->interval > 0) {
          strtgNextIntervalWindow(pInterval, pWindow);
        } else {
          strtgNextPeriodWindow(pInterval, pWindow);
        }
      }
      QUERY_CHECK_CONDITION(pWindow->skey <= ts && pWindow->ekey >= ts, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      break;
    }
    case STREAM_TRIGGER_SESSION:
    case STREAM_TRIGGER_COUNT:
    case STREAM_TRIGGER_STATE:
    case STREAM_TRIGGER_EVENT: {
      pWindow->skey = ts;
      pWindow->ekey = ts;
      break;
    }

    default: {
      ST_TASK_ELOG("invalid stream trigger type %d", pTask->triggerType);
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  SSTriggerCalcParam param = {
      .currentTs = pWindow->skey + 1,
      .wstart = pWindow->skey,
      .wend = pWindow->ekey,
      .wduration = pWindow->ekey - pWindow->skey,
      .wrownum = 0,
      .triggerTime = taosGetTimestampNs(),
      .notifyType = (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_OPEN),
      .extraNotifyContent = pExtraNotifyContent,
  };
  if (pTask->triggerType == STREAM_TRIGGER_SLIDING) {
    param.wend++;
    param.wduration++;
  }
  if (pTask->calcEventType & STRIGGER_EVENT_WINDOW_OPEN) {
    SSTriggerCalcRequest *pReq = &pContext->calcReq;
    if (pContext->calcStatus == STRIGGER_REQUEST_IDLE) {
      pContext->calcStatus = STRIGGER_REQUEST_TO_RUN;
      pContext->pCalcGroup = pGroup;
    } else {
      QUERY_CHECK_CONDITION(pContext->calcStatus == STRIGGER_REQUEST_TO_RUN && pContext->pCalcGroup == pGroup, code,
                            lino, _end, TSDB_CODE_INTERNAL_ERROR);
    }
    void *px = taosArrayPush(pReq->params, &param);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
  } else if (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_OPEN) {
    void *px = taosArrayPush(pContext->pNotifyParams, &param);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
  } else {
    QUERY_CHECK_CONDITION(pExtraNotifyContent == NULL, code, lino, _end, TSDB_CODE_INVALID_PARA);
  }

  pGroup->winStatus = STRIGGER_WINDOW_OPENED;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t strtgCloseCurrentWindow(SSTriggerRealtimeGroup *pGroup, char *pExtraNotifyContent) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;
  STimeWindow              *pWindow = &pGroup->curWindow;

  if (pGroup->winStatus != STRIGGER_WINDOW_OPENED) {
    goto _end;
  }

  SSTriggerCalcParam param = {
      .currentTs = pWindow->ekey + 1,
      .wstart = pWindow->skey,
      .wend = pWindow->ekey,
      .wduration = pWindow->ekey - pWindow->skey,
      .wrownum = (pTask->triggerType == STREAM_TRIGGER_COUNT) ? pTask->windowCount : pGroup->nrowsInWindow,
      .triggerTime = taosGetTimestampNs(),
      .notifyType = (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_CLOSE),
      .extraNotifyContent = pExtraNotifyContent,
  };
  if (pTask->triggerType == STREAM_TRIGGER_SLIDING) {
    param.wend++;
    param.wduration++;
  }
  if (pTask->triggerType == STREAM_TRIGGER_SLIDING && pTask->interval.interval == 0) {
    param.prevTs = param.currentTs - pTask->interval.sliding;
    param.nextTs = param.currentTs + pTask->interval.sliding;
  }
  if (pTask->calcEventType & STRIGGER_EVENT_WINDOW_CLOSE) {
    SSTriggerCalcRequest *pReq = &pContext->calcReq;
    if (pContext->calcStatus == STRIGGER_REQUEST_IDLE) {
      pContext->calcStatus = STRIGGER_REQUEST_TO_RUN;
      pContext->pCalcGroup = pGroup;
    } else {
      QUERY_CHECK_CONDITION(pContext->calcStatus == STRIGGER_REQUEST_TO_RUN && pContext->pCalcGroup == pGroup, code,
                            lino, _end, TSDB_CODE_INTERNAL_ERROR);
    }
    void *px = taosArrayPush(pReq->params, &param);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
  } else if (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_CLOSE) {
    void *px = taosArrayPush(pContext->pNotifyParams, &param);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
  } else {
    QUERY_CHECK_CONDITION(pExtraNotifyContent == NULL, code, lino, _end, TSDB_CODE_INVALID_PARA);
  }

  pGroup->winStatus = STRIGGER_WINDOW_CLOSED;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t strtgDoCheck(SSTriggerRealtimeGroup *pGroup) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;
  SSTriggerCalcRequest     *pReq = &pContext->calcReq;
  int64_t                   endtime = INT64_MIN;
  SSDataBlock              *pDataBlock = NULL;
  int32_t                   startIdx = 0;
  int32_t                   endIdx = 0;
  bool                      needFetch = false;
  bool                      needFree = false;
  char                     *pExtraNotifyContent = NULL;

  QUERY_CHECK_CONDITION(pReq->gid == pGroup->groupId, code, lino, _end, TSDB_CODE_INVALID_PARA);

  switch (pTask->triggerType) {
    case STREAM_TRIGGER_SLIDING: {
      int64_t ts = INT64_MIN;
      switch (pGroup->winStatus) {
        case STRIGGER_WINDOW_INITIALIZED: {
          SSTriggerWalMeta *pMeta = taosArrayGet(pGroup->pMetas, 0);
          ts = pMeta->skey;
          for (int32_t i = 0; i < taosArrayGetSize(pGroup->pMetas); ++i) {
            ts = TMIN((pMeta + i)->skey, ts);
          }
          break;
        }
        case STRIGGER_WINDOW_OPENED: {
          ts = pGroup->curWindow.ekey;
          break;
        }
        case STRIGGER_WINDOW_CLOSED: {
          ts = pGroup->curWindow.ekey + 1;
          break;
        }
        default: {
          ST_TASK_ELOG("invalid stream trigger window status %d", pGroup->winStatus);
          code = TSDB_CODE_INVALID_PARA;
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
      while (ts <= pGroup->newThreshold) {
        if (pGroup->winStatus == STRIGGER_WINDOW_OPENED) {
          code = strtgCloseCurrentWindow(pGroup, NULL);
          QUERY_CHECK_CODE(code, lino, _end);
          if (taosArrayGetSize(pReq->params) >= pTask->calcParamLimit) {
            break;
          }
          ts = pGroup->curWindow.ekey + 1;
        } else {
          code = strtgOpenNewWindow(pGroup, ts, NULL);
          QUERY_CHECK_CODE(code, lino, _end);
          if (taosArrayGetSize(pReq->params) >= pTask->calcParamLimit) {
            break;
          }
          ts = pGroup->curWindow.ekey;
        }
      }
      if (pTask->fillHistory) {
        void *px = tSimpleHashGet(pTask->pHistoryCutoffTime, &pGroup->groupId, sizeof(int64_t));
        if (px != NULL && pGroup->newThreshold == *(int64_t *)px && pGroup->winStatus == STRIGGER_WINDOW_OPENED &&
            (pTask->calcEventType & STRIGGER_EVENT_WINDOW_CLOSE)) {
          STimeWindow       *pWindow = &pGroup->curWindow;
          SSTriggerCalcParam param = {
              .currentTs = pWindow->ekey + 1,
              .wstart = pWindow->skey,
              .wend = pWindow->ekey + 1,
              .wduration = pWindow->ekey + 1 - pWindow->skey,
              .wrownum = pGroup->nrowsInWindow,
              .triggerTime = taosGetTimestampNs(),
              .notifyType = (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_CLOSE),
              .extraNotifyContent = NULL,
          };
          pContext->calcStatus = STRIGGER_REQUEST_TO_RUN;
          pContext->pCalcGroup = pGroup;
          void *px = taosArrayPush(pReq->params, &param);
          QUERY_CHECK_NULL(px, code, lino, _end, terrno);
        }
      }
      endtime = TMIN(pGroup->newThreshold, ts);
      break;
    }
    case STREAM_TRIGGER_SESSION: {
      SSTriggerWalMetaMerger *pMerger = pContext->pMerger;
      if (IS_TRIGGER_WAL_META_MERGER_EMPTY(pMerger)) {
        // build session window merger
        SSTriggerWalMeta *pMetas = taosArrayGet(pGroup->pMetas, pGroup->metaIdx);
        int32_t           nMetas = taosArrayGetSize(pGroup->pMetas) - pGroup->metaIdx;
        code = stwmSetWalMetas(pMerger, pMetas, nMetas, pTask->primaryTsIndex);
        QUERY_CHECK_CODE(code, lino, _end);
        code = stwmBuildSessMerger(pMerger, pGroup->oldThreshold + 1, pGroup->newThreshold, pTask->gap);
        QUERY_CHECK_CODE(code, lino, _end);
      }

      while (true) {
        int64_t nextStart = INT64_MAX;
        if (IS_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger)) {
          // use metadata to determine windows first, reducing the need for data fetches
          while (pMerger->useMetaAccel) {
            SSTriggerWalMetaList *pSessWinner = stwmGetSessWinner(pMerger);
            QUERY_CHECK_NULL(pSessWinner, code, lino, _end, terrno);
            if (pGroup->curWindow.ekey + pTask->gap < pSessWinner->curSessWin.skey) {
              nextStart = pSessWinner->curSessWin.skey;
              break;
            } else {
              QUERY_CHECK_CONDITION(pGroup->winStatus == STRIGGER_WINDOW_OPENED, code, lino, _end,
                                    TSDB_CODE_INTERNAL_ERROR);
              pGroup->curWindow.ekey = TMAX(pGroup->curWindow.ekey, pSessWinner->curSessWin.ekey);
            }
            code = stwmMetaListNextSess(pMerger, pSessWinner, pTask->gap);
            QUERY_CHECK_CODE(code, lino, _end);
            code = stwmAdjustSessWinner(pMerger);
            QUERY_CHECK_CODE(code, lino, _end);
          }

          code = stwmBuildDataMerger(pMerger, pGroup->curWindow.ekey + 1, TMIN(pGroup->newThreshold, nextStart - 1));
          QUERY_CHECK_CODE(code, lino, _end);
        } else if (pMerger->useMetaAccel) {
          SSTriggerWalMetaList *pSessWinner = stwmGetSessWinner(pMerger);
          QUERY_CHECK_NULL(pSessWinner, code, lino, _end, terrno);
          nextStart = pSessWinner->curSessWin.skey;
        }

        bool hasRemain = false;

        while (true) {
          SSTriggerWalMetaList *pDataWinner = stwmGetDataWinner(pMerger);
          if (pDataWinner == NULL) {
            break;
          }
          while (true) {
            code = stwmMetaListNextData(pMerger, pDataWinner, &pDataBlock, &startIdx, &endIdx, &needFetch, &needFree);
            QUERY_CHECK_CODE(code, lino, _end);
            if (needFetch) {
              if (taosArrayGetSize(pReq->params) >= pTask->calcParamLimit ||
                  pGroup->curWindow.ekey + pTask->gap >= nextStart) {
                SET_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger);
                hasRemain = true;
                break;
              }
              code = strtcSendPullReq(pContext, STRIGGER_PULL_WAL_TS_DATA, pDataWinner->head->pMeta);
              QUERY_CHECK_CODE(code, lino, _end);
              pGroup->status = STRIGGER_GROUP_WAITING_TDATA;
              goto _end;
            }
            if (pDataBlock == NULL) {
              code = stwmAdjustDataWinner(pMerger);
              QUERY_CHECK_CODE(code, lino, _end);
              break;
            }
            SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pMerger->tsSlotId);
            for (int32_t i = startIdx; i < endIdx; ++i) {
              int64_t ts = *(int64_t *)colDataGetNumData(pTsCol, i);
              if (pGroup->curWindow.ekey + pTask->gap < ts) {
                code = strtgCloseCurrentWindow(pGroup, NULL);
                QUERY_CHECK_CODE(code, lino, _end);
                code = strtgOpenNewWindow(pGroup, ts, NULL);
                QUERY_CHECK_CODE(code, lino, _end);
              } else {
                QUERY_CHECK_CONDITION(pGroup->winStatus == STRIGGER_WINDOW_OPENED, code, lino, _end,
                                      TSDB_CODE_INTERNAL_ERROR);
                pGroup->curWindow.ekey = ts;
              }
            }
            if (needFree) {
              blockDataDestroy(pDataBlock);
              pDataBlock = NULL;
            }
          }
        }

        endtime = hasRemain ? pGroup->curWindow.ekey : pGroup->newThreshold;

        if (taosArrayGetSize(pReq->params) >= pTask->calcParamLimit) {
          // calculation requests is full
          break;
        } else if (nextStart != INT64_MAX) {
          if (pGroup->curWindow.ekey + pTask->gap < nextStart) {
            code = strtgCloseCurrentWindow(pGroup, NULL);
            QUERY_CHECK_CODE(code, lino, _end);
            code = strtgOpenNewWindow(pGroup, nextStart, NULL);
            QUERY_CHECK_CODE(code, lino, _end);
          }
          // go back to use metadata to extend the window
          continue;
        } else {
          if (pGroup->curWindow.ekey + pTask->gap < pGroup->newThreshold) {
            code = strtgCloseCurrentWindow(pGroup, NULL);
            QUERY_CHECK_CODE(code, lino, _end);
          }
          break;
        }
      }
      stwmClear(pMerger);
      break;
    }
    case STREAM_TRIGGER_COUNT: {
      SSTriggerWalMetaMerger *pMerger = pContext->pMerger;
      if (IS_TRIGGER_WAL_META_MERGER_EMPTY(pMerger)) {
        SSTriggerWalMeta *pMetas = taosArrayGet(pGroup->pMetas, pGroup->metaIdx);
        int32_t           nMetas = taosArrayGetSize(pGroup->pMetas) - pGroup->metaIdx;
        code = stwmSetWalMetas(pMerger, pMetas, nMetas, pTask->primaryTsIndex);
        QUERY_CHECK_CODE(code, lino, _end);
        code = stwmBuildDataMerger(pMerger, pGroup->oldThreshold + 1, pGroup->newThreshold);
        QUERY_CHECK_CODE(code, lino, _end);
      }

      bool hasRemain = false;

      while (true) {
        SSTriggerWalMetaList *pDataWinner = stwmGetDataWinner(pMerger);
        if (pDataWinner == NULL) {
          break;
        }
        while (true) {
          int32_t skipped = 0;
          int64_t lastTs = INT64_MIN;
          if (pGroup->nrowsInWindow >= pTask->windowCount) {
            code = strtgCloseCurrentWindow(pGroup, NULL);
            QUERY_CHECK_CODE(code, lino, _end);
            pGroup->nrowsInWindow -= pTask->windowSliding;
            if (!TRINGBUF_IS_EMPTY(&pGroup->wstartBuf)) {
              int64_t wstart = TRINGBUF_FIRST(&pGroup->wstartBuf);
              TRINGBUF_DEQUEUE(&pGroup->wstartBuf);
              code = strtgOpenNewWindow(pGroup, wstart, NULL);
              QUERY_CHECK_CODE(code, lino, _end);
            }
          } else {
#define ALIGN_UP(x, b) (((x) + (b) - 1) / (b) * (b))
            int32_t nrowsNextWstart = ALIGN_UP(pGroup->nrowsInWindow, pTask->windowSliding) + 1;
            int32_t nrowsToSkip = TMIN(nrowsNextWstart, pTask->windowCount) - pGroup->nrowsInWindow;
            code = stwmMetaListSkipNrow(pMerger, pDataWinner, nrowsToSkip, &skipped, &lastTs, &needFetch);
            QUERY_CHECK_CODE(code, lino, _end);
            if (needFetch) {
              if (taosArrayGetSize(pReq->params) >= pTask->calcParamLimit) {
                SET_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger);
                hasRemain = true;
                break;
              }
              code = strtcSendPullReq(pContext, STRIGGER_PULL_WAL_TS_DATA, pDataWinner->head->pMeta);
              QUERY_CHECK_CODE(code, lino, _end);
              pGroup->status = STRIGGER_GROUP_WAITING_TDATA;
              goto _end;
            }
            if (skipped == 0) {
              code = stwmAdjustDataWinner(pMerger);
              QUERY_CHECK_CODE(code, lino, _end);
              break;
            }
            pGroup->nrowsInWindow += skipped;
            pGroup->curWindow.ekey = lastTs;
            if (pGroup->winStatus != STRIGGER_WINDOW_OPENED) {
              code = strtgOpenNewWindow(pGroup, lastTs, NULL);
              QUERY_CHECK_CODE(code, lino, _end);
            } else if (pGroup->nrowsInWindow == nrowsNextWstart) {
              code = TRINGBUF_APPEND(&pGroup->wstartBuf, lastTs);
              QUERY_CHECK_CODE(code, lino, _end);
            }
          }
        }
      }

      endtime = hasRemain ? pGroup->curWindow.ekey : pGroup->newThreshold;
      stwmClear(pMerger);
      break;
    }
    case STREAM_TRIGGER_STATE: {
      SSTriggerWalMetaMerger *pMerger = pContext->pMerger;
      if (IS_TRIGGER_WAL_META_MERGER_EMPTY(pMerger)) {
        SSTriggerWalMeta *pMetas = taosArrayGet(pGroup->pMetas, pGroup->metaIdx);
        int32_t           nMetas = taosArrayGetSize(pGroup->pMetas) - pGroup->metaIdx;
        code = stwmSetWalMetas(pMerger, pMetas, nMetas, pTask->primaryTsIndex);
        QUERY_CHECK_CODE(code, lino, _end);
        code = stwmBuildDataMerger(pMerger, pGroup->oldThreshold + 1, pGroup->newThreshold);
        QUERY_CHECK_CODE(code, lino, _end);
      }

      bool hasRemain = false;

      while (true) {
        SSTriggerWalMetaList *pDataWinner = stwmGetDataWinner(pMerger);
        if (pDataWinner == NULL) {
          break;
        }
        while (true) {
          code = stwmMetaListNextData(pMerger, pDataWinner, &pDataBlock, &startIdx, &endIdx, &needFetch, &needFree);
          QUERY_CHECK_CODE(code, lino, _end);
          if (needFetch) {
            if (taosArrayGetSize(pReq->params) >= pTask->calcParamLimit) {
              SET_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger);
              hasRemain = true;
              break;
            }
            code = strtcSendPullReq(pContext, STRIGGER_PULL_WAL_TRIGGER_DATA, pDataWinner->head->pMeta);
            QUERY_CHECK_CODE(code, lino, _end);
            pGroup->status = STRIGGER_GROUP_WAITING_TDATA;
            goto _end;
          }
          if (pDataBlock == NULL) {
            code = stwmAdjustDataWinner(pMerger);
            QUERY_CHECK_CODE(code, lino, _end);
            break;
          }
          SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pMerger->tsSlotId);
          SColumnInfoData *pStateCol = taosArrayGet(pDataBlock->pDataBlock, pTask->stateColId);
          bool             isVarType = IS_VAR_DATA_TYPE(pStateCol->info.type);
          void            *pStateData = isVarType ? (void *)pGroup->stateVal.pData : (void *)&pGroup->stateVal.val;
          if (pGroup->winStatus == STRIGGER_WINDOW_INITIALIZED) {
            // initialize state value
            SValue *pStateVal = &pGroup->stateVal;
            pStateVal->type = pStateCol->info.type;
            if (isVarType) {
              pStateVal->nData = pStateCol->info.bytes;
              pStateVal->pData = taosMemoryCalloc(pStateVal->nData, 1);
              QUERY_CHECK_CONDITION(pStateVal->pData, code, lino, _end, terrno);
              pStateData = pStateVal->pData;
            }
            int64_t ts = *(int64_t *)colDataGetNumData(pTsCol, startIdx);
            char   *val = colDataGetData(pStateCol, startIdx);
            int32_t bytes = isVarType ? varDataTLen(val) : pStateCol->info.bytes;
            if (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_OPEN) {
              code = streamBuildStateNotifyContent(STRIGGER_EVENT_WINDOW_OPEN, pGroup->stateVal.type, NULL, val,
                                                   &pExtraNotifyContent);
              QUERY_CHECK_CODE(code, lino, _end);
            }
            code = strtgOpenNewWindow(pGroup, ts, pExtraNotifyContent);
            QUERY_CHECK_CODE(code, lino, _end);
            pExtraNotifyContent = NULL;
            memcpy(pStateData, val, bytes);
            pGroup->nrowsInWindow = 0;
          }
          for (int32_t i = startIdx; i < endIdx; ++i) {
            int64_t ts = *(int64_t *)colDataGetNumData(pTsCol, i);
            char   *val = colDataGetData(pStateCol, i);
            int32_t bytes = isVarType ? varDataTLen(val) : pStateCol->info.bytes;
            if (memcmp(pStateData, val, bytes) == 0) {
              pGroup->curWindow.ekey = ts;
            } else {
              if (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_CLOSE) {
                code = streamBuildStateNotifyContent(STRIGGER_EVENT_WINDOW_CLOSE, pGroup->stateVal.type, pStateData,
                                                     val, &pExtraNotifyContent);
                QUERY_CHECK_CODE(code, lino, _end);
              }
              code = strtgCloseCurrentWindow(pGroup, pExtraNotifyContent);
              QUERY_CHECK_CODE(code, lino, _end);
              pExtraNotifyContent = NULL;
              if (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_OPEN) {
                code = streamBuildStateNotifyContent(STRIGGER_EVENT_WINDOW_OPEN, pGroup->stateVal.type, pStateData, val,
                                                     &pExtraNotifyContent);
                QUERY_CHECK_CODE(code, lino, _end);
              }
              code = strtgOpenNewWindow(pGroup, ts, pExtraNotifyContent);
              QUERY_CHECK_CODE(code, lino, _end);
              pExtraNotifyContent = NULL;
              memcpy(pStateData, val, bytes);
              pGroup->nrowsInWindow = 0;
            }
            pGroup->nrowsInWindow++;
          }
          if (needFree) {
            blockDataDestroy(pDataBlock);
            pDataBlock = NULL;
          }
        }
      }

      endtime = hasRemain ? pGroup->curWindow.ekey : pGroup->newThreshold;
      stwmClear(pMerger);
      break;
    }
    case STREAM_TRIGGER_EVENT: {
      SSTriggerWalMetaMerger *pMerger = pContext->pMerger;
      if (IS_TRIGGER_WAL_META_MERGER_EMPTY(pMerger)) {
        SSTriggerWalMeta *pMetas = taosArrayGet(pGroup->pMetas, pGroup->metaIdx);
        int32_t           nMetas = taosArrayGetSize(pGroup->pMetas) - pGroup->metaIdx;
        code = stwmSetWalMetas(pMerger, pMetas, nMetas, pTask->primaryTsIndex);
        QUERY_CHECK_CODE(code, lino, _end);
        code = stwmBuildDataMerger(pMerger, pGroup->oldThreshold + 1, pGroup->newThreshold);
        QUERY_CHECK_CODE(code, lino, _end);
      }

      bool hasRemain = false;

      while (true) {
        SSTriggerWalMetaList *pDataWinner = stwmGetDataWinner(pMerger);
        if (pDataWinner == NULL) {
          break;
        }
        while (true) {
          code = stwmMetaListNextData(pMerger, pDataWinner, &pDataBlock, &startIdx, &endIdx, &needFetch, &needFree);
          QUERY_CHECK_CODE(code, lino, _end);
          if (needFetch) {
            if (taosArrayGetSize(pReq->params) >= pTask->calcParamLimit) {
              SET_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger);
              hasRemain = true;
              break;
            }
            code = strtcSendPullReq(pContext, STRIGGER_PULL_WAL_TRIGGER_DATA, pDataWinner->head->pMeta);
            QUERY_CHECK_CODE(code, lino, _end);
            pGroup->status = STRIGGER_GROUP_WAITING_TDATA;
            goto _end;
          }
          if (pDataBlock == NULL) {
            code = stwmAdjustDataWinner(pMerger);
            QUERY_CHECK_CODE(code, lino, _end);
            break;
          }
          SColumnInfoData *ps = NULL, *pe = NULL;
          SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pMerger->tsSlotId);
          for (int32_t i = startIdx; i < endIdx; ++i) {
            int64_t ts = *(int64_t *)colDataGetNumData(pTsCol, i);
            if (pGroup->winStatus != STRIGGER_WINDOW_OPENED) {
              if (ps == NULL) {
                SFilterColumnParam param = {.numOfCols = taosArrayGetSize(pDataBlock->pDataBlock),
                                            .pDataBlock = pDataBlock->pDataBlock};
                code = filterSetDataFromSlotId(pContext->pStartCond, &param);
                QUERY_CHECK_CODE(code, lino, _end);
                int32_t status = 0;
                code = filterExecute(pContext->pStartCond, pDataBlock, &ps, NULL, param.numOfCols, &status);
                QUERY_CHECK_CODE(code, lino, _end);
              }
              if (*(bool *)colDataGetNumData(ps, i)) {
                if (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_OPEN) {
                  code = streamBuildEventNotifyContent(pDataBlock, pTask->pStartCondCols, i, &pExtraNotifyContent);
                  QUERY_CHECK_CODE(code, lino, _end);
                }
                code = strtgOpenNewWindow(pGroup, ts, pExtraNotifyContent);
                QUERY_CHECK_CODE(code, lino, _end);
                pExtraNotifyContent = NULL;
                pGroup->nrowsInWindow = 1;
              }
            }
            if (pGroup->winStatus == STRIGGER_WINDOW_OPENED) {
              pGroup->curWindow.ekey = ts;
              if (pe == NULL) {
                SFilterColumnParam param = {.numOfCols = taosArrayGetSize(pDataBlock->pDataBlock),
                                            .pDataBlock = pDataBlock->pDataBlock};
                code = filterSetDataFromSlotId(pContext->pEndCond, &param);
                QUERY_CHECK_CODE(code, lino, _end);
                int32_t status = 0;
                code = filterExecute(pContext->pEndCond, pDataBlock, &pe, NULL, param.numOfCols, &status);
                QUERY_CHECK_CODE(code, lino, _end);
              }
              if (*(bool *)colDataGetNumData(pe, i)) {
                if (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_CLOSE) {
                  code = streamBuildEventNotifyContent(pDataBlock, pTask->pEndCondCols, i, &pExtraNotifyContent);
                  QUERY_CHECK_CODE(code, lino, _end);
                }
                code = strtgCloseCurrentWindow(pGroup, pExtraNotifyContent);
                QUERY_CHECK_CODE(code, lino, _end);
                pExtraNotifyContent = NULL;
                pGroup->nrowsInWindow = 0;
              } else {
                pGroup->nrowsInWindow++;
              }
            }
          }
          if (needFree) {
            blockDataDestroy(pDataBlock);
            pDataBlock = NULL;
          }
        }
      }

      endtime = hasRemain ? pGroup->curWindow.ekey : pGroup->newThreshold;
      stwmClear(pMerger);
      break;
    }
    default: {
      ST_TASK_ELOG("invalid stream trigger type %d", pTask->triggerType);
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  pGroup->oldThreshold = endtime;
  pGroup->metaIdx = taosArraySearchIdx(pGroup->pMetas, &endtime, strtgSearchMeta, TD_GT);
  if (pGroup->metaIdx < 0) {
    pGroup->metaIdx = taosArrayGetSize(pGroup->pMetas);
  }

  // if data caching is needed, metas will be held until send calculation request
  if (!pTask->needCacheData && !(pTask->needRowNumber && pTask->triggerType == STREAM_TRIGGER_SLIDING)) {
    taosArrayPopFrontBatch(pGroup->pMetas, pGroup->metaIdx);
    pGroup->metaIdx = 0;
  }

  pGroup->status = taosArrayGetSize(pReq->params) > 0 ? STRIGGER_GROUP_WAITING_CALC : STRIGGER_GROUP_WAITING_META;

_end:
  if (pDataBlock != NULL && needFree) {
    blockDataDestroy(pDataBlock);
  }
  if (pExtraNotifyContent != NULL) {
    taosMemoryFreeClear(pExtraNotifyContent);
  }
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t strtgResumeCheck(SSTriggerRealtimeGroup *pGroup) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  switch (pGroup->status) {
    case STRIGGER_GROUP_WAITING_META: {
      code = strtgAddNewMeta(pGroup);
      QUERY_CHECK_CODE(code, lino, _end);

      code = strtgUpdateThreshold(pGroup);
      QUERY_CHECK_CODE(code, lino, _end);
      if (pGroup->metaIdx < taosArrayGetSize(pGroup->pMetas)) {
        SSTriggerWalMeta *pMetas = taosArrayGet(pGroup->pMetas, pGroup->metaIdx);
        taosSort(pMetas, taosArrayGetSize(pGroup->pMetas) - pGroup->metaIdx, pGroup->pMetas->elemSize,
                 strtgCompareMeta);
      }

      // don't break, continue to the next case
    }
    case STRIGGER_GROUP_WAITING_CALC: {
      int32_t numToCheck = taosArrayGetSize(pGroup->pMetas);
      numToCheck -= pGroup->metaIdx;
      // todo(kjq):  check if the timeout expired
      if (numToCheck < pGroup->minMetaThreshold) {
        // not enough metas to check, wait for more metas
        pGroup->status = STRIGGER_GROUP_WAITING_META;
        goto _end;
      }

      if (pContext->calcStatus != STRIGGER_REQUEST_IDLE) {
        // calc is running, wait for it to finish
        pGroup->status = STRIGGER_GROUP_WAITING_CALC;
        goto _end;
      }
      SSTriggerCalcRequest *pReq = &pContext->calcReq;
      pReq->gid = pGroup->groupId;
      QUERY_CHECK_CONDITION(taosArrayGetSize(pReq->params) == 0, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

      // wait until the thread can send calc request
      code = streamTriggerAcquireCalcReq();
      QUERY_CHECK_CODE(code, lino, _end);

      // don't break, continue to the next case
    }
    case STRIGGER_GROUP_WAITING_TDATA: {
      code = strtgDoCheck(pGroup);
      QUERY_CHECK_CODE(code, lino, _end);

      break;
    }
    defaut: {
      ST_TASK_ELOG("invalid group status %d", pGroup->status);
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

#define SSTRIGGER_REALTIME_SESSIONID_PREFIX (1L << 16)
#define SSTRIGGER_HISTORY_SESSIONID_PREFIX  (2L << 16)
#define SSTRIGGER_RECALC_SESSIONID_PREFIX   (3L << 16)

static int32_t strtcInit(SSTriggerRealtimeContext *pContext, SStreamTriggerTask *pTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  pContext->pTask = pTask;
  pContext->sessionId = SSTRIGGER_REALTIME_SESSIONID_PREFIX | pTask->nextSessionId;
  pTask->nextSessionId++;

  pContext->pReaderWalProgress = tSimpleHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  QUERY_CHECK_NULL(pContext->pReaderWalProgress, code, lino, _end, terrno);
  int32_t numReaders = taosArrayGetSize(pTask->readerList);
  for (int32_t i = 0; i < numReaders; ++i) {
    SStreamTaskAddr     *pReader = taosArrayGet(pTask->readerList, i);
    SSTriggerWalProgress progress = {.pTaskAddr = pReader};
    code = tSimpleHashPut(pContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t), &progress,
                          sizeof(SSTriggerWalProgress));
    QUERY_CHECK_CODE(code, lino, _end);
  }
  pContext->curReaderIdx = -1;

  pContext->pGroups = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  QUERY_CHECK_NULL(pContext->pGroups, code, lino, _end, terrno);
  tSimpleHashSetFreeFp(pContext->pGroups, strtgDestroy);
  TRINGBUF_INIT(&pContext->groupsToCheck);
  TRINGBUF_INIT(&pContext->groupsMaxDelay);

  pContext->pMerger = taosMemoryCalloc(1, sizeof(SSTriggerWalMetaMerger));
  QUERY_CHECK_NULL(pContext->pMerger, code, lino, _end, terrno);
  code = stwmInit(pContext->pMerger, pContext);
  QUERY_CHECK_CODE(code, lino, _end);

  if (pTask->triggerType == STREAM_TRIGGER_EVENT) {
    code = filterInitFromNode(pTask->pStartCond, &pContext->pStartCond, 0, NULL);
    QUERY_CHECK_CODE(code, lino, _end);
    code = filterInitFromNode(pTask->pEndCond, &pContext->pEndCond, 0, NULL);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  pContext->pNotifyParams = taosArrayInit(0, sizeof(SSTriggerCalcParam));
  QUERY_CHECK_NULL(pContext->pNotifyParams, code, lino, _end, terrno);

  SSTriggerPullRequest *pPullReq = &pContext->pullReq.base;
  pPullReq->streamId = pTask->task.streamId;
  pPullReq->sessionId = pContext->sessionId;
  pPullReq->triggerTaskId = pTask->task.taskId;

  SSTriggerCalcRequest *pCalcReq = &pContext->calcReq;
  pCalcReq->streamId = pTask->task.streamId;
  pCalcReq->sessionId = pContext->sessionId;
  pCalcReq->triggerTaskId = pTask->task.taskId;
  pCalcReq->triggerType = pTask->triggerType;
  pCalcReq->params = taosArrayInit(0, sizeof(SSTriggerCalcParam));
  QUERY_CHECK_NULL(pCalcReq->params, code, lino, _end, terrno);

_end:
  return code;
}

static void strtcDestroy(void *ptr) {
  SSTriggerRealtimeContext **ppContext = ptr;
  if (ppContext == NULL || *ppContext == NULL) {
    return;
  }

  SSTriggerRealtimeContext *pContext = *ppContext;
  if (pContext->pReaderWalProgress) {
    tSimpleHashCleanup(pContext->pReaderWalProgress);
    pContext->pReaderWalProgress = NULL;
  }
  if (pContext->pGroups) {
    tSimpleHashCleanup(pContext->pGroups);
    pContext->pGroups = NULL;
  }
  TRINGBUF_DESTROY(&pContext->groupsToCheck);
  TRINGBUF_DESTROY(&pContext->groupsMaxDelay);
  stwmDestroy(&pContext->pMerger);

  if (pContext->pStartCond) {
    filterFreeInfo(pContext->pStartCond);
    pContext->pStartCond = NULL;
  }
  if (pContext->pEndCond) {
    filterFreeInfo(pContext->pEndCond);
    pContext->pEndCond = NULL;
  }

  if (pContext->pNotifyParams != NULL) {
    taosArrayDestroyEx(pContext->pNotifyParams, tDestroySSTriggerCalcParam);
    pContext->pNotifyParams = NULL;
  }

  for (int32_t i = 0; i < STRIGGER_PULL_TYPE_MAX; ++i) {
    if (pContext->pullResDataBlock[i] != NULL) {
      blockDataDestroy(pContext->pullResDataBlock[i]);
      pContext->pullResDataBlock[i] = NULL;
    }
  }

  tDestroySTriggerCalcRequest(&pContext->calcReq);
  if (pContext->pCalcDataCache) {
    destroyStreamDataCache(pContext->pCalcDataCache);
    pContext->pCalcDataCache = NULL;
  }

  taosMemFreeClear(*ppContext);
}

static int32_t strtcSendPullReq(SSTriggerRealtimeContext *pContext, ESTriggerPullType type, void *param) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pContext->pTask;
  SStreamTaskAddr    *pReader = NULL;

  QUERY_CHECK_CONDITION(pContext->pullStatus == STRIGGER_REQUEST_IDLE, code, lino, _end, TSDB_CODE_INVALID_PARA);

  switch (type) {
    case STRIGGER_PULL_LAST_TS: {
      pReader = param;
      break;
    }
    case STRIGGER_PULL_WAL_META: {
      SSTriggerWalProgress    *pProgress = param;
      SSTriggerWalMetaRequest *pReq = &pContext->pullReq.walMetaReq;
      pReader = pProgress->pTaskAddr;
      pReq->lastVer = pProgress->lastScanVer;
      pReq->ctime = INT64_MAX;
      break;
    }
    case STRIGGER_PULL_WAL_TS_DATA: {
      SSTriggerWalMeta          *pMeta = param;
      SSTriggerWalTsDataRequest *pReq = &pContext->pullReq.walTsDataReq;
      SSTriggerWalProgress *pProgress = tSimpleHashGet(pContext->pReaderWalProgress, &pMeta->vgId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pReader = pProgress->pTaskAddr;
      QUERY_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pReq->uid = pMeta->uid;
      pReq->ver = pMeta->ver;
      pReq->skey = pMeta->skey;
      pReq->ekey = pMeta->ekey;
      break;
    }
    case STRIGGER_PULL_WAL_TRIGGER_DATA: {
      SSTriggerWalMeta               *pMeta = param;
      SSTriggerWalTriggerDataRequest *pReq = &pContext->pullReq.walTriggerDataReq;
      SSTriggerWalProgress *pProgress = tSimpleHashGet(pContext->pReaderWalProgress, &pMeta->vgId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pReader = pProgress->pTaskAddr;
      QUERY_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pReq->uid = pMeta->uid;
      pReq->ver = pMeta->ver;
      pReq->skey = pMeta->skey;
      pReq->ekey = pMeta->ekey;
      break;
    }
    case STRIGGER_PULL_WAL_CALC_DATA: {
      SSTriggerWalMeta            *pMeta = param;
      SSTriggerWalCalcDataRequest *pReq = &pContext->pullReq.walCalcDataReq;
      SSTriggerWalProgress *pProgress = tSimpleHashGet(pContext->pReaderWalProgress, &pMeta->vgId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pReader = pProgress->pTaskAddr;
      QUERY_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pReq->uid = pMeta->uid;
      pReq->ver = pMeta->ver;
      pReq->skey = pMeta->skey;
      pReq->ekey = pMeta->ekey;
      break;
    }

    case STRIGGER_PULL_GROUP_COL_VALUE: {
      SSTriggerRealtimeGroup        *pGroup = param;
      SSTriggerGroupColValueRequest *pReq = &pContext->pullReq.groupColValueReq;
      SSTriggerWalProgress *pProgress = tSimpleHashGet(pContext->pReaderWalProgress, &pGroup->vgId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pReader = pProgress->pTaskAddr;
      QUERY_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pReq->gid = pGroup->groupId;
      break;
    }

    default: {
      ST_TASK_ELOG("invalid trigger pull type %d for realtime calc", type);
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  SSTriggerPullRequest *pReq = &pContext->pullReq.base;
  pReq->type = type;
  pReq->readerTaskId = pReader->taskId;

  // serialize and send request
  SRpcMsg msg = {.msgType = TDMT_STREAM_TRIGGER_PULL, .info.notFreeAhandle = true};
  msg.info.ahandle = pReq;
  msg.contLen = tSerializeSTriggerPullRequest(NULL, 0, pReq);
  QUERY_CHECK_CONDITION(msg.contLen > 0, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  msg.contLen += sizeof(SMsgHead);
  msg.pCont = rpcMallocCont(msg.contLen);
  QUERY_CHECK_NULL(msg.pCont, code, lino, _end, terrno);
  SMsgHead *pMsgHead = (SMsgHead *)msg.pCont;
  pMsgHead->contLen = htonl(msg.contLen);
  pMsgHead->vgId = htonl(pReader->nodeId);
  int32_t tlen = tSerializeSTriggerPullRequest(msg.pCont + sizeof(SMsgHead), msg.contLen - sizeof(SMsgHead), pReq);
  QUERY_CHECK_CONDITION(tlen == msg.contLen - sizeof(SMsgHead), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  code = tmsgSendReq(&pReader->epset, &msg);
  QUERY_CHECK_CODE(code, lino, _end);

  pContext->pullStatus = STRIGGER_REQUEST_RUNNING;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t strtcSendCalcReq(SSTriggerRealtimeContext *pContext) {
  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               lino = 0;
  SStreamTriggerTask   *pTask = pContext->pTask;
  SSTriggerCalcRequest *pReq = &pContext->calcReq;
  SSDataBlock          *pDataBlock = NULL;
  int32_t               startIdx = 0;
  int32_t               endIdx = 0;
  bool                  needFetch = false;
  bool                  needFree = false;

  QUERY_CHECK_CONDITION(pContext->calcStatus == STRIGGER_REQUEST_TO_RUN, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

  pReq->gid = pContext->pCalcGroup->groupId;
  pReq->createTable = true;  // todo(kjq): only create table at first time

  bool needGroupColValue = pTask->needGroupColValue || (pReq->createTable && LIST_LENGTH(pTask->partitionCols) > 0);
  if (taosArrayGetSize(pReq->groupColVals) == 0 && needGroupColValue) {
    code = strtcSendPullReq(pContext, STRIGGER_PULL_GROUP_COL_VALUE, pContext->pCalcGroup);
    QUERY_CHECK_CODE(code, lino, _end);
    goto _end;
  }

  if (pTask->needCacheData || (pTask->needRowNumber && pTask->triggerType == STREAM_TRIGGER_SLIDING)) {
    SSTriggerWalMetaMerger *pMerger = pContext->pMerger;
    SSTriggerRealtimeGroup *pGroup = pContext->pCalcGroup;
    SSTriggerCalcParam     *pFirstWin = taosArrayGet(pReq->params, 0);
    SSTriggerCalcParam     *pLastWin = taosArrayGet(pReq->params, taosArrayGetSize(pReq->params) - 1);
    if (IS_TRIGGER_WAL_META_MERGER_EMPTY(pMerger)) {
      // build session window merger
      SSTriggerWalMeta *pMetas = taosArrayGet(pGroup->pMetas, 0);
      int32_t           nMetas = taosArrayGetSize(pGroup->pMetas);
      code =
          stwmSetWalMetas(pMerger, pMetas, nMetas, pTask->needCacheData ? pTask->calcTsIndex : pTask->primaryTsIndex);
      QUERY_CHECK_CODE(code, lino, _end);
      code = stwmBuildDataMerger(pMerger, pFirstWin->wstart,
                                 pLastWin->wend - (pTask->triggerType == STREAM_TRIGGER_SLIDING));
      QUERY_CHECK_CODE(code, lino, _end);
      if (pContext->pCalcDataCache == NULL) {
        int32_t cleanMode = DATA_CLEAN_IMMEDIATE;
        if (pTask->triggerType == STREAM_TRIGGER_SLIDING) {
          SInterval *pInterval = &pTask->interval;
          if ((pInterval->sliding > 0) && (pInterval->sliding < pInterval->interval)) {
            cleanMode = DATA_CLEAN_EXPIRED;
          }
        } else if (pTask->triggerType == STREAM_TRIGGER_COUNT) {
          if ((pTask->windowSliding > 0) && (pTask->windowSliding < pTask->windowCount)) {
            cleanMode = DATA_CLEAN_EXPIRED;
          }
        }
        code = initStreamDataCache(pTask->task.streamId, pTask->task.taskId, pContext->sessionId, cleanMode,
                                   pTask->calcTsIndex, &pContext->pCalcDataCache);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
    while (true) {
      SSTriggerWalMetaList *pDataWinner = stwmGetDataWinner(pMerger);
      if (pDataWinner == NULL) {
        break;
      }
      while (true) {
        code = stwmMetaListNextData(pMerger, pDataWinner, &pDataBlock, &startIdx, &endIdx, &needFetch, &needFree);
        QUERY_CHECK_CODE(code, lino, _end);
        if (needFetch) {
          code =
              strtcSendPullReq(pContext, pTask->needCacheData ? STRIGGER_PULL_WAL_CALC_DATA : STRIGGER_PULL_WAL_TS_DATA,
                               pDataWinner->head->pMeta);
          QUERY_CHECK_CODE(code, lino, _end);
          goto _end;
        }
        if (pDataBlock == NULL) {
          code = stwmAdjustDataWinner(pMerger);
          QUERY_CHECK_CODE(code, lino, _end);
          break;
        }
        SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pMerger->tsSlotId);
        int32_t          idx = startIdx;
        while (idx < endIdx) {
          SSTriggerCalcParam *pParam = NULL;
          int64_t             ts = *(int64_t *)colDataGetNumData(pTsCol, idx);
          for (int32_t i = 0; i < TARRAY_SIZE(pReq->params); ++i) {
            pParam = taosArrayGet(pReq->params, i);
            if (pParam->wstart <= ts && ts <= (pParam->wend - (pTask->triggerType == STREAM_TRIGGER_SLIDING))) {
              break;
            }
          }
          int32_t nextIdx = idx;
          while (nextIdx < endIdx) {
            int64_t nextTs = *(int64_t *)colDataGetNumData(pTsCol, nextIdx);
            if (nextTs > (pParam->wend - (pTask->triggerType == STREAM_TRIGGER_SLIDING))) {
              break;
            }
            nextIdx++;
          }
          pParam->wrownum += nextIdx - idx;
          code = putStreamDataCache(pContext->pCalcDataCache, pGroup->groupId, pParam->wstart,
                                    pParam->wend - (pTask->triggerType == STREAM_TRIGGER_SLIDING), pDataBlock, idx,
                                    nextIdx - 1);
          QUERY_CHECK_CODE(code, lino, _end);
          idx = nextIdx;
        }
        if (needFree) {
          blockDataDestroy(pDataBlock);
          pDataBlock = NULL;
        }
      }
    }
    if (TRINGBUF_IS_EMPTY(&pContext->groupsMaxDelay)) {
      taosArrayPopFrontBatch(pGroup->pMetas, pGroup->metaIdx);
      pGroup->metaIdx = 0;
    }
    stwmClear(pMerger);
  }

  SStreamRunnerTarget *pRunner = NULL;
  code = stTriggerChooseRunner(pTask, &pRunner);
  QUERY_CHECK_CODE(code, lino, _end);
  pReq->runnerTaskId = pRunner->addr.taskId;

  // serialize and send request
  SRpcMsg msg = {.msgType = TDMT_STREAM_TRIGGER_CALC, .info.notFreeAhandle = true};
  msg.info.ahandle = pReq;
  msg.contLen = tSerializeSTriggerCalcRequest(NULL, 0, pReq);
  QUERY_CHECK_CONDITION(msg.contLen > 0, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  msg.contLen += sizeof(SMsgHead);
  msg.pCont = rpcMallocCont(msg.contLen);
  QUERY_CHECK_NULL(msg.pCont, code, lino, _end, terrno);
  SMsgHead *pMsgHead = (SMsgHead *)msg.pCont;
  pMsgHead->contLen = htonl(msg.contLen);
  pMsgHead->vgId = htonl(SNODE_HANDLE);
  for (int32_t i = 0; i < TARRAY_SIZE(pReq->params); ++i) {
    SSTriggerCalcParam *pParam = taosArrayGet(pReq->params, i);
    ST_TASK_ILOG("[calc param %d]: gid=%" PRId64 ", wstart=%" PRId64 ", wend=%" PRId64 ", nrows=%" PRId64, i, pReq->gid,
                 pParam->wstart, pParam->wend, pParam->wrownum);
  }
  int32_t tlen = tSerializeSTriggerCalcRequest(msg.pCont + sizeof(SMsgHead), msg.contLen - sizeof(SMsgHead), pReq);
  QUERY_CHECK_CONDITION(tlen == msg.contLen - sizeof(SMsgHead), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  code = tmsgSendReq(&pRunner->addr.epset, &msg);
  QUERY_CHECK_CODE(code, lino, _end);

  pContext->calcStatus = STRIGGER_REQUEST_RUNNING;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t strtcPullNewMeta(SSTriggerRealtimeContext *pContext) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pContext->pTask;

  int32_t numReader = taosArrayGetSize(pTask->readerList);
  pContext->curReaderIdx = (pContext->curReaderIdx + 1) % numReader;
  if (pContext->curReaderIdx == 0) {
    pContext->getWalMeta = false;
  }
  SStreamTaskAddr      *pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
  SSTriggerWalProgress *pProgress = tSimpleHashGet(pContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t));
  QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  code = strtcSendPullReq(pContext, STRIGGER_PULL_WAL_META, pProgress);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t strtcResumeCheck(SSTriggerRealtimeContext *pContext) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pContext->pTask;

  if (pContext->retryPull) {
    pContext->retryPull = false;
    SSTriggerPullRequest *pReq = &pContext->pullReq.base;
    SStreamTaskAddr      *pReader = NULL;
    for (int32_t i = 0; i < taosArrayGetSize(pTask->readerList); i++) {
      SStreamTaskAddr *pCurReader = taosArrayGet(pTask->readerList, i);
      if (pCurReader->taskId == pReq->readerTaskId) {
        pReader = pCurReader;
        break;
      }
    }
    QUERY_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    SRpcMsg msg = {.msgType = TDMT_STREAM_TRIGGER_PULL, .info.notFreeAhandle = true};
    msg.info.ahandle = pReq;
    msg.contLen = tSerializeSTriggerPullRequest(NULL, 0, pReq);
    QUERY_CHECK_CONDITION(msg.contLen > 0, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    msg.contLen += sizeof(SMsgHead);
    msg.pCont = rpcMallocCont(msg.contLen);
    QUERY_CHECK_NULL(msg.pCont, code, lino, _end, terrno);
    SMsgHead *pMsgHead = (SMsgHead *)msg.pCont;
    pMsgHead->contLen = htonl(msg.contLen);
    pMsgHead->vgId = htonl(pReader->nodeId);
    int32_t tlen = tSerializeSTriggerPullRequest(msg.pCont + sizeof(SMsgHead), msg.contLen - sizeof(SMsgHead), pReq);
    QUERY_CHECK_CONDITION(tlen == msg.contLen - sizeof(SMsgHead), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    code = tmsgSendReq(&pReader->epset, &msg);
    QUERY_CHECK_CODE(code, lino, _end);

    pContext->pullStatus = STRIGGER_REQUEST_RUNNING;
  } else if (pTask->triggerType == STREAM_TRIGGER_PERIOD) {
    int64_t                 gid = 0;
    void                   *px = tSimpleHashGet(pContext->pGroups, &gid, sizeof(int64_t));
    SSTriggerRealtimeGroup *pGroup = NULL;
    if (px == NULL) {
      pGroup = taosMemoryCalloc(1, sizeof(SSTriggerRealtimeGroup));
      QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
      code = tSimpleHashPut(pContext->pGroups, &gid, sizeof(int64_t), &pGroup, POINTER_BYTES);
      if (code != TSDB_CODE_SUCCESS) {
        taosMemoryFreeClear(pGroup);
      }
      QUERY_CHECK_CODE(code, lino, _end);
      code = strtgInit(pGroup, pContext, gid);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      pGroup = *(SSTriggerRealtimeGroup **)px;
    }
    int64_t now = taosGetTimestampMs();
    if (pGroup->winStatus == STRIGGER_WINDOW_INITIALIZED) {
      code = strtgOpenNewWindow(pGroup, now, NULL);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    if (pGroup->curWindow.ekey <= now) {
      SSTriggerCalcRequest *pReq = &pContext->calcReq;
      pReq->gid = pGroup->groupId;
      QUERY_CHECK_CONDITION(taosArrayGetSize(pReq->params) == 0, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

      // wait until the thread can send calc request
      code = streamTriggerAcquireCalcReq();
      QUERY_CHECK_CODE(code, lino, _end);

      while (pGroup->curWindow.ekey <= now) {
        code = strtgCloseCurrentWindow(pGroup, NULL);
        QUERY_CHECK_CODE(code, lino, _end);
        code = strtgOpenNewWindow(pGroup, now + 1, NULL);
        QUERY_CHECK_CODE(code, lino, _end);
        if (taosArrayGetSize(pReq->params) >= pTask->calcParamLimit) {
          break;
        }
      }
      if (pContext->calcStatus == STRIGGER_REQUEST_TO_RUN) {
        code = strtcSendCalcReq(pContext);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
    code = streamTriggerAddWaitTask(pTask, pGroup->curWindow.ekey);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    while (!TRINGBUF_IS_EMPTY(&pContext->groupsToCheck)) {
      SSTriggerRealtimeGroup *pCurGroup = TRINGBUF_FIRST(&pContext->groupsToCheck);
      code = strtgResumeCheck(pCurGroup);
      QUERY_CHECK_CODE(code, lino, _end);
      if (taosArrayGetSize(pContext->pNotifyParams) > 0) {
        code = streamSendNotifyContent(&pTask->task, pTask->triggerType, pCurGroup->groupId, pTask->pNotifyAddrUrls,
                                       pTask->notifyErrorHandle, TARRAY_DATA(pContext->pNotifyParams),
                                       taosArrayGetSize(pContext->pNotifyParams));
        QUERY_CHECK_CODE(code, lino, _end);
        taosArrayClearEx(pContext->pNotifyParams, tDestroySSTriggerCalcParam);
      }
      if (pCurGroup->status == STRIGGER_GROUP_WAITING_TDATA) {
        goto _end;
      } else if (pContext->calcStatus == STRIGGER_REQUEST_TO_RUN) {
        code = strtcSendCalcReq(pContext);
        QUERY_CHECK_CODE(code, lino, _end);
        goto _end;
      } else {
        TRINGBUF_DEQUEUE(&pContext->groupsToCheck);
      }
    }

    if ((pContext->curReaderIdx == taosArrayGetSize(pTask->readerList) - 1) && pTask->maxDelay > 0 &&
        TRINGBUF_IS_EMPTY(&pContext->groupsMaxDelay)) {
      int32_t iter = 0;
      void   *px = tSimpleHashIterate(pContext->pGroups, NULL, &iter);
      while (px != NULL) {
        SSTriggerRealtimeGroup *pGroup = *(SSTriggerRealtimeGroup **)px;
        if (pGroup->winStatus == STRIGGER_WINDOW_OPENED && pGroup->status == STRIGGER_GROUP_WAITING_META &&
            pGroup->curWindow.skey + pTask->maxDelay * 1000 < taosGetTimestampUs()) {
          code = TRINGBUF_APPEND(&pContext->groupsMaxDelay, pGroup);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        px = tSimpleHashIterate(pContext->pGroups, px, &iter);
      }
    }

    if (!TRINGBUF_IS_EMPTY(&pContext->groupsMaxDelay)) {
      SSTriggerRealtimeGroup *pCurGroup = TRINGBUF_FIRST(&pContext->groupsMaxDelay);

      QUERY_CHECK_CONDITION(pContext->calcStatus == STRIGGER_REQUEST_IDLE, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SSTriggerCalcRequest *pReq = &pContext->calcReq;
      pReq->gid = pCurGroup->groupId;
      QUERY_CHECK_CONDITION(taosArrayGetSize(pReq->params) == 0, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

      // wait until the thread can send calc request
      code = streamTriggerAcquireCalcReq();
      QUERY_CHECK_CODE(code, lino, _end);

      STimeWindow       *pWindow = &pCurGroup->curWindow;
      SSTriggerCalcParam param = {
          .currentTs = pWindow->ekey + 1,
          .wstart = pWindow->skey,
          .wend = pWindow->ekey,
          .wduration = pWindow->ekey - pWindow->skey,
          .wrownum = (pTask->triggerType == STREAM_TRIGGER_COUNT) ? pTask->windowCount : pCurGroup->nrowsInWindow,
          .triggerTime = taosGetTimestampNs(),
          .notifyType = (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_CLOSE),
          .extraNotifyContent = NULL,
      };
      if (pTask->triggerType == STREAM_TRIGGER_SLIDING) {
        param.wend++;
        param.wduration++;
      }
      pContext->calcStatus = STRIGGER_REQUEST_TO_RUN;
      pContext->pCalcGroup = pCurGroup;
      void *px = taosArrayPush(pReq->params, &param);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      code = strtcSendCalcReq(pContext);
      QUERY_CHECK_CODE(code, lino, _end);
      goto _end;
    }

    if ((pContext->curReaderIdx == taosArrayGetSize(pTask->readerList) - 1) && !pContext->getWalMeta) {
      // add the task to wait list since it catches up all readers
      int64_t resumeTime = taosGetTimestampMs() + STREAM_TRIGGER_WAIT_TIME_MS;
      code = streamTriggerAddWaitTask(pTask, resumeTime);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      code = strtcPullNewMeta(pContext);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t strtcProcessPullRsp(SSTriggerRealtimeContext *pContext, SSDataBlock *pResDataBlock) {
  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               lino = 0;
  SStreamTriggerTask   *pTask = pContext->pTask;
  SSTriggerPullRequest *pReq = &pContext->pullReq.base;

  QUERY_CHECK_CONDITION(pContext->pullStatus == STRIGGER_REQUEST_RUNNING, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  pContext->pullStatus = STRIGGER_REQUEST_IDLE;

  switch (pReq->type) {
    case STRIGGER_PULL_LAST_TS: {
      SStreamTaskAddr *pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
      int64_t          latestVer = pResDataBlock->info.id.groupId;
      if (pTask->pRealtimeStartVer == NULL) {
        pTask->pRealtimeStartVer = tSimpleHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
        QUERY_CHECK_NULL(pTask->pRealtimeStartVer, code, lino, _end, terrno);
      }
      void *px = tSimpleHashGet(pTask->pRealtimeStartVer, &pReader->nodeId, sizeof(int64_t));
      QUERY_CHECK_CONDITION(px == NULL, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      code = tSimpleHashPut(pTask->pRealtimeStartVer, &pReader->nodeId, sizeof(int64_t), &latestVer, sizeof(int64_t));
      QUERY_CHECK_CODE(code, lino, _end);

      if (pTask->pHistoryCutoffTime == NULL) {
        pTask->pHistoryCutoffTime = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
        QUERY_CHECK_NULL(pTask->pHistoryCutoffTime, code, lino, _end, terrno);
      }
      int32_t numOfRows = blockDataGetNumOfRows(pResDataBlock);
      for (int32_t i = 0; i < numOfRows; ++i) {
        SColumnInfoData *pGidCol = taosArrayGet(pResDataBlock->pDataBlock, 0);
        SColumnInfoData *pTsCol = taosArrayGet(pResDataBlock->pDataBlock, 1);
        int64_t          gid = *(int64_t *)colDataGetNumData(pGidCol, i);
        int64_t          ts = *(int64_t *)colDataGetNumData(pTsCol, i);
        px = tSimpleHashGet(pTask->pHistoryCutoffTime, &gid, sizeof(int64_t));
        if (px == NULL) {
          code = tSimpleHashPut(pTask->pHistoryCutoffTime, &gid, sizeof(int64_t), &ts, sizeof(int64_t));
          QUERY_CHECK_CODE(code, lino, _end);
        } else {
          *(int64_t *)px = TMAX(*(int64_t *)px, ts);
        }
      }
      pContext->curReaderIdx++;
      if (pContext->curReaderIdx == taosArrayGetSize(pTask->readerList)) {
        pContext->curReaderIdx = -1;
        if (pTask->fillHistory && pTask->fillHistoryFirst) {
          QUERY_CHECK_NULL(pTask->pHistoryCtx, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
          pTask->pHistoryCtx->curReaderIdx = 0;
          code = sthcSendPullReq(pTask->pHistoryCtx, STRIGGER_PULL_FIRST_TS, pReader);
          QUERY_CHECK_CODE(code, lino, _end);
        } else {
          code = strtcResumeCheck(pContext);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      } else {
        SStreamTaskAddr *pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
        code = strtcSendPullReq(pContext, STRIGGER_PULL_LAST_TS, pReader);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      break;
    }
    case STRIGGER_PULL_WAL_META: {
      QUERY_CHECK_CONDITION(TRINGBUF_IS_EMPTY(&pContext->groupsToCheck), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      int32_t numNewMeta = blockDataGetNumOfRows(pResDataBlock);
      for (int32_t i = 0; i < numNewMeta; ++i) {
        SColumnInfoData        *pGidCol = taosArrayGet(pResDataBlock->pDataBlock, 1);
        int64_t                 gid = *(int64_t *)colDataGetNumData(pGidCol, i);
        void                   *px = tSimpleHashGet(pContext->pGroups, &gid, sizeof(int64_t));
        SSTriggerRealtimeGroup *pGroup = NULL;
        if (px == NULL) {
          pGroup = taosMemoryCalloc(1, sizeof(SSTriggerRealtimeGroup));
          QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
          code = tSimpleHashPut(pContext->pGroups, &gid, sizeof(int64_t), &pGroup, POINTER_BYTES);
          if (code != TSDB_CODE_SUCCESS) {
            taosMemoryFreeClear(pGroup);
          }
          QUERY_CHECK_CODE(code, lino, _end);
          code = strtgInit(pGroup, pContext, gid);
          QUERY_CHECK_CODE(code, lino, _end);
        } else {
          pGroup = *(SSTriggerRealtimeGroup **)px;
        }
        if (pGroup->pWalMetaData == NULL) {
          pGroup->pWalMetaData = pResDataBlock;
          code = TRINGBUF_APPEND(&pContext->groupsToCheck, pGroup);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }

      int64_t lastScanVer = 0;
      if (blockDataGetNumOfRows(pResDataBlock) > 0) {
        SColumnInfoData *pVerCol = taosArrayGet(pResDataBlock->pDataBlock, 5);
        lastScanVer = *(int64_t *)colDataGetNumData(pVerCol, numNewMeta - 1);
      } else {
        lastScanVer = pResDataBlock->info.id.groupId;
      }
      SStreamTaskAddr      *pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
      SSTriggerWalProgress *pProgress = tSimpleHashGet(pContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pProgress->lastScanVer = lastScanVer;
      pProgress->latestVer = pResDataBlock->info.id.groupId;

      code = strtcResumeCheck(pContext);
      QUERY_CHECK_CODE(code, lino, _end);
      break;
    }
    case STRIGGER_PULL_WAL_TS_DATA:
    case STRIGGER_PULL_WAL_TRIGGER_DATA: {
      if (pTask->triggerType != STREAM_TRIGGER_SLIDING) {
        QUERY_CHECK_CONDITION(!TRINGBUF_IS_EMPTY(&pContext->groupsToCheck), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        code = stwmBindDataBlock(pContext->pMerger, pResDataBlock);
        QUERY_CHECK_CODE(code, lino, _end);
        code = strtcResumeCheck(pContext);
        QUERY_CHECK_CODE(code, lino, _end);
        break;
      }
    }
    case STRIGGER_PULL_WAL_CALC_DATA: {
      code = stwmBindDataBlock(pContext->pMerger, pResDataBlock);
      QUERY_CHECK_CODE(code, lino, _end);
      QUERY_CHECK_CONDITION(pContext->calcStatus == STRIGGER_REQUEST_TO_RUN, code, lino, _end,
                            TSDB_CODE_INTERNAL_ERROR);
      code = strtcSendCalcReq(pContext);
      QUERY_CHECK_CODE(code, lino, _end);
      break;
    }

    default: {
      ST_TASK_ELOG("invalid trigger pull type %d for realtime calc", pReq->type);
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t strtcProcessCalcRsp(SSTriggerRealtimeContext *pContext, int32_t retCode) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pContext->pTask;

  QUERY_CHECK_CONDITION(pContext->calcStatus == STRIGGER_REQUEST_RUNNING, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  pContext->calcStatus = STRIGGER_REQUEST_IDLE;

  for (int32_t i = 0; i < taosArrayGetSize(pTask->runnerList); ++i) {
    SStreamRunnerTarget *pRunner = taosArrayGet(pTask->runnerList, i);
    if (pRunner->addr.taskId == pContext->calcReq.runnerTaskId) {
      atomic_sub_fetch_32(&pTask->pCalcExecCount[i], 1);
      break;
    }
  }

  if (!TRINGBUF_IS_EMPTY(&pContext->groupsToCheck)) {
    SSTriggerRealtimeGroup *pCurGroup = TRINGBUF_FIRST(&pContext->groupsToCheck);
    if (pCurGroup->status == STRIGGER_GROUP_WAITING_CALC) {
      code = strtcResumeCheck(pContext);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  if (!TRINGBUF_IS_EMPTY(&pContext->groupsMaxDelay)) {
    TRINGBUF_DEQUEUE(&pContext->groupsMaxDelay);
    if (!TRINGBUF_IS_EMPTY(&pContext->groupsMaxDelay)) {
      code = strtcResumeCheck(pContext);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      if ((pContext->curReaderIdx == taosArrayGetSize(pTask->readerList) - 1) && !pContext->getWalMeta) {
        // add the task to wait list since it catches up all readers
        int64_t resumeTime = taosGetTimestampMs() + STREAM_TRIGGER_WAIT_TIME_MS;
        code = streamTriggerAddWaitTask(pTask, resumeTime);
        QUERY_CHECK_CODE(code, lino, _end);
      } else {
        code = strtcPullNewMeta(pContext);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t strtcGenCheckpoint(SSTriggerRealtimeContext *pContext, uint8_t *buf, int64_t *pLen) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pContext->pTask;
  SEncoder            encoder = {0};
  int32_t             iter = 0;
  tEncoderInit(&encoder, buf, *pLen);

  code = tEncodeI32(&encoder, tSimpleHashGetSize(pContext->pReaderWalProgress));
  QUERY_CHECK_CODE(code, lino, _end);
  iter = 0;
  SSTriggerWalProgress *pProgress = tSimpleHashIterate(pContext->pReaderWalProgress, NULL, &iter);
  while (pProgress != NULL) {
    code = tEncodeI32(&encoder, pProgress->pTaskAddr->nodeId);
    QUERY_CHECK_CODE(code, lino, _end);
    code = tEncodeI64(&encoder, pProgress->latestVer);
    QUERY_CHECK_CODE(code, lino, _end);
    code = tEncodeI64(&encoder, pProgress->lastScanVer);
    QUERY_CHECK_CODE(code, lino, _end);
    pProgress = tSimpleHashIterate(pContext->pReaderWalProgress, pProgress, &iter);
  }

  code = tEncodeI32(&encoder, tSimpleHashGetSize(pContext->pGroups));
  QUERY_CHECK_CODE(code, lino, _end);
  iter = 0;
  SSTriggerRealtimeGroup *pGroup = tSimpleHashIterate(pContext->pGroups, NULL, &iter);
  while (pGroup != NULL) {
    code = tEncodeI64(&encoder, pGroup->groupId);
    QUERY_CHECK_CODE(code, lino, _end);
    code = tEncodeI64(&encoder, pGroup->curWindow.skey);
    QUERY_CHECK_CODE(code, lino, _end);
    code = tEncodeI64(&encoder, pGroup->curWindow.ekey);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  tEndEncode(&encoder);

  *pLen = encoder.pos;

_end:
  tEncoderClear(&encoder);
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t sthgInit(SSTriggerHistoryGroup *pGroup, SSTriggerHistoryContext *pContext, int64_t groupId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  pGroup->pContext = pContext;
  pGroup->groupId = groupId;
  pGroup->pMetas = taosArrayInit(0, sizeof(SSTriggerTsdbMeta));
  QUERY_CHECK_NULL(pGroup->pMetas, code, lino, _end, terrno);
  pGroup->curWindow = (STimeWindow){.skey = INT64_MIN, .ekey = INT64_MIN};
  if (pContext->pTask->triggerType == STREAM_TRIGGER_COUNT) {
    TRINGBUF_INIT(&pGroup->wstartBuf);
    int32_t cap = (pContext->pTask->windowCount / pContext->pTask->windowSliding) + 1;
    code = TRINGBUF_RESERVE(&pGroup->wstartBuf, cap);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  return code;
}

static void sthgDestroy(void *ptr) {
  SSTriggerHistoryGroup **ppGroup = ptr;
  if (ppGroup == NULL || *ppGroup == NULL) {
    return;
  }

  SSTriggerHistoryGroup *pGroup = *ppGroup;
  if (pGroup->pMetas) {
    taosArrayDestroy(pGroup->pMetas);
    pGroup->pMetas = NULL;
  }
  if (pGroup->pContext->pTask->triggerType == STREAM_TRIGGER_COUNT) {
    TRINGBUF_DESTROY(&pGroup->wstartBuf);
  } else if (pGroup->pContext->pTask->triggerType == STREAM_TRIGGER_STATE) {
    if (IS_VAR_DATA_TYPE(pGroup->stateVal.type)) {
      taosMemoryFreeClear(pGroup->stateVal.pData);
    }
  }

  taosMemFreeClear(*ppGroup);
}

static int32_t sthcInit(SSTriggerHistoryContext *pContext, SStreamTriggerTask *pTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  pContext->pTask = pTask;
  pContext->sessionId = SSTRIGGER_HISTORY_SESSIONID_PREFIX | pTask->nextSessionId;
  pTask->nextSessionId++;

  pContext->curReaderIdx = -1;

  pContext->pGroups = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  QUERY_CHECK_NULL(pContext->pGroups, code, lino, _end, terrno);
  tSimpleHashSetFreeFp(pContext->pGroups, sthgDestroy);
  TRINGBUF_INIT(&pContext->groupsToCheck);

  if (pTask->triggerType == STREAM_TRIGGER_EVENT) {
    code = filterInitFromNode(pTask->pStartCond, &pContext->pStartCond, 0, NULL);
    QUERY_CHECK_CODE(code, lino, _end);
    code = filterInitFromNode(pTask->pEndCond, &pContext->pEndCond, 0, NULL);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  pContext->pNotifyParams = taosArrayInit(0, sizeof(SSTriggerCalcParam));
  QUERY_CHECK_NULL(pContext->pNotifyParams, code, lino, _end, terrno);

  SSTriggerPullRequest *pPullReq = &pContext->pullReq.base;
  pPullReq->streamId = pTask->task.streamId;
  pPullReq->sessionId = pContext->sessionId;
  pPullReq->triggerTaskId = pTask->task.taskId;

  SSTriggerCalcRequest *pCalcReq = &pContext->calcReq;
  pCalcReq->streamId = pTask->task.streamId;
  pCalcReq->sessionId = pContext->sessionId;
  pCalcReq->triggerTaskId = pTask->task.taskId;
  pCalcReq->triggerType = pTask->triggerType;
  pCalcReq->params = taosArrayInit(0, sizeof(SSTriggerCalcParam));
  QUERY_CHECK_NULL(pCalcReq->params, code, lino, _end, terrno);

_end:
  return code;
}

static void sthcDestroy(void *ptr) {
  SSTriggerHistoryContext **ppContext = ptr;
  if (ppContext == NULL || *ppContext == NULL) {
    return;
  }

  SSTriggerHistoryContext *pContext = *ppContext;
  if (pContext->pGroups) {
    tSimpleHashCleanup(pContext->pGroups);
    pContext->pGroups = NULL;
  }
  TRINGBUF_DESTROY(&pContext->groupsToCheck);

  if (pContext->pStartCond) {
    filterFreeInfo(pContext->pStartCond);
    pContext->pStartCond = NULL;
  }
  if (pContext->pEndCond) {
    filterFreeInfo(pContext->pEndCond);
    pContext->pEndCond = NULL;
  }

  if (pContext->pNotifyParams != NULL) {
    taosArrayDestroyEx(pContext->pNotifyParams, tDestroySSTriggerCalcParam);
    pContext->pNotifyParams = NULL;
  }

  for (int32_t i = 0; i < STRIGGER_PULL_TYPE_MAX; ++i) {
    if (pContext->pullResDataBlock[i] != NULL) {
      blockDataDestroy(pContext->pullResDataBlock[i]);
      pContext->pullResDataBlock[i] = NULL;
    }
  }

  tDestroySTriggerCalcRequest(&pContext->calcReq);
  if (pContext->pCalcDataCache) {
    destroyStreamDataCache(pContext->pCalcDataCache);
    pContext->pCalcDataCache = NULL;
  }

  taosMemFreeClear(*ppContext);
}

static int32_t sthcSendPullReq(SSTriggerHistoryContext *pContext, ESTriggerPullType type, void *param) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pContext->pTask;
  SStreamTaskAddr    *pReader = NULL;

  switch (type) {
    case STRIGGER_PULL_FIRST_TS: {
      pReader = param;
      SSTriggerFirstTsRequest *pReq = &pContext->pullReq.firstTsReq;
      pReq->startTime = pContext->pTask->fillHistoryStartTime;
      break;
    }
    default: {
      ST_TASK_ELOG("invalid trigger pull type %d for history calc", type);
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  SSTriggerPullRequest *pReq = &pContext->pullReq.base;
  pReq->type = type;
  pReq->readerTaskId = pReader->taskId;

  // serialize and send request
  SRpcMsg msg = {.msgType = TDMT_STREAM_TRIGGER_PULL, .info.notFreeAhandle = true};
  msg.info.ahandle = pReq;
  msg.contLen = tSerializeSTriggerPullRequest(NULL, 0, pReq);
  QUERY_CHECK_CONDITION(msg.contLen > 0, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  msg.contLen += sizeof(SMsgHead);
  msg.pCont = rpcMallocCont(msg.contLen);
  QUERY_CHECK_NULL(msg.pCont, code, lino, _end, terrno);
  SMsgHead *pMsgHead = (SMsgHead *)msg.pCont;
  pMsgHead->contLen = htonl(msg.contLen);
  pMsgHead->vgId = htonl(pReader->nodeId);
  int32_t tlen = tSerializeSTriggerPullRequest(msg.pCont + sizeof(SMsgHead), msg.contLen - sizeof(SMsgHead), pReq);
  QUERY_CHECK_CONDITION(tlen == msg.contLen - sizeof(SMsgHead), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  code = tmsgSendReq(&pReader->epset, &msg);
  QUERY_CHECK_CODE(code, lino, _end);

  pContext->pullStatus = STRIGGER_REQUEST_RUNNING;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t sthcProcessPullRsp(SSTriggerHistoryContext *pContext, SSDataBlock *pResDataBlock) {
  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               lino = 0;
  SStreamTriggerTask   *pTask = pContext->pTask;
  SSTriggerPullRequest *pReq = &pContext->pullReq.base;

  QUERY_CHECK_CONDITION(pContext->pullStatus == STRIGGER_REQUEST_RUNNING, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  pContext->pullStatus = STRIGGER_REQUEST_IDLE;

  switch (pReq->type) {
    case STRIGGER_PULL_FIRST_TS: {
      SStreamTaskAddr *pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
      int32_t          numOfRows = blockDataGetNumOfRows(pResDataBlock);
      for (int32_t i = 0; i < numOfRows; ++i) {
        SColumnInfoData       *pGidCol = taosArrayGet(pResDataBlock->pDataBlock, 0);
        SColumnInfoData       *pTsCol = taosArrayGet(pResDataBlock->pDataBlock, 1);
        int64_t                gid = *(int64_t *)colDataGetNumData(pGidCol, i);
        int64_t                ts = *(int64_t *)colDataGetNumData(pTsCol, i);
        void                  *px = tSimpleHashGet(pContext->pGroups, &gid, sizeof(int64_t));
        SSTriggerHistoryGroup *pGroup = NULL;
        if (px == NULL) {
          pGroup = taosMemoryCalloc(1, sizeof(SSTriggerHistoryGroup));
          QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
          code = tSimpleHashPut(pContext->pGroups, &gid, sizeof(int64_t), &pGroup, POINTER_BYTES);
          if (code != TSDB_CODE_SUCCESS) {
            taosMemoryFreeClear(pGroup);
            QUERY_CHECK_CODE(code, lino, _end);
          }
          QUERY_CHECK_CODE(code, lino, _end);
          code = sthgInit(pGroup, pContext, gid);
          QUERY_CHECK_CODE(code, lino, _end);
        } else {
          pGroup = *(SSTriggerHistoryGroup **)px;
        }
        pGroup->pTsdbMetaData = pResDataBlock;
      }
      break;
    }
    default: {
      ST_TASK_ELOG("invalid trigger pull type %d for history calc", pReq->type);
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t stTriggerChooseRunner(SStreamTriggerTask *pTask, SStreamRunnerTarget **ppRunner) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  // todo(kjq): implement smarter runner selection for better load balancing
  SStreamRunnerTarget *pRunner = NULL;
  int32_t              i = 0;
  while (true) {
    pRunner = taosArrayGet(pTask->runnerList, i);
    int32_t execCnt = atomic_add_fetch_32(&pTask->pCalcExecCount[i], 1);
    if (execCnt <= pRunner->execReplica) {
      break;
    }
    execCnt = atomic_sub_fetch_32(&pTask->pCalcExecCount[i], 1);
    i = (i + 1) % taosArrayGetSize(pTask->runnerList);
  }

  *ppRunner = pRunner;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t stTriggerTaskMarkRecalc(SStreamTriggerTask *pTask, int64_t groupId, int64_t skey, int64_t ekey) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  // todo(kjq): mark recalculation interval

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t stTriggerTaskDeploy(SStreamTriggerTask *pTask, const SStreamTriggerDeployMsg *pMsg) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  // todo (kjq): add more check of pMsg
  pTask->leaderSnodeId = pMsg->leaderSnodeId;
  pTask->primaryTsIndex = 0;
  EWindowType type = pMsg->triggerType;
  switch (pMsg->triggerType) {
    case WINDOW_TYPE_INTERVAL: {
      pTask->triggerType = STREAM_TRIGGER_SLIDING;
      const SSlidingTrigger *pSliding = &pMsg->trigger.sliding;
      SInterval             *pInterval = &pTask->interval;
      pInterval->timezone = NULL;
      pInterval->intervalUnit = pSliding->intervalUnit;
      pInterval->slidingUnit = pSliding->slidingUnit;
      pInterval->offsetUnit = pSliding->offsetUnit;
      pInterval->precision = pSliding->precision;
      pInterval->interval = pSliding->interval;
      pInterval->sliding = pSliding->sliding;
      pInterval->offset = pSliding->offset;
      pInterval->timeRange = (STimeWindow){.skey = INT64_MIN, .ekey = INT64_MIN};
      break;
    }
    case WINDOW_TYPE_SESSION: {
      pTask->triggerType = STREAM_TRIGGER_SESSION;
      const SSessionTrigger *pSession = &pMsg->trigger.session;
      pTask->gap = pSession->sessionVal;
      pTask->primaryTsIndex = pSession->slotId;
      break;
    }
    case WINDOW_TYPE_STATE: {
      pTask->triggerType = STREAM_TRIGGER_STATE;
      const SStateWinTrigger *pState = &pMsg->trigger.stateWin;
      pTask->stateColId = pState->slotId;
      pTask->stateTrueFor = pState->trueForDuration;
      break;
    }
    case WINDOW_TYPE_EVENT: {
      pTask->triggerType = STREAM_TRIGGER_EVENT;
      const SEventTrigger *pEvent = &pMsg->trigger.event;
      code = nodesStringToNode(pEvent->startCond, &pTask->pStartCond);
      QUERY_CHECK_CODE(code, lino, _end);
      code = nodesStringToNode(pEvent->endCond, &pTask->pEndCond);
      QUERY_CHECK_CODE(code, lino, _end);
      pTask->eventTrueFor = pEvent->trueForDuration;
      code = nodesCollectColumnsFromNode(pTask->pStartCond, NULL, COLLECT_COL_TYPE_ALL, &pTask->pStartCondCols);
      QUERY_CHECK_CODE(code, lino, _end);
      code = nodesCollectColumnsFromNode(pTask->pEndCond, NULL, COLLECT_COL_TYPE_ALL, &pTask->pEndCondCols);
      QUERY_CHECK_CODE(code, lino, _end);
      break;
    }
    case WINDOW_TYPE_COUNT: {
      pTask->triggerType = STREAM_TRIGGER_COUNT;
      const SCountTrigger *pCount = &pMsg->trigger.count;
      pTask->windowCount = pCount->countVal;
      pTask->windowSliding = pCount->sliding > 0 ? pCount->sliding : pCount->countVal;
      break;
    }
    case WINDOW_TYPE_PERIOD: {
      pTask->triggerType = STREAM_TRIGGER_PERIOD;
      const SPeriodTrigger *pPeriod = &pMsg->trigger.period;
      SInterval            *pInterval = &pTask->interval;
      pInterval->timezone = NULL;
      pInterval->intervalUnit = pPeriod->periodUnit;
      pInterval->slidingUnit = pPeriod->periodUnit;
      pInterval->offsetUnit = pPeriod->offsetUnit;
      pInterval->precision = pPeriod->precision;
      pInterval->interval = 0;
      pInterval->sliding = pPeriod->period;
      pInterval->offset = pPeriod->offset;
      pInterval->timeRange = (STimeWindow){.skey = INT64_MIN, .ekey = INT64_MIN};
      break;
    }
    default: {
      ST_TASK_ELOG("invalid stream trigger window type %d", type);
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  pTask->maxDelay = pMsg->maxDelay;
  pTask->fillHistoryStartTime = pMsg->fillHistoryStartTime;
  pTask->watermark = pMsg->watermark;
  pTask->expiredTime = pMsg->expiredTime;
  pTask->calcTsIndex = pMsg->tsSlotId;
  code = nodesStringToList(pMsg->partitionCols, &pTask->partitionCols);
  QUERY_CHECK_CODE(code, lino, _end);
  pTask->ignoreDisorder = pMsg->igDisorder;
  pTask->fillHistory = pMsg->fillHistory;
  pTask->fillHistoryFirst = pMsg->fillHistoryFirst;
  pTask->lowLatencyCalc = pMsg->lowLatencyCalc;

  pTask->calcEventType = taosArrayGetSize(pMsg->runnerList) > 0 ? pMsg->eventTypes : STRIGGER_EVENT_WINDOW_NONE;
  pTask->notifyEventType = pMsg->notifyEventTypes;
  pTask->pNotifyAddrUrls = pMsg->pNotifyAddrUrls;
  if (pTask->notifyEventType == STRIGGER_EVENT_WINDOW_NONE && taosArrayGetSize(pTask->pNotifyAddrUrls) > 0) {
    QUERY_CHECK_CONDITION(pTask->triggerType == STREAM_TRIGGER_PERIOD || pTask->triggerType == STREAM_TRIGGER_SLIDING,
                          code, lino, _end, TSDB_CODE_INVALID_PARA);
    pTask->notifyEventType = STRIGGER_EVENT_WINDOW_CLOSE;
  }
  pTask->notifyErrorHandle = pMsg->notifyErrorHandle;
  pTask->notifyHistory = pMsg->notifyHistory;
  pTask->readerList = pMsg->readerList;
  pTask->runnerList = pMsg->runnerList;

  if (LIST_LENGTH(pTask->partitionCols) == 1) {
    SNode *pNode = pTask->partitionCols->pHead->pNode;
    if ((pNode->type == QUERY_NODE_FUNCTION) &&
        (strcmp(((struct SFunctionNode *)pNode)->functionName, "tbname") == 0)) {
      pTask->singleVnodePerGroup = true;
    }
  }
  pTask->singleVnodePerGroup = pTask->singleVnodePerGroup || taosArrayGetSize(pTask->readerList) == 1;
  pTask->needRowNumber = pMsg->placeHolderBitmap & PLACE_HOLDER_WROWNUM;
  pTask->needGroupColValue = (pMsg->placeHolderBitmap & PLACE_HOLDER_PARTITION_IDX) ||
                             (pMsg->placeHolderBitmap & PLACE_HOLDER_PARTITION_TBNAME);
  pTask->needCacheData = pMsg->placeHolderBitmap & PLACE_HOLDER_PARTITION_ROWS;

  pTask->calcParamLimit = 10;  // todo(kjq): adjust dynamically
  pTask->nextSessionId = 1;
  pTask->pRealtimeCtx = NULL;
  if (taosArrayGetSize(pTask->runnerList) > 0) {
    pTask->pCalcExecCount = taosMemoryCalloc(taosArrayGetSize(pTask->runnerList), sizeof(int32_t));
    QUERY_CHECK_NULL(pTask->pCalcExecCount, code, lino, _end, terrno);
  } else {
    pTask->pCalcExecCount = NULL;
  }

  pTask->task.status = STREAM_STATUS_INIT;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTask->task.status = STREAM_STATUS_FAILED;
  }
  return code;
}

int32_t stTriggerTaskUndeploy(SStreamTriggerTask **ppTask, const SStreamUndeployTaskMsg *pMsg, taskUndeplyCallback cb) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = *ppTask;

  // if (pMsg->doCheckpoint && pTask->pRealtimeCtx) {
  //   uint8_t *buf = NULL;
  //   int64_t  len = 0;
  //   code = strtcGenCheckpoint(pTask->pRealtimeCtx, buf, &len);
  //   QUERY_CHECK_CODE(code, lino, _end);
  //   buf = taosMemoryMalloc(len);
  //   code = strtcGenCheckpoint(pTask->pRealtimeCtx, buf, &len);
  //   QUERY_CHECK_CODE(code, lino, _end);
  //   code = streamWriteCheckPoint(pTask->task.streamId, buf, len);
  //   QUERY_CHECK_CODE(code, lino, _end);
  //   int32_t leaderSid = (*ppTask)->leaderSnodeId;
  //   SEpSet *epSet = gStreamMgmt.getSynEpset(leaderSid);
  //   if (epSet != NULL) {
  //     code = streamSyncWriteCheckpoint((*ppTask)->task.streamId, epSet, buf, len);
  //     QUERY_CHECK_CODE(code, lino, _end);
  //   }
  //   taosMemoryFree(buf);
  // }
  // if (pMsg->doCleanup) {
  //   streamDeleteCheckPoint((*ppTask)->task.streamId);
  //   int32_t leaderSid = (*ppTask)->leaderSnodeId;
  //   SEpSet *epSet = gStreamMgmt.getSynEpset(leaderSid);
  //   if (epSet != NULL) {
  //     code = streamSyncDeleteCheckpoint((*ppTask)->task.streamId, epSet);
  //     QUERY_CHECK_CODE(code, lino, _end);
  //   }
  // }

  taosWLockLatch(&gStreamTriggerWaitLatch);
  SListNode *pNode = TD_DLIST_HEAD(&gStreamTriggerWaitList);
  while (pNode != NULL) {
    SListNode *pCurNode = pNode;
    pNode = TD_DLIST_NODE_NEXT(pCurNode);
    StreamTriggerWaitInfo *pInfo = (StreamTriggerWaitInfo *)pCurNode->data;
    if (pInfo != NULL && pInfo->pTask == pTask) {
      TD_DLIST_POP(&gStreamTriggerWaitList, pCurNode);
      taosMemoryFreeClear(pCurNode);
    }
  }
  taosWUnLockLatch(&gStreamTriggerWaitLatch);

  if ((*ppTask)->triggerType == STREAM_TRIGGER_EVENT) {
    if ((*ppTask)->pStartCond != NULL) {
      nodesDestroyNode((*ppTask)->pStartCond);
      (*ppTask)->pStartCond = NULL;
    }
    if ((*ppTask)->pEndCond != NULL) {
      nodesDestroyNode((*ppTask)->pEndCond);
      (*ppTask)->pEndCond = NULL;
    }
    if ((*ppTask)->pStartCondCols != NULL) {
      nodesDestroyList((*ppTask)->pStartCondCols);
      (*ppTask)->pStartCondCols = NULL;
    }
    if ((*ppTask)->pEndCondCols != NULL) {
      nodesDestroyList((*ppTask)->pEndCondCols);
      (*ppTask)->pEndCondCols = NULL;
    }
  }

  if ((*ppTask)->partitionCols != NULL) {
    nodesDestroyList((*ppTask)->partitionCols);
    (*ppTask)->partitionCols = NULL;
  }

  if ((*ppTask)->pHistoryCutoffTime != NULL) {
    tSimpleHashCleanup((*ppTask)->pHistoryCutoffTime);
    (*ppTask)->pHistoryCutoffTime = NULL;
  }
  if ((*ppTask)->pRealtimeStartVer != NULL) {
    tSimpleHashCleanup((*ppTask)->pRealtimeStartVer);
    (*ppTask)->pRealtimeStartVer = NULL;
  }

  if ((*ppTask)->pRealtimeCtx != NULL) {
    strtcDestroy(&(*ppTask)->pRealtimeCtx);
  }

  if ((*ppTask)->pCalcExecCount != NULL) {
    taosMemFreeClear((*ppTask)->pCalcExecCount);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  (*cb)(ppTask);

  return code;
}

int32_t stTriggerTaskExecute(SStreamTriggerTask *pTask, const SStreamMsg *pMsg) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  switch (pMsg->msgType) {
    case STREAM_MSG_START: {
      // todo
      // if (streamCheckpointIsReady(pTask->task.streamId)){
      //   void* data = NULL;
      //   int64_t dataLen = 0;
      //   code = streamReadCheckPoint(pTask->task.streamId, &data, &dataLen);
      // } else {
      //   // retry
      // }

      if (pTask->pRealtimeCtx == NULL) {
        pTask->pRealtimeCtx = taosMemoryCalloc(1, sizeof(SSTriggerRealtimeContext));
        QUERY_CHECK_NULL(pTask->pRealtimeCtx, code, lino, _end, terrno);
        code = strtcInit(pTask->pRealtimeCtx, pTask);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      if (pTask->pHistoryCtx == NULL) {
        pTask->pHistoryCtx = taosMemoryCalloc(1, sizeof(SSTriggerHistoryContext));
        QUERY_CHECK_NULL(pTask->pHistoryCtx, code, lino, _end, terrno);
        code = sthcInit(pTask->pHistoryCtx, pTask);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      if (taosArrayGetSize(pTask->readerList) == 0) {
        code = strtcResumeCheck(pTask->pRealtimeCtx);
        QUERY_CHECK_CODE(code, lino, _end);
      } else {
        pTask->pRealtimeCtx->curReaderIdx = 0;
        SStreamTaskAddr *pReader = taosArrayGet(pTask->readerList, 0);
        code = strtcSendPullReq(pTask->pRealtimeCtx, STRIGGER_PULL_LAST_TS, pReader);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      pTask->task.status = STREAM_STATUS_RUNNING;
      break;
    }
    case STREAM_MSG_ORIGTBL_READER_INFO: {
      // todo(kjq): handle original table reader info
      break;
    }
    default: {
      ST_TASK_ELOG("invalid stream trigger message type %d", pMsg->msgType);
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t streamTriggerProcessRsp(SStreamTask *pStreamTask, SRpcMsg *pRsp, int64_t *pErrTaskId) {
  int32_t             code = 0;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = (SStreamTriggerTask *)pStreamTask;

  *pErrTaskId = pStreamTask->taskId;

  if (gStreamTriggerToStop) {
    ST_TASK_DLOG("skip process stream trigger response %p with type %d since dnode is stopping", pRsp, pRsp->msgType);
    goto _end;
  }

  if (pRsp->msgType == TDMT_STREAM_TRIGGER_PULL_RSP) {
    SSTriggerPullRequest *pReq = pRsp->info.ahandle;
    switch (pReq->type) {
      case STRIGGER_PULL_FIRST_TS:
      case STRIGGER_PULL_TSDB_META:
      case STRIGGER_PULL_TSDB_META_NEXT:
      case STRIGGER_PULL_TSDB_TS_DATA:
      case STRIGGER_PULL_TSDB_TRIGGER_DATA:
      case STRIGGER_PULL_TSDB_TRIGGER_DATA_NEXT:
      case STRIGGER_PULL_TSDB_CALC_DATA:
      case STRIGGER_PULL_TSDB_CALC_DATA_NEXT: {
        SSTriggerHistoryContext *pContext = pTask->pHistoryCtx;
        QUERY_CHECK_CONDITION(pReq == &pContext->pullReq.base, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        if (pRsp->code == TSDB_CODE_SUCCESS) {
          SSDataBlock *pResBlock = pContext->pullResDataBlock[pReq->type];
          if (pResBlock == NULL) {
            pResBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
            QUERY_CHECK_NULL(pResBlock, code, lino, _end, terrno);
            pContext->pullResDataBlock[pReq->type] = pResBlock;
          }
          if (pRsp->contLen == 0) {
            blockDataEmpty(pResBlock);
          } else if (pReq->type == STRIGGER_PULL_FIRST_TS) {
            code = tDeserializeSStreamTsResponse(pRsp->pCont, pRsp->contLen, pResBlock);
            QUERY_CHECK_CODE(code, lino, _end);
          } else {
            const char *pEnd = pRsp->pCont;
            code = blockDecode(pResBlock, pRsp->pCont, &pEnd);
            QUERY_CHECK_CODE(code, lino, _end);
            QUERY_CHECK_CONDITION(pEnd == pRsp->pCont + pRsp->contLen, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
          }
          code = sthcProcessPullRsp(pContext, pResBlock);
          QUERY_CHECK_CODE(code, lino, _end);
        } else {
          // todo(kjq): handle error code
        }
        break;
      }
      case STRIGGER_PULL_LAST_TS:
      case STRIGGER_PULL_WAL_META:
      case STRIGGER_PULL_WAL_TS_DATA:
      case STRIGGER_PULL_WAL_TRIGGER_DATA:
      case STRIGGER_PULL_WAL_CALC_DATA: {
        SSTriggerRealtimeContext *pContext = pTask->pRealtimeCtx;
        QUERY_CHECK_CONDITION(pReq == &pContext->pullReq.base, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        if (pRsp->code == TSDB_CODE_SUCCESS) {
          SSDataBlock *pResBlock = pContext->pullResDataBlock[pReq->type];
          if (pResBlock == NULL) {
            pResBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
            QUERY_CHECK_NULL(pResBlock, code, lino, _end, terrno);
            pContext->pullResDataBlock[pReq->type] = pResBlock;
            if (pReq->type == STRIGGER_PULL_WAL_TS_DATA || pReq->type == STRIGGER_PULL_WAL_TRIGGER_DATA ||
                pReq->type == STRIGGER_PULL_WAL_CALC_DATA) {
              // ownership of the data block will be transfered to the merger
              pContext->pullResDataBlock[pReq->type] = NULL;
            }
          }
          if (pRsp->code == TSDB_CODE_WAL_LOG_NOT_EXIST && pReq->type == STRIGGER_PULL_WAL_META) {
            QUERY_CHECK_CONDITION(pRsp->contLen == 8, code, lino, _end, TSDB_CODE_INVALID_PARA);
            blockDataEmpty(pResBlock);
            pResBlock->info.id.groupId = *(int64_t *)pRsp->pCont;
          } else if (pRsp->contLen == 0) {
            blockDataEmpty(pResBlock);
          } else if (pReq->type == STRIGGER_PULL_LAST_TS) {
            code = tDeserializeSStreamTsResponse(pRsp->pCont, pRsp->contLen, pResBlock);
            QUERY_CHECK_CODE(code, lino, _end);
          } else {
            const char *pEnd = pRsp->pCont;
            code = blockDecode(pResBlock, pRsp->pCont, &pEnd);
            QUERY_CHECK_CODE(code, lino, _end);
            QUERY_CHECK_CONDITION(pEnd == pRsp->pCont + pRsp->contLen, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
          }
          code = strtcProcessPullRsp(pContext, pResBlock);
          QUERY_CHECK_CODE(code, lino, _end);
        } else if (pRsp->code == TSDB_CODE_STREAM_TASK_NOT_EXIST) {
          pContext->retryPull = true;
          int64_t resumeTime = taosGetTimestampMs() + STREAM_ACT_MIN_DELAY_MSEC;
          code = streamTriggerAddWaitTask(pTask, resumeTime);
          QUERY_CHECK_CODE(code, lino, _end);
        } else {
          *pErrTaskId = pReq->readerTaskId;
          code = pRsp->code;
          QUERY_CHECK_CODE(code, lino, _end);
        }
        break;
      }
      case STRIGGER_PULL_GROUP_COL_VALUE: {
        SSTriggerRealtimeContext *pContext = pTask->pRealtimeCtx;
        QUERY_CHECK_CONDITION(pReq == &pContext->pullReq.base, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        if (pRsp->code == TSDB_CODE_SUCCESS) {
          SSTriggerCalcRequest *pCalcReq = &pContext->calcReq;
          SStreamGroupInfo      groupInfo = {.gInfo = pCalcReq->groupColVals};
          code = tDeserializeSStreamGroupInfo(pRsp->pCont, pRsp->contLen, &groupInfo);
          QUERY_CHECK_CODE(code, lino, _end);
          pContext->pullStatus = STRIGGER_REQUEST_IDLE;
          pCalcReq->groupColVals = groupInfo.gInfo;
          code = strtcSendCalcReq(pContext);
          QUERY_CHECK_CODE(code, lino, _end);
        } else if (pRsp->code == TSDB_CODE_STREAM_TASK_NOT_EXIST) {
          pContext->retryPull = true;
          int64_t resumeTime = taosGetTimestampMs() + STREAM_ACT_MIN_DELAY_MSEC;
          code = streamTriggerAddWaitTask(pTask, resumeTime);
          QUERY_CHECK_CODE(code, lino, _end);
        } else {
          *pErrTaskId = pReq->readerTaskId;
          code = pRsp->code;
          QUERY_CHECK_CODE(code, lino, _end);
        }
        break;
      }
      default: {
        ST_TASK_ELOG("invalid stream trigger pull type %d", pReq->type);
        code = TSDB_CODE_INVALID_PARA;
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
  } else if (pRsp->msgType == TDMT_STREAM_TRIGGER_CALC_RSP) {
    SSTriggerCalcRequest     *pReq = pRsp->info.ahandle;
    SSTriggerRealtimeContext *pContext = pTask->pRealtimeCtx;
    QUERY_CHECK_CONDITION(pReq == &pContext->calcReq, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    if (pRsp->code == TSDB_CODE_SUCCESS) {
      taosArrayClearEx(pReq->params, tDestroySSTriggerCalcParam);
      taosArrayClearEx(pReq->groupColVals, tDestroySStreamGroupValue);
      code = strtcProcessCalcRsp(pContext, pRsp->code);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      // todo(kjq): handle error code
      *pErrTaskId = pReq->runnerTaskId;
      code = pRsp->code;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}
