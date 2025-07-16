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
#include "osMemPool.h"
#include "plannodes.h"
#include "streamInt.h"
#include "streamReader.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "thash.h"
#include "ttime.h"

#define STREAM_TRIGGER_CHECK_INTERVAL_MS    1000                    // 1s
#define STREAM_TRIGGER_WAIT_TIME_NS         1 * NANOSECOND_PER_SEC  // 1s, todo(kjq): increase the wait time to 10s
#define STREAM_TRIGGER_BATCH_WINDOW_WAIT_NS 1 * NANOSECOND_PER_SEC  // 1s, todo(kjq): increase the wait time to 30s
#define SSTRIGGER_REALTIME_SESSIONID        1
#define SSTRIGGER_HISTORY_SESSIONID         2

#define IS_TRIGGER_GROUP_NONE_WINDOW(pGroup) (TRINGBUF_CAPACITY(&(pGroup)->winBuf) == 0)
#define IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup) (TRINGBUF_SIZE(&(pGroup)->winBuf) > 0)

static int32_t stRealtimeGroupInit(SSTriggerRealtimeGroup *pGroup, SSTriggerRealtimeContext *pContext, int64_t gid);
static void    stRealtimeGroupDestroy(void *ptr);
// Add metadatas to the group, which are used to check trigger conditions later.
static int32_t stRealtimeGroupAddMetaDatas(SSTriggerRealtimeGroup *pGroup, SArray *pMetadatas, SArray *pVgIds);
// Use metadatas to check trigger conditions, and generate notification and calculation requests.
static int32_t stRealtimeGroupCheck(SSTriggerRealtimeGroup *pGroup);
// Get the next data block from the group, which comes from the metadatas in the group.
static int32_t stRealtimeGroupGetDataBlock(SSTriggerRealtimeGroup *pGroup, bool saveWindow, SSDataBlock **ppDataBlock,
                                           int32_t *pStartIdx, int32_t *pEndIdx, bool *pAllTableProcessed,
                                           bool *pNeedFetchData);
// Clear all temporary states and variables in the group after checking.
static void stRealtimeGroupClearTempState(SSTriggerRealtimeGroup *pGroup);
// Clear metadatas that have been checked
static void stRealtimeGroupClearMetadatas(SSTriggerRealtimeGroup *pGroup);

static int32_t stHistoryGroupInit(SSTriggerHistoryGroup *pGroup, SSTriggerHistoryContext *pContext, int64_t gid);
static void    stHistoryGroupDestroy(void *ptr);
static int32_t stHistoryGroupAddMetaDatas(SSTriggerHistoryGroup *pGroup, SSDataBlock *pMetaDataBlock, bool *pAdded);
static int32_t stHistoryGroupCheck(SSTriggerHistoryGroup *pGroup);
static int32_t stHistoryGroupGetDataBlock(SSTriggerHistoryGroup *pGroup, bool saveWindow, SSDataBlock **ppDataBlock,
                                          int32_t *pStartIdx, int32_t *pEndIdx, bool *pAllTableProcessed,
                                          bool *pNeedFetchData);
static void    stHistoryGroupClearTempState(SSTriggerHistoryGroup *pGroup);
static void    stHistoryGroupClearMetadatas(SSTriggerHistoryGroup *pGroup);

static int32_t stRealtimeContextInit(SSTriggerRealtimeContext *pContext, SStreamTriggerTask *pTask);
static void    stRealtimeContextDestroy(void *ptr);
// Start or continue the realtime context after receiving pull/calc responses.
static int32_t stRealtimeContextCheck(SSTriggerRealtimeContext *pContext);
// Process the pull responses from readers.
static int32_t stRealtimeContextProcPullRsp(SSTriggerRealtimeContext *pContext, SRpcMsg *pRsp);
// Process the calc responses from runners.
static int32_t stRealtimeContextProcCalcRsp(SSTriggerRealtimeContext *pContext, SRpcMsg *pRsp);

static int32_t stHistoryContextInit(SSTriggerHistoryContext *pContext, SStreamTriggerTask *pTask);
static void    stHistoryContextDestroy(void *ptr);
static int32_t stHistoryContextCheck(SSTriggerHistoryContext *pContext);
static int32_t stHistoryContextProcPullRsp(SSTriggerHistoryContext *pContext, SRpcMsg *pRsp);
static int32_t stHistoryContextProcCalcRsp(SSTriggerHistoryContext *pContext, SRpcMsg *pRsp);

typedef struct SRewriteSlotidCxt {
  int32_t errCode;
  SArray *newSlotIds;
} SRewriteSlotidCxt;

typedef struct SSTriggerOrigTableInfo {
  int32_t    vgId;
  int64_t    suid;
  int64_t    uid;
  SSHashObj *pColumns;  // SSHashObj<col_name, col_id>
} SSTriggerOrigTableInfo;

typedef struct StreamTriggerWaitInfo {
  int64_t streamId;
  int64_t taskId;
  int64_t sessionId;
  int64_t resumeTime;
} StreamTriggerWaitInfo;

static EDealRes nodeRewriteSlotid(SNode *pNode, void *pContext) {
  SRewriteSlotidCxt *pCxt = (SRewriteSlotidCxt *)pContext;
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    SColumnNode *pCol = (SColumnNode *)pNode;
    void        *px = taosArrayGet(pCxt->newSlotIds, pCol->slotId);
    if (px == NULL) {
      pCxt->errCode = terrno;
      return DEAL_RES_ERROR;
    }
    pCol->slotId = *(int32_t *)px;
    return DEAL_RES_IGNORE_CHILD;
  }
  return DEAL_RES_CONTINUE;
}

static TdThreadOnce     gStreamTriggerModuleInit = PTHREAD_ONCE_INIT;
static volatile int32_t gStreamTriggerInitRes = TSDB_CODE_SUCCESS;
// When the trigger task's real-time calculation catches up with the latest WAL
// progress, it will wait and be awakened later by a timer.
static SRWLatch gStreamTriggerWaitLatch;
static SList    gStreamTriggerWaitList;
static tmr_h    gStreamTriggerTimerId = NULL;

static int32_t stTriggerTaskAddWaitSession(SStreamTriggerTask *pTask, int64_t sessionId, int64_t resumeTime) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  StreamTriggerWaitInfo info = {
      .streamId = pTask->task.streamId,
      .taskId = pTask->task.taskId,
      .sessionId = sessionId,
      .resumeTime = resumeTime,
  };
  taosWLockLatch(&gStreamTriggerWaitLatch);
  code = tdListAppend(&gStreamTriggerWaitList, &info);
  taosWUnLockLatch(&gStreamTriggerWaitLatch);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("failed to add session %" PRIx64 " to wait list since %s", sessionId, tstrerror(code));
  } else {
    ST_TASK_DLOG("add session %" PRIx64 " to wait list, resumeTime:%" PRId64, sessionId, resumeTime);
  }
  return code;
}

static void stTriggerTaskCheckWaitSession(void *param, void *tmrId) {
  int64_t    now = taosGetTimestampNs();
  SListIter  iter = {0};
  SListNode *pNode = NULL;
  SList      readylist = {0};

  taosWLockLatch(&gStreamTriggerWaitLatch);
  tdListInitIter(&gStreamTriggerWaitList, &iter, TD_LIST_FORWARD);
  while ((pNode = tdListNext(&iter)) != NULL) {
    StreamTriggerWaitInfo *pInfo = (StreamTriggerWaitInfo *)pNode->data;
    if (pInfo->resumeTime <= now) {
      TD_DLIST_POP(&gStreamTriggerWaitList, pNode);
      TD_DLIST_APPEND(&readylist, pNode);
    }
  }
  taosWUnLockLatch(&gStreamTriggerWaitLatch);

  tdListInitIter(&readylist, &iter, TD_LIST_FORWARD);
  while ((pNode = tdListNext(&iter)) != NULL) {
    StreamTriggerWaitInfo *pInfo = (StreamTriggerWaitInfo *)pNode->data;
    SStreamTask           *task = NULL;
    void                  *taskAddr = NULL;
    int32_t                code = streamAcquireTask(pInfo->streamId, pInfo->taskId, &task, &taskAddr);
    if (code != TSDB_CODE_SUCCESS) {
      stError("failed to acquire stream trigger task %" PRIx64 "-%" PRIx64 "-%" PRIx64 " since %s", pInfo->streamId,
              pInfo->taskId, pInfo->sessionId, tstrerror(code));
      TD_DLIST_POP(&readylist, pNode);
      taosMemoryFreeClear(pNode);
      continue;
    }

    // resume the session
    SStreamTriggerTask *pTask = (SStreamTriggerTask *)task;
    if (pInfo->sessionId == SSTRIGGER_REALTIME_SESSIONID) {
      SSTriggerRealtimeContext *pContext = pTask->pRealtimeContext;
      code = stRealtimeContextCheck(pContext);
    } else if (pInfo->sessionId == SSTRIGGER_HISTORY_SESSIONID) {
      SSTriggerHistoryContext *pContext = pTask->pHistoryContext;
      code = stHistoryContextCheck(pContext);
    }
    if (code != TSDB_CODE_SUCCESS) {
      atomic_store_32(&pTask->task.errorCode, code);
      atomic_store_32((int32_t *)&pTask->task.status, STREAM_STATUS_FAILED);
      stError("failed to resume stream trigger session %" PRIx64 "-%" PRIx64 "-%" PRIx64 " since %s", pInfo->streamId,
              pInfo->taskId, pInfo->sessionId, tstrerror(code));
    } else {
      stDebug("resume stream trigger session %" PRIx64 "-%" PRIx64 "-%" PRIx64 " since now:%" PRId64
              ", resumeTime:%" PRId64,
              pInfo->streamId, pInfo->taskId, pInfo->sessionId, now, pInfo->resumeTime);
    }

    TD_DLIST_POP(&readylist, pNode);
    taosMemoryFreeClear(pNode);
    streamReleaseTask(taskAddr);
  }

  streamTmrStart(stTriggerTaskCheckWaitSession, STREAM_TRIGGER_CHECK_INTERVAL_MS, NULL, gStreamMgmt.timer,
                 &gStreamTriggerTimerId, "stream-trigger");
  return;
}

static void stTriggerTaskEnvDoInit() {
  taosInitRWLatch(&gStreamTriggerWaitLatch);
  tdListInit(&gStreamTriggerWaitList, sizeof(StreamTriggerWaitInfo));
  streamTmrStart(stTriggerTaskCheckWaitSession, STREAM_TRIGGER_CHECK_INTERVAL_MS, NULL, gStreamMgmt.timer,
                 &gStreamTriggerTimerId, "stream-trigger");
}

int32_t stTriggerTaskEnvInit() {
  int32_t code = taosThreadOnce(&gStreamTriggerModuleInit, stTriggerTaskEnvDoInit);
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to init stream trigger module since %s", tstrerror(code));
    return code;
  }
  return atomic_load_32(&gStreamTriggerInitRes);
}

void stTriggerTaskEnvCleanup() {
  streamTmrStop(gStreamTriggerTimerId);
  taosWLockLatch(&gStreamTriggerWaitLatch);
  tdListEmpty(&gStreamTriggerWaitList);
  taosWUnLockLatch(&gStreamTriggerWaitLatch);
}

static int32_t stTriggerTaskAllocAhandle(SStreamTriggerTask *pTask, int64_t sessionId, void *param, void **ppAhandle) {
  int32_t code = 0, lino = 0;

  SMsgSendInfo *pInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  TSDB_CHECK_NULL(pInfo, code, lino, _exit, terrno);

  pInfo->param = taosMemoryCalloc(1, sizeof(SSTriggerAHandle));
  TSDB_CHECK_NULL(pInfo->param, code, lino, _exit, terrno);

  pInfo->paramFreeFp = taosAutoMemoryFree;

  SSTriggerAHandle *pRes = pInfo->param;
  pRes->streamId = pTask->task.streamId;
  pRes->taskId = pTask->task.taskId;
  pRes->sessionId = sessionId;
  pRes->param = param;

  *ppAhandle = pInfo;

_exit:

  if (code) {
    taosMemoryFree(pInfo);
    ST_TASK_ELOG("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static STimeWindow stTriggerTaskGetIntervalWindow(SStreamTriggerTask *pTask, int64_t ts) {
  SInterval  *pInterval = &pTask->interval;
  STimeWindow win;
  win.skey = taosTimeTruncate(ts, pInterval);
  win.ekey = taosTimeGetIntervalEnd(win.skey, pInterval);
  if (win.ekey < win.skey) {
    win.ekey = INT64_MAX;
  }
  return win;
}

static void stTriggerTaskPrevIntervalWindow(SStreamTriggerTask *pTask, STimeWindow *pWindow) {
  SInterval *pInterval = &pTask->interval;
  TSKEY      prevStart =
      taosTimeAdd(pWindow->skey, -1 * pInterval->offset, pInterval->offsetUnit, pInterval->precision, NULL);
  prevStart = taosTimeAdd(prevStart, -1 * pInterval->sliding, pInterval->slidingUnit, pInterval->precision, NULL);
  prevStart = taosTimeAdd(prevStart, pInterval->offset, pInterval->offsetUnit, pInterval->precision, NULL);
  pWindow->skey = prevStart;
  pWindow->ekey = taosTimeGetIntervalEnd(prevStart, pInterval);
}

static void stTriggerTaskNextIntervalWindow(SStreamTriggerTask *pTask, STimeWindow *pWindow) {
  SInterval *pInterval = &pTask->interval;
  TSKEY      nextStart =
      taosTimeAdd(pWindow->skey, -1 * pInterval->offset, pInterval->offsetUnit, pInterval->precision, NULL);
  nextStart = taosTimeAdd(nextStart, pInterval->sliding, pInterval->slidingUnit, pInterval->precision, NULL);
  nextStart = taosTimeAdd(nextStart, pInterval->offset, pInterval->offsetUnit, pInterval->precision, NULL);
  pWindow->skey = nextStart;
  pWindow->ekey = taosTimeGetIntervalEnd(nextStart, pInterval);
}

static STimeWindow stTriggerTaskGetPeriodWindow(SStreamTriggerTask *pTask, int64_t ts) {
  SInterval *pInterval = &pTask->interval;
  int64_t    day = convertTimePrecision(24 * 60 * 60 * 1000, TSDB_TIME_PRECISION_MILLI, pInterval->precision);
  // truncate to the start of day
  SInterval   interval = {.intervalUnit = 'd',
                          .slidingUnit = 'd',
                          .offsetUnit = pInterval->offsetUnit,
                          .precision = pInterval->precision,
                          .interval = day,
                          .sliding = day};
  int64_t     first = taosTimeTruncate(ts, &interval) + pInterval->offset;
  STimeWindow win;
  if (pInterval->sliding > day) {
    if (first >= ts) {
      win.skey = first - pInterval->sliding + 1;
      win.ekey = first;
    } else {
      win.skey = first + 1;
      win.ekey = first + pInterval->sliding;
    }
  } else {
    if (first >= ts) {
      int64_t prev = first - day;
      win.skey = (ts - prev - 1) / pInterval->sliding * pInterval->sliding + prev + 1;
      win.ekey = first;
    } else {
      win.skey = (ts - first - 1) / pInterval->sliding * pInterval->sliding + first + 1;
      win.ekey = win.skey + pInterval->sliding - 1;
    }
  }
  return win;
}

static void stTriggerTaskNextPeriodWindow(SStreamTriggerTask *pTask, STimeWindow *pWindow) {
  SInterval *pInterval = &pTask->interval;
  int64_t    day = convertTimePrecision(24 * 60 * 60 * 1000, TSDB_TIME_PRECISION_MILLI, pInterval->precision);
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
                          .interval = day,
                          .sliding = day};
    int64_t   first = taosTimeTruncate(pWindow->ekey, &interval) + pInterval->offset;
    if (first > pWindow->skey && first < pWindow->ekey) {
      pWindow->ekey = first;
    }
  }
}

static int32_t stTriggerTaskGenCheckpoint(SStreamTriggerTask *pTask, uint8_t *buf, int64_t *pLen) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pTask->pRealtimeContext;
  SEncoder                  encoder = {0};
  int32_t                   iter = 0;
  tEncoderInit(&encoder, buf, *pLen);
  static int32_t ver = 0;
  code = tEncodeI32(&encoder, ver);  // version
  QUERY_CHECK_CODE(code, lino, _end);
  code = tEncodeI64(&encoder, pTask->task.streamId);
  QUERY_CHECK_CODE(code, lino, _end);
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
  void *px = tSimpleHashIterate(pContext->pGroups, NULL, &iter);
  while (px != NULL) {
    SSTriggerRealtimeGroup *pGroup = *(SSTriggerRealtimeGroup **)px;
    code = tEncodeI64(&encoder, pGroup->gid);
    QUERY_CHECK_CODE(code, lino, _end);
    px = tSimpleHashIterate(pContext->pGroups, px, &iter);
  }

  tEndEncode(&encoder);

  *pLen = encoder.pos;
  ST_TASK_DLOG("[checkpoint] generate checkpoint, ver %d, len:%" PRId64, ver, *pLen);

_end:
  tEncoderClear(&encoder);
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stTriggerTaskGenVirColRefs(SStreamTriggerTask *pTask, VTableInfo *pInfo, SArray *pSlots,
                                          SArray **ppColRefs) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t nCols = taosArrayGetSize(pSlots);

  for (int32_t i = 0; i < nCols; i++) {
    int32_t          slotId = *(int32_t *)TARRAY_GET_ELEM(pSlots, i);
    SColumnInfoData *pCol = TARRAY_GET_ELEM(pTask->pVirDataBlock->pDataBlock, slotId);
    col_id_t         colId = pCol->info.colId;
    SColRef         *pColRef = NULL;
    for (int32_t j = 0; j < pInfo->cols.nCols; j++) {
      SColRef *pTmpColRef = &pInfo->cols.pColRef[j];
      if (pTmpColRef->hasRef && pTmpColRef->id == colId) {
        pColRef = pTmpColRef;
        break;
      }
    }
    if (pColRef == NULL) {
      continue;
    }
    if (*ppColRefs == NULL) {
      *ppColRefs = taosArrayInit(0, sizeof(SSTriggerTableColRef));
      QUERY_CHECK_NULL(*ppColRefs, code, lino, _end, terrno);
    }

    size_t dbNameLen = strlen(pColRef->refDbName) + 1;
    size_t tbNameLen = strlen(pColRef->refTableName) + 1;
    size_t colNameLen = strlen(pColRef->refColName) + 1;
    void  *px = tSimpleHashGet(pTask->pOrigTableCols, pColRef->refDbName, dbNameLen);
    QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    SSHashObj              *pDbInfo = *(SSHashObj **)px;
    SSTriggerOrigTableInfo *pTbInfo = tSimpleHashGet(pDbInfo, pColRef->refTableName, tbNameLen);
    QUERY_CHECK_NULL(pTbInfo, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    col_id_t *pOrigColId = tSimpleHashGet(pTbInfo->pColumns, pColRef->refColName, colNameLen);
    QUERY_CHECK_NULL(pOrigColId, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

    SSTriggerTableColRef *pRef = NULL;
    for (int32_t j = 0; j < TARRAY_SIZE(*ppColRefs); j++) {
      SSTriggerTableColRef *pTmpRef = TARRAY_GET_ELEM(*ppColRefs, j);
      if (pTmpRef->otbSuid == pTbInfo->suid && pTmpRef->otbUid == pTbInfo->uid) {
        pRef = pTmpRef;
        break;
      }
    }
    if (pRef == NULL) {
      pRef = taosArrayReserve(*ppColRefs, 1);
      QUERY_CHECK_NULL(pRef, code, lino, _end, terrno);
      pRef->otbSuid = pTbInfo->suid;
      pRef->otbUid = pTbInfo->uid;
      pRef->otbVgId = pTbInfo->vgId;
      pRef->pColMatches = taosArrayInit(0, sizeof(SSTriggerColMatch));
      QUERY_CHECK_NULL(pRef->pColMatches, code, lino, _end, terrno);
    }
    SSTriggerColMatch *pMatch = taosArrayReserve(pRef->pColMatches, 1);
    pMatch->otbColId = *pOrigColId;
    pMatch->vtbColId = colId;
    pMatch->vtbSlotId = slotId;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t stTriggerTaskAcquireRequest(SStreamTriggerTask *pTask, int64_t sessionId, int64_t gid,
                                    SSTriggerCalcRequest **ppRequest) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  int32_t            nCalcNodes = 0;
  int32_t            nIdleSlots = 0;
  SSTriggerCalcNode *pNode = NULL;
  bool              *pRunningFlag = NULL;
  bool               needUnlock = false;

  *ppRequest = NULL;

  taosWLockLatch(&pTask->calcPoolLock);
  needUnlock = true;

  // check if have any free slot
  nCalcNodes = taosArrayGetSize(pTask->pCalcNodes);
  for (int32_t i = 0; i < nCalcNodes; i++) {
    pNode = TARRAY_GET_ELEM(pTask->pCalcNodes, i);
    nIdleSlots += TD_DLIST_NELES(&pNode->idleSlots);
  }
  if (nIdleSlots == 0) {
    goto _end;
  }

  // check if the group is running
  int64_t p[2] = {sessionId, gid};
  pRunningFlag = tSimpleHashGet(pTask->pGroupRunning, p, sizeof(p));
  if (pRunningFlag == NULL) {
    bool *flag = taosMemoryCalloc(nCalcNodes + 1, sizeof(bool));
    QUERY_CHECK_NULL(flag, code, lino, _end, terrno);
    code = tSimpleHashPut(pTask->pGroupRunning, p, sizeof(p), flag, nCalcNodes + 1);
    taosMemoryFree(flag);
    QUERY_CHECK_CODE(code, lino, _end);
    pRunningFlag = tSimpleHashGet(pTask->pGroupRunning, p, sizeof(p));
    QUERY_CHECK_NULL(pRunningFlag, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  }
  if (pRunningFlag[0] == true) {
    goto _end;
  }

  // use weighted average to select the free slot
  int32_t rnd = taosRand() % nIdleSlots;
  for (int32_t i = 0; i < nCalcNodes; i++) {
    pNode = TARRAY_GET_ELEM(pTask->pCalcNodes, i);
    if (TD_DLIST_NELES(&pNode->idleSlots) > rnd) {
      break;
    }
    rnd -= TD_DLIST_NELES(&pNode->idleSlots);
  }
  SSTriggerCalcSlot *pSlot = TD_DLIST_HEAD(&pNode->idleSlots);
  QUERY_CHECK_NULL(pSlot, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

  SSTriggerCalcRequest *pReq = &pSlot->req;
  int32_t               idx = TARRAY_ELEM_IDX(pTask->pCalcNodes, pNode);
  SStreamRunnerTarget  *pRunner = taosArrayGet(pTask->runnerList, idx);
  QUERY_CHECK_NULL(pRunner, code, lino, _end, terrno);
  pReq->streamId = pTask->task.streamId;
  pReq->runnerTaskId = pRunner->addr.taskId;
  pReq->sessionId = sessionId;
  pReq->triggerType = pTask->triggerType;
  pReq->triggerTaskId = pTask->task.taskId;
  pReq->gid = gid;
  if (pReq->params == NULL) {
    pReq->params = taosArrayInit(0, sizeof(SSTriggerCalcParam));
    QUERY_CHECK_NULL(pReq->params, code, lino, _end, terrno);
  } else {
    taosArrayClearEx(pReq->params, tDestroySSTriggerCalcParam);
  }
  if (pReq->groupColVals == NULL) {
    pReq->groupColVals = taosArrayInit(0, sizeof(SStreamGroupValue));
    QUERY_CHECK_NULL(pReq->groupColVals, code, lino, _end, terrno);
  } else {
    taosArrayClearEx(pReq->groupColVals, tDestroySStreamGroupValue);
  }
  pReq->createTable = (pRunningFlag[idx + 1] == false);
  pRunningFlag[0] = true;

  *ppRequest = pReq;
  TD_DLIST_POP(&pNode->idleSlots, pSlot);

_end:
  if (needUnlock) {
    taosWUnLockLatch(&pTask->calcPoolLock);
  }
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t stTriggerTaskReleaseRequest(SStreamTriggerTask *pTask, SSTriggerCalcRequest **ppRequest) {
  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               lino = 0;
  SSTriggerCalcRequest *pReq = NULL;
  SSTriggerCalcNode    *pNode = NULL;
  bool                 *pRunningFlag = NULL;
  bool                  needUnlock = false;
  bool                  hasSent = false;

  pReq = *ppRequest;
  *ppRequest = NULL;
  hasSent = taosArrayGetSize(pReq->params) > 0;
  taosArrayClearEx(pReq->params, tDestroySSTriggerCalcParam);
  taosArrayClearEx(pReq->groupColVals, tDestroySStreamGroupValue);

  int32_t idx = 0;
  int32_t nRunners = taosArrayGetSize(pTask->runnerList);
  while (idx < nRunners) {
    SStreamRunnerTarget *pRunner = TARRAY_GET_ELEM(pTask->runnerList, idx);
    if (pRunner->addr.taskId == pReq->runnerTaskId) {
      break;
    }
    idx++;
  }
  QUERY_CHECK_CONDITION(idx < nRunners, code, lino, _end, TSDB_CODE_INVALID_PARA);

  taosWLockLatch(&pTask->calcPoolLock);
  needUnlock = true;

  int64_t p[] = {pReq->sessionId, pReq->gid};
  pRunningFlag = tSimpleHashGet(pTask->pGroupRunning, p, sizeof(p));
  QUERY_CHECK_NULL(pRunningFlag, code, lino, _end, TSDB_CODE_INVALID_PARA);
  pRunningFlag[0] = false;
  pRunningFlag[idx + 1] = hasSent;

  pNode = taosArrayGet(pTask->pCalcNodes, idx);
  QUERY_CHECK_NULL(pNode, code, lino, _end, terrno);
  SSTriggerCalcSlot *pSlot = (SSTriggerCalcSlot *)pReq;
  int32_t            eIdx = TARRAY_ELEM_IDX(pNode->pSlots, pSlot);
  QUERY_CHECK_CONDITION(eIdx >= 0 && eIdx < TARRAY_SIZE(pNode->pSlots), code, lino, _end, TSDB_CODE_INVALID_PARA);
  TD_DLIST_APPEND(&pNode->idleSlots, pSlot);

_end:
  if (needUnlock) {
    taosWUnLockLatch(&pTask->calcPoolLock);
  }
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t stTriggerTaskMarkRecalc(SStreamTriggerTask *pTask, int64_t groupId, int64_t skey, int64_t ekey) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pTask->pRealtimeContext;

  // todo(kjq): mark recalculation interval

#if !TRIGGER_USE_HISTORY_META
  if ((pTask->fillHistory || pTask->fillHistoryFirst) && pTask->fillHistoryStartTime > 0 &&
      ekey < pTask->fillHistoryStartTime) {
    goto _end;
  }

  pContext->haveToRecalc = true;
  int32_t               iter = 0;
  SSTriggerWalProgress *pProgress = tSimpleHashIterate(pContext->pReaderWalProgress, NULL, &iter);
  while (pProgress != NULL) {
    code = tSimpleHashPut(pTask->pRecalcLastVer, &pProgress->pTaskAddr->nodeId, sizeof(int32_t),
                          &pProgress->lastScanVer, sizeof(int64_t));
    QUERY_CHECK_CODE(code, lino, _end);
    pProgress = tSimpleHashIterate(pContext->pReaderWalProgress, pProgress, &iter);
  }
#endif

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stTriggerTaskCollectVirCols(SStreamTriggerTask *pTask, void *plan, SArray **ppColids,
                                           SNodeList **ppSlots) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SNode  *pScanPlan = NULL;
  SArray *pColids = NULL;

  *ppColids = NULL;
  *ppSlots = NULL;

  if (plan == NULL) {
    goto _end;
  }

  code = nodesStringToNode(plan, &pScanPlan);
  QUERY_CHECK_CODE(code, lino, _end);
  QUERY_CHECK_NULL(pScanPlan, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  QUERY_CHECK_CONDITION(nodeType(pScanPlan) == QUERY_NODE_PHYSICAL_SUBPLAN, code, lino, _end, TSDB_CODE_INVALID_PARA);
  STableScanPhysiNode *pScanNode = (STableScanPhysiNode *)((SSubplan *)pScanPlan)->pNode;
  QUERY_CHECK_CONDITION(nodeType(pScanNode) == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, code, lino, _end,
                        TSDB_CODE_INVALID_PARA);
  SNodeList *pSlots = pScanNode->scan.node.pOutputDataBlockDesc->pSlots;
  SNodeList *pScanCols = pScanNode->scan.pScanCols;

  int32_t nTrigCols = LIST_LENGTH(pSlots);
  pColids = taosArrayInit(nTrigCols, sizeof(col_id_t));
  QUERY_CHECK_NULL(pColids, code, lino, _end, terrno);
  SNode *pColNode = NULL;
  SNode *pSlotNode = NULL;
  FOREACH(pSlotNode, pSlots) {
    SSlotDescNode *p1 = (SSlotDescNode *)pSlotNode;
    FOREACH(pColNode, pScanCols) {
      STargetNode *pTarget = (STargetNode *)pColNode;
      SColumnNode *p2 = (SColumnNode *)pTarget->pExpr;
      if (nodeType(p2) == QUERY_NODE_COLUMN && pTarget->slotId == p1->slotId) {
        void *px = taosArrayPush(pColids, &p2->colId);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
        break;
      }
    }
  }

  *ppColids = pColids;
  pColids = NULL;
  *ppSlots = pSlots;
  pScanNode->scan.node.pOutputDataBlockDesc->pSlots = NULL;

_end:
  if (pScanPlan != NULL) {
    nodesDestroyNode(pScanPlan);
  }
  if (pColids != NULL) {
    taosArrayDestroy(pColids);
  }
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stTriggerTaskParseVirtScan(SStreamTriggerTask *pTask, void *triggerScanPlan, void *calcCacheScanPlan) {
  int32_t    code = TSDB_CODE_SUCCESS;
  int32_t    lino = 0;
  SArray    *pTrigColids = NULL;
  SArray    *pCalcColids = NULL;
  SNodeList *pTrigSlots = NULL;
  SNodeList *pCalcSlots = NULL;
  SArray    *pVirColIds = NULL;
  SArray    *pTrigSlotids = NULL;
  SArray    *pCalcSlotids = NULL;
  char      *infoBuf = NULL;
  int64_t    bufLen = 0;
  int64_t    bufCap = 1024;

  code = stTriggerTaskCollectVirCols(pTask, triggerScanPlan, &pTrigColids, &pTrigSlots);
  QUERY_CHECK_CODE(code, lino, _end);
  code = stTriggerTaskCollectVirCols(pTask, calcCacheScanPlan, &pCalcColids, &pCalcSlots);
  QUERY_CHECK_CODE(code, lino, _end);

  // combine all column ids from trig-cols and calc-cols
  int32_t nTrigCols = taosArrayGetSize(pTrigColids);
  int32_t nCalcCols = taosArrayGetSize(pCalcColids);
  pVirColIds = taosArrayInit(nTrigCols + nCalcCols, sizeof(col_id_t));
  QUERY_CHECK_NULL(pVirColIds, code, lino, _end, terrno);
  for (int32_t i = 0; i < nTrigCols; i++) {
    col_id_t id = *(col_id_t *)TARRAY_GET_ELEM(pTrigColids, i);
    void    *px = taosArrayPush(pVirColIds, &id);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
  }
  for (int32_t i = 0; i < nCalcCols; i++) {
    col_id_t id = *(col_id_t *)TARRAY_GET_ELEM(pCalcColids, i);
    void    *px = taosArrayPush(pVirColIds, &id);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
  }
  QUERY_CHECK_CONDITION(TARRAY_SIZE(pVirColIds) > 0, code, lino, _end, TSDB_CODE_INVALID_PARA);

  // sort and unique these column ids
  taosArraySort(pVirColIds, compareInt16Val);
  col_id_t *pColIds = pVirColIds->pData;
  int32_t   j = 0;
  for (int32_t i = 1; i < TARRAY_SIZE(pVirColIds); i++) {
    if (pColIds[i] != pColIds[j]) {
      ++j;
      pColIds[j] = pColIds[i];
    }
  }
  TARRAY_SIZE(pVirColIds) = j + 1;
  QUERY_CHECK_CONDITION(*(col_id_t *)TARRAY_DATA(pVirColIds) == PRIMARYKEY_TIMESTAMP_COL_ID, code, lino, _end,
                        TSDB_CODE_INVALID_PARA);

  if (stDebugFlag & DEBUG_DEBUG) {
    infoBuf = taosMemoryMalloc(bufCap);
    QUERY_CHECK_NULL(infoBuf, code, lino, _end, terrno);
  }

  if (infoBuf && bufLen < bufCap) {
    bufLen += tsnprintf(infoBuf + bufLen, bufCap - bufLen, "columnId in the datablock: {");
  }
  // create the data block for virtual table
  int32_t nTotalCols = TARRAY_SIZE(pVirColIds);
  code = createDataBlock(&pTask->pVirDataBlock);
  for (int32_t i = 0; i < nTotalCols; i++) {
    col_id_t       id = *(col_id_t *)TARRAY_GET_ELEM(pVirColIds, i);
    SSlotDescNode *pn = NULL;
    for (int32_t j = 0; j < nTrigCols; j++) {
      if (id == *(col_id_t *)TARRAY_GET_ELEM(pTrigColids, j)) {
        pn = (SSlotDescNode *)nodesListGetNode(pTrigSlots, j);
        break;
      }
    }
    for (int32_t j = 0; j < nCalcCols; j++) {
      if (id == *(col_id_t *)TARRAY_GET_ELEM(pCalcColids, j)) {
        pn = (SSlotDescNode *)nodesListGetNode(pCalcSlots, j);
        break;
      }
    }
    QUERY_CHECK_NULL(pn, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    SColumnInfoData col = createColumnInfoData(pn->dataType.type, pn->dataType.bytes, id);
    col.info.scale = pn->dataType.scale;
    col.info.precision = pn->dataType.precision;
    col.info.noData = pn->reserve;
    code = blockDataAppendColInfo(pTask->pVirDataBlock, &col);
    QUERY_CHECK_CODE(code, lino, _end);
    if (infoBuf && bufLen < bufCap) {
      bufLen += tsnprintf(infoBuf + bufLen, bufCap - bufLen, "%d,", id);
    }
  }

  if (infoBuf && bufLen < bufCap) {
    infoBuf[bufLen - 1] = '}';
    bufLen += tsnprintf(infoBuf + bufLen, bufCap - bufLen, "; slotId of trigger data:{");
  }

  // get new slot id of trig data block and calc data block
  pTrigSlotids = taosArrayInit(nTrigCols, sizeof(int32_t));
  QUERY_CHECK_NULL(pTrigSlotids, code, lino, _end, terrno);
  for (int32_t i = 0; i < nTrigCols; i++) {
    col_id_t id = *(col_id_t *)TARRAY_GET_ELEM(pTrigColids, i);
    int32_t  slotid = taosArraySearchIdx(pVirColIds, &id, compareInt16Val, TD_EQ);
    QUERY_CHECK_CONDITION(slotid >= 0, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    void *px = taosArrayPush(pTrigSlotids, &slotid);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
    if (infoBuf && bufLen < bufCap) {
      bufLen += tsnprintf(infoBuf + bufLen, bufCap - bufLen, "%d,", slotid);
    }
  }

  if (infoBuf && bufLen < bufCap) {
    infoBuf[bufLen - 1] = '}';
    bufLen += tsnprintf(infoBuf + bufLen, bufCap - bufLen, "; slotId of calc data:{");
  }

  pCalcSlotids = taosArrayInit(nCalcCols, sizeof(int32_t));
  QUERY_CHECK_NULL(pCalcSlotids, code, lino, _end, terrno);
  for (int32_t i = 0; i < nCalcCols; i++) {
    col_id_t id = *(col_id_t *)TARRAY_GET_ELEM(pCalcColids, i);
    int32_t  slotid = taosArraySearchIdx(pVirColIds, &id, compareInt16Val, TD_EQ);
    QUERY_CHECK_CONDITION(slotid >= 0, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    void *px = taosArrayPush(pCalcSlotids, &slotid);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
    if (infoBuf && bufLen < bufCap) {
      bufLen += tsnprintf(infoBuf + bufLen, bufCap - bufLen, "%d,", slotid);
    }
  }
  if (infoBuf && bufLen < bufCap) {
    infoBuf[bufLen - 1] = '}';
  }

  SRewriteSlotidCxt cxt = {
      .errCode = TSDB_CODE_SUCCESS,
      .newSlotIds = pTrigSlotids,
  };
  nodesWalkExpr(pTask->triggerFilter, nodeRewriteSlotid, &cxt);
  code = cxt.errCode;
  QUERY_CHECK_CODE(code, lino, _end);
  if (pTask->triggerType == STREAM_TRIGGER_STATE) {
    void *px = taosArrayGet(pTrigSlotids, pTask->stateSlotId);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
    pTask->stateSlotId = *(int32_t *)px;
  } else if (pTask->triggerType == STREAM_TRIGGER_EVENT) {
    nodesWalkExpr(pTask->pStartCond, nodeRewriteSlotid, &cxt);
    code = cxt.errCode;
    QUERY_CHECK_CODE(code, lino, _end);
    nodesWalkExpr(pTask->pEndCond, nodeRewriteSlotid, &cxt);
    code = cxt.errCode;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  pTask->pVirTrigSlots = pTrigSlotids;
  pTrigSlotids = NULL;
  pTask->pVirCalcSlots = pCalcSlotids;
  pCalcSlotids = NULL;

  if (infoBuf) {
    infoBuf[bufCap - 1] = '\0';
    ST_TASK_DLOG("virtual table info: %s", infoBuf);
  }

_end:
  if (pTrigColids != NULL) {
    taosArrayDestroy(pTrigColids);
  }
  if (pCalcColids != NULL) {
    taosArrayDestroy(pCalcColids);
  }
  if (pTrigSlots != NULL) {
    nodesDestroyList(pTrigSlots);
  }
  if (pCalcSlots != NULL) {
    nodesDestroyList(pCalcSlots);
  }
  if (pVirColIds != NULL) {
    taosArrayDestroy(pVirColIds);
  }
  if (pTrigSlotids != NULL) {
    taosArrayDestroy(pTrigSlotids);
  }
  if (pCalcSlotids != NULL) {
    taosArrayDestroy(pCalcSlotids);
  }
  if (infoBuf != NULL) {
    taosMemoryFreeClear(infoBuf);
  }
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void stTriggerTaskDestroyOrigTableInfo(void *ptr) {
  SSTriggerOrigTableInfo *pInfo = ptr;
  if (pInfo == NULL) {
    return;
  }
  if (pInfo->pColumns != NULL) {
    tSimpleHashCleanup(pInfo->pColumns);
  }
}

static void stTriggerTaskDestroyOrigDbInfo(void *ptr) {
  SSHashObj **ppInfos = ptr;
  if (ppInfos == NULL || *ppInfos == NULL) {
    return;
  }
  tSimpleHashSetFreeFp(*ppInfos, stTriggerTaskDestroyOrigTableInfo);
  tSimpleHashCleanup(*ppInfos);
  *ppInfos = NULL;
}

static void stTriggerTaskDestroyReaderUids(void *ptr) {
  SArray **ppUids = ptr;
  if (ppUids == NULL || *ppUids == NULL) {
    return;
  }
  taosArrayDestroy(*ppUids);
  *ppUids = NULL;
}

static void stTriggerTaskDestroyTableInfo(void *ptr) {
  SSTriggerVirTableInfo *pTableInfo = ptr;
  if (pTableInfo == NULL) {
    return;
  }
  if (pTableInfo->pTrigColRefs != NULL) {
    for (int32_t i = 0; i < TARRAY_SIZE(pTableInfo->pTrigColRefs); i++) {
      SSTriggerTableColRef *pColRef = TARRAY_GET_ELEM(pTableInfo->pTrigColRefs, i);
      if (pColRef->pColMatches != NULL) {
        taosArrayDestroy(pColRef->pColMatches);
        pColRef->pColMatches = NULL;
      }
    }
    taosArrayDestroy(pTableInfo->pTrigColRefs);
    pTableInfo->pTrigColRefs = NULL;
  }
  if (pTableInfo->pCalcColRefs != NULL) {
    for (int32_t i = 0; i < TARRAY_SIZE(pTableInfo->pCalcColRefs); i++) {
      SSTriggerTableColRef *pColRef = TARRAY_GET_ELEM(pTableInfo->pCalcColRefs, i);
      if (pColRef->pColMatches != NULL) {
        taosArrayDestroy(pColRef->pColMatches);
        pColRef->pColMatches = NULL;
      }
    }
    taosArrayDestroy(pTableInfo->pCalcColRefs);
    pTableInfo->pCalcColRefs = NULL;
  }
}

static void stTriggerTaskDestroyCalcNode(void *ptr) {
  SSTriggerCalcNode *pNode = ptr;
  if (pNode->pSlots != NULL) {
    for (int32_t i = 0; i < TARRAY_SIZE(pNode->pSlots); i++) {
      SSTriggerCalcSlot *pSlot = TARRAY_GET_ELEM(pNode->pSlots, i);
      tDestroySTriggerCalcRequest(&pSlot->req);
    }
    taosArrayDestroy(pNode->pSlots);
    pNode->pSlots = NULL;
  }
}

int32_t stTriggerTaskDeploy(SStreamTriggerTask *pTask, SStreamTriggerDeployMsg *pMsg) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

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
      pInterval->sliding = pSliding->sliding > 0 ? pSliding->sliding : pSliding->interval;
      pInterval->offset = pSliding->offset;
      pInterval->timeRange = (STimeWindow){.skey = INT64_MIN, .ekey = INT64_MIN};
      if (pSliding->interval == 0) {
        pInterval->offset = pSliding->soffset;
        pInterval->offsetUnit = pSliding->soffsetUnit;
      }
      break;
    }
    case WINDOW_TYPE_SESSION: {
      pTask->triggerType = STREAM_TRIGGER_SESSION;
      const SSessionTrigger *pSession = &pMsg->trigger.session;
      pTask->gap = pSession->sessionVal;
      break;
    }
    case WINDOW_TYPE_STATE: {
      pTask->triggerType = STREAM_TRIGGER_STATE;
      const SStateWinTrigger *pState = &pMsg->trigger.stateWin;
      pTask->stateSlotId = pState->slotId;
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
      pInterval->precision = TSDB_TIME_PRECISION_NANO;
      pInterval->interval = 0;
      pInterval->sliding = convertTimePrecision(pPeriod->period, pPeriod->precision, TSDB_TIME_PRECISION_NANO);
      pInterval->offset = convertTimePrecision(pPeriod->offset, pPeriod->precision, TSDB_TIME_PRECISION_NANO);
      pInterval->timeRange = (STimeWindow){.skey = INT64_MIN, .ekey = INT64_MIN};
      break;
    }
    default: {
      ST_TASK_ELOG("invalid stream trigger window type %d at %s:%d", type, __func__, __LINE__);
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  if (pTask->triggerType == STREAM_TRIGGER_SESSION || pTask->triggerType == STREAM_TRIGGER_SLIDING ||
      pTask->triggerType == STREAM_TRIGGER_COUNT) {
    pTask->trigTsIndex = 0;
  } else {
    pTask->trigTsIndex = pMsg->triTsSlotId;
  }
  pTask->calcTsIndex = pMsg->calcTsSlotId;
  pTask->maxDelayNs = pMsg->maxDelay * NANOSECOND_PER_MSEC;
  pTask->fillHistoryStartTime = pMsg->fillHistoryStartTime;
  pTask->watermark = pMsg->watermark;
  pTask->expiredTime = pMsg->expiredTime;
  pTask->ignoreDisorder = pMsg->igDisorder;
  if ((pTask->triggerType == STREAM_TRIGGER_SLIDING && pTask->interval.interval == 0) ||
      pTask->triggerType == STREAM_TRIGGER_COUNT) {
    pTask->ignoreDisorder = true;  // sliding trigger and count window trigger has no recalculation
  }
  pTask->fillHistory = pMsg->fillHistory;
  pTask->fillHistoryFirst = pMsg->fillHistoryFirst;
  // todo(kjq): fix here
  pTask->lowLatencyCalc = pMsg->lowLatencyCalc || true;
  pTask->igNoDataTrigger = pMsg->igNoDataTrigger;
  pTask->hasPartitionBy = pMsg->hasPartitionBy;
  pTask->isVirtualTable = pMsg->isTriggerTblVirt;
  pTask->placeHolderBitmap = pMsg->placeHolderBitmap;
  pTask->streamName = taosStrdup(pMsg->streamName);
  code = nodesStringToNode(pMsg->triggerPrevFilter, &pTask->triggerFilter);
  QUERY_CHECK_CODE(code, lino, _end);

  if (pTask->isVirtualTable) {
    code = stTriggerTaskParseVirtScan(pTask, pMsg->triggerScanPlan, pMsg->calcCacheScanPlan);
    QUERY_CHECK_CODE(code, lino, _end);
    pTask->pVirTableInfoRsp = taosArrayInit(0, sizeof(VTableInfo));
    QUERY_CHECK_NULL(pTask->pVirTableInfoRsp, code, lino, _end, terrno);
    pTask->pOrigTableCols = tSimpleHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
    QUERY_CHECK_NULL(pTask->pOrigTableCols, code, lino, _end, terrno);
    tSimpleHashSetFreeFp(pTask->pOrigTableCols, stTriggerTaskDestroyOrigDbInfo);
    pTask->pReaderUidMap = tSimpleHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
    QUERY_CHECK_NULL(pTask->pReaderUidMap, code, lino, _end, terrno);
    tSimpleHashSetFreeFp(pTask->pReaderUidMap, stTriggerTaskDestroyReaderUids);
    pTask->pVirTableInfos = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    QUERY_CHECK_NULL(pTask->pVirTableInfos, code, lino, _end, terrno);
    tSimpleHashSetFreeFp(pTask->pVirTableInfos, stTriggerTaskDestroyTableInfo);
  }

  pTask->calcEventType = taosArrayGetSize(pMsg->runnerList) > 0 ? pMsg->eventTypes : STRIGGER_EVENT_WINDOW_NONE;
  pTask->notifyEventType = pMsg->notifyEventTypes;
  TSWAP(pTask->pNotifyAddrUrls, pMsg->pNotifyAddrUrls);
  pTask->notifyErrorHandle = pMsg->notifyErrorHandle;
  pTask->notifyHistory = pMsg->notifyHistory;
  if ((pTask->triggerType == STREAM_TRIGGER_PERIOD) ||
      (pTask->triggerType == STREAM_TRIGGER_SLIDING && pTask->interval.interval == 0)) {
    if (pTask->calcEventType != STRIGGER_EVENT_WINDOW_NONE) {
      pTask->calcEventType = STRIGGER_EVENT_WINDOW_CLOSE;
    }
    if (taosArrayGetSize(pTask->pNotifyAddrUrls) > 0) {
      pTask->notifyEventType = STRIGGER_EVENT_WINDOW_CLOSE;
    }
  }

  pTask->leaderSnodeId = pMsg->leaderSnodeId;
  if (pTask->isVirtualTable) {
    TSWAP(pTask->virtReaderList, pMsg->readerList);
  } else {
    TSWAP(pTask->readerList, pMsg->readerList);
  }
  TSWAP(pTask->runnerList, pMsg->runnerList);

  taosInitRWLatch(&pTask->calcPoolLock);
  int32_t nRunner = taosArrayGetSize(pTask->runnerList);
  if (nRunner > 0) {
    pTask->pCalcNodes = taosArrayInit_s(sizeof(SSTriggerCalcNode), nRunner);
    QUERY_CHECK_NULL(pTask->pCalcNodes, code, lino, _end, terrno);
    for (int32_t i = 0; i < nRunner; i++) {
      SStreamRunnerTarget *pRunner = TARRAY_GET_ELEM(pTask->runnerList, i);
      SSTriggerCalcNode   *pNode = TARRAY_GET_ELEM(pTask->pCalcNodes, i);
      pNode->pSlots = taosArrayInit_s(sizeof(SSTriggerCalcSlot), pRunner->execReplica);
      QUERY_CHECK_NULL(pNode->pSlots, code, lino, _end, terrno);
      for (int32_t j = 0; j < pRunner->execReplica; j++) {
        SSTriggerCalcSlot *pSlot = TARRAY_GET_ELEM(pNode->pSlots, j);
        TD_DLIST_APPEND(&pNode->idleSlots, pSlot);
      }
    }
  }
  pTask->pGroupRunning = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
  QUERY_CHECK_NULL(pTask->pGroupRunning, code, lino, _end, terrno);

  pTask->pRealtimeStartVer = tSimpleHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  QUERY_CHECK_NULL(pTask->pRealtimeStartVer, code, lino, _end, terrno);
  pTask->pHistoryCutoffTime = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  QUERY_CHECK_NULL(pTask->pHistoryCutoffTime, code, lino, _end, terrno);
  pTask->pRecalcLastVer = tSimpleHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  QUERY_CHECK_NULL(pTask->pRecalcLastVer, code, lino, _end, terrno);

  pTask->task.status = STREAM_STATUS_INIT;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTask->task.status = STREAM_STATUS_FAILED;
  }
  return code;
}

int32_t stTriggerTaskUndeployImpl(SStreamTriggerTask **ppTask, const SStreamUndeployTaskMsg *pMsg,
                                  taskUndeplyCallback cb) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = *ppTask;

  stDebug("[checkpoint] stTriggerTaskUndeploy, taskId: %" PRIx64 ", streamId: %" PRIx64
          ", doCheckpoint: %d, doCleanup: %d",
          pTask->task.taskId, pTask->task.streamId, pMsg->doCheckpoint, pMsg->doCleanup);

  if (pMsg->doCheckpoint && pTask->pRealtimeContext) {
    uint8_t *buf = NULL;
    int64_t  len = 0;
    do {
      code = stTriggerTaskGenCheckpoint(pTask, buf, &len);
      if (code != 0) break;
      buf = taosMemoryMalloc(len);
      code = stTriggerTaskGenCheckpoint(pTask, buf, &len);
      if (code != 0) break;
      code = streamWriteCheckPoint(pTask->task.streamId, buf, len);
      if (code != 0) break;
      int32_t leaderSid = pTask->leaderSnodeId;
      SEpSet *epSet = gStreamMgmt.getSynEpset(leaderSid);
      if (epSet != NULL) {
        streamSyncWriteCheckpoint(pTask->task.streamId, epSet, buf, len);
        buf = NULL;
      }
    } while (0);
    taosMemoryFree(buf);
  }

  if (pMsg->doCleanup) {
    streamDeleteCheckPoint(pTask->task.streamId);
    int32_t leaderSid = pTask->leaderSnodeId;
    SEpSet *epSet = gStreamMgmt.getSynEpset(leaderSid);
    if (epSet != NULL) {
      streamSyncDeleteCheckpoint(pTask->task.streamId, epSet);
    }
  }

  taosWLockLatch(&gStreamTriggerWaitLatch);
  SListNode *pNode = TD_DLIST_HEAD(&gStreamTriggerWaitList);
  while (pNode != NULL) {
    SListNode *pCurNode = pNode;
    pNode = TD_DLIST_NODE_NEXT(pCurNode);
    StreamTriggerWaitInfo *pInfo = (StreamTriggerWaitInfo *)pCurNode->data;
    if (pInfo != NULL && pInfo->streamId == pTask->task.streamId) {
      TD_DLIST_POP(&gStreamTriggerWaitList, pCurNode);
      taosMemoryFreeClear(pCurNode);
    }
  }
  taosWUnLockLatch(&gStreamTriggerWaitLatch);

  if (pTask->triggerType == STREAM_TRIGGER_EVENT) {
    if (pTask->pStartCond != NULL) {
      nodesDestroyNode(pTask->pStartCond);
      pTask->pStartCond = NULL;
    }
    if (pTask->pEndCond != NULL) {
      nodesDestroyNode(pTask->pEndCond);
      pTask->pEndCond = NULL;
    }
    if (pTask->pStartCondCols != NULL) {
      nodesDestroyList(pTask->pStartCondCols);
      pTask->pStartCondCols = NULL;
    }
    if (pTask->pEndCondCols != NULL) {
      nodesDestroyList(pTask->pEndCondCols);
      pTask->pEndCondCols = NULL;
    }
  }

  if (pTask->triggerFilter != NULL) {
    nodesDestroyNode(pTask->triggerFilter);
    pTask->triggerFilter = NULL;
  }

  if (pTask->pNotifyAddrUrls != NULL) {
    for (int32_t i = 0; i < TARRAY_SIZE(pTask->pNotifyAddrUrls); i++) {
      char **url = TARRAY_GET_ELEM(pTask->pNotifyAddrUrls, i);
      taosMemoryFreeClear(*url);
    }
    taosArrayDestroy(pTask->pNotifyAddrUrls);
    pTask->pNotifyAddrUrls = NULL;
  }

  if (pTask->readerList != NULL) {
    taosArrayDestroy(pTask->readerList);
    pTask->readerList = NULL;
  }
  if (pTask->virtReaderList != NULL) {
    taosArrayDestroy(pTask->virtReaderList);
    pTask->virtReaderList = NULL;
  }
  if (pTask->runnerList != NULL) {
    taosArrayDestroy(pTask->runnerList);
    pTask->runnerList = NULL;
  }

  if (pTask->pRealtimeContext != NULL) {
    stRealtimeContextDestroy(&pTask->pRealtimeContext);
  }
  if (pTask->pHistoryContext != NULL) {
    stHistoryContextDestroy(&pTask->pHistoryContext);
  }
  if (pTask->pRealtimeStartVer != NULL) {
    tSimpleHashCleanup(pTask->pRealtimeStartVer);
    pTask->pRealtimeStartVer = NULL;
  }
  if (pTask->pHistoryCutoffTime != NULL) {
    tSimpleHashCleanup(pTask->pHistoryCutoffTime);
    pTask->pHistoryCutoffTime = NULL;
  }

  if (pTask->pVirDataBlock != NULL) {
    blockDataDestroy(pTask->pVirDataBlock);
    pTask->pVirDataBlock = NULL;
  }
  if (pTask->pVirTrigSlots != NULL) {
    taosArrayDestroy(pTask->pVirTrigSlots);
    pTask->pVirTrigSlots = NULL;
  }
  if (pTask->pVirCalcSlots != NULL) {
    taosArrayDestroy(pTask->pVirCalcSlots);
    pTask->pVirCalcSlots = NULL;
  }
  if (pTask->pVirTableInfoRsp != NULL) {
    taosArrayDestroyEx(pTask->pVirTableInfoRsp, tDestroyVTableInfo);
  }
  if (pTask->pOrigTableCols != NULL) {
    tSimpleHashCleanup(pTask->pOrigTableCols);
    pTask->pOrigTableCols = NULL;
  }
  if (pTask->pReaderUidMap != NULL) {
    tSimpleHashCleanup(pTask->pReaderUidMap);
    pTask->pReaderUidMap = NULL;
  }
  if (pTask->pVirTableInfos != NULL) {
    tSimpleHashCleanup(pTask->pVirTableInfos);
    pTask->pVirTableInfos = NULL;
  }

  if (pTask->pCalcNodes != NULL) {
    taosArrayDestroyEx(pTask->pCalcNodes, stTriggerTaskDestroyCalcNode);
    pTask->pCalcNodes = NULL;
  }
  if (pTask->pGroupRunning != NULL) {
    tSimpleHashCleanup(pTask->pGroupRunning);
    pTask->pGroupRunning = NULL;
  }

  if (pTask->streamName != NULL) {
    taosMemoryFree(pTask->streamName);
    pTask->streamName = NULL;
  }
  if (pTask->pRecalcLastVer != NULL) {
    tSimpleHashCleanup(pTask->pRecalcLastVer);
    pTask->pRecalcLastVer = NULL;
  }

  SStreamMgmtReq *pMgmtReq = atomic_load_ptr(&pTask->task.pMgmtReq);
  if (pMgmtReq && pMgmtReq == atomic_val_compare_exchange_ptr(&pTask->task.pMgmtReq, pMgmtReq, NULL)) {
    stmDestroySStreamMgmtReq(pMgmtReq);
    taosMemoryFree(pMgmtReq);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  (*cb)(ppTask);

  return code;
}

int32_t stTriggerTaskUndeploy(SStreamTriggerTask **ppTask, bool force) {
  int32_t             code = TSDB_CODE_SUCCESS;
  SStreamTriggerTask *pTask = *ppTask;

  if (!force && taosWTryForceLockLatch(&pTask->task.entryLock)) {
    ST_TASK_DLOG("ignore undeploy trigger task since working, entryLock:%x", pTask->task.entryLock);
    return code;
  }

  return stTriggerTaskUndeployImpl(ppTask, &pTask->task.undeployMsg, pTask->task.undeployCb);
}

int32_t stTriggerTaskExecute(SStreamTriggerTask *pTask, const SStreamMsg *pMsg) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  switch (pMsg->msgType) {
    case STREAM_MSG_START: {
      if (pTask->task.status != STREAM_STATUS_INIT) {
        // redundant message, ignore it
        break;
      }
      if (pTask->pRealtimeContext == NULL) {
        pTask->pRealtimeContext = taosMemoryCalloc(1, sizeof(SSTriggerRealtimeContext));
        QUERY_CHECK_NULL(pTask->pRealtimeContext, code, lino, _end, terrno);
        code = stRealtimeContextInit(pTask->pRealtimeContext, pTask);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      code = stRealtimeContextCheck(pTask->pRealtimeContext);
      QUERY_CHECK_CODE(code, lino, _end);
      pTask->task.status = STREAM_STATUS_RUNNING;
      break;
    }
    case STREAM_MSG_ORIGTBL_READER_INFO: {
      if (pTask->task.status != STREAM_STATUS_INIT || taosArrayGetSize(pTask->readerList) > 0) {
        // redundant message, ignore it
        break;
      }
      SStreamMgmtRsp *pRsp = (SStreamMgmtRsp *)pMsg;
      int32_t        *pVgId = TARRAY_DATA(pRsp->cont.vgIds);
      int32_t         iter1 = 0;
      void           *px = tSimpleHashIterate(pTask->pOrigTableCols, NULL, &iter1);
      while (px != NULL) {
        SSHashObj              *pDbInfo = *(SSHashObj **)px;
        int32_t                 iter2 = 0;
        SSTriggerOrigTableInfo *pTbInfo = tSimpleHashIterate(pDbInfo, NULL, &iter2);
        while (pTbInfo != NULL) {
          pTbInfo->vgId = *(pVgId++);
          void   *px2 = tSimpleHashGet(pTask->pReaderUidMap, &pTbInfo->vgId, sizeof(int32_t));
          SArray *pUids = NULL;
          if (px2 == NULL) {
            pUids = taosArrayInit(0, sizeof(int64_t) * 2);
            QUERY_CHECK_NULL(pUids, code, lino, _end, terrno);
            code = tSimpleHashPut(pTask->pReaderUidMap, &pTbInfo->vgId, sizeof(int32_t), &pUids, POINTER_BYTES);
            if (code != TSDB_CODE_SUCCESS) {
              taosArrayDestroy(pUids);
              QUERY_CHECK_CODE(code, lino, _end);
            }
          } else {
            pUids = *(SArray **)px2;
          }
          void **ptr = taosArrayReserve(pUids, 1);
          QUERY_CHECK_NULL(ptr, code, lino, _end, terrno);
          *ptr = pTbInfo;
          pTbInfo = tSimpleHashIterate(pDbInfo, pTbInfo, &iter2);
        }
        px = tSimpleHashIterate(pTask->pOrigTableCols, px, &iter1);
      }
      QUERY_CHECK_CONDITION(TARRAY_ELEM_IDX(pRsp->cont.vgIds, pVgId) == TARRAY_SIZE(pRsp->cont.vgIds), code, lino, _end,
                            TSDB_CODE_INVALID_PARA);

      SSTriggerRealtimeContext *pContext = pTask->pRealtimeContext;
      if (pTask->readerList == NULL) {
        pTask->readerList = taosArrayInit(0, sizeof(SStreamTaskAddr));
        QUERY_CHECK_NULL(pTask->readerList, code, lino, _end, terrno);
      }
      for (int32_t i = 0; i < TARRAY_SIZE(pTask->virtReaderList); i++) {
        SStreamTaskAddr *pReader = TARRAY_GET_ELEM(pTask->virtReaderList, i);
        if (tSimpleHashGet(pTask->pReaderUidMap, &pReader->nodeId, sizeof(int32_t)) != NULL) {
          px = taosArrayPush(pTask->readerList, pReader);
          QUERY_CHECK_NULL(px, code, lino, _end, terrno);
        }
      }
      int32_t nPartReaders = TARRAY_SIZE(pTask->readerList);
      if (taosArrayGetSize(pRsp->cont.readerList) > 0) {
        px = taosArrayAddAll(pTask->readerList, pRsp->cont.readerList);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      }
      QUERY_CHECK_CONDITION(TARRAY_SIZE(pTask->readerList) == tSimpleHashGetSize(pTask->pReaderUidMap), code, lino,
                            _end, TSDB_CODE_INVALID_PARA);

      for (int32_t i = nPartReaders; i < TARRAY_SIZE(pTask->readerList); i++) {
        SStreamTaskAddr     *pReader = TARRAY_GET_ELEM(pTask->readerList, i);
        SSTriggerWalProgress progress = {0};
        code = tSimpleHashPut(pTask->pRealtimeContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t), &progress,
                              sizeof(SSTriggerWalProgress));
        QUERY_CHECK_CODE(code, lino, _end);
        SSTriggerWalProgress *pProgress =
            tSimpleHashGet(pTask->pRealtimeContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t));
        QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        pProgress->pTaskAddr = pReader;
        pProgress->pMetadatas = taosArrayInit(0, POINTER_BYTES);
        QUERY_CHECK_NULL(pProgress->pMetadatas, code, lino, _end, terrno);
        SSTriggerPullRequest *pPullReq = &pProgress->pullReq.base;
        pPullReq->streamId = pTask->task.streamId;
        pPullReq->sessionId = pContext->sessionId;
        pPullReq->triggerTaskId = pTask->task.taskId;
        if (pTask->isVirtualTable) {
          pProgress->reqCids = taosArrayInit(0, sizeof(col_id_t));
          QUERY_CHECK_NULL(pProgress->reqCids, code, lino, _end, terrno);
          pProgress->reqCols = taosArrayInit(0, sizeof(OTableInfo));
          QUERY_CHECK_NULL(pProgress->reqCols, code, lino, _end, terrno);
        }
      }
      break;
    }
    case STREAM_MSG_UPDATE_RUNNER:
    case STREAM_MSG_USER_RECALC: {
      // todo(kjq): handle original table reader info
      break;
    }
    default: {
      ST_TASK_ELOG("invalid stream trigger message type %d at %s:%d", pMsg->msgType, __func__, __LINE__);
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

int32_t stTriggerTaskProcessRsp(SStreamTask *pStreamTask, SRpcMsg *pRsp, int64_t *pErrTaskId) {
  int32_t             code = 0;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = (SStreamTriggerTask *)pStreamTask;

  *pErrTaskId = pStreamTask->taskId;

  SMsgSendInfo     *ahandle = pRsp->info.ahandle;
  SSTriggerAHandle *pAhandle = ahandle->param;

  if (pRsp->msgType == TDMT_STREAM_TRIGGER_PULL_RSP) {
    SSTriggerPullRequest *pReq = pAhandle->param;
    switch (pRsp->code) {
      case TSDB_CODE_SUCCESS:
      case TSDB_CODE_STREAM_NO_DATA: {
        if (pReq->sessionId == SSTRIGGER_REALTIME_SESSIONID) {
          code = stRealtimeContextProcPullRsp(pTask->pRealtimeContext, pRsp);
          QUERY_CHECK_CODE(code, lino, _end);
        } else if (pReq->sessionId == SSTRIGGER_HISTORY_SESSIONID) {
          code = stHistoryContextProcPullRsp(pTask->pHistoryContext, pRsp);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        break;
      }
      case TSDB_CODE_STREAM_TASK_NOT_EXIST: {
        if (pReq->sessionId == SSTRIGGER_REALTIME_SESSIONID) {
          pTask->pRealtimeContext->pRetryReq = pReq;
        } else if (pReq->sessionId == SSTRIGGER_HISTORY_SESSIONID) {
          pTask->pHistoryContext->pRetryReq = pReq;
        }
        int64_t resumeTime = taosGetTimestampNs() + STREAM_ACT_MIN_DELAY_MSEC * NANOSECOND_PER_MSEC;
        code = stTriggerTaskAddWaitSession(pTask, pReq->sessionId, resumeTime);
        QUERY_CHECK_CODE(code, lino, _end);
        break;
      }
      default: {
        *pErrTaskId = pReq->readerTaskId;
        code = pRsp->code;
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
  } else if (pRsp->msgType == TDMT_STREAM_TRIGGER_CALC_RSP) {
    SSTriggerCalcRequest *pReq = pAhandle->param;
    if (pRsp->code == TSDB_CODE_SUCCESS) {
      if (pReq->sessionId == SSTRIGGER_REALTIME_SESSIONID) {
        code = stRealtimeContextProcCalcRsp(pTask->pRealtimeContext, pRsp);
        QUERY_CHECK_CODE(code, lino, _end);
      } else if (pReq->sessionId == SSTRIGGER_HISTORY_SESSIONID) {
        code = stHistoryContextProcCalcRsp(pTask->pHistoryContext, pRsp);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    } else {
      *pErrTaskId = pReq->runnerTaskId;
      code = pRsp->code;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  } else if (pRsp->msgType == TDMT_SND_BATCH_META) {
    // todo(kjq): handle progress request
    code = TSDB_CODE_OPS_NOT_SUPPORT;
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t stTriggerTaskGetStatus(SStreamTask *pTask, SSTriggerRuntimeStatus *pStatus) {
  // todo(kjq): implement how to get recalculation progress
  return TSDB_CODE_SUCCESS;
}

static void stRealtimeContextDestroyWalProgress(void *ptr) {
  SSTriggerWalProgress *pProgress = ptr;
  if (pProgress == NULL) {
    return;
  }
  if (pProgress->reqCids != NULL) {
    taosArrayDestroy(pProgress->reqCids);
    pProgress->reqCids = NULL;
  }
  if (pProgress->reqCols != NULL) {
    taosArrayDestroy(pProgress->reqCols);
    pProgress->reqCols = NULL;
  }
  if (pProgress->pMetadatas != NULL) {
    taosArrayDestroyP(pProgress->pMetadatas, (FDelete)blockDataDestroy);
    pProgress->pMetadatas = NULL;
  }
}

static int32_t stRealtimeContextInit(SSTriggerRealtimeContext *pContext, SStreamTriggerTask *pTask) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSDataBlock *pVirDataBlock = NULL;
  SFilterInfo *pVirDataFilter = NULL;

  pContext->pTask = pTask;
  pContext->sessionId = SSTRIGGER_REALTIME_SESSIONID;

  pContext->pReaderWalProgress = tSimpleHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  QUERY_CHECK_NULL(pContext->pReaderWalProgress, code, lino, _end, terrno);
  tSimpleHashSetFreeFp(pContext->pReaderWalProgress, stRealtimeContextDestroyWalProgress);
  SArray *pReaderList = pTask->isVirtualTable ? pTask->virtReaderList : pTask->readerList;
  int32_t nReaders = taosArrayGetSize(pReaderList);
  for (int32_t i = 0; i < nReaders; i++) {
    SStreamTaskAddr     *pReader = TARRAY_GET_ELEM(pReaderList, i);
    SSTriggerWalProgress progress = {0};
    code = tSimpleHashPut(pContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t), &progress,
                          sizeof(SSTriggerWalProgress));
    QUERY_CHECK_CODE(code, lino, _end);
    SSTriggerWalProgress *pProgress = tSimpleHashGet(pContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t));
    QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    pProgress->pTaskAddr = pReader;
    pProgress->pMetadatas = taosArrayInit(0, POINTER_BYTES);
    QUERY_CHECK_NULL(pProgress->pMetadatas, code, lino, _end, terrno);
    SSTriggerPullRequest *pPullReq = &pProgress->pullReq.base;
    pPullReq->streamId = pTask->task.streamId;
    pPullReq->sessionId = pContext->sessionId;
    pPullReq->triggerTaskId = pTask->task.taskId;
    if (pTask->isVirtualTable) {
      pProgress->reqCids = taosArrayInit(0, sizeof(col_id_t));
      QUERY_CHECK_NULL(pProgress->reqCids, code, lino, _end, terrno);
      pProgress->reqCols = taosArrayInit(0, sizeof(OTableInfo));
      QUERY_CHECK_NULL(pProgress->reqCols, code, lino, _end, terrno);
    }
  }

  pContext->pGroups = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  QUERY_CHECK_NULL(pContext->pGroups, code, lino, _end, terrno);
  tSimpleHashSetFreeFp(pContext->pGroups, stRealtimeGroupDestroy);
  TD_DLIST_INIT(&pContext->groupsToCheck);

  pContext->pSorter = taosMemoryCalloc(1, sizeof(SSTriggerTimestampSorter));
  QUERY_CHECK_NULL(pContext->pSorter, code, lino, _end, terrno);
  code = stTimestampSorterInit(pContext->pSorter, pTask);
  QUERY_CHECK_CODE(code, lino, _end);
  if (pTask->isVirtualTable) {
    code = createOneDataBlock(pTask->pVirDataBlock, false, &pVirDataBlock);
    QUERY_CHECK_CODE(code, lino, _end);
    code = filterInitFromNode(pTask->triggerFilter, &pVirDataFilter, 0, NULL);
    QUERY_CHECK_CODE(code, lino, _end);
    pContext->pMerger = taosMemoryCalloc(1, sizeof(SSTriggerVtableMerger));
    QUERY_CHECK_NULL(pContext->pMerger, code, lino, _end, terrno);
    code = stVtableMergerInit(pContext->pMerger, pTask, &pVirDataBlock, &pVirDataFilter);
    QUERY_CHECK_CODE(code, lino, _end);
  }
  if (pTask->triggerType == STREAM_TRIGGER_SLIDING || pTask->triggerType == STREAM_TRIGGER_SESSION) {
    pContext->pSavedWindows = taosArrayInit(0, sizeof(SSTriggerWindow));
    QUERY_CHECK_NULL(pContext->pSavedWindows, code, lino, _end, terrno);
    pContext->pInitWindows = taosArrayInit(0, sizeof(STimeWindow));
    QUERY_CHECK_NULL(pContext->pInitWindows, code, lino, _end, terrno);
  } else if (pTask->triggerType == STREAM_TRIGGER_EVENT) {
    code = filterInitFromNode(pTask->pStartCond, &pContext->pStartCond, 0, NULL);
    QUERY_CHECK_CODE(code, lino, _end);
    code = filterInitFromNode(pTask->pEndCond, &pContext->pEndCond, 0, NULL);
    QUERY_CHECK_CODE(code, lino, _end);
  }
  if (pTask->notifyEventType != STRIGGER_EVENT_WINDOW_NONE) {
    pContext->pNotifyParams = taosArrayInit(0, sizeof(SSTriggerCalcParam));
    QUERY_CHECK_NULL(pContext->pNotifyParams, code, lino, _end, terrno);
  }

  pContext->pCalcDataCacheIters =
      taosHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  taosHashSetFreeFp(pContext->pCalcDataCacheIters, (_hash_free_fn_t)releaseDataResult);
  QUERY_CHECK_NULL(pContext->pCalcDataCacheIters, code, lino, _end, errno);

  pContext->periodWindow = (STimeWindow){.skey = INT64_MIN, .ekey = INT64_MIN};
  pContext->lastCheckpointTime = taosGetTimestampNs();

_end:
  return code;
}

static void stRealtimeContextDestroy(void *ptr) {
  SSTriggerRealtimeContext **ppContext = ptr;
  if (ppContext == NULL || *ppContext == NULL) {
    return;
  }

  SSTriggerRealtimeContext *pContext = *ppContext;
  if (pContext->pReaderWalProgress != NULL) {
    tSimpleHashCleanup(pContext->pReaderWalProgress);
    pContext->pReaderWalProgress = NULL;
  }

  if (pContext->pGroups != NULL) {
    tSimpleHashCleanup(pContext->pGroups);
    pContext->pGroups = NULL;
  }

  if (pContext->pSorter != NULL) {
    stTimestampSorterDestroy(&pContext->pSorter);
  }
  if (pContext->pMerger != NULL) {
    stVtableMergerDestroy(&pContext->pMerger);
    taosMemoryFreeClear(pContext->pMerger);
  }

  if (pContext->pSavedWindows != NULL) {
    taosArrayDestroy(pContext->pSavedWindows);
    pContext->pSavedWindows = NULL;
  }
  if (pContext->pInitWindows != NULL) {
    taosArrayDestroy(pContext->pInitWindows);
    pContext->pInitWindows = NULL;
  }
  if (pContext->pStartCond != NULL) {
    filterFreeInfo(pContext->pStartCond);
    pContext->pStartCond = NULL;
  }
  if (pContext->pEndCond != NULL) {
    filterFreeInfo(pContext->pEndCond);
    pContext->pEndCond = NULL;
  }
  if (pContext->pNotifyParams != NULL) {
    taosArrayDestroyEx(pContext->pNotifyParams, tDestroySSTriggerCalcParam);
    pContext->pNotifyParams = NULL;
  }

  if (pContext->pCalcDataCache != NULL) {
    destroyStreamDataCache(pContext->pCalcDataCache);
    pContext->pCalcDataCache = NULL;
  }
  if (pContext->pCalcDataCacheIters != NULL) {
    taosHashCleanup(pContext->pCalcDataCacheIters);
    pContext->pCalcDataCacheIters = NULL;
  }

  taosMemFreeClear(*ppContext);
}

static FORCE_INLINE SSTriggerRealtimeGroup *stRealtimeContextGetCurrentGroup(SSTriggerRealtimeContext *pContext) {
  if (TD_DLIST_NELES(&pContext->groupsToCheck) > 0) {
    return TD_DLIST_HEAD(&pContext->groupsToCheck);
  } else if (TD_DLIST_NELES(&pContext->groupsMaxDelay) > 0) {
    return TD_DLIST_HEAD(&pContext->groupsMaxDelay);
  } else {
    terrno = TSDB_CODE_INTERNAL_ERROR;
    SStreamTriggerTask *pTask = pContext->pTask;
    ST_TASK_ELOG("failed to get the group in realtime context %" PRId64, pContext->sessionId);
    return NULL;
  }
}

static int32_t stRealtimeContextSendPullReq(SSTriggerRealtimeContext *pContext, ESTriggerPullType type) {
  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               lino = 0;
  SStreamTriggerTask   *pTask = pContext->pTask;
  SSTriggerWalProgress *pProgress = NULL;
  SRpcMsg               msg = {.msgType = TDMT_STREAM_TRIGGER_PULL};

  switch (type) {
    case STRIGGER_PULL_LAST_TS: {
      SStreamTaskAddr *pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
      QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
      pProgress = tSimpleHashGet(pContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      break;
    }

    case STRIGGER_PULL_WAL_META: {
      SStreamTaskAddr *pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
      QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
      pProgress = tSimpleHashGet(pContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SSTriggerWalMetaRequest *pReq = &pProgress->pullReq.walMetaReq;
      pReq->lastVer = pProgress->lastScanVer;
      if (pTask->triggerType == STREAM_TRIGGER_PERIOD) {
        pReq->ctime = pContext->periodWindow.ekey;
      } else {
        pReq->ctime = INT64_MAX;
      }
      break;
    }

    case STRIGGER_PULL_WAL_TS_DATA:
    case STRIGGER_PULL_WAL_TRIGGER_DATA:
    case STRIGGER_PULL_WAL_CALC_DATA: {
      SSTriggerTableMeta *pCurTableMeta = pContext->pCurTableMeta;
      SSTriggerMetaData  *pMetaToFetch = pContext->pMetaToFetch;
      pProgress = tSimpleHashGet(pContext->pReaderWalProgress, &pCurTableMeta->vgId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SSTriggerWalDataRequest *pReq = &pProgress->pullReq.walDataReq;
      pReq->uid = pCurTableMeta->tbUid;
      pReq->ver = pMetaToFetch->ver;
      pReq->skey = pMetaToFetch->skey;
      pReq->ekey = pMetaToFetch->ekey;
      pReq->cids = NULL;
      break;
    }

    case STRIGGER_PULL_WAL_DATA: {
      SSTriggerTableColRef *pColRefToFetch = pContext->pColRefToFetch;
      SSTriggerMetaData    *pMetaToFetch = pContext->pMetaToFetch;
      pProgress = tSimpleHashGet(pContext->pReaderWalProgress, &pColRefToFetch->otbVgId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SSTriggerWalDataRequest *pReq = &pProgress->pullReq.walDataReq;
      pReq->uid = pColRefToFetch->otbUid;
      pReq->ver = pMetaToFetch->ver;
      pReq->skey = pMetaToFetch->skey;
      pReq->ekey = pMetaToFetch->ekey;
      pReq->cids = pProgress->reqCids;
      taosArrayClear(pReq->cids);
      *(col_id_t *)TARRAY_DATA(pReq->cids) = PRIMARYKEY_TIMESTAMP_COL_ID;
      TARRAY_SIZE(pReq->cids) = 1;
      int32_t nCols = taosArrayGetSize(pColRefToFetch->pColMatches);
      for (int32_t i = 0; i < nCols; i++) {
        SSTriggerColMatch *pColMatch = TARRAY_GET_ELEM(pColRefToFetch->pColMatches, i);
        void              *px = taosArrayPush(pReq->cids, &pColMatch->otbColId);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      }
      break;
    }

    case STRIGGER_PULL_GROUP_COL_VALUE: {
      SSTriggerRealtimeGroup *pGroup = stRealtimeContextGetCurrentGroup(pContext);
      QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
      if (pTask->isVirtualTable) {
        SSTriggerVirTableInfo *pTable = taosArrayGetP(pGroup->pVirTableInfos, 0);
        QUERY_CHECK_NULL(pTable, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        pProgress = tSimpleHashGet(pContext->pReaderWalProgress, &pTable->vgId, sizeof(int32_t));
        QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      } else {
        int32_t             iter = 0;
        SSTriggerTableMeta *pTable = tSimpleHashIterate(pGroup->pTableMetas, NULL, &iter);
        QUERY_CHECK_NULL(pTable, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        pProgress = tSimpleHashGet(pContext->pReaderWalProgress, &pTable->vgId, sizeof(int32_t));
        QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      }
      SSTriggerGroupColValueRequest *pReq = &pProgress->pullReq.groupColValueReq;
      pReq->gid = pGroup->gid;
      break;
    }

    case STRIGGER_PULL_VTABLE_INFO: {
      SStreamTaskAddr *pReader = taosArrayGet(pTask->virtReaderList, pContext->curReaderIdx);
      QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
      pProgress = tSimpleHashGet(pContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SSTriggerVirTableInfoRequest *pReq = &pProgress->pullReq.virTableInfoReq;
      int32_t                       nCols = taosArrayGetSize(pTask->pVirDataBlock->pDataBlock);
      pReq->cids = pProgress->reqCids;
      taosArrayEnsureCap(pReq->cids, nCols);
      TARRAY_SIZE(pReq->cids) = nCols;
      for (int32_t i = 0; i < nCols; i++) {
        SColumnInfoData *pCol = TARRAY_GET_ELEM(pTask->pVirDataBlock->pDataBlock, i);
        *(col_id_t *)TARRAY_GET_ELEM(pReq->cids, i) = pCol->info.colId;
      }
      break;
    }

    case STRIGGER_PULL_OTABLE_INFO: {
      SStreamTaskAddr *pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
      QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
      pProgress = tSimpleHashGet(pContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SSTriggerOrigTableInfoRequest *pReq = &pProgress->pullReq.origTableInfoReq;
      pReq->cols = pProgress->reqCols;
      taosArrayClear(pReq->cols);
      void *px = tSimpleHashGet(pTask->pReaderUidMap, &pReader->nodeId, sizeof(int32_t));
      QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SArray *pUids = *(SArray **)px;
      for (int32_t i = 0; i < TARRAY_SIZE(pUids); i++) {
        SSTriggerOrigTableInfo *pTbInfo = *(SSTriggerOrigTableInfo **)TARRAY_GET_ELEM(pUids, i);
        char                   *tbName = tSimpleHashGetKey(pTbInfo, NULL);
        int32_t                 iter = 0;
        void                   *px = tSimpleHashIterate(pTbInfo->pColumns, NULL, &iter);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
        while (px != NULL) {
          char       *colName = tSimpleHashGetKey(px, NULL);
          OTableInfo *pInfo = taosArrayReserve(pReq->cols, 1);
          QUERY_CHECK_NULL(pInfo, code, lino, _end, terrno);
          (void)strncpy(pInfo->refTableName, tbName, sizeof(pInfo->refTableName));
          (void)strncpy(pInfo->refColName, colName, sizeof(pInfo->refColName));
          px = tSimpleHashIterate(pTbInfo->pColumns, px, &iter);
        }
      }
      break;
    }

    case STRIGGER_PULL_SET_TABLE: {
      SStreamTaskAddr *pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
      QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
      pProgress = tSimpleHashGet(pContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SSTriggerSetTableRequest *pReq = &pProgress->pullReq.setTableReq;
      void                     *px = tSimpleHashGet(pTask->pReaderUidMap, &pReader->nodeId, sizeof(int32_t));
      QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SArray *pUids = *(SArray **)px;
      pReq->uids = pUids;
      break;
    }

    default: {
      ST_TASK_ELOG("invalid pull request type %d at %s", type, __func__);
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  SSTriggerPullRequest *pReq = &pProgress->pullReq.base;
  SStreamTaskAddr      *pReader = pProgress->pTaskAddr;
  pReq->type = type;
  pReq->readerTaskId = pReader->taskId;

  // serialize and send request
  QUERY_CHECK_CODE(stTriggerTaskAllocAhandle(pTask, pContext->sessionId, pReq, &msg.info.ahandle), lino, _end);
  msg.contLen = tSerializeSTriggerPullRequest(NULL, 0, pReq);
  QUERY_CHECK_CONDITION(msg.contLen > 0, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  msg.contLen += sizeof(SMsgHead);
  msg.pCont = rpcMallocCont(msg.contLen);
  QUERY_CHECK_NULL(msg.pCont, code, lino, _end, terrno);
  SMsgHead *pMsgHead = (SMsgHead *)msg.pCont;
  pMsgHead->contLen = htonl(msg.contLen);
  pMsgHead->vgId = htonl(pReader->nodeId);
  int32_t tlen =
      tSerializeSTriggerPullRequest((char *)msg.pCont + sizeof(SMsgHead), msg.contLen - sizeof(SMsgHead), pReq);
  QUERY_CHECK_CONDITION(tlen == msg.contLen - sizeof(SMsgHead), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  code = tmsgSendReq(&pReader->epset, &msg);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    destroyAhandle(msg.info.ahandle);
    ST_TASK_ELOG("%s failed at line %d since %s, type: %d", __func__, lino, tstrerror(code), type);
  }
  return code;
}

static int32_t stRealtimeContextSendCalcReq(SSTriggerRealtimeContext *pContext) {
  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               lino = 0;
  SStreamTriggerTask   *pTask = pContext->pTask;
  SSTriggerCalcRequest *pCalcReq = pContext->pCalcReq;
  SStreamRunnerTarget  *pCalcRunner = NULL;
  bool                  needTagValue = false;
  SRpcMsg               msg = {.msgType = TDMT_STREAM_TRIGGER_CALC};
  SSDataBlock          *pCalcDataBlock = NULL;

  QUERY_CHECK_NULL(pCalcReq, code, lino, _end, TSDB_CODE_INVALID_PARA);

  int32_t nRunners = taosArrayGetSize(pTask->runnerList);
  for (int32_t i = 0; i < taosArrayGetSize(pTask->runnerList); i++) {
    pCalcRunner = TARRAY_GET_ELEM(pTask->runnerList, i);
    if (pCalcRunner->addr.taskId == pCalcReq->runnerTaskId) {
      break;
    }
    pCalcRunner = NULL;
  }
  QUERY_CHECK_NULL(pCalcRunner, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

  if (pCalcReq->createTable && pTask->hasPartitionBy || (pTask->placeHolderBitmap & PLACE_HOLDER_PARTITION_IDX) ||
      (pTask->placeHolderBitmap & PLACE_HOLDER_PARTITION_TBNAME)) {
    needTagValue = true;
  }

  if (needTagValue && taosArrayGetSize(pCalcReq->groupColVals) == 0) {
    code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_GROUP_COL_VALUE);
    QUERY_CHECK_CODE(code, lino, _end);
    goto _end;
  }

  if (pTask->placeHolderBitmap & PLACE_HOLDER_PARTITION_ROWS) {
    // create data cache handle
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

    SSTriggerRealtimeGroup *pGroup = stRealtimeContextGetCurrentGroup(pContext);
    QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
    if (pContext->pParamToFetch == NULL) {
      pContext->pParamToFetch = TARRAY_DATA(pCalcReq->params);
    }

    while (TARRAY_ELEM_IDX(pCalcReq->params, pContext->pParamToFetch) < TARRAY_SIZE(pCalcReq->params)) {
      bool allTableProcessed = false;
      bool needFetchData = false;
      while (!allTableProcessed && !needFetchData) {
        SSDataBlock *pDataBlock = NULL;
        int32_t      startIdx = 0;
        int32_t      endIdx = 0;
        code = stRealtimeGroupGetDataBlock(pGroup, false, &pDataBlock, &startIdx, &endIdx, &allTableProcessed,
                                           &needFetchData);
        QUERY_CHECK_CODE(code, lino, _end);

        if (allTableProcessed || needFetchData) {
          break;
        }
        if (!pTask->isVirtualTable) {
          code = putStreamDataCache(pContext->pCalcDataCache, pGroup->gid, pContext->pParamToFetch->wstart,
                                    pContext->pParamToFetch->wend, pDataBlock, startIdx, endIdx - 1);
          QUERY_CHECK_CODE(code, lino, _end);
        } else {
          if (pCalcDataBlock == NULL) {
            code = createDataBlock(&pCalcDataBlock);
            QUERY_CHECK_CODE(code, lino, _end);
          }
          taosArrayClear(pCalcDataBlock->pDataBlock);
          pCalcDataBlock->info.rowSize = 0;
          int32_t nCols = TARRAY_SIZE(pTask->pVirCalcSlots);
          for (int32_t i = 0; i < nCols; i++) {
            int32_t          slotId = *(int32_t *)TARRAY_GET_ELEM(pTask->pVirCalcSlots, i);
            SColumnInfoData *pCol = TARRAY_GET_ELEM(pDataBlock->pDataBlock, slotId);
            code = blockDataAppendColInfo(pCalcDataBlock, pCol);
            QUERY_CHECK_CODE(code, lino, _end);
          }
          pCalcDataBlock->info.rows = pDataBlock->info.rows;
          code = putStreamDataCache(pContext->pCalcDataCache, pGroup->gid, pContext->pParamToFetch->wstart,
                                    pContext->pParamToFetch->wend, pCalcDataBlock, startIdx, endIdx - 1);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }

      if (needFetchData) {
        if (pContext->pColRefToFetch != NULL) {
          code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_WAL_DATA);
          QUERY_CHECK_CODE(code, lino, _end);
          goto _end;
        } else {
          QUERY_CHECK_NULL(pContext->pMetaToFetch, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
          code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_WAL_CALC_DATA);
          QUERY_CHECK_CODE(code, lino, _end);
          goto _end;
        }
      }
      SSTriggerCalcParam *pNextParam = pContext->pParamToFetch + 1;
      stRealtimeGroupClearTempState(pGroup);
      pContext->pParamToFetch = pNextParam;
    }
  }

  // amend ekey of interval window trigger and sliding trigger
  for (int32_t i = 0; i < TARRAY_SIZE(pCalcReq->params); ++i) {
    SSTriggerCalcParam *pParam = taosArrayGet(pCalcReq->params, i);
    if (pTask->triggerType == STREAM_TRIGGER_SLIDING) {
      pParam->wend++;
      pParam->wduration++;
    }
    ST_TASK_ILOG("[calc param %d]: gid=%" PRId64 ", wstart=%" PRId64 ", wend=%" PRId64 ", nrows=%" PRId64
                 ", prevTs=%" PRId64 ", currentTs=%" PRId64 ", nextTs=%" PRId64 ", prevLocalTime=%" PRId64
                 ", nextLocalTime=%" PRId64 ", localTime=%" PRId64 ", create=%d",
                 i, pCalcReq->gid, pParam->wstart, pParam->wend, pParam->wrownum, pParam->prevTs, pParam->currentTs,
                 pParam->nextTs, pParam->prevLocalTime, pParam->nextLocalTime, pParam->triggerTime,
                 pCalcReq->createTable);
  }

  // serialize and send request
  QUERY_CHECK_CODE(stTriggerTaskAllocAhandle(pTask, pContext->sessionId, pCalcReq, &msg.info.ahandle), lino, _end);
  msg.contLen = tSerializeSTriggerCalcRequest(NULL, 0, pCalcReq);
  QUERY_CHECK_CONDITION(msg.contLen > 0, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  msg.contLen += sizeof(SMsgHead);
  msg.pCont = rpcMallocCont(msg.contLen);
  QUERY_CHECK_NULL(msg.pCont, code, lino, _end, terrno);
  SMsgHead *pMsgHead = (SMsgHead *)msg.pCont;
  pMsgHead->contLen = htonl(msg.contLen);
  pMsgHead->vgId = htonl(SNODE_HANDLE);
  int32_t tlen = tSerializeSTriggerCalcRequest((char*)msg.pCont + sizeof(SMsgHead), msg.contLen - sizeof(SMsgHead), pCalcReq);
  QUERY_CHECK_CONDITION(tlen == msg.contLen - sizeof(SMsgHead), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  code = tmsgSendReq(&pCalcRunner->addr.epset, &msg);
  QUERY_CHECK_CODE(code, lino, _end);

  ST_TASK_DLOG("calc request is sent to node:%d task:%" PRIx64, pCalcRunner->addr.nodeId, pCalcRunner->addr.taskId);

  pContext->pCalcReq = NULL;

_end:
  if (pCalcDataBlock != NULL) {
    taosArrayClear(pCalcDataBlock->pDataBlock);
    blockDataDestroy(pCalcDataBlock);
  }
  if (code != TSDB_CODE_SUCCESS) {
    destroyAhandle(msg.info.ahandle);
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stRealtimeContextCheck(SSTriggerRealtimeContext *pContext) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pContext->pTask;

  if (!pContext->haveReadCheckpoint) {
    stDebug("[checkpoint] read checkpoint for stream %" PRIx64, pTask->task.streamId);
    if (pTask->isCheckpointReady) {
      void   *buf = NULL;
      int64_t len = 0;
      code = streamReadCheckPoint(pTask->task.streamId, &buf, &len);
      // todo(kjq): parse the checkpoint data and restore status
      taosMemoryFree(buf);
      QUERY_CHECK_CODE(code, lino, _end);
      pContext->haveReadCheckpoint = true;
    } else {
      // wait 1 second and retry
      int64_t resumeTime = taosGetTimestampNs() + 1 * NANOSECOND_PER_SEC;
      code = stTriggerTaskAddWaitSession(pTask, pContext->sessionId, pContext->periodWindow.ekey);
      QUERY_CHECK_CODE(code, lino, _end);
      goto _end;
    }
  }

  if (pContext->pRetryReq != NULL) {
    code = stRealtimeContextSendPullReq(pContext, pContext->pRetryReq->type);
    QUERY_CHECK_CODE(code, lino, _end);
    pContext->pRetryReq = NULL;
    goto _end;
  }

  if (pContext->status == STRIGGER_CONTEXT_IDLE) {
    if (pTask->isVirtualTable && !pTask->virTableInfoReady) {
      pContext->status = STRIGGER_CONTEXT_GATHER_VTABLE_INFO;
      if (taosArrayGetSize(pTask->virtReaderList) > 0 && taosArrayGetSize(pTask->pVirTableInfoRsp) == 0) {
        for (pContext->curReaderIdx = 0; pContext->curReaderIdx < TARRAY_SIZE(pTask->virtReaderList);
             pContext->curReaderIdx++) {
          code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_VTABLE_INFO);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      } else if (taosArrayGetSize(pTask->readerList) > 0) {
        for (pContext->curReaderIdx = 0; pContext->curReaderIdx < TARRAY_SIZE(pTask->readerList);
             pContext->curReaderIdx++) {
          code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_OTABLE_INFO);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
      goto _end;
    }

    if (taosArrayGetSize(pTask->readerList) > 0 && tSimpleHashGetSize(pTask->pRealtimeStartVer) == 0) {
      pContext->status = STRIGGER_CONTEXT_DETERMINE_BOUND;
      for (pContext->curReaderIdx = 0; pContext->curReaderIdx < TARRAY_SIZE(pTask->readerList);
           pContext->curReaderIdx++) {
        code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_LAST_TS);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      goto _end;
    }

    if (pTask->triggerType != STREAM_TRIGGER_PERIOD) {
      // todo(kjq): start history calc first
      pContext->status = STRIGGER_CONTEXT_FETCH_META;
      for (pContext->curReaderIdx = 0; pContext->curReaderIdx < TARRAY_SIZE(pTask->readerList);
           pContext->curReaderIdx++) {
        code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_WAL_META);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      goto _end;
    }

    // check if to start for period trigger
    int64_t now = taosGetTimestampNs();
    if (pContext->periodWindow.skey == INT64_MIN) {
      pContext->periodWindow = stTriggerTaskGetPeriodWindow(pTask, now);
    }
    if (now >= pContext->periodWindow.ekey) {
      pContext->status = STRIGGER_CONTEXT_FETCH_META;
      if (taosArrayGetSize(pTask->readerList) > 0) {
        // fetch wal meta from all readers
        for (pContext->curReaderIdx = 0; pContext->curReaderIdx < TARRAY_SIZE(pTask->readerList);
             pContext->curReaderIdx++) {
          code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_WAL_META);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        goto _end;
      } else {
        // add a fake group to trigger the notification/calculation
        SSTriggerRealtimeGroup *pGroup = NULL;
        if (tSimpleHashGetSize(pContext->pGroups) == 0) {
          pGroup = taosMemoryCalloc(1, sizeof(SSTriggerRealtimeGroup));
          QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
          code = tSimpleHashPut(pContext->pGroups, &pGroup->gid, sizeof(int64_t), &pGroup, POINTER_BYTES);
          if (code != TSDB_CODE_SUCCESS) {
            taosMemoryFreeClear(pGroup);
            QUERY_CHECK_CODE(code, lino, _end);
          }
          code = stRealtimeGroupInit(pGroup, pContext, 0);
          QUERY_CHECK_CODE(code, lino, _end);
        } else {
          int32_t iter = 0;
          void   *px = tSimpleHashIterate(pContext->pGroups, NULL, &iter);
          QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
          pGroup = *(SSTriggerRealtimeGroup **)px;
        }
        pGroup->oldThreshold = INT64_MIN;
        pGroup->newThreshold = INT64_MAX;
        if (TD_DLIST_NODE_NEXT(pGroup) == NULL && TD_DLIST_TAIL(&pContext->groupsToCheck) != pGroup) {
          TD_DLIST_APPEND(&pContext->groupsToCheck, pGroup);
        }
      }
    } else {
      QUERY_CHECK_CONDITION(TD_DLIST_NELES(&pContext->groupsToCheck) == 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
      code = stTriggerTaskAddWaitSession(pTask, pContext->sessionId, pContext->periodWindow.ekey);
      QUERY_CHECK_CODE(code, lino, _end);
      goto _end;
    }
  }

  while (TD_DLIST_NELES(&pContext->groupsToCheck) > 0) {
    SSTriggerRealtimeGroup *pGroup = TD_DLIST_HEAD(&pContext->groupsToCheck);
    switch (pContext->status) {
      case STRIGGER_CONTEXT_FETCH_META: {
        pContext->status = STRIGGER_CONTEXT_ACQUIRE_REQUEST;
      }
      case STRIGGER_CONTEXT_ACQUIRE_REQUEST: {
        if (pContext->pCalcReq == NULL && pTask->calcEventType != STRIGGER_EVENT_WINDOW_NONE) {
          code = stTriggerTaskAcquireRequest(pTask, pContext->sessionId, pGroup->gid, &pContext->pCalcReq);
          QUERY_CHECK_CODE(code, lino, _end);
          if (pContext->pCalcReq == NULL) {
            ST_TASK_DLOG("no available runner for group %" PRId64, pGroup->gid);
            goto _end;
          }
        }
        pContext->status = STRIGGER_CONTEXT_CHECK_CONDITION;
      }
      case STRIGGER_CONTEXT_CHECK_CONDITION: {
        code = stRealtimeGroupCheck(pGroup);
        QUERY_CHECK_CODE(code, lino, _end);
        pContext->reenterCheck = true;
        if (pContext->pColRefToFetch != NULL) {
          code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_WAL_DATA);
          QUERY_CHECK_CODE(code, lino, _end);
          goto _end;
        } else if (pContext->pMetaToFetch != NULL) {
          if (pTask->triggerType == STREAM_TRIGGER_SLIDING || pTask->triggerType == STREAM_TRIGGER_SESSION ||
              pTask->triggerType == STREAM_TRIGGER_COUNT) {
            code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_WAL_TS_DATA);
            QUERY_CHECK_CODE(code, lino, _end);
          } else {
            code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_WAL_TRIGGER_DATA);
            QUERY_CHECK_CODE(code, lino, _end);
          }
          goto _end;
        }

        if (taosArrayGetSize(pContext->pNotifyParams) > 0) {
          code = streamSendNotifyContent(&pTask->task, pTask->streamName, pTask->triggerType, pGroup->gid,
                                         pTask->pNotifyAddrUrls, pTask->notifyErrorHandle,
                                         TARRAY_DATA(pContext->pNotifyParams), TARRAY_SIZE(pContext->pNotifyParams));
          QUERY_CHECK_CODE(code, lino, _end);
        }
        stRealtimeGroupClearTempState(pGroup);
        pContext->status = STRIGGER_CONTEXT_SEND_CALC_REQ;
      }
      case STRIGGER_CONTEXT_SEND_CALC_REQ: {
        if (pContext->pCalcReq == NULL) {
          QUERY_CHECK_CONDITION(TARRAY_SIZE(pGroup->pPendingCalcParams) == 0, code, lino, _end,
                                TSDB_CODE_INTERNAL_ERROR);
          // do nothing
        } else {
          if (TARRAY_SIZE(pContext->pCalcReq->params) == 0) {
            int32_t nParams = taosArrayGetSize(pGroup->pPendingCalcParams);
            bool    needCalc = (pTask->lowLatencyCalc && (nParams > 0)) || (nParams >= STREAM_CALC_REQ_MAX_WIN_NUM);
            if (needCalc) {
              int32_t nCalcParams = TMIN(nParams, STREAM_CALC_REQ_MAX_WIN_NUM);
              void   *px =
                  taosArrayAddBatch(pContext->pCalcReq->params, TARRAY_DATA(pGroup->pPendingCalcParams), nCalcParams);
              QUERY_CHECK_NULL(px, code, lino, _end, terrno);
              taosArrayPopFrontBatch(pGroup->pPendingCalcParams, nCalcParams);
            }
          }
          if (TARRAY_SIZE(pContext->pCalcReq->params) > 0) {
            code = stRealtimeContextSendCalcReq(pContext);
            QUERY_CHECK_CODE(code, lino, _end);
            if (pContext->pCalcReq != NULL) {
              // calc req has not been sent
              goto _end;
            }
            stRealtimeGroupClearTempState(pGroup);
          } else {
            code = stTriggerTaskReleaseRequest(pTask, &pContext->pCalcReq);
            QUERY_CHECK_CODE(code, lino, _end);
          }
        }
        stRealtimeGroupClearMetadatas(pGroup);
        break;
      }
      default: {
        ST_TASK_ELOG("invalid context status %d at %s:%d", pContext->status, __func__, __LINE__);
        code = TSDB_CODE_INVALID_PARA;
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
    TD_DLIST_POP(&pContext->groupsToCheck, pGroup);
    int32_t nRemainParams = taosArrayGetSize(pGroup->pPendingCalcParams);
    bool    needMoreCalc =
        (pTask->lowLatencyCalc && (nRemainParams > 0) || (nRemainParams >= STREAM_CALC_REQ_MAX_WIN_NUM));
    if (needMoreCalc) {
      // the group has remaining calc params to be calculated
      TD_DLIST_APPEND(&pContext->groupsToCheck, pGroup);
    }
    pContext->status = STRIGGER_CONTEXT_ACQUIRE_REQUEST;
  }

  if (pTask->maxDelayNs > 0 && TD_DLIST_NELES(&pContext->groupsMaxDelay) == 0) {
    int64_t now = taosGetTimestampNs();
    int32_t iter = 0;
    void   *px = tSimpleHashIterate(pContext->pGroups, NULL, &iter);
    while (px != NULL) {
      SSTriggerRealtimeGroup *pGroup = *(SSTriggerRealtimeGroup **)px;
      if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
        SSTriggerWindow *p = TRINGBUF_HEAD(&pGroup->winBuf);
        do {
          if (p->prevProcTime + pTask->maxDelayNs <= now) {
            TD_DLIST_APPEND(&pContext->groupsMaxDelay, pGroup);
            break;
          }
          TRINGBUF_MOVE_NEXT(&pGroup->winBuf, p);
        } while (p != TRINGBUF_TAIL(&pGroup->winBuf));
      }
      px = tSimpleHashIterate(pContext->pGroups, px, &iter);
    }
  }

  while (TD_DLIST_NELES(&pContext->groupsMaxDelay) > 0) {
    SSTriggerRealtimeGroup *pGroup = TD_DLIST_HEAD(&pContext->groupsMaxDelay);
    switch (pContext->status) {
      case STRIGGER_CONTEXT_FETCH_META: {
        pContext->status = STRIGGER_CONTEXT_ACQUIRE_REQUEST;
      }
      case STRIGGER_CONTEXT_ACQUIRE_REQUEST: {
        if (pContext->pCalcReq == NULL && pTask->calcEventType != STRIGGER_EVENT_WINDOW_NONE) {
          code = stTriggerTaskAcquireRequest(pTask, pContext->sessionId, pGroup->gid, &pContext->pCalcReq);
          QUERY_CHECK_CODE(code, lino, _end);
          if (pContext->pCalcReq == NULL) {
            ST_TASK_DLOG("no available runner for group %" PRId64, pGroup->gid);
            goto _end;
          }
        }
        pContext->status = STRIGGER_CONTEXT_CHECK_CONDITION;
      }
      case STRIGGER_CONTEXT_CHECK_CONDITION: {
        int64_t now = taosGetTimestampNs();
        QUERY_CHECK_CONDITION(IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        SSTriggerWindow *pHead = TRINGBUF_HEAD(&pGroup->winBuf);
        SSTriggerWindow *p = pHead;
        do {
          if (p->prevProcTime + pTask->maxDelayNs <= now) {
            SSTriggerCalcParam param = {
                .triggerTime = now,
                .wstart = p->range.skey,
                .wend = p->range.ekey,
                .wduration = p->range.ekey - p->range.skey,
                .wrownum = (p == pHead) ? p->wrownum : (pHead->wrownum - p->wrownum),
            };
            if (pTask->calcEventType & STRIGGER_EVENT_WINDOW_CLOSE) {
              void *px = taosArrayPush(pContext->pCalcReq->params, &param);
              QUERY_CHECK_NULL(px, code, lino, _end, terrno);
            } else if (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_CLOSE) {
              void *px = taosArrayPush(pContext->pNotifyParams, &param);
              QUERY_CHECK_NULL(px, code, lino, _end, terrno);
            }
            p->prevProcTime = now;
          }
          TRINGBUF_MOVE_NEXT(&pGroup->winBuf, p);
        } while (p != TRINGBUF_TAIL(&pGroup->winBuf));

        if (taosArrayGetSize(pContext->pNotifyParams) > 0) {
          code = streamSendNotifyContent(&pTask->task, pTask->streamName, pTask->triggerType, pGroup->gid,
                                         pTask->pNotifyAddrUrls, pTask->notifyErrorHandle,
                                         TARRAY_DATA(pContext->pNotifyParams), TARRAY_SIZE(pContext->pNotifyParams));
          QUERY_CHECK_CODE(code, lino, _end);
        }
        stRealtimeGroupClearTempState(pGroup);
        pContext->status = STRIGGER_CONTEXT_SEND_CALC_REQ;
      }
      case STRIGGER_CONTEXT_SEND_CALC_REQ: {
        int32_t nParams = taosArrayGetSize(pContext->pCalcReq->params);
        bool    needCalc = (nParams > 0);
        if (needCalc) {
          QUERY_CHECK_NULL(pContext->pCalcReq, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
          QUERY_CHECK_CONDITION(nParams <= STREAM_CALC_REQ_MAX_WIN_NUM, code, lino, _end, TSDB_CODE_INVALID_PARA);
          code = stRealtimeContextSendCalcReq(pContext);
          QUERY_CHECK_CODE(code, lino, _end);
          if (pContext->pCalcReq != NULL) {
            // calc req has not been sent
            goto _end;
          }
          stRealtimeGroupClearTempState(pGroup);
        } else if (pContext->pCalcReq != NULL) {
          code = stTriggerTaskReleaseRequest(pTask, &pContext->pCalcReq);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        break;
      }
      default: {
        ST_TASK_ELOG("invalid context status %d at %s:%d", pContext->status, __func__, __LINE__);
        code = TSDB_CODE_INVALID_PARA;
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
    TD_DLIST_POP(&pContext->groupsMaxDelay, pGroup);
    pContext->status = STRIGGER_CONTEXT_ACQUIRE_REQUEST;
  }

  if (pTask->triggerType == STREAM_TRIGGER_PERIOD) {
    stTriggerTaskNextPeriodWindow(pTask, &pContext->periodWindow);
    pContext->status = STRIGGER_CONTEXT_IDLE;
    code = stTriggerTaskAddWaitSession(pTask, pContext->sessionId, pContext->periodWindow.ekey);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
#define STRIGGER_CHECKPOINT_INTERVAL_NS 10 * NANOSECOND_PER_MINUTE  // 10min
    int64_t now = taosGetTimestampNs();
    if (pContext->lastCheckpointTime + STRIGGER_CHECKPOINT_INTERVAL_NS <= now) {
      // do checkpoint
      uint8_t *buf = NULL;
      int64_t  len = 0;
      do {
        stDebug("[checkpoint] generate checkpoint for stream %" PRIx64, pTask->task.streamId);
        code = stTriggerTaskGenCheckpoint(pTask, buf, &len);
        if (code != 0) break;
        buf = taosMemoryMalloc(len);
        code = stTriggerTaskGenCheckpoint(pTask, buf, &len);
        if (code != 0) break;
        code = streamWriteCheckPoint(pTask->task.streamId, buf, len);
        if (code != 0) break;
        int32_t leaderSid = pTask->leaderSnodeId;
        SEpSet *epSet = gStreamMgmt.getSynEpset(leaderSid);
        if (epSet != NULL) {
          code = streamSyncWriteCheckpoint(pTask->task.streamId, epSet, buf, len);
          buf = NULL;
        }
      } while (0);
      taosMemoryFree(buf);
      QUERY_CHECK_CODE(code, lino, _end);
      pContext->lastCheckpointTime = now;
    }
    // todo(kjq): start history calc if needed
    if (!pContext->getWalMetaThisRound) {
      // add the task to wait list since it catches up all readers
      pContext->status = STRIGGER_CONTEXT_IDLE;
      int64_t resumeTime = taosGetTimestampNs() + STREAM_TRIGGER_WAIT_TIME_NS;
      code = stTriggerTaskAddWaitSession(pTask, pContext->sessionId, resumeTime);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      // pull new wal metas
      pContext->status = STRIGGER_CONTEXT_FETCH_META;
      for (pContext->curReaderIdx = 0; pContext->curReaderIdx < TARRAY_SIZE(pTask->readerList);
           pContext->curReaderIdx++) {
        code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_WAL_META);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stRealtimeContextRestart(SSTriggerRealtimeContext *pContext) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pContext->pTask;
  int32_t             iter = 0;
  if (!pTask->fillHistory && !pTask->fillHistoryFirst) {
    for (int32_t i = 0; i < TARRAY_SIZE(pTask->readerList); i++) {
      SStreamTaskAddr      *pReader = TARRAY_GET_ELEM(pTask->readerList, i);
      SSTriggerWalProgress *pProgress = tSimpleHashGet(pContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      void *px = tSimpleHashGet(pTask->pRealtimeStartVer, &pProgress->pTaskAddr->nodeId, sizeof(int32_t));
      QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pProgress->lastScanVer = pProgress->latestVer = *(int64_t *)px;
    }
  } else {
    SSTriggerWalProgress *pProgress = tSimpleHashIterate(pContext->pReaderWalProgress, NULL, &iter);
    while (pProgress != NULL) {
      pProgress->lastScanVer = pProgress->latestVer = 0;
      pProgress = tSimpleHashIterate(pContext->pReaderWalProgress, pProgress, &iter);
    }
  }
  tSimpleHashClear(pContext->pGroups);
  if (pTask->isVirtualTable) {
    int32_t nVirTables = taosArrayGetSize(pTask->pVirTableInfoRsp);
    for (int32_t i = 0; i < nVirTables; i++) {
      VTableInfo *pInfo = TARRAY_GET_ELEM(pTask->pVirTableInfoRsp, i);

      void *px = tSimpleHashGet(pContext->pGroups, &pInfo->gId, sizeof(int64_t));
      if (px == NULL) {
        SSTriggerRealtimeGroup *pGroup = taosMemoryCalloc(1, sizeof(SSTriggerRealtimeGroup));
        QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
        code = tSimpleHashPut(pContext->pGroups, &pInfo->gId, sizeof(int64_t), &pGroup, POINTER_BYTES);
        if (code != TSDB_CODE_SUCCESS) {
          taosMemoryFreeClear(pGroup);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        code = stRealtimeGroupInit(pGroup, pContext, pInfo->gId);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
  }
  TD_DLIST_INIT(&pContext->groupsToCheck);
  TD_DLIST_INIT(&pContext->groupsMaxDelay);
  pContext->haveToRecalc = false;

  pContext->status = STRIGGER_CONTEXT_FETCH_META;
  for (pContext->curReaderIdx = 0; pContext->curReaderIdx < TARRAY_SIZE(pTask->readerList); pContext->curReaderIdx++) {
    code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_WAL_META);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stRealtimeContextProcPullRsp(SSTriggerRealtimeContext *pContext, SRpcMsg *pRsp) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SStreamTriggerTask       *pTask = pContext->pTask;
  SSDataBlock              *pDataBlock = NULL;
  SArray                   *pAllMetadatas = NULL;
  SArray                   *pVgIds = NULL;
  SArray                   *pSavedGroupsToCheck = NULL;
  SStreamMsgVTableInfo      vtableInfo = {0};
  SSTriggerOrigTableInfoRsp otableInfo = {0};
  SArray                   *pOrigTableNames = NULL;

  QUERY_CHECK_CONDITION(pRsp->code == TSDB_CODE_SUCCESS || pRsp->code == TSDB_CODE_STREAM_NO_DATA, code, lino, _end,
                        TSDB_CODE_INVALID_PARA);

  SMsgSendInfo         *ahandle = pRsp->info.ahandle;
  SSTriggerAHandle     *pAhandle = ahandle->param;
  SSTriggerPullRequest *pReq = pAhandle->param;
  switch (pReq->type) {
    case STRIGGER_PULL_LAST_TS: {
      QUERY_CHECK_CONDITION(pContext->status == STRIGGER_CONTEXT_DETERMINE_BOUND, code, lino, _end,
                            TSDB_CODE_INTERNAL_ERROR);
      SSTriggerWalProgress *pProgress = NULL;
      for (int32_t i = 0; i < TARRAY_SIZE(pTask->readerList); i++) {
        SStreamTaskAddr      *pReader = TARRAY_GET_ELEM(pTask->readerList, i);
        SSTriggerWalProgress *pTempProgress =
            tSimpleHashGet(pContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t));
        QUERY_CHECK_NULL(pTempProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        if (&pTempProgress->pullReq.base == pReq) {
          pProgress = pTempProgress;
          break;
        }
      }
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INVALID_PARA);
      int32_t vgId = pProgress->pTaskAddr->nodeId;

      pDataBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
      QUERY_CHECK_NULL(pDataBlock, code, lino, _end, terrno);
      code = tDeserializeSStreamTsResponse(pRsp->pCont, pRsp->contLen, pDataBlock);
      QUERY_CHECK_CODE(code, lino, _end);
      int64_t latestVer = pDataBlock->info.id.groupId;
      void   *px = tSimpleHashGet(pTask->pRealtimeStartVer, &vgId, sizeof(int32_t));
      QUERY_CHECK_CONDITION(px == NULL, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      code = tSimpleHashPut(pTask->pRealtimeStartVer, &vgId, sizeof(int32_t), &latestVer, sizeof(int64_t));
      QUERY_CHECK_CODE(code, lino, _end);
      int32_t nrows = blockDataGetNumOfRows(pDataBlock);
      if (nrows > 0) {
        int32_t          iCol = 0;
        SColumnInfoData *pGidCol = taosArrayGet(pDataBlock->pDataBlock, iCol++);
        QUERY_CHECK_NULL(pGidCol, code, lino, _end, terrno);
        int64_t         *pGidData = (int64_t *)pGidCol->pData;
        SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, iCol++);
        QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
        int64_t *pTsData = (int64_t *)pTsCol->pData;
        for (int32_t i = 0; i < nrows; i++) {
          px = tSimpleHashGet(pTask->pHistoryCutoffTime, &pGidData[i], sizeof(int64_t));
          if (px == NULL) {
            code =
                tSimpleHashPut(pTask->pHistoryCutoffTime, &pGidData[i], sizeof(int64_t), &pTsData[i], sizeof(int64_t));
            QUERY_CHECK_CODE(code, lino, _end);
          } else {
            *(int64_t *)px = TMAX(*(int64_t *)px, pTsData[i]);
          }
        }
      }

      if (--pContext->curReaderIdx > 0) {
        // wait for responses from other readers
        goto _end;
      }

#if !TRIGGER_USE_HISTORY_META
      bool startFromBound = !pTask->fillHistory && !pTask->fillHistoryFirst;
#else
      bool startFromBound = true;
#endif
      if (startFromBound) {
        for (int32_t i = 0; i < TARRAY_SIZE(pTask->readerList); i++) {
          SStreamTaskAddr      *pReader = TARRAY_GET_ELEM(pTask->readerList, i);
          SSTriggerWalProgress *pProgress =
              tSimpleHashGet(pContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t));
          QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
          void *px = tSimpleHashGet(pTask->pRealtimeStartVer, &pProgress->pTaskAddr->nodeId, sizeof(int32_t));
          QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
          pProgress->lastScanVer = pProgress->latestVer = *(int64_t *)px;
        }
      }
      pContext->status = STRIGGER_CONTEXT_IDLE;
      code = stRealtimeContextCheck(pContext);
      QUERY_CHECK_CODE(code, lino, _end);
      break;
    }

    case STRIGGER_PULL_WAL_META: {
      QUERY_CHECK_CONDITION(pContext->status == STRIGGER_CONTEXT_FETCH_META, code, lino, _end,
                            TSDB_CODE_INTERNAL_ERROR);
      SSTriggerWalProgress *pProgress = NULL;
      for (int32_t i = 0; i < TARRAY_SIZE(pTask->readerList); i++) {
        SStreamTaskAddr      *pReader = TARRAY_GET_ELEM(pTask->readerList, i);
        SSTriggerWalProgress *pTempProgress =
            tSimpleHashGet(pContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t));
        QUERY_CHECK_NULL(pTempProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        if (&pTempProgress->pullReq.base == pReq) {
          pProgress = pTempProgress;
          break;
        }
      }
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INVALID_PARA);

      pDataBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
      QUERY_CHECK_NULL(pDataBlock, code, lino, _end, terrno);
      if (pRsp->code == TSDB_CODE_STREAM_NO_DATA) {
        QUERY_CHECK_CONDITION(pRsp->contLen == sizeof(int64_t), code, lino, _end, TSDB_CODE_INVALID_PARA);
        blockDataEmpty(pDataBlock);
        pDataBlock->info.id.groupId = *(int64_t *)pRsp->pCont;
      } else {
        QUERY_CHECK_CONDITION(pRsp->contLen > 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
        const char *pCont = pRsp->pCont;
        code = blockDecode(pDataBlock, pCont, &pCont);
        QUERY_CHECK_CODE(code, lino, _end);
        QUERY_CHECK_CONDITION(pCont == (char *)pRsp->pCont + pRsp->contLen, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      }

      // update reader wal progress
      int32_t nrows = blockDataGetNumOfRows(pDataBlock);
      pProgress->lastScanVer = pDataBlock->info.id.groupId;
      pProgress->latestVer = pDataBlock->info.id.groupId;
      if (nrows > 0) {
        int32_t          ncols = blockDataGetNumOfCols(pDataBlock);
        SColumnInfoData *pVerCol = taosArrayGet(pDataBlock->pDataBlock, ncols - 2);
        pProgress->lastScanVer = *(int64_t *)colDataGetNumData(pVerCol, nrows - 1);
        pContext->getWalMetaThisRound = true;
      }
      void *px = taosArrayPush(pProgress->pMetadatas, &pDataBlock);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      pDataBlock = NULL;

      if (--pContext->curReaderIdx > 0) {
        // wait for responses from other readers
        goto _end;
      }

      bool continueToFetch = false;
      pContext->getWalMetaThisRound = false;
      for (int32_t i = 0; i < TARRAY_SIZE(pTask->readerList); i++) {
        SStreamTaskAddr      *pReader = TARRAY_GET_ELEM(pTask->readerList, i);
        SSTriggerWalProgress *pTempProgress =
            tSimpleHashGet(pContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t));
        QUERY_CHECK_NULL(pTempProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        SSDataBlock *pBlock =
            *(SSDataBlock **)TARRAY_GET_ELEM(pTempProgress->pMetadatas, TARRAY_SIZE(pTempProgress->pMetadatas) - 1);
        int32_t nrows = blockDataGetNumOfRows(pBlock);
        if (nrows >= STREAM_RETURN_ROWS_NUM) {
          continueToFetch = true;
          break;
        } else if (nrows > 0) {
          pContext->getWalMetaThisRound = true;
        }
      }

      if (continueToFetch) {
        for (pContext->curReaderIdx = 0; pContext->curReaderIdx < TARRAY_SIZE(pTask->readerList);
             pContext->curReaderIdx++) {
          code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_WAL_META);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        goto _end;
      }

      // collect all metadatas
      pAllMetadatas = taosArrayInit(0, sizeof(SSDataBlock *));
      QUERY_CHECK_NULL(pAllMetadatas, code, lino, _end, terrno);
      pVgIds = taosArrayInit(0, sizeof(int32_t));
      QUERY_CHECK_NULL(pVgIds, code, lino, _end, terrno);
      for (int32_t i = 0; i < TARRAY_SIZE(pTask->readerList); i++) {
        SStreamTaskAddr      *pReader = TARRAY_GET_ELEM(pTask->readerList, i);
        SSTriggerWalProgress *pTempProgress =
            tSimpleHashGet(pContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t));
        QUERY_CHECK_NULL(pTempProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        void *px = taosArrayAddAll(pAllMetadatas, pTempProgress->pMetadatas);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
        for (int32_t j = 0; j < TARRAY_SIZE(pTempProgress->pMetadatas); j++) {
          void *px = taosArrayPush(pVgIds, &pTempProgress->pTaskAddr->nodeId);
          QUERY_CHECK_NULL(px, code, lino, _end, terrno);
        }
        taosArrayClear(pTempProgress->pMetadatas);
      }

#if !TRIGGER_USE_HISTORY_META
      while (TD_DLIST_NELES(&pContext->groupsToCheck) > 0) {
        SSTriggerRealtimeGroup *pGroup = TD_DLIST_HEAD(&pContext->groupsToCheck);
        if (pSavedGroupsToCheck == NULL) {
          pSavedGroupsToCheck = taosArrayInit(0, POINTER_BYTES);
          QUERY_CHECK_NULL(pSavedGroupsToCheck, code, lino, _end, terrno);
        }
        void *px = taosArrayPush(pSavedGroupsToCheck, &pGroup);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
        TD_DLIST_POP(&pContext->groupsToCheck, pGroup);
      }
#endif

      if (!pTask->isVirtualTable) {
        for (int32_t i = 0; i < TARRAY_SIZE(pAllMetadatas); i++) {
          SSDataBlock *pBlock = *(SSDataBlock **)TARRAY_GET_ELEM(pAllMetadatas, i);
          int32_t      nrows = blockDataGetNumOfRows(pBlock);
          if (nrows == 0) {
            continue;
          }
          SColumnInfoData *pGidCol = taosArrayGet(pBlock->pDataBlock, 1);
          QUERY_CHECK_NULL(pGidCol, code, lino, _end, terrno);
          int64_t *pGidData = (int64_t *)pGidCol->pData;
          for (int32_t i = 0; i < nrows; i++) {
            void                   *px = tSimpleHashGet(pContext->pGroups, &pGidData[i], sizeof(int64_t));
            SSTriggerRealtimeGroup *pGroup = NULL;
            if (px == NULL) {
              pGroup = taosMemoryCalloc(1, sizeof(SSTriggerRealtimeGroup));
              QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
              code = tSimpleHashPut(pContext->pGroups, &pGidData[i], sizeof(int64_t), &pGroup, POINTER_BYTES);
              if (code != TSDB_CODE_SUCCESS) {
                taosMemoryFreeClear(pGroup);
                QUERY_CHECK_CODE(code, lino, _end);
              }
              code = stRealtimeGroupInit(pGroup, pContext, pGidData[i]);
              QUERY_CHECK_CODE(code, lino, _end);
            } else {
              pGroup = *(SSTriggerRealtimeGroup **)px;
            }
            if (TD_DLIST_NODE_NEXT(pGroup) == NULL && TD_DLIST_TAIL(&pContext->groupsToCheck) != pGroup) {
              code = stRealtimeGroupAddMetaDatas(pGroup, pAllMetadatas, pVgIds);
              QUERY_CHECK_CODE(code, lino, _end);
              if (pGroup->newThreshold > pGroup->oldThreshold) {
                TD_DLIST_APPEND(&pContext->groupsToCheck, pGroup);
              }
            }
          }
        }
      } else {
        int32_t iter = 0;
        void   *px = tSimpleHashIterate(pContext->pGroups, NULL, &iter);
        while (px != NULL) {
          SSTriggerRealtimeGroup *pGroup = *(SSTriggerRealtimeGroup **)px;
          code = stRealtimeGroupAddMetaDatas(pGroup, pAllMetadatas, pVgIds);
          QUERY_CHECK_CODE(code, lino, _end);
          if (pGroup->newThreshold > pGroup->oldThreshold) {
            TD_DLIST_APPEND(&pContext->groupsToCheck, pGroup);
          }
          px = tSimpleHashIterate(pContext->pGroups, px, &iter);
        }
      }

#if !TRIGGER_USE_HISTORY_META
      while (taosArrayGetSize(pSavedGroupsToCheck) > 0) {
        SSTriggerRealtimeGroup *pGroup = *(SSTriggerRealtimeGroup **)taosArrayPop(pSavedGroupsToCheck);
        if (TD_DLIST_NODE_NEXT(pGroup) == NULL && TD_DLIST_TAIL(&pContext->groupsToCheck) != pGroup) {
          TD_DLIST_PREPEND(&pContext->groupsToCheck, pGroup);
        }
      }
      if (pContext->haveToRecalc) {
        ST_TASK_DLOG("[recalc] restart realtime context: %p", pContext);
        code = stRealtimeContextRestart(pContext);
        QUERY_CHECK_CODE(code, lino, _end);
        goto _end;
      } else if (tSimpleHashGetSize(pTask->pRecalcLastVer) > 0) {
        bool                  needMoreMeta = false;
        int32_t               iter = 0;
        SSTriggerWalProgress *pProgress = tSimpleHashIterate(pContext->pReaderWalProgress, NULL, &iter);
        while (pProgress != NULL) {
          void *px = tSimpleHashGet(pTask->pRecalcLastVer, &pProgress->pTaskAddr->nodeId, sizeof(int32_t));
          QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
          int64_t ver = *(int64_t *)px;
          if (pProgress->lastScanVer < ver) {
            needMoreMeta = true;
            break;
          }
          pProgress = tSimpleHashIterate(pContext->pReaderWalProgress, pProgress, &iter);
        }

        if (needMoreMeta) {
          ST_TASK_DLOG("[recalc] context need more meta: %p", pContext);
          for (pContext->curReaderIdx = 0; pContext->curReaderIdx < TARRAY_SIZE(pTask->readerList);
               pContext->curReaderIdx++) {
            code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_WAL_META);
            QUERY_CHECK_CODE(code, lino, _end);
          }
          goto _end;
        } else {
          ST_TASK_DLOG("[recalc] context start to check: %p", pContext);
          tSimpleHashClear(pTask->pRecalcLastVer);
        }
      }
#endif

      code = stRealtimeContextCheck(pContext);
      QUERY_CHECK_CODE(code, lino, _end);
      break;
    }

    case STRIGGER_PULL_WAL_TS_DATA:
    case STRIGGER_PULL_WAL_TRIGGER_DATA:
    case STRIGGER_PULL_WAL_CALC_DATA:
    case STRIGGER_PULL_WAL_DATA: {
      QUERY_CHECK_CONDITION(
          pContext->status == STRIGGER_CONTEXT_CHECK_CONDITION || pContext->status == STRIGGER_CONTEXT_SEND_CALC_REQ,
          code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pDataBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
      QUERY_CHECK_NULL(pDataBlock, code, lino, _end, terrno);
      if (pRsp->contLen > 0) {
        const char *pCont = pRsp->pCont;
        code = blockDecode(pDataBlock, pCont, &pCont);
        QUERY_CHECK_CODE(code, lino, _end);
        QUERY_CHECK_CONDITION(pCont == (char *)pRsp->pCont + pRsp->contLen, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      } else {
        blockDataEmpty(pDataBlock);
      }
      if (pContext->pColRefToFetch != NULL) {
        code = stVtableMergerBindDataBlock(pContext->pMerger, &pDataBlock);
        TSDB_CHECK_CODE(code, lino, _end);
      } else {
        code = stTimestampSorterBindDataBlock(pContext->pSorter, &pDataBlock);
        TSDB_CHECK_CODE(code, lino, _end);
      }
      pContext->pColRefToFetch = NULL;
      pContext->pMetaToFetch = NULL;
      code = stRealtimeContextCheck(pContext);
      QUERY_CHECK_CODE(code, lino, _end);
      break;
    }

    case STRIGGER_PULL_GROUP_COL_VALUE: {
      QUERY_CHECK_CONDITION(pContext->status == STRIGGER_CONTEXT_SEND_CALC_REQ, code, lino, _end,
                            TSDB_CODE_INTERNAL_ERROR);
      SStreamGroupInfo groupInfo = {.gInfo = pContext->pCalcReq->groupColVals};
      code = tDeserializeSStreamGroupInfo(pRsp->pCont, pRsp->contLen, &groupInfo);
      QUERY_CHECK_CODE(code, lino, _end);
      code = stRealtimeContextCheck(pContext);
      QUERY_CHECK_CODE(code, lino, _end);
      break;
    }

    case STRIGGER_PULL_VTABLE_INFO: {
      QUERY_CHECK_CONDITION(pContext->status == STRIGGER_CONTEXT_GATHER_VTABLE_INFO, code, lino, _end,
                            TSDB_CODE_INTERNAL_ERROR);
      SSTriggerWalProgress *pProgress = NULL;
      for (int32_t i = 0; i < TARRAY_SIZE(pTask->virtReaderList); i++) {
        SStreamTaskAddr      *pReader = TARRAY_GET_ELEM(pTask->virtReaderList, i);
        SSTriggerWalProgress *pTempProgress =
            tSimpleHashGet(pContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t));
        QUERY_CHECK_NULL(pTempProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        if (&pTempProgress->pullReq.base == pReq) {
          pProgress = pTempProgress;
          break;
        }
      }
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INVALID_PARA);
      int32_t vgId = pProgress->pTaskAddr->nodeId;

      code = tDeserializeSStreamMsgVTableInfo(pRsp->pCont, pRsp->contLen, &vtableInfo);
      QUERY_CHECK_CODE(code, lino, _end);
      int32_t nVirTables = taosArrayGetSize(vtableInfo.infos);
      for (int32_t i = 0; i < nVirTables; i++) {
        VTableInfo           *pInfo = TARRAY_GET_ELEM(vtableInfo.infos, i);
        SSTriggerVirTableInfo newInfo = {.tbGid = pInfo->gId, .tbUid = pInfo->uid, .tbVer = pInfo->ver, .vgId = vgId};
        code = tSimpleHashPut(pTask->pVirTableInfos, &newInfo.tbUid, sizeof(int64_t), &newInfo,
                              sizeof(SSTriggerVirTableInfo));
        QUERY_CHECK_CODE(code, lino, _end);
        for (int32_t j = 0; j < pInfo->cols.nCols; j++) {
          SColRef *pColRef = &pInfo->cols.pColRef[j];
          if (!pColRef->hasRef) {
            continue;
          }
          SSHashObj *pDbInfo = NULL;
          size_t     dbNameLen = strlen(pColRef->refDbName) + 1;
          size_t     tbNameLen = strlen(pColRef->refTableName) + 1;
          size_t     colNameLen = strlen(pColRef->refColName) + 1;
          void      *px = tSimpleHashGet(pTask->pOrigTableCols, pColRef->refDbName, dbNameLen);
          if (px == NULL) {
            pDbInfo = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
            QUERY_CHECK_NULL(pDbInfo, code, lino, _end, terrno);
            code = tSimpleHashPut(pTask->pOrigTableCols, pColRef->refDbName, dbNameLen, &pDbInfo, POINTER_BYTES);
            if (code != TSDB_CODE_SUCCESS) {
              tSimpleHashCleanup(pDbInfo);
              QUERY_CHECK_CODE(code, lino, _end);
            }
          } else {
            pDbInfo = *(SSHashObj **)px;
          }
          SSTriggerOrigTableInfo *pTbInfo = tSimpleHashGet(pDbInfo, pColRef->refTableName, tbNameLen);
          if (pTbInfo == NULL) {
            SSTriggerOrigTableInfo newInfo = {0};
            code = tSimpleHashPut(pDbInfo, pColRef->refTableName, tbNameLen, &newInfo, sizeof(SSTriggerOrigTableInfo));
            QUERY_CHECK_CODE(code, lino, _end);
            pTbInfo = tSimpleHashGet(pDbInfo, pColRef->refTableName, tbNameLen);
            QUERY_CHECK_NULL(pTbInfo, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
          }
          if (pTbInfo->pColumns == NULL) {
            pTbInfo->pColumns = tSimpleHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
            QUERY_CHECK_NULL(pTbInfo->pColumns, code, lino, _end, terrno);
          }
          col_id_t colid = 0;
          tSimpleHashPut(pTbInfo->pColumns, pColRef->refColName, colNameLen, &colid, sizeof(col_id_t));
        }
      }

      void *px = taosArrayAddAll(pTask->pVirTableInfoRsp, vtableInfo.infos);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      taosArrayClear(vtableInfo.infos);

      if (--pContext->curReaderIdx > 0) {
        // wait for responses from other readers
        goto _end;
      }

      pOrigTableNames = taosArrayInit(0, sizeof(SStreamDbTableName));
      QUERY_CHECK_NULL(pOrigTableNames, code, lino, _end, terrno);
      int32_t iter1 = 0;
      px = tSimpleHashIterate(pTask->pOrigTableCols, NULL, &iter1);
      while (px != NULL) {
        char                   *dbName = tSimpleHashGetKey(px, NULL);
        SSHashObj              *pDbInfo = *(SSHashObj **)px;
        int32_t                 iter2 = 0;
        SSTriggerOrigTableInfo *pTbInfo = tSimpleHashIterate(pDbInfo, NULL, &iter2);
        while (pTbInfo != NULL) {
          char               *tbName = tSimpleHashGetKey(pTbInfo, NULL);
          SStreamDbTableName *pName = taosArrayReserve(pOrigTableNames, 1);
          QUERY_CHECK_NULL(pName, code, lino, _end, terrno);
          (void)snprintf(pName->dbFName, sizeof(pName->dbFName), "%d.%s", 1, dbName);
          (void)strncpy(pName->tbName, tbName, sizeof(pName->tbName));
          pTbInfo = tSimpleHashIterate(pDbInfo, pTbInfo, &iter2);
        }
        px = tSimpleHashIterate(pTask->pOrigTableCols, px, &iter1);
      }
      SStreamMgmtReq *pReq = taosMemoryCalloc(1, sizeof(SStreamMgmtReq));
      QUERY_CHECK_NULL(pReq, code, lino, _end, terrno);
      pReq->reqId = atomic_fetch_add_64(&pTask->mgmtReqId, 1);
      pReq->type = STREAM_MGMT_REQ_TRIGGER_ORIGTBL_READER;
      pReq->cont.fullTableNames = pOrigTableNames;
      pOrigTableNames = NULL;

      // wait to be exeucted again
      pContext->status = STRIGGER_CONTEXT_IDLE;
      pTask->task.pMgmtReq = pReq;
      pTask->task.status = STREAM_STATUS_INIT;
      break;
    }

    case STRIGGER_PULL_OTABLE_INFO: {
      QUERY_CHECK_CONDITION(pContext->status == STRIGGER_CONTEXT_GATHER_VTABLE_INFO, code, lino, _end,
                            TSDB_CODE_INTERNAL_ERROR);
      SSTriggerWalProgress *pProgress = NULL;
      for (int32_t i = 0; i < TARRAY_SIZE(pTask->readerList); i++) {
        SStreamTaskAddr      *pReader = TARRAY_GET_ELEM(pTask->readerList, i);
        SSTriggerWalProgress *pTempProgress =
            tSimpleHashGet(pContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t));
        QUERY_CHECK_NULL(pTempProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        if (&pTempProgress->pullReq.base == pReq) {
          pProgress = pTempProgress;
          break;
        }
      }
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INVALID_PARA);
      int32_t vgId = pProgress->pTaskAddr->nodeId;

      code = tDserializeSTriggerOrigTableInfoRsp(pRsp->pCont, pRsp->contLen, &otableInfo);
      QUERY_CHECK_CODE(code, lino, _end);
      SSTriggerOrigTableInfoRequest *pOrigReq = (SSTriggerOrigTableInfoRequest *)pReq;
      QUERY_CHECK_CONDITION(taosArrayGetSize(otableInfo.cols) == taosArrayGetSize(pOrigReq->cols), code, lino, _end,
                            TSDB_CODE_INVALID_PARA);
      OTableInfoRsp *pRsp = TARRAY_DATA(otableInfo.cols);

      void *px = tSimpleHashGet(pTask->pReaderUidMap, &vgId, sizeof(int32_t));
      QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SArray *pUids = *(SArray **)px;
      for (int32_t i = 0; i < TARRAY_SIZE(pUids); i++) {
        SSTriggerOrigTableInfo *pTbInfo = *(SSTriggerOrigTableInfo **)TARRAY_GET_ELEM(pUids, i);
        int32_t                 iter = 0;
        void                   *px = tSimpleHashIterate(pTbInfo->pColumns, NULL, &iter);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
        while (px != NULL) {
          pTbInfo->suid = pRsp->suid;
          pTbInfo->uid = pRsp->uid;
          *(col_id_t *)px = pRsp->cid;
          pRsp++;
          px = tSimpleHashIterate(pTbInfo->pColumns, px, &iter);
        }
        int64_t *pEle = TARRAY_GET_ELEM(pUids, i);
        pEle[0] = pTbInfo->suid;
        pEle[1] = pTbInfo->uid;
      }

      if (--pContext->curReaderIdx > 0) {
        // wait for responses from other readers
        goto _end;
      }

      for (pContext->curReaderIdx = 0; pContext->curReaderIdx < TARRAY_SIZE(pTask->readerList);
           pContext->curReaderIdx++) {
        code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_SET_TABLE);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      break;
    }

    case STRIGGER_PULL_SET_TABLE: {
      if (--pContext->curReaderIdx > 0) {
        // wait for responses from other readers
        goto _end;
      }

      int32_t nVirTables = taosArrayGetSize(pTask->pVirTableInfoRsp);
      for (int32_t i = 0; i < nVirTables; i++) {
        VTableInfo            *pInfo = TARRAY_GET_ELEM(pTask->pVirTableInfoRsp, i);
        SSTriggerVirTableInfo *pNewInfo = tSimpleHashGet(pTask->pVirTableInfos, &pInfo->uid, sizeof(int64_t));
        QUERY_CHECK_NULL(pNewInfo, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        code = stTriggerTaskGenVirColRefs(pTask, pInfo, pTask->pVirTrigSlots, &pNewInfo->pTrigColRefs);
        QUERY_CHECK_CODE(code, lino, _end);
        code = stTriggerTaskGenVirColRefs(pTask, pInfo, pTask->pVirCalcSlots, &pNewInfo->pCalcColRefs);
        QUERY_CHECK_CODE(code, lino, _end);

        void *px = tSimpleHashGet(pContext->pGroups, &pInfo->gId, sizeof(int64_t));
        if (px == NULL) {
          SSTriggerRealtimeGroup *pGroup = taosMemoryCalloc(1, sizeof(SSTriggerRealtimeGroup));
          QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
          code = tSimpleHashPut(pContext->pGroups, &pInfo->gId, sizeof(int64_t), &pGroup, POINTER_BYTES);
          if (code != TSDB_CODE_SUCCESS) {
            taosMemoryFreeClear(pGroup);
            QUERY_CHECK_CODE(code, lino, _end);
          }
          code = stRealtimeGroupInit(pGroup, pContext, pInfo->gId);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }

      pTask->virTableInfoReady = true;
      pContext->status = STRIGGER_CONTEXT_IDLE;
      code = stRealtimeContextCheck(pContext);
      QUERY_CHECK_CODE(code, lino, _end);
      break;
    }

    default: {
      ST_TASK_ELOG("invalid pull request type %d at %s", pReq->type, __func__);
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (pDataBlock != NULL) {
    blockDataDestroy(pDataBlock);
  }
  if (pAllMetadatas != NULL) {
    taosArrayDestroyP(pAllMetadatas, (FDelete)blockDataDestroy);
  }
  if (pVgIds != NULL) {
    taosArrayDestroy(pVgIds);
  }
  if (pSavedGroupsToCheck != NULL) {
    taosArrayDestroy(pSavedGroupsToCheck);
  }
  tDestroySStreamMsgVTableInfo(&vtableInfo);
  tDestroySTriggerOrigTableInfoRsp(&otableInfo);
  if (pOrigTableNames != NULL) {
    taosArrayDestroy(pOrigTableNames);
  }
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s, type: %d", __func__, lino, tstrerror(code), pReq->type);
  }
  return code;
}

static int32_t stRealtimeContextProcCalcRsp(SSTriggerRealtimeContext *pContext, SRpcMsg *pRsp) {
  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               lino = 0;
  SStreamTriggerTask   *pTask = pContext->pTask;
  SSTriggerCalcRequest *pReq = NULL;
  SStreamRunnerTarget  *pRunner = NULL;

  QUERY_CHECK_CONDITION(pRsp->code == TSDB_CODE_SUCCESS, code, lino, _end, TSDB_CODE_INVALID_PARA);

  SMsgSendInfo     *ahandle = pRsp->info.ahandle;
  SSTriggerAHandle *pAhandle = ahandle->param;
  pReq = pAhandle->param;

  code = stTriggerTaskReleaseRequest(pTask, &pReq);
  QUERY_CHECK_CODE(code, lino, _end);

  if (pContext->status == STRIGGER_CONTEXT_ACQUIRE_REQUEST) {
    // continue check if the context is waiting for any available request
    code = stRealtimeContextCheck(pContext);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stHistoryContextInit(SSTriggerHistoryContext *pContext, SStreamTriggerTask *pTask) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSDataBlock *pVirDataBlock = NULL;
  SFilterInfo *pVirDataFilter = NULL;

  pContext->pTask = pTask;
  pContext->sessionId = SSTRIGGER_HISTORY_SESSIONID;
  if (pTask->triggerType == STREAM_TRIGGER_SLIDING) {
    pContext->needTsdbMeta = (pTask->placeHolderBitmap & PLACE_HOLDER_WROWNUM);
  } else if (pTask->isVirtualTable || (pTask->triggerType == STREAM_TRIGGER_SESSION) ||
             (pTask->triggerType == STREAM_TRIGGER_COUNT)) {
    pContext->needTsdbMeta = true;
  }

  pContext->pReaderMap = tSimpleHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  QUERY_CHECK_NULL(pContext->pReaderMap, code, lino, _end, terrno);
  int32_t nReaders = taosArrayGetSize(pTask->readerList);
  for (int32_t i = 0; i < nReaders; i++) {
    SStreamTaskAddr *pReader = TARRAY_GET_ELEM(pTask->readerList, i);
    code = tSimpleHashPut(pContext->pReaderMap, &pReader->nodeId, sizeof(int32_t), &pReader, POINTER_BYTES);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  pContext->curRange.skey = pTask->fillHistoryStartTime;

  pContext->pGroups = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  QUERY_CHECK_NULL(pContext->pGroups, code, lino, _end, terrno);
  tSimpleHashSetFreeFp(pContext->pGroups, stHistoryGroupDestroy);
  TD_DLIST_INIT(&pContext->groupsToCheck);
  int32_t iter = 0;
  void   *px = tSimpleHashIterate(pTask->pHistoryCutoffTime, NULL, &iter);
  while (px != NULL) {
    int64_t                gid = *(int64_t *)tSimpleHashGetKey(px, NULL);
    SSTriggerHistoryGroup *pGroup = taosMemoryCalloc(1, sizeof(SSTriggerHistoryGroup));
    QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
    code = tSimpleHashPut(pContext->pGroups, &gid, sizeof(int64_t), &pGroup, POINTER_BYTES);
    if (code != TSDB_CODE_SUCCESS) {
      taosMemoryFreeClear(pGroup);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    code = stHistoryGroupInit(pGroup, pContext, gid);
    QUERY_CHECK_CODE(code, lino, _end);
    int64_t ts = *(int64_t *)px;
    pContext->curRange.ekey = TMAX(pContext->curRange.ekey, ts);
    px = tSimpleHashIterate(pTask->pHistoryCutoffTime, px, &iter);
  }

  pContext->pSorter = taosMemoryCalloc(1, sizeof(SSTriggerTimestampSorter));
  QUERY_CHECK_NULL(pContext->pSorter, code, lino, _end, terrno);
  code = stTimestampSorterInit(pContext->pSorter, pTask);
  QUERY_CHECK_CODE(code, lino, _end);
  if (pTask->isVirtualTable) {
    code = createOneDataBlock(pTask->pVirDataBlock, false, &pVirDataBlock);
    QUERY_CHECK_CODE(code, lino, _end);
    code = filterInitFromNode(pTask->triggerFilter, &pVirDataFilter, 0, NULL);
    QUERY_CHECK_CODE(code, lino, _end);
    pContext->pMerger = taosMemoryCalloc(1, sizeof(SSTriggerVtableMerger));
    QUERY_CHECK_NULL(pContext->pMerger, code, lino, _end, terrno);
    code = stVtableMergerInit(pContext->pMerger, pTask, &pVirDataBlock, &pVirDataFilter);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (pTask->triggerType == STREAM_TRIGGER_SLIDING || pTask->triggerType == STREAM_TRIGGER_SESSION) {
    pContext->pSavedWindows = taosArrayInit(0, sizeof(SSTriggerWindow));
    QUERY_CHECK_NULL(pContext->pSavedWindows, code, lino, _end, terrno);
    pContext->pInitWindows = taosArrayInit(0, sizeof(STimeWindow));
    QUERY_CHECK_NULL(pContext->pInitWindows, code, lino, _end, terrno);
  } else if (pTask->triggerType == STREAM_TRIGGER_EVENT) {
    code = filterInitFromNode(pTask->pStartCond, &pContext->pStartCond, 0, NULL);
    QUERY_CHECK_CODE(code, lino, _end);
    code = filterInitFromNode(pTask->pEndCond, &pContext->pEndCond, 0, NULL);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (pTask->notifyEventType != STRIGGER_EVENT_WINDOW_NONE) {
    pContext->pNotifyParams = taosArrayInit(0, sizeof(SSTriggerCalcParam));
    QUERY_CHECK_NULL(pContext->pNotifyParams, code, lino, _end, terrno);
  }
  SSTriggerPullRequest *pPullReq = &pContext->pullReq.base;
  pPullReq->streamId = pTask->task.streamId;
  pPullReq->sessionId = pContext->sessionId;
  pPullReq->triggerTaskId = pTask->task.taskId;
  if (pTask->isVirtualTable) {
    pContext->reqCids = taosArrayInit(0, sizeof(col_id_t));
    QUERY_CHECK_NULL(pContext->reqCids, code, lino, _end, terrno);
  }

  pContext->pCalcDataCacheIters =
      taosHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  QUERY_CHECK_NULL(pContext->pCalcDataCacheIters, code, lino, _end, errno);

_end:
  return code;
}

static void stHistoryContextDestroy(void *ptr) {
  SSTriggerHistoryContext **ppContext = ptr;
  if (ppContext == NULL || *ppContext == NULL) {
    return;
  }

  SSTriggerHistoryContext *pContext = *ppContext;
  if (pContext->pReaderMap != NULL) {
    tSimpleHashCleanup(pContext->pReaderMap);
    pContext->pReaderMap = NULL;
  }

  if (pContext->pFirstTsMap != NULL) {
    tSimpleHashCleanup(pContext->pFirstTsMap);
    pContext->pFirstTsMap = NULL;
  }

  if (pContext->pGroups != NULL) {
    tSimpleHashCleanup(pContext->pGroups);
    pContext->pGroups = NULL;
  }

  if (pContext->pSorter != NULL) {
    stTimestampSorterDestroy(&pContext->pSorter);
  }
  if (pContext->pMerger != NULL) {
    stVtableMergerDestroy(&pContext->pMerger);
  }

  if (pContext->pSavedWindows != NULL) {
    taosArrayDestroy(pContext->pSavedWindows);
    pContext->pSavedWindows = NULL;
  }
  if (pContext->pInitWindows != NULL) {
    taosArrayDestroy(pContext->pInitWindows);
    pContext->pInitWindows = NULL;
  }
  if (pContext->pStartCond != NULL) {
    filterFreeInfo(pContext->pStartCond);
    pContext->pStartCond = NULL;
  }
  if (pContext->pEndCond != NULL) {
    filterFreeInfo(pContext->pEndCond);
    pContext->pEndCond = NULL;
  }

  if (pContext->pNotifyParams != NULL) {
    taosArrayDestroyEx(pContext->pNotifyParams, tDestroySSTriggerCalcParam);
    pContext->pNotifyParams = NULL;
  }

  if (pContext->pCalcDataCache != NULL) {
    destroyStreamDataCache(pContext->pCalcDataCache);
    pContext->pCalcDataCache = NULL;
  }
  if (pContext->pCalcDataCacheIters != NULL) {
    taosHashCleanup(pContext->pCalcDataCacheIters);
    pContext->pCalcDataCacheIters = NULL;
  }

  taosMemFreeClear(*ppContext);
}

static FORCE_INLINE SSTriggerHistoryGroup *stHistoryContextGetCurrentGroup(SSTriggerHistoryContext *pContext) {
  if (TD_DLIST_NELES(&pContext->groupsToCheck) > 0) {
    return TD_DLIST_HEAD(&pContext->groupsToCheck);
  } else {
    terrno = TSDB_CODE_INTERNAL_ERROR;
    SStreamTriggerTask *pTask = pContext->pTask;
    ST_TASK_ELOG("failed to get the group in history context %" PRId64, pContext->sessionId);
    return NULL;
  }
}

static int32_t stHistoryContextSendPullReq(SSTriggerHistoryContext *pContext, ESTriggerPullType type) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pContext->pTask;
  SStreamTaskAddr    *pReader = NULL;
  SRpcMsg             msg = {.msgType = TDMT_STREAM_TRIGGER_PULL};

  switch (type) {
    case STRIGGER_PULL_FIRST_TS: {
      pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
      QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
      SSTriggerFirstTsRequest *pReq = &pContext->pullReq.firstTsReq;
      pReq->startTime = pContext->startTime;
      break;
    }

    case STRIGGER_PULL_TSDB_META:
    case STRIGGER_PULL_TSDB_META_NEXT: {
      pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
      QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
      // todo(kjq): add endTime in metadata request
      SSTriggerTsdbMetaRequest *pReq = &pContext->pullReq.tsdbMetaReq;
      pReq->startTime = pContext->curRange.skey;
      if (tSimpleHashGetSize(pContext->pGroups) == 1) {
        int32_t iter = 0;
        void   *px = tSimpleHashIterate(pContext->pGroups, NULL, &iter);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
        SSTriggerHistoryGroup *pGroup = *(SSTriggerHistoryGroup **)px;
        pReq->gid = pGroup->gid;
      } else {
        pReq->gid = 0;
      }
      pReq->order = 1;
      break;
    }

    case STRIGGER_PULL_TSDB_TS_DATA: {
      SSTriggerTsdbTsDataRequest *pReq = &pContext->pullReq.tsdbTsDataReq;
      SSTriggerHistoryGroup      *pGroup = stHistoryContextGetCurrentGroup(pContext);
      QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
      SSTriggerTableMeta *pCurTableMeta = pGroup->pCurTableMeta;
      SSTriggerMetaData  *pMetaToFetch = pContext->pMetaToFetch;
      pReq->suid = 0;
      pReq->uid = pCurTableMeta->tbUid;
      pReq->skey = pMetaToFetch->skey;
      pReq->ekey = pMetaToFetch->ekey;
      void *px = tSimpleHashGet(pContext->pReaderMap, &pCurTableMeta->vgId, sizeof(int32_t));
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      pReader = *(SStreamTaskAddr **)px;
      break;
    }

    case STRIGGER_PULL_TSDB_TRIGGER_DATA:
    case STRIGGER_PULL_TSDB_TRIGGER_DATA_NEXT: {
      pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
      QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
      SSTriggerTsdbTriggerDataRequest *pReq = &pContext->pullReq.tsdbTriggerDataReq;
      pReq->startTime = pContext->startTime;
      if (tSimpleHashGetSize(pContext->pGroups) == 1) {
        int32_t iter = 0;
        void   *px = tSimpleHashIterate(pContext->pGroups, NULL, &iter);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
        SSTriggerHistoryGroup *pGroup = *(SSTriggerHistoryGroup **)px;
        pReq->gid = pGroup->gid;
      } else {
        pReq->gid = 0;
      }
      pReq->order = 1;
      break;
    }

    case STRIGGER_PULL_TSDB_CALC_DATA:
    case STRIGGER_PULL_TSDB_CALC_DATA_NEXT: {
      pReader = taosArrayGet(pTask->readerList, pContext->curCalcReaderIdx);
      QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
      SSTriggerTsdbCalcDataRequest *pReq = &pContext->pullReq.tsdbCalcDataReq;
      SSTriggerHistoryGroup        *pGroup = stHistoryContextGetCurrentGroup(pContext);
      QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
      pReq->gid = pGroup->gid;
      pReq->skey = pContext->pParamToFetch->wstart;
      pReq->ekey = pContext->pParamToFetch->wend;
      break;
    }

    case STRIGGER_PULL_TSDB_DATA: {
      SSTriggerTsdbDataRequest *pReq = &pContext->pullReq.tsdbDataReq;
      SSTriggerHistoryGroup    *pGroup = stHistoryContextGetCurrentGroup(pContext);
      QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
      SSTriggerTableColRef *pColRefToFetch = pContext->pColRefToFetch;
      SSTriggerMetaData    *pMetaToFetch = pContext->pMetaToFetch;
      pReq->suid = pColRefToFetch->otbSuid;
      pReq->uid = pColRefToFetch->otbUid;
      pReq->skey = pMetaToFetch->skey;
      pReq->ekey = pMetaToFetch->ekey;
      pReq->cids = pContext->reqCids;
      taosArrayClear(pReq->cids);
      *(col_id_t *)TARRAY_DATA(pReq->cids) = PRIMARYKEY_TIMESTAMP_COL_ID;
      TARRAY_SIZE(pReq->cids) = 1;
      int32_t nCols = taosArrayGetSize(pColRefToFetch->pColMatches);
      for (int32_t i = 0; i < nCols; i++) {
        SSTriggerColMatch *pColMatch = TARRAY_GET_ELEM(pColRefToFetch->pColMatches, i);
        void              *px = taosArrayPush(pReq->cids, &pColMatch->otbColId);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      }
      pReq->order = 1;
      void *px = tSimpleHashGet(pContext->pReaderMap, &pColRefToFetch->otbVgId, sizeof(int32_t));
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      pReader = *(SStreamTaskAddr **)px;
      break;
    }

    case STRIGGER_PULL_GROUP_COL_VALUE: {
      SSTriggerGroupColValueRequest *pReq = &pContext->pullReq.groupColValueReq;
      SSTriggerHistoryGroup         *pGroup = stHistoryContextGetCurrentGroup(pContext);
      QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
      pReq->gid = pGroup->gid;
      int32_t vgId = 0;
      if (pTask->isVirtualTable) {
        SSTriggerVirTableInfo *pTable = TARRAY_DATA(pGroup->pVirTableInfos);
        QUERY_CHECK_NULL(pTable, code, lino, _end, terrno);
        vgId = pTable->vgId;
      } else {
        int32_t             iter = 0;
        SSTriggerTableMeta *pTable = tSimpleHashIterate(pGroup->pTableMetas, NULL, &iter);
        QUERY_CHECK_NULL(pTable, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        vgId = pTable->vgId;
      }
      void *px = tSimpleHashGet(pContext->pReaderMap, &vgId, sizeof(int32_t));
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      pReader = *(SStreamTaskAddr **)px;
      break;
    }

    default: {
      ST_TASK_ELOG("invalid pull request type %d at %s", type, __func__);
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  SSTriggerPullRequest *pReq = &pContext->pullReq.base;
  pReq->type = type;
  pReq->readerTaskId = pReader->taskId;

  // serialize and send request
  QUERY_CHECK_CODE(stTriggerTaskAllocAhandle(pTask, pContext->sessionId, pReq, &msg.info.ahandle), lino, _end);
  msg.contLen = tSerializeSTriggerPullRequest(NULL, 0, pReq);
  QUERY_CHECK_CONDITION(msg.contLen > 0, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  msg.contLen += sizeof(SMsgHead);
  msg.pCont = rpcMallocCont(msg.contLen);
  QUERY_CHECK_NULL(msg.pCont, code, lino, _end, terrno);
  SMsgHead *pMsgHead = (SMsgHead *)msg.pCont;
  pMsgHead->contLen = htonl(msg.contLen);
  pMsgHead->vgId = htonl(pReader->nodeId);
  int32_t tlen =
      tSerializeSTriggerPullRequest((char *)msg.pCont + sizeof(SMsgHead), msg.contLen - sizeof(SMsgHead), pReq);
  QUERY_CHECK_CONDITION(tlen == msg.contLen - sizeof(SMsgHead), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  code = tmsgSendReq(&pReader->epset, &msg);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    destroyAhandle(msg.info.ahandle);
    ST_TASK_ELOG("%s failed at line %d since %s, type: %d", __func__, lino, tstrerror(code), type);
  }
  return code;
}

static int32_t stHistoryContextSendCalcReq(SSTriggerHistoryContext *pContext) {
  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               lino = 0;
  SStreamTriggerTask   *pTask = pContext->pTask;
  SSTriggerCalcRequest *pCalcReq = pContext->pCalcReq;
  SStreamRunnerTarget  *pCalcRunner = NULL;
  bool                  needTagValue = false;
  SRpcMsg               msg = {.msgType = TDMT_STREAM_TRIGGER_CALC};

  QUERY_CHECK_NULL(pCalcReq, code, lino, _end, TSDB_CODE_INVALID_PARA);

  int32_t nRunners = taosArrayGetSize(pTask->runnerList);
  for (int32_t i = 0; i < taosArrayGetSize(pTask->runnerList); i++) {
    pCalcRunner = TARRAY_GET_ELEM(pTask->runnerList, i);
    if (pCalcRunner->addr.taskId == pCalcReq->runnerTaskId) {
      break;
    }
    pCalcRunner = NULL;
  }
  QUERY_CHECK_NULL(pCalcRunner, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

  if (pCalcReq->createTable && pTask->hasPartitionBy || (pTask->placeHolderBitmap & PLACE_HOLDER_PARTITION_IDX) ||
      (pTask->placeHolderBitmap & PLACE_HOLDER_PARTITION_TBNAME)) {
    needTagValue = true;
  }

  if (needTagValue && taosArrayGetSize(pCalcReq->groupColVals) == 0) {
    code = stHistoryContextSendPullReq(pContext, STRIGGER_PULL_GROUP_COL_VALUE);
    QUERY_CHECK_CODE(code, lino, _end);
    goto _end;
  }

  if (pTask->placeHolderBitmap & PLACE_HOLDER_PARTITION_ROWS) {
    // create data cache handle
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

    SSTriggerHistoryGroup *pGroup = stHistoryContextGetCurrentGroup(pContext);
    QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
    if (pContext->pParamToFetch == NULL) {
      pContext->pParamToFetch = TARRAY_DATA(pCalcReq->params);
      pContext->curCalcReaderIdx = 0;
    }

    while (TARRAY_ELEM_IDX(pCalcReq->params, pContext->pParamToFetch) < TARRAY_SIZE(pCalcReq->params)) {
      bool allTableProcessed = false;
      bool needFetchData = false;
      while (!allTableProcessed && !needFetchData) {
        SSDataBlock *pDataBlock = NULL;
        int32_t      startIdx = 0;
        int32_t      endIdx = 0;
        code = stHistoryGroupGetDataBlock(pGroup, false, &pDataBlock, &startIdx, &endIdx, &allTableProcessed,
                                          &needFetchData);
        QUERY_CHECK_CODE(code, lino, _end);

        if (allTableProcessed || needFetchData) {
          break;
        }
        int32_t nrows = blockDataGetNumOfRows(pDataBlock);
        code = putStreamDataCache(pContext->pCalcDataCache, pGroup->gid, pContext->pParamToFetch->wstart,
                                  pContext->pParamToFetch->wend, pDataBlock, 0, nrows - 1);
        QUERY_CHECK_CODE(code, lino, _end);
      }

      if (needFetchData) {
        if (pContext->pColRefToFetch != NULL) {
          code = stHistoryContextSendPullReq(pContext, STRIGGER_PULL_TSDB_DATA);
          QUERY_CHECK_CODE(code, lino, _end);
          goto _end;
        } else {
          QUERY_CHECK_CONDITION(pContext->pMetaToFetch == NULL, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
          code = stHistoryContextSendPullReq(pContext, STRIGGER_PULL_TSDB_CALC_DATA);
          QUERY_CHECK_CODE(code, lino, _end);
          goto _end;
        }
      }
      pContext->pParamToFetch++;
      pContext->curCalcReaderIdx = 0;
      pGroup->tbIter = 0;
    }
  }

  // amend ekey of interval window trigger and sliding trigger
  for (int32_t i = 0; i < TARRAY_SIZE(pCalcReq->params); ++i) {
    SSTriggerCalcParam *pParam = taosArrayGet(pCalcReq->params, i);
    if (pTask->triggerType == STREAM_TRIGGER_SLIDING) {
      pParam->wend++;
      pParam->wduration++;
    }
    ST_TASK_ILOG("[calc param %d]: gid=%" PRId64 ", wstart=%" PRId64 ", wend=%" PRId64 ", nrows=%" PRId64
                 ", prevTs=%" PRId64 ", currentTs=%" PRId64 ", nextTs=%" PRId64 ", prevLocalTime=%" PRId64
                 ", nextLocalTime=%" PRId64 ", localTime=%" PRId64 ", create=%d",
                 i, pCalcReq->gid, pParam->wstart, pParam->wend, pParam->wrownum, pParam->prevTs, pParam->currentTs,
                 pParam->nextTs, pParam->prevLocalTime, pParam->nextLocalTime, pParam->triggerTime,
                 pCalcReq->createTable);
  }

  // serialize and send request
  QUERY_CHECK_CODE(stTriggerTaskAllocAhandle(pTask, pContext->sessionId, pCalcReq, &msg.info.ahandle), lino, _end);
  msg.contLen = tSerializeSTriggerCalcRequest(NULL, 0, pCalcReq);
  QUERY_CHECK_CONDITION(msg.contLen > 0, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  msg.contLen += sizeof(SMsgHead);
  msg.pCont = rpcMallocCont(msg.contLen);
  QUERY_CHECK_NULL(msg.pCont, code, lino, _end, terrno);
  SMsgHead *pMsgHead = (SMsgHead *)msg.pCont;
  pMsgHead->contLen = htonl(msg.contLen);
  pMsgHead->vgId = htonl(SNODE_HANDLE);
  int32_t tlen = tSerializeSTriggerCalcRequest((char*)msg.pCont + sizeof(SMsgHead), msg.contLen - sizeof(SMsgHead), pCalcReq);
  QUERY_CHECK_CONDITION(tlen == msg.contLen - sizeof(SMsgHead), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  code = tmsgSendReq(&pCalcRunner->addr.epset, &msg);
  QUERY_CHECK_CODE(code, lino, _end);

  ST_TASK_DLOG("calc request is sent to node:%d task:%" PRIx64, pCalcRunner->addr.nodeId, pCalcRunner->addr.taskId);

  pContext->pCalcReq = NULL;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    destroyAhandle(msg.info.ahandle);
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stHistoryContextCheck(SSTriggerHistoryContext *pContext) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pContext->pTask;

  if (pContext->status == STRIGGER_CONTEXT_IDLE) {
    pContext->status = STRIGGER_CONTEXT_FETCH_META;
    if (pTask->triggerType == STREAM_TRIGGER_SLIDING && pContext->pFirstTsMap == NULL) {
      pContext->pFirstTsMap = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
      QUERY_CHECK_NULL(pContext->pFirstTsMap, code, lino, _end, terrno);
      code = stHistoryContextSendPullReq(pContext, STRIGGER_PULL_FIRST_TS);
      QUERY_CHECK_CODE(code, lino, _end);
      goto _end;
    } else if (pContext->needTsdbMeta) {
      pContext->curReaderIdx = 0;
      code = stHistoryContextSendPullReq(pContext, STRIGGER_PULL_TSDB_META);
      QUERY_CHECK_CODE(code, lino, _end);
      goto _end;
    } else if (pTask->triggerType != STREAM_TRIGGER_SLIDING) {
      pContext->curReaderIdx = 0;
      code = stHistoryContextSendPullReq(pContext, STRIGGER_PULL_TSDB_TRIGGER_DATA);
      QUERY_CHECK_CODE(code, lino, _end);
      goto _end;
    }
  }

  while (TD_DLIST_NELES(&pContext->groupsToCheck) > 0) {
    SSTriggerHistoryGroup *pGroup = TD_DLIST_HEAD(&pContext->groupsToCheck);
    switch (pContext->status) {
      case STRIGGER_CONTEXT_FETCH_META: {
        pContext->status = STRIGGER_CONTEXT_ACQUIRE_REQUEST;
      }
      case STRIGGER_CONTEXT_ACQUIRE_REQUEST: {
        if (pContext->pCalcReq == NULL && pTask->calcEventType != STRIGGER_EVENT_WINDOW_NONE) {
          code = stTriggerTaskAcquireRequest(pTask, pContext->sessionId, pGroup->gid, &pContext->pCalcReq);
          QUERY_CHECK_CODE(code, lino, _end);
          if (pContext->pCalcReq == NULL) {
            ST_TASK_DLOG("no available runner for group %" PRId64, pGroup->gid);
            goto _end;
          }
        }
        pContext->status = STRIGGER_CONTEXT_CHECK_CONDITION;
      }
      case STRIGGER_CONTEXT_CHECK_CONDITION: {
        if (taosArrayGetSize(pGroup->pPendingCalcReqs) > 0) {
          QUERY_CHECK_NULL(pContext->pCalcReq, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
          void *px = taosArrayAddAll(pContext->pCalcReq->params, pGroup->pPendingCalcReqs);
          QUERY_CHECK_NULL(px, code, lino, _end, terrno);
          TARRAY_SIZE(pGroup->pPendingCalcReqs) = 0;
        }
        code = stHistoryGroupCheck(pGroup);
        QUERY_CHECK_CODE(code, lino, _end);
        if (pContext->pColRefToFetch != NULL) {
          code = stHistoryContextSendPullReq(pContext, STRIGGER_PULL_TSDB_DATA);
          QUERY_CHECK_CODE(code, lino, _end);
          goto _end;
        } else if (pContext->pMetaToFetch != NULL) {
          code = stHistoryContextSendPullReq(pContext, STRIGGER_PULL_TSDB_TS_DATA);
          QUERY_CHECK_CODE(code, lino, _end);
          goto _end;
        }

        if (taosArrayGetSize(pContext->pNotifyParams) > 0) {
          code = streamSendNotifyContent(&pTask->task, pTask->streamName, pTask->triggerType, pGroup->gid,
                                         pTask->pNotifyAddrUrls, pTask->notifyErrorHandle,
                                         TARRAY_DATA(pContext->pNotifyParams), TARRAY_SIZE(pContext->pNotifyParams));
          QUERY_CHECK_CODE(code, lino, _end);
        }
        stHistoryGroupClearTempState(pGroup);
        pContext->status = STRIGGER_CONTEXT_SEND_CALC_REQ;
      }
      case STRIGGER_CONTEXT_SEND_CALC_REQ: {
        if (pContext->pCalcReq == NULL) {
          // do nothing
        } else if (taosArrayGetSize(pContext->pCalcReq->params) > 0) {
          if (TARRAY_SIZE(pContext->pCalcReq->params) > STREAM_CALC_REQ_MAX_WIN_NUM) {
            SSTriggerCalcParam *p = TARRAY_GET_ELEM(pContext->pCalcReq->params, STREAM_CALC_REQ_MAX_WIN_NUM);
            int32_t             nPending = TARRAY_SIZE(pContext->pCalcReq->params) - STREAM_CALC_REQ_MAX_WIN_NUM;
            void               *px = taosArrayAddBatch(pGroup->pPendingCalcReqs, p, nPending);
            QUERY_CHECK_NULL(px, code, lino, _end, terrno);
            TARRAY_SIZE(pContext->pCalcReq->params) = STREAM_CALC_REQ_MAX_WIN_NUM;
          }
          pContext->status = STRIGGER_CONTEXT_SEND_CALC_REQ;
          code = stHistoryContextSendCalcReq(pContext);
          QUERY_CHECK_CODE(code, lino, _end);
          if (pContext->pCalcReq != NULL) {
            // calc req has not been sent
            goto _end;
          }
          stHistoryGroupClearTempState(pGroup);
        } else {
          code = stTriggerTaskReleaseRequest(pTask, &pContext->pCalcReq);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        break;
      }
      default: {
        ST_TASK_ELOG("invalid context status %d at %s:%d", pContext->status, __func__, __LINE__);
        code = TSDB_CODE_INVALID_PARA;
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
    TD_DLIST_POP(&pContext->groupsToCheck, pGroup);
    if (taosArrayGetSize(pGroup->pPendingCalcReqs) > 0) {
      // todo(kjq): implement batch window mode
      TD_DLIST_APPEND(&pContext->groupsToCheck, pGroup);
    }
    pContext->status = STRIGGER_CONTEXT_ACQUIRE_REQUEST;
  }

  pContext->status = STRIGGER_CONTEXT_FETCH_META;
  if (pContext->needTsdbMeta) {
    // todo(kjq): pull tsdb meta of new range
  } else if (pTask->triggerType != STREAM_TRIGGER_SLIDING) {
    int32_t nrows = blockDataGetNumOfRows(pContext->pullRes[STRIGGER_PULL_TSDB_TRIGGER_DATA]);
    if (nrows >= STREAM_RETURN_ROWS_NUM) {
      code = stHistoryContextSendPullReq(pContext, STRIGGER_PULL_TSDB_TRIGGER_DATA_NEXT);
      QUERY_CHECK_CODE(code, lino, _end);
    } else if (pContext->curReaderIdx != taosArrayGetSize(pTask->readerList) - 1) {
      pContext->curReaderIdx++;
      code = stHistoryContextSendPullReq(pContext, STRIGGER_PULL_TSDB_TRIGGER_DATA);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stHistoryContextProcPullRsp(SSTriggerHistoryContext *pContext, SRpcMsg *pRsp) {
  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               lino = 0;
  SStreamTriggerTask   *pTask = pContext->pTask;
  SSTriggerPullRequest *pReq = NULL;
  SSDataBlock          *pTempDataBlock = NULL;

  QUERY_CHECK_CONDITION(pRsp->code == TSDB_CODE_SUCCESS || pRsp->code == TSDB_CODE_STREAM_NO_DATA, code, lino, _end,
                        TSDB_CODE_INVALID_PARA);

  SMsgSendInfo     *ahandle = pRsp->info.ahandle;
  SSTriggerAHandle *pAhandle = ahandle->param;
  pReq = pAhandle->param;
  switch (pReq->type) {
    case STRIGGER_PULL_FIRST_TS: {
      QUERY_CHECK_CONDITION(pContext->status == STRIGGER_CONTEXT_FETCH_META, code, lino, _end,
                            TSDB_CODE_INTERNAL_ERROR);
      SSDataBlock *pDataBlock = pContext->pullRes[pReq->type];
      if (pDataBlock == NULL) {
        pDataBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
        QUERY_CHECK_NULL(pDataBlock, code, lino, _end, terrno);
        pContext->pullRes[pReq->type] = pDataBlock;
      }
      code = tDeserializeSStreamTsResponse(pRsp->pCont, pRsp->contLen, pDataBlock);
      QUERY_CHECK_CODE(code, lino, _end);

      SStreamTaskAddr *pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
      QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
      int32_t nrows = blockDataGetNumOfRows(pDataBlock);
      if (nrows > 0) {
        SColumnInfoData *pGidCol = taosArrayGet(pDataBlock->pDataBlock, 0);
        QUERY_CHECK_NULL(pGidCol, code, lino, _end, terrno);
        int64_t         *pGidData = (int64_t *)pGidCol->pData;
        SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, 1);
        QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
        int64_t *pTsData = (int64_t *)pTsCol->pData;
        for (int32_t i = 0; i < nrows; i++) {
          void *px = tSimpleHashGet(pContext->pGroups, &pGidData[i], sizeof(int64_t));
          if (px == NULL) {
            continue;
          }
          SSTriggerHistoryGroup *pGroup = *(SSTriggerHistoryGroup **)px;
          if (tSimpleHashGetSize(pGroup->pTableMetas) == 0) {
            SSTriggerTableMeta newTableMeta = {.vgId = pReader->nodeId};
            code = tSimpleHashPut(pGroup->pTableMetas, &newTableMeta.tbUid, sizeof(int64_t), &newTableMeta,
                                  sizeof(SSTriggerTableMeta));
            QUERY_CHECK_CODE(code, lino, _end);
          }
          px = tSimpleHashGet(pContext->pFirstTsMap, &pGidData[i], sizeof(int64_t));
          if (px == NULL) {
            code = tSimpleHashPut(pContext->pFirstTsMap, &pGidData[i], sizeof(int64_t), &pTsData[i], sizeof(int64_t));
            QUERY_CHECK_CODE(code, lino, _end);
          } else {
            *(int64_t *)px = TMIN(*(int64_t *)px, pTsData[i]);
          }
          if (!pContext->needTsdbMeta && TD_DLIST_NODE_NEXT(pGroup) == NULL &&
              TD_DLIST_TAIL(&pContext->groupsToCheck) != pGroup) {
            TD_DLIST_APPEND(&pContext->groupsToCheck, pGroup);
          }
        }
      }

      if (pContext->curReaderIdx != taosArrayGetSize(pTask->readerList) - 1) {
        pContext->curReaderIdx++;
        code = stHistoryContextSendPullReq(pContext, STRIGGER_PULL_FIRST_TS);
        QUERY_CHECK_CODE(code, lino, _end);
      } else {
        pContext->status = STRIGGER_CONTEXT_IDLE;
        code = stHistoryContextCheck(pContext);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      break;
    }

    case STRIGGER_PULL_TSDB_META:
    case STRIGGER_PULL_TSDB_META_NEXT: {
      QUERY_CHECK_CONDITION(pContext->status == STRIGGER_CONTEXT_FETCH_META, code, lino, _end,
                            TSDB_CODE_INTERNAL_ERROR);
      SSDataBlock *pDataBlock = pContext->pullRes[STRIGGER_PULL_TSDB_META];
      if (pDataBlock == NULL) {
        pDataBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
        QUERY_CHECK_NULL(pDataBlock, code, lino, _end, terrno);
        pContext->pullRes[STRIGGER_PULL_TSDB_META] = pDataBlock;
      }
      if (pRsp->contLen > 0) {
        const char *pCont = pRsp->pCont;
        code = blockDecode(pDataBlock, pCont, &pCont);
        QUERY_CHECK_CODE(code, lino, _end);
        QUERY_CHECK_CONDITION(pCont == (char *)pRsp->pCont + pRsp->contLen, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      } else {
        blockDataEmpty(pDataBlock);
      }
      int32_t nrows = blockDataGetNumOfRows(pDataBlock);
      if (nrows > 0) {
        // find groups to be checked
        if (!pTask->isVirtualTable) {
          SColumnInfoData *pGidCol = taosArrayGet(pDataBlock->pDataBlock, 3);
          QUERY_CHECK_NULL(pGidCol, code, lino, _end, terrno);
          int64_t *pGidData = (int64_t *)pGidCol->pData;
          for (int32_t i = 0; i < nrows; i++) {
            void *px = tSimpleHashGet(pContext->pGroups, &pGidData[i], sizeof(int64_t));
            if (px == NULL) {
              continue;
            }
            SSTriggerHistoryGroup *pGroup = *(SSTriggerHistoryGroup **)px;
            if (TD_DLIST_NODE_NEXT(pGroup) == NULL && TD_DLIST_TAIL(&pContext->groupsToCheck) != pGroup) {
              bool added = false;
              code = stHistoryGroupAddMetaDatas(pGroup, pDataBlock, &added);
              QUERY_CHECK_CODE(code, lino, _end);
              QUERY_CHECK_CONDITION(added, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
              TD_DLIST_APPEND(&pContext->groupsToCheck, pGroup);
            }
          }
        } else {
          SColumnInfoData *pUidCol = taosArrayGet(pDataBlock->pDataBlock, 2);
          QUERY_CHECK_NULL(pUidCol, code, lino, _end, terrno);
          int64_t *pUidData = (int64_t *)pUidCol->pData;
          int32_t  iter = 0;
          void    *px = tSimpleHashIterate(pContext->pGroups, NULL, &iter);
          while (px != NULL) {
            SSTriggerHistoryGroup *pGroup = *(SSTriggerHistoryGroup **)px;
            bool                   added = false;
            code = stHistoryGroupAddMetaDatas(pGroup, pDataBlock, &added);
            QUERY_CHECK_CODE(code, lino, _end);
            if (added) {
              TD_DLIST_APPEND(&pContext->groupsToCheck, pGroup);
            }
            px = tSimpleHashIterate(pContext->pGroups, px, &iter);
          }
        }
      }
      if (nrows >= STREAM_RETURN_ROWS_NUM) {
        code = stHistoryContextSendPullReq(pContext, STRIGGER_PULL_TSDB_META_NEXT);
        QUERY_CHECK_CODE(code, lino, _end);
      } else if (pContext->curReaderIdx != taosArrayGetSize(pTask->readerList) - 1) {
        pContext->curReaderIdx++;
        code = stHistoryContextSendPullReq(pContext, STRIGGER_PULL_TSDB_META);
        QUERY_CHECK_CODE(code, lino, _end);
      } else {
        // force to close all windows
        code = stHistoryContextCheck(pContext);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      break;
    }

    case STRIGGER_PULL_TSDB_TRIGGER_DATA:
    case STRIGGER_PULL_TSDB_TRIGGER_DATA_NEXT: {
      QUERY_CHECK_CONDITION(pContext->status == STRIGGER_CONTEXT_FETCH_META, code, lino, _end,
                            TSDB_CODE_INTERNAL_ERROR);
      SSDataBlock *pDataBlock = pContext->pullRes[STRIGGER_PULL_TSDB_TRIGGER_DATA];
      if (pDataBlock == NULL) {
        pDataBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
        QUERY_CHECK_NULL(pDataBlock, code, lino, _end, terrno);
        pContext->pullRes[STRIGGER_PULL_TSDB_TRIGGER_DATA] = pDataBlock;
      }
      if (pRsp->contLen > 0) {
        const char *pCont = pRsp->pCont;
        code = blockDecode(pDataBlock, pCont, &pCont);
        QUERY_CHECK_CODE(code, lino, _end);
        QUERY_CHECK_CONDITION(pCont == (char *)pRsp->pCont + pRsp->contLen, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      } else {
        blockDataEmpty(pDataBlock);
      }
      int32_t nrows = blockDataGetNumOfRows(pDataBlock);
      if (nrows > 0) {
        int64_t gid = pDataBlock->info.id.groupId;
        void   *px = tSimpleHashGet(pContext->pGroups, &gid, sizeof(int64_t));
        if (px != NULL) {
          SSTriggerHistoryGroup *pGroup = *(SSTriggerHistoryGroup **)px;
          TD_DLIST_APPEND(&pContext->groupsToCheck, pGroup);
        }
      }
      code = stHistoryContextCheck(pContext);
      QUERY_CHECK_CODE(code, lino, _end);
      break;
    }

    case STRIGGER_PULL_TSDB_TS_DATA: {
      QUERY_CHECK_CONDITION(pContext->status == STRIGGER_CONTEXT_CHECK_CONDITION, code, lino, _end,
                            TSDB_CODE_INTERNAL_ERROR);
      pTempDataBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
      if (pRsp->contLen > 0) {
        const char *pCont = pRsp->pCont;
        code = blockDecode(pTempDataBlock, pCont, &pCont);
        QUERY_CHECK_CODE(code, lino, _end);
        QUERY_CHECK_CONDITION(pCont == (char *)pRsp->pCont + pRsp->contLen, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      } else {
        blockDataEmpty(pTempDataBlock);
      }
      QUERY_CHECK_CONDITION(pContext->pColRefToFetch == NULL, code, lino, _end, TSDB_CODE_INVALID_PARA);
      code = stTimestampSorterBindDataBlock(pContext->pSorter, &pTempDataBlock);
      TSDB_CHECK_CODE(code, lino, _end);
      pContext->pColRefToFetch = NULL;
      pContext->pMetaToFetch = NULL;
      code = stHistoryContextCheck(pContext);
      QUERY_CHECK_CODE(code, lino, _end);
      break;
    }

    case STRIGGER_PULL_TSDB_CALC_DATA:
    case STRIGGER_PULL_TSDB_CALC_DATA_NEXT: {
      QUERY_CHECK_CONDITION(pContext->status == STRIGGER_CONTEXT_SEND_CALC_REQ, code, lino, _end,
                            TSDB_CODE_INTERNAL_ERROR);
      SSDataBlock *pDataBlock = pContext->pullRes[STRIGGER_PULL_TSDB_CALC_DATA];
      if (pDataBlock == NULL) {
        pDataBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
        QUERY_CHECK_NULL(pDataBlock, code, lino, _end, terrno);
        pContext->pullRes[STRIGGER_PULL_TSDB_CALC_DATA] = pDataBlock;
      }
      if (pRsp->contLen > 0) {
        const char *pCont = pRsp->pCont;
        code = blockDecode(pDataBlock, pCont, &pCont);
        QUERY_CHECK_CODE(code, lino, _end);
        QUERY_CHECK_CONDITION(pCont == (char *)pRsp->pCont + pRsp->contLen, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      } else {
        blockDataEmpty(pDataBlock);
      }
      if ((blockDataGetNumOfRows(pDataBlock) == 0) &&
          (pContext->curCalcReaderIdx != taosArrayGetSize(pTask->readerList) - 1)) {
        pContext->curCalcReaderIdx++;
        code = stHistoryContextSendPullReq(pContext, STRIGGER_PULL_TSDB_CALC_DATA);
        QUERY_CHECK_CODE(code, lino, _end);
      } else {
        pContext->pFetchedDataBlock = pDataBlock;
        code = stHistoryContextCheck(pContext);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      break;
    }

    case STRIGGER_PULL_TSDB_DATA: {
      QUERY_CHECK_CONDITION(
          pContext->status == STRIGGER_CONTEXT_CHECK_CONDITION || pContext->status == STRIGGER_CONTEXT_SEND_CALC_REQ,
          code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pTempDataBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
      if (pRsp->contLen > 0) {
        const char *pCont = pRsp->pCont;
        code = blockDecode(pTempDataBlock, pCont, &pCont);
        QUERY_CHECK_CODE(code, lino, _end);
        QUERY_CHECK_CONDITION(pCont == (char *)pRsp->pCont + pRsp->contLen, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      } else {
        blockDataEmpty(pTempDataBlock);
      }
      QUERY_CHECK_NULL(pContext->pColRefToFetch, code, lino, _end, TSDB_CODE_INVALID_PARA);
      code = stVtableMergerBindDataBlock(pContext->pMerger, &pTempDataBlock);
      TSDB_CHECK_CODE(code, lino, _end);
      pContext->pColRefToFetch = NULL;
      pContext->pMetaToFetch = NULL;
      code = stHistoryContextCheck(pContext);
      QUERY_CHECK_CODE(code, lino, _end);
      break;
    }

    case STRIGGER_PULL_GROUP_COL_VALUE: {
      QUERY_CHECK_CONDITION(pContext->status == STRIGGER_CONTEXT_SEND_CALC_REQ, code, lino, _end,
                            TSDB_CODE_INTERNAL_ERROR);
      SStreamGroupInfo groupInfo = {.gInfo = pContext->pCalcReq->groupColVals};
      code = tDeserializeSStreamGroupInfo(pRsp->pCont, pRsp->contLen, &groupInfo);
      QUERY_CHECK_CODE(code, lino, _end);
      code = stHistoryContextCheck(pContext);
      QUERY_CHECK_CODE(code, lino, _end);
      break;
    }

    default: {
      ST_TASK_ELOG("invalid pull request type %d at %s", pReq->type, __func__);
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (pTempDataBlock != NULL) {
    blockDataDestroy(pTempDataBlock);
  }
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s, type: %d", __func__, lino, tstrerror(code), pReq->type);
  }
  return code;
}

static int32_t stHistoryContextProcCalcRsp(SSTriggerHistoryContext *pContext, SRpcMsg *pRsp) {
  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               lino = 0;
  SStreamTriggerTask   *pTask = pContext->pTask;
  SSTriggerCalcRequest *pReq = NULL;
  SStreamRunnerTarget  *pRunner = NULL;

  QUERY_CHECK_CONDITION(pRsp->code == TSDB_CODE_SUCCESS, code, lino, _end, TSDB_CODE_INVALID_PARA);

  SMsgSendInfo     *ahandle = pRsp->info.ahandle;
  SSTriggerAHandle *pAhandle = ahandle->param;
  pReq = pAhandle->param;

  code = stTriggerTaskReleaseRequest(pTask, &pReq);
  QUERY_CHECK_CODE(code, lino, _end);

  if (pContext->status == STRIGGER_CONTEXT_ACQUIRE_REQUEST) {
    // continue check if the context is waiting for any available request
    code = stHistoryContextCheck(pContext);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void stRealtimeGroupDestroyTableMeta(void *ptr) {
  SSTriggerTableMeta *pTableMeta = ptr;
  if (pTableMeta == NULL) {
    return;
  }
  if (pTableMeta->pMetas != NULL) {
    taosArrayDestroy(pTableMeta->pMetas);
    pTableMeta->pMetas = NULL;
  }
}

static int32_t stRealtimeGroupMetaDataCompare(const void *pLeft, const void *pRight) {
  const SSTriggerMetaData *pLeftMeta = (const SSTriggerMetaData *)pLeft;
  const SSTriggerMetaData *pRightMeta = (const SSTriggerMetaData *)pRight;

  if (pLeftMeta->ekey < pRightMeta->ekey) {
    return -1;
  } else if (pLeftMeta->ekey > pRightMeta->ekey) {
    return 1;
  } else if (pLeftMeta->skey < pRightMeta->skey) {
    return -1;
  } else if (pLeftMeta->skey > pRightMeta->skey) {
    return 1;
  }
  return 0;
}

static int32_t stRealtimeGroupMetaDataSearch(const void *pLeft, const void *pRight) {
  int64_t                  ts = *(const int64_t *)pLeft;
  const SSTriggerMetaData *pMeta = (const SSTriggerMetaData *)pRight;
  return ts - pMeta->ekey;
}

static int32_t stRealtimeGroupWindowCompare(const void *pLeft, const void *pRight) {
  const SSTriggerWindow *pLeftWin = (const SSTriggerWindow *)pLeft;
  const SSTriggerWindow *pRightWin = (const SSTriggerWindow *)pRight;

  if (pLeftWin->range.skey < pRightWin->range.skey) {
    return -1;
  } else if (pLeftWin->range.skey > pRightWin->range.skey) {
    return 1;
  } else if (pLeftWin->range.ekey < pRightWin->range.ekey) {
    return -1;
  } else if (pLeftWin->range.ekey > pRightWin->range.ekey) {
    return 1;
  }
  return 0;
}

static int32_t stRealtimeGroupInit(SSTriggerRealtimeGroup *pGroup, SSTriggerRealtimeContext *pContext, int64_t gid) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pContext->pTask;

  pGroup->pContext = pContext;
  pGroup->gid = gid;

  pGroup->pTableMetas = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  QUERY_CHECK_NULL(pGroup->pTableMetas, code, lino, _end, terrno);
  tSimpleHashSetFreeFp(pGroup->pTableMetas, stRealtimeGroupDestroyTableMeta);

  pGroup->oldThreshold = INT64_MIN;
  pGroup->newThreshold = INT64_MIN;

#if !TRIGGER_USE_HISTORY_META
  if ((pTask->fillHistory || pTask->fillHistoryFirst) && pTask->fillHistoryStartTime > 0) {
    pGroup->oldThreshold = pTask->fillHistoryStartTime - 1;
  }
#endif

  TRINGBUF_INIT(&pGroup->winBuf);

  if (pContext->pTask->isVirtualTable) {
    pGroup->pVirTableInfos = taosArrayInit(0, POINTER_BYTES);
    QUERY_CHECK_NULL(pGroup->pVirTableInfos, code, lino, _end, terrno);
    int32_t                iter = 0;
    SSTriggerVirTableInfo *pInfo = tSimpleHashIterate(pTask->pVirTableInfos, NULL, &iter);
    while (pInfo != NULL) {
      if (pInfo->tbGid == gid) {
        void *px = taosArrayPush(pGroup->pVirTableInfos, &pInfo);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
        int32_t nTrigCols = taosArrayGetSize(pInfo->pTrigColRefs);
        for (int32_t i = 0; i < nTrigCols; i++) {
          SSTriggerTableColRef *pColRef = TARRAY_GET_ELEM(pInfo->pTrigColRefs, i);
          SSTriggerTableMeta   *pTableMeta = tSimpleHashGet(pGroup->pTableMetas, &pColRef->otbUid, sizeof(int64_t));
          if (pTableMeta == NULL) {
            SSTriggerTableMeta newTableMeta = {.tbUid = pColRef->otbUid, .vgId = pColRef->otbVgId};
            code = tSimpleHashPut(pGroup->pTableMetas, &pColRef->otbUid, sizeof(int64_t), &newTableMeta,
                                  sizeof(SSTriggerTableMeta));
            QUERY_CHECK_CODE(code, lino, _end);
          }
        }
        int32_t nCalcCols = taosArrayGetSize(pInfo->pCalcColRefs);
        for (int32_t i = 0; i < nCalcCols; i++) {
          SSTriggerTableColRef *pColRef = TARRAY_GET_ELEM(pInfo->pCalcColRefs, i);
          SSTriggerTableMeta   *pTableMeta = tSimpleHashGet(pGroup->pTableMetas, &pColRef->otbUid, sizeof(int64_t));
          if (pTableMeta == NULL) {
            SSTriggerTableMeta newTableMeta = {.tbUid = pColRef->otbUid, .vgId = pColRef->otbVgId};
            code = tSimpleHashPut(pGroup->pTableMetas, &pColRef->otbUid, sizeof(int64_t), &newTableMeta,
                                  sizeof(SSTriggerTableMeta));
            QUERY_CHECK_CODE(code, lino, _end);
          }
        }
      }
      pInfo = tSimpleHashIterate(pTask->pVirTableInfos, pInfo, &iter);
    }
  }

  pGroup->pPendingCalcParams = taosArrayInit(0, sizeof(SSTriggerCalcParam));
  QUERY_CHECK_NULL(pGroup->pPendingCalcParams, code, lino, _end, terrno);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void stRealtimeGroupDestroy(void *ptr) {
  SSTriggerRealtimeGroup **ppGroup = ptr;
  if (ppGroup == NULL || *ppGroup == NULL) {
    return;
  }

  SSTriggerRealtimeGroup *pGroup = *ppGroup;
  if (pGroup->pVirTableInfos != NULL) {
    taosArrayDestroy(pGroup->pVirTableInfos);
    pGroup->pVirTableInfos = NULL;
  }
  if (pGroup->pTableMetas != NULL) {
    tSimpleHashCleanup(pGroup->pTableMetas);
    pGroup->pTableMetas = NULL;
  }

  TRINGBUF_DESTROY(&pGroup->winBuf);
  if ((pGroup->pContext->pTask->triggerType == STREAM_TRIGGER_STATE) && IS_VAR_DATA_TYPE(pGroup->stateVal.type)) {
    taosMemoryFreeClear(pGroup->stateVal.pData);
  }

  if (pGroup->pPendingCalcParams) {
    taosArrayDestroyEx(pGroup->pPendingCalcParams, tDestroySSTriggerCalcParam);
    pGroup->pPendingCalcParams = NULL;
  }

  taosMemFreeClear(*ppGroup);
}

static void stRealtimeGroupClearTempState(SSTriggerRealtimeGroup *pGroup) {
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  pContext->reenterCheck = false;
  pContext->tbIter = 0;
  pContext->pCurVirTable = NULL;
  pContext->pCurTableMeta = NULL;
  pContext->pMetaToFetch = NULL;
  pContext->pColRefToFetch = NULL;
  pContext->pParamToFetch = NULL;

  stTimestampSorterReset(pContext->pSorter);
  stVtableMergerReset(pContext->pMerger);
  if (pContext->pSavedWindows != NULL) {
    taosArrayClear(pContext->pSavedWindows);
  }
  if (pContext->pInitWindows != NULL) {
    taosArrayClear(pContext->pInitWindows);
  }
  if (pContext->pNotifyParams != NULL) {
    taosArrayClearEx(pContext->pNotifyParams, tDestroySSTriggerCalcParam);
  }
}

static void stRealtimeGroupClearMetadatas(SSTriggerRealtimeGroup *pGroup) {
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;
  int32_t                   iter = 0;
  SSTriggerTableMeta       *pTableMeta = tSimpleHashIterate(pGroup->pTableMetas, NULL, &iter);
  while (pTableMeta != NULL) {
    if ((pTask->placeHolderBitmap & PLACE_HOLDER_PARTITION_ROWS) && IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup) &&
        (taosArrayGetSize(pTableMeta->pMetas) > 0)) {
      int64_t endTime = TRINGBUF_HEAD(&pGroup->winBuf)->range.skey - 1;
      endTime = TMAX(endTime, pGroup->prevWindowEnd);
      int32_t idx = taosArraySearchIdx(pTableMeta->pMetas, &endTime, stRealtimeGroupMetaDataSearch, TD_GT);
      taosArrayPopFrontBatch(pTableMeta->pMetas, idx);
      idx = taosArraySearchIdx(pTableMeta->pMetas, &pGroup->newThreshold, stRealtimeGroupMetaDataSearch, TD_GT);
      pTableMeta->metaIdx = (idx == -1) ? TARRAY_SIZE(pTableMeta->pMetas) : idx;
    } else {
      taosArrayClear(pTableMeta->pMetas);
      pTableMeta->metaIdx = 0;
    }
    pTableMeta = tSimpleHashIterate(pGroup->pTableMetas, pTableMeta, &iter);
  }
  pGroup->oldThreshold = pGroup->newThreshold;
}

static int32_t stRealtimeGroupAddMetaDatas(SSTriggerRealtimeGroup *pGroup, SArray *pMetadatas, SArray *pVgIds) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;
  SSTriggerTableMeta       *pTableMeta = NULL;
  SSHashObj                *pAddedUids = NULL;

  QUERY_CHECK_NULL(pMetadatas, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(taosArrayGetSize(pMetadatas) == taosArrayGetSize(pVgIds), code, lino, _end,
                        TSDB_CODE_INVALID_PARA);

  if (pTask->triggerType == STREAM_TRIGGER_PERIOD) {
    pGroup->oldThreshold = INT64_MIN;
  }

  pGroup->newThreshold = pGroup->oldThreshold;
  pAddedUids = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  QUERY_CHECK_NULL(pAddedUids, code, lino, _end, terrno);

  for (int32_t i = 0; i < TARRAY_SIZE(pMetadatas); i++) {
    SSDataBlock *pBlock = *(SSDataBlock **)TARRAY_GET_ELEM(pMetadatas, i);
    int32_t      vgId = *(int32_t *)TARRAY_GET_ELEM(pVgIds, i);
    int32_t      nrows = blockDataGetNumOfRows(pBlock);
    if (nrows == 0) {
      continue;
    }
    int32_t          iCol = 0;
    SColumnInfoData *pTypeCol = taosArrayGet(pBlock->pDataBlock, iCol++);
    QUERY_CHECK_NULL(pTypeCol, code, lino, _end, terrno);
    uint8_t *pTypes = (uint8_t *)pTypeCol->pData;
    int64_t *pGids = NULL;
    if (!pTask->isVirtualTable) {
      SColumnInfoData *pGidCol = taosArrayGet(pBlock->pDataBlock, iCol++);
      QUERY_CHECK_NULL(pGidCol, code, lino, _end, terrno);
      pGids = (int64_t *)pGidCol->pData;
    }
    SColumnInfoData *pUidCol = taosArrayGet(pBlock->pDataBlock, iCol++);
    QUERY_CHECK_NULL(pUidCol, code, lino, _end, terrno);
    int64_t         *pUids = (int64_t *)pUidCol->pData;
    SColumnInfoData *pSkeyCol = taosArrayGet(pBlock->pDataBlock, iCol++);
    QUERY_CHECK_NULL(pSkeyCol, code, lino, _end, terrno);
    int64_t         *pSkeys = (int64_t *)pSkeyCol->pData;
    SColumnInfoData *pEkeyCol = taosArrayGet(pBlock->pDataBlock, iCol++);
    QUERY_CHECK_NULL(pEkeyCol, code, lino, _end, terrno);
    int64_t         *pEkeys = (int64_t *)pEkeyCol->pData;
    SColumnInfoData *pVerCol = taosArrayGet(pBlock->pDataBlock, iCol++);
    QUERY_CHECK_NULL(pVerCol, code, lino, _end, terrno);
    int64_t         *pVers = (int64_t *)pVerCol->pData;
    SColumnInfoData *pNrowsCol = taosArrayGet(pBlock->pDataBlock, iCol++);
    QUERY_CHECK_NULL(pNrowsCol, code, lino, _end, terrno);
    int64_t *pNrows = (int64_t *)pNrowsCol->pData;

    for (int32_t i = 0; i < nrows; i++) {
      bool inGroup = false;
      if (pTask->isVirtualTable) {
        inGroup = (tSimpleHashGet(pGroup->pTableMetas, &pUids[i], sizeof(int64_t)) != NULL);
      } else {
        inGroup = (pGids[i] == pGroup->gid);
      }
      if (!inGroup) {
        continue;
      }

      if (pSkeys[i] <= pGroup->oldThreshold &&
          ((pTypes[i] == WAL_DELETE_DATA) || (pTypes[i] == WAL_SUBMIT_DATA && !pTask->ignoreDisorder))) {
        // mark recalc time range for disordered data
        int64_t recalcSkey = pSkeys[i];
        if (pTask->expiredTime > 0) {
          recalcSkey = TMAX(recalcSkey, pGroup->oldThreshold - pTask->expiredTime + 1);
        }
        int64_t recalcEkey = TMIN(pEkeys[i], pGroup->oldThreshold);
        if (recalcSkey <= recalcEkey) {
          code = stTriggerTaskMarkRecalc(pTask, pGroup->gid, recalcSkey, recalcEkey);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }

      if (pEkeys[i] > pGroup->oldThreshold) {
        code = tSimpleHashPut(pAddedUids, &pUids[i], sizeof(int64_t), NULL, 0);
        QUERY_CHECK_CODE(code, lino, _end);
        if (pTableMeta == NULL || pTableMeta->tbUid != pUids[i]) {
          pTableMeta = tSimpleHashGet(pGroup->pTableMetas, &pUids[i], sizeof(int64_t));
          if (pTableMeta == NULL) {
            SSTriggerTableMeta newTableMeta = {.tbUid = pUids[i], .vgId = vgId};
            code = tSimpleHashPut(pGroup->pTableMetas, &pUids[i], sizeof(int64_t), &newTableMeta,
                                  sizeof(SSTriggerTableMeta));
            QUERY_CHECK_CODE(code, lino, _end);
            pTableMeta = tSimpleHashGet(pGroup->pTableMetas, &pUids[i], sizeof(int64_t));
            QUERY_CHECK_NULL(pTableMeta, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
          }
        }
        if (pTableMeta->pMetas == NULL) {
          pTableMeta->pMetas = taosArrayInit(0, sizeof(SSTriggerMetaData));
          QUERY_CHECK_NULL(pTableMeta->pMetas, code, lino, _end, terrno);
        }

        if (pTypes[i] == WAL_DELETE_DATA) {
          // shrink the range of existing metas for delete metadata
          for (int32_t j = 0; j < TARRAY_SIZE(pTableMeta->pMetas); j++) {
            SSTriggerMetaData *pMeta = TARRAY_GET_ELEM(pTableMeta->pMetas, j);
            if (pMeta->skey > pMeta->ekey || pMeta->skey > pEkeys[i] || pMeta->ekey < pSkeys[i]) {
              continue;
            } else if (pMeta->skey >= pSkeys[i]) {
              pMeta->skey = pEkeys[i] + 1;
              SET_TRIGGER_META_SKEY_INACCURATE(pMeta);
            } else if (pMeta->ekey <= pEkeys[i]) {
              pMeta->ekey = pSkeys[i] - 1;
              SET_TRIGGER_META_EKEY_INACCURATE(pMeta);
            } else {
              SSTriggerMetaData *pNewMeta = taosArrayPush(pTableMeta->pMetas, pMeta);
              QUERY_CHECK_NULL(pNewMeta, code, lino, _end, terrno);
              pMeta->ekey = pSkeys[i] - 1;
              SET_TRIGGER_META_EKEY_INACCURATE(pMeta);
              pNewMeta->skey = pEkeys[i] + 1;
              SET_TRIGGER_META_SKEY_INACCURATE(pNewMeta);
            }
            if (pMeta->skey > pMeta->ekey) {
              // set the range of invalid metadata to INT64_MAX, so they will be sorted to the end
              pMeta->skey = pMeta->ekey = INT64_MAX;
            }
          }
        } else if (pTypes[i] == WAL_SUBMIT_DATA) {
          // add new insert metadata
          int64_t skey = TMAX(pSkeys[i], pGroup->oldThreshold + 1);
          if (pTask->ignoreDisorder && TARRAY_SIZE(pTableMeta->pMetas) > 0) {
            SSTriggerMetaData *pLastMeta = TARRAY_GET_ELEM(pTableMeta->pMetas, TARRAY_SIZE(pTableMeta->pMetas) - 1);
            skey = TMAX(skey, pLastMeta->ekey + 1);
          }
          if (skey <= pEkeys[i]) {
            SSTriggerMetaData *pNewMeta = taosArrayReserve(pTableMeta->pMetas, 1);
            QUERY_CHECK_NULL(pNewMeta, code, lino, _end, terrno);
            pNewMeta->skey = TMAX(pSkeys[i], pGroup->oldThreshold + 1);
            pNewMeta->ekey = pEkeys[i];
            pNewMeta->ver = pVers[i];
            pNewMeta->nrows = pNrows[i];
            if (skey != pSkeys[i]) {
              SET_TRIGGER_META_SKEY_INACCURATE(pNewMeta);
            }
          }
        }
      }
    }
  }

  int32_t iter = 0;
  void   *px = tSimpleHashIterate(pAddedUids, NULL, &iter);
  while (px != NULL) {
    int64_t uid = *(int64_t *)tSimpleHashGetKey(px, NULL);
    pTableMeta = tSimpleHashGet(pGroup->pTableMetas, &uid, sizeof(int64_t));
    if (pTableMeta->metaIdx < TARRAY_SIZE(pTableMeta->pMetas)) {
      SSTriggerMetaData *pMeta = TARRAY_DATA(pTableMeta->pMetas);
      taosSort(pMeta + pTableMeta->metaIdx, TARRAY_SIZE(pTableMeta->pMetas) - pTableMeta->metaIdx,
               sizeof(SSTriggerMetaData), stRealtimeGroupMetaDataCompare);
      while (TARRAY_SIZE(pTableMeta->pMetas) > pTableMeta->metaIdx) {
        SSTriggerMetaData *pMeta = taosArrayGetLast(pTableMeta->pMetas);
        if (pMeta->skey != INT64_MAX || pMeta->ekey != INT64_MAX) {
          break;
        }
        // remove invalid metadata
        TARRAY_SIZE(pTableMeta->pMetas)--;
      }
    }
    px = tSimpleHashIterate(pAddedUids, px, &iter);
  }

  if (pTask->triggerType == STREAM_TRIGGER_PERIOD) {
    pGroup->newThreshold = INT64_MAX;
    goto _end;
  }

  // update the group threshold
  iter = 0;
  pTableMeta = tSimpleHashIterate(pGroup->pTableMetas, NULL, &iter);
  while (pTableMeta != NULL) {
    if (taosArrayGetSize(pTableMeta->pMetas) > pTableMeta->metaIdx) {
      SSTriggerMetaData *pMeta = taosArrayGetLast(pTableMeta->pMetas);
      pGroup->newThreshold = TMAX(pGroup->newThreshold, pMeta->ekey - pTask->watermark);
    }
    pTableMeta = tSimpleHashIterate(pGroup->pTableMetas, pTableMeta, &iter);
  }
  QUERY_CHECK_CONDITION(pGroup->newThreshold != INT64_MAX, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

_end:

  tSimpleHashCleanup(pAddedUids);
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stRealtimeGroupOpenWindow(SSTriggerRealtimeGroup *pGroup, int64_t ts, char **ppExtraNotifyContent,
                                         bool saveWindow, bool hasStartData) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;
  SSTriggerWindow           newWindow = {0};
  SSTriggerCalcParam        param = {0};

  bool    needCalc = (pTask->calcEventType & STRIGGER_EVENT_WINDOW_OPEN);
  bool    needNotify = (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_OPEN);
  int64_t now = taosGetTimestampNs();
  if (needCalc || needNotify) {
    param.triggerTime = now;
    param.notifyType = needNotify ? STRIGGER_EVENT_WINDOW_OPEN : STRIGGER_EVENT_WINDOW_NONE;
    param.extraNotifyContent = ppExtraNotifyContent ? *ppExtraNotifyContent : NULL;
  }
  newWindow.prevProcTime = now;
  newWindow.wrownum = hasStartData ? 1 : 0;
  if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
    newWindow.wrownum = TRINGBUF_HEAD(&pGroup->winBuf)->wrownum - newWindow.wrownum;
  }

  switch (pTask->triggerType) {
    case STREAM_TRIGGER_SLIDING: {
      if (pTask->interval.interval > 0) {
        // interval window trigger
        if (IS_TRIGGER_GROUP_NONE_WINDOW(pGroup)) {
          pGroup->nextWindow = stTriggerTaskGetIntervalWindow(pTask, ts);
        }
        newWindow.range = pGroup->nextWindow;
        stTriggerTaskNextIntervalWindow(pTask, &pGroup->nextWindow);
        if (needCalc || needNotify) {
          STimeWindow prevWindow = newWindow.range;
          stTriggerTaskPrevIntervalWindow(pTask, &prevWindow);
          param.wstart = newWindow.range.skey;
          param.wend = newWindow.range.ekey;
          param.wduration = param.wend - param.wstart;
          param.prevTs = prevWindow.skey;
          param.currentTs = newWindow.range.skey;
          param.nextTs = pGroup->nextWindow.skey;
        }
        break;
      }
      // sliding trigger works the same as period trigger
    }
    case STREAM_TRIGGER_PERIOD: {
      QUERY_CHECK_CONDITION(!IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INVALID_PARA);
      if (IS_TRIGGER_GROUP_NONE_WINDOW(pGroup)) {
        pGroup->nextWindow = stTriggerTaskGetPeriodWindow(pTask, ts);
      }
      newWindow.range = pGroup->nextWindow;
      stTriggerTaskNextPeriodWindow(pTask, &pGroup->nextWindow);
      QUERY_CHECK_CONDITION(!needCalc && !needNotify, code, lino, _end, TSDB_CODE_INVALID_PARA);
      break;
    }
    case STREAM_TRIGGER_SESSION:
    case STREAM_TRIGGER_STATE:
    case STREAM_TRIGGER_EVENT: {
      QUERY_CHECK_CONDITION(!IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INVALID_PARA);
      // works the same as count window trigger
    }
    case STREAM_TRIGGER_COUNT: {
      newWindow.range = (STimeWindow){.skey = ts, .ekey = ts};
      if (needCalc || needNotify) {
        param.wstart = ts;
        param.wend = ts;
      }
      break;
    }

    default: {
      ST_TASK_ELOG("invalid stream trigger type %d at %s:%d", pTask->triggerType, __func__, __LINE__);
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  code = TRINGBUF_APPEND(&pGroup->winBuf, newWindow);
  QUERY_CHECK_CODE(code, lino, _end);

  if (saveWindow) {
    // only save window when close window
  } else if (needCalc) {
    void *px = taosArrayPush(pGroup->pPendingCalcParams, &param);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
  } else if (needNotify) {
    void *px = taosArrayPush(pContext->pNotifyParams, &param);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
  } else {
    QUERY_CHECK_CONDITION(ppExtraNotifyContent == NULL || *ppExtraNotifyContent == NULL, code, lino, _end,
                          TSDB_CODE_INVALID_PARA);
  }

  if (ppExtraNotifyContent) {
    *ppExtraNotifyContent = NULL;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stRealtimeGroupCloseWindow(SSTriggerRealtimeGroup *pGroup, char **ppExtraNotifyContent,
                                          bool saveWindow) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;
  SSTriggerWindow          *pCurWindow = NULL;
  SSTriggerCalcParam        param = {0};
  bool                      needCalc = false;
  bool                      needNotify = false;

  if (IS_TRIGGER_GROUP_NONE_WINDOW(pGroup)) {
    goto _end;
  }
  QUERY_CHECK_CONDITION(IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INVALID_PARA);

  if (pTask->calcEventType & STRIGGER_EVENT_WINDOW_CLOSE) {
    needCalc = true;
  }
  if (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_CLOSE) {
    needNotify = true;
  }
  if (needCalc || needNotify) {
    param.triggerTime = taosGetTimestampNs();
    param.extraNotifyContent = ppExtraNotifyContent ? *ppExtraNotifyContent : NULL;
    if (needNotify) {
      if ((pTask->triggerType == STREAM_TRIGGER_PERIOD) ||
          (pTask->triggerType == STREAM_TRIGGER_SLIDING && pTask->interval.interval == 0)) {
        param.notifyType = STRIGGER_EVENT_ON_TIME;
      } else {
        param.notifyType = STRIGGER_EVENT_WINDOW_CLOSE;
      }
    }
  }

  pCurWindow = TRINGBUF_HEAD(&pGroup->winBuf);

  if ((pTask->triggerType == STREAM_TRIGGER_STATE &&
       pCurWindow->range.ekey - pCurWindow->range.skey < pTask->stateTrueFor) ||
      (pTask->triggerType == STREAM_TRIGGER_EVENT &&
       pCurWindow->range.ekey - pCurWindow->range.skey < pTask->eventTrueFor)) {
    // check TRUE FOR condition
    needCalc = needNotify = false;
  }

  switch (pTask->triggerType) {
    case STREAM_TRIGGER_PERIOD: {
      SInterval *pInterval = &pTask->interval;
      QUERY_CHECK_CONDITION(needCalc || needNotify, code, lino, _end, TSDB_CODE_INVALID_PARA);
      param.prevLocalTime = pCurWindow->range.skey - 1;
      param.triggerTime = pCurWindow->range.ekey;
      param.nextLocalTime = pGroup->nextWindow.ekey;
      break;
    }
    case STREAM_TRIGGER_SLIDING: {
      if (pTask->interval.interval == 0) {
        // sliding trigger
        QUERY_CHECK_CONDITION(needCalc || needNotify, code, lino, _end, TSDB_CODE_INVALID_PARA);
        param.prevTs = pCurWindow->range.skey - 1;
        param.currentTs = pCurWindow->range.ekey;
        param.nextTs = pGroup->nextWindow.ekey;
      } else {
        STimeWindow prevWindow = pCurWindow->range;
        stTriggerTaskPrevIntervalWindow(pTask, &prevWindow);
        param.prevTs = prevWindow.ekey + 1;
        param.currentTs = pCurWindow->range.ekey + 1;
        param.nextTs = pGroup->nextWindow.ekey + 1;
      }
      // fill the param the same way as other window trigger
    }
    case STREAM_TRIGGER_SESSION:
    case STREAM_TRIGGER_COUNT:
    case STREAM_TRIGGER_STATE:
    case STREAM_TRIGGER_EVENT: {
      if (needCalc || needNotify) {
        param.wstart = pCurWindow->range.skey;
        param.wend = pCurWindow->range.ekey;
        param.wduration = param.wend - param.wstart;
        param.wrownum = pCurWindow->wrownum;
      }
      break;
    }

    default: {
      ST_TASK_ELOG("invalid stream trigger type %d at %s:%d", pTask->triggerType, __func__, __LINE__);
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  TRINGBUF_DEQUEUE(&pGroup->winBuf);
  if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
    // ajustify the following window's wrownum
    SSTriggerWindow *pHead = TRINGBUF_HEAD(&pGroup->winBuf);
    SSTriggerWindow *p = pHead;
    int32_t          bias = pHead->wrownum;
    do {
      p->wrownum -= bias;
      TRINGBUF_MOVE_NEXT(&pGroup->winBuf, p);
    } while (p != TRINGBUF_TAIL(&pGroup->winBuf));
    pHead->range.ekey = TMAX(pHead->range.ekey, pCurWindow->range.ekey);
    pHead->wrownum = pCurWindow->wrownum - bias;
  }

  if (saveWindow) {
    // skip add window for session trigger, since it will be merged after processing all tables
    void *px = taosArrayPush(pContext->pSavedWindows, pCurWindow);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
  } else if (needCalc) {
    void *px = taosArrayPush(pGroup->pPendingCalcParams, &param);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
  } else if (needNotify) {
    void *px = taosArrayPush(pContext->pNotifyParams, &param);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
  } else if (ppExtraNotifyContent != NULL && *ppExtraNotifyContent != NULL) {
    taosMemoryFreeClear(*ppExtraNotifyContent);
  }

  if (ppExtraNotifyContent) {
    *ppExtraNotifyContent = NULL;
  }

  if (!saveWindow) {
    pGroup->prevWindowEnd = param.wend;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stRealtimeGroupSaveInitWindow(SSTriggerRealtimeGroup *pGroup, SArray *pInitWindows) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  QUERY_CHECK_NULL(pInitWindows, code, lino, _end, TSDB_CODE_INVALID_PARA);

  taosArrayClear(pInitWindows);
  if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
    SSTriggerWindow *p = TRINGBUF_HEAD(&pGroup->winBuf);
    do {
      void *px = taosArrayPush(pInitWindows, &p->range);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      TRINGBUF_MOVE_NEXT(&pGroup->winBuf, p);
    } while (p != TRINGBUF_TAIL(&pGroup->winBuf));
  }

  if (pTask->triggerType == STREAM_TRIGGER_SLIDING) {
    void *px = taosArrayPush(pInitWindows, &pGroup->nextWindow);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stRealtimeGroupRestoreInitWindow(SSTriggerRealtimeGroup *pGroup, SArray *pInitWindows) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  QUERY_CHECK_CONDITION(!IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INVALID_PARA);

  int32_t nWindows = taosArrayGetSize(pInitWindows);

  if (pTask->triggerType == STREAM_TRIGGER_SLIDING) {
    QUERY_CHECK_CONDITION(nWindows > 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
    pGroup->nextWindow = *(STimeWindow *)taosArrayGetLast(pInitWindows);
    nWindows--;
  }

  for (int32_t i = 0; i < nWindows; i++) {
    STimeWindow    *pRange = TARRAY_GET_ELEM(pInitWindows, i);
    SSTriggerWindow win = {.range = *pRange};
    code = TRINGBUF_APPEND(&pGroup->winBuf, win);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stRealtimeGroupMergeSavedWindows(SSTriggerRealtimeGroup *pGroup, int64_t gap) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  QUERY_CHECK_CONDITION(!IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INVALID_PARA);

  if (taosArrayGetSize(pContext->pSavedWindows) == 0) {
    goto _end;
  }

  taosArraySort(pContext->pSavedWindows, stRealtimeGroupWindowCompare);
  SSTriggerWindow *pWin = TARRAY_GET_ELEM(pContext->pSavedWindows, 0);
  for (int32_t i = 1; i < TARRAY_SIZE(pContext->pSavedWindows); i++) {
    SSTriggerWindow *pCurWin = TARRAY_GET_ELEM(pContext->pSavedWindows, i);
    if ((gap > 0 && pWin->range.ekey + gap >= pCurWin->range.skey) ||
        (gap == 0 && pWin->range.skey == pCurWin->range.skey)) {
      pWin->range.ekey = TMAX(pWin->range.ekey, pCurWin->range.ekey);
      pWin->wrownum += pCurWin->wrownum;
    } else {
      ++pWin;
      *pWin = *pCurWin;
    }
  }
  TARRAY_SIZE(pContext->pSavedWindows) = TARRAY_ELEM_IDX(pContext->pSavedWindows, pWin) + 1;

  bool    calcOpen = (pTask->calcEventType & STRIGGER_EVENT_WINDOW_OPEN);
  bool    calcClose = (pTask->calcEventType & STRIGGER_EVENT_WINDOW_CLOSE);
  bool    notifyOpen = (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_OPEN);
  bool    notifyClose = (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_CLOSE);
  int32_t nInitWins = taosArrayGetSize(pContext->pInitWindows);

  // trigger all window open/close events
  for (int32_t i = 0; i < TARRAY_SIZE(pContext->pSavedWindows); i++) {
    pWin = TARRAY_GET_ELEM(pContext->pSavedWindows, i);
    // window open event may have been triggered previously
    if ((calcOpen || notifyOpen) && i >= nInitWins) {
      SSTriggerCalcParam param = {.triggerTime = taosGetTimestampNs(),
                                  .notifyType = (notifyOpen ? STRIGGER_EVENT_WINDOW_OPEN : STRIGGER_EVENT_WINDOW_NONE),
                                  .wstart = pWin->range.skey,
                                  .wend = pWin->range.ekey,
                                  .wduration = pWin->range.ekey - pWin->range.skey,
                                  .wrownum = pWin->wrownum};
      if (calcOpen) {
        void *px = taosArrayPush(pGroup->pPendingCalcParams, &param);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      } else if (notifyOpen) {
        void *px = taosArrayPush(pContext->pNotifyParams, &param);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      }
    }

    // some window may have not been closed yet
    if (pWin->range.ekey + gap > pGroup->newThreshold) {
      // todo(kjq): restore prevProcTime from saved init windows
      pWin->prevProcTime = taosGetTimestampNs();
      if (TRINGBUF_SIZE(&pGroup->winBuf) > 0) {
        pWin->wrownum = TRINGBUF_HEAD(&pGroup->winBuf)->wrownum - pWin->wrownum;
      }
      code = TRINGBUF_APPEND(&pGroup->winBuf, *pWin);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      SSTriggerCalcParam param = {.triggerTime = taosGetTimestampNs(),
                                  .wstart = pWin->range.skey,
                                  .wend = pWin->range.ekey,
                                  .wduration = pWin->range.ekey - pWin->range.skey,
                                  .wrownum = pWin->wrownum};
      if (notifyClose) {
        if ((pTask->triggerType == STREAM_TRIGGER_PERIOD) ||
            (pTask->triggerType == STREAM_TRIGGER_SLIDING && pTask->interval.interval == 0)) {
          param.notifyType = STRIGGER_EVENT_ON_TIME;
        } else {
          param.notifyType = STRIGGER_EVENT_WINDOW_CLOSE;
        }
      }
      if (calcClose) {
        void *px = taosArrayPush(pGroup->pPendingCalcParams, &param);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      } else if (notifyClose) {
        void *px = taosArrayPush(pContext->pNotifyParams, &param);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      }
      pGroup->prevWindowEnd = param.wend;
    }
  }

  if (pTask->triggerType == STREAM_TRIGGER_SLIDING) {
    pWin = TARRAY_GET_ELEM(pContext->pSavedWindows, TARRAY_SIZE(pContext->pSavedWindows) - 1);
    pGroup->nextWindow = pWin->range;
    SInterval *pInterval = &pTask->interval;
    if (pInterval->interval > 0) {
      stTriggerTaskNextIntervalWindow(pTask, &pGroup->nextWindow);
    } else {
      stTriggerTaskNextPeriodWindow(pTask, &pGroup->nextWindow);
    }
  }

  taosArrayClear(pContext->pSavedWindows);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stRealtimeGroupGetDataBlock(SSTriggerRealtimeGroup *pGroup, bool saveWindow, SSDataBlock **ppDataBlock,
                                           int32_t *pStartIdx, int32_t *pEndIdx, bool *pAllTableProcessed,
                                           bool *pNeedFetchData) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;
  bool                      isCalcData = (pContext->status == STRIGGER_CONTEXT_SEND_CALC_REQ);

  *pAllTableProcessed = false;
  *pNeedFetchData = false;

  while (!*pAllTableProcessed && !*pNeedFetchData) {
    if (!pTask->isVirtualTable) {
      if (IS_TRIGGER_TIMESTAMP_SORTER_EMPTY(pContext->pSorter)) {
        while (saveWindow && IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
          code = stRealtimeGroupCloseWindow(pGroup, NULL, saveWindow);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        stTimestampSorterReset(pContext->pSorter);
        pContext->pCurTableMeta = tSimpleHashIterate(pGroup->pTableMetas, pContext->pCurTableMeta, &pContext->tbIter);
        if (pContext->pCurTableMeta == NULL) {
          *pAllTableProcessed = true;
          break;
        }
        if (saveWindow) {
          code = stRealtimeGroupRestoreInitWindow(pGroup, pContext->pInitWindows);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        STimeWindow range = {.skey = INT64_MIN, .ekey = INT64_MAX - 1};
        if (pContext->status == STRIGGER_CONTEXT_CHECK_CONDITION) {
          range.skey = pGroup->oldThreshold + 1;
          range.ekey = pGroup->newThreshold;
        } else if (pContext->status == STRIGGER_CONTEXT_SEND_CALC_REQ) {
          if (pTask->triggerType != STREAM_TRIGGER_PERIOD) {
            range.skey = pContext->pParamToFetch->wstart;
            range.ekey = pContext->pParamToFetch->wend;
            if (TARRAY_ELEM_IDX(pContext->pCalcReq->params, pContext->pParamToFetch) > 0) {
              SSTriggerCalcParam *pPrevParam = pContext->pParamToFetch - 1;
              range.skey = TMAX(range.skey, pPrevParam->wend + 1);
            }
          }
        } else {
          code = TSDB_CODE_INTERNAL_ERROR;
          QUERY_CHECK_CODE(code, lino, _end);
        }
        code = stTimestampSorterSetSortInfo(pContext->pSorter, &range, pContext->pCurTableMeta->tbUid,
                                            isCalcData ? pTask->calcTsIndex : pTask->trigTsIndex);
        QUERY_CHECK_CODE(code, lino, _end);
        code = stTimestampSorterSetMetaDatas(pContext->pSorter, pContext->pCurTableMeta);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      code = stTimestampSorterNextDataBlock(pContext->pSorter, ppDataBlock, pStartIdx, pEndIdx);
      QUERY_CHECK_CODE(code, lino, _end);
      if (*ppDataBlock == NULL) {
        if (!IS_TRIGGER_TIMESTAMP_SORTER_EMPTY(pContext->pSorter)) {
          *pNeedFetchData = true;
          code = stTimestampSorterGetMetaToFetch(pContext->pSorter, &pContext->pMetaToFetch);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        continue;
      }
      break;
    } else {
      if (IS_TRIGGER_VTABLE_MERGER_EMPTY(pContext->pMerger)) {
        while (saveWindow && IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
          code = stRealtimeGroupCloseWindow(pGroup, NULL, saveWindow);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        stVtableMergerReset(pContext->pMerger);
        if (pContext->tbIter >= taosArrayGetSize(pGroup->pVirTableInfos)) {
          *pAllTableProcessed = true;
          break;
        } else {
          pContext->pCurVirTable = *(SSTriggerVirTableInfo **)TARRAY_GET_ELEM(pGroup->pVirTableInfos, pContext->tbIter);
          pContext->tbIter++;
        }
        if (saveWindow) {
          code = stRealtimeGroupRestoreInitWindow(pGroup, pContext->pInitWindows);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        STimeWindow range = {.skey = INT64_MIN, .ekey = INT64_MAX - 1};
        if (pContext->status == STRIGGER_CONTEXT_CHECK_CONDITION) {
          range.skey = pGroup->oldThreshold + 1;
          range.ekey = pGroup->newThreshold;
        } else if (pContext->status == STRIGGER_CONTEXT_SEND_CALC_REQ) {
          if (pTask->triggerType != STREAM_TRIGGER_PERIOD) {
            range.skey = pContext->pParamToFetch->wstart;
            range.ekey = pContext->pParamToFetch->wend;
            if (TARRAY_ELEM_IDX(pContext->pCalcReq->params, pContext->pParamToFetch) > 0) {
              SSTriggerCalcParam *pPrevParam = pContext->pParamToFetch - 1;
              range.skey = TMAX(range.skey, pPrevParam->wend + 1);
            }
          }
        } else {
          code = TSDB_CODE_INTERNAL_ERROR;
          QUERY_CHECK_CODE(code, lino, _end);
        }
        code = stVtableMergerSetMergeInfo(
            pContext->pMerger, &range,
            isCalcData ? pContext->pCurVirTable->pCalcColRefs : pContext->pCurVirTable->pTrigColRefs);
        QUERY_CHECK_CODE(code, lino, _end);
        code = stVtableMergerSetMetaDatas(pContext->pMerger, pGroup->pTableMetas);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      code = stVtableMergerNextDataBlock(pContext->pMerger, ppDataBlock);
      QUERY_CHECK_CODE(code, lino, _end);
      *pStartIdx = 0;
      *pEndIdx = *ppDataBlock ? blockDataGetNumOfRows(*ppDataBlock) : 0;
      if (*ppDataBlock == NULL) {
        if (!IS_TRIGGER_VTABLE_MERGER_EMPTY(pContext->pMerger)) {
          *pNeedFetchData = true;
          code = stVtableMergerGetMetaToFetch(pContext->pMerger, &pContext->pMetaToFetch, &pContext->pColRefToFetch);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        continue;
      }
      break;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stRealtimeGroupDoPeriodCheck(SSTriggerRealtimeGroup *pGroup) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  // enable to get all calc data when sending calc request
  pGroup->oldThreshold = INT64_MIN;
  pGroup->newThreshold = INT64_MAX;

  QUERY_CHECK_CONDITION(!IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INVALID_PARA);
  pGroup->nextWindow = pContext->periodWindow;
  code = stRealtimeGroupOpenWindow(pGroup, pContext->periodWindow.ekey, NULL, false, false);
  QUERY_CHECK_CODE(code, lino, _end);
  STimeWindow *pCurWin = &TRINGBUF_HEAD(&pGroup->winBuf)->range;
  QUERY_CHECK_CONDITION(memcmp(pCurWin, &pContext->periodWindow, sizeof(STimeWindow)) == 0, code, lino, _end,
                        TSDB_CODE_INTERNAL_ERROR);
  code = stRealtimeGroupCloseWindow(pGroup, NULL, false);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stRealtimeGroupDoSlidingCheck(SSTriggerRealtimeGroup *pGroup) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;
  bool                      readAllData = false;
  bool                      allTableProcessed = false;
  bool                      needFetchData = false;

  if (IS_TRIGGER_GROUP_NONE_WINDOW(pGroup)) {
    int64_t             ts = INT64_MAX;
    int32_t             iter = 0;
    SSTriggerTableMeta *pTableMeta = tSimpleHashIterate(pGroup->pTableMetas, NULL, &iter);
    while (pTableMeta != NULL) {
      for (int32_t i = 0; i < taosArrayGetSize(pTableMeta->pMetas); i++) {
        SSTriggerMetaData *pMeta = TARRAY_GET_ELEM(pTableMeta->pMetas, i);
        ts = TMIN(ts, pMeta->skey);
      }
      pTableMeta = tSimpleHashIterate(pGroup->pTableMetas, pTableMeta, &iter);
    }
    QUERY_CHECK_CONDITION(ts != INT64_MAX, code, lino, _end, TSDB_CODE_INVALID_PARA);
    code = stRealtimeGroupOpenWindow(pGroup, ts, NULL, false, false);
    QUERY_CHECK_CODE(code, lino, _end);
    pGroup->oldThreshold = ts - 1;
  }

  if (!pContext->reenterCheck) {
    // save initial windows at the first check
    code = stRealtimeGroupSaveInitWindow(pGroup, pContext->pInitWindows);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (pTask->placeHolderBitmap & PLACE_HOLDER_WROWNUM) {
    readAllData = true;
  }

  if (readAllData) {
    // read all data of the current table
    while (!allTableProcessed && !needFetchData) {
      SSDataBlock *pDataBlock = NULL;
      int32_t      startIdx = 0;
      int32_t      endIdx = 0;
      code = stRealtimeGroupGetDataBlock(pGroup, true, &pDataBlock, &startIdx, &endIdx, &allTableProcessed,
                                         &needFetchData);
      QUERY_CHECK_CODE(code, lino, _end);
      if (allTableProcessed || needFetchData) {
        break;
      }
      SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pTask->trigTsIndex);
      QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
      int64_t *pTsData = (int64_t *)pTsCol->pData;
      for (int32_t r = startIdx; r < endIdx;) {
        int64_t nextStart = pGroup->nextWindow.skey;
        int64_t curEnd = IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup) ? TRINGBUF_HEAD(&pGroup->winBuf)->range.ekey : INT64_MAX;
        int64_t ts = TMIN(nextStart, curEnd);
        void   *px = taosbsearch(&ts, pTsData + r, endIdx - r, sizeof(int64_t), compareInt64Val, TD_GT);
        int32_t nrows = (px != NULL) ? (POINTER_DISTANCE(px, &pTsData[r]) / sizeof(int64_t)) : (endIdx - r);
        r += nrows;
        if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
          TRINGBUF_HEAD(&pGroup->winBuf)->wrownum += nrows;
        }
        if (ts == nextStart) {
          code = stRealtimeGroupOpenWindow(pGroup, ts, NULL, true, r > 0 && pTsData[r - 1] == nextStart);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        QUERY_CHECK_CONDITION(IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        if (TRINGBUF_HEAD(&pGroup->winBuf)->range.ekey == ts) {
          code = stRealtimeGroupCloseWindow(pGroup, NULL, true);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
    }
  } else {
    allTableProcessed = true;
  }

  if (allTableProcessed) {
    if (readAllData) {
      code = stRealtimeGroupMergeSavedWindows(pGroup, 0);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    while (true) {
      int64_t nextStart = pGroup->nextWindow.skey;
      int64_t curEnd = IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup) ? TRINGBUF_HEAD(&pGroup->winBuf)->range.ekey : INT64_MAX;
      int64_t ts = TMIN(nextStart, curEnd);
      if (ts > pGroup->newThreshold) {
        break;
      }
      if (ts == nextStart) {
        code = stRealtimeGroupOpenWindow(pGroup, ts, NULL, false, false);
        QUERY_CHECK_CODE(code, lino, _end);
        TRINGBUF_HEAD(&pGroup->winBuf)->wrownum = 0;
      }
      if (ts == curEnd) {
        code = stRealtimeGroupCloseWindow(pGroup, NULL, false);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
  }

#if !TRIGGER_USE_HISTORY_META
  if (pTask->fillHistory) {
    void *px = tSimpleHashGet(pTask->pHistoryCutoffTime, &pGroup->gid, sizeof(int64_t));
    if (px != NULL && pGroup->newThreshold == *(int64_t *)px && IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup) &&
        (pTask->calcEventType & STRIGGER_EVENT_WINDOW_CLOSE)) {
      SSTriggerWindow *pHead = TRINGBUF_HEAD(&pGroup->winBuf);
      SSTriggerWindow *p = pHead;
      do {
        SSTriggerCalcParam param = {
            .triggerTime = taosGetTimestampNs(),
            .wstart = p->range.skey,
            .wend = p->range.ekey,
            .wduration = p->range.ekey - p->range.skey,
            .wrownum = (p == pHead) ? p->wrownum : (pHead->wrownum - p->wrownum),
        };
        TRINGBUF_MOVE_NEXT(&pGroup->winBuf, p);
        void *px = taosArrayPush(pGroup->pPendingCalcParams, &param);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      } while (p != TRINGBUF_TAIL(&pGroup->winBuf));
    }
  }
#endif

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stRealtimeGroupDoSessionCheck(SSTriggerRealtimeGroup *pGroup) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;
  bool                      readAllData = false;
  bool                      allTableProcessed = false;
  bool                      needFetchData = false;

  if (!pContext->reenterCheck) {
    // save initial windows at the first check
    code = stRealtimeGroupSaveInitWindow(pGroup, pContext->pInitWindows);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (pTask->placeHolderBitmap & PLACE_HOLDER_WROWNUM) {
    readAllData = true;
  } else if (pTask->triggerFilter != NULL) {
    readAllData = true;
  }

  while (!allTableProcessed && !needFetchData) {
    if (!readAllData) {
      // use table metadatas to accelerate the session window check
      if (IS_TRIGGER_TIMESTAMP_SORTER_EMPTY(pContext->pSorter)) {
        // save unclosed window of the previous table to merge
        while (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
          code = stRealtimeGroupCloseWindow(pGroup, NULL, true);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        stTimestampSorterReset(pContext->pSorter);
        pContext->pCurTableMeta = tSimpleHashIterate(pGroup->pTableMetas, pContext->pCurTableMeta, &pContext->tbIter);
        if (pContext->pCurTableMeta == NULL) {
          allTableProcessed = true;
          break;
        }
        code = stRealtimeGroupRestoreInitWindow(pGroup, pContext->pInitWindows);
        QUERY_CHECK_CODE(code, lino, _end);
        STimeWindow range = {.skey = pGroup->oldThreshold + 1, .ekey = pGroup->newThreshold};
        code =
            stTimestampSorterSetSortInfo(pContext->pSorter, &range, pContext->pCurTableMeta->tbUid, pTask->trigTsIndex);
        QUERY_CHECK_CODE(code, lino, _end);
        code = stTimestampSorterSetMetaDatas(pContext->pSorter, pContext->pCurTableMeta);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      int64_t ts = IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup) ? TRINGBUF_HEAD(&pGroup->winBuf)->range.ekey : INT64_MIN;
      int64_t lastTs = ts, nextTs = ts;
      code = stTimestampSorterForwardTs(pContext->pSorter, ts, pTask->gap, &lastTs, &nextTs);
      QUERY_CHECK_CODE(code, lino, _end);
      if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
        TRINGBUF_HEAD(&pGroup->winBuf)->range.ekey = lastTs;
      }
      if (nextTs == INT64_MAX) {
        if (!IS_TRIGGER_TIMESTAMP_SORTER_EMPTY(pContext->pSorter)) {
          needFetchData = true;
          code = stTimestampSorterGetMetaToFetch(pContext->pSorter, &pContext->pMetaToFetch);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        continue;
      }
      if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
        code = stRealtimeGroupCloseWindow(pGroup, NULL, true);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      code = stRealtimeGroupOpenWindow(pGroup, nextTs, NULL, true, true);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      // read all data of the current table
      SSDataBlock *pDataBlock = NULL;
      int32_t      startIdx = 0;
      int32_t      endIdx = 0;
      code = stRealtimeGroupGetDataBlock(pGroup, true, &pDataBlock, &startIdx, &endIdx, &allTableProcessed,
                                         &needFetchData);
      QUERY_CHECK_CODE(code, lino, _end);
      if (allTableProcessed || needFetchData) {
        break;
      }
      SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pTask->trigTsIndex);
      QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
      int64_t *pTsData = (int64_t *)pTsCol->pData;
      for (int32_t r = startIdx; r < endIdx; r++) {
        int64_t          ts = pTsData[r];
        SSTriggerWindow *pCurWin = TRINGBUF_HEAD(&pGroup->winBuf);
        if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup) && pCurWin->range.ekey + pTask->gap >= ts) {
          pCurWin->range.ekey = ts;
          pCurWin->wrownum++;
        } else {
          if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
            code = stRealtimeGroupCloseWindow(pGroup, NULL, true);
            QUERY_CHECK_CODE(code, lino, _end);
          }
          code = stRealtimeGroupOpenWindow(pGroup, ts, NULL, true, true);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
    }
  }

  if (allTableProcessed) {
    code = stRealtimeGroupMergeSavedWindows(pGroup, pTask->gap);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stRealtimeGroupDoCountCheck(SSTriggerRealtimeGroup *pGroup) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;
  bool                      readAllData = false;
  bool                      allTableProcessed = false;
  bool                      needFetchData = false;

  if (pTask->triggerFilter != NULL) {
    readAllData = true;
  } else if (pTask->isVirtualTable) {
    readAllData = true;
  }

#define ALIGN_UP(x, b) (((x) + (b) - 1) / (b) * (b))
  while (!allTableProcessed && !needFetchData) {
    if (!readAllData) {
      // use table metadatas to accelerate the count window check
      if (IS_TRIGGER_TIMESTAMP_SORTER_EMPTY(pContext->pSorter)) {
        stTimestampSorterReset(pContext->pSorter);
        pContext->pCurTableMeta = tSimpleHashIterate(pGroup->pTableMetas, pContext->pCurTableMeta, &pContext->tbIter);
        if (pContext->pCurTableMeta == NULL) {
          // actually, it has only one table
          allTableProcessed = true;
          break;
        }
        STimeWindow range = {.skey = pGroup->oldThreshold + 1, .ekey = pGroup->newThreshold};
        code =
            stTimestampSorterSetSortInfo(pContext->pSorter, &range, pContext->pCurTableMeta->tbUid, pTask->trigTsIndex);
        QUERY_CHECK_CODE(code, lino, _end);
        code = stTimestampSorterSetMetaDatas(pContext->pSorter, pContext->pCurTableMeta);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      int64_t skipped = 0;
      int64_t lastTs = INT64_MIN;
      int64_t nrowsCurWin = IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup) ? TRINGBUF_HEAD(&pGroup->winBuf)->wrownum : 0;
      int64_t nrowsNextWstart = ALIGN_UP(nrowsCurWin, pTask->windowSliding) + 1;
      int64_t nrowsToSkip = TMIN(nrowsNextWstart, pTask->windowCount) - nrowsCurWin;
      code = stTimestampSorterForwardNrows(pContext->pSorter, nrowsToSkip, &skipped, &lastTs);
      QUERY_CHECK_CODE(code, lino, _end);
      if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup) && skipped > 0) {
        TRINGBUF_HEAD(&pGroup->winBuf)->range.ekey = lastTs;
        TRINGBUF_HEAD(&pGroup->winBuf)->wrownum += skipped;
      }
      if (skipped < nrowsToSkip) {
        if (!IS_TRIGGER_TIMESTAMP_SORTER_EMPTY(pContext->pSorter)) {
          needFetchData = true;
          code = stTimestampSorterGetMetaToFetch(pContext->pSorter, &pContext->pMetaToFetch);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        continue;
      }
      if (nrowsCurWin + skipped == nrowsNextWstart) {
        code = stRealtimeGroupOpenWindow(pGroup, lastTs, NULL, false, true);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      QUERY_CHECK_CONDITION(IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      if (TRINGBUF_HEAD(&pGroup->winBuf)->wrownum == pTask->windowCount) {
        code = stRealtimeGroupCloseWindow(pGroup, NULL, false);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    } else {
      // read all data of the current table
      SSDataBlock *pDataBlock = NULL;
      int32_t      startIdx = 0;
      int32_t      endIdx = 0;
      code = stRealtimeGroupGetDataBlock(pGroup, false, &pDataBlock, &startIdx, &endIdx, &allTableProcessed,
                                         &needFetchData);
      QUERY_CHECK_CODE(code, lino, _end);
      if (allTableProcessed || needFetchData) {
        break;
      }
      SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pTask->trigTsIndex);
      QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
      int64_t *pTsData = (int64_t *)pTsCol->pData;
      for (int32_t r = startIdx; r < endIdx;) {
        int64_t nrowsCurWin = IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup) ? TRINGBUF_HEAD(&pGroup->winBuf)->wrownum : 0;
        int64_t nrowsNextWstart = ALIGN_UP(nrowsCurWin, pTask->windowSliding) + 1;
        int64_t nrowsToSkip = TMIN(nrowsNextWstart, pTask->windowCount) - nrowsCurWin;
        int64_t skipped = TMIN(nrowsToSkip, endIdx - r);
        int64_t lastTs = pTsData[r + skipped - 1];
        r += skipped;
        if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup) && skipped > 0) {
          TRINGBUF_HEAD(&pGroup->winBuf)->range.ekey = lastTs;
          TRINGBUF_HEAD(&pGroup->winBuf)->wrownum += skipped;
        }
        if (skipped == nrowsNextWstart) {
          code = stRealtimeGroupOpenWindow(pGroup, lastTs, NULL, false, true);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        QUERY_CHECK_CONDITION(IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        if (TRINGBUF_HEAD(&pGroup->winBuf)->wrownum == pTask->windowCount) {
          code = stRealtimeGroupCloseWindow(pGroup, NULL, false);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stRealtimeGroupDoStateCheck(SSTriggerRealtimeGroup *pGroup) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;
  bool                      allTableProcessed = false;
  bool                      needFetchData = false;
  char                     *pExtraNotifyContent = NULL;

  while (!allTableProcessed && !needFetchData) {
    //  read all data of the current table
    SSDataBlock *pDataBlock = NULL;
    int32_t      startIdx = 0;
    int32_t      endIdx = 0;
    code =
        stRealtimeGroupGetDataBlock(pGroup, false, &pDataBlock, &startIdx, &endIdx, &allTableProcessed, &needFetchData);
    QUERY_CHECK_CODE(code, lino, _end);
    if (allTableProcessed || needFetchData) {
      break;
    }
    SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pTask->trigTsIndex);
    QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
    int64_t         *pTsData = (int64_t *)pTsCol->pData;
    SColumnInfoData *pStateCol = taosArrayGet(pDataBlock->pDataBlock, pTask->stateSlotId);
    QUERY_CHECK_NULL(pStateCol, code, lino, _end, terrno);
    bool  isVarType = IS_VAR_DATA_TYPE(pStateCol->info.type);
    void *pStateData = isVarType ? (void *)pGroup->stateVal.pData : (void *)&pGroup->stateVal.val;
    if (IS_TRIGGER_GROUP_NONE_WINDOW(pGroup)) {
      // initialize state value
      SValue *pStateVal = &pGroup->stateVal;
      pStateVal->type = pStateCol->info.type;
      if (isVarType) {
        pStateVal->nData = pStateCol->info.bytes;
        pStateVal->pData = taosMemoryCalloc(pStateVal->nData, 1);
        QUERY_CHECK_CONDITION(pStateVal->pData, code, lino, _end, terrno);
        pStateData = pStateVal->pData;
      }

      // open the first window
      char   *newVal = colDataGetData(pStateCol, startIdx);
      int32_t bytes = isVarType ? varDataTLen(newVal) : pStateCol->info.bytes;
      if (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_OPEN) {
        code = streamBuildStateNotifyContent(STRIGGER_EVENT_WINDOW_OPEN, pStateCol->info.type, NULL, newVal,
                                             &pExtraNotifyContent);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      code = stRealtimeGroupOpenWindow(pGroup, pTsData[startIdx], &pExtraNotifyContent, false, true);
      QUERY_CHECK_CODE(code, lino, _end);
      memcpy(pStateData, newVal, bytes);
      startIdx++;
    }
    for (int32_t r = startIdx; r < endIdx; r++) {
      char   *newVal = colDataGetData(pStateCol, r);
      int32_t bytes = isVarType ? varDataTLen(newVal) : pStateCol->info.bytes;
      if (memcmp(pStateData, newVal, bytes) == 0) {
        TRINGBUF_HEAD(&pGroup->winBuf)->wrownum++;
        TRINGBUF_HEAD(&pGroup->winBuf)->range.ekey = pTsData[r];
      } else {
        if (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_CLOSE) {
          code = streamBuildStateNotifyContent(STRIGGER_EVENT_WINDOW_CLOSE, pStateCol->info.type, pStateData, newVal,
                                               &pExtraNotifyContent);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        code = stRealtimeGroupCloseWindow(pGroup, &pExtraNotifyContent, false);
        QUERY_CHECK_CODE(code, lino, _end);
        if (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_OPEN) {
          code = streamBuildStateNotifyContent(STRIGGER_EVENT_WINDOW_OPEN, pStateCol->info.type, pStateData, newVal,
                                               &pExtraNotifyContent);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        code = stRealtimeGroupOpenWindow(pGroup, pTsData[r], &pExtraNotifyContent, false, true);
        QUERY_CHECK_CODE(code, lino, _end);
        memcpy(pStateData, newVal, bytes);
      }
    }
  }

_end:
  if (pExtraNotifyContent != NULL) {
    taosMemoryFreeClear(pExtraNotifyContent);
  }
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stRealtimeGroupDoEventCheck(SSTriggerRealtimeGroup *pGroup) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;
  bool                      allTableProcessed = false;
  bool                      needFetchData = false;
  char                     *pExtraNotifyContent = NULL;
  SColumnInfoData          *psCol = NULL;
  SColumnInfoData          *peCol = NULL;

  while (!allTableProcessed && !needFetchData) {
    //  read all data of the current table
    SSDataBlock *pDataBlock = NULL;
    int32_t      startIdx = 0;
    int32_t      endIdx = 0;
    code =
        stRealtimeGroupGetDataBlock(pGroup, false, &pDataBlock, &startIdx, &endIdx, &allTableProcessed, &needFetchData);
    QUERY_CHECK_CODE(code, lino, _end);
    if (allTableProcessed || needFetchData) {
      break;
    }
    SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pTask->trigTsIndex);
    QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
    int64_t *pTsData = (int64_t *)pTsCol->pData;
    bool    *ps = NULL, *pe = NULL;
    psCol = NULL;
    peCol = NULL;

    for (int32_t r = startIdx; r < endIdx; r++) {
      if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
        TRINGBUF_HEAD(&pGroup->winBuf)->range.ekey = pTsData[r];
        TRINGBUF_HEAD(&pGroup->winBuf)->wrownum++;
      } else {
        if (ps == NULL) {
          SFilterColumnParam param = {.numOfCols = taosArrayGetSize(pDataBlock->pDataBlock),
                                      .pDataBlock = pDataBlock->pDataBlock};
          code = filterSetDataFromSlotId(pContext->pStartCond, &param);
          QUERY_CHECK_CODE(code, lino, _end);
          int32_t status = 0;
          code = filterExecute(pContext->pStartCond, pDataBlock, &psCol, NULL, param.numOfCols, &status);
          QUERY_CHECK_CODE(code, lino, _end);
          ps = (bool *)psCol->pData;
        }
        if (ps[r]) {
          if (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_OPEN) {
            code = streamBuildEventNotifyContent(pDataBlock, pTask->pStartCondCols, r, &pExtraNotifyContent);
            QUERY_CHECK_CODE(code, lino, _end);
          }
          code = stRealtimeGroupOpenWindow(pGroup, pTsData[r], &pExtraNotifyContent, false, true);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
      if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
        if (pe == NULL) {
          SFilterColumnParam param = {.numOfCols = taosArrayGetSize(pDataBlock->pDataBlock),
                                      .pDataBlock = pDataBlock->pDataBlock};
          code = filterSetDataFromSlotId(pContext->pEndCond, &param);
          QUERY_CHECK_CODE(code, lino, _end);
          int32_t status = 0;
          code = filterExecute(pContext->pEndCond, pDataBlock, &peCol, NULL, param.numOfCols, &status);
          QUERY_CHECK_CODE(code, lino, _end);
          pe = (bool *)peCol->pData;
        }
        if (pe[r]) {
          if (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_CLOSE) {
            code = streamBuildEventNotifyContent(pDataBlock, pTask->pEndCondCols, r, &pExtraNotifyContent);
            QUERY_CHECK_CODE(code, lino, _end);
          }
          code = stRealtimeGroupCloseWindow(pGroup, &pExtraNotifyContent, false);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
    }

    colDataDestroy(psCol);
    taosMemoryFreeClear(psCol);
    colDataDestroy(peCol);
    taosMemoryFreeClear(peCol);
  }

_end:

  colDataDestroy(psCol);
  taosMemoryFreeClear(psCol);

  if (pExtraNotifyContent != NULL) {
    taosMemoryFreeClear(pExtraNotifyContent);
  }
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stRealtimeGroupCheck(SSTriggerRealtimeGroup *pGroup) {
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  if (pGroup->oldThreshold == pGroup->newThreshold) {
    return TSDB_CODE_SUCCESS;
  }

  switch (pTask->triggerType) {
    case STREAM_TRIGGER_PERIOD:
      return stRealtimeGroupDoPeriodCheck(pGroup);

    case STREAM_TRIGGER_SLIDING:
      return stRealtimeGroupDoSlidingCheck(pGroup);

    case STREAM_TRIGGER_SESSION:
      return stRealtimeGroupDoSessionCheck(pGroup);

    case STREAM_TRIGGER_COUNT:
      return stRealtimeGroupDoCountCheck(pGroup);

    case STREAM_TRIGGER_STATE:
      return stRealtimeGroupDoStateCheck(pGroup);

    case STREAM_TRIGGER_EVENT:
      return stRealtimeGroupDoEventCheck(pGroup);

    default: {
      ST_TASK_ELOG("invalid stream trigger type %d at %s:%d", pTask->triggerType, __func__, __LINE__);
      return TSDB_CODE_INVALID_PARA;
    }
  }
}

static int32_t stHistoryGroupInit(SSTriggerHistoryGroup *pGroup, SSTriggerHistoryContext *pContext, int64_t gid) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pContext->pTask;

  pGroup->pContext = pContext;
  pGroup->gid = gid;

  pGroup->pTableMetas = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  QUERY_CHECK_NULL(pGroup->pTableMetas, code, lino, _end, terrno);
  tSimpleHashSetFreeFp(pGroup->pTableMetas, stRealtimeGroupDestroyTableMeta);

  TRINGBUF_INIT(&pGroup->winBuf);

  if (pContext->pTask->isVirtualTable) {
    pGroup->pVirTableInfos = taosArrayInit(0, POINTER_BYTES);
    QUERY_CHECK_NULL(pGroup->pVirTableInfos, code, lino, _end, terrno);
    int32_t                iter = 0;
    SSTriggerVirTableInfo *pInfo = tSimpleHashIterate(pTask->pVirTableInfos, NULL, &iter);
    while (pInfo != NULL) {
      if (pInfo->tbGid == gid) {
        void *px = taosArrayPush(pGroup->pVirTableInfos, &pInfo);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
        int32_t nTrigCols = taosArrayGetSize(pInfo->pTrigColRefs);
        for (int32_t i = 0; i < nTrigCols; i++) {
          SSTriggerTableColRef *pColRef = TARRAY_GET_ELEM(pInfo->pTrigColRefs, i);
          SSTriggerTableMeta   *pTableMeta = tSimpleHashGet(pGroup->pTableMetas, &pColRef->otbUid, sizeof(int64_t));
          if (pTableMeta == NULL) {
            SSTriggerTableMeta newTableMeta = {.tbUid = pColRef->otbUid, .vgId = pColRef->otbVgId};
            code = tSimpleHashPut(pGroup->pTableMetas, &pColRef->otbUid, sizeof(int64_t), &newTableMeta,
                                  sizeof(SSTriggerTableMeta));
            QUERY_CHECK_CODE(code, lino, _end);
          }
        }
        int32_t nCalcCols = taosArrayGetSize(pInfo->pCalcColRefs);
        for (int32_t i = 0; i < nCalcCols; i++) {
          SSTriggerTableColRef *pColRef = TARRAY_GET_ELEM(pInfo->pCalcColRefs, i);
          SSTriggerTableMeta   *pTableMeta = tSimpleHashGet(pGroup->pTableMetas, &pColRef->otbUid, sizeof(int64_t));
          if (pTableMeta == NULL) {
            SSTriggerTableMeta newTableMeta = {.tbUid = pColRef->otbUid, .vgId = pColRef->otbVgId};
            code = tSimpleHashPut(pGroup->pTableMetas, &pColRef->otbUid, sizeof(int64_t), &newTableMeta,
                                  sizeof(SSTriggerTableMeta));
            QUERY_CHECK_CODE(code, lino, _end);
          }
        }
      }
      pInfo = tSimpleHashIterate(pTask->pVirTableInfos, pInfo, &iter);
    }
  }

  pGroup->pPendingCalcReqs = taosArrayInit(0, sizeof(SSTriggerCalcParam));
  QUERY_CHECK_NULL(pGroup->pPendingCalcReqs, code, lino, _end, terrno);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void stHistoryGroupDestroy(void *ptr) {
  SSTriggerHistoryGroup **ppGroup = ptr;
  if (ppGroup == NULL || *ppGroup == NULL) {
    return;
  }

  SSTriggerHistoryGroup *pGroup = *ppGroup;
  if (pGroup->pVirTableInfos != NULL) {
    taosArrayDestroy(pGroup->pVirTableInfos);
  }
  if (pGroup->pTableMetas != NULL) {
    tSimpleHashCleanup(pGroup->pTableMetas);
    pGroup->pTableMetas = NULL;
  }

  TRINGBUF_DESTROY(&pGroup->winBuf);
  if ((pGroup->pContext->pTask->triggerType == STREAM_TRIGGER_STATE) && IS_VAR_DATA_TYPE(pGroup->stateVal.type)) {
    taosMemoryFreeClear(pGroup->stateVal.pData);
  }

  if (pGroup->pPendingCalcReqs) {
    taosArrayDestroy(pGroup->pPendingCalcReqs);
    pGroup->pPendingCalcReqs = NULL;
  }

  taosMemFreeClear(*ppGroup);
}

static void stHistoryGroupClearTempState(SSTriggerHistoryGroup *pGroup) {
  pGroup->tbIter = 0;
  pGroup->pCurVirTable = NULL;
  pGroup->pCurTableMeta = NULL;

  SSTriggerHistoryContext *pContext = pGroup->pContext;
  stTimestampSorterReset(pContext->pSorter);
  stVtableMergerReset(pContext->pMerger);
  pContext->pMetaToFetch = NULL;
  pContext->pColRefToFetch = NULL;

  if (pContext->pSavedWindows != NULL) {
    taosArrayClear(pContext->pSavedWindows);
  }
  if (pContext->pInitWindows != NULL) {
    taosArrayClear(pContext->pInitWindows);
  }

  if (pContext->pNotifyParams != NULL) {
    taosArrayClearEx(pContext->pNotifyParams, tDestroySSTriggerCalcParam);
  }
  pContext->pParamToFetch = NULL;
}

static int32_t stHistoryGroupAddMetaDatas(SSTriggerHistoryGroup *pGroup, SSDataBlock *pMetaDataBlock, bool *pAdded) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SSTriggerHistoryContext *pContext = pGroup->pContext;
  SStreamTriggerTask      *pTask = pContext->pTask;
  SSTriggerTableMeta      *pTableMeta = NULL;

  QUERY_CHECK_NULL(pMetaDataBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(pTask->triggerType != STREAM_TRIGGER_PERIOD, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *pAdded = false;

  int32_t          iCol = 0;
  SColumnInfoData *pSkeyCol = taosArrayGet(pMetaDataBlock->pDataBlock, iCol++);
  QUERY_CHECK_NULL(pSkeyCol, code, lino, _end, terrno);
  int64_t         *pSkeys = (int64_t *)pSkeyCol->pData;
  SColumnInfoData *pEkeyCol = taosArrayGet(pMetaDataBlock->pDataBlock, iCol++);
  QUERY_CHECK_NULL(pEkeyCol, code, lino, _end, terrno);
  int64_t         *pEkeys = (int64_t *)pEkeyCol->pData;
  SColumnInfoData *pUidCol = taosArrayGet(pMetaDataBlock->pDataBlock, iCol++);
  QUERY_CHECK_NULL(pUidCol, code, lino, _end, terrno);
  int64_t *pUids = (int64_t *)pUidCol->pData;
  int64_t *pGids = NULL;
  if (!pTask->isVirtualTable) {
    SColumnInfoData *pGidCol = taosArrayGet(pMetaDataBlock->pDataBlock, iCol++);
    QUERY_CHECK_NULL(pGidCol, code, lino, _end, terrno);
    pGids = (int64_t *)pGidCol->pData;
  }
  SColumnInfoData *pNrowsCol = taosArrayGet(pMetaDataBlock->pDataBlock, iCol++);
  QUERY_CHECK_NULL(pNrowsCol, code, lino, _end, terrno);
  int64_t *pNrows = (int64_t *)pNrowsCol->pData;

  int32_t numNewMeta = blockDataGetNumOfRows(pMetaDataBlock);
  for (int32_t i = 0; i < numNewMeta; i++) {
    bool inGroup = false;
    if (pTask->isVirtualTable) {
      if (pTableMeta == NULL || pTableMeta->tbUid != pUids[i]) {
        pTableMeta = tSimpleHashGet(pGroup->pTableMetas, &pUids[i], sizeof(int64_t));
      }
      inGroup = (pTableMeta != NULL);
    } else {
      inGroup = (pGids[i] == pGroup->gid);
    }
    if (!inGroup) {
      continue;
    }

    *pAdded = true;

    if (pTableMeta == NULL || pTableMeta->tbUid != pUids[i]) {
      pTableMeta = tSimpleHashGet(pGroup->pTableMetas, &pUids[i], sizeof(int64_t));
      if (pTableMeta == NULL) {
        SStreamTaskAddr *pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
        QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
        SSTriggerTableMeta newTableMeta = {.tbUid = pUids[i], .vgId = pReader->nodeId};
        code =
            tSimpleHashPut(pGroup->pTableMetas, &pUids[i], sizeof(int64_t), &newTableMeta, sizeof(SSTriggerTableMeta));
        QUERY_CHECK_CODE(code, lino, _end);
        pTableMeta = tSimpleHashGet(pGroup->pTableMetas, &pUids[i], sizeof(int64_t));
        QUERY_CHECK_NULL(pTableMeta, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      }
    }
    if (pTableMeta->pMetas == NULL) {
      pTableMeta->pMetas = taosArrayInit(0, sizeof(SSTriggerMetaData));
      QUERY_CHECK_NULL(pTableMeta->pMetas, code, lino, _end, terrno);
    }
    SSTriggerMetaData *pNewMeta = taosArrayReserve(pTableMeta->pMetas, 1);
    QUERY_CHECK_NULL(pNewMeta, code, lino, _end, terrno);
    pNewMeta->skey = pSkeys[i];
    pNewMeta->ekey = pEkeys[i];
    pNewMeta->ver = 0;
    pNewMeta->nrows = pNrows[i];
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stHistoryGroupOpenWindow(SSTriggerHistoryGroup *pGroup, int64_t ts, char **ppExtraNotifyContent,
                                        bool saveWindow, bool hasStartData) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SSTriggerHistoryContext *pContext = pGroup->pContext;
  SStreamTriggerTask      *pTask = pContext->pTask;
  SSTriggerWindow          newWindow = {0};
  SSTriggerCalcParam       param = {0};

  bool    needCalc = (pTask->calcEventType & STRIGGER_EVENT_WINDOW_OPEN);
  bool    needNotify = pTask->notifyHistory && (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_OPEN);
  int64_t now = taosGetTimestampNs();
  if (needCalc || needNotify) {
    param.triggerTime = now;
    param.notifyType = needNotify ? STRIGGER_EVENT_WINDOW_OPEN : STRIGGER_EVENT_WINDOW_NONE;
    param.extraNotifyContent = ppExtraNotifyContent ? *ppExtraNotifyContent : NULL;
  }
  newWindow.prevProcTime = now;
  newWindow.wrownum = hasStartData ? 1 : 0;
  if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
    newWindow.wrownum = TRINGBUF_HEAD(&pGroup->winBuf)->wrownum - newWindow.wrownum;
  }

  switch (pTask->triggerType) {
    case STREAM_TRIGGER_SLIDING: {
      if (pTask->interval.interval > 0) {
        // interval window trigger
        if (IS_TRIGGER_GROUP_NONE_WINDOW(pGroup)) {
          pGroup->nextWindow = stTriggerTaskGetIntervalWindow(pTask, ts);
        }
        newWindow.range = pGroup->nextWindow;
        stTriggerTaskNextIntervalWindow(pTask, &pGroup->nextWindow);
        if (needCalc || needNotify) {
          STimeWindow prevWindow = newWindow.range;
          stTriggerTaskPrevIntervalWindow(pTask, &prevWindow);
          param.wstart = newWindow.range.skey;
          param.wend = newWindow.range.ekey;
          param.wduration = param.wend - param.wstart;
          param.prevTs = prevWindow.skey;
          param.currentTs = newWindow.range.skey;
          param.nextTs = pGroup->nextWindow.skey;
        }
        break;
      }
      // sliding trigger works the same as period trigger
    }
    case STREAM_TRIGGER_PERIOD: {
      QUERY_CHECK_CONDITION(!IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INVALID_PARA);
      if (IS_TRIGGER_GROUP_NONE_WINDOW(pGroup)) {
        pGroup->nextWindow = stTriggerTaskGetPeriodWindow(pTask, ts);
      }
      newWindow.range = pGroup->nextWindow;
      stTriggerTaskNextPeriodWindow(pTask, &pGroup->nextWindow);
      QUERY_CHECK_CONDITION(!needCalc && !needNotify, code, lino, _end, TSDB_CODE_INVALID_PARA);
      break;
    }
    case STREAM_TRIGGER_SESSION:
    case STREAM_TRIGGER_STATE:
    case STREAM_TRIGGER_EVENT: {
      QUERY_CHECK_CONDITION(!IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INVALID_PARA);
      // works the same as count window trigger
    }
    case STREAM_TRIGGER_COUNT: {
      newWindow.range = (STimeWindow){.skey = ts, .ekey = ts};
      if (needCalc || needNotify) {
        param.wstart = ts;
        param.wend = ts;
      }
      break;
    }

    default: {
      ST_TASK_ELOG("invalid stream trigger type %d at %s:%d", pTask->triggerType, __func__, __LINE__);
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  code = TRINGBUF_APPEND(&pGroup->winBuf, newWindow);
  QUERY_CHECK_CODE(code, lino, _end);

  if (saveWindow) {
    // only save window when close window
  } else if (needCalc) {
    void *px = taosArrayPush(pContext->pCalcReq->params, &param);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
  } else if (needNotify) {
    void *px = taosArrayPush(pContext->pNotifyParams, &param);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
  } else {
    QUERY_CHECK_CONDITION(ppExtraNotifyContent == NULL || *ppExtraNotifyContent == NULL, code, lino, _end,
                          TSDB_CODE_INVALID_PARA);
  }

  if (ppExtraNotifyContent) {
    *ppExtraNotifyContent = NULL;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stHistoryGroupCloseWindow(SSTriggerHistoryGroup *pGroup, char **ppExtraNotifyContent, bool saveWindow) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SSTriggerHistoryContext *pContext = pGroup->pContext;
  SStreamTriggerTask      *pTask = pContext->pTask;
  SSTriggerWindow         *pCurWindow = NULL;
  SSTriggerCalcParam       param = {0};
  bool                     needCalc = false;
  bool                     needNotify = false;

  if (IS_TRIGGER_GROUP_NONE_WINDOW(pGroup)) {
    goto _end;
  }
  QUERY_CHECK_CONDITION(IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INVALID_PARA);

  if (pTask->calcEventType & STRIGGER_EVENT_WINDOW_CLOSE) {
    needCalc = true;
  }
  if (pTask->notifyHistory && (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_CLOSE)) {
    needNotify = true;
  }
  if (needCalc || needNotify) {
    param.triggerTime = taosGetTimestampNs();
    param.extraNotifyContent = ppExtraNotifyContent ? *ppExtraNotifyContent : NULL;
    if (needNotify) {
      if ((pTask->triggerType == STREAM_TRIGGER_PERIOD) ||
          (pTask->triggerType == STREAM_TRIGGER_SLIDING && pTask->interval.interval == 0)) {
        param.notifyType = STRIGGER_EVENT_ON_TIME;
      } else {
        param.notifyType = STRIGGER_EVENT_WINDOW_CLOSE;
      }
    }
  }

  pCurWindow = TRINGBUF_HEAD(&pGroup->winBuf);

  if ((pTask->triggerType == STREAM_TRIGGER_STATE &&
       pCurWindow->range.ekey - pCurWindow->range.skey < pTask->stateTrueFor) ||
      (pTask->triggerType == STREAM_TRIGGER_EVENT &&
       pCurWindow->range.ekey - pCurWindow->range.skey < pTask->eventTrueFor)) {
    // check TRUE FOR condition
    needCalc = needNotify = false;
  }

  switch (pTask->triggerType) {
    case STREAM_TRIGGER_PERIOD: {
      SInterval *pInterval = &pTask->interval;
      QUERY_CHECK_CONDITION(needCalc || needNotify, code, lino, _end, TSDB_CODE_INVALID_PARA);
      param.prevLocalTime = pCurWindow->range.skey - 1;
      param.triggerTime = pCurWindow->range.ekey;
      param.nextLocalTime = pGroup->nextWindow.ekey;
      break;
    }
    case STREAM_TRIGGER_SLIDING: {
      SInterval *pInterval = &pTask->interval;
      if (pInterval->interval == 0) {
        // sliding trigger
        QUERY_CHECK_CONDITION(needCalc || needNotify, code, lino, _end, TSDB_CODE_INVALID_PARA);
        param.prevTs = pCurWindow->range.skey - 1;
        param.currentTs = pCurWindow->range.ekey;
        param.nextTs = pGroup->nextWindow.ekey;
      } else {
        STimeWindow prevWindow = pCurWindow->range;
        stTriggerTaskPrevIntervalWindow(pTask, &prevWindow);
        param.prevTs = prevWindow.ekey + 1;
        param.currentTs = pCurWindow->range.ekey + 1;
        param.nextTs = pGroup->nextWindow.ekey + 1;
      }
      // fill the param the same way as other window trigger
    }
    case STREAM_TRIGGER_SESSION:
    case STREAM_TRIGGER_COUNT:
    case STREAM_TRIGGER_STATE:
    case STREAM_TRIGGER_EVENT: {
      if (needCalc || needNotify) {
        param.wstart = pCurWindow->range.skey;
        param.wend = pCurWindow->range.ekey;
        param.wduration = param.wend - param.wstart;
        param.wrownum = pCurWindow->wrownum;
      }
      break;
    }

    default: {
      ST_TASK_ELOG("invalid stream trigger type %d at %s:%d", pTask->triggerType, __func__, __LINE__);
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  TRINGBUF_DEQUEUE(&pGroup->winBuf);
  if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
    // ajustify the following window's wrownum
    SSTriggerWindow *pHead = TRINGBUF_HEAD(&pGroup->winBuf);
    SSTriggerWindow *p = pHead;
    int32_t          bias = pHead->wrownum;
    do {
      p->wrownum -= bias;
      TRINGBUF_MOVE_NEXT(&pGroup->winBuf, p);
    } while (p != TRINGBUF_TAIL(&pGroup->winBuf));
    pHead->range.ekey = TMAX(pHead->range.ekey, pCurWindow->range.ekey);
    pHead->wrownum = pCurWindow->wrownum - bias;
  }

  if (saveWindow) {
    // skip add window for session trigger, since it will be merged after processing all tables
    void *px = taosArrayPush(pContext->pSavedWindows, pCurWindow);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
  } else if (needCalc) {
    void *px = taosArrayPush(pContext->pCalcReq->params, &param);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
  } else if (needNotify) {
    void *px = taosArrayPush(pContext->pNotifyParams, &param);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
  } else if (ppExtraNotifyContent != NULL && *ppExtraNotifyContent != NULL) {
    taosMemoryFreeClear(*ppExtraNotifyContent);
  }

  if (ppExtraNotifyContent) {
    *ppExtraNotifyContent = NULL;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stHistoryGroupSaveInitWindow(SSTriggerHistoryGroup *pGroup, SArray *pInitWindows) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SSTriggerHistoryContext *pContext = pGroup->pContext;
  SStreamTriggerTask      *pTask = pContext->pTask;

  QUERY_CHECK_NULL(pInitWindows, code, lino, _end, TSDB_CODE_INVALID_PARA);

  taosArrayClear(pInitWindows);
  if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
    SSTriggerWindow *p = TRINGBUF_HEAD(&pGroup->winBuf);
    do {
      void *px = taosArrayPush(pInitWindows, &p->range);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      TRINGBUF_MOVE_NEXT(&pGroup->winBuf, p);
    } while (p != TRINGBUF_TAIL(&pGroup->winBuf));
  }

  if (pTask->triggerType == STREAM_TRIGGER_SLIDING) {
    void *px = taosArrayPush(pInitWindows, &pGroup->nextWindow);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stHistoryGroupRestoreInitWindow(SSTriggerHistoryGroup *pGroup, SArray *pInitWindows) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SSTriggerHistoryContext *pContext = pGroup->pContext;
  SStreamTriggerTask      *pTask = pContext->pTask;

  QUERY_CHECK_CONDITION(!IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INVALID_PARA);

  int32_t nWindows = taosArrayGetSize(pInitWindows);

  if (pTask->triggerType == STREAM_TRIGGER_SLIDING) {
    QUERY_CHECK_CONDITION(nWindows > 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
    pGroup->nextWindow = *(STimeWindow *)taosArrayGetLast(pInitWindows);
    nWindows--;
  }

  for (int32_t i = 0; i < nWindows; i++) {
    STimeWindow    *pRange = TARRAY_GET_ELEM(pInitWindows, i);
    SSTriggerWindow win = {.range = *pRange};
    code = TRINGBUF_APPEND(&pGroup->winBuf, win);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stHistoryGroupMergeSavedWindows(SSTriggerHistoryGroup *pGroup, int64_t gap) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SSTriggerHistoryContext *pContext = pGroup->pContext;
  SStreamTriggerTask      *pTask = pContext->pTask;

  QUERY_CHECK_CONDITION(!IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INVALID_PARA);

  if (taosArrayGetSize(pContext->pSavedWindows) == 0) {
    goto _end;
  }

  taosArraySort(pContext->pSavedWindows, stRealtimeGroupWindowCompare);
  SSTriggerWindow *pWin = TARRAY_GET_ELEM(pContext->pSavedWindows, 0);
  for (int32_t i = 1; i < TARRAY_SIZE(pContext->pSavedWindows); i++) {
    SSTriggerWindow *pCurWin = TARRAY_GET_ELEM(pContext->pSavedWindows, i);
    if ((gap > 0 && pWin->range.ekey + gap >= pCurWin->range.skey) ||
        (gap == 0 && pWin->range.skey == pCurWin->range.skey)) {
      pWin->range.ekey = TMAX(pWin->range.ekey, pCurWin->range.ekey);
      pWin->wrownum += pCurWin->wrownum;
    } else {
      ++pWin;
      *pWin = *pCurWin;
    }
  }
  TARRAY_SIZE(pContext->pSavedWindows) = TARRAY_ELEM_IDX(pContext->pSavedWindows, pWin) + 1;

  bool    calcOpen = (pTask->calcEventType & STRIGGER_EVENT_WINDOW_OPEN);
  bool    calcClose = (pTask->calcEventType & STRIGGER_EVENT_WINDOW_CLOSE);
  bool    notifyOpen = pTask->notifyHistory && (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_OPEN);
  bool    notifyClose = pTask->notifyHistory && (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_CLOSE);
  int32_t nInitWins = taosArrayGetSize(pContext->pInitWindows);

  // trigger all window open/close events
  for (int32_t i = 0; i < TARRAY_SIZE(pContext->pSavedWindows); i++) {
    pWin = TARRAY_GET_ELEM(pContext->pSavedWindows, i);
    // window open event may have been triggered previously
    if ((calcOpen || notifyOpen) && i >= nInitWins) {
      SSTriggerCalcParam param = {.triggerTime = taosGetTimestampNs(),
                                  .notifyType = (notifyOpen ? STRIGGER_EVENT_WINDOW_OPEN : STRIGGER_EVENT_WINDOW_NONE),
                                  .wstart = pWin->range.skey,
                                  .wend = pWin->range.ekey,
                                  .wduration = pWin->range.ekey - pWin->range.skey,
                                  .wrownum = pWin->wrownum};
      if (calcOpen) {
        void *px = taosArrayPush(pContext->pCalcReq->params, &param);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      } else if (notifyOpen) {
        void *px = taosArrayPush(pContext->pNotifyParams, &param);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      }
    }

    // some window may have not been closed yet
    if (pWin->range.ekey + gap > pContext->curRange.ekey) {
      // todo(kjq): restore prevProcTime from saved init windows
      pWin->prevProcTime = taosGetTimestampNs();
      if (TRINGBUF_SIZE(&pGroup->winBuf) > 0) {
        pWin->wrownum = TRINGBUF_HEAD(&pGroup->winBuf)->wrownum - pWin->wrownum;
      }
      code = TRINGBUF_APPEND(&pGroup->winBuf, *pWin);
      QUERY_CHECK_CODE(code, lino, _end);
    } else if ((calcClose || notifyClose)) {
      SSTriggerCalcParam param = {.triggerTime = taosGetTimestampNs(),
                                  .wstart = pWin->range.skey,
                                  .wend = pWin->range.ekey,
                                  .wduration = pWin->range.ekey - pWin->range.skey,
                                  .wrownum = pWin->wrownum};
      if (notifyClose) {
        if ((pTask->triggerType == STREAM_TRIGGER_PERIOD) ||
            (pTask->triggerType == STREAM_TRIGGER_SLIDING && pTask->interval.interval == 0)) {
          param.notifyType = STRIGGER_EVENT_ON_TIME;
        } else {
          param.notifyType = STRIGGER_EVENT_WINDOW_CLOSE;
        }
      }
      if (calcClose) {
        void *px = taosArrayPush(pContext->pCalcReq->params, &param);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      } else if (notifyClose) {
        void *px = taosArrayPush(pContext->pNotifyParams, &param);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      }
    }
  }

  if (pTask->triggerType == STREAM_TRIGGER_SLIDING) {
    pWin = TARRAY_GET_ELEM(pContext->pSavedWindows, TARRAY_SIZE(pContext->pSavedWindows) - 1);
    pGroup->nextWindow = pWin->range;
    if (pTask->interval.interval > 0) {
      stTriggerTaskNextIntervalWindow(pTask, &pGroup->nextWindow);
    } else {
      stTriggerTaskNextPeriodWindow(pTask, &pGroup->nextWindow);
    }
  }

  taosArrayClear(pContext->pSavedWindows);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stHistoryGroupGetDataBlock(SSTriggerHistoryGroup *pGroup, bool saveWindow, SSDataBlock **ppDataBlock,
                                          int32_t *pStartIdx, int32_t *pEndIdx, bool *pAllTableProcessed,
                                          bool *pNeedFetchData) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SSTriggerHistoryContext *pContext = pGroup->pContext;
  SStreamTriggerTask      *pTask = pContext->pTask;
  bool                     isCalcData = (pContext->status == STRIGGER_CONTEXT_SEND_CALC_REQ);

  *pAllTableProcessed = false;
  *pNeedFetchData = false;

  if (isCalcData && !pTask->isVirtualTable) {
    if (pContext->pFetchedDataBlock != NULL) {
      int32_t nrows = blockDataGetNumOfRows(pContext->pFetchedDataBlock);
      if (nrows == 0) {
        *ppDataBlock = NULL;
        *pStartIdx = *pEndIdx = 0;
        *pAllTableProcessed = true;
      } else {
        *ppDataBlock = pContext->pFetchedDataBlock;
        *pStartIdx = 0;
        *pEndIdx = nrows;
        pContext->pFetchedDataBlock = NULL;
      }
    } else {
      *ppDataBlock = NULL;
      *pStartIdx = *pEndIdx = 0;
      *pNeedFetchData = true;
    }
    goto _end;
  } else if (!pContext->needTsdbMeta) {
    if (pContext->pFetchedDataBlock != NULL) {
      int32_t nrows = blockDataGetNumOfRows(pContext->pFetchedDataBlock);
      *ppDataBlock = pContext->pFetchedDataBlock;
      *pStartIdx = 0;
      *pEndIdx = nrows;
      pContext->pFetchedDataBlock = NULL;
    } else {
      *ppDataBlock = NULL;
      *pStartIdx = *pEndIdx = 0;
      *pAllTableProcessed = true;
    }
    goto _end;
  }

  while (!*pAllTableProcessed && !*pNeedFetchData) {
    if (!pTask->isVirtualTable) {
      if (IS_TRIGGER_TIMESTAMP_SORTER_EMPTY(pContext->pSorter)) {
        while (saveWindow && IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
          code = stHistoryGroupCloseWindow(pGroup, NULL, saveWindow);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        stTimestampSorterReset(pContext->pSorter);
        pGroup->pCurTableMeta = tSimpleHashIterate(pGroup->pTableMetas, pGroup->pCurTableMeta, &pGroup->tbIter);
        if (pGroup->pCurTableMeta == NULL) {
          *pAllTableProcessed = true;
          break;
        }
        if (saveWindow) {
          code = stHistoryGroupRestoreInitWindow(pGroup, pContext->pInitWindows);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        STimeWindow range = {.skey = INT64_MIN, .ekey = INT64_MAX - 1};
        if (pContext->status == STRIGGER_CONTEXT_CHECK_CONDITION) {
          range = pContext->curRange;
        } else if (pContext->status == STRIGGER_CONTEXT_SEND_CALC_REQ) {
          if (pTask->triggerType != STREAM_TRIGGER_PERIOD) {
            range.skey = pContext->pParamToFetch->wstart;
            range.ekey = pContext->pParamToFetch->wend;
          }
        } else {
          code = TSDB_CODE_INTERNAL_ERROR;
          QUERY_CHECK_CODE(code, lino, _end);
        }
        code = stTimestampSorterSetSortInfo(pContext->pSorter, &range, pGroup->pCurTableMeta->tbUid,
                                            isCalcData ? pTask->calcTsIndex : pTask->trigTsIndex);
        QUERY_CHECK_CODE(code, lino, _end);
        code = stTimestampSorterSetMetaDatas(pContext->pSorter, pGroup->pCurTableMeta);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      code = stTimestampSorterNextDataBlock(pContext->pSorter, ppDataBlock, pStartIdx, pEndIdx);
      QUERY_CHECK_CODE(code, lino, _end);
      if (*ppDataBlock == NULL) {
        if (!IS_TRIGGER_TIMESTAMP_SORTER_EMPTY(pContext->pSorter)) {
          *pNeedFetchData = true;
          code = stTimestampSorterGetMetaToFetch(pContext->pSorter, &pContext->pMetaToFetch);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        continue;
      }
      break;
    } else {
      if (IS_TRIGGER_VTABLE_MERGER_EMPTY(pContext->pMerger)) {
        while (saveWindow && IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
          code = stHistoryGroupCloseWindow(pGroup, NULL, saveWindow);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        stVtableMergerReset(pContext->pMerger);
        if (pGroup->tbIter >= taosArrayGetSize(pGroup->pVirTableInfos)) {
          *pAllTableProcessed = true;
          break;
        } else {
          pGroup->pCurVirTable = *(SSTriggerVirTableInfo **)TARRAY_GET_ELEM(pGroup->pVirTableInfos, pGroup->tbIter);
          pGroup->tbIter++;
        }
        if (saveWindow) {
          code = stHistoryGroupRestoreInitWindow(pGroup, pContext->pInitWindows);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        STimeWindow range = {.skey = INT64_MIN, .ekey = INT64_MAX - 1};
        if (pContext->status == STRIGGER_CONTEXT_CHECK_CONDITION) {
          range = pContext->curRange;
        } else if (pContext->status == STRIGGER_CONTEXT_SEND_CALC_REQ) {
          if (pTask->triggerType != STREAM_TRIGGER_PERIOD) {
            QUERY_CHECK_CONDITION(taosArrayGetSize(pContext->pCalcReq->params) > 0, code, lino, _end,
                                  TSDB_CODE_INTERNAL_ERROR);
            SSTriggerCalcParam *pFirst = TARRAY_DATA(pContext->pCalcReq->params);
            SSTriggerCalcParam *pLast = pFirst + TARRAY_SIZE(pContext->pCalcReq->params) - 1;
            range.skey = pFirst->wstart;
            range.ekey = pLast->wend;
          }
        } else {
          code = TSDB_CODE_INTERNAL_ERROR;
          QUERY_CHECK_CODE(code, lino, _end);
        }
        code = stVtableMergerSetMergeInfo(
            pContext->pMerger, &range,
            isCalcData ? pGroup->pCurVirTable->pCalcColRefs : pGroup->pCurVirTable->pTrigColRefs);
        QUERY_CHECK_CODE(code, lino, _end);
        code = stVtableMergerSetMetaDatas(pContext->pMerger, pGroup->pTableMetas);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      code = stVtableMergerNextDataBlock(pContext->pMerger, ppDataBlock);
      QUERY_CHECK_CODE(code, lino, _end);
      *pStartIdx = 0;
      *pEndIdx = *ppDataBlock ? blockDataGetNumOfRows(*ppDataBlock) : 0;
      if (*ppDataBlock == NULL) {
        if (!IS_TRIGGER_VTABLE_MERGER_EMPTY(pContext->pMerger)) {
          *pNeedFetchData = true;
          code = stVtableMergerGetMetaToFetch(pContext->pMerger, &pContext->pMetaToFetch, &pContext->pColRefToFetch);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        continue;
      }
      break;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stHistoryGroupDoSlidingCheck(SSTriggerHistoryGroup *pGroup) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SSTriggerHistoryContext *pContext = pGroup->pContext;
  SStreamTriggerTask      *pTask = pContext->pTask;
  bool                     readAllData = false;
  bool                     allTableProcessed = false;
  bool                     needFetchData = false;

  if (IS_TRIGGER_GROUP_NONE_WINDOW(pGroup)) {
    void *px = tSimpleHashGet(pContext->pFirstTsMap, &pGroup->gid, sizeof(int64_t));
    QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    int64_t ts = *(int64_t *)px;
    code = stHistoryGroupOpenWindow(pGroup, ts, NULL, false, false);
    QUERY_CHECK_CODE(code, lino, _end);
    code = stHistoryGroupSaveInitWindow(pGroup, pContext->pInitWindows);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (pTask->placeHolderBitmap & PLACE_HOLDER_WROWNUM) {
    readAllData = true;
  }

  if (readAllData) {
    // read all data of the current table
    while (!allTableProcessed && !needFetchData) {
      SSDataBlock *pDataBlock = NULL;
      int32_t      startIdx = 0;
      int32_t      endIdx = 0;
      code =
          stHistoryGroupGetDataBlock(pGroup, true, &pDataBlock, &startIdx, &endIdx, &allTableProcessed, &needFetchData);
      QUERY_CHECK_CODE(code, lino, _end);
      if (allTableProcessed || needFetchData) {
        break;
      }
      SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pTask->trigTsIndex);
      QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
      int64_t *pTsData = (int64_t *)pTsCol->pData;
      for (int32_t r = startIdx; r < endIdx;) {
        int64_t nextStart = pGroup->nextWindow.skey;
        int64_t curEnd = IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup) ? TRINGBUF_HEAD(&pGroup->winBuf)->range.ekey : INT64_MAX;
        int64_t ts = TMIN(nextStart, curEnd);
        void   *px = taosbsearch(&ts, pTsData + r, endIdx - r, sizeof(int64_t), compareInt64Val, TD_GT);
        int32_t nrows = (px != NULL) ? (POINTER_DISTANCE(px, &pTsData[r]) / sizeof(int64_t)) : (endIdx - r);
        r += nrows;
        if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
          TRINGBUF_HEAD(&pGroup->winBuf)->wrownum += nrows;
        }
        if (ts == nextStart) {
          code = stHistoryGroupOpenWindow(pGroup, ts, NULL, true, r > 0 && pTsData[r - 1] == nextStart);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        QUERY_CHECK_CONDITION(IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        if (TRINGBUF_HEAD(&pGroup->winBuf)->range.ekey == ts) {
          code = stHistoryGroupCloseWindow(pGroup, NULL, true);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
    }
  } else {
    allTableProcessed = true;
  }

  if (allTableProcessed) {
    if (readAllData) {
      code = stHistoryGroupMergeSavedWindows(pGroup, 0);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    while (true) {
      int64_t nextStart = pGroup->nextWindow.skey;
      int64_t curEnd = IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup) ? TRINGBUF_HEAD(&pGroup->winBuf)->range.ekey : INT64_MAX;
      int64_t ts = TMIN(nextStart, curEnd);
      if (ts > pContext->curRange.ekey) {
        break;
      }
      if (ts == nextStart) {
        code = stHistoryGroupOpenWindow(pGroup, ts, NULL, false, false);
        QUERY_CHECK_CODE(code, lino, _end);
        TRINGBUF_HEAD(&pGroup->winBuf)->wrownum = 0;
      }
      if (ts == curEnd) {
        code = stHistoryGroupCloseWindow(pGroup, NULL, false);
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

static int32_t stHistoryGroupDoSessionCheck(SSTriggerHistoryGroup *pGroup) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SSTriggerHistoryContext *pContext = pGroup->pContext;
  SStreamTriggerTask      *pTask = pContext->pTask;
  bool                     readAllData = false;
  bool                     allTableProcessed = false;
  bool                     needFetchData = false;

  if (pTask->placeHolderBitmap & PLACE_HOLDER_WROWNUM) {
    readAllData = true;
  } else if (pTask->triggerFilter != NULL) {
    readAllData = true;
  }

  while (!allTableProcessed && !needFetchData) {
    if (!readAllData) {
      // use table metadatas to accelerate the session window check
      if (IS_TRIGGER_TIMESTAMP_SORTER_EMPTY(pContext->pSorter)) {
        // save unclosed window of the previous table to merge
        while (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
          code = stHistoryGroupCloseWindow(pGroup, NULL, true);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        stTimestampSorterReset(pContext->pSorter);
        pGroup->pCurTableMeta = tSimpleHashIterate(pGroup->pTableMetas, pGroup->pCurTableMeta, &pGroup->tbIter);
        if (pGroup->pCurTableMeta == NULL) {
          allTableProcessed = true;
          break;
        }
        code = stHistoryGroupRestoreInitWindow(pGroup, pContext->pInitWindows);
        QUERY_CHECK_CODE(code, lino, _end);
        STimeWindow range = pContext->curRange;
        code =
            stTimestampSorterSetSortInfo(pContext->pSorter, &range, pGroup->pCurTableMeta->tbUid, pTask->trigTsIndex);
        QUERY_CHECK_CODE(code, lino, _end);
        code = stTimestampSorterSetMetaDatas(pContext->pSorter, pGroup->pCurTableMeta);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      int64_t ts = IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup) ? TRINGBUF_HEAD(&pGroup->winBuf)->range.ekey : INT64_MIN;
      int64_t lastTs = ts, nextTs = ts;
      code = stTimestampSorterForwardTs(pContext->pSorter, ts, pTask->gap, &lastTs, &nextTs);
      QUERY_CHECK_CODE(code, lino, _end);
      if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
        TRINGBUF_HEAD(&pGroup->winBuf)->range.ekey = lastTs;
      }
      if (nextTs == INT64_MAX) {
        if (!IS_TRIGGER_TIMESTAMP_SORTER_EMPTY(pContext->pSorter)) {
          needFetchData = true;
          code = stTimestampSorterGetMetaToFetch(pContext->pSorter, &pContext->pMetaToFetch);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        continue;
      }
      if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
        code = stHistoryGroupCloseWindow(pGroup, NULL, true);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      code = stHistoryGroupOpenWindow(pGroup, nextTs, NULL, true, true);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      // read all data of the current table
      SSDataBlock *pDataBlock = NULL;
      int32_t      startIdx = 0;
      int32_t      endIdx = 0;
      code =
          stHistoryGroupGetDataBlock(pGroup, true, &pDataBlock, &startIdx, &endIdx, &allTableProcessed, &needFetchData);
      QUERY_CHECK_CODE(code, lino, _end);
      if (allTableProcessed || needFetchData) {
        break;
      }
      SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pTask->trigTsIndex);
      QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
      int64_t *pTsData = (int64_t *)pTsCol->pData;
      for (int32_t r = startIdx; r < endIdx; r++) {
        int64_t          ts = pTsData[r];
        SSTriggerWindow *pCurWin = TRINGBUF_HEAD(&pGroup->winBuf);
        if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup) && pCurWin->range.ekey + pTask->gap >= ts) {
          pCurWin->range.ekey = ts;
          pCurWin->wrownum++;
        } else {
          code = stHistoryGroupCloseWindow(pGroup, NULL, true);
          QUERY_CHECK_CODE(code, lino, _end);
          code = stHistoryGroupOpenWindow(pGroup, ts, NULL, true, true);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
    }
  }

  if (allTableProcessed) {
    code = stHistoryGroupMergeSavedWindows(pGroup, pTask->gap);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stHistoryGroupDoCountCheck(SSTriggerHistoryGroup *pGroup) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SSTriggerHistoryContext *pContext = pGroup->pContext;
  SStreamTriggerTask      *pTask = pContext->pTask;
  bool                     readAllData = false;
  bool                     allTableProcessed = false;
  bool                     needFetchData = false;

  if (pTask->triggerFilter != NULL) {
    readAllData = true;
  } else if (pTask->isVirtualTable) {
    readAllData = true;
  }

#define ALIGN_UP(x, b) (((x) + (b) - 1) / (b) * (b))
  while (!allTableProcessed && !needFetchData) {
    if (!readAllData) {
      // use table metadatas to accelerate the count window check
      if (IS_TRIGGER_TIMESTAMP_SORTER_EMPTY(pContext->pSorter)) {
        stTimestampSorterReset(pContext->pSorter);
        pGroup->pCurTableMeta = tSimpleHashIterate(pGroup->pTableMetas, pGroup->pCurTableMeta, &pGroup->tbIter);
        if (pGroup->pCurTableMeta == NULL) {
          // actually, it has only one table
          allTableProcessed = true;
          break;
        }
        STimeWindow range = pContext->curRange;
        code =
            stTimestampSorterSetSortInfo(pContext->pSorter, &range, pGroup->pCurTableMeta->tbUid, pTask->trigTsIndex);
        QUERY_CHECK_CODE(code, lino, _end);
        code = stTimestampSorterSetMetaDatas(pContext->pSorter, pGroup->pCurTableMeta);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      int64_t skipped = 0;
      int64_t lastTs = INT64_MIN;
      int64_t nrowsCurWin = IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup) ? TRINGBUF_HEAD(&pGroup->winBuf)->wrownum : 0;
      int64_t nrowsNextWstart = ALIGN_UP(nrowsCurWin, pTask->windowSliding) + 1;
      int64_t nrowsToSkip = TMIN(nrowsNextWstart, pTask->windowCount) - nrowsCurWin;
      code = stTimestampSorterForwardNrows(pContext->pSorter, nrowsToSkip, &skipped, &lastTs);
      QUERY_CHECK_CODE(code, lino, _end);
      if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup) && skipped > 0) {
        TRINGBUF_HEAD(&pGroup->winBuf)->range.ekey = lastTs;
        TRINGBUF_HEAD(&pGroup->winBuf)->wrownum += skipped;
      }
      if (skipped < nrowsToSkip) {
        if (!IS_TRIGGER_TIMESTAMP_SORTER_EMPTY(pContext->pSorter)) {
          needFetchData = true;
          code = stTimestampSorterGetMetaToFetch(pContext->pSorter, &pContext->pMetaToFetch);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        continue;
      }
      if (nrowsCurWin + skipped == nrowsNextWstart) {
        code = stHistoryGroupOpenWindow(pGroup, lastTs, NULL, false, true);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      QUERY_CHECK_CONDITION(IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      if (TRINGBUF_HEAD(&pGroup->winBuf)->wrownum == pTask->windowCount) {
        code = stHistoryGroupCloseWindow(pGroup, NULL, false);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    } else {
      // read all data of the current table
      SSDataBlock *pDataBlock = NULL;
      int32_t      startIdx = 0;
      int32_t      endIdx = 0;
      code = stHistoryGroupGetDataBlock(pGroup, false, &pDataBlock, &startIdx, &endIdx, &allTableProcessed,
                                        &needFetchData);
      QUERY_CHECK_CODE(code, lino, _end);
      if (allTableProcessed || needFetchData) {
        break;
      }
      SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pTask->trigTsIndex);
      QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
      int64_t *pTsData = (int64_t *)pTsCol->pData;
      for (int32_t r = startIdx; r < endIdx;) {
        int64_t nrowsCurWin = IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup) ? TRINGBUF_HEAD(&pGroup->winBuf)->wrownum : 0;
        int64_t nrowsNextWstart = ALIGN_UP(nrowsCurWin, pTask->windowSliding) + 1;
        int64_t nrowsToSkip = TMIN(nrowsNextWstart, pTask->windowCount) - nrowsCurWin;
        int64_t skipped = TMIN(nrowsToSkip, endIdx - r);
        int64_t lastTs = pTsData[r + skipped - 1];
        r += skipped;
        if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup) && skipped > 0) {
          TRINGBUF_HEAD(&pGroup->winBuf)->range.ekey = lastTs;
          TRINGBUF_HEAD(&pGroup->winBuf)->wrownum += skipped;
        }
        if (skipped == nrowsNextWstart) {
          code = stHistoryGroupOpenWindow(pGroup, lastTs, NULL, false, true);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        QUERY_CHECK_CONDITION(IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        if (TRINGBUF_HEAD(&pGroup->winBuf)->wrownum == pTask->windowCount) {
          code = stHistoryGroupCloseWindow(pGroup, NULL, false);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stHitoryGrupDoStateCheck(SSTriggerHistoryGroup *pGroup) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SSTriggerHistoryContext *pContext = pGroup->pContext;
  SStreamTriggerTask      *pTask = pContext->pTask;
  bool                     allTableProcessed = false;
  bool                     needFetchData = false;
  char                    *pExtraNotifyContent = NULL;

  while (!allTableProcessed && !needFetchData) {
    //  read all data of the current table
    SSDataBlock *pDataBlock = NULL;
    int32_t      startIdx = 0;
    int32_t      endIdx = 0;
    code =
        stHistoryGroupGetDataBlock(pGroup, false, &pDataBlock, &startIdx, &endIdx, &allTableProcessed, &needFetchData);
    QUERY_CHECK_CODE(code, lino, _end);
    if (allTableProcessed || needFetchData) {
      break;
    }
    SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pTask->trigTsIndex);
    QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
    int64_t         *pTsData = (int64_t *)pTsCol->pData;
    SColumnInfoData *pStateCol = taosArrayGet(pDataBlock->pDataBlock, pTask->stateSlotId);
    QUERY_CHECK_NULL(pStateCol, code, lino, _end, terrno);
    bool  isVarType = IS_VAR_DATA_TYPE(pStateCol->info.type);
    void *pStateData = isVarType ? (void *)pGroup->stateVal.pData : (void *)&pGroup->stateVal.val;
    if (IS_TRIGGER_GROUP_NONE_WINDOW(pGroup)) {
      // initialize state value
      SValue *pStateVal = &pGroup->stateVal;
      pStateVal->type = pStateCol->info.type;
      if (isVarType) {
        pStateVal->nData = pStateCol->info.bytes;
        pStateVal->pData = taosMemoryCalloc(pStateVal->nData, 1);
        QUERY_CHECK_CONDITION(pStateVal->pData, code, lino, _end, terrno);
        pStateData = pStateVal->pData;
      }

      // open the first window
      char   *newVal = colDataGetData(pStateCol, startIdx);
      int32_t bytes = isVarType ? varDataTLen(newVal) : pStateCol->info.bytes;
      if (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_OPEN) {
        code = streamBuildStateNotifyContent(STRIGGER_EVENT_WINDOW_OPEN, pStateCol->info.type, NULL, newVal,
                                             &pExtraNotifyContent);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      code = stHistoryGroupOpenWindow(pGroup, pTsData[startIdx], &pExtraNotifyContent, false, true);
      QUERY_CHECK_CODE(code, lino, _end);
      memcpy(pStateData, newVal, bytes);
      startIdx++;
    }
    for (int32_t r = startIdx; r < endIdx; r++) {
      char   *newVal = colDataGetData(pStateCol, r);
      int32_t bytes = isVarType ? varDataTLen(newVal) : pStateCol->info.bytes;
      if (memcmp(pStateData, newVal, bytes) == 0) {
        TRINGBUF_HEAD(&pGroup->winBuf)->wrownum++;
        TRINGBUF_HEAD(&pGroup->winBuf)->range.ekey = pTsData[r];
      } else {
        if (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_CLOSE) {
          code = streamBuildStateNotifyContent(STRIGGER_EVENT_WINDOW_CLOSE, pStateCol->info.type, pStateData, newVal,
                                               &pExtraNotifyContent);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        code = stHistoryGroupCloseWindow(pGroup, &pExtraNotifyContent, false);
        QUERY_CHECK_CODE(code, lino, _end);
        if (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_OPEN) {
          code = streamBuildStateNotifyContent(STRIGGER_EVENT_WINDOW_OPEN, pStateCol->info.type, pStateData, newVal,
                                               &pExtraNotifyContent);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        code = stHistoryGroupOpenWindow(pGroup, pTsData[r], &pExtraNotifyContent, false, true);
        QUERY_CHECK_CODE(code, lino, _end);
        memcpy(pStateData, newVal, bytes);
      }
    }
  }

_end:
  if (pExtraNotifyContent != NULL) {
    taosMemoryFreeClear(pExtraNotifyContent);
  }
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stHistoryGroupDoEventCheck(SSTriggerHistoryGroup *pGroup) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SSTriggerHistoryContext *pContext = pGroup->pContext;
  SStreamTriggerTask      *pTask = pContext->pTask;
  bool                     allTableProcessed = false;
  bool                     needFetchData = false;
  char                    *pExtraNotifyContent = NULL;
  SColumnInfoData         *psCol = NULL;
  SColumnInfoData         *peCol = NULL;

  while (!allTableProcessed && !needFetchData) {
    //  read all data of the current table
    SSDataBlock *pDataBlock = NULL;
    int32_t      startIdx = 0;
    int32_t      endIdx = 0;
    code =
        stHistoryGroupGetDataBlock(pGroup, false, &pDataBlock, &startIdx, &endIdx, &allTableProcessed, &needFetchData);
    QUERY_CHECK_CODE(code, lino, _end);
    if (allTableProcessed || needFetchData) {
      break;
    }
    SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pTask->trigTsIndex);
    QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
    int64_t *pTsData = (int64_t *)pTsCol->pData;
    bool    *ps = NULL, *pe = NULL;
    psCol = NULL;
    peCol = NULL;

    for (int32_t r = startIdx; r < endIdx; r++) {
      if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
        TRINGBUF_HEAD(&pGroup->winBuf)->range.ekey = pTsData[r];
        TRINGBUF_HEAD(&pGroup->winBuf)->wrownum++;
      } else {
        if (ps == NULL) {
          SFilterColumnParam param = {.numOfCols = taosArrayGetSize(pDataBlock->pDataBlock),
                                      .pDataBlock = pDataBlock->pDataBlock};
          code = filterSetDataFromSlotId(pContext->pStartCond, &param);
          QUERY_CHECK_CODE(code, lino, _end);
          int32_t status = 0;
          code = filterExecute(pContext->pStartCond, pDataBlock, &psCol, NULL, param.numOfCols, &status);
          QUERY_CHECK_CODE(code, lino, _end);
          ps = (bool *)psCol->pData;
        }
        if (ps[r]) {
          if (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_OPEN) {
            code = streamBuildEventNotifyContent(pDataBlock, pTask->pStartCondCols, r, &pExtraNotifyContent);
            QUERY_CHECK_CODE(code, lino, _end);
          }
          code = stHistoryGroupOpenWindow(pGroup, pTsData[r], &pExtraNotifyContent, false, true);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
      if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
        if (pe == NULL) {
          SFilterColumnParam param = {.numOfCols = taosArrayGetSize(pDataBlock->pDataBlock),
                                      .pDataBlock = pDataBlock->pDataBlock};
          code = filterSetDataFromSlotId(pContext->pEndCond, &param);
          QUERY_CHECK_CODE(code, lino, _end);
          int32_t status = 0;
          code = filterExecute(pContext->pEndCond, pDataBlock, &peCol, NULL, param.numOfCols, &status);
          QUERY_CHECK_CODE(code, lino, _end);
          pe = (bool *)peCol->pData;
        }
        if (pe[r]) {
          if (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_CLOSE) {
            code = streamBuildEventNotifyContent(pDataBlock, pTask->pEndCondCols, r, &pExtraNotifyContent);
            QUERY_CHECK_CODE(code, lino, _end);
          }
          code = stHistoryGroupCloseWindow(pGroup, &pExtraNotifyContent, false);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
    }

    colDataDestroy(psCol);
    taosMemoryFreeClear(psCol);
    colDataDestroy(peCol);
    taosMemoryFreeClear(peCol);
  }

_end:

  colDataDestroy(psCol);
  taosMemoryFreeClear(psCol);

  if (pExtraNotifyContent != NULL) {
    taosMemoryFreeClear(pExtraNotifyContent);
  }
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stHistoryGroupCheck(SSTriggerHistoryGroup *pGroup) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SSTriggerHistoryContext *pContext = pGroup->pContext;
  SStreamTriggerTask      *pTask = pContext->pTask;

  switch (pTask->triggerType) {
    case STREAM_TRIGGER_SLIDING:
      return stHistoryGroupDoSlidingCheck(pGroup);

    case STREAM_TRIGGER_SESSION:
      return stHistoryGroupDoSessionCheck(pGroup);

    case STREAM_TRIGGER_COUNT:
      return stHistoryGroupDoCountCheck(pGroup);

    case STREAM_TRIGGER_STATE:
      return stHitoryGrupDoStateCheck(pGroup);

    case STREAM_TRIGGER_EVENT:
      return stHistoryGroupDoEventCheck(pGroup);

    default: {
      ST_TASK_ELOG("invalid stream trigger type %d at %s:%d", pTask->triggerType, __func__, __LINE__);
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
