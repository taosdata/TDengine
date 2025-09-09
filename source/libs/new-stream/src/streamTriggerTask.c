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
#include "tarray.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "thash.h"
#include "ttime.h"

#define STREAM_TRIGGER_CHECK_INTERVAL_MS    1000                    // 1s
#define STREAM_TRIGGER_WAIT_TIME_NS         1 * NANOSECOND_PER_SEC  // 1s, todo(kjq): increase the wait time to 10s
#define STREAM_TRIGGER_BATCH_WINDOW_WAIT_NS 1 * NANOSECOND_PER_SEC  // 1s, todo(kjq): increase the wait time to 30s
#define STREAM_TRIGGER_REALTIME_SESSIONID   1
#define STREAM_TRIGGER_HISTORY_SESSIONID    2

#define STREAM_TRIGGER_HISTORY_STEP_MS 10 * 24 * 60 * 60 * 1000  // 10d

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
static void stRealtimeGroupClearMetadatas(SSTriggerRealtimeGroup *pGroup, int64_t prevWindowEnd);

static int32_t stHistoryGroupInit(SSTriggerHistoryGroup *pGroup, SSTriggerHistoryContext *pContext, int64_t gid);
static void    stHistoryGroupDestroy(void *ptr);
static int32_t stHistoryGroupAddMetaDatas(SSTriggerHistoryGroup *pGroup, SArray *pMetadatas, SArray *pVgIds,
                                          bool *pAdded);
static int32_t stHistoryGroupCheck(SSTriggerHistoryGroup *pGroup);
static int32_t stHistoryGroupGetDataBlock(SSTriggerHistoryGroup *pGroup, bool saveWindow, SSDataBlock **ppDataBlock,
                                          int32_t *pStartIdx, int32_t *pEndIdx, bool *pAllTableProcessed,
                                          bool *pNeedFetchData);
static void    stHistoryGroupClearTempState(SSTriggerHistoryGroup *pGroup);
static void    stHistoryGroupClearMetadatas(SSTriggerHistoryGroup *pGroup, int64_t prevWindowEnd);

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
    stDebug("resume stream trigger session %" PRIx64 "-%" PRIx64 "-%" PRIx64 " since now:%" PRId64
            ", resumeTime:%" PRId64,
            pInfo->streamId, pInfo->taskId, pInfo->sessionId, now, pInfo->resumeTime);

    SStreamTask *pTask = NULL;
    void        *pTaskAddr = NULL;
    int32_t      code = streamAcquireTask(pInfo->streamId, pInfo->taskId, &pTask, &pTaskAddr);
    if (code != TSDB_CODE_SUCCESS) {
      stError("failed to acquire stream trigger session %" PRIx64 "-%" PRIx64 "-%" PRIx64 " since %s", pInfo->streamId,
              pInfo->taskId, pInfo->sessionId, tstrerror(code));
      TD_DLIST_POP(&readylist, pNode);
      taosMemoryFreeClear(pNode);
      continue;
    }

    SSTriggerCtrlRequest req = {.type = STRIGGER_CTRL_START,
                                .streamId = pInfo->streamId,
                                .taskId = pInfo->taskId,
                                .sessionId = pInfo->sessionId};
    SRpcMsg              msg = {.msgType = TDMT_STREAM_TRIGGER_CTRL};
    msg.contLen = tSerializeSTriggerCtrlRequest(NULL, 0, &req);
    if (msg.contLen > 0) {
      msg.pCont = rpcMallocCont(msg.contLen);
      if (msg.pCont != NULL) {
        int32_t tlen = tSerializeSTriggerCtrlRequest(msg.pCont, msg.contLen, &req);
        if (tlen == msg.contLen) {
          TRACE_SET_ROOTID(&msg.info.traceId, pInfo->streamId);
          TRACE_SET_MSGID(&msg.info.traceId, tGenIdPI64());
          SMsgCb *pCb = &gStreamMgmt.msgCb;
          int32_t code = pCb->putToQueueFp(pCb->mgmt, STREAM_TRIGGER_QUEUE, &msg);
          if (code != TSDB_CODE_SUCCESS) {
            stError("failed to send trigger start request stream:%" PRIx64 ", task:%" PRIx64 ", session:%" PRIx64,
                    pInfo->streamId, pInfo->taskId, pInfo->sessionId);
          }
        } else {
          stError("failed to serialize trigger start request stream:%" PRIx64 ", task:%" PRIx64 ", session:%" PRIx64,
                  pInfo->streamId, pInfo->taskId, pInfo->sessionId);
        }
      } else {
        stError("failed to alloc trigger start request stream:%" PRIx64 ", task:%" PRIx64 ", session:%" PRIx64,
                pInfo->streamId, pInfo->taskId, pInfo->sessionId);
      }
    } else {
      stError("failed to get length of trigger start request stream:%" PRIx64 ", task:%" PRIx64 ", session:%" PRIx64,
              pInfo->streamId, pInfo->taskId, pInfo->sessionId);
    }
    TD_DLIST_POP(&readylist, pNode);
    taosMemoryFreeClear(pNode);
    streamReleaseTask(pTaskAddr);
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

  pInfo->streamAHandle = 1;
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

#define STREAM_TRIGGER_CHECKPOINT_FORMAT_VERSION 1

static int32_t stTriggerTaskGenCheckpoint(SStreamTriggerTask *pTask, uint8_t *buf, int64_t *pLen) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pTask->pRealtimeContext;
  SEncoder                  encoder = {0};
  int32_t                   iter = 0;
  int32_t                   ver = atomic_add_fetch_32(&pTask->checkpointVersion, 1);

  if (tSimpleHashGetSize(pTask->pRealtimeStartVer) == 0) {
    goto _end;
  }

  tEncoderInit(&encoder, buf, *pLen);
  code = tStartEncode(&encoder);
  QUERY_CHECK_CODE(code, lino, _end);

  code = tEncodeI32(&encoder, ver);  // version
  QUERY_CHECK_CODE(code, lino, _end);
  code = tEncodeI64(&encoder, pTask->task.streamId);
  QUERY_CHECK_CODE(code, lino, _end);

  code = tEncodeI32(&encoder, STREAM_TRIGGER_CHECKPOINT_FORMAT_VERSION);
  QUERY_CHECK_CODE(code, lino, _end);

  code = tEncodeI32(&encoder, tSimpleHashGetSize(pTask->pRealtimeStartVer));
  QUERY_CHECK_CODE(code, lino, _end);
  iter = 0;
  void *px = tSimpleHashIterate(pTask->pRealtimeStartVer, NULL, &iter);
  while (px != NULL) {
    int32_t vgId = *(int32_t *)tSimpleHashGetKey(px, NULL);
    int64_t startVer = *(int64_t *)px;
    code = tEncodeI32(&encoder, vgId);
    QUERY_CHECK_CODE(code, lino, _end);
    code = tEncodeI64(&encoder, startVer);
    QUERY_CHECK_CODE(code, lino, _end);
    px = tSimpleHashIterate(pTask->pRealtimeStartVer, px, &iter);
  }

  code = tEncodeI32(&encoder, tSimpleHashGetSize(pTask->pHistoryCutoffTime));
  QUERY_CHECK_CODE(code, lino, _end);
  iter = 0;
  px = tSimpleHashIterate(pTask->pHistoryCutoffTime, NULL, &iter);
  while (px != NULL) {
    int64_t gid = *(int64_t *)tSimpleHashGetKey(px, NULL);
    int64_t cutoffTime = *(int64_t *)px;
    code = tEncodeI64(&encoder, gid);
    QUERY_CHECK_CODE(code, lino, _end);
    code = tEncodeI64(&encoder, cutoffTime);
    QUERY_CHECK_CODE(code, lino, _end);
    px = tSimpleHashIterate(pTask->pHistoryCutoffTime, px, &iter);
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

static int32_t stTriggerTaskParseCheckpoint(SStreamTriggerTask *pTask, uint8_t *buf, int64_t len) {
  SDecoder decoder = {0};
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  int32_t  ver = 0;
  int32_t  formatVer = 0;
  int64_t  streamId = 0;

  if (buf == NULL || len == 0) {
    goto _end;
  }

  tDecoderInit(&decoder, buf, len);
  code = tStartDecode(&decoder);
  QUERY_CHECK_CODE(code, lino, _end);

  code = tDecodeI32(&decoder, &ver);
  QUERY_CHECK_CODE(code, lino, _end);
  code = tDecodeI64(&decoder, &streamId);
  QUERY_CHECK_CODE(code, lino, _end);
  QUERY_CHECK_CONDITION(streamId == pTask->task.streamId, code, lino, _end, TSDB_CODE_INVALID_PARA);

  code = tDecodeI32(&decoder, &formatVer);
  QUERY_CHECK_CODE(code, lino, _end);
  QUERY_CHECK_CONDITION(formatVer == STREAM_TRIGGER_CHECKPOINT_FORMAT_VERSION, code, lino, _end,
                        TSDB_CODE_INVALID_PARA);
  int32_t nVgroups = 0;
  code = tDecodeI32(&decoder, &nVgroups);
  QUERY_CHECK_CODE(code, lino, _end);
  for (int32_t i = 0; i < nVgroups; i++) {
    int32_t vgId = 0;
    int64_t startVer = 0;
    code = tDecodeI32(&decoder, &vgId);
    QUERY_CHECK_CODE(code, lino, _end);
    code = tDecodeI64(&decoder, &startVer);
    QUERY_CHECK_CODE(code, lino, _end);
    void *px = tSimpleHashGet(pTask->pRealtimeStartVer, &vgId, sizeof(int32_t));
    QUERY_CHECK_CONDITION(px == NULL, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    code = tSimpleHashPut(pTask->pRealtimeStartVer, &vgId, sizeof(int32_t), &startVer, sizeof(int64_t));
    QUERY_CHECK_CODE(code, lino, _end);
    ST_TASK_DLOG("parse checkpoint, vgId: %d, startVer: %" PRId64, vgId, startVer);
  }

  int32_t nGroups = 0;
  code = tDecodeI32(&decoder, &nGroups);
  QUERY_CHECK_CODE(code, lino, _end);
  for (int32_t i = 0; i < nGroups; i++) {
    int64_t gid = 0;
    int64_t cutoffTime = 0;
    code = tDecodeI64(&decoder, &gid);
    QUERY_CHECK_CODE(code, lino, _end);
    code = tDecodeI64(&decoder, &cutoffTime);
    QUERY_CHECK_CODE(code, lino, _end);
    void *px = tSimpleHashGet(pTask->pHistoryCutoffTime, &gid, sizeof(int64_t));
    QUERY_CHECK_CONDITION(px == NULL, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    code = tSimpleHashPut(pTask->pHistoryCutoffTime, &gid, sizeof(int64_t), &cutoffTime, sizeof(int64_t));
    QUERY_CHECK_CODE(code, lino, _end);
    ST_TASK_DLOG("parse checkpoint, gid: %" PRId64 ", cutoffTime: %" PRId64, gid, cutoffTime);
  }

  tEndDecode(&decoder);
  QUERY_CHECK_CONDITION(decoder.pos == len, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  atomic_store_32(&pTask->checkpointVersion, ver);

#if !TRIGGER_USE_HISTORY_META
  bool startFromBound = !pTask->fillHistoryFirst;
#else
  bool startFromBound = true;
#endif
  if (startFromBound) {
    for (int32_t i = 0; i < TARRAY_SIZE(pTask->readerList); i++) {
      SStreamTaskAddr      *pReader = TARRAY_GET_ELEM(pTask->readerList, i);
      SSTriggerWalProgress *pProgress =
          tSimpleHashGet(pTask->pRealtimeContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      void *px = tSimpleHashGet(pTask->pRealtimeStartVer, &pProgress->pTaskAddr->nodeId, sizeof(int32_t));
      QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pProgress->lastScanVer = pProgress->latestVer = *(int64_t *)px;
    }
  }

_end:
  tDecoderClear(&decoder);
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
    int32_t slotId = *(int32_t *)TARRAY_GET_ELEM(pSlots, i);
    if (slotId >= pTask->nVirDataCols) {
      // ignore pseudo columns
      continue;
    }
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

int32_t stTriggerTaskAddRecalcRequest(SStreamTriggerTask *pTask, SSTriggerRealtimeGroup *pGroup,
                                      STimeWindow *pCalcRange, SSHashObj *pWalProgress, bool isHistory) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;
  bool                    needUnlock = false;
  SSTriggerRecalcRequest *pReq = NULL;

  QUERY_CHECK_NULL(pWalProgress, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pReq = taosMemoryCalloc(1, sizeof(SSTriggerRecalcRequest));
  QUERY_CHECK_NULL(pReq, code, lino, _end, terrno);

  if (isHistory) {
    pReq->gid = 0;
    pReq->scanRange.skey = pTask->fillHistoryStartTime;
    pReq->scanRange.ekey = INT64_MIN;
    int32_t iter = 0;
    void   *px = tSimpleHashIterate(pTask->pHistoryCutoffTime, NULL, &iter);
    while (px != NULL) {
      pReq->scanRange.ekey = TMAX(pReq->scanRange.ekey, *(int64_t *)px);
      px = tSimpleHashIterate(pTask->pHistoryCutoffTime, px, &iter);
    }
    pReq->calcRange = pReq->scanRange;
    pReq->isHistory = true;
  } else {
    QUERY_CHECK_NULL(pGroup, code, lino, _end, TSDB_CODE_INVALID_PARA);
    QUERY_CHECK_NULL(pCalcRange, code, lino, _end, TSDB_CODE_INVALID_PARA);
    pReq->gid = pGroup->gid;
    if (pTask->fillHistory || pTask->fillHistoryFirst) {
      pReq->scanRange.skey = pTask->fillHistoryStartTime;
    } else {
      void *px = tSimpleHashGet(pTask->pHistoryCutoffTime, &pReq->gid, sizeof(int64_t));
      pReq->scanRange.skey = ((px == NULL) ? INT64_MIN : *(int64_t *)px) + 1;
    }
    pReq->scanRange.ekey = pGroup->newThreshold;
    pReq->calcRange = *pCalcRange;
    pReq->isHistory = false;
  }

  if (pReq->scanRange.skey > pReq->scanRange.ekey) {
    goto _end;
  }

  ST_TASK_DLOG("add recalc request, gid: %" PRId64 ", scanRange: [%" PRId64 ", %" PRId64 "], calcRange: [%" PRId64
               ", %" PRId64 "]",
               pReq->gid, pReq->scanRange.skey, pReq->scanRange.ekey, pReq->calcRange.skey, pReq->calcRange.ekey);

  pReq->pTsdbVersions = tSimpleHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  QUERY_CHECK_NULL(pReq->pTsdbVersions, code, lino, _end, terrno);
  pReq->isHistory = isHistory;

  int32_t               iter = 0;
  SSTriggerWalProgress *pProgress = tSimpleHashIterate(pWalProgress, NULL, &iter);
  while (pProgress != NULL) {
    int32_t vgId = *(int32_t *)tSimpleHashGetKey(pProgress, NULL);
    code = tSimpleHashPut(pReq->pTsdbVersions, &vgId, sizeof(int32_t), &pProgress->lastScanVer, sizeof(int64_t));
    QUERY_CHECK_CODE(code, lino, _end);
    pProgress = tSimpleHashIterate(pWalProgress, pProgress, &iter);
  }

  taosWLockLatch(&pTask->recalcRequestLock);
  needUnlock = true;

  code = tdListAppend(pTask->pRecalcRequests, &pReq);
  QUERY_CHECK_CODE(code, lino, _end);
  pReq = NULL;

_end:
  if (needUnlock) {
    taosWUnLockLatch(&pTask->recalcRequestLock);
  }
  if (pReq != NULL) {
    if (pReq->pTsdbVersions != NULL) {
      tSimpleHashCleanup(pReq->pTsdbVersions);
    }
    taosMemoryFreeClear(pReq);
  }
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t stTriggerTaskFetchRecalcRequest(SStreamTriggerTask *pTask, SSTriggerRecalcRequest **ppReq) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  bool    needUnlock = false;

  taosRLockLatch(&pTask->recalcRequestLock);
  needUnlock = true;

  // todo(kjq): merge recalc requests

  SListNode *pNode = tdListPopHead(pTask->pRecalcRequests);
  if (pNode != NULL) {
    *ppReq = *(SSTriggerRecalcRequest **)pNode->data;
    taosMemoryFreeClear(pNode);
    ST_TASK_DLOG("start recalc request, gid: %" PRId64 ", scanRange: [%" PRId64 ", %" PRId64 "], calcRange: [%" PRId64
                 ", %" PRId64 "]",
                 (*ppReq)->gid, (*ppReq)->scanRange.skey, (*ppReq)->scanRange.ekey, (*ppReq)->calcRange.skey,
                 (*ppReq)->calcRange.ekey);

  } else {
    *ppReq = NULL;
  }

_end:
  if (needUnlock) {
    taosWUnLockLatch(&pTask->recalcRequestLock);
  }
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stTriggerTaskCollectVirCols(SStreamTriggerTask *pTask, void *plan, SArray **ppColids,
                                           SArray **ppIsPseudoCol, SNodeList **ppSlots) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SNode  *pScanPlan = NULL;
  SArray *pColids = NULL;
  SArray *pIsPseudoCol = NULL;

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
  SNodeList *pScanPseudoCols = pScanNode->scan.pScanPseudoCols;

  int32_t nTrigCols = LIST_LENGTH(pSlots);
  pColids = taosArrayInit(nTrigCols, sizeof(col_id_t));
  QUERY_CHECK_NULL(pColids, code, lino, _end, terrno);
  pIsPseudoCol = taosArrayInit(nTrigCols, sizeof(bool));
  QUERY_CHECK_NULL(pIsPseudoCol, code, lino, _end, terrno);
  SNode *pColNode = NULL;
  SNode *pSlotNode = NULL;
  FOREACH(pSlotNode, pSlots) {
    SSlotDescNode *p1 = (SSlotDescNode *)pSlotNode;
    bool           isPseudoCol = false;
    col_id_t       colId = 0;
    FOREACH(pColNode, pScanCols) {
      STargetNode *pTarget = (STargetNode *)pColNode;
      if (pTarget->slotId != p1->slotId) {
        continue;
      }
      QUERY_CHECK_CONDITION(nodeType(pTarget->pExpr) == QUERY_NODE_COLUMN, code, lino, _end, TSDB_CODE_INVALID_PARA);
      colId = ((SColumnNode *)pTarget->pExpr)->colId;
      isPseudoCol = false;
      break;
    }
    if (colId == 0) {
      FOREACH(pColNode, pScanPseudoCols) {
        STargetNode *pTarget = (STargetNode *)pColNode;
        if (pTarget->slotId != p1->slotId) {
          continue;
        }
        if (nodeType(pTarget->pExpr) == QUERY_NODE_COLUMN) {
          colId = ((SColumnNode *)pTarget->pExpr)->colId;
        } else if (nodeType(pTarget->pExpr) == QUERY_NODE_FUNCTION) {
          SFunctionNode *p2 = (SFunctionNode *)pTarget->pExpr;
          QUERY_CHECK_CONDITION(strcmp(p2->functionName, "tbname") == 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
          colId = -1;
        } else {
          code = TSDB_CODE_INVALID_PARA;
          QUERY_CHECK_CODE(code, lino, _end);
        }
        isPseudoCol = true;
        break;
      }
    }
    QUERY_CHECK_CONDITION(colId != 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
    void *px = taosArrayPush(pColids, &colId);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
    px = taosArrayPush(pIsPseudoCol, &isPseudoCol);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
  }

  *ppColids = pColids;
  pColids = NULL;
  *ppIsPseudoCol = pIsPseudoCol;
  pIsPseudoCol = NULL;
  *ppSlots = pSlots;
  pScanNode->scan.node.pOutputDataBlockDesc->pSlots = NULL;

_end:
  if (pScanPlan != NULL) {
    nodesDestroyNode(pScanPlan);
  }
  if (pColids != NULL) {
    taosArrayDestroy(pColids);
  }
  if (pIsPseudoCol != NULL) {
    taosArrayDestroy(pIsPseudoCol);
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
  SArray    *pTrigIsPseudoCol = NULL;
  SArray    *pCalcColids = NULL;
  SArray    *pCalcIsPseudoCol = NULL;
  SNodeList *pTrigSlots = NULL;
  SNodeList *pCalcSlots = NULL;
  SArray    *pVirColIds = NULL;
  SArray    *pTrigSlotids = NULL;
  SArray    *pCalcSlotids = NULL;
  char      *infoBuf = NULL;
  int64_t    bufLen = 0;
  int64_t    bufCap = 1024;

  code = stTriggerTaskCollectVirCols(pTask, triggerScanPlan, &pTrigColids, &pTrigIsPseudoCol, &pTrigSlots);
  QUERY_CHECK_CODE(code, lino, _end);
  code = stTriggerTaskCollectVirCols(pTask, calcCacheScanPlan, &pCalcColids, &pCalcIsPseudoCol, &pCalcSlots);
  QUERY_CHECK_CODE(code, lino, _end);

  int32_t nTrigCols = taosArrayGetSize(pTrigColids);
  int32_t nCalcCols = taosArrayGetSize(pCalcColids);
  pVirColIds = taosArrayInit(nTrigCols + nCalcCols, sizeof(col_id_t));
  QUERY_CHECK_NULL(pVirColIds, code, lino, _end, terrno);

  // combine non-pseudo column ids from trig-cols and calc-cols
  for (int32_t i = 0; i < nTrigCols; i++) {
    if (*(bool *)TARRAY_GET_ELEM(pTrigIsPseudoCol, i)) {
      continue;
    }
    col_id_t id = *(col_id_t *)TARRAY_GET_ELEM(pTrigColids, i);
    void    *px = taosArrayPush(pVirColIds, &id);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
  }
  for (int32_t i = 0; i < nCalcCols; i++) {
    if (*(bool *)TARRAY_GET_ELEM(pCalcIsPseudoCol, i)) {
      continue;
    }
    col_id_t id = *(col_id_t *)TARRAY_GET_ELEM(pCalcColids, i);
    void    *px = taosArrayPush(pVirColIds, &id);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
  }
  int32_t nDataCols = TARRAY_SIZE(pVirColIds);
  if (nDataCols > 0) {
    taosSort(TARRAY_DATA(pVirColIds), nDataCols, sizeof(col_id_t), compareUint16Val);
    col_id_t *pColIds = pVirColIds->pData;
    int32_t   j = 0;
    for (int32_t i = 1; i < TARRAY_SIZE(pVirColIds); i++) {
      if (pColIds[i] != pColIds[j]) {
        ++j;
        pColIds[j] = pColIds[i];
      }
    }
    TARRAY_SIZE(pVirColIds) = j + 1;
    nDataCols = TARRAY_SIZE(pVirColIds);
  }

  // combine pseudo column ids from trig-cols and calc-cols
  for (int32_t i = 0; i < nTrigCols; i++) {
    if (!*(bool *)TARRAY_GET_ELEM(pTrigIsPseudoCol, i)) {
      continue;
    }
    col_id_t id = *(col_id_t *)TARRAY_GET_ELEM(pTrigColids, i);
    void    *px = taosArrayPush(pVirColIds, &id);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
  }
  for (int32_t i = 0; i < nCalcCols; i++) {
    if (!*(bool *)TARRAY_GET_ELEM(pCalcIsPseudoCol, i)) {
      continue;
    }
    col_id_t id = *(col_id_t *)TARRAY_GET_ELEM(pCalcColids, i);
    void    *px = taosArrayPush(pVirColIds, &id);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
  }
  int32_t nPseudoCols = TARRAY_SIZE(pVirColIds) - nDataCols;
  if (nPseudoCols > 0) {
    taosSort((char *)TARRAY_DATA(pVirColIds) + nDataCols * sizeof(col_id_t), nPseudoCols, sizeof(col_id_t), compareUint16Val);
    col_id_t *pColIds = pVirColIds->pData;
    int32_t   j = nDataCols;
    for (int32_t i = nDataCols + 1; i < TARRAY_SIZE(pVirColIds); i++) {
      if (pColIds[i] != pColIds[j]) {
        ++j;
        pColIds[j] = pColIds[i];
      }
    }
    TARRAY_SIZE(pVirColIds) = j + 1;
    nPseudoCols = TARRAY_SIZE(pVirColIds) - nDataCols;
  }

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
  pTask->nVirDataCols = nDataCols;

  if (infoBuf && bufLen < bufCap) {
    infoBuf[bufLen - 1] = '}';
    bufLen += tsnprintf(infoBuf + bufLen, bufCap - bufLen, "; slotId of trigger data:{");
  }

  // get new slot id of trig data block and calc data block
  pTrigSlotids = taosArrayInit(nTrigCols, sizeof(int32_t));
  QUERY_CHECK_NULL(pTrigSlotids, code, lino, _end, terrno);
  for (int32_t i = 0; i < nTrigCols; i++) {
    col_id_t id = *(col_id_t *)TARRAY_GET_ELEM(pTrigColids, i);
    int32_t  slotid = -1;
    for (int32_t j = 0; j < nTotalCols; j++) {
      if (id == *(col_id_t *)TARRAY_GET_ELEM(pVirColIds, j)) {
        slotid = j;
        break;
      }
    }
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
    int32_t  slotid = -1;
    for (int32_t j = 0; j < nTotalCols; j++) {
      if (id == *(col_id_t *)TARRAY_GET_ELEM(pVirColIds, j)) {
        slotid = j;
        break;
      }
    }
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
  if (pTrigIsPseudoCol != NULL) {
    taosArrayDestroy(pTrigIsPseudoCol);
  }
  if (pCalcColids != NULL) {
    taosArrayDestroy(pCalcColids);
  }
  if (pCalcIsPseudoCol != NULL) {
    taosArrayDestroy(pCalcIsPseudoCol);
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
  pTask->hasPartitionBy = pMsg->hasPartitionBy;
  pTask->isVirtualTable = pMsg->isTriggerTblVirt;
  pTask->ignoreNoDataTrigger = pMsg->igNoDataTrigger;
  pTask->hasTriggerFilter = pMsg->triggerHasPF;
  if (pTask->ignoreNoDataTrigger) {
    QUERY_CHECK_CONDITION(
        (pTask->triggerType == STREAM_TRIGGER_PERIOD) || (pTask->triggerType == STREAM_TRIGGER_SLIDING), code, lino,
        _end, TSDB_CODE_INVALID_PARA);
  }
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

  taosInitRWLatch(&pTask->recalcRequestLock);
  pTask->pRecalcRequests = tdListNew(POINTER_BYTES);
  QUERY_CHECK_NULL(pTask->pRecalcRequests, code, lino, _end, terrno);

  pTask->pRealtimeStartVer = tSimpleHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  QUERY_CHECK_NULL(pTask->pRealtimeStartVer, code, lino, _end, terrno);
  pTask->pHistoryCutoffTime = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  QUERY_CHECK_NULL(pTask->pHistoryCutoffTime, code, lino, _end, terrno);

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
        code = streamSyncWriteCheckpoint(pTask->task.streamId, epSet, buf, len);
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
      code = streamSyncDeleteCheckpoint(pTask->task.streamId, epSet);
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

  if (pTask->pRecalcRequests != NULL) {
    SListIter  iter = {0};
    SListNode *pNode = NULL;
    tdListInitIter(pTask->pRecalcRequests, &iter, TD_LIST_FORWARD);
    while ((pNode = tdListNext(&iter)) != NULL) {
      SSTriggerRecalcRequest *pReq = *(SSTriggerRecalcRequest **)pNode->data;
      if (pReq->pTsdbVersions != NULL) {
        tSimpleHashCleanup(pReq->pTsdbVersions);
      }
      taosMemoryFreeClear(pReq);
    }
    pTask->pRecalcRequests = tdListFree(pTask->pRecalcRequests);
  }

  if (pTask->streamName != NULL) {
    taosMemoryFree(pTask->streamName);
    pTask->streamName = NULL;
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
      SSTriggerCtrlRequest req = {.type = STRIGGER_CTRL_START,
                                  .streamId = pTask->task.streamId,
                                  .taskId = pTask->task.taskId,
                                  .sessionId = pTask->pRealtimeContext->sessionId};
      SRpcMsg              msg = {.msgType = TDMT_STREAM_TRIGGER_CTRL};
      msg.contLen = tSerializeSTriggerCtrlRequest(NULL, 0, &req);
      QUERY_CHECK_CONDITION(msg.contLen > 0, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      msg.pCont = rpcMallocCont(msg.contLen);
      QUERY_CHECK_NULL(msg.pCont, code, lino, _end, terrno);
      int32_t tlen = tSerializeSTriggerCtrlRequest(msg.pCont, msg.contLen, &req);
      QUERY_CHECK_CONDITION(tlen == msg.contLen, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      TRACE_SET_ROOTID(&msg.info.traceId, pTask->task.streamId);
      TRACE_SET_MSGID(&msg.info.traceId, tGenIdPI64());

      SMsgCb *pCb = &gStreamMgmt.msgCb;
      code = pCb->putToQueueFp(pCb->mgmt, STREAM_TRIGGER_QUEUE, &msg);
      QUERY_CHECK_CODE(code, lino, _end);

      ST_TASK_DLOG("send start control request for session: %" PRIx64, req.sessionId);
      ST_TASK_DLOG("control request 0x%" PRIx64 ":0x%" PRIx64 " sent", msg.info.traceId.rootId, msg.info.traceId.msgId);

      pTask->task.status = STREAM_STATUS_RUNNING;

      int32_t leaderSid = pTask->leaderSnodeId;
      SEpSet *epSet = gStreamMgmt.getSynEpset(leaderSid);
      if (epSet != NULL) {
        ST_TASK_DLOG("[checkpoint] trigger task deploy, sync checkpoint leaderSnodeId:%d", leaderSid);
        atomic_store_8(&pTask->isCheckpointReady, 0);
        code = streamSyncWriteCheckpoint(pTask->task.streamId, epSet, NULL, 0);
        if (code != 0) {
          atomic_store_8(&pTask->isCheckpointReady, 1);
        }
      } else {
        atomic_store_8(&pTask->isCheckpointReady, 1);
      }
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
    case STREAM_MSG_UPDATE_RUNNER: {
      // todo(kjq): handle update runner message
      break;
    }
    case STREAM_MSG_USER_RECALC: {
      SStreamMgmtRsp           *pRsp = (SStreamMgmtRsp *)pMsg;
      SArray                   *pRecalcList = pRsp->cont.recalcList;
      int32_t                   nRecalcReq = taosArrayGetSize(pRecalcList);
      SSTriggerRealtimeContext *pContext = pTask->pRealtimeContext;
      QUERY_CHECK_NULL(pContext, code, lino, _end, TSDB_CODE_INVALID_PARA);
      for (int32_t i = 0; i < nRecalcReq; i++) {
        SStreamRecalcReq *pReq = TARRAY_GET_ELEM(pRecalcList, i);
        int32_t           iter = 0;
        void             *px = tSimpleHashIterate(pContext->pGroups, NULL, &iter);
        while (px != NULL) {
          SSTriggerRealtimeGroup *pGroup = *(SSTriggerRealtimeGroup **)px;
          STimeWindow             range = {.skey = pReq->start, .ekey = pReq->end - 1};
          code = stTriggerTaskAddRecalcRequest(pTask, pGroup, &range, pContext->pReaderWalProgress, false);
          QUERY_CHECK_CODE(code, lino, _end);
          px = tSimpleHashIterate(pContext->pGroups, px, &iter);
        }
      }
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

  if (pRsp->msgType == TDMT_STREAM_TRIGGER_PULL_RSP) {
    SMsgSendInfo         *ahandle = pRsp->info.ahandle;
    SSTriggerAHandle     *pAhandle = ahandle->param;
    SSTriggerPullRequest *pReq = pAhandle->param;
    switch (pRsp->code) {
      case TSDB_CODE_SUCCESS:
      case TSDB_CODE_STREAM_NO_DATA:
      case TSDB_CODE_STREAM_NO_CONTEXT: {
        if (pReq->sessionId == STREAM_TRIGGER_REALTIME_SESSIONID) {
          code = stRealtimeContextProcPullRsp(pTask->pRealtimeContext, pRsp);
          QUERY_CHECK_CODE(code, lino, _end);
        } else if (pReq->sessionId == STREAM_TRIGGER_HISTORY_SESSIONID) {
          code = stHistoryContextProcPullRsp(pTask->pHistoryContext, pRsp);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        break;
      }
      case TSDB_CODE_STREAM_TASK_NOT_EXIST: {
        bool addWait = false;
        if (pReq->sessionId == STREAM_TRIGGER_REALTIME_SESSIONID) {
          addWait = (listNEles(&pTask->pRealtimeContext->retryPullReqs) == 0);
          code = tdListAppend(&pTask->pRealtimeContext->retryPullReqs, &pReq);
          QUERY_CHECK_CODE(code, lino, _end);
        } else if (pReq->sessionId == STREAM_TRIGGER_HISTORY_SESSIONID) {
          addWait = (listNEles(&pTask->pHistoryContext->retryPullReqs) == 0);
          code = tdListAppend(&pTask->pHistoryContext->retryPullReqs, &pReq);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        if (addWait) {
          int64_t resumeTime = taosGetTimestampNs() + STREAM_ACT_MIN_DELAY_MSEC * NANOSECOND_PER_MSEC;
          code = stTriggerTaskAddWaitSession(pTask, pReq->sessionId, resumeTime);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        break;
      }
      default: {
        *pErrTaskId = pReq->readerTaskId;
        code = pRsp->code;
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
  } else if (pRsp->msgType == TDMT_STREAM_TRIGGER_CALC_RSP) {
    SMsgSendInfo         *ahandle = pRsp->info.ahandle;
    SSTriggerAHandle     *pAhandle = ahandle->param;
    SSTriggerCalcRequest *pReq = pAhandle->param;
    if (pRsp->code == TSDB_CODE_MND_STREAM_TABLE_NOT_CREATE) {
      taosArrayClearEx(pReq->params, tDestroySSTriggerCalcParam);
      pRsp->code = TSDB_CODE_SUCCESS;
    }
    switch (pRsp->code) {
      case TSDB_CODE_SUCCESS:
      case TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER:
      case TSDB_CODE_STREAM_INSERT_TBINFO_NOT_FOUND: {
        // todo(kjq): retry calc request when trigger could clear data cache manually
        if (pRsp->code != TSDB_CODE_SUCCESS && (pTask->placeHolderBitmap & PLACE_HOLDER_PARTITION_ROWS)) {
          *pErrTaskId = pReq->runnerTaskId;
          code = pRsp->code;
          QUERY_CHECK_CODE(code, lino, _end);
        } else if (pReq->sessionId == STREAM_TRIGGER_REALTIME_SESSIONID) {
          code = stRealtimeContextProcCalcRsp(pTask->pRealtimeContext, pRsp);
          QUERY_CHECK_CODE(code, lino, _end);
        } else if (pReq->sessionId == STREAM_TRIGGER_HISTORY_SESSIONID) {
          code = stHistoryContextProcCalcRsp(pTask->pHistoryContext, pRsp);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        break;
      }
      default: {
        *pErrTaskId = pReq->runnerTaskId;
        code = pRsp->code;
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
  } else if (pRsp->msgType == TDMT_STREAM_TRIGGER_CTRL) {
    SSTriggerCtrlRequest req = {0};
    code = tDeserializeSTriggerCtrlRequest(pRsp->pCont, pRsp->contLen, &req);
    QUERY_CHECK_CODE(code, lino, _end);
    switch (req.type) {
      case STRIGGER_CTRL_START: {
        if (req.sessionId == STREAM_TRIGGER_REALTIME_SESSIONID) {
          code = stRealtimeContextCheck(pTask->pRealtimeContext);
          QUERY_CHECK_CODE(code, lino, _end);
        } else if (req.sessionId == STREAM_TRIGGER_HISTORY_SESSIONID) {
          code = stHistoryContextCheck(pTask->pHistoryContext);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        break;
      }
      default: {
        ST_TASK_ELOG("invalid stream trigger control request type %d at %s:%d", req.type, __func__, __LINE__);
        code = TSDB_CODE_INVALID_PARA;
        QUERY_CHECK_CODE(code, lino, _end);
      }
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
  pContext->sessionId = STREAM_TRIGGER_REALTIME_SESSIONID;

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
    code = stVtableMergerInit(pContext->pMerger, pTask, &pVirDataBlock, &pVirDataFilter, pTask->nVirDataCols);
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

  tdListInit(&pContext->retryPullReqs, POINTER_BYTES);
  tdListInit(&pContext->retryCalcReqs, POINTER_BYTES);

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

  tdListEmpty(&pContext->retryPullReqs);
  tdListEmpty(&pContext->retryCalcReqs);

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
      int32_t                       nCols = pTask->nVirDataCols;
      pReq->cids = pProgress->reqCids;
      code = taosArrayEnsureCap(pReq->cids, nCols);
      QUERY_CHECK_CODE(code, lino, _end);
      TARRAY_SIZE(pReq->cids) = nCols;
      for (int32_t i = 0; i < nCols; i++) {
        SColumnInfoData *pCol = TARRAY_GET_ELEM(pTask->pVirDataBlock->pDataBlock, i);
        *(col_id_t *)TARRAY_GET_ELEM(pReq->cids, i) = pCol->info.colId;
      }
      break;
    }

    case STRIGGER_PULL_VTABLE_PSEUDO_COL: {
      SSTriggerRealtimeGroup *pGroup = stRealtimeContextGetCurrentGroup(pContext);
      QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
      QUERY_CHECK_CONDITION(pTask->isVirtualTable, code, lino, _end, TSDB_CODE_INVALID_PARA);
      SSTriggerVirTableInfo *pTable = taosArrayGetP(pGroup->pVirTableInfos, 0);
      QUERY_CHECK_NULL(pTable, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pProgress = tSimpleHashGet(pContext->pReaderWalProgress, &pTable->vgId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SSTriggerVirTablePseudoColRequest *pReq = &pProgress->pullReq.virTablePseudoColReq;
      pReq->uid = pTable->tbUid;
      pReq->cids = pProgress->reqCids;
      taosArrayClear(pReq->cids);
      int32_t nCol = taosArrayGetSize(pTask->pVirDataBlock->pDataBlock);
      for (int32_t i = pTask->nVirDataCols; i < nCol; i++) {
        SColumnInfoData *pCol = TARRAY_GET_ELEM(pTask->pVirDataBlock->pDataBlock, i);
        void            *px = taosArrayPush(pReq->cids, &pCol->info.colId);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
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
  ST_TASK_DLOG("trigger pull req ahandle %p allocated", msg.info.ahandle);

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
  TRACE_SET_ROOTID(&msg.info.traceId, pTask->task.streamId);
  TRACE_SET_MSGID(&msg.info.traceId, tGenIdPI64());

  code = tmsgSendReq(&pReader->epset, &msg);
  QUERY_CHECK_CODE(code, lino, _end);

  ST_TASK_DLOG("send pull request of type %d to node:%d task:%" PRIx64, pReq->type, pReader->nodeId, pReader->taskId);
  ST_TASK_DLOG("trigger pull req 0x%" PRIx64 ":0x%" PRIx64 " sent", msg.info.traceId.rootId, msg.info.traceId.msgId);

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
  for (int32_t i = 0; i < nRunners; i++) {
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
        if(startIdx >= endIdx) continue;
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
        if (pContext->pColRefToFetch != NULL && pContext->pMetaToFetch != NULL) {
          code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_WAL_DATA);
          QUERY_CHECK_CODE(code, lino, _end);
          goto _end;
        } else if (pContext->pColRefToFetch != NULL && pContext->pMetaToFetch == NULL) {
          code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_VTABLE_PSEUDO_COL);
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
    ST_TASK_DLOG("[calc param %d]: gid=%" PRId64 ", wstart=%" PRId64 ", wend=%" PRId64 ", nrows=%" PRId64
                 ", prevTs=%" PRId64 ", currentTs=%" PRId64 ", nextTs=%" PRId64 ", prevLocalTime=%" PRId64
                 ", nextLocalTime=%" PRId64 ", localTime=%" PRId64 ", create=%d",
                 i, pCalcReq->gid, pParam->wstart, pParam->wend, pParam->wrownum, pParam->prevTs, pParam->currentTs,
                 pParam->nextTs, pParam->prevLocalTime, pParam->nextLocalTime, pParam->triggerTime,
                 pCalcReq->createTable);
  }

  // serialize and send request
  QUERY_CHECK_CODE(stTriggerTaskAllocAhandle(pTask, pContext->sessionId, pCalcReq, &msg.info.ahandle), lino, _end);
  ST_TASK_DLOG("trigger calc req ahandle %p allocated", msg.info.ahandle);

  msg.contLen = tSerializeSTriggerCalcRequest(NULL, 0, pCalcReq);
  QUERY_CHECK_CONDITION(msg.contLen > 0, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  msg.contLen += sizeof(SMsgHead);
  msg.pCont = rpcMallocCont(msg.contLen);
  QUERY_CHECK_NULL(msg.pCont, code, lino, _end, terrno);
  SMsgHead *pMsgHead = (SMsgHead *)msg.pCont;
  pMsgHead->contLen = htonl(msg.contLen);
  pMsgHead->vgId = htonl(SNODE_HANDLE);
  int32_t tlen =
      tSerializeSTriggerCalcRequest((char *)msg.pCont + sizeof(SMsgHead), msg.contLen - sizeof(SMsgHead), pCalcReq);
  QUERY_CHECK_CONDITION(tlen == msg.contLen - sizeof(SMsgHead), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  TRACE_SET_ROOTID(&msg.info.traceId, pTask->task.streamId);
  TRACE_SET_MSGID(&msg.info.traceId, tGenIdPI64());

  code = tmsgSendReq(&pCalcRunner->addr.epset, &msg);
  QUERY_CHECK_CODE(code, lino, _end);

  ST_TASK_DLOG("send calc request to node:%d task:%" PRIx64, pCalcRunner->addr.nodeId, pCalcRunner->addr.taskId);
  ST_TASK_DLOG("trigger calc req 0x%" PRIx64 ":0x%" PRIx64 " sent", msg.info.traceId.rootId, msg.info.traceId.msgId);

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

static int32_t stRealtimeContextRetryPullRequest(SSTriggerRealtimeContext *pContext, SListNode *pNode,
                                                 SSTriggerPullRequest *pReq) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pContext->pTask;
  SStreamTaskAddr    *pReader = NULL;
  SRpcMsg             msg = {.msgType = TDMT_STREAM_TRIGGER_PULL};

  QUERY_CHECK_NULL(pNode, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pReq, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(*(SSTriggerPullRequest **)pNode->data == pReq, code, lino, _end, TSDB_CODE_INVALID_PARA);

  for (int32_t i = 0; i < taosArrayGetSize(pTask->virtReaderList); i++) {
    SStreamTaskAddr *pTempReader = TARRAY_GET_ELEM(pTask->virtReaderList, i);
    if (pTempReader->taskId == pReq->readerTaskId) {
      pReader = pTempReader;
      break;
    }
  }
  for (int32_t i = 0; i < taosArrayGetSize(pTask->readerList); i++) {
    SStreamTaskAddr *pTempReader = TARRAY_GET_ELEM(pTask->readerList, i);
    if (pTempReader->taskId == pReq->readerTaskId) {
      pReader = pTempReader;
      break;
    }
  }
  QUERY_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

  // serialize and send request
  QUERY_CHECK_CODE(stTriggerTaskAllocAhandle(pTask, pContext->sessionId, pReq, &msg.info.ahandle), lino, _end);
  stDebug("trigger pull req ahandle %p allocated", msg.info.ahandle);

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
  TRACE_SET_ROOTID(&msg.info.traceId, pTask->task.streamId);
  TRACE_SET_MSGID(&msg.info.traceId, tGenIdPI64());

  code = tmsgSendReq(&pReader->epset, &msg);
  QUERY_CHECK_CODE(code, lino, _end);

  ST_TASK_DLOG("send pull request of type %d to node:%d task:%" PRIx64, pReq->type, pReader->nodeId, pReader->taskId);
  ST_TASK_DLOG("trigger pull req 0x%" PRIx64 ":0x%" PRIx64 " sent", msg.info.traceId.rootId, msg.info.traceId.msgId);

  pNode = tdListPopNode(&pContext->retryPullReqs, pNode);
  taosMemoryFreeClear(pNode);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    destroyAhandle(msg.info.ahandle);
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stRealtimeContextRetryCalcRequest(SSTriggerRealtimeContext *pContext, SListNode *pNode,
                                                 SSTriggerCalcRequest *pReq) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  SStreamTriggerTask  *pTask = pContext->pTask;
  SStreamRunnerTarget *pRunner = NULL;
  bool                 needTagValue = false;
  SRpcMsg              msg = {.msgType = TDMT_STREAM_TRIGGER_CALC};

  QUERY_CHECK_NULL(pNode, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pReq, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(*(SSTriggerCalcRequest **)pNode->data == pReq, code, lino, _end, TSDB_CODE_INVALID_PARA);

  int32_t nRunners = taosArrayGetSize(pTask->runnerList);
  for (int32_t i = 0; i < nRunners; i++) {
    SStreamRunnerTarget *pTempRunner = TARRAY_GET_ELEM(pTask->runnerList, i);
    if (pTempRunner->addr.taskId == pReq->runnerTaskId) {
      pRunner = pTempRunner;
      break;
    }
  }
  QUERY_CHECK_NULL(pRunner, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

  pReq->createTable = true;

  if (pReq->createTable && pTask->hasPartitionBy || (pTask->placeHolderBitmap & PLACE_HOLDER_PARTITION_IDX) ||
      (pTask->placeHolderBitmap & PLACE_HOLDER_PARTITION_TBNAME)) {
    needTagValue = true;
  }

  if (needTagValue && taosArrayGetSize(pReq->groupColVals) == 0) {
    code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_GROUP_COL_VALUE);
    QUERY_CHECK_CODE(code, lino, _end);
    goto _end;
  }

  // serialize and send request
  QUERY_CHECK_CODE(stTriggerTaskAllocAhandle(pTask, pContext->sessionId, pReq, &msg.info.ahandle), lino, _end);
  stDebug("trigger calc req ahandle %p allocated", msg.info.ahandle);

  msg.contLen = tSerializeSTriggerCalcRequest(NULL, 0, pReq);
  QUERY_CHECK_CONDITION(msg.contLen > 0, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  msg.contLen += sizeof(SMsgHead);
  msg.pCont = rpcMallocCont(msg.contLen);
  QUERY_CHECK_NULL(msg.pCont, code, lino, _end, terrno);
  SMsgHead *pMsgHead = (SMsgHead *)msg.pCont;
  pMsgHead->contLen = htonl(msg.contLen);
  pMsgHead->vgId = htonl(SNODE_HANDLE);
  int32_t tlen =
      tSerializeSTriggerCalcRequest((char *)msg.pCont + sizeof(SMsgHead), msg.contLen - sizeof(SMsgHead), pReq);
  QUERY_CHECK_CONDITION(tlen == msg.contLen - sizeof(SMsgHead), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  TRACE_SET_ROOTID(&msg.info.traceId, pTask->task.streamId);
  TRACE_SET_MSGID(&msg.info.traceId, tGenIdPI64());

  code = tmsgSendReq(&pRunner->addr.epset, &msg);
  QUERY_CHECK_CODE(code, lino, _end);

  ST_TASK_DLOG("send calc request to node:%d task:%" PRIx64, pRunner->addr.nodeId, pRunner->addr.taskId);
  ST_TASK_DLOG("trigger calc req 0x%" PRIx64 ":0x%" PRIx64 " sent", msg.info.traceId.rootId, msg.info.traceId.msgId);

  pNode = tdListPopNode(&pContext->retryCalcReqs, pNode);
  taosMemoryFreeClear(pNode);

_end:
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

  if (listNEles(&pContext->retryPullReqs) > 0) {
    while (listNEles(&pContext->retryPullReqs) > 0) {
      SListNode            *pNode = TD_DLIST_HEAD(&pContext->retryPullReqs);
      SSTriggerPullRequest *pReq = *(SSTriggerPullRequest **)pNode->data;
      code = stRealtimeContextRetryPullRequest(pContext, pNode, pReq);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    goto _end;
  }

  if (pContext->status == STRIGGER_CONTEXT_IDLE && pTask->isVirtualTable && !pTask->virTableInfoReady) {
    pContext->status = STRIGGER_CONTEXT_GATHER_VTABLE_INFO;
    if (taosArrayGetSize(pTask->virtReaderList) > 0 && taosArrayGetSize(pTask->pVirTableInfoRsp) == 0) {
      pContext->lastVirtTableInfoTime = taosGetTimestampNs();
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

  if (!pContext->haveReadCheckpoint) {
    stDebug("[checkpoint] read checkpoint for stream %" PRIx64, pTask->task.streamId);
    if (atomic_load_8(&pTask->isCheckpointReady) == 1) {
      void   *buf = NULL;
      int64_t len = 0;
      code = streamReadCheckPoint(pTask->task.streamId, &buf, &len);
      if (code != TSDB_CODE_SUCCESS) {
        taosMemoryFree(buf);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      code = stTriggerTaskParseCheckpoint(pTask, buf, len);
      if (code != TSDB_CODE_SUCCESS) {
        taosMemoryFree(buf);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      taosMemoryFree(buf);
      pContext->haveReadCheckpoint = true;
    } else {
      // wait 1 second and retry
      int64_t resumeTime = taosGetTimestampNs() + 1 * NANOSECOND_PER_SEC;
      code = stTriggerTaskAddWaitSession(pTask, pContext->sessionId, pContext->periodWindow.ekey);
      QUERY_CHECK_CODE(code, lino, _end);
      goto _end;
    }
  }

  if (pContext->status == STRIGGER_CONTEXT_IDLE) {
    if (taosArrayGetSize(pTask->readerList) > 0 && tSimpleHashGetSize(pTask->pRealtimeStartVer) == 0) {
      pContext->status = STRIGGER_CONTEXT_DETERMINE_BOUND;
      for (pContext->curReaderIdx = 0; pContext->curReaderIdx < TARRAY_SIZE(pTask->readerList);
           pContext->curReaderIdx++) {
        code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_LAST_TS);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      goto _end;
    }

    if (!pTask->historyCalcStarted) {
      QUERY_CHECK_CONDITION(pTask->pHistoryContext == NULL, code, lino, _end, TSDB_CODE_INVALID_PARA);
      pTask->pHistoryContext = taosMemoryCalloc(1, sizeof(SSTriggerHistoryContext));
      QUERY_CHECK_NULL(pTask->pHistoryContext, code, lino, _end, terrno);
      code = stHistoryContextInit(pTask->pHistoryContext, pTask);
      QUERY_CHECK_CODE(code, lino, _end);
      SSTriggerCtrlRequest req = {.type = STRIGGER_CTRL_START,
                                  .streamId = pTask->task.streamId,
                                  .taskId = pTask->task.taskId,
                                  .sessionId = pTask->pHistoryContext->sessionId};
      SRpcMsg              msg = {.msgType = TDMT_STREAM_TRIGGER_CTRL};
      msg.contLen = tSerializeSTriggerCtrlRequest(NULL, 0, &req);
      QUERY_CHECK_CONDITION(msg.contLen > 0, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      msg.pCont = rpcMallocCont(msg.contLen);
      QUERY_CHECK_NULL(msg.pCont, code, lino, _end, terrno);
      int32_t tlen = tSerializeSTriggerCtrlRequest(msg.pCont, msg.contLen, &req);
      QUERY_CHECK_CONDITION(tlen == msg.contLen, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      TRACE_SET_ROOTID(&msg.info.traceId, pTask->task.streamId);
      TRACE_SET_MSGID(&msg.info.traceId, tGenIdPI64());

      SMsgCb *pCb = &gStreamMgmt.msgCb;
      code = pCb->putToQueueFp(pCb->mgmt, STREAM_TRIGGER_QUEUE, &msg);
      QUERY_CHECK_CODE(code, lino, _end);

      ST_TASK_DLOG("send start control request for session: %" PRIx64, req.sessionId);
      ST_TASK_DLOG("control request 0x%" PRIx64 ":0x%" PRIx64 " sent", msg.info.traceId.rootId, msg.info.traceId.msgId);
      pTask->historyCalcStarted = true;

      if (pTask->fillHistory) {
        code = stTriggerTaskAddRecalcRequest(pTask, NULL, NULL, pContext->pReaderWalProgress, true);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }

    if (pTask->triggerType != STREAM_TRIGGER_PERIOD) {
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
        if (pContext->pColRefToFetch != NULL && pContext->pMetaToFetch != NULL) {
          code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_WAL_DATA);
          QUERY_CHECK_CODE(code, lino, _end);
          goto _end;
        } else if (pContext->pColRefToFetch != NULL && pContext->pMetaToFetch == NULL) {
          code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_VTABLE_PSEUDO_COL);
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
          code = streamSendNotifyContent(&pTask->task, pTask->streamName, NULL, pTask->triggerType, pGroup->gid,
                                         pTask->pNotifyAddrUrls, pTask->notifyErrorHandle,
                                         TARRAY_DATA(pContext->pNotifyParams), TARRAY_SIZE(pContext->pNotifyParams));
          QUERY_CHECK_CODE(code, lino, _end);
        }
        stRealtimeGroupClearTempState(pGroup);
        pContext->status = STRIGGER_CONTEXT_SEND_CALC_REQ;
      }
      case STRIGGER_CONTEXT_SEND_CALC_REQ: {
        int64_t prevWindowEnd = INT64_MIN;
        if (pContext->pCalcReq == NULL) {
          QUERY_CHECK_CONDITION(TARRAY_SIZE(pGroup->pPendingCalcParams) == 0, code, lino, _end,
                                TSDB_CODE_INTERNAL_ERROR);
          // do nothing
        } else {
          if (TARRAY_SIZE(pContext->pCalcReq->params) == 0) {
            if (pGroup->recalcNextWindow && taosArrayGetSize(pGroup->pPendingCalcParams) > 0) {
              SSTriggerCalcParam *pParam = TARRAY_DATA(pGroup->pPendingCalcParams);
              STimeWindow         recalcRange = {.skey = pParam->wstart, .ekey = pParam->wend};
              code = stTriggerTaskAddRecalcRequest(pTask, pGroup, &recalcRange, pContext->pReaderWalProgress, false);
              QUERY_CHECK_CODE(code, lino, _end);
              taosArrayPopFrontBatch(pGroup->pPendingCalcParams, 1);
              pGroup->recalcNextWindow = false;
            }
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
            SSTriggerCalcParam *pParam = taosArrayGetLast(pContext->pCalcReq->params);
            QUERY_CHECK_NULL(pParam, code, lino, _end, terrno);
            prevWindowEnd = pParam->wend;
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
        stRealtimeGroupClearMetadatas(pGroup, prevWindowEnd);
        break;
      }
      default: {
        ST_TASK_ELOG("invalid context status %d at %s:%d", pContext->status, __func__, __LINE__);
        code = TSDB_CODE_INVALID_PARA;
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
    TD_DLIST_POP(&pContext->groupsToCheck, pGroup);
    if (pContext->needCheckAgain) {
      pContext->needCheckAgain = false;
      TD_DLIST_APPEND(&pContext->groupsToCheck, pGroup);
    } else {
      int32_t nRemainParams = taosArrayGetSize(pGroup->pPendingCalcParams);
      bool    needMoreCalc =
          (pTask->lowLatencyCalc && (nRemainParams > 0) || (nRemainParams >= STREAM_CALC_REQ_MAX_WIN_NUM));
      if (needMoreCalc) {
        // the group has remaining calc params to be calculated
        TD_DLIST_APPEND(&pContext->groupsToCheck, pGroup);
      }
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
          code = streamSendNotifyContent(&pTask->task, pTask->streamName, NULL, pTask->triggerType, pGroup->gid,
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

#define STRIGGER_VIRTUAL_TABLE_INFO_INTERVAL_NS 10 * NANOSECOND_PER_SEC  // 10s
  if (pTask->isVirtualTable && pContext->lastVirtTableInfoTime + STRIGGER_VIRTUAL_TABLE_INFO_INTERVAL_NS <= now) {
    // check virtual table info
    pContext->status = STRIGGER_CONTEXT_FETCH_META;
    for (pContext->curReaderIdx = 0; pContext->curReaderIdx < TARRAY_SIZE(pTask->virtReaderList);
         pContext->curReaderIdx++) {
      code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_VTABLE_INFO);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    pContext->lastVirtTableInfoTime = now;
    goto _end;
  }

  if (pTask->triggerType == STREAM_TRIGGER_PERIOD) {
    stTriggerTaskNextPeriodWindow(pTask, &pContext->periodWindow);
    pContext->status = STRIGGER_CONTEXT_IDLE;
    code = stTriggerTaskAddWaitSession(pTask, pContext->sessionId, pContext->periodWindow.ekey);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
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

static int32_t stRealtimeContextProcPullRsp(SSTriggerRealtimeContext *pContext, SRpcMsg *pRsp) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SStreamTriggerTask       *pTask = pContext->pTask;
  SSDataBlock              *pDataBlock = NULL;
  SArray                   *pAllMetadatas = NULL;
  SArray                   *pVgIds = NULL;
  SStreamMsgVTableInfo      vtableInfo = {0};
  SSTriggerOrigTableInfoRsp otableInfo = {0};
  SArray                   *pOrigTableNames = NULL;

  QUERY_CHECK_CONDITION(pRsp->code == TSDB_CODE_SUCCESS || pRsp->code == TSDB_CODE_STREAM_NO_DATA, code, lino, _end,
                        TSDB_CODE_INVALID_PARA);

  SMsgSendInfo         *ahandle = pRsp->info.ahandle;
  SSTriggerAHandle     *pAhandle = ahandle->param;
  SSTriggerPullRequest *pReq = pAhandle->param;

  ST_TASK_DLOG("receive pull response of type %d from task:%" PRIx64, pReq->type, pReq->readerTaskId);

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
          if (pTask->isVirtualTable) {
            int32_t iter = 0;
            void   *px = tSimpleHashIterate(pContext->pGroups, NULL, &iter);
            while (px != NULL) {
              SSTriggerRealtimeGroup *pGroup = *(SSTriggerRealtimeGroup **)px;
              bool inGroup = (tSimpleHashGet(pGroup->pTableMetas, &pGidData[i], sizeof(int64_t)) != NULL);
              if (inGroup) {
                void *px2 = tSimpleHashGet(pTask->pHistoryCutoffTime, &pGroup->gid, sizeof(int64_t));
                if (px2 == NULL) {
                  code = tSimpleHashPut(pTask->pHistoryCutoffTime, &pGroup->gid, sizeof(int64_t), &pTsData[i],
                                        sizeof(int64_t));
                  QUERY_CHECK_CODE(code, lino, _end);
                } else {
                  *(int64_t *)px2 = TMAX(*(int64_t *)px, pTsData[i]);
                }
                if (pTask->fillHistory) {
                  pGroup->oldThreshold = TMAX(pGroup->oldThreshold, pTsData[i]);
                }
              }
              px = tSimpleHashIterate(pContext->pGroups, px, &iter);
            }
          } else {
            px = tSimpleHashGet(pTask->pHistoryCutoffTime, &pGidData[i], sizeof(int64_t));
            if (px == NULL) {
              code = tSimpleHashPut(pTask->pHistoryCutoffTime, &pGidData[i], sizeof(int64_t), &pTsData[i],
                                    sizeof(int64_t));
              QUERY_CHECK_CODE(code, lino, _end);
            } else {
              *(int64_t *)px = TMAX(*(int64_t *)px, pTsData[i]);
            }
          }
        }
      }

      if (--pContext->curReaderIdx > 0) {
        // wait for responses from other readers
        goto _end;
      }

#if !TRIGGER_USE_HISTORY_META
      bool startFromBound = !pTask->fillHistoryFirst;
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
      }
      void *px = taosArrayPush(pProgress->pMetadatas, &pDataBlock);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      pDataBlock = NULL;

      if (--pContext->curReaderIdx > 0) {
        ST_TASK_DLOG("wait for response from other %d readers", pContext->curReaderIdx);
        goto _end;
      }

      bool continueToFetch = false;
      pContext->getWalMetaThisRound = false;
      for (int32_t i = 0; i < TARRAY_SIZE(pTask->readerList); i++) {
        SStreamTaskAddr      *pReader = TARRAY_GET_ELEM(pTask->readerList, i);
        SSTriggerWalProgress *pTempProgress =
            tSimpleHashGet(pContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t));
        QUERY_CHECK_NULL(pTempProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        SSDataBlock *pBlock = *(SSDataBlock **)taosArrayGetLast(pTempProgress->pMetadatas);
        int32_t      nrows = blockDataGetNumOfRows(pBlock);
        if (nrows >= STREAM_RETURN_ROWS_NUM) {
          continueToFetch = true;
          break;
        } else if (nrows > 0) {
          pContext->getWalMetaThisRound = true;
        }
      }

      if (continueToFetch) {
        ST_TASK_DLOG("continue to fetch wal metas since some readers are not exhausted: %" PRIzu,
                     TARRAY_SIZE(pTask->readerList));
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

      if ((pTask->triggerType == STREAM_TRIGGER_PERIOD) && !pTask->ignoreNoDataTrigger) {
        int32_t iter = 0;
        void   *px = tSimpleHashIterate(pContext->pGroups, NULL, &iter);
        while (px != NULL) {
          SSTriggerRealtimeGroup *pGroup = *(SSTriggerRealtimeGroup **)px;
          if (TD_DLIST_NODE_NEXT(pGroup) == NULL && TD_DLIST_TAIL(&pContext->groupsToCheck) != pGroup) {
            // add the group to check list
            pGroup->oldThreshold = INT64_MIN;
            pGroup->newThreshold = INT64_MAX;
            TD_DLIST_APPEND(&pContext->groupsToCheck, pGroup);
          }
          px = tSimpleHashIterate(pContext->pGroups, px, &iter);
        }
      }

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
      SSTriggerGroupColValueRequest *pRequest = (SSTriggerGroupColValueRequest *)pReq;
      if (pContext->pCalcReq != NULL && pContext->pCalcReq->gid == pRequest->gid) {
        SStreamGroupInfo groupInfo = {.gInfo = pContext->pCalcReq->groupColVals};
        code = tDeserializeSStreamGroupInfo(pRsp->pCont, pRsp->contLen, &groupInfo);
        QUERY_CHECK_CODE(code, lino, _end);
        code = stRealtimeContextCheck(pContext);
        QUERY_CHECK_CODE(code, lino, _end);
      } else {
        SListIter  iter = {0};
        SListNode *pNode = NULL;
        tdListInitIter(&pContext->retryCalcReqs, &iter, TD_LIST_FORWARD);
        while ((pNode = tdListNext(&iter)) != NULL) {
          SSTriggerCalcRequest *pCalcReq = *(SSTriggerCalcRequest **)pNode->data;
          if (pCalcReq->gid == pRequest->gid) {
            SStreamGroupInfo groupInfo = {.gInfo = pCalcReq->groupColVals};
            code = tDeserializeSStreamGroupInfo(pRsp->pCont, pRsp->contLen, &groupInfo);
            QUERY_CHECK_CODE(code, lino, _end);
            code = stRealtimeContextRetryCalcRequest(pContext, pNode, pCalcReq);
            QUERY_CHECK_CODE(code, lino, _end);
            break;
          }
        }
      }
      break;
    }

    case STRIGGER_PULL_VTABLE_PSEUDO_COL: {
      QUERY_CHECK_CONDITION(
          pContext->status == STRIGGER_CONTEXT_SEND_CALC_REQ || pContext->status == STRIGGER_CONTEXT_CHECK_CONDITION,
          code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pDataBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
      QUERY_CHECK_NULL(pDataBlock, code, lino, _end, terrno);
      QUERY_CHECK_CONDITION(pRsp->contLen > 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
      const char *pCont = pRsp->pCont;
      code = blockDecode(pDataBlock, pCont, &pCont);
      QUERY_CHECK_CODE(code, lino, _end);
      QUERY_CHECK_CONDITION(pCont == (char *)pRsp->pCont + pRsp->contLen, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      QUERY_CHECK_NULL(pContext->pColRefToFetch, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      code = stVtableMergerSetPseudoCols(pContext->pMerger, &pDataBlock);
      QUERY_CHECK_CODE(code, lino, _end);
      pContext->pColRefToFetch = NULL;
      code = stRealtimeContextCheck(pContext);
      QUERY_CHECK_CODE(code, lino, _end);
      break;
    }

    case STRIGGER_PULL_VTABLE_INFO: {
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

      if (pTask->virTableInfoReady) {
        // check virtual table info
        for (int32_t i = 0; i < nVirTables; i++) {
          VTableInfo            *pInfo = TARRAY_GET_ELEM(vtableInfo.infos, i);
          SSTriggerVirTableInfo *pTable = tSimpleHashGet(pTask->pVirTableInfos, &pInfo->uid, sizeof(int64_t));
          if (pTable == NULL) {
            ST_TASK_DLOG("found new added virtual table, gid:%" PRId64 ", uid:%" PRId64 ", ver:%d", pInfo->gId,
                         pInfo->uid, pInfo->cols.version);
            code = TSDB_CODE_INTERNAL_ERROR;
            QUERY_CHECK_CODE(code, lino, _end);
          }
          if (pTable->tbVer != pInfo->cols.version) {
            ST_TASK_DLOG("virtual table version changed, gid:%" PRId64 ", uid:%" PRId64 ", ver:%" PRId64 " -> %d",
                         pInfo->gId, pInfo->uid, pTable->tbVer, pInfo->cols.version);
            code = TSDB_CODE_INTERNAL_ERROR;
            QUERY_CHECK_CODE(code, lino, _end);
          }
        }

        if (--pContext->curReaderIdx > 0) {
          // wait for responses from other readers
          goto _end;
        }

        code = stRealtimeContextCheck(pContext);
        QUERY_CHECK_CODE(code, lino, _end);
        break;
      }

      QUERY_CHECK_CONDITION(pContext->status == STRIGGER_CONTEXT_GATHER_VTABLE_INFO, code, lino, _end,
                            TSDB_CODE_INTERNAL_ERROR);
      for (int32_t i = 0; i < nVirTables; i++) {
        VTableInfo           *pInfo = TARRAY_GET_ELEM(vtableInfo.infos, i);
        SSTriggerVirTableInfo newInfo = {
            .tbGid = pInfo->gId, .tbUid = pInfo->uid, .tbVer = pInfo->cols.version, .vgId = vgId};
        ST_TASK_DLOG("got virtual table info, gid:%" PRId64 ", uid:%" PRId64 ", ver:%d", pInfo->gId, pInfo->uid,
                     pInfo->cols.version);
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
          code = tSimpleHashPut(pTbInfo->pColumns, pColRef->refColName, colNameLen, &colid, sizeof(col_id_t));
          QUERY_CHECK_CODE(code, lino, _end);
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
      }

      for (int32_t i = 0; i < nVirTables; i++) {
        VTableInfo *pInfo = TARRAY_GET_ELEM(pTask->pVirTableInfoRsp, i);
        void       *px = tSimpleHashGet(pContext->pGroups, &pInfo->gId, sizeof(int64_t));
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

  SMsgSendInfo     *ahandle = pRsp->info.ahandle;
  SSTriggerAHandle *pAhandle = ahandle->param;
  pReq = pAhandle->param;

  ST_TASK_DLOG("receive calc response from task:%" PRIx64 ", code:%d", pReq->runnerTaskId, pRsp->code);

  if (pRsp->code == TSDB_CODE_SUCCESS) {
    code = stTriggerTaskReleaseRequest(pTask, &pReq);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pContext->status == STRIGGER_CONTEXT_ACQUIRE_REQUEST) {
      // continue check if the context is waiting for any available request
      code = stRealtimeContextCheck(pContext);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  } else {
    code = tdListAppend(&pContext->retryCalcReqs, &pReq);
    QUERY_CHECK_CODE(code, lino, _end);
    SListNode *pNode = TD_DLIST_TAIL(&pContext->retryCalcReqs);
    code = stRealtimeContextRetryCalcRequest(pContext, pNode, pReq);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void stHistoryContextDestroyTsdbProgress(void *ptr) {
  SSTriggerTsdbProgress *pProgress = ptr;
  if (pProgress == NULL) {
    return;
  }
  if (pProgress->reqCids != NULL) {
    taosArrayDestroy(pProgress->reqCids);
    pProgress->reqCids = NULL;
  }
  if (pProgress->pMetadatas != NULL) {
    taosArrayDestroyP(pProgress->pMetadatas, (FDelete)blockDataDestroy);
    pProgress->pMetadatas = NULL;
  }
}

static int32_t stHistoryContextInit(SSTriggerHistoryContext *pContext, SStreamTriggerTask *pTask) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSDataBlock *pVirDataBlock = NULL;
  SFilterInfo *pVirDataFilter = NULL;

  pContext->pTask = pTask;
  pContext->sessionId = STREAM_TRIGGER_HISTORY_SESSIONID;
  pContext->status = STRIGGER_CONTEXT_WAIT_RECALC_REQ;
  if (pTask->triggerType == STREAM_TRIGGER_SLIDING) {
    pContext->needTsdbMeta = (pTask->triggerFilter != NULL) || pTask->hasTriggerFilter ||
                             (pTask->placeHolderBitmap & PLACE_HOLDER_WROWNUM) || pTask->ignoreNoDataTrigger;
  } else if (pTask->isVirtualTable || (pTask->triggerType == STREAM_TRIGGER_SESSION) ||
             (pTask->triggerType == STREAM_TRIGGER_COUNT)) {
    pContext->needTsdbMeta = true;
  }

  pContext->pReaderTsdbProgress = tSimpleHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  QUERY_CHECK_NULL(pContext->pReaderTsdbProgress, code, lino, _end, terrno);
  tSimpleHashSetFreeFp(pContext->pReaderTsdbProgress, stHistoryContextDestroyTsdbProgress);
  int32_t nVirReaders = taosArrayGetSize(pTask->virtReaderList);
  int32_t nReaders = taosArrayGetSize(pTask->readerList);
  for (int32_t i = 0; i < nVirReaders + nReaders; i++) {
    SStreamTaskAddr *pReader = NULL;
    if (i < nVirReaders) {
      pReader = TARRAY_GET_ELEM(pTask->virtReaderList, i);
    } else {
      pReader = TARRAY_GET_ELEM(pTask->readerList, i - nVirReaders);
    }
    if (tSimpleHashGet(pContext->pReaderTsdbProgress, &pReader->nodeId, sizeof(int32_t)) != NULL) {
      continue;
    }
    SSTriggerTsdbProgress progress = {0};
    code = tSimpleHashPut(pContext->pReaderTsdbProgress, &pReader->nodeId, sizeof(int32_t), &progress,
                          sizeof(SSTriggerTsdbProgress));
    QUERY_CHECK_CODE(code, lino, _end);
    SSTriggerTsdbProgress *pProgress = tSimpleHashGet(pContext->pReaderTsdbProgress, &pReader->nodeId, sizeof(int32_t));
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
    }
  }

  pContext->pTrigDataBlocks = taosArrayInit(0, POINTER_BYTES);
  QUERY_CHECK_NULL(pContext->pTrigDataBlocks, code, lino, _end, terrno);
  pContext->pCalcDataBlocks = taosArrayInit(0, POINTER_BYTES);
  QUERY_CHECK_NULL(pContext->pCalcDataBlocks, code, lino, _end, terrno);

  pContext->pGroups = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  QUERY_CHECK_NULL(pContext->pGroups, code, lino, _end, terrno);
  tSimpleHashSetFreeFp(pContext->pGroups, stHistoryGroupDestroy);
  TD_DLIST_INIT(&pContext->groupsToCheck);
  int32_t nVirTables = taosArrayGetSize(pTask->pVirTableInfoRsp);
  for (int32_t i = 0; i < nVirTables; i++) {
    VTableInfo *pInfo = TARRAY_GET_ELEM(pTask->pVirTableInfoRsp, i);
    void       *px = tSimpleHashGet(pContext->pGroups, &pInfo->gId, sizeof(int64_t));
    if (px == NULL) {
      SSTriggerHistoryGroup *pGroup = taosMemoryCalloc(1, sizeof(SSTriggerHistoryGroup));
      QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
      code = tSimpleHashPut(pContext->pGroups, &pInfo->gId, sizeof(int64_t), &pGroup, POINTER_BYTES);
      if (code != TSDB_CODE_SUCCESS) {
        taosMemoryFreeClear(pGroup);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      code = stHistoryGroupInit(pGroup, pContext, pInfo->gId);
      QUERY_CHECK_CODE(code, lino, _end);
    }
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
    code = stVtableMergerInit(pContext->pMerger, pTask, &pVirDataBlock, &pVirDataFilter, pTask->nVirDataCols);
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

  tdListInit(&pContext->retryPullReqs, POINTER_BYTES);
  tdListInit(&pContext->retryCalcReqs, POINTER_BYTES);

_end:
  return code;
}

static int32_t stHistoryContextHandleRequest(SSTriggerHistoryContext *pContext, SSTriggerRecalcRequest *pReq) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pContext->pTask;
  pContext->gid = pReq->gid;
  pContext->scanRange = pReq->scanRange;
  pContext->calcRange = pReq->calcRange;
  pContext->stepRange = pContext->scanRange;
  pContext->isHistory = pReq->isHistory;
  int32_t                iter = 0;
  SSTriggerTsdbProgress *pProgress = tSimpleHashIterate(pContext->pReaderTsdbProgress, NULL, &iter);
  while (pProgress != NULL) {
    void *px = tSimpleHashGet(pReq->pTsdbVersions, &pProgress->pTaskAddr->nodeId, sizeof(int32_t));
    QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    pProgress->version = *(int64_t *)px;
    pProgress = tSimpleHashIterate(pContext->pReaderTsdbProgress, pProgress, &iter);
  }
_end:
  return code;
}

static void stHistoryContextDestroy(void *ptr) {
  SSTriggerHistoryContext **ppContext = ptr;
  if (ppContext == NULL || *ppContext == NULL) {
    return;
  }

  SSTriggerHistoryContext *pContext = *ppContext;
  if (pContext->pReaderTsdbProgress != NULL) {
    tSimpleHashCleanup(pContext->pReaderTsdbProgress);
    pContext->pReaderTsdbProgress = NULL;
  }

  if (pContext->pFirstTsMap != NULL) {
    tSimpleHashCleanup(pContext->pFirstTsMap);
    pContext->pFirstTsMap = NULL;
  }
  if (pContext->pTrigDataBlocks != NULL) {
    taosArrayDestroyP(pContext->pTrigDataBlocks, (FDelete)blockDataDestroy);
    pContext->pTrigDataBlocks = NULL;
  }
  if (pContext->pCalcDataBlocks != NULL) {
    taosArrayDestroyP(pContext->pCalcDataBlocks, (FDelete)blockDataDestroy);
    pContext->pCalcDataBlocks = NULL;
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

  tdListEmpty(&pContext->retryPullReqs);
  tdListEmpty(&pContext->retryCalcReqs);

  taosMemFreeClear(*ppContext);
}

static FORCE_INLINE SSTriggerHistoryGroup *stHistoryContextGetCurrentGroup(SSTriggerHistoryContext *pContext) {
  if (TD_DLIST_NELES(&pContext->groupsToCheck) > 0) {
    return TD_DLIST_HEAD(&pContext->groupsToCheck);
  } else if (TD_DLIST_NELES(&pContext->groupsForceClose) > 0) {
    return TD_DLIST_HEAD(&pContext->groupsForceClose);
  } else {
    terrno = TSDB_CODE_INTERNAL_ERROR;
    SStreamTriggerTask *pTask = pContext->pTask;
    ST_TASK_ELOG("failed to get the group in history context %" PRId64, pContext->sessionId);
    return NULL;
  }
}

static int32_t stHistoryContextSendPullReq(SSTriggerHistoryContext *pContext, ESTriggerPullType type) {
  int32_t                code = TSDB_CODE_SUCCESS;
  int32_t                lino = 0;
  SStreamTriggerTask    *pTask = pContext->pTask;
  SSTriggerTsdbProgress *pProgress = NULL;
  SRpcMsg                msg = {.msgType = TDMT_STREAM_TRIGGER_PULL};

  switch (type) {
    case STRIGGER_PULL_FIRST_TS: {
      SStreamTaskAddr *pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
      QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
      pProgress = tSimpleHashGet(pContext->pReaderTsdbProgress, &pReader->nodeId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SSTriggerFirstTsRequest *pReq = &pProgress->pullReq.firstTsReq;
      pReq->startTime = pContext->scanRange.skey;
      pReq->ver = pProgress->version;
      break;
    }

    case STRIGGER_PULL_TSDB_META:
    case STRIGGER_PULL_TSDB_META_NEXT: {
      SStreamTaskAddr *pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
      QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
      pProgress = tSimpleHashGet(pContext->pReaderTsdbProgress, &pReader->nodeId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SSTriggerTsdbMetaRequest *pReq = &pProgress->pullReq.tsdbMetaReq;
      pReq->startTime = pContext->stepRange.skey;
      pReq->endTime = pContext->stepRange.ekey;
      pReq->gid = pContext->gid;
      pReq->order = 1;
      pReq->ver = pProgress->version;
      break;
    }

    case STRIGGER_PULL_TSDB_TS_DATA: {
      SSTriggerTableMeta *pCurTableMeta = pContext->pCurTableMeta;
      SSTriggerMetaData  *pMetaToFetch = pContext->pMetaToFetch;
      pProgress = tSimpleHashGet(pContext->pReaderTsdbProgress, &pCurTableMeta->vgId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SSTriggerTsdbTsDataRequest *pReq = &pProgress->pullReq.tsdbTsDataReq;
      pReq->suid = 0;
      pReq->uid = pCurTableMeta->tbUid;
      pReq->skey = pMetaToFetch->skey;
      pReq->ekey = pMetaToFetch->ekey;
      pReq->ver = pProgress->version;
      break;
    }

    case STRIGGER_PULL_TSDB_TRIGGER_DATA:
    case STRIGGER_PULL_TSDB_TRIGGER_DATA_NEXT: {
      SStreamTaskAddr *pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
      QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
      pProgress = tSimpleHashGet(pContext->pReaderTsdbProgress, &pReader->nodeId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SSTriggerTsdbTriggerDataRequest *pReq = &pProgress->pullReq.tsdbTriggerDataReq;
      pReq->startTime = pContext->scanRange.skey;
      pReq->gid = pContext->gid;
      pReq->order = 1;
      pReq->ver = pProgress->version;
      break;
    }

    case STRIGGER_PULL_TSDB_CALC_DATA:
    case STRIGGER_PULL_TSDB_CALC_DATA_NEXT: {
      SStreamTaskAddr *pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
      QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
      pProgress = tSimpleHashGet(pContext->pReaderTsdbProgress, &pReader->nodeId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SSTriggerTsdbCalcDataRequest *pReq = &pProgress->pullReq.tsdbCalcDataReq;
      SSTriggerHistoryGroup        *pGroup = stHistoryContextGetCurrentGroup(pContext);
      pReq->gid = pGroup->gid;
      pReq->skey = pContext->pParamToFetch->wstart;
      pReq->ekey = pContext->pParamToFetch->wend;
      pReq->ver = pProgress->version;
      break;
    }

    case STRIGGER_PULL_TSDB_DATA: {
      SSTriggerTableColRef *pColRefToFetch = pContext->pColRefToFetch;
      SSTriggerMetaData    *pMetaToFetch = pContext->pMetaToFetch;
      pProgress = tSimpleHashGet(pContext->pReaderTsdbProgress, &pColRefToFetch->otbVgId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SSTriggerTsdbDataRequest *pReq = &pProgress->pullReq.tsdbDataReq;
      pReq->suid = pColRefToFetch->otbSuid;
      pReq->uid = pColRefToFetch->otbUid;
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
      pReq->order = 1;
      pReq->ver = pProgress->version;
      break;
    }

    case STRIGGER_PULL_GROUP_COL_VALUE: {
      SSTriggerHistoryGroup *pGroup = stHistoryContextGetCurrentGroup(pContext);
      QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
      if (pTask->isVirtualTable) {
        SSTriggerVirTableInfo *pTable = taosArrayGetP(pGroup->pVirTableInfos, 0);
        QUERY_CHECK_NULL(pTable, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        pProgress = tSimpleHashGet(pContext->pReaderTsdbProgress, &pTable->vgId, sizeof(int32_t));
        QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      } else {
        int32_t             iter = 0;
        SSTriggerTableMeta *pTable = tSimpleHashIterate(pGroup->pTableMetas, NULL, &iter);
        QUERY_CHECK_NULL(pTable, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        pProgress = tSimpleHashGet(pContext->pReaderTsdbProgress, &pTable->vgId, sizeof(int32_t));
        QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      }
      SSTriggerGroupColValueRequest *pReq = &pProgress->pullReq.groupColValueReq;
      pReq->gid = pGroup->gid;
      break;
    }

    case STRIGGER_PULL_VTABLE_PSEUDO_COL: {
      SSTriggerHistoryGroup *pGroup = stHistoryContextGetCurrentGroup(pContext);
      QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
      QUERY_CHECK_CONDITION(pTask->isVirtualTable, code, lino, _end, TSDB_CODE_INVALID_PARA);
      SSTriggerVirTableInfo *pTable = taosArrayGetP(pGroup->pVirTableInfos, 0);
      QUERY_CHECK_NULL(pTable, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pProgress = tSimpleHashGet(pContext->pReaderTsdbProgress, &pTable->vgId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SSTriggerVirTablePseudoColRequest *pReq = &pProgress->pullReq.virTablePseudoColReq;
      pReq->uid = pTable->tbUid;
      pReq->cids = pProgress->reqCids;
      taosArrayClear(pReq->cids);
      int32_t nCol = taosArrayGetSize(pTask->pVirDataBlock->pDataBlock);
      for (int32_t i = pTask->nVirDataCols; i < nCol; i++) {
        SColumnInfoData *pCol = TARRAY_GET_ELEM(pTask->pVirDataBlock->pDataBlock, i);
        void            *px = taosArrayPush(pReq->cids, &pCol->info.colId);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      }
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
  ST_TASK_DLOG("trigger pull req ahandle %p allocated", msg.info.ahandle);

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
  TRACE_SET_ROOTID(&msg.info.traceId, pTask->task.streamId);
  TRACE_SET_MSGID(&msg.info.traceId, tGenIdPI64());

  code = tmsgSendReq(&pReader->epset, &msg);
  QUERY_CHECK_CODE(code, lino, _end);

  ST_TASK_DLOG("send pull request of type %d to node:%d task:%" PRIx64, pReq->type, pReader->nodeId, pReader->taskId);
  ST_TASK_DLOG("trigger pull req 0x%" PRIx64 ":0x%" PRIx64 " sent", msg.info.traceId.rootId, msg.info.traceId.msgId);

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
    }

    while (TARRAY_ELEM_IDX(pCalcReq->params, pContext->pParamToFetch) < TARRAY_SIZE(pCalcReq->params)) {
      bool allTableProcessed = false;
      bool needFetchData = false;
      bool everGetData = false;
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
        everGetData = true;
        if (!pTask->isVirtualTable) {
          code = putStreamDataCache(pContext->pCalcDataCache, pGroup->gid, pContext->pParamToFetch->wstart,
                                    pContext->pParamToFetch->wend, pDataBlock, startIdx, endIdx - 1);
          QUERY_CHECK_CODE(code, lino, _end);
        } else {
          if (pCalcDataBlock == NULL) {
            code = createOneDataBlock(pTask->pVirDataBlock, false, &pCalcDataBlock);
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
        if (pContext->pColRefToFetch != NULL && pContext->pMetaToFetch != NULL) {
          code = stHistoryContextSendPullReq(pContext, STRIGGER_PULL_TSDB_DATA);
          QUERY_CHECK_CODE(code, lino, _end);
          goto _end;
        } else if (pContext->pColRefToFetch != NULL && pContext->pMetaToFetch == NULL) {
          code = stHistoryContextSendPullReq(pContext, STRIGGER_PULL_VTABLE_PSEUDO_COL);
          QUERY_CHECK_CODE(code, lino, _end);
          goto _end;
        } else {
          QUERY_CHECK_CONDITION(pContext->pMetaToFetch == NULL, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
          taosArrayClearP(pContext->pCalcDataBlocks, (FDelete)blockDataDestroy); 
          pContext->calcDataBlockIdx = 0;
          for (pContext->curReaderIdx = 0; pContext->curReaderIdx < TARRAY_SIZE(pTask->readerList);
               pContext->curReaderIdx++) {
            code = stHistoryContextSendPullReq(
                pContext, everGetData ? STRIGGER_PULL_TSDB_CALC_DATA_NEXT : STRIGGER_PULL_TSDB_CALC_DATA);
            QUERY_CHECK_CODE(code, lino, _end);
          }
          goto _end;
        }
      }
      SSTriggerCalcParam *pNextParam = pContext->pParamToFetch + 1;
      stHistoryGroupClearTempState(pGroup);
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
    ST_TASK_DLOG("[calc param %d]: gid=%" PRId64 ", wstart=%" PRId64 ", wend=%" PRId64 ", nrows=%" PRId64
                 ", prevTs=%" PRId64 ", currentTs=%" PRId64 ", nextTs=%" PRId64 ", prevLocalTime=%" PRId64
                 ", nextLocalTime=%" PRId64 ", localTime=%" PRId64 ", create=%d",
                 i, pCalcReq->gid, pParam->wstart, pParam->wend, pParam->wrownum, pParam->prevTs, pParam->currentTs,
                 pParam->nextTs, pParam->prevLocalTime, pParam->nextLocalTime, pParam->triggerTime,
                 pCalcReq->createTable);
  }

  // serialize and send request
  QUERY_CHECK_CODE(stTriggerTaskAllocAhandle(pTask, pContext->sessionId, pCalcReq, &msg.info.ahandle), lino, _end);
  ST_TASK_DLOG("trigger calc req ahandle %p allocated", msg.info.ahandle);

  msg.contLen = tSerializeSTriggerCalcRequest(NULL, 0, pCalcReq);
  QUERY_CHECK_CONDITION(msg.contLen > 0, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  msg.contLen += sizeof(SMsgHead);
  msg.pCont = rpcMallocCont(msg.contLen);
  QUERY_CHECK_NULL(msg.pCont, code, lino, _end, terrno);
  SMsgHead *pMsgHead = (SMsgHead *)msg.pCont;
  pMsgHead->contLen = htonl(msg.contLen);
  pMsgHead->vgId = htonl(SNODE_HANDLE);
  int32_t tlen =
      tSerializeSTriggerCalcRequest((char *)msg.pCont + sizeof(SMsgHead), msg.contLen - sizeof(SMsgHead), pCalcReq);
  QUERY_CHECK_CONDITION(tlen == msg.contLen - sizeof(SMsgHead), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  TRACE_SET_ROOTID(&msg.info.traceId, pTask->task.streamId);
  TRACE_SET_MSGID(&msg.info.traceId, tGenIdPI64());

  code = tmsgSendReq(&pCalcRunner->addr.epset, &msg);
  QUERY_CHECK_CODE(code, lino, _end);

  ST_TASK_DLOG("send calc request to node:%d task:%" PRIx64, pCalcRunner->addr.nodeId, pCalcRunner->addr.taskId);
  ST_TASK_DLOG("trigger calc req 0x%" PRIx64 ":0x%" PRIx64 " sent", msg.info.traceId.rootId, msg.info.traceId.msgId);

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

static int32_t stHistoryContextRetryPullRequest(SSTriggerHistoryContext *pContext, SListNode *pNode,
                                                SSTriggerPullRequest *pReq) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pContext->pTask;
  SStreamTaskAddr    *pReader = NULL;
  SRpcMsg             msg = {.msgType = TDMT_STREAM_TRIGGER_PULL};

  QUERY_CHECK_NULL(pNode, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pReq, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(*(SSTriggerPullRequest **)pNode->data == pReq, code, lino, _end, TSDB_CODE_INVALID_PARA);

  for (int32_t i = 0; i < taosArrayGetSize(pTask->virtReaderList); i++) {
    SStreamTaskAddr *pTempReader = TARRAY_GET_ELEM(pTask->virtReaderList, i);
    if (pTempReader->taskId == pReq->readerTaskId) {
      pReader = pTempReader;
      break;
    }
  }
  for (int32_t i = 0; i < taosArrayGetSize(pTask->readerList); i++) {
    SStreamTaskAddr *pTempReader = TARRAY_GET_ELEM(pTask->readerList, i);
    if (pTempReader->taskId == pReq->readerTaskId) {
      pReader = pTempReader;
      break;
    }
  }
  QUERY_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

  // serialize and send request
  QUERY_CHECK_CODE(stTriggerTaskAllocAhandle(pTask, pContext->sessionId, pReq, &msg.info.ahandle), lino, _end);
  stDebug("trigger pull req ahandle %p allocated", msg.info.ahandle);

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
  TRACE_SET_ROOTID(&msg.info.traceId, pTask->task.streamId);
  TRACE_SET_MSGID(&msg.info.traceId, tGenIdPI64());

  code = tmsgSendReq(&pReader->epset, &msg);
  QUERY_CHECK_CODE(code, lino, _end);

  ST_TASK_DLOG("send pull request of type %d to node:%d task:%" PRIx64, pReq->type, pReader->nodeId, pReader->taskId);
  ST_TASK_DLOG("trigger pull req 0x%" PRIx64 ":0x%" PRIx64 " sent", msg.info.traceId.rootId, msg.info.traceId.msgId);

  pNode = tdListPopNode(&pContext->retryPullReqs, pNode);
  taosMemoryFreeClear(pNode);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    destroyAhandle(msg.info.ahandle);
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stHistoryContextRetryCalcRequest(SSTriggerHistoryContext *pContext, SListNode *pNode,
                                                SSTriggerCalcRequest *pReq) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  SStreamTriggerTask  *pTask = pContext->pTask;
  SStreamRunnerTarget *pRunner = NULL;
  bool                 needTagValue = false;
  SRpcMsg              msg = {.msgType = TDMT_STREAM_TRIGGER_CALC};

  QUERY_CHECK_NULL(pNode, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pReq, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(*(SSTriggerCalcRequest **)pNode->data == pReq, code, lino, _end, TSDB_CODE_INVALID_PARA);

  int32_t nRunners = taosArrayGetSize(pTask->runnerList);
  for (int32_t i = 0; i < nRunners; i++) {
    SStreamRunnerTarget *pTempRunner = TARRAY_GET_ELEM(pTask->runnerList, i);
    if (pTempRunner->addr.taskId == pReq->runnerTaskId) {
      pRunner = pTempRunner;
      break;
    }
  }
  QUERY_CHECK_NULL(pRunner, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

  pReq->createTable = true;

  if (pReq->createTable && pTask->hasPartitionBy || (pTask->placeHolderBitmap & PLACE_HOLDER_PARTITION_IDX) ||
      (pTask->placeHolderBitmap & PLACE_HOLDER_PARTITION_TBNAME)) {
    needTagValue = true;
  }

  if (needTagValue && taosArrayGetSize(pReq->groupColVals) == 0) {
    code = stHistoryContextSendPullReq(pContext, STRIGGER_PULL_GROUP_COL_VALUE);
    QUERY_CHECK_CODE(code, lino, _end);
    goto _end;
  }

  // serialize and send request
  QUERY_CHECK_CODE(stTriggerTaskAllocAhandle(pTask, pContext->sessionId, pReq, &msg.info.ahandle), lino, _end);
  stDebug("trigger calc req ahandle %p allocated", msg.info.ahandle);

  msg.contLen = tSerializeSTriggerCalcRequest(NULL, 0, pReq);
  QUERY_CHECK_CONDITION(msg.contLen > 0, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  msg.contLen += sizeof(SMsgHead);
  msg.pCont = rpcMallocCont(msg.contLen);
  QUERY_CHECK_NULL(msg.pCont, code, lino, _end, terrno);
  SMsgHead *pMsgHead = (SMsgHead *)msg.pCont;
  pMsgHead->contLen = htonl(msg.contLen);
  pMsgHead->vgId = htonl(SNODE_HANDLE);
  int32_t tlen =
      tSerializeSTriggerCalcRequest((char *)msg.pCont + sizeof(SMsgHead), msg.contLen - sizeof(SMsgHead), pReq);
  QUERY_CHECK_CONDITION(tlen == msg.contLen - sizeof(SMsgHead), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  TRACE_SET_ROOTID(&msg.info.traceId, pTask->task.streamId);
  TRACE_SET_MSGID(&msg.info.traceId, tGenIdPI64());

  code = tmsgSendReq(&pRunner->addr.epset, &msg);
  QUERY_CHECK_CODE(code, lino, _end);

  ST_TASK_DLOG("send calc request to node:%d task:%" PRIx64, pRunner->addr.nodeId, pRunner->addr.taskId);
  ST_TASK_DLOG("trigger calc req 0x%" PRIx64 ":0x%" PRIx64 " sent", msg.info.traceId.rootId, msg.info.traceId.msgId);

  pNode = tdListPopNode(&pContext->retryCalcReqs, pNode);
  taosMemoryFreeClear(pNode);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    destroyAhandle(msg.info.ahandle);
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stHistoryContextAllCalcFinish(SSTriggerHistoryContext *pContext, bool *pFinished) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pContext->pTask;
  bool                needUnlock = false;

  *pFinished = true;

  taosWLockLatch(&pTask->calcPoolLock);
  needUnlock = true;

  int32_t iter = 0;
  void   *px = tSimpleHashIterate(pTask->pGroupRunning, NULL, &iter);
  while (px != NULL) {
    int64_t *pSession = tSimpleHashGetKey(px, NULL);
    if ((*pSession == pContext->sessionId) && *(bool *)px) {
      *pFinished = false;
      break;
    }
    px = tSimpleHashIterate(pTask->pGroupRunning, px, &iter);
  }

_end:
  if (needUnlock) {
    taosWUnLockLatch(&pTask->calcPoolLock);
  }
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stHistoryContextCheck(SSTriggerHistoryContext *pContext) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pContext->pTask;

  if (listNEles(&pContext->retryPullReqs) > 0) {
    while (listNEles(&pContext->retryPullReqs) > 0) {
      SListNode            *pNode = TD_DLIST_HEAD(&pContext->retryPullReqs);
      SSTriggerPullRequest *pReq = *(SSTriggerPullRequest **)pNode->data;
      code = stHistoryContextRetryPullRequest(pContext, pNode, pReq);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    goto _end;
  }

  if (pContext->status == STRIGGER_CONTEXT_WAIT_RECALC_REQ) {
    SSTriggerRecalcRequest *pReq = NULL;
    code = stTriggerTaskFetchRecalcRequest(pTask, &pReq);
    QUERY_CHECK_CODE(code, lino, _end);
    if (pReq == NULL) {
      int64_t resumeTime = taosGetTimestampNs() + STREAM_TRIGGER_WAIT_TIME_NS;
      code = stTriggerTaskAddWaitSession(pTask, pContext->sessionId, resumeTime);
      QUERY_CHECK_CODE(code, lino, _end);
      goto _end;
    }
    code = stHistoryContextHandleRequest(pContext, pReq);
    if (pReq->pTsdbVersions != NULL) {
      tSimpleHashCleanup(pReq->pTsdbVersions);
    }
    taosMemoryFreeClear(pReq);
    QUERY_CHECK_CODE(code, lino, _end);
    pContext->status = STRIGGER_CONTEXT_IDLE;
  }

  if (pContext->status == STRIGGER_CONTEXT_IDLE) {
    pContext->status = STRIGGER_CONTEXT_ADJUST_START;
    if (pContext->pFirstTsMap == NULL) {
      // forward start time to the firstTs of each group
      pContext->pFirstTsMap = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
      QUERY_CHECK_NULL(pContext->pFirstTsMap, code, lino, _end, terrno);
      for (pContext->curReaderIdx = 0; pContext->curReaderIdx < TARRAY_SIZE(pTask->readerList);
           pContext->curReaderIdx++) {
        code = stHistoryContextSendPullReq(pContext, STRIGGER_PULL_FIRST_TS);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      goto _end;
      // TODO(kjq): backward start time to the previous window end of each group
    } else if (pContext->scanRange.skey > pContext->scanRange.ekey) {
      goto _end;
    }

    pContext->status = STRIGGER_CONTEXT_FETCH_META;
    if (pContext->needTsdbMeta) {
      // TODO(kjq): use precision of trigger table
      int64_t step = STREAM_TRIGGER_HISTORY_STEP_MS;
      pContext->stepRange.skey = pContext->scanRange.skey / step * step;
      pContext->stepRange.ekey = pContext->stepRange.skey + step - 1;
      for (pContext->curReaderIdx = 0; pContext->curReaderIdx < TARRAY_SIZE(pTask->readerList);
           pContext->curReaderIdx++) {
        code = stHistoryContextSendPullReq(pContext, STRIGGER_PULL_TSDB_META);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      goto _end;
    } else if (pTask->triggerType != STREAM_TRIGGER_SLIDING) {
      taosArrayClearP(pContext->pTrigDataBlocks, (FDelete)blockDataDestroy);
      for (pContext->curReaderIdx = 0; pContext->curReaderIdx < TARRAY_SIZE(pTask->readerList);
           pContext->curReaderIdx++) {
        code = stHistoryContextSendPullReq(pContext, STRIGGER_PULL_TSDB_TRIGGER_DATA);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      goto _end;
    } else if (listNEles(&pContext->groupsToCheck) == 0) {
      int32_t iter = 0;
      void   *px = tSimpleHashIterate(pContext->pGroups, NULL, &iter);
      while (px != NULL) {
        SSTriggerHistoryGroup *pGroup = *(SSTriggerHistoryGroup **)px;
        TD_DLIST_APPEND(&pContext->groupsToCheck, pGroup);
        px = tSimpleHashIterate(pContext->pGroups, px, &iter);
      }
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
        code = stHistoryGroupCheck(pGroup);
        QUERY_CHECK_CODE(code, lino, _end);
        pContext->reenterCheck = true;
        if (pContext->pColRefToFetch != NULL && pContext->pMetaToFetch != NULL) {
          code = stHistoryContextSendPullReq(pContext, STRIGGER_PULL_TSDB_DATA);
          QUERY_CHECK_CODE(code, lino, _end);
          goto _end;
        } else if (pContext->pColRefToFetch != NULL && pContext->pMetaToFetch == NULL) {
          code = stHistoryContextSendPullReq(pContext, STRIGGER_PULL_VTABLE_PSEUDO_COL);
          QUERY_CHECK_CODE(code, lino, _end);
          goto _end;
        } else if (pContext->pMetaToFetch != NULL) {
          code = stHistoryContextSendPullReq(pContext, STRIGGER_PULL_TSDB_TS_DATA);
          QUERY_CHECK_CODE(code, lino, _end);
          goto _end;
        }

        if (taosArrayGetSize(pContext->pNotifyParams) > 0) {
          code = streamSendNotifyContent(&pTask->task, pTask->streamName, NULL, pTask->triggerType, pGroup->gid,
                                         pTask->pNotifyAddrUrls, pTask->notifyErrorHandle,
                                         TARRAY_DATA(pContext->pNotifyParams), TARRAY_SIZE(pContext->pNotifyParams));
          QUERY_CHECK_CODE(code, lino, _end);
        }
        stHistoryGroupClearTempState(pGroup);
        pContext->status = STRIGGER_CONTEXT_SEND_CALC_REQ;
      }
      case STRIGGER_CONTEXT_SEND_CALC_REQ: {
        int64_t prevWindowEnd = INT64_MIN;
        if (pContext->pCalcReq == NULL) {
          QUERY_CHECK_CONDITION(TARRAY_SIZE(pGroup->pPendingCalcParams) == 0, code, lino, _end,
                                TSDB_CODE_INTERNAL_ERROR);
          // do nothing
        } else {
          if (TARRAY_SIZE(pContext->pCalcReq->params) == 0) {
            int32_t nParams = taosArrayGetSize(pGroup->pPendingCalcParams);
            bool    needCalc = (pTask->lowLatencyCalc && (nParams > 0)) || (nParams >= STREAM_CALC_REQ_MAX_WIN_NUM);
            if (needCalc) {
              SSTriggerCalcParam *pParam = NULL;
              for (int32_t i = 0; i < nParams; i++) {
                pParam = TARRAY_GET_ELEM(pGroup->pPendingCalcParams, i);
                if ((i + 1 < nParams && (pParam + 1)->wstart <= pContext->calcRange.skey) || pGroup->finished) {
                  // skip params out of calc range
                  continue;
                }
                void *px = taosArrayPush(pContext->pCalcReq->params, pParam);
                QUERY_CHECK_NULL(px, code, lino, _end, terrno);
                pGroup->finished = (pParam->wend >= pContext->calcRange.ekey);
                if (TARRAY_SIZE(pContext->pCalcReq->params) >= STREAM_CALC_REQ_MAX_WIN_NUM) {
                  // max windows reached, send calc request
                  break;
                }
              }
              int32_t nCalcParams = TARRAY_ELEM_IDX(pGroup->pPendingCalcParams, pParam) + 1;
              taosArrayPopFrontBatch(pGroup->pPendingCalcParams, nCalcParams);
            }
          }
          if (TARRAY_SIZE(pContext->pCalcReq->params) > 0) {
            SSTriggerCalcParam *pParam = taosArrayGetLast(pContext->pCalcReq->params);
            QUERY_CHECK_NULL(pParam, code, lino, _end, terrno);
            prevWindowEnd = pParam->wend;
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
        }
        stHistoryGroupClearMetadatas(pGroup, prevWindowEnd);
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

  bool finished = true;
  if (TD_DLIST_NELES(&pContext->groupsForceClose) == 0) {
    if (pContext->needTsdbMeta) {
      // TODO(kjq): use precision of trigger table
      int64_t step = STREAM_TRIGGER_HISTORY_STEP_MS;
      QUERY_CHECK_CONDITION(pContext->stepRange.skey + step - 1 == pContext->stepRange.ekey, code, lino, _end,
                            TSDB_CODE_INTERNAL_ERROR);
      finished = (pContext->stepRange.skey + step > pContext->scanRange.ekey);
    } else if (pTask->triggerType != STREAM_TRIGGER_SLIDING) {
      for (int32_t i = 0; i < TARRAY_SIZE(pContext->pTrigDataBlocks); i++) {
        SSDataBlock *pDataBlock = *(SSDataBlock **)TARRAY_GET_ELEM(pContext->pTrigDataBlocks, i);
        if (blockDataGetNumOfRows(pDataBlock) > 0) {
          finished = false;
          break;
        }
      }
    }

    if (finished && pContext->isHistory &&
        (pTask->triggerType == STREAM_TRIGGER_SLIDING || pTask->triggerType == STREAM_TRIGGER_SESSION ||
         pTask->triggerType == STREAM_TRIGGER_STATE)) {
      int32_t iter = 0;
      void   *px = tSimpleHashIterate(pContext->pGroups, NULL, &iter);
      while (px != NULL) {
        SSTriggerHistoryGroup *pGroup = *(SSTriggerHistoryGroup **)px;
        if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
          TD_DLIST_APPEND(&pContext->groupsForceClose, pGroup);
        }
        px = tSimpleHashIterate(pContext->pGroups, px, &iter);
      }
    }
  }

  while (TD_DLIST_NELES(&pContext->groupsForceClose) > 0) {
    SSTriggerHistoryGroup *pGroup = TD_DLIST_HEAD(&pContext->groupsForceClose);
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
          TRINGBUF_MOVE_NEXT(&pGroup->winBuf, p);
        } while (p != TRINGBUF_TAIL(&pGroup->winBuf));

        if (taosArrayGetSize(pContext->pNotifyParams) > 0) {
          code = streamSendNotifyContent(&pTask->task, pTask->streamName, NULL, pTask->triggerType, pGroup->gid,
                                         pTask->pNotifyAddrUrls, pTask->notifyErrorHandle,
                                         TARRAY_DATA(pContext->pNotifyParams), TARRAY_SIZE(pContext->pNotifyParams));
          QUERY_CHECK_CODE(code, lino, _end);
        }
        stHistoryGroupClearTempState(pGroup);
        pContext->status = STRIGGER_CONTEXT_SEND_CALC_REQ;
      }
      case STRIGGER_CONTEXT_SEND_CALC_REQ: {
        int32_t nParams = taosArrayGetSize(pContext->pCalcReq->params);
        bool    needCalc = (nParams > 0);
        if (needCalc) {
          QUERY_CHECK_NULL(pContext->pCalcReq, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
          QUERY_CHECK_CONDITION(nParams <= STREAM_CALC_REQ_MAX_WIN_NUM, code, lino, _end, TSDB_CODE_INVALID_PARA);
          code = stHistoryContextSendCalcReq(pContext);
          QUERY_CHECK_CODE(code, lino, _end);
          if (pContext->pCalcReq != NULL) {
            // calc req has not been sent
            goto _end;
          }
          stHistoryGroupClearTempState(pGroup);
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
    TD_DLIST_POP(&pContext->groupsForceClose, pGroup);
    pContext->status = STRIGGER_CONTEXT_ACQUIRE_REQUEST;
  }

  if (!finished) {
    pContext->status = STRIGGER_CONTEXT_FETCH_META;
    if (pContext->needTsdbMeta) {
      // TODO(kjq): use precision of trigger table
      int64_t step = STREAM_TRIGGER_HISTORY_STEP_MS;
      pContext->stepRange.skey += step;
      pContext->stepRange.ekey += step;
      for (pContext->curReaderIdx = 0; pContext->curReaderIdx < TARRAY_SIZE(pTask->readerList);
           pContext->curReaderIdx++) {
        code = stHistoryContextSendPullReq(pContext, STRIGGER_PULL_TSDB_META);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    } else if (pTask->triggerType != STREAM_TRIGGER_SLIDING) {
      taosArrayClearP(pContext->pTrigDataBlocks, (FDelete)blockDataDestroy);
      for (pContext->curReaderIdx = 0; pContext->curReaderIdx < TARRAY_SIZE(pTask->readerList);
           pContext->curReaderIdx++) {
        code = stHistoryContextSendPullReq(pContext, STRIGGER_PULL_TSDB_TRIGGER_DATA_NEXT);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
  } else {
    bool calcFinish = false;
    code = stHistoryContextAllCalcFinish(pContext, &calcFinish);
    QUERY_CHECK_CODE(code, lino, _end);
    if (calcFinish) {
      stHistoryContextDestroy(&pTask->pHistoryContext);
      pTask->pHistoryContext = taosMemoryCalloc(1, sizeof(SSTriggerHistoryContext));
      QUERY_CHECK_NULL(pTask->pHistoryContext, code, lino, _end, terrno);
      pContext = pTask->pHistoryContext;
      code = stHistoryContextInit(pContext, pTask);
      QUERY_CHECK_CODE(code, lino, _end);
      int64_t resumeTime = taosGetTimestampNs() + STREAM_TRIGGER_WAIT_TIME_NS;
      code = stTriggerTaskAddWaitSession(pTask, pContext->sessionId, resumeTime);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      pContext->pendingToFinish = true;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stHistoryContextProcPullRsp(SSTriggerHistoryContext *pContext, SRpcMsg *pRsp) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pContext->pTask;
  SSDataBlock        *pDataBlock = NULL;
  SArray             *pAllMetadatas = NULL;
  SArray             *pVgIds = NULL;

  QUERY_CHECK_CONDITION(pRsp->code == TSDB_CODE_SUCCESS || pRsp->code == TSDB_CODE_STREAM_NO_DATA ||
                            pRsp->code == TSDB_CODE_STREAM_NO_CONTEXT,
                        code, lino, _end, TSDB_CODE_INVALID_PARA);

  SMsgSendInfo         *ahandle = pRsp->info.ahandle;
  SSTriggerAHandle     *pAhandle = ahandle->param;
  SSTriggerPullRequest *pReq = pAhandle->param;

  ST_TASK_DLOG("receive pull response of type %d from task:%" PRIx64, pReq->type, pReq->readerTaskId);

  switch (pReq->type) {
    case STRIGGER_PULL_FIRST_TS: {
      QUERY_CHECK_CONDITION(pContext->status == STRIGGER_CONTEXT_ADJUST_START, code, lino, _end,
                            TSDB_CODE_INTERNAL_ERROR);
      SSTriggerTsdbProgress *pProgress = NULL;
      for (int32_t i = 0; i < TARRAY_SIZE(pTask->readerList); i++) {
        SStreamTaskAddr       *pReader = TARRAY_GET_ELEM(pTask->readerList, i);
        SSTriggerTsdbProgress *pTempProgress =
            tSimpleHashGet(pContext->pReaderTsdbProgress, &pReader->nodeId, sizeof(int32_t));
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

      int32_t nrows = blockDataGetNumOfRows(pDataBlock);
      if (nrows > 0) {
        SColumnInfoData *pGidCol = taosArrayGet(pDataBlock->pDataBlock, 0);
        QUERY_CHECK_NULL(pGidCol, code, lino, _end, terrno);
        int64_t         *pGidData = (int64_t *)pGidCol->pData;
        SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, 1);
        QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
        int64_t *pTsData = (int64_t *)pTsCol->pData;
        for (int32_t i = 0; i < nrows; i++) {
          if (pTask->isVirtualTable) {
            int32_t iter = 0;
            void   *px = tSimpleHashIterate(pContext->pGroups, NULL, &iter);
            while (px != NULL) {
              SSTriggerHistoryGroup *pGroup = *(SSTriggerHistoryGroup **)px;
              bool inGroup = (tSimpleHashGet(pGroup->pTableMetas, &pGidData[i], sizeof(int64_t)) != NULL);
              if (inGroup) {
                void *px2 = tSimpleHashGet(pContext->pFirstTsMap, &pGroup->gid, sizeof(int64_t));
                if (px2 == NULL) {
                  code = tSimpleHashPut(pContext->pFirstTsMap, &pGroup->gid, sizeof(int64_t), &pTsData[i],
                                        sizeof(int64_t));
                  QUERY_CHECK_CODE(code, lino, _end);
                } else {
                  *(int64_t *)px2 = TMIN(*(int64_t *)px2, pTsData[i]);
                }
              }
              px = tSimpleHashIterate(pContext->pGroups, px, &iter);
            }
            continue;
          }
          void *px = tSimpleHashGet(pContext->pFirstTsMap, &pGidData[i], sizeof(int64_t));
          if (px == NULL) {
            code = tSimpleHashPut(pContext->pFirstTsMap, &pGidData[i], sizeof(int64_t), &pTsData[i], sizeof(int64_t));
            QUERY_CHECK_CODE(code, lino, _end);
          } else {
            *(int64_t *)px = TMIN(*(int64_t *)px, pTsData[i]);
          }
          px = tSimpleHashGet(pContext->pGroups, &pGidData[i], sizeof(int64_t));
          if (px == NULL) {
            SSTriggerHistoryGroup *pGroup = taosMemoryCalloc(1, sizeof(SSTriggerHistoryGroup));
            QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
            code = tSimpleHashPut(pContext->pGroups, &pGidData[i], sizeof(int64_t), &pGroup, POINTER_BYTES);
            if (code != TSDB_CODE_SUCCESS) {
              taosMemoryFreeClear(pGroup);
              QUERY_CHECK_CODE(code, lino, _end);
            }
            code = stHistoryGroupInit(pGroup, pContext, pGidData[i]);
            QUERY_CHECK_CODE(code, lino, _end);
            if (!pContext->needTsdbMeta && (tSimpleHashGetSize(pGroup->pTableMetas) == 0)) {
              SSTriggerTableMeta newTableMeta = {.vgId = vgId};
              code = tSimpleHashPut(pGroup->pTableMetas, &newTableMeta.tbUid, sizeof(int64_t), &newTableMeta,
                                    sizeof(SSTriggerTableMeta));
              QUERY_CHECK_CODE(code, lino, _end);
            }
          }
        }
      }

      if (--pContext->curReaderIdx > 0) {
        // wait for responses from other readers
        goto _end;
      }

      int32_t iter = 0;
      void   *px = tSimpleHashIterate(pContext->pFirstTsMap, NULL, &iter);
      while (px != NULL) {
        pContext->scanRange.skey = TMAX(pContext->scanRange.skey, *(int64_t *)px);
        px = tSimpleHashIterate(pContext->pFirstTsMap, px, &iter);
      }

      pContext->status = STRIGGER_CONTEXT_IDLE;
      code = stHistoryContextCheck(pContext);
      QUERY_CHECK_CODE(code, lino, _end);
      break;
    }

    case STRIGGER_PULL_TSDB_META:
    case STRIGGER_PULL_TSDB_META_NEXT: {
      QUERY_CHECK_CONDITION(pContext->status == STRIGGER_CONTEXT_FETCH_META, code, lino, _end,
                            TSDB_CODE_INTERNAL_ERROR);
      SSTriggerTsdbProgress *pProgress = NULL;
      for (int32_t i = 0; i < TARRAY_SIZE(pTask->readerList); i++) {
        SStreamTaskAddr       *pReader = TARRAY_GET_ELEM(pTask->readerList, i);
        SSTriggerTsdbProgress *pTempProgress =
            tSimpleHashGet(pContext->pReaderTsdbProgress, &pReader->nodeId, sizeof(int32_t));
        QUERY_CHECK_NULL(pTempProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        if (&pTempProgress->pullReq.base == pReq) {
          pProgress = pTempProgress;
          break;
        }
      }
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INVALID_PARA);

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

      void *px = taosArrayPush(pProgress->pMetadatas, &pDataBlock);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      pDataBlock = NULL;

      if (--pContext->curReaderIdx > 0) {
        ST_TASK_DLOG("wait for response from other %d readers", pContext->curReaderIdx);
        goto _end;
      }

      bool continueToFetch = false;
      for (int32_t i = 0; i < TARRAY_SIZE(pTask->readerList); i++) {
        SStreamTaskAddr       *pReader = TARRAY_GET_ELEM(pTask->readerList, i);
        SSTriggerTsdbProgress *pTempProgress =
            tSimpleHashGet(pContext->pReaderTsdbProgress, &pReader->nodeId, sizeof(int32_t));
        QUERY_CHECK_NULL(pTempProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        SSDataBlock *pBlock = *(SSDataBlock **)taosArrayGetLast(pTempProgress->pMetadatas);
        int32_t      nrows = blockDataGetNumOfRows(pBlock);
        if (nrows >= STREAM_RETURN_ROWS_NUM) {
          continueToFetch = true;
          break;
        }
      }

      if (continueToFetch) {
        ST_TASK_DLOG("continue to fetch wal metas since some readers are not exhausted: %" PRIzu,
                     TARRAY_SIZE(pTask->readerList));
        for (pContext->curReaderIdx = 0; pContext->curReaderIdx < TARRAY_SIZE(pTask->readerList);
             pContext->curReaderIdx++) {
          code = stHistoryContextSendPullReq(pContext, STRIGGER_PULL_TSDB_META_NEXT);
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
        SStreamTaskAddr       *pReader = TARRAY_GET_ELEM(pTask->readerList, i);
        SSTriggerTsdbProgress *pTempProgress =
            tSimpleHashGet(pContext->pReaderTsdbProgress, &pReader->nodeId, sizeof(int32_t));
        QUERY_CHECK_NULL(pTempProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        void *px = taosArrayAddAll(pAllMetadatas, pTempProgress->pMetadatas);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
        for (int32_t j = 0; j < TARRAY_SIZE(pTempProgress->pMetadatas); j++) {
          void *px = taosArrayPush(pVgIds, &pTempProgress->pTaskAddr->nodeId);
          QUERY_CHECK_NULL(px, code, lino, _end, terrno);
        }
        taosArrayClear(pTempProgress->pMetadatas);
      }

      if (!pTask->isVirtualTable) {
        for (int32_t i = 0; i < TARRAY_SIZE(pAllMetadatas); i++) {
          SSDataBlock *pBlock = *(SSDataBlock **)TARRAY_GET_ELEM(pAllMetadatas, i);
          int32_t      nrows = blockDataGetNumOfRows(pBlock);
          if (nrows == 0) {
            continue;
          }
          SColumnInfoData *pGidCol = taosArrayGet(pBlock->pDataBlock, 3);
          QUERY_CHECK_NULL(pGidCol, code, lino, _end, terrno);
          int64_t *pGidData = (int64_t *)pGidCol->pData;
          for (int32_t i = 0; i < nrows; i++) {
            void                  *px = tSimpleHashGet(pContext->pGroups, &pGidData[i], sizeof(int64_t));
            SSTriggerHistoryGroup *pGroup = NULL;
            if (px == NULL) {
              pGroup = taosMemoryCalloc(1, sizeof(SSTriggerHistoryGroup));
              QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
              code = tSimpleHashPut(pContext->pGroups, &pGidData[i], sizeof(int64_t), &pGroup, POINTER_BYTES);
              if (code != TSDB_CODE_SUCCESS) {
                taosMemoryFreeClear(pGroup);
                QUERY_CHECK_CODE(code, lino, _end);
              }
              code = stHistoryGroupInit(pGroup, pContext, pGidData[i]);
              QUERY_CHECK_CODE(code, lino, _end);
            } else {
              pGroup = *(SSTriggerHistoryGroup **)px;
            }
            if (TD_DLIST_NODE_NEXT(pGroup) == NULL && TD_DLIST_TAIL(&pContext->groupsToCheck) != pGroup) {
              bool added = false;
              code = stHistoryGroupAddMetaDatas(pGroup, pAllMetadatas, pVgIds, &added);
              QUERY_CHECK_CODE(code, lino, _end);
              if (added) {
                TD_DLIST_APPEND(&pContext->groupsToCheck, pGroup);
              }
            }
          }
        }
      } else {
        int32_t iter = 0;
        void   *px = tSimpleHashIterate(pContext->pGroups, NULL, &iter);
        while (px != NULL) {
          SSTriggerHistoryGroup *pGroup = *(SSTriggerHistoryGroup **)px;
          bool                   added = false;
          code = stHistoryGroupAddMetaDatas(pGroup, pAllMetadatas, pVgIds, &added);
          QUERY_CHECK_CODE(code, lino, _end);
          if (added) {
            TD_DLIST_APPEND(&pContext->groupsToCheck, pGroup);
          }
          px = tSimpleHashIterate(pContext->pGroups, px, &iter);
        }
      }

      code = stHistoryContextCheck(pContext);
      QUERY_CHECK_CODE(code, lino, _end);
      break;
    }

    case STRIGGER_PULL_TSDB_TRIGGER_DATA:
    case STRIGGER_PULL_TSDB_TRIGGER_DATA_NEXT: {
      QUERY_CHECK_CONDITION(pContext->status == STRIGGER_CONTEXT_FETCH_META, code, lino, _end,
                            TSDB_CODE_INTERNAL_ERROR);
      SSTriggerTsdbProgress *pProgress = NULL;
      for (int32_t i = 0; i < TARRAY_SIZE(pTask->readerList); i++) {
        SStreamTaskAddr       *pReader = TARRAY_GET_ELEM(pTask->readerList, i);
        SSTriggerTsdbProgress *pTempProgress =
            tSimpleHashGet(pContext->pReaderTsdbProgress, &pReader->nodeId, sizeof(int32_t));
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
      if (pRsp->contLen > 0) {
        const char *pCont = pRsp->pCont;
        code = blockDecode(pDataBlock, pCont, &pCont);
        QUERY_CHECK_CODE(code, lino, _end);
        QUERY_CHECK_CONDITION(pCont == (char *)pRsp->pCont + pRsp->contLen, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      } else {
        blockDataEmpty(pDataBlock);
      }

      void *px = taosArrayPush(pContext->pTrigDataBlocks, &pDataBlock);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      pDataBlock = NULL;

      if (--pContext->curReaderIdx > 0) {
        ST_TASK_DLOG("wait for response from other %d readers", pContext->curReaderIdx);
        goto _end;
      }

      for (int32_t i = 0; i < TARRAY_SIZE(pContext->pTrigDataBlocks); i++) {
        SSDataBlock *pBlock = *(SSDataBlock **)TARRAY_GET_ELEM(pContext->pTrigDataBlocks, i);
        int32_t      nrows = blockDataGetNumOfRows(pBlock);
        if (nrows == 0) {
          continue;
        }
        int64_t                gid = pBlock->info.id.groupId;
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
          code = stHistoryGroupInit(pGroup, pContext, gid);
          QUERY_CHECK_CODE(code, lino, _end);
        } else {
          pGroup = *(SSTriggerHistoryGroup **)px;
        }
        if (!pContext->needTsdbMeta && (tSimpleHashGetSize(pGroup->pTableMetas) == 0)) {
          SSTriggerTableMeta newTableMeta = {.vgId = vgId};
          code = tSimpleHashPut(pGroup->pTableMetas, &newTableMeta.tbUid, sizeof(int64_t), &newTableMeta,
                                sizeof(SSTriggerTableMeta));
          QUERY_CHECK_CODE(code, lino, _end);
        }
        if (TD_DLIST_NODE_NEXT(pGroup) == NULL && TD_DLIST_TAIL(&pContext->groupsToCheck) != pGroup) {
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
      QUERY_CHECK_CONDITION(pContext->pColRefToFetch == NULL, code, lino, _end, TSDB_CODE_INVALID_PARA);
      code = stTimestampSorterBindDataBlock(pContext->pSorter, &pDataBlock);
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

      void *px = taosArrayPush(pContext->pCalcDataBlocks, &pDataBlock);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      pDataBlock = NULL;

      if (--pContext->curReaderIdx > 0) {
        ST_TASK_DLOG("wait for response from other %d readers", pContext->curReaderIdx);
        goto _end;
      }

      code = stHistoryContextCheck(pContext);
      QUERY_CHECK_CODE(code, lino, _end);
      break;
    }

    case STRIGGER_PULL_TSDB_DATA: {
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
      QUERY_CHECK_NULL(pContext->pColRefToFetch, code, lino, _end, TSDB_CODE_INVALID_PARA);
      code = stVtableMergerBindDataBlock(pContext->pMerger, &pDataBlock);
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
      SSTriggerGroupColValueRequest *pRequest = (SSTriggerGroupColValueRequest *)pReq;
      if (pContext->pCalcReq != NULL && pContext->pCalcReq->gid == pRequest->gid) {
        SStreamGroupInfo groupInfo = {.gInfo = pContext->pCalcReq->groupColVals};
        code = tDeserializeSStreamGroupInfo(pRsp->pCont, pRsp->contLen, &groupInfo);
        QUERY_CHECK_CODE(code, lino, _end);
        code = stHistoryContextCheck(pContext);
        QUERY_CHECK_CODE(code, lino, _end);
      } else {
        SListIter  iter = {0};
        SListNode *pNode = NULL;
        tdListInitIter(&pContext->retryCalcReqs, &iter, TD_LIST_FORWARD);
        while ((pNode = tdListNext(&iter)) != NULL) {
          SSTriggerCalcRequest *pCalcReq = *(SSTriggerCalcRequest **)pNode->data;
          if (pCalcReq->gid == pRequest->gid) {
            SStreamGroupInfo groupInfo = {.gInfo = pCalcReq->groupColVals};
            code = tDeserializeSStreamGroupInfo(pRsp->pCont, pRsp->contLen, &groupInfo);
            QUERY_CHECK_CODE(code, lino, _end);
            code = stHistoryContextRetryCalcRequest(pContext, pNode, pCalcReq);
            QUERY_CHECK_CODE(code, lino, _end);
            break;
          }
        }
      }
      break;
    }

    case STRIGGER_PULL_VTABLE_PSEUDO_COL: {
      QUERY_CHECK_CONDITION(
          pContext->status == STRIGGER_CONTEXT_SEND_CALC_REQ || pContext->status == STRIGGER_CONTEXT_CHECK_CONDITION,
          code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pDataBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
      QUERY_CHECK_NULL(pDataBlock, code, lino, _end, terrno);
      QUERY_CHECK_CONDITION(pRsp->contLen > 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
      const char *pCont = pRsp->pCont;
      code = blockDecode(pDataBlock, pCont, &pCont);
      QUERY_CHECK_CODE(code, lino, _end);
      QUERY_CHECK_CONDITION(pCont == (char *)pRsp->pCont + pRsp->contLen, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      QUERY_CHECK_NULL(pContext->pColRefToFetch, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      code = stVtableMergerSetPseudoCols(pContext->pMerger, &pDataBlock);
      QUERY_CHECK_CODE(code, lino, _end);
      pContext->pColRefToFetch = NULL;
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
  if (pDataBlock != NULL) {
    blockDataDestroy(pDataBlock);
  }
  if (pAllMetadatas != NULL) {
    taosArrayDestroyP(pAllMetadatas, (FDelete)blockDataDestroy);
  }
  if (pVgIds != NULL) {
    taosArrayDestroy(pVgIds);
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

  SMsgSendInfo     *ahandle = pRsp->info.ahandle;
  SSTriggerAHandle *pAhandle = ahandle->param;
  pReq = pAhandle->param;

  ST_TASK_DLOG("receive calc response from task:%" PRIx64 ", code:%d", pReq->runnerTaskId, pRsp->code);

  if (pRsp->code == TSDB_CODE_SUCCESS) {
    code = stTriggerTaskReleaseRequest(pTask, &pReq);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pContext->pendingToFinish) {
      bool calcFinish = false;
      code = stHistoryContextAllCalcFinish(pContext, &calcFinish);
      QUERY_CHECK_CODE(code, lino, _end);
      if (calcFinish) {
        stHistoryContextDestroy(&pTask->pHistoryContext);
        pTask->pHistoryContext = taosMemoryCalloc(1, sizeof(SSTriggerHistoryContext));
        QUERY_CHECK_NULL(pTask->pHistoryContext, code, lino, _end, terrno);
        pContext = pTask->pHistoryContext;
        code = stHistoryContextInit(pContext, pTask);
        QUERY_CHECK_CODE(code, lino, _end);
        int64_t resumeTime = taosGetTimestampNs() + STREAM_TRIGGER_WAIT_TIME_NS;
        code = stTriggerTaskAddWaitSession(pTask, pContext->sessionId, resumeTime);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    } else if (pContext->status == STRIGGER_CONTEXT_ACQUIRE_REQUEST) {
      // continue check if the context is waiting for any available request
      code = stHistoryContextCheck(pContext);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  } else {
    code = tdListAppend(&pContext->retryCalcReqs, &pReq);
    QUERY_CHECK_CODE(code, lino, _end);
    SListNode *pNode = TD_DLIST_TAIL(&pContext->retryCalcReqs);
    code = stHistoryContextRetryCalcRequest(pContext, pNode, pReq);
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
  if (pTask->fillHistoryFirst && pTask->fillHistoryStartTime > 0) {
    pGroup->oldThreshold = pTask->fillHistoryStartTime - 1;
  }
#endif
  if (pTask->fillHistory) {
    void *px = tSimpleHashGet(pTask->pHistoryCutoffTime, &gid, sizeof(int64_t));
    if (px != NULL) {
      pGroup->oldThreshold = TMAX(pGroup->oldThreshold, *(int64_t *)px);
    }
  }

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

  pGroup->recalcNextWindow = pTask->fillHistory;
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

static void stRealtimeGroupClearMetadatas(SSTriggerRealtimeGroup *pGroup, int64_t prevWindowEnd) {
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  if (pContext->needCheckAgain) {
    return;
  }

  int32_t             iter = 0;
  SSTriggerTableMeta *pTableMeta = tSimpleHashIterate(pGroup->pTableMetas, NULL, &iter);
  while (pTableMeta != NULL) {
    if (taosArrayGetSize(pTableMeta->pMetas) > 0) {
      int64_t endTime = prevWindowEnd;
      if (pTask->placeHolderBitmap & PLACE_HOLDER_PARTITION_ROWS) {
        if (TARRAY_SIZE(pGroup->pPendingCalcParams) > 0) {
          SSTriggerCalcParam *pParam = TARRAY_DATA(pGroup->pPendingCalcParams);
          endTime = TMAX(endTime, pParam->wstart - 1);
        } else if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
          endTime = TMAX(endTime, TRINGBUF_HEAD(&pGroup->winBuf)->range.skey - 1);
        } else {
          endTime = TMAX(endTime, pGroup->newThreshold);
        }
      } else {
        endTime = TMAX(endTime, pGroup->newThreshold);
      }
      if (endTime == INT64_MAX) {
        taosArrayClear(pTableMeta->pMetas);
        pTableMeta->metaIdx = 0;
      } else {
        int32_t idx = taosArraySearchIdx(pTableMeta->pMetas, &endTime, stRealtimeGroupMetaDataSearch, TD_GT);
        taosArrayPopFrontBatch(pTableMeta->pMetas, (idx == -1) ? TARRAY_SIZE(pTableMeta->pMetas) : idx);
        idx = taosArraySearchIdx(pTableMeta->pMetas, &pGroup->newThreshold, stRealtimeGroupMetaDataSearch, TD_GT);
        pTableMeta->metaIdx = (idx == -1) ? TARRAY_SIZE(pTableMeta->pMetas) : idx;
      }
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
  STimeWindow               recalcRange = {.skey = INT64_MAX, .ekey = INT64_MIN};

  QUERY_CHECK_NULL(pMetadatas, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(taosArrayGetSize(pMetadatas) == taosArrayGetSize(pVgIds), code, lino, _end,
                        TSDB_CODE_INVALID_PARA);

  if (pTask->triggerType == STREAM_TRIGGER_PERIOD) {
    pGroup->oldThreshold = INT64_MIN;
  }
  pGroup->newThreshold = pGroup->oldThreshold;
  pAddedUids = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  QUERY_CHECK_NULL(pAddedUids, code, lino, _end, terrno);

  bool hasData = false;

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
      hasData = true;

      if (pSkeys[i] <= pGroup->oldThreshold &&
          ((pTypes[i] == WAL_DELETE_DATA) || (pTypes[i] == WAL_SUBMIT_DATA && !pTask->ignoreDisorder))) {
        // mark recalc time range for disordered data
        int64_t recalcSkey = pSkeys[i];
        if (pTask->expiredTime > 0) {
          recalcSkey = TMAX(recalcSkey, pGroup->oldThreshold - pTask->expiredTime + 1);
        }
        int64_t recalcEkey = TMIN(pEkeys[i], pGroup->oldThreshold);
        if (recalcSkey <= recalcEkey) {
          recalcRange.skey = TMIN(recalcRange.skey, recalcSkey);
          recalcRange.ekey = TMAX(recalcRange.ekey, recalcEkey);
        }
      }

      if (pEkeys[i] <= pGroup->oldThreshold) {
        continue;
      }

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
            SSTriggerMetaData newMeta = *pMeta;
            newMeta.skey = pEkeys[i] + 1;
            SET_TRIGGER_META_SKEY_INACCURATE(&newMeta);
            pMeta->ekey = pSkeys[i] - 1;
            SET_TRIGGER_META_EKEY_INACCURATE(pMeta);
            void *px = taosArrayPush(pTableMeta->pMetas, &newMeta);
            QUERY_CHECK_NULL(px, code, lino, _end, terrno);
            continue;
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
          SSTriggerMetaData *pLastMeta = taosArrayGetLast(pTableMeta->pMetas);
          skey = TMAX(skey, pLastMeta->ekey + 1);
        }
        if (skey <= pEkeys[i]) {
          SSTriggerMetaData *pNewMeta = taosArrayReserve(pTableMeta->pMetas, 1);
          QUERY_CHECK_NULL(pNewMeta, code, lino, _end, terrno);
          pNewMeta->skey = skey;
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

  // add recalc request
  if (recalcRange.skey <= recalcRange.ekey) {
    if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup) && TRINGBUF_HEAD(&pGroup->winBuf)->range.skey <= recalcRange.ekey) {
      pGroup->recalcNextWindow = true;
    }
    code = stTriggerTaskAddRecalcRequest(pTask, pGroup, &recalcRange, pContext->pReaderWalProgress, false);
    QUERY_CHECK_CODE(code, lino, _end);
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
    if(hasData) pGroup->newThreshold = INT64_MAX;
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
  // todo(kjq): should delete (pTask->placeHolderBitmap & PLACE_HOLDER_WROWNUM)
  if (pTask->ignoreNoDataTrigger && param.wrownum == 0 && (pTask->placeHolderBitmap & PLACE_HOLDER_WROWNUM)) {
    needCalc = needNotify = false;
  }

  switch (pTask->triggerType) {
    case STREAM_TRIGGER_PERIOD: {
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
    if (!IS_TRIGGER_GROUP_NONE_WINDOW(pGroup)) {
      void *px = taosArrayPush(pInitWindows, &pGroup->nextWindow);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
    }
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
    if (nWindows > 0) {
      pGroup->nextWindow = *(STimeWindow *)taosArrayGetLast(pInitWindows);
      nWindows--;
    } else {
      TRINGBUF_DESTROY(&pGroup->winBuf);
      pGroup->nextWindow = (STimeWindow){0};
    }
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
      if (pTask->triggerType == STREAM_TRIGGER_SLIDING) {
        STimeWindow prevWindow = pWin->range;
        stTriggerTaskPrevIntervalWindow(pTask, &prevWindow);
        STimeWindow nextWindow = pWin->range;
        stTriggerTaskNextIntervalWindow(pTask, &nextWindow);
        param.prevTs = prevWindow.skey;
        param.currentTs = pWin->range.skey;
        param.nextTs = nextWindow.skey;
      }
      bool ignore = pTask->ignoreNoDataTrigger && (param.wrownum == 0);
      if (calcOpen && !ignore) {
        void *px = taosArrayPush(pGroup->pPendingCalcParams, &param);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      } else if (notifyOpen && !ignore) {
        void *px = taosArrayPush(pContext->pNotifyParams, &param);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      }
    }

    // some window may have not been closed yet
    if (pWin->range.ekey + gap > pGroup->newThreshold) {
      // TODO(kjq): restore prevProcTime from saved init windows
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
      if (pTask->triggerType == STREAM_TRIGGER_SLIDING) {
        if (pTask->interval.interval == 0) {
          param.prevTs = pWin->range.skey - 1;
          param.currentTs = pWin->range.ekey;
          STimeWindow nextWindow = pWin->range;
          stTriggerTaskNextPeriodWindow(pTask, &nextWindow);
          param.nextTs = nextWindow.ekey;
        } else {
          STimeWindow prevWindow = pWin->range;
          stTriggerTaskPrevIntervalWindow(pTask, &prevWindow);
          STimeWindow nextWindow = pWin->range;
          stTriggerTaskNextIntervalWindow(pTask, &nextWindow);
          param.prevTs = prevWindow.ekey + 1;
          param.currentTs = pWin->range.ekey + 1;
          param.nextTs = nextWindow.ekey + 1;
        }
      }
      bool ignore = pTask->ignoreNoDataTrigger && (param.wrownum == 0);
      if (notifyClose) {
        if ((pTask->triggerType == STREAM_TRIGGER_PERIOD) ||
            (pTask->triggerType == STREAM_TRIGGER_SLIDING && pTask->interval.interval == 0)) {
          param.notifyType = STRIGGER_EVENT_ON_TIME;
        } else {
          param.notifyType = STRIGGER_EVENT_WINDOW_CLOSE;
        }
      }
      if (calcClose && !ignore) {
        void *px = taosArrayPush(pGroup->pPendingCalcParams, &param);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      } else if (notifyClose && !ignore) {
        void *px = taosArrayPush(pContext->pNotifyParams, &param);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      }
    }
  }

  if (pTask->triggerType == STREAM_TRIGGER_SLIDING) {
    pWin = taosArrayGetLast(pContext->pSavedWindows);
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

  if (!pContext->reenterCheck) {
    // save initial windows at the first check
    code = stRealtimeGroupSaveInitWindow(pGroup, pContext->pInitWindows);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if ((pTask->triggerFilter != NULL) || pTask->hasTriggerFilter) {
    readAllData = true;
  } else if (pTask->placeHolderBitmap & PLACE_HOLDER_WROWNUM) {
    readAllData = true;
  } else if (pTask->ignoreNoDataTrigger) {
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
        bool meetBound = (r < endIdx) || (r > 0 && pTsData[r - 1] == ts);
        if (ts == nextStart && meetBound) {
          if (IS_TRIGGER_GROUP_NONE_WINDOW(pGroup)) {
            code = stRealtimeGroupOpenWindow(pGroup, pTsData[r], NULL, true, true);
            QUERY_CHECK_CODE(code, lino, _end);
            r++;
          } else {
            code = stRealtimeGroupOpenWindow(pGroup, ts, NULL, true, r > 0 && pTsData[r - 1] >= nextStart);
            QUERY_CHECK_CODE(code, lino, _end);
          }
        }
        if ((TRINGBUF_HEAD(&pGroup->winBuf)->range.ekey == ts) && meetBound) {
          code = stRealtimeGroupCloseWindow(pGroup, NULL, true);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
    }
  } else {
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
      if (ts > pGroup->newThreshold) {
        goto _end;
      }
      code = stRealtimeGroupOpenWindow(pGroup, ts, NULL, false, false);
      QUERY_CHECK_CODE(code, lino, _end);
      pGroup->oldThreshold = ts - 1;
    }
    allTableProcessed = true;
  }

  if (allTableProcessed) {
    if (readAllData) {
      code = stRealtimeGroupMergeSavedWindows(pGroup, 0);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (IS_TRIGGER_GROUP_NONE_WINDOW(pGroup)) {
      goto _end;
    }

    while (true) {
      int64_t nextStart = pGroup->nextWindow.skey;
      int64_t curEnd = IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup) ? TRINGBUF_HEAD(&pGroup->winBuf)->range.ekey : INT64_MAX;
      int64_t ts = TMIN(nextStart, curEnd);
      if (taosArrayGetSize(pGroup->pPendingCalcParams) >= STREAM_CALC_REQ_MAX_WIN_NUM) {
        pContext->needCheckAgain = true;
        goto _end;
      }
      if (ts > pGroup->newThreshold) {
        break;
      }
      if (ts == nextStart) {
        code = stRealtimeGroupOpenWindow(pGroup, ts, NULL, false, false);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      if (ts == curEnd) {
        code = stRealtimeGroupCloseWindow(pGroup, NULL, false);
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

  if ((pTask->triggerFilter != NULL) || pTask->hasTriggerFilter) {
    readAllData = true;
  } else if (pTask->placeHolderBitmap & PLACE_HOLDER_WROWNUM) {
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

  if ((pTask->triggerFilter != NULL) || pTask->hasTriggerFilter) {
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
        if (nrowsCurWin + skipped == nrowsNextWstart) {
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
        code = streamBuildStateNotifyContent(STRIGGER_EVENT_WINDOW_OPEN, &pStateCol->info, NULL, newVal,
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
          code = streamBuildStateNotifyContent(STRIGGER_EVENT_WINDOW_CLOSE, &pStateCol->info, pStateData, newVal,
                                               &pExtraNotifyContent);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        code = stRealtimeGroupCloseWindow(pGroup, &pExtraNotifyContent, false);
        QUERY_CHECK_CODE(code, lino, _end);
        if (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_OPEN) {
          code = streamBuildStateNotifyContent(STRIGGER_EVENT_WINDOW_OPEN, &pStateCol->info, pStateData, newVal,
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

  pGroup->pPendingCalcParams = taosArrayInit(0, sizeof(SSTriggerCalcParam));
  QUERY_CHECK_NULL(pGroup->pPendingCalcParams, code, lino, _end, terrno);

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

  if (pGroup->pPendingCalcParams) {
    taosArrayDestroyEx(pGroup->pPendingCalcParams, tDestroySSTriggerCalcParam);
    pGroup->pPendingCalcParams = NULL;
  }

  taosMemFreeClear(*ppGroup);
}

static void stHistoryGroupClearTempState(SSTriggerHistoryGroup *pGroup) {
  SSTriggerHistoryContext *pContext = pGroup->pContext;
  pContext->reenterCheck = false;
  pContext->tbIter = 0;
  pContext->pCurVirTable = NULL;
  pContext->pCurTableMeta = NULL;
  pContext->pMetaToFetch = NULL;
  pContext->pColRefToFetch = NULL;
  pContext->pParamToFetch = NULL;
  pContext->trigDataBlockIdx = 0;
  pContext->calcDataBlockIdx = 0;
  taosArrayClearP(pContext->pCalcDataBlocks, (FDelete)blockDataDestroy);

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

static void stHistoryGroupClearMetadatas(SSTriggerHistoryGroup *pGroup, int64_t prevWindowEnd) {
  SSTriggerHistoryContext *pContext = pGroup->pContext;
  SStreamTriggerTask      *pTask = pContext->pTask;
  int32_t                  iter = 0;
  SSTriggerTableMeta      *pTableMeta = tSimpleHashIterate(pGroup->pTableMetas, NULL, &iter);
  while (pTableMeta != NULL) {
    if (taosArrayGetSize(pTableMeta->pMetas) > 0) {
      int64_t endTime = prevWindowEnd;
      if (pTask->placeHolderBitmap & PLACE_HOLDER_PARTITION_ROWS) {
        if (TARRAY_SIZE(pGroup->pPendingCalcParams) > 0) {
          SSTriggerCalcParam *pParam = TARRAY_DATA(pGroup->pPendingCalcParams);
          endTime = TMAX(endTime, pParam->wstart - 1);
        } else if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
          endTime = TMAX(endTime, TRINGBUF_HEAD(&pGroup->winBuf)->range.skey - 1);
        } else {
          endTime = TMAX(endTime, pContext->stepRange.ekey);
        }
      } else {
        endTime = TMAX(endTime, pContext->stepRange.ekey);
      }
      int32_t idx = taosArraySearchIdx(pTableMeta->pMetas, &endTime, stRealtimeGroupMetaDataSearch, TD_GT);
      taosArrayPopFrontBatch(pTableMeta->pMetas, (idx == -1) ? TARRAY_SIZE(pTableMeta->pMetas) : idx);
      idx = taosArraySearchIdx(pTableMeta->pMetas, &pContext->stepRange.ekey, stRealtimeGroupMetaDataSearch, TD_GT);
      pTableMeta->metaIdx = (idx == -1) ? TARRAY_SIZE(pTableMeta->pMetas) : idx;
    }
    pTableMeta = tSimpleHashIterate(pGroup->pTableMetas, pTableMeta, &iter);
  }
}

static int32_t stHistoryGroupAddMetaDatas(SSTriggerHistoryGroup *pGroup, SArray *pMetadatas, SArray *pVgIds,
                                          bool *pAdded) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SSTriggerHistoryContext *pContext = pGroup->pContext;
  SStreamTriggerTask      *pTask = pContext->pTask;
  SSTriggerTableMeta      *pTableMeta = NULL;

  QUERY_CHECK_NULL(pMetadatas, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(taosArrayGetSize(pMetadatas) == taosArrayGetSize(pVgIds), code, lino, _end,
                        TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(pTask->triggerType != STREAM_TRIGGER_PERIOD, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *pAdded = false;

  for (int32_t i = 0; i < TARRAY_SIZE(pMetadatas); i++) {
    SSDataBlock *pBlock = *(SSDataBlock **)TARRAY_GET_ELEM(pMetadatas, i);
    int32_t      vgId = *(int32_t *)TARRAY_GET_ELEM(pVgIds, i);
    int32_t      nrows = blockDataGetNumOfRows(pBlock);
    if (nrows == 0) {
      continue;
    }
    int32_t          iCol = 0;
    SColumnInfoData *pSkeyCol = taosArrayGet(pBlock->pDataBlock, iCol++);
    QUERY_CHECK_NULL(pSkeyCol, code, lino, _end, terrno);
    int64_t         *pSkeys = (int64_t *)pSkeyCol->pData;
    SColumnInfoData *pEkeyCol = taosArrayGet(pBlock->pDataBlock, iCol++);
    QUERY_CHECK_NULL(pEkeyCol, code, lino, _end, terrno);
    int64_t         *pEkeys = (int64_t *)pEkeyCol->pData;
    SColumnInfoData *pUidCol = taosArrayGet(pBlock->pDataBlock, iCol++);
    QUERY_CHECK_NULL(pUidCol, code, lino, _end, terrno);
    int64_t *pUids = (int64_t *)pUidCol->pData;
    int64_t *pGids = NULL;
    if (!pTask->isVirtualTable) {
      SColumnInfoData *pGidCol = taosArrayGet(pBlock->pDataBlock, iCol++);
      QUERY_CHECK_NULL(pGidCol, code, lino, _end, terrno);
      pGids = (int64_t *)pGidCol->pData;
    }
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

      *pAdded = true;

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
      if (TARRAY_SIZE(pTableMeta->pMetas) > 0) {
        SSTriggerMetaData *pLastMeta = taosArrayGetLast(pTableMeta->pMetas);
        QUERY_CHECK_CONDITION(pLastMeta->ekey < pSkeys[i], code, lino, _end, TSDB_CODE_INVALID_PARA);
      }
      SSTriggerMetaData *pNewMeta = taosArrayReserve(pTableMeta->pMetas, 1);
      QUERY_CHECK_NULL(pNewMeta, code, lino, _end, terrno);
      pNewMeta->skey = pSkeys[i];
      pNewMeta->ekey = pEkeys[i];
      pNewMeta->ver = 0;
      pNewMeta->nrows = pNrows[i];
    }
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
    if (!IS_TRIGGER_GROUP_NONE_WINDOW(pGroup)) {
      void *px = taosArrayPush(pInitWindows, &pGroup->nextWindow);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
    }
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
    if (nWindows > 0) {
      pGroup->nextWindow = *(STimeWindow *)taosArrayGetLast(pInitWindows);
      nWindows--;
    } else {
      TRINGBUF_DESTROY(&pGroup->winBuf);
      pGroup->nextWindow = (STimeWindow){0};
    }
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
      if (pTask->triggerType == STREAM_TRIGGER_SLIDING) {
        STimeWindow prevWindow = pWin->range;
        stTriggerTaskPrevIntervalWindow(pTask, &prevWindow);
        STimeWindow nextWindow = pWin->range;
        stTriggerTaskNextIntervalWindow(pTask, &nextWindow);
        param.prevTs = prevWindow.skey;
        param.currentTs = pWin->range.skey;
        param.nextTs = nextWindow.skey;
      }
      bool ignore = pTask->ignoreNoDataTrigger && (param.wrownum == 0);
      if (calcOpen && !ignore) {
        void *px = taosArrayPush(pGroup->pPendingCalcParams, &param);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      } else if (notifyOpen && !ignore) {
        void *px = taosArrayPush(pContext->pNotifyParams, &param);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      }
    }

    // some window may have not been closed yet
    if (pWin->range.ekey + gap > pContext->stepRange.ekey || pWin->range.ekey + gap > pContext->scanRange.ekey) {
      // TODO(kjq): restore prevProcTime from saved init windows
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
      if (pTask->triggerType == STREAM_TRIGGER_SLIDING) {
        if (pTask->interval.interval == 0) {
          param.prevTs = pWin->range.skey - 1;
          param.currentTs = pWin->range.ekey;
          STimeWindow nextWindow = pWin->range;
          stTriggerTaskNextPeriodWindow(pTask, &nextWindow);
          param.nextTs = nextWindow.ekey;
        } else {
          STimeWindow prevWindow = pWin->range;
          stTriggerTaskPrevIntervalWindow(pTask, &prevWindow);
          STimeWindow nextWindow = pWin->range;
          stTriggerTaskNextIntervalWindow(pTask, &nextWindow);
          param.prevTs = prevWindow.ekey + 1;
          param.currentTs = pWin->range.ekey + 1;
          param.nextTs = nextWindow.ekey + 1;
        }
      }
      bool ignore = pTask->ignoreNoDataTrigger && (param.wrownum == 0);
      if (notifyClose) {
        if ((pTask->triggerType == STREAM_TRIGGER_PERIOD) ||
            (pTask->triggerType == STREAM_TRIGGER_SLIDING && pTask->interval.interval == 0)) {
          param.notifyType = STRIGGER_EVENT_ON_TIME;
        } else {
          param.notifyType = STRIGGER_EVENT_WINDOW_CLOSE;
        }
      }
      if (calcClose && !ignore) {
        void *px = taosArrayPush(pGroup->pPendingCalcParams, &param);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      } else if (notifyClose && !ignore) {
        void *px = taosArrayPush(pContext->pNotifyParams, &param);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      }
    }
  }

  if (pTask->triggerType == STREAM_TRIGGER_SLIDING) {
    pWin = taosArrayGetLast(pContext->pSavedWindows);
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
    bool startFromHead = (pContext->calcDataBlockIdx == 0);
    *ppDataBlock = NULL;
    *pStartIdx = *pEndIdx = 0;
    while (pContext->calcDataBlockIdx < TARRAY_SIZE(pContext->pCalcDataBlocks)) {
      SSDataBlock *pBlock = *(SSDataBlock **)TARRAY_GET_ELEM(pContext->pCalcDataBlocks, pContext->calcDataBlockIdx);
      int32_t      nrows = blockDataGetNumOfRows(pBlock);
      pContext->calcDataBlockIdx++;
      if (nrows > 0) {
        *ppDataBlock = pBlock;
        *pEndIdx = nrows;
        break;
      }
    }
    if (TARRAY_SIZE(pContext->pCalcDataBlocks) == 0) {
      *pNeedFetchData = true;
    } else if (*ppDataBlock == NULL) {
      bool finished = true;
      for (int32_t i = 0; i < TARRAY_SIZE(pContext->pCalcDataBlocks); i++) {
        SSDataBlock *pDataBlock = *(SSDataBlock **)TARRAY_GET_ELEM(pContext->pCalcDataBlocks, i);
        if (blockDataGetNumOfRows(pDataBlock) >= STREAM_RETURN_ROWS_NUM) {
          finished = false;
          break;
        }
      }
      if (finished) {
        *pAllTableProcessed = true;
      } else {
        *pNeedFetchData = true;
      }
    }
    goto _end;
  } else if (!pContext->needTsdbMeta) {
    *ppDataBlock = NULL;
    *pStartIdx = *pEndIdx = 0;
    while (pContext->trigDataBlockIdx < TARRAY_SIZE(pContext->pTrigDataBlocks)) {
      SSDataBlock *pBlock = *(SSDataBlock **)TARRAY_GET_ELEM(pContext->pTrigDataBlocks, pContext->trigDataBlockIdx);
      int32_t      nrows = blockDataGetNumOfRows(pBlock);
      pContext->trigDataBlockIdx++;
      if (nrows > 0 && pBlock->info.id.groupId == pGroup->gid) {
        *ppDataBlock = pBlock;
        *pEndIdx = nrows;
        break;
      }
    }
    if (*ppDataBlock == NULL) {
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
        pContext->pCurTableMeta = tSimpleHashIterate(pGroup->pTableMetas, pContext->pCurTableMeta, &pContext->tbIter);
        if (pContext->pCurTableMeta == NULL) {
          *pAllTableProcessed = true;
          break;
        }
        if (saveWindow) {
          code = stHistoryGroupRestoreInitWindow(pGroup, pContext->pInitWindows);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        STimeWindow range = {.skey = INT64_MIN, .ekey = INT64_MAX - 1};
        if (pContext->status == STRIGGER_CONTEXT_CHECK_CONDITION) {
          range = pContext->stepRange;
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
          code = stHistoryGroupCloseWindow(pGroup, NULL, saveWindow);
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
          code = stHistoryGroupRestoreInitWindow(pGroup, pContext->pInitWindows);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        STimeWindow range = {.skey = INT64_MIN, .ekey = INT64_MAX - 1};
        if (pContext->status == STRIGGER_CONTEXT_CHECK_CONDITION) {
          range = pContext->stepRange;
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

static int32_t stHistoryGroupDoSlidingCheck(SSTriggerHistoryGroup *pGroup) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SSTriggerHistoryContext *pContext = pGroup->pContext;
  SStreamTriggerTask      *pTask = pContext->pTask;
  bool                     readAllData = false;
  bool                     allTableProcessed = false;
  bool                     needFetchData = false;

  if (!pContext->reenterCheck) {
    // save initial windows at the first check
    code = stHistoryGroupSaveInitWindow(pGroup, pContext->pInitWindows);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if ((pTask->triggerFilter != NULL) || pTask->hasTriggerFilter) {
    readAllData = true;
  } else if (pTask->placeHolderBitmap & PLACE_HOLDER_WROWNUM) {
    readAllData = true;
  } else if (pTask->ignoreNoDataTrigger) {
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
        bool meetBound = (r < endIdx) || (r > 0 && pTsData[r - 1] == ts);
        if (ts == nextStart && meetBound) {
          if (IS_TRIGGER_GROUP_NONE_WINDOW(pGroup)) {
            code = stHistoryGroupOpenWindow(pGroup, pTsData[r], NULL, true, true);
            QUERY_CHECK_CODE(code, lino, _end);
            r++;
          } else {
            code = stHistoryGroupOpenWindow(pGroup, ts, NULL, true, r > 0 && pTsData[r - 1] == nextStart);
            QUERY_CHECK_CODE(code, lino, _end);
          }
        }
        if ((TRINGBUF_HEAD(&pGroup->winBuf)->range.ekey == ts) && meetBound) {
          code = stHistoryGroupCloseWindow(pGroup, NULL, true);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
    }
  } else {
    if (IS_TRIGGER_GROUP_NONE_WINDOW(pGroup)) {
      void *px = tSimpleHashGet(pContext->pFirstTsMap, &pGroup->gid, sizeof(int64_t));
      QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      int64_t ts = *(int64_t *)px;
      code = stHistoryGroupOpenWindow(pGroup, ts, NULL, false, false);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    allTableProcessed = true;
  }

  if (allTableProcessed) {
    if (readAllData) {
      code = stHistoryGroupMergeSavedWindows(pGroup, 0);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (IS_TRIGGER_GROUP_NONE_WINDOW(pGroup)) {
      goto _end;
    }

    while (true) {
      int64_t nextStart = pGroup->nextWindow.skey;
      int64_t curEnd = IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup) ? TRINGBUF_HEAD(&pGroup->winBuf)->range.ekey : INT64_MAX;
      int64_t ts = TMIN(nextStart, curEnd);
      if (ts > pContext->stepRange.ekey || ts > pContext->scanRange.ekey) {
        break;
      }
      if (ts == nextStart) {
        code = stHistoryGroupOpenWindow(pGroup, ts, NULL, false, false);
        QUERY_CHECK_CODE(code, lino, _end);
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

  if (!pContext->reenterCheck) {
    // save initial windows at the first check
    code = stHistoryGroupSaveInitWindow(pGroup, pContext->pInitWindows);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if ((pTask->triggerFilter != NULL) || pTask->hasTriggerFilter) {
    readAllData = true;
  } else if (pTask->placeHolderBitmap & PLACE_HOLDER_WROWNUM) {
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
        pContext->pCurTableMeta = tSimpleHashIterate(pGroup->pTableMetas, pContext->pCurTableMeta, &pContext->tbIter);
        if (pContext->pCurTableMeta == NULL) {
          allTableProcessed = true;
          break;
        }
        code = stHistoryGroupRestoreInitWindow(pGroup, pContext->pInitWindows);
        QUERY_CHECK_CODE(code, lino, _end);
        STimeWindow range = pContext->stepRange;
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
          if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
            code = stHistoryGroupCloseWindow(pGroup, NULL, true);
            QUERY_CHECK_CODE(code, lino, _end);
          }
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

  if ((pTask->triggerFilter != NULL) || pTask->hasTriggerFilter) {
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
        STimeWindow range = pContext->stepRange;
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
        if (nrowsCurWin + skipped == nrowsNextWstart) {
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

static int32_t stHistoryGroupDoStateCheck(SSTriggerHistoryGroup *pGroup) {
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
      if (pTask->notifyHistory && (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_OPEN)) {
        code = streamBuildStateNotifyContent(STRIGGER_EVENT_WINDOW_OPEN, &pStateCol->info, NULL, newVal,
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
        if (pTask->notifyHistory && (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_CLOSE)) {
          code = streamBuildStateNotifyContent(STRIGGER_EVENT_WINDOW_CLOSE, &pStateCol->info, pStateData, newVal,
                                               &pExtraNotifyContent);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        code = stHistoryGroupCloseWindow(pGroup, &pExtraNotifyContent, false);
        QUERY_CHECK_CODE(code, lino, _end);
        if (pTask->notifyHistory && (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_OPEN)) {
          code = streamBuildStateNotifyContent(STRIGGER_EVENT_WINDOW_OPEN, &pStateCol->info, pStateData, newVal,
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
          if (pTask->notifyHistory && (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_OPEN)) {
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
          if (pTask->notifyHistory && (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_CLOSE)) {
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
      return stHistoryGroupDoStateCheck(pGroup);

    case STREAM_TRIGGER_EVENT:
      return stHistoryGroupDoEventCheck(pGroup);

    default: {
      ST_TASK_ELOG("invalid stream trigger type %d at %s:%d", pTask->triggerType, __func__, __LINE__);
      return TSDB_CODE_INVALID_PARA;
    }
  }
}
