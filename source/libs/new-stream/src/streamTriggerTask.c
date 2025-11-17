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
#include "scalar.h"
#include "streamInt.h"
#include "taos.h"
#include "taoserror.h"
#include "tarray.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "thash.h"
#include "tmsg.h"
#include "ttime.h"
#include "tutil.h"

#define STREAM_TRIGGER_CHECK_INTERVAL_MS  1000                    // 1s
#define STREAM_TRIGGER_IDLE_TIME_NS       1 * NANOSECOND_PER_SEC  // 1s, todo(kjq): increase the wait time to 10s
#define STREAM_TRIGGER_MAX_METAS          1 * 1024 * 1024         // 1M
#define STREAM_TRIGGER_MAX_PENDING_PARAMS 1 * 1024 * 1024         // 1M
#define STREAM_TRIGGER_REALTIME_SESSIONID 1
#define STREAM_TRIGGER_HISTORY_SESSIONID  2

#define STREAM_TRIGGER_RECALC_MERGE_MS (30 * MILLISECOND_PER_MINUTE)  // 30min

#define IS_TRIGGER_GROUP_TO_CHECK(pGroup) \
  (TD_DLIST_NODE_NEXT(pGroup) != NULL || TD_DLIST_TAIL(&pContext->groupsToCheck) == pGroup)
#define IS_TRIGGER_GROUP_NONE_WINDOW(pGroup) (TRINGBUF_CAPACITY(&(pGroup)->winBuf) == 0)
#define IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup) (TRINGBUF_SIZE(&(pGroup)->winBuf) > 0)
#define TRIGGER_GROUP_UNCLOSED_WINDOW_MASK   (1L << 62)
#define container_of(ptr, type, member)      ((type *)((char *)(ptr) - offsetof(type, member)))

static int32_t stRealtimeGroupInit(SSTriggerRealtimeGroup *pGroup, SSTriggerRealtimeContext *pContext, int64_t gid,
                                   int32_t vgId);
static void    stRealtimeGroupDestroy(void *ptr);
// Add metadatas to the group, which are used to check trigger conditions later.
static int32_t stRealtimeGroupAddMeta(SSTriggerRealtimeGroup *pGroup, int32_t vgId, SSTriggerMetaData *pMeta);
// Use metadatas to check trigger conditions, and generate notification and calculation requests.
static int32_t stRealtimeGroupCheck(SSTriggerRealtimeGroup *pGroup);
static int32_t stRealtimeGroupNextDataBlock(SSTriggerRealtimeGroup *pGroup, SSDataBlock **ppDataBlock,
                                            int32_t *pStartIdx, int32_t *pEndIdx);
// Clear all temporary states and variables in the group after checking.
static void stRealtimeGroupClearTempState(SSTriggerRealtimeGroup *pGroup);
// Clear metadatas that have been checked
static void stRealtimeGroupClearMetadatas(SSTriggerRealtimeGroup *pGroup);
// Retrieve pending calc params from the group
static int32_t stRealtimeGroupRetrievePendingCalc(SSTriggerRealtimeGroup *pGroup);
static int32_t stRealtimeGroupRemovePendingCalc(SSTriggerRealtimeGroup *pGroup, STimeWindow *pRange);

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

typedef struct SSTriggerOrigColumnInfo {
  int32_t    vgId;
  int64_t    suid;
  int64_t    uid;
  SSHashObj *pColumns;  // SSHashObj<col_name, col_id>
} SSTriggerOrigColumnInfo;

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

static STimeWindow stTriggerTaskGetTimeWindow(SStreamTriggerTask *pTask, int64_t ts) {
  SInterval  *pInterval = &pTask->interval;
  STimeWindow win = {0};
  if (pInterval->interval > 0) {
    win.skey = taosTimeTruncate(ts, pInterval);
    win.ekey = taosTimeGetIntervalEnd(win.skey, pInterval);
    if (win.ekey < win.skey) {
      win.ekey = INT64_MAX;
    }
  } else {
    int64_t day = convertTimePrecision(24 * 60 * 60 * 1000, TSDB_TIME_PRECISION_MILLI, pInterval->precision);
    // truncate to the start of day
    SInterval interval = {.intervalUnit = 'd',
                          .slidingUnit = 'd',
                          .offsetUnit = pInterval->offsetUnit,
                          .precision = pInterval->precision,
                          .interval = day,
                          .sliding = day};
    int64_t   first = taosTimeTruncate(ts, &interval) + pInterval->offset;
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
  }
  return win;
}

static void stTriggerTaskPrevTimeWindow(SStreamTriggerTask *pTask, STimeWindow *pWindow) {
  SInterval *pInterval = &pTask->interval;
  if (pInterval->interval > 0) {
    TSKEY prevStart =
        taosTimeAdd(pWindow->skey, -1 * pInterval->offset, pInterval->offsetUnit, pInterval->precision, NULL);
    prevStart = taosTimeAdd(prevStart, -1 * pInterval->sliding, pInterval->slidingUnit, pInterval->precision, NULL);
    prevStart = taosTimeAdd(prevStart, pInterval->offset, pInterval->offsetUnit, pInterval->precision, NULL);
    pWindow->skey = prevStart;
    pWindow->ekey = taosTimeGetIntervalEnd(prevStart, pInterval);
  } else {
    int64_t day = convertTimePrecision(24 * 60 * 60 * 1000, TSDB_TIME_PRECISION_MILLI, pInterval->precision);
    if (pInterval->sliding > day) {
      pWindow->skey -= pInterval->sliding;
      pWindow->ekey -= pInterval->sliding;
    } else {
      pWindow->ekey = pWindow->skey - 1;
      pWindow->skey -= pInterval->sliding;
      // truncate to the start of day
      SInterval interval = {.intervalUnit = 'd',
                            .slidingUnit = 'd',
                            .offsetUnit = pInterval->offsetUnit,
                            .precision = pInterval->precision,
                            .interval = day,
                            .sliding = day};
      int64_t   first = taosTimeTruncate(pWindow->skey, &interval) + pInterval->offset;
      if (first > pWindow->skey) {
        first -= day;
      }
      pWindow->skey = (pWindow->skey - first - 1) / pInterval->sliding * pInterval->sliding + first + 1;
    }
  }
}

static void stTriggerTaskNextTimeWindow(SStreamTriggerTask *pTask, STimeWindow *pWindow) {
  SInterval *pInterval = &pTask->interval;
  if (pInterval->interval > 0) {
    TSKEY nextStart =
        taosTimeAdd(pWindow->skey, -1 * pInterval->offset, pInterval->offsetUnit, pInterval->precision, NULL);
    nextStart = taosTimeAdd(nextStart, pInterval->sliding, pInterval->slidingUnit, pInterval->precision, NULL);
    nextStart = taosTimeAdd(nextStart, pInterval->offset, pInterval->offsetUnit, pInterval->precision, NULL);
    pWindow->skey = nextStart;
    pWindow->ekey = taosTimeGetIntervalEnd(nextStart, pInterval);
  } else {
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
                            .interval = day,
                            .sliding = day};
      int64_t   first = taosTimeTruncate(pWindow->ekey, &interval) + pInterval->offset;
      if (first > pWindow->skey && first < pWindow->ekey) {
        pWindow->ekey = first;
      }
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
      pProgress->lastScanVer = *(int64_t *)px;
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
                                          SArray *pColRefs) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t nCols = taosArrayGetSize(pSlots);

  if (pTask->nVirDataCols == 1) {
    // merge all original tables since it scans only timestamp column
    for (int32_t i = 0; i < pInfo->cols.nCols; i++) {
      SColRef *pColRef = &pInfo->cols.pColRef[i];
      if (!pColRef->hasRef) {
        continue;
      }

      size_t dbNameLen = strlen(pColRef->refDbName) + 1;
      size_t tbNameLen = strlen(pColRef->refTableName) + 1;
      size_t colNameLen = strlen(pColRef->refColName) + 1;
      void  *px = tSimpleHashGet(pTask->pOrigTableCols, pColRef->refDbName, dbNameLen);
      QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SSHashObj               *pDbInfo = *(SSHashObj **)px;
      SSTriggerOrigColumnInfo *pTbInfo = tSimpleHashGet(pDbInfo, pColRef->refTableName, tbNameLen);
      QUERY_CHECK_NULL(pTbInfo, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

      SSTriggerTableColRef *pRef = NULL;
      for (int32_t j = 0; j < TARRAY_SIZE(pColRefs); j++) {
        SSTriggerTableColRef *pTmpRef = TARRAY_GET_ELEM(pColRefs, j);
        if (pTmpRef->otbSuid == pTbInfo->suid && pTmpRef->otbUid == pTbInfo->uid) {
          pRef = pTmpRef;
          break;
        }
      }
      if (pRef == NULL) {
        pRef = taosArrayReserve(pColRefs, 1);
        QUERY_CHECK_NULL(pRef, code, lino, _end, terrno);
        pRef->otbSuid = pTbInfo->suid;
        pRef->otbUid = pTbInfo->uid;
        pRef->otbVgId = pTbInfo->vgId;
        pRef->pColMatches = taosArrayInit(0, sizeof(SSTriggerColMatch));
        QUERY_CHECK_NULL(pRef->pColMatches, code, lino, _end, terrno);
        pRef->pNewColMatches = taosArrayInit(0, sizeof(SSTriggerColMatch));
        QUERY_CHECK_NULL(pRef->pNewColMatches, code, lino, _end, terrno);
      }
    }
    goto _end;
  }

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

    size_t dbNameLen = strlen(pColRef->refDbName) + 1;
    size_t tbNameLen = strlen(pColRef->refTableName) + 1;
    size_t colNameLen = strlen(pColRef->refColName) + 1;
    void  *px = tSimpleHashGet(pTask->pOrigTableCols, pColRef->refDbName, dbNameLen);
    QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    SSHashObj               *pDbInfo = *(SSHashObj **)px;
    SSTriggerOrigColumnInfo *pTbInfo = tSimpleHashGet(pDbInfo, pColRef->refTableName, tbNameLen);
    QUERY_CHECK_NULL(pTbInfo, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    col_id_t *pOrigColId = tSimpleHashGet(pTbInfo->pColumns, pColRef->refColName, colNameLen);
    QUERY_CHECK_NULL(pOrigColId, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

    SSTriggerTableColRef *pRef = NULL;
    for (int32_t j = 0; j < TARRAY_SIZE(pColRefs); j++) {
      SSTriggerTableColRef *pTmpRef = TARRAY_GET_ELEM(pColRefs, j);
      if (pTmpRef->otbSuid == pTbInfo->suid && pTmpRef->otbUid == pTbInfo->uid) {
        pRef = pTmpRef;
        break;
      }
    }
    if (pRef == NULL) {
      pRef = taosArrayReserve(pColRefs, 1);
      QUERY_CHECK_NULL(pRef, code, lino, _end, terrno);
      pRef->otbSuid = pTbInfo->suid;
      pRef->otbUid = pTbInfo->uid;
      pRef->otbVgId = pTbInfo->vgId;
      pRef->pColMatches = taosArrayInit(0, sizeof(SSTriggerColMatch));
      QUERY_CHECK_NULL(pRef->pColMatches, code, lino, _end, terrno);
      pRef->pNewColMatches = taosArrayInit(0, sizeof(SSTriggerColMatch));
      QUERY_CHECK_NULL(pRef->pNewColMatches, code, lino, _end, terrno);
    }
    SSTriggerColMatch match = {.otbColId = *pOrigColId, .vtbSlotId = slotId};
    px = taosArrayPush(pRef->pColMatches, &match);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stTriggerTaskNewGenVirColRefs(SStreamTriggerTask *pTask, VTableInfo *pInfo, bool isTriggerData,
                                             SArray *pColRefs) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  SSDataBlock *pDataBlock = isTriggerData ? pTask->pVirtTrigBlock : pTask->pVirtCalcBlock;
  SArray      *pIsPseudoCol = isTriggerData ? pTask->pTrigIsPseudoCol : pTask->pCalcIsPseudoCol;
  col_id_t     tsColId = PRIMARYKEY_TIMESTAMP_COL_ID;
  col_id_t     tsSlotId = isTriggerData ? pTask->trigTsSlotId : pTask->calcTsSlotId;
  int32_t      ncols = blockDataGetNumOfCols(pDataBlock);
  if (ncols == 0) {
    goto _end;
  }

  if (pTask->virScanTsOnly) {
    // merge all original tables since it scans only timestamp column
    for (int32_t i = 0; i < pInfo->cols.nCols; i++) {
      SColRef *pColRef = &pInfo->cols.pColRef[i];
      if (!pColRef->hasRef) {
        continue;
      }

      size_t dbNameLen = strlen(pColRef->refDbName) + 1;
      size_t tbNameLen = strlen(pColRef->refTableName) + 1;
      size_t colNameLen = strlen(pColRef->refColName) + 1;
      void  *px = tSimpleHashGet(pTask->pOrigTableCols, pColRef->refDbName, dbNameLen);
      QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SSHashObj               *pDbInfo = *(SSHashObj **)px;
      SSTriggerOrigColumnInfo *pTbInfo = tSimpleHashGet(pDbInfo, pColRef->refTableName, tbNameLen);
      QUERY_CHECK_NULL(pTbInfo, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

      // set original table info
      SSTriggerOrigTableInfo *pOrigTableInfo = tSimpleHashGet(pTask->pOrigTableInfos, &pTbInfo->uid, sizeof(int64_t));
      if (pOrigTableInfo == NULL) {
        SSTriggerOrigTableInfo newInfo = {.tbSuid = pTbInfo->suid, .tbUid = pTbInfo->uid, .vgId = pTbInfo->vgId};
        code = tSimpleHashPut(pTask->pOrigTableInfos, &pTbInfo->uid, sizeof(int64_t), &newInfo,
                              sizeof(SSTriggerOrigTableInfo));
        QUERY_CHECK_CODE(code, lino, _end);
        pOrigTableInfo = tSimpleHashGet(pTask->pOrigTableInfos, &pTbInfo->uid, sizeof(int64_t));
        QUERY_CHECK_NULL(pOrigTableInfo, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        pOrigTableInfo->pTrigColMap = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT));
        QUERY_CHECK_NULL(pOrigTableInfo->pTrigColMap, code, lino, _end, terrno);
        pOrigTableInfo->pCalcColMap = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT));
        QUERY_CHECK_NULL(pOrigTableInfo->pCalcColMap, code, lino, _end, terrno);
        pOrigTableInfo->pVtbUids = taosArrayInit(0, sizeof(int64_t));
        QUERY_CHECK_NULL(pOrigTableInfo->pVtbUids, code, lino, _end, terrno);
      }
      SSHashObj *pColMap = isTriggerData ? pOrigTableInfo->pTrigColMap : pOrigTableInfo->pCalcColMap;

      // set virtual table info
      SSTriggerTableColRef *pRef = NULL;
      for (int32_t i = 0; i < TARRAY_SIZE(pColRefs); i++) {
        SSTriggerTableColRef *pTmpRef = TARRAY_GET_ELEM(pColRefs, i);
        if (pTmpRef->otbSuid == pTbInfo->suid && pTmpRef->otbUid == pTbInfo->uid) {
          pRef = pTmpRef;
          break;
        }
      }
      if (pRef == NULL) {
        pRef = taosArrayReserve(pColRefs, 1);
        QUERY_CHECK_NULL(pRef, code, lino, _end, terrno);
        pRef->otbSuid = pTbInfo->suid;
        pRef->otbUid = pTbInfo->uid;
        pRef->otbVgId = pTbInfo->vgId;
        pRef->pColMatches = taosArrayInit(0, sizeof(SSTriggerColMatch));
        QUERY_CHECK_NULL(pRef->pColMatches, code, lino, _end, terrno);
        pRef->pNewColMatches = taosArrayInit(0, sizeof(SSTriggerColMatch));
        QUERY_CHECK_NULL(pRef->pNewColMatches, code, lino, _end, terrno);
      }
      bool firstAdd = (TARRAY_SIZE(pRef->pNewColMatches) == 0);
      if (firstAdd) {
        code = tSimpleHashPut(pColMap, &tsColId, sizeof(col_id_t), &tsSlotId, sizeof(col_id_t));
        QUERY_CHECK_CODE(code, lino, _end);
        if (isTriggerData) {
          void *px = taosArrayPush(pOrigTableInfo->pVtbUids, &pInfo->uid);
          QUERY_CHECK_NULL(px, code, lino, _end, terrno);
        }
        SSTriggerColMatch match = {.otbSlotId = tsSlotId, .vtbSlotId = tsSlotId};
        void             *px = taosArrayPush(pRef->pNewColMatches, &match);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      }
    }
    goto _end;
  }

  for (int32_t i = 0; i < ncols; i++) {
    if (*(bool *)TARRAY_GET_ELEM(pIsPseudoCol, i)) {
      // ignore pseudo columns
      continue;
    }
    SColumnInfoData *pCol = TARRAY_GET_ELEM(pDataBlock->pDataBlock, i);
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

    size_t dbNameLen = strlen(pColRef->refDbName) + 1;
    size_t tbNameLen = strlen(pColRef->refTableName) + 1;
    size_t colNameLen = strlen(pColRef->refColName) + 1;
    void  *px = tSimpleHashGet(pTask->pOrigTableCols, pColRef->refDbName, dbNameLen);
    QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    SSHashObj               *pDbInfo = *(SSHashObj **)px;
    SSTriggerOrigColumnInfo *pTbInfo = tSimpleHashGet(pDbInfo, pColRef->refTableName, tbNameLen);
    QUERY_CHECK_NULL(pTbInfo, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    col_id_t *pOrigColId = tSimpleHashGet(pTbInfo->pColumns, pColRef->refColName, colNameLen);
    QUERY_CHECK_NULL(pOrigColId, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

    // set original table info
    SSTriggerOrigTableInfo *pOrigTableInfo = tSimpleHashGet(pTask->pOrigTableInfos, &pTbInfo->uid, sizeof(int64_t));
    if (pOrigTableInfo == NULL) {
      SSTriggerOrigTableInfo newInfo = {.tbSuid = pTbInfo->suid, .tbUid = pTbInfo->uid, .vgId = pTbInfo->vgId};
      code = tSimpleHashPut(pTask->pOrigTableInfos, &pTbInfo->uid, sizeof(int64_t), &newInfo,
                            sizeof(SSTriggerOrigTableInfo));
      QUERY_CHECK_CODE(code, lino, _end);
      pOrigTableInfo = tSimpleHashGet(pTask->pOrigTableInfos, &pTbInfo->uid, sizeof(int64_t));
      QUERY_CHECK_NULL(pOrigTableInfo, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pOrigTableInfo->pTrigColMap = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT));
      QUERY_CHECK_NULL(pOrigTableInfo->pTrigColMap, code, lino, _end, terrno);
      pOrigTableInfo->pCalcColMap = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT));
      QUERY_CHECK_NULL(pOrigTableInfo->pCalcColMap, code, lino, _end, terrno);
      pOrigTableInfo->pVtbUids = taosArrayInit(0, sizeof(int64_t));
      QUERY_CHECK_NULL(pOrigTableInfo->pVtbUids, code, lino, _end, terrno);
    }
    SSHashObj *pColMap = isTriggerData ? pOrigTableInfo->pTrigColMap : pOrigTableInfo->pCalcColMap;
    col_id_t  *pOrigSlotId = tSimpleHashGet(pColMap, pOrigColId, sizeof(col_id_t));
    if (pOrigSlotId == NULL) {
      code = tSimpleHashPut(pColMap, pOrigColId, sizeof(col_id_t), pOrigColId, sizeof(col_id_t));
      QUERY_CHECK_CODE(code, lino, _end);
      pOrigSlotId = tSimpleHashGet(pColMap, pOrigColId, sizeof(col_id_t));
      QUERY_CHECK_NULL(pOrigSlotId, code, lino, _end, terrno);
      *pOrigSlotId = i;
    }

    // set virtual table info
    SSTriggerTableColRef *pRef = NULL;
    for (int32_t i = 0; i < TARRAY_SIZE(pColRefs); i++) {
      SSTriggerTableColRef *pTmpRef = TARRAY_GET_ELEM(pColRefs, i);
      if (pTmpRef->otbSuid == pTbInfo->suid && pTmpRef->otbUid == pTbInfo->uid) {
        pRef = pTmpRef;
        break;
      }
    }
    if (pRef == NULL) {
      pRef = taosArrayReserve(pColRefs, 1);
      QUERY_CHECK_NULL(pRef, code, lino, _end, terrno);
      pRef->otbSuid = pTbInfo->suid;
      pRef->otbUid = pTbInfo->uid;
      pRef->otbVgId = pTbInfo->vgId;
      pRef->pColMatches = taosArrayInit(0, sizeof(SSTriggerColMatch));
      QUERY_CHECK_NULL(pRef->pColMatches, code, lino, _end, terrno);
      pRef->pNewColMatches = taosArrayInit(0, sizeof(SSTriggerColMatch));
      QUERY_CHECK_NULL(pRef->pNewColMatches, code, lino, _end, terrno);
    }
    bool firstAdd = (TARRAY_SIZE(pRef->pNewColMatches) == 0);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
    if (firstAdd) {
      code = tSimpleHashPut(pColMap, &tsColId, sizeof(col_id_t), &tsSlotId, sizeof(col_id_t));
      QUERY_CHECK_CODE(code, lino, _end);
      if (isTriggerData) {
        void *px = taosArrayPush(pOrigTableInfo->pVtbUids, &pInfo->uid);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      }
      SSTriggerColMatch match = {.otbSlotId = tsSlotId, .vtbSlotId = tsSlotId};
      void             *px = taosArrayPush(pRef->pNewColMatches, &match);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
    }
    SSTriggerColMatch match = {.otbSlotId = *pOrigSlotId, .vtbSlotId = i};
    px = taosArrayPush(pRef->pNewColMatches, &match);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
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
  int32_t            nTotalSlots = 0;
  SSTriggerCalcNode *pNode = NULL;
  bool              *pRunningFlag = NULL;
  bool               needUnlock = false;
  int64_t           *pRunningCnt = NULL;

  *ppRequest = NULL;

  taosWLockLatch(&pTask->calcPoolLock);
  needUnlock = true;

  pRunningCnt = tSimpleHashGet(pTask->pSessionRunning, &sessionId, sizeof(int64_t));
  if (pRunningCnt == NULL) {
    int64_t cnt = 0;
    code = tSimpleHashPut(pTask->pSessionRunning, &sessionId, sizeof(int64_t), &cnt, sizeof(int64_t));
    QUERY_CHECK_CODE(code, lino, _end);
    pRunningCnt = tSimpleHashGet(pTask->pSessionRunning, &sessionId, sizeof(int64_t));
    QUERY_CHECK_NULL(pRunningCnt, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  }

  // check if have any free slot
  nCalcNodes = taosArrayGetSize(pTask->pCalcNodes);
  for (int32_t i = 0; i < nCalcNodes; i++) {
    pNode = TARRAY_GET_ELEM(pTask->pCalcNodes, i);
    nIdleSlots += TD_DLIST_NELES(&pNode->idleSlots);
    nTotalSlots += TARRAY_SIZE(pNode->pSlots);
  }
  if (nIdleSlots == 0 || (*pRunningCnt >= nTotalSlots - 1)) {
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
  if (pRunningFlag[0] == true && (pTask->placeHolderBitmap & PLACE_HOLDER_PARTITION_ROWS)) {
    goto _end;
  }

  // select a free slot with payload balance
  pNode = TARRAY_GET_ELEM(pTask->pCalcNodes, 0);
  for (int32_t i = 1; i < nCalcNodes; i++) {
    SSTriggerCalcNode *pTmpNode = TARRAY_GET_ELEM(pTask->pCalcNodes, i);
    if (TD_DLIST_NELES(&pTmpNode->idleSlots) > TD_DLIST_NELES(&pNode->idleSlots)) {
      pNode = pTmpNode;
    }
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
  pReq->isWindowTrigger = !(pTask->triggerType == STREAM_TRIGGER_PERIOD ||
                            (pTask->triggerType == STREAM_TRIGGER_SLIDING && pTask->interval.interval == 0));
  pReq->precision = pTask->precision;
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
  *pRunningCnt += 1;

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

  int64_t *pRunningCnt = tSimpleHashGet(pTask->pSessionRunning, &pReq->sessionId, sizeof(int64_t));
  QUERY_CHECK_NULL(pRunningCnt, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(*pRunningCnt > 0, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  *pRunningCnt -= 1;

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

int32_t stTriggerTaskAcquireDropTableRequest(SStreamTriggerTask *pTask, int64_t sessionId, int64_t gid,
                                             SSTriggerDropRequest **ppRequest) {
  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               lino = 0;
  bool                 *pRunningFlag = NULL;
  bool                  needUnlock = false;
  SSTriggerDropRequest *pRequest = NULL;

  *ppRequest = NULL;

  taosWLockLatch(&pTask->calcPoolLock);
  needUnlock = true;

  // check if the group is running
  int64_t p[2] = {sessionId, gid};
  pRunningFlag = tSimpleHashGet(pTask->pGroupRunning, p, sizeof(p));
  if (pRunningFlag && pRunningFlag[0] == true) {
    // if group is running, drop later
    goto _end;
  }

  pRequest = taosMemoryCalloc(1, sizeof(SSTriggerDropRequest));
  QUERY_CHECK_NULL(pRequest, code, lino, _end, terrno);

  // random select a calc node
  SSTriggerCalcNode   *pNode = TARRAY_GET_ELEM(pTask->pCalcNodes, taosRand() % taosArrayGetSize(pTask->pCalcNodes));
  int32_t              idx = TARRAY_ELEM_IDX(pTask->pCalcNodes, pNode);
  SStreamRunnerTarget *pRunner = taosArrayGet(pTask->runnerList, idx);
  QUERY_CHECK_NULL(pRunner, code, lino, _end, terrno);
  pRequest->streamId = pTask->task.streamId;
  pRequest->runnerTaskId = pRunner->addr.taskId;
  pRequest->sessionId = sessionId;
  pRequest->triggerTaskId = pTask->task.taskId;
  pRequest->gid = gid;

  if (pRequest->groupColVals == NULL) {
    pRequest->groupColVals = taosArrayInit(0, sizeof(SStreamGroupValue));
    QUERY_CHECK_NULL(pRequest->groupColVals, code, lino, _end, terrno);
  } else {
    taosArrayClearEx(pRequest->groupColVals, tDestroySStreamGroupValue);
  }
  *ppRequest = pRequest;

_end:
  if (needUnlock) {
    taosWUnLockLatch(&pTask->calcPoolLock);
  }
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    if (pRequest != NULL) {
      taosMemoryFree(pRequest);
    }
  }
  return code;
}

int32_t stTriggerTaskReleaseDropTableRequest(SStreamTriggerTask *pTask, SSTriggerDropRequest **ppRequest) {
  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               lino = 0;
  SSTriggerDropRequest *pReq = NULL;
  bool                 *pRunningFlag = NULL;
  bool                  hasSent = false;

  pReq = *ppRequest;
  *ppRequest = NULL;
  taosArrayDestroyEx(pReq->groupColVals, tDestroySStreamGroupValue);

  taosMemoryFree(pReq);

  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t stTriggerTaskAddRecalcRequest(SStreamTriggerTask *pTask, SSTriggerRealtimeGroup *pGroup,
                                      STimeWindow *pCalcRange, SSHashObj *pWalProgress, bool isHistory,
                                      bool shrinkRange) {
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
    if (pTask->triggerType == STREAM_TRIGGER_SLIDING) {
      if (pCalcRange->skey != INT64_MIN) {
        STimeWindow win = stTriggerTaskGetTimeWindow(pTask, pCalcRange->skey);
        pReq->scanRange.skey = TMAX(pReq->scanRange.skey, win.skey);
      }
      if (pCalcRange->ekey != INT64_MAX) {
        STimeWindow win = stTriggerTaskGetTimeWindow(pTask, pCalcRange->ekey);
        pReq->scanRange.ekey = TMIN(pReq->scanRange.ekey, win.ekey);
      }
    } else if (shrinkRange && pTask->fillHistoryStartTime > 0 && pCalcRange->skey != INT64_MIN) {
      pReq->scanRange.skey = TMAX(pReq->scanRange.skey, pCalcRange->skey - pTask->historyStep);
    }
    pReq->calcRange = *pCalcRange;
    pReq->isHistory = false;
  }

  if (pReq->scanRange.skey > pReq->scanRange.ekey) {
    goto _end;
  }

  ST_TASK_DLOG("add recalc request, isHistory: %d, gid: %" PRId64 ", scanRange: [%" PRId64 ", %" PRId64
               "], calcRange: [%" PRId64 ", %" PRId64 "]",
               isHistory, pReq->gid, pReq->scanRange.skey, pReq->scanRange.ekey, pReq->calcRange.skey,
               pReq->calcRange.ekey);

  pReq->pTsdbVersions = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
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

  if (!pReq->isHistory) {
    // try to merge with existing requests if calc range diff is no more than 1 hour
    SListNode *pNode = TD_DLIST_HEAD(pTask->pRecalcRequests);
    while (pNode != NULL) {
      SSTriggerRecalcRequest *pTmpReq = *(SSTriggerRecalcRequest **)pNode->data;
      if (!pTmpReq->isHistory && pTmpReq->gid == pReq->gid &&
          (pTmpReq->calcRange.ekey + STREAM_TRIGGER_RECALC_MERGE_MS) >= pReq->calcRange.skey &&
          (pReq->calcRange.ekey + STREAM_TRIGGER_RECALC_MERGE_MS >= pTmpReq->calcRange.skey)) {
        STimeWindow newScanRange = {
            .skey = TMIN(pTmpReq->scanRange.skey, pReq->scanRange.skey),
            .ekey = TMAX(pTmpReq->scanRange.ekey, pReq->scanRange.ekey),
        };
        STimeWindow newCalcRange = {
            .skey = TMIN(pTmpReq->calcRange.skey, pReq->calcRange.skey),
            .ekey = TMAX(pTmpReq->calcRange.ekey, pReq->calcRange.ekey),
        };
        ST_TASK_DLOG("merge recalc request, gid: %" PRId64 ", calcRange1: [%" PRId64 ", %" PRId64
                     "], calcRange2: [%" PRId64 ", %" PRId64 "] to scanRange: [%" PRId64 ", %" PRId64
                     "], calcRange: [%" PRId64 ", %" PRId64 "]",
                     pReq->gid, pTmpReq->calcRange.skey, pTmpReq->calcRange.ekey, pReq->calcRange.skey,
                     pReq->calcRange.ekey, newScanRange.skey, newScanRange.ekey, newCalcRange.skey, newCalcRange.ekey);
        pTmpReq->scanRange = newScanRange;
        pTmpReq->calcRange = newCalcRange;
        TSWAP(pTmpReq->pTsdbVersions, pReq->pTsdbVersions);
        break;
      }
      pNode = TD_DLIST_NODE_NEXT(pNode);
    }
    if (pNode != NULL) {
      // merged
      goto _end;
    }
  }

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
    taosRUnLockLatch(&pTask->recalcRequestLock);
  }
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void stTriggerTaskDestroyOrigColumnInfo(void *ptr) {
  SSTriggerOrigColumnInfo *pInfo = ptr;
  if (pInfo == NULL) {
    return;
  }
  if (pInfo->pColumns != NULL) {
    tSimpleHashCleanup(pInfo->pColumns);
  }
}

static void stTriggerTaskDestroyHashElem(void *ptr) {
  SSHashObj **ppHash = ptr;
  if (ppHash == NULL || *ppHash == NULL) {
    return;
  }
  tSimpleHashCleanup(*ppHash);
  *ppHash = NULL;
}

static void stTriggerTaskDestroyOrigTableInfo(void *ptr) {
  SSTriggerOrigTableInfo *pTableInfo = ptr;
  if (pTableInfo == NULL) {
    return;
  }

  if (pTableInfo->pTrigColMap != NULL) {
    tSimpleHashCleanup(pTableInfo->pTrigColMap);
    pTableInfo->pTrigColMap = NULL;
  }
  if (pTableInfo->pCalcColMap != NULL) {
    tSimpleHashCleanup(pTableInfo->pCalcColMap);
    pTableInfo->pCalcColMap = NULL;
  }
  if (pTableInfo->pVtbUids != NULL) {
    taosArrayDestroy(pTableInfo->pVtbUids);
    pTableInfo->pVtbUids = NULL;
  }
}

static void stTriggerTaskDestroyTableInfo(void *ptr) {
  SSTriggerVirtTableInfo *pTableInfo = ptr;
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
      if (pColRef->pNewColMatches != NULL) {
        taosArrayDestroy(pColRef->pNewColMatches);
        pColRef->pNewColMatches = NULL;
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
      if (pColRef->pNewColMatches != NULL) {
        taosArrayDestroy(pColRef->pNewColMatches);
        pColRef->pNewColMatches = NULL;
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
        j++;
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
    taosSort((char *)TARRAY_DATA(pVirColIds) + nDataCols * sizeof(col_id_t), nPseudoCols, sizeof(col_id_t),
             compareUint16Val);
    col_id_t *pColIds = pVirColIds->pData;
    int32_t   j = nDataCols;
    for (int32_t i = nDataCols + 1; i < TARRAY_SIZE(pVirColIds); i++) {
      if (pColIds[i] != pColIds[j]) {
        j++;
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
  nodesWalkExpr(pTask->histTriggerFilter, nodeRewriteSlotid, &cxt);
  code = cxt.errCode;
  QUERY_CHECK_CODE(code, lino, _end);

  pTask->histTrigTsIndex = 0;
  pTask->histCalcTsIndex = 0;

  if (pTask->triggerType == STREAM_TRIGGER_STATE) {
    if (pTask->histStateSlotId != -1) {
      void *px = taosArrayGet(pTrigSlotids, pTask->histStateSlotId);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      pTask->histStateSlotId = *(int32_t *)px;
    }
    if (pTask->histStateExpr != NULL) {
      nodesWalkExpr(pTask->histStateExpr, nodeRewriteSlotid, &cxt);
      code = cxt.errCode;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  } else if (pTask->triggerType == STREAM_TRIGGER_EVENT) {
    nodesWalkExpr(pTask->histStartCond, nodeRewriteSlotid, &cxt);
    code = cxt.errCode;
    QUERY_CHECK_CODE(code, lino, _end);
    nodesWalkExpr(pTask->histEndCond, nodeRewriteSlotid, &cxt);
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

static int32_t stTriggerTaskNewParseVirtScan(SStreamTriggerTask *pTask, void *triggerScanPlan,
                                             void *calcCacheScanPlan) {
  int32_t    code = TSDB_CODE_SUCCESS;
  int32_t    lino = 0;
  SArray    *pTrigColids = NULL;
  SArray    *pCalcColids = NULL;
  SNodeList *pTrigSlots = NULL;
  SNodeList *pCalcSlots = NULL;
  int32_t    i = 0;
  SNode     *pNode = NULL;

  code = stTriggerTaskCollectVirCols(pTask, triggerScanPlan, &pTrigColids, &pTask->pTrigIsPseudoCol, &pTrigSlots);
  QUERY_CHECK_CODE(code, lino, _end);
  code = stTriggerTaskCollectVirCols(pTask, calcCacheScanPlan, &pCalcColids, &pTask->pCalcIsPseudoCol, &pCalcSlots);
  QUERY_CHECK_CODE(code, lino, _end);

  pTask->virScanTsOnly = (pTrigSlots != NULL) || (pCalcSlots != NULL);
  code = createDataBlock(&pTask->pVirtTrigBlock);
  QUERY_CHECK_CODE(code, lino, _end);
  i = 0;
  pNode = NULL;
  FOREACH(pNode, pTrigSlots) {
    SSlotDescNode *pn = (SSlotDescNode *)pNode;
    col_id_t       id = *(col_id_t *)TARRAY_GET_ELEM(pTrigColids, i);
    if (id == PRIMARYKEY_TIMESTAMP_COL_ID) {
      pTask->trigTsSlotId = i;
    } else if (!*(bool *)TARRAY_GET_ELEM(pTask->pTrigIsPseudoCol, i)) {
      pTask->virScanTsOnly = false;
    }
    i++;
    SColumnInfoData col = createColumnInfoData(pn->dataType.type, pn->dataType.bytes, id);
    col.info.scale = pn->dataType.scale;
    col.info.precision = pn->dataType.precision;
    col.info.noData = pn->reserve;
    code = blockDataAppendColInfo(pTask->pVirtTrigBlock, &col);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  code = createDataBlock(&pTask->pVirtCalcBlock);
  QUERY_CHECK_CODE(code, lino, _end);
  i = 0;
  pNode = NULL;
  FOREACH(pNode, pCalcSlots) {
    SSlotDescNode *pn = (SSlotDescNode *)pNode;
    col_id_t       id = *(col_id_t *)TARRAY_GET_ELEM(pCalcColids, i);
    if (id == PRIMARYKEY_TIMESTAMP_COL_ID) {
      pTask->calcTsSlotId = i;
    } else if (!*(bool *)TARRAY_GET_ELEM(pTask->pCalcIsPseudoCol, i)) {
      pTask->virScanTsOnly = false;
    }
    i++;
    SColumnInfoData col = createColumnInfoData(pn->dataType.type, pn->dataType.bytes, id);
    col.info.scale = pn->dataType.scale;
    col.info.precision = pn->dataType.precision;
    col.info.noData = pn->reserve;
    code = blockDataAppendColInfo(pTask->pVirtCalcBlock, &col);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (pTask->pTrigIsPseudoCol == NULL) {
    pTask->pTrigIsPseudoCol = taosMemoryCalloc(1, sizeof(SArray));
  }
  if (pTask->pCalcIsPseudoCol == NULL) {
    pTask->pCalcIsPseudoCol = taosMemoryCalloc(1, sizeof(SArray));
  }
  pTask->pVirtTableInfos = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  QUERY_CHECK_NULL(pTask->pVirtTableInfos, code, lino, _end, terrno);
  tSimpleHashSetFreeFp(pTask->pVirtTableInfos, stTriggerTaskDestroyTableInfo);
  pTask->pOrigTableInfos = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  QUERY_CHECK_NULL(pTask->pOrigTableInfos, code, lino, _end, terrno);
  tSimpleHashSetFreeFp(pTask->pOrigTableInfos, stTriggerTaskDestroyOrigTableInfo);

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
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t stTriggerTaskDeploy(SStreamTriggerTask *pTask, SStreamTriggerDeployMsg *pMsg) {
  int32_t    code = TSDB_CODE_SUCCESS;
  int32_t    lino = 0;
  SNodeList *pPartitionCols = NULL;

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
      pTask->stateExtend = pState->extend;
      code = nodesStringToNode(pState->zeroth, &pTask->pStateZeroth);
      QUERY_CHECK_CODE(code, lino, _end);
      pTask->stateTrueFor = pState->trueForDuration;
      code = nodesStringToNode(pState->expr, &pTask->pStateExpr);
      QUERY_CHECK_CODE(code, lino, _end);
      if (pTask->pStateExpr != NULL && nodeType(pTask->pStateExpr) != QUERY_NODE_COLUMN) {
        pTask->stateSlotId = -1;
      }
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

  pTask->trigTsIndex = pMsg->triTsSlotId;
  pTask->calcTsIndex = pMsg->calcTsSlotId;
  pTask->maxDelayNs = pMsg->maxDelay * NANOSECOND_PER_MSEC;
  pTask->fillHistoryStartTime = pMsg->fillHistoryStartTime;
  pTask->watermark = pMsg->watermark;
  pTask->expiredTime = pMsg->expiredTime;
  pTask->ignoreDisorder = pMsg->igDisorder;
  if (pTask->triggerType == STREAM_TRIGGER_COUNT) {
    pTask->ignoreDisorder = true;  // count window trigger has no recalculation
  }
  pTask->fillHistory = pMsg->fillHistory;
  if (pMsg->fillHistoryFirst) {
    if (pTask->triggerType == STREAM_TRIGGER_COUNT) {
      pTask->fillHistory = true;
    } else {
      pTask->fillHistoryFirst = true;
    }
  }
  pTask->lowLatencyCalc = pMsg->lowLatencyCalc;
  if (pTask->triggerType == STREAM_TRIGGER_PERIOD) {
    // always enable low latency calc for period trigger
    pTask->lowLatencyCalc = true;
  }
  pTask->hasPartitionBy = (pMsg->partitionCols != NULL);
  pTask->isVirtualTable = pMsg->isTriggerTblVirt;
  pTask->isStbPartitionByTag = pMsg->isTriggerTblStb;
  if (pMsg->isTriggerTblStb && pMsg->partitionCols != NULL) {
    code = nodesStringToList(pMsg->partitionCols, &pPartitionCols);
    QUERY_CHECK_CODE(code, lino, _end);
    SNode *pNode = NULL;
    FOREACH(pNode, pPartitionCols) {
      if ((pNode->type == QUERY_NODE_FUNCTION) &&
          (strcmp(((struct SFunctionNode *)pNode)->functionName, "tbname") == 0)) {
        pTask->isStbPartitionByTag = false;
        break;
      }
    }
  }
  pTask->ignoreNoDataTrigger = pMsg->igNoDataTrigger;
  pTask->hasTriggerFilter = pMsg->triggerHasPF;
  if (pTask->ignoreNoDataTrigger) {
    QUERY_CHECK_CONDITION(
        (pTask->triggerType == STREAM_TRIGGER_PERIOD) || (pTask->triggerType == STREAM_TRIGGER_SLIDING), code, lino,
        _end, TSDB_CODE_INVALID_PARA);
  }
  pTask->precision = pMsg->precision;
  pTask->historyStep = convertTimePrecision(10 * MILLISECOND_PER_DAY, TSDB_TIME_PRECISION_MILLI, pTask->precision);
  pTask->placeHolderBitmap = pMsg->placeHolderBitmap;
  pTask->streamName = taosStrdup(pMsg->streamName);
  code = nodesStringToNode(pMsg->triggerPrevFilter, &pTask->triggerFilter);
  QUERY_CHECK_CODE(code, lino, _end);

  if (pTask->triggerType == STREAM_TRIGGER_SESSION || pTask->triggerType == STREAM_TRIGGER_SLIDING ||
      pTask->triggerType == STREAM_TRIGGER_COUNT) {
    pTask->histTrigTsIndex = 0;
  } else {
    pTask->histTrigTsIndex = pTask->trigTsIndex;
  }
  pTask->histCalcTsIndex = pTask->calcTsIndex;
  if (pTask->triggerFilter != NULL) {
    code = nodesCloneNode(pTask->triggerFilter, &pTask->histTriggerFilter);
    QUERY_CHECK_CODE(code, lino, _end);
  }
  if (pTask->triggerType == STREAM_TRIGGER_STATE) {
    pTask->histStateSlotId = pTask->stateSlotId;
    code = nodesCloneNode(pTask->pStateExpr, &pTask->histStateExpr);
    QUERY_CHECK_CODE(code, lino, _end);
  } else if (pTask->triggerType == STREAM_TRIGGER_EVENT) {
    code = nodesCloneNode(pTask->pStartCond, &pTask->histStartCond);
    QUERY_CHECK_CODE(code, lino, _end);
    code = nodesCloneNode(pTask->pEndCond, &pTask->histEndCond);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (pTask->isVirtualTable) {
    code = stTriggerTaskParseVirtScan(pTask, pMsg->triggerScanPlan, pMsg->calcCacheScanPlan);
    QUERY_CHECK_CODE(code, lino, _end);
    code = stTriggerTaskNewParseVirtScan(pTask, pMsg->triggerScanPlan, pMsg->calcCacheScanPlan);
    QUERY_CHECK_CODE(code, lino, _end);
    pTask->pVirTableInfoRsp = taosArrayInit(0, sizeof(VTableInfo));
    QUERY_CHECK_NULL(pTask->pVirTableInfoRsp, code, lino, _end, terrno);
    pTask->pOrigTableCols = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
    QUERY_CHECK_NULL(pTask->pOrigTableCols, code, lino, _end, terrno);
    tSimpleHashSetFreeFp(pTask->pOrigTableCols, stTriggerTaskDestroyHashElem);
  }

  pTask->calcEventType = taosArrayGetSize(pMsg->runnerList) > 0 ? pMsg->eventTypes : STRIGGER_EVENT_WINDOW_NONE;
  pTask->notifyEventType = pMsg->notifyEventTypes;
  TSWAP(pTask->pNotifyAddrUrls, pMsg->pNotifyAddrUrls);
  pTask->addOptions = pMsg->addOptions;
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
  pTask->pSessionRunning = tSimpleHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  QUERY_CHECK_NULL(pTask->pSessionRunning, code, lino, _end, terrno);

  taosInitRWLatch(&pTask->recalcRequestLock);
  pTask->pRecalcRequests = tdListNew(POINTER_BYTES);
  QUERY_CHECK_NULL(pTask->pRecalcRequests, code, lino, _end, terrno);

  pTask->pRealtimeStartVer = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  QUERY_CHECK_NULL(pTask->pRealtimeStartVer, code, lino, _end, terrno);
  pTask->pHistoryCutoffTime = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  QUERY_CHECK_NULL(pTask->pHistoryCutoffTime, code, lino, _end, terrno);

  pTask->task.status = STREAM_STATUS_INIT;

_end:
  if (pPartitionCols != NULL) {
    nodesDestroyList(pPartitionCols);
  }
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

  if (pTask->triggerType == STREAM_TRIGGER_STATE) {
    if (pTask->pStateZeroth != NULL) {
      nodesDestroyNode(pTask->pStateZeroth);
      pTask->pStateZeroth = NULL;
    }
    if (pTask->pStateExpr != NULL) {
      nodesDestroyNode(pTask->pStateExpr);
      pTask->pStateExpr = NULL;
    }
  } else if (pTask->triggerType == STREAM_TRIGGER_EVENT) {
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

  if (pTask->histTriggerFilter != NULL) {
    nodesDestroyNode(pTask->histTriggerFilter);
    pTask->histTriggerFilter = NULL;
  }
  if (pTask->histStateExpr != NULL) {
    nodesDestroyNode(pTask->histStateExpr);
    pTask->histStateExpr = NULL;
  }
  if (pTask->histStartCond != NULL) {
    nodesDestroyNode(pTask->histStartCond);
    pTask->histStartCond = NULL;
  }
  if (pTask->histEndCond != NULL) {
    nodesDestroyNode(pTask->histEndCond);
    pTask->histEndCond = NULL;
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
  if (pTask->pVirtTrigBlock != NULL) {
    blockDataDestroy(pTask->pVirtTrigBlock);
    pTask->pVirtTrigBlock = NULL;
  }
  if (pTask->pVirtCalcBlock != NULL) {
    blockDataDestroy(pTask->pVirtCalcBlock);
    pTask->pVirtCalcBlock = NULL;
  }
  if (pTask->pTrigIsPseudoCol != NULL) {
    taosArrayDestroy(pTask->pTrigIsPseudoCol);
    pTask->pTrigIsPseudoCol = NULL;
  }
  if (pTask->pCalcIsPseudoCol != NULL) {
    taosArrayDestroy(pTask->pCalcIsPseudoCol);
    pTask->pCalcIsPseudoCol = NULL;
  }
  if (pTask->pVirtTableInfos != NULL) {
    tSimpleHashCleanup(pTask->pVirtTableInfos);
    pTask->pVirtTableInfos = NULL;
  }
  if (pTask->pOrigTableInfos != NULL) {
    tSimpleHashCleanup(pTask->pOrigTableInfos);
    pTask->pOrigTableInfos = NULL;
  }
  if (pTask->pVirTableInfoRsp != NULL) {
    taosArrayDestroyEx(pTask->pVirTableInfoRsp, tDestroyVTableInfo);
  }
  if (pTask->pOrigTableCols != NULL) {
    tSimpleHashCleanup(pTask->pOrigTableCols);
    pTask->pOrigTableCols = NULL;
  }

  if (pTask->pCalcNodes != NULL) {
    taosArrayDestroyEx(pTask->pCalcNodes, stTriggerTaskDestroyCalcNode);
    pTask->pCalcNodes = NULL;
  }
  if (pTask->pGroupRunning != NULL) {
    tSimpleHashCleanup(pTask->pGroupRunning);
    pTask->pGroupRunning = NULL;
  }
  if (pTask->pSessionRunning != NULL) {
    tSimpleHashCleanup(pTask->pSessionRunning);
    pTask->pSessionRunning = NULL;
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
    tFreeSStreamMgmtReq(pMgmtReq);
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
        ST_TASK_DLOG("ignore redundant start message, current status: %d", pTask->task.status);
        break;
      }
      if (taosArrayGetSize(pTask->pVirTableInfoRsp) > 0 && taosArrayGetSize(pTask->readerList) == 0) {
        ST_TASK_DLOG("ignore start message since orig table readers are not ready, vir table count: %" PRIzu,
                     TARRAY_SIZE(pTask->pVirTableInfoRsp));
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
        ST_TASK_DLOG("ignore redundant original reader info, current status: %d, reader count: %" PRIzu,
                     pTask->task.status, TARRAY_SIZE(pTask->readerList));
        break;
      }
      SStreamMgmtRsp *pRsp = (SStreamMgmtRsp *)pMsg;
      int32_t        *pVgId = TARRAY_DATA(pRsp->cont.vgIds);
      int32_t         iter1 = 0;
      void           *px = tSimpleHashIterate(pTask->pOrigTableCols, NULL, &iter1);
      while (px != NULL) {
        SSHashObj               *pDbInfo = *(SSHashObj **)px;
        int32_t                  iter2 = 0;
        SSTriggerOrigColumnInfo *pTbInfo = tSimpleHashIterate(pDbInfo, NULL, &iter2);
        while (pTbInfo != NULL) {
          pTbInfo->vgId = *(pVgId++);
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
      px = taosArrayAddAll(pTask->readerList, pTask->virtReaderList);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      int32_t nPartReaders = TARRAY_SIZE(pTask->readerList);
      if (taosArrayGetSize(pRsp->cont.readerList) > 0) {
        px = taosArrayAddAll(pTask->readerList, pRsp->cont.readerList);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      }

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
        SSTriggerPullRequest *pPullReq = &pProgress->pullReq.base;
        pPullReq->streamId = pTask->task.streamId;
        pPullReq->sessionId = pContext->sessionId;
        pPullReq->triggerTaskId = pTask->task.taskId;
        if (pTask->isVirtualTable) {
          pProgress->reqCids = taosArrayInit(0, sizeof(col_id_t));
          QUERY_CHECK_NULL(pProgress->reqCids, code, lino, _end, terrno);
          pProgress->reqCols = taosArrayInit(0, sizeof(OTableInfo));
          QUERY_CHECK_NULL(pProgress->reqCols, code, lino, _end, terrno);
          pProgress->uidInfoTrigger = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
          QUERY_CHECK_NULL(pProgress->uidInfoTrigger, code, lino, _end, terrno);
          tSimpleHashSetFreeFp(pProgress->uidInfoTrigger, stTriggerTaskDestroyHashElem);
          pProgress->uidInfoCalc = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
          QUERY_CHECK_NULL(pProgress->uidInfoCalc, code, lino, _end, terrno);
          tSimpleHashSetFreeFp(pProgress->uidInfoCalc, stTriggerTaskDestroyHashElem);
        }
        pProgress->pVersions = taosArrayInit(0, sizeof(int64_t));
        QUERY_CHECK_NULL(pProgress->pVersions, code, lino, _end, terrno);
        pProgress->pTrigBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
        QUERY_CHECK_NULL(pProgress->pTrigBlock, code, lino, _end, terrno);
        pProgress->pCalcBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
        QUERY_CHECK_NULL(pProgress->pCalcBlock, code, lino, _end, terrno);
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
        ST_TASK_DLOG("add user recalc request, start: %" PRId64 ", end: %" PRId64, pReq->start, pReq->end - 1);
        while (px != NULL) {
          SSTriggerRealtimeGroup *pGroup = *(SSTriggerRealtimeGroup **)px;
          STimeWindow             range = {.skey = pReq->start, .ekey = pReq->end - 1};
          code = stTriggerTaskAddRecalcRequest(pTask, pGroup, &range, pContext->pReaderWalProgress, false, false);
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
      default: {
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
        if (code != TSDB_CODE_STREAM_TASK_NOT_EXIST) {
          *pErrTaskId = pReq->readerTaskId;
          code = pRsp->code;
          QUERY_CHECK_CODE(code, lino, _end);
        }
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
      case TSDB_CODE_STREAM_INSERT_TBINFO_NOT_FOUND:
      case TSDB_CODE_STREAM_VTABLE_NEED_REDEPLOY: {
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
  } else if (pRsp->msgType == TDMT_STREAM_TRIGGER_DROP_RSP) {
    // TODO kuang
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

int32_t stTriggerTaskGetDelay(SStreamTask *pStreamTask, int64_t *pDelay, bool *pFillHisFinished) {
  SStreamTriggerTask *pTask = (SStreamTriggerTask *)pStreamTask;
  int64_t             now = taosGetTimestampNs();
  *pDelay = now - atomic_load_64(&pTask->latestVersionTime);
  *pFillHisFinished = atomic_load_8(&pTask->historyFinished);
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
  if (pProgress->uidInfoTrigger != NULL) {
    tSimpleHashCleanup(pProgress->uidInfoTrigger);
    pProgress->uidInfoTrigger = NULL;
  }
  if (pProgress->uidInfoCalc != NULL) {
    tSimpleHashCleanup(pProgress->uidInfoCalc);
    pProgress->uidInfoCalc = NULL;
  }
  if (pProgress->pVersions != NULL) {
    taosArrayDestroy(pProgress->pVersions);
    pProgress->pVersions = NULL;
  }
  if (pProgress->pTrigBlock != NULL) {
    blockDataDestroy(pProgress->pTrigBlock);
    pProgress->pTrigBlock = NULL;
  }
  if (pProgress->pCalcBlock != NULL) {
    blockDataDestroy(pProgress->pCalcBlock);
    pProgress->pCalcBlock = NULL;
  }
}

static void stRealtimeContextDestroyWindow(void *ptr) {
  SSTriggerNotifyWindow *pWin = ptr;
  if (pWin == NULL) {
    return;
  }
  taosMemoryFreeClear(pWin->pWinOpenNotify);
  taosMemoryFreeClear(pWin->pWinCloseNotify);
}

static int32_t stRealtimeContextCalcExpr(SSTriggerRealtimeContext *pContext, SSDataBlock *pDataBlock, SNode *pExpr,
                                         SColumnInfoData *pResCol) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pContext->pTask;
  SArray             *pList = NULL;

  pList = taosArrayInit(1, POINTER_BYTES);
  QUERY_CHECK_NULL(pList, code, lino, _end, terrno);
  void *px = taosArrayPush(pList, &pDataBlock);
  QUERY_CHECK_NULL(px, code, lino, _end, terrno);

  SDataType *pType = &((SExprNode *)pExpr)->resType;
  pResCol->info.type = pType->type;
  pResCol->info.bytes = pType->bytes;
  pResCol->info.scale = pType->scale;
  pResCol->info.precision = pType->precision;

  int32_t      nrows = blockDataGetNumOfRows(pDataBlock);
  SScalarParam output = {.columnData = pResCol};
  code = scalarCalculate(pExpr, pList, &output, NULL, NULL);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (pList != NULL) {
    taosArrayDestroy(pList);
  }
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stRealtimeContextCompareGroup(const HeapNode *a, const HeapNode *b) {
  SSTriggerRealtimeGroup *pGroup1 = container_of(a, SSTriggerRealtimeGroup, heapNode);
  SSTriggerRealtimeGroup *pGroup2 = container_of(b, SSTriggerRealtimeGroup, heapNode);
  return pGroup1->nextExecTime < pGroup2->nextExecTime;
}

static int32_t stRealtimeContextInit(SSTriggerRealtimeContext *pContext, SStreamTriggerTask *pTask) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSDataBlock *pVirDataBlock = NULL;
  SFilterInfo *pVirDataFilter = NULL;

  pContext->pTask = pTask;
  pContext->sessionId = STREAM_TRIGGER_REALTIME_SESSIONID;

  bool needTrigData = true;
  if (pTask->triggerType == STREAM_TRIGGER_PERIOD) {
    needTrigData = false;
  } else if (pTask->triggerType == STREAM_TRIGGER_SLIDING) {
    needTrigData = (pTask->placeHolderBitmap & PLACE_HOLDER_WROWNUM) || pTask->ignoreNoDataTrigger;
  }
  if (!needTrigData) {
    pContext->walMode = STRIGGER_WAL_META_ONLY;
  } else if (pTask->watermark > 0 || pTask->isVirtualTable) {
    pContext->walMode = STRIGGER_WAL_META_THEN_DATA;
  } else {
    pContext->walMode = STRIGGER_WAL_META_WITH_DATA;
  }

  pContext->pReaderWalProgress = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
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
    SSTriggerPullRequest *pPullReq = &pProgress->pullReq.base;
    pPullReq->streamId = pTask->task.streamId;
    pPullReq->sessionId = pContext->sessionId;
    pPullReq->triggerTaskId = pTask->task.taskId;
    if (pTask->isVirtualTable) {
      pProgress->reqCids = taosArrayInit(0, sizeof(col_id_t));
      QUERY_CHECK_NULL(pProgress->reqCids, code, lino, _end, terrno);
      pProgress->reqCols = taosArrayInit(0, sizeof(OTableInfo));
      QUERY_CHECK_NULL(pProgress->reqCols, code, lino, _end, terrno);
      pProgress->uidInfoTrigger = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
      QUERY_CHECK_NULL(pProgress->uidInfoTrigger, code, lino, _end, terrno);
      tSimpleHashSetFreeFp(pProgress->uidInfoTrigger, stTriggerTaskDestroyHashElem);
      pProgress->uidInfoCalc = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
      QUERY_CHECK_NULL(pProgress->uidInfoCalc, code, lino, _end, terrno);
      tSimpleHashSetFreeFp(pProgress->uidInfoCalc, stTriggerTaskDestroyHashElem);
    }
    pProgress->pVersions = taosArrayInit(0, sizeof(int64_t));
    QUERY_CHECK_NULL(pProgress->pVersions, code, lino, _end, terrno);
    pProgress->pTrigBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
    QUERY_CHECK_NULL(pProgress->pTrigBlock, code, lino, _end, terrno);
    pProgress->pCalcBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
    QUERY_CHECK_NULL(pProgress->pCalcBlock, code, lino, _end, terrno);
  }

  pContext->pMetaBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  QUERY_CHECK_NULL(pContext->pMetaBlock, code, lino, _end, terrno);
  pContext->pDeleteBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  QUERY_CHECK_NULL(pContext->pDeleteBlock, code, lino, _end, terrno);
  pContext->pDropBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  QUERY_CHECK_NULL(pContext->pDropBlock, code, lino, _end, terrno);
  pContext->pTempSlices = taosArrayInit(0, sizeof(int64_t) * 3);
  QUERY_CHECK_NULL(pContext->pTempSlices, code, lino, _end, terrno);
  pContext->pRanges = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  QUERY_CHECK_NULL(pContext->pRanges, code, lino, _end, terrno);
  pContext->pGroups = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  QUERY_CHECK_NULL(pContext->pGroups, code, lino, _end, terrno);
  tSimpleHashSetFreeFp(pContext->pGroups, stRealtimeGroupDestroy);
  TD_DLIST_INIT(&pContext->groupsToCheck);
  pContext->pMaxDelayHeap = heapCreate(stRealtimeContextCompareGroup);
  QUERY_CHECK_NULL(pContext->pMaxDelayHeap, code, lino, _end, terrno);
  pContext->groupsToDelete = taosArrayInit(0, sizeof(int64_t));
  QUERY_CHECK_NULL(pContext->groupsToDelete, code, lino, _end, terrno);
  pContext->pGroupColVals = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  QUERY_CHECK_NULL(pContext->pGroupColVals, code, lino, _end, terrno);

  pContext->pSlices = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  QUERY_CHECK_NULL(pContext->pSlices, code, lino, _end, terrno);

  if (pTask->isVirtualTable) {
    pContext->pMerger = taosMemoryCalloc(1, sizeof(SSTriggerNewVtableMerger));
    QUERY_CHECK_NULL(pContext->pMerger, code, lino, _end, terrno);
    code = stNewVtableMergerInit(pContext->pMerger, pTask, pTask->pVirtTrigBlock, TARRAY_DATA(pTask->pTrigIsPseudoCol),
                                 pTask->triggerFilter);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    pContext->pSorter = taosMemoryCalloc(1, sizeof(SSTriggerNewTimestampSorter));
    QUERY_CHECK_NULL(pContext->pSorter, code, lino, _end, terrno);
    int32_t verColBias = 0;
    if (pTask->triggerType == STREAM_TRIGGER_EVENT) {
      verColBias = 2;
    } else if (pTask->triggerType == STREAM_TRIGGER_STATE && pTask->stateSlotId == -1) {
      verColBias = 1;
    }
    code = stNewTimestampSorterInit(pContext->pSorter, pTask, verColBias);
    QUERY_CHECK_CODE(code, lino, _end);
  }
  pContext->pWindows = taosArrayInit(0, sizeof(SSTriggerNotifyWindow));
  QUERY_CHECK_NULL(pContext->pWindows, code, lino, _end, terrno);
  if (pTask->notifyEventType != STRIGGER_EVENT_WINDOW_NONE) {
    pContext->pNotifyParams = taosArrayInit(0, sizeof(SSTriggerCalcParam));
    QUERY_CHECK_NULL(pContext->pNotifyParams, code, lino, _end, terrno);
  }
  code = taosObjListInit(&pContext->pAllCalcTableUids, &pContext->tableUidPool);
  QUERY_CHECK_CODE(code, lino, _end);
  code = taosObjListInit(&pContext->pCalcTableUids, &pContext->tableUidPool);
  QUERY_CHECK_CODE(code, lino, _end);
  if (pTask->placeHolderBitmap & PLACE_HOLDER_PARTITION_ROWS) {
    if (pTask->isVirtualTable) {
      pContext->pCalcMerger = taosMemoryCalloc(1, sizeof(SSTriggerNewVtableMerger));
      QUERY_CHECK_NULL(pContext->pCalcMerger, code, lino, _end, terrno);
      code = stNewVtableMergerInit(pContext->pCalcMerger, pTask, pTask->pVirtCalcBlock,
                                   TARRAY_DATA(pTask->pCalcIsPseudoCol), NULL);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      pContext->pCalcSorter = taosMemoryCalloc(1, sizeof(SSTriggerNewTimestampSorter));
      QUERY_CHECK_NULL(pContext->pCalcSorter, code, lino, _end, terrno);
      code = stNewTimestampSorterInit(pContext->pCalcSorter, pTask, 0);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  pContext->periodWindow = (STimeWindow){.skey = INT64_MIN, .ekey = INT64_MIN};
  code = taosObjPoolInit(&pContext->metaPool, 1024, sizeof(SSTriggerMetaData));
  QUERY_CHECK_CODE(code, lino, _end);
  code = taosObjPoolInit(&pContext->tableUidPool, 1024, sizeof(int64_t) * 2);
  QUERY_CHECK_CODE(code, lino, _end);
  code = taosObjPoolInit(&pContext->windowPool, 1024, sizeof(SSTriggerWindow));
  QUERY_CHECK_CODE(code, lino, _end);
  code = taosObjPoolInit(&pContext->calcParamPool, 1024, sizeof(SSTriggerCalcParam));
  QUERY_CHECK_CODE(code, lino, _end);

  if (pTask->placeHolderBitmap & PLACE_HOLDER_PARTITION_ROWS) {
    pContext->pCalcDataCacheIters =
        taosHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
    QUERY_CHECK_NULL(pContext->pCalcDataCacheIters, code, lino, _end, errno);
    taosHashSetFreeFp(pContext->pCalcDataCacheIters, (FDelete)releaseDataResult);
  }

  tdListInit(&pContext->retryPullReqs, POINTER_BYTES);
  tdListInit(&pContext->retryCalcReqs, POINTER_BYTES);
  tdListInit(&pContext->dropTableReqs, POINTER_BYTES);

  if (pTask->triggerType == STREAM_TRIGGER_PERIOD) {
    pContext->haveReadCheckpoint = true;
  }
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

  if (pContext->pMetaBlock != NULL) {
    blockDataDestroy(pContext->pMetaBlock);
    pContext->pMetaBlock = NULL;
  }
  if (pContext->pDeleteBlock != NULL) {
    blockDataDestroy(pContext->pDeleteBlock);
    pContext->pDeleteBlock = NULL;
  }
  if (pContext->pDropBlock != NULL) {
    blockDataDestroy(pContext->pDropBlock);
    pContext->pDropBlock = NULL;
  }
  if (pContext->pTempSlices != NULL) {
    taosArrayDestroy(pContext->pTempSlices);
    pContext->pTempSlices = NULL;
  }
  if (pContext->pRanges != NULL) {
    tSimpleHashCleanup(pContext->pRanges);
    pContext->pRanges = NULL;
  }

  if (pContext->pGroups != NULL) {
    tSimpleHashCleanup(pContext->pGroups);
    pContext->pGroups = NULL;
  }
  if (pContext->pMaxDelayHeap != NULL) {
    heapDestroy(pContext->pMaxDelayHeap);
    pContext->pMaxDelayHeap = NULL;
  }
  if (pContext->groupsToDelete != NULL) {
    taosArrayDestroy(pContext->groupsToDelete);
    pContext->groupsToDelete = NULL;
  }
  if (pContext->pGroupColVals != NULL) {
    int32_t iter = 0;
    void   *px = tSimpleHashIterate(pContext->pGroupColVals, NULL, &iter);
    while (px != NULL) {
      SArray *pColVals = *(SArray **)px;
      taosArrayDestroyEx(pColVals, tDestroySStreamGroupValue);
      px = tSimpleHashIterate(pContext->pGroupColVals, px, &iter);
    }
    tSimpleHashCleanup(pContext->pGroupColVals);
    pContext->pGroupColVals = NULL;
  }

  if (pContext->pSlices != NULL) {
    tSimpleHashCleanup(pContext->pSlices);
    pContext->pSlices = NULL;
  }

  if (pContext->pSorter != NULL) {
    stNewTimestampSorterDestroy(&pContext->pSorter);
  }
  if (pContext->pMerger != NULL) {
    stNewVtableMergerDestroy(&pContext->pMerger);
  }
  if (pContext->pWindows != NULL) {
    taosArrayDestroyEx(pContext->pWindows, stRealtimeContextDestroyWindow);
    pContext->pWindows = NULL;
  }
  if (pContext->pNotifyParams != NULL) {
    taosArrayDestroyEx(pContext->pNotifyParams, tDestroySSTriggerCalcParam);
    pContext->pNotifyParams = NULL;
  }
  taosObjListClear(&pContext->pAllCalcTableUids);
  taosObjListClear(&pContext->pCalcTableUids);
  if (pContext->pCalcSorter != NULL) {
    stNewTimestampSorterDestroy(&pContext->pCalcSorter);
  }
  if (pContext->pCalcMerger != NULL) {
    stNewVtableMergerDestroy(&pContext->pCalcMerger);
  }

  colDataDestroy(&pContext->stateCol);
  colDataDestroy(&pContext->eventStartCol);
  colDataDestroy(&pContext->eventEndCol);
  taosObjPoolDestroy(&pContext->metaPool);
  taosObjPoolDestroy(&pContext->tableUidPool);
  taosObjPoolDestroy(&pContext->windowPool);
  taosObjPoolDestroy(&pContext->calcParamPool);

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
  tdListEmpty(&pContext->dropTableReqs);

  taosMemFreeClear(*ppContext);
}

static SSTriggerRealtimeGroup *stRealtimeContextGetCurrentGroup(SSTriggerRealtimeContext *pContext) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pContext->pTask;
  if (pContext->status == STRIGGER_CONTEXT_SEND_DROP_REQ) {
    int64_t *pGid = taosArrayGet(pContext->groupsToDelete, pContext->dropReqIndex);
    QUERY_CHECK_NULL(pGid, code, lino, _end, terrno);
    void *px = tSimpleHashGet(pContext->pGroups, pGid, sizeof(int64_t));
    QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    return *(SSTriggerRealtimeGroup **)px;
  } else if (pContext->pMinGroup != NULL) {
    return pContext->pMinGroup;
  } else {
    return TD_DLIST_HEAD(&pContext->groupsToCheck);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return NULL;
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

    case STRIGGER_PULL_WAL_META_NEW: {
      SStreamTaskAddr *pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
      QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
      pProgress = tSimpleHashGet(pContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SSTriggerWalMetaNewRequest *pReq = &pProgress->pullReq.walMetaNewReq;
      pReq->lastVer = pProgress->lastScanVer;
      if (pTask->triggerType == STREAM_TRIGGER_PERIOD) {
        pReq->ctime = pContext->periodWindow.ekey;
      } else {
        pReq->ctime = INT64_MAX;
      }
      break;
    }

    case STRIGGER_PULL_WAL_DATA_NEW: {
      SStreamTaskAddr *pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
      QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
      pProgress = tSimpleHashGet(pContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SSTriggerWalDataNewRequest *pReq = &pProgress->pullReq.walDataNewReq;
      pReq->versions = pProgress->pVersions;
      pReq->ranges = pContext->pRanges;
      // fill versions according to groupsToCheck
      taosArrayClear(pReq->versions);
      SSTriggerRealtimeGroup *pGroup = TD_DLIST_HEAD(&pContext->groupsToCheck);
      while (pGroup != NULL) {
        if (pGroup->oldThreshold < pGroup->newThreshold) {
          SObjList *pMetas = tSimpleHashGet(pGroup->pWalMetas, &pProgress->pTaskAddr->nodeId, sizeof(int32_t));
          if (pMetas != NULL) {
            SSTriggerMetaData *pMeta = NULL;
            SObjListIter       iter = {0};
            taosObjListInitIter(pMetas, &iter, TOBJLIST_ITER_FORWARD);
            while ((pMeta = taosObjListIterNext(&iter)) != NULL) {
              if (pMeta->skey > pGroup->newThreshold || pMeta->ekey <= pGroup->oldThreshold) {
                continue;
              }
              void *px = taosArrayPush(pReq->versions, &pMeta->ver);
              QUERY_CHECK_NULL(px, code, lino, _end, terrno);
            }
          }
        }
        pGroup = TD_DLIST_NODE_NEXT(pGroup);
      }
      taosArraySort(pReq->versions, compareInt64Val);
      int64_t *pv = TARRAY_DATA(pReq->versions);
      for (int32_t i = 1; i < TARRAY_SIZE(pReq->versions); i++) {
        int64_t *pi = TARRAY_GET_ELEM(pReq->versions, i);
        if (*pv != *pi) {
          pv++;
          if (pv != pi) {
            *pv = *pi;
          }
        }
      }
      if (TARRAY_SIZE(pReq->versions) > 0) {
        TARRAY_SIZE(pReq->versions) = TARRAY_ELEM_IDX(pReq->versions, pv) + 1;
      }
      break;
    }

    case STRIGGER_PULL_WAL_META_DATA_NEW: {
      SStreamTaskAddr *pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
      QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
      pProgress = tSimpleHashGet(pContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SSTriggerWalMetaDataNewRequest *pReq = &pProgress->pullReq.walMetaDataNewReq;
      pReq->lastVer = pProgress->lastScanVer;
      break;
    }

    case STRIGGER_PULL_WAL_CALC_DATA_NEW: {
      SStreamTaskAddr *pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
      QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
      pProgress = tSimpleHashGet(pContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SSTriggerWalDataNewRequest *pReq = &pProgress->pullReq.walDataNewReq;
      pReq->versions = pProgress->pVersions;
      pReq->ranges = pContext->pRanges;
      taosArrayClear(pReq->versions);
      SSTriggerRealtimeGroup *pGroup = stRealtimeContextGetCurrentGroup(pContext);
      QUERY_CHECK_NULL(pGroup, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SObjList *pMetas = tSimpleHashGet(pGroup->pWalMetas, &pProgress->pTaskAddr->nodeId, sizeof(int32_t));
      if (pMetas != NULL) {
        SSTriggerMetaData *pMeta = NULL;
        SObjListIter       iter = {0};
        taosObjListInitIter(pMetas, &iter, TOBJLIST_ITER_FORWARD);
        while ((pMeta = taosObjListIterNext(&iter)) != NULL) {
          if (pMeta->skey > pContext->calcRange.ekey || pMeta->ekey < pContext->calcRange.skey) {
            continue;
          }
          void *px = taosArrayPush(pReq->versions, &pMeta->ver);
          QUERY_CHECK_NULL(px, code, lino, _end, terrno);
        }
      }
      break;
    }

    case STRIGGER_PULL_GROUP_COL_VALUE: {
      SSTriggerRealtimeGroup *pGroup = stRealtimeContextGetCurrentGroup(pContext);
      QUERY_CHECK_NULL(pGroup, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pProgress = tSimpleHashGet(pContext->pReaderWalProgress, &pGroup->vgId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
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
      pReq->cids = pProgress->reqCids;
      taosArrayClear(pReq->cids);
      int32_t nTrigCols = blockDataGetNumOfCols(pTask->pVirtTrigBlock);
      int32_t nCalcCols = blockDataGetNumOfCols(pTask->pVirtCalcBlock);
      for (int32_t i = 0; i < nTrigCols; i++) {
        SColumnInfoData *pCol = TARRAY_GET_ELEM(pTask->pVirtTrigBlock->pDataBlock, i);
        if (*(bool *)TARRAY_GET_ELEM(pTask->pTrigIsPseudoCol, i)) {
          continue;
        }
        void *px = taosArrayPush(pReq->cids, &pCol->info.colId);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      }
      for (int32_t i = 0; i < nCalcCols; i++) {
        SColumnInfoData *pCol = TARRAY_GET_ELEM(pTask->pVirtCalcBlock->pDataBlock, i);
        bool             hasExist = false;
        for (int32_t j = 0; j < nTrigCols; j++) {
          SColumnInfoData *pTrigCol = TARRAY_GET_ELEM(pTask->pVirtTrigBlock->pDataBlock, j);
          if (pCol->info.colId == pTrigCol->info.colId) {
            hasExist = true;
            break;
          }
        }
        if (!hasExist) {
          void *px = taosArrayPush(pReq->cids, &pCol->info.colId);
          QUERY_CHECK_NULL(px, code, lino, _end, terrno);
        }
      }
      break;
    }

    case STRIGGER_PULL_VTABLE_PSEUDO_COL: {
      SSTriggerRealtimeGroup *pGroup = stRealtimeContextGetCurrentGroup(pContext);
      QUERY_CHECK_NULL(pGroup, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SObjList *pUids =
          pContext->status == STRIGGER_CONTEXT_CHECK_CONDITION ? &pGroup->tableUids : &pContext->pCalcTableUids;
      int64_t *ar = taosObjListGetHead(pUids);
      QUERY_CHECK_NULL(ar, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      int64_t                 vtbUid = ar[0];
      SSTriggerVirtTableInfo *pTable = tSimpleHashGet(pTask->pVirtTableInfos, &vtbUid, sizeof(int64_t));
      QUERY_CHECK_NULL(pTable, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pProgress = tSimpleHashGet(pContext->pReaderWalProgress, &pTable->vgId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SSTriggerVirTablePseudoColRequest *pReq = &pProgress->pullReq.virTablePseudoColReq;
      pReq->uid = pTable->tbUid;
      pReq->cids = pProgress->reqCids;
      taosArrayClear(pReq->cids);
      SSDataBlock *pVirDataBlock =
          (pContext->status == STRIGGER_CONTEXT_CHECK_CONDITION) ? pTask->pVirtTrigBlock : pTask->pVirtCalcBlock;
      SArray *pIsPseudoCol =
          (pContext->status == STRIGGER_CONTEXT_CHECK_CONDITION) ? pTask->pTrigIsPseudoCol : pTask->pCalcIsPseudoCol;
      int32_t ncol = blockDataGetNumOfCols(pVirDataBlock);
      QUERY_CHECK_CONDITION(ncol == TARRAY_SIZE(pIsPseudoCol), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      for (int32_t i = 0; i < ncol; i++) {
        if (!*(bool *)TARRAY_GET_ELEM(pIsPseudoCol, i)) {
          continue;
        }
        SColumnInfoData *pCol = TARRAY_GET_ELEM(pVirDataBlock->pDataBlock, i);
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
      int32_t iter1 = 0;
      void   *px = tSimpleHashIterate(pTask->pOrigTableCols, NULL, &iter1);
      while (px != NULL) {
        SSHashObj               *pDbInfo = *(SSHashObj **)px;
        int32_t                  iter2 = 0;
        SSTriggerOrigColumnInfo *pTbInfo = tSimpleHashIterate(pDbInfo, NULL, &iter2);
        while (pTbInfo != NULL) {
          if (pTbInfo->vgId == pProgress->pTaskAddr->nodeId) {
            char   *tbName = tSimpleHashGetKey(pTbInfo, NULL);
            int32_t iter3 = 0;
            void   *px2 = tSimpleHashIterate(pTbInfo->pColumns, NULL, &iter3);
            while (px2 != NULL) {
              char       *colName = tSimpleHashGetKey(px2, NULL);
              OTableInfo *pInfo = taosArrayReserve(pReq->cols, 1);
              QUERY_CHECK_NULL(pInfo, code, lino, _end, terrno);
              (void)strncpy(pInfo->refTableName, tbName, sizeof(pInfo->refTableName));
              (void)strncpy(pInfo->refColName, colName, sizeof(pInfo->refColName));
              px2 = tSimpleHashIterate(pTbInfo->pColumns, px2, &iter3);
            }
          }
          pTbInfo = tSimpleHashIterate(pDbInfo, pTbInfo, &iter2);
        }
        px = tSimpleHashIterate(pTask->pOrigTableCols, px, &iter1);
      }
      break;
    }

    case STRIGGER_PULL_SET_TABLE: {
      SStreamTaskAddr *pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
      QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
      pProgress = tSimpleHashGet(pContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SSTriggerSetTableRequest *pReq = &pProgress->pullReq.setTableReq;
      pReq->uidInfoTrigger = pProgress->uidInfoTrigger;
      pReq->uidInfoCalc = pProgress->uidInfoCalc;
      tSimpleHashClear(pReq->uidInfoTrigger);
      tSimpleHashClear(pReq->uidInfoCalc);
      int32_t                 iter1 = 0;
      SSTriggerOrigTableInfo *pInfo = tSimpleHashIterate(pTask->pOrigTableInfos, NULL, &iter1);
      while (pInfo != NULL) {
        if (tSimpleHashGetSize(pInfo->pTrigColMap) > 0) {
          SSHashObj *pMatch = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT));
          QUERY_CHECK_NULL(pMatch, code, lino, _end, terrno);
          int64_t id[2] = {pInfo->tbSuid, pInfo->tbUid};
          code = tSimpleHashPut(pReq->uidInfoTrigger, id, sizeof(id), &pMatch, POINTER_BYTES);
          if (code != TSDB_CODE_SUCCESS) {
            tSimpleHashCleanup(pMatch);
            QUERY_CHECK_CODE(code, lino, _end);
          }

          int32_t   iter2 = 0;
          col_id_t *pSlotId = tSimpleHashIterate(pInfo->pTrigColMap, NULL, &iter2);
          while (pSlotId != NULL) {
            col_id_t *pColId = tSimpleHashGetKey(pSlotId, NULL);
            code = tSimpleHashPut(pMatch, pSlotId, sizeof(col_id_t), pColId, sizeof(col_id_t));
            QUERY_CHECK_CODE(code, lino, _end);
            pSlotId = tSimpleHashIterate(pInfo->pTrigColMap, pSlotId, &iter2);
          }
        }
        if (tSimpleHashGetSize(pInfo->pCalcColMap) > 0) {
          SSHashObj *pMatch = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT));
          QUERY_CHECK_NULL(pMatch, code, lino, _end, terrno);
          int64_t id[2] = {pInfo->tbSuid, pInfo->tbUid};
          code = tSimpleHashPut(pReq->uidInfoCalc, id, sizeof(id), &pMatch, POINTER_BYTES);
          if (code != TSDB_CODE_SUCCESS) {
            tSimpleHashCleanup(pMatch);
            QUERY_CHECK_CODE(code, lino, _end);
          }

          int32_t   iter2 = 0;
          col_id_t *pSlotId = tSimpleHashIterate(pInfo->pCalcColMap, NULL, &iter2);
          while (pSlotId != NULL) {
            col_id_t *pColId = tSimpleHashGetKey(pSlotId, NULL);
            code = tSimpleHashPut(pMatch, pSlotId, sizeof(col_id_t), pColId, sizeof(col_id_t));
            QUERY_CHECK_CODE(code, lino, _end);
            pSlotId = tSimpleHashIterate(pInfo->pCalcColMap, pSlotId, &iter2);
          }
        }
        pInfo = tSimpleHashIterate(pTask->pOrigTableInfos, pInfo, &iter1);
      }
      int32_t iter = 0;
      void   *px = tSimpleHashIterate(pReq->uidInfoTrigger, NULL, &iter);
      while (px != NULL) {
        int64_t   *pUid = tSimpleHashGetKey(px, NULL);
        SSHashObj *info = *(SSHashObj **)px;
        int32_t    iter1 = 0;
        void      *px1 = tSimpleHashIterate(info, NULL, &iter1);
        while (px1 != NULL) {
          int16_t *slot = tSimpleHashGetKey(px1, NULL);
          int16_t *cid = (int16_t *)px1;
          ST_TASK_DLOG("SetTable: [trigger] suid: %" PRId64 ", uid: %" PRId64 ", slot: %d, cid: %d", *pUid, *(pUid + 1),
                       *slot, *cid);
          px1 = tSimpleHashIterate(info, px1, &iter1);
        }
        px = tSimpleHashIterate(pReq->uidInfoTrigger, px, &iter);
      }
      iter = 0;
      px = tSimpleHashIterate(pReq->uidInfoCalc, NULL, &iter);
      while (px != NULL) {
        int64_t   *pUid = tSimpleHashGetKey(px, NULL);
        SSHashObj *info = *(SSHashObj **)px;
        int32_t    iter1 = 0;
        void      *px1 = tSimpleHashIterate(info, NULL, &iter1);
        while (px1 != NULL) {
          int16_t *slot = tSimpleHashGetKey(px1, NULL);
          int16_t *cid = (int16_t *)px1;
          ST_TASK_DLOG("SetTable: [calc] suid: %" PRId64 ", uid: %" PRId64 ", slot: %d, cid: %d", *pUid, *(pUid + 1),
                       *slot, *cid);
          px1 = tSimpleHashIterate(info, px1, &iter1);
        }
        px = tSimpleHashIterate(pReq->uidInfoCalc, px, &iter);
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

  ST_TASK_DLOG("send pull request of type %d to node:%d task:%" PRIx64 ", msgId: 0x%" PRIx64 ":0x%" PRIx64, pReq->type,
               pReader->nodeId, pReader->taskId, msg.info.traceId.rootId, msg.info.traceId.msgId);

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

  SSTriggerRealtimeGroup *pGroup = stRealtimeContextGetCurrentGroup(pContext);
  QUERY_CHECK_NULL(pGroup, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

  if (pCalcReq->createTable && pTask->hasPartitionBy || (pTask->placeHolderBitmap & PLACE_HOLDER_PARTITION_IDX) ||
      (pTask->placeHolderBitmap & PLACE_HOLDER_PARTITION_TBNAME)) {
    needTagValue = true;
  }

  if (needTagValue && taosArrayGetSize(pCalcReq->groupColVals) == 0) {
    void *px = tSimpleHashGet(pContext->pGroupColVals, &pGroup->gid, sizeof(int64_t));
    if (px == NULL) {
      code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_GROUP_COL_VALUE);
      QUERY_CHECK_CODE(code, lino, _end);
      goto _end;
    } else {
      SArray *pGroupColVals = *(SArray **)px;
      for (int32_t i = 0; i < TARRAY_SIZE(pGroupColVals); i++) {
        SStreamGroupValue *pValue = TARRAY_GET_ELEM(pGroupColVals, i);
        SStreamGroupValue *pDst = taosArrayPush(pCalcReq->groupColVals, pValue);
        QUERY_CHECK_NULL(pDst, code, lino, _end, terrno);
        if (IS_VAR_DATA_TYPE(pValue->data.type) || pValue->data.type == TSDB_DATA_TYPE_DECIMAL) {
          pDst->data.pData = taosMemoryMalloc(pDst->data.nData);
          QUERY_CHECK_NULL(pDst->data.pData, code, lino, _end, terrno);
          TAOS_MEMCPY(pDst->data.pData, pValue->data.pData, pDst->data.nData);
        }
      }
    }
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

    if (pContext->calcRange.ekey == INT64_MIN) {
      SSTriggerCalcParam *pFirstParam = TARRAY_DATA(pCalcReq->params);
      pContext->calcRange.skey = pFirstParam->wstart;
      pContext->calcRange.ekey = pGroup->newThreshold;
      STimeWindow metaRange = {.skey = INT64_MAX, .ekey = INT64_MIN};
      int32_t     iter1 = 0;
      SObjList   *pMetas = tSimpleHashIterate(pGroup->pWalMetas, NULL, &iter1);
      while (pMetas != NULL) {
        SSTriggerMetaData *pMeta = NULL;
        SObjListIter       iter2 = {0};
        taosObjListInitIter(pMetas, &iter2, TOBJLIST_ITER_FORWARD);
        while ((pMeta = taosObjListIterNext(&iter2)) != NULL) {
          metaRange.skey = TMIN(metaRange.skey, pMeta->skey);
          metaRange.ekey = TMAX(metaRange.ekey, pMeta->ekey);
        }
        pMetas = tSimpleHashIterate(pGroup->pWalMetas, pMetas, &iter1);
      }
      ST_TASK_DLOG("meta range is [%" PRId64 ", %" PRId64 "] for groupId:%" PRId64, metaRange.skey, metaRange.ekey,
                   pGroup->gid);
      ST_TASK_DLOG("calc range is [%" PRId64 ", %" PRId64 "] for groupId:%" PRId64, pContext->calcRange.skey,
                   pContext->calcRange.ekey, pGroup->gid);
      QUERY_CHECK_CONDITION(pContext->calcRange.skey <= pContext->calcRange.ekey, code, lino, _end,
                            TSDB_CODE_INVALID_PARA);
      if (pContext->calcRange.skey < metaRange.skey && metaRange.skey <= pContext->calcRange.ekey) {
        pContext->calcRange.skey = metaRange.skey;
      }

      // fill calc range
      tSimpleHashClear(pContext->pRanges);
      if (pTask->isVirtualTable) {
        int32_t                 iter = 0;
        SSTriggerVirtTableInfo *pVirtTableInfo = tSimpleHashIterate(pTask->pVirtTableInfos, NULL, &iter);
        while (pVirtTableInfo != NULL) {
          if (pVirtTableInfo->tbGid == pGroup->gid) {
            for (int32_t i = 0; i < TARRAY_SIZE(pVirtTableInfo->pCalcColRefs); i++) {
              SSTriggerTableColRef *pRef = TARRAY_GET_ELEM(pVirtTableInfo->pCalcColRefs, i);
              code = tSimpleHashPut(pContext->pRanges, &pRef->otbUid, sizeof(int64_t), &pContext->calcRange,
                                    sizeof(STimeWindow));
              QUERY_CHECK_CODE(code, lino, _end);
            }
          }
          pVirtTableInfo = tSimpleHashIterate(pTask->pVirtTableInfos, pVirtTableInfo, &iter);
        }
      } else {
        code =
            tSimpleHashPut(pContext->pRanges, &pGroup->gid, sizeof(int64_t), &pContext->calcRange, sizeof(STimeWindow));
        QUERY_CHECK_CODE(code, lino, _end);
      }
      for (pContext->curReaderIdx = 0; pContext->curReaderIdx < TARRAY_SIZE(pTask->readerList);
           pContext->curReaderIdx++) {
        code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_WAL_CALC_DATA_NEW);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      goto _end;
    }

    if (pContext->pCurParam == NULL) {
      pContext->pCurParam = TARRAY_DATA(pCalcReq->params);
      pContext->curParamRows = 0;
      taosObjListClear(&pContext->pCalcTableUids);
      int64_t     *ar = NULL;
      SObjListIter iter = {0};
      taosObjListInitIter(&pContext->pAllCalcTableUids, &iter, TOBJLIST_ITER_FORWARD);
      while ((ar = taosObjListIterNext(&iter)) != NULL) {
        code = taosObjListAppend(&pContext->pCalcTableUids, ar);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }

    while (TARRAY_ELEM_IDX(pCalcReq->params, pContext->pCurParam) < TARRAY_SIZE(pCalcReq->params)) {
      SSDataBlock *pDataBlock = NULL;
      int32_t      startIdx = 0;
      int32_t      endIdx = 0;
      while (true) {
        code = stRealtimeGroupNextDataBlock(pGroup, &pDataBlock, &startIdx, &endIdx);
        QUERY_CHECK_CODE(code, lino, _end);
        if (pContext->needPseudoCols || pDataBlock == NULL || startIdx >= endIdx) {
          break;
        }
        if (pTask->isVirtualTable) {
          code = putStreamDataCache(pContext->pCalcDataCache, pGroup->gid, pContext->pCurParam->wstart,
                                    pContext->pCurParam->wend, pDataBlock, startIdx, endIdx - 1);
          QUERY_CHECK_CODE(code, lino, _end);
        } else {
          TARRAY_SIZE(pDataBlock->pDataBlock)--;
          code = putStreamDataCache(pContext->pCalcDataCache, pGroup->gid, pContext->pCurParam->wstart,
                                    pContext->pCurParam->wend, pDataBlock, startIdx, endIdx - 1);
          QUERY_CHECK_CODE(code, lino, _end);
          TARRAY_SIZE(pDataBlock->pDataBlock)++;
        }
        pContext->curParamRows += (endIdx - startIdx);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      if (pContext->needPseudoCols) {
        code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_VTABLE_PSEUDO_COL);
        QUERY_CHECK_CODE(code, lino, _end);
        pContext->needPseudoCols = false;
        goto _end;
      }
      ST_TASK_DLOG("write data cache of groupId:%" PRId64 " wstart:%" PRId64 " wend:%" PRId64 " nrows:%" PRId64,
                   pGroup->gid, pContext->pCurParam->wstart, pContext->pCurParam->wend, pContext->curParamRows);
      pContext->pCurParam++;
      pContext->curParamRows = 0;
      taosObjListClear(&pContext->pCalcTableUids);
      int64_t     *ar = NULL;
      SObjListIter iter = {0};
      taosObjListInitIter(&pContext->pAllCalcTableUids, &iter, TOBJLIST_ITER_FORWARD);
      while ((ar = taosObjListIterNext(&iter)) != NULL) {
        code = taosObjListAppend(&pContext->pCalcTableUids, ar);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
  }

  // amend ekey of interval window trigger and sliding trigger
  for (int32_t i = 0; i < TARRAY_SIZE(pCalcReq->params); i++) {
    SSTriggerCalcParam *pParam = taosArrayGet(pCalcReq->params, i);
    if (pTask->triggerType == STREAM_TRIGGER_PERIOD) {
      pParam->prevLocalTime = pContext->periodWindow.skey - 1;
      pParam->triggerTime = pContext->periodWindow.ekey;
      if (pTask->placeHolderBitmap & PLACE_HOLDER_NEXT_LOCAL) {
        STimeWindow nextWin = pContext->periodWindow;
        stTriggerTaskNextTimeWindow(pTask, &nextWin);
        pParam->nextLocalTime = nextWin.ekey;
      }
    } else if (pTask->triggerType == STREAM_TRIGGER_SLIDING) {
      if (pTask->interval.interval > 0) {
        pParam->wend++;
        pParam->wduration++;
      } else {
        pParam->prevTs--;
      }
    }
    ST_TASK_DLOG("[calc param %d]: gid=%" PRId64 ", wstart=%" PRId64 ", wend=%" PRId64 ", nrows=%" PRId64
                 ", prevTs=%" PRId64 ", currentTs=%" PRId64 ", nextTs=%" PRId64 ", prevLocalTime=%" PRId64
                 ", nextLocalTime=%" PRId64 ", localTime=%" PRId64 ", create=%d",
                 i, pCalcReq->gid, pParam->wstart, pParam->wend, pParam->wrownum, pParam->prevTs, pParam->currentTs,
                 pParam->nextTs, pParam->prevLocalTime, pParam->nextLocalTime, pParam->triggerTime,
                 pCalcReq->createTable);
  }

#ifdef SKIP_SEND_CALC_REQUEST
  code = stTriggerTaskReleaseRequest(pTask, &pContext->pCalcReq);
  QUERY_CHECK_CODE(code, lino, _end);
  goto _end;
#endif

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
  if (code != TSDB_CODE_SUCCESS) {
    destroyAhandle(msg.info.ahandle);
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stRealtimeContextSendDropTableReq(SSTriggerRealtimeContext *pContext, int64_t gid,
                                                 SSTriggerDropRequest *pDropReq, bool *needColVal) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  SStreamTriggerTask  *pTask = pContext->pTask;
  SStreamRunnerTarget *pCalcRunner = NULL;
  bool                 needTagValue = false;
  SRpcMsg              msg = {.msgType = TDMT_STREAM_TRIGGER_DROP};

  int32_t nRunners = taosArrayGetSize(pTask->runnerList);
  for (int32_t i = 0; i < nRunners; i++) {
    pCalcRunner = TARRAY_GET_ELEM(pTask->runnerList, i);
    if (pCalcRunner->addr.taskId == pDropReq->runnerTaskId) {
      break;
    }
    pCalcRunner = NULL;
  }
  QUERY_CHECK_NULL(pCalcRunner, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

  if (pTask->hasPartitionBy || (pTask->placeHolderBitmap & PLACE_HOLDER_PARTITION_IDX) ||
      (pTask->placeHolderBitmap & PLACE_HOLDER_PARTITION_TBNAME)) {
    needTagValue = true;
  }

  if (needTagValue && taosArrayGetSize(pDropReq->groupColVals) == 0) {
    *needColVal = true;
    code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_GROUP_COL_VALUE);
    QUERY_CHECK_CODE(code, lino, _end);
    code = tdListAppend(&pContext->dropTableReqs, &pDropReq);
    QUERY_CHECK_CODE(code, lino, _end);
    goto _end;
  }
  *needColVal = false;

  // serialize and send request
  QUERY_CHECK_CODE(stTriggerTaskAllocAhandle(pTask, pContext->sessionId, pDropReq, &msg.info.ahandle), lino, _end);
  ST_TASK_DLOG("trigger calc req ahandle %p allocated", msg.info.ahandle);

  msg.contLen = tSerializeSTriggerDropTableRequest(NULL, 0, pDropReq);
  QUERY_CHECK_CONDITION(msg.contLen > 0, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  msg.contLen += sizeof(SMsgHead);
  msg.pCont = rpcMallocCont(msg.contLen);
  QUERY_CHECK_NULL(msg.pCont, code, lino, _end, terrno);
  SMsgHead *pMsgHead = (SMsgHead *)msg.pCont;
  pMsgHead->contLen = htonl(msg.contLen);
  pMsgHead->vgId = htonl(SNODE_HANDLE);
  int32_t tlen = tSerializeSTriggerDropTableRequest((char *)msg.pCont + sizeof(SMsgHead),
                                                    msg.contLen - sizeof(SMsgHead), pDropReq);
  QUERY_CHECK_CONDITION(tlen == msg.contLen - sizeof(SMsgHead), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  TRACE_SET_ROOTID(&msg.info.traceId, pTask->task.streamId);
  TRACE_SET_MSGID(&msg.info.traceId, tGenIdPI64());

  code = tmsgSendReq(&pCalcRunner->addr.epset, &msg);
  QUERY_CHECK_CODE(code, lino, _end);

  ST_TASK_DLOG("send drop table request to node:%d task:%" PRIx64, pCalcRunner->addr.nodeId, pCalcRunner->addr.taskId);
  ST_TASK_DLOG("trigger drop table req 0x%" PRIx64 ":0x%" PRIx64 " sent", msg.info.traceId.rootId,
               msg.info.traceId.msgId);

_end:
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
    void *px = tSimpleHashGet(pContext->pGroupColVals, &pReq->gid, sizeof(int64_t));
    QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    SArray *pGroupColVals = *(SArray **)px;
    for (int32_t i = 0; i < TARRAY_SIZE(pGroupColVals); i++) {
      SStreamGroupValue *pValue = TARRAY_GET_ELEM(pGroupColVals, i);
      SStreamGroupValue *pDst = taosArrayPush(pReq->groupColVals, pValue);
      QUERY_CHECK_NULL(pDst, code, lino, _end, terrno);
      if (IS_VAR_DATA_TYPE(pValue->data.type) || pValue->data.type == TSDB_DATA_TYPE_DECIMAL) {
        pDst->data.pData = taosMemoryMalloc(pDst->data.nData);
        QUERY_CHECK_NULL(pDst->data.pData, code, lino, _end, terrno);
        TAOS_MEMCPY(pDst->data.pData, pValue->data.pData, pDst->data.nData);
      }
    }
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

static int32_t stRealtimeContextRetryDropRequest(SSTriggerRealtimeContext *pContext, SListNode *pNode,
                                                 SSTriggerDropRequest *pReq) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  SStreamTriggerTask  *pTask = pContext->pTask;
  SStreamRunnerTarget *pRunner = NULL;
  bool                 needTagValue = false;
  SRpcMsg              msg = {.msgType = TDMT_STREAM_TRIGGER_DROP};

  QUERY_CHECK_NULL(pNode, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pReq, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(*(SSTriggerDropRequest **)pNode->data == pReq, code, lino, _end, TSDB_CODE_INVALID_PARA);

  int32_t nRunners = taosArrayGetSize(pTask->runnerList);
  for (int32_t i = 0; i < nRunners; i++) {
    SStreamRunnerTarget *pTempRunner = TARRAY_GET_ELEM(pTask->runnerList, i);
    if (pTempRunner->addr.taskId == pReq->runnerTaskId) {
      pRunner = pTempRunner;
      break;
    }
  }
  QUERY_CHECK_NULL(pRunner, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

  if (pTask->hasPartitionBy || (pTask->placeHolderBitmap & PLACE_HOLDER_PARTITION_IDX) ||
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

  msg.contLen = tSerializeSTriggerDropTableRequest(NULL, 0, pReq);
  QUERY_CHECK_CONDITION(msg.contLen > 0, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  msg.contLen += sizeof(SMsgHead);
  msg.pCont = rpcMallocCont(msg.contLen);
  QUERY_CHECK_NULL(msg.pCont, code, lino, _end, terrno);
  SMsgHead *pMsgHead = (SMsgHead *)msg.pCont;
  pMsgHead->contLen = htonl(msg.contLen);
  pMsgHead->vgId = htonl(SNODE_HANDLE);
  int32_t tlen =
      tSerializeSTriggerDropTableRequest((char *)msg.pCont + sizeof(SMsgHead), msg.contLen - sizeof(SMsgHead), pReq);
  QUERY_CHECK_CONDITION(tlen == msg.contLen - sizeof(SMsgHead), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  TRACE_SET_ROOTID(&msg.info.traceId, pTask->task.streamId);
  TRACE_SET_MSGID(&msg.info.traceId, tGenIdPI64());

  code = tmsgSendReq(&pRunner->addr.epset, &msg);
  QUERY_CHECK_CODE(code, lino, _end);

  ST_TASK_DLOG("%s send drop table request to node:%d task:%" PRIx64, __func__, pRunner->addr.nodeId, pRunner->addr.taskId);
  ST_TASK_DLOG("%s trigger drop table req 0x%" PRIx64 ":0x%" PRIx64 " sent", __func__, msg.info.traceId.rootId, msg.info.traceId.msgId);

  pNode = tdListPopNode(&pContext->dropTableReqs, pNode);
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
  int64_t             now = taosGetTimestampNs();

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

      if (pTask->fillHistory || pTask->fillHistoryFirst) {
        code = stTriggerTaskAddRecalcRequest(pTask, NULL, NULL, pContext->pReaderWalProgress, true, false);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }

    if (pTask->triggerType != STREAM_TRIGGER_PERIOD) {
      pContext->status = STRIGGER_CONTEXT_FETCH_META;
      for (pContext->curReaderIdx = 0; pContext->curReaderIdx < TARRAY_SIZE(pTask->readerList);
           pContext->curReaderIdx++) {
        code = stRealtimeContextSendPullReq(pContext, (pContext->walMode == STRIGGER_WAL_META_WITH_DATA)
                                                          ? STRIGGER_PULL_WAL_META_DATA_NEW
                                                          : STRIGGER_PULL_WAL_META_NEW);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      goto _end;
    }

    // check if to start for period trigger
    if (pContext->periodWindow.skey == INT64_MIN) {
      pContext->periodWindow = stTriggerTaskGetTimeWindow(pTask, now);
    }
    if (now >= pContext->periodWindow.ekey) {
      pContext->status = STRIGGER_CONTEXT_FETCH_META;
      if (taosArrayGetSize(pTask->readerList) > 0) {
        // fetch wal meta from all readers
        for (pContext->curReaderIdx = 0; pContext->curReaderIdx < TARRAY_SIZE(pTask->readerList);
             pContext->curReaderIdx++) {
          code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_WAL_META_NEW);
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
          code = stRealtimeGroupInit(pGroup, pContext, 0, 0);
          QUERY_CHECK_CODE(code, lino, _end);
        } else {
          int32_t iter = 0;
          void   *px = tSimpleHashIterate(pContext->pGroups, NULL, &iter);
          QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
          pGroup = *(SSTriggerRealtimeGroup **)px;
        }
        pGroup->oldThreshold = INT64_MIN;
        pGroup->newThreshold = INT64_MAX;
        if (!IS_TRIGGER_GROUP_TO_CHECK(pGroup)) {
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
        pContext->status = STRIGGER_CONTEXT_CHECK_CONDITION;
      }
      case STRIGGER_CONTEXT_CHECK_CONDITION: {
        code = stRealtimeGroupCheck(pGroup);
        QUERY_CHECK_CODE(code, lino, _end);
        if (pContext->needPseudoCols) {
          code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_VTABLE_PSEUDO_COL);
          QUERY_CHECK_CODE(code, lino, _end);
          pContext->needPseudoCols = false;
          goto _end;
        }

        if (taosArrayGetSize(pContext->pNotifyParams) > 0) {
          code = streamSendNotifyContent(&pTask->task, pTask->streamName, NULL, pTask->triggerType, pGroup->gid,
                                         pTask->pNotifyAddrUrls, pTask->addOptions,
                                         TARRAY_DATA(pContext->pNotifyParams), TARRAY_SIZE(pContext->pNotifyParams));
          QUERY_CHECK_CODE(code, lino, _end);
        }
        stRealtimeGroupClearTempState(pGroup);
        break;
      }
      default: {
        ST_TASK_ELOG("invalid context status %d at %s:%d", pContext->status, __func__, __LINE__);
        code = TSDB_CODE_INVALID_PARA;
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
    if (!pContext->needCheckAgain) {
      stRealtimeGroupClearMetadatas(pGroup);
      TD_DLIST_POP(&pContext->groupsToCheck, pGroup);
    }
    pContext->status = STRIGGER_CONTEXT_ACQUIRE_REQUEST;
  }

  if (pContext->pMinGroup == NULL && pContext->pMaxDelayHeap->min != NULL) {
    pContext->pMinGroup = container_of(pContext->pMaxDelayHeap->min, SSTriggerRealtimeGroup, heapNode);
    if (pContext->pMinGroup->nextExecTime > now) {
      pContext->pMinGroup = NULL;
    }
  }
  while (pContext->pMinGroup != NULL) {
    SSTriggerRealtimeGroup *pGroup = pContext->pMinGroup;
    switch (pContext->status) {
      case STRIGGER_CONTEXT_FETCH_META: {
        pContext->status = STRIGGER_CONTEXT_ACQUIRE_REQUEST;
      }
      case STRIGGER_CONTEXT_ACQUIRE_REQUEST: {
        if (pContext->pCalcReq == NULL && pTask->calcEventType != STRIGGER_EVENT_WINDOW_NONE) {
          code = stTriggerTaskAcquireRequest(pTask, pContext->sessionId, pGroup->gid, &pContext->pCalcReq);
          QUERY_CHECK_CODE(code, lino, _end);
          if (pContext->pCalcReq == NULL) {
            if (pTask->lowLatencyCalc || pContext->calcParamPool.size >= STREAM_TRIGGER_MAX_PENDING_PARAMS) {
              ST_TASK_DLOG("pause due to no available runner for group %" PRId64, pGroup->gid);
              goto _end;
            } else {
              ST_TASK_DLOG("skip calc due to no available runner for group %" PRId64, pGroup->gid);
              pContext->pMinGroup = NULL;
              continue;
            }
          }
        }
        pContext->status = STRIGGER_CONTEXT_CHECK_CONDITION;
      }
      case STRIGGER_CONTEXT_CHECK_CONDITION: {
        code = stRealtimeGroupRetrievePendingCalc(pGroup);
        QUERY_CHECK_CODE(code, lino, _end);
        pContext->status = STRIGGER_CONTEXT_SEND_CALC_REQ;
      }
      case STRIGGER_CONTEXT_SEND_CALC_REQ: {
        if (pContext->pCalcReq == NULL) {
          // do nothing
        } else if (TARRAY_SIZE(pContext->pCalcReq->params) > 0) {
          code = stRealtimeContextSendCalcReq(pContext);
          QUERY_CHECK_CODE(code, lino, _end);
          if (pContext->pCalcReq != NULL) {
            // calc req has not been set
            goto _end;
          }
          if (pTask->placeHolderBitmap & PLACE_HOLDER_PARTITION_ROWS) {
            stRealtimeGroupClearMetadatas(pGroup);
          }
          stRealtimeGroupClearTempState(pGroup);
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
    heapRemove(pContext->pMaxDelayHeap, &pGroup->heapNode);
    if (pGroup->nextExecTime > 0) {
      heapInsert(pContext->pMaxDelayHeap, &pGroup->heapNode);
    }
    pContext->status = STRIGGER_CONTEXT_ACQUIRE_REQUEST;
    if (pContext->pMaxDelayHeap->min != NULL) {
      pContext->pMinGroup = container_of(pContext->pMaxDelayHeap->min, SSTriggerRealtimeGroup, heapNode);
      if (pContext->pMinGroup->nextExecTime <= now) {
        continue;
      }
    }
    pContext->pMinGroup = NULL;
  }

  int32_t deleteGroupNum = taosArrayGetSize(pContext->groupsToDelete);
  if (deleteGroupNum > 0) {
    pContext->status = STRIGGER_CONTEXT_SEND_DROP_REQ;
    bool allSent = true;
    for (int32_t i = deleteGroupNum - 1; i >= 0; i--) {
      int64_t               gid = ((int64_t *)TARRAY_DATA(pContext->groupsToDelete))[i];
      bool                  drop = true;
      SSTriggerDropRequest *pDropReq = NULL;
      code = stTriggerTaskAcquireDropTableRequest(pTask, pContext->sessionId, gid, &pDropReq);
      QUERY_CHECK_CODE(code, lino, _end);
      if (pDropReq) {
        pContext->dropReqIndex = i;
        bool needColVal = false;
        code = stRealtimeContextSendDropTableReq(pContext, gid, pDropReq, &needColVal);
        QUERY_CHECK_CODE(code, lino, _end);
        if (needColVal) {
          allSent = false;
        } else {
          code = stTriggerTaskReleaseDropTableRequest(pTask, &pDropReq);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        taosArrayRemove(pContext->groupsToDelete, i);
      }
    }
    if (!allSent) {
      goto _end;
    }
  }

#define STRIGGER_CHECKPOINT_INTERVAL_NS 10 * NANOSECOND_PER_MINUTE  // 10min
  if (pContext->lastCheckpointTime + STRIGGER_CHECKPOINT_INTERVAL_NS <= now) {
    // do checkpoint
    uint8_t *buf = NULL;
    int64_t  len = 0;
    do {
      stDebug("[checkpoint] generate checkpoint for stream %" PRIx64, pTask->task.streamId);
      code = stTriggerTaskGenCheckpoint(pTask, buf, &len);
      if (code != 0 || len == 0) break;
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
    stTriggerTaskNextTimeWindow(pTask, &pContext->periodWindow);
    pContext->status = STRIGGER_CONTEXT_IDLE;
    code = stTriggerTaskAddWaitSession(pTask, pContext->sessionId, pContext->periodWindow.ekey);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    // todo(kjq): start history calc if needed
    if (pContext->catchUp) {
      // add the task to wait list since it catches up all readers
      pContext->status = STRIGGER_CONTEXT_IDLE;
      int64_t resumeTime = taosGetTimestampNs() + STREAM_TRIGGER_IDLE_TIME_NS;
      code = stTriggerTaskAddWaitSession(pTask, pContext->sessionId, resumeTime);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      // pull new wal metas
      pContext->status = STRIGGER_CONTEXT_FETCH_META;
      for (pContext->curReaderIdx = 0; pContext->curReaderIdx < TARRAY_SIZE(pTask->readerList);
           pContext->curReaderIdx++) {
        code = stRealtimeContextSendPullReq(pContext, (pContext->walMode == STRIGGER_WAL_META_WITH_DATA)
                                                          ? STRIGGER_PULL_WAL_META_DATA_NEW
                                                          : STRIGGER_PULL_WAL_META_NEW);
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

static int32_t stRealtimeContextProcWalMeta(SSTriggerRealtimeContext *pContext, SSTriggerWalProgress *pProgress) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pContext->pTask;

  pProgress->lastScanVer = pContext->pMetaBlock->info.version;
  int32_t vgId = pProgress->pTaskAddr->nodeId;

  // add wal meta in groups
  int32_t nrows = blockDataGetNumOfRows(pContext->pMetaBlock);
  if (nrows > 0) {
    int32_t          iCol = 0;
    SColumnInfoData *pGidCol = taosArrayGet(pContext->pMetaBlock->pDataBlock, iCol++);
    QUERY_CHECK_NULL(pGidCol, code, lino, _end, terrno);
    int64_t         *pGids = (int64_t *)pGidCol->pData;
    SColumnInfoData *pSkeyCol = taosArrayGet(pContext->pMetaBlock->pDataBlock, iCol++);
    QUERY_CHECK_NULL(pSkeyCol, code, lino, _end, terrno);
    int64_t         *pSkeys = (int64_t *)pSkeyCol->pData;
    SColumnInfoData *pEkeyCol = taosArrayGet(pContext->pMetaBlock->pDataBlock, iCol++);
    QUERY_CHECK_NULL(pEkeyCol, code, lino, _end, terrno);
    int64_t         *pEkeys = (int64_t *)pEkeyCol->pData;
    SColumnInfoData *pVerCol = taosArrayGet(pContext->pMetaBlock->pDataBlock, iCol++);
    QUERY_CHECK_NULL(pVerCol, code, lino, _end, terrno);
    int64_t *pVers = (int64_t *)pVerCol->pData;
    if (pTask->isVirtualTable) {
      for (int32_t i = 0; i < nrows; i++) {
        int64_t                 otbUid = pGids[i];
        SSTriggerOrigTableInfo *pOrigTableInfo = tSimpleHashGet(pTask->pOrigTableInfos, &otbUid, sizeof(int64_t));
        QUERY_CHECK_NULL(pOrigTableInfo, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        for (int32_t j = 0; j < TARRAY_SIZE(pOrigTableInfo->pVtbUids); j++) {
          int64_t                 vtbUid = *(int64_t *)TARRAY_GET_ELEM(pOrigTableInfo->pVtbUids, j);
          SSTriggerVirtTableInfo *pVirtTableInfo = tSimpleHashGet(pTask->pVirtTableInfos, &vtbUid, sizeof(int64_t));
          QUERY_CHECK_NULL(pVirtTableInfo, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
          int64_t                 gid = pVirtTableInfo->tbGid;
          void                   *px = tSimpleHashGet(pContext->pGroups, &gid, sizeof(int64_t));
          SSTriggerRealtimeGroup *pGroup = NULL;
          if (px == NULL) {
            pGroup = taosMemoryCalloc(1, sizeof(SSTriggerRealtimeGroup));
            QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
            code = tSimpleHashPut(pContext->pGroups, &gid, sizeof(int64_t), &pGroup, POINTER_BYTES);
            if (code != TSDB_CODE_SUCCESS) {
              taosMemoryFreeClear(pGroup);
              QUERY_CHECK_CODE(code, lino, _end);
            }
            code = stRealtimeGroupInit(pGroup, pContext, gid, pVirtTableInfo->vgId);
            QUERY_CHECK_CODE(code, lino, _end);
          } else {
            pGroup = *(SSTriggerRealtimeGroup **)px;
          }
          SSTriggerMetaData meta = {.skey = pSkeys[i], .ekey = pEkeys[i], .ver = pVers[i]};
          code = stRealtimeGroupAddMeta(pGroup, vgId, &meta);
          QUERY_CHECK_CODE(code, lino, _end);
          if (pGroup->oldThreshold < pGroup->newThreshold && !IS_TRIGGER_GROUP_TO_CHECK(pGroup)) {
            TD_DLIST_APPEND(&pContext->groupsToCheck, pGroup);
          }
        }
      }
    } else {
      for (int32_t i = 0; i < nrows; i++) {
        int64_t                 gid = pGids[i];
        void                   *px = tSimpleHashGet(pContext->pGroups, &gid, sizeof(int64_t));
        SSTriggerRealtimeGroup *pGroup = NULL;
        if (px == NULL) {
          pGroup = taosMemoryCalloc(1, sizeof(SSTriggerRealtimeGroup));
          QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
          code = tSimpleHashPut(pContext->pGroups, &gid, sizeof(int64_t), &pGroup, POINTER_BYTES);
          if (code != TSDB_CODE_SUCCESS) {
            taosMemoryFreeClear(pGroup);
            QUERY_CHECK_CODE(code, lino, _end);
          }
          code = stRealtimeGroupInit(pGroup, pContext, gid, vgId);
          QUERY_CHECK_CODE(code, lino, _end);
        } else {
          pGroup = *(SSTriggerRealtimeGroup **)px;
        }
        SSTriggerMetaData meta = {.skey = pSkeys[i], .ekey = pEkeys[i], .ver = pVers[i]};
        code = stRealtimeGroupAddMeta(pGroup, vgId, &meta);
        QUERY_CHECK_CODE(code, lino, _end);
        if (pGroup->oldThreshold < pGroup->newThreshold && !IS_TRIGGER_GROUP_TO_CHECK(pGroup)) {
          TD_DLIST_APPEND(&pContext->groupsToCheck, pGroup);
        }
      }
    }
  }

  // process delete data
  nrows = blockDataGetNumOfRows(pContext->pDeleteBlock);
  if (nrows > 0) {
    ST_TASK_DLOG("got %d rows of delete data", nrows);
    int32_t          iCol = 0;
    SColumnInfoData *pGidCol = taosArrayGet(pContext->pDeleteBlock->pDataBlock, iCol++);
    QUERY_CHECK_NULL(pGidCol, code, lino, _end, terrno);
    int64_t         *pGids = (int64_t *)pGidCol->pData;
    SColumnInfoData *pSkeyCol = taosArrayGet(pContext->pDeleteBlock->pDataBlock, iCol++);
    QUERY_CHECK_NULL(pSkeyCol, code, lino, _end, terrno);
    int64_t         *pSkeys = (int64_t *)pSkeyCol->pData;
    SColumnInfoData *pEkeyCol = taosArrayGet(pContext->pDeleteBlock->pDataBlock, iCol++);
    QUERY_CHECK_NULL(pEkeyCol, code, lino, _end, terrno);
    int64_t         *pEkeys = (int64_t *)pEkeyCol->pData;
    SColumnInfoData *pVerCol = taosArrayGet(pContext->pDeleteBlock->pDataBlock, iCol++);
    QUERY_CHECK_NULL(pVerCol, code, lino, _end, terrno);
    int64_t *pVers = (int64_t *)pVerCol->pData;
    if (pTask->isVirtualTable) {
      for (int32_t i = 0; i < nrows; i++) {
        int64_t                 otbUid = pGids[i];
        SSTriggerOrigTableInfo *pOrigTableInfo = tSimpleHashGet(pTask->pOrigTableInfos, &otbUid, sizeof(int64_t));
        QUERY_CHECK_NULL(pOrigTableInfo, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        for (int32_t j = 0; j < TARRAY_SIZE(pOrigTableInfo->pVtbUids); j++) {
          int64_t                 vtbUid = *(int64_t *)TARRAY_GET_ELEM(pOrigTableInfo->pVtbUids, j);
          SSTriggerVirtTableInfo *pVirtTableInfo = tSimpleHashGet(pTask->pVirtTableInfos, &vtbUid, sizeof(int64_t));
          QUERY_CHECK_NULL(pVirtTableInfo, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
          int64_t                 gid = pVirtTableInfo->tbGid;
          void                   *px = tSimpleHashGet(pContext->pGroups, &gid, sizeof(int64_t));
          SSTriggerRealtimeGroup *pGroup = NULL;
          if (px == NULL) {
            pGroup = taosMemoryCalloc(1, sizeof(SSTriggerRealtimeGroup));
            QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
            code = tSimpleHashPut(pContext->pGroups, &gid, sizeof(int64_t), &pGroup, POINTER_BYTES);
            if (code != TSDB_CODE_SUCCESS) {
              taosMemoryFreeClear(pGroup);
              QUERY_CHECK_CODE(code, lino, _end);
            }
            code = stRealtimeGroupInit(pGroup, pContext, gid, pVirtTableInfo->vgId);
            QUERY_CHECK_CODE(code, lino, _end);
          } else {
            pGroup = *(SSTriggerRealtimeGroup **)px;
          }
          STimeWindow range = {.skey = pSkeys[i], .ekey = pEkeys[i]};
          if (pGroup->windows.neles > 0) {
            SSTriggerWindow *pWin = taosObjListGetHead(&pGroup->windows);
            if (pWin->range.skey <= range.ekey) {
              pGroup->recalcNextWindow = true;
            }
          }
          ST_TASK_DLOG("add recalc request for delete data, start: %" PRId64 ", end: %" PRId64, range.skey, range.ekey);
          code = stTriggerTaskAddRecalcRequest(pTask, pGroup, &range, pContext->pReaderWalProgress, false, true);
          QUERY_CHECK_CODE(code, lino, _end);
          code = stRealtimeGroupRemovePendingCalc(pGroup, &range);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
    } else {
      for (int32_t i = 0; i < nrows; i++) {
        int64_t                 gid = pGids[i];
        void                   *px = tSimpleHashGet(pContext->pGroups, &gid, sizeof(int64_t));
        SSTriggerRealtimeGroup *pGroup = NULL;
        if (px == NULL) {
          pGroup = taosMemoryCalloc(1, sizeof(SSTriggerRealtimeGroup));
          QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
          code = tSimpleHashPut(pContext->pGroups, &gid, sizeof(int64_t), &pGroup, POINTER_BYTES);
          if (code != TSDB_CODE_SUCCESS) {
            taosMemoryFreeClear(pGroup);
            QUERY_CHECK_CODE(code, lino, _end);
          }
          code = stRealtimeGroupInit(pGroup, pContext, gid, vgId);
          QUERY_CHECK_CODE(code, lino, _end);
        } else {
          pGroup = *(SSTriggerRealtimeGroup **)px;
        }
        STimeWindow range = {.skey = pSkeys[i], .ekey = pEkeys[i]};
        if (pGroup->windows.neles > 0) {
          SSTriggerWindow *pWin = taosObjListGetHead(&pGroup->windows);
          if (pWin->range.skey <= range.ekey) {
            pGroup->recalcNextWindow = true;
          }
        }
        ST_TASK_DLOG("add recalc request for delete data, start: %" PRId64 ", end: %" PRId64, range.skey, range.ekey);
        code = stTriggerTaskAddRecalcRequest(pTask, pGroup, &range, pContext->pReaderWalProgress, false, true);
        QUERY_CHECK_CODE(code, lino, _end);
        code = stRealtimeGroupRemovePendingCalc(pGroup, &range);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
  }

  // process new dropped tables
  nrows = blockDataGetNumOfRows(pContext->pDropBlock);
  if (nrows > 0) {
    SColumnInfoData *pGidCol = taosArrayGet(pContext->pDropBlock->pDataBlock, 0);
    QUERY_CHECK_NULL(pGidCol, code, lino, _end, terrno);
    int64_t *pGids = (int64_t *)pGidCol->pData;
    for (int32_t i = 0; i < nrows; i++) {
      int64_t gid = pGids[i];
      void   *pGroup = tSimpleHashGet(pContext->pGroups, &gid, sizeof(int64_t));
      if (pGroup == NULL) {
        pGroup = taosMemoryCalloc(1, sizeof(SSTriggerRealtimeGroup));
        QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
        code = tSimpleHashPut(pContext->pGroups, &gid, sizeof(int64_t), &pGroup, POINTER_BYTES);
        if (code != TSDB_CODE_SUCCESS) {
          taosMemoryFreeClear(pGroup);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        code = stRealtimeGroupInit(pGroup, pContext, gid, vgId);
        QUERY_CHECK_CODE(code, lino, _end);
      }

      void *px = taosArrayPush(pContext->groupsToDelete, &pGids[i]);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
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

  SMsgSendInfo         *ahandle = pRsp->info.ahandle;
  SSTriggerAHandle     *pAhandle = ahandle->param;
  SSTriggerPullRequest *pReq = pAhandle->param;

  QUERY_CHECK_CONDITION(pRsp->code == TSDB_CODE_SUCCESS || pRsp->code == TSDB_CODE_STREAM_NO_DATA, code, lino, _end,
                        TSDB_CODE_INVALID_PARA);

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
        if (pTask->isVirtualTable) {
          for (int32_t i = 0; i < nrows; i++) {
            int64_t                 obtUid = pGidData[i];
            SSTriggerOrigTableInfo *pOrigTableInfo = tSimpleHashGet(pTask->pOrigTableInfos, &obtUid, sizeof(int64_t));
            QUERY_CHECK_NULL(pOrigTableInfo, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
            for (int32_t j = 0; j < TARRAY_SIZE(pOrigTableInfo->pVtbUids); j++) {
              int64_t                 vtbUid = *(int64_t *)TARRAY_GET_ELEM(pOrigTableInfo->pVtbUids, j);
              SSTriggerVirtTableInfo *pVirtTableInfo = tSimpleHashGet(pTask->pVirtTableInfos, &vtbUid, sizeof(int64_t));
              QUERY_CHECK_NULL(pVirtTableInfo, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
              int64_t gid = pVirtTableInfo->tbGid;
              px = tSimpleHashGet(pTask->pHistoryCutoffTime, &gid, sizeof(int64_t));
              if (px == NULL) {
                code = tSimpleHashPut(pTask->pHistoryCutoffTime, &gid, sizeof(int64_t), &pTsData[i], sizeof(int64_t));
                QUERY_CHECK_CODE(code, lino, _end);
              } else {
                *(int64_t *)px = TMAX(*(int64_t *)px, pTsData[i]);
              }
            }
          }
        } else {
          for (int32_t i = 0; i < nrows; i++) {
            int64_t gid = pGidData[i];
            px = tSimpleHashGet(pTask->pHistoryCutoffTime, &gid, sizeof(int64_t));
            if (px == NULL) {
              code = tSimpleHashPut(pTask->pHistoryCutoffTime, &gid, sizeof(int64_t), &pTsData[i], sizeof(int64_t));
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
          pProgress->lastScanVer = *(int64_t *)px;
        }
      }
      pContext->status = STRIGGER_CONTEXT_IDLE;
      code = stRealtimeContextCheck(pContext);
      QUERY_CHECK_CODE(code, lino, _end);
      break;
    }

    case STRIGGER_PULL_WAL_META_NEW: {
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

      if (pRsp->code == TSDB_CODE_STREAM_NO_DATA) {
        QUERY_CHECK_CONDITION(pRsp->contLen == sizeof(int64_t), code, lino, _end, TSDB_CODE_INVALID_PARA);
        blockDataEmpty(pContext->pMetaBlock);
        blockDataEmpty(pContext->pDeleteBlock);
        blockDataEmpty(pContext->pDropBlock);
        pContext->pMetaBlock->info.version = *(int64_t *)pRsp->pCont;
      } else {
        QUERY_CHECK_CONDITION(pRsp->contLen > 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
        SSTriggerWalNewRsp rsp = {.metaBlock = pContext->pMetaBlock,
                                  .deleteBlock = pContext->pDeleteBlock,
                                  .dropBlock = pContext->pDropBlock};
        code = tDeserializeSStreamWalDataResponse(pRsp->pCont, pRsp->contLen, &rsp, NULL);
        QUERY_CHECK_CODE(code, lino, _end);
        pContext->pMetaBlock->info.version = rsp.ver;
        pProgress->verTime = rsp.verTime;
      }

      code = stRealtimeContextProcWalMeta(pContext, pProgress);
      QUERY_CHECK_CODE(code, lino, _end);

      if (blockDataGetNumOfRows(pContext->pMetaBlock) >= STREAM_RETURN_ROWS_NUM) {
        pContext->continueToFetch = true;
      }

      if (--pContext->curReaderIdx > 0) {
        ST_TASK_DLOG("wait for response from other %d readers", pContext->curReaderIdx);
        goto _end;
      }

      int64_t latestVersionTime = INT64_MAX;
      for (int32_t i = 0; i < TARRAY_SIZE(pTask->readerList); i++) {
        SStreamTaskAddr      *pReader = TARRAY_GET_ELEM(pTask->readerList, i);
        SSTriggerWalProgress *pTempProgress =
            tSimpleHashGet(pContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t));
        QUERY_CHECK_NULL(pTempProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        latestVersionTime = TMIN(latestVersionTime, pTempProgress->verTime);
      }
      if (latestVersionTime != INT64_MAX) {
        atomic_store_64(&pTask->latestVersionTime, latestVersionTime);
      }

      if (pContext->metaPool.size >= STREAM_TRIGGER_MAX_METAS) {
        ST_TASK_DLOG("stop fetch wal metas since meta pool is full, size: %" PRId64, pContext->metaPool.size);
        pContext->continueToFetch = false;
      }

      if (pContext->continueToFetch) {
        ST_TASK_DLOG("continue to fetch wal metas since some readers are not exhausted: %" PRIzu,
                     TARRAY_SIZE(pTask->readerList));
        for (pContext->curReaderIdx = 0; pContext->curReaderIdx < TARRAY_SIZE(pTask->readerList);
             pContext->curReaderIdx++) {
          code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_WAL_META_NEW);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        pContext->continueToFetch = false;
        goto _end;
      }

      if (pTask->triggerType == STREAM_TRIGGER_PERIOD && !pTask->ignoreNoDataTrigger) {
        int32_t iter = 0;
        void   *px = tSimpleHashIterate(pContext->pGroups, NULL, &iter);
        while (px != NULL) {
          SSTriggerRealtimeGroup *pGroup = *(SSTriggerRealtimeGroup **)px;
          if (!IS_TRIGGER_GROUP_TO_CHECK(pGroup)) {
            pGroup->oldThreshold = INT64_MIN;
            pGroup->newThreshold = INT64_MAX;
            TD_DLIST_APPEND(&pContext->groupsToCheck, pGroup);
          }
          px = tSimpleHashIterate(pContext->pGroups, px, &iter);
        }
      }

      if (pContext->walMode == STRIGGER_WAL_META_ONLY) {
        pContext->catchUp = (TD_DLIST_NELES(&pContext->groupsToCheck) == 0);
        code = stRealtimeContextCheck(pContext);
        QUERY_CHECK_CODE(code, lino, _end);
      } else {
        // fill ranges according to groupsToCheck
        tSimpleHashClear(pContext->pRanges);
        if (pTask->isVirtualTable) {
          int32_t                 iter = 0;
          SSTriggerVirtTableInfo *pVirtTableInfo = tSimpleHashIterate(pTask->pVirtTableInfos, NULL, &iter);
          while (pVirtTableInfo != NULL) {
            void *px = tSimpleHashGet(pContext->pGroups, &pVirtTableInfo->tbGid, sizeof(int64_t));
            if (px != NULL) {
              SSTriggerRealtimeGroup *pGroup = *(SSTriggerRealtimeGroup **)px;
              if (pGroup->oldThreshold < pGroup->newThreshold && IS_TRIGGER_GROUP_TO_CHECK(pGroup)) {
                STimeWindow range = {.skey = pGroup->oldThreshold + 1, .ekey = pGroup->newThreshold};
                for (int32_t i = 0; i < TARRAY_SIZE(pVirtTableInfo->pTrigColRefs); i++) {
                  SSTriggerTableColRef *pRef = TARRAY_GET_ELEM(pVirtTableInfo->pTrigColRefs, i);
                  STimeWindow          *pRange = tSimpleHashGet(pContext->pRanges, &pRef->otbUid, sizeof(int64_t));
                  if (pRange == NULL) {
                    code =
                        tSimpleHashPut(pContext->pRanges, &pRef->otbUid, sizeof(int64_t), &range, sizeof(STimeWindow));
                    QUERY_CHECK_CODE(code, lino, _end);
                  } else {
                    pRange->skey = TMIN(pRange->skey, range.skey);
                    pRange->ekey = TMAX(pRange->ekey, range.ekey);
                  }
                }
              }
            }
            pVirtTableInfo = tSimpleHashIterate(pTask->pVirtTableInfos, pVirtTableInfo, &iter);
          }
        } else {
          SSTriggerRealtimeGroup *pGroup = TD_DLIST_HEAD(&pContext->groupsToCheck);
          while (pGroup != NULL) {
            if (pGroup->oldThreshold < pGroup->newThreshold) {
              STimeWindow range = {.skey = pGroup->oldThreshold + 1, .ekey = pGroup->newThreshold};
              code = tSimpleHashPut(pContext->pRanges, &pGroup->gid, sizeof(int64_t), &range, sizeof(range));
              QUERY_CHECK_CODE(code, lino, _end);
            }
            pGroup = TD_DLIST_NODE_NEXT(pGroup);
          }
        }
        for (pContext->curReaderIdx = 0; pContext->curReaderIdx < TARRAY_SIZE(pTask->readerList);
             pContext->curReaderIdx++) {
          code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_WAL_DATA_NEW);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
      break;
    }

    case STRIGGER_PULL_WAL_DATA_NEW:
    case STRIGGER_PULL_WAL_META_DATA_NEW: {
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
      bool firstDataBlock = (blockDataGetNumOfCols(pProgress->pTrigBlock) == 0);

      if (pRsp->code == TSDB_CODE_STREAM_NO_DATA) {
        QUERY_CHECK_CONDITION(pRsp->contLen == sizeof(int64_t), code, lino, _end, TSDB_CODE_INVALID_PARA);
        if (pContext->walMode == STRIGGER_WAL_META_WITH_DATA) {
          blockDataEmpty(pContext->pMetaBlock);
          blockDataEmpty(pContext->pDeleteBlock);
          blockDataEmpty(pContext->pDropBlock);
          pContext->pMetaBlock->info.version = *(int64_t *)pRsp->pCont;
        }
        taosArrayClear(pContext->pTempSlices);
      } else {
        QUERY_CHECK_CONDITION(pRsp->contLen > 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
        SSTriggerWalNewRsp rsp = {.dataBlock = pProgress->pTrigBlock};
        if (pContext->walMode == STRIGGER_WAL_META_WITH_DATA) {
          rsp.metaBlock = pContext->pMetaBlock;
          rsp.deleteBlock = pContext->pDeleteBlock;
          rsp.dropBlock = pContext->pDropBlock;
        }
        code = tDeserializeSStreamWalDataResponse(pRsp->pCont, pRsp->contLen, &rsp, pContext->pTempSlices);
        QUERY_CHECK_CODE(code, lino, _end);
        if (pContext->walMode == STRIGGER_WAL_META_WITH_DATA) {
          pContext->pMetaBlock->info.version = rsp.ver;
          pProgress->verTime = rsp.verTime;
        }
      }

      if (pContext->walMode == STRIGGER_WAL_META_WITH_DATA) {
        code = stRealtimeContextProcWalMeta(pContext, pProgress);
        QUERY_CHECK_CODE(code, lino, _end);
      }

      int32_t nTables = TARRAY_SIZE(pContext->pTempSlices);
      ST_TASK_DLOG("receive %" PRId64 " rows trig data of %d tables from vnode %d", pProgress->pTrigBlock->info.rows,
                   nTables, pProgress->pTaskAddr->nodeId);
      if (pTask->isVirtualTable) {
        for (int32_t i = 0; i < nTables; i++) {
          int64_t           *ar = TARRAY_GET_ELEM(pContext->pTempSlices, i);
          int64_t            gid = ar[0];
          int64_t            otbUid = ar[1];
          int32_t            startIdx = ar[2] >> 32;
          int32_t            endIdx = ar[2];
          SSTriggerDataSlice slice = {.pDataBlock = pProgress->pTrigBlock, .startIdx = startIdx, .endIdx = endIdx};
          code = tSimpleHashPut(pContext->pSlices, &otbUid, sizeof(int64_t), &slice, sizeof(SSTriggerDataSlice));
          QUERY_CHECK_CODE(code, lino, _end);
          SSTriggerOrigTableInfo *pOrigTableInfo = tSimpleHashGet(pTask->pOrigTableInfos, &otbUid, sizeof(int64_t));
          QUERY_CHECK_NULL(pOrigTableInfo, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
          for (int32_t j = 0; j < TARRAY_SIZE(pOrigTableInfo->pVtbUids); j++) {
            int64_t                 vtbUid = *(int64_t *)TARRAY_GET_ELEM(pOrigTableInfo->pVtbUids, j);
            SSTriggerVirtTableInfo *pVirtTableInfo = tSimpleHashGet(pTask->pVirtTableInfos, &vtbUid, sizeof(int64_t));
            QUERY_CHECK_NULL(pVirtTableInfo, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
            int64_t gid = pVirtTableInfo->tbGid;
            void   *px = tSimpleHashGet(pContext->pGroups, &gid, sizeof(int64_t));
            if (px == NULL) {
              ST_TASK_ELOG("unable to find group %" PRId64 " for virt table %" PRId64 " orig table %" PRId64
                           " from vnode %d",
                           gid, vtbUid, otbUid, pProgress->pTaskAddr->nodeId);
              QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
            }
            SSTriggerRealtimeGroup *pGroup = *(SSTriggerRealtimeGroup **)px;
            if (IS_TRIGGER_GROUP_TO_CHECK(pGroup)) {
              int64_t id[2] = {vtbUid, otbUid};
              code = taosObjListAppend(&pGroup->tableUids, id);
              QUERY_CHECK_CODE(code, lino, _end);
            }
          }
        }
      } else {
        for (int32_t i = 0; i < nTables; i++) {
          int64_t           *ar = TARRAY_GET_ELEM(pContext->pTempSlices, i);
          int64_t            gid = ar[0];
          int64_t            uid = ar[1];
          int32_t            startIdx = ar[2] >> 32;
          int32_t            endIdx = ar[2];
          SSTriggerDataSlice slice = {.pDataBlock = pProgress->pTrigBlock, .startIdx = startIdx, .endIdx = endIdx};
          code = tSimpleHashPut(pContext->pSlices, &uid, sizeof(int64_t), &slice, sizeof(SSTriggerDataSlice));
          QUERY_CHECK_CODE(code, lino, _end);
          void *px = tSimpleHashGet(pContext->pGroups, &gid, sizeof(int64_t));
          if (px == NULL) {
            ST_TASK_ELOG("unable to find group %" PRId64 " for table %" PRId64 " from vnode %d", gid, uid,
                         pProgress->pTaskAddr->nodeId);
            QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
          }
          SSTriggerRealtimeGroup *pGroup = *(SSTriggerRealtimeGroup **)px;
          if (IS_TRIGGER_GROUP_TO_CHECK(pGroup)) {
            int64_t id[2] = {uid, pProgress->pTaskAddr->nodeId};
            code = taosObjListAppend(&pGroup->tableUids, id);
            QUERY_CHECK_CODE(code, lino, _end);
          }
        }
      }

      if (!pTask->isVirtualTable && blockDataGetNumOfRows(pProgress->pTrigBlock) > 0) {
        if (pTask->triggerType == STREAM_TRIGGER_EVENT) {
          SColumnInfoData *pStartCol = NULL;
          SColumnInfoData *pEndCol = NULL;
          if (firstDataBlock) {
            SColumnInfoData startCol = {0};
            void           *px = taosArrayPush(pProgress->pTrigBlock->pDataBlock, &startCol);
            QUERY_CHECK_NULL(px, code, lino, _end, terrno);
            SColumnInfoData endCol = {0};
            px = taosArrayPush(pProgress->pTrigBlock->pDataBlock, &endCol);
            QUERY_CHECK_NULL(px, code, lino, _end, terrno);
          }
          pEndCol = taosArrayGetLast(pProgress->pTrigBlock->pDataBlock);
          QUERY_CHECK_NULL(pEndCol, code, lino, _end, terrno);
          pStartCol = pEndCol - 1;
          code = stRealtimeContextCalcExpr(pContext, pProgress->pTrigBlock, pTask->pStartCond, pStartCol);
          QUERY_CHECK_CODE(code, lino, _end);
          code = stRealtimeContextCalcExpr(pContext, pProgress->pTrigBlock, pTask->pEndCond, pEndCol);
          QUERY_CHECK_CODE(code, lino, _end);
        } else if (pTask->triggerType == STREAM_TRIGGER_STATE && pTask->stateSlotId == -1) {
          SColumnInfoData *pStateCol = NULL;
          if (firstDataBlock) {
            SColumnInfoData stateCol = {0};
            void           *px = taosArrayPush(pProgress->pTrigBlock->pDataBlock, &stateCol);
            QUERY_CHECK_NULL(px, code, lino, _end, terrno);
          }
          pStateCol = taosArrayGetLast(pProgress->pTrigBlock->pDataBlock);
          QUERY_CHECK_NULL(pStateCol, code, lino, _end, terrno);
          code = stRealtimeContextCalcExpr(pContext, pProgress->pTrigBlock, pTask->pStateExpr, pStateCol);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }

      if (--pContext->curReaderIdx > 0) {
        ST_TASK_DLOG("wait for response from other %d readers", pContext->curReaderIdx);
        goto _end;
      }

      if (pContext->walMode == STRIGGER_WAL_META_WITH_DATA) {
        int64_t latestVersionTime = INT64_MAX;
        for (int32_t i = 0; i < TARRAY_SIZE(pTask->readerList); i++) {
          SStreamTaskAddr      *pReader = TARRAY_GET_ELEM(pTask->readerList, i);
          SSTriggerWalProgress *pTempProgress =
              tSimpleHashGet(pContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t));
          QUERY_CHECK_NULL(pTempProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
          latestVersionTime = TMIN(latestVersionTime, pTempProgress->verTime);
        }
        if (latestVersionTime != INT64_MAX) {
          atomic_store_64(&pTask->latestVersionTime, latestVersionTime);
        }
      }

      pContext->catchUp = (TD_DLIST_NELES(&pContext->groupsToCheck) == 0);
      code = stRealtimeContextCheck(pContext);
      QUERY_CHECK_CODE(code, lino, _end);
      break;
    }

    case STRIGGER_PULL_WAL_CALC_DATA_NEW: {
      QUERY_CHECK_CONDITION(pContext->status == STRIGGER_CONTEXT_SEND_CALC_REQ, code, lino, _end,
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
      if (pRsp->code == TSDB_CODE_STREAM_NO_DATA) {
        QUERY_CHECK_CONDITION(pRsp->contLen == sizeof(int64_t), code, lino, _end, TSDB_CODE_INVALID_PARA);
        blockDataEmpty(pProgress->pCalcBlock);
        taosArrayClear(pContext->pTempSlices);
      } else {
        QUERY_CHECK_CONDITION(pRsp->contLen > 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
        SSTriggerWalNewRsp rsp = {.dataBlock = pProgress->pCalcBlock};
        code = tDeserializeSStreamWalDataResponse(pRsp->pCont, pRsp->contLen, &rsp, pContext->pTempSlices);
        QUERY_CHECK_CODE(code, lino, _end);
      }

      int32_t nTables = TARRAY_SIZE(pContext->pTempSlices);
      ST_TASK_DLOG("receive %" PRId64 " rows calc data of %d tables from vnode %d", pProgress->pCalcBlock->info.rows,
                   nTables, pProgress->pTaskAddr->nodeId);
      if (pTask->isVirtualTable) {
        SSTriggerRealtimeGroup *pGroup = stRealtimeContextGetCurrentGroup(pContext);
        QUERY_CHECK_NULL(pGroup, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        for (int32_t i = 0; i < nTables; i++) {
          int64_t           *ar = TARRAY_GET_ELEM(pContext->pTempSlices, i);
          int64_t            gid = ar[0];
          int64_t            otbUid = ar[1];
          int32_t            startIdx = ar[2] >> 32;
          int32_t            endIdx = ar[2];
          SSTriggerDataSlice slice = {.pDataBlock = pProgress->pCalcBlock, .startIdx = startIdx, .endIdx = endIdx};
          code = tSimpleHashPut(pContext->pSlices, &otbUid, sizeof(int64_t), &slice, sizeof(SSTriggerDataSlice));
          QUERY_CHECK_CODE(code, lino, _end);
          SSTriggerOrigTableInfo *pOrigTableInfo = tSimpleHashGet(pTask->pOrigTableInfos, &otbUid, sizeof(int64_t));
          QUERY_CHECK_NULL(pOrigTableInfo, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
          for (int32_t j = 0; j < TARRAY_SIZE(pOrigTableInfo->pVtbUids); j++) {
            int64_t                 vtbUid = *(int64_t *)TARRAY_GET_ELEM(pOrigTableInfo->pVtbUids, j);
            SSTriggerVirtTableInfo *pVirtTableInfo = tSimpleHashGet(pTask->pVirtTableInfos, &vtbUid, sizeof(int64_t));
            QUERY_CHECK_NULL(pVirtTableInfo, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
            if (pVirtTableInfo->tbGid == pGroup->gid) {
              int64_t id[2] = {vtbUid, otbUid};
              code = taosObjListAppend(&pContext->pAllCalcTableUids, id);
              QUERY_CHECK_CODE(code, lino, _end);
            }
          }
        }
      } else {
        for (int32_t i = 0; i < nTables; i++) {
          int64_t           *ar = TARRAY_GET_ELEM(pContext->pTempSlices, i);
          int64_t            gid = ar[0];
          int64_t            uid = ar[1];
          int32_t            startIdx = ar[2] >> 32;
          int32_t            endIdx = ar[2];
          SSTriggerDataSlice slice = {.pDataBlock = pProgress->pCalcBlock, .startIdx = startIdx, .endIdx = endIdx};
          code = tSimpleHashPut(pContext->pSlices, &uid, sizeof(int64_t), &slice, sizeof(SSTriggerDataSlice));
          QUERY_CHECK_CODE(code, lino, _end);
          int64_t id[2] = {uid, pProgress->pTaskAddr->nodeId};
          code = taosObjListAppend(&pContext->pAllCalcTableUids, id);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }

      if (--pContext->curReaderIdx > 0) {
        ST_TASK_DLOG("wait for response from other %d readers", pContext->curReaderIdx);
        goto _end;
      }

      code = stRealtimeContextCheck(pContext);
      QUERY_CHECK_CODE(code, lino, _end);
      break;
    }

    case STRIGGER_PULL_GROUP_COL_VALUE: {
      QUERY_CHECK_CONDITION(
          (pContext->status == STRIGGER_CONTEXT_SEND_CALC_REQ || pContext->status == STRIGGER_CONTEXT_SEND_DROP_REQ),
          code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SSTriggerGroupColValueRequest *pRequest = (SSTriggerGroupColValueRequest *)pReq;
      switch (pContext->status) {
        case STRIGGER_CONTEXT_SEND_CALC_REQ: {
          SStreamGroupInfo groupInfo = {0};
          code = tDeserializeSStreamGroupInfo(pRsp->pCont, pRsp->contLen, &groupInfo);
          QUERY_CHECK_CODE(code, lino, _end);
          code =
              tSimpleHashPut(pContext->pGroupColVals, &pRequest->gid, sizeof(int64_t), &groupInfo.gInfo, POINTER_BYTES);
          if (code != TSDB_CODE_SUCCESS) {
            taosArrayClearEx(groupInfo.gInfo, tDestroySStreamGroupValue);
            QUERY_CHECK_CODE(code, lino, _end);
          }
          if (pContext->pCalcReq != NULL && pContext->pCalcReq->gid == pRequest->gid) {
            code = stRealtimeContextCheck(pContext);
            QUERY_CHECK_CODE(code, lino, _end);
          } else {
            SListIter  iter = {0};
            SListNode *pNode = NULL;
            tdListInitIter(&pContext->retryCalcReqs, &iter, TD_LIST_FORWARD);
            while ((pNode = tdListNext(&iter)) != NULL) {
              SSTriggerCalcRequest *pCalcReq = *(SSTriggerCalcRequest **)pNode->data;
              if (pCalcReq->gid == pRequest->gid) {
                QUERY_CHECK_CODE(code, lino, _end);
                code = stRealtimeContextRetryCalcRequest(pContext, pNode, pCalcReq);
                QUERY_CHECK_CODE(code, lino, _end);
                break;
              }
            }
          }
          break;
        }
        case STRIGGER_CONTEXT_SEND_DROP_REQ: {
          SListIter  iter = {0};
          SListNode *pNode = NULL;
          tdListInitIter(&pContext->dropTableReqs, &iter, TD_LIST_FORWARD);
          while ((pNode = tdListNext(&iter)) != NULL) {
            SSTriggerDropRequest *pDropReq = *(SSTriggerDropRequest **)pNode->data;
            if (pDropReq->gid == pRequest->gid) {
              SStreamGroupInfo groupInfo = {.gInfo = pDropReq->groupColVals};
              code = tDeserializeSStreamGroupInfo(pRsp->pCont, pRsp->contLen, &groupInfo);
              QUERY_CHECK_CODE(code, lino, _end);
              code = stRealtimeContextRetryDropRequest(pContext, pNode, pDropReq);
              QUERY_CHECK_CODE(code, lino, _end);
              code = stTriggerTaskReleaseDropTableRequest(pTask, &pDropReq);
              QUERY_CHECK_CODE(code, lino, _end);
              break;
            }
          }
          if (listNEles(&pContext->dropTableReqs) == 0) {
            code = stRealtimeContextCheck(pContext);
            QUERY_CHECK_CODE(code, lino, _end);
          }
          break;
        }
        default:
          break;
      }
      break;
    }

    case STRIGGER_PULL_VTABLE_PSEUDO_COL: {
      QUERY_CHECK_CONDITION(
          pContext->status == STRIGGER_CONTEXT_SEND_CALC_REQ || pContext->status == STRIGGER_CONTEXT_CHECK_CONDITION,
          code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      QUERY_CHECK_CONDITION(pRsp->contLen > 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
      SSTriggerNewVtableMerger *pMerger =
          (pContext->status == STRIGGER_CONTEXT_CHECK_CONDITION) ? pContext->pMerger : pContext->pCalcMerger;
      const char *pCont = pRsp->pCont;
      code = blockDecode(pMerger->pPseudoColValues, pCont, &pCont);
      QUERY_CHECK_CODE(code, lino, _end);
      QUERY_CHECK_CONDITION(pCont == (char *)pRsp->pCont + pRsp->contLen, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
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
          VTableInfo             *pInfo = TARRAY_GET_ELEM(vtableInfo.infos, i);
          SSTriggerVirtTableInfo *pTable = tSimpleHashGet(pTask->pVirtTableInfos, &pInfo->uid, sizeof(int64_t));
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
        VTableInfo            *pInfo = TARRAY_GET_ELEM(vtableInfo.infos, i);
        SSTriggerVirtTableInfo newInfo = {
            .tbGid = pInfo->gId, .tbUid = pInfo->uid, .tbVer = pInfo->cols.version, .vgId = vgId};
        ST_TASK_DLOG("got virtual table info, gid:%" PRId64 ", uid:%" PRId64 ", ver:%d", pInfo->gId, pInfo->uid,
                     pInfo->cols.version);
        code = tSimpleHashPut(pTask->pVirtTableInfos, &newInfo.tbUid, sizeof(int64_t), &newInfo,
                              sizeof(SSTriggerVirtTableInfo));
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
            tSimpleHashSetFreeFp(pDbInfo, stTriggerTaskDestroyOrigColumnInfo);
            code = tSimpleHashPut(pTask->pOrigTableCols, pColRef->refDbName, dbNameLen, &pDbInfo, POINTER_BYTES);
            if (code != TSDB_CODE_SUCCESS) {
              tSimpleHashCleanup(pDbInfo);
              QUERY_CHECK_CODE(code, lino, _end);
            }
          } else {
            pDbInfo = *(SSHashObj **)px;
          }
          SSTriggerOrigColumnInfo *pTbInfo = tSimpleHashGet(pDbInfo, pColRef->refTableName, tbNameLen);
          if (pTbInfo == NULL) {
            SSTriggerOrigColumnInfo newInfo = {0};
            code = tSimpleHashPut(pDbInfo, pColRef->refTableName, tbNameLen, &newInfo, sizeof(SSTriggerOrigColumnInfo));
            QUERY_CHECK_CODE(code, lino, _end);
            pTbInfo = tSimpleHashGet(pDbInfo, pColRef->refTableName, tbNameLen);
            QUERY_CHECK_NULL(pTbInfo, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
          }
          if (pTbInfo->pColumns == NULL) {
            pTbInfo->pColumns = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
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
        char                    *dbName = tSimpleHashGetKey(px, NULL);
        SSHashObj               *pDbInfo = *(SSHashObj **)px;
        int32_t                  iter2 = 0;
        SSTriggerOrigColumnInfo *pTbInfo = tSimpleHashIterate(pDbInfo, NULL, &iter2);
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
      pReq->cont.pReqs = pOrigTableNames;
      pOrigTableNames = NULL;

      // wait to be exeucted again
      pContext->status = STRIGGER_CONTEXT_IDLE;
      atomic_store_ptr(&pTask->task.pMgmtReq, pReq);
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

      int32_t iter1 = 0;
      void   *px = tSimpleHashIterate(pTask->pOrigTableCols, NULL, &iter1);
      while (px != NULL) {
        SSHashObj               *pDbInfo = *(SSHashObj **)px;
        int32_t                  iter2 = 0;
        SSTriggerOrigColumnInfo *pTbInfo = tSimpleHashIterate(pDbInfo, NULL, &iter2);
        while (pTbInfo != NULL) {
          if (pTbInfo->vgId == pProgress->pTaskAddr->nodeId) {
            int32_t iter3 = 0;
            void   *px2 = tSimpleHashIterate(pTbInfo->pColumns, NULL, &iter3);
            while (px2 != NULL) {
              pTbInfo->suid = pRsp->suid;
              pTbInfo->uid = pRsp->uid;
              *(col_id_t *)px2 = pRsp->cid;
              pRsp++;
              px2 = tSimpleHashIterate(pTbInfo->pColumns, px2, &iter3);
            }
          }
          pTbInfo = tSimpleHashIterate(pDbInfo, pTbInfo, &iter2);
        }
        px = tSimpleHashIterate(pTask->pOrigTableCols, px, &iter1);
      }

      if (--pContext->curReaderIdx > 0) {
        // wait for responses from other readers
        goto _end;
      }

      int32_t nVirTables = taosArrayGetSize(pTask->pVirTableInfoRsp);
      for (int32_t i = 0; i < nVirTables; i++) {
        VTableInfo             *pInfo = TARRAY_GET_ELEM(pTask->pVirTableInfoRsp, i);
        SSTriggerVirtTableInfo *pNewInfo = tSimpleHashGet(pTask->pVirtTableInfos, &pInfo->uid, sizeof(int64_t));
        QUERY_CHECK_NULL(pNewInfo, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        if (pNewInfo->pTrigColRefs == NULL) {
          pNewInfo->pTrigColRefs = taosArrayInit(0, sizeof(SSTriggerTableColRef));
          QUERY_CHECK_NULL(pNewInfo->pTrigColRefs, code, lino, _end, terrno);
        }
        if (pNewInfo->pCalcColRefs == NULL) {
          pNewInfo->pCalcColRefs = taosArrayInit(0, sizeof(SSTriggerTableColRef));
          QUERY_CHECK_NULL(pNewInfo->pCalcColRefs, code, lino, _end, terrno);
        }
        code = stTriggerTaskGenVirColRefs(pTask, pInfo, pTask->pVirTrigSlots, pNewInfo->pTrigColRefs);
        QUERY_CHECK_CODE(code, lino, _end);
        code = stTriggerTaskGenVirColRefs(pTask, pInfo, pTask->pVirCalcSlots, pNewInfo->pCalcColRefs);
        QUERY_CHECK_CODE(code, lino, _end);
        code = stTriggerTaskNewGenVirColRefs(pTask, pInfo, true, pNewInfo->pTrigColRefs);
        QUERY_CHECK_CODE(code, lino, _end);
        code = stTriggerTaskNewGenVirColRefs(pTask, pInfo, false, pNewInfo->pCalcColRefs);
        QUERY_CHECK_CODE(code, lino, _end);
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
    } else if (!pTask->lowLatencyCalc && pContext->pMaxDelayHeap->min != NULL &&
               pContext->status == STRIGGER_CONTEXT_IDLE) {
      // continue check if there are delayed requests to be processed
      SSTriggerRealtimeGroup *pMinGroup = container_of(pContext->pMaxDelayHeap->min, SSTriggerRealtimeGroup, heapNode);
      int64_t                 now = taosGetTimestampNs();
      if (pMinGroup->nextExecTime <= now) {
        ST_TASK_DLOG("wake trigger for available runner since next exec time: %" PRId64 ", now: %" PRId64,
                     pMinGroup->nextExecTime, now);
        SListIter  iter = {0};
        SListNode *pNode = NULL;
        taosWLockLatch(&gStreamTriggerWaitLatch);
        tdListInitIter(&gStreamTriggerWaitList, &iter, TD_LIST_FORWARD);
        while ((pNode = tdListNext(&iter)) != NULL) {
          StreamTriggerWaitInfo *pInfo = (StreamTriggerWaitInfo *)pNode->data;
          if (pInfo->streamId == pTask->task.streamId && pInfo->taskId == pTask->task.taskId &&
              pInfo->sessionId == pContext->sessionId) {
            TD_DLIST_POP(&gStreamTriggerWaitList, pNode);
            break;
          }
        }
        taosWUnLockLatch(&gStreamTriggerWaitLatch);
        if (pNode != NULL) {
          taosMemoryFreeClear(pNode);
          code = stRealtimeContextCheck(pContext);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
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
    pContext->needTsdbMeta = (pTask->histTriggerFilter != NULL) || pTask->hasTriggerFilter ||
                             (pTask->placeHolderBitmap & PLACE_HOLDER_PARTITION_ROWS) ||
                             (pTask->placeHolderBitmap & PLACE_HOLDER_WROWNUM) || pTask->ignoreNoDataTrigger;
  } else if (pTask->isVirtualTable || (pTask->triggerType == STREAM_TRIGGER_SESSION) ||
             (pTask->triggerType == STREAM_TRIGGER_COUNT)) {
    pContext->needTsdbMeta = true;
  }

  pContext->pReaderTsdbProgress = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
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
    code = filterInitFromNode(pTask->histTriggerFilter, &pVirDataFilter, 0, NULL);
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
    code = filterInitFromNode(pTask->histStartCond, &pContext->pStartCond, 0, NULL);
    QUERY_CHECK_CODE(code, lino, _end);
    code = filterInitFromNode(pTask->histEndCond, &pContext->pEndCond, 0, NULL);
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
      pReq->gid = pTask->isVirtualTable ? 0 : pContext->gid;
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
      pReq->gid = pTask->isVirtualTable ? 0 : pContext->gid;
      pReq->order = 1;
      pReq->ver = pProgress->version;
      ST_TASK_DLOG("pull tsdb meta from vgId:%d, gid:%" PRId64 ", time range:[%" PRId64 ", %" PRId64 "]",
                   pReader->nodeId, pReq->gid, pReq->startTime, pReq->endTime);
      break;
    }

    case STRIGGER_PULL_TSDB_TS_DATA: {
      SSTriggerTableMeta *pCurTableMeta = pContext->pCurTableMeta;
      SSTriggerMetaData  *pMetaToFetch = pContext->pMetaToFetch;
      pProgress = tSimpleHashGet(pContext->pReaderTsdbProgress, &pCurTableMeta->vgId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SSTriggerTsdbTsDataRequest *pReq = &pProgress->pullReq.tsdbTsDataReq;
      pReq->suid = 0;
      if (pTask->isVirtualTable) {
        SSTriggerOrigTableInfo *pInfo = tSimpleHashGet(pTask->pOrigTableInfos, &pCurTableMeta->tbUid, sizeof(int64_t));
        QUERY_CHECK_NULL(pInfo, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        pReq->suid = pInfo->tbSuid;
      }
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
      if (stDebugFlag & DEBUG_DEBUG) {
        char    buf[128];
        int32_t bufLen = 0;
        buf[0] = '\0';
        for (int32_t i = 0; i < TARRAY_SIZE(pReq->cids); i++) {
          col_id_t colId = *(col_id_t *)TARRAY_GET_ELEM(pReq->cids, i);
          bufLen += tsnprintf(buf + bufLen, sizeof(buf) - bufLen, "%d,", colId);
        }
        if (bufLen > 0) {
          buf[bufLen - 1] = '\0';
        }
        ST_TASK_DLOG("pull data request for table:%" PRId64 " on node:%d, cids:%s", pReq->uid,
                     pProgress->pTaskAddr->nodeId, buf);
      }
      pReq->order = 1;
      pReq->ver = pProgress->version;
      break;
    }

    case STRIGGER_PULL_GROUP_COL_VALUE: {
      SSTriggerHistoryGroup *pGroup = stHistoryContextGetCurrentGroup(pContext);
      QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
      if (pTask->isVirtualTable) {
        SSTriggerVirtTableInfo *pTable = taosArrayGetP(pGroup->pVirtTableInfos, 0);
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
      SSTriggerVirtTableInfo *pTable = taosArrayGetP(pGroup->pVirtTableInfos, 0);
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
                                 pTask->histCalcTsIndex, &pContext->pCalcDataCache);
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
  for (int32_t i = 0; i < TARRAY_SIZE(pCalcReq->params); i++) {
    SSTriggerCalcParam *pParam = taosArrayGet(pCalcReq->params, i);
    if (pTask->triggerType == STREAM_TRIGGER_SLIDING) {
      if (pTask->interval.interval > 0) {
        pParam->wend++;
        pParam->wduration++;
      } else {
        pParam->prevTs--;
      }
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
      int64_t resumeTime = taosGetTimestampNs() + STREAM_TRIGGER_IDLE_TIME_NS;
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
      // history calculation finished since no data in scan range
      bool calcFinish = false;
      code = stHistoryContextAllCalcFinish(pContext, &calcFinish);
      QUERY_CHECK_CODE(code, lino, _end);
      QUERY_CHECK_CONDITION(calcFinish, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      if (pContext->isHistory) {
        atomic_store_8(&pTask->historyFinished, 1);
      }
      stHistoryContextDestroy(&pTask->pHistoryContext);
      pTask->pHistoryContext = taosMemoryCalloc(1, sizeof(SSTriggerHistoryContext));
      QUERY_CHECK_NULL(pTask->pHistoryContext, code, lino, _end, terrno);
      pContext = pTask->pHistoryContext;
      code = stHistoryContextInit(pContext, pTask);
      QUERY_CHECK_CODE(code, lino, _end);
      int64_t resumeTime = taosGetTimestampNs() + STREAM_TRIGGER_IDLE_TIME_NS;
      code = stTriggerTaskAddWaitSession(pTask, pContext->sessionId, resumeTime);
      QUERY_CHECK_CODE(code, lino, _end);
      goto _end;
    }

    pContext->status = STRIGGER_CONTEXT_FETCH_META;
    if (pContext->needTsdbMeta) {
      int64_t step = pTask->historyStep;
      pContext->stepRange.skey = pContext->scanRange.skey;
      pContext->stepRange.ekey = pContext->scanRange.skey / step * step + step - 1;
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
        if (pContext->gid == pGroup->gid || pContext->gid == 0) {
          TD_DLIST_APPEND(&pContext->groupsToCheck, pGroup);
        }
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
                                         pTask->pNotifyAddrUrls, pTask->addOptions,
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
            bool    needCalc = (nParams > STREAM_CALC_REQ_MAX_WIN_NUM);
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
    bool    needMoreCalc = (nRemainParams > STREAM_CALC_REQ_MAX_WIN_NUM);
    if (needMoreCalc) {
      // the group has remaining calc params to be calculated
      TD_DLIST_APPEND(&pContext->groupsToCheck, pGroup);
    }
    pContext->status = STRIGGER_CONTEXT_ACQUIRE_REQUEST;
  }

  bool forceClose = pContext->isHistory &&
                    (pTask->triggerType == STREAM_TRIGGER_SLIDING || pTask->triggerType == STREAM_TRIGGER_SESSION ||
                     pTask->triggerType == STREAM_TRIGGER_STATE);
  bool finished = true;
  if (TD_DLIST_NELES(&pContext->groupsForceClose) == 0) {
    if (pContext->needTsdbMeta) {
      // TODO(kjq): use precision of trigger table
      int64_t step = pTask->historyStep;
      QUERY_CHECK_CONDITION(pContext->stepRange.skey <= pContext->stepRange.ekey, code, lino, _end,
                            TSDB_CODE_INTERNAL_ERROR);
      finished = (pContext->stepRange.ekey + 1 > pContext->scanRange.ekey);
    } else if (pTask->triggerType != STREAM_TRIGGER_SLIDING) {
      for (int32_t i = 0; i < TARRAY_SIZE(pContext->pTrigDataBlocks); i++) {
        SSDataBlock *pDataBlock = *(SSDataBlock **)TARRAY_GET_ELEM(pContext->pTrigDataBlocks, i);
        if (blockDataGetNumOfRows(pDataBlock) > 0) {
          finished = false;
          break;
        }
      }
    }

    if (finished) {
      int32_t iter = 0;
      void   *px = tSimpleHashIterate(pContext->pGroups, NULL, &iter);
      while (px != NULL) {
        SSTriggerHistoryGroup *pGroup = *(SSTriggerHistoryGroup **)px;
        if (forceClose && IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
          TD_DLIST_APPEND(&pContext->groupsForceClose, pGroup);
        } else if (taosArrayGetSize(pGroup->pPendingCalcParams) > 0) {
          // have remaining calc params to be calculated
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
        if (forceClose && IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup)) {
          SSTriggerWindow *pHead = TRINGBUF_HEAD(&pGroup->winBuf);
          SSTriggerWindow *p = pHead;
          do {
            if (pTask->triggerType == STREAM_TRIGGER_STATE) {
              // for state trigger, check if state equals to the zeroth state
              bool stEqualZeroth = false;
              void *pStateData = IS_VAR_DATA_TYPE(pGroup->stateVal.type) ?
                    (void *)pGroup->stateVal.pData : (void *)&pGroup->stateVal.val;
              code = stIsStateEqualZeroth(pStateData, pTask->pStateZeroth, &stEqualZeroth);
              QUERY_CHECK_CODE(code, lino, _end);
              if (stEqualZeroth) {
                TRINGBUF_MOVE_NEXT(&pGroup->winBuf, p);
                continue;
              }
            }
            SSTriggerCalcParam param = {
                .triggerTime = now,
                .wstart = p->range.skey,
                .wend = p->range.ekey,
                .wduration = p->range.ekey - p->range.skey,
                .wrownum = (p == pHead) ? p->wrownum : (pHead->wrownum - p->wrownum),
            };
            if (pTask->calcEventType & STRIGGER_EVENT_WINDOW_CLOSE) {
              void *px = taosArrayPush(pGroup->pPendingCalcParams, &param);
              QUERY_CHECK_NULL(px, code, lino, _end, terrno);
            } else if (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_CLOSE) {
              void *px = taosArrayPush(pContext->pNotifyParams, &param);
              QUERY_CHECK_NULL(px, code, lino, _end, terrno);
            }
            TRINGBUF_MOVE_NEXT(&pGroup->winBuf, p);
          } while (p != TRINGBUF_TAIL(&pGroup->winBuf));
          TRINGBUF_DESTROY(&pGroup->winBuf);
          TRINGBUF_INIT(&pGroup->winBuf);
        }

        if (taosArrayGetSize(pContext->pNotifyParams) > 0) {
          code = streamSendNotifyContent(&pTask->task, pTask->streamName, NULL, pTask->triggerType, pGroup->gid,
                                         pTask->pNotifyAddrUrls, pTask->addOptions,
                                         TARRAY_DATA(pContext->pNotifyParams), TARRAY_SIZE(pContext->pNotifyParams));
          QUERY_CHECK_CODE(code, lino, _end);
        }
        stHistoryGroupClearTempState(pGroup);
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
            bool    needCalc = (nParams > 0);
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
      int64_t step = pTask->historyStep;
      pContext->stepRange.skey = pContext->stepRange.ekey + 1;
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
      if (pContext->isHistory) {
        atomic_store_8(&pTask->historyFinished, 1);
      }
      stHistoryContextDestroy(&pTask->pHistoryContext);
      pTask->pHistoryContext = taosMemoryCalloc(1, sizeof(SSTriggerHistoryContext));
      QUERY_CHECK_NULL(pTask->pHistoryContext, code, lino, _end, terrno);
      pContext = pTask->pHistoryContext;
      code = stHistoryContextInit(pContext, pTask);
      QUERY_CHECK_CODE(code, lino, _end);
      int64_t resumeTime = taosGetTimestampNs() + STREAM_TRIGGER_IDLE_TIME_NS;
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
      if (pRsp->code != TSDB_CODE_SUCCESS) {
        blockDataEmpty(pDataBlock);
      } else {
        code = tDeserializeSStreamTsResponse(pRsp->pCont, pRsp->contLen, pDataBlock);
        QUERY_CHECK_CODE(code, lino, _end);
      }

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
              if (inGroup && (pContext->gid == 0 || pContext->gid == pGroup->gid)) {
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
      int64_t globalMinTs = INT64_MAX;
      void   *px = tSimpleHashIterate(pContext->pFirstTsMap, NULL, &iter);
      while (px != NULL) {
        globalMinTs = TMIN(globalMinTs, *(int64_t *)px);
        px = tSimpleHashIterate(pContext->pFirstTsMap, px, &iter);
      }
      if (globalMinTs == INT64_MAX) {
        // no data in the whole scan range
        pContext->scanRange.ekey = pContext->scanRange.skey;
      } else {
        pContext->scanRange.skey = TMAX(pContext->scanRange.skey, globalMinTs);
      }
      ST_TASK_DLOG("update scan range to [%" PRId64 ", %" PRId64 "]", pContext->scanRange.skey,
                   pContext->scanRange.ekey);

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
            if (!IS_TRIGGER_GROUP_TO_CHECK(pGroup)) {
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
          if (pContext->gid == pGroup->gid || pContext->gid == 0) {
            bool added = false;
            code = stHistoryGroupAddMetaDatas(pGroup, pAllMetadatas, pVgIds, &added);
            QUERY_CHECK_CODE(code, lino, _end);
            if (added) {
              TD_DLIST_APPEND(&pContext->groupsToCheck, pGroup);
            }
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

      if (pRsp->contLen > 0) {
        const char *pCont = pRsp->pCont;
        int32_t     nBlocks = *(int32_t *)pCont;
        pCont += sizeof(int32_t);
        for (int32_t i = 0; i < nBlocks; i++) {
          pDataBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
          QUERY_CHECK_NULL(pDataBlock, code, lino, _end, terrno);
          code = blockDecode(pDataBlock, pCont, &pCont);
          QUERY_CHECK_CODE(code, lino, _end);
          void *px = taosArrayPush(pContext->pTrigDataBlocks, &pDataBlock);
          QUERY_CHECK_NULL(px, code, lino, _end, terrno);
          pDataBlock = NULL;
        }
        QUERY_CHECK_CONDITION(pCont == (char *)pRsp->pCont + pRsp->contLen, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      } else {
        pDataBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
        QUERY_CHECK_NULL(pDataBlock, code, lino, _end, terrno);
        void *px = taosArrayPush(pContext->pTrigDataBlocks, &pDataBlock);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
        pDataBlock = NULL;
      }

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
        if (!IS_TRIGGER_GROUP_TO_CHECK(pGroup)) {
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
        if (pContext->isHistory) {
          atomic_store_8(&pTask->historyFinished, 1);
        }
        stHistoryContextDestroy(&pTask->pHistoryContext);
        pTask->pHistoryContext = taosMemoryCalloc(1, sizeof(SSTriggerHistoryContext));
        QUERY_CHECK_NULL(pTask->pHistoryContext, code, lino, _end, terrno);
        pContext = pTask->pHistoryContext;
        code = stHistoryContextInit(pContext, pTask);
        QUERY_CHECK_CODE(code, lino, _end);
        int64_t resumeTime = taosGetTimestampNs() + STREAM_TRIGGER_IDLE_TIME_NS;
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

#define TRIGGER_GROUP_HAS_OPEN_WINDOW(pGroup)   ((pGroup)->windows.neles > 0)
#define TRIGGER_GROUP_NEVER_OPEN_WINDOW(pGroup) ((pGroup)->prevWinEnd == INT64_MIN)

static int32_t stRealtimeGroupInit(SSTriggerRealtimeGroup *pGroup, SSTriggerRealtimeContext *pContext, int64_t gid,
                                   int32_t vgId) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pContext->pTask;

  pGroup->pContext = pContext;
  pGroup->gid = gid;
  pGroup->recalcNextWindow = pTask->fillHistory && !pTask->ignoreDisorder;
  pGroup->vgId = vgId;

  pGroup->pWalMetas = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  QUERY_CHECK_NULL(pGroup->pWalMetas, code, lino, _end, terrno);
  tSimpleHashSetFreeFp(pGroup->pWalMetas, (FDelete)taosObjListClear);
  code = taosObjListInit(&pGroup->tableUids, &pContext->tableUidPool);
  QUERY_CHECK_CODE(code, lino, _end);
  pGroup->oldThreshold = INT64_MIN;
#if !TRIGGER_USE_HISTORY_META
  if (pTask->fillHistoryFirst && pTask->fillHistoryStartTime > 0) {
    pGroup->oldThreshold = pTask->fillHistoryStartTime - 1;
  }
#endif
  if (pTask->fillHistory || pTask->fillHistoryFirst) {
    void *px = tSimpleHashGet(pTask->pHistoryCutoffTime, &gid, sizeof(int64_t));
    if (px != NULL) {
      pGroup->oldThreshold = TMAX(pGroup->oldThreshold, *(int64_t *)px);
    }
  }
  pGroup->newThreshold = pGroup->oldThreshold;

  pGroup->pendingNullStart = INT64_MIN;
  pGroup->prevWindow = (STimeWindow){.skey = INT64_MIN, .ekey = INT64_MIN};
  code = taosObjListInit(&pGroup->windows, &pContext->windowPool);
  QUERY_CHECK_CODE(code, lino, _end);
  code = taosObjListInit(&pGroup->pPendingCalcParams, &pContext->calcParamPool);
  QUERY_CHECK_CODE(code, lino, _end);

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
  if (pGroup->pWalMetas != NULL) {
    tSimpleHashCleanup(pGroup->pWalMetas);
    pGroup->pWalMetas = NULL;
  }
  taosObjListClear(&pGroup->tableUids);

  if ((pGroup->pContext->pTask->triggerType == STREAM_TRIGGER_STATE) && IS_VAR_DATA_TYPE(pGroup->stateVal.type)) {
    taosMemoryFreeClear(pGroup->stateVal.pData);
  }
  taosObjListClear(&pGroup->windows);
  taosObjListClearEx(&pGroup->pPendingCalcParams, tDestroySSTriggerCalcParam);

  taosMemFreeClear(*ppGroup);
}

static void stRealtimeGroupClearTempState(SSTriggerRealtimeGroup *pGroup) {
  SSTriggerRealtimeContext *pContext = pGroup->pContext;

  pContext->needPseudoCols = false;
  pContext->needMergeWindow = false;
  pContext->needCheckAgain = false;
  if (pContext->pSorter != NULL) {
    stNewTimestampSorterReset(pContext->pSorter);
  }
  if (pContext->pMerger != NULL) {
    stNewVtableMergerReset(pContext->pMerger);
  }
  if (pContext->pWindows != NULL) {
    taosArrayClearEx(pContext->pWindows, stRealtimeContextDestroyWindow);
  }
  if (pContext->pNotifyParams != NULL) {
    taosArrayClearEx(pContext->pNotifyParams, tDestroySSTriggerCalcParam);
  }
  pContext->calcRange = (STimeWindow){.skey = INT64_MIN, .ekey = INT64_MIN};
  pContext->pCurParam = NULL;
  pContext->curParamRows = 0;
  pContext->lastSentWinEnd = INT64_MIN;
  taosObjListClear(&pContext->pAllCalcTableUids);
  taosObjListClear(&pContext->pCalcTableUids);
  if (pContext->pCalcSorter != NULL) {
    stNewTimestampSorterReset(pContext->pCalcSorter);
  }
  if (pContext->pCalcMerger != NULL) {
    stNewVtableMergerReset(pContext->pCalcMerger);
  }
}

static void stRealtimeGroupClearMetadatas(SSTriggerRealtimeGroup *pGroup) {
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;
  int64_t                   threshold = 0;

  // todo(kjq): update DoneVersion of each vnode for checkpoint

  if (pTask->placeHolderBitmap & PLACE_HOLDER_PARTITION_ROWS) {
    if (pGroup->pPendingCalcParams.neles > 0) {
      if (pTask->triggerType == STREAM_TRIGGER_PERIOD) {
        threshold = INT64_MIN;
      } else {
        SSTriggerCalcParam *pParam = taosObjListGetHead(&pGroup->pPendingCalcParams);
        threshold = TMAX(pParam->wstart - 1, pContext->lastSentWinEnd);
      }
    } else if (pGroup->windows.neles > 0) {
      SSTriggerWindow *pWin = taosObjListGetHead(&pGroup->windows);
      threshold = TMAX(pWin->range.skey - 1, pContext->lastSentWinEnd);
    } else {
      threshold = pGroup->newThreshold;
    }
  } else if (pTask->watermark > 0) {
    threshold = pGroup->newThreshold;
  } else {
    // clear all metas
    threshold = INT64_MAX;
  }

  int32_t   iter = 0;
  SObjList *pMetas = tSimpleHashIterate(pGroup->pWalMetas, NULL, &iter);
  while (pMetas != NULL) {
    if (threshold == INT64_MAX) {
      taosObjListClear(pMetas);
    } else if (threshold != INT64_MIN) {
      SSTriggerMetaData *pMeta = NULL;
      SObjListIter       iter = {0};
      taosObjListInitIter(pMetas, &iter, TOBJLIST_ITER_FORWARD);
      while ((pMeta = taosObjListIterNext(&iter)) != NULL) {
        if (pMeta->ekey <= threshold) {
          taosObjListPopObj(pMetas, pMeta);
        } else {
          pMeta->skey = TMAX(pMeta->skey, threshold + 1);
        }
      }
    }
    pMetas = tSimpleHashIterate(pGroup->pWalMetas, pMetas, &iter);
  }

  taosObjListClear(&pGroup->tableUids);
  pGroup->oldThreshold = pGroup->newThreshold;
}

static int32_t stRealtimeGroupAddMeta(SSTriggerRealtimeGroup *pGroup, int32_t vgId, SSTriggerMetaData *pMeta) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;
  SObjList                 *pMetas = NULL;

  if (!pTask->isVirtualTable) {
    pGroup->vgId = vgId;
  }
  pMetas = tSimpleHashGet(pGroup->pWalMetas, &vgId, sizeof(int32_t));
  if (pMetas == NULL) {
    SObjList newMetas = {0};
    code = taosObjListInit(&newMetas, &pContext->metaPool);
    QUERY_CHECK_CODE(code, lino, _end);
    code = tSimpleHashPut(pGroup->pWalMetas, &vgId, sizeof(int32_t), &newMetas, sizeof(SObjList));
    QUERY_CHECK_CODE(code, lino, _end);
    pMetas = tSimpleHashGet(pGroup->pWalMetas, &vgId, sizeof(int32_t));
    QUERY_CHECK_NULL(pMetas, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  }

  if (pTask->triggerType == STREAM_TRIGGER_PERIOD) {
    code = taosObjListAppend(pMetas, pMeta);
    QUERY_CHECK_CODE(code, lino, _end);
    pGroup->oldThreshold = INT64_MIN;
    pGroup->newThreshold = INT64_MAX;
    goto _end;
  }

  // check disorder data
  if (!pTask->ignoreDisorder) {
    if (pMeta->skey <= pGroup->oldThreshold) {
      STimeWindow range = {.skey = pMeta->skey, .ekey = pMeta->ekey};
      if (pTask->expiredTime > 0) {
        range.skey = TMAX(range.skey, pGroup->oldThreshold - pTask->expiredTime + 1);
      }
      range.ekey = TMIN(range.ekey, pGroup->oldThreshold);
      if (range.skey <= range.ekey) {
        ST_TASK_DLOG("add recalc request for disorder data, threshold: %" PRId64 ", start: %" PRId64 ", end: %" PRId64,
                     pGroup->oldThreshold, pMeta->skey, pMeta->ekey);
        code = stTriggerTaskAddRecalcRequest(pTask, pGroup, &range, pContext->pReaderWalProgress, false, true);
        QUERY_CHECK_CODE(code, lino, _end);
        code = stRealtimeGroupRemovePendingCalc(pGroup, &range);
        QUERY_CHECK_CODE(code, lino, _end);
        if (pGroup->windows.neles > 0) {
          SSTriggerWindow *pWin = taosObjListGetHead(&pGroup->windows);
          if (pWin->range.skey <= range.ekey) {
            ST_TASK_DLOG("need to recalc next window, groupId: %" PRId64 ", start: %" PRId64 ", end: %" PRId64,
                         pGroup->gid, pWin->range.skey, pWin->range.ekey);
            pGroup->recalcNextWindow = true;
          }
        }
      }
    }
  }

  // save meta and update threshold
  pMeta->skey = TMAX(pMeta->skey, pGroup->oldThreshold + 1);
  if (pMeta->skey <= pMeta->ekey) {
    code = taosObjListAppend(pMetas, pMeta);
    QUERY_CHECK_CODE(code, lino, _end);
    pGroup->newThreshold = TMAX(pGroup->newThreshold, pMeta->ekey - pTask->watermark);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}
static int32_t stRealtimeGroupNextDataBlock(SSTriggerRealtimeGroup *pGroup, SSDataBlock **ppDataBlock,
                                            int32_t *pStartIdx, int32_t *pEndIdx) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  *ppDataBlock = NULL;
  *pStartIdx = 0;
  *pEndIdx = 0;

  if (pContext->status == STRIGGER_CONTEXT_CHECK_CONDITION && !pTask->isVirtualTable) {
    while (pGroup->tableUids.neles > 0) {
      if (!pContext->pSorter->inUse) {
        int64_t            *ar = taosObjListGetHead(&pGroup->tableUids);
        int64_t             tbUid = ar[0];
        int32_t             vgId = ar[1];
        SSTriggerDataSlice *pSlice = tSimpleHashGet(pContext->pSlices, &tbUid, sizeof(int64_t));
        QUERY_CHECK_NULL(pSlice, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        SObjList *pMetas = tSimpleHashGet(pGroup->pWalMetas, &vgId, sizeof(int32_t));
        QUERY_CHECK_NULL(pMetas, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        STimeWindow range = {.skey = pGroup->oldThreshold + 1, .ekey = pGroup->newThreshold};
        code = stNewTimestampSorterSetData(pContext->pSorter, tbUid, pTask->trigTsIndex, &range, pMetas, pSlice);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      code = stNewTimestampSorterNextDataBlock(pContext->pSorter, ppDataBlock, pStartIdx, pEndIdx);
      QUERY_CHECK_CODE(code, lino, _end);
      if (*ppDataBlock != NULL && *pStartIdx < *pEndIdx) {
        break;
      }
      stNewTimestampSorterReset(pContext->pSorter);
      taosObjListPopHead(&pGroup->tableUids);
    }
  } else if (pContext->status == STRIGGER_CONTEXT_CHECK_CONDITION && pTask->isVirtualTable) {
    while (pGroup->tableUids.neles > 0) {
      if (!pContext->pMerger->inUse) {
        int64_t                *ar = taosObjListGetHead(&pGroup->tableUids);
        int64_t                 vtbUid = ar[0];
        SSTriggerVirtTableInfo *pInfo = tSimpleHashGet(pTask->pVirtTableInfos, &vtbUid, sizeof(int64_t));
        QUERY_CHECK_NULL(pInfo, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        if (stNewVtableMergerNeedPseudoCols(pContext->pMerger)) {
          pContext->needPseudoCols = true;
          break;
        }
        STimeWindow range = {.skey = pGroup->oldThreshold + 1, .ekey = pGroup->newThreshold};
        code = stNewVtableMergerSetData(pContext->pMerger, vtbUid, pTask->trigTsIndex, &range, &pGroup->tableUids,
                                        pInfo->pTrigColRefs, pGroup->pWalMetas, pContext->pSlices);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      code = stNewVtableMergerNextDataBlock(pContext->pMerger, ppDataBlock, pStartIdx, pEndIdx);
      QUERY_CHECK_CODE(code, lino, _end);
      if (*ppDataBlock != NULL && *pStartIdx < *pEndIdx) {
        break;
      }
      stNewVtableMergerReset(pContext->pMerger);
      int64_t     *ar = taosObjListGetHead(&pGroup->tableUids);
      int64_t      vtbUid = ar[0];
      SObjListIter iter = {0};
      taosObjListInitIter(&pGroup->tableUids, &iter, TOBJLIST_ITER_FORWARD);
      while ((ar = taosObjListIterNext(&iter)) != NULL) {
        if (ar[0] == vtbUid) {
          taosObjListPopObj(&pGroup->tableUids, ar);
        }
      }
    }
  } else if (pContext->status == STRIGGER_CONTEXT_SEND_CALC_REQ && !pTask->isVirtualTable) {
    while (pContext->pCalcTableUids.neles > 0) {
      if (!pContext->pCalcSorter->inUse) {
        int64_t            *ar = taosObjListGetHead(&pContext->pCalcTableUids);
        int64_t             tbUid = ar[0];
        int32_t             vgId = ar[1];
        SSTriggerDataSlice *pSlice = tSimpleHashGet(pContext->pSlices, &tbUid, sizeof(int64_t));
        QUERY_CHECK_NULL(pSlice, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        SObjList *pMetas = tSimpleHashGet(pGroup->pWalMetas, &vgId, sizeof(int32_t));
        QUERY_CHECK_NULL(pMetas, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        QUERY_CHECK_NULL(pContext->pCurParam, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        STimeWindow range = {.skey = pContext->pCurParam->wstart, .ekey = pContext->pCurParam->wend};
        if (TARRAY_DATA(pContext->pCalcReq->params) != pContext->pCurParam) {
          range.skey = TMAX(range.skey, (pContext->pCurParam - 1)->wend + 1);
        }
        code = stNewTimestampSorterSetData(pContext->pCalcSorter, tbUid, pTask->calcTsIndex, &range, pMetas, pSlice);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      code = stNewTimestampSorterNextDataBlock(pContext->pCalcSorter, ppDataBlock, pStartIdx, pEndIdx);
      QUERY_CHECK_CODE(code, lino, _end);
      if (*ppDataBlock != NULL && *pStartIdx < *pEndIdx) {
        break;
      }
      stNewTimestampSorterReset(pContext->pCalcSorter);
      taosObjListPopHead(&pContext->pCalcTableUids);
    }
  } else if (pContext->status == STRIGGER_CONTEXT_SEND_CALC_REQ && pTask->isVirtualTable) {
    while (pContext->pCalcTableUids.neles > 0) {
      if (!pContext->pCalcMerger->inUse) {
        int64_t                *ar = taosObjListGetHead(&pContext->pCalcTableUids);
        int64_t                 vtbUid = ar[0];
        SSTriggerVirtTableInfo *pInfo = tSimpleHashGet(pTask->pVirtTableInfos, &vtbUid, sizeof(int64_t));
        QUERY_CHECK_NULL(pInfo, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        if (stNewVtableMergerNeedPseudoCols(pContext->pCalcMerger)) {
          pContext->needPseudoCols = true;
          break;
        }
        QUERY_CHECK_NULL(pContext->pCurParam, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        STimeWindow range = {.skey = pContext->pCurParam->wstart, .ekey = pContext->pCurParam->wend};
        if (TARRAY_DATA(pContext->pCalcReq->params) != pContext->pCurParam) {
          range.skey = TMAX(range.skey, (pContext->pCurParam - 1)->wend + 1);
        }
        code = stNewVtableMergerSetData(pContext->pCalcMerger, vtbUid, pTask->calcTsIndex, &range,
                                        &pContext->pCalcTableUids, pInfo->pCalcColRefs, pGroup->pWalMetas,
                                        pContext->pSlices);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      code = stNewVtableMergerNextDataBlock(pContext->pCalcMerger, ppDataBlock, pStartIdx, pEndIdx);
      QUERY_CHECK_CODE(code, lino, _end);
      if (*ppDataBlock != NULL && *pStartIdx < *pEndIdx) {
        break;
      }
      stNewVtableMergerReset(pContext->pCalcMerger);
      int64_t     *ar = taosObjListGetHead(&pContext->pCalcTableUids);
      int64_t      vtbUid = ar[0];
      SObjListIter iter = {0};
      taosObjListInitIter(&pContext->pCalcTableUids, &iter, TOBJLIST_ITER_FORWARD);
      while ((ar = taosObjListIterNext(&iter)) != NULL) {
        if (ar[0] == vtbUid) {
          taosObjListPopObj(&pContext->pCalcTableUids, ar);
        }
      }
    }
  } else {
    ST_TASK_DLOG("invalid context status %d at %s", pContext->status, __func__);
    code = TSDB_CODE_INVALID_PARA;
    QUERY_CHECK_CODE(code, lino, _end);
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
  SSDataBlock              *pDataBlock = NULL;
  int32_t                   startIdx = 0;
  int32_t                   endIdx = 0;

  SSTriggerNotifyWindow newWin = {0};
  newWin.range.skey = INT64_MIN;
  newWin.range.ekey = INT64_MAX;
  newWin.wrownum = 1;
  void *px = taosArrayPush(pContext->pWindows, &newWin);
  QUERY_CHECK_NULL(px, code, lino, _end, terrno);

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
  SSDataBlock              *pDataBlock = NULL;
  int32_t                   startIdx = 0;
  int32_t                   endIdx = 0;

  if (pGroup->prevWindow.ekey == INT64_MIN) {
    int64_t   firstTs = INT64_MAX;
    int32_t   iter = 0;
    SObjList *pMetas = tSimpleHashIterate(pGroup->pWalMetas, NULL, &iter);
    while (pMetas != NULL) {
      SSTriggerMetaData *pMeta = NULL;
      SObjListIter       iter2 = {0};
      taosObjListInitIter(pMetas, &iter2, TOBJLIST_ITER_FORWARD);
      if (pTask->ignoreDisorder) {
        int64_t ts = INT64_MAX;
        while ((pMeta = taosObjListIterNext(&iter2)) != NULL) {
          if (ts == INT64_MAX) {
            ts = pMeta->skey;
          } else {
            int64_t skey = TMAX(ts - pTask->watermark, pMeta->skey);
            if (skey <= pMeta->ekey) {
              ts = TMIN(ts, skey);
            }
          }
        }
        firstTs = TMIN(firstTs, ts);
      } else {
        while ((pMeta = taosObjListIterNext(&iter2)) != NULL) {
          firstTs = TMIN(firstTs, pMeta->skey);
        }
      }
      pMetas = tSimpleHashIterate(pGroup->pWalMetas, pMetas, &iter);
    }
    QUERY_CHECK_CONDITION(firstTs != INT64_MAX, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    pGroup->prevWindow = stTriggerTaskGetTimeWindow(pTask, firstTs);
    stTriggerTaskPrevTimeWindow(pTask, &pGroup->prevWindow);
  }

  if (TARRAY_SIZE(pContext->pWindows) == 0) {
    SSTriggerNotifyWindow newWin = {.range = pGroup->prevWindow};
    SSTriggerWindow      *pWin = NULL;
    SObjListIter          iter = {0};
    taosObjListInitIter(&pGroup->windows, &iter, TOBJLIST_ITER_FORWARD);
    while ((pWin = taosObjListIterNext(&iter)) != NULL) {
      newWin.range = pWin->range;
      void *px = taosArrayPush(pContext->pWindows, &newWin);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
    }
    stTriggerTaskNextTimeWindow(pTask, &newWin.range);
    while (newWin.range.skey <= pGroup->newThreshold) {
      void *px = taosArrayPush(pContext->pWindows, &newWin);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      if (pContext->walMode == STRIGGER_WAL_META_ONLY &&
          TARRAY_SIZE(pContext->pWindows) >= STREAM_CALC_REQ_MAX_WIN_NUM) {
        pContext->needCheckAgain = true;
        goto _end;
      }
      stTriggerTaskNextTimeWindow(pTask, &newWin.range);
    }
  }

  while (true) {
    code = stRealtimeGroupNextDataBlock(pGroup, &pDataBlock, &startIdx, &endIdx);
    QUERY_CHECK_CODE(code, lino, _end);
    if (pContext->needPseudoCols || pDataBlock == NULL || startIdx >= endIdx) {
      break;
    }
    SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pTask->trigTsIndex);
    QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
    int64_t *pTsData = (int64_t *)pTsCol->pData;
    int32_t  l = startIdx, r = startIdx;
    for (int32_t i = 0; i < TARRAY_SIZE(pContext->pWindows); i++) {
      SSTriggerNotifyWindow *pWin = TARRAY_GET_ELEM(pContext->pWindows, i);
      while (l < endIdx && (pTsData[l] < pWin->range.skey)) {
        l++;
      }
      while (r < endIdx && (pTsData[r] <= pWin->range.ekey)) {
        r++;
      }
      pWin->wrownum += (r - l);
      if (l >= endIdx) {
        break;
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
  SSDataBlock              *pDataBlock = NULL;
  int32_t                   startIdx = 0;
  int32_t                   endIdx = 0;

  if (TARRAY_SIZE(pContext->pWindows) == 0) {
    SSTriggerNotifyWindow newWin = {0};
    SSTriggerWindow      *pWin = NULL;
    SObjListIter          iter = {0};
    taosObjListInitIter(&pGroup->windows, &iter, TOBJLIST_ITER_FORWARD);
    while ((pWin = taosObjListIterNext(&iter)) != NULL) {
      newWin.range = pWin->range;
      void *px = taosArrayPush(pContext->pWindows, &newWin);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
    }
  }

  SSTriggerNotifyWindow *pWin = taosArrayGetLast(pContext->pWindows);

  while (true) {
    code = stRealtimeGroupNextDataBlock(pGroup, &pDataBlock, &startIdx, &endIdx);
    QUERY_CHECK_CODE(code, lino, _end);
    if (pContext->needPseudoCols || pDataBlock == NULL || startIdx >= endIdx) {
      break;
    }
    SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pTask->trigTsIndex);
    QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
    int64_t *pTsData = (int64_t *)pTsCol->pData;
    if (pWin != NULL && pTsData[startIdx] < pWin->range.skey) {
      pContext->needMergeWindow = true;
      pWin = NULL;
    }
    for (int32_t i = startIdx; i < endIdx; i++) {
      if (pWin == NULL || pTsData[i] > pWin->range.ekey + pTask->gap) {
        SSTriggerNotifyWindow newWin = {0};
        newWin.range.skey = pTsData[i];
        newWin.range.ekey = pTsData[i];
        newWin.wrownum = 1;
        pWin = taosArrayPush(pContext->pWindows, &newWin);
        QUERY_CHECK_NULL(pWin, code, lino, _end, terrno);
      } else {
        pWin->wrownum++;
        pWin->range.ekey = TMAX(pWin->range.ekey, pTsData[i]);
      }
    }
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
  SSDataBlock              *pDataBlock = NULL;
  int32_t                   startIdx = 0;
  int32_t                   endIdx = 0;

  if (TARRAY_SIZE(pContext->pWindows) == 0) {
    SSTriggerNotifyWindow newWin = {0};
    SSTriggerWindow      *pWin = NULL;
    SObjListIter          iter = {0};
    taosObjListInitIter(&pGroup->windows, &iter, TOBJLIST_ITER_FORWARD);
    while ((pWin = taosObjListIterNext(&iter)) != NULL) {
      newWin.range = pWin->range;
      newWin.wrownum = pWin->wrownum;
      void *px = taosArrayPush(pContext->pWindows, &newWin);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
    }
  }

  SSTriggerNotifyWindow *pFirstWin = NULL;
  SSTriggerNotifyWindow *pLastWin = NULL;
  if (TARRAY_SIZE(pContext->pWindows) > 0) {
    pFirstWin = TARRAY_DATA(pContext->pWindows);
    pLastWin = pFirstWin + TARRAY_SIZE(pContext->pWindows) - 1;
  }

  while (true) {
    code = stRealtimeGroupNextDataBlock(pGroup, &pDataBlock, &startIdx, &endIdx);
    QUERY_CHECK_CODE(code, lino, _end);
    if (pContext->needPseudoCols || pDataBlock == NULL || startIdx >= endIdx) {
      break;
    }
    SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pTask->trigTsIndex);
    QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
    int64_t *pTsData = (int64_t *)pTsCol->pData;
    for (int32_t i = startIdx; i < endIdx; i++) {
      if (pGroup->totalCount % pTask->windowSliding == 0) {
        SSTriggerNotifyWindow newWin = {0};
        newWin.range.skey = pTsData[i];
        newWin.range.ekey = INT64_MAX;
        if (pFirstWin == NULL) {
          pLastWin = taosArrayPush(pContext->pWindows, &newWin);
          QUERY_CHECK_NULL(pLastWin, code, lino, _end, terrno);
          pFirstWin = pLastWin;
        } else {
          int32_t idx = TARRAY_ELEM_IDX(pContext->pWindows, pFirstWin);
          pLastWin = taosArrayPush(pContext->pWindows, &newWin);
          QUERY_CHECK_NULL(pLastWin, code, lino, _end, terrno);
          pFirstWin = TARRAY_GET_ELEM(pContext->pWindows, idx);
        }
      }
      pGroup->totalCount++;
      if (pFirstWin == NULL) {
        continue;
      }
      for (SSTriggerNotifyWindow *pWin = pFirstWin; pWin <= pLastWin; pWin++) {
        pWin->range.ekey = (pTsData[i] | TRIGGER_GROUP_UNCLOSED_WINDOW_MASK);
        pWin->wrownum++;
      }
      if (pFirstWin->wrownum == pTask->windowCount) {
        pFirstWin->range.ekey = pTsData[i];
        if (pFirstWin == pLastWin) {
          pFirstWin = NULL;
          pLastWin = NULL;
        } else {
          pFirstWin++;
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
  SSDataBlock              *pDataBlock = NULL;
  int32_t                   startIdx = 0;
  int32_t                   endIdx = 0;

  if (TARRAY_SIZE(pContext->pWindows) == 0) {
    SSTriggerNotifyWindow newWin = {0};
    SSTriggerWindow      *pWin = NULL;
    SObjListIter          iter = {0};
    taosObjListInitIter(&pGroup->windows, &iter, TOBJLIST_ITER_FORWARD);
    while ((pWin = taosObjListIterNext(&iter)) != NULL) {
      newWin.range = pWin->range;
      void *px = taosArrayPush(pContext->pWindows, &newWin);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
    }
  }

  SSTriggerNotifyWindow *pWin = taosArrayGetLast(pContext->pWindows);

  while (true) {
    code = stRealtimeGroupNextDataBlock(pGroup, &pDataBlock, &startIdx, &endIdx);
    QUERY_CHECK_CODE(code, lino, _end);
    if (pContext->needPseudoCols || pDataBlock == NULL || startIdx >= endIdx) {
      break;
    }
    SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pTask->trigTsIndex);
    QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
    int64_t         *pTsData = (int64_t *)pTsCol->pData;
    SColumnInfoData *pStateCol = NULL;
    if (pTask->stateSlotId != -1) {
      pStateCol = taosArrayGet(pDataBlock->pDataBlock, pTask->stateSlotId);
      QUERY_CHECK_NULL(pStateCol, code, lino, _end, terrno);
    } else if (pTask->isVirtualTable) {
      code = stRealtimeContextCalcExpr(pContext, pDataBlock, pTask->pStateExpr, &pContext->stateCol);
      QUERY_CHECK_CODE(code, lino, _end);
      pStateCol = &pContext->stateCol;
    } else {
      pStateCol = taosArrayGetLast(pDataBlock->pDataBlock);
      QUERY_CHECK_NULL(pStateCol, code, lino, _end, terrno);
    }
    bool  isVarType = IS_VAR_DATA_TYPE(pStateCol->info.type);
    void *pStateData = isVarType ? (void *)pGroup->stateVal.pData : (void *)&pGroup->stateVal.val;
    if (TARRAY_SIZE(pContext->pWindows) == 0 && pGroup->stateVal.type == 0) {
      // initialize state value
      SValue *pStateVal = &pGroup->stateVal;
      pStateVal->type = pStateCol->info.type;
      if (isVarType && pStateVal->pData == NULL) {
        pStateVal->nData = pStateCol->info.bytes;
        pStateVal->pData = taosMemoryCalloc(pStateVal->nData, 1);
        QUERY_CHECK_CONDITION(pStateVal->pData, code, lino, _end, terrno);
        pStateData = pStateVal->pData;
      }
    }
    for (int32_t i = startIdx; i < endIdx; i++) {
      bool isNull = colDataIsNull_s(pStateCol, i);
      if (isNull) {
        if (pGroup->numPendingNull == 0) {
          pGroup->pendingNullStart = pTsData[i];
        }
        pGroup->numPendingNull++;
      } else {
        char   *oldVal = (pWin != NULL) ? pStateData : NULL;
        char   *newVal = colDataGetData(pStateCol, i);
        int32_t bytes = isVarType ? varDataTLen(newVal) : pStateCol->info.bytes;
        int64_t startTs = pGroup->numPendingNull > 0 ? pGroup->pendingNullStart : pTsData[i];
        if (pWin != NULL) {
          if (memcmp(pStateData, newVal, bytes) == 0) {
            pWin->wrownum += pGroup->numPendingNull + 1;
          } else {
            // mark window as closed
            pWin->range.ekey = pWin->range.ekey & (~TRIGGER_GROUP_UNCLOSED_WINDOW_MASK);
            if (pTask->stateExtend == STATE_WIN_EXTEND_OPTION_BACKWARD) {
              pWin->wrownum += pGroup->numPendingNull;
              pWin->range.ekey = pTsData[i] - 1;
            } else if (pTask->stateExtend == STATE_WIN_EXTEND_OPTION_FORWARD) {
              startTs = pWin->range.ekey + 1;
            }
            bool stEqualZeroth = false;
            code = stIsStateEqualZeroth(pStateData, pTask->pStateZeroth, &stEqualZeroth);
            QUERY_CHECK_CODE(code, lino, _end);
            if (stEqualZeroth) {
              pWin = taosArrayPop(pContext->pWindows);
              stRealtimeContextDestroyWindow((void*)pWin);
            } else if (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_CLOSE) {
              code = streamBuildStateNotifyContent(STRIGGER_EVENT_WINDOW_CLOSE, &pStateCol->info, oldVal, newVal,
                                                   &pWin->pWinCloseNotify);
              QUERY_CHECK_CODE(code, lino, _end);
            }
            pWin = NULL;
          }
        }
        if (pWin == NULL) {
          SSTriggerNotifyWindow newWin = {0};
          newWin.range.skey = pTsData[i];
          newWin.range.ekey = INT64_MAX;
          newWin.wrownum = 1;
          if (pTask->stateExtend == STATE_WIN_EXTEND_OPTION_FORWARD) {
            newWin.range.skey = startTs;
            newWin.wrownum += pGroup->numPendingNull;
          }
          pWin = taosArrayPush(pContext->pWindows, &newWin);
          QUERY_CHECK_NULL(pWin, code, lino, _end, terrno);
          if (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_OPEN) {
            code = streamBuildStateNotifyContent(STRIGGER_EVENT_WINDOW_OPEN, &pStateCol->info, oldVal, newVal,
                                                 &pWin->pWinOpenNotify);
            QUERY_CHECK_CODE(code, lino, _end);
          }
          memcpy(pStateData, newVal, bytes);
        }
        pWin->range.ekey = (pTsData[i] | TRIGGER_GROUP_UNCLOSED_WINDOW_MASK);
        pGroup->numPendingNull = 0;
      }
    }
  }

_end:
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
  SSDataBlock              *pDataBlock = NULL;
  int32_t                   startIdx = 0;
  int32_t                   endIdx = 0;

  if (TARRAY_SIZE(pContext->pWindows) == 0) {
    SSTriggerNotifyWindow newWin = {0};
    SSTriggerWindow      *pWin = NULL;
    SObjListIter          iter = {0};
    taosObjListInitIter(&pGroup->windows, &iter, TOBJLIST_ITER_FORWARD);
    while ((pWin = taosObjListIterNext(&iter)) != NULL) {
      newWin.range = pWin->range;
      void *px = taosArrayPush(pContext->pWindows, &newWin);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
    }
  }

  SSTriggerNotifyWindow *pWin = taosArrayGetLast(pContext->pWindows);

  while (true) {
    code = stRealtimeGroupNextDataBlock(pGroup, &pDataBlock, &startIdx, &endIdx);
    QUERY_CHECK_CODE(code, lino, _end);
    if (pContext->needPseudoCols || pDataBlock == NULL || startIdx >= endIdx) {
      break;
    }
    SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pTask->trigTsIndex);
    QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
    int64_t         *pTsData = (int64_t *)pTsCol->pData;
    SColumnInfoData *psCol = NULL;
    SColumnInfoData *peCol = NULL;
    if (pTask->isVirtualTable) {
      code = stRealtimeContextCalcExpr(pContext, pDataBlock, pTask->pStartCond, &pContext->eventStartCol);
      QUERY_CHECK_CODE(code, lino, _end);
      code = stRealtimeContextCalcExpr(pContext, pDataBlock, pTask->pEndCond, &pContext->eventEndCol);
      QUERY_CHECK_CODE(code, lino, _end);
      psCol = &pContext->eventStartCol;
      peCol = &pContext->eventEndCol;
    } else {
      peCol = taosArrayGetLast(pDataBlock->pDataBlock);
      QUERY_CHECK_NULL(peCol, code, lino, _end, terrno);
      psCol = peCol - 1;
    }
    bool *ps = (bool *)psCol->pData;
    bool *pe = (bool *)peCol->pData;
    for (int32_t i = startIdx; i < endIdx; i++) {
      if ((pWin == NULL) && ps[i]) {
        SSTriggerNotifyWindow newWin = {0};
        newWin.range.skey = pTsData[i];
        newWin.range.ekey = INT64_MAX;
        pWin = taosArrayPush(pContext->pWindows, &newWin);
        QUERY_CHECK_NULL(pWin, code, lino, _end, terrno);
        if (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_OPEN) {
          code = streamBuildEventNotifyContent(pDataBlock, pTask->pStartCondCols, i, &pWin->pWinOpenNotify);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
      if (pWin != NULL) {
        pWin->wrownum++;
        if (pe[i]) {
          pWin->range.ekey = pTsData[i];
          if (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_CLOSE) {
            code = streamBuildEventNotifyContent(pDataBlock, pTask->pEndCondCols, i, &pWin->pWinCloseNotify);
            QUERY_CHECK_CODE(code, lino, _end);
          }
          pWin = NULL;
        }
      }
      if (pWin != NULL) {
        pWin->range.ekey = (pTsData[i] | TRIGGER_GROUP_UNCLOSED_WINDOW_MASK);
      }
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stRealtimeGroupCompareWindows(const void *pLeft, const void *pRight) {
  const SSTriggerNotifyWindow *pLeftWin = pLeft;
  const SSTriggerNotifyWindow *pRightWin = pRight;

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

static int32_t stRealtimeGroupMergeWindows(SSTriggerRealtimeGroup *pGroup) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;
  SSTriggerNotifyWindow    *pWin = NULL;
  SSTriggerWindow          *pTmpWin = NULL;
  SObjListIter              iter = {0};
  int64_t                   gap = 0;

  if (pTask->triggerType == STREAM_TRIGGER_SESSION) {
    gap = pTask->gap;
  }

  // merge windows if there are multiple tables in the group
  if (TARRAY_SIZE(pContext->pWindows) > 0 && pContext->needMergeWindow) {
    taosArraySort(pContext->pWindows, stRealtimeGroupCompareWindows);
    pWin = TARRAY_GET_ELEM(pContext->pWindows, 0);
    for (int32_t i = 1; i < TARRAY_SIZE(pContext->pWindows); i++) {
      SSTriggerNotifyWindow *pTmpWin = TARRAY_GET_ELEM(pContext->pWindows, i);
      if ((gap > 0 && pWin->range.ekey + gap >= pTmpWin->range.skey) ||
          (gap == 0 && pWin->range.skey == pTmpWin->range.skey)) {
        pWin->range.ekey = TMAX(pWin->range.ekey, pTmpWin->range.ekey);
        pWin->wrownum += pTmpWin->wrownum;
      } else {
        pWin++;
        *pWin = *pTmpWin;
      }
    }
    TARRAY_SIZE(pContext->pWindows) = TARRAY_ELEM_IDX(pContext->pWindows, pWin) + 1;
  }

  QUERY_CHECK_CONDITION(TARRAY_SIZE(pContext->pWindows) >= pGroup->windows.neles, code, lino, _end,
                        TSDB_CODE_INTERNAL_ERROR);

  if (TARRAY_SIZE(pContext->pWindows) == 0) {
    goto _end;
  }

  // sum up the rownum of existing windows
  if (pTask->triggerType != STREAM_TRIGGER_COUNT) {
    pWin = TARRAY_DATA(pContext->pWindows);
    taosObjListInitIter(&pGroup->windows, &iter, TOBJLIST_ITER_FORWARD);
    while ((pTmpWin = taosObjListIterNext(&iter)) != NULL) {
      QUERY_CHECK_CONDITION(pTmpWin->range.skey == pWin->range.skey, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pWin->wrownum += pTmpWin->wrownum;
      pWin++;
    }
  }

  // get number of unclosed windows
  int64_t numWin = TARRAY_SIZE(pContext->pWindows);
  int64_t numUnclosed = 0;
  while (numUnclosed < numWin) {
    pWin = TARRAY_GET_ELEM(pContext->pWindows, numWin - numUnclosed - 1);
    if (pWin->range.ekey + gap <= pGroup->newThreshold) {
      pGroup->prevWindow = pWin->range;
      break;
    }
    numUnclosed++;
  }
  int64_t numClosed = numWin - numUnclosed;
  pWin = TARRAY_GET_ELEM(pContext->pWindows, numClosed);

  // remove closed windows
  if (numClosed >= pGroup->windows.neles) {
    taosObjListClear(&pGroup->windows);
  } else if (numClosed > 0) {
    for (int32_t i = 0; i < numClosed; i++) {
      taosObjListPopHead(&pGroup->windows);
    }
  }

  // update rownum of unclosed windows
  if (pGroup->windows.neles > 0) {
    taosObjListInitIter(&pGroup->windows, &iter, TOBJLIST_ITER_FORWARD);
    while ((pTmpWin = taosObjListIterNext(&iter)) != NULL) {
      QUERY_CHECK_CONDITION(pTmpWin->range.skey == pWin->range.skey, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pTmpWin->range.ekey = pWin->range.ekey;
      pTmpWin->wrownum = pWin->wrownum;
      pWin++;
    }
  }

  // add new unclosed windows
  int64_t                now = taosGetTimestampNs();
  SSTriggerNotifyWindow *pEnd = TARRAY_GET_ELEM(pContext->pWindows, TARRAY_SIZE(pContext->pWindows));
  while (pWin < pEnd) {
    SSTriggerWindow win = {0};
    win.range = pWin->range;
    win.wrownum = pWin->wrownum;
    win.prevProcTime = now;
    code = taosObjListAppend(&pGroup->windows, &win);
    QUERY_CHECK_CODE(code, lino, _end);
    pWin++;
  }

  QUERY_CHECK_CONDITION(
      pGroup->windows.neles <= 1 ||
          (pTask->triggerType == STREAM_TRIGGER_SLIDING && pTask->interval.interval > pTask->interval.sliding) ||
          (pTask->triggerType == STREAM_TRIGGER_COUNT && pTask->windowCount > pTask->windowSliding),
      code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stRealtimeGroupFillParam(SSTriggerRealtimeGroup *pGroup, SSTriggerCalcParam *pParam,
                                        SSTriggerNotifyWindow *pWin) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  switch (pTask->triggerType) {
    case STREAM_TRIGGER_PERIOD: {
      pParam->wstart = pWin->range.skey;
      pParam->wend = pWin->range.ekey;
      if (pParam->notifyType != STRIGGER_EVENT_WINDOW_NONE) {
        pParam->notifyType = STRIGGER_EVENT_ON_TIME;
      }
      break;
    }
    case STREAM_TRIGGER_SLIDING: {
      if (pTask->interval.interval == 0) {
        // sliding trigger
        pParam->prevTs = pWin->range.skey;
        pParam->currentTs = pWin->range.ekey;
        if (pTask->placeHolderBitmap & PLACE_HOLDER_NEXT_TS) {
          STimeWindow nextWin = pWin->range;
          stTriggerTaskNextTimeWindow(pTask, &nextWin);
          pParam->nextTs = nextWin.ekey;
        }
        if (pParam->notifyType != STRIGGER_EVENT_WINDOW_NONE) {
          pParam->notifyType = STRIGGER_EVENT_ON_TIME;
        }
        break;
      }
      // fill the param the same way as other trigger types for interval window trigger
    }
    case STREAM_TRIGGER_SESSION:
    case STREAM_TRIGGER_COUNT:
    case STREAM_TRIGGER_STATE:
    case STREAM_TRIGGER_EVENT: {
      pParam->wstart = pWin->range.skey;
      pParam->wend = pWin->range.ekey;
      pParam->wduration = pParam->wend - pParam->wstart;
      pParam->wrownum = pWin->wrownum;
      break;
    }
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

static int32_t stRealtimeGroupGenCalcParams(SSTriggerRealtimeGroup *pGroup, int32_t nInitWins) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;
  int64_t                   now = taosGetTimestampNs();
  int64_t                   gap = 0;
  bool                      calcOpen = false;
  bool                      calcClose = false;
  bool                      notifyOpen = false;
  bool                      notifyClose = false;

  if (pTask->triggerType == STREAM_TRIGGER_SESSION) {
    gap = pTask->gap;
  }
  if (pTask->triggerType == STREAM_TRIGGER_PERIOD) {
    now = pContext->periodWindow.ekey;
  }

  calcOpen = (pTask->calcEventType & STRIGGER_EVENT_WINDOW_OPEN);
  calcClose = (pTask->calcEventType & STRIGGER_EVENT_WINDOW_CLOSE);
  notifyOpen = (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_OPEN);
  notifyClose = (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_CLOSE);

  // check whether calc params in this round should be pending
  int64_t numWin = TARRAY_SIZE(pContext->pWindows);
  int64_t numUnclosed = 0;
  while (numUnclosed < numWin) {
    SSTriggerNotifyWindow *pWin = TARRAY_GET_ELEM(pContext->pWindows, numWin - numUnclosed - 1);
    if (pWin->range.ekey + gap <= pGroup->newThreshold) {
      break;
    }
    numUnclosed++;
  }
  int64_t numClosed = numWin - numUnclosed;

  int64_t initPendingSize = pGroup->pPendingCalcParams.neles;
  // trigger all window open/close events
  for (int32_t i = 0; i < TARRAY_SIZE(pContext->pWindows); i++) {
    SSTriggerNotifyWindow *pWin = TARRAY_GET_ELEM(pContext->pWindows, i);
    // window open event may have been triggered previously
    if ((calcOpen || notifyOpen) && i >= nInitWins) {
      SSTriggerCalcParam    param = {.triggerTime = now,
                                     .notifyType = (notifyOpen ? STRIGGER_EVENT_WINDOW_OPEN : STRIGGER_EVENT_WINDOW_NONE),
                                     .extraNotifyContent = pWin->pWinOpenNotify};
      SSTriggerNotifyWindow win = *pWin;
      if (pTask->triggerType != STREAM_TRIGGER_SLIDING) {
        win.range.ekey = win.range.skey;
      }
      code = stRealtimeGroupFillParam(pGroup, &param, &win);
      QUERY_CHECK_CODE(code, lino, _end);
      if (calcOpen) {
        code = taosObjListAppend(&pGroup->pPendingCalcParams, &param);
        QUERY_CHECK_CODE(code, lino, _end);
        pWin->pWinOpenNotify = NULL;
      } else if (notifyOpen) {
        void *px = taosArrayPush(pContext->pNotifyParams, &param);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
        pWin->pWinOpenNotify = NULL;
      }
    }

    // check TRUE FOR condition and unclosed windows
    bool ignore =
        (i >= numClosed) || (pTask->ignoreNoDataTrigger && pWin->wrownum == 0) ||
        (pTask->triggerType == STREAM_TRIGGER_STATE && pWin->range.ekey - pWin->range.skey < pTask->stateTrueFor) ||
        (pTask->triggerType == STREAM_TRIGGER_EVENT && pWin->range.ekey - pWin->range.skey < pTask->eventTrueFor);

    if ((calcClose || notifyClose) && !ignore) {
      SSTriggerCalcParam param = {
          .triggerTime = now,
          .notifyType = (notifyClose ? STRIGGER_EVENT_WINDOW_CLOSE : STRIGGER_EVENT_WINDOW_NONE),
          .extraNotifyContent = pWin->pWinCloseNotify};
      code = stRealtimeGroupFillParam(pGroup, &param, pWin);
      QUERY_CHECK_CODE(code, lino, _end);
      if (calcClose) {
        // skip calc if it should be recalculated later
        if (pGroup->recalcNextWindow) {
          STimeWindow range = {.skey = param.wstart, .ekey = param.wend};
          ST_TASK_DLOG("add recalc request for next window, groupId: %" PRId64, pGroup->gid);
          code = stTriggerTaskAddRecalcRequest(pTask, pGroup, &range, pContext->pReaderWalProgress, false, true);
          QUERY_CHECK_CODE(code, lino, _end);
        } else {
          code = taosObjListAppend(&pGroup->pPendingCalcParams, &param);
          QUERY_CHECK_CODE(code, lino, _end);
          pWin->pWinCloseNotify = NULL;
        }
        pGroup->recalcNextWindow = false;
      } else if (notifyClose) {
        void *px = taosArrayPush(pContext->pNotifyParams, &param);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
        pWin->pWinCloseNotify = NULL;
      }
    }
  }

  // check next exec time for max delay and non-low-latency calc
  int64_t nextExecTime = INT64_MAX;
  if (pTask->maxDelayNs > 0 && pGroup->windows.neles > 0) {
    SSTriggerWindow *pTmpWin = NULL;
    SObjListIter     iter = {0};
    taosObjListInitIter(&pGroup->windows, &iter, TOBJLIST_ITER_FORWARD);
    while ((pTmpWin = taosObjListIterNext(&iter)) != NULL) {
      int64_t t = pTmpWin->prevProcTime + pTask->maxDelayNs;
      nextExecTime = TMIN(nextExecTime, t);
    }
  }
  if (initPendingSize == 0 && pGroup->pPendingCalcParams.neles > 0) {
    int64_t t = pTask->lowLatencyCalc ? now : (now + tsStreamBatchRequestWaitMs * NANOSECOND_PER_MSEC);
    nextExecTime = TMIN(nextExecTime, t);
  }

  if (nextExecTime != INT64_MAX && nextExecTime > pGroup->nextExecTime) {
    if (pGroup->nextExecTime != 0) {
      ST_TASK_DLOG("group %" PRId64 " update next exec time from %" PRId64 " to %" PRId64, pGroup->gid,
                   pGroup->nextExecTime, nextExecTime);
      heapRemove(pContext->pMaxDelayHeap, &pGroup->heapNode);
    }
    pGroup->nextExecTime = nextExecTime;
    heapInsert(pContext->pMaxDelayHeap, &pGroup->heapNode);
    ST_TASK_DLOG("group %" PRId64 " holds %" PRId64 " params, expecting to exec at %" PRId64, pGroup->gid,
                 pGroup->pPendingCalcParams.neles, pGroup->nextExecTime);
  }

  taosArrayClearEx(pContext->pWindows, stRealtimeContextDestroyWindow);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stRealtimeGroupCheck(SSTriggerRealtimeGroup *pGroup) {
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;

  if (pGroup->oldThreshold == pGroup->newThreshold) {
    goto _end;
  }

  switch (pTask->triggerType) {
    case STREAM_TRIGGER_PERIOD: {
      code = stRealtimeGroupDoPeriodCheck(pGroup);
      QUERY_CHECK_CODE(code, lino, _end);
      break;
    }

    case STREAM_TRIGGER_SLIDING: {
      code = stRealtimeGroupDoSlidingCheck(pGroup);
      QUERY_CHECK_CODE(code, lino, _end);
      break;
    }

    case STREAM_TRIGGER_SESSION: {
      code = stRealtimeGroupDoSessionCheck(pGroup);
      QUERY_CHECK_CODE(code, lino, _end);
      break;
    }

    case STREAM_TRIGGER_COUNT: {
      code = stRealtimeGroupDoCountCheck(pGroup);
      QUERY_CHECK_CODE(code, lino, _end);
      break;
    }

    case STREAM_TRIGGER_STATE: {
      code = stRealtimeGroupDoStateCheck(pGroup);
      QUERY_CHECK_CODE(code, lino, _end);
      break;
    }

    case STREAM_TRIGGER_EVENT: {
      code = stRealtimeGroupDoEventCheck(pGroup);
      QUERY_CHECK_CODE(code, lino, _end);
      break;
    }

    default: {
      ST_TASK_ELOG("invalid stream trigger type %d at %s:%d", pTask->triggerType, __func__, __LINE__);
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  if (!pContext->needPseudoCols) {
    int32_t nInitWins = pGroup->windows.neles;
    code = stRealtimeGroupMergeWindows(pGroup);
    QUERY_CHECK_CODE(code, lino, _end);
    code = stRealtimeGroupGenCalcParams(pGroup, nInitWins);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stRealtimeGroupRetrievePendingCalc(SSTriggerRealtimeGroup *pGroup) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;
  int64_t                   now = taosGetTimestampNs();
  int64_t                   nextExecTime = INT64_MAX;

  ST_TASK_DLOG("group %" PRId64 " starts to exec %" PRId64 " pending params", pGroup->gid,
               pGroup->pPendingCalcParams.neles);

  if (pGroup->pPendingCalcParams.neles > 0) {
    int64_t             origNele = pGroup->pPendingCalcParams.neles;
    int64_t             origTotal = pContext->calcParamPool.size;
    int32_t             nele = 0;
    SSTriggerCalcParam *pParam = NULL;
    SObjListIter        iter = {0};
    taosObjListInitIter(&pGroup->pPendingCalcParams, &iter, TOBJLIST_ITER_FORWARD);
    while ((pParam = taosObjListIterNext(&iter)) != NULL &&
           TARRAY_SIZE(pContext->pCalcReq->params) < STREAM_CALC_REQ_MAX_WIN_NUM) {
      void *px = taosArrayPush(pContext->pCalcReq->params, pParam);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      pParam->extraNotifyContent = NULL;
      nele++;
    }
    taosObjListPopHeadTo(&pGroup->pPendingCalcParams, pParam, nele);
    int64_t freedNele = origNele - pGroup->pPendingCalcParams.neles;
#define LOG_EVERY_NUM_PARAM 10000
    if (stDebugFlag & DEBUG_DEBUG) {
      ST_TASK_DLOG("group %" PRId64 " retrieved %" PRId64 " pending params, calc param pool size from %" PRId64
                   " to %" PRId64,
                   pGroup->gid, freedNele, origTotal, pContext->calcParamPool.size);
    } else if ((origTotal / LOG_EVERY_NUM_PARAM) != (pContext->calcParamPool.size / LOG_EVERY_NUM_PARAM)) {
      ST_TASK_ILOG("group %" PRId64 " retrieved %" PRId64 " pending params, calc param pool size from %" PRId64
                   " to %" PRId64,
                   pGroup->gid, freedNele, origTotal, pContext->calcParamPool.size);
    }
  }

  SSTriggerCalcParam *pLastParam = taosArrayGetLast(pContext->pCalcReq->params);
  pContext->lastSentWinEnd = pLastParam ? pLastParam->wend : INT64_MIN;

  if (pTask->maxDelayNs > 0 && pGroup->windows.neles > 0) {
    SSTriggerWindow *pWin = NULL;
    SObjListIter     iter = {0};
    taosObjListInitIter(&pGroup->windows, &iter, TOBJLIST_ITER_FORWARD);
    while ((pWin = taosObjListIterNext(&iter)) != NULL) {
      if (pWin->prevProcTime + pTask->maxDelayNs <= now &&
          TARRAY_SIZE(pContext->pCalcReq->params) < STREAM_CALC_REQ_MAX_WIN_NUM) {
        SSTriggerNotifyWindow win = {.range = pWin->range, .wrownum = pWin->wrownum};
        win.range.ekey &= (~TRIGGER_GROUP_UNCLOSED_WINDOW_MASK);
        SSTriggerCalcParam param = {.triggerTime = now};
        code = stRealtimeGroupFillParam(pGroup, &param, &win);
        QUERY_CHECK_CODE(code, lino, _end);
        void *px = taosArrayPush(pContext->pCalcReq->params, &param);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
        pWin->prevProcTime = now;
      }
      int64_t t = pWin->prevProcTime + pTask->maxDelayNs;
      nextExecTime = TMIN(nextExecTime, t);
    }
  }

  QUERY_CHECK_CONDITION(TARRAY_SIZE(pContext->pCalcReq->params) <= STREAM_CALC_REQ_MAX_WIN_NUM, code, lino, _end,
                        TSDB_CODE_INTERNAL_ERROR);

  if (pGroup->pPendingCalcParams.neles >= STREAM_CALC_REQ_MAX_WIN_NUM) {
    nextExecTime = TMIN(nextExecTime, now);
  } else if (pGroup->pPendingCalcParams.neles > 0) {
    int64_t t = pTask->lowLatencyCalc ? now : (now + tsStreamBatchRequestWaitMs * NANOSECOND_PER_MSEC);
    nextExecTime = TMIN(nextExecTime, t);
  }

  if (nextExecTime != INT64_MAX) {
    pGroup->nextExecTime = nextExecTime;
    ST_TASK_DLOG("group %" PRId64 " expects to exec again at %" PRId64, pGroup->gid, pGroup->nextExecTime);
  } else {
    pGroup->nextExecTime = 0;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stRealtimeGroupRemovePendingCalc(SSTriggerRealtimeGroup *pGroup, STimeWindow *pRange) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  if (pGroup->pPendingCalcParams.neles > 0) {
    ST_TASK_DLOG("remove pending calc params for group %" PRId64 " in range [%" PRId64 ", %" PRId64 "]", pGroup->gid,
                 pRange->skey, pRange->ekey);
    SSTriggerCalcParam *pParam = NULL;
    SObjListIter        iter = {0};
    taosObjListInitIter(&pGroup->pPendingCalcParams, &iter, TOBJLIST_ITER_FORWARD);
    while ((pParam = taosObjListIterNext(&iter)) != NULL) {
      if (pParam->wstart <= pRange->ekey && pParam->wend >= pRange->skey) {
        // remove this param
        taosMemoryFreeClear(pParam->extraNotifyContent);
        taosObjListPopObj(&pGroup->pPendingCalcParams, pParam);
      }
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void stHistoryGroupDestroyTableMeta(void *ptr) {
  SSTriggerTableMeta *pTableMeta = ptr;
  if (pTableMeta == NULL) {
    return;
  }
  if (pTableMeta->pMetas != NULL) {
    taosArrayDestroy(pTableMeta->pMetas);
    pTableMeta->pMetas = NULL;
  }
}

static int32_t stHistoryGroupMetaDataSearch(const void *pLeft, const void *pRight) {
  int64_t                  ts = *(const int64_t *)pLeft;
  const SSTriggerMetaData *pMeta = (const SSTriggerMetaData *)pRight;
  return ts - pMeta->ekey;
}

static int32_t stHistoryGroupInit(SSTriggerHistoryGroup *pGroup, SSTriggerHistoryContext *pContext, int64_t gid) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pContext->pTask;

  pGroup->pContext = pContext;
  pGroup->gid = gid;

  pGroup->pTableMetas = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  QUERY_CHECK_NULL(pGroup->pTableMetas, code, lino, _end, terrno);
  tSimpleHashSetFreeFp(pGroup->pTableMetas, stHistoryGroupDestroyTableMeta);

  TRINGBUF_INIT(&pGroup->winBuf);

  if (pContext->pTask->isVirtualTable) {
    pGroup->pVirtTableInfos = taosArrayInit(0, POINTER_BYTES);
    QUERY_CHECK_NULL(pGroup->pVirtTableInfos, code, lino, _end, terrno);
    int32_t                 iter = 0;
    SSTriggerVirtTableInfo *pInfo = tSimpleHashIterate(pTask->pVirtTableInfos, NULL, &iter);
    while (pInfo != NULL) {
      if (pInfo->tbGid == gid) {
        void *px = taosArrayPush(pGroup->pVirtTableInfos, &pInfo);
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
      pInfo = tSimpleHashIterate(pTask->pVirtTableInfos, pInfo, &iter);
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
  if (pGroup->pVirtTableInfos != NULL) {
    taosArrayDestroy(pGroup->pVirtTableInfos);
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
      int32_t idx = taosArraySearchIdx(pTableMeta->pMetas, &endTime, stHistoryGroupMetaDataSearch, TD_GT);
      taosArrayPopFrontBatch(pTableMeta->pMetas, (idx == -1) ? TARRAY_SIZE(pTableMeta->pMetas) : idx);
      idx = taosArraySearchIdx(pTableMeta->pMetas, &pContext->stepRange.ekey, stHistoryGroupMetaDataSearch, TD_GT);
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
          pGroup->nextWindow = stTriggerTaskGetTimeWindow(pTask, ts);
        }
        newWindow.range = pGroup->nextWindow;
        stTriggerTaskNextTimeWindow(pTask, &pGroup->nextWindow);
        if (needCalc || needNotify) {
          param.wstart = newWindow.range.skey;
          param.wend = newWindow.range.ekey;
          param.wduration = param.wend - param.wstart;
        }
        break;
      }
      // sliding trigger works the same as period trigger
    }
    case STREAM_TRIGGER_PERIOD: {
      QUERY_CHECK_CONDITION(!IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INVALID_PARA);
      if (IS_TRIGGER_GROUP_NONE_WINDOW(pGroup)) {
        pGroup->nextWindow = stTriggerTaskGetTimeWindow(pTask, ts);
      }
      newWindow.range = pGroup->nextWindow;
      stTriggerTaskNextTimeWindow(pTask, &pGroup->nextWindow);
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
        param.prevTs = pCurWindow->range.skey;
        param.currentTs = pCurWindow->range.ekey;
        param.nextTs = pGroup->nextWindow.ekey;
        break;
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

static int32_t stHistoryGroupWindowCompare(const void *pLeft, const void *pRight) {
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

static int32_t stHistoryGroupMergeSavedWindows(SSTriggerHistoryGroup *pGroup, int64_t gap) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SSTriggerHistoryContext *pContext = pGroup->pContext;
  SStreamTriggerTask      *pTask = pContext->pTask;

  QUERY_CHECK_CONDITION(!IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INVALID_PARA);

  if (taosArrayGetSize(pContext->pSavedWindows) == 0) {
    goto _end;
  }

  taosArraySort(pContext->pSavedWindows, stHistoryGroupWindowCompare);
  SSTriggerWindow *pWin = TARRAY_GET_ELEM(pContext->pSavedWindows, 0);
  for (int32_t i = 1; i < TARRAY_SIZE(pContext->pSavedWindows); i++) {
    SSTriggerWindow *pCurWin = TARRAY_GET_ELEM(pContext->pSavedWindows, i);
    if ((gap > 0 && pWin->range.ekey + gap >= pCurWin->range.skey) ||
        (gap == 0 && pWin->range.skey == pCurWin->range.skey)) {
      pWin->range.ekey = TMAX(pWin->range.ekey, pCurWin->range.ekey);
      pWin->wrownum += pCurWin->wrownum;
    } else {
      pWin++;
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
      if (pTask->triggerType == STREAM_TRIGGER_SLIDING && pTask->interval.interval == 0) {
        STimeWindow nextWindow = pWin->range;
        stTriggerTaskNextTimeWindow(pTask, &nextWindow);
        param.nextTs = nextWindow.ekey;
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
          STimeWindow nextWindow = pWin->range;
          stTriggerTaskNextTimeWindow(pTask, &nextWindow);
          param.nextTs = nextWindow.ekey;
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
      stTriggerTaskNextTimeWindow(pTask, &pGroup->nextWindow);
    } else {
      stTriggerTaskNextTimeWindow(pTask, &pGroup->nextWindow);
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
                                            isCalcData ? pTask->histCalcTsIndex : pTask->histTrigTsIndex);
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
        if (pContext->tbIter >= taosArrayGetSize(pGroup->pVirtTableInfos)) {
          *pAllTableProcessed = true;
          break;
        } else {
          pContext->pCurVirTable =
              *(SSTriggerVirtTableInfo **)TARRAY_GET_ELEM(pGroup->pVirtTableInfos, pContext->tbIter);
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

  if ((pTask->histTriggerFilter != NULL) || pTask->hasTriggerFilter) {
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
      SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pTask->histTrigTsIndex);
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

  if ((pTask->histTriggerFilter != NULL) || pTask->hasTriggerFilter) {
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
        code = stTimestampSorterSetSortInfo(pContext->pSorter, &range, pContext->pCurTableMeta->tbUid,
                                            pTask->histTrigTsIndex);
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
      SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pTask->histTrigTsIndex);
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

  if ((pTask->histTriggerFilter != NULL) || pTask->hasTriggerFilter) {
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
        code = stTimestampSorterSetSortInfo(pContext->pSorter, &range, pContext->pCurTableMeta->tbUid,
                                            pTask->histTrigTsIndex);
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
      if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup) && TRINGBUF_HEAD(&pGroup->winBuf)->wrownum == pTask->windowCount) {
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
      SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pTask->histTrigTsIndex);
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
        if (IS_TRIGGER_GROUP_OPEN_WINDOW(pGroup) && TRINGBUF_HEAD(&pGroup->winBuf)->wrownum == pTask->windowCount) {
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
  SArray                  *pList = NULL;
  SScalarParam             output = {0};

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
    SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pTask->histTrigTsIndex);
    QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
    int64_t         *pTsData = (int64_t *)pTsCol->pData;
    SColumnInfoData *pStateCol = NULL;
    if (pTask->histStateSlotId != -1) {
      pStateCol = taosArrayGet(pDataBlock->pDataBlock, pTask->histStateSlotId);
      QUERY_CHECK_NULL(pStateCol, code, lino, _end, terrno);
    } else {
      if (pList == NULL) {
        pList = taosArrayInit(1, POINTER_BYTES);
        QUERY_CHECK_NULL(pList, code, lino, _end, terrno);
      } else {
        taosArrayClear(pList);
      }
      void *px = taosArrayPush(pList, &pDataBlock);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);

      if (output.columnData == NULL) {
        SDataType       *pType = &((SExprNode *)pTask->pStateExpr)->resType;
        SColumnInfoData *pColumnData = taosMemoryCalloc(1, sizeof(SColumnInfoData));
        QUERY_CHECK_NULL(pColumnData, code, lino, _end, terrno);
        pColumnData->info.type = pType->type;
        pColumnData->info.bytes = pType->bytes;
        pColumnData->info.scale = pType->scale;
        pColumnData->info.precision = pType->precision;
        output.columnData = pColumnData;
        output.colAlloced = true;
      }
      SColumnInfoData *pColumnData = output.columnData;
      int32_t          numOfRows = blockDataGetNumOfRows(pDataBlock);
      code = colInfoDataEnsureCapacity(pColumnData, numOfRows, true);
      QUERY_CHECK_CODE(code, lino, _end);
      output.numOfRows = numOfRows;

      code = scalarCalculate(pTask->pStateExpr, pList, &output, NULL, NULL);
      QUERY_CHECK_CODE(code, lino, _end);
      pStateCol = output.columnData;
      QUERY_CHECK_NULL(pStateCol, code, lino, _end, terrno);
    }
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
        bool stEqualZeroth = false;
        code = stIsStateEqualZeroth(pStateData, pTask->pStateZeroth, &stEqualZeroth);
        QUERY_CHECK_CODE(code, lino, _end);
        if (stEqualZeroth) {
          TRINGBUF_DEQUEUE(&pGroup->winBuf);
        } else {   
          code = stHistoryGroupCloseWindow(pGroup, &pExtraNotifyContent, false);
          QUERY_CHECK_CODE(code, lino, _end);
          if (pTask->notifyHistory && (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_OPEN)) {
            code = streamBuildStateNotifyContent(STRIGGER_EVENT_WINDOW_OPEN, &pStateCol->info, pStateData, newVal,
                                                 &pExtraNotifyContent);
            QUERY_CHECK_CODE(code, lino, _end);
          }
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
  if (pList != NULL) {
    taosArrayDestroy(pList);
  }
  sclFreeParam(&output);
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
    SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pTask->histTrigTsIndex);
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

int32_t stIsStateEqualZeroth(void* pStateData, void* pZeroth, bool* pIsEqual) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (pStateData == NULL || pZeroth == NULL) {
    return code;
  }

  SValueNode* pZerothState = (SValueNode*)pZeroth;
  int8_t type = pZerothState->node.resType.type;
  if (IS_VAR_DATA_TYPE(type)) {
    *pIsEqual = compareLenPrefixedStr(pStateData, pZerothState->datum.p) == 0;
  } else if (IS_INTEGER_TYPE(type)) {
    *pIsEqual = memcmp(pStateData, &pZerothState->datum.i, pZerothState->node.resType.bytes) == 0;
  } else if (IS_BOOLEAN_TYPE(type)) {
    *pIsEqual = memcmp(pStateData, &pZerothState->datum.b, pZerothState->node.resType.bytes) == 0;
  } else {
    *pIsEqual = false;
    code = TSDB_CODE_INVALID_PARA;
  }

  if (code != TSDB_CODE_SUCCESS) {
    stError("%s failed since %s, zeroth state param type: %d", __func__, tstrerror(code), type);
  }
  return code;
}
