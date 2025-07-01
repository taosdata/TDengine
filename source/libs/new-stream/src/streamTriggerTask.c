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
#include "tcompare.h"
#include "tdatablock.h"
#include "ttime.h"

static int32_t stRealtimeContextCheck(SSTriggerRealtimeContext *pContext);

static TdThreadOnce     gStreamTriggerModuleInit = PTHREAD_ONCE_INIT;
static volatile int32_t gStreamTriggerInitRes = TSDB_CODE_SUCCESS;
static volatile bool    gStreamTriggerToStop = false;
// When the trigger task's real-time calculation catches up with the latest WAL
// progress, it will wait and be awakened later by a timer.
static SRWLatch gStreamTriggerWaitLatch;
static SList    gStreamTriggerWaitList;
static tmr_h    gStreamTriggerTimerId = NULL;

#define STREAM_TRIGGER_CHECK_INTERVAL_MS 1000                    // 1s
#define STREAM_TRIGGER_WAIT_TIME_NS      1 * NANOSECOND_PER_SEC  // 1s

typedef struct StreamTriggerWaitInfo {
  int64_t streamId;
  int64_t taskId;
  int64_t sessionId;
  int64_t resumeTime;
} StreamTriggerWaitInfo;

static int32_t streamTriggerAllocAhandle(SStreamTriggerTask *pTask, void *param, void **ppAhandle) {
  int32_t code = 0, lino = 0;

  *ppAhandle = taosMemoryCalloc(1, sizeof(SSTriggerAHandle));
  TSDB_CHECK_NULL(*ppAhandle, code, lino, _exit, terrno);

  SSTriggerAHandle *pRes = *ppAhandle;
  pRes->streamId = pTask->task.streamId;
  pRes->taskId = pTask->task.taskId;
  pRes->param = param;

_exit:

  if (code) {
    ST_TASK_ELOG("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t streamTriggerAddWaitContext(SSTriggerRealtimeContext *pContext, int64_t resumeTime) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pContext->pTask;

  StreamTriggerWaitInfo info = {
      .streamId = pTask->task.streamId,
      .taskId = pTask->task.taskId,
      .sessionId = pContext->sessionId,
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
  int64_t now = taosGetTimestampNs();
  SList   readylist = {0};
  taosWLockLatch(&gStreamTriggerWaitLatch);
  SListNode *pNode = TD_DLIST_HEAD(&gStreamTriggerWaitList);
  while (pNode != NULL) {
    SListNode *pCurNode = pNode;
    pNode = TD_DLIST_NODE_NEXT(pCurNode);
    StreamTriggerWaitInfo *pInfo = (StreamTriggerWaitInfo *)pCurNode->data;
    if (pInfo->resumeTime <= now) {
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
    SStreamTask           *task = NULL;
    void                  *taskAddr = NULL;
    int32_t                code = streamAcquireTask(pInfo->streamId, pInfo->taskId, &task, &taskAddr);
    if (code != TSDB_CODE_SUCCESS) {
      stError("failed to acquire stream trigger task %" PRIx64 "-%" PRIx64 "-%" PRIx64 " since %s", pInfo->streamId,
              pInfo->taskId, pInfo->sessionId, tstrerror(code));
      TD_DLIST_POP(&readylist, pCurNode);
      taosMemoryFreeClear(pCurNode);
      continue;
    }

    SStreamTriggerTask *pTask = (SStreamTriggerTask *)task;
    // resume the task
    SSTriggerRealtimeContext *pContext = pTask->pRealtimeContext;
    code = stRealtimeContextCheck(pContext);
    if (code != TSDB_CODE_SUCCESS) {
      stError("failed to resume stream trigger realtime context%" PRIx64 "-%" PRIx64 "-%" PRIx64 " since %s",
              pContext->pTask->task.streamId, pContext->pTask->task.taskId, pContext->sessionId, tstrerror(code));
      streamReleaseTask(taskAddr);
      continue;
    }

    stDebug("resume stream trigger realtime context %" PRIx64 "-%" PRIx64 "-%" PRIx64 " since now:%" PRId64
            ", resumeTime:%" PRId64,
            pContext->pTask->task.streamId, pContext->pTask->task.taskId, pContext->sessionId, now, pInfo->resumeTime);
    TD_DLIST_POP(&readylist, pCurNode);
    taosMemoryFreeClear(pCurNode);
    streamReleaseTask(taskAddr);
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
  taosWLockLatch(&gStreamTriggerWaitLatch);
  tdListEmpty(&gStreamTriggerWaitList);
  taosWUnLockLatch(&gStreamTriggerWaitLatch);
}

void streamTriggerEnvCleanup() {}

static STimeWindow stTriggerTaskGetIntervalWindow(const SInterval *pInterval, int64_t ts) {
  STimeWindow win;
  win.skey = taosTimeTruncate(ts, pInterval);
  win.ekey = taosTimeGetIntervalEnd(win.skey, pInterval);
  if (win.ekey < win.skey) {
    win.ekey = INT64_MAX;
  }
  return win;
}

static void stTriggerTaskPrevIntervalWindow(const SInterval *pInterval, STimeWindow *pWindow) {
  TSKEY prevStart =
      taosTimeAdd(pWindow->skey, -1 * pInterval->offset, pInterval->offsetUnit, pInterval->precision, NULL);
  prevStart = taosTimeAdd(prevStart, -1 * pInterval->sliding, pInterval->slidingUnit, pInterval->precision, NULL);
  prevStart = taosTimeAdd(prevStart, pInterval->offset, pInterval->offsetUnit, pInterval->precision, NULL);
  pWindow->skey = prevStart;
  pWindow->ekey = taosTimeGetIntervalEnd(prevStart, pInterval);
}

static void stTriggerTaskNextIntervalWindow(const SInterval *pInterval, STimeWindow *pWindow) {
  TSKEY nextStart =
      taosTimeAdd(pWindow->skey, -1 * pInterval->offset, pInterval->offsetUnit, pInterval->precision, NULL);
  nextStart = taosTimeAdd(nextStart, pInterval->sliding, pInterval->slidingUnit, pInterval->precision, NULL);
  nextStart = taosTimeAdd(nextStart, pInterval->offset, pInterval->offsetUnit, pInterval->precision, NULL);
  pWindow->skey = nextStart;
  pWindow->ekey = taosTimeGetIntervalEnd(nextStart, pInterval);
}

static STimeWindow stTriggerTaskGetPeriodWindow(const SInterval *pInterval, int64_t ts) {
  int64_t day = convertTimePrecision(24 * 60 * 60 * 1000, TSDB_TIME_PRECISION_MILLI, pInterval->precision);
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

static void stTriggerTaskNextPeriodWindow(const SInterval *pInterval, STimeWindow *pWindow) {
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
  stDebug("[checkpoint] gen checkpoint for task, ver %d, len:%" PRId64, ver, *pLen);

_end:
  tEncoderClear(&encoder);
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

typedef struct SRewriteSlotidCxt {
  int32_t errCode;
  SArray *newSlotIds;
} SRewriteSlotidCxt;

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

typedef struct SSTriggerOrigTableInfo {
  int32_t    vgId;
  int64_t    suid;
  int64_t    uid;
  SSHashObj *pColumns;  // SSHashObj<col_name, col_id>
} SSTriggerOrigTableInfo;

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

static int32_t stTriggerTaskGenVirColRefs(SStreamTriggerTask *pTask, VTableInfo *pInfo, SArray *pSlots,
                                          SArray **pColRefs) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t nCols = taosArrayGetSize(pSlots);

  for (int32_t j = 0; j < nCols; j++) {
    int32_t          slotId = *(int32_t *)TARRAY_GET_ELEM(pSlots, j);
    SColumnInfoData *pCol = TARRAY_GET_ELEM(pTask->pVirDataBlock->pDataBlock, slotId);
    col_id_t         colId = pCol->info.colId;
    for (int32_t k = 0; k < pInfo->cols.nCols; k++) {
      SColRef *pColRef = &pInfo->cols.pColRef[k];
      if (!pColRef->hasRef || pColRef->id != colId) {
        continue;
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
      if (*pColRefs == NULL) {
        *pColRefs = taosArrayInit(0, sizeof(SSTriggerTableColRef));
        QUERY_CHECK_NULL(*pColRefs, code, lino, _end, terrno);
      }
      SSTriggerTableColRef *pRef = NULL;
      for (int32_t l = 0; l < TARRAY_SIZE(*pColRefs); l++) {
        pRef = TARRAY_GET_ELEM(*pColRefs, l);
        if (pRef->otbSuid == pTbInfo->suid && pRef->otbUid == pTbInfo->uid) {
          break;
        }
        pRef = NULL;
      }
      if (pRef == NULL) {
        pRef = taosArrayReserve(*pColRefs, 1);
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

  if (pLeftMeta->ekey == pRightMeta->ekey) {
    return pLeftMeta->skey - pRightMeta->skey;
  }
  return pLeftMeta->ekey - pRightMeta->ekey;
}

static int32_t stRealtimeGroupMetaDataSearch(const void *pLeft, const void *pRight) {
  int64_t                  ts = *(const int64_t *)pLeft;
  const SSTriggerMetaData *pMeta = (const SSTriggerMetaData *)pRight;
  return ts - pMeta->ekey;
}

static int32_t stRealtimeGroupWindowCompare(const void *pLeft, const void *pRight) {
  const SSTriggerWindow *pLeftWin = (const SSTriggerWindow *)pLeft;
  const SSTriggerWindow *pRightWin = (const SSTriggerWindow *)pRight;

  if (pLeftWin->range.skey == pRightWin->range.skey) {
    return pLeftWin->range.ekey - pRightWin->range.ekey;
  }
  return pLeftWin->range.skey - pRightWin->range.skey;
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

_end:
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
  }
  if (pGroup->pTableMetas != NULL) {
    tSimpleHashCleanup(pGroup->pTableMetas);
    pGroup->pTableMetas = NULL;
  }

  TRINGBUF_DESTROY(&pGroup->winBuf);
  if ((pGroup->pContext->pTask->triggerType == STREAM_TRIGGER_STATE) && IS_VAR_DATA_TYPE(pGroup->stateVal.type)) {
    taosMemoryFreeClear(pGroup->stateVal.pData);
  }

  taosMemFreeClear(*ppGroup);
}

static void stRealtimeGroupClearTemp(SSTriggerRealtimeGroup *pGroup) {
  pGroup->tbIter = 0;
  pGroup->pCurVirTable = NULL;
  pGroup->pCurTableMeta = NULL;

  SSTriggerRealtimeContext *pContext = pGroup->pContext;
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
}

static int32_t stRealtimeGroupAddMetaDatas(SSTriggerRealtimeGroup *pGroup, SSDataBlock *pMetaDataBlock) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;
  SSTriggerTableMeta       *pTableMeta = NULL;
  SSHashObj                *pAddedUids = NULL;

  QUERY_CHECK_NULL(pMetaDataBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);

  if (pTask->triggerType == STREAM_TRIGGER_PERIOD) {
    pGroup->oldThreshold = INT64_MIN;
  }

  pGroup->newThreshold = pGroup->oldThreshold;
  pAddedUids = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  QUERY_CHECK_NULL(pAddedUids, code, lino, _end, terrno);

  int32_t          iCol = 0;
  SColumnInfoData *pTypeCol = taosArrayGet(pMetaDataBlock->pDataBlock, iCol++);
  QUERY_CHECK_NULL(pTypeCol, code, lino, _end, terrno);
  uint8_t *pTypes = (uint8_t *)pTypeCol->pData;
  int64_t *pGids = NULL;
  if (!pTask->isVirtualTable) {
    SColumnInfoData *pGidCol = taosArrayGet(pMetaDataBlock->pDataBlock, iCol++);
    QUERY_CHECK_NULL(pGidCol, code, lino, _end, terrno);
    pGids = (int64_t *)pGidCol->pData;
  }
  SColumnInfoData *pUidCol = taosArrayGet(pMetaDataBlock->pDataBlock, iCol++);
  QUERY_CHECK_NULL(pUidCol, code, lino, _end, terrno);
  int64_t         *pUids = (int64_t *)pUidCol->pData;
  SColumnInfoData *pSkeyCol = taosArrayGet(pMetaDataBlock->pDataBlock, iCol++);
  QUERY_CHECK_NULL(pSkeyCol, code, lino, _end, terrno);
  int64_t         *pSkeys = (int64_t *)pSkeyCol->pData;
  SColumnInfoData *pEkeyCol = taosArrayGet(pMetaDataBlock->pDataBlock, iCol++);
  QUERY_CHECK_NULL(pEkeyCol, code, lino, _end, terrno);
  int64_t         *pEkeys = (int64_t *)pEkeyCol->pData;
  SColumnInfoData *pVerCol = taosArrayGet(pMetaDataBlock->pDataBlock, iCol++);
  QUERY_CHECK_NULL(pVerCol, code, lino, _end, terrno);
  int64_t         *pVers = (int64_t *)pVerCol->pData;
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

    if (pSkeys[i] <= pGroup->oldThreshold && !pTask->ignoreDisorder) {
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
          SStreamTaskAddr *pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
          QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
          SSTriggerTableMeta newTableMeta = {.tbUid = pUids[i], .vgId = pReader->nodeId};
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
        SSTriggerMetaData *pNewMeta = taosArrayReserve(pTableMeta->pMetas, 1);
        QUERY_CHECK_NULL(pNewMeta, code, lino, _end, terrno);
        pNewMeta->skey = TMAX(pSkeys[i], pGroup->oldThreshold + 1);
        pNewMeta->ekey = pEkeys[i];
        pNewMeta->ver = pVers[i];
        pNewMeta->nrows = pNrows[i];
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
  int32_t maxNumHold = 0;
  pTableMeta = tSimpleHashIterate(pGroup->pTableMetas, NULL, &iter);
  while (pTableMeta != NULL) {
    maxNumHold = TMAX(maxNumHold, TARRAY_SIZE(pTableMeta->pMetas) - pTableMeta->metaIdx);
    pTableMeta = tSimpleHashIterate(pGroup->pTableMetas, pTableMeta, &iter);
  }
  if (maxNumHold < pContext->minMetaThreshold) {
    // not enough metadata to check, so no need to update the threshold
    goto _end;
  }
  int32_t numHoldThreshold = maxNumHold - pContext->maxMetaDelta;
  iter = 0;
  pTableMeta = tSimpleHashIterate(pGroup->pTableMetas, NULL, &iter);
  pGroup->newThreshold = INT64_MAX;
  while (pTableMeta != NULL) {
    if (TARRAY_SIZE(pTableMeta->pMetas) - pTableMeta->metaIdx >= numHoldThreshold) {
      SSTriggerMetaData *pMeta = taosArrayGetLast(pTableMeta->pMetas);
      pGroup->newThreshold = TMIN(pGroup->newThreshold, pMeta->ekey - pTask->watermark);
    }
    pTableMeta = tSimpleHashIterate(pGroup->pTableMetas, pTableMeta, &iter);
  }
  QUERY_CHECK_CONDITION(pGroup->newThreshold != INT64_MAX, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

_end:
  if (pAddedUids == NULL) {
    tSimpleHashCleanup(pAddedUids);
  }
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

#define IS_REALTIME_GROUP_NONE_WINDOW(pGroup) (TRINGBUF_CAPACITY(&(pGroup)->winBuf) == 0)
#define IS_REALTIME_GROUP_OPEN_WINDOW(pGroup) (TRINGBUF_SIZE(&(pGroup)->winBuf) > 0)

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
    param.notifyType = (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_OPEN);
    param.extraNotifyContent = ppExtraNotifyContent ? *ppExtraNotifyContent : NULL;
  }
  newWindow.prevProcTime = now;
  newWindow.wrownum = hasStartData ? 1 : 0;
  if (IS_REALTIME_GROUP_OPEN_WINDOW(pGroup)) {
    newWindow.wrownum = TRINGBUF_FIRST(&pGroup->winBuf).wrownum - newWindow.wrownum;
  }

  switch (pTask->triggerType) {
    case STREAM_TRIGGER_SLIDING: {
      SInterval *pInterval = &pTask->interval;
      if (pInterval->interval > 0) {
        // interval window trigger
        if (IS_REALTIME_GROUP_NONE_WINDOW(pGroup)) {
          pGroup->nextWindow = stTriggerTaskGetIntervalWindow(pInterval, ts);
        }
        newWindow.range = pGroup->nextWindow;
        stTriggerTaskNextIntervalWindow(pInterval, &pGroup->nextWindow);
        if (needCalc || needNotify) {
          STimeWindow prevWindow = newWindow.range;
          stTriggerTaskPrevIntervalWindow(&pTask->interval, &prevWindow);
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
      SInterval *pInterval = &pTask->interval;
      QUERY_CHECK_CONDITION(!IS_REALTIME_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INVALID_PARA);
      if (IS_REALTIME_GROUP_NONE_WINDOW(pGroup)) {
        pGroup->nextWindow = stTriggerTaskGetPeriodWindow(pInterval, ts);
      }
      newWindow.range = pGroup->nextWindow;
      stTriggerTaskNextPeriodWindow(pInterval, &pGroup->nextWindow);
      QUERY_CHECK_CONDITION(!needCalc && !needNotify, code, lino, _end, TSDB_CODE_INVALID_PARA);
      break;
    }
    case STREAM_TRIGGER_SESSION:
    case STREAM_TRIGGER_STATE:
    case STREAM_TRIGGER_EVENT: {
      QUERY_CHECK_CONDITION(!IS_REALTIME_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INVALID_PARA);
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
      ST_TASK_ELOG("invalid stream trigger type %d at %s", pTask->triggerType, __func__);
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

  if (IS_REALTIME_GROUP_NONE_WINDOW(pGroup)) {
    goto _end;
  }
  QUERY_CHECK_CONDITION(IS_REALTIME_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INVALID_PARA);

  if (pTask->calcEventType & STRIGGER_EVENT_WINDOW_CLOSE) {
    needCalc = true;
  }
  if (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_CLOSE) {
    needNotify = true;
  }
  if (needCalc || needNotify) {
    param.triggerTime = taosGetTimestampNs();
    param.notifyType = (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_CLOSE);
    param.extraNotifyContent = ppExtraNotifyContent ? *ppExtraNotifyContent : NULL;
  }

  pCurWindow = &TRINGBUF_FIRST(&pGroup->winBuf);

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
        stTriggerTaskPrevIntervalWindow(&pTask->interval, &prevWindow);
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
      ST_TASK_ELOG("invalid stream trigger type %d at %s", pTask->triggerType, __func__);
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  TRINGBUF_DEQUEUE(&pGroup->winBuf);
  if (IS_REALTIME_GROUP_OPEN_WINDOW(pGroup)) {
    // ajustify the following window's wrownum
    SSTriggerWindow *pFirst = &TRINGBUF_FIRST(&pGroup->winBuf);
    SSTriggerWindow *pLast = &TRINGBUF_LAST(&pGroup->winBuf);
    SSTriggerWindow *p = pFirst;
    int32_t          bias = pFirst->wrownum;
    while (true) {
      p->wrownum -= bias;
      if (p == pLast) {
        break;
      }
      TRINGBUF_MOVE_NEXT(&pGroup->winBuf, p);
    }
    pFirst->range.ekey = TMAX(pFirst->range.ekey, pCurWindow->range.ekey);
    pFirst->wrownum = pCurWindow->wrownum - bias;
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

static int32_t stRealtimeGroupSaveInitWindow(SSTriggerRealtimeGroup *pGroup, SArray *pInitWindows) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  QUERY_CHECK_NULL(pInitWindows, code, lino, _end, TSDB_CODE_INVALID_PARA);

  taosArrayClear(pInitWindows);
  if (IS_REALTIME_GROUP_OPEN_WINDOW(pGroup)) {
    SSTriggerWindow *pFirst = &TRINGBUF_FIRST(&pGroup->winBuf);
    SSTriggerWindow *pLast = &TRINGBUF_LAST(&pGroup->winBuf);
    SSTriggerWindow *p = pFirst;
    while (true) {
      void *px = taosArrayPush(pInitWindows, &p->range);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      if (p == pLast) {
        break;
      }
      TRINGBUF_MOVE_NEXT(&pGroup->winBuf, p);
    }
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

  QUERY_CHECK_CONDITION(!IS_REALTIME_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INVALID_PARA);

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

  QUERY_CHECK_CONDITION(!IS_REALTIME_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INVALID_PARA);

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
                                  .notifyType = (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_OPEN),
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
    if (pWin->range.ekey + gap > pGroup->newThreshold) {
      // todo(kjq): restore prevProcTime from saved init windows
      pWin->prevProcTime = taosGetTimestampNs();
      if (TRINGBUF_SIZE(&pGroup->winBuf) > 0) {
        pWin->wrownum = TRINGBUF_FIRST(&pGroup->winBuf).wrownum - pWin->wrownum;
      }
      code = TRINGBUF_APPEND(&pGroup->winBuf, *pWin);
      QUERY_CHECK_CODE(code, lino, _end);
    } else if ((calcClose || notifyClose)) {
      SSTriggerCalcParam param = {.triggerTime = taosGetTimestampNs(),
                                  .notifyType = (pTask->notifyEventType & STRIGGER_EVENT_WINDOW_CLOSE),
                                  .wstart = pWin->range.skey,
                                  .wend = pWin->range.ekey,
                                  .wduration = pWin->range.ekey - pWin->range.skey,
                                  .wrownum = pWin->wrownum};
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
    SInterval *pInterval = &pTask->interval;
    if (pInterval->interval > 0) {
      stTriggerTaskNextIntervalWindow(pInterval, &pGroup->nextWindow);
    } else {
      stTriggerTaskNextPeriodWindow(pInterval, &pGroup->nextWindow);
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
        while (saveWindow && IS_REALTIME_GROUP_OPEN_WINDOW(pGroup)) {
          code = stRealtimeGroupCloseWindow(pGroup, NULL, saveWindow);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        stTimestampSorterReset(pContext->pSorter);
        pGroup->pCurTableMeta = tSimpleHashIterate(pGroup->pTableMetas, pGroup->pCurTableMeta, &pGroup->tbIter);
        if (pGroup->pCurTableMeta == NULL) {
          *pAllTableProcessed = true;
          break;
        }
        if (saveWindow) {
          code = stRealtimeGroupRestoreInitWindow(pGroup, pContext->pInitWindows);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        STimeWindow range = {.skey = INT64_MIN, .ekey = INT64_MAX};
        if (pContext->status == STRIGGER_CONTEXT_CHECK_CONDITION) {
          range.skey = pGroup->oldThreshold + 1;
          range.ekey = pGroup->newThreshold;
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
        while (saveWindow && IS_REALTIME_GROUP_OPEN_WINDOW(pGroup)) {
          code = stRealtimeGroupCloseWindow(pGroup, NULL, saveWindow);
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
          code = stRealtimeGroupRestoreInitWindow(pGroup, pContext->pInitWindows);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        STimeWindow range = {.skey = INT64_MIN, .ekey = INT64_MAX};
        if (pContext->status == STRIGGER_CONTEXT_CHECK_CONDITION) {
          range.skey = pGroup->oldThreshold + 1;
          range.ekey = pGroup->newThreshold;
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

static int32_t stRealtimeGroupDoPeriodCheck(SSTriggerRealtimeGroup *pGroup) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  // enable to get all calc data when sending calc request
  pGroup->oldThreshold = INT64_MIN;
  pGroup->newThreshold = INT64_MAX;

  QUERY_CHECK_CONDITION(!IS_REALTIME_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INVALID_PARA);
  pGroup->nextWindow = pContext->periodWindow;
  code = stRealtimeGroupOpenWindow(pGroup, pContext->periodWindow.ekey, NULL, false, false);
  QUERY_CHECK_CODE(code, lino, _end);
  STimeWindow *pCurWin = &TRINGBUF_FIRST(&pGroup->winBuf).range;
  QUERY_CHECK_CONDITION(
      memcmp(&TRINGBUF_FIRST(&pGroup->winBuf).range, &pContext->periodWindow, sizeof(STimeWindow)) == 0, code, lino,
      _end, TSDB_CODE_INTERNAL_ERROR);
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

  if (IS_REALTIME_GROUP_NONE_WINDOW(pGroup)) {
    int64_t             ts = INT64_MAX;
    int32_t             iter = 0;
    SSTriggerTableMeta *pTableMeta = tSimpleHashIterate(pGroup->pTableMetas, NULL, &iter);
    while (pTableMeta != NULL) {
      for (int32_t i = 0; i < TARRAY_SIZE(pTableMeta->pMetas); i++) {
        SSTriggerMetaData *pMeta = TARRAY_GET_ELEM(pTableMeta->pMetas, i);
        ts = TMIN(ts, pMeta->skey);
      }
      pTableMeta = tSimpleHashIterate(pGroup->pTableMetas, pTableMeta, &iter);
    }
    QUERY_CHECK_CONDITION(ts != INT64_MAX, code, lino, _end, TSDB_CODE_INVALID_PARA);
    code = stRealtimeGroupOpenWindow(pGroup, ts, NULL, false, false);
    QUERY_CHECK_CODE(code, lino, _end);
    pGroup->oldThreshold = ts - 1;
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
      SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, 0);
      QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
      int64_t *pTsData = (int64_t *)pTsCol->pData;
      for (int32_t r = startIdx; r < endIdx;) {
        int64_t nextStart = pGroup->nextWindow.skey;
        int64_t curEnd = IS_REALTIME_GROUP_OPEN_WINDOW(pGroup) ? TRINGBUF_FIRST(&pGroup->winBuf).range.ekey : INT64_MAX;
        int64_t ts = TMIN(nextStart, curEnd);
        void   *px = taosbsearch(&ts, pTsData + r, endIdx - r, sizeof(int64_t), compareInt64Val, TD_GT);
        int32_t nrows = (px != NULL) ? (POINTER_DISTANCE(px, &pTsData[r]) / sizeof(int64_t)) : (endIdx - r);
        r += nrows;
        if (IS_REALTIME_GROUP_OPEN_WINDOW(pGroup)) {
          TRINGBUF_FIRST(&pGroup->winBuf).wrownum += nrows;
        }
        if (ts == nextStart) {
          code = stRealtimeGroupOpenWindow(pGroup, ts, NULL, true, r > 0 && pTsData[r - 1] == nextStart);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        QUERY_CHECK_CONDITION(IS_REALTIME_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        if (TRINGBUF_FIRST(&pGroup->winBuf).range.ekey == ts) {
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
      int64_t curEnd = IS_REALTIME_GROUP_OPEN_WINDOW(pGroup) ? TRINGBUF_FIRST(&pGroup->winBuf).range.ekey : INT64_MAX;
      int64_t ts = TMIN(nextStart, curEnd);
      if (ts > pGroup->newThreshold) {
        break;
      }
      if (ts == nextStart) {
        code = stRealtimeGroupOpenWindow(pGroup, ts, NULL, false, false);
        QUERY_CHECK_CODE(code, lino, _end);
        TRINGBUF_FIRST(&pGroup->winBuf).wrownum = 0;
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
        while (IS_REALTIME_GROUP_OPEN_WINDOW(pGroup)) {
          code = stRealtimeGroupCloseWindow(pGroup, NULL, true);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        stTimestampSorterReset(pContext->pSorter);
        pGroup->pCurTableMeta = tSimpleHashIterate(pGroup->pTableMetas, pGroup->pCurTableMeta, &pGroup->tbIter);
        if (pGroup->pCurTableMeta == NULL) {
          allTableProcessed = true;
          break;
        }
        code = stRealtimeGroupRestoreInitWindow(pGroup, pContext->pInitWindows);
        QUERY_CHECK_CODE(code, lino, _end);
        STimeWindow range = {.skey = pGroup->oldThreshold + 1, .ekey = pGroup->newThreshold};
        code =
            stTimestampSorterSetSortInfo(pContext->pSorter, &range, pGroup->pCurTableMeta->tbUid, pTask->trigTsIndex);
        QUERY_CHECK_CODE(code, lino, _end);
        code = stTimestampSorterSetMetaDatas(pContext->pSorter, pGroup->pCurTableMeta);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      int64_t ts = IS_REALTIME_GROUP_OPEN_WINDOW(pGroup) ? TRINGBUF_FIRST(&pGroup->winBuf).range.ekey : INT64_MIN;
      int64_t lastTs = ts, nextTs = ts;
      code = stTimestampSorterForwardTs(pContext->pSorter, ts, pTask->gap, &lastTs, &nextTs);
      QUERY_CHECK_CODE(code, lino, _end);
      if (IS_REALTIME_GROUP_OPEN_WINDOW(pGroup)) {
        TRINGBUF_FIRST(&pGroup->winBuf).range.ekey = lastTs;
      }
      if (nextTs == INT64_MAX) {
        if (!IS_TRIGGER_TIMESTAMP_SORTER_EMPTY(pContext->pSorter)) {
          needFetchData = true;
          code = stTimestampSorterGetMetaToFetch(pContext->pSorter, &pContext->pMetaToFetch);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        continue;
      }
      code = stRealtimeGroupCloseWindow(pGroup, NULL, true);
      QUERY_CHECK_CODE(code, lino, _end);
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
        SSTriggerWindow *pCurWin = &TRINGBUF_FIRST(&pGroup->winBuf);
        if (IS_REALTIME_GROUP_OPEN_WINDOW(pGroup) && pCurWin->range.ekey + pTask->gap >= ts) {
          pCurWin->range.ekey = ts;
          pCurWin->wrownum++;
        } else {
          code = stRealtimeGroupCloseWindow(pGroup, NULL, true);
          QUERY_CHECK_CODE(code, lino, _end);
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
        pGroup->pCurTableMeta = tSimpleHashIterate(pGroup->pTableMetas, pGroup->pCurTableMeta, &pGroup->tbIter);
        if (pGroup->pCurTableMeta == NULL) {
          // actually, it has only one table
          allTableProcessed = true;
          break;
        }
        STimeWindow range = {.skey = pGroup->oldThreshold + 1, .ekey = pGroup->newThreshold};
        code =
            stTimestampSorterSetSortInfo(pContext->pSorter, &range, pGroup->pCurTableMeta->tbUid, pTask->trigTsIndex);
        QUERY_CHECK_CODE(code, lino, _end);
        code = stTimestampSorterSetMetaDatas(pContext->pSorter, pGroup->pCurTableMeta);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      int64_t skipped = 0;
      int64_t lastTs = INT64_MIN;
      int64_t nrowsCurWin = IS_REALTIME_GROUP_OPEN_WINDOW(pGroup) ? TRINGBUF_FIRST(&pGroup->winBuf).wrownum : 0;
      int64_t nrowsNextWstart = ALIGN_UP(nrowsCurWin, pTask->windowSliding) + 1;
      int64_t nrowsToSkip = TMIN(nrowsNextWstart, pTask->windowCount) - nrowsCurWin;
      code = stTimestampSorterForwardNrows(pContext->pSorter, nrowsToSkip, &skipped, &lastTs);
      QUERY_CHECK_CODE(code, lino, _end);
      if (IS_REALTIME_GROUP_OPEN_WINDOW(pGroup) && skipped > 0) {
        TRINGBUF_FIRST(&pGroup->winBuf).range.ekey = lastTs;
        TRINGBUF_FIRST(&pGroup->winBuf).wrownum += skipped;
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
      QUERY_CHECK_CONDITION(IS_REALTIME_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      if (TRINGBUF_FIRST(&pGroup->winBuf).wrownum == pTask->windowCount) {
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
        int64_t nrowsCurWin = IS_REALTIME_GROUP_OPEN_WINDOW(pGroup) ? TRINGBUF_FIRST(&pGroup->winBuf).wrownum : 0;
        int64_t nrowsNextWstart = ALIGN_UP(nrowsCurWin, pTask->windowSliding) + 1;
        int64_t nrowsToSkip = TMIN(nrowsNextWstart, pTask->windowCount) - nrowsCurWin;
        int64_t skipped = TMIN(nrowsToSkip, endIdx - r);
        int64_t lastTs = pTsData[r + skipped - 1];
        r += skipped;
        if (IS_REALTIME_GROUP_OPEN_WINDOW(pGroup) && skipped > 0) {
          TRINGBUF_FIRST(&pGroup->winBuf).range.ekey = lastTs;
          TRINGBUF_FIRST(&pGroup->winBuf).wrownum += skipped;
        }
        if (skipped == nrowsNextWstart) {
          code = stRealtimeGroupOpenWindow(pGroup, lastTs, NULL, false, true);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        QUERY_CHECK_CONDITION(IS_REALTIME_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        if (TRINGBUF_FIRST(&pGroup->winBuf).wrownum == pTask->windowCount) {
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
    SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, 0);
    QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
    int64_t         *pTsData = (int64_t *)pTsCol->pData;
    SColumnInfoData *pStateCol = taosArrayGet(pDataBlock->pDataBlock, pTask->stateSlotId);
    QUERY_CHECK_NULL(pStateCol, code, lino, _end, terrno);
    bool  isVarType = IS_VAR_DATA_TYPE(pStateCol->info.type);
    void *pStateData = isVarType ? (void *)pGroup->stateVal.pData : (void *)&pGroup->stateVal.val;
    if (IS_REALTIME_GROUP_NONE_WINDOW(pGroup)) {
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
        TRINGBUF_FIRST(&pGroup->winBuf).wrownum++;
        TRINGBUF_FIRST(&pGroup->winBuf).range.ekey = pTsData[r];
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
    SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, 0);
    QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
    int64_t *pTsData = (int64_t *)pTsCol->pData;
    bool    *ps = NULL, *pe = NULL;
    for (int32_t r = startIdx; r < endIdx; r++) {
      if (IS_REALTIME_GROUP_OPEN_WINDOW(pGroup)) {
        TRINGBUF_FIRST(&pGroup->winBuf).range.ekey = pTsData[r];
        TRINGBUF_FIRST(&pGroup->winBuf).wrownum++;
      } else {
        if (ps == NULL) {
          SFilterColumnParam param = {.numOfCols = taosArrayGetSize(pDataBlock->pDataBlock),
                                      .pDataBlock = pDataBlock->pDataBlock};
          code = filterSetDataFromSlotId(pContext->pStartCond, &param);
          QUERY_CHECK_CODE(code, lino, _end);
          int32_t          status = 0;
          SColumnInfoData *psCol = NULL;
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
      if (IS_REALTIME_GROUP_OPEN_WINDOW(pGroup)) {
        if (pe == NULL) {
          SFilterColumnParam param = {.numOfCols = taosArrayGetSize(pDataBlock->pDataBlock),
                                      .pDataBlock = pDataBlock->pDataBlock};
          code = filterSetDataFromSlotId(pContext->pEndCond, &param);
          QUERY_CHECK_CODE(code, lino, _end);
          int32_t          status = 0;
          SColumnInfoData *peCol = NULL;
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

static int32_t stRealtimeGroupCheck(SSTriggerRealtimeGroup *pGroup) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

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
      ST_TASK_ELOG("invalid stream trigger type %d", pTask->triggerType);
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

#define SSTRIGGER_REALTIME_SESSIONID 1
#define SSTRIGGER_HISTORY_SESSIONID  2

static int32_t stRealtimeContextInit(SSTriggerRealtimeContext *pContext, SStreamTriggerTask *pTask) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSDataBlock *pVirDataBlock = NULL;
  SFilterInfo *pVirDataFilter = NULL;

  pContext->pTask = pTask;
  pContext->sessionId = SSTRIGGER_REALTIME_SESSIONID;
  pContext->maxMetaDelta = 0;      // todo(kjq): adjust dynamically
  pContext->minMetaThreshold = 1;  // todo(kjq): adjust dynamically

  pContext->pReaderWalProgress = tSimpleHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  QUERY_CHECK_NULL(pContext->pReaderWalProgress, code, lino, _end, terrno);
  int32_t nReaders = taosArrayGetSize(pTask->readerList);
  for (int32_t i = 0; i < nReaders; i++) {
    SStreamTaskAddr     *pReader = TARRAY_GET_ELEM(pTask->readerList, i);
    SSTriggerWalProgress progress = {.pTaskAddr = pReader};
    code = tSimpleHashPut(pContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t), &progress,
                          sizeof(SSTriggerWalProgress));
    QUERY_CHECK_CODE(code, lino, _end);
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
  SSTriggerPullRequest *pPullReq = &pContext->pullReq.base;
  pPullReq->streamId = pTask->task.streamId;
  pPullReq->sessionId = pContext->sessionId;
  pPullReq->triggerTaskId = pTask->task.taskId;
  if (pTask->isVirtualTable) {
    pContext->reqCids = taosArrayInit(0, sizeof(col_id_t));
    QUERY_CHECK_NULL(pContext->reqCids, code, lino, _end, terrno);
    pContext->reqCols = taosArrayInit(0, sizeof(OTableInfo));
    QUERY_CHECK_NULL(pContext->reqCols, code, lino, _end, terrno);
  }

  pContext->pCalcDataCacheIters =
      taosHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
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
  for (int32_t i = 0; i < STRIGGER_PULL_TYPE_MAX; ++i) {
    if (pContext->pullRes[i] != NULL) {
      blockDataDestroy(pContext->pullRes[i]);
      pContext->pullRes[i] = NULL;
    }
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

static int32_t stRealtimeContextBuildVirGroups(SSTriggerRealtimeContext *pContext) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pContext->pTask;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stRealtimeContextSendPullReq(SSTriggerRealtimeContext *pContext, ESTriggerPullType type) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pContext->pTask;
  SStreamTaskAddr    *pReader = NULL;

  switch (type) {
    case STRIGGER_PULL_LAST_TS: {
      pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
      QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
      break;
    }

    case STRIGGER_PULL_WAL_META: {
      pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
      QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
      SSTriggerWalMetaRequest *pReq = &pContext->pullReq.walMetaReq;
      SSTriggerWalProgress *pProgress = tSimpleHashGet(pContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
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
      SSTriggerWalTriggerDataRequest *pReq = &pContext->pullReq.walTriggerDataReq;
      SSTriggerRealtimeGroup         *pGroup = stRealtimeContextGetCurrentGroup(pContext);
      QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
      SSTriggerTableMeta *pCurTableMeta = pGroup->pCurTableMeta;
      SSTriggerMetaData  *pMetaToFetch = pContext->pMetaToFetch;
      pReq->uid = pCurTableMeta->tbUid;
      pReq->ver = pMetaToFetch->ver;
      pReq->skey = pMetaToFetch->skey;
      pReq->ekey = pMetaToFetch->ekey;
      SSTriggerWalProgress *pProgress =
          tSimpleHashGet(pContext->pReaderWalProgress, &pCurTableMeta->vgId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pReader = pProgress->pTaskAddr;
      break;
    }

    case STRIGGER_PULL_WAL_DATA: {
      SSTriggerWalDataRequest *pReq = &pContext->pullReq.walDataReq;
      SSTriggerRealtimeGroup  *pGroup = stRealtimeContextGetCurrentGroup(pContext);
      QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
      SSTriggerTableColRef *pColRefToFetch = pContext->pColRefToFetch;
      SSTriggerTableMeta *pCurTableMeta = tSimpleHashGet(pGroup->pTableMetas, &pColRefToFetch->otbUid, sizeof(int64_t));
      QUERY_CHECK_NULL(pCurTableMeta, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SSTriggerMetaData *pMetaToFetch = pContext->pMetaToFetch;
      pReq->uid = pCurTableMeta->tbUid;
      pReq->ver = pMetaToFetch->ver;
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
      SSTriggerWalProgress *pProgress =
          tSimpleHashGet(pContext->pReaderWalProgress, &pCurTableMeta->vgId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pReader = pProgress->pTaskAddr;
      break;
    }

    case STRIGGER_PULL_GROUP_COL_VALUE: {
      SSTriggerGroupColValueRequest *pReq = &pContext->pullReq.groupColValueReq;
      SSTriggerRealtimeGroup        *pGroup = stRealtimeContextGetCurrentGroup(pContext);
      QUERY_CHECK_NULL(pGroup, code, lino, _end, terrno);
      pReq->gid = pGroup->gid;
      int32_t vgId = 0;
      if (pTask->isVirtualTable) {
        int32_t                iter = 0;
        SSTriggerVirTableInfo *pTable = TARRAY_DATA(pGroup->pVirTableInfos);
        QUERY_CHECK_NULL(pTable, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        vgId = pTable->vgId;
      } else {
        int32_t             iter = 0;
        SSTriggerTableMeta *pTable = tSimpleHashIterate(pGroup->pTableMetas, NULL, &iter);
        QUERY_CHECK_NULL(pTable, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        vgId = pTable->vgId;
      }
      SSTriggerWalProgress *pProgress = tSimpleHashGet(pContext->pReaderWalProgress, &vgId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pReader = pProgress->pTaskAddr;
      break;
    }

    case STRIGGER_PULL_VTABLE_INFO: {
      SSTriggerVirTableInfoRequest *pReq = &pContext->pullReq.virTableInfoReq;
      int32_t                       nCols = taosArrayGetSize(pTask->pVirDataBlock->pDataBlock);
      pReq->cids = pContext->reqCids;
      taosArrayEnsureCap(pReq->cids, nCols);
      TARRAY_SIZE(pReq->cids) = nCols;
      for (int32_t i = 0; i < nCols; i++) {
        SColumnInfoData *pCol = TARRAY_GET_ELEM(pTask->pVirDataBlock->pDataBlock, i);
        *(col_id_t *)TARRAY_GET_ELEM(pReq->cids, i) = pCol->info.colId;
      }
      pReader = taosArrayGet(pTask->virtReaderList, pContext->curReaderIdx);
      QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
      break;
    }

    case STRIGGER_PULL_OTABLE_INFO: {
      pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
      QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
      SSTriggerOrigTableInfoRequest *pReq = &pContext->pullReq.origTableInfoReq;
      pReq->cols = pContext->reqCols;
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
      pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
      QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
      SSTriggerSetTableRequest *pReq = &pContext->pullReq.setTableReq;
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

  SSTriggerPullRequest *pReq = &pContext->pullReq.base;
  pReq->type = type;
  pReq->readerTaskId = pReader->taskId;

  // serialize and send request
  SRpcMsg msg = {.msgType = TDMT_STREAM_TRIGGER_PULL, .info.notFreeAhandle = 1};
  QUERY_CHECK_CODE(streamTriggerAllocAhandle(pTask, pReq, &msg.info.ahandle), lino, _end);
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

_end:
  if (code != TSDB_CODE_SUCCESS) {
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
      SSTriggerCalcParam *pParam = TARRAY_DATA(pCalcReq->params);
      SColumnInfoData    *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pTask->calcTsIndex);
      QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
      int64_t *pTsData = (int64_t *)pTsCol->pData;
      int32_t  r = startIdx;
      while (r < endIdx) {
        int64_t ts = pTsData[r];
        while (ts > pParam->wend) {
          pParam++;
          if (TARRAY_ELEM_IDX(pCalcReq->params, pParam) == TARRAY_SIZE(pCalcReq->params)) {
            allTableProcessed = true;
            break;
          }
        }
        if (allTableProcessed) {
          break;
        }
        void   *px = taosbsearch(&pParam->wend, &pTsData[r], endIdx - r, sizeof(int64_t), compareInt64Val, TD_GT);
        int32_t nrows = (px != NULL) ? (POINTER_DISTANCE(px, &pTsData[r]) / sizeof(int64_t)) : (endIdx - r);
        code = putStreamDataCache(pContext->pCalcDataCache, pGroup->gid, pParam->wstart, pParam->wend, pDataBlock, r,
                                  r + nrows - 1);
        QUERY_CHECK_CODE(code, lino, _end);
        r += nrows;
      }
    }

    if (pContext->pColRefToFetch != NULL) {
      code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_WAL_DATA);
      QUERY_CHECK_CODE(code, lino, _end);
      goto _end;
    } else if (pContext->pMetaToFetch != NULL) {
      code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_WAL_CALC_DATA);
      QUERY_CHECK_CODE(code, lino, _end);
      goto _end;
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
  SRpcMsg msg = {.msgType = TDMT_STREAM_TRIGGER_CALC, .info.notFreeAhandle = 1};
  QUERY_CHECK_CODE(streamTriggerAllocAhandle(pTask, pCalcReq, &msg.info.ahandle), lino, _end);
  msg.contLen = tSerializeSTriggerCalcRequest(NULL, 0, pCalcReq);
  QUERY_CHECK_CONDITION(msg.contLen > 0, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  msg.contLen += sizeof(SMsgHead);
  msg.pCont = rpcMallocCont(msg.contLen);
  QUERY_CHECK_NULL(msg.pCont, code, lino, _end, terrno);
  SMsgHead *pMsgHead = (SMsgHead *)msg.pCont;
  pMsgHead->contLen = htonl(msg.contLen);
  pMsgHead->vgId = htonl(SNODE_HANDLE);
  int32_t tlen = tSerializeSTriggerCalcRequest(msg.pCont + sizeof(SMsgHead), msg.contLen - sizeof(SMsgHead), pCalcReq);
  QUERY_CHECK_CONDITION(tlen == msg.contLen - sizeof(SMsgHead), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  code = tmsgSendReq(&pCalcRunner->addr.epset, &msg);
  QUERY_CHECK_CODE(code, lino, _end);

  ST_TASK_DLOG("calc request is sent to node:%d task:%" PRIx64, pCalcRunner->addr.nodeId, pCalcRunner->addr.taskId);

  pContext->pCalcReq = NULL;

_end:
  if (code != TSDB_CODE_SUCCESS) {
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
      QUERY_CHECK_CODE(code, lino, _end);
      pContext->haveReadCheckpoint = true;
    } else {
      // wait 1 second and retry
      int64_t resumeTime = taosGetTimestampNs() + 1 * NANOSECOND_PER_SEC;
      code = streamTriggerAddWaitContext(pContext, pContext->periodWindow.ekey);
      QUERY_CHECK_CODE(code, lino, _end);
      goto _end;
    }
  }
  if (pContext->retryPull) {
    code = stRealtimeContextSendPullReq(pContext, pContext->pullReq.base.type);
    QUERY_CHECK_CODE(code, lino, _end);
    goto _end;
  }

  if (pContext->status == STRIGGER_CONTEXT_IDLE) {
    if (pTask->isVirtualTable && !pTask->virTableInfoReady) {
      pContext->status = STRIGGER_CONTEXT_GATHER_VTABLE_INFO;
      pContext->curReaderIdx = 0;
      if (taosArrayGetSize(pTask->pVirTableInfoRsp) == 0) {
        code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_VTABLE_INFO);
        QUERY_CHECK_CODE(code, lino, _end);
      } else {
        code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_OTABLE_INFO);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      goto _end;
    }

    if (taosArrayGetSize(pTask->readerList) > 0 && tSimpleHashGetSize(pTask->pRealtimeStartVer) == 0) {
      pContext->status = STRIGGER_CONTEXT_DETERMINE_BOUND;
      pContext->curReaderIdx = 0;
      code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_LAST_TS);
      QUERY_CHECK_CODE(code, lino, _end);
      goto _end;
    }

    if (pTask->triggerType != STREAM_TRIGGER_PERIOD) {
      // todo(kjq): start history calc first
      pContext->status = STRIGGER_CONTEXT_FETCH_META;
      pContext->curReaderIdx = 0;
      pContext->getWalMetaThisRound = false;
      code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_WAL_META);
      QUERY_CHECK_CODE(code, lino, _end);
      goto _end;
    }

    // check if to start for period trigger
    int64_t now = taosGetTimestampNs();
    if (pContext->periodWindow.skey == INT64_MIN) {
      pContext->periodWindow = stTriggerTaskGetPeriodWindow(&pTask->interval, now);
    }
    if (now >= pContext->periodWindow.ekey) {
      pContext->status = STRIGGER_CONTEXT_FETCH_META;
      if (taosArrayGetSize(pTask->readerList) > 0) {
        // fetch wal meta from all readers
        pContext->curReaderIdx = 0;
        pContext->getWalMetaThisRound = false;
        code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_WAL_META);
        QUERY_CHECK_CODE(code, lino, _end);
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
        if (TD_DLIST_NODE_NEXT(pGroup) == NULL && TD_DLIST_TAIL(&pContext->groupsToCheck) != pGroup) {
          TD_DLIST_APPEND(&pContext->groupsToCheck, pGroup);
        }
      }
    } else {
      QUERY_CHECK_CONDITION(TD_DLIST_NELES(&pContext->groupsToCheck) == 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
      code = streamTriggerAddWaitContext(pContext, pContext->periodWindow.ekey);
      QUERY_CHECK_CODE(code, lino, _end);
      goto _end;
    }
  }

  while (TD_DLIST_NELES(&pContext->groupsToCheck) > 0) {
    SSTriggerRealtimeGroup *pGroup = TD_DLIST_HEAD(&pContext->groupsToCheck);
    if (pContext->pCalcReq == NULL && pTask->calcEventType != STRIGGER_EVENT_WINDOW_NONE) {
      pContext->status = STRIGGER_CONTEXT_ACQUIRE_REQUEST;
      code = stTriggerTaskAcquireRequest(pTask, pContext->sessionId, pGroup->gid, &pContext->pCalcReq);
      QUERY_CHECK_CODE(code, lino, _end);
      if (pContext->pCalcReq == NULL) {
        ST_TASK_DLOG("no available runner for group %" PRId64, pGroup->gid);
        goto _end;
      }
      if (pTask->triggerType == STREAM_TRIGGER_SLIDING || pTask->triggerType == STREAM_TRIGGER_SESSION) {
        code = stRealtimeGroupSaveInitWindow(pGroup, pContext->pInitWindows);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
    if (pContext->status != STRIGGER_CONTEXT_SEND_CALC_REQ) {
      pContext->status = STRIGGER_CONTEXT_CHECK_CONDITION;
      code = stRealtimeGroupCheck(pGroup);
      QUERY_CHECK_CODE(code, lino, _end);
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
        code = streamSendNotifyContent(&pTask->task, pTask->triggerType, pGroup->gid, pTask->pNotifyAddrUrls,
                                       pTask->notifyErrorHandle, TARRAY_DATA(pContext->pNotifyParams),
                                       TARRAY_SIZE(pContext->pNotifyParams));
        QUERY_CHECK_CODE(code, lino, _end);
      }
      stRealtimeGroupClearTemp(pGroup);
    }
    if (pContext->pCalcReq) {
      if (taosArrayGetSize(pContext->pCalcReq->params) > 0) {
        // todo(kjq): ensure that params are no more than 4096
        pContext->status = STRIGGER_CONTEXT_SEND_CALC_REQ;
        code = stRealtimeContextSendCalcReq(pContext);
        QUERY_CHECK_CODE(code, lino, _end);
        if (pContext->pCalcReq != NULL) {
          // calc req has not been sent
          goto _end;
        }
        stRealtimeGroupClearTemp(pGroup);
      } else {
        code = stTriggerTaskReleaseRequest(pTask, &pContext->pCalcReq);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
    TD_DLIST_POP(&pContext->groupsToCheck, pGroup);

    // clear wal meta and update threshold
    int32_t             iter = 0;
    SSTriggerTableMeta *pTableMeta = tSimpleHashIterate(pGroup->pTableMetas, NULL, &iter);
    while (pTableMeta != NULL) {
      if ((pTask->placeHolderBitmap & PLACE_HOLDER_PARTITION_ROWS) && IS_REALTIME_GROUP_OPEN_WINDOW(pGroup)) {
        int64_t endTime = TRINGBUF_FIRST(&pGroup->winBuf).range.skey - 1;
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
    pContext->status = STRIGGER_CONTEXT_CHECK_CONDITION;
  }

  if (pTask->maxDelay > 0 && TD_DLIST_NELES(&pContext->groupsMaxDelay) == 0 &&
      pContext->curReaderIdx == taosArrayGetSize(pTask->readerList) - 1) {
    int64_t now = taosGetTimestampNs();
    int32_t iter = 0;
    void   *px = tSimpleHashIterate(pContext->pGroups, NULL, &iter);
    while (px != NULL) {
      SSTriggerRealtimeGroup *pGroup = *(SSTriggerRealtimeGroup **)px;
      if (IS_REALTIME_GROUP_OPEN_WINDOW(pGroup)) {
        SSTriggerWindow *pFirst = &TRINGBUF_FIRST(&pGroup->winBuf);
        SSTriggerWindow *pLast = &TRINGBUF_LAST(&pGroup->winBuf);
        SSTriggerWindow *p = pFirst;
        while (true) {
          if (p->prevProcTime + pTask->maxDelay <= now) {
            TD_DLIST_APPEND(&pContext->groupsMaxDelay, pGroup);
            break;
          }
          if (p == pLast) {
            break;
          }
          TRINGBUF_MOVE_NEXT(&pGroup->winBuf, p);
        }
      }
      px = tSimpleHashIterate(pContext->pGroups, px, &iter);
    }
  }

  while (TD_DLIST_NELES(&pContext->groupsMaxDelay) > 0) {
    SSTriggerRealtimeGroup *pGroup = TD_DLIST_HEAD(&pContext->groupsMaxDelay);
    if (pContext->pCalcReq == NULL && (pTask->calcEventType & STRIGGER_EVENT_WINDOW_CLOSE)) {
      pContext->status = STRIGGER_CONTEXT_ACQUIRE_REQUEST;
      code = stTriggerTaskAcquireRequest(pTask, pContext->sessionId, pGroup->gid, &pContext->pCalcReq);
      QUERY_CHECK_CODE(code, lino, _end);
      if (pContext->pCalcReq == NULL) {
        ST_TASK_DLOG("no available runner for group %" PRId64, pGroup->gid);
        goto _end;
      }
    }
    if (pContext->status != STRIGGER_CONTEXT_SEND_CALC_REQ) {
      int64_t now = taosGetTimestampNs();
      QUERY_CHECK_CONDITION(IS_REALTIME_GROUP_OPEN_WINDOW(pGroup), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      SSTriggerWindow *pFirst = &TRINGBUF_FIRST(&pGroup->winBuf);
      SSTriggerWindow *pLast = &TRINGBUF_LAST(&pGroup->winBuf);
      SSTriggerWindow *p = pFirst;
      while (true) {
        if (p->prevProcTime + pTask->maxDelay <= now) {
          SSTriggerCalcParam param = {
              .triggerTime = now,
              .wstart = p->range.skey,
              .wend = p->range.ekey,
              .wduration = p->range.ekey - p->range.skey,
              .wrownum = (p == pFirst) ? p->wrownum : (pFirst->wrownum - p->wrownum),
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
        if (p == pLast) {
          break;
        }
        TRINGBUF_MOVE_NEXT(&pGroup->winBuf, p);
      }

      if (taosArrayGetSize(pContext->pNotifyParams) > 0) {
        code = streamSendNotifyContent(&pTask->task, pTask->triggerType, pGroup->gid, pTask->pNotifyAddrUrls,
                                       pTask->notifyErrorHandle, TARRAY_DATA(pContext->pNotifyParams),
                                       TARRAY_SIZE(pContext->pNotifyParams));
        QUERY_CHECK_CODE(code, lino, _end);
      }
      stRealtimeGroupClearTemp(pGroup);
    }
    if (pContext->pCalcReq) {
      if (taosArrayGetSize(pContext->pCalcReq->params) > 0) {
        // todo(kjq): ensure that params are no more than 4096
        pContext->status = STRIGGER_CONTEXT_SEND_CALC_REQ;
        code = stRealtimeContextSendCalcReq(pContext);
        QUERY_CHECK_CODE(code, lino, _end);
        if (pContext->pCalcReq != NULL) {
          // calc req has not been sent
          goto _end;
        }
        stRealtimeGroupClearTemp(pGroup);
      } else {
        code = stTriggerTaskReleaseRequest(pTask, &pContext->pCalcReq);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
    TD_DLIST_POP(&pContext->groupsMaxDelay, pGroup);
    pContext->status = STRIGGER_CONTEXT_CHECK_CONDITION;
  }

  if (pTask->triggerType == STREAM_TRIGGER_PERIOD) {
    stTriggerTaskNextPeriodWindow(&pTask->interval, &pContext->periodWindow);
    pContext->status = STRIGGER_CONTEXT_IDLE;
    code = streamTriggerAddWaitContext(pContext, pContext->periodWindow.ekey);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    pContext->curReaderIdx = (pContext->curReaderIdx + 1) % taosArrayGetSize(pTask->readerList);
    if (pContext->curReaderIdx == 0) {
      // todo(kjq): start history calc if needed
      pContext->getWalMetaThisRound = false;
#define STRIGGER_CHECK_POINT_INTERVAL_NS 10 * NANOSECOND_PER_MINUTE  // 10min
      int64_t now = taosGetTimestampNs();
      if (now > pContext->lastCheckpointTime + STRIGGER_CHECK_POINT_INTERVAL_NS) {
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
      if (!pContext->getWalMetaThisRound) {
        // add the task to wait list since it catches up all readers
        pContext->status = STRIGGER_CONTEXT_IDLE;
        int64_t resumeTime = taosGetTimestampNs() + STREAM_TRIGGER_WAIT_TIME_NS;
        code = streamTriggerAddWaitContext(pContext, resumeTime);
        QUERY_CHECK_CODE(code, lino, _end);
        goto _end;
      }
    }
    // pull new wal metas
    pContext->status = STRIGGER_CONTEXT_FETCH_META;
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
  SSTriggerPullRequest     *pReq = NULL;
  SSDataBlock              *pTempDataBlock = NULL;
  SStreamMsgVTableInfo      vtableInfo = {0};
  SSTriggerOrigTableInfoRsp otableInfo = {0};
  SArray                   *pOrigTableNames = NULL;

  QUERY_CHECK_CONDITION(pRsp->code == TSDB_CODE_SUCCESS || pRsp->code == TSDB_CODE_WAL_LOG_NOT_EXIST, code, lino, _end,
                        TSDB_CODE_INVALID_PARA);

  SSTriggerAHandle *pAhandle = pRsp->info.ahandle;
  pReq = pAhandle->param;
  switch (pReq->type) {
    case STRIGGER_PULL_LAST_TS: {
      QUERY_CHECK_CONDITION(pContext->status == STRIGGER_CONTEXT_DETERMINE_BOUND, code, lino, _end,
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
      int64_t latestVer = pDataBlock->info.id.groupId;
      void   *px = tSimpleHashGet(pTask->pRealtimeStartVer, &pReader->nodeId, sizeof(int32_t));
      QUERY_CHECK_CONDITION(px == NULL, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      code = tSimpleHashPut(pTask->pRealtimeStartVer, &pReader->nodeId, sizeof(int32_t), &latestVer, sizeof(int64_t));
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

      if (pContext->curReaderIdx != taosArrayGetSize(pTask->readerList) - 1) {
        pContext->curReaderIdx++;
        code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_LAST_TS);
        QUERY_CHECK_CODE(code, lino, _end);
      } else {
        if (!pTask->fillHistory && !pTask->fillHistoryFirst) {
          int32_t               iter = 0;
          SSTriggerWalProgress *pProgress = tSimpleHashIterate(pContext->pReaderWalProgress, NULL, &iter);
          while (pProgress != NULL) {
            void *px = tSimpleHashGet(pTask->pRealtimeStartVer, &pProgress->pTaskAddr->nodeId, sizeof(int32_t));
            QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
            pProgress->lastScanVer = pProgress->latestVer = *(int64_t *)px;
            pProgress = tSimpleHashIterate(pContext->pReaderWalProgress, pProgress, &iter);
          }
        }
        pContext->status = STRIGGER_CONTEXT_IDLE;
        code = stRealtimeContextCheck(pContext);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      break;
    }

    case STRIGGER_PULL_WAL_META: {
      QUERY_CHECK_CONDITION(pContext->status == STRIGGER_CONTEXT_FETCH_META, code, lino, _end,
                            TSDB_CODE_INTERNAL_ERROR);
      SSDataBlock *pDataBlock = pContext->pullRes[pReq->type];
      if (pDataBlock == NULL) {
        pDataBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
        QUERY_CHECK_NULL(pDataBlock, code, lino, _end, terrno);
        pContext->pullRes[pReq->type] = pDataBlock;
      }
      if (pRsp->code == TSDB_CODE_WAL_LOG_NOT_EXIST) {
        QUERY_CHECK_CONDITION(pRsp->contLen == sizeof(int64_t), code, lino, _end, TSDB_CODE_INVALID_PARA);
        blockDataEmpty(pDataBlock);
        pDataBlock->info.id.groupId = *(int64_t *)pRsp->pCont;
      } else {
        QUERY_CHECK_CONDITION(pRsp->contLen > 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
        const char *pCont = pRsp->pCont;
        code = blockDecode(pDataBlock, pCont, &pCont);
        QUERY_CHECK_CODE(code, lino, _end);
        QUERY_CHECK_CONDITION(pCont == pRsp->pCont + pRsp->contLen, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      }

      QUERY_CHECK_CONDITION(TD_DLIST_NELES(&pContext->groupsToCheck) == 0, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      int32_t nrows = blockDataGetNumOfRows(pDataBlock);
      // update reader wal progress
      SStreamTaskAddr *pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
      QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
      SSTriggerWalProgress *pProgress = tSimpleHashGet(pContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t));
      QUERY_CHECK_NULL(pProgress, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pProgress->lastScanVer = pDataBlock->info.id.groupId;
      pProgress->latestVer = pDataBlock->info.id.groupId;
      if (nrows > 0) {
        SColumnInfoData *pVerCol = taosArrayGet(pDataBlock->pDataBlock, 5);
        pProgress->lastScanVer = *(int64_t *)colDataGetNumData(pVerCol, nrows - 1);
        pContext->getWalMetaThisRound = true;
      }
      if (nrows > 0) {
        // find groups to be checked
        if (!pTask->isVirtualTable) {
          SColumnInfoData *pGidCol = taosArrayGet(pDataBlock->pDataBlock, 1);
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
              code = stRealtimeGroupAddMetaDatas(pGroup, pDataBlock);
              QUERY_CHECK_CODE(code, lino, _end);
              if (pGroup->newThreshold > pGroup->oldThreshold) {
                TD_DLIST_APPEND(&pContext->groupsToCheck, pGroup);
              }
            }
          }
        } else {
          int32_t iter = 0;
          void   *px = tSimpleHashIterate(pContext->pGroups, NULL, &iter);
          while (px != NULL) {
            SSTriggerRealtimeGroup *pGroup = *(SSTriggerRealtimeGroup **)px;
            code = stRealtimeGroupAddMetaDatas(pGroup, pDataBlock);
            QUERY_CHECK_CODE(code, lino, _end);
            if (pGroup->newThreshold > pGroup->oldThreshold) {
              TD_DLIST_APPEND(&pContext->groupsToCheck, pGroup);
            }
            px = tSimpleHashIterate(pContext->pGroups, px, &iter);
          }
        }
      }
      if (pTask->triggerType == STREAM_TRIGGER_PERIOD &&
          pContext->curReaderIdx != taosArrayGetSize(pTask->readerList) - 1) {
        pContext->curReaderIdx++;
        code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_WAL_META);
        QUERY_CHECK_CODE(code, lino, _end);
      } else {
        code = stRealtimeContextCheck(pContext);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      break;
    }

    case STRIGGER_PULL_WAL_TS_DATA:
    case STRIGGER_PULL_WAL_TRIGGER_DATA:
    case STRIGGER_PULL_WAL_CALC_DATA:
    case STRIGGER_PULL_WAL_DATA: {
      QUERY_CHECK_CONDITION(
          pContext->status == STRIGGER_CONTEXT_CHECK_CONDITION || pContext->status == STRIGGER_CONTEXT_SEND_CALC_REQ,
          code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pTempDataBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
      QUERY_CHECK_NULL(pTempDataBlock, code, lino, _end, terrno);
      if (pRsp->contLen > 0) {
        const char *pCont = pRsp->pCont;
        code = blockDecode(pTempDataBlock, pCont, &pCont);
        QUERY_CHECK_CODE(code, lino, _end);
        QUERY_CHECK_CONDITION(pCont == pRsp->pCont + pRsp->contLen, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      } else {
        blockDataEmpty(pTempDataBlock);
      }
      if (pContext->pColRefToFetch != NULL) {
        code = stVtableMergerBindDataBlock(pContext->pMerger, &pTempDataBlock);
        TSDB_CHECK_CODE(code, lino, _end);
      } else {
        code = stTimestampSorterBindDataBlock(pContext->pSorter, &pTempDataBlock);
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
      code = tDeserializeSStreamMsgVTableInfo(pRsp->pCont, pRsp->contLen, &vtableInfo);
      QUERY_CHECK_CODE(code, lino, _end);
      SStreamTaskAddr *pReader = taosArrayGet(pTask->virtReaderList, pContext->curReaderIdx);
      QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
      int32_t nVirTables = taosArrayGetSize(vtableInfo.infos);
      for (int32_t i = 0; i < nVirTables; i++) {
        VTableInfo           *pInfo = TARRAY_GET_ELEM(vtableInfo.infos, i);
        SSTriggerVirTableInfo newInfo = {
            .tbGid = pInfo->gId, .tbUid = pInfo->uid, .tbVer = pInfo->ver, .vgId = pReader->nodeId};
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

      if (pContext->curReaderIdx != taosArrayGetSize(pTask->virtReaderList) - 1) {
        pContext->curReaderIdx++;
        code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_VTABLE_INFO);
        QUERY_CHECK_CODE(code, lino, _end);
      } else {
        pOrigTableNames = taosArrayInit(0, sizeof(SStreamDbTableName));
        QUERY_CHECK_NULL(pOrigTableNames, code, lino, _end, terrno);
        int32_t iter1 = 0;
        void   *px = tSimpleHashIterate(pTask->pOrigTableCols, NULL, &iter1);
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
        pReq->type = STREAM_MGMT_REQ_TRIGGER_ORIGTBL_READER;
        pReq->cont.fullTableNames = pOrigTableNames;
        pOrigTableNames = NULL;

        // wait to be exeucted again
        pContext->status = STRIGGER_CONTEXT_IDLE;
        pTask->task.pMgmtReq = pReq;
        pTask->task.status = STREAM_STATUS_INIT;
      }
      break;
    }

    case STRIGGER_PULL_OTABLE_INFO: {
      QUERY_CHECK_CONDITION(pContext->status == STRIGGER_CONTEXT_GATHER_VTABLE_INFO, code, lino, _end,
                            TSDB_CODE_INTERNAL_ERROR);
      code = tDserializeSTriggerOrigTableInfoRsp(pRsp->pCont, pRsp->contLen, &otableInfo);
      QUERY_CHECK_CODE(code, lino, _end);
      SSTriggerOrigTableInfoRequest *pOrigReq = (SSTriggerOrigTableInfoRequest *)pReq;
      QUERY_CHECK_CONDITION(taosArrayGetSize(otableInfo.cols) == taosArrayGetSize(pOrigReq->cols), code, lino, _end,
                            TSDB_CODE_INVALID_PARA);
      OTableInfoRsp *pRsp = TARRAY_DATA(otableInfo.cols);

      SStreamTaskAddr *pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
      QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
      void *px = tSimpleHashGet(pTask->pReaderUidMap, &pReader->nodeId, sizeof(int32_t));
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

      if (pContext->curReaderIdx != taosArrayGetSize(pTask->readerList) - 1) {
        pContext->curReaderIdx++;
        code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_OTABLE_INFO);
        QUERY_CHECK_CODE(code, lino, _end);
      } else {
        pContext->curReaderIdx = 0;
        code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_SET_TABLE);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      break;
    }

    case STRIGGER_PULL_SET_TABLE: {
      if (pContext->curReaderIdx != taosArrayGetSize(pTask->readerList) - 1) {
        pContext->curReaderIdx++;
        code = stRealtimeContextSendPullReq(pContext, STRIGGER_PULL_SET_TABLE);
        QUERY_CHECK_CODE(code, lino, _end);
      } else {
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
      }
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
  tDestroySStreamMsgVTableInfo(&vtableInfo);
  tDestroySTriggerOrigTableInfoRsp(&otableInfo);
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

  SSTriggerAHandle *pAhandle = pRsp->info.ahandle;
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
  int64_t p[] = {sessionId, gid};
  pRunningFlag = tSimpleHashGet(pTask->pGroupRunning, p, sizeof(p));
  if (pRunningFlag == NULL) {
    bool flag[nCalcNodes + 1];
    code = tSimpleHashPut(pTask->pGroupRunning, p, sizeof(p), flag, sizeof(flag));
    QUERY_CHECK_CODE(code, lino, _end);
    pRunningFlag = tSimpleHashGet(pTask->pGroupRunning, p, sizeof(p));
    QUERY_CHECK_NULL(pRunningFlag, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    memset(pRunningFlag, 0, sizeof(flag));
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
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  // todo(kjq): mark recalculation interval

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
  }
  pCalcSlotids = taosArrayInit(nCalcCols, sizeof(int32_t));
  QUERY_CHECK_NULL(pCalcSlotids, code, lino, _end, terrno);
  for (int32_t i = 0; i < nCalcCols; i++) {
    col_id_t id = *(col_id_t *)TARRAY_GET_ELEM(pCalcColids, i);
    int32_t  slotid = taosArraySearchIdx(pVirColIds, &id, compareInt16Val, TD_EQ);
    QUERY_CHECK_CONDITION(slotid >= 0, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    void *px = taosArrayPush(pCalcSlotids, &slotid);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
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
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t stTriggerTaskDeploy(SStreamTriggerTask *pTask, SStreamTriggerDeployMsg *pMsg) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  EWindowType type = pMsg->triggerType;
  pTask->trigTsIndex = 0;
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
      pTask->trigTsIndex = pSession->slotId;
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
      ST_TASK_ELOG("invalid stream trigger window type %d", type);
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  pTask->leaderSnodeId = pMsg->leaderSnodeId;
  pTask->calcTsIndex = pMsg->tsSlotId;
  pTask->maxDelay = pMsg->maxDelay * NANOSECOND_PER_MSEC;
  pTask->fillHistoryStartTime = pMsg->fillHistoryStartTime;
  pTask->watermark = pMsg->watermark;
  pTask->expiredTime = pMsg->expiredTime;
  pTask->ignoreDisorder = pMsg->igDisorder;
  pTask->fillHistory = pMsg->fillHistory;
  pTask->fillHistoryFirst = pMsg->fillHistoryFirst;
  pTask->lowLatencyCalc = pMsg->lowLatencyCalc;
  pTask->hasPartitionBy = pMsg->hasPartitionBy;
  pTask->isVirtualTable = pMsg->isTriggerTblVirt;
  pTask->placeHolderBitmap = pMsg->placeHolderBitmap;
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
  if (pTask->notifyEventType == STRIGGER_EVENT_WINDOW_NONE && taosArrayGetSize(pTask->pNotifyAddrUrls) > 0) {
    QUERY_CHECK_CONDITION(pTask->triggerType == STREAM_TRIGGER_PERIOD || pTask->triggerType == STREAM_TRIGGER_SLIDING,
                          code, lino, _end, TSDB_CODE_INVALID_PARA);
    pTask->notifyEventType = STRIGGER_EVENT_WINDOW_CLOSE;
  }
  pTask->notifyErrorHandle = pMsg->notifyErrorHandle;
  pTask->notifyHistory = pMsg->notifyHistory;
  if (pTask->isVirtualTable) {
    TSWAP(pTask->virtReaderList, pMsg->readerList);
  } else {
    TSWAP(pTask->readerList, pMsg->readerList);
  }
  TSWAP(pTask->runnerList, pMsg->runnerList);

  pTask->pRealtimeStartVer = tSimpleHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  QUERY_CHECK_NULL(pTask->pRealtimeStartVer, code, lino, _end, terrno);
  pTask->pHistoryCutoffTime = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  QUERY_CHECK_NULL(pTask->pHistoryCutoffTime, code, lino, _end, terrno);

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

  stDebug("[checkpoint] stTriggerTaskUndeploy, taskId: %" PRId64 ", streamId: %" PRIx64
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
    QUERY_CHECK_CODE(code, lino, _end);
  }
  if (pMsg->doCleanup) {
    streamDeleteCheckPoint(pTask->task.streamId);
    int32_t leaderSid = pTask->leaderSnodeId;
    SEpSet *epSet = gStreamMgmt.getSynEpset(leaderSid);
    if (epSet != NULL) {
      code = streamSyncDeleteCheckpoint(pTask->task.streamId, epSet);
      QUERY_CHECK_CODE(code, lino, _end);
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
    for (int32_t i = 0; i < TARRAY_SIZE(pTask->pVirTableInfoRsp); i++) {
      VTableInfo *pInfo = TARRAY_GET_ELEM(pTask->pVirTableInfoRsp, i);
      taosMemoryFreeClear(pInfo->cols.pColRef);
    }
    taosArrayDestroy(pTask->pVirTableInfoRsp);
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
      if (pTask->pRealtimeContext == NULL) {
        pTask->pRealtimeContext = taosMemoryCalloc(1, sizeof(SSTriggerRealtimeContext));
        code = stRealtimeContextInit(pTask->pRealtimeContext, pTask);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      code = stRealtimeContextCheck(pTask->pRealtimeContext);
      QUERY_CHECK_CODE(code, lino, _end);
      pTask->task.status = STREAM_STATUS_RUNNING;
      break;
    }
    case STREAM_MSG_ORIGTBL_READER_INFO: {
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
      if (taosArrayGetSize(pRsp->cont.readerList) > 0) {
        px = taosArrayAddAll(pTask->readerList, pRsp->cont.readerList);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      }
      QUERY_CHECK_CONDITION(taosArrayGetSize(pTask->readerList) == tSimpleHashGetSize(pTask->pReaderUidMap), code, lino,
                            _end, TSDB_CODE_INVALID_PARA);

      int32_t nReaders = taosArrayGetSize(pTask->readerList);
      for (int32_t i = 0; i < nReaders; i++) {
        SStreamTaskAddr     *pReader = TARRAY_GET_ELEM(pTask->readerList, i);
        SSTriggerWalProgress progress = {.pTaskAddr = pReader};
        code = tSimpleHashPut(pTask->pRealtimeContext->pReaderWalProgress, &pReader->nodeId, sizeof(int32_t), &progress,
                              sizeof(SSTriggerWalProgress));
        QUERY_CHECK_CODE(code, lino, _end);
      }
      break;
    }
    case STREAM_MSG_UPDATE_RUNNER:
    case STREAM_MSG_USER_RECALC: {
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

int32_t stTriggerTaskProcessRsp(SStreamTask *pStreamTask, SRpcMsg *pRsp, int64_t *pErrTaskId) {
  int32_t             code = 0;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = (SStreamTriggerTask *)pStreamTask;

  *pErrTaskId = pStreamTask->taskId;

  if (gStreamTriggerToStop) {
    ST_TASK_DLOG("skip process stream trigger response %p with type %d since dnode is stopping", pRsp, pRsp->msgType);
    goto _end;
  }

  SSTriggerAHandle *pAhandle = pRsp->info.ahandle;

  if (pRsp->msgType == TDMT_STREAM_TRIGGER_PULL_RSP) {
    SSTriggerPullRequest *pReq = pAhandle->param;
    switch (pRsp->code) {
      case TSDB_CODE_SUCCESS:
      case TSDB_CODE_WAL_LOG_NOT_EXIST: {
        code = stRealtimeContextProcPullRsp(pTask->pRealtimeContext, pRsp);
        QUERY_CHECK_CODE(code, lino, _end);
        break;
      }
      case TSDB_CODE_STREAM_TASK_NOT_EXIST: {
        pTask->pRealtimeContext->retryPull = true;
        int64_t resumeTime = taosGetTimestampNs() + STREAM_ACT_MIN_DELAY_MSEC * NANOSECOND_PER_MSEC;
        code = streamTriggerAddWaitContext(pTask->pRealtimeContext, resumeTime);
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
      code = stRealtimeContextProcCalcRsp(pTask->pRealtimeContext, pRsp);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
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

int32_t stTriggerTaskGetStatus(SStreamTask *pTask, SSTriggerRuntimeStatus *pStatus) {
  // todo(kjq): implement how to get recalculation progress
  return TSDB_CODE_SUCCESS;
}
