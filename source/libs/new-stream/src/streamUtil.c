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

#include "cJSON.h"
#include "cmdnodes.h"
#include "dataSink.h"
#include "decimal.h"
#include "osMemPool.h"
#include "streamInt.h"
#include "tcurl.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "tstrbuild.h"
#include "ttime.h"

int32_t streamGetThreadIdx(int32_t threadNum, int64_t streamGId) { return threadNum ? (streamGId % threadNum) : 0; }

int64_t streamTaskGetMonotonicUs(void) {
  struct timespec now = {0};
  (void)taosClockGetTime(CLOCK_MONOTONIC, &now);
  return (int64_t)now.tv_sec * 1000000LL + (int64_t)now.tv_nsec / 1000;
}

uint64_t streamTaskMetricMask(const SStreamTask* pTask) {
  if (pTask == NULL) return 0;

  switch (pTask->type) {
    case STREAM_READER_TASK:
      return STREAM_METRIC_PHYSICAL_INPUT;
    case STREAM_TRIGGER_TASK:
      return STREAM_METRIC_LOGICAL_INPUT | STREAM_METRIC_REALTIME_LAG | STREAM_METRIC_HISTORY_PROGRESS |
             STREAM_METRIC_RECALCULATES;
    case STREAM_RUNNER_TASK:
      return ((const SStreamRunnerTask*)pTask)->topTask ? STREAM_METRIC_DELIVERED_OUTPUT | STREAM_METRIC_RESULT_LATENCY
                                                        : 0;
  }
  return 0;
}

SStreamTaskStats** streamTaskGetStatsSlot(SStreamTask* pTask) {
  if (pTask == NULL) return NULL;

  switch (pTask->type) {
    case STREAM_READER_TASK:
      return &((SStreamReaderTask*)pTask)->pStats;
    case STREAM_TRIGGER_TASK:
      return &((SStreamTriggerTask*)pTask)->pStats;
    case STREAM_RUNNER_TASK:
      return &((SStreamRunnerTask*)pTask)->pStats;
  }
  return NULL;
}

SStreamTaskStats* streamTaskGetStats(SStreamTask* pTask) {
  SStreamTaskStats** ppStats = streamTaskGetStatsSlot(pTask);
  return ppStats == NULL ? NULL : *ppStats;
}

int32_t streamTaskStatsInit(SStreamTask* pTask, SStreamTaskStats** ppStats) {
  if (pTask == NULL || ppStats == NULL || *ppStats != NULL) return TSDB_CODE_INVALID_PARA;

  return stTaskStatsCreate(pTask->type, streamTaskMetricMask(pTask), streamTaskGetMonotonicUs(), taosGetTimestampMs(),
                           ppStats);
}

void streamTaskStatsHandleLifecycle(SStreamTaskStats** ppStats, EStreamTaskStatsLifecycle event) {
  switch (event) {
    case STREAM_TASK_STATS_UNDEPLOYED:
      return;
    case STREAM_TASK_STATS_DEPLOY_FAILED:
    case STREAM_TASK_STATS_REMOVED:
    case STREAM_TASK_STATS_OWNER_DESTROYED:
      stTaskStatsDestroy(ppStats);
      return;
  }
}

int32_t stmAddFetchStreamGid(void) {
  if (++gStreamMgmt.stmGrpIdx >= STREAM_MAX_GROUP_NUM) {
    gStreamMgmt.stmGrpIdx = 0;
  }

  return gStreamMgmt.stmGrpIdx;
}

int32_t stmAddMgmtReq(int64_t streamId, SArray** ppReq, int32_t idx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (NULL == *ppReq) {
    *ppReq = taosArrayInit(5, sizeof(int32_t));
    TSDB_CHECK_NULL(*ppReq, code, lino, _exit, terrno);
  }

  TSDB_CHECK_NULL(taosArrayPush(*ppReq, &idx), code, lino, _exit, terrno);

  stsDebug("task with mgmtReq added, idx:%d", idx);

_exit:

  return code;
}

static int32_t stmCloneTaskExtraErrMsg(SStreamTask* pTask, SStmTaskStatusMsg* pStatus) {
  pStatus->extraErrMsg = NULL;
  if (pTask->extraErrMsg == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  pStatus->extraErrMsg = taosStrdup(pTask->extraErrMsg);
  return pStatus->extraErrMsg == NULL ? terrno : TSDB_CODE_SUCCESS;
}

static int32_t stmInitTaskMetrics(SStreamHbMsg* pMsg, int32_t capacity) {
  if (pMsg == NULL || capacity < 0) return TSDB_CODE_INVALID_PARA;

  pMsg->observabilityVersion = STREAM_HB_OBSERVABILITY_VERSION_V1;
  if (pMsg->pTaskMetrics != NULL) return TSDB_CODE_SUCCESS;

  pMsg->pTaskMetrics = taosArrayInit(capacity, sizeof(SStreamTaskMetricsEntry));
  return pMsg->pTaskMetrics == NULL ? terrno : TSDB_CODE_SUCCESS;
}

static int32_t stmAppendTaskMetrics(SStreamHbMsg* pMsg, SStreamTask* pTask, SStreamTaskStats* pStats,
                                    int32_t taskStatusIndex) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  SStreamTaskMetricsEntry entry = {
      .taskStatusIndex = taskStatusIndex,
      .streamId = pTask->streamId,
      .taskId = pTask->taskId,
      .seriousId = pTask->seriousId,
      .decodeCode = TSDB_CODE_SUCCESS,
  };

  code = stmInitTaskMetrics(pMsg, taosArrayGetSize(pMsg->pStreamStatus));
  if (code != TSDB_CODE_SUCCESS) return code;

  if (pStats != NULL) {
    if (pTask->type == STREAM_TRIGGER_TASK) {
      code = stTriggerTaskGetMetrics((SStreamTriggerTask*)pTask, &entry.snapshot);
    } else {
      code = stTaskStatsSnapshot1m(pStats, streamTaskGetMonotonicUs(), &entry.snapshot);
    }
    if (code != TSDB_CODE_SUCCESS) goto _exit;
  }

  if (taosArrayPush(pMsg->pTaskMetrics, &entry) == NULL) {
    code = terrno;
    goto _exit;
  }
  entry.snapshot.pRecalculates = NULL;

_exit:
  taosArrayDestroy(entry.snapshot.pRecalculates);
  return code;
}

static int32_t streamTaskLogPeriod(SStreamTask* pTask, const SStreamTaskPeriodSnapshot* pSnapshot) {
  switch (pTask->type) {
    case STREAM_READER_TASK:
      return stReaderTaskLogStats(pTask, pSnapshot);
    case STREAM_TRIGGER_TASK:
      return stTriggerTaskLogStats((SStreamTriggerTask*)pTask, pSnapshot);
    case STREAM_RUNNER_TASK:
      return stRunnerTaskLogStats((SStreamRunnerTask*)pTask, pSnapshot);
  }
  return TSDB_CODE_INVALID_PARA;
}

int32_t stmMaybeRotateTaskStats(SStreamTask* pTask, SStreamTaskStats* pStats, int64_t nowMonoUs, bool debugEnabled) {
  if (pTask == NULL) return TSDB_CODE_INVALID_PARA;
  if (pStats == NULL) return TSDB_CODE_SUCCESS;

  SStreamTaskPeriodSnapshot snapshot = {0};
  bool                      rotated = false;
  int32_t                   code = stTaskStatsRotatePeriod(pStats, nowMonoUs, &snapshot, &rotated);
  if (code != TSDB_CODE_SUCCESS || !debugEnabled) return code;
  if (pTask->type == STREAM_TRIGGER_TASK) {
    return streamTaskLogPeriod(pTask, rotated ? &snapshot : NULL);
  }
  if (!rotated) return TSDB_CODE_SUCCESS;

  return streamTaskLogPeriod(pTask, &snapshot);
}

static int32_t stmPushTaskStatus(SStreamHbMsg* pMsg, SStreamTask* pTask, SStreamTaskStats* pStats) {
  int32_t code = TSDB_CODE_SUCCESS;

  int32_t statsCode =
      stmMaybeRotateTaskStats(pTask, pStats, streamTaskGetMonotonicUs(), (stDebugFlag & DEBUG_DEBUG) != 0);
  if (statsCode != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("failed to rotate or log task statistics: %s", tstrerror(statsCode));
  }

  if (taosArrayPush(pMsg->pStreamStatus, pTask) == NULL) {
    return terrno;
  }

  SStmTaskStatusMsg* pStatus = taosArrayGetLast(pMsg->pStreamStatus);
  code = stmCloneTaskExtraErrMsg(pTask, pStatus);
  if (code != TSDB_CODE_SUCCESS) {
    TARRAY_SIZE(pMsg->pStreamStatus)--;
    return code;
  }

  int32_t metricCode = stmAppendTaskMetrics(pMsg, pTask, pStats, taosArrayGetSize(pMsg->pStreamStatus) - 1);
  if (metricCode != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("failed to append task metrics to heartbeat: %s", tstrerror(metricCode));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t stmAddPeriodReport(int64_t streamId, SArray** ppReport, SStreamTriggerTask* triggerTask) {
  int32_t                code = TSDB_CODE_SUCCESS;
  int32_t                lino = 0;
  SSTriggerRuntimeStatus status = {0};

  if (NULL == *ppReport) {
    *ppReport = taosArrayInit(5, sizeof(SSTriggerRuntimeStatus));
    TSDB_CHECK_NULL(*ppReport, code, lino, _exit, terrno);
  }

  TAOS_CHECK_EXIT(stTriggerTaskGetStatus((SStreamTask*)triggerTask, &status));

  TSDB_CHECK_NULL(taosArrayPush(*ppReport, &status), code, lino, _exit, terrno);
  status.userRecalcs = NULL;

  stsDebug("trigger task period report added, recalcNum:%d", (int32_t)taosArrayGetSize(status.userRecalcs));

_exit:

  taosArrayDestroy(status.userRecalcs);

  if (code) {
    stsError("%s failed at line %d since %s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

void stmHandleStreamRemovedTasks(SStreamInfo* pStream, int64_t streamId, int32_t gid) {
  if (taosArrayGetSize(pStream->undeployReaders) > 0) {
    smHandleRemovedTask(pStream, streamId, gid, STREAM_READER_TASK, pStream->undeployReaders, pStream->readerList);
  }

  if (taosArrayGetSize(pStream->undeployTriggers) > 0) {
    smHandleRemovedTask(pStream, streamId, gid, STREAM_TRIGGER_TASK, pStream->undeployTriggers, pStream->triggerList);
  }

  if (taosArrayGetSize(pStream->undeployRunners) > 0) {
    smHandleRemovedTask(pStream, streamId, gid, STREAM_RUNNER_TASK, pStream->undeployRunners, pStream->runnerList);
  }
}

int32_t stmHbAddTaskStatus(int64_t streamId, SStreamHbMsg* pMsg, SStreamTask* pTask, SStreamTaskStats* pStats) {
  int32_t code = 0, lino = 0;

  taosWLockLatch(&pTask->mgmtReqLock);
  SStreamMgmtReq* pMgmtReq = pTask->pMgmtReq;
  if (pMgmtReq) {
    TAOS_CHECK_EXIT(stmPushTaskStatus(pMsg, pTask, pStats));
    SStmTaskStatusMsg* pStatus = taosArrayGetLast(pMsg->pStreamStatus);
    pStatus->pMgmtReq = NULL;
    TAOS_CHECK_EXIT(tCloneSStreamMgmtReq(pMgmtReq, &pStatus->pMgmtReq));
    TAOS_CHECK_EXIT(stmAddMgmtReq(streamId, &pMsg->pStreamReq, taosArrayGetSize(pMsg->pStreamStatus) - 1));
  } else {
    TAOS_CHECK_EXIT(stmPushTaskStatus(pMsg, pTask, pStats));
  }

_exit:

  taosWUnLockLatch(&pTask->mgmtReqLock);

  if (code) {
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t stmHbAddStreamStatus(SStreamHbMsg* pMsg, SStreamInfo* pStream, int64_t streamId, bool reportPeriod) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SListIter    iter = {0};
  SListNode*   listNode = NULL;
  SStreamTask* pTask = NULL;

  taosWLockLatch(&pStream->lock);

  stmHandleStreamRemovedTasks(pStream, streamId, pMsg->streamGId);

  if (pStream->taskNum <= 0) {
    stsDebug("ignore stream status update since stream taskNum %d is invalid", pStream->taskNum);
    goto _exit;
  }

  if (NULL == pMsg->pStreamStatus) {
    pMsg->pStreamStatus = taosArrayInit(pStream->taskNum, sizeof(SStmTaskStatusMsg));
    TSDB_CHECK_NULL(pMsg->pStreamStatus, code, lino, _exit, terrno);
  }

  int32_t origTaskNum = taosArrayGetSize(pMsg->pStreamStatus);

  if (pStream->readerList) {
    tdListInitIter(pStream->readerList, &iter, TD_LIST_FORWARD);
    while ((listNode = tdListNext(&iter)) != NULL) {
      SStreamReaderTask* pReader = (SStreamReaderTask*)listNode->data;
      pTask = (SStreamTask*)pReader;
      TAOS_CHECK_EXIT(stmPushTaskStatus(pMsg, &pReader->task, pReader->pStats));
      //if (pReader->task.pMgmtReq) {
      //  TAOS_CHECK_EXIT(stmAddMgmtReq(streamId, &pMsg->pStreamReq, taosArrayGetSize(pMsg->pStreamStatus) - 1));
      //}
      ST_TASK_DLOG("task status added to hb %s mgmtReq", pReader->task.pMgmtReq ? "with" : "without");
    }

    stsDebug("%d reader tasks status added to hb", TD_DLIST_NELES(pStream->readerList));
  }

  if (pStream->triggerList && (TD_DLIST_NELES(pStream->triggerList) > 0)) {
    listNode = TD_DLIST_HEAD(pStream->triggerList);
    pTask = (SStreamTask*)listNode->data;
    if (reportPeriod) {
      TAOS_CHECK_EXIT(stmAddPeriodReport(streamId, &pMsg->pTriggerStatus, (SStreamTriggerTask*)pTask));
      pTask->detailStatus = taosArrayGetSize(pMsg->pTriggerStatus) - 1;
    } else {
      pTask->detailStatus = -1;
    }

    SStreamTriggerTask* pTrigger = (SStreamTriggerTask*)pTask;
    TAOS_CHECK_EXIT(stmHbAddTaskStatus(streamId, pMsg, pTask, pTrigger->pStats));

    ST_TASK_DLOG("task status added to hb %s mgmtReq", pTask->pMgmtReq ? "with" : "without");
    stsDebug("%d trigger tasks status added to hb", 1);
  }

  if (pStream->runnerList) {
    memset(&iter, 0, sizeof(iter));

    tdListInitIter(pStream->runnerList, &iter, TD_LIST_FORWARD);
    while ((listNode = tdListNext(&iter)) != NULL) {
      SStreamRunnerTask* pRunner = (SStreamRunnerTask*)listNode->data;
      pTask = (SStreamTask*)pRunner;
      if (atomic_val_compare_exchange_8(&pRunner->vtableDeployGot, 1, 0)) {
        TAOS_CHECK_EXIT(stRunnerBuildTaskMgmtReq(pRunner));
        TAOS_CHECK_EXIT(stmHbAddTaskStatus(streamId, pMsg, pTask, pRunner->pStats));
      } else {
        TAOS_CHECK_EXIT(stmPushTaskStatus(pMsg, &pRunner->task, pRunner->pStats));
      }
      ST_TASK_DLOG("task status added to hb %s mgmtReq", pRunner->task.pMgmtReq ? "with" : "without");
    }

    stsDebug("%d runner tasks status added to hb", TD_DLIST_NELES(pStream->runnerList));
  }

  stsDebug("total %d:%d tasks status added to hb", (int32_t)taosArrayGetSize(pMsg->pStreamStatus) - origTaskNum,
           pStream->taskNum);

_exit:

  taosWUnLockLatch(&pStream->lock);

  if (code) {
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t stmBuildHbStreamsStatusReq(SStreamHbMsg* pMsg) {
  static bool reportPeriod = true;

  int32_t metricCode = stmInitTaskMetrics(pMsg, 0);
  if (metricCode != TSDB_CODE_SUCCESS) {
    stError("failed to initialize task metrics in heartbeat: %s", tstrerror(metricCode));
  }

  if (0 == pMsg->streamGId) {
    reportPeriod = !reportPeriod;
  }

  stDebug("start to build hb status req, gid:%d", pMsg->streamGId);

  SHashObj* pHash = gStreamMgmt.stmGrp[pMsg->streamGId];
  if (NULL == pHash) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  void*   pIter = NULL;
  while (true) {
    pIter = taosHashIterate(pHash, pIter);
    if (NULL == pIter) {
      break;
    }

    SStreamInfo* pStream = (SStreamInfo*)pIter;
    int64_t*     streamId = taosHashGetKey(pIter, NULL);

    (void)stmHbAddStreamStatus(pMsg, pStream, *streamId, reportPeriod);
  }

  return code;
}

static void stmClearTaskExtraErrMsg(SStreamTask* pTask) {
  if (pTask != NULL) {
    taosMemoryFreeClear(pTask->extraErrMsg);
  }
}

void stmDestroySStreamInfo(void* param) {
  if (NULL == param) {
    return;
  }

  stDebug("start to destroy stream info");

  SStreamInfo* p = (SStreamInfo*)param;

  SListIter  iter = {0};
  SListNode* listNode = NULL;
  tdListInitIter(p->readerList, &iter, TD_LIST_FORWARD);
  while ((listNode = tdListNext(&iter)) != NULL) {
    SStreamTask* pTask = (SStreamTask*)listNode->data;
    streamTaskStatsHandleLifecycle(streamTaskGetStatsSlot(pTask), STREAM_TASK_STATS_OWNER_DESTROYED);
    stmClearTaskExtraErrMsg(pTask);
    SListNode* tmp = tdListPopNode(p->readerList, listNode);
    ST_TASK_DLOG("task removed from stream readerList, remain:%d, listNode:%p", TD_DLIST_NELES(p->readerList), tmp);
    taosMemoryFreeClear(tmp);
  }
  p->readerList = tdListFree(p->readerList);

  memset(&iter, 0, sizeof(iter));
  tdListInitIter(p->triggerList, &iter, TD_LIST_FORWARD);
  while ((listNode = tdListNext(&iter)) != NULL) {
    SStreamTask* pTask = (SStreamTask*)listNode->data;
    streamTaskStatsHandleLifecycle(streamTaskGetStatsSlot(pTask), STREAM_TASK_STATS_OWNER_DESTROYED);
    stmClearTaskExtraErrMsg(pTask);
    SListNode* tmp = tdListPopNode(p->triggerList, listNode);
    ST_TASK_DLOG("task removed from stream triggerList, remain:%d", TD_DLIST_NELES(p->triggerList));
    taosMemoryFreeClear(tmp);
  }
  p->triggerList = tdListFree(p->triggerList);

  memset(&iter, 0, sizeof(iter));
  tdListInitIter(p->runnerList, &iter, TD_LIST_FORWARD);
  while ((listNode = tdListNext(&iter)) != NULL) {
    SStreamTask* pTask = (SStreamTask*)listNode->data;
    streamTaskStatsHandleLifecycle(streamTaskGetStatsSlot(pTask), STREAM_TASK_STATS_OWNER_DESTROYED);
    stmClearTaskExtraErrMsg(pTask);
    SListNode* tmp = tdListPopNode(p->runnerList, listNode);
    ST_TASK_DLOG("task removed from stream runnerList, remain:%d", TD_DLIST_NELES(p->runnerList));
    taosMemoryFreeClear(tmp);
  }
  p->runnerList = tdListFree(p->runnerList);

  taosArrayDestroy(p->undeployReaders);
  p->undeployReaders = NULL;
  taosArrayDestroy(p->undeployTriggers);
  p->undeployTriggers = NULL;
  taosArrayDestroy(p->undeployRunners);
  p->undeployRunners = NULL;
}

/*
 * JSON_CHECK_ADD_ITEM      — on failure, caller must free item in _end cleanup.
 * JSON_CHECK_ADD_ITEM_SAFE — on failure, frees and NULLs itemVar inside macro.
 */
#define JSON_CHECK_ADD_ITEM(obj, str, item) \
  QUERY_CHECK_CONDITION(cJSON_AddItemToObjectCS(obj, str, item), code, lino, _end, TSDB_CODE_OUT_OF_MEMORY)

#define JSON_CHECK_ADD_ITEM_SAFE(obj, str, itemVar)          \
  do {                                                       \
    if (!cJSON_AddItemToObjectCS((obj), (str), (itemVar))) { \
      cJSON_Delete(itemVar);                                 \
      (itemVar) = NULL;                                      \
      code = TSDB_CODE_OUT_OF_MEMORY;                        \
      lino = __LINE__;                                       \
      goto _end;                                             \
    }                                                        \
  } while (0)

#define JSON_CHECK_ADD_ARRAY_ITEM(arr, itemVar)    \
  do {                                             \
    if (!cJSON_AddItemToArray((arr), (itemVar))) { \
      cJSON_Delete(itemVar);                       \
      (itemVar) = NULL;                            \
      code = TSDB_CODE_OUT_OF_MEMORY;              \
      lino = __LINE__;                             \
      goto _end;                                   \
    }                                              \
  } while (0)

static int32_t jsonCreateColumnValue(const SColumnInfo* colInfo, bool isNull, const char* pData, cJSON** ppItem) {
  int8_t  type = colInfo->type;
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  char*   temp = NULL;

  *ppItem = NULL;
  QUERY_CHECK_CONDITION(isNull || (pData != NULL), code, lino, _end, TSDB_CODE_INVALID_PARA);

  if (isNull) {
    *ppItem = cJSON_CreateNull();
    QUERY_CHECK_NULL(*ppItem, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
    goto _end;
  }

  switch (type) {
    case TSDB_DATA_TYPE_BOOL: {
      bool val = *(const bool*)pData;
      *ppItem = cJSON_CreateBool(val);
      QUERY_CHECK_NULL(*ppItem, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
      break;
    }

    case TSDB_DATA_TYPE_TINYINT: {
      int8_t val = *(const int8_t*)pData;
      *ppItem = cJSON_CreateNumber(val);
      QUERY_CHECK_NULL(*ppItem, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
      break;
    }

    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t val = *(const int16_t*)pData;
      *ppItem = cJSON_CreateNumber(val);
      QUERY_CHECK_NULL(*ppItem, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
      break;
    }

    case TSDB_DATA_TYPE_INT: {
      int32_t val = *(const int32_t*)pData;
      *ppItem = cJSON_CreateNumber(val);
      QUERY_CHECK_NULL(*ppItem, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
      break;
    }

    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP: {
      int64_t val = *(const int64_t*)pData;
      *ppItem = cJSON_CreateNumber(val);
      QUERY_CHECK_NULL(*ppItem, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
      break;
    }

    case TSDB_DATA_TYPE_FLOAT: {
      float val = *(const float*)pData;
      *ppItem = cJSON_CreateNumber(val);
      QUERY_CHECK_NULL(*ppItem, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
      break;
    }

    case TSDB_DATA_TYPE_DOUBLE: {
      double val = *(const double*)pData;
      *ppItem = cJSON_CreateNumber(val);
      QUERY_CHECK_NULL(*ppItem, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
      break;
    }

    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_NCHAR: {
      // cJSON requires null-terminated strings, but this data is not null-terminated,
      // so we need to manually copy the string and add null termination.
      const char* src = varDataVal(pData);
      int32_t     len = varDataLen(pData);
      temp = cJSON_malloc(len + 1);
      QUERY_CHECK_NULL(temp, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
      memcpy(temp, src, len);
      temp[len] = '\0';

      *ppItem = cJSON_CreateStringReference(temp);
      QUERY_CHECK_NULL(*ppItem, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);

      // let the cjson object to free memory later
      (*ppItem)->type &= ~cJSON_IsReference;
      temp = NULL;
      break;
    }

    case TSDB_DATA_TYPE_UTINYINT: {
      uint8_t val = *(const uint8_t*)pData;
      *ppItem = cJSON_CreateNumber(val);
      QUERY_CHECK_NULL(*ppItem, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
      break;
    }

    case TSDB_DATA_TYPE_USMALLINT: {
      uint16_t val = *(const uint16_t*)pData;
      *ppItem = cJSON_CreateNumber(val);
      QUERY_CHECK_NULL(*ppItem, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
      break;
    }

    case TSDB_DATA_TYPE_UINT: {
      uint32_t val = *(const uint32_t*)pData;
      *ppItem = cJSON_CreateNumber(val);
      QUERY_CHECK_NULL(*ppItem, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
      break;
    }

    case TSDB_DATA_TYPE_UBIGINT: {
      uint64_t val = *(const uint64_t*)pData;
      *ppItem = cJSON_CreateNumber(val);
      QUERY_CHECK_NULL(*ppItem, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
      break;
    }

    case TSDB_DATA_TYPE_DECIMAL64:
    case TSDB_DATA_TYPE_DECIMAL: {
      Decimal128* pIn = (Decimal128*)pData;
      uint8_t     inputPrec = colInfo->precision;
      uint8_t     inputScale = colInfo->scale;

      const int32_t len = 64;
      temp = cJSON_malloc(len + 1);
      QUERY_CHECK_NULL(temp, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
      QUERY_CHECK_CODE(decimalToStr(pData, colInfo->type, inputPrec, inputScale, temp, len), lino, _end);
      temp[len] = '\0';

      *ppItem = cJSON_CreateStringReference(temp);
      QUERY_CHECK_NULL(*ppItem, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);

      // let the cjson object to free memory later
      (*ppItem)->type &= ~cJSON_IsReference;
      temp = NULL;
      break;
    }

    default: {
      *ppItem = cJSON_CreateStringReference("<Unable to display this data type>");
      QUERY_CHECK_NULL(*ppItem, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
      break;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (temp) {
    cJSON_free(temp);
  }
  return code;
}

static int32_t jsonAddColumnField(const char* colName, const SColumnInfo* colInfo, bool isNull, const char* pData,
                                  cJSON* obj) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  cJSON*  item = NULL;

  QUERY_CHECK_NULL(colName, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(obj, code, lino, _end, TSDB_CODE_INVALID_PARA);

  code = jsonCreateColumnValue(colInfo, isNull, pData, &item);
  QUERY_CHECK_CODE(code, lino, _end);
  JSON_CHECK_ADD_ITEM(obj, colName, item);
  item = NULL;

_end:
  if (item != NULL) {
    cJSON_Delete(item);
  }
  if (code != TSDB_CODE_SUCCESS) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t jsonAddNullField(const char* fieldName, cJSON* obj) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  cJSON*  item = NULL;

  QUERY_CHECK_NULL(fieldName, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(obj, code, lino, _end, TSDB_CODE_INVALID_PARA);

  item = cJSON_CreateNull();
  QUERY_CHECK_NULL(item, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  JSON_CHECK_ADD_ITEM(obj, fieldName, item);
  item = NULL;

_end:
  if (item != NULL) {
    cJSON_Delete(item);
  }
  if (code != TSDB_CODE_SUCCESS) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t jsonAddStateArrayField(const char* fieldName, const SArray* pStateCols, const SArray* pStateVals,
                                      const bool* pDefined, cJSON* obj) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  cJSON*  arr = NULL;

  QUERY_CHECK_NULL(fieldName, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pStateCols, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(obj, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(
      pStateVals == NULL || taosArrayGetSize((SArray*)pStateVals) == taosArrayGetSize((SArray*)pStateCols), code, lino,
      _end, TSDB_CODE_INVALID_PARA);

  arr = cJSON_CreateArray();
  QUERY_CHECK_NULL(arr, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  JSON_CHECK_ADD_ITEM_SAFE(obj, fieldName, arr);

  for (int32_t i = 0; i < taosArrayGetSize((SArray*)pStateCols); ++i) {
    cJSON*           item = NULL;
    SColumnInfoData* pCol = *(SColumnInfoData**)taosArrayGet((SArray*)pStateCols, i);
    QUERY_CHECK_NULL(pCol, code, lino, _end, TSDB_CODE_INVALID_PARA);

    if (pStateVals == NULL) {
      code = jsonCreateColumnValue(&pCol->info, true, NULL, &item);
    } else {
      SValue* pVal = taosArrayGet((SArray*)pStateVals, i);
      QUERY_CHECK_NULL(pVal, code, lino, _end, TSDB_CODE_INVALID_PARA);
      bool isNull = (pDefined != NULL) ? !pDefined[i] : (pVal->type == TSDB_DATA_TYPE_NULL);
      code = jsonCreateColumnValue(&pCol->info, isNull, isNull ? NULL : VALUE_GET_DATUM(pVal, pVal->type), &item);
    }
    QUERY_CHECK_CODE(code, lino, _end);
    JSON_CHECK_ADD_ARRAY_ITEM(arr, item);
  }

_end:
  if (code != TSDB_CODE_SUCCESS && arr != NULL) {
    cJSON_DeleteItemFromObjectCaseSensitive(obj, fieldName);
  }
  if (code != TSDB_CODE_SUCCESS) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

/* Build JSON notify content for multi-column state transitions. */
int32_t streamBuildMultiStateNotifyContent(ESTriggerEventType eventType, const SArray* pStateCols,
                                           const SArray* pFromStates, const bool* pFromDefined, const SArray* pToStates,
                                           const bool* pToDefined, char** ppContent) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  cJSON*  obj = NULL;

  *ppContent = NULL;

  obj = cJSON_CreateObject();
  QUERY_CHECK_NULL(obj, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  if (eventType == STRIGGER_EVENT_WINDOW_OPEN) {
    if (pFromStates == NULL) {
      code = jsonAddNullField("prevState", obj);
    } else {
      code = jsonAddStateArrayField("prevState", pStateCols, pFromStates, pFromDefined, obj);
    }
    QUERY_CHECK_CODE(code, lino, _end);
    code = jsonAddStateArrayField("curState", pStateCols, pToStates, pToDefined, obj);
    QUERY_CHECK_CODE(code, lino, _end);
  } else if (eventType == STRIGGER_EVENT_WINDOW_CLOSE) {
    code = jsonAddStateArrayField("curState", pStateCols, pFromStates, pFromDefined, obj);
    QUERY_CHECK_CODE(code, lino, _end);
    code = jsonAddStateArrayField("nextState", pStateCols, pToStates, pToDefined, obj);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  *ppContent = cJSON_PrintUnformatted(obj);
  QUERY_CHECK_NULL(*ppContent, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);

_end:
  if (obj != NULL) {
    cJSON_Delete(obj);
  }
  if (code != TSDB_CODE_SUCCESS) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t streamBuildIdleNotifyContent(ESTriggerEventType eventType, int64_t idleDurationMs, char** ppContent) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  cJSON*  obj = NULL;

  *ppContent = NULL;

  QUERY_CHECK_CONDITION(eventType == STRIGGER_EVENT_IDLE || eventType == STRIGGER_EVENT_RESUME, code, lino, _end,
                        TSDB_CODE_INVALID_PARA);

  obj = cJSON_CreateObject();
  QUERY_CHECK_NULL(obj, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  QUERY_CHECK_NULL(cJSON_AddNumberToObject(obj, "idleDurationMs", idleDurationMs), code, lino, _end,
                   TSDB_CODE_OUT_OF_MEMORY);

  *ppContent = cJSON_PrintUnformatted(obj);
  QUERY_CHECK_NULL(*ppContent, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);

_end:
  if (obj != NULL) {
    cJSON_Delete(obj);
  }
  if (code != TSDB_CODE_SUCCESS) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t streamBuildEventNotifyContent(const SSDataBlock* pInputBlock, const SNodeList* pCondCols, int32_t rowIdx,
                                      const char* conditionPath, const SArray* parentTriggerIds, int64_t groupId,
                                      int64_t windowStart, char** ppContent) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  const SNode* pNode = NULL;
  cJSON*       obj = NULL;
  cJSON*       cond = NULL;
  cJSON*       fields = NULL;
  cJSON*       parents = NULL;

  *ppContent = NULL;

  fields = cJSON_CreateObject();
  QUERY_CHECK_NULL(fields, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  FOREACH(pNode, pCondCols) {
    const SColumnNode*     pColDef = (const SColumnNode*)pNode;
    const SColumnInfoData* pColData = taosArrayGet(pInputBlock->pDataBlock, pColDef->slotId);
    code = jsonAddColumnField(pColDef->colName, &pColData->info, colDataIsNull_s(pColData, rowIdx),
                              colDataGetData(pColData, rowIdx), fields);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  cond = cJSON_CreateObject();
  QUERY_CHECK_NULL(cond, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  JSON_CHECK_ADD_ITEM(cond, "conditionPath", cJSON_CreateString(conditionPath != NULL ? conditionPath : ""));
  JSON_CHECK_ADD_ITEM(cond, "fieldValues", fields);
  fields = NULL;

  obj = cJSON_CreateObject();
  QUERY_CHECK_NULL(obj, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  char triggerId[32];
  code = streamBuildNotifyTriggerId(groupId, windowStart, conditionPath, triggerId);
  QUERY_CHECK_CODE(code, lino, _end);
  JSON_CHECK_ADD_ITEM(obj, "triggerId", cJSON_CreateString(triggerId));
  JSON_CHECK_ADD_ITEM(obj, "triggerCondition", cond);
  parents = cJSON_CreateArray();
  QUERY_CHECK_NULL(parents, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  int32_t parentNum = (parentTriggerIds != NULL) ? taosArrayGetSize(parentTriggerIds) : 0;
  for (int32_t i = 0; i < parentNum; ++i) {
    const char* parentId = (const char*)taosArrayGet(parentTriggerIds, i);
    cJSON*      parent = cJSON_CreateString(parentId);
    QUERY_CHECK_NULL(parent, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
    JSON_CHECK_ADD_ARRAY_ITEM(parents, parent);
  }
  JSON_CHECK_ADD_ITEM(obj, "parentTriggerId", parents);
  parents = NULL;
  cond = NULL;

  *ppContent = cJSON_PrintUnformatted(obj);
  QUERY_CHECK_NULL(*ppContent, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);

_end:
  if (fields != NULL) {
    cJSON_Delete(fields);
  }
  if (cond != NULL) {
    cJSON_Delete(cond);
  }
  if (parents != NULL) {
    cJSON_Delete(parents);
  }
  if (obj != NULL) {
    cJSON_Delete(obj);
  }
  if (code != TSDB_CODE_SUCCESS) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t streamBuildBlockResultNotifyContent(const SStreamRunnerTask* pTask, const SSDataBlock* pBlock, char** ppContent,
                                            const SArray* pFields, const int32_t startRow, const int32_t endRow,
                                            bool* pHasNotifyRows) {
  int32_t code = 0, lino = 0;
  cJSON*  pContent = NULL;
  cJSON*  pResult = NULL;
  cJSON*  pRow = NULL;
  bool    hasNotifyFilter = (pTask->addOptions & NOTIFY_HAS_FILTER) != 0;
  int32_t filteredRows = 0;
  int32_t curSize = endRow - startRow + 1;

  if (pHasNotifyRows != NULL) {
    *pHasNotifyRows = !hasNotifyFilter;
  }

  pResult = cJSON_CreateObject();
  QUERY_CHECK_NULL(pResult, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);

  cJSON* pArr = cJSON_AddArrayToObject(pResult, "data");
  QUERY_CHECK_NULL(pArr, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);

  if (pBlock && pBlock->info.rows > 0) {
    int32_t          realCols = taosArrayGetSize(pBlock->pDataBlock);
    SColumnInfoData* pFilterCol = NULL;
    if (hasNotifyFilter) {
      realCols -= 1;
      pFilterCol = taosArrayGet(pBlock->pDataBlock, realCols);
      if (pFilterCol->info.type != TSDB_DATA_TYPE_BOOL) {
        stError("invalid filter column type: %d", pFilterCol->info.type);
        code = TSDB_CODE_INVALID_PARA;
        goto _end;
      }
    }

    for (int32_t rowIdx = startRow; rowIdx <= endRow && rowIdx < pBlock->info.rows; ++rowIdx) {
      if (pFilterCol != NULL) {
        if (colDataIsNull_s(pFilterCol, rowIdx)) {
          continue;
        }
        bool filter = *(bool*)colDataGetData(pFilterCol, rowIdx);
        if (!filter) {
          continue;
        }
      }
      pRow = cJSON_CreateObject();
      QUERY_CHECK_NULL(pRow, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);

      for (int32_t colIdx = 0; colIdx < realCols; ++colIdx) {
        const SColumnInfoData*   pCol = taosArrayGet(pBlock->pDataBlock, colIdx);
        const SFieldWithOptions* pField = taosArrayGet(pFields, colIdx);
        const char*              colName = "unknown";
        if (!pField) {
          stError("failed to get field name for notification, colIdx: %d, fields arr size: %" PRId64, colIdx,
                  (int64_t)taosArrayGetSize(pFields));
          continue;
        }
        colName = pField->name;
        bool isNull = colDataIsNull_s(pCol, rowIdx);
        code = jsonAddColumnField(colName, &pCol->info, isNull, isNull ? NULL : colDataGetData(pCol, rowIdx), pRow);
        QUERY_CHECK_CODE(code, lino, _end);
      }

      TSDB_CHECK_CONDITION(cJSON_AddItemToArray(pArr, pRow), code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
      filteredRows++;
      pRow = NULL;
    }
  }

  if (hasNotifyFilter) {
    curSize = filteredRows;
    if (pHasNotifyRows != NULL) {
      *pHasNotifyRows = (filteredRows > 0);
    }
  }

  cJSON* size = cJSON_CreateNumber(curSize);
  QUERY_CHECK_NULL(size, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  JSON_CHECK_ADD_ITEM_SAFE(pResult, "curSize", size);

  cJSON* offset = cJSON_CreateNumber(0);
  QUERY_CHECK_NULL(offset, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  JSON_CHECK_ADD_ITEM_SAFE(pResult, "curOffset", offset);

  cJSON* finish = cJSON_CreateTrue();
  QUERY_CHECK_NULL(finish, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  JSON_CHECK_ADD_ITEM_SAFE(pResult, "finish", finish);

  pContent = cJSON_CreateObject();
  QUERY_CHECK_NULL(pContent, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  JSON_CHECK_ADD_ITEM(pContent, "result", pResult);
  pResult = NULL;
  *ppContent = cJSON_PrintUnformatted(pContent);
  QUERY_CHECK_NULL(*ppContent, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);

_end:
  if (pRow) cJSON_Delete(pRow);
  if (pResult) cJSON_Delete(pResult);
  if (pContent) cJSON_Delete(pContent);
  if (code) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t streamAppendNotifyHeader(const char* streamName, SStringBuilder* pBuilder) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  cJSON*  stream = NULL;
  cJSON*  streams = NULL;
  cJSON*  obj = NULL;
  char*   temp = NULL;

  char msgId[37];
  code = taosGetSystemUUIDLimit36(msgId, sizeof(msgId));
  QUERY_CHECK_CODE(code, lino, _end);

  stream = cJSON_CreateObject();
  QUERY_CHECK_NULL(stream, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  JSON_CHECK_ADD_ITEM(stream, "streamName", cJSON_CreateStringReference(streamName));
  JSON_CHECK_ADD_ITEM(stream, "events", cJSON_CreateArray());

  streams = cJSON_CreateArray();
  QUERY_CHECK_NULL(streams, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  TSDB_CHECK_CONDITION(cJSON_AddItemToArray(streams, stream), code, lino, _end, TSDB_CODE_OUT_OF_MEMORY)
  stream = NULL;

  obj = cJSON_CreateObject();
  QUERY_CHECK_NULL(obj, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  JSON_CHECK_ADD_ITEM(obj, "messageId", cJSON_CreateStringReference(msgId));
  JSON_CHECK_ADD_ITEM(obj, "timestamp", cJSON_CreateNumber(taosGetTimestampMs()));
  JSON_CHECK_ADD_ITEM(obj, "streams", streams);
  streams = NULL;

  temp = cJSON_PrintUnformatted(obj);
  QUERY_CHECK_NULL(temp, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  taosStringBuilderAppendString(pBuilder, temp);

_end:
  if (temp != NULL) {
    cJSON_free(temp);
  }
  if (obj != NULL) {
    cJSON_Delete(obj);
  }
  if (streams != NULL) {
    cJSON_Delete(streams);
  }
  if (stream != NULL) {
    cJSON_Delete(stream);
  }
  if (code != TSDB_CODE_SUCCESS) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t streamBuildNotifyTriggerId(int64_t groupId, int64_t windowStart, const char* conditionPath, char* triggerId) {
  size_t pathLen = (conditionPath != NULL) ? strlen(conditionPath) : 0;
  size_t bufLen = sizeof(uint64_t) + sizeof(uint64_t) + pathLen;
  char*  buf = taosMemoryMalloc(bufLen);
  if (buf == NULL) {
    return terrno;
  }
  size_t   pos = 0;
  uint64_t group = (uint64_t)groupId;
  uint64_t start = (uint64_t)windowStart;
  TAOS_MEMCPY(buf + pos, &group, sizeof(group));
  pos += sizeof(group);
  TAOS_MEMCPY(buf + pos, &start, sizeof(start));
  pos += sizeof(start);
  if (pathLen > 0) {
    TAOS_MEMCPY(buf + pos, conditionPath, pathLen);
    pos += pathLen;
  }
  uint64_t hash = MurmurHash3_64(buf, (uint32_t)pos);
  taosMemoryFree(buf);
  (void)u64toaFastLut(hash, triggerId);
  return TSDB_CODE_SUCCESS;
}

static int64_t streamNotifyTsToMs(int64_t ts, int8_t precision) {
  if (precision == TSDB_TIME_PRECISION_MILLI) {
    return ts;
  }
  return convertTimePrecision(ts, precision, TSDB_TIME_PRECISION_MILLI);
}

static int64_t streamNotifyWallNsToMs(int64_t wallNs) { return wallNs / 1000000LL; }

static int32_t streamAppendNotifyContent(int32_t triggerType, int64_t groupId, const SSTriggerCalcParam* pParam,
                                         SStringBuilder* pBuilder, const char* tableName, int8_t precision) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  cJSON*  obj = NULL;
  char*   temp = NULL;

  const char* eventType = NULL;
  if (pParam->notifyType == STRIGGER_EVENT_WINDOW_OPEN) {
    eventType = "WINDOW_OPEN";
  } else if (pParam->notifyType == STRIGGER_EVENT_WINDOW_CLOSE) {
    eventType = "WINDOW_CLOSE";
  } else if (pParam->notifyType == STRIGGER_EVENT_IDLE) {
    eventType = "IDLE";
  } else if (pParam->notifyType == STRIGGER_EVENT_RESUME) {
    eventType = "RESUME";
  } else if (pParam->notifyType == STRIGGER_EVENT_ON_TIME) {
    eventType = "ON_TIME";
  }

  char triggerId[32];
  bool hasEventTriggerId =
      triggerType == STREAM_TRIGGER_EVENT &&
      (pParam->notifyType == STRIGGER_EVENT_WINDOW_OPEN || pParam->notifyType == STRIGGER_EVENT_WINDOW_CLOSE) &&
      pParam->extraNotifyContent != NULL;
  if (!hasEventTriggerId) {
    code = streamBuildNotifyTriggerId(groupId, pParam->wstart, "", triggerId);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  const char* triggerTypeStr = NULL;
  switch (triggerType) {
    case STREAM_TRIGGER_PERIOD:
      triggerTypeStr = "Period";
      break;
    case STREAM_TRIGGER_SLIDING:
      triggerTypeStr = (pParam->notifyType == STRIGGER_EVENT_ON_TIME) ? "Sliding" : "Interval";
      break;
    case STREAM_TRIGGER_SESSION:
      triggerTypeStr = "Session";
      break;
    case STREAM_TRIGGER_COUNT:
      triggerTypeStr = "Count";
      break;
    case STREAM_TRIGGER_STATE:
      triggerTypeStr = "State";
      break;
    case STREAM_TRIGGER_EVENT:
      triggerTypeStr = "Event";
      break;
    default:
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
  }

  obj = cJSON_CreateObject();
  QUERY_CHECK_NULL(obj, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  JSON_CHECK_ADD_ITEM(obj, "eventType", cJSON_CreateStringReference(eventType));
  // eventTime: per-event data-domain time in milliseconds (unique across events).
  int64_t eventTimeMs;
  if (pParam->notifyType == STRIGGER_EVENT_IDLE || pParam->notifyType == STRIGGER_EVENT_RESUME) {
    // idlestart is wall-clock ns (union alias of wstart).
    eventTimeMs = streamNotifyWallNsToMs(pParam->idlestart);
  } else if (pParam->notifyType != STRIGGER_EVENT_ON_TIME) {
    eventTimeMs = streamNotifyTsToMs(pParam->wstart, precision);
  } else if (triggerType == STREAM_TRIGGER_PERIOD) {
    // Period ON_TIME uses taosGetTimestampNs() wall clock, not table precision.
    eventTimeMs = streamNotifyWallNsToMs(pParam->triggerTime);
  } else if (triggerType == STREAM_TRIGGER_SLIDING) {
    eventTimeMs = streamNotifyTsToMs(pParam->prevTs, precision);
  } else {
    eventTimeMs = streamNotifyWallNsToMs(pParam->triggerTime);
  }
  JSON_CHECK_ADD_ITEM(obj, "eventTime", cJSON_CreateNumber(eventTimeMs));
  if (!hasEventTriggerId) {
    JSON_CHECK_ADD_ITEM(obj, "triggerId", cJSON_CreateStringReference(triggerId));
  }
  JSON_CHECK_ADD_ITEM(obj, "triggerType", cJSON_CreateStringReference(triggerTypeStr));

  if (tableName != NULL) {
    JSON_CHECK_ADD_ITEM(obj, "tableName", cJSON_CreateStringReference(tableName));
  }

  char gidBuf[32];
  snprintf(gidBuf, sizeof(gidBuf), "%" PRId64, groupId);
  JSON_CHECK_ADD_ITEM(obj, "groupId", cJSON_CreateString(gidBuf));

  if (pParam->notifyType == STRIGGER_EVENT_IDLE || pParam->notifyType == STRIGGER_EVENT_RESUME) {
    JSON_CHECK_ADD_ITEM(obj, "idleStart", cJSON_CreateNumber(streamNotifyWallNsToMs(pParam->idlestart)));
    JSON_CHECK_ADD_ITEM(obj, "idleEnd", cJSON_CreateNumber(streamNotifyWallNsToMs(pParam->idleend)));
  } else if (pParam->notifyType != STRIGGER_EVENT_ON_TIME) {
    JSON_CHECK_ADD_ITEM(obj, "windowStart", cJSON_CreateNumber(streamNotifyTsToMs(pParam->wstart, precision)));
    if (pParam->notifyType == STRIGGER_EVENT_WINDOW_CLOSE) {
      int64_t wend = streamNotifyTsToMs(pParam->wend, precision);
      JSON_CHECK_ADD_ITEM(obj, "windowEnd", cJSON_CreateNumber(wend));
    }
  } else if (triggerType == STREAM_TRIGGER_PERIOD) {
    JSON_CHECK_ADD_ITEM(obj, "windowStart", cJSON_CreateNumber(streamNotifyWallNsToMs(pParam->triggerTime)));
    JSON_CHECK_ADD_ITEM(obj, "windowEnd", cJSON_CreateNumber(streamNotifyWallNsToMs(pParam->triggerTime)));
  } else if (triggerType == STREAM_TRIGGER_SLIDING) {
    JSON_CHECK_ADD_ITEM(obj, "windowStart", cJSON_CreateNumber(streamNotifyTsToMs(pParam->prevTs, precision)));
    JSON_CHECK_ADD_ITEM(obj, "windowEnd", cJSON_CreateNumber(streamNotifyTsToMs(pParam->currentTs, precision)));
  }

  temp = cJSON_PrintUnformatted(obj);
  QUERY_CHECK_NULL(temp, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  taosStringBuilderAppendString(pBuilder, temp);

  if (pParam->extraNotifyContent != NULL) {
    pBuilder->pos -= 1;
    taosStringBuilderAppendChar(pBuilder, ',');
    taosStringBuilderAppendStringLen(pBuilder, pParam->extraNotifyContent + 1, strlen(pParam->extraNotifyContent) - 1);
  }

  if (pParam->resultNotifyContent != NULL) {
    pBuilder->pos -= 1;
    taosStringBuilderAppendChar(pBuilder, ',');
    taosStringBuilderAppendStringLen(pBuilder, pParam->resultNotifyContent + 1,
                                     strlen(pParam->resultNotifyContent) - 1);
  }

_end:
  if (temp != NULL) {
    cJSON_free(temp);
  }
  if (obj != NULL) {
    cJSON_Delete(obj);
  }
  if (code != TSDB_CODE_SUCCESS) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

#define STREAM_EVENT_NOTIFY_RETRY_MS 50  // 50 ms

// Build JSON from pending items (uses per-item tableName when present).
static int32_t streamBuildNotifyMsgFromItems(const char* streamName, int32_t triggerType, int64_t groupId,
                                             int8_t precision, SStreamNotifyPendingItem** ppItems, int32_t nItems,
                                             char** ppMsg, size_t* pMsgLen) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SStringBuilder sb = {0};
  const char*    msgTail = "]}]}";
  bool           shouldNotify = false;

  *ppMsg = NULL;
  *pMsgLen = 0;

  for (int32_t i = 0; i < nItems; ++i) {
    if (ppItems[i]->param.notifyType != STRIGGER_EVENT_WINDOW_NONE) {
      shouldNotify = true;
      break;
    }
  }
  if (!shouldNotify) {
    goto _end;
  }

  size_t msgTailLen = strlen(msgTail);
  taosStringBuilderEnsureCapacity(&sb, 4096);

  code = streamAppendNotifyHeader(streamName, &sb);
  QUERY_CHECK_CODE(code, lino, _end);
  sb.pos -= msgTailLen;
  for (int32_t i = 0; i < nItems; ++i) {
    if (ppItems[i]->param.notifyType == STRIGGER_EVENT_WINDOW_NONE) {
      continue;
    }
    code = streamAppendNotifyContent(triggerType, groupId, &ppItems[i]->param, &sb, ppItems[i]->tableName, precision);
    QUERY_CHECK_CODE(code, lino, _end);
    taosStringBuilderAppendChar(&sb, ',');
  }
  sb.pos -= 1;
  taosStringBuilderAppendStringLen(&sb, msgTail, msgTailLen);

  taosStringBuilderGetResult(&sb, NULL);
  *ppMsg = sb.buf;
  *pMsgLen = sb.pos;
  sb.buf = NULL;

_end:
  taosStringBuilderDestroy(&sb);
  if (code != TSDB_CODE_SUCCESS) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

// Build JSON from an array of param pointers (zero-copy; params must outlive the call).
static int32_t streamBuildNotifyMsgFromPtrs(const char* streamName, const char* tableName, int32_t triggerType,
                                            int64_t groupId, int8_t precision,
                                            const SSTriggerCalcParam* const* ppParams, int32_t nParam, char** ppMsg,
                                            size_t* pMsgLen) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SStringBuilder sb = {0};
  const char*    msgTail = "]}]}";
  bool           shouldNotify = false;

  *ppMsg = NULL;
  *pMsgLen = 0;

  for (int32_t i = 0; i < nParam; ++i) {
    if (ppParams[i]->notifyType != STRIGGER_EVENT_WINDOW_NONE) {
      shouldNotify = true;
      break;
    }
  }
  if (!shouldNotify) {
    goto _end;
  }

  size_t msgTailLen = strlen(msgTail);
  taosStringBuilderEnsureCapacity(&sb, 4096);

  code = streamAppendNotifyHeader(streamName, &sb);
  QUERY_CHECK_CODE(code, lino, _end);
  sb.pos -= msgTailLen;
  for (int32_t i = 0; i < nParam; ++i) {
    if (ppParams[i]->notifyType == STRIGGER_EVENT_WINDOW_NONE) {
      continue;
    }
    code = streamAppendNotifyContent(triggerType, groupId, ppParams[i], &sb, tableName, precision);
    QUERY_CHECK_CODE(code, lino, _end);
    taosStringBuilderAppendChar(&sb, ',');
  }
  sb.pos -= 1;
  taosStringBuilderAppendStringLen(&sb, msgTail, msgTailLen);

  taosStringBuilderGetResult(&sb, NULL);
  *ppMsg = sb.buf;
  *pMsgLen = sb.pos;
  sb.buf = NULL;

_end:
  taosStringBuilderDestroy(&sb);
  if (code != TSDB_CODE_SUCCESS) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

// Build the JSON notification message from trigger params.
// On success, *ppMsg is a heap-owned NUL-terminated string (caller must free).
// If no params need notification, *ppMsg is NULL and *pMsgLen is 0.
static int32_t streamBuildNotifyMsgInternal(const char* streamName, const char* tableName, int32_t triggerType,
                                            int64_t groupId, int8_t precision, const SSTriggerCalcParam* pParams,
                                            int32_t nParam, char** ppMsg, size_t* pMsgLen) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SStringBuilder sb = {0};
  const char*    msgTail = "]}]}";
  bool           shouldNotify = false;

  *ppMsg = NULL;
  *pMsgLen = 0;

  for (int32_t i = 0; i < nParam; ++i) {
    if (pParams[i].notifyType != STRIGGER_EVENT_WINDOW_NONE) {
      shouldNotify = true;
      break;
    }
  }
  if (!shouldNotify) {
    goto _end;
  }

  size_t msgTailLen = strlen(msgTail);
  taosStringBuilderEnsureCapacity(&sb, 1024);

  code = streamAppendNotifyHeader(streamName, &sb);
  QUERY_CHECK_CODE(code, lino, _end);
  sb.pos -= msgTailLen;
  for (int32_t i = 0; i < nParam; ++i) {
    if (pParams[i].notifyType == STRIGGER_EVENT_WINDOW_NONE) {
      continue;
    }
    code = streamAppendNotifyContent(triggerType, groupId, &pParams[i], &sb, tableName, precision);
    QUERY_CHECK_CODE(code, lino, _end);
    taosStringBuilderAppendChar(&sb, ',');
  }
  sb.pos -= 1;
  taosStringBuilderAppendStringLen(&sb, msgTail, msgTailLen);

  // Ensure NUL terminator and transfer ownership of buffer.
  taosStringBuilderGetResult(&sb, NULL);
  *ppMsg = sb.buf;
  *pMsgLen = sb.pos;
  sb.buf = NULL;  // prevent Destroy from freeing the transferred buffer

_end:
  taosStringBuilderDestroy(&sb);
  if (code != TSDB_CODE_SUCCESS) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t streamNotifyEnqueueParams(int64_t streamId, const char* streamName, const char* tableName,
                                         int8_t precision, int32_t triggerType, int64_t groupId, int32_t addOptions,
                                         const SArray* pNotifyAddrUrls, const SSTriggerCalcParam* pParams,
                                         int32_t nParam, bool* pAccepted);

// Send a pre-built JSON message to all URLs synchronously.
// When NOTIFY_ON_FAILURE_PAUSE is set in addOptions, retries on failure.
// Otherwise, failures are logged and ignored.
static int32_t streamSendNotifyMsgToUrls(const char* msg, size_t msgLen, const SArray* pNotifyAddrUrls,
                                         int32_t addOptions, int64_t streamId, bool* pAttempted, bool* pDelivered) {
  int32_t code = TSDB_CODE_SUCCESS;
  SCURL   conn = {0};
  bool    attempted = false;
  bool    allTargetsDelivered = true;

  if (pAttempted != NULL) *pAttempted = false;
  if (pDelivered != NULL) *pDelivered = false;

  for (int32_t i = 0; i < TARRAY_SIZE(pNotifyAddrUrls); ++i) {
    char** pUrl = TARRAY_GET_ELEM(pNotifyAddrUrls, i);
    if (*pUrl == NULL) {
      continue;
    }
    attempted = true;

    conn.url = taosStrdup(*pUrl);
    if (!conn.url) {
      stError("streamId:%" PRIx64 " failed to dup notify URL", streamId);
      code = terrno;
      goto _end;
    }
    code = tcurlConnect(&conn.pConn, *pUrl);
    if (code != TSDB_CODE_SUCCESS) {
      stError("streamId:%" PRIx64 " failed to connect notify URL %s", streamId, *pUrl);
      tcurlClose(&conn);
      if (addOptions & NOTIFY_ON_FAILURE_PAUSE) {
        taosMsleep(STREAM_EVENT_NOTIFY_RETRY_MS);
        --i;
        continue;
      } else {
        allTargetsDelivered = false;
        code = TSDB_CODE_SUCCESS;
        continue;
      }
    }

    size_t  totalLen = msgLen;
    size_t  sentLen = 0;
    size_t  frameSize = (size_t)tsStreamNotifyFrameSize * 1024;
    int32_t res = TSDB_CODE_SUCCESS;
    while (sentLen < totalLen) {
      size_t frameLen = totalLen - sentLen;
      if (frameLen > frameSize) {
        frameLen = frameSize;
      }

      size_t       frameSentLen = 0;
      unsigned int flags = CURLWS_TEXT | CURLWS_OFFSET;
      if (sentLen + frameLen < totalLen) {
        flags |= CURLWS_CONT;
      }

      while (frameSentLen < frameLen) {
        size_t nbytes = 0;
        res = tcurlSend(&conn, msg + sentLen + frameSentLen, frameLen - frameSentLen, &nbytes,
                        frameSentLen == 0 ? (curl_off_t)frameLen : 0, flags);
        if (res != TSDB_CODE_SUCCESS) {
          break;
        }
        if (nbytes == 0) {
          res = TSDB_CODE_FAILED;
          break;
        }
        frameSentLen += nbytes;
      }
      if (res != TSDB_CODE_SUCCESS) {
        break;
      }
      sentLen += frameSentLen;
    }
    tcurlClose(&conn);
    if (res != TSDB_CODE_SUCCESS) {
      stError("streamId:%" PRIx64 " failed to send notify msg to %s, code:%d", streamId, *pUrl, res);
      if (addOptions & NOTIFY_ON_FAILURE_PAUSE) {
        taosMsleep(STREAM_EVENT_NOTIFY_RETRY_MS);
        --i;
      } else {
        allTargetsDelivered = false;
      }
    } else {
      stDebug("streamId:%" PRIx64 " notify sent to %s successfully", streamId, *pUrl);
    }
  }

_end:
  if (pAttempted != NULL) *pAttempted = attempted;
  if (pDelivered != NULL) *pDelivered = attempted && allTargetsDelivered && code == TSDB_CODE_SUCCESS;
  tcurlClose(&conn);
  return code;
}

// DROP mode reports queue acceptance without waiting for network delivery.
int32_t streamSendNotifyContentWithResult(SStreamTask* pTask, const char* streamName, const char* tableName,
                                          int32_t triggerType, int64_t groupId, int8_t precision,
                                          const SArray* pNotifyAddrUrls, int32_t addOptions,
                                          const SSTriggerCalcParam* pParams, int32_t nParam, bool* pAttempted,
                                          bool* pAccepted) {
  int64_t t0 = taosGetTimestampNs();
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  char*   msg = NULL;
  size_t  msgLen = 0;
  bool    attempted = false;
  bool    accepted = false;

  if (nParam <= 0 || taosArrayGetSize(pNotifyAddrUrls) <= 0) {
    goto _end;
  }

  if (addOptions & NOTIFY_ON_FAILURE_PAUSE) {
    char* pos = strstr(streamName, TS_PATH_DELIMITER);
    if (pos != NULL) {
      streamName = ++pos;
    }
    code = streamBuildNotifyMsgInternal(streamName, tableName, triggerType, groupId, precision, pParams, nParam, &msg,
                                        &msgLen);
    QUERY_CHECK_CODE(code, lino, _end);
    if (msg != NULL) {
      code =
          streamSendNotifyMsgToUrls(msg, msgLen, pNotifyAddrUrls, addOptions, pTask->streamId, &attempted, &accepted);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  } else {
    code = streamNotifyEnqueueParams(pTask->streamId, streamName, tableName, precision, triggerType, groupId,
                                     addOptions, pNotifyAddrUrls, pParams, nParam, &accepted);
    QUERY_CHECK_CODE(code, lino, _end);
    attempted = accepted;
  }

_end:
  if (pAttempted != NULL) *pAttempted = attempted;
  if (pAccepted != NULL) *pAccepted = accepted;
  taosMemoryFree(msg);
  if (code != TSDB_CODE_SUCCESS) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  } else if (nParam > 0 && taosArrayGetSize(pNotifyAddrUrls) > 0) {
    stTrace("[notify_trace] streamId:%" PRIx64 " gid:%" PRId64 " total_us:%" PRId64 " nParam:%d async:%d",
            pTask->streamId, groupId, (taosGetTimestampNs() - t0) / 1000, nParam,
            !(addOptions & NOTIFY_ON_FAILURE_PAUSE));
  }
  return code;
}

// ---------------------------------------------------------------------------
// Async notification path — per-stream param queue + worker pool
//
// Lock order (always acquire in this order; never hold queueLock while waiting on mapLock):
//   mapLock  -> queueLock  (worker pop / schedule / unregister)
//   poolLock is independent; take only while holding queueLock or without other notify locks.
//
// Architecture:
//   paramPool         — SObjPool<SStreamNotifyPendingItem> pre-allocated at pool init
//   queueMap          — streamId => active SStreamNotifyQueue (producer lookup)
//   globalQueueList   — FIFO tdlist of queues waiting for workers (protected by mapLock)
//
// Producer: enqueue params under queueLock; EnsureScheduled bumps refCount, releases
//           queueLock, takes mapLock, re-takes queueLock, appends to globalQueueList.
// Consumer: worker pops globalQueueList under mapLock, drains paramList under queueLock;
//           unlocks queueLock only during network send (JSON already built).
// ---------------------------------------------------------------------------

#define STREAM_NOTIFY_QUEUE_DLIST_FIELD queueNode
#define STREAM_NOTIFY_DROP_MAX_RETRIES      16
#define STREAM_NOTIFY_DROP_WORKER_BUDGET_MS 10000
#define STREAM_NOTIFY_WORKER_WAIT_MS        1000

static void    streamNotifyQueueDestroy(SStreamNotifyQueue* pQueue);
static bool    streamNotifyQueueDestroyIfReady(SStreamNotifyQueue* pQueue);
static void    streamNotifyQueueHashFree(void* pHashData);
static int32_t streamNotifyQueueGetOrCreate(int64_t streamId, const char* streamName, const char* tableName,
                                            int8_t precision, int32_t triggerType, int32_t addOptions,
                                            const SArray* pNotifyAddrUrls, SStreamNotifyQueue** ppQueue,
                                            bool* pNewQueue);
static void    streamNotifyQueueRelease(SStreamNotifyQueue* pQueue);
static void    streamNotifyWakeWorkers(SStreamNotifyWorkerPool* pPool);
static int32_t streamNotifyQueueEnsureScheduled(SStreamNotifyWorkerPool* pPool, SStreamNotifyQueue* pQueue);
static void    streamNotifyQueueAdjustPendingBytes(SStreamNotifyQueue* pQueue, size_t itemBytes);

static void streamNotifyPendingItemDestroy(void* pObj) {
  if (pObj == NULL) {
    return;
  }
  SStreamNotifyPendingItem* pItem = (SStreamNotifyPendingItem*)pObj;
  taosMemoryFreeClear(pItem->tableName);
  tDestroySSTriggerCalcParam(&pItem->param);
}

static int32_t streamNotifyCopyCalcParam(const SSTriggerCalcParam* src, SSTriggerCalcParam* dst) {
  memcpy(dst, src, sizeof(SSTriggerCalcParam));
  dst->eventConditionPath = NULL;
  dst->extraNotifyContent = NULL;
  dst->resultNotifyContent = NULL;
  dst->pExternalWindowData = NULL;

  if (src->eventConditionPath != NULL) {
    dst->eventConditionPath = taosStrdup(src->eventConditionPath);
    if (dst->eventConditionPath == NULL) {
      return terrno;
    }
  }
  if (src->extraNotifyContent != NULL) {
    dst->extraNotifyContent = taosStrdup(src->extraNotifyContent);
    if (dst->extraNotifyContent == NULL) {
      taosMemoryFreeClear(dst->eventConditionPath);
      return terrno;
    }
  }
  if (src->resultNotifyContent != NULL) {
    dst->resultNotifyContent = taosStrdup(src->resultNotifyContent);
    if (dst->resultNotifyContent == NULL) {
      taosMemoryFreeClear(dst->eventConditionPath);
      taosMemoryFreeClear(dst->extraNotifyContent);
      return terrno;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static size_t streamNotifyEstimateParamBytes(const SSTriggerCalcParam* pParam) {
  // Estimate serialized JSON size (not in-memory struct footprint) for batching.
  size_t bytes = 384;
  if (pParam->extraNotifyContent != NULL) {
    bytes += strlen(pParam->extraNotifyContent);
  }
  if (pParam->resultNotifyContent != NULL) {
    bytes += strlen(pParam->resultNotifyContent);
  }
  return bytes;
}

static size_t streamNotifyBatchMaxBytes(void) { return (size_t)tsStreamNotifyFrameSize * 1024; }

static int64_t streamNotifyParamPoolMaxBytes(void) { return (int64_t)tsStreamNotifyParamPoolSize * 1024; }

static int64_t streamNotifyParamPoolMaxCap(const SObjPool* pPool) {
  if (pPool == NULL || pPool->nodeSize <= 0) {
    return 4;
  }
  int64_t cap = streamNotifyParamPoolMaxBytes() / (int64_t)pPool->nodeSize;
  return cap < 4 ? 4 : cap;
}

static int32_t streamNotifyListAppendGrow(SObjList* pList, const void* pData) {
  if (pList == NULL || pData == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SStreamNotifyWorkerPool* pWorkerPool = &gStreamMgmt.notifyWorkerPool;
  SObjPool*                pPool = pList->pPool;

  taosThreadMutexLock(&pWorkerPool->poolLock);
  if (pPool->freeHeadIdx == TOBJPOOL_INVALID_IDX) {
    int64_t maxCap = streamNotifyParamPoolMaxCap(pPool);
    if (pPool->capacity >= maxCap) {
      taosThreadMutexUnlock(&pWorkerPool->poolLock);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    int64_t newCap = pPool->capacity * 2;
    if (newCap > maxCap) {
      newCap = maxCap;
    }
    if (newCap <= pPool->capacity) {
      newCap = pPool->capacity + 1;
    }
    int32_t code = taosObjPoolEnsureCap(pPool, newCap);
    if (code != TSDB_CODE_SUCCESS) {
      taosThreadMutexUnlock(&pWorkerPool->poolLock);
      return code;
    }
  }
  int32_t code = taosObjListAppend(pList, pData);
  taosThreadMutexUnlock(&pWorkerPool->poolLock);
  return code;
}

static void streamNotifyPoolPopHeadEx(SStreamNotifyWorkerPool* pWorkerPool, SObjList* pList, FDelete fp) {
  taosThreadMutexLock(&pWorkerPool->poolLock);
  taosObjListPopHeadEx(pList, fp);
  taosThreadMutexUnlock(&pWorkerPool->poolLock);
}

static void streamNotifyPoolPopTailEx(SStreamNotifyWorkerPool* pWorkerPool, SObjList* pList, FDelete fp) {
  taosThreadMutexLock(&pWorkerPool->poolLock);
  taosObjListPopTailEx(pList, fp);
  taosThreadMutexUnlock(&pWorkerPool->poolLock);
}

static void streamNotifyWakeWorkers(SStreamNotifyWorkerPool* pPool) {
  for (int32_t i = 0; i < pPool->numWorkers; i++) {
    taosThreadMutexLock(&pPool->workers[i].lock);
    pPool->workers[i].pendingWork = true;
    taosThreadCondBroadcast(&pPool->workers[i].cond);
    taosThreadMutexUnlock(&pPool->workers[i].lock);
  }
}

static void streamWorkerSleepMsInterruptible(const SStreamNotifyWorkerPool* pPool, int32_t totalMs) {
  int32_t sleptMs = 0;
  while (sleptMs < totalMs && !pPool->shuttingDown) {
    int32_t stepMs = (int32_t)TMIN(50, totalMs - sleptMs);
    taosMsleep(stepMs);
    sleptMs += stepMs;
  }
}

static void streamNotifyWorkerPoolMarkQueueDestroy(SStreamNotifyWorkerPool* pPool, SStreamNotifyQueue* pQueue) {
  if (pQueue == NULL) {
    return;
  }

  (void)taosThreadMutexLock(&pQueue->lock);
  if (pQueue->inGlobalList) {
    TD_DLIST_POP_WITH_FIELD(&pPool->globalQueueList, pQueue, STREAM_NOTIFY_QUEUE_DLIST_FIELD);
    pQueue->inGlobalList = false;
  }
  pQueue->pendingDestroy = true;
  taosThreadMutexUnlock(&pQueue->lock);
}

static void streamNotifyWorkerPoolPrepareShutdown(SStreamNotifyWorkerPool* pPool) {
  pPool->shuttingDown = true;

  taosThreadMutexLock(&pPool->mapLock);

  // Unregister every queue from queueMap (same as stream drop) so workers may
  // destroy via pendingDestroy+refCount; PurgeQueues must not chase map pointers.
  while (taosHashGetSize(pPool->queueMap) > 0) {
    void* pIter = taosHashIterate(pPool->queueMap, NULL);
    if (pIter == NULL) {
      break;
    }

    SStreamNotifyQueue* pQueue = *(SStreamNotifyQueue**)pIter;
    int64_t             streamId = 0;
    bool                hasStreamId = false;
    void*               pKey = taosHashGetKey(pIter, NULL);
    if (pKey != NULL) {
      streamId = *(int64_t*)pKey;
      hasStreamId = true;
    }
    taosHashCancelIterate(pPool->queueMap, pIter);

    streamNotifyWorkerPoolMarkQueueDestroy(pPool, pQueue);

    if (hasStreamId) {
      if (taosHashRemove(pPool->queueMap, &streamId, sizeof(streamId)) != TSDB_CODE_SUCCESS) {
        stError("[notify] streamId:%" PRIx64 " not found in queue map during shutdown", streamId);
      }
    }

    (void)taosThreadMutexLock(&pQueue->lock);
    if (pQueue->refCount == 0) {
      (void)streamNotifyQueueDestroyIfReady(pQueue);
    } else {
      taosThreadMutexUnlock(&pQueue->lock);
    }
  }

  while (TD_DLIST_NELES(&pPool->globalQueueList) > 0) {
    SStreamNotifyQueue* pQueue = TD_DLIST_HEAD(&pPool->globalQueueList);
    TD_DLIST_POP_WITH_FIELD(&pPool->globalQueueList, pQueue, STREAM_NOTIFY_QUEUE_DLIST_FIELD);
    (void)taosThreadMutexLock(&pQueue->lock);
    pQueue->inGlobalList = false;
    pQueue->pendingDestroy = true;
    taosThreadMutexUnlock(&pQueue->lock);
  }
  taosThreadMutexUnlock(&pPool->mapLock);

  streamNotifyWakeWorkers(pPool);
}

static void streamNotifyWorkerPoolPurgeQueues(SStreamNotifyWorkerPool* pPool) {
  if (pPool->queueMap == NULL) {
    return;
  }

  // PrepareShutdown should have drained queueMap; this is a safety net only.
  while (taosHashGetSize(pPool->queueMap) > 0) {
    void* pIter = taosHashIterate(pPool->queueMap, NULL);
    if (pIter == NULL) {
      break;
    }

    SStreamNotifyQueue* pQueue = *(SStreamNotifyQueue**)pIter;
    int64_t             streamId = 0;
    bool                hasStreamId = false;
    void*               pKey = taosHashGetKey(pIter, NULL);
    if (pKey != NULL) {
      streamId = *(int64_t*)pKey;
      hasStreamId = true;
    }
    taosHashCancelIterate(pPool->queueMap, pIter);

    if (hasStreamId) {
      if (taosHashRemove(pPool->queueMap, &streamId, sizeof(streamId)) != TSDB_CODE_SUCCESS) {
        stError("[notify] streamId:%" PRIx64 " not found in queue map", streamId);
      }
    }

    if (pQueue != NULL) {
      (void)taosThreadMutexLock(&pQueue->lock);
      taosObjListClearEx(&pQueue->paramList, streamNotifyPendingItemDestroy);
      pQueue->pendingBytes = 0;
      pQueue->refCount = 0;
      pQueue->consuming = false;
      pQueue->inGlobalList = false;
      taosThreadMutexUnlock(&pQueue->lock);
      streamNotifyQueueDestroy(pQueue);
    }
  }

  TD_DLIST_INIT(&pPool->globalQueueList);
}

static void streamNotifyLogEvictedParam(int64_t ownerStreamId, int64_t victimStreamId,
                                        const SStreamNotifyPendingItem* pItem, const char* reason) {
  if (pItem == NULL) {
    return;
  }
  stWarn("[notify] streamId:%" PRIx64 " %s, evicted streamId:%" PRIx64 " groupId:%" PRId64
         " notifyType:%d triggerTime:%" PRId64,
         ownerStreamId, reason, victimStreamId, pItem->groupId, pItem->param.notifyType, pItem->param.triggerTime);
}

// Free one obj-pool node by dropping the oldest pending param on another stream queue.
static bool streamNotifyParamPoolTryReclaim(SStreamNotifyWorkerPool* pWorkerPool, SStreamNotifyQueue* pSkipQueue) {
  if (pWorkerPool == NULL || pWorkerPool->queueMap == NULL) {
    return false;
  }

  if (taosThreadMutexTryLock(&pWorkerPool->mapLock) != 0) {
    return false;
  }

  bool  reclaimed = false;
  void* pIter = taosHashIterate(pWorkerPool->queueMap, NULL);
  while (pIter != NULL) {
    SStreamNotifyQueue* pQueue = *(SStreamNotifyQueue**)pIter;
    if (pQueue != NULL && pQueue != pSkipQueue && !pQueue->consuming && taosThreadMutexTryLock(&pQueue->lock) == 0) {
      SStreamNotifyPendingItem* pHead = (SStreamNotifyPendingItem*)taosObjListGetHead(&pQueue->paramList);
      if (pHead != NULL) {
        streamNotifyLogEvictedParam(pSkipQueue != NULL ? pSkipQueue->streamId : 0, pQueue->streamId, pHead,
                                    "param pool full, reclaim node from");
        size_t itemBytes = streamNotifyEstimateParamBytes(&pHead->param);
        streamNotifyPoolPopHeadEx(pWorkerPool, &pQueue->paramList, streamNotifyPendingItemDestroy);
        streamNotifyQueueAdjustPendingBytes(pQueue, itemBytes);
        reclaimed = true;
        taosThreadMutexUnlock(&pQueue->lock);
        taosHashCancelIterate(pWorkerPool->queueMap, pIter);
        break;
      }
      taosThreadMutexUnlock(&pQueue->lock);
    }
    pIter = taosHashIterate(pWorkerPool->queueMap, pIter);
  }
  taosThreadMutexUnlock(&pWorkerPool->mapLock);
  return reclaimed;
}

// Overwrite the oldest pending param in-place (no extra pool node).
static int32_t streamNotifyListReplaceHeadParam(SStreamNotifyWorkerPool* pWorkerPool, SStreamNotifyQueue* pQueue,
                                                SObjList* pList, const SStreamNotifyPendingItem* pNewItem) {
  if (pWorkerPool == NULL || pQueue == NULL || pList == NULL || pNewItem == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (pQueue->consuming) {
    return TSDB_CODE_FAILED;
  }

  taosThreadMutexLock(&pWorkerPool->poolLock);
  SStreamNotifyPendingItem* pHead = (SStreamNotifyPendingItem*)taosObjListGetHead(pList);
  if (pHead == NULL) {
    taosThreadMutexUnlock(&pWorkerPool->poolLock);
    return TSDB_CODE_FAILED;
  }

  streamNotifyLogEvictedParam(pQueue->streamId, pQueue->streamId, pHead,
                              "param pool full, replace oldest pending notify with");
  size_t oldBytes = streamNotifyEstimateParamBytes(&pHead->param);
  tDestroySSTriggerCalcParam(&pHead->param);
  memset(&pHead->param, 0, sizeof(pHead->param));

  pHead->groupId = pNewItem->groupId;
  taosMemoryFreeClear(pHead->tableName);
  if (pNewItem->tableName != NULL) {
    pHead->tableName = taosStrdup(pNewItem->tableName);
    if (pHead->tableName == NULL) {
      taosThreadMutexUnlock(&pWorkerPool->poolLock);
      return terrno;
    }
  }
  int32_t code = streamNotifyCopyCalcParam(&pNewItem->param, &pHead->param);
  taosThreadMutexUnlock(&pWorkerPool->poolLock);
  if (code != TSDB_CODE_SUCCESS) {
    streamNotifyPoolPopHeadEx(pWorkerPool, pList, streamNotifyPendingItemDestroy);
    return code;
  }

  streamNotifyQueueAdjustPendingBytes(pQueue, oldBytes);
  pQueue->pendingBytes += streamNotifyEstimateParamBytes(&pHead->param);
  return TSDB_CODE_SUCCESS;
}

static int32_t streamNotifyListAppendParam(SObjList* pList, const void* pData) {
  return streamNotifyListAppendGrow(pList, pData);
}

static int32_t streamNotifyListPushParam(SStreamNotifyWorkerPool* pPool, SStreamNotifyQueue* pQueue, int64_t groupId,
                                         const char* tableName, const SSTriggerCalcParam* src) {
  SStreamNotifyPendingItem item = {0};
  item.groupId = groupId;
  int32_t code = streamNotifyCopyCalcParam(src, &item.param);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (tableName != NULL) {
    item.tableName = taosStrdup(tableName);
    if (item.tableName == NULL) {
      tDestroySSTriggerCalcParam(&item.param);
      return terrno;
    }
  }

  code = streamNotifyListAppendParam(&pQueue->paramList, &item);
  if (code == TSDB_CODE_SUCCESS) {
    pQueue->pendingBytes += streamNotifyEstimateParamBytes(&item.param);
    return code;
  }

  // Pool exhausted: reclaim -> replace head -> drop new param (WARN only).
  taosThreadMutexUnlock(&pQueue->lock);
  bool reclaimed = streamNotifyParamPoolTryReclaim(pPool, pQueue);
  (void)taosThreadMutexLock(&pQueue->lock);
  if (pQueue->pendingDestroy) {
    taosMemoryFreeClear(item.tableName);
    tDestroySSTriggerCalcParam(&item.param);
    return TSDB_CODE_SUCCESS;
  }
  if (reclaimed) {
    code = streamNotifyListAppendParam(&pQueue->paramList, &item);
    if (code == TSDB_CODE_SUCCESS) {
      pQueue->pendingBytes += streamNotifyEstimateParamBytes(&item.param);
      return code;
    }
  }

  if (streamNotifyListReplaceHeadParam(pPool, pQueue, &pQueue->paramList, &item) == TSDB_CODE_SUCCESS) {
    taosMemoryFreeClear(item.tableName);
    tDestroySSTriggerCalcParam(&item.param);
    return TSDB_CODE_SUCCESS;
  }

  taosMemoryFreeClear(item.tableName);
  tDestroySSTriggerCalcParam(&item.param);
  stWarn("[notify] streamId:%" PRIx64 " param pool full, drop new notify groupId:%" PRId64 " notifyType:%d",
         pQueue->streamId, groupId, src->notifyType);
  return TSDB_CODE_SUCCESS;
}

static void streamNotifyQueueDestroy(SStreamNotifyQueue* pQ) {
  if (pQ == NULL) {
    return;
  }

  taosObjListClearEx(&pQ->paramList, streamNotifyPendingItemDestroy);

  if (pQ->pUrls != NULL) {
    for (int32_t i = 0; i < (int32_t)taosArrayGetSize(pQ->pUrls); i++) {
      char* url = *(char**)taosArrayGet(pQ->pUrls, i);
      taosMemoryFree(url);
    }
    taosArrayDestroy(pQ->pUrls);
  }
  taosMemoryFree(pQ->streamName);
  taosMemoryFree(pQ->tableName);
  taosThreadMutexDestroy(&pQ->lock);
  taosMemoryFree(pQ);
}

// Caller must hold pQueue->lock. Returns true if the queue was destroyed (lock released).
static bool streamNotifyQueueDestroyIfReady(SStreamNotifyQueue* pQueue) {
  if (pQueue == NULL || !pQueue->pendingDestroy || pQueue->refCount > 0) {
    return false;
  }

  taosThreadMutexUnlock(&pQueue->lock);
  streamNotifyQueueDestroy(pQueue);
  return true;
}

// queueMap stores borrowed queue pointers. pendingDestroy and refCount own queue lifetime.
static void streamNotifyQueueHashFree(void* pHashData) { (void)pHashData; }

static SArray* streamNotifyUrlsDeepCopy(const SArray* pNotifyAddrUrls) {
  int32_t urlCount = (int32_t)taosArrayGetSize(pNotifyAddrUrls);
  SArray* pUrlsCopy = taosArrayInit(urlCount, POINTER_BYTES);
  if (pUrlsCopy == NULL) {
    return NULL;
  }

  for (int32_t i = 0; i < urlCount; i++) {
    char* url = *(char**)taosArrayGet(pNotifyAddrUrls, i);
    char* copy = url ? taosStrdup(url) : NULL;
    if (url && copy == NULL) {
      for (int32_t j = 0; j < (int32_t)taosArrayGetSize(pUrlsCopy); j++) {
        taosMemoryFree(*(char**)taosArrayGet(pUrlsCopy, j));
      }
      taosArrayDestroy(pUrlsCopy);
      return NULL;
    }
    if (!taosArrayPush(pUrlsCopy, &copy)) {
      taosMemoryFree(copy);
      for (int32_t j = 0; j < (int32_t)taosArrayGetSize(pUrlsCopy); j++) {
        taosMemoryFree(*(char**)taosArrayGet(pUrlsCopy, j));
      }
      taosArrayDestroy(pUrlsCopy);
      return NULL;
    }
  }
  return pUrlsCopy;
}

static void streamNotifyQueueRelease(SStreamNotifyQueue* pQueue) {
  if (pQueue != NULL) {
    taosThreadMutexUnlock(&pQueue->lock);
  }
}

// Link idle queue onto globalQueueList for workers; worker coalesces by bytes/count/time.
// Caller must hold pQueue->lock; returns with pQueue->lock held.
static int32_t streamNotifyQueueEnsureScheduled(SStreamNotifyWorkerPool* pPool, SStreamNotifyQueue* pQueue) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (pQueue->pendingDestroy) {
    return TSDB_CODE_SUCCESS;
  }

  pQueue->refCount++;
  taosThreadMutexUnlock(&pQueue->lock);

  taosThreadMutexLock(&pPool->mapLock);
  (void)taosThreadMutexLock(&pQueue->lock);
  pQueue->refCount--;

  if (!pQueue->pendingDestroy && !pQueue->inGlobalList && !pQueue->consuming) {
    TD_DLIST_APPEND_WITH_FIELD(&pPool->globalQueueList, pQueue, STREAM_NOTIFY_QUEUE_DLIST_FIELD);
    pQueue->inGlobalList = true;
  }

  taosThreadMutexUnlock(&pPool->mapLock);
  return code;
}

static int32_t streamNotifyQueueCreateLocked(SStreamNotifyWorkerPool* pPool, int64_t streamId, const char* streamName,
                                             const char* tableName, int8_t precision, int32_t triggerType,
                                             int32_t addOptions, const SArray* pNotifyAddrUrls,
                                             SStreamNotifyQueue** ppQueue) {
  int32_t code = TSDB_CODE_SUCCESS;

  SStreamNotifyQueue* pQueue = taosMemCalloc(1, sizeof(SStreamNotifyQueue));
  if (pQueue == NULL) {
    return terrno;
  }

  pQueue->streamId = streamId;
  pQueue->triggerType = triggerType;
  pQueue->addOptions = addOptions;
  pQueue->precision = precision;
  pQueue->inGlobalList = false;
  pQueue->consuming = false;
  pQueue->pendingDestroy = false;
  pQueue->refCount = 0;
  pQueue->pendingBytes = 0;

  code = taosThreadMutexInit(&pQueue->lock, NULL);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pQueue);
    return code;
  }

  code = taosObjListInit(&pQueue->paramList, &pPool->paramPool);
  if (code != TSDB_CODE_SUCCESS) {
    taosThreadMutexDestroy(&pQueue->lock);
    taosMemoryFree(pQueue);
    return code;
  }

  if (streamName != NULL) {
    const char* name = streamName;
    char*       pos = strstr(streamName, TS_PATH_DELIMITER);
    if (pos != NULL) {
      name = pos + 1;
    }
    pQueue->streamName = taosStrdup(name);
    if (pQueue->streamName == NULL) {
      taosThreadMutexDestroy(&pQueue->lock);
      taosMemoryFree(pQueue);
      return terrno;
    }
  }

  if (tableName != NULL) {
    pQueue->tableName = taosStrdup(tableName);
    if (pQueue->tableName == NULL) {
      taosMemoryFree(pQueue->streamName);
      taosThreadMutexDestroy(&pQueue->lock);
      taosMemoryFree(pQueue);
      return terrno;
    }
  }

  pQueue->pUrls = streamNotifyUrlsDeepCopy(pNotifyAddrUrls);
  if (pQueue->pUrls == NULL) {
    taosMemoryFree(pQueue->tableName);
    taosMemoryFree(pQueue->streamName);
    taosThreadMutexDestroy(&pQueue->lock);
    taosMemoryFree(pQueue);
    return terrno;
  }

  code = taosHashPut(pPool->queueMap, &streamId, sizeof(streamId), &pQueue, POINTER_BYTES);
  if (code != TSDB_CODE_SUCCESS) {
    streamNotifyQueueDestroy(pQueue);
    return code;
  }

  pQueue->inGlobalList = false;

  (void)taosThreadMutexLock(&pQueue->lock);
  *ppQueue = pQueue;
  stInfo("streamId:%" PRIx64 " notify queue created", streamId);
  return TSDB_CODE_SUCCESS;
}

static int32_t streamNotifyQueueGetOrCreate(int64_t streamId, const char* streamName, const char* tableName,
                                            int8_t precision, int32_t triggerType, int32_t addOptions,
                                            const SArray* pNotifyAddrUrls, SStreamNotifyQueue** ppQueue,
                                            bool* pNewQueue) {
  SStreamNotifyWorkerPool* pPool = &gStreamMgmt.notifyWorkerPool;
  *ppQueue = NULL;
  if (pNewQueue != NULL) {
    *pNewQueue = false;
  }

  if (!pPool->inited || pPool->shuttingDown || pNotifyAddrUrls == NULL || taosArrayGetSize(pNotifyAddrUrls) <= 0) {
    return TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
  }

  taosThreadMutexLock(&pPool->mapLock);
  SStreamNotifyQueue** ppExist = taosHashGet(pPool->queueMap, &streamId, sizeof(streamId));
  if (ppExist != NULL && *ppExist != NULL) {
    SStreamNotifyQueue* pQueue = *ppExist;
    (void)taosThreadMutexLock(&pQueue->lock);
    if (pQueue->pendingDestroy) {
      taosThreadMutexUnlock(&pQueue->lock);
      taosThreadMutexUnlock(&pPool->mapLock);
      return TSDB_CODE_SUCCESS;
    }
    if (tableName != NULL && pQueue->tableName == NULL) {
      pQueue->tableName = taosStrdup(tableName);
      if (pQueue->tableName == NULL) {
        taosThreadMutexUnlock(&pQueue->lock);
        taosThreadMutexUnlock(&pPool->mapLock);
        return terrno;
      }
    }
    *ppQueue = pQueue;
    taosThreadMutexUnlock(&pPool->mapLock);
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = streamNotifyQueueCreateLocked(pPool, streamId, streamName, tableName, precision, triggerType,
                                               addOptions, pNotifyAddrUrls, ppQueue);
  if (code == TSDB_CODE_SUCCESS && pNewQueue != NULL) {
    *pNewQueue = true;
  }
  taosThreadMutexUnlock(&pPool->mapLock);
  return code;
}

void streamNotifyQueueUnregister(int64_t streamId) {
  SStreamNotifyWorkerPool* pPool = &gStreamMgmt.notifyWorkerPool;
  if (!pPool->inited) {
    return;
  }

  taosThreadMutexLock(&pPool->mapLock);
  SStreamNotifyQueue** ppQueue = taosHashGet(pPool->queueMap, &streamId, sizeof(streamId));
  SStreamNotifyQueue*  pQueue = (ppQueue != NULL) ? *ppQueue : NULL;
  if (pQueue != NULL) {
    bool deferDestroy = false;

    (void)taosThreadMutexLock(&pQueue->lock);
    if (pQueue->inGlobalList) {
      TD_DLIST_POP_WITH_FIELD(&pPool->globalQueueList, pQueue, STREAM_NOTIFY_QUEUE_DLIST_FIELD);
      pQueue->inGlobalList = false;
    }
    pQueue->pendingDestroy = true;
    int32_t refCount = pQueue->refCount;
    deferDestroy = (refCount > 0);
    taosThreadMutexUnlock(&pQueue->lock);
    if (taosHashRemove(pPool->queueMap, &streamId, sizeof(streamId)) != TSDB_CODE_SUCCESS) {
      stError("[streamNotifyQueueUnregister] failed to remove queue from queueMap");
    }
    taosThreadMutexUnlock(&pPool->mapLock);
    streamNotifyWakeWorkers(pPool);
    if (deferDestroy) {
      stInfo("streamId:%" PRIx64 " notify queue unregistered (deferred destroy, refCount:%d)", streamId, refCount);
    } else {
      (void)taosThreadMutexLock(&pQueue->lock);
      if (!streamNotifyQueueDestroyIfReady(pQueue)) {
        taosThreadMutexUnlock(&pQueue->lock);
      }
      stInfo("streamId:%" PRIx64 " notify queue unregistered", streamId);
    }
    return;
  }
  taosThreadMutexUnlock(&pPool->mapLock);
}

static int32_t streamNotifyEnqueueParams(int64_t streamId, const char* streamName, const char* tableName,
                                         int8_t precision, int32_t triggerType, int64_t groupId, int32_t addOptions,
                                         const SArray* pNotifyAddrUrls, const SSTriggerCalcParam* pParams,
                                         int32_t nParam, bool* pAccepted) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  bool    shouldNotify = false;
  int32_t enqueued = 0;

  if (pAccepted != NULL) {
    *pAccepted = false;
  }

  for (int32_t i = 0; i < nParam; ++i) {
    if (pParams[i].notifyType != STRIGGER_EVENT_WINDOW_NONE) {
      shouldNotify = true;
      break;
    }
  }
  if (!shouldNotify) {
    return TSDB_CODE_SUCCESS;
  }

  SStreamNotifyQueue* pQueue = NULL;
  code = streamNotifyQueueGetOrCreate(streamId, streamName, tableName, precision, triggerType, addOptions,
                                      pNotifyAddrUrls, &pQueue, NULL);
  if (code != TSDB_CODE_SUCCESS || pQueue == NULL) {
    return code;
  }

  SStreamNotifyWorkerPool* pPool = &gStreamMgmt.notifyWorkerPool;
  if (pPool->shuttingDown) {
    streamNotifyQueueRelease(pQueue);
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < nParam; i++) {
    if (pParams[i].notifyType == STRIGGER_EVENT_WINDOW_NONE) {
      continue;
    }

    code = streamNotifyListPushParam(pPool, pQueue, groupId, tableName, &pParams[i]);
    if (code != TSDB_CODE_SUCCESS) {
      lino = __LINE__;
      goto _end;
    }
    enqueued++;
  }

  if (enqueued > 0) {
    code = streamNotifyQueueEnsureScheduled(pPool, pQueue);
    if (code != TSDB_CODE_SUCCESS) {
      lino = __LINE__;
      goto _end;
    }
    if (pAccepted != NULL) {
      *pAccepted = true;
    }
    streamNotifyWakeWorkers(pPool);
  }

_end:
  if (pQueue != NULL) {
    if (!streamNotifyQueueDestroyIfReady(pQueue)) {
      streamNotifyQueueRelease(pQueue);
    }
  }
  if (code != TSDB_CODE_SUCCESS) {
    stError("streamId:%" PRIx64 " failed to enqueue notify params at line %d since %s", streamId, lino,
            tstrerror(code));
  }
  return code;
}

int32_t streamSendNotifyContent(SStreamTask* pTask, const char* streamName, const char* tableName, int32_t triggerType,
                                int64_t groupId, int8_t precision, const SArray* pNotifyAddrUrls, int32_t addOptions,
                                const SSTriggerCalcParam* pParams, int32_t nParam) {
  return streamSendNotifyContentWithResult(pTask, streamName, tableName, triggerType, groupId, precision,
                                           pNotifyAddrUrls, addOptions, pParams, nParam, NULL, NULL);
}

// Low-level: send a buffer to one URL on the thread-local persistent connection.
// Before sending, tcurlEnsureAlive validates the WS session via recv-drain and
// ping-pong so that half-open handles are detected and replaced proactively.
// Returns true when the full message is delivered; at most one reconnect retry.
static bool streamWorkerSendOnUrl(const char* url, const char* msg, size_t msgLen) {
  for (int32_t attempt = 0; attempt < 2; attempt++) {
    int64_t retryAfterMs = tcurlUrlRetryAfterMs(url);
    if (retryAfterMs > 0) {
      if (retryAfterMs > 5000) {
        stWarn("[notify_worker] url %s congested, retry after %" PRId64 " ms", url, retryAfterMs);
        return false;
      }
      taosMsleep((int32_t)TMIN(retryAfterMs, 500));
    }

    SCURL*  pConn = NULL;
    int32_t getCode = tcurlGetConnection(url, &pConn);
    if (getCode != TSDB_CODE_SUCCESS || !pConn || !pConn->pConn) {
      if (getCode == TSDB_CODE_RPC_NETWORK_BUSY) {
        stWarn("[notify_worker] url %s connect congested on attempt %d", url, attempt + 1);
      } else {
        stWarn("[notify_worker] get connection for %s failed on attempt %d", url, attempt + 1);
      }
      if (attempt == 0) {
        taosMsleep(100);
      } else {
        taosMsleep(200);
      }
      continue;
    }

    if (tcurlEnsureAlive(pConn) != TSDB_CODE_SUCCESS) {
      stWarn("[notify_worker] ensure alive for %s failed on attempt %d", url, attempt + 1);
      tcurlRemoveConnection(url);
      if (attempt == 0) {
        taosMsleep(100);
      } else {
        taosMsleep(200);
      }
      continue;
    }

    int32_t code = tcurlSendMessage(pConn, msg, msgLen, streamNotifyBatchMaxBytes());
    if (code == TSDB_CODE_SUCCESS) {
      pConn->lastActiveMs = taosGetTimestampMs();
      return true;
    }

    stWarn("[notify_worker] send to %s failed on attempt %d, dropping cached connection", url, attempt + 1);
    tcurlRemoveConnection(url);
  }

  stError("[notify_worker] send to %s failed after reconnect retry", url);
  return false;
}

static void streamWorkerPrepareUrl(const char* url) {
  if (url == NULL || tcurlUrlCongested(url)) {
    return;
  }
  SCURL* pConn = NULL;
  if (tcurlGetConnection(url, &pConn) != TSDB_CODE_SUCCESS || !pConn) {
    return;
  }
  if (tcurlEnsureAlive(pConn) != TSDB_CODE_SUCCESS) {
    tcurlRemoveConnection(url);
  }
}

static int64_t streamWorkerQueueMaxRetryAfterMs(SStreamNotifyQueue* pQueue) {
  int64_t maxWaitMs = 0;
  if (pQueue == NULL || pQueue->pUrls == NULL) {
    return 0;
  }
  for (int32_t i = 0; i < (int32_t)taosArrayGetSize(pQueue->pUrls); i++) {
    char* url = *(char**)taosArrayGet(pQueue->pUrls, i);
    if (url == NULL) {
      continue;
    }
    int64_t waitMs = tcurlUrlRetryAfterMs(url);
    if (waitMs > maxWaitMs) {
      maxWaitMs = waitMs;
    }
  }
  return maxWaitMs;
}

static void streamWorkerPrepareQueueUrls(SStreamNotifyQueue* pQueue) {
  if (pQueue == NULL || pQueue->pUrls == NULL) {
    return;
  }
  for (int32_t i = 0; i < (int32_t)taosArrayGetSize(pQueue->pUrls); i++) {
    char* url = *(char**)taosArrayGet(pQueue->pUrls, i);
    if (url) {
      streamWorkerPrepareUrl(url);
    }
  }
}

static bool streamNotifyQueueHasPendingParams(SStreamNotifyQueue* pQueue) {
  return taosObjListGetHead(&pQueue->paramList) != NULL;
}

static int32_t streamNotifyQueuePendingCount(SStreamNotifyQueue* pQueue) {
  int32_t n = (int32_t)pQueue->paramList.neles;
  return n > 0 ? n : 0;
}

static bool streamNotifyQueueHeavyBacklog(SStreamNotifyQueue* pQueue) {
  if (pQueue->pendingBytes >= streamNotifyBatchMaxBytes() / 4) {
    return true;
  }
  return streamNotifyQueuePendingCount(pQueue) >= 16;
}

static bool streamNotifyQueueMustFlush(SStreamNotifyQueue* pQueue) {
  if (pQueue->pendingBytes >= streamNotifyBatchMaxBytes()) {
    return true;
  }
  return !streamNotifyQueueHasPendingParams(pQueue);
}

// Release queue lock while waiting for more params to coalesce into larger WS batches.
static void streamWorkerDeferConsume(SStreamNotifyWorkerPool* pPool, SStreamNotifyQueue* pQueue) {
  if (pPool->shuttingDown || pQueue->pendingDestroy || streamNotifyQueueMustFlush(pQueue)) {
    return;
  }

  int32_t minEvents = 32;
  int64_t waitMs = tsStreamNotifyBatchIntervalMs;
  if (waitMs < 0) {
    waitMs = 0;
  }
  int64_t deadline = taosGetTimestampMs() + waitMs;

  while (taosGetTimestampMs() < deadline) {
    if (pPool->shuttingDown || pQueue->pendingDestroy) {
      break;
    }
    if (streamNotifyQueueHeavyBacklog(pQueue)) {
      break;
    }
    if (streamNotifyQueuePendingCount(pQueue) >= minEvents) {
      break;
    }
    taosThreadMutexUnlock(&pQueue->lock);
    taosMsleep(1);
    taosThreadMutexLock(&pQueue->lock);
    if (pPool->shuttingDown || pQueue->pendingDestroy || !streamNotifyQueueHasPendingParams(pQueue)) {
      break;
    }
  }
}

// Send built JSON to every URL registered for the stream queue.
static bool streamWorkerSendToQueueUrls(SStreamNotifyQueue* pQueue, const char* msg, size_t msgLen) {
  streamWorkerPrepareQueueUrls(pQueue);
  for (int32_t i = 0; i < (int32_t)taosArrayGetSize(pQueue->pUrls); i++) {
    char* url = *(char**)taosArrayGet(pQueue->pUrls, i);
    if (url && !streamWorkerSendOnUrl(url, msg, msgLen)) {
      return false;
    }
  }
  return true;
}

static void streamNotifyQueueAdjustPendingBytes(SStreamNotifyQueue* pQueue, size_t itemBytes) {
  if (pQueue->pendingBytes >= itemBytes) {
    pQueue->pendingBytes -= itemBytes;
  } else {
    pQueue->pendingBytes = 0;
  }
}

// Merge and send one same-groupId run from ppItems[0..nItems); params stay in queue until caller pops.
// Returns nMerged (>0) on successful send, 0 on merge/build failure (caller may drop head),
// -nMerged on send failure (caller must keep queue and retry).
static int32_t streamWorkerFlushGroupBatch(SStreamNotifyQueue* pQueue, SStreamNotifyPendingItem** ppItems,
                                           int32_t nItems) {
  if (pQueue == NULL || ppItems == NULL || nItems <= 0) {
    return 0;
  }

  int64_t                   groupId = ppItems[0]->groupId;
  SStreamNotifyPendingItem* pMergedItems[256];
  int32_t                   nMerged = 0;

  for (int32_t i = 0; i < nItems && nMerged < (int32_t)(sizeof(pMergedItems) / sizeof(pMergedItems[0])); i++) {
    if (ppItems[i]->groupId != groupId) {
      break;
    }
    pMergedItems[nMerged++] = ppItems[i];
  }

  if (nMerged <= 0) {
    return 0;
  }

  char*  msg = NULL;
  size_t msgLen = 0;
  if (streamBuildNotifyMsgFromItems(pQueue->streamName, pQueue->triggerType, groupId, pQueue->precision, pMergedItems,
                                    nMerged, &msg, &msgLen) != TSDB_CODE_SUCCESS ||
      msg == NULL) {
    taosMemoryFree(msg);
    return 0;
  }

  taosThreadMutexUnlock(&pQueue->lock);
  bool sendOk = streamWorkerSendToQueueUrls(pQueue, msg, msgLen);
  (void)taosThreadMutexLock(&pQueue->lock);
  taosMemoryFree(msg);
  if (!sendOk) {
    return -nMerged;
  }
  return nMerged;
}

static SStreamNotifyQueue* streamWorkerPopGlobalQueue(SStreamNotifyWorkerPool* pPool) {
  SStreamNotifyQueue* pQueue = NULL;

  taosThreadMutexLock(&pPool->mapLock);
  pQueue = TD_DLIST_HEAD(&pPool->globalQueueList);
  if (pQueue != NULL) {
    (void)taosThreadMutexLock(&pQueue->lock);
    TD_DLIST_POP_WITH_FIELD(&pPool->globalQueueList, pQueue, STREAM_NOTIFY_QUEUE_DLIST_FIELD);
    pQueue->inGlobalList = false;
    pQueue->consuming = true;
    pQueue->refCount++;
  }
  taosThreadMutexUnlock(&pPool->mapLock);

  if (pQueue == NULL) {
    return NULL;
  }

  return pQueue;
}

// Drop the current coalesced batch from queue head after DROP-mode send retries are exhausted.
static void streamWorkerDropHeadBatch(SStreamNotifyWorkerPool* pWorkerPool, SStreamNotifyQueue* pQueue) {
  size_t    batchMaxBytes = streamNotifyBatchMaxBytes();
  SObjPool* pPool = pQueue->paramList.pPool;
  int32_t   nDrop = 0;

  taosThreadMutexLock(&pWorkerPool->poolLock);
  int64_t idx = pQueue->paramList.headIdx;
  size_t  batchBytes = 0;
  while (idx != TOBJPOOL_INVALID_IDX) {
    SObjPoolNode*             pNode = TOBJPOOL_GET_NODE(pPool, idx);
    SStreamNotifyPendingItem* pItem = (SStreamNotifyPendingItem*)TOBJPOOL_NODE_GET_OBJ(pNode);
    size_t                    itemBytes = streamNotifyEstimateParamBytes(&pItem->param);
    if (batchBytes > 0 && batchBytes + itemBytes >= batchMaxBytes) {
      break;
    }
    batchBytes += itemBytes;
    nDrop++;
    idx = pNode->nextIdx;
    if (batchBytes >= batchMaxBytes) {
      break;
    }
  }
  taosThreadMutexUnlock(&pWorkerPool->poolLock);

  for (int32_t i = 0; i < nDrop; i++) {
    SStreamNotifyPendingItem* pHead = taosObjListGetHead(&pQueue->paramList);
    if (pHead == NULL) {
      break;
    }
    size_t itemBytes = streamNotifyEstimateParamBytes(&pHead->param);
    streamNotifyPoolPopHeadEx(pWorkerPool, &pQueue->paramList, streamNotifyPendingItemDestroy);
    streamNotifyQueueAdjustPendingBytes(pQueue, itemBytes);
  }
  if (nDrop > 0) {
    stWarn("[notify_worker] streamId:%" PRIx64 " drop %d notify item(s) after send retry limit", pQueue->streamId,
           nDrop);
  }
}

// Returns 0 on success, -1 when send failed (queue unchanged, retry later).
static int32_t streamWorkerDrainOneBatch(SStreamNotifyWorkerPool* pWorkerPool, SStreamNotifyQueue* pQueue) {
  size_t    batchMaxBytes = streamNotifyBatchMaxBytes();
  SObjPool* pPool = pQueue->paramList.pPool;

  SArray* pBatch = taosArrayInit(8, POINTER_BYTES);
  if (pBatch == NULL) {
    return 0;
  }

  size_t  batchBytes = 0;
  int64_t idx = pQueue->paramList.headIdx;
  taosThreadMutexLock(&pWorkerPool->poolLock);
  while (idx != TOBJPOOL_INVALID_IDX) {
    SObjPoolNode*             pNode = TOBJPOOL_GET_NODE(pPool, idx);
    SStreamNotifyPendingItem* pItem = (SStreamNotifyPendingItem*)TOBJPOOL_NODE_GET_OBJ(pNode);
    size_t itemBytes = streamNotifyEstimateParamBytes(&pItem->param);
    if (batchBytes > 0 && batchBytes + itemBytes >= batchMaxBytes) {
      break;
    }
    if (!taosArrayPush(pBatch, &pItem)) {
      break;
    }
    batchBytes += itemBytes;
    idx = pNode->nextIdx;
    if (batchBytes >= batchMaxBytes) {
      break;
    }
  }
  taosThreadMutexUnlock(&pWorkerPool->poolLock);

  while (taosArrayGetSize(pBatch) > 0) {
    SStreamNotifyPendingItem** ppItems = (SStreamNotifyPendingItem**)TARRAY_DATA(pBatch);
    int32_t                    nItems = (int32_t)taosArrayGetSize(pBatch);
    int32_t                    nSent = streamWorkerFlushGroupBatch(pQueue, ppItems, nItems);
    if (nSent < 0) {
      taosArrayDestroy(pBatch);
      return -1;
    }
    if (nSent == 0) {
      SStreamNotifyPendingItem* pHead = taosObjListGetHead(&pQueue->paramList);
      if (pHead != NULL) {
        size_t itemBytes = streamNotifyEstimateParamBytes(&pHead->param);
        streamNotifyPoolPopHeadEx(pWorkerPool, &pQueue->paramList, streamNotifyPendingItemDestroy);
        streamNotifyQueueAdjustPendingBytes(pQueue, itemBytes);
        stError("[notify_worker] streamId:%" PRIx64 " drop notify item after merge failure", pQueue->streamId);
      }
      break;
    }

    SStreamNotifyPendingItem* pExpectHead = ppItems[0];
    SStreamNotifyPendingItem* pHead = taosObjListGetHead(&pQueue->paramList);
    if (pHead != pExpectHead) {
      stError("[notify_worker] streamId:%" PRIx64 " queue head changed after send, stop pop", pQueue->streamId);
      break;
    }

    for (int32_t i = 0; i < nSent; i++) {
      pHead = taosObjListGetHead(&pQueue->paramList);
      if (pHead == NULL) {
        break;
      }
      size_t itemBytes = streamNotifyEstimateParamBytes(&pHead->param);
      streamNotifyPoolPopHeadEx(pWorkerPool, &pQueue->paramList, streamNotifyPendingItemDestroy);
      streamNotifyQueueAdjustPendingBytes(pQueue, itemBytes);
    }
    taosArrayPopFrontBatch(pBatch, nSent);
  }

  taosArrayDestroy(pBatch);
  return 0;
}

static void streamWorkerConsumeQueue(SStreamNotifyWorkerPool* pPool, SStreamNotifyQueue* pQueue) {
  int64_t streamId = 0;
  int32_t sendFailures = 0;
  bool    dropMode = false;
  int64_t consumeDeadlineMs = 0;

  if (pQueue == NULL) {
    return;
  }

  streamId = pQueue->streamId;
  dropMode = !(pQueue->addOptions & NOTIFY_ON_FAILURE_PAUSE);
  if (dropMode) {
    consumeDeadlineMs = taosGetTimestampMs() + STREAM_NOTIFY_DROP_WORKER_BUDGET_MS;
  }

  while (true) {
    if (pPool->shuttingDown || pQueue->pendingDestroy) {
      break;
    }

    if (dropMode && taosGetTimestampMs() >= consumeDeadlineMs) {
      stWarn("[notify_worker] streamId:%" PRIx64 " DROP worker budget exhausted, requeue", streamId);
      goto _release;
    }

    streamWorkerDeferConsume(pPool, pQueue);
    if (pPool->shuttingDown || pQueue->pendingDestroy) {
      break;
    }

    while (streamNotifyQueueHasPendingParams(pQueue)) {
      if (pPool->shuttingDown) {
        goto _release;
      }
      if (streamWorkerDrainOneBatch(pPool, pQueue) < 0) {
        sendFailures++;
        if (pPool->shuttingDown) {
          goto _release;
        }
        if (dropMode && sendFailures >= STREAM_NOTIFY_DROP_MAX_RETRIES) {
          streamWorkerDropHeadBatch(pPool, pQueue);
          sendFailures = 0;
          continue;
        }
        if (dropMode && taosGetTimestampMs() >= consumeDeadlineMs) {
          stWarn("[notify_worker] streamId:%" PRIx64 " DROP worker budget exhausted after send failure, requeue",
                 streamId);
          goto _release;
        }
        int64_t backoffMs = streamWorkerQueueMaxRetryAfterMs(pQueue);
        if (backoffMs < 150) {
          backoffMs = 150;
        }
        backoffMs = TMIN(backoffMs, 5000);
        if (dropMode) {
          int64_t remainMs = consumeDeadlineMs - taosGetTimestampMs();
          if (remainMs <= 0) {
            stWarn("[notify_worker] streamId:%" PRIx64 " DROP worker budget exhausted before backoff, requeue",
                   streamId);
            goto _release;
          }
          backoffMs = TMIN(backoffMs, remainMs);
        }
        taosThreadMutexUnlock(&pQueue->lock);
        streamWorkerSleepMsInterruptible(pPool, (int32_t)backoffMs);
        taosThreadMutexLock(&pQueue->lock);
        if (pPool->shuttingDown || pQueue->pendingDestroy) {
          goto _release;
        }
        break;
      }
      sendFailures = 0;
    }

    if (pPool->shuttingDown || pQueue->pendingDestroy) {
      break;
    }

    if (!streamNotifyQueueHasPendingParams(pQueue)) {
      break;
    }
  }

_release:
  pQueue->consuming = false;
  if (!pPool->shuttingDown && !pQueue->pendingDestroy && streamNotifyQueueHasPendingParams(pQueue)) {
    (void)streamNotifyQueueEnsureScheduled(pPool, pQueue);
    streamNotifyWakeWorkers(pPool);
  }

  pQueue->refCount--;
  if (streamNotifyQueueDestroyIfReady(pQueue)) {
    stInfo("streamId:%" PRIx64 " deferred notify queue destroyed after worker release", streamId);
    return;
  }
  taosThreadMutexUnlock(&pQueue->lock);
}

static void* streamNotifyWorkerThread(void* arg) {
  SStreamNotifyWorker*     pWorker = (SStreamNotifyWorker*)arg;
  SStreamNotifyWorkerPool* pPool = &gStreamMgmt.notifyWorkerPool;
  setThreadName("stm-notify");

  while (true) {
    if (pPool->shuttingDown) {
      break;
    }

    for (int32_t pass = 0; pass < 2; pass++) {
      SStreamNotifyQueue* pQueue = NULL;
      while (!pPool->shuttingDown && (pQueue = streamWorkerPopGlobalQueue(pPool)) != NULL) {
        streamWorkerConsumeQueue(pPool, pQueue);
      }
    }

    taosThreadMutexLock(&pWorker->lock);
    if (!pWorker->running || pPool->shuttingDown) {
      taosThreadMutexUnlock(&pWorker->lock);
      break;
    }
    while (pWorker->running && !pWorker->pendingWork && !pPool->shuttingDown) {
      int64_t         deadlineMs = taosGetTimestampMs() + STREAM_NOTIFY_WORKER_WAIT_MS;
      struct timespec abstime = {0};
      abstime.tv_sec = (time_t)(deadlineMs / 1000);
      abstime.tv_nsec = (long)((deadlineMs % 1000) * 1000000);
      (void)taosThreadCondTimedWait(&pWorker->cond, &pWorker->lock, &abstime);
    }
    if (!pWorker->running || pPool->shuttingDown) {
      taosThreadMutexUnlock(&pWorker->lock);
      break;
    }
    pWorker->pendingWork = false;
    taosThreadMutexUnlock(&pWorker->lock);
  }

  stInfo("[notify_worker] worker-%d exited", pWorker->id);
  return NULL;
}

static int32_t streamNotifyWorkerInit(SStreamNotifyWorker* pWorker, int32_t id) {
  pWorker->id = id;

  int32_t code = taosThreadMutexInit(&pWorker->lock, NULL);
  if (code) {
    return code;
  }

  code = taosThreadCondInit(&pWorker->cond, NULL);
  if (code) {
    taosThreadMutexDestroy(&pWorker->lock);
    return code;
  }

  pWorker->running = true;
  pWorker->pendingWork = false;
  pWorker->inited = true;

  TdThreadAttr attr;
  taosThreadAttrInit(&attr);
  code = taosThreadCreate(&pWorker->thread, &attr, streamNotifyWorkerThread, pWorker);
  taosThreadAttrDestroy(&attr);
  if (code) {
    pWorker->running = false;
    pWorker->inited = false;
    taosThreadCondDestroy(&pWorker->cond);
    taosThreadMutexDestroy(&pWorker->lock);
    return code;
  }
  return TSDB_CODE_SUCCESS;
}

static void streamNotifyWorkerDestroy(SStreamNotifyWorker* pWorker) {
  if (!pWorker->inited) {
    return;
  }

  taosThreadMutexLock(&pWorker->lock);
  pWorker->running = false;
  taosThreadCondSignal(&pWorker->cond);
  taosThreadMutexUnlock(&pWorker->lock);

  taosThreadJoin(pWorker->thread, NULL);
  taosThreadCondDestroy(&pWorker->cond);
  taosThreadMutexDestroy(&pWorker->lock);
  pWorker->inited = false;
}

int32_t streamNotifyWorkerPoolInit(SStreamNotifyWorkerPool* pPool, int32_t numWorkers) {
  pPool->inited = false;
  pPool->numWorkers = 0;
  pPool->workers = NULL;
  pPool->queueMap = NULL;
  memset(&pPool->paramPool, 0, sizeof(SObjPool));
  TD_DLIST_INIT(&pPool->globalQueueList);

  if (numWorkers <= 0) {
    numWorkers = 1;
  }

  int32_t code = taosThreadMutexInit(&pPool->mapLock, NULL);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = taosThreadMutexInit(&pPool->poolLock, NULL);
  if (code != TSDB_CODE_SUCCESS) {
    taosThreadMutexDestroy(&pPool->mapLock);
    return code;
  }

  size_t  nodeSize = sizeof(SObjPoolNode) + sizeof(SStreamNotifyPendingItem);
  int64_t poolCap = streamNotifyParamPoolMaxBytes() / (int64_t)nodeSize;
  if (poolCap < 4) {
    poolCap = 4;
  }
  // Pre-allocate the full pool up front to avoid taosMemoryRealloc races with worker readers.
  int64_t initCap = poolCap;
  code = taosObjPoolInit(&pPool->paramPool, initCap, sizeof(SStreamNotifyPendingItem));
  if (code != TSDB_CODE_SUCCESS) {
    taosThreadMutexDestroy(&pPool->poolLock);
    taosThreadMutexDestroy(&pPool->mapLock);
    return code;
  }

  pPool->queueMap = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (pPool->queueMap == NULL) {
    taosObjPoolDestroy(&pPool->paramPool);
    taosThreadMutexDestroy(&pPool->poolLock);
    taosThreadMutexDestroy(&pPool->mapLock);
    return terrno;
  }
  taosHashSetFreeFp(pPool->queueMap, streamNotifyQueueHashFree);

  pPool->workers = taosMemCalloc(numWorkers, sizeof(SStreamNotifyWorker));
  if (!pPool->workers) {
    taosHashCleanup(pPool->queueMap);
    pPool->queueMap = NULL;
    taosObjPoolDestroy(&pPool->paramPool);
    taosThreadMutexDestroy(&pPool->poolLock);
    taosThreadMutexDestroy(&pPool->mapLock);
    return terrno;
  }

  for (int32_t i = 0; i < numWorkers; i++) {
    code = streamNotifyWorkerInit(&pPool->workers[i], i);
    if (code != TSDB_CODE_SUCCESS) {
      for (int32_t j = i - 1; j >= 0; j--) {
        streamNotifyWorkerDestroy(&pPool->workers[j]);
      }
      taosMemoryFree(pPool->workers);
      pPool->workers = NULL;
      taosHashCleanup(pPool->queueMap);
      pPool->queueMap = NULL;
      taosObjPoolDestroy(&pPool->paramPool);
      taosThreadMutexDestroy(&pPool->poolLock);
      taosThreadMutexDestroy(&pPool->mapLock);
      return code;
    }
  }

  pPool->numWorkers = numWorkers;
  pPool->shuttingDown = false;
  pPool->inited = true;
  stInfo("stream async notify worker pool started (%d workers, param pool init:%" PRId64 " max:%" PRId64
         " slots, %" PRId64 " KB)",
         numWorkers, initCap, poolCap, (int64_t)tsStreamNotifyParamPoolSize);
  return TSDB_CODE_SUCCESS;
}

void streamNotifyWorkerPoolDestroy(SStreamNotifyWorkerPool* pPool) {
  if (!pPool->inited) {
    return;
  }

  streamNotifyWorkerPoolPrepareShutdown(pPool);

  for (int32_t i = 0; i < pPool->numWorkers; i++) {
    streamNotifyWorkerDestroy(&pPool->workers[i]);
  }

  taosMemoryFree(pPool->workers);
  pPool->workers = NULL;
  pPool->numWorkers = 0;

  taosThreadMutexLock(&pPool->mapLock);
  streamNotifyWorkerPoolPurgeQueues(pPool);
  taosThreadMutexUnlock(&pPool->mapLock);

  taosHashCleanup(pPool->queueMap);
  pPool->queueMap = NULL;
  taosObjPoolDestroy(&pPool->paramPool);
  taosThreadMutexDestroy(&pPool->poolLock);
  taosThreadMutexDestroy(&pPool->mapLock);
  tcurlGlobalPoolShutdown();
  pPool->shuttingDown = false;
  pPool->inited = false;
  stInfo("stream async notify worker pool stopped");
}

typedef struct SStreamDataCacheIterKey {
  int64_t groupId;
  TSKEY   start;
  TSKEY   end;
  char    eventConditionPath[32];
} SStreamDataCacheIterKey;

int32_t readStreamDataCache(int64_t streamId, int64_t taskId, int64_t sessionId, int64_t groupId, TSKEY start,
                            TSKEY end, const char* eventConditionPath, void*** pppIter) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask* pTask = NULL;
  void*               taskAddr = NULL;

  *pppIter = NULL;

  code = streamAcquireTask(streamId, taskId, (SStreamTask**)&pTask, &taskAddr);
  QUERY_CHECK_CODE(code, lino, _end);

  QUERY_CHECK_CONDITION(pTask->task.type == STREAM_TRIGGER_TASK, code, lino, _end, TSDB_CODE_STREAM_TASK_NOT_EXIST);

  if (((SStreamTriggerTask*)pTask)->triggerType == STREAM_TRIGGER_PERIOD) {
    start = INT64_MIN;
    end = INT64_MAX;
  } else if (((SStreamTriggerTask*)pTask)->triggerType == STREAM_TRIGGER_SLIDING) {
    if (((SStreamTriggerTask*)pTask)->interval.interval > 0) {
      end--;
    } else {
      start++;
    }
  }
  SHashObj* pCalcDataCacheIters = NULL;
  void*     pCalcDataCache = NULL;
  if (pTask->pRealtimeContext->sessionId == sessionId) {
    pCalcDataCacheIters = pTask->pRealtimeContext->pCalcDataCacheIters;
    pCalcDataCache = pTask->pRealtimeContext->pCalcDataCache;
  } else if (pTask->pHistoryContext->sessionId == sessionId) {
    pCalcDataCacheIters = pTask->pHistoryContext->pCalcDataCacheIters;
    pCalcDataCache = pTask->pHistoryContext->pCalcDataCache;
  } else {
    stsError("sessionId %" PRId64 " not found in task %" PRId64, sessionId, pTask->task.taskId);
    code = TSDB_CODE_INTERNAL_ERROR;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  SStreamDataCacheIterKey key = {.groupId = groupId, .start = start, .end = end};
  if (eventConditionPath != NULL) {
    tstrncpy(key.eventConditionPath, eventConditionPath, sizeof(key.eventConditionPath));
  }
  void** px = taosHashGet(pCalcDataCacheIters, &key, sizeof(key));
  if (px == NULL) {
    void* pIter = NULL;
    code = taosHashPut(pCalcDataCacheIters, &key, sizeof(key), &pIter, POINTER_BYTES);
    QUERY_CHECK_CODE(code, lino, _end);
    px = taosHashGet(pCalcDataCacheIters, &key, sizeof(key));
    QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INVALID_PARA);
  }
  if (*px == NULL) {
    code = getStreamDataCache(pCalcDataCache, groupId, start, end, px);
    QUERY_CHECK_CODE(code, lino, _end);
    if (*px != NULL && pTask->triggerType == STREAM_TRIGGER_EVENT && pTask->pEventTree != NULL) {
      ((SResultIter*)*px)->exactWindow = true;
      tstrncpy(((SResultIter*)*px)->eventConditionPath, key.eventConditionPath,
               sizeof(((SResultIter*)*px)->eventConditionPath));
    }
  }
  *pppIter = px;

_end:

  streamReleaseTask(taskAddr);

  if (code != TSDB_CODE_SUCCESS) {
    stsError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}
