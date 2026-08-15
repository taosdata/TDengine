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
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SListIter iter = {0};
  SListNode* listNode = NULL;
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
  
  stsDebug("total %d:%d tasks status added to hb", (int32_t)taosArrayGetSize(pMsg->pStreamStatus) - origTaskNum, pStream->taskNum);

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

  SListIter iter = {0};
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

#define JSON_CHECK_ADD_ITEM_SAFE(obj, str, itemVar) \
  do { \
    if (!cJSON_AddItemToObjectCS((obj), (str), (itemVar))) { \
      cJSON_Delete(itemVar); \
      (itemVar) = NULL; \
      code = TSDB_CODE_OUT_OF_MEMORY; \
      lino = __LINE__; \
      goto _end; \
    } \
  } while (0)

#define JSON_CHECK_ADD_ARRAY_ITEM(arr, itemVar) \
  do { \
    if (!cJSON_AddItemToArray((arr), (itemVar))) { \
      cJSON_Delete(itemVar); \
      (itemVar) = NULL; \
      code = TSDB_CODE_OUT_OF_MEMORY; \
      lino = __LINE__; \
      goto _end; \
    } \
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

static int32_t jsonAddColumnField(const char* colName, const SColumnInfo* colInfo,
                                  bool isNull, const char* pData, cJSON* obj) {
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
    stError("%s failed at line %d since %s", __func__, lino,
            tstrerror(code));
  }
  return code;
}

static int32_t jsonAddStateArrayField(const char* fieldName, const SArray* pStateCols,
                                      const SArray* pStateVals, const bool* pDefined,
                                      cJSON* obj) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  cJSON*  arr = NULL;

  QUERY_CHECK_NULL(fieldName, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pStateCols, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(obj, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(pStateVals == NULL || taosArrayGetSize((SArray*)pStateVals) == taosArrayGetSize((SArray*)pStateCols),
                        code, lino, _end, TSDB_CODE_INVALID_PARA);

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
                                           const SArray* pFromStates, const bool* pFromDefined,
                                           const SArray* pToStates, const bool* pToDefined,
                                           char** ppContent) {
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
      code = jsonAddStateArrayField("prevState", pStateCols, pFromStates,
                                    pFromDefined, obj);
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

static void streamBuildNotifyTriggerId(int64_t groupId, int64_t windowStart, int32_t winIdx, char* triggerId);

int32_t streamBuildEventNotifyContent(const SSDataBlock* pInputBlock, const SNodeList* pCondCols, int32_t rowIdx,
                                      int32_t condIdx, int32_t winIdx, int64_t groupId, int64_t windowStart,
                                      int64_t parentWindowStart, char** ppContent) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  const SNode* pNode = NULL;
  cJSON*       obj = NULL;
  cJSON*       cond = NULL;
  cJSON*       fields = NULL;

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
  JSON_CHECK_ADD_ITEM(cond, "conditionIndex", cJSON_CreateNumber(condIdx));
  JSON_CHECK_ADD_ITEM(cond, "fieldValues", fields);
  fields = NULL;

  obj = cJSON_CreateObject();
  QUERY_CHECK_NULL(obj, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  char triggerId[32];
  streamBuildNotifyTriggerId(groupId, windowStart, winIdx, triggerId);
  JSON_CHECK_ADD_ITEM(obj, "triggerId", cJSON_CreateString(triggerId));
  JSON_CHECK_ADD_ITEM(obj, "triggerCondition", cond);
  JSON_CHECK_ADD_ITEM(obj, "windowIndex", cJSON_CreateNumber(winIdx));
  if (winIdx >= 0) {
    char parentTriggerId[32];
    streamBuildNotifyTriggerId(groupId, parentWindowStart, -1, parentTriggerId);
    JSON_CHECK_ADD_ITEM(obj, "parentTriggerId", cJSON_CreateString(parentTriggerId));
  }
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

static void streamBuildNotifyTriggerId(int64_t groupId, int64_t windowStart, int32_t winIdx, char* triggerId) {
  uint64_t hash = 0;
  if (winIdx >= 0) {
    uint64_t ar[] = {(uint64_t)groupId, (uint64_t)windowStart, (uint64_t)(uint32_t)winIdx};
    hash = MurmurHash3_64((const char*)ar, sizeof(ar));
  } else {
    uint64_t ar[] = {(uint64_t)groupId, (uint64_t)windowStart};
    hash = MurmurHash3_64((const char*)ar, sizeof(ar));
  }
  (void)u64toaFastLut(hash, triggerId);
}

static int32_t streamAppendNotifyContent(int32_t triggerType, int64_t groupId, const SSTriggerCalcParam* pParam,
                                         SStringBuilder* pBuilder, const char* tableName) {
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
    streamBuildNotifyTriggerId(groupId, pParam->wstart, -1, triggerId);
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
  JSON_CHECK_ADD_ITEM(obj, "eventTime", cJSON_CreateNumber(taosGetTimestampMs()));
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
    JSON_CHECK_ADD_ITEM(obj, "idleStart", cJSON_CreateNumber(pParam->idlestart));
    JSON_CHECK_ADD_ITEM(obj, "idleEnd", cJSON_CreateNumber(pParam->idleend));
  } else if (pParam->notifyType != STRIGGER_EVENT_ON_TIME) {
    JSON_CHECK_ADD_ITEM(obj, "windowStart", cJSON_CreateNumber(pParam->wstart));
    if (pParam->notifyType == STRIGGER_EVENT_WINDOW_CLOSE) {
      int64_t wend = pParam->wend;
      JSON_CHECK_ADD_ITEM(obj, "windowEnd", cJSON_CreateNumber(wend));
    }
  } else if (triggerType == STREAM_TRIGGER_PERIOD) {
    JSON_CHECK_ADD_ITEM(obj, "windowStart", cJSON_CreateNumber(pParam->triggerTime));
    JSON_CHECK_ADD_ITEM(obj, "windowEnd", cJSON_CreateNumber(pParam->triggerTime));
  } else if (triggerType == STREAM_TRIGGER_SLIDING) {
    JSON_CHECK_ADD_ITEM(obj, "windowStart", cJSON_CreateNumber(pParam->prevTs));
    JSON_CHECK_ADD_ITEM(obj, "windowEnd", cJSON_CreateNumber(pParam->currentTs));
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

int32_t streamSendNotifyContent(SStreamTask* pTask, const char* streamName, const char* tableName, int32_t triggerType,
                                int64_t groupId, const SArray* pNotifyAddrUrls, int32_t addOptions,
                                const SSTriggerCalcParam* pParams, int32_t nParam) {
  return streamSendNotifyContentWithResult(pTask, streamName, tableName, triggerType, groupId, pNotifyAddrUrls,
                                           addOptions, pParams, nParam, NULL, NULL);
}

int32_t streamSendNotifyContentWithResult(SStreamTask* pTask, const char* streamName, const char* tableName,
                                          int32_t triggerType, int64_t groupId, const SArray* pNotifyAddrUrls,
                                          int32_t addOptions, const SSTriggerCalcParam* pParams, int32_t nParam,
                                          bool* pAttempted, bool* pDelivered) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SStringBuilder sb = {0};
  const char*    msgTail = "]}]}";
  char*          msg = NULL;
  SCURL          conn = {0};
  bool           shouldNotify = false;
  bool           attempted = false;
  bool           delivered = false;
  bool           allTargetsDelivered = true;

  if (pAttempted != NULL) *pAttempted = false;
  if (pDelivered != NULL) *pDelivered = false;

  // Remove prefix 1. 
  char*          pos = strstr(streamName, TS_PATH_DELIMITER);
  if (pos != NULL) streamName = ++pos;

  if (nParam <= 0 || taosArrayGetSize(pNotifyAddrUrls) <= 0) {
    goto _end;
  }

  for (int32_t i = 0; i < nParam; ++i) {
    if (pParams[i].notifyType != STRIGGER_EVENT_WINDOW_NONE) {
      shouldNotify = true;
      break;
    }
  }

  if (!shouldNotify) {
    goto _end;
  }

  for (int32_t i = 0; i < TARRAY_SIZE(pNotifyAddrUrls); ++i) {
    char** pUrl = TARRAY_GET_ELEM(pNotifyAddrUrls, i);
    if (pUrl != NULL && *pUrl != NULL) {
      attempted = true;
      break;
    }
  }
  if (!attempted) goto _end;

  taosStringBuilderEnsureCapacity(&sb, 1024);
  size_t msgTailLen = strlen(msgTail);

  code = streamAppendNotifyHeader(streamName, &sb);
  QUERY_CHECK_CODE(code, lino, _end);
  sb.pos -= msgTailLen;
  int32_t nSentParams = 0;
  for (int32_t i = 0; i < nParam; ++i) {
    if (pParams[i].notifyType == STRIGGER_EVENT_WINDOW_NONE) {
      continue;
    }
    code = streamAppendNotifyContent(triggerType, groupId, &pParams[i], &sb, tableName);
    QUERY_CHECK_CODE(code, lino, _end);
    taosStringBuilderAppendChar(&sb, ',');
    nSentParams++;
  }
  sb.pos -= 1;
  taosStringBuilderAppendStringLen(&sb, msgTail, msgTailLen);
  msg = taosStringBuilderGetResult(&sb, NULL);

  for (int32_t i = 0; i < TARRAY_SIZE(pNotifyAddrUrls); ++i) {
    char** pUrl = TARRAY_GET_ELEM(pNotifyAddrUrls, i);
    if (*pUrl == NULL) {
      continue;
    }

    // todo(kjq): check if task should stop
    conn.url = taosStrdup(*pUrl);
    QUERY_CHECK_NULL(conn.url, code, lino, _end, terrno);
    code = tcurlConnect(&conn.pConn, *pUrl);
    if (code != TSDB_CODE_SUCCESS) {
      ST_TASK_ELOG("failed to get stream notify handle of %s", *pUrl);
      tcurlClose(&conn);
      if (addOptions & NOTIFY_ON_FAILURE_PAUSE) {
        // retry for event message sending in PAUSE error handling mode
        taosMsleep(STREAM_EVENT_NOTIFY_RETRY_MS);
        --i;
        continue;
      } else {
        // simply ignore the failure in DROP error handling mode
        allTargetsDelivered = false;
        code = TSDB_CODE_SUCCESS;
        continue;
      }
    }

    size_t  totalLen = sb.pos;
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
      ST_TASK_ELOG("failed to send stream notify msg to %s for %d", *pUrl, res);
      if (addOptions & NOTIFY_ON_FAILURE_PAUSE) {
        // retry for event message sending in PAUSE error handling mode
        taosMsleep(STREAM_EVENT_NOTIFY_RETRY_MS);
        --i;
      } else {
        // simply ignore the failure in DROP error handling mode
        allTargetsDelivered = false;
        code = TSDB_CODE_SUCCESS;
      }
    } else {
      ST_TASK_DLOG("notify %d events to %s successfully", nSentParams, *pUrl);
    }
  }

_end:
  delivered = attempted && allTargetsDelivered && code == TSDB_CODE_SUCCESS;
  if (pAttempted != NULL) *pAttempted = attempted;
  if (pDelivered != NULL) *pDelivered = delivered;
  tcurlClose(&conn);
  taosStringBuilderDestroy(&sb);
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t readStreamDataCache(int64_t streamId, int64_t taskId, int64_t sessionId, int64_t groupId, TSKEY start,
                            TSKEY end, void*** pppIter) {
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

  void** px = taosHashGet(pCalcDataCacheIters, &groupId, sizeof(int64_t));
  if (px == NULL) {
    void* pIter = NULL;
    code = taosHashPut(pCalcDataCacheIters, &groupId, sizeof(int64_t), &pIter, POINTER_BYTES);
    QUERY_CHECK_CODE(code, lino, _end);
    px = taosHashGet(pCalcDataCacheIters, &groupId, sizeof(int64_t));
    QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INVALID_PARA);
  }
  if (*px == NULL) {
    code = getStreamDataCache(pCalcDataCache, groupId, start, end, px);
    QUERY_CHECK_CODE(code, lino, _end);
  }
  *pppIter = px;

_end:

  streamReleaseTask(taskAddr);

  if (code != TSDB_CODE_SUCCESS) {
    stsError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}
