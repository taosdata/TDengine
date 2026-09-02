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
#include "tmd5.h"
#include "tstrbuild.h"
#include "tworker.h"

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

static void stmDestroyRecalcDetail(void* param) {
  SStreamRecalcDetail* pDetail = param;
  taosMemoryFreeClear(pDetail->errorText);
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
  entry.snapshot.pRecalcDetails = NULL;

_exit:
  taosArrayDestroy(entry.snapshot.pRecalculates);
  taosArrayDestroyEx(entry.snapshot.pRecalcDetails, stmDestroyRecalcDetail);
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
  size_t tempLen = strlen(temp);
  size_t required = pBuilder->pos + tempLen;
  if (required > pBuilder->size) {
    size_t capacity = required * 2;
    char*  pBuf = taosMemoryRealloc(pBuilder->buf, capacity);
    QUERY_CHECK_NULL(pBuf, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
    pBuilder->buf = pBuf;
    pBuilder->size = capacity;
  }
  (void)memcpy(pBuilder->buf + pBuilder->pos, temp, tempLen);
  pBuilder->pos = required;

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

static void stNestedTriggerIdUpdateI32(T_MD5_CTX* pContext, int32_t value) {
  uint32_t encoded = (uint32_t)value;
  uint8_t  bytes[sizeof(encoded)] = {
      (uint8_t)(encoded >> 24),
      (uint8_t)(encoded >> 16),
      (uint8_t)(encoded >> 8),
      (uint8_t)encoded,
  };
  tMD5Update(pContext, bytes, sizeof(bytes));
}

static void stNestedTriggerIdUpdateI64(T_MD5_CTX* pContext, int64_t value) {
  uint64_t encoded = (uint64_t)value;
  uint8_t  bytes[sizeof(encoded)] = {
      (uint8_t)(encoded >> 56), (uint8_t)(encoded >> 48), (uint8_t)(encoded >> 40), (uint8_t)(encoded >> 32),
      (uint8_t)(encoded >> 24), (uint8_t)(encoded >> 16), (uint8_t)(encoded >> 8),  (uint8_t)encoded,
  };
  tMD5Update(pContext, bytes, sizeof(bytes));
}

int32_t stBuildNestedTriggerId(int64_t gid, const SWindowLineage* pLineage, TSKEY leafStart, int32_t windowIndex,
                               char triggerId[STREAM_NESTED_TRIGGER_ID_LEN]) {
  if (pLineage == NULL || pLineage->pScopes == NULL || triggerId == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  triggerId[0] = '\0';
  int32_t scopeCount = taosArrayGetSize(pLineage->pScopes);
  if (scopeCount <= 0 || scopeCount >= STREAM_WINDOW_MAX_LAYERS ||
      pLineage->pScopes->elemSize != sizeof(SScopeInstanceId)) {
    return TSDB_CODE_INVALID_PARA;
  }

  T_MD5_CTX context = {0};
  tMD5Init(&context);
  uint8_t version = 1;
  tMD5Update(&context, &version, sizeof(version));
  stNestedTriggerIdUpdateI64(&context, gid);
  uint8_t lineageDepth = (uint8_t)scopeCount;
  tMD5Update(&context, &lineageDepth, sizeof(lineageDepth));
  for (int32_t i = 0; i < scopeCount; ++i) {
    const SScopeInstanceId* pScope = taosArrayGet(pLineage->pScopes, i);
    if (pScope == NULL || pScope->layerIndex != i) {
      return TSDB_CODE_INVALID_PARA;
    }
    stNestedTriggerIdUpdateI64(&context, pScope->openingTs);
  }
  stNestedTriggerIdUpdateI64(&context, leafStart);
  if (windowIndex >= 0) stNestedTriggerIdUpdateI32(&context, windowIndex);
  tMD5Final(&context);

  int32_t len = snprintf(
      triggerId, STREAM_NESTED_TRIGGER_ID_LEN, "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x",
      context.digest[0], context.digest[1], context.digest[2], context.digest[3], context.digest[4], context.digest[5],
      context.digest[6], context.digest[7], context.digest[8], context.digest[9], context.digest[10],
      context.digest[11], context.digest[12], context.digest[13], context.digest[14], context.digest[15]);
  return len == STREAM_NESTED_TRIGGER_ID_LEN - 1 ? TSDB_CODE_SUCCESS : TSDB_CODE_FAILED;
}

typedef struct {
  char*    pPayload;
  int32_t  payloadLen;
} SStreamTriggerNoticeItem;

typedef struct {
  int32_t urlLen;
  int32_t payloadLen;
  char    data[];
} SStreamQueuedNotice;

#define STREAM_NOTICE_QUEUE_CAPACITY     4096
#define STREAM_NOTICE_QUEUE_MEMORY_BYTES (64LL * 1024 * 1024)

static SSingleWorker gStreamNoticeWorker = {0};
static bool          gStreamNoticeWorkerReady = false;

struct SStreamTriggerNoticeBatch {
  const SArray* pNotifyAddrUrls;
  SList         staged;
};

static void stTriggerNoticeItemDestroy(SStreamTriggerNoticeItem* pItem) {
  if (pItem == NULL) {
    return;
  }
  taosMemoryFreeClear(pItem->pPayload);
}

static void stTriggerNoticeListClear(SList* pList) {
  if (pList == NULL) {
    return;
  }

  SListNode* pNode = NULL;
  while ((pNode = tdListPopHead(pList)) != NULL) {
    stTriggerNoticeItemDestroy((SStreamTriggerNoticeItem*)pNode->data);
    taosMemoryFree(pNode);
  }
}

int32_t stTriggerNoticeBatchCreate(SStreamTriggerNoticeBatch** ppBatch) {
  if (ppBatch == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  *ppBatch = NULL;
  SStreamTriggerNoticeBatch* pBatch = taosMemoryCalloc(1, sizeof(*pBatch));
  if (pBatch == NULL) {
    return terrno;
  }
  tdListInit(&pBatch->staged, sizeof(SStreamTriggerNoticeItem));
  *ppBatch = pBatch;
  return TSDB_CODE_SUCCESS;
}

static int32_t streamNestedNotifyEventType(int32_t notifyType, const char** ppEventType) {
  if (ppEventType == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  switch (notifyType) {
    case STRIGGER_EVENT_WINDOW_OPEN:
      *ppEventType = "WINDOW_OPEN";
      return TSDB_CODE_SUCCESS;
    case STRIGGER_EVENT_WINDOW_CLOSE:
      *ppEventType = "WINDOW_CLOSE";
      return TSDB_CODE_SUCCESS;
    case STRIGGER_EVENT_IDLE:
      *ppEventType = "IDLE";
      return TSDB_CODE_SUCCESS;
    case STRIGGER_EVENT_RESUME:
      *ppEventType = "RESUME";
      return TSDB_CODE_SUCCESS;
    case STRIGGER_EVENT_ON_TIME:
      *ppEventType = "ON_TIME";
      return TSDB_CODE_SUCCESS;
    default:
      *ppEventType = NULL;
      return TSDB_CODE_INVALID_PARA;
  }
}

static bool streamNestedNotifyUsesLeafIdentity(int32_t notifyType) {
  return notifyType == STRIGGER_EVENT_WINDOW_OPEN || notifyType == STRIGGER_EVENT_WINDOW_CLOSE ||
         notifyType == STRIGGER_EVENT_ON_TIME;
}

static int32_t streamNestedNotifyTriggerType(int32_t triggerType, int32_t notifyType, const char** ppTriggerType) {
  if (ppTriggerType == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  switch (triggerType) {
    case STREAM_TRIGGER_PERIOD:
      *ppTriggerType = "Period";
      return TSDB_CODE_SUCCESS;
    case STREAM_TRIGGER_SLIDING:
      *ppTriggerType = notifyType == STRIGGER_EVENT_ON_TIME ? "Sliding" : "Interval";
      return TSDB_CODE_SUCCESS;
    case STREAM_TRIGGER_SESSION:
      *ppTriggerType = "Session";
      return TSDB_CODE_SUCCESS;
    case STREAM_TRIGGER_COUNT:
      *ppTriggerType = "Count";
      return TSDB_CODE_SUCCESS;
    case STREAM_TRIGGER_STATE:
      *ppTriggerType = "State";
      return TSDB_CODE_SUCCESS;
    case STREAM_TRIGGER_EVENT:
      *ppTriggerType = "Event";
      return TSDB_CODE_SUCCESS;
    default:
      *ppTriggerType = NULL;
      return TSDB_CODE_INVALID_PARA;
  }
}

static int32_t streamMergeNestedNotifyContent(cJSON* pTarget, const char* pContent) {
  if (pContent == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  cJSON* pSource = cJSON_Parse(pContent);
  if (!cJSON_IsObject(pSource)) {
    cJSON_Delete(pSource);
    return TSDB_CODE_INVALID_PARA;
  }

  cJSON* pItem = pSource->child;
  while (pItem != NULL) {
    cJSON* pNext = pItem->next;
    cJSON* pDetached = cJSON_DetachItemViaPointer(pSource, pItem);
    if (pDetached == NULL) {
      cJSON_Delete(pSource);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    if (pDetached->string != NULL &&
        (strcmp(pDetached->string, "triggerId") == 0 || strcmp(pDetached->string, "parentTriggerId") == 0)) {
      cJSON_Delete(pDetached);
      pItem = pNext;
      continue;
    }

    bool   added = false;
    cJSON* pExisting = pDetached->string == NULL ? NULL : cJSON_GetObjectItemCaseSensitive(pTarget, pDetached->string);
    if (pExisting != NULL) {
      added = cJSON_ReplaceItemViaPointer(pTarget, pExisting, pDetached);
    } else if (pDetached->string != NULL) {
      added = cJSON_AddItemToObject(pTarget, pDetached->string, pDetached);
    }
    if (!added) {
      cJSON_Delete(pDetached);
      cJSON_Delete(pSource);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    pItem = pNext;
  }

  cJSON_Delete(pSource);
  return TSDB_CODE_SUCCESS;
}

static bool streamIsCanonicalNestedTriggerId(const char* pTriggerId) {
  if (pTriggerId == NULL || strnlen(pTriggerId, STREAM_NESTED_TRIGGER_ID_LEN) != STREAM_NESTED_TRIGGER_ID_LEN - 1) {
    return false;
  }
  for (int32_t i = 0; i < STREAM_NESTED_TRIGGER_ID_LEN - 1; ++i) {
    const char c = pTriggerId[i];
    if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
      return false;
    }
  }
  return true;
}

typedef struct {
  bool    hasWindowIndex;
  int32_t windowIndex;
  bool    hasParentTriggerId;
  char    parentTriggerId[STREAM_NESTED_TRIGGER_ID_LEN];
} SStreamNestedEventIdentityMetadata;

static int32_t streamReadNestedEventIdentityMetadata(const char* pContent, int32_t triggerType, int32_t notifyType,
                                                     SStreamNestedEventIdentityMetadata* pMetadata) {
  *pMetadata = (SStreamNestedEventIdentityMetadata){0};
  const bool eventIdentity = triggerType == STREAM_TRIGGER_EVENT &&
                             (notifyType == STRIGGER_EVENT_WINDOW_OPEN || notifyType == STRIGGER_EVENT_WINDOW_CLOSE);
  if (pContent == NULL) return eventIdentity ? TSDB_CODE_INVALID_PARA : TSDB_CODE_SUCCESS;

  cJSON* pObject = cJSON_Parse(pContent);
  if (!cJSON_IsObject(pObject)) {
    cJSON_Delete(pObject);
    return TSDB_CODE_INVALID_PARA;
  }

  const cJSON* pWindowIndex = NULL;
  const cJSON* pParentTriggerId = NULL;
  int32_t      windowIndexCount = 0;
  int32_t      parentTriggerIdCount = 0;
  for (const cJSON* pItem = pObject->child; pItem != NULL; pItem = pItem->next) {
    if (pItem->string == NULL) continue;
    if (strcmp(pItem->string, "windowIndex") == 0) {
      pWindowIndex = pItem;
      ++windowIndexCount;
    } else if (strcmp(pItem->string, "parentTriggerId") == 0) {
      pParentTriggerId = pItem;
      ++parentTriggerIdCount;
    }
  }

  if (!eventIdentity) {
    cJSON_Delete(pObject);
    return windowIndexCount == 0 ? TSDB_CODE_SUCCESS : TSDB_CODE_INVALID_PARA;
  }

  if (windowIndexCount != 1 || parentTriggerIdCount > 1) {
    cJSON_Delete(pObject);
    return TSDB_CODE_INVALID_PARA;
  }
  if (pWindowIndex != NULL) {
    if (!cJSON_IsNumber(pWindowIndex)) {
      cJSON_Delete(pObject);
      return TSDB_CODE_INVALID_PARA;
    }
    const double value = cJSON_GetNumberValue(pWindowIndex);
    if (!(value >= INT32_MIN && value <= INT32_MAX)) {
      cJSON_Delete(pObject);
      return TSDB_CODE_INVALID_PARA;
    }
    const int32_t index = (int32_t)value;
    if ((double)index != value || index < -1) {
      cJSON_Delete(pObject);
      return TSDB_CODE_INVALID_PARA;
    }
    pMetadata->hasWindowIndex = true;
    pMetadata->windowIndex = index;
  }

  if (pParentTriggerId != NULL && !pMetadata->hasWindowIndex) {
    cJSON_Delete(pObject);
    return TSDB_CODE_INVALID_PARA;
  }
  if (pMetadata->hasWindowIndex && pMetadata->windowIndex >= 0) {
    if (pParentTriggerId == NULL || !cJSON_IsString(pParentTriggerId) ||
        !streamIsCanonicalNestedTriggerId(cJSON_GetStringValue(pParentTriggerId))) {
      cJSON_Delete(pObject);
      return TSDB_CODE_INVALID_PARA;
    }
    pMetadata->hasParentTriggerId = true;
    memcpy(pMetadata->parentTriggerId, cJSON_GetStringValue(pParentTriggerId), STREAM_NESTED_TRIGGER_ID_LEN);
  } else if (pParentTriggerId != NULL) {
    cJSON_Delete(pObject);
    return TSDB_CODE_INVALID_PARA;
  }
  cJSON_Delete(pObject);
  return TSDB_CODE_SUCCESS;
}

static int32_t streamNestedLeafRuntimeTriggerType(int8_t leafTriggerType) {
  switch (leafTriggerType) {
    case WINDOW_TYPE_INTERVAL:
      return STREAM_TRIGGER_SLIDING;
    case WINDOW_TYPE_SESSION:
      return STREAM_TRIGGER_SESSION;
    case WINDOW_TYPE_STATE:
      return STREAM_TRIGGER_STATE;
    case WINDOW_TYPE_EVENT:
      return STREAM_TRIGGER_EVENT;
    case WINDOW_TYPE_COUNT:
      return STREAM_TRIGGER_COUNT;
    default:
      return -1;
  }
}

int32_t stResolveNestedLeafWindowIndex(int32_t runtimeTriggerType, const SLeafInstanceId* pLeafIdentity,
                                       int32_t notifyType, const char* pExtraNotifyContent, int32_t* pWindowIndex) {
  if (pLeafIdentity == NULL || pWindowIndex == NULL || !streamNestedNotifyUsesLeafIdentity(notifyType) ||
      streamNestedLeafRuntimeTriggerType(pLeafIdentity->triggerType) != runtimeTriggerType) {
    return TSDB_CODE_INVALID_PARA;
  }

  *pWindowIndex = -1;
  SStreamNestedEventIdentityMetadata metadata = {0};
  int32_t code = streamReadNestedEventIdentityMetadata(pExtraNotifyContent, runtimeTriggerType, notifyType, &metadata);
  if (code != TSDB_CODE_SUCCESS) return code;
  if (pLeafIdentity->triggerType != WINDOW_TYPE_EVENT) return TSDB_CODE_SUCCESS;
  if (!metadata.hasWindowIndex || pLeafIdentity->nativeDiscriminator < -1 ||
      pLeafIdentity->nativeDiscriminator > INT32_MAX || pLeafIdentity->nativeDiscriminator != metadata.windowIndex) {
    return TSDB_CODE_INVALID_PARA;
  }
  *pWindowIndex = metadata.windowIndex;
  return TSDB_CODE_SUCCESS;
}

static int32_t streamAddNotifyCommonFields(cJSON* pObject, const char* pEventType, const char* pTriggerType,
                                           int64_t gid) {
  if (pObject == NULL || pEventType == NULL || pTriggerType == NULL) return TSDB_CODE_INVALID_PARA;

  char gidBuf[32] = {0};
  snprintf(gidBuf, sizeof(gidBuf), "%" PRId64, gid);
  if (cJSON_AddStringToObject(pObject, "eventType", pEventType) == NULL ||
      cJSON_AddNumberToObject(pObject, "eventTime", taosGetTimestampMs()) == NULL ||
      cJSON_AddStringToObject(pObject, "triggerType", pTriggerType) == NULL ||
      cJSON_AddStringToObject(pObject, "groupId", gidBuf) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t streamBuildNestedNotifyContent(const char* pTableName, int64_t gid, int32_t triggerType,
                                              const SSTriggerCalcParam* pParam, const char* pTriggerId,
                                              char** ppContent) {
  if (pParam == NULL || ppContent == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  char        groupTriggerId[32] = {0};
  const char* pEffectiveTriggerId = pTriggerId;
  if (streamNestedNotifyUsesLeafIdentity(pParam->notifyType)) {
    if (!streamIsCanonicalNestedTriggerId(pTriggerId)) return TSDB_CODE_INVALID_PARA;
  } else if (pParam->notifyType == STRIGGER_EVENT_IDLE || pParam->notifyType == STRIGGER_EVENT_RESUME) {
    streamBuildNotifyTriggerId(gid, pParam->wstart, -1, groupTriggerId);
    pEffectiveTriggerId = groupTriggerId;
  } else {
    return TSDB_CODE_INVALID_PARA;
  }

  *ppContent = NULL;
  int32_t                            code = TSDB_CODE_SUCCESS;
  const char*                        pEventType = NULL;
  const char*                        pTriggerType = NULL;
  SStreamNestedEventIdentityMetadata eventIdentity = {0};
  code = streamNestedNotifyEventType(pParam->notifyType, &pEventType);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  code = streamNestedNotifyTriggerType(triggerType, pParam->notifyType, &pTriggerType);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  code = streamReadNestedEventIdentityMetadata(pParam->extraNotifyContent, triggerType, pParam->notifyType,
                                               &eventIdentity);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  cJSON* pObject = cJSON_CreateObject();
  if (pObject == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  code = streamAddNotifyCommonFields(pObject, pEventType, pTriggerType, gid);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  if (pTableName != NULL && cJSON_AddStringToObject(pObject, "tableName", pTableName) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  if (pParam->notifyType == STRIGGER_EVENT_IDLE || pParam->notifyType == STRIGGER_EVENT_RESUME) {
    if (cJSON_AddNumberToObject(pObject, "idleStart", pParam->idlestart) == NULL ||
        cJSON_AddNumberToObject(pObject, "idleEnd", pParam->idleend) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
  } else if (pParam->notifyType != STRIGGER_EVENT_ON_TIME) {
    if (cJSON_AddNumberToObject(pObject, "windowStart", pParam->wstart) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
    if (pParam->notifyType == STRIGGER_EVENT_WINDOW_CLOSE &&
        cJSON_AddNumberToObject(pObject, "windowEnd", pParam->wend) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
  } else if (triggerType == STREAM_TRIGGER_PERIOD) {
    if (cJSON_AddNumberToObject(pObject, "windowStart", pParam->triggerTime) == NULL ||
        cJSON_AddNumberToObject(pObject, "windowEnd", pParam->triggerTime) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
  } else if (triggerType == STREAM_TRIGGER_SLIDING) {
    if (cJSON_AddNumberToObject(pObject, "windowStart", pParam->prevTs) == NULL ||
        cJSON_AddNumberToObject(pObject, "windowEnd", pParam->currentTs) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
  }

  code = streamMergeNestedNotifyContent(pObject, pParam->extraNotifyContent);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }
  code = streamMergeNestedNotifyContent(pObject, pParam->resultNotifyContent);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }
  cJSON_DeleteItemFromObjectCaseSensitive(pObject, "triggerId");
  cJSON_DeleteItemFromObjectCaseSensitive(pObject, "parentTriggerId");
  if (eventIdentity.hasWindowIndex) {
    cJSON* pWindowIndex = cJSON_CreateNumber(eventIdentity.windowIndex);
    if (pWindowIndex == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
    cJSON* pExisting = cJSON_GetObjectItemCaseSensitive(pObject, "windowIndex");
    bool   added = pExisting == NULL ? cJSON_AddItemToObject(pObject, "windowIndex", pWindowIndex)
                                     : cJSON_ReplaceItemInObjectCaseSensitive(pObject, "windowIndex", pWindowIndex);
    if (!added) {
      cJSON_Delete(pWindowIndex);
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
  }
  if (cJSON_AddStringToObject(pObject, "triggerId", pEffectiveTriggerId) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  if (eventIdentity.hasParentTriggerId &&
      cJSON_AddStringToObject(pObject, "parentTriggerId", eventIdentity.parentTriggerId) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  *ppContent = cJSON_PrintUnformatted(pObject);
  if (*ppContent == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }

_exit:
  cJSON_Delete(pObject);
  return code;
}

int32_t streamBuildNestedTriggerWindowNotice(const SStreamTriggerTask* pTask, const char* pTableName, int64_t gid,
                                             int32_t triggerType, const SStreamNestedPendingCalcEvent* pEvent,
                                             char** ppPayload, int32_t* pPayloadLen) {
  if (pTask == NULL || pTask->streamName == NULL || pEvent == NULL || ppPayload == NULL || pPayloadLen == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  *ppPayload = NULL;
  *pPayloadLen = 0;
  int32_t        code = TSDB_CODE_SUCCESS;
  char           triggerId[STREAM_NESTED_TRIGGER_ID_LEN] = {0};
  char*          pContent = NULL;
  SStringBuilder builder = {0};
  if (taosStringBuilderSetJmp(&builder) != 0) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  if (gid != pEvent->leafIdentity.gid) {
    code = TSDB_CODE_INVALID_PARA;
    goto _exit;
  }
  int32_t windowIndex = -1;
  code = stResolveNestedLeafWindowIndex(triggerType, &pEvent->leafIdentity, pEvent->calcParam.notifyType,
                                        pEvent->calcParam.extraNotifyContent, &windowIndex);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  code = stBuildNestedTriggerId(pEvent->leafIdentity.gid, &pEvent->leafIdentity.lineage, pEvent->leafIdentity.openingTs,
                                windowIndex, triggerId);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }
  code = streamBuildNestedNotifyContent(pTableName, gid, triggerType, &pEvent->calcParam, triggerId, &pContent);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }

  const char* pStreamName = pTask->streamName;
  const char* pDelimiter = strstr(pStreamName, TS_PATH_DELIMITER);
  if (pDelimiter != NULL) {
    pStreamName = pDelimiter + 1;
  }
  code = streamAppendNotifyHeader(pStreamName, &builder);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }

  const char* pTail = "]}]}";
  size_t      tailLen = strlen(pTail);
  if (builder.pos < tailLen) {
    code = TSDB_CODE_INTERNAL_ERROR;
    goto _exit;
  }
  builder.pos -= tailLen;
  taosStringBuilderAppendString(&builder, pContent);
  taosStringBuilderAppendStringLen(&builder, pTail, tailLen);

  size_t payloadLen = 0;
  char*  pPayload = taosStringBuilderGetResult(&builder, &payloadLen);
  if (payloadLen > INT32_MAX) {
    code = TSDB_CODE_INVALID_PARA;
    goto _exit;
  }
  *ppPayload = pPayload;
  *pPayloadLen = (int32_t)payloadLen;
  builder.buf = NULL;
  builder.pos = 0;
  builder.size = 0;

_exit:
  cJSON_free(pContent);
  taosStringBuilderDestroy(&builder);
  return code;
}

int32_t stTriggerNoticeBatchStageWindow(SStreamTriggerNoticeBatch* pBatch, const SStreamTriggerTask* pTask,
                                        const char* pTableName, int64_t gid, int32_t triggerType,
                                        const SStreamNestedPendingCalcEvent* pEvent) {
  if (pBatch == NULL || pTask == NULL || pEvent == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (pEvent->calcParam.notifyType == STRIGGER_EVENT_WINDOW_NONE ||
      (pTask->notifyEventType & pEvent->calcParam.notifyType) == 0 || pTask->pNotifyAddrUrls == NULL ||
      taosArrayGetSize(pTask->pNotifyAddrUrls) == 0) {
    return TSDB_CODE_SUCCESS;
  }

  bool hasValidDestination = false;
  for (int32_t i = 0; i < taosArrayGetSize(pTask->pNotifyAddrUrls); ++i) {
    char** ppUrl = taosArrayGet(pTask->pNotifyAddrUrls, i);
    if (ppUrl != NULL && *ppUrl != NULL && (*ppUrl)[0] != '\0') {
      hasValidDestination = true;
      break;
    }
  }
  if (!hasValidDestination) {
    return TSDB_CODE_SUCCESS;
  }
  if (pBatch->pNotifyAddrUrls != NULL && pBatch->pNotifyAddrUrls != pTask->pNotifyAddrUrls) {
    return TSDB_CODE_INVALID_PARA;
  }
  pBatch->pNotifyAddrUrls = pTask->pNotifyAddrUrls;

  int32_t code = TSDB_CODE_SUCCESS;
  char*   pPayload = NULL;
  int32_t payloadLen = 0;
  code = streamBuildNestedTriggerWindowNotice(pTask, pTableName, gid, triggerType, pEvent, &pPayload, &payloadLen);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SListNode* pNode = taosMemoryCalloc(1, sizeof(SListNode) + sizeof(SStreamTriggerNoticeItem));
  if (pNode == NULL) {
    taosMemoryFree(pPayload);
    return terrno;
  }
  SStreamTriggerNoticeItem* pItem = (SStreamTriggerNoticeItem*)pNode->data;
  pItem->pPayload = pPayload;
  pItem->payloadLen = payloadLen;
  tdListAppendNode(&pBatch->staged, pNode);
  return TSDB_CODE_SUCCESS;
}

bool stTriggerNoticeBatchHasItems(const SStreamTriggerNoticeBatch* pBatch) {
  return pBatch != NULL && listHead(&pBatch->staged) != NULL;
}

int32_t streamSendNotifyPayloadCached(const char* pUrl, const char* pPayload, int32_t payloadLen) {
  if (pUrl == NULL || pUrl[0] == '\0' || pPayload == NULL || payloadLen <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  SCURL*  pConnection = NULL;
  int32_t code = tcurlGetConnection(pUrl, &pConnection);
  if (code != TSDB_CODE_SUCCESS || pConnection == NULL || pConnection->pConn == NULL) {
    closeThreadNotificationConn();
    return code == TSDB_CODE_SUCCESS ? TSDB_CODE_RPC_NETWORK_UNAVAIL : code;
  }

  size_t totalLen = (size_t)payloadLen;
  size_t sentLen = 0;
  while (sentLen < totalLen) {
    size_t nbytes = 0;
    code = tcurlSend(pConnection, pPayload + sentLen, totalLen - sentLen, &nbytes,
                     sentLen == 0 ? (curl_off_t)totalLen : 0, CURLWS_TEXT | CURLWS_OFFSET);
    if (code != TSDB_CODE_SUCCESS || nbytes == 0 || nbytes > totalLen - sentLen) {
      closeThreadNotificationConn();
      return TSDB_CODE_RPC_NETWORK_UNAVAIL;
    }
    sentLen += nbytes;
  }
  return TSDB_CODE_SUCCESS;
}

static void streamNoticeQueueProcess(SQueueInfo* pInfo, void* pItem) {
  TAOS_UNUSED(pInfo);
  SStreamQueuedNotice* pNotice = pItem;
  const char*          pUrl = pNotice->data;
  const char*          pPayload = pNotice->data + pNotice->urlLen + 1;
  int32_t              code = streamSendNotifyPayloadCached(pUrl, pPayload, pNotice->payloadLen);
  if (code != TSDB_CODE_SUCCESS) {
    stWarn("failed to send nested window notice to %s since %s", pUrl, tstrerror(code));
  }
  taosFreeQitem(pItem);
}

int32_t streamNoticeQueueInit(void) {
  if (gStreamNoticeWorkerReady) return TSDB_CODE_SUCCESS;
  SSingleWorkerCfg cfg = {
      .name = "stream-notice",
      .min = 1,
      .max = 1,
      .fp = streamNoticeQueueProcess,
      .poolType = QUERY_AUTO_QWORKER_POOL,
      .stopNoWaitQueue = true,
      .threadCategory = -1,
  };
  int32_t code = tSingleWorkerInit(&gStreamNoticeWorker, &cfg);
  if (code != TSDB_CODE_SUCCESS) return code;
  taosSetQueueCapacity(gStreamNoticeWorker.queue, STREAM_NOTICE_QUEUE_CAPACITY);
  taosSetQueueMemoryCapacity(gStreamNoticeWorker.queue, STREAM_NOTICE_QUEUE_MEMORY_BYTES);
  gStreamNoticeWorkerReady = true;
  return TSDB_CODE_SUCCESS;
}

void streamNoticeQueueCleanup(void) {
  if (!gStreamNoticeWorkerReady) return;
  gStreamNoticeWorkerReady = false;
  tSingleWorkerCleanup(&gStreamNoticeWorker);
  memset(&gStreamNoticeWorker, 0, sizeof(gStreamNoticeWorker));
}

int32_t streamEnqueueNotifyPayload(const char* pUrl, const char* pPayload, int32_t payloadLen) {
  if (pUrl == NULL || pUrl[0] == '\0' || pPayload == NULL || payloadLen <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (!gStreamNoticeWorkerReady) {
    return streamSendNotifyPayloadOnce(pUrl, pPayload, payloadLen);
  }

  size_t urlLen = strlen(pUrl);
  if (urlLen > INT32_MAX || urlLen + 1 + (size_t)payloadLen > INT32_MAX - sizeof(SStreamQueuedNotice)) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t              itemSize = sizeof(SStreamQueuedNotice) + (int32_t)urlLen + 1 + payloadLen;
  SStreamQueuedNotice* pNotice = NULL;
  int32_t              code = taosAllocateQitem(itemSize, DEF_QITEM, 0, (void**)&pNotice);
  if (code != TSDB_CODE_SUCCESS) return code;
  pNotice->urlLen = (int32_t)urlLen;
  pNotice->payloadLen = payloadLen;
  memcpy(pNotice->data, pUrl, urlLen + 1);
  memcpy(pNotice->data + urlLen + 1, pPayload, payloadLen);
  code = taosWriteQitem(gStreamNoticeWorker.queue, pNotice);
  if (code != TSDB_CODE_SUCCESS) taosFreeQitem(pNotice);
  return code;
}

void stTriggerNoticeBatchSend(SStreamTriggerNoticeBatch** ppBatch) {
  if (ppBatch == NULL || *ppBatch == NULL) {
    return;
  }

  SStreamTriggerNoticeBatch* pBatch = *ppBatch;
  SListNode*                 pNode = NULL;
  while ((pNode = tdListPopHead(&pBatch->staged)) != NULL) {
    SStreamTriggerNoticeItem* pItem = (SStreamTriggerNoticeItem*)pNode->data;
    for (int32_t i = 0; i < taosArrayGetSize(pBatch->pNotifyAddrUrls); ++i) {
      char** ppUrl = taosArrayGet(pBatch->pNotifyAddrUrls, i);
      if (ppUrl == NULL || *ppUrl == NULL || (*ppUrl)[0] == '\0') continue;
      int32_t code = streamEnqueueNotifyPayload(*ppUrl, pItem->pPayload, pItem->payloadLen);
      if (code != TSDB_CODE_SUCCESS) {
        stWarn("failed to enqueue nested window notice to %s since %s", *ppUrl, tstrerror(code));
      }
    }
    stTriggerNoticeItemDestroy(pItem);
    taosMemoryFree(pNode);
  }
  taosMemoryFree(pBatch);
  *ppBatch = NULL;
}

void stTriggerNoticeBatchAbort(SStreamTriggerNoticeBatch** ppBatch) {
  if (ppBatch == NULL || *ppBatch == NULL) {
    return;
  }

  SStreamTriggerNoticeBatch* pBatch = *ppBatch;
  stTriggerNoticeListClear(&pBatch->staged);
  taosMemoryFree(pBatch);
  *ppBatch = NULL;
}

int32_t streamSendNotifyPayloadOnce(const char* pUrl, const char* pPayload, int32_t payloadLen) {
  if (pUrl == NULL || pUrl[0] == '\0' || pPayload == NULL || payloadLen <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  SCURL   connection = {0};
  connection.url = taosStrdup(pUrl);
  if (connection.url == NULL) {
    return terrno;
  }

  code = tcurlConnect(&connection.pConn, pUrl);
  if (code != TSDB_CODE_SUCCESS || connection.pConn == NULL) {
    if (code == TSDB_CODE_SUCCESS) {
      code = TSDB_CODE_FAILED;
    }
    goto _exit;
  }

  size_t totalLen = (size_t)payloadLen;
  size_t sentLen = 0;
  while (sentLen < totalLen) {
    size_t   nbytes = 0;
    CURLcode result = curl_ws_send(connection.pConn, pPayload + sentLen, totalLen - sentLen, &nbytes,
                                   sentLen == 0 ? (curl_off_t)totalLen : 0, CURLWS_TEXT | CURLWS_OFFSET);
    if (result != CURLE_OK || nbytes == 0 || nbytes > totalLen - sentLen) {
      code = TSDB_CODE_RPC_NETWORK_UNAVAIL;
      goto _exit;
    }
    sentLen += nbytes;
  }

_exit:
  tcurlClose(&connection);
  return code;
}

#define STREAM_EVENT_NOTIFY_RETRY_MS 50  // 50 ms

int32_t streamSendNestedResultNotifyContent(SStreamTask* pTask, const char* pStreamName, const char* pTableName,
                                            int32_t triggerType, int64_t gid, const SArray* pNotifyAddrUrls,
                                            int32_t addOptions, const SSTriggerCalcParam* pParams,
                                            const char* const* pTriggerIds, int32_t nParam) {
  if (pTask == NULL || pStreamName == NULL || pNotifyAddrUrls == NULL ||
      (nParam > 0 && (pParams == NULL || pTriggerIds == NULL))) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  int32_t        nSentParams = 0;
  SStringBuilder sb = {0};
  const char*    pMsgTail = "]}]}";
  char*          pMsg = NULL;
  char*          pContent = NULL;
  SCURL          conn = {0};
  bool           shouldNotify = false;

  if (nParam <= 0 || taosArrayGetSize(pNotifyAddrUrls) <= 0) {
    goto _end;
  }

  for (int32_t i = 0; i < nParam; ++i) {
    if (pParams[i].notifyType == STRIGGER_EVENT_WINDOW_NONE) {
      continue;
    }
    if (streamNestedNotifyUsesLeafIdentity(pParams[i].notifyType) &&
        !streamIsCanonicalNestedTriggerId(pTriggerIds[i])) {
      code = TSDB_CODE_INVALID_PARA;
      goto _end;
    }
    if (!streamNestedNotifyUsesLeafIdentity(pParams[i].notifyType) && pParams[i].notifyType != STRIGGER_EVENT_IDLE &&
        pParams[i].notifyType != STRIGGER_EVENT_RESUME) {
      code = TSDB_CODE_INVALID_PARA;
      goto _end;
    }
    shouldNotify = true;
  }
  if (!shouldNotify) {
    goto _end;
  }
  if (taosStringBuilderSetJmp(&sb) != 0) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _end;
  }

  const char* pDelimiter = strstr(pStreamName, TS_PATH_DELIMITER);
  if (pDelimiter != NULL) {
    pStreamName = pDelimiter + 1;
  }

  taosStringBuilderEnsureCapacity(&sb, 1024);
  size_t msgTailLen = strlen(pMsgTail);
  code = streamAppendNotifyHeader(pStreamName, &sb);
  QUERY_CHECK_CODE(code, lino, _end);
  sb.pos -= msgTailLen;
  for (int32_t i = 0; i < nParam; ++i) {
    if (pParams[i].notifyType == STRIGGER_EVENT_WINDOW_NONE) {
      continue;
    }
    code = streamBuildNestedNotifyContent(pTableName, gid, triggerType, &pParams[i], pTriggerIds[i], &pContent);
    QUERY_CHECK_CODE(code, lino, _end);
    taosStringBuilderAppendString(&sb, pContent);
    cJSON_free(pContent);
    pContent = NULL;
    taosStringBuilderAppendChar(&sb, ',');
    ++nSentParams;
  }
  sb.pos -= 1;
  taosStringBuilderAppendStringLen(&sb, pMsgTail, msgTailLen);
  pMsg = taosStringBuilderGetResult(&sb, NULL);

  for (int32_t i = 0; i < TARRAY_SIZE(pNotifyAddrUrls); ++i) {
    char** pUrl = TARRAY_GET_ELEM(pNotifyAddrUrls, i);
    if (*pUrl == NULL) {
      continue;
    }

    conn.url = taosStrdup(*pUrl);
    QUERY_CHECK_NULL(conn.url, code, lino, _end, terrno);
    code = tcurlConnect(&conn.pConn, *pUrl);
    if (code != TSDB_CODE_SUCCESS) {
      ST_TASK_ELOG("failed to get stream notify handle of %s", *pUrl);
      tcurlClose(&conn);
      if (addOptions & NOTIFY_ON_FAILURE_PAUSE) {
        taosMsleep(STREAM_EVENT_NOTIFY_RETRY_MS);
        --i;
        continue;
      }
      code = TSDB_CODE_SUCCESS;
      continue;
    }

    size_t   totalLen = sb.pos;
    size_t   sentLen = 0;
    CURLcode res = CURLE_OK;
    while (sentLen < totalLen) {
      size_t nbytes = 0;
      if (sentLen == 0) {
        res = tcurlSend(&conn, pMsg, totalLen, &nbytes, totalLen, CURLWS_TEXT | CURLWS_OFFSET);
      } else {
        res = tcurlSend(&conn, pMsg + sentLen, totalLen - sentLen, &nbytes, 0, CURLWS_TEXT | CURLWS_OFFSET);
      }
      if (res != CURLE_OK || nbytes == 0 || nbytes > totalLen - sentLen) {
        if (res == CURLE_OK) {
          res = CURLE_SEND_ERROR;
        }
        break;
      }
      sentLen += nbytes;
    }
    tcurlClose(&conn);
    if (res != CURLE_OK) {
      ST_TASK_ELOG("failed to send stream notify msg to %s for %d", *pUrl, res);
      if (addOptions & NOTIFY_ON_FAILURE_PAUSE) {
        taosMsleep(STREAM_EVENT_NOTIFY_RETRY_MS);
        --i;
      } else {
        code = TSDB_CODE_SUCCESS;
      }
    } else {
      ST_TASK_DLOG("notify %d events to %s successfully", nSentParams, *pUrl);
    }
  }

_end:
  cJSON_free(pContent);
  tcurlClose(&conn);
  taosStringBuilderDestroy(&sb);
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
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

static void stReleaseCalcDataCacheIter(void* pValue) { releaseDataResultAndResetMgrStatus((void**)pValue); }

int32_t stCreateCalcDataCacheIterMap(SHashObj** ppMap) {
  if (ppMap == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SHashObj* pMap = taosHashInit(256, MurmurHash3_32, false, HASH_ENTRY_LOCK);
  if (pMap == NULL) {
    return terrno;
  }
  taosHashSetFreeFp(pMap, stReleaseCalcDataCacheIter);
  *ppMap = pMap;
  return TSDB_CODE_SUCCESS;
}

static int32_t stBuildCacheIteratorKey(const SStreamCacheScope* pScope, void** ppKey, int32_t* pKeyLen) {
  if (pScope == NULL || ppKey == NULL || pKeyLen == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t depth = taosArrayGetSize(pScope->lineage.pScopes);
  if (pScope->lineage.pScopes != NULL && pScope->lineage.pScopes->elemSize != sizeof(SScopeInstanceId)) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t keyLen = sizeof(pScope->gid) + sizeof(depth) +
                   depth * (sizeof(int32_t) + sizeof(int8_t) + sizeof(TSKEY) + sizeof(int64_t));
  uint8_t* pKey = taosMemoryMalloc(keyLen);
  if (pKey == NULL) {
    return terrno;
  }
  int32_t offset = 0;
  memcpy(pKey + offset, &pScope->gid, sizeof(pScope->gid));
  offset += sizeof(pScope->gid);
  memcpy(pKey + offset, &depth, sizeof(depth));
  offset += sizeof(depth);
  for (int32_t i = 0; i < depth; ++i) {
    const SScopeInstanceId* pId = taosArrayGet(pScope->lineage.pScopes, i);
    if (pId == NULL) {
      taosMemoryFree(pKey);
      return TSDB_CODE_INVALID_PARA;
    }
    memcpy(pKey + offset, &pId->layerIndex, sizeof(pId->layerIndex));
    offset += sizeof(pId->layerIndex);
    memcpy(pKey + offset, &pId->triggerType, sizeof(pId->triggerType));
    offset += sizeof(pId->triggerType);
    memcpy(pKey + offset, &pId->openingTs, sizeof(pId->openingTs));
    offset += sizeof(pId->openingTs);
    memcpy(pKey + offset, &pId->nativeDiscriminator, sizeof(pId->nativeDiscriminator));
    offset += sizeof(pId->nativeDiscriminator);
  }
  *ppKey = pKey;
  *pKeyLen = keyLen;
  return TSDB_CODE_SUCCESS;
}

static SHashObj* stGetCalcDataCacheIterMap(SStreamTriggerTask* pTask, int64_t sessionId) {
  if (pTask->pRealtimeContext != NULL && pTask->pRealtimeContext->sessionId == sessionId) {
    return pTask->pRealtimeContext->pCalcDataCacheIters;
  }
  if (pTask->pHistoryContext != NULL && pTask->pHistoryContext->sessionId == sessionId) {
    return pTask->pHistoryContext->pCalcDataCacheIters;
  }
  return NULL;
}

static int32_t stRemoveCalcDataCacheIterForScopeLocked(SStreamTriggerTask* pTask, int64_t sessionId,
                                                       const SStreamCacheScope* pScope) {
  SHashObj* pCalcDataCacheIters = stGetCalcDataCacheIterMap(pTask, sessionId);
  if (pCalcDataCacheIters == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  void*   pKey = NULL;
  int32_t keyLen = 0;
  int32_t code = stBuildCacheIteratorKey(pScope, &pKey, &keyLen);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  code = taosHashRemove(pCalcDataCacheIters, pKey, keyLen);
  taosMemoryFree(pKey);
  if (code == TSDB_CODE_INVALID_PARA || code == TSDB_CODE_NOT_FOUND) {
    return TSDB_CODE_SUCCESS;
  }
  return code;
}

static int32_t stRemoveCalcDataCacheIterForScope(SStreamTriggerTask* pTask, int64_t sessionId,
                                                 const SStreamCacheScope* pScope) {
  taosWLockLatch(&pTask->calcDataCacheIterLock);
  int32_t code = stRemoveCalcDataCacheIterForScopeLocked(pTask, sessionId, pScope);
  taosWUnLockLatch(&pTask->calcDataCacheIterLock);
  return code;
}

int32_t stCleanupCalcDataCacheItersForRequest(SStreamTriggerTask* pTask, const SSTriggerCalcRequest* pReq) {
  if (pTask == NULL || pReq == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (pReq->pAncestorContext == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  const SArray* pIdentities =
      pReq->isMultiGroupCalc ? pReq->pAncestorContext->pReadScopeBindings : pReq->pAncestorContext->pParamContexts;
  if (pIdentities == NULL) {
    return TSDB_CODE_SUCCESS;
  }
  size_t expectedElemSize =
      pReq->isMultiGroupCalc ? sizeof(SStreamReadScopeBinding) : sizeof(SStreamAncestorParamContext);
  if (pIdentities->elemSize != expectedElemSize) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t firstCode = TSDB_CODE_SUCCESS;
  taosWLockLatch(&pTask->calcDataCacheIterLock);
  for (int32_t i = 0; i < taosArrayGetSize(pIdentities); ++i) {
    const SStreamCacheScope* pScope = NULL;
    SStreamCacheScope        scope = {0};
    if (pReq->isMultiGroupCalc) {
      const SStreamReadScopeBinding* pBinding = taosArrayGet(pIdentities, i);
      if (pBinding != NULL) {
        pScope = &pBinding->scope;
      }
    } else {
      const SStreamAncestorParamContext* pParam = taosArrayGet(pIdentities, i);
      if (pParam != NULL) {
        scope.gid = pParam->leafIdentity.gid;
        scope.lineage = pParam->leafIdentity.lineage;
        pScope = &scope;
      }
    }

    int32_t code = pScope == NULL ? TSDB_CODE_INVALID_PARA
                                  : stRemoveCalcDataCacheIterForScopeLocked(pTask, pReq->sessionId, pScope);
    if (firstCode == TSDB_CODE_SUCCESS && code != TSDB_CODE_SUCCESS) {
      firstCode = code;
    }
  }
  taosWUnLockLatch(&pTask->calcDataCacheIterLock);
  return firstCode;
}

int32_t stRemoveStreamCacheReadScope(const SStreamCacheReadInfo* pInfo) {
  if (pInfo == NULL || !pInfo->hasCacheScope) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t             code = TSDB_CODE_SUCCESS;
  SStreamTriggerTask* pTask = NULL;
  void*               taskAddr = NULL;
  code = streamAcquireTask(pInfo->taskInfo.streamId, pInfo->taskInfo.taskId, (SStreamTask**)&pTask, &taskAddr);
  if (code == TSDB_CODE_SUCCESS) {
    if (pTask->task.type != STREAM_TRIGGER_TASK) {
      code = TSDB_CODE_STREAM_TASK_NOT_EXIST;
    } else {
      code = stRemoveCalcDataCacheIterForScope(pTask, pInfo->taskInfo.sessionId, &pInfo->cacheScope);
    }
  }
  streamReleaseTask(taskAddr);
  return code;
}

static int32_t readStreamDataCacheScoped(SStreamTriggerTask* pAcquiredTask, int64_t sessionId,
                                         const SStreamCacheScope* pScope, TSKEY start, TSKEY end, SSDataBlock** ppBlock,
                                         bool reset, bool* finished) {
  int32_t   code = TSDB_CODE_SUCCESS;
  int32_t   lino = 0;
  SHashObj* pCalcDataCacheIters = NULL;
  int64_t   streamId = pAcquiredTask->task.streamId;
  void*     pKey = NULL;
  int32_t   keyLen = 0;
  bool      removeEntry = false;
  bool      iterLockHeld = false;

  taosWLockLatch(&pAcquiredTask->calcDataCacheIterLock);
  iterLockHeld = true;
  pCalcDataCacheIters = stGetCalcDataCacheIterMap(pAcquiredTask, sessionId);
  if (pCalcDataCacheIters == NULL) {
    stsError("sessionId %" PRId64 " not found in task %" PRId64, sessionId, pAcquiredTask->task.taskId);
    code = TSDB_CODE_INTERNAL_ERROR;
    lino = __LINE__;
    goto _end;
  }

  if (reset) {
    code = stRemoveCalcDataCacheIterForScopeLocked(pAcquiredTask, sessionId, pScope);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  code = stBuildCacheIteratorKey(pScope, &pKey, &keyLen);
  QUERY_CHECK_CODE(code, lino, _end);
  void** px = taosHashGet(pCalcDataCacheIters, pKey, keyLen);
  if (px == NULL) {
    void* pIter = NULL;
    code = taosHashPut(pCalcDataCacheIters, pKey, keyLen, &pIter, POINTER_BYTES);
    QUERY_CHECK_CODE(code, lino, _end);
    px = taosHashGet(pCalcDataCacheIters, pKey, keyLen);
    QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INVALID_PARA);
  }
  if (*px == NULL) {
    SStreamDataCacheLease* pLease = NULL;
    void*                  pCache = NULL;
    code = acquireStreamDataCacheLease(pAcquiredTask->task.streamId, pAcquiredTask->task.taskId, sessionId, &pLease,
                                       &pCache);
    QUERY_CHECK_CODE(code, lino, _end);
    code = getStreamDataCacheScoped(pCache, pScope, start, end, px);
    if (code != TSDB_CODE_SUCCESS || *px == NULL) {
      releaseStreamDataCacheLease(&pLease);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      ((SResultIter*)*px)->pLease = pLease;
    }
  }
  if (*px != NULL) {
    code = getNextStreamDataCache(px, ppBlock);
    QUERY_CHECK_CODE(code, lino, _end);
  }
  *finished = *px == NULL;
  removeEntry = *finished;

_end:
  if (removeEntry || code != TSDB_CODE_SUCCESS) {
    int32_t removeCode = stRemoveCalcDataCacheIterForScopeLocked(pAcquiredTask, sessionId, pScope);
    if (code == TSDB_CODE_SUCCESS && removeCode != TSDB_CODE_SUCCESS) {
      code = removeCode;
    }
  }
  taosMemoryFree(pKey);
  if (iterLockHeld) {
    taosWUnLockLatch(&pAcquiredTask->calcDataCacheIterLock);
  }
  return code;
}

static int32_t readStreamDataCacheLegacy(SStreamTriggerTask* pTask, int64_t sessionId, int64_t gid, TSKEY start,
                                         TSKEY end, SSDataBlock** ppBlock, bool reset, bool* finished) {
  int32_t   code = TSDB_CODE_SUCCESS;
  SHashObj* pCalcDataCacheIters = NULL;
  void*     pCalcDataCache = NULL;
  bool      removeEntry = false;
  int64_t   streamId = pTask->task.streamId;

  if (pTask->pRealtimeContext != NULL && pTask->pRealtimeContext->sessionId == sessionId) {
    pCalcDataCacheIters = pTask->pRealtimeContext->pCalcDataCacheIters;
    pCalcDataCache = pTask->pRealtimeContext->pCalcDataCache;
  } else if (pTask->pHistoryContext != NULL && pTask->pHistoryContext->sessionId == sessionId) {
    pCalcDataCacheIters = pTask->pHistoryContext->pCalcDataCacheIters;
    pCalcDataCache = pTask->pHistoryContext->pCalcDataCache;
  } else {
    stsError("sessionId %" PRId64 " not found in task %" PRId64, sessionId, pTask->task.taskId);
    return TSDB_CODE_INTERNAL_ERROR;
  }
  if (pCalcDataCacheIters == NULL || pCalcDataCache == NULL) {
    return TSDB_CODE_INTERNAL_ERROR;
  }

  if (reset) {
    code = taosHashRemove(pCalcDataCacheIters, &gid, sizeof(gid));
    if (code == TSDB_CODE_INVALID_PARA || code == TSDB_CODE_NOT_FOUND) {
      code = TSDB_CODE_SUCCESS;
    }
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  void** px = taosHashGet(pCalcDataCacheIters, &gid, sizeof(gid));
  if (px == NULL) {
    void* pIter = NULL;
    code = taosHashPut(pCalcDataCacheIters, &gid, sizeof(gid), &pIter, POINTER_BYTES);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    px = taosHashGet(pCalcDataCacheIters, &gid, sizeof(gid));
    if (px == NULL) {
      code = TSDB_CODE_INVALID_PARA;
      removeEntry = true;
      goto _end;
    }
  }

  if (*px == NULL) {
    code = getStreamDataCache(pCalcDataCache, gid, start, end, px);
    if (code != TSDB_CODE_SUCCESS) {
      removeEntry = true;
      goto _end;
    }
  }
  if (*px != NULL) {
    code = getNextStreamDataCache(px, ppBlock);
    if (code != TSDB_CODE_SUCCESS) {
      removeEntry = true;
      goto _end;
    }
  }
  *finished = *px == NULL;
  removeEntry = *finished;

_end:
  if (removeEntry) {
    int32_t removeCode = taosHashRemove(pCalcDataCacheIters, &gid, sizeof(gid));
    if (removeCode == TSDB_CODE_INVALID_PARA || removeCode == TSDB_CODE_NOT_FOUND) {
      removeCode = TSDB_CODE_SUCCESS;
    }
    if (code == TSDB_CODE_SUCCESS && removeCode != TSDB_CODE_SUCCESS) {
      code = removeCode;
    }
  }
  return code;
}

int32_t readStreamDataCache(SStreamCacheReadInfo* pInfo, bool* finished) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask* pTask = NULL;
  void*               taskAddr = NULL;
  int64_t             streamId = pInfo == NULL ? 0 : pInfo->taskInfo.streamId;

  if (pInfo == NULL || finished == NULL || pInfo->pBlock != NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  *finished = false;

  code = streamAcquireTask(pInfo->taskInfo.streamId, pInfo->taskInfo.taskId, (SStreamTask**)&pTask, &taskAddr);
  QUERY_CHECK_CODE(code, lino, _end);

  QUERY_CHECK_CONDITION(pTask->task.type == STREAM_TRIGGER_TASK, code, lino, _end, TSDB_CODE_STREAM_TASK_NOT_EXIST);
  const bool requiresContextPolicy = BIT_FLAG_TEST_MASK(pTask->addOptions, STREAM_OPTION_NESTED_WINDOW_PLAN);
  QUERY_CHECK_CONDITION(!requiresContextPolicy || pInfo->pRuntime != NULL, code, lino, _end, TSDB_CODE_INVALID_PARA);
  const SStreamContextPolicy* pContextPolicy =
      pInfo->pRuntime == NULL ? pInfo->pContextPolicy : pInfo->pRuntime->pContextPolicy;
  const SStreamAncestorContext* pAncestorContext =
      pInfo->pRuntime == NULL ? pInfo->pAncestorContext : pInfo->pRuntime->pAncestorContext;
  code = tAdmitStreamContext(pContextPolicy, pAncestorContext, requiresContextPolicy);
  QUERY_CHECK_CODE(code, lino, _end);
  if (pInfo->pRuntime != NULL) {
    QUERY_CHECK_CONDITION(pInfo->gid == pInfo->pRuntime->groupId, code, lino, _end, TSDB_CODE_INVALID_PARA);
  }

  TSKEY start = pInfo->start;
  TSKEY end = pInfo->end;
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

  if (!requiresContextPolicy) {
    code = readStreamDataCacheLegacy(pTask, pInfo->taskInfo.sessionId, pInfo->gid, start, end, &pInfo->pBlock,
                                     pInfo->reset, finished);
    QUERY_CHECK_CODE(code, lino, _end);
    goto _end;
  }

  code = stBindStreamCacheReadScopeForTask(pInfo->pRuntime, true, pTask->task.nodeId, pInfo);
  QUERY_CHECK_CODE(code, lino, _end);
  QUERY_CHECK_CONDITION(pInfo->cacheScope.gid == pInfo->gid, code, lino, _end, TSDB_CODE_INVALID_PARA);
  code = readStreamDataCacheScoped(pTask, pInfo->taskInfo.sessionId, &pInfo->cacheScope, start, end, &pInfo->pBlock,
                                   pInfo->reset, finished);
  QUERY_CHECK_CODE(code, lino, _end);

_end:

  streamReleaseTask(taskAddr);

  if (code != TSDB_CODE_SUCCESS) {
    stsError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}
