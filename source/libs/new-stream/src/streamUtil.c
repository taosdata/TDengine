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
#include "dataSink.h"
#include "streamInt.h"
#include "tdatablock.h"
#include "tstrbuild.h"

#ifndef WINDOWS
#include "curl/curl.h"
#endif

int32_t streamGetThreadIdx(int32_t threadNum, int64_t streamGId) { return streamGId % threadNum; }

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

int32_t stmAddPeriodReport(int64_t streamId, SArray** ppReport, SStreamTriggerTask* triggerTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (NULL == *ppReport) {
    *ppReport = taosArrayInit(5, sizeof(SSTriggerRuntimeStatus));
    TSDB_CHECK_NULL(*ppReport, code, lino, _exit, terrno);
  }

  SSTriggerRuntimeStatus status = {0};
  stTriggerTaskGetStatus((SStreamTask*)triggerTask, &status);

  TSDB_CHECK_NULL(taosArrayPush(*ppReport, &status), code, lino, _exit, terrno);

  stsDebug("trigger task period report added, recalcNum:%d", (int32_t)taosArrayGetSize(status.userRecalcs));

_exit:

  return code;
}

void stmHandleStreamRemovedTasks(SStreamInfo* pStream, int64_t streamId, int32_t gid) {
  bool isLastTask = false;

  if (taosArrayGetSize(pStream->undeployReaders) > 0) {
    smHandleRemovedTask(pStream, streamId, gid, true);
  }

  if (taosArrayGetSize(pStream->undeployRunners) > 0) {
    smHandleRemovedTask(pStream, streamId, gid, false);
  }

  taosWLockLatch(&pStream->undeployLock);
  if (0 == pStream->undeployTriggerId) {
    taosWUnLockLatch(&pStream->undeployLock);
    return;
  }

  if (pStream->triggerTask->task.taskId != pStream->undeployTriggerId) {
    stsWarn("undeploy trigger task %" PRIx64 " mismatch with current trigger taskId:%" PRIx64,
            pStream->undeployTriggerId, pStream->triggerTask->task.taskId);
    pStream->undeployTriggerId = 0;
    taosWUnLockLatch(&pStream->undeployLock);
    return;
  }

  pStream->undeployTriggerId = 0;
  taosMemoryFreeClear(pStream->triggerTask);
  smRemoveTaskPostCheck(streamId, pStream, &isLastTask);
  taosWUnLockLatch(&pStream->undeployLock);

  if (!isLastTask) {
    return;
  }

  int32_t code = taosHashRemove(gStreamMgmt.stmGrp[gid], &streamId, sizeof(streamId));
  if (TSDB_CODE_SUCCESS == code) {
    stsInfo("stream removed from streamGrpHash %d, remainStream:%d", gid, taosHashGetSize(gStreamMgmt.stmGrp[gid]));
  } else {
    stsWarn("stream remove from streamGrpHash %d failed, remainStream:%d, error:%s", gid,
            taosHashGetSize(gStreamMgmt.stmGrp[gid]), tstrerror(code));
  }
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
      TSDB_CHECK_NULL(taosArrayPush(pMsg->pStreamStatus, &pReader->task), code, lino, _exit, terrno);
      if (pReader->task.pMgmtReq) {
        TAOS_CHECK_EXIT(stmAddMgmtReq(streamId, &pMsg->pStreamReq, taosArrayGetSize(pMsg->pStreamStatus) - 1));
      }
      ST_TASK_DLOG("task status added to hb %s mgmtReq", pReader->task.pMgmtReq ? "with" : "without");
    }

    stsDebug("%d reader tasks status added to hb", TD_DLIST_NELES(pStream->readerList));
  }

  if (pStream->triggerTask) {
    pTask = (SStreamTask*)pStream->triggerTask;
    if (reportPeriod) {
      TAOS_CHECK_EXIT(stmAddPeriodReport(streamId, &pMsg->pTriggerStatus, pStream->triggerTask));
      pStream->triggerTask->task.detailStatus = taosArrayGetSize(pMsg->pTriggerStatus) - 1;
    } else {
      pStream->triggerTask->task.detailStatus = -1;
    }
    TSDB_CHECK_NULL(taosArrayPush(pMsg->pStreamStatus, &pStream->triggerTask->task), code, lino, _exit, terrno);
    if (pStream->triggerTask->task.pMgmtReq) {
      TAOS_CHECK_EXIT(stmAddMgmtReq(streamId, &pMsg->pStreamReq, taosArrayGetSize(pMsg->pStreamStatus) - 1));
    }
    
    ST_TASK_DLOG("task status added to hb %s mgmtReq", pStream->triggerTask->task.pMgmtReq ? "with" : "without");
    stsDebug("%d trigger tasks status added to hb", 1);
  }

  if (pStream->runnerList) {
    memset(&iter, 0, sizeof(iter));

    tdListInitIter(pStream->runnerList, &iter, TD_LIST_FORWARD);
    while ((listNode = tdListNext(&iter)) != NULL) {
      SStreamRunnerTask* pRunner = (SStreamRunnerTask*)listNode->data;
      pTask = (SStreamTask*)pRunner;
      TSDB_CHECK_NULL(taosArrayPush(pMsg->pStreamStatus, &pRunner->task), code, lino, _exit, terrno);
      if (pRunner->task.pMgmtReq) {
        TAOS_CHECK_EXIT(stmAddMgmtReq(streamId, &pMsg->pStreamReq, taosArrayGetSize(pMsg->pStreamStatus) - 1));
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

    stmHbAddStreamStatus(pMsg, pStream, *streamId, reportPeriod);
  }

  return code;
}

void stmDestroySStreamInfo(void* param) {
  if (NULL == param) {
    return;
  }
  
  SStreamInfo* p = (SStreamInfo*)param;
  tdListFree(p->readerList);
  p->readerList = NULL;
  taosMemoryFreeClear(p->triggerTask);
  tdListFree(p->runnerList);
  p->runnerList = NULL;
  taosArrayDestroy(p->undeployReaders);
  p->undeployReaders = NULL;
  taosArrayDestroy(p->undeployRunners);
  p->undeployRunners = NULL;
}

void stmDestroySStreamMgmtReq(SStreamMgmtReq* pReq) {
  if (NULL == pReq) {
    return;
  }

  taosArrayDestroy(pReq->cont.fullTableNames);
}

#define JSON_CHECK_ADD_ITEM(obj, str, item) \
  QUERY_CHECK_CONDITION(cJSON_AddItemToObjectCS(obj, str, item), code, lino, _end, TSDB_CODE_OUT_OF_MEMORY)

static int32_t jsonAddColumnField(const char* colName, int16_t type, bool isNull, const char* pData, cJSON* obj) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  char*   temp = NULL;

  QUERY_CHECK_NULL(colName, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(isNull || (pData != NULL), code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(obj, code, lino, _end, TSDB_CODE_INVALID_PARA);

  if (isNull) {
    JSON_CHECK_ADD_ITEM(obj, colName, cJSON_CreateNull());
    goto _end;
  }

  switch (type) {
    case TSDB_DATA_TYPE_BOOL: {
      bool val = *(const bool*)pData;
      JSON_CHECK_ADD_ITEM(obj, colName, cJSON_CreateBool(val));
      break;
    }

    case TSDB_DATA_TYPE_TINYINT: {
      int8_t val = *(const int8_t*)pData;
      JSON_CHECK_ADD_ITEM(obj, colName, cJSON_CreateNumber(val));
      break;
    }

    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t val = *(const int16_t*)pData;
      JSON_CHECK_ADD_ITEM(obj, colName, cJSON_CreateNumber(val));
      break;
    }

    case TSDB_DATA_TYPE_INT: {
      int32_t val = *(const int32_t*)pData;
      JSON_CHECK_ADD_ITEM(obj, colName, cJSON_CreateNumber(val));
      break;
    }

    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP: {
      int64_t val = *(const int64_t*)pData;
      JSON_CHECK_ADD_ITEM(obj, colName, cJSON_CreateNumber(val));
      break;
    }

    case TSDB_DATA_TYPE_FLOAT: {
      float val = *(const float*)pData;
      JSON_CHECK_ADD_ITEM(obj, colName, cJSON_CreateNumber(val));
      break;
    }

    case TSDB_DATA_TYPE_DOUBLE: {
      double val = *(const double*)pData;
      JSON_CHECK_ADD_ITEM(obj, colName, cJSON_CreateNumber(val));
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

      cJSON* item = cJSON_CreateStringReference(temp);
      JSON_CHECK_ADD_ITEM(obj, colName, item);

      // let the cjson object to free memory later
      item->type &= ~cJSON_IsReference;
      temp = NULL;
      break;
    }

    case TSDB_DATA_TYPE_UTINYINT: {
      uint8_t val = *(const uint8_t*)pData;
      JSON_CHECK_ADD_ITEM(obj, colName, cJSON_CreateNumber(val));
      break;
    }

    case TSDB_DATA_TYPE_USMALLINT: {
      uint16_t val = *(const uint16_t*)pData;
      JSON_CHECK_ADD_ITEM(obj, colName, cJSON_CreateNumber(val));
      break;
    }

    case TSDB_DATA_TYPE_UINT: {
      uint32_t val = *(const uint32_t*)pData;
      JSON_CHECK_ADD_ITEM(obj, colName, cJSON_CreateNumber(val));
      break;
    }

    case TSDB_DATA_TYPE_UBIGINT: {
      uint64_t val = *(const uint64_t*)pData;
      JSON_CHECK_ADD_ITEM(obj, colName, cJSON_CreateNumber(val));
      break;
    }

    default: {
      JSON_CHECK_ADD_ITEM(obj, colName, cJSON_CreateStringReference("<Unable to display this data type>"));
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

int32_t streamBuildStateNotifyContent(ESTriggerEventType eventType, int16_t dataType, const char* pFromState,
                                      const char* pToState, char** ppContent) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  cJSON*  obj = NULL;

  *ppContent = NULL;

  obj = cJSON_CreateObject();
  QUERY_CHECK_NULL(obj, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  if (eventType == STRIGGER_EVENT_WINDOW_OPEN) {
    code = jsonAddColumnField("prevState", dataType, pFromState == NULL, pFromState, obj);
    QUERY_CHECK_CODE(code, lino, _end);
    code = jsonAddColumnField("curState", dataType, false, pToState, obj);
    QUERY_CHECK_CODE(code, lino, _end);
  } else if (eventType == STRIGGER_EVENT_WINDOW_CLOSE) {
    code = jsonAddColumnField("curState", dataType, false, pFromState, obj);
    QUERY_CHECK_CODE(code, lino, _end);
    code = jsonAddColumnField("nextState", dataType, false, pToState, obj);
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

int32_t streamBuildEventNotifyContent(const SSDataBlock* pInputBlock, const SNodeList* pCondCols, int32_t rowIdx,
                                      char** ppContent) {
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
    code = jsonAddColumnField(pColDef->colName, pColData->info.type, colDataIsNull_s(pColData, rowIdx),
                              colDataGetData(pColData, rowIdx), fields);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  cond = cJSON_CreateObject();
  QUERY_CHECK_NULL(cond, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  // todo(kjq): support condition index
  JSON_CHECK_ADD_ITEM(cond, "conditionIndex", cJSON_CreateNumber(0));
  JSON_CHECK_ADD_ITEM(cond, "fieldValues", fields);
  fields = NULL;

  obj = cJSON_CreateObject();
  QUERY_CHECK_NULL(obj, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  JSON_CHECK_ADD_ITEM(obj, "triggerCondition", cond);
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

int32_t streamBuildBlockResultNotifyContent(const SSDataBlock* pBlock, char** ppContent, const SArray* pFields) {
  int32_t code = 0, lino = 0;
  cJSON*  pContent = NULL;
  cJSON*  pResult = NULL;
  cJSON*  pRow = NULL;
  pResult = cJSON_CreateObject();
  QUERY_CHECK_NULL(pResult, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);

  cJSON* pArr = cJSON_AddArrayToObject(pResult, "data");
  QUERY_CHECK_NULL(pArr, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);

  cJSON* size = cJSON_CreateNumber(pBlock->info.rows);
  QUERY_CHECK_NULL(size, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  if (!cJSON_AddItemToObjectCS(pResult, "curSize", size)) {
    cJSON_Delete(size);
    goto _end;
  }
  cJSON* offset = cJSON_CreateNumber(0);
  QUERY_CHECK_NULL(offset, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  if (!cJSON_AddItemToObjectCS(pResult, "curOffset", offset)) {
    cJSON_Delete(offset);
    goto _end;
  }
  cJSON* finish = cJSON_CreateTrue();
  QUERY_CHECK_NULL(finish, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  if (!cJSON_AddItemToObjectCS(pResult, "finish", finish)) {
    cJSON_Delete(finish);
    goto _end;
  }

  for (int32_t rowIdx = 0; rowIdx < pBlock->info.rows; ++rowIdx) {
    pRow = cJSON_CreateObject();
    QUERY_CHECK_NULL(pRow, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);

    for (int32_t colIdx = 0; colIdx < taosArrayGetSize(pBlock->pDataBlock); ++colIdx) {
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
      code = jsonAddColumnField(colName, pCol->info.type, isNull, isNull ? NULL : colDataGetData(pCol, rowIdx),
                                pRow);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    TSDB_CHECK_CONDITION(cJSON_AddItemToArray(pArr, pRow), code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
    pRow = NULL;
  }
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

static int32_t streamAppendNotifyContent(int32_t triggerType, int64_t groupId, const SSTriggerCalcParam* pParam,
                                         SStringBuilder* pBuilder) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  cJSON*  obj = NULL;
  char*   temp = NULL;

  const char* eventType = NULL;
  if (pParam->notifyType == STRIGGER_EVENT_WINDOW_OPEN) {
    eventType = "WINDOW_OPEN";
  } else if (pParam->notifyType == STRIGGER_EVENT_WINDOW_CLOSE) {
    eventType = "WINDOW_CLOSE";
  } else if (pParam->notifyType == STRIGGER_EVENT_ON_TIME) {
    eventType = "ON_TIME";
  }

  uint64_t ar[] = {groupId, pParam->wstart};
  uint64_t hash = MurmurHash3_64((const char*)ar, sizeof(ar));
  char     windowId[32];
  u64toaFastLut(hash, windowId);

  const char* windowType = NULL;
  switch (triggerType) {
    case STREAM_TRIGGER_PERIOD:
      windowType = "Period";
      break;
    case STREAM_TRIGGER_SLIDING:
      windowType = (pParam->notifyType == STRIGGER_EVENT_ON_TIME) ? "Sliding" : "Time";
      break;
    case STREAM_TRIGGER_SESSION:
      windowType = "Session";
      break;
    case STREAM_TRIGGER_COUNT:
      windowType = "Count";
      break;
    case STREAM_TRIGGER_STATE:
      windowType = "State";
      break;
    case STREAM_TRIGGER_EVENT:
      windowType = "Event";
      break;
    default:
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
  }

  obj = cJSON_CreateObject();
  QUERY_CHECK_NULL(obj, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  JSON_CHECK_ADD_ITEM(obj, "eventType", cJSON_CreateStringReference(eventType));
  JSON_CHECK_ADD_ITEM(obj, "eventTime", cJSON_CreateNumber(taosGetTimestampMs()));
  JSON_CHECK_ADD_ITEM(obj, "windowId", cJSON_CreateString(windowId));

  if (STREAM_TRIGGER_PERIOD != triggerType && STREAM_TRIGGER_SLIDING != triggerType) {
    JSON_CHECK_ADD_ITEM(obj, "windowType", cJSON_CreateStringReference(windowType));
    JSON_CHECK_ADD_ITEM(obj, "windowStart", cJSON_CreateNumber(pParam->wstart));
    if (pParam->notifyType == STRIGGER_EVENT_WINDOW_CLOSE) {
      int64_t wend = pParam->wend;
      JSON_CHECK_ADD_ITEM(obj, "windowEnd", cJSON_CreateNumber(wend));
    }
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

#ifndef WINDOWS
static int32_t streamNotifyConnect(const char* url, CURL** pConn) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  CURL*    conn = NULL;
  CURLcode res = CURLE_OK;

  conn = curl_easy_init();
  TSDB_CHECK_NULL(conn, code, lino, _end, TSDB_CODE_FAILED);
  res = curl_easy_setopt(conn, CURLOPT_URL, url);
  TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);
  res = curl_easy_setopt(conn, CURLOPT_SSL_VERIFYPEER, 0L);
  TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);
  res = curl_easy_setopt(conn, CURLOPT_SSL_VERIFYHOST, 0L);
  TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);
  res = curl_easy_setopt(conn, CURLOPT_TIMEOUT, 3L);
  TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);
  res = curl_easy_setopt(conn, CURLOPT_CONNECT_ONLY, 2L);
  TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);
  res = curl_easy_perform(conn);
  TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);

  *pConn = conn;
  conn = NULL;

_end:
  if (conn != NULL) {
    curl_easy_cleanup(conn);
  }
  if (code != TSDB_CODE_SUCCESS) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void streamNotifyClose(CURL** pConn, const char* url) {
  if (pConn == NULL || *pConn == NULL) {
    return;
  }

  // status code 1000 means normal closure
  size_t   len = 0;
  uint16_t status = htons(1000);
  CURLcode res = curl_ws_send(*pConn, &status, sizeof(status), &len, 0, CURLWS_CLOSE);
  if (res != CURLE_OK) {
    stWarn("failed to send ws-close msg to %s for %d", url ? url : "", res);
  }
  curl_easy_cleanup(*pConn);
  *pConn = NULL;
}

#define STREAM_EVENT_NOTIFY_RETRY_MS 50  // 50 ms

int32_t streamSendNotifyContent(SStreamTask* pTask, const char* streamName, int32_t triggerType, int64_t groupId,
                                const SArray* pNotifyAddrUrls, int32_t errorHandle, const SSTriggerCalcParam* pParams,
                                int32_t nParam) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SStringBuilder sb = {0};
  const char*    msgTail = "]}]}";
  char*          msg = NULL;
  CURL*          conn = NULL;
  bool           shouldNotify = false;

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

  taosStringBuilderEnsureCapacity(&sb, 1024);
  size_t msgTailLen = strlen(msgTail);

  code = streamAppendNotifyHeader(streamName, &sb);
  QUERY_CHECK_CODE(code, lino, _end);
  sb.pos -= msgTailLen;
  for (int32_t i = 0; i < nParam; ++i) {
    if (pParams[i].notifyType == STRIGGER_EVENT_WINDOW_NONE) {
      continue;
    }
    code = streamAppendNotifyContent(triggerType, groupId, &pParams[i], &sb);
    QUERY_CHECK_CODE(code, lino, _end);
    taosStringBuilderAppendChar(&sb, ',');
  }
  sb.pos -= 1;
  taosStringBuilderAppendStringLen(&sb, msgTail, msgTailLen);
  msg = taosStringBuilderGetResult(&sb, NULL);

  for (int32_t i = 0; i < TARRAY_SIZE(pNotifyAddrUrls); ++i) {
    const char** pUrl = TARRAY_GET_ELEM(pNotifyAddrUrls, i);
    if (*pUrl == NULL) {
      continue;
    }

    // todo(kjq): check if task should stop

    code = streamNotifyConnect(*pUrl, &conn);
    if (code != TSDB_CODE_SUCCESS) {
      ST_TASK_ELOG("failed to get stream notify handle of %s", *pUrl);
      if (errorHandle > 0) {
        // retry for event message sending in PAUSE error handling mode
        taosMsleep(STREAM_EVENT_NOTIFY_RETRY_MS);
        --i;
        continue;
      } else {
        // simply ignore the failure in DROP error handling mode
        code = TSDB_CODE_SUCCESS;
        continue;
      }
    }

    size_t   totalLen = sb.pos;
    size_t   sentLen = 0;
    CURLcode res = CURLE_OK;
    while (sentLen < totalLen) {
      size_t nbytes = 0;
      if (sentLen == 0) {
        res = curl_ws_send(conn, msg, totalLen, &nbytes, totalLen, CURLWS_TEXT | CURLWS_OFFSET);
      } else {
        res = curl_ws_send(conn, msg + sentLen, totalLen - sentLen, &nbytes, 0, CURLWS_TEXT | CURLWS_OFFSET);
      }
      if (res != CURLE_OK) {
        break;
      }
      sentLen += nbytes;
    }
    if (res != CURLE_OK) {
      ST_TASK_ELOG("failed to send stream notify msg to %s for %d", *pUrl, res);
      if (errorHandle > 0) {
        // retry for event message sending in PAUSE error handling mode
        taosMsleep(STREAM_EVENT_NOTIFY_RETRY_MS);
        --i;
      } else {
        // simply ignore the failure in DROP error handling mode
        code = TSDB_CODE_SUCCESS;
      }
    }
    streamNotifyClose(&conn, *pUrl);
  }

_end:
  if (conn != NULL) {
    streamNotifyClose(&conn, NULL);
  }
  taosStringBuilderDestroy(&sb);
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}
#else
int32_t streamSendNotifyContent(SStreamTask* pTask, int32_t triggerType, int64_t groupId, const SArray* pNotifyAddrUrls,
                                int32_t notifyErrorHandle, const SSTriggerCalcParam* pParams, int32_t nParam) {
  ST_TASK_ELOG("stream notify events is not supported on windows");
  return TSDB_CODE_NOT_SUPPORTTED_IN_WINDOWS;
}
#endif

int32_t readStreamDataCache(int64_t streamId, int64_t taskId, int64_t sessionId, int64_t groupId, TSKEY start,
                            TSKEY end, void*** pppIter) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask* pTask = NULL;
  void* taskAddr = NULL;

  *pppIter = NULL;

  code = streamAcquireTask(streamId, taskId, (SStreamTask**)&pTask, &taskAddr);
  QUERY_CHECK_CODE(code, lino, _end);

  QUERY_CHECK_CONDITION(pTask->task.type == STREAM_TRIGGER_TASK, code, lino, _end, TSDB_CODE_STREAM_TASK_NOT_EXIST);

  if (((SStreamTriggerTask*)pTask)->triggerType == STREAM_TRIGGER_SLIDING) {
    end = end - 1;
  }
  if (pTask->pRealtimeContext->sessionId == sessionId) {
    void** px = taosHashGet(pTask->pRealtimeContext->pCalcDataCacheIters, &groupId, sizeof(int64_t));
    if (px == NULL) {
      void* pIter = NULL;
      code =
          taosHashPut(pTask->pRealtimeContext->pCalcDataCacheIters, &groupId, sizeof(int64_t), &pIter, POINTER_BYTES);
      QUERY_CHECK_CODE(code, lino, _end);
      px = taosHashGet(pTask->pRealtimeContext->pCalcDataCacheIters, &groupId, sizeof(int64_t));
      QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INVALID_PARA);
    }
    if (*px == NULL) {
      code = getStreamDataCache(pTask->pRealtimeContext->pCalcDataCache, groupId, start, end, px);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    *pppIter = px;
  } else {
    stsError("sessionId %" PRId64 " not match with task %" PRId64, sessionId, pTask->pRealtimeContext->sessionId);
    code = TSDB_CODE_INTERNAL_ERROR;
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:

  streamReleaseTask(taskAddr);

  if (code != TSDB_CODE_SUCCESS) {
    stsError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}
