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

#include "streamexecutorInt.h"

#include "executorInt.h"
#include "tdatablock.h"

#define NOTIFY_EVENT_NAME_CACHE_LIMIT_MB 16

typedef struct SStreamNotifyEvent {
  uint64_t gid;
  TSKEY    skey;
  char*    content;
  bool     isEnd;
} SStreamNotifyEvent;

void setStreamOperatorState(SSteamOpBasicInfo* pBasicInfo, EStreamType type) {
  if (type != STREAM_GET_ALL && type != STREAM_CHECKPOINT) {
    pBasicInfo->updateOperatorInfo = true;
  }
}

bool needSaveStreamOperatorInfo(SSteamOpBasicInfo* pBasicInfo) {
  return pBasicInfo->updateOperatorInfo;
}

void saveStreamOperatorStateComplete(SSteamOpBasicInfo* pBasicInfo) {
  pBasicInfo->updateOperatorInfo = false;
}

static void destroyStreamWindowEvent(void* ptr) {
  SStreamNotifyEvent* pEvent = ptr;
  if (pEvent == NULL || pEvent->content == NULL) return;
  cJSON_free(pEvent->content);
}

static void destroyStreamNotifyEventSupp(SStreamNotifyEventSupp* sup) {
  if (sup == NULL) return;
  taosArrayDestroyEx(sup->pWindowEvents, destroyStreamWindowEvent);
  taosHashCleanup(sup->pTableNameHashMap);
  taosHashCleanup(sup->pResultHashMap);
  blockDataDestroy(sup->pEventBlock);
  *sup = (SStreamNotifyEventSupp){0};
}

static int32_t initStreamNotifyEventSupp(SStreamNotifyEventSupp *sup) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SSDataBlock* pBlock = NULL;
  SColumnInfoData infoData = {0};

  if (sup == NULL) {
    goto _end;
  }

  code = createDataBlock(&pBlock);
  QUERY_CHECK_CODE(code, lino, _end);

  pBlock->info.type = STREAM_NOTIFY_EVENT;
  pBlock->info.watermark = INT64_MIN;

  infoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  infoData.info.bytes = tDataTypes[infoData.info.type].bytes;
  code = blockDataAppendColInfo(pBlock, &infoData);
  QUERY_CHECK_CODE(code, lino, _end);

  sup->pWindowEvents = taosArrayInit(0, sizeof(SStreamNotifyEvent));
  QUERY_CHECK_NULL(sup->pWindowEvents, code, lino, _end, terrno);
  sup->pTableNameHashMap = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_NO_LOCK);
  QUERY_CHECK_NULL(sup->pTableNameHashMap, code, lino, _end, terrno);
  sup->pResultHashMap = taosHashInit(4096, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  QUERY_CHECK_NULL(sup->pResultHashMap, code, lino, _end, terrno);
  taosHashSetFreeFp(sup->pResultHashMap, destroyStreamWindowEvent);
  sup->pEventBlock = pBlock;
  pBlock = NULL;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    if (sup) {
      destroyStreamNotifyEventSupp(sup);
    }
  }
  if (pBlock != NULL) {
    blockDataDestroy(pBlock);
  }
  return code;
}

int32_t initStreamBasicInfo(SSteamOpBasicInfo* pBasicInfo) {
  pBasicInfo->primaryPkIndex = -1;
  pBasicInfo->updateOperatorInfo = false;
  return initStreamNotifyEventSupp(&pBasicInfo->windowEventSup);
}

void destroyStreamBasicInfo(SSteamOpBasicInfo* pBasicInfo) {
  destroyStreamNotifyEventSupp(&pBasicInfo->windowEventSup);
}

static void streamNotifyGetEventWindowId(const SSessionKey* pSessionKey, char *buf) {
  uint64_t hash = 0;
  uint64_t ar[2];

  ar[0] = pSessionKey->groupId;
  ar[1] = pSessionKey->win.skey;
  hash = MurmurHash3_64((char*)ar, sizeof(ar));
  buf = u64toaFastLut(hash, buf);
}

#define JSON_CHECK_ADD_ITEM(obj, str, item) \
  QUERY_CHECK_CONDITION(cJSON_AddItemToObjectCS(obj, str, item), code, lino, _end, TSDB_CODE_OUT_OF_MEMORY)

static int32_t jsonAddColumnField(const char* colName, const SColumnInfoData* pColData, int32_t ri, cJSON* obj) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  char*   temp = NULL;

  QUERY_CHECK_NULL(colName, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pColData, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(obj, code, lino, _end, TSDB_CODE_INVALID_PARA);

  if (colDataIsNull_s(pColData, ri)) {
    JSON_CHECK_ADD_ITEM(obj, colName, cJSON_CreateNull());
    goto _end;
  }

  switch (pColData->info.type) {
    case TSDB_DATA_TYPE_BOOL: {
      bool val = *(bool*)colDataGetNumData(pColData, ri);
      JSON_CHECK_ADD_ITEM(obj, colName, cJSON_CreateBool(val));
      break;
    }

    case TSDB_DATA_TYPE_TINYINT: {
      int8_t val = *(int8_t*)colDataGetNumData(pColData, ri);
      JSON_CHECK_ADD_ITEM(obj, colName, cJSON_CreateNumber(val));
      break;
    }

    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t val = *(int16_t*)colDataGetNumData(pColData, ri);
      JSON_CHECK_ADD_ITEM(obj, colName, cJSON_CreateNumber(val));
      break;
    }

    case TSDB_DATA_TYPE_INT: {
      int32_t val = *(int32_t*)colDataGetNumData(pColData, ri);
      JSON_CHECK_ADD_ITEM(obj, colName, cJSON_CreateNumber(val));
      break;
    }

    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP: {
      int64_t val = *(int64_t*)colDataGetNumData(pColData, ri);
      JSON_CHECK_ADD_ITEM(obj, colName, cJSON_CreateNumber(val));
      break;
    }

    case TSDB_DATA_TYPE_FLOAT: {
      float val = *(float*)colDataGetNumData(pColData, ri);
      JSON_CHECK_ADD_ITEM(obj, colName, cJSON_CreateNumber(val));
      break;
    }

    case TSDB_DATA_TYPE_DOUBLE: {
      double val = *(double*)colDataGetNumData(pColData, ri);
      JSON_CHECK_ADD_ITEM(obj, colName, cJSON_CreateNumber(val));
      break;
    }

    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_NCHAR: {
      // cJSON requires null-terminated strings, but this data is not null-terminated,
      // so we need to manually copy the string and add null termination.
      const char* src = varDataVal(colDataGetVarData(pColData, ri));
      int32_t     len = varDataLen(colDataGetVarData(pColData, ri));
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
      uint8_t val = *(uint8_t*)colDataGetNumData(pColData, ri);
      JSON_CHECK_ADD_ITEM(obj, colName, cJSON_CreateNumber(val));
      break;
    }

    case TSDB_DATA_TYPE_USMALLINT: {
      uint16_t val = *(uint16_t*)colDataGetNumData(pColData, ri);
      JSON_CHECK_ADD_ITEM(obj, colName, cJSON_CreateNumber(val));
      break;
    }

    case TSDB_DATA_TYPE_UINT: {
      uint32_t val = *(uint32_t*)colDataGetNumData(pColData, ri);
      JSON_CHECK_ADD_ITEM(obj, colName, cJSON_CreateNumber(val));
      break;
    }

    case TSDB_DATA_TYPE_UBIGINT: {
      uint64_t val = *(uint64_t*)colDataGetNumData(pColData, ri);
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
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (temp) {
    cJSON_free(temp);
  }
  return code;
}

int32_t addEventAggNotifyEvent(EStreamNotifyEventType eventType, const SSessionKey* pSessionKey,
                               const SSDataBlock* pInputBlock, const SNodeList* pCondCols, int32_t ri,
                               SStreamNotifyEventSupp* sup) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SNode*             node = NULL;
  cJSON*             event = NULL;
  cJSON*             fields = NULL;
  cJSON*             cond = NULL;
  SStreamNotifyEvent item = {0};
  char               windowId[32];

  QUERY_CHECK_NULL(pSessionKey, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pInputBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pInputBlock->pDataBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pCondCols, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(sup, code, lino, _end, TSDB_CODE_INVALID_PARA);

  qDebug("add stream notify event from event window, type: %s, start: %" PRId64 ", end: %" PRId64,
         (eventType == SNOTIFY_EVENT_WINDOW_OPEN) ? "WINDOW_OPEN" : "WINDOW_CLOSE", pSessionKey->win.skey,
         pSessionKey->win.ekey);

  event = cJSON_CreateObject();
  QUERY_CHECK_NULL(event, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);

  // add basic info
  streamNotifyGetEventWindowId(pSessionKey, windowId);
  if (eventType == SNOTIFY_EVENT_WINDOW_OPEN) {
    JSON_CHECK_ADD_ITEM(event, "eventType", cJSON_CreateStringReference("WINDOW_OPEN"));
  } else if (eventType == SNOTIFY_EVENT_WINDOW_CLOSE) {
    JSON_CHECK_ADD_ITEM(event, "eventType", cJSON_CreateStringReference("WINDOW_CLOSE"));
  }
  JSON_CHECK_ADD_ITEM(event, "eventTime", cJSON_CreateNumber(taosGetTimestampMs()));
  JSON_CHECK_ADD_ITEM(event, "windowId", cJSON_CreateStringReference(windowId));
  JSON_CHECK_ADD_ITEM(event, "windowType", cJSON_CreateStringReference("Event"));
  JSON_CHECK_ADD_ITEM(event, "windowStart", cJSON_CreateNumber(pSessionKey->win.skey));
  if (eventType == SNOTIFY_EVENT_WINDOW_CLOSE) {
    JSON_CHECK_ADD_ITEM(event, "windowEnd", cJSON_CreateNumber(pSessionKey->win.ekey));
  }

  // create fields object to store matched column values
  fields = cJSON_CreateObject();
  QUERY_CHECK_NULL(fields, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  FOREACH(node, pCondCols) {
    SColumnNode*     pColDef = (SColumnNode*)node;
    SColumnInfoData* pColData = taosArrayGet(pInputBlock->pDataBlock, pColDef->slotId);
    code = jsonAddColumnField(pColDef->colName, pColData, ri, fields);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  // add trigger condition
  cond = cJSON_CreateObject();
  QUERY_CHECK_NULL(cond, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  JSON_CHECK_ADD_ITEM(cond, "conditionIndex", cJSON_CreateNumber(0));
  JSON_CHECK_ADD_ITEM(cond, "fieldValues", fields);
  fields = NULL;
  JSON_CHECK_ADD_ITEM(event, "triggerConditions", cond);
  cond = NULL;

  // convert json object to string value
  item.gid = pSessionKey->groupId;
  item.skey = pSessionKey->win.skey;
  item.isEnd = (eventType == SNOTIFY_EVENT_WINDOW_CLOSE);
  item.content = cJSON_PrintUnformatted(event);
  QUERY_CHECK_NULL(taosArrayPush(sup->pWindowEvents, &item), code, lino, _end, terrno);
  item.content = NULL;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  destroyStreamWindowEvent(&item);
  if (cond != NULL) {
    cJSON_Delete(cond);
  }
  if (fields != NULL) {
    cJSON_Delete(fields);
  }
  if (event != NULL) {
    cJSON_Delete(event);
  }
  return code;
}

int32_t addAggResultNotifyEvent(const SSDataBlock* pResultBlock, const SSchemaWrapper* pSchemaWrapper,
                                SStreamNotifyEventSupp* sup) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SNode *            node = NULL;
  cJSON*             event = NULL;
  cJSON*             result = NULL;
  SStreamNotifyEvent item = {0};
  SColumnInfoData*   pWstartCol = NULL;

  QUERY_CHECK_NULL(pResultBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pSchemaWrapper, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(sup, code, lino, _end, TSDB_CODE_INVALID_PARA);

  qDebug("add %" PRId64 " stream notify results from window agg", pResultBlock->info.rows);

  pWstartCol = taosArrayGet(pResultBlock->pDataBlock, 0);
  for (int32_t i = 0; i< pResultBlock->info.rows; ++i) {
    event = cJSON_CreateObject();
    QUERY_CHECK_NULL(event, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);

    // convert the result row into json
    result = cJSON_CreateObject();
    QUERY_CHECK_NULL(result, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
    for (int32_t j = 0; j < pSchemaWrapper->nCols; ++j) {
      SSchema *pCol = pSchemaWrapper->pSchema + j;
      SColumnInfoData *pColData = taosArrayGet(pResultBlock->pDataBlock, pCol->colId - 1);
      code = jsonAddColumnField(pCol->name, pColData, i, result);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    JSON_CHECK_ADD_ITEM(event, "result", result);
    result = NULL;

    item.gid = pResultBlock->info.id.groupId;
    item.skey = *(uint64_t*)colDataGetNumData(pWstartCol, i);
    item.content = cJSON_PrintUnformatted(event);
    code = taosHashPut(sup->pResultHashMap, &item.gid, sizeof(item.gid) + sizeof(item.skey), &item, sizeof(item));
    TSDB_CHECK_CODE(code, lino, _end);
    item.content = NULL;

    cJSON_Delete(event);
    event = NULL;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  destroyStreamWindowEvent(&item);
  if (result != NULL) {
    cJSON_Delete(result);
  }
  if (event != NULL) {
    cJSON_Delete(event);
  }
  return code;
}

static int32_t streamNotifyGetDestTableName(const SExecTaskInfo* pTaskInfo, uint64_t gid, char** pTableName) {
  int32_t                code = TSDB_CODE_SUCCESS;
  int32_t                lino = 0;
  const SStorageAPI*     pAPI = NULL;
  void*                  tbname = NULL;
  int32_t                winCode = TSDB_CODE_SUCCESS;
  char                   parTbName[TSDB_TABLE_NAME_LEN];
  const SStreamTaskInfo* pStreamInfo = NULL;

  QUERY_CHECK_NULL(pTaskInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pTableName, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *pTableName = NULL;

  pAPI = &pTaskInfo->storageAPI;
  code = pAPI->stateStore.streamStateGetParName((void*)pTaskInfo->streamInfo.pState, gid, &tbname, false, &winCode);
  QUERY_CHECK_CODE(code, lino, _end);
  if (winCode != TSDB_CODE_SUCCESS) {
    parTbName[0] = '\0';
  } else {
    tstrncpy(parTbName, tbname, sizeof(parTbName));
  }
  pAPI->stateStore.streamStateFreeVal(tbname);

  pStreamInfo = &pTaskInfo->streamInfo;
  code = buildSinkDestTableName(parTbName, pStreamInfo->stbFullName, gid, pStreamInfo->newSubTableRule, pTableName);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t streamNotifyFillTableName(const char* tableName, const SStreamNotifyEvent* pEvent,
                                         const SStreamNotifyEvent* pResult, char** pVal) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  static const char* prefix = "{\"tableName\":\"";
  uint64_t           prefixLen = 0;
  uint64_t           nameLen = 0;
  uint64_t           eventLen = 0;
  uint64_t           resultLen = 0;
  uint64_t           valLen = 0;
  char*              val = NULL;
  char*              p = NULL;

  QUERY_CHECK_NULL(tableName, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pEvent, code, lino , _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pVal, code, lino , _end, TSDB_CODE_INVALID_PARA);

  *pVal = NULL;
  prefixLen = strlen(prefix);
  nameLen = strlen(tableName);
  eventLen = strlen(pEvent->content);

  if (pResult != NULL) {
    resultLen = strlen(pResult->content);
    valLen = VARSTR_HEADER_SIZE + prefixLen + nameLen + eventLen + resultLen;
  } else {
    valLen = VARSTR_HEADER_SIZE + prefixLen + nameLen + eventLen + 1;
  }
  val = taosMemoryMalloc(valLen);
  QUERY_CHECK_NULL(val, code, lino, _end, terrno);
  varDataSetLen(val, valLen - VARSTR_HEADER_SIZE);

  p = varDataVal(val);
  TAOS_STRNCPY(p, prefix, prefixLen);
  p += prefixLen;
  TAOS_STRNCPY(p, tableName, nameLen);
  p += nameLen;
  *(p++) = '\"';
  TAOS_STRNCPY(p, pEvent->content, eventLen);
  *p = ',';

  if (pResult != NULL) {
    p += eventLen - 1;
    TAOS_STRNCPY(p, pResult->content, resultLen);
    *p = ',';
  }
  *pVal = val;
  val = NULL;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (val != NULL) {
    taosMemoryFreeClear(val);
  }
  return code;
}

int32_t buildNotifyEventBlock(const SExecTaskInfo* pTaskInfo, SStreamNotifyEventSupp* sup) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SColumnInfoData* pEventStrCol = NULL;
  int32_t          nWindowEvents = 0;
  int32_t          nWindowResults = 0;
  char*            val = NULL;

  if (pTaskInfo == NULL || sup == NULL) {
    goto _end;
  }

  QUERY_CHECK_NULL(sup->pEventBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);
  blockDataCleanup(sup->pEventBlock);
  nWindowEvents = taosArrayGetSize(sup->pWindowEvents);
  nWindowResults = taosHashGetSize(sup->pResultHashMap);
  qDebug("start to build stream notify event block, nWindowEvents: %d, nWindowResults: %d", nWindowEvents,
         nWindowResults);
  if (nWindowEvents == 0) {
    goto _end;
  }

  code = blockDataEnsureCapacity(sup->pEventBlock, nWindowEvents);
  QUERY_CHECK_CODE(code, lino, _end);

  pEventStrCol = taosArrayGet(sup->pEventBlock->pDataBlock, NOTIFY_EVENT_STR_COLUMN_INDEX);
  QUERY_CHECK_NULL(pEventStrCol, code, lino, _end, terrno);

  for (int32_t i = 0; i < nWindowEvents; ++i) {
    SStreamNotifyEvent*  pResult = NULL;
    SStreamNotifyEvent*  pEvent = taosArrayGet(sup->pWindowEvents, i);
    char*                tableName = taosHashGet(sup->pTableNameHashMap, &pEvent->gid, sizeof(pEvent->gid));
    if (tableName == NULL) {
      code = streamNotifyGetDestTableName(pTaskInfo, pEvent->gid, &tableName);
      QUERY_CHECK_CODE(code, lino, _end);
      code = taosHashPut(sup->pTableNameHashMap, &pEvent->gid, sizeof(pEvent->gid), tableName, strlen(tableName) + 1);
      taosMemoryFreeClear(tableName);
      QUERY_CHECK_CODE(code, lino, _end);
      tableName = taosHashGet(sup->pTableNameHashMap, &pEvent->gid, sizeof(pEvent->gid));
      QUERY_CHECK_NULL(tableName, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    }
    if (pEvent->isEnd) {
      pResult = taosHashGet(sup->pResultHashMap, &pEvent->gid, sizeof(pEvent->gid) + sizeof(pEvent->skey));
      QUERY_CHECK_NULL(pResult, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    }
    code = streamNotifyFillTableName(tableName, pEvent, pResult, &val);
    QUERY_CHECK_CODE(code, lino, _end);
    code = colDataSetVal(pEventStrCol, i, val, false);
    QUERY_CHECK_CODE(code, lino, _end);
    taosMemoryFreeClear(val);
    sup->pEventBlock->info.rows++;
  }

  if (taosHashGetMemSize(sup->pTableNameHashMap) >= NOTIFY_EVENT_NAME_CACHE_LIMIT_MB * 1024 * 1024) {
    taosHashClear(sup->pTableNameHashMap);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (val != NULL) {
    taosMemoryFreeClear(val);
  }
  if (sup != NULL) {
    taosArrayClearEx(sup->pWindowEvents, destroyStreamWindowEvent);
    taosHashClear(sup->pResultHashMap);
  }
  return code;
}
