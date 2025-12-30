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
#include "operator.h"
#include "cmdnodes.h"
#include "tdatablock.h"

#define UPDATE_OPERATOR_INFO       BIT_FLAG_MASK(0)
#define FILL_HISTORY_OPERATOR      BIT_FLAG_MASK(1)
#define RECALCULATE_OPERATOR       BIT_FLAG_MASK(2)
#define SEMI_OPERATOR              BIT_FLAG_MASK(3)
#define FINAL_OPERATOR             BIT_FLAG_MASK(4)
#define SINGLE_OPERATOR            BIT_FLAG_MASK(5)

#define NOTIFY_EVENT_NAME_CACHE_LIMIT_MB 16

typedef struct SStreamNotifyEvent {
  uint64_t    gid;
  int64_t     eventType;
  STimeWindow win;
  cJSON*      pJson;
} SStreamNotifyEvent;

#define NOTIFY_EVENT_KEY_SIZE                                                                            \
  ((sizeof(((struct SStreamNotifyEvent*)0)->gid) + sizeof(((struct SStreamNotifyEvent*)0)->eventType)) + \
   sizeof(((struct SStreamNotifyEvent*)0)->win.skey))

static void destroyStreamWindowEvent(void* ptr) {
  SStreamNotifyEvent* pEvent = (SStreamNotifyEvent*)ptr;
  if (pEvent) {
    if (pEvent->pJson) {
      cJSON_Delete(pEvent->pJson);
    }
    *pEvent = (SStreamNotifyEvent){0};
  }
}

static void destroyStreamNotifyEventSupp(SStreamNotifyEventSupp* sup) {
  if (sup == NULL) return;
  taosHashCleanup(sup->pWindowEventHashMap);
  taosHashCleanup(sup->pTableNameHashMap);
  blockDataDestroy(sup->pEventBlock);
  taosArrayDestroy(sup->pSessionKeys);
  *sup = (SStreamNotifyEventSupp){0};
}

static int32_t initStreamNotifyEventSupp(SStreamNotifyEventSupp* sup, const char* windowType, int32_t resCapacity) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SSDataBlock*    pBlock = NULL;
  SColumnInfoData infoData = {0};

  if (sup == NULL || sup->pWindowEventHashMap != NULL) {
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

  sup->pWindowEventHashMap = taosHashInit(4096, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  QUERY_CHECK_NULL(sup->pWindowEventHashMap, code, lino, _end, terrno);
  taosHashSetFreeFp(sup->pWindowEventHashMap, destroyStreamWindowEvent);
  sup->pTableNameHashMap = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_NO_LOCK);
  QUERY_CHECK_NULL(sup->pTableNameHashMap, code, lino, _end, terrno);
  sup->pEventBlock = pBlock;
  pBlock = NULL;
  code = blockDataEnsureCapacity(sup->pEventBlock, resCapacity);
  QUERY_CHECK_CODE(code, lino, _end);
  sup->windowType = windowType;
  sup->pSessionKeys = taosArrayInit(resCapacity, sizeof(SSessionKey));
  QUERY_CHECK_NULL(sup->pSessionKeys, code, lino, _end, terrno);

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

int32_t initStreamBasicInfo(SSteamOpBasicInfo* pBasicInfo, const struct SOperatorInfo* pOperator) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  pBasicInfo->primaryPkIndex = -1;
  pBasicInfo->operatorFlag = 0;
  code = createSpecialDataBlock(STREAM_DELETE_RESULT, &pBasicInfo->pDelRes);
  QUERY_CHECK_CODE(code, lino, _end);

  pBasicInfo->pUpdated = taosArrayInit(1024, sizeof(SResultWindowInfo));
  QUERY_CHECK_NULL(pBasicInfo->pUpdated, code, lino, _end, terrno);

  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pBasicInfo->pSeDeleted = tSimpleHashInit(32, hashFn);

  const char* windowType = NULL;
//  if (IS_NORMAL_INTERVAL_OP(pOperator)) {
//    windowType = "Time";
//  } else if (IS_NORMAL_SESSION_OP(pOperator)) {
//    windowType = "Session";
//  } else if (IS_NORMAL_STATE_OP(pOperator)) {
//    windowType = "State";
//  } else if (IS_NORMAL_EVENT_OP(pOperator)) {
//    windowType = "Event";
//  } else if (IS_NORMAL_COUNT_OP(pOperator)) {
//    windowType = "Count";
//  } else {
//    return TSDB_CODE_SUCCESS;
//  }
  code = initStreamNotifyEventSupp(&pBasicInfo->notifyEventSup, windowType, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

void destroyStreamBasicInfo(SSteamOpBasicInfo* pBasicInfo) {
  blockDataDestroy(pBasicInfo->pCheckpointRes);
  pBasicInfo->pCheckpointRes = NULL;

  tSimpleHashCleanup(pBasicInfo->pSeDeleted);
  pBasicInfo->pSeDeleted = NULL;

  blockDataDestroy(pBasicInfo->pDelRes);
  pBasicInfo->pDelRes = NULL;
  pBasicInfo->pUpdated = NULL;

  pBasicInfo->pTsDataState = NULL;

  destroyStreamNotifyEventSupp(&pBasicInfo->notifyEventSup);
}

static int32_t encodeStreamNotifyEventSupp(void** buf, const SStreamNotifyEventSupp* sup) {
  int32_t tlen = 0;
  void*   pIter = NULL;
  char*   str = NULL;

  if (sup == NULL) {
    return tlen;
  }

  tlen += taosEncodeFixedI32(buf, taosHashGetSize(sup->pWindowEventHashMap));
  pIter = taosHashIterate(sup->pWindowEventHashMap, NULL);
  while (pIter) {
    const SStreamNotifyEvent* pEvent = (const SStreamNotifyEvent*)pIter;
    str = cJSON_PrintUnformatted(pEvent->pJson);

    tlen += taosEncodeFixedU64(buf, pEvent->gid);
    tlen += taosEncodeFixedI64(buf, pEvent->eventType);
    tlen += taosEncodeFixedI64(buf, pEvent->win.skey);
    tlen += taosEncodeFixedI64(buf, pEvent->win.ekey);
    tlen += taosEncodeString(buf, str);
    cJSON_free(str);
    pIter = taosHashIterate(sup->pWindowEventHashMap, pIter);
  }
  return tlen;
}

static int32_t decodeStreamNotifyEventSupp(void** buf, SStreamNotifyEventSupp* sup) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  void*              p = *buf;
  int32_t            size = 0;
  uint64_t           len = 0;
  SStreamNotifyEvent item = {0};

  p = taosDecodeFixedI32(p, &size);
  for (int32_t i = 0; i < size; i++) {
    p = taosDecodeFixedU64(p, &item.gid);
    p = taosDecodeFixedI64(p, &item.eventType);
    p = taosDecodeFixedI64(p, &item.win.skey);
    p = taosDecodeFixedI64(p, &item.win.ekey);
    p = taosDecodeVariantU64(p, &len);
    item.pJson = cJSON_Parse(p);
    if (item.pJson == NULL) {
      qWarn("failed to parse the json content since %s", cJSON_GetErrorPtr());
    }
    QUERY_CHECK_NULL(item.pJson, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    p = POINTER_SHIFT(p, len);
    code = taosHashPut(sup->pWindowEventHashMap, &item, NOTIFY_EVENT_KEY_SIZE, &item, sizeof(SStreamNotifyEvent));
    QUERY_CHECK_CODE(code, lino, _end);
    item.pJson = NULL;
  }
  *buf = p;
_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  destroyStreamWindowEvent(&item);
  return code;
}

int32_t encodeStreamBasicInfo(void** buf, const SSteamOpBasicInfo* pBasicInfo) {
  return encodeStreamNotifyEventSupp(buf, &pBasicInfo->notifyEventSup);
}

int32_t decodeStreamBasicInfo(void** buf, SSteamOpBasicInfo* pBasicInfo) {
  return decodeStreamNotifyEventSupp(buf, &pBasicInfo->notifyEventSup);
}

static void streamNotifyGetEventWindowId(const SSessionKey* pSessionKey, char* buf) {
  uint64_t hash = 0;
  uint64_t ar[2];

  ar[0] = pSessionKey->groupId;
  ar[1] = pSessionKey->win.skey;
  hash = MurmurHash3_64((char*)ar, sizeof(ar));
  buf = u64toaFastLut(hash, buf);
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
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (temp) {
    cJSON_free(temp);
  }
  return code;
}

static cJSON* createBasicAggNotifyEvent(const char* windowType, EStreamNotifyEventType eventType,
                                        const SSessionKey* pSessionKey) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  const char* eventTypeStr = NULL;
  cJSON*      event = NULL;
  char        windowId[32];
  char        groupId[32];

  QUERY_CHECK_NULL(windowType, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pSessionKey, code, lino, _end, TSDB_CODE_INVALID_PARA);

  if (eventType == SNOTIFY_EVENT_WINDOW_OPEN) {
    eventTypeStr = "WINDOW_OPEN";
  } else if (eventType == SNOTIFY_EVENT_WINDOW_CLOSE) {
    eventTypeStr = "WINDOW_CLOSE";
  } else if (eventType == SNOTIFY_EVENT_WINDOW_INVALIDATION) {
    eventTypeStr = "WINDOW_INVALIDATION";
  } else {
    QUERY_CHECK_CONDITION(false, code, lino, _end, TSDB_CODE_INVALID_PARA);
  }

  qDebug("add stream notify event from %s Window, type: %s, start: %" PRId64 ", end: %" PRId64, windowType,
         eventTypeStr, pSessionKey->win.skey, pSessionKey->win.ekey);

  event = cJSON_CreateObject();
  QUERY_CHECK_NULL(event, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);

  // add basic info
  streamNotifyGetEventWindowId(pSessionKey, windowId);
  JSON_CHECK_ADD_ITEM(event, "eventType", cJSON_CreateStringReference(eventTypeStr));
  JSON_CHECK_ADD_ITEM(event, "eventTime", cJSON_CreateNumber(taosGetTimestampMs()));
  JSON_CHECK_ADD_ITEM(event, "windowId", cJSON_CreateString(windowId));
  JSON_CHECK_ADD_ITEM(event, "windowType", cJSON_CreateStringReference(windowType));
  char* p = u64toaFastLut(pSessionKey->groupId, groupId);
  JSON_CHECK_ADD_ITEM(event, "groupId", cJSON_CreateString(groupId));
  JSON_CHECK_ADD_ITEM(event, "windowStart", cJSON_CreateNumber(pSessionKey->win.skey));
  if (eventType != SNOTIFY_EVENT_WINDOW_OPEN) {
    if (strcmp(windowType, "Time") == 0) {
      JSON_CHECK_ADD_ITEM(event, "windowEnd", cJSON_CreateNumber(pSessionKey->win.ekey + 1));
    } else {
      JSON_CHECK_ADD_ITEM(event, "windowEnd", cJSON_CreateNumber(pSessionKey->win.ekey));
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    terrno = code;
    cJSON_Delete(event);
    event = NULL;
  }
  return event;
}

int32_t addEventAggNotifyEvent(EStreamNotifyEventType eventType, const SSessionKey* pSessionKey,
                               const SSDataBlock* pInputBlock, const SNodeList* pCondCols, int32_t ri,
                               SStreamNotifyEventSupp* sup, STaskNotifyEventStat* pNotifyEventStat) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  cJSON*             event = NULL;
  cJSON*             fields = NULL;
  cJSON*             cond = NULL;
  const SNode*       pNode = NULL;
  int32_t            origSize = 0;
  int64_t            startTime = 0;
  int64_t            endTime = 0;
  SStreamNotifyEvent item = {0};

  QUERY_CHECK_NULL(pInputBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pInputBlock->pDataBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pCondCols, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(sup, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(sup->windowType, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pNotifyEventStat, code, lino, _end, TSDB_CODE_INVALID_PARA);

  startTime = taosGetMonoTimestampMs();
  event = createBasicAggNotifyEvent(sup->windowType, eventType, pSessionKey);
  QUERY_CHECK_NULL(event, code, lino, _end, terrno);

  // create fields object to store matched column values
  fields = cJSON_CreateObject();
  QUERY_CHECK_NULL(fields, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  FOREACH(pNode, pCondCols) {
    const SColumnNode*     pColDef = (const SColumnNode*)pNode;
    const SColumnInfoData* pColData = taosArrayGet(pInputBlock->pDataBlock, pColDef->slotId);
    code = jsonAddColumnField(pColDef->colName, pColData->info.type, colDataIsNull_s(pColData, ri),
                              colDataGetData(pColData, ri), fields);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  // add trigger condition
  cond = cJSON_CreateObject();
  QUERY_CHECK_NULL(cond, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  JSON_CHECK_ADD_ITEM(cond, "conditionIndex", cJSON_CreateNumber(0));
  JSON_CHECK_ADD_ITEM(cond, "fieldValues", fields);
  fields = NULL;
  JSON_CHECK_ADD_ITEM(event, "triggerCondition", cond);
  cond = NULL;

  item.gid = pSessionKey->groupId;
  item.win = pSessionKey->win;
  item.eventType = eventType;
  item.pJson = event;
  event = NULL;
  origSize = taosHashGetSize(sup->pWindowEventHashMap);
  code = taosHashPut(sup->pWindowEventHashMap, &item, NOTIFY_EVENT_KEY_SIZE, &item, sizeof(SStreamNotifyEvent));
  QUERY_CHECK_CODE(code, lino, _end);
  item.pJson = NULL;

  endTime = taosGetMonoTimestampMs();
  pNotifyEventStat->notifyEventAddTimes++;
  pNotifyEventStat->notifyEventAddElems += taosHashGetSize(sup->pWindowEventHashMap) - origSize;
  pNotifyEventStat->notifyEventAddCostSec += (endTime - startTime) / 1000.0;
  pNotifyEventStat->notifyEventHoldElems = taosHashGetSize(sup->pWindowEventHashMap);

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

int32_t addStateAggNotifyEvent(EStreamNotifyEventType eventType, const SSessionKey* pSessionKey,
                               const SStateKeys* pCurState, const SStateKeys* pAnotherState, bool onlyUpdate,
                               SStreamNotifyEventSupp* sup, STaskNotifyEventStat* pNotifyEventStat) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  cJSON*             event = NULL;
  int32_t            origSize = 0;
  int64_t            startTime = 0;
  int64_t            endTime = 0;
  SStreamNotifyEvent item = {0};

  QUERY_CHECK_NULL(pCurState, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(sup, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(sup->windowType, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pNotifyEventStat, code, lino, _end, TSDB_CODE_INVALID_PARA);

  item.gid = pSessionKey->groupId;
  item.win = pSessionKey->win;
  item.eventType = eventType;
  // Check if the notify event exists for update
  if (onlyUpdate && taosHashGet(sup->pWindowEventHashMap, &item, NOTIFY_EVENT_KEY_SIZE) == NULL) {
    goto _end;
  }

  startTime = taosGetMonoTimestampMs();
  event = createBasicAggNotifyEvent(sup->windowType, eventType, pSessionKey);
  QUERY_CHECK_NULL(event, code, lino, _end, terrno);

  // add state value
  if (eventType == SNOTIFY_EVENT_WINDOW_OPEN) {
    if (pAnotherState) {
      code = jsonAddColumnField("prevState", pAnotherState->type, pAnotherState->isNull, pAnotherState->pData, event);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      code = jsonAddColumnField("prevState", pCurState->type, true, NULL, event);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }
  code = jsonAddColumnField("curState", pCurState->type, pCurState->isNull, pCurState->pData, event);
  QUERY_CHECK_CODE(code, lino, _end);
  if (eventType == SNOTIFY_EVENT_WINDOW_CLOSE) {
    if (pAnotherState) {
      code = jsonAddColumnField("nextState", pAnotherState->type, pAnotherState->isNull, pAnotherState->pData, event);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      code = jsonAddColumnField("nextState", pCurState->type, true, NULL, event);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  item.pJson = event;
  event = NULL;
  origSize = taosHashGetSize(sup->pWindowEventHashMap);
  code = taosHashPut(sup->pWindowEventHashMap, &item, NOTIFY_EVENT_KEY_SIZE, &item, sizeof(SStreamNotifyEvent));
  QUERY_CHECK_CODE(code, lino, _end);
  item.pJson = NULL;

  endTime = taosGetMonoTimestampMs();
  pNotifyEventStat->notifyEventAddTimes++;
  pNotifyEventStat->notifyEventAddElems += taosHashGetSize(sup->pWindowEventHashMap) - origSize;
  pNotifyEventStat->notifyEventAddCostSec += (endTime - startTime) / 1000.0;
  pNotifyEventStat->notifyEventHoldElems = taosHashGetSize(sup->pWindowEventHashMap);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  destroyStreamWindowEvent(&item);
  if (event != NULL) {
    cJSON_Delete(event);
  }
  return code;
}

static int32_t addNormalAggNotifyEvent(EStreamNotifyEventType eventType, const SSessionKey* pSessionKey,
                                       SStreamNotifyEventSupp* sup, STaskNotifyEventStat* pNotifyEventStat) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  cJSON*             event = NULL;
  int32_t            origSize = 0;
  int64_t            startTime = 0;
  int64_t            endTime = 0;
  SStreamNotifyEvent item = {0};

  QUERY_CHECK_NULL(pSessionKey, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(sup, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(sup->windowType, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pNotifyEventStat, code, lino, _end, TSDB_CODE_INVALID_PARA);

  startTime = taosGetMonoTimestampMs();
  event = createBasicAggNotifyEvent(sup->windowType, eventType, pSessionKey);
  QUERY_CHECK_NULL(event, code, lino, _end, terrno);

  item.gid = pSessionKey->groupId;
  item.win = pSessionKey->win;
  item.eventType = eventType;
  item.pJson = event;
  event = NULL;
  origSize = taosHashGetSize(sup->pWindowEventHashMap);
  code = taosHashPut(sup->pWindowEventHashMap, &item, NOTIFY_EVENT_KEY_SIZE, &item, sizeof(SStreamNotifyEvent));
  QUERY_CHECK_CODE(code, lino, _end);
  item.pJson = NULL;

  endTime = taosGetMonoTimestampMs();
  pNotifyEventStat->notifyEventAddTimes++;
  pNotifyEventStat->notifyEventAddElems += taosHashGetSize(sup->pWindowEventHashMap) - origSize;
  pNotifyEventStat->notifyEventAddCostSec += (endTime - startTime) / 1000.0;
  pNotifyEventStat->notifyEventHoldElems = taosHashGetSize(sup->pWindowEventHashMap);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  destroyStreamWindowEvent(&item);
  if (event != NULL) {
    cJSON_Delete(event);
  }
  return code;
}

int32_t addIntervalAggNotifyEvent(EStreamNotifyEventType eventType, const SSessionKey* pSessionKey,
                                  SStreamNotifyEventSupp* sup, STaskNotifyEventStat* pNotifyEventStat) {
  return addNormalAggNotifyEvent(eventType, pSessionKey, sup, pNotifyEventStat);
}

int32_t addSessionAggNotifyEvent(EStreamNotifyEventType eventType, const SSessionKey* pSessionKey,
                                 SStreamNotifyEventSupp* sup, STaskNotifyEventStat* pNotifyEventStat) {
  return addNormalAggNotifyEvent(eventType, pSessionKey, sup, pNotifyEventStat);
}

int32_t addCountAggNotifyEvent(EStreamNotifyEventType eventType, const SSessionKey* pSessionKey,
                               SStreamNotifyEventSupp* sup, STaskNotifyEventStat* pNotifyEventStat) {
  return addNormalAggNotifyEvent(eventType, pSessionKey, sup, pNotifyEventStat);
}

int32_t addAggResultNotifyEvent(const SSDataBlock* pResultBlock, const SArray* pSessionKeys,
                                const SSchemaWrapper* pSchemaWrapper, SStreamNotifyEventSupp* sup,
                                STaskNotifyEventStat* pNotifyEventStat) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  cJSON*             result = NULL;
  int32_t            origSize = 0;
  int64_t            startTime = 0;
  int64_t            endTime = 0;
  SStreamNotifyEvent item = {0};

  QUERY_CHECK_NULL(pResultBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pSessionKeys, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pSchemaWrapper, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(sup, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pNotifyEventStat, code, lino, _end, TSDB_CODE_INVALID_PARA);

  qDebug("add %" PRId64 " stream notify results from window agg", pResultBlock->info.rows);
  startTime = taosGetMonoTimestampMs();
  origSize = taosHashGetSize(sup->pWindowEventHashMap);

  for (int32_t i = 0; i < pResultBlock->info.rows; ++i) {
    const SSessionKey* pSessionKey = taosArrayGet(pSessionKeys, i);
    item.gid = pSessionKey->groupId;
    item.win = pSessionKey->win;
    item.eventType = SNOTIFY_EVENT_WINDOW_CLOSE;
    SStreamNotifyEvent* pItem = taosHashGet(sup->pWindowEventHashMap, &item, NOTIFY_EVENT_KEY_SIZE);
    if (pItem == NULL) {
      item.pJson = createBasicAggNotifyEvent(sup->windowType, SNOTIFY_EVENT_WINDOW_CLOSE, pSessionKey);
      QUERY_CHECK_NULL(item.pJson, code, lino, _end, terrno);
      if (strcmp(sup->windowType, "Event") == 0) {
        JSON_CHECK_ADD_ITEM(item.pJson, "triggerCondition", cJSON_CreateNull());
      } else if (strcmp(sup->windowType, "State") == 0) {
        JSON_CHECK_ADD_ITEM(item.pJson, "curState", cJSON_CreateNull());
        JSON_CHECK_ADD_ITEM(item.pJson, "nextState", cJSON_CreateNull());
      }
      code = taosHashPut(sup->pWindowEventHashMap, &item, NOTIFY_EVENT_KEY_SIZE, &item, sizeof(SStreamNotifyEvent));
      QUERY_CHECK_CODE(code, lino, _end);
      item.pJson = NULL;
      pItem = taosHashGet(sup->pWindowEventHashMap, &item, NOTIFY_EVENT_KEY_SIZE);
      QUERY_CHECK_NULL(pItem, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    }

    // convert the result row into json
    result = cJSON_CreateObject();
    QUERY_CHECK_NULL(result, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
    for (int32_t j = 0; j < pSchemaWrapper->nCols; ++j) {
      const SSchema* pCol = pSchemaWrapper->pSchema + j;
      if (pCol->colId - 1 < taosArrayGetSize(pResultBlock->pDataBlock)) {
        const SColumnInfoData* pColData = taosArrayGet(pResultBlock->pDataBlock, pCol->colId - 1);
        code = jsonAddColumnField(pCol->name, pColData->info.type, colDataIsNull_s(pColData, i),
                                  colDataGetData(pColData, i), result);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
    JSON_CHECK_ADD_ITEM(pItem->pJson, "result", result);
    result = NULL;
  }

  endTime = taosGetMonoTimestampMs();
  pNotifyEventStat->notifyEventAddTimes++;
  pNotifyEventStat->notifyEventAddElems += taosHashGetSize(sup->pWindowEventHashMap) - origSize;
  pNotifyEventStat->notifyEventAddCostSec += (endTime - startTime) / 1000.0;
  pNotifyEventStat->notifyEventHoldElems = taosHashGetSize(sup->pWindowEventHashMap);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  destroyStreamWindowEvent(&item);
  if (result != NULL) {
    cJSON_Delete(result);
  }
  return code;
}

int32_t addAggDeleteNotifyEvent(const SSDataBlock* pDeleteBlock, SStreamNotifyEventSupp* sup,
                                STaskNotifyEventStat* pNotifyEventStat) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SSessionKey      sessionKey = {0};
  SColumnInfoData* pWstartCol = NULL;
  SColumnInfoData* pWendCol = NULL;
  SColumnInfoData* pGroupIdCol = NULL;

  QUERY_CHECK_NULL(pDeleteBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(sup, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pNotifyEventStat, code, lino, _end, TSDB_CODE_INVALID_PARA);

  qDebug("add %" PRId64 " stream notify delete events from window agg", pDeleteBlock->info.rows);

  pWstartCol = taosArrayGet(pDeleteBlock->pDataBlock, START_TS_COLUMN_INDEX);
  pWendCol = taosArrayGet(pDeleteBlock->pDataBlock, END_TS_COLUMN_INDEX);
  pGroupIdCol = taosArrayGet(pDeleteBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  for (int32_t i = 0; i < pDeleteBlock->info.rows; ++i) {
    sessionKey.win.skey = *(int64_t*)colDataGetNumData(pWstartCol, i);
    sessionKey.win.ekey = *(int64_t*)colDataGetNumData(pWendCol, i);
    sessionKey.groupId = *(uint64_t*)colDataGetNumData(pGroupIdCol, i);
    code = addNormalAggNotifyEvent(SNOTIFY_EVENT_WINDOW_INVALIDATION, &sessionKey, sup, pNotifyEventStat);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
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

int32_t buildNotifyEventBlock(const SExecTaskInfo* pTaskInfo, SStreamNotifyEventSupp* sup,
                              STaskNotifyEventStat* pNotifyEventStat) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  int32_t          nWindowEvents = 0;
  SColumnInfoData* pEventStrCol = NULL;
  int64_t          startTime = 0;
  int64_t          endTime = 0;
  void*            pIter = NULL;

  if (pTaskInfo == NULL || sup == NULL || sup->pEventBlock == NULL || pNotifyEventStat == NULL) {
    goto _end;
  }

  QUERY_CHECK_NULL(sup->pEventBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);

  startTime = taosGetMonoTimestampMs();
  blockDataCleanup(sup->pEventBlock);
  nWindowEvents = taosHashGetSize(sup->pWindowEventHashMap);
  qDebug("start to build stream notify event block, nWindowEvents: %d", nWindowEvents);

  pEventStrCol = taosArrayGet(sup->pEventBlock->pDataBlock, NOTIFY_EVENT_STR_COLUMN_INDEX);
  QUERY_CHECK_NULL(pEventStrCol, code, lino, _end, terrno);

  // Append all events content into data block.
  pIter = taosHashIterate(sup->pWindowEventHashMap, NULL);
  while (pIter) {
    const SStreamNotifyEvent* pEvent = (const SStreamNotifyEvent*)pIter;
    pIter = taosHashIterate(sup->pWindowEventHashMap, pIter);
    if (pEvent->eventType == SNOTIFY_EVENT_WINDOW_CLOSE && !cJSON_HasObjectItem(pEvent->pJson, "result")) {
      // current WINDOW_CLOSE event cannot be pushed yet due to watermark
      continue;
    }

    // get name of the dest child table
    char* tableName = taosHashGet(sup->pTableNameHashMap, &pEvent->gid, sizeof(&pEvent->gid));
    if (tableName == NULL) {
      code = streamNotifyGetDestTableName(pTaskInfo, pEvent->gid, &tableName);
      QUERY_CHECK_CODE(code, lino, _end);
      code = taosHashPut(sup->pTableNameHashMap, &pEvent->gid, sizeof(pEvent->gid), tableName, strlen(tableName) + 1);
      taosMemoryFreeClear(tableName);
      QUERY_CHECK_CODE(code, lino, _end);
      tableName = taosHashGet(sup->pTableNameHashMap, &pEvent->gid, sizeof(pEvent->gid));
      QUERY_CHECK_NULL(tableName, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    }
    JSON_CHECK_ADD_ITEM(pEvent->pJson, "tableName", cJSON_CreateStringReference(tableName));

    // convert the json object into string and append it into the block
    char* str = cJSON_PrintUnformatted(pEvent->pJson);
    QUERY_CHECK_NULL(str, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
    int32_t len = strlen(str);
    code = varColSetVarData(pEventStrCol, sup->pEventBlock->info.rows, str, len, false);
    cJSON_free(str);
    QUERY_CHECK_CODE(code, lino, _end);
    sup->pEventBlock->info.rows++;
    code = taosHashRemove(sup->pWindowEventHashMap, pEvent, NOTIFY_EVENT_KEY_SIZE);
    if (code == TSDB_CODE_NOT_FOUND) {
      code = TSDB_CODE_SUCCESS;
    }
    QUERY_CHECK_CODE(code, lino, _end);
    if (sup->pEventBlock->info.rows >= sup->pEventBlock->info.capacity) {
      break;
    }
  }

  if (taosHashGetMemSize(sup->pTableNameHashMap) >= NOTIFY_EVENT_NAME_CACHE_LIMIT_MB * 1024 * 1024) {
    taosHashClear(sup->pTableNameHashMap);
  }

  endTime = taosGetMonoTimestampMs();
  if (sup->pEventBlock->info.rows > 0) {
    pNotifyEventStat->notifyEventPushTimes++;
    pNotifyEventStat->notifyEventPushElems += sup->pEventBlock->info.rows;
    pNotifyEventStat->notifyEventPushCostSec += (endTime - startTime) / 1000.0;
  }
  pNotifyEventStat->notifyEventHoldElems = taosHashGetSize(sup->pWindowEventHashMap);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (pIter) {
    taosHashCancelIterate(sup->pWindowEventHashMap, pIter);
  }
  return code;
}

int32_t removeOutdatedNotifyEvents(STimeWindowAggSupp* pTwSup, SStreamNotifyEventSupp* sup,
                                   STaskNotifyEventStat* pNotifyEventStat) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  void*   pIter = NULL;

  if (pTwSup || sup == NULL || pNotifyEventStat == NULL) {
    goto _end;
  }

  pIter = taosHashIterate(sup->pWindowEventHashMap, NULL);
  while (pIter) {
    const SStreamNotifyEvent* pEvent = (const SStreamNotifyEvent*)pIter;
    pIter = taosHashIterate(sup->pWindowEventHashMap, pIter);
  }

  pNotifyEventStat->notifyEventHoldElems = taosHashGetSize(sup->pWindowEventHashMap);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

void setFinalOperatorFlag(SSteamOpBasicInfo* pBasicInfo) {
  BIT_FLAG_SET_MASK(pBasicInfo->operatorFlag, FINAL_OPERATOR);
}

bool isFinalOperator(SSteamOpBasicInfo* pBasicInfo) {
  return BIT_FLAG_TEST_MASK(pBasicInfo->operatorFlag, FINAL_OPERATOR);
}

void setRecalculateOperatorFlag(SSteamOpBasicInfo* pBasicInfo) {
  BIT_FLAG_SET_MASK(pBasicInfo->operatorFlag, RECALCULATE_OPERATOR);
}

void unsetRecalculateOperatorFlag(SSteamOpBasicInfo* pBasicInfo) {
  BIT_FLAG_UNSET_MASK(pBasicInfo->operatorFlag, RECALCULATE_OPERATOR);
}

bool isRecalculateOperator(SSteamOpBasicInfo* pBasicInfo) {
  return BIT_FLAG_TEST_MASK(pBasicInfo->operatorFlag, RECALCULATE_OPERATOR);
}

void setSingleOperatorFlag(SSteamOpBasicInfo* pBasicInfo) {
  BIT_FLAG_SET_MASK(pBasicInfo->operatorFlag, SINGLE_OPERATOR);
}

bool isSingleOperator(SSteamOpBasicInfo* pBasicInfo) {
  return BIT_FLAG_TEST_MASK(pBasicInfo->operatorFlag, SINGLE_OPERATOR);
}
