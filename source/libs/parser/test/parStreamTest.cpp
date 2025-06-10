/*
* Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
*
* This program is free software: you can use, redistribute, and/or modify
* it under the terms of the GNU Affero General Public License, version 3
* or later ("AGPL"), AS published by the Free Software Foundation.
*
* This program is distributed in the hope that it will be useful, but WITHOUT
* ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
* FITNESS FOR A PARTICULAR PURPOSE.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

#include <fstream>

#include "cJSON.h"
#include "parTestUtil.h"

using namespace std;

namespace ParserTest {
class ParserStreamTest : public ParserDdlTest {};

/*
* CREATE STREAM [IF NOT EXISTS] stream_name stream_options [INTO [db_name.]table_name] [OUTPUT_SUBTABLE(tbname_expr)] [(column_name1, column_name2 [PRIMARY KEY][, ...])] [TAGS (tag_definition [, ...])] [AS subquery]
*
* stream_options: {
*     trigger_type [FROM [db_name.]table_name] [PARTITION BY col1 [, ...]] [OPTIONS(stream_option [|...])] [notification_definition]
* }
*
* trigger_type: {
*     SESSION(ts_col, session_val)
*   | STATE_WINDOW(col) [TRUE_FOR(duration_time)]
*   | [INTERVAL(interval_val[, interval_offset])] SLIDING(sliding_val[, offset_time])
*   | EVENT_WINDOW(START WITH start_condition END WITH end_condition) [TRUE_FOR(duration_time)]
*   | COUNT_WINDOW(count_val[, sliding_val][, col1[, ...]])
*   | PERIOD(period_time[, offset_time])
* }
*
* event_types:
*     event_type [|event_type]
*
* event_type: {WINDOW_OPEN | WINDOW_CLOSE}
*
* stream_option: {WATERMARK(duration_time) | EXPIRED_TIME(exp_time) | IGNORE_DISORDER | DELETE_RECALC | DELETE_OUTPUT_TABLE | FILL_HISTORY(start_time) | FILL_HISTORY_FIRST(start_time) | CALC_NOTIFY_ONLY | LOW_LATENCY_CALC | PRE_FILTER(expr) | FORCE_OUTPUT | MAX_DELAY(delay_time) | EVENT_TYPE(event_types)}
*
* notification_definition:
*     NOTIFY(url [, ...]) [ON (event_types)] [WHERE condition] [NOTIFY_OPTIONS(notify_option [| notify_option])]
*
* notify_option: [NOTIFY_HISTORY | ON_FAILURE_PAUSE]
*
* tag_definition:
*     tag_name type_name [COMMENT 'string_value'] AS expr
*/

void setCreateStreamStreamName(SCMCreateStreamReq *expect, const char* pStream, const char* pStreamDb) {
  expect->name = taosStrdup(pStream);
  expect->streamDB = taosStrdup(pStreamDb);
}

// trigger part
void setCreateStreamTriggerType(SCMCreateStreamReq *expect, int8_t triggerType) {
  expect->triggerType = triggerType;
}

void setCreateStreamTriggerTableType(SCMCreateStreamReq *expect, int8_t triggerTableType) {
  expect->triggerTblType = triggerTableType;
}

void setCreateStreamTriggerTableSuid(SCMCreateStreamReq *expect, uint64_t triggerTblSUid) {
  expect->triggerTblSuid = triggerTblSUid;
}

void setCreateStreamTriggerTblUid(SCMCreateStreamReq *expect, uint64_t triggerTblUid) {
  expect->triggerTblUid = triggerTblUid;
}

void setCreateStreamTriggerTblVgid(SCMCreateStreamReq *expect, int8_t triggerTblVgid) {
  expect->triggerTblVgId = triggerTblVgid;
}

void setCreateStreamTriggerName(SCMCreateStreamReq *expect, const char* pTriggerDb, const char* pTriggerTblName) {
  expect->triggerDB = taosStrdup(pTriggerDb);
  expect->triggerTblName = taosStrdup(pTriggerTblName);
}

void setCreateStreamTriggerTable(SCMCreateStreamReq *expect, const char* pTriggerDb, const char* pTriggerTblName,
                                int8_t triggerType, int8_t triggerTblType, uint64_t triggerTblUid, uint64_t triggerTblSuid, int8_t triggerTblVgid) {
  setCreateStreamTriggerName(expect, pTriggerDb, pTriggerTblName);
  setCreateStreamTriggerType(expect, triggerType);
  setCreateStreamTriggerTblUid(expect, triggerTblUid);
  setCreateStreamTriggerTableType(expect, triggerTblType);
  setCreateStreamTriggerTableSuid(expect, triggerTblSuid);
  setCreateStreamTriggerTblVgid(expect, triggerTblVgid);
}

// out table part
void setCreateStreamOutName(SCMCreateStreamReq *expect, const char* pOutDb, const char* pOutTbleName) {
   expect->outDB = taosStrdup(pOutDb);
   expect->outTblName = taosStrdup(pOutTbleName);
}

void setCreateStreamOutTblType(SCMCreateStreamReq *expect, int8_t outTblType) {
  expect->outTblType = outTblType;
}

void setCreateStreamOutStbExists(SCMCreateStreamReq *expect, int8_t outStbExists) {
  expect->outStbExists = outStbExists;
}

void setCreateStreamOutStbUid(SCMCreateStreamReq *expect, uint64_t outStbUid) {
  expect->outStbUid = outStbUid;
}

void setCreateStreamOutStbSversion(SCMCreateStreamReq *expect, int32_t outStbSversion) {
  expect->outStbSversion = outStbSversion;
}

void setCreateStreamOutTbleVgid(SCMCreateStreamReq *expect, int32_t outTblVgid) {
   expect->outTblVgId = outTblVgid;
}

void setCreateStreamOutTable(SCMCreateStreamReq *expect, const char* pOutDb, const char* pOutTbleName, int8_t outTblType,
                            int8_t outStbExists, uint64_t outStbUid, int32_t outStbSversion, int32_t outTblVgid) {
  setCreateStreamOutName(expect, pOutDb, pOutTbleName);
  setCreateStreamOutTblType(expect, outTblType);
  setCreateStreamOutStbExists(expect, outStbExists);
  setCreateStreamOutStbUid(expect, outStbUid);
  setCreateStreamOutStbSversion(expect, outStbSversion);
  setCreateStreamOutTbleVgid(expect, outTblVgid);
}


void setCreateStreamCalcDb(SCMCreateStreamReq *expect, const char* pCalcDb) {
  if (expect->calcDB != nullptr) {
   taosArrayClearP(expect->calcDB, nullptr);
   taosMemoryFreeClear(expect->calcDB);
  }
  expect->calcDB = taosArrayInit(1, POINTER_BYTES);
  char *tmpCalcDb = taosStrdup(pCalcDb);
  ASSERT_TRUE(taosArrayPush(expect->calcDB, &tmpCalcDb));
}

void setCreateStreamSql(SCMCreateStreamReq *expect, const char* pSql) {
  expect->sql = taosStrdup(pSql);
}

void setCreateStreamIgExists(SCMCreateStreamReq *expect, int8_t igExists) {
  expect->igExists = igExists;
}

void setCreateStreamReq(SCMCreateStreamReq *expect, const char* pStream, const char* pStreamDb, const char* pCalcDb,
                       const char* pSql, int8_t igExists) {
  setCreateStreamStreamName(expect, pStream, pStreamDb);
  setCreateStreamCalcDb(expect, pCalcDb);
  setCreateStreamSql(expect, pSql);
  setCreateStreamIgExists(expect, igExists);
}

void setCreateStreamTriggerScanPlan(SCMCreateStreamReq *expect, const char* pTriggerScanPlan) {
  expect->triggerScanPlan = taosStrdup(pTriggerScanPlan);
}

void setCreateStreamQueryCalcPlan(SCMCreateStreamReq *expect, const char* pStreamQueryCalcPlan) {
  expect->calcPlan = taosStrdup(pStreamQueryCalcPlan);
}

void setCreateStreamSubTblNameExpr(SCMCreateStreamReq *expect, const char* pSubTblNameExpr) {
  expect->subTblNameExpr = taosStrdup(pSubTblNameExpr);
}

void setCreateStreamTagValueExpr(SCMCreateStreamReq *expect, const char* pTagValueExpr) {
  expect->tagValueExpr = taosStrdup(pTagValueExpr);
}

void setCreateStreamIgDisorder(SCMCreateStreamReq *expect, int8_t igDisorder) {
  expect->igDisorder = igDisorder;
}

void setCreateStreamDeleteReCalc(SCMCreateStreamReq *expect, int8_t deleteReCalc) {
  expect->deleteReCalc = deleteReCalc;
}

void setCreateStreamDeleteOutTbl(SCMCreateStreamReq *expect, int8_t deleteOutTbl) {
  expect->deleteOutTbl = deleteOutTbl;
}

void setCreateStreamFillHistory(SCMCreateStreamReq *expect, int8_t fillHistory) {
  expect->fillHistory = fillHistory;
}

void setCreateStreamFillHistoryFirst(SCMCreateStreamReq *expect, int8_t fillHistoryFirst) {
  expect->fillHistoryFirst = fillHistoryFirst;
}

void setCreateStreamCalcNotifyOnly(SCMCreateStreamReq *expect, int8_t calcNotifyOnly) {
  expect->calcNotifyOnly = calcNotifyOnly;
}

void setCreateStreamLowLatencyCalc(SCMCreateStreamReq *expect, int8_t lowLatencyCalc) {
  expect->lowLatencyCalc = lowLatencyCalc;
}

void setCreateStreamMaxDelay(SCMCreateStreamReq *expect, int64_t maxDelay) {
  expect->maxDelay = maxDelay;
}

void setCreateStreamFillHistoryStartTime(SCMCreateStreamReq *expect, int64_t fillHistoryStartTime) {
  expect->fillHistoryStartTime = fillHistoryStartTime;
}

void setCreateStreamWatermark(SCMCreateStreamReq *expect, int64_t watermark) {
  expect->watermark = watermark;
}

void setCreateStreamExpiredTime(SCMCreateStreamReq *expect, int64_t expiredTime) {
  expect->expiredTime = expiredTime;
}

void setCreateStreamEventTypes(SCMCreateStreamReq *expect, int64_t eventTypes) {
  expect->eventTypes = eventTypes;
}

void setCreateStreamFlags(SCMCreateStreamReq *expect, int64_t flags) {
  expect->flags = flags;
}

void setCreateStreamTsmaId(SCMCreateStreamReq *expect, int64_t tsmaId) {
  expect->tsmaId = tsmaId;
}

void setCreateStreamPlaceHolderBitmap(SCMCreateStreamReq *expect, int64_t placeHolderBitmap) {
  expect->placeHolderBitmap = placeHolderBitmap;
}

void setCreateStreamTsSlotId(SCMCreateStreamReq *expect, int16_t tsSlotId) {
  expect->tsSlotId = tsSlotId;
}

void setCreateStreamOption(SCMCreateStreamReq *expect, int8_t igExists, int8_t igDisorder, int8_t deleteReCalc,
                          int8_t deleteOutTbl, int8_t fillHistory, int8_t fillHistoryFirst, int8_t calcNotifyOnly, int8_t lowLatencyCalc,
                          int64_t maxDelay, int64_t fillHistoryStartTime, int64_t watermark, int64_t expiredTime,
                          int64_t eventTypes, int64_t flags, int64_t tsmaId, int64_t placeHolderBitmap, int16_t tsSlotId){
  expect->igExists = igExists;
  setCreateStreamIgDisorder(expect, igDisorder);
  setCreateStreamDeleteReCalc(expect, deleteReCalc);
  setCreateStreamDeleteOutTbl(expect, deleteOutTbl);
  setCreateStreamFillHistory(expect, fillHistory);
  setCreateStreamFillHistoryFirst(expect, fillHistoryFirst);
  setCreateStreamCalcNotifyOnly(expect, calcNotifyOnly);
  setCreateStreamLowLatencyCalc(expect, lowLatencyCalc);
  setCreateStreamMaxDelay(expect, maxDelay);
  setCreateStreamFillHistoryStartTime(expect, fillHistoryStartTime);
  setCreateStreamWatermark(expect, watermark);
  setCreateStreamExpiredTime(expect, expiredTime);
  setCreateStreamEventTypes(expect, eventTypes);
  setCreateStreamFlags(expect, flags);
  setCreateStreamTsmaId(expect, tsmaId);
  setCreateStreamPlaceHolderBitmap(expect, placeHolderBitmap);
  setCreateStreamTsSlotId(expect, tsSlotId);
}


void addCreateStreamOutCols(SCMCreateStreamReq *expect, const char* name, uint8_t type, int8_t flag, int32_t bytes, uint32_t compress, STypeMod typeMod) {
  SFieldWithOptions outCol = {0};
  strcpy(outCol.name, name);
  outCol.type = type;
  outCol.bytes = bytes;
  outCol.compress = compress;
  outCol.typeMod = typeMod;
  outCol.flags = flag;
  if (expect->outCols == nullptr) {
    expect->outCols = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SFieldWithOptions));
    ASSERT_TRUE(expect->outCols != nullptr);
  }
  ASSERT_TRUE(taosArrayPush(expect->outCols, &outCol) != nullptr);
}

void addCreateStreamOutTags(SCMCreateStreamReq *expect, const char* name, uint8_t type, int8_t flag, int32_t bytes, uint32_t compress, STypeMod typeMod) {
  SFieldWithOptions outTags = {0};
  strcpy(outTags.name, name);
  outTags.type = type;
  outTags.bytes = bytes;
  outTags.compress = compress;
  outTags.typeMod = typeMod;
  outTags.flags = flag;
  if (expect->outTags == nullptr) {
    expect->outTags = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SFieldWithOptions));
    ASSERT_TRUE(expect->outTags != nullptr);
  }
  ASSERT_TRUE(taosArrayPush(expect->outTags, &outTags) != nullptr);
}

void setCreateStreamTriggerCols(SCMCreateStreamReq *expect, const char* pCols) {
  expect->triggerCols = taosStrdup(pCols);
}

void resetCreateStreamTriggerCols(SCMCreateStreamReq *expect) {
  taosMemoryFree(expect->triggerCols);
  expect->triggerCols = nullptr;
}

void setCreateStreamPartitionCols(SCMCreateStreamReq *expect, const char* pCols) {
  expect->partitionCols = taosStrdup(pCols);
}

typedef enum EWindowType {
  WINDOW_TYPE_INTERVAL = 1,
  WINDOW_TYPE_SESSION,
  WINDOW_TYPE_STATE,
  WINDOW_TYPE_EVENT,
  WINDOW_TYPE_COUNT,
  WINDOW_TYPE_ANOMALY,
  WINDOW_TYPE_EXTERNAL,
  WINDOW_TYPE_PERIOD
} EWindowType;

void setCreateStreamTriggerSliding(SCMCreateStreamReq *expect, int8_t intervalUnit, int8_t slidingUnit,
  int8_t offsetUnit, int8_t soffsetUnit, int8_t precision, int64_t interval, int64_t offset, int64_t sliding,
  int64_t soffset) {
  expect->trigger.sliding.intervalUnit = intervalUnit;
  expect->trigger.sliding.slidingUnit = slidingUnit;
  expect->trigger.sliding.offsetUnit = offsetUnit;
  expect->trigger.sliding.soffsetUnit = soffsetUnit;
  expect->trigger.sliding.precision = precision;
  expect->trigger.sliding.interval = interval;
  expect->trigger.sliding.offset = offset;
  expect->trigger.sliding.sliding = sliding;
  expect->trigger.sliding.soffset = soffset;
}

void setCreateStreamTriggerSession(SCMCreateStreamReq *expect,int64_t pSessionVal, int16_t slotId) {
  expect->trigger.session.sessionVal = pSessionVal;
  expect->trigger.session.slotId = slotId;
}

void setCreateStreamTriggerCount(SCMCreateStreamReq *expect, int64_t countVal, int64_t sliding) {
  expect->trigger.count.countVal = countVal;
  expect->trigger.count.sliding = sliding;
}

void setCreateStreamTriggerEvent(SCMCreateStreamReq *expect, const char* startCond, const char* endCond, int64_t trueForDuration) {
  expect->trigger.event.startCond = taosStrdup(startCond);
  expect->trigger.event.endCond = taosStrdup(endCond);
  expect->trigger.event.trueForDuration = trueForDuration;
}

void setCreateStreamTriggerState(SCMCreateStreamReq *expect, int16_t slotId, int64_t trueForDuration) {
  expect->trigger.stateWin.slotId = slotId;
  expect->trigger.stateWin.trueForDuration = trueForDuration;
}

void setCreateStreamTriggerPeriod(SCMCreateStreamReq *expect, int8_t offsetUnit, int8_t periodUnit, int8_t precision, int64_t offset, int64_t period) {
  expect->trigger.period.offsetUnit = offsetUnit;
  expect->trigger.period.periodUnit = periodUnit;
  expect->trigger.period.precision = precision;
  expect->trigger.period.offset = offset;
  expect->trigger.period.period = period;
}

void addCreateStreamNotifyUrl(SCMCreateStreamReq *expect, const char* pUrl) {
  if (nullptr == expect->pNotifyAddrUrls) {
    expect->pNotifyAddrUrls = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
    ASSERT_TRUE(expect->pNotifyAddrUrls != nullptr);
  }
  char *tmpUrl = taosStrdup(pUrl);
  ASSERT_TRUE(taosArrayPush(expect->pNotifyAddrUrls, &tmpUrl));
};

void setCreateStreamNotify(SCMCreateStreamReq *expect, int8_t notifyEventTypes, int8_t notifyErrorHandle, int8_t notifyHistory) {
  expect->notifyEventTypes = notifyEventTypes;
  expect->notifyErrorHandle = notifyErrorHandle;
  expect->notifyHistory = notifyHistory;
}

void setCreateStreamForceOutputCols(SCMCreateStreamReq *expect, uint8_t type, uint8_t precision, uint8_t scale, int32_t bytes, const char* expr) {
  SStreamOutCol outCol = {0};
  outCol.type.type = type;
  outCol.type.precision = precision;
  outCol.type.scale = scale;
  outCol.type.bytes = bytes;
  outCol.expr = taosStrdup(expr);
  if (expect->outCols == nullptr) {
    expect->outCols = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SStreamOutCol));
    ASSERT_TRUE(expect->outCols != nullptr);
  }
  ASSERT_TRUE(taosArrayPush(expect->outCols, &outCol) != nullptr);
}

void resetCreateStreamQueryScanPlan(SCMCreateStreamReq *expect) {
  taosArrayDestroyP(expect->calcScanPlanList, nullptr);
  expect->calcScanPlanList = nullptr;
}

void addCreateStreamQueryScanPlan(SCMCreateStreamReq *expect, bool readFromCache, const char* pStreamQueryScanPlan) {
  SStreamCalcScan scan = {0};
  scan.readFromCache = (int8_t)readFromCache;
  scan.scanPlan = taosStrdup(pStreamQueryScanPlan);
  if (expect->calcScanPlanList == nullptr) {
    expect->calcScanPlanList = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SStreamCalcScan));
    ASSERT_TRUE(expect->calcScanPlanList != nullptr);
  }
  ASSERT_TRUE(taosArrayPush(expect->calcScanPlanList, &scan) != nullptr);
}

void delete_all_name_fields(cJSON* node) {
  if (!node) return;

  if (cJSON_IsObject(node)) {
    cJSON* item = node->child;
    cJSON* next = NULL;

    while (item) {
      next = item->next;

      if (strcmp(item->string, "Name") == 0) {
        cJSON_DeleteItemFromObject(node, "Name");
      } else {
        delete_all_name_fields(item);
      }

      item = next;
    }

  } else if (cJSON_IsArray(node)) {
    int32_t size = cJSON_GetArraySize(node);
    for (int i = 0; i < size; ++i) {
      delete_all_name_fields(cJSON_GetArrayItem(node, i));
    }
  }
}

void checkCreateStreamCalcPlan(SCMCreateStreamReq *expect, SCMCreateStreamReq *req) {
  cJSON* j1 = cJSON_Parse((char*)expect->calcPlan);
  cJSON* j2 = cJSON_Parse((char*)req->calcPlan);

  if (!j1 || !j2) {
    printf("Invalid JSON input.\n");
    ASSERT_TRUE(false);
  }

  // Since projection's slot name is not fixed, we need to delete all "Name" fields
  delete_all_name_fields(j1);
  delete_all_name_fields(j2);

  char *j1Str = cJSON_PrintUnformatted(j1);
  char *j2Str = cJSON_PrintUnformatted(j2);

  ASSERT_EQ(std::string(j1Str), std::string(j2Str));

  cJSON_Delete(j1);
  cJSON_Delete(j2);
}

void checkCreateStreamReq(SCMCreateStreamReq *expect, SCMCreateStreamReq *req) {
  // stream name part
  cout << "checkCreateStreamReq: " << expect->sql << endl;
  ASSERT_NE(req->streamId, 0);
  ASSERT_EQ(std::string(req->name), std::string(expect->name));
  ASSERT_EQ(std::string(req->sql), std::string(expect->sql));
  ASSERT_EQ(std::string(req->streamDB), std::string(expect->streamDB));
  ASSERT_EQ(std::string(req->triggerDB), std::string(expect->triggerDB));
  ASSERT_EQ(std::string(req->outDB), std::string(expect->outDB));
  ASSERT_EQ(std::string(req->triggerTblName), std::string(expect->triggerTblName));
  ASSERT_EQ(std::string(req->outTblName), std::string(expect->outTblName));
  ASSERT_EQ(taosArrayGetSize(req->calcDB), taosArrayGetSize(expect->calcDB));
  for (int32_t i = 0; i < taosArrayGetSize(req->calcDB); i++) {
    char *pCalcDb = (char*)taosArrayGetP(req->calcDB, i);
    char *pExpectCalcDb = (char*)taosArrayGetP(expect->calcDB, i);
    ASSERT_EQ(std::string(pCalcDb), std::string(pExpectCalcDb));
  }

  // stream trigger option part
  ASSERT_EQ(req->igExists, expect->igExists);
  ASSERT_EQ(req->triggerType, expect->triggerType);
  ASSERT_EQ(req->igDisorder, expect->igDisorder);
  ASSERT_EQ(req->deleteReCalc, expect->deleteReCalc);
  ASSERT_EQ(req->deleteOutTbl, expect->deleteOutTbl);
  ASSERT_EQ(req->fillHistory, expect->fillHistory);
  ASSERT_EQ(req->fillHistoryFirst, expect->fillHistoryFirst);
  ASSERT_EQ(req->calcNotifyOnly, expect->calcNotifyOnly);
  ASSERT_EQ(req->lowLatencyCalc, expect->lowLatencyCalc);

  // stream notify part
  ASSERT_EQ(taosArrayGetSize(req->pNotifyAddrUrls), taosArrayGetSize(expect->pNotifyAddrUrls));
  for (int32_t i = 0; i < taosArrayGetSize(req->pNotifyAddrUrls); i++) {
    char *pUrl = (char*)taosArrayGetP(req->pNotifyAddrUrls, i);
    char *pExpectUrl = (char*)taosArrayGetP(expect->pNotifyAddrUrls, i);
    ASSERT_EQ(std::string(pUrl), std::string(pExpectUrl));
  }
  ASSERT_EQ(req->notifyEventTypes, expect->notifyEventTypes);
  ASSERT_EQ(req->notifyErrorHandle, expect->notifyErrorHandle);
  ASSERT_EQ(req->notifyHistory, expect->notifyHistory);

  if (req->triggerCols == nullptr) {
    ASSERT_TRUE(expect->triggerCols == nullptr);
  } else {
    ASSERT_EQ(std::string((char*)req->triggerCols), std::string((char*)expect->triggerCols));
  }

  if (req->partitionCols == nullptr) {
    ASSERT_TRUE(expect->partitionCols == nullptr);
  } else {
    ASSERT_EQ(std::string((char*)req->partitionCols), std::string((char*)expect->partitionCols));
  }

  ASSERT_EQ(taosArrayGetSize(req->outCols), taosArrayGetSize(expect->outCols));
  for (int32_t i = 0; i < taosArrayGetSize(req->outCols); i++) {
    auto pOutCols = (SFieldWithOptions *)taosArrayGet(req->outCols, i);
    auto expectOutCols = (SFieldWithOptions *)taosArrayGet(expect->outCols, i);
    ASSERT_EQ(pOutCols->type, expectOutCols->type);
    ASSERT_EQ(pOutCols->bytes, expectOutCols->bytes);
    ASSERT_EQ(pOutCols->compress, expectOutCols->compress);
    ASSERT_EQ(pOutCols->flags, expectOutCols->flags);
    ASSERT_EQ(pOutCols->typeMod, expectOutCols->typeMod);
    ASSERT_EQ(std::string(pOutCols->name), std::string(expectOutCols->name));
  }

  ASSERT_EQ(taosArrayGetSize(req->outTags), taosArrayGetSize(expect->outTags));
  for (int32_t i = 0; i < taosArrayGetSize(req->outTags); i++) {
    auto pOutTags = (SFieldWithOptions *)taosArrayGet(req->outTags, i);
    auto expectOutTags = (SFieldWithOptions *)taosArrayGet(expect->outTags, i);
    ASSERT_EQ(pOutTags->type, expectOutTags->type);
    ASSERT_EQ(pOutTags->bytes, expectOutTags->bytes);
    ASSERT_EQ(pOutTags->compress, expectOutTags->compress);
    ASSERT_EQ(pOutTags->flags, expectOutTags->flags);
    ASSERT_EQ(pOutTags->typeMod, expectOutTags->typeMod);
    ASSERT_EQ(std::string(pOutTags->name), std::string(expectOutTags->name));
  }

  ASSERT_EQ(req->maxDelay, expect->maxDelay);
  ASSERT_EQ(req->fillHistoryStartTime, expect->fillHistoryStartTime);
  ASSERT_EQ(req->watermark, expect->watermark);
  ASSERT_EQ(req->expiredTime, expect->expiredTime);

  switch (req->triggerType) {
    case WINDOW_TYPE_INTERVAL: {
      ASSERT_EQ(req->trigger.sliding.sliding, expect->trigger.sliding.sliding);
      ASSERT_EQ(req->trigger.sliding.interval, expect->trigger.sliding.interval);
      ASSERT_EQ(req->trigger.sliding.offset, expect->trigger.sliding.offset);
      ASSERT_EQ(req->trigger.sliding.soffset, expect->trigger.sliding.soffset);
      ASSERT_EQ(req->trigger.sliding.intervalUnit, expect->trigger.sliding.intervalUnit);
      ASSERT_EQ(req->trigger.sliding.slidingUnit, expect->trigger.sliding.slidingUnit);
      ASSERT_EQ(req->trigger.sliding.offsetUnit, expect->trigger.sliding.offsetUnit);
      ASSERT_EQ(req->trigger.sliding.soffsetUnit, expect->trigger.sliding.soffsetUnit);
      ASSERT_EQ(req->trigger.sliding.precision, expect->trigger.sliding.precision);
      break;
    }
    case WINDOW_TYPE_SESSION: {
      ASSERT_EQ(req->trigger.session.sessionVal, expect->trigger.session.sessionVal);
      ASSERT_EQ(req->trigger.session.slotId, expect->trigger.session.slotId);
      break;
    }
    case WINDOW_TYPE_STATE: {
      ASSERT_EQ(req->trigger.stateWin.trueForDuration, expect->trigger.stateWin.trueForDuration);
      ASSERT_EQ(req->trigger.stateWin.slotId, expect->trigger.stateWin.slotId);
      break;
    }
    case WINDOW_TYPE_EVENT: {
      ASSERT_EQ(std::string((char*)req->trigger.event.startCond), std::string((char*)expect->trigger.event.startCond));
      ASSERT_EQ(std::string((char*)req->trigger.event.endCond), std::string((char*)expect->trigger.event.endCond));
      ASSERT_EQ(req->trigger.event.trueForDuration, expect->trigger.event.trueForDuration);
      break;
    }
    case WINDOW_TYPE_COUNT: {
      ASSERT_EQ(req->trigger.count.sliding, expect->trigger.count.sliding);
      ASSERT_EQ(req->trigger.count.countVal, expect->trigger.count.countVal);
      break;
    }
    case WINDOW_TYPE_PERIOD: {
      ASSERT_EQ(req->trigger.period.offset, expect->trigger.period.offset);
      ASSERT_EQ(req->trigger.period.offsetUnit, expect->trigger.period.offsetUnit);
      ASSERT_EQ(req->trigger.period.period, expect->trigger.period.period);
      ASSERT_EQ(req->trigger.period.periodUnit, expect->trigger.period.periodUnit);
      ASSERT_EQ(req->trigger.period.precision, expect->trigger.period.precision);
      break;
    }
    default:
      ASSERT_TRUE(false);
  }

  ASSERT_EQ(req->triggerTblType, expect->triggerTblType);
  ASSERT_EQ(req->triggerTblUid, expect->triggerTblUid);
  ASSERT_EQ(req->outTblType, expect->outTblType);
  ASSERT_EQ(req->outStbExists, expect->outStbExists);
  ASSERT_EQ(req->outStbUid, expect->outStbUid);
  ASSERT_EQ(req->outStbSversion, expect->outStbSversion);
  ASSERT_EQ(req->eventTypes, expect->eventTypes);
  ASSERT_EQ(req->flags, expect->flags);
  ASSERT_EQ(req->tsmaId, expect->tsmaId);
  ASSERT_EQ(req->placeHolderBitmap, expect->placeHolderBitmap);
  if (BIT_FLAG_TEST_MASK(req->placeHolderBitmap, PLACE_HOLDER_PARTITION_ROWS)) {
    ASSERT_EQ(req->tsSlotId, expect->tsSlotId);
  }

  ASSERT_EQ(req->triggerTblVgId, expect->triggerTblVgId);
  ASSERT_EQ(req->outTblVgId, expect->outTblVgId);

  ASSERT_EQ(std::string((char*)req->triggerScanPlan), std::string((char*)expect->triggerScanPlan));
  checkCreateStreamCalcPlan(expect, req);

  ASSERT_EQ(taosArrayGetSize(req->calcScanPlanList), taosArrayGetSize(expect->calcScanPlanList));
  for (int32_t i = 0; i < taosArrayGetSize(req->calcScanPlanList); i++) {
    auto pCalcScan = (SStreamCalcScan *)taosArrayGet(req->calcScanPlanList, i);
    auto expectScan = (SStreamCalcScan *)taosArrayGet(expect->calcScanPlanList, i);
    ASSERT_EQ(pCalcScan->readFromCache, expectScan->readFromCache);
    ASSERT_EQ(std::string((char*)pCalcScan->scanPlan), std::string((char*)expectScan->scanPlan));
  }

  if (req->subTblNameExpr == nullptr) {
    ASSERT_TRUE(expect->subTblNameExpr == nullptr);
  } else {
    ASSERT_EQ(std::string((char*)req->subTblNameExpr), std::string((char*)expect->subTblNameExpr));
  }

  if (req->tagValueExpr == nullptr) {
    ASSERT_TRUE(expect->tagValueExpr == nullptr);
  } else {
    ASSERT_EQ(std::string((char*)req->tagValueExpr), std::string((char*)expect->tagValueExpr));
  }

  ASSERT_EQ(taosArrayGetSize(req->forceOutCols), taosArrayGetSize(expect->forceOutCols));
  for (int32_t i = 0; i < taosArrayGetSize(req->forceOutCols); i++) {
    auto pOutCols = (SStreamOutCol *)taosArrayGet(req->forceOutCols, i);
    auto expectOutCols = (SStreamOutCol *)taosArrayGet(expect->forceOutCols, i);
    ASSERT_EQ(pOutCols->type.type, expectOutCols->type.type);
    ASSERT_EQ(pOutCols->type.bytes, expectOutCols->type.bytes);
    ASSERT_EQ(pOutCols->type.precision, expectOutCols->type.precision);
    ASSERT_EQ(pOutCols->type.scale, expectOutCols->type.scale);

    ASSERT_EQ(std::string((char*)pOutCols->expr), std::string((char*)expectOutCols->expr));
  }

}

TEST_F(ParserStreamTest, createStream) {
 useDb("root", "test");

 SCMCreateStreamReq expect = {0};

 auto clearCreateStreamReq = [&]() {
   tFreeSCMCreateStreamReq(&expect);
   memset(&expect, 0, sizeof(SCMCreateStreamReq));
 };

 setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
   ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_STREAM_STMT);
   SCMCreateStreamReq req = {0};
   ASSERT_TRUE(TSDB_CODE_SUCCESS ==
               tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
   checkCreateStreamReq(&expect, &req);
   tFreeSCMCreateStreamReq(&req);
 });

 //setCreateStreamReq(&expect, "0.test.s1", "0.test", "0.test", "0.test", "0.test", "t1", "stream_out", "create stream s1 interval(1s) sliding(1s) from t1 into stream_out as select _twstart, avg(c1) from t2", 0);
 //setCreateStreamOption(&expect, 0, WINDOW_TYPE_INTERVAL, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, TSDB_NORMAL_TABLE, 9, TSDB_NORMAL_TABLE, 0, 0, 1, 1, 0, 0, 8, -1);
 setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1128\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"0\",\"DbFName\":\"0.test\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"304\",\"OutputRowSize\":\"304\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"8280654498900312045\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3557205140367942817\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6164016115312601301\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_4\"}}],\"Precision\":\"0\"}},\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"9\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"9\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"expr_3\",\"UserAlias\":\"c2\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"9\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"3\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_4\",\"UserAlias\":\"tbname\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"9\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"test\",\"TableName\":\"t1\"},\"GroupOrderScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"StreamResInfoStbFullName\":\"\",\"StreamResInfoWstartName\":\"\",\"StreamResInfoWendName\":\"\",\"StreamResInfoGroupIdName\":\"\",\"StreamResInfoIsWindowFilledName\":\"\"}},\"DataSink\":{\"NodeType\":\"1124\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"304\",\"OutputRowSize\":\"304\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false}}");
 addCreateStreamQueryScanPlan(&expect, false, "{\"NodeType\":\"1128\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"2\",\"SubplanId\":\"2\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"1\",\"DbFName\":\"0.test\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3612687029497005528\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"281596734614822715\"}}],\"Precision\":\"0\"}},\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"10\",\"TableType\":\"0\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"\",\"TableName\":\"t2\",\"TableAlias\":\"t2\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"10\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"t2\",\"TableAlias\":\"t2\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}],\"TableId\":\"10\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"test\",\"TableName\":\"t2\"},\"GroupOrderScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"2\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"StreamResInfoStbFullName\":\"\",\"StreamResInfoWstartName\":\"\",\"StreamResInfoWendName\":\"\",\"StreamResInfoGroupIdName\":\"\",\"StreamResInfoIsWindowFilledName\":\"\"}},\"DataSink\":{\"NodeType\":\"1124\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false}}");
 setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1129\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"NodeList\":[{\"NodeType\":\"1128\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}},\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false}}],\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"avg(c1)\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"10\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"t2\",\"TableAlias\":\"t2\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"_twstart\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"178\",\"Type\":\"3524\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"avg(c1)\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"ColName\":\"expr_2\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1124\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false}}]}}}}");
 setCreateStreamSubTblNameExpr(&expect, "{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"59\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"concat\",\"Id\":\"70\",\"Type\":\"1502\",\"Parameters\":[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"34\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"md5\",\"Id\":\"134\",\"Type\":\"1509\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"9\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"7\",\"Literal\":\"test.s1\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"test.s1\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"3\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"1\",\"Literal\":\"_\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"_\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"22\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"cast\",\"Id\":\"77\",\"Type\":\"2000\",\"Parameters\":[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_tgrpid\",\"Id\":\"185\",\"Type\":\"3531\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"2\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}");
 setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"9\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}]");
 addCreateStreamOutCols(&expect, "_twstart", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, {0});
 addCreateStreamOutCols(&expect, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, {0});
 setCreateStreamTriggerSliding(&expect, 's', 's', 0, 0, 0, 1000, 0, 1000, 0);
 run("create stream s1 interval(1s) sliding(1s) from t1 into stream_out as select _twstart, avg(c1) from t2");
 clearCreateStreamReq();
}

TEST_F(ParserStreamTest, TestName) {
  useDb("root", "stream_streamdb");
  SCMCreateStreamReq expect = {0};

  auto clearCreateStreamReq = [&]() {
    tFreeSCMCreateStreamReq(&expect);
    memset(&expect, 0, sizeof(SCMCreateStreamReq));
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_STREAM_STMT);
    SCMCreateStreamReq req = {0};
    ASSERT_TRUE(TSDB_CODE_SUCCESS ==
                tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    checkCreateStreamReq(&expect, &req);
    tFreeSCMCreateStreamReq(&req);
  });

  setCreateStreamTriggerTable(&expect, "0.stream_triggerdb", "t1", WINDOW_TYPE_INTERVAL, TSDB_NORMAL_TABLE, 49, 0, 1);

  setCreateStreamOutTable(&expect, "0.stream_outdb", "stream_out", TSDB_NORMAL_TABLE, 0, 0, 1, 1);

  setCreateStreamReq(&expect, "0.stream_streamdb.s1", "0.stream_streamdb", "0.stream_querydb", "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2", 0);

  setCreateStreamOption(&expect, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, EVENT_WINDOW_CLOSE, 0, 0, 8, -1);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1128\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"0\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"304\",\"OutputRowSize\":\"304\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"8280654498900312045\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3557205140367942817\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6164016115312601301\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_4\"}}],\"Precision\":\"0\"}},\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"expr_3\",\"UserAlias\":\"c2\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"3\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_4\",\"UserAlias\":\"tbname\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"49\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\"},\"GroupOrderScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"StreamResInfoStbFullName\":\"\",\"StreamResInfoWstartName\":\"\",\"StreamResInfoWendName\":\"\",\"StreamResInfoGroupIdName\":\"\",\"StreamResInfoIsWindowFilledName\":\"\"}},\"DataSink\":{\"NodeType\":\"1124\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"304\",\"OutputRowSize\":\"304\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false}}");
  addCreateStreamQueryScanPlan(&expect, false, "{\"NodeType\":\"1128\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"2\",\"SubplanId\":\"2\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"1\",\"DbFName\":\"0.stream_querydb\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3612687029497005528\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"281596734614822715\"}}],\"Precision\":\"0\"}},\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"0\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"\",\"TableName\":\"t2\",\"TableAlias\":\"t2\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"t2\",\"TableAlias\":\"t2\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}],\"TableId\":\"30\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_querydb\",\"TableName\":\"t2\"},\"GroupOrderScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"2\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"StreamResInfoStbFullName\":\"\",\"StreamResInfoWstartName\":\"\",\"StreamResInfoWendName\":\"\",\"StreamResInfoGroupIdName\":\"\",\"StreamResInfoIsWindowFilledName\":\"\"}},\"DataSink\":{\"NodeType\":\"1124\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false}}");
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1129\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"NodeList\":[{\"NodeType\":\"1128\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}},\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false}}],\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"avg(c1)\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"t2\",\"TableAlias\":\"t2\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"_twstart\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"178\",\"Type\":\"3524\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"avg(c1)\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"ColName\":\"expr_2\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1124\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false}}]}}}}");
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}]");
  addCreateStreamOutCols(&expect, "_twstart", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, {0});
  addCreateStreamOutCols(&expect, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, {0});
  setCreateStreamTriggerSliding(&expect, 's', 's', 0, 0, 0, 1000, 0, 1000, 0);

  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");

  // stream db name
  setCreateStreamStreamName(&expect, "0.stream_streamdb_2.s1", "0.stream_streamdb_2");
  setCreateStreamSql(&expect, "create stream stream_streamdb_2.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb_2.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");

  // stream name
  setCreateStreamStreamName(&expect, "0.stream_streamdb.s1_2", "0.stream_streamdb");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1_2 interval(1s) sliding(1s) from stream_triggerdb.t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1_2 interval(1s) sliding(1s) from stream_triggerdb.t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");

  // trigger db name
  setCreateStreamTriggerTable(&expect, "0.stream_triggerdb_2", "t1", WINDOW_TYPE_INTERVAL, TSDB_NORMAL_TABLE, 59, 0, 1);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1_2 interval(1s) sliding(1s) from stream_triggerdb_2.t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"59\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb_2\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}]");
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1128\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"0\",\"DbFName\":\"0.stream_triggerdb_2\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"304\",\"OutputRowSize\":\"304\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"8280654498900312045\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3557205140367942817\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6164016115312601301\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_4\"}}],\"Precision\":\"0\"}},\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"59\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb_2\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"59\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb_2\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"expr_3\",\"UserAlias\":\"c2\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"59\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb_2\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"3\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_4\",\"UserAlias\":\"tbname\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"59\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb_2\",\"TableName\":\"t1\"},\"GroupOrderScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"StreamResInfoStbFullName\":\"\",\"StreamResInfoWstartName\":\"\",\"StreamResInfoWendName\":\"\",\"StreamResInfoGroupIdName\":\"\",\"StreamResInfoIsWindowFilledName\":\"\"}},\"DataSink\":{\"NodeType\":\"1124\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"304\",\"OutputRowSize\":\"304\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false}}");
  run("create stream stream_streamdb.s1_2 interval(1s) sliding(1s) from stream_triggerdb_2.t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");

  // trigger table name
  setCreateStreamTriggerTable(&expect, "0.stream_triggerdb_2", "t2", WINDOW_TYPE_INTERVAL, TSDB_NORMAL_TABLE, 60, 0, 1);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1_2 interval(1s) sliding(1s) from stream_triggerdb_2.t2 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"59\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb_2\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}]");
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"60\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb_2\",\"TableName\":\"t2\",\"TableAlias\":\"t2\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}]");
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1128\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"0\",\"DbFName\":\"0.stream_triggerdb_2\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"312\",\"OutputRowSize\":\"312\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"281596734614822715\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3612687029497005528\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"7068875265887176256\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"15470507899942139204\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"4\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_5\"}}],\"Precision\":\"0\"}},\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"60\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb_2\",\"TableName\":\"t2\",\"TableAlias\":\"t2\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"60\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb_2\",\"TableName\":\"t2\",\"TableAlias\":\"t2\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"expr_3\",\"UserAlias\":\"c2\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"60\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb_2\",\"TableName\":\"t2\",\"TableAlias\":\"t2\",\"ColName\":\"c2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"3\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_4\",\"UserAlias\":\"c3\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"60\",\"TableType\":\"3\",\"ColId\":\"4\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb_2\",\"TableName\":\"t2\",\"TableAlias\":\"t2\",\"ColName\":\"c3\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"4\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_5\",\"UserAlias\":\"tbname\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"60\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb_2\",\"TableName\":\"t2\"},\"GroupOrderScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"StreamResInfoStbFullName\":\"\",\"StreamResInfoWstartName\":\"\",\"StreamResInfoWendName\":\"\",\"StreamResInfoGroupIdName\":\"\",\"StreamResInfoIsWindowFilledName\":\"\"}},\"DataSink\":{\"NodeType\":\"1124\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"312\",\"OutputRowSize\":\"312\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"4\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false}}");
  run("create stream stream_streamdb.s1_2 interval(1s) sliding(1s) from stream_triggerdb_2.t2 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");

  // out db name
  setCreateStreamOutName(&expect, "0.stream_outdb_2", "stream_out");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1_2 interval(1s) sliding(1s) from stream_triggerdb_2.t2 into stream_outdb_2.stream_out as select _twstart, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1_2 interval(1s) sliding(1s) from stream_triggerdb_2.t2 into stream_outdb_2.stream_out as select _twstart, avg(c1) from stream_querydb.t2");

  // out table name
  setCreateStreamOutName(&expect, "0.stream_outdb_2", "stream_out");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1_2 interval(1s) sliding(1s) from stream_triggerdb_2.t2 into stream_outdb_2.stream_out as select _twstart, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1_2 interval(1s) sliding(1s) from stream_triggerdb_2.t2 into stream_outdb_2.stream_out as select _twstart, avg(c1) from stream_querydb.t2");

  // query db name
  setCreateStreamCalcDb(&expect, "0.stream_querydb_2");
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1129\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"NodeList\":[{\"NodeType\":\"1128\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}},\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false}}],\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"avg(c1)\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"40\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb_2\",\"TableName\":\"t2\",\"TableAlias\":\"t2\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"_twstart\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"178\",\"Type\":\"3524\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"avg(c1)\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"ColName\":\"expr_2\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1124\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false}}]}}}}");
  resetCreateStreamQueryScanPlan(&expect);
  addCreateStreamQueryScanPlan(&expect, false, "{\"NodeType\":\"1128\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"2\",\"SubplanId\":\"2\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"1\",\"DbFName\":\"0.stream_querydb_2\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3612687029497005528\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"281596734614822715\"}}],\"Precision\":\"0\"}},\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"40\",\"TableType\":\"0\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"\",\"TableName\":\"t2\",\"TableAlias\":\"t2\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"40\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb_2\",\"TableName\":\"t2\",\"TableAlias\":\"t2\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}],\"TableId\":\"40\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_querydb_2\",\"TableName\":\"t2\"},\"GroupOrderScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"2\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"StreamResInfoStbFullName\":\"\",\"StreamResInfoWstartName\":\"\",\"StreamResInfoWendName\":\"\",\"StreamResInfoGroupIdName\":\"\",\"StreamResInfoIsWindowFilledName\":\"\"}},\"DataSink\":{\"NodeType\":\"1124\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false}}");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1_2 interval(1s) sliding(1s) from stream_triggerdb_2.t2 into stream_outdb_2.stream_out as select _twstart, avg(c1) from stream_querydb_2.t2");
  run("create stream stream_streamdb.s1_2 interval(1s) sliding(1s) from stream_triggerdb_2.t2 into stream_outdb_2.stream_out as select _twstart, avg(c1) from stream_querydb_2.t2");

  // query table name
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1129\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"NodeList\":[{\"NodeType\":\"1128\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}},\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false}}],\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"avg(c1)\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"39\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb_2\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"_twstart\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"178\",\"Type\":\"3524\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"avg(c1)\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"ColName\":\"expr_2\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1124\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false}}]}}}}");
  resetCreateStreamQueryScanPlan(&expect);
  addCreateStreamQueryScanPlan(&expect, false, "{\"NodeType\":\"1128\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"2\",\"SubplanId\":\"2\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"1\",\"DbFName\":\"0.stream_querydb_2\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3557205140367942817\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"8280654498900312045\"}}],\"Precision\":\"0\"}},\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"39\",\"TableType\":\"0\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"39\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb_2\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}],\"TableId\":\"39\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_querydb_2\",\"TableName\":\"t1\"},\"GroupOrderScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"2\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"StreamResInfoStbFullName\":\"\",\"StreamResInfoWstartName\":\"\",\"StreamResInfoWendName\":\"\",\"StreamResInfoGroupIdName\":\"\",\"StreamResInfoIsWindowFilledName\":\"\"}},\"DataSink\":{\"NodeType\":\"1124\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false}}");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1_2 interval(1s) sliding(1s) from stream_triggerdb_2.t2 into stream_outdb_2.stream_out as select _twstart, avg(c1) from stream_querydb_2.t1");
  run("create stream stream_streamdb.s1_2 interval(1s) sliding(1s) from stream_triggerdb_2.t2 into stream_outdb_2.stream_out as select _twstart, avg(c1) from stream_querydb_2.t1");

  clearCreateStreamReq();
}

TEST_F(ParserStreamTest, TestTriggerOption) {
  useDb("root", "stream_streamdb");
  SCMCreateStreamReq expect = {0};

  auto clearCreateStreamReq = [&]() {
    tFreeSCMCreateStreamReq(&expect);
    memset(&expect, 0, sizeof(SCMCreateStreamReq));
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_STREAM_STMT);
    SCMCreateStreamReq req = {0};
    ASSERT_TRUE(TSDB_CODE_SUCCESS ==
               tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    checkCreateStreamReq(&expect, &req);
    tFreeSCMCreateStreamReq(&req);
 });

  setCreateStreamTriggerTable(&expect, "0.stream_triggerdb", "t1", WINDOW_TYPE_INTERVAL, TSDB_NORMAL_TABLE, 49, 0, 1);

  setCreateStreamOutTable(&expect, "0.stream_outdb", "stream_out", TSDB_NORMAL_TABLE, 0, 0, 1, 1);

  setCreateStreamReq(&expect, "0.stream_streamdb.s1", "0.stream_streamdb", "0.stream_querydb", "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2", 0);

  setCreateStreamOption(&expect, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, EVENT_WINDOW_CLOSE, 0, 0, 8, -1);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1128\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"0\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"304\",\"OutputRowSize\":\"304\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"8280654498900312045\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3557205140367942817\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6164016115312601301\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_4\"}}],\"Precision\":\"0\"}},\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"expr_3\",\"UserAlias\":\"c2\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"3\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_4\",\"UserAlias\":\"tbname\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"49\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\"},\"GroupOrderScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"StreamResInfoStbFullName\":\"\",\"StreamResInfoWstartName\":\"\",\"StreamResInfoWendName\":\"\",\"StreamResInfoGroupIdName\":\"\",\"StreamResInfoIsWindowFilledName\":\"\"}},\"DataSink\":{\"NodeType\":\"1124\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"304\",\"OutputRowSize\":\"304\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false}}");
  addCreateStreamQueryScanPlan(&expect, false, "{\"NodeType\":\"1128\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"2\",\"SubplanId\":\"2\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"1\",\"DbFName\":\"0.stream_querydb\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3612687029497005528\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"281596734614822715\"}}],\"Precision\":\"0\"}},\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"0\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"\",\"TableName\":\"t2\",\"TableAlias\":\"t2\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"t2\",\"TableAlias\":\"t2\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}],\"TableId\":\"30\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_querydb\",\"TableName\":\"t2\"},\"GroupOrderScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"2\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"StreamResInfoStbFullName\":\"\",\"StreamResInfoWstartName\":\"\",\"StreamResInfoWendName\":\"\",\"StreamResInfoGroupIdName\":\"\",\"StreamResInfoIsWindowFilledName\":\"\"}},\"DataSink\":{\"NodeType\":\"1124\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false}}");
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1129\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"NodeList\":[{\"NodeType\":\"1128\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}},\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false}}],\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"avg(c1)\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"t2\",\"TableAlias\":\"t2\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"_twstart\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"178\",\"Type\":\"3524\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"avg(c1)\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"ColName\":\"expr_2\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1124\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false}}]}}}}");
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}]");
  addCreateStreamOutCols(&expect, "_twstart", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, {0});
  addCreateStreamOutCols(&expect, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, {0});
  setCreateStreamTriggerSliding(&expect, 's', 's', 0, 0, 0, 1000, 0, 1000, 0);

  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");

  // test water mark
  setCreateStreamWatermark(&expect, 1000);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.t1  options(watermark(1000a)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.t1  options(watermark(1000a)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");

  setCreateStreamWatermark(&expect, 1000000);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.t1  options(watermark(1000s)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.t1  options(watermark(1000s)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");
  setCreateStreamWatermark(&expect, 0);

  // expired_time
  setCreateStreamExpiredTime(&expect, 1000);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.t1  options(expired_time(1000a)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.t1  options(expired_time(1000a)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");

  setCreateStreamExpiredTime(&expect, 1000000);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.t1  options(expired_time(1000s)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.t1  options(expired_time(1000s)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");
  setCreateStreamExpiredTime(&expect, 0);

  // ignore disorder
  setCreateStreamIgDisorder(&expect, true);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.t1  options(ignore_disorder) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.t1  options(ignore_disorder) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");
  setCreateStreamIgDisorder(&expect, false);

  // delete recalc
  setCreateStreamDeleteReCalc(&expect, true);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.t1  options(delete_recalc) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.t1  options(delete_recalc) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");
  setCreateStreamDeleteReCalc(&expect, false);

  // delete output table
  setCreateStreamDeleteOutTbl(&expect, true);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.t1  options(delete_output_table ) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.t1  options(delete_output_table ) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");
  setCreateStreamDeleteOutTbl(&expect, false);

  // fill history
  setCreateStreamFillHistory(&expect, true);
  setCreateStreamFillHistoryStartTime(&expect, 1749626234000);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.t1  options(fill_history('2025-06-11 15:17:14')) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.t1  options(fill_history('2025-06-11 15:17:14')) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");
  setCreateStreamFillHistory(&expect, false);
  setCreateStreamFillHistoryStartTime(&expect, 0);

  // fill history first
  setCreateStreamFillHistoryFirst(&expect, true);
  setCreateStreamFillHistoryStartTime(&expect, 1749626234000);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.t1  options(fill_history_first('2025-06-11 15:17:14')) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.t1  options(fill_history_first('2025-06-11 15:17:14')) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");
  setCreateStreamFillHistoryFirst(&expect, false);
  setCreateStreamFillHistoryStartTime(&expect, 0);

  // calc notify only
  setCreateStreamCalcNotifyOnly(&expect, true);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.t1  options(calc_notify_only) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.t1  options(calc_notify_only) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");
  setCreateStreamCalcNotifyOnly(&expect, false);

  // low latency calc
  setCreateStreamLowLatencyCalc(&expect, true);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.t1  options(low_latency_calc) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.t1  options(low_latency_calc) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");
  setCreateStreamLowLatencyCalc(&expect, false);

  // force output
  // TODO(smj) : add later
  //setCreateStreamForceOutputCols(&expect, true);

  // pre filter
  // TODO(smj) : add later

  // max delay
  setCreateStreamMaxDelay(&expect, 1000);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.t1  options(max_delay(1000a)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.t1  options(max_delay(1000a)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");

  setCreateStreamMaxDelay(&expect, 1000000);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.t1  options(max_delay(1000s)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.t1  options(max_delay(1000s)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");
  setCreateStreamMaxDelay(&expect, 0);

  // event type
  setCreateStreamEventTypes(&expect, EVENT_WINDOW_CLOSE);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1  options(event_type(window_close)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1  options(event_type(window_close)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2");

  setCreateStreamEventTypes(&expect, EVENT_WINDOW_OPEN);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 options(event_type(window_open)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2 ");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 options(event_type(window_open)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2 ");

  setCreateStreamEventTypes(&expect, EVENT_WINDOW_OPEN | EVENT_WINDOW_CLOSE);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 options(event_type(window_open | window_close)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2 ");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 options(event_type(window_open | window_close)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.t2 ");

  clearCreateStreamReq();
}

TEST_F(ParserStreamTest, TestTriggerType) {
  useDb("root", "stream_streamdb");
  SCMCreateStreamReq expect = {0};

  auto clearCreateStreamReq = [&]() {
    tFreeSCMCreateStreamReq(&expect);
    memset(&expect, 0, sizeof(SCMCreateStreamReq));
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_STREAM_STMT);
    SCMCreateStreamReq req = {0};
    ASSERT_TRUE(TSDB_CODE_SUCCESS ==
               tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    checkCreateStreamReq(&expect, &req);
    tFreeSCMCreateStreamReq(&req);
 });

  setCreateStreamTriggerTable(&expect, "0.stream_triggerdb", "t1", WINDOW_TYPE_INTERVAL, TSDB_NORMAL_TABLE, 49, 0, 1);
  setCreateStreamOutTable(&expect, "0.stream_outdb", "stream_out", TSDB_NORMAL_TABLE, 0, 0, 1, 1);
  setCreateStreamReq(&expect, "0.stream_streamdb.s1", "0.stream_streamdb", "0.stream_querydb", "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2", 0);
  setCreateStreamOption(&expect, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, EVENT_WINDOW_CLOSE, 0, 0, 512, -1);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1128\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"0\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"304\",\"OutputRowSize\":\"304\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"8280654498900312045\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3557205140367942817\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6164016115312601301\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_4\"}}],\"Precision\":\"0\"}},\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"expr_3\",\"UserAlias\":\"c2\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"3\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_4\",\"UserAlias\":\"tbname\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"49\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\"},\"GroupOrderScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"StreamResInfoStbFullName\":\"\",\"StreamResInfoWstartName\":\"\",\"StreamResInfoWendName\":\"\",\"StreamResInfoGroupIdName\":\"\",\"StreamResInfoIsWindowFilledName\":\"\"}},\"DataSink\":{\"NodeType\":\"1124\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"304\",\"OutputRowSize\":\"304\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false}}");
  addCreateStreamQueryScanPlan(&expect, false, "{\"NodeType\":\"1128\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"2\",\"SubplanId\":\"2\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"1\",\"DbFName\":\"0.stream_querydb\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3612687029497005528\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"281596734614822715\"}}],\"Precision\":\"0\"}},\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"0\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"\",\"TableName\":\"t2\",\"TableAlias\":\"t2\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"t2\",\"TableAlias\":\"t2\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}],\"TableId\":\"30\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_querydb\",\"TableName\":\"t2\"},\"GroupOrderScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"2\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"StreamResInfoStbFullName\":\"\",\"StreamResInfoWstartName\":\"\",\"StreamResInfoWendName\":\"\",\"StreamResInfoGroupIdName\":\"\",\"StreamResInfoIsWindowFilledName\":\"\"}},\"DataSink\":{\"NodeType\":\"1124\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false}}");
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1129\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"NodeList\":[{\"NodeType\":\"1128\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}},\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false}}],\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"avg(c1)\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"t2\",\"TableAlias\":\"t2\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"_tlocaltime\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"184\",\"Type\":\"3530\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"avg(c1)\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"ColName\":\"expr_2\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1124\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false}}]}}}}");
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}]");
  addCreateStreamOutCols(&expect, "_tlocaltime", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, {0});
  addCreateStreamOutCols(&expect, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, {0});
  setCreateStreamTriggerSliding(&expect, 's', 's', 0, 0, 0, 1000, 0, 1000, 0);

  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");

  // test session window
  setCreateStreamTriggerType(&expect, WINDOW_TYPE_SESSION);
  setCreateStreamTriggerSession(&expect, 1, 0);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 session(ts, 1a) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 session(ts, 1a) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");

  setCreateStreamTriggerSession(&expect, 1000, 0);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 session(ts, 1s) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 session(ts, 1s) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");


  // test state window
  setCreateStreamTriggerType(&expect, WINDOW_TYPE_STATE);
  setCreateStreamTriggerState(&expect, 1, 0);
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}]");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 state_window(c1) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 state_window(c1) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");

  setCreateStreamTriggerState(&expect, 2, 0);
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"c2\",\"UserAlias\":\"c2\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c2\",\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}]");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 state_window(c2) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 state_window(c2) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");

  setCreateStreamTriggerState(&expect, 2, 1);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 state_window(c2) true_for(1a) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 state_window(c2) true_for(1a) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");

  setCreateStreamTriggerState(&expect, 2, 1000);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 state_window(c2) true_for(1s) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 state_window(c2) true_for(1s) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");

  // interval window
  setCreateStreamTriggerType(&expect, WINDOW_TYPE_INTERVAL);
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}]");
  setCreateStreamTriggerSliding(&expect, 0, 's', 0, 0, 0, 0, 0, 1000, 0);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 sliding(1s) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 sliding(1s) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");

  setCreateStreamTriggerSliding(&expect, 0, 's', 0, 's', 0, 0, 0, 100000, 1000);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 sliding(100s, 1s) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 sliding(100s, 1s) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");

  setCreateStreamTriggerSliding(&expect, 'h', 's', 0, 's', 0, 3600000, 0, 100000, 1000);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1h) sliding(100s, 1s) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 interval(1h) sliding(100s, 1s) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");

  setCreateStreamTriggerSliding(&expect, 'h', 's', 'm', 's', 0, 3600000, 60000, 100000, 1000);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1h, 1m) sliding(100s, 1s) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 interval(1h, 1m) sliding(100s, 1s) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");

  // event window
  setCreateStreamTriggerType(&expect, WINDOW_TYPE_EVENT);
  setCreateStreamTriggerEvent(&expect,
    "{\"NodeType\":\"3\",\"Name\":\"Operator\",\"Operator\":{\"DataType\":{\"Type\":\"1\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"3831883314967181845\",\"UserAlias\":\"c1 > 1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"OpType\":\"40\",\"Left\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}},\"Right\":{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"5001870860487857737\",\"UserAlias\":\"1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"1\",\"Literal\":\"1\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"1\"}}}}",
    "{\"NodeType\":\"3\",\"Name\":\"Operator\",\"Operator\":{\"DataType\":{\"Type\":\"1\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"8823744644992822608\",\"UserAlias\":\"c2 < 1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"OpType\":\"42\",\"Left\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"c2\",\"UserAlias\":\"c2\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c2\",\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}},\"Right\":{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"5001870860487857737\",\"UserAlias\":\"1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"1\",\"Literal\":\"1\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"1\"}}}}",
    0);
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"c2\",\"UserAlias\":\"c2\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c2\",\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}]");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 event_window(start with c1 > 1 end with c2 < 1) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 event_window(start with c1 > 1 end with c2 < 1) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");

  setCreateStreamTriggerEvent(&expect,
    "{\"NodeType\":\"3\",\"Name\":\"Operator\",\"Operator\":{\"DataType\":{\"Type\":\"1\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"3831883314967181845\",\"UserAlias\":\"c1 > 1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"OpType\":\"40\",\"Left\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}},\"Right\":{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"5001870860487857737\",\"UserAlias\":\"1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"1\",\"Literal\":\"1\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"1\"}}}}",
    "{\"NodeType\":\"3\",\"Name\":\"Operator\",\"Operator\":{\"DataType\":{\"Type\":\"1\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"8823744644992822608\",\"UserAlias\":\"c2 < 1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"OpType\":\"42\",\"Left\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"c2\",\"UserAlias\":\"c2\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c2\",\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}},\"Right\":{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"5001870860487857737\",\"UserAlias\":\"1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"1\",\"Literal\":\"1\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"1\"}}}}",
    3600000);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 event_window(start with c1 > 1 end with c2 < 1) true_for(1h) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 event_window(start with c1 > 1 end with c2 < 1) true_for(1h) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");


  // count window
  setCreateStreamTriggerType(&expect, WINDOW_TYPE_COUNT);
  setCreateStreamTriggerCount(&expect, 10, 10);
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}]");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 count_window(10) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 count_window(10) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");

  setCreateStreamTriggerCount(&expect, 20, 10);
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}]");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 count_window(20, 10) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 count_window(20, 10) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");

  setCreateStreamTriggerCount(&expect, 20, 20);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1128\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"0\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"304\",\"OutputRowSize\":\"304\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"8280654498900312045\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3557205140367942817\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6164016115312601301\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_4\"}}],\"Precision\":\"0\"}},\"Conditions\":{\"NodeType\":\"4\",\"Name\":\"LogicCondition\",\"LogicCondition\":{\"DataType\":{\"Type\":\"1\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"CondType\":\"2\",\"Parameters\":[{\"NodeType\":\"3\",\"Name\":\"Operator\",\"Operator\":{\"DataType\":{\"Type\":\"1\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"OpType\":\"101\",\"Left\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}},\"Right\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"3\",\"Name\":\"Operator\",\"Operator\":{\"DataType\":{\"Type\":\"1\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"OpType\":\"101\",\"Left\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"c2\",\"UserAlias\":\"c2\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c2\",\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}},\"Right\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"c2\",\"UserAlias\":\"c2\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c2\",\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}]}},\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"c2\",\"UserAlias\":\"c2\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"3\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_4\",\"UserAlias\":\"tbname\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"49\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\"},\"GroupOrderScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"StreamResInfoStbFullName\":\"\",\"StreamResInfoWstartName\":\"\",\"StreamResInfoWendName\":\"\",\"StreamResInfoGroupIdName\":\"\",\"StreamResInfoIsWindowFilledName\":\"\"}},\"DataSink\":{\"NodeType\":\"1124\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"304\",\"OutputRowSize\":\"304\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false}}");
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}]");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 count_window(20, c1, c2) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 count_window(20, c1, c2) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");

  setCreateStreamTriggerCount(&expect, 20, 10);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1128\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"0\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"304\",\"OutputRowSize\":\"304\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"8280654498900312045\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3557205140367942817\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6164016115312601301\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_4\"}}],\"Precision\":\"0\"}},\"Conditions\":{\"NodeType\":\"4\",\"Name\":\"LogicCondition\",\"LogicCondition\":{\"DataType\":{\"Type\":\"1\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"CondType\":\"2\",\"Parameters\":[{\"NodeType\":\"3\",\"Name\":\"Operator\",\"Operator\":{\"DataType\":{\"Type\":\"1\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"OpType\":\"101\",\"Left\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}},\"Right\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"3\",\"Name\":\"Operator\",\"Operator\":{\"DataType\":{\"Type\":\"1\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"OpType\":\"101\",\"Left\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"c2\",\"UserAlias\":\"c2\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c2\",\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}},\"Right\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"c2\",\"UserAlias\":\"c2\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c2\",\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}]}},\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"c2\",\"UserAlias\":\"c2\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"3\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_4\",\"UserAlias\":\"tbname\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"49\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\"},\"GroupOrderScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"StreamResInfoStbFullName\":\"\",\"StreamResInfoWstartName\":\"\",\"StreamResInfoWendName\":\"\",\"StreamResInfoGroupIdName\":\"\",\"StreamResInfoIsWindowFilledName\":\"\"}},\"DataSink\":{\"NodeType\":\"1124\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"304\",\"OutputRowSize\":\"304\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false}}");
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}]");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 count_window(20, 10, c1, c2) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 count_window(20, 10, c1, c2) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");

  // period window
  resetCreateStreamTriggerCols(&expect);
  setCreateStreamTriggerType(&expect, WINDOW_TYPE_PERIOD);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1128\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"0\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"304\",\"OutputRowSize\":\"304\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"8280654498900312045\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3557205140367942817\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6164016115312601301\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_4\"}}],\"Precision\":\"0\"}},\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"expr_3\",\"UserAlias\":\"c2\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"3\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_4\",\"UserAlias\":\"tbname\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"49\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\"},\"GroupOrderScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"StreamResInfoStbFullName\":\"\",\"StreamResInfoWstartName\":\"\",\"StreamResInfoWendName\":\"\",\"StreamResInfoGroupIdName\":\"\",\"StreamResInfoIsWindowFilledName\":\"\"}},\"DataSink\":{\"NodeType\":\"1124\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"304\",\"OutputRowSize\":\"304\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false}}");
  setCreateStreamTriggerPeriod(&expect, 0, 's', 0, 0, 1000);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 period(1s) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 period(1s) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");

  setCreateStreamTriggerPeriod(&expect, 's', 'm', 0, 1000, 60000);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 period(1m, 1s) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 period(1m, 1s) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");

  clearCreateStreamReq();
}

TEST_F(ParserStreamTest, TestNotify) {
  useDb("root", "stream_streamdb");
  SCMCreateStreamReq expect = {0};

  auto clearCreateStreamReq = [&]() {
    tFreeSCMCreateStreamReq(&expect);
    memset(&expect, 0, sizeof(SCMCreateStreamReq));
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_STREAM_STMT);
    SCMCreateStreamReq req = {0};
    ASSERT_TRUE(TSDB_CODE_SUCCESS ==
               tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    checkCreateStreamReq(&expect, &req);
    tFreeSCMCreateStreamReq(&req);
 });

  setCreateStreamTriggerTable(&expect, "0.stream_triggerdb", "t1", WINDOW_TYPE_INTERVAL, TSDB_NORMAL_TABLE, 49, 0, 1);
  setCreateStreamOutTable(&expect, "0.stream_outdb", "stream_out", TSDB_NORMAL_TABLE, 0, 0, 1, 1);
  setCreateStreamReq(&expect, "0.stream_streamdb.s1", "0.stream_streamdb", "0.stream_querydb", "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2", 0);
  setCreateStreamOption(&expect, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, EVENT_WINDOW_CLOSE, 0, 0, 512, -1);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1128\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"0\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"304\",\"OutputRowSize\":\"304\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"8280654498900312045\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3557205140367942817\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6164016115312601301\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_4\"}}],\"Precision\":\"0\"}},\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"expr_3\",\"UserAlias\":\"c2\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"3\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_4\",\"UserAlias\":\"tbname\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"49\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\"},\"GroupOrderScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"StreamResInfoStbFullName\":\"\",\"StreamResInfoWstartName\":\"\",\"StreamResInfoWendName\":\"\",\"StreamResInfoGroupIdName\":\"\",\"StreamResInfoIsWindowFilledName\":\"\"}},\"DataSink\":{\"NodeType\":\"1124\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"304\",\"OutputRowSize\":\"304\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false}}");
  addCreateStreamQueryScanPlan(&expect, false, "{\"NodeType\":\"1128\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"2\",\"SubplanId\":\"2\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"1\",\"DbFName\":\"0.stream_querydb\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3612687029497005528\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"281596734614822715\"}}],\"Precision\":\"0\"}},\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"0\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"\",\"TableName\":\"t2\",\"TableAlias\":\"t2\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"t2\",\"TableAlias\":\"t2\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}],\"TableId\":\"30\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_querydb\",\"TableName\":\"t2\"},\"GroupOrderScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"2\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"StreamResInfoStbFullName\":\"\",\"StreamResInfoWstartName\":\"\",\"StreamResInfoWendName\":\"\",\"StreamResInfoGroupIdName\":\"\",\"StreamResInfoIsWindowFilledName\":\"\"}},\"DataSink\":{\"NodeType\":\"1124\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false}}");
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1129\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"NodeList\":[{\"NodeType\":\"1128\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}},\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false}}],\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"avg(c1)\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"t2\",\"TableAlias\":\"t2\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"_tlocaltime\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"184\",\"Type\":\"3530\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"avg(c1)\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"ColName\":\"expr_2\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1124\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false}}]}}}}");
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}]");
  addCreateStreamOutCols(&expect, "_tlocaltime", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, {0});
  addCreateStreamOutCols(&expect, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, {0});
  setCreateStreamTriggerSliding(&expect, 's', 's', 0, 0, 0, 1000, 0, 1000, 0);

  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");

  // url
  addCreateStreamNotifyUrl(&expect, "ws://localhost:8080");
  setCreateStreamNotify(&expect, EVENT_NONE, false, false);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 notify('ws://localhost:8080') into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 notify('ws://localhost:8080') into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");

  addCreateStreamNotifyUrl(&expect, "ws://localhost:8080/notify");
  setCreateStreamNotify(&expect, EVENT_NONE, false, false);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 notify('ws://localhost:8080', 'ws://localhost:8080/notify') into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 notify('ws://localhost:8080', 'ws://localhost:8080/notify') into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");

  // ON EVENT
  setCreateStreamNotify(&expect, EVENT_WINDOW_CLOSE, false, false);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 notify('ws://localhost:8080', 'ws://localhost:8080/notify') on (window_close) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 notify('ws://localhost:8080', 'ws://localhost:8080/notify') on (window_close) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");

  setCreateStreamNotify(&expect, EVENT_WINDOW_OPEN, false, false);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 notify('ws://localhost:8080', 'ws://localhost:8080/notify') on (window_open) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 notify('ws://localhost:8080', 'ws://localhost:8080/notify') on (window_open) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");

  setCreateStreamNotify(&expect, EVENT_WINDOW_OPEN | EVENT_WINDOW_CLOSE, false, false);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 notify('ws://localhost:8080', 'ws://localhost:8080/notify') on (window_close|window_open) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 notify('ws://localhost:8080', 'ws://localhost:8080/notify') on (window_close|window_open) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");

  // where condition
  // TODO(smj) add later

  // notify options
  setCreateStreamNotify(&expect, EVENT_NONE, false, true);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 notify('ws://localhost:8080', 'ws://localhost:8080/notify') notify_options(notify_history) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 notify('ws://localhost:8080', 'ws://localhost:8080/notify') notify_options(notify_history) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");

  setCreateStreamNotify(&expect, EVENT_NONE, true, false);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 notify('ws://localhost:8080', 'ws://localhost:8080/notify') notify_options(on_failure_pause) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 notify('ws://localhost:8080', 'ws://localhost:8080/notify') notify_options(on_failure_pause) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");

  setCreateStreamNotify(&expect, EVENT_NONE, true, true);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 notify('ws://localhost:8080', 'ws://localhost:8080/notify') notify_options(notify_history|on_failure_pause) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.t1 notify('ws://localhost:8080', 'ws://localhost:8080/notify') notify_options(notify_history|on_failure_pause) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.t2");

  clearCreateStreamReq();
}

}  // namespace ParserTest
