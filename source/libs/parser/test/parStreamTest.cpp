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
*     trigger_type [FROM [db_name.]table_name] [PARTITION BY col1 [, ...]] [OPTIONS(stream_option [|...]] [notification_definition]
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
*     NOTIFY(url [, ...]) [ON (event_types)] [WHERE condition] [NOTIFY_OPTIONS(notify_option[|notify_option)]]
*
* notify_option: [NOTIFY_HISTORY | ON_FAILURE_PAUSE]
*
* tag_definition:
*     tag_name type_name [COMMENT 'string_value'] AS expr
*/

void setCreateStreamReq(SCMCreateStreamReq *expect, const char* pStream, const char* pStreamDb, const char* pTriggerDb,
                        const char* pOutDb, const char* pCalcDb, const char* pTriggerTblName, const char* pOutTbleName,
                        const char* pSql, int8_t igExists) {
  expect->name = taosStrdup(pStream);
  expect->igExists = igExists;
  expect->sql = taosStrdup(pSql);
  expect->streamDB = taosStrdup(pStreamDb);
  expect->triggerDB = taosStrdup(pTriggerDb);
  expect->outDB = taosStrdup(pOutDb);
  expect->triggerTblName = taosStrdup(pTriggerTblName);
  expect->outTblName = taosStrdup(pOutTbleName);
  if (expect->calcDB == NULL) {
    expect->calcDB = taosArrayInit(1, POINTER_BYTES);
  }
  char *tmpCalcDb = taosStrdup(pCalcDb);
  ASSERT_TRUE(taosArrayPush(expect->calcDB, &tmpCalcDb));
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

void setCreateStreamOption(SCMCreateStreamReq *expect, int8_t igExists, int8_t triggerType, int8_t igDisorder, int8_t deleteReCalc,
  int8_t deleteOutTbl, int8_t fillHistory, int8_t fillHistoryFirst, int8_t calcNotifyOnly, int8_t lowLatencyCalc,
  int64_t maxDelay, int64_t fillHistoryStartTime, int64_t watermark, int64_t expiredTime, int8_t triggerTblType,
  uint64_t triggerTblUid, int8_t outTblType, int8_t outStbExists, uint64_t outStbUid, int32_t outStbSversion,
  int64_t eventTypes, int64_t flags, int64_t tsmaId, int64_t placeHolderBitmap){
  expect->igExists = igExists;
  expect->triggerType = triggerType;
  expect->igDisorder = igDisorder;
  expect->deleteReCalc = deleteReCalc;
  expect->deleteOutTbl = deleteOutTbl;
  expect->fillHistory = fillHistory;
  expect->fillHistoryFirst = fillHistoryFirst;
  expect->calcNotifyOnly = calcNotifyOnly;
  expect->lowLatencyCalc = lowLatencyCalc;
  expect->maxDelay = maxDelay;
  expect->fillHistoryStartTime = fillHistoryStartTime;
  expect->watermark = watermark;
  expect->expiredTime = expiredTime;
  expect->triggerTblType = triggerTblType;
  expect->triggerTblUid = triggerTblUid;
  expect->outTblType = outTblType;
  expect->outStbExists = outStbExists;
  expect->outStbUid = outStbUid;
  expect->outStbSversion = outStbSversion;
  expect->eventTypes = eventTypes;
  expect->flags = flags;
  expect->tsmaId = tsmaId;
  expect->placeHolderBitmap = placeHolderBitmap;
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

void setCreateStreamTriggerCount(SCMCreateStreamReq *expect, int64_t countVal, int64_t sliding, const char* condCols) {
  expect->trigger.count.countVal = countVal;
  expect->trigger.count.sliding = sliding;
  expect->trigger.count.condCols = taosStrdup(condCols);
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
  ASSERT_TRUE(taosArrayPush(expect->calcDB, &tmpUrl));
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
      ASSERT_EQ(std::string((char*)req->trigger.count.condCols), std::string((char*)expect->trigger.count.condCols));
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
  if (BIT_FLAG_TEST_MASK(req->placeHolderBitmap, SP_PARTITION_ROWS)) {
    ASSERT_EQ(req->tsSlotId, expect->tsSlotId);
  }

  ASSERT_EQ(std::string((char*)req->triggerScanPlan), std::string((char*)expect->triggerScanPlan));
  checkCreateStreamCalcPlan(expect, req);

  ASSERT_EQ(taosArrayGetSize(req->calcScanPlanList), taosArrayGetSize(expect->calcScanPlanList));
  for (int32_t i = 0; i < taosArrayGetSize(req->calcScanPlanList); i++) {
    auto pCalcScan = (SStreamCalcScan *)taosArrayGet(req->calcScanPlanList, i);
    auto expectScan = (SStreamCalcScan *)taosArrayGet(expect->calcScanPlanList, i);
    ASSERT_EQ(pCalcScan->readFromCache, expectScan->readFromCache);
    ASSERT_EQ(std::string((char*)pCalcScan->scanPlan), std::string((char*)expectScan->scanPlan));
  }

  ASSERT_EQ(std::string((char*)req->subTblNameExpr), std::string((char*)expect->subTblNameExpr));
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

  setCreateStreamReq(&expect, "s1", "0.test", "0.test", "0.test", "0.test", "t1", "stream_out", "create stream s1 interval(1s) sliding(1s) from t1 into stream_out as select now, avg(c1) from t2", 0);
  setCreateStreamOption(&expect, 0, WINDOW_TYPE_INTERVAL, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, TSDB_NORMAL_TABLE, 9, TSDB_NORMAL_TABLE, 0, 0, 1, 1, 0, 0, 0);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1128\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"0\",\"DbFName\":\"0.test\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"32\",\"OutputRowSize\":\"32\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"8280654498900312045\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3557205140367942817\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6164016115312601301\"}}],\"Precision\":\"0\"}},\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"9\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"9\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"expr_3\",\"UserAlias\":\"c2\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"9\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"c2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}],\"TableId\":\"9\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"test\",\"TableName\":\"t1\"},\"GroupOrderScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"StreamResInfoStbFullName\":\"\",\"StreamResInfoWstartName\":\"\",\"StreamResInfoWendName\":\"\",\"StreamResInfoGroupIdName\":\"\",\"StreamResInfoIsWindowFilledName\":\"\"}},\"DataSink\":{\"NodeType\":\"1124\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"32\",\"OutputRowSize\":\"32\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false}}");
  addCreateStreamQueryScanPlan(&expect, false, "{\"NodeType\":\"1128\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"2\",\"SubplanId\":\"2\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"1\",\"DbFName\":\"0.test\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3612687029497005528\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"281596734614822715\"}}],\"Precision\":\"0\"}},\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"10\",\"TableType\":\"0\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"\",\"TableName\":\"t2\",\"TableAlias\":\"t2\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"10\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"t2\",\"TableAlias\":\"t2\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}],\"TableId\":\"10\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"test\",\"TableName\":\"t2\"},\"GroupOrderScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"2\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"StreamResInfoStbFullName\":\"\",\"StreamResInfoWstartName\":\"\",\"StreamResInfoWendName\":\"\",\"StreamResInfoGroupIdName\":\"\",\"StreamResInfoIsWindowFilledName\":\"\"}},\"DataSink\":{\"NodeType\":\"1124\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false}}");
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1129\",\"Name\":\"PhysiPlan\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"Name\":\"NodeList\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"NodeList\":[{\"NodeType\":\"1128\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"Name\":\"PhysiProject\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"7367396282153980292\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"4056049374920546965\"}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"Name\":\"PhysiAgg\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_2\"}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"Name\":\"PhysiExchange\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3612687029497005528\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"281596734614822715\"}}],\"Precision\":\"0\"}},\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":true}}],\"AggFuncs\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"avg(c1)\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"avg\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"10\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"t2\",\"TableAlias\":\"t2\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"Projections\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"now\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"now\",\"Id\":\"82\",\"Type\":\"2500\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"2\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"avg(c1)\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"ColName\":\"expr_2\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1124\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\"}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false}}]}}}}");
  setCreateStreamSubTblNameExpr(&expect, "{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"14\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"concat\",\"Id\":\"70\",\"Type\":\"1502\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"2\",\"Literal\":\"t_\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"t_\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"10\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"cast\",\"Id\":\"77\",\"Type\":\"2000\",\"Parameters\":[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_tgrpid\",\"Id\":\"181\",\"Type\":\"3527\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"2\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}");
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"9\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"t1\",\"TableAlias\":\"t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}]");
  addCreateStreamOutCols(&expect, "now", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, {0});
  addCreateStreamOutCols(&expect, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, {0});
  setCreateStreamTriggerSliding(&expect, 's', 's', 0, 0, 0, 1000, 0, 1000, 0);
  run("create stream s1 interval(1s) sliding(1s) from t1 into stream_out as select now, avg(c1) from t2");
  clearCreateStreamReq();
}

}  // namespace ParserTest
