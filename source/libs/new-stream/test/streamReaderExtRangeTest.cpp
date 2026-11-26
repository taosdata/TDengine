/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

// Unit tests for streamReaderExtCalcFetchRange: the external-table reader's
// counterpart of vnodeStream.c processCalaTimeRange.  Kept apart from
// streamReaderTest.cpp because streamReaderExt.h cannot share a translation
// unit with streamReader.h.

#include <gtest/gtest.h>
#include <cstring>
#include <string>
#include <thread>
#include <vector>
#include "stub.h"

#include "functionMgt.h"
#include "nodes.h"
#include "querynodes.h"
#include "streamReaderExt.h"
#include "tarray.h"
#include "tdatablock.h"
#include "tdataformat.h"
#include "tsimplehash.h"

namespace {

SNode *makePlaceholder(const char *name) {
  const int32_t funcId = fmGetFuncId(name);
  EXPECT_GE(funcId, 0);
  SFunctionNode *func = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_FUNCTION, reinterpret_cast<SNode **>(&func)));
  if (func == nullptr) return nullptr;
  func->funcId = funcId;
  func->funcType = fmGetFuncTypeFromId(funcId);
  func->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
  func->node.resType.bytes = sizeof(int64_t);

  SValueNode *result = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode **>(&result)));
  if (result == nullptr) {
    nodesDestroyNode(reinterpret_cast<SNode *>(func));
    return nullptr;
  }
  result->node.resType = func->node.resType;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&func->pParameterList, reinterpret_cast<SNode *>(result)));
  return reinterpret_cast<SNode *>(func);
}

// ts <op> <placeholder>
SNode *makeBound(EOperatorType opType, const char *placeholder) {
  SColumnNode *column = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_COLUMN, reinterpret_cast<SNode **>(&column)));
  if (column == nullptr) return nullptr;
  column->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
  column->node.resType.bytes = sizeof(int64_t);
  column->colId = PRIMARYKEY_TIMESTAMP_COL_ID;

  SOperatorNode *op = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_OPERATOR, reinterpret_cast<SNode **>(&op)));
  if (op == nullptr) {
    nodesDestroyNode(reinterpret_cast<SNode *>(column));
    return nullptr;
  }
  op->node.resType.type = TSDB_DATA_TYPE_BOOL;
  op->node.resType.bytes = sizeof(bool);
  op->opType = opType;
  op->pLeft = reinterpret_cast<SNode *>(column);
  op->pRight = makePlaceholder(placeholder);
  if (op->pRight == nullptr) {
    nodesDestroyNode(reinterpret_cast<SNode *>(op));
    return nullptr;
  }
  return reinterpret_cast<SNode *>(op);
}

// Owns an STimeRangeNode handed to the reader (freed with the fixture).
SNode *makeTimeRange(bool needCalc, EOperatorType startOp, const char *startPh, EOperatorType endOp,
                     const char *endPh) {
  STimeRangeNode *node = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_TIME_RANGE, reinterpret_cast<SNode **>(&node)));
  if (node == nullptr) return nullptr;
  node->needCalc = needCalc;
  if (needCalc) {
    node->pStart = makeBound(startOp, startPh);
    node->pEnd = makeBound(endOp, endPh);
    EXPECT_NE(nullptr, node->pStart);
    EXPECT_NE(nullptr, node->pEnd);
  }
  return reinterpret_cast<SNode *>(node);
}

SArray *makeParams(std::initializer_list<std::pair<int64_t, int64_t>> windows) {
  SArray *params = taosArrayInit(windows.size(), sizeof(SSTriggerCalcParam));
  EXPECT_NE(nullptr, params);
  for (const auto &w : windows) {
    SSTriggerCalcParam p = {};
    p.wstart = w.first;
    p.wend = w.second;
    EXPECT_NE(nullptr, taosArrayPush(params, &p));
  }
  return params;
}

class StreamReaderExtRangeTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { ASSERT_EQ(TSDB_CODE_SUCCESS, fmFuncMgtInit()); }

  void TearDown() override {
    nodesDestroyNode(info.pCalcTimeRange);
    taosArrayDestroy(rt.pStreamPesudoFuncVals);
    if (rt.pGroupCalcInfos != nullptr) {
      int32_t iter = 0;
      void   *p = tSimpleHashIterate(rt.pGroupCalcInfos, nullptr, &iter);
      while (p != nullptr) {
        taosArrayDestroy(static_cast<SSTriggerGroupCalcInfo *>(p)->pParams);
        p = tSimpleHashIterate(rt.pGroupCalcInfos, p, &iter);
      }
      tSimpleHashCleanup(rt.pGroupCalcInfos);
    }
  }

  void addGroup(int64_t gid, SArray *params) {
    if (rt.pGroupCalcInfos == nullptr) {
      rt.pGroupCalcInfos = tSimpleHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
      ASSERT_NE(nullptr, rt.pGroupCalcInfos);
    }
    SSTriggerGroupCalcInfo calc = {};
    calc.pParams = params;
    ASSERT_EQ(TSDB_CODE_SUCCESS, tSimpleHashPut(rt.pGroupCalcInfos, &gid, sizeof(gid), &calc, sizeof(calc)));
  }

  int32_t calc() { return streamReaderExtCalcFetchRange(&info, &rt, &skey, &ekey); }

  SStreamExtReaderInfo   info = {};
  SStreamRuntimeFuncInfo rt = {};
  int64_t                skey = 0;
  int64_t                ekey = 0;
};

TEST_F(StreamReaderExtRangeTest, NoTimeRangeIsUnbounded) {
  rt.pStreamPesudoFuncVals = makeParams({{1000, 2000}});
  ASSERT_EQ(TSDB_CODE_SUCCESS, calc());
  EXPECT_EQ(INT64_MIN, skey);
  EXPECT_EQ(INT64_MAX, ekey);
}

TEST_F(StreamReaderExtRangeTest, NoWindowParamsIsUnbounded) {
  info.pCalcTimeRange = makeTimeRange(false, OP_TYPE_GREATER_EQUAL, nullptr, OP_TYPE_LOWER_THAN, nullptr);
  ASSERT_EQ(TSDB_CODE_SUCCESS, calc());
  EXPECT_EQ(INT64_MIN, skey);
  EXPECT_EQ(INT64_MAX, ekey);
}

TEST_F(StreamReaderExtRangeTest, OutOfRangeCurIdxIsError) {
  info.pCalcTimeRange = makeTimeRange(false, OP_TYPE_GREATER_EQUAL, nullptr, OP_TYPE_LOWER_THAN, nullptr);
  rt.pStreamPesudoFuncVals = makeParams({{1000, 2000}});
  rt.curIdx = 3;
  EXPECT_EQ(TSDB_CODE_STREAM_INTERNAL_ERROR, calc());
}

TEST_F(StreamReaderExtRangeTest, RawRangeUsesCurrentWindow) {
  info.pCalcTimeRange = makeTimeRange(false, OP_TYPE_GREATER_EQUAL, nullptr, OP_TYPE_LOWER_THAN, nullptr);
  rt.pStreamPesudoFuncVals = makeParams({{1000, 1999}, {2000, 2999}});
  rt.curIdx = 1;
  ASSERT_EQ(TSDB_CODE_SUCCESS, calc());
  EXPECT_EQ(2000, skey);
  EXPECT_EQ(2999, ekey);
}

TEST_F(StreamReaderExtRangeTest, RawRangeSlidingExcludesWindowEnd) {
  info.pCalcTimeRange = makeTimeRange(false, OP_TYPE_GREATER_EQUAL, nullptr, OP_TYPE_LOWER_THAN, nullptr);
  rt.triggerType = STREAM_TRIGGER_SLIDING;
  rt.pStreamPesudoFuncVals = makeParams({{1000, 2000}});
  ASSERT_EQ(TSDB_CODE_SUCCESS, calc());
  EXPECT_EQ(1000, skey);
  EXPECT_EQ(1999, ekey);
}

TEST_F(StreamReaderExtRangeTest, RawRangeExternalWindowSpansAllWindows) {
  info.pCalcTimeRange = makeTimeRange(false, OP_TYPE_GREATER_EQUAL, nullptr, OP_TYPE_LOWER_THAN, nullptr);
  rt.withExternalWindow = true;
  rt.triggerType = STREAM_TRIGGER_SLIDING;
  rt.pStreamPesudoFuncVals = makeParams({{1000, 2000}, {2000, 3000}, {3000, 4000}});
  ASSERT_EQ(TSDB_CODE_SUCCESS, calc());
  EXPECT_EQ(1000, skey);
  EXPECT_EQ(3999, ekey);
}

TEST_F(StreamReaderExtRangeTest, CalcRangeHalfOpenUpperBound) {
  // ts >= _twstart AND ts < _twend
  info.pCalcTimeRange = makeTimeRange(true, OP_TYPE_GREATER_EQUAL, "_twstart", OP_TYPE_LOWER_THAN, "_twend");
  rt.pStreamPesudoFuncVals = makeParams({{1000, 2000}});
  ASSERT_EQ(TSDB_CODE_SUCCESS, calc());
  EXPECT_EQ(1000, skey);
  EXPECT_EQ(1999, ekey);
}

TEST_F(StreamReaderExtRangeTest, CalcRangeInclusiveUpperBoundKeepsBoundaryRow) {
  // ts > _twstart AND ts <= _twend
  info.pCalcTimeRange = makeTimeRange(true, OP_TYPE_GREATER_THAN, "_twstart", OP_TYPE_LOWER_EQUAL, "_twend");
  rt.pStreamPesudoFuncVals = makeParams({{1000, 2000}});
  ASSERT_EQ(TSDB_CODE_SUCCESS, calc());
  EXPECT_EQ(1001, skey);
  EXPECT_EQ(2000, ekey);
}

TEST_F(StreamReaderExtRangeTest, CalcRangeExternalWindowUsesFirstStartAndLastEnd) {
  info.pCalcTimeRange = makeTimeRange(true, OP_TYPE_GREATER_EQUAL, "_twstart", OP_TYPE_LOWER_THAN, "_twend");
  rt.withExternalWindow = true;
  rt.pStreamPesudoFuncVals = makeParams({{1000, 2000}, {5000, 6000}, {3000, 4000}});
  ASSERT_EQ(TSDB_CODE_SUCCESS, calc());
  EXPECT_EQ(1000, skey);
  EXPECT_EQ(3999, ekey);
}

TEST_F(StreamReaderExtRangeTest, CalcRangeSentinelEndBoundIsError) {
  // _twend collapsing to the INT64_MIN sentinel is an invalid window; the ETR
  // must fail loudly like vnodeCalculateNodeGroupReadRange does instead of
  // silently turning the fetch into an empty range.
  info.pCalcTimeRange = makeTimeRange(true, OP_TYPE_GREATER_EQUAL, "_twstart", OP_TYPE_LOWER_THAN, "_twend");
  rt.pStreamPesudoFuncVals = makeParams({{1000, INT64_MIN}});
  EXPECT_EQ(TSDB_CODE_STREAM_INTERNAL_ERROR, calc());
}

TEST_F(StreamReaderExtRangeTest, CalcRangeEmptyFetchesNothing) {
  // ts >= _twend AND ts < _twstart: empty for any non-degenerate window.
  info.pCalcTimeRange = makeTimeRange(true, OP_TYPE_GREATER_EQUAL, "_twend", OP_TYPE_LOWER_THAN, "_twstart");
  rt.pStreamPesudoFuncVals = makeParams({{1000, 2000}});
  ASSERT_EQ(TSDB_CODE_SUCCESS, calc());
  EXPECT_GT(skey, ekey);
  EXPECT_NE(INT64_MIN, skey);
  EXPECT_NE(INT64_MAX, ekey);
}

TEST_F(StreamReaderExtRangeTest, MultiGroupUnionsGroupRanges) {
  info.pCalcTimeRange = makeTimeRange(true, OP_TYPE_GREATER_EQUAL, "_twstart", OP_TYPE_LOWER_THAN, "_twend");
  rt.isMultiGroupCalc = true;
  rt.withExternalWindow = true;
  addGroup(101, makeParams({{5000, 6000}, {6000, 7000}}));
  addGroup(202, makeParams({{1000, 2000}}));
  addGroup(303, makeParams({{8000, 9000}, {9000, 10000}}));
  ASSERT_EQ(TSDB_CODE_SUCCESS, calc());
  EXPECT_EQ(1000, skey);
  EXPECT_EQ(9999, ekey);
}

TEST_F(StreamReaderExtRangeTest, MultiGroupRawRangeUnionsGroupRanges) {
  info.pCalcTimeRange = makeTimeRange(false, OP_TYPE_GREATER_EQUAL, nullptr, OP_TYPE_LOWER_THAN, nullptr);
  rt.isMultiGroupCalc = true;
  rt.triggerType = STREAM_TRIGGER_SLIDING;
  addGroup(101, makeParams({{3000, 4000}}));
  addGroup(202, makeParams({{1000, 2000}}));
  ASSERT_EQ(TSDB_CODE_SUCCESS, calc());
  EXPECT_EQ(1000, skey);
  EXPECT_EQ(3999, ekey);
}

TEST_F(StreamReaderExtRangeTest, MultiGroupWithoutParamsIsUnbounded) {
  info.pCalcTimeRange = makeTimeRange(false, OP_TYPE_GREATER_EQUAL, nullptr, OP_TYPE_LOWER_THAN, nullptr);
  rt.isMultiGroupCalc = true;
  ASSERT_EQ(TSDB_CODE_SUCCESS, calc());
  EXPECT_EQ(INT64_MIN, skey);
  EXPECT_EQ(INT64_MAX, ekey);
}

TEST_F(StreamReaderExtRangeTest, CalcReplicaLookupSelectsExecIdAndRejectsInvalidSlots) {
  SStreamReaderTask task = {};
  SStreamExtReaderInfo other = {};
  SStreamExtReaderInfo *first = &info;
  SStreamExtReaderInfo *second = &other;
  SStreamExtReaderInfo *missing = nullptr;
  SStreamExtReaderInfo *selected = nullptr;
  SArray *readers = taosArrayInit(3, POINTER_BYTES);
  ASSERT_NE(readers, nullptr);
  ASSERT_NE(taosArrayPush(readers, &first), nullptr);
  ASSERT_NE(taosArrayPush(readers, &second), nullptr);
  ASSERT_NE(taosArrayPush(readers, &missing), nullptr);
  task.info = readers;
  task.task.flags = STREAM_FLAG_REF_EXT_SOURCE;

  EXPECT_EQ(streamReaderExtGetCalcInfo(&task, 0, &selected), TSDB_CODE_SUCCESS);
  EXPECT_EQ(selected, first);
  EXPECT_EQ(streamReaderExtGetCalcInfo(&task, 1, &selected), TSDB_CODE_SUCCESS);
  EXPECT_EQ(selected, second);
  for (int32_t execId : {-1, 3, INT32_MAX}) {
    EXPECT_EQ(streamReaderExtGetCalcInfo(&task, execId, &selected), TSDB_CODE_INVALID_PARA);
    EXPECT_EQ(selected, nullptr);
  }
  EXPECT_EQ(streamReaderExtGetCalcInfo(&task, 2, &selected),
            TSDB_CODE_STREAM_EXT_READER_NO_LOCAL_INFO);
  EXPECT_EQ(selected, nullptr);
  task.triggerReader = 1;
  EXPECT_EQ(streamReaderExtGetCalcInfo(&task, 0, &selected), TSDB_CODE_INVALID_MSG);
  task.triggerReader = 0;
  task.task.flags = 0;
  EXPECT_EQ(streamReaderExtGetCalcInfo(&task, 0, &selected), TSDB_CODE_INVALID_MSG);
  task.task.flags = STREAM_FLAG_REF_EXT_SOURCE;
  task.info = nullptr;
  EXPECT_EQ(streamReaderExtGetCalcInfo(&task, 0, &selected),
            TSDB_CODE_STREAM_EXT_READER_NO_LOCAL_INFO);
  EXPECT_EQ(streamReaderExtGetCalcInfo(nullptr, 0, &selected), TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(streamReaderExtGetCalcInfo(&task, 0, nullptr), TSDB_CODE_INVALID_PARA);
  taosArrayDestroy(readers);
}

TEST_F(StreamReaderExtRangeTest, ConcurrentReplicasKeepIndependentWindowBounds) {
  info.pCalcTimeRange = makeTimeRange(true, OP_TYPE_GREATER_EQUAL, "_twstart", OP_TYPE_LOWER_THAN, "_twend");
  SStreamExtReaderInfo other = {};
  ASSERT_EQ(nodesCloneNode(info.pCalcTimeRange, &other.pCalcTimeRange), TSDB_CODE_SUCCESS);
  rt.pStreamPesudoFuncVals = makeParams({{1000, 2000}});
  SStreamRuntimeFuncInfo otherRuntime = {};
  otherRuntime.pStreamPesudoFuncVals = makeParams({{9000, 10000}});
  bool firstCorrect = true;
  bool secondCorrect = true;
  auto evaluate = [](SStreamExtReaderInfo *reader, SStreamRuntimeFuncInfo *runtime,
                     int64_t expectedStart, int64_t expectedEnd, bool *correct) {
    for (int32_t iteration = 0; iteration < 1000; ++iteration) {
      int64_t actualStart = 0;
      int64_t actualEnd = 0;
      if (streamReaderExtCalcFetchRange(reader, runtime, &actualStart, &actualEnd) != TSDB_CODE_SUCCESS ||
          actualStart != expectedStart || actualEnd != expectedEnd) {
        *correct = false;
        return;
      }
    }
  };
  std::thread firstThread(evaluate, &info, &rt, 1000, 1999, &firstCorrect);
  std::thread secondThread(evaluate, &other, &otherRuntime, 9000, 9999, &secondCorrect);
  firstThread.join();
  secondThread.join();
  EXPECT_TRUE(firstCorrect);
  EXPECT_TRUE(secondCorrect);
  taosArrayDestroy(otherRuntime.pStreamPesudoFuncVals);
  nodesDestroyNode(other.pCalcTimeRange);
}

std::vector<SExtSourceCfg> fetchedConfigs;
std::vector<std::string> fetchedSql;
int32_t returnedConnections = 0;

int32_t captureReaderConnection(const SExtSourceCfg *config, SExtConnectorHandle **handle) {
  fetchedConfigs.push_back(*config);
  *handle = reinterpret_cast<SExtConnectorHandle *>(&returnedConnections);
  return TSDB_CODE_SUCCESS;
}

void returnReaderConnection(SExtConnectorHandle *) { ++returnedConnections; }

int32_t captureReaderQuery(SExtConnectorHandle *, const SFederatedScanPhysiNode *, const char *,
                           const char *sql, SExtQueryHandle **, SExtConnectorError *) {
  fetchedSql.emplace_back(sql);
  return TSDB_CODE_EXT_REMOTE_INTERNAL;
}

TEST(StreamReaderExtConnectionTest, CrossDatabaseMysqlFetchSelectsMatchingConnectionDatabase) {
  Stub stub;
  stub.set(extConnectorOpen, captureReaderConnection);
  stub.set(extConnectorClose, returnReaderConnection);
  stub.set(extConnectorExecQuery, captureReaderQuery);
  fetchedConfigs.clear();
  fetchedSql.clear();
  returnedConnections = 0;
  SStreamTask task = {};
  SStreamExtReaderInfo reader = {};
  reader.pTask = &task;
  reader.borrowConn = true;
  reader.spec.sourceType = EXT_SOURCE_MYSQL;
  tstrncpy(reader.spec.sourceName, "shared_source", sizeof(reader.spec.sourceName));
  tstrncpy(reader.spec.extTable, "metrics", sizeof(reader.spec.extTable));
  for (const char *database : {"db_a", "db_b", "db_a"}) {
    tstrncpy(reader.spec.extDb, database, sizeof(reader.spec.extDb));
    SSDataBlock *block = nullptr;
    EXPECT_EQ(streamReaderExtFetchData(&reader, INT64_MIN, INT64_MAX, &block),
              TSDB_CODE_EXT_REMOTE_INTERNAL);
    EXPECT_EQ(block, nullptr);
  }
  ASSERT_EQ(fetchedConfigs.size(), 3);
  ASSERT_EQ(fetchedSql.size(), 3);
  EXPECT_EQ(returnedConnections, 3);
  for (const auto &config : fetchedConfigs) {
    EXPECT_STREQ(config.source_name, "shared_source");
  }
  EXPECT_STREQ(fetchedConfigs[0].default_database, "db_a");
  EXPECT_STREQ(fetchedConfigs[1].default_database, "db_b");
  EXPECT_STREQ(fetchedConfigs[2].default_database, "db_a");
  // extQuoteIdent always quotes identifiers: MySQL uses backticks.
  EXPECT_NE(fetchedSql[0].find("FROM `db_a`.`metrics` "), std::string::npos);
  EXPECT_NE(fetchedSql[1].find("FROM `db_b`.`metrics` "), std::string::npos);
  EXPECT_NE(fetchedSql[2].find("FROM `db_a`.`metrics` "), std::string::npos);
}

TEST(StreamReaderExtConnectionTest, DatabaseScopedProvidersKeepTheirConnectionDatabase) {
  Stub stub;
  stub.set(extConnectorOpen, captureReaderConnection);
  stub.set(extConnectorClose, returnReaderConnection);
  stub.set(extConnectorExecQuery, captureReaderQuery);
  fetchedConfigs.clear();
  fetchedSql.clear();
  returnedConnections = 0;
  SStreamTask task = {};
  SStreamExtReaderInfo reader = {};
  reader.pTask = &task;
  reader.borrowConn = true;
  tstrncpy(reader.spec.sourceName, "database_source", sizeof(reader.spec.sourceName));
  tstrncpy(reader.spec.extDb, "database_a", sizeof(reader.spec.extDb));
  tstrncpy(reader.spec.extSchema, "public", sizeof(reader.spec.extSchema));
  tstrncpy(reader.spec.extTable, "metrics", sizeof(reader.spec.extTable));
  for (EExtSourceType sourceType : {EXT_SOURCE_POSTGRESQL, EXT_SOURCE_INFLUXDB}) {
    reader.spec.sourceType = sourceType;
    SSDataBlock *block = nullptr;
    EXPECT_EQ(streamReaderExtFetchData(&reader, INT64_MIN, INT64_MAX, &block),
              TSDB_CODE_EXT_REMOTE_INTERNAL);
  }
  ASSERT_EQ(fetchedConfigs.size(), 2);
  ASSERT_EQ(fetchedSql.size(), 2);
  EXPECT_EQ(returnedConnections, 2);
  for (const auto &config : fetchedConfigs) {
    EXPECT_STREQ(config.default_database, "database_a");
  }
  // extQuoteIdent always quotes identifiers: PostgreSQL/InfluxDB use double quotes.
  EXPECT_NE(fetchedSql[0].find("FROM \"public\".\"metrics\" "), std::string::npos);
  EXPECT_NE(fetchedSql[1].find("FROM \"metrics\" "), std::string::npos);
}

const SNode *gPreparedScanExtraFilter = nullptr;
std::vector<std::pair<std::string, std::string>> gPreparedTagBindings;

int32_t capturePreparedScan(SExtConnectorHandle *, const SExtInfluxScanSpec *spec, char **sql) {
  gPreparedScanExtraFilter = spec != nullptr ? spec->pExtraFilter : nullptr;
  if (sql != nullptr) *sql = nullptr;
  return TSDB_CODE_EXT_REMOTE_INTERNAL;
}

SExtTableMeta *makeTimeOnlySchemaForTest(void) {
  auto *meta = static_cast<SExtTableMeta *>(taosMemoryCalloc(1, sizeof(SExtTableMeta)));
  auto *columns = static_cast<SExtColumnDef *>(taosMemoryCalloc(1, sizeof(SExtColumnDef)));
  if (meta == nullptr || columns == nullptr) {
    taosMemoryFree(meta);
    taosMemoryFree(columns);
    return nullptr;
  }
  tstrncpy(columns[0].colName, "time", sizeof(columns[0].colName));
  tstrncpy(columns[0].extTypeName, "Timestamp(Nanosecond, None)", sizeof(columns[0].extTypeName));
  columns[0].isPrimaryKey = true;
  meta->pCols = columns;
  meta->numOfCols = 1;
  return meta;
}

void freeTimeOnlySchemaForTest(SExtTableMeta *meta) {
  if (meta == nullptr) return;
  taosMemoryFree(meta->pCols);
  taosMemoryFree(meta);
}

int32_t returnTimeOnlySchema(SExtConnectorHandle *, const SExtTableNode *, SExtTableMeta **meta) {
  *meta = makeTimeOnlySchemaForTest();
  return *meta != nullptr ? TSDB_CODE_SUCCESS : TSDB_CODE_OUT_OF_MEMORY;
}

TEST(StreamReaderExtFilterTest, Influx18CalcFetchDoesNotApplyTriggerPrefilter) {
  Stub stub;
  stub.set(extConnectorOpen, captureReaderConnection);
  stub.set(extConnectorClose, returnReaderConnection);
  stub.set(extConnectorPrepareInfluxV1Scan, capturePreparedScan);
  stub.set(extConnectorGetTableSchema, returnTimeOnlySchema);
  stub.set(extConnectorFreeTableSchema, freeTimeOnlySchemaForTest);
  returnedConnections = 0;
  gPreparedScanExtraFilter = nullptr;

  SStreamTask task = {};
  SStreamExtReaderInfo reader = {};
  reader.pTask = &task;
  reader.borrowConn = true;
  reader.influxV1 = true;
  reader.spec.sourceType = EXT_SOURCE_MYSQL;
  reader.spec.precision = TSDB_TIME_PRECISION_NANO;
  tstrncpy(reader.spec.extDb, "db", sizeof(reader.spec.extDb));
  tstrncpy(reader.spec.extTable, "metrics", sizeof(reader.spec.extTable));

  // A calculation fetch refreshes the remote schema, so the cached one has to be heap allocated
  // and freed through the connector, exactly like the real reader's.
  reader.pInflux18Meta = makeTimeOnlySchemaForTest();
  ASSERT_NE(reader.pInflux18Meta, nullptr);

  SNode *triggerFilter = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_COLUMN, &triggerFilter), TSDB_CODE_SUCCESS);
  reader.pTriggerPrefilter = triggerFilter;

  SSDataBlock *block = nullptr;
  EXPECT_EQ(streamReaderExtFetchData(&reader, INT64_MIN, INT64_MAX, &block),
            TSDB_CODE_EXT_REMOTE_INTERNAL);
  EXPECT_EQ(block, nullptr);
  EXPECT_EQ(gPreparedScanExtraFilter, nullptr);
  EXPECT_EQ(returnedConnections, 1);

  nodesDestroyNode(triggerFilter);
  reader.pTriggerPrefilter = nullptr;
  freeTimeOnlySchemaForTest(reader.pInflux18Meta);
  reader.pInflux18Meta = nullptr;
  taosMemoryFree(reader.pInflux18Table);
}

int32_t capturePreparedTags(SExtConnectorHandle *, const SExtInfluxScanSpec *spec, char **sql) {
  gPreparedTagBindings.clear();
  if (spec != nullptr) {
    for (int32_t i = 0; i < spec->numTagBindings; ++i) {
      const SExtInfluxTagValue *binding = &spec->pTagBindings[i];
      gPreparedTagBindings.emplace_back(binding->pName != nullptr ? binding->pName : "",
                                        binding->pValue != nullptr ? binding->pValue : "");
    }
  }
  if (sql != nullptr) *sql = nullptr;
  return TSDB_CODE_EXT_REMOTE_INTERNAL;
}

TEST(StreamReaderExtFilterTest, Influx18TagsetRoundTripsEscapedAndEmptyIdentity) {
  Stub stub;
  stub.set(extConnectorOpen, captureReaderConnection);
  stub.set(extConnectorClose, returnReaderConnection);
  stub.set(extConnectorPrepareInfluxV1Scan, capturePreparedTags);
  returnedConnections = 0;

  SStreamTask task = {};
  SStreamExtReaderInfo reader = {};
  reader.pTask = &task;
  reader.influxV1 = true;
  reader.spec.sourceType = EXT_SOURCE_MYSQL;
  reader.spec.precision = TSDB_TIME_PRECISION_NANO;
  tstrncpy(reader.spec.extDb, "db", sizeof(reader.spec.extDb));
  tstrncpy(reader.spec.extTable, "metrics", sizeof(reader.spec.extTable));
  reader.pInfluxTagCols = taosArrayInit(2, EXT_INFLUX_KEY_NCHAR_CHARS);
  ASSERT_NE(reader.pInfluxTagCols, nullptr);
  char siteTag[EXT_INFLUX_KEY_NCHAR_CHARS] = "site";
  char zoneTag[EXT_INFLUX_KEY_NCHAR_CHARS] = "zone";
  ASSERT_NE(taosArrayPush(reader.pInfluxTagCols, siteTag), nullptr);
  ASSERT_NE(taosArrayPush(reader.pInfluxTagCols, zoneTag), nullptr);

  SExtColumnDef columns[3] = {};
  tstrncpy(columns[0].colName, "time", sizeof(columns[0].colName));
  tstrncpy(columns[0].extTypeName, "Timestamp(Nanosecond, None)", sizeof(columns[0].extTypeName));
  columns[0].isPrimaryKey = true;
  tstrncpy(columns[1].colName, "site", sizeof(columns[1].colName));
  tstrncpy(columns[1].extTypeName, "Utf8", sizeof(columns[1].extTypeName));
  columns[1].isTag = true;
  tstrncpy(columns[2].colName, "zone", sizeof(columns[2].colName));
  tstrncpy(columns[2].extTypeName, "Utf8", sizeof(columns[2].extTypeName));
  columns[2].isTag = true;
  SExtTableMeta meta = {};
  meta.pCols = columns;
  meta.numOfCols = 3;
  reader.pInflux18Meta = &meta;

  reader.pUidIndex = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(reader.pUidIndex, nullptr);
  uint64_t uid = 42;
  SUidIndexEntry entry = {};
  tstrncpy(entry.tagsetKey, "site=a\\|b|zone=c", sizeof(entry.tagsetKey));
  ASSERT_EQ(tSimpleHashPut(reader.pUidIndex, &uid, sizeof(uid), &entry, sizeof(entry)),
            TSDB_CODE_SUCCESS);

  SSTriggerExtPullReq request = {};
  request.pUidWindow = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(request.pUidWindow, nullptr);
  SExtUidWindow window = {.skey = 100, .ekey = 200};
  ASSERT_EQ(tSimpleHashPut(request.pUidWindow, &uid, sizeof(uid), &window, sizeof(window)),
            TSDB_CODE_SUCCESS);

  void *response = nullptr;
  EXPECT_EQ(streamReaderExtHandlePull(&reader, STRIGGER_PULL_DATA_EXT, &request, &response),
            TSDB_CODE_EXT_REMOTE_INTERNAL);
  ASSERT_EQ(gPreparedTagBindings.size(), 2u);
  EXPECT_EQ(gPreparedTagBindings[0], std::make_pair(std::string("site"), std::string("a|b")));
  EXPECT_EQ(gPreparedTagBindings[1], std::make_pair(std::string("zone"), std::string("c")));

  streamExtPullRspFree(static_cast<SSTriggerExtPullRsp *>(response));

  // An untagged series in a tagged measurement has an empty sparse identity.
  // The scan must still bind every known tag to empty so it cannot read peer
  // series from the same measurement.
  tSimpleHashClear(reader.pUidIndex);
  SUidIndexEntry emptyEntry = {};
  ASSERT_EQ(tSimpleHashPut(reader.pUidIndex, &uid, sizeof(uid), &emptyEntry, sizeof(emptyEntry)),
            TSDB_CODE_SUCCESS);
  response = nullptr;
  EXPECT_EQ(streamReaderExtHandlePull(&reader, STRIGGER_PULL_DATA_EXT, &request, &response),
            TSDB_CODE_EXT_REMOTE_INTERNAL);
  ASSERT_EQ(gPreparedTagBindings.size(), 2u);
  EXPECT_EQ(gPreparedTagBindings[0], std::make_pair(std::string("site"), std::string("")));
  EXPECT_EQ(gPreparedTagBindings[1], std::make_pair(std::string("zone"), std::string("")));

  streamExtPullRspFree(static_cast<SSTriggerExtPullRsp *>(response));
  tSimpleHashCleanup(request.pUidWindow);
  tSimpleHashCleanup(reader.pUidIndex);
  taosArrayDestroy(reader.pInfluxTagCols);
  reader.pInfluxTagCols = nullptr;
  reader.pInflux18Meta = nullptr;
  taosMemoryFree(reader.pInflux18Table);
}

int32_t failSeriesSnapshot(SExtConnectorHandle *, const SExtTableNode *, const SExtTableMeta *,
                           const char *const *, int32_t, const SNode *, const SNode *, bool,
                           int8_t, SSDataBlock **block) {
  if (block != nullptr) *block = nullptr;
  return TSDB_CODE_EXT_FETCH_FAILED;
}

TEST(StreamReaderExtFilterTest, Influx18InitDoesNotAdoptTagColumnsBeforeSnapshotSucceeds) {
  Stub stub;
  stub.set(extConnectorInfluxV1SeriesSnapshot, failSeriesSnapshot);

  SStreamTask task = {};
  SStreamExtReaderInfo reader = {};
  reader.pTask = &task;
  reader.influxV1 = true;
  reader.spec.sourceType = EXT_SOURCE_INFLUXDB;
  reader.spec.precision = TSDB_TIME_PRECISION_NANO;
  tstrncpy(reader.spec.extDb, "db", sizeof(reader.spec.extDb));
  tstrncpy(reader.spec.extTable, "metrics", sizeof(reader.spec.extTable));
  reader.pConn = reinterpret_cast<SExtConnectorHandle *>(0x1);

  SExtColumnDef columns[2] = {};
  tstrncpy(columns[0].colName, "time", sizeof(columns[0].colName));
  tstrncpy(columns[0].extTypeName, "Timestamp(Nanosecond, None)", sizeof(columns[0].extTypeName));
  columns[0].isPrimaryKey = true;
  tstrncpy(columns[1].colName, "site", sizeof(columns[1].colName));
  tstrncpy(columns[1].extTypeName, "Utf8", sizeof(columns[1].extTypeName));
  columns[1].isTag = true;
  SExtTableMeta meta = {};
  meta.pCols = columns;
  meta.numOfCols = 2;
  reader.pInflux18Meta = &meta;

  reader.pUidIndex = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(reader.pUidIndex, nullptr);
  reader.pGroupIndex = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(reader.pGroupIndex, nullptr);
  reader.pTagsetIndex = tSimpleHashInit(1, MurmurHash3_32);
  ASSERT_NE(reader.pTagsetIndex, nullptr);

  SSTriggerExtPullReq request = {};
  void *response = nullptr;
  EXPECT_EQ(streamReaderExtHandlePull(&reader, STRIGGER_PULL_LAST_TS_EXT, &request, &response),
            TSDB_CODE_EXT_FETCH_FAILED);
  EXPECT_EQ(reader.pInfluxTagCols, nullptr);

  streamExtPullRspFree(static_cast<SSTriggerExtPullRsp *>(response));
  tSimpleHashCleanup(reader.pUidIndex);
  tSimpleHashCleanup(reader.pGroupIndex);
  tSimpleHashCleanup(reader.pTagsetIndex);
  taosArrayDestroy(reader.pInfluxTagCols);
  reader.pInfluxTagCols = nullptr;
  reader.pInflux18Meta = nullptr;
  taosMemoryFree(reader.pInflux18Table);
}

int32_t gPrepareSchemaCalls = 0;
int32_t gGetSchemaCalls = 0;

SExtTableMeta *makeInflux18TestMeta() {
  auto *meta = static_cast<SExtTableMeta *>(taosMemoryCalloc(1, sizeof(SExtTableMeta)));
  auto *columns = static_cast<SExtColumnDef *>(taosMemoryCalloc(1, sizeof(SExtColumnDef)));
  if (meta == nullptr || columns == nullptr) {
    taosMemoryFree(meta);
    taosMemoryFree(columns);
    return nullptr;
  }
  tstrncpy(columns[0].colName, "time", sizeof(columns[0].colName));
  tstrncpy(columns[0].extTypeName, "Timestamp(Nanosecond, None)", sizeof(columns[0].extTypeName));
  columns[0].isPrimaryKey = true;
  meta->pCols = columns;
  meta->numOfCols = 1;
  return meta;
}

int32_t retryPreparedScan(SExtConnectorHandle *, const SExtInfluxScanSpec *, char **sql) {
  ++gPrepareSchemaCalls;
  if (sql != nullptr) *sql = nullptr;
  return gPrepareSchemaCalls == 1 ? TSDB_CODE_EXT_SCHEMA_CHANGED : TSDB_CODE_EXT_REMOTE_INTERNAL;
}

int32_t returnFreshSchema(SExtConnectorHandle *, const SExtTableNode *, SExtTableMeta **meta) {
  ++gGetSchemaCalls;
  *meta = makeInflux18TestMeta();
  return *meta != nullptr ? TSDB_CODE_SUCCESS : TSDB_CODE_OUT_OF_MEMORY;
}

void freeInflux18TestSchema(SExtTableMeta *meta) {
  if (meta == nullptr) return;
  taosMemoryFree(meta->pCols);
  taosMemoryFree(meta);
}

TEST(StreamReaderExtFilterTest, Influx18SchemaChangeRefreshesMetadataAndRetriesOnce) {
  Stub stub;
  stub.set(extConnectorOpen, captureReaderConnection);
  stub.set(extConnectorClose, returnReaderConnection);
  stub.set(extConnectorGetTableSchema, returnFreshSchema);
  stub.set(extConnectorFreeTableSchema, freeInflux18TestSchema);
  stub.set(extConnectorPrepareInfluxV1Scan, retryPreparedScan);
  returnedConnections = 0;
  gPrepareSchemaCalls = 0;
  gGetSchemaCalls = 0;

  SStreamTask task = {};
  SStreamExtReaderInfo reader = {};
  reader.pTask = &task;
  reader.borrowConn = true;
  reader.influxV1 = true;
  reader.spec.sourceType = EXT_SOURCE_MYSQL;
  reader.spec.precision = TSDB_TIME_PRECISION_NANO;
  tstrncpy(reader.spec.extDb, "db", sizeof(reader.spec.extDb));
  tstrncpy(reader.spec.extTable, "metrics", sizeof(reader.spec.extTable));
  reader.pInflux18Meta = makeInflux18TestMeta();
  ASSERT_NE(reader.pInflux18Meta, nullptr);

  SSDataBlock *block = nullptr;
  EXPECT_EQ(streamReaderExtFetchData(&reader, INT64_MIN, INT64_MAX, &block),
            TSDB_CODE_EXT_REMOTE_INTERNAL);
  EXPECT_EQ(block, nullptr);
  EXPECT_EQ(gPrepareSchemaCalls, 2);
  // One probe for the calculation refresh plus the retry after the reported schema change.
  EXPECT_EQ(gGetSchemaCalls, 2);
  EXPECT_NE(reader.pInflux18Meta, nullptr);

  freeInflux18TestSchema(reader.pInflux18Meta);
  reader.pInflux18Meta = nullptr;
  taosMemoryFree(reader.pInflux18Table);
}

int32_t gFirstTsPrepareCalls = 0;
int32_t gSeriesTimestampCalls = 0;
const SNode *gFirstTsExtraFilter = nullptr;

int32_t countFirstTsPrepare(SExtConnectorHandle *, const SExtInfluxScanSpec *spec, char **sql) {
  ++gFirstTsPrepareCalls;
  gFirstTsExtraFilter = spec != nullptr ? spec->pExtraFilter : nullptr;
  if (sql != nullptr) *sql = nullptr;
  return TSDB_CODE_SUCCESS;
}

int32_t failFirstTsExec(SExtConnectorHandle *, const SFederatedScanPhysiNode *, const char *,
                        const char *, SExtQueryHandle **, SExtConnectorError *) {
  return TSDB_CODE_EXT_REMOTE_INTERNAL;
}

int32_t returnSeriesTimestamps(SExtConnectorHandle *, const SExtTableNode *, const SExtTableMeta *,
                               const char *const *, int32_t, const SNode *, const SNode *, bool,
                               int8_t, SSDataBlock **block) {
  ++gSeriesTimestampCalls;
  *block = nullptr;
  int32_t code = createDataBlock(block);
  if (code != TSDB_CODE_SUCCESS) return code;
  SColumnInfoData tag = createColumnInfoData(TSDB_DATA_TYPE_NCHAR, EXT_INFLUX_TAG_NCHAR_CHARS, 1);
  SColumnInfoData time = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 2);
  time.info.precision = TSDB_TIME_PRECISION_NANO;
  code = blockDataAppendColInfo(*block, &tag);
  if (code == TSDB_CODE_SUCCESS) code = blockDataAppendColInfo(*block, &time);
  if (code == TSDB_CODE_SUCCESS) code = blockDataEnsureCapacity(*block, 2);
  if (code != TSDB_CODE_SUCCESS) return code;
  auto *tagColumn = static_cast<SColumnInfoData *>(taosArrayGet((*block)->pDataBlock, 0));
  auto *timeColumn = static_cast<SColumnInfoData *>(taosArrayGet((*block)->pDataBlock, 1));
  const char *tagValues[] = {"a", "b"};
  int64_t timestamps[] = {100, 200};
  for (int32_t row = 0; row < 2; ++row) {
    char tagValue[EXT_INFLUX_TAG_NCHAR_CHARS] = {0};
    varDataSetLen(tagValue, TSDB_NCHAR_SIZE);
    *reinterpret_cast<unsigned char *>(varDataVal(tagValue)) =
        static_cast<unsigned char>(tagValues[row][0]);
    code = colDataSetVal(tagColumn, row, tagValue, false);
    if (code != TSDB_CODE_SUCCESS) return code;
    code = colDataSetVal(timeColumn, row, reinterpret_cast<const char *>(&timestamps[row]), false);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  (*block)->info.rows = 2;
  return TSDB_CODE_SUCCESS;
}

int32_t returnSeriesTimestampsAfter(SExtConnectorHandle *handle, const SExtTableNode *table,
                                    const SExtTableMeta *meta, const char *const *tagNames,
                                    int32_t numTags, const SNode *seriesFilter,
                                    const SNode *rowFilter, bool first, int8_t precision,
                                    int64_t timeLower, SSDataBlock **block) {
  (void)timeLower;
  return returnSeriesTimestamps(handle, table, meta, tagNames, numTags, seriesFilter, rowFilter,
                                first, precision, block);
}

SExtTableMeta *makeInflux18Schema(int32_t numTags) {
  auto *meta = static_cast<SExtTableMeta *>(taosMemoryCalloc(1, sizeof(SExtTableMeta)));
  auto *columns =
      static_cast<SExtColumnDef *>(taosMemoryCalloc(1 + numTags, sizeof(SExtColumnDef)));
  if (meta == nullptr || columns == nullptr) {
    taosMemoryFree(meta);
    taosMemoryFree(columns);
    return nullptr;
  }
  const char *names[] = {"time", "site", "zone"};
  tstrncpy(columns[0].colName, names[0], sizeof(columns[0].colName));
  tstrncpy(columns[0].extTypeName, "Timestamp(Nanosecond, None)",
           sizeof(columns[0].extTypeName));
  columns[0].isPrimaryKey = true;
  for (int32_t i = 0; i < numTags; ++i) {
    tstrncpy(columns[i + 1].colName, names[i + 1], sizeof(columns[i + 1].colName));
    tstrncpy(columns[i + 1].extTypeName, "Utf8", sizeof(columns[i + 1].extTypeName));
    columns[i + 1].isTag = true;
  }
  meta->pCols = columns;
  meta->numOfCols = 1 + numTags;
  return meta;
}

int32_t returnTwoTagSchema(SExtConnectorHandle *, const SExtTableNode *, SExtTableMeta **meta) {
  *meta = makeInflux18Schema(2);
  return *meta != nullptr ? TSDB_CODE_SUCCESS : TSDB_CODE_OUT_OF_MEMORY;
}

int32_t makeSeriesSnapshotBlock(const char *const *tagNames, int32_t numTags,
                                SSDataBlock **block) {
  *block = nullptr;
  int32_t code = createDataBlock(block);
  if (code != TSDB_CODE_SUCCESS) return code;
  for (int32_t i = 0; i < numTags; ++i) {
    SColumnInfoData tag =
        createColumnInfoData(TSDB_DATA_TYPE_NCHAR, EXT_INFLUX_TAG_NCHAR_CHARS, (int16_t)(i + 1));
    code = blockDataAppendColInfo(*block, &tag);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  SColumnInfoData time =
      createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), (int16_t)(numTags + 1));
  time.info.precision = TSDB_TIME_PRECISION_NANO;
  code = blockDataAppendColInfo(*block, &time);
  if (code != TSDB_CODE_SUCCESS) return code;

  int32_t rows = numTags == 1 ? 2 : 1;
  code = blockDataEnsureCapacity(*block, rows);
  if (code != TSDB_CODE_SUCCESS) return code;
  for (int32_t row = 0; row < rows; ++row) {
    for (int32_t tag = 0; tag < numTags; ++tag) {
      char value[EXT_INFLUX_TAG_NCHAR_CHARS] = {0};
      varDataSetLen(value, TSDB_NCHAR_SIZE);
      *reinterpret_cast<unsigned char *>(varDataVal(value)) =
          static_cast<unsigned char>(tag == 0 ? 'a' + row : 'b');
      auto *column = static_cast<SColumnInfoData *>(taosArrayGet((*block)->pDataBlock, tag));
      code = colDataSetVal(column, row, value, false);
      if (code != TSDB_CODE_SUCCESS) return code;
    }
    int64_t timestamp = 100 + row * 100;
    auto *timeColumn =
        static_cast<SColumnInfoData *>(taosArrayGet((*block)->pDataBlock, numTags));
    code = colDataSetVal(timeColumn, row, reinterpret_cast<const char *>(&timestamp), false);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  (*block)->info.rows = rows;
  return TSDB_CODE_SUCCESS;
}

int32_t gSchemaChangeSnapshotCalls = 0;

int32_t schemaChangeThenSnapshot(SExtConnectorHandle *, const SExtTableNode *,
                                 const SExtTableMeta *, const char *const *tagNames,
                                 int32_t numTags, const SNode *, const SNode *, bool,
                                 int8_t, SSDataBlock **block) {
  if (gSchemaChangeSnapshotCalls++ == 0) {
    *block = nullptr;
    return TSDB_CODE_EXT_SCHEMA_CHANGED;
  }
  return makeSeriesSnapshotBlock(tagNames, numTags, block);
}

int32_t snapshotWithTwoTags(SExtConnectorHandle *, const SExtTableNode *, const SExtTableMeta *,
                            const char *const *tagNames, int32_t numTags, const SNode *,
                            const SNode *, bool, int8_t, SSDataBlock **block) {
  return makeSeriesSnapshotBlock(tagNames, numTags, block);
}

int32_t gDataPrepareCalls = 0;

int32_t prepareAlwaysChangesSchema(SExtConnectorHandle *, const SExtInfluxScanSpec *, char **sql) {
  ++gDataPrepareCalls;
  if (sql != nullptr) *sql = nullptr;
  return TSDB_CODE_EXT_SCHEMA_CHANGED;
}

void freeInflux18TestGroupIndexValue(void *value) {
  auto **uids = static_cast<SArray **>(value);
  taosArrayDestroy(*uids);
}

void cleanupInflux18Reader(SStreamExtReaderInfo *reader) {
  if (reader->pGroupIndex != nullptr) {
    tSimpleHashSetFreeFp(reader->pGroupIndex, freeInflux18TestGroupIndexValue);
  }
  tSimpleHashCleanup(reader->pUidIndex);
  tSimpleHashCleanup(reader->pGroupIndex);
  tSimpleHashCleanup(reader->pTagsetIndex);
  tSimpleHashCleanup(reader->pInflux18MetaEkey);
  taosArrayDestroy(reader->pInfluxTagCols);
  if (reader->pInflux18Meta != nullptr) {
    freeInflux18TestSchema(reader->pInflux18Meta);
  }
  taosMemoryFree(reader->pInflux18Table);
  reader->pUidIndex = nullptr;
  reader->pGroupIndex = nullptr;
  reader->pTagsetIndex = nullptr;
  reader->pInflux18MetaEkey = nullptr;
  reader->pInfluxTagCols = nullptr;
  reader->pInflux18Meta = nullptr;
  reader->pInflux18Table = nullptr;
}

TEST(StreamReaderExtFilterTest, Influx18BootstrapFetchesFirstTimestampsInOneSnapshot) {
  Stub stub;
  stub.set(extConnectorInfluxV1SeriesSnapshot, returnSeriesTimestamps);
  stub.set(extConnectorPrepareInfluxV1Scan, countFirstTsPrepare);
  stub.set(extConnectorExecQuery, failFirstTsExec);
  gFirstTsPrepareCalls = 0;

  SStreamTask task = {};
  SStreamExtReaderInfo reader = {};
  reader.pTask = &task;
  reader.influxV1 = true;
  reader.spec.sourceType = EXT_SOURCE_INFLUXDB;
  reader.spec.precision = TSDB_TIME_PRECISION_NANO;
  tstrncpy(reader.spec.extDb, "db", sizeof(reader.spec.extDb));
  tstrncpy(reader.spec.extTable, "metrics", sizeof(reader.spec.extTable));
  reader.pConn = reinterpret_cast<SExtConnectorHandle *>(0x1);
  reader.pInfluxTagCols = taosArrayInit(1, EXT_INFLUX_KEY_NCHAR_CHARS);
  ASSERT_NE(reader.pInfluxTagCols, nullptr);
  char tagName[EXT_INFLUX_KEY_NCHAR_CHARS] = "site";
  ASSERT_NE(taosArrayPush(reader.pInfluxTagCols, tagName), nullptr);

  SExtColumnDef columns[2] = {};
  tstrncpy(columns[0].colName, "time", sizeof(columns[0].colName));
  tstrncpy(columns[0].extTypeName, "Timestamp(Nanosecond, None)", sizeof(columns[0].extTypeName));
  columns[0].isPrimaryKey = true;
  tstrncpy(columns[1].colName, "site", sizeof(columns[1].colName));
  tstrncpy(columns[1].extTypeName, "Utf8", sizeof(columns[1].extTypeName));
  columns[1].isTag = true;
  SExtTableMeta meta = {};
  meta.pCols = columns;
  meta.numOfCols = 2;
  reader.pInflux18Meta = &meta;

  reader.pUidIndex = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(reader.pUidIndex, nullptr);
  const char *keys[] = {"site=a", "site=b"};
  uint64_t uids[2];
  for (int32_t i = 0; i < 2; ++i) {
    SUidIndexEntry entry = {};
    entry.groupId = (uint64_t)(i + 1);
    tstrncpy(entry.tagsetKey, keys[i], sizeof(entry.tagsetKey));
    uids[i] = MurmurHash3_64(keys[i], (int32_t)strlen(keys[i])) | 1ULL;
    ASSERT_EQ(tSimpleHashPut(reader.pUidIndex, &uids[i], sizeof(uids[i]), &entry, sizeof(entry)),
              TSDB_CODE_SUCCESS);
  }

  SSTriggerExtPullReq request = {};
  request.pUidMaxTs = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(request.pUidMaxTs, nullptr);
  int64_t noWatermark = INT64_MIN;
  for (uint64_t uid : uids) {
    ASSERT_EQ(tSimpleHashPut(request.pUidMaxTs, &uid, sizeof(uid), &noWatermark, sizeof(noWatermark)),
              TSDB_CODE_SUCCESS);
  }

  void *response = nullptr;
  int32_t code = streamReaderExtHandlePull(&reader, STRIGGER_PULL_META_EXT, &request, &response);
  EXPECT_EQ(gFirstTsPrepareCalls, 0);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  auto *rsp = static_cast<SSTriggerExtPullRsp *>(response);
  ASSERT_NE(rsp, nullptr);
  ASSERT_NE(rsp->pMetaBlock, nullptr);
  EXPECT_EQ(rsp->pMetaBlock->info.rows, 2);

  streamExtPullRspFree(rsp);
  tSimpleHashCleanup(request.pUidMaxTs);
  tSimpleHashCleanup(reader.pUidIndex);
  taosArrayDestroy(reader.pInfluxTagCols);
  reader.pInfluxTagCols = nullptr;
  reader.pInflux18Meta = nullptr;
  taosMemoryFree(reader.pInflux18Table);
}

TEST(StreamReaderExtFilterTest, Influx18MetaRegistersSeriesDiscoveredAfterInitialEnumeration) {
  Stub stub;
  stub.set(extConnectorInfluxV1SeriesSnapshot, returnSeriesTimestamps);
  stub.set(extConnectorPrepareInfluxV1Scan, countFirstTsPrepare);
  stub.set(extConnectorExecQuery, failFirstTsExec);
  gFirstTsPrepareCalls = 0;
  gSeriesTimestampCalls = 0;

  SStreamTask task = {};
  SStreamExtReaderInfo reader = {};
  reader.pTask = &task;
  reader.influxV1 = true;
  reader.spec.sourceType = EXT_SOURCE_INFLUXDB;
  reader.spec.precision = TSDB_TIME_PRECISION_NANO;
  tstrncpy(reader.spec.extDb, "db", sizeof(reader.spec.extDb));
  tstrncpy(reader.spec.extTable, "metrics", sizeof(reader.spec.extTable));
  reader.pConn = reinterpret_cast<SExtConnectorHandle *>(0x1);
  reader.pInflux18Meta = makeInflux18Schema(1);
  ASSERT_NE(reader.pInflux18Meta, nullptr);
  reader.pInfluxTagCols = taosArrayInit(1, EXT_INFLUX_KEY_NCHAR_CHARS);
  ASSERT_NE(reader.pInfluxTagCols, nullptr);
  char tagName[EXT_INFLUX_KEY_NCHAR_CHARS] = "site";
  ASSERT_NE(taosArrayPush(reader.pInfluxTagCols, tagName), nullptr);

  reader.pUidIndex = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  reader.pGroupIndex = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  reader.pTagsetIndex = tSimpleHashInit(2, MurmurHash3_32);
  reader.pInflux18MetaEkey =
      tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(reader.pUidIndex, nullptr);
  ASSERT_NE(reader.pGroupIndex, nullptr);
  ASSERT_NE(reader.pTagsetIndex, nullptr);
  ASSERT_NE(reader.pInflux18MetaEkey, nullptr);

  const char *initialKey = "site=a";
  uint64_t initialUid = MurmurHash3_64(initialKey, (int32_t)strlen(initialKey)) | 1ULL;
  SUidIndexEntry initialEntry = {};
  initialEntry.groupId = initialUid;
  tstrncpy(initialEntry.tagsetKey, initialKey, sizeof(initialEntry.tagsetKey));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tSimpleHashPut(reader.pUidIndex, &initialUid, sizeof(initialUid), &initialEntry,
                           sizeof(initialEntry)));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tSimpleHashPut(reader.pTagsetIndex, initialKey, (int32_t)strlen(initialKey), &initialUid,
                           sizeof(initialUid)));

  SSTriggerExtPullReq request = {};
  void *response = nullptr;
  EXPECT_EQ(streamReaderExtHandlePull(&reader, STRIGGER_PULL_META_EXT, &request, &response),
            TSDB_CODE_SUCCESS);
  const char *newKey = "site=b";
  uint64_t newUid = MurmurHash3_64(newKey, (int32_t)strlen(newKey)) | 1ULL;
  EXPECT_EQ(tSimpleHashGetSize(reader.pUidIndex), 2);
  EXPECT_NE(tSimpleHashGet(reader.pUidIndex, &newUid, sizeof(newUid)), nullptr);
  EXPECT_NE(tSimpleHashGet(reader.pTagsetIndex, newKey, (int32_t)strlen(newKey)), nullptr);
  auto *rsp = static_cast<SSTriggerExtPullRsp *>(response);
  ASSERT_NE(rsp, nullptr);
  ASSERT_NE(rsp->pMetaBlock, nullptr);
  EXPECT_EQ(rsp->pMetaBlock->info.rows, 2);
  EXPECT_EQ(gFirstTsPrepareCalls, 0);
  EXPECT_EQ(gSeriesTimestampCalls, 2);

  streamExtPullRspFree(rsp);
  cleanupInflux18Reader(&reader);
}

TEST(StreamReaderExtFilterTest, Influx18RestartUsesOneSnapshotForFirstTimestamps) {
  Stub stub;
  stub.set(extConnectorInfluxV1SeriesSnapshot, returnSeriesTimestamps);
  stub.set(extConnectorInfluxV1SeriesSnapshotAfter, returnSeriesTimestampsAfter);
  stub.set(extConnectorPrepareInfluxV1Scan, countFirstTsPrepare);
  stub.set(extConnectorExecQuery, failFirstTsExec);
  gFirstTsPrepareCalls = 0;
  gSeriesTimestampCalls = 0;

  SStreamTask task = {};
  SStreamExtReaderInfo reader = {};
  reader.pTask = &task;
  reader.influxV1 = true;
  reader.spec.sourceType = EXT_SOURCE_INFLUXDB;
  reader.spec.precision = TSDB_TIME_PRECISION_NANO;
  tstrncpy(reader.spec.extDb, "db", sizeof(reader.spec.extDb));
  tstrncpy(reader.spec.extTable, "metrics", sizeof(reader.spec.extTable));
  reader.pConn = reinterpret_cast<SExtConnectorHandle *>(0x1);
  reader.pInflux18Meta = makeInflux18Schema(1);
  ASSERT_NE(reader.pInflux18Meta, nullptr);
  reader.pInfluxTagCols = taosArrayInit(1, EXT_INFLUX_KEY_NCHAR_CHARS);
  ASSERT_NE(reader.pInfluxTagCols, nullptr);
  char tagName[EXT_INFLUX_KEY_NCHAR_CHARS] = "site";
  ASSERT_NE(taosArrayPush(reader.pInfluxTagCols, tagName), nullptr);

  reader.pUidIndex = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  reader.pGroupIndex = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  reader.pTagsetIndex = tSimpleHashInit(2, MurmurHash3_32);
  reader.pInflux18MetaEkey =
      tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(reader.pUidIndex, nullptr);
  ASSERT_NE(reader.pGroupIndex, nullptr);
  ASSERT_NE(reader.pTagsetIndex, nullptr);
  ASSERT_NE(reader.pInflux18MetaEkey, nullptr);

  const char *keys[] = {"site=a", "site=b"};
  uint64_t uids[2];
  for (int32_t i = 0; i < 2; ++i) {
    SUidIndexEntry entry = {};
    entry.groupId = (uint64_t)(i + 1);
    tstrncpy(entry.tagsetKey, keys[i], sizeof(entry.tagsetKey));
    uids[i] = MurmurHash3_64(keys[i], (int32_t)strlen(keys[i])) | 1ULL;
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              tSimpleHashPut(reader.pUidIndex, &uids[i], sizeof(uids[i]), &entry, sizeof(entry)));
  }

  SSTriggerExtPullReq request = {};
  request.pUidMaxTs = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(request.pUidMaxTs, nullptr);
  int64_t restartWatermark = 50;
  for (uint64_t uid : uids) {
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              tSimpleHashPut(request.pUidMaxTs, &uid, sizeof(uid), &restartWatermark,
                             sizeof(restartWatermark)));
  }

  void *response = nullptr;
  EXPECT_EQ(streamReaderExtHandlePull(&reader, STRIGGER_PULL_META_EXT, &request, &response),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(gFirstTsPrepareCalls, 0);
  EXPECT_EQ(gSeriesTimestampCalls, 2);
  auto *rsp = static_cast<SSTriggerExtPullRsp *>(response);
  ASSERT_NE(rsp, nullptr);
  ASSERT_NE(rsp->pMetaBlock, nullptr);
  EXPECT_EQ(rsp->pMetaBlock->info.rows, 2);

  streamExtPullRspFree(rsp);
  tSimpleHashCleanup(request.pUidMaxTs);
  cleanupInflux18Reader(&reader);
}

/* A 1.8 series leaves the tags it does not carry out of its identity, so "site=a" and
 * "zone=a" are two series whose tagset keys differ only by the tag name.  Their
 * sub-table names must differ as well, otherwise both groups write into one table. */
TEST(StreamReaderExtFilterTest, Influx18SparseTagsetKeepsDistinctTbnames) {
  SStreamTask task = {};
  SStreamExtReaderInfo reader = {};
  reader.pTask = &task;
  reader.influxV1 = true;
  reader.spec.sourceType = EXT_SOURCE_INFLUXDB;
  reader.spec.precision = TSDB_TIME_PRECISION_NANO;
  tstrncpy(reader.spec.extDb, "db", sizeof(reader.spec.extDb));
  tstrncpy(reader.spec.extTable, "metrics", sizeof(reader.spec.extTable));
  reader.spec.partitionByTbname = true;
  reader.spec.partitionTagCols = taosArrayInit(1, TSDB_COL_NAME_LEN);
  ASSERT_NE(reader.spec.partitionTagCols, nullptr);
  char tbnameCol[TSDB_COL_NAME_LEN] = INFLUXDB_PARTITION_BY_TBNAME;
  ASSERT_NE(taosArrayPush(reader.spec.partitionTagCols, tbnameCol), nullptr);

  reader.pInflux18Meta = makeInflux18Schema(2);  // time, site, zone
  ASSERT_NE(reader.pInflux18Meta, nullptr);
  reader.pInfluxTagCols = taosArrayInit(2, EXT_INFLUX_KEY_NCHAR_CHARS);
  ASSERT_NE(reader.pInfluxTagCols, nullptr);
  const char *tagNames[] = {"site", "zone"};
  for (int32_t i = 0; i < 2; ++i) {
    char nameBuf[EXT_INFLUX_KEY_NCHAR_CHARS] = {0};
    tstrncpy(nameBuf, tagNames[i], sizeof(nameBuf));
    ASSERT_NE(taosArrayPush(reader.pInfluxTagCols, nameBuf), nullptr);
  }

  reader.pUidIndex = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  reader.pGroupIndex = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  reader.pTagsetIndex = tSimpleHashInit(2, MurmurHash3_32);
  ASSERT_NE(reader.pUidIndex, nullptr);
  ASSERT_NE(reader.pGroupIndex, nullptr);
  ASSERT_NE(reader.pTagsetIndex, nullptr);

  /* "site=a" (sparse) and "site=site|zone=a" (complete) must not compose the same
   * name: with a value-only form for complete tagsets both would be "metrics_site_a".
   * The last four entries pin the escape rule: '_' is escaped as "~_" and '~' as
   * "~~", so a component never ends with the separator and every tuple has exactly
   * one name, including the leading/trailing-underscore case the doubled '_' form
   * still merged. */
  const int32_t numCases = 7;
  const char *keys[] = {"site=a",
                        "zone=a",
                        "site=site|zone=a",
                        "site=web|zone=cn_north",
                        "site=web_zone_cn_north",
                        "a_=b",
                        "a=_b"};
  const char *expected[] = {"metrics_site_a",
                            "metrics_zone_a",
                            "metrics_site_site_zone_a",
                            "metrics_site_web_zone_cn~_north",
                            "metrics_site_web~_zone~_cn~_north",
                            "metrics_a~__b",
                            "metrics_a_~_b"};
  for (int32_t i = 0; i < numCases; ++i) {
    uint64_t gid = (uint64_t)(i + 1);
    SUidIndexEntry entry = {};
    entry.groupId = gid;
    tstrncpy(entry.tagsetKey, keys[i], sizeof(entry.tagsetKey));
    entry.partitionValues = taosArrayInit(1, sizeof(SStreamGroupValue));
    ASSERT_NE(entry.partitionValues, nullptr);
    SStreamGroupValue cachedSlot = {.isNull = true};
    ASSERT_NE(taosArrayPush(entry.partitionValues, &cachedSlot), nullptr);
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              tSimpleHashPut(reader.pUidIndex, &gid, sizeof(gid), &entry, sizeof(entry)));

    SSTriggerExtPullReq request = {};
    request.gid = (int64_t)gid;
    void *response = nullptr;
    EXPECT_EQ(streamReaderExtHandlePull(&reader, STRIGGER_PULL_GROUP_COL_VALUE_EXT, &request,
                                        &response),
              TSDB_CODE_SUCCESS);
    auto *rsp = static_cast<SSTriggerExtPullRsp *>(response);
    ASSERT_NE(rsp, nullptr);
    ASSERT_NE(rsp->pGroupColVals, nullptr);
    ASSERT_EQ(taosArrayGetSize(rsp->pGroupColVals), 1u);
    auto *tbname = static_cast<SStreamGroupValue *>(taosArrayGet(rsp->pGroupColVals, 0));
    ASSERT_NE(tbname, nullptr);
    EXPECT_EQ(tbname->data.type, TSDB_DATA_TYPE_VARCHAR);
    EXPECT_EQ(std::string(reinterpret_cast<const char *>(tbname->data.pData),
                          static_cast<size_t>(tbname->data.nData)),
              std::string(expected[i]));
    streamExtPullRspFree(rsp);
  }

  for (int32_t i = 0; i < numCases; ++i) {
    uint64_t gid = (uint64_t)(i + 1);
    auto *entry = static_cast<SUidIndexEntry *>(tSimpleHashGet(reader.pUidIndex, &gid, sizeof(gid)));
    if (entry != nullptr) {
      taosArrayDestroy(entry->partitionValues);
      entry->partitionValues = nullptr;
    }
  }
  taosArrayDestroy(reader.spec.partitionTagCols);
  reader.spec.partitionTagCols = nullptr;
  reader.spec.partitionByTbname = false;
  cleanupInflux18Reader(&reader);
}

int32_t gPagingSnapshotRows = 0;
int32_t gPagingSnapshotCalls = 0;

/* One snapshot describing gPagingSnapshotRows series ("site=v00000" ..), each with a
 * distinct latest point so every row becomes a META candidate. */
int32_t returnManySeriesSnapshot(SExtConnectorHandle *, const SExtTableNode *, const SExtTableMeta *,
                                 const char *const *, int32_t, const SNode *, const SNode *, bool,
                                 int8_t, SSDataBlock **block) {
  ++gPagingSnapshotCalls;
  *block = nullptr;
  int32_t code = createDataBlock(block);
  if (code != TSDB_CODE_SUCCESS) return code;
  SColumnInfoData tag = createColumnInfoData(TSDB_DATA_TYPE_NCHAR, EXT_INFLUX_TAG_NCHAR_CHARS, 1);
  SColumnInfoData time = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 2);
  time.info.precision = TSDB_TIME_PRECISION_NANO;
  code = blockDataAppendColInfo(*block, &tag);
  if (code == TSDB_CODE_SUCCESS) code = blockDataAppendColInfo(*block, &time);
  if (code == TSDB_CODE_SUCCESS) code = blockDataEnsureCapacity(*block, (uint32_t)gPagingSnapshotRows);
  if (code != TSDB_CODE_SUCCESS) return code;
  auto *tagColumn = static_cast<SColumnInfoData *>(taosArrayGet((*block)->pDataBlock, 0));
  auto *timeColumn = static_cast<SColumnInfoData *>(taosArrayGet((*block)->pDataBlock, 1));
  for (int32_t row = 0; row < gPagingSnapshotRows; ++row) {
    char text[32] = {0};
    int32_t len = snprintf(text, sizeof(text), "v%05d", row);
    char tagValue[EXT_INFLUX_TAG_NCHAR_CHARS] = {0};
    varDataSetLen(tagValue, len * TSDB_NCHAR_SIZE);
    for (int32_t c = 0; c < len; ++c) {
      reinterpret_cast<unsigned char *>(varDataVal(tagValue))[c * TSDB_NCHAR_SIZE] =
          static_cast<unsigned char>(text[c]);
    }
    code = colDataSetVal(tagColumn, row, tagValue, false);
    if (code != TSDB_CODE_SUCCESS) return code;
    int64_t ts = 1000 + row;
    code = colDataSetVal(timeColumn, row, reinterpret_cast<const char *>(&ts), false);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  (*block)->info.rows = gPagingSnapshotRows;
  return TSDB_CODE_SUCCESS;
}

/* The trigger treats metaRows >= STREAM_RETURN_ROWS_NUM as "more pages" and re-pulls
 * META after adopting every reported watermark.  A reader that answers every pull with
 * the whole snapshot makes that follow-up pull empty, so the windows of the first
 * response are never fetched.  The reader must page instead. */
TEST(StreamReaderExtFilterTest, Influx18MetaPagesAtTheTriggerRowLimit) {
  Stub stub;
  stub.set(extConnectorInfluxV1SeriesSnapshot, returnManySeriesSnapshot);
  gPagingSnapshotRows = STREAM_RETURN_ROWS_NUM + 904;
  gPagingSnapshotCalls = 0;

  SStreamTask task = {};
  SStreamExtReaderInfo reader = {};
  reader.pTask = &task;
  reader.influxV1 = true;
  reader.spec.sourceType = EXT_SOURCE_INFLUXDB;
  reader.spec.precision = TSDB_TIME_PRECISION_NANO;
  tstrncpy(reader.spec.extDb, "db", sizeof(reader.spec.extDb));
  tstrncpy(reader.spec.extTable, "metrics", sizeof(reader.spec.extTable));
  reader.pConn = reinterpret_cast<SExtConnectorHandle *>(0x1);
  reader.pInflux18Meta = makeInflux18Schema(1);
  ASSERT_NE(reader.pInflux18Meta, nullptr);
  reader.pInfluxTagCols = taosArrayInit(1, EXT_INFLUX_KEY_NCHAR_CHARS);
  ASSERT_NE(reader.pInfluxTagCols, nullptr);
  char tagName[EXT_INFLUX_KEY_NCHAR_CHARS] = "site";
  ASSERT_NE(taosArrayPush(reader.pInfluxTagCols, tagName), nullptr);

  reader.pUidIndex = tSimpleHashInit(gPagingSnapshotRows, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  reader.pGroupIndex = tSimpleHashInit(gPagingSnapshotRows, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  reader.pTagsetIndex = tSimpleHashInit(gPagingSnapshotRows, MurmurHash3_32);
  reader.pInflux18MetaEkey = tSimpleHashInit(gPagingSnapshotRows, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(reader.pUidIndex, nullptr);
  ASSERT_NE(reader.pGroupIndex, nullptr);
  ASSERT_NE(reader.pTagsetIndex, nullptr);
  ASSERT_NE(reader.pInflux18MetaEkey, nullptr);

  for (int32_t i = 0; i < gPagingSnapshotRows; ++i) {
    char key[64] = {0};
    snprintf(key, sizeof(key), "site=v%05d", i);
    SUidIndexEntry entry = {};
    entry.groupId = (uint64_t)(i + 1);
    tstrncpy(entry.tagsetKey, key, sizeof(entry.tagsetKey));
    uint64_t uid = MurmurHash3_64(key, (int32_t)strlen(key)) | 1ULL;
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              tSimpleHashPut(reader.pUidIndex, &uid, sizeof(uid), &entry, sizeof(entry)));
  }

  SSTriggerExtPullReq request = {};
  request.pUidMaxTs = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(request.pUidMaxTs, nullptr);

  void *response = nullptr;
  EXPECT_EQ(streamReaderExtHandlePull(&reader, STRIGGER_PULL_META_EXT, &request, &response),
            TSDB_CODE_SUCCESS);
  auto *rsp = static_cast<SSTriggerExtPullRsp *>(response);
  ASSERT_NE(rsp, nullptr);
  ASSERT_NE(rsp->pMetaBlock, nullptr);
  EXPECT_EQ(rsp->pMetaBlock->info.rows, STREAM_RETURN_ROWS_NUM);
  // The follow-up pull continues where this page stopped instead of re-reading the snapshot.
  EXPECT_EQ(reader.influxBatchOffset, STREAM_RETURN_ROWS_NUM);
  streamExtPullRspFree(rsp);

  response = nullptr;
  EXPECT_EQ(streamReaderExtHandlePull(&reader, STRIGGER_PULL_META_EXT, &request, &response),
            TSDB_CODE_SUCCESS);
  rsp = static_cast<SSTriggerExtPullRsp *>(response);
  ASSERT_NE(rsp, nullptr);
  ASSERT_NE(rsp->pMetaBlock, nullptr);
  EXPECT_EQ(rsp->pMetaBlock->info.rows, 904);
  // Below the limit the enumeration is finished, so the next pull starts over.
  EXPECT_EQ(reader.influxBatchOffset, 0);
  streamExtPullRspFree(rsp);

  // Both pages came from one snapshot pair: one latest-point snapshot plus, because
  // every series is new here, one history-head snapshot.  Re-reading the
  // measurement per page would multiply the remote I/O by the page count.
  EXPECT_EQ(gPagingSnapshotCalls, 2);
  EXPECT_EQ(reader.pInflux18MetaSnap, nullptr);
  EXPECT_EQ(reader.pInflux18MetaFirstSnap, nullptr);

  tSimpleHashCleanup(request.pUidMaxTs);
  cleanupInflux18Reader(&reader);
}

/* Snapshot stub of a series that a multi-shard enumeration reports twice with a
 * different point of its own history: the merged response carries the later
 * point (200) and the earlier one (50) under the same tagset.  The FIRST
 * snapshot must keep 50, the LATEST snapshot 200. */
int32_t returnDuplicatedSeriesSnapshot(SExtConnectorHandle *, const SExtTableNode *,
                                       const SExtTableMeta *, const char *const *,
                                       int32_t, const SNode *, const SNode *, bool first, int8_t,
                                       SSDataBlock **block) {
  *block = nullptr;
  int32_t code = createDataBlock(block);
  if (code != TSDB_CODE_SUCCESS) return code;
  SColumnInfoData tag = createColumnInfoData(TSDB_DATA_TYPE_NCHAR, EXT_INFLUX_TAG_NCHAR_CHARS, 1);
  SColumnInfoData time = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 2);
  time.info.precision = TSDB_TIME_PRECISION_NANO;
  code = blockDataAppendColInfo(*block, &tag);
  if (code == TSDB_CODE_SUCCESS) code = blockDataAppendColInfo(*block, &time);
  if (code == TSDB_CODE_SUCCESS) code = blockDataEnsureCapacity(*block, 2);
  if (code != TSDB_CODE_SUCCESS) return code;
  auto *tagColumn = static_cast<SColumnInfoData *>(taosArrayGet((*block)->pDataBlock, 0));
  auto *timeColumn = static_cast<SColumnInfoData *>(taosArrayGet((*block)->pDataBlock, 1));
  char  tagValue[EXT_INFLUX_TAG_NCHAR_CHARS] = {0};
  varDataSetLen(tagValue, TSDB_NCHAR_SIZE);
  *reinterpret_cast<unsigned char *>(varDataVal(tagValue)) = static_cast<unsigned char>('a');
  int32_t nRows = first ? 2 : 1;
  for (int32_t row = 0; row < nRows; ++row) {
    code = colDataSetVal(tagColumn, row, tagValue, false);
    if (code != TSDB_CODE_SUCCESS) return code;
    // FIRST: later point first, then the real history head, like two shards that
    // report the series at different pages.  LATEST: only the newest point.
    int64_t ts = first ? (row == 0 ? 200 : 50) : 250;
    code = colDataSetVal(timeColumn, row, reinterpret_cast<const char *>(&ts), false);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  (*block)->info.rows = nRows;
  return TSDB_CODE_SUCCESS;
}

/* First META row of a series (watermark INT64_MIN): the trigger aligns its first
 * window at the reported skey, so a FIRST snapshot that kept the maximum of two
 * reported points would start the stream after data that is never fetched. */
TEST(StreamReaderExtFilterTest, Influx18FirstSnapshotKeepsEarliestPoint) {
  Stub stub;
  stub.set(extConnectorInfluxV1SeriesSnapshot, returnDuplicatedSeriesSnapshot);
  stub.set(extConnectorPrepareInfluxV1Scan, countFirstTsPrepare);
  gFirstTsPrepareCalls = 0;

  SStreamTask task = {};
  SStreamExtReaderInfo reader = {};
  reader.pTask = &task;
  reader.influxV1 = true;
  reader.spec.sourceType = EXT_SOURCE_INFLUXDB;
  reader.spec.precision = TSDB_TIME_PRECISION_NANO;
  tstrncpy(reader.spec.extDb, "db", sizeof(reader.spec.extDb));
  tstrncpy(reader.spec.extTable, "metrics", sizeof(reader.spec.extTable));
  reader.pConn = reinterpret_cast<SExtConnectorHandle *>(0x1);
  reader.pInflux18Meta = makeInflux18Schema(1);
  ASSERT_NE(reader.pInflux18Meta, nullptr);
  reader.pInfluxTagCols = taosArrayInit(1, EXT_INFLUX_KEY_NCHAR_CHARS);
  ASSERT_NE(reader.pInfluxTagCols, nullptr);
  char tagName[EXT_INFLUX_KEY_NCHAR_CHARS] = "site";
  ASSERT_NE(taosArrayPush(reader.pInfluxTagCols, tagName), nullptr);

  reader.pUidIndex = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  reader.pGroupIndex = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  reader.pTagsetIndex = tSimpleHashInit(2, MurmurHash3_32);
  reader.pInflux18MetaEkey = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(reader.pUidIndex, nullptr);
  ASSERT_NE(reader.pGroupIndex, nullptr);
  ASSERT_NE(reader.pTagsetIndex, nullptr);
  ASSERT_NE(reader.pInflux18MetaEkey, nullptr);

  const char *key = "site=a";
  uint64_t    uid = MurmurHash3_64(key, (int32_t)strlen(key)) | 1ULL;
  SUidIndexEntry entry = {};
  entry.groupId = 3;
  tstrncpy(entry.tagsetKey, key, sizeof(entry.tagsetKey));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tSimpleHashPut(reader.pUidIndex, &uid, sizeof(uid), &entry, sizeof(entry)));

  SSTriggerExtPullReq request = {};
  request.pUidMaxTs = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(request.pUidMaxTs, nullptr);

  void *response = nullptr;
  EXPECT_EQ(streamReaderExtHandlePull(&reader, STRIGGER_PULL_META_EXT, &request, &response),
            TSDB_CODE_SUCCESS);
  auto *rsp = static_cast<SSTriggerExtPullRsp *>(response);
  ASSERT_NE(rsp, nullptr);
  ASSERT_NE(rsp->pMetaBlock, nullptr);
  ASSERT_EQ(rsp->pMetaBlock->info.rows, 1);

  // meta block column 1 is skey (0=groupId, 1=skey, 2=ekey, 3=uid, 4=rows).
  SColumnInfoData *pSkey = static_cast<SColumnInfoData *>(taosArrayGet(rsp->pMetaBlock->pDataBlock, 1));
  ASSERT_NE(pSkey, nullptr);
  EXPECT_EQ(*(int64_t *)colDataGetData(pSkey, 0), 50);
  // The batched history head was usable, so no per-uid first-point query is made.
  EXPECT_EQ(gFirstTsPrepareCalls, 0);

  streamExtPullRspFree(rsp);
  tSimpleHashCleanup(request.pUidMaxTs);
  cleanupInflux18Reader(&reader);
}

int32_t gLatestVsFirstSnapshotCalls = 0;
int32_t gFirstTsFetchCalls = 0;
const SNode *gLatestSnapshotSeriesFilter = nullptr;
const SNode *gLatestSnapshotRowFilter = nullptr;
const SNode *gFirstSnapshotSeriesFilter = nullptr;
const SNode *gFirstSnapshotRowFilter = nullptr;

/* Series snapshot stub where the latest point (200) is newer than the
 * watermark (100) but the history head (50) is older.  This is exactly the
 * shape of a stream whose pre-CREATE-STREAM stale batch predates the watermark:
 * the batched first snapshot must not be trusted as the first new timestamp. */
int32_t returnLatestVsFirstTimestamp(SExtConnectorHandle *, const SExtTableNode *,
                                     const SExtTableMeta *, const char *const *,
                                     int32_t, const SNode *seriesFilter, const SNode *rowFilter,
                                     bool first, int8_t,
                                     SSDataBlock **block) {
  ++gLatestVsFirstSnapshotCalls;
  if (first) {
    gFirstSnapshotSeriesFilter = seriesFilter;
    gFirstSnapshotRowFilter = rowFilter;
  } else {
    gLatestSnapshotSeriesFilter = seriesFilter;
    gLatestSnapshotRowFilter = rowFilter;
  }
  *block = nullptr;
  int32_t code = createDataBlock(block);
  if (code != TSDB_CODE_SUCCESS) return code;
  SColumnInfoData tag = createColumnInfoData(TSDB_DATA_TYPE_NCHAR, EXT_INFLUX_TAG_NCHAR_CHARS, 1);
  SColumnInfoData time = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 1);
  time.info.precision = TSDB_TIME_PRECISION_NANO;
  code = blockDataAppendColInfo(*block, &tag);
  if (code == TSDB_CODE_SUCCESS) code = blockDataAppendColInfo(*block, &time);
  if (code == TSDB_CODE_SUCCESS) code = blockDataEnsureCapacity(*block, 1);
  if (code != TSDB_CODE_SUCCESS) return code;
  auto *tagColumn = static_cast<SColumnInfoData *>(taosArrayGet((*block)->pDataBlock, 0));
  auto *timeColumn = static_cast<SColumnInfoData *>(taosArrayGet((*block)->pDataBlock, 1));
  char tagValue[EXT_INFLUX_TAG_NCHAR_CHARS] = {0};
  varDataSetLen(tagValue, TSDB_NCHAR_SIZE);
  *reinterpret_cast<unsigned char *>(varDataVal(tagValue)) = static_cast<unsigned char>('a');
  code = colDataSetVal(tagColumn, 0, tagValue, false);
  if (code != TSDB_CODE_SUCCESS) return code;
  int64_t ts = first ? 50 : 200;
  code = colDataSetVal(timeColumn, 0, reinterpret_cast<const char *>(&ts), false);
  if (code != TSDB_CODE_SUCCESS) return code;
  (*block)->info.rows = 1;
  return TSDB_CODE_SUCCESS;
}

int32_t returnFirstAfterWatermark(SExtConnectorHandle *handle, const SExtTableNode *table,
                                  const SExtTableMeta *meta, const char *const *tagNames,
                                  int32_t numTags, const SNode *seriesFilter,
                                  const SNode *rowFilter, bool first, int8_t precision,
                                  int64_t timeLower, SSDataBlock **block) {
  (void)timeLower;
  return returnLatestVsFirstTimestamp(handle, table, meta, tagNames, numTags, seriesFilter,
                                      rowFilter, first, precision, block);
}

int32_t succeedFirstTsExec(SExtConnectorHandle *, const SFederatedScanPhysiNode *, const char *,
                           const char *, SExtQueryHandle **handle, SExtConnectorError *) {
  *handle = reinterpret_cast<SExtQueryHandle *>(0x1);
  return TSDB_CODE_SUCCESS;
}

/* Return the real first point newer than the watermark (150 > 100). */
int32_t returnFirstTsBlock(SExtQueryHandle *, const SExtColTypeMapping *, int32_t,
                           SSDataBlock **block, SExtConnectorError *) {
  ++gFirstTsFetchCalls;
  *block = nullptr;
  int32_t code = createDataBlock(block);
  if (code != TSDB_CODE_SUCCESS) return code;
  SColumnInfoData time = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 1);
  time.info.precision = TSDB_TIME_PRECISION_NANO;
  code = blockDataAppendColInfo(*block, &time);
  if (code == TSDB_CODE_SUCCESS) code = blockDataEnsureCapacity(*block, 1);
  if (code != TSDB_CODE_SUCCESS) return code;
  auto *timeColumn = static_cast<SColumnInfoData *>(taosArrayGet((*block)->pDataBlock, 0));
  int64_t ts = 150;
  code = colDataSetVal(timeColumn, 0, reinterpret_cast<const char *>(&ts), false);
  if (code != TSDB_CODE_SUCCESS) return code;
  (*block)->info.rows = 1;
  return TSDB_CODE_SUCCESS;
}

void noopCloseQuery(SExtQueryHandle *) {}

TEST(StreamReaderExtFilterTest, Influx18MetaSkipsHistoryHeadWhenWatermarkSet) {
  Stub stub;
  stub.set(extConnectorInfluxV1SeriesSnapshot, returnLatestVsFirstTimestamp);
  stub.set(extConnectorInfluxV1SeriesSnapshotAfter, returnFirstAfterWatermark);
  stub.set(extConnectorPrepareInfluxV1Scan, countFirstTsPrepare);
  stub.set(extConnectorExecQuery, succeedFirstTsExec);
  stub.set(extConnectorFetchBlock, returnFirstTsBlock);
  stub.set(extConnectorCloseQuery, noopCloseQuery);
  gFirstTsPrepareCalls = 0;
  gFirstTsFetchCalls = 0;
  gLatestVsFirstSnapshotCalls = 0;
  gLatestSnapshotSeriesFilter = nullptr;
  gLatestSnapshotRowFilter = nullptr;
  gFirstSnapshotSeriesFilter = nullptr;
  gFirstSnapshotRowFilter = nullptr;
  gFirstTsExtraFilter = nullptr;

  SStreamTask task = {};
  SStreamExtReaderInfo reader = {};
  reader.pTask = &task;
  reader.influxV1 = true;
  reader.spec.sourceType = EXT_SOURCE_INFLUXDB;
  reader.spec.precision = TSDB_TIME_PRECISION_NANO;
  tstrncpy(reader.spec.extDb, "db", sizeof(reader.spec.extDb));
  tstrncpy(reader.spec.extTable, "metrics", sizeof(reader.spec.extTable));
  reader.pConn = reinterpret_cast<SExtConnectorHandle *>(0x1);
  reader.pInflux18Meta = makeInflux18Schema(1);
  ASSERT_NE(reader.pInflux18Meta, nullptr);
  reader.pInfluxTagCols = taosArrayInit(1, EXT_INFLUX_KEY_NCHAR_CHARS);
  ASSERT_NE(reader.pInfluxTagCols, nullptr);
  char tagName[EXT_INFLUX_KEY_NCHAR_CHARS] = "site";
  ASSERT_NE(taosArrayPush(reader.pInfluxTagCols, tagName), nullptr);

  reader.pUidIndex = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  reader.pGroupIndex = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  reader.pTagsetIndex = tSimpleHashInit(2, MurmurHash3_32);
  reader.pInflux18MetaEkey =
      tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(reader.pUidIndex, nullptr);
  ASSERT_NE(reader.pGroupIndex, nullptr);
  ASSERT_NE(reader.pTagsetIndex, nullptr);
  ASSERT_NE(reader.pInflux18MetaEkey, nullptr);
  SNode *triggerFilter = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_COLUMN, &triggerFilter), TSDB_CODE_SUCCESS);
  reader.pTriggerPrefilter = triggerFilter;

  const char *key = "site=a";
  uint64_t uid = MurmurHash3_64(key, (int32_t)strlen(key)) | 1ULL;
  SUidIndexEntry entry = {};
  entry.groupId = 7;
  tstrncpy(entry.tagsetKey, key, sizeof(entry.tagsetKey));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tSimpleHashPut(reader.pUidIndex, &uid, sizeof(uid), &entry, sizeof(entry)));

  SSTriggerExtPullReq request = {};
  request.pUidMaxTs = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(request.pUidMaxTs, nullptr);
  int64_t watermark = 100;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tSimpleHashPut(request.pUidMaxTs, &uid, sizeof(uid), &watermark, sizeof(watermark)));

  void *response = nullptr;
  EXPECT_EQ(streamReaderExtHandlePull(&reader, STRIGGER_PULL_META_EXT, &request, &response),
            TSDB_CODE_SUCCESS);
  auto *rsp = static_cast<SSTriggerExtPullRsp *>(response);
  ASSERT_NE(rsp, nullptr);
  ASSERT_NE(rsp->pMetaBlock, nullptr);
  ASSERT_EQ(rsp->pMetaBlock->info.rows, 1);

  // The batched first snapshot reports the history head (50), which is older
  // than the watermark (100).  The reader must fall back to the per-uid first
  // timestamp query (150) instead of synthesizing wm+1 (101), which would open
  // an extra empty window before the data.
  EXPECT_GE(gLatestVsFirstSnapshotCalls, 2);  // latest snapshot + first snapshot
  EXPECT_EQ(gLatestSnapshotRowFilter, triggerFilter);
  EXPECT_EQ(gFirstSnapshotRowFilter, triggerFilter);
  EXPECT_EQ(gFirstTsExtraFilter, triggerFilter);
  EXPECT_EQ(gFirstTsPrepareCalls, 1);
  EXPECT_EQ(gFirstTsFetchCalls, 1);
  // meta block column 1 is skey (0=groupId, 1=skey, 2=ekey, 3=uid, 4=rows).
  SColumnInfoData *pSkey =
      static_cast<SColumnInfoData *>(taosArrayGet(rsp->pMetaBlock->pDataBlock, 1));
  ASSERT_NE(pSkey, nullptr);
  int64_t skey = *(int64_t *)colDataGetData(pSkey, 0);
  EXPECT_EQ(skey, 150);

  streamExtPullRspFree(rsp);
  nodesDestroyNode(triggerFilter);
  reader.pTriggerPrefilter = nullptr;
  tSimpleHashCleanup(request.pUidMaxTs);
  cleanupInflux18Reader(&reader);
}

int32_t gRestartLatestCalls = 0;
int32_t gRestartFirstAfterCalls = 0;
int64_t gRestartFirstAfterLower = INT64_MIN;

static int32_t makeTwoSeriesTimeBlock(int64_t timestamp, SSDataBlock **block) {
  *block = nullptr;
  int32_t code = createDataBlock(block);
  if (code != TSDB_CODE_SUCCESS) return code;
  SColumnInfoData tag = createColumnInfoData(TSDB_DATA_TYPE_NCHAR, EXT_INFLUX_TAG_NCHAR_CHARS, 1);
  SColumnInfoData time = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 2);
  time.info.precision = TSDB_TIME_PRECISION_NANO;
  code = blockDataAppendColInfo(*block, &tag);
  if (code == TSDB_CODE_SUCCESS) code = blockDataAppendColInfo(*block, &time);
  if (code == TSDB_CODE_SUCCESS) code = blockDataEnsureCapacity(*block, 2);
  if (code != TSDB_CODE_SUCCESS) return code;
  auto *tagColumn = static_cast<SColumnInfoData *>(taosArrayGet((*block)->pDataBlock, 0));
  auto *timeColumn = static_cast<SColumnInfoData *>(taosArrayGet((*block)->pDataBlock, 1));
  const char *values[] = {"a", "b"};
  for (int32_t row = 0; row < 2; ++row) {
    char tagValue[EXT_INFLUX_TAG_NCHAR_CHARS] = {0};
    varDataSetLen(tagValue, TSDB_NCHAR_SIZE);
    *reinterpret_cast<unsigned char *>(varDataVal(tagValue)) =
        static_cast<unsigned char>(values[row][0]);
    code = colDataSetVal(tagColumn, row, tagValue, false);
    if (code == TSDB_CODE_SUCCESS) {
      code = colDataSetVal(timeColumn, row, reinterpret_cast<const char *>(&timestamp), false);
    }
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  (*block)->info.rows = 2;
  return TSDB_CODE_SUCCESS;
}

int32_t returnTwoSeriesLatest(SExtConnectorHandle *, const SExtTableNode *,
                              const SExtTableMeta *, const char *const *, int32_t,
                              const SNode *, const SNode *, bool, int8_t, SSDataBlock **block) {
  ++gRestartLatestCalls;
  return makeTwoSeriesTimeBlock(200, block);
}

int32_t returnTwoSeriesFirstAfter(SExtConnectorHandle *, const SExtTableNode *,
                                  const SExtTableMeta *, const char *const *, int32_t,
                                  const SNode *, const SNode *, bool, int8_t, int64_t timeLower,
                                  SSDataBlock **block) {
  ++gRestartFirstAfterCalls;
  gRestartFirstAfterLower = timeLower;
  return makeTwoSeriesTimeBlock(150, block);
}

TEST(StreamReaderExtFilterTest, Influx18RestartBatchesFirstPointProbesAtCommonWatermark) {
  Stub stub;
  stub.set(extConnectorInfluxV1SeriesSnapshot, returnTwoSeriesLatest);
  stub.set(extConnectorInfluxV1SeriesSnapshotAfter, returnTwoSeriesFirstAfter);
  stub.set(extConnectorPrepareInfluxV1Scan, countFirstTsPrepare);
  gRestartLatestCalls = 0;
  gRestartFirstAfterCalls = 0;
  gRestartFirstAfterLower = INT64_MIN;
  gFirstTsPrepareCalls = 0;

  SStreamTask task = {};
  SStreamExtReaderInfo reader = {};
  reader.pTask = &task;
  reader.influxV1 = true;
  reader.spec.sourceType = EXT_SOURCE_INFLUXDB;
  reader.spec.precision = TSDB_TIME_PRECISION_NANO;
  tstrncpy(reader.spec.extDb, "db", sizeof(reader.spec.extDb));
  tstrncpy(reader.spec.extTable, "metrics", sizeof(reader.spec.extTable));
  reader.pConn = reinterpret_cast<SExtConnectorHandle *>(0x1);
  reader.pInflux18Meta = makeInflux18Schema(1);
  ASSERT_NE(reader.pInflux18Meta, nullptr);
  reader.pInfluxTagCols = taosArrayInit(1, EXT_INFLUX_KEY_NCHAR_CHARS);
  ASSERT_NE(reader.pInfluxTagCols, nullptr);
  char tagName[EXT_INFLUX_KEY_NCHAR_CHARS] = "site";
  ASSERT_NE(taosArrayPush(reader.pInfluxTagCols, tagName), nullptr);

  reader.pUidIndex = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  reader.pGroupIndex = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  reader.pTagsetIndex = tSimpleHashInit(2, MurmurHash3_32);
  reader.pInflux18MetaEkey =
      tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(reader.pUidIndex, nullptr);
  ASSERT_NE(reader.pGroupIndex, nullptr);
  ASSERT_NE(reader.pTagsetIndex, nullptr);
  ASSERT_NE(reader.pInflux18MetaEkey, nullptr);

  SSTriggerExtPullReq request = {};
  request.pUidMaxTs = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(request.pUidMaxTs, nullptr);
  for (const char *value : {"a", "b"}) {
    char key[32] = {0};
    snprintf(key, sizeof(key), "site=%s", value);
    uint64_t uid = MurmurHash3_64(key, (int32_t)strlen(key)) | 1ULL;
    SUidIndexEntry entry = {};
    entry.groupId = uid;
    tstrncpy(entry.tagsetKey, key, sizeof(entry.tagsetKey));
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              tSimpleHashPut(reader.pUidIndex, &uid, sizeof(uid), &entry, sizeof(entry)));
    int64_t watermark = 100;
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              tSimpleHashPut(request.pUidMaxTs, &uid, sizeof(uid), &watermark,
                             sizeof(watermark)));
  }

  void *response = nullptr;
  EXPECT_EQ(streamReaderExtHandlePull(&reader, STRIGGER_PULL_META_EXT, &request, &response),
            TSDB_CODE_SUCCESS);
  auto *rsp = static_cast<SSTriggerExtPullRsp *>(response);
  ASSERT_NE(rsp, nullptr);
  ASSERT_NE(rsp->pMetaBlock, nullptr);
  EXPECT_EQ(rsp->pMetaBlock->info.rows, 2);
  EXPECT_EQ(gRestartLatestCalls, 1);
  EXPECT_EQ(gRestartFirstAfterCalls, 1);
  EXPECT_EQ(gRestartFirstAfterLower, 101);
  EXPECT_EQ(gFirstTsPrepareCalls, 0);
  auto *pSkey = static_cast<SColumnInfoData *>(taosArrayGet(rsp->pMetaBlock->pDataBlock, 1));
  ASSERT_NE(pSkey, nullptr);
  EXPECT_EQ(*(int64_t *)colDataGetData(pSkey, 0), 150);
  EXPECT_EQ(*(int64_t *)colDataGetData(pSkey, 1), 150);

  streamExtPullRspFree(rsp);
  tSimpleHashCleanup(request.pUidMaxTs);
  cleanupInflux18Reader(&reader);
}

TEST(StreamReaderExtFilterTest, Influx18SchemaChangeRebuildsSeriesIdentityBeforeRetry) {
  Stub stub;
  stub.set(extConnectorInfluxV1SeriesSnapshot, schemaChangeThenSnapshot);
  stub.set(extConnectorGetTableSchema, returnTwoTagSchema);
  stub.set(extConnectorFreeTableSchema, freeInflux18TestSchema);
  gSchemaChangeSnapshotCalls = 0;

  SStreamTask task = {};
  SStreamExtReaderInfo reader = {};
  reader.pTask = &task;
  reader.influxV1 = true;
  reader.spec.sourceType = EXT_SOURCE_INFLUXDB;
  reader.spec.precision = TSDB_TIME_PRECISION_NANO;
  tstrncpy(reader.spec.extDb, "db", sizeof(reader.spec.extDb));
  tstrncpy(reader.spec.extTable, "metrics", sizeof(reader.spec.extTable));
  reader.pConn = reinterpret_cast<SExtConnectorHandle *>(0x1);
  reader.pInflux18Meta = makeInflux18Schema(1);
  ASSERT_NE(reader.pInflux18Meta, nullptr);
  reader.pInfluxTagCols = taosArrayInit(1, EXT_INFLUX_KEY_NCHAR_CHARS);
  ASSERT_NE(reader.pInfluxTagCols, nullptr);
  char oldTag[EXT_INFLUX_KEY_NCHAR_CHARS] = "site";
  ASSERT_NE(taosArrayPush(reader.pInfluxTagCols, oldTag), nullptr);
  reader.pUidIndex = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  reader.pGroupIndex = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  reader.pTagsetIndex = tSimpleHashInit(1, MurmurHash3_32);
  reader.pInflux18MetaEkey =
      tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(reader.pUidIndex, nullptr);
  ASSERT_NE(reader.pGroupIndex, nullptr);
  ASSERT_NE(reader.pTagsetIndex, nullptr);
  ASSERT_NE(reader.pInflux18MetaEkey, nullptr);

  const char *oldKey = "site=a";
  uint64_t oldUid = MurmurHash3_64(oldKey, (int32_t)strlen(oldKey)) | 1ULL;
  SUidIndexEntry oldEntry = {};
  oldEntry.groupId = oldUid;
  tstrncpy(oldEntry.tagsetKey, oldKey, sizeof(oldEntry.tagsetKey));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tSimpleHashPut(reader.pUidIndex, &oldUid, sizeof(oldUid), &oldEntry,
                           sizeof(oldEntry)));

  SSTriggerExtPullReq request = {};
  void *response = nullptr;
  EXPECT_EQ(streamReaderExtHandlePull(&reader, STRIGGER_PULL_LAST_TS_EXT, &request, &response),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(reader.pInfluxTagCols, nullptr);
  EXPECT_EQ(taosArrayGetSize(reader.pInfluxTagCols), 2);
  EXPECT_EQ(tSimpleHashGetSize(reader.pUidIndex), 1);
  const char *newKey = "site=a|zone=b";
  uint64_t newUid = MurmurHash3_64(newKey, (int32_t)strlen(newKey)) | 1ULL;
  EXPECT_NE(tSimpleHashGet(reader.pUidIndex, &newUid, sizeof(newUid)), nullptr);
  EXPECT_EQ(tSimpleHashGet(reader.pUidIndex, &oldUid, sizeof(oldUid)), nullptr);
  EXPECT_EQ(gSchemaChangeSnapshotCalls, 3);

  streamExtPullRspFree(static_cast<SSTriggerExtPullRsp *>(response));
  cleanupInflux18Reader(&reader);
}

TEST(StreamReaderExtFilterTest, Influx18DataSchemaChangeDoesNotReuseStaleUid) {
  Stub stub;
  stub.set(extConnectorInfluxV1SeriesSnapshot, snapshotWithTwoTags);
  stub.set(extConnectorGetTableSchema, returnTwoTagSchema);
  stub.set(extConnectorFreeTableSchema, freeInflux18TestSchema);
  stub.set(extConnectorPrepareInfluxV1Scan, prepareAlwaysChangesSchema);
  gDataPrepareCalls = 0;

  SStreamTask task = {};
  SStreamExtReaderInfo reader = {};
  reader.pTask = &task;
  reader.influxV1 = true;
  reader.spec.sourceType = EXT_SOURCE_INFLUXDB;
  reader.spec.precision = TSDB_TIME_PRECISION_NANO;
  tstrncpy(reader.spec.extDb, "db", sizeof(reader.spec.extDb));
  tstrncpy(reader.spec.extTable, "metrics", sizeof(reader.spec.extTable));
  reader.pConn = reinterpret_cast<SExtConnectorHandle *>(0x1);
  reader.pInflux18Meta = makeInflux18Schema(1);
  ASSERT_NE(reader.pInflux18Meta, nullptr);
  reader.pInfluxTagCols = taosArrayInit(1, EXT_INFLUX_KEY_NCHAR_CHARS);
  ASSERT_NE(reader.pInfluxTagCols, nullptr);
  char oldTag[EXT_INFLUX_KEY_NCHAR_CHARS] = "site";
  ASSERT_NE(taosArrayPush(reader.pInfluxTagCols, oldTag), nullptr);
  reader.pUidIndex = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  reader.pGroupIndex = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  reader.pTagsetIndex = tSimpleHashInit(1, MurmurHash3_32);
  reader.pInflux18MetaEkey =
      tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(reader.pUidIndex, nullptr);
  ASSERT_NE(reader.pGroupIndex, nullptr);
  ASSERT_NE(reader.pTagsetIndex, nullptr);
  ASSERT_NE(reader.pInflux18MetaEkey, nullptr);

  const char *oldKey = "site=a";
  uint64_t oldUid = MurmurHash3_64(oldKey, (int32_t)strlen(oldKey)) | 1ULL;
  SUidIndexEntry oldEntry = {};
  oldEntry.groupId = oldUid;
  tstrncpy(oldEntry.tagsetKey, oldKey, sizeof(oldEntry.tagsetKey));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tSimpleHashPut(reader.pUidIndex, &oldUid, sizeof(oldUid), &oldEntry,
                           sizeof(oldEntry)));

  SSTriggerExtPullReq request = {};
  request.pUidWindow = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(request.pUidWindow, nullptr);
  SExtUidWindow window = {.skey = 100, .ekey = 200};
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tSimpleHashPut(request.pUidWindow, &oldUid, sizeof(oldUid), &window, sizeof(window)));

  void *response = nullptr;
  EXPECT_EQ(streamReaderExtHandlePull(&reader, STRIGGER_PULL_DATA_EXT, &request, &response),
            TSDB_CODE_EXT_SCHEMA_CHANGED);
  EXPECT_EQ(response, nullptr);
  EXPECT_EQ(gDataPrepareCalls, 1);
  EXPECT_EQ(tSimpleHashGetSize(reader.pUidIndex), 0);

  streamExtPullRspFree(static_cast<SSTriggerExtPullRsp *>(response));
  tSimpleHashCleanup(request.pUidWindow);
  cleanupInflux18Reader(&reader);
}

const SNode *gEnumerationSeriesFilter = nullptr;
const SNode *gEnumerationRowFilter = nullptr;
bool        gEnumerationFirst = false;
int32_t     gEnumerationSnapshotCalls = 0;

int32_t captureEnumerationSnapshot(SExtConnectorHandle *, const SExtTableNode *,
                                   const SExtTableMeta *, const char *const *, int32_t,
                                   const SNode *seriesFilter, const SNode *rowFilter, bool first,
                                   int8_t, SSDataBlock **block) {
  if (gEnumerationSnapshotCalls++ == 0) {
    gEnumerationSeriesFilter = seriesFilter;
    gEnumerationRowFilter = rowFilter;
    gEnumerationFirst = first;
  }
  if (block != nullptr) *block = nullptr;
  return TSDB_CODE_SUCCESS;
}

TEST(StreamReaderExtFilterTest, Influx18SeriesEnumerationUsesOnlySeriesIdentityFilter) {
  Stub stub;
  stub.set(extConnectorInfluxV1SeriesSnapshot, captureEnumerationSnapshot);
  gEnumerationSeriesFilter = nullptr;
  gEnumerationRowFilter = nullptr;
  gEnumerationFirst = true;
  gEnumerationSnapshotCalls = 0;

  SStreamTask task = {};
  SStreamExtReaderInfo reader = {};
  reader.pTask = &task;
  reader.influxV1 = true;
  reader.spec.sourceType = EXT_SOURCE_INFLUXDB;
  reader.spec.precision = TSDB_TIME_PRECISION_NANO;
  tstrncpy(reader.spec.extDb, "db", sizeof(reader.spec.extDb));
  tstrncpy(reader.spec.extTable, "metrics", sizeof(reader.spec.extTable));
  reader.pConn = reinterpret_cast<SExtConnectorHandle *>(0x1);

  SExtColumnDef columns[2] = {};
  tstrncpy(columns[0].colName, "time", sizeof(columns[0].colName));
  tstrncpy(columns[0].extTypeName, "Timestamp(Nanosecond, None)", sizeof(columns[0].extTypeName));
  columns[0].isPrimaryKey = true;
  tstrncpy(columns[1].colName, "site", sizeof(columns[1].colName));
  tstrncpy(columns[1].extTypeName, "Utf8", sizeof(columns[1].extTypeName));
  columns[1].isTag = true;
  SExtTableMeta meta = {};
  meta.pCols = columns;
  meta.numOfCols = 2;
  reader.pInflux18Meta = &meta;

  SNode *seriesFilter = nullptr;
  SNode *triggerFilter = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_COLUMN, &seriesFilter), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_COLUMN, &triggerFilter), TSDB_CODE_SUCCESS);
  reader.pInfluxSeriesFilter = seriesFilter;
  reader.pTriggerPrefilter = triggerFilter;
  reader.pUidIndex = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(reader.pUidIndex, nullptr);
  reader.pGroupIndex = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(reader.pGroupIndex, nullptr);
  reader.pTagsetIndex = tSimpleHashInit(1, MurmurHash3_32);
  ASSERT_NE(reader.pTagsetIndex, nullptr);

  SSTriggerExtPullReq request = {};
  void *response = nullptr;
  EXPECT_EQ(streamReaderExtHandlePull(&reader, STRIGGER_PULL_LAST_TS_EXT, &request, &response),
            TSDB_CODE_SUCCESS);
  EXPECT_GE(gEnumerationSnapshotCalls, 1);
  EXPECT_EQ(gEnumerationSeriesFilter, seriesFilter);
  EXPECT_EQ(gEnumerationRowFilter, nullptr);
  EXPECT_FALSE(gEnumerationFirst);
  EXPECT_EQ(tSimpleHashGetSize(reader.pUidIndex), 0);
  EXPECT_NE(reader.influxEmptyEnumTsMs, 0);

  streamExtPullRspFree(static_cast<SSTriggerExtPullRsp *>(response));
  nodesDestroyNode(seriesFilter);
  nodesDestroyNode(triggerFilter);
  reader.pInfluxSeriesFilter = nullptr;
  reader.pTriggerPrefilter = nullptr;
  tSimpleHashCleanup(reader.pUidIndex);
  tSimpleHashCleanup(reader.pGroupIndex);
  tSimpleHashCleanup(reader.pTagsetIndex);
  taosArrayDestroy(reader.pInfluxTagCols);
  reader.pInfluxTagCols = nullptr;
  reader.pInflux18Meta = nullptr;
  taosMemoryFree(reader.pInflux18Table);
}

TEST(StreamReaderExtFilterTest, OpenParsesInflux18SeriesIdentityAst) {
  Stub stub;
  stub.set(extConnectorOpen, captureReaderConnection);
  stub.set(extConnectorClose, returnReaderConnection);
  returnedConnections = 0;

  SNode *condition = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_COLUMN, &condition), TSDB_CODE_SUCCESS);
  char *conditionJson = nullptr;
  ASSERT_EQ(nodesNodeToString(condition, false, &conditionJson, nullptr), TSDB_CODE_SUCCESS);
  nodesDestroyNode(condition);

  SStreamReaderTask task = {};
  task.triggerReader = 1;
  SStreamExtTriggerSpec spec = {};
  spec.sourceType = EXT_SOURCE_INFLUXDB;
  tstrncpy(spec.sourceName, "influx18", sizeof(spec.sourceName));
  tstrncpy(spec.options, "{\"version\":\"1.8\"}", sizeof(spec.options));
  spec.tagCondJson = conditionJson;
  SStreamExtReaderInfo *reader = nullptr;
  ASSERT_EQ(streamReaderExtOpen(&spec, &task.task, nullptr, "", EXT_TIMESTAMP_TZ_CLASS_UNKNOWN, &reader),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(reader, nullptr);
  EXPECT_NE(reader->pInfluxSeriesFilter, nullptr);
  streamReaderExtClose(reader);
  taosMemoryFree(conditionJson);
}

TEST(StreamReaderExtFilterTest, CalcReaderDoesNotInheritTriggerSeriesIdentityAst) {
  SNode *condition = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_COLUMN, &condition), TSDB_CODE_SUCCESS);
  char *conditionJson = nullptr;
  ASSERT_EQ(nodesNodeToString(condition, false, &conditionJson, nullptr), TSDB_CODE_SUCCESS);
  nodesDestroyNode(condition);

  SStreamReaderTask task = {};
  task.triggerReader = 0;
  SStreamExtTriggerSpec spec = {};
  spec.sourceType = EXT_SOURCE_INFLUXDB;
  tstrncpy(spec.sourceName, "influx18", sizeof(spec.sourceName));
  tstrncpy(spec.options, "{\"version\":\"1.8\"}", sizeof(spec.options));
  spec.tagCondJson = conditionJson;
  spec.triggerPrefilter = taosStrdup("(voltage > 220)");
  ASSERT_NE(spec.triggerPrefilter, nullptr);
  SStreamExtReaderInfo *reader = nullptr;
  ASSERT_EQ(streamReaderExtOpen(&spec, &task.task, nullptr, "", EXT_TIMESTAMP_TZ_CLASS_UNKNOWN, &reader),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(reader, nullptr);
  EXPECT_EQ(reader->pInfluxSeriesFilter, nullptr);
  EXPECT_EQ(reader->pTriggerPrefilter, nullptr);
  streamReaderExtClose(reader);
  taosMemoryFree(conditionJson);
  taosMemoryFree(spec.triggerPrefilter);
}

TEST(StreamReaderExtFilterTest, OpenRejectsInflux18PrefilterWithoutAst) {
  Stub stub;
  stub.set(extConnectorOpen, captureReaderConnection);
  stub.set(extConnectorClose, returnReaderConnection);
  returnedConnections = 0;

  SStreamReaderTask task = {};
  task.triggerReader = 1;
  SStreamExtTriggerSpec spec = {};
  spec.sourceType = EXT_SOURCE_INFLUXDB;
  tstrncpy(spec.sourceName, "influx18", sizeof(spec.sourceName));
  tstrncpy(spec.options, "{\"version\":\"1.8\"}", sizeof(spec.options));
  spec.triggerPrefilter = taosStrdup("(voltage > 220)");
  ASSERT_NE(spec.triggerPrefilter, nullptr);

  SStreamExtReaderInfo *reader = nullptr;
  EXPECT_EQ(streamReaderExtOpen(&spec, &task.task, nullptr, "", EXT_TIMESTAMP_TZ_CLASS_UNKNOWN, &reader),
            TSDB_CODE_EXT_SYNTAX_UNSUPPORTED);
  EXPECT_EQ(reader, nullptr);
  EXPECT_EQ(returnedConnections, 0);
  taosMemoryFree(spec.triggerPrefilter);
}

/* Fill one NCHAR cell with `chars` copies of one ASCII code point.  A tag value
 * that exactly fills the column (257 characters) is the boundary the reader's
 * conversion buffer has to accept. */
int32_t setNcharTagValue(SColumnInfoData *column, int32_t row, int32_t chars, uint32_t codePoint) {
  char value[EXT_INFLUX_TAG_NCHAR_CHARS] = {0};
  varDataSetLen(value, chars * TSDB_NCHAR_SIZE);
  for (int32_t i = 0; i < chars; ++i) {
    memcpy(varDataVal(value) + (size_t)i * TSDB_NCHAR_SIZE, &codePoint, TSDB_NCHAR_SIZE);
  }
  return colDataSetVal(column, row, value, false);
}

/* Schema with `numTags` tag columns named t0..t{numTags-1}: makeInflux18Schema
 * only knows the two fixed names site/zone. */
SExtTableMeta *makeInflux18SchemaWithTagNames(int32_t numTags) {
  auto *meta = static_cast<SExtTableMeta *>(taosMemoryCalloc(1, sizeof(SExtTableMeta)));
  auto *columns =
      static_cast<SExtColumnDef *>(taosMemoryCalloc((size_t)(1 + numTags), sizeof(SExtColumnDef)));
  if (meta == nullptr || columns == nullptr) {
    taosMemoryFree(meta);
    taosMemoryFree(columns);
    return nullptr;
  }
  tstrncpy(columns[0].colName, "time", sizeof(columns[0].colName));
  tstrncpy(columns[0].extTypeName, "Timestamp(Nanosecond, None)", sizeof(columns[0].extTypeName));
  columns[0].isPrimaryKey = true;
  for (int32_t i = 0; i < numTags; ++i) {
    char name[16] = {0};
    snprintf(name, sizeof(name), "t%d", i);
    tstrncpy(columns[i + 1].colName, name, sizeof(columns[i + 1].colName));
    tstrncpy(columns[i + 1].extTypeName, "Utf8", sizeof(columns[i + 1].extTypeName));
    columns[i + 1].isTag = true;
  }
  meta->pCols = columns;
  meta->numOfCols = 1 + numTags;
  return meta;
}

/* Snapshot of one series whose tag value exactly fills the tag column. */
int32_t returnFullWidthTagSnapshot(SExtConnectorHandle *, const SExtTableNode *, const SExtTableMeta *,
                                  const char *const *, int32_t, const SNode *, const SNode *, bool,
                                  int8_t, SSDataBlock **block) {
  *block = nullptr;
  int32_t code = createDataBlock(block);
  if (code != TSDB_CODE_SUCCESS) return code;
  SColumnInfoData tag = createColumnInfoData(TSDB_DATA_TYPE_NCHAR, EXT_INFLUX_TAG_NCHAR_CHARS, 1);
  SColumnInfoData time = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 2);
  time.info.precision = TSDB_TIME_PRECISION_NANO;
  code = blockDataAppendColInfo(*block, &tag);
  if (code == TSDB_CODE_SUCCESS) code = blockDataAppendColInfo(*block, &time);
  if (code == TSDB_CODE_SUCCESS) code = blockDataEnsureCapacity(*block, 1);
  if (code != TSDB_CODE_SUCCESS) return code;
  auto *tagColumn = static_cast<SColumnInfoData *>(taosArrayGet((*block)->pDataBlock, 0));
  auto *timeColumn = static_cast<SColumnInfoData *>(taosArrayGet((*block)->pDataBlock, 1));
  code = setNcharTagValue(tagColumn, 0, 257, 'a');
  if (code != TSDB_CODE_SUCCESS) return code;
  int64_t ts = 100;
  code = colDataSetVal(timeColumn, 0, reinterpret_cast<const char *>(&ts), false);
  if (code != TSDB_CODE_SUCCESS) return code;
  (*block)->info.rows = 1;
  return TSDB_CODE_SUCCESS;
}

/* A tag value that exactly fills the NCHAR column must be readable.  The reader
 * used to reserve only the column width minus the varstr header for the UCS-4 ->
 * MBS conversion, so the widest value the schema can hold was rejected and every
 * META pull of that measurement failed (the trigger turns that into a recovery
 * loop). */
TEST(StreamReaderExtFilterTest, Influx18ReadsTagValueThatFillsTheColumn) {
  Stub stub;
  stub.set(extConnectorInfluxV1SeriesSnapshot, returnFullWidthTagSnapshot);

  SStreamTask task = {};
  SStreamExtReaderInfo reader = {};
  reader.pTask = &task;
  reader.influxV1 = true;
  reader.spec.sourceType = EXT_SOURCE_INFLUXDB;
  reader.spec.precision = TSDB_TIME_PRECISION_NANO;
  tstrncpy(reader.spec.extDb, "db", sizeof(reader.spec.extDb));
  tstrncpy(reader.spec.extTable, "metrics", sizeof(reader.spec.extTable));
  reader.pConn = reinterpret_cast<SExtConnectorHandle *>(0x1);
  reader.pInflux18Meta = makeInflux18Schema(1);
  ASSERT_NE(reader.pInflux18Meta, nullptr);
  reader.pInfluxTagCols = taosArrayInit(1, EXT_INFLUX_KEY_NCHAR_CHARS);
  ASSERT_NE(reader.pInfluxTagCols, nullptr);
  char tagName[EXT_INFLUX_KEY_NCHAR_CHARS] = "site";
  ASSERT_NE(taosArrayPush(reader.pInfluxTagCols, tagName), nullptr);

  reader.pUidIndex = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  reader.pGroupIndex = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  reader.pTagsetIndex = tSimpleHashInit(2, MurmurHash3_32);
  reader.pInflux18MetaEkey = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(reader.pUidIndex, nullptr);
  ASSERT_NE(reader.pGroupIndex, nullptr);
  ASSERT_NE(reader.pTagsetIndex, nullptr);
  ASSERT_NE(reader.pInflux18MetaEkey, nullptr);

  std::string key = "site=" + std::string(257, 'a');
  uint64_t    uid = MurmurHash3_64(key.c_str(), (int32_t)key.size()) | 1ULL;
  SUidIndexEntry entry = {};
  entry.groupId = 3;
  tstrncpy(entry.tagsetKey, key.c_str(), sizeof(entry.tagsetKey));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tSimpleHashPut(reader.pUidIndex, &uid, sizeof(uid), &entry, sizeof(entry)));

  SSTriggerExtPullReq request = {};
  request.pUidMaxTs = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(request.pUidMaxTs, nullptr);

  void *response = nullptr;
  EXPECT_EQ(streamReaderExtHandlePull(&reader, STRIGGER_PULL_META_EXT, &request, &response),
            TSDB_CODE_SUCCESS);
  auto *rsp = static_cast<SSTriggerExtPullRsp *>(response);
  ASSERT_NE(rsp, nullptr);
  ASSERT_NE(rsp->pMetaBlock, nullptr);
  EXPECT_EQ(rsp->pMetaBlock->info.rows, 1);

  streamExtPullRspFree(rsp);
  tSimpleHashCleanup(request.pUidMaxTs);
  cleanupInflux18Reader(&reader);
}

/* Snapshot of two series: row 0 fills every tag with 257 characters, so its
 * composed tagset key exceeds EXT_TAGSET_KEY_MAX, while row 1 is an ordinary
 * series. */
int32_t returnOverlongTagsetKeySnapshot(SExtConnectorHandle *, const SExtTableNode *,
                                        const SExtTableMeta *, const char *const *, int32_t,
                                        const SNode *, const SNode *, bool, int8_t,
                                        SSDataBlock **block) {
  constexpr int32_t kTags = 8;
  *block = nullptr;
  int32_t code = createDataBlock(block);
  if (code != TSDB_CODE_SUCCESS) return code;
  for (int32_t i = 0; i < kTags; ++i) {
    SColumnInfoData tag =
        createColumnInfoData(TSDB_DATA_TYPE_NCHAR, EXT_INFLUX_TAG_NCHAR_CHARS, (int16_t)(i + 1));
    code = blockDataAppendColInfo(*block, &tag);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  SColumnInfoData time =
      createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), (int16_t)(kTags + 1));
  time.info.precision = TSDB_TIME_PRECISION_NANO;
  code = blockDataAppendColInfo(*block, &time);
  if (code != TSDB_CODE_SUCCESS) return code;
  code = blockDataEnsureCapacity(*block, 2);
  if (code != TSDB_CODE_SUCCESS) return code;
  for (int32_t row = 0; row < 2; ++row) {
    for (int32_t tag = 0; tag < kTags; ++tag) {
      auto *column = static_cast<SColumnInfoData *>(taosArrayGet((*block)->pDataBlock, tag));
      code = row == 0 ? setNcharTagValue(column, row, 257, 'a')
                      : setNcharTagValue(column, row, 1, (uint32_t)('a' + tag));
      if (code != TSDB_CODE_SUCCESS) return code;
    }
    int64_t ts = 100 + row * 100;
    auto   *timeColumn = static_cast<SColumnInfoData *>(taosArrayGet((*block)->pDataBlock, kTags));
    code = colDataSetVal(timeColumn, row, reinterpret_cast<const char *>(&ts), false);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  (*block)->info.rows = 2;
  return TSDB_CODE_SUCCESS;
}

/* A single series whose tags do not fit the tagset-key budget must be skipped
 * like the enumeration does, not fail the whole META pull. */
TEST(StreamReaderExtFilterTest, Influx18SkipsSeriesWhoseTagsetKeyOverflows) {
  constexpr int32_t kTags = 8;
  Stub stub;
  stub.set(extConnectorInfluxV1SeriesSnapshot, returnOverlongTagsetKeySnapshot);

  SStreamTask task = {};
  SStreamExtReaderInfo reader = {};
  reader.pTask = &task;
  reader.influxV1 = true;
  reader.spec.sourceType = EXT_SOURCE_INFLUXDB;
  reader.spec.precision = TSDB_TIME_PRECISION_NANO;
  tstrncpy(reader.spec.extDb, "db", sizeof(reader.spec.extDb));
  tstrncpy(reader.spec.extTable, "metrics", sizeof(reader.spec.extTable));
  reader.pConn = reinterpret_cast<SExtConnectorHandle *>(0x1);
  reader.pInflux18Meta = makeInflux18SchemaWithTagNames(kTags);
  ASSERT_NE(reader.pInflux18Meta, nullptr);
  reader.pInfluxTagCols = taosArrayInit(kTags, EXT_INFLUX_KEY_NCHAR_CHARS);
  ASSERT_NE(reader.pInfluxTagCols, nullptr);
  for (int32_t i = 0; i < kTags; ++i) {
    char tagName[EXT_INFLUX_KEY_NCHAR_CHARS] = {0};
    snprintf(tagName, sizeof(tagName), "t%d", i);
    ASSERT_NE(taosArrayPush(reader.pInfluxTagCols, tagName), nullptr);
  }

  reader.pUidIndex = tSimpleHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  reader.pGroupIndex = tSimpleHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  reader.pTagsetIndex = tSimpleHashInit(4, MurmurHash3_32);
  reader.pInflux18MetaEkey = tSimpleHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(reader.pUidIndex, nullptr);
  ASSERT_NE(reader.pGroupIndex, nullptr);
  ASSERT_NE(reader.pTagsetIndex, nullptr);
  ASSERT_NE(reader.pInflux18MetaEkey, nullptr);

  // Only the ordinary series (row 1) is enumerable; the oversized one never made it into the
  // UID index because the enumeration skips it for the same reason.
  std::string key;
  for (int32_t i = 0; i < kTags; ++i) {
    if (i > 0) key += "|";
    key += "t" + std::to_string(i) + "=" + std::string(1, (char)('a' + i));
  }
  uint64_t uid = MurmurHash3_64(key.c_str(), (int32_t)key.size()) | 1ULL;
  SUidIndexEntry entry = {};
  entry.groupId = 7;
  tstrncpy(entry.tagsetKey, key.c_str(), sizeof(entry.tagsetKey));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tSimpleHashPut(reader.pUidIndex, &uid, sizeof(uid), &entry, sizeof(entry)));

  SSTriggerExtPullReq request = {};
  request.pUidMaxTs = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(request.pUidMaxTs, nullptr);

  void *response = nullptr;
  EXPECT_EQ(streamReaderExtHandlePull(&reader, STRIGGER_PULL_META_EXT, &request, &response),
            TSDB_CODE_SUCCESS);
  auto *rsp = static_cast<SSTriggerExtPullRsp *>(response);
  ASSERT_NE(rsp, nullptr);
  ASSERT_NE(rsp->pMetaBlock, nullptr);
  ASSERT_EQ(rsp->pMetaBlock->info.rows, 1);
  // meta block layout: 0=groupId, 1=skey, 2=ekey, 3=uid, 4=rows.
  SColumnInfoData *pUid = static_cast<SColumnInfoData *>(taosArrayGet(rsp->pMetaBlock->pDataBlock, 3));
  ASSERT_NE(pUid, nullptr);
  EXPECT_EQ(*(int64_t *)colDataGetData(pUid, 0), (int64_t)uid);

  streamExtPullRspFree(rsp);
  tSimpleHashCleanup(request.pUidMaxTs);
  cleanupInflux18Reader(&reader);
}

/* A page that is exactly full AND exhausts the snapshot must not restart the enumeration.  The
 * trigger re-pulls META whenever the response holds metaRows >= STREAM_RETURN_ROWS_NUM, so under
 * continuous writes a fresh snapshot could again be exactly full every time and the stream would
 * never request DATA_EXT.  Keeping the exhausted cursor makes that follow-up pull return the
 * terminating empty page from the same snapshot instead. */
TEST(StreamReaderExtFilterTest, Influx18FullPageEndingKeepsTheExhaustedCursor) {
  Stub stub;
  stub.set(extConnectorInfluxV1SeriesSnapshot, returnManySeriesSnapshot);
  gPagingSnapshotRows = STREAM_RETURN_ROWS_NUM;
  gPagingSnapshotCalls = 0;

  SStreamTask task = {};
  SStreamExtReaderInfo reader = {};
  reader.pTask = &task;
  reader.influxV1 = true;
  reader.spec.sourceType = EXT_SOURCE_INFLUXDB;
  reader.spec.precision = TSDB_TIME_PRECISION_NANO;
  tstrncpy(reader.spec.extDb, "db", sizeof(reader.spec.extDb));
  tstrncpy(reader.spec.extTable, "metrics", sizeof(reader.spec.extTable));
  reader.pConn = reinterpret_cast<SExtConnectorHandle *>(0x1);
  reader.pInflux18Meta = makeInflux18Schema(1);
  ASSERT_NE(reader.pInflux18Meta, nullptr);
  reader.pInfluxTagCols = taosArrayInit(1, EXT_INFLUX_KEY_NCHAR_CHARS);
  ASSERT_NE(reader.pInfluxTagCols, nullptr);
  char tagName[EXT_INFLUX_KEY_NCHAR_CHARS] = "site";
  ASSERT_NE(taosArrayPush(reader.pInfluxTagCols, tagName), nullptr);

  reader.pUidIndex = tSimpleHashInit(gPagingSnapshotRows, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  reader.pGroupIndex = tSimpleHashInit(gPagingSnapshotRows, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  reader.pTagsetIndex = tSimpleHashInit(gPagingSnapshotRows, MurmurHash3_32);
  reader.pInflux18MetaEkey =
      tSimpleHashInit(gPagingSnapshotRows, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(reader.pUidIndex, nullptr);
  ASSERT_NE(reader.pGroupIndex, nullptr);
  ASSERT_NE(reader.pTagsetIndex, nullptr);
  ASSERT_NE(reader.pInflux18MetaEkey, nullptr);

  for (int32_t i = 0; i < gPagingSnapshotRows; ++i) {
    char key[64] = {0};
    snprintf(key, sizeof(key), "site=v%05d", i);
    SUidIndexEntry entry = {};
    entry.groupId = (uint64_t)(i + 1);
    tstrncpy(entry.tagsetKey, key, sizeof(entry.tagsetKey));
    uint64_t uid = MurmurHash3_64(key, (int32_t)strlen(key)) | 1ULL;
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              tSimpleHashPut(reader.pUidIndex, &uid, sizeof(uid), &entry, sizeof(entry)));
  }

  SSTriggerExtPullReq request = {};
  request.pUidMaxTs = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(request.pUidMaxTs, nullptr);

  void *response = nullptr;
  EXPECT_EQ(streamReaderExtHandlePull(&reader, STRIGGER_PULL_META_EXT, &request, &response),
            TSDB_CODE_SUCCESS);
  auto *rsp = static_cast<SSTriggerExtPullRsp *>(response);
  ASSERT_NE(rsp, nullptr);
  ASSERT_NE(rsp->pMetaBlock, nullptr);
  EXPECT_EQ(rsp->pMetaBlock->info.rows, STREAM_RETURN_ROWS_NUM);
  // The cursor stays at the end of the exhausted snapshot instead of restarting the enumeration.
  EXPECT_EQ(reader.influxBatchOffset, gPagingSnapshotRows);
  streamExtPullRspFree(rsp);

  int32_t snapshotCalls = gPagingSnapshotCalls;
  response = nullptr;
  EXPECT_EQ(streamReaderExtHandlePull(&reader, STRIGGER_PULL_META_EXT, &request, &response),
            TSDB_CODE_STREAM_NO_DATA);
  streamExtPullRspFree(static_cast<SSTriggerExtPullRsp *>(response));
  // No third snapshot: the follow-up pull answered from the cached enumeration.
  EXPECT_EQ(gPagingSnapshotCalls, snapshotCalls);
  EXPECT_EQ(reader.influxBatchOffset, 0);
  EXPECT_EQ(reader.pInflux18MetaSnap, nullptr);

  tSimpleHashCleanup(request.pUidMaxTs);
  cleanupInflux18Reader(&reader);
}

int32_t gCalcSchemaCalls = 0;

int32_t countCalcSchema(SExtConnectorHandle *, const SExtTableNode *, SExtTableMeta **meta) {
  ++gCalcSchemaCalls;
  *meta = makeInflux18Schema(1);
  return *meta != nullptr ? TSDB_CODE_SUCCESS : TSDB_CODE_OUT_OF_MEMORY;
}

/* An external calculation reader is created independently of the trigger reader and never runs the
 * trigger-side field probe, so it must not keep a schema cached forever: a field added after the
 * first fetch would stay outside the projection and a point carrying only that field would yield
 * no row (COUNT(*) undercounts). */
TEST(StreamReaderExtFilterTest, Influx18CalcReaderRefreshesSchemaPerFetch) {
  Stub stub;
  stub.set(extConnectorOpen, captureReaderConnection);
  stub.set(extConnectorClose, returnReaderConnection);
  stub.set(extConnectorGetTableSchema, countCalcSchema);
  stub.set(extConnectorFreeTableSchema, freeInflux18TestSchema);
  stub.set(extConnectorPrepareInfluxV1Scan, countFirstTsPrepare);
  stub.set(extConnectorExecQuery, failFirstTsExec);
  gCalcSchemaCalls = 0;

  SStreamTask task = {};
  SStreamExtReaderInfo reader = {};
  reader.pTask = &task;
  reader.influxV1 = true;
  reader.borrowConn = true;
  reader.spec.sourceType = EXT_SOURCE_INFLUXDB;
  reader.spec.precision = TSDB_TIME_PRECISION_NANO;
  tstrncpy(reader.spec.extDb, "db", sizeof(reader.spec.extDb));
  tstrncpy(reader.spec.extTable, "calc_metrics", sizeof(reader.spec.extTable));
  reader.pInflux18Meta = makeInflux18Schema(1);
  ASSERT_NE(reader.pInflux18Meta, nullptr);

  SSDataBlock *block = nullptr;
  EXPECT_NE(streamReaderExtFetchData(&reader, 0, 100, &block), TSDB_CODE_SUCCESS);
  EXPECT_EQ(block, nullptr);
  EXPECT_EQ(gCalcSchemaCalls, 1);

  // The second window must probe the remote schema again; a cached one would keep the call count
  // at 1 and silently drop field-only points written in between.
  EXPECT_NE(streamReaderExtFetchData(&reader, 100, 200, &block), TSDB_CODE_SUCCESS);
  EXPECT_EQ(gCalcSchemaCalls, 2);

  cleanupInflux18Reader(&reader);
}

// ---------------------------------------------------------------------------
// Remote time-bound literals.
//
// A timezone-aware column stores an absolute instant, but the literal that
// bounds it has to be understood by the remote session.  MySQL reads an
// offset-less datetime literal in its session timezone and only accepts an
// explicit "+00:00" suffix from 8.0.19 on, while 5.7 (still supported) treats
// the suffix as trailing garbage.  The reader therefore renders MySQL
// timezone-aware bounds as FROM_UNIXTIME(<epoch>) - an expression both
// versions evaluate per the remote session timezone - and keeps
// "<wall clock>+00:00" for PostgreSQL and a bare UTC wall clock for SQL
// Server.  These cases pin the emitted META/DATA bound SQL per source dialect.
// ---------------------------------------------------------------------------
SStreamExtReaderInfo makeBoundReader(EExtSourceType sourceType, EExtTimestampTzClass tzClass,
                                     const char *timezone, int8_t precision) {
  SStreamExtReaderInfo reader = {};
  reader.spec.sourceType = sourceType;
  reader.spec.precision = precision;
  reader.tsTzClass = static_cast<int8_t>(tzClass);
  tstrncpy(reader.timezone, timezone, sizeof(reader.timezone));
  return reader;
}

std::string captureFetchSql(SStreamExtReaderInfo *reader, int64_t skey, int64_t ekey) {
  Stub stub;
  stub.set(extConnectorOpen, captureReaderConnection);
  stub.set(extConnectorClose, returnReaderConnection);
  stub.set(extConnectorExecQuery, captureReaderQuery);
  fetchedSql.clear();
  SSDataBlock *block = nullptr;
  EXPECT_EQ(streamReaderExtFetchData(reader, skey, ekey, &block),
            TSDB_CODE_EXT_REMOTE_INTERNAL);
  EXPECT_EQ(block, nullptr);
  EXPECT_EQ(fetchedSql.size(), 1u);
  return fetchedSql.empty() ? std::string() : fetchedSql[0];
}

// 2026-01-01 00:00:00.000000 UTC expressed in microseconds.
constexpr int64_t kBoundEpochUs = 1767225600000000LL;

TEST(StreamReaderExtTimeBoundTest, MysqlAwareFetchBoundUsesUnixEpoch) {
  SStreamTask task = {};
  SStreamExtReaderInfo reader = makeBoundReader(EXT_SOURCE_MYSQL, EXT_TIMESTAMP_TZ_CLASS_AWARE,
                                                "Asia/Shanghai", TSDB_TIME_PRECISION_MICRO);
  reader.pTask = &task;
  reader.borrowConn = true;

  std::string sql = captureFetchSql(&reader, kBoundEpochUs, kBoundEpochUs);
  EXPECT_NE(sql.find(">= FROM_UNIXTIME(1767225600.000000)"), std::string::npos) << sql;
  EXPECT_NE(sql.find("<= FROM_UNIXTIME(1767225600.000000)"), std::string::npos) << sql;
  // MySQL 5.7 does not understand an offset suffix on a datetime literal.
  EXPECT_EQ(sql.find("+00:00"), std::string::npos) << sql;
}

TEST(StreamReaderExtTimeBoundTest, MysqlAwareFetchBoundIsSessionTimezoneIndependent) {
  SStreamTask task = {};
  SStreamExtReaderInfo withStreamTz = makeBoundReader(EXT_SOURCE_MYSQL, EXT_TIMESTAMP_TZ_CLASS_AWARE,
                                                      "Asia/Shanghai", TSDB_TIME_PRECISION_MICRO);
  withStreamTz.pTask = &task;
  withStreamTz.borrowConn = true;
  SStreamExtReaderInfo withoutStreamTz = makeBoundReader(EXT_SOURCE_MYSQL,
                                                         EXT_TIMESTAMP_TZ_CLASS_AWARE, "",
                                                         TSDB_TIME_PRECISION_MICRO);
  withoutStreamTz.pTask = &task;
  withoutStreamTz.borrowConn = true;

  // FROM_UNIXTIME() names one instant, so neither the stream timezone nor the
  // server global timezone may leak into the literal.
  EXPECT_EQ(captureFetchSql(&withStreamTz, kBoundEpochUs, kBoundEpochUs),
            captureFetchSql(&withoutStreamTz, kBoundEpochUs, kBoundEpochUs));
}

TEST(StreamReaderExtTimeBoundTest, MysqlNaiveFetchBoundKeepsStreamTimezone) {
  SStreamTask task = {};
  SStreamExtReaderInfo reader = makeBoundReader(EXT_SOURCE_MYSQL, EXT_TIMESTAMP_TZ_CLASS_NAIVE,
                                                "Asia/Shanghai", TSDB_TIME_PRECISION_MICRO);
  reader.pTask = &task;
  reader.borrowConn = true;

  // A DATETIME column holds a wall clock, which the connector decodes with the
  // stream timezone, so the bound must spell that wall clock.
  std::string sql = captureFetchSql(&reader, kBoundEpochUs, kBoundEpochUs);
  EXPECT_NE(sql.find("'2026-01-01 08:00:00.000000'"), std::string::npos) << sql;
}

TEST(StreamReaderExtTimeBoundTest, PostgresAwareFetchBoundSpellsUtcOffset) {
  SStreamTask task = {};
  SStreamExtReaderInfo reader = makeBoundReader(EXT_SOURCE_POSTGRESQL,
                                                EXT_TIMESTAMP_TZ_CLASS_AWARE, "Asia/Shanghai",
                                                TSDB_TIME_PRECISION_MICRO);
  reader.pTask = &task;
  reader.borrowConn = true;

  // PostgreSQL accepts an offset on a datetime literal in every supported
  // version, so the bound stays an explicit instant.
  std::string sql = captureFetchSql(&reader, kBoundEpochUs, kBoundEpochUs);
  EXPECT_NE(sql.find("'2026-01-01 00:00:00.000000+00:00'"), std::string::npos) << sql;
}

TEST(StreamReaderExtTimeBoundTest, SqlServerAwareFetchBoundStaysBareUtcWallClock) {
  SStreamTask task = {};
  SStreamExtReaderInfo reader = makeBoundReader(EXT_SOURCE_SQLSERVER,
                                                EXT_TIMESTAMP_TZ_CLASS_AWARE, "Asia/Shanghai",
                                                TSDB_TIME_PRECISION_MICRO);
  reader.pTask = &task;
  reader.borrowConn = true;

  // T-SQL reads an offset-less datetimeoffset literal as +00:00 already.
  std::string sql = captureFetchSql(&reader, kBoundEpochUs, kBoundEpochUs);
  EXPECT_NE(sql.find("'2026-01-01 00:00:00.000000'"), std::string::npos) << sql;
  EXPECT_EQ(sql.find("+00:00"), std::string::npos) << sql;
}

TEST(StreamReaderExtTimeBoundTest, OracleAwareFetchBoundUsesAnsiTimestampLiteral) {
  SStreamTask task = {};
  SStreamExtReaderInfo reader = makeBoundReader(EXT_SOURCE_ORACLE,
                                                EXT_TIMESTAMP_TZ_CLASS_AWARE, "Asia/Shanghai",
                                                TSDB_TIME_PRECISION_MICRO);
  reader.pTask = &task;
  reader.borrowConn = true;

  // Oracle pins the connector session time zone to UTC, so the bound spells a
  // bare UTC wall clock with the ANSI "TIMESTAMP" prefix that keeps Oracle from
  // reinterpreting the literal through NLS_TIMESTAMP_FORMAT.
  std::string sql = captureFetchSql(&reader, kBoundEpochUs, kBoundEpochUs);
  EXPECT_NE(sql.find(">= TIMESTAMP '2026-01-01 00:00:00.000000'"), std::string::npos) << sql;
  EXPECT_NE(sql.find("<= TIMESTAMP '2026-01-01 00:00:00.000000'"), std::string::npos) << sql;
  EXPECT_EQ(sql.find("+00:00"), std::string::npos) << sql;
}

// An Oracle TIMESTAMP(7..9) column is nanosecond resolution, and a sliding
// window end is the half-open upper bound minus one tick while the fetch keeps an
// inclusive "<=".  A six-fraction literal would round that end down to the
// previous microsecond and lose the last 999 ns of every window, so the nine
// digits Oracle accepts must survive the render.
TEST(StreamReaderExtTimeBoundTest, OracleNanoFetchBoundKeepsNineFractionDigits) {
  SStreamTask task = {};
  SStreamExtReaderInfo reader = makeBoundReader(EXT_SOURCE_ORACLE,
                                                EXT_TIMESTAMP_TZ_CLASS_AWARE, "Asia/Shanghai",
                                                TSDB_TIME_PRECISION_NANO);
  reader.pTask = &task;
  reader.borrowConn = true;

  // 1767225600123456789 ns is 2026-01-01T00:00:00.123456789 UTC.
  const int64_t bound = 1767225600123456789LL;
  std::string sql = captureFetchSql(&reader, bound, bound);
  EXPECT_NE(sql.find(">= TIMESTAMP '2026-01-01 00:00:00.123456789'"), std::string::npos) << sql;
  EXPECT_NE(sql.find("<= TIMESTAMP '2026-01-01 00:00:00.123456789'"), std::string::npos) << sql;
}

// A pre-1970 fetch bound must keep its own wall clock.  Clamping every negative
// instant to the epoch origin made the remote start each range at 1970-01-01, so
// history rows before it could never be read - and the static prefilter already
// renders those instants (renderExtPrefilterTimestamp in parTranslater.c), which
// left the runtime META/DATA bounds disagreeing with it.
TEST(StreamReaderExtTimeBoundTest, Pre1970FetchBoundKeepsItsWallClock) {
  SStreamTask task = {};
  SStreamExtReaderInfo reader = makeBoundReader(EXT_SOURCE_MYSQL, EXT_TIMESTAMP_TZ_CLASS_NAIVE,
                                                "UTC", TSDB_TIME_PRECISION_MICRO);
  reader.pTask = &task;
  reader.borrowConn = true;

  // -1 us is 1969-12-31 23:59:59.999999 UTC.
  const int64_t bound = -1;
  std::string sql = captureFetchSql(&reader, bound, bound);
  EXPECT_NE(sql.find(">= '1969-12-31 23:59:59.999999'"), std::string::npos) << sql;
  EXPECT_NE(sql.find("<= '1969-12-31 23:59:59.999999'"), std::string::npos) << sql;
  EXPECT_EQ(sql.find("'1970-01-01"), std::string::npos) << sql;
}

TEST(StreamReaderExtTimeBoundTest, MilliFetchBoundOverflowFailsBeforeConversion) {
  SStreamExtReaderInfo reader = makeBoundReader(EXT_SOURCE_MYSQL, EXT_TIMESTAMP_TZ_CLASS_NAIVE,
                                                "UTC", TSDB_TIME_PRECISION_MILLI);
  char buf[64] = {0};
  EXPECT_EQ(streamReaderExtRenderTimeBound(&reader, INT64_MAX, TSDB_TIME_PRECISION_MILLI,
                                           buf, sizeof(buf)),
            TSDB_CODE_STREAM_INTERNAL_ERROR);
  EXPECT_EQ(buf[0], '\0');
}

// A valid timestamp can still be outside the platform calendar's range.  The
// reader has no local filter fallback, so it must fail the fetch instead of
// silently replacing the requested instant with the epoch origin.
TEST(StreamReaderExtTimeBoundTest, OutOfDomainFetchBoundFailsBeforeRemoteQuery) {
  Stub stub;
  stub.set(extConnectorOpen, captureReaderConnection);
  stub.set(extConnectorClose, returnReaderConnection);
  stub.set(extConnectorExecQuery, captureReaderQuery);
  fetchedConfigs.clear();
  fetchedSql.clear();
  returnedConnections = 0;

  SStreamTask task = {};
  SStreamExtReaderInfo reader = makeBoundReader(EXT_SOURCE_MYSQL, EXT_TIMESTAMP_TZ_CLASS_NAIVE,
                                                "UTC", TSDB_TIME_PRECISION_MICRO);
  reader.pTask = &task;
  reader.borrowConn = true;

  // 0000-01-01T00:00:00Z.  MySQL DATETIME cannot hold it, and Windows cannot
  // convert it, so no valid remote SQL literal exists.
  const int64_t bound = -62167219200000000LL;
  SSDataBlock *block = nullptr;
  EXPECT_EQ(streamReaderExtFetchData(&reader, bound, bound, &block),
            TSDB_CODE_STREAM_INTERNAL_ERROR);
  EXPECT_EQ(block, nullptr);
  EXPECT_TRUE(fetchedConfigs.empty());
  EXPECT_TRUE(fetchedSql.empty());
  EXPECT_EQ(returnedConnections, 0);
}

// The MySQL epoch form still starts at the origin: FROM_UNIXTIME() returns NULL
// for a negative argument and a TIMESTAMP column cannot hold a pre-1970 value,
// so rendering the negative instant as an epoch would empty the whole fetch.
TEST(StreamReaderExtTimeBoundTest, MysqlAwarePre1970BoundStaysAtEpochOrigin) {
  SStreamTask task = {};
  SStreamExtReaderInfo reader = makeBoundReader(EXT_SOURCE_MYSQL, EXT_TIMESTAMP_TZ_CLASS_AWARE,
                                                "UTC", TSDB_TIME_PRECISION_MICRO);
  reader.pTask = &task;
  reader.borrowConn = true;

  std::string sql = captureFetchSql(&reader, -1, -1);
  EXPECT_NE(sql.find(">= FROM_UNIXTIME(0.000000)"), std::string::npos) << sql;
  EXPECT_NE(sql.find("<= FROM_UNIXTIME(0.000000)"), std::string::npos) << sql;
}

TEST(StreamReaderExtTimeBoundTest, MysqlAwareBoundKeepsFractionAtEveryPrecision) {
  SStreamExtReaderInfo reader = makeBoundReader(EXT_SOURCE_MYSQL, EXT_TIMESTAMP_TZ_CLASS_AWARE,
                                                "Asia/Shanghai", TSDB_TIME_PRECISION_MICRO);
  char buf[64] = {0};

  EXPECT_GT(streamReaderExtRenderTimeBound(&reader, 1767225600123456LL, TSDB_TIME_PRECISION_MICRO,
                                           buf, sizeof(buf)),
            0);
  EXPECT_STREQ(buf, "FROM_UNIXTIME(1767225600.123456)");

  EXPECT_GT(streamReaderExtRenderTimeBound(&reader, 1767225600123LL, TSDB_TIME_PRECISION_MILLI,
                                           buf, sizeof(buf)),
            0);
  EXPECT_STREQ(buf, "FROM_UNIXTIME(1767225600.123000)");

  EXPECT_GT(streamReaderExtRenderTimeBound(&reader, 1767225600123456000LL,
                                           TSDB_TIME_PRECISION_NANO, buf, sizeof(buf)),
            0);
  EXPECT_STREQ(buf, "FROM_UNIXTIME(1767225600.123456)");
}

// 2038-01-19 04:00:00 UTC, one hour past the MySQL TIMESTAMP ceiling.
constexpr int64_t kPastMysqlCeilingUs = 2147486400LL * 1000000LL;

TEST(StreamReaderExtTimeBoundTest, MysqlAwareUpperBoundClampsIntoTheTimestampDomain) {
  SStreamTask task = {};
  SStreamExtReaderInfo reader = makeBoundReader(EXT_SOURCE_MYSQL, EXT_TIMESTAMP_TZ_CLASS_AWARE,
                                                "Asia/Shanghai", TSDB_TIME_PRECISION_MICRO);
  reader.pTask = &task;
  reader.borrowConn = true;

  // MySQL 5.7 and 8.0 both return NULL from FROM_UNIXTIME() once the seconds
  // argument passes INT_MAX32, so an unclamped upper bound would render
  // "ts <= NULL" and drop every row of the window even though rows up to the
  // 2038 ceiling are perfectly legal TIMESTAMP values.
  std::string sql = captureFetchSql(&reader, kBoundEpochUs, kPastMysqlCeilingUs);
  EXPECT_NE(sql.find("<= FROM_UNIXTIME(2147483647.999999)"), std::string::npos) << sql;
  // An in-range lower bound is never clamped.
  EXPECT_NE(sql.find(">= FROM_UNIXTIME(1767225600.000000)"), std::string::npos) << sql;
}

TEST(StreamReaderExtTimeBoundTest, MysqlAwareLowerBoundPastTheDomainStaysRaw) {
  SStreamTask task = {};
  SStreamExtReaderInfo reader = makeBoundReader(EXT_SOURCE_MYSQL, EXT_TIMESTAMP_TZ_CLASS_AWARE,
                                                "Asia/Shanghai", TSDB_TIME_PRECISION_MICRO);
  reader.pTask = &task;
  reader.borrowConn = true;

  // "ts >= NULL" already yields the empty result such a bound asks for;
  // clamping it to the ceiling would wrongly re-admit the newest rows.
  std::string sql = captureFetchSql(&reader, kPastMysqlCeilingUs, kPastMysqlCeilingUs);
  EXPECT_NE(sql.find(">= FROM_UNIXTIME(2147486400.000000)"), std::string::npos) << sql;
  EXPECT_NE(sql.find("<= FROM_UNIXTIME(2147483647.999999)"), std::string::npos) << sql;
}

TEST(StreamReaderExtTimeBoundTest, MysqlAwareUpperBoundClampFollowsPrecision) {
  SStreamTask task = {};
  SStreamExtReaderInfo reader = makeBoundReader(EXT_SOURCE_MYSQL, EXT_TIMESTAMP_TZ_CLASS_AWARE,
                                                "Asia/Shanghai", TSDB_TIME_PRECISION_MILLI);
  reader.pTask = &task;
  reader.borrowConn = true;

  std::string sql = captureFetchSql(&reader, 1767225600000LL, 2147486400000LL);
  EXPECT_NE(sql.find("<= FROM_UNIXTIME(2147483647.999000)"), std::string::npos) << sql;
}

}  // namespace

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
