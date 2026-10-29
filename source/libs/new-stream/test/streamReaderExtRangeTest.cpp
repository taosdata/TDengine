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
#include <thread>
#include <string>
#include <vector>
#include "stub.h"

#include "functionMgt.h"
#include "nodes.h"
#include "querynodes.h"
#include "streamReaderExt.h"
#include "tarray.h"
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
