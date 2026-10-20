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

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <iostream>

#include <memory>
#include <mutex>
#include <vector>

#include "dataSink.h"
#include "nodes.h"
#include "stream.h"
#include "streamInt.h"
#include "streamReader.h"
#include "streamReaderExt.h"
#include "extConnector.h"
#include "streamRunner.h"
#include "stub.h"
#include "tmsg.h"

#include "../../../common/test/readerFetchBenchmarkTest.h"

using reader_fetch_benchmark::ReaderFetchLayout;

extern "C" int32_t stExtReaderOpen(void *pSpec, const SStreamTask *pTask, void **ppInfo);

namespace {

std::string gInfluxPrecisionSql;

int32_t captureInfluxPrecisionQuery(SExtConnectorHandle *, const SFederatedScanPhysiNode *, const char *sql,
                                    SExtQueryHandle **query, SExtConnectorError *) {
  gInfluxPrecisionSql = sql;
  *query = nullptr;
  return TSDB_CODE_SUCCESS;
}

int32_t finishInfluxPrecisionQuery(SExtQueryHandle *, const SExtColTypeMapping *, int32_t,
                                   SSDataBlock **block, SExtConnectorError *) {
  *block = nullptr;
  return TSDB_CODE_SUCCESS;
}

void closeInfluxPrecisionQuery(SExtQueryHandle *) {}

TEST(StreamReaderTest, InfluxCalcBoundsUseReaderPrecisionAndSaturate) {
  Stub stub;
  stub.set(extConnectorExecQuery, captureInfluxPrecisionQuery);
  stub.set(extConnectorFetchBlock, finishInfluxPrecisionQuery);
  stub.set(extConnectorCloseQuery, closeInfluxPrecisionQuery);
  SStreamTask task = {};
  SStreamExtReaderInfo reader = {};
  reader.pTask = &task;
  reader.spec.sourceType = EXT_SOURCE_INFLUXDB;
  tstrncpy(reader.spec.extTable, "metrics", sizeof(reader.spec.extTable));
  for (int8_t precision : {TSDB_TIME_PRECISION_MILLI, TSDB_TIME_PRECISION_MICRO, TSDB_TIME_PRECISION_NANO}) {
    reader.spec.precision = precision;
    int64_t factor = precision == TSDB_TIME_PRECISION_MILLI ? 1000000 :
                     precision == TSDB_TIME_PRECISION_MICRO ? 1000 : 1;
    SSDataBlock *block = nullptr;
    ASSERT_EQ(streamReaderExtFetchData(&reader, 1700000000, 1700000001, &block), TSDB_CODE_SUCCESS);
    EXPECT_NE(gInfluxPrecisionSql.find("to_timestamp_nanos(" + std::to_string(1700000000LL * factor) + ")"),
              std::string::npos);
    EXPECT_NE(gInfluxPrecisionSql.find("to_timestamp_nanos(" + std::to_string(1700000001LL * factor + factor - 1) + ")"),
              std::string::npos);
    ASSERT_EQ(streamReaderExtFetchData(&reader, INT64_MIN, INT64_MAX, &block), TSDB_CODE_SUCCESS);
    EXPECT_EQ(gInfluxPrecisionSql.find("to_timestamp_nanos"), std::string::npos);
    ASSERT_EQ(streamReaderExtFetchData(&reader, INT64_MIN + 1, INT64_MAX - 1, &block), TSDB_CODE_SUCCESS);
    EXPECT_NE(gInfluxPrecisionSql.find("to_timestamp_nanos(" +
                  std::to_string(factor == 1 ? INT64_MIN + 1 : INT64_MIN) + ")"), std::string::npos);
    EXPECT_NE(gInfluxPrecisionSql.find("to_timestamp_nanos(" +
                  std::to_string(factor == 1 ? INT64_MAX - 1 : INT64_MAX) + ")"), std::string::npos);
  }
}

bool gInfluxTimestampBatchPending = false;

int32_t fetchInfluxTimestampBatch(SExtQueryHandle *, const SExtColTypeMapping *, int32_t,
                                 SSDataBlock **block, SExtConnectorError *) {
  *block = nullptr;
  if (!gInfluxTimestampBatchPending) return TSDB_CODE_SUCCESS;
  gInfluxTimestampBatchPending = false;
  int32_t code = createDataBlock(block);
  if (code != TSDB_CODE_SUCCESS) return code;
  SColumnInfoData column = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 0);
  column.info.precision = TSDB_TIME_PRECISION_NANO;
  code = blockDataAppendColInfo(*block, &column);
  if (code != TSDB_CODE_SUCCESS) return code;
  code = blockDataEnsureCapacity(*block, 2);
  if (code != TSDB_CODE_SUCCESS) return code;
  auto *ts = static_cast<SColumnInfoData *>(taosArrayGet((*block)->pDataBlock, 0));
  int64_t values[] = {1700000000123456789LL, 1700000000456789123LL};
  for (int32_t row = 0; row < 2; ++row) {
    code = colDataSetVal(ts, row, reinterpret_cast<const char *>(&values[row]), false);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  (*block)->info.rows = 2;
  return TSDB_CODE_SUCCESS;
}

TEST(StreamReaderTest, InfluxMetaAndLastTimestampShareReaderPrecision) {
  Stub stub;
  stub.set(extConnectorExecQuery, captureInfluxPrecisionQuery);
  stub.set(extConnectorFetchBlock, fetchInfluxTimestampBatch);
  stub.set(extConnectorCloseQuery, closeInfluxPrecisionQuery);
  SStreamTask task = {};
  SStreamExtReaderInfo reader = {};
  reader.pTask = &task;
  reader.spec.sourceType = EXT_SOURCE_INFLUXDB;
  tstrncpy(reader.spec.extTable, "metrics", sizeof(reader.spec.extTable));
  reader.pUidIndex = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(reader.pUidIndex, nullptr);
  uint64_t uid = 42;
  SUidIndexEntry entry = {};
  entry.groupId = 7;
  ASSERT_EQ(tSimpleHashPut(reader.pUidIndex, &uid, sizeof(uid), &entry, sizeof(entry)), TSDB_CODE_SUCCESS);
  SSTriggerExtPullReq request = {};
  request.pUidMaxTs = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(request.pUidMaxTs, nullptr);
  for (int8_t precision : {TSDB_TIME_PRECISION_MILLI, TSDB_TIME_PRECISION_MICRO, TSDB_TIME_PRECISION_NANO}) {
    reader.spec.precision = precision;
    int64_t divisor = precision == TSDB_TIME_PRECISION_MILLI ? 1000000 :
                      precision == TSDB_TIME_PRECISION_MICRO ? 1000 : 1;
    int64_t watermark = 1700000000000000000LL / divisor;
    ASSERT_EQ(tSimpleHashPut(request.pUidMaxTs, &uid, sizeof(uid), &watermark, sizeof(watermark)),
              TSDB_CODE_SUCCESS);
    for (auto type : {STRIGGER_PULL_LAST_TS_EXT, STRIGGER_PULL_META_EXT}) {
      void *response = nullptr;
      gInfluxTimestampBatchPending = true;
      ASSERT_EQ(streamReaderExtHandlePull(&reader, type, &request, &response), TSDB_CODE_SUCCESS);
      auto *rsp = static_cast<SSTriggerExtPullRsp *>(response);
      ASSERT_NE(rsp, nullptr);
      if (type == STRIGGER_PULL_LAST_TS_EXT) {
        auto *last = static_cast<SExtLastTsInfo *>(taosArrayGet(rsp->pLastTsArr, 0));
        ASSERT_NE(last, nullptr);
        EXPECT_EQ(last->ts, 1700000000456789123LL / divisor);
      } else {
        ASSERT_NE(rsp->pMetaBlock, nullptr);
        auto *first = static_cast<SColumnInfoData *>(taosArrayGet(rsp->pMetaBlock->pDataBlock, 1));
        auto *last = static_cast<SColumnInfoData *>(taosArrayGet(rsp->pMetaBlock->pDataBlock, 2));
        EXPECT_EQ(*reinterpret_cast<int64_t *>(colDataGetData(first, 0)), 1700000000123456789LL / divisor);
        EXPECT_EQ(*reinterpret_cast<int64_t *>(colDataGetData(last, 0)), 1700000000456789123LL / divisor);
        EXPECT_NE(gInfluxPrecisionSql.find("to_timestamp_nanos(" + std::to_string(1700000000000000000LL + divisor - 1) + ")"), std::string::npos);
      }
      streamExtPullRspFree(rsp);
    }
    watermark = 1700000000456789123LL / divisor;
    ASSERT_EQ(tSimpleHashPut(request.pUidMaxTs, &uid, sizeof(uid), &watermark, sizeof(watermark)),
              TSDB_CODE_SUCCESS);
    void *response = nullptr;
    gInfluxTimestampBatchPending = false;
    ASSERT_EQ(streamReaderExtHandlePull(&reader, STRIGGER_PULL_META_EXT, &request, &response),
              TSDB_CODE_STREAM_NO_DATA);
    int64_t scannedThrough = watermark * divisor + divisor - 1;
    EXPECT_GE(scannedThrough, 1700000000456789123LL);
    EXPECT_NE(gInfluxPrecisionSql.find("> to_timestamp_nanos(" + std::to_string(scannedThrough) + ")"),
              std::string::npos);
    streamExtPullRspFree(static_cast<SSTriggerExtPullRsp *>(response));
  }
  tSimpleHashCleanup(request.pUidMaxTs);
  tSimpleHashCleanup(reader.pUidIndex);
}

TEST(StreamReaderTest, InfluxDataBoundsUseReaderPrecisionInSingleAndBatchFetch) {
  Stub stub;
  stub.set(extConnectorExecQuery, captureInfluxPrecisionQuery);
  stub.set(extConnectorFetchBlock, finishInfluxPrecisionQuery);
  stub.set(extConnectorCloseQuery, closeInfluxPrecisionQuery);
  SStreamTask task = {};
  SStreamExtReaderInfo reader = {};
  reader.pTask = &task;
  reader.spec.sourceType = EXT_SOURCE_INFLUXDB;
  tstrncpy(reader.spec.extTable, "metrics", sizeof(reader.spec.extTable));
  reader.pUidIndex = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(reader.pUidIndex, nullptr);
  uint64_t uid = 42;
  SUidIndexEntry entry = {};
  entry.groupId = 7;
  ASSERT_EQ(tSimpleHashPut(reader.pUidIndex, &uid, sizeof(uid), &entry, sizeof(entry)), TSDB_CODE_SUCCESS);
  SSTriggerExtPullReq request = {};
  request.pUidWindow = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(request.pUidWindow, nullptr);
  SExtColTypeMapping mapping = {};
  mapping.tdType.type = TSDB_DATA_TYPE_TIMESTAMP;
  mapping.tdType.bytes = sizeof(int64_t);
  mapping.tdType.precision = TSDB_TIME_PRECISION_NANO;
  tstrncpy(mapping.colName, "ts", sizeof(mapping.colName));
  reader.spec.pColMappings = &mapping;
  reader.spec.numColMappings = 1;
  reader.spec.triggerColumns = taosArrayInit(1, TSDB_COL_NAME_LEN);
  ASSERT_NE(reader.spec.triggerColumns, nullptr);
  char timestampColumn[TSDB_COL_NAME_LEN] = "ts";
  ASSERT_NE(taosArrayPush(reader.spec.triggerColumns, timestampColumn), nullptr);
  for (bool batched : {false, true}) {
    if (batched) {
      reader.pInfluxTagCols = taosArrayInit(1, TSDB_COL_NAME_LEN);
      ASSERT_NE(reader.pInfluxTagCols, nullptr);
      char tagColumn[TSDB_COL_NAME_LEN] = "host";
      ASSERT_NE(taosArrayPush(reader.pInfluxTagCols, tagColumn), nullptr);
    }
    for (int8_t precision : {TSDB_TIME_PRECISION_MILLI, TSDB_TIME_PRECISION_MICRO, TSDB_TIME_PRECISION_NANO}) {
      reader.spec.precision = precision;
      int64_t divisor = precision == TSDB_TIME_PRECISION_MILLI ? 1000000 :
                        precision == TSDB_TIME_PRECISION_MICRO ? 1000 : 1;
      SExtUidWindow window = {};
      window.skey = 1700000000123000000LL / divisor;
      window.ekey = 1700000000456000000LL / divisor;
      ASSERT_EQ(tSimpleHashPut(request.pUidWindow, &uid, sizeof(uid), &window, sizeof(window)),
                TSDB_CODE_SUCCESS);
      void *response = nullptr;
      ASSERT_EQ(streamReaderExtHandlePull(&reader, STRIGGER_PULL_DATA_EXT, &request, &response),
                TSDB_CODE_STREAM_NO_DATA);
      EXPECT_NE(gInfluxPrecisionSql.find("to_timestamp_nanos(1700000000123000000)"), std::string::npos);
      EXPECT_NE(gInfluxPrecisionSql.find("to_timestamp_nanos(" + std::to_string(1700000000456000000LL + divisor - 1) + ")"), std::string::npos);
      streamExtPullRspFree(static_cast<SSTriggerExtPullRsp *>(response));
    }
  }
  taosArrayDestroy(reader.pInfluxTagCols);
  taosArrayDestroy(reader.spec.triggerColumns);
  tSimpleHashCleanup(request.pUidWindow);
  tSimpleHashCleanup(reader.pUidIndex);
}

TEST(StreamReaderTest, InfluxCalcBoundsCoverTruncatedNegativeAndZeroTicks) {
  Stub stub;
  stub.set(extConnectorExecQuery, captureInfluxPrecisionQuery);
  stub.set(extConnectorFetchBlock, finishInfluxPrecisionQuery);
  stub.set(extConnectorCloseQuery, closeInfluxPrecisionQuery);
  SStreamTask task = {};
  SStreamExtReaderInfo reader = {};
  reader.pTask = &task;
  reader.spec.sourceType = EXT_SOURCE_INFLUXDB;
  tstrncpy(reader.spec.extTable, "metrics", sizeof(reader.spec.extTable));
  for (int8_t precision : {TSDB_TIME_PRECISION_MILLI, TSDB_TIME_PRECISION_MICRO, TSDB_TIME_PRECISION_NANO}) {
    reader.spec.precision = precision;
    int64_t factor = precision == TSDB_TIME_PRECISION_MILLI ? 1000000 :
                     precision == TSDB_TIME_PRECISION_MICRO ? 1000 : 1;
    for (int64_t tick : {-2LL, -1LL, 0LL, 1LL}) {
      SSDataBlock *block = nullptr;
      ASSERT_EQ(streamReaderExtFetchData(&reader, tick, tick, &block), TSDB_CODE_SUCCESS);
      int64_t first = tick * factor - (tick <= 0 ? factor - 1 : 0);
      int64_t last = tick * factor + (tick >= 0 ? factor - 1 : 0);
      EXPECT_NE(gInfluxPrecisionSql.find(">= to_timestamp_nanos(" + std::to_string(first) + ")"),
                std::string::npos);
      EXPECT_NE(gInfluxPrecisionSql.find("<= to_timestamp_nanos(" + std::to_string(last) + ")"),
                std::string::npos);
      EXPECT_EQ(first / factor, tick);
      EXPECT_EQ(last / factor, tick);
      EXPECT_GT((last + 1) / factor, tick);
    }
  }
}

SNode *gExternalReaderPlan = nullptr;
int32_t gExternalReaderOpenCalls = 0;

int32_t cloneExternalReaderPlan(const char *, SNode **ppNode) {
  return nodesCloneNode(gExternalReaderPlan, ppNode);
}

int32_t stopExternalReaderOpen(void *, const SStreamTask *, void **) {
  ++gExternalReaderOpenCalls;
  return TSDB_CODE_OPS_NOT_SUPPORT;
}

void buildExternalReaderPlan(SNode **ppNode, bool matchingRoot) {
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN, ppNode), TSDB_CODE_SUCCESS);
  auto *scan = reinterpret_cast<SFederatedScanPhysiNode *>(*ppNode);
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SCAN, &scan->pRemotePlan), TSDB_CODE_SUCCESS);
  auto *remote = reinterpret_cast<SLogicNode *>(scan->pRemotePlan);
  SNode *hint = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_HINT, &hint), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesListMakeAppend(&remote->pHint, hint), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_EXTERNAL_TABLE, &scan->pExtTable), TSDB_CODE_SUCCESS);
  auto *table = reinterpret_cast<SExtTableNode *>(scan->pExtTable);
  tstrncpy(table->sourceName, matchingRoot ? "remote" : "other", sizeof(table->sourceName));
  tstrncpy(table->table.dbName, "metrics", sizeof(table->table.dbName));

  if (!matchingRoot) {
    SNode *child = nullptr;
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN, &child), TSDB_CODE_SUCCESS);
    ASSERT_EQ(nodesListMakeAppend(&scan->node.pChildren, child), TSDB_CODE_SUCCESS);
    scan = reinterpret_cast<SFederatedScanPhysiNode *>(child);
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_EXTERNAL_TABLE, &scan->pExtTable), TSDB_CODE_SUCCESS);
    table = reinterpret_cast<SExtTableNode *>(scan->pExtTable);
    tstrncpy(table->sourceName, "remote", sizeof(table->sourceName));
    tstrncpy(table->table.dbName, "metrics", sizeof(table->table.dbName));
  }

  SNode *column = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_COLUMN, &column), TSDB_CODE_SUCCESS);
  auto *columnInfo = reinterpret_cast<SColumnNode *>(column);
  tstrncpy(columnInfo->colName, "cpu", sizeof(columnInfo->colName));
  columnInfo->node.resType.type = TSDB_DATA_TYPE_DOUBLE;
  columnInfo->node.resType.bytes = sizeof(double);
  ASSERT_EQ(nodesListMakeAppend(&scan->pScanCols, column), TSDB_CODE_SUCCESS);
}

TEST(StreamReaderTest, ExternalDeployDoesNotTraverseRemoteLogicalHintsAsPhysicalNodes) {
  for (bool matchingRoot : {true, false}) {
    buildExternalReaderPlan(&gExternalReaderPlan, matchingRoot);
    ASSERT_NE(gExternalReaderPlan, nullptr);
    std::unique_ptr<SNode, decltype(&nodesDestroyNode)> plan(gExternalReaderPlan, nodesDestroyNode);
    SStreamReaderTask task = {};
    SStreamExtTriggerSpec spec = {};
    tstrncpy(spec.sourceName, "remote", sizeof(spec.sourceName));
    SStreamReaderDeployMsg message = {};
    message.pExtSpec = &spec;
    message.msg.calc.calcScanPlan = const_cast<char *>("external-reader-plan");
    gExternalReaderOpenCalls = 0;
    Stub stub;
    stub.set(nodesStringToNode, cloneExternalReaderPlan);
    stub.set(stExtReaderOpen, stopExternalReaderOpen);

    EXPECT_EQ(stReaderTaskDeploy(&task, &message), TSDB_CODE_OPS_NOT_SUPPORT);
    EXPECT_EQ(gExternalReaderOpenCalls, 1);
    EXPECT_STREQ(spec.extDb, "metrics");
    EXPECT_EQ(spec.numCalcMappings, 1);
    if (taosArrayGetSize(spec.calcColumns) == 1) {
      EXPECT_STREQ(static_cast<const char *>(taosArrayGet(spec.calcColumns, 0)), "cpu");
    } else {
      ADD_FAILURE() << "Missing external calculation column";
    }
    stub.reset(stExtReaderOpen);
    stub.reset(nodesStringToNode);
    streamTaskStatsHandleLifecycle(&task.pStats, STREAM_TASK_STATS_OWNER_DESTROYED);
    taosArrayDestroy(spec.calcColumns);
    taosMemoryFree(spec.pCalcMappings);
    gExternalReaderPlan = nullptr;
  }
}

SSDataBlock *makeReaderFetchDataBlock(uint64_t baseGId, int32_t value) {
  SSDataBlock *pBlock = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, createDataBlock(&pBlock));
  if (pBlock == nullptr) return nullptr;

  SColumnInfoData column = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 1);
  EXPECT_EQ(TSDB_CODE_SUCCESS, blockDataAppendColInfo(pBlock, &column));
  EXPECT_EQ(TSDB_CODE_SUCCESS, blockDataEnsureCapacity(pBlock, 1));
  auto *pColumn = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
  EXPECT_NE(nullptr, pColumn);
  if (pColumn == nullptr ||
      colDataSetVal(pColumn, 0, reinterpret_cast<const char *>(&value), false) != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    return nullptr;
  }
  pBlock->info.rows = 1;
  pBlock->info.id.groupId = baseGId;
  pBlock->info.id.baseGId = baseGId;
  return pBlock;
}

std::vector<uint8_t> encodeReaderFetchBatchFields(uint8_t version, int32_t itemCount, uint8_t type, uint64_t baseGId,
                                                  const std::vector<uint8_t> &block) {
  std::vector<uint8_t> bytes(sizeof(uint8_t) + sizeof(int32_t) + sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint32_t) +
                             block.size());
  SEncoder             encoder = {};
  tEncoderInit(&encoder, bytes.data(), bytes.size());
  EXPECT_EQ(TSDB_CODE_SUCCESS, tEncodeU8(&encoder, version));
  EXPECT_EQ(TSDB_CODE_SUCCESS, tEncodeI32(&encoder, itemCount));
  if (itemCount > 0) {
    EXPECT_EQ(TSDB_CODE_SUCCESS, tEncodeU8(&encoder, type));
    EXPECT_EQ(TSDB_CODE_SUCCESS, tEncodeU64(&encoder, baseGId));
    EXPECT_EQ(TSDB_CODE_SUCCESS,
              tEncodeBinary(&encoder, block.empty() ? nullptr : block.data(), static_cast<uint32_t>(block.size())));
  }
  bytes.resize(encoder.pos);
  tEncoderClear(&encoder);
  return bytes;
}

std::vector<uint8_t> encodeReaderFetchBlock(const SSDataBlock *pBlock) {
  const int32_t blockLen = blockGetInternalEncodeSize(pBlock);
  EXPECT_GT(blockLen, 0);
  std::vector<uint8_t> bytes(blockLen);
  EXPECT_EQ(blockLen, blockEncodeInternal(pBlock, reinterpret_cast<char *>(bytes.data()), bytes.size(),
                                          taosArrayGetSize(pBlock->pDataBlock)));
  return bytes;
}

void expectReaderFetchBatchDecodeFailure(const std::vector<uint8_t> &bytes) {
  SArray *pItems = reinterpret_cast<SArray *>(static_cast<uintptr_t>(1));
  EXPECT_NE(TSDB_CODE_SUCCESS,
            tDecodeStreamReaderFetchBatch(bytes.empty() ? nullptr : bytes.data(), bytes.size(), &pItems));
  EXPECT_EQ(nullptr, pItems);
}

TEST(StreamReaderFetchBatchTest, LegacyResponseDropsEogBetweenDataBlocks) {
  SArray *pBlocks = taosArrayInit(3, sizeof(SSDataBlock *));
  ASSERT_NE(nullptr, pBlocks);
  SSDataBlock *pFirst = makeReaderFetchDataBlock(101, 11);
  SSDataBlock *pEog = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, createDataBlock(&pEog));
  ASSERT_EQ(TSDB_CODE_SUCCESS, qSetMultiGroupEogBlock(pEog, 101));
  SSDataBlock *pSecond = makeReaderFetchDataBlock(202, 22);
  ASSERT_NE(nullptr, pFirst);
  ASSERT_NE(nullptr, pSecond);
  ASSERT_NE(nullptr, taosArrayPush(pBlocks, &pFirst));
  ASSERT_NE(nullptr, taosArrayPush(pBlocks, &pEog));
  ASSERT_NE(nullptr, taosArrayPush(pBlocks, &pSecond));

  void  *pData = nullptr;
  size_t size = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, streamBuildFetchRsp(pBlocks, true, &pData, &size, TSDB_TIME_PRECISION_MILLI));
  ASSERT_NE(nullptr, pData);
  const auto *pRsp = static_cast<const SRetrieveTableRsp *>(pData);
  EXPECT_EQ(2, ntohl(pRsp->numOfBlocks));
  EXPECT_EQ(2, be64toh(pRsp->numOfRows));
  EXPECT_EQ(size - sizeof(*pRsp), ntohl(pRsp->payloadLen));
  EXPECT_EQ(size - sizeof(*pRsp), ntohl(pRsp->compLen));
  EXPECT_EQ(0, pRsp->compressed);

  const char *pCursor = pRsp->data;
  for (uint64_t expectedGid : {UINT64_C(101), UINT64_C(202)}) {
    const int32_t blockLen = *reinterpret_cast<const int32_t *>(pCursor);
    pCursor += INT_BYTES * 2;
    SSDataBlock decoded = {};
    const char *pEnd = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, blockDecodeInternalBounded(&decoded, pCursor, blockLen, &pEnd));
    EXPECT_EQ(expectedGid, decoded.info.id.baseGId);
    EXPECT_EQ(pCursor + blockLen, pEnd);
    blockDataFreeRes(&decoded);
    pCursor = pEnd;
  }
  EXPECT_EQ(static_cast<const char *>(pData) + size, pCursor);

  rpcFreeCont(pData);
  blockDataDestroy(pFirst);
  blockDataDestroy(pEog);
  blockDataDestroy(pSecond);
  taosArrayDestroy(pBlocks);
}

TEST(StreamReaderFetchBatchTest, LegacyEmptyResponseHasZeroPayloadLength) {
  SArray *pBlocks = taosArrayInit(1, sizeof(SSDataBlock *));
  ASSERT_NE(nullptr, pBlocks);
  void *pData = nullptr;
  size_t size = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, streamBuildFetchRsp(pBlocks, false, &pData, &size, TSDB_TIME_PRECISION_MILLI));
  ASSERT_NE(nullptr, pData);
  const auto *pRsp = static_cast<const SRetrieveTableRsp *>(pData);
  EXPECT_EQ(sizeof(*pRsp), size);
  EXPECT_EQ(0, ntohl(pRsp->payloadLen));
  EXPECT_EQ(0, ntohl(pRsp->compLen));
  EXPECT_EQ(1, pRsp->completed);
  rpcFreeCont(pData);
  taosArrayDestroy(pBlocks);
}

int32_t invalidLegacyFetchBlockSize(const SSDataBlock *) { return INT32_MAX; }

TEST(StreamReaderFetchBatchTest, LegacyResponseRejectsOversizedBlock) {
  SArray *pBlocks = taosArrayInit(1, sizeof(SSDataBlock *));
  ASSERT_NE(nullptr, pBlocks);
  SSDataBlock *pBlock = makeReaderFetchDataBlock(101, 11);
  ASSERT_NE(nullptr, pBlock);
  ASSERT_NE(nullptr, taosArrayPush(pBlocks, &pBlock));
  Stub sizeStub;
  sizeStub.set(blockGetInternalEncodeSize, invalidLegacyFetchBlockSize);
  void *pData = reinterpret_cast<void *>(static_cast<uintptr_t>(1));
  size_t size = 123;
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, streamBuildFetchRsp(pBlocks, false, &pData, &size, TSDB_TIME_PRECISION_MILLI));
  EXPECT_EQ(nullptr, pData);
  EXPECT_EQ(0, size);
  blockDataDestroy(pBlock);
  taosArrayDestroy(pBlocks);
}

TEST(StreamReaderFetchBatchTest, DedicatedResponseRoundTripsDataEogData) {
  SArray *pBlocks = taosArrayInit(3, sizeof(SSDataBlock *));
  ASSERT_NE(nullptr, pBlocks);
  SSDataBlock *pFirst = makeReaderFetchDataBlock(101, 11);
  SSDataBlock *pEog = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, createDataBlock(&pEog));
  ASSERT_EQ(TSDB_CODE_SUCCESS, qSetMultiGroupEogBlock(pEog, 101));
  SSDataBlock *pSecond = makeReaderFetchDataBlock(202, 22);
  ASSERT_NE(nullptr, pFirst);
  ASSERT_NE(nullptr, pSecond);
  ASSERT_NE(nullptr, taosArrayPush(pBlocks, &pFirst));
  ASSERT_NE(nullptr, taosArrayPush(pBlocks, &pEog));
  ASSERT_NE(nullptr, taosArrayPush(pBlocks, &pSecond));

  void  *pData = nullptr;
  size_t size = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, streamBuildGroupFetchRsp(pBlocks, false, &pData, &size, TSDB_TIME_PRECISION_MILLI));
  ASSERT_NE(nullptr, pData);
  const auto *pRsp = static_cast<const SRetrieveTableRsp *>(pData);
  EXPECT_EQ(3, pRsp->streamBlockType);
  EXPECT_EQ(1, pRsp->completed);
  EXPECT_EQ(2, ntohl(pRsp->numOfBlocks));
  EXPECT_EQ(2, be64toh(pRsp->numOfRows));
  const int32_t payloadLen = ntohl(pRsp->payloadLen);
  EXPECT_EQ(size - sizeof(SRetrieveTableRsp), static_cast<size_t>(payloadLen));

  SArray *pItems = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDecodeStreamReaderFetchBatch(pRsp->data, payloadLen, &pItems));
  ASSERT_NE(nullptr, pItems);
  ASSERT_EQ(3, taosArrayGetSize(pItems));
  const auto *pFirstItem = static_cast<const SStreamReaderFetchItem *>(taosArrayGet(pItems, 0));
  const auto *pEndItem = static_cast<const SStreamReaderFetchItem *>(taosArrayGet(pItems, 1));
  const auto *pSecondItem = static_cast<const SStreamReaderFetchItem *>(taosArrayGet(pItems, 2));
  ASSERT_NE(nullptr, pFirstItem);
  ASSERT_NE(nullptr, pEndItem);
  ASSERT_NE(nullptr, pSecondItem);
  EXPECT_EQ(STREAM_RUNNER_FETCH_DATA, pFirstItem->type);
  ASSERT_NE(nullptr, pFirstItem->pBlock);
  EXPECT_EQ(101U, pFirstItem->baseGId);
  EXPECT_EQ(101U, pFirstItem->pBlock->info.id.baseGId);
  const auto *pFirstColumn = static_cast<const SColumnInfoData *>(taosArrayGet(pFirstItem->pBlock->pDataBlock, 0));
  ASSERT_NE(nullptr, pFirstColumn);
  EXPECT_EQ(11, *reinterpret_cast<const int32_t *>(colDataGetData(pFirstColumn, 0)));
  EXPECT_EQ(STREAM_RUNNER_FETCH_EOG, pEndItem->type);
  EXPECT_EQ(101U, pEndItem->baseGId);
  EXPECT_EQ(nullptr, pEndItem->pBlock);
  EXPECT_EQ(STREAM_RUNNER_FETCH_DATA, pSecondItem->type);
  ASSERT_NE(nullptr, pSecondItem->pBlock);
  EXPECT_EQ(202U, pSecondItem->baseGId);
  EXPECT_EQ(202U, pSecondItem->pBlock->info.id.baseGId);
  const auto *pSecondColumn = static_cast<const SColumnInfoData *>(taosArrayGet(pSecondItem->pBlock->pDataBlock, 0));
  ASSERT_NE(nullptr, pSecondColumn);
  EXPECT_EQ(22, *reinterpret_cast<const int32_t *>(colDataGetData(pSecondColumn, 0)));

  for (int32_t cut = 0; cut < payloadLen; ++cut) {
    SArray *pTruncatedItems = nullptr;
    EXPECT_NE(TSDB_CODE_SUCCESS, tDecodeStreamReaderFetchBatch(pRsp->data, cut, &pTruncatedItems));
    EXPECT_EQ(nullptr, pTruncatedItems);
  }

  taosArrayDestroyEx(pItems, tDestroyStreamReaderFetchItem);
  rpcFreeCont(pData);
  blockDataDestroy(pFirst);
  blockDataDestroy(pEog);
  blockDataDestroy(pSecond);
  taosArrayDestroy(pBlocks);
}

TEST(StreamReaderFetchBatchTest, DecoderRejectsMalformedEnvelopeAndItems) {
  SSDataBlock *pBlock = makeReaderFetchDataBlock(101, 11);
  ASSERT_NE(nullptr, pBlock);
  const auto encodedBlock = encodeReaderFetchBlock(pBlock);
  const auto valid =
      encodeReaderFetchBatchFields(STREAM_READER_FETCH_BATCH_VERSION, 1, STREAM_RUNNER_FETCH_DATA, 101, encodedBlock);

  for (size_t cut = 0; cut < valid.size(); ++cut) {
    expectReaderFetchBatchDecodeFailure(std::vector<uint8_t>(valid.begin(), valid.begin() + cut));
  }

  expectReaderFetchBatchDecodeFailure(encodeReaderFetchBatchFields(STREAM_READER_FETCH_BATCH_VERSION + 1, 1,
                                                                   STREAM_RUNNER_FETCH_DATA, 101, encodedBlock));
  expectReaderFetchBatchDecodeFailure(
      encodeReaderFetchBatchFields(STREAM_READER_FETCH_BATCH_VERSION, 1, 0, 101, encodedBlock));
  expectReaderFetchBatchDecodeFailure(
      encodeReaderFetchBatchFields(STREAM_READER_FETCH_BATCH_VERSION, 1, 99, 101, encodedBlock));
  expectReaderFetchBatchDecodeFailure(
      encodeReaderFetchBatchFields(STREAM_READER_FETCH_BATCH_VERSION, 1, STREAM_RUNNER_FETCH_DATA, 0, encodedBlock));
  expectReaderFetchBatchDecodeFailure(
      encodeReaderFetchBatchFields(STREAM_READER_FETCH_BATCH_VERSION, 1, STREAM_RUNNER_FETCH_DATA, 202, encodedBlock));
  expectReaderFetchBatchDecodeFailure(
      encodeReaderFetchBatchFields(STREAM_READER_FETCH_BATCH_VERSION, 1, STREAM_RUNNER_FETCH_DATA, 101, {}));
  expectReaderFetchBatchDecodeFailure(
      encodeReaderFetchBatchFields(STREAM_READER_FETCH_BATCH_VERSION, 1, STREAM_RUNNER_FETCH_EOG, 101, {1}));
  expectReaderFetchBatchDecodeFailure(
      encodeReaderFetchBatchFields(STREAM_READER_FETCH_BATCH_VERSION, INT32_MAX, 0, 0, {}));
  expectReaderFetchBatchDecodeFailure(encodeReaderFetchBatchFields(STREAM_READER_FETCH_BATCH_VERSION, -1, 0, 0, {}));

  auto zeroRowsBlock = encodedBlock;
  ASSERT_GE(zeroRowsBlock.size(), sizeof(int32_t) * 3);
  const int32_t zeroRows = 0;
  memcpy(zeroRowsBlock.data() + sizeof(int32_t) * 2, &zeroRows, sizeof(zeroRows));
  expectReaderFetchBatchDecodeFailure(
      encodeReaderFetchBatchFields(STREAM_READER_FETCH_BATCH_VERSION, 1, STREAM_RUNNER_FETCH_DATA, 101, zeroRowsBlock));

  auto trailing = valid;
  trailing.push_back(0);
  expectReaderFetchBatchDecodeFailure(trailing);

  auto shortBlock = valid;
  ASSERT_GT(shortBlock.size(), encodedBlock.size());
  const size_t blockLengthOffset = shortBlock.size() - encodedBlock.size() - 1;
  ASSERT_LT(encodedBlock.size(), 128);
  ASSERT_EQ(encodedBlock.size(), shortBlock[blockLengthOffset]);
  --shortBlock[blockLengthOffset];
  expectReaderFetchBatchDecodeFailure(shortBlock);

  blockDataDestroy(pBlock);
}

TEST(StreamReaderFetchBatchTest, BuilderRejectsInvalidDataAndClearsOutputs) {
  SSDataBlock *pBlock = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, createDataBlock(&pBlock));
  pBlock->info.id.baseGId = 101;
  SArray *pBlocks = taosArrayInit(1, sizeof(SSDataBlock *));
  ASSERT_NE(nullptr, pBlocks);
  ASSERT_NE(nullptr, taosArrayPush(pBlocks, &pBlock));

  void  *pData = reinterpret_cast<void *>(static_cast<uintptr_t>(1));
  size_t size = 99;
  EXPECT_NE(TSDB_CODE_SUCCESS, streamBuildGroupFetchRsp(pBlocks, true, &pData, &size, TSDB_TIME_PRECISION_MILLI));
  EXPECT_EQ(nullptr, pData);
  EXPECT_EQ(0, size);

  blockDataDestroy(pBlock);
  taosArrayDestroy(pBlocks);

  pBlock = makeReaderFetchDataBlock(101, 11);
  ASSERT_NE(nullptr, pBlock);
  pBlock->info.id.baseGId = 0;
  pBlocks = taosArrayInit(1, sizeof(SSDataBlock *));
  ASSERT_NE(nullptr, pBlocks);
  ASSERT_NE(nullptr, taosArrayPush(pBlocks, &pBlock));
  pData = reinterpret_cast<void *>(static_cast<uintptr_t>(1));
  size = 99;
  EXPECT_NE(TSDB_CODE_SUCCESS, streamBuildGroupFetchRsp(pBlocks, true, &pData, &size, TSDB_TIME_PRECISION_MILLI));
  EXPECT_EQ(nullptr, pData);
  EXPECT_EQ(0, size);
  blockDataDestroy(pBlock);
  taosArrayDestroy(pBlocks);
}

TEST(StreamReaderFetchBatchTest, PureEogAndEofOnlyDoNotCountAsData) {
  SSDataBlock *pEog = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, createDataBlock(&pEog));
  ASSERT_EQ(TSDB_CODE_SUCCESS, qSetMultiGroupEogBlock(pEog, 101));
  SArray *pBlocks = taosArrayInit(1, sizeof(SSDataBlock *));
  ASSERT_NE(nullptr, pBlocks);
  ASSERT_NE(nullptr, taosArrayPush(pBlocks, &pEog));

  void  *pData = nullptr;
  size_t size = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, streamBuildGroupFetchRsp(pBlocks, true, &pData, &size, TSDB_TIME_PRECISION_MICRO));
  const auto *pRsp = static_cast<const SRetrieveTableRsp *>(pData);
  EXPECT_EQ(0, pRsp->completed);
  EXPECT_EQ(0, ntohl(pRsp->numOfBlocks));
  EXPECT_EQ(0, be64toh(pRsp->numOfRows));
  EXPECT_EQ(0, be64toh(pRsp->numOfCols));
  EXPECT_EQ(ntohl(pRsp->payloadLen), ntohl(pRsp->compLen));
  SArray *pItems = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDecodeStreamReaderFetchBatch(pRsp->data, ntohl(pRsp->payloadLen), &pItems));
  ASSERT_EQ(1, taosArrayGetSize(pItems));
  const auto *pItem = static_cast<const SStreamReaderFetchItem *>(taosArrayGet(pItems, 0));
  ASSERT_NE(nullptr, pItem);
  EXPECT_EQ(STREAM_RUNNER_FETCH_EOG, pItem->type);
  EXPECT_EQ(101U, pItem->baseGId);
  EXPECT_EQ(nullptr, pItem->pBlock);
  taosArrayDestroyEx(pItems, tDestroyStreamReaderFetchItem);
  rpcFreeCont(pData);

  pData = nullptr;
  size = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, streamBuildGroupFetchRsp(nullptr, false, &pData, &size, TSDB_TIME_PRECISION_NANO));
  pRsp = static_cast<const SRetrieveTableRsp *>(pData);
  EXPECT_EQ(1, pRsp->completed);
  EXPECT_EQ(0, ntohl(pRsp->numOfBlocks));
  EXPECT_EQ(0, be64toh(pRsp->numOfRows));
  pItems = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDecodeStreamReaderFetchBatch(pRsp->data, ntohl(pRsp->payloadLen), &pItems));
  EXPECT_EQ(0, taosArrayGetSize(pItems));
  taosArrayDestroyEx(pItems, tDestroyStreamReaderFetchItem);
  rpcFreeCont(pData);

  blockDataDestroy(pEog);
  taosArrayDestroy(pBlocks);
}

void releaseStackReaderTask(void *pParam) {
  auto **ppTask = static_cast<SStreamReaderTask **>(pParam);
  streamTaskStatsHandleLifecycle(&(*ppTask)->pStats, STREAM_TASK_STATS_OWNER_DESTROYED);
  *ppTask = nullptr;
}

void appendReaderPolicy(SStreamRuntimeFuncInfo *runtime, int64_t gid, int32_t paramIndex) {
  if (runtime->pContextPolicy == nullptr) {
    runtime->pContextPolicy = static_cast<SStreamContextPolicy *>(taosMemoryCalloc(1, sizeof(SStreamContextPolicy)));
    ASSERT_NE(runtime->pContextPolicy, nullptr);
    runtime->pContextPolicy->pEntries = taosArrayInit(1, sizeof(SStreamContextPolicyEntry));
    ASSERT_NE(runtime->pContextPolicy->pEntries, nullptr);
  }
  SStreamContextPolicyEntry entry = {};
  entry.gid = gid;
  entry.paramIndex = paramIndex;
  entry.contextPolicy = STREAM_CONTEXT_POLICY_ANCESTOR;
  ASSERT_NE(taosArrayPush(runtime->pContextPolicy->pEntries, &entry), nullptr);
}

void appendReaderNonePolicy(SStreamRuntimeFuncInfo *runtime, int64_t gid, int32_t paramIndex) {
  if (runtime->pContextPolicy == nullptr) {
    runtime->pContextPolicy = static_cast<SStreamContextPolicy *>(taosMemoryCalloc(1, sizeof(SStreamContextPolicy)));
    ASSERT_NE(runtime->pContextPolicy, nullptr);
    runtime->pContextPolicy->pEntries = taosArrayInit(1, sizeof(SStreamContextPolicyEntry));
    ASSERT_NE(runtime->pContextPolicy->pEntries, nullptr);
  }
  SStreamContextPolicyEntry entry = {};
  entry.gid = gid;
  entry.paramIndex = paramIndex;
  entry.contextPolicy = STREAM_CONTEXT_POLICY_NONE;
  ASSERT_NE(taosArrayPush(runtime->pContextPolicy->pEntries, &entry), nullptr);
}

void appendReaderAncestorContext(SStreamRuntimeFuncInfo *runtime, int64_t gid, int32_t paramIndex, int64_t lineageStart,
                                 int64_t ancestorStart) {
  if (runtime->pAncestorContext == nullptr) {
    runtime->pAncestorContext =
        static_cast<SStreamAncestorContext *>(taosMemoryCalloc(1, sizeof(SStreamAncestorContext)));
    ASSERT_NE(runtime->pAncestorContext, nullptr);
    runtime->pAncestorContext->pParamContexts = taosArrayInit(1, sizeof(SStreamAncestorParamContext));
    ASSERT_NE(runtime->pAncestorContext->pParamContexts, nullptr);
  }

  SStreamAncestorParamContext context = {};
  context.paramIndex = paramIndex;
  context.leafIdentity.gid = gid;
  context.leafIdentity.triggerType = WINDOW_TYPE_COUNT;
  context.leafIdentity.openingTs = lineageStart + 10;
  context.leafIdentity.nativeDiscriminator = paramIndex + 1;
  context.leafIdentity.lineage.pScopes = taosArrayInit(1, sizeof(SScopeInstanceId));
  ASSERT_NE(context.leafIdentity.lineage.pScopes, nullptr);
  SScopeInstanceId scope = {};
  scope.layerIndex = 0;
  scope.triggerType = WINDOW_TYPE_INTERVAL;
  scope.openingTs = lineageStart;
  scope.nativeDiscriminator = 7;
  ASSERT_NE(taosArrayPush(context.leafIdentity.lineage.pScopes, &scope), nullptr);
  context.pSnapshots = taosArrayInit(1, sizeof(SWindowAncestorSnapshot));
  ASSERT_NE(context.pSnapshots, nullptr);
  SWindowAncestorSnapshot snapshot = {};
  snapshot.layerIndex = 0;
  snapshot.triggerType = WINDOW_TYPE_INTERVAL;
  snapshot.placeholderMask = PLACE_HOLDER_WSTART | PLACE_HOLDER_WEND;
  snapshot.values.window.start = ancestorStart;
  snapshot.values.window.end = ancestorStart + 99;
  snapshot.values.window.duration = 100;
  snapshot.values.window.rownum = 1;
  ASSERT_NE(taosArrayPush(context.pSnapshots, &snapshot), nullptr);
  ASSERT_NE(taosArrayPush(runtime->pAncestorContext->pParamContexts, &context), nullptr);
}

void appendReaderBinding(SStreamRuntimeFuncInfo *runtime, int32_t vgId, int32_t readInfoIndex,
                         int32_t paramContextIndex) {
  if (runtime->pAncestorContext->pReadScopeBindings == nullptr) {
    runtime->pAncestorContext->pReadScopeBindings = taosArrayInit(1, sizeof(SStreamReadScopeBinding));
    ASSERT_NE(runtime->pAncestorContext->pReadScopeBindings, nullptr);
  }
  const auto *context = static_cast<const SStreamAncestorParamContext *>(
      taosArrayGet(runtime->pAncestorContext->pParamContexts, paramContextIndex));
  ASSERT_NE(context, nullptr);
  SStreamReadScopeBinding binding = {};
  binding.vgId = vgId;
  binding.readInfoIndex = readInfoIndex;
  binding.scope.gid = context->leafIdentity.gid;
  binding.scope.lineage.pScopes = taosArrayDup(context->leafIdentity.lineage.pScopes, nullptr);
  ASSERT_NE(binding.scope.lineage.pScopes, nullptr);
  ASSERT_NE(taosArrayPush(runtime->pAncestorContext->pReadScopeBindings, &binding), nullptr);
}

void extendReaderAncestorDepth(SStreamAncestorParamContext *context, int32_t ancestorDepth) {
  ASSERT_NE(context, nullptr);
  ASSERT_TRUE(ancestorDepth == 1 || ancestorDepth == 3);
  while (taosArrayGetSize(context->leafIdentity.lineage.pScopes) < ancestorDepth) {
    const int32_t    layer = taosArrayGetSize(context->leafIdentity.lineage.pScopes);
    SScopeInstanceId scope = {};
    scope.layerIndex = layer;
    scope.triggerType = WINDOW_TYPE_COUNT;
    scope.openingTs = layer * 100;
    scope.nativeDiscriminator = layer + 20;
    ASSERT_NE(taosArrayPush(context->leafIdentity.lineage.pScopes, &scope), nullptr);
    SWindowAncestorSnapshot snapshot = {};
    snapshot.layerIndex = layer;
    snapshot.triggerType = WINDOW_TYPE_COUNT;
    snapshot.values.window.start = layer * 100;
    snapshot.values.window.end = layer * 100 + 99;
    snapshot.values.window.duration = 100;
    snapshot.values.window.rownum = 1;
    ASSERT_NE(taosArrayPush(context->pSnapshots, &snapshot), nullptr);
  }
}

SStreamRuntimeFuncInfo makeReaderFetchScaleRuntime(int32_t totalParams, int32_t nodeCount, ReaderFetchLayout layout,
                                                   int32_t ancestorDepth) {
  SStreamRuntimeFuncInfo runtime = {};
  EXPECT_GT(totalParams, 0);
  EXPECT_GT(nodeCount, 0);
  EXPECT_TRUE(ancestorDepth == 1 || ancestorDepth == 3);
  if (totalParams <= 0 || nodeCount <= 0 || (ancestorDepth != 1 && ancestorDepth != 3)) return runtime;

  const auto    groups = reader_fetch_benchmark::makeScalePlan(totalParams, nodeCount, layout);
  const int32_t groupCount = static_cast<int32_t>(groups.size());
  runtime.isMultiGroupCalc = true;
  runtime.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  runtime.sessionId = 33;
  runtime.triggerType = STREAM_TRIGGER_COUNT;
  runtime.isWindowTrigger = true;
  runtime.precision = TSDB_TIME_PRECISION_MILLI;
  runtime.pGroupCalcInfos = tSimpleHashInit(groupCount, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  runtime.pGroupReadInfos = tSimpleHashInit(nodeCount, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  runtime.pGroupCalcOrder = taosArrayInit(groupCount, sizeof(uint64_t));
  EXPECT_NE(runtime.pGroupCalcInfos, nullptr);
  EXPECT_NE(runtime.pGroupReadInfos, nullptr);
  EXPECT_NE(runtime.pGroupCalcOrder, nullptr);
  tSimpleHashSetFreeFp(runtime.pGroupCalcInfos, tDestroySSTriggerGroupCalcInfo);
  tSimpleHashSetFreeFp(runtime.pGroupReadInfos, tDestroySSTriggerGroupReadInfoArray);

  std::vector<SArray *> nodeReads(nodeCount, nullptr);
  for (int32_t node = 0; node < nodeCount; ++node) {
    nodeReads[node] = taosArrayInit(1, sizeof(SSTriggerGroupReadInfo));
    EXPECT_NE(nodeReads[node], nullptr);
  }

  int32_t globalParamIndex = 0;
  for (const auto &group : groups) {
    const int64_t gid = group.gid;
    EXPECT_NE(taosArrayPush(runtime.pGroupCalcOrder, &gid), nullptr);
    SSTriggerGroupCalcInfo calc = {};
    calc.pParams = taosArrayInit(group.paramCount, sizeof(SSTriggerCalcParam));
    EXPECT_NE(calc.pParams, nullptr);
    for (int32_t paramIndex = 0; paramIndex < group.paramCount; ++paramIndex) {
      SSTriggerCalcParam param = {};
      param.wstart = static_cast<int64_t>(globalParamIndex) * 100;
      param.wend = param.wstart + 99;
      param.notifyType = BIT_FLAG_MASK(0);
      EXPECT_NE(taosArrayPush(calc.pParams, &param), nullptr);
      appendReaderPolicy(&runtime, gid, paramIndex);
      appendReaderAncestorContext(&runtime, gid, paramIndex, group.gid * 1000, group.gid * 1000);
      auto *context =
          static_cast<SStreamAncestorParamContext *>(taosArrayGetLast(runtime.pAncestorContext->pParamContexts));
      extendReaderAncestorDepth(context, ancestorDepth);
      ++globalParamIndex;
    }
    EXPECT_EQ(TSDB_CODE_SUCCESS, tSimpleHashPut(runtime.pGroupCalcInfos, &gid, sizeof(gid), &calc, sizeof(calc)));
  }

  if (layout == ReaderFetchLayout::SharedGroup) {
    for (const auto &segment : groups.front().segments) {
      for (int32_t paramIndex = segment.beginParam; paramIndex < segment.endParam; ++paramIndex) {
        auto *context = static_cast<SStreamAncestorParamContext *>(
            taosArrayGet(runtime.pAncestorContext->pParamContexts, paramIndex));
        for (int32_t layer = 0; layer < taosArrayGetSize(context->leafIdentity.lineage.pScopes); ++layer) {
          auto *scope = static_cast<SScopeInstanceId *>(taosArrayGet(context->leafIdentity.lineage.pScopes, layer));
          scope->openingTs += static_cast<int64_t>(segment.nodeIndex) * 100000;
          scope->nativeDiscriminator += segment.nodeIndex * 100;
        }
      }
    }
  }

  for (const auto &group : groups) {
    const int64_t gid = group.gid;
    auto *calc = static_cast<SSTriggerGroupCalcInfo *>(tSimpleHashGet(runtime.pGroupCalcInfos, &gid, sizeof(gid)));
    EXPECT_NE(calc, nullptr);
    for (const auto &segment : group.segments) {
      SSTriggerGroupReadInfo read = {};
      read.gid = gid;
      read.firstParam = *static_cast<SSTriggerCalcParam *>(taosArrayGet(calc->pParams, segment.beginParam));
      read.lastParam = *static_cast<SSTriggerCalcParam *>(taosArrayGet(calc->pParams, segment.endParam - 1));
      const int32_t readInfoIndex = taosArrayGetSize(nodeReads[segment.nodeIndex]);
      EXPECT_NE(taosArrayPush(nodeReads[segment.nodeIndex], &read), nullptr);
      appendReaderBinding(&runtime, segment.nodeIndex + 1, readInfoIndex, group.paramOffset + segment.beginParam);
    }
  }
  for (int32_t node = 0; node < nodeCount; ++node) {
    if (taosArrayGetSize(nodeReads[node]) == 0) {
      taosArrayDestroy(nodeReads[node]);
      continue;
    }
    const int32_t nodeId = node + 1;
    EXPECT_EQ(TSDB_CODE_SUCCESS,
              tSimpleHashPut(runtime.pGroupReadInfos, &nodeId, sizeof(nodeId), &nodeReads[node], POINTER_BYTES));
  }
  EXPECT_EQ(totalParams, globalParamIndex);
  SSTriggerCalcRequest validation = {};
  validation.isMultiGroupCalc = true;
  validation.pGroupCalcInfos = runtime.pGroupCalcInfos;
  validation.pGroupReadInfos = runtime.pGroupReadInfos;
  validation.pContextPolicy = runtime.pContextPolicy;
  validation.pAncestorContext = runtime.pAncestorContext;
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateSTriggerCalcRequest(&validation));
  EXPECT_EQ(TSDB_CODE_SUCCESS, tAdmitStreamContext(runtime.pContextPolicy, runtime.pAncestorContext, true));
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateStreamCalcContextForFetch(&runtime, true, false));
  return runtime;
}

TEST(StreamReaderFetchScaleTest, BoundaryInputsDecodeAndPassNodeAdmission) {
  struct BoundaryCase {
    int32_t           params;
    int32_t           nodes;
    ReaderFetchLayout layout;
    int32_t           depth;
  };
  for (const auto &testCase :
       {BoundaryCase{2, 3, ReaderFetchLayout::Even, 1}, BoundaryCase{128, 3, ReaderFetchLayout::Skewed, 3},
        BoundaryCase{128, 3, ReaderFetchLayout::SharedGroup, 3}, BoundaryCase{40960, 30, ReaderFetchLayout::Even, 1}}) {
    SCOPED_TRACE(testCase.params);
    SStreamRuntimeFuncInfo runtime =
        makeReaderFetchScaleRuntime(testCase.params, testCase.nodes, testCase.layout, testCase.depth);
    const int32_t nodeId = 1;
    auto        **reads = static_cast<SArray **>(tSimpleHashGet(runtime.pGroupReadInfos, &nodeId, sizeof(nodeId)));
    ASSERT_NE(reads, nullptr);
    runtime.curNodeId = nodeId;
    runtime.curGrpRead = *reads;
    runtime.curGrpReadOwnership = STREAM_GRP_READ_BORROWED;
    SResFetchReq request = {};
    request.header.vgId = nodeId;
    request.reset = true;
    request.readerGroupMode = true;
    request.readerFetchId = 300;
    request.pStRtFuncInfo = &runtime;
    void   *wire = nullptr;
    int32_t wireLen = 0;
    ASSERT_EQ(TSDB_CODE_SUCCESS, tBuildStreamReaderFetchReq(&request, &wire, &wireLen));
    SResFetchReq decoded = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(wire, wireLen, &decoded));
    ASSERT_NE(decoded.pStRtFuncInfo, nullptr);
    EXPECT_EQ(TSDB_CODE_SUCCESS, stValidateNodeGroupReadBindings(decoded.pStRtFuncInfo, nodeId));
    const auto expected = reader_fetch_benchmark::expectedSelection(
        reader_fetch_benchmark::makeScalePlan(testCase.params, testCase.nodes, testCase.layout), nodeId - 1);
    EXPECT_EQ(expected.first, tSimpleHashGetSize(decoded.pStRtFuncInfo->pGroupCalcInfos));
    EXPECT_EQ(expected.second, reader_fetch_benchmark::paramCount(decoded.pStRtFuncInfo));
    tDestroySResFetchReq(&decoded);
    taosMemoryFree(wire);
    runtime.curGrpRead = nullptr;
    runtime.curGrpReadOwnership = STREAM_GRP_READ_BORROWED;
    tDestroyStRtFuncInfo(&runtime);
  }
}

TEST(StreamReaderFetchBenchmark, DISABLED_Scale) {
  const std::array<int32_t, 4>           params = {2, 128, 4096, 40960};
  const std::array<int32_t, 3>           nodes = {1, 3, 30};
  const std::array<ReaderFetchLayout, 3> layouts = {ReaderFetchLayout::Even, ReaderFetchLayout::Skewed,
                                                    ReaderFetchLayout::SharedGroup};
  const std::array<int32_t, 2>           depths = {1, 3};

  for (int32_t totalParams : params) {
    for (int32_t nodeCount : nodes) {
      for (ReaderFetchLayout layout : layouts) {
        for (int32_t ancestorDepth : depths) {
          SStreamRuntimeFuncInfo runtime = makeReaderFetchScaleRuntime(totalParams, nodeCount, layout, ancestorDepth);
          ASSERT_EQ(TSDB_CODE_SUCCESS, tValidateStreamCalcContextForFetch(&runtime, true, false));
          for (int32_t nodeId = 1; nodeId <= nodeCount; ++nodeId) {
            auto **reads = static_cast<SArray **>(tSimpleHashGet(runtime.pGroupReadInfos, &nodeId, sizeof(nodeId)));
            if (reads == nullptr || taosArrayGetSize(*reads) == 0) {
              reader_fetch_benchmark::emit("admission", "skip", totalParams, nodeCount, layout, ancestorDepth, nodeId,
                                           0, 0, 0, 0, 0, 0, 0);
              continue;
            }
            runtime.curNodeId = nodeId;
            runtime.curGrpRead = *reads;
            runtime.curGrpReadOwnership = STREAM_GRP_READ_BORROWED;
            SResFetchReq request = {};
            request.header.vgId = nodeId;
            request.reset = true;
            request.readerGroupMode = true;
            request.readerFetchId = 500;
            request.pStRtFuncInfo = &runtime;
            void   *wire = nullptr;
            int32_t wireLen = 0;
            ASSERT_EQ(TSDB_CODE_SUCCESS, tBuildStreamReaderFetchReq(&request, &wire, &wireLen));
            SResFetchReq warmup = {};
            ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(wire, wireLen, &warmup));
            ASSERT_NE(warmup.pStRtFuncInfo, nullptr);
            ASSERT_EQ(TSDB_CODE_SUCCESS, stValidateNodeGroupReadBindings(warmup.pStRtFuncInfo, nodeId));
            tDestroySResFetchReq(&warmup);

            for (int32_t sample = 0; sample < 3; ++sample) {
              SResFetchReq  decoded = {};
              const int64_t decodeNs = reader_fetch_benchmark::elapsedNs(
                  [&] { ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(wire, wireLen, &decoded)); });
              ASSERT_NE(decoded.pStRtFuncInfo, nullptr);
              const int64_t admissionNs = reader_fetch_benchmark::elapsedNs([&] {
                ASSERT_EQ(TSDB_CODE_SUCCESS, stValidateNodeGroupReadBindings(decoded.pStRtFuncInfo, nodeId));
              });
              reader_fetch_benchmark::emit("admission", "pruned", totalParams, nodeCount, layout, ancestorDepth, nodeId,
                                           sample, tSimpleHashGetSize(decoded.pStRtFuncInfo->pGroupCalcInfos),
                                           reader_fetch_benchmark::paramCount(decoded.pStRtFuncInfo), wireLen, 0,
                                           decodeNs, admissionNs);
              tDestroySResFetchReq(&decoded);
            }
            taosMemoryFree(wire);
            runtime.curGrpRead = nullptr;
            runtime.curGrpReadOwnership = STREAM_GRP_READ_BORROWED;
          }
          tDestroyStRtFuncInfo(&runtime);
        }
      }
    }
  }
}

const SStreamAncestorParamContext *onlyProjectedReaderContext(const SStreamRuntimeFuncInfo &runtime) {
  if (runtime.pAncestorContext == nullptr || taosArrayGetSize(runtime.pAncestorContext->pParamContexts) != 1) {
    return nullptr;
  }
  return static_cast<const SStreamAncestorParamContext *>(taosArrayGet(runtime.pAncestorContext->pParamContexts, 0));
}

char *makeCalcSubplanJson(bool requiresAncestorContext) {
  SSubplan *subplan = nullptr;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, reinterpret_cast<SNode **>(&subplan)), TSDB_CODE_SUCCESS);
  if (subplan == nullptr) return nullptr;
  subplan->requiresAncestorContext = requiresAncestorContext;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, reinterpret_cast<SNode **>(&subplan->pNode)),
            TSDB_CODE_SUCCESS);

  char   *json = nullptr;
  int32_t length = 0;
  EXPECT_EQ(nodesNodeToString(reinterpret_cast<SNode *>(subplan), false, &json, &length), TSDB_CODE_SUCCESS);
  nodesDestroyNode(reinterpret_cast<SNode *>(subplan));
  return json;
}

struct ScopedReadCalls {
  int32_t hashGet;
  int32_t hashPut;
  int32_t dataCache;
};

ScopedReadCalls gScopedReadCalls = {};
void           *gScopedReadIter = nullptr;
char            gCacheSentinel = 0;

struct CacheReadConcurrencyGate {
  std::mutex              mutex;
  std::condition_variable condition;
  bool                    fetchEntered = false;
  bool                    allowFetch = false;
};

CacheReadConcurrencyGate *gCacheReadConcurrencyGate = nullptr;

class ScopedCacheReadConcurrencyGate {
 public:
  explicit ScopedCacheReadConcurrencyGate(CacheReadConcurrencyGate *gate) {
    EXPECT_EQ(gCacheReadConcurrencyGate, nullptr);
    gCacheReadConcurrencyGate = gate;
  }

  ~ScopedCacheReadConcurrencyGate() { gCacheReadConcurrencyGate = nullptr; }
};

void *countScopedHashGet(SHashObj *, const void *, size_t) {
  ++gScopedReadCalls.hashGet;
  return gScopedReadCalls.hashGet == 1 ? nullptr : &gScopedReadIter;
}

int32_t countScopedHashPut(SHashObj *, const void *, size_t, const void *, size_t) {
  ++gScopedReadCalls.hashPut;
  return TSDB_CODE_SUCCESS;
}

int32_t countScopedDataCache(void *, int64_t, TSKEY, TSKEY, void **pIter) {
  ++gScopedReadCalls.dataCache;
  *pIter = reinterpret_cast<void *>(static_cast<uintptr_t>(0x6001));
  return TSDB_CODE_SUCCESS;
}

int32_t acquireFakeCacheLease(int64_t, int64_t, int64_t, SStreamDataCacheLease **ppLease, void **ppCache) {
  ++gScopedReadCalls.dataCache;
  *ppLease = nullptr;
  *ppCache = &gCacheSentinel;
  return TSDB_CODE_SUCCESS;
}

void releaseFakeCacheLease(SStreamDataCacheLease **ppLease) { *ppLease = nullptr; }

int32_t makeEmptyScopedIterator(void *, const SStreamCacheScope *, TSKEY, TSKEY, void **ppIter) {
  auto *pIter = static_cast<SResultIter *>(taosMemoryCalloc(1, sizeof(SResultIter)));
  if (pIter == nullptr) return terrno;
  pIter->scopedResult = true;
  *ppIter = pIter;
  return TSDB_CODE_SUCCESS;
}

int32_t makeEmptyLegacyIterator(void *, int64_t, TSKEY, TSKEY, void **ppIter) {
  return makeEmptyScopedIterator(nullptr, nullptr, 0, 0, ppIter);
}

int32_t blockConcurrentCacheRead(void **, SSDataBlock **ppBlock) {
  CacheReadConcurrencyGate *gate = gCacheReadConcurrencyGate;
  if (gate == nullptr) return TSDB_CODE_INTERNAL_ERROR;

  std::unique_lock<std::mutex> lock(gate->mutex);
  gate->fetchEntered = true;
  gate->condition.notify_all();
  if (!gate->condition.wait_for(lock, std::chrono::seconds(5), [gate] { return gate->allowFetch; })) {
    return TSDB_CODE_INTERNAL_ERROR;
  }
  *ppBlock = nullptr;
  return TSDB_CODE_SUCCESS;
}

int32_t removeConcurrentCacheReadScope(SHashObj *, const void *, size_t) { return TSDB_CODE_SUCCESS; }

class StreamReaderCacheTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_EQ(gStreamMgmt.taskMap, nullptr);
    gStreamMgmt.taskMap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
    ASSERT_NE(gStreamMgmt.taskMap, nullptr);

    ASSERT_EQ(stCreateCalcDataCacheIterMap(&scopedIters_), TSDB_CODE_SUCCESS);
    realtime_.sessionId = kSessionId;
    realtime_.pCalcDataCacheIters = scopedIters_;

    task_.task.type = STREAM_TRIGGER_TASK;
    task_.task.streamId = kStreamId;
    task_.task.taskId = kTaskId;
    task_.triggerType = STREAM_TRIGGER_COUNT;
    task_.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
    task_.pRealtimeContext = &realtime_;
    SStreamTask *pTask = reinterpret_cast<SStreamTask *>(&task_);
    ASSERT_EQ(taosHashPut(gStreamMgmt.taskMap, &task_.task.streamId,
                          sizeof(task_.task.streamId) + sizeof(task_.task.taskId), &pTask, POINTER_BYTES),
              TSDB_CODE_SUCCESS);

    gScopedReadCalls = {};
    gScopedReadIter = nullptr;
  }

  void TearDown() override {
    const int64_t key[2] = {kStreamId, kTaskId};
    if (gStreamMgmt.taskMap != nullptr) {
      EXPECT_EQ(taosHashRemove(gStreamMgmt.taskMap, key, sizeof(key)), TSDB_CODE_SUCCESS);
      EXPECT_EQ(taosHashGetSize(gStreamMgmt.taskMap), 0);
      taosHashCleanup(gStreamMgmt.taskMap);
      gStreamMgmt.taskMap = nullptr;
    }
    taosHashCleanup(scopedIters_);
    scopedIters_ = nullptr;
  }

  void expectNoScopedRead() {
    EXPECT_EQ(task_.task.entryLock, 0);
    EXPECT_EQ(gScopedReadCalls.hashGet, 0);
    EXPECT_EQ(gScopedReadCalls.hashPut, 0);
    EXPECT_EQ(gScopedReadCalls.dataCache, 0);
  }

  static constexpr int64_t kStreamId = 0x1234;
  static constexpr int64_t kTaskId = 0x5678;
  static constexpr int64_t kSessionId = 0x9abc;

  SStreamTriggerTask       task_ = {};
  SSTriggerRealtimeContext realtime_ = {};
  SHashObj                *scopedIters_ = nullptr;
};

TEST(StreamReaderTest, DeployCopiesRequiresContextPolicyCapability) {
  SStreamReaderTask task = {};
  task.task.undeployCb = releaseStackReaderTask;
  SStreamReaderDeployMsg msg = {};
  msg.msg.calc.execReplica = 1;
  msg.msg.calc.calcScanPlan = makeCalcSubplanJson(true);
  ASSERT_NE(msg.msg.calc.calcScanPlan, nullptr);

  ASSERT_EQ(stReaderTaskDeploy(&task, &msg), TSDB_CODE_SUCCESS);
  ASSERT_NE(task.info, nullptr);
  auto *info = static_cast<SStreamTriggerReaderCalcInfo *>(taosArrayGetP(static_cast<SArray *>(task.info), 0));
  ASSERT_NE(info, nullptr);
  EXPECT_TRUE(info->requiresContextPolicy);

  SStreamReaderTask *taskPtr = &task;
  ASSERT_EQ(stReaderTaskUndeploy(&taskPtr, true), TSDB_CODE_SUCCESS);
  EXPECT_EQ(taskPtr, nullptr);
  taosMemoryFree(msg.msg.calc.calcScanPlan);
}

TEST(StreamReaderTest, ProjectsSingleGroupFirstAndLastAncestorContextsToTemporaryParam) {
  SStreamRuntimeFuncInfo source = {};
  source.pStreamPesudoFuncVals = taosArrayInit_s(sizeof(SSTriggerCalcParam), 2);
  source.groupId = 101;
  ASSERT_NE(source.pStreamPesudoFuncVals, nullptr);
  appendReaderPolicy(&source, source.groupId, 0);
  appendReaderPolicy(&source, source.groupId, 1);
  appendReaderAncestorContext(&source, source.groupId, 0, 1000, 1100);
  appendReaderAncestorContext(&source, source.groupId, 1, 2000, 2100);

  SStreamRuntimeFuncInfo target = {};
  target.pStreamPesudoFuncVals = taosArrayInit_s(sizeof(SSTriggerCalcParam), 1);
  ASSERT_NE(target.pStreamPesudoFuncVals, nullptr);

  ASSERT_EQ(stProjectReaderCalcContext(&source, 7, -1, 0, &target), TSDB_CODE_SUCCESS);
  EXPECT_EQ(target.groupId, source.groupId);
  EXPECT_EQ(target.curIdx, 0);
  ASSERT_EQ(taosArrayGetSize(target.pContextPolicy->pEntries), 1);
  const auto *first = onlyProjectedReaderContext(target);
  ASSERT_NE(first, nullptr);
  EXPECT_EQ(first->paramIndex, 0);
  const auto *firstSnapshot = static_cast<const SWindowAncestorSnapshot *>(taosArrayGet(first->pSnapshots, 0));
  ASSERT_NE(firstSnapshot, nullptr);
  EXPECT_EQ(firstSnapshot->values.window.start, 1100);

  ASSERT_EQ(stProjectReaderCalcContext(&source, 7, -1, 1, &target), TSDB_CODE_SUCCESS);
  const auto *last = onlyProjectedReaderContext(target);
  ASSERT_NE(last, nullptr);
  EXPECT_EQ(last->paramIndex, 0);
  const auto *lastSnapshot = static_cast<const SWindowAncestorSnapshot *>(taosArrayGet(last->pSnapshots, 0));
  ASSERT_NE(lastSnapshot, nullptr);
  EXPECT_EQ(lastSnapshot->values.window.start, 2100);

  tDestroyStreamContextPolicy(&source.pContextPolicy);
  tDestroyStreamAncestorContext(&source.pAncestorContext);
  taosArrayDestroy(source.pStreamPesudoFuncVals);
  tDestroyStreamContextPolicy(&target.pContextPolicy);
  tDestroyStreamAncestorContext(&target.pAncestorContext);
  taosArrayDestroy(target.pStreamPesudoFuncVals);
}

TEST(StreamReaderTest, ProjectsMultiGroupAncestorContextSelectedByReadBinding) {
  SStreamRuntimeFuncInfo source = {};
  source.isMultiGroupCalc = true;
  source.curGrpRead = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 2);
  ASSERT_NE(source.curGrpRead, nullptr);
  static_cast<SSTriggerGroupReadInfo *>(taosArrayGet(source.curGrpRead, 0))->gid = 101;
  static_cast<SSTriggerGroupReadInfo *>(taosArrayGet(source.curGrpRead, 1))->gid = 202;
  appendReaderPolicy(&source, 101, 0);
  appendReaderPolicy(&source, 202, 3);
  appendReaderAncestorContext(&source, 101, 0, 1000, 1100);
  appendReaderAncestorContext(&source, 202, 3, 2000, 2100);
  appendReaderBinding(&source, 7, 0, 0);
  appendReaderBinding(&source, 7, 1, 1);

  SStreamRuntimeFuncInfo target = {};
  target.pStreamPesudoFuncVals = taosArrayInit_s(sizeof(SSTriggerCalcParam), 1);
  ASSERT_NE(target.pStreamPesudoFuncVals, nullptr);

  ASSERT_EQ(stProjectReaderCalcContext(&source, 7, 1, -1, &target), TSDB_CODE_SUCCESS);
  EXPECT_EQ(target.groupId, 202);
  EXPECT_EQ(target.curIdx, 0);
  ASSERT_EQ(taosArrayGetSize(target.pContextPolicy->pEntries), 1);
  const auto *entry = static_cast<const SStreamContextPolicyEntry *>(taosArrayGet(target.pContextPolicy->pEntries, 0));
  ASSERT_NE(entry, nullptr);
  EXPECT_EQ(entry->gid, 202);
  EXPECT_EQ(entry->paramIndex, 0);
  const auto *context = onlyProjectedReaderContext(target);
  ASSERT_NE(context, nullptr);
  EXPECT_EQ(context->leafIdentity.gid, 202);
  EXPECT_EQ(context->paramIndex, 0);
  const SStreamAncestorParamContext *indexed = nullptr;
  ASSERT_EQ(tLookupStreamAncestorParam(target.pAncestorContext, 202, 0, &indexed), TSDB_CODE_SUCCESS);
  EXPECT_EQ(indexed, context);
  EXPECT_NE(tLookupStreamAncestorParam(target.pAncestorContext, 202, 3, &indexed), TSDB_CODE_SUCCESS);
  EXPECT_EQ(indexed, nullptr);
  const auto *snapshot = static_cast<const SWindowAncestorSnapshot *>(taosArrayGet(context->pSnapshots, 0));
  ASSERT_NE(snapshot, nullptr);
  EXPECT_EQ(snapshot->values.window.start, 2100);
  ASSERT_EQ(taosArrayGetSize(target.pAncestorContext->pReadScopeBindings), 1);
  const auto *binding =
      static_cast<const SStreamReadScopeBinding *>(taosArrayGet(target.pAncestorContext->pReadScopeBindings, 0));
  ASSERT_NE(binding, nullptr);
  EXPECT_EQ(binding->vgId, 7);
  EXPECT_EQ(binding->readInfoIndex, 1);
  EXPECT_NE(stProjectReaderCalcContext(&source, 8, 1, -1, &target), TSDB_CODE_SUCCESS);

  tDestroyStreamContextPolicy(&source.pContextPolicy);
  tDestroyStreamAncestorContext(&source.pAncestorContext);
  taosArrayDestroy(source.curGrpRead);
  tDestroyStreamContextPolicy(&target.pContextPolicy);
  tDestroyStreamAncestorContext(&target.pAncestorContext);
  taosArrayDestroy(target.pStreamPesudoFuncVals);
}

TEST(StreamReaderBindingTest, ValidatesMultipleGidsForOneActualNode) {
  SStreamRuntimeFuncInfo source = {};
  source.isMultiGroupCalc = true;
  source.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  source.curGrpRead = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 3);
  ASSERT_NE(nullptr, source.curGrpRead);
  static_cast<SSTriggerGroupReadInfo *>(taosArrayGet(source.curGrpRead, 0))->gid = 101;
  static_cast<SSTriggerGroupReadInfo *>(taosArrayGet(source.curGrpRead, 1))->gid = 101;
  static_cast<SSTriggerGroupReadInfo *>(taosArrayGet(source.curGrpRead, 2))->gid = 202;
  source.pGroupCalcInfos = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(nullptr, source.pGroupCalcInfos);
  tSimpleHashSetFreeFp(source.pGroupCalcInfos, tDestroySSTriggerGroupCalcInfo);
  SSTriggerCalcParam firstParam = {};
  firstParam.wstart = 1000;
  firstParam.wend = 1099;
  SSTriggerGroupCalcInfo firstCalc = {};
  firstCalc.pParams = taosArrayInit(1, sizeof(SSTriggerCalcParam));
  ASSERT_NE(nullptr, firstCalc.pParams);
  ASSERT_NE(nullptr, taosArrayPush(firstCalc.pParams, &firstParam));
  const int64_t firstGid = 101;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tSimpleHashPut(source.pGroupCalcInfos, &firstGid, sizeof(firstGid), &firstCalc, sizeof(firstCalc)));
  SSTriggerCalcParam secondParam = {};
  secondParam.wstart = 2000;
  secondParam.wend = 2099;
  SSTriggerGroupCalcInfo secondCalc = {};
  secondCalc.pParams = taosArrayInit(1, sizeof(SSTriggerCalcParam));
  ASSERT_NE(nullptr, secondCalc.pParams);
  ASSERT_NE(nullptr, taosArrayPush(secondCalc.pParams, &secondParam));
  const int64_t otherGid = 202;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tSimpleHashPut(source.pGroupCalcInfos, &otherGid, sizeof(otherGid), &secondCalc, sizeof(secondCalc)));
  auto *firstRead = static_cast<SSTriggerGroupReadInfo *>(taosArrayGet(source.curGrpRead, 0));
  auto *secondRead = static_cast<SSTriggerGroupReadInfo *>(taosArrayGet(source.curGrpRead, 1));
  auto *thirdRead = static_cast<SSTriggerGroupReadInfo *>(taosArrayGet(source.curGrpRead, 2));
  firstRead->firstParam = firstRead->lastParam = firstParam;
  secondRead->firstParam = secondRead->lastParam = firstParam;
  thirdRead->firstParam = thirdRead->lastParam = secondParam;
  appendReaderPolicy(&source, 101, 0);
  appendReaderPolicy(&source, 202, 0);
  appendReaderAncestorContext(&source, 101, 0, 1000, 1100);
  appendReaderAncestorContext(&source, 202, 0, 2000, 2100);
  appendReaderBinding(&source, 7, 0, 0);
  appendReaderBinding(&source, 7, 1, 0);
  appendReaderBinding(&source, 7, 2, 1);

  EXPECT_EQ(TSDB_CODE_SUCCESS, stValidateNodeGroupReadBindings(&source, 7));

  taosArrayDestroy(source.curGrpRead);
  tSimpleHashCleanup(source.pGroupCalcInfos);
  tDestroyStreamContextPolicy(&source.pContextPolicy);
  tDestroyStreamAncestorContext(&source.pAncestorContext);
}

TEST(StreamReaderBindingTest, FromTriggerFetchKeepsFullBatchForNodeValidation) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.isMultiGroupCalc = true;
  runtime.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  runtime.curNodeId = 7;
  runtime.groupId = 101;
  runtime.curIdx = 0;
  runtime.curGrpRead = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 1);
  ASSERT_NE(nullptr, runtime.curGrpRead);
  runtime.curGrpReadOwnership = STREAM_GRP_READ_OWNED_DEEP;
  auto *read = static_cast<SSTriggerGroupReadInfo *>(taosArrayGet(runtime.curGrpRead, 0));
  ASSERT_NE(nullptr, read);
  read->gid = 101;
  read->pTables = taosArrayInit_s(sizeof(uint64_t), 1);
  ASSERT_NE(nullptr, read->pTables);
  const uint64_t uid = 9001;
  ASSERT_NE(nullptr, taosArrayPush(read->pTables, &uid));

  runtime.pGroupCalcInfos = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(nullptr, runtime.pGroupCalcInfos);
  tSimpleHashSetFreeFp(runtime.pGroupCalcInfos, tDestroySSTriggerGroupCalcInfo);
  SSTriggerGroupCalcInfo calc = {};
  calc.pParams = taosArrayInit(2, sizeof(SSTriggerCalcParam));
  ASSERT_NE(nullptr, calc.pParams);
  SSTriggerCalcParam param = {};
  param.wstart = 1000;
  param.wend = 1099;
  ASSERT_NE(nullptr, taosArrayPush(calc.pParams, &param));
  SSTriggerCalcParam otherParam = {};
  otherParam.wstart = 2000;
  otherParam.wend = 2099;
  ASSERT_NE(nullptr, taosArrayPush(calc.pParams, &otherParam));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tSimpleHashPut(runtime.pGroupCalcInfos, &read->gid, sizeof(read->gid), &calc, sizeof(calc)));
  read->firstParam = param;
  read->lastParam = param;
  runtime.pGroupReadInfos = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  ASSERT_NE(nullptr, runtime.pGroupReadInfos);
  tSimpleHashSetFreeFp(runtime.pGroupReadInfos, tDestroySSTriggerGroupReadInfoArray);
  SArray *nodeReads = taosArrayInit(1, sizeof(SSTriggerGroupReadInfo));
  ASSERT_NE(nullptr, nodeReads);
  SSTriggerGroupReadInfo sourceRead = *read;
  sourceRead.pTables = nullptr;
  ASSERT_NE(nullptr, taosArrayPush(nodeReads, &sourceRead));
  const int32_t nodeId = 7;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tSimpleHashPut(runtime.pGroupReadInfos, &nodeId, sizeof(nodeId), &nodeReads, POINTER_BYTES));
  SArray *otherNodeReads = taosArrayInit(1, sizeof(SSTriggerGroupReadInfo));
  ASSERT_NE(nullptr, otherNodeReads);
  SSTriggerGroupReadInfo otherRead = sourceRead;
  otherRead.firstParam = otherParam;
  otherRead.lastParam = otherParam;
  ASSERT_NE(nullptr, taosArrayPush(otherNodeReads, &otherRead));
  const int32_t otherNodeId = 8;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tSimpleHashPut(runtime.pGroupReadInfos, &otherNodeId, sizeof(otherNodeId), &otherNodeReads, POINTER_BYTES));
  appendReaderPolicy(&runtime, read->gid, 0);
  appendReaderPolicy(&runtime, read->gid, 1);
  appendReaderAncestorContext(&runtime, read->gid, 0, 1000, 1100);
  appendReaderAncestorContext(&runtime, read->gid, 1, 2000, 2100);
  appendReaderBinding(&runtime, 7, 0, 0);
  appendReaderBinding(&runtime, 8, 0, 1);

  SResFetchReq request = {};
  request.header.vgId = 7;
  request.reset = true;
  request.pStRtFuncInfo = &runtime;
  void   *wire = nullptr;
  int32_t wireLen = 0;
  request.readerFetchId = 200;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tBuildStreamReaderFetchReq(&request, &wire, &wireLen));

  SResFetchReq decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(wire, wireLen, &decoded));
  taosMemoryFree(wire);
  ASSERT_NE(nullptr, decoded.pStRtFuncInfo);
  auto *decodedCalc = static_cast<SSTriggerGroupCalcInfo *>(
      tSimpleHashGet(decoded.pStRtFuncInfo->pGroupCalcInfos, &read->gid, sizeof(read->gid)));
  ASSERT_NE(nullptr, decodedCalc);
  EXPECT_EQ(taosArrayGetSize(decodedCalc->pParams), 2);
  EXPECT_EQ(TSDB_CODE_SUCCESS, stValidateNodeGroupReadBindings(decoded.pStRtFuncInfo, 7));
  auto *decodedRead = static_cast<SSTriggerGroupReadInfo *>(taosArrayGet(decoded.pStRtFuncInfo->curGrpRead, 0));
  ASSERT_NE(nullptr, decodedRead);
  --decodedRead->firstParam.wstart;
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, stValidateNodeGroupReadBindings(decoded.pStRtFuncInfo, 7));
  ++decodedRead->firstParam.wstart;
  ++decodedRead->lastParam.wend;
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, stValidateNodeGroupReadBindings(decoded.pStRtFuncInfo, 7));
  --decodedRead->lastParam.wend;
  auto *decodedBinding = static_cast<SStreamReadScopeBinding *>(
      taosArrayGet(decoded.pStRtFuncInfo->pAncestorContext->pReadScopeBindings, 0));
  ASSERT_NE(nullptr, decodedBinding);
  decodedBinding->vgId = 8;
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, stValidateNodeGroupReadBindings(decoded.pStRtFuncInfo, 7));
  decodedBinding->vgId = 7;
  taosArrayDestroy(decodedBinding->scope.lineage.pScopes);
  decodedBinding->scope.lineage.pScopes = nullptr;
  taosArrayRemove(decoded.pStRtFuncInfo->pAncestorContext->pReadScopeBindings, 0);
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, stValidateNodeGroupReadBindings(decoded.pStRtFuncInfo, 7));

  tDestroySResFetchReq(&decoded);
  tDestroyStRtFuncInfo(&runtime);
}

TEST(StreamReaderBindingTest, DecodedRegularNodeValidatesAtAdmissionLimit) {
  constexpr int32_t      kParamCount = STREAM_CALC_REQ_MAX_WIN_NUM;
  SStreamRuntimeFuncInfo runtime = {};
  runtime.isMultiGroupCalc = true;
  runtime.curGrpRead = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 1);
  ASSERT_NE(nullptr, runtime.curGrpRead);
  runtime.curGrpReadOwnership = STREAM_GRP_READ_OWNED_DEEP;
  auto *read = static_cast<SSTriggerGroupReadInfo *>(taosArrayGet(runtime.curGrpRead, 0));
  ASSERT_NE(nullptr, read);
  read->gid = 101;

  runtime.pGroupCalcInfos = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(nullptr, runtime.pGroupCalcInfos);
  tSimpleHashSetFreeFp(runtime.pGroupCalcInfos, tDestroySSTriggerGroupCalcInfo);
  SSTriggerGroupCalcInfo calc = {};
  calc.pParams = taosArrayInit(kParamCount, sizeof(SSTriggerCalcParam));
  ASSERT_NE(nullptr, calc.pParams);
  for (int32_t i = 0; i < kParamCount; ++i) {
    SSTriggerCalcParam param = {};
    param.wstart = i * 100;
    param.wend = param.wstart + 99;
    ASSERT_NE(nullptr, taosArrayPush(calc.pParams, &param));
  }
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tSimpleHashPut(runtime.pGroupCalcInfos, &read->gid, sizeof(read->gid), &calc, sizeof(calc)));
  read->firstParam = *static_cast<SSTriggerCalcParam *>(taosArrayGet(calc.pParams, 0));
  read->lastParam = *static_cast<SSTriggerCalcParam *>(taosArrayGet(calc.pParams, kParamCount - 1));

  SResFetchReq request = {};
  request.header.vgId = 7;
  request.pStRtFuncInfo = &runtime;
  const int32_t size = tSerializeSResFetchReq(nullptr, 0, &request, true, true);
  ASSERT_GT(size, 0);
  std::vector<uint8_t> bytes(size);
  ASSERT_EQ(size, tSerializeSResFetchReq(bytes.data(), bytes.size(), &request, true, true));

  SResFetchReq decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(bytes.data(), bytes.size(), &decoded));
  ASSERT_NE(nullptr, decoded.pStRtFuncInfo);
  EXPECT_EQ(nullptr, decoded.pStRtFuncInfo->pGroupReadInfos);
  EXPECT_EQ(TSDB_CODE_SUCCESS, stValidateNodeGroupReadBindings(decoded.pStRtFuncInfo, 7));

  SSTriggerCalcParam excess = {};
  excess.wstart = kParamCount * 100LL;
  excess.wend = excess.wstart + 99;
  ASSERT_NE(nullptr, taosArrayPush(calc.pParams, &excess));
  read->lastParam = excess;
  const int32_t oversizedSize = tSerializeSResFetchReq(nullptr, 0, &request, true, true);
  ASSERT_GT(oversizedSize, 0);
  std::vector<uint8_t> oversizedBytes(oversizedSize);
  ASSERT_EQ(oversizedSize, tSerializeSResFetchReq(oversizedBytes.data(), oversizedBytes.size(), &request, true, true));
  SResFetchReq oversized = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(oversizedBytes.data(), oversizedBytes.size(), &oversized));
  ASSERT_NE(nullptr, oversized.pStRtFuncInfo);
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, stValidateNodeGroupReadBindings(oversized.pStRtFuncInfo, 7));

  tDestroySResFetchReq(&oversized);
  tDestroySResFetchReq(&decoded);
  tDestroyStRtFuncInfo(&runtime);
}

TEST(StreamReaderBindingTest, DecodedRegularNodeRejectsZeroParamCalcGroup) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.isMultiGroupCalc = true;
  runtime.curGrpRead = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 1);
  ASSERT_NE(nullptr, runtime.curGrpRead);
  runtime.curGrpReadOwnership = STREAM_GRP_READ_OWNED_DEEP;
  auto *read = static_cast<SSTriggerGroupReadInfo *>(taosArrayGet(runtime.curGrpRead, 0));
  ASSERT_NE(nullptr, read);
  read->gid = 101;

  runtime.pGroupCalcInfos = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(nullptr, runtime.pGroupCalcInfos);
  tSimpleHashSetFreeFp(runtime.pGroupCalcInfos, tDestroySSTriggerGroupCalcInfo);
  SSTriggerGroupCalcInfo calc = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tSimpleHashPut(runtime.pGroupCalcInfos, &read->gid, sizeof(read->gid), &calc, sizeof(calc)));

  SResFetchReq request = {};
  request.header.vgId = 7;
  request.pStRtFuncInfo = &runtime;
  const int32_t size = tSerializeSResFetchReq(nullptr, 0, &request, true, true);
  ASSERT_GT(size, 0);
  std::vector<uint8_t> bytes(size);
  ASSERT_EQ(size, tSerializeSResFetchReq(bytes.data(), bytes.size(), &request, true, true));

  SResFetchReq decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(bytes.data(), bytes.size(), &decoded));
  ASSERT_NE(nullptr, decoded.pStRtFuncInfo);
  const auto *decodedCalc = static_cast<const SSTriggerGroupCalcInfo *>(
      tSimpleHashGet(decoded.pStRtFuncInfo->pGroupCalcInfos, &read->gid, sizeof(read->gid)));
  ASSERT_NE(nullptr, decodedCalc);
  EXPECT_EQ(nullptr, decodedCalc->pParams);
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, stValidateNodeGroupReadBindings(decoded.pStRtFuncInfo, 7));

  tDestroySResFetchReq(&decoded);
  tDestroyStRtFuncInfo(&runtime);
}

TEST(StreamReaderBindingTest, RejectsWrongNodeDuplicateBindingAndUnknownGid) {
  SStreamRuntimeFuncInfo source = {};
  source.isMultiGroupCalc = true;
  source.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  source.curGrpRead = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 1);
  ASSERT_NE(nullptr, source.curGrpRead);
  static_cast<SSTriggerGroupReadInfo *>(taosArrayGet(source.curGrpRead, 0))->gid = 101;
  source.pGroupCalcInfos = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(nullptr, source.pGroupCalcInfos);
  tSimpleHashSetFreeFp(source.pGroupCalcInfos, tDestroySSTriggerGroupCalcInfo);
  SSTriggerCalcParam param = {};
  param.wstart = 1000;
  param.wend = 1099;
  SSTriggerGroupCalcInfo calc = {};
  calc.pParams = taosArrayInit(1, sizeof(SSTriggerCalcParam));
  ASSERT_NE(nullptr, calc.pParams);
  ASSERT_NE(nullptr, taosArrayPush(calc.pParams, &param));
  const int64_t gid = 101;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tSimpleHashPut(source.pGroupCalcInfos, &gid, sizeof(gid), &calc, sizeof(calc)));
  auto *read = static_cast<SSTriggerGroupReadInfo *>(taosArrayGet(source.curGrpRead, 0));
  read->firstParam = read->lastParam = param;
  appendReaderPolicy(&source, gid, 0);
  appendReaderAncestorContext(&source, gid, 0, 1000, 1100);
  appendReaderBinding(&source, 8, 0, 0);

  EXPECT_EQ(TSDB_CODE_INVALID_PARA, stValidateNodeGroupReadBindings(&source, 7));
  appendReaderBinding(&source, 7, 0, 0);
  EXPECT_EQ(TSDB_CODE_SUCCESS, stValidateNodeGroupReadBindings(&source, 7));
  appendReaderBinding(&source, 7, 0, 0);
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, stValidateNodeGroupReadBindings(&source, 7));
  static_cast<SSTriggerGroupReadInfo *>(taosArrayGet(source.curGrpRead, 0))->gid = 999;
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, stValidateNodeGroupReadBindings(&source, 7));

  taosArrayDestroy(source.curGrpRead);
  tSimpleHashCleanup(source.pGroupCalcInfos);
  tDestroyStreamContextPolicy(&source.pContextPolicy);
  tDestroyStreamAncestorContext(&source.pAncestorContext);
}

TEST(StreamReaderBindingTest, OneBindingMayCoverSameLineageFirstAndLastParams) {
  SStreamRuntimeFuncInfo source = {};
  source.isMultiGroupCalc = true;
  source.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  source.curGrpRead = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 1);
  ASSERT_NE(nullptr, source.curGrpRead);
  static_cast<SSTriggerGroupReadInfo *>(taosArrayGet(source.curGrpRead, 0))->gid = 101;
  source.pGroupCalcInfos = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(nullptr, source.pGroupCalcInfos);
  SSTriggerGroupCalcInfo calc = {};
  const int64_t          gid = 101;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tSimpleHashPut(source.pGroupCalcInfos, &gid, sizeof(gid), &calc, sizeof(calc)));
  appendReaderPolicy(&source, gid, 0);
  appendReaderPolicy(&source, gid, 1);
  appendReaderAncestorContext(&source, gid, 0, 1000, 1100);
  appendReaderAncestorContext(&source, gid, 1, 1000, 1200);
  appendReaderBinding(&source, 7, 0, 0);

  SStreamRuntimeFuncInfo target = {};
  target.pStreamPesudoFuncVals = taosArrayInit_s(sizeof(SSTriggerCalcParam), 1);
  ASSERT_NE(nullptr, target.pStreamPesudoFuncVals);
  EXPECT_EQ(TSDB_CODE_SUCCESS, stProjectReaderCalcContext(&source, 7, 0, -1, &target));
  EXPECT_EQ(gid, target.groupId);
  ASSERT_NE(nullptr, target.pContextPolicy);
  EXPECT_EQ(0, static_cast<SStreamContextPolicyEntry *>(taosArrayGet(target.pContextPolicy->pEntries, 0))->paramIndex);
  const auto *projected = onlyProjectedReaderContext(target);
  ASSERT_NE(nullptr, projected);
  const auto *snapshot = static_cast<const SWindowAncestorSnapshot *>(taosArrayGet(projected->pSnapshots, 0));
  ASSERT_NE(nullptr, snapshot);
  EXPECT_EQ(1100, snapshot->values.window.start);

  taosArrayDestroy(source.curGrpRead);
  tSimpleHashCleanup(source.pGroupCalcInfos);
  tDestroyStreamContextPolicy(&source.pContextPolicy);
  tDestroyStreamAncestorContext(&source.pAncestorContext);
  tDestroyStreamContextPolicy(&target.pContextPolicy);
  tDestroyStreamAncestorContext(&target.pAncestorContext);
  taosArrayDestroy(target.pStreamPesudoFuncVals);
}

TEST(StreamReaderTest, MultiGroupReadWithoutAncestorBindingKeepsLeafOnlyTemporaryContext) {
  SStreamRuntimeFuncInfo source = {};
  source.isMultiGroupCalc = true;
  source.curNodeId = 0;
  source.curGrpRead = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 2);
  ASSERT_NE(source.curGrpRead, nullptr);
  static_cast<SSTriggerGroupReadInfo *>(taosArrayGet(source.curGrpRead, 0))->gid = 101;
  static_cast<SSTriggerGroupReadInfo *>(taosArrayGet(source.curGrpRead, 1))->gid = 202;
  appendReaderNonePolicy(&source, 101, 0);
  appendReaderPolicy(&source, 202, 0);
  appendReaderAncestorContext(&source, 202, 0, 2000, 2100);
  appendReaderBinding(&source, 7, 1, 0);

  SStreamRuntimeFuncInfo target = {};
  target.pStreamPesudoFuncVals = taosArrayInit_s(sizeof(SSTriggerCalcParam), 1);
  ASSERT_NE(target.pStreamPesudoFuncVals, nullptr);

  ASSERT_EQ(stProjectReaderCalcContext(&source, 7, 0, -1, &target), TSDB_CODE_SUCCESS);
  EXPECT_EQ(target.groupId, 101);
  EXPECT_EQ(target.curIdx, 0);
  EXPECT_EQ(target.pContextPolicy, nullptr);
  EXPECT_EQ(target.pAncestorContext, nullptr);

  tDestroyStreamContextPolicy(&source.pContextPolicy);
  tDestroyStreamAncestorContext(&source.pAncestorContext);
  taosArrayDestroy(source.curGrpRead);
  taosArrayDestroy(target.pStreamPesudoFuncVals);
}

TEST(StreamReaderTest, ProjectsSameReadInfoIndexIndependentlyForEachActualNode) {
  SStreamRuntimeFuncInfo source = {};
  source.isMultiGroupCalc = true;
  source.curGrpRead = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 1);
  ASSERT_NE(source.curGrpRead, nullptr);
  auto *currentRead = static_cast<SSTriggerGroupReadInfo *>(taosArrayGet(source.curGrpRead, 0));
  ASSERT_NE(currentRead, nullptr);
  currentRead->gid = 101;
  appendReaderPolicy(&source, 101, 0);
  appendReaderPolicy(&source, 202, 0);
  appendReaderAncestorContext(&source, 101, 0, 1000, 1100);
  appendReaderAncestorContext(&source, 202, 0, 2000, 2100);
  appendReaderBinding(&source, 7, 0, 0);
  appendReaderBinding(&source, 8, 0, 1);

  SStreamRuntimeFuncInfo target = {};
  target.pStreamPesudoFuncVals = taosArrayInit_s(sizeof(SSTriggerCalcParam), 1);
  ASSERT_NE(target.pStreamPesudoFuncVals, nullptr);

  int32_t code = stProjectReaderCalcContext(&source, 7, 0, -1, &target);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  if (code == TSDB_CODE_SUCCESS) {
    EXPECT_EQ(target.groupId, 101);
    const auto *context = onlyProjectedReaderContext(target);
    ASSERT_NE(context, nullptr);
    EXPECT_EQ(context->leafIdentity.gid, 101);
  }

  currentRead->gid = 202;
  code = stProjectReaderCalcContext(&source, 8, 0, -1, &target);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  if (code == TSDB_CODE_SUCCESS) {
    EXPECT_EQ(target.groupId, 202);
    const auto *context = onlyProjectedReaderContext(target);
    ASSERT_NE(context, nullptr);
    EXPECT_EQ(context->leafIdentity.gid, 202);
  }

  tDestroyStreamContextPolicy(&source.pContextPolicy);
  tDestroyStreamAncestorContext(&source.pAncestorContext);
  taosArrayDestroy(source.curGrpRead);
  tDestroyStreamContextPolicy(&target.pContextPolicy);
  tDestroyStreamAncestorContext(&target.pAncestorContext);
  taosArrayDestroy(target.pStreamPesudoFuncVals);
}

TEST(StreamReaderTest, RejectsAncestorDependencyWithoutBindingForActualNode) {
  SStreamRuntimeFuncInfo source = {};
  source.isMultiGroupCalc = true;
  source.curGrpRead = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 1);
  ASSERT_NE(source.curGrpRead, nullptr);
  static_cast<SSTriggerGroupReadInfo *>(taosArrayGet(source.curGrpRead, 0))->gid = 101;
  appendReaderPolicy(&source, 101, 0);
  appendReaderAncestorContext(&source, 101, 0, 1000, 1100);

  SStreamRuntimeFuncInfo target = {};
  target.pStreamPesudoFuncVals = taosArrayInit_s(sizeof(SSTriggerCalcParam), 1);
  ASSERT_NE(target.pStreamPesudoFuncVals, nullptr);

  EXPECT_NE(stProjectReaderCalcContext(&source, 7, 0, -1, &target), TSDB_CODE_SUCCESS);

  tDestroyStreamContextPolicy(&source.pContextPolicy);
  tDestroyStreamAncestorContext(&source.pAncestorContext);
  taosArrayDestroy(source.curGrpRead);
  taosArrayDestroy(target.pStreamPesudoFuncVals);
}

TEST_F(StreamReaderCacheTest, NestedMissingPolicyRejectsBeforeScopedRead) {
  Stub scopedReadStubs;
  scopedReadStubs.set(taosHashGet, countScopedHashGet);
  scopedReadStubs.set(taosHashPut, countScopedHashPut);
  scopedReadStubs.set(getStreamDataCache, countScopedDataCache);

  SStreamCacheReadInfo readInfo = {};
  readInfo.taskInfo.streamId = kStreamId;
  readInfo.taskInfo.taskId = kTaskId;
  readInfo.taskInfo.sessionId = kSessionId;
  readInfo.gid = 101;
  readInfo.start = 100;
  readInfo.end = 199;
  bool finished = false;
  EXPECT_EQ(stRunnerFetchDataFromCache(&readInfo, &finished), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(readInfo.pBlock, nullptr);
  expectNoScopedRead();
}

TEST_F(StreamReaderCacheTest, NestedAncestorPolicyWithoutContextRejectsBeforeScopedRead) {
  SStreamContextPolicy policy = {};
  policy.pEntries = taosArrayInit(1, sizeof(SStreamContextPolicyEntry));
  ASSERT_NE(policy.pEntries, nullptr);
  SStreamContextPolicyEntry entry = {};
  entry.gid = 101;
  entry.paramIndex = 0;
  entry.contextPolicy = STREAM_CONTEXT_POLICY_ANCESTOR;
  ASSERT_NE(taosArrayPush(policy.pEntries, &entry), nullptr);

  Stub scopedReadStubs;
  scopedReadStubs.set(taosHashGet, countScopedHashGet);
  scopedReadStubs.set(taosHashPut, countScopedHashPut);
  scopedReadStubs.set(getStreamDataCache, countScopedDataCache);

  SStreamCacheReadInfo readInfo = {};
  readInfo.taskInfo.streamId = kStreamId;
  readInfo.taskInfo.taskId = kTaskId;
  readInfo.taskInfo.sessionId = kSessionId;
  readInfo.gid = 101;
  readInfo.start = 100;
  readInfo.end = 199;
  readInfo.pContextPolicy = &policy;
  bool finished = false;
  EXPECT_EQ(stRunnerFetchDataFromCache(&readInfo, &finished), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(readInfo.pBlock, nullptr);
  expectNoScopedRead();

  taosArrayDestroy(policy.pEntries);
}

TEST_F(StreamReaderCacheTest, NestedTaskRejectsLegacyScopeBeforeCacheRead) {
  SStreamContextPolicy policy = {};
  policy.pEntries = taosArrayInit(1, sizeof(SStreamContextPolicyEntry));
  ASSERT_NE(policy.pEntries, nullptr);

  Stub cacheStubs;
  cacheStubs.set(acquireStreamDataCacheLease, acquireFakeCacheLease);
  cacheStubs.set(releaseStreamDataCacheLease, releaseFakeCacheLease);
  cacheStubs.set(getStreamDataCache, makeEmptyLegacyIterator);

  SStreamCacheReadInfo readInfo = {};
  readInfo.taskInfo.streamId = kStreamId;
  readInfo.taskInfo.taskId = kTaskId;
  readInfo.taskInfo.sessionId = kSessionId;
  readInfo.gid = 101;
  readInfo.start = 100;
  readInfo.end = 199;
  readInfo.pContextPolicy = &policy;
  readInfo.cacheScope.gid = readInfo.gid;
  readInfo.hasCacheScope = true;
  bool finished = false;

  EXPECT_EQ(TSDB_CODE_INVALID_PARA, stRunnerFetchDataFromCache(&readInfo, &finished));
  EXPECT_EQ(0, gScopedReadCalls.dataCache);
  EXPECT_EQ(0, taosHashGetSize(scopedIters_));
  taosArrayDestroy(policy.pEntries);
}

TEST_F(StreamReaderCacheTest, NestedTaskRejectsPreboundPopulatedScopeWithoutRuntime) {
  SStreamContextPolicy policy = {};
  policy.pEntries = taosArrayInit(1, sizeof(SStreamContextPolicyEntry));
  ASSERT_NE(policy.pEntries, nullptr);
  SStreamCacheScope scope = {};
  scope.gid = 101;
  scope.lineage.pScopes = taosArrayInit(1, sizeof(SScopeInstanceId));
  ASSERT_NE(scope.lineage.pScopes, nullptr);
  SScopeInstanceId scopeId = {};
  scopeId.layerIndex = 0;
  scopeId.triggerType = WINDOW_TYPE_INTERVAL;
  scopeId.openingTs = 100;
  scopeId.nativeDiscriminator = 7;
  ASSERT_NE(taosArrayPush(scope.lineage.pScopes, &scopeId), nullptr);

  Stub cacheStubs;
  cacheStubs.set(acquireStreamDataCacheLease, acquireFakeCacheLease);
  cacheStubs.set(releaseStreamDataCacheLease, releaseFakeCacheLease);
  cacheStubs.set(getStreamDataCacheScoped, makeEmptyScopedIterator);

  SStreamCacheReadInfo readInfo = {};
  readInfo.taskInfo.streamId = kStreamId;
  readInfo.taskInfo.taskId = kTaskId;
  readInfo.taskInfo.sessionId = kSessionId;
  readInfo.gid = scope.gid;
  readInfo.start = 100;
  readInfo.end = 199;
  readInfo.pContextPolicy = &policy;
  readInfo.cacheScope = scope;
  readInfo.hasCacheScope = true;
  bool finished = false;

  EXPECT_EQ(TSDB_CODE_INVALID_PARA, stRunnerFetchDataFromCache(&readInfo, &finished));
  EXPECT_FALSE(finished);
  EXPECT_EQ(nullptr, readInfo.pBlock);
  EXPECT_EQ(0, taosHashGetSize(scopedIters_));

  taosArrayDestroy(scope.lineage.pScopes);
  taosArrayDestroy(policy.pEntries);
}

TEST_F(StreamReaderCacheTest, ConcurrentExactScopeCleanupWaitsForActiveFetch) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.groupId = 101;
  runtime.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  appendReaderPolicy(&runtime, runtime.groupId, 0);
  appendReaderAncestorContext(&runtime, runtime.groupId, 0, 1000, 1100);
  SStreamCacheReadInfo   readInfo = {};
  readInfo.taskInfo.streamId = kStreamId;
  readInfo.taskInfo.taskId = kTaskId;
  readInfo.taskInfo.sessionId = kSessionId;
  readInfo.gid = runtime.groupId;
  readInfo.start = 100;
  readInfo.end = 199;
  readInfo.pRuntime = &runtime;

  CacheReadConcurrencyGate       gate;
  ScopedCacheReadConcurrencyGate scopedGate(&gate);
  Stub                           cacheStubs;
  cacheStubs.set(acquireStreamDataCacheLease, acquireFakeCacheLease);
  cacheStubs.set(getStreamDataCacheScoped, makeEmptyScopedIterator);
  cacheStubs.set(getNextStreamDataCache, blockConcurrentCacheRead);
  cacheStubs.set(taosHashRemove, removeConcurrentCacheReadScope);

  auto fetch = std::async(std::launch::async, [&readInfo] {
    bool finished = false;
    return stRunnerFetchDataFromCache(&readInfo, &finished);
  });
  {
    std::unique_lock<std::mutex> lock(gate.mutex);
    ASSERT_TRUE(gate.condition.wait_for(lock, std::chrono::seconds(5), [&gate] { return gate.fetchEntered; }));
  }

  auto cleanup = std::async(std::launch::async, [&readInfo] { return stRemoveStreamCacheReadScope(&readInfo); });
  EXPECT_EQ(cleanup.wait_for(std::chrono::milliseconds(200)), std::future_status::timeout);

  {
    std::lock_guard<std::mutex> lock(gate.mutex);
    gate.allowFetch = true;
  }
  gate.condition.notify_all();

  ASSERT_EQ(fetch.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  EXPECT_EQ(fetch.get(), TSDB_CODE_SUCCESS);
  ASSERT_EQ(cleanup.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  EXPECT_EQ(cleanup.get(), TSDB_CODE_SUCCESS);
  stClearStreamCacheReadScope(&readInfo);
  tDestroyStreamContextPolicy(&runtime.pContextPolicy);
  tDestroyStreamAncestorContext(&runtime.pAncestorContext);
}

}  // namespace

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

namespace {
int     readerGroupEnableCalls;
bool    readerGroupEnableRequested;
bool    readerGroupEnableSupported;
int32_t readerGroupEnableCode;
int32_t readerGroupEnableStub(qTaskInfo_t, bool requested, bool *enabled) {
  ++readerGroupEnableCalls;
  readerGroupEnableRequested = requested;
  *enabled = requested && readerGroupEnableSupported && readerGroupEnableCode == 0;
  return readerGroupEnableCode;
}
}  // namespace

TEST(StreamReaderGroupModeTest, ResetDecidesAndContinuationRetainsCapability) {
  Stub stub;
  stub.set(qEnableReaderGroupOutput, readerGroupEnableStub);
  readerGroupEnableCalls = 0;
  readerGroupEnableSupported = true;
  readerGroupEnableCode = 0;
  SStreamTriggerReaderCalcInfo calc = {};
  SResFetchReq                 req = {};
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, stReaderSetGroupOutputMode(&calc, &req));
  req.reset = true;
  req.readerGroupMode = true;
  ASSERT_EQ(0, stReaderSetGroupOutputMode(&calc, &req));
  EXPECT_TRUE(calc.readerGroupAccepted);
  EXPECT_EQ(1, readerGroupEnableCalls);
  req.reset = false;
  ASSERT_EQ(0, stReaderSetGroupOutputMode(&calc, &req));
  EXPECT_TRUE(calc.readerGroupAccepted);
  EXPECT_EQ(1, readerGroupEnableCalls);
  req.readerGroupMode = false;
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, stReaderSetGroupOutputMode(&calc, &req));
  EXPECT_TRUE(calc.readerGroupAccepted);
  req.reset = true;
  ASSERT_EQ(0, stReaderSetGroupOutputMode(&calc, &req));
  EXPECT_FALSE(calc.readerGroupAccepted);
  EXPECT_FALSE(readerGroupEnableRequested);
}

TEST(StreamReaderGroupModeTest, UnsupportedFallbackAndResetFailureDoNotClaimGroupedOutput) {
  Stub stub;
  stub.set(qEnableReaderGroupOutput, readerGroupEnableStub);
  readerGroupEnableCalls = 0;
  readerGroupEnableSupported = false;
  readerGroupEnableCode = 0;
  SStreamTriggerReaderCalcInfo calc = {};
  SResFetchReq                 req = {};
  req.reset = true;
  req.readerGroupMode = true;
  ASSERT_EQ(0, stReaderSetGroupOutputMode(&calc, &req));
  EXPECT_FALSE(calc.readerGroupAccepted);
  req.reset = false;
  ASSERT_EQ(0, stReaderSetGroupOutputMode(&calc, &req));
  EXPECT_EQ(1, readerGroupEnableCalls);
  req.reset = true;
  readerGroupEnableSupported = true;
  readerGroupEnableCode = TSDB_CODE_OUT_OF_MEMORY;
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, stReaderSetGroupOutputMode(&calc, &req));
  EXPECT_FALSE(calc.readerGroupAccepted);
  EXPECT_FALSE(calc.readerGroupModeSet);
  req.reset = false;
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, stReaderSetGroupOutputMode(&calc, &req));
}

TEST(StreamReaderFetchStateTest, StaleContinuationPreservesReplacement) {
  SStreamTriggerReaderCalcInfo calc = {};
  SStreamRuntimeFuncInfo       runtime = {};
  runtime.isMultiGroupCalc = true;
  runtime.streamGen = 12;
  runtime.sessionId = 1;
  SResFetchReq request = {};
  request.readerFetchId = 200;
  request.queryId = 11;
  request.taskId = 22;
  request.execId = 3;
  request.pStRtFuncInfo = &runtime;
  stReaderCaptureFetchState(&calc.fetchState, &request, 8);
  request.readerFetchId = 100;
  EXPECT_EQ(stReaderCheckFetchContinuation(&calc, &request, 8), TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(calc.fetchState.id, 200);
  request.readerFetchId = 200;
  EXPECT_EQ(stReaderCheckFetchContinuation(&calc, &request, 8), TSDB_CODE_SUCCESS);
  stReaderInvalidateFetchState(&calc);
  EXPECT_TRUE(calc.fetchState.enabled);
  EXPECT_EQ(stReaderCheckFetchContinuation(&calc, &request, 8), TSDB_CODE_INVALID_MSG);
}

TEST(StreamReaderFetchStateTest, DimensionBatchIdentityAndThinContinuation) {
  SStreamTriggerReaderCalcInfo calc = {};
  SStreamRuntimeFuncInfo       runtime = {};
  runtime.isMultiGroupCalc = true;
  runtime.streamGen = 12;
  runtime.sessionId = 3;
  SResFetchReq request = {};
  request.reset = true;
  request.readerFetchId = 205;
  request.queryId = 11;
  request.taskId = 22;
  request.execId = 4;
  request.readerGroupMode = true;
  request.readerDimensionBatch = true;
  request.pStRtFuncInfo = &runtime;
  stReaderCaptureFetchState(&calc.fetchState, &request, 8);
  request.reset = false;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stReaderCheckFetchContinuation(&calc, &request, 8));
  const SStreamReaderFetchState snapshot = calc.fetchState;
  for (int field = 0; field < 9; ++field) {
    SCOPED_TRACE(field);
    SResFetchReq           changed = request;
    SStreamRuntimeFuncInfo changedRuntime = runtime;
    changed.pStRtFuncInfo = &changedRuntime;
    int node = 8;
    switch (field) {
      case 0:
        ++changed.queryId;
        break;
      case 1:
        ++changed.taskId;
        break;
      case 2:
        ++changed.execId;
        break;
      case 3:
        ++node;
        break;
      case 4:
        ++changedRuntime.streamGen;
        break;
      case 5:
        ++changedRuntime.sessionId;
        break;
      case 6:
        ++changed.readerFetchId;
        break;
      case 7:
        changed.readerGroupMode = false;
        break;
      case 8:
        changed.readerDimensionBatch = false;
        break;
    }
    EXPECT_EQ(TSDB_CODE_INVALID_MSG, stReaderCheckFetchContinuation(&calc, &changed, node));
    EXPECT_EQ(0, memcmp(&snapshot, &calc.fetchState, sizeof(snapshot)));
  }
  SStreamContextPolicy unexpected = {};
  runtime.pContextPolicy = &unexpected;
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, stReaderCheckFetchContinuation(&calc, &request, 8));
  EXPECT_EQ(0, memcmp(&snapshot, &calc.fetchState, sizeof(snapshot)));
  runtime.pContextPolicy = nullptr;
  SArray *params = taosArrayInit_s(sizeof(SSTriggerCalcParam), 1);
  ASSERT_NE(nullptr, params);
  SSTriggerGroupCalcInfo group = {};
  group.pParams = params;
  SSHashObj *groups = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(nullptr, groups);
  const int64_t gid = 1000;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tSimpleHashPut(groups, &gid, sizeof(gid), &group, sizeof(group)));
  runtime.pGroupCalcInfos = groups;
  void   *smallWire = nullptr;
  int32_t smallBytes = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tBuildStreamReaderFetchReq(&request, &smallWire, &smallBytes));
  SArray *largeParams = taosArrayInit_s(sizeof(SSTriggerCalcParam), 40960);
  ASSERT_NE(nullptr, largeParams);
  auto *storedGroup = static_cast<SSTriggerGroupCalcInfo *>(tSimpleHashGet(groups, &gid, sizeof(gid)));
  ASSERT_NE(nullptr, storedGroup);
  storedGroup->pParams = largeParams;
  void   *largeWire = nullptr;
  int32_t largeBytes = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tBuildStreamReaderFetchReq(&request, &largeWire, &largeBytes));
  EXPECT_EQ(smallBytes, largeBytes);
  SResFetchReq decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(largeWire, largeBytes, &decoded));
  EXPECT_TRUE(decoded.readerDimensionBatch);
  EXPECT_EQ(nullptr, decoded.pStRtFuncInfo->pGroupCalcInfos);
  EXPECT_EQ(nullptr, decoded.pStRtFuncInfo->pContextPolicy);
  EXPECT_EQ(nullptr, decoded.pStRtFuncInfo->pAncestorContext);
  tDestroySResFetchReq(&decoded);
  taosMemoryFree(smallWire);
  taosMemoryFree(largeWire);
  tSimpleHashCleanup(groups);
  taosArrayDestroy(params);
  taosArrayDestroy(largeParams);
}

class StreamReaderFetchIdentityTest : public testing::TestWithParam<int> {};
TEST_P(StreamReaderFetchIdentityTest, MismatchDoesNotChangeState) {
  SStreamTriggerReaderCalcInfo calc = {};
  SStreamRuntimeFuncInfo       runtime = {};
  runtime.isMultiGroupCalc = true;
  runtime.streamGen = 12;
  runtime.sessionId = 1;
  SResFetchReq request = {};
  request.readerFetchId = 200;
  request.queryId = 11;
  request.taskId = 22;
  request.execId = 3;
  request.pStRtFuncInfo = &runtime;
  stReaderCaptureFetchState(&calc.fetchState, &request, 8);
  int node = 8;
  switch (GetParam()) {
    case 0:
      request.readerFetchId = 0;
      break;
    case 1:
      ++request.queryId;
      break;
    case 2:
      ++request.taskId;
      break;
    case 3:
      ++request.execId;
      break;
    case 4:
      ++node;
      break;
    case 5:
      ++runtime.streamGen;
      break;
    case 6:
      ++runtime.sessionId;
      break;
    case 7:
      request.dynTbname = true;
      break;
    case 8:
      request.readerGroupMode = true;
      break;
    case 9:
      request.reset = true;
      break;
    case 10:
      request.reuseReader = true;
      break;
    case 11:
      runtime.isMultiGroupCalc = false;
      break;
  }
  EXPECT_EQ(stReaderCheckFetchContinuation(&calc, &request, node), TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(calc.fetchState.id, 200);
}
INSTANTIATE_TEST_SUITE_P(Identity, StreamReaderFetchIdentityTest, testing::Range(0, 12));

TEST(StreamReaderFetchStateTest, ContinuationRejectsAttachedContext) {
  SStreamTriggerReaderCalcInfo calc = {};
  SStreamRuntimeFuncInfo       runtime = {};
  runtime.isMultiGroupCalc = true;
  SResFetchReq request = {};
  request.readerFetchId = 200;
  request.pStRtFuncInfo = &runtime;
  stReaderCaptureFetchState(&calc.fetchState, &request, 8);
  SSHashObj *hash = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(hash, nullptr);
  SArray                 array = {};
  SStreamContextPolicy   policy = {};
  SStreamAncestorContext ancestor = {};
  SStreamContextFrames   frames = {};
  SOperatorParam         op = {};
  for (int field = 0; field < 7; ++field) {
    SCOPED_TRACE(field);
    runtime.pGroupCalcInfos = field == 0 ? hash : nullptr;
    runtime.pGroupReadInfos = field == 1 ? hash : nullptr;
    runtime.curGrpRead = field == 2 ? &array : nullptr;
    runtime.pContextPolicy = field == 3 ? &policy : nullptr;
    runtime.pAncestorContext = field == 4 ? &ancestor : nullptr;
    request.pContextFrames = field == 5 ? &frames : nullptr;
    request.pOpParam = field == 6 ? &op : nullptr;
    EXPECT_EQ(stReaderCheckFetchContinuation(&calc, &request, 8), TSDB_CODE_INVALID_MSG);
    EXPECT_EQ(calc.fetchState.id, 200);
  }
  tSimpleHashCleanup(hash);
}

namespace {
int  dimensionOwnerDestroyCalls = 0;
void destroyDimensionBorrower(qTaskInfo_t task) {
  if (task == nullptr) return;
  auto *calc = static_cast<SStreamTriggerReaderCalcInfo *>(task);
  EXPECT_TRUE(calc->dimensionBatch.installed);
  EXPECT_NE(nullptr, calc->dimensionBatch.runtime.pGroupCalcInfos);
  EXPECT_NE(nullptr, calc->rtInfo.funcInfo.pStreamPesudoFuncVals);
  EXPECT_EQ(2, taosArrayGetSize(calc->rtInfo.funcInfo.pStreamPesudoFuncVals));
  ++dimensionOwnerDestroyCalls;
}
}  // namespace

TEST(StreamReaderDimensionBatchTest, BindsAllWindowsAndClearsBorrowerBeforeOwner) {
  Stub probe;
  probe.set(qDestroyTask, destroyDimensionBorrower);
  dimensionOwnerDestroyCalls = 0;
  SStreamTriggerReaderCalcInfo calc = {};
  auto                        &batch = calc.dimensionBatch;
  batch.installed = true;
  batch.runtime.isMultiGroupCalc = true;
  batch.runtime.pGroupCalcInfos = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(nullptr, batch.runtime.pGroupCalcInfos);
  tSimpleHashSetFreeFp(batch.runtime.pGroupCalcInfos, tDestroySSTriggerGroupCalcInfo);
  SSTriggerGroupCalcInfo group = {};
  group.pParams = taosArrayInit(2, sizeof(SSTriggerCalcParam));
  group.pGroupColVals = taosArrayInit(1, sizeof(SStreamGroupValue));
  group.createTable = 1;
  for (int64_t start : {100, 300}) {
    SSTriggerCalcParam param = {};
    param.wstart = start;
    ASSERT_NE(nullptr, taosArrayPush(group.pParams, &param));
  }
  const uint64_t gid = UINT64_C(0x8000000000000001);
  ASSERT_EQ(0, tSimpleHashPut(batch.runtime.pGroupCalcInfos, &gid, sizeof(gid), &group, sizeof(group)));
  batch.pGroupOrder = taosArrayInit(1, sizeof(uint64_t));
  ASSERT_NE(nullptr, taosArrayPush(batch.pGroupOrder, &gid));
  ASSERT_EQ(0, tProjectStreamCalcContextForAllGroups(nullptr, nullptr, &batch.pContextViews));
  ASSERT_EQ(0, stReaderBindDimensionGroup(&calc, gid));
  EXPECT_FALSE(calc.rtInfo.funcInfo.isMultiGroupCalc);
  EXPECT_EQ(gid, static_cast<uint64_t>(calc.rtInfo.funcInfo.groupId));
  EXPECT_EQ(group.pParams, calc.rtInfo.funcInfo.pStreamPesudoFuncVals);
  EXPECT_EQ(group.pGroupColVals, calc.rtInfo.funcInfo.pStreamPartColVals);
  EXPECT_EQ(nullptr, calc.rtInfo.funcInfo.pGroupCalcInfos);
  EXPECT_EQ(nullptr, calc.rtInfo.funcInfo.curGrpRead);
  EXPECT_EQ(1, *calc.rtInfo.funcInfo.createTable);
  EXPECT_NE(0, stReaderBindDimensionGroup(&calc, 7));
  EXPECT_EQ(group.pParams, calc.rtInfo.funcInfo.pStreamPesudoFuncVals);
  calc.pTaskInfo = &calc;
  stReaderClearDimensionBatch(&calc);
  EXPECT_EQ(1, dimensionOwnerDestroyCalls);
  EXPECT_EQ(nullptr, calc.pTaskInfo);
  EXPECT_EQ(nullptr, calc.rtInfo.funcInfo.pStreamPesudoFuncVals);
  EXPECT_EQ(nullptr, batch.runtime.pGroupCalcInfos);
  EXPECT_EQ(nullptr, batch.pContextViews);
  EXPECT_EQ(nullptr, batch.pGroupOrder);
  EXPECT_FALSE(batch.installed);
  stReaderClearDimensionBatch(&calc);
  EXPECT_EQ(1, dimensionOwnerDestroyCalls);
}
