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

#include <cstring>
#include <string>
#include <vector>

#include "hashjoin.h"
#include "streamjoinfilter.h"
#include "stub.h"
#include "taoserror.h"

namespace {
uint32_t collide(const char*, uint32_t) { return 0; }

void* failMalloc(int64_t) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return nullptr;
}
void* failRealloc(void*, int64_t) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return nullptr;
}
Stub* allocationStub = nullptr;
int   callocCountdown = 0;
void* failCalloc(int64_t count, int64_t bytes) {
  if (--callocCountdown == 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return nullptr;
  }
  allocationStub->reset(taosMemCalloc);
  void* result = taosMemCalloc(count, bytes);
  allocationStub->set(taosMemCalloc, failCalloc);
  return result;
}

struct EncodedKey {
  char*   scratch = nullptr;
  int32_t capacity = 0;
  int32_t length = 0;
  bool    isNull = false;

  ~EncodedKey() { taosMemoryFree(scratch); }
  int32_t encode(const SStreamJoinFilterSpec* spec, const SSDataBlock* block, int32_t row, bool build = true) {
    return streamJoinFilterEncodeKey(spec, block, row, build, &scratch, &capacity, &length, &isNull);
  }
};

class StreamJoinFilterTest : public ::testing::Test {
 protected:
  SStreamJoinFilterSpec     spec = {};
  SStreamJoinFilter*        filter = nullptr;
  std::vector<SSDataBlock*> blocks;

  void SetUp() override {
    spec.probeBlockId = 13;
    spec.receiverBlockId = 12;
    spec.inputBlockId = 11;
    spec.pColumns = taosArrayInit(2, sizeof(SStreamJoinFilterColumn));
    ASSERT_NE(nullptr, spec.pColumns);
  }
  void TearDown() override {
    streamJoinFilterDestroy(&filter);
    EXPECT_EQ(nullptr, filter);
    streamJoinFilterSpecClear(&spec);
    for (auto* block : blocks) blockDataDestroy(block);
  }
  void column(int8_t type, int32_t bytes, uint8_t buildPrecision = 0, uint8_t probePrecision = 0) {
    SStreamJoinFilterColumn desc = {};
    desc.joinKeyIndex = static_cast<int32_t>(taosArrayGetSize(spec.pColumns));
    desc.buildSlot = desc.probeSlot = desc.inputSlot = desc.joinKeyIndex;
    desc.buildType.type = desc.probeType.type = type;
    desc.buildType.bytes = desc.probeType.bytes = bytes;
    desc.buildType.precision = buildPrecision;
    desc.probeType.precision = probePrecision;
    desc.keyPrecision = std::max(buildPrecision, probePrecision);
    ASSERT_NE(nullptr, taosArrayPush(spec.pColumns, &desc));
  }
  void create() {
    ASSERT_EQ(0, streamJoinFilterCreate(&spec, 7, 101, &filter));
    ASSERT_NE(nullptr, filter);
  }
  SSDataBlock* block(int32_t rows, bool build = true) {
    SSDataBlock* result = nullptr;
    EXPECT_EQ(0, createDataBlock(&result));
    if (result == nullptr) return nullptr;
    blocks.push_back(result);
    for (int32_t i = 0; i < taosArrayGetSize(spec.pColumns); ++i) {
      const auto* desc = static_cast<const SStreamJoinFilterColumn*>(taosArrayGet(spec.pColumns, i));
      const auto& type = build ? desc->buildType : desc->probeType;
      auto        info = createColumnInfoData(type.type, type.bytes, i);
      info.info.precision = type.precision;
      EXPECT_EQ(0, blockDataAppendColInfo(result, &info));
    }
    EXPECT_EQ(0, blockDataEnsureCapacity(result, rows));
    result->info.rows = rows;
    result->info.id.blockId = build ? 10 : 11;
    return result;
  }
  SColumnInfoData* data(SSDataBlock* block, int32_t slot = 0) {
    return static_cast<SColumnInfoData*>(taosArrayGet(block->pDataBlock, slot));
  }
  void integer(SSDataBlock* block, int32_t row, int32_t value) {
    ASSERT_EQ(0, colDataSetVal(data(block), row, reinterpret_cast<const char*>(&value), false));
  }
  void timestamp(SSDataBlock* block, int64_t value) {
    ASSERT_EQ(0, colDataSetVal(data(block), 0, reinterpret_cast<const char*>(&value), false));
  }
  void string(SSDataBlock* block, int32_t row, const std::string& value, int32_t slot = 0) {
    std::vector<char> encoded(VARSTR_HEADER_SIZE + value.size());
    varDataSetLen(encoded.data(), value.size());
    std::memcpy(varDataVal(encoded.data()), value.data(), value.size());
    ASSERT_EQ(0, colDataSetVal(data(block, slot), row, encoded.data(), false));
  }
  void add(SSDataBlock* block, int32_t row, EStreamJoinFilterFallback expected = STREAM_JOIN_FILTER_NO_FALLBACK) {
    EStreamJoinFilterFallback fallback = STREAM_JOIN_FILTER_BYTE_LIMIT;
    ASSERT_EQ(0, streamJoinFilterAddRow(filter, block, row, &fallback));
    EXPECT_EQ(expected, fallback);
  }
  void match(SSDataBlock* block, int32_t row, bool expected) {
    bool matched = !expected;
    ASSERT_EQ(0, streamJoinFilterMatchRow(filter, block, row, &matched));
    EXPECT_EQ(expected, matched);
  }
};

TEST_F(StreamJoinFilterTest, DirectCodecOwnsScratchAfterGroupDestruction) {
  column(TSDB_DATA_TYPE_BINARY, 32);
  auto* desc = static_cast<SStreamJoinFilterColumn*>(taosArrayGet(spec.pColumns, 0));
  desc->probeType.bytes = 64;
  desc->probeSlot = 99;
  auto* build = block(1);
  auto* probe = block(2, false);
  string(build, 0, "same");
  string(probe, 0, "same");
  string(probe, 1, "other");
  create();
  add(build, 0);

  EncodedKey buildKey, probeKey;
  ASSERT_EQ(0, buildKey.encode(&spec, build, 0));
  ASSERT_FALSE(buildKey.isNull);
  const std::string saved(buildKey.scratch, buildKey.length);
  EXPECT_NE(filter->pScratch, buildKey.scratch);
  // spec owns an independent layout; neither the group nor its stored keys own these buffers.
  streamJoinFilterDestroy(&filter);
  EXPECT_EQ(saved, std::string(buildKey.scratch, buildKey.length));
  ASSERT_EQ(0, probeKey.encode(&spec, probe, 0, false));
  EXPECT_EQ(saved, std::string(probeKey.scratch, probeKey.length));
  ASSERT_EQ(0, probeKey.encode(&spec, probe, 1, false));
  EXPECT_NE(saved, std::string(probeKey.scratch, probeKey.length));
}

TEST_F(StreamJoinFilterTest, DirectCodecPreservesCompositeBoundaries) {
  column(TSDB_DATA_TYPE_BINARY, 30);
  column(TSDB_DATA_TYPE_BINARY, 30);
  auto* input = block(2);
  string(input, 0, "a");
  string(input, 0, "bc", 1);
  string(input, 1, "ab");
  string(input, 1, "c", 1);
  EncodedKey first, second;
  ASSERT_EQ(0, first.encode(&spec, input, 0));
  ASSERT_EQ(0, second.encode(&spec, input, 1));
  ASSERT_EQ(first.length, second.length);
  EXPECT_NE(0, std::memcmp(first.scratch, second.scratch, first.length));
}

TEST_F(StreamJoinFilterTest, DirectCodecNullRulesOutOversizedTuple) {
  column(TSDB_DATA_TYPE_BINARY, 20000);
  column(TSDB_DATA_TYPE_INT, 4);
  auto* input = block(1);
  string(input, 0, std::string(16378, 'x'));
  colDataSetNULL(data(input, 1), 0);
  data(input, 1)->hasNull = false;
  EncodedKey key;
  ASSERT_EQ(0, key.encode(&spec, input, 0));
  EXPECT_TRUE(key.isNull);
  EXPECT_EQ(0, key.length);
  EXPECT_EQ(0, key.capacity);
  EXPECT_EQ(nullptr, key.scratch);
}

TEST_F(StreamJoinFilterTest, DirectCodecEncodedLengthBoundary) {
  column(TSDB_DATA_TYPE_BINARY, 20000);
  auto* input = block(3);
  string(input, 0, std::string(16377, 'a'));
  string(input, 1, std::string(16378, 'b'));
  string(input, 2, "small");
  EncodedKey key;
  ASSERT_EQ(0, key.encode(&spec, input, 2));
  char*         priorScratch = key.scratch;
  const int32_t priorCapacity = key.capacity;
  ASSERT_EQ(0, key.encode(&spec, input, 1));
  EXPECT_FALSE(key.isNull);
  EXPECT_EQ(16385, key.length);
  EXPECT_EQ(priorScratch, key.scratch);
  EXPECT_EQ(priorCapacity, key.capacity);
  ASSERT_EQ(0, key.encode(&spec, input, 0));
  EXPECT_EQ(16384, key.length);
  EXPECT_EQ(16384, key.capacity);
  priorScratch = key.scratch;
  ASSERT_EQ(0, key.encode(&spec, input, 1));
  EXPECT_EQ(16385, key.length);
  EXPECT_EQ(priorScratch, key.scratch);
  EXPECT_EQ(16384, key.capacity);
}

TEST_F(StreamJoinFilterTest, DirectCodecAllocationFailurePreservesCallerScratch) {
  column(TSDB_DATA_TYPE_BINARY, 30);
  auto* input = block(2);
  string(input, 0, "a");
  string(input, 1, "longer");
  EncodedKey key;
  {
    Stub stub;
    stub.set(taosMemRealloc, failRealloc);
    EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, key.encode(&spec, input, 0));
    EXPECT_EQ(nullptr, key.scratch);
    EXPECT_EQ(0, key.capacity);
  }
  ASSERT_EQ(0, key.encode(&spec, input, 0));
  char*             priorScratch = key.scratch;
  const int32_t     priorCapacity = key.capacity;
  const std::string saved(key.scratch, key.length);
  {
    Stub stub;
    stub.set(taosMemRealloc, failRealloc);
    EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, key.encode(&spec, input, 1));
    EXPECT_EQ(priorScratch, key.scratch);
    EXPECT_EQ(priorCapacity, key.capacity);
    EXPECT_EQ(saved, std::string(key.scratch, saved.size()));
    EXPECT_EQ(0, key.length);
  }
  ASSERT_EQ(0, key.encode(&spec, input, 1));
  EXPECT_FALSE(key.isNull);
  EXPECT_GT(key.length, saved.size());
  priorScratch = key.scratch;
  {
    Stub stub;
    stub.set(taosMemRealloc, failRealloc);
    // A caller's already-sized buffer must not allocate again.
    EXPECT_EQ(0, key.encode(&spec, input, 0));
    EXPECT_EQ(priorScratch, key.scratch);
  }
}

TEST_F(StreamJoinFilterTest, DirectCodecRetainsRowAndColumnValidation) {
  column(TSDB_DATA_TYPE_INT, 4);
  auto* input = block(1);
  integer(input, 0, 42);
  EncodedKey key;
  EXPECT_EQ(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR, key.encode(&spec, input, -1));
  EXPECT_EQ(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR, key.encode(&spec, input, 1));
  data(input)->info.bytes = 8;
  EXPECT_EQ(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR, key.encode(&spec, input, 0));
  EXPECT_EQ(nullptr, key.scratch);
}

TEST_F(StreamJoinFilterTest, DistinctCountBoundary) {
  column(TSDB_DATA_TYPE_INT, 4);
  create();
  auto* input = block(65);
  for (int32_t row = 0; row < 65; ++row) integer(input, row, row);
  for (int32_t row = 0; row < 64; ++row) add(input, row);
  EXPECT_EQ(64, filter->keyCount);
  add(input, 64, STREAM_JOIN_FILTER_KEY_LIMIT);
  EXPECT_EQ(64, filter->keyCount);
  match(input, 63, true);
  match(input, 64, false);
}

TEST_F(StreamJoinFilterTest, EncodedBytesBoundary) {
  column(TSDB_DATA_TYPE_BINARY, 9000);
  auto* input = block(3);
  // Each encoding adds one type byte, four length bytes and two var-length bytes.
  string(input, 0, std::string(8185, 'a'));
  string(input, 1, std::string(8185, 'b'));
  string(input, 2, std::string(8186, 'c'));
  create();
  add(input, 0);
  add(input, 1);
  EXPECT_EQ(16384, filter->keyBytes);
  streamJoinFilterDestroy(&filter);
  create();
  add(input, 0);
  add(input, 2, STREAM_JOIN_FILTER_BYTE_LIMIT);
  EXPECT_EQ(1, filter->keyCount);
  EXPECT_EQ(8192, filter->keyBytes);
  EXPECT_LE(filter->keyBytes, 16384);
  match(input, 2, false);
}

TEST_F(StreamJoinFilterTest, DuplicatesDoNotConsumeLimits) {
  column(TSDB_DATA_TYPE_BINARY, 300);
  create();
  auto* input = block(64);
  for (int32_t row = 0; row < 64; ++row) {
    string(input, row, std::string(249, static_cast<char>(row + 1)));
    add(input, row);
  }
  EXPECT_EQ(64, filter->keyCount);
  EXPECT_EQ(16384, filter->keyBytes);
  for (int32_t row = 0; row < 64; ++row) add(input, row);
  EXPECT_EQ(64, filter->keyCount);
  EXPECT_EQ(16384, filter->keyBytes);
}

TEST_F(StreamJoinFilterTest, CompositeKeysDoNotCrossMatch) {
  column(TSDB_DATA_TYPE_BINARY, 30);
  column(TSDB_DATA_TYPE_BINARY, 30);
  auto* input = block(4);
  string(input, 0, "a");
  string(input, 0, "bc", 1);
  string(input, 1, "ab");
  string(input, 1, "c", 1);
  string(input, 2, "a");
  string(input, 2, "c", 1);
  string(input, 3, "ab");
  string(input, 3, "bc", 1);
  for (bool collision : {false, true}) {
    create();
    if (collision && filter->pKeys != nullptr) {
      tSimpleHashCleanup(filter->pKeys);
      filter->pKeys = tSimpleHashInit(128, collide);
      ASSERT_NE(nullptr, filter->pKeys);
    }
    add(input, 0);
    add(input, 1);
    match(input, 0, true);
    match(input, 1, true);
    match(input, 2, false);
    match(input, 3, false);
    streamJoinFilterDestroy(&filter);
  }
}

TEST_F(StreamJoinFilterTest, RawNullMarkersAndEmptyValues) {
  column(TSDB_DATA_TYPE_INT, 4);
  create();
  auto* fixed = block(2);
  integer(fixed, 0, 42);
  integer(fixed, 1, 42);
  colDataSetNULL(data(fixed), 1);
  data(fixed)->hasNull = false;
  add(fixed, 0);
  add(fixed, 1);
  EXPECT_EQ(1, filter->keyCount);
  match(fixed, 0, true);
  match(fixed, 1, false);
  EXPECT_FALSE(data(fixed)->hasNull);
  streamJoinFilterDestroy(&filter);
  taosArrayClear(spec.pColumns);
  column(TSDB_DATA_TYPE_BINARY, 30);
  create();
  auto* variable = block(2);
  string(variable, 0, "");
  ASSERT_EQ(0, colDataSetVal(data(variable), 1, nullptr, true));
  data(variable)->hasNull = false;
  add(variable, 0);
  add(variable, 1);
  EXPECT_EQ(1, filter->keyCount);
  match(variable, 0, true);
  match(variable, 1, false);
  EXPECT_FALSE(data(variable)->hasNull);
}

TEST_F(StreamJoinFilterTest, TimestampPrecisionMatchesJoin) {
  column(TSDB_DATA_TYPE_TIMESTAMP, 8, TSDB_TIME_PRECISION_MILLI, TSDB_TIME_PRECISION_MICRO);
  create();
  auto* build = block(1);
  auto* probe = block(1, false);
  timestamp(build, 1234);
  // Actual decoded column precision, rather than the descriptor, governs source conversion.
  data(probe)->info.precision = TSDB_TIME_PRECISION_NANO;
  timestamp(probe, 1234000000);
  SHJoinColInfo buildKey = {}, probeKey = {};
  char          buildBuffer[16] = {}, probeBuffer[16] = {};
  buildKey.type = probeKey.type = TSDB_DATA_TYPE_TIMESTAMP;
  buildKey.bytes = probeKey.bytes = 8;
  buildKey.precision = probeKey.precision = TSDB_TIME_PRECISION_MICRO;
  buildKey.sourcePrecision = TSDB_TIME_PRECISION_MILLI;
  probeKey.sourcePrecision = TSDB_TIME_PRECISION_NANO;
  buildKey.colData = data(build);
  buildKey.data = data(build)->pData;
  probeKey.colData = data(probe);
  probeKey.data = data(probe)->pData;
  SHJoinTableCtx left = {}, right = {};
  left.keyNum = right.keyNum = 1;
  left.keyCols = &buildKey;
  right.keyCols = &probeKey;
  left.keyBuf = buildBuffer;
  right.keyBuf = probeBuffer;
  size_t leftBytes = 0, rightBytes = 0;
  bool   nullKey = false;
  ASSERT_EQ(0, hJoinCopyKeyColsDataToBuf(&left, 0, &leftBytes, &nullKey));
  ASSERT_FALSE(nullKey);
  ASSERT_EQ(0, hJoinCopyKeyColsDataToBuf(&right, 0, &rightBytes, &nullKey));
  ASSERT_FALSE(nullKey);
  ASSERT_EQ(leftBytes, rightBytes);
  ASSERT_EQ(0, std::memcmp(left.keyData, right.keyData, leftBytes));
  EncodedKey buildEncoded, probeEncoded;
  ASSERT_EQ(0, buildEncoded.encode(&spec, build, 0));
  ASSERT_EQ(0, probeEncoded.encode(&spec, probe, 0, false));
  ASSERT_EQ(buildEncoded.length, probeEncoded.length);
  EXPECT_EQ(0, std::memcmp(buildEncoded.scratch, probeEncoded.scratch, buildEncoded.length));
  add(build, 0);
  match(probe, 0, true);
  timestamp(probe, 1235000000);
  ASSERT_EQ(0, hJoinCopyKeyColsDataToBuf(&right, 0, &rightBytes, &nullKey));
  ASSERT_FALSE(nullKey);
  ASSERT_NE(0, std::memcmp(left.keyData, right.keyData, leftBytes));
  match(probe, 0, false);
}

TEST_F(StreamJoinFilterTest, OversizedKeyDoesNotGrowScratch) {
  column(TSDB_DATA_TYPE_BINARY, 20000);
  create();
  auto* input = block(2);
  string(input, 0, "small");
  string(input, 1, std::string(16378, 'x'));
  add(input, 0);
  const int32_t priorCapacity = filter->scratchCapacity;
  add(input, 1, STREAM_JOIN_FILTER_BYTE_LIMIT);
  match(input, 1, false);
  EXPECT_EQ(priorCapacity, filter->scratchCapacity);
  EXPECT_LE(filter->scratchCapacity, 16384);
  EXPECT_EQ(1, filter->keyCount);
}

TEST_F(StreamJoinFilterTest, VariableDeclaredWidthsPreserveJoinEquality) {
  column(TSDB_DATA_TYPE_BINARY, 32);
  auto* desc = static_cast<SStreamJoinFilterColumn*>(taosArrayGet(spec.pColumns, 0));
  desc->probeType.bytes = 64;
  desc->probeSlot = 99;  // The resolved physical input slot governs matching.
  auto* build = block(1);
  auto* probe = block(2, false);
  string(build, 0, "same");
  string(probe, 0, "same");
  string(probe, 1, "other");
  ASSERT_EQ(0, streamJoinFilterCreate(&spec, 7, 101, &filter));
  ASSERT_NE(nullptr, filter);
  // The collection owns descriptors and stored keys after the source spec and build values change.
  streamJoinFilterSpecClear(&spec);
  add(build, 0);
  string(build, 0, "changed");
  match(probe, 0, true);
  match(probe, 1, false);
}

TEST_F(StreamJoinFilterTest, VirtualValuesWiderThanDeclaredColumnsPreserveEquality) {
  column(TSDB_DATA_TYPE_VARCHAR, VARSTR_HEADER_SIZE + 4);
  column(TSDB_DATA_TYPE_NCHAR, VARSTR_HEADER_SIZE + 4);
  auto*             build = block(1);
  auto*             probe = block(2, false);
  const uint32_t    wide[] = {'a', 'b'};
  const std::string nchar(reinterpret_cast<const char*>(wide), sizeof(wide));
  string(build, 0, "abcdef");
  string(build, 0, nchar, 1);
  string(probe, 0, "abcdef");
  string(probe, 0, nchar, 1);
  string(probe, 1, "abcdeg");
  string(probe, 1, nchar, 1);
  create();
  add(build, 0);
  match(probe, 0, true);
  match(probe, 1, false);

  // Declared width is not a bound, but a truncated physical payload still is invalid.
  auto* col = data(probe, 1);
  --col->varmeta.length;
  bool matched = false;
  EXPECT_EQ(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR, streamJoinFilterMatchRow(filter, probe, 1, &matched));
}

TEST_F(StreamJoinFilterTest, WideVirtualValueUsesActualSizeForByteLimit) {
  column(TSDB_DATA_TYPE_VARCHAR, VARSTR_HEADER_SIZE + 4);
  auto* input = block(2);
  string(input, 0, "small");
  string(input, 1, std::string(STREAM_JOIN_FILTER_MAX_KEY_BYTES, 'x'));
  create();
  add(input, 0);
  const int32_t priorCapacity = filter->scratchCapacity;
  add(input, 1, STREAM_JOIN_FILTER_BYTE_LIMIT);
  EXPECT_EQ(priorCapacity, filter->scratchCapacity);
  EXPECT_EQ(1, filter->keyCount);
}

TEST_F(StreamJoinFilterTest, AllocationFailureIsFatal) {
  column(TSDB_DATA_TYPE_INT, 4);
  auto* input = block(1);
  integer(input, 0, 42);
  for (int stage : {1, 2}) {
    SCOPED_TRACE(stage);
    Stub stub;
    allocationStub = &stub;
    callocCountdown = stage;
    stub.set(taosMemCalloc, failCalloc);
    EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, streamJoinFilterCreate(&spec, 7, 101, &filter));
    streamJoinFilterDestroy(&filter);
    EXPECT_EQ(nullptr, filter);
  }
  create();
  EStreamJoinFilterFallback fallback = STREAM_JOIN_FILTER_NO_FALLBACK;
  {
    Stub stub;
    stub.set(taosMemRealloc, failRealloc);
    EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, streamJoinFilterAddRow(filter, input, 0, &fallback));
    EXPECT_EQ(STREAM_JOIN_FILTER_NO_FALLBACK, fallback);
    EXPECT_EQ(0, filter->keyCount);
  }
  add(input, 0);
  integer(input, 0, 43);
  {
    Stub stub;
    stub.set(taosMemMalloc, failMalloc);
    EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, streamJoinFilterAddRow(filter, input, 0, &fallback));
    EXPECT_EQ(STREAM_JOIN_FILTER_NO_FALLBACK, fallback);
    EXPECT_EQ(1, filter->keyCount);
  }
  streamJoinFilterDestroy(&filter);
  EXPECT_EQ(nullptr, filter);
}
}  // namespace
