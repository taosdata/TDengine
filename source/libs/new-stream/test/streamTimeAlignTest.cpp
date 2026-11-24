/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include <gtest/gtest.h>

#include "streamTimeAlign.h"
#include "tdatablock.h"

TEST(StreamTimeAlignTest, SourceColumnIdsAreMappedExplicitly) {
  SStreamTimeAlignSpec spec = {};
  spec.pColumns = taosArrayInit(2, sizeof(SStreamTimeAlignColumn));
  SArray*                bounds = taosArrayInit(1, sizeof(SSTimeAlignSourceBound));
  SSTimeAlignSourceBound bound = {};
  bound.uid = 77;
  bound.maxVer = 10;
  bound.pColRefs = taosArrayInit(2, sizeof(SSTimeAlignColumnRef));
  for (col_id_t virtualId : {15, 21}) {
    SStreamTimeAlignColumn definition = {};
    definition.srcColId = virtualId;
    definition.type.type = TSDB_DATA_TYPE_INT;
    definition.type.bytes = sizeof(int32_t);
    ASSERT_NE(nullptr, taosArrayPush(spec.pColumns, &definition));
    SSTimeAlignColumnRef ref = {virtualId, 7};
    ASSERT_NE(nullptr, taosArrayPush(bound.pColRefs, &ref));
  }
  ASSERT_NE(nullptr, taosArrayPush(bounds, &bound));
  SSTimeAlignSeed* seed = nullptr;
  ASSERT_EQ(0, stTimeAlignSeedCreate(&spec, 81, 3, 9000, bounds, &seed));
  taosArrayDestroy(bound.pColRefs);
  taosArrayDestroy(bounds);
  taosArrayDestroy(spec.pColumns);

  SSDataBlock* block = nullptr;
  ASSERT_EQ(0, createDataBlock(&block));
  SColumnInfoData ts = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), PRIMARYKEY_TIMESTAMP_COL_ID);
  SColumnInfoData value = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 7);
  ASSERT_EQ(0, blockDataAppendColInfo(block, &ts));
  ASSERT_EQ(0, blockDataAppendColInfo(block, &value));
  ASSERT_EQ(0, blockDataEnsureCapacity(block, 3));
  auto* tsCol = static_cast<SColumnInfoData*>(taosArrayGet(block->pDataBlock, 0));
  auto* dataCol = static_cast<SColumnInfoData*>(taosArrayGet(block->pDataBlock, 1));
  for (int row = 0; row < 3; ++row) {
    int64_t time = 9000 - row * 500;
    int32_t input = 90 - row * 5;
    ASSERT_EQ(0, colDataSetVal(tsCol, row, reinterpret_cast<const char*>(&time), false));
    ASSERT_EQ(0, colDataSetVal(dataCol, row, reinterpret_cast<const char*>(&input), row == 1));
  }
  block->info.rows = 3;
  bool ready = false;
  ASSERT_EQ(0, stTimeAlignSeedConsume(seed, 77, block, false, &ready));
  EXPECT_TRUE(ready);
  blockDataDestroy(block);
  for (col_id_t virtualId : {15, 21}) {
    const SFillPrevValue* found = stTimeAlignSeedValue(seed, virtualId);
    ASSERT_NE(nullptr, found);
    EXPECT_EQ(8000, found->sourceTs);
    int32_t actual = 0;
    memcpy(&actual, found->fixed, sizeof(actual));
    EXPECT_EQ(80, actual);
  }
  stTimeAlignSeedDestroy(&seed);
  EXPECT_EQ(nullptr, seed);
}

class StreamAlignStateTest : public ::testing::Test {
 protected:
  SStreamTimeAlignSpec spec = {};
  SSTimeAlignState*    state = nullptr;
  SArray*              slots = nullptr;
  SSDataBlock*         block = nullptr;
  void                 SetUp() override {
    spec.pColumns = taosArrayInit(2, sizeof(SStreamTimeAlignColumn));
    ASSERT_NE(nullptr, spec.pColumns);
    slots = taosArrayInit(2, sizeof(SSTimeAlignSlot));
    ASSERT_NE(nullptr, slots);
    for (int32_t i = 0; i < 2; ++i) {
      SStreamTimeAlignColumn def = {};
      def.srcColId = 9;
      def.type.type = TSDB_DATA_TYPE_INT;
      def.type.bytes = sizeof(int32_t);
      def.hasSpan = i != 0;
      def.amount = 2;
      def.unit = 's';
      ASSERT_NE(nullptr, taosArrayPush(spec.pColumns, &def));
      SSTimeAlignSlot slot = {9, 1, 2 + i};
      ASSERT_NE(nullptr, taosArrayPush(slots, &slot));
    }
    ASSERT_EQ(0, stTimeAlignCreate(&spec, &state));
    ASSERT_EQ(0, createDataBlock(&block));
    for (int i = 0; i < 4; ++i) {
      auto c = createColumnInfoData(i ? TSDB_DATA_TYPE_INT : TSDB_DATA_TYPE_TIMESTAMP, i ? 4 : 8, i + 1);
      ASSERT_EQ(0, blockDataAppendColInfo(block, &c));
    }
    ASSERT_EQ(0, blockDataEnsureCapacity(block, 4));
  }
  void TearDown() override {
    stTimeAlignDestroy(&state);
    blockDataDestroy(block);
    taosArrayDestroy(slots);
    taosArrayDestroy(spec.pColumns);
  }
  SColumnInfoData* column(int slot) { return static_cast<SColumnInfoData*>(taosArrayGet(block->pDataBlock, slot)); }
  void             seedEmpty(uint64_t uid, uint64_t generation, SSTimeAlignState* target = nullptr) {
    if (!target) target = state;
    ASSERT_EQ(0, stTimeAlignRegisterTable(target, uid, generation));
    SSTimeAlignSeed* seed = nullptr;
    SArray*          bounds = taosArrayInit(1, sizeof(SSTimeAlignSourceBound));
    ASSERT_EQ(0, stTimeAlignSeedCreate(&spec, uid, generation, 0, bounds, &seed));
    bool applied = false;
    ASSERT_EQ(0, stTimeAlignInstallSeed(target, seed, &applied));
    EXPECT_TRUE(applied);
    stTimeAlignSeedDestroy(&seed);
    taosArrayDestroy(bounds);
  }
  void addStringRule(int32_t bytes = 8192 + VARSTR_HEADER_SIZE) {
    stTimeAlignDestroy(&state);
    SStreamTimeAlignColumn def = {};
    def.srcColId = 10;
    def.hasSpan = true;
    def.amount = 1;
    def.unit = 's';
    def.type.type = TSDB_DATA_TYPE_VARCHAR;
    def.type.bytes = bytes;
    ASSERT_NE(nullptr, taosArrayPush(spec.pColumns, &def));
    SSTimeAlignSlot slot = {10, 4, 5};
    ASSERT_NE(nullptr, taosArrayPush(slots, &slot));
    ASSERT_EQ(0, stTimeAlignCreate(&spec, &state));
    for (col_id_t id : {10, 11}) {
      auto col = createColumnInfoData(def.type.type, def.type.bytes, id);
      ASSERT_EQ(0, blockDataAppendColInfo(block, &col));
    }
    ASSERT_EQ(0, blockDataEnsureCapacity(block, 5));
    seedEmpty(71, 1);
  }
  void row(int index, int64_t ts, const int32_t* value) {
    ASSERT_EQ(0, colDataSetVal(column(0), index, reinterpret_cast<const char*>(&ts), false));
    ASSERT_EQ(0, colDataSetVal(column(1), index, reinterpret_cast<const char*>(value), value == nullptr));
    block->info.rows = index + 1;
  }
  int32_t result(int slot, int index) {
    int32_t value;
    memcpy(&value, colDataGetData(column(slot), index), sizeof(value));
    return value;
  }
};

TEST_F(StreamAlignStateTest, PackedStringSeedAndRealtimeRows) {
  addStringRule();
  alignas(VarDataLenT) char input[] = {0, 0, 'a', 'b', 'c'};
  VarDataLenT               one = 1, three = 3;
  memcpy(input, &one, sizeof(one));
  row(0, 1000, nullptr);
  ASSERT_EQ(0, colDataSetVal(column(4), 0, input, false));
  memcpy(input, &three, sizeof(three));
  row(1, 1010, nullptr);
  ASSERT_EQ(0, colDataSetVal(column(4), 1, input, false));
  ASSERT_EQ(3, colDataGetData(column(4), 1) - colDataGetData(column(4), 0));
  // Read the second packed value as the first eligible seed row.
  SArray*              refs = taosArrayInit(1, sizeof(SSTimeAlignColumnRef));
  SSTimeAlignColumnRef ref = {10, 10};
  ASSERT_NE(nullptr, taosArrayPush(refs, &ref));
  SSTimeAlignSourceBound bound = {};
  bound.uid = 93;
  bound.pColRefs = refs;
  SArray* bounds = taosArrayInit(1, sizeof(bound));
  ASSERT_NE(nullptr, taosArrayPush(bounds, &bound));
  SSTimeAlignSeed* seed = nullptr;
  ASSERT_EQ(0, stTimeAlignSeedCreate(&spec, 71, 2, 1020, bounds, &seed));
  // The first row is newer than the cut and must be skipped.
  int64_t afterCut = 1020;
  ASSERT_EQ(0, colDataSetVal(column(0), 0, reinterpret_cast<const char*>(&afterCut), false));
  bool ready = false;
  ASSERT_EQ(0, stTimeAlignSeedConsume(seed, 93, block, true, &ready));
  ASSERT_TRUE(ready);
  const SFillPrevValue* saved = stTimeAlignSeedValue(seed, 10);
  ASSERT_NE(nullptr, saved);
  ASSERT_TRUE(saved->nonnull);
  EXPECT_EQ(0, memcmp(input, saved->data, sizeof(input)));
  stTimeAlignSeedDestroy(&seed);
  taosArrayDestroy(bounds);
  taosArrayDestroy(refs);
  row(0, 1000, nullptr);
  row(1, 1010, nullptr);
  row(2, 1020, nullptr);
  colDataSetNULL(column(4), 2);
  ASSERT_EQ(0, stTimeAlignApply(state, 71, slots, 0, block, 0, 3));
  ASSERT_FALSE(colDataIsNull_s(column(5), 2));
  EXPECT_EQ(0, memcmp(input, colDataGetData(column(5), 2), sizeof(input)));
}

TEST_F(StreamAlignStateTest, InPlaceWidenKeepsOutstandingSeedWidth) {
  const int32_t oldBytes = 4 + VARSTR_HEADER_SIZE;
  const int32_t newBytes = 2048 + VARSTR_HEADER_SIZE;
  addStringRule(oldBytes);
  SSTimeAlignColumnRef   ref = {10, 10, oldBytes};
  SSTimeAlignSourceBound bound = {};
  bound.uid = 93;
  bound.pColRefs = taosArrayInit(1, sizeof(ref));
  SArray* bounds = taosArrayInit(1, sizeof(bound));
  auto*   installed = static_cast<SSTimeAlignColumnRef*>(taosArrayPush(bound.pColRefs, &ref));
  ASSERT_NE(nullptr, installed);
  ASSERT_NE(nullptr, taosArrayPush(bounds, &bound));
  SSTimeAlignSeed* oldSeed = nullptr;
  SSTimeAlignSeed* newSeed = nullptr;
  ASSERT_EQ(0, stTimeAlignSeedCreate(&spec, 71, 1, 1020, bounds, &oldSeed));
  installed->sourceBytes = newBytes;
  ASSERT_EQ(0, stTimeAlignSeedCreate(&spec, 71, 2, 1020, bounds, &newSeed));
  taosArrayDestroy(bound.pColRefs);
  taosArrayDestroy(bounds);

  row(0, 1000, nullptr);
  char value[1200 + VARSTR_HEADER_SIZE] = {};
  bool ready = false;
  for (auto* seed : {newSeed, oldSeed}) {
    const VarDataLenT length = seed == newSeed ? 1200 : 4;
    memcpy(value, &length, sizeof(length));
    memset(value + VARSTR_HEADER_SIZE, 'a', length);
    column(4)->info.bytes = seed == newSeed ? newBytes : oldBytes;
    ASSERT_EQ(0, colDataSetVal(column(4), 0, value, false));
    ASSERT_EQ(0, stTimeAlignSeedConsume(seed, 93, block, true, &ready));
    ASSERT_TRUE(ready);
    const SFillPrevValue* result = stTimeAlignSeedValue(seed, 10);
    ASSERT_NE(nullptr, result);
    ASSERT_TRUE(result->nonnull);
    EXPECT_EQ(column(4)->info.bytes, result->type.bytes);
    EXPECT_EQ(0, memcmp(value, result->data, length + VARSTR_HEADER_SIZE));
  }

  stTimeAlignForgetTable(state, 71);
  ASSERT_EQ(0, stTimeAlignRegisterTable(state, 71, 2));
  bool applied = true;
  ASSERT_EQ(0, stTimeAlignInstallSeed(state, oldSeed, &applied));
  EXPECT_FALSE(applied);
  ASSERT_EQ(0, stTimeAlignInstallSeed(state, newSeed, &applied));
  EXPECT_TRUE(applied);
  stTimeAlignSeedDestroy(&oldSeed);
  stTimeAlignSeedDestroy(&newSeed);
}

TEST_F(StreamAlignStateTest, RemovedTableCannotBeResurrectedBySeed) {
  SArray*          bounds = taosArrayInit(1, sizeof(SSTimeAlignSourceBound));
  SSTimeAlignSeed* oldSeed = nullptr;
  ASSERT_EQ(0, stTimeAlignSeedCreate(&spec, 71, 1, 1000, bounds, &oldSeed));
  taosArrayDestroy(bounds);
  ASSERT_EQ(0, stTimeAlignRegisterTable(state, 71, 1));
  bool applied = false;
  ASSERT_EQ(0, stTimeAlignInstallSeed(state, oldSeed, &applied));
  ASSERT_TRUE(applied);
  stTimeAlignForgetTable(state, 71);
  ASSERT_EQ(0, stTimeAlignInstallSeed(state, oldSeed, &applied));
  EXPECT_FALSE(applied);

  seedEmpty(71, 2);
  int32_t value = 90;
  row(0, 1000, &value);
  ASSERT_EQ(0, stTimeAlignApply(state, 71, slots, 0, block, 0, 1));
  applied = true;
  ASSERT_EQ(0, stTimeAlignInstallSeed(state, oldSeed, &applied));
  EXPECT_FALSE(applied);
  stTimeAlignSeedDestroy(&oldSeed);
  row(0, 1001, nullptr);
  ASSERT_EQ(0, stTimeAlignApply(state, 71, slots, 0, block, 0, 1));
  ASSERT_FALSE(colDataIsNull_s(column(2), 0));
  EXPECT_EQ(value, result(2, 0));
}

TEST_F(StreamAlignStateTest, AdmittedSubmitUsesItsOwnPhysicalPresence) {
  SSTimeAlignColumnRef   ref = {9, 17};
  SSTimeAlignSourceBound source = {};
  source.readerTaskId = 21;
  source.uid = 32;
  source.pColRefs = taosArrayInit(1, sizeof(ref));
  ASSERT_NE(nullptr, taosArrayPush(source.pColRefs, &ref));
  SArray* bounds = taosArrayInit(1, sizeof(source));
  ASSERT_NE(nullptr, taosArrayPush(bounds, &source));
  SSTimeAlignSeed* seed = nullptr;
  ASSERT_EQ(0, stTimeAlignSeedCreate(&spec, 71, 1, 1001, bounds, &seed));
  const int32_t value = 10;
  row(0, 1000, &value);
  column(1)->info.colId = 17;
  const int64_t physical = 1000000123;
  auto native = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(physical), STREAM_WAL_PHYSICAL_TS_COL_ID);
  ASSERT_EQ(0, blockDataAppendColInfo(block, &native));
  auto* key = static_cast<SColumnInfoData*>(taosArrayGetLast(block->pDataBlock));
  ASSERT_EQ(0, colInfoDataEnsureCapacity(key, block->info.capacity, true));
  ASSERT_EQ(0, colDataSetVal(key, 0, reinterpret_cast<const char*>(&physical), false));
  bool ready = false;
  ASSERT_EQ(0, stTimeAlignSeedConsume(seed, 32, block, true, &ready));
  ASSERT_EQ(0, stTimeAlignRegisterTable(state, 71, 1));
  ASSERT_EQ(0, stTimeAlignInstallSeed(state, seed, &ready));
  ASSERT_TRUE(ready);
  SArray* saved = stTimeAlignTakeCandidates(state, 71);
  stTimeAlignSeedDestroy(&seed);
  taosArrayDestroy(source.pColRefs);
  taosArrayDestroy(bounds);

  STimeWindow cell = {1000, 1000};
  SArray*     probes = nullptr;
  for (int64_t version : {42, 43}) ASSERT_EQ(0, stTimeAlignCollectWalProbes(saved, 21, 32, cell, version, &probes));
  ASSERT_EQ(2U, taosArrayGetSize(probes));
  auto* first = static_cast<STimeAlignWalProbe*>(taosArrayGet(probes, 0));
  auto* later = static_cast<STimeAlignWalProbe*>(taosArrayGet(probes, 1));
  ASSERT_TRUE(first->physical);
  ASSERT_EQ(physical, first->ts);
  first->written = true;
  // The first submit was ignored. An admitted sparse submit must keep its candidate.
  stTimeAlignDiscardWalWrites(saved, 21, 32, cell, 43, probes);
  SArray* remaining = nullptr;
  ASSERT_EQ(0, stTimeAlignCollectWalProbes(saved, 21, 32, cell, 43, &remaining));
  ASSERT_EQ(1U, taosArrayGetSize(remaining));
  taosArrayDestroy(remaining);
  remaining = nullptr;
  // An exact write in the admitted version clears it even if the first submit was sparse.
  first->written = false;
  later->written = true;
  stTimeAlignDiscardWalWrites(saved, 21, 32, cell, 43, probes);
  ASSERT_EQ(0, stTimeAlignCollectWalProbes(saved, 21, 32, cell, 43, &remaining));
  EXPECT_EQ(nullptr, remaining);
  taosArrayDestroy(remaining);
  taosArrayDestroy(probes);
  stTimeAlignDestroyCandidates(saved);
}

TEST_F(StreamAlignStateTest, TwoChildrenDoNotSharePrev) {
  seedEmpty(71, 1);
  seedEmpty(72, 1);
  int32_t temp = 100;
  row(0, 0, &temp);
  row(1, 1000, nullptr);
  ASSERT_EQ(0, stTimeAlignApply(state, 71, slots, 0, block, 0, 2));
  EXPECT_EQ(100, result(2, 1));
  ASSERT_EQ(0, stTimeAlignApply(state, 72, slots, 0, block, 1, 1));
  EXPECT_TRUE(colDataIsNull_s(column(2), 1));
  EXPECT_TRUE(colDataIsNull_s(column(1), 1));
}

TEST_F(StreamAlignStateTest, SameTimestampAcrossBlocks) {
  seedEmpty(71, 1);
  int32_t temp = 80;
  row(0, 8000, &temp);
  ASSERT_EQ(0, stTimeAlignApply(state, 71, slots, 0, block, 0, 1));
  blockDataCleanup(block);
  row(0, 8000, nullptr);
  row(1, 10000, nullptr);
  row(2, 10001, nullptr);
  ASSERT_EQ(0, stTimeAlignApply(state, 71, slots, 0, block, 0, 3));
  EXPECT_EQ(80, result(3, 0));
  EXPECT_EQ(80, result(3, 1));
  EXPECT_TRUE(colDataIsNull_s(column(3), 2));
  EXPECT_EQ(80, result(2, 2));
}

TEST_F(StreamAlignStateTest, LongNanosecondSpanUsesTheSameSeedAndContinuousCutoff) {
  stTimeAlignDestroy(&state);
  spec.precision = TSDB_TIME_PRECISION_NANO;
  for (size_t i = 0; i < taosArrayGetSize(spec.pColumns); ++i) {
    auto* rule = static_cast<SStreamTimeAlignColumn*>(taosArrayGet(spec.pColumns, i));
    rule->hasSpan = true;
    rule->amount = i ? 110000 : 1;
    rule->unit = 'd';
  }
  SSTimeAlignConfig* config = nullptr;
  ASSERT_EQ(0, stTimeAlignConfigCreate(&spec, &config));
  ASSERT_EQ(0, stTimeAlignCreateFromConfig(config, &state));
  // UTC 1900-01-01, 2200-01-01, and 110000 days after the sample.
  const int64_t sample = -2208988800000000000LL, heartbeat = 7258118400000000000LL, cutoff = 7295011200000000000LL;
  int64_t       floor = 0, stable = 0;
  ASSERT_TRUE(stTimeAlignSeedFloor(config, 9, cutoff, &floor));
  EXPECT_EQ(sample, floor);
  ASSERT_TRUE(stTimeAlignSeedFloor(config, 9, cutoff + 1, &floor));
  EXPECT_EQ(sample + 1, floor);
  ASSERT_TRUE(stTimeAlignRuleExpiryAfter(config, 1, sample, &stable));
  EXPECT_EQ(cutoff + 1, stable);
  EXPECT_FALSE(stTimeAlignRuleExpiryAfter(config, 1, 0, &stable));

  int32_t input = 42;
  row(0, sample, &input);
  seedEmpty(71, 1);
  ASSERT_EQ(0, stTimeAlignApply(state, 71, slots, 0, block, 0, 1));

  SArray* bounds = taosArrayInit_s(sizeof(SSTimeAlignSourceBound), 1);
  auto*   bound = static_cast<SSTimeAlignSourceBound*>(taosArrayGet(bounds, 0));
  bound->uid = 33;
  bound->pColRefs = taosArrayInit(1, sizeof(SSTimeAlignColumnRef));
  SSTimeAlignColumnRef ref = {9, 2};
  ASSERT_NE(nullptr, taosArrayPush(bound->pColRefs, &ref));
  SSTimeAlignSeed* seed = nullptr;
  ASSERT_EQ(0, stTimeAlignSeedCreate(&spec, 72, 1, heartbeat, bounds, &seed));
  taosArrayDestroy(bound->pColRefs);
  taosArrayDestroy(bounds);
  bool ready = false;
  ASSERT_EQ(0, stTimeAlignSeedConsume(seed, 33, block, true, &ready));
  ASSERT_TRUE(ready);
  ASSERT_EQ(0, stTimeAlignRegisterTable(state, 72, 1));
  ASSERT_EQ(0, stTimeAlignInstallSeed(state, seed, &ready));
  ASSERT_TRUE(ready);
  stTimeAlignSeedDestroy(&seed);

  for (uint64_t uid : {71, 72}) {
    blockDataCleanup(block);
    row(0, heartbeat, nullptr);
    row(1, cutoff, nullptr);
    row(2, cutoff + 1, nullptr);
    ASSERT_EQ(0, stTimeAlignApply(state, uid, slots, 0, block, 0, 3));
    for (int i : {0, 1}) {
      EXPECT_TRUE(colDataIsNull_s(column(2), i));
      if (!colDataIsNull_s(column(3), i)) EXPECT_EQ(input, result(3, i));
      EXPECT_FALSE(colDataIsNull_s(column(3), i));
    }
    EXPECT_TRUE(colDataIsNull_s(column(3), 2));
  }
  stTimeAlignDestroy(&state);
  stTimeAlignConfigDestroy(&config);
}

TEST_F(StreamAlignStateTest, ExpiryAndSeedFloorAtUnsignedDistanceLimit) {
  spec.precision = TSDB_TIME_PRECISION_NANO;
  for (int i : {0, 1}) {
    auto* rule = static_cast<SStreamTimeAlignColumn*>(taosArrayGet(spec.pColumns, i));
    rule->srcColId = 9 + i;
    rule->hasSpan = true;
    rule->amount = UINT64_MAX / 1000 + i;
    rule->unit = 'u';
  }
  SSTimeAlignConfig* config = nullptr;
  ASSERT_EQ(0, stTimeAlignConfigCreate(&spec, &config));
  int64_t ts = 0;
  ASSERT_TRUE(stTimeAlignSeedFloor(config, 9, INT64_MAX, &ts));
  EXPECT_EQ(INT64_MIN + 615, ts);
  ASSERT_TRUE(stTimeAlignRuleExpiryAfter(config, 0, INT64_MIN + 614, &ts));
  EXPECT_EQ(INT64_MAX, ts);
  EXPECT_FALSE(stTimeAlignRuleExpiryAfter(config, 0, INT64_MIN + 615, &ts));
  ASSERT_TRUE(stTimeAlignSeedFloor(config, 10, INT64_MAX, &ts));
  EXPECT_EQ(INT64_MIN, ts);
  EXPECT_FALSE(stTimeAlignRuleExpiryAfter(config, 1, INT64_MIN, &ts));
  stTimeAlignConfigDestroy(&config);
}

TEST_F(StreamAlignStateTest, SparseExpiredStringsRemainNullAcrossHeartbeatBlocks) {
  addStringRule();
  std::vector<char> text(8192 + VARSTR_HEADER_SIZE, 'x');
  varDataSetLen(text.data(), 8192);
  row(0, 0, nullptr);
  ASSERT_EQ(0, colDataSetVal(column(4), 0, text.data(), false));
  ASSERT_EQ(0, stTimeAlignApply(state, 71, slots, 0, block, 0, 1));
  ASSERT_FALSE(colDataIsNull_s(column(5), 0));
  EXPECT_EQ(0, memcmp(colDataGetData(column(5), 0), text.data(), text.size()));
  for (int32_t beat = 1; beat <= 64; ++beat) {
    blockDataCleanup(block);
    row(0, 1000 + beat, &beat);
    colDataSetNULL(column(4), 0);
    ASSERT_EQ(0, stTimeAlignSavePassStart(state, 71));
    ASSERT_EQ(0, stTimeAlignApply(state, 71, slots, 0, block, 0, 1));
    EXPECT_EQ(beat, result(2, 0));
    EXPECT_TRUE(colDataIsNull_s(column(5), 0));
    stTimeAlignFinishPass(state, false);
  }
}

TEST_F(StreamAlignStateTest, FailedBatchAndRepeatedPassRestoreVariableCandidates) {
  addStringRule();
  char original[] = {0, 0, 'a'}, replacement[] = {0, 0, 'b'};
  varDataSetLen(original, 1);
  varDataSetLen(replacement, 1);
  row(0, 1000, nullptr);
  ASSERT_EQ(0, colDataSetVal(column(4), 0, original, false));
  ASSERT_EQ(0, stTimeAlignApply(state, 71, slots, 0, block, 0, 1));
  for (bool fail : {true, false}) {
    blockDataCleanup(block);
    ASSERT_EQ(0, stTimeAlignSavePassStart(state, 71));
    row(0, 1090, nullptr);
    ASSERT_EQ(0, colDataSetVal(column(4), 0, replacement, false));
    row(1, 1100, nullptr);
    if (fail) colDataSetNULL(column(0), 1);
    colDataSetNULL(column(4), 1);
    EXPECT_EQ(fail ? TSDB_CODE_INVALID_MSG : 0, stTimeAlignApply(state, 71, slots, 0, block, 0, 2));
    stTimeAlignFinishPass(state, !fail);
    blockDataCleanup(block);
    row(0, 2000, nullptr);
    row(1, 2001, nullptr);
    colDataSetNULL(column(4), 0);
    colDataSetNULL(column(4), 1);
    ASSERT_EQ(0, stTimeAlignApply(state, 71, slots, 0, block, 0, 2));
    ASSERT_FALSE(colDataIsNull_s(column(5), 0));
    EXPECT_EQ(0, memcmp(colDataGetData(column(5), 0), original, sizeof(original)));
    EXPECT_TRUE(colDataIsNull_s(column(5), 1));
  }
}

TEST_F(StreamAlignStateTest, SlidingPassRestoresPredecessorAcrossPagesAndCommitsLastPass) {
  seedEmpty(71, 1);
  int32_t initial = 7, final = 19;
  row(0, 0, &initial);
  ASSERT_EQ(0, stTimeAlignApply(state, 71, slots, 0, block, 0, 1));
  for (int pass = 0; pass < 3; ++pass) {
    ASSERT_EQ(0, stTimeAlignSavePassStart(state, 71));
    row(0, 1000, nullptr);
    ASSERT_EQ(0, stTimeAlignApply(state, 71, slots, 0, block, 0, 1));
    EXPECT_EQ(initial, result(2, 0));
    ASSERT_EQ(0, stTimeAlignSavePassStart(state, 71));
    row(0, 2000, &final);
    ASSERT_EQ(0, stTimeAlignApply(state, 71, slots, 0, block, 0, 1));
    stTimeAlignFinishPass(state, pass < 2);
  }
  row(0, 3000, nullptr);
  ASSERT_EQ(0, stTimeAlignApply(state, 71, slots, 0, block, 0, 1));
  EXPECT_EQ(final, result(2, 0));
}

TEST_F(StreamAlignStateTest, WideCalendarConfigReleasesTimezoneOnSuccessAndFailure) {
  taosArrayClear(spec.pColumns);
  char timezone[] = "America/New_York";
  spec.timezone = timezone;
  SStreamTimeAlignColumn def = {};
  def.hasSpan = true;
  def.amount = 1;
  for (int i = 0; i < 1024; ++i) {
    def.srcColId = static_cast<col_id_t>(i + 1);
    def.unit = (i % 2 == 0) ? 'n' : 'y';
    ASSERT_NE(nullptr, taosArrayPush(spec.pColumns, &def));
  }
  SSTimeAlignConfig* config = nullptr;
  ASSERT_EQ(0, stTimeAlignConfigCreate(&spec, &config));
  stTimeAlignConfigDestroy(&config);
  def.amount = -1;
  ASSERT_NE(nullptr, taosArrayPush(spec.pColumns, &def));
  EXPECT_EQ(TSDB_CODE_OUT_OF_RANGE, stTimeAlignConfigCreate(&spec, &config));
  EXPECT_EQ(nullptr, config);
}

TEST_F(StreamAlignStateTest, CalendarDeadlinesFollowSourceChangesAcrossHeartbeatBatches) {
  stTimeAlignDestroy(&state);
  taosArrayClear(spec.pColumns);
  taosArrayClear(slots);
  char zone[] = "America/New_York";
  spec.timezone = zone;
  constexpr int rules = 1024;
  for (int i = 0; i < rules; ++i) {
    SStreamTimeAlignColumn def = {};
    def.srcColId = 9;
    def.hasSpan = true;
    def.amount = 1;
    def.unit = i % 2 ? 'y' : 'n';
    def.type.type = TSDB_DATA_TYPE_INT;
    def.type.bytes = sizeof(int32_t);
    ASSERT_NE(nullptr, taosArrayPush(spec.pColumns, &def));
    SSTimeAlignSlot slot = {9, 1, 2 + i};
    ASSERT_NE(nullptr, taosArrayPush(slots, &slot));
    if (i >= 2) {
      auto col = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), i + 3);
      ASSERT_EQ(0, blockDataAppendColInfo(block, &col));
    }
  }
  constexpr int batchRows = 100;
  ASSERT_EQ(0, blockDataEnsureCapacity(block, batchRows));
  ASSERT_EQ(0, stTimeAlignCreate(&spec, &state));
  seedEmpty(71, 1);
  int32_t value = 7;
  int64_t end = 1710055800000LL;
  row(0, 1707550200000LL, &value);
  ASSERT_EQ(0, stTimeAlignApply(state, 71, slots, 0, block, 0, 1));
  for (int batch = 0; batch < 10; ++batch) {
    for (int r = 0; r < batchRows; ++r) row(r, end - 998 + batch * batchRows + r, nullptr);
    ASSERT_EQ(0, stTimeAlignApply(state, 71, slots, 0, block, 0, batchRows));
  }
  for (int i = 0; i < rules; ++i) {
    EXPECT_EQ(value, result(2 + i, batchRows - 2));
    EXPECT_EQ(i % 2 == 0, colDataIsNull_s(column(2 + i), batchRows - 1));
  }
  value = 9;
  row(0, end + 2, &value);
  row(1, end + 3, nullptr);
  ASSERT_EQ(0, stTimeAlignApply(state, 71, slots, 0, block, 0, 2));
  for (int i = 0; i < rules; ++i) EXPECT_EQ(value, result(2 + i, 1));
}

TEST_F(StreamAlignStateTest, FixedOnlyConfigDoesNotRequireTimezone) {
  char timezone[] = "invalid timezone";
  spec.timezone = timezone;
  SSTimeAlignConfig* config = nullptr;
  EXPECT_EQ(0, stTimeAlignConfigCreate(&spec, &config));
  stTimeAlignConfigDestroy(&config);
}

TEST_F(StreamAlignStateTest, SharedCalendarRuleKeepsIndependentGroupCandidatesAcrossDst) {
  stTimeAlignDestroy(&state);
  auto* calendar = static_cast<SStreamTimeAlignColumn*>(taosArrayGet(spec.pColumns, 1));
  calendar->unit = 'n';
  calendar->amount = 1;
  char timezone[] = "America/New_York";
  spec.timezone = timezone;
  SSTimeAlignConfig* config = nullptr;
  ASSERT_EQ(0, stTimeAlignConfigCreate(&spec, &config));
  SSTimeAlignState* other = nullptr;
  ASSERT_EQ(0, stTimeAlignCreateFromConfig(config, &state));
  ASSERT_EQ(0, stTimeAlignCreateFromConfig(config, &other));
  seedEmpty(71, 1);
  seedEmpty(71, 1, other);
  int32_t value = 19;
  // March 9 and April 9 at local noon are one calendar month apart across spring DST.
  row(0, 1710003600000LL, &value);
  ASSERT_EQ(0, stTimeAlignApply(state, 71, slots, 0, block, 0, 1));
  row(0, 1712678400000LL, nullptr);
  ASSERT_EQ(0, stTimeAlignApply(other, 71, slots, 0, block, 0, 1));
  EXPECT_TRUE(colDataIsNull_s(column(3), 0));
  stTimeAlignDestroy(&other);
  row(1, 1712678400001LL, nullptr);
  ASSERT_EQ(0, stTimeAlignApply(state, 71, slots, 0, block, 0, 2));
  EXPECT_EQ(value, result(3, 0));
  EXPECT_TRUE(colDataIsNull_s(column(3), 1));
  stTimeAlignDestroy(&state);
  stTimeAlignConfigDestroy(&config);
}

TEST(StreamTimeAlignTest, SeedFloorCombinesRepeatedRulesAndSaturatesAtTheFirstTimestamp) {
  SStreamTimeAlignSpec spec = {};
  spec.precision = TSDB_TIME_PRECISION_MILLI;
  struct Rule {
    col_id_t id;
    bool     bounded;
    int64_t  amount;
    char     unit;
  };
  const Rule rules[] = {{2, true, 5, 's'}, {2, true, 0, 's'},  {3, true, 1, 'n'}, {3, true, 5, 's'},
                        {4, true, 1, 's'}, {4, false, 0, 's'}, {5, true, 1, 'u'}, {6, true, 0, 'n'}};
  spec.pColumns = taosArrayInit(7, sizeof(SStreamTimeAlignColumn));
  for (const auto& rule : rules) {
    SStreamTimeAlignColumn def = {};
    def.srcColId = rule.id;
    def.hasSpan = rule.bounded;
    def.amount = rule.amount;
    def.unit = rule.unit;
    ASSERT_NE(nullptr, taosArrayPush(spec.pColumns, &def));
  }
  SSTimeAlignConfig* config = nullptr;
  ASSERT_EQ(0, stTimeAlignConfigCreate(&spec, &config));
  int64_t floor = 0;
  ASSERT_TRUE(stTimeAlignSeedFloor(config, 2, -10, &floor));
  EXPECT_EQ(-5010, floor);
  ASSERT_TRUE(stTimeAlignSeedFloor(config, 2, INT64_MIN + 5, &floor));
  EXPECT_EQ(INT64_MIN, floor);
  ASSERT_TRUE(stTimeAlignSeedFloor(config, 3, 10000, &floor));
  EXPECT_EQ(10000 - 33LL * 86400000, floor);
  ASSERT_TRUE(stTimeAlignSeedFloor(config, 4, 10000, &floor));
  EXPECT_EQ(INT64_MIN, floor);
  ASSERT_TRUE(stTimeAlignSeedFloor(config, 5, 10000, &floor));
  EXPECT_EQ(10000, floor);
  ASSERT_TRUE(stTimeAlignSeedFloor(config, 6, 10000, &floor));
  EXPECT_EQ(10000, floor);
  EXPECT_FALSE(stTimeAlignSeedFloor(config, 7, 10000, &floor));
  stTimeAlignConfigDestroy(&config);
  taosArrayDestroy(spec.pColumns);
}

TEST(StreamTimeAlignTest, FixedExpiryRequiresAnInstantAfterTheInclusiveBoundary) {
  SStreamTimeAlignSpec spec = {};
  spec.precision = TSDB_TIME_PRECISION_MILLI;
  spec.pColumns = taosArrayInit(4, sizeof(SStreamTimeAlignColumn));
  ASSERT_NE(nullptr, spec.pColumns);
  SStreamTimeAlignColumn def = {};
  def.srcColId = 2;
  def.type.type = TSDB_DATA_TYPE_INT;
  def.type.bytes = sizeof(int32_t);
  for (const char unit : {'s', 's', 'n', 's'}) {
    const auto index = taosArrayGetSize(spec.pColumns);
    def.hasSpan = index != 3;
    def.unit = unit;
    def.amount = index == 0 ? 5 : index == 1 ? 0 : 1;
    ASSERT_NE(nullptr, taosArrayPush(spec.pColumns, &def));
  }
  SSTimeAlignConfig* config = nullptr;
  ASSERT_EQ(0, stTimeAlignConfigCreate(&spec, &config));
  int64_t stable = 0;
  EXPECT_TRUE(stTimeAlignRuleExpiryAfter(config, 0, 10000, &stable));
  EXPECT_EQ(15001, stable);
  EXPECT_TRUE(stTimeAlignRuleExpiryAfter(config, 1, -1, &stable));
  EXPECT_EQ(0, stable);
  EXPECT_FALSE(stTimeAlignRuleExpiryAfter(config, 0, INT64_MAX - 5000, &stable));
  EXPECT_FALSE(stTimeAlignRuleExpiryAfter(config, 2, 10000, &stable));
  EXPECT_FALSE(stTimeAlignRuleExpiryAfter(config, 3, 10000, &stable));
  stTimeAlignConfigDestroy(&config);
  taosArrayDestroy(spec.pColumns);
}
