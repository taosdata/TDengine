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
#include <cstdint>
#include <cstdio>
#include <initializer_list>
#include <string>
#include <tuple>
#include <vector>
#include "dataSink.h"
#include "tglobal.h"

#include "streamTriggerMerger.h"
#include "streamTriggerTask.h"
#include "nodes.h"
#include "stub.h"
#include "tdatablock.h"

namespace {

Stub   *gPeerArrayGetStub = nullptr;
int64_t gPeerArrayGetCalls = 0;

void *countPeerArrayGet(const SArray *array, int32_t index) {
  ++gPeerArrayGetCalls;
  gPeerArrayGetStub->reset(taosArrayGet);
  void *value = taosArrayGet(array, index);
  gPeerArrayGetStub->set(taosArrayGet, countPeerArrayGet);
  return value;
}

struct TestRow {
  TSKEY              eventTs;
  SWindowChainRowRef ref;
};

struct TestSource {
  TestSource() = default;
  TestSource(std::initializer_list<TestRow> initialRows) : rows(initialRows) {}

  std::vector<TestRow> rows;
  size_t               index = 0;
  int32_t              consumeCalls = 0;
  int32_t              peekCalls = 0;
  bool                 needInput = false;
  int32_t              failCode = TSDB_CODE_SUCCESS;
  int32_t              eventTsSlot = -1;
};

int32_t peekSource(void *pSource, SStreamTriggerPeerHead *pHead, EStreamTriggerPeerSourceStatus *pStatus) {
  auto *source = static_cast<TestSource *>(pSource);
  ++source->peekCalls;
  if (source->failCode != TSDB_CODE_SUCCESS) return source->failCode;
  if (source->needInput) {
    *pStatus = STREAM_TRIGGER_PEER_SOURCE_NEED_INPUT;
    return TSDB_CODE_SUCCESS;
  }
  if (source->index == source->rows.size()) {
    *pStatus = STREAM_TRIGGER_PEER_SOURCE_EOF;
    return TSDB_CODE_SUCCESS;
  }
  const TestRow &row = source->rows[source->index];
  pHead->eventTs = row.eventTs;
  pHead->row = row.ref;
  if (source->eventTsSlot >= 0) {
    const auto *column =
        static_cast<const SColumnInfoData *>(taosArrayGet(row.ref.pBlock->pDataBlock, source->eventTsSlot));
    if (column == nullptr || column->info.type != TSDB_DATA_TYPE_TIMESTAMP) return TSDB_CODE_INVALID_PARA;
    pHead->eventTs = *reinterpret_cast<const TSKEY *>(colDataGetData(column, row.ref.rowIndex));
  }
  *pStatus = STREAM_TRIGGER_PEER_SOURCE_ROW;
  return TSDB_CODE_SUCCESS;
}

void consumeSource(void *pSource) {
  auto *source = static_cast<TestSource *>(pSource);
  ++source->consumeCalls;
  ++source->index;
}

const SStreamTriggerPeerSourceOps kSourceOps = {peekSource, consumeSource};

class PeerMerger {
 public:
  explicit PeerMerger(int64_t gid) { EXPECT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerPeerCreate(gid, &value_)); }
  ~PeerMerger() { stTriggerMergerPeerDestroy(&value_); }

  SStreamTriggerPeerMerger *get() const { return value_; }

 private:
  SStreamTriggerPeerMerger *value_ = nullptr;
};

SSDataBlock makeBorrowedBlock(int64_t rows = 4) {
  SSDataBlock block = {};
  block.info.rows = rows;
  return block;
}

TestRow row(TSKEY ts, const SSDataBlock *pBlock, int32_t rowIndex, int64_t tableUid) {
  return TestRow{ts, {pBlock, rowIndex, tableUid}};
}

SSDataBlock *makeConfiguredTimestampBlock(TSKEY firstSlotTs, TSKEY configuredSlotTs) {
  SSDataBlock *block = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, createDataBlock(&block));
  if (block == nullptr) return nullptr;
  SColumnInfoData first = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(TSKEY), 1);
  SColumnInfoData configured = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(TSKEY), 2);
  EXPECT_EQ(TSDB_CODE_SUCCESS, blockDataAppendColInfo(block, &first));
  EXPECT_EQ(TSDB_CODE_SUCCESS, blockDataAppendColInfo(block, &configured));
  EXPECT_EQ(TSDB_CODE_SUCCESS, blockDataEnsureCapacity(block, 1));
  auto *firstColumn = static_cast<SColumnInfoData *>(taosArrayGet(block->pDataBlock, 0));
  auto *configuredColumn = static_cast<SColumnInfoData *>(taosArrayGet(block->pDataBlock, 1));
  EXPECT_EQ(TSDB_CODE_SUCCESS, colDataSetVal(firstColumn, 0, reinterpret_cast<const char *>(&firstSlotTs), false));
  EXPECT_EQ(TSDB_CODE_SUCCESS,
            colDataSetVal(configuredColumn, 0, reinterpret_cast<const char *>(&configuredSlotTs), false));
  block->info.rows = 1;
  return block;
}

void *failMemoryRealloc(void *, int64_t) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return nullptr;
}

using GroupView = std::pair<TSKEY, std::vector<std::pair<int64_t, int32_t>>>;

std::vector<GroupView> drain(SStreamTriggerPeerMerger *pMerger) {
  std::vector<GroupView> groups;
  for (;;) {
    EStreamTriggerPeerGroupStatus status = STREAM_TRIGGER_PEER_GROUP_NEED_INPUT;
    int32_t                       needSource = -1;
    const SWindowChainPeerGroup  *group = nullptr;
    EXPECT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerNextPeerGroup(pMerger, &status, &needSource, &group));
    if (status == STREAM_TRIGGER_PEER_GROUP_EOF) break;
    EXPECT_EQ(STREAM_TRIGGER_PEER_GROUP_READY, status);
    EXPECT_EQ(-1, needSource);
    if (group == nullptr) {
      ADD_FAILURE() << "READY status returned without a peer group";
      break;
    }
    GroupView view = {group->ts, {}};
    for (int32_t i = 0; i < taosArrayGetSize(group->pRows); ++i) {
      const auto *ref = static_cast<const SWindowChainRowRef *>(taosArrayGet(group->pRows, i));
      view.second.emplace_back(ref->tableUid, ref->rowIndex);
    }
    std::sort(view.second.begin(), view.second.end());
    groups.push_back(view);
  }
  return groups;
}

TEST(StreamTriggerMergerTest, GroupsEverySameTimestampPeerAcrossTables) {
  SSDataBlock blockA = makeBorrowedBlock();
  SSDataBlock blockB = makeBorrowedBlock();
  TestSource  a{{row(10, &blockA, 0, 101), row(30, &blockA, 1, 101)}};
  TestSource  b{{row(10, &blockB, 0, 202), row(20, &blockB, 1, 202)}};
  PeerMerger  merger(7);
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerPeerAddSource(merger.get(), 101, &kSourceOps, &a));
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerPeerAddSource(merger.get(), 202, &kSourceOps, &b));

  const auto groups = drain(merger.get());
  ASSERT_EQ(3U, groups.size());
  EXPECT_EQ(10, groups[0].first);
  EXPECT_EQ((std::vector<std::pair<int64_t, int32_t>>{{101, 0}, {202, 0}}), groups[0].second);
}

TEST(StreamTriggerMergerTest, ManySourcesDoNotRescanEveryHeadPerGroup) {
  constexpr int32_t       sourceCount = 128;
  SSDataBlock             block = makeBorrowedBlock(1);
  std::vector<TestSource> sources;
  sources.reserve(sourceCount);
  PeerMerger merger(7);
  for (int32_t i = 0; i < sourceCount; ++i) {
    const int64_t tableUid = 1000 + i;
    sources.emplace_back(std::initializer_list<TestRow>{row(i, &block, 0, tableUid)});
    ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerPeerAddSource(merger.get(), tableUid, &kSourceOps, &sources.back()));
  }

  Stub arrayGetCounter;
  gPeerArrayGetStub = &arrayGetCounter;
  gPeerArrayGetCalls = 0;
  arrayGetCounter.set(taosArrayGet, countPeerArrayGet);
  const auto groups = drain(merger.get());
  arrayGetCounter.reset(taosArrayGet);
  gPeerArrayGetStub = nullptr;

  ASSERT_EQ(sourceCount, groups.size());
  EXPECT_LT(gPeerArrayGetCalls, INT64_C(30000));
}

TEST(StreamTriggerMergerTest, ManySourcesRegisterWithoutQuadraticArrayScans) {
  constexpr int32_t       sourceCount = 128;
  SSDataBlock             block = makeBorrowedBlock(1);
  std::vector<TestSource> sources;
  sources.reserve(sourceCount);
  PeerMerger merger(7);

  Stub arrayGetCounter;
  gPeerArrayGetStub = &arrayGetCounter;
  gPeerArrayGetCalls = 0;
  arrayGetCounter.set(taosArrayGet, countPeerArrayGet);
  for (int32_t i = 0; i < sourceCount; ++i) {
    const int64_t tableUid = 1000 + i;
    sources.emplace_back(std::initializer_list<TestRow>{row(i, &block, 0, tableUid)});
    ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerPeerAddSource(merger.get(), tableUid, &kSourceOps, &sources.back()));
  }
  arrayGetCounter.reset(taosArrayGet);
  gPeerArrayGetStub = nullptr;

  EXPECT_LT(gPeerArrayGetCalls, INT64_C(512));
}

TEST(StreamTriggerMergerTest, TableEnumerationOrderDoesNotChangePeerGroups) {
  SSDataBlock blockA = makeBorrowedBlock();
  SSDataBlock blockB = makeBorrowedBlock();
  TestSource  a1{{row(10, &blockA, 0, 101), row(30, &blockA, 1, 101)}};
  TestSource  b1{{row(10, &blockB, 0, 202), row(20, &blockB, 1, 202)}};
  TestSource  a2 = a1;
  TestSource  b2 = b1;
  PeerMerger  first(7);
  PeerMerger  second(7);
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerPeerAddSource(first.get(), 101, &kSourceOps, &a1));
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerPeerAddSource(first.get(), 202, &kSourceOps, &b1));
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerPeerAddSource(second.get(), 202, &kSourceOps, &b2));
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerPeerAddSource(second.get(), 101, &kSourceOps, &a2));
  EXPECT_EQ(drain(first.get()), drain(second.get()));
}

TEST(StreamTriggerMergerTest, EmitsGidEventTimeInNondecreasingOrder) {
  SSDataBlock blockA = makeBorrowedBlock();
  SSDataBlock blockB = makeBorrowedBlock();
  TestSource  a{{row(5, &blockA, 0, 101), row(50, &blockA, 1, 101)}};
  TestSource  b{{row(10, &blockB, 0, 202), row(40, &blockB, 1, 202)}};
  PeerMerger  merger(99);
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerPeerAddSource(merger.get(), 101, &kSourceOps, &a));
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerPeerAddSource(merger.get(), 202, &kSourceOps, &b));
  const auto         groups = drain(merger.get());
  std::vector<TSKEY> timestamps;
  for (const auto &group : groups) timestamps.push_back(group.first);
  EXPECT_EQ((std::vector<TSKEY>{5, 10, 40, 50}), timestamps);
}

TEST(StreamTriggerMergerTest, NeedInputBlocksUntilEverySourceHeadIsKnown) {
  SSDataBlock block = makeBorrowedBlock();
  TestSource  ready{{row(10, &block, 0, 101)}};
  TestSource  blocked{{row(5, &block, 1, 202)}};
  blocked.needInput = true;
  PeerMerger merger(7);
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerPeerAddSource(merger.get(), 101, &kSourceOps, &ready));
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerPeerAddSource(merger.get(), 202, &kSourceOps, &blocked));

  EStreamTriggerPeerGroupStatus status = STREAM_TRIGGER_PEER_GROUP_READY;
  int32_t                       needSource = -1;
  const SWindowChainPeerGroup  *group = reinterpret_cast<const SWindowChainPeerGroup *>(1);
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerNextPeerGroup(merger.get(), &status, &needSource, &group));
  EXPECT_EQ(STREAM_TRIGGER_PEER_GROUP_NEED_INPUT, status);
  EXPECT_EQ(1, needSource);
  EXPECT_EQ(nullptr, group);
  EXPECT_EQ(0, ready.consumeCalls);
  blocked.needInput = false;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerNextPeerGroup(merger.get(), &status, &needSource, &group));
  ASSERT_EQ(STREAM_TRIGGER_PEER_GROUP_READY, status);
  EXPECT_EQ(5, group->ts);
}

TEST(StreamTriggerMergerTest, ReadsEventTimestampFromConfiguredSlot) {
  SSDataBlock *block = makeConfiguredTimestampBlock(9999, 1234);
  ASSERT_NE(nullptr, block);
  TestSource source{{row(7777, block, 0, 101)}};
  source.eventTsSlot = 1;
  PeerMerger merger(7);
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerPeerAddSource(merger.get(), 101, &kSourceOps, &source));
  const auto groups = drain(merger.get());
  ASSERT_EQ(1U, groups.size());
  EXPECT_EQ(1234, groups[0].first);
  blockDataDestroy(block);
}

TEST(StreamTriggerMergerTest, BlockBoundaryKeepsReturnedPeerRowsPinned) {
  SSDataBlock first = makeBorrowedBlock(1);
  SSDataBlock second = makeBorrowedBlock(1);
  TestSource  source{{row(10, &first, 0, 101), row(20, &second, 0, 101)}};
  PeerMerger  merger(7);
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerPeerAddSource(merger.get(), 101, &kSourceOps, &source));
  EStreamTriggerPeerGroupStatus status;
  int32_t                       needSource;
  const SWindowChainPeerGroup  *group = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerNextPeerGroup(merger.get(), &status, &needSource, &group));
  ASSERT_EQ(STREAM_TRIGGER_PEER_GROUP_READY, status);
  EXPECT_EQ(0, source.consumeCalls);
  const auto *ref = static_cast<const SWindowChainRowRef *>(taosArrayGet(group->pRows, 0));
  EXPECT_EQ(&first, ref->pBlock);
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerNextPeerGroup(merger.get(), &status, &needSource, &group));
  EXPECT_EQ(1, source.consumeCalls);
}

TEST(StreamTriggerMergerTest, AllocationFailureDoesNotAdvancePartialPeer) {
  SSDataBlock block = makeBorrowedBlock(5);
  TestSource  first{{row(10, &block, 0, 101)}};
  TestSource  second{{row(10, &block, 1, 202)}};
  TestSource  third{{row(10, &block, 2, 303)}};
  TestSource  fourth{{row(10, &block, 3, 404)}};
  TestSource  fifth{{row(10, &block, 4, 505)}};
  PeerMerger merger(7);
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerPeerAddSource(merger.get(), 101, &kSourceOps, &first));
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerPeerAddSource(merger.get(), 202, &kSourceOps, &second));
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerPeerAddSource(merger.get(), 303, &kSourceOps, &third));
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerPeerAddSource(merger.get(), 404, &kSourceOps, &fourth));
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerPeerAddSource(merger.get(), 505, &kSourceOps, &fifth));
  EStreamTriggerPeerGroupStatus status;
  int32_t                       needSource;
  const SWindowChainPeerGroup  *group = nullptr;
  {
    Stub allocationFailure;
    allocationFailure.set(taosMemRealloc, failMemoryRealloc);
    EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, stTriggerMergerNextPeerGroup(merger.get(), &status, &needSource, &group));
  }
  EXPECT_EQ(0, first.consumeCalls);
  EXPECT_EQ(0, second.consumeCalls);
  EXPECT_EQ(0, third.consumeCalls);
  EXPECT_EQ(0, fourth.consumeCalls);
  EXPECT_EQ(0, fifth.consumeCalls);
  EXPECT_NE(TSDB_CODE_SUCCESS, stTriggerMergerNextPeerGroup(merger.get(), &status, &needSource, &group));
  stTriggerMergerPeerReset(merger.get(), 8);
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerPeerAddSource(merger.get(), 101, &kSourceOps, &first));
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerPeerAddSource(merger.get(), 202, &kSourceOps, &second));
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerPeerAddSource(merger.get(), 303, &kSourceOps, &third));
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerPeerAddSource(merger.get(), 404, &kSourceOps, &fourth));
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerPeerAddSource(merger.get(), 505, &kSourceOps, &fifth));
  const auto groups = drain(merger.get());
  ASSERT_EQ(1U, groups.size());
  EXPECT_EQ(10, groups[0].first);
}

TEST(StreamTriggerMergerTest, ResetDiscardsReadyWithoutConsumingSources) {
  SSDataBlock block = makeBorrowedBlock();
  TestSource  source{{row(10, &block, 0, 101)}};
  PeerMerger  merger(7);
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerPeerAddSource(merger.get(), 101, &kSourceOps, &source));
  EStreamTriggerPeerGroupStatus status;
  int32_t                       needSource;
  const SWindowChainPeerGroup  *group = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerNextPeerGroup(merger.get(), &status, &needSource, &group));
  stTriggerMergerPeerReset(merger.get(), 8);
  EXPECT_EQ(0, source.consumeCalls);
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerPeerAddSource(merger.get(), 101, &kSourceOps, &source));
}

TEST(StreamTriggerMergerTest, DestroyDoesNotCallBorrowedSourceOps) {
  SSDataBlock               block = makeBorrowedBlock();
  TestSource                source{{row(10, &block, 0, 101)}};
  SStreamTriggerPeerMerger *merger = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerPeerCreate(7, &merger));
  ASSERT_EQ(TSDB_CODE_SUCCESS, stTriggerMergerPeerAddSource(merger, 101, &kSourceOps, &source));
  stTriggerMergerPeerDestroy(&merger);
  EXPECT_EQ(nullptr, merger);
  EXPECT_EQ(0, source.peekCalls);
  EXPECT_EQ(0, source.consumeCalls);
}

}  // namespace

class StreamHistoryFilteredMergerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    merger_ = static_cast<SSTriggerVtableMerger *>(taosMemoryCalloc(1, sizeof(SSTriggerVtableMerger)));
    ASSERT_NE(merger_, nullptr);
    SSDataBlock *block = MakeBlock(0, 0, false);
    ASSERT_NE(block, nullptr);
    SNode *column = nullptr, *value = nullptr, *condition = nullptr;
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_COLUMN, &column), 0);
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_VALUE, &value), 0);
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_OPERATOR, &condition), 0);
    auto *col = reinterpret_cast<SColumnNode *>(column);
    col->node.resType.type = TSDB_DATA_TYPE_BIGINT;
    col->node.resType.bytes = sizeof(int64_t);
    col->slotId = 1;
    auto *val = reinterpret_cast<SValueNode *>(value);
    val->node.resType = col->node.resType;
    val->translate = true;
    int64_t one = 1;
    ASSERT_EQ(nodesSetValueNodeValue(val, &one), 0);
    auto *op = reinterpret_cast<SOperatorNode *>(condition);
    op->node.resType.type = TSDB_DATA_TYPE_BOOL;
    op->node.resType.bytes = sizeof(bool);
    op->opType = OP_TYPE_EQUAL;
    op->pLeft = column;
    op->pRight = value;
    SFilterInfo *filter = nullptr;
    ASSERT_EQ(filterInitFromNode(condition, &filter, 0, nullptr), 0);
    condition_ = condition;
    ASSERT_EQ(stVtableMergerInit(merger_, &task_, &block, &filter, 2), 0);

    refs_ = taosArrayInit(1, sizeof(SSTriggerTableColRef));
    matches_ = taosArrayInit(1, sizeof(SSTriggerColMatch));
    metas_ = taosArrayInit(2, sizeof(SSTriggerMetaData));
    ASSERT_NE(refs_, nullptr);
    ASSERT_NE(matches_, nullptr);
    ASSERT_NE(metas_, nullptr);
    SSTriggerColMatch match = {};
    match.otbSlotId = match.vtbSlotId = 1;
    ASSERT_NE(taosArrayPush(matches_, &match), nullptr);
    SSTriggerTableColRef ref = {};
    ref.otbUid = 1;
    ref.pColMatches = matches_;
    ASSERT_NE(taosArrayPush(refs_, &ref), nullptr);
  }

  void TearDown() override {
    stVtableMergerDestroy(&merger_);
    nodesDestroyNode(condition_);
    taosArrayDestroy(refs_);
    taosArrayDestroy(matches_);
    taosArrayDestroy(metas_);
  }

  SSDataBlock *MakeBlock(int64_t start, int32_t rows, bool lastMatches) {
    SSDataBlock *block = nullptr;
    EXPECT_EQ(createDataBlock(&block), 0);
    if (block == nullptr) return nullptr;
    SColumnInfoData ts = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 0);
    SColumnInfoData value = createColumnInfoData(TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), 1);
    EXPECT_EQ(blockDataAppendColInfo(block, &ts), 0);
    EXPECT_EQ(blockDataAppendColInfo(block, &value), 0);
    EXPECT_EQ(blockDataEnsureCapacity(block, rows), 0);
    auto *tsCol = static_cast<SColumnInfoData *>(taosArrayGet(block->pDataBlock, 0));
    auto *valueCol = static_cast<SColumnInfoData *>(taosArrayGet(block->pDataBlock, 1));
    for (int32_t row = 0; row < rows; ++row) {
      int64_t timestamp = start + row;
      int64_t data = lastMatches && row == rows - 1 ? 1 : 0;
      EXPECT_EQ(colDataSetVal(tsCol, row, reinterpret_cast<const char *>(&timestamp), false), 0);
      EXPECT_EQ(colDataSetVal(valueCol, row, reinterpret_cast<const char *>(&data), false), 0);
    }
    block->info.rows = rows;
    return block;
  }

  void Prepare(std::initializer_list<int32_t> blockRows) {
    int64_t start = 1;
    for (int32_t rows : blockRows) {
      SSTriggerMetaData meta = {};
      meta.skey = start;
      meta.ekey = start + rows - 1;
      meta.nrows = rows;
      meta.ver = 1;
      ASSERT_NE(taosArrayPush(metas_, &meta), nullptr);
      start += rows;
    }
    STimeWindow range = {};
    range.skey = 1;
    range.ekey = start - 1;
    ASSERT_EQ(stVtableMergerSetMergeInfo(merger_, &range, refs_), 0);
    SSTriggerTableMeta table = {};
    table.tbUid = 1;
    table.pMetas = metas_;
    SSHashObj *tables = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    ASSERT_NE(tables, nullptr);
    ASSERT_EQ(tSimpleHashPut(tables, &table.tbUid, sizeof(table.tbUid), &table, sizeof(table)), 0);
    EXPECT_EQ(stVtableMergerSetMetaDatas(merger_, tables), 0);
    tSimpleHashCleanup(tables);
    SSDataBlock *output = nullptr;
    ASSERT_EQ(stVtableMergerNextDataBlock(merger_, &output), 0);
    EXPECT_EQ(output, nullptr);
    ExpectFetch(1);
  }

  void ExpectFetch(int64_t start) {
    SSTriggerMetaData    *meta = nullptr;
    SSTriggerTableColRef *ref = nullptr;
    ASSERT_EQ(stVtableMergerGetMetaToFetch(merger_, &meta, &ref), 0);
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(meta->skey, start);
  }

  void Bind(int64_t start, int32_t rows, bool lastMatches) {
    SSDataBlock *block = MakeBlock(start, rows, lastMatches);
    ASSERT_NE(block, nullptr);
    EXPECT_EQ(stVtableMergerBindDataBlock(merger_, &block), 0);
    EXPECT_EQ(block, nullptr);
    blockDataDestroy(block);
  }

  SStreamTriggerTask     task_ = {};
  SNode                 *condition_ = nullptr;
  SSTriggerVtableMerger *merger_ = nullptr;
  SArray                *refs_ = nullptr;
  SArray                *matches_ = nullptr;
  SArray                *metas_ = nullptr;
};

TEST_F(StreamHistoryFilteredMergerTest, EntirelyFilteredInputReturnsEofWithoutEmptyBlock) {
  Prepare({3});
  Bind(1, 3, false);
  SSDataBlock *output = nullptr;
  ASSERT_EQ(stVtableMergerNextDataBlock(merger_, &output), 0);
  EXPECT_EQ(output, nullptr);
  EXPECT_TRUE(IS_TRIGGER_VTABLE_MERGER_EMPTY(merger_));
}

TEST_F(StreamHistoryFilteredMergerTest, SkipsFilteredBatchesWithoutLosingBufferedMatch) {
  Prepare({8193});
  Bind(1, 8193, true);
  SSDataBlock *output = nullptr;
  ASSERT_EQ(stVtableMergerNextDataBlock(merger_, &output), 0);
  ASSERT_NE(output, nullptr);
  ASSERT_EQ(output->info.rows, 1);
  auto *ts = static_cast<SColumnInfoData *>(taosArrayGet(output->pDataBlock, 0));
  EXPECT_EQ(*reinterpret_cast<const int64_t *>(colDataGetData(ts, 0)), 8193);
  ASSERT_EQ(stVtableMergerNextDataBlock(merger_, &output), 0);
  EXPECT_EQ(output, nullptr);
  EXPECT_TRUE(IS_TRIGGER_VTABLE_MERGER_EMPTY(merger_));
}

TEST_F(StreamHistoryFilteredMergerTest, FilteredFullBatchStillRequestsFollowingData) {
  Prepare({4096, 1});
  Bind(1, 4096, false);
  SSDataBlock *output = nullptr;
  ASSERT_EQ(stVtableMergerNextDataBlock(merger_, &output), 0);
  EXPECT_EQ(output, nullptr);
  EXPECT_FALSE(IS_TRIGGER_VTABLE_MERGER_EMPTY(merger_));
  ExpectFetch(4097);
  Bind(4097, 1, true);
  ASSERT_EQ(stVtableMergerNextDataBlock(merger_, &output), 0);
  ASSERT_NE(output, nullptr);
  ASSERT_EQ(output->info.rows, 1);
  auto *ts = static_cast<SColumnInfoData *>(taosArrayGet(output->pDataBlock, 0));
  EXPECT_EQ(*reinterpret_cast<const int64_t *>(colDataGetData(ts, 0)), 4097);
}

class StreamRealtimeFilteredMergerTest : public StreamHistoryFilteredMergerTest {
 protected:
  void SetUp() override {
    StreamHistoryFilteredMergerTest::SetUp();
    ASSERT_FALSE(HasFatalFailure());
    realtime_ = static_cast<SSTriggerNewVtableMerger *>(taosMemoryCalloc(1, sizeof(SSTriggerNewVtableMerger)));
    ASSERT_NE(realtime_, nullptr);
    ASSERT_EQ(stNewVtableMergerInit(realtime_, &task_, merger_->pDataBlock, pseudoCols_, condition_), 0);
    input_ = MakeBlock(1, 8193, true);
    ASSERT_NE(input_, nullptr);
    ASSERT_EQ(taosObjPoolInit(&metaPool_, 1, sizeof(SSTriggerMetaData)), 0);
    ASSERT_EQ(taosObjPoolInit(&tablePool_, 1, sizeof(SSTriggerVirtualTableSliceRef)), 0);
    ASSERT_EQ(taosObjListInit(&metaList_, &metaPool_), 0);
    ASSERT_EQ(taosObjListInit(&tables_, &tablePool_), 0);
    SSTriggerMetaData meta = {};
    meta.skey = 1;
    meta.ekey = 8193;
    meta.nrows = 8193;
    ASSERT_EQ(taosObjListAppend(&metaList_, &meta), 0);
    SSTriggerVirtualTableSliceRef table = {};
    table.virtualUid = 2;
    table.readerKey = stTriggerReaderDataKey(STRIGGER_READER_KIND_EXTERNAL, 1, 1);
    ASSERT_EQ(taosObjListAppend(&tables_, &table), 0);
    auto *ref = static_cast<SSTriggerTableColRef *>(taosArrayGet(refs_, 0));
    ref->readerKey = table.readerKey;
    ref->pNewColMatches = matches_;
    SSTriggerColMatch tsMatch = {};
    ASSERT_NE(taosArrayPush(matches_, &tsMatch), nullptr);
    metaHash_ = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
    slices_ = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
    ASSERT_NE(metaHash_, nullptr);
    ASSERT_NE(slices_, nullptr);
    ASSERT_EQ(tSimpleHashPut(metaHash_, &table.readerKey, sizeof(table.readerKey), &metaList_, sizeof(metaList_)), 0);
    SSTriggerDataSlice slice = {};
    slice.pDataBlock = input_;
    slice.endIdx = 8193;
    ASSERT_EQ(tSimpleHashPut(slices_, &table.readerKey, sizeof(table.readerKey), &slice, sizeof(slice)), 0);
    STimeWindow range = {};
    range.skey = 1;
    range.ekey = 8193;
    ASSERT_EQ(
        stNewVtableMergerSetData(realtime_, 2, 0, -1, &range, &tables_, refs_, metaHash_, slices_, nullptr, false), 0);
  }

  void TearDown() override {
    stNewVtableMergerDestroy(&realtime_);
    blockDataDestroy(input_);
    tSimpleHashCleanup(metaHash_);
    tSimpleHashCleanup(slices_);
    taosObjListClear(&tables_);
    taosObjListClear(&metaList_);
    taosObjPoolDestroy(&tablePool_);
    taosObjPoolDestroy(&metaPool_);
    StreamHistoryFilteredMergerTest::TearDown();
  }

  SSTriggerNewVtableMerger *realtime_ = nullptr;
  SSDataBlock              *input_ = nullptr;
  SSHashObj                *metaHash_ = nullptr;
  SSHashObj                *slices_ = nullptr;
  SObjPool                  metaPool_ = {}, tablePool_ = {};
  SObjList                  metaList_ = {}, tables_ = {};
  bool                      pseudoCols_[2] = {};
};

TEST_F(StreamRealtimeFilteredMergerTest, ReleasesSkippedFilterResultsAndReturnsFollowingRow) {
  SSDataBlock *output = nullptr;
  int32_t      start = 0, end = 0;
  ASSERT_EQ(stNewVtableMergerNextDataBlock(realtime_, &output, &start, &end), 0);
  ASSERT_NE(output, nullptr);
  ASSERT_EQ(end - start, 1);
  auto *ts = static_cast<SColumnInfoData *>(taosArrayGet(output->pDataBlock, 0));
  EXPECT_EQ(*reinterpret_cast<const int64_t *>(colDataGetData(ts, start)), 8193);
  ASSERT_EQ(stNewVtableMergerNextDataBlock(realtime_, &output, &start, &end), 0);
  EXPECT_EQ(output, nullptr);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

class StreamPeriodSnapshotSorterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    savedTempDir_ = tsTempDir;
    savedBufferSize_ = tsStreamBufferSizeBytes;
    if (tsTempDir[0] == 0) std::snprintf(tsTempDir, TD_PATH_MAX, "%s", "/tmp");
    savedTempSpace_ = tsTempSpace;
    ASSERT_EQ(taosGetDiskSize(tsTempDir, &tsTempSpace.size), 0);
    ASSERT_EQ(initStreamDataSink(), 0);
    ASSERT_EQ(createDetachedStreamDataCache(901, 902, 903, DATA_CLEAN_IMMEDIATE, 0, &cache_), 0);
    sorter_ = static_cast<SSTriggerNewTimestampSorter *>(taosMemoryCalloc(1, sizeof(*sorter_)));
    ASSERT_NE(sorter_, nullptr);
    ASSERT_EQ(stNewTimestampSorterInit(sorter_, &task_, 0), 0);
  }
  void TearDown() override {
    stNewTimestampSorterDestroy(&sorter_);
    destroyStreamDataCache(cache_);
    destroyDataSinkMgr();
    setDataSinkMaxMemSize(savedBufferSize_);
    std::snprintf(tsTempDir, TD_PATH_MAX, "%s", savedTempDir_.c_str());
    tsTempSpace = savedTempSpace_;
  }
  void Append(int64_t source, const std::vector<std::array<int64_t, 4>> &rows, int32_t nullValueRow = -1) {
    SSDataBlock *block = nullptr;
    ASSERT_EQ(createDataBlock(&block), 0);
    for (int32_t i = 0; i < 4; ++i) {
      SColumnInfoData col = createColumnInfoData(i == 0 ? TSDB_DATA_TYPE_TIMESTAMP : TSDB_DATA_TYPE_BIGINT, 8, i + 1);
      EXPECT_EQ(blockDataAppendColInfo(block, &col), 0);
    }
    EXPECT_EQ(blockDataEnsureCapacity(block, rows.size()), 0);
    for (size_t row = 0; row < rows.size(); ++row) {
      for (int32_t col = 0; col < 4; ++col) {
        EXPECT_EQ(colDataSetVal(static_cast<SColumnInfoData *>(taosArrayGet(block->pDataBlock, col)), row,
                                reinterpret_cast<const char *>(&rows[row][col]), false),
                  0);
      }
    }
    block->info.rows = rows.size();
    if (nullValueRow >= 0) {
      colDataSetNULL(static_cast<SColumnInfoData *>(taosArrayGet(block->pDataBlock, 2)), nullValueRow);
    }
    EXPECT_EQ(putStreamDataCacheSnapshot(cache_, source, 0, block, 0, rows.size() - 1), 0);
    blockDataDestroy(block);
  }
  int32_t Bind(bool ordered) {
    SSTriggerDataSlice slice = {};
    slice.pSnapshotCache = cache_;
    slice.snapshotSourceId = 1;
    SSTriggerReaderDataKey key = stTriggerReaderDataKey(STRIGGER_READER_KIND_INTERNAL, 7, 17);
    STimeWindow            range = {};
    range.skey = TSKEY_MIN;
    range.ekey = TSKEY_MAX;
    return stNewTimestampSorterSetData(sorter_, &key, 0, 1, &range, nullptr, &slice, ordered);
  }
  int64_t              savedBufferSize_ = 0;
  std::vector<int64_t> Values() {
    std::vector<int64_t> values;
    for (;;) {
      SSDataBlock *block = nullptr;
      int32_t      start = 0, end = 0;
      int32_t      code = stNewTimestampSorterNextDataBlock(sorter_, &block, &start, &end);
      EXPECT_EQ(code, 0);
      if (code != 0 || block == nullptr) break;
      EXPECT_LE(end - start, 4096);
      auto *col = static_cast<SColumnInfoData *>(taosArrayGet(block->pDataBlock, 2));
      for (int32_t row = start; row < end; ++row)
        values.push_back(*reinterpret_cast<int64_t *>(colDataGetData(col, row)));
    }
    return values;
  }
  SDiskSpace                   savedTempSpace_ = {};
  std::string                  savedTempDir_;
  SStreamTriggerTask           task_ = {};
  SSTriggerNewTimestampSorter *sorter_ = nullptr;
  void                        *cache_ = nullptr;
};

TEST_F(StreamPeriodSnapshotSorterTest, MergesBatchesAndKeepsLatestVersionOfTimestampAndPrimaryKey) {
  Append(1, {{{100, 1, 11, 1}}, {{100, 2, 12, 1}}, {{300, 1, 13, 1}}});
  Append(1, {{{100, 1, 21, 2}}, {{200, 1, 22, 2}}});
  ASSERT_EQ(Bind(false), 0);
  EXPECT_EQ(Values(), (std::vector<int64_t>{21, 12, 22, 13}));
  stNewTimestampSorterReset(sorter_);
  ASSERT_EQ(Bind(false), 0);
  EXPECT_EQ(Values(), (std::vector<int64_t>{21, 12, 22, 13}));
}

TEST_F(StreamPeriodSnapshotSorterTest, OrderedInputFrontierSurvivesCacheBatchBoundaries) {
  Append(1, {{{100, 1, 11, 1}}, {{300, 1, 13, 1}}});
  Append(1, {{{100, 1, 21, 2}}, {{400, 1, 24, 2}}});
  ASSERT_EQ(Bind(true), 0);
  EXPECT_EQ(Values(), (std::vector<int64_t>{11, 13, 24}));
}

TEST_F(StreamPeriodSnapshotSorterTest, ReturnsBoundedBlocksAcrossLargePeriod) {
  std::vector<std::array<int64_t, 4>> rows;
  for (int64_t row = 1; row <= 8200; ++row) rows.push_back({{row, 1, row, 1}});
  Append(1, rows);
  Append(1, {{{4096, 1, 9000, 2}}});
  ASSERT_EQ(Bind(false), 0);
  auto values = Values();
  ASSERT_EQ(values.size(), 8200U);
  EXPECT_EQ(values.front(), 1);
  EXPECT_EQ(values[4095], 9000);
  EXPECT_EQ(values.back(), 8200);
}

class StreamPeriodSnapshotVtableTest : public StreamPeriodSnapshotSorterTest {
 protected:
  void SetUp() override {
    StreamPeriodSnapshotSorterTest::SetUp();
    ASSERT_FALSE(HasFatalFailure());
    SSDataBlock *shape = nullptr;
    ASSERT_EQ(createDataBlock(&shape), 0);
    for (int32_t i = 0; i < 3; ++i) {
      SColumnInfoData col = createColumnInfoData(i == 0 ? TSDB_DATA_TYPE_TIMESTAMP : TSDB_DATA_TYPE_BIGINT, 8, i + 1);
      ASSERT_EQ(blockDataAppendColInfo(shape, &col), 0);
    }
    merger_ = static_cast<SSTriggerNewVtableMerger *>(taosMemoryCalloc(1, sizeof(*merger_)));
    ASSERT_NE(merger_, nullptr);
    const int32_t code = stNewVtableMergerInit(merger_, &task_, shape, pseudo_, nullptr);
    blockDataDestroy(shape);
    ASSERT_EQ(code, 0);
    refs_ = taosArrayInit(2, sizeof(SSTriggerTableColRef));
    slices_ = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
    metas_ = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
    ASSERT_NE(refs_, nullptr);
    ASSERT_NE(slices_, nullptr);
    ASSERT_NE(metas_, nullptr);
    ASSERT_EQ(taosObjPoolInit(&pool_, 2, sizeof(SSTriggerVirtualTableSliceRef)), 0);
    ASSERT_EQ(taosObjListInit(&tables_, &pool_), 0);
    for (int64_t source = 1; source <= 2; ++source) {
      SSTriggerTableColRef ref = {};
      ref.readerKey = stTriggerReaderDataKey(STRIGGER_READER_KIND_INTERNAL, source, source);
      ref.pNewColMatches = taosArrayInit(2, sizeof(SSTriggerColMatch));
      ASSERT_NE(ref.pNewColMatches, nullptr);
      matches_[source - 1] = ref.pNewColMatches;
      SSTriggerColMatch timestamp = {};
      ASSERT_NE(taosArrayPush(ref.pNewColMatches, &timestamp), nullptr);
      SSTriggerColMatch value = {};
      value.otbSlotId = 2;
      value.vtbSlotId = source;
      ASSERT_NE(taosArrayPush(ref.pNewColMatches, &value), nullptr);
      ASSERT_NE(taosArrayPush(refs_, &ref), nullptr);
      SSTriggerVirtualTableSliceRef table = {};
      table.virtualUid = 42;
      table.readerKey = ref.readerKey;
      ASSERT_EQ(taosObjListAppend(&tables_, &table), 0);
      SSTriggerDataSlice slice = {};
      slice.pSnapshotCache = cache_;
      slice.snapshotSourceId = source;
      ASSERT_EQ(tSimpleHashPut(slices_, &ref.readerKey, sizeof(ref.readerKey), &slice, sizeof(slice)), 0);
    }
  }
  void TearDown() override {
    stNewVtableMergerDestroy(&merger_);
    taosArrayDestroy(matches_[0]);
    taosArrayDestroy(matches_[1]);
    taosArrayDestroy(refs_);
    tSimpleHashCleanup(slices_);
    tSimpleHashCleanup(metas_);
    taosObjListClear(&tables_);
    taosObjPoolDestroy(&pool_);
    StreamPeriodSnapshotSorterTest::TearDown();
  }
  SSTriggerNewVtableMerger *merger_ = nullptr;
  SArray                   *refs_ = nullptr;
  SArray                   *matches_[2] = {};
  SSHashObj                *slices_ = nullptr;
  SSHashObj                *metas_ = nullptr;
  SObjPool                  pool_ = {};
  SObjList                  tables_ = {};
  bool                      pseudo_[3] = {};
};

TEST_F(StreamPeriodSnapshotVtableTest, ColumnsFromDifferentPollsMergeAtTheSameEventTimestamp) {
  Append(1, {{{100, 1, 11, 1}}, {{200, 1, 12, 1}}});
  Append(2, {{{100, 1, 21, 1}}, {{200, 1, 22, 1}}});
  Append(1, {{{100, 1, 31, 2}}});
  STimeWindow range = {};
  range.skey = TSKEY_MIN;
  range.ekey = TSKEY_MAX;
  ASSERT_EQ(stNewVtableMergerSetData(merger_, 42, 0, -1, &range, &tables_, refs_, metas_, slices_, nullptr, false), 0);
  SSDataBlock *block = nullptr;
  int32_t      start = 0, end = 0;
  ASSERT_EQ(stNewVtableMergerNextDataBlock(merger_, &block, &start, &end), 0);
  ASSERT_NE(block, nullptr);
  ASSERT_EQ(end - start, 2);
  const int64_t expected[2][3] = {{100, 31, 21}, {200, 12, 22}};
  for (int32_t row = 0; row < 2; ++row) {
    for (int32_t col = 0; col < 3; ++col) {
      auto *data = static_cast<SColumnInfoData *>(taosArrayGet(block->pDataBlock, col));
      ASSERT_FALSE(colDataIsNull_s(data, row));
      EXPECT_EQ(*reinterpret_cast<int64_t *>(colDataGetData(data, row)), expected[row][col]);
    }
  }
  ASSERT_EQ(stNewVtableMergerNextDataBlock(merger_, &block, &start, &end), 0);
  EXPECT_EQ(block, nullptr);
}

TEST_F(StreamPeriodSnapshotVtableTest, BindingGenerationsMergeWithIndependentLayoutsAndLatestColumns) {
  Append(1, {{{100, 1, 11, 1}}, {{200, 1, 12, 1}}});
  Append(2, {{{100, 1, 21, 1}}, {{200, 1, 22, 1}}});
  Append(3, {{{100, 1, 31, 2}}, {{200, 1, 32, 2}}});
  Append(3, {{{100, 1, 33, 3}}});
  Append(4, {{{100, 1, 41, 2}}, {{200, 1, 42, 2}}});
  SArray *sources = taosArrayInit(4, sizeof(SSTriggerVtableSource));
  ASSERT_NE(sources, nullptr);
  // New generation first deliberately: equal timestamps must merge by generation,
  // not descriptor order. Both layouts refer to the same physical Reader identity.
  for (int64_t id : {3, 4, 1, 2}) {
    SSTriggerVtableSource source = {};
    source.readerKey = stTriggerReaderDataKey(STRIGGER_READER_KIND_INTERNAL, 7, 17);
    source.pColRef = static_cast<SSTriggerTableColRef *>(taosArrayGet(refs_, (id - 1) % 2));
    source.generation = id > 2 ? 1 : 0;
    source.slice.pSnapshotCache = cache_;
    source.slice.snapshotSourceId = id;
    ASSERT_NE(taosArrayPush(sources, &source), nullptr);
  }
  STimeWindow range = {TSKEY_MIN, TSKEY_MAX};
  int32_t     code = stNewVtableMergerSetSources(merger_, 42, 0, -1, &range, sources, false);
  taosArrayDestroy(sources);
  ASSERT_EQ(code, 0);
  SSDataBlock *block = nullptr;
  int32_t      start = 0, end = 0;
  ASSERT_EQ(stNewVtableMergerNextDataBlock(merger_, &block, &start, &end), 0);
  ASSERT_NE(block, nullptr);
  ASSERT_EQ(end - start, 2);
  const int64_t expected[2][3] = {{100, 33, 41}, {200, 32, 42}};
  for (int32_t row = 0; row < 2; ++row) {
    for (int32_t col = 0; col < 3; ++col) {
      auto *data = static_cast<SColumnInfoData *>(taosArrayGet(block->pDataBlock, col));
      ASSERT_FALSE(colDataIsNull_s(data, row));
      EXPECT_EQ(*reinterpret_cast<int64_t *>(colDataGetData(data, row)), expected[row][col]);
    }
  }
  ASSERT_EQ(stNewVtableMergerNextDataBlock(merger_, &block, &start, &end), 0);
  EXPECT_EQ(block, nullptr);
}

namespace {
Stub   *periodSortWriteStub = nullptr;
int64_t periodSortWrites = 0;
int64_t countPeriodSortWrite(TdFilePtr file, const void *buffer, int64_t size) {
  ++periodSortWrites;
  periodSortWriteStub->reset(taosWriteFile);
  int64_t result = taosWriteFile(file, buffer, size);
  periodSortWriteStub->set(taosWriteFile, countPeriodSortWrite);
  return result;
}
int64_t countPeriodSnapshotWrite(TdFilePtr file, const void *buffer, int64_t size, int64_t offset) {
  ++periodSortWrites;
  periodSortWriteStub->reset(taosPWriteFile);
  int64_t result = taosPWriteFile(file, buffer, size, offset);
  periodSortWriteStub->set(taosPWriteFile, countPeriodSnapshotWrite);
  return result;
}
int32_t failPeriodSnapshotRead(void **, SSDataBlock **) { return TSDB_CODE_INVALID_MSG; }
}  // namespace

TEST_F(StreamPeriodSnapshotVtableTest, GenerationsSpillMergeNullAtBlockBoundaryAndCanBeReadAgain) {
  setDataSinkMaxMemSize(DS_MEM_SIZE_RESERVED + 1);
  std::vector<std::array<int64_t, 4>> oldRows, newRows, otherColumn;
  for (int64_t ts = 0; ts < 5000; ++ts) {
    oldRows.push_back({ts, 1, 10000 + ts, 1});
    newRows.push_back({ts, 1, 20000 + ts, 2});
    otherColumn.push_back({ts, 1, 30000 + ts, 1});
  }
  Stub writes;
  periodSortWriteStub = &writes;
  periodSortWrites = 0;
  writes.set(taosPWriteFile, countPeriodSnapshotWrite);
  Append(1, oldRows);
  Append(2, newRows, 4095);
  Append(3, otherColumn);
  writes.reset(taosPWriteFile);
  periodSortWriteStub = nullptr;
  ASSERT_GT(periodSortWrites, 0);
  SArray *sources = taosArrayInit(3, sizeof(SSTriggerVtableSource));
  ASSERT_NE(sources, nullptr);
  for (int64_t id : {2, 1, 3}) {
    SSTriggerVtableSource source = {};
    source.pColRef = static_cast<SSTriggerTableColRef *>(taosArrayGet(refs_, id == 3 ? 1 : 0));
    source.readerKey = source.pColRef->readerKey;
    source.generation = id == 2 ? 1 : 0;
    source.slice.pSnapshotCache = cache_;
    source.slice.snapshotSourceId = id;
    ASSERT_NE(taosArrayPush(sources, &source), nullptr);
  }
  STimeWindow range = {TSKEY_MIN, TSKEY_MAX};
  for (int pass = 0; pass < 2; ++pass) {
    EXPECT_EQ(stNewVtableMergerSetSources(merger_, 42, 0, -1, &range, sources, false), 0);
    int64_t count = 0;
    for (;;) {
      SSDataBlock *block = nullptr;
      int32_t      start = 0, end = 0;
      int32_t      code = stNewVtableMergerNextDataBlock(merger_, &block, &start, &end);
      EXPECT_EQ(code, 0);
      if (code != 0 || block == nullptr) break;
      EXPECT_LE(end - start, 4096);
      auto *tsCol = static_cast<SColumnInfoData *>(taosArrayGet(block->pDataBlock, 0));
      auto *first = static_cast<SColumnInfoData *>(taosArrayGet(block->pDataBlock, 1));
      auto *second = static_cast<SColumnInfoData *>(taosArrayGet(block->pDataBlock, 2));
      for (int32_t row = start; row < end; ++row, ++count) {
        EXPECT_EQ(*reinterpret_cast<int64_t *>(colDataGetData(tsCol, row)), count);
        EXPECT_EQ(colDataIsNull_s(first, row), count == 4095);
        if (count != 4095 && !colDataIsNull_s(first, row))
          EXPECT_EQ(*reinterpret_cast<int64_t *>(colDataGetData(first, row)), 20000 + count);
        EXPECT_EQ(*reinterpret_cast<int64_t *>(colDataGetData(second, row)), 30000 + count);
      }
    }
    EXPECT_EQ(count, 5000);
    stNewVtableMergerReset(merger_);
  }
  taosArrayDestroy(sources);
}

TEST_F(StreamPeriodSnapshotSorterTest, SortSpillsAndReturnsAllRowsInOrder) {
  std::vector<std::array<int64_t, 4>> rows;
  for (int64_t row = 1; row <= 200000; ++row) rows.push_back({{row, 1, row, 1}});
  Append(1, rows);
  Append(1, {{{4096, 1, 9000, 2}}});
  periodSortWrites = 0;
  {
    Stub stub;
    periodSortWriteStub = &stub;
    stub.set(taosWriteFile, countPeriodSortWrite);
    ASSERT_EQ(Bind(false), 0);
    periodSortWriteStub = nullptr;
  }
  EXPECT_GT(periodSortWrites, 0);
  auto values = Values();
  ASSERT_EQ(values.size(), 200000U);
  EXPECT_EQ(values[4095], 9000);
  EXPECT_EQ(values.front(), 1);
  EXPECT_EQ(values.back(), 200000);
}

TEST_F(StreamPeriodSnapshotSorterTest, ReadFailureReleasesIteratorWithoutConsumingSnapshot) {
  Append(1, {{{100, 1, 11, 1}}});
  {
    Stub stub;
    stub.set(getNextStreamDataCache, failPeriodSnapshotRead);
    EXPECT_EQ(Bind(false), TSDB_CODE_INVALID_MSG);
  }
  stNewTimestampSorterReset(sorter_);
  ASSERT_EQ(Bind(false), 0);
  EXPECT_EQ(Values(), (std::vector<int64_t>{11}));
}
