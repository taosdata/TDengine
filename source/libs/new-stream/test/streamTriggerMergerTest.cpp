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
#include <cstdint>
#include <initializer_list>
#include <tuple>
#include <vector>

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
