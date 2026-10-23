// Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This program is free software: you can use, redistribute, and/or modify
// it under the terms of the GNU Affero General Public License, version 3
// or later ("AGPL"), as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

#include <gtest/gtest.h>

#include <algorithm>
#include <csetjmp>
#include <cstdint>
#include <cstring>
#include <set>
#include <vector>

#include "executorInt.h"
#include "functionMgt.h"
#include "nodes.h"
#include "operator.h"
#include "querytask.h"
#include "stub.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "tpagedbuf.h"

namespace {

constexpr int16_t kInputBlock = 101;
constexpr int16_t kOutputBlock = 201;

struct Value {
  int64_t number;
  bool isNull;
};

struct Source {
  std::vector<SSDataBlock*> blocks;
  size_t next = 0;
};

int32_t sourceNext(SOperatorInfo* op, SSDataBlock** result) {
  Source* source = static_cast<Source*>(op->info);
  *result = source->next == source->blocks.size()
                ? nullptr : source->blocks[source->next++];
  return TSDB_CODE_SUCCESS;
}

void destroySource(void* info) {
  Source* source = static_cast<Source*>(info);
  for (SSDataBlock* block : source->blocks) {
    blockDataDestroy(block);
  }
  delete source;
}

SColumnNode* makeColumn(int16_t block, int16_t slot) {
  SColumnNode* column = nullptr;
  if (nodesMakeNode(QUERY_NODE_COLUMN,
                    reinterpret_cast<SNode**>(&column)) != 0) {
    return nullptr;
  }
  column->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  column->node.resType.bytes = sizeof(int64_t);
  column->dataBlockId = block;
  column->slotId = slot;
  column->colId = slot + 1;
  column->colType = COLUMN_TYPE_COLUMN;
  return column;
}

SValueNode* makeInteger(int64_t value) {
  SValueNode* node = nullptr;
  if (nodesMakeNode(QUERY_NODE_VALUE,
                    reinterpret_cast<SNode**>(&node)) != 0) {
    return nullptr;
  }
  node->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  node->node.resType.bytes = sizeof(value);
  node->translate = true;
  if (nodesSetValueNodeValue(node, &value) != 0) {
    nodesDestroyNode(reinterpret_cast<SNode*>(node));
    return nullptr;
  }
  return node;
}

enum class LimitKind { kNone, kRows, kGroups };

class AggregateHarness {
 public:
  AggregateHarness() {
    task_.id.str = const_cast<char*>("aggregate-batch-test");
  }

  ~AggregateHarness() {
    Close();
    nodesDestroyNode(reinterpret_cast<SNode*>(plan_));
  }

  int32_t Create(bool merge = true, bool positiveFilter = false,
                 const char* name = "sum", bool distinct = false,
                 LimitKind limit = LimitKind::kNone) {
    int32_t code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_HASH_AGG,
                                 reinterpret_cast<SNode**>(&plan_));
    if (code != 0) return code;
    plan_->node.forceCreateNonBlockingOptr = true;
    plan_->mergeDataBlock = merge;
    plan_->node.inputTsOrder = ORDER_ASC;
    if (limit != LimitKind::kNone) {
      code = Limit(limit == LimitKind::kGroups);
      if (code != 0) return code;
    }

    SFunctionNode* function = nullptr;
    code = nodesMakeNode(QUERY_NODE_FUNCTION,
                         reinterpret_cast<SNode**>(&function));
    if (code != 0) return code;
    tstrncpy(function->functionName, name, TSDB_FUNC_NAME_LEN);
    function->isDistinct = distinct;
    code = nodesListMakeStrictAppend(&function->pParameterList,
        reinterpret_cast<SNode*>(makeColumn(kInputBlock, 1)));
    if (code == 0 && strcmp(name, "top") == 0) {
      code = nodesListMakeStrictAppend(&function->pParameterList,
          reinterpret_cast<SNode*>(makeInteger(3)));
    }
    if (code == 0) code = fmGetFuncInfo(function, nullptr, 0);
    if (code != 0) {
      nodesDestroyNode(reinterpret_cast<SNode*>(function));
      return code;
    }

    STargetNode* target = nullptr;
    code = nodesMakeNode(QUERY_NODE_TARGET,
                         reinterpret_cast<SNode**>(&target));
    if (code != 0) {
      nodesDestroyNode(reinterpret_cast<SNode*>(function));
      return code;
    }
    target->dataBlockId = kOutputBlock;
    target->slotId = 0;
    target->pExpr = reinterpret_cast<SNode*>(function);
    code = nodesListMakeStrictAppend(&plan_->pAggFuncs,
                                     reinterpret_cast<SNode*>(target));
    if (code != 0) return code;
    code = MakeOutput(function->node.resType);
    if (code != 0) return code;
    if (positiveFilter) {
      SOperatorNode* condition = nullptr;
      code = nodesMakeNode(QUERY_NODE_OPERATOR,
                           reinterpret_cast<SNode**>(&condition));
      if (code != 0) return code;
      condition->node.resType.type = TSDB_DATA_TYPE_BOOL;
      condition->node.resType.bytes = sizeof(bool);
      condition->opType = OP_TYPE_GREATER_THAN;
      condition->pLeft = reinterpret_cast<SNode*>(
          makeColumn(kOutputBlock, 0));
      condition->pRight = reinterpret_cast<SNode*>(makeInteger(0));
      plan_->node.pConditions = reinterpret_cast<SNode*>(condition);
    }

    SOperatorInfo* source = static_cast<SOperatorInfo*>(
        taosMemoryCalloc(1, sizeof(SOperatorInfo)));
    if (source == nullptr) return terrno;
    source_ = new Source;
    setOperatorInfo(source, "AggregateBatchInput", sourceType_, false,
                    OP_NOT_OPENED, source_, &task_);
    source->resultDataBlockId = kInputBlock;
    source->fpSet = createOperatorFpSet(optrDummyOpenFn, sourceNext,
        nullptr, destroySource, optrDefaultBufFn, nullptr,
        optrDefaultGetNextExtFn, nullptr);
    initOperatorCostInfo(source);
    SOperatorInfo* inputs[] = {source};
    code = createAggregateOperatorInfo(inputs, 1, plan_, &task_, &agg_);
    if (code == 0) task_.pRoot = agg_;
    return code;
  }

  int32_t Add(uint64_t group, const std::vector<Value>& values) {
    SSDataBlock* block = nullptr;
    int32_t code = createDataBlock(&block);
    if (code != 0) return code;
    for (int16_t slot = 0; slot < 2; ++slot) {
      SColumnInfoData column = createColumnInfoData(
          TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), slot + 1);
      code = blockDataAppendColInfo(block, &column);
      if (code != 0) {
        blockDataDestroy(block);
        return code;
      }
    }
    code = blockDataEnsureCapacity(block,
                                   static_cast<int32_t>(values.size()));
    if (code != 0) {
      blockDataDestroy(block);
      return code;
    }
    for (size_t row = 0; row < values.size(); ++row) {
      int64_t ts = static_cast<int64_t>(row);
      for (int16_t slot = 0; slot < 2; ++slot) {
        SColumnInfoData* column = static_cast<SColumnInfoData*>(
            taosArrayGet(block->pDataBlock, slot));
        const int64_t* value = slot == 0 ? &ts : &values[row].number;
        code = colDataSetVal(column, static_cast<int32_t>(row),
                             reinterpret_cast<const char*>(value),
                             slot == 1 && values[row].isNull);
        if (code != 0) {
          blockDataDestroy(block);
          return code;
        }
      }
    }
    block->info.rows = static_cast<int64_t>(values.size());
    block->info.id.blockId = kInputBlock;
    block->info.id.groupId = group;
    block->info.id.uid = group;
    block->info.dataLoad = 1;
    source_->blocks.push_back(block);
    return TSDB_CODE_SUCCESS;
  }

  int32_t Fetch(SSDataBlock** result) {
    int32_t code = setjmp(task_.env);
    if (code != 0) {
      *result = nullptr;
      return code;
    }
    return agg_->fpSet.getNextFn(agg_, result);
  }

  void SetCapacity(int32_t capacity, int32_t threshold) {
    agg_->resultInfo.capacity = capacity;
    agg_->resultInfo.threshold = threshold;
  }

  int32_t Limit(bool groups) {
    SLimitNode* limit = nullptr;
    int32_t code = nodesMakeNode(QUERY_NODE_LIMIT,
                                 reinterpret_cast<SNode**>(&limit));
    if (code != 0) return code;
    limit->limit = makeInteger(1);
    if (groups) {
      plan_->node.pSlimit = reinterpret_cast<SNode*>(limit);
    } else {
      plan_->node.pLimit = reinterpret_cast<SNode*>(limit);
    }
    return limit->limit == nullptr ? terrno : TSDB_CODE_SUCCESS;
  }

  int32_t Reset() {
    int32_t code = agg_->fpSet.resetStateFn(agg_);
    if (code == 0) {
      source_->next = 0;
      task_.code = TSDB_CODE_SUCCESS;
    }
    return code;
  }

  size_t pulled() const { return source_->next; }

  void Close() {
    destroyOperator(agg_);
    agg_ = nullptr;
  }

  SOperatorInfo* op() { return agg_; }

  // Call before Create().
  void SetSourceType(uint16_t type) { sourceType_ = type; }

 private:
  int32_t MakeOutput(const SDataType& type) {
    SDataBlockDescNode* desc = nullptr;
    int32_t code = nodesMakeNode(QUERY_NODE_DATABLOCK_DESC,
                                 reinterpret_cast<SNode**>(&desc));
    if (code != 0) return code;
    plan_->node.pOutputDataBlockDesc = desc;
    desc->dataBlockId = kOutputBlock;
    desc->totalRowSize = type.bytes;
    desc->outputRowSize = type.bytes;
    SSlotDescNode* slot = nullptr;
    code = nodesMakeNode(QUERY_NODE_SLOT_DESC,
                         reinterpret_cast<SNode**>(&slot));
    if (code != 0) return code;
    slot->slotId = 0;
    slot->dataType = type;
    slot->output = true;
    return nodesListMakeStrictAppend(&desc->pSlots,
                                     reinterpret_cast<SNode*>(slot));
  }

  SExecTaskInfo task_ = {};
  SAggPhysiNode* plan_ = nullptr;
  SOperatorInfo* agg_ = nullptr;
  Source* source_ = nullptr;
  uint16_t sourceType_ = QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN;
};

std::vector<int64_t> numbers(const SSDataBlock* block) {
  std::vector<int64_t> values;
  SColumnInfoData* column = static_cast<SColumnInfoData*>(
      taosArrayGet(block->pDataBlock, 0));
  for (int32_t row = 0; row < block->info.rows; ++row) {
    EXPECT_FALSE(colDataIsNull_s(column, row));
    values.push_back(*reinterpret_cast<int64_t*>(
        colDataGetData(column, row)));
  }
  return values;
}

// Fetches one block per expected batch, then expects end of output.
void ExpectBatches(AggregateHarness& harness,
                   const std::vector<std::vector<int64_t>>& batches,
                   bool sorted = false) {
  SSDataBlock* block = nullptr;
  for (const auto& expected : batches) {
    ASSERT_EQ(harness.Fetch(&block), 0);
    ASSERT_NE(block, nullptr);
    std::vector<int64_t> actual = numbers(block);
    if (sorted) std::sort(actual.begin(), actual.end());
    EXPECT_EQ(actual, expected);
  }
  ASSERT_EQ(harness.Fetch(&block), 0);
  EXPECT_EQ(block, nullptr);
}

class AggregateBatchTest : public testing::Test {
 protected:
  void SetUp() override {
    ASSERT_EQ(fmFuncMgtInit(), TSDB_CODE_SUCCESS);
    savedSpace_ = tsTempSpace;
    tsTempSpace.size.avail = 1024 * 1024;
  }

  void TearDown() override { tsTempSpace = savedSpace_; }

 private:
  SDiskSpace savedSpace_ = {};
};

TEST_F(AggregateBatchTest, MergesGroupsAndFlushesTail) {
  AggregateHarness harness;
  ASSERT_EQ(harness.Create(), 0);
  harness.SetCapacity(4, 4);
  for (int64_t group = 1; group <= 6; ++group) {
    ASSERT_EQ(harness.Add(group, {{group, false}}), 0);
  }
  SSDataBlock* block = nullptr;
  ASSERT_EQ(harness.Fetch(&block), 0);
  ASSERT_NE(block, nullptr);
  EXPECT_EQ(numbers(block), (std::vector<int64_t>{1, 2, 3, 4}));
  EXPECT_EQ(block->info.id.groupId, 0U);
  ExpectBatches(harness, {{5, 6}});
}

TEST_F(AggregateBatchTest, FiltersEachGroupBeforeMerging) {
  AggregateHarness harness;
  ASSERT_EQ(harness.Create(true, true), 0);
  harness.SetCapacity(3, 3);
  const int64_t values[] = {-1, 1, -2, 2, 3, -3, 4, -4};
  for (int64_t group = 1; group <= 8; ++group) {
    ASSERT_EQ(harness.Add(group, {{values[group - 1], false}}), 0);
  }
  ExpectBatches(harness, {{1, 2, 3}, {4}});
}

TEST_F(AggregateBatchTest, DefaultCapacityBatchesThousandsOfGroups) {
  AggregateHarness harness;
  ASSERT_EQ(harness.Create(), 0);
  for (int64_t group = 1; group <= 4097; ++group) {
    ASSERT_EQ(harness.Add(group, {{group, false}}), 0);
  }
  const int32_t threshold = harness.op()->resultInfo.threshold;
  std::vector<int64_t> actual;
  int32_t batches = 0;
  for (int32_t attempt = 0; attempt < 4098; ++attempt) {
    SSDataBlock* block = nullptr;
    ASSERT_EQ(harness.Fetch(&block), 0);
    if (block == nullptr) break;
    if (batches++ == 0) EXPECT_EQ(block->info.rows, threshold);
    std::vector<int64_t> batch = numbers(block);
    actual.insert(actual.end(), batch.begin(), batch.end());
  }
  ASSERT_EQ(actual.size(), 4097U);
  EXPECT_EQ(batches, 2);
  for (size_t row = 0; row < actual.size(); ++row) {
    EXPECT_EQ(actual[row], static_cast<int64_t>(row + 1));
  }
}

TEST_F(AggregateBatchTest, LimitKeepsSingleGroupAndAvoidsExtraReads) {
  for (bool groupLimit : {false, true}) {
    AggregateHarness harness;
    ASSERT_EQ(harness.Create(true, false, "sum", false,
                             groupLimit ? LimitKind::kGroups
                                        : LimitKind::kRows), 0);
    ASSERT_EQ(harness.Add(1, {{1, false}}), 0);
    ASSERT_EQ(harness.Add(2, {{2, false}}), 0);
    ASSERT_EQ(harness.Add(3, {{3, false}}), 0);
    SSDataBlock* block = nullptr;
    ASSERT_EQ(harness.Fetch(&block), 0);
    ASSERT_NE(block, nullptr);
    EXPECT_EQ(numbers(block), (std::vector<int64_t>{1}));
    EXPECT_EQ(harness.pulled(), 2U);
  }
}

TEST_F(AggregateBatchTest, AllFilteredGroupsFinishWithoutAnEmptyBlock) {
  AggregateHarness harness;
  ASSERT_EQ(harness.Create(true, true), 0);
  ASSERT_EQ(harness.Add(1, {{-1, false}}), 0);
  ASSERT_EQ(harness.Add(2, {{-2, false}}), 0);
  ExpectBatches(harness, {});
  ExpectBatches(harness, {});
}

TEST_F(AggregateBatchTest, NonCacheInputKeepsOneGroupPerBlock) {
  // E.g. a window join below the aggregate: groups are merged (group id
  // cleared) but never batched across groups.
  AggregateHarness harness;
  harness.SetSourceType(QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN);
  ASSERT_EQ(harness.Create(), 0);
  for (int64_t group = 1; group <= 3; ++group) {
    ASSERT_EQ(harness.Add(group, {{group, false}}), 0);
  }
  for (int64_t group = 1; group <= 3; ++group) {
    SSDataBlock* block = nullptr;
    ASSERT_EQ(harness.Fetch(&block), 0);
    ASSERT_NE(block, nullptr);
    EXPECT_EQ(block->info.id.groupId, 0U);
    EXPECT_EQ(numbers(block), (std::vector<int64_t>{group}));
  }
  ExpectBatches(harness, {});
}

TEST_F(AggregateBatchTest, UnmergedOutputPreservesGroupBoundaries) {
  AggregateHarness harness;
  ASSERT_EQ(harness.Create(false), 0);
  ASSERT_EQ(harness.Add(11, {{2, false}, {3, false}}), 0);
  ASSERT_EQ(harness.Add(11, {{5, false}}), 0);
  ASSERT_EQ(harness.Add(22, {{7, false}}), 0);
  SSDataBlock* block = nullptr;
  ASSERT_EQ(harness.Fetch(&block), 0);
  ASSERT_NE(block, nullptr);
  EXPECT_EQ(block->info.id.groupId, 11U);
  EXPECT_EQ(numbers(block), (std::vector<int64_t>{10}));
  ASSERT_EQ(harness.Fetch(&block), 0);
  ASSERT_NE(block, nullptr);
  EXPECT_EQ(block->info.id.groupId, 22U);
  EXPECT_EQ(numbers(block), (std::vector<int64_t>{7}));
  ASSERT_EQ(harness.Fetch(&block), 0);
  EXPECT_EQ(block, nullptr);
}

TEST_F(AggregateBatchTest, MultirowSelectionCrossesBatchThreshold) {
  AggregateHarness harness;
  ASSERT_EQ(harness.Create(true, false, "top"), 0);
  harness.SetCapacity(4, 4);
  ASSERT_EQ(harness.Add(1, {{1, false}, {3, false}, {2, false}}), 0);
  ASSERT_EQ(harness.Add(2, {{11, false}, {13, false}, {12, false}}), 0);
  ASSERT_EQ(harness.Add(3, {{21, false}, {23, false}, {22, false}}), 0);
  ExpectBatches(harness, {{1, 2, 3, 11, 12, 13}, {21, 22, 23}}, true);
}

int mergeLimitHits = 0;

bool rejectNonemptyMerge(const SSDataBlock* dest, const SSDataBlock*) {
  if (dest->info.rows > 0) {
    ++mergeLimitHits;
    return true;
  }
  return false;
}

TEST_F(AggregateBatchTest, ByteLimitRetainsFinalGroupAfterEndOfInput) {
  AggregateHarness harness;
  ASSERT_EQ(harness.Create(), 0);
  ASSERT_EQ(harness.Add(1, {{11, false}}), 0);
  ASSERT_EQ(harness.Add(2, {{22, false}}), 0);
  mergeLimitHits = 0;
  Stub stub;
  stub.set(blockDataMergeExceedLimit, rejectNonemptyMerge);
  ExpectBatches(harness, {{11}, {22}});
  EXPECT_EQ(mergeLimitHits, 1);
}

TEST_F(AggregateBatchTest, ResetDiscardsPendingFinalGroup) {
  AggregateHarness harness;
  ASSERT_EQ(harness.Create(), 0);
  ASSERT_EQ(harness.Add(1, {{11, false}}), 0);
  ASSERT_EQ(harness.Add(2, {{22, false}}), 0);
  Stub stub;
  stub.set(blockDataMergeExceedLimit, rejectNonemptyMerge);
  SSDataBlock* block = nullptr;
  ASSERT_EQ(harness.Fetch(&block), 0);
  ASSERT_NE(block, nullptr);
  EXPECT_EQ(numbers(block), (std::vector<int64_t>{11}));
  ASSERT_EQ(harness.Reset(), 0);
  ExpectBatches(harness, {{11}, {22}});
}

class BlockLifetimes {
 public:
  BlockLifetimes() {
    active_ = this;
    stub_.set(createDataBlock, Create);
    stub_.set(createOneDataBlock, Clone);
    stub_.set(blockDataDestroy, Destroy);
  }

  ~BlockLifetimes() {
    stub_.reset(createDataBlock);
    stub_.reset(createOneDataBlock);
    stub_.reset(blockDataDestroy);
    active_ = nullptr;
  }

  std::set<SSDataBlock*> live;

 private:
  static int32_t Create(SSDataBlock** result) {
    active_->stub_.reset(createDataBlock);
    int32_t code = createDataBlock(result);
    active_->stub_.set(createDataBlock, Create);
    if (code == 0) active_->live.insert(*result);
    return code;
  }

  static int32_t Clone(const SSDataBlock* input, bool copy,
                       SSDataBlock** result) {
    active_->stub_.reset(createOneDataBlock);
    int32_t code = createOneDataBlock(input, copy, result);
    active_->stub_.set(createOneDataBlock, Clone);
    if (code == 0) active_->live.insert(*result);
    return code;
  }

  static void Destroy(SSDataBlock* block) {
    active_->live.erase(block);
    active_->stub_.reset(blockDataDestroy);
    blockDataDestroy(block);
    active_->stub_.set(blockDataDestroy, Destroy);
  }

  static BlockLifetimes* active_;
  Stub stub_;
};

BlockLifetimes* BlockLifetimes::active_ = nullptr;

TEST_F(AggregateBatchTest, EarlyCloseDestroysPendingAndMergedBlocks) {
  BlockLifetimes lifetimes;
  AggregateHarness harness;
  ASSERT_EQ(harness.Create(), 0);
  ASSERT_EQ(harness.Add(1, {{11, false}}), 0);
  ASSERT_EQ(harness.Add(2, {{22, false}}), 0);
  Stub stub;
  stub.set(blockDataMergeExceedLimit, rejectNonemptyMerge);
  SSDataBlock* block = nullptr;
  ASSERT_EQ(harness.Fetch(&block), 0);
  ASSERT_NE(block, nullptr);
  EXPECT_EQ(numbers(block), (std::vector<int64_t>{11}));
  EXPECT_FALSE(lifetimes.live.empty());
  harness.Close();
  EXPECT_TRUE(lifetimes.live.empty());
}

class PageFailure {
 public:
  PageFailure() {
    active_ = this;
    stub_.set(getNewBufPage, Allocate);
  }

  ~PageFailure() {
    stub_.reset(getNewBufPage);
    active_ = nullptr;
  }

  int calls = 0;

 private:
  static void* Allocate(SDiskbasedBuf* buffer, int32_t* pageId) {
    if (++active_->calls == 2) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return nullptr;
    }
    active_->stub_.reset(getNewBufPage);
    void* page = getNewBufPage(buffer, pageId);
    active_->stub_.set(getNewBufPage, Allocate);
    return page;
  }

  static PageFailure* active_;
  Stub stub_;
};

PageFailure* PageFailure::active_ = nullptr;

TEST_F(AggregateBatchTest, PageFailureAfterRecyclingClearsFunctionContext) {
  AggregateHarness harness;
  ASSERT_EQ(harness.Create(true, false, "sum", true), 0);
  ASSERT_EQ(harness.Add(1, {{0, true}}), 0);
  ASSERT_EQ(harness.Add(2, {{7, false}}), 0);
  PageFailure failure;
  SSDataBlock* block = nullptr;
  int32_t code = 0;
  for (int32_t attempt = 0; attempt < 3 && code == 0; ++attempt) {
    code = harness.Fetch(&block);
  }
  EXPECT_EQ(code, TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_GE(failure.calls, 2);
  ASSERT_NE(harness.op()->exprSupp.pCtx, nullptr);
  SqlFunctionCtx* ctx = &harness.op()->exprSupp.pCtx[0];
  EXPECT_EQ(ctx->resultInfo, nullptr);
  EXPECT_EQ(ctx->saveHandle.currentPage, -1);
  // Avoid dereferencing a stale pointer on the failing baseline.
  ctx->resultInfo = nullptr;
  harness.Close();
}

}  // namespace
