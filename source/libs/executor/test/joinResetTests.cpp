// Copyright (c) 2026 TAOS Data, Inc.

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#endif

#include <gtest/gtest.h>

#include <algorithm>
#include <csetjmp>
#include <cstdlib>
#include <string>
#include <vector>

#include "executor.h"
#include "executorInt.h"
#include "operator.h"
#include "querytask.h"
#include "tdatablock.h"

namespace {

struct Row {
  int64_t ts;
  int64_t value;
  bool nullValue = false;
  Row(int64_t time, int64_t val, bool isNull = false)
      : ts(time), value(val), nullValue(isNull) {}
};

using Rows = std::vector<Row>;
using Result = std::vector<std::string>;

struct Source {
  std::vector<SSDataBlock*> blocks;
  size_t next = 0;
  int failAt = -1;
  int resets = 0;

  ~Source() {
    for (auto* block : blocks) blockDataDestroy(block);
  }
};

int32_t sourceNext(SOperatorInfo* op, SSDataBlock** result) {
  auto* source = static_cast<Source*>(op->info);
  *result = nullptr;
  if (source->failAt == static_cast<int>(source->next)) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  if (source->next == source->blocks.size()) {
    setOperatorCompleted(op);
  } else {
    *result = source->blocks[source->next++];
  }
  return TSDB_CODE_SUCCESS;
}

int32_t sourceReset(SOperatorInfo* op) {
  auto* source = static_cast<Source*>(op->info);
  source->next = 0;
  ++source->resets;
  return TSDB_CODE_SUCCESS;
}

void sourceDestroy(void* info) { delete static_cast<Source*>(info); }

template <typename T>
T* makeNode(ENodeType type) {
  SNode* node = nullptr;
  EXPECT_EQ(nodesMakeNode(type, &node), TSDB_CODE_SUCCESS);
  return reinterpret_cast<T*>(node);
}

SColumnNode* column(int block, int slot) {
  auto* col = makeNode<SColumnNode>(QUERY_NODE_COLUMN);
  col->dataBlockId = block;
  col->slotId = slot;
  col->node.resType.type = slot % 2 == 0 ? TSDB_DATA_TYPE_TIMESTAMP :
                                        TSDB_DATA_TYPE_BIGINT;
  col->node.resType.bytes = sizeof(int64_t);
  return col;
}

void setRows(SOperatorInfo* op, const Rows& rows, size_t blockRows = 2) {
  auto* source = static_cast<Source*>(op->info);
  for (auto* block : source->blocks) blockDataDestroy(block);
  source->blocks.clear();
  source->next = 0;
  for (size_t start = 0; start < rows.size(); start += blockRows) {
    SSDataBlock* block = nullptr;
    ASSERT_EQ(createDataBlock(&block), TSDB_CODE_SUCCESS);
    block->info.id.blockId = op->resultDataBlockId;
    block->info.calWin = {INT64_MIN, INT64_MAX};
    for (int slot = 0; slot < 2; ++slot) {
      auto col = createColumnInfoData(slot == 0 ? TSDB_DATA_TYPE_TIMESTAMP :
                                                 TSDB_DATA_TYPE_BIGINT,
                                      sizeof(int64_t), slot);
      ASSERT_EQ(blockDataAppendColInfo(block, &col), TSDB_CODE_SUCCESS);
    }
    ASSERT_EQ(blockDataEnsureCapacity(block, blockRows), TSDB_CODE_SUCCESS);
    size_t end = std::min(rows.size(), start + blockRows);
    for (size_t i = start; i < end; ++i) {
      const Row& row = rows[i];
      for (int slot = 0; slot < 2; ++slot) {
        auto* col = static_cast<SColumnInfoData*>(
            taosArrayGet(block->pDataBlock, slot));
        const int64_t* value = slot == 0 ? &row.ts : &row.value;
        ASSERT_EQ(colDataSetVal(col, i - start,
                                reinterpret_cast<const char*>(value),
                                slot == 1 && row.nullValue),
                  TSDB_CODE_SUCCESS);
      }
      ++block->info.rows;
    }
    source->blocks.push_back(block);
  }
}

SOperatorInfo* makeSource(int id, SExecTaskInfo* task) {
  auto* op = static_cast<SOperatorInfo*>(
      taosMemoryCalloc(1, sizeof(SOperatorInfo)));
  setOperatorInfo(op, "ResetTestSource",
                  QUERY_NODE_PHYSICAL_PLAN_ROWSET_SOURCE, false,
                  OP_NOT_OPENED, new Source(), task);
  op->resultDataBlockId = id;
  op->fpSet = createOperatorFpSet(optrDummyOpenFn, sourceNext, nullptr,
                                 sourceDestroy, optrDefaultBufFn, nullptr,
                                 optrDefaultGetNextExtFn, nullptr);
  setOperatorResetStateFn(op, sourceReset);
  return op;
}

enum class Algorithm { Hash, Merge, Theta };

struct JoinSpec {
  Algorithm algorithm;
  EJoinType type;
  EJoinSubType subtype;
  EOrder order = ORDER_ASC;
  int limit = 1;
  EOperatorType comparison = OP_TYPE_EQUAL;
  JoinSpec(Algorithm algo, EJoinType joinType, EJoinSubType joinSubtype)
      : algorithm(algo), type(joinType), subtype(joinSubtype) {}
};

bool oneSide(const JoinSpec& spec) {
  return spec.subtype == JOIN_STYPE_SEMI ||
         spec.subtype == JOIN_STYPE_ANTI;
}

template <typename T>
void fillJoin(T* node, const JoinSpec& spec, SOperatorInfo** children,
              int id, bool conditionColumns = false) {
  node->joinType = spec.type;
  node->subType = spec.subtype;
  node->leftPrimSlotId = 0;
  node->rightPrimSlotId = 0;
  auto* desc = makeNode<SDataBlockDescNode>(QUERY_NODE_DATABLOCK_DESC);
  node->node.pOutputDataBlockDesc = desc;
  desc->dataBlockId = id;
  // SEMI/ANTI expose one side, but Theta evaluates ON over both sides.
  for (int i = 0; i < 2; ++i) {
    int side = conditionColumns && oneSide(spec) ?
        (i + (spec.type == JOIN_TYPE_RIGHT)) % 2 : i;
    bool visible = !oneSide(spec) || side == (spec.type == JOIN_TYPE_RIGHT);
    if (!visible && !conditionColumns) continue;
    for (int slot = 0; slot < 2; ++slot) {
      int dst = LIST_LENGTH(node->pTargets);
      auto* target = makeNode<STargetNode>(QUERY_NODE_TARGET);
      target->dataBlockId = id;
      target->slotId = dst;
      target->pExpr = reinterpret_cast<SNode*>(
          column(children[side]->resultDataBlockId, slot));
      EXPECT_EQ(nodesListMakeStrictAppend(&node->pTargets,
                                          reinterpret_cast<SNode*>(target)),
                TSDB_CODE_SUCCESS);
      auto* output = makeNode<SSlotDescNode>(QUERY_NODE_SLOT_DESC);
      output->slotId = dst;
      output->dataType = reinterpret_cast<SExprNode*>(target->pExpr)->resType;
      output->output = visible;
      EXPECT_EQ(nodesListMakeStrictAppend(&desc->pSlots,
                                          reinterpret_cast<SNode*>(output)),
                TSDB_CODE_SUCCESS);
      desc->totalRowSize += sizeof(int64_t);
      if (visible) desc->outputRowSize += sizeof(int64_t);
    }
  }
}

SValueNode* intValue(int64_t value) {
  auto* node = makeNode<SValueNode>(QUERY_NODE_VALUE);
  node->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  node->node.resType.bytes = sizeof(int64_t);
  node->datum.i = value;
  return node;
}

SOperatorInfo* makeJoin(const JoinSpec& spec, SOperatorInfo* left,
                        SOperatorInfo* right, int id, SExecTaskInfo* task) {
  SOperatorInfo* children[] = {left, right};
  SOperatorInfo* op = nullptr;
  SNode* physical = nullptr;
  int32_t code = 0;
  if (spec.algorithm == Algorithm::Hash) {
    auto* node = makeNode<SHashJoinPhysiNode>(
        QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN);
    node->timeRange = {INT64_MIN, INT64_MAX};
    fillJoin(node, spec, children, id);
    EXPECT_EQ(nodesListMakeStrictAppend(&node->pOnLeft,
        reinterpret_cast<SNode*>(column(left->resultDataBlockId, 0))), 0);
    EXPECT_EQ(nodesListMakeStrictAppend(&node->pOnRight,
        reinterpret_cast<SNode*>(column(right->resultDataBlockId, 0))), 0);
    physical = reinterpret_cast<SNode*>(node);
    code = createHashJoinOperatorInfo(children, 2, node, task, &op);
  } else if (spec.algorithm == Algorithm::Theta) {
    auto* node = makeNode<SThetaJoinPhysiNode>(
        QUERY_NODE_PHYSICAL_PLAN_THETA_JOIN);
    fillJoin(node, spec, children, id, true);
    // These fields carry child block ids in createThetaJoinPhysiNode().
    node->leftPrimSlotId = left->resultDataBlockId;
    node->rightPrimSlotId = right->resultDataBlockId;
    auto* condition = makeNode<SOperatorNode>(QUERY_NODE_OPERATOR);
    condition->opType = spec.comparison;
    condition->node.resType.type = TSDB_DATA_TYPE_BOOL;
    condition->node.resType.bytes = sizeof(bool);
    bool rightOnly = oneSide(spec) && spec.type == JOIN_TYPE_RIGHT;
    condition->pLeft = reinterpret_cast<SNode*>(
        column(id, rightOnly ? 2 : 0));
    condition->pRight = reinterpret_cast<SNode*>(
        column(id, rightOnly ? 0 : 2));
    node->pFullOnCond = reinterpret_cast<SNode*>(condition);
    physical = reinterpret_cast<SNode*>(node);
    code = createThetaJoinOperatorInfo(children, 2, node, task, &op);
  } else {
    auto* node = makeNode<SSortMergeJoinPhysiNode>(
        QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN);
    fillJoin(node, spec, children, id);
    node->node.inputTsOrder = spec.order;
    node->asofOpType = spec.comparison;
    if (spec.subtype == JOIN_STYPE_ASOF || spec.subtype == JOIN_STYPE_WIN) {
      auto* limit = makeNode<SLimitNode>(QUERY_NODE_LIMIT);
      limit->limit = intValue(spec.limit);
      node->pJLimit = reinterpret_cast<SNode*>(limit);
    }
    if (spec.subtype == JOIN_STYPE_WIN) {
      auto* offset = makeNode<SWindowOffsetNode>(QUERY_NODE_WINDOW_OFFSET);
      offset->pStartOffset = reinterpret_cast<SNode*>(intValue(-2));
      offset->pEndOffset = reinterpret_cast<SNode*>(intValue(1));
      node->pWindowOffset = reinterpret_cast<SNode*>(offset);
    }
    physical = reinterpret_cast<SNode*>(node);
    code = createMergeJoinOperatorInfo(children, 2, node, task, &op);
  }
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  if (op) {
    op->resultDataBlockId = id;
    op->pPhyNode = physical;
  } else {
    nodesDestroyNode(physical);
  }
  return op;
}

void collectPlans(SOperatorInfo* op, std::vector<SNode*>* plans) {
  if (op->pPhyNode) {
    plans->push_back(const_cast<SNode*>(
        static_cast<const SNode*>(op->pPhyNode)));
  }
  for (int i = 0; i < op->numOfDownstream; ++i) {
    collectPlans(op->pDownstream[i], plans);
  }
}

SOperatorInfo* filterTime(SOperatorInfo* child, int id, SExecTaskInfo* task) {
  SHashJoinPhysiNode metadata = {};
  SOperatorInfo* children[] = {child, child};
  JoinSpec projection(Algorithm::Hash, JOIN_TYPE_LEFT, JOIN_STYPE_SEMI);
  fillJoin(&metadata, projection, children, id);
  auto* node = makeNode<SProjectPhysiNode>(QUERY_NODE_PHYSICAL_PLAN_PROJECT);
  node->pProjections = metadata.pTargets;
  node->node.pOutputDataBlockDesc = metadata.node.pOutputDataBlockDesc;
  auto* condition = makeNode<SOperatorNode>(QUERY_NODE_OPERATOR);
  condition->opType = OP_TYPE_IS_NOT_NULL;
  condition->node.resType.type = TSDB_DATA_TYPE_BOOL;
  condition->node.resType.bytes = sizeof(bool);
  condition->pLeft = reinterpret_cast<SNode*>(column(id, 0));
  node->node.pConditions = reinterpret_cast<SNode*>(condition);
  SOperatorInfo* op = nullptr;
  EXPECT_EQ(createProjectOperatorInfo(child, node, task, &op),
            TSDB_CODE_SUCCESS);
  if (op) op->resultDataBlockId = id;
  return op;
}

std::string rowText(const Row* row) {
  if (!row) return "NULL|NULL";
  return std::to_string(row->ts) + "|" +
      (row->nullValue ? "NULL" : std::to_string(row->value));
}

Result oracle(const JoinSpec& spec, const Rows& left, const Rows& right) {
  Result result;
  std::vector<bool> matched(right.size(), false);
  for (const auto& l : left) {
    bool found = false;
    for (size_t j = 0; j < right.size(); ++j) {
      bool match = spec.comparison == OP_TYPE_LOWER_THAN ?
                       l.ts < right[j].ts : l.ts == right[j].ts;
      if (!match) continue;
      found = true;
      matched[j] = true;
      if (!oneSide(spec)) {
        result.push_back(rowText(&l) + "|" + rowText(&right[j]));
      }
    }
    if (oneSide(spec)) {
      if (spec.type == JOIN_TYPE_LEFT &&
          found == (spec.subtype == JOIN_STYPE_SEMI)) {
        result.push_back(rowText(&l));
      }
    } else if (!found &&
               (spec.type == JOIN_TYPE_LEFT || spec.type == JOIN_TYPE_FULL)) {
      result.push_back(rowText(&l) + "|" + rowText(nullptr));
    }
  }
  for (size_t j = 0; j < right.size(); ++j) {
    if (oneSide(spec)) {
      if (spec.type == JOIN_TYPE_RIGHT &&
          matched[j] == (spec.subtype == JOIN_STYPE_SEMI)) {
        result.push_back(rowText(&right[j]));
      }
    } else if (!matched[j] &&
               (spec.type == JOIN_TYPE_RIGHT || spec.type == JOIN_TYPE_FULL)) {
      result.push_back(rowText(nullptr) + "|" + rowText(&right[j]));
    }
  }
  std::sort(result.begin(), result.end());
  return result;
}

int32_t nextBlock(SOperatorInfo* op, SSDataBlock** block) {
  int32_t code = setjmp(op->pTaskInfo->env);
  if (code) return code;
  return op->fpSet.getNextFn(op, block);
}

Result drain(SOperatorInfo* op, const JoinSpec& spec, int maxBlocks = 100000) {
  Result result;
  for (int i = 0; i < maxBlocks; ++i) {
    SSDataBlock* block = nullptr;
    int32_t code = nextBlock(op, &block);
    EXPECT_EQ(code, TSDB_CODE_SUCCESS);
    if (code || !block) break;
    int first = 0;
    int count = oneSide(spec) ? 2 : 4;
    for (int row = 0; row < block->info.rows; ++row) {
      std::string text;
      for (int slot = first; slot < first + count; ++slot) {
        auto* col = static_cast<SColumnInfoData*>(
            taosArrayGet(block->pDataBlock, slot));
        if (slot != first) text += "|";
        text += colDataIsNull_s(col, row) ? "NULL" :
            std::to_string(*reinterpret_cast<int64_t*>(
                colDataGetData(col, row)));
      }
      result.push_back(text);
    }
    if (i == maxBlocks - 1 && maxBlocks > 1) {
      ADD_FAILURE() << "join did not finish within the block bound";
    }
  }
  std::sort(result.begin(), result.end());
  return result;
}

struct Tree {
  SExecTaskInfo task = {};
  SOperatorInfo* left;
  SOperatorInfo* right;
  JoinSpec spec;

  explicit Tree(const JoinSpec& config) : spec(config) {
    task.id.str = const_cast<char*>("joinResetTest");
    left = makeSource(7, &task);
    right = makeSource(8, &task);
    task.pRoot = makeJoin(spec, left, right, 9, &task);
  }
  ~Tree() {
    if (!task.pRoot) return;
    std::vector<SNode*> plans;
    collectPlans(task.pRoot, &plans);
    destroyOperator(task.pRoot);
    for (auto* plan : plans) nodesDestroyNode(plan);
  }

  void input(const Rows& l, const Rows& r, size_t blockRows = 2) {
    setRows(left, l, blockRows);
    setRows(right, r, blockRows);
  }

  void reset() {
    task.code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    ASSERT_EQ(streamClearStatesForOperators(&task), TSDB_CODE_SUCCESS);
    EXPECT_EQ(task.code, TSDB_CODE_SUCCESS);
    EXPECT_EQ(task.pRoot->status, OP_NOT_OPENED);
    EXPECT_EQ(task.pRoot->resultInfo.totalRows, 0);
  }
};

TEST(JoinReset, OrdinaryAlgorithmsMatchFreshTrees) {
  const std::vector<Rows> inputs = {
      {}, {{1, 10}}, {{1, 10}, {1, 11}, {2, 20, true}, {4, 40}},
      {{2, 200}, {3, 300}, {3, 301}, {5, 500, true}}};
  for (auto algorithm : {Algorithm::Hash, Algorithm::Merge,
                         Algorithm::Theta}) {
    for (auto type : {JOIN_TYPE_INNER, JOIN_TYPE_LEFT, JOIN_TYPE_RIGHT,
                      JOIN_TYPE_FULL}) {
      for (auto subtype : {JOIN_STYPE_OUTER, JOIN_STYPE_SEMI,
                           JOIN_STYPE_ANTI}) {
        if ((type == JOIN_TYPE_INNER || type == JOIN_TYPE_FULL) &&
            subtype != JOIN_STYPE_OUTER) continue;
        JoinSpec spec(algorithm, type, type == JOIN_TYPE_INNER ?
                                     JOIN_STYPE_NONE : subtype);
        SCOPED_TRACE(std::to_string(static_cast<int>(algorithm)) + ":" +
                     std::to_string(type) + ":" + std::to_string(subtype));
        Tree reused(spec);
        ASSERT_NE(reused.task.pRoot, nullptr);
        reused.reset();
        reused.reset();
        for (const auto& l : inputs) {
          for (const auto& r : inputs) {
            reused.reset();
            reused.input(l, r, 1);
            Tree fresh(spec);
            fresh.input(l, r, 1);
            auto expected = oracle(spec, l, r);
            EXPECT_EQ(drain(fresh.task.pRoot, spec), expected);
            EXPECT_EQ(drain(reused.task.pRoot, spec), expected);
          }
        }
        reused.reset();
        reused.input(inputs[2], inputs[2], 1);
        drain(reused.task.pRoot, spec, 1);
        reused.reset();
        reused.input(inputs[3], inputs[1], 1);
        EXPECT_EQ(drain(reused.task.pRoot, spec),
                  oracle(spec, inputs[3], inputs[1]));
      }
    }
  }
}

TEST(JoinReset, NestedMixedAlgorithmsConsumeDirectChildBlocks) {
  for (auto child : {Algorithm::Hash, Algorithm::Merge, Algorithm::Theta}) {
    for (auto parent : {Algorithm::Hash, Algorithm::Merge,
                        Algorithm::Theta}) {
      for (bool nestedRight : {false, true}) {
        SCOPED_TRACE(std::to_string(static_cast<int>(child)) + ":" +
                     std::to_string(static_cast<int>(parent)) + ":" +
                     std::to_string(nestedRight));
        JoinSpec inner(child, JOIN_TYPE_INNER, JOIN_STYPE_NONE);
        JoinSpec outer(parent, JOIN_TYPE_INNER, JOIN_STYPE_NONE);
        Tree tree(inner);
        auto* third = makeSource(10, &tree.task);
        tree.task.pRoot = nestedRight ?
            makeJoin(outer, third, tree.task.pRoot, 11, &tree.task) :
            makeJoin(outer, tree.task.pRoot, third, 11, &tree.task);
        ASSERT_NE(tree.task.pRoot, nullptr);
        for (int run = 0; run < 3; ++run) {
          tree.reset();
          int64_t ts = run + 1;
          Rows left = {{ts, 10 + run}, {ts, 20 + run, true}};
          Rows right = {{ts, 100}, {ts, 101}};
          Rows tail = {{ts, 1000 + run}, {ts, 1001 + run}};
          tree.input(left, right, 1);
          setRows(third, tail, 1);
          Rows projected = {left[0], left[0], left[1], left[1]};
          auto expected = nestedRight ? oracle(outer, tail, projected) :
                                        oracle(outer, projected, tail);
          EXPECT_EQ(drain(tree.task.pRoot, outer), expected);
        }
      }
    }
  }
}

TEST(JoinReset, CrossBlockOutputCanResetBeforeExhaustion) {
  for (auto algorithm : {Algorithm::Hash, Algorithm::Merge,
                         Algorithm::Theta}) {
    JoinSpec spec(algorithm, JOIN_TYPE_FULL, JOIN_STYPE_OUTER);
    Tree tree(spec);
    Rows left;
    Rows right;
    // Hash/Merge allocate 10 MiB blocks; Theta emits one candidate at a time.
    int inputRows = algorithm == Algorithm::Theta ? 3 : 577;
    for (int i = 0; i < inputRows; ++i) {
      left.emplace_back(1, i, i % 7 == 0);
      right.emplace_back(1, 100 + i, i % 9 == 0);
    }
    tree.input(left, right, 7);
    auto partial = drain(tree.task.pRoot, spec, 1);
    EXPECT_GT(partial.size(), 0u);
    EXPECT_LT(partial.size(), left.size() * right.size());
    tree.reset();
    Rows nextLeft = {{2, 20}, {3, 30}};
    Rows nextRight = {{3, 300}, {4, 400, true}};
    tree.input(nextLeft, nextRight, 1);
    EXPECT_EQ(drain(tree.task.pRoot, spec), oracle(spec, nextLeft, nextRight));
    tree.reset();
    tree.input(left, right, 7);
    EXPECT_EQ(drain(tree.task.pRoot, spec), oracle(spec, left, right));
  }
}

TEST(JoinReset, OrdinaryAlgorithmsResetAfterDownstreamError) {
  for (auto algorithm : {Algorithm::Hash, Algorithm::Merge,
                         Algorithm::Theta}) {
    for (bool failLeft : {false, true}) {
      SCOPED_TRACE(std::to_string(static_cast<int>(algorithm)) + ":" +
                   std::to_string(failLeft));
      JoinSpec spec(algorithm, JOIN_TYPE_FULL, JOIN_STYPE_OUTER);
      Tree tree(spec);
      tree.input({{1, 10}, {2, 20}}, {{1, 100}, {2, 200}}, 1);
      auto* source = static_cast<Source*>(
          (failLeft ? tree.left : tree.right)->info);
      source->failAt = 1;
      SSDataBlock* block = nullptr;
      int32_t code = TSDB_CODE_SUCCESS;
      for (int i = 0; i < 10 && !code; ++i) {
        code = nextBlock(tree.task.pRoot, &block);
        if (!block && !code) break;
      }
      EXPECT_EQ(code, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
      tree.reset();
      source->failAt = -1;
      Rows left = {{3, 30}, {4, 40, true}};
      Rows right = {{4, 400}, {5, 500}};
      tree.input(left, right, 1);
      EXPECT_EQ(drain(tree.task.pRoot, spec), oracle(spec, left, right));
    }
  }
}

TEST(JoinReset, ThetaNonEqualityPredicateSurvivesReset) {
  JoinSpec spec(Algorithm::Theta, JOIN_TYPE_FULL, JOIN_STYPE_OUTER);
  spec.comparison = OP_TYPE_LOWER_THAN;
  Tree tree(spec);
  for (int run = 0; run < 3; ++run) {
    tree.reset();
    int shift = run * 10;
    Rows left = {{1 + shift, 10}, {4 + shift, 40, true}};
    Rows right = {{shift, 100}, {3 + shift, 300}};
    tree.input(left, right, 1);
    EXPECT_EQ(drain(tree.task.pRoot, spec), oracle(spec, left, right));
    tree.reset();
    tree.input(right, left, 1);
    EXPECT_FALSE(drain(tree.task.pRoot, spec, 1).empty());
  }
}

bool timeMatches(EOperatorType op, int64_t left, int64_t right) {
  switch (op) {
    case OP_TYPE_GREATER_THAN: return left > right;
    case OP_TYPE_GREATER_EQUAL: return left >= right;
    case OP_TYPE_LOWER_THAN: return left < right;
    case OP_TYPE_LOWER_EQUAL: return left <= right;
    default: return left == right;
  }
}

Result temporalOracle(const JoinSpec& spec, const Rows& left,
                       const Rows& right) {
  bool rightDrive = spec.type == JOIN_TYPE_RIGHT;
  const Rows& drive = rightDrive ? right : left;
  const Rows& build = rightDrive ? left : right;
  Result result;
  for (const auto& row : drive) {
    std::vector<const Row*> matches;
    for (const auto& candidate : build) {
      bool match = spec.subtype == JOIN_STYPE_WIN ?
          candidate.ts >= row.ts - 2 && candidate.ts <= row.ts + 1 :
          timeMatches(spec.comparison, rightDrive ? candidate.ts : row.ts,
                      rightDrive ? row.ts : candidate.ts);
      if (match) matches.push_back(&candidate);
    }
    if (spec.subtype == JOIN_STYPE_ASOF) {
      std::stable_sort(matches.begin(), matches.end(),
                       [&row](const Row* a, const Row* b) {
        return std::abs(a->ts - row.ts) < std::abs(b->ts - row.ts);
      });
    } else {
      // WINDOW JLIMIT selects the earliest rows in the time window,
      // independently of the physical input scan direction.
      std::stable_sort(matches.begin(), matches.end(),
                       [](const Row* a, const Row* b) {
        return a->ts < b->ts;
      });
    }
    if (matches.size() > static_cast<size_t>(spec.limit)) {
      matches.resize(spec.limit);
    }
    if (matches.empty()) matches.push_back(nullptr);
    for (const Row* match : matches) {
      result.push_back(rightDrive ? rowText(match) + "|" + rowText(&row) :
                                   rowText(&row) + "|" + rowText(match));
    }
  }
  std::sort(result.begin(), result.end());
  return result;
}

TEST(JoinReset, ThetaAndTemporalParentsConsumeDirectChildBlocks) {
  for (auto type : {JOIN_TYPE_LEFT, JOIN_TYPE_RIGHT}) {
    for (auto subtype : {JOIN_STYPE_ASOF, JOIN_STYPE_WIN}) {
      for (bool thetaParent : {false, true}) {
        for (bool nestedRight : {false, true}) {
          SCOPED_TRACE(std::to_string(type) + ":" +
                       std::to_string(subtype) + ":" +
                       std::to_string(thetaParent) + ":" +
                       std::to_string(nestedRight));
          JoinSpec theta(Algorithm::Theta, JOIN_TYPE_INNER,
                          JOIN_STYPE_NONE);
          JoinSpec temporal(Algorithm::Merge, type, subtype);
          const JoinSpec& inner = thetaParent ? temporal : theta;
          const JoinSpec& outer = thetaParent ? theta : temporal;
          Tree tree(inner);
          auto* third = makeSource(10, &tree.task);
          tree.task.pRoot = nestedRight ?
              makeJoin(outer, third, tree.task.pRoot, 11, &tree.task) :
              makeJoin(outer, tree.task.pRoot, third, 11, &tree.task);
          ASSERT_NE(tree.task.pRoot, nullptr);
          for (int run = 0; run < 3; ++run) {
            tree.reset();
            int shift = run * 100;
            Rows left = {{1 + shift, 10}, {11 + shift, 20, true},
                         {21 + shift, 30}};
            Rows right = {{1 + shift, 100}, {11 + shift, 200},
                          {21 + shift, 300}};
            Rows tail = {{1 + shift, 1000}, {11 + shift, 2000},
                         {21 + shift, 3000}};
            tree.input(left, right, 1);
            setRows(third, tail, 1);
            // Each child produces the left rows once, in timestamp order.
            const Rows& outerLeft = nestedRight ? tail : left;
            const Rows& outerRight = nestedRight ? left : tail;
            auto expected = thetaParent ?
                oracle(outer, outerLeft, outerRight) :
                temporalOracle(outer, outerLeft, outerRight);
            EXPECT_EQ(drain(tree.task.pRoot, outer), expected);
          }
        }
      }
    }
  }
}

TEST(JoinReset, TemporalDirectionLimitsAndExhaustionMatchFreshTrees) {
  for (auto type : {JOIN_TYPE_LEFT, JOIN_TYPE_RIGHT}) {
    for (auto subtype : {JOIN_STYPE_ASOF, JOIN_STYPE_WIN}) {
      for (auto order : {ORDER_ASC, ORDER_DESC}) {
        for (auto comparison : {OP_TYPE_GREATER_THAN, OP_TYPE_GREATER_EQUAL,
                                OP_TYPE_EQUAL, OP_TYPE_LOWER_THAN,
                                OP_TYPE_LOWER_EQUAL}) {
          if (subtype == JOIN_STYPE_WIN && comparison != OP_TYPE_EQUAL) {
            continue;
          }
          for (int limit : {1, 2}) {
            JoinSpec spec(Algorithm::Merge, type, subtype);
            spec.order = order;
            spec.comparison = comparison;
            spec.limit = limit;
            SCOPED_TRACE(std::to_string(type) + ":" +
                         std::to_string(subtype) + ":" +
                         std::to_string(order) + ":" +
                         std::to_string(comparison) + ":" +
                         std::to_string(limit));
            Tree reused(spec);
            for (int run = 0; run < 4; ++run) {
              // Equal timestamps use equal payloads: JLIMIT may select any
              // tied row, but duplicates and the limit still matter.
              Rows left = {{0, 10}, {2, 20}, {2, 20}, {6, 60, true}};
              Rows right = {{1, 100}, {2, 200}, {2, 200}, {4, 400}};
              if (run == 1) {
                left = {{3, 30}, {8, 80}};
                right = {{2, 220}, {7, 770, true}};
              } else if (run == 2) {
                left.clear();
              } else if (run == 3) {
                right.clear();
              }
              if (order == ORDER_DESC) {
                std::reverse(left.begin(), left.end());
                std::reverse(right.begin(), right.end());
              }
              reused.reset();
              reused.input(left, right, 1);
              Tree fresh(spec);
              fresh.input(left, right, 1);
              auto expected = temporalOracle(spec, left, right);
              EXPECT_EQ(drain(fresh.task.pRoot, spec), expected);
              EXPECT_EQ(drain(reused.task.pRoot, spec), expected);
            }
            reused.reset();
            reused.input({{2, 20}}, {{2, 200}, {2, 200}}, 1);
            drain(reused.task.pRoot, spec, 1);
            reused.reset();
            reused.input({{9, 90}}, {{9, 900}}, 1);
            EXPECT_EQ(drain(reused.task.pRoot, spec),
                      temporalOracle(spec, {{9, 90}}, {{9, 900}}));
          }
        }
      }
    }
  }
}

TEST(JoinReset, NestedTemporalInputsFilterNullTimesAcrossReset) {
  for (auto subtype : {JOIN_STYPE_ASOF, JOIN_STYPE_WIN}) {
    for (auto type : {JOIN_TYPE_LEFT, JOIN_TYPE_RIGHT}) {
      for (bool nestedRight : {false, true}) {
        JoinSpec inner(Algorithm::Hash, JOIN_TYPE_FULL, JOIN_STYPE_OUTER);
        JoinSpec outer(Algorithm::Merge, type, subtype);
        outer.comparison = OP_TYPE_GREATER_EQUAL;
        outer.limit = 2;
        Tree tree(inner);
        auto* filtered = filterTime(tree.task.pRoot, 10, &tree.task);
        ASSERT_NE(filtered, nullptr);
        auto* third = makeSource(11, &tree.task);
        tree.task.pRoot = nestedRight ?
            makeJoin(outer, third, filtered, 12, &tree.task) :
            makeJoin(outer, filtered, third, 12, &tree.task);
        ASSERT_NE(tree.task.pRoot, nullptr);
        for (int run = 0; run < 3; ++run) {
          tree.reset();
          int shift = run * 4;
          Rows left = {{1 + shift, 10}, {3 + shift, 30}};
          Rows right = {{1 + shift, 100}, {2 + shift, 200}};
          Rows tail = {{shift, 1000}, {2 + shift, 2000}, {4 + shift, 4000}};
          tree.input(left, right, 1);
          setRows(third, tail, 1);
          auto expected = nestedRight ? temporalOracle(outer, tail, left) :
                                        temporalOracle(outer, left, tail);
          EXPECT_EQ(drain(tree.task.pRoot, outer), expected);
        }
      }
    }
  }
}

}  // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  tsTempSpace.size.avail = 1024 * 1024;
  return RUN_ALL_TESTS();
}
