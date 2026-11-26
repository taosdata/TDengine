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

#include <algorithm>
#include <array>
#include <functional>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "planTestUtil.h"
#include "planJoinParallel.h"
#include "functionMgt.h"
#include "mockCatalogService.h"
#include "scalar.h"
#include "stub.h"
#include "tglobal.h"

namespace {

using NodePtr = std::unique_ptr<SNode, decltype(&nodesDestroyNode)>;
using ModelPtr = std::unique_ptr<SJoinRewriteContext,
                                 decltype(&planDestroyJoinRewriteContext)>;
using EqPtr = std::unique_ptr<SJoinEqClasses,
                              decltype(&planDestroyJoinEqClasses)>;
using ConflictPtr = std::unique_ptr<SJoinConflictModel,
                                    decltype(&planDestroyJoinConflicts)>;
using ChoicePtr = std::unique_ptr<SJoinTreeChoice,
                                  decltype(&planDestroyJoinTreeChoice)>;

std::string joinChoiceText(const SJoinTreeChoice* choice, int32_t cellId) {
  const auto* cell = planJoinTreeCell(choice, cellId);
  EXPECT_NE(cell, nullptr);
  if (cell == nullptr) return "invalid";
  if (cell->pPureChoice != nullptr) {
    return "pure" + joinChoiceText(cell->pPureChoice,
                                   cell->pPureChoice->rootCell);
  }
  if (cell->inputId >= 0) return std::to_string(cell->inputId);
  return "(" + std::to_string(cell->opId) + ":" +
         std::to_string(cell->swapped) + ":" +
         joinChoiceText(choice, cell->left) + "," +
         joinChoiceText(choice, cell->right) + ")";
}

void checkJoinChoice(const SJoinTreeChoice* choice, int32_t cellId,
                       std::set<int32_t>& inputs, std::set<int32_t>& ops) {
  const auto* cell = planJoinTreeCell(choice, cellId);
  ASSERT_NE(cell, nullptr);
  ASSERT_TRUE(cell->valid);
  if (cell->pPureChoice != nullptr) {
    ASSERT_EQ(choice->mode, JOIN_REGION_MIXED);
    const auto* pure = cell->pPureChoice;
    ASSERT_EQ(pure->mode, JOIN_REGION_PURE_INNER);
    EXPECT_EQ(pure->pRegion, choice->pRegion);
    EXPECT_EQ(pure->pConflicts, nullptr);
    EXPECT_EQ(pure->pRootFilter, nullptr);
    EXPECT_EQ(cell->inputId, -1);
    EXPECT_EQ(cell->opId, -1);
    EXPECT_EQ(cell->left, -1);
    EXPECT_EQ(cell->right, -1);
    const auto* root = planJoinTreeCell(pure, pure->rootCell);
    ASSERT_NE(root, nullptr);
    EXPECT_EQ(root->inputs, cell->inputs);
    EXPECT_EQ(root->height, cell->height);
    std::set<int32_t> pureOps;
    checkJoinChoice(pure, pure->rootCell, inputs, pureOps);
    EXPECT_TRUE(pureOps.empty());
    for (int32_t i = 0; i < 8; ++i) {
      if ((cell->usedOps & (1U << i)) != 0) {
        EXPECT_TRUE(ops.insert(i).second);
      }
    }
    return;
  }
  if (cell->inputId >= 0) {
    EXPECT_TRUE(inputs.insert(cell->inputId).second);
    EXPECT_EQ(cell->inputs, UINT64_C(1) << cell->inputId);
    EXPECT_EQ(cell->height, 0);
    EXPECT_EQ(cell->usedOps, 0U);
    EXPECT_EQ(cell->opId, -1);
    return;
  }
  ASSERT_GE(cell->left, 0);
  ASSERT_GE(cell->right, 0);
  ASSERT_LT(cell->left, cellId);
  ASSERT_LT(cell->right, cellId);
  checkJoinChoice(choice, cell->left, inputs, ops);
  checkJoinChoice(choice, cell->right, inputs, ops);
  const auto* left = planJoinTreeCell(choice, cell->left);
  const auto* right = planJoinTreeCell(choice, cell->right);
  ASSERT_NE(left, nullptr);
  ASSERT_NE(right, nullptr);
  EXPECT_EQ(left->inputs & right->inputs, 0U);
  EXPECT_EQ(cell->inputs, left->inputs | right->inputs);
  EXPECT_EQ(cell->height, 1 + (std::max)(left->height, right->height));
  if (choice->mode == JOIN_REGION_MIXED) {
    ASSERT_GE(cell->opId, 0);
    EXPECT_TRUE(ops.insert(cell->opId).second);
    const uint32_t root = 1U << cell->opId;
    EXPECT_EQ(left->usedOps & right->usedOps, 0U);
    EXPECT_EQ((left->usedOps | right->usedOps) & root, 0U);
    EXPECT_EQ(cell->usedOps, left->usedOps | right->usedOps | root);
    EXPECT_TRUE(planJoinSplitIsLegal(choice->pConflicts, cell->opId,
                                      left->inputs, right->inputs,
                                      cell->swapped));
  } else {
    EXPECT_EQ(cell->opId, -1);
    EXPECT_EQ(cell->usedOps, 0U);
  }
}

void collectChosenPure(const SJoinTreeChoice* choice, int32_t cellId,
                         std::vector<const SJoinTreeChoice*>& pure) {
  const auto* cell = planJoinTreeCell(choice, cellId);
  ASSERT_NE(cell, nullptr);
  if (cell->pPureChoice != nullptr) {
    pure.push_back(cell->pPureChoice);
    return;
  }
  if (cell->inputId >= 0) return;
  collectChosenPure(choice, cell->left, pure);
  collectChosenPure(choice, cell->right, pure);
}

const SLogicNode* findNode(const SLogicNode* node, ENodeType type) {
  if (nodeType(node) == type) return node;
  SNode* child = nullptr;
  FOREACH(child, node->pChildren) {
    const auto* found = findNode((const SLogicNode*)child, type);
    if (found != nullptr) return found;
  }
  return nullptr;
}

const SJoinLogicNode* firstJoin(const SLogicNode* root) {
  return (const SJoinLogicNode*)findNode(root, QUERY_NODE_LOGIC_PLAN_JOIN);
}

const SJoinLogicNode* temporalJoin(const SLogicNode* root) {
  if (nodeType(root) == QUERY_NODE_LOGIC_PLAN_JOIN) {
    const auto* join = (const SJoinLogicNode*)root;
    if (IS_ASOF_JOIN(join->subType) || IS_WINDOW_JOIN(join->subType)) {
      return join;
    }
  }
  SNode* child = nullptr;
  FOREACH(child, root->pChildren) {
    const auto* found = temporalJoin((const SLogicNode*)child);
    if (found != nullptr) return found;
  }
  return nullptr;
}

void setParents(SLogicNode* node, SLogicNode* parent = nullptr) {
  node->pParent = parent;
  SNode* child = nullptr;
  FOREACH(child, node->pChildren) { setParents((SLogicNode*)child, node); }
}

std::string nodeText(const SNode* node) {
  char*         text = nullptr;
  int32_t       length = 0;
  const int32_t code = nodesNodeToString(node, false, &text, &length);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  const std::string result = text == nullptr ? "" : text;
  taosMemoryFree(text);
  return result;
}

struct TreeCopy {
  SPlanContext context;
  NodePtr      tree;
  int32_t      code;

  TreeCopy(const SPlanContext* original, const SLogicNode* root)
      : context(*original), tree(nullptr, nodesDestroyNode) {
    context.pJoinPropsStore = nullptr;
    SNode* copy = nullptr;
    code = nodesCloneNode((const SNode*)root, &copy);
    tree.reset(copy);
    if (code == TSDB_CODE_SUCCESS) setParents((SLogicNode*)copy);
  }

  ~TreeCopy() { planDestroyJoinPropsStore(&context); }

  SLogicNode* root() { return (SLogicNode*)tree.get(); }
};

struct BuiltJoinTree {
  SPlanContext context;
  NodePtr tree;

  BuiltJoinTree(const SPlanContext* original, SLogicNode* root)
      : context(*original), tree((SNode*)root, nodesDestroyNode) {
    context.pJoinPropsStore = nullptr;
  }

  ~BuiltJoinTree() { planDestroyJoinPropsStore(&context); }

  SLogicNode* root() { return (SLogicNode*)tree.get(); }
};

struct SplitTreeCopy {
  SPlanContext context;
  NodePtr owner;
  int32_t code;

  SplitTreeCopy(const SPlanContext* original, const SLogicNode* root)
      : context(*original), owner(nullptr, nodesDestroyNode), code(0) {
    context.pJoinPropsStore = nullptr;
    context.groupId = 100;
    SNode* holder = nullptr;
    code = nodesMakeNode(QUERY_NODE_LOGIC_SUBPLAN, &holder);
    owner.reset(holder);
    if (code != 0) return;
    plan()->id.groupId = 1;
    plan()->subplanType = SUBPLAN_TYPE_MERGE;
    code = nodesCloneNode((const SNode*)root, (SNode**)&plan()->pNode);
    if (code == 0) setParents(plan()->pNode);
  }

  ~SplitTreeCopy() { planDestroyJoinPropsStore(&context); }

  SLogicSubplan* plan() { return (SLogicSubplan*)owner.get(); }
};

int32_t builtJoinHeight(const SLogicNode* node) {
  int32_t height = 0;
  SNode* child = nullptr;
  FOREACH(child, node->pChildren) {
    height = (std::max)(height, builtJoinHeight((const SLogicNode*)child));
  }
  return height + (nodeType(node) == QUERY_NODE_LOGIC_PLAN_JOIN);
}

void collectBuiltNodes(const SLogicNode* node,
                         std::set<const SLogicNode*>& nodes) {
  EXPECT_TRUE(nodes.insert(node).second);
  SNode* child = nullptr;
  FOREACH(child, node->pChildren) {
    collectBuiltNodes((const SLogicNode*)child, nodes);
  }
}

bool builtHasTarget(const SLogicNode* node, const SColumnNode* column) {
  SNode* target = nullptr;
  FOREACH(target, node->pTargets) {
    if (nodeType(target) != QUERY_NODE_COLUMN) continue;
    const auto* output = (const SColumnNode*)target;
    if (strcmp(output->tableAlias, column->tableAlias) == 0 &&
        strcmp(output->colName, column->colName) == 0) return true;
  }
  return false;
}

void checkBuiltParents(const SLogicNode* node, const SLogicNode* parent,
                         const std::set<const SLogicNode*>& original) {
  EXPECT_EQ(node->pParent, parent);
  EXPECT_EQ(original.count(node), 0U);
  SNode* child = nullptr;
  FOREACH(child, node->pChildren) {
    checkBuiltParents((const SLogicNode*)child, node, original);
  }
}

void checkBuiltJoinBindings(const SLogicNode* node) {
  if (nodeType(node) == QUERY_NODE_LOGIC_PLAN_JOIN) {
    const auto* join = (const SJoinLogicNode*)node;
    ASSERT_EQ(LIST_LENGTH(node->pChildren), 2);
    const auto* left = (const SLogicNode*)nodesListGetNode(node->pChildren, 0);
    const auto* right = (const SLogicNode*)nodesListGetNode(node->pChildren, 1);
    for (auto* condition : {join->pFullOnCond, join->pPrimKeyEqCond,
                              join->addPrimEqCond, join->pColEqCond,
                              join->pColOnCond, join->pTagEqCond,
                              join->pTagOnCond, join->pLeftOnCond,
                              join->pRightOnCond, node->pConditions}) {
      SNodeList* columns = nullptr;
      ASSERT_EQ(nodesCollectColumnsFromNode(condition, nullptr,
                                             COLLECT_COL_TYPE_ALL, &columns),
                0);
      SNode* column = nullptr;
      FOREACH(column, columns) {
        const auto* ref = (const SColumnNode*)column;
        EXPECT_EQ((builtHasTarget(left, ref) ? 1 : 0) +
                      (builtHasTarget(right, ref) ? 1 : 0), 1)
            << ref->tableAlias << "." << ref->colName;
      }
      nodesDestroyList(columns);
    }
  }
  SNode* child = nullptr;
  FOREACH(child, node->pChildren) {
    checkBuiltJoinBindings((const SLogicNode*)child);
  }
}

void checkBuiltTargets(const SLogicNode* original, const SLogicNode* built) {
  ASSERT_EQ(LIST_LENGTH(original->pTargets), LIST_LENGTH(built->pTargets));
  for (int32_t i = 0; i < LIST_LENGTH(original->pTargets); ++i) {
    EXPECT_TRUE(nodesEqualNode(nodesListGetNode(original->pTargets, i),
                                nodesListGetNode(built->pTargets, i)));
  }
}

void checkHashJoinsWithoutPartitions(const SLogicNode* root) {
  std::set<const SLogicNode*> nodes;
  collectBuiltNodes(root, nodes);
  int32_t joins = 0;
  for (const auto* node : nodes) {
    EXPECT_NE(nodeType(node), QUERY_NODE_LOGIC_PLAN_PARTITION);
    if (nodeType(node) != QUERY_NODE_LOGIC_PLAN_JOIN ||
        ((const SJoinLogicNode*)node)->joinAlgo != JOIN_ALGO_HASH) continue;
    ++joins;
    ASSERT_EQ(LIST_LENGTH(node->pChildren), 2);
  }
  EXPECT_GT(joins, 0);
}

bool hasPositiveLimitPredicate(SNode* condition) {
  bool found = false;
  nodesWalkExpr(condition, [](SNode* node, void* context) {
    if (nodeType(node) != QUERY_NODE_OPERATOR) return DEAL_RES_CONTINUE;
    const auto* op = (const SOperatorNode*)node;
    if (op->opType != OP_TYPE_GREATER_THAN || op->pLeft == nullptr ||
        op->pRight == nullptr || nodeType(op->pLeft) != QUERY_NODE_COLUMN ||
        nodeType(op->pRight) != QUERY_NODE_VALUE) return DEAL_RES_CONTINUE;
    const auto* column = (const SColumnNode*)op->pLeft;
    const auto* value = (const SValueNode*)op->pRight;
    if (strcmp(column->colName, "c1") == 0 && !value->isNull &&
        value->datum.i == 0) *(bool*)context = true;
    return DEAL_RES_CONTINUE;
  }, &found);
  return found;
}

void checkFilterAboveLimit(const SLogicNode* root) {
  std::set<const SLogicNode*> nodes;
  collectBuiltNodes(root, nodes);
  const SLogicNode* limited = nullptr;
  const SLogicNode* filtered = nullptr;
  for (const auto* node : nodes) {
    if (node->pLimit != nullptr) {
      EXPECT_EQ(limited, nullptr);
      limited = node;
      const auto* limit = (const SLimitNode*)node->pLimit;
      ASSERT_NE(limit->limit, nullptr);
      EXPECT_EQ(limit->limit->datum.i, 2);
    }
    if (hasPositiveLimitPredicate(node->pConditions)) {
      EXPECT_EQ(filtered, nullptr);
      filtered = node;
    }
  }
  ASSERT_NE(limited, nullptr);
  ASSERT_NE(filtered, nullptr);
  EXPECT_NE(filtered, limited);
  const SLogicNode* ancestor = limited->pParent;
  for (; ancestor != nullptr && ancestor != filtered;
         ancestor = ancestor->pParent) {}
  EXPECT_EQ(ancestor, filtered);
}

ModelPtr prepareJoinModel(const SPlanContext* context, const SLogicNode* root) {
  SJoinRewriteContext* model = nullptr;
  EXPECT_EQ(planPrepareJoinRewrite(context, root, &model), TSDB_CODE_SUCCESS);
  return ModelPtr(model, planDestroyJoinRewriteContext);
}

const SJoinParallelRegion* regionAt(const SJoinRewriteContext* model,
                                    size_t                     index) {
  return (const SJoinParallelRegion*)taosArrayGetP(model->pRegions, index);
}

const SJoinParallelRegion* regionFor(const SJoinRewriteContext* model,
                                     const SLogicNode*          root) {
  for (size_t i = 0; i < taosArrayGetSize(model->pRegions); ++i) {
    const auto* region = regionAt(model, i);
    if (&region->pRoot->node == root) return region;
  }
  return nullptr;
}

const SJoinOutputCol* inputColumn(const SJoinParallelRegion* region,
                                  int32_t inputId, const char* name) {
  const auto* input =
      (const SJoinParallelInput*)taosArrayGet(region->pInputs, inputId);
  for (size_t i = 0; i < taosArrayGetSize(input->pProps->pOutputCols); ++i) {
    const auto* column =
        (const SJoinOutputCol*)taosArrayGet(input->pProps->pOutputCols, i);
    if (nodeType(column->pExpr) == QUERY_NODE_COLUMN &&
        strcmp(((const SColumnNode*)column->pExpr)->colName, name) == 0) {
      return column;
    }
  }
  return nullptr;
}

EqPtr buildEqClasses(const SJoinRewriteContext* model,
                      const SJoinParallelRegion* region) {
  SJoinEqClasses* classes = nullptr;
  EXPECT_EQ(planBuildJoinEqClasses(model, region, &classes), TSDB_CODE_SUCCESS);
  return EqPtr(classes, planDestroyJoinEqClasses);
}

std::set<int32_t> dependencies(const SJoinParallelRegion*    region,
                               const SJoinParallelPredicate* predicate) {
  std::set<int32_t> result;
  for (size_t i = 0; i < taosArrayGetSize(region->pInputs); ++i) {
    if (planJoinInputSetContains(predicate->inputs, (int32_t)i)) {
      result.insert((int32_t)i);
    }
  }
  return result;
}

bool hasNotNull(const SNode* expression, const SNode* column) {
  if (expression == nullptr) return false;
  if (nodeType(expression) == QUERY_NODE_OPERATOR) {
    const auto* op = (const SOperatorNode*)expression;
    return op->opType == OP_TYPE_IS_NOT_NULL &&
           nodesEqualNode(op->pLeft, column);
  }
  if (nodeType(expression) != QUERY_NODE_LOGIC_CONDITION) return false;
  SNode* parameter = nullptr;
  FOREACH(parameter, ((const SLogicConditionNode*)expression)->pParameterList) {
    if (hasNotNull(parameter, column)) return true;
  }
  return false;
}

// Include pushed filters, which may no longer be on the input's root node.
int countNotNullFilters(const SLogicNode* node) {
  int count = 0;
  nodesWalkExpr(node->pConditions, [](SNode* expr, void* context) {
    if (nodeType(expr) == QUERY_NODE_OPERATOR &&
        ((const SOperatorNode*)expr)->opType == OP_TYPE_IS_NOT_NULL) {
      ++*static_cast<int*>(context);
    }
    return DEAL_RES_CONTINUE;
  }, &count);
  SNode* child = nullptr;
  FOREACH(child, node->pChildren) {
    count += countNotNullFilters((const SLogicNode*)child);
  }
  return count;
}

SArray* failArrayInit(size_t, size_t) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return nullptr;
}

int32_t failEqHashPut(SSHashObj*, const void*, size_t, const void*, size_t) {
  return TSDB_CODE_OUT_OF_MEMORY;
}

int32_t conflictArrayCountdown = 0;

SArray* failConflictArrayInit(size_t size, size_t elemSize) {
  if (--conflictArrayCountdown == 0) return failArrayInit(size, elemSize);
  // Use the alternate public initializer while taosArrayInit is stubbed.
  SArray* result = taosArrayInit_s(elemSize, size);
  if (result != nullptr) taosArrayClear(result);
  return result;
}

int32_t pureCellArrayCountdown = 0;

SArray* failNestedPureCells(size_t elemSize, size_t size) {
  if (--pureCellArrayCountdown == 0) return failArrayInit(size, elemSize);
  SArray* result = taosArrayInit(size, elemSize);
  if (result != nullptr) result->size = size;
  return result;
}

SArray* failPurePredicateArray(size_t size, size_t elemSize) {
  if (elemSize == sizeof(SJoinParallelPredicate)) {
    return failArrayInit(size, elemSize);
  }
  SArray* result = taosArrayInit_s(elemSize, size);
  if (result != nullptr) taosArrayClear(result);
  return result;
}

int32_t failBuildClone(const SNode*, SNode** output) {
  *output = nullptr;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return terrno;
}

int32_t failBuildAppend(SNodeList**, SNode*) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return terrno;
}

int32_t failBuildInputBinding(SPlanContext*, SLogicNode*) {
  return TSDB_CODE_PLAN_SLOT_NOT_FOUND;
}

struct NullTestValues {
  int32_t values[3];  // -1, 0, 1, or 2 for NULL; aliases a, b, c.
};

EDealRes replaceNullTestColumn(SNode** node, void* context) {
  if (nodeType(*node) != QUERY_NODE_COLUMN) return DEAL_RES_CONTINUE;
  const auto* column = (const SColumnNode*)*node;
  const int32_t input = column->tableAlias[0] - 'a';
  if (input < 0 || input >= 3 || column->tableAlias[1] != '\0') {
    ADD_FAILURE() << "Unexpected column in scalar evaluation";
    return DEAL_RES_ERROR;
  }
  const auto* values = (const NullTestValues*)context;
  SNode* literal = nullptr;
  if (nodesMakeValueNodeFromInt32(values->values[input], &literal) != 0) {
    ADD_FAILURE() << "Failed to allocate scalar sample";
    return DEAL_RES_ERROR;
  }
  ((SValueNode*)literal)->isNull = values->values[input] == 2;
  nodesDestroyNode(*node);
  *node = literal;
  return DEAL_RES_CONTINUE;
}

// Check every claimed rejection with Scalar, independently of the analysis.
void checkNullRejectionWithScalar(const SNode* condition,
                                   SJoinInputSet rejected) {
  for (int32_t input = 0; input < 3; ++input) {
    if (!planJoinInputSetContains(rejected, input)) continue;
    for (int32_t sample = 0; sample < 16; ++sample) {
      NullTestValues values = {};
      int32_t encoded = sample;
      for (int32_t i = 0; i < 3; ++i) {
        values.values[i] = i == input ? 2 : encoded % 4 - 1;
        if (i != input) encoded /= 4;
      }
      SCOPED_TRACE(input);
      SCOPED_TRACE(sample);
      SNode* copy = nullptr;
      ASSERT_EQ(nodesCloneNode(condition, &copy), TSDB_CODE_SUCCESS);
      nodesRewriteExpr(&copy, replaceNullTestColumn, &values);
      SNode* evaluated = nullptr;
      ASSERT_EQ(scalarCalculateConstants(copy, &evaluated), TSDB_CODE_SUCCESS);
      NodePtr result(evaluated, nodesDestroyNode);
      ASSERT_NE(result, nullptr);
      ASSERT_EQ(nodeType(result.get()), QUERY_NODE_VALUE);
      const auto* value = (const SValueNode*)result.get();
      EXPECT_TRUE(value->isNull ||
                  (value->node.resType.type == TSDB_DATA_TYPE_BOOL &&
                   !value->datum.b));
    }
  }
}

// A small bag interpreter, independent of the CD-C property tables. Value 2
// denotes NULL; test tables have one integer column, aliased a/b/c/d.
using ConflictRow = std::array<int32_t, 4>;
using ConflictBag = std::vector<ConflictRow>;

int32_t conflictValue(const SNode* expr, const ConflictRow& row) {
  if (expr == nullptr) return 1;
  switch (nodeType(expr)) {
    case QUERY_NODE_COLUMN: {
      const auto* column = (const SColumnNode*)expr;
      const int32_t input = column->tableAlias[0] - 'a';
      if (input < 0 || input >= 4 || column->tableAlias[1] != '\0') {
        ADD_FAILURE() << "Unexpected bag column alias";
        return 2;
      }
      return row[input];
    }
    case QUERY_NODE_VALUE: {
      const auto* value = (const SValueNode*)expr;
      if (value->isNull) return 2;
      return value->node.resType.type == TSDB_DATA_TYPE_BOOL ? value->datum.b
                                                            : value->datum.i;
    }
    case QUERY_NODE_OPERATOR: {
      const auto* op = (const SOperatorNode*)expr;
      int32_t left = conflictValue(op->pLeft, row);
      if (op->opType == OP_TYPE_IS_NULL) return left == 2;
      if (op->opType == OP_TYPE_IS_NOT_NULL) return left != 2;
      int32_t right = conflictValue(op->pRight, row);
      if (left == 2 || right == 2) return 2;
      switch (op->opType) {
        case OP_TYPE_EQUAL: return left == right;
        case OP_TYPE_NOT_EQUAL: return left != right;
        case OP_TYPE_GREATER_THAN: return left > right;
        case OP_TYPE_GREATER_EQUAL: return left >= right;
        case OP_TYPE_LOWER_THAN: return left < right;
        case OP_TYPE_LOWER_EQUAL: return left <= right;
        default:
          ADD_FAILURE() << "Unexpected bag operator " << op->opType;
          return 2;
      }
    }
    case QUERY_NODE_LOGIC_CONDITION: {
      const auto* logic = (const SLogicConditionNode*)expr;
      if (logic->condType == LOGIC_COND_TYPE_NOT) {
        const int32_t value = conflictValue(
            nodesListGetNode(logic->pParameterList, 0), row);
        return value == 2 ? 2 : !value;
      }
      const bool isAnd = logic->condType == LOGIC_COND_TYPE_AND;
      bool hasNull = false;
      SNode* child = nullptr;
      FOREACH(child, logic->pParameterList) {
        int32_t value = conflictValue(child, row);
        if (isAnd && value == 0) return 0;
        if (!isAnd && value == 1) return 1;
        hasNull |= value == 2;
      }
      return hasNull ? 2 : (isAnd ? 1 : 0);
    }
    default:
      ADD_FAILURE() << "Unexpected bag predicate node " << nodeType(expr);
      return 2;
  }
}

ConflictBag conflictFilter(const ConflictBag& rows, const SNode* condition) {
  if (condition == nullptr) return rows;
  ConflictBag result;
  for (const auto& row : rows) {
    if (conflictValue(condition, row) == 1) result.push_back(row);
  }
  return result;
}

struct ConflictRows {
  ConflictBag rows;
  SJoinInputSet inputs;
};

ConflictRows conflictBagJoin(const SJoinLogicNode* op, bool swapped,
                              const ConflictRows& physicalLeft,
                              const ConflictRows& physicalRight) {
  const bool reverse = (op->joinType == JOIN_TYPE_RIGHT) != swapped;
  const auto& left = reverse ? physicalRight : physicalLeft;
  const auto& right = reverse ? physicalLeft : physicalRight;
  const bool semi = op->subType == JOIN_STYPE_SEMI;
  const bool anti = op->subType == JOIN_STYPE_ANTI;
  const bool outer = op->joinType != JOIN_TYPE_INNER && !semi && !anti;
  std::vector<bool> rightMatched(right.rows.size(), false);
  ConflictRows result = {{}, left.inputs | right.inputs};
  for (const auto& l : left.rows) {
    bool matched = false;
    for (size_t j = 0; j < right.rows.size(); ++j) {
      ConflictRow combined = {};
      for (int32_t i = 0; i < 4; ++i) {
        combined[i] = planJoinInputSetContains(left.inputs, i)
                          ? l[i] : right.rows[j][i];
      }
      if (conflictValue(op->pFullOnCond, combined) != 1 ||
          conflictValue(op->pPrimKeyEqCond, combined) != 1) continue;
      matched = true;
      rightMatched[j] = true;
      if (!semi && !anti) result.rows.push_back(combined);
    }
    if ((semi && matched) || (anti && !matched) || (outer && !matched)) {
      result.rows.push_back(l);
    }
  }
  if (op->joinType == JOIN_TYPE_FULL) {
    for (size_t j = 0; j < right.rows.size(); ++j) {
      if (!rightMatched[j]) result.rows.push_back(right.rows[j]);
    }
  }
  return result;
}

struct ConflictChoice {
  SJoinInputSet inputs;
  SJoinInputSet operators;
  int32_t root;
  bool swapped;
  std::shared_ptr<const ConflictChoice> left;
  std::shared_ptr<const ConflictChoice> right;
};

using ConflictChoicePtr = std::shared_ptr<const ConflictChoice>;
using ConflictChoices = std::vector<ConflictChoicePtr>;

int32_t conflictBitCount(SJoinInputSet bits) {
  int32_t count = 0;
  for (; bits != 0; bits &= bits - 1) ++count;
  return count;
}

// Build the reference from physical children, independently of normalization.
ConflictChoicePtr conflictOriginalNode(const SJoinConflictModel* model,
                                        const SLogicNode* node,
                                        int32_t& nextInput) {
  auto choice = std::make_shared<ConflictChoice>();
  choice->root = -1;
  for (size_t i = 0; i < taosArrayGetSize(model->pOps); ++i) {
    const auto* op = (const SJoinConflictOp*)taosArrayGet(model->pOps, i);
    if (&op->pNode->node != node) continue;
    choice->root = (int32_t)i;
    choice->left = conflictOriginalNode(model,
        (const SLogicNode*)nodesListGetNode(node->pChildren, 0), nextInput);
    choice->right = conflictOriginalNode(model,
        (const SLogicNode*)nodesListGetNode(node->pChildren, 1), nextInput);
    choice->inputs = choice->left->inputs | choice->right->inputs;
    choice->operators = (UINT64_C(1) << i) | choice->left->operators |
                        choice->right->operators;
    return choice;
  }
  choice->inputs = UINT64_C(1) << nextInput++;
  return choice;
}

ConflictChoicePtr conflictOriginal(const SJoinConflictModel* model) {
  int32_t nextInput = 0;
  const auto* root = (const SJoinConflictOp*)taosArrayGet(model->pOps, 0);
  auto choice = conflictOriginalNode(model, &root->pNode->node, nextInput);
  EXPECT_EQ(choice->inputs, model->inputs);
  return choice;
}

ConflictChoicePtr conflictOriginalOp(const ConflictChoicePtr& choice,
                                      int32_t opId) {
  if (choice->root == opId) return choice;
  if (choice->root < 0) return {};
  auto found = conflictOriginalOp(choice->left, opId);
  return found ? found : conflictOriginalOp(choice->right, opId);
}

struct ConflictEnumerator {
  const SJoinConflictModel* model;
  // Four inputs and three operators need at most 16 * 8 distinct states.
  std::array<ConflictChoices, 128> choices;
  std::array<bool, 128> visited = {};

  explicit ConflictEnumerator(const SJoinConflictModel* value) : model(value) {}

  const ConflictChoices& enumerate(SJoinInputSet inputs,
                                    SJoinInputSet operators) {
    const size_t index = (size_t)(inputs * 8 + operators);
    auto& result = choices[index];
    if (visited[index]) return result;
    visited[index] = true;
    const int32_t count = conflictBitCount(inputs);
    if (count != conflictBitCount(operators) + 1) return result;
    if (count == 1) {
      auto leaf = std::make_shared<ConflictChoice>();
      leaf->inputs = inputs;
      leaf->root = -1;
      result.push_back(leaf);
      return result;
    }
    for (size_t op = 0; op < taosArrayGetSize(model->pOps); ++op) {
      const SJoinInputSet bit = UINT64_C(1) << op;
      if ((operators & bit) == 0) continue;
      const SJoinInputSet remaining = operators ^ bit;
      for (SJoinInputSet left = (inputs - 1) & inputs; left != 0;
           left = (left - 1) & inputs) {
        const SJoinInputSet right = inputs ^ left;
        const bool legal[2] = {
            planJoinSplitIsLegal(model, (int32_t)op, left, right, false),
            planJoinSplitIsLegal(model, (int32_t)op, left, right, true)};
        if (!legal[0] && !legal[1]) continue;
        for (SJoinInputSet used = remaining;; used = (used - 1) & remaining) {
          const auto& l = enumerate(left, used);
          const auto& r = enumerate(right, remaining ^ used);
          for (int32_t swapped = 0; swapped < 2; ++swapped) {
            if (!legal[swapped]) continue;
            for (const auto& a : l) {
              for (const auto& b : r) {
                auto choice = std::make_shared<ConflictChoice>();
                choice->inputs = inputs;
                choice->operators = operators;
                choice->root = (int32_t)op;
                choice->swapped = swapped != 0;
                choice->left = a;
                choice->right = b;
                result.push_back(choice);
              }
            }
          }
          if (used == 0) break;
        }
      }
    }
    return result;
  }
};

int32_t conflictChoiceHeight(const ConflictChoicePtr& choice) {
  if (choice->root < 0) return 0;
  const int32_t left = conflictChoiceHeight(choice->left);
  const int32_t right = conflictChoiceHeight(choice->right);
  return 1 + (left > right ? left : right);
}

int32_t minimumConflictHeight(const SJoinConflictModel* model) {
  const int32_t count = conflictBitCount(model->inputs);
  if (count < 2 || count > 4) {
    ADD_FAILURE() << "Bag enumeration supports two to four inputs";
    return -1;
  }
  ConflictEnumerator enumerator(model);
  const auto& choices = enumerator.enumerate(
      model->inputs, (UINT64_C(1) << (count - 1)) - 1);
  int32_t minimum = -1;
  for (const auto& choice : choices) {
    const int32_t height = conflictChoiceHeight(choice);
    if (minimum < 0 || height < minimum) minimum = height;
  }
  return minimum;
}

ConflictRows conflictEvaluateNode(const SJoinConflictModel* model,
                                  const ConflictChoicePtr& choice,
                                  const std::array<ConflictBag, 4>& tables) {
  if (choice->root < 0) {
    for (int32_t i = 0; i < 4; ++i) {
      if (choice->inputs == (UINT64_C(1) << i)) {
        return {tables[i], choice->inputs};
      }
    }
    ADD_FAILURE() << "Expected one input bit";
    return {{}, 0};
  }
  const auto* op = (const SJoinConflictOp*)
      taosArrayGet(model->pOps, choice->root);
  auto rows = conflictBagJoin(op->pNode, choice->swapped,
                              conflictEvaluateNode(model, choice->left, tables),
                              conflictEvaluateNode(model, choice->right,
                                                     tables));
  // Root filtering belongs to the complete region output, independently of
  // where the original root operator appears in the candidate.
  if (choice->root != 0) {
    rows.rows = conflictFilter(rows.rows, op->pNode->node.pConditions);
  }
  return rows;
}

std::multiset<ConflictRow> conflictEvaluate(
    const SJoinConflictModel* model, const ConflictChoicePtr& choice,
    const std::array<ConflictBag, 4>& tables, SJoinInputSet visible) {
  const auto rows = conflictEvaluateNode(model, choice, tables);
  const auto filtered = conflictFilter(rows.rows, model->pRootFilter);
  std::multiset<ConflictRow> projected;
  for (auto row : filtered) {
    for (int32_t i = 0; i < 4; ++i) {
      if (!planJoinInputSetContains(visible, i)) row[i] = 2;
    }
    projected.insert(row);
  }
  return projected;
}

bool checkConflictSamples(const SJoinConflictModel* model,
                            const ConflictChoicePtr& original,
                            const ConflictChoices& choices,
                            SJoinInputSet visible,
                            std::array<ConflictBag, 4>& tables,
                            int32_t input) {
  const int32_t count = conflictBitCount(model->inputs);
  if (input == count) {
    const auto expected = conflictEvaluate(model, original, tables, visible);
    for (const auto& choice : choices) {
      const auto actual = conflictEvaluate(model, choice, tables, visible);
      if (actual == expected) continue;
      EXPECT_EQ(actual, expected)
          << "root=" << choice->root << " left=" << choice->left->inputs
          << " right=" << choice->right->inputs
          << " swapped=" << choice->swapped;
      return false;
    }
    return true;
  }
  const std::vector<std::vector<int32_t>> values = count == 3
      ? std::vector<std::vector<int32_t>>{
            {}, {0}, {1}, {2}, {0, 0}, {0, 1}, {0, 2}}
      : std::vector<std::vector<int32_t>>{
            {}, {0}, {1}, {2}, {0, 0}, {0, 2}};
  for (const auto& bag : values) {
    tables[input].clear();
    for (int32_t value : bag) {
      ConflictRow row = {2, 2, 2, 2};
      row[input] = value;
      tables[input].push_back(row);
    }
    if (!checkConflictSamples(model, original, choices, visible, tables,
                               input + 1)) return false;
  }
  return true;
}

void checkConflictBags(const SJoinConflictModel* model,
                         SJoinInputSet visible) {
  const int32_t count = conflictBitCount(model->inputs);
  ASSERT_GE(count, 2);
  ASSERT_LE(count, 4);
  ASSERT_EQ(taosArrayGetSize(model->pOps), (size_t)count - 1);
  const auto original = conflictOriginal(model);
  ConflictEnumerator enumerator(model);
  const auto& choices = enumerator.enumerate(
      model->inputs, (UINT64_C(1) << (count - 1)) - 1);
  ASSERT_FALSE(choices.empty());
  std::array<ConflictBag, 4> tables;
  ASSERT_TRUE(checkConflictSamples(model, original, choices, visible, tables,
                                    0));
}

ConflictChoicePtr conflictFromDp(const SJoinTreeChoice* tree,
                                  int32_t cellId, int32_t depth = 0) {
  const auto* cell = planJoinTreeCell(tree, cellId);
  if (cell == nullptr || !cell->valid || depth >= 4) {
    ADD_FAILURE() << "Invalid bag DP cell";
    return {};
  }
  auto choice = std::make_shared<ConflictChoice>();
  choice->inputs = cell->inputs;
  choice->operators = cell->usedOps;
  choice->root = cell->inputId >= 0 ? -1 : cell->opId;
  choice->swapped = cell->swapped;
  if (cell->inputId >= 0) {
    if (cell->inputId >= 4 || cell->usedOps != 0 ||
        cell->inputs != (UINT64_C(1) << cell->inputId)) {
      ADD_FAILURE() << "Invalid bag DP leaf";
      return {};
    }
    return choice;
  }
  if (cell->opId < 0 ||
      (size_t)cell->opId >= taosArrayGetSize(tree->pConflicts->pOps)) {
    ADD_FAILURE() << "Expected a mixed-region DP operator";
    return {};
  }
  choice->left = conflictFromDp(tree, cell->left, depth + 1);
  choice->right = conflictFromDp(tree, cell->right, depth + 1);
  if (!choice->left || !choice->right) return {};
  const auto& l = *choice->left;
  const auto& r = *choice->right;
  const uint32_t root = UINT32_C(1) << cell->opId;
  if ((l.inputs & r.inputs) != 0 ||
      (l.inputs | r.inputs) != choice->inputs ||
      (l.operators & r.operators) != 0 ||
      ((l.operators | r.operators) & root) != 0 ||
      (l.operators | r.operators | root) != choice->operators) {
    ADD_FAILURE() << "DP bag choice repeats an input or operator";
    return {};
  }
  EXPECT_TRUE(planJoinSplitIsLegal(tree->pConflicts, cell->opId, l.inputs,
                                   r.inputs, cell->swapped));
  return choice;
}

void checkJoinChoiceBags(const SJoinTreeChoice* tree,
                          SJoinInputSet visible = 0) {
  ASSERT_NE(tree, nullptr);
  ASSERT_NE(tree->pConflicts, nullptr);
  const auto* model = tree->pConflicts;
  const int32_t count = conflictBitCount(model->inputs);
  ASSERT_GE(count, 2);
  ASSERT_LE(count, 4);
  const auto candidate = conflictFromDp(tree, tree->rootCell);
  ASSERT_NE(candidate, nullptr);
  ASSERT_EQ(candidate->inputs, model->inputs);
  ASSERT_EQ(candidate->operators, (UINT64_C(1) << (count - 1)) - 1);
  EXPECT_EQ(tree->pRootFilter, model->pRootFilter);
  const auto original = conflictOriginal(model);
  const ConflictChoices choices = {candidate};
  std::array<ConflictBag, 4> tables;
  if (visible == 0) visible = model->inputs;
  ASSERT_TRUE(checkConflictSamples(model, original, choices, visible,
                                    tables, 0));
}

void checkConflictFilterSample(const SJoinConflictModel* model) {
  std::array<ConflictBag, 4> tables;
  tables[0] = {{0, 2, 2, 2}, {1, 2, 2, 2}};
  const auto original = conflictOriginal(model);
  const std::multiset<ConflictRow> expected = {{1, 2, 2, 2}};
  EXPECT_EQ(conflictEvaluate(model, original, tables, model->inputs), expected);
}

struct PureBagPredicate {
  SJoinInputSet inputs;
  NodePtr expression;
};

EDealRes remapPureBagColumn(SNode** node, void* context) {
  if (nodeType(*node) != QUERY_NODE_COLUMN) return DEAL_RES_CONTINUE;
  auto* column = (SColumnNode*)*node;
  const auto& mapping = *(const std::array<int32_t, 8>*)context;
  const int32_t global = column->tableAlias[0] - 'a';
  if (global < 0 || global >= 8 || column->tableAlias[1] != '\0' ||
      mapping[global] < 0) {
    ADD_FAILURE() << "Pure predicate escapes its active input set";
    return DEAL_RES_ERROR;
  }
  column->tableAlias[0] = 'a' + mapping[global];
  return DEAL_RES_CONTINUE;
}

NodePtr pureBagExpression(const SNode* expression,
                            const std::array<int32_t, 8>& mapping) {
  SNode* copy = nullptr;
  EXPECT_EQ(nodesCloneNode(expression, &copy), TSDB_CODE_SUCCESS);
  if (copy != nullptr) {
    nodesRewriteExpr(&copy, remapPureBagColumn, (void*)&mapping);
  }
  return NodePtr(copy, nodesDestroyNode);
}

ConflictBag pureBagProduct(const ConflictBag& left, const ConflictBag& right,
                             SJoinInputSet rightInputs) {
  ConflictBag result;
  for (const auto& l : left) {
    for (const auto& r : right) {
      auto row = l;
      for (int32_t i = 0; i < 4; ++i) {
        if (planJoinInputSetContains(rightInputs, i)) row[i] = r[i];
      }
      result.push_back(row);
    }
  }
  return result;
}

SJoinInputSet pureBagLocalInputs(SJoinInputSet global,
                                   const std::array<int32_t, 8>& mapping) {
  SJoinInputSet result = 0;
  for (int32_t i = 0; i < 8; ++i) {
    if (mapping[i] >= 0 && planJoinInputSetContains(global, i)) {
      result |= UINT64_C(1) << mapping[i];
    }
  }
  return result;
}

ConflictBag pureBagEvaluate(const SJoinTreeChoice* pure, int32_t cellId,
                              const std::array<ConflictBag, 4>& tables,
                              const std::array<int32_t, 8>& mapping,
                              const std::vector<PureBagPredicate>& clauses) {
  const auto* cell = planJoinTreeCell(pure, cellId);
  if (cell == nullptr) return {};
  ConflictBag rows;
  SJoinInputSet available = cell->inputs;
  SJoinInputSet left = 0;
  SJoinInputSet right = 0;
  if (cell->inputId >= 0) {
    rows = tables[mapping[cell->inputId]];
  } else {
    left = planJoinTreeCell(pure, cell->left)->inputs;
    right = planJoinTreeCell(pure, cell->right)->inputs;
    rows = pureBagProduct(
        pureBagEvaluate(pure, cell->left, tables, mapping, clauses),
        pureBagEvaluate(pure, cell->right, tables, mapping, clauses),
        pureBagLocalInputs(right, mapping));
  }
  for (const auto& clause : clauses) {
    if ((clause.inputs & available) != clause.inputs) continue;
    if (cell->inputId < 0 &&
        ((clause.inputs & left) == clause.inputs ||
         (clause.inputs & right) == clause.inputs)) continue;
    rows = conflictFilter(rows, clause.expression.get());
  }
  return rows;
}

// This checks the selected pure view, before any logical-tree construction.
// The reference evaluates original U's whole ON clauses over a cartesian bag;
// the candidate evaluates its split predicate view at available DP cells.
void checkNestedPureBags(const SJoinTreeChoice* mixed,
                           const SJoinTreeChoice* pure) {
  const auto* root = planJoinTreeCell(pure, pure->rootCell);
  ASSERT_NE(root, nullptr);
  ASSERT_EQ(conflictBitCount(root->inputs), 4);
  std::array<int32_t, 8> mapping;
  mapping.fill(-1);
  int32_t local = 0;
  for (int32_t i = 0; i < 8; ++i) {
    if (planJoinInputSetContains(root->inputs, i)) mapping[i] = local++;
  }
  std::vector<PureBagPredicate> clauses;
  for (size_t i = 0; i < taosArrayGetSize(pure->pPredicates); ++i) {
    const auto* pred = (const SJoinParallelPredicate*)
        taosArrayGet(pure->pPredicates, i);
    ASSERT_NE(pred->source, JOIN_PREDICATE_FILTER);
    clauses.push_back({pred->inputs, pureBagExpression(pred->pExpr, mapping)});
  }
  uint32_t usedOps = 0;
  for (size_t i = 0; i < taosArrayGetSize(mixed->pCells); ++i) {
    const auto* cell = planJoinTreeCell(mixed, (int32_t)i);
    if (cell->pPureChoice == pure) usedOps = cell->usedOps;
  }
  ASSERT_NE(usedOps, 0U);
  for (size_t i = 0; i < taosArrayGetSize(pure->pPredicates); ++i) {
    const auto* pred = (const SJoinParallelPredicate*)
        taosArrayGet(pure->pPredicates, i);
    ASSERT_GE(pred->sourceOpId, 0);
    EXPECT_NE(usedOps & (1U << pred->sourceOpId), 0U);
  }
  std::vector<NodePtr> original;
  for (size_t i = 0; i < taosArrayGetSize(mixed->pConflicts->pOps); ++i) {
    if ((usedOps & (1U << i)) == 0) continue;
    const auto* op = (const SJoinConflictOp*)
        taosArrayGet(mixed->pConflicts->pOps, i);
    for (const auto* expression : {op->pNode->pFullOnCond,
                                    op->pNode->pPrimKeyEqCond}) {
      if (expression != nullptr) {
        original.push_back(pureBagExpression(expression, mapping));
      }
    }
  }
  const std::vector<std::vector<int32_t>> samples = {
      {}, {0}, {1}, {2}, {0, 0}, {0, 2}};
  std::array<ConflictBag, 4> tables;
  std::function<void(int32_t)> check = [&](int32_t input) {
    if (input == 4) {
      ConflictBag expected = {{2, 2, 2, 2}};
      for (int32_t i = 0; i < 4; ++i) {
        expected = pureBagProduct(expected, tables[i], UINT64_C(1) << i);
      }
      for (const auto& expression : original) {
        expected = conflictFilter(expected, expression.get());
      }
      const auto actual = pureBagEvaluate(pure, pure->rootCell, tables,
                                           mapping, clauses);
      EXPECT_EQ(std::multiset<ConflictRow>(actual.begin(), actual.end()),
                std::multiset<ConflictRow>(expected.begin(), expected.end()));
      return;
    }
    for (const auto& values : samples) {
      tables[input].clear();
      for (int32_t value : values) {
        ConflictRow row = {2, 2, 2, 2};
        row[input] = value;
        tables[input].push_back(row);
      }
      check(input + 1);
    }
  };
  check(0);
}

ConflictRows evaluateBuiltJoin(const SLogicNode* node,
                                 const std::array<ConflictBag, 4>& tables) {
  ConflictRows result = {{}, 0};
  if (nodeType(node) == QUERY_NODE_LOGIC_PLAN_JOIN) {
    const auto* join = (const SJoinLogicNode*)node;
    result = conflictBagJoin(join, false,
        evaluateBuiltJoin((const SLogicNode*)
                              nodesListGetNode(node->pChildren, 0), tables),
        evaluateBuiltJoin((const SLogicNode*)
                              nodesListGetNode(node->pChildren, 1), tables));
  } else if (LIST_LENGTH(node->pChildren) == 1) {
    result = evaluateBuiltJoin(
        (const SLogicNode*)nodesListGetNode(node->pChildren, 0), tables);
  } else {
    SNode* target = nullptr;
    FOREACH(target, node->pTargets) {
      if (nodeType(target) != QUERY_NODE_COLUMN) continue;
      const auto* column = (const SColumnNode*)target;
      const int32_t input = column->tableAlias[0] - 'a';
      if (input < 0 || input >= 4 || column->tableAlias[1] != '\0') continue;
      result = {tables[input], UINT64_C(1) << input};
      break;
    }
    EXPECT_NE(result.inputs, 0U);
  }
  result.rows = conflictFilter(result.rows, node->pConditions);
  return result;
}

// Read the constructed logical tree itself, including derived equalities and
// filters at every level. No DP cells or conflict-model operator ids are used.
void checkBuiltJoinBags(const SLogicNode* original,
                          const SLogicNode* built) {
  const std::vector<std::vector<int32_t>> samples = {
      {}, {0}, {1}, {2}, {0, 0}, {0, 2}};
  std::array<ConflictBag, 4> tables;
  std::function<void(int32_t)> check = [&](int32_t input) {
    if (input == 4) {
      const auto expected = evaluateBuiltJoin(original, tables);
      const auto actual = evaluateBuiltJoin(built, tables);
      EXPECT_EQ(actual.inputs, expected.inputs);
      EXPECT_EQ(std::multiset<ConflictRow>(actual.rows.begin(),
                                             actual.rows.end()),
                std::multiset<ConflictRow>(expected.rows.begin(),
                                             expected.rows.end()));
      return;
    }
    for (const auto& values : samples) {
      tables[input].clear();
      for (int32_t value : values) {
        ConflictRow row = {2, 2, 2, 2};
        row[input] = value;
        tables[input].push_back(row);
      }
      check(input + 1);
    }
  };
  check(0);
}

// Keep ts and c3 independent: a global class may connect them only above a
// child join. Row ids preserve duplicate multiplicities in the comparison.
void checkBuiltTwoColumnBags(const SLogicNode* original,
                               const SLogicNode* built) {
  using Sample = std::array<int32_t, 2>;
  const std::vector<Sample> samples = {
      {0, 0}, {0, 1}, {0, 2}, {0, 0}, {1, 0}};
  struct Values {
    const std::vector<Sample>* samples;
    const ConflictRow* row;
  };
  auto accepts = [&](const SNode* expression, const ConflictRow& row) {
    if (expression == nullptr) return true;
    SNode* copy = nullptr;
    if (nodesCloneNode(expression, &copy) != 0) {
      ADD_FAILURE() << "Failed to clone two-column bag condition";
      return false;
    }
    Values values = {&samples, &row};
    nodesRewriteExpr(&copy, [](SNode** node, void* context) {
      if (nodeType(*node) != QUERY_NODE_COLUMN) return DEAL_RES_CONTINUE;
      const auto* column = (const SColumnNode*)*node;
      const int32_t input = column->tableAlias[0] - 'a';
      const int32_t key = strcmp(column->colName, "ts") == 0 ? 0 : 1;
      const auto* values = (const Values*)context;
      if (input < 0 || input >= 4 || column->tableAlias[1] != '\0' ||
          (key == 1 && strcmp(column->colName, "c3") != 0)) {
        ADD_FAILURE() << "Unexpected two-column bag key";
        return DEAL_RES_ERROR;
      }
      const int32_t value = (*values->samples)[(*values->row)[input]][key];
      SNode* literal = nullptr;
      if (nodesMakeValueNodeFromInt32(value, &literal) != 0) {
        ADD_FAILURE() << "Failed to allocate two-column bag value";
        return DEAL_RES_ERROR;
      }
      ((SValueNode*)literal)->isNull = value == 2;
      nodesDestroyNode(*node);
      *node = literal;
      return DEAL_RES_CONTINUE;
    }, &values);
    NodePtr condition(copy, nodesDestroyNode);
    return conflictValue(condition.get(), row) == 1;
  };
  for (int32_t empty = -1; empty < 4; ++empty) {
    std::function<ConflictBag(const SLogicNode*)> evaluate =
        [&](const SLogicNode* node) -> ConflictBag {
      ConflictBag rows;
      if (nodeType(node) == QUERY_NODE_LOGIC_PLAN_JOIN) {
        const auto* join = (const SJoinLogicNode*)node;
        EXPECT_EQ(join->joinType, JOIN_TYPE_INNER);
        const auto left = evaluate((const SLogicNode*)
            nodesListGetNode(node->pChildren, 0));
        const auto right = evaluate((const SLogicNode*)
            nodesListGetNode(node->pChildren, 1));
        for (const auto& l : left) {
          for (const auto& r : right) {
            ConflictRow row;
            for (int32_t i = 0; i < 4; ++i) row[i] = l[i] < 0 ? r[i] : l[i];
            if (accepts(join->pFullOnCond, row) &&
                accepts(join->pPrimKeyEqCond, row) &&
                accepts(join->addPrimEqCond, row)) rows.push_back(row);
          }
        }
      } else if (LIST_LENGTH(node->pChildren) == 1) {
        rows = evaluate((const SLogicNode*)
            nodesListGetNode(node->pChildren, 0));
      } else {
        const auto* column = (const SColumnNode*)
            nodesListGetNode(node->pTargets, 0);
        const int32_t input = column->tableAlias[0] - 'a';
        EXPECT_GE(input, 0);
        EXPECT_LT(input, 4);
        if (input == empty || input < 0 || input >= 4) return rows;
        for (size_t i = 0; i < samples.size(); ++i) {
          ConflictRow row = {-1, -1, -1, -1};
          row[input] = (int32_t)i;
          rows.push_back(row);
        }
      }
      rows.erase(std::remove_if(rows.begin(), rows.end(),
          [&](const ConflictRow& row) {
            return !accepts(node->pConditions, row);
          }), rows.end());
      return rows;
    };
    const auto expected = evaluate(original);
    const auto actual = evaluate(built);
    EXPECT_EQ(std::multiset<ConflictRow>(actual.begin(), actual.end()),
              std::multiset<ConflictRow>(expected.begin(), expected.end()));
  }
}

bool hasBuiltDerivedEquality(const SLogicNode* node) {
  bool found = false;
  if (nodeType(node) == QUERY_NODE_LOGIC_PLAN_JOIN) {
    nodesWalkExpr(((const SJoinLogicNode*)node)->pFullOnCond,
        [](SNode* expression, void* context) {
          if (nodeType(expression) != QUERY_NODE_OPERATOR) {
            return DEAL_RES_CONTINUE;
          }
          const auto* op = (const SOperatorNode*)expression;
          if (op->opType != OP_TYPE_EQUAL || op->pLeft == nullptr ||
              op->pRight == nullptr ||
              nodeType(op->pLeft) != QUERY_NODE_COLUMN ||
              nodeType(op->pRight) != QUERY_NODE_COLUMN) {
            return DEAL_RES_CONTINUE;
          }
          const auto* left = (const SColumnNode*)op->pLeft;
          const auto* right = (const SColumnNode*)op->pRight;
          if (left->tableAlias[0] != 'a' && right->tableAlias[0] != 'a' &&
              strcmp(left->tableAlias, right->tableAlias) != 0) {
            *(bool*)context = true;
          }
          return DEAL_RES_CONTINUE;
        }, &found);
  }
  SNode* child = nullptr;
  FOREACH(child, node->pChildren) {
    found |= hasBuiltDerivedEquality((const SLogicNode*)child);
  }
  return found;
}

void checkBuiltEqualityCount(const SLogicNode* root, const char* column,
                               int32_t expected) {
  std::set<const SLogicNode*> nodes;
  collectBuiltNodes(root, nodes);
  for (const auto* node : nodes) {
    if (nodeType(node) != QUERY_NODE_LOGIC_PLAN_JOIN) continue;
    const auto* join = (const SJoinLogicNode*)node;
    if (join->joinType != JOIN_TYPE_INNER ||
        join->subType != JOIN_STYPE_NONE) continue;
    struct Count {
      const char* column;
      int32_t value;
    } count = {column, 0};
    for (auto* condition : {join->pFullOnCond, join->pPrimKeyEqCond,
                              join->addPrimEqCond}) {
      nodesWalkExpr(condition, [](SNode* expression, void* context) {
        if (nodeType(expression) != QUERY_NODE_OPERATOR) {
          return DEAL_RES_CONTINUE;
        }
        const auto* op = (const SOperatorNode*)expression;
        if (op->opType != OP_TYPE_EQUAL || op->pLeft == nullptr ||
            op->pRight == nullptr ||
            nodeType(op->pLeft) != QUERY_NODE_COLUMN ||
            nodeType(op->pRight) != QUERY_NODE_COLUMN) {
          return DEAL_RES_CONTINUE;
        }
        auto* count = (Count*)context;
        const auto* left = (const SColumnNode*)op->pLeft;
        const auto* right = (const SColumnNode*)op->pRight;
        if (strcmp(left->colName, count->column) == 0 &&
            strcmp(right->colName, count->column) == 0) ++count->value;
        return DEAL_RES_CONTINUE;
      }, &count);
    }
    EXPECT_EQ(count.value, expected) << nodeText((const SNode*)node);
  }
}

bool hasBuiltColumnEquality(const SLogicNode* root, const std::string& left,
                              const std::string& right) {
  struct Equality {
    const std::string* left;
    const std::string* right;
    bool found;
  } equality = {&left, &right, false};
  std::set<const SLogicNode*> nodes;
  collectBuiltNodes(root, nodes);
  for (const auto* node : nodes) {
    std::vector<SNode*> conditions = {node->pConditions};
    if (nodeType(node) == QUERY_NODE_LOGIC_PLAN_JOIN) {
      const auto* join = (const SJoinLogicNode*)node;
      conditions.insert(conditions.end(), {join->pFullOnCond,
          join->pPrimKeyEqCond, join->addPrimEqCond});
    }
    for (auto* condition : conditions) {
      nodesWalkExpr(condition, [](SNode* expression, void* context) {
        if (nodeType(expression) != QUERY_NODE_OPERATOR) {
          return DEAL_RES_CONTINUE;
        }
        const auto* op = (const SOperatorNode*)expression;
        if (op->opType != OP_TYPE_EQUAL || op->pLeft == nullptr ||
            op->pRight == nullptr ||
            nodeType(op->pLeft) != QUERY_NODE_COLUMN ||
            nodeType(op->pRight) != QUERY_NODE_COLUMN) {
          return DEAL_RES_CONTINUE;
        }
        const auto* l = (const SColumnNode*)op->pLeft;
        const auto* r = (const SColumnNode*)op->pRight;
        const auto left = std::string(l->tableAlias) + "." + l->colName;
        const auto right = std::string(r->tableAlias) + "." + r->colName;
        auto* equality = (Equality*)context;
        equality->found |=
            (left == *equality->left && right == *equality->right) ||
            (left == *equality->right && right == *equality->left);
        return DEAL_RES_CONTINUE;
      }, &equality);
    }
  }
  return equality.found;
}

void checkPhysicalJoinAlgorithms(const SQueryPlan* plan, int32_t merges,
                                   int32_t hashes) {
  int32_t actualMerges = 0;
  int32_t actualHashes = 0;
  std::function<void(const SPhysiNode*)> inspect =
      [&](const SPhysiNode* node) {
        actualMerges += nodeType(node) == QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN;
        actualHashes += nodeType(node) == QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN;
        SNode* child = nullptr;
        FOREACH(child, node->pChildren) {
          inspect((const SPhysiNode*)child);
        }
      };
  SNode* level = nullptr;
  FOREACH(level, plan->pSubplans) {
    SNode* subplan = nullptr;
    FOREACH(subplan, ((const SNodeListNode*)level)->pNodeList) {
      inspect(((const SSubplan*)subplan)->pNode);
    }
  }
  EXPECT_EQ(actualMerges, merges);
  EXPECT_EQ(actualHashes, hashes);
}

int32_t splitJoinCount(const SLogicNode* node) {
  int32_t count = nodeType(node) == QUERY_NODE_LOGIC_PLAN_JOIN;
  SNode* child = nullptr;
  FOREACH(child, node->pChildren) {
    count += splitJoinCount((const SLogicNode*)child);
  }
  return count;
}

bool failedSplitProducerAppend = false;

int32_t failSplitProducerAppend(SNodeList** list, SNode* node) {
  if (node != nullptr && nodeType(node) == QUERY_NODE_LOGIC_SUBPLAN) {
    const auto* producer = (const SLogicSubplan*)node;
    if (producer->pNode != nullptr && splitJoinCount(producer->pNode) > 0) {
      failedSplitProducerAppend = true;
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return terrno;
    }
  }
  if (*list == nullptr) {
    const int32_t code = nodesMakeList(list);
    if (code != 0) return code;
  }
  return nodesListAppend(*list, node);
}

SLogicSubplan* installSplitDependency(SLogicSubplan* root, SLogicNode* input,
                                       int32_t group) {
  SLogicNode* parent = input->pParent;
  if (parent == nullptr) return nullptr;
  SListCell* cell = parent->pChildren->pHead;
  for (; cell != nullptr && cell->pNode != (SNode*)input;
         cell = cell->pNext) {}
  if (cell == nullptr) return nullptr;
  SNode* node = nullptr;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_EXCHANGE, &node), 0);
  if (node == nullptr) return nullptr;
  NodePtr exchangeOwner(node, nodesDestroyNode);
  auto* exchange = (SExchangeLogicNode*)node;
  exchange->srcStartGroupId = exchange->srcEndGroupId = group;
  exchange->node.precision = input->precision;
  exchange->node.inputTsOrder = input->inputTsOrder;
  exchange->node.outputTsOrder = input->outputTsOrder;
  exchange->node.requireDataOrder = input->requireDataOrder;
  exchange->node.resultDataOrder = input->resultDataOrder;
  if (nodesCloneList(input->pTargets, &exchange->node.pTargets) != 0) {
    ADD_FAILURE() << "Cannot clone existing Exchange targets";
    return nullptr;
  }
  SNode* holder = nullptr;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_LOGIC_SUBPLAN, &holder), 0);
  if (holder == nullptr) return nullptr;
  NodePtr producerOwner(holder, nodesDestroyNode);
  auto* producer = (SLogicSubplan*)holder;
  producer->id.groupId = group;
  producer->subplanType = nodeType(input) == QUERY_NODE_LOGIC_PLAN_SCAN
                             ? SUBPLAN_TYPE_SCAN : SUBPLAN_TYPE_MERGE;
  producer->pNode = input;
  cell->pNode = exchangeOwner.release();
  exchange->node.pParent = parent;
  input->pParent = nullptr;
  if (nodeType(input) == QUERY_NODE_LOGIC_PLAN_SCAN) {
    input->splitDone = true;
    std::swap(producer->pVgroupList, ((SScanLogicNode*)input)->pVgroupList);
  }
  if (nodesListMakeAppend(&root->pChildren, holder) != 0) {
    ADD_FAILURE() << "Cannot attach existing Exchange producer";
    return nullptr;
  }
  producerOwner.release();
  return producer;
}

void collectJoinSplitSubplans(const SLogicSubplan* subplan,
                                std::vector<const SLogicSubplan*>& plans) {
  if (std::find(plans.begin(), plans.end(), subplan) != plans.end()) return;
  plans.push_back(subplan);
  SNode* child = nullptr;
  FOREACH(child, subplan->pChildren) {
    collectJoinSplitSubplans((const SLogicSubplan*)child, plans);
  }
}

void checkCompleteJoinSplit(SLogicSubplan* root, int32_t inputs) {
  std::vector<const SLogicSubplan*> plans;
  collectJoinSplitSubplans(root, plans);
  const SJoinLogicNode* join = nullptr;
  for (const auto* plan : plans) {
    join = firstJoin(plan->pNode);
    if (join == nullptr) continue;
    EXPECT_EQ(plan->subplanType, SUBPLAN_TYPE_MERGE);
    break;
  }
  ASSERT_NE(join, nullptr);
  ASSERT_EQ(LIST_LENGTH(join->node.pChildren), 2);
  std::set<int32_t> groups;
  int32_t joins = 0;
  for (const auto* plan : plans) {
    EXPECT_TRUE(groups.insert(plan->id.groupId).second);
    const int32_t local = splitJoinCount(plan->pNode);
    EXPECT_LE(local, 1);
    joins += local;
  }
  EXPECT_EQ(joins, inputs - 1);
  std::set<int32_t> producers;
  SNode* child = nullptr;
  FOREACH(child, join->node.pChildren) {
    ASSERT_EQ(nodeType(child), QUERY_NODE_LOGIC_PLAN_EXCHANGE);
    const auto* exchange = (const SExchangeLogicNode*)child;
    EXPECT_EQ(exchange->srcStartGroupId, exchange->srcEndGroupId);
    EXPECT_TRUE(producers.insert(exchange->srcStartGroupId).second);
    const SLogicSubplan* producer = nullptr;
    for (const auto* plan : plans) {
      if (plan->id.groupId == exchange->srcStartGroupId) producer = plan;
    }
    ASSERT_NE(producer, nullptr);
    EXPECT_EQ(splitJoinCount(producer->pNode), 1);
  }
}

void checkPhysicalJoinCopies(const SQueryPlan* plan, int32_t expected) {
  int32_t joins = 0;
  std::set<int32_t> groups;
  SNode* level = nullptr;
  FOREACH(level, plan->pSubplans) {
    SNode* node = nullptr;
    FOREACH(node, ((const SNodeListNode*)level)->pNodeList) {
      const auto* subplan = (const SSubplan*)node;
      std::function<void(const SPhysiNode*)> inspect =
          [&](const SPhysiNode* physical) {
            if (nodeType(physical) == QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN ||
                nodeType(physical) == QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN) {
              ++joins;
              EXPECT_TRUE(groups.insert(subplan->id.groupId).second);
            }
            SNode* child = nullptr;
            FOREACH(child, physical->pChildren) {
              inspect((const SPhysiNode*)child);
            }
          };
      inspect(subplan->pNode);
    }
  }
  EXPECT_EQ(joins, expected);
}

}  // namespace

using EqCheck = std::function<void(const SJoinParallelRegion*,
                                   const SJoinEqClasses*)>;

class PlanJoinParallelTest : public PlannerTestBase {
 protected:
  // Check the classes of the created plan's only JOIN region.
  void runEqClasses(const std::string& sql, EqCheck check) {
    setJoinPropsCheck([check](const SPlanContext* context,
                              const SLogicNode* root, bool optimized) {
      if (optimized) return;
      auto model = prepareJoinModel(context, root);
      ASSERT_NE(model, nullptr);
      ASSERT_EQ(taosArrayGetSize(model->pRegions), 1U);
      const auto* region = regionAt(model.get(), 0);
      auto classes = buildEqClasses(model.get(), region);
      ASSERT_NE(classes, nullptr);
      check(region, classes.get());
    });
    run(sql);
  }

  void runNullRejection(const std::string& condition, SJoinInputSet expected,
                        bool scalarCheck = false) {
    setJoinPropsCheck([expected, scalarCheck](const SPlanContext* context,
                                             const SLogicNode* root,
                                             bool optimized) {
      if (optimized) return;
      const std::string before = nodeText((const SNode*)root);
      auto model = prepareJoinModel(context, root);
      ASSERT_NE(model, nullptr);
      ASSERT_EQ(taosArrayGetSize(model->pRegions), 1U);
      const auto* region = regionAt(model.get(), 0);
      ASSERT_TRUE(region->supported);
      ASSERT_EQ(taosArrayGetSize(region->pInputs), 3U);
      const auto* join = firstJoin(root);
      ASSERT_NE(join, nullptr);
      ASSERT_NE(join->pFullOnCond, nullptr);
      const auto* props = planGetJoinInputProps(context, &join->node);
      ASSERT_NE(props, nullptr);
      EXPECT_EQ(planJoinCondRejectsNull(region, props, join->pFullOnCond),
                expected) << nodeText(join->pFullOnCond);
      if (scalarCheck) {
        checkNullRejectionWithScalar(join->pFullOnCond, expected);
      }
      EXPECT_EQ(nodeText((const SNode*)root), before);
    });
    run("SELECT a.c1 FROM (t1 a LEFT JOIN t1 b ON a.c1=b.c1) "
        "LEFT JOIN t1 c ON " + condition);
  }

  void runConflicts(const std::string& sql,
                     const std::function<void(const SJoinConflictModel*)>&
                         check) {
    setJoinPropsCheck([check](const SPlanContext* context,
                              const SLogicNode* root, bool optimized) {
      if (optimized) return;
      const std::string before = nodeText((const SNode*)root);
      auto model = prepareJoinModel(context, root);
      ASSERT_NE(model, nullptr);
      ASSERT_EQ(taosArrayGetSize(model->pRegions), 1U);
      const auto* region = regionAt(model.get(), 0);
      SJoinConflictModel* raw = nullptr;
      ASSERT_EQ(planBuildJoinConflicts(model.get(), region, &raw), 0);
      ConflictPtr conflicts(raw, planDestroyJoinConflicts);
      ASSERT_NE(conflicts, nullptr);
      ASSERT_EQ(taosArrayGetSize(conflicts->pOps),
                taosArrayGetSize(region->pOps));
      const auto original = conflictOriginal(conflicts.get());
      for (size_t i = 0; i < taosArrayGetSize(conflicts->pOps); ++i) {
        const auto op = conflictOriginalOp(original, (int32_t)i);
        ASSERT_NE(op, nullptr);
        EXPECT_TRUE(planJoinSplitIsLegal(conflicts.get(), (int32_t)i,
                                         op->left->inputs,
                                         op->right->inputs, false));
        EXPECT_TRUE(planJoinSplitIsLegal(conflicts.get(), (int32_t)i,
                                         op->right->inputs,
                                         op->left->inputs, true));
      }
      check(conflicts.get());
      EXPECT_EQ(nodeText((const SNode*)root), before);
    });
    run(sql);
  }

  void runJoinSearch(const std::string& sql, int32_t height,
                      const std::function<void(const SJoinTreeChoice*)>&
                          check = {}) {
    setJoinPropsCheck([height, check](const SPlanContext* context,
                                      const SLogicNode* root, bool optimized) {
      if (optimized) return;
      const auto before = nodeText((const SNode*)root);
      auto model = prepareJoinModel(context, root);
      ASSERT_NE(model, nullptr);
      const auto* region = regionFor(model.get(), &firstJoin(root)->node);
      ASSERT_NE(region, nullptr);
      SJoinTreeChoice* raw = nullptr;
      ASSERT_EQ(planSearchJoinTree(model.get(), region, &raw), 0);
      ChoicePtr choice(raw, planDestroyJoinTreeChoice);
      if (height == 0) {
        EXPECT_EQ(choice, nullptr);
      } else {
        ASSERT_NE(choice, nullptr);
        const auto* chosen = planJoinTreeCell(choice.get(), choice->rootCell);
        ASSERT_NE(chosen, nullptr);
        EXPECT_EQ(chosen->height, height);
        EXPECT_LT(height, choice->originalHeight);
        std::set<int32_t> inputs;
        std::set<int32_t> ops;
        checkJoinChoice(choice.get(), choice->rootCell, inputs, ops);
        const size_t count = taosArrayGetSize(region->pInputs);
        EXPECT_EQ(inputs.size(), count);
        EXPECT_EQ(ops.size(), region->mode == JOIN_REGION_MIXED ? count - 1
                                                               : 0);
        EXPECT_EQ(chosen->inputs, (UINT64_C(1) << count) - 1);
        if (check) check(choice.get());
        SJoinTreeChoice* other = nullptr;
        ASSERT_EQ(planSearchJoinTree(model.get(), region, &other), 0);
        ChoicePtr again(other, planDestroyJoinTreeChoice);
        ASSERT_NE(again, nullptr);
        EXPECT_EQ(joinChoiceText(choice.get(), choice->rootCell),
                  joinChoiceText(again.get(), again->rootCell));
      }
      EXPECT_EQ(nodeText((const SNode*)root), before);
    });
    run(sql);
  }

  void runBuildJoinTree(const std::string& sql, int32_t height,
      const std::function<void(const SLogicNode*, const SLogicNode*)>&
          check = {},
      const std::function<void(const SLogicNode*)>& checkOptimized = {}) {
    setJoinPropsCheck([height, check, checkOptimized](
                         const SPlanContext* context, const SLogicNode* root,
                         bool optimized) {
      if (optimized) {
        if (checkOptimized) checkOptimized(root);
        return;
      }
      const auto before = nodeText((const SNode*)root);
      auto model = prepareJoinModel(context, root);
      ASSERT_NE(model, nullptr);
      const auto* region = regionFor(model.get(), &firstJoin(root)->node);
      ASSERT_NE(region, nullptr);
      SJoinTreeChoice* rawChoice = nullptr;
      ASSERT_EQ(planSearchJoinTree(model.get(), region, &rawChoice), 0);
      ChoicePtr choice(rawChoice, planDestroyJoinTreeChoice);
      ASSERT_NE(choice, nullptr);
      const auto choiceBefore = joinChoiceText(choice.get(), choice->rootCell);
      const auto* props = context->pJoinPropsStore;
      SLogicNode* raw = nullptr;
      ASSERT_EQ(planBuildJoinTree(context, root, choice.get(), &raw), 0);
      BuiltJoinTree built(context, raw);
      ASSERT_NE(built.root(), nullptr);
      EXPECT_EQ(builtJoinHeight(built.root()), height);
      EXPECT_EQ(context->pJoinPropsStore, props);
      EXPECT_EQ(nodeText((const SNode*)root), before);
      EXPECT_EQ(joinChoiceText(choice.get(), choice->rootCell), choiceBefore);
      std::set<const SLogicNode*> originalNodes;
      collectBuiltNodes(root, originalNodes);
      std::set<const SLogicNode*> builtNodes;
      collectBuiltNodes(built.root(), builtNodes);
      checkBuiltParents(built.root(), nullptr, originalNodes);
      EXPECT_EQ(built.root()->stmtRoot, root->stmtRoot);
      checkBuiltTargets(&region->pRoot->node, &firstJoin(built.root())->node);
      checkBuiltJoinBindings(built.root());
      if (check) check(root, built.root());
      ASSERT_EQ(planRefreshJoinRewriteInputs(&built.context, built.root()), 0);
      bool supported = false;
      ASSERT_EQ(planCheckJoinRewriteConditions(&built.context, built.root(),
                                                 &supported), 0);
      ASSERT_TRUE(supported);
      ASSERT_EQ(planRefreshJoinRewriteInputs(&built.context, built.root()), 0);
      checkBuiltJoinBindings(built.root());
      EXPECT_EQ(nodeText((const SNode*)root), before);
    });
    run(sql);
  }

};

TEST_F(PlanJoinParallelTest, splitFourInputsIntoCompleteJoinProducers) {
  useDb("root", "test");
  setPhysiPlanCheck([](const SQueryPlan* plan) {
    checkPhysicalJoinCopies(plan, 3);
  });
  runWithPostSplitHooks(
      "SELECT a.c1,b.c1,c.c1,d.c1 FROM "
      "(t1 a JOIN t1 b ON a.c1=b.c1) "
      "JOIN (t1 c JOIN t1 d ON c.c1=d.c1) ON b.c1=c.c1",
      [](SLogicSubplan* plan) { checkCompleteJoinSplit(plan, 4); }, nullptr);
}

TEST_F(PlanJoinParallelTest, splitEightInputsFinishesNestedJoinCuts) {
  useDb("root", "test");
  setPhysiPlanCheck([](const SQueryPlan* plan) {
    checkPhysicalJoinCopies(plan, 7);
  });
  runWithPostSplitHooks(
      "SELECT a.c1 FROM "
      "((t1 a JOIN t1 b ON a.c1=b.c1) "
      "JOIN (t1 c JOIN t1 d ON c.c1=d.c1) ON b.c1=c.c1) "
      "JOIN ((t1 e JOIN t1 f ON e.c1=f.c1) "
      "JOIN (t1 g JOIN t1 h ON g.c1=h.c1) ON f.c1=g.c1) ON d.c1=e.c1",
      [](SLogicSubplan* plan) { checkCompleteJoinSplit(plan, 8); }, nullptr);
}

TEST_F(PlanJoinParallelTest, splitFiveInputsKeepsExchangeOnlyJoinsOnMerge) {
  useDb("root", "test");
  for (const std::string table : {"t1", "st1s1"}) {
    for (const std::string key : {"ts", "c1"}) {
      SCOPED_TRACE(table + "." + key);
      std::string sql = "SELECT a.c1 FROM " + table + " a";
      for (char alias : {'b', 'c', 'd', 'e'}) {
        sql += " JOIN " + table + " " + alias + " ON a." + key + "=" +
               alias + "." + key;
      }
      runWithPostSplitHooks(sql, [](SLogicSubplan* root) {
        std::vector<const SLogicSubplan*> plans;
        collectJoinSplitSubplans(root, plans);
        int32_t exchangeJoins = 0;
        for (const auto* plan : plans) {
          std::set<int32_t> producers;
          SNode* child = nullptr;
          FOREACH(child, plan->pChildren) {
            producers.insert(((const SLogicSubplan*)child)->id.groupId);
          }
          std::set<const SLogicNode*> nodes;
          collectBuiltNodes(plan->pNode, nodes);
          for (const auto* node : nodes) {
            if (nodeType(node) != QUERY_NODE_LOGIC_PLAN_EXCHANGE) continue;
            const auto* exchange = (const SExchangeLogicNode*)node;
            for (int32_t group = exchange->srcStartGroupId;
                 group <= exchange->srcEndGroupId; ++group) {
              EXPECT_EQ(producers.count(group), 1U);
            }
          }
          if (splitJoinCount(plan->pNode) == 0 ||
              findNode(plan->pNode, QUERY_NODE_LOGIC_PLAN_SCAN) != nullptr) {
            continue;
          }
          ++exchangeJoins;
          EXPECT_EQ(plan->subplanType, SUBPLAN_TYPE_MERGE);
        }
        EXPECT_GT(exchangeJoins, 0);
      }, nullptr);
    }
  }
}

TEST_F(PlanJoinParallelTest, splitSuperTableJoinComputationsAreNotReplicated) {
  useDb("root", "test");
  setPhysiPlanCheck([](const SQueryPlan* plan) {
    checkPhysicalJoinCopies(plan, 3);
    EXPECT_GT(plan->numOfSubplans, 7);
  });
  runWithPostSplitHooks(
      "SELECT /*+ hash_join() */ a.c1 FROM "
      "(st1 a JOIN st1 b ON a.c1=b.c1) "
      "JOIN (st1 c JOIN st1 d ON c.c1=d.c1) ON b.c1=c.c1",
      [](SLogicSubplan* plan) {
        checkCompleteJoinSplit(plan, 4);
        std::vector<const SLogicSubplan*> plans;
        collectJoinSplitSubplans(plan, plans);
        for (const auto* producer : plans) {
          if (splitJoinCount(producer->pNode) == 0) continue;
          EXPECT_EQ(producer->subplanType, SUBPLAN_TYPE_MERGE);
          EXPECT_EQ(producer->pVgroupList, nullptr);
        }
      }, nullptr);
}

TEST_F(PlanJoinParallelTest, splitTemporalParentKeepsCompleteMergeInputs) {
  useDb("root", "test");
  setPhysiPlanCheck([](const SQueryPlan* plan) {
    checkPhysicalJoinCopies(plan, 3);
  });
  for (bool window : {false, true}) {
    SCOPED_TRACE(window ? "WINDOW" : "ASOF");
    const std::string tail = window
        ? " WINDOW_TIME(b.ts,c.ts) WINDOW_OFFSET(-1s,1s)"
        : " ON b.ts>=c.ts";
    runWithPostSplitHooks(
        "SELECT a.c1,c.c1 FROM (t1 a JOIN t1 b ON a.ts=b.ts) LEFT " +
            std::string(window ? "WINDOW" : "ASOF") +
            " JOIN (t1 c JOIN t1 d ON c.ts=d.ts)" + tail,
        [](SLogicSubplan* plan) { checkCompleteJoinSplit(plan, 4); }, nullptr);
  }
}

TEST_F(PlanJoinParallelTest, splitMovesProjectSortAndLimitWithEachJoin) {
  useDb("root", "test");
  setPhysiPlanCheck([](const SQueryPlan* plan) {
    checkPhysicalJoinCopies(plan, 3);
  });
  runWithPostSplitHooks(
      "SELECT l.payload,r.payload FROM "
      "(SELECT a.c1 k,a.c2 payload FROM t1 a "
      "JOIN t1 b ON a.ts=b.ts ORDER BY a.c1 LIMIT 3) l "
      "JOIN (SELECT c.c1 k,c.c2 payload FROM t1 c "
      "JOIN t1 d ON c.ts=d.ts ORDER BY c.c1 DESC LIMIT 4) r ON l.k=r.k",
      [](SLogicSubplan* plan) {
        checkCompleteJoinSplit(plan, 4);
        std::vector<const SLogicSubplan*> plans;
        collectJoinSplitSubplans(plan, plans);
        int32_t wrappers = 0;
        std::set<int64_t> limits;
        for (const auto* producer : plans) {
          if (findNode(producer->pNode, QUERY_NODE_LOGIC_PLAN_SORT) ==
              nullptr) continue;
          ++wrappers;
          EXPECT_NE(findNode(producer->pNode, QUERY_NODE_LOGIC_PLAN_PROJECT),
                    nullptr);
          EXPECT_EQ(splitJoinCount(producer->pNode), 1);
          std::set<const SLogicNode*> nodes;
          collectBuiltNodes(producer->pNode, nodes);
          for (const auto* node : nodes) {
            if (node->pLimit == nullptr) continue;
            const auto* limit = (const SLimitNode*)node->pLimit;
            ASSERT_NE(limit->limit, nullptr);
            limits.insert(limit->limit->datum.i);
          }
          checkBuiltJoinBindings(producer->pNode);
        }
        EXPECT_EQ(wrappers, 2);
        EXPECT_EQ(limits, (std::set<int64_t>{3, 4}));
      }, nullptr);
}

TEST_F(PlanJoinParallelTest, splitMovesExistingDependenciesOnlyOnce) {
  useDb("root", "test");
  setJoinPropsCheck([](const SPlanContext* context, const SLogicNode* root,
                       bool optimized) {
    if (!optimized) return;
    for (bool nested : {true, false}) {
      SCOPED_TRACE(nested);
      SplitTreeCopy copy(context, root);
      ASSERT_EQ(copy.code, 0);
      auto* plan = copy.plan();
      auto* join = (SJoinLogicNode*)firstJoin(plan->pNode);
      ASSERT_NE(join, nullptr);
      auto* left = (SLogicNode*)nodesListGetNode(join->node.pChildren, 0);
      auto* input = nested
          ? (SLogicNode*)findNode(left, QUERY_NODE_LOGIC_PLAN_SCAN) : left;
      ASSERT_NE(input, nullptr);
      const auto* dependency = installSplitDependency(plan, input, 100);
      ASSERT_NE(dependency, nullptr);
      const auto* existing = nodesListGetNode(join->node.pChildren, 0);
      ASSERT_EQ(splitLogicPlan(&copy.context, plan), 0);
      const SLogicSubplan* expectedOwner = plan;
      if (nested) {
        const auto* exchange = (const SExchangeLogicNode*)
            nodesListGetNode(join->node.pChildren, 0);
        ASSERT_EQ(nodeType(exchange), QUERY_NODE_LOGIC_PLAN_EXCHANGE);
        std::vector<const SLogicSubplan*> plans;
        collectJoinSplitSubplans(plan, plans);
        expectedOwner = nullptr;
        for (const auto* producer : plans) {
          if (producer->id.groupId == exchange->srcStartGroupId) {
            expectedOwner = producer;
          }
        }
        ASSERT_NE(expectedOwner, nullptr);
        EXPECT_NE(expectedOwner, plan);
      } else {
        EXPECT_EQ(nodesListGetNode(join->node.pChildren, 0), existing);
        const auto* exchange = (const SExchangeLogicNode*)existing;
        EXPECT_EQ(exchange->srcStartGroupId, 100);
        EXPECT_EQ(exchange->srcEndGroupId, 100);
      }
      std::vector<const SLogicSubplan*> plans;
      collectJoinSplitSubplans(plan, plans);
      int32_t owners = 0;
      for (const auto* producer : plans) {
        SNode* child = nullptr;
        FOREACH(child, producer->pChildren) {
          if (child != (const SNode*)dependency) continue;
          ++owners;
          EXPECT_EQ(producer, expectedOwner);
        }
      }
      EXPECT_EQ(owners, 1);
      const auto once = nodeText((SNode*)plan);
      ASSERT_EQ(splitLogicPlan(&copy.context, plan), 0);
      EXPECT_EQ(nodeText((SNode*)plan), once);
    }
  });
  run("SELECT a.c1 FROM (t1 a JOIN t1 b ON a.c1=b.c1) "
      "JOIN (t1 c JOIN t1 d ON c.c1=d.c1) ON b.c1=c.c1");
}

TEST_F(PlanJoinParallelTest, splitFailureReleasesDetachedJoinProducer) {
  useDb("root", "test");
  setJoinPropsCheck([](const SPlanContext* context, const SLogicNode* root,
                       bool optimized) {
    if (!optimized) return;
    SplitTreeCopy copy(context, root);
    ASSERT_EQ(copy.code, 0);
    failedSplitProducerAppend = false;
    int32_t code;
    {
      Stub stub;
      stub.set(nodesListMakeAppend, failSplitProducerAppend);
      code = splitLogicPlan(&copy.context, copy.plan());
    }
    EXPECT_TRUE(failedSplitProducerAppend);
    EXPECT_EQ(code, TSDB_CODE_OUT_OF_MEMORY);
    std::set<const SLogicNode*> nodes;
    collectBuiltNodes(copy.plan()->pNode, nodes);
    planDestroyJoinPropsStore(&copy.context);
    copy.owner.reset();
  });
  run("SELECT a.c1 FROM (t1 a JOIN t1 b ON a.c1=b.c1) "
      "JOIN (t1 c JOIN t1 d ON c.c1=d.c1) ON b.c1=c.c1");
}

TEST_F(PlanJoinParallelTest, splitSkipsDynamicStreamAndRuntimeDependentPair) {
  useDb("root", "test");
  setJoinPropsCheck([](const SPlanContext* context, const SLogicNode* root,
                       bool optimized) {
    if (!optimized) return;
    for (int32_t restriction = 0; restriction < 5; ++restriction) {
      SCOPED_TRACE(restriction);
      SNode* holder = nullptr;
      ASSERT_EQ(nodesMakeNode(QUERY_NODE_LOGIC_SUBPLAN, &holder), 0);
      NodePtr owner(holder, nodesDestroyNode);
      TreeCopy copy(context, root);
      ASSERT_EQ(copy.code, 0);
      auto* plan = (SLogicSubplan*)holder;
      plan->id.groupId = copy.context.groupId = 1;
      plan->subplanType = SUBPLAN_TYPE_MERGE;
      plan->pNode = (SLogicNode*)copy.tree.release();
      auto* join = (SJoinLogicNode*)firstJoin(plan->pNode);
      ASSERT_NE(join, nullptr);
      if (restriction == 0) join->node.dynamicOp = true;
      if (restriction == 1) copy.context.streamCxt.isCalc = true;
      if (restriction == 2) {
        bool marked = false;
        planWalkLogicExprFields(&join->node,
            [](SNode* node, void* context) {
              auto* marked = (bool*)context;
              if (!*marked && nodeType(node) == QUERY_NODE_COLUMN) {
                ((SColumnNode*)node)->hasDep = true;
                *marked = true;
              }
              return DEAL_RES_CONTINUE;
            }, &marked);
        ASSERT_TRUE(marked);
      }
      if (restriction >= 3) {
        SNode* remote = nullptr;
        ASSERT_EQ(nodesMakeNode(restriction == 3 ? QUERY_NODE_REMOTE_VALUE_LIST
                                                 : QUERY_NODE_REMOTE_ZERO_ROWS,
                                  &remote), 0);
        ((SExprNode*)remote)->resType.type = TSDB_DATA_TYPE_BOOL;
        ((SExprNode*)remote)->resType.bytes = sizeof(bool);
        nodesDestroyNode(join->node.pConditions);
        join->node.pConditions = remote;
      }
      ASSERT_EQ(splitLogicPlan(&copy.context, plan), 0);
      const auto* left = nodesListGetNode(join->node.pChildren, 0);
      const auto* right = nodesListGetNode(join->node.pChildren, 1);
      // Existing split rules may still extract scans or a single side.
      EXPECT_FALSE(nodeType(left) == QUERY_NODE_LOGIC_PLAN_EXCHANGE &&
                   nodeType(right) == QUERY_NODE_LOGIC_PLAN_EXCHANGE);
    }
  });
  run("SELECT a.c1 FROM (t1 a JOIN t1 b ON a.c1=b.c1) "
      "JOIN (t1 c JOIN t1 d ON c.c1=d.c1) ON b.c1=c.c1");
}

TEST_F(PlanJoinParallelTest, buildPureStarUsesDerivedEqualityAndActualBags) {
  useDb("root", "test");
  runBuildJoinTree("SELECT a.c1,b.c1,c.c1,d.c1 FROM t1 a "
                   "JOIN t1 b ON a.c1=b.c1 JOIN t1 c ON a.c1=c.c1 "
                   "JOIN t1 d ON a.c1=d.c1", 2,
                   [](const SLogicNode* original, const SLogicNode* built) {
    EXPECT_TRUE(hasBuiltDerivedEquality(built));
    checkBuiltJoinBags(original, built);
  });
}

TEST_F(PlanJoinParallelTest, buildTimestampChainsAndStarsKeepMergeJoins) {
  useDb("root", "test");
  for (int32_t count : {4, 8}) {
    for (bool star : {false, true}) {
      SCOPED_TRACE(count);
      SCOPED_TRACE(star);
      std::string sql = "SELECT a.ts FROM t1 a";
      for (int32_t input = 1; input < count; ++input) {
        const std::string alias(1, 'a' + input);
        const std::string previous(1, star ? 'a' : 'a' + input - 1);
        sql += " JOIN t1 " + alias + " ON " + previous + ".ts=" +
               alias + ".ts";
      }
      setPhysiPlanCheck([count](const SQueryPlan* plan) {
        checkPhysicalJoinAlgorithms(plan, count - 1, 0);
      });
      runBuildJoinTree(sql, count == 4 ? 2 : 3,
          [count](const SLogicNode* original, const SLogicNode* built) {
            checkBuiltEqualityCount(built, "ts", 1);
            if (count == 4) checkBuiltJoinBags(original, built);
          });
    }
  }
}

TEST_F(PlanJoinParallelTest, buildTimestampRewritesKeepIndependentKeys) {
  useDb("root", "test");
  setPhysiPlanCheck([](const SQueryPlan* plan) {
    checkPhysicalJoinAlgorithms(plan, 3, 0);
  });
  runBuildJoinTree("SELECT a.ts FROM t1 a "
                   "JOIN t1 b ON a.ts=b.ts AND a.c1=b.c1 "
                   "JOIN t1 c ON a.ts=c.ts AND a.c1=c.c1 "
                   "JOIN t1 d ON a.ts=d.ts AND a.c1=d.c1", 2,
                   [](const SLogicNode*, const SLogicNode* built) {
    checkBuiltEqualityCount(built, "ts", 1);
    checkBuiltEqualityCount(built, "c1", 1);
  });
  setPhysiPlanCheck([](const SQueryPlan* plan) {
    checkPhysicalJoinAlgorithms(plan, 3, 0);
  });
  runBuildJoinTree("SELECT a.ts FROM stream_t2 a "
                   "JOIN stream_t2 b ON a.ts=b.ts AND a.c3=b.c3 "
                   "JOIN stream_t2 c ON b.ts=c.ts AND b.c3=c.ts "
                   "JOIN stream_t2 d ON c.ts=d.ts", 2,
                   [](const SLogicNode* original, const SLogicNode* built) {
    EXPECT_TRUE(hasBuiltColumnEquality(built, "a.c3", "b.c3"));
    checkBuiltTwoColumnBags(original, built);
  });
}

TEST_F(PlanJoinParallelTest,
       buildTimestampPredicatesPreserveNullableAndOuterSemantics) {
  useDb("root", "test");
  setPhysiPlanCheck([](const SQueryPlan* plan) {
    checkPhysicalJoinAlgorithms(plan, 3, 1);
  });
  runBuildJoinTree("SELECT a.ts,b.c1 FROM t1 a "
                   "LEFT JOIN t1 b ON a.c1=b.c1 "
                   "JOIN t1 c ON a.ts=c.ts JOIN t1 d ON a.ts=d.ts "
                   "JOIN t1 e ON a.ts=e.ts", 3,
                   [](const SLogicNode*, const SLogicNode* built) {
    checkBuiltEqualityCount(built, "ts", 1);
  });
  setPhysiPlanCheck([](const SQueryPlan* plan) {
    checkPhysicalJoinAlgorithms(plan, 0, 3);
  });
  runBuildJoinTree("SELECT a.c3,b.c3,c.c3,d.c3 FROM stream_t2 a "
                   "JOIN stream_t2 b ON a.c3=b.c3 "
                   "JOIN stream_t2 c ON a.c3=c.c3 "
                   "JOIN stream_t2 d ON a.c3=d.c3", 2,
                   [](const SLogicNode* original, const SLogicNode* built) {
    checkBuiltEqualityCount(built, "c3", 1);
    checkBuiltJoinBags(original, built);
  });
  setPhysiPlanCheck([](const SQueryPlan* plan) {
    checkPhysicalJoinAlgorithms(plan, 3, 0);
  });
  runBuildJoinTree("SELECT a.ts FROM stream_t2 a "
                   "JOIN stream_t2 b ON a.ts=b.ts "
                   "JOIN stream_t2 c ON a.ts=c.ts "
                   "JOIN stream_t2 d ON a.ts=d.ts AND d.c3=d.c3", 2,
                   [](const SLogicNode* original, const SLogicNode* built) {
    EXPECT_TRUE(hasBuiltColumnEquality(built, "d.c3", "d.c3"));
    checkBuiltTwoColumnBags(original, built);
  });
}

TEST_F(PlanJoinParallelTest, buildExpandsReachableInnerStateAfterLeft) {
  useDb("root", "test");
  runBuildJoinTree("SELECT a.c1,b.c1,e.c1 FROM t1 a "
                   "LEFT JOIN t1 b ON a.c1=b.c1 "
                   "JOIN t1 c ON a.c1=c.c1 JOIN t1 d ON a.c1=d.c1 "
                   "JOIN t1 e ON a.c1=e.c1", 3,
                   [](const SLogicNode*, const SLogicNode* built) {
    EXPECT_TRUE(hasBuiltDerivedEquality(built));
    int32_t outer = 0;
    std::function<void(const SLogicNode*)> inspect =
        [&](const SLogicNode* node) {
          if (nodeType(node) == QUERY_NODE_LOGIC_PLAN_JOIN) {
            const auto* join = (const SJoinLogicNode*)node;
            if (join->joinType == JOIN_TYPE_LEFT ||
                join->joinType == JOIN_TYPE_RIGHT) {
              ++outer;
              const int32_t retained = join->joinType == JOIN_TYPE_LEFT
                                           ? 0 : 1;
              const auto* input = (const SLogicNode*)nodesListGetNode(
                  node->pChildren, retained);
              bool hasA = false;
              SNode* target = nullptr;
              FOREACH(target, input->pTargets) {
                hasA |= nodeType(target) == QUERY_NODE_COLUMN &&
                        strcmp(((const SColumnNode*)target)->tableAlias,
                                "a") == 0;
              }
              EXPECT_TRUE(hasA);
            }
          }
          SNode* child = nullptr;
          FOREACH(child, node->pChildren) {
            inspect((const SLogicNode*)child);
          }
        };
    inspect(built);
    EXPECT_EQ(outer, 1);
  });
}

TEST_F(PlanJoinParallelTest, buildMixedTreesPreserveActualBagsAndDirection) {
  useDb("root", "test");
  for (const std::string kind : {"LEFT", "FULL", "RIGHT"}) {
    SCOPED_TRACE(kind);
    runBuildJoinTree("SELECT a.c1,b.c1,c.c1,d.c1 FROM t1 a " + kind +
                     " JOIN t1 b ON a.c1=b.c1 " + kind +
                     " JOIN t1 c ON b.c1=c.c1 " + kind +
                     " JOIN t1 d ON c.c1=d.c1", 2,
                     [](const SLogicNode* original, const SLogicNode* built) {
      checkBuiltJoinBags(original, built);
    });
  }
}

TEST_F(PlanJoinParallelTest, buildKeepsRootFilterAtFinalOutput) {
  useDb("root", "test");
  runBuildJoinTree("SELECT a.c1,b.c1,c.c1,d.c1 FROM t1 a "
                   "LEFT JOIN t1 b ON a.c1=b.c1 "
                   "LEFT JOIN t1 c ON b.c1=c.c1 "
                   "LEFT JOIN t1 d ON c.c1=d.c1 "
                   "WHERE a.c1>0 OR d.c1 IS NULL", 2,
                   [](const SLogicNode* original, const SLogicNode* built) {
    const auto* source = firstJoin(original);
    const auto* target = firstJoin(built);
    ASSERT_NE(source->node.pConditions, nullptr);
    EXPECT_TRUE(nodesEqualNode(source->node.pConditions,
                                target->node.pConditions));
    checkBuiltJoinBags(original, built);
  });
}

TEST_F(PlanJoinParallelTest, buildKeepsUnaryAndOrTermsFromPureOn) {
  useDb("root", "test");
  runBuildJoinTree("SELECT a.c1,b.c1,c.c1,d.c1 FROM t1 a "
                   "JOIN t1 b ON a.c1=b.c1 JOIN t1 c ON a.c1=c.c1 "
                   "JOIN t1 d ON a.c1=d.c1 AND d.c1>0 "
                   "AND (b.c1>0 OR c.c1 IS NULL)", 2,
                   [](const SLogicNode* original, const SLogicNode* built) {
    checkBuiltJoinBags(original, built);
  });
}

TEST_F(PlanJoinParallelTest, buildRetainsHiddenJoinKeysAndProjectAliases) {
  useDb("root", "test");
  runBuildJoinTree("SELECT a.c2 FROM t1 a JOIN t1 b ON a.c1=b.c1 "
                   "JOIN t1 c ON a.c1=c.c1 JOIN t1 d ON a.c1=d.c1", 2);
  runBuildJoinTree("SELECT q.payload FROM "
                   "(SELECT c1 k,c2 payload FROM t1) q "
                   "JOIN t1 b ON q.k=b.c1 JOIN t1 c ON q.k=c.c1 "
                   "JOIN t1 d ON q.k=d.c1", 2,
                   [](const SLogicNode* original, const SLogicNode* built) {
    checkBuiltTargets(original, built);
    EXPECT_NE(findNode(built, QUERY_NODE_LOGIC_PLAN_PROJECT), nullptr);
  });
}

TEST_F(PlanJoinParallelTest, buildEightInputPureStarSmoke) {
  useDb("root", "test");
  std::string sql = "SELECT a.c2 FROM t1 a";
  for (char alias = 'b'; alias <= 'h'; ++alias) {
    sql += " JOIN t1 " + std::string(1, alias) +
           " ON a.c1=" + alias + ".c1";
  }
  runBuildJoinTree(sql, 3);
}

TEST_F(PlanJoinParallelTest, buildFailureLeavesOriginalAndChoiceReusable) {
  useDb("root", "test");
  setJoinPropsCheck([](const SPlanContext* context, const SLogicNode* root,
                       bool optimized) {
    if (optimized) return;
    const auto before = nodeText((const SNode*)root);
    auto model = prepareJoinModel(context, root);
    ASSERT_NE(model, nullptr);
    const auto* region = regionFor(model.get(), &firstJoin(root)->node);
    ASSERT_NE(region, nullptr);
    SJoinTreeChoice* rawChoice = nullptr;
    ASSERT_EQ(planSearchJoinTree(model.get(), region, &rawChoice), 0);
    ChoicePtr choice(rawChoice, planDestroyJoinTreeChoice);
    ASSERT_NE(choice, nullptr);
    const auto choiceBefore = joinChoiceText(choice.get(), choice->rootCell);
    const auto* props = context->pJoinPropsStore;
    for (bool clone : {true, false}) {
      SLogicNode* failed = nullptr;
      int32_t code;
      {
        Stub stub;
        if (clone) stub.set(nodesCloneNode, failBuildClone);
        else stub.set(nodesListMakeAppend, failBuildAppend);
        code = planBuildJoinTree(context, root, choice.get(), &failed);
      }
      NodePtr cleanup((SNode*)failed, nodesDestroyNode);
      EXPECT_EQ(code, TSDB_CODE_OUT_OF_MEMORY);
      EXPECT_EQ(failed, nullptr);
      EXPECT_EQ(context->pJoinPropsStore, props);
      EXPECT_EQ(nodeText((const SNode*)root), before);
      EXPECT_EQ(joinChoiceText(choice.get(), choice->rootCell), choiceBefore);
    }
    SLogicNode* raw = nullptr;
    EXPECT_EQ(planBuildJoinTree(nullptr, root, choice.get(), &raw),
              TSDB_CODE_INVALID_PARA);
    EXPECT_EQ(raw, nullptr);
    EXPECT_EQ(planBuildJoinTree(context, nullptr, choice.get(), &raw),
              TSDB_CODE_INVALID_PARA);
    EXPECT_EQ(planBuildJoinTree(context, root, nullptr, &raw),
              TSDB_CODE_INVALID_PARA);
    EXPECT_EQ(planBuildJoinTree(context, root, choice.get(), nullptr),
              TSDB_CODE_INVALID_PARA);
    const auto* unrelated = findNode(root, QUERY_NODE_LOGIC_PLAN_SCAN);
    ASSERT_NE(unrelated, nullptr);
    EXPECT_EQ(planBuildJoinTree(context, unrelated, choice.get(), &raw),
              TSDB_CODE_INVALID_PARA);
    EXPECT_EQ(raw, nullptr);
    ASSERT_EQ(planBuildJoinTree(context, root, choice.get(), &raw), 0);
    BuiltJoinTree retry(context, raw);
    ASSERT_NE(retry.root(), nullptr);
    EXPECT_EQ(builtJoinHeight(retry.root()), 2);
    checkBuiltJoinBags(root, retry.root());
    EXPECT_EQ(nodeText((const SNode*)root), before);
    for (int32_t expected : {TSDB_CODE_OUT_OF_MEMORY,
                               TSDB_CODE_PLAN_SLOT_NOT_FOUND}) {
      TreeCopy source(context, root);
      ASSERT_EQ(source.code, 0);
      SLogicSubplan subplan = {};
      subplan.pNode = source.root();
      const auto* originalRoot = source.root();
      const auto originalText = nodeText(source.tree.get());
      bool changed = true;
      int32_t code;
      {
        Stub stub;
        if (expected == TSDB_CODE_OUT_OF_MEMORY) {
          stub.set(nodesCloneNode, failBuildClone);
        } else {
          stub.set(planRefreshJoinRewriteInputs, failBuildInputBinding);
        }
        code = planRewriteJoinParallel(&source.context, &subplan, &changed);
      }
      source.tree.release();
      source.tree.reset((SNode*)subplan.pNode);
      EXPECT_EQ(code, expected);
      EXPECT_FALSE(changed);
      EXPECT_EQ(subplan.pNode, originalRoot);
      EXPECT_EQ(nodeText(source.tree.get()), originalText);
    }
  });
  run("SELECT a.c1,b.c1,c.c1,d.c1 FROM t1 a "
      "JOIN t1 b ON a.c1=b.c1 JOIN t1 c ON a.c1=c.c1 "
      "JOIN t1 d ON a.c1=d.c1");
}

TEST_F(PlanJoinParallelTest, buildRequiresProofForSystemInputDistribution) {
  useDb("root", "test");
  setJoinPropsCheck([](const SPlanContext* context, const SLogicNode* root,
                       bool optimized) {
    if (optimized) return;
    for (bool proven : {true, false}) {
      SCOPED_TRACE(proven);
      TreeCopy source(context, root);
      ASSERT_EQ(source.code, 0);
      auto* system = (SScanLogicNode*)
          findNode(source.root(), QUERY_NODE_LOGIC_PLAN_SCAN);
      ASSERT_NE(system, nullptr);
      system->tableType = TSDB_SYSTEM_TABLE;
      std::set<const SLogicNode*> sourceNodes;
      collectBuiltNodes(source.root(), sourceNodes);
      for (const auto* node : sourceNodes) {
        if (nodeType(node) == QUERY_NODE_LOGIC_PLAN_JOIN) {
          ((SJoinLogicNode*)node)->isSingleTableJoin = proven;
        }
      }
      ASSERT_EQ(planRebuildJoinInputProps(&source.context, source.root()), 0);
      const auto before = nodeText(source.tree.get());
      auto model = prepareJoinModel(&source.context, source.root());
      ASSERT_NE(model, nullptr);
      const auto* top = firstJoin(source.root());
      ASSERT_NE(top, nullptr);
      const auto* region = regionFor(model.get(), &top->node);
      ASSERT_NE(region, nullptr);
      ASSERT_EQ(taosArrayGetSize(region->pInputs), 4U);
      SJoinTreeChoice* rawChoice = nullptr;
      ASSERT_EQ(planSearchJoinTree(model.get(), region, &rawChoice), 0);
      ChoicePtr choice(rawChoice, planDestroyJoinTreeChoice);
      ASSERT_NE(choice, nullptr);
      SLogicNode* raw = nullptr;
      const int32_t code = planBuildJoinTree(&source.context, source.root(),
                                              choice.get(), &raw);
      BuiltJoinTree built(&source.context, raw);
      if (proven) {
        ASSERT_EQ(code, TSDB_CODE_SUCCESS);
        ASSERT_NE(built.root(), nullptr);
        EXPECT_EQ(builtJoinHeight(built.root()), 2);
        std::set<const SLogicNode*> builtNodes;
        collectBuiltNodes(built.root(), builtNodes);
        int32_t joins = 0;
        for (const auto* node : builtNodes) {
          if (nodeType(node) != QUERY_NODE_LOGIC_PLAN_JOIN) continue;
          ++joins;
          EXPECT_TRUE(((const SJoinLogicNode*)node)->isSingleTableJoin);
        }
        EXPECT_EQ(joins, 3);
        checkBuiltJoinBindings(built.root());
      } else {
        EXPECT_EQ(code, TSDB_CODE_PLAN_NOT_SUPPORT_JOIN_COND);
        EXPECT_EQ(built.root(), nullptr);
      }
      EXPECT_EQ(nodeText(source.tree.get()), before);
    }
  });
  run("SELECT a.c1,b.c1,c.c1,d.c1 FROM t1 a "
      "JOIN t1 b ON a.c1=b.c1 JOIN t1 c ON a.c1=c.c1 "
      "JOIN t1 d ON a.c1=d.c1");
}

TEST_F(PlanJoinParallelTest, buildKeepsExplicitTemporalOpaqueInput) {
  useDb("root", "test");
  runBuildJoinTree("SELECT a.c1,b.c1 FROM "
                   "(stream_t2 a LEFT ASOF JOIN stream_t2 b "
                   "ON a.ts>=b.ts) JOIN stream_t2 c ON a.c1=c.c1 "
                   "JOIN stream_t2 d ON a.c1=d.c1 "
                   "JOIN stream_t2 e ON a.c1=e.c1", 3,
                   [](const SLogicNode* original, const SLogicNode* built) {
    const auto* before = temporalJoin(original);
    const auto* after = temporalJoin(built);
    ASSERT_NE(before, nullptr);
    ASSERT_NE(after, nullptr);
    EXPECT_EQ(after->joinType, before->joinType);
    EXPECT_EQ(after->subType, before->subType);
    EXPECT_TRUE(nodesEqualNode(planGetTemporalJoinTimeCond(before),
                                planGetTemporalJoinTimeCond(after)));
    EXPECT_EQ(countNotNullFilters(&after->node),
              countNotNullFilters(&before->node));
  });
}

TEST_F(PlanJoinParallelTest, buildKeepsPinnedFilterAfterNullExtension) {
  useDb("root", "test");
  setJoinPropsCheck([](const SPlanContext* context, const SLogicNode* root,
                       bool optimized) {
    if (optimized) return;
    TreeCopy source(context, root);
    ASSERT_EQ(source.code, 0);
    auto* top = (SJoinLogicNode*)firstJoin(source.root());
    auto* middle = (SLogicNode*)nodesListGetNode(top->node.pChildren, 0);
    auto* bottom = (SLogicNode*)nodesListGetNode(middle->pChildren, 0);
    ASSERT_EQ(nodeType(bottom), QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(top->node.pConditions, nullptr);
    ASSERT_EQ(bottom->pConditions, nullptr);
    bottom->pConditions = top->node.pConditions;
    top->node.pConditions = nullptr;
    ASSERT_EQ(planRebuildJoinInputProps(&source.context, source.root()), 0);
    auto model = prepareJoinModel(&source.context, source.root());
    ASSERT_NE(model, nullptr);
    const auto* region = regionFor(model.get(), &top->node);
    ASSERT_NE(region, nullptr);
    SJoinTreeChoice* rawChoice = nullptr;
    ASSERT_EQ(planSearchJoinTree(model.get(), region, &rawChoice), 0);
    ChoicePtr choice(rawChoice, planDestroyJoinTreeChoice);
    ASSERT_NE(choice, nullptr);
    SLogicNode* raw = nullptr;
    ASSERT_EQ(planBuildJoinTree(&source.context, source.root(), choice.get(),
                                 &raw), 0);
    BuiltJoinTree built(&source.context, raw);
    ASSERT_NE(built.root(), nullptr);
    EXPECT_EQ(builtJoinHeight(built.root()), 2);
    checkBuiltJoinBindings(built.root());
    checkBuiltJoinBags(source.root(), built.root());
  });
  run("SELECT a.c1,b.c1,c.c1,d.c1 FROM t1 a "
      "LEFT JOIN t1 b ON a.c1=b.c1 LEFT JOIN t1 c ON b.c1=c.c1 "
      "LEFT JOIN t1 d ON c.c1=d.c1 WHERE b.c1 IS NULL");
}

TEST_F(PlanJoinParallelTest, conditionCheckKeepsOriginalDiagnostic) {
  useDb("root", "test");
  setJoinPropsCheck([](const SPlanContext* context, const SLogicNode* root,
                       bool optimized) {
    if (optimized) return;
    TreeCopy candidate(context, root);
    ASSERT_EQ(candidate.code, 0);
    ASSERT_EQ(planRefreshJoinRewriteInputs(&candidate.context,
                                            candidate.root()), 0);
    char diagnostic[TSDB_ERROR_MSG_LEN] = "original query diagnostic";
    candidate.context.pMsg = diagnostic;
    candidate.context.msgLen = sizeof(diagnostic);
    auto* join = (SJoinLogicNode*)firstJoin(candidate.root());
    SFunctionNode* unsupported = nullptr;
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&unsupported), 0);
    unsupported->funcType = FUNCTION_TYPE_NOW;
    tstrncpy(unsupported->functionName, "now",
               sizeof(unsupported->functionName));
    nodesDestroyNode(join->pFullOnCond);
    join->pFullOnCond = (SNode*)unsupported;
    join->joinAlgoHint = JOIN_ALGO_THETA;
    bool supported = true;
    ASSERT_EQ(planCheckJoinRewriteConditions(&candidate.context,
                                               candidate.root(), &supported),
              0);
    EXPECT_FALSE(supported);
    EXPECT_STREQ(diagnostic, "original query diagnostic");
  });
  run("SELECT a.c1 FROM t1 a JOIN t1 b ON a.c1=b.c1");
}

TEST_F(PlanJoinParallelTest, rewriteKeepsVisibleSemiAntiInputOpaque) {
  useDb("root", "test");
  for (bool anti : {false, true}) {
    SCOPED_TRACE(anti ? "ANTI" : "SEMI");
    bool sawOptimized = false;
    setJoinPropsCheck([anti, &sawOptimized](const SPlanContext* context,
                                            const SLogicNode* root,
                                            bool optimized) {
      auto model = prepareJoinModel(context, root);
      ASSERT_NE(model, nullptr);
      if (!optimized) {
        const auto* region = regionFor(model.get(), &firstJoin(root)->node);
        ASSERT_NE(region, nullptr);
        EXPECT_EQ(region->mode, JOIN_REGION_PURE_INNER);
        ASSERT_EQ(taosArrayGetSize(region->pInputs), 4U);
        const auto* input = (const SJoinParallelInput*)
            taosArrayGet(region->pInputs, 0);
        ASSERT_EQ(nodeType(input->pNode), QUERY_NODE_LOGIC_PLAN_JOIN);
        EXPECT_EQ(((const SJoinLogicNode*)input->pNode)->subType,
                  anti ? JOIN_STYPE_ANTI : JOIN_STYPE_SEMI);
        EXPECT_EQ(builtJoinHeight(root), 4);
        return;
      }
      sawOptimized = true;
      EXPECT_EQ(builtJoinHeight(root), 3);
      std::set<const SLogicNode*> nodes;
      collectBuiltNodes(root, nodes);
      const std::set<const SLogicNode*> original;
      checkBuiltParents(root, nullptr, original);
      checkBuiltJoinBindings(root);
      checkHashJoinsWithoutPartitions(root);
      int32_t opaque = 0;
      for (const auto* node : nodes) {
        if (nodeType(node) != QUERY_NODE_LOGIC_PLAN_JOIN) continue;
        const auto* join = (const SJoinLogicNode*)node;
        if (join->subType != (anti ? JOIN_STYPE_ANTI : JOIN_STYPE_SEMI)) {
          continue;
        }
        ++opaque;
        const auto* props = planGetJoinInputProps(context, node);
        ASSERT_NE(props, nullptr);
        bool matchingVisible = false;
        for (size_t i = 0; i < taosArrayGetSize(props->pOutputCols); ++i) {
          const auto* output = (const SJoinOutputCol*)
              taosArrayGet(props->pOutputCols, i);
          if (nodeType(output->pExpr) != QUERY_NODE_COLUMN) continue;
          const auto* column = (const SColumnNode*)output->pExpr;
          if (strcmp(column->tableAlias, "b") != 0 ||
              strcmp(column->colName, "c1") != 0) continue;
          matchingVisible = true;
          if (anti) EXPECT_TRUE(output->nullable);
        }
        EXPECT_TRUE(matchingVisible);
        for (size_t i = 0; i < taosArrayGetSize(model->pRegions); ++i) {
          const auto* region = regionAt(model.get(), i);
          for (size_t j = 0; j < taosArrayGetSize(region->pOps); ++j) {
            EXPECT_NE(taosArrayGetP(region->pOps, j), join);
          }
        }
      }
      EXPECT_EQ(opaque, 1);
    });
    const std::string kind = anti ? "ANTI" : "SEMI";
    const std::string key = anti ? "a.c1" : "b.c1";
    run("SELECT b.c1,c.c1,d.c1,e.c1 FROM (t1 a LEFT " + kind +
        " JOIN t1 b ON a.c1=b.c1) JOIN t1 c ON " + key +
        "=c.c1 JOIN t1 d ON c.c1=d.c1 JOIN t1 e ON d.c1=e.c1");
    EXPECT_TRUE(sawOptimized);
  }
}

TEST_F(PlanJoinParallelTest, rewriteKeepsSingleInputOnFilterAboveLimit) {
  useDb("root", "test");
  bool sawOptimized = false;
  runBuildJoinTree("SELECT q.c1 FROM (SELECT c1 FROM t1 LIMIT 2) q "
                   "JOIN t1 b ON q.c1=b.c1 JOIN t1 c ON q.c1=c.c1 "
                   "JOIN t1 d ON q.c1=d.c1 AND q.c1>0", 2,
                   [](const SLogicNode* original, const SLogicNode*) {
    std::set<const SLogicNode*> nodes;
    collectBuiltNodes(original, nodes);
    bool limitedProject = false;
    for (const auto* node : nodes) {
      limitedProject |= nodeType(node) == QUERY_NODE_LOGIC_PLAN_PROJECT &&
                        node->pLimit != nullptr;
    }
    EXPECT_TRUE(limitedProject);
  }, [&sawOptimized](const SLogicNode* root) {
    sawOptimized = true;
    EXPECT_EQ(builtJoinHeight(root), 2);
    checkFilterAboveLimit(root);
    checkBuiltJoinBindings(root);
    checkHashJoinsWithoutPartitions(root);
  });
  EXPECT_TRUE(sawOptimized);
}

TEST_F(PlanJoinParallelTest, rewriteRejectsExternalRootTargetExpansion) {
#ifndef TD_ENTERPRISE
  GTEST_SKIP() << "Federated query parse path is gated by TD_ENTERPRISE";
#else
  struct RestoreFlag {
    bool value;
    ~RestoreFlag() { tsFederatedQueryEnable = value; }
  } restore{tsFederatedQueryEnable};
  tsFederatedQueryEnable = true;
  g_mockCatalogService->createExtSource("rewrite_remote", EXT_SOURCE_MYSQL,
                                       "127.0.0.1", 1, "user", "db", "");
  const std::vector<MockCatalogService::MockExtColDef> columns = {
      {"ts", TSDB_DATA_TYPE_TIMESTAMP, true, false, false, "datetime"},
      {"c1", TSDB_DATA_TYPE_INT, false, true, false, "int"},
      {"c2", TSDB_DATA_TYPE_INT, false, true, false, "int"}};
  g_mockCatalogService->createExtTable("rewrite_remote", "", "", "metrics",
                                      columns, TSDB_TIME_PRECISION_MILLI);
  useDb("root", "test");
  bool sawOptimized = false;
  setJoinPropsCheck([&sawOptimized](const SPlanContext* context,
                                    const SLogicNode* root, bool optimized) {
    if (optimized) {
      sawOptimized = true;
      EXPECT_EQ(builtJoinHeight(root), 2);
      checkBuiltJoinBindings(root);
      return;
    }
    ASSERT_EQ(nodeType(root), QUERY_NODE_LOGIC_PLAN_PROJECT);
    TreeCopy source(context, &firstJoin(root)->node);
    ASSERT_EQ(source.code, 0);
    source.root()->stmtRoot = true;
    ASSERT_EQ(planRebuildJoinInputProps(&source.context, source.root()), 0);
    const auto before = nodeText(source.tree.get());
    ASSERT_EQ(LIST_LENGTH(source.root()->pTargets), 1);
    auto model = prepareJoinModel(&source.context, source.root());
    ASSERT_NE(model, nullptr);
    const auto* region = regionFor(model.get(), source.root());
    ASSERT_NE(region, nullptr);
    SJoinTreeChoice* rawChoice = nullptr;
    ASSERT_EQ(planSearchJoinTree(model.get(), region, &rawChoice), 0);
    ChoicePtr choice(rawChoice, planDestroyJoinTreeChoice);
    ASSERT_NE(choice, nullptr);
    SLogicNode* raw = nullptr;
    ASSERT_EQ(planBuildJoinTree(&source.context, source.root(), choice.get(),
                                 &raw), 0);
    BuiltJoinTree candidate(&source.context, raw);
    ASSERT_NE(candidate.root(), nullptr);
    ASSERT_EQ(planRefreshJoinRewriteInputs(&candidate.context,
                                            candidate.root()), 0);
    bool supported = false;
    ASSERT_EQ(planCheckJoinRewriteConditions(&candidate.context,
                                               candidate.root(), &supported),
              0);
    ASSERT_TRUE(supported);
    ASSERT_EQ(planRefreshJoinRewriteInputs(&candidate.context,
                                            candidate.root()), 0);
    std::set<std::string> outputs;
    SNode* target = nullptr;
    FOREACH(target, candidate.root()->pTargets) {
      ASSERT_EQ(nodeType(target), QUERY_NODE_COLUMN);
      const auto* column = (const SColumnNode*)target;
      outputs.insert(std::string(column->tableAlias) + "." + column->colName);
    }
    EXPECT_EQ(outputs, (std::set<std::string>{"a.c1", "c.c1"}));
    EXPECT_EQ(LIST_LENGTH(candidate.root()->pTargets), 2);
    choice.reset();
    model.reset();
    const auto* props = planGetJoinInputProps(&source.context, source.root());
    ASSERT_NE(props, nullptr);
    std::vector<SJoinOutputCol> original;
    for (size_t i = 0; i < taosArrayGetSize(props->pOutputCols); ++i) {
      original.push_back(*(const SJoinOutputCol*)
          taosArrayGet(props->pOutputCols, i));
    }
    SLogicSubplan subplan = {};
    subplan.pNode = source.root();
    const auto* originalRoot = source.root();
    for (int32_t retry = 0; retry < 2; ++retry) {
      bool changed = true;
      const int32_t code = planRewriteJoinParallel(&source.context, &subplan,
                                                     &changed);
      source.tree.release();
      source.tree.reset((SNode*)subplan.pNode);
      EXPECT_EQ(code, TSDB_CODE_SUCCESS);
      EXPECT_FALSE(changed);
      EXPECT_EQ(subplan.pNode, originalRoot);
      EXPECT_EQ(nodeText(source.tree.get()), before);
      props = planGetJoinInputProps(&source.context, source.root());
      ASSERT_NE(props, nullptr);
      ASSERT_EQ(taosArrayGetSize(props->pOutputCols), original.size());
      for (size_t i = 0; i < original.size(); ++i) {
        const auto* output = (const SJoinOutputCol*)
            taosArrayGet(props->pOutputCols, i);
        EXPECT_EQ(output->id.relationId, original[i].id.relationId);
        EXPECT_EQ(output->id.columnId, original[i].id.columnId);
        EXPECT_EQ(output->type.type, original[i].type.type);
        EXPECT_EQ(output->type.bytes, original[i].type.bytes);
        EXPECT_EQ(output->type.precision, original[i].type.precision);
      }
    }
  });
  run("SELECT a.c1 FROM rewrite_remote.metrics a "
      "JOIN t1 b ON a.c1=b.c1 JOIN t1 c ON a.c1=c.c1 "
      "JOIN t1 d ON a.c1=d.c1 WHERE a.c2>0");
  EXPECT_TRUE(sawOptimized);
#endif
}

TEST_F(PlanJoinParallelTest, searchBalancesPureStarUsingEqualityClasses) {
  useDb("root", "test");
  for (int32_t count : {4, 8}) {
    SCOPED_TRACE(count);
    std::string sql = "SELECT a.c1 FROM t1 a";
    for (int32_t i = 1; i < count; ++i) {
      const std::string alias(1, 'a' + i);
      sql += " JOIN t1 " + alias + " ON a.c1=" + alias + ".c1";
    }
    runJoinSearch(sql, count == 4 ? 2 : 3,
                  [](const SJoinTreeChoice* choice) {
      EXPECT_NE(choice->pEqClasses, nullptr);
      EXPECT_EQ(choice->pConflicts, nullptr);
      EXPECT_EQ(choice->pRootFilter, nullptr);
      EXPECT_TRUE(planJoinEqClassesConnect(choice->pEqClasses, 4, 8));
    });
  }
}

TEST_F(PlanJoinParallelTest, searchRespectsNonEquiConnectivity) {
  useDb("root", "test");
  runJoinSearch("SELECT a.c1 FROM t1 a JOIN t1 b ON a.c1<b.c1 "
                "JOIN t1 c ON b.c1<c.c1 JOIN t1 d ON c.c1<d.c1", 2);
  runJoinSearch("SELECT a.c1 FROM t1 a JOIN t1 b ON a.c1<b.c1 "
                "JOIN t1 c ON a.c1<c.c1 JOIN t1 d ON a.c1<d.c1", 0);
  runJoinSearch("SELECT a.c1 FROM t1 a JOIN t1 b ON a.c1=b.c1 "
                "JOIN t1 c ON a.c3=c.c3 JOIN t1 d ON a.c4=d.c4", 0);
}

TEST_F(PlanJoinParallelTest, searchKeepsIncompletePredicatesOutOfSubtrees) {
  useDb("root", "test");
  runJoinSearch("SELECT a.c1 FROM t1 a JOIN t1 b ON a.c1=b.c1 "
                "JOIN t1 c ON b.c1=c.c1 OR a.c3=c.c3 "
                "JOIN t1 d ON a.c1=d.c1 OR b.c3=d.c3", 0);
  for (const std::string condition : {"c.c1>0", "1=1"}) {
    runJoinSearch("SELECT a.c1 FROM t1 a JOIN t1 b ON a.c1=b.c1 "
                  "JOIN t1 c ON b.c1=c.c1 JOIN t1 d ON " + condition, 0);
  }
}

TEST_F(PlanJoinParallelTest, searchKeepsOriginalCrossProductRanges) {
  useDb("root", "test");
  runJoinSearch("SELECT a.c1 FROM (t1 a CROSS JOIN t1 b) "
                "JOIN t1 c ON b.c1=c.c1 JOIN t1 d ON c.c1=d.c1", 2,
                [](const SJoinTreeChoice* choice) {
    bool found = false;
    for (size_t i = 0; i < taosArrayGetSize(choice->pCells); ++i) {
      const auto* cell = planJoinTreeCell(choice, (int32_t)i);
      if (cell->valid && cell->isCross) {
        EXPECT_EQ(cell->inputs, 3U);
        found = true;
      }
    }
    EXPECT_TRUE(found);
  });
  runJoinSearch("SELECT a.c1 FROM t1 a CROSS JOIN "
                "(t1 b JOIN t1 c ON b.c1=c.c1 "
                "JOIN t1 d ON c.c1=d.c1)", 0);
}

TEST_F(PlanJoinParallelTest, searchKeepsAlreadyOptimalAndOversizedRegions) {
  useDb("root", "test");
  runJoinSearch("SELECT a.c1 FROM (t1 a JOIN t1 b ON a.c1=b.c1) "
                "JOIN (t1 c JOIN t1 d ON c.c1=d.c1) ON b.c1=c.c1", 0);
  std::string sql = "SELECT a.c1 FROM t1 a";
  for (char alias = 'b'; alias <= 'i'; ++alias) {
    sql += " LEFT JOIN t1 " + std::string(1, alias) +
           " ON a.c1=" + alias + ".c1";
  }
  runJoinSearch(sql, 0);
}

TEST_F(PlanJoinParallelTest, searchTracksEveryMixedOperatorOnce) {
  useDb("root", "test");
  for (int32_t count : {4, 8}) {
    std::string sql = "SELECT a.c1 FROM t1 a";
    for (int32_t i = 1; i < count; ++i) {
      const std::string previous(1, 'a' + i - 1);
      const std::string alias(1, 'a' + i);
      sql += " LEFT JOIN t1 " + alias + " ON " + previous + ".c1=" +
             alias + ".c1";
    }
    // The static CD-C rules block the balanced E/F/G/H subtree through
    // E -> D. For eight inputs, height three is outside this legal space.
    runJoinSearch(sql, count == 4 ? 2 : 4,
                  [count](const SJoinTreeChoice* choice) {
      const auto* root = planJoinTreeCell(choice, choice->rootCell);
      EXPECT_EQ(root->usedOps, (1U << (count - 1)) - 1);
      EXPECT_NE(choice->pConflicts, nullptr);
      EXPECT_EQ(choice->pEqClasses, nullptr);
      if (count == 8) {
        EXPECT_FALSE(planJoinSplitIsLegal(choice->pConflicts, 1, 48, 192,
                                           false));
      }
    });
  }
  runJoinSearch("SELECT a.c1 FROM t1 a LEFT JOIN t1 b ON a.c1=b.c1 "
                "LEFT JOIN t1 c ON a.c1=c.c1 "
                "LEFT JOIN t1 d ON a.c1=d.c1", 0);
}

TEST_F(PlanJoinParallelTest, searchBalancesReachableInnerStateAfterLeft) {
  useDb("root", "test");
  runJoinSearch("SELECT a.c1 FROM t1 a LEFT JOIN t1 b ON a.c1=b.c1 "
                "JOIN t1 c ON a.c1=c.c1 JOIN t1 d ON a.c1=d.c1 "
                "JOIN t1 e ON a.c1=e.c1", 3,
                [](const SJoinTreeChoice* choice) {
    EXPECT_EQ(choice->originalHeight, 4);
    EXPECT_EQ(choice->mode, JOIN_REGION_MIXED);
    std::vector<const SJoinTreeChoice*> pure;
    collectChosenPure(choice, choice->rootCell, pure);
    ASSERT_EQ(pure.size(), 1U);
    EXPECT_EQ(pure[0]->rootCell, 29);
    EXPECT_EQ(pure[0]->originalHeight, 3);
    EXPECT_EQ(taosArrayGetSize(pure[0]->pPredicates), 3U);
    EXPECT_TRUE(planJoinEqClassesConnect(pure[0]->pEqClasses, 4, 8));
    EXPECT_NE(pure[0]->pPredicates, choice->pRegion->pPredicates);
    checkNestedPureBags(choice, pure[0]);
  });
}

TEST_F(PlanJoinParallelTest, searchBalancesInnerStarBeforeFinalLeft) {
  useDb("root", "test");
  runJoinSearch("SELECT a.c1 FROM t1 a JOIN t1 b ON a.c1=b.c1 "
                "JOIN t1 c ON a.c1=c.c1 JOIN t1 d ON a.c1=d.c1 "
                "JOIN t1 e ON a.c1=e.c1 JOIN t1 f ON a.c1=f.c1 "
                "JOIN t1 g ON a.c1=g.c1 "
                "LEFT JOIN t1 h ON g.c1=h.c1", 4,
                [](const SJoinTreeChoice* choice) {
    EXPECT_EQ(choice->originalHeight, 7);
    std::vector<const SJoinTreeChoice*> pure;
    collectChosenPure(choice, choice->rootCell, pure);
    ASSERT_FALSE(pure.empty());
    std::set<const SJoinTreeChoice*> owners;
    for (size_t i = 0; i < taosArrayGetSize(choice->pCells); ++i) {
      const auto* cell = planJoinTreeCell(choice, (int32_t)i);
      if (cell->pPureChoice != nullptr) {
        EXPECT_TRUE(owners.insert(cell->pPureChoice).second);
      }
    }
    for (const auto* nested : pure) {
      EXPECT_EQ(owners.count(nested), 1U);
      EXPECT_EQ(nested->mode, JOIN_REGION_PURE_INNER);
      EXPECT_EQ(nested->pRegion, choice->pRegion);
      const auto* root = planJoinTreeCell(nested, nested->rootCell);
      ASSERT_NE(root, nullptr);
      EXPECT_EQ(root->inputs & 128U, 0U);
    }
  });
}

TEST_F(PlanJoinParallelTest, searchSplitsOnlyInnerSubsetOnConjunctions) {
  useDb("root", "test");
  runJoinSearch("SELECT a.c1 FROM t1 a LEFT JOIN t1 b ON a.c1=b.c1 "
                "JOIN t1 c ON a.c1=c.c1 JOIN t1 d ON a.c1=d.c1 "
                "JOIN t1 e ON a.c1=e.c1 AND e.c1>0 "
                "WHERE c.c1>0", 3,
                [](const SJoinTreeChoice* choice) {
    ASSERT_NE(choice->pRootFilter, nullptr);
    std::vector<const SJoinTreeChoice*> pure;
    collectChosenPure(choice, choice->rootCell, pure);
    ASSERT_EQ(pure.size(), 1U);
    EXPECT_EQ(taosArrayGetSize(pure[0]->pPredicates), 4U);
    for (size_t i = 0; i < taosArrayGetSize(pure[0]->pPredicates); ++i) {
      const auto* pred = (const SJoinParallelPredicate*)
          taosArrayGet(pure[0]->pPredicates, i);
      EXPECT_NE(pred->source, JOIN_PREDICATE_FILTER);
      EXPECT_NE(nodeType(pred->pExpr), QUERY_NODE_LOGIC_CONDITION);
      EXPECT_EQ(pred->inputs & 2U, 0U);
    }
    checkNestedPureBags(choice, pure[0]);
  });
}

TEST_F(PlanJoinParallelTest, searchDoesNotBorrowOuterOnOrRootFilter) {
  useDb("root", "test");
  const std::string prefix =
      "SELECT a.c1 FROM t1 a JOIN t1 b ON a.c1=b.c1 "
      "JOIN t1 c ON a.c1<c.c1 JOIN t1 d ON a.c1<d.c1 "
      "LEFT JOIN t1 e ON a.c1=e.c1";
  // Either extra equality would connect C/D if incorrectly borrowed into U.
  runJoinSearch(prefix + " AND c.c1=d.c1", 0);
  runJoinSearch(prefix + " WHERE c.c1=d.c1", 0);
}

TEST_F(PlanJoinParallelTest, searchKeepsOtherInnerOperatorsOutOfPureState) {
  useDb("root", "test");
  runJoinSearch("SELECT a.c1 FROM t1 a LEFT JOIN t1 b ON a.c1=b.c1 "
                "JOIN t1 c ON a.c1=c.c1 JOIN t1 d ON a.c1<d.c1 "
                "JOIN t1 e ON a.c1<e.c1 "
                "JOIN t1 f ON d.c1=e.c1 AND a.c1=f.c1", 4,
                [](const SJoinTreeChoice* choice) {
    bool found = false;
    for (size_t i = 0; i < taosArrayGetSize(choice->pCells); ++i) {
      const auto* cell = planJoinTreeCell(choice, (int32_t)i);
      if (cell->pPureChoice == nullptr) continue;
      // D=E comes from F's operator, outside the A/C/D/E state's U.
      EXPECT_NE(cell->inputs, 29U);
      found = true;
    }
    EXPECT_TRUE(found);
  });
}

TEST_F(PlanJoinParallelTest, searchDoesNotInjectAcrossCrossOperator) {
  useDb("root", "test");
  runJoinSearch("SELECT a.c1 FROM (t1 a CROSS JOIN t1 b) "
                "JOIN t1 c ON a.c1=c.c1 JOIN t1 d ON a.c1=d.c1 "
                "LEFT JOIN t1 e ON d.c1=e.c1", 3,
                [](const SJoinTreeChoice* choice) {
    for (size_t i = 0; i < taosArrayGetSize(choice->pCells); ++i) {
      EXPECT_EQ(planJoinTreeCell(choice, (int32_t)i)->pPureChoice, nullptr);
    }
  });
}

TEST_F(PlanJoinParallelTest, searchDoesNotInjectAcrossPinnedFilter) {
  useDb("root", "test");
  setJoinPropsCheck([](const SPlanContext* context, const SLogicNode* root,
                       bool optimized) {
    if (optimized) return;
    TreeCopy copy(context, root);
    ASSERT_EQ(copy.code, TSDB_CODE_SUCCESS);
    auto* outer = (SJoinLogicNode*)firstJoin(copy.root());
    auto* inner = (SJoinLogicNode*)nodesListGetNode(outer->node.pChildren, 0);
    ASSERT_EQ(nodeType(inner), QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(outer->node.pConditions, nullptr);
    ASSERT_EQ(inner->node.pConditions, nullptr);
    inner->node.pConditions = outer->node.pConditions;
    outer->node.pConditions = nullptr;
    ASSERT_EQ(planRebuildJoinInputProps(&copy.context, copy.root()), 0);
    auto model = prepareJoinModel(&copy.context, copy.root());
    ASSERT_NE(model, nullptr);
    const auto* region = regionFor(model.get(), &outer->node);
    ASSERT_NE(region, nullptr);
    SJoinTreeChoice* raw = nullptr;
    ASSERT_EQ(planSearchJoinTree(model.get(), region, &raw), 0);
    ChoicePtr choice(raw, planDestroyJoinTreeChoice);
    EXPECT_EQ(choice, nullptr);
  });
  run("SELECT a.c1 FROM t1 a JOIN t1 b ON a.c1=b.c1 "
      "JOIN t1 c ON a.c1=c.c1 JOIN t1 d ON a.c1=d.c1 "
      "LEFT JOIN t1 e ON d.c1=e.c1 WHERE a.c1>0");
}

TEST_F(PlanJoinParallelTest, searchRetriesAfterNestedPureAllocationFailure) {
  useDb("root", "test");
  setJoinPropsCheck([](const SPlanContext* context, const SLogicNode* root,
                       bool optimized) {
    if (optimized) return;
    const auto before = nodeText((const SNode*)root);
    auto model = prepareJoinModel(context, root);
    ASSERT_NE(model, nullptr);
    const auto* region = regionFor(model.get(), &firstJoin(root)->node);
    ASSERT_NE(region, nullptr);
    for (bool cells : {true, false}) {
      SJoinTreeChoice* failed = nullptr;
      int32_t code;
      {
        Stub stub;
        if (cells) {
          // The first cell table is mixed; the next belongs to its pure view.
          pureCellArrayCountdown = 2;
          stub.set(taosArrayInit_s, failNestedPureCells);
        } else {
          stub.set(taosArrayInit, failPurePredicateArray);
        }
        code = planSearchJoinTree(model.get(), region, &failed);
      }
      ChoicePtr cleanup(failed, planDestroyJoinTreeChoice);
      EXPECT_EQ(code, TSDB_CODE_OUT_OF_MEMORY);
      EXPECT_EQ(failed, nullptr);
      EXPECT_EQ(nodeText((const SNode*)root), before);
    }
    SJoinTreeChoice* raw = nullptr;
    ASSERT_EQ(planSearchJoinTree(model.get(), region, &raw), 0);
    ChoicePtr retry(raw, planDestroyJoinTreeChoice);
    ASSERT_NE(retry, nullptr);
    EXPECT_EQ(planJoinTreeCell(retry.get(), retry->rootCell)->height, 3);
    EXPECT_EQ(nodeText((const SNode*)root), before);
  });
  run("SELECT a.c1 FROM t1 a LEFT JOIN t1 b ON a.c1=b.c1 "
      "JOIN t1 c ON a.c1=c.c1 JOIN t1 d ON a.c1=d.c1 "
      "JOIN t1 e ON a.c1=e.c1");
}

TEST_F(PlanJoinParallelTest, searchCarriesRootFilterToChosenRegionOutput) {
  useDb("root", "test");
  runJoinSearch("SELECT a.c1,b.c1,c.c1,d.c1 FROM t1 a "
                "LEFT JOIN t1 b ON a.c1=b.c1 "
                "LEFT JOIN t1 c ON b.c1=c.c1 "
                "LEFT JOIN t1 d ON c.c1=d.c1 WHERE a.c1>0", 2,
                [](const SJoinTreeChoice* choice) {
    EXPECT_NE(choice->pRootFilter, nullptr);
    EXPECT_EQ(choice->pRootFilter, choice->pConflicts->pRootFilter);
    const auto* root = planJoinTreeCell(choice, choice->rootCell);
    EXPECT_NE(root->opId, 0);
    checkJoinChoiceBags(choice);
  });
}

TEST_F(PlanJoinParallelTest, searchPreservesRightJoinOrientation) {
  useDb("root", "test");
  runJoinSearch("SELECT a.c1,b.c1,c.c1,d.c1 FROM t1 a "
                "RIGHT JOIN t1 b ON a.c1=b.c1 "
                "RIGHT JOIN t1 c ON b.c1=c.c1 "
                "RIGHT JOIN t1 d ON c.c1=d.c1", 2,
                [](const SJoinTreeChoice* choice) {
    checkJoinChoiceBags(choice);
  });
}

TEST_F(PlanJoinParallelTest, searchRetainsNonRootFilterScope) {
  useDb("root", "test");
  setJoinPropsCheck([](const SPlanContext* context, const SLogicNode* root,
                       bool optimized) {
    if (optimized) return;
    TreeCopy copy(context, root);
    ASSERT_EQ(copy.code, TSDB_CODE_SUCCESS);
    auto* top = (SJoinLogicNode*)firstJoin(copy.root());
    ASSERT_NE(top, nullptr);
    auto* middle = (SJoinLogicNode*)nodesListGetNode(top->node.pChildren, 0);
    ASSERT_EQ(nodeType(middle), QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(top->node.pConditions, nullptr);
    ASSERT_EQ(middle->node.pConditions, nullptr);
    // A filter on A/B/C must retain that scope, preventing a 2+2 split.
    middle->node.pConditions = top->node.pConditions;
    top->node.pConditions = nullptr;
    ASSERT_EQ(planRebuildJoinInputProps(&copy.context, copy.root()), 0);
    const auto before = nodeText(copy.tree.get());
    auto model = prepareJoinModel(&copy.context, copy.root());
    ASSERT_NE(model, nullptr);
    const auto* region = regionFor(model.get(), &top->node);
    ASSERT_NE(region, nullptr);
    SJoinConflictModel* rawModel = nullptr;
    ASSERT_EQ(planBuildJoinConflicts(model.get(), region, &rawModel), 0);
    ConflictPtr conflicts(rawModel, planDestroyJoinConflicts);
    ASSERT_NE(conflicts, nullptr);
    EXPECT_EQ(minimumConflictHeight(conflicts.get()), 3);
    SJoinTreeChoice* raw = nullptr;
    EXPECT_EQ(planSearchJoinTree(model.get(), region, &raw), 0);
    ChoicePtr choice(raw, planDestroyJoinTreeChoice);
    EXPECT_EQ(choice, nullptr);
    EXPECT_EQ(nodeText(copy.tree.get()), before);
  });
  run("SELECT a.c1 FROM t1 a LEFT JOIN t1 b ON a.c1=b.c1 "
      "LEFT JOIN t1 c ON b.c1=c.c1 LEFT JOIN t1 d ON c.c1=d.c1 "
      "WHERE a.c1>0");
}

TEST_F(PlanJoinParallelTest, searchMixedHeightMatchesIndependentEnumeration) {
  useDb("root", "test");
  const std::array<std::string, 5> types = {
      "JOIN", "LEFT JOIN", "FULL JOIN", "LEFT SEMI JOIN", "LEFT ANTI JOIN"};
  for (size_t x = 0; x < types.size(); ++x) {
    for (size_t y = 0; y < types.size(); ++y) {
      for (size_t z = 0; z < types.size(); ++z) {
        if (x == 0 && y == 0 && z == 0) continue;
        for (bool strict : {false, true}) {
          const std::array<size_t, 3> joins = {x, y, z};
          SJoinInputSet visible = 1;
          char previous = 'a';
          std::string from = "t1 a";
          for (int32_t i = 0; i < 3; ++i) {
            const char alias = 'b' + i;
            const std::string left = std::string(1, previous) + ".c1";
            const std::string right = std::string(1, alias) + ".c1";
            const std::string predicate = strict ? left + "=" + right
                : "(" + left + " IS NULL OR " + right + " IS NULL OR " +
                      left + "=" + right + ")";
            from = "(" + from + " " + types[joins[i]] + " t1 " + alias +
                   " ON " + predicate + ")";
            if (joins[i] < 3) {
              visible |= UINT64_C(1) << (i + 1);
              previous = alias;
            }
          }
          std::string columns;
          for (int32_t i = 0; i < 4; ++i) {
            if (!planJoinInputSetContains(visible, i)) continue;
            if (!columns.empty()) columns += ",";
            columns += std::string(1, 'a' + i) + ".c1";
          }
          const std::string sql = "SELECT " + columns + " FROM " + from;
          SCOPED_TRACE(sql);
          setJoinPropsCheck([visible](const SPlanContext* context,
                                      const SLogicNode* root, bool optimized) {
            if (optimized) return;
            const auto before = nodeText((const SNode*)root);
            auto model = prepareJoinModel(context, root);
            ASSERT_NE(model, nullptr);
            ASSERT_EQ(taosArrayGetSize(model->pRegions), 1U);
            const auto* region = regionAt(model.get(), 0);
            ASSERT_EQ(taosArrayGetSize(region->pOps), 3U);
            SJoinConflictModel* rawModel = nullptr;
            ASSERT_EQ(planBuildJoinConflicts(model.get(), region, &rawModel),
                      0);
            ConflictPtr conflicts(rawModel, planDestroyJoinConflicts);
            ASSERT_NE(conflicts, nullptr);
            const int32_t minimum = minimumConflictHeight(conflicts.get());
            ASSERT_GE(minimum, 2);
            SJoinTreeChoice* raw = nullptr;
            ASSERT_EQ(planSearchJoinTree(model.get(), region, &raw), 0);
            ChoicePtr choice(raw, planDestroyJoinTreeChoice);
            if (minimum == 3) {
              EXPECT_EQ(choice, nullptr);
            } else {
              ASSERT_NE(choice, nullptr);
              EXPECT_EQ(planJoinTreeCell(choice.get(), choice->rootCell)
                            ->height, minimum);
              checkJoinChoiceBags(choice.get(), visible);
            }
            EXPECT_EQ(nodeText((const SNode*)root), before);
          });
          run(sql);
        }
      }
    }
  }
}

TEST_F(PlanJoinParallelTest, searchFailurePreservesTreeAndRetries) {
  useDb("root", "test");
  for (const std::string join : {" JOIN ", " LEFT JOIN "}) {
    SCOPED_TRACE(join);
    setJoinPropsCheck([](const SPlanContext* context, const SLogicNode* root,
                         bool optimized) {
      if (optimized) return;
      const auto before = nodeText((const SNode*)root);
      auto model = prepareJoinModel(context, root);
      ASSERT_NE(model, nullptr);
      const auto* region = regionAt(model.get(), 0);
      for (bool cells : {true, false}) {
        SJoinTreeChoice* choice = nullptr;
        int32_t code;
        {
          Stub stub;
          if (cells) stub.set(taosArrayInit_s, failArrayInit);
          else stub.set(taosArrayInit, failArrayInit);
          code = planSearchJoinTree(model.get(), region, &choice);
        }
        EXPECT_EQ(code, TSDB_CODE_OUT_OF_MEMORY);
        EXPECT_EQ(choice, nullptr);
        EXPECT_EQ(nodeText((const SNode*)root), before);
      }
      SJoinTreeChoice* raw = nullptr;
      EXPECT_EQ(planSearchJoinTree(nullptr, region, &raw),
                TSDB_CODE_INVALID_PARA);
      EXPECT_EQ(raw, nullptr);
      EXPECT_EQ(planSearchJoinTree(model.get(), nullptr, &raw),
                TSDB_CODE_INVALID_PARA);
      EXPECT_EQ(planSearchJoinTree(model.get(), region, nullptr),
                TSDB_CODE_INVALID_PARA);
      ASSERT_EQ(planSearchJoinTree(model.get(), region, &raw), 0);
      ChoicePtr retry(raw, planDestroyJoinTreeChoice);
      ASSERT_NE(retry, nullptr);
      EXPECT_EQ(planJoinTreeCell(retry.get(), retry->rootCell)->height, 2);
      EXPECT_EQ(planJoinTreeCell(retry.get(), -1), nullptr);
      EXPECT_EQ(planJoinTreeCell(nullptr, 0), nullptr);
      EXPECT_EQ(planJoinTreeCell(retry.get(), (int32_t)
                                  taosArrayGetSize(retry->pCells)), nullptr);
      EXPECT_EQ(nodeText((const SNode*)root), before);
    });
    run("SELECT a.c1 FROM t1 a" + join + "t1 b ON a.c1=b.c1" + join +
        "t1 c ON b.c1=c.c1" + join + "t1 d ON c.c1=d.c1");
  }
}

TEST_F(PlanJoinParallelTest, searchCountsOpaqueInputsAtHeightZero) {
  useDb("root", "test");
  runJoinSearch("SELECT a.c1 FROM "
                "(SELECT y.c1 FROM (t1 x JOIN t1 y ON x.c1=y.c1) "
                "JOIN t1 z ON y.c1=z.c1) a "
                "JOIN t1 b ON a.c1=b.c1 JOIN t1 c ON b.c1=c.c1 "
                "JOIN t1 d ON c.c1=d.c1", 2,
                [](const SJoinTreeChoice* choice) {
    EXPECT_EQ(choice->originalHeight, 3);
    EXPECT_EQ(taosArrayGetSize(choice->pRegion->pInputs), 4U);
  });
}

TEST_F(PlanJoinParallelTest, conflictsRequireNullRejectionForLeftAssociation) {
  useDb("root", "test");
  for (bool strict : {true, false}) {
    SCOPED_TRACE(strict);
    const std::string on = strict ? "b.c1=c.c1"
                                  : "b.c1 IS NULL OR b.c1=c.c1";
    runConflicts("SELECT a.c1 FROM t1 a LEFT JOIN t1 b ON a.c1=b.c1 "
                 "LEFT JOIN t1 c ON " + on,
                 [strict](const SJoinConflictModel* model) {
      EXPECT_EQ(planJoinSplitIsLegal(model, 0, 2, 4, false), strict);
      EXPECT_TRUE(planJoinSplitIsLegal(model, 1, 1, 6, false));
      EXPECT_FALSE(planJoinSplitIsLegal(model, 0, 1, 4, false));
    });
  }
}

TEST_F(PlanJoinParallelTest, conflictsNormalizeRightAndCandidateSwap) {
  useDb("root", "test");
  runConflicts("SELECT a.c1 FROM t1 a RIGHT JOIN t1 b ON a.c1=b.c1 "
               "LEFT JOIN t1 c ON b.c1=c.c1",
               [](const SJoinConflictModel* model) {
    const auto* child = (const SJoinConflictOp*)taosArrayGet(model->pOps, 1);
    EXPECT_TRUE(child->normalizedSwap);
    EXPECT_EQ(child->left, 2U);
    EXPECT_EQ(child->right, 1U);
    EXPECT_TRUE(planJoinSplitIsLegal(model, 0, 2, 4, false));
    EXPECT_TRUE(planJoinSplitIsLegal(model, 1, 1, 6, false));
    EXPECT_TRUE(planJoinSplitIsLegal(model, 1, 6, 1, true));
    EXPECT_FALSE(planJoinSplitIsLegal(model, 1, 6, 1, false));
  });
}

TEST_F(PlanJoinParallelTest, conflictsPreserveOnDependenciesInMixedRegions) {
  useDb("root", "test");
  runConflicts("SELECT a.c1 FROM t1 a LEFT JOIN t1 b ON a.c1=b.c1 "
               "JOIN t1 c ON b.c1=c.c1 AND a.c3=c.c3",
               [](const SJoinConflictModel* model) {
    const auto* op = (const SJoinConflictOp*)taosArrayGet(model->pOps, 0);
    EXPECT_EQ(op->requiredLeft, 3U);
    EXPECT_EQ(op->requiredRight, 4U);
    EXPECT_FALSE(planJoinSplitIsLegal(model, 0, 2, 4, false));
    EXPECT_FALSE(planJoinSplitIsLegal(model, 0, 0, 4, false));
    EXPECT_FALSE(planJoinSplitIsLegal(model, 0, 3, 6, false));
    EXPECT_FALSE(planJoinSplitIsLegal(model, 0, 3, 12, false));
    EXPECT_FALSE(planJoinSplitIsLegal(model, -1, 3, 4, false));
    EXPECT_FALSE(planJoinSplitIsLegal(model, 2, 3, 4, false));
    EXPECT_FALSE(planJoinSplitIsLegal(nullptr, 0, 3, 4, false));
  });
}

TEST_F(PlanJoinParallelTest, conflictsFullLeftNeedsSharedSideNullRejection) {
  useDb("root", "test");
  for (bool shared : {false, true}) {
    const std::string condition = shared ? "a.c1>0"
                                         : "a.c1 IS NULL AND c.c1>0";
    SCOPED_TRACE(condition);
    runConflicts("SELECT a.c1,b.c1,c.c1 FROM "
                 "(t1 a FULL JOIN t1 b ON a.c1=b.c1) LEFT JOIN t1 c ON " +
                     condition,
                 [shared](const SJoinConflictModel* model) {
      EXPECT_EQ(planJoinSplitIsLegal(model, 0, 1, 4, false), shared);
      checkConflictBags(model, 7);
    });
  }
}

TEST_F(PlanJoinParallelTest, conflictsAnchorDegenerateOnToOriginalSides) {
  useDb("root", "test");
  for (const std::string on : {"b.c1>0", "1=1", "NULL=1"}) {
    SCOPED_TRACE(on);
    runConflicts("SELECT a.c1 FROM t1 a LEFT JOIN t1 b ON a.c1=b.c1 "
                 "LEFT JOIN t1 c ON " + on,
                 [](const SJoinConflictModel* model) {
      EXPECT_FALSE(planJoinSplitIsLegal(model, 0, 1, 2, false));
      EXPECT_FALSE(planJoinSplitIsLegal(model, 0, 4, 3, false));
    });
  }
}

TEST_F(PlanJoinParallelTest, conflictsNormalizeRightSemiAndAnti) {
  useDb("root", "test");
  for (const std::string type : {"RIGHT SEMI JOIN", "RIGHT ANTI JOIN"}) {
    SCOPED_TRACE(type);
    runConflicts("SELECT b.c1 FROM (t1 a " + type +
                 " t1 b ON a.c1=b.c1) LEFT ANTI JOIN t1 c ON b.c1=c.c1",
                 [](const SJoinConflictModel* model) {
      const auto* child = (const SJoinConflictOp*)taosArrayGet(model->pOps, 1);
      EXPECT_TRUE(child->normalizedSwap);
      EXPECT_EQ(child->left, 2U);
      EXPECT_EQ(child->right, 1U);
      EXPECT_TRUE(planJoinSplitIsLegal(model, 0, 2, 4, false));
      EXPECT_TRUE(planJoinSplitIsLegal(model, 1, 1, 6, false));
      EXPECT_TRUE(planJoinSplitIsLegal(model, 1, 6, 1, true));
    });
  }
}

TEST_F(PlanJoinParallelTest, conflictsConstrainCrossProductsInMixedRegions) {
  useDb("root", "test");
  runConflicts("SELECT a.c1 FROM t1 a LEFT JOIN "
               "(t1 b CROSS JOIN t1 c) ON a.c1=b.c1",
               [](const SJoinConflictModel* model) {
    const auto* cross = (const SJoinConflictOp*)taosArrayGet(model->pOps, 1);
    EXPECT_EQ(cross->onInputs, 0U);
    EXPECT_FALSE(planJoinSplitIsLegal(model, 0, 1, 2, false));
    EXPECT_FALSE(planJoinSplitIsLegal(model, 0, 1, 4, false));
    EXPECT_FALSE(planJoinSplitIsLegal(model, 1, 1, 2, false));
  });
  runConflicts("SELECT a.c1 FROM (t1 a CROSS JOIN t1 b) "
               "LEFT JOIN t1 c ON b.c1=c.c1",
               [](const SJoinConflictModel* model) {
    EXPECT_TRUE(planJoinSplitIsLegal(model, 0, 2, 4, false));
    EXPECT_TRUE(planJoinSplitIsLegal(model, 1, 1, 6, false));
  });
}

TEST_F(PlanJoinParallelTest, conflictsInspectAllDescendantsAndNarrowRequired) {
  useDb("root", "test");
  runConflicts("SELECT a.c1 FROM ((t1 a JOIN t1 b ON a.c1=b.c1) "
               "LEFT JOIN t1 c ON b.c1=c.c1) JOIN t1 d ON c.c1=d.c1",
               [](const SJoinConflictModel* model) {
    const auto* root = (const SJoinConflictOp*)taosArrayGet(model->pOps, 0);
    bool narrowed = false;
    for (size_t i = 0; i < taosArrayGetSize(root->pRules); ++i) {
      const auto* rule =
          (const SJoinConflictRule*)taosArrayGet(root->pRules, i);
      narrowed |= rule->trigger == 4 && rule->required == 2;
    }
    EXPECT_TRUE(narrowed);
    EXPECT_FALSE(planJoinSplitIsLegal(model, 0, 4, 8, false));
    EXPECT_TRUE(planJoinSplitIsLegal(model, 0, 6, 8, false));
  });
  runConflicts("SELECT a.c1 FROM ((t1 a LEFT JOIN t1 b ON a.c1=b.c1) "
               "JOIN t1 c ON a.c1=c.c1) JOIN t1 d ON c.c1=d.c1",
               [](const SJoinConflictModel* model) {
    EXPECT_FALSE(planJoinSplitIsLegal(model, 0, 6, 8, false));
    EXPECT_TRUE(planJoinSplitIsLegal(model, 0, 4, 8, false));
  });
}

TEST_F(PlanJoinParallelTest, conflictsKeepRootFilterAtRegionOutput) {
  useDb("root", "test");
  runConflicts("SELECT a.c1 FROM t1 a LEFT JOIN t1 b ON a.c1=b.c1 "
               "LEFT JOIN t1 c ON b.c1=c.c1 WHERE a.c1>0",
               [](const SJoinConflictModel* model) {
    const auto* root = (const SJoinConflictOp*)taosArrayGet(model->pOps, 0);
    EXPECT_FALSE(root->pinnedFilter);
    EXPECT_EQ(model->pRootFilter, root->pNode->node.pConditions);
    EXPECT_EQ(root->nr, 6U);
    EXPECT_EQ(root->requiredLeft, 2U);
    EXPECT_TRUE(planJoinSplitIsLegal(model, 0, 2, 4, false));
  });
}

TEST_F(PlanJoinParallelTest, conflictsSkipPureAndUnsupportedRegions) {
  useDb("root", "test");
  setJoinPropsCheck([](const SPlanContext* context, const SLogicNode* root,
                       bool optimized) {
    if (optimized) return;
    auto model = prepareJoinModel(context, root);
    ASSERT_NE(model, nullptr);
    ASSERT_EQ(taosArrayGetSize(model->pRegions), 1U);
    const auto* region = regionAt(model.get(), 0);
    SJoinConflictModel* conflicts = nullptr;
    EXPECT_EQ(planBuildJoinConflicts(model.get(), region, &conflicts), 0);
    EXPECT_EQ(conflicts, nullptr);
    EXPECT_EQ(planBuildJoinConflicts(nullptr, region, &conflicts),
              TSDB_CODE_INVALID_PARA);
    EXPECT_EQ(planBuildJoinConflicts(model.get(), nullptr, &conflicts),
              TSDB_CODE_INVALID_PARA);
    EXPECT_EQ(planBuildJoinConflicts(model.get(), region, nullptr),
              TSDB_CODE_INVALID_PARA);
  });
  run("SELECT a.c1 FROM t1 a JOIN t1 b ON a.c1=b.c1");
  std::string sql = "SELECT a.c1 FROM t1 a";
  for (char alias = 'b'; alias <= 'i'; ++alias) {
    sql += " LEFT JOIN t1 " + std::string(1, alias) +
           " ON a.c1=" + alias + ".c1";
  }
  run(sql);
}

TEST_F(PlanJoinParallelTest, conflictsTreatTemporalSubtreeAsOneInput) {
  useDb("root", "test");
  runConflicts("SELECT a.c1 FROM t1 a LEFT JOIN "
               "(stream_t2 b LEFT ASOF JOIN stream_t2 c ON b.ts>=c.ts) "
               "ON a.c1=b.c1 AND a.c3=c.c1",
               [](const SJoinConflictModel* model) {
    ASSERT_EQ(taosArrayGetSize(model->pOps), 1U);
    const auto* op = (const SJoinConflictOp*)taosArrayGet(model->pOps, 0);
    EXPECT_EQ(model->inputs, 3U);
    EXPECT_FALSE(op->normalizedSwap);
    EXPECT_EQ(op->left, 1U);
    EXPECT_EQ(op->right, 2U);
    EXPECT_EQ(op->requiredRight, 2U);
    EXPECT_EQ(op->nr, 3U);
  });
}

TEST_F(PlanJoinParallelTest, conflictsFailureReleasesPartialModelAndRetries) {
  useDb("root", "test");
  setJoinPropsCheck([](const SPlanContext* context, const SLogicNode* root,
                       bool optimized) {
    if (optimized) return;
    const auto before = nodeText((const SNode*)root);
    auto model = prepareJoinModel(context, root);
    ASSERT_NE(model, nullptr);
    const auto* region = regionAt(model.get(), 0);
    for (int32_t failure = 1; failure <= 3; ++failure) {
      SJoinConflictModel* conflicts = nullptr;
      int32_t code;
      {
        Stub stub;
        conflictArrayCountdown = failure;
        stub.set(taosArrayInit, failConflictArrayInit);
        code = planBuildJoinConflicts(model.get(), region, &conflicts);
      }
      EXPECT_EQ(code, TSDB_CODE_OUT_OF_MEMORY);
      EXPECT_EQ(conflicts, nullptr);
      EXPECT_EQ(nodeText((const SNode*)root), before);
    }
    SJoinConflictModel* raw = nullptr;
    ASSERT_EQ(planBuildJoinConflicts(model.get(), region, &raw), 0);
    ConflictPtr retry(raw, planDestroyJoinConflicts);
    ASSERT_NE(retry, nullptr);
    EXPECT_TRUE(planJoinSplitIsLegal(retry.get(), 0, 3, 4, false));
  });
  run("SELECT a.c1 FROM t1 a LEFT JOIN t1 b ON a.c1=b.c1 "
      "LEFT JOIN t1 c ON b.c1=c.c1");
}

TEST_F(PlanJoinParallelTest, conflictsPreserveBagsAcrossOperatorProperties) {
  useDb("root", "test");
  const std::array<std::string, 5> joins = {
      "JOIN", "LEFT JOIN", "FULL JOIN", "LEFT SEMI JOIN", "LEFT ANTI JOIN"};
  auto predicate = [](char left, char right, bool strict) {
    const std::string l = std::string(1, left) + ".c1";
    const std::string r = std::string(1, right) + ".c1";
    return strict ? l + "=" + r
                   : "(" + l + " IS NULL OR " + r + " IS NULL OR " +
                         l + "=" + r + ")";
  };
  for (int32_t pattern = 0; pattern < 4; ++pattern) {
    for (size_t x = 0; x < joins.size(); ++x) {
      for (size_t y = 0; y < joins.size(); ++y) {
        if (x == 0 && y == 0) continue;
        // A SEMI/ANTI matching column cannot feed an upper ON in this model.
        if ((pattern == 0 && x >= 3) || (pattern == 3 && y >= 3)) continue;
        const bool leftNested = pattern < 2;
        SJoinInputSet visible = leftNested ? (x >= 3 ? 1 : 3)
                                            : (y >= 3 ? 2 : 6);
        if (leftNested && y < 3) visible |= 4;
        if (!leftNested) visible = x >= 3 ? 1 : visible | 1;
        std::string select;
        for (int32_t i = 0; i < 3; ++i) {
          if (!planJoinInputSetContains(visible, i)) continue;
          if (!select.empty()) select += ",";
          select += std::string(1, 'a' + i) + ".c1";
        }
        for (bool px : {false, true}) {
          for (bool py : {false, true}) {
            const std::string p = predicate('a', pattern == 3 ? 'c' : 'b',
                                             px);
            const std::string q = predicate(pattern == 1 ? 'a' : 'b', 'c',
                                             py);
            const std::string from = leftNested
                ? "(t1 a " + joins[x] + " t1 b ON " + p + ") " + joins[y] +
                      " t1 c ON " + q
                : "t1 a " + joins[x] + " (t1 b " + joins[y] +
                      " t1 c ON " + q + ") ON " + p;
            const std::string sql = "SELECT " + select + " FROM " + from;
            SCOPED_TRACE(sql);
            runConflicts(sql, [visible](const SJoinConflictModel* model) {
              checkConflictBags(model, visible);
            });
          }
        }
      }
    }
  }
}

TEST_F(PlanJoinParallelTest, nullRejectionRecognizesStrictOperators) {
  useDb("root", "test");
  const std::vector<std::pair<std::string, SJoinInputSet>> cases = {
      {"b.c1=c.c1", 6}, {"b.c1<>c.c1", 6}, {"b.c1<c.c1", 6},
      {"b.c1<=c.c1", 6}, {"b.c1>c.c1", 6}, {"b.c1>=c.c1", 6},
      {"b.c1+1>c.c1", 6}, {"b.c1-c.c1>0", 6}, {"b.c1*c.c1>0", 6},
      {"b.c1/c.c1>0", 6}, {"b.c1%c.c1>0", 6}, {"-b.c1>0", 2},
      {"(b.c1 & c.c1)>0", 6}, {"(b.c1 | c.c1)>0", 6},
      {"b.c2 LIKE 'x%'", 2}, {"b.c2 NOT LIKE 'x%'", 2},
      {"b.c2 MATCH 'x.*'", 2}, {"b.c2 NMATCH 'x.*'", 2},
      {"a.ts IS NOT NULL", 1}};
  for (const auto& item : cases) {
    SCOPED_TRACE(item.first);
    runNullRejection(item.first, item.second);
  }
}

TEST_F(PlanJoinParallelTest, nullRejectionCombinesAndOrWithoutSplittingOn) {
  useDb("root", "test");
  const std::vector<std::pair<std::string, SJoinInputSet>> cases = {
      {"a.c1=b.c1 AND b.c1=c.c1", 7},
      {"a.c1=b.c1 OR b.c1=c.c1", 2},
      {"b.c1>0 OR c.c1>0", 0},
      {"(b.c1>0 OR b.c3>0) AND c.c1>0", 6},
      {"b.c1 IS NULL OR b.c1=c.c1", 0},
      {"b.c1 IS NULL AND b.c1=c.c1", 6},
      {"b.c1 IS NOT NULL", 2}, {"b.c1 IS NULL", 0}};
  for (const auto& item : cases) {
    SCOPED_TRACE(item.first);
    runNullRejection(item.first, item.second);
  }
}

TEST_F(PlanJoinParallelTest, nullRejectionHandlesNotWithThreeValuedLogic) {
  useDb("root", "test");
  const std::vector<std::pair<std::string, SJoinInputSet>> cases = {
      {"NOT(b.c1=c.c1)", 6}, {"NOT(b.c1 IS NOT NULL)", 0},
      {"NOT(b.c1 IS NULL)", 2},
      {"NOT(a.c1=b.c1 AND b.c1=c.c1)", 2},
      {"NOT(a.c1=b.c1 OR b.c1=c.c1)", 7},
      {"NOT(b.c1 IS NOT NULL AND c.c1>0)", 0}};
  for (const auto& item : cases) {
    SCOPED_TRACE(item.first);
    runNullRejection(item.first, item.second);
  }
}

TEST_F(PlanJoinParallelTest, nullRejectionPropagatesKnownStrictFunctions) {
  useDb("root", "test");
  const std::vector<std::string> conditions = {
      "ABS(b.c1)>c.c1", "CEIL(b.c4)>c.c1", "FLOOR(b.c4)>c.c1",
      "CAST(b.c1 AS BIGINT)>c.c1", "LENGTH(b.c2)>c.c1",
      "LOWER(b.c2)=c.c2", "UPPER(b.c2)=c.c2",
      "ABS(CAST(b.c1 AS BIGINT))+c.c1>0"};
  for (const auto& condition : conditions) {
    SCOPED_TRACE(condition);
    runNullRejection(condition, 6);
  }
}

TEST_F(PlanJoinParallelTest, nullRejectionKeepsUnknownExpressionsConservative) {
  useDb("root", "test");
  const std::vector<std::pair<std::string, SJoinInputSet>> cases = {
      {"CASE WHEN b.c1 IS NULL THEN 1 ELSE b.c1 END>0", 0},
      {"CONCAT_WS(',',b.c2,'x')='x'", 0},
      {"b.c1 IN (1,2)", 0}, {"b.c1 NOT IN (1,2)", 0},
      {"b.c1>0 AND CONCAT_WS(',',c.c2,'x')='x'", 2},
      {"b.c1>0 OR CONCAT_WS(',',c.c2,'x')='x'", 0}};
  for (const auto& item : cases) {
    SCOPED_TRACE(item.first);
    runNullRejection(item.first, item.second);
  }
}

TEST_F(PlanJoinParallelTest, nullRejectionKeepsQuantifiedSubqueriesUnknown) {
  useDb("root", "test");
  // An empty ALL set is TRUE for a NULL operand, an empty ANY set FALSE.
  const std::vector<std::pair<std::string, SJoinInputSet>> cases = {
      {"c.c1 > ALL (SELECT c1 FROM t1)", 0},
      {"c.c1 <= ALL (SELECT c1 FROM t1)", 0},
      {"c.c1 >= ANY (SELECT c1 FROM t1)", 0},
      {"c.c1 < ANY (SELECT c1 FROM t1)", 0},
      {"c.c1 = (SELECT MAX(c1) FROM t1)", 0},
      {"b.c1=c.c1 AND c.c1 > ALL (SELECT c1 FROM t1)", 6},
      {"b.c1>0 OR c.c1 > ALL (SELECT c1 FROM t1)", 0}};
  for (const auto& item : cases) {
    SCOPED_TRACE(item.first);
    runNullRejection(item.first, item.second);
  }
}

TEST_F(PlanJoinParallelTest, nullRejectionHandlesBooleanAndNullConstants) {
  useDb("root", "test");
  const std::vector<std::pair<std::string, SJoinInputSet>> cases = {
      {"b.c1>0 AND (NULL IS NOT NULL)", 7},
      {"b.c1>0 AND (NULL IS NULL)", 2},
      {"b.c1>0 OR (NULL IS NOT NULL)", 2},
      {"b.c1>0 OR (NULL IS NULL)", 0},
      {"b.c1>0 AND (NULL=1)", 7}, {"b.c1>0 OR (NULL=1)", 2},
      {"NOT(b.c1>0 AND (NULL=1))", 2},
      {"NOT(b.c1>0 OR (NULL=1))", 7}};
  for (const auto& item : cases) {
    SCOPED_TRACE(item.first);
    runNullRejection(item.first, item.second);
  }
}

TEST_F(PlanJoinParallelTest, nullRejectionAgreesWithScalarEvaluation) {
  useDb("root", "test");
  const std::vector<std::pair<std::string, SJoinInputSet>> cases = {
      {"b.c1=c.c1", 6}, {"b.c1+1>c.c1", 6},
      {"ABS(CAST(b.c1 AS BIGINT))>c.c1", 6},
      {"b.c1 IS NOT NULL", 2}, {"NOT(b.c1 IS NULL)", 2},
      {"a.c1=b.c1 AND b.c1=c.c1", 7},
      {"a.c1=b.c1 OR b.c1=c.c1", 2},
      {"NOT(a.c1=b.c1 AND b.c1=c.c1)", 2},
      {"NOT(a.c1=b.c1 OR b.c1=c.c1)", 7},
      {"b.c1 IS NULL AND b.c1=c.c1", 6},
      {"b.c1>0 AND (NULL=1)", 7}, {"NOT(b.c1>0 OR (NULL=1))", 7}};
  for (const auto& item : cases) {
    SCOPED_TRACE(item.first);
    runNullRejection(item.first, item.second, true);
  }
}

TEST_F(PlanJoinParallelTest, nullRejectionTreatsBoundaryOutputAsOneInput) {
  useDb("root", "test");
  setJoinPropsCheck([](const SPlanContext* context, const SLogicNode* root,
                       bool optimized) {
    if (optimized) return;
    auto model = prepareJoinModel(context, root);
    ASSERT_NE(model, nullptr);
    const auto* join = firstJoin(root);
    ASSERT_NE(join, nullptr);
    const auto* region = regionFor(model.get(), &join->node);
    ASSERT_NE(region, nullptr);
    ASSERT_EQ(taosArrayGetSize(region->pInputs), 2U);
    const auto* props = planGetJoinInputProps(context, &join->node);
    EXPECT_EQ(planJoinCondRejectsNull(region, props, join->pFullOnCond), 3U);
  });
  run("SELECT a.c1 FROM t1 a LEFT JOIN "
      "(SELECT b.c1 AS k FROM t1 b JOIN t1 c ON b.c1=c.c1) q "
      "ON a.c1=q.k");
}

TEST_F(PlanJoinParallelTest, nullRejectionRejectsAmbiguousColumnProofs) {
  useDb("root", "test");
  setJoinPropsCheck([](const SPlanContext* context, const SLogicNode* root,
                       bool optimized) {
    if (optimized) return;
    auto model = prepareJoinModel(context, root);
    ASSERT_NE(model, nullptr);
    const auto* region = regionAt(model.get(), 0);
    const auto* props = planGetJoinInputProps(context, &region->pRoot->node);
    ASSERT_NE(props, nullptr);
    const auto* source = inputColumn(region, 0, "c1");
    ASSERT_NE(source, nullptr);
    SColumnNode column = *(const SColumnNode*)source->pExpr;
    EXPECT_EQ(planJoinCondRejectsNull(region, props, (SNode*)&column), 1U);
    column.hasDep = true;
    EXPECT_EQ(planJoinCondRejectsNull(region, props, (SNode*)&column), 0U);
    column.hasDep = false;
    strcpy(column.tableAlias, "missing");
    EXPECT_EQ(planJoinCondRejectsNull(region, props, (SNode*)&column), 0U);
    column = *(const SColumnNode*)source->pExpr;
    std::unique_ptr<SArray, decltype(&taosArrayDestroy)> columns(
        taosArrayDup(props->pOutputCols, nullptr), taosArrayDestroy);
    ASSERT_NE(columns, nullptr);
    SJoinOutputCol duplicate = *source;
    ++duplicate.id.relationId;
    ASSERT_NE(taosArrayPush(columns.get(), &duplicate), nullptr);
    SJoinInputProps ambiguous = *props;
    ambiguous.pOutputCols = columns.get();
    EXPECT_EQ(planJoinCondRejectsNull(region, &ambiguous, (SNode*)&column), 0U);
    SJoinParallelRegion unsupported = *region;
    unsupported.supported = false;
    EXPECT_EQ(planJoinCondRejectsNull(&unsupported, props, (SNode*)&column),
              0U);
    EXPECT_EQ(planJoinCondRejectsNull(nullptr, props, (SNode*)&column), 0U);
    EXPECT_EQ(planJoinCondRejectsNull(region, nullptr, (SNode*)&column), 0U);
    EXPECT_EQ(planJoinCondRejectsNull(region, props, nullptr), 0U);
  });
  run("SELECT a.c1 FROM t1 a JOIN t1 b ON a.c1=b.c1");
}

TEST_F(PlanJoinParallelTest, nullRejectionKeepsRuntimeValuesUnknown) {
  useDb("root", "test");
  setJoinPropsCheck([](const SPlanContext* context, const SLogicNode* root,
                       bool optimized) {
    if (optimized) return;
    auto model = prepareJoinModel(context, root);
    ASSERT_NE(model, nullptr);
    const auto* region = regionAt(model.get(), 0);
    const auto* props = planGetJoinInputProps(context, &region->pRoot->node);
    ASSERT_NE(props, nullptr);
    SValueNode value = {};
    value.node.type = QUERY_NODE_VALUE;
    value.node.resType.type = TSDB_DATA_TYPE_BOOL;
    value.translate = true;
    EXPECT_EQ(planJoinCondRejectsNull(region, props, (SNode*)&value), 3U);
    for (int32_t flag : {VALUE_FLAG_VAL_UNSET, VALUE_FLAG_RAW_SQL_FRAG,
                         VALUE_FLAG_REMOTE_PLAN,
                         VALUE_FLAG_DEFERRED_QUERY_TIME}) {
      value.flag = flag;
      EXPECT_EQ(planJoinCondRejectsNull(region, props, (SNode*)&value), 0U);
    }
    value.flag = 0;
    value.placeholderNo = 1;
    EXPECT_EQ(planJoinCondRejectsNull(region, props, (SNode*)&value), 0U);
    value.placeholderNo = 0;
    value.translate = false;
    EXPECT_EQ(planJoinCondRejectsNull(region, props, (SNode*)&value), 0U);
    SRemoteValueNode remote = {};
    remote.val.node.type = QUERY_NODE_REMOTE_VALUE;
    const auto* condition = region->pRoot->pFullOnCond;
    ASSERT_EQ(nodeType(condition), QUERY_NODE_OPERATOR);
    SOperatorNode comparison = *(const SOperatorNode*)condition;
    comparison.pRight = (SNode*)&remote;
    EXPECT_EQ(planJoinCondRejectsNull(region, props, (SNode*)&comparison), 0U);
  });
  run("SELECT a.c1 FROM t1 a JOIN t1 b ON a.c1=b.c1");
}

TEST_F(PlanJoinParallelTest, eqClassesConnectStarWithoutChangingPredicates) {
  useDb("root", "test");
  setJoinPropsCheck([](const SPlanContext* context, const SLogicNode* root,
                       bool optimized) {
    if (optimized) return;
    const std::string before = nodeText((const SNode*)root);
    auto model = prepareJoinModel(context, root);
    ASSERT_NE(model, nullptr);
    ASSERT_EQ(taosArrayGetSize(model->pRegions), 1U);
    const auto* region = regionAt(model.get(), 0);
    auto classes = buildEqClasses(model.get(), region);
    ASSERT_NE(classes, nullptr);
    ASSERT_EQ(taosArrayGetSize(classes->pMembers), 4U);
    int32_t common = -1;
    for (int32_t i = 0; i < 4; ++i) {
      const auto* column = inputColumn(region, i, "c1");
      ASSERT_NE(column, nullptr);
      const int32_t id = planJoinEqClassForColumn(classes.get(), &column->id);
      ASSERT_GE(id, 0);
      if (i == 0) common = id;
      EXPECT_EQ(id, common);
    }
    EXPECT_TRUE(planJoinEqClassesConnect(classes.get(), 4, 8));
    EXPECT_TRUE(planJoinEqClassesConnect(classes.get(), 3, 12));
    EXPECT_FALSE(planJoinEqClassesConnect(classes.get(), 0, 8));
    EXPECT_FALSE(planJoinEqClassesConnect(classes.get(), 3, 6));
    EXPECT_FALSE(planJoinEqClassesConnect(classes.get(), 1, 16));
    EXPECT_EQ(taosArrayGetSize(region->pPredicates), 3U);
    EXPECT_EQ(nodeText((const SNode*)root), before);
    auto again = buildEqClasses(model.get(), region);
    ASSERT_NE(again, nullptr);
    EXPECT_EQ(taosArrayGetSize(again->pMembers), 4U);
  });
  run("SELECT a.c1 FROM t1 a JOIN t1 b ON a.c1=b.c1 "
      "JOIN t1 c ON a.c1=c.c1 JOIN t1 d ON a.c1=d.c1");
}


TEST_F(PlanJoinParallelTest, eqClassesDoNotConnectDifferentColumnsOfOneInput) {
  useDb("root", "test");
  runEqClasses("SELECT a.c1 FROM t1 a JOIN t1 b ON a.c1=b.c1 "
               "JOIN t1 c ON b.c3=c.c3",
               [](const SJoinParallelRegion* region,
                  const SJoinEqClasses* classes) {
    EXPECT_EQ(taosArrayGetSize(classes->pMembers), 4U);
    EXPECT_TRUE(planJoinEqClassesConnect(classes, 1, 2));
    EXPECT_TRUE(planJoinEqClassesConnect(classes, 2, 4));
    EXPECT_FALSE(planJoinEqClassesConnect(classes, 1, 4));
    const auto* first = inputColumn(region, 1, "c1");
    const auto* second = inputColumn(region, 1, "c3");
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    EXPECT_NE(planJoinEqClassForColumn(classes, &first->id),
              planJoinEqClassForColumn(classes, &second->id));
  });
}

TEST_F(PlanJoinParallelTest, eqClassesMergeChainsAndRepeatedEqualities) {
  useDb("root", "test");
  runEqClasses("SELECT a.c1 FROM t1 a JOIN t1 b ON a.c1=b.c1 "
               "JOIN t1 c ON b.c1=c.c1 JOIN t1 d ON c.c1=d.c1 "
               "JOIN t1 e ON d.c1=e.c1 AND a.c1=e.c1 AND a.c1=b.c1",
               [](const SJoinParallelRegion* region,
                  const SJoinEqClasses* classes) {
    EXPECT_EQ(taosArrayGetSize(classes->pMembers), 5U);
    const auto* first = inputColumn(region, 0, "c1");
    ASSERT_NE(first, nullptr);
    const int32_t common = planJoinEqClassForColumn(classes, &first->id);
    ASSERT_GE(common, 0);
    for (int32_t i = 1; i < 5; ++i) {
      const auto* column = inputColumn(region, i, "c1");
      ASSERT_NE(column, nullptr);
      EXPECT_EQ(planJoinEqClassForColumn(classes, &column->id), common);
    }
    EXPECT_TRUE(planJoinEqClassesConnect(classes, 1, 16));
  });
}

TEST_F(PlanJoinParallelTest, eqClassesMergeManyColumnsInBothDirections) {
  useDb("root", "test");
  const int32_t columnCount = 1024;
  auto& builder = g_mockCatalogService->createTableBuilder(
      "test", "eq_rank", TSDB_NORMAL_TABLE, columnCount + 1);
  builder.setPrecision(TSDB_TIME_PRECISION_MILLI).setVgid(1)
      .addColumn("ts", TSDB_DATA_TYPE_TIMESTAMP);
  std::string sql = "SELECT a.k0 FROM eq_rank a JOIN eq_rank b ON ";
  for (int32_t i = 0; i < columnCount; ++i) {
    const std::string column = "k" + std::to_string(i);
    builder.addColumn(column, TSDB_DATA_TYPE_INT);
    if (i > 0) sql += " AND ";
    sql += i % 2 == 0 ? "a." + column + "=b.k0"
                      : "b.k0=a." + column;
  }
  builder.done();
  sql += " JOIN eq_rank c ON b.k0=c.k0";
  runEqClasses(sql, [columnCount](const SJoinParallelRegion* region,
                                  const SJoinEqClasses* classes) {
    EXPECT_EQ(taosArrayGetSize(classes->pMembers),
              (size_t)columnCount + 2);
    const auto* key = inputColumn(region, 1, "k0");
    ASSERT_NE(key, nullptr);
    const int32_t common = planJoinEqClassForColumn(classes, &key->id);
    ASSERT_GE(common, 0);
    for (int32_t i = 0; i < columnCount; ++i) {
      const std::string name = "k" + std::to_string(i);
      SCOPED_TRACE(name);
      const auto* column = inputColumn(region, 0, name.c_str());
      ASSERT_NE(column, nullptr);
      EXPECT_EQ(planJoinEqClassForColumn(classes, &column->id), common);
    }
    EXPECT_TRUE(planJoinEqClassesConnect(classes, 1, 2));
    EXPECT_TRUE(planJoinEqClassesConnect(classes, 1, 4));
    EXPECT_TRUE(planJoinEqClassesConnect(classes, 2, 4));
  });
}

TEST_F(PlanJoinParallelTest, eqClassesIgnoreInexactOrNonColumnPredicates) {
  useDb("root", "test");
  g_mockCatalogService->createTableBuilder(
      "testus", "eq_micro", TSDB_NORMAL_TABLE, 2)
      .setPrecision(TSDB_TIME_PRECISION_MICRO).setVgid(1)
      .addColumn("ts", TSDB_DATA_TYPE_TIMESTAMP)
      .addColumn("c1", TSDB_DATA_TYPE_INT).done();
  const std::string join = "SELECT a.c1 FROM t1 a JOIN t1 b ON ";
  const std::vector<std::string> sqls = {
      join + "a.c4=b.c4", join + "a.c1=b.c3", join + "a.c1=b.c1 OR a.c3=b.c3",
      join + "NOT(a.c1=b.c1)", join + "a.c1+1=b.c1", join + "a.c1=1",
      "SELECT a.c1 FROM t1 a JOIN testus.eq_micro b ON a.ts=b.ts"};
  for (const auto& sql : sqls) {
    SCOPED_TRACE(sql);
    runEqClasses(sql, [](const SJoinParallelRegion*,
                         const SJoinEqClasses* classes) {
      EXPECT_EQ(taosArrayGetSize(classes->pMembers), 0U);
      EXPECT_FALSE(planJoinEqClassesConnect(classes, 1, 2));
    });
  }
}

TEST_F(PlanJoinParallelTest, eqClassesRespectMixedAndOversizedRegionBounds) {
  useDb("root", "test");
  setJoinPropsCheck([](const SPlanContext* context, const SLogicNode* root,
                       bool optimized) {
    if (optimized) return;
    auto model = prepareJoinModel(context, root);
    ASSERT_NE(model, nullptr);
    ASSERT_EQ(taosArrayGetSize(model->pRegions), 1U);
    auto classes = buildEqClasses(model.get(), regionAt(model.get(), 0));
    EXPECT_EQ(classes, nullptr);
  });
  run("SELECT a.c1 FROM t1 a LEFT JOIN t1 b ON a.c1=b.c1 "
      "JOIN t1 c ON b.c1=c.c1");
  std::string sql = "SELECT a.c1 FROM t1 a";
  for (char alias = 'b'; alias <= 'i'; ++alias) {
    sql += " JOIN t1 " + std::string(1, alias) + " ON a.c1=" + alias + ".c1";
  }
  run(sql);
}


TEST_F(PlanJoinParallelTest, eqClassesFailurePreservesTreeAndAllowsRetry) {
  useDb("root", "test");
  setJoinPropsCheck([](const SPlanContext* context, const SLogicNode* root,
                       bool optimized) {
    if (optimized) return;
    const std::string before = nodeText((const SNode*)root);
    auto model = prepareJoinModel(context, root);
    ASSERT_NE(model, nullptr);
    const auto* region = regionAt(model.get(), 0);
    SJoinEqClasses* classes = nullptr;
    int32_t code;
    {
      Stub stub;
      stub.set(taosArrayInit, failArrayInit);
      code = planBuildJoinEqClasses(model.get(), region, &classes);
    }
    EXPECT_EQ(code, TSDB_CODE_OUT_OF_MEMORY);
    EXPECT_EQ(classes, nullptr);
    EXPECT_EQ(nodeText((const SNode*)root), before);
    {
      Stub stub;
      stub.set(tSimpleHashPut, failEqHashPut);
      code = planBuildJoinEqClasses(model.get(), region, &classes);
    }
    EXPECT_EQ(code, TSDB_CODE_OUT_OF_MEMORY);
    EXPECT_EQ(classes, nullptr);
    EXPECT_EQ(nodeText((const SNode*)root), before);
    auto retry = buildEqClasses(model.get(), region);
    ASSERT_NE(retry, nullptr);
    EXPECT_EQ(taosArrayGetSize(retry->pMembers), 2U);
  });
  run("SELECT a.c1 FROM t1 a JOIN t1 b ON a.c1=b.c1");
}


TEST_F(PlanJoinParallelTest, eqClassesSupportExactColumnTypes) {
  useDb("root", "test");
  const std::vector<int8_t> types = {
      TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_INT,
      TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_UTINYINT, TSDB_DATA_TYPE_USMALLINT,
      TSDB_DATA_TYPE_UINT, TSDB_DATA_TYPE_UBIGINT, TSDB_DATA_TYPE_VARCHAR,
      TSDB_DATA_TYPE_NCHAR, TSDB_DATA_TYPE_VARBINARY};
  auto& builder = g_mockCatalogService->createTableBuilder(
      "test", "eq_types", TSDB_NORMAL_TABLE, (int32_t)types.size() + 1);
  builder.setPrecision(TSDB_TIME_PRECISION_MILLI).setVgid(1)
      .addColumn("ts", TSDB_DATA_TYPE_TIMESTAMP);
  std::vector<std::string> columns = {"ts"};
  for (size_t i = 0; i < types.size(); ++i) {
    columns.push_back("k" + std::to_string(i));
    builder.addColumn(columns.back(), types[i],
                      i >= 8 ? 82 : tDataTypes[types[i]].bytes);
  }
  builder.done();
  for (const auto& column : columns) {
    SCOPED_TRACE(column);
    runEqClasses("SELECT a.ts,a.k0 FROM eq_types a JOIN eq_types b ON a." +
                     column + "=b." + column + " JOIN eq_types c ON a." +
                     column + "=c." + column,
                 [&column](const SJoinParallelRegion* region,
                           const SJoinEqClasses* classes) {
      EXPECT_EQ(taosArrayGetSize(classes->pMembers), 3U);
      EXPECT_TRUE(planJoinEqClassesConnect(classes, 2, 4));
      const auto* absent = inputColumn(region, 0, column == "ts" ? "k0" : "ts");
      ASSERT_NE(absent, nullptr);
      EXPECT_EQ(planJoinEqClassForColumn(classes, &absent->id), -1);
    });
  }
}

TEST_F(PlanJoinParallelTest, eqClassesKeepAndEqualityOutsideOr) {
  useDb("root", "test");
  runEqClasses("SELECT a.c1 FROM t1 a JOIN t1 b ON a.c1=b.c1 "
               "JOIN t1 c ON a.c1=c.c1 OR a.c3=c.c3",
               [](const SJoinParallelRegion*, const SJoinEqClasses* classes) {
    EXPECT_EQ(taosArrayGetSize(classes->pMembers), 2U);
    EXPECT_TRUE(planJoinEqClassesConnect(classes, 1, 2));
    EXPECT_FALSE(planJoinEqClassesConnect(classes, 1, 4));
  });
}

TEST_F(PlanJoinParallelTest, pureRegionSplitsAndPreservesOr) {
  useDb("root", "test");
  int checks = 0;
  setJoinPropsCheck([&checks](const SPlanContext* context,
                              const SLogicNode* root, bool optimized) {
    if (optimized) return;
    ++checks;
    const std::string before = nodeText((const SNode*)root);
    auto              model = prepareJoinModel(context, root);
    ASSERT_NE(model, nullptr);
    ASSERT_EQ(taosArrayGetSize(model->pRegions), 1U);
    const auto* region = regionAt(model.get(), 0);
    EXPECT_EQ(region->mode, JOIN_REGION_PURE_INNER);
    EXPECT_TRUE(region->supported);
    ASSERT_EQ(taosArrayGetSize(region->pInputs), 3U);
    ASSERT_EQ(taosArrayGetSize(region->pOps), 2U);
    ASSERT_EQ(taosArrayGetSize(region->pPredicates), 3U);
    const auto* join = firstJoin(root);
    ASSERT_NE(join, nullptr);
    int preservedOr = 0;
    int separateAnd = 0;
    for (size_t i = 0; i < taosArrayGetSize(region->pPredicates); ++i) {
      const auto* pred =
          (const SJoinParallelPredicate*)taosArrayGet(region->pPredicates, i);
      EXPECT_FALSE(pred->unresolved);
      EXPECT_EQ(pred->source, JOIN_PREDICATE_FULL_ON);
      if (pred->pExpr == join->pFullOnCond) {
        ++preservedOr;
        ASSERT_EQ(nodeType(pred->pExpr), QUERY_NODE_LOGIC_CONDITION);
        EXPECT_EQ(((const SLogicConditionNode*)pred->pExpr)->condType,
                  LOGIC_COND_TYPE_OR);
        EXPECT_EQ(dependencies(region, pred), (std::set<int32_t>{1, 2}));
      } else {
        ++separateAnd;
        EXPECT_EQ(dependencies(region, pred), (std::set<int32_t>{0, 1}));
      }
    }
    EXPECT_EQ(preservedOr, 1);
    EXPECT_EQ(separateAnd, 2);
    EXPECT_EQ(nodeText((const SNode*)root), before);
  });
  run("SELECT a.c1, b.c1, c.c1 FROM t1 a JOIN t1 b "
      "ON a.c1=b.c1 AND a.c3=b.c3 JOIN t1 c "
      "ON b.c1=c.c1 OR b.c3=c.c3");
  EXPECT_EQ(checks, 1);
}

TEST_F(PlanJoinParallelTest, mixedRegionRetainsEachCompleteOn) {
  useDb("root", "test");
  setJoinPropsCheck([](const SPlanContext* context, const SLogicNode* root,
                       bool optimized) {
    if (optimized) return;
    auto model = prepareJoinModel(context, root);
    ASSERT_NE(model, nullptr);
    ASSERT_EQ(taosArrayGetSize(model->pRegions), 1U);
    const auto* region = regionAt(model.get(), 0);
    EXPECT_EQ(region->mode, JOIN_REGION_MIXED);
    ASSERT_EQ(taosArrayGetSize(region->pInputs), 3U);
    ASSERT_EQ(taosArrayGetSize(region->pPredicates), 2U);
    std::set<const SNode*> originals;
    for (size_t i = 0; i < taosArrayGetSize(region->pOps); ++i) {
      const auto* join = (const SJoinLogicNode*)taosArrayGetP(region->pOps, i);
      originals.insert(join->pFullOnCond);
    }
    for (size_t i = 0; i < taosArrayGetSize(region->pPredicates); ++i) {
      const auto* pred =
          (const SJoinParallelPredicate*)taosArrayGet(region->pPredicates, i);
      EXPECT_EQ(originals.erase(pred->pExpr), 1U);
      EXPECT_FALSE(pred->unresolved);
      EXPECT_EQ(pred->source, JOIN_PREDICATE_FULL_ON);
      ASSERT_EQ(nodeType(pred->pExpr), QUERY_NODE_LOGIC_CONDITION);
      EXPECT_EQ(((const SLogicConditionNode*)pred->pExpr)->condType,
                LOGIC_COND_TYPE_AND);
    }
    EXPECT_TRUE(originals.empty());
  });
  run("SELECT a.c1, b.c1, c.c1 FROM t1 a JOIN t1 b "
      "ON a.c1=b.c1 AND a.c3=b.c3 LEFT JOIN t1 c "
      "ON b.c1=c.c1 AND c.c3>0");
}

TEST_F(PlanJoinParallelTest, selfJoinInputsHaveDistinctColumnIdentities) {
  useDb("root", "test");
  setJoinPropsCheck([](const SPlanContext* context, const SLogicNode* root,
                       bool optimized) {
    if (optimized) return;
    auto model = prepareJoinModel(context, root);
    ASSERT_NE(model, nullptr);
    ASSERT_EQ(taosArrayGetSize(model->pRegions), 1U);
    const auto* region = regionAt(model.get(), 0);
    ASSERT_EQ(taosArrayGetSize(region->pInputs), 3U);
    std::set<uint64_t> relations;
    for (size_t i = 0; i < 3; ++i) {
      const auto* input =
          (const SJoinParallelInput*)taosArrayGet(region->pInputs, i);
      ASSERT_NE(input->pProps, nullptr);
      EXPECT_EQ(input->inputId, (int32_t)i);
      const auto* columns = input->pProps->pOutputCols;
      bool        found = false;
      for (size_t j = 0; j < taosArrayGetSize(columns); ++j) {
        const auto* column = (const SJoinOutputCol*)taosArrayGet(columns, j);
        if (nodeType(column->pExpr) != QUERY_NODE_COLUMN) continue;
        const auto* ref = (const SColumnNode*)column->pExpr;
        if (std::string(ref->colName) != "c1") continue;
        EXPECT_EQ(std::string(ref->tableAlias), std::string(1, 'a' + i));
        EXPECT_EQ(planJoinInputForColumn(region, &column->id), (int32_t)i);
        relations.insert(column->id.relationId);
        found = true;
      }
      EXPECT_TRUE(found);
    }
    EXPECT_EQ(relations.size(), 3U);
  });
  run("SELECT a.c1, b.c1, c.c1 FROM t1 a JOIN t1 b ON a.c1=b.c1 "
      "JOIN t1 c ON b.c1=c.c1");
}

TEST_F(PlanJoinParallelTest, temporalBoundaryHasInnerAndOuterRegions) {
  useDb("root", "test");
  setJoinPropsCheck(
      [](const SPlanContext* context, const SLogicNode* root, bool optimized) {
        if (optimized) return;
        auto model = prepareJoinModel(context, root);
        ASSERT_NE(model, nullptr);
        ASSERT_EQ(taosArrayGetSize(model->pRegions), 2U);
        const auto* time = temporalJoin(root);
        ASSERT_NE(time, nullptr);
        const auto* outer = regionFor(model.get(), &firstJoin(root)->node);
        ASSERT_NE(outer, nullptr);
        ASSERT_EQ(taosArrayGetSize(outer->pInputs), 3U);
        const auto* input =
            (const SJoinParallelInput*)taosArrayGet(outer->pInputs, 0);
        EXPECT_EQ(input->pNode, &time->node);
        EXPECT_EQ(regionAt(model.get(), 1), outer);
        EXPECT_EQ(taosArrayGetSize(regionAt(model.get(), 0)->pInputs), 2U);
        EXPECT_EQ(regionFor(model.get(), &time->node), nullptr);
      });
  run("SELECT a.c1, b.c1, c.c1, d.c1, e.c1 FROM "
      "((t1 a JOIN t1 b ON a.ts=b.ts) LEFT ASOF JOIN t1 c) "
      "JOIN t1 d ON a.c1=d.c1 JOIN t1 e ON d.c1=e.c1");
}

TEST_F(PlanJoinParallelTest, limitBoundaryRetainsRecursiveRegion) {
  useDb("root", "test");
  setJoinPropsCheck(
      [](const SPlanContext* context, const SLogicNode* root, bool optimized) {
        if (optimized) return;
        auto model = prepareJoinModel(context, root);
        ASSERT_NE(model, nullptr);
        ASSERT_EQ(taosArrayGetSize(model->pRegions), 2U);
        const auto* outer = regionFor(model.get(), &firstJoin(root)->node);
        ASSERT_NE(outer, nullptr);
        EXPECT_EQ(taosArrayGetSize(outer->pInputs), 2U);
        const auto* inner = regionAt(model.get(), 0);
        EXPECT_EQ(taosArrayGetSize(inner->pInputs), 4U);
        const auto* input =
            (const SJoinParallelInput*)taosArrayGet(outer->pInputs, 0);
        EXPECT_EQ(nodeType(input->pNode), QUERY_NODE_LOGIC_PLAN_PROJECT);
        EXPECT_NE(input->pNode->pLimit, nullptr);
      });
  run("SELECT q.c1, e.c1 FROM (SELECT a.ts, a.c1 FROM t1 a "
      "JOIN t1 b ON a.ts=b.ts JOIN t1 c ON b.ts=c.ts "
      "JOIN t1 d ON c.ts=d.ts LIMIT 3) q JOIN t1 e ON q.c1=e.c1");
}

TEST_F(PlanJoinParallelTest, semiAntiBoundaryUsesUpperColumnReferences) {
  useDb("root", "test");
  struct Case {
    const char* select;
    const char* tail;
    bool        matchingUsed;
  };
  const Case cases[] = {
      {"a.c1", "", false},
      {"b.c1", "", true},
      {"a.c1", " WHERE a.c2 + b.c2 > 0", true},
      {"a.c1", " ORDER BY b.c1", true},
  };
  for (const std::string kind : {"SEMI", "ANTI"}) {
    for (const auto& item : cases) {
      SCOPED_TRACE(kind + " " + item.select + item.tail);
      const bool used = item.matchingUsed;
      setJoinPropsCheck([used](const SPlanContext* context,
                               const SLogicNode* root, bool optimized) {
        if (optimized) return;
        auto model = prepareJoinModel(context, root);
        ASSERT_NE(model, nullptr);
        const auto* outer = regionFor(model.get(), &firstJoin(root)->node);
        ASSERT_NE(outer, nullptr);
        EXPECT_EQ(taosArrayGetSize(outer->pInputs), used ? 2U : 3U);
        EXPECT_EQ(outer->mode,
                  used ? JOIN_REGION_PURE_INNER : JOIN_REGION_MIXED);
      });
      run(std::string("SELECT ") + item.select + " FROM (t1 a LEFT " + kind +
          " JOIN t1 b ON a.c1=b.c1) JOIN t1 c ON a.c1=c.c1" + item.tail);
    }
  }
}

TEST_F(PlanJoinParallelTest, unresolvedUpperColumnKeepsSemiBoundary) {
  useDb("root", "test");
  setJoinPropsCheck([](const SPlanContext* context, const SLogicNode* root,
                       bool optimized) {
    if (optimized) return;
    TreeCopy copy(context, root);
    ASSERT_EQ(copy.code, TSDB_CODE_SUCCESS);
    auto* outer = (SJoinLogicNode*)firstJoin(copy.root());
    ASSERT_NE(outer, nullptr);
    SLogicNode* parent = outer->node.pParent;
    ASSERT_NE(parent, nullptr);
    ASSERT_EQ(parent->pConditions, nullptr);
    SColumnNode* column = nullptr;
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&column),
              TSDB_CODE_SUCCESS);
    tstrncpy(column->tableAlias, "unknown", sizeof(column->tableAlias));
    tstrncpy(column->colName, "c1", sizeof(column->colName));
    parent->pConditions = (SNode*)column;
    ASSERT_EQ(planRebuildJoinInputProps(&copy.context, copy.root()),
              TSDB_CODE_SUCCESS);
    auto model = prepareJoinModel(&copy.context, copy.root());
    ASSERT_NE(model, nullptr);
    const auto* region = regionFor(model.get(), &outer->node);
    ASSERT_NE(region, nullptr);
    EXPECT_EQ(taosArrayGetSize(region->pInputs), 2U);
    EXPECT_EQ(region->mode, JOIN_REGION_PURE_INNER);
  });
  run("SELECT a.c1 FROM (t1 a LEFT SEMI JOIN t1 b ON a.c1=b.c1) "
      "JOIN t1 c ON a.c1=c.c1");
}

TEST_F(PlanJoinParallelTest, oversizedRegionIsNotTruncated) {
  useDb("root", "test");
  setJoinPropsCheck(
      [](const SPlanContext* context, const SLogicNode* root, bool optimized) {
        if (optimized) return;
        auto model = prepareJoinModel(context, root);
        ASSERT_NE(model, nullptr);
        ASSERT_EQ(taosArrayGetSize(model->pRegions), 1U);
        const auto* region = regionAt(model.get(), 0);
        EXPECT_EQ(taosArrayGetSize(region->pInputs), 9U);
        EXPECT_EQ(taosArrayGetSize(region->pOps), 8U);
        EXPECT_FALSE(region->supported);
      });
  std::string sql = "SELECT a.c1 FROM t1 a";
  for (char name = 'b'; name <= 'i'; ++name) {
    sql += " JOIN t1 " + std::string(1, name) + " ON a.c1=" + name + ".c1";
  }
  run(sql);
}

TEST_F(PlanJoinParallelTest, streamTopicAndRsmaSkipAnalysis) {
  useDb("root", "test");
  setJoinPropsCheck(
      [](const SPlanContext* context, const SLogicNode* root, bool optimized) {
        if (optimized) return;
        for (int mode = 0; mode < 5; ++mode) {
          SPlanContext restricted = *context;
          restricted.topicQuery = mode == 0;
          restricted.rSmaQuery = mode == 1;
          restricted.streamCxt.isCalc = mode == 2;
          restricted.streamCxt.isTrigger = mode == 3;
          restricted.streamCxt.isVtableCalc = mode == 4;
          auto model = prepareJoinModel(&restricted, root);
          ASSERT_NE(model, nullptr);
          EXPECT_EQ(taosArrayGetSize(model->pRegions), 0U);
        }
      });
  run("SELECT a.c1 FROM t1 a JOIN t1 b ON a.c1=b.c1");
}

TEST_F(PlanJoinParallelTest, nodeRestrictionsAreBoundaries) {
  useDb("root", "test");
  setJoinPropsCheck([](const SPlanContext* context, const SLogicNode* root,
                       bool optimized) {
    if (optimized) return;
    for (int restriction = 0; restriction < 5; ++restriction) {
      TreeCopy copy(context, root);
      ASSERT_EQ(copy.code, TSDB_CODE_SUCCESS);
      auto* join = (SJoinLogicNode*)firstJoin(copy.root());
      ASSERT_NE(join, nullptr);
      if (restriction == 0) join->node.dynamicOp = true;
      if (restriction == 1) {
        ASSERT_EQ(nodesMakeNode(QUERY_NODE_LIMIT, &join->node.pLimit),
                  TSDB_CODE_SUCCESS);
      }
      if (restriction == 2) {
        ASSERT_EQ(nodesMakeNode(QUERY_NODE_LIMIT, &join->node.pSlimit),
                  TSDB_CODE_SUCCESS);
      }
      if (restriction == 3) {
        SHintNode* hint = nullptr;
        ASSERT_EQ(nodesMakeNode(QUERY_NODE_HINT, (SNode**)&hint),
                  TSDB_CODE_SUCCESS);
        hint->option = HINT_BATCH_SCAN;
        ASSERT_EQ(nodesListMakeStrictAppend(&join->node.pHint, (SNode*)hint),
                  TSDB_CODE_SUCCESS);
      }
      if (restriction == 4) join->grpJoin = true;
      ASSERT_EQ(planRebuildJoinInputProps(&copy.context, copy.root()),
                TSDB_CODE_SUCCESS);
      auto model = prepareJoinModel(&copy.context, copy.root());
      ASSERT_NE(model, nullptr);
      EXPECT_EQ(regionFor(model.get(), &join->node), nullptr);
    }
  });
  run("SELECT a.c1 FROM t1 a JOIN t1 b ON a.c1=b.c1 "
      "JOIN t1 c ON b.c1=c.c1");
}

TEST_F(PlanJoinParallelTest, dependentInputMakesRegionUnsupported) {
  useDb("root", "test");
  setJoinPropsCheck(
      [](const SPlanContext* context, const SLogicNode* root, bool optimized) {
        if (optimized) return;
        TreeCopy copy(context, root);
        ASSERT_EQ(copy.code, TSDB_CODE_SUCCESS);
        auto* join = (SJoinLogicNode*)firstJoin(copy.root());
        ASSERT_NE(join, nullptr);
        auto* left = (SLogicNode*)nodesListGetNode(join->node.pChildren, 0);
        ASSERT_EQ(nodeType(left), QUERY_NODE_LOGIC_PLAN_JOIN);
        left->dynamicOp = true;
        ASSERT_EQ(planRebuildJoinInputProps(&copy.context, copy.root()),
                  TSDB_CODE_SUCCESS);
        auto model = prepareJoinModel(&copy.context, copy.root());
        ASSERT_NE(model, nullptr);
        ASSERT_EQ(taosArrayGetSize(model->pRegions), 1U);
        const auto* region = regionFor(model.get(), &join->node);
        ASSERT_NE(region, nullptr);
        ASSERT_EQ(taosArrayGetSize(region->pInputs), 2U);
        EXPECT_FALSE(region->supported);
        const auto* input =
            (const SJoinParallelInput*)taosArrayGet(region->pInputs, 0);
        EXPECT_EQ(input->pNode, left);
        EXPECT_EQ(regionFor(model.get(), left), nullptr);
      });
  run("SELECT a.c1 FROM t1 a JOIN t1 b ON a.c1=b.c1 "
      "JOIN t1 c ON b.c1=c.c1");
}

TEST_F(PlanJoinParallelTest, whereSourceAndPushedInputFilterStayDistinct) {
  useDb("root", "test");
  setJoinPropsCheck([](const SPlanContext* context, const SLogicNode* root,
                       bool optimized) {
    if (optimized) return;
    TreeCopy copy(context, root);
    ASSERT_EQ(copy.code, TSDB_CODE_SUCCESS);
    auto* join = (SJoinLogicNode*)firstJoin(copy.root());
    ASSERT_NE(join, nullptr);
    ASSERT_NE(join->node.pConditions, nullptr);
    ASSERT_EQ(planRebuildJoinInputProps(&copy.context, copy.root()),
              TSDB_CODE_SUCCESS);
    auto model = prepareJoinModel(&copy.context, copy.root());
    ASSERT_NE(model, nullptr);
    const auto* region = regionFor(model.get(), &join->node);
    ASSERT_NE(region, nullptr);
    ASSERT_EQ(taosArrayGetSize(region->pPredicates), 2U);
    bool sawFilter = false;
    for (size_t i = 0; i < taosArrayGetSize(region->pPredicates); ++i) {
      const auto* pred =
          (const SJoinParallelPredicate*)taosArrayGet(region->pPredicates, i);
      if (pred->source != JOIN_PREDICATE_FILTER) continue;
      sawFilter = true;
      EXPECT_EQ(pred->pExpr, join->node.pConditions);
      EXPECT_EQ(dependencies(region, pred), (std::set<int32_t>{0}));
    }
    EXPECT_TRUE(sawFilter);
    model.reset();
    auto* left = (SLogicNode*)nodesListGetNode(join->node.pChildren, 0);
    ASSERT_EQ(left->pConditions, nullptr);
    left->pConditions = join->node.pConditions;
    join->node.pConditions = nullptr;
    const SNode* filter = left->pConditions;
    ASSERT_EQ(planRebuildJoinInputProps(&copy.context, copy.root()),
              TSDB_CODE_SUCCESS);
    model = prepareJoinModel(&copy.context, copy.root());
    ASSERT_NE(model, nullptr);
    region = regionFor(model.get(), &join->node);
    ASSERT_NE(region, nullptr);
    EXPECT_EQ(taosArrayGetSize(region->pPredicates), 1U);
    model.reset();
    EXPECT_EQ(left->pConditions, filter);
  });
  run("SELECT a.c1 FROM t1 a JOIN t1 b ON a.c1=b.c1 WHERE a.c1>0");
}

TEST_F(PlanJoinParallelTest, prepareFailurePreservesTreeAndOutput) {
  useDb("root", "test");
  setJoinPropsCheck(
      [](const SPlanContext* context, const SLogicNode* root, bool optimized) {
        if (optimized) return;
        const std::string    before = nodeText((const SNode*)root);
        SJoinRewriteContext* model = nullptr;
        int32_t              code;
        {
          Stub stub;
          stub.set(taosArrayInit, failArrayInit);
          code = planPrepareJoinRewrite(context, root, &model);
        }
        EXPECT_EQ(code, TSDB_CODE_OUT_OF_MEMORY);
        EXPECT_EQ(model, nullptr);
        planDestroyJoinRewriteContext(model);
        EXPECT_EQ(nodeText((const SNode*)root), before);
        auto retry = prepareJoinModel(context, root);
        ASSERT_NE(retry, nullptr);
        EXPECT_EQ(taosArrayGetSize(retry->pRegions), 1U);
      });
  run("SELECT a.c1 FROM t1 a JOIN t1 b ON a.c1=b.c1");
}

TEST_F(PlanJoinParallelTest, rewriteOrdinaryTreeThenStops) {
  useDb("root", "test");
  setJoinPropsCheck(
      [](const SPlanContext* context, const SLogicNode* root, bool optimized) {
        if (optimized) return;
        TreeCopy copy(context, root);
        ASSERT_EQ(copy.code, TSDB_CODE_SUCCESS);
        SLogicSubplan     subplan = {};
        subplan.pNode = copy.root();
        std::string stable;
        for (int i = 0; i < 2; ++i) {
          bool changed = true;
          const int32_t code = planRewriteJoinParallel(
              &copy.context, &subplan, &changed);
          copy.tree.release();
          copy.tree.reset((SNode*)subplan.pNode);
          ASSERT_EQ(code, TSDB_CODE_SUCCESS);
          EXPECT_EQ(changed, i == 0);
          EXPECT_EQ(builtJoinHeight(copy.root()), 2);
          if (i == 0) stable = nodeText(copy.tree.get());
          else EXPECT_EQ(nodeText(copy.tree.get()), stable);
        }
      });
  run("SELECT a.c1 FROM t1 a JOIN t1 b ON a.c1=b.c1 "
      "JOIN t1 c ON b.c1=c.c1 JOIN t1 d ON c.c1=d.c1");
}

TEST_F(PlanJoinParallelTest, temporalBindingIsStableAndCloned) {
  useDb("root", "test");
  for (const std::string kind : {"LEFT ASOF", "RIGHT ASOF", "LEFT WINDOW"}) {
    SCOPED_TRACE(kind);
    setJoinPropsCheck([kind](const SPlanContext* context,
                             const SLogicNode* root, bool optimized) {
      if (optimized) return;
      TreeCopy copy(context, root);
      ASSERT_EQ(copy.code, TSDB_CODE_SUCCESS);
      auto* join = (SJoinLogicNode*)temporalJoin(copy.root());
      ASSERT_NE(join, nullptr);
      ASSERT_EQ(join->pPrimKeyEqCond, nullptr);
      ASSERT_EQ(join->addPrimEqCond, nullptr);
      SLogicSubplan subplan = {};
      subplan.pNode = copy.root();
      bool changed = false;
      ASSERT_EQ(planRewriteJoinParallel(&copy.context, &subplan, &changed),
                TSDB_CODE_SUCCESS);
      EXPECT_TRUE(changed);
      const auto* condition = (const SOperatorNode*)join->addPrimEqCond;
      ASSERT_NE(condition, nullptr);
      const EOperatorType expected = kind == "LEFT WINDOW" ? OP_TYPE_EQUAL
                                     : kind == "LEFT ASOF"
                                         ? OP_TYPE_GREATER_EQUAL
                                         : OP_TYPE_LOWER_EQUAL;
      EXPECT_EQ(condition->opType, expected);
      EXPECT_STREQ(((const SColumnNode*)condition->pLeft)->tableAlias, "a");
      EXPECT_STREQ(((const SColumnNode*)condition->pRight)->tableAlias, "c");
      changed = true;
      ASSERT_EQ(planRewriteJoinParallel(&copy.context, &subplan, &changed),
                TSDB_CODE_SUCCESS);
      EXPECT_FALSE(changed);
      EXPECT_EQ(join->addPrimEqCond, (const SNode*)condition);
      SNode* cloned = nullptr;
      ASSERT_EQ(nodesCloneNode((SNode*)join, &cloned), TSDB_CODE_SUCCESS);
      NodePtr     clone(cloned, nodesDestroyNode);
      const auto* clonedJoin = (const SJoinLogicNode*)clone.get();
      ASSERT_NE(clonedJoin->addPrimEqCond, nullptr);
      EXPECT_TRUE(
          nodesEqualNode(clonedJoin->addPrimEqCond, join->addPrimEqCond));
    });
    run("SELECT a.c1 FROM (stream_t2 a JOIN stream_t2 b ON a.ts=b.ts) " + kind +
        " JOIN stream_t2 c" +
        (kind == "LEFT WINDOW" ? " WINDOW_OFFSET(-1s, 1s)" : ""));
  }
}

TEST_F(PlanJoinParallelTest, temporalOptionsSurviveTwoInputRewrites) {
  useDb("root", "test");
  const std::string left =
      "(stream_t2 a JOIN stream_t2 b ON a.ts=b.ts "
      "JOIN stream_t2 c ON a.ts=c.ts JOIN stream_t2 d ON a.ts=d.ts) ";
  const std::string right =
      "(stream_t2 e JOIN stream_t2 f ON e.ts=f.ts "
      "JOIN stream_t2 g ON e.ts=g.ts JOIN stream_t2 h ON e.ts=h.ts) ";
  for (const std::string clause : {
           "WINDOW_OFFSET(0s,0s)",
           "WINDOW_OFFSET(0s,0s) JLIMIT 1",
           "WINDOW_TIME(a.ts,e.ts) WINDOW_OFFSET(-1s,1s)",
           "WINDOW_TIME(a.ts,e.ts) WINDOW_OFFSET(-1s,1s) JLIMIT 1",
           "JLIMIT 1", "ON a.ts>=e.ts JLIMIT 1"}) {
    SCOPED_TRACE(clause);
    const bool window = clause.find("WINDOW_") != std::string::npos;
    const bool limited = clause.find("JLIMIT") != std::string::npos;
    int checks = 0;
    setJoinPropsCheck([&checks, window, limited](const SPlanContext*,
                                                const SLogicNode* root,
                                                bool optimized) {
      ++checks;
      const auto* join = temporalJoin(root);
      ASSERT_NE(join, nullptr);
      EXPECT_EQ(IS_WINDOW_JOIN(join->subType), window);
      EXPECT_EQ(join->pWindowOffset != nullptr, window);
      EXPECT_EQ(join->pJLimit != nullptr, limited);
      if (!optimized) return;
      const auto* condition =
          (const SOperatorNode*)planGetTemporalJoinTimeCond(join);
      ASSERT_NE(condition, nullptr);
      ASSERT_EQ(nodeType(condition->pLeft), QUERY_NODE_COLUMN);
      ASSERT_EQ(nodeType(condition->pRight), QUERY_NODE_COLUMN);
      EXPECT_STREQ(((const SColumnNode*)condition->pLeft)->tableAlias, "a");
      EXPECT_STREQ(((const SColumnNode*)condition->pRight)->tableAlias, "e");
      EXPECT_EQ(builtJoinHeight(root), 3);
    });
    run("SELECT a.c1,b.c1,c.c1,d.c1,e.c1,f.c1,g.c1,h.c1 FROM " + left +
        (window ? "LEFT WINDOW JOIN " : "LEFT ASOF JOIN ") + right + clause);
    EXPECT_EQ(checks, 2);
  }
}

TEST_F(PlanJoinParallelTest, existingTemporalKeysAreNotReplaced) {
  useDb("root", "test");
  setJoinPropsCheck([](const SPlanContext* context, const SLogicNode* root,
                       bool optimized) {
    if (optimized) return;
    TreeCopy copy(context, root);
    ASSERT_EQ(copy.code, TSDB_CODE_SUCCESS);
    auto* join = (SJoinLogicNode*)temporalJoin(copy.root());
    ASSERT_NE(join, nullptr);
    const SNode* condition = join->pPrimKeyEqCond != nullptr
                                 ? join->pPrimKeyEqCond
                                 : join->addPrimEqCond;
    ASSERT_NE(condition, nullptr);
    ASSERT_EQ(nodeType(condition), QUERY_NODE_OPERATOR);
    const auto* time = (const SOperatorNode*)condition;
    ASSERT_EQ(nodeType(time->pLeft), QUERY_NODE_COLUMN);
    ASSERT_EQ(nodeType(time->pRight), QUERY_NODE_COLUMN);
    EXPECT_STREQ(((const SColumnNode*)time->pLeft)->tableAlias, "b");
    EXPECT_STREQ(((const SColumnNode*)time->pLeft)->colName, "ts");
    EXPECT_STREQ(((const SColumnNode*)time->pRight)->tableAlias, "c");
    const std::string before = nodeText(condition);
    ASSERT_EQ(planBindTemporalJoinTimeKeys(&copy.context, join),
              TSDB_CODE_SUCCESS);
    SLogicSubplan subplan = {};
    subplan.pNode = copy.root();
    bool changed = true;
    ASSERT_EQ(planRewriteJoinParallel(&copy.context, &subplan, &changed),
              TSDB_CODE_SUCCESS);
    EXPECT_FALSE(changed);
    const SNode* after = join->pPrimKeyEqCond != nullptr ? join->pPrimKeyEqCond
                                                         : join->addPrimEqCond;
    EXPECT_EQ(after, condition);
    EXPECT_EQ(nodeText(after), before);
  });
  run("SELECT a.c1 FROM (stream_t2 a JOIN stream_t2 b ON a.ts=b.ts) "
      "LEFT ASOF JOIN stream_t2 c ON b.ts>=c.ts");
  run("SELECT a.c1 FROM (stream_t2 a JOIN stream_t2 b ON a.ts=b.ts) "
      "LEFT WINDOW JOIN stream_t2 c "
      "WINDOW_TIME(b.ts,c.ts) WINDOW_OFFSET(-1s,1s)");
  // Parser-generated defaults are also bound before logical optimization.
  run("SELECT b.c1 FROM stream_t2 b LEFT ASOF JOIN stream_t2 c");
  run("SELECT b.c1 FROM stream_t2 b LEFT WINDOW JOIN stream_t2 c "
      "WINDOW_OFFSET(-1s,1s)");
}

TEST_F(PlanJoinParallelTest, earlyDefaultBindingKeepsNullableInputFilter) {
  useDb("root", "test");
  setJoinPropsCheck(
      [](const SPlanContext* context, const SLogicNode* root, bool optimized) {
        if (optimized) return;
        TreeCopy copy(context, root);
        ASSERT_EQ(copy.code, TSDB_CODE_SUCCESS);
        auto* join = (SJoinLogicNode*)temporalJoin(copy.root());
        ASSERT_NE(join, nullptr);
        ASSERT_EQ(planBindTemporalJoinTimeKeys(&copy.context, join),
                  TSDB_CODE_SUCCESS);
        EXPECT_EQ(copy.context.pJoinPropsStore, nullptr);
        const auto* condition = (const SOperatorNode*)join->addPrimEqCond;
        ASSERT_NE(condition, nullptr);
        EXPECT_STREQ(((const SColumnNode*)condition->pRight)->colName, "c3");
        const auto* right =
            (const SLogicNode*)nodesListGetNode(join->node.pChildren, 1);
        EXPECT_EQ(countNotNullFilters(right), 1);
        EXPECT_TRUE(hasNotNull(right->pConditions, condition->pRight));
        ASSERT_EQ(planBindTemporalJoinTimeKeys(&copy.context, join),
                  TSDB_CODE_SUCCESS);
        EXPECT_EQ(countNotNullFilters(right), 1);
        ASSERT_EQ(planFinalizeTemporalJoins(&copy.context, copy.root()),
                  TSDB_CODE_SUCCESS);
        // Finalize may wrap the filtered input in a corrective Sort.
        right = (const SLogicNode*)nodesListGetNode(join->node.pChildren, 1);
        EXPECT_EQ(countNotNullFilters(right), 1);

        TreeCopy deferred(context, root);
        ASSERT_EQ(deferred.code, TSDB_CODE_SUCCESS);
        auto* deferredJoin = (SJoinLogicNode*)temporalJoin(deferred.root());
        ASSERT_NE(deferredJoin, nullptr);
        ASSERT_EQ(deferredJoin->addPrimEqCond, nullptr);
        ASSERT_EQ(planFinalizeTemporalJoins(&deferred.context,
                                            deferred.root()),
                  TSDB_CODE_SUCCESS);
        const auto* deferredRight = (const SLogicNode*)nodesListGetNode(
            deferredJoin->node.pChildren, 1);
        EXPECT_EQ(countNotNullFilters(deferredRight), 1);
      });
  run("SELECT a.c1, q.c1 FROM "
      "(stream_t2 a JOIN stream_t2 b ON a.ts=b.ts) LEFT ASOF JOIN "
      "(SELECT c3,c1 FROM stream_t2) q");
}

TEST_F(PlanJoinParallelTest, explicitTimeFiltersNonJoinInputOnce) {
  useDb("root", "test");
  int checks = 0;
  setJoinPropsCheck([&checks](const SPlanContext*, const SLogicNode* root,
                              bool optimized) {
    SCOPED_TRACE(optimized ? "optimized" : "created");
    const auto* join = temporalJoin(root);
    ASSERT_NE(join, nullptr);
    const auto* right =
        (const SLogicNode*)nodesListGetNode(join->node.pChildren, 1);
    EXPECT_EQ(countNotNullFilters(right), 1);
    ++checks;
  });
  run("SELECT a.c1 FROM (stream_t2 a JOIN stream_t2 b ON a.ts=b.ts) "
      "LEFT WINDOW JOIN (SELECT c3 ets,c1 FROM stream_t2) q "
      "WINDOW_TIME(b.ts,q.ets) WINDOW_OFFSET(-1s,1s)");
  run("SELECT a.c1 FROM (stream_t2 a JOIN stream_t2 b ON a.ts=b.ts) "
      "LEFT ASOF JOIN (SELECT _wstart ets,count(*) cnt FROM stream_t2 "
      "INTERVAL(1s)) q ON b.ts>=q.ets");
  EXPECT_EQ(checks, 4);
}

TEST_F(PlanJoinParallelTest, boundTimeSurvivesInputProjectElimination) {
  useDb("root", "test");
  bool sawOptimized = false;
  setJoinPropsCheck([&sawOptimized](const SPlanContext*,
                                    const SLogicNode* root, bool optimized) {
    const auto* join = temporalJoin(root);
    ASSERT_NE(join, nullptr);
    const auto* right =
        (const SLogicNode*)nodesListGetNode(join->node.pChildren, 1);
    if (!optimized) {
      EXPECT_NE(findNode(right, QUERY_NODE_LOGIC_PLAN_PROJECT), nullptr);
      EXPECT_EQ(join->addPrimEqCond, nullptr);
      return;
    }
    sawOptimized = true;
    EXPECT_EQ(findNode(right, QUERY_NODE_LOGIC_PLAN_PROJECT), nullptr);
    const auto* condition = (const SOperatorNode*)join->addPrimEqCond;
    ASSERT_NE(condition, nullptr);
    ASSERT_EQ(nodeType(condition->pRight), QUERY_NODE_COLUMN);
    const auto* key = (const SColumnNode*)condition->pRight;
    EXPECT_STREQ(key->tableAlias, "q");
    EXPECT_STREQ(key->colName, "ts");
    EXPECT_EQ(countNotNullFilters(right), 0);
  });
  run("SELECT a.c1 FROM (stream_t2 a JOIN stream_t2 b ON a.ts=b.ts) "
      "LEFT ASOF JOIN (SELECT ts,c1 FROM stream_t2) q");
  EXPECT_TRUE(sawOptimized);
}

TEST_F(PlanJoinParallelTest, nestedTemporalBindingRestartsInPostorder) {
  useDb("root", "test");
  bool sawOptimized = false;
  setJoinPropsCheck([&sawOptimized](const SPlanContext* context,
                                    const SLogicNode* root, bool optimized) {
    const auto* outer = temporalJoin(root);
    ASSERT_NE(outer, nullptr);
    const auto* inner = temporalJoin(
        (const SLogicNode*)nodesListGetNode(outer->node.pChildren, 0));
    ASSERT_NE(inner, nullptr);
    if (optimized) {
      sawOptimized = true;
      EXPECT_NE(inner->addPrimEqCond, nullptr);
      EXPECT_NE(outer->addPrimEqCond, nullptr);
      return;
    }
    TreeCopy copy(context, root);
    ASSERT_EQ(copy.code, TSDB_CODE_SUCCESS);
    auto* copiedOuter = (SJoinLogicNode*)temporalJoin(copy.root());
    auto* copiedInner = (SJoinLogicNode*)temporalJoin(
        (const SLogicNode*)nodesListGetNode(copiedOuter->node.pChildren, 0));
    ASSERT_EQ(copiedInner->addPrimEqCond, nullptr);
    ASSERT_EQ(copiedOuter->addPrimEqCond, nullptr);
    SLogicSubplan subplan = {};
    subplan.pNode = copy.root();
    bool changed = false;
    ASSERT_EQ(planRewriteJoinParallel(&copy.context, &subplan, &changed),
              TSDB_CODE_SUCCESS);
    EXPECT_TRUE(changed);
    EXPECT_EQ(copiedOuter->addPrimEqCond, nullptr);
    const SNode* innerKey = copiedInner->addPrimEqCond;
    ASSERT_NE(innerKey, nullptr);
    changed = false;
    ASSERT_EQ(planRewriteJoinParallel(&copy.context, &subplan, &changed),
              TSDB_CODE_SUCCESS);
    EXPECT_TRUE(changed);
    EXPECT_EQ(copiedInner->addPrimEqCond, innerKey);
    const SNode* outerKey = copiedOuter->addPrimEqCond;
    ASSERT_NE(outerKey, nullptr);
    changed = true;
    ASSERT_EQ(planRewriteJoinParallel(&copy.context, &subplan, &changed),
              TSDB_CODE_SUCCESS);
    EXPECT_FALSE(changed);
    EXPECT_EQ(copiedInner->addPrimEqCond, innerKey);
    EXPECT_EQ(copiedOuter->addPrimEqCond, outerKey);
  });
  run("SELECT a.c1 FROM ((stream_t2 a JOIN stream_t2 b ON a.ts=b.ts) "
      "LEFT ASOF JOIN stream_t2 c) LEFT ASOF JOIN stream_t2 d");
  EXPECT_TRUE(sawOptimized);
}

TEST_F(PlanJoinParallelTest, boundTimeSurvivesInnerInputSwapAndFinalize) {
  useDb("root", "test");
  setJoinPropsCheck([](const SPlanContext* context, const SLogicNode* root,
                       bool optimized) {
    if (optimized) return;
    TreeCopy copy(context, root);
    ASSERT_EQ(copy.code, TSDB_CODE_SUCCESS);
    auto* join = (SJoinLogicNode*)temporalJoin(copy.root());
    ASSERT_NE(join, nullptr);
    ASSERT_EQ(planBindTemporalJoinTimeKeys(&copy.context, join),
              TSDB_CODE_SUCCESS);
    const auto* before = (const SOperatorNode*)join->addPrimEqCond;
    ASSERT_NE(before, nullptr);
    EXPECT_STREQ(((const SColumnNode*)before->pLeft)->tableAlias, "a");
    auto* inner = (SJoinLogicNode*)nodesListGetNode(join->node.pChildren, 0);
    ASSERT_EQ(nodeType(inner), QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_EQ(inner->joinType, JOIN_TYPE_INNER);
    std::swap(inner->node.pChildren->pHead->pNode,
              inner->node.pChildren->pTail->pNode);

    // A fresh default selection after the swap would choose b.ts.
    TreeCopy unbound(&copy.context, copy.root());
    ASSERT_EQ(unbound.code, TSDB_CODE_SUCCESS);
    auto* unboundJoin = (SJoinLogicNode*)temporalJoin(unbound.root());
    nodesDestroyNode(unboundJoin->addPrimEqCond);
    unboundJoin->addPrimEqCond = nullptr;
    ASSERT_EQ(planBindTemporalJoinTimeKeys(&unbound.context, unboundJoin),
              TSDB_CODE_SUCCESS);
    const auto* newKey = (const SOperatorNode*)unboundJoin->addPrimEqCond;
    ASSERT_NE(newKey, nullptr);
    EXPECT_STREQ(((const SColumnNode*)newKey->pLeft)->tableAlias, "b");

    TreeCopy finalized(&copy.context, copy.root());
    ASSERT_EQ(finalized.code, TSDB_CODE_SUCCESS);
    ASSERT_EQ(planFinalizeTemporalJoins(&finalized.context, finalized.root()),
              TSDB_CODE_SUCCESS);
    const auto* finalJoin = temporalJoin(finalized.root());
    ASSERT_NE(finalJoin, nullptr);
    ASSERT_NE(finalJoin->addPrimEqCond, nullptr);
    EXPECT_TRUE(nodesEqualNode(finalJoin->addPrimEqCond, (const SNode*)before));
    const auto* finalKey = (const SOperatorNode*)finalJoin->addPrimEqCond;
    EXPECT_STREQ(((const SColumnNode*)finalKey->pLeft)->tableAlias, "a");
  });
  run("SELECT a.c1, b.c1 FROM "
      "(stream_t2 a JOIN stream_t2 b ON a.c1=b.c1) "
      "LEFT ASOF JOIN stream_t2 c");
}

// HashJoin marks its JOINs processed with the StableJoin flag; that alone
// must not make them StableJoin boundaries.
TEST_F(PlanJoinParallelTest, optimizedHashJoinStillFormsRegion) {
  useDb("root", "test");
  int checks = 0;
  setJoinPropsCheck([&checks](const SPlanContext* context,
                              const SLogicNode* root, bool optimized) {
    if (!optimized) return;
    ++checks;
    const auto* join = firstJoin(root);
    ASSERT_NE(join, nullptr);
    ASSERT_EQ(join->joinAlgo, JOIN_ALGO_HASH);
    auto model = prepareJoinModel(context, root);
    ASSERT_NE(model, nullptr);
    ASSERT_EQ(taosArrayGetSize(model->pRegions), 1U);
    const auto* region = regionAt(model.get(), 0);
    ASSERT_EQ(region->pRoot, join);
    EXPECT_TRUE(region->supported);
    ASSERT_EQ(taosArrayGetSize(region->pOps), 2U);
    for (size_t i = 0; i < 2; ++i) {
      const auto* op = (const SJoinLogicNode*)taosArrayGetP(region->pOps, i);
      EXPECT_EQ(op->joinAlgo, JOIN_ALGO_HASH);
    }
    ASSERT_EQ(taosArrayGetSize(region->pInputs), 3U);
    for (size_t i = 0; i < 3; ++i) {
      const auto* input =
          (const SJoinParallelInput*)taosArrayGet(region->pInputs, i);
      EXPECT_EQ(nodeType(input->pNode), QUERY_NODE_LOGIC_PLAN_SCAN);
    }
  });
  run("SELECT a.c1, b.c1, c.c1 FROM t1 a JOIN t1 b ON a.c1=b.c1 "
      "JOIN t1 c ON b.c1=c.c1");
  EXPECT_EQ(checks, 1);
}

TEST_F(PlanJoinParallelTest, conflictsFourInputBagsCoverRuleNarrowing) {
  useDb("root", "test");
  runConflicts("SELECT a.c1,b.c1,c.c1,d.c1 FROM "
               "((t1 a JOIN t1 b ON a.c1=b.c1) LEFT JOIN t1 c ON b.c1=c.c1) "
               "JOIN t1 d ON c.c1=d.c1",
               [](const SJoinConflictModel* model) {
    ASSERT_EQ(taosArrayGetSize(model->pOps), 3U);
    ConflictEnumerator enumerator(model);
    const auto& choices = enumerator.enumerate(15, 7);
    bool usesNarrowing = false;
    for (const auto& choice : choices) {
      std::vector<ConflictChoicePtr> pending = {choice};
      while (!pending.empty()) {
        const auto node = pending.back();
        pending.pop_back();
        usesNarrowing |= node->root == 0 && node->inputs == 14;
        if (node->left) pending.push_back(node->left);
        if (node->right) pending.push_back(node->right);
      }
    }
    EXPECT_TRUE(usesNarrowing);
    checkConflictBags(model, 15);
  });
  runConflicts("SELECT a.c1,b.c1,c.c1,d.c1 FROM "
               "((t1 a LEFT JOIN t1 b ON a.c1=b.c1) JOIN t1 c ON a.c1=c.c1) "
               "JOIN t1 d ON c.c1=d.c1",
               [](const SJoinConflictModel* model) {
    EXPECT_FALSE(planJoinSplitIsLegal(model, 0, 6, 8, false));
    checkConflictBags(model, 15);
  });
}

TEST_F(PlanJoinParallelTest, conflictsFourInputBagsApplyRootFilterLast) {
  useDb("root", "test");
  runConflicts("SELECT a.c1,b.c1,c.c1,d.c1 FROM "
               "t1 a LEFT JOIN t1 b ON a.c1=b.c1 "
               "LEFT JOIN t1 c ON b.c1=c.c1 LEFT JOIN t1 d ON c.c1=d.c1 "
               "WHERE a.c1>0",
               [](const SJoinConflictModel* model) {
    ASSERT_NE(model->pRootFilter, nullptr);
    const auto* root = (const SJoinConflictOp*)taosArrayGet(model->pOps, 0);
    EXPECT_FALSE(root->pinnedFilter);
    EXPECT_TRUE(planJoinSplitIsLegal(model, 0, 4, 8, false));
    checkConflictFilterSample(model);
    checkConflictBags(model, 15);
  });
}

TEST_F(PlanJoinParallelTest, conflictsFourInputBagsApplyNonRootFilters) {
  useDb("root", "test");
  for (bool nullFilter : {false, true}) {
    SCOPED_TRACE(nullFilter);
    setJoinPropsCheck([nullFilter](const SPlanContext* context,
                                   const SLogicNode* root, bool optimized) {
      if (optimized) return;
      TreeCopy copy(context, root);
      ASSERT_EQ(copy.code, TSDB_CODE_SUCCESS);
      auto* top = (SJoinLogicNode*)firstJoin(copy.root());
      ASSERT_NE(top, nullptr);
      ASSERT_NE(top->node.pConditions, nullptr);
      auto* middle = (SJoinLogicNode*)nodesListGetNode(top->node.pChildren, 0);
      ASSERT_EQ(nodeType(middle), QUERY_NODE_LOGIC_PLAN_JOIN);
      auto* bottom =
          (SJoinLogicNode*)nodesListGetNode(middle->node.pChildren, 0);
      ASSERT_EQ(nodeType(bottom), QUERY_NODE_LOGIC_PLAN_JOIN);
      ASSERT_EQ(bottom->node.pConditions, nullptr);
      // Model a filter pushed onto the retained A/B subtree by PDC.
      bottom->node.pConditions = top->node.pConditions;
      top->node.pConditions = nullptr;
      ASSERT_EQ(planRebuildJoinInputProps(&copy.context, copy.root()), 0);
      auto model = prepareJoinModel(&copy.context, copy.root());
      ASSERT_NE(model, nullptr);
      ASSERT_EQ(taosArrayGetSize(model->pRegions), 1U);
      SJoinConflictModel* raw = nullptr;
      ASSERT_EQ(planBuildJoinConflicts(model.get(), regionAt(model.get(), 0),
                                        &raw), 0);
      ConflictPtr conflicts(raw, planDestroyJoinConflicts);
      ASSERT_NE(conflicts, nullptr);
      EXPECT_EQ(conflicts->pRootFilter, nullptr);
      const auto* lower =
          (const SJoinConflictOp*)taosArrayGet(conflicts->pOps, 2);
      EXPECT_TRUE(lower->pinnedFilter);
      EXPECT_FALSE(planJoinSplitIsLegal(conflicts.get(), 2, 1, 14, false));
      if (nullFilter) {
        // A match is removed after JOIN. Filtering B first would instead
        // manufacture an unmatched A row, which IS NULL would retain.
        std::array<ConflictBag, 4> tables;
        tables[0] = {{0, 2, 2, 2}};
        tables[1] = {{2, 0, 2, 2}};
        const auto original = conflictOriginal(conflicts.get());
        EXPECT_TRUE(conflictEvaluate(conflicts.get(), original, tables, 15)
                        .empty());
      } else {
        checkConflictFilterSample(conflicts.get());
      }
      checkConflictBags(conflicts.get(), 15);
    });
    const std::string condition = nullFilter ? "b.c1 IS NULL" : "a.c1>0";
    run("SELECT a.c1,b.c1,c.c1,d.c1 FROM "
        "t1 a LEFT JOIN t1 b ON a.c1=b.c1 LEFT JOIN t1 c ON b.c1=c.c1 "
        "LEFT JOIN t1 d ON c.c1=d.c1 WHERE " + condition);
  }
}
