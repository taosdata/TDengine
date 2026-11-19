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

#include "planJoinParallel.h"

#include <string.h>

#include "functionMgt.h"
#include "nodes.h"
#include "os.h"
#include "planInt.h"
#include "tarray.h"
#include "taoserror.h"
#include "thash.h"
#include "tsimplehash.h"
#include "ttypes.h"

// SJoinInputSet holds one bit per region input.
typedef char SJoinInputSetWidthCheck[JOIN_PARALLEL_MAX_INPUTS <= 64 ? 1 : -1];

typedef struct SJoinExprTraits {
  bool runtimeDependent;
  bool unsafe;
} SJoinExprTraits;

typedef struct SJoinUseWalker {
  const SNodeList* pTargets;
  FNodeWalker      walker;
  void*            pContext;
} SJoinUseWalker;

typedef struct SJoinMatchingUse {
  const SPlanContext*    pPlanCxt;
  const SLogicNode*      pOwner;
  const SJoinInputProps* pMatching;
  bool                   used;
} SJoinMatchingUse;

typedef struct SJoinDependencyWalker {
  const SJoinInputProps*     pProps;
  const SJoinParallelRegion* pRegion;
  SJoinParallelPredicate*    pPredicate;
} SJoinDependencyWalker;

bool planJoinInputSetContains(SJoinInputSet set, int32_t inputId) {
  if (inputId < 0 || inputId >= JOIN_PARALLEL_MAX_INPUTS) return false;
  return 0 != (set & (UINT64_C(1) << inputId));
}

int32_t planJoinInputForColumn(const SJoinParallelRegion* pRegion,
                               const SJoinColId*          pId) {
  if (NULL == pRegion || NULL == pId || NULL == pRegion->pColumnInputs) {
    return -1;
  }
  int32_t* pInput = tSimpleHashGet(pRegion->pColumnInputs, pId, sizeof(*pId));
  return NULL == pInput ? -1 : *pInput;
}

// Not resolveJoinColumn(): its name index keeps only the last identity of
// a duplicated name, while an ambiguous name must reject the column here.
static const SJoinOutputCol* joinParallelResolveColumn(
    const SJoinInputProps* pProps, const SColumnNode* pColumn) {
  if (NULL == pProps) return NULL;
  const SJoinOutputCol* pFound = NULL;
  for (size_t i = 0; i < taosArrayGetSize(pProps->pOutputCols); ++i) {
    const SJoinOutputCol* pOutput = taosArrayGet(pProps->pOutputCols, i);
    if (QUERY_NODE_COLUMN != nodeType(pOutput->pExpr)) continue;
    const SColumnNode* pCandidate = (const SColumnNode*)pOutput->pExpr;
    if (!planJoinColumnNamesEqual(pCandidate, pColumn)) {
      continue;
    }
    if (NULL != pFound &&
        (pFound->id.relationId != pOutput->id.relationId ||
         pFound->id.columnId != pOutput->id.columnId)) {
      return NULL;
    }
    pFound = pOutput;
  }
  return pFound;
}

// Map a column in the owner's scope to its region input, or -1. Owner
// entries copy their input's id and type.
static int32_t joinParallelColumnInput(const SJoinParallelRegion* pRegion,
                                       const SJoinInputProps*     pProps,
                                       const SColumnNode*         pColumn,
                                       const SJoinOutputCol**     ppOutput) {
  *ppOutput = joinParallelResolveColumn(pProps, pColumn);
  return NULL == *ppOutput ? -1
                           : planJoinInputForColumn(pRegion, &(*ppOutput)->id);
}

typedef struct SJoinNullRegion {
  const SJoinParallelRegion* pRegion;
  const SJoinInputProps*     pProps;
} SJoinNullRegion;

static uint64_t joinNullResolveRegionColumn(const SColumnNode* pColumn,
                                            const void* pContext) {
  const SJoinNullRegion* pRegion = pContext;
  const SJoinOutputCol* pOutput = NULL;
  int32_t input = joinParallelColumnInput(pRegion->pRegion, pRegion->pProps,
                                          pColumn, &pOutput);
  if (input < 0 || input >= JOIN_PARALLEL_MAX_INPUTS) return 0;
  return UINT64_C(1) << input;
}

SJoinInputSet planJoinCondRejectsNull(const SJoinParallelRegion* pRegion,
                                      const SJoinInputProps* pProps,
                                      const SNode* pCondition) {
  if (NULL == pRegion || NULL == pProps || !pRegion->supported) return 0;
  size_t numInputs = taosArrayGetSize(pRegion->pInputs);
  if (0 == numInputs || numInputs > JOIN_PARALLEL_MAX_INPUTS) return 0;
  SJoinNullRegion context = {.pRegion = pRegion, .pProps = pProps};
  return planCondRejectsNull(pCondition, (UINT64_C(1) << numInputs) - 1,
                             joinNullResolveRegionColumn, &context);
}

// Moerkotte et al., SIGMOD 2013: Tables 2/3. Rows are the first operator,
// columns the second; conditional entries are handled separately below.
// Category order: INNER/CROSS, LEFT OUTER, FULL OUTER, LEFT SEMI, LEFT ANTI.
static const bool joinConflictAssoc[][JOIN_CONFLICT_TYPE_COUNT] = {
    {true, true, false, true, true},
    {false, false, false, false, false},
    {false, false, false, false, false},
    {false, false, false, false, false},
    {false, false, false, false, false}};
static const bool joinConflictLeftAsscom[][JOIN_CONFLICT_TYPE_COUNT] = {
    {true, true, false, true, true},
    {true, true, false, true, true},
    {false, false, false, false, false},
    {true, true, false, true, true},
    {true, true, false, true, true}};
static const bool joinConflictRightAsscom[][JOIN_CONFLICT_TYPE_COUNT] = {
    {true, false, false, false, false},
    {false, false, false, false, false},
    {false, false, false, false, false},
    {false, false, false, false, false},
    {false, false, false, false, false}};

static bool joinConflictAssocHolds(const SJoinConflictOp* x,
                                    const SJoinConflictOp* y,
                                    SJoinInputSet shared) {
  if (joinConflictAssoc[x->type][y->type]) return true;
  if (JOIN_CONFLICT_LEFT == y->type &&
      (JOIN_CONFLICT_LEFT == x->type || JOIN_CONFLICT_FULL == x->type)) {
    return 0 != (y->nr & shared);
  }
  return JOIN_CONFLICT_FULL == x->type && JOIN_CONFLICT_FULL == y->type &&
         0 != (x->nr & shared) && 0 != (y->nr & shared);
}

static bool joinConflictLeftAsscomHolds(const SJoinConflictOp* x,
                                         const SJoinConflictOp* y,
                                         SJoinInputSet shared) {
  if (joinConflictLeftAsscom[x->type][y->type]) return true;
  if (JOIN_CONFLICT_LEFT == x->type && JOIN_CONFLICT_FULL == y->type) {
    return 0 != (x->nr & shared);
  }
  if (JOIN_CONFLICT_FULL == x->type && JOIN_CONFLICT_LEFT == y->type) {
    // Erratum: Table 3 footnote 2 says A(e3); it must be A(e1), the shared
    // input. Let e1={}, e2={1}, e3={1}, p12=(a=b), and
    // p13=(a IS NULL AND c>0). (e1 FULL e2) LEFT e3 yields (NULL,1,1),
    // but (e1 LEFT e3) FULL e2 yields (NULL,1,NULL).
    return 0 != (y->nr & shared);
  }
  return JOIN_CONFLICT_FULL == x->type && JOIN_CONFLICT_FULL == y->type &&
         0 != (x->nr & shared) && 0 != (y->nr & shared);
}

static bool joinConflictRightAsscomHolds(const SJoinConflictOp* x,
                                          const SJoinConflictOp* y,
                                          SJoinInputSet shared) {
  if (joinConflictRightAsscom[x->type][y->type]) return true;
  return JOIN_CONFLICT_FULL == x->type && JOIN_CONFLICT_FULL == y->type &&
         0 != (x->nr & shared) && 0 != (y->nr & shared);
}

static bool joinConflictSubset(SJoinInputSet subset, SJoinInputSet set) {
  return 0 == (subset & ~set);
}

// Stop at an opaque region input; its internal tables share this one bit.
static SJoinInputSet joinConflictInputSet(const SJoinParallelRegion* pRegion,
                                           const SLogicNode* pNode) {
  for (size_t i = 0; i < taosArrayGetSize(pRegion->pInputs); ++i) {
    const SJoinParallelInput* pInput = taosArrayGet(pRegion->pInputs, i);
    if (pInput->pNode == pNode) return UINT64_C(1) << i;
  }
  SJoinInputSet inputs = 0;
  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    inputs |= joinConflictInputSet(pRegion, (const SLogicNode*)pChild);
  }
  return inputs;
}

static bool joinConflictNormalize(SJoinConflictOp* pOp,
                                  SJoinInputSet originalLeft,
                                  SJoinInputSet originalRight) {
  EJoinType type = pOp->pNode->joinType;
  EJoinSubType subType = pOp->pNode->subType;
  if (JOIN_STYPE_SEMI == subType || JOIN_STYPE_ANTI == subType) {
    if (JOIN_TYPE_LEFT != type && JOIN_TYPE_RIGHT != type) return false;
    pOp->type = JOIN_STYPE_SEMI == subType ? JOIN_CONFLICT_SEMI
                                          : JOIN_CONFLICT_ANTI;
  } else if (JOIN_STYPE_NONE == subType || JOIN_STYPE_OUTER == subType) {
    switch (type) {
      case JOIN_TYPE_INNER:
        pOp->type = JOIN_CONFLICT_INNER;
        break;
      case JOIN_TYPE_LEFT:
      case JOIN_TYPE_RIGHT:
        pOp->type = JOIN_CONFLICT_LEFT;
        break;
      case JOIN_TYPE_FULL:
        pOp->type = JOIN_CONFLICT_FULL;
        break;
      default:
        return false;
    }
  } else {
    return false;
  }
  pOp->normalizedSwap = JOIN_TYPE_RIGHT == type;
  pOp->left = pOp->normalizedSwap ? originalRight : originalLeft;
  pOp->right = pOp->normalizedSwap ? originalLeft : originalRight;
  return true;
}

static int32_t joinConflictCollectOps(const SJoinRewriteContext* pContext,
                                      const SJoinParallelRegion* pRegion,
                                      SJoinConflictModel* pModel) {
  for (size_t i = 0; i < taosArrayGetSize(pRegion->pOps); ++i) {
    const SJoinLogicNode* pJoin = taosArrayGetP(pRegion->pOps, i);
    SJoinConflictOp op = {.pNode = pJoin, .opId = (int32_t)i,
                          .pinnedFilter = pJoin != pRegion->pRoot &&
                                          NULL != pJoin->node.pConditions};
    const SLogicNode* pLeft =
        (const SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0);
    const SLogicNode* pRight =
        (const SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1);
    SJoinInputSet originalLeft = joinConflictInputSet(pRegion, pLeft);
    SJoinInputSet originalRight = joinConflictInputSet(pRegion, pRight);
    if (!joinConflictNormalize(&op, originalLeft, originalRight)) {
      return TSDB_CODE_INVALID_PARA;
    }
    op.pRules = taosArrayInit(4, sizeof(SJoinConflictRule));
    if (NULL == op.pRules) return terrno;
    if (NULL == taosArrayPush(pModel->pOps, &op)) {
      taosArrayDestroy(op.pRules);
      return terrno;
    }
  }
  for (size_t i = 0; i < taosArrayGetSize(pRegion->pPredicates); ++i) {
    const SJoinParallelPredicate* pPred = taosArrayGet(pRegion->pPredicates, i);
    if (JOIN_PREDICATE_FILTER == pPred->source) continue;
    SJoinConflictOp* pOp = taosArrayGet(pModel->pOps, pPred->sourceOpId);
    const SJoinInputProps* pProps =
        planGetJoinInputProps(pContext->pPlanCxt, &pOp->pNode->node);
    if (NULL == pProps) return TSDB_CODE_INVALID_PARA;
    pOp->onInputs |= pPred->inputs;
    pOp->nr |= planJoinCondRejectsNull(pRegion, pProps, pPred->pExpr);
  }
  for (size_t i = 0; i < taosArrayGetSize(pModel->pOps); ++i) {
    SJoinConflictOp* pOp = taosArrayGet(pModel->pOps, i);
    pOp->requiredLeft = pOp->onInputs & pOp->left;
    pOp->requiredRight = pOp->onInputs & pOp->right;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t joinConflictAddRule(SJoinConflictOp* pOp,
                                    SJoinInputSet trigger,
                                    SJoinInputSet required,
                                    SJoinInputSet referenced) {
  // Figure 11 narrows only the required side to the descendant's ON uses;
  // if that intersection is empty, keep its entire original input side.
  if (0 != (required & referenced)) required &= referenced;
  for (size_t i = 0; i < taosArrayGetSize(pOp->pRules); ++i) {
    const SJoinConflictRule* pRule = taosArrayGet(pOp->pRules, i);
    if (pRule->trigger == trigger && pRule->required == required) {
      return TSDB_CODE_SUCCESS;
    }
  }
  SJoinConflictRule rule = {.trigger = trigger, .required = required};
  return NULL == taosArrayPush(pOp->pRules, &rule) ? terrno : TSDB_CODE_SUCCESS;
}

static int32_t joinConflictBuildRules(SJoinConflictOp* b,
                                      const SJoinConflictOp* a) {
  SJoinInputSet inputs = a->left | a->right;
  if (joinConflictSubset(inputs, b->left)) {
    if (!joinConflictAssocHolds(a, b, a->right)) {
      PLAN_ERR_RET(joinConflictAddRule(b, a->right, a->left, a->onInputs));
    }
    if (!joinConflictLeftAsscomHolds(a, b, a->left)) {
      PLAN_ERR_RET(joinConflictAddRule(b, a->left, a->right, a->onInputs));
    }
  } else if (joinConflictSubset(inputs, b->right)) {
    if (!joinConflictAssocHolds(b, a, a->left)) {
      PLAN_ERR_RET(joinConflictAddRule(b, a->left, a->right, a->onInputs));
    }
    if (!joinConflictRightAsscomHolds(b, a, a->right)) {
      PLAN_ERR_RET(joinConflictAddRule(b, a->right, a->left, a->onInputs));
    }
  }
  return TSDB_CODE_SUCCESS;
}

void planDestroyJoinConflicts(SJoinConflictModel* pModel) {
  if (NULL == pModel) return;
  for (size_t i = 0; i < taosArrayGetSize(pModel->pOps); ++i) {
    SJoinConflictOp* pOp = taosArrayGet(pModel->pOps, i);
    taosArrayDestroy(pOp->pRules);
  }
  taosArrayDestroy(pModel->pOps);
  taosMemoryFree(pModel);
}

int32_t planBuildJoinConflicts(const SJoinRewriteContext* pContext,
                                const SJoinParallelRegion* pRegion,
                                SJoinConflictModel** ppModel) {
  if (NULL == ppModel) return TSDB_CODE_INVALID_PARA;
  *ppModel = NULL;
  if (NULL == pContext || NULL == pRegion) return TSDB_CODE_INVALID_PARA;
  if (!pRegion->supported || JOIN_REGION_MIXED != pRegion->mode) {
    return TSDB_CODE_SUCCESS;
  }
  SJoinConflictModel* pModel = taosMemoryCalloc(1, sizeof(*pModel));
  if (NULL == pModel) return terrno;
  pModel->pRootFilter = pRegion->pRoot->node.pConditions;
  pModel->pOps = taosArrayInit(4, sizeof(SJoinConflictOp));
  int32_t code = NULL == pModel->pOps ? terrno : TSDB_CODE_SUCCESS;
  if (TSDB_CODE_SUCCESS == code) {
    pModel->inputs =
        (UINT64_C(1) << taosArrayGetSize(pRegion->pInputs)) - 1;
    code = joinConflictCollectOps(pContext, pRegion, pModel);
  }
  for (size_t j = 0; code == 0 && j < taosArrayGetSize(pModel->pOps); ++j) {
    SJoinConflictOp* b = taosArrayGet(pModel->pOps, j);
    for (size_t i = 0; code == 0 && i < taosArrayGetSize(pModel->pOps); ++i) {
      if (i != j) code = joinConflictBuildRules(b,
                                               taosArrayGet(pModel->pOps, i));
    }
  }
  if (TSDB_CODE_SUCCESS != code) {
    planDestroyJoinConflicts(pModel);
    return code;
  }
  *ppModel = pModel;
  return TSDB_CODE_SUCCESS;
}

bool planJoinSplitIsLegal(const SJoinConflictModel* pModel, int32_t opId,
                           SJoinInputSet left, SJoinInputSet right,
                           bool swapped) {
  if (NULL == pModel || opId < 0 ||
      (size_t)opId >= taosArrayGetSize(pModel->pOps) ||
      0 == left || 0 == right || 0 != (left & right) ||
      !joinConflictSubset(left | right, pModel->inputs)) return false;
  const SJoinConflictOp* pOp = taosArrayGet(pModel->pOps, opId);
  SJoinInputSet inputs = left | right;
  if (pOp->pinnedFilter && inputs != (pOp->left | pOp->right)) return false;
  if (pOp->normalizedSwap != swapped) {
    SJoinInputSet tmp = left;
    left = right;
    right = tmp;
  }
  if (!joinConflictSubset(pOp->requiredLeft, left) ||
      !joinConflictSubset(pOp->requiredRight, right)) return false;
  // Section 6.2 anchors CROSS/constant/one-sided ONs. Two-sided ONs already
  // imply these intersections through their nonempty required sets.
  if (0 == (left & pOp->left) || 0 == (right & pOp->right)) return false;
  for (size_t i = 0; i < taosArrayGetSize(pOp->pRules); ++i) {
    const SJoinConflictRule* pRule = taosArrayGet(pOp->pRules, i);
    if (0 != (inputs & pRule->trigger) &&
        !joinConflictSubset(pRule->required, inputs)) return false;
  }
  return true;
}

const SJoinDpCell* planJoinTreeCell(const SJoinTreeChoice* pChoice,
                                    int32_t cellId) {
  if (NULL == pChoice || cellId < 0 ||
      (size_t)cellId >= taosArrayGetSize(pChoice->pCells)) return NULL;
  return taosArrayGet(pChoice->pCells, cellId);
}

void planDestroyJoinTreeChoice(SJoinTreeChoice* pChoice) {
  if (NULL == pChoice) return;
  for (size_t i = 0; i < taosArrayGetSize(pChoice->pCells); ++i) {
    const SJoinDpCell* pCell = taosArrayGet(pChoice->pCells, i);
    planDestroyJoinTreeChoice(pCell->pPureChoice);
  }
  planDestroyJoinEqClasses(pChoice->pEqClasses);
  planDestroyJoinConflicts(pChoice->pConflicts);
  taosArrayDestroy(pChoice->pPredicates);
  taosArrayDestroy(pChoice->pCells);
  taosMemoryFree(pChoice);
}

typedef struct SJoinSearchContext {
  const SJoinRewriteContext* pContext;
  SJoinTreeChoice*            pChoice;
  uint32_t                   stride;
  int32_t                    numOps;
  int32_t                    numCross;
  SJoinInputSet              crossLeft[JOIN_PARALLEL_MAX_INPUTS - 1];
  SJoinInputSet              crossRight[JOIN_PARALLEL_MAX_INPUTS - 1];
} SJoinSearchContext;

static int32_t joinDpBitCount(uint64_t bits) {
  int32_t count = 0;
  for (; bits != 0; bits &= bits - 1) ++count;
  return count;
}

static int32_t joinDpLowerBound(int32_t inputs) {
  int32_t height = 0;
  for (int32_t leaves = 1; leaves < inputs; leaves *= 2) ++height;
  return height;
}

// Opaque inputs have height zero even when their own plans contain JOINs.
static int32_t joinDpOriginalHeight(const SJoinParallelRegion* pRegion,
                                    const SLogicNode* pNode) {
  for (size_t i = 0; i < taosArrayGetSize(pRegion->pInputs); ++i) {
    const SJoinParallelInput* pInput = taosArrayGet(pRegion->pInputs, i);
    if (pInput->pNode == pNode) return 0;
  }
  if (NULL == pNode || QUERY_NODE_LOGIC_PLAN_JOIN != nodeType(pNode) ||
      2 != LIST_LENGTH(pNode->pChildren)) return -1;
  int32_t left = joinDpOriginalHeight(pRegion,
      (const SLogicNode*)nodesListGetNode(pNode->pChildren, 0));
  int32_t right = joinDpOriginalHeight(pRegion,
      (const SLogicNode*)nodesListGetNode(pNode->pChildren, 1));
  return left < 0 || right < 0 ? -1 : 1 + TMAX(left, right);
}

static int32_t joinDpIndex(const SJoinSearchContext* pSearch,
                            SJoinInputSet inputs, uint32_t usedOps) {
  return (int32_t)(inputs * pSearch->stride + usedOps);
}

static bool joinDpConnected(const SJoinSearchContext* pSearch,
                             SJoinInputSet left, SJoinInputSet right,
                             bool* pCross) {
  SJoinInputSet inputs = left | right;
  *pCross = false;
  const SArray* pPredicates = pSearch->pChoice->pPredicates;
  for (size_t i = 0; i < taosArrayGetSize(pPredicates); ++i) {
    const SJoinParallelPredicate* pPred = taosArrayGet(pPredicates, i);
    // A whole OR/NOT or a multi-input expression can connect this split only
    // after all its dependencies are available. Single-side filters cannot.
    if (joinConflictSubset(pPred->inputs, inputs) &&
        0 != (pPred->inputs & left) && 0 != (pPred->inputs & right)) {
      return true;
    }
  }
  if (planJoinEqClassesConnect(pSearch->pChoice->pEqClasses, left, right)) {
    return true;
  }
  for (int32_t i = 0; i < pSearch->numCross; ++i) {
    if ((left == pSearch->crossLeft[i] && right == pSearch->crossRight[i]) ||
        (right == pSearch->crossLeft[i] && left == pSearch->crossRight[i])) {
      *pCross = true;
      return true;
    }
  }
  return false;
}

static void joinDpRecord(SJoinSearchContext* pSearch, SJoinInputSet inputs,
                          uint32_t usedOps, int32_t left, int32_t right,
                          int32_t opId, bool swapped, bool isCross) {
  const SJoinDpCell* l = planJoinTreeCell(pSearch->pChoice, left);
  const SJoinDpCell* r = planJoinTreeCell(pSearch->pChoice, right);
  if (!l->valid || !r->valid) return;
  int32_t height = 1 + TMAX(l->height, r->height);
  SJoinDpCell* pCell = taosArrayGet(pSearch->pChoice->pCells,
                                    joinDpIndex(pSearch, inputs, usedOps));
  // Keep every reachable state: a tall INNER state may become shorter after
  // pure optimization. Fixed enumeration order keeps the first minimum.
  if (pCell->valid && height >= pCell->height) return;
  planDestroyJoinTreeChoice(pCell->pPureChoice);
  *pCell = (SJoinDpCell){.inputs = inputs, .usedOps = usedOps, .height = height,
                         .left = left, .right = right, .inputId = -1,
                         .opId = opId, .valid = true, .swapped = swapped,
                         .isCross = isCross};
}

static void joinDpPureState(SJoinSearchContext* pSearch,
                             SJoinInputSet inputs) {
  int32_t lowerBound = joinDpLowerBound(joinDpBitCount(inputs));
  for (SJoinInputSet left = 1; left < inputs; ++left) {
    if (!joinConflictSubset(left, inputs)) continue;
    SJoinInputSet right = inputs ^ left;
    if (left > right) continue;
    bool isCross = false;
    if (!joinDpConnected(pSearch, left, right, &isCross)) continue;
    joinDpRecord(pSearch, inputs, 0, (int32_t)left, (int32_t)right, -1, false,
                   isCross);
    const SJoinDpCell* cell = planJoinTreeCell(pSearch->pChoice,
                                              (int32_t)inputs);
    if (cell->valid && cell->height == lowerBound) return;
  }
}

typedef int32_t (*FJoinConjunctVisitor)(SNode** ppExpr, void* pContext);

// Analysis borrows each slot; construction may detach leaves from its copy.
// OR/NOT stay intact so their internal ANDs never become separate predicates.
static int32_t joinParallelVisitConjuncts(SNode** ppExpr,
                                         FJoinConjunctVisitor visitor,
                                         void* pContext) {
  if (NULL == *ppExpr) return TSDB_CODE_SUCCESS;
  if (QUERY_NODE_LOGIC_CONDITION != nodeType(*ppExpr) ||
      LOGIC_COND_TYPE_AND != ((SLogicConditionNode*)*ppExpr)->condType) {
    return visitor(ppExpr, pContext);
  }
  SNodeList* pChildren =
      ((SLogicConditionNode*)*ppExpr)->pParameterList;
  for (SListCell* pCell = NULL == pChildren ? NULL : pChildren->pHead;
       NULL != pCell; pCell = pCell->pNext) {
    PLAN_ERR_RET(joinParallelVisitConjuncts(&pCell->pNode, visitor, pContext));
  }
  return TSDB_CODE_SUCCESS;
}

static void joinParallelDescribePredicate(const SPlanContext* pCxt,
                                          const SJoinParallelRegion* pRegion,
                                          SJoinParallelPredicate* pPredicate);
static int32_t joinParallelAddPredicate(const SJoinRewriteContext* pCxt,
                                        SJoinParallelRegion* pRegion,
                                        const SNode* pExpr, int32_t opId,
                                        EJoinPredicateSource source);
static void joinDpInitialize(SJoinSearchContext* pSearch,
                              SJoinInputSet inputs, uint32_t usedOps);

static SJoinTreeChoice* joinDpCreateChoice(const SJoinParallelRegion* pRegion,
                                          EJoinRegionMode mode,
                                          SJoinInputSet inputs, uint32_t stride,
                                          int32_t originalHeight) {
  SJoinTreeChoice* pChoice = taosMemoryCalloc(1, sizeof(*pChoice));
  if (NULL == pChoice) return NULL;
  pChoice->pRegion = pRegion;
  pChoice->mode = mode;
  pChoice->originalHeight = originalHeight;
  pChoice->pCells = taosArrayInit_s(sizeof(SJoinDpCell),
                                   (size_t)(inputs + 1) * stride);
  if (NULL == pChoice->pCells) {
    planDestroyJoinTreeChoice(pChoice);
    return NULL;
  }
  return pChoice;
}

static int32_t joinDpPurePredicates(const SJoinRewriteContext* pContext,
                                    SJoinTreeChoice* pChoice,
                                    SJoinInputSet inputs, uint32_t usedOps,
                                    bool filters) {
  // Keep original owners and column identities while splitting only this
  // state's ON clauses. No equality may come from an operator outside U.
  SJoinParallelRegion view = *pChoice->pRegion;
  view.mode = JOIN_REGION_PURE_INNER;
  pChoice->pPredicates = taosArrayInit(8, sizeof(SJoinParallelPredicate));
  if (NULL == pChoice->pPredicates) return terrno;
  view.pPredicates = pChoice->pPredicates;
  const SArray* pPredicates = pChoice->pRegion->pPredicates;
  for (size_t i = 0; i < taosArrayGetSize(pPredicates); ++i) {
    const SJoinParallelPredicate* pPred = taosArrayGet(pPredicates, i);
    if (0 == (usedOps & (UINT32_C(1) << pPred->sourceOpId)) ||
        (!filters && JOIN_PREDICATE_FILTER == pPred->source)) continue;
    if (!joinConflictSubset(pPred->inputs, inputs)) {
      return TSDB_CODE_INVALID_PARA;
    }
    PLAN_ERR_RET(joinParallelAddPredicate(pContext, &view, pPred->pExpr,
                                            pPred->sourceOpId, pPred->source));
  }
  PLAN_ERR_RET(planBuildJoinEqClasses(pContext, &view, &pChoice->pEqClasses));
  if (NULL == pChoice->pEqClasses) return TSDB_CODE_INVALID_PARA;
  pChoice->pEqClasses->inputs = inputs;
  return TSDB_CODE_SUCCESS;
}

// Explicit S/U replaces a physical root: this state need not occur in the
// original tree. Pure cells retain parent input ids in their own state table.
static int32_t joinDpSearchPure(const SJoinRewriteContext* pContext,
                                const SJoinParallelRegion* pRegion,
                                SJoinInputSet inputs, uint32_t usedOps,
                                int32_t height, bool filters,
                                SJoinTreeChoice** ppChoice) {
  *ppChoice = NULL;
  SJoinTreeChoice* pChoice = joinDpCreateChoice(pRegion, JOIN_REGION_PURE_INNER,
                                               inputs, 1, height);
  if (NULL == pChoice) return terrno;
  int32_t code = joinDpPurePredicates(pContext, pChoice, inputs, usedOps,
                                      filters);
  if (TSDB_CODE_SUCCESS != code) goto _end;
  SJoinSearchContext search = {.pContext = pContext, .pChoice = pChoice,
                               .stride = 1};
  joinDpInitialize(&search, inputs, usedOps);
  for (SJoinInputSet subset = 1; subset <= inputs; ++subset) {
    if (joinConflictSubset(subset, inputs) && joinDpBitCount(subset) > 1) {
      joinDpPureState(&search, subset);
    }
  }
  pChoice->rootCell = (int32_t)inputs;
  const SJoinDpCell* pRoot = planJoinTreeCell(pChoice, pChoice->rootCell);
  if (pRoot->valid && pRoot->height < height) {
    *ppChoice = pChoice;
    return TSDB_CODE_SUCCESS;
  }
_end:
  planDestroyJoinTreeChoice(pChoice);
  return code;
}

static int32_t joinDpImprovePure(SJoinSearchContext* pSearch,
                                 SJoinInputSet inputs, uint32_t usedOps) {
  SJoinDpCell* pCell = taosArrayGet(pSearch->pChoice->pCells,
                                    joinDpIndex(pSearch, inputs, usedOps));
  int32_t count = joinDpBitCount(inputs);
  if (!pCell->valid || count < 4 ||
      pCell->height == joinDpLowerBound(count)) return TSDB_CODE_SUCCESS;
  const SJoinConflictModel* pModel = pSearch->pChoice->pConflicts;
  for (int32_t i = 0; i < pSearch->numOps; ++i) {
    if (0 == (usedOps & (UINT32_C(1) << i))) continue;
    const SJoinConflictOp* pOp = taosArrayGet(pModel->pOps, i);
    if (!IS_INNER_NONE_JOIN(pOp->pNode->joinType, pOp->pNode->subType) ||
        pOp->pNode->isCrossJoin || pOp->pinnedFilter) {
      return TSDB_CODE_SUCCESS;
    }
  }
  SJoinTreeChoice* pPure = NULL;
  PLAN_ERR_RET(joinDpSearchPure(pSearch->pContext, pSearch->pChoice->pRegion,
                                 inputs, usedOps, pCell->height, false,
                                 &pPure));
  if (NULL == pPure) return TSDB_CODE_SUCCESS;
  const SJoinDpCell* pRoot = planJoinTreeCell(pPure, pPure->rootCell);
  // The ordinary transition proves this S/U reachable. Replace only its
  // complete result; pure intermediate states have no mixed operator mapping.
  *pCell = (SJoinDpCell){.inputs = inputs, .usedOps = usedOps,
                         .height = pRoot->height,
                         .left = -1, .right = -1, .inputId = -1, .opId = -1,
                         .valid = true, .pPureChoice = pPure};
  return TSDB_CODE_SUCCESS;
}

static void joinDpMixedSplit(SJoinSearchContext* pSearch,
                              SJoinInputSet inputs, uint32_t usedOps,
                              SJoinInputSet left, SJoinInputSet right) {
  int32_t leftOps = joinDpBitCount(left) - 1;
  for (int32_t opId = 0; opId < pSearch->numOps; ++opId) {
    uint32_t bit = UINT32_C(1) << opId;
    if (0 == (usedOps & bit)) continue;
    const SJoinConflictModel* pModel = pSearch->pChoice->pConflicts;
    bool legal = planJoinSplitIsLegal(pModel, opId, left, right, false);
    bool swapped = !legal;
    if (!legal && !planJoinSplitIsLegal(pModel, opId, left, right, true)) {
      continue;
    }
    uint32_t remaining = usedOps ^ bit;
    for (uint32_t used = 0; used <= remaining; ++used) {
      if (!joinConflictSubset(used, remaining) ||
          joinDpBitCount(used) != leftOps) continue;
      joinDpRecord(pSearch, inputs, usedOps,
                     joinDpIndex(pSearch, left, used),
                     joinDpIndex(pSearch, right, remaining ^ used), opId,
                     swapped, false);
    }
  }
}

static int32_t joinDpMixedState(SJoinSearchContext* pSearch,
                                 SJoinInputSet inputs, uint32_t usedOps) {
  int32_t lowerBound = joinDpLowerBound(joinDpBitCount(inputs));
  int32_t index = joinDpIndex(pSearch, inputs, usedOps);
  for (SJoinInputSet left = 1; left < inputs; ++left) {
    if (!joinConflictSubset(left, inputs)) continue;
    SJoinInputSet right = inputs ^ left;
    // Mirrored physical sides are represented by swapped plus LEFT/RIGHT
    // normalization, so one numeric ordering covers both orientations.
    if (left > right) continue;
    joinDpMixedSplit(pSearch, inputs, usedOps, left, right);
    const SJoinDpCell* cell = planJoinTreeCell(pSearch->pChoice, index);
    if (cell->valid && cell->height == lowerBound) break;
  }
  return joinDpImprovePure(pSearch, inputs, usedOps);
}

static void joinDpInitialize(SJoinSearchContext* pSearch,
                              SJoinInputSet inputs, uint32_t usedOps) {
  SJoinTreeChoice* pChoice = pSearch->pChoice;
  int32_t numInputs = (int32_t)taosArrayGetSize(pChoice->pRegion->pInputs);
  for (int32_t i = 0; i < numInputs; ++i) {
    SJoinInputSet bit = UINT64_C(1) << i;
    if (0 == (inputs & bit)) continue;
    SJoinDpCell* cell = taosArrayGet(pChoice->pCells,
                                    joinDpIndex(pSearch, bit, 0));
    *cell = (SJoinDpCell){.inputs = bit, .inputId = i, .opId = -1,
                          .left = -1, .right = -1, .valid = true};
  }
  if (JOIN_REGION_PURE_INNER != pChoice->mode) return;
  for (size_t i = 0; i < taosArrayGetSize(pChoice->pRegion->pOps); ++i) {
    const SJoinLogicNode* pJoin = taosArrayGetP(pChoice->pRegion->pOps, i);
    if (0 == (usedOps & (UINT32_C(1) << i)) || !pJoin->isCrossJoin) continue;
    const SLogicNode* left =
        (const SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0);
    const SLogicNode* right =
        (const SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1);
    int32_t index = pSearch->numCross++;
    pSearch->crossLeft[index] = joinConflictInputSet(pChoice->pRegion, left);
    pSearch->crossRight[index] = joinConflictInputSet(pChoice->pRegion, right);
  }
}

int32_t planSearchJoinTree(const SJoinRewriteContext* pContext,
                            const SJoinParallelRegion* pRegion,
                            SJoinTreeChoice** ppChoice) {
  if (NULL == ppChoice) return TSDB_CODE_INVALID_PARA;
  *ppChoice = NULL;
  if (NULL == pContext || NULL == pRegion || NULL == pContext->pPlanCxt) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (!pRegion->supported) return TSDB_CODE_SUCCESS;
  int32_t numInputs = (int32_t)taosArrayGetSize(pRegion->pInputs);
  if (numInputs < 2 || numInputs > JOIN_PARALLEL_MAX_INPUTS ||
      taosArrayGetSize(pRegion->pOps) != (size_t)(numInputs - 1)) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t height = joinDpOriginalHeight(pRegion, &pRegion->pRoot->node);
  if (height <= joinDpLowerBound(numInputs)) return TSDB_CODE_SUCCESS;
  SJoinInputSet all = (UINT64_C(1) << numInputs) - 1;
  if (JOIN_REGION_PURE_INNER == pRegion->mode) {
    return joinDpSearchPure(pContext, pRegion, all,
                             (UINT32_C(1) << (numInputs - 1)) - 1, height,
                             true, ppChoice);
  }
  if (JOIN_REGION_MIXED != pRegion->mode) return TSDB_CODE_INVALID_PARA;
  uint32_t stride = UINT32_C(1) << (numInputs - 1);
  SJoinTreeChoice* pChoice = joinDpCreateChoice(pRegion, JOIN_REGION_MIXED,
                                               all, stride, height);
  if (NULL == pChoice) return terrno;
  SJoinSearchContext search = {.pContext = pContext, .pChoice = pChoice,
                               .numOps = numInputs - 1, .stride = stride};
  int32_t code = planBuildJoinConflicts(pContext, pRegion,
                                        &pChoice->pConflicts);
  if (TSDB_CODE_SUCCESS != code) goto _end;
  pChoice->pRootFilter = pChoice->pConflicts->pRootFilter;
  joinDpInitialize(&search, all, 0);
  for (SJoinInputSet inputs = 1; inputs <= all; ++inputs) {
    int32_t count = joinDpBitCount(inputs);
    if (count == 1) continue;
    for (uint32_t used = 1; used < search.stride; ++used) {
      if (joinDpBitCount(used) == count - 1) {
        code = joinDpMixedState(&search, inputs, used);
        if (TSDB_CODE_SUCCESS != code) goto _end;
      }
    }
  }
  pChoice->rootCell = joinDpIndex(&search, all, search.stride - 1);
  const SJoinDpCell* pRoot = planJoinTreeCell(pChoice, pChoice->rootCell);
  if (!pRoot->valid || pRoot->height >= height) goto _end;
  *ppChoice = pChoice;
  return TSDB_CODE_SUCCESS;

_end:
  planDestroyJoinTreeChoice(pChoice);
  return code;
}

typedef struct SJoinEqMember {
  int32_t       parent;
  int32_t       rank;
  SJoinInputSet inputs;  // Valid on class roots only.
} SJoinEqMember;

void planDestroyJoinEqClasses(SJoinEqClasses* pClasses) {
  if (NULL == pClasses) return;
  taosArrayDestroy(pClasses->pMembers);
  tSimpleHashCleanup(pClasses->pColumnMembers);
  taosMemoryFree(pClasses);
}

static int32_t joinEqColumnMember(const SJoinEqClasses* pClasses,
                                  const SJoinColId* pId) {
  if (NULL == pClasses || NULL == pId) return -1;
  const int32_t* pIndex =
      tSimpleHashGet(pClasses->pColumnMembers, pId, sizeof(*pId));
  return NULL == pIndex ? -1 : *pIndex;
}

int32_t planJoinEqClassForColumn(const SJoinEqClasses* pClasses,
                                 const SJoinColId* pId) {
  int32_t index = joinEqColumnMember(pClasses, pId);
  if (index < 0) return -1;
  const SJoinEqMember* pMember = taosArrayGet(pClasses->pMembers, index);
  return pMember->parent;
}

bool planJoinEqClassesConnect(const SJoinEqClasses* pClasses,
                              SJoinInputSet left, SJoinInputSet right) {
  if (NULL == pClasses || 0 == left || 0 == right || 0 != (left & right) ||
      0 != ((left | right) & ~pClasses->inputs)) {
    return false;
  }
  for (size_t i = 0; i < taosArrayGetSize(pClasses->pMembers); ++i) {
    const SJoinEqMember* pMember = taosArrayGet(pClasses->pMembers, i);
    if (pMember->parent == (int32_t)i && 0 != (pMember->inputs & left) &&
        0 != (pMember->inputs & right)) {
      return true;
    }
  }
  return false;
}

static bool joinEqTypesCompatible(SDataType left, SDataType right) {
  if (left.type != right.type) return false;
  if (TSDB_DATA_TYPE_TIMESTAMP == left.type) {
    return left.precision == right.precision;
  }
  // Float comparisons use tolerance; casts and other coercions do not prove
  // the exact, transitive equality required when deriving new JOIN edges.
  return IS_INTEGER_TYPE(left.type) || IS_STR_DATA_TYPE(left.type);
}

// Columns are keyed by output identity, not by name, so self-JOIN aliases
// and projections of one input stay distinct.
static int32_t joinEqAddMember(SJoinEqClasses* pClasses, const SJoinColId* pId,
                               int32_t inputId, int32_t* pIndex) {
  const int32_t* pExisting =
      tSimpleHashGet(pClasses->pColumnMembers, pId, sizeof(*pId));
  if (NULL != pExisting) {
    *pIndex = *pExisting;
    return TSDB_CODE_SUCCESS;
  }
  *pIndex = (int32_t)taosArrayGetSize(pClasses->pMembers);
  SJoinEqMember member = {.parent = *pIndex,
                          .inputs = UINT64_C(1) << inputId};
  if (NULL == taosArrayPush(pClasses->pMembers, &member)) return terrno;
  return tSimpleHashPut(pClasses->pColumnMembers, pId, sizeof(*pId), pIndex,
                        sizeof(*pIndex));
}

static int32_t joinEqFindRoot(SJoinEqClasses* pClasses, int32_t index) {
  SJoinEqMember* pMember = taosArrayGet(pClasses->pMembers, index);
  if (pMember->parent != index) {
    pMember->parent = joinEqFindRoot(pClasses, pMember->parent);
  }
  return pMember->parent;
}

static bool joinEqUnionMembers(SJoinEqClasses* pClasses, int32_t left,
                               int32_t right) {
  left = joinEqFindRoot(pClasses, left);
  right = joinEqFindRoot(pClasses, right);
  if (left == right) return false;
  SJoinEqMember* pLeftRoot = taosArrayGet(pClasses->pMembers, left);
  SJoinEqMember* pRightRoot = taosArrayGet(pClasses->pMembers, right);
  // Column count is not bounded by the input limit.
  if (pLeftRoot->rank < pRightRoot->rank) {
    pLeftRoot->parent = right;
    pRightRoot->inputs |= pLeftRoot->inputs;
  } else {
    pRightRoot->parent = left;
    pLeftRoot->inputs |= pRightRoot->inputs;
    if (pLeftRoot->rank == pRightRoot->rank) ++pLeftRoot->rank;
  }
  return true;
}

// Supported regions resolve every predicate column to one input.
static int32_t joinEqAddPredicate(SJoinEqClasses* pClasses,
                                  const SJoinRewriteContext* pContext,
                                  const SJoinParallelRegion* pRegion,
                                  const SJoinParallelPredicate* pPredicate) {
  // Region collection splits top-level AND; equalities inside OR/NOT do not
  // hold for every output row and must remain opaque here.
  if (QUERY_NODE_OPERATOR != nodeType(pPredicate->pExpr)) {
    return TSDB_CODE_SUCCESS;
  }
  const SOperatorNode* pOp = (const SOperatorNode*)pPredicate->pExpr;
  if (OP_TYPE_EQUAL != pOp->opType || NULL == pOp->pLeft ||
      NULL == pOp->pRight || QUERY_NODE_COLUMN != nodeType(pOp->pLeft) ||
      QUERY_NODE_COLUMN != nodeType(pOp->pRight)) {
    return TSDB_CODE_SUCCESS;
  }
  const SJoinLogicNode* const* ppOwner =
      taosArrayGet(pRegion->pOps, pPredicate->sourceOpId);
  const SJoinInputProps* pProps =
      planGetJoinInputProps(pContext->pPlanCxt, &(*ppOwner)->node);
  const SJoinOutputCol* pLeft = NULL;
  const SJoinOutputCol* pRight = NULL;
  int32_t leftInput = joinParallelColumnInput(
      pRegion, pProps, (const SColumnNode*)pOp->pLeft, &pLeft);
  int32_t rightInput = joinParallelColumnInput(
      pRegion, pProps, (const SColumnNode*)pOp->pRight, &pRight);
  if (leftInput < 0 || rightInput < 0 ||
      !joinEqTypesCompatible(pLeft->type, pRight->type)) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t left = -1;
  int32_t right = -1;
  PLAN_ERR_RET(joinEqAddMember(pClasses, &pLeft->id, leftInput, &left));
  PLAN_ERR_RET(joinEqAddMember(pClasses, &pRight->id, rightInput, &right));
  (void)joinEqUnionMembers(pClasses, left, right);
  return TSDB_CODE_SUCCESS;
}

int32_t planBuildJoinEqClasses(const SJoinRewriteContext* pContext,
                               const SJoinParallelRegion* pRegion,
                               SJoinEqClasses** ppClasses) {
  if (NULL == ppClasses) return TSDB_CODE_INVALID_PARA;
  *ppClasses = NULL;
  if (NULL == pContext || NULL == pRegion) return TSDB_CODE_INVALID_PARA;
  // Over-limit regions are never supported.
  if (!pRegion->supported || JOIN_REGION_PURE_INNER != pRegion->mode) {
    return TSDB_CODE_SUCCESS;
  }
  SJoinEqClasses* pClasses = taosMemoryCalloc(1, sizeof(*pClasses));
  if (NULL == pClasses) return terrno;
  int32_t code = TSDB_CODE_SUCCESS;
  pClasses->pMembers = taosArrayInit(8, sizeof(SJoinEqMember));
  pClasses->pColumnMembers =
      tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
  if (NULL == pClasses->pMembers || NULL == pClasses->pColumnMembers) {
    code = terrno;
    goto _end;
  }
  pClasses->inputs =
      (UINT64_C(1) << taosArrayGetSize(pRegion->pInputs)) - 1;
  for (size_t i = 0; i < taosArrayGetSize(pRegion->pPredicates); ++i) {
    code = joinEqAddPredicate(pClasses, pContext, pRegion,
                              taosArrayGet(pRegion->pPredicates, i));
    if (TSDB_CODE_SUCCESS != code) goto _end;
  }
  // Flatten once so read-only class queries need no mutable analysis state.
  for (size_t i = 0; i < taosArrayGetSize(pClasses->pMembers); ++i) {
    (void)joinEqFindRoot(pClasses, (int32_t)i);
  }
  *ppClasses = pClasses;
  return TSDB_CODE_SUCCESS;

_end:
  planDestroyJoinEqClasses(pClasses);
  return code;
}

typedef struct SJoinBuildResult {
  SLogicNode* pRoot;
  SLogicNode* pRegionRoot;
  SLogicNode* pInputs[JOIN_PARALLEL_MAX_INPUTS];
  const SLogicNode* pInputOrigins[JOIN_PARALLEL_MAX_INPUTS];
  bool singleTableInputs[JOIN_PARALLEL_MAX_INPUTS];
  SJoinLogicNode* pJoins[JOIN_PARALLEL_MAX_INPUTS - 1];
} SJoinBuildResult;

typedef struct SJoinBuildContext {
  const SPlanContext* pPlanCxt;
  const SJoinTreeChoice* pChoice;
  SJoinBuildResult result;
  SListCell* pRegionCell;
  SLogicNode* pRegionParent;
  SNodeList* pTargets;
  SNode* pRootFilter;
  SJoinInputSet usedInputs;
  uint32_t usedOps;
  bool stmtRoot;
  bool detached;
} SJoinBuildContext;

typedef struct SJoinBuildPredicate {
  SNode* pExpr;
  SJoinInputSet inputs;
  EJoinPredicateSource source;
  int32_t cellId;
  int32_t leftMember;
  int32_t rightMember;
} SJoinBuildPredicate;

typedef struct SJoinPureBuild {
  const SJoinTreeChoice* pChoice;
  SJoinLogicNode* pNodes[1 << JOIN_PARALLEL_MAX_INPUTS];
  uint32_t available;
  SArray* pPredicates;
} SJoinPureBuild;

typedef struct SJoinBuildPredicateCollector {
  const SJoinBuildContext* pCxt;
  SJoinPureBuild* pPure;
  int32_t opId;
  EJoinPredicateSource source;
} SJoinBuildPredicateCollector;

static int32_t joinBuildMap(SJoinBuildContext* pCxt,
                            const SLogicNode* pOld, SLogicNode* pCopy,
                            SLogicNode* pParent) {
  if (LIST_LENGTH(pOld->pChildren) != LIST_LENGTH(pCopy->pChildren)) {
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }
  pCopy->pParent = pParent;
  pCopy->stmtRoot = pOld->stmtRoot;
  const SJoinParallelRegion* pRegion = pCxt->pChoice->pRegion;
  for (size_t i = 0; i < taosArrayGetSize(pRegion->pInputs); ++i) {
    const SJoinParallelInput* pInput = taosArrayGet(pRegion->pInputs, i);
    if (pOld == pInput->pNode) {
      pCxt->result.pInputs[i] = pCopy;
      pCxt->result.pInputOrigins[i] = pOld;
    }
  }
  for (size_t i = 0; i < taosArrayGetSize(pRegion->pOps); ++i) {
    if (pOld == taosArrayGetP(pRegion->pOps, i)) {
      pCxt->result.pJoins[i] = (SJoinLogicNode*)pCopy;
    }
  }
  for (int32_t i = 0; i < LIST_LENGTH(pOld->pChildren); ++i) {
    PLAN_ERR_RET(joinBuildMap(pCxt,
        (const SLogicNode*)nodesListGetNode(pOld->pChildren, i),
        (SLogicNode*)nodesListGetNode(pCopy->pChildren, i), pCopy));
  }
  return TSDB_CODE_SUCCESS;
}

static void joinBuildReset(SJoinLogicNode* pJoin) {
  SNode** fields[] = {&pJoin->pColEqCond, &pJoin->pColOnCond,
      &pJoin->pTagEqCond, &pJoin->pTagOnCond, &pJoin->pLeftOnCond,
      &pJoin->pRightOnCond};
  for (size_t i = 0; i < sizeof(fields) / sizeof(fields[0]); ++i) {
    nodesDestroyNode(*fields[i]);
    *fields[i] = NULL;
  }
  nodesDestroyList(pJoin->pLeftEqNodes);
  nodesDestroyList(pJoin->pRightEqNodes);
  pJoin->pLeftEqNodes = NULL;
  pJoin->pRightEqNodes = NULL;
  pJoin->joinAlgo = JOIN_ALGO_UNKNOWN;
  pJoin->allEqTags = false;
  pJoin->noPrimKeyEqCond = false;
  pJoin->leftConstPrimGot = false;
  pJoin->rightConstPrimGot = false;
  pJoin->timeRangeTarget = 0;
  TAOS_SET_OBJ_ALIGNED(&pJoin->timeRange, TSWINDOW_INITIALIZER);
  pJoin->node.optimizedFlag = 0;
  pJoin->node.splitDone = false;
  pJoin->node.stmtRoot = false;
  pJoin->node.groupAction = GROUP_ACTION_CLEAR;
  pJoin->node.inputTsOrder = ORDER_ASC;
  pJoin->node.outputTsOrder = TSDB_ORDER_NONE;
  pJoin->node.requireDataOrder = DATA_ORDER_LEVEL_NONE;
  pJoin->node.resultDataOrder = DATA_ORDER_LEVEL_NONE;
}

static bool joinBuildInputDistribution(const SLogicNode* pNode,
                                       bool* pSingleTable) {
  if (QUERY_NODE_LOGIC_PLAN_JOIN == nodeType(pNode)) {
    *pSingleTable = ((const SJoinLogicNode*)pNode)->isSingleTableJoin;
    return true;
  }
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    int8_t type = ((const SScanLogicNode*)pNode)->tableType;
    if (TSDB_SYSTEM_TABLE == type) return false;
    *pSingleTable = TSDB_CHILD_TABLE == type || TSDB_NORMAL_TABLE == type ||
                   TSDB_VIRTUAL_CHILD_TABLE == type ||
                   TSDB_VIRTUAL_NORMAL_TABLE == type;
    return true;
  }
  if (1 == LIST_LENGTH(pNode->pChildren)) {
    return joinBuildInputDistribution(
        (const SLogicNode*)nodesListGetNode(pNode->pChildren, 0), pSingleTable);
  }
  if (QUERY_NODE_LOGIC_PLAN_PROJECT == nodeType(pNode) &&
      0 == LIST_LENGTH(pNode->pChildren)) {
    *pSingleTable = true;
    return true;
  }
  return false;
}

static bool joinBuildOriginalDistribution(const SLogicNode* pNode,
                                          bool* pSingleTable) {
  if (joinBuildInputDistribution(pNode, pSingleTable)) return true;
  // System tables have different distribution rules. A single-table JOIN
  // proves both original inputs are single without duplicating parser rules.
  for (pNode = pNode->pParent; NULL != pNode; pNode = pNode->pParent) {
    if (QUERY_NODE_LOGIC_PLAN_JOIN == nodeType(pNode) &&
        ((const SJoinLogicNode*)pNode)->isSingleTableJoin) {
      *pSingleTable = true;
      return true;
    }
  }
  return false;
}

static int32_t joinBuildStripHashPartitions(SJoinBuildContext* pCxt) {
  const SJoinParallelRegion* pRegion = pCxt->pChoice->pRegion;
  for (size_t i = 0; i < taosArrayGetSize(pRegion->pInputs); ++i) {
    if (NULL == pCxt->result.pInputs[i]) return TSDB_CODE_INVALID_PARA;
  }
  for (size_t i = 0; i < taosArrayGetSize(pRegion->pOps); ++i) {
    if (NULL == pCxt->result.pJoins[i]) return TSDB_CODE_INVALID_PARA;
  }
  for (size_t i = 0; i < taosArrayGetSize(pRegion->pInputs); ++i) {
    if (!joinBuildOriginalDistribution(pCxt->result.pInputOrigins[i],
                                       &pCxt->result.singleTableInputs[i])) {
      return TSDB_CODE_PLAN_NOT_SUPPORT_JOIN_COND;
    }
  }
  for (size_t i = 0; i < taosArrayGetSize(pRegion->pInputs); ++i) {
    SLogicNode* pInput = pCxt->result.pInputs[i];
    SLogicNode* pParent = pInput->pParent;
    if (QUERY_NODE_LOGIC_PLAN_PARTITION != nodeType(pInput) ||
        NULL == pParent || QUERY_NODE_LOGIC_PLAN_JOIN != nodeType(pParent) ||
        JOIN_ALGO_HASH != ((SJoinLogicNode*)pParent)->joinAlgo) continue;
    bool owned = false;
    for (size_t j = 0; j < taosArrayGetSize(pRegion->pOps); ++j) {
      owned |= pParent == &pCxt->result.pJoins[j]->node;
    }
    if (!owned) continue;
    // HashJoin will rebuild this implementation wrapper for the new keys.
    // A wrapper with its own semantics cannot be discarded as cached state.
    if (1 != LIST_LENGTH(pInput->pChildren) ||
        NULL != pInput->pConditions || NULL != pInput->pLimit ||
        NULL != pInput->pSlimit || NULL != pInput->pHint ||
        pInput->stmtRoot) return TSDB_CODE_PLAN_NOT_SUPPORT_JOIN_COND;
    SLogicNode* pChild =
        (SLogicNode*)nodesListGetNode(pInput->pChildren, 0);
    SListCell* pCell = pParent->pChildren->pHead;
    for (; NULL != pCell; pCell = pCell->pNext) {
      if (pCell->pNode == (SNode*)pInput) break;
    }
    if (NULL == pCell) return TSDB_CODE_PLAN_INTERNAL_ERROR;
    pCell->pNode = (SNode*)pChild;
    pChild->pParent = pParent;
    const SLogicNode* pOrigin = pCxt->result.pInputOrigins[i];
    pCxt->result.pInputOrigins[i] =
        (const SLogicNode*)nodesListGetNode(pOrigin->pChildren, 0);
    pCxt->result.pInputs[i] = pChild;
    nodesClearList(pInput->pChildren);
    pInput->pChildren = NULL;
    nodesDestroyNode((SNode*)pInput);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t joinBuildDetach(SJoinBuildContext* pCxt) {
  const SJoinParallelRegion* pRegion = pCxt->pChoice->pRegion;
  SJoinLogicNode* pRoot = pCxt->result.pJoins[0];
  pCxt->stmtRoot = pRoot->node.stmtRoot;
  pCxt->pRegionParent = pRoot->node.pParent;
  if (NULL != pCxt->pRegionParent) {
    SListCell* pCell = pCxt->pRegionParent->pChildren->pHead;
    for (; NULL != pCell; pCell = pCell->pNext) {
      if (pCell->pNode == (SNode*)pRoot) break;
    }
    if (NULL == pCell) return TSDB_CODE_PLAN_INTERNAL_ERROR;
    pCxt->pRegionCell = pCell;
    pCell->pNode = NULL;
  } else {
    pCxt->result.pRoot = NULL;
  }
  pCxt->pTargets = pRoot->node.pTargets;
  pRoot->node.pTargets = NULL;
  if (JOIN_REGION_MIXED == pRegion->mode) {
    pCxt->pRootFilter = pRoot->node.pConditions;
    pRoot->node.pConditions = NULL;
  }
  for (size_t i = 0; i < taosArrayGetSize(pRegion->pOps); ++i) {
    SJoinLogicNode* pJoin = pCxt->result.pJoins[i];
    nodesClearList(pJoin->node.pChildren);
    pJoin->node.pChildren = NULL;
    pJoin->node.pParent = NULL;
    nodesDestroyList(pJoin->node.pTargets);
    pJoin->node.pTargets = NULL;
    joinBuildReset(pJoin);
  }
  for (size_t i = 0; i < taosArrayGetSize(pRegion->pInputs); ++i) {
    pCxt->result.pInputs[i]->pParent = NULL;
  }
  pCxt->detached = true;
  return TSDB_CODE_SUCCESS;
}

static void joinBuildDestroy(SJoinBuildContext* pCxt) {
  nodesDestroyList(pCxt->pTargets);
  nodesDestroyNode(pCxt->pRootFilter);
  if (!pCxt->detached) {
    nodesDestroyNode((SNode*)pCxt->result.pRoot);
    return;
  }
  // Collect roots before freeing any node: attached descendants are also in
  // these arrays, and reading their parent after freeing a root is unsafe.
  SNode* roots[2 * JOIN_PARALLEL_MAX_INPUTS] = {0};
  int32_t count = 0;
  if (NULL != pCxt->result.pRoot) {
    roots[count++] = (SNode*)pCxt->result.pRoot;
  }
  const SJoinParallelRegion* pRegion = pCxt->pChoice->pRegion;
  for (size_t i = 0; i < taosArrayGetSize(pRegion->pInputs); ++i) {
    SLogicNode* pNode = pCxt->result.pInputs[i];
    if (NULL == pNode->pParent) roots[count++] = (SNode*)pNode;
  }
  for (size_t i = 0; i < taosArrayGetSize(pRegion->pOps); ++i) {
    SLogicNode* pNode = &pCxt->result.pJoins[i]->node;
    if (NULL == pNode->pParent) roots[count++] = (SNode*)pNode;
  }
  for (int32_t i = 0; i < count; ++i) nodesDestroyNode(roots[i]);
}

static int32_t joinBuildCollectPredicate(SNode** ppExpr, void* pContext) {
  SJoinBuildPredicateCollector* pCollector = pContext;
  const SJoinTreeChoice* pChoice = pCollector->pPure->pChoice;
  SJoinParallelPredicate predicate = {.pExpr = *ppExpr,
      .sourceOpId = pCollector->opId, .source = pCollector->source};
  // Resolve in the original owner's scope before refreshing the copy's ids.
  joinParallelDescribePredicate(pCollector->pCxt->pPlanCxt, pChoice->pRegion,
                                 &predicate);
  const SJoinDpCell* pRoot = planJoinTreeCell(pChoice, pChoice->rootCell);
  if (NULL == pRoot || !pRoot->valid) return TSDB_CODE_INVALID_PARA;
  if (predicate.unresolved ||
      !joinConflictSubset(predicate.inputs, pRoot->inputs)) {
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }
  SJoinBuildPredicate owned = {.pExpr = *ppExpr, .inputs = predicate.inputs,
                                .source = predicate.source,
                                .leftMember = -1, .rightMember = -1};
  if (QUERY_NODE_OPERATOR == nodeType(*ppExpr)) {
    const SOperatorNode* pOp = (const SOperatorNode*)*ppExpr;
    if (OP_TYPE_EQUAL == pOp->opType && NULL != pOp->pLeft &&
        NULL != pOp->pRight && QUERY_NODE_COLUMN == nodeType(pOp->pLeft) &&
        QUERY_NODE_COLUMN == nodeType(pOp->pRight)) {
      const SJoinLogicNode* pOwner =
          taosArrayGetP(pChoice->pRegion->pOps, pCollector->opId);
      const SJoinInputProps* pProps =
          planGetJoinInputProps(pCollector->pCxt->pPlanCxt, &pOwner->node);
      const SColumnNode* pLeftCol = (const SColumnNode*)pOp->pLeft;
      const SColumnNode* pRightCol = (const SColumnNode*)pOp->pRight;
      const SJoinOutputCol* pLeft = joinParallelResolveColumn(pProps, pLeftCol);
      const SJoinOutputCol* pRight = joinParallelResolveColumn(pProps,
                                                               pRightCol);
      if (NULL != pLeft && NULL != pRight &&
          joinEqTypesCompatible(pLeft->type, pRight->type)) {
        owned.leftMember = joinEqColumnMember(pChoice->pEqClasses, &pLeft->id);
        owned.rightMember = joinEqColumnMember(pChoice->pEqClasses,
                                               &pRight->id);
      }
    }
  }
  if (NULL == taosArrayPush(pCollector->pPure->pPredicates, &owned)) {
    return terrno;
  }
  *ppExpr = NULL;
  return TSDB_CODE_SUCCESS;
}

static int32_t joinBuildPureExpressions(SJoinBuildContext* pCxt,
                                        SJoinPureBuild* pPure,
                                        uint32_t usedOps) {
  pPure->pPredicates = taosArrayInit(8, sizeof(SJoinBuildPredicate));
  if (NULL == pPure->pPredicates) return terrno;
  SJoinBuildPredicateCollector collector = {.pCxt = pCxt, .pPure = pPure};
  size_t numOps = taosArrayGetSize(pCxt->pChoice->pRegion->pOps);
  for (size_t i = 0; i < numOps; ++i) {
    if (0 == (usedOps & (UINT32_C(1) << i))) continue;
    SJoinLogicNode* pJoin = pCxt->result.pJoins[i];
    const struct {
      SNode** ppExpr;
      EJoinPredicateSource source;
    } clauses[] = {{&pJoin->pFullOnCond, JOIN_PREDICATE_FULL_ON},
        {&pJoin->pPrimKeyEqCond, JOIN_PREDICATE_PRIMARY_ON},
        {&pJoin->node.pConditions, JOIN_PREDICATE_FILTER}};
    size_t count = sizeof(clauses) / sizeof(clauses[0]);
    if (JOIN_REGION_MIXED == pCxt->pChoice->mode) --count;
    collector.opId = (int32_t)i;
    for (size_t j = 0; j < count; ++j) {
      collector.source = clauses[j].source;
      int32_t code = joinParallelVisitConjuncts(clauses[j].ppExpr,
                                                joinBuildCollectPredicate,
                                                &collector);
      // Transferred leaves are NULL; destroy only AND shells and leftovers.
      nodesDestroyNode(*clauses[j].ppExpr);
      *clauses[j].ppExpr = NULL;
      PLAN_ERR_RET(code);
    }
  }
  if (taosArrayGetSize(pPure->pChoice->pPredicates) !=
      taosArrayGetSize(pPure->pPredicates)) {
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t joinBuildReserve(SJoinBuildContext* pCxt,
                                SJoinPureBuild* pPure, int32_t cellId,
                                bool cross) {
  const SJoinDpCell* pCell = planJoinTreeCell(pPure->pChoice, cellId);
  if (NULL == pCell || !pCell->valid) return TSDB_CODE_INVALID_PARA;
  if (pCell->inputId >= 0) return TSDB_CODE_SUCCESS;
  if (pCell->isCross == cross) {
    size_t numOps = taosArrayGetSize(pCxt->pChoice->pRegion->pOps);
    size_t i = 0;
    for (; i < numOps; ++i) {
      if (0 == (pPure->available & (UINT32_C(1) << i))) continue;
      if (!cross || pCxt->result.pJoins[i]->isCrossJoin) break;
    }
    if (i == numOps) return TSDB_CODE_PLAN_INTERNAL_ERROR;
    pPure->available &= ~(UINT32_C(1) << i);
    pCxt->usedOps |= UINT32_C(1) << i;
    pPure->pNodes[cellId] = pCxt->result.pJoins[i];
  }
  PLAN_ERR_RET(joinBuildReserve(pCxt, pPure, pCell->left, cross));
  return joinBuildReserve(pCxt, pPure, pCell->right, cross);
}

static bool joinBuildSingleTable(const SJoinBuildContext* pCxt,
                                 const SLogicNode* pNode) {
  size_t count = taosArrayGetSize(pCxt->pChoice->pRegion->pInputs);
  for (size_t i = 0; i < count; ++i) {
    if (pNode == pCxt->result.pInputs[i]) {
      return pCxt->result.singleTableInputs[i];
    }
  }
  return ((const SJoinLogicNode*)pNode)->isSingleTableJoin;
}

static int32_t joinBuildConnect(SJoinBuildContext* pCxt,
                                SJoinLogicNode* pJoin, SLogicNode* pLeft,
                                SLogicNode* pRight) {
  PLAN_ERR_RET(nodesListMakeAppend(&pJoin->node.pChildren, (SNode*)pLeft));
  pLeft->pParent = &pJoin->node;
  PLAN_ERR_RET(nodesListMakeAppend(&pJoin->node.pChildren, (SNode*)pRight));
  pRight->pParent = &pJoin->node;
  pJoin->isSingleTableJoin = joinBuildSingleTable(pCxt, pLeft) &&
                             joinBuildSingleTable(pCxt, pRight);
  pJoin->hasSubQuery = QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pLeft) ||
                       QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pRight);
  pJoin->leftNoOrderedSubQuery =
      QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pLeft) &&
      QUERY_NODE_LOGIC_PLAN_JOIN != nodeType(pLeft) &&
      TSDB_ORDER_NONE == pLeft->outputTsOrder;
  pJoin->rightNoOrderedSubQuery =
      QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pRight) &&
      QUERY_NODE_LOGIC_PLAN_JOIN != nodeType(pRight) &&
      TSDB_ORDER_NONE == pRight->outputTsOrder;
  pJoin->node.precision = TMIN(pLeft->precision, pRight->precision);
  SNode* pChild = NULL;
  FOREACH(pChild, pJoin->node.pChildren) {
    SNode* pTarget = NULL;
    FOREACH(pTarget, ((SLogicNode*)pChild)->pTargets) {
      SNode* pCopy = NULL;
      PLAN_ERR_RET(nodesCloneNode(pTarget, &pCopy));
      int32_t code = nodesListMakeAppend(&pJoin->node.pTargets, pCopy);
      if (TSDB_CODE_SUCCESS != code) {
        nodesDestroyNode(pCopy);
        return code;
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t joinBuildCells(SJoinBuildContext* pCxt,
                              const SJoinTreeChoice* pChoice,
                              SJoinPureBuild* pPure, int32_t cellId,
                              SLogicNode** ppRoot);

static int32_t joinBuildPredicateCell(const SJoinTreeChoice* pChoice,
                                      int32_t cellId,
                                      SJoinInputSet inputs) {
  const SJoinDpCell* pCell = planJoinTreeCell(pChoice, cellId);
  if (0 == inputs) return cellId;
  int32_t children[] = {pCell->left, pCell->right};
  for (size_t i = 0; i < sizeof(children) / sizeof(children[0]); ++i) {
    const SJoinDpCell* pChild = planJoinTreeCell(pChoice, children[i]);
    if (pChild->inputId < 0 &&
        joinConflictSubset(inputs, pChild->inputs)) {
      return joinBuildPredicateCell(pChoice, children[i], inputs);
    }
  }
  // Keep single-input predicates above opaque inputs; PDC decides whether
  // they may descend past a Project, LIMIT, or another evaluation boundary.
  return cellId;
}

static const SJoinOutputCol* joinBuildClassColumn(
    const SJoinTreeChoice* pChoice, int32_t classId, SJoinInputSet inputs) {
  const SJoinParallelRegion* pRegion = pChoice->pRegion;
  for (size_t i = 0; i < taosArrayGetSize(pRegion->pInputs); ++i) {
    if (!planJoinInputSetContains(inputs, (int32_t)i)) continue;
    const SJoinParallelInput* pInput = taosArrayGet(pRegion->pInputs, i);
    const SArray* pColumns = pInput->pProps->pOutputCols;
    for (size_t j = 0; j < taosArrayGetSize(pColumns); ++j) {
      const SJoinOutputCol* pCol = taosArrayGet(pColumns, j);
      if (classId == planJoinEqClassForColumn(pChoice->pEqClasses, &pCol->id)) {
        return pCol;
      }
    }
  }
  return NULL;
}

static int32_t joinBuildDerivedEquality(SJoinBuildContext* pCxt,
                                        SJoinLogicNode* pJoin,
                                        const SJoinOutputCol* pLeft,
                                        const SJoinOutputCol* pRight) {
  SOperatorNode* pEq = NULL;
  PLAN_ERR_RET(nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pEq));
  pEq->opType = OP_TYPE_EQUAL;
  pEq->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pEq->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
  pEq->charsetCxt = pCxt->pPlanCxt->charsetCxt;
  int32_t code = nodesCloneNode(pLeft->pExpr, &pEq->pLeft);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesCloneNode(pRight->pExpr, &pEq->pRight);
  }
  SNode* pExpr = (SNode*)pEq;
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesMergeNode(&pJoin->pFullOnCond, &pExpr);
  }
  nodesDestroyNode(pExpr);
  return code;
}

static int32_t joinBuildPureCellPredicates(SJoinBuildContext* pCxt,
                                           SJoinPureBuild* pPure,
                                           int32_t cellId,
                                           SJoinEqClasses* pProof) {
  const SJoinTreeChoice* pChoice = pPure->pChoice;
  const SJoinDpCell* pCell = planJoinTreeCell(pChoice, cellId);
  if (pCell->inputId >= 0) return TSDB_CODE_SUCCESS;
  // Ancestor equalities are not yet true in a child. Previously visited
  // sibling subtrees have disjoint column identities, so one proof suffices.
  PLAN_ERR_RET(joinBuildPureCellPredicates(pCxt, pPure, pCell->left, pProof));
  PLAN_ERR_RET(joinBuildPureCellPredicates(pCxt, pPure, pCell->right, pProof));
  SJoinLogicNode* pJoin = pPure->pNodes[cellId];
  SJoinInputSet left = planJoinTreeCell(pChoice, pCell->left)->inputs;
  SJoinInputSet right = planJoinTreeCell(pChoice, pCell->right)->inputs;
  const SArray* pMembers = pChoice->pEqClasses->pMembers;
  for (size_t j = 0; j < taosArrayGetSize(pMembers); ++j) {
    const SJoinEqMember* pMember = taosArrayGet(pMembers, j);
    if (pMember->parent != (int32_t)j || 0 == (pMember->inputs & left) ||
        0 == (pMember->inputs & right)) continue;
    const SJoinOutputCol* a =
        joinBuildClassColumn(pChoice, (int32_t)j, left);
    const SJoinOutputCol* b =
        joinBuildClassColumn(pChoice, (int32_t)j, right);
    if (NULL == a || NULL == b) return TSDB_CODE_PLAN_INTERNAL_ERROR;
    PLAN_ERR_RET(joinBuildDerivedEquality(pCxt, pJoin, a, b));
    int32_t aMember = joinEqColumnMember(pChoice->pEqClasses, &a->id);
    int32_t bMember = joinEqColumnMember(pChoice->pEqClasses, &b->id);
    (void)joinEqUnionMembers(pProof, aMember, bMember);
  }
  for (size_t i = 0; i < taosArrayGetSize(pPure->pPredicates); ++i) {
    SJoinBuildPredicate* pPred = taosArrayGet(pPure->pPredicates, i);
    if (pPred->cellId != cellId) continue;
    // A path between distinct columns proves equality and non-NULL values.
    // The zero-length path for x=x does not prove its NULL filter redundant.
    if (pPred->leftMember >= 0 && pPred->rightMember >= 0 &&
        pPred->leftMember != pPred->rightMember &&
        !joinEqUnionMembers(pProof, pPred->leftMember, pPred->rightMember)) {
      nodesDestroyNode(pPred->pExpr);
      pPred->pExpr = NULL;
      continue;
    }
    SNode** ppCondition = JOIN_PREDICATE_FILTER == pPred->source
                             ? &pJoin->node.pConditions : &pJoin->pFullOnCond;
    PLAN_ERR_RET(nodesMergeNode(ppCondition, &pPred->pExpr));
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t joinBuildPurePredicates(SJoinBuildContext* pCxt,
                                       SJoinPureBuild* pPure) {
  size_t count = taosArrayGetSize(pPure->pChoice->pEqClasses->pMembers);
  SJoinEqClasses proof = {
      .pMembers = taosArrayInit(count, sizeof(SJoinEqMember))};
  if (NULL == proof.pMembers) return terrno;
  int32_t code = TSDB_CODE_SUCCESS;
  // Global classes justify derivation, but are not local deletion proofs.
  for (size_t i = 0; i < count; ++i) {
    SJoinEqMember member = {.parent = (int32_t)i};
    if (NULL == taosArrayPush(proof.pMembers, &member)) {
      code = terrno;
      goto _end;
    }
  }
  for (size_t i = 0; i < taosArrayGetSize(pPure->pPredicates); ++i) {
    SJoinBuildPredicate* pPred = taosArrayGet(pPure->pPredicates, i);
    pPred->cellId = joinBuildPredicateCell(pPure->pChoice,
                                           pPure->pChoice->rootCell,
                                           pPred->inputs);
  }
  code = joinBuildPureCellPredicates(pCxt, pPure, pPure->pChoice->rootCell,
                                     &proof);
_end:
  taosArrayDestroy(proof.pMembers);
  return code;
}

static int32_t joinBuildPure(SJoinBuildContext* pCxt,
                             const SJoinTreeChoice* pChoice,
                             uint32_t usedOps, SLogicNode** ppRoot) {
  if (0 != (pCxt->usedOps & usedOps)) return TSDB_CODE_PLAN_INTERNAL_ERROR;
  SJoinPureBuild pure = {.pChoice = pChoice, .available = usedOps};
  int32_t code = joinBuildPureExpressions(pCxt, &pure, usedOps);
  if (TSDB_CODE_SUCCESS == code) {
    code = joinBuildReserve(pCxt, &pure, pChoice->rootCell, true);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = joinBuildReserve(pCxt, &pure, pChoice->rootCell, false);
  }
  if (TSDB_CODE_SUCCESS == code && 0 != pure.available) {
    code = TSDB_CODE_PLAN_INTERNAL_ERROR;
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = joinBuildCells(pCxt, pChoice, &pure, pChoice->rootCell, ppRoot);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = joinBuildPurePredicates(pCxt, &pure);
  }
  for (size_t i = 0; i < taosArrayGetSize(pure.pPredicates); ++i) {
    SJoinBuildPredicate* pPred = taosArrayGet(pure.pPredicates, i);
    nodesDestroyNode(pPred->pExpr);
  }
  taosArrayDestroy(pure.pPredicates);
  return code;
}

static int32_t joinBuildCells(SJoinBuildContext* pCxt,
                              const SJoinTreeChoice* pChoice,
                              SJoinPureBuild* pPure, int32_t cellId,
                              SLogicNode** ppRoot) {
  const SJoinDpCell* pCell = planJoinTreeCell(pChoice, cellId);
  if (NULL == pCell || !pCell->valid) return TSDB_CODE_INVALID_PARA;
  if (NULL != pCell->pPureChoice) {
    return joinBuildPure(pCxt, pCell->pPureChoice, pCell->usedOps, ppRoot);
  }
  if (pCell->inputId >= 0) {
    if (pCell->inputId >= JOIN_PARALLEL_MAX_INPUTS ||
        0 != (pCxt->usedInputs & pCell->inputs)) {
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }
    pCxt->usedInputs |= pCell->inputs;
    *ppRoot = pCxt->result.pInputs[pCell->inputId];
    return TSDB_CODE_SUCCESS;
  }
  SJoinLogicNode* pJoin = NULL;
  if (NULL != pPure) {
    pJoin = pPure->pNodes[cellId];
    pJoin->joinType = JOIN_TYPE_INNER;
    pJoin->subType = JOIN_STYPE_NONE;
    pJoin->isCrossJoin = pCell->isCross;
  } else {
    uint32_t bit = UINT32_C(1) << pCell->opId;
    if (0 != (pCxt->usedOps & bit)) return TSDB_CODE_PLAN_INTERNAL_ERROR;
    pCxt->usedOps |= bit;
    pJoin = pCxt->result.pJoins[pCell->opId];
    if (pCell->swapped && JOIN_TYPE_LEFT == pJoin->joinType) {
      pJoin->joinType = JOIN_TYPE_RIGHT;
    } else if (pCell->swapped && JOIN_TYPE_RIGHT == pJoin->joinType) {
      pJoin->joinType = JOIN_TYPE_LEFT;
    }
  }
  SLogicNode* pLeft = NULL;
  SLogicNode* pRight = NULL;
  PLAN_ERR_RET(joinBuildCells(pCxt, pChoice, pPure, pCell->left, &pLeft));
  PLAN_ERR_RET(joinBuildCells(pCxt, pChoice, pPure, pCell->right, &pRight));
  PLAN_ERR_RET(joinBuildConnect(pCxt, pJoin, pLeft, pRight));
  if (NULL != pJoin->pPrimKeyEqCond) {
    PLAN_ERR_RET(nodesMergeNode(&pJoin->pFullOnCond, &pJoin->pPrimKeyEqCond));
  }
  if (NULL != pJoin->addPrimEqCond) {
    PLAN_ERR_RET(nodesMergeNode(&pJoin->pFullOnCond, &pJoin->addPrimEqCond));
  }
  *ppRoot = &pJoin->node;
  return TSDB_CODE_SUCCESS;
}

static int32_t joinBuildTree(const SPlanContext* pCxt,
                             const SLogicNode* pRoot,
                             const SJoinTreeChoice* pChoice,
                             SJoinBuildResult* pResult) {
  SJoinBuildContext build = {.pPlanCxt = pCxt, .pChoice = pChoice};
  int32_t code = nodesCloneNode((SNode*)pRoot,
                                 (SNode**)&build.result.pRoot);
  if (TSDB_CODE_SUCCESS == code) {
    code = joinBuildMap(&build, pRoot, build.result.pRoot, NULL);
  }
  if (TSDB_CODE_SUCCESS == code) code = joinBuildStripHashPartitions(&build);
  if (TSDB_CODE_SUCCESS == code) code = joinBuildDetach(&build);
  SLogicNode* pRegionRoot = NULL;
  size_t count = taosArrayGetSize(pChoice->pRegion->pInputs);
  uint32_t allOps = (UINT32_C(1) << (count - 1)) - 1;
  if (TSDB_CODE_SUCCESS == code) {
    code = JOIN_REGION_PURE_INNER == pChoice->mode
               ? joinBuildPure(&build, pChoice, allOps, &pRegionRoot)
               : joinBuildCells(&build, pChoice, NULL, pChoice->rootCell,
                                  &pRegionRoot);
  }
  if (TSDB_CODE_SUCCESS == code &&
      (build.usedOps != allOps ||
       build.usedInputs != (UINT64_C(1) << count) - 1)) {
    code = TSDB_CODE_PLAN_INTERNAL_ERROR;
  }
  if (TSDB_CODE_SUCCESS == code && NULL != build.pRootFilter) {
    code = nodesMergeNode(&pRegionRoot->pConditions, &build.pRootFilter);
  }
  if (TSDB_CODE_SUCCESS == code) {
    nodesDestroyList(pRegionRoot->pTargets);
    pRegionRoot->pTargets = build.pTargets;
    build.pTargets = NULL;
    pRegionRoot->stmtRoot = build.stmtRoot;
    pRegionRoot->pParent = build.pRegionParent;
    if (NULL != build.pRegionCell) {
      build.pRegionCell->pNode = (SNode*)pRegionRoot;
    } else {
      build.result.pRoot = pRegionRoot;
    }
    build.result.pRegionRoot = pRegionRoot;
    for (size_t i = 0; i < taosArrayGetSize(pChoice->pRegion->pOps); ++i) {
      SJoinLogicNode* pJoin = build.result.pJoins[i];
      pJoin->isLowLevelJoin = NULL != pJoin->node.pParent &&
          QUERY_NODE_LOGIC_PLAN_JOIN == nodeType(pJoin->node.pParent);
    }
    planResetJoinRewriteFlags(pRegionRoot);
    *pResult = build.result;
    return TSDB_CODE_SUCCESS;
  }
  joinBuildDestroy(&build);
  return code;
}

int32_t planBuildJoinTree(const SPlanContext* pCxt, const SLogicNode* pRoot,
                           const SJoinTreeChoice* pChoice,
                           SLogicNode** ppRoot) {
  if (NULL == ppRoot) return TSDB_CODE_INVALID_PARA;
  *ppRoot = NULL;
  if (NULL == pCxt || NULL == pRoot || NULL == pChoice ||
      NULL == pChoice->pRegion || !pChoice->pRegion->supported) {
    return TSDB_CODE_INVALID_PARA;
  }
  SJoinBuildResult result = {0};
  int32_t code = joinBuildTree(pCxt, pRoot, pChoice, &result);
  if (TSDB_CODE_SUCCESS == code) *ppRoot = result.pRoot;
  return code;
}

typedef struct SJoinBuildBinding {
  const SPlanContext* pCxt;
  const SLogicNode* pJoin;
  bool valid;
} SJoinBuildBinding;

static EDealRes joinBuildCheckColumn(SNode* pNode, void* pContext) {
  SJoinBuildBinding* pBinding = pContext;
  if (QUERY_NODE_COLUMN != nodeType(pNode)) return DEAL_RES_CONTINUE;
  const SColumnNode* pColumn = (const SColumnNode*)pNode;
  SNode* pChild = NULL;
  FOREACH(pChild, pBinding->pJoin->pChildren) {
    const SJoinInputProps* pProps =
        planGetJoinInputProps(pBinding->pCxt, (SLogicNode*)pChild);
    if (NULL == joinParallelResolveColumn(pProps, pColumn)) continue;
    SNode* pTarget = NULL;
    FOREACH(pTarget, ((SLogicNode*)pChild)->pTargets) {
      if (QUERY_NODE_COLUMN != nodeType(pTarget)) continue;
      const SColumnNode* pOutput = (const SColumnNode*)pTarget;
      if (0 == strcmp(pColumn->tableAlias, pOutput->tableAlias) &&
          0 == strcmp(pColumn->colName, pOutput->colName)) {
        return DEAL_RES_CONTINUE;
      }
    }
  }
  pBinding->valid = false;
  return DEAL_RES_END;
}

static int32_t joinBuildCheckBindings(const SPlanContext* pCxt,
                                      SLogicNode* pRoot) {
  if (QUERY_NODE_LOGIC_PLAN_JOIN == nodeType(pRoot)) {
    SJoinBuildBinding binding = {.pCxt = pCxt, .pJoin = pRoot, .valid = true};
    planWalkLogicExprFields(pRoot, joinBuildCheckColumn, &binding);
    if (!binding.valid) return TSDB_CODE_PLAN_SLOT_NOT_FOUND;
  }
  SNode* pChild = NULL;
  FOREACH(pChild, pRoot->pChildren) {
    if (((SLogicNode*)pChild)->pParent != pRoot) {
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }
    PLAN_ERR_RET(joinBuildCheckBindings(pCxt, (SLogicNode*)pChild));
  }
  return TSDB_CODE_SUCCESS;
}

static bool joinBuildEqualExpr(const SNode* pLeft, const SNode* pRight) {
  if (NULL == pLeft || NULL == pRight) return pLeft == pRight;
  return nodesEqualNode(pLeft, pRight);
}

static bool joinBuildEqualOptionValue(const SNode* pLeft,
                                       const SNode* pRight) {
  if (NULL == pLeft || NULL == pRight) return pLeft == pRight;
  if (QUERY_NODE_VALUE != nodeType(pLeft) ||
      QUERY_NODE_VALUE != nodeType(pRight)) return false;
  const SValueNode* a = (const SValueNode*)pLeft;
  const SValueNode* b = (const SValueNode*)pRight;
  return nodesEqualNode(pLeft, pRight) && a->datum.i == b->datum.i &&
         a->unit == b->unit && a->flag == b->flag &&
         a->translate == b->translate && a->isNull == b->isNull &&
         a->placeholderNo == b->placeholderNo;
}

static bool joinBuildEqualTemporalOption(const SNode* pLeft,
                                          const SNode* pRight) {
  if (NULL == pLeft || NULL == pRight) return pLeft == pRight;
  if (nodeType(pLeft) != nodeType(pRight)) return false;
  // The generic equality API does not compare these option wrappers.
  // Check their integer payloads too: execution reads datum, not typeData.
  if (QUERY_NODE_WINDOW_OFFSET == nodeType(pLeft)) {
    const SWindowOffsetNode* a = (const SWindowOffsetNode*)pLeft;
    const SWindowOffsetNode* b = (const SWindowOffsetNode*)pRight;
    return joinBuildEqualOptionValue(a->pStartOffset, b->pStartOffset) &&
           joinBuildEqualOptionValue(a->pEndOffset, b->pEndOffset);
  }
  if (QUERY_NODE_LIMIT == nodeType(pLeft)) {
    const SLimitNode* a = (const SLimitNode*)pLeft;
    const SLimitNode* b = (const SLimitNode*)pRight;
    return joinBuildEqualOptionValue((SNode*)a->limit, (SNode*)b->limit) &&
           joinBuildEqualOptionValue((SNode*)a->offset, (SNode*)b->offset);
  }
  return false;
}

static bool joinBuildTemporalMatches(const SLogicNode* pOld,
                                     const SLogicNode* pNew,
                                     const SLogicNode* pChanged) {
  if (pOld == pChanged) return true;
  if (nodeType(pOld) != nodeType(pNew) ||
      LIST_LENGTH(pOld->pChildren) != LIST_LENGTH(pNew->pChildren)) {
    return false;
  }
  if (QUERY_NODE_LOGIC_PLAN_JOIN == nodeType(pOld)) {
    const SJoinLogicNode* a = (const SJoinLogicNode*)pOld;
    const SJoinLogicNode* b = (const SJoinLogicNode*)pNew;
    if (IS_ASOF_JOIN(a->subType) || IS_WINDOW_JOIN(a->subType)) {
      if (a->joinType != b->joinType || a->subType != b->subType ||
          !joinBuildEqualExpr(a->addPrimEqCond, b->addPrimEqCond) ||
          !joinBuildEqualExpr(a->pPrimKeyEqCond, b->pPrimKeyEqCond) ||
          !joinBuildEqualTemporalOption(a->pWindowOffset, b->pWindowOffset) ||
          !joinBuildEqualTemporalOption(a->pJLimit, b->pJLimit)) return false;
    }
  }
  for (int32_t i = 0; i < LIST_LENGTH(pOld->pChildren); ++i) {
    if (!joinBuildTemporalMatches(
            (const SLogicNode*)nodesListGetNode(pOld->pChildren, i),
            (const SLogicNode*)nodesListGetNode(pNew->pChildren, i),
            pChanged)) return false;
  }
  return true;
}

static int32_t joinBuildFinalHeight(const SJoinBuildResult* pResult,
                                    const SJoinParallelRegion* pRegion,
                                    const SLogicNode* pNode,
                                    SJoinInputSet* pInputs,
                                    uint32_t* pOps) {
  for (size_t i = 0; i < taosArrayGetSize(pRegion->pInputs); ++i) {
    if (pNode != pResult->pInputs[i]) continue;
    SJoinInputSet bit = UINT64_C(1) << i;
    if (0 != (*pInputs & bit)) return -1;
    *pInputs |= bit;
    return 0;
  }
  size_t count = taosArrayGetSize(pRegion->pOps);
  size_t i = 0;
  for (; i < count; ++i) {
    if (pNode == &pResult->pJoins[i]->node) break;
  }
  if (i == count || 0 != (*pOps & (UINT32_C(1) << i)) ||
      2 != LIST_LENGTH(pNode->pChildren)) return -1;
  *pOps |= UINT32_C(1) << i;
  int32_t left = joinBuildFinalHeight(pResult, pRegion,
      (const SLogicNode*)nodesListGetNode(pNode->pChildren, 0), pInputs, pOps);
  int32_t right = joinBuildFinalHeight(pResult, pRegion,
      (const SLogicNode*)nodesListGetNode(pNode->pChildren, 1), pInputs, pOps);
  return left < 0 || right < 0 ? -1 : 1 + TMAX(left, right);
}

static void joinBuildRefreshOrder(const SJoinBuildResult* pResult,
                                  const SJoinParallelRegion* pRegion,
                                  SLogicNode* pNode) {
  bool rewritten = false;
  for (size_t i = 0; i < taosArrayGetSize(pRegion->pOps); ++i) {
    rewritten |= pNode == &pResult->pJoins[i]->node;
  }
  if (!rewritten) return;
  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    joinBuildRefreshOrder(pResult, pRegion, (SLogicNode*)pChild);
  }
  planRefreshJoinRewriteOrder((SJoinLogicNode*)pNode);
}

static int32_t joinBuildValidate(SPlanContext* pCxt,
                                 const SLogicNode* pOriginal,
                                 const SJoinTreeChoice* pChoice,
                                 SJoinBuildResult* pResult,
                                 bool* pSupported) {
  *pSupported = false;
  PLAN_ERR_RET(planRefreshJoinRewriteInputs(pCxt, pResult->pRoot));
  bool executable = false;
  PLAN_ERR_RET(planCheckJoinRewriteConditions(pCxt, pResult->pRoot,
                                              &executable));
  if (!executable) return TSDB_CODE_SUCCESS;
  joinBuildRefreshOrder(pResult, pChoice->pRegion, pResult->pRegionRoot);
  PLAN_ERR_RET(planRefreshJoinRewriteInputs(pCxt, pResult->pRoot));
  PLAN_ERR_RET(joinBuildCheckBindings(pCxt, pResult->pRoot));
  // PDC may expose extra execution columns. Reject the candidate until these
  // can be distinguished from the root's visible output columns.
  if (LIST_LENGTH(pOriginal->pTargets) !=
      LIST_LENGTH(pResult->pRoot->pTargets)) {
    return TSDB_CODE_SUCCESS;
  }
  for (int32_t i = 0; i < LIST_LENGTH(pOriginal->pTargets); ++i) {
    if (!nodesEqualNode(nodesListGetNode(pOriginal->pTargets, i),
                        nodesListGetNode(pResult->pRoot->pTargets, i))) {
      return TSDB_CODE_SUCCESS;
    }
  }
  const SJoinParallelRegion* pRegion = pChoice->pRegion;
  if (!joinBuildTemporalMatches(pOriginal, pResult->pRoot,
                                 &pRegion->pRoot->node)) {
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }
  for (size_t i = 0; i < taosArrayGetSize(pRegion->pInputs); ++i) {
    if (!joinBuildTemporalMatches(pResult->pInputOrigins[i],
                                   pResult->pInputs[i], NULL)) {
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }
  }
  SJoinInputSet inputs = 0;
  uint32_t ops = 0;
  int32_t height = joinBuildFinalHeight(pResult, pRegion,
                                        pResult->pRegionRoot, &inputs, &ops);
  size_t count = taosArrayGetSize(pRegion->pInputs);
  if (height < 0 || inputs != (UINT64_C(1) << count) - 1 ||
      ops != (UINT32_C(1) << (count - 1)) - 1) {
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }
  *pSupported = height < pChoice->originalHeight;
  return TSDB_CODE_SUCCESS;
}

// Detect runtime dependencies and functions unsafe to move across JOINs.
static EDealRes joinParallelExprTraits(SNode* pNode, void* pContext) {
  SJoinExprTraits* pTraits = pContext;
  if (planExprIsRuntimeDependent(pNode)) {
    pTraits->runtimeDependent = true;
    return DEAL_RES_END;
  }
  switch (nodeType(pNode)) {
    case QUERY_NODE_FUNCTION: {
      const SFunctionNode* pFunc = (const SFunctionNode*)pNode;
      // Moving these calls may change their evaluation count or context.
      pTraits->unsafe |= !fmIsScalarFunc(pFunc->funcId) ||
                         fmIsUserDefinedFunc(pFunc->funcId) ||
                         fmIsVolatileFunc(pFunc->funcId) ||
                         fmIsNoPushdownFunc(pFunc->funcId);
      break;
    }
    default:
      break;
  }
  return DEAL_RES_CONTINUE;
}

// Summarize dynamic-operator and expression restrictions for one node.
static SJoinExprTraits joinParallelNodeTraits(const SLogicNode* pNode) {
  SJoinExprTraits traits = {
      .runtimeDependent =
          pNode->dynamicOp ||
          QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL == nodeType(pNode),
  };
  planWalkLogicExprFields((SLogicNode*)pNode, joinParallelExprTraits, &traits);
  return traits;
}

static bool joinParallelRuntimeTree(const SLogicNode* pNode) {
  if (joinParallelNodeTraits(pNode).runtimeDependent) return true;
  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    if (joinParallelRuntimeTree((const SLogicNode*)pChild)) return true;
  }
  return false;
}

static EDealRes joinParallelSkipTargets(SNode* pNode, void* pContext) {
  SJoinUseWalker* pUse = pContext;
  SNode*          pTarget = NULL;
  FOREACH(pTarget, pUse->pTargets) {
    if (pTarget == pNode) return DEAL_RES_IGNORE_CHILD;
  }
  return pUse->walker(pNode, pUse->pContext);
}

static void joinParallelWalkUses(const SLogicNode* pNode, FNodeWalker walker,
                                 void* pContext) {
  SJoinUseWalker use = {
      .pTargets = pNode->pTargets, .walker = walker, .pContext = pContext};
  // Intermediate targets are transport columns, not consumers of those values.
  planWalkLogicExprFields((SLogicNode*)pNode, joinParallelSkipTargets, &use);
}

static bool joinParallelUsesMatching(const SJoinMatchingUse* pUse,
                                     const SLogicNode*       pNode,
                                     const SColumnNode*      pColumn,
                                     bool*                   pResolved) {
  const SJoinInputProps* pProps = planGetJoinInputProps(pUse->pPlanCxt, pNode);
  const SJoinOutputCol*  pOutput = joinParallelResolveColumn(pProps, pColumn);
  if (NULL == pOutput) return false;
  *pResolved = true;
  return NULL != tSimpleHashGet(pUse->pMatching->pColIds, &pOutput->id,
                                sizeof(pOutput->id));
}

static EDealRes joinParallelMatchingUse(SNode* pNode, void* pContext) {
  SJoinMatchingUse* pUse = pContext;
  if (pUse->used) return DEAL_RES_END;
  if (QUERY_NODE_COLUMN != nodeType(pNode)) return DEAL_RES_CONTINUE;
  const SColumnNode* pColumn = (const SColumnNode*)pNode;
  bool               resolved = false;
  SNode*             pChild = NULL;
  FOREACH(pChild, pUse->pOwner->pChildren) {
    if (joinParallelUsesMatching(pUse, (const SLogicNode*)pChild, pColumn,
                                 &resolved)) {
      pUse->used = true;
      return DEAL_RES_END;
    }
  }
  // A column of unknown origin may come from the matching side.
  pUse->used =
      joinParallelUsesMatching(pUse, pUse->pOwner, pColumn, &resolved) ||
      !resolved;
  return pUse->used ? DEAL_RES_END : DEAL_RES_CONTINUE;
}

static bool joinParallelMatchingSideUsed(const SJoinRewriteContext* pCxt,
                                         const SJoinLogicNode*      pJoin) {
  int32_t           side = JOIN_TYPE_RIGHT == pJoin->joinType ? 0 : 1;
  const SLogicNode* pMatching =
      (const SLogicNode*)nodesListGetNode(pJoin->node.pChildren, side);
  SJoinMatchingUse use = {
      .pPlanCxt = pCxt->pPlanCxt,
      .pOwner = &pJoin->node,
      .pMatching = planGetJoinInputProps(pCxt->pPlanCxt, pMatching),
  };
  if (NULL == use.pMatching) return true;
  nodesWalkExpr(pJoin->node.pConditions, joinParallelMatchingUse, &use);
  const SLogicNode* pAncestor = pJoin->node.pParent;
  while (NULL != pAncestor && !use.used) {
    use.pOwner = pAncestor;
    joinParallelWalkUses(pAncestor, joinParallelMatchingUse, &use);
    pAncestor = pAncestor->pParent;
  }
  if (!use.used) {
    use.pOwner = pCxt->pRoot;
    nodesWalkExprs(pCxt->pRoot->pTargets, joinParallelMatchingUse, &use);
  }
  return use.used;
}

static bool joinParallelRestrictedHint(const SLogicNode* pNode) {
  SNode* pHintNode = NULL;
  FOREACH(pHintNode, pNode->pHint) {
    const SHintNode* pHint = (const SHintNode*)pHintNode;
    if (HINT_HASH_JOIN != pHint->option && HINT_MERGE_JOIN != pHint->option &&
        HINT_JOIN_ALGO != pHint->option) {
      return true;
    }
  }
  return false;
}

static bool joinParallelStableJoin(const SJoinLogicNode* pJoin) {
  return pJoin->grpJoin || pJoin->multiGroupJoin || pJoin->seqWinGroup ||
         planJoinMayBeStableJoin(pJoin);
}

static bool joinParallelCanFlatten(const SJoinRewriteContext* pCxt,
                                   const SLogicNode*          pNode,
                                   SJoinExprTraits            traits) {
  if (QUERY_NODE_LOGIC_PLAN_JOIN != nodeType(pNode) ||
      2 != LIST_LENGTH(pNode->pChildren) || NULL != pNode->pLimit ||
      NULL != pNode->pSlimit || joinParallelRestrictedHint(pNode)) {
    return false;
  }
  const SJoinLogicNode* pJoin = (const SJoinLogicNode*)pNode;
  if (IS_ASOF_JOIN(pJoin->subType) || IS_WINDOW_JOIN(pJoin->subType) ||
      joinParallelStableJoin(pJoin)) {
    return false;
  }
  if (traits.runtimeDependent || traits.unsafe) return false;
  if (IS_SEMI_JOIN(pJoin->subType) || JOIN_STYPE_ANTI == pJoin->subType) {
    return !joinParallelMatchingSideUsed(pCxt, pJoin);
  }
  return JOIN_STYPE_NONE == pJoin->subType ||
         JOIN_STYPE_OUTER == pJoin->subType;
}

static void joinParallelDestroyRegion(SJoinParallelRegion* pRegion) {
  if (NULL == pRegion) return;
  taosArrayDestroy(pRegion->pPredicates);
  taosArrayDestroy(pRegion->pOps);
  taosArrayDestroy(pRegion->pInputs);
  tSimpleHashCleanup(pRegion->pColumnInputs);
  taosMemoryFree(pRegion);
}

void planDestroyJoinRewriteContext(SJoinRewriteContext* pContext) {
  if (NULL == pContext) return;
  for (size_t i = 0; i < taosArrayGetSize(pContext->pRegions); ++i) {
    SJoinParallelRegion** ppRegion = taosArrayGet(pContext->pRegions, i);
    joinParallelDestroyRegion(*ppRegion);
  }
  taosArrayDestroy(pContext->pRegions);
  taosMemoryFree(pContext);
}

static SJoinParallelRegion* joinParallelCreateRegion(
    const SJoinLogicNode* pRoot) {
  SJoinParallelRegion* pRegion = taosMemoryCalloc(1, sizeof(*pRegion));
  if (NULL == pRegion) return NULL;
  pRegion->pRoot = pRoot;
  pRegion->supported = true;
  pRegion->pInputs = taosArrayInit(4, sizeof(SJoinParallelInput));
  pRegion->pOps = taosArrayInit(4, sizeof(const SJoinLogicNode*));
  pRegion->pPredicates = taosArrayInit(4, sizeof(SJoinParallelPredicate));
  pRegion->pColumnInputs =
      tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
  if (NULL == pRegion->pInputs || NULL == pRegion->pOps ||
      NULL == pRegion->pPredicates || NULL == pRegion->pColumnInputs) {
    joinParallelDestroyRegion(pRegion);
    return NULL;
  }
  return pRegion;
}

static int32_t joinParallelCollectRegions(SJoinRewriteContext* pCxt,
                                          const SLogicNode*    pNode);

static int32_t joinParallelCollectTree(SJoinRewriteContext* pCxt,
                                       SJoinParallelRegion* pRegion,
                                       const SLogicNode*    pNode,
                                       SJoinExprTraits      traits) {
  if (joinParallelCanFlatten(pCxt, pNode, traits)) {
    const SJoinLogicNode* pJoin = (const SJoinLogicNode*)pNode;
    if (NULL == taosArrayPush(pRegion->pOps, &pJoin)) return terrno;
    if (!IS_INNER_NONE_JOIN(pJoin->joinType, pJoin->subType)) {
      pRegion->mode = JOIN_REGION_MIXED;
    }
    SNode* pChild = NULL;
    FOREACH(pChild, pNode->pChildren) {
      const SLogicNode* pInput = (const SLogicNode*)pChild;
      PLAN_ERR_RET(joinParallelCollectTree(pCxt, pRegion, pInput,
                                            joinParallelNodeTraits(pInput)));
    }
    return TSDB_CODE_SUCCESS;
  }
  PLAN_ERR_RET(joinParallelCollectRegions(pCxt, pNode));
  bool runtimeDependent = joinParallelRuntimeTree(pNode);
  SJoinParallelInput input = {
      .inputId = (int32_t)taosArrayGetSize(pRegion->pInputs),
      .pNode = pNode,
      .pProps = planGetJoinInputProps(pCxt->pPlanCxt, pNode),
  };
  if (NULL == input.pProps || runtimeDependent) {
    pRegion->supported = false;
  }
  return NULL == taosArrayPush(pRegion->pInputs, &input) ? terrno : 0;
}

static int32_t joinParallelIndexInputs(SJoinParallelRegion* pRegion) {
  for (size_t i = 0; i < taosArrayGetSize(pRegion->pInputs); ++i) {
    const SJoinParallelInput* pInput = taosArrayGet(pRegion->pInputs, i);
    if (NULL == pInput->pProps) continue;
    const SArray* pColumns = pInput->pProps->pOutputCols;
    for (size_t j = 0; j < taosArrayGetSize(pColumns); ++j) {
      const SJoinOutputCol* pColumn = taosArrayGet(pColumns, j);
      int32_t existing = planJoinInputForColumn(pRegion, &pColumn->id);
      if (existing >= 0 && existing != pInput->inputId) {
        pRegion->supported = false;
        continue;
      }
      PLAN_ERR_RET(tSimpleHashPut(pRegion->pColumnInputs, &pColumn->id,
                                  sizeof(pColumn->id), &pInput->inputId,
                                  sizeof(pInput->inputId)));
    }
  }
  return TSDB_CODE_SUCCESS;
}

static EDealRes joinParallelPredicateDependency(SNode* pNode, void* pContext) {
  SJoinDependencyWalker* pWalker = pContext;
  if (QUERY_NODE_COLUMN != nodeType(pNode)) return DEAL_RES_CONTINUE;
  const SJoinOutputCol* pOutput = NULL;
  int32_t input = joinParallelColumnInput(
      pWalker->pRegion, pWalker->pProps, (const SColumnNode*)pNode, &pOutput);
  if (input < 0) {
    pWalker->pPredicate->unresolved = true;
  } else {
    pWalker->pPredicate->inputs |= UINT64_C(1) << input;
  }
  return DEAL_RES_CONTINUE;
}

static void joinParallelDescribePredicate(const SPlanContext* pCxt,
                                          const SJoinParallelRegion* pRegion,
                                          SJoinParallelPredicate* pPredicate) {
  const SJoinLogicNode* pJoin =
      taosArrayGetP(pRegion->pOps, pPredicate->sourceOpId);
  SJoinDependencyWalker  walker = {
       .pProps = planGetJoinInputProps(pCxt, &pJoin->node),
       .pRegion = pRegion,
       .pPredicate = pPredicate,
  };
  nodesWalkExpr((SNode*)pPredicate->pExpr, joinParallelPredicateDependency,
                  &walker);
}

typedef struct SJoinPredicateCollector {
  const SPlanContext* pPlanCxt;
  SJoinParallelRegion* pRegion;
  int32_t opId;
  EJoinPredicateSource source;
} SJoinPredicateCollector;

static int32_t joinParallelCollectPredicate(SNode** ppExpr, void* pContext) {
  if (NULL == *ppExpr) return TSDB_CODE_SUCCESS;
  SJoinPredicateCollector* pCollector = pContext;
  SJoinParallelRegion* pRegion = pCollector->pRegion;
  SJoinParallelPredicate predicate = {.pExpr = *ppExpr,
      .sourceOpId = pCollector->opId, .source = pCollector->source};
  joinParallelDescribePredicate(pCollector->pPlanCxt, pRegion, &predicate);
  pRegion->supported = pRegion->supported && !predicate.unresolved;
  return NULL == taosArrayPush(pRegion->pPredicates, &predicate) ? terrno : 0;
}

static int32_t joinParallelAddPredicate(const SJoinRewriteContext* pCxt,
                                        SJoinParallelRegion* pRegion,
                                        const SNode* pExpr, int32_t opId,
                                        EJoinPredicateSource source) {
  SJoinPredicateCollector collector = {.pPlanCxt = pCxt->pPlanCxt,
      .pRegion = pRegion, .opId = opId, .source = source};
  SNode* pBorrowed = (SNode*)pExpr;
  return JOIN_REGION_PURE_INNER == pRegion->mode
      ? joinParallelVisitConjuncts(&pBorrowed, joinParallelCollectPredicate,
                                     &collector)
      : joinParallelCollectPredicate(&pBorrowed, &collector);
}

static int32_t joinParallelCollectPredicates(const SJoinRewriteContext* pCxt,
                                             SJoinParallelRegion* pRegion) {
  for (size_t i = 0; i < taosArrayGetSize(pRegion->pOps); ++i) {
    const SJoinLogicNode** ppJoin = taosArrayGet(pRegion->pOps, i);
    const SJoinLogicNode*  pJoin = *ppJoin;
    // addPrimEqCond belongs to ASOF/WINDOW JOINs, which are region inputs.
    const struct {
      const SNode*         pExpr;
      EJoinPredicateSource source;
    } clauses[] = {
        {pJoin->pFullOnCond, JOIN_PREDICATE_FULL_ON},
        {pJoin->pPrimKeyEqCond, JOIN_PREDICATE_PRIMARY_ON},
        {pJoin->node.pConditions, JOIN_PREDICATE_FILTER},
    };
    for (size_t j = 0; j < sizeof(clauses) / sizeof(clauses[0]); ++j) {
      PLAN_ERR_RET(joinParallelAddPredicate(pCxt, pRegion, clauses[j].pExpr,
                                            (int32_t)i, clauses[j].source));
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t joinParallelCollectRegions(SJoinRewriteContext* pCxt,
                                          const SLogicNode*    pNode) {
  SJoinExprTraits traits = joinParallelNodeTraits(pNode);
  if (traits.runtimeDependent) return TSDB_CODE_SUCCESS;
  if (!joinParallelCanFlatten(pCxt, pNode, traits)) {
    SNode* pChild = NULL;
    FOREACH(pChild, pNode->pChildren) {
      PLAN_ERR_RET(joinParallelCollectRegions(pCxt, (const SLogicNode*)pChild));
    }
    return TSDB_CODE_SUCCESS;
  }
  SJoinParallelRegion* pRegion =
      joinParallelCreateRegion((const SJoinLogicNode*)pNode);
  if (NULL == pRegion) return terrno;
  int32_t code = joinParallelCollectTree(pCxt, pRegion, pNode, traits);
  if (TSDB_CODE_SUCCESS == code) {
    bool overLimit =
        taosArrayGetSize(pRegion->pInputs) > JOIN_PARALLEL_MAX_INPUTS;
    pRegion->supported = pRegion->supported && !overLimit;
    code = joinParallelIndexInputs(pRegion);
  }
  if (TSDB_CODE_SUCCESS == code && pRegion->supported) {
    code = joinParallelCollectPredicates(pCxt, pRegion);
  }
  if (TSDB_CODE_SUCCESS == code &&
      NULL == taosArrayPush(pCxt->pRegions, &pRegion)) {
    code = terrno;
  }
  if (TSDB_CODE_SUCCESS != code) joinParallelDestroyRegion(pRegion);
  return code;
}

int32_t planPrepareJoinRewrite(const SPlanContext*   pCxt,
                               const SLogicNode*     pRoot,
                               SJoinRewriteContext** ppContext) {
  if (NULL == ppContext) return TSDB_CODE_INVALID_PARA;
  *ppContext = NULL;
  if (NULL == pCxt || NULL == pRoot) return TSDB_CODE_INVALID_PARA;
  SJoinRewriteContext* pContext = taosMemoryCalloc(1, sizeof(*pContext));
  if (NULL == pContext) return terrno;
  pContext->pPlanCxt = pCxt;
  pContext->pRoot = pRoot;
  pContext->pRegions = taosArrayInit(4, sizeof(SJoinParallelRegion*));
  int32_t code = NULL == pContext->pRegions ? terrno : TSDB_CODE_SUCCESS;
  if (TSDB_CODE_SUCCESS == code && planJoinParallelQuerySupported(pCxt)) {
    code = joinParallelCollectRegions(pContext, pRoot);
  }
  if (TSDB_CODE_SUCCESS != code) {
    planDestroyJoinRewriteContext(pContext);
    return code;
  }
  *ppContext = pContext;
  return TSDB_CODE_SUCCESS;
}

// Check for JOINs without crossing runtime-controlled operator boundaries.
static bool joinParallelHasJoin(const SLogicNode* pNode) {
  if (pNode->dynamicOp ||
      QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL == nodeType(pNode)) {
    return false;
  }
  if (QUERY_NODE_LOGIC_PLAN_JOIN == nodeType(pNode)) return true;
  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    if (joinParallelHasJoin((const SLogicNode*)pChild)) return true;
  }
  return false;
}

// Bind default temporal keys bottom-up, stopping after the first new binding.
static int32_t joinParallelBindTimeKeys(SPlanContext* pCxt, SLogicNode* pNode,
                                        bool* pChanged) {
  // Keep runtime-dependent subtrees in their original evaluation context.
  if (joinParallelNodeTraits(pNode).runtimeDependent) {
    return TSDB_CODE_SUCCESS;
  }
  // Bind inner temporal JOINs before deriving defaults for their parents.
  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    PLAN_ERR_RET(joinParallelBindTimeKeys(pCxt, (SLogicNode*)pChild, pChanged));
    // Let earlier optimizer rules process this change before binding more keys.
    if (*pChanged) return TSDB_CODE_SUCCESS;
  }
  if (QUERY_NODE_LOGIC_PLAN_JOIN != nodeType(pNode)) {
    return TSDB_CODE_SUCCESS;
  }
  SJoinLogicNode* pJoin = (SJoinLogicNode*)pNode;
  if (!IS_ASOF_JOIN(pJoin->subType) && !IS_WINDOW_JOIN(pJoin->subType)) {
    return TSDB_CODE_SUCCESS;
  }
  // Explicit or already-bound keys must not report another optimization.
  bool bound = NULL != planGetTemporalJoinTimeCond(pJoin);
  PLAN_ERR_RET(planBindTemporalJoinTimeKeys(pCxt, pJoin));
  *pChanged = !bound && NULL != planGetTemporalJoinTimeCond(pJoin);
  return TSDB_CODE_SUCCESS;
}

// Commit one validated improvement per pass, then let earlier rules restart.
int32_t planRewriteJoinParallel(SPlanContext* pCxt, SLogicSubplan* pSubplan,
                                bool* pChanged) {
  if (NULL == pChanged) return TSDB_CODE_INVALID_PARA;
  *pChanged = false;
  if (NULL == pCxt || NULL == pSubplan || NULL == pSubplan->pNode) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (!planJoinParallelQuerySupported(pCxt) ||
      !joinParallelHasJoin(pSubplan->pNode)) {
    return TSDB_CODE_SUCCESS;
  }
  PLAN_ERR_RET(joinParallelBindTimeKeys(pCxt, pSubplan->pNode, pChanged));
  if (*pChanged) return TSDB_CODE_SUCCESS;
  PLAN_ERR_RET(planRebuildJoinInputProps(pCxt, pSubplan->pNode));
  SJoinRewriteContext* pContext = NULL;
  int32_t code = planPrepareJoinRewrite(pCxt, pSubplan->pNode, &pContext);
  if (TSDB_CODE_SUCCESS != code) return code;
  SLogicNode* pWinner = NULL;
  SPlanContext candidate = *pCxt;
  candidate.pJoinPropsStore = NULL;
  for (size_t i = 0; i < taosArrayGetSize(pContext->pRegions); ++i) {
    const SJoinParallelRegion* pRegion = taosArrayGetP(pContext->pRegions, i);
    SJoinTreeChoice* pChoice = NULL;
    code = planSearchJoinTree(pContext, pRegion, &pChoice);
    if (TSDB_CODE_SUCCESS != code) break;
    if (NULL == pChoice) continue;
    SJoinBuildResult result = {0};
    code = joinBuildTree(pCxt, pSubplan->pNode, pChoice, &result);
    bool supported = false;
    if (TSDB_CODE_PAR_NOT_SUPPORT_JOIN == code ||
        TSDB_CODE_PLAN_NOT_SUPPORT_JOIN_COND == code) {
      code = TSDB_CODE_SUCCESS;
    } else if (TSDB_CODE_SUCCESS == code) {
      code = joinBuildValidate(&candidate, pSubplan->pNode, pChoice, &result,
                                 &supported);
    }
    planDestroyJoinTreeChoice(pChoice);
    if (TSDB_CODE_SUCCESS == code && supported) {
      pWinner = result.pRoot;
      break;
    }
    nodesDestroyNode((SNode*)result.pRoot);
    planDestroyJoinPropsStore(&candidate);
    if (TSDB_CODE_SUCCESS != code) break;
  }
  planDestroyJoinRewriteContext(pContext);
  if (NULL != pWinner) {
    planDestroyJoinPropsStore(pCxt);
    nodesDestroyNode((SNode*)pSubplan->pNode);
    pSubplan->pNode = pWinner;
    pCxt->pJoinPropsStore = candidate.pJoinPropsStore;
    candidate.pJoinPropsStore = NULL;
    *pChanged = true;
  }
  planDestroyJoinPropsStore(&candidate);
  return code;
}
