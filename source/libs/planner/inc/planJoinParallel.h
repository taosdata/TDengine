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

#ifndef TDENGINE_PLANNER_JOIN_PARALLEL_H_
#define TDENGINE_PLANNER_JOIN_PARALLEL_H_

#include "planInt.h"
#include "tarray.h"
#include "tsimplehash.h"

#ifdef __cplusplus
extern "C" {
#endif

// Inputs of a supported region fit in one SJoinInputSet word.
enum { JOIN_PARALLEL_MAX_INPUTS = 8 };

typedef enum EJoinRegionMode {
  JOIN_REGION_PURE_INNER,
  JOIN_REGION_MIXED,
} EJoinRegionMode;

typedef enum EJoinPredicateSource {
  JOIN_PREDICATE_FULL_ON,
  JOIN_PREDICATE_PRIMARY_ON,
  JOIN_PREDICATE_FILTER,
} EJoinPredicateSource;

// Bit i marks region input i.
typedef uint64_t SJoinInputSet;

typedef struct SJoinParallelInput {
  int32_t                inputId;
  const SLogicNode*      pNode;
  const SJoinInputProps* pProps;
} SJoinParallelInput;

typedef struct SJoinParallelPredicate {
  const SNode*         pExpr;
  int32_t              sourceOpId;
  EJoinPredicateSource source;
  SJoinInputSet        inputs;
  bool                 unresolved;
} SJoinParallelPredicate;

typedef struct SJoinParallelRegion {
  const SJoinLogicNode* pRoot;
  EJoinRegionMode       mode;
  SArray*               pInputs;  // SJoinParallelInput, original leaf order.
  SArray*               pOps;     // const SJoinLogicNode*, preorder.
  SArray*               pPredicates;  // SJoinParallelPredicate, borrowed.
  SSHashObj*            pColumnInputs;  // SJoinColId -> input id.
  bool                  supported;
} SJoinParallelRegion;

typedef struct SJoinRewriteContext {
  const SPlanContext* pPlanCxt;
  const SLogicNode*   pRoot;
  SArray*             pRegions;  // SJoinParallelRegion*, inner regions first.
} SJoinRewriteContext;

typedef struct SJoinEqClasses {
  SArray*       pMembers;  // One member per classified column.
  SSHashObj*    pColumnMembers;  // SJoinColId -> member index.
  SJoinInputSet inputs;
} SJoinEqClasses;

typedef enum EJoinConflictType {
  JOIN_CONFLICT_INNER,
  JOIN_CONFLICT_LEFT,
  JOIN_CONFLICT_FULL,
  JOIN_CONFLICT_SEMI,
  JOIN_CONFLICT_ANTI,
  JOIN_CONFLICT_TYPE_COUNT,
} EJoinConflictType;

typedef struct SJoinConflictRule {
  SJoinInputSet trigger;
  SJoinInputSet required;
} SJoinConflictRule;

typedef struct SJoinConflictOp {
  const SJoinLogicNode* pNode;
  int32_t              opId;
  EJoinConflictType    type;
  SJoinInputSet        left;  // Left-canonical inputs, retained side first.
  SJoinInputSet        right;
  SJoinInputSet        onInputs;
  SJoinInputSet        requiredLeft;
  SJoinInputSet        requiredRight;
  SJoinInputSet        nr;
  SArray*              pRules;  // SJoinConflictRule.
  bool                 normalizedSwap;
  bool                 pinnedFilter;
} SJoinConflictOp;

typedef struct SJoinConflictModel {
  SArray*       pOps;  // SJoinConflictOp, indexed by region opId.
  SJoinInputSet inputs;
  const SNode*  pRootFilter;  // Apply once above the final region output.
} SJoinConflictModel;

typedef struct SJoinDpCell {
  SJoinInputSet inputs;
  uint32_t      usedOps;  // Mixed mode: one bit per original opId.
  int32_t       height;
  int32_t       left;
  int32_t       right;
  int32_t       inputId;  // Nonnegative for leaves only.
  int32_t       opId;  // -1 for leaves, pure nodes, and pure references.
  bool          valid;
  bool          swapped;
  bool          isCross;
  // Owns a complete pure alternative; usedOps reserves its original operators.
  struct SJoinTreeChoice* pPureChoice;
} SJoinDpCell;

typedef struct SJoinTreeChoice {
  const SJoinParallelRegion* pRegion;
  EJoinRegionMode           mode;
  SArray*                   pCells;  // SJoinDpCell, children by array index.
  SArray*                   pPredicates;  // Owned pure predicate view.
  SJoinEqClasses*            pEqClasses;
  SJoinConflictModel*        pConflicts;
  const SNode*              pRootFilter;  // Mixed region's output filter.
  int32_t                   rootCell;
  int32_t                   originalHeight;
} SJoinTreeChoice;

// Requires current JOIN properties. All node, expression, and property
// references are borrowed until the next tree change or property rebuild.
// Region predicates describe JOIN clauses; input filters stay on their nodes.
// Predicates are collected only for supported regions.
// On failure *ppContext is NULL and all partially built analysis is released.
int32_t planPrepareJoinRewrite(const SPlanContext*   pCxt,
                               const SLogicNode*     pRoot,
                               SJoinRewriteContext** ppContext);
void    planDestroyJoinRewriteContext(SJoinRewriteContext* pContext);
bool    planJoinInputSetContains(SJoinInputSet set, int32_t inputId);
int32_t planJoinInputForColumn(const SJoinParallelRegion* pRegion,
                               const SJoinColId*          pId);

// Build exact equality classes for one supported pure INNER/CROSS region.
// Other regions return success with *ppClasses == NULL. The tree is unchanged.
// The caller owns the model, which keeps no tree or property references.
// Errors leave the output NULL.
int32_t planBuildJoinEqClasses(const SJoinRewriteContext* pContext,
                               const SJoinParallelRegion* pRegion,
                               SJoinEqClasses** ppClasses);
void    planDestroyJoinEqClasses(SJoinEqClasses* pClasses);
// Class ids are local to this model; -1 means the column has no exact class.
int32_t planJoinEqClassForColumn(const SJoinEqClasses* pClasses,
                                 const SJoinColId* pId);
// True if an exact class spans two nonempty, disjoint subsets of this region.
bool    planJoinEqClassesConnect(const SJoinEqClasses* pClasses,
                                 SJoinInputSet left, SJoinInputSet right);

// Bit i proves the condition cannot be TRUE when every column of region
// input i is NULL. pProps resolves columns in the condition owner's scope.
// Uses current region/properties, borrows all references, and changes no tree.
// Unknown expressions and invalid analysis inputs contribute no proof.
SJoinInputSet planJoinCondRejectsNull(const SJoinParallelRegion* pRegion,
                                      const SJoinInputProps* pProps,
                                      const SNode* pCondition);

// Build CD-C rules for a supported mixed region without changing its tree.
// Other regions return success with *ppModel == NULL. The caller owns the
// model; original nodes are borrowed until the next tree/property rebuild.
// Failure releases partial analysis and leaves *ppModel == NULL.
// Consumers must apply pRootFilter at the final region output, independently
// of which original operator becomes the root. Other filters remain pinned.
int32_t planBuildJoinConflicts(const SJoinRewriteContext* pContext,
                                const SJoinParallelRegion* pRegion,
                                SJoinConflictModel** ppModel);
void planDestroyJoinConflicts(SJoinConflictModel* pModel);
// swapped maps the candidate's physical sides back to the original operator.
// This checks one split; the enumerator must track disjoint operator use.
bool planJoinSplitIsLegal(const SJoinConflictModel* pModel, int32_t opId,
                           SJoinInputSet left, SJoinInputSet right,
                           bool swapped);

// Find a strictly shorter region tree without changing the logical plan.
// Success with NULL means unsupported or no strict improvement: keep the tree.
// The caller owns the choice and its models; region/node references are
// borrowed until the next tree or property rebuild. Errors leave output NULL.
// Pure nodes have no original operator identity; construction assigns each
// original INNER/CROSS node once and redistributes predicates separately.
// Nested pure alternatives borrow the same region and retain its input ids.
// Their owned predicate view contains only the reserved operators' ON clauses.
int32_t planSearchJoinTree(const SJoinRewriteContext* pContext,
                            const SJoinParallelRegion* pRegion,
                            SJoinTreeChoice** ppChoice);
void planDestroyJoinTreeChoice(SJoinTreeChoice* pChoice);
const SJoinDpCell* planJoinTreeCell(const SJoinTreeChoice* pChoice,
                                    int32_t cellId);

// Clone the query block once and construct the selected region in the copy.
// The caller owns the result. The original tree, properties and choice stay
// unchanged; errors release the partial copy and leave *ppRoot NULL.
int32_t planBuildJoinTree(const SPlanContext* pCxt, const SLogicNode* pRoot,
                           const SJoinTreeChoice* pChoice,
                           SLogicNode** ppRoot);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_PLANNER_JOIN_PARALLEL_H_
