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

#include "filter.h"
#include "functionMgt.h"
#include "planInt.h"
#include "ttime.h"

// Capture syntax that createLogicPlan consumes (notably LIMIT and GROUP BY).
bool planCanDecorrelateSelect(SNode* pAst, bool inner) {
  if (!pAst || nodeType(pAst) != QUERY_NODE_SELECT_STMT) {
    return false;
  }
  SSelectStmt* select = (SSelectStmt*)pAst;
  if (!select->pFromTable || nodeType(select->pFromTable) != QUERY_NODE_REAL_TABLE ||
      getOptHint(select->pHint, HINT_NO_DECORRELATE)) {
    return false;
  }
  if (select->isDistinct || select->pGroupByList || select->pPartitionByList || select->pHaving ||
      select->pWindow || select->pWindowList || select->pFill || select->pRange || select->pEvery ||
      select->pSlimit || select->hasIndefiniteRowsFunc || select->hasMultiRowsFunc) {
    return false;
  }
  if (inner) {
    return LIST_LENGTH(select->pProjectionList) == 1 && !select->pLimit && !select->pOrderByList;
  }
  return !select->hasAggFuncs;
}

static SLogicNode* onlyChild(SLogicNode* pNode) {
  return pNode && LIST_LENGTH(pNode->pChildren) == 1 ? (SLogicNode*)nodesListGetNode(pNode->pChildren, 0) : NULL;
}

static bool tableScan(SLogicNode* pNode) {
  if (!pNode || nodeType(pNode) != QUERY_NODE_LOGIC_PLAN_SCAN) return false;
  SScanLogicNode* s = (SScanLogicNode*)pNode;
  return s->scanType == SCAN_TYPE_TABLE && !s->virtualStableScan &&
         (s->tableType == TSDB_NORMAL_TABLE || s->tableType == TSDB_CHILD_TABLE || s->tableType == TSDB_SUPER_TABLE);
}

static int32_t appendClone(SNodeList** ppList, const SNode* pNode) {
  SNode* pCopy = NULL;
  PLAN_ERR_RET(nodesCloneNode(pNode, &pCopy));
  return nodesListMakeStrictAppend(ppList, pCopy);
}

static bool plainColumn(SNode* n) {
  if (!n || nodeType(n) != QUERY_NODE_COLUMN) return false;
  SColumnNode* c = (SColumnNode*)n;
  return c->colType == COLUMN_TYPE_COLUMN || c->colType == COLUMN_TYPE_TAG;
}

static SColumnNode* scanColumn(SLogicNode* scan, const SColumnNode* ref) {
  SNode* n = NULL;
  FOREACH(n, scan->pTargets) {
    SColumnNode* col = (SColumnNode*)n;
    if (plainColumn(n) && col->colId == ref->colId && col->tableId == ref->tableId &&
        strcmp(col->tableAlias, ref->tableAlias) == 0)
      return col;
  }
  return NULL;
}

static bool literal(const SNode* n) {
  if (!n || nodeType(n) != QUERY_NODE_VALUE) return false;
  const SValueNode* v = (const SValueNode*)n;
  return v->translate && !v->placeholderNo && !v->isNull && !IS_VAL_UNSET(v->flag);
}

static bool localFilter(SNode* n, int32_t* code) {
  if (nodeType(n) == QUERY_NODE_LOGIC_CONDITION) {
    SNode* child = NULL;
    FOREACH(child, ((SLogicConditionNode*)n)->pParameterList) {
      if (!localFilter(child, code)) return false;
    }
    return true;
  }
  if (nodeType(n) != QUERY_NODE_OPERATOR) return false;
  SOperatorNode* op = (SOperatorNode*)n;
  if (op->opType == OP_TYPE_IS_NULL || op->opType == OP_TYPE_IS_NOT_NULL) return plainColumn(op->pLeft);
  switch (op->opType) {
    case OP_TYPE_EQUAL:
    case OP_TYPE_NOT_EQUAL:
    case OP_TYPE_GREATER_THAN:
    case OP_TYPE_GREATER_EQUAL:
    case OP_TYPE_LOWER_THAN:
    case OP_TYPE_LOWER_EQUAL:
      break;
    default:
      return false;
  }
  SNode* col = plainColumn(op->pLeft) ? op->pLeft : op->pRight;
  SNode* val = col == op->pLeft ? op->pRight : op->pLeft;
  if (!plainColumn(col) || !literal(val)) return false;
  int8_t type = ((SExprNode*)col)->resType.type;
  // The translator keeps integer literals as BIGINT even for an INT
  // column. Keep the original comparison and its coercion intact.
  int8_t valueType = ((SExprNode*)val)->resType.type;
  if (type != TSDB_DATA_TYPE_TIMESTAMP) return IS_INTEGER_TYPE(type) && IS_INTEGER_TYPE(valueType);
  if (valueType == TSDB_DATA_TYPE_TIMESTAMP || IS_INTEGER_TYPE(valueType)) return true;
  if (valueType == TSDB_DATA_TYPE_VARCHAR) {
    SValueNode* v = (SValueNode*)val;
    int64_t     ts;
    int32_t result = convertStringToTimestamp(valueType, v->datum.p, ((SExprNode*)col)->resType.precision, &ts, v->tz,
                                              v->charsetCxt);
    if (result != TSDB_CODE_INVALID_TIMESTAMP) *code = result;
    return result == TSDB_CODE_SUCCESS;
  }
  return false;
}

typedef struct {
  SNodeList*                     outerKeys;
  SNodeList*                     innerKeys;
  SNode*                         residual;
  const SCorrelatedSubqueryNode* call;
  SLogicNode*                    outerScan;
  bool                           eligible;
} SScalarKeys;

static SColumnNode* outerKey(SScalarKeys* keys, int32_t id) {
  SNode* n = NULL;
  FOREACH(n, keys->call->pParamBindings) {
    SCorrelationParamBindingNode* b = (SCorrelationParamBindingNode*)n;
    if (b->paramId == id && b->scopeId == keys->call->parentScopeId && b->sourceKind == CORRELATION_SOURCE_LOGICAL &&
        b->tableRefId && plainColumn(b->pSource))
      return scanColumn(keys->outerScan, (SColumnNode*)b->pSource);
  }
  return NULL;
}

static int32_t extractKeys(SNode* n, SScalarKeys* keys) {
  if (!n || !keys->eligible) return TSDB_CODE_SUCCESS;
  if (nodeType(n) == QUERY_NODE_LOGIC_CONDITION && ((SLogicConditionNode*)n)->condType == LOGIC_COND_TYPE_AND) {
    SNode* child = NULL;
    FOREACH(child, ((SLogicConditionNode*)n)->pParameterList) { PLAN_ERR_RET(extractKeys(child, keys)); }
    return TSDB_CODE_SUCCESS;
  }
  int32_t code = TSDB_CODE_SUCCESS;
  if (localFilter(n, &code)) {
    SNode* copy = NULL;
    PLAN_ERR_RET(nodesCloneNode(n, &copy));
    code = nodesMergeNode(&keys->residual, &copy);
    nodesDestroyNode(copy);
    return code;
  }
  PLAN_ERR_RET(code);
  SOperatorNode* op = (SOperatorNode*)n;
  if (nodeType(n) == QUERY_NODE_OPERATOR && op->opType == OP_TYPE_EQUAL) {
    SNode* col = plainColumn(op->pLeft) ? op->pLeft : op->pRight;
    SNode* param = col == op->pLeft ? op->pRight : op->pLeft;
    if (plainColumn(col) && param && nodeType(param) == QUERY_NODE_CORRELATION_PARAM) {
      SDataType    type = ((SExprNode*)col)->resType;
      SColumnNode* outer = outerKey(keys, ((SCorrelationParamNode*)param)->paramId);
      if (outer && type.type == outer->node.resType.type &&
          (IS_INTEGER_TYPE(type.type) ||
           (type.type == TSDB_DATA_TYPE_TIMESTAMP && type.precision == outer->node.resType.precision))) {
        PLAN_ERR_RET(appendClone(&keys->outerKeys, (SNode*)outer));
        return appendClone(&keys->innerKeys, col);
      }
    }
  }
  keys->eligible = false;
  return TSDB_CODE_SUCCESS;
}

// A missing group yields NULL after the left join. Only rewrite comparisons
// that reject both NULL and a count of zero in the original WHERE clause.
static bool canRewriteCountPredicate(SNode* predicate, int32_t id) {
  if (!predicate || nodeType(predicate) != QUERY_NODE_OPERATOR) return false;
  SOperatorNode* op = (SOperatorNode*)predicate;
  if (!op->pLeft || nodeType(op->pLeft) != QUERY_NODE_CORRELATED_SUBQUERY ||
      ((SCorrelatedSubqueryNode*)op->pLeft)->templateId != id || !literal(op->pRight))
    return false;
  SValueNode* v = (SValueNode*)op->pRight;
  if (!IS_INTEGER_TYPE(v->node.resType.type) ||
      (IS_UNSIGNED_NUMERIC_TYPE(v->node.resType.type) && v->datum.u > INT64_MAX))
    return false;
  return (op->opType == OP_TYPE_GREATER_THAN && v->datum.i >= 0) ||
         ((op->opType == OP_TYPE_GREATER_EQUAL || op->opType == OP_TYPE_EQUAL) && v->datum.i > 0);
}

static bool safeAggregate(SFunctionNode* f, bool allowCount) {
  if (f->isDistinct || f->pOver || fmIsUserDefinedFunc(f->funcId) || !fmIsAggFunc(f->funcId) ||
      fmGetFuncReturnRows(f) != FUNC_RETURN_ROWS_NORMAL || LIST_LENGTH(f->pParameterList) != 1)
    return false;
  SNode* arg = nodesListGetNode(f->pParameterList, 0);
  if (!plainColumn(arg)) return false;
  int8_t type = ((SExprNode*)arg)->resType.type;
  switch (f->funcType) {
    case FUNCTION_TYPE_COUNT:
      return allowCount && ((SColumnNode*)arg)->colType == COLUMN_TYPE_COLUMN;
    case FUNCTION_TYPE_MIN:
    case FUNCTION_TYPE_MAX:
      return true;
    case FUNCTION_TYPE_AVG:
    case FUNCTION_TYPE_SUM:
    case FUNCTION_TYPE_STDDEV:
    case FUNCTION_TYPE_SPREAD:
      return IS_INTEGER_TYPE(type) || IS_FLOAT_TYPE(type);
    default:
      return false;
  }
}

typedef struct {
  int32_t      templateId;
  bool         hasCall;
  bool         hasOtherSubquery;
  bool         hasUnsupportedExpr;
  SColumnNode* result;
  int32_t      code;
} SCallSite;

static EDealRes inspectExpr(SNode* n, void* ctx) {
  SCallSite* site = ctx;
  if (nodeType(n) == QUERY_NODE_CORRELATED_SUBQUERY) {
    if (((SCorrelatedSubqueryNode*)n)->templateId == site->templateId) {
      site->hasCall = true;
    } else {
      site->hasOtherSubquery = true;
    }
    return DEAL_RES_IGNORE_CHILD;
  }
  switch (nodeType(n)) {
    case QUERY_NODE_CASE_WHEN:
    case QUERY_NODE_FUNCTION:
      site->hasUnsupportedExpr = true;
      break;
    case QUERY_NODE_REMOTE_VALUE:
    case QUERY_NODE_REMOTE_ROW:
    case QUERY_NODE_REMOTE_VALUE_LIST:
    case QUERY_NODE_REMOTE_ZERO_ROWS:
      site->hasOtherSubquery = true;
      break;
    default:
      break;
  }
  return DEAL_RES_CONTINUE;
}

static EDealRes replaceCall(SNode** ppNode, void* ctx) {
  if (nodeType(*ppNode) != QUERY_NODE_CORRELATED_SUBQUERY) return DEAL_RES_CONTINUE;
  SCallSite*               site = ctx;
  SCorrelatedSubqueryNode* call = (SCorrelatedSubqueryNode*)*ppNode;
  if (call->templateId != site->templateId) return DEAL_RES_IGNORE_CHILD;
  SColumnNode* col = NULL;
  site->code = nodesCloneNode((SNode*)site->result, (SNode**)&col);
  if (site->code) return DEAL_RES_ERROR;
  tstrncpy(col->node.aliasName, call->node.aliasName, sizeof(col->node.aliasName));
  tstrncpy(col->node.userAlias, call->node.userAlias, sizeof(col->node.userAlias));
  col->node.projIdx = call->node.projIdx;
  col->node.relatedTo = call->node.relatedTo;
  nodesDestroyNode(*ppNode);
  *ppNode = (SNode*)col;
  return DEAL_RES_IGNORE_CHILD;
}

static int32_t appendAggregateFunction(SAggLogicNode* agg, const char* name, SNode* arg) {
  SNodeList*     parameters = NULL;
  SFunctionNode* function = NULL;
  int32_t code = appendClone(&parameters, arg);
  if (code == TSDB_CODE_SUCCESS) {
    code = createFunction(name, parameters, &function);
  }
  if (code != TSDB_CODE_SUCCESS) {
    nodesDestroyList(parameters);
    return code;
  }
  snprintf(function->node.aliasName, sizeof(function->node.aliasName), "%s.%p", name, (void*)function);
  PLAN_ERR_RET(nodesListMakeStrictAppend(&agg->pAggFuncs, (SNode*)function));
  return createColumnByRewriteExpr((SNode*)function, &agg->node.pTargets);
}

static int32_t projectOutput(SProjectLogicNode* p, SNode* input, const char* name) {
  PLAN_ERR_RET(appendClone(&p->pProjections, input));
  SExprNode* expr = (SExprNode*)p->pProjections->pTail->pNode;
  tstrncpy(expr->aliasName, name, sizeof(expr->aliasName));
  SColumnNode* col = createColumnByExpr(p->stmtName, expr);
  if (!col) return terrno;
  return nodesListMakeStrictAppend(&p->node.pTargets, (SNode*)col);
}

static int32_t groupRight(SProjectLogicNode* right, SScalarKeys* keys, bool needsRowCheck) {
  SLogicNode*    child = onlyChild(&right->node);
  SAggLogicNode* agg = (SAggLogicNode*)child;
  if (needsRowCheck) {
    PLAN_ERR_RET(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_AGG, (SNode**)&agg));
    agg->node.pChildren = right->node.pChildren;
    right->node.pChildren = NULL;
    PLAN_ERR_RET(nodesListMakeStrictAppend(&right->node.pChildren, (SNode*)agg));
    agg->node.precision = child->precision;
    PLAN_ERR_RET(appendAggregateFunction(agg, "_group_key", nodesListGetNode(right->pProjections, 0)));
    // Count the primary timestamp, which is non-null for every stored row.
    SNode* timestamp = NULL;
    PLAN_ERR_RET(stbSplFindPrimaryKeyFromScan((SScanLogicNode*)child, &timestamp));
    if (!timestamp) {
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }
    PLAN_ERR_RET(appendAggregateFunction(agg, "count", timestamp));
  }
  SLogicNode* scan = onlyChild(&agg->node);
  // The outer scan may live on another vnode even when both tables have
  // just one vgroup. Route this input through its own scan subplan.
  ((SScanLogicNode*)scan)->needSplit = true;
  nodesDestroyNode(scan->pConditions);
  scan->pConditions = keys->residual;
  keys->residual = NULL;
  agg->node.groupAction = GROUP_ACTION_CLEAR;
  agg->hasGroup = true;
  agg->node.resultDataOrder = DATA_ORDER_LEVEL_NONE;
  right->node.stmtRoot = false;
  snprintf(right->stmtName, sizeof(right->stmtName), "#scalar.%d", keys->call->templateId);
  SColumnNode* outer = (SColumnNode*)nodesListGetNode(keys->outerKeys, 0);
  if (strcmp(right->stmtName, outer->tableAlias) == 0) {
    snprintf(right->stmtName, sizeof(right->stmtName), "#scalar.%d_", keys->call->templateId);
  }
  NODES_DESTORY_LIST(right->pProjections);
  NODES_DESTORY_LIST(right->node.pTargets);
  PLAN_ERR_RET(projectOutput(right, nodesListGetNode(agg->node.pTargets, 0), "value"));
  if (needsRowCheck) PLAN_ERR_RET(projectOutput(right, nodesListGetNode(agg->node.pTargets, 1), "rows"));
  SNode* key = NULL;
  FOREACH(key, keys->innerKeys) {
    SGroupingSetNode* g = NULL;
    PLAN_ERR_RET(nodesMakeNode(QUERY_NODE_GROUPING_SET, (SNode**)&g));
    PLAN_ERR_RET(nodesListMakeStrictAppend(&agg->pGroupKeys, (SNode*)g));
    g->groupingSetType = GP_TYPE_NORMAL;
    PLAN_ERR_RET(appendClone(&g->pParameterList, key));
    PLAN_ERR_RET(appendClone(&agg->node.pTargets, key));
    char name[32];
    snprintf(name, sizeof(name), "key%d", LIST_LENGTH(agg->pGroupKeys));
    PLAN_ERR_RET(projectOutput(right, key, name));
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t joinKeys(SJoinLogicNode* join, SScalarKeys* keys, SProjectLogicNode* right, bool needsRowCheck) {
  int32_t idx = needsRowCheck ? 2 : 1;
  SNode*  left = NULL;
  FOREACH(left, keys->outerKeys) {
    SOperatorNode* op = NULL;
    PLAN_ERR_RET(nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&op));
    op->opType = OP_TYPE_EQUAL;
    op->node.resType.type = TSDB_DATA_TYPE_BOOL;
    op->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
    SNode*  n = (SNode*)op;
    int32_t code = nodesMergeNode(&join->pFullOnCond, &n);
    nodesDestroyNode(n);
    PLAN_ERR_RET(code);
    PLAN_ERR_RET(nodesCloneNode(left, &op->pLeft));
    PLAN_ERR_RET(nodesCloneNode(nodesListGetNode(right->node.pTargets, idx++), &op->pRight));
  }
  return TSDB_CODE_SUCCESS;
}

static EDealRes checkDependency(SNode* n, void* ctx) {
  switch (nodeType(n)) {
    case QUERY_NODE_CORRELATION_PARAM:
    case QUERY_NODE_OUTER_REF:
    case QUERY_NODE_CORRELATED_SUBQUERY:
    case QUERY_NODE_REMOTE_VALUE:
    case QUERY_NODE_REMOTE_ROW:
    case QUERY_NODE_REMOTE_VALUE_LIST:
    case QUERY_NODE_REMOTE_ZERO_ROWS:
      *(bool*)ctx = true;
      return DEAL_RES_END;
    default:
      return DEAL_RES_CONTINUE;
  }
}

static bool hasDependency(SLogicNode* n) {
  bool found = false;
  planWalkLogicExprFields(n, checkDependency, &found);
  SNode* child = NULL;
  FOREACH(child, n->pChildren) {
    if (found || hasDependency((SLogicNode*)child)) return true;
  }
  return found;
}

static int32_t buildJoin(SLogicNode* parent, SProjectLogicNode** ppRight, SScalarKeys* keys, bool needsRowCheck) {
  SProjectLogicNode* right = *ppRight;
  SJoinLogicNode*    join = NULL;
  PLAN_ERR_RET(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_JOIN, (SNode**)&join));
  SLogicNode* left = onlyChild(parent);
  join->node.pChildren = parent->pChildren;
  parent->pChildren = NULL;
  PLAN_ERR_RET(nodesListMakeStrictAppend(&parent->pChildren, (SNode*)join));
  PLAN_ERR_RET(nodesListAppend(join->node.pChildren, (SNode*)right));
  *ppRight = NULL;
  join->joinType = JOIN_TYPE_LEFT;
  join->subType = JOIN_STYPE_OUTER;
  join->joinAlgoHint = JOIN_ALGO_HASH;
  join->rightNoOrderedSubQuery = true;
  join->hasSubQuery = true;
  join->scalarSubqueryJoin = true;
  join->node.groupAction = GROUP_ACTION_CLEAR;
  join->node.precision = left->precision;
  PLAN_ERR_RET(joinKeys(join, keys, right, needsRowCheck));
  PLAN_ERR_RET(nodesCloneList(left->pTargets, &join->node.pTargets));
  PLAN_ERR_RET(appendClone(&join->node.pTargets, nodesListGetNode(right->node.pTargets, 0)));
  if (needsRowCheck) {
    SNode* count = nodesListGetNode(right->node.pTargets, 1);
    PLAN_ERR_RET(appendClone(&join->node.pTargets, count));
    SScalarCheckLogicNode* check = NULL;
    PLAN_ERR_RET(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SCALAR_CHECK, (SNode**)&check));
    check->node.pChildren = parent->pChildren;
    parent->pChildren = NULL;
    PLAN_ERR_RET(nodesListMakeStrictAppend(&parent->pChildren, (SNode*)check));
    check->node.precision = left->precision;
    PLAN_ERR_RET(nodesCloneList(join->node.pTargets, &check->node.pTargets));
    PLAN_ERR_RET(appendClone(&check->pProjections, count));
    PLAN_ERR_RET(appendClone(&check->pProjections, nodesListGetNode(right->node.pTargets, 0)));
  }
  return TSDB_CODE_SUCCESS;
}

static SLogicNode* leftScan(SLogicNode* n) {
  while (n && (nodeType(n) == QUERY_NODE_LOGIC_PLAN_SCALAR_CHECK || nodeType(n) == QUERY_NODE_LOGIC_PLAN_JOIN))
    n = (SLogicNode*)nodesListGetNode(n->pChildren, 0);
  return tableScan(n) ? n : NULL;
}

static bool canGroupScalarResult(SProjectLogicNode* project, bool allowCount, bool* needsRowCheck) {
  SLogicNode* child = onlyChild(&project->node);
  SNode*      out = nodesListGetNode(project->pProjections, 0);
  if (!child || !out || nodeType(out) != QUERY_NODE_COLUMN || LIST_LENGTH(project->pProjections) != 1) return false;
  if (tableScan(child)) {
    SColumnNode* column = (SColumnNode*)out;
    // _group_key reserves the schema width for each value. A BLOB payload can exceed that width.
    *needsRowCheck = true;
    return column->colType == COLUMN_TYPE_COLUMN && !IS_STR_DATA_BLOB(column->node.resType.type) &&
           scanColumn(child, column);
  }
  if (nodeType(child) != QUERY_NODE_LOGIC_PLAN_AGG || !tableScan(onlyChild(child))) return false;
  SAggLogicNode* agg = (SAggLogicNode*)child;
  if (LIST_LENGTH(agg->pAggFuncs) != 1 || agg->pGroupKeys || agg->node.pConditions) return false;
  SFunctionNode* func = (SFunctionNode*)nodesListGetNode(agg->pAggFuncs, 0);
  return strcmp(((SColumnNode*)out)->colName, func->node.aliasName) == 0 && safeAggregate(func, allowCount);
}

static bool containsScalarCall(SLogicNode* node, int32_t templateId) {
  SCallSite site = {.templateId = templateId};
  planWalkLogicExprFields(node, inspectExpr, &site);
  if (site.hasCall) {
    return true;
  }
  SNode* child = NULL;
  FOREACH(child, node->pChildren) {
    if (containsScalarCall((SLogicNode*)child, templateId)) {
      return true;
    }
  }
  return false;
}

// Keep the same storage-level time and tag filters as the original scan.
// Its remaining predicate evaluates the scalar call for every surviving row.
static int32_t splitOuterFilter(SLogicNode* scan, SLogicNode* stage, bool* eligible) {
  SNode*  time = NULL;
  SNode*  tags = NULL;
  int32_t code = filterPartitionCond(&scan->pConditions, &time, NULL, &tags, &stage->pConditions);
  if (code == TSDB_CODE_SUCCESS && time) {
    STimeWindow range = TSWINDOW_INITIALIZER;
    bool        strict = false, remote = false;
    code = filterGetTimeRange(time, &range, &strict, &remote);
    *eligible = strict && !remote;
  }
  if (code == TSDB_CODE_SUCCESS && time) code = nodesMergeNode(&scan->pConditions, &time);
  if (code == TSDB_CODE_SUCCESS && tags) code = nodesMergeNode(&scan->pConditions, &tags);
  nodesDestroyNode(time);
  nodesDestroyNode(tags);
  return code;
}

int32_t planDecorrelateScalar(SPlanContext* ctx, SLogicSubplan* outer, SLogicSubplan* inner,
                              const SCorrelatedSubqueryNode* call, bool onlySubquery, bool rewriteFilters) {
  if (nodeType(outer->pNode) != QUERY_NODE_LOGIC_PLAN_PROJECT ||
      nodeType(inner->pNode) != QUERY_NODE_LOGIC_PLAN_PROJECT) {
    return TSDB_CODE_SUCCESS;
  }
  SProjectLogicNode* proj = (SProjectLogicNode*)outer->pNode;
  SLogicNode*        child = onlyChild(outer->pNode);
  bool               sorted = child && nodeType(child) == QUERY_NODE_LOGIC_PLAN_SORT;
  SLogicNode*        parent = sorted ? child : outer->pNode;
  SLogicNode*        scan = leftScan(onlyChild(parent));
  if (!scan) {
    return TSDB_CODE_SUCCESS;
  }
  SCallSite where = {.templateId = call->templateId};
  nodesWalkExpr(scan->pConditions, inspectExpr, &where);
  if (where.hasCall != rewriteFilters) {
    return TSDB_CODE_SUCCESS;
  }
  if (where.hasCall && (where.hasUnsupportedExpr || where.hasOtherSubquery || onlyChild(parent) != scan)) {
    return TSDB_CODE_SUCCESS;
  }
  bool   inProjection = false;
  SNode* expr = NULL;
  FOREACH(expr, proj->pProjections) {
    SCallSite site = {.templateId = call->templateId};
    nodesWalkExpr(expr, inspectExpr, &site);
    if (site.hasCall) {
      if (site.hasUnsupportedExpr) {
        return TSDB_CODE_SUCCESS;
      }
      inProjection = true;
    }
  }
  if (!where.hasCall && !inProjection) {
    return TSDB_CODE_SUCCESS;
  }
  SProjectLogicNode* producer = (SProjectLogicNode*)inner->pNode;
  bool               needsRowCheck = false;
  bool               allowCount = !inProjection && canRewriteCountPredicate(scan->pConditions, call->templateId);
  if (!canGroupScalarResult(producer, allowCount, &needsRowCheck)) {
    return TSDB_CODE_SUCCESS;
  }
  if (proj->node.pLimit && (needsRowCheck || !onlySubquery)) {
    return TSDB_CODE_SUCCESS;
  }
  SScalarKeys keys = {.call = call, .outerScan = scan, .eligible = true};
  SLogicNode* inScan = onlyChild(&producer->node);
  if (!needsRowCheck) inScan = onlyChild(inScan);
  int32_t            code = extractKeys(inScan->pConditions, &keys);
  SProjectLogicNode* right = NULL;
  SProjectLogicNode* copy = NULL;
  if (code || !keys.eligible || !keys.innerKeys) goto _return;
  PLAN_ERR_JRET(nodesCloneNode((SNode*)producer, (SNode**)&right));
  PLAN_ERR_JRET(groupRight(right, &keys, needsRowCheck));
  if (hasDependency(&right->node)) goto _return;

  PLAN_ERR_JRET(nodesCloneNode((SNode*)proj, (SNode**)&copy));
  copy->node.stmtRoot = proj->node.stmtRoot;
  parent = sorted ? onlyChild(&copy->node) : &copy->node;
  scan = leftScan(onlyChild(parent));
  SCallSite replace = {.templateId = call->templateId,
                       .result = (SColumnNode*)nodesListGetNode(right->node.pTargets, 0)};
  PLAN_ERR_JRET(buildJoin(parent, &right, &keys, needsRowCheck));
  if (where.hasCall) {
    SLogicNode* stage = onlyChild(parent);
    PLAN_ERR_JRET(splitOuterFilter(scan, stage, &keys.eligible));
    if (!keys.eligible) goto _return;
    nodesRewriteExpr(&stage->pConditions, replaceCall, &replace);
    PLAN_ERR_JRET(replace.code);
  }
  if (inProjection) {
    nodesRewriteExprs(copy->pProjections, replaceCall, &replace);
    PLAN_ERR_JRET(replace.code);
    if (sorted) PLAN_ERR_JRET(appendClone(&parent->pTargets, (SNode*)replace.result));
  }
  if (containsScalarCall(&copy->node, call->templateId)) {
    goto _return;
  }
  PLAN_ERR_JRET(planFinalizeScalarRewrite(ctx, &copy->node));
  nodesDestroyNode((SNode*)outer->pNode);
  outer->pNode = &copy->node;
  copy = NULL;
  inner->pushedDown = true;

_return:
  nodesDestroyNode((SNode*)copy);
  nodesDestroyNode((SNode*)right);
  nodesDestroyNode(keys.residual);
  nodesDestroyList(keys.innerKeys);
  nodesDestroyList(keys.outerKeys);
  return code;
}
