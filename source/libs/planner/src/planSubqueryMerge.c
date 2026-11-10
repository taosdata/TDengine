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

#include <string.h>

#include "functionMgt.h"
#include "planInt.h"
#include "query.h"
#include "querynodes.h"
#include "tarray.h"

// The explicit comparators below must be audited when node layouts change.
// These 64-bit tripwires catch layout changes to existing supported node types.
// They are not a substitute for reviewing fields added within padding.
// Use typedef assertions to retain C99/MSVC and GCC 7 compatibility.
#define SUBQUERY_CHECK_LAYOUT(type, bytes) \
  typedef char SSubqueryLayoutGuard_##type[(sizeof(void*) != 8 || sizeof(type) == (bytes)) ? 1 : -1]

SUBQUERY_CHECK_LAYOUT(SDataType, 8);
SUBQUERY_CHECK_LAYOUT(SExprNode, 368);
SUBQUERY_CHECK_LAYOUT(SColumnNode, 1392);
SUBQUERY_CHECK_LAYOUT(SValueNode, 432);
SUBQUERY_CHECK_LAYOUT(SFunctionNode, 640);
SUBQUERY_CHECK_LAYOUT(STableNode, 824);
SUBQUERY_CHECK_LAYOUT(SRealTableNode, 1168);
SUBQUERY_CHECK_LAYOUT(STempTableNode, 840);
SUBQUERY_CHECK_LAYOUT(SJoinTableNode, 896);
SUBQUERY_CHECK_LAYOUT(SSelectStmt, 840);
SUBQUERY_CHECK_LAYOUT(SSetOperator, 656);
SUBQUERY_CHECK_LAYOUT(SOperatorNode, 512);
SUBQUERY_CHECK_LAYOUT(SLogicConditionNode, 384);
SUBQUERY_CHECK_LAYOUT(SGroupingSetNode, 16);
SUBQUERY_CHECK_LAYOUT(SOrderByExprNode, 24);
SUBQUERY_CHECK_LAYOUT(SLimitNode, 24);
SUBQUERY_CHECK_LAYOUT(SNodeListNode, 376);
SUBQUERY_CHECK_LAYOUT(SWhenThenNode, 384);
SUBQUERY_CHECK_LAYOUT(SCaseWhenNode, 408);
SUBQUERY_CHECK_LAYOUT(SIntervalWindowNode, 80);
SUBQUERY_CHECK_LAYOUT(SSessionWindowNode, 24);
SUBQUERY_CHECK_LAYOUT(SCountWindowNode, 40);
SUBQUERY_CHECK_LAYOUT(SFillNode, 56);
SUBQUERY_CHECK_LAYOUT(STimeRangeNode, 32);
SUBQUERY_CHECK_LAYOUT(SRangeAroundNode, 24);
SUBQUERY_CHECK_LAYOUT(SWindowOffsetNode, 24);

#undef SUBQUERY_CHECK_LAYOUT

// This comparison also checks sharing safety. Do not shortcut identical pointers:
// a volatile function or an unsupported node must never make a query shareable.
typedef struct SSubqueryCompareContext {
  const SNode*     pLeftFrom;
  const SNode*     pRightFrom;
  const SNodeList* pLeftOutput;
  const SNodeList* pRightOutput;
} SSubqueryCompareContext;

static bool subqueryNodeEqual(const SNode* a, const SNode* b, const SSubqueryCompareContext* pCtx);

#define SUBQUERY_COMPARE_SCALAR(field) \
  do {                                \
    if (a->field != b->field) {        \
      return false;                   \
    }                                 \
  } while (0)

#define SUBQUERY_COMPARE_STRING(field)    \
  do {                                   \
    if (0 != strcmp(a->field, b->field)) { \
      return false;                      \
    }                                    \
  } while (0)

#define SUBQUERY_COMPARE_NODE(field)                                          \
  do {                                                                        \
    if (!subqueryNodeEqual((const SNode*)a->field, (const SNode*)b->field, pCtx)) { \
      return false;                                                           \
    }                                                                         \
  } while (0)

#define SUBQUERY_COMPARE_LIST(field)                    \
  do {                                                 \
    if (!subqueryListEqual(a->field, b->field, pCtx)) { \
      return false;                                    \
    }                                                  \
  } while (0)

static bool subqueryListEqual(const SNodeList* a, const SNodeList* b, const SSubqueryCompareContext* pCtx) {
  if (LIST_LENGTH(a) != LIST_LENGTH(b)) {
    return false;
  }
  const SListCell* pLeft = a != NULL ? a->pHead : NULL;
  const SListCell* pRight = b != NULL ? b->pHead : NULL;
  for (; pLeft != NULL && pRight != NULL; pLeft = pLeft->pNext, pRight = pRight->pNext) {
    if (!subqueryNodeEqual(pLeft->pNode, pRight->pNode, pCtx)) {
      return false;
    }
  }
  return pLeft == NULL && pRight == NULL;
}

static bool subqueryExprEqual(const SExprNode* a, const SExprNode* b) {
  // Display aliases and generated statement names do not determine the result.
  // Tuple-function bindings and positional references do determine it.
  // asParam marks function/operator arguments, not prepared-statement values.
  return a->asParam == b->asParam && planSubqueryTypeEqual(a->resType, b->resType) &&
         a->relatedTo == b->relatedTo && a->bindExprID == b->bindExprID && a->asPosition == b->asPosition &&
         (!a->asPosition || a->projIdx == b->projIdx);
}

// A table UID identifies the source, not its occurrence in a self join. Resolve
// translated qualifiers to FROM leaves and compare their structural positions.
static const STableNode* subqueryFindColumnTable(const SNode* pFrom, const SColumnNode* pColumn,
                                                int32_t* pNextIndex, int32_t* pIndex) {
  if (pFrom == NULL) {
    return NULL;
  }
  if (nodeType(pFrom) == QUERY_NODE_JOIN_TABLE) {
    const SJoinTableNode* pJoin = (const SJoinTableNode*)pFrom;
    const STableNode* pTable = subqueryFindColumnTable(pJoin->pLeft, pColumn, pNextIndex, pIndex);
    return pTable != NULL ? pTable : subqueryFindColumnTable(pJoin->pRight, pColumn, pNextIndex, pIndex);
  }
  int32_t index = (*pNextIndex)++;
  if (nodeType(pFrom) != QUERY_NODE_REAL_TABLE && nodeType(pFrom) != QUERY_NODE_TEMP_TABLE) {
    return NULL;
  }
  const STableNode* pTable = (const STableNode*)pFrom;
  if (pColumn->tableAlias[0] == '\0' || strcmp(pColumn->tableAlias, pTable->tableAlias) != 0) {
    return NULL;
  }
  *pIndex = index;
  return pTable;
}

static const SNodeList* subqueryProjectionList(const SNode* pQuery) {
  if (pQuery != NULL && nodeType(pQuery) == QUERY_NODE_SELECT_STMT) {
    return ((const SSelectStmt*)pQuery)->pProjectionList;
  }
  if (pQuery != NULL && nodeType(pQuery) == QUERY_NODE_SET_OPERATOR) {
    return ((const SSetOperator*)pQuery)->pProjectionList;
  }
  return NULL;
}

static int32_t subqueryColumnOutputIndex(const SNodeList* pOutput, const SColumnNode* pColumn) {
  int32_t index = 0;
  int32_t found = -1;
  SNode* pNode = NULL;
  FOREACH(pNode, pOutput) {
    if (!nodesIsExprNode(pNode)) {
      return -1;
    }
    const SExprNode* pExpr = (const SExprNode*)pNode;
    bool matches = pColumn->projRefIdx > 0 ? pColumn->projRefIdx == pExpr->projIdx
                                         : strcmp(pColumn->colName, pExpr->aliasName) == 0;
    if (matches) {
      if (found >= 0) {
        // Ambiguous output names need their own binding rule.
        return -1;
      }
      found = index;
    }
    ++index;
  }
  return found;
}

static bool subqueryColumnEqual(const SColumnNode* a, const SColumnNode* b, const SSubqueryCompareContext* pCtx) {
  SUBQUERY_COMPARE_SCALAR(tableId);
  SUBQUERY_COMPARE_SCALAR(tableType);
  SUBQUERY_COMPARE_SCALAR(colId);
  SUBQUERY_COMPARE_SCALAR(colType);
  SUBQUERY_COMPARE_SCALAR(isPrimTs);
  SUBQUERY_COMPARE_SCALAR(isPk);
  SUBQUERY_COMPARE_SCALAR(tableHasPk);
  SUBQUERY_COMPARE_SCALAR(flags);
  int32_t leftNext = 0, rightNext = 0;
  int32_t leftIndex = -1, rightIndex = -1;
  const STableNode* pLeftTable =
      pCtx == NULL ? NULL : subqueryFindColumnTable(pCtx->pLeftFrom, a, &leftNext, &leftIndex);
  const STableNode* pRightTable =
      pCtx == NULL ? NULL : subqueryFindColumnTable(pCtx->pRightFrom, b, &rightNext, &rightIndex);
  if (pLeftTable != NULL || pRightTable != NULL) {
    if (pLeftTable == NULL || pRightTable == NULL || leftIndex != rightIndex ||
        nodeType(pLeftTable) != nodeType(pRightTable)) {
      return false;
    }
    if (nodeType(pLeftTable) == QUERY_NODE_TEMP_TABLE) {
      int32_t leftOutput = subqueryColumnOutputIndex(
          subqueryProjectionList(((const STempTableNode*)pLeftTable)->pSubquery), a);
      int32_t rightOutput = subqueryColumnOutputIndex(
          subqueryProjectionList(((const STempTableNode*)pRightTable)->pSubquery), b);
      return leftOutput >= 0 && leftOutput == rightOutput;
    }
  } else {
    // Unbound generated/window columns retain their existing strict comparison.
    if (a->tableAlias[0] != '\0' || b->tableAlias[0] != '\0') {
      return false;
    }
    SUBQUERY_COMPARE_STRING(tableAlias);
  }
  SUBQUERY_COMPARE_SCALAR(projRefIdx);
  SUBQUERY_COMPARE_STRING(dbName);
  SUBQUERY_COMPARE_STRING(tableName);
  SUBQUERY_COMPARE_STRING(colName);
  SUBQUERY_COMPARE_STRING(refSourceName);
  SUBQUERY_COMPARE_STRING(refDbName);
  SUBQUERY_COMPARE_STRING(refTableName);
  SUBQUERY_COMPARE_STRING(refColName);
  return true;
}

static bool subqueryValueEqual(const SValueNode* a, const SValueNode* b) {
  if (a->placeholderNo > 0 || b->placeholderNo > 0 || IS_VAL_UNSET(a->flag) || IS_VAL_UNSET(b->flag) ||
      (a->flag & VALUE_FLAG_DEFERRED_QUERY_TIME) || (b->flag & VALUE_FLAG_DEFERRED_QUERY_TIME)) {
    return false;
  }
  SUBQUERY_COMPARE_SCALAR(flag);
  SUBQUERY_COMPARE_SCALAR(isNull);
  SUBQUERY_COMPARE_SCALAR(unit);
  SUBQUERY_COMPARE_SCALAR(tz);
  SUBQUERY_COMPARE_SCALAR(charsetCxt);
  if (a->isNull) {
    return true;
  }
  switch (a->node.resType.type) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT:
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE:
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_GEOMETRY:
    case TSDB_DATA_TYPE_BLOB:
      return nodesEqualNode((const SNode*)a, (const SNode*)b);
    default:
      return false;
  }
}

static bool subqueryFunctionEqual(const SFunctionNode* a, const SFunctionNode* b, const SSubqueryCompareContext* pCtx) {
  if (fmIsUserDefinedFunc(a->funcId) || fmIsUserDefinedFunc(b->funcId) || fmIsVolatileFunc(a->funcId) ||
      fmIsVolatileFunc(b->funcId) || fmIsBlockDistFunc(a->funcId) || fmIsBlockDistFunc(b->funcId) ||
      fmIsDBUsageFunc(a->funcId) || fmIsDBUsageFunc(b->funcId) || fmIsPlaceHolderFunc(a->funcId) ||
      fmIsPlaceHolderFunc(b->funcId) || a->funcType == FUNCTION_TYPE_RAND || b->funcType == FUNCTION_TYPE_RAND ||
      a->funcType == FUNCTION_TYPE_SAMPLE || b->funcType == FUNCTION_TYPE_SAMPLE ||
      a->funcType == FUNCTION_TYPE_UDF || b->funcType == FUNCTION_TYPE_UDF ||
      a->pSrcFuncRef != NULL || b->pSrcFuncRef != NULL) {
    return false;
  }
  SUBQUERY_COMPARE_SCALAR(funcId);
  SUBQUERY_COMPARE_SCALAR(funcType);
  SUBQUERY_COMPARE_SCALAR(hasPk);
  SUBQUERY_COMPARE_SCALAR(pkBytes);
  SUBQUERY_COMPARE_SCALAR(hasOriginalFunc);
  SUBQUERY_COMPARE_SCALAR(originalFuncId);
  SUBQUERY_COMPARE_SCALAR(trimType);
  SUBQUERY_COMPARE_SCALAR(dual);
  SUBQUERY_COMPARE_SCALAR(isDistinct);
  SUBQUERY_COMPARE_SCALAR(tz);
  SUBQUERY_COMPARE_SCALAR(charsetCxt);
  SUBQUERY_COMPARE_SCALAR(firstDayOfWeek);
  SUBQUERY_COMPARE_STRING(functionName);
  SUBQUERY_COMPARE_STRING(tzName);
  SUBQUERY_COMPARE_LIST(pParameterList);
  SUBQUERY_COMPARE_NODE(pOver);
  return planSubqueryTypeEqual(a->srcFuncInputType, b->srcFuncInputType);
}

static bool subqueryTableEqual(const STableNode* a, const STableNode* b) {
  // Real sources have semantic names. Derived/join names and all table aliases
  // are local bindings; their columns are matched through the SELECT context.
  if (nodeType(a) == QUERY_NODE_REAL_TABLE) {
    SUBQUERY_COMPARE_STRING(dbName);
    SUBQUERY_COMPARE_STRING(tableName);
  }
  SUBQUERY_COMPARE_SCALAR(precision);
  SUBQUERY_COMPARE_SCALAR(singleTable);
  SUBQUERY_COMPARE_SCALAR(inJoin);
  return true;
}

static bool subqueryRealTableEqual(const SRealTableNode* a, const SRealTableNode* b) {
  // External/stream sources and TSMA alternatives need their own sharing rules.
  if (a->pExtTableNode != NULL || b->pExtTableNode != NULL || a->placeholderType != SP_NONE ||
      b->placeholderType != SP_NONE || taosArrayGetSize(a->pTsmas) > 0 || taosArrayGetSize(b->pTsmas) > 0 ||
      taosArrayGetSize(a->tsmaTargetTbInfo) > 0 || taosArrayGetSize(b->tsmaTargetTbInfo) > 0) {
    return false;
  }
  if (!subqueryTableEqual(&a->table, &b->table) || (a->pMeta == NULL) != (b->pMeta == NULL)) {
    return false;
  }
  if (a->pMeta != NULL && (a->pMeta->uid != b->pMeta->uid || a->pMeta->suid != b->pMeta->suid ||
                          a->pMeta->tableType != b->pMeta->tableType || a->pMeta->sversion != b->pMeta->sversion ||
                          a->pMeta->tversion != b->pMeta->tversion)) {
    return false;
  }
  SUBQUERY_COMPARE_STRING(qualDbName);
  SUBQUERY_COMPARE_SCALAR(ratio);
  SUBQUERY_COMPARE_SCALAR(cacheLastMode);
  SUBQUERY_COMPARE_SCALAR(stbRewrite);
  SUBQUERY_COMPARE_SCALAR(asSingleTable);
  return true;
}

static bool subqueryJoinEqual(const SJoinTableNode* a, const SJoinTableNode* b, const SSubqueryCompareContext* pCtx) {
  if (!subqueryTableEqual(&a->table, &b->table)) {
    return false;
  }
  SUBQUERY_COMPARE_SCALAR(joinType);
  SUBQUERY_COMPARE_SCALAR(subType);
  SUBQUERY_COMPARE_SCALAR(isCrossJoin);
  SUBQUERY_COMPARE_SCALAR(isLowLevelJoin);
  SUBQUERY_COMPARE_SCALAR(leftNoOrderedSubQuery);
  SUBQUERY_COMPARE_SCALAR(rightNoOrderedSubQuery);
  SUBQUERY_COMPARE_LIST(pWindowTime);
  SUBQUERY_COMPARE_NODE(pWindowOffset);
  SUBQUERY_COMPARE_NODE(pJLimit);
  SUBQUERY_COMPARE_NODE(addPrimCond);
  SUBQUERY_COMPARE_NODE(pLeft);
  SUBQUERY_COMPARE_NODE(pRight);
  SUBQUERY_COMPARE_NODE(pOnCond);
  return true;
}

static bool subquerySelectEqual(const SSelectStmt* a, const SSelectStmt* b) {
  // Each SELECT owns its bindings. Never resolve a column in an enclosing scope.
  const SSubqueryCompareContext scope = {.pLeftFrom = a->pFromTable, .pRightFrom = b->pFromTable};
  const SSubqueryCompareContext* pCtx = &scope;
  if (a->hasUdaf || b->hasUdaf || a->hasForecastFunc || b->hasForecastFunc ||
      a->hasGenericAnalysisFunc || b->hasGenericAnalysisFunc || a->pTimeRange != NULL || b->pTimeRange != NULL ||
      LIST_LENGTH(a->pHint) > 0 || LIST_LENGTH(b->pHint) > 0 || LIST_LENGTH(a->pSubQueries) > 0 ||
      LIST_LENGTH(b->pSubQueries) > 0 || a->timeRangeParamDependent || b->timeRangeParamDependent ||
      a->interpRangeParamDependent || b->interpRangeParamDependent) {
    return false;
  }
  SUBQUERY_COMPARE_SCALAR(subQType);
  // Translation has lowered quantified membership to IN/NOT IN operators.
  // The original ANY/ALL spelling does not change the produced column set;
  // consumer compatibility is checked separately before this comparison.
  if (a->subQType != E_SUB_QUERY_COLUMN) {
    SUBQUERY_COMPARE_SCALAR(quantify);
  }
  SUBQUERY_COMPARE_SCALAR(isDistinct);
  SUBQUERY_COMPARE_SCALAR(timeRange.skey);
  SUBQUERY_COMPARE_SCALAR(timeRange.ekey);
  SUBQUERY_COMPARE_SCALAR(precision);
  SUBQUERY_COMPARE_SCALAR(selectFuncNum);
  SUBQUERY_COMPARE_SCALAR(returnRows);
  SUBQUERY_COMPARE_SCALAR(timeLineCurMode);
  SUBQUERY_COMPARE_SCALAR(timeLineResMode);
  SUBQUERY_COMPARE_SCALAR(windowMode);
  SUBQUERY_COMPARE_SCALAR(lastProcessByRowFuncId);
  SUBQUERY_COMPARE_SCALAR(multiRowsFuncKParam);
  SUBQUERY_COMPARE_SCALAR(mavgFuncKParam);
  SUBQUERY_COMPARE_SCALAR(timeLineFromOrderBy);
  SUBQUERY_COMPARE_SCALAR(isEmptyResult);
  SUBQUERY_COMPARE_SCALAR(implicitPartitionHaving);
  SUBQUERY_COMPARE_SCALAR(groupSort);
  SUBQUERY_COMPARE_SCALAR(tagScan);
  SUBQUERY_COMPARE_SCALAR(windowScalarMode);
  SUBQUERY_COMPARE_SCALAR(deferredFillAllowNoTimeRange);
  SUBQUERY_COMPARE_NODE(pFromTable);
  SUBQUERY_COMPARE_LIST(pProjectionList);
  SUBQUERY_COMPARE_LIST(pProjectionBindList);
  SUBQUERY_COMPARE_NODE(pWhere);
  SUBQUERY_COMPARE_LIST(pPartitionByList);
  SUBQUERY_COMPARE_NODE(pWindow);
  SUBQUERY_COMPARE_LIST(pWindowList);
  SUBQUERY_COMPARE_LIST(pGroupByList);
  SUBQUERY_COMPARE_NODE(pHaving);
  SUBQUERY_COMPARE_NODE(pRange);
  SUBQUERY_COMPARE_NODE(pRangeAround);
  SUBQUERY_COMPARE_NODE(pEvery);
  SUBQUERY_COMPARE_NODE(pFill);
  SUBQUERY_COMPARE_LIST(pOrderByList);
  SUBQUERY_COMPARE_NODE(pLimit);
  SUBQUERY_COMPARE_NODE(pSlimit);
  return true;
}

static bool subquerySetOutputEqual(const SNode* a, const SNode* b, const char* leftStmt, const char* rightStmt,
                                   const SSubqueryCompareContext* pCtx) {
  if (a == NULL || b == NULL || nodeType(a) != nodeType(b)) {
    return a == b;
  }
  if (nodeType(a) == QUERY_NODE_COLUMN) {
    SColumnNode left = *(const SColumnNode*)a;
    SColumnNode right = *(const SColumnNode*)b;
    // UNION outputs bind to the generated statement and an output slot. Match
    // the slot before ignoring generated qualifiers or renamed output aliases.
    if (0 == strcmp(left.tableAlias, leftStmt) && 0 == strcmp(right.tableAlias, rightStmt)) {
      int32_t leftOutput = subqueryColumnOutputIndex(pCtx->pLeftOutput, &left);
      int32_t rightOutput = subqueryColumnOutputIndex(pCtx->pRightOutput, &right);
      if (leftOutput < 0 || leftOutput != rightOutput) {
        return false;
      }
      left.tableAlias[0] = '\0';
      right.tableAlias[0] = '\0';
      left.colName[0] = '\0';
      right.colName[0] = '\0';
      left.projRefIdx = 0;
      right.projRefIdx = 0;
      return subqueryNodeEqual((const SNode*)&left, (const SNode*)&right, pCtx);
    }
  } else if (nodeType(a) == QUERY_NODE_ORDER_BY_EXPR) {
    const SOrderByExprNode* pLeft = (const SOrderByExprNode*)a;
    const SOrderByExprNode* pRight = (const SOrderByExprNode*)b;
    return pLeft->order == pRight->order && pLeft->nullOrder == pRight->nullOrder &&
           subquerySetOutputEqual(pLeft->pExpr, pRight->pExpr, leftStmt, rightStmt, pCtx);
  }
  return subqueryNodeEqual(a, b, pCtx);
}

static bool subquerySetOutputListEqual(const SNodeList* a, const SNodeList* b,
                                       const char* leftStmt, const char* rightStmt,
                                       const SSubqueryCompareContext* pCtx) {
  if (LIST_LENGTH(a) != LIST_LENGTH(b)) {
    return false;
  }
  const SListCell* pLeft = a != NULL ? a->pHead : NULL;
  const SListCell* pRight = b != NULL ? b->pHead : NULL;
  for (; pLeft != NULL && pRight != NULL; pLeft = pLeft->pNext, pRight = pRight->pNext) {
    if (!subquerySetOutputEqual(pLeft->pNode, pRight->pNode, leftStmt, rightStmt, pCtx)) {
      return false;
    }
  }
  return pLeft == NULL && pRight == NULL;
}

static bool subquerySetOperatorEqual(const SSetOperator* a, const SSetOperator* b) {
  const SSubqueryCompareContext scope = {.pLeftOutput = a->pProjectionList, .pRightOutput = b->pProjectionList};
  const SSubqueryCompareContext* pCtx = &scope;
  if (LIST_LENGTH(a->pSubQueries) > 0 || LIST_LENGTH(b->pSubQueries) > 0) {
    return false;
  }
  SUBQUERY_COMPARE_SCALAR(opType);
  SUBQUERY_COMPARE_SCALAR(subQType);
  if (a->subQType != E_SUB_QUERY_COLUMN) {
    SUBQUERY_COMPARE_SCALAR(quantify);
  }
  SUBQUERY_COMPARE_SCALAR(precision);
  SUBQUERY_COMPARE_SCALAR(timeLineResMode);
  SUBQUERY_COMPARE_SCALAR(timeLineFromOrderBy);
  const int32_t numOperands = setOperatorChildCount(a);
  if (numOperands != setOperatorChildCount(b) || numOperands < 2) {
    return false;
  }
  const SListCell* pLeftCell = a->pOperands != NULL ? a->pOperands->pHead : NULL;
  const SListCell* pRightCell = b->pOperands != NULL ? b->pOperands->pHead : NULL;
  for (int32_t i = 0; i < numOperands; ++i) {
    const SNode* pLeft = pLeftCell != NULL ? pLeftCell->pNode : setOperatorChildAt(a, i);
    const SNode* pRight = pRightCell != NULL ? pRightCell->pNode : setOperatorChildAt(b, i);
    if (pLeft == NULL || pRight == NULL || !subqueryNodeEqual(pLeft, pRight, pCtx)) {
      return false;
    }
    pLeftCell = pLeftCell != NULL ? pLeftCell->pNext : NULL;
    pRightCell = pRightCell != NULL ? pRightCell->pNext : NULL;
  }
  SUBQUERY_COMPARE_NODE(pLimit);
  return subquerySetOutputListEqual(a->pProjectionList, b->pProjectionList, a->stmtName, b->stmtName, pCtx) &&
         subquerySetOutputListEqual(a->pOrderByList, b->pOrderByList, a->stmtName, b->stmtName, pCtx);
}

static bool subqueryNodeEqual(const SNode* a, const SNode* b, const SSubqueryCompareContext* pCtx) {
  if (a == NULL || b == NULL) {
    return a == b;
  }
  if (nodeType(a) != nodeType(b) ||
      (nodesIsExprNode(a) && !subqueryExprEqual((const SExprNode*)a, (const SExprNode*)b))) {
    return false;
  }
  switch (nodeType(a)) {
    case QUERY_NODE_SELECT_STMT:
      return subquerySelectEqual((const SSelectStmt*)a, (const SSelectStmt*)b);
    case QUERY_NODE_SET_OPERATOR:
      return subquerySetOperatorEqual((const SSetOperator*)a, (const SSetOperator*)b);
    case QUERY_NODE_COLUMN:
      return subqueryColumnEqual((const SColumnNode*)a, (const SColumnNode*)b, pCtx);
    case QUERY_NODE_VALUE:
      return subqueryValueEqual((const SValueNode*)a, (const SValueNode*)b);
    case QUERY_NODE_FUNCTION:
      return subqueryFunctionEqual((const SFunctionNode*)a, (const SFunctionNode*)b, pCtx);
    case QUERY_NODE_REAL_TABLE:
      return subqueryRealTableEqual((const SRealTableNode*)a, (const SRealTableNode*)b);
    case QUERY_NODE_TEMP_TABLE: {
      const STempTableNode* pLeft = (const STempTableNode*)a;
      const STempTableNode* pRight = (const STempTableNode*)b;
      return subqueryTableEqual(&pLeft->table, &pRight->table) &&
             subqueryNodeEqual(pLeft->pSubquery, pRight->pSubquery, pCtx);
    }
    case QUERY_NODE_JOIN_TABLE:
      return subqueryJoinEqual((const SJoinTableNode*)a, (const SJoinTableNode*)b, pCtx);
    case QUERY_NODE_OPERATOR: {
      const SOperatorNode* pLeft = (const SOperatorNode*)a;
      const SOperatorNode* pRight = (const SOperatorNode*)b;
      return pLeft->opType == pRight->opType && pLeft->flag == pRight->flag && pLeft->tz == pRight->tz &&
             pLeft->charsetCxt == pRight->charsetCxt && 0 == strcmp(pLeft->timezoneName, pRight->timezoneName) &&
             subqueryNodeEqual(pLeft->pLeft, pRight->pLeft, pCtx) && subqueryNodeEqual(pLeft->pRight, pRight->pRight, pCtx);
    }
    case QUERY_NODE_LOGIC_CONDITION: {
      const SLogicConditionNode* pLeft = (const SLogicConditionNode*)a;
      const SLogicConditionNode* pRight = (const SLogicConditionNode*)b;
      return pLeft->condType == pRight->condType && subqueryListEqual(pLeft->pParameterList, pRight->pParameterList, pCtx);
    }
    case QUERY_NODE_GROUPING_SET: {
      const SGroupingSetNode* pLeft = (const SGroupingSetNode*)a;
      const SGroupingSetNode* pRight = (const SGroupingSetNode*)b;
      return pLeft->groupingSetType == pRight->groupingSetType &&
             subqueryListEqual(pLeft->pParameterList, pRight->pParameterList, pCtx);
    }
    case QUERY_NODE_ORDER_BY_EXPR: {
      const SOrderByExprNode* pLeft = (const SOrderByExprNode*)a;
      const SOrderByExprNode* pRight = (const SOrderByExprNode*)b;
      return pLeft->order == pRight->order && pLeft->nullOrder == pRight->nullOrder &&
             subqueryNodeEqual(pLeft->pExpr, pRight->pExpr, pCtx);
    }
    case QUERY_NODE_LIMIT: {
      const SLimitNode* pLeft = (const SLimitNode*)a;
      const SLimitNode* pRight = (const SLimitNode*)b;
      return subqueryNodeEqual((const SNode*)pLeft->limit, (const SNode*)pRight->limit, pCtx) &&
             subqueryNodeEqual((const SNode*)pLeft->offset, (const SNode*)pRight->offset, pCtx);
    }
    case QUERY_NODE_NODE_LIST: {
      const SNodeListNode* pLeft = (const SNodeListNode*)a;
      const SNodeListNode* pRight = (const SNodeListNode*)b;
      return planSubqueryTypeEqual(pLeft->node.resType, pRight->node.resType) &&
             subqueryListEqual(pLeft->pNodeList, pRight->pNodeList, pCtx);
    }
    case QUERY_NODE_WHEN_THEN: {
      const SWhenThenNode* pLeft = (const SWhenThenNode*)a;
      const SWhenThenNode* pRight = (const SWhenThenNode*)b;
      return subqueryExprEqual(&pLeft->node, &pRight->node) && subqueryNodeEqual(pLeft->pWhen, pRight->pWhen, pCtx) &&
             subqueryNodeEqual(pLeft->pThen, pRight->pThen, pCtx);
    }
    case QUERY_NODE_CASE_WHEN: {
      const SCaseWhenNode* pLeft = (const SCaseWhenNode*)a;
      const SCaseWhenNode* pRight = (const SCaseWhenNode*)b;
      return pLeft->tz == pRight->tz && pLeft->charsetCxt == pRight->charsetCxt &&
             subqueryNodeEqual(pLeft->pCase, pRight->pCase, pCtx) && subqueryNodeEqual(pLeft->pElse, pRight->pElse, pCtx) &&
             subqueryListEqual(pLeft->pWhenThenList, pRight->pWhenThenList, pCtx);
    }
    case QUERY_NODE_INTERVAL_WINDOW: {
      const SIntervalWindowNode* pLeft = (const SIntervalWindowNode*)a;
      const SIntervalWindowNode* pRight = (const SIntervalWindowNode*)b;
      return pLeft->timeRange.skey == pRight->timeRange.skey && pLeft->timeRange.ekey == pRight->timeRange.ekey &&
             pLeft->timezone == pRight->timezone && subqueryNodeEqual(pLeft->pCol, pRight->pCol, pCtx) &&
             subqueryNodeEqual(pLeft->pInterval, pRight->pInterval, pCtx) &&
             subqueryNodeEqual(pLeft->pOffset, pRight->pOffset, pCtx) &&
             subqueryNodeEqual(pLeft->pSliding, pRight->pSliding, pCtx) &&
             subqueryNodeEqual(pLeft->pSOffset, pRight->pSOffset, pCtx) &&
             subqueryNodeEqual(pLeft->pFill, pRight->pFill, pCtx);
    }
    case QUERY_NODE_SESSION_WINDOW: {
      const SSessionWindowNode* pLeft = (const SSessionWindowNode*)a;
      const SSessionWindowNode* pRight = (const SSessionWindowNode*)b;
      return subqueryNodeEqual((const SNode*)pLeft->pCol, (const SNode*)pRight->pCol, pCtx) &&
             subqueryNodeEqual((const SNode*)pLeft->pGap, (const SNode*)pRight->pGap, pCtx);
    }
    case QUERY_NODE_COUNT_WINDOW: {
      const SCountWindowNode* pLeft = (const SCountWindowNode*)a;
      const SCountWindowNode* pRight = (const SCountWindowNode*)b;
      return pLeft->windowCount == pRight->windowCount && pLeft->windowSliding == pRight->windowSliding &&
             subqueryNodeEqual(pLeft->pCol, pRight->pCol, pCtx) && subqueryListEqual(pLeft->pColList, pRight->pColList, pCtx);
    }
    case QUERY_NODE_FILL: {
      const SFillNode* pLeft = (const SFillNode*)a;
      const SFillNode* pRight = (const SFillNode*)b;
      return pLeft->mode == pRight->mode && pLeft->timeRange.skey == pRight->timeRange.skey &&
             pLeft->timeRange.ekey == pRight->timeRange.ekey &&
             pLeft->pTimeRange == NULL && pRight->pTimeRange == NULL &&
             subqueryNodeEqual(pLeft->pValues, pRight->pValues, pCtx) &&
             subqueryNodeEqual(pLeft->pWStartTs, pRight->pWStartTs, pCtx) &&
             subqueryNodeEqual(pLeft->pSurroundingTime, pRight->pSurroundingTime, pCtx);
    }
    case QUERY_NODE_TIME_RANGE: {
      const STimeRangeNode* pLeft = (const STimeRangeNode*)a;
      const STimeRangeNode* pRight = (const STimeRangeNode*)b;
      return pLeft->needCalc == pRight->needCalc && subqueryNodeEqual(pLeft->pStart, pRight->pStart, pCtx) &&
             subqueryNodeEqual(pLeft->pEnd, pRight->pEnd, pCtx);
    }
    case QUERY_NODE_RANGE_AROUND: {
      const SRangeAroundNode* pLeft = (const SRangeAroundNode*)a;
      const SRangeAroundNode* pRight = (const SRangeAroundNode*)b;
      return subqueryNodeEqual(pLeft->pRange, pRight->pRange, pCtx) && subqueryNodeEqual(pLeft->pInterval, pRight->pInterval, pCtx);
    }
    case QUERY_NODE_WINDOW_OFFSET: {
      const SWindowOffsetNode* pLeft = (const SWindowOffsetNode*)a;
      const SWindowOffsetNode* pRight = (const SWindowOffsetNode*)b;
      return subqueryNodeEqual(pLeft->pStartOffset, pRight->pStartOffset, pCtx) &&
             subqueryNodeEqual(pLeft->pEndOffset, pRight->pEndOffset, pCtx);
    }
    default:
      // Includes remote results, correlation, virtual/external sources and
      // windows whose semantics are not covered above. Keep independent plans.
      return false;
  }
}

bool planSubqueryCanShare(const SNode* a, const SNode* b) {
  if (a == NULL || b == NULL || nodeType(a) != nodeType(b)) {
    return false;
  }
  ESubQueryType subQType;
  if (nodeType(a) == QUERY_NODE_SELECT_STMT) {
    subQType = ((const SSelectStmt*)a)->subQType;
  } else if (nodeType(a) == QUERY_NODE_SET_OPERATOR) {
    subQType = ((const SSetOperator*)a)->subQType;
  } else {
    return false;
  }
  switch (subQType) {
    case E_SUB_QUERY_SCALAR:
    case E_SUB_QUERY_COLUMN:
    case E_SUB_QUERY_ROWNUM:
    case E_SUB_QUERY_ROW:
      return subqueryNodeEqual(a, b, NULL);
    default:
      return false;
  }
}
