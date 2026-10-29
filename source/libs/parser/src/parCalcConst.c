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

#include "functionMgt.h"
#include "parInt.h"
#include "scalar.h"
#include "ttime.h"

typedef struct SNodeReplaceContext {
  SNode* pTarget;
  SNode* pNew;
  bool   replaced;
} SNodeReplaceContext;

typedef struct SCalcConstContext {
  SParseContext*      pParseCxt;
  SNodeReplaceContext replaceCxt;
  SMsgBuf             msgBuf;
  int32_t             code;
  SNodeList*          pSubQueries;
  int32_t*            pSubqueryMap;
  // Preserve timestamp projections while folding nested temporal JOIN inputs;
  // the planner selects the effective keys and prunes unused candidates.
  bool                keepTemporalCandidates;
  bool                tagScan;
} SCalcConstContext;

static int32_t calcConstQuery(SCalcConstContext* pCxt, SNode* pStmt, bool subquery);
static bool isEmptyResultQuery(SNode* pStmt);
static bool emptySubquery(SCalcConstContext* pCxt, SNode* pNode);
static EDealRes rewriteEmptySubquery(SNode** ppNode, void* pContext);
static EDealRes replaceSubqueryValue(SCalcConstContext* pCxt, SNode** ppNode,
                                    bool isNull, int64_t value);

static int32_t calcConstSubquery(SCalcConstContext* pCxt, STempTableNode* pTempTable) {
  return calcConstQuery(pCxt, pTempTable->pSubquery, true);
}

static int32_t calcConstNode(SNode** pNode) {
  if (NULL == *pNode) {
    return TSDB_CODE_SUCCESS;
  }

  SNode*  pNew = NULL;
  int32_t code = scalarCalculateConstants(*pNode, &pNew);
  if (TSDB_CODE_SUCCESS == code) {
    *pNode = pNew;
  }
  return code;
}

static int32_t calcConstList(SNodeList* pList) {
  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    SNode*  pNew = NULL;
    int32_t code = scalarCalculateConstants(pNode, &pNew);
    if (TSDB_CODE_SUCCESS == code) {
      REPLACE_NODE(pNew);
    } else {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static bool isCondition(const SNode* pNode) {
  if (QUERY_NODE_OPERATOR == nodeType(pNode)) {
    return nodesIsComparisonOp((const SOperatorNode*)pNode);
  }
  return (QUERY_NODE_LOGIC_CONDITION == nodeType(pNode));
}

static int32_t rewriteIsTrue(SNode* pSrc, SNode** pIsTrue) {
  SOperatorNode* pOp = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pOp);
  if (NULL == pOp) {
    return code;
  }
  pOp->opType = OP_TYPE_IS_TRUE;
  pOp->pLeft = pSrc;
  pOp->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
  *pIsTrue = (SNode*)pOp;
  return TSDB_CODE_SUCCESS;
}

static EDealRes doRewriteCondition(SNode** pNode, void* pContext) {
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(*pNode)) {
    SNode* pParam = NULL;
    FOREACH(pParam, ((SLogicConditionNode*)*pNode)->pParameterList) {
      if (!isCondition(pParam)) {
        SNode* pIsTrue = NULL;
        if (TSDB_CODE_SUCCESS != rewriteIsTrue(pParam, &pIsTrue)) {
          ((SCalcConstContext*)pContext)->code = TSDB_CODE_OUT_OF_MEMORY;
          return DEAL_RES_ERROR;
        }
        REPLACE_NODE(pIsTrue);
      }
    }
  }
  return DEAL_RES_CONTINUE;
}

static int32_t rewriteCondition(SCalcConstContext* pCxt, SNode** pNode) {
  if (!isCondition(*pNode)) {
    return rewriteIsTrue(*pNode, pNode);
  }
  nodesRewriteExprPostOrder(pNode, doRewriteCondition, pCxt);
  return pCxt->code;
}

static int32_t calcConstCondition(SCalcConstContext* pCxt, SNode** pNode) {
  int32_t code = rewriteCondition(pCxt, pNode);
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstNode(pNode);
  }
  return code;
}

static EDealRes rewriteCalcConstValue(SNode** pNode, void* pContext) {
  int32_t                code = TSDB_CODE_SUCCESS;
  int32_t                lino = 0;
  SCalcConstContext* pCtx = (SCalcConstContext*)pContext;

  if (QUERY_NODE_LOGIC_CONDITION == nodeType(*pNode)) {
    return DEAL_RES_CONTINUE;
  } else if (QUERY_NODE_OPERATOR == nodeType(*pNode)) {
    SOperatorNode* pOp = (SOperatorNode*)*pNode;
    if (OP_TYPE_EQUAL == pOp->opType && (TSDB_DATA_TYPE_TIMESTAMP == ((SExprNode*)pOp->pLeft)->resType.type || TSDB_DATA_TYPE_TIMESTAMP == ((SExprNode*)pOp->pRight)->resType.type)) {
      code = calcConstNode(&pOp->pLeft);
      if (TSDB_CODE_SUCCESS == code) {
        code = calcConstNode(&pOp->pRight);
      }
      
      goto _end;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstCondition(pCtx, pNode);
  }
  
_end:

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    return DEAL_RES_ERROR;
  }
  
  return DEAL_RES_IGNORE_CHILD;
}

static bool calcConstInputContainsJoin(SNode* pTable) {
  if (QUERY_NODE_JOIN_TABLE == nodeType(pTable)) return true;
  if (QUERY_NODE_TEMP_TABLE != nodeType(pTable)) return false;
  SNode* pSubquery = ((STempTableNode*)pTable)->pSubquery;
  if (QUERY_NODE_SELECT_STMT == nodeType(pSubquery)) {
    return ((SSelectStmt*)pSubquery)->joinContains;
  }
  if (QUERY_NODE_SET_OPERATOR == nodeType(pSubquery)) {
    return ((SSetOperator*)pSubquery)->joinContains;
  }
  return false;
}

static int32_t rewriteConditionForFromTable(SCalcConstContext* pCxt, SNode* pTable) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (nodeType(pTable)) {
    case QUERY_NODE_TEMP_TABLE: {
      code = calcConstSubquery(pCxt, (STempTableNode*)pTable);
      break;
    }
    case QUERY_NODE_JOIN_TABLE: {
      SJoinTableNode* pJoin = (SJoinTableNode*)pTable;
      SNode* pCond = NULL;
      bool keepCandidates = pCxt->keepTemporalCandidates;
      if ((IS_ASOF_JOIN(pJoin->subType) || IS_WINDOW_JOIN(pJoin->subType)) &&
          (calcConstInputContainsJoin(pJoin->pLeft) ||
           calcConstInputContainsJoin(pJoin->pRight))) {
        pCxt->keepTemporalCandidates = true;
      }
      code = rewriteConditionForFromTable(pCxt, pJoin->pLeft);
      if (TSDB_CODE_SUCCESS == code) {
        code = rewriteConditionForFromTable(pCxt, pJoin->pRight);
      }
      pCxt->keepTemporalCandidates = keepCandidates;
      if (TSDB_CODE_SUCCESS == code && NULL != pJoin->pOnCond) {
        code = rewriteCondition(pCxt, &pJoin->pOnCond);
      }
      if (TSDB_CODE_SUCCESS == code && NULL != pJoin->pOnCond) {
        nodesRewriteExpr(&pJoin->pOnCond, rewriteCalcConstValue, pCxt);
      }

/*
      if (TSDB_CODE_SUCCESS == code && NULL != pJoin->pOnCond) {
        code = nodesCloneNode(pJoin->pOnCond, &pCond);
      }
      if (TSDB_CODE_SUCCESS == code && NULL != pJoin->pOnCond) {
        code = calcConstCondition(pCxt, &pJoin->pOnCond);
      }
      if (TSDB_CODE_SUCCESS == code && pJoin->pOnCond && QUERY_NODE_VALUE == nodeType(pJoin->pOnCond)) {
        nodesDestroyNode(pJoin->pOnCond);
        pJoin->pOnCond = pCond;
        pCond = NULL;
      }
      nodesDestroyNode(pCond);
*/
      // todo empty table
      break;
    }
    default:
      break;
  }
  return code;
}

static int32_t calcConstFromTable(SCalcConstContext* pCxt, SNode* pTable) {
  return rewriteConditionForFromTable(pCxt, pTable);
}

static bool conditionCannotBeTrue(SNode* pCond) {
  if (QUERY_NODE_VALUE == nodeType(pCond)) {
    SValueNode* pValue = (SValueNode*)pCond;
    if (!pValue->translate ||
        (pValue->flag & (VALUE_FLAG_VAL_UNSET | VALUE_FLAG_DEFERRED_QUERY_TIME))) {
      return false;
    }
    return pValue->isNull ||
           (TSDB_DATA_TYPE_BOOL == pValue->node.resType.type && !pValue->datum.b);
  }
  if (QUERY_NODE_LOGIC_CONDITION != nodeType(pCond)) {
    return false;
  }
  SLogicConditionNode* pLogic = (SLogicConditionNode*)pCond;
  if (LOGIC_COND_TYPE_NOT == pLogic->condType) {
    return false;
  }
  bool isAnd = LOGIC_COND_TYPE_AND == pLogic->condType;
  SNode* pParam = NULL;
  FOREACH(pParam, pLogic->pParameterList) {
    if (conditionCannotBeTrue(pParam) == isAnd) {
      return isAnd;
    }
  }
  return !isAnd;
}

static void rewriteConstCondition(SNode** pCond, bool* pAlwaysFalse) {
  if (QUERY_NODE_VALUE != nodeType(*pCond)) {
    // A filter rejects both FALSE and NULL; keep their distinction in the AST.
    *pAlwaysFalse = *pAlwaysFalse ||
                    (nodesExprHasCorrelation(*pCond) && conditionCannotBeTrue(*pCond));
    return;
  }
  SValueNode* pValue = (SValueNode*)*pCond;
  if (!pValue->isNull && pValue->datum.b) {
    nodesDestroyNode(*pCond);
    *pCond = NULL;
  } else {
    *pAlwaysFalse = true;
  }
}

static int32_t calcConstCorrelatedCondition(SCalcConstContext* pCxt, SNode** pCond) {
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(*pCond)) {
    SLogicConditionNode* pLogic = (SLogicConditionNode*)*pCond;
    bool hasConstantResult = false;
    bool constantResult = false;
    SNode* pParam = NULL;
    FOREACH(pParam, pLogic->pParameterList) {
      int32_t code = calcConstCorrelatedCondition(pCxt, &cell->pNode);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      pParam = cell->pNode;
      if (QUERY_NODE_VALUE != nodeType(pParam) ||
          !((SValueNode*)pParam)->translate ||
          (((SValueNode*)pParam)->flag &
           (VALUE_FLAG_VAL_UNSET | VALUE_FLAG_DEFERRED_QUERY_TIME)) ||
          ((SValueNode*)pParam)->isNull ||
          TSDB_DATA_TYPE_BOOL != ((SValueNode*)pParam)->node.resType.type) {
        continue;
      }
      bool value = ((SValueNode*)pParam)->datum.b;
      if ((LOGIC_COND_TYPE_AND == pLogic->condType && !value) ||
          (LOGIC_COND_TYPE_OR == pLogic->condType && value)) {
        hasConstantResult = true;
        constantResult = value;
      }
    }
    if (hasConstantResult) {
      replaceSubqueryValue(pCxt, pCond, false, constantResult);
      return pCxt->code;
    }
  }
  // The scalar engine deliberately skips expressions with unbound parameters.
  // Fold independent conditions without evaluating those parameters.
  return calcConstNode(pCond);
}

static int32_t calcConstStmtCondition(SCalcConstContext* pCxt, SNode** pCond, bool* pAlwaysFalse) {
  if (NULL == *pCond) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = rewriteCondition(pCxt, pCond);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesExprHasCorrelation(*pCond) ? calcConstCorrelatedCondition(pCxt, pCond)
                                         : calcConstNode(pCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    rewriteConstCondition(pCond, pAlwaysFalse);
  }
  return code;
}

static EDealRes doFindAndReplaceNode(SNode** pNode, void* pContext) {
  SCalcConstContext* pCxt = pContext;
  if (pCxt->replaceCxt.pTarget == *pNode) {
    char aliasName[TSDB_COL_NAME_LEN] = {0};
    tstrncpy(aliasName, ((SExprNode*)*pNode)->aliasName, TSDB_COL_NAME_LEN);
    nodesDestroyNode(*pNode);
    *pNode = NULL;
    pCxt->code = nodesCloneNode(pCxt->replaceCxt.pNew, pNode);
    if (NULL == *pNode) {
      return DEAL_RES_ERROR;
    }
    tstrncpy(((SExprNode*)*pNode)->aliasName, aliasName, TSDB_COL_NAME_LEN);

    pCxt->replaceCxt.replaced = true;
    return DEAL_RES_END;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t findAndReplaceNode(SCalcConstContext* pCxt, SNode** pRoot, SNode* pTarget, SNode* pNew, bool strict) {
  pCxt->replaceCxt.pNew = pNew;
  pCxt->replaceCxt.pTarget = pTarget;

  nodesRewriteExprPostOrder(pRoot, doFindAndReplaceNode, pCxt);
  if (TSDB_CODE_SUCCESS == pCxt->code && strict && !pCxt->replaceCxt.replaced) {
    parserError("target replace node not found, %p", pTarget);
    return TSDB_CODE_PAR_INTERNAL_ERROR;
  }
  return pCxt->code;
}

static int32_t calcConstProject(SCalcConstContext* pCxt, SNode* pProject, bool dual, SNode** pNew) {
  SArray* pAssociation = NULL;
  
  if (NULL != ((SExprNode*)pProject)->pAssociation) {
    pAssociation = taosArrayDup(((SExprNode*)pProject)->pAssociation, NULL);
    if (NULL == pAssociation) {
      return terrno;
    }
  }

  char    aliasName[TSDB_COL_NAME_LEN] = {0}, srcTable[TSDB_TABLE_NAME_LEN] = {0};
  int32_t code = TSDB_CODE_SUCCESS;
  if (dual) {
    code = scalarCalculateConstantsFromDual(pProject, pNew);
  } else {
    code = scalarCalculateConstants(pProject, pNew);
  }
  if (TSDB_CODE_SUCCESS == code) {
    if (QUERY_NODE_VALUE == nodeType(*pNew) && NULL != pAssociation) {
      int32_t size = taosArrayGetSize(pAssociation);
      for (int32_t i = 0; i < size; ++i) {
        SAssociationNode* pAssNode = taosArrayGet(pAssociation, i);
        SNode**           pCol = pAssNode->pPlace;
        if (((SExprNode*)pAssNode->pAssociationNode)->joinSrc) {
          //((SExprNode*)pAssNode->pAssociationNode)->constValue = true;
          continue;
        }
        
        if (*pCol == pAssNode->pAssociationNode) {
          tstrncpy(aliasName, ((SExprNode*)*pCol)->aliasName, TSDB_COL_NAME_LEN);
          if (QUERY_NODE_COLUMN == nodeType(*pCol)) {
            tstrncpy(srcTable, ((SColumnNode*)*pCol)->tableAlias, TSDB_TABLE_NAME_LEN);
          }
          SArray* pOrigAss = NULL;
          TSWAP(((SExprNode*)*pCol)->pAssociation, pOrigAss);
          nodesDestroyNode(*pCol);
          *pCol = NULL;
          code = nodesCloneNode(*pNew, pCol);
          if (TSDB_CODE_SUCCESS == code) {
            tstrncpy(((SExprNode*)*pCol)->aliasName, aliasName, TSDB_COL_NAME_LEN);
            if (srcTable[0]) {
              tstrncpy(((SExprNode*)*pCol)->srcTable, srcTable, TSDB_TABLE_NAME_LEN);
            }
            TSWAP(pOrigAss, ((SExprNode*)*pCol)->pAssociation);
          }
          taosArrayDestroy(pOrigAss);
          if (TSDB_CODE_SUCCESS != code) {
            break;
          }
        } else {
          code = findAndReplaceNode(pCxt, pCol, pAssNode->pAssociationNode, *pNew, true);
          if (TSDB_CODE_SUCCESS != code) {
            break;
          }
        }
      }
    }
  }
  taosArrayDestroy(pAssociation);
  return code;
}

typedef struct SIsUselessColCtx {
  bool isUseless;
} SIsUselessColCtx;

EDealRes checkUselessCol(SNode* pNode, void* pContext) {
  SIsUselessColCtx* ctx = (SIsUselessColCtx*)pContext;
  if (QUERY_NODE_FUNCTION == nodeType(pNode) &&
      !fmIsScalarFunc(((SFunctionNode*)pNode)->funcId) &&
      !fmIsPseudoColumnFunc(((SFunctionNode*)pNode)->funcId)) {
    ctx->isUseless = false;
    return DEAL_RES_END;
  }

  return DEAL_RES_CONTINUE;
}

static bool isUselessCol(SExprNode* pProj) {
  SIsUselessColCtx ctx = {.isUseless = true};
  nodesWalkExpr((SNode*)pProj, checkUselessCol, (void*)&ctx);
  if (!ctx.isUseless) {
    return false;
  }
  return NULL == ((SExprNode*)pProj)->pAssociation;
}

static int32_t createConstantValue(SValueNode** ppNode) {
  SValueNode* pVal = NULL;
  int32_t     code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pVal);
  if (NULL == pVal) {
    return code;
  }
  pVal->node.resType.type = TSDB_DATA_TYPE_INT;
  pVal->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_INT].bytes;
  const int32_t val = 1;
  code = nodesSetValueNodeValue(pVal, (void*)&val);
  if (TSDB_CODE_SUCCESS == code) {
    pVal->translate = true;
    *ppNode = pVal;
  } else {
    nodesDestroyNode((SNode*)pVal);
  }
  return code;
}

static int32_t calcConstProjections(SCalcConstContext* pCxt, SSelectStmt* pSelect, bool subquery) {
  SNode* pProj = NULL;
  WHERE_EACH(pProj, pSelect->pProjectionList) {
    // JOIN time candidates are chosen after constant folding, in public order.
    bool keepTime = pCxt->keepTemporalCandidates &&
                    TSDB_DATA_TYPE_TIMESTAMP ==
                        ((SExprNode*)pProj)->resType.type;
    if (subquery && !keepTime && !pSelect->isDistinct && !pSelect->tagScan &&
        isUselessCol((SExprNode*)pProj) && pSelect->pFromTable &&
        QUERY_NODE_VIRTUAL_TABLE != nodeType(pSelect->pFromTable)) {
      ERASE_NODE(pSelect->pProjectionList);
      continue;
    }
    SNode*  pNew = NULL;
    int32_t code = calcConstProject(pCxt, pProj, (NULL == pSelect->pFromTable), &pNew);
    if (TSDB_CODE_SUCCESS == code) {
      REPLACE_NODE(pNew);
    } else {
      return code;
    }
    WHERE_NEXT;
  }
  if (0 == LIST_LENGTH(pSelect->pProjectionList)) {
    SValueNode* pVal = NULL;
    int32_t     code = createConstantValue(&pVal);
    if (TSDB_CODE_SUCCESS == code) {
      return nodesListStrictAppend(pSelect->pProjectionList, (SNode*)pVal);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t calcConstGroupBy(SCalcConstContext* pCxt, SSelectStmt* pSelect) {
  int32_t code = calcConstList(pSelect->pGroupByList);
  if (TSDB_CODE_SUCCESS == code) {
    SNode* pNode = NULL;
    FOREACH(pNode, pSelect->pGroupByList) {
      bool   hasNotValue = false;
      SNode* pGroupPara = NULL;
      FOREACH(pGroupPara, ((SGroupingSetNode*)pNode)->pParameterList) {
        if (QUERY_NODE_VALUE != nodeType(pGroupPara)) {
          hasNotValue = true;
          break;
        }
      }
      if (!hasNotValue) {
        if (pSelect->hasAggFuncs) {
          ERASE_NODE(pSelect->pGroupByList);
        } else {
          if (!cell->pPrev && !cell->pNext) continue;
          ERASE_NODE(pSelect->pGroupByList);
        }
      }
    }
  }
  return code;
}

static int32_t calcConstSelectWithoutFrom(SCalcConstContext* pCxt, SSelectStmt* pSelect, bool subquery) {
  int32_t code = calcConstProjections(pCxt, pSelect, subquery);
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstStmtCondition(pCxt, &pSelect->pWhere,
                                  &pSelect->isEmptyResult);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstStmtCondition(pCxt, &pSelect->pHaving,
                                  &pSelect->isEmptyResult);
  }
  return code;
}

static EDealRes findCountLikeFunction(SNode* pNode, void* pContext) {
  if (QUERY_NODE_FUNCTION == nodeType(pNode)) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    int32_t funcId = pFunc->hasOriginalFunc ? pFunc->originalFuncId
                                          : pFunc->funcId;
    if (fmIsCountLikeFunc(funcId)) {
      *(bool*)pContext = true;
      return DEAL_RES_END;
    }
  }
  return DEAL_RES_CONTINUE;
}

static bool countMayReturnEmptyInputRow(SSelectStmt* pSelect) {
  if (NULL != pSelect->pGroupByList || NULL != pSelect->pPartitionByList ||
      NULL != pSelect->pWindow || pSelect->implicitPartitionHaving) {
    return false;
  }
  bool hasCount = pSelect->hasCountFunc;
  nodesWalkSelectStmt(pSelect, SQL_CLAUSE_FROM, findCountLikeFunction,
                      &hasCount);
  return hasCount;
}

static int32_t calcConstSelectFrom(SCalcConstContext* pCxt, SSelectStmt* pSelect, bool subquery) {
  int32_t code = calcConstFromTable(pCxt, pSelect->pFromTable);
  bool emptyInput = pSelect->isEmptyResult;
  bool emptyFromSubquery = false;
  if (TSDB_CODE_SUCCESS == code &&
      QUERY_NODE_TEMP_TABLE == nodeType(pSelect->pFromTable)) {
    // Semantic analysis may have propagated an empty-input flag from COUNT.
    // Recheck the child's final output after folding it.
    emptyFromSubquery = isEmptyResultQuery(
        ((STempTableNode*)pSelect->pFromTable)->pSubquery);
    emptyInput = pSelect->timeRange.skey > pSelect->timeRange.ekey;
  }
  if (pSelect->mixSysTableAndActualTable) {
    return code;
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstProjections(pCxt, pSelect, subquery);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstStmtCondition(pCxt, &pSelect->pWhere, &emptyInput);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstList(pSelect->pPartitionByList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstNode(&pSelect->pWindow);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstGroupBy(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    bool countReturnsRow = countMayReturnEmptyInputRow(pSelect);
    bool topLevel = !pSelect->isSubquery && E_SUB_QUERY_NOT_SET == pSelect->subQType;
    // COUNT over an empty derived table still returns a row. Preserve the
    // top-level empty-result shortcut for the query's own empty conditions.
    pSelect->isEmptyResult = (emptyFromSubquery && !countReturnsRow) ||
                            (emptyInput && (topLevel || !countReturnsRow));
    code = calcConstStmtCondition(pCxt, &pSelect->pHaving, &pSelect->isEmptyResult);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstList(pSelect->pOrderByList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstNode(&pSelect->pFill);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstNode(&pSelect->pTimeRange);
  }
  return code;
}


static bool isZeroLimit(SLimitNode* pLimit) {
  return NULL != pLimit && NULL != pLimit->limit &&
         QUERY_NODE_VALUE == nodeType(pLimit->limit) &&
         pLimit->limit->translate && !pLimit->limit->isNull &&
         0 == pLimit->limit->datum.i;
}

static bool isEmptyResultQuery(SNode* pStmt) {
  if (NULL == pStmt) {
    return false;
  }
  bool isEmptyResult = false;
  switch (nodeType(pStmt)) {
    case QUERY_NODE_SELECT_STMT: {
      SSelectStmt* pSelect = (SSelectStmt*)pStmt;
      isEmptyResult = pSelect->isEmptyResult || isZeroLimit(pSelect->pLimit) ||
                      isZeroLimit(pSelect->pSlimit);
      break;
    }
    case QUERY_NODE_EXPLAIN_STMT:
      // Newly folded empty inputs still need an EXPLAIN plan. Preexisting
      // empty-result decisions are preserved separately in calculateConstant.
      break;
    case QUERY_NODE_SET_OPERATOR: {
      SSetOperator* pSetOp = (SSetOperator*)pStmt;
      if (isZeroLimit((SLimitNode*)pSetOp->pLimit)) {
        isEmptyResult = true;
        break;
      }
      int32_t childCount = setOperatorChildCount(pSetOp);
      if (childCount > 0) {
        isEmptyResult = true;
        for (int32_t i = 0; i < childCount; ++i) {
          if (!isEmptyResultQuery(setOperatorChildAt(pSetOp, i))) {
            isEmptyResult = false;
            break;
          }
        }
      }
      break;
    }
    case QUERY_NODE_DELETE_STMT:
      isEmptyResult = ((SDeleteStmt*)pStmt)->deleteZeroRows;
      break;
    default:
      break;
  }
  return isEmptyResult;
}

static int32_t calcSubQueries(SCalcConstContext* pCxt, SNodeList* pSubQueries) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode* pNode = NULL;
  FOREACH(pNode, pSubQueries) {
    code = calcConstQuery(pCxt, pNode, false);
    if (code) {
      break;
    }
  }

  return code;
}

static int32_t calcConstSelect(SCalcConstContext* pCxt, SSelectStmt* pSelect, bool subquery) {
  int32_t code = TSDB_CODE_SUCCESS;
  // The shared list is ordered with dependencies before their users.
  code = calcSubQueries(pCxt, pSelect->pSubQueries);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  bool tagScan = pCxt->tagScan;
  pCxt->tagScan = pSelect->tagScan;
  nodesRewriteSelectStmt(pSelect, SQL_CLAUSE_FROM, rewriteEmptySubquery, pCxt);
  pCxt->tagScan = tagScan;
  if (TSDB_CODE_SUCCESS != pCxt->code) {
    return pCxt->code;
  }

  if (NULL == pSelect->pFromTable) {
    code = calcConstSelectWithoutFrom(pCxt, pSelect, subquery);
  } else {
    code = calcConstSelectFrom(pCxt, pSelect, subquery);
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pSelect->pWindow &&
      QUERY_NODE_EXTERNAL_WINDOW == nodeType(pSelect->pWindow)) {
    SNode* pBoundaries = ((SExternalWindowNode*)pSelect->pWindow)->pSubquery;
    if (NULL != pBoundaries && QUERY_NODE_REMOTE_TABLE == nodeType(pBoundaries) &&
        emptySubquery(pCxt, pBoundaries)) {
      // Empty boundaries create no windows, including for COUNT aggregates.
      pSelect->isEmptyResult = true;
    }
    code = pCxt->code;
  }

  if (TSDB_CODE_SUCCESS == code &&
      (isZeroLimit(pSelect->pLimit) || isZeroLimit(pSelect->pSlimit))) {
    pSelect->isEmptyResult = true;
  }

  return code;
}

static int32_t calcConstDelete(SCalcConstContext* pCxt, SDeleteStmt* pDelete) {
  int32_t code = calcConstFromTable(pCxt, pDelete->pFromTable);
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstStmtCondition(pCxt, &pDelete->pWhere, &pDelete->deleteZeroRows);
  }
  if (code == TSDB_CODE_SUCCESS && pDelete->timeRange.skey > pDelete->timeRange.ekey) {
    pDelete->deleteZeroRows = true;
  }
  return code;
}

static int32_t calcConstInsert(SCalcConstContext* pCxt, SInsertStmt* pInsert) {
  int32_t code = calcConstFromTable(pCxt, pInsert->pTable);
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstQuery(pCxt, pInsert->pQuery, false);
  }
  return code;
}

static SNodeList* getChildProjection(SNode* pStmt) {
  switch (nodeType(pStmt)) {
    case QUERY_NODE_SELECT_STMT:
      return ((SSelectStmt*)pStmt)->pProjectionList;
    case QUERY_NODE_SET_OPERATOR:
      return ((SSetOperator*)pStmt)->pProjectionList;
    default:
      break;
  }
  return NULL;
}

static void eraseSetOpChildProjection(SSetOperator* pSetOp, int32_t index) {
  for (int32_t i = 0; i < setOperatorChildCount(pSetOp); ++i) {
    SNode* pOperand = setOperatorChildAt(pSetOp, i);
    SNodeList* pProjs = getChildProjection(pOperand);
    if (pProjs == NULL) continue;
    SListCell* pCell = nodesListGetCell(pProjs, index);
    if (pCell != NULL) (void)nodesListErase(pProjs, pCell);
    if (QUERY_NODE_SET_OPERATOR == nodeType(pOperand)) {
      eraseSetOpChildProjection((SSetOperator*)pOperand, index);
    }
  }
}

typedef struct SNotRefByOrderByCxt {
  SColumnNode* pCol;
  bool         hasThisCol;
} SNotRefByOrderByCxt;

static EDealRes notRefByOrderByImpl(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    SNotRefByOrderByCxt* pCxt = (SNotRefByOrderByCxt*)pContext;
    if (nodesEqualNode((SNode*)pCxt->pCol, pNode)) {
      pCxt->hasThisCol = true;
      return DEAL_RES_END;
    }
  }
  return DEAL_RES_CONTINUE;
}

static bool notRefByOrderBy(SColumnNode* pCol, SNodeList* pOrderByList) {
  SNotRefByOrderByCxt cxt = {.pCol = pCol, .hasThisCol = false};
  nodesWalkExprs(pOrderByList, notRefByOrderByImpl, &cxt);
  return !cxt.hasThisCol;
}

/*
 * @brief Check whether the projection column at `index` can be treated as
 * useless for a subquery output. Rules:
 *   SELECT: useless only when the statement is not DISTINCT and the indexed
 *     projection is useless.
 *   SET_OPERATOR:
 *     UNION (distinct): never useless, because output depends on all
 *       projected columns.
 *     UNION ALL: useless only when both left and right branches are useless
 *       at the same index (checked recursively).
 * @param pStmt subquery node, expected to be SELECT or SET_OPERATOR.
 * @param index zero-based projection position to check.
 */
static bool isSubqueryProjUseless(SNode* pStmt, int32_t index) {
  if (NULL == pStmt || index < 0) {
    return false;
  }

  switch (nodeType(pStmt)) {
    case QUERY_NODE_SELECT_STMT: {
      SSelectStmt* pSelect = (SSelectStmt*)pStmt;
      if (pSelect->isDistinct) {
        return false;
      }
      SNode* pProj = nodesListGetNode(pSelect->pProjectionList, index);
      return (NULL != pProj) && isUselessCol((SExprNode*)pProj);
    }
    case QUERY_NODE_SET_OPERATOR: {
      SSetOperator* pSetOp = (SSetOperator*)pStmt;
      /*
       * UNION (distinct) depends on all output columns,
       * projection pruning is unsafe here.
       */
      if (pSetOp->opType == SET_OP_TYPE_UNION) {
        return false;
      }
      for (int32_t i = 0; i < setOperatorChildCount(pSetOp); ++i) {
        if (!isSubqueryProjUseless(setOperatorChildAt(pSetOp, i), index)) return false;
      }
      return true;
    }
    default:
      break;
  }
  return false;
}

static bool isSetUselessCol(SSetOperator* pSetOp, int32_t index, SExprNode* pProj) {
  if (!isUselessCol(pProj)) {
    return false;
  }

  for (int32_t i = 0; i < setOperatorChildCount(pSetOp); ++i) {
    if (!isSubqueryProjUseless(setOperatorChildAt(pSetOp, i), index)) return false;
  }
  return true;
}

static int32_t calcConstSetOpProjections(SCalcConstContext* pCxt, SSetOperator* pSetOp, bool subquery) {
  if (subquery && pSetOp->opType == SET_OP_TYPE_UNION) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t index = 0;
  SNode*  pProj = NULL;
  WHERE_EACH(pProj, pSetOp->pProjectionList) {
    bool keepTime = pCxt->keepTemporalCandidates &&
                    TSDB_DATA_TYPE_TIMESTAMP ==
                        ((SExprNode*)pProj)->resType.type;
    if (subquery && !keepTime &&
        notRefByOrderBy((SColumnNode*)pProj, pSetOp->pOrderByList) &&
        isSetUselessCol(pSetOp, index, (SExprNode*)pProj)) {
      ERASE_NODE(pSetOp->pProjectionList);
      eraseSetOpChildProjection(pSetOp, index);
      continue;
    }
    ++index;
    WHERE_NEXT;
  }
  if (0 == LIST_LENGTH(pSetOp->pProjectionList)) {
    SValueNode* pVal = NULL;
    int32_t     code = createConstantValue(&pVal);
    if (TSDB_CODE_SUCCESS == code) {
      return nodesListStrictAppend(pSetOp->pProjectionList, (SNode*)pVal);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t calcConstSetOperator(SCalcConstContext* pCxt, SSetOperator* pSetOp, bool subquery) {
  int32_t code = calcSubQueries(pCxt, pSetOp->pSubQueries);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  nodesRewriteExprs(pSetOp->pProjectionList, rewriteEmptySubquery, pCxt);
  nodesRewriteExprs(pSetOp->pOrderByList, rewriteEmptySubquery, pCxt);
  if (TSDB_CODE_SUCCESS != pCxt->code) {
    return pCxt->code;
  }
  code = calcConstSetOpProjections(pCxt, pSetOp, subquery);
  for (int32_t i = 0; TSDB_CODE_SUCCESS == code && i < setOperatorChildCount(pSetOp); ++i) {
    code = calcConstQuery(pCxt, setOperatorChildAt(pSetOp, i), false);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstList(pSetOp->pOrderByList);
  }
  return code;
}

static int32_t calcConstQuery(SCalcConstContext* pCxt, SNode* pStmt, bool subquery) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (nodeType(pStmt)) {
    case QUERY_NODE_SELECT_STMT:
      code = calcConstSelect(pCxt, (SSelectStmt*)pStmt, subquery);
      break;
    case QUERY_NODE_EXPLAIN_STMT:
      code = calcConstQuery(pCxt, ((SExplainStmt*)pStmt)->pQuery, subquery);
      break;
    case QUERY_NODE_SET_OPERATOR: {
      code = calcConstSetOperator(pCxt, (SSetOperator*)pStmt, subquery);
      break;
    }
    case QUERY_NODE_DELETE_STMT:
      code = calcConstDelete(pCxt, (SDeleteStmt*)pStmt);
      break;
    case QUERY_NODE_INSERT_STMT:
      code = calcConstInsert(pCxt, (SInsertStmt*)pStmt);
      break;
    default:
      break;
  }
  return code;
}

static void replaceSetOpWithChild(SNode** ppStmt, SSetOperator* pSetOp, SNode* pKeep, SNode* pDrop) {
  SNode* pKeepProj = NULL;
  SNode* pSetOpProj = NULL;
  FORBOTH(pKeepProj, getChildProjection(pKeep), pSetOpProj, pSetOp->pProjectionList) {
    tstrncpy(((SExprNode*)pKeepProj)->aliasName, ((SExprNode*)pSetOpProj)->aliasName, TSDB_COL_NAME_LEN);
  }

  // Outer column references must retain the set operator's output namespace.
  tstrncpy(((SExprNode*)pKeep)->userAlias, pSetOp->node.userAlias,
            TSDB_COL_NAME_LEN);
  if (QUERY_NODE_SELECT_STMT == nodeType(pKeep)) {
    SSelectStmt* pSelect = (SSelectStmt*)pKeep;
    tstrncpy(pSelect->stmtName, pSetOp->stmtName, TSDB_TABLE_NAME_LEN);
    if (E_SUB_QUERY_NOT_SET != pSetOp->subQType) {
      pSelect->subQType = pSetOp->subQType;
      pSelect->quantify = pSetOp->quantify;
    }
    if (NULL != pSetOp->pSubQueries) {
      TSWAP(pSelect->pSubQueries, pSetOp->pSubQueries);
    }
    if (NULL != pSetOp->pOrigSubquerySql) {
      TSWAP(pSelect->pOrigSubquerySql, pSetOp->pOrigSubquerySql);
    }
  } else if (QUERY_NODE_SET_OPERATOR == nodeType(pKeep)) {
    SSetOperator* pSet = (SSetOperator*)pKeep;
    tstrncpy(pSet->stmtName, pSetOp->stmtName, TSDB_TABLE_NAME_LEN);
    if (E_SUB_QUERY_NOT_SET != pSetOp->subQType) {
      pSet->subQType = pSetOp->subQType;
      pSet->quantify = pSetOp->quantify;
    }
    if (NULL != pSetOp->pSubQueries) {
      TSWAP(pSet->pSubQueries, pSetOp->pSubQueries);
    }
    if (NULL != pSetOp->pOrigSubquerySql) {
      TSWAP(pSet->pOrigSubquerySql, pSetOp->pOrigSubquerySql);
    }
  }

  if (pSetOp->pOperands != NULL) {
    SListCell* pKeepCell = nodesListGetCell(pSetOp->pOperands, 0);
    if (pKeepCell != NULL) pKeepCell->pNode = NULL;
  } else {
    if (pSetOp->pLeft == pKeep) pSetOp->pLeft = NULL;
    if (pSetOp->pRight == pKeep) pSetOp->pRight = NULL;
    if (pSetOp->pLeft == pDrop) pSetOp->pLeft = NULL;
    if (pSetOp->pRight == pDrop) pSetOp->pRight = NULL;
  }
  if (pDrop != NULL) nodesDestroyNode(pDrop);
  nodesDestroyList(pSetOp->pOperands);
  pSetOp->pOperands = NULL;
  nodesDestroyList(pSetOp->pProjectionList);
  pSetOp->pProjectionList = NULL;
  nodesDestroyNode((SNode*)pSetOp);
  *ppStmt = pKeep;
}


static int32_t pruneEmptyUnionAllBranches(SNode** ppStmt);

static int32_t pruneEmptyUnionAllBranchesInList(SNodeList* pStmts) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pStmt = NULL;
  FOREACH(pStmt, pStmts) {
    code = pruneEmptyUnionAllBranches(&cell->pNode);
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }
  return code;
}

static int32_t pruneEmptyUnionAllBranches(SNode** ppStmt) {
  if (NULL == ppStmt || NULL == *ppStmt) {
    return TSDB_CODE_SUCCESS;
  }

  switch (nodeType(*ppStmt)) {
    case QUERY_NODE_SELECT_STMT: {
      SSelectStmt* pSelect = (SSelectStmt*)*ppStmt;
      int32_t code = pruneEmptyUnionAllBranches(&pSelect->pFromTable);
      if (TSDB_CODE_SUCCESS == code && pSelect->pSubQueries) {
        code = pruneEmptyUnionAllBranchesInList(pSelect->pSubQueries);
      }
      return code;
    }
    case QUERY_NODE_TEMP_TABLE:
      return pruneEmptyUnionAllBranches(&((STempTableNode*)*ppStmt)->pSubquery);
    case QUERY_NODE_JOIN_TABLE: {
      SJoinTableNode* pJoin = (SJoinTableNode*)*ppStmt;
      int32_t code = pruneEmptyUnionAllBranches(&pJoin->pLeft);
      if (TSDB_CODE_SUCCESS == code) {
        code = pruneEmptyUnionAllBranches(&pJoin->pRight);
      }
      return code;
    }
    case QUERY_NODE_EXPLAIN_STMT:
      return pruneEmptyUnionAllBranches(&((SExplainStmt*)*ppStmt)->pQuery);
    case QUERY_NODE_SET_OPERATOR:
      break;
    default:
      return TSDB_CODE_SUCCESS;
  }

  SSetOperator* pSetOp = (SSetOperator*)*ppStmt;
  int32_t       code = TSDB_CODE_SUCCESS;
  if (pSetOp->pOperands != NULL) {
    for (SListCell* pCell = pSetOp->pOperands->pHead; TSDB_CODE_SUCCESS == code && pCell != NULL;
         pCell = pCell->pNext) {
      code = pruneEmptyUnionAllBranches(&pCell->pNode);
    }
  } else {
    code = pruneEmptyUnionAllBranches(&pSetOp->pLeft);
    if (TSDB_CODE_SUCCESS == code) {
      code = pruneEmptyUnionAllBranches(&pSetOp->pRight);
    }
  }
  if (TSDB_CODE_SUCCESS == code && pSetOp->pSubQueries) {
    code = pruneEmptyUnionAllBranchesInList(pSetOp->pSubQueries);
  }
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  if (SET_OP_TYPE_UNION_ALL != pSetOp->opType || NULL != pSetOp->pOrderByList || NULL != pSetOp->pLimit) {
    return TSDB_CODE_SUCCESS;
  }

  bool hasNonEmptyOperand = false;
  for (int32_t i = 0; i < setOperatorChildCount(pSetOp); ++i) {
    if (!isEmptyResultQuery(setOperatorChildAt(pSetOp, i))) {
      hasNonEmptyOperand = true;
      break;
    }
  }
  // Keep an all-empty UNION ALL intact so it continues to carry the output
  // projection/type metadata; removing every operand would create an invalid
  // zero-child set operator.
  if (!hasNonEmptyOperand) return TSDB_CODE_SUCCESS;

  if (NULL != pSetOp->pOperands) {
    SNode* pOperand = NULL;
    WHERE_EACH(pOperand, pSetOp->pOperands) {
      if (isEmptyResultQuery(pOperand)) {
        ERASE_NODE(pSetOp->pOperands);
      } else {
        WHERE_NEXT;
      }
    }
  }
  if (setOperatorChildCount(pSetOp) == 1 && pSetOp->pOperands != NULL) {
    SNode* pKeep = setOperatorChildAt(pSetOp, 0);
    replaceSetOpWithChild(ppStmt, pSetOp, pKeep, NULL);
  } else if (pSetOp->pOperands == NULL) {
    bool leftEmpty = isEmptyResultQuery(pSetOp->pLeft);
    bool rightEmpty = isEmptyResultQuery(pSetOp->pRight);
    if (leftEmpty != rightEmpty) {
      replaceSetOpWithChild(ppStmt, pSetOp, leftEmpty ? pSetOp->pRight : pSetOp->pLeft,
                            leftEmpty ? pSetOp->pLeft : pSetOp->pRight);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static void resetProjectNullTypeImpl(SNodeList* pProjects) {
  SNode* pProj = NULL;
  FOREACH(pProj, pProjects) {
    SExprNode* pExpr = (SExprNode*)pProj;
    if (TSDB_DATA_TYPE_NULL == pExpr->resType.type) {
      pExpr->resType.type = TSDB_DATA_TYPE_VARCHAR;
      pExpr->resType.bytes = VARSTR_HEADER_SIZE;
      if (QUERY_NODE_VALUE == nodeType(pExpr)) {
        ((SValueNode*)pExpr)->isNull = true;
      }
    }
  }
}

static void resetProjectNullType(SNode* pStmt) {
  switch (nodeType(pStmt)) {
    case QUERY_NODE_SELECT_STMT:
      resetProjectNullTypeImpl(((SSelectStmt*)pStmt)->pProjectionList);
      break;
    case QUERY_NODE_SET_OPERATOR: {
      SSetOperator* pSetOp = (SSetOperator*)pStmt;
      resetProjectNullTypeImpl(pSetOp->pProjectionList);
      for (int32_t i = 0; i < setOperatorChildCount(pSetOp); ++i)
        resetProjectNullType(setOperatorChildAt(pSetOp, i));
      break;
    }
    default:
      break;
  }
}

static SNodeList** querySubqueries(SNode* pStmt) {
  if (NULL == pStmt) {
    return NULL;
  }
  switch (nodeType(pStmt)) {
    case QUERY_NODE_SELECT_STMT:
      return &((SSelectStmt*)pStmt)->pSubQueries;
    case QUERY_NODE_SET_OPERATOR:
      return &((SSetOperator*)pStmt)->pSubQueries;
    case QUERY_NODE_EXPLAIN_STMT:
      return querySubqueries(((SExplainStmt*)pStmt)->pQuery);
    case QUERY_NODE_INSERT_STMT:
      return querySubqueries(((SInsertStmt*)pStmt)->pQuery);
    default:
      return NULL;
  }
}

static int32_t* subqueryIndex(SNode* pNode) {
  switch (nodeType(pNode)) {
    case QUERY_NODE_REMOTE_VALUE:
    case QUERY_NODE_REMOTE_ZERO_ROWS:
      return &((SRemoteValueNode*)pNode)->subQIdx;
    case QUERY_NODE_REMOTE_VALUE_LIST:
      return &((SRemoteValueListNode*)pNode)->subQIdx;
    case QUERY_NODE_REMOTE_ROW:
      return &((SRemoteRowNode*)pNode)->subQIdx;
    case QUERY_NODE_REMOTE_TABLE:
      return &((SRemoteTableNode*)pNode)->subQIdx;
    case QUERY_NODE_CORRELATED_SUBQUERY:
      return &((SCorrelatedSubqueryNode*)pNode)->templateId;
    default:
      return NULL;
  }
}

static bool emptySubquery(SCalcConstContext* pCxt, SNode* pNode) {
  int32_t* pIndex = subqueryIndex(pNode);
  if (NULL == pIndex) {
    return false;
  }
  if (*pIndex < 0 || *pIndex >= LIST_LENGTH(pCxt->pSubQueries)) {
    pCxt->code = TSDB_CODE_PAR_INTERNAL_ERROR;
    return false;
  }
  return isEmptyResultQuery(nodesListGetNode(pCxt->pSubQueries, *pIndex));
}

static EDealRes replaceSubqueryValue(SCalcConstContext* pCxt, SNode** ppNode,
                                    bool isNull, int64_t value) {
  SValueNode* pValue = NULL;
  if (isNull) {
    pCxt->code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pValue);
  } else if (TSDB_DATA_TYPE_BOOL == ((SExprNode*)*ppNode)->resType.type) {
    pCxt->code = nodesMakeValueNodeFromBool(value != 0, &pValue);
  } else {
    pCxt->code = nodesMakeValueNodeFromInt64(value, (SNode**)&pValue);
  }
  if (TSDB_CODE_SUCCESS != pCxt->code) {
    return DEAL_RES_ERROR;
  }
  SExprNode* pOld = (SExprNode*)*ppNode;
  pValue->node = *pOld;
  pValue->node.type = QUERY_NODE_VALUE;
  pValue->translate = true;
  pValue->isNull = isNull;
  // Associations belong to the replacement expression, not the remote node.
  pOld->pAssociation = NULL;
  nodesDestroyNode(*ppNode);
  *ppNode = (SNode*)pValue;
  return DEAL_RES_IGNORE_CHILD;
}

static EDealRes findRowCountFunction(SNode* pNode, void* pContext) {
  if (QUERY_NODE_FUNCTION == nodeType(pNode)) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    int32_t funcId = pFunc->hasOriginalFunc ? pFunc->originalFuncId
                                          : pFunc->funcId;
    if (fmIsAggFunc(funcId) || fmIsIndefiniteRowsFunc(funcId) ||
        fmIsMultiRowsFunc(funcId)) {
      *(bool*)pContext = true;
      return DEAL_RES_END;
    }
  }
  return DEAL_RES_CONTINUE;
}

static EDealRes replaceEmptySubqueryPredicate(SCalcConstContext* pCxt,
                                             SNode** ppNode, bool value) {
  SOperatorNode* pOp = (SOperatorNode*)*ppNode;
  // String-to-geometry comparison validates each input value before checking
  // the empty set. Keep that evaluation, since valid WKT cannot be inferred
  // from the input type alone.
  if (QUERY_NODE_REMOTE_VALUE_LIST == nodeType(pOp->pRight) &&
      TSDB_DATA_TYPE_VARCHAR == ((SExprNode*)pOp->pLeft)->resType.type &&
      TSDB_DATA_TYPE_GEOMETRY == ((SExprNode*)pOp->pRight)->resType.type) {
    return DEAL_RES_CONTINUE;
  }
  // Preserve validation normally performed before evaluating the predicate.
  pCxt->code = scalarConvertOpValueNodeTs(pOp);
  if (TSDB_CODE_SUCCESS == pCxt->code &&
      QUERY_NODE_REMOTE_VALUE_LIST == nodeType(pOp->pRight) &&
      vectorGetConvertType(((SExprNode*)pOp->pLeft)->resType.type,
                           ((SExprNode*)pOp->pRight)->resType.type) < 0) {
    pCxt->code = TSDB_CODE_SCALAR_CONVERT_ERROR;
  }
  if (TSDB_CODE_SUCCESS != pCxt->code) {
    return DEAL_RES_ERROR;
  }
  bool keepInput = pCxt->tagScan;
  if (!keepInput) {
    nodesWalkExpr(pOp->pLeft, findRowCountFunction, &keepInput);
  }
  if (!keepInput) {
    return replaceSubqueryValue(pCxt, ppNode, false, value);
  }

  // Retain aggregates and SELECT TAGS data columns that determine row counts.
  // IS NULL always returns a non-NULL boolean, so a second null check yields
  // the empty-set result even when the original input is NULL.
  SOperatorNode* pIsNull = NULL;
  pCxt->code = nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pIsNull);
  if (TSDB_CODE_SUCCESS != pCxt->code) {
    return DEAL_RES_ERROR;
  }
  pIsNull->node.resType = pOp->node.resType;
  pIsNull->opType = OP_TYPE_IS_NULL;
  pIsNull->pLeft = pOp->pLeft;
  pOp->pLeft = (SNode*)pIsNull;
  nodesDestroyNode(pOp->pRight);
  pOp->pRight = NULL;
  pOp->opType = value ? OP_TYPE_IS_NOT_NULL : OP_TYPE_IS_NULL;
  pOp->flag = 0;
  return DEAL_RES_CONTINUE;
}

static EDealRes rewriteEmptySubquery(SNode** ppNode, void* pContext) {
  SCalcConstContext* pCxt = pContext;
  if (TSDB_CODE_SUCCESS != pCxt->code) {
    return DEAL_RES_ERROR;
  }
  switch (nodeType(*ppNode)) {
    case QUERY_NODE_REMOTE_VALUE:
    case QUERY_NODE_CORRELATED_SUBQUERY:
      if (emptySubquery(pCxt, *ppNode)) {
        return replaceSubqueryValue(pCxt, ppNode, true, 0);
      }
      break;
    case QUERY_NODE_REMOTE_ZERO_ROWS:
      if (emptySubquery(pCxt, *ppNode)) {
        return replaceSubqueryValue(pCxt, ppNode, false, 0);
      }
      break;
    case QUERY_NODE_OPERATOR: {
      SOperatorNode* pOp = (SOperatorNode*)*ppNode;
      if (NULL != pOp->pRight &&
          QUERY_NODE_REMOTE_VALUE_LIST == nodeType(pOp->pRight) &&
          emptySubquery(pCxt, pOp->pRight)) {
        // Quantified equality's negative flag does not invert the empty case.
        return replaceEmptySubqueryPredicate(pCxt, ppNode,
                                              OP_TYPE_NOT_IN == pOp->opType);
      }
      if (NULL != pOp->pRight &&
          QUERY_NODE_REMOTE_ROW == nodeType(pOp->pRight) &&
          emptySubquery(pCxt, pOp->pRight)) {
        bool min = ((SRemoteRowNode*)pOp->pRight)->isMinVal;
        bool any = ((OP_TYPE_GREATER_EQUAL == pOp->opType ||
                     OP_TYPE_GREATER_THAN == pOp->opType) && min) ||
                    ((OP_TYPE_LOWER_EQUAL == pOp->opType ||
                      OP_TYPE_LOWER_THAN == pOp->opType) && !min);
        return replaceEmptySubqueryPredicate(pCxt, ppNode, !any);
      }
      break;
    }
    default:
      break;
  }
  return TSDB_CODE_SUCCESS == pCxt->code ? DEAL_RES_CONTINUE : DEAL_RES_ERROR;
}

static void rewriteQueryExpressions(SNode* pStmt, FNodeRewriter rewriter,
                                    SCalcConstContext* pCxt);

static void rewriteTableExpressions(SNode* pTable, FNodeRewriter rewriter,
                                    SCalcConstContext* pCxt) {
  if (NULL == pTable || TSDB_CODE_SUCCESS != pCxt->code) {
    return;
  }
  if (QUERY_NODE_TEMP_TABLE == nodeType(pTable)) {
    rewriteQueryExpressions(((STempTableNode*)pTable)->pSubquery, rewriter,
                             pCxt);
  } else if (QUERY_NODE_JOIN_TABLE == nodeType(pTable)) {
    SJoinTableNode* pJoin = (SJoinTableNode*)pTable;
    nodesRewriteExpr(&pJoin->pWindowOffset, rewriter, pCxt);
    nodesRewriteExpr(&pJoin->pJLimit, rewriter, pCxt);
    rewriteTableExpressions(pJoin->pLeft, rewriter, pCxt);
    rewriteTableExpressions(pJoin->pRight, rewriter, pCxt);
  }
}

static void rewriteQueryExpressions(SNode* pStmt, FNodeRewriter rewriter,
                                    SCalcConstContext* pCxt) {
  if (NULL == pStmt || TSDB_CODE_SUCCESS != pCxt->code) {
    return;
  }
  switch (nodeType(pStmt)) {
    case QUERY_NODE_SELECT_STMT: {
      SSelectStmt* pSelect = (SSelectStmt*)pStmt;
      nodesRewriteSelectStmt(pSelect, SQL_CLAUSE_FROM, rewriter, pCxt);
      rewriteTableExpressions(pSelect->pFromTable, rewriter, pCxt);
      break;
    }
    case QUERY_NODE_SET_OPERATOR: {
      SSetOperator* pSet = (SSetOperator*)pStmt;
      nodesRewriteExprs(pSet->pProjectionList, rewriter, pCxt);
      nodesRewriteExprs(pSet->pOrderByList, rewriter, pCxt);
      for (int32_t i = 0; i < setOperatorChildCount(pSet); ++i) {
        rewriteQueryExpressions(setOperatorChildAt(pSet, i), rewriter, pCxt);
      }
      break;
    }
    case QUERY_NODE_EXPLAIN_STMT:
      rewriteQueryExpressions(((SExplainStmt*)pStmt)->pQuery, rewriter, pCxt);
      break;
    case QUERY_NODE_INSERT_STMT:
      rewriteQueryExpressions(((SInsertStmt*)pStmt)->pQuery, rewriter, pCxt);
      break;
    default:
      break;
  }
}

static EDealRes markLiveSubquery(SNode** ppNode, void* pContext) {
  SCalcConstContext* pCxt = pContext;
  int32_t* pIndex = subqueryIndex(*ppNode);
  if (NULL == pIndex) {
    return DEAL_RES_CONTINUE;
  }
  if (*pIndex < 0 || *pIndex >= LIST_LENGTH(pCxt->pSubQueries)) {
    pCxt->code = TSDB_CODE_PAR_INTERNAL_ERROR;
    return DEAL_RES_ERROR;
  }
  if (pCxt->pSubqueryMap[*pIndex] < 0) {
    pCxt->pSubqueryMap[*pIndex] = 0;
    rewriteQueryExpressions(nodesListGetNode(pCxt->pSubQueries, *pIndex),
                             markLiveSubquery, pCxt);
  }
  return TSDB_CODE_SUCCESS == pCxt->code ? DEAL_RES_CONTINUE : DEAL_RES_ERROR;
}

static EDealRes remapSubquery(SNode** ppNode, void* pContext) {
  SCalcConstContext* pCxt = pContext;
  int32_t* pIndex = subqueryIndex(*ppNode);
  if (NULL != pIndex) {
    if (*pIndex < 0 || *pIndex >= LIST_LENGTH(pCxt->pSubQueries) ||
        pCxt->pSubqueryMap[*pIndex] < 0) {
      pCxt->code = TSDB_CODE_PAR_INTERNAL_ERROR;
      return DEAL_RES_ERROR;
    }
    *pIndex = pCxt->pSubqueryMap[*pIndex];
  }
  return DEAL_RES_CONTINUE;
}

static int32_t pruneUnusedSubqueries(SCalcConstContext* pCxt, SNode* pRoot) {
  int32_t count = LIST_LENGTH(pCxt->pSubQueries);
  if (0 == count) {
    return TSDB_CODE_SUCCESS;
  }
  pCxt->pSubqueryMap = taosMemoryMalloc(count * sizeof(int32_t));
  if (NULL == pCxt->pSubqueryMap) {
    return terrno;
  }
  memset(pCxt->pSubqueryMap, -1, count * sizeof(int32_t));
  rewriteQueryExpressions(pRoot, markLiveSubquery, pCxt);
  int32_t retained = 0;
  for (int32_t i = 0; i < count; ++i) {
    if (pCxt->pSubqueryMap[i] >= 0) {
      pCxt->pSubqueryMap[i] = retained++;
    }
  }
  rewriteQueryExpressions(pRoot, remapSubquery, pCxt);
  int32_t index = 0;
  SNode* pSubquery = NULL;
  FOREACH(pSubquery, pCxt->pSubQueries) {
    if (pCxt->pSubqueryMap[index++] >= 0) {
      rewriteQueryExpressions(pSubquery, remapSubquery, pCxt);
    }
  }
  if (TSDB_CODE_SUCCESS == pCxt->code) {
    index = 0;
    WHERE_EACH(pSubquery, pCxt->pSubQueries) {
      if (pCxt->pSubqueryMap[index++] < 0) {
        ERASE_NODE(pCxt->pSubQueries);
        continue;
      }
      WHERE_NEXT;
    }
  }
  taosMemoryFreeClear(pCxt->pSubqueryMap);
  return pCxt->code;
}

int32_t calculateConstant(SParseContext* pParseCxt, SQuery* pQuery) {
  SSelectStmt* pExplainedSelect = NULL;
  if (QUERY_NODE_EXPLAIN_STMT == nodeType(pQuery->pRoot)) {
    SNode* pStmt = ((SExplainStmt*)pQuery->pRoot)->pQuery;
    if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
      pExplainedSelect = (SSelectStmt*)pStmt;
    }
  }
  bool wasEmptyExplain = NULL != pExplainedSelect && pExplainedSelect->isEmptyResult;
  SNodeList** ppSubQueries = querySubqueries(pQuery->pRoot);
  SCalcConstContext cxt = {.pParseCxt = pParseCxt,
                           .msgBuf.buf = pParseCxt->pMsg,
                           .msgBuf.len = pParseCxt->msgLen,
                           .code = TSDB_CODE_SUCCESS,
                           .pSubQueries = ppSubQueries ? *ppSubQueries : NULL};

  int32_t code = TSDB_CODE_SUCCESS;

  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstQuery(&cxt, pQuery->pRoot, false);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = pruneEmptyUnionAllBranches(&pQuery->pRoot);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = pruneUnusedSubqueries(&cxt, pQuery->pRoot);
  }
  if (TSDB_CODE_SUCCESS == code) {
    resetProjectNullType(pQuery->pRoot);
    // Keep semantic-analysis shortcuts such as contradictory ASOF ranges,
    // unless folding corrected an empty-input flag (for example, COUNT).
    if (isEmptyResultQuery(pQuery->pRoot) ||
        (wasEmptyExplain && pExplainedSelect->isEmptyResult)) {
      pQuery->execMode = QUERY_EXEC_MODE_EMPTY_RESULT;
    }
  }
  
  return code;
}
