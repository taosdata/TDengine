/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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
  SParseContext*       pParseCxt;
  SNodeReplaceContext  replaceCxt;
  SMsgBuf              msgBuf;
  int32_t              code;
} SCalcConstContext;

static int32_t calcConstQuery(SCalcConstContext* pCxt, SNode* pStmt, bool subquery);

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
  SOperatorNode* pOp = (SOperatorNode*)nodesMakeNode(QUERY_NODE_OPERATOR);
  if (NULL == pOp) {
    return TSDB_CODE_OUT_OF_MEMORY;
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

static int32_t rewriteConditionForFromTable(SCalcConstContext* pCxt, SNode* pTable) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (nodeType(pTable)) {
    case QUERY_NODE_TEMP_TABLE: {
      code = calcConstSubquery(pCxt, (STempTableNode*)pTable);
      break;
    }
    case QUERY_NODE_JOIN_TABLE: {
      SJoinTableNode* pJoin = (SJoinTableNode*)pTable;
      code = rewriteConditionForFromTable(pCxt, pJoin->pLeft);
      if (TSDB_CODE_SUCCESS == code) {
        code = rewriteConditionForFromTable(pCxt, pJoin->pRight);
      }
      if (TSDB_CODE_SUCCESS == code && NULL != pJoin->pOnCond) {
        code = calcConstCondition(pCxt, &pJoin->pOnCond);
      }
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

static void rewriteConstCondition(SNode** pCond, bool* pAlwaysFalse) {
  if (QUERY_NODE_VALUE != nodeType(*pCond)) {
    return;
  }
  if (((SValueNode*)*pCond)->datum.b) {
    nodesDestroyNode(*pCond);
    *pCond = NULL;
  } else {
    *pAlwaysFalse = true;
  }
}

static int32_t calcConstStmtCondition(SCalcConstContext* pCxt, SNode** pCond, bool* pAlwaysFalse) {
  if (NULL == *pCond) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = rewriteCondition(pCxt, pCond);
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstNode(pCond);
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
    strcpy(aliasName, ((SExprNode*)*pNode)->aliasName);
    nodesDestroyNode(*pNode);
    *pNode = nodesCloneNode(pCxt->replaceCxt.pNew);
    if (NULL == *pNode) {
      pCxt->code = TSDB_CODE_OUT_OF_MEMORY;
      return DEAL_RES_ERROR;
    }
    strcpy(((SExprNode*)*pNode)->aliasName, aliasName);

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
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  char aliasName[TSDB_COL_NAME_LEN] = {0};
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
        SNode** pCol = pAssNode->pPlace;
        if (*pCol == pAssNode->pAssociationNode) {
          strcpy(aliasName, ((SExprNode*)*pCol)->aliasName);
          SArray* pOrigAss = NULL;
          TSWAP(((SExprNode*)*pCol)->pAssociation, pOrigAss);
          nodesDestroyNode(*pCol);
          *pCol = nodesCloneNode(*pNew);
          TSWAP(pOrigAss, ((SExprNode*)*pCol)->pAssociation);
          taosArrayDestroy(pOrigAss);
          strcpy(((SExprNode*)*pCol)->aliasName, aliasName);
          if (NULL == *pCol) {
            code = TSDB_CODE_OUT_OF_MEMORY;
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

typedef struct SIsUselessColCtx  {
  bool      isUseless;
} SIsUselessColCtx ;

EDealRes checkUselessCol(SNode *pNode, void *pContext) {
  SIsUselessColCtx  *ctx = (SIsUselessColCtx *)pContext;
  if (QUERY_NODE_FUNCTION == nodeType(pNode) && !fmIsScalarFunc(((SFunctionNode*)pNode)->funcId) &&
      !fmIsPseudoColumnFunc(((SFunctionNode*)pNode)->funcId)) {
    ctx->isUseless = false;  
    return DEAL_RES_END;
  }

  return DEAL_RES_CONTINUE;
}

static bool isUselessCol(SExprNode* pProj) {
  SIsUselessColCtx ctx = {.isUseless = true};
  nodesWalkExpr((SNode*)pProj, checkUselessCol, (void *)&ctx);
  if (!ctx.isUseless) {
    return false;
  }
  return NULL == ((SExprNode*)pProj)->pAssociation;
}

static SNode* createConstantValue() {
  SValueNode* pVal = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
  if (NULL == pVal) {
    return NULL;
  }
  pVal->node.resType.type = TSDB_DATA_TYPE_INT;
  pVal->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_INT].bytes;
  const int32_t val = 1;
  nodesSetValueNodeValue(pVal, (void*)&val);
  pVal->translate = true;
  return (SNode*)pVal;
}

static int32_t calcConstProjections(SCalcConstContext* pCxt, SSelectStmt* pSelect, bool subquery) {
  SNode* pProj = NULL;
  WHERE_EACH(pProj, pSelect->pProjectionList) {
    if (subquery && !pSelect->isDistinct && !pSelect->tagScan && isUselessCol((SExprNode*)pProj)) {
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
    return nodesListStrictAppend(pSelect->pProjectionList, createConstantValue());
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t calcConstGroupBy(SCalcConstContext* pCxt, SSelectStmt* pSelect) {
  int32_t code = calcConstList(pSelect->pGroupByList);
  if (TSDB_CODE_SUCCESS == code) {
    SNode* pNode = NULL;
    FOREACH(pNode, pSelect->pGroupByList) {
      SNode* pGroupPara = NULL;
      FOREACH(pGroupPara, ((SGroupingSetNode*)pNode)->pParameterList) {
        if (QUERY_NODE_VALUE != nodeType(pGroupPara)) {
          return code;
        }
      }
    }
    NODES_DESTORY_LIST(pSelect->pGroupByList);
  }
  return code;
}

static int32_t calcConstSelectWithoutFrom(SCalcConstContext* pCxt, SSelectStmt* pSelect, bool subquery) {
  return calcConstProjections(pCxt, pSelect, subquery);
}

static int32_t calcConstSelectFrom(SCalcConstContext* pCxt, SSelectStmt* pSelect, bool subquery) {
  int32_t code = calcConstFromTable(pCxt, pSelect->pFromTable);
  if (TSDB_CODE_SUCCESS == code && QUERY_NODE_TEMP_TABLE == nodeType(pSelect->pFromTable) &&
      ((STempTableNode*)pSelect->pFromTable)->pSubquery != NULL &&
      QUERY_NODE_SELECT_STMT == nodeType(((STempTableNode*)pSelect->pFromTable)->pSubquery) &&
      ((SSelectStmt*)((STempTableNode*)pSelect->pFromTable)->pSubquery)->isEmptyResult){
    pSelect->isEmptyResult = true;
    return code;
  }      
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstProjections(pCxt, pSelect, subquery);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstStmtCondition(pCxt, &pSelect->pWhere, &pSelect->isEmptyResult);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstList(pSelect->pPartitionByList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstList(pSelect->pTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstNode(&pSelect->pSubtable);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstNode(&pSelect->pWindow);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstGroupBy(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstStmtCondition(pCxt, &pSelect->pHaving, &pSelect->isEmptyResult);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstList(pSelect->pOrderByList);
  }
  return code;
}

static int32_t calcConstSelect(SCalcConstContext* pCxt, SSelectStmt* pSelect, bool subquery) {
  if (NULL == pSelect->pFromTable) {
    return calcConstSelectWithoutFrom(pCxt, pSelect, subquery);
  } else {
    return calcConstSelectFrom(pCxt, pSelect, subquery);
  }
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
  SNodeList* pLeftProjs = getChildProjection(pSetOp->pLeft);
  nodesListErase(pLeftProjs, nodesListGetCell(pLeftProjs, index));
  if (QUERY_NODE_SET_OPERATOR == nodeType(pSetOp->pLeft)) {
    eraseSetOpChildProjection((SSetOperator*)pSetOp->pLeft, index);
  }
  SNodeList* pRightProjs = getChildProjection(pSetOp->pRight);
  nodesListErase(pRightProjs, nodesListGetCell(pRightProjs, index));
  if (QUERY_NODE_SET_OPERATOR == nodeType(pSetOp->pRight)) {
    eraseSetOpChildProjection((SSetOperator*)pSetOp->pRight, index);
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

static bool isDistinctSubQuery(SNode* pNode) {
  if (NULL == pNode) {
    return false;
  }
  switch (nodeType(pNode)) {
    case QUERY_NODE_SELECT_STMT:
      return ((SSelectStmt*)pNode)->isDistinct;
    case QUERY_NODE_SET_OPERATOR:
      return isDistinctSubQuery((((SSetOperator*)pNode)->pLeft)) || isDistinctSubQuery((((SSetOperator*)pNode)->pLeft));
    default:
      break;
  }
  return false;
}

static bool isSetUselessCol(SSetOperator* pSetOp, int32_t index, SExprNode* pProj) {
  if (!isUselessCol(pProj)) {
    return false;
  }

  SNodeList* pLeftProjs = getChildProjection(pSetOp->pLeft);
  if (!isUselessCol((SExprNode*)nodesListGetNode(pLeftProjs, index)) || isDistinctSubQuery(pSetOp->pLeft)) {
    return false;
  }

  SNodeList* pRightProjs = getChildProjection(pSetOp->pRight);
  if (!isUselessCol((SExprNode*)nodesListGetNode(pRightProjs, index)) || isDistinctSubQuery(pSetOp->pLeft)) {
    return false;
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
    if (subquery && notRefByOrderBy((SColumnNode*)pProj, pSetOp->pOrderByList) &&
        isSetUselessCol(pSetOp, index, (SExprNode*)pProj)) {
      ERASE_NODE(pSetOp->pProjectionList);
      eraseSetOpChildProjection(pSetOp, index);
      continue;
    }
    ++index;
    WHERE_NEXT;
  }
  if (0 == LIST_LENGTH(pSetOp->pProjectionList)) {
    return nodesListStrictAppend(pSetOp->pProjectionList, createConstantValue());
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t calcConstSetOperator(SCalcConstContext* pCxt, SSetOperator* pSetOp, bool subquery) {
  int32_t code = calcConstSetOpProjections(pCxt, pSetOp, subquery);
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstQuery(pCxt, pSetOp->pLeft, false);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstQuery(pCxt, pSetOp->pRight, false);
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

static bool isEmptyResultQuery(SNode* pStmt) {
  bool isEmptyResult = false;
  switch (nodeType(pStmt)) {
    case QUERY_NODE_SELECT_STMT:
      isEmptyResult = ((SSelectStmt*)pStmt)->isEmptyResult;
      break;
    case QUERY_NODE_EXPLAIN_STMT:
      isEmptyResult = isEmptyResultQuery(((SExplainStmt*)pStmt)->pQuery);
      break;
    case QUERY_NODE_SET_OPERATOR: {
      SSetOperator* pSetOp = (SSetOperator*)pStmt;
      isEmptyResult = isEmptyResultQuery(pSetOp->pLeft);
      if (isEmptyResult) {
        isEmptyResult = isEmptyResultQuery(pSetOp->pRight);
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

static void resetProjectNullTypeImpl(SNodeList* pProjects) {
  SNode* pProj = NULL;
  FOREACH(pProj, pProjects) {
    SExprNode* pExpr = (SExprNode*)pProj;
    if (TSDB_DATA_TYPE_NULL == pExpr->resType.type) {
      pExpr->resType.type = TSDB_DATA_TYPE_VARCHAR;
      pExpr->resType.bytes = VARSTR_HEADER_SIZE;
    }
  }
}

static void resetProjectNullType(SNode* pStmt) {
  switch (nodeType(pStmt)) {
    case QUERY_NODE_SELECT_STMT:
      resetProjectNullTypeImpl(((SSelectStmt*)pStmt)->pProjectionList);
      break;
    case QUERY_NODE_SET_OPERATOR: {
      resetProjectNullTypeImpl(((SSetOperator*)pStmt)->pProjectionList);
      break;
    }
    default:
      break;
  }
}

int32_t calculateConstant(SParseContext* pParseCxt, SQuery* pQuery) {
  SCalcConstContext cxt = {.pParseCxt = pParseCxt,
                           .msgBuf.buf = pParseCxt->pMsg,
                           .msgBuf.len = pParseCxt->msgLen,
                           .code = TSDB_CODE_SUCCESS};
  int32_t           code = calcConstQuery(&cxt, pQuery->pRoot, false);
  if (TSDB_CODE_SUCCESS == code) {
    resetProjectNullType(pQuery->pRoot);
    if (isEmptyResultQuery(pQuery->pRoot)) {
      pQuery->execMode = QUERY_EXEC_MODE_EMPTY_RESULT;
    }
  }
  return code;
}
