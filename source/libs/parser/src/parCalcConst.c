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

typedef struct SCalcConstContext {
  int32_t code;
} SCalcConstContext;

static int32_t calcConstQuery(SNode* pStmt);

static EDealRes doCalcConst(SNode** pNode, SCalcConstContext* pCxt) {
  SNode* pNew = NULL;
  pCxt->code = scalarCalculateConstants(*pNode, &pNew);
  if (TSDB_CODE_SUCCESS != pCxt->code) {
    return DEAL_RES_ERROR;
  }
  ((SValueNode*)pNew)->genByCalc = true;
  ((SValueNode*)pNew)->translate = true;
  *pNode = pNew;
  return DEAL_RES_CONTINUE;
}

static EDealRes calcConstOperator(SOperatorNode** pNode, void* pContext) {
  SOperatorNode* pOp = *pNode;
  if (QUERY_NODE_VALUE == nodeType(pOp->pLeft) && (NULL == pOp->pRight || QUERY_NODE_VALUE == nodeType(pOp->pRight))) {
    return doCalcConst((SNode**)pNode, (SCalcConstContext*)pContext);
  }
  return DEAL_RES_CONTINUE;
}

static EDealRes calcConstFunction(SFunctionNode** pNode, void* pContext) {
  SFunctionNode* pFunc = *pNode;
  if (fmIsPseudoColumnFunc(pFunc->funcId)) {
    return DEAL_RES_CONTINUE;
  }
  SNode* pParam = NULL;
  FOREACH(pParam, pFunc->pParameterList) {
    if (QUERY_NODE_VALUE != nodeType(pParam)) {
      return DEAL_RES_CONTINUE;
    }
  }
  return doCalcConst((SNode**)pNode, (SCalcConstContext*)pContext);
}

static EDealRes calcConstLogicCond(SLogicConditionNode** pNode, void* pContext) {
  SLogicConditionNode* pCond = *pNode;
  SNode* pParam = NULL;
  FOREACH(pParam, pCond->pParameterList) {
    if (QUERY_NODE_VALUE != nodeType(pParam)) {
      return DEAL_RES_CONTINUE;
    }
  }
  return doCalcConst((SNode**)pNode, (SCalcConstContext*)pContext);
}

static EDealRes calcConstSubquery(STempTableNode** pNode, void* pContext) {
  SCalcConstContext* pCxt = pContext;
  pCxt->code = calcConstQuery((*pNode)->pSubquery);
  return (TSDB_CODE_SUCCESS == pCxt->code ? DEAL_RES_CONTINUE : DEAL_RES_ERROR);
}

static EDealRes calcConst(SNode** pNode, void* pContext) {
  switch (nodeType(*pNode)) {
    case QUERY_NODE_OPERATOR:
      return calcConstOperator((SOperatorNode**)pNode, pContext);
    case QUERY_NODE_FUNCTION:
      return calcConstFunction((SFunctionNode**)pNode, pContext);
    case QUERY_NODE_LOGIC_CONDITION:
      return calcConstLogicCond((SLogicConditionNode**)pNode, pContext);
    case QUERY_NODE_TEMP_TABLE:
      return calcConstSubquery((STempTableNode**)pNode, pContext);
    default:
      break;
  }
  return DEAL_RES_CONTINUE;
}

static bool isCondition(const SNode* pNode) {
  if (QUERY_NODE_OPERATOR == nodeType(pNode)) {
    return nodesIsComparisonOp((const SOperatorNode*)pNode);
  }
  return (QUERY_NODE_LOGIC_CONDITION == nodeType(pNode));
}

static int32_t rewriteIsTrue(SNode* pSrc, SNode** pIsTrue) {
  SOperatorNode* pOp = nodesMakeNode(QUERY_NODE_OPERATOR);
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

static int32_t rewriteConditionForFromTable(SCalcConstContext* pCxt, SNode* pTable) {
  if (QUERY_NODE_JOIN_TABLE == nodeType(pTable)) {
    SJoinTableNode* pJoin = (SJoinTableNode*)pTable;
    pCxt->code = rewriteConditionForFromTable(pCxt, pJoin->pLeft);
    if (TSDB_CODE_SUCCESS == pCxt->code) {
      pCxt->code = rewriteConditionForFromTable(pCxt, pJoin->pRight);
    }
    if (TSDB_CODE_SUCCESS == pCxt->code) {
      pCxt->code = rewriteCondition(pCxt, &pJoin->pOnCond);
    }
  }
  return pCxt->code;
}

static int32_t calcConstFromTable(SCalcConstContext* pCxt, SSelectStmt* pSelect) {
  nodesRewriteExprPostOrder(&pSelect->pFromTable, calcConst, pCxt);
  if (TSDB_CODE_SUCCESS == pCxt->code) {
    pCxt->code = rewriteConditionForFromTable(pCxt, pSelect->pFromTable);
  }
  return pCxt->code;
}

static int32_t calcConstCondition(SCalcConstContext* pCxt, SNode** pCond) {
  if (NULL == *pCond) {
    return TSDB_CODE_SUCCESS;
  }

  nodesRewriteExprPostOrder(pCond, calcConst, pCxt);
  if (TSDB_CODE_SUCCESS == pCxt->code) {
    pCxt->code = rewriteCondition(pCxt, pCond);
  }
  return pCxt->code;
}

static int32_t calcConstSelect(SSelectStmt* pSelect) {
  SCalcConstContext cxt = { .code = TSDB_CODE_SUCCESS };
  nodesRewriteExprsPostOrder(pSelect->pProjectionList, calcConst, &cxt);
  if (TSDB_CODE_SUCCESS == cxt.code) {
    cxt.code = calcConstFromTable(&cxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == cxt.code) {
    cxt.code = calcConstCondition(&cxt, &pSelect->pWhere);
  }
  if (TSDB_CODE_SUCCESS == cxt.code) {
    nodesRewriteExprsPostOrder(pSelect->pPartitionByList, calcConst, &cxt);
  }
  if (TSDB_CODE_SUCCESS == cxt.code) {
    nodesRewriteExprPostOrder(&pSelect->pWindow, calcConst, &cxt);
  }
  if (TSDB_CODE_SUCCESS == cxt.code) {
    nodesRewriteExprsPostOrder(pSelect->pGroupByList, calcConst, &cxt);
  }
  if (TSDB_CODE_SUCCESS == cxt.code) {
    cxt.code = calcConstCondition(&cxt, &pSelect->pHaving);
  }
  if (TSDB_CODE_SUCCESS == cxt.code) {
    nodesRewriteExprsPostOrder(pSelect->pOrderByList, calcConst, &cxt);
  }
  return cxt.code;
}

static int32_t calcConstQuery(SNode* pStmt) {
  switch (nodeType(pStmt)) {
    case QUERY_NODE_SELECT_STMT:
      return calcConstSelect((SSelectStmt*)pStmt);    
    case QUERY_NODE_EXPLAIN_STMT:
      return calcConstQuery(((SExplainStmt*)pStmt)->pQuery);
    default:
      break;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t calculateConstant(SParseContext* pParseCxt, SQuery* pQuery) {
  return calcConstQuery(pQuery->pRoot);
}
