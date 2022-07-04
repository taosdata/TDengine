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

#include "planInt.h"

static char* getUsageErrFormat(int32_t errCode) {
  switch (errCode) {
    case TSDB_CODE_PLAN_EXPECTED_TS_EQUAL:
      return "left.ts = right.ts is expected in join expression";
    case TSDB_CODE_PLAN_NOT_SUPPORT_CROSS_JOIN:
      return "not support cross join";
    default:
      break;
  }
  return "Unknown error";
}

int32_t generateUsageErrMsg(char* pBuf, int32_t len, int32_t errCode, ...) {
  va_list vArgList;
  va_start(vArgList, errCode);
  vsnprintf(pBuf, len, getUsageErrFormat(errCode), vArgList);
  va_end(vArgList);
  return errCode;
}

typedef struct SCreateColumnCxt {
  int32_t    errCode;
  SNodeList* pList;
} SCreateColumnCxt;

static EDealRes doCreateColumn(SNode* pNode, void* pContext) {
  SCreateColumnCxt* pCxt = (SCreateColumnCxt*)pContext;
  switch (nodeType(pNode)) {
    case QUERY_NODE_COLUMN: {
      SNode* pCol = nodesCloneNode(pNode);
      if (NULL == pCol) {
        return DEAL_RES_ERROR;
      }
      return (TSDB_CODE_SUCCESS == nodesListAppend(pCxt->pList, pCol) ? DEAL_RES_IGNORE_CHILD : DEAL_RES_ERROR);
    }
    case QUERY_NODE_OPERATOR:
    case QUERY_NODE_LOGIC_CONDITION:
    case QUERY_NODE_FUNCTION: {
      SExprNode*   pExpr = (SExprNode*)pNode;
      SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      if (NULL == pCol) {
        return DEAL_RES_ERROR;
      }
      pCol->node.resType = pExpr->resType;
      strcpy(pCol->colName, pExpr->aliasName);
      return (TSDB_CODE_SUCCESS == nodesListStrictAppend(pCxt->pList, (SNode*)pCol) ? DEAL_RES_IGNORE_CHILD
                                                                                    : DEAL_RES_ERROR);
    }
    default:
      break;
  }

  return DEAL_RES_CONTINUE;
}

int32_t createColumnByRewriteExprs(SNodeList* pExprs, SNodeList** pList) {
  SCreateColumnCxt cxt = {.errCode = TSDB_CODE_SUCCESS, .pList = (NULL == *pList ? nodesMakeList() : *pList)};
  if (NULL == cxt.pList) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  nodesWalkExprs(pExprs, doCreateColumn, &cxt);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyList(cxt.pList);
    return cxt.errCode;
  }
  if (NULL == *pList) {
    *pList = cxt.pList;
  }
  return cxt.errCode;
}

int32_t createColumnByRewriteExpr(SNode* pExpr, SNodeList** pList) {
  SCreateColumnCxt cxt = {.errCode = TSDB_CODE_SUCCESS, .pList = (NULL == *pList ? nodesMakeList() : *pList)};
  if (NULL == cxt.pList) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  nodesWalkExpr(pExpr, doCreateColumn, &cxt);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyList(cxt.pList);
    return cxt.errCode;
  }
  if (NULL == *pList) {
    *pList = cxt.pList;
  }
  return cxt.errCode;
}

int32_t replaceLogicNode(SLogicSubplan* pSubplan, SLogicNode* pOld, SLogicNode* pNew) {
  if (NULL == pOld->pParent) {
    pSubplan->pNode = (SLogicNode*)pNew;
    pNew->pParent = NULL;
    return TSDB_CODE_SUCCESS;
  }

  SNode* pNode;
  FOREACH(pNode, pOld->pParent->pChildren) {
    if (nodesEqualNode(pNode, (SNode*)pOld)) {
      REPLACE_NODE(pNew);
      pNew->pParent = pOld->pParent;
      return TSDB_CODE_SUCCESS;
    }
  }
  return TSDB_CODE_PLAN_INTERNAL_ERROR;
}
