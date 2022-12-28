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
    case QUERY_NODE_FUNCTION:
    case QUERY_NODE_CASE_WHEN: {
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

static int32_t adjustScanDataRequirement(SScanLogicNode* pScan, EDataOrderLevel requirement) {
  if ((SCAN_TYPE_TABLE != pScan->scanType && SCAN_TYPE_TABLE_MERGE != pScan->scanType) ||
      DATA_ORDER_LEVEL_GLOBAL == pScan->node.requireDataOrder) {
    return TSDB_CODE_SUCCESS;
  }
  // The lowest sort level of scan output data is DATA_ORDER_LEVEL_IN_BLOCK
  if (requirement < DATA_ORDER_LEVEL_IN_BLOCK) {
    requirement = DATA_ORDER_LEVEL_IN_BLOCK;
  }
  if (DATA_ORDER_LEVEL_IN_BLOCK == requirement) {
    pScan->scanType = SCAN_TYPE_TABLE;
  } else if (TSDB_SUPER_TABLE == pScan->tableType) {
    pScan->scanType = SCAN_TYPE_TABLE_MERGE;
  }

  if (TSDB_NORMAL_TABLE != pScan->tableType && TSDB_CHILD_TABLE != pScan->tableType) {
    pScan->node.resultDataOrder = requirement;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t adjustJoinDataRequirement(SJoinLogicNode* pJoin, EDataOrderLevel requirement) {
  // The lowest sort level of join input and output data is DATA_ORDER_LEVEL_GLOBAL
  return TSDB_CODE_SUCCESS;
}

static int32_t adjustAggDataRequirement(SAggLogicNode* pAgg, EDataOrderLevel requirement) {
  // The sort level of agg with group by output data can only be DATA_ORDER_LEVEL_NONE
  if (requirement > DATA_ORDER_LEVEL_NONE && (NULL != pAgg->pGroupKeys || !pAgg->onlyHasKeepOrderFunc)) {
    planError(
        "The output of aggregate cannot meet the requirements(%s) of the upper operator. "
        "Illegal statement, should be intercepted in parser",
        dataOrderStr(requirement));
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }
  pAgg->node.resultDataOrder = requirement;
  if (pAgg->hasTimeLineFunc) {
    pAgg->node.requireDataOrder = requirement < DATA_ORDER_LEVEL_IN_GROUP ? DATA_ORDER_LEVEL_IN_GROUP : requirement;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t adjustProjectDataRequirement(SProjectLogicNode* pProject, EDataOrderLevel requirement) {
  pProject->node.resultDataOrder = requirement;
  pProject->node.requireDataOrder = requirement;
  return TSDB_CODE_SUCCESS;
}

static int32_t adjustIntervalDataRequirement(SWindowLogicNode* pWindow, EDataOrderLevel requirement) {
  // The lowest sort level of interval output data is DATA_ORDER_LEVEL_IN_GROUP
  if (requirement < DATA_ORDER_LEVEL_IN_GROUP) {
    requirement = DATA_ORDER_LEVEL_IN_GROUP;
  }
  // The sort level of interval input data is always DATA_ORDER_LEVEL_IN_BLOCK
  pWindow->node.resultDataOrder = requirement;
  return TSDB_CODE_SUCCESS;
}

static int32_t adjustSessionDataRequirement(SWindowLogicNode* pWindow, EDataOrderLevel requirement) {
  if (requirement <= pWindow->node.resultDataOrder) {
    return TSDB_CODE_SUCCESS;
  }
  pWindow->node.resultDataOrder = requirement;
  pWindow->node.requireDataOrder = requirement;
  return TSDB_CODE_SUCCESS;
}

static int32_t adjustStateDataRequirement(SWindowLogicNode* pWindow, EDataOrderLevel requirement) {
  if (requirement <= pWindow->node.resultDataOrder) {
    return TSDB_CODE_SUCCESS;
  }
  pWindow->node.resultDataOrder = requirement;
  pWindow->node.requireDataOrder = requirement;
  return TSDB_CODE_SUCCESS;
}

static int32_t adjustWindowDataRequirement(SWindowLogicNode* pWindow, EDataOrderLevel requirement) {
  switch (pWindow->winType) {
    case WINDOW_TYPE_INTERVAL:
      return adjustIntervalDataRequirement(pWindow, requirement);
    case WINDOW_TYPE_SESSION:
      return adjustSessionDataRequirement(pWindow, requirement);
    case WINDOW_TYPE_STATE:
      return adjustStateDataRequirement(pWindow, requirement);
    default:
      break;
  }
  return TSDB_CODE_PLAN_INTERNAL_ERROR;
}

static int32_t adjustFillDataRequirement(SFillLogicNode* pFill, EDataOrderLevel requirement) {
  if (requirement <= pFill->node.requireDataOrder) {
    return TSDB_CODE_SUCCESS;
  }
  pFill->node.resultDataOrder = requirement;
  pFill->node.requireDataOrder = requirement;
  return TSDB_CODE_SUCCESS;
}

static int32_t adjustSortDataRequirement(SSortLogicNode* pSort, EDataOrderLevel requirement) {
  return TSDB_CODE_SUCCESS;
}

static int32_t adjustPartitionDataRequirement(SPartitionLogicNode* pPart, EDataOrderLevel requirement) {
  if (DATA_ORDER_LEVEL_GLOBAL == requirement) {
    planError(
        "The output of partition cannot meet the requirements(%s) of the upper operator. "
        "Illegal statement, should be intercepted in parser",
        dataOrderStr(requirement));
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }
  pPart->node.resultDataOrder = requirement;
  pPart->node.requireDataOrder =
      (requirement >= DATA_ORDER_LEVEL_IN_BLOCK ? DATA_ORDER_LEVEL_GLOBAL : DATA_ORDER_LEVEL_NONE);
  return TSDB_CODE_SUCCESS;
}

static int32_t adjustIndefRowsDataRequirement(SIndefRowsFuncLogicNode* pIndef, EDataOrderLevel requirement) {
  if (requirement <= pIndef->node.resultDataOrder) {
    return TSDB_CODE_SUCCESS;
  }
  pIndef->node.resultDataOrder = requirement;
  pIndef->node.requireDataOrder = requirement;
  return TSDB_CODE_SUCCESS;
}

static int32_t adjustInterpDataRequirement(SInterpFuncLogicNode* pInterp, EDataOrderLevel requirement) {
  if (requirement <= pInterp->node.requireDataOrder) {
    return TSDB_CODE_SUCCESS;
  }
  pInterp->node.resultDataOrder = requirement;
  pInterp->node.requireDataOrder = requirement;
  return TSDB_CODE_SUCCESS;
}

int32_t adjustLogicNodeDataRequirement(SLogicNode* pNode, EDataOrderLevel requirement) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (nodeType(pNode)) {
    case QUERY_NODE_LOGIC_PLAN_SCAN:
      code = adjustScanDataRequirement((SScanLogicNode*)pNode, requirement);
      break;
    case QUERY_NODE_LOGIC_PLAN_JOIN:
      code = adjustJoinDataRequirement((SJoinLogicNode*)pNode, requirement);
      break;
    case QUERY_NODE_LOGIC_PLAN_AGG:
      code = adjustAggDataRequirement((SAggLogicNode*)pNode, requirement);
      break;
    case QUERY_NODE_LOGIC_PLAN_PROJECT:
      code = adjustProjectDataRequirement((SProjectLogicNode*)pNode, requirement);
      break;
    case QUERY_NODE_LOGIC_PLAN_VNODE_MODIFY:
    case QUERY_NODE_LOGIC_PLAN_EXCHANGE:
    case QUERY_NODE_LOGIC_PLAN_MERGE:
      break;
    case QUERY_NODE_LOGIC_PLAN_WINDOW:
      code = adjustWindowDataRequirement((SWindowLogicNode*)pNode, requirement);
      break;
    case QUERY_NODE_LOGIC_PLAN_FILL:
      code = adjustFillDataRequirement((SFillLogicNode*)pNode, requirement);
      break;
    case QUERY_NODE_LOGIC_PLAN_SORT:
      code = adjustSortDataRequirement((SSortLogicNode*)pNode, requirement);
      break;
    case QUERY_NODE_LOGIC_PLAN_PARTITION:
      code = adjustPartitionDataRequirement((SPartitionLogicNode*)pNode, requirement);
      break;
    case QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC:
      code = adjustIndefRowsDataRequirement((SIndefRowsFuncLogicNode*)pNode, requirement);
      break;
    case QUERY_NODE_LOGIC_PLAN_INTERP_FUNC:
      code = adjustInterpDataRequirement((SInterpFuncLogicNode*)pNode, requirement);
      break;
    default:
      break;
  }
  if (TSDB_CODE_SUCCESS == code) {
    SNode* pChild = NULL;
    FOREACH(pChild, pNode->pChildren) {
      code = adjustLogicNodeDataRequirement((SLogicNode*)pChild, pNode->requireDataOrder);
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    }
  }
  return code;
}
