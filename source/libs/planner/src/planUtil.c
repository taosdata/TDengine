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
    case TSDB_CODE_PLAN_NOT_SUPPORT_JOIN_COND:
      return "Not supported join conditions";
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
    case QUERY_NODE_VALUE:
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
      if (QUERY_NODE_FUNCTION == nodeType(pNode)) {
        SFunctionNode* pFunc = (SFunctionNode*)pNode;
        if (pFunc->funcType == FUNCTION_TYPE_TBNAME) {
          SValueNode* pVal = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 0);
          if (NULL != pVal) {
            strcpy(pCol->tableAlias, pVal->literal);
            strcpy(pCol->tableName, pVal->literal);
          }
        }
      }
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
  pNew->stmtRoot = pOld->stmtRoot;
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

SLogicNode* getLogicNodeRootNode(SLogicNode* pCurr) {
  while (pCurr) {
    if (pCurr->stmtRoot || NULL == pCurr->pParent) {
      return pCurr;
    }

    pCurr = pCurr->pParent;
  }

  return NULL;
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
    pScan->filesetDelimited = true;
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

static int32_t adjustEventDataRequirement(SWindowLogicNode* pWindow, EDataOrderLevel requirement) {
  if (requirement <= pWindow->node.resultDataOrder) {
    return TSDB_CODE_SUCCESS;
  }
  pWindow->node.resultDataOrder = requirement;
  pWindow->node.requireDataOrder = requirement;
  return TSDB_CODE_SUCCESS;
}

static int32_t adjustCountDataRequirement(SWindowLogicNode* pWindow, EDataOrderLevel requirement) {
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
    case WINDOW_TYPE_EVENT:
      return adjustEventDataRequirement(pWindow, requirement);
    case WINDOW_TYPE_COUNT:
      return adjustCountDataRequirement(pWindow, requirement);
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
  pPart->node.requireDataOrder = requirement;
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

static bool stbNotSystemScan(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    return SCAN_TYPE_SYSTEM_TABLE != ((SScanLogicNode*)pNode)->scanType;
  } else if (QUERY_NODE_LOGIC_PLAN_PARTITION == nodeType(pNode)) {
    return stbNotSystemScan((SLogicNode*)nodesListGetNode(pNode->pChildren, 0));
  } else {
    return true;
  }
}

bool keysHasTbname(SNodeList* pKeys) {
  if (NULL == pKeys) {
    return false;
  }
  SNode* pPartKey = NULL;
  FOREACH(pPartKey, pKeys) {
    if (QUERY_NODE_GROUPING_SET == nodeType(pPartKey)) {
      pPartKey = nodesListGetNode(((SGroupingSetNode*)pPartKey)->pParameterList, 0);
    }
    if ((QUERY_NODE_FUNCTION == nodeType(pPartKey) && FUNCTION_TYPE_TBNAME == ((SFunctionNode*)pPartKey)->funcType) ||
        (QUERY_NODE_COLUMN == nodeType(pPartKey) && COLUMN_TYPE_TBNAME == ((SColumnNode*)pPartKey)->colType)) {
      return true;
    }
  }
  return false;
}

static SNodeList* stbGetPartKeys(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    return ((SScanLogicNode*)pNode)->pGroupTags;
  } else if (QUERY_NODE_LOGIC_PLAN_PARTITION == nodeType(pNode)) {
    return ((SPartitionLogicNode*)pNode)->pPartitionKeys;
  } else {
    return NULL;
  }
}

bool isPartTableAgg(SAggLogicNode* pAgg) {
  if (1 != LIST_LENGTH(pAgg->node.pChildren)) {
    return false;
  }
  if (NULL != pAgg->pGroupKeys) {
    return (pAgg->isGroupTb || keysHasTbname(pAgg->pGroupKeys)) &&
           stbNotSystemScan((SLogicNode*)nodesListGetNode(pAgg->node.pChildren, 0));
  }
  return pAgg->isPartTb || keysHasTbname(stbGetPartKeys((SLogicNode*)nodesListGetNode(pAgg->node.pChildren, 0)));
}

static bool stbHasPartTag(SNodeList* pPartKeys) {
  if (NULL == pPartKeys) {
    return false;
  }
  SNode* pPartKey = NULL;
  FOREACH(pPartKey, pPartKeys) {
    if (QUERY_NODE_GROUPING_SET == nodeType(pPartKey)) {
      pPartKey = nodesListGetNode(((SGroupingSetNode*)pPartKey)->pParameterList, 0);
    }
    if ((QUERY_NODE_FUNCTION == nodeType(pPartKey) && FUNCTION_TYPE_TAGS == ((SFunctionNode*)pPartKey)->funcType) ||
        (QUERY_NODE_COLUMN == nodeType(pPartKey) && COLUMN_TYPE_TAG == ((SColumnNode*)pPartKey)->colType)) {
      return true;
    }
  }
  return false;
}

bool getBatchScanOptionFromHint(SNodeList* pList) {
  SNode* pNode = NULL;
  bool batchScan = true;
  FOREACH(pNode, pList) {
    SHintNode* pHint = (SHintNode*)pNode;
    if (pHint->option == HINT_BATCH_SCAN) {
      batchScan = true;
      break;
    } else if (pHint->option == HINT_NO_BATCH_SCAN) {
      batchScan = false;
      break;
    }
  }

  return batchScan;
}

bool getSortForGroupOptHint(SNodeList* pList) {
  if (!pList) return false;
  SNode* pNode;
  FOREACH(pNode, pList) {
    SHintNode* pHint = (SHintNode*)pNode;
    if (pHint->option == HINT_SORT_FOR_GROUP) {
      return true;
    }
  }
  return false;
}

bool getOptHint(SNodeList* pList, EHintOption hint) {
  if (!pList) return false;
  SNode* pNode;
  FOREACH(pNode, pList) {
    SHintNode* pHint = (SHintNode*)pNode;
    if (pHint->option == hint) {
      return true;
    }
  }
  return false;
}

bool getparaTablesSortOptHint(SNodeList* pList) {
  if (!pList) return false;
  SNode* pNode;
  FOREACH(pNode, pList) {
    SHintNode* pHint = (SHintNode*)pNode;
    if (pHint->option == HINT_PARA_TABLES_SORT) {
      return true;
    }
  }
  return false;
}

int32_t collectTableAliasFromNodes(SNode* pNode, SSHashObj** ppRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  SLogicNode* pCurr = (SLogicNode*)pNode;
  FOREACH(pNode, pCurr->pTargets) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    if (NULL == *ppRes) {
      *ppRes = tSimpleHashInit(5, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
      if (NULL == *ppRes) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
    }

    tSimpleHashPut(*ppRes, pCol->tableAlias, strlen(pCol->tableAlias), NULL, 0);
  }
  
  FOREACH(pNode, pCurr->pChildren) {
    code = collectTableAliasFromNodes(pNode, ppRes);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }
  
  return TSDB_CODE_SUCCESS;
}

bool isPartTagAgg(SAggLogicNode* pAgg) {
  if (1 != LIST_LENGTH(pAgg->node.pChildren)) {
    return false;
  }
  if (pAgg->pGroupKeys) {
    return stbHasPartTag(pAgg->pGroupKeys) &&
      stbNotSystemScan((SLogicNode*)nodesListGetNode(pAgg->node.pChildren, 0));
  }
  return stbHasPartTag(stbGetPartKeys((SLogicNode*)nodesListGetNode(pAgg->node.pChildren, 0)));
}

bool isPartTableWinodw(SWindowLogicNode* pWindow) {
  return pWindow->isPartTb || keysHasTbname(stbGetPartKeys((SLogicNode*)nodesListGetNode(pWindow->node.pChildren, 0)));
}

bool cloneLimit(SLogicNode* pParent, SLogicNode* pChild, uint8_t cloneWhat) {
  SLimitNode* pLimit;
  bool cloned = false;
  if (pParent->pLimit && (cloneWhat & CLONE_LIMIT)) {
    pChild->pLimit = nodesCloneNode(pParent->pLimit);
    pLimit = (SLimitNode*)pChild->pLimit;
    pLimit->limit += pLimit->offset;
    pLimit->offset = 0;
    cloned = true;
  }

  if (pParent->pSlimit && (cloneWhat & CLONE_SLIMIT)) {
    pChild->pSlimit = nodesCloneNode(pParent->pSlimit);
    pLimit = (SLimitNode*)pChild->pSlimit;
    pLimit->limit += pLimit->offset;
    pLimit->offset = 0;
    cloned = true;
  }
  return cloned;
}

static EDealRes partTagsOptHasColImpl(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    if (COLUMN_TYPE_TAG != ((SColumnNode*)pNode)->colType && COLUMN_TYPE_TBNAME != ((SColumnNode*)pNode)->colType) {
      *(bool*)pContext = true;
      return DEAL_RES_END;
    }
  }
  return DEAL_RES_CONTINUE;
}

bool keysHasCol(SNodeList* pKeys) {
  bool hasCol = false;
  nodesWalkExprs(pKeys, partTagsOptHasColImpl, &hasCol);
  return hasCol;
}
