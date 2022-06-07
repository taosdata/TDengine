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

#define SPLIT_FLAG_MASK(n) (1 << n)

#define SPLIT_FLAG_STABLE_SPLIT SPLIT_FLAG_MASK(0)

#define SPLIT_FLAG_SET_MASK(val, mask)  (val) |= (mask)
#define SPLIT_FLAG_TEST_MASK(val, mask) (((val) & (mask)) != 0)

typedef struct SSplitContext {
  SPlanContext* pPlanCxt;
  uint64_t      queryId;
  int32_t       groupId;
  bool          split;
} SSplitContext;

typedef int32_t (*FSplit)(SSplitContext* pCxt, SLogicSubplan* pSubplan);

typedef struct SSplitRule {
  char*  pName;
  FSplit splitFunc;
} SSplitRule;

typedef bool (*FSplFindSplitNode)(SSplitContext* pCxt, SLogicSubplan* pSubplan, void* pInfo);

static void splSetSubplanVgroups(SLogicSubplan* pSubplan, SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    TSWAP(pSubplan->pVgroupList, ((SScanLogicNode*)pNode)->pVgroupList);
  } else {
    if (1 == LIST_LENGTH(pNode->pChildren)) {
      splSetSubplanVgroups(pSubplan, (SLogicNode*)nodesListGetNode(pNode->pChildren, 0));
    }
  }
}

static SLogicSubplan* splCreateScanSubplan(SSplitContext* pCxt, SLogicNode* pNode, int32_t flag) {
  SLogicSubplan* pSubplan = nodesMakeNode(QUERY_NODE_LOGIC_SUBPLAN);
  if (NULL == pSubplan) {
    return NULL;
  }
  pSubplan->id.queryId = pCxt->queryId;
  pSubplan->id.groupId = pCxt->groupId;
  pSubplan->subplanType = SUBPLAN_TYPE_SCAN;
  pSubplan->pNode = pNode;
  pSubplan->pNode->pParent = NULL;
  splSetSubplanVgroups(pSubplan, pNode);
  SPLIT_FLAG_SET_MASK(pSubplan->splitFlag, flag);
  return pSubplan;
}

static int32_t splCreateExchangeNode(SSplitContext* pCxt, SLogicNode* pChild, SExchangeLogicNode** pOutput) {
  SExchangeLogicNode* pExchange = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_EXCHANGE);
  if (NULL == pExchange) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pExchange->srcGroupId = pCxt->groupId;
  pExchange->node.precision = pChild->precision;
  pExchange->node.pTargets = nodesCloneList(pChild->pTargets);
  if (NULL == pExchange->node.pTargets) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  *pOutput = pExchange;
  return TSDB_CODE_SUCCESS;
}

static int32_t splReplaceLogicNode(SLogicSubplan* pSubplan, SLogicNode* pOld, SLogicNode* pNew) {
  if (NULL == pOld->pParent) {
    pSubplan->pNode = (SLogicNode*)pNew;
    return TSDB_CODE_SUCCESS;
  }

  SNode* pNode;
  FOREACH(pNode, pOld->pParent->pChildren) {
    if (nodesEqualNode(pNode, pOld)) {
      REPLACE_NODE(pNew);
      pNew->pParent = pOld->pParent;
      return TSDB_CODE_SUCCESS;
    }
  }
  return TSDB_CODE_PLAN_INTERNAL_ERROR;
}

static int32_t splCreateExchangeNodeForSubplan(SSplitContext* pCxt, SLogicSubplan* pSubplan, SLogicNode* pSplitNode,
                                               ESubplanType subplanType) {
  SExchangeLogicNode* pExchange = NULL;
  int32_t             code = splCreateExchangeNode(pCxt, pSplitNode, &pExchange);
  if (TSDB_CODE_SUCCESS == code) {
    code = splReplaceLogicNode(pSubplan, pSplitNode, (SLogicNode*)pExchange);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pSubplan->subplanType = subplanType;
  } else {
    nodesDestroyNode(pExchange);
  }
  return code;
}

static bool splMatch(SSplitContext* pCxt, SLogicSubplan* pSubplan, int32_t flag, FSplFindSplitNode func, void* pInfo) {
  if (!SPLIT_FLAG_TEST_MASK(pSubplan->splitFlag, flag)) {
    if (func(pCxt, pSubplan, pInfo)) {
      return true;
    }
  }
  SNode* pChild;
  FOREACH(pChild, pSubplan->pChildren) {
    if (splMatch(pCxt, (SLogicSubplan*)pChild, flag, func, pInfo)) {
      return true;
    }
  }
  return false;
}

typedef struct SStableSplitInfo {
  SLogicNode*    pSplitNode;
  SLogicSubplan* pSubplan;
} SStableSplitInfo;

static bool stbSplHasGatherExecFunc(const SNodeList* pFuncs) {
  SNode* pFunc = NULL;
  FOREACH(pFunc, pFuncs) {
    if (!fmIsDistExecFunc(((SFunctionNode*)pFunc)->funcId)) {
      return true;
    }
  }
  return false;
}

static bool stbSplIsMultiTbScan(bool streamQuery, SScanLogicNode* pScan) {
  return (NULL != pScan->pVgroupList && pScan->pVgroupList->numOfVgroups > 1) ||
         (streamQuery && TSDB_SUPER_TABLE == pScan->tableType);
}

static bool stbSplHasMultiTbScan(bool streamQuery, SLogicNode* pNode) {
  if (1 != LIST_LENGTH(pNode->pChildren)) {
    return false;
  }
  SNode* pChild = nodesListGetNode(pNode->pChildren, 0);
  return (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pChild) && stbSplIsMultiTbScan(streamQuery, (SScanLogicNode*)pChild));
}

static bool stbSplNeedSplit(bool streamQuery, SLogicNode* pNode) {
  switch (nodeType(pNode)) {
    case QUERY_NODE_LOGIC_PLAN_AGG:
      return !stbSplHasGatherExecFunc(((SAggLogicNode*)pNode)->pAggFuncs) && stbSplHasMultiTbScan(streamQuery, pNode);
    case QUERY_NODE_LOGIC_PLAN_WINDOW: {
      SWindowLogicNode* pWindow = (SWindowLogicNode*)pNode;
      if (WINDOW_TYPE_INTERVAL != pWindow->winType) {
        return false;
      }
      return !stbSplHasGatherExecFunc(pWindow->pFuncs) && stbSplHasMultiTbScan(streamQuery, pNode);
    }
    // case QUERY_NODE_LOGIC_PLAN_SORT:
    //   return stbSplHasMultiTbScan(streamQuery, pNode);
    case QUERY_NODE_LOGIC_PLAN_SCAN:
      return stbSplIsMultiTbScan(streamQuery, (SScanLogicNode*)pNode);
    default:
      break;
  }
  return false;
}

static SLogicNode* stbSplMatchByNode(bool streamQuery, SLogicNode* pNode) {
  if (stbSplNeedSplit(streamQuery, pNode)) {
    return pNode;
  }
  SNode* pChild;
  FOREACH(pChild, pNode->pChildren) {
    SLogicNode* pSplitNode = stbSplMatchByNode(streamQuery, (SLogicNode*)pChild);
    if (NULL != pSplitNode) {
      return pSplitNode;
    }
  }
  return NULL;
}

static bool stbSplFindSplitNode(SSplitContext* pCxt, SLogicSubplan* pSubplan, SStableSplitInfo* pInfo) {
  SLogicNode* pSplitNode = stbSplMatchByNode(pCxt->pPlanCxt->streamQuery, pSubplan->pNode);
  if (NULL != pSplitNode) {
    pInfo->pSplitNode = pSplitNode;
    pInfo->pSubplan = pSubplan;
  }
  return NULL != pSplitNode;
}

static int32_t stbSplRewriteFuns(const SNodeList* pFuncs, SNodeList** pPartialFuncs, SNodeList** pMergeFuncs) {
  SNode* pNode = NULL;
  FOREACH(pNode, pFuncs) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    SFunctionNode* pPartFunc = NULL;
    SFunctionNode* pMergeFunc = NULL;
    int32_t        code = TSDB_CODE_SUCCESS;
    if (fmIsWindowPseudoColumnFunc(pFunc->funcId)) {
      pPartFunc = nodesCloneNode(pFunc);
      pMergeFunc = nodesCloneNode(pFunc);
      if (NULL == pPartFunc || NULL == pMergeFunc) {
        nodesDestroyNode(pPartFunc);
        nodesDestroyNode(pMergeFunc);
        code = TSDB_CODE_OUT_OF_MEMORY;
      }
    } else {
      code = fmGetDistMethod(pFunc, &pPartFunc, &pMergeFunc);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListMakeStrictAppend(pPartialFuncs, pPartFunc);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListMakeStrictAppend(pMergeFuncs, pMergeFunc);
    }
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyList(*pPartialFuncs);
      nodesDestroyList(*pMergeFuncs);
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t stbSplAppendWStart(SNodeList* pFuncs, int32_t* pIndex) {
  int32_t index = 0;
  SNode*  pFunc = NULL;
  FOREACH(pFunc, pFuncs) {
    if (FUNCTION_TYPE_WSTARTTS == ((SFunctionNode*)pFunc)->funcType) {
      *pIndex = index;
      return TSDB_CODE_SUCCESS;
    }
    ++index;
  }

  SFunctionNode* pWStart = nodesMakeNode(QUERY_NODE_FUNCTION);
  if (NULL == pWStart) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  strcpy(pWStart->functionName, "_wstartts");
  snprintf(pWStart->node.aliasName, sizeof(pWStart->node.aliasName), "%s.%p", pWStart->functionName, pWStart);
  int32_t code = fmGetFuncInfo(pWStart, NULL, 0);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListStrictAppend(pFuncs, pWStart);
  }
  *pIndex = index;
  return code;
}

static int32_t stbSplCreatePartWindowNode(SWindowLogicNode* pMergeWindow, SLogicNode** pPartWindow) {
  SNodeList* pFunc = pMergeWindow->pFuncs;
  pMergeWindow->pFuncs = NULL;
  SNodeList* pTargets = pMergeWindow->node.pTargets;
  pMergeWindow->node.pTargets = NULL;
  SNodeList* pChildren = pMergeWindow->node.pChildren;
  pMergeWindow->node.pChildren = NULL;

  int32_t           code = TSDB_CODE_SUCCESS;
  SWindowLogicNode* pPartWin = nodesCloneNode(pMergeWindow);
  if (NULL == pPartWin) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }

  if (TSDB_CODE_SUCCESS == code) {
    pMergeWindow->node.pTargets = pTargets;
    pPartWin->node.pChildren = pChildren;
    code = stbSplRewriteFuns(pFunc, &pPartWin->pFuncs, &pMergeWindow->pFuncs);
  }
  int32_t index = 0;
  if (TSDB_CODE_SUCCESS == code) {
    code = stbSplAppendWStart(pPartWin->pFuncs, &index);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExps(pPartWin->pFuncs, &pPartWin->node.pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) {
    nodesDestroyNode(pMergeWindow->pTspk);
    pMergeWindow->pTspk = nodesCloneNode(nodesListGetNode(pPartWin->node.pTargets, index));
    if (NULL == pMergeWindow->pTspk) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  nodesDestroyList(pFunc);
  if (TSDB_CODE_SUCCESS == code) {
    *pPartWindow = (SLogicNode*)pPartWin;
  } else {
    nodesDestroyNode(pPartWin);
  }

  return code;
}

static int32_t stbSplCreateMergeNode(SSplitContext* pCxt, SLogicSubplan* pSubplan, SLogicNode* pSplitNode,
                                     SNodeList* pMergeKeys, SLogicNode* pPartChild) {
  SMergeLogicNode* pMerge = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_MERGE);
  if (NULL == pMerge) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pMerge->numOfChannels = ((SScanLogicNode*)nodesListGetNode(pPartChild->pChildren, 0))->pVgroupList->numOfVgroups;
  pMerge->srcGroupId = pCxt->groupId;
  pMerge->node.precision = pPartChild->precision;
  pMerge->pMergeKeys = pMergeKeys;

  int32_t code = TSDB_CODE_SUCCESS;
  pMerge->pInputs = nodesCloneList(pPartChild->pTargets);
  pMerge->node.pTargets = nodesCloneList(pSplitNode->pTargets);
  if (NULL == pMerge->node.pTargets || NULL == pMerge->pInputs) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }
  if (TSDB_CODE_SUCCESS == code) {
    if (NULL == pSubplan) {
      code = nodesListMakeAppend(&pSplitNode->pChildren, pMerge);
    } else {
      code = splReplaceLogicNode(pSubplan, pSplitNode, (SLogicNode*)pMerge);
    }
  }
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pMerge);
  }
  return code;
}

static int32_t stbSplCreateExchangeNode(SSplitContext* pCxt, SLogicNode* pParent, SLogicNode* pPartChild) {
  SExchangeLogicNode* pExchange = NULL;
  int32_t             code = splCreateExchangeNode(pCxt, pPartChild, &pExchange);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeAppend(&pParent->pChildren, pExchange);
  }
  return code;
}

static int32_t stbSplSplitIntervalForBatch(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  SLogicNode* pPartWindow = NULL;
  int32_t     code = stbSplCreatePartWindowNode((SWindowLogicNode*)pInfo->pSplitNode, &pPartWindow);
  if (TSDB_CODE_SUCCESS == code) {
    ((SWindowLogicNode*)pPartWindow)->intervalAlgo = INTERVAL_ALGO_HASH;
    ((SWindowLogicNode*)pInfo->pSplitNode)->intervalAlgo = INTERVAL_ALGO_MERGE;
    SNodeList* pMergeKeys = NULL;
    code = nodesListMakeStrictAppend(&pMergeKeys, nodesCloneNode(((SWindowLogicNode*)pInfo->pSplitNode)->pTspk));
    if (TSDB_CODE_SUCCESS == code) {
      code = stbSplCreateMergeNode(pCxt, NULL, pInfo->pSplitNode, pMergeKeys, pPartWindow);
    }
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyList(pMergeKeys);
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pInfo->pSubplan->pChildren,
                                     splCreateScanSubplan(pCxt, pPartWindow, SPLIT_FLAG_STABLE_SPLIT));
  }
  pInfo->pSubplan->subplanType = SUBPLAN_TYPE_MERGE;
  return code;
}

static int32_t stbSplSplitIntervalForStream(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  SLogicNode* pPartWindow = NULL;
  int32_t     code = stbSplCreatePartWindowNode((SWindowLogicNode*)pInfo->pSplitNode, &pPartWindow);
  if (TSDB_CODE_SUCCESS == code) {
    ((SWindowLogicNode*)pPartWindow)->intervalAlgo = INTERVAL_ALGO_STREAM_SEMI;
    ((SWindowLogicNode*)pInfo->pSplitNode)->intervalAlgo = INTERVAL_ALGO_STREAM_FINAL;
    code = stbSplCreateExchangeNode(pCxt, pInfo->pSplitNode, pPartWindow);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pInfo->pSubplan->pChildren,
                                     splCreateScanSubplan(pCxt, pPartWindow, SPLIT_FLAG_STABLE_SPLIT));
  }
  pInfo->pSubplan->subplanType = SUBPLAN_TYPE_MERGE;
  return code;
}

static int32_t stbSplSplitInterval(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  if (pCxt->pPlanCxt->streamQuery) {
    return stbSplSplitIntervalForStream(pCxt, pInfo);
  } else {
    return stbSplSplitIntervalForBatch(pCxt, pInfo);
  }
}

static int32_t stbSplSplitSession(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  return TSDB_CODE_PLAN_INTERNAL_ERROR;
}

static int32_t stbSplSplitWindowNode(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  switch (((SWindowLogicNode*)pInfo->pSplitNode)->winType) {
    case WINDOW_TYPE_INTERVAL:
      return stbSplSplitInterval(pCxt, pInfo);
    case WINDOW_TYPE_SESSION:
      return stbSplSplitSession(pCxt, pInfo);
    default:
      break;
  }
  return TSDB_CODE_PLAN_INTERNAL_ERROR;
}

static int32_t stbSplCreatePartAggNode(SAggLogicNode* pMergeAgg, SLogicNode** pOutput) {
  SNodeList* pFunc = pMergeAgg->pAggFuncs;
  pMergeAgg->pAggFuncs = NULL;
  SNodeList* pGroupKeys = pMergeAgg->pGroupKeys;
  pMergeAgg->pGroupKeys = NULL;
  SNodeList* pTargets = pMergeAgg->node.pTargets;
  pMergeAgg->node.pTargets = NULL;
  SNodeList* pChildren = pMergeAgg->node.pChildren;
  pMergeAgg->node.pChildren = NULL;

  int32_t        code = TSDB_CODE_SUCCESS;
  SAggLogicNode* pPartAgg = nodesCloneNode(pMergeAgg);
  if (NULL == pPartAgg) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pGroupKeys) {
    pPartAgg->pGroupKeys = pGroupKeys;
    code = createColumnByRewriteExps(pPartAgg->pGroupKeys, &pPartAgg->node.pTargets);
  }
  if (TSDB_CODE_SUCCESS == code && NULL != pGroupKeys) {
    pMergeAgg->pGroupKeys = nodesCloneList(pPartAgg->node.pTargets);
    if (NULL == pMergeAgg->pGroupKeys) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    pMergeAgg->node.pTargets = pTargets;
    pPartAgg->node.pChildren = pChildren;

    code = stbSplRewriteFuns(pFunc, &pPartAgg->pAggFuncs, &pMergeAgg->pAggFuncs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExps(pPartAgg->pAggFuncs, &pPartAgg->node.pTargets);
  }

  nodesDestroyList(pFunc);
  if (TSDB_CODE_SUCCESS == code) {
    *pOutput = (SLogicNode*)pPartAgg;
  } else {
    nodesDestroyNode(pPartAgg);
  }

  return code;
}

static int32_t stbSplSplitAggNode(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  SLogicNode* pPartAgg = NULL;
  int32_t     code = stbSplCreatePartAggNode((SAggLogicNode*)pInfo->pSplitNode, &pPartAgg);
  if (TSDB_CODE_SUCCESS == code) {
    code = stbSplCreateExchangeNode(pCxt, pInfo->pSplitNode, pPartAgg);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pInfo->pSubplan->pChildren,
                                     splCreateScanSubplan(pCxt, pPartAgg, SPLIT_FLAG_STABLE_SPLIT));
  }
  pInfo->pSubplan->subplanType = SUBPLAN_TYPE_MERGE;
  return code;
}

static SNode* stbSplCreateColumnNode(SExprNode* pExpr) {
  SColumnNode* pCol = nodesMakeNode(QUERY_NODE_COLUMN);
  if (NULL == pCol) {
    return NULL;
  }
  if (QUERY_NODE_COLUMN == nodeType(pExpr)) {
    strcpy(pCol->tableAlias, ((SColumnNode*)pExpr)->tableAlias);
  }
  strcpy(pCol->colName, pExpr->aliasName);
  strcpy(pCol->node.aliasName, pExpr->aliasName);
  pCol->node.resType = pExpr->resType;
  return (SNode*)pCol;
}

static SNode* stbSplCreateOrderByExpr(SOrderByExprNode* pSortKey, SNode* pCol) {
  SOrderByExprNode* pOutput = nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR);
  if (NULL == pOutput) {
    return NULL;
  }
  pOutput->pExpr = nodesCloneNode(pCol);
  if (NULL == pOutput->pExpr) {
    nodesDestroyNode(pOutput);
    return NULL;
  }
  pOutput->order = pSortKey->order;
  pOutput->nullOrder = pSortKey->nullOrder;
  return (SNode*)pOutput;
}

static int32_t stbSplCreateMergeKeys(SNodeList* pSortKeys, SNodeList* pTargets, SNodeList** pOutput) {
  int32_t    code = TSDB_CODE_SUCCESS;
  SNodeList* pMergeKeys = NULL;
  SNode*     pNode = NULL;
  FOREACH(pNode, pSortKeys) {
    SOrderByExprNode* pSortKey = (SOrderByExprNode*)pNode;
    SNode*            pTarget = NULL;
    bool              found = false;
    FOREACH(pTarget, pTargets) {
      if (0 == strcmp(((SExprNode*)pSortKey->pExpr)->aliasName, ((SColumnNode*)pTarget)->colName)) {
        code = nodesListMakeStrictAppend(&pMergeKeys, stbSplCreateOrderByExpr(pSortKey, pTarget));
        if (TSDB_CODE_SUCCESS != code) {
          break;
        }
        found = true;
      }
    }
    if (TSDB_CODE_SUCCESS == code && !found) {
      SNode* pCol = stbSplCreateColumnNode((SExprNode*)pSortKey->pExpr);
      code = nodesListMakeStrictAppend(&pMergeKeys, stbSplCreateOrderByExpr(pSortKey, pCol));
      if (TSDB_CODE_SUCCESS == code) {
        code = nodesListStrictAppend(pTargets, pCol);
      } else {
        nodesDestroyNode(pCol);
      }
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    *pOutput = pMergeKeys;
  } else {
    nodesDestroyList(pMergeKeys);
  }
  return code;
}

static int32_t stbSplCreatePartSortNode(SSortLogicNode* pSort, SLogicNode** pOutputPartSort,
                                        SNodeList** pOutputMergeKeys) {
  SNodeList* pSortKeys = pSort->pSortKeys;
  pSort->pSortKeys = NULL;
  SNodeList* pChildren = pSort->node.pChildren;
  pSort->node.pChildren = NULL;

  int32_t         code = TSDB_CODE_SUCCESS;
  SSortLogicNode* pPartSort = nodesCloneNode(pSort);
  if (NULL == pPartSort) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }

  SNodeList* pMergeKeys = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    pPartSort->node.pChildren = pChildren;
    pPartSort->pSortKeys = pSortKeys;
    code = stbSplCreateMergeKeys(pPartSort->pSortKeys, pPartSort->node.pTargets, &pMergeKeys);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pOutputPartSort = (SLogicNode*)pPartSort;
    *pOutputMergeKeys = pMergeKeys;
  } else {
    nodesDestroyNode(pPartSort);
    nodesDestroyList(pMergeKeys);
  }

  return code;
}

static int32_t stbSplSplitSortNode(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  SLogicNode* pPartSort = NULL;
  SNodeList*  pMergeKeys = NULL;
  int32_t     code = stbSplCreatePartSortNode((SSortLogicNode*)pInfo->pSplitNode, &pPartSort, &pMergeKeys);
  if (TSDB_CODE_SUCCESS == code) {
    code = stbSplCreateMergeNode(pCxt, pInfo->pSubplan, pInfo->pSplitNode, pMergeKeys, pPartSort);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pInfo->pSubplan->pChildren,
                                     splCreateScanSubplan(pCxt, pPartSort, SPLIT_FLAG_STABLE_SPLIT));
  }
  pInfo->pSubplan->subplanType = SUBPLAN_TYPE_MERGE;
  return code;
}

static int32_t stbSplSplitScanNode(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  int32_t code = splCreateExchangeNodeForSubplan(pCxt, pInfo->pSubplan, pInfo->pSplitNode, SUBPLAN_TYPE_MERGE);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pInfo->pSubplan->pChildren,
                                     splCreateScanSubplan(pCxt, pInfo->pSplitNode, SPLIT_FLAG_STABLE_SPLIT));
  }
  return code;
}

static int32_t stableSplit(SSplitContext* pCxt, SLogicSubplan* pSubplan) {
  if (pCxt->pPlanCxt->rSmaQuery) {
    return TSDB_CODE_SUCCESS;
  }

  SStableSplitInfo info = {0};
  if (!splMatch(pCxt, pSubplan, SPLIT_FLAG_STABLE_SPLIT, (FSplFindSplitNode)stbSplFindSplitNode, &info)) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  switch (nodeType(info.pSplitNode)) {
    case QUERY_NODE_LOGIC_PLAN_AGG:
      code = stbSplSplitAggNode(pCxt, &info);
      break;
    case QUERY_NODE_LOGIC_PLAN_WINDOW:
      code = stbSplSplitWindowNode(pCxt, &info);
      break;
    case QUERY_NODE_LOGIC_PLAN_SORT:
      code = stbSplSplitSortNode(pCxt, &info);
      break;
    case QUERY_NODE_LOGIC_PLAN_SCAN:
      code = stbSplSplitScanNode(pCxt, &info);
      break;
    default:
      break;
  }

  ++(pCxt->groupId);
  pCxt->split = true;
  return code;
}

typedef struct SSigTbJoinSplitInfo {
  SJoinLogicNode* pJoin;
  SLogicNode*     pSplitNode;
  SLogicSubplan*  pSubplan;
} SSigTbJoinSplitInfo;

static bool sigTbJoinSplNeedSplit(SJoinLogicNode* pJoin) {
  if (!pJoin->isSingleTableJoin) {
    return false;
  }
  return QUERY_NODE_LOGIC_PLAN_EXCHANGE != nodeType(nodesListGetNode(pJoin->node.pChildren, 0)) &&
         QUERY_NODE_LOGIC_PLAN_EXCHANGE != nodeType(nodesListGetNode(pJoin->node.pChildren, 1));
}

static SJoinLogicNode* sigTbJoinSplMatchByNode(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_JOIN == nodeType(pNode) && sigTbJoinSplNeedSplit((SJoinLogicNode*)pNode)) {
    return (SJoinLogicNode*)pNode;
  }
  SNode* pChild;
  FOREACH(pChild, pNode->pChildren) {
    SJoinLogicNode* pSplitNode = sigTbJoinSplMatchByNode((SLogicNode*)pChild);
    if (NULL != pSplitNode) {
      return pSplitNode;
    }
  }
  return NULL;
}

static bool sigTbJoinSplFindSplitNode(SSplitContext* pCxt, SLogicSubplan* pSubplan, SSigTbJoinSplitInfo* pInfo) {
  SJoinLogicNode* pJoin = sigTbJoinSplMatchByNode(pSubplan->pNode);
  if (NULL != pJoin) {
    pInfo->pJoin = pJoin;
    pInfo->pSplitNode = nodesListGetNode(pJoin->node.pChildren, 1);
    pInfo->pSubplan = pSubplan;
  }
  return NULL != pJoin;
}

static int32_t singleTableJoinSplit(SSplitContext* pCxt, SLogicSubplan* pSubplan) {
  SSigTbJoinSplitInfo info = {0};
  if (!splMatch(pCxt, pSubplan, 0, (FSplFindSplitNode)sigTbJoinSplFindSplitNode, &info)) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t code = splCreateExchangeNodeForSubplan(pCxt, info.pSubplan, info.pSplitNode, info.pSubplan->subplanType);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&info.pSubplan->pChildren, splCreateScanSubplan(pCxt, info.pSplitNode, 0));
  }
  ++(pCxt->groupId);
  pCxt->split = true;
  return code;
}

static bool unionIsChildSubplan(SLogicNode* pLogicNode, int32_t groupId) {
  if (QUERY_NODE_LOGIC_PLAN_EXCHANGE == nodeType(pLogicNode)) {
    return ((SExchangeLogicNode*)pLogicNode)->srcGroupId == groupId;
  }

  SNode* pChild;
  FOREACH(pChild, pLogicNode->pChildren) {
    bool isChild = unionIsChildSubplan((SLogicNode*)pChild, groupId);
    if (isChild) {
      return isChild;
    }
  }
  return false;
}

static int32_t unionMountSubplan(SLogicSubplan* pParent, SNodeList* pChildren) {
  SNode* pChild = NULL;
  WHERE_EACH(pChild, pChildren) {
    if (unionIsChildSubplan(pParent->pNode, ((SLogicSubplan*)pChild)->id.groupId)) {
      int32_t code = nodesListMakeAppend(&pParent->pChildren, pChild);
      if (TSDB_CODE_SUCCESS == code) {
        REPLACE_NODE(NULL);
        ERASE_NODE(pChildren);
        continue;
      } else {
        return code;
      }
    }
    WHERE_NEXT;
  }
  return TSDB_CODE_SUCCESS;
}

static SLogicSubplan* unionCreateSubplan(SSplitContext* pCxt, SLogicNode* pNode) {
  SLogicSubplan* pSubplan = nodesMakeNode(QUERY_NODE_LOGIC_SUBPLAN);
  if (NULL == pSubplan) {
    return NULL;
  }
  pSubplan->id.queryId = pCxt->queryId;
  pSubplan->id.groupId = pCxt->groupId;
  pSubplan->subplanType = SUBPLAN_TYPE_SCAN;
  pSubplan->pNode = pNode;
  pNode->pParent = NULL;
  return pSubplan;
}

static int32_t unionSplitSubplan(SSplitContext* pCxt, SLogicSubplan* pUnionSubplan, SLogicNode* pSplitNode) {
  SNodeList* pSubplanChildren = pUnionSubplan->pChildren;
  pUnionSubplan->pChildren = NULL;

  int32_t code = TSDB_CODE_SUCCESS;

  SNode* pChild = NULL;
  FOREACH(pChild, pSplitNode->pChildren) {
    SLogicSubplan* pNewSubplan = unionCreateSubplan(pCxt, (SLogicNode*)pChild);
    code = nodesListMakeStrictAppend(&pUnionSubplan->pChildren, pNewSubplan);
    if (TSDB_CODE_SUCCESS == code) {
      REPLACE_NODE(NULL);
      code = unionMountSubplan(pNewSubplan, pSubplanChildren);
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    nodesDestroyList(pSubplanChildren);
    DESTORY_LIST(pSplitNode->pChildren);
  }
  return code;
}

typedef struct SUnionAllSplitInfo {
  SProjectLogicNode* pProject;
  SLogicSubplan*     pSubplan;
} SUnionAllSplitInfo;

static SLogicNode* unAllSplMatchByNode(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_PROJECT == nodeType(pNode) && LIST_LENGTH(pNode->pChildren) > 1) {
    return pNode;
  }
  SNode* pChild;
  FOREACH(pChild, pNode->pChildren) {
    SLogicNode* pSplitNode = unAllSplMatchByNode((SLogicNode*)pChild);
    if (NULL != pSplitNode) {
      return pSplitNode;
    }
  }
  return NULL;
}

static bool unAllSplFindSplitNode(SSplitContext* pCxt, SLogicSubplan* pSubplan, SUnionAllSplitInfo* pInfo) {
  SLogicNode* pSplitNode = unAllSplMatchByNode(pSubplan->pNode);
  if (NULL != pSplitNode) {
    pInfo->pProject = (SProjectLogicNode*)pSplitNode;
    pInfo->pSubplan = pSubplan;
  }
  return NULL != pSplitNode;
}

static int32_t unAllSplCreateExchangeNode(SSplitContext* pCxt, SLogicSubplan* pSubplan, SProjectLogicNode* pProject) {
  SExchangeLogicNode* pExchange = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_EXCHANGE);
  if (NULL == pExchange) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pExchange->srcGroupId = pCxt->groupId;
  pExchange->node.precision = pProject->node.precision;
  pExchange->node.pTargets = nodesCloneList(pProject->node.pTargets);
  if (NULL == pExchange->node.pTargets) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pSubplan->subplanType = SUBPLAN_TYPE_MERGE;

  if (NULL == pProject->node.pParent) {
    pSubplan->pNode = (SLogicNode*)pExchange;
    nodesDestroyNode(pProject);
    return TSDB_CODE_SUCCESS;
  }

  SNode* pNode;
  FOREACH(pNode, pProject->node.pParent->pChildren) {
    if (nodesEqualNode(pNode, pProject)) {
      REPLACE_NODE(pExchange);
      nodesDestroyNode(pNode);
      return TSDB_CODE_SUCCESS;
    }
  }
  nodesDestroyNode(pExchange);
  return TSDB_CODE_FAILED;
}

static int32_t unionAllSplit(SSplitContext* pCxt, SLogicSubplan* pSubplan) {
  SUnionAllSplitInfo info = {0};
  if (!splMatch(pCxt, pSubplan, 0, (FSplFindSplitNode)unAllSplFindSplitNode, &info)) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = unionSplitSubplan(pCxt, info.pSubplan, (SLogicNode*)info.pProject);
  if (TSDB_CODE_SUCCESS == code) {
    code = unAllSplCreateExchangeNode(pCxt, info.pSubplan, info.pProject);
  }
  ++(pCxt->groupId);
  pCxt->split = true;
  return code;
}

typedef struct SUnionDistinctSplitInfo {
  SAggLogicNode* pAgg;
  SLogicSubplan* pSubplan;
} SUnionDistinctSplitInfo;

static SLogicNode* unDistSplMatchByNode(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_AGG == nodeType(pNode) && LIST_LENGTH(pNode->pChildren) > 1) {
    return pNode;
  }
  SNode* pChild;
  FOREACH(pChild, pNode->pChildren) {
    SLogicNode* pSplitNode = unDistSplMatchByNode((SLogicNode*)pChild);
    if (NULL != pSplitNode) {
      return pSplitNode;
    }
  }
  return NULL;
}

static int32_t unDistSplCreateExchangeNode(SSplitContext* pCxt, SLogicSubplan* pSubplan, SAggLogicNode* pAgg) {
  SExchangeLogicNode* pExchange = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_EXCHANGE);
  if (NULL == pExchange) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pExchange->srcGroupId = pCxt->groupId;
  pExchange->node.precision = pAgg->node.precision;
  pExchange->node.pTargets = nodesCloneList(pAgg->pGroupKeys);
  if (NULL == pExchange->node.pTargets) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pSubplan->subplanType = SUBPLAN_TYPE_MERGE;

  return nodesListMakeAppend(&pAgg->node.pChildren, pExchange);
}

static bool unDistSplFindSplitNode(SSplitContext* pCxt, SLogicSubplan* pSubplan, SUnionDistinctSplitInfo* pInfo) {
  SLogicNode* pSplitNode = unDistSplMatchByNode(pSubplan->pNode);
  if (NULL != pSplitNode) {
    pInfo->pAgg = (SAggLogicNode*)pSplitNode;
    pInfo->pSubplan = pSubplan;
  }
  return NULL != pSplitNode;
}

static int32_t unionDistinctSplit(SSplitContext* pCxt, SLogicSubplan* pSubplan) {
  SUnionDistinctSplitInfo info = {0};
  if (!splMatch(pCxt, pSubplan, 0, (FSplFindSplitNode)unDistSplFindSplitNode, &info)) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = unionSplitSubplan(pCxt, info.pSubplan, (SLogicNode*)info.pAgg);
  if (TSDB_CODE_SUCCESS == code) {
    code = unDistSplCreateExchangeNode(pCxt, info.pSubplan, info.pAgg);
  }
  ++(pCxt->groupId);
  pCxt->split = true;
  return code;
}

// clang-format off
static const SSplitRule splitRuleSet[] = {
  {.pName = "SuperTableSplit",      .splitFunc = stableSplit},
  {.pName = "SingleTableJoinSplit", .splitFunc = singleTableJoinSplit},
  {.pName = "UnionAllSplit",        .splitFunc = unionAllSplit},
  {.pName = "UnionDistinctSplit",   .splitFunc = unionDistinctSplit}
};
// clang-format on

static const int32_t splitRuleNum = (sizeof(splitRuleSet) / sizeof(SSplitRule));

static void dumpLogicSubplan(const char* pRuleName, SLogicSubplan* pSubplan) {
  char* pStr = NULL;
  nodesNodeToString(pSubplan, false, &pStr, NULL);
  qDebugL("apply %s rule: %s", pRuleName, pStr);
  taosMemoryFree(pStr);
}

static int32_t applySplitRule(SPlanContext* pCxt, SLogicSubplan* pSubplan) {
  SSplitContext cxt = {
      .pPlanCxt = pCxt, .queryId = pSubplan->id.queryId, .groupId = pSubplan->id.groupId + 1, .split = false};
  bool split = false;
  do {
    split = false;
    for (int32_t i = 0; i < splitRuleNum; ++i) {
      cxt.split = false;
      int32_t code = splitRuleSet[i].splitFunc(&cxt, pSubplan);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      if (cxt.split) {
        split = true;
        dumpLogicSubplan(splitRuleSet[i].pName, pSubplan);
      }
    }
  } while (split);
  return TSDB_CODE_SUCCESS;
}

static void doSetLogicNodeParent(SLogicNode* pNode, SLogicNode* pParent) {
  pNode->pParent = pParent;
  SNode* pChild;
  FOREACH(pChild, pNode->pChildren) { doSetLogicNodeParent((SLogicNode*)pChild, pNode); }
}

static void setLogicNodeParent(SLogicNode* pNode) { doSetLogicNodeParent(pNode, NULL); }

static void setVgroupsInfo(SLogicNode* pNode, SLogicSubplan* pSubplan) {
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    TSWAP(((SScanLogicNode*)pNode)->pVgroupList, pSubplan->pVgroupList);
    return;
  }

  SNode* pChild;
  FOREACH(pChild, pNode->pChildren) { setVgroupsInfo((SLogicNode*)pChild, pSubplan); }
}

int32_t splitLogicPlan(SPlanContext* pCxt, SLogicNode* pLogicNode, SLogicSubplan** pLogicSubplan) {
  SLogicSubplan* pSubplan = (SLogicSubplan*)nodesMakeNode(QUERY_NODE_LOGIC_SUBPLAN);
  if (NULL == pSubplan) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pSubplan->pNode = nodesCloneNode(pLogicNode);
  if (NULL == pSubplan->pNode) {
    nodesDestroyNode(pSubplan);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pSubplan->id.queryId = pCxt->queryId;
  pSubplan->id.groupId = 1;
  setLogicNodeParent(pSubplan->pNode);

  int32_t code = TSDB_CODE_SUCCESS;
  if (QUERY_NODE_LOGIC_PLAN_VNODE_MODIFY == nodeType(pLogicNode)) {
    pSubplan->subplanType = SUBPLAN_TYPE_MODIFY;
    TSWAP(((SVnodeModifyLogicNode*)pLogicNode)->pDataBlocks, ((SVnodeModifyLogicNode*)pSubplan->pNode)->pDataBlocks);
    setVgroupsInfo(pSubplan->pNode, pSubplan);
  } else {
    pSubplan->subplanType = SUBPLAN_TYPE_SCAN;
    code = applySplitRule(pCxt, pSubplan);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicSubplan = pSubplan;
  } else {
    nodesDestroyNode(pSubplan);
  }

  return code;
}