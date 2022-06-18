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
#include "tglobal.h"

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

// typedef bool (*FSplFindSplitNode)(SSplitContext* pCxt, SLogicSubplan* pSubplan, void* pInfo);
typedef bool (*FSplFindSplitNode)(SSplitContext* pCxt, SLogicSubplan* pSubplan, SLogicNode* pNode, void* pInfo);

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
  SLogicSubplan* pSubplan = (SLogicSubplan*)nodesMakeNode(QUERY_NODE_LOGIC_SUBPLAN);
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
  SExchangeLogicNode* pExchange = (SExchangeLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_EXCHANGE);
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

static int32_t splCreateExchangeNodeForSubplan(SSplitContext* pCxt, SLogicSubplan* pSubplan, SLogicNode* pSplitNode,
                                               ESubplanType subplanType) {
  SExchangeLogicNode* pExchange = NULL;
  int32_t             code = splCreateExchangeNode(pCxt, pSplitNode, &pExchange);
  if (TSDB_CODE_SUCCESS == code) {
    code = replaceLogicNode(pSubplan, pSplitNode, (SLogicNode*)pExchange);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pSubplan->subplanType = subplanType;
  } else {
    nodesDestroyNode((SNode*)pExchange);
  }
  return code;
}

static bool splMatchByNode(SSplitContext* pCxt, SLogicSubplan* pSubplan, SLogicNode* pNode, FSplFindSplitNode func,
                           void* pInfo) {
  if (func(pCxt, pSubplan, pNode, pInfo)) {
    return true;
  }
  SNode* pChild;
  FOREACH(pChild, pNode->pChildren) {
    if (splMatchByNode(pCxt, pSubplan, (SLogicNode*)pChild, func, pInfo)) {
      return true;
    }
  }
  return NULL;
}

static bool splMatch(SSplitContext* pCxt, SLogicSubplan* pSubplan, int32_t flag, FSplFindSplitNode func, void* pInfo) {
  if (!SPLIT_FLAG_TEST_MASK(pSubplan->splitFlag, flag)) {
    if (splMatchByNode(pCxt, pSubplan, pSubplan->pNode, func, pInfo)) {
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

static void splSetParent(SLogicNode* pNode) {
  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) { ((SLogicNode*)pChild)->pParent = pNode; }
}

typedef struct SStableSplitInfo {
  SLogicNode*    pSplitNode;
  SLogicSubplan* pSubplan;
} SStableSplitInfo;

static bool stbSplHasGatherExecFunc(const SNodeList* pFuncs) {
  SNode* pFunc = NULL;
  FOREACH(pFunc, pFuncs) {
    if (!fmIsWindowPseudoColumnFunc(((SFunctionNode*)pFunc)->funcId) &&
        !fmIsDistExecFunc(((SFunctionNode*)pFunc)->funcId)) {
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
  if (QUERY_NODE_LOGIC_PLAN_PARTITION == nodeType(pChild)) {
    if (1 != LIST_LENGTH(((SLogicNode*)pChild)->pChildren)) {
      return false;
    }
    pChild = nodesListGetNode(((SLogicNode*)pChild)->pChildren, 0);
  }
  return (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pChild) && stbSplIsMultiTbScan(streamQuery, (SScanLogicNode*)pChild));
}

static bool stbSplNeedSplit(bool streamQuery, SLogicNode* pNode) {
  switch (nodeType(pNode)) {
    case QUERY_NODE_LOGIC_PLAN_SCAN:
      return stbSplIsMultiTbScan(streamQuery, (SScanLogicNode*)pNode);
    case QUERY_NODE_LOGIC_PLAN_JOIN:
      return !(((SJoinLogicNode*)pNode)->isSingleTableJoin);
    case QUERY_NODE_LOGIC_PLAN_AGG:
      return !stbSplHasGatherExecFunc(((SAggLogicNode*)pNode)->pAggFuncs) && stbSplHasMultiTbScan(streamQuery, pNode);
    case QUERY_NODE_LOGIC_PLAN_WINDOW: {
      SWindowLogicNode* pWindow = (SWindowLogicNode*)pNode;
      if (WINDOW_TYPE_INTERVAL != pWindow->winType) {
        return false;
      }
      return !stbSplHasGatherExecFunc(pWindow->pFuncs) && stbSplHasMultiTbScan(streamQuery, pNode);
    }
    case QUERY_NODE_LOGIC_PLAN_SORT:
      return stbSplHasMultiTbScan(streamQuery, pNode);
    default:
      break;
  }
  return false;
}

static bool stbSplFindSplitNode(SSplitContext* pCxt, SLogicSubplan* pSubplan, SLogicNode* pNode,
                                SStableSplitInfo* pInfo) {
  if (stbSplNeedSplit(pCxt->pPlanCxt->streamQuery, pNode)) {
    pInfo->pSplitNode = pNode;
    pInfo->pSubplan = pSubplan;
    return true;
  }
  return false;
}

static int32_t stbSplRewriteFuns(const SNodeList* pFuncs, SNodeList** pPartialFuncs, SNodeList** pMergeFuncs) {
  SNode* pNode = NULL;
  FOREACH(pNode, pFuncs) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    SFunctionNode* pPartFunc = NULL;
    SFunctionNode* pMergeFunc = NULL;
    int32_t        code = TSDB_CODE_SUCCESS;
    if (fmIsWindowPseudoColumnFunc(pFunc->funcId)) {
      pPartFunc = (SFunctionNode*)nodesCloneNode(pNode);
      pMergeFunc = (SFunctionNode*)nodesCloneNode(pNode);
      if (NULL == pPartFunc || NULL == pMergeFunc) {
        nodesDestroyNode((SNode*)pPartFunc);
        nodesDestroyNode((SNode*)pMergeFunc);
        code = TSDB_CODE_OUT_OF_MEMORY;
      }
    } else {
      code = fmGetDistMethod(pFunc, &pPartFunc, &pMergeFunc);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListMakeStrictAppend(pPartialFuncs, (SNode*)pPartFunc);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListMakeStrictAppend(pMergeFuncs, (SNode*)pMergeFunc);
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

  SFunctionNode* pWStart = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  if (NULL == pWStart) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  strcpy(pWStart->functionName, "_wstartts");
  snprintf(pWStart->node.aliasName, sizeof(pWStart->node.aliasName), "%s.%p", pWStart->functionName, pWStart);
  int32_t code = fmGetFuncInfo(pWStart, NULL, 0);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListStrictAppend(pFuncs, (SNode*)pWStart);
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
  SWindowLogicNode* pPartWin = (SWindowLogicNode*)nodesCloneNode((SNode*)pMergeWindow);
  if (NULL == pPartWin) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }

  if (TSDB_CODE_SUCCESS == code) {
    pMergeWindow->node.pTargets = pTargets;
    pPartWin->node.pChildren = pChildren;
    splSetParent((SLogicNode*)pPartWin);
    code = stbSplRewriteFuns(pFunc, &pPartWin->pFuncs, &pMergeWindow->pFuncs);
  }
  int32_t index = 0;
  if (TSDB_CODE_SUCCESS == code) {
    code = stbSplAppendWStart(pPartWin->pFuncs, &index);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExprs(pPartWin->pFuncs, &pPartWin->node.pTargets);
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
    nodesDestroyNode((SNode*)pPartWin);
  }

  return code;
}

static int32_t stbSplGetNumOfVgroups(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    return ((SScanLogicNode*)pNode)->pVgroupList->numOfVgroups;
  } else {
    if (1 == LIST_LENGTH(pNode->pChildren)) {
      return stbSplGetNumOfVgroups((SLogicNode*)nodesListGetNode(pNode->pChildren, 0));
    }
  }
  return 0;
}

static int32_t stbSplCreateMergeNode(SSplitContext* pCxt, SLogicSubplan* pSubplan, SLogicNode* pSplitNode,
                                     SNodeList* pMergeKeys, SLogicNode* pPartChild) {
  SMergeLogicNode* pMerge = (SMergeLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_MERGE);
  if (NULL == pMerge) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pMerge->numOfChannels = stbSplGetNumOfVgroups(pPartChild);
  pMerge->srcGroupId = pCxt->groupId;
  pMerge->node.precision = pPartChild->precision;
  pMerge->pMergeKeys = pMergeKeys;

  int32_t code = TSDB_CODE_SUCCESS;
  pMerge->pInputs = nodesCloneList(pPartChild->pTargets);
  // NULL == pSubplan means 'merge node' replaces 'split node'.
  if (NULL == pSubplan) {
    pMerge->node.pTargets = nodesCloneList(pPartChild->pTargets);
  } else {
    pMerge->node.pTargets = nodesCloneList(pSplitNode->pTargets);
  }
  if (NULL == pMerge->node.pTargets || NULL == pMerge->pInputs) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }
  if (TSDB_CODE_SUCCESS == code) {
    if (NULL == pSubplan) {
      code = nodesListMakeAppend(&pSplitNode->pChildren, (SNode*)pMerge);
    } else {
      code = replaceLogicNode(pSubplan, pSplitNode, (SLogicNode*)pMerge);
    }
  }
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pMerge);
  }
  return code;
}

static int32_t stbSplCreateExchangeNode(SSplitContext* pCxt, SLogicNode* pParent, SLogicNode* pPartChild) {
  SExchangeLogicNode* pExchange = NULL;
  int32_t             code = splCreateExchangeNode(pCxt, pPartChild, &pExchange);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeAppend(&pParent->pChildren, (SNode*)pExchange);
  }
  return code;
}

static int32_t stbSplCreateMergeKeysByPrimaryKey(SNode* pPrimaryKey, SNodeList** pMergeKeys) {
  SOrderByExprNode* pMergeKey = (SOrderByExprNode*)nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR);
  if (NULL == pMergeKey) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pMergeKey->pExpr = nodesCloneNode(pPrimaryKey);
  if (NULL == pMergeKey->pExpr) {
    nodesDestroyNode((SNode*)pMergeKey);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pMergeKey->order = ORDER_ASC;
  pMergeKey->nullOrder = NULL_ORDER_FIRST;
  return nodesListMakeStrictAppend(pMergeKeys, (SNode*)pMergeKey);
}

static int32_t stbSplSplitIntervalForBatch(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  SLogicNode* pPartWindow = NULL;
  int32_t     code = stbSplCreatePartWindowNode((SWindowLogicNode*)pInfo->pSplitNode, &pPartWindow);
  if (TSDB_CODE_SUCCESS == code) {
    ((SWindowLogicNode*)pPartWindow)->windowAlgo = INTERVAL_ALGO_HASH;
    ((SWindowLogicNode*)pInfo->pSplitNode)->windowAlgo = INTERVAL_ALGO_MERGE;
    SNodeList* pMergeKeys = NULL;
    code = stbSplCreateMergeKeysByPrimaryKey(((SWindowLogicNode*)pInfo->pSplitNode)->pTspk, &pMergeKeys);
    if (TSDB_CODE_SUCCESS == code) {
      code = stbSplCreateMergeNode(pCxt, NULL, pInfo->pSplitNode, pMergeKeys, pPartWindow);
    }
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyList(pMergeKeys);
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pInfo->pSubplan->pChildren,
                                     (SNode*)splCreateScanSubplan(pCxt, pPartWindow, SPLIT_FLAG_STABLE_SPLIT));
  }
  pInfo->pSubplan->subplanType = SUBPLAN_TYPE_MERGE;
  ++(pCxt->groupId);
  return code;
}

static int32_t stbSplSplitIntervalForStream(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  SLogicNode* pPartWindow = NULL;
  int32_t     code = stbSplCreatePartWindowNode((SWindowLogicNode*)pInfo->pSplitNode, &pPartWindow);
  if (TSDB_CODE_SUCCESS == code) {
    ((SWindowLogicNode*)pPartWindow)->windowAlgo = INTERVAL_ALGO_STREAM_SEMI;
    ((SWindowLogicNode*)pInfo->pSplitNode)->windowAlgo = INTERVAL_ALGO_STREAM_FINAL;
    code = stbSplCreateExchangeNode(pCxt, pInfo->pSplitNode, pPartWindow);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pInfo->pSubplan->pChildren,
                                     (SNode*)splCreateScanSubplan(pCxt, pPartWindow, SPLIT_FLAG_STABLE_SPLIT));
  }
  pInfo->pSubplan->subplanType = SUBPLAN_TYPE_MERGE;
  ++(pCxt->groupId);
  return code;
}

static int32_t stbSplSplitInterval(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  if (pCxt->pPlanCxt->streamQuery) {
    return stbSplSplitIntervalForStream(pCxt, pInfo);
  } else {
    return stbSplSplitIntervalForBatch(pCxt, pInfo);
  }
}

static int32_t stbSplSplitSessionForStream(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  SLogicNode* pPartWindow = NULL;
  int32_t     code = stbSplCreatePartWindowNode((SWindowLogicNode*)pInfo->pSplitNode, &pPartWindow);
  if (TSDB_CODE_SUCCESS == code) {
    ((SWindowLogicNode*)pPartWindow)->windowAlgo = SESSION_ALGO_STREAM_SEMI;
    ((SWindowLogicNode*)pInfo->pSplitNode)->windowAlgo = SESSION_ALGO_STREAM_FINAL;
    code = stbSplCreateExchangeNode(pCxt, pInfo->pSplitNode, pPartWindow);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pInfo->pSubplan->pChildren,
                                     (SNode*)splCreateScanSubplan(pCxt, pPartWindow, SPLIT_FLAG_STABLE_SPLIT));
  }
  pInfo->pSubplan->subplanType = SUBPLAN_TYPE_MERGE;
  ++(pCxt->groupId);
  return code;
}

static int32_t stbSplSplitSession(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  if (pCxt->pPlanCxt->streamQuery) {
    return stbSplSplitSessionForStream(pCxt, pInfo);
  } else {
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }
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
  SNode* pConditions = pMergeAgg->node.pConditions;
  pMergeAgg->node.pConditions = NULL;

  int32_t        code = TSDB_CODE_SUCCESS;
  SAggLogicNode* pPartAgg = (SAggLogicNode*)nodesCloneNode((SNode*)pMergeAgg);
  if (NULL == pPartAgg) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pGroupKeys) {
    pPartAgg->pGroupKeys = pGroupKeys;
    code = createColumnByRewriteExprs(pPartAgg->pGroupKeys, &pPartAgg->node.pTargets);
  }
  if (TSDB_CODE_SUCCESS == code && NULL != pGroupKeys) {
    pMergeAgg->pGroupKeys = nodesCloneList(pPartAgg->node.pTargets);
    if (NULL == pMergeAgg->pGroupKeys) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    pMergeAgg->node.pConditions = pConditions;
    pMergeAgg->node.pTargets = pTargets;
    pPartAgg->node.pChildren = pChildren;
    splSetParent((SLogicNode*)pPartAgg);

    code = stbSplRewriteFuns(pFunc, &pPartAgg->pAggFuncs, &pMergeAgg->pAggFuncs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExprs(pPartAgg->pAggFuncs, &pPartAgg->node.pTargets);
  }

  nodesDestroyList(pFunc);
  if (TSDB_CODE_SUCCESS == code) {
    *pOutput = (SLogicNode*)pPartAgg;
  } else {
    nodesDestroyNode((SNode*)pPartAgg);
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
                                     (SNode*)splCreateScanSubplan(pCxt, pPartAgg, SPLIT_FLAG_STABLE_SPLIT));
  }
  pInfo->pSubplan->subplanType = SUBPLAN_TYPE_MERGE;
  ++(pCxt->groupId);
  return code;
}

static SNode* stbSplCreateColumnNode(SExprNode* pExpr) {
  SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
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
  SOrderByExprNode* pOutput = (SOrderByExprNode*)nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR);
  if (NULL == pOutput) {
    return NULL;
  }
  pOutput->pExpr = nodesCloneNode(pCol);
  if (NULL == pOutput->pExpr) {
    nodesDestroyNode((SNode*)pOutput);
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
    SExprNode*        pSortExpr = (SExprNode*)pSortKey->pExpr;
    SNode*            pTarget = NULL;
    bool              found = false;
    FOREACH(pTarget, pTargets) {
      if ((QUERY_NODE_COLUMN == nodeType(pSortExpr) && nodesEqualNode((SNode*)pSortExpr, pTarget)) ||
          (0 == strcmp(pSortExpr->aliasName, ((SColumnNode*)pTarget)->colName))) {
        code = nodesListMakeStrictAppend(&pMergeKeys, stbSplCreateOrderByExpr(pSortKey, pTarget));
        if (TSDB_CODE_SUCCESS != code) {
          break;
        }
        found = true;
      }
    }
    if (TSDB_CODE_SUCCESS == code && !found) {
      SNode* pCol = stbSplCreateColumnNode(pSortExpr);
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
  SSortLogicNode* pPartSort = (SSortLogicNode*)nodesCloneNode((SNode*)pSort);
  if (NULL == pPartSort) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }

  SNodeList* pMergeKeys = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    pPartSort->node.pChildren = pChildren;
    splSetParent((SLogicNode*)pPartSort);
    pPartSort->pSortKeys = pSortKeys;
    code = stbSplCreateMergeKeys(pPartSort->pSortKeys, pPartSort->node.pTargets, &pMergeKeys);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pOutputPartSort = (SLogicNode*)pPartSort;
    *pOutputMergeKeys = pMergeKeys;
  } else {
    nodesDestroyNode((SNode*)pPartSort);
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
                                     (SNode*)splCreateScanSubplan(pCxt, pPartSort, SPLIT_FLAG_STABLE_SPLIT));
  }
  pInfo->pSubplan->subplanType = SUBPLAN_TYPE_MERGE;
  ++(pCxt->groupId);
  return code;
}

static int32_t stbSplSplitScanNode(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  int32_t code = splCreateExchangeNodeForSubplan(pCxt, pInfo->pSubplan, pInfo->pSplitNode, SUBPLAN_TYPE_MERGE);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pInfo->pSubplan->pChildren,
                                     (SNode*)splCreateScanSubplan(pCxt, pInfo->pSplitNode, SPLIT_FLAG_STABLE_SPLIT));
  }
  ++(pCxt->groupId);
  return code;
}

static SNode* stbSplFindPrimaryKeyFromScan(SScanLogicNode* pScan) {
  SNode* pCol = NULL;
  FOREACH(pCol, pScan->pScanCols) {
    if (PRIMARYKEY_TIMESTAMP_COL_ID == ((SColumnNode*)pCol)->colId) {
      return pCol;
    }
  }
  return NULL;
}

static int32_t stbSplSplitScanNodeForJoin(SSplitContext* pCxt, SLogicSubplan* pSubplan, SScanLogicNode* pScan) {
  SNodeList* pMergeKeys = NULL;
  int32_t    code = stbSplCreateMergeKeysByPrimaryKey(stbSplFindPrimaryKeyFromScan(pScan), &pMergeKeys);
  if (TSDB_CODE_SUCCESS == code) {
    code = stbSplCreateMergeNode(pCxt, pSubplan, (SLogicNode*)pScan, pMergeKeys, (SLogicNode*)pScan);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pSubplan->pChildren,
                                     (SNode*)splCreateScanSubplan(pCxt, (SLogicNode*)pScan, SPLIT_FLAG_STABLE_SPLIT));
  }
  pScan->scanType = SCAN_TYPE_TABLE_MERGE;
  ++(pCxt->groupId);
  return code;
}

static int32_t stbSplSplitJoinNodeImpl(SSplitContext* pCxt, SLogicSubplan* pSubplan, SJoinLogicNode* pJoin) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pChild = NULL;
  FOREACH(pChild, pJoin->node.pChildren) {
    if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pChild)) {
      code = stbSplSplitScanNodeForJoin(pCxt, pSubplan, (SScanLogicNode*)pChild);
    } else if (QUERY_NODE_LOGIC_PLAN_JOIN == nodeType(pChild)) {
      code = stbSplSplitJoinNodeImpl(pCxt, pSubplan, (SJoinLogicNode*)pChild);
    } else {
      code = TSDB_CODE_PLAN_INTERNAL_ERROR;
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }
  return code;
}

static int32_t stbSplSplitJoinNode(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  int32_t code = stbSplSplitJoinNodeImpl(pCxt, pInfo->pSubplan, (SJoinLogicNode*)pInfo->pSplitNode);
  if (TSDB_CODE_SUCCESS == code) {
    pInfo->pSubplan->subplanType = SUBPLAN_TYPE_MERGE;
    SPLIT_FLAG_SET_MASK(pInfo->pSubplan->splitFlag, SPLIT_FLAG_STABLE_SPLIT);
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
    case QUERY_NODE_LOGIC_PLAN_SCAN:
      code = stbSplSplitScanNode(pCxt, &info);
      break;
    case QUERY_NODE_LOGIC_PLAN_JOIN:
      code = stbSplSplitJoinNode(pCxt, &info);
      break;
    case QUERY_NODE_LOGIC_PLAN_AGG:
      code = stbSplSplitAggNode(pCxt, &info);
      break;
    case QUERY_NODE_LOGIC_PLAN_WINDOW:
      code = stbSplSplitWindowNode(pCxt, &info);
      break;
    case QUERY_NODE_LOGIC_PLAN_SORT:
      code = stbSplSplitSortNode(pCxt, &info);
      break;
    default:
      break;
  }

  pCxt->split = true;
  return code;
}

typedef struct SSigTbJoinSplitInfo {
  SJoinLogicNode* pJoin;
  SLogicNode*     pSplitNode;
  SLogicSubplan*  pSubplan;
} SSigTbJoinSplitInfo;

static bool sigTbJoinSplNeedSplit(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_JOIN != nodeType(pNode)) {
    return false;
  }

  SJoinLogicNode* pJoin = (SJoinLogicNode*)pNode;
  if (!pJoin->isSingleTableJoin) {
    return false;
  }
  return QUERY_NODE_LOGIC_PLAN_EXCHANGE != nodeType(nodesListGetNode(pJoin->node.pChildren, 0)) &&
         QUERY_NODE_LOGIC_PLAN_EXCHANGE != nodeType(nodesListGetNode(pJoin->node.pChildren, 1));
}

static bool sigTbJoinSplFindSplitNode(SSplitContext* pCxt, SLogicSubplan* pSubplan, SLogicNode* pNode,
                                      SSigTbJoinSplitInfo* pInfo) {
  if (sigTbJoinSplNeedSplit(pNode)) {
    pInfo->pJoin = (SJoinLogicNode*)pNode;
    pInfo->pSplitNode = (SLogicNode*)nodesListGetNode(pNode->pChildren, 1);
    pInfo->pSubplan = pSubplan;
    return true;
  }
  return false;
}

static int32_t singleTableJoinSplit(SSplitContext* pCxt, SLogicSubplan* pSubplan) {
  SSigTbJoinSplitInfo info = {0};
  if (!splMatch(pCxt, pSubplan, 0, (FSplFindSplitNode)sigTbJoinSplFindSplitNode, &info)) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t code = splCreateExchangeNodeForSubplan(pCxt, info.pSubplan, info.pSplitNode, info.pSubplan->subplanType);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&info.pSubplan->pChildren, (SNode*)splCreateScanSubplan(pCxt, info.pSplitNode, 0));
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
  SLogicSubplan* pSubplan = (SLogicSubplan*)nodesMakeNode(QUERY_NODE_LOGIC_SUBPLAN);
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
    code = nodesListMakeStrictAppend(&pUnionSubplan->pChildren, (SNode*)pNewSubplan);
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

static bool unAllSplFindSplitNode(SSplitContext* pCxt, SLogicSubplan* pSubplan, SLogicNode* pNode,
                                  SUnionAllSplitInfo* pInfo) {
  if (QUERY_NODE_LOGIC_PLAN_PROJECT == nodeType(pNode) && LIST_LENGTH(pNode->pChildren) > 1) {
    pInfo->pProject = (SProjectLogicNode*)pNode;
    pInfo->pSubplan = pSubplan;
    return true;
  }
  return false;
}

static int32_t unAllSplCreateExchangeNode(SSplitContext* pCxt, SLogicSubplan* pSubplan, SProjectLogicNode* pProject) {
  SExchangeLogicNode* pExchange = (SExchangeLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_EXCHANGE);
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
    nodesDestroyNode((SNode*)pProject);
    return TSDB_CODE_SUCCESS;
  }

  SNode* pNode;
  FOREACH(pNode, pProject->node.pParent->pChildren) {
    if (nodesEqualNode(pNode, (SNode*)pProject)) {
      REPLACE_NODE(pExchange);
      nodesDestroyNode(pNode);
      return TSDB_CODE_SUCCESS;
    }
  }
  nodesDestroyNode((SNode*)pExchange);
  return TSDB_CODE_PLAN_INTERNAL_ERROR;
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

static int32_t unDistSplCreateExchangeNode(SSplitContext* pCxt, SLogicSubplan* pSubplan, SAggLogicNode* pAgg) {
  SExchangeLogicNode* pExchange = (SExchangeLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_EXCHANGE);
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

  return nodesListMakeAppend(&pAgg->node.pChildren, (SNode*)pExchange);
}

static bool unDistSplFindSplitNode(SSplitContext* pCxt, SLogicSubplan* pSubplan, SLogicNode* pNode,
                                   SUnionDistinctSplitInfo* pInfo) {
  if (QUERY_NODE_LOGIC_PLAN_AGG == nodeType(pNode) && LIST_LENGTH(pNode->pChildren) > 1) {
    pInfo->pAgg = (SAggLogicNode*)pNode;
    pInfo->pSubplan = pSubplan;
    return true;
  }
  return false;
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

typedef struct SSmaIndexSplitInfo {
  SMergeLogicNode* pMerge;
  SLogicSubplan*   pSubplan;
} SSmaIndexSplitInfo;

static bool smaIdxSplFindSplitNode(SSplitContext* pCxt, SLogicSubplan* pSubplan, SLogicNode* pNode,
                                   SSmaIndexSplitInfo* pInfo) {
  if (QUERY_NODE_LOGIC_PLAN_MERGE == nodeType(pNode) && LIST_LENGTH(pNode->pChildren) > 1) {
    pInfo->pMerge = (SMergeLogicNode*)pNode;
    pInfo->pSubplan = pSubplan;
    return true;
  }
  return false;
}

static int32_t smaIndexSplit(SSplitContext* pCxt, SLogicSubplan* pSubplan) {
  SSmaIndexSplitInfo info = {0};
  if (!splMatch(pCxt, pSubplan, 0, (FSplFindSplitNode)smaIdxSplFindSplitNode, &info)) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = unionSplitSubplan(pCxt, info.pSubplan, (SLogicNode*)info.pMerge);
  if (TSDB_CODE_SUCCESS == code) {
    info.pMerge->srcGroupId = pCxt->groupId;
  }
  ++(pCxt->groupId);
  pCxt->split = true;
  return code;
}

typedef struct SQnodeSplitInfo {
  SLogicNode*    pSplitNode;
  SLogicSubplan* pSubplan;
} SQnodeSplitInfo;

static bool qndSplFindSplitNode(SSplitContext* pCxt, SLogicSubplan* pSubplan, SLogicNode* pNode,
                                SQnodeSplitInfo* pInfo) {
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode) && NULL != pNode->pParent) {
    pInfo->pSplitNode = pNode;
    pInfo->pSubplan = pSubplan;
    return true;
  }
  return false;
}

static int32_t qnodeSplit(SSplitContext* pCxt, SLogicSubplan* pSubplan) {
  if (QUERY_POLICY_QNODE != tsQueryPolicy) {
    return TSDB_CODE_SUCCESS;
  }

  SQnodeSplitInfo info = {0};
  if (!splMatch(pCxt, pSubplan, 0, (FSplFindSplitNode)qndSplFindSplitNode, &info)) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t code = splCreateExchangeNodeForSubplan(pCxt, info.pSubplan, info.pSplitNode, info.pSubplan->subplanType);
  if (TSDB_CODE_SUCCESS == code) {
    SLogicSubplan* pScanSubplan = splCreateScanSubplan(pCxt, info.pSplitNode, 0);
    if (NULL != pScanSubplan) {
      if (NULL != info.pSubplan->pVgroupList) {
        info.pSubplan->numOfComputeNodes = info.pSubplan->pVgroupList->numOfVgroups;
        TSWAP(pScanSubplan->pVgroupList, info.pSubplan->pVgroupList);
      } else {
        info.pSubplan->numOfComputeNodes = 1;
      }
      code = nodesListMakeStrictAppend(&info.pSubplan->pChildren, (SNode*)pScanSubplan);
    } else {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  info.pSubplan->subplanType = SUBPLAN_TYPE_COMPUTE;
  ++(pCxt->groupId);
  pCxt->split = true;
  return code;
}

// clang-format off
static const SSplitRule splitRuleSet[] = {
  {.pName = "SuperTableSplit",      .splitFunc = stableSplit},
  {.pName = "SingleTableJoinSplit", .splitFunc = singleTableJoinSplit},
  {.pName = "UnionAllSplit",        .splitFunc = unionAllSplit},
  {.pName = "UnionDistinctSplit",   .splitFunc = unionDistinctSplit},
  {.pName = "SmaIndexSplit",        .splitFunc = smaIndexSplit}
};
// clang-format on

static const int32_t splitRuleNum = (sizeof(splitRuleSet) / sizeof(SSplitRule));

static void dumpLogicSubplan(const char* pRuleName, SLogicSubplan* pSubplan) {
  char* pStr = NULL;
  nodesNodeToString((SNode*)pSubplan, false, &pStr, NULL);
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
  return qnodeSplit(&cxt, pSubplan);
}

static void setVgroupsInfo(SLogicNode* pNode, SLogicSubplan* pSubplan) {
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    TSWAP(((SScanLogicNode*)pNode)->pVgroupList, pSubplan->pVgroupList);
    return;
  }

  SNode* pChild;
  FOREACH(pChild, pNode->pChildren) { setVgroupsInfo((SLogicNode*)pChild, pSubplan); }
}

int32_t splitLogicPlan(SPlanContext* pCxt, SLogicSubplan* pLogicSubplan) {
  if (QUERY_NODE_LOGIC_PLAN_VNODE_MODIFY == nodeType(pLogicSubplan->pNode)) {
    setVgroupsInfo(pLogicSubplan->pNode, pLogicSubplan);
    return TSDB_CODE_SUCCESS;
  }
  return applySplitRule(pCxt, pLogicSubplan);
}
