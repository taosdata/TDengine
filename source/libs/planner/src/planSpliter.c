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
#define SPLIT_FLAG_INSERT_SPLIT SPLIT_FLAG_MASK(1)

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

typedef bool (*FSplFindSplitNode)(SSplitContext* pCxt, SLogicSubplan* pSubplan, SLogicNode* pNode, void* pInfo);

static int32_t stbSplCreateMergeKeys(SNodeList* pSortKeys, SNodeList* pTargets, SNodeList** pOutput);

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

static bool splHasScan(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    return true;
  }

  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pChild)) {
      return true;
    }
    return splHasScan((SLogicNode*)pChild);
  }

  return false;
}

static void splSetSubplanType(SLogicSubplan* pSubplan) {
  pSubplan->subplanType = splHasScan(pSubplan->pNode) ? SUBPLAN_TYPE_SCAN : SUBPLAN_TYPE_MERGE;
}

static SLogicSubplan* splCreateSubplan(SSplitContext* pCxt, SLogicNode* pNode) {
  SLogicSubplan* pSubplan = (SLogicSubplan*)nodesMakeNode(QUERY_NODE_LOGIC_SUBPLAN);
  if (NULL == pSubplan) {
    return NULL;
  }
  pSubplan->id.queryId = pCxt->queryId;
  pSubplan->id.groupId = pCxt->groupId;
  pSubplan->pNode = pNode;
  pNode->pParent = NULL;
  splSetSubplanType(pSubplan);
  return pSubplan;
}

static int32_t splCreateExchangeNode(SSplitContext* pCxt, SLogicNode* pChild, SExchangeLogicNode** pOutput) {
  SExchangeLogicNode* pExchange = (SExchangeLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_EXCHANGE);
  if (NULL == pExchange) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pExchange->srcStartGroupId = pCxt->groupId;
  pExchange->srcEndGroupId = pCxt->groupId;
  pExchange->node.precision = pChild->precision;
  pExchange->node.dynamicOp = pChild->dynamicOp;
  pExchange->node.pTargets = nodesCloneList(pChild->pTargets);
  if (NULL == pExchange->node.pTargets) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (NULL != pChild->pLimit) {
    pExchange->node.pLimit = nodesCloneNode(pChild->pLimit);
    if (NULL == pExchange->node.pLimit) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    ((SLimitNode*)pChild->pLimit)->limit += ((SLimitNode*)pChild->pLimit)->offset;
    ((SLimitNode*)pChild->pLimit)->offset = 0;
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

static bool splIsChildSubplan(SLogicNode* pLogicNode, int32_t groupId) {
  if (QUERY_NODE_LOGIC_PLAN_EXCHANGE == nodeType(pLogicNode)) {
    return groupId >= ((SExchangeLogicNode*)pLogicNode)->srcStartGroupId &&
           groupId <= ((SExchangeLogicNode*)pLogicNode)->srcEndGroupId;
  }

  if (QUERY_NODE_LOGIC_PLAN_MERGE == nodeType(pLogicNode)) {
    return ((SMergeLogicNode*)pLogicNode)->srcGroupId == groupId;
  }

  SNode* pChild;
  FOREACH(pChild, pLogicNode->pChildren) {
    bool isChild = splIsChildSubplan((SLogicNode*)pChild, groupId);
    if (isChild) {
      return isChild;
    }
  }
  return false;
}

static int32_t splMountSubplan(SLogicSubplan* pParent, SNodeList* pChildren) {
  SNode* pChild = NULL;
  WHERE_EACH(pChild, pChildren) {
    if (splIsChildSubplan(pParent->pNode, ((SLogicSubplan*)pChild)->id.groupId)) {
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
  return (NULL != pScan->pVgroupList && pScan->pVgroupList->numOfVgroups > 1);
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
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pChild) && stbSplIsMultiTbScan(streamQuery, (SScanLogicNode*)pChild)) {
    return true;
  }
  return false;
}

static bool stbSplIsMultiTbScanChild(bool streamQuery, SLogicNode* pNode) {
  if (1 != LIST_LENGTH(pNode->pChildren)) {
    return false;
  }
  SNode* pChild = nodesListGetNode(pNode->pChildren, 0);
  return (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pChild) && stbSplIsMultiTbScan(streamQuery, (SScanLogicNode*)pChild));
}

static bool stbSplNeedSplitWindow(bool streamQuery, SLogicNode* pNode) {
  SWindowLogicNode* pWindow = (SWindowLogicNode*)pNode;
  if (WINDOW_TYPE_INTERVAL == pWindow->winType) {
    return !stbSplHasGatherExecFunc(pWindow->pFuncs) && stbSplHasMultiTbScan(streamQuery, pNode);
  }

  if (WINDOW_TYPE_SESSION == pWindow->winType) {
    if (!streamQuery) {
      return stbSplHasMultiTbScan(streamQuery, pNode);
    } else {
      return !stbSplHasGatherExecFunc(pWindow->pFuncs) && stbSplHasMultiTbScan(streamQuery, pNode);
    }
  }

  if (WINDOW_TYPE_STATE == pWindow->winType) {
    if (!streamQuery) {
      return stbSplHasMultiTbScan(streamQuery, pNode);
    } else {
      return false;
    }
  }

  return false;
}

static bool stbSplNeedSplitJoin(bool streamQuery, SJoinLogicNode* pJoin) {
  if (pJoin->isSingleTableJoin || JOIN_ALGO_HASH == pJoin->joinAlgo) {
    return false;
  }
  SNode* pChild = NULL;
  FOREACH(pChild, pJoin->node.pChildren) {
    if (QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pChild) && QUERY_NODE_LOGIC_PLAN_JOIN != nodeType(pChild)) {
      return false;
    }
  }
  return true;
}

static bool stbSplIsTableCountQuery(SLogicNode* pNode) {
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
  return QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pChild) && SCAN_TYPE_TABLE_COUNT == ((SScanLogicNode*)pChild)->scanType;
}

static bool stbSplNeedSplit(bool streamQuery, SLogicNode* pNode) {
  switch (nodeType(pNode)) {
    case QUERY_NODE_LOGIC_PLAN_SCAN:
      return streamQuery ? false : stbSplIsMultiTbScan(streamQuery, (SScanLogicNode*)pNode);
    case QUERY_NODE_LOGIC_PLAN_JOIN:
      return stbSplNeedSplitJoin(streamQuery, (SJoinLogicNode*)pNode);
    case QUERY_NODE_LOGIC_PLAN_PARTITION:
      return streamQuery ? false : stbSplIsMultiTbScanChild(streamQuery, pNode);
    case QUERY_NODE_LOGIC_PLAN_AGG:
      return (!stbSplHasGatherExecFunc(((SAggLogicNode*)pNode)->pAggFuncs) ||
              isPartTableAgg((SAggLogicNode*)pNode)) &&
             stbSplHasMultiTbScan(streamQuery, pNode) && !stbSplIsTableCountQuery(pNode);
    case QUERY_NODE_LOGIC_PLAN_WINDOW:
      return stbSplNeedSplitWindow(streamQuery, pNode);
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

static int32_t stbSplRewriteFuns(const SNodeList* pFuncs, SNodeList** pPartialFuncs, SNodeList** pMidFuncs, SNodeList** pMergeFuncs) {
  SNode* pNode = NULL;
  FOREACH(pNode, pFuncs) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    SFunctionNode* pPartFunc = NULL;
    SFunctionNode* pMidFunc = NULL;
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
      if(pMidFuncs != NULL){
        pMidFunc = (SFunctionNode*)nodesCloneNode(pNode);
        if (NULL == pMidFunc) {
          nodesDestroyNode((SNode*)pMidFunc);
          code = TSDB_CODE_OUT_OF_MEMORY;
        }
      }
    } else {
      code = fmGetDistMethod(pFunc, &pPartFunc, &pMidFunc, &pMergeFunc);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListMakeStrictAppend(pPartialFuncs, (SNode*)pPartFunc);
    }
    if (TSDB_CODE_SUCCESS == code) {
      if(pMidFuncs != NULL){
        code = nodesListMakeStrictAppend(pMidFuncs, (SNode*)pMidFunc);
      }else{
        nodesDestroyNode((SNode*)pMidFunc);
      }
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListMakeStrictAppend(pMergeFuncs, (SNode*)pMergeFunc);
    }
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode((SNode*)pPartFunc);
      nodesDestroyNode((SNode*)pMidFunc);
      nodesDestroyNode((SNode*)pMergeFunc);
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t stbSplAppendWStart(SNodeList* pFuncs, int32_t* pIndex, uint8_t precision) {
  int32_t index = 0;
  SNode*  pFunc = NULL;
  FOREACH(pFunc, pFuncs) {
    if (FUNCTION_TYPE_WSTART == ((SFunctionNode*)pFunc)->funcType) {
      *pIndex = index;
      return TSDB_CODE_SUCCESS;
    }
    ++index;
  }

  SFunctionNode* pWStart = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  if (NULL == pWStart) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  strcpy(pWStart->functionName, "_wstart");
  int64_t pointer = (int64_t)pWStart;
  char name[TSDB_COL_NAME_LEN + TSDB_POINTER_PRINT_BYTES + TSDB_NAME_DELIMITER_LEN + 1] = {0};
  int32_t len = snprintf(name, sizeof(name) - 1, "%s.%" PRId64 "", pWStart->functionName, pointer);
  taosCreateMD5Hash(name, len);
  strncpy(pWStart->node.aliasName, name, TSDB_COL_NAME_LEN - 1);
  pWStart->node.resType.precision = precision;

  int32_t code = fmGetFuncInfo(pWStart, NULL, 0);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListStrictAppend(pFuncs, (SNode*)pWStart);
  }
  *pIndex = index;
  return code;
}

static int32_t stbSplAppendWEnd(SWindowLogicNode* pWin, int32_t* pIndex) {
  int32_t index = 0;
  SNode*  pFunc = NULL;
  FOREACH(pFunc, pWin->pFuncs) {
    if (FUNCTION_TYPE_WEND == ((SFunctionNode*)pFunc)->funcType) {
      *pIndex = index;
      return TSDB_CODE_SUCCESS;
    }
    ++index;
  }

  SFunctionNode* pWEnd = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  if (NULL == pWEnd) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  strcpy(pWEnd->functionName, "_wend");
  int64_t pointer = (int64_t)pWEnd;
  char name[TSDB_COL_NAME_LEN + TSDB_POINTER_PRINT_BYTES + TSDB_NAME_DELIMITER_LEN + 1] = {0};
  int32_t len = snprintf(name, sizeof(name) - 1, "%s.%" PRId64 "", pWEnd->functionName, pointer);
  taosCreateMD5Hash(name, len);
  strncpy(pWEnd->node.aliasName, name, TSDB_COL_NAME_LEN - 1);

  int32_t code = fmGetFuncInfo(pWEnd, NULL, 0);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListStrictAppend(pWin->pFuncs, (SNode*)pWEnd);
  }
  *pIndex = index;
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExpr(nodesListGetNode(pWin->pFuncs, index), &pWin->node.pTargets);
  }
  return code;
}

static int32_t stbSplCreatePartWindowNode(SWindowLogicNode* pMergeWindow, SLogicNode** pPartWindow) {
  SNodeList* pFunc = pMergeWindow->pFuncs;
  pMergeWindow->pFuncs = NULL;
  SNodeList* pTargets = pMergeWindow->node.pTargets;
  pMergeWindow->node.pTargets = NULL;
  SNodeList* pChildren = pMergeWindow->node.pChildren;
  pMergeWindow->node.pChildren = NULL;
  SNode* pConditions = pMergeWindow->node.pConditions;
  pMergeWindow->node.pConditions = NULL;

  SWindowLogicNode* pPartWin = (SWindowLogicNode*)nodesCloneNode((SNode*)pMergeWindow);
  if (NULL == pPartWin) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pPartWin->node.groupAction = GROUP_ACTION_KEEP;
  pMergeWindow->node.pTargets = pTargets;
  pMergeWindow->node.pConditions = pConditions;
  pPartWin->node.pChildren = pChildren;
  splSetParent((SLogicNode*)pPartWin);

  int32_t index = 0;
  int32_t code = stbSplRewriteFuns(pFunc, &pPartWin->pFuncs, NULL, &pMergeWindow->pFuncs);
  if (TSDB_CODE_SUCCESS == code) {
    code = stbSplAppendWStart(pPartWin->pFuncs, &index, ((SColumnNode*)pMergeWindow->pTspk)->node.resType.precision);
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

static int32_t stbSplCreatePartMidWindowNode(SWindowLogicNode* pMergeWindow, SLogicNode** pPartWindow, SLogicNode** pMidWindow) {
  SNodeList* pFunc = pMergeWindow->pFuncs;
  pMergeWindow->pFuncs = NULL;
  SNodeList* pTargets = pMergeWindow->node.pTargets;
  pMergeWindow->node.pTargets = NULL;
  SNodeList* pChildren = pMergeWindow->node.pChildren;
  pMergeWindow->node.pChildren = NULL;
  SNode* pConditions = pMergeWindow->node.pConditions;
  pMergeWindow->node.pConditions = NULL;

  SWindowLogicNode* pPartWin = (SWindowLogicNode*)nodesCloneNode((SNode*)pMergeWindow);
  if (NULL == pPartWin) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SWindowLogicNode* pMidWin = (SWindowLogicNode*)nodesCloneNode((SNode*)pMergeWindow);
  if (NULL == pMidWin) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pPartWin->node.groupAction = GROUP_ACTION_KEEP;
  pMidWin->node.groupAction = GROUP_ACTION_KEEP;
  pMergeWindow->node.pTargets = pTargets;
  pMergeWindow->node.pConditions = pConditions;

  pPartWin->node.pChildren = pChildren;
  splSetParent((SLogicNode*)pPartWin);

  SNodeList* pFuncPart = NULL;
  SNodeList* pFuncMid = NULL;
  SNodeList* pFuncMerge = NULL;
  int32_t code = stbSplRewriteFuns(pFunc, &pFuncPart, &pFuncMid, &pFuncMerge);
  pPartWin->pFuncs = pFuncPart;
  pMidWin->pFuncs = pFuncMid;
  pMergeWindow->pFuncs = pFuncMerge;

  int32_t index = 0;
  if (TSDB_CODE_SUCCESS == code) {
    code = stbSplAppendWStart(pPartWin->pFuncs, &index, ((SColumnNode*)pMergeWindow->pTspk)->node.resType.precision);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExprs(pPartWin->pFuncs, &pPartWin->node.pTargets);
  }

  if (TSDB_CODE_SUCCESS == code) {
    nodesDestroyNode(pMidWin->pTspk);
    pMidWin->pTspk = nodesCloneNode(nodesListGetNode(pPartWin->node.pTargets, index));
    if (NULL == pMidWin->pTspk) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = stbSplAppendWStart(pMidWin->pFuncs, &index, ((SColumnNode*)pMergeWindow->pTspk)->node.resType.precision);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExprs(pMidWin->pFuncs, &pMidWin->node.pTargets);
  }

  if (TSDB_CODE_SUCCESS == code) {
    nodesDestroyNode(pMergeWindow->pTspk);
    pMergeWindow->pTspk = nodesCloneNode(nodesListGetNode(pMidWin->node.pTargets, index));
    if (NULL == pMergeWindow->pTspk) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  nodesDestroyList(pFunc);
  if (TSDB_CODE_SUCCESS == code) {
    *pPartWindow = (SLogicNode*)pPartWin;
    *pMidWindow = (SLogicNode*)pMidWin;
  } else {
    nodesDestroyNode((SNode*)pPartWin);
    nodesDestroyNode((SNode*)pMidWin);
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

static int32_t stbSplRewriteFromMergeNode(SMergeLogicNode* pMerge, SLogicNode* pNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  pMerge->node.inputTsOrder = pNode->outputTsOrder;
  pMerge->node.outputTsOrder = pNode->outputTsOrder;

  switch (nodeType(pNode)) {
    case QUERY_NODE_LOGIC_PLAN_PROJECT: {
      SProjectLogicNode *pLogicNode = (SProjectLogicNode*)pNode;
      if (pLogicNode->ignoreGroupId && (pMerge->node.pLimit || pMerge->node.pSlimit)) {
        pMerge->ignoreGroupId = true;
        pLogicNode->ignoreGroupId = false;
      }
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_WINDOW: {
      SWindowLogicNode* pWindow = (SWindowLogicNode*)pNode;
      if (pMerge->node.pLimit) {
        nodesDestroyNode(pMerge->node.pLimit);
        pMerge->node.pLimit = NULL;
      }
      if (pMerge->node.pSlimit) {
        nodesDestroyNode(pMerge->node.pSlimit);
        pMerge->node.pSlimit = NULL;
      }
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_SORT: {
      SSortLogicNode* pSort = (SSortLogicNode*)pNode;
      if (pSort->calcGroupId) pMerge->inputWithGroupId = true;
      break;
    }
    default:
      break;
  }

  return code;
}

static int32_t stbSplCreateMergeNode(SSplitContext* pCxt, SLogicSubplan* pSubplan, SLogicNode* pSplitNode,
                                     SNodeList* pMergeKeys, SLogicNode* pPartChild, bool groupSort, bool needSort) {
  SMergeLogicNode* pMerge = (SMergeLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_MERGE);
  if (NULL == pMerge) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pMerge->needSort = needSort;
  pMerge->numOfChannels = stbSplGetNumOfVgroups(pPartChild);
  pMerge->srcGroupId = pCxt->groupId;
  pMerge->node.precision = pPartChild->precision;
  pMerge->pMergeKeys = pMergeKeys;
  pMerge->groupSort = groupSort;

  int32_t code = TSDB_CODE_SUCCESS;
  pMerge->pInputs = nodesCloneList(pPartChild->pTargets);
  // NULL != pSubplan means 'merge node' replaces 'split node'.
  if (NULL == pSubplan) {
    pMerge->node.pTargets = nodesCloneList(pPartChild->pTargets);
  } else {
    pMerge->node.pTargets = nodesCloneList(pSplitNode->pTargets);
  }
  if (NULL == pMerge->node.pTargets || NULL == pMerge->pInputs) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }
  if (TSDB_CODE_SUCCESS == code && NULL != pSplitNode->pLimit) {
    pMerge->node.pLimit = nodesCloneNode(pSplitNode->pLimit);
    if (NULL == pMerge->node.pLimit) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
    ((SLimitNode*)pSplitNode->pLimit)->limit += ((SLimitNode*)pSplitNode->pLimit)->offset;
    ((SLimitNode*)pSplitNode->pLimit)->offset = 0;
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = stbSplRewriteFromMergeNode(pMerge, pSplitNode);
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
    pExchange->node.pParent = pParent;
    code = nodesListMakeAppend(&pParent->pChildren, (SNode*)pExchange);
  }
  return code;
}

static int32_t stbSplCreateMergeKeysByPrimaryKey(SNode* pPrimaryKey, EOrder order, SNodeList** pMergeKeys) {
  SOrderByExprNode* pMergeKey = (SOrderByExprNode*)nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR);
  if (NULL == pMergeKey) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pMergeKey->pExpr = nodesCloneNode(pPrimaryKey);
  if (NULL == pMergeKey->pExpr) {
    nodesDestroyNode((SNode*)pMergeKey);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pMergeKey->order = order;
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
    code = stbSplCreateMergeKeysByPrimaryKey(((SWindowLogicNode*)pInfo->pSplitNode)->pTspk,
                                             ((SWindowLogicNode*)pInfo->pSplitNode)->node.outputTsOrder, &pMergeKeys);
    if (TSDB_CODE_SUCCESS == code) {
      code = stbSplCreateMergeNode(pCxt, NULL, pInfo->pSplitNode, pMergeKeys, pPartWindow, true, true);
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

static int32_t stbSplSplitIntervalForStreamMultiAgg(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  SLogicNode* pPartWindow = NULL;
  SLogicNode* pMidWindow  = NULL;
  int32_t     code = stbSplCreatePartMidWindowNode((SWindowLogicNode*)pInfo->pSplitNode, &pPartWindow, &pMidWindow);
  if (TSDB_CODE_SUCCESS == code) {
    ((SWindowLogicNode*)pMidWindow)->windowAlgo = INTERVAL_ALGO_STREAM_MID;
    ((SWindowLogicNode*)pInfo->pSplitNode)->windowAlgo = INTERVAL_ALGO_STREAM_FINAL;
    ((SWindowLogicNode*)pPartWindow)->windowAlgo = INTERVAL_ALGO_STREAM_SEMI;
    code = stbSplCreateExchangeNode(pCxt, pInfo->pSplitNode, pMidWindow);
    if (TSDB_CODE_SUCCESS == code) {
      code = stbSplCreateExchangeNode(pCxt, pMidWindow, pPartWindow);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    SNode* subPlan = (SNode*)splCreateSubplan(pCxt, pMidWindow);
    ((SLogicSubplan*)subPlan)->subplanType = SUBPLAN_TYPE_MERGE;

    code = nodesListMakeStrictAppend(&((SLogicSubplan*)subPlan)->pChildren,
                                     (SNode*)splCreateScanSubplan(pCxt, pPartWindow, SPLIT_FLAG_STABLE_SPLIT));
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListMakeStrictAppend(&pInfo->pSubplan->pChildren, subPlan);
    }
  }

  pInfo->pSubplan->subplanType = SUBPLAN_TYPE_MERGE;
  ++(pCxt->groupId);
  return code;
}

static int32_t stbSplSplitInterval(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  if (pCxt->pPlanCxt->streamQuery) {
    return stbSplSplitIntervalForStreamMultiAgg(pCxt, pInfo);
  } else {
    return stbSplSplitIntervalForBatch(pCxt, pInfo);
  }
}

static int32_t stbSplSplitSessionForStream(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  SLogicNode* pPartWindow = NULL;
  int32_t     code = stbSplCreatePartWindowNode((SWindowLogicNode*)pInfo->pSplitNode, &pPartWindow);
  if (TSDB_CODE_SUCCESS == code) {
    SWindowLogicNode* pPartWin = (SWindowLogicNode*)pPartWindow;
    SWindowLogicNode* pMergeWin = (SWindowLogicNode*)pInfo->pSplitNode;
    pPartWin->windowAlgo = SESSION_ALGO_STREAM_SEMI;
    pMergeWin->windowAlgo = SESSION_ALGO_STREAM_FINAL;
    int32_t index = 0;
    int32_t code = stbSplAppendWEnd(pPartWin, &index);
    if (TSDB_CODE_SUCCESS == code) {
      nodesDestroyNode(pMergeWin->pTsEnd);
      pMergeWin->pTsEnd = nodesCloneNode(nodesListGetNode(pPartWin->node.pTargets, index));
      if (NULL == pMergeWin->pTsEnd) {
        code = TSDB_CODE_OUT_OF_MEMORY;
      }
    }
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

static void stbSplSetTableMergeScan(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    SScanLogicNode* pScan = (SScanLogicNode*)pNode;
    pScan->scanType = SCAN_TYPE_TABLE_MERGE;
    pScan->filesetDelimited = true;
    if (NULL != pScan->pGroupTags) {
      pScan->groupSort = true;
    }
  } else {
    if (1 == LIST_LENGTH(pNode->pChildren)) {
      stbSplSetTableMergeScan((SLogicNode*)nodesListGetNode(pNode->pChildren, 0));
    }
  }
}

static int32_t stbSplSplitSessionOrStateForBatch(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  SLogicNode* pWindow = pInfo->pSplitNode;
  SLogicNode* pChild = (SLogicNode*)nodesListGetNode(pWindow->pChildren, 0);

  SNodeList* pMergeKeys = NULL;
  int32_t    code = stbSplCreateMergeKeysByPrimaryKey(((SWindowLogicNode*)pWindow)->pTspk,
                                                      ((SWindowLogicNode*)pWindow)->node.inputTsOrder, &pMergeKeys);

  if (TSDB_CODE_SUCCESS == code) {
    code = stbSplCreateMergeNode(pCxt, pInfo->pSubplan, pChild, pMergeKeys, (SLogicNode*)pChild, true, true);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pInfo->pSubplan->pChildren,
                                     (SNode*)splCreateScanSubplan(pCxt, pChild, SPLIT_FLAG_STABLE_SPLIT));
  }

  if (TSDB_CODE_SUCCESS == code) {
    stbSplSetTableMergeScan(pChild);
    pInfo->pSubplan->subplanType = SUBPLAN_TYPE_MERGE;
    SPLIT_FLAG_SET_MASK(pInfo->pSubplan->splitFlag, SPLIT_FLAG_STABLE_SPLIT);
    ++(pCxt->groupId);
  } else {
    nodesDestroyList(pMergeKeys);
  }

  return code;
}

static int32_t stbSplSplitSession(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  if (pCxt->pPlanCxt->streamQuery) {
    return stbSplSplitSessionForStream(pCxt, pInfo);
  } else {
    return stbSplSplitSessionOrStateForBatch(pCxt, pInfo);
  }
}

static int32_t stbSplSplitStateForStream(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  return TSDB_CODE_PLAN_INTERNAL_ERROR;
}

static int32_t stbSplSplitState(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  if (pCxt->pPlanCxt->streamQuery) {
    return stbSplSplitStateForStream(pCxt, pInfo);
  } else {
    return stbSplSplitSessionOrStateForBatch(pCxt, pInfo);
  }
}

static int32_t stbSplSplitEventForStream(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  return TSDB_CODE_PLAN_INTERNAL_ERROR;
}

static int32_t stbSplSplitEvent(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  if (pCxt->pPlanCxt->streamQuery) {
    return stbSplSplitEventForStream(pCxt, pInfo);
  } else {
    return stbSplSplitSessionOrStateForBatch(pCxt, pInfo);
  }
}

static int32_t stbSplSplitCountForStream(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  return TSDB_CODE_PLAN_INTERNAL_ERROR;
}

static int32_t stbSplSplitCount(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  if (pCxt->pPlanCxt->streamQuery) {
    return stbSplSplitCountForStream(pCxt, pInfo);
  } else {
    return stbSplSplitSessionOrStateForBatch(pCxt, pInfo);
  }
}

static int32_t stbSplSplitWindowForCrossTable(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  switch (((SWindowLogicNode*)pInfo->pSplitNode)->winType) {
    case WINDOW_TYPE_INTERVAL:
      return stbSplSplitInterval(pCxt, pInfo);
    case WINDOW_TYPE_SESSION:
      return stbSplSplitSession(pCxt, pInfo);
    case WINDOW_TYPE_STATE:
      return stbSplSplitState(pCxt, pInfo);
    case WINDOW_TYPE_EVENT:
      return stbSplSplitEvent(pCxt, pInfo);
    case WINDOW_TYPE_COUNT:
      return stbSplSplitCount(pCxt, pInfo);
    default:
      break;
  }
  return TSDB_CODE_PLAN_INTERNAL_ERROR;
}

static bool stbSplNeedSeqRecvData(SLogicNode* pNode) {
  if (NULL == pNode) {
    return false;
  }

  if (NULL != pNode->pLimit || NULL != pNode->pSlimit) {
    return true;
  }
  return stbSplNeedSeqRecvData(pNode->pParent);
}

static int32_t stbSplSplitWindowForPartTable(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  if (pCxt->pPlanCxt->streamQuery) {
    SPLIT_FLAG_SET_MASK(pInfo->pSubplan->splitFlag, SPLIT_FLAG_STABLE_SPLIT);
    return TSDB_CODE_SUCCESS;
  }

  if (NULL != pInfo->pSplitNode->pParent && QUERY_NODE_LOGIC_PLAN_FILL == nodeType(pInfo->pSplitNode->pParent)) {
    pInfo->pSplitNode = pInfo->pSplitNode->pParent;
  }
  SExchangeLogicNode* pExchange = NULL;
  int32_t             code = splCreateExchangeNode(pCxt, pInfo->pSplitNode, &pExchange);
  if (TSDB_CODE_SUCCESS == code) {
    code = replaceLogicNode(pInfo->pSubplan, pInfo->pSplitNode, (SLogicNode*)pExchange);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pExchange->seqRecvData = stbSplNeedSeqRecvData((SLogicNode*)pExchange);
    code = nodesListMakeStrictAppend(&pInfo->pSubplan->pChildren,
                                     (SNode*)splCreateScanSubplan(pCxt, pInfo->pSplitNode, SPLIT_FLAG_STABLE_SPLIT));
  }
  pInfo->pSubplan->subplanType = SUBPLAN_TYPE_MERGE;
  ++(pCxt->groupId);
  return code;
}

static int32_t stbSplSplitWindowNode(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  if (isPartTableWinodw((SWindowLogicNode*)pInfo->pSplitNode)) {
    return stbSplSplitWindowForPartTable(pCxt, pInfo);
  } else {
    return stbSplSplitWindowForCrossTable(pCxt, pInfo);
  }
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

  SAggLogicNode* pPartAgg = (SAggLogicNode*)nodesCloneNode((SNode*)pMergeAgg);
  if (NULL == pPartAgg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pPartAgg->node.groupAction = GROUP_ACTION_KEEP;

  int32_t code = TSDB_CODE_SUCCESS;

  if (NULL != pGroupKeys) {
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

    code = stbSplRewriteFuns(pFunc, &pPartAgg->pAggFuncs, NULL, &pMergeAgg->pAggFuncs);
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

static int32_t stbSplSplitAggNodeForPartTable(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  int32_t code = splCreateExchangeNodeForSubplan(pCxt, pInfo->pSubplan, pInfo->pSplitNode, SUBPLAN_TYPE_MERGE);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pInfo->pSubplan->pChildren,
                                     (SNode*)splCreateScanSubplan(pCxt, pInfo->pSplitNode, SPLIT_FLAG_STABLE_SPLIT));
  }
  ++(pCxt->groupId);
  return code;
}

static SFunctionNode* createGroupKeyAggFunc(SColumnNode* pGroupCol) {
  SFunctionNode* pFunc = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  if (pFunc) {
    strcpy(pFunc->functionName, "_group_key");
    strcpy(pFunc->node.aliasName, pGroupCol->node.aliasName);
    strcpy(pFunc->node.userAlias, pGroupCol->node.userAlias);
    int32_t code = nodesListMakeStrictAppend(&pFunc->pParameterList, nodesCloneNode((SNode*)pGroupCol));
    if (code == TSDB_CODE_SUCCESS) {
      code = fmGetFuncInfo(pFunc, NULL, 0);
    }
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode((SNode*)pFunc);
      pFunc = NULL;
    }
    char    name[TSDB_FUNC_NAME_LEN + TSDB_NAME_DELIMITER_LEN + TSDB_POINTER_PRINT_BYTES + 1] = {0};
    int32_t len = snprintf(name, sizeof(name) - 1, "%s.%p", pFunc->functionName, pFunc);
    taosCreateMD5Hash(name, len);
    strncpy(pFunc->node.aliasName, name, TSDB_COL_NAME_LEN - 1);
  }
  return pFunc;
}

/**
 * @brief For pipelined agg node, add a SortMergeNode to merge result from vnodes.
 *        For agg + partition, results are sorted by group id, use group sort.
 *        For agg + sort for group, results are sorted by partition keys, not group id, merges keys should be the same
 *            as partition keys
 */
static int32_t stbSplAggNodeCreateMerge(SSplitContext* pCtx, SStableSplitInfo* pInfo, SLogicNode* pChildAgg) {
  bool       groupSort = true;
  SNodeList* pMergeKeys = NULL;
  int32_t    code = TSDB_CODE_SUCCESS;
  bool       sortForGroup = false;

  if (pChildAgg->pChildren->length != 1) return TSDB_CODE_TSC_INTERNAL_ERROR;

  SLogicNode* pChild = (SLogicNode*)nodesListGetNode(pChildAgg->pChildren, 0);
  if (nodeType(pChild) == QUERY_NODE_LOGIC_PLAN_SORT) {
    SSortLogicNode* pSort = (SSortLogicNode*)pChild;
    if (pSort->calcGroupId) {
      SNode *node, *node2;
      groupSort = false;
      sortForGroup = true;
      SNodeList* extraAggFuncs = NULL;
      uint32_t   originalLen = LIST_LENGTH(pSort->node.pTargets), idx = 0;
      code = stbSplCreateMergeKeys(pSort->pSortKeys, pSort->node.pTargets, &pMergeKeys);
      if (TSDB_CODE_SUCCESS != code) return code;

      // Create group_key func for all sort keys.
      // We only need newly added nodes in pSort.node.pTargets when stbSplCreateMergeKeys
      FOREACH(node, pSort->node.pTargets) {
        if (idx++ < originalLen) continue;
        SFunctionNode* pGroupKeyFunc = createGroupKeyAggFunc((SColumnNode*)node);
        if (!pGroupKeyFunc) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          break;
        }
        code = nodesListMakeStrictAppend(&extraAggFuncs, (SNode*)pGroupKeyFunc);
        if (code != TSDB_CODE_SUCCESS) {
          nodesDestroyNode((SNode*)pGroupKeyFunc);
        }
      }

      if (TSDB_CODE_SUCCESS == code) {
        // add these extra group_key funcs into targets
        code = createColumnByRewriteExprs(extraAggFuncs, &pChildAgg->pTargets);
      }
      if (code == TSDB_CODE_SUCCESS) {
        nodesListAppendList(((SAggLogicNode*)pChildAgg)->pAggFuncs, extraAggFuncs);
        extraAggFuncs = NULL;
      }

      if (code == TSDB_CODE_SUCCESS) {
        FOREACH(node, pMergeKeys) {
          SOrderByExprNode* pOrder = (SOrderByExprNode*)node;
          SColumnNode*      pCol = (SColumnNode*)pOrder->pExpr;
          FOREACH(node2, ((SAggLogicNode*)pChildAgg)->pAggFuncs) {
            SFunctionNode* pFunc = (SFunctionNode*)node2;
            if (0 != strcmp(pFunc->functionName, "_group_key")) continue;
            SNode* pParam = nodesListGetNode(pFunc->pParameterList, 0);
            if (!nodesEqualNode(pParam, (SNode*)pCol)) continue;

            // use the colName of group_key func to make sure finding the right slot id for merge keys.
            strcpy(pCol->colName, pFunc->node.aliasName);
            strcpy(pCol->node.aliasName, pFunc->node.aliasName);
            memset(pCol->tableAlias, 0, TSDB_TABLE_NAME_LEN);
            break;
          }
        }
      }
      if (TSDB_CODE_SUCCESS != code) {
        nodesDestroyList(pMergeKeys);
        nodesDestroyList(extraAggFuncs);
      }
    }
  }
  code = stbSplCreateMergeNode(pCtx, NULL, pInfo->pSplitNode, pMergeKeys, pChildAgg, groupSort, true);
  if (TSDB_CODE_SUCCESS == code && sortForGroup) {
    SMergeLogicNode* pMerge =
        (SMergeLogicNode*)nodesListGetNode(pInfo->pSplitNode->pChildren, LIST_LENGTH(pInfo->pSplitNode->pChildren) - 1);
    pMerge->inputWithGroupId = true;
  }
  return code;
}

static int32_t stbSplSplitAggNodeForCrossTable(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  SLogicNode* pPartAgg = NULL;
  int32_t     code = stbSplCreatePartAggNode((SAggLogicNode*)pInfo->pSplitNode, &pPartAgg);

  if (TSDB_CODE_SUCCESS == code) {
    // if slimit was pushed down to agg, agg will be pipelined mode, add sort merge before parent agg
    if (pInfo->pSplitNode->forceCreateNonBlockingOptr)
      code = stbSplAggNodeCreateMerge(pCxt, pInfo, pPartAgg);
    else
      code = stbSplCreateExchangeNode(pCxt, pInfo->pSplitNode, pPartAgg);
  } else {
    nodesDestroyNode((SNode*)pPartAgg);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pInfo->pSubplan->pChildren,
                                     (SNode*)splCreateScanSubplan(pCxt, pPartAgg, SPLIT_FLAG_STABLE_SPLIT));
  }
  pInfo->pSubplan->subplanType = SUBPLAN_TYPE_MERGE;
  ++(pCxt->groupId);
  return code;
}

static int32_t stbSplSplitAggNode(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  if (isPartTableAgg((SAggLogicNode*)pInfo->pSplitNode)) {
    return stbSplSplitAggNodeForPartTable(pCxt, pInfo);
  }
  return stbSplSplitAggNodeForCrossTable(pCxt, pInfo);
}

static SNode* stbSplCreateColumnNode(SExprNode* pExpr) {
  SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
  if (NULL == pCol) {
    return NULL;
  }
  if (QUERY_NODE_COLUMN == nodeType(pExpr)) {
    strcpy(pCol->dbName, ((SColumnNode*)pExpr)->dbName);
    strcpy(pCol->tableName, ((SColumnNode*)pExpr)->tableName);
    strcpy(pCol->tableAlias, ((SColumnNode*)pExpr)->tableAlias);
    strcpy(pCol->colName, ((SColumnNode*)pExpr)->colName);
  } else {
    strcpy(pCol->colName, pExpr->aliasName);
  }
  strcpy(pCol->node.aliasName, pExpr->aliasName);
  strcpy(pCol->node.userAlias, pExpr->userAlias);
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
    pPartSort->groupSort = pSort->groupSort;
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

static void stbSplSetScanPartSort(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    SScanLogicNode* pScan = (SScanLogicNode*)pNode;
    if (NULL != pScan->pGroupTags) {
      pScan->groupSort = true;
    }
  } else {
    if (1 == LIST_LENGTH(pNode->pChildren)) {
      stbSplSetScanPartSort((SLogicNode*)nodesListGetNode(pNode->pChildren, 0));
    }
  }
}

static int32_t stbSplSplitSortNode(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  SLogicNode* pPartSort = NULL;
  SNodeList*  pMergeKeys = NULL;
  bool        groupSort = ((SSortLogicNode*)pInfo->pSplitNode)->groupSort;
  int32_t     code = stbSplCreatePartSortNode((SSortLogicNode*)pInfo->pSplitNode, &pPartSort, &pMergeKeys);
  if (TSDB_CODE_SUCCESS == code) {
    code = stbSplCreateMergeNode(pCxt, pInfo->pSubplan, pInfo->pSplitNode, pMergeKeys, pPartSort, groupSort, true);
  }
  if (TSDB_CODE_SUCCESS == code) {
    nodesDestroyNode((SNode*)pInfo->pSplitNode);
    if (groupSort) {
      stbSplSetScanPartSort(pPartSort);
    }
    code = nodesListMakeStrictAppend(&pInfo->pSubplan->pChildren,
                                     (SNode*)splCreateScanSubplan(pCxt, pPartSort, SPLIT_FLAG_STABLE_SPLIT));
  }
  pInfo->pSubplan->subplanType = SUBPLAN_TYPE_MERGE;
  ++(pCxt->groupId);
  return code;
}

static int32_t stbSplGetSplitNodeForScan(SStableSplitInfo* pInfo, SLogicNode** pSplitNode) {
  *pSplitNode = pInfo->pSplitNode;
  if (NULL != pInfo->pSplitNode->pParent && 
      QUERY_NODE_LOGIC_PLAN_PROJECT == nodeType(pInfo->pSplitNode->pParent) &&
      NULL == pInfo->pSplitNode->pParent->pLimit && NULL == pInfo->pSplitNode->pParent->pSlimit && 
      !((SProjectLogicNode*)pInfo->pSplitNode->pParent)->inputIgnoreGroup) {
    *pSplitNode = pInfo->pSplitNode->pParent;
    if (NULL != pInfo->pSplitNode->pLimit) {
      (*pSplitNode)->pLimit = nodesCloneNode(pInfo->pSplitNode->pLimit);
      if (NULL == (*pSplitNode)->pLimit) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      ((SLimitNode*)pInfo->pSplitNode->pLimit)->limit += ((SLimitNode*)pInfo->pSplitNode->pLimit)->offset;
      ((SLimitNode*)pInfo->pSplitNode->pLimit)->offset = 0;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t stbSplSplitScanNodeWithoutPartTags(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  SLogicNode* pSplitNode = NULL;
  int32_t     code = stbSplGetSplitNodeForScan(pInfo, &pSplitNode);
  if (TSDB_CODE_SUCCESS == code) {
    code = splCreateExchangeNodeForSubplan(pCxt, pInfo->pSubplan, pSplitNode, SUBPLAN_TYPE_MERGE);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pInfo->pSubplan->pChildren,
                                     (SNode*)splCreateScanSubplan(pCxt, pSplitNode, SPLIT_FLAG_STABLE_SPLIT));
  }
  ++(pCxt->groupId);
  return code;
}

static int32_t stbSplSplitScanNodeWithPartTags(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  SLogicNode* pSplitNode = NULL;
  int32_t     code = stbSplGetSplitNodeForScan(pInfo, &pSplitNode);
  if (TSDB_CODE_SUCCESS == code) {
    code = stbSplCreateMergeNode(pCxt, pInfo->pSubplan, pSplitNode, NULL, pSplitNode, true, true);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pInfo->pSubplan->pChildren,
                                     (SNode*)splCreateScanSubplan(pCxt, pSplitNode, SPLIT_FLAG_STABLE_SPLIT));
  }
  pInfo->pSubplan->subplanType = SUBPLAN_TYPE_MERGE;
  ++(pCxt->groupId);
  return code;
}

static SNode* stbSplFindPrimaryKeyFromScan(SScanLogicNode* pScan) {
  bool   find = false;
  SNode* pCol = NULL;
  FOREACH(pCol, pScan->pScanCols) {
    if (PRIMARYKEY_TIMESTAMP_COL_ID == ((SColumnNode*)pCol)->colId) {
      find = true;
      break;
    }
  }
  if (!find) {
    return NULL;
  }
  SNode* pTarget = NULL;
  FOREACH(pTarget, pScan->node.pTargets) {
    if (nodesEqualNode(pTarget, pCol)) {
      return pCol;
    }
  }
  nodesListStrictAppend(pScan->node.pTargets, nodesCloneNode(pCol));
  return pCol;
}

static int32_t stbSplCreateMergeScanNode(SScanLogicNode* pScan, SLogicNode** pOutputMergeScan,
                                         SNodeList** pOutputMergeKeys) {
  SNodeList* pChildren = pScan->node.pChildren;
  pScan->node.pChildren = NULL;

  int32_t         code = TSDB_CODE_SUCCESS;
  SScanLogicNode* pMergeScan = (SScanLogicNode*)nodesCloneNode((SNode*)pScan);
  if (NULL == pMergeScan) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }

  SNodeList* pMergeKeys = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    pMergeScan->scanType = SCAN_TYPE_TABLE_MERGE;
    pMergeScan->filesetDelimited = true;
    pMergeScan->node.pChildren = pChildren;
    splSetParent((SLogicNode*)pMergeScan);
    code = stbSplCreateMergeKeysByPrimaryKey(stbSplFindPrimaryKeyFromScan(pMergeScan),
                                             pMergeScan->scanSeq[0] > 0 ? ORDER_ASC : ORDER_DESC, &pMergeKeys);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pOutputMergeScan = (SLogicNode*)pMergeScan;
    *pOutputMergeKeys = pMergeKeys;
  } else {
    nodesDestroyNode((SNode*)pMergeScan);
    nodesDestroyList(pMergeKeys);
  }

  return code;
}

static int32_t stbSplSplitMergeScanNode(SSplitContext* pCxt, SLogicSubplan* pSubplan, SScanLogicNode* pScan,
                                        bool groupSort) {
  SLogicNode* pMergeScan = NULL;
  SNodeList*  pMergeKeys = NULL;
  int32_t     code = stbSplCreateMergeScanNode(pScan, &pMergeScan, &pMergeKeys);
  if (TSDB_CODE_SUCCESS == code) {
    if (NULL != pMergeScan->pLimit) {
      ((SLimitNode*)pMergeScan->pLimit)->limit += ((SLimitNode*)pMergeScan->pLimit)->offset;
      ((SLimitNode*)pMergeScan->pLimit)->offset = 0;
    }
    code = stbSplCreateMergeNode(pCxt, pSubplan, (SLogicNode*)pScan, pMergeKeys, pMergeScan, groupSort, true);
  }
  if (TSDB_CODE_SUCCESS == code) {
    nodesDestroyNode((SNode*)pScan);
    code = nodesListMakeStrictAppend(&pSubplan->pChildren,
                                     (SNode*)splCreateScanSubplan(pCxt, pMergeScan, SPLIT_FLAG_STABLE_SPLIT));
  }
  ++(pCxt->groupId);
  return code;
}

static int32_t stbSplSplitScanNode(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  SScanLogicNode* pScan = (SScanLogicNode*)pInfo->pSplitNode;
  if (SCAN_TYPE_TABLE_MERGE == pScan->scanType) {
    pInfo->pSubplan->subplanType = SUBPLAN_TYPE_MERGE;
    return stbSplSplitMergeScanNode(pCxt, pInfo->pSubplan, pScan, true);
  }
  if (NULL != pScan->pGroupTags) {
    return stbSplSplitScanNodeWithPartTags(pCxt, pInfo);
  }
  return stbSplSplitScanNodeWithoutPartTags(pCxt, pInfo);
}

static int32_t stbSplSplitJoinNodeImpl(SSplitContext* pCxt, SLogicSubplan* pSubplan, SJoinLogicNode* pJoin) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pChild = NULL;
  FOREACH(pChild, pJoin->node.pChildren) {
    if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pChild)) {
      //if (pJoin->node.dynamicOp) {
      //  code = TSDB_CODE_SUCCESS;
      //} else {
        code = stbSplSplitMergeScanNode(pCxt, pSubplan, (SScanLogicNode*)pChild, false);
      //}
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
    //if (!pInfo->pSplitNode->dynamicOp) {
      pInfo->pSubplan->subplanType = SUBPLAN_TYPE_MERGE;
    //}
    SPLIT_FLAG_SET_MASK(pInfo->pSubplan->splitFlag, SPLIT_FLAG_STABLE_SPLIT);
  }
  return code;
}

static int32_t stbSplCreateMergeKeysForPartitionNode(SLogicNode* pPart, SNodeList** pMergeKeys) {
  SScanLogicNode* pScan = (SScanLogicNode*)nodesListGetNode(pPart->pChildren, 0);
  SNode*          pPrimaryKey = nodesCloneNode(stbSplFindPrimaryKeyFromScan(pScan));
  if (NULL == pPrimaryKey) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  int32_t code = nodesListAppend(pPart->pTargets, pPrimaryKey);
  if (TSDB_CODE_SUCCESS == code) {
    code = stbSplCreateMergeKeysByPrimaryKey(pPrimaryKey, pScan->scanSeq[0] > 0 ? ORDER_ASC : ORDER_DESC, pMergeKeys);
  }
  return code;
}

static int32_t stbSplSplitPartitionNode(SSplitContext* pCxt, SStableSplitInfo* pInfo) {
  int32_t    code = TSDB_CODE_SUCCESS;
  SNodeList* pMergeKeys = NULL;
  if (pInfo->pSplitNode->requireDataOrder >= DATA_ORDER_LEVEL_IN_GROUP) {
    code = stbSplCreateMergeKeysForPartitionNode(pInfo->pSplitNode, &pMergeKeys);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = stbSplCreateMergeNode(pCxt, pInfo->pSubplan, pInfo->pSplitNode, pMergeKeys, pInfo->pSplitNode, true, true);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pInfo->pSubplan->pChildren,
                                     (SNode*)splCreateScanSubplan(pCxt, pInfo->pSplitNode, SPLIT_FLAG_STABLE_SPLIT));
  }
  pInfo->pSubplan->subplanType = SUBPLAN_TYPE_MERGE;
  ++(pCxt->groupId);
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
    case QUERY_NODE_LOGIC_PLAN_PARTITION:
      code = stbSplSplitPartitionNode(pCxt, &info);
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

static int32_t unionSplitSubplan(SSplitContext* pCxt, SLogicSubplan* pUnionSubplan, SLogicNode* pSplitNode) {
  SNodeList* pSubplanChildren = pUnionSubplan->pChildren;
  pUnionSubplan->pChildren = NULL;

  int32_t code = TSDB_CODE_SUCCESS;

  SNode* pChild = NULL;
  FOREACH(pChild, pSplitNode->pChildren) {
    SLogicSubplan* pNewSubplan = splCreateSubplan(pCxt, (SLogicNode*)pChild);
    code = nodesListMakeStrictAppend(&pUnionSubplan->pChildren, (SNode*)pNewSubplan);
    if (TSDB_CODE_SUCCESS == code) {
      REPLACE_NODE(NULL);
      code = splMountSubplan(pNewSubplan, pSubplanChildren);
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
    ++(pCxt->groupId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    nodesDestroyList(pSubplanChildren);
    NODES_DESTORY_LIST(pSplitNode->pChildren);
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

static int32_t unAllSplCreateExchangeNode(SSplitContext* pCxt, int32_t startGroupId, SLogicSubplan* pSubplan,
                                          SProjectLogicNode* pProject) {
  SExchangeLogicNode* pExchange = (SExchangeLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_EXCHANGE);
  if (NULL == pExchange) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pExchange->srcStartGroupId = startGroupId;
  pExchange->srcEndGroupId = pCxt->groupId - 1;
  pExchange->node.precision = pProject->node.precision;
  pExchange->node.pTargets = nodesCloneList(pProject->node.pTargets);
  pExchange->node.pConditions = nodesCloneNode(pProject->node.pConditions);
  if (NULL == pExchange->node.pTargets || (NULL != pProject->node.pConditions && NULL == pExchange->node.pConditions)) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  TSWAP(pExchange->node.pLimit, pProject->node.pLimit);

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

  int32_t startGroupId = pCxt->groupId;
  int32_t code = unionSplitSubplan(pCxt, info.pSubplan, (SLogicNode*)info.pProject);
  if (TSDB_CODE_SUCCESS == code) {
    code = unAllSplCreateExchangeNode(pCxt, startGroupId, info.pSubplan, info.pProject);
  }
  pCxt->split = true;
  return code;
}

typedef struct SUnionDistinctSplitInfo {
  SAggLogicNode* pAgg;
  SLogicSubplan* pSubplan;
} SUnionDistinctSplitInfo;

static int32_t unDistSplCreateExchangeNode(SSplitContext* pCxt, int32_t startGroupId, SLogicSubplan* pSubplan,
                                           SAggLogicNode* pAgg) {
  SExchangeLogicNode* pExchange = (SExchangeLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_EXCHANGE);
  if (NULL == pExchange) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pExchange->srcStartGroupId = startGroupId;
  pExchange->srcEndGroupId = pCxt->groupId - 1;
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

  int32_t startGroupId = pCxt->groupId;
  int32_t code = unionSplitSubplan(pCxt, info.pSubplan, (SLogicNode*)info.pAgg);
  if (TSDB_CODE_SUCCESS == code) {
    code = unDistSplCreateExchangeNode(pCxt, startGroupId, info.pSubplan, info.pAgg);
  }
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
    int32_t nodeType = nodeType(nodesListGetNode(pNode->pChildren, 0));
    if (nodeType == QUERY_NODE_LOGIC_PLAN_EXCHANGE || nodeType == QUERY_NODE_LOGIC_PLAN_MERGE) {
      pInfo->pMerge = (SMergeLogicNode*)pNode;
      pInfo->pSubplan = pSubplan;
      return true;
    }
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

typedef struct SInsertSelectSplitInfo {
  SLogicNode*    pQueryRoot;
  SLogicSubplan* pSubplan;
} SInsertSelectSplitInfo;

static bool insSelSplFindSplitNode(SSplitContext* pCxt, SLogicSubplan* pSubplan, SLogicNode* pNode,
                                   SInsertSelectSplitInfo* pInfo) {
  if (QUERY_NODE_LOGIC_PLAN_VNODE_MODIFY == nodeType(pNode) && 1 == LIST_LENGTH(pNode->pChildren) &&
      MODIFY_TABLE_TYPE_INSERT == ((SVnodeModifyLogicNode*)pNode)->modifyType) {
    pInfo->pQueryRoot = (SLogicNode*)nodesListGetNode(pNode->pChildren, 0);
    pInfo->pSubplan = pSubplan;
    return true;
  }
  return false;
}

static int32_t insertSelectSplit(SSplitContext* pCxt, SLogicSubplan* pSubplan) {
  SInsertSelectSplitInfo info = {0};
  if (!splMatch(pCxt, pSubplan, SPLIT_FLAG_INSERT_SPLIT, (FSplFindSplitNode)insSelSplFindSplitNode, &info)) {
    return TSDB_CODE_SUCCESS;
  }

  SLogicSubplan* pNewSubplan = NULL;
  SNodeList*     pSubplanChildren = info.pSubplan->pChildren;
  int32_t        code = splCreateExchangeNodeForSubplan(pCxt, info.pSubplan, info.pQueryRoot, SUBPLAN_TYPE_MODIFY);
  if (TSDB_CODE_SUCCESS == code) {
    pNewSubplan = splCreateSubplan(pCxt, info.pQueryRoot);
    if (NULL == pNewSubplan) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&info.pSubplan->pChildren, (SNode*)pNewSubplan);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = splMountSubplan(pNewSubplan, pSubplanChildren);
  }

  SPLIT_FLAG_SET_MASK(info.pSubplan->splitFlag, SPLIT_FLAG_INSERT_SPLIT);
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
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode) && NULL != pNode->pParent &&
      QUERY_NODE_LOGIC_PLAN_INTERP_FUNC != nodeType(pNode->pParent) && ((SScanLogicNode*)pNode)->scanSeq[0] <= 1 &&
      ((SScanLogicNode*)pNode)->scanSeq[1] <= 1) {
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
  ((SScanLogicNode*)info.pSplitNode)->dataRequired = FUNC_DATA_REQUIRED_DATA_LOAD;
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
  {.pName = "SmaIndexSplit",        .splitFunc = smaIndexSplit}, // not used yet
  {.pName = "InsertSelectSplit",    .splitFunc = insertSelectSplit}
};
// clang-format on

static const int32_t splitRuleNum = (sizeof(splitRuleSet) / sizeof(SSplitRule));

static void dumpLogicSubplan(const char* pRuleName, SLogicSubplan* pSubplan) {
  if (!tsQueryPlannerTrace) {
    return;
  }
  char* pStr = NULL;
  nodesNodeToString((SNode*)pSubplan, false, &pStr, NULL);
  if (NULL == pRuleName) {
    qDebugL("before split, JsonPlan: %s", pStr);
  } else {
    qDebugL("apply split %s rule, JsonPlan: %s", pRuleName, pStr);
  }
  taosMemoryFree(pStr);
}

static int32_t applySplitRule(SPlanContext* pCxt, SLogicSubplan* pSubplan) {
  SSplitContext cxt = {
      .pPlanCxt = pCxt, .queryId = pSubplan->id.queryId, .groupId = pSubplan->id.groupId + 1, .split = false};
  bool split = false;
  dumpLogicSubplan(NULL, pSubplan);
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

static bool needSplitSubplan(SLogicSubplan* pLogicSubplan) {
  if (QUERY_NODE_LOGIC_PLAN_VNODE_MODIFY != nodeType(pLogicSubplan->pNode)) {
    return true;
  }
  SVnodeModifyLogicNode* pModify = (SVnodeModifyLogicNode*)pLogicSubplan->pNode;
  return (MODIFY_TABLE_TYPE_INSERT == pModify->modifyType && NULL != pModify->node.pChildren);
}

int32_t splitLogicPlan(SPlanContext* pCxt, SLogicSubplan* pLogicSubplan) {
  if (!needSplitSubplan(pLogicSubplan)) {
    setVgroupsInfo(pLogicSubplan->pNode, pLogicSubplan);
    return TSDB_CODE_SUCCESS;
  }
  return applySplitRule(pCxt, pLogicSubplan);
}
