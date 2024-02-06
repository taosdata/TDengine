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

#include "filter.h"
#include "functionMgt.h"
#include "planInt.h"
#include "systable.h"
#include "tglobal.h"
#include "ttime.h"

#define OPTIMIZE_FLAG_MASK(n) (1 << n)

#define OPTIMIZE_FLAG_SCAN_PATH       OPTIMIZE_FLAG_MASK(0)
#define OPTIMIZE_FLAG_PUSH_DOWN_CONDE OPTIMIZE_FLAG_MASK(1)

#define OPTIMIZE_FLAG_SET_MASK(val, mask)  (val) |= (mask)
#define OPTIMIZE_FLAG_CLEAR_MASK(val, mask)  (val) &= (~(mask))
#define OPTIMIZE_FLAG_TEST_MASK(val, mask) (((val) & (mask)) != 0)

typedef struct SOptimizeContext {
  SPlanContext* pPlanCxt;
  bool          optimized;
} SOptimizeContext;

typedef int32_t (*FOptimize)(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan);

typedef struct SOptimizeRule {
  char*     pName;
  FOptimize optimizeFunc;
} SOptimizeRule;

typedef enum EScanOrder { SCAN_ORDER_ASC = 1, SCAN_ORDER_DESC, SCAN_ORDER_BOTH } EScanOrder;

typedef struct SOsdInfo {
  SScanLogicNode* pScan;
  SNodeList*      pSdrFuncs;
  SNodeList*      pDsoFuncs;
  EScanOrder      scanOrder;
} SOsdInfo;

typedef struct SCpdIsMultiTableCondCxt {
  SSHashObj* pLeftTbls;
  SSHashObj* pRightTbls;
  bool       havaLeftCol;
  bool       haveRightCol;
} SCpdIsMultiTableCondCxt;

typedef enum ECondAction {
  COND_ACTION_STAY = 1,
  COND_ACTION_PUSH_JOIN,
  COND_ACTION_PUSH_LEFT_CHILD,
  COND_ACTION_PUSH_RIGHT_CHILD
  // after supporting outer join, there are other possibilities
} ECondAction;

typedef bool (*FMayBeOptimized)(SLogicNode* pNode);
typedef bool (*FShouldBeOptimized)(SLogicNode* pNode, void* pInfo);

static SLogicNode* optFindPossibleNode(SLogicNode* pNode, FMayBeOptimized func) {
  if (func(pNode)) {
    return pNode;
  }
  SNode* pChild;
  FOREACH(pChild, pNode->pChildren) {
    SLogicNode* pScanNode = optFindPossibleNode((SLogicNode*)pChild, func);
    if (NULL != pScanNode) {
      return pScanNode;
    }
  }
  return NULL;
}

static bool optFindEligibleNode(SLogicNode* pNode, FShouldBeOptimized func, void* pInfo) {
  if (func(pNode, pInfo)) {
    return true;
  }
  SNode* pChild;
  FOREACH(pChild, pNode->pChildren) {
    if (optFindEligibleNode((SLogicNode*)pChild, func, pInfo)) {
      return true;
    }
  }
  return false;
}

static void optResetParent(SLogicNode* pNode) {
  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) { ((SLogicNode*)pChild)->pParent = pNode; }
}

static EDealRes optRebuildTbanme(SNode** pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(*pNode) && COLUMN_TYPE_TBNAME == ((SColumnNode*)*pNode)->colType) {
    SFunctionNode* pFunc = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
    if (NULL == pFunc) {
      *(int32_t*)pContext = TSDB_CODE_OUT_OF_MEMORY;
      return DEAL_RES_ERROR;
    }
    strcpy(pFunc->functionName, "tbname");
    pFunc->funcType = FUNCTION_TYPE_TBNAME;
    pFunc->node.resType = ((SColumnNode*)*pNode)->node.resType;
    nodesDestroyNode(*pNode);
    *pNode = (SNode*)pFunc;
    return DEAL_RES_IGNORE_CHILD;
  }
  return DEAL_RES_CONTINUE;
}

static void optSetParentOrder(SLogicNode* pNode, EOrder order, SLogicNode* pNodeForcePropagate) {
  if (NULL == pNode) {
    return;
  }
  pNode->inputTsOrder = order;
  switch (nodeType(pNode)) {
    // for those nodes that will change the order, stop propagating
    // case QUERY_NODE_LOGIC_PLAN_WINDOW:
    case QUERY_NODE_LOGIC_PLAN_JOIN:
    case QUERY_NODE_LOGIC_PLAN_AGG:
    case QUERY_NODE_LOGIC_PLAN_SORT:
      if (pNode == pNodeForcePropagate) {
        pNode->outputTsOrder = order;
        break;
      } else
        return;
    case QUERY_NODE_LOGIC_PLAN_WINDOW:
      // Window output ts order default to be asc, and changed when doing sort by primary key optimization.
      // We stop propagate the original order to parents.
      // Use window output ts order instead.
      order = pNode->outputTsOrder;
      break;
    default:
      pNode->outputTsOrder = order;
      break;
  }
  optSetParentOrder(pNode->pParent, order, pNodeForcePropagate);
}

EDealRes scanPathOptHaveNormalColImpl(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    *((bool*)pContext) =
        (COLUMN_TYPE_TAG != ((SColumnNode*)pNode)->colType && COLUMN_TYPE_TBNAME != ((SColumnNode*)pNode)->colType);
    return *((bool*)pContext) ? DEAL_RES_END : DEAL_RES_IGNORE_CHILD;
  }
  return DEAL_RES_CONTINUE;
}

static bool scanPathOptHaveNormalCol(SNodeList* pList) {
  bool res = false;
  nodesWalkExprsPostOrder(pList, scanPathOptHaveNormalColImpl, &res);
  return res;
}

static bool scanPathOptMayBeOptimized(SLogicNode* pNode) {
  if (OPTIMIZE_FLAG_TEST_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_SCAN_PATH)) {
    return false;
  }
  if (QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pNode)) {
    return false;
  }
  return true;
}

static bool scanPathOptShouldGetFuncs(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_PARTITION == nodeType(pNode)) {
    if (pNode->pParent && QUERY_NODE_LOGIC_PLAN_WINDOW == nodeType(pNode->pParent)) {
      if (WINDOW_TYPE_INTERVAL == ((SWindowLogicNode*)pNode->pParent)->winType) return true;
    } else {
      return !scanPathOptHaveNormalCol(((SPartitionLogicNode*)pNode)->pPartitionKeys);
    }
  }

  if ((QUERY_NODE_LOGIC_PLAN_WINDOW == nodeType(pNode) &&
       WINDOW_TYPE_INTERVAL == ((SWindowLogicNode*)pNode)->winType)) {
    return true;
  }
  if (QUERY_NODE_LOGIC_PLAN_AGG == nodeType(pNode)) {
    return !scanPathOptHaveNormalCol(((SAggLogicNode*)pNode)->pGroupKeys);
  }
  return false;
}

static SNodeList* scanPathOptGetAllFuncs(SLogicNode* pNode) {
  if (!scanPathOptShouldGetFuncs(pNode)) return NULL;
  switch (nodeType(pNode)) {
    case QUERY_NODE_LOGIC_PLAN_WINDOW:
      return ((SWindowLogicNode*)pNode)->pFuncs;
    case QUERY_NODE_LOGIC_PLAN_AGG:
      return ((SAggLogicNode*)pNode)->pAggFuncs;
    case QUERY_NODE_LOGIC_PLAN_PARTITION:
      return ((SPartitionLogicNode*)pNode)->pAggFuncs;
    default:
      break;
  }
  return NULL;
}

static bool scanPathOptIsSpecifiedFuncType(const SFunctionNode* pFunc, bool (*typeCheckFn)(int32_t)) {
  if (!typeCheckFn(pFunc->funcId)) return false;
  SNode* pPara;
  FOREACH(pPara, pFunc->pParameterList) {
    if (QUERY_NODE_COLUMN != nodeType(pPara) && QUERY_NODE_VALUE != nodeType(pPara)) {
      return false;
    }
  }
  return true;
}

static int32_t scanPathOptGetRelatedFuncs(SScanLogicNode* pScan, SNodeList** pSdrFuncs, SNodeList** pDsoFuncs) {
  SNodeList* pAllFuncs = scanPathOptGetAllFuncs(pScan->node.pParent);
  SNodeList* pTmpSdrFuncs = NULL;
  SNodeList* pTmpDsoFuncs = NULL;
  SNode*     pNode = NULL;
  bool       otherFunc = false;
  FOREACH(pNode, pAllFuncs) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    int32_t        code = TSDB_CODE_SUCCESS;
    if (scanPathOptIsSpecifiedFuncType(pFunc, fmIsSpecialDataRequiredFunc)) {
      code = nodesListMakeStrictAppend(&pTmpSdrFuncs, nodesCloneNode(pNode));
    } else if (scanPathOptIsSpecifiedFuncType(pFunc, fmIsDynamicScanOptimizedFunc)) {
      code = nodesListMakeStrictAppend(&pTmpDsoFuncs, nodesCloneNode(pNode));
    } else if (scanPathOptIsSpecifiedFuncType(pFunc, fmIsSkipScanCheckFunc)) {
      continue;
    } else {
      otherFunc = true;
      break;
    }
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyList(pTmpSdrFuncs);
      nodesDestroyList(pTmpDsoFuncs);
      return code;
    }
  }
  if (otherFunc) {
    nodesDestroyList(pTmpSdrFuncs);
    nodesDestroyList(pTmpDsoFuncs);
  } else {
    *pSdrFuncs = pTmpSdrFuncs;
    *pDsoFuncs = pTmpDsoFuncs;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t scanPathOptGetScanOrder(SScanLogicNode* pScan, EScanOrder* pScanOrder) {
  SNodeList* pAllFuncs = scanPathOptGetAllFuncs(pScan->node.pParent);
  SNode*     pNode = NULL;
  bool       hasFirst = false;
  bool       hasLast = false;
  bool       otherFunc = false;
  FOREACH(pNode, pAllFuncs) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    if (FUNCTION_TYPE_FIRST == pFunc->funcType) {
      hasFirst = true;
    } else if (FUNCTION_TYPE_LAST == pFunc->funcType || FUNCTION_TYPE_LAST_ROW == pFunc->funcType) {
      hasLast = true;
    } else if (FUNCTION_TYPE_SELECT_VALUE != pFunc->funcType) {
      otherFunc = true;
    }
  }
  if (hasFirst && hasLast && !otherFunc) {
    *pScanOrder = SCAN_ORDER_BOTH;
  } else if (hasLast) {
    *pScanOrder = SCAN_ORDER_DESC;
  } else {
    *pScanOrder = SCAN_ORDER_ASC;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t scanPathOptSetOsdInfo(SOsdInfo* pInfo) {
  int32_t code = scanPathOptGetRelatedFuncs(pInfo->pScan, &pInfo->pSdrFuncs, &pInfo->pDsoFuncs);
  if (TSDB_CODE_SUCCESS == code) {
    code = scanPathOptGetScanOrder(pInfo->pScan, &pInfo->scanOrder);
  }
  return code;
}

static int32_t scanPathOptMatch(SOptimizeContext* pCxt, SLogicNode* pLogicNode, SOsdInfo* pInfo) {
  pInfo->pScan = (SScanLogicNode*)optFindPossibleNode(pLogicNode, scanPathOptMayBeOptimized);
  if (NULL == pInfo->pScan) {
    return TSDB_CODE_SUCCESS;
  }
  return scanPathOptSetOsdInfo(pInfo);
}

static EFuncDataRequired scanPathOptPromoteDataRequired(EFuncDataRequired l, EFuncDataRequired r) {
  switch (l) {
    case FUNC_DATA_REQUIRED_DATA_LOAD:
      return l;
    case FUNC_DATA_REQUIRED_SMA_LOAD:
      return FUNC_DATA_REQUIRED_DATA_LOAD == r ? r : l;
    case FUNC_DATA_REQUIRED_NOT_LOAD:
      return FUNC_DATA_REQUIRED_FILTEROUT == r ? l : r;
    default:
      break;
  }
  return r;
}

static int32_t scanPathOptGetDataRequired(SNodeList* pFuncs) {
  if (NULL == pFuncs) {
    return FUNC_DATA_REQUIRED_DATA_LOAD;
  }
  EFuncDataRequired dataRequired = FUNC_DATA_REQUIRED_FILTEROUT;
  SNode*            pFunc = NULL;
  FOREACH(pFunc, pFuncs) {
    dataRequired = scanPathOptPromoteDataRequired(dataRequired, fmFuncDataRequired((SFunctionNode*)pFunc, NULL));
  }
  return dataRequired;
}

static void scanPathOptSetScanWin(SScanLogicNode* pScan) {
  SLogicNode* pParent = pScan->node.pParent;
  if (QUERY_NODE_LOGIC_PLAN_PARTITION == nodeType(pParent) && pParent->pParent &&
      QUERY_NODE_LOGIC_PLAN_WINDOW == nodeType(pParent->pParent)) {
    pParent = pParent->pParent;
  }
  if (QUERY_NODE_LOGIC_PLAN_WINDOW == nodeType(pParent)) {
    pScan->interval = ((SWindowLogicNode*)pParent)->interval;
    pScan->offset = ((SWindowLogicNode*)pParent)->offset;
    pScan->sliding = ((SWindowLogicNode*)pParent)->sliding;
    pScan->intervalUnit = ((SWindowLogicNode*)pParent)->intervalUnit;
    pScan->slidingUnit = ((SWindowLogicNode*)pParent)->slidingUnit;
  }
}

static void scanPathOptSetScanOrder(EScanOrder scanOrder, SScanLogicNode* pScan) {
  if (pScan->sortPrimaryKey || pScan->scanSeq[0] > 1 || pScan->scanSeq[1] > 1) {
    return;
  }
  pScan->node.outputTsOrder = (SCAN_ORDER_ASC == scanOrder) ? ORDER_ASC : ORDER_DESC;
  switch (scanOrder) {
    case SCAN_ORDER_ASC:
      pScan->scanSeq[0] = 1;
      pScan->scanSeq[1] = 0;
      optSetParentOrder(pScan->node.pParent, ORDER_ASC, NULL);
      break;
    case SCAN_ORDER_DESC:
      pScan->scanSeq[0] = 0;
      pScan->scanSeq[1] = 1;
      optSetParentOrder(pScan->node.pParent, ORDER_DESC, NULL);
      break;
    case SCAN_ORDER_BOTH:
      pScan->scanSeq[0] = 1;
      pScan->scanSeq[1] = 1;
      break;
    default:
      break;
  }
}

static void scanPathOptSetGroupOrderScan(SScanLogicNode* pScan) {
  if (pScan->tableType != TSDB_SUPER_TABLE) return;

  if (pScan->node.pParent && nodeType(pScan->node.pParent) == QUERY_NODE_LOGIC_PLAN_AGG) {
    SAggLogicNode* pAgg = (SAggLogicNode*)pScan->node.pParent;
    bool           withSlimit = pAgg->node.pSlimit != NULL;
    if (withSlimit && (isPartTableAgg(pAgg) || isPartTagAgg(pAgg))) {
      pScan->groupOrderScan = pAgg->node.forceCreateNonBlockingOptr = true;
    }
  }
}

static int32_t scanPathOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SOsdInfo info = {.scanOrder = SCAN_ORDER_ASC};
  int32_t  code = scanPathOptMatch(pCxt, pLogicSubplan->pNode, &info);
  if (TSDB_CODE_SUCCESS == code && info.pScan) {
    scanPathOptSetScanWin(info.pScan);
    if (!pCxt->pPlanCxt->streamQuery) {
      scanPathOptSetScanOrder(info.scanOrder, info.pScan);
    }
    scanPathOptSetGroupOrderScan(info.pScan);
  }
  if (TSDB_CODE_SUCCESS == code && (NULL != info.pDsoFuncs || NULL != info.pSdrFuncs)) {
    info.pScan->dataRequired = scanPathOptGetDataRequired(info.pSdrFuncs);
    info.pScan->pDynamicScanFuncs = info.pDsoFuncs;
  }
  if (TSDB_CODE_SUCCESS == code && info.pScan) {
    OPTIMIZE_FLAG_SET_MASK(info.pScan->node.optimizedFlag, OPTIMIZE_FLAG_SCAN_PATH);
    pCxt->optimized = true;
  }
  nodesDestroyList(info.pSdrFuncs);
  return code;
}

static int32_t pushDownCondOptMergeCond(SNode** pDst, SNode** pSrc) {
  SLogicConditionNode* pLogicCond = (SLogicConditionNode*)nodesMakeNode(QUERY_NODE_LOGIC_CONDITION);
  if (NULL == pLogicCond) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pLogicCond->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pLogicCond->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
  pLogicCond->condType = LOGIC_COND_TYPE_AND;
  int32_t code = nodesListMakeAppend(&pLogicCond->pParameterList, *pSrc);
  if (TSDB_CODE_SUCCESS == code) {
    *pSrc = NULL;
    code = nodesListMakeAppend(&pLogicCond->pParameterList, *pDst);
  }
  if (TSDB_CODE_SUCCESS == code) {
    *pDst = (SNode*)pLogicCond;
  } else {
    nodesDestroyNode((SNode*)pLogicCond);
  }
  return code;
}

static int32_t pushDownCondOptAppendCond(SNode** pCond, SNode** pAdditionalCond) {
  if (NULL == *pCond) {
    TSWAP(*pCond, *pAdditionalCond);
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(*pCond) &&
      LOGIC_COND_TYPE_AND == ((SLogicConditionNode*)*pCond)->condType) {
    code = nodesListAppend(((SLogicConditionNode*)*pCond)->pParameterList, *pAdditionalCond);
    if (TSDB_CODE_SUCCESS == code) {
      *pAdditionalCond = NULL;
    }
  } else {
    code = pushDownCondOptMergeCond(pCond, pAdditionalCond);
  }
  return code;
}

static int32_t pushDownCondOptCalcTimeRange(SOptimizeContext* pCxt, SScanLogicNode* pScan, SNode** pPrimaryKeyCond,
                                            SNode** pOtherCond) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (pCxt->pPlanCxt->topicQuery || pCxt->pPlanCxt->streamQuery) {
    code = pushDownCondOptAppendCond(pOtherCond, pPrimaryKeyCond);
  } else {
    bool isStrict = false;
    code = filterGetTimeRange(*pPrimaryKeyCond, &pScan->scanRange, &isStrict);
    if (TSDB_CODE_SUCCESS == code) {
      if (isStrict) {
        nodesDestroyNode(*pPrimaryKeyCond);
      } else {
        code = pushDownCondOptAppendCond(pOtherCond, pPrimaryKeyCond);
      }
      *pPrimaryKeyCond = NULL;
    }
  }
  return code;
}

static int32_t pushDownCondOptRebuildTbanme(SNode** pTagCond) {
  int32_t code = TSDB_CODE_SUCCESS;
  nodesRewriteExpr(pTagCond, optRebuildTbanme, &code);
  return code;
}

static int32_t pushDownCondOptDealScan(SOptimizeContext* pCxt, SScanLogicNode* pScan) {
  if (NULL == pScan->node.pConditions ||
      OPTIMIZE_FLAG_TEST_MASK(pScan->node.optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE) ||
      TSDB_SYSTEM_TABLE == pScan->tableType) {
    return TSDB_CODE_SUCCESS;
  }

  SNode*  pPrimaryKeyCond = NULL;
  SNode*  pOtherCond = NULL;
  int32_t code = filterPartitionCond(&pScan->node.pConditions, &pPrimaryKeyCond, &pScan->pTagIndexCond,
                                     &pScan->pTagCond, &pOtherCond);
  if (TSDB_CODE_SUCCESS == code && NULL != pScan->pTagCond) {
    code = pushDownCondOptRebuildTbanme(&pScan->pTagCond);
  }
  if (TSDB_CODE_SUCCESS == code && NULL != pPrimaryKeyCond) {
    code = pushDownCondOptCalcTimeRange(pCxt, pScan, &pPrimaryKeyCond, &pOtherCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pScan->node.pConditions = pOtherCond;
  }

  if (TSDB_CODE_SUCCESS == code) {
    OPTIMIZE_FLAG_SET_MASK(pScan->node.optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE);
    pCxt->optimized = true;
  } else {
    nodesDestroyNode(pPrimaryKeyCond);
    nodesDestroyNode(pOtherCond);
  }

  return code;
}

static bool pushDownCondOptBelongThisTable(SNode* pCondCol, SNodeList* pTableCols) {
  SNode* pTableCol = NULL;
  FOREACH(pTableCol, pTableCols) {
    if (nodesEqualNode(pCondCol, pTableCol)) {
      return true;
    }
  }
  return false;
}

static bool pushDownCondOptColInTableList(SNode* pCondCol, SSHashObj* pTables) {
  SColumnNode* pTableCol = (SColumnNode*)pCondCol;
  if (NULL == tSimpleHashGet(pTables, pTableCol->tableAlias, strlen(pTableCol->tableAlias))) {
    return false;
  }
  return true;
}


static EDealRes pushDownCondOptIsCrossTableCond(SNode* pNode, void* pContext) {
  SCpdIsMultiTableCondCxt* pCxt = pContext;
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    if (pushDownCondOptColInTableList(pNode, pCxt->pLeftTbls)) {
      pCxt->havaLeftCol = true;
    } else if (pushDownCondOptColInTableList(pNode, pCxt->pRightTbls)) {
      pCxt->haveRightCol = true;
    }
    return pCxt->havaLeftCol && pCxt->haveRightCol ? DEAL_RES_END : DEAL_RES_CONTINUE;
  }
  return DEAL_RES_CONTINUE;
}

static ECondAction pushDownCondOptGetCondAction(EJoinType joinType, SSHashObj* pLeftTbls, SSHashObj* pRightTbls,
                                                SNode* pNode) {
  SCpdIsMultiTableCondCxt cxt = {
      .pLeftTbls = pLeftTbls, .pRightTbls = pRightTbls, .havaLeftCol = false, .haveRightCol = false};
  nodesWalkExpr(pNode, pushDownCondOptIsCrossTableCond, &cxt);
  return (JOIN_TYPE_INNER != joinType
              ? COND_ACTION_STAY
              : (cxt.havaLeftCol && cxt.haveRightCol
                     ? COND_ACTION_PUSH_JOIN
                     : (cxt.havaLeftCol ? COND_ACTION_PUSH_LEFT_CHILD : COND_ACTION_PUSH_RIGHT_CHILD)));
}

static int32_t pushDownCondOptPartLogicCond(SJoinLogicNode* pJoin, SNode** pOnCond, SNode** pLeftChildCond,
                                            SNode** pRightChildCond) {
  SLogicConditionNode* pLogicCond = (SLogicConditionNode*)pJoin->node.pConditions;
  if (LOGIC_COND_TYPE_AND != pLogicCond->condType) {
    return TSDB_CODE_PLAN_NOT_SUPPORT_JOIN_COND;
  }

  int32_t    code = TSDB_CODE_SUCCESS;
  SSHashObj* pLeftTables = NULL;
  SSHashObj* pRightTables = NULL;
  collectTableAliasFromNodes(nodesListGetNode(pJoin->node.pChildren, 0), &pLeftTables);
  collectTableAliasFromNodes(nodesListGetNode(pJoin->node.pChildren, 1), &pRightTables);

  SNodeList* pOnConds = NULL;
  SNodeList* pLeftChildConds = NULL;
  SNodeList* pRightChildConds = NULL;
  SNodeList* pRemainConds = NULL;
  SNode*     pCond = NULL;
  FOREACH(pCond, pLogicCond->pParameterList) {
    ECondAction condAction = pushDownCondOptGetCondAction(pJoin->joinType, pLeftTables, pRightTables, pCond);
    if (COND_ACTION_PUSH_JOIN == condAction) {
      code = nodesListMakeAppend(&pOnConds, nodesCloneNode(pCond));
    } else if (COND_ACTION_PUSH_LEFT_CHILD == condAction) {
      code = nodesListMakeAppend(&pLeftChildConds, nodesCloneNode(pCond));
    } else if (COND_ACTION_PUSH_RIGHT_CHILD == condAction) {
      code = nodesListMakeAppend(&pRightChildConds, nodesCloneNode(pCond));
    } else {
      code = nodesListMakeAppend(&pRemainConds, nodesCloneNode(pCond));
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }

  tSimpleHashCleanup(pLeftTables);
  tSimpleHashCleanup(pRightTables);

  SNode* pTempOnCond = NULL;
  SNode* pTempLeftChildCond = NULL;
  SNode* pTempRightChildCond = NULL;
  SNode* pTempRemainCond = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesMergeConds(&pTempOnCond, &pOnConds);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesMergeConds(&pTempLeftChildCond, &pLeftChildConds);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesMergeConds(&pTempRightChildCond, &pRightChildConds);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesMergeConds(&pTempRemainCond, &pRemainConds);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pOnCond = pTempOnCond;
    *pLeftChildCond = pTempLeftChildCond;
    *pRightChildCond = pTempRightChildCond;
    nodesDestroyNode(pJoin->node.pConditions);
    pJoin->node.pConditions = pTempRemainCond;
  } else {
    nodesDestroyList(pOnConds);
    nodesDestroyList(pLeftChildConds);
    nodesDestroyList(pRightChildConds);
    nodesDestroyList(pRemainConds);
    nodesDestroyNode(pTempOnCond);
    nodesDestroyNode(pTempLeftChildCond);
    nodesDestroyNode(pTempRightChildCond);
    nodesDestroyNode(pTempRemainCond);
  }

  return code;
}

static int32_t pushDownCondOptPartOpCond(SJoinLogicNode* pJoin, SNode** pOnCond, SNode** pLeftChildCond,
                                         SNode** pRightChildCond) {
  SSHashObj* pLeftTables = NULL;
  SSHashObj* pRightTables = NULL;
  collectTableAliasFromNodes(nodesListGetNode(pJoin->node.pChildren, 0), &pLeftTables);
  collectTableAliasFromNodes(nodesListGetNode(pJoin->node.pChildren, 1), &pRightTables);
  
  ECondAction condAction =
      pushDownCondOptGetCondAction(pJoin->joinType, pLeftTables, pRightTables, pJoin->node.pConditions);

  tSimpleHashCleanup(pLeftTables);
  tSimpleHashCleanup(pRightTables);

  if (COND_ACTION_STAY == condAction) {
    return TSDB_CODE_SUCCESS;
  } else if (COND_ACTION_PUSH_JOIN == condAction) {
    *pOnCond = pJoin->node.pConditions;
  } else if (COND_ACTION_PUSH_LEFT_CHILD == condAction) {
    *pLeftChildCond = pJoin->node.pConditions;
  } else if (COND_ACTION_PUSH_RIGHT_CHILD == condAction) {
    *pRightChildCond = pJoin->node.pConditions;
  }
  pJoin->node.pConditions = NULL;
  return TSDB_CODE_SUCCESS;
}

static int32_t pushDownCondOptPartCond(SJoinLogicNode* pJoin, SNode** pOnCond, SNode** pLeftChildCond,
                                       SNode** pRightChildCond) {
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pJoin->node.pConditions)) {
    return pushDownCondOptPartLogicCond(pJoin, pOnCond, pLeftChildCond, pRightChildCond);
  } else {
    return pushDownCondOptPartOpCond(pJoin, pOnCond, pLeftChildCond, pRightChildCond);
  }
}

static int32_t pushDownCondOptPushCondToOnCond(SOptimizeContext* pCxt, SJoinLogicNode* pJoin, SNode** pCond) {
  return pushDownCondOptAppendCond(&pJoin->pOtherOnCond, pCond);
}

static int32_t pushDownCondOptPushCondToChild(SOptimizeContext* pCxt, SLogicNode* pChild, SNode** pCond) {
  return pushDownCondOptAppendCond(&pChild->pConditions, pCond);
}

static bool pushDownCondOptIsPriKey(SNode* pNode, SSHashObj* pTables) {
  if (QUERY_NODE_COLUMN != nodeType(pNode)) {
    return false;
  }
  SColumnNode* pCol = (SColumnNode*)pNode;
  if (PRIMARYKEY_TIMESTAMP_COL_ID != pCol->colId || TSDB_SYSTEM_TABLE == pCol->tableType) {
    return false;
  }
  return pushDownCondOptColInTableList(pNode, pTables);
}

static bool pushDownCondOptIsPriKeyEqualCond(SJoinLogicNode* pJoin, SNode* pCond) {
  if (QUERY_NODE_OPERATOR != nodeType(pCond)) {
    return false;
  }

  SOperatorNode* pOper = (SOperatorNode*)pCond;
  if (OP_TYPE_EQUAL != pOper->opType) {
    return false;
  }

  SSHashObj* pLeftTables = NULL;
  SSHashObj* pRightTables = NULL;
  collectTableAliasFromNodes(nodesListGetNode(pJoin->node.pChildren, 0), &pLeftTables);
  collectTableAliasFromNodes(nodesListGetNode(pJoin->node.pChildren, 1), &pRightTables);

  bool res = false;
  if (pushDownCondOptIsPriKey(pOper->pLeft, pLeftTables)) {
    res = pushDownCondOptIsPriKey(pOper->pRight, pRightTables);
  } else if (pushDownCondOptIsPriKey(pOper->pLeft, pRightTables)) {
    res = pushDownCondOptIsPriKey(pOper->pRight, pLeftTables);
  }

  tSimpleHashCleanup(pLeftTables);
  tSimpleHashCleanup(pRightTables);
  
  return res;
}

static bool pushDownCondOptContainPriKeyEqualCond(SJoinLogicNode* pJoin, SNode* pCond, bool* errCond) {
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pCond)) {
    SLogicConditionNode* pLogicCond = (SLogicConditionNode*)pCond;
    if (LOGIC_COND_TYPE_AND != pLogicCond->condType) {
      if (errCond) {
        *errCond = true;
      }
      return false;
    }
    bool   hasPrimaryKeyEqualCond = false;
    SNode* pCond = NULL;
    FOREACH(pCond, pLogicCond->pParameterList) {
      if (pushDownCondOptContainPriKeyEqualCond(pJoin, pCond, NULL)) {
        hasPrimaryKeyEqualCond = true;
        break;
      }
    }
    return hasPrimaryKeyEqualCond;
  } else {
    return pushDownCondOptIsPriKeyEqualCond(pJoin, pCond);
  }
}

static int32_t pushDownCondOptCheckJoinOnCond(SOptimizeContext* pCxt, SJoinLogicNode* pJoin) {
  if (NULL == pJoin->pOtherOnCond) {
    return generateUsageErrMsg(pCxt->pPlanCxt->pMsg, pCxt->pPlanCxt->msgLen, TSDB_CODE_PLAN_NOT_SUPPORT_CROSS_JOIN);
  }
  bool errCond = false;
  if (!pushDownCondOptContainPriKeyEqualCond(pJoin, pJoin->pOtherOnCond, &errCond)) {
    if (errCond) {
      return generateUsageErrMsg(pCxt->pPlanCxt->pMsg, pCxt->pPlanCxt->msgLen, TSDB_CODE_PLAN_NOT_SUPPORT_JOIN_COND);
    }
    return generateUsageErrMsg(pCxt->pPlanCxt->pMsg, pCxt->pPlanCxt->msgLen, TSDB_CODE_PLAN_EXPECTED_TS_EQUAL);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t pushDownCondOptPartJoinOnCondLogicCond(SJoinLogicNode* pJoin, SNode** ppPrimKeyEqCond, SNode** ppOnCond) {
  SLogicConditionNode* pLogicCond = (SLogicConditionNode*)(pJoin->pOtherOnCond);

  int32_t    code = TSDB_CODE_SUCCESS;
  SNodeList* pOnConds = NULL;
  SNode*     pCond = NULL;
  WHERE_EACH(pCond, pLogicCond->pParameterList) {
    if (pushDownCondOptIsPriKeyEqualCond(pJoin, pCond)) {
      nodesDestroyNode(*ppPrimKeyEqCond);
      *ppPrimKeyEqCond = nodesCloneNode(pCond);
      ERASE_NODE(pLogicCond->pParameterList);
    } else {
      code = nodesListMakeAppend(&pOnConds, nodesCloneNode(pCond));
      WHERE_NEXT;
    }
  }

  SNode* pTempOnCond = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesMergeConds(&pTempOnCond, &pOnConds);
  }

  if (TSDB_CODE_SUCCESS == code && NULL != *ppPrimKeyEqCond) {
    *ppOnCond = pTempOnCond;
    nodesDestroyNode(pJoin->pOtherOnCond);
    pJoin->pOtherOnCond = NULL;
    return TSDB_CODE_SUCCESS;
  } else {
    nodesDestroyList(pOnConds);
    nodesDestroyNode(pTempOnCond);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }
}

static int32_t pushDownCondOptPartJoinOnCond(SJoinLogicNode* pJoin, SNode** ppPrimKeyEqCond, SNode** ppOnCond) {
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pJoin->pOtherOnCond) &&
      LOGIC_COND_TYPE_AND == ((SLogicConditionNode*)(pJoin->pOtherOnCond))->condType) {
    return pushDownCondOptPartJoinOnCondLogicCond(pJoin, ppPrimKeyEqCond, ppOnCond);
  }

  if (pushDownCondOptIsPriKeyEqualCond(pJoin, pJoin->pOtherOnCond)) {
    *ppPrimKeyEqCond = pJoin->pOtherOnCond;
    *ppOnCond = NULL;
    pJoin->pOtherOnCond = NULL;
    return TSDB_CODE_SUCCESS;
  } else {
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }
}

static int32_t pushDownCondOptJoinExtractCond(SOptimizeContext* pCxt, SJoinLogicNode* pJoin) {
  int32_t code = pushDownCondOptCheckJoinOnCond(pCxt, pJoin);
  SNode*  pPrimKeyEqCond = NULL;
  SNode*  pJoinOnCond = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = pushDownCondOptPartJoinOnCond(pJoin, &pPrimKeyEqCond, &pJoinOnCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pJoin->pPrimKeyEqCond = pPrimKeyEqCond;
    pJoin->pOtherOnCond = pJoinOnCond;
  } else {
    nodesDestroyNode(pPrimKeyEqCond);
    nodesDestroyNode(pJoinOnCond);
  }
  return code;
}

static bool pushDownCondOptIsTableColumn(SNode* pNode, SNodeList* pTableCols) {
  if (QUERY_NODE_COLUMN != nodeType(pNode)) {
    return false;
  }
  SColumnNode* pCol = (SColumnNode*)pNode;
  return pushDownCondOptBelongThisTable(pNode, pTableCols);
}

static bool pushDownCondOptIsColEqualOnCond(SJoinLogicNode* pJoin, SNode* pCond, bool* allTags) {
  if (QUERY_NODE_OPERATOR != nodeType(pCond)) {
    return false;
  }
  SOperatorNode* pOper = (SOperatorNode*)pCond;
  if (QUERY_NODE_COLUMN != nodeType(pOper->pLeft) || NULL == pOper->pRight || QUERY_NODE_COLUMN != nodeType(pOper->pRight)) {
    return false;
  }
  SColumnNode* pLeft = (SColumnNode*)(pOper->pLeft);
  SColumnNode* pRight = (SColumnNode*)(pOper->pRight);

  *allTags = (COLUMN_TYPE_TAG == pLeft->colType) && (COLUMN_TYPE_TAG == pRight->colType);

  if (OP_TYPE_EQUAL != pOper->opType) {
    return false;
  }

  if (pLeft->node.resType.type != pRight->node.resType.type ||
      pLeft->node.resType.bytes != pRight->node.resType.bytes) {
    return false;
  }
  SNodeList* pLeftCols = ((SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0))->pTargets;
  SNodeList* pRightCols = ((SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1))->pTargets;
  bool isEqual = false;
  if (pushDownCondOptIsTableColumn(pOper->pLeft, pLeftCols)) {
    isEqual = pushDownCondOptIsTableColumn(pOper->pRight, pRightCols);
  } else if (pushDownCondOptIsTableColumn(pOper->pLeft, pRightCols)) {
    isEqual = pushDownCondOptIsTableColumn(pOper->pRight, pLeftCols);
  }
  if (isEqual) {
  }
  return isEqual;
}

static int32_t pushDownCondOptJoinExtractEqualOnLogicCond(SJoinLogicNode* pJoin) {
  SLogicConditionNode* pLogicCond = (SLogicConditionNode*)(pJoin->pOtherOnCond);

  int32_t    code = TSDB_CODE_SUCCESS;
  SNodeList* pColEqOnConds = NULL;
  SNodeList* pTagEqOnConds = NULL;
  SNodeList* pTagOnConds = NULL;
  SNode*     pCond = NULL;
  bool       allTags = false;
  FOREACH(pCond, pLogicCond->pParameterList) {
    allTags = false;
    if (pushDownCondOptIsColEqualOnCond(pJoin, pCond, &allTags)) {
      if (allTags) {
        code = nodesListMakeAppend(&pTagEqOnConds, nodesCloneNode(pCond));
      } else {
        code = nodesListMakeAppend(&pColEqOnConds, nodesCloneNode(pCond));
      }
    } else if (allTags) {
      code = nodesListMakeAppend(&pTagOnConds, nodesCloneNode(pCond));
    }
    if (code) {
      break;
    }
  }

  SNode* pTempTagEqCond = NULL;
  SNode* pTempColEqCond = NULL;
  SNode* pTempTagOnCond = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesMergeConds(&pTempColEqCond, &pColEqOnConds);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesMergeConds(&pTempTagEqCond, &pTagEqOnConds);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesMergeConds(&pTempTagOnCond, &pTagOnConds);
  }

  if (TSDB_CODE_SUCCESS == code) {
    pJoin->pColEqCond = pTempColEqCond;
    pJoin->pTagEqCond = pTempTagEqCond;
    pJoin->pTagOnCond = pTempTagOnCond;
    return TSDB_CODE_SUCCESS;
  } else {
    nodesDestroyList(pColEqOnConds);
    nodesDestroyList(pTagEqOnConds);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t pushDownCondOptJoinExtractEqualOnCond(SOptimizeContext* pCxt, SJoinLogicNode* pJoin) {
  if (NULL == pJoin->pOtherOnCond) {
    pJoin->pColEqCond = NULL;
    pJoin->pTagEqCond = NULL;
    return TSDB_CODE_SUCCESS;
  }
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pJoin->pOtherOnCond) &&
      LOGIC_COND_TYPE_AND == ((SLogicConditionNode*)(pJoin->pOtherOnCond))->condType) {
    return pushDownCondOptJoinExtractEqualOnLogicCond(pJoin);
  }

  bool allTags = false;
  if (pushDownCondOptIsColEqualOnCond(pJoin, pJoin->pOtherOnCond, &allTags)) {
    if (allTags) {
      pJoin->pTagEqCond = nodesCloneNode(pJoin->pOtherOnCond);
    } else {
      pJoin->pColEqCond = nodesCloneNode(pJoin->pOtherOnCond);
    }
  } else if (allTags) {
    pJoin->pTagOnCond = nodesCloneNode(pJoin->pOtherOnCond);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t pushDownCondOptAppendFilterCol(SOptimizeContext* pCxt, SJoinLogicNode* pJoin) {
  if (NULL == pJoin->pOtherOnCond) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  SNodeList* pCondCols = nodesMakeList();
  SNodeList* pTargets = NULL;
  if (NULL == pCondCols) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  } else {
    code = nodesCollectColumnsFromNode(pJoin->pOtherOnCond, NULL, COLLECT_COL_TYPE_ALL, &pCondCols);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExprs(pCondCols, &pTargets);
  }
  
  nodesDestroyList(pCondCols);
  
  if (TSDB_CODE_SUCCESS == code) {
    SNode* pNode = NULL;
    FOREACH(pNode, pTargets) {
      SNode* pTmp = NULL;
      bool found = false;
      FOREACH(pTmp, pJoin->node.pTargets) {
        if (nodesEqualNode(pTmp, pNode)) {
          found = true;
          break;
        }
      }
      if (!found) {
        nodesListStrictAppend(pJoin->node.pTargets, nodesCloneNode(pNode));
      }
    }
  }    

  nodesDestroyList(pTargets);

  return code;
}


static int32_t pushDownCondOptDealJoin(SOptimizeContext* pCxt, SJoinLogicNode* pJoin) {
  if (OPTIMIZE_FLAG_TEST_MASK(pJoin->node.optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE)) {
    return TSDB_CODE_SUCCESS;
  }
  if (pJoin->joinAlgo != JOIN_ALGO_UNKNOWN) {
    return TSDB_CODE_SUCCESS;
  }

  if (NULL == pJoin->node.pConditions) {
    int32_t code = pushDownCondOptJoinExtractCond(pCxt, pJoin);
    if (TSDB_CODE_SUCCESS == code) {
      code = pushDownCondOptJoinExtractEqualOnCond(pCxt, pJoin);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = pushDownCondOptAppendFilterCol(pCxt, pJoin);
    }
    if (TSDB_CODE_SUCCESS == code) {
      OPTIMIZE_FLAG_SET_MASK(pJoin->node.optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE);
      pCxt->optimized = true;
    }
    return code;
  }

  SNode*  pOnCond = NULL;
  SNode*  pLeftChildCond = NULL;
  SNode*  pRightChildCond = NULL;
  int32_t code = pushDownCondOptPartCond(pJoin, &pOnCond, &pLeftChildCond, &pRightChildCond);
  if (TSDB_CODE_SUCCESS == code && NULL != pOnCond) {
    code = pushDownCondOptPushCondToOnCond(pCxt, pJoin, &pOnCond);
  }
  if (TSDB_CODE_SUCCESS == code && NULL != pLeftChildCond) {
    code =
        pushDownCondOptPushCondToChild(pCxt, (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0), &pLeftChildCond);
  }
  if (TSDB_CODE_SUCCESS == code && NULL != pRightChildCond) {
    code =
        pushDownCondOptPushCondToChild(pCxt, (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1), &pRightChildCond);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = pushDownCondOptJoinExtractCond(pCxt, pJoin);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = pushDownCondOptJoinExtractEqualOnCond(pCxt, pJoin);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = pushDownCondOptAppendFilterCol(pCxt, pJoin);
  }

  if (TSDB_CODE_SUCCESS == code) {
    OPTIMIZE_FLAG_SET_MASK(pJoin->node.optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE);
    pCxt->optimized = true;
  } else {
    nodesDestroyNode(pOnCond);
    nodesDestroyNode(pLeftChildCond);
    nodesDestroyNode(pRightChildCond);
  }

  return code;
}

typedef struct SPartAggCondContext {
  SAggLogicNode* pAgg;
  bool           hasAggFunc;
} SPartAggCondContext;

static EDealRes partAggCondHasAggFuncImpl(SNode* pNode, void* pContext) {
  SPartAggCondContext* pCxt = pContext;
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    SNode* pAggFunc = NULL;
    FOREACH(pAggFunc, pCxt->pAgg->pAggFuncs) {
      if (strcmp(((SColumnNode*)pNode)->colName, ((SFunctionNode*)pAggFunc)->node.aliasName) == 0) {
        pCxt->hasAggFunc = true;
        return DEAL_RES_END;
      }
    }
  }
  return DEAL_RES_CONTINUE;
}

static int32_t partitionAggCondHasAggFunc(SAggLogicNode* pAgg, SNode* pCond) {
  SPartAggCondContext cxt = {.pAgg = pAgg, .hasAggFunc = false};
  nodesWalkExpr(pCond, partAggCondHasAggFuncImpl, &cxt);
  return cxt.hasAggFunc;
}

static int32_t partitionAggCondConj(SAggLogicNode* pAgg, SNode** ppAggFuncCond, SNode** ppGroupKeyCond) {
  SLogicConditionNode* pLogicCond = (SLogicConditionNode*)pAgg->node.pConditions;
  int32_t              code = TSDB_CODE_SUCCESS;

  SNodeList* pAggFuncConds = NULL;
  SNodeList* pGroupKeyConds = NULL;
  SNode*     pCond = NULL;
  FOREACH(pCond, pLogicCond->pParameterList) {
    if (partitionAggCondHasAggFunc(pAgg, pCond)) {
      code = nodesListMakeAppend(&pAggFuncConds, nodesCloneNode(pCond));
    } else {
      code = nodesListMakeAppend(&pGroupKeyConds, nodesCloneNode(pCond));
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }

  SNode* pTempAggFuncCond = NULL;
  SNode* pTempGroupKeyCond = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesMergeConds(&pTempAggFuncCond, &pAggFuncConds);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesMergeConds(&pTempGroupKeyCond, &pGroupKeyConds);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *ppAggFuncCond = pTempAggFuncCond;
    *ppGroupKeyCond = pTempGroupKeyCond;
  } else {
    nodesDestroyList(pAggFuncConds);
    nodesDestroyList(pGroupKeyConds);
    nodesDestroyNode(pTempAggFuncCond);
    nodesDestroyNode(pTempGroupKeyCond);
  }
  nodesDestroyNode(pAgg->node.pConditions);
  pAgg->node.pConditions = NULL;
  return code;
}

static int32_t partitionAggCond(SAggLogicNode* pAgg, SNode** ppAggFunCond, SNode** ppGroupKeyCond) {
  SNode* pAggNodeCond = pAgg->node.pConditions;
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pAggNodeCond) &&
      LOGIC_COND_TYPE_AND == ((SLogicConditionNode*)(pAggNodeCond))->condType) {
    return partitionAggCondConj(pAgg, ppAggFunCond, ppGroupKeyCond);
  }
  if (partitionAggCondHasAggFunc(pAgg, pAggNodeCond)) {
    *ppAggFunCond = pAggNodeCond;
  } else {
    *ppGroupKeyCond = pAggNodeCond;
  }
  pAgg->node.pConditions = NULL;
  return TSDB_CODE_SUCCESS;
}

static int32_t pushCondToAggCond(SOptimizeContext* pCxt, SAggLogicNode* pAgg, SNode** pAggFuncCond) {
  return pushDownCondOptAppendCond(&pAgg->node.pConditions, pAggFuncCond);
}

typedef struct SRewriteAggGroupKeyCondContext {
  SAggLogicNode* pAgg;
  int32_t        errCode;
} SRewriteAggGroupKeyCondContext;

static EDealRes rewriteAggGroupKeyCondForPushDownImpl(SNode** pNode, void* pContext) {
  SRewriteAggGroupKeyCondContext* pCxt = pContext;
  SAggLogicNode*                  pAgg = pCxt->pAgg;
  if (QUERY_NODE_COLUMN == nodeType(*pNode)) {
    SNode* pGroupKey = NULL;
    FOREACH(pGroupKey, pAgg->pGroupKeys) {
      SNode* pGroup = NULL;
      FOREACH(pGroup, ((SGroupingSetNode*)pGroupKey)->pParameterList) {
        if (0 == strcmp(((SExprNode*)pGroup)->aliasName, ((SColumnNode*)(*pNode))->colName)) {
          SNode* pExpr = nodesCloneNode(pGroup);
          if (pExpr == NULL) {
            pCxt->errCode = TSDB_CODE_OUT_OF_MEMORY;
            return DEAL_RES_ERROR;
          }
          nodesDestroyNode(*pNode);
          *pNode = pExpr;
          return DEAL_RES_IGNORE_CHILD;
        }
      }
    }
  }
  return DEAL_RES_CONTINUE;
}

static int32_t rewriteAggGroupKeyCondForPushDown(SOptimizeContext* pCxt, SAggLogicNode* pAgg, SNode* pGroupKeyCond) {
  SRewriteAggGroupKeyCondContext cxt = {.pAgg = pAgg, .errCode = TSDB_CODE_SUCCESS};
  nodesRewriteExpr(&pGroupKeyCond, rewriteAggGroupKeyCondForPushDownImpl, &cxt);
  return cxt.errCode;
}

static int32_t pushDownCondOptDealAgg(SOptimizeContext* pCxt, SAggLogicNode* pAgg) {
  if (NULL == pAgg->node.pConditions ||
      OPTIMIZE_FLAG_TEST_MASK(pAgg->node.optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE)) {
    return TSDB_CODE_SUCCESS;
  }
  // TODO: remove it after full implementation of pushing down to child
  if (1 != LIST_LENGTH(pAgg->node.pChildren)) {
    return TSDB_CODE_SUCCESS;
  }

  SNode*  pAggFuncCond = NULL;
  SNode*  pGroupKeyCond = NULL;
  int32_t code = partitionAggCond(pAgg, &pAggFuncCond, &pGroupKeyCond);
  if (TSDB_CODE_SUCCESS == code && NULL != pAggFuncCond) {
    code = pushCondToAggCond(pCxt, pAgg, &pAggFuncCond);
  }
  if (TSDB_CODE_SUCCESS == code && NULL != pGroupKeyCond) {
    code = rewriteAggGroupKeyCondForPushDown(pCxt, pAgg, pGroupKeyCond);
  }
  if (TSDB_CODE_SUCCESS == code && NULL != pGroupKeyCond) {
    SLogicNode* pChild = (SLogicNode*)nodesListGetNode(pAgg->node.pChildren, 0);
    code = pushDownCondOptPushCondToChild(pCxt, pChild, &pGroupKeyCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    OPTIMIZE_FLAG_SET_MASK(pAgg->node.optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE);
    pCxt->optimized = true;
  } else {
    nodesDestroyNode(pGroupKeyCond);
    nodesDestroyNode(pAggFuncCond);
  }
  return code;
}

typedef struct SRewriteProjCondContext {
  SProjectLogicNode* pProj;
  int32_t            errCode;
} SRewriteProjCondContext;

static EDealRes rewriteProjectCondForPushDownImpl(SNode** ppNode, void* pContext) {
  SRewriteProjCondContext* pCxt = pContext;
  SProjectLogicNode*       pProj = pCxt->pProj;
  if (QUERY_NODE_COLUMN == nodeType(*ppNode)) {
    SNode* pTarget = NULL;
    FOREACH(pTarget, pProj->node.pTargets) {
      if (nodesEqualNode(pTarget, *ppNode)) {
        SNode* pProjection = NULL;
        FOREACH(pProjection, pProj->pProjections) {
          if (0 == strcmp(((SExprNode*)pProjection)->aliasName, ((SColumnNode*)(*ppNode))->colName)) {
            SNode* pExpr = nodesCloneNode(pProjection);
            if (pExpr == NULL) {
              pCxt->errCode = TSDB_CODE_OUT_OF_MEMORY;
              return DEAL_RES_ERROR;
            }
            nodesDestroyNode(*ppNode);
            *ppNode = pExpr;
            return DEAL_RES_IGNORE_CHILD;
          }  // end if expr alias name equal column name
        }    // end for each project
      }      // end if target node equals cond column node
    }        // end for each targets
  }
  return DEAL_RES_CONTINUE;
}

static int32_t rewriteProjectCondForPushDown(SOptimizeContext* pCxt, SProjectLogicNode* pProject,
                                             SNode** ppProjectCond) {
  SRewriteProjCondContext cxt = {.pProj = pProject, .errCode = TSDB_CODE_SUCCESS};
  SNode*                  pProjectCond = pProject->node.pConditions;
  nodesRewriteExpr(&pProjectCond, rewriteProjectCondForPushDownImpl, &cxt);
  *ppProjectCond = pProjectCond;
  pProject->node.pConditions = NULL;
  return cxt.errCode;
}

static int32_t pushDownCondOptDealProject(SOptimizeContext* pCxt, SProjectLogicNode* pProject) {
  if (NULL == pProject->node.pConditions ||
      OPTIMIZE_FLAG_TEST_MASK(pProject->node.optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE)) {
    return TSDB_CODE_SUCCESS;
  }
  // TODO: remove it after full implementation of pushing down to child
  if (1 != LIST_LENGTH(pProject->node.pChildren)) {
    return TSDB_CODE_SUCCESS;
  }

  if (NULL != pProject->node.pLimit || NULL != pProject->node.pSlimit) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pProjCond = NULL;
  code = rewriteProjectCondForPushDown(pCxt, pProject, &pProjCond);
  if (TSDB_CODE_SUCCESS == code) {
    SLogicNode* pChild = (SLogicNode*)nodesListGetNode(pProject->node.pChildren, 0);
    code = pushDownCondOptPushCondToChild(pCxt, pChild, &pProjCond);
  }

  if (TSDB_CODE_SUCCESS == code) {
    OPTIMIZE_FLAG_SET_MASK(pProject->node.optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE);
    pCxt->optimized = true;
  } else {
    nodesDestroyNode(pProjCond);
  }
  return code;
}

static int32_t pushDownCondOptTrivialPushDown(SOptimizeContext* pCxt, SLogicNode* pLogicNode) {
  if (NULL == pLogicNode->pConditions ||
      OPTIMIZE_FLAG_TEST_MASK(pLogicNode->optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE)) {
    return TSDB_CODE_SUCCESS;
  }
  SLogicNode* pChild = (SLogicNode*)nodesListGetNode(pLogicNode->pChildren, 0);
  int32_t     code = pushDownCondOptPushCondToChild(pCxt, pChild, &pLogicNode->pConditions);
  if (TSDB_CODE_SUCCESS == code) {
    OPTIMIZE_FLAG_SET_MASK(pLogicNode->optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE);
    pCxt->optimized = true;
  }
  return code;
}

static int32_t pushDownCondOptimizeImpl(SOptimizeContext* pCxt, SLogicNode* pLogicNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (nodeType(pLogicNode)) {
    case QUERY_NODE_LOGIC_PLAN_SCAN:
      code = pushDownCondOptDealScan(pCxt, (SScanLogicNode*)pLogicNode);
      break;
    case QUERY_NODE_LOGIC_PLAN_JOIN:
      code = pushDownCondOptDealJoin(pCxt, (SJoinLogicNode*)pLogicNode);
      break;
    case QUERY_NODE_LOGIC_PLAN_AGG:
      code = pushDownCondOptDealAgg(pCxt, (SAggLogicNode*)pLogicNode);
      break;
    case QUERY_NODE_LOGIC_PLAN_PROJECT:
      code = pushDownCondOptDealProject(pCxt, (SProjectLogicNode*)pLogicNode);
      break;
    case QUERY_NODE_LOGIC_PLAN_SORT:
    case QUERY_NODE_LOGIC_PLAN_PARTITION:
      code = pushDownCondOptTrivialPushDown(pCxt, pLogicNode);
      break;
    default:
      break;
  }
  if (TSDB_CODE_SUCCESS == code) {
    SNode* pChild = NULL;
    FOREACH(pChild, pLogicNode->pChildren) {
      code = pushDownCondOptimizeImpl(pCxt, (SLogicNode*)pChild);
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    }
  }
  return code;
}

static int32_t pushDownCondOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  return pushDownCondOptimizeImpl(pCxt, pLogicSubplan->pNode);
}

static bool sortPriKeyOptIsPriKeyOrderBy(SNodeList* pSortKeys) {
  if (1 != LIST_LENGTH(pSortKeys)) {
    return false;
  }
  SNode* pNode = ((SOrderByExprNode*)nodesListGetNode(pSortKeys, 0))->pExpr;
  return (QUERY_NODE_COLUMN == nodeType(pNode) ? (PRIMARYKEY_TIMESTAMP_COL_ID == ((SColumnNode*)pNode)->colId) : false);
}

static bool sortPriKeyOptMayBeOptimized(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_SORT != nodeType(pNode)) {
    return false;
  }
  SSortLogicNode* pSort = (SSortLogicNode*)pNode;
  if (pSort->skipPKSortOpt || !sortPriKeyOptIsPriKeyOrderBy(pSort->pSortKeys) ||
      1 != LIST_LENGTH(pSort->node.pChildren)) {
    return false;
  }
  SNode* pChild;
  FOREACH(pChild, pSort->node.pChildren) {
    SLogicNode* pSortDescendent = optFindPossibleNode((SLogicNode*)pChild, sortPriKeyOptMayBeOptimized);
    if (pSortDescendent != NULL) {
      return false;
    }
  }
  return true;
}

static int32_t sortPriKeyOptGetSequencingNodesImpl(SLogicNode* pNode, bool groupSort, EOrder sortOrder,
                                                   bool* pNotOptimize, SNodeList** pSequencingNodes) {
  if (NULL != pNode->pLimit || NULL != pNode->pSlimit) {
    *pNotOptimize = false;
    return TSDB_CODE_SUCCESS;
  }

  switch (nodeType(pNode)) {
    case QUERY_NODE_LOGIC_PLAN_SCAN: {
      SScanLogicNode* pScan = (SScanLogicNode*)pNode;
      if ((!groupSort && NULL != pScan->pGroupTags) || TSDB_SYSTEM_TABLE == pScan->tableType) {
        *pNotOptimize = true;
        return TSDB_CODE_SUCCESS;
      }
      return nodesListMakeAppend(pSequencingNodes, (SNode*)pNode);
    }
    case QUERY_NODE_LOGIC_PLAN_JOIN: {
      int32_t code = sortPriKeyOptGetSequencingNodesImpl((SLogicNode*)nodesListGetNode(pNode->pChildren, 0), groupSort,
                                                         sortOrder, pNotOptimize, pSequencingNodes);
      if (TSDB_CODE_SUCCESS == code) {
        code = sortPriKeyOptGetSequencingNodesImpl((SLogicNode*)nodesListGetNode(pNode->pChildren, 1), groupSort,
                                                   sortOrder, pNotOptimize, pSequencingNodes);
      }
      return code;
    }
    case QUERY_NODE_LOGIC_PLAN_WINDOW: {
      SWindowLogicNode* pWindowLogicNode = (SWindowLogicNode*)pNode;
      // For interval window, we always apply sortPriKey optimization.
      // For session/event/state window, the output ts order will always be ASC.
      // If sort order is also asc, we apply optimization, otherwise we keep sort node to get correct output order.
      if (pWindowLogicNode->winType == WINDOW_TYPE_INTERVAL || sortOrder == ORDER_ASC)
        return nodesListMakeAppend(pSequencingNodes, (SNode*)pNode);
    }
    case QUERY_NODE_LOGIC_PLAN_AGG:
    case QUERY_NODE_LOGIC_PLAN_PARTITION:
    case QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL:
      *pNotOptimize = true;
      return TSDB_CODE_SUCCESS;
    default:
      break;
  }

  if (1 != LIST_LENGTH(pNode->pChildren)) {
    *pNotOptimize = true;
    return TSDB_CODE_SUCCESS;
  }

  return sortPriKeyOptGetSequencingNodesImpl((SLogicNode*)nodesListGetNode(pNode->pChildren, 0), groupSort, sortOrder,
                                             pNotOptimize, pSequencingNodes);
}

static EOrder sortPriKeyOptGetPriKeyOrder(SSortLogicNode* pSort) {
  return ((SOrderByExprNode*)nodesListGetNode(pSort->pSortKeys, 0))->order;
}

static int32_t sortPriKeyOptGetSequencingNodes(SSortLogicNode* pSort, bool groupSort, SNodeList** pSequencingNodes) {
  bool    notOptimize = false;
  int32_t code =
      sortPriKeyOptGetSequencingNodesImpl((SLogicNode*)nodesListGetNode(pSort->node.pChildren, 0), groupSort,
                                          sortPriKeyOptGetPriKeyOrder(pSort), &notOptimize, pSequencingNodes);
  if (TSDB_CODE_SUCCESS != code || notOptimize) {
    NODES_CLEAR_LIST(*pSequencingNodes);
  }
  return code;
}

static int32_t sortPriKeyOptApply(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan, SSortLogicNode* pSort,
                                  SNodeList* pSequencingNodes) {
  EOrder order = sortPriKeyOptGetPriKeyOrder(pSort);
  SNode* pSequencingNode = NULL;
  FOREACH(pSequencingNode, pSequencingNodes) {
    if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pSequencingNode)) {
      SScanLogicNode* pScan = (SScanLogicNode*)pSequencingNode;
      if ((ORDER_DESC == order && pScan->scanSeq[0] > 0) || (ORDER_ASC == order && pScan->scanSeq[1] > 0)) {
        TSWAP(pScan->scanSeq[0], pScan->scanSeq[1]);
      }
      pScan->node.outputTsOrder = order;
      if (TSDB_SUPER_TABLE == pScan->tableType) {
        pScan->scanType = SCAN_TYPE_TABLE_MERGE;
        pScan->filesetDelimited = true;
        pScan->node.resultDataOrder = DATA_ORDER_LEVEL_GLOBAL;
        pScan->node.requireDataOrder = DATA_ORDER_LEVEL_GLOBAL;
      }
      pScan->sortPrimaryKey = true;
    } else if (QUERY_NODE_LOGIC_PLAN_WINDOW == nodeType(pSequencingNode)) {
      ((SLogicNode*)pSequencingNode)->outputTsOrder = order;
    }
    optSetParentOrder(((SLogicNode*)pSequencingNode)->pParent, order, (SLogicNode*)pSort);
  }

  SLogicNode* pChild = (SLogicNode*)nodesListGetNode(pSort->node.pChildren, 0);
  if (NULL == pSort->node.pParent) {
    TSWAP(pSort->node.pTargets, pChild->pTargets);
  }
  int32_t code = replaceLogicNode(pLogicSubplan, (SLogicNode*)pSort, pChild);
  if (TSDB_CODE_SUCCESS == code) {
    NODES_CLEAR_LIST(pSort->node.pChildren);
    nodesDestroyNode((SNode*)pSort);
  }
  pCxt->optimized = true;
  return code;
}

static int32_t sortPrimaryKeyOptimizeImpl(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan, SSortLogicNode* pSort) {
  SNodeList* pSequencingNodes = NULL;
  int32_t    code = sortPriKeyOptGetSequencingNodes(pSort, pSort->groupSort, &pSequencingNodes);
  if (TSDB_CODE_SUCCESS == code) {
    if (pSequencingNodes != NULL) {
      code = sortPriKeyOptApply(pCxt, pLogicSubplan, pSort, pSequencingNodes);
    } else {
      // if we decided not to push down sort info to children, we should propagate output ts order to parents of pSort
      optSetParentOrder(pSort->node.pParent, sortPriKeyOptGetPriKeyOrder(pSort), 0);
      // we need to prevent this pSort from being chosen to do optimization again
      pSort->skipPKSortOpt = true;
      pCxt->optimized = true;
    }
  }
  nodesClearList(pSequencingNodes);
  return code;
}

static int32_t sortPrimaryKeyOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SSortLogicNode* pSort = (SSortLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, sortPriKeyOptMayBeOptimized);
  if (NULL == pSort) {
    return TSDB_CODE_SUCCESS;
  }
  return sortPrimaryKeyOptimizeImpl(pCxt, pLogicSubplan, pSort);
}

static int32_t sortForJoinOptimizeImpl(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan, SJoinLogicNode* pJoin) {
  SLogicNode* pLeft = (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0);
  SLogicNode* pRight = (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1);
  SScanLogicNode* pScan = NULL;
  SLogicNode* pChild = NULL;
  SNode** pChildPos = NULL;
  EOrder targetOrder = 0;
  SSHashObj* pTables = NULL;
  
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pLeft) && ((SScanLogicNode*)pLeft)->node.outputTsOrder != SCAN_ORDER_BOTH) {
    pScan = (SScanLogicNode*)pLeft;
    pChild = pRight;
    pChildPos = &pJoin->node.pChildren->pTail->pNode;
    targetOrder = pScan->node.outputTsOrder;
  } else if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pRight) && ((SScanLogicNode*)pRight)->node.outputTsOrder != SCAN_ORDER_BOTH) {
    pScan = (SScanLogicNode*)pRight;
    pChild = pLeft;
    pChildPos = &pJoin->node.pChildren->pHead->pNode;
    targetOrder = pScan->node.outputTsOrder;
  } else {
    pChild = pRight;
    pChildPos = &pJoin->node.pChildren->pTail->pNode;
    targetOrder = pLeft->outputTsOrder;
  }

  if (QUERY_NODE_OPERATOR != nodeType(pJoin->pPrimKeyEqCond)) {
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  bool res = false;
  SOperatorNode* pOp = (SOperatorNode*)pJoin->pPrimKeyEqCond;
  if (QUERY_NODE_COLUMN != nodeType(pOp->pLeft) || QUERY_NODE_COLUMN != nodeType(pOp->pRight)) {
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  SNode* pOrderByNode = NULL;

  collectTableAliasFromNodes((SNode*)pChild, &pTables);  
  if (NULL != tSimpleHashGet(pTables, ((SColumnNode*)pOp->pLeft)->tableAlias, strlen(((SColumnNode*)pOp->pLeft)->tableAlias))) {
    pOrderByNode = pOp->pLeft;
  } else if (NULL != tSimpleHashGet(pTables, ((SColumnNode*)pOp->pRight)->tableAlias, strlen(((SColumnNode*)pOp->pRight)->tableAlias))) {
    pOrderByNode = pOp->pRight;
  }

  tSimpleHashCleanup(pTables);

  if (NULL == pOrderByNode) {
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  SSortLogicNode* pSort = (SSortLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SORT);
  if (NULL == pSort) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pSort->node.outputTsOrder = targetOrder;
  pSort->node.pTargets = nodesCloneList(pChild->pTargets);
  if (NULL == pSort->node.pTargets) {
    nodesDestroyNode((SNode *)pSort);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  
  pSort->groupSort = false;
  SOrderByExprNode* pOrder = (SOrderByExprNode*)nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR);
  if (NULL == pOrder) {
    nodesDestroyNode((SNode *)pSort);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  nodesListMakeAppend(&pSort->pSortKeys, (SNode*)pOrder);
  pOrder->order = targetOrder;
  pOrder->pExpr = nodesCloneNode(pOrderByNode);
  pOrder->nullOrder = (ORDER_ASC == pOrder->order) ? NULL_ORDER_FIRST : NULL_ORDER_LAST;
  if (!pOrder->pExpr) {
    nodesDestroyNode((SNode *)pSort);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pChild->pParent = (SLogicNode*)pSort;
  nodesListMakeAppend(&pSort->node.pChildren, (SNode*)pChild);
  *pChildPos = (SNode*)pSort;
  pSort->node.pParent = (SLogicNode*)pJoin;;

_return:

  pCxt->optimized = true;

  return TSDB_CODE_SUCCESS;
}


static bool sortForJoinOptMayBeOptimized(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_JOIN != nodeType(pNode)) {
    return false;
  }
  
  SJoinLogicNode* pJoin = (SJoinLogicNode*)pNode;
  if (pNode->pChildren->length != 2 || !pJoin->hasSubQuery || pJoin->isLowLevelJoin) {
    return false;
  }

  SLogicNode* pLeft = (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0);
  SLogicNode* pRight = (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1);

  if (ORDER_ASC != pLeft->outputTsOrder && ORDER_DESC != pLeft->outputTsOrder) {
    return false;
  }
  if (ORDER_ASC != pRight->outputTsOrder && ORDER_DESC != pRight->outputTsOrder) {
    return false;
  }

  if (pLeft->outputTsOrder == pRight->outputTsOrder) {
    return false;
  }

  return true;
}


static int32_t sortForJoinOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SJoinLogicNode* pJoin = (SJoinLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, sortForJoinOptMayBeOptimized);
  if (NULL == pJoin) {
    return TSDB_CODE_SUCCESS;
  }
  return sortForJoinOptimizeImpl(pCxt, pLogicSubplan, pJoin);
}


static bool smaIndexOptMayBeOptimized(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pNode) || NULL == pNode->pParent ||
      QUERY_NODE_LOGIC_PLAN_WINDOW != nodeType(pNode->pParent) ||
      WINDOW_TYPE_INTERVAL != ((SWindowLogicNode*)pNode->pParent)->winType) {
    return false;
  }

  SScanLogicNode* pScan = (SScanLogicNode*)pNode;
  if (NULL == pScan->pSmaIndexes || NULL != pScan->node.pConditions) {
    return false;
  }

  return true;
}

static int32_t smaIndexOptCreateSmaScan(SScanLogicNode* pScan, STableIndexInfo* pIndex, SNodeList* pCols,
                                        SLogicNode** pOutput) {
  SScanLogicNode* pSmaScan = (SScanLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SCAN);
  if (NULL == pSmaScan) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pSmaScan->pScanCols = pCols;
  pSmaScan->tableType = TSDB_SUPER_TABLE;
  pSmaScan->tableId = pIndex->dstTbUid;
  pSmaScan->stableId = pIndex->dstTbUid;
  pSmaScan->scanType = SCAN_TYPE_TABLE;
  pSmaScan->scanSeq[0] = pScan->scanSeq[0];
  pSmaScan->scanSeq[1] = pScan->scanSeq[1];
  pSmaScan->scanRange = pScan->scanRange;
  pSmaScan->dataRequired = FUNC_DATA_REQUIRED_DATA_LOAD;

  pSmaScan->pVgroupList = taosMemoryCalloc(1, sizeof(SVgroupsInfo) + sizeof(SVgroupInfo));
  pSmaScan->node.pTargets = nodesCloneList(pCols);
  if (NULL == pSmaScan->pVgroupList || NULL == pSmaScan->node.pTargets) {
    nodesDestroyNode((SNode*)pSmaScan);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pSmaScan->pVgroupList->numOfVgroups = 1;
  pSmaScan->pVgroupList->vgroups[0].vgId = pIndex->dstVgId;
  memcpy(&(pSmaScan->pVgroupList->vgroups[0].epSet), &pIndex->epSet, sizeof(SEpSet));

  *pOutput = (SLogicNode*)pSmaScan;
  return TSDB_CODE_SUCCESS;
}

static bool smaIndexOptEqualInterval(SScanLogicNode* pScan, SWindowLogicNode* pWindow, STableIndexInfo* pIndex) {
  if (pWindow->interval != pIndex->interval || pWindow->intervalUnit != pIndex->intervalUnit ||
      pWindow->offset != pIndex->offset || pWindow->sliding != pIndex->sliding ||
      pWindow->slidingUnit != pIndex->slidingUnit) {
    return false;
  }
  if (IS_TSWINDOW_SPECIFIED(pScan->scanRange)) {
    SInterval interval = {.interval = pIndex->interval,
                          .intervalUnit = pIndex->intervalUnit,
                          .offset = pIndex->offset,
                          .offsetUnit = TIME_UNIT_MILLISECOND,
                          .sliding = pIndex->sliding,
                          .slidingUnit = pIndex->slidingUnit,
                          .precision = pScan->node.precision};
    return (pScan->scanRange.skey == taosTimeTruncate(pScan->scanRange.skey, &interval)) &&
           (pScan->scanRange.ekey + 1 == taosTimeTruncate(pScan->scanRange.ekey + 1, &interval));
  }
  return true;
}

static SNode* smaIndexOptCreateSmaCol(SNode* pFunc, uint64_t tableId, int32_t colId) {
  SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
  if (NULL == pCol) {
    return NULL;
  }
  pCol->tableId = tableId;
  pCol->tableType = TSDB_SUPER_TABLE;
  pCol->colId = colId;
  pCol->colType = COLUMN_TYPE_COLUMN;
  strcpy(pCol->colName, ((SExprNode*)pFunc)->aliasName);
  pCol->node.resType = ((SExprNode*)pFunc)->resType;
  strcpy(pCol->node.aliasName, ((SExprNode*)pFunc)->aliasName);
  return (SNode*)pCol;
}

static int32_t smaIndexOptFindSmaFunc(SNode* pQueryFunc, SNodeList* pSmaFuncs) {
  int32_t index = 0;
  SNode*  pSmaFunc = NULL;
  FOREACH(pSmaFunc, pSmaFuncs) {
    if (nodesEqualNode(pQueryFunc, pSmaFunc)) {
      return index;
    }
    ++index;
  }
  return -1;
}

static SNode* smaIndexOptFindWStartFunc(SNodeList* pSmaFuncs) {
  SNode* pSmaFunc = NULL;
  FOREACH(pSmaFunc, pSmaFuncs) {
    if (QUERY_NODE_FUNCTION == nodeType(pSmaFunc) && FUNCTION_TYPE_WSTART == ((SFunctionNode*)pSmaFunc)->funcType) {
      return pSmaFunc;
    }
  }
  return NULL;
}

static int32_t smaIndexOptCreateSmaCols(SNodeList* pFuncs, uint64_t tableId, SNodeList* pSmaFuncs,
                                        SNodeList** pOutput) {
  SNodeList* pCols = NULL;
  SNode*     pFunc = NULL;
  int32_t    code = TSDB_CODE_SUCCESS;
  int32_t    index = 0;
  int32_t    smaFuncIndex = -1;
  bool       hasWStart = false;
  FOREACH(pFunc, pFuncs) {
    smaFuncIndex = smaIndexOptFindSmaFunc(pFunc, pSmaFuncs);
    if (smaFuncIndex < 0) {
      break;
    } else {
      code = nodesListMakeStrictAppend(&pCols, smaIndexOptCreateSmaCol(pFunc, tableId, smaFuncIndex + 1));
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
      if (!hasWStart) {
        if (PRIMARYKEY_TIMESTAMP_COL_ID == ((SColumnNode*)pCols->pTail->pNode)->colId) {
          hasWStart = true;
        }
      }
    }
    ++index;
  }

  if (TSDB_CODE_SUCCESS == code && smaFuncIndex >= 0) {
    if (!hasWStart) {
      SNode* pWsNode = smaIndexOptFindWStartFunc(pSmaFuncs);
      if (!pWsNode) {
        nodesDestroyList(pCols);
        code = TSDB_CODE_APP_ERROR;
        qError("create sma cols failed since %s(_wstart not exist)", tstrerror(code));
        return code;
      }
      SExprNode exprNode;
      exprNode.resType = ((SExprNode*)pWsNode)->resType;
      sprintf(exprNode.aliasName, "#expr_%d", index + 1);
      SNode* pkNode = smaIndexOptCreateSmaCol((SNode*)&exprNode, tableId, PRIMARYKEY_TIMESTAMP_COL_ID);
      code = nodesListPushFront(pCols, pkNode);
      if (TSDB_CODE_SUCCESS != code) {
        nodesDestroyNode(pkNode);
        nodesDestroyList(pCols);
        return code;
      }
    }
    *pOutput = pCols;
  } else {
    nodesDestroyList(pCols);
  }

  return code;
}

static int32_t smaIndexOptCouldApplyIndex(SScanLogicNode* pScan, STableIndexInfo* pIndex, SNodeList** pCols) {
  SWindowLogicNode* pWindow = (SWindowLogicNode*)pScan->node.pParent;
  if (!smaIndexOptEqualInterval(pScan, pWindow, pIndex)) {
    return TSDB_CODE_SUCCESS;
  }
  SNodeList* pSmaFuncs = NULL;
  int32_t    code = nodesStringToList(pIndex->expr, &pSmaFuncs);
  if (TSDB_CODE_SUCCESS == code) {
    code = smaIndexOptCreateSmaCols(pWindow->pFuncs, pIndex->dstTbUid, pSmaFuncs, pCols);
  }
  nodesDestroyList(pSmaFuncs);
  return code;
}

static int32_t smaIndexOptApplyIndex(SLogicSubplan* pLogicSubplan, SScanLogicNode* pScan, STableIndexInfo* pIndex,
                                     SNodeList* pSmaCols) {
  SLogicNode* pSmaScan = NULL;
  int32_t     code = smaIndexOptCreateSmaScan(pScan, pIndex, pSmaCols, &pSmaScan);
  if (TSDB_CODE_SUCCESS == code) {
    code = replaceLogicNode(pLogicSubplan, pScan->node.pParent, pSmaScan);
  }
  if (TSDB_CODE_SUCCESS == code) {
    nodesDestroyNode((SNode*)pScan->node.pParent);
  }
  return code;
}

static int32_t smaIndexOptimizeImpl(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan, SScanLogicNode* pScan) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t nindexes = taosArrayGetSize(pScan->pSmaIndexes);
  for (int32_t i = 0; i < nindexes; ++i) {
    STableIndexInfo* pIndex = taosArrayGet(pScan->pSmaIndexes, i);
    SNodeList*       pSmaCols = NULL;
    code = smaIndexOptCouldApplyIndex(pScan, pIndex, &pSmaCols);
    if (TSDB_CODE_SUCCESS == code && NULL != pSmaCols) {
      code = smaIndexOptApplyIndex(pLogicSubplan, pScan, pIndex, pSmaCols);
      pCxt->optimized = true;
      break;
    }
  }
  return code;
}

static int32_t smaIndexOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SScanLogicNode* pScan = (SScanLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, smaIndexOptMayBeOptimized);
  if (NULL == pScan) {
    return TSDB_CODE_SUCCESS;
  }
  return smaIndexOptimizeImpl(pCxt, pLogicSubplan, pScan);
}

static EDealRes partTagsOptHasTbname(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    if (COLUMN_TYPE_TBNAME == ((SColumnNode*)pNode)->colType) {
      *(bool*)pContext = true;
      return DEAL_RES_END;
    }
  }
  return DEAL_RES_CONTINUE;
}

static bool planOptNodeListHasTbname(SNodeList* pKeys) {
  bool hasCol = false;
  nodesWalkExprs(pKeys, partTagsOptHasTbname, &hasCol);
  return hasCol;
}

static bool partTagsIsOptimizableNode(SLogicNode* pNode) {
  bool ret = 1 == LIST_LENGTH(pNode->pChildren) &&
             QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(nodesListGetNode(pNode->pChildren, 0)) &&
             SCAN_TYPE_TAG != ((SScanLogicNode*)nodesListGetNode(pNode->pChildren, 0))->scanType;
  if (!ret) return ret;
  switch (nodeType(pNode)) {
    case QUERY_NODE_LOGIC_PLAN_PARTITION: {
      if (pNode->pParent && nodeType(pNode->pParent) == QUERY_NODE_LOGIC_PLAN_WINDOW) {
        SWindowLogicNode* pWindow = (SWindowLogicNode*)pNode->pParent;
        if (pWindow->winType == WINDOW_TYPE_INTERVAL) {
          // if interval has slimit, we push down partition node to scan, and scan will set groupOrderScan to true
          //   we want to skip groups of blocks after slimit satisfied
          // if interval only has limit, we do not push down partition node to scan
          //   we want to get grouped output from partition node and make use of limit
          // if no slimit and no limit, we push down partition node and groupOrderScan is false, cause we do not need
          //   group ordered output
          if (!pWindow->node.pSlimit && pWindow->node.pLimit) ret = false;
        }
      }
    } break;
    case QUERY_NODE_LOGIC_PLAN_AGG: {
      SAggLogicNode* pAgg = (SAggLogicNode*)pNode;
      ret = pAgg->pGroupKeys && pAgg->pAggFuncs;
    } break;
    default:
      ret = false;
      break;
  }
  return ret;
}

static SNodeList* partTagsGetPartKeys(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_PARTITION == nodeType(pNode)) {
    return ((SPartitionLogicNode*)pNode)->pPartitionKeys;
  } else {
    return ((SAggLogicNode*)pNode)->pGroupKeys;
  }
}

static SNodeList* partTagsGetFuncs(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_PARTITION == nodeType(pNode)) {
    return NULL;
  } else {
    return ((SAggLogicNode*)pNode)->pAggFuncs;
  }
}

static bool partTagsOptAreSupportedFuncs(SNodeList* pFuncs) {
  SNode* pFunc = NULL;
  FOREACH(pFunc, pFuncs) {
    if (fmIsIndefiniteRowsFunc(((SFunctionNode*)pFunc)->funcId) && !fmIsSelectFunc(((SFunctionNode*)pFunc)->funcId)) {
      return false;
    }
  }
  return true;
}

static bool partTagsOptMayBeOptimized(SLogicNode* pNode) {
  if (!partTagsIsOptimizableNode(pNode)) {
    return false;
  }

  return !keysHasCol(partTagsGetPartKeys(pNode)) && partTagsOptAreSupportedFuncs(partTagsGetFuncs(pNode));
}

static int32_t partTagsOptRebuildTbanme(SNodeList* pPartKeys) {
  int32_t code = TSDB_CODE_SUCCESS;
  nodesRewriteExprs(pPartKeys, optRebuildTbanme, &code);
  return code;
}

// todo refact: just to mask compilation warnings
static void partTagsSetAlias(char* pAlias, const char* pTableAlias, const char* pColName) {
  char    name[TSDB_COL_FNAME_LEN + 1] = {0};
  int32_t len = snprintf(name, TSDB_COL_FNAME_LEN, "%s.%s", pTableAlias, pColName);

  taosCreateMD5Hash(name, len);
  strncpy(pAlias, name, TSDB_COL_NAME_LEN - 1);
}

static SNode* partTagsCreateWrapperFunc(const char* pFuncName, SNode* pNode) {
  SFunctionNode* pFunc = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  if (NULL == pFunc) {
    return NULL;
  }

  snprintf(pFunc->functionName, sizeof(pFunc->functionName), "%s", pFuncName);
  if (QUERY_NODE_COLUMN == nodeType(pNode) && COLUMN_TYPE_TBNAME != ((SColumnNode*)pNode)->colType) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    partTagsSetAlias(pFunc->node.aliasName, pCol->tableAlias, pCol->colName);
  } else {
    strcpy(pFunc->node.aliasName, ((SExprNode*)pNode)->aliasName);
  }
  int32_t code = nodesListMakeStrictAppend(&pFunc->pParameterList, nodesCloneNode(pNode));
  if (TSDB_CODE_SUCCESS == code) {
    code = fmGetFuncInfo(pFunc, NULL, 0);
  }

  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pFunc);
    return NULL;
  }

  return (SNode*)pFunc;
}

static bool partTagsHasIndefRowsSelectFunc(SNodeList* pFuncs) {
  SNode* pFunc = NULL;
  FOREACH(pFunc, pFuncs) {
    if (fmIsIndefiniteRowsFunc(((SFunctionNode*)pFunc)->funcId)) {
      return true;
    }
  }
  return false;
}

static bool partTagsNeedOutput(SNode* pExpr, SNodeList* pTargets) {
  SNode* pOutput = NULL;
  FOREACH(pOutput, pTargets) {
    if (QUERY_NODE_COLUMN == nodeType(pExpr)) {
      if (nodesEqualNode(pExpr, pOutput)) {
        return true;
      }
    } else if (0 == strcmp(((SExprNode*)pExpr)->aliasName, ((SColumnNode*)pOutput)->colName)) {
      return true;
    }
  }
  return false;
}

static int32_t partTagsRewriteGroupTagsToFuncs(SNodeList* pGroupTags, int32_t start, SAggLogicNode* pAgg) {
  bool    hasIndefRowsSelectFunc = partTagsHasIndefRowsSelectFunc(pAgg->pAggFuncs);
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t index = 0;
  SNode*  pNode = NULL;
  FOREACH(pNode, pGroupTags) {
    if (index++ < start || !partTagsNeedOutput(pNode, pAgg->node.pTargets)) {
      continue;
    }
    if (hasIndefRowsSelectFunc) {
      code = nodesListStrictAppend(pAgg->pAggFuncs, partTagsCreateWrapperFunc("_select_value", pNode));
    } else {
      code = nodesListStrictAppend(pAgg->pAggFuncs, partTagsCreateWrapperFunc("_group_key", pNode));
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }
  return code;
}

static int32_t partTagsOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SLogicNode* pNode = optFindPossibleNode(pLogicSubplan->pNode, partTagsOptMayBeOptimized);
  if (NULL == pNode) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t         code = TSDB_CODE_SUCCESS;
  SScanLogicNode* pScan = (SScanLogicNode*)nodesListGetNode(pNode->pChildren, 0);
  if (QUERY_NODE_LOGIC_PLAN_PARTITION == nodeType(pNode)) {
    TSWAP(((SPartitionLogicNode*)pNode)->pPartitionKeys, pScan->pGroupTags);
    TSWAP(((SPartitionLogicNode*)pNode)->pTags, pScan->pTags);
    TSWAP(((SPartitionLogicNode*)pNode)->pSubtable, pScan->pSubtable);
    int32_t code = replaceLogicNode(pLogicSubplan, pNode, (SLogicNode*)pScan);
    if (TSDB_CODE_SUCCESS == code) {
      code = adjustLogicNodeDataRequirement((SLogicNode*)pScan, pNode->resultDataOrder);
    }
    if (TSDB_CODE_SUCCESS == code) {
      if (QUERY_NODE_LOGIC_PLAN_AGG == pNode->pParent->type) {
        SAggLogicNode* pParent = (SAggLogicNode*)(pNode->pParent);
        scanPathOptSetGroupOrderScan(pScan);
        pParent->hasGroupKeyOptimized = true;
      }
      if (pNode->pParent->pSlimit)
        pScan->groupOrderScan = true;

      NODES_CLEAR_LIST(pNode->pChildren);
      nodesDestroyNode((SNode*)pNode);
    }
  } else {
    SAggLogicNode* pAgg = (SAggLogicNode*)pNode;
    int32_t        start = -1;
    SNode*         pGroupKey = NULL;
    FOREACH(pGroupKey, pAgg->pGroupKeys) {
      SNode* pGroupExpr = nodesListGetNode(((SGroupingSetNode*)pGroupKey)->pParameterList, 0);
      if (NULL != pScan->pGroupTags) {
        SNode* pGroupTag = NULL;
        FOREACH(pGroupTag, pScan->pGroupTags) {
          if (nodesEqualNode(pGroupTag, pGroupExpr)) {
            continue;
          }
        }
      }
      if (start < 0) {
        start = LIST_LENGTH(pScan->pGroupTags);
      }
      code = nodesListMakeStrictAppend(&pScan->pGroupTags, nodesCloneNode(pGroupExpr));
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    }
    pAgg->hasGroupKeyOptimized = true;

    NODES_DESTORY_LIST(pAgg->pGroupKeys);
    if (TSDB_CODE_SUCCESS == code && start >= 0) {
      code = partTagsRewriteGroupTagsToFuncs(pScan->pGroupTags, start, pAgg);
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = partTagsOptRebuildTbanme(pScan->pGroupTags);
  }

  pCxt->optimized = true;
  return code;
}

static bool eliminateProjOptCheckProjColumnNames(SProjectLogicNode* pProjectNode) {
  SHashObj* pProjColNameHash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  SNode*    pProjection;
  FOREACH(pProjection, pProjectNode->pProjections) {
    char*    projColumnName = ((SColumnNode*)pProjection)->colName;
    int32_t* pExist = taosHashGet(pProjColNameHash, projColumnName, strlen(projColumnName));
    if (NULL != pExist) {
      taosHashCleanup(pProjColNameHash);
      return false;
    } else {
      int32_t exist = 1;
      taosHashPut(pProjColNameHash, projColumnName, strlen(projColumnName), &exist, sizeof(exist));
    }
  }
  taosHashCleanup(pProjColNameHash);
  return true;
}

static bool eliminateProjOptMayBeOptimized(SLogicNode* pNode) {
  // TODO: enable this optimization after new mechanising that map projection and targets of project node
  if (NULL != pNode->pParent) {
    return false;
  }

  // Super table scan requires project operator to merge packets to improve performance.
  if (QUERY_NODE_LOGIC_PLAN_PROJECT != nodeType(pNode) || 1 != LIST_LENGTH(pNode->pChildren) ||
      (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(nodesListGetNode(pNode->pChildren, 0)) &&
       TSDB_SUPER_TABLE == ((SScanLogicNode*)nodesListGetNode(pNode->pChildren, 0))->tableType)) {
    return false;
  }

  SProjectLogicNode* pProjectNode = (SProjectLogicNode*)pNode;
  if (NULL != pProjectNode->node.pLimit || NULL != pProjectNode->node.pSlimit ||
      NULL != pProjectNode->node.pConditions) {
    return false;
  }

  SNode* pProjection;
  FOREACH(pProjection, pProjectNode->pProjections) {
    SExprNode* pExprNode = (SExprNode*)pProjection;
    if (QUERY_NODE_COLUMN != nodeType(pExprNode)) {
      return false;
    }
  }

  return eliminateProjOptCheckProjColumnNames(pProjectNode);
}

typedef struct CheckNewChildTargetsCxt {
  SNodeList* pNewChildTargets;
  bool       canUse;
} CheckNewChildTargetsCxt;

static EDealRes eliminateProjOptCanUseNewChildTargetsImpl(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    CheckNewChildTargetsCxt* pCxt = pContext;
    SNode*                   pTarget = NULL;
    FOREACH(pTarget, pCxt->pNewChildTargets) {
      if (nodesEqualNode(pTarget, pNode)) {
        pCxt->canUse = true;
        return DEAL_RES_CONTINUE;
      }
    }
    pCxt->canUse = false;
    return DEAL_RES_END;
  }
  return DEAL_RES_CONTINUE;
}

static bool eliminateProjOptCanChildConditionUseChildTargets(SLogicNode* pChild, SNodeList* pNewChildTargets) {
  if (NULL != pChild->pConditions) {
    CheckNewChildTargetsCxt cxt = {.pNewChildTargets = pNewChildTargets, .canUse = false};
    nodesWalkExpr(pChild->pConditions, eliminateProjOptCanUseNewChildTargetsImpl, &cxt);
    if (!cxt.canUse) return false;
  }
  if (QUERY_NODE_LOGIC_PLAN_JOIN == nodeType(pChild) && ((SJoinLogicNode*)pChild)->joinAlgo != JOIN_ALGO_UNKNOWN) {
    return false;
  }  
  if (QUERY_NODE_LOGIC_PLAN_JOIN == nodeType(pChild) && ((SJoinLogicNode*)pChild)->pOtherOnCond) {
    SJoinLogicNode*         pJoinLogicNode = (SJoinLogicNode*)pChild;
    CheckNewChildTargetsCxt cxt = {.pNewChildTargets = pNewChildTargets, .canUse = false};
    nodesWalkExpr(pJoinLogicNode->pOtherOnCond, eliminateProjOptCanUseNewChildTargetsImpl, &cxt);
    if (!cxt.canUse) return false;
  }
  return true;
}

static void alignProjectionWithTarget(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_PROJECT != pNode->type) {
    return;
  }

  SProjectLogicNode* pProjectNode = (SProjectLogicNode*)pNode;
  SNode*             pProjection = NULL;
  FOREACH(pProjection, pProjectNode->pProjections) {
    SNode* pTarget = NULL;
    bool   keep = false;
    FOREACH(pTarget, pNode->pTargets) {
      if (0 == strcmp(((SColumnNode*)pProjection)->node.aliasName, ((SColumnNode*)pTarget)->colName)) {
        keep = true;
        break;
      }
    }
    if (!keep) {
      nodesListErase(pProjectNode->pProjections, cell);
    }
  }
}

static int32_t eliminateProjOptimizeImpl(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan,
                                         SProjectLogicNode* pProjectNode) {
  SLogicNode* pChild = (SLogicNode*)nodesListGetNode(pProjectNode->node.pChildren, 0);
  SNodeList*  pNewChildTargets = nodesMakeList();

  SNode* pProjection = NULL;
  FOREACH(pProjection, pProjectNode->pProjections) {
    SNode* pChildTarget = NULL;
    FOREACH(pChildTarget, pChild->pTargets) {
      if (0 == strcmp(((SColumnNode*)pProjection)->colName, ((SColumnNode*)pChildTarget)->colName)) {
        nodesListAppend(pNewChildTargets, nodesCloneNode(pChildTarget));
        break;
      }
    }
  }
  if (eliminateProjOptCanChildConditionUseChildTargets(pChild, pNewChildTargets)) {
    nodesDestroyList(pChild->pTargets);
    pChild->pTargets = pNewChildTargets;
  } else {
    nodesDestroyList(pNewChildTargets);
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = replaceLogicNode(pLogicSubplan, (SLogicNode*)pProjectNode, pChild);
  if (pProjectNode->node.pHint && !pChild->pHint) TSWAP(pProjectNode->node.pHint, pChild->pHint);
  if (TSDB_CODE_SUCCESS == code) {
    NODES_CLEAR_LIST(pProjectNode->node.pChildren);
    nodesDestroyNode((SNode*)pProjectNode);
    // if pChild is a project logic node, remove its projection which is not reference by its target.
    alignProjectionWithTarget(pChild);
  }
  pCxt->optimized = true;
  return code;
}

static int32_t eliminateProjOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SProjectLogicNode* pProjectNode =
      (SProjectLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, eliminateProjOptMayBeOptimized);

  if (NULL == pProjectNode) {
    return TSDB_CODE_SUCCESS;
  }

  return eliminateProjOptimizeImpl(pCxt, pLogicSubplan, pProjectNode);
}

static bool rewriteTailOptMayBeOptimized(SLogicNode* pNode) {
  return QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC == nodeType(pNode) && ((SIndefRowsFuncLogicNode*)pNode)->isTailFunc;
}

static SNode* rewriteTailOptCreateOrderByExpr(SNode* pSortKey) {
  SOrderByExprNode* pOrder = (SOrderByExprNode*)nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR);
  if (NULL == pOrder) {
    return NULL;
  }
  pOrder->order = ORDER_DESC;
  pOrder->pExpr = nodesCloneNode(pSortKey);
  if (NULL == pOrder->pExpr) {
    nodesDestroyNode((SNode*)pOrder);
    return NULL;
  }
  return (SNode*)pOrder;
}

static int32_t rewriteTailOptCreateLimit(SNode* pLimit, SNode* pOffset, SNode** pOutput) {
  SLimitNode* pLimitNode = (SLimitNode*)nodesMakeNode(QUERY_NODE_LIMIT);
  if (NULL == pLimitNode) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pLimitNode->limit = NULL == pLimit ? -1 : ((SValueNode*)pLimit)->datum.i;
  pLimitNode->offset = NULL == pOffset ? 0 : ((SValueNode*)pOffset)->datum.i;
  *pOutput = (SNode*)pLimitNode;
  return TSDB_CODE_SUCCESS;
}

static bool rewriteTailOptNeedGroupSort(SIndefRowsFuncLogicNode* pIndef) {
  if (1 != LIST_LENGTH(pIndef->node.pChildren)) {
    return false;
  }
  SNode* pChild = nodesListGetNode(pIndef->node.pChildren, 0);
  return QUERY_NODE_LOGIC_PLAN_PARTITION == nodeType(pChild) ||
         (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pChild) && NULL != ((SScanLogicNode*)pChild)->pGroupTags);
}

static int32_t rewriteTailOptCreateSort(SIndefRowsFuncLogicNode* pIndef, SLogicNode** pOutput) {
  SSortLogicNode* pSort = (SSortLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SORT);
  if (NULL == pSort) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pSort->groupSort = rewriteTailOptNeedGroupSort(pIndef);
  TSWAP(pSort->node.pChildren, pIndef->node.pChildren);
  optResetParent((SLogicNode*)pSort);
  pSort->node.precision = pIndef->node.precision;

  SFunctionNode* pTail = NULL;
  SNode*         pFunc = NULL;
  FOREACH(pFunc, pIndef->pFuncs) {
    if (FUNCTION_TYPE_TAIL == ((SFunctionNode*)pFunc)->funcType) {
      pTail = (SFunctionNode*)pFunc;
      break;
    }
  }

  // tail(expr, [limit, offset,] _rowts)
  int32_t rowtsIndex = LIST_LENGTH(pTail->pParameterList) - 1;

  int32_t code = nodesListMakeStrictAppend(
      &pSort->pSortKeys, rewriteTailOptCreateOrderByExpr(nodesListGetNode(pTail->pParameterList, rowtsIndex)));
  if (TSDB_CODE_SUCCESS == code) {
    pSort->node.pTargets = nodesCloneList(((SLogicNode*)nodesListGetNode(pSort->node.pChildren, 0))->pTargets);
    if (NULL == pSort->node.pTargets) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pOutput = (SLogicNode*)pSort;
  } else {
    nodesDestroyNode((SNode*)pSort);
  }

  return code;
}

static SNode* rewriteTailOptCreateProjectExpr(SFunctionNode* pFunc) {
  SNode* pExpr = nodesCloneNode(nodesListGetNode(pFunc->pParameterList, 0));
  if (NULL == pExpr) {
    return NULL;
  }
  strcpy(((SExprNode*)pExpr)->aliasName, pFunc->node.aliasName);
  return pExpr;
}

static int32_t rewriteTailOptCreateProject(SIndefRowsFuncLogicNode* pIndef, SLogicNode** pOutput) {
  SProjectLogicNode* pProject = (SProjectLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_PROJECT);
  if (NULL == pProject) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  TSWAP(pProject->node.pTargets, pIndef->node.pTargets);
  pProject->node.precision = pIndef->node.precision;

  int32_t        code = TSDB_CODE_SUCCESS;
  SFunctionNode* pTail = NULL;
  SNode*         pFunc = NULL;
  FOREACH(pFunc, pIndef->pFuncs) {
    code = nodesListMakeStrictAppend(&pProject->pProjections, rewriteTailOptCreateProjectExpr((SFunctionNode*)pFunc));
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
    if (FUNCTION_TYPE_TAIL == ((SFunctionNode*)pFunc)->funcType) {
      pTail = (SFunctionNode*)pFunc;
    }
  }

  // tail(expr, [limit, offset,] _rowts)
  int32_t limitIndex = LIST_LENGTH(pTail->pParameterList) > 2 ? 1 : -1;
  int32_t offsetIndex = LIST_LENGTH(pTail->pParameterList) > 3 ? 2 : -1;
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteTailOptCreateLimit(limitIndex < 0 ? NULL : nodesListGetNode(pTail->pParameterList, limitIndex),
                                     offsetIndex < 0 ? NULL : nodesListGetNode(pTail->pParameterList, offsetIndex),
                                     &pProject->node.pLimit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    *pOutput = (SLogicNode*)pProject;
  } else {
    nodesDestroyNode((SNode*)pProject);
  }
  return code;
}

static int32_t rewriteTailOptimizeImpl(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan,
                                       SIndefRowsFuncLogicNode* pIndef) {
  SLogicNode* pSort = NULL;
  SLogicNode* pProject = NULL;
  int32_t     code = rewriteTailOptCreateSort(pIndef, &pSort);
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteTailOptCreateProject(pIndef, &pProject);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeAppend(&pProject->pChildren, (SNode*)pSort);
    pSort->pParent = pProject;
    pSort = NULL;
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = replaceLogicNode(pLogicSubplan, (SLogicNode*)pIndef, pProject);
  }
  if (TSDB_CODE_SUCCESS == code) {
    nodesDestroyNode((SNode*)pIndef);
  } else {
    nodesDestroyNode((SNode*)pSort);
    nodesDestroyNode((SNode*)pProject);
  }
  pCxt->optimized = true;
  return code;
}

static int32_t rewriteTailOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SIndefRowsFuncLogicNode* pIndef =
      (SIndefRowsFuncLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, rewriteTailOptMayBeOptimized);

  if (NULL == pIndef) {
    return TSDB_CODE_SUCCESS;
  }

  return rewriteTailOptimizeImpl(pCxt, pLogicSubplan, pIndef);
}

static bool eliminateSetOpMayBeOptimized(SLogicNode* pNode) {
  SLogicNode* pParent = pNode->pParent;
  if (NULL == pParent ||
      QUERY_NODE_LOGIC_PLAN_AGG != nodeType(pParent) && QUERY_NODE_LOGIC_PLAN_PROJECT != nodeType(pParent) ||
      LIST_LENGTH(pParent->pChildren) < 2) {
    return false;
  }
  if (nodeType(pNode) != nodeType(pNode->pParent) || LIST_LENGTH(pNode->pChildren) < 2) {
    return false;
  }
  return true;
}

static int32_t eliminateSetOpOptimizeImpl(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan,
                                          SLogicNode* pSetOpNode) {
  SNode* pSibling;
  FOREACH(pSibling, pSetOpNode->pParent->pChildren) {
    if (nodesEqualNode(pSibling, (SNode*)pSetOpNode)) {
      SNode* pChild;
      FOREACH(pChild, pSetOpNode->pChildren) { ((SLogicNode*)pChild)->pParent = pSetOpNode->pParent; }
      INSERT_LIST(pSetOpNode->pParent->pChildren, pSetOpNode->pChildren);

      pSetOpNode->pChildren = NULL;
      ERASE_NODE(pSetOpNode->pParent->pChildren);
      pCxt->optimized = true;
      return TSDB_CODE_SUCCESS;
    }
  }

  return TSDB_CODE_PLAN_INTERNAL_ERROR;
}

static int32_t eliminateSetOpOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SLogicNode* pSetOpNode = optFindPossibleNode(pLogicSubplan->pNode, eliminateSetOpMayBeOptimized);
  if (NULL == pSetOpNode) {
    return TSDB_CODE_SUCCESS;
  }

  return eliminateSetOpOptimizeImpl(pCxt, pLogicSubplan, pSetOpNode);
}

static bool rewriteUniqueOptMayBeOptimized(SLogicNode* pNode) {
  return QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC == nodeType(pNode) && ((SIndefRowsFuncLogicNode*)pNode)->isUniqueFunc;
}

static SNode* rewriteUniqueOptCreateGroupingSet(SNode* pExpr) {
  SGroupingSetNode* pGroupingSet = (SGroupingSetNode*)nodesMakeNode(QUERY_NODE_GROUPING_SET);
  if (NULL == pGroupingSet) {
    return NULL;
  }
  pGroupingSet->groupingSetType = GP_TYPE_NORMAL;
  SExprNode* pGroupExpr = (SExprNode*)nodesCloneNode(pExpr);
  if (TSDB_CODE_SUCCESS != nodesListMakeStrictAppend(&pGroupingSet->pParameterList, (SNode*)pGroupExpr)) {
    nodesDestroyNode((SNode*)pGroupingSet);
    return NULL;
  }
  return (SNode*)pGroupingSet;
}

static SNode* rewriteUniqueOptCreateFirstFunc(SFunctionNode* pSelectValue, SNode* pCol) {
  SFunctionNode* pFunc = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  if (NULL == pFunc) {
    return NULL;
  }

  strcpy(pFunc->functionName, "first");
  if (NULL != pSelectValue) {
    strcpy(pFunc->node.aliasName, pSelectValue->node.aliasName);
  } else {
    int64_t pointer = (int64_t)pFunc;
    char name[TSDB_FUNC_NAME_LEN + TSDB_POINTER_PRINT_BYTES + TSDB_NAME_DELIMITER_LEN + 1] = {0};
    int32_t len = snprintf(name, sizeof(name) - 1, "%s.%" PRId64 "", pFunc->functionName, pointer);
    taosCreateMD5Hash(name, len);
    strncpy(pFunc->node.aliasName, name, TSDB_COL_NAME_LEN - 1);
  }
  int32_t code = nodesListMakeStrictAppend(&pFunc->pParameterList, nodesCloneNode(pCol));
  if (TSDB_CODE_SUCCESS == code) {
    code = fmGetFuncInfo(pFunc, NULL, 0);
  }

  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pFunc);
    return NULL;
  }

  return (SNode*)pFunc;
}

static int32_t rewriteUniqueOptCreateAgg(SIndefRowsFuncLogicNode* pIndef, SLogicNode** pOutput) {
  SAggLogicNode* pAgg = (SAggLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_AGG);
  if (NULL == pAgg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  TSWAP(pAgg->node.pChildren, pIndef->node.pChildren);
  optResetParent((SLogicNode*)pAgg);
  pAgg->node.precision = pIndef->node.precision;
  pAgg->node.requireDataOrder = DATA_ORDER_LEVEL_IN_BLOCK;  // first function requirement
  pAgg->node.resultDataOrder = DATA_ORDER_LEVEL_NONE;

  int32_t code = TSDB_CODE_SUCCESS;
  bool    hasSelectPrimaryKey = false;
  SNode*  pPrimaryKey = NULL;
  SNode*  pNode = NULL;
  FOREACH(pNode, pIndef->pFuncs) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    SNode*         pExpr = nodesListGetNode(pFunc->pParameterList, 0);
    if (FUNCTION_TYPE_UNIQUE == pFunc->funcType) {
      pPrimaryKey = nodesListGetNode(pFunc->pParameterList, 1);
      code = nodesListMakeStrictAppend(&pAgg->pGroupKeys, rewriteUniqueOptCreateGroupingSet(pExpr));
    } else if (PRIMARYKEY_TIMESTAMP_COL_ID == ((SColumnNode*)pExpr)->colId) {  // _select_value(ts) => first(ts)
      hasSelectPrimaryKey = true;
      code = nodesListMakeStrictAppend(&pAgg->pAggFuncs, rewriteUniqueOptCreateFirstFunc(pFunc, pExpr));
    } else {  // _select_value(other_col)
      code = nodesListMakeStrictAppend(&pAgg->pAggFuncs, nodesCloneNode(pNode));
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExprs(pAgg->pGroupKeys, &pAgg->node.pTargets);
  }
  if (TSDB_CODE_SUCCESS == code && NULL != pAgg->pAggFuncs) {
    code = createColumnByRewriteExprs(pAgg->pAggFuncs, &pAgg->node.pTargets);
  }

  if (TSDB_CODE_SUCCESS == code && !hasSelectPrimaryKey && NULL != pAgg->pAggFuncs) {
    code = nodesListMakeStrictAppend(&pAgg->pAggFuncs, rewriteUniqueOptCreateFirstFunc(NULL, pPrimaryKey));
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pOutput = (SLogicNode*)pAgg;
  } else {
    nodesDestroyNode((SNode*)pAgg);
  }
  return code;
}

static SNode* rewriteUniqueOptCreateProjectCol(SFunctionNode* pFunc) {
  SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
  if (NULL == pCol) {
    return NULL;
  }

  pCol->node.resType = pFunc->node.resType;
  if (FUNCTION_TYPE_UNIQUE == pFunc->funcType) {
    SExprNode* pExpr = (SExprNode*)nodesListGetNode(pFunc->pParameterList, 0);
    if (QUERY_NODE_COLUMN == nodeType(pExpr)) {
      strcpy(pCol->tableAlias, ((SColumnNode*)pExpr)->tableAlias);
      strcpy(pCol->colName, ((SColumnNode*)pExpr)->colName);
    } else {
      strcpy(pCol->colName, pExpr->aliasName);
    }
  } else {
    strcpy(pCol->colName, pFunc->node.aliasName);
  }
  strcpy(pCol->node.aliasName, pFunc->node.aliasName);

  return (SNode*)pCol;
}

static int32_t rewriteUniqueOptCreateProject(SIndefRowsFuncLogicNode* pIndef, SLogicNode** pOutput) {
  SProjectLogicNode* pProject = (SProjectLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_PROJECT);
  if (NULL == pProject) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  TSWAP(pProject->node.pTargets, pIndef->node.pTargets);
  pProject->node.precision = pIndef->node.precision;
  pProject->node.requireDataOrder = DATA_ORDER_LEVEL_NONE;
  pProject->node.resultDataOrder = DATA_ORDER_LEVEL_NONE;

  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pNode = NULL;
  FOREACH(pNode, pIndef->pFuncs) {
    code = nodesListMakeStrictAppend(&pProject->pProjections, rewriteUniqueOptCreateProjectCol((SFunctionNode*)pNode));
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pOutput = (SLogicNode*)pProject;
  } else {
    nodesDestroyNode((SNode*)pProject);
  }
  return code;
}

static int32_t rewriteUniqueOptimizeImpl(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan,
                                         SIndefRowsFuncLogicNode* pIndef) {
  SLogicNode* pAgg = NULL;
  SLogicNode* pProject = NULL;
  int32_t     code = rewriteUniqueOptCreateAgg(pIndef, &pAgg);
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteUniqueOptCreateProject(pIndef, &pProject);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeAppend(&pProject->pChildren, (SNode*)pAgg);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pAgg->pParent = pProject;
    pAgg = NULL;
    code = replaceLogicNode(pLogicSubplan, (SLogicNode*)pIndef, pProject);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = adjustLogicNodeDataRequirement(
        pProject, NULL == pProject->pParent ? DATA_ORDER_LEVEL_NONE : pProject->pParent->requireDataOrder);
    pProject = NULL;
  }
  if (TSDB_CODE_SUCCESS == code) {
    nodesDestroyNode((SNode*)pIndef);
  } else {
    nodesDestroyNode((SNode*)pAgg);
    nodesDestroyNode((SNode*)pProject);
  }
  pCxt->optimized = true;
  return code;
}

static int32_t rewriteUniqueOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SIndefRowsFuncLogicNode* pIndef =
      (SIndefRowsFuncLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, rewriteUniqueOptMayBeOptimized);

  if (NULL == pIndef) {
    return TSDB_CODE_SUCCESS;
  }

  return rewriteUniqueOptimizeImpl(pCxt, pLogicSubplan, pIndef);
}

typedef struct SLastRowScanOptLastParaCkCxt {
  bool hasTag;
  bool hasCol;
} SLastRowScanOptLastParaCkCxt;

static EDealRes lastRowScanOptLastParaIsTagImpl(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    SLastRowScanOptLastParaCkCxt* pCxt = pContext;
    if (COLUMN_TYPE_TAG == ((SColumnNode*)pNode)->colType || COLUMN_TYPE_TBNAME == ((SColumnNode*)pNode)->colType) {
      pCxt->hasTag = true;
    } else {
      pCxt->hasCol = true;
    }
    return DEAL_RES_END;
  }
  return DEAL_RES_CONTINUE;
}

static bool lastRowScanOptLastParaIsTag(SNode* pExpr) {
  SLastRowScanOptLastParaCkCxt cxt = {.hasTag = false, .hasCol = false};
  nodesWalkExpr(pExpr, lastRowScanOptLastParaIsTagImpl, &cxt);
  return cxt.hasTag && !cxt.hasCol;
}

static bool hasSuitableCache(int8_t cacheLastMode, bool hasLastRow, bool hasLast) {
  switch (cacheLastMode) {
    case TSDB_CACHE_MODEL_NONE:
      return false;
    case TSDB_CACHE_MODEL_LAST_ROW:
      return hasLastRow;
    case TSDB_CACHE_MODEL_LAST_VALUE:
      return hasLast;
    case TSDB_CACHE_MODEL_BOTH:
      return true;
    default:
      break;
  }
  return false;
}

/// @brief check if we can apply last row scan optimization
/// @param lastColNum how many distinct last col specified
/// @param lastColId only used when lastColNum equals 1, the col id of the only one last col
/// @param selectNonPKColNum num of normal cols
/// @param selectNonPKColId only used when selectNonPKColNum equals 1, the col id of the only one select col
static bool lastRowScanOptCheckColNum(int32_t lastColNum, col_id_t lastColId,
                                      int32_t selectNonPKColNum, col_id_t selectNonPKColId) {
  // multi select non pk col + last func: select c1, c2, last(c1)
  if (selectNonPKColNum > 1 && lastColNum > 0) return false;

  if (selectNonPKColNum == 1) {
    // select last(c1), last(c2), c1 ...
    // which is not possible currently
    if (lastColNum > 1) return false;

    // select last(c1), c2 ...
    if (lastColNum == 1 && lastColId != selectNonPKColId) return false;
  }
  return true;
}

static bool isNeedSplitCacheLastFunc(SFunctionNode* pFunc, SScanLogicNode* pScan) {
  int32_t funcType = pFunc->funcType;
  if ((FUNCTION_TYPE_LAST_ROW != funcType || (FUNCTION_TYPE_LAST_ROW == funcType && TSDB_CACHE_MODEL_LAST_VALUE == pScan->cacheLastMode)) &&
       (FUNCTION_TYPE_LAST != funcType || (FUNCTION_TYPE_LAST == funcType && (TSDB_CACHE_MODEL_LAST_ROW == pScan->cacheLastMode ||
         QUERY_NODE_OPERATOR == nodeType(nodesListGetNode(pFunc->pParameterList, 0)) || QUERY_NODE_VALUE == nodeType(nodesListGetNode(pFunc->pParameterList, 0))))) &&
        FUNCTION_TYPE_SELECT_VALUE != funcType && FUNCTION_TYPE_GROUP_KEY != funcType) {
    return true;
  }
  return false;
}

static bool lastRowScanOptCheckFuncList(SLogicNode* pNode, int8_t cacheLastModel, bool* hasOtherFunc) {
  bool     hasNonPKSelectFunc = false;
  SNode*   pFunc = NULL;
  int32_t  lastColNum = 0, selectNonPKColNum = 0;
  col_id_t lastColId = -1, selectNonPKColId = -1;
  SScanLogicNode* pScan = (SScanLogicNode*)nodesListGetNode(((SAggLogicNode*)pNode)->node.pChildren, 0);
  uint32_t needSplitFuncCount = 0;
  FOREACH(pFunc, ((SAggLogicNode*)pNode)->pAggFuncs) {
    SFunctionNode* pAggFunc = (SFunctionNode*)pFunc;
    SNode* pParam = nodesListGetNode(pAggFunc->pParameterList, 0);
    if (FUNCTION_TYPE_LAST == pAggFunc->funcType) {
      if (QUERY_NODE_COLUMN == nodeType(pParam)) {
        SColumnNode* pCol = (SColumnNode*)pParam;
        if (pCol->colType != COLUMN_TYPE_COLUMN) {
          return false;
        }
        if (lastColId != pCol->colId) {
          lastColId = pCol->colId;
          lastColNum++;
        }
      }
      else if (QUERY_NODE_VALUE == nodeType(pParam) || QUERY_NODE_OPERATOR == nodeType(pParam)) {
        needSplitFuncCount++;
        *hasOtherFunc = true;
      }
      if (!lastRowScanOptCheckColNum(lastColNum, lastColId, selectNonPKColNum, selectNonPKColId)) {
        return false;
      }
      if (TSDB_CACHE_MODEL_LAST_ROW == cacheLastModel) {
        needSplitFuncCount++;
        *hasOtherFunc = true;
      }
    } else if (FUNCTION_TYPE_SELECT_VALUE == pAggFunc->funcType) {
      if (QUERY_NODE_COLUMN == nodeType(pParam)) {
        SColumnNode* pCol = (SColumnNode*)pParam;
        if (COLUMN_TYPE_COLUMN == pCol->colType && PRIMARYKEY_TIMESTAMP_COL_ID != pCol->colId) {
          if (selectNonPKColId != pCol->colId) {
            selectNonPKColId = pCol->colId;
            selectNonPKColNum++;
          }
        } else {
          continue;
        }
      } else if (lastColNum > 0) {
        return false;
      }
      if (!lastRowScanOptCheckColNum(lastColNum, lastColId, selectNonPKColNum, selectNonPKColId))
        return false;
    } else if (FUNCTION_TYPE_GROUP_KEY == pAggFunc->funcType) {
      if (!lastRowScanOptLastParaIsTag(nodesListGetNode(pAggFunc->pParameterList, 0))) {
        return false;
      }
    } else if (FUNCTION_TYPE_LAST_ROW != pAggFunc->funcType) {
      *hasOtherFunc = true;
      needSplitFuncCount++;
    } else if (FUNCTION_TYPE_LAST_ROW == pAggFunc->funcType && TSDB_CACHE_MODEL_LAST_VALUE == cacheLastModel) {
      *hasOtherFunc = true;
      needSplitFuncCount++;
    }
  }
  if (needSplitFuncCount >= ((SAggLogicNode*)pNode)->pAggFuncs->length) {
    return false;
  }

  return true;
}

static bool lastRowScanOptCheckLastCache(SAggLogicNode* pAgg, SScanLogicNode* pScan) {
  if ((pAgg->hasLastRow == pAgg->hasLast && !pAgg->hasLastRow) || (!pAgg->hasLast && !pAgg->hasLastRow) || NULL != pAgg->pGroupKeys || NULL != pScan->node.pConditions ||
      !hasSuitableCache(pScan->cacheLastMode, pAgg->hasLastRow, pAgg->hasLast) ||
      IS_TSWINDOW_SPECIFIED(pScan->scanRange)) {
    return false;
  }

  return true;
}

static bool lastRowScanOptMayBeOptimized(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_AGG != nodeType(pNode) || 1 != LIST_LENGTH(pNode->pChildren) ||
      QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(nodesListGetNode(pNode->pChildren, 0))) {
    return false;
  }

  SAggLogicNode*  pAgg = (SAggLogicNode*)pNode;
  SScanLogicNode* pScan = (SScanLogicNode*)nodesListGetNode(pNode->pChildren, 0);
  if (!lastRowScanOptCheckLastCache(pAgg, pScan)) {
    return false;
  }
  
  bool hasOtherFunc = false;
  if (!lastRowScanOptCheckFuncList(pNode, pScan->cacheLastMode, &hasOtherFunc)) {
    return false;
  }

  if (hasOtherFunc) {
    return false;
  }

  return true;
}

typedef struct SLastRowScanOptSetColDataTypeCxt {
  bool       doAgg;
  SNodeList* pLastCols;
  SNodeList* pOtherCols;
  int32_t    funcType;
} SLastRowScanOptSetColDataTypeCxt;

static EDealRes lastRowScanOptSetColDataType(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    SLastRowScanOptSetColDataTypeCxt* pCxt = pContext;
    if (pCxt->doAgg) {
      nodesListMakeAppend(&pCxt->pLastCols, pNode);
      getLastCacheDataType(&(((SColumnNode*)pNode)->node.resType));
    } else {
      SNode* pCol = NULL;
      FOREACH(pCol, pCxt->pLastCols) {
        if (nodesEqualNode(pCol, pNode)) {
          getLastCacheDataType(&(((SColumnNode*)pNode)->node.resType));
          break;
        }
      }
    }
    return DEAL_RES_IGNORE_CHILD;
  }
  return DEAL_RES_CONTINUE;
}

static void lastRowScanOptSetLastTargets(SNodeList* pTargets, SNodeList* pLastCols, SNodeList* pLastRowCols, bool erase) {
  SNode* pTarget = NULL;
  WHERE_EACH(pTarget, pTargets) {
    bool   found = false;
    SNode* pCol = NULL;
    FOREACH(pCol, pLastCols) {
      if (nodesEqualNode(pCol, pTarget)) {
        getLastCacheDataType(&(((SColumnNode*)pTarget)->node.resType));
        found = true;
        break;
      }
    }
    if (!found && nodeListNodeEqual(pLastRowCols, pTarget)) {
      found = true;
    }

    if (!found && erase) {
      ERASE_NODE(pTargets);
      continue;
    }
    WHERE_NEXT;
  }
}

static void lastRowScanOptRemoveUslessTargets(SNodeList* pTargets, SNodeList* pList1, SNodeList* pList2, SNodeList* pList3) {
  SNode* pTarget = NULL;
  WHERE_EACH(pTarget, pTargets) {
    bool   found = false;
    SNode* pCol = NULL;
    FOREACH(pCol, pList1) {
      if (nodesEqualNode(pCol, pTarget)) {
        found = true;
        break;
      }
    }
    if (!found) {
      FOREACH(pCol, pList2) {
        if (nodesEqualNode(pCol, pTarget)) {
          found = true;
          break;
        }
      }
    }

    if (!found && nodeListNodeEqual(pList3, pTarget)) {
      found = true;
    }

    if (!found) {
      ERASE_NODE(pTargets);
      continue;
    }
    WHERE_NEXT;
  }
}

static int32_t lastRowScanBuildFuncTypes(SScanLogicNode* pScan, SColumnNode* pColNode, int32_t funcType) {
  SFunctParam* pFuncTypeParam = taosMemoryCalloc(1, sizeof(SFunctParam));
  if (NULL == pFuncTypeParam) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pFuncTypeParam->type = funcType;
  if (NULL == pScan->pFuncTypes) {
    pScan->pFuncTypes = taosArrayInit(pScan->pScanCols->length, sizeof(SFunctParam));
    if (NULL == pScan->pFuncTypes) {
      taosMemoryFree(pFuncTypeParam);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
 
  pFuncTypeParam->pCol = taosMemoryCalloc(1, sizeof(SColumn));
  if (NULL == pFuncTypeParam->pCol) {
    taosMemoryFree(pFuncTypeParam);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pFuncTypeParam->pCol->colId = pColNode->colId;
  strcpy(pFuncTypeParam->pCol->name, pColNode->colName);
  taosArrayPush(pScan->pFuncTypes, pFuncTypeParam);

  taosMemoryFree(pFuncTypeParam);
  return TSDB_CODE_SUCCESS;
}

static int32_t lastRowScanOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SAggLogicNode* pAgg = (SAggLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, lastRowScanOptMayBeOptimized);

  if (NULL == pAgg) {
    return TSDB_CODE_SUCCESS;
  }

  SLastRowScanOptSetColDataTypeCxt cxt = {.doAgg = true, .pLastCols = NULL, .pOtherCols = NULL};
  SNode*                           pNode = NULL;
  SColumnNode*                     pPKTsCol = NULL;
  SColumnNode*                     pNonPKCol = NULL;
  SScanLogicNode* pScan = (SScanLogicNode*)nodesListGetNode(pAgg->node.pChildren, 0);
  pScan->scanType = SCAN_TYPE_LAST_ROW;
  pScan->igLastNull = pAgg->hasLast ? true : false;
  SArray* isDuplicateCol = taosArrayInit(pScan->pScanCols->length, sizeof(bool));
  SNodeList* pLastRowCols = NULL;

  FOREACH(pNode, pAgg->pAggFuncs) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    int32_t        funcType = pFunc->funcType;
    SNode* pParamNode = nodesListGetNode(pFunc->pParameterList, 0);
    if (FUNCTION_TYPE_LAST_ROW == funcType || FUNCTION_TYPE_LAST == funcType) {
      int32_t len = snprintf(pFunc->functionName, sizeof(pFunc->functionName),
                             FUNCTION_TYPE_LAST_ROW == funcType ? "_cache_last_row" : "_cache_last");
      pFunc->functionName[len] = '\0';
      int32_t code = fmGetFuncInfo(pFunc, NULL, 0);
      if (TSDB_CODE_SUCCESS != code) {
        nodesClearList(cxt.pLastCols);
        return code;
      }
      cxt.funcType = pFunc->funcType;
      // add duplicate cols which be removed for both last_row, last
      if (pAgg->hasLast && pAgg->hasLastRow) {
        if (QUERY_NODE_COLUMN == nodeType(pParamNode)) {
          SNode* pColNode = NULL;
          int i = 0;
          FOREACH(pColNode, pScan->pScanCols) {
            bool isDup = false;
            bool* isDuplicate = taosArrayGet(isDuplicateCol, i);
            if (NULL == isDuplicate) {
              taosArrayInsert(isDuplicateCol, i, &isDup);
              isDuplicate = taosArrayGet(isDuplicateCol, i);
            }
            i++;
            if (nodesEqualNode(pParamNode, pColNode)) {
              if (*isDuplicate) {
                if (0 == strncmp(((SColumnNode*)pColNode)->colName, "#dup_col.", 9)) {
                  continue;
                }
                SNode* newColNode = nodesCloneNode(pColNode);
                sprintf(((SColumnNode*)newColNode)->colName, "#dup_col.%p", newColNode);
                sprintf(((SColumnNode*)pParamNode)->colName, "#dup_col.%p", newColNode);

                nodesListAppend(pScan->pScanCols, newColNode);
                isDup = true;
                taosArrayInsert(isDuplicateCol, pScan->pScanCols->length, &isDup);
                nodesListAppend(pScan->node.pTargets, nodesCloneNode(newColNode));
                if (funcType != FUNCTION_TYPE_LAST) {
                  nodesListMakeAppend(&pLastRowCols, nodesCloneNode(newColNode));
                }

                lastRowScanBuildFuncTypes(pScan, (SColumnNode*)newColNode, pFunc->funcType);
              } else {
                isDup = true;
                *isDuplicate = isDup;
                if (funcType != FUNCTION_TYPE_LAST && !nodeListNodeEqual(cxt.pLastCols, pColNode)) {
                  nodesListMakeAppend(&pLastRowCols, nodesCloneNode(pColNode));
                }
                lastRowScanBuildFuncTypes(pScan, (SColumnNode*)pColNode, pFunc->funcType);
              }
              continue;
            }else if (nodeListNodeEqual(pFunc->pParameterList, pColNode)) {
              if (funcType != FUNCTION_TYPE_LAST && ((SColumnNode*)pColNode)->colId == PRIMARYKEY_TIMESTAMP_COL_ID &&
                  !nodeListNodeEqual(pLastRowCols, pColNode)) {
                nodesListMakeAppend(&pLastRowCols, nodesCloneNode(pColNode));

                lastRowScanBuildFuncTypes(pScan, (SColumnNode*)pColNode, pFunc->funcType);
                isDup = true;
                *isDuplicate = isDup;
              }
            }
          }
        }
      }

      if (FUNCTION_TYPE_LAST == funcType) {
        nodesWalkExpr(nodesListGetNode(pFunc->pParameterList, 0), lastRowScanOptSetColDataType, &cxt);
        nodesListErase(pFunc->pParameterList, nodesListGetCell(pFunc->pParameterList, 1));
      }
    } else {
      pNode = nodesListGetNode(pFunc->pParameterList, 0);
      nodesListMakeAppend(&cxt.pOtherCols, pNode);
      
      if (FUNCTION_TYPE_SELECT_VALUE == funcType) {
        if (nodeType(pNode) == QUERY_NODE_COLUMN) {
          SColumnNode* pCol = (SColumnNode*)pNode;
          if (pCol->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
            pPKTsCol = pCol;
          } else {
            pNonPKCol = pCol;
          }
        }
      }
    }
  }

  if (NULL != cxt.pLastCols) {
    cxt.doAgg = false;
    cxt.funcType = FUNCTION_TYPE_CACHE_LAST;
    lastRowScanOptSetLastTargets(pScan->pScanCols, cxt.pLastCols, pLastRowCols, true);
    nodesWalkExprs(pScan->pScanPseudoCols, lastRowScanOptSetColDataType, &cxt);
    lastRowScanOptSetLastTargets(pScan->node.pTargets, cxt.pLastCols, pLastRowCols, false);
    lastRowScanOptRemoveUslessTargets(pScan->node.pTargets, cxt.pLastCols, cxt.pOtherCols, pLastRowCols);
    if (pPKTsCol && pScan->node.pTargets->length == 1) {
      // when select last(ts),ts from ..., we add another ts to targets
      sprintf(pPKTsCol->colName, "#sel_val.%p", pPKTsCol);
      nodesListAppend(pScan->node.pTargets, nodesCloneNode((SNode*)pPKTsCol));
    }
    if (pNonPKCol && cxt.pLastCols->length == 1 && nodesEqualNode((SNode*)pNonPKCol, nodesListGetNode(cxt.pLastCols, 0))) {
      // when select last(c1), c1 from ..., we add c1 to targets
      sprintf(pNonPKCol->colName, "#sel_val.%p", pNonPKCol);
      nodesListAppend(pScan->node.pTargets, nodesCloneNode((SNode*)pNonPKCol));
    }
    nodesClearList(cxt.pLastCols);
  }
  nodesClearList(cxt.pOtherCols);

  pAgg->hasLastRow = false;
  pAgg->hasLast = false;

  pCxt->optimized = true;
  taosArrayDestroy(isDuplicateCol);
  return TSDB_CODE_SUCCESS;
}


static bool splitCacheLastFuncOptMayBeOptimized(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_AGG != nodeType(pNode) || 1 != LIST_LENGTH(pNode->pChildren) ||
      QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(nodesListGetNode(pNode->pChildren, 0))) {
    return false;
  }

  SAggLogicNode*  pAgg = (SAggLogicNode*)pNode;
  SScanLogicNode* pScan = (SScanLogicNode*)nodesListGetNode(pNode->pChildren, 0);
  if (!lastRowScanOptCheckLastCache(pAgg, pScan)) {
    return false;
  }

  bool hasOtherFunc = false;
  if (!lastRowScanOptCheckFuncList(pNode, pScan->cacheLastMode, &hasOtherFunc)) {
    return false;
  }

  if (pAgg->hasGroup || !hasOtherFunc) {
    return false;
  }

  return true;
}

static int32_t splitCacheLastFuncOptCreateAggLogicNode(SAggLogicNode** pNewAgg, SAggLogicNode* pAgg, SNodeList* pFunc, SNodeList* pTargets) {
  SAggLogicNode* pNew = (SAggLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_AGG);
  if (NULL == pNew) {
    nodesDestroyList(pFunc);
    nodesDestroyList(pTargets);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pNew->hasLastRow = false;
  pNew->hasLast = false;
  SNode* pFuncNode = NULL;
  FOREACH(pFuncNode, pFunc) {
    SFunctionNode* pFunc = (SFunctionNode*)pFuncNode;
    if (FUNCTION_TYPE_LAST_ROW == pFunc->funcType) {
      pNew->hasLastRow = true;
    } else if (FUNCTION_TYPE_LAST == pFunc->funcType) {
      pNew->hasLast = true;
    }
  }

  pNew->hasTimeLineFunc = pAgg->hasTimeLineFunc;
  pNew->hasGroupKeyOptimized = false;
  pNew->onlyHasKeepOrderFunc = pAgg->onlyHasKeepOrderFunc;
  pNew->node.groupAction = pAgg->node.groupAction;
  pNew->node.requireDataOrder = pAgg->node.requireDataOrder;
  pNew->node.resultDataOrder = pAgg->node.resultDataOrder;
  pNew->node.pTargets = pTargets;
  pNew->pAggFuncs = pFunc;
  pNew->pGroupKeys = nodesCloneList(pAgg->pGroupKeys);
  pNew->node.pConditions = nodesCloneNode(pAgg->node.pConditions);
  pNew->isGroupTb = pAgg->isGroupTb;
  pNew->isPartTb = pAgg->isPartTb;
  pNew->hasGroup = pAgg->hasGroup;
  pNew->node.pChildren = nodesCloneList(pAgg->node.pChildren);

  int32_t code = 0;
  SNode* pNode = nodesListGetNode(pNew->node.pChildren, 0);
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    SScanLogicNode* pScan = (SScanLogicNode*)pNode;
    SNodeList* pOldScanCols = NULL;
    TSWAP(pScan->pScanCols, pOldScanCols);
    nodesDestroyList(pScan->pScanPseudoCols);
    pScan->pScanPseudoCols = NULL;
    nodesDestroyList(pScan->node.pTargets);
    pScan->node.pTargets = NULL;
    SNodeListNode* list = (SNodeListNode*)nodesMakeNode(QUERY_NODE_NODE_LIST);
    list->pNodeList = pFunc;
    code = nodesCollectColumnsFromNode((SNode*)list, NULL, COLLECT_COL_TYPE_COL, &pScan->pScanCols);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    nodesFree(list);
    bool found = false;
    FOREACH(pNode, pScan->pScanCols) {
      if (PRIMARYKEY_TIMESTAMP_COL_ID == ((SColumnNode*)pNode)->colId) {
        found = true;
        break;
      }
    }
    if (!found) {
      FOREACH(pNode, pOldScanCols) {
        if (PRIMARYKEY_TIMESTAMP_COL_ID == ((SColumnNode*)pNode)->colId) {
          nodesListMakeStrictAppend(&pScan->pScanCols, nodesCloneNode(pNode));
          break;
        }
      }
    }
    nodesDestroyList(pOldScanCols);
    code = createColumnByRewriteExprs(pScan->pScanCols, &pScan->node.pTargets);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    OPTIMIZE_FLAG_CLEAR_MASK(pScan->node.optimizedFlag, OPTIMIZE_FLAG_SCAN_PATH);
  }

  *pNewAgg = pNew;

  return TSDB_CODE_SUCCESS;
}

static int32_t splitCacheLastFuncOptModifyAggLogicNode(SAggLogicNode* pAgg) {
  pAgg->hasTimeLineFunc = false;
  pAgg->onlyHasKeepOrderFunc = true;

  return TSDB_CODE_SUCCESS;
}

static int32_t splitCacheLastFuncOptCreateMergeLogicNode(SMergeLogicNode** pNew, SAggLogicNode* pAgg1, SAggLogicNode* pAgg2) {
  SMergeLogicNode* pMerge = (SMergeLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_MERGE);
  if (NULL == pMerge) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pMerge->colsMerge = true;
  pMerge->numOfChannels = 2;
  pMerge->srcGroupId = -1;
  pMerge->node.precision = pAgg1->node.precision;

  SNode* pNewAgg1 = nodesCloneNode((SNode*)pAgg1);
  SNode* pNewAgg2 = nodesCloneNode((SNode*)pAgg2);
  if (NULL == pNewAgg1 || NULL == pNewAgg2) {
    nodesDestroyNode(pNewAgg1);
    nodesDestroyNode(pNewAgg2);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  
  ((SAggLogicNode*)pNewAgg1)->node.pParent = (SLogicNode*)pMerge;
  ((SAggLogicNode*)pNewAgg2)->node.pParent = (SLogicNode*)pMerge;

  SNode* pNode = NULL;
  FOREACH(pNode, ((SAggLogicNode*)pNewAgg1)->node.pChildren) {
    ((SLogicNode*)pNode)->pParent = (SLogicNode*)pNewAgg1;
  }
  FOREACH(pNode, ((SAggLogicNode*)pNewAgg2)->node.pChildren) {
    ((SLogicNode*)pNode)->pParent = (SLogicNode*)pNewAgg2;
  }

  int32_t code = nodesListMakeStrictAppendList(&pMerge->node.pTargets, nodesCloneList(pAgg1->node.pTargets));
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppendList(&pMerge->node.pTargets, nodesCloneList(pAgg2->node.pTargets));
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pMerge->node.pChildren, pNewAgg1);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pMerge->node.pChildren, pNewAgg2);
  }

  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pNewAgg1);
    nodesDestroyNode(pNewAgg2);
    nodesDestroyNode((SNode*)pMerge);
  } else {
    *pNew = pMerge;
  }
  
  return code;
}

static int32_t splitCacheLastFuncOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SAggLogicNode* pAgg = (SAggLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, splitCacheLastFuncOptMayBeOptimized);

  if (NULL == pAgg) {
    return TSDB_CODE_SUCCESS;
  }
  SScanLogicNode* pScan = (SScanLogicNode*)nodesListGetNode(pAgg->node.pChildren, 0);
  SNode* pNode = NULL;
  SNodeList* pAggFuncList = NULL;

  {
    bool hasLast = false;
    bool hasLastRow = false;
    WHERE_EACH(pNode, pAgg->pAggFuncs) {
      SFunctionNode* pFunc = (SFunctionNode*)pNode;
      int32_t        funcType = pFunc->funcType;

      if (isNeedSplitCacheLastFunc(pFunc, pScan)) {
        nodesListMakeStrictAppend(&pAggFuncList, nodesCloneNode(pNode));
        ERASE_NODE(pAgg->pAggFuncs);
        continue;
      }
      if (FUNCTION_TYPE_LAST_ROW == funcType ) {
        hasLastRow = true;
      } else if (FUNCTION_TYPE_LAST == funcType) {
        hasLast = true;
      }
      WHERE_NEXT;    
    }
    pAgg->hasLast = hasLast;
    pAgg->hasLastRow = hasLastRow;
  }

  if (NULL == pAggFuncList) {
    planError("empty agg func list while splite projections, funcNum:%d", pAgg->pAggFuncs->length);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  SNodeList* pTargets = NULL;
  {
    WHERE_EACH(pNode, pAgg->node.pTargets) {
      SColumnNode* pCol = (SColumnNode*)pNode;
      SNode* pFuncNode = NULL;
      bool found = false;
      FOREACH(pFuncNode, pAggFuncList) {
        SFunctionNode* pFunc = (SFunctionNode*)pFuncNode;
        if (0 == strcmp(pFunc->node.aliasName, pCol->colName)) {
          nodesListMakeStrictAppend(&pTargets, nodesCloneNode(pNode));
          found = true;
          break;
        }
      }
      if (found) {
        ERASE_NODE(pAgg->node.pTargets);
        continue;
      }
      WHERE_NEXT;    
    }
  }

  if (NULL == pTargets) {
    planError("empty target func list while splite projections, targetsNum:%d", pAgg->node.pTargets->length);
    nodesDestroyList(pAggFuncList);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  SMergeLogicNode* pMerge = NULL;
  SAggLogicNode* pNewAgg = NULL;
  int32_t code = splitCacheLastFuncOptCreateAggLogicNode(&pNewAgg, pAgg, pAggFuncList, pTargets);
  if (TSDB_CODE_SUCCESS == code) {
    code = splitCacheLastFuncOptModifyAggLogicNode(pAgg);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = splitCacheLastFuncOptCreateMergeLogicNode(&pMerge, pNewAgg, pAgg);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = replaceLogicNode(pLogicSubplan, (SLogicNode*)pAgg, (SLogicNode*)pMerge);
  }

  nodesDestroyNode((SNode *)pAgg);
  nodesDestroyNode((SNode *)pNewAgg);

  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode *)pMerge);
  }
  
  pCxt->optimized = true;
  return code;
}



// merge projects
static bool mergeProjectsMayBeOptimized(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_PROJECT != nodeType(pNode) || 1 != LIST_LENGTH(pNode->pChildren)) {
    return false;
  }
  SLogicNode* pChild = (SLogicNode*)nodesListGetNode(pNode->pChildren, 0);
  if (QUERY_NODE_LOGIC_PLAN_PROJECT != nodeType(pChild) || 1 < LIST_LENGTH(pChild->pChildren) ||
      NULL != pChild->pConditions || NULL != pChild->pLimit || NULL != pChild->pSlimit) {
    return false;
  }

  return true;
}

typedef struct SMergeProjectionsContext {
  SProjectLogicNode* pChildProj;
  int32_t            errCode;
} SMergeProjectionsContext;

static EDealRes mergeProjectionsExpr(SNode** pNode, void* pContext) {
  SMergeProjectionsContext* pCxt = pContext;
  SProjectLogicNode*        pChildProj = pCxt->pChildProj;
  if (QUERY_NODE_COLUMN == nodeType(*pNode)) {
    SNode* pTarget;
    FOREACH(pTarget, ((SLogicNode*)pChildProj)->pTargets) {
      if (nodesEqualNode(pTarget, *pNode)) {
        SNode* pProjection;
        FOREACH(pProjection, pChildProj->pProjections) {
          if (0 == strcmp(((SColumnNode*)pTarget)->colName, ((SExprNode*)pProjection)->aliasName)) {
            SNode* pExpr = nodesCloneNode(pProjection);
            if (pExpr == NULL) {
              pCxt->errCode = terrno;
              return DEAL_RES_ERROR;
            }
            snprintf(((SExprNode*)pExpr)->aliasName, sizeof(((SExprNode*)pExpr)->aliasName), "%s",
                     ((SExprNode*)*pNode)->aliasName);
            nodesDestroyNode(*pNode);
            *pNode = pExpr;
            return DEAL_RES_IGNORE_CHILD;
          }
        }
      }
    }
  }
  return DEAL_RES_CONTINUE;
}

static int32_t mergeProjectsOptimizeImpl(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan, SLogicNode* pSelfNode) {
  SLogicNode* pChild = (SLogicNode*)nodesListGetNode(pSelfNode->pChildren, 0);
  if (((SProjectLogicNode*)pChild)->ignoreGroupId) {
    ((SProjectLogicNode*)pSelfNode)->inputIgnoreGroup = true;
  }
  SMergeProjectionsContext cxt = {.pChildProj = (SProjectLogicNode*)pChild, .errCode = TSDB_CODE_SUCCESS};
  nodesRewriteExprs(((SProjectLogicNode*)pSelfNode)->pProjections, mergeProjectionsExpr, &cxt);
  int32_t code = cxt.errCode;

  if (TSDB_CODE_SUCCESS == code) {
    if (1 == LIST_LENGTH(pChild->pChildren)) {
      SLogicNode* pGrandChild = (SLogicNode*)nodesListGetNode(pChild->pChildren, 0);
      code = replaceLogicNode(pLogicSubplan, pChild, pGrandChild);
    } else {  // no grand child
      NODES_CLEAR_LIST(pSelfNode->pChildren);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    NODES_CLEAR_LIST(pChild->pChildren);
  }
  nodesDestroyNode((SNode*)pChild);
  pCxt->optimized = true;
  return code;
}

static int32_t mergeProjectsOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SLogicNode* pProjectNode = optFindPossibleNode(pLogicSubplan->pNode, mergeProjectsMayBeOptimized);
  if (NULL == pProjectNode) {
    return TSDB_CODE_SUCCESS;
  }

  return mergeProjectsOptimizeImpl(pCxt, pLogicSubplan, pProjectNode);
}

static bool tagScanOptShouldBeOptimized(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pNode) || (SCAN_TYPE_TAG == ((SScanLogicNode*)pNode)->scanType)) {
    return false;
  }
  SScanLogicNode* pScan = (SScanLogicNode*)pNode;
  if (pScan->hasNormalCols) {
    return false;
  }
  if (pScan->tableType == TSDB_SYSTEM_TABLE) {
    return false;
  }
  if (NULL == pNode->pParent || QUERY_NODE_LOGIC_PLAN_AGG != nodeType(pNode->pParent) ||
      1 != LIST_LENGTH(pNode->pParent->pChildren)) {
    return false;
  }

  SAggLogicNode* pAgg = (SAggLogicNode*)(pNode->pParent);
  if (NULL == pAgg->pGroupKeys || NULL != pAgg->pAggFuncs || keysHasCol(pAgg->pGroupKeys) ||
      !planOptNodeListHasTbname(pAgg->pGroupKeys)) {
    return false;
  }

  SNode* pGroupKey = NULL;
  FOREACH(pGroupKey, pAgg->pGroupKeys) {
    SNode* pGroup = NULL;
    FOREACH(pGroup, ((SGroupingSetNode*)pGroupKey)->pParameterList) {
      if (QUERY_NODE_COLUMN != nodeType(pGroup)) {
        return false;
      }
    }
  }
  return true;
}

static int32_t tagScanOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SScanLogicNode* pScanNode = (SScanLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, tagScanOptShouldBeOptimized);
  if (NULL == pScanNode) {
    return TSDB_CODE_SUCCESS;
  }

  pScanNode->scanType = SCAN_TYPE_TAG;
  SNode* pTarget = NULL;
  FOREACH(pTarget, pScanNode->node.pTargets) {
    if (PRIMARYKEY_TIMESTAMP_COL_ID == ((SColumnNode*)(pTarget))->colId) {
      ERASE_NODE(pScanNode->node.pTargets);
      break;
    }
  }

  NODES_DESTORY_LIST(pScanNode->pScanCols);

  SLogicNode* pAgg = pScanNode->node.pParent;
  if (NULL == pAgg->pParent) {
    SNodeList* pScanTargets = nodesMakeList();

    SNode* pAggTarget = NULL;
    FOREACH(pAggTarget, pAgg->pTargets) {
      SNode* pScanTarget = NULL;
      FOREACH(pScanTarget, pScanNode->node.pTargets) {
        if (0 == strcmp(((SColumnNode*)pAggTarget)->colName, ((SColumnNode*)pScanTarget)->colName)) {
          nodesListAppend(pScanTargets, nodesCloneNode(pScanTarget));
          break;
        }
      }
    }
    nodesDestroyList(pScanNode->node.pTargets);
    pScanNode->node.pTargets = pScanTargets;
  }

  pScanNode->onlyMetaCtbIdx = false;
  
  pCxt->optimized = true;
  return TSDB_CODE_SUCCESS;
}

static bool pushDownLimitOptShouldBeOptimized(SLogicNode* pNode) {
  if ((NULL == pNode->pLimit && pNode->pSlimit == NULL) || 1 != LIST_LENGTH(pNode->pChildren)) {
    return false;
  }

  SLogicNode* pChild = (SLogicNode*)nodesListGetNode(pNode->pChildren, 0);
  if (pChild->pLimit || pChild->pSlimit) return false;
  return true;
}

static void swapLimit(SLogicNode* pParent, SLogicNode* pChild) {
  pChild->pLimit = pParent->pLimit;
  pParent->pLimit = NULL;
}

static bool pushDownLimitHow(SLogicNode* pNodeWithLimit, SLogicNode* pNodeLimitPushTo);
static bool pushDownLimitTo(SLogicNode* pNodeWithLimit, SLogicNode* pNodeLimitPushTo) {
  switch (nodeType(pNodeLimitPushTo)) {
    case QUERY_NODE_LOGIC_PLAN_WINDOW: {
      SWindowLogicNode* pWindow = (SWindowLogicNode*)pNodeLimitPushTo;
      if (pWindow->winType != WINDOW_TYPE_INTERVAL) break;
      cloneLimit(pNodeWithLimit, pNodeLimitPushTo, CLONE_LIMIT_SLIMIT);
      return true;
    }
    case QUERY_NODE_LOGIC_PLAN_SORT:
      if (((SSortLogicNode*)pNodeLimitPushTo)->calcGroupId) break;
      // fall through
    case QUERY_NODE_LOGIC_PLAN_FILL:
      cloneLimit(pNodeWithLimit, pNodeLimitPushTo, CLONE_LIMIT_SLIMIT);
      SNode* pChild = NULL;
      FOREACH(pChild, pNodeLimitPushTo->pChildren) { pushDownLimitHow(pNodeLimitPushTo, (SLogicNode*)pChild); }
      return true;
    case QUERY_NODE_LOGIC_PLAN_AGG: {
      if (nodeType(pNodeWithLimit) == QUERY_NODE_LOGIC_PLAN_PROJECT &&
          (isPartTagAgg((SAggLogicNode*)pNodeLimitPushTo) || isPartTableAgg((SAggLogicNode*)pNodeLimitPushTo))) {
        // when part by tag/tbname, slimit will be cloned to agg, and it will be pipelined.
        // The scan below will do scanning with group order
        return cloneLimit(pNodeWithLimit, pNodeLimitPushTo, CLONE_SLIMIT);
      }
      // else if not part by tag and tbname, the partition node below indicates that results are sorted, the agg node can
      // be pipelined.
      if (nodeType(pNodeWithLimit) == QUERY_NODE_LOGIC_PLAN_PROJECT && LIST_LENGTH(pNodeLimitPushTo->pChildren) == 1) {
        SLogicNode* pChild = (SLogicNode*)nodesListGetNode(pNodeLimitPushTo->pChildren, 0);
        if (nodeType(pChild) == QUERY_NODE_LOGIC_PLAN_PARTITION) {
          pNodeLimitPushTo->forceCreateNonBlockingOptr = true;
          return cloneLimit(pNodeWithLimit, pNodeLimitPushTo, CLONE_SLIMIT);
        }
        // Currently, partColOpt is executed after pushDownLimitOpt, and partColOpt will replace partition node with
        // sort node.
        // To avoid dependencies between these two optimizations, we add sort node too.
        if (nodeType(pChild) == QUERY_NODE_LOGIC_PLAN_SORT && ((SSortLogicNode*)pChild)->calcGroupId) {
          pNodeLimitPushTo->forceCreateNonBlockingOptr = true;
          return cloneLimit(pNodeWithLimit, pNodeLimitPushTo, CLONE_SLIMIT);
        }
      }
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_SCAN:
      if (nodeType(pNodeWithLimit) == QUERY_NODE_LOGIC_PLAN_PROJECT && pNodeWithLimit->pLimit) {
        if (((SProjectLogicNode*)pNodeWithLimit)->inputIgnoreGroup) {
          cloneLimit(pNodeWithLimit, pNodeLimitPushTo, CLONE_LIMIT);
        } else {
          swapLimit(pNodeWithLimit, pNodeLimitPushTo);
        }
        return true;
      }
    default:
      break;
  }
  return false;
}

static bool pushDownLimitHow(SLogicNode* pNodeWithLimit, SLogicNode* pNodeLimitPushTo) {
  switch (nodeType(pNodeWithLimit)) {
    case QUERY_NODE_LOGIC_PLAN_PROJECT:
    case QUERY_NODE_LOGIC_PLAN_FILL:
      return pushDownLimitTo(pNodeWithLimit, pNodeLimitPushTo);
    case QUERY_NODE_LOGIC_PLAN_SORT: {
      SSortLogicNode* pSort = (SSortLogicNode*)pNodeWithLimit;
      if (sortPriKeyOptIsPriKeyOrderBy(pSort->pSortKeys)) return pushDownLimitTo(pNodeWithLimit, pNodeLimitPushTo);
    }
    default:
      break;
  }
  return false;
}

static int32_t pushDownLimitOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SLogicNode* pNode = optFindPossibleNode(pLogicSubplan->pNode, pushDownLimitOptShouldBeOptimized);
  if (NULL == pNode) {
    return TSDB_CODE_SUCCESS;
  }

  SLogicNode* pChild = (SLogicNode*)nodesListGetNode(pNode->pChildren, 0);
  nodesDestroyNode(pChild->pLimit);
  if (pushDownLimitHow(pNode, pChild)) {
    pCxt->optimized = true;
  }
  return TSDB_CODE_SUCCESS;
}

typedef struct STbCntScanOptInfo {
  SAggLogicNode*  pAgg;
  SScanLogicNode* pScan;
  SName           table;
} STbCntScanOptInfo;

static bool tbCntScanOptIsEligibleGroupKeys(SNodeList* pGroupKeys) {
  if (NULL == pGroupKeys) {
    return true;
  }

  SNode* pGroupKey = NULL;
  FOREACH(pGroupKey, pGroupKeys) {
    SNode* pKey = nodesListGetNode(((SGroupingSetNode*)pGroupKey)->pParameterList, 0);
    if (QUERY_NODE_COLUMN != nodeType(pKey)) {
      return false;
    }
    SColumnNode* pCol = (SColumnNode*)pKey;
    if (0 != strcmp(pCol->colName, "db_name") && 0 != strcmp(pCol->colName, "stable_name")) {
      return false;
    }
  }

  return true;
}

static bool tbCntScanOptNotNullableExpr(SNode* pNode) {
  if (QUERY_NODE_COLUMN != nodeType(pNode)) {
    return false;
  }
  const char* pColName = ((SColumnNode*)pNode)->colName;
  return 0 == strcmp(pColName, "*") || 0 == strcmp(pColName, "db_name") || 0 == strcmp(pColName, "stable_name") ||
         0 == strcmp(pColName, "table_name");
}

static bool tbCntScanOptIsEligibleAggFuncs(SNodeList* pAggFuncs) {
  SNode* pNode = NULL;
  FOREACH(pNode, pAggFuncs) {
    SFunctionNode* pFunc = (SFunctionNode*)nodesListGetNode(pAggFuncs, 0);
    if (FUNCTION_TYPE_COUNT != pFunc->funcType ||
        !tbCntScanOptNotNullableExpr(nodesListGetNode(pFunc->pParameterList, 0))) {
      return false;
    }
  }
  return LIST_LENGTH(pAggFuncs) > 0;
}

static bool tbCntScanOptIsEligibleAgg(SAggLogicNode* pAgg) {
  return tbCntScanOptIsEligibleGroupKeys(pAgg->pGroupKeys) && tbCntScanOptIsEligibleAggFuncs(pAgg->pAggFuncs);
}

static bool tbCntScanOptGetColValFromCond(SOperatorNode* pOper, SColumnNode** pCol, SValueNode** pVal) {
  if (OP_TYPE_EQUAL != pOper->opType) {
    return false;
  }

  *pCol = NULL;
  *pVal = NULL;
  if (QUERY_NODE_COLUMN == nodeType(pOper->pLeft)) {
    *pCol = (SColumnNode*)pOper->pLeft;
  } else if (QUERY_NODE_VALUE == nodeType(pOper->pLeft)) {
    *pVal = (SValueNode*)pOper->pLeft;
  }
  if (QUERY_NODE_COLUMN == nodeType(pOper->pRight)) {
    *pCol = (SColumnNode*)pOper->pRight;
  } else if (QUERY_NODE_VALUE == nodeType(pOper->pRight)) {
    *pVal = (SValueNode*)pOper->pRight;
  }

  return NULL != *pCol && NULL != *pVal;
}

static bool tbCntScanOptIsEligibleLogicCond(STbCntScanOptInfo* pInfo, SLogicConditionNode* pCond) {
  if (LOGIC_COND_TYPE_AND != pCond->condType) {
    return false;
  }

  bool         hasDbCond = false;
  bool         hasStbCond = false;
  SColumnNode* pCol = NULL;
  SValueNode*  pVal = NULL;
  SNode*       pNode = NULL;
  FOREACH(pNode, pCond->pParameterList) {
    if (QUERY_NODE_OPERATOR != nodeType(pNode) || !tbCntScanOptGetColValFromCond((SOperatorNode*)pNode, &pCol, &pVal)) {
      return false;
    }
    if (!hasDbCond && 0 == strcmp(pCol->colName, "db_name")) {
      hasDbCond = true;
      strcpy(pInfo->table.dbname, pVal->literal);
    } else if (!hasStbCond && 0 == strcmp(pCol->colName, "stable_name")) {
      hasStbCond = true;
      strcpy(pInfo->table.tname, pVal->literal);
    } else {
      return false;
    }
  }
  return hasDbCond;
}

static bool tbCntScanOptIsEligibleOpCond(SOperatorNode* pCond) {
  SColumnNode* pCol = NULL;
  SValueNode*  pVal = NULL;
  if (!tbCntScanOptGetColValFromCond(pCond, &pCol, &pVal)) {
    return false;
  }
  return 0 == strcmp(pCol->colName, "db_name");
}

static bool tbCntScanOptIsEligibleConds(STbCntScanOptInfo* pInfo, SNode* pConditions) {
  if (NULL == pConditions) {
    return true;
  }
  if (LIST_LENGTH(pInfo->pAgg->pGroupKeys) != 0) {
    return false;
  }
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pConditions)) {
    return tbCntScanOptIsEligibleLogicCond(pInfo, (SLogicConditionNode*)pConditions);
  }

  if (QUERY_NODE_OPERATOR == nodeType(pConditions)) {
    return tbCntScanOptIsEligibleOpCond((SOperatorNode*)pConditions);
  }

  return false;
}

static bool tbCntScanOptIsEligibleScan(STbCntScanOptInfo* pInfo) {
  if (0 != strcmp(pInfo->pScan->tableName.dbname, TSDB_INFORMATION_SCHEMA_DB) ||
      0 != strcmp(pInfo->pScan->tableName.tname, TSDB_INS_TABLE_TABLES) || NULL != pInfo->pScan->pGroupTags) {
    return false;
  }
  if (1 == pInfo->pScan->pVgroupList->numOfVgroups && MNODE_HANDLE == pInfo->pScan->pVgroupList->vgroups[0].vgId) {
    return false;
  }
  return tbCntScanOptIsEligibleConds(pInfo, pInfo->pScan->node.pConditions);
}

static bool tbCntScanOptShouldBeOptimized(SLogicNode* pNode, STbCntScanOptInfo* pInfo) {
  if (QUERY_NODE_LOGIC_PLAN_AGG != nodeType(pNode) || 1 != LIST_LENGTH(pNode->pChildren) ||
      QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(nodesListGetNode(pNode->pChildren, 0))) {
    return false;
  }

  pInfo->pAgg = (SAggLogicNode*)pNode;
  pInfo->pScan = (SScanLogicNode*)nodesListGetNode(pNode->pChildren, 0);
  return tbCntScanOptIsEligibleAgg(pInfo->pAgg) && tbCntScanOptIsEligibleScan(pInfo);
}

static SNode* tbCntScanOptCreateTableCountFunc() {
  SFunctionNode* pFunc = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  if (NULL == pFunc) {
    return NULL;
  }
  strcpy(pFunc->functionName, "_table_count");
  strcpy(pFunc->node.aliasName, "_table_count");
  if (TSDB_CODE_SUCCESS != fmGetFuncInfo(pFunc, NULL, 0)) {
    nodesDestroyNode((SNode*)pFunc);
    return NULL;
  }
  return (SNode*)pFunc;
}

static int32_t tbCntScanOptRewriteScan(STbCntScanOptInfo* pInfo) {
  pInfo->pScan->scanType = SCAN_TYPE_TABLE_COUNT;
  strcpy(pInfo->pScan->tableName.dbname, pInfo->table.dbname);
  strcpy(pInfo->pScan->tableName.tname, pInfo->table.tname);
  NODES_DESTORY_LIST(pInfo->pScan->node.pTargets);
  NODES_DESTORY_LIST(pInfo->pScan->pScanCols);
  NODES_DESTORY_NODE(pInfo->pScan->node.pConditions);
  NODES_DESTORY_LIST(pInfo->pScan->pScanPseudoCols);
  int32_t code = nodesListMakeStrictAppend(&pInfo->pScan->pScanPseudoCols, tbCntScanOptCreateTableCountFunc());
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExpr(nodesListGetNode(pInfo->pScan->pScanPseudoCols, 0), &pInfo->pScan->node.pTargets);
  }
  SNode* pGroupKey = NULL;
  FOREACH(pGroupKey, pInfo->pAgg->pGroupKeys) {
    SNode* pGroupCol = nodesListGetNode(((SGroupingSetNode*)pGroupKey)->pParameterList, 0);
    code = nodesListMakeStrictAppend(&pInfo->pScan->pGroupTags, nodesCloneNode(pGroupCol));
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListMakeStrictAppend(&pInfo->pScan->pScanCols, nodesCloneNode(pGroupCol));
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListMakeStrictAppend(&pInfo->pScan->node.pTargets, nodesCloneNode(pGroupCol));
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }
  return code;
}

static int32_t tbCntScanOptCreateSumFunc(SFunctionNode* pCntFunc, SNode* pParam, SNode** pOutput) {
  SFunctionNode* pFunc = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  if (NULL == pFunc) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  strcpy(pFunc->functionName, "sum");
  strcpy(pFunc->node.aliasName, pCntFunc->node.aliasName);
  int32_t code = createColumnByRewriteExpr(pParam, &pFunc->pParameterList);
  if (TSDB_CODE_SUCCESS == code) {
    code = fmGetFuncInfo(pFunc, NULL, 0);
  }
  if (TSDB_CODE_SUCCESS == code) {
    *pOutput = (SNode*)pFunc;
  } else {
    nodesDestroyNode((SNode*)pFunc);
  }
  return code;
}

static int32_t tbCntScanOptRewriteAgg(SAggLogicNode* pAgg) {
  SScanLogicNode* pScan = (SScanLogicNode*)nodesListGetNode(pAgg->node.pChildren, 0);
  SNode*          pSum = NULL;
  int32_t         code = tbCntScanOptCreateSumFunc((SFunctionNode*)nodesListGetNode(pAgg->pAggFuncs, 0),
                                                   nodesListGetNode(pScan->pScanPseudoCols, 0), &pSum);
  if (TSDB_CODE_SUCCESS == code) {
    NODES_DESTORY_LIST(pAgg->pAggFuncs);
    code = nodesListMakeStrictAppend(&pAgg->pAggFuncs, pSum);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = partTagsRewriteGroupTagsToFuncs(pScan->pGroupTags, 0, pAgg);
  }
  NODES_DESTORY_LIST(pAgg->pGroupKeys);
  return code;
}

static int32_t tableCountScanOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  STbCntScanOptInfo info = {0};
  if (!optFindEligibleNode(pLogicSubplan->pNode, (FShouldBeOptimized)tbCntScanOptShouldBeOptimized, &info)) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = tbCntScanOptRewriteScan(&info);
  if (TSDB_CODE_SUCCESS == code) {
    code = tbCntScanOptRewriteAgg(info.pAgg);
  }
  return code;
}

static SSortLogicNode* sortNonPriKeySatisfied(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_SORT != nodeType(pNode)) {
    return NULL;
  }
  SSortLogicNode* pSort = (SSortLogicNode*)pNode;
  if (sortPriKeyOptIsPriKeyOrderBy(pSort->pSortKeys)) {
    return NULL;
  }
  SNode *pSortKeyNode = NULL, *pSortKeyExpr = NULL;
  FOREACH(pSortKeyNode, pSort->pSortKeys) {
    pSortKeyExpr = ((SOrderByExprNode*)pSortKeyNode)->pExpr;
    switch (nodeType(pSortKeyExpr)) {
      case QUERY_NODE_COLUMN:
        break;
      case QUERY_NODE_VALUE:
        continue;
      default:
        return NULL;
    }
  }

  if (!pSortKeyExpr || ((SColumnNode*)pSortKeyExpr)->projIdx != 1 ||
      ((SColumnNode*)pSortKeyExpr)->node.resType.type != TSDB_DATA_TYPE_TIMESTAMP) {
    return NULL;
  }
  return pSort;
}

static bool sortNonPriKeyShouldOptimize(SLogicNode* pNode, void* pInfo) {
  SSortLogicNode* pSort = sortNonPriKeySatisfied(pNode);
  if (!pSort) return false;
  SNodeList* pSortNodeList = pInfo;
  nodesListAppend(pSortNodeList, (SNode*)pSort);
  return false;
}

static int32_t sortNonPriKeyOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SNodeList* pNodeList = nodesMakeList();
  optFindEligibleNode(pLogicSubplan->pNode, sortNonPriKeyShouldOptimize, pNodeList);
  SNode* pNode = NULL;
  FOREACH(pNode, pNodeList) {
    SSortLogicNode*   pSort = (SSortLogicNode*)pNode;
    SOrderByExprNode* pOrderByExpr = (SOrderByExprNode*)nodesListGetNode(pSort->pSortKeys, 0);
    pSort->node.outputTsOrder = pOrderByExpr->order;
    optSetParentOrder(pSort->node.pParent, pOrderByExpr->order, NULL);
  }
  pCxt->optimized = false;
  nodesClearList(pNodeList);
  return TSDB_CODE_SUCCESS;
}

static bool stbJoinOptShouldBeOptimized(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_JOIN != nodeType(pNode)) {
    return false;
  }

  SJoinLogicNode* pJoin = (SJoinLogicNode*)pNode;
  if (pJoin->isSingleTableJoin || NULL == pJoin->pTagEqCond || NULL != pJoin->pTagOnCond || pNode->pChildren->length != 2 
      || pJoin->hasSubQuery || pJoin->joinAlgo != JOIN_ALGO_UNKNOWN || pJoin->isLowLevelJoin) {
    if (pJoin->joinAlgo == JOIN_ALGO_UNKNOWN) {
      pJoin->joinAlgo = JOIN_ALGO_MERGE;
    }
    return false;
  }

  return true;
}


int32_t stbJoinOptAddFuncToScanNode(char* funcName, SScanLogicNode* pScan) {
  SFunctionNode* pUidFunc = createFunction(funcName, NULL);
  snprintf(pUidFunc->node.aliasName, sizeof(pUidFunc->node.aliasName), "%s.%p",
           pUidFunc->functionName, pUidFunc);
  int32_t code = nodesListStrictAppend(pScan->pScanPseudoCols, (SNode *)pUidFunc);
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExpr((SNode*)pUidFunc, &pScan->node.pTargets);
  }
  return code;
}


int32_t stbJoinOptRewriteToTagScan(SLogicNode* pJoin, SNode* pNode) {
  SScanLogicNode* pScan = (SScanLogicNode*)pNode;
  SJoinLogicNode* pJoinNode = (SJoinLogicNode*)pJoin;

  pScan->scanType = SCAN_TYPE_TAG;
  NODES_DESTORY_LIST(pScan->pScanCols);
  NODES_DESTORY_NODE(pScan->node.pConditions);
  pScan->node.requireDataOrder = DATA_ORDER_LEVEL_NONE;
  pScan->node.resultDataOrder = DATA_ORDER_LEVEL_NONE;
  pScan->onlyMetaCtbIdx = true;

  SNodeList* pTags = nodesMakeList();
  int32_t code = nodesCollectColumnsFromNode(pJoinNode->pTagEqCond, NULL, COLLECT_COL_TYPE_TAG, &pTags);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesCollectColumnsFromNode(pJoinNode->pTagOnCond, NULL, COLLECT_COL_TYPE_TAG, &pTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    SNode* pTarget = NULL;
    SNode* pTag = NULL;
    bool found = false;
    WHERE_EACH(pTarget, pScan->node.pTargets) {
      found = false;
      FOREACH(pTag, pTags) {
        if (nodesEqualNode(pTarget, pTag)) {
          found = true;
          break;
        }
      }
      if (!found) {
        ERASE_NODE(pScan->node.pTargets);
      } else {
        WHERE_NEXT;
      }
    }
  } 
  if (TSDB_CODE_SUCCESS == code) {
    code = stbJoinOptAddFuncToScanNode("_tbuid", pScan);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = stbJoinOptAddFuncToScanNode("_vgid", pScan);
  }

  if (code) {
    nodesDestroyList(pTags);
  }
  
  return code;
}

static int32_t stbJoinOptCreateTagScanNode(SLogicNode* pJoin, SNodeList** ppList) {
  SNodeList* pList = nodesCloneList(pJoin->pChildren);
  if (NULL == pList) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    code = stbJoinOptRewriteToTagScan(pJoin, pNode);
    if (code) {
      break;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    *ppList = pList;
  } else {
    nodesDestroyList(pList);
  }

  return code;
}

static int32_t stbJoinOptCreateTagHashJoinNode(SLogicNode* pOrig, SNodeList* pChildren, SLogicNode** ppLogic) {
  SJoinLogicNode* pOrigJoin = (SJoinLogicNode*)pOrig;
  SJoinLogicNode* pJoin = (SJoinLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_JOIN);
  if (NULL == pJoin) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pJoin->joinType = pOrigJoin->joinType;
  pJoin->joinAlgo = JOIN_ALGO_HASH;
  pJoin->isSingleTableJoin = pOrigJoin->isSingleTableJoin;
  pJoin->hasSubQuery = pOrigJoin->hasSubQuery;
  pJoin->node.inputTsOrder = pOrigJoin->node.inputTsOrder;
  pJoin->node.groupAction = pOrigJoin->node.groupAction;
  pJoin->node.requireDataOrder = DATA_ORDER_LEVEL_NONE;
  pJoin->node.resultDataOrder = DATA_ORDER_LEVEL_NONE;
  pJoin->pTagEqCond = nodesCloneNode(pOrigJoin->pTagEqCond);
  pJoin->pOtherOnCond = nodesCloneNode(pOrigJoin->pTagOnCond);
  
  int32_t code = TSDB_CODE_SUCCESS;
  pJoin->node.pChildren = pChildren;

  SNode* pNode = NULL;
  FOREACH(pNode, pChildren) {
    SScanLogicNode* pScan = (SScanLogicNode*)pNode;
    SNode* pCol = NULL;
    FOREACH(pCol, pScan->pScanPseudoCols) {
      if (QUERY_NODE_FUNCTION == nodeType(pCol) && (((SFunctionNode*)pCol)->funcType == FUNCTION_TYPE_TBUID || ((SFunctionNode*)pCol)->funcType == FUNCTION_TYPE_VGID)) {
        code = createColumnByRewriteExpr(pCol, &pJoin->node.pTargets);
        if (code) {
          break;
        }
      }
    }
    if (code) {
      break;
    }
    pScan->node.pParent = (SLogicNode*)pJoin;
  }

  if (TSDB_CODE_SUCCESS == code) {
    *ppLogic = (SLogicNode*)pJoin;
  } else {
    nodesDestroyNode((SNode*)pJoin);
  }

  return code;
}

static int32_t stbJoinOptCreateTableScanNodes(SLogicNode* pJoin, SNodeList** ppList, bool* srcScan) {
  SNodeList* pList = nodesCloneList(pJoin->pChildren);
  if (NULL == pList) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  int32_t i = 0;
  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    SScanLogicNode* pScan = (SScanLogicNode*)pNode;
    //code = stbJoinOptAddFuncToScanNode("_tbuid", pScan);
    //if (code) {
    //  break;
    //}

    pScan->node.dynamicOp = true;
    *(srcScan + i++) = pScan->pVgroupList->numOfVgroups <= 1;
    
    pScan->scanType = SCAN_TYPE_TABLE;
  }

  *ppList = pList;

  return code;
}

static int32_t stbJoinOptCreateGroupCacheNode(SLogicNode* pRoot, SNodeList* pChildren, SLogicNode** ppLogic) {
  int32_t code = TSDB_CODE_SUCCESS;
  SGroupCacheLogicNode* pGrpCache = (SGroupCacheLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_GROUP_CACHE);
  if (NULL == pGrpCache) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  
  //pGrpCache->node.dynamicOp = true;
  pGrpCache->grpColsMayBeNull = false;
  pGrpCache->grpByUid = true;
  pGrpCache->batchFetch = getBatchScanOptionFromHint(pRoot->pHint);
  pGrpCache->node.pChildren = pChildren;
  pGrpCache->node.pTargets = nodesMakeList();
  if (NULL == pGrpCache->node.pTargets) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }
  if (TSDB_CODE_SUCCESS == code) {
    SScanLogicNode* pScan = (SScanLogicNode*)nodesListGetNode(pChildren, 0);
    code = nodesListStrictAppendList(pGrpCache->node.pTargets, nodesCloneList(pScan->node.pTargets));
  }

  SScanLogicNode* pScan = (SScanLogicNode*)nodesListGetNode(pChildren, 0);
  SNode* pCol = NULL;
  FOREACH(pCol, pScan->pScanPseudoCols) {
    if (QUERY_NODE_FUNCTION == nodeType(pCol) && (((SFunctionNode*)pCol)->funcType == FUNCTION_TYPE_TBUID || ((SFunctionNode*)pCol)->funcType == FUNCTION_TYPE_VGID)) {
      code = createColumnByRewriteExpr(pCol, &pGrpCache->pGroupCols);
      if (code) {
        break;
      }
    }
  }

  bool hasCond = false;
  SNode* pNode = NULL;
  FOREACH(pNode, pChildren) {
    SScanLogicNode* pScan = (SScanLogicNode*)pNode;
    if (pScan->node.pConditions) {
      hasCond = true;
    }
    pScan->node.pParent = (SLogicNode*)pGrpCache;
  }
  pGrpCache->globalGrp = false;
  
  if (TSDB_CODE_SUCCESS == code) {
    *ppLogic = (SLogicNode*)pGrpCache;
  } else {
    nodesDestroyNode((SNode*)pGrpCache);
  }

  return code;
}

static void stbJoinOptRemoveTagEqCond(SJoinLogicNode* pJoin) {
  if (QUERY_NODE_OPERATOR == nodeType(pJoin->pOtherOnCond) && nodesEqualNode(pJoin->pOtherOnCond, pJoin->pTagEqCond)) {
    NODES_DESTORY_NODE(pJoin->pOtherOnCond);
    return;
  }
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pJoin->pOtherOnCond)) {
    SLogicConditionNode* pLogic = (SLogicConditionNode*)pJoin->pOtherOnCond;
    SNode* pNode = NULL;
    FOREACH(pNode, pLogic->pParameterList) {
      if (nodesEqualNode(pNode, pJoin->pTagEqCond)) {
        ERASE_NODE(pLogic->pParameterList);
        break;
      } else if (QUERY_NODE_LOGIC_CONDITION == nodeType(pJoin->pTagEqCond)) {
        SLogicConditionNode* pTags = (SLogicConditionNode*)pJoin->pTagEqCond;
        SNode* pTag = NULL;
        FOREACH(pTag, pTags->pParameterList) {
          if (nodesEqualNode(pTag, pNode)) {
            ERASE_NODE(pLogic->pParameterList);
            break;
          }
        }
      }
    }

    if (pLogic->pParameterList->length <= 0) {
      NODES_DESTORY_NODE(pJoin->pOtherOnCond);
    }
  }
}

static int32_t stbJoinOptCreateMergeJoinNode(SLogicNode* pOrig, SLogicNode* pChild, SLogicNode** ppLogic) {
  SJoinLogicNode* pOrigJoin = (SJoinLogicNode*)pOrig;
  SJoinLogicNode* pJoin = (SJoinLogicNode*)nodesCloneNode((SNode*)pOrig);
  if (NULL == pJoin) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pJoin->joinAlgo = JOIN_ALGO_MERGE;
  //pJoin->node.dynamicOp = true;

  stbJoinOptRemoveTagEqCond(pJoin);
  NODES_DESTORY_NODE(pJoin->pTagEqCond);
  
  SNode* pNode = NULL;
  FOREACH(pNode, pJoin->node.pChildren) {
    ERASE_NODE(pJoin->node.pChildren);
  }
  int32_t code = nodesListStrictAppend(pJoin->node.pChildren, (SNode *)pChild);
  if (TSDB_CODE_SUCCESS == code) {
    pChild->pParent = (SLogicNode*)pJoin;
    *ppLogic = (SLogicNode*)pJoin;
  } else {
    nodesDestroyNode((SNode*)pJoin);
  }

  return code;
}

static int32_t stbJoinOptCreateDynQueryCtrlNode(SLogicNode* pRoot, SLogicNode* pPrev, SLogicNode* pPost, bool* srcScan, SLogicNode** ppDynNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  SDynQueryCtrlLogicNode* pDynCtrl = (SDynQueryCtrlLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL);
  if (NULL == pDynCtrl) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pDynCtrl->qType = DYN_QTYPE_STB_HASH;
  pDynCtrl->stbJoin.batchFetch = getBatchScanOptionFromHint(pRoot->pHint);
  memcpy(pDynCtrl->stbJoin.srcScan, srcScan, sizeof(pDynCtrl->stbJoin.srcScan));
  
  if (TSDB_CODE_SUCCESS == code) {  
    pDynCtrl->node.pChildren = nodesMakeList();
    if (NULL == pDynCtrl->node.pChildren) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {  
    pDynCtrl->stbJoin.pVgList = nodesMakeList();
    if (NULL == pDynCtrl->stbJoin.pVgList) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {  
    pDynCtrl->stbJoin.pUidList = nodesMakeList();
    if (NULL == pDynCtrl->stbJoin.pUidList) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  SJoinLogicNode* pHJoin = (SJoinLogicNode*)pPrev;
  nodesListStrictAppend(pDynCtrl->stbJoin.pUidList, nodesListGetNode(pHJoin->node.pTargets, 0));
  nodesListStrictAppend(pDynCtrl->stbJoin.pUidList, nodesListGetNode(pHJoin->node.pTargets, 2));
  nodesListStrictAppend(pDynCtrl->stbJoin.pVgList, nodesListGetNode(pHJoin->node.pTargets, 1));
  nodesListStrictAppend(pDynCtrl->stbJoin.pVgList, nodesListGetNode(pHJoin->node.pTargets, 3));

  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListStrictAppend(pDynCtrl->node.pChildren, (SNode*)pPrev);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListStrictAppend(pDynCtrl->node.pChildren, (SNode*)pPost);
    }
    if (TSDB_CODE_SUCCESS == code) {
      pDynCtrl->node.pTargets = nodesCloneList(pPost->pTargets);
      if (!pDynCtrl->node.pTargets) {
        code = TSDB_CODE_OUT_OF_MEMORY;
      }
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    pPrev->pParent = (SLogicNode*)pDynCtrl;
    pPost->pParent = (SLogicNode*)pDynCtrl;
    
    *ppDynNode = (SLogicNode*)pDynCtrl;
  } else {
    nodesDestroyNode((SNode*)pDynCtrl);
    *ppDynNode = NULL;
  }

  return code;
}

static int32_t stbJoinOptRewriteStableJoin(SOptimizeContext* pCxt, SLogicNode* pJoin, SLogicSubplan* pLogicSubplan) {
  SNodeList*  pTagScanNodes = NULL;
  SNodeList*  pTbScanNodes = NULL;
  SLogicNode* pGrpCacheNode = NULL;
  SLogicNode* pHJoinNode = NULL;
  SLogicNode* pMJoinNode = NULL;
  SLogicNode* pDynNode = NULL;  
  bool        srcScan[2] = {0};
  
  int32_t code = stbJoinOptCreateTagScanNode(pJoin, &pTagScanNodes);
  if (TSDB_CODE_SUCCESS == code) {
    code = stbJoinOptCreateTagHashJoinNode(pJoin, pTagScanNodes, &pHJoinNode);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = stbJoinOptCreateTableScanNodes(pJoin, &pTbScanNodes, srcScan);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = stbJoinOptCreateGroupCacheNode(getLogicNodeRootNode(pJoin), pTbScanNodes, &pGrpCacheNode);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = stbJoinOptCreateMergeJoinNode(pJoin, pGrpCacheNode, &pMJoinNode);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = stbJoinOptCreateDynQueryCtrlNode(getLogicNodeRootNode(pJoin), pHJoinNode, pMJoinNode, srcScan, &pDynNode);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = replaceLogicNode(pLogicSubplan, pJoin, (SLogicNode*)pDynNode);
  }
  if (TSDB_CODE_SUCCESS == code) {
    nodesDestroyNode((SNode*)pJoin);
    pCxt->optimized = true;
  }
  return code;
}

static int32_t stableJoinOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SLogicNode* pNode = optFindPossibleNode(pLogicSubplan->pNode, stbJoinOptShouldBeOptimized);
  if (NULL == pNode) {
    return TSDB_CODE_SUCCESS;
  }

  return stbJoinOptRewriteStableJoin(pCxt, pNode, pLogicSubplan);
}

static bool partColOptShouldBeOptimized(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_PARTITION == nodeType(pNode)) {
    SPartitionLogicNode* pPartition = (SPartitionLogicNode*)pNode;
    if (keysHasCol(pPartition->pPartitionKeys)) return true;
  }
  return false;
}

static SSortLogicNode* partColOptCreateSort(SPartitionLogicNode* pPartition) {
  SNode* node;
  int32_t code = TSDB_CODE_SUCCESS;
  SSortLogicNode* pSort = (SSortLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SORT);
  if (pSort) {
    bool alreadyPartByPKTs = false;
    pSort->groupSort = false;
    FOREACH(node, pPartition->pPartitionKeys) {
      SOrderByExprNode* pOrder = (SOrderByExprNode*)nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR);
      if (QUERY_NODE_COLUMN == nodeType(node) && ((SColumnNode*)node)->colId == pPartition->pkTsColId &&
          ((SColumnNode*)node)->tableId == pPartition->pkTsColTbId)
        alreadyPartByPKTs = true;
      if (!pOrder) {
        code = TSDB_CODE_OUT_OF_MEMORY;
      } else {
        nodesListMakeAppend(&pSort->pSortKeys, (SNode*)pOrder);
        pOrder->order = ORDER_ASC;
        pOrder->pExpr = nodesCloneNode(node);
        pOrder->nullOrder = NULL_ORDER_FIRST;
        if (!pOrder->pExpr) code = TSDB_CODE_OUT_OF_MEMORY;
      }
    }

    if (pPartition->needBlockOutputTsOrder && !alreadyPartByPKTs) {
      SOrderByExprNode* pOrder = (SOrderByExprNode*)nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR);
      if (!pOrder) {
        code = TSDB_CODE_OUT_OF_MEMORY;
      } else {
        pSort->excludePkCol = true;
        nodesListMakeAppend(&pSort->pSortKeys, (SNode*)pOrder);
        pOrder->order = ORDER_ASC;
        pOrder->pExpr = 0;
        FOREACH(node, pPartition->node.pTargets) {
          if (nodeType(node) == QUERY_NODE_COLUMN) {
            SColumnNode* pCol = (SColumnNode*)node;
            if (pCol->colId == pPartition->pkTsColId && pCol->tableId == pPartition->pkTsColTbId) {
              pOrder->pExpr = nodesCloneNode((SNode*)pCol);
              break;
            }
          }
        }
        if (!pOrder->pExpr) {
          code = TSDB_CODE_PAR_INTERNAL_ERROR;
        }
      }
    }
  }
  if (code != TSDB_CODE_SUCCESS) {
    nodesDestroyNode((SNode*)pSort);
    pSort = NULL;
  }
  return pSort;
}

static int32_t partitionColsOpt(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SNode*               node;
  int32_t              code = TSDB_CODE_SUCCESS;
  SPartitionLogicNode* pNode =
      (SPartitionLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, partColOptShouldBeOptimized);
  if (NULL == pNode) return TSDB_CODE_SUCCESS;
  SLogicNode* pRootNode = getLogicNodeRootNode((SLogicNode*)pNode);


  if (pRootNode->pHint && getSortForGroupOptHint(pRootNode->pHint)) {
    // replace with sort node
    SSortLogicNode* pSort = partColOptCreateSort(pNode);
    if (!pSort) {
      // if sort create failed, we eat the error, skip the optimization
      code = TSDB_CODE_SUCCESS;
    } else {
      TSWAP(pSort->node.pChildren, pNode->node.pChildren);
      TSWAP(pSort->node.pTargets, pNode->node.pTargets);
      optResetParent((SLogicNode*)pSort);
      pSort->calcGroupId = true;
      code = replaceLogicNode(pLogicSubplan, (SLogicNode*)pNode, (SLogicNode*)pSort);
      if (code == TSDB_CODE_SUCCESS) {
        pCxt->optimized = true;
      } else {
        nodesDestroyNode((SNode*)pSort);
      }
    }
    return code;
  } else if (pNode->node.pParent && nodeType(pNode->node.pParent) == QUERY_NODE_LOGIC_PLAN_AGG &&
             !getOptHint(pRootNode->pHint, HINT_PARTITION_FIRST)) {
    // Check if we can delete partition node
    SAggLogicNode* pAgg = (SAggLogicNode*)pNode->node.pParent;
    FOREACH(node, pNode->pPartitionKeys) {
      SGroupingSetNode* pgsNode = (SGroupingSetNode*)nodesMakeNode(QUERY_NODE_GROUPING_SET);
      if (!pgsNode) code = TSDB_CODE_OUT_OF_MEMORY;
      if (code == TSDB_CODE_SUCCESS) {
        pgsNode->groupingSetType = GP_TYPE_NORMAL;
        pgsNode->pParameterList = nodesMakeList();
        if (!pgsNode->pParameterList) code = TSDB_CODE_OUT_OF_MEMORY;
      }
      if (code == TSDB_CODE_SUCCESS) {
        code = nodesListAppend(pgsNode->pParameterList, nodesCloneNode(node));
      }
      if (code == TSDB_CODE_SUCCESS) {
        // Now we are using hash agg
        code = nodesListMakeAppend(&pAgg->pGroupKeys, (SNode*)pgsNode);
      }
      if (code != TSDB_CODE_SUCCESS) {
        nodesDestroyNode((SNode*)pgsNode);
        break;
      }
    }

    if (code == TSDB_CODE_SUCCESS) {
      code =
          replaceLogicNode(pLogicSubplan, (SLogicNode*)pNode, (SLogicNode*)nodesListGetNode(pNode->node.pChildren, 0));
      NODES_CLEAR_LIST(pNode->node.pChildren);
    }
    if (code == TSDB_CODE_SUCCESS) {
      // For hash agg, nonblocking mode is meaningless, slimit is useless, so we reset it
      pAgg->node.forceCreateNonBlockingOptr = false;
      nodesDestroyNode(pAgg->node.pSlimit);
      pAgg->node.pSlimit = NULL;
      nodesDestroyNode((SNode*)pNode);
      pCxt->optimized = true;
    }
    return code;
  }

  return code;
}

// clang-format off
static const SOptimizeRule optimizeRuleSet[] = {
  {.pName = "ScanPath",                   .optimizeFunc = scanPathOptimize},
  {.pName = "PushDownCondition",          .optimizeFunc = pushDownCondOptimize},
  {.pName = "StableJoin",                 .optimizeFunc = stableJoinOptimize},
  {.pName = "sortNonPriKeyOptimize",      .optimizeFunc = sortNonPriKeyOptimize},
  {.pName = "SortPrimaryKey",             .optimizeFunc = sortPrimaryKeyOptimize},
  {.pName = "SortForjoin",                .optimizeFunc = sortForJoinOptimize},
  {.pName = "SmaIndex",                   .optimizeFunc = smaIndexOptimize},
  {.pName = "PushDownLimit",              .optimizeFunc = pushDownLimitOptimize},
  {.pName = "PartitionTags",              .optimizeFunc = partTagsOptimize},
  {.pName = "MergeProjects",              .optimizeFunc = mergeProjectsOptimize},
  {.pName = "RewriteTail",                .optimizeFunc = rewriteTailOptimize},
  {.pName = "RewriteUnique",              .optimizeFunc = rewriteUniqueOptimize},
  {.pName = "splitCacheLastFunc",         .optimizeFunc = splitCacheLastFuncOptimize},
  {.pName = "LastRowScan",                .optimizeFunc = lastRowScanOptimize},
  {.pName = "TagScan",                    .optimizeFunc = tagScanOptimize},
  {.pName = "TableCountScan",             .optimizeFunc = tableCountScanOptimize},
  {.pName = "EliminateProject",           .optimizeFunc = eliminateProjOptimize},
  {.pName = "EliminateSetOperator",       .optimizeFunc = eliminateSetOpOptimize},
  {.pName = "PartitionCols",              .optimizeFunc = partitionColsOpt},
};
// clang-format on

static const int32_t optimizeRuleNum = (sizeof(optimizeRuleSet) / sizeof(SOptimizeRule));

static void dumpLogicSubplan(const char* pRuleName, SLogicSubplan* pSubplan) {
  if (!tsQueryPlannerTrace) {
    return;
  }
  char* pStr = NULL;
  nodesNodeToString((SNode*)pSubplan, false, &pStr, NULL);
  if (NULL == pRuleName) {
    qDebugL("before optimize, JsonPlan: %s", pStr);
  } else {
    qDebugL("apply optimize %s rule, JsonPlan: %s", pRuleName, pStr);
  }
  taosMemoryFree(pStr);
}

static int32_t applyOptimizeRule(SPlanContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SOptimizeContext cxt = {.pPlanCxt = pCxt, .optimized = false};
  bool             optimized = false;
  dumpLogicSubplan(NULL, pLogicSubplan);
  do {
    optimized = false;
    for (int32_t i = 0; i < optimizeRuleNum; ++i) {
      cxt.optimized = false;
      int32_t code = optimizeRuleSet[i].optimizeFunc(&cxt, pLogicSubplan);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      if (cxt.optimized) {
        optimized = true;
        dumpLogicSubplan(optimizeRuleSet[i].pName, pLogicSubplan);
        break;
      }
    }
  } while (optimized);
  return TSDB_CODE_SUCCESS;
}

int32_t optimizeLogicPlan(SPlanContext* pCxt, SLogicSubplan* pLogicSubplan) {
  if (SUBPLAN_TYPE_MODIFY == pLogicSubplan->subplanType && NULL == pLogicSubplan->pNode->pChildren) {
    return TSDB_CODE_SUCCESS;
  }
  return applyOptimizeRule(pCxt, pLogicSubplan);
}
