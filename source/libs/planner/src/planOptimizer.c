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
#include "ttime.h"

#define OPTIMIZE_FLAG_MASK(n) (1 << n)

#define OPTIMIZE_FLAG_SCAN_PATH       OPTIMIZE_FLAG_MASK(0)
#define OPTIMIZE_FLAG_PUSH_DOWN_CONDE OPTIMIZE_FLAG_MASK(1)

#define OPTIMIZE_FLAG_SET_MASK(val, mask)  (val) |= (mask)
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

typedef struct SOsdInfo {
  SScanLogicNode* pScan;
  SNodeList*      pSdrFuncs;
  SNodeList*      pDsoFuncs;
} SOsdInfo;

typedef struct SCpdIsMultiTableCondCxt {
  SNodeList* pLeftCols;
  SNodeList* pRightCols;
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

EDealRes scanPathOptHaveNormalColImpl(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    // *((bool*)pContext) = (COLUMN_TYPE_TAG != ((SColumnNode*)pNode)->colType);
    *((bool*)pContext) = true;
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
  if (NULL == pNode->pParent || (QUERY_NODE_LOGIC_PLAN_WINDOW != nodeType(pNode->pParent) &&
                                 QUERY_NODE_LOGIC_PLAN_AGG != nodeType(pNode->pParent) &&
                                 QUERY_NODE_LOGIC_PLAN_PARTITION != nodeType(pNode->pParent))) {
    return false;
  }
  if ((QUERY_NODE_LOGIC_PLAN_WINDOW == nodeType(pNode->pParent) &&
       WINDOW_TYPE_INTERVAL == ((SWindowLogicNode*)pNode->pParent)->winType) ||
      (QUERY_NODE_LOGIC_PLAN_PARTITION == nodeType(pNode->pParent) && pNode->pParent->pParent &&
       QUERY_NODE_LOGIC_PLAN_WINDOW == nodeType(pNode->pParent->pParent) &&
       WINDOW_TYPE_INTERVAL == ((SWindowLogicNode*)pNode->pParent)->winType)) {
    return true;
  }
  if (QUERY_NODE_LOGIC_PLAN_AGG == nodeType(pNode->pParent)) {
    return !scanPathOptHaveNormalCol(((SAggLogicNode*)pNode->pParent)->pGroupKeys);
  }
  return false;
}

static SNodeList* scanPathOptGetAllFuncs(SLogicNode* pNode) {
  switch (nodeType(pNode)) {
    case QUERY_NODE_LOGIC_PLAN_WINDOW:
      return ((SWindowLogicNode*)pNode)->pFuncs;
    case QUERY_NODE_LOGIC_PLAN_AGG:
      return ((SAggLogicNode*)pNode)->pAggFuncs;
    default:
      break;
  }
  return NULL;
}

static bool scanPathOptNeedOptimizeDataRequire(const SFunctionNode* pFunc) {
  if (!fmIsSpecialDataRequiredFunc(pFunc->funcId)) {
    return false;
  }
  SNode* pPara = NULL;
  FOREACH(pPara, pFunc->pParameterList) {
    if (QUERY_NODE_COLUMN != nodeType(pPara) && QUERY_NODE_VALUE != nodeType(pPara)) {
      return false;
    }
  }
  return true;
}

static bool scanPathOptNeedDynOptimize(const SFunctionNode* pFunc) {
  if (!fmIsDynamicScanOptimizedFunc(pFunc->funcId)) {
    return false;
  }
  SNode* pPara = NULL;
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
  SNode*     pFunc = NULL;
  bool       otherFunc = false;
  FOREACH(pFunc, pAllFuncs) {
    int32_t code = TSDB_CODE_SUCCESS;
    if (scanPathOptNeedOptimizeDataRequire((SFunctionNode*)pFunc)) {
      code = nodesListMakeStrictAppend(&pTmpSdrFuncs, nodesCloneNode(pFunc));
    } else if (scanPathOptNeedDynOptimize((SFunctionNode*)pFunc)) {
      code = nodesListMakeStrictAppend(&pTmpDsoFuncs, nodesCloneNode(pFunc));
    } else {
      otherFunc = true;
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

static int32_t scanPathOptMatch(SOptimizeContext* pCxt, SLogicNode* pLogicNode, SOsdInfo* pInfo) {
  pInfo->pScan = (SScanLogicNode*)optFindPossibleNode(pLogicNode, scanPathOptMayBeOptimized);
  if (NULL == pInfo->pScan) {
    return TSDB_CODE_SUCCESS;
  }
  return scanPathOptGetRelatedFuncs(pInfo->pScan, &pInfo->pSdrFuncs, &pInfo->pDsoFuncs);
}

static EFuncDataRequired scanPathOptPromoteDataRequired(EFuncDataRequired l, EFuncDataRequired r) {
  switch (l) {
    case FUNC_DATA_REQUIRED_DATA_LOAD:
      return l;
    case FUNC_DATA_REQUIRED_STATIS_LOAD:
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
    pScan->triggerType = ((SWindowLogicNode*)pParent)->triggerType;
    pScan->watermark = ((SWindowLogicNode*)pParent)->watermark;
    pScan->igExpired = ((SWindowLogicNode*)pParent)->igExpired;
  }
}

static int32_t scanPathOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SOsdInfo info = {0};
  int32_t  code = scanPathOptMatch(pCxt, pLogicSubplan->pNode, &info);
  if (TSDB_CODE_SUCCESS == code && info.pScan) {
    scanPathOptSetScanWin((SScanLogicNode*)info.pScan);
  }
  if (TSDB_CODE_SUCCESS == code && (NULL != info.pDsoFuncs || NULL != info.pSdrFuncs)) {
    info.pScan->dataRequired = scanPathOptGetDataRequired(info.pSdrFuncs);
    info.pScan->pDynamicScanFuncs = info.pDsoFuncs;
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
  int32_t code = nodesPartitionCond(&pScan->node.pConditions, &pPrimaryKeyCond, &pScan->pTagIndexCond, &pScan->pTagCond,
                                    &pOtherCond);
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

static EDealRes pushDownCondOptIsCrossTableCond(SNode* pNode, void* pContext) {
  SCpdIsMultiTableCondCxt* pCxt = pContext;
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    if (pushDownCondOptBelongThisTable(pNode, pCxt->pLeftCols)) {
      pCxt->havaLeftCol = true;
    } else if (pushDownCondOptBelongThisTable(pNode, pCxt->pRightCols)) {
      pCxt->haveRightCol = true;
    }
    return pCxt->havaLeftCol && pCxt->haveRightCol ? DEAL_RES_END : DEAL_RES_CONTINUE;
  }
  return DEAL_RES_CONTINUE;
}

static ECondAction pushDownCondOptGetCondAction(EJoinType joinType, SNodeList* pLeftCols, SNodeList* pRightCols,
                                                SNode* pNode) {
  SCpdIsMultiTableCondCxt cxt = {
      .pLeftCols = pLeftCols, .pRightCols = pRightCols, .havaLeftCol = false, .haveRightCol = false};
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
    return TSDB_CODE_SUCCESS;
  }

  SNodeList* pLeftCols = ((SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0))->pTargets;
  SNodeList* pRightCols = ((SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1))->pTargets;
  int32_t    code = TSDB_CODE_SUCCESS;

  SNodeList* pOnConds = NULL;
  SNodeList* pLeftChildConds = NULL;
  SNodeList* pRightChildConds = NULL;
  SNodeList* pRemainConds = NULL;
  SNode*     pCond = NULL;
  FOREACH(pCond, pLogicCond->pParameterList) {
    ECondAction condAction = pushDownCondOptGetCondAction(pJoin->joinType, pLeftCols, pRightCols, pCond);
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
  SNodeList*  pLeftCols = ((SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0))->pTargets;
  SNodeList*  pRightCols = ((SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1))->pTargets;
  ECondAction condAction =
      pushDownCondOptGetCondAction(pJoin->joinType, pLeftCols, pRightCols, pJoin->node.pConditions);
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
  return pushDownCondOptAppendCond(&pJoin->pOnConditions, pCond);
}

static int32_t pushDownCondOptPushCondToScan(SOptimizeContext* pCxt, SScanLogicNode* pScan, SNode** pCond) {
  return pushDownCondOptAppendCond(&pScan->node.pConditions, pCond);
}

static int32_t pushDownCondOptPushCondToProject(SOptimizeContext* pCxt, SProjectLogicNode* pProject, SNode** pCond) {
  return pushDownCondOptAppendCond(&pProject->node.pConditions, pCond);
}

static int32_t pushDownCondOptPushCondToJoin(SOptimizeContext* pCxt, SJoinLogicNode* pJoin, SNode** pCond) {
  return pushDownCondOptAppendCond(&pJoin->node.pConditions, pCond);
}

static int32_t pushDownCondOptPushCondToChild(SOptimizeContext* pCxt, SLogicNode* pChild, SNode** pCond) {
  return pushDownCondOptAppendCond(&pChild->pConditions, pCond);
}

static bool pushDownCondOptIsPriKey(SNode* pNode, SNodeList* pTableCols) {
  if (QUERY_NODE_COLUMN != nodeType(pNode)) {
    return false;
  }
  SColumnNode* pCol = (SColumnNode*)pNode;
  if (PRIMARYKEY_TIMESTAMP_COL_ID != pCol->colId) {
    return false;
  }
  return pushDownCondOptBelongThisTable(pNode, pTableCols);
}

static bool pushDownCondOptIsPriKeyEqualCond(SJoinLogicNode* pJoin, SNode* pCond) {
  if (QUERY_NODE_OPERATOR != nodeType(pCond)) {
    return false;
  }

  SOperatorNode* pOper = (SOperatorNode*)pCond;
  if (OP_TYPE_EQUAL != pOper->opType) {
    return false;
  }

  SNodeList* pLeftCols = ((SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0))->pTargets;
  SNodeList* pRightCols = ((SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1))->pTargets;
  if (pushDownCondOptIsPriKey(pOper->pLeft, pLeftCols)) {
    return pushDownCondOptIsPriKey(pOper->pRight, pRightCols);
  } else if (pushDownCondOptIsPriKey(pOper->pLeft, pRightCols)) {
    return pushDownCondOptIsPriKey(pOper->pRight, pLeftCols);
  }
  return false;
}

static bool pushDownCondOptContainPriKeyEqualCond(SJoinLogicNode* pJoin, SNode* pCond) {
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pCond)) {
    SLogicConditionNode* pLogicCond = (SLogicConditionNode*)pCond;
    if (LOGIC_COND_TYPE_AND != pLogicCond->condType) {
      return false;
    }
    bool   hasPrimaryKeyEqualCond = false;
    SNode* pCond = NULL;
    FOREACH(pCond, pLogicCond->pParameterList) {
      if (pushDownCondOptContainPriKeyEqualCond(pJoin, pCond)) {
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
  if (NULL == pJoin->pOnConditions) {
    return generateUsageErrMsg(pCxt->pPlanCxt->pMsg, pCxt->pPlanCxt->msgLen, TSDB_CODE_PLAN_NOT_SUPPORT_CROSS_JOIN);
  }
  if (!pushDownCondOptContainPriKeyEqualCond(pJoin, pJoin->pOnConditions)) {
    return generateUsageErrMsg(pCxt->pPlanCxt->pMsg, pCxt->pPlanCxt->msgLen, TSDB_CODE_PLAN_EXPECTED_TS_EQUAL);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t pushDownCondOptPartJoinOnCondLogicCond(SJoinLogicNode* pJoin, SNode** ppMergeCond, SNode** ppOnCond) {
  SLogicConditionNode* pLogicCond = (SLogicConditionNode*)(pJoin->pOnConditions);

  int32_t    code = TSDB_CODE_SUCCESS;
  SNodeList* pOnConds = NULL;
  SNode*     pCond = NULL;
  FOREACH(pCond, pLogicCond->pParameterList) {
    if (pushDownCondOptIsPriKeyEqualCond(pJoin, pCond)) {
      *ppMergeCond = nodesCloneNode(pCond);
    } else {
      code = nodesListMakeAppend(&pOnConds, nodesCloneNode(pCond));
    }
  }

  SNode* pTempOnCond = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesMergeConds(&pTempOnCond, &pOnConds);
  }

  if (TSDB_CODE_SUCCESS == code && NULL != *ppMergeCond) {
    *ppOnCond = pTempOnCond;
    nodesDestroyNode(pJoin->pOnConditions);
    pJoin->pOnConditions = NULL;
    return TSDB_CODE_SUCCESS;
  } else {
    nodesDestroyList(pOnConds);
    nodesDestroyNode(pTempOnCond);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }
}

static int32_t pushDownCondOptPartJoinOnCond(SJoinLogicNode* pJoin, SNode** ppMergeCond, SNode** ppOnCond) {
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pJoin->pOnConditions) &&
      LOGIC_COND_TYPE_AND == ((SLogicConditionNode*)(pJoin->pOnConditions))->condType) {
    return pushDownCondOptPartJoinOnCondLogicCond(pJoin, ppMergeCond, ppOnCond);
  }

  if (pushDownCondOptIsPriKeyEqualCond(pJoin, pJoin->pOnConditions)) {
    *ppMergeCond = nodesCloneNode(pJoin->pOnConditions);
    *ppOnCond = NULL;
    nodesDestroyNode(pJoin->pOnConditions);
    pJoin->pOnConditions = NULL;
    return TSDB_CODE_SUCCESS;
  } else {
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }
}

static int32_t pushDownCondOptJoinExtractMergeCond(SOptimizeContext* pCxt, SJoinLogicNode* pJoin) {
  int32_t code = pushDownCondOptCheckJoinOnCond(pCxt, pJoin);
  SNode*  pJoinMergeCond = NULL;
  SNode*  pJoinOnCond = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = pushDownCondOptPartJoinOnCond(pJoin, &pJoinMergeCond, &pJoinOnCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pJoin->pMergeCondition = pJoinMergeCond;
    pJoin->pOnConditions = pJoinOnCond;
  } else {
    nodesDestroyNode(pJoinMergeCond);
    nodesDestroyNode(pJoinOnCond);
  }
  return code;
}

static int32_t pushDownCondOptDealJoin(SOptimizeContext* pCxt, SJoinLogicNode* pJoin) {
  if (OPTIMIZE_FLAG_TEST_MASK(pJoin->node.optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE)) {
    return TSDB_CODE_SUCCESS;
  }

  if (NULL == pJoin->node.pConditions) {
    int32_t code = pushDownCondOptJoinExtractMergeCond(pCxt, pJoin);
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
    code = pushDownCondOptJoinExtractMergeCond(pCxt, pJoin);
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
  pushDownCondOptAppendCond(&pAgg->node.pConditions, pAggFuncCond);
  return TSDB_CODE_SUCCESS;
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
  if (pSort->groupSort || !sortPriKeyOptIsPriKeyOrderBy(pSort->pSortKeys) || 1 != LIST_LENGTH(pSort->node.pChildren)) {
    return TSDB_CODE_SUCCESS;
  }
  return true;
}

static int32_t sortPriKeyOptGetScanNodesImpl(SLogicNode* pNode, bool* pNotOptimize, SNodeList** pScanNodes) {
  int32_t code = TSDB_CODE_SUCCESS;

  switch (nodeType(pNode)) {
    case QUERY_NODE_LOGIC_PLAN_SCAN:
      if (TSDB_SUPER_TABLE != ((SScanLogicNode*)pNode)->tableType) {
        return nodesListMakeAppend(pScanNodes, (SNode*)pNode);
      }
      break;
    case QUERY_NODE_LOGIC_PLAN_JOIN:
      code =
          sortPriKeyOptGetScanNodesImpl((SLogicNode*)nodesListGetNode(pNode->pChildren, 0), pNotOptimize, pScanNodes);
      if (TSDB_CODE_SUCCESS == code) {
        code =
            sortPriKeyOptGetScanNodesImpl((SLogicNode*)nodesListGetNode(pNode->pChildren, 1), pNotOptimize, pScanNodes);
      }
      return code;
    case QUERY_NODE_LOGIC_PLAN_AGG:
      *pNotOptimize = true;
      return code;
    default:
      break;
  }

  if (1 != LIST_LENGTH(pNode->pChildren)) {
    *pNotOptimize = true;
    return TSDB_CODE_SUCCESS;
  }

  return sortPriKeyOptGetScanNodesImpl((SLogicNode*)nodesListGetNode(pNode->pChildren, 0), pNotOptimize, pScanNodes);
}

static int32_t sortPriKeyOptGetScanNodes(SLogicNode* pNode, SNodeList** pScanNodes) {
  bool    notOptimize = false;
  int32_t code = sortPriKeyOptGetScanNodesImpl(pNode, &notOptimize, pScanNodes);
  if (TSDB_CODE_SUCCESS != code || notOptimize) {
    nodesClearList(*pScanNodes);
  }
  return code;
}

static EOrder sortPriKeyOptGetPriKeyOrder(SSortLogicNode* pSort) {
  return ((SOrderByExprNode*)nodesListGetNode(pSort->pSortKeys, 0))->order;
}

static int32_t sortPriKeyOptApply(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan, SSortLogicNode* pSort,
                                  SNodeList* pScanNodes) {
  EOrder order = sortPriKeyOptGetPriKeyOrder(pSort);
  if (ORDER_DESC == order) {
    SNode* pScanNode = NULL;
    FOREACH(pScanNode, pScanNodes) {
      SScanLogicNode* pScan = (SScanLogicNode*)pScanNode;
      if (pScan->scanSeq[0] > 0) {
        TSWAP(pScan->scanSeq[0], pScan->scanSeq[1]);
      }
    }
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
  SNodeList* pScanNodes = NULL;
  int32_t    code = sortPriKeyOptGetScanNodes((SLogicNode*)nodesListGetNode(pSort->node.pChildren, 0), &pScanNodes);
  if (TSDB_CODE_SUCCESS == code && NULL != pScanNodes) {
    code = sortPriKeyOptApply(pCxt, pLogicSubplan, pSort, pScanNodes);
  }
  nodesClearList(pScanNodes);
  return code;
}

static int32_t sortPrimaryKeyOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SSortLogicNode* pSort = (SSortLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, sortPriKeyOptMayBeOptimized);
  if (NULL == pSort) {
    return TSDB_CODE_SUCCESS;
  }
  return sortPrimaryKeyOptimizeImpl(pCxt, pLogicSubplan, pSort);
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

static int32_t smaIndexOptCreateMerge(SLogicNode* pChild, SNodeList* pMergeKeys, SNodeList* pTargets,
                                      SLogicNode** pOutput) {
  SMergeLogicNode* pMerge = (SMergeLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_MERGE);
  if (NULL == pMerge) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pMerge->node.precision = pChild->precision;
  pMerge->numOfChannels = 2;
  pMerge->pMergeKeys = pMergeKeys;
  pMerge->node.pTargets = pTargets;
  pMerge->pInputs = nodesCloneList(pChild->pTargets);
  if (NULL == pMerge->pInputs) {
    nodesDestroyNode((SNode*)pMerge);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  *pOutput = (SLogicNode*)pMerge;
  return TSDB_CODE_SUCCESS;
}

static int32_t smaIndexOptRecombinationNode(SLogicSubplan* pLogicSubplan, SLogicNode* pInterval, SLogicNode* pMerge,
                                            SLogicNode* pSmaScan) {
  int32_t code = nodesListMakeAppend(&pMerge->pChildren, (SNode*)pInterval);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeAppend(&pMerge->pChildren, (SNode*)pSmaScan);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = replaceLogicNode(pLogicSubplan, pInterval, pMerge);
    pSmaScan->pParent = pMerge;
    pInterval->pParent = pMerge;
  }
  return code;
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
    return (pScan->scanRange.skey == taosTimeTruncate(pScan->scanRange.skey, &interval, pScan->node.precision)) &&
           (pScan->scanRange.ekey + 1 == taosTimeTruncate(pScan->scanRange.ekey + 1, &interval, pScan->node.precision));
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

static int32_t smaIndexOptCreateSmaCols(SNodeList* pFuncs, uint64_t tableId, SNodeList* pSmaFuncs, SNodeList** pOutput,
                                        int32_t* pWStrartIndex) {
  SNodeList* pCols = NULL;
  SNode*     pFunc = NULL;
  int32_t    code = TSDB_CODE_SUCCESS;
  int32_t    index = 0;
  int32_t    smaFuncIndex = -1;
  *pWStrartIndex = -1;
  FOREACH(pFunc, pFuncs) {
    if (FUNCTION_TYPE_WSTART == ((SFunctionNode*)pFunc)->funcType) {
      *pWStrartIndex = index;
    }
    smaFuncIndex = smaIndexOptFindSmaFunc(pFunc, pSmaFuncs);
    if (smaFuncIndex < 0) {
      break;
    } else {
      code = nodesListMakeStrictAppend(&pCols, smaIndexOptCreateSmaCol(pFunc, tableId, smaFuncIndex + 1));
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    }
    ++index;
  }

  if (TSDB_CODE_SUCCESS == code && smaFuncIndex >= 0) {
    *pOutput = pCols;
  } else {
    nodesDestroyList(pCols);
  }

  return code;
}

static int32_t smaIndexOptCouldApplyIndex(SScanLogicNode* pScan, STableIndexInfo* pIndex, SNodeList** pCols,
                                          int32_t* pWStrartIndex) {
  SWindowLogicNode* pWindow = (SWindowLogicNode*)pScan->node.pParent;
  if (!smaIndexOptEqualInterval(pScan, pWindow, pIndex)) {
    return TSDB_CODE_SUCCESS;
  }
  SNodeList* pSmaFuncs = NULL;
  int32_t    code = nodesStringToList(pIndex->expr, &pSmaFuncs);
  if (TSDB_CODE_SUCCESS == code) {
    code = smaIndexOptCreateSmaCols(pWindow->pFuncs, pIndex->dstTbUid, pSmaFuncs, pCols, pWStrartIndex);
  }
  nodesDestroyList(pSmaFuncs);
  return code;
}

static SNode* smaIndexOptCreateWStartTs() {
  SFunctionNode* pWStart = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  if (NULL == pWStart) {
    return NULL;
  }
  strcpy(pWStart->functionName, "_wstart");
  snprintf(pWStart->node.aliasName, sizeof(pWStart->node.aliasName), "%s.%p", pWStart->functionName, pWStart);
  if (TSDB_CODE_SUCCESS != fmGetFuncInfo(pWStart, NULL, 0)) {
    nodesDestroyNode((SNode*)pWStart);
    return NULL;
  }
  return (SNode*)pWStart;
}

static int32_t smaIndexOptCreateMergeKey(SNode* pCol, SNodeList** pMergeKeys) {
  SOrderByExprNode* pMergeKey = (SOrderByExprNode*)nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR);
  if (NULL == pMergeKey) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pMergeKey->pExpr = nodesCloneNode(pCol);
  if (NULL == pMergeKey->pExpr) {
    nodesDestroyNode((SNode*)pMergeKey);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pMergeKey->order = ORDER_ASC;
  pMergeKey->nullOrder = NULL_ORDER_FIRST;
  return nodesListMakeStrictAppend(pMergeKeys, (SNode*)pMergeKey);
}

static int32_t smaIndexOptRewriteInterval(SWindowLogicNode* pInterval, int32_t wstrartIndex, SNodeList** pMergeKeys) {
  if (wstrartIndex < 0) {
    SNode* pWStart = smaIndexOptCreateWStartTs();
    if (NULL == pWStart) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    int32_t code = createColumnByRewriteExpr(pWStart, &pInterval->node.pTargets);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode(pWStart);
      return code;
    }
    wstrartIndex = LIST_LENGTH(pInterval->node.pTargets) - 1;
  }
  return smaIndexOptCreateMergeKey(nodesListGetNode(pInterval->node.pTargets, wstrartIndex), pMergeKeys);
}

static int32_t smaIndexOptApplyIndexExt(SLogicSubplan* pLogicSubplan, SScanLogicNode* pScan, STableIndexInfo* pIndex,
                                        SNodeList* pSmaCols, int32_t wstrartIndex) {
  SWindowLogicNode* pInterval = (SWindowLogicNode*)pScan->node.pParent;
  SNodeList*        pMergeTargets = nodesCloneList(pInterval->node.pTargets);
  if (NULL == pMergeTargets) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  SLogicNode* pSmaScan = NULL;
  SLogicNode* pMerge = NULL;
  SNodeList*  pMergeKeys = NULL;
  int32_t     code = smaIndexOptRewriteInterval(pInterval, wstrartIndex, &pMergeKeys);
  if (TSDB_CODE_SUCCESS == code) {
    code = smaIndexOptCreateSmaScan(pScan, pIndex, pSmaCols, &pSmaScan);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = smaIndexOptCreateMerge(pScan->node.pParent, pMergeKeys, pMergeTargets, &pMerge);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = smaIndexOptRecombinationNode(pLogicSubplan, pScan->node.pParent, pMerge, pSmaScan);
  }
  return code;
}

static int32_t smaIndexOptApplyIndex(SLogicSubplan* pLogicSubplan, SScanLogicNode* pScan, STableIndexInfo* pIndex,
                                     SNodeList* pSmaCols, int32_t wstrartIndex) {
  SLogicNode* pSmaScan = NULL;
  int32_t     code = smaIndexOptCreateSmaScan(pScan, pIndex, pSmaCols, &pSmaScan);
  if (TSDB_CODE_SUCCESS == code) {
    code = replaceLogicNode(pLogicSubplan, pScan->node.pParent, pSmaScan);
  }
  return code;
}

static void smaIndexOptDestroySmaIndex(void* p) { taosMemoryFree(((STableIndexInfo*)p)->expr); }

static int32_t smaIndexOptimizeImpl(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan, SScanLogicNode* pScan) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t nindexes = taosArrayGetSize(pScan->pSmaIndexes);
  for (int32_t i = 0; i < nindexes; ++i) {
    STableIndexInfo* pIndex = taosArrayGet(pScan->pSmaIndexes, i);
    SNodeList*       pSmaCols = NULL;
    int32_t          wstrartIndex = -1;
    code = smaIndexOptCouldApplyIndex(pScan, pIndex, &pSmaCols, &wstrartIndex);
    if (TSDB_CODE_SUCCESS == code && NULL != pSmaCols) {
      code = smaIndexOptApplyIndex(pLogicSubplan, pScan, pIndex, pSmaCols, wstrartIndex);
      taosArrayDestroyEx(pScan->pSmaIndexes, smaIndexOptDestroySmaIndex);
      pScan->pSmaIndexes = NULL;
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

static EDealRes partTagsOptHasColImpl(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    if (COLUMN_TYPE_TAG != ((SColumnNode*)pNode)->colType && COLUMN_TYPE_TBNAME != ((SColumnNode*)pNode)->colType) {
      *(bool*)pContext = true;
      return DEAL_RES_END;
    }
  }
  return DEAL_RES_CONTINUE;
}

static bool planOptNodeListHasCol(SNodeList* pKeys) {
  bool hasCol = false;
  nodesWalkExprs(pKeys, partTagsOptHasColImpl, &hasCol);
  return hasCol;
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
  return ((QUERY_NODE_LOGIC_PLAN_PARTITION == nodeType(pNode) ||
           (QUERY_NODE_LOGIC_PLAN_AGG == nodeType(pNode) && NULL != ((SAggLogicNode*)pNode)->pGroupKeys &&
            NULL != ((SAggLogicNode*)pNode)->pAggFuncs)) &&
          1 == LIST_LENGTH(pNode->pChildren) &&
          QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(nodesListGetNode(pNode->pChildren, 0)));
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

  return !planOptNodeListHasCol(partTagsGetPartKeys(pNode)) && partTagsOptAreSupportedFuncs(partTagsGetFuncs(pNode));
}

static int32_t partTagsOptRebuildTbanme(SNodeList* pPartKeys) {
  int32_t code = TSDB_CODE_SUCCESS;
  nodesRewriteExprs(pPartKeys, optRebuildTbanme, &code);
  return code;
}

// todo refact: just to mask compilation warnings
static void partTagsSetAlias(char* pAlias, int32_t len, const char* pTableAlias, const char* pColName) {
  snprintf(pAlias, len, "%s.%s", pTableAlias, pColName);
}

static SNode* partTagsCreateWrapperFunc(const char* pFuncName, SNode* pNode) {
  SFunctionNode* pFunc = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  if (NULL == pFunc) {
    return NULL;
  }

  strcpy(pFunc->functionName, pFuncName);
  if (QUERY_NODE_COLUMN == nodeType(pNode) && COLUMN_TYPE_TBNAME != ((SColumnNode*)pNode)->colType) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    partTagsSetAlias(pFunc->node.aliasName, sizeof(pFunc->node.aliasName), pCol->tableAlias, pCol->colName);
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

static int32_t partTagsRewriteGroupTagsToFuncs(SNodeList* pGroupTags, SNodeList* pAggFuncs) {
  bool    hasIndefRowsSelectFunc = partTagsHasIndefRowsSelectFunc(pAggFuncs);
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pNode = NULL;
  FOREACH(pNode, pGroupTags) {
    if (hasIndefRowsSelectFunc) {
      code = nodesListStrictAppend(pAggFuncs, partTagsCreateWrapperFunc("_select_value", pNode));
    } else {
      code = nodesListStrictAppend(pAggFuncs, partTagsCreateWrapperFunc("_group_key", pNode));
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
    int32_t code = replaceLogicNode(pLogicSubplan, pNode, (SLogicNode*)pScan);
    if (TSDB_CODE_SUCCESS == code) {
      NODES_CLEAR_LIST(pNode->pChildren);
      nodesDestroyNode((SNode*)pNode);
    }
  } else {
    SAggLogicNode* pAgg = (SAggLogicNode*)pNode;
    SNode*         pGroupKey = NULL;
    FOREACH(pGroupKey, pAgg->pGroupKeys) {
      code = nodesListMakeStrictAppend(
          &pScan->pGroupTags, nodesCloneNode(nodesListGetNode(((SGroupingSetNode*)pGroupKey)->pParameterList, 0)));
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    }
    NODES_DESTORY_LIST(pAgg->pGroupKeys);
    code = partTagsRewriteGroupTagsToFuncs(pScan->pGroupTags, pAgg->pAggFuncs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = partTagsOptRebuildTbanme(pScan->pGroupTags);
  }
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

  if (QUERY_NODE_LOGIC_PLAN_PROJECT != nodeType(pNode) || 1 != LIST_LENGTH(pNode->pChildren)) {
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
      if (!nodesEqualNode(pTarget, pNode)) {
        pCxt->canUse = false;
        return DEAL_RES_END;
      }
    }
  }
  return DEAL_RES_CONTINUE;
}

static bool eliminateProjOptCanUseNewChildTargets(SLogicNode* pChild, SNodeList* pNewChildTargets) {
  if (NULL == pChild->pConditions) {
    return true;
  }
  CheckNewChildTargetsCxt cxt = {.pNewChildTargets = pNewChildTargets, .canUse = true};
  nodesWalkExpr(pChild->pConditions, eliminateProjOptCanUseNewChildTargetsImpl, &cxt);
  return cxt.canUse;
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
  if (eliminateProjOptCanUseNewChildTargets(pChild, pNewChildTargets)) {
    nodesDestroyList(pChild->pTargets);
    pChild->pTargets = pNewChildTargets;
  } else {
    nodesDestroyList(pNewChildTargets);
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = replaceLogicNode(pLogicSubplan, (SLogicNode*)pProjectNode, pChild);
  if (TSDB_CODE_SUCCESS == code) {
    NODES_CLEAR_LIST(pProjectNode->node.pChildren);
    nodesDestroyNode((SNode*)pProjectNode);
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
  pLimitNode->offset = NULL == pOffset ? -1 : ((SValueNode*)pOffset)->datum.i;
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
    snprintf(pFunc->node.aliasName, sizeof(pFunc->node.aliasName), "%s.%p", pFunc->functionName, pFunc);
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

static bool lastRowScanOptMayBeOptimized(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_AGG != nodeType(pNode) || 1 != LIST_LENGTH(pNode->pChildren) ||
      QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(nodesListGetNode(pNode->pChildren, 0))) {
    return false;
  }

  SAggLogicNode*  pAgg = (SAggLogicNode*)pNode;
  SScanLogicNode* pScan = (SScanLogicNode*)nodesListGetNode(pNode->pChildren, 0);
  if (!pAgg->hasLastRow || NULL != pAgg->pGroupKeys || NULL != pScan->node.pConditions || 0 == pScan->cacheLastMode ||
      IS_TSWINDOW_SPECIFIED(pScan->scanRange)) {
    return false;
  }

  SNode* pFunc = NULL;
  FOREACH(pFunc, ((SAggLogicNode*)pNode)->pAggFuncs) {
    if (FUNCTION_TYPE_LAST_ROW != ((SFunctionNode*)pFunc)->funcType &&
        FUNCTION_TYPE_SELECT_VALUE != ((SFunctionNode*)pFunc)->funcType &&
        FUNCTION_TYPE_GROUP_KEY != ((SFunctionNode*)pFunc)->funcType) {
      return false;
    }
  }

  return true;
}

static int32_t lastRowScanOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SAggLogicNode* pAgg = (SAggLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, lastRowScanOptMayBeOptimized);

  if (NULL == pAgg) {
    return TSDB_CODE_SUCCESS;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pAgg->pAggFuncs) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    if (FUNCTION_TYPE_LAST_ROW == pFunc->funcType) {
      int32_t len = snprintf(pFunc->functionName, sizeof(pFunc->functionName), "_cache_last_row");
      pFunc->functionName[len] = '\0';
      int32_t code = fmGetFuncInfo(pFunc, NULL, 0);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
    }
  }
  pAgg->hasLastRow = false;

  ((SScanLogicNode*)nodesListGetNode(pAgg->node.pChildren, 0))->scanType = SCAN_TYPE_LAST_ROW;

  pCxt->optimized = true;
  return TSDB_CODE_SUCCESS;
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

static bool tagScanMayBeOptimized(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pNode) || (SCAN_TYPE_TAG == ((SScanLogicNode*)pNode)->scanType)) {
    return false;
  }
  SScanLogicNode* pScan = (SScanLogicNode*)pNode;
  if (pScan->hasNormalCols) {
    return false;
  }
  if (NULL == pNode->pParent || QUERY_NODE_LOGIC_PLAN_AGG != nodeType(pNode->pParent) ||
      1 != LIST_LENGTH(pNode->pParent->pChildren)) {
    return false;
  }

  SAggLogicNode* pAgg = (SAggLogicNode*)(pNode->pParent);
  if (NULL == pAgg->pGroupKeys || NULL != pAgg->pAggFuncs || planOptNodeListHasCol(pAgg->pGroupKeys) ||
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
  SScanLogicNode* pScanNode = (SScanLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, tagScanMayBeOptimized);
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
        if (0 == strcmp(((SColumnNode*)pAggTarget)->colName, ((SColumnNode*)pAggTarget)->colName)) {
          nodesListAppend(pScanTargets, nodesCloneNode(pScanTarget));
          break;
        }
      }
    }
    nodesDestroyList(pScanNode->node.pTargets);
    pScanNode->node.pTargets = pScanTargets;
  }

  int32_t code = replaceLogicNode(pLogicSubplan, pAgg, (SLogicNode*)pScanNode);
  if (TSDB_CODE_SUCCESS == code) {
    NODES_CLEAR_LIST(pAgg->pChildren);
  }
  nodesDestroyNode((SNode*)pAgg);
  pCxt->optimized = true;
  return TSDB_CODE_SUCCESS;
}

// clang-format off
static const SOptimizeRule optimizeRuleSet[] = {
  {.pName = "ScanPath",                   .optimizeFunc = scanPathOptimize},
  {.pName = "PushDownCondition",          .optimizeFunc = pushDownCondOptimize},
  {.pName = "SortPrimaryKey",             .optimizeFunc = sortPrimaryKeyOptimize},
  {.pName = "SmaIndex",                   .optimizeFunc = smaIndexOptimize},
  {.pName = "PartitionTags",              .optimizeFunc = partTagsOptimize},
  {.pName = "MergeProjects",              .optimizeFunc = mergeProjectsOptimize},
  {.pName = "EliminateProject",           .optimizeFunc = eliminateProjOptimize},
  {.pName = "EliminateSetOperator",       .optimizeFunc = eliminateSetOpOptimize},
  {.pName = "RewriteTail",                .optimizeFunc = rewriteTailOptimize},
  {.pName = "RewriteUnique",              .optimizeFunc = rewriteUniqueOptimize},
  {.pName = "LastRowScan",               .optimizeFunc = lastRowScanOptimize},
  {.pName = "TagScan",                   .optimizeFunc = tagScanOptimize}
};
// clang-format on

static const int32_t optimizeRuleNum = (sizeof(optimizeRuleSet) / sizeof(SOptimizeRule));

static void dumpLogicSubplan(const char* pRuleName, SLogicSubplan* pSubplan) {
  char* pStr = NULL;
  nodesNodeToString((SNode*)pSubplan, false, &pStr, NULL);
  if (NULL == pRuleName) {
    qDebugL("before optimize: %s", pStr);
  } else {
    qDebugL("apply optimize %s rule: %s", pRuleName, pStr);
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
      }
    }
  } while (optimized);
  return TSDB_CODE_SUCCESS;
}

int32_t optimizeLogicPlan(SPlanContext* pCxt, SLogicSubplan* pLogicSubplan) {
  return applyOptimizeRule(pCxt, pLogicSubplan);
}
