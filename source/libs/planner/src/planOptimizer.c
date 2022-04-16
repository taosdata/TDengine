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
#include "functionMgt.h"

#define OPTIMIZE_FLAG_MASK(n)    (1 << n)

#define OPTIMIZE_FLAG_OSD OPTIMIZE_FLAG_MASK(0)

#define OPTIMIZE_FLAG_SET_MASK(val, mask) (val) |= (mask)
#define OPTIMIZE_FLAG_TEST_MASK(val, mask) (((val) & (mask)) != 0)

typedef struct SOptimizeContext {
  bool optimized;
} SOptimizeContext;

typedef int32_t (*FMatch)(SOptimizeContext* pCxt, SLogicNode* pLogicNode);
typedef int32_t (*FOptimize)(SOptimizeContext* pCxt, SLogicNode* pLogicNode);

typedef struct SOptimizeRule {
  char* pName;
  FOptimize optimizeFunc;
} SOptimizeRule;

typedef struct SOsdInfo {
  SScanLogicNode* pScan;
  SNodeList* pSdrFuncs;
  SNodeList* pDsoFuncs;
} SOsdInfo;

typedef struct SCpdIsMultiTableCondCxt {
  SNodeList* pLeftCols;
  SNodeList* pRightCols;
  bool havaLeftCol;
  bool haveRightCol;
} SCpdIsMultiTableCondCxt;

typedef enum ECondAction {
  COND_ACTION_STAY = 1,
  COND_ACTION_PUSH_JOIN,
  COND_ACTION_PUSH_LEFT_CHILD,
  COND_ACTION_PUSH_RIGHT_CHILD
  // after supporting outer join, there are other possibilities
} ECondAction;

EDealRes haveNormalColImpl(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    *((bool*)pContext) = (COLUMN_TYPE_TAG != ((SColumnNode*)pNode)->colType);
    return *((bool*)pContext) ? DEAL_RES_END : DEAL_RES_IGNORE_CHILD;
  }
  return DEAL_RES_CONTINUE;
}

static bool haveNormalCol(SNodeList* pList) {
  bool res = false;
  nodesWalkExprsPostOrder(pList, haveNormalColImpl, &res);
  return res;
}

static bool osdMayBeOptimized(SLogicNode* pNode) {
  if (OPTIMIZE_FLAG_TEST_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_OSD)) {
    return false;
  }
  if (QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pNode)) {
    return false;
  }
  if (NULL == pNode->pParent || 
      (QUERY_NODE_LOGIC_PLAN_WINDOW != nodeType(pNode->pParent) && QUERY_NODE_LOGIC_PLAN_AGG != nodeType(pNode->pParent))) {
    return false;
  }
  if (QUERY_NODE_LOGIC_PLAN_WINDOW == nodeType(pNode->pParent)) {
    return (WINDOW_TYPE_INTERVAL == ((SWindowLogicNode*)pNode->pParent)->winType);
  }
  return !haveNormalCol(((SAggLogicNode*)pNode->pParent)->pGroupKeys);
}

static SLogicNode* osdFindPossibleScanNode(SLogicNode* pNode) {
  if (osdMayBeOptimized(pNode)) {
    return pNode;
  }
  SNode* pChild;
  FOREACH(pChild, pNode->pChildren) {
    SLogicNode* pScanNode = osdFindPossibleScanNode((SLogicNode*)pChild);
    if (NULL != pScanNode) {
      return pScanNode;
    }
  }
  return NULL;
}

static SNodeList* osdGetAllFuncs(SLogicNode* pNode) {
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

static int32_t osdGetRelatedFuncs(SScanLogicNode* pScan, SNodeList** pSdrFuncs, SNodeList** pDsoFuncs) {
  SNodeList* pAllFuncs = osdGetAllFuncs(pScan->node.pParent);
  SNode* pFunc = NULL;
  FOREACH(pFunc, pAllFuncs) {
    int32_t code = TSDB_CODE_SUCCESS;
    if (fmIsSpecialDataRequiredFunc(((SFunctionNode*)pFunc)->funcId)) {
      code = nodesListMakeStrictAppend(pSdrFuncs, nodesCloneNode(pFunc));
    } else if (fmIsDynamicScanOptimizedFunc(((SFunctionNode*)pFunc)->funcId)) {
      code = nodesListMakeStrictAppend(pDsoFuncs, nodesCloneNode(pFunc));
    }
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyList(*pSdrFuncs);
      nodesDestroyList(*pDsoFuncs);
      return code;
    }
  }  
  return TSDB_CODE_SUCCESS;
}

static int32_t osdMatch(SOptimizeContext* pCxt, SLogicNode* pLogicNode, SOsdInfo* pInfo) {
  pInfo->pScan = (SScanLogicNode*)osdFindPossibleScanNode(pLogicNode);
  if (NULL == pInfo->pScan) {
    return TSDB_CODE_SUCCESS;
  }
  return osdGetRelatedFuncs(pInfo->pScan, &pInfo->pSdrFuncs, &pInfo->pDsoFuncs);
}

static EFuncDataRequired osdPromoteDataRequired(EFuncDataRequired l , EFuncDataRequired r) {
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

static int32_t osdGetDataRequired(SNodeList* pFuncs) {
  if (NULL == pFuncs) {
    return FUNC_DATA_REQUIRED_DATA_LOAD;
  }
  EFuncDataRequired dataRequired = FUNC_DATA_REQUIRED_FILTEROUT;
  SNode* pFunc = NULL;
  FOREACH(pFunc, pFuncs) {
    dataRequired = osdPromoteDataRequired(dataRequired, fmFuncDataRequired((SFunctionNode*)pFunc, NULL));
  }
  return dataRequired;
}

static int32_t osdOptimize(SOptimizeContext* pCxt, SLogicNode* pLogicNode) {
  SOsdInfo info = {0};
  int32_t code = osdMatch(pCxt, pLogicNode, &info);
  if (TSDB_CODE_SUCCESS == code && (NULL != info.pDsoFuncs || NULL != info.pSdrFuncs)) {
    info.pScan->dataRequired = osdGetDataRequired(info.pSdrFuncs);
    info.pScan->pDynamicScanFuncs = info.pDsoFuncs;
    OPTIMIZE_FLAG_SET_MASK(info.pScan->node.optimizedFlag, OPTIMIZE_FLAG_OSD);
    pCxt->optimized = true;
  }
  nodesDestroyList(info.pSdrFuncs);
  return code;
}

static int32_t cpdOptimizeScanCondition(SOptimizeContext* pCxt, SScanLogicNode* pScan) {
  // todo
  return TSDB_CODE_SUCCESS;
}

static bool belongThisTable(SNode* pCondCol, SNodeList* pTableCols) {
  SNode* pTableCol = NULL;
  FOREACH(pTableCol, pTableCols) {
    if (nodesEqualNode(pCondCol, pTableCol)) {
      return true;
    }
  }
  return false;
}

static EDealRes cpdIsMultiTableCondImpl(SNode* pNode, void* pContext) {
  SCpdIsMultiTableCondCxt* pCxt = pContext;
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    if (belongThisTable(pNode, pCxt->pLeftCols)) {
      pCxt->havaLeftCol = true;
    } else if (belongThisTable(pNode, pCxt->pRightCols)) {
      pCxt->haveRightCol = true;
    }
    return pCxt->havaLeftCol && pCxt->haveRightCol ? DEAL_RES_END : DEAL_RES_CONTINUE;
  }
  return DEAL_RES_CONTINUE;
}

static ECondAction cpdCondAction(EJoinType joinType, SNodeList* pLeftCols, SNodeList* pRightCols, SNode* pNode) {
  SCpdIsMultiTableCondCxt cxt = { .pLeftCols = pLeftCols, .pRightCols = pRightCols, .havaLeftCol = false, .haveRightCol = false };
  nodesWalkExpr(pNode, cpdIsMultiTableCondImpl, &cxt);
  return (JOIN_TYPE_INNER != joinType ? COND_ACTION_STAY :
      (cxt.havaLeftCol && cxt.haveRightCol ? COND_ACTION_PUSH_JOIN : (cxt.havaLeftCol ? COND_ACTION_PUSH_LEFT_CHILD : COND_ACTION_PUSH_RIGHT_CHILD)));
}

static int32_t cpdMakeCond(SNodeList** pConds, SNode** pCond) {
  if (NULL == *pConds) {
    return TSDB_CODE_SUCCESS;
  }

  if (1 == LIST_LENGTH(*pConds)) {
    *pCond = nodesListGetNode(*pConds, 0);
    nodesClearList(*pConds);
  } else {
    SLogicConditionNode* pLogicCond = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION);
    if (NULL == pLogicCond) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    pLogicCond->condType = LOGIC_COND_TYPE_AND;
    pLogicCond->pParameterList = *pConds;
    *pCond = (SNode*)pLogicCond;
  }
  *pConds = NULL;

  return TSDB_CODE_SUCCESS;
}

static int32_t cpdPartitionLogicCond(SJoinLogicNode* pJoin, SNode** pOnCond, SNode** pLeftChildCond, SNode** pRightChildCond) {
  SLogicConditionNode* pLogicCond = (SLogicConditionNode*)pJoin->node.pConditions;
  if (LOGIC_COND_TYPE_AND != pLogicCond->condType) {
    return TSDB_CODE_SUCCESS;
  }

  SNodeList* pLeftCols = ((SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0))->pTargets;
  SNodeList* pRightCols = ((SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1))->pTargets;
  int32_t code = TSDB_CODE_SUCCESS;

  SNodeList* pOnConds = NULL;
  SNodeList* pLeftChildConds = NULL;
  SNodeList* pRightChildConds = NULL;
  SNodeList* pRemainConds = NULL;
  SNode* pCond = NULL;
  FOREACH(pCond, pLogicCond->pParameterList) {
    ECondAction condAction = cpdCondAction(pJoin->joinType, pLeftCols, pRightCols, pCond);
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
    code = cpdMakeCond(&pOnConds, &pTempOnCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = cpdMakeCond(&pLeftChildConds, &pTempLeftChildCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = cpdMakeCond(&pRightChildConds, &pTempRightChildCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = cpdMakeCond(&pRemainConds, &pTempRemainCond);
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

static int32_t cpdPartitionOpCond(SJoinLogicNode* pJoin, SNode** pOnCond, SNode** pLeftChildCond, SNode** pRightChildCond) {
  SNodeList* pLeftCols = ((SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0))->pTargets;
  SNodeList* pRightCols = ((SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1))->pTargets;
  ECondAction condAction = cpdCondAction(pJoin->joinType, pLeftCols, pRightCols, pJoin->node.pConditions);
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

static int32_t cpdPartitionCond(SJoinLogicNode* pJoin, SNode** pOnCond, SNode** pLeftChildCond, SNode** pRightChildCond) {
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pJoin->node.pConditions)) {
    return cpdPartitionLogicCond(pJoin, pOnCond, pLeftChildCond, pRightChildCond);
  } else {
    return cpdPartitionOpCond(pJoin, pOnCond, pLeftChildCond, pRightChildCond);
  }
}

static int32_t cpdCondAppend(SOptimizeContext* pCxt, SNode** pCond, SNode** pAdditionalCond) {
  if (NULL == *pCond) {
    TSWAP(*pCond, *pAdditionalCond, SNode*);
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(*pCond)) {
    code = nodesListAppend(((SLogicConditionNode*)*pCond)->pParameterList, *pAdditionalCond);
    if (TSDB_CODE_SUCCESS == code) {
      *pAdditionalCond = NULL;
    }
  } else {
    SLogicConditionNode* pLogicCond = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION);
    if (NULL == pLogicCond) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    pLogicCond->condType = LOGIC_COND_TYPE_AND;
    code = nodesListMakeAppend(&pLogicCond->pParameterList, *pAdditionalCond);
    if (TSDB_CODE_SUCCESS == code) {
      *pAdditionalCond = NULL;
      code = nodesListMakeAppend(&pLogicCond->pParameterList, *pCond);
    }
    if (TSDB_CODE_SUCCESS == code) {
      *pCond = (SNode*)pLogicCond;
    } else {
      nodesDestroyNode(pLogicCond);
    }
  }
  return code;
}

static int32_t cpdPushCondToOnCond(SOptimizeContext* pCxt, SJoinLogicNode* pJoin, SNode** pCond) {
  return cpdCondAppend(pCxt, &pJoin->pOnConditions, pCond);
}

static int32_t cpdPushCondToScan(SOptimizeContext* pCxt, SScanLogicNode* pScan, SNode** pCond) {
  return cpdCondAppend(pCxt, &pScan->node.pConditions, pCond);
}

static int32_t cpdPushCondToChild(SOptimizeContext* pCxt, SLogicNode* pChild, SNode** pCond) {
  switch (nodeType(pChild)) {
    case QUERY_NODE_LOGIC_PLAN_SCAN:
      return cpdPushCondToScan(pCxt, (SScanLogicNode*)pChild, pCond);    
    default:
      break;
  }
  return TSDB_CODE_PLAN_INTERNAL_ERROR;
}

static int32_t cpdPushJoinCondition(SOptimizeContext* pCxt, SJoinLogicNode* pJoin) {
  if (NULL == pJoin->node.pConditions) {
    return TSDB_CODE_SUCCESS;
  }

  SNode* pOnCond = NULL;
  SNode* pLeftChildCond = NULL;
  SNode* pRightChildCond = NULL;
  int32_t code = cpdPartitionCond(pJoin, &pOnCond, &pLeftChildCond, &pRightChildCond);
  if (TSDB_CODE_SUCCESS == code && NULL != pOnCond) {
    code = cpdPushCondToOnCond(pCxt, pJoin, &pOnCond);
  }
  if (TSDB_CODE_SUCCESS == code && NULL != pLeftChildCond) {
    code = cpdPushCondToChild(pCxt, (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0), &pLeftChildCond);
  }
  if (TSDB_CODE_SUCCESS == code && NULL != pRightChildCond) {
    code = cpdPushCondToChild(pCxt, (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1), &pRightChildCond);
  }

  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pOnCond);
    nodesDestroyNode(pLeftChildCond);
    nodesDestroyNode(pRightChildCond);
  }

  return code;
}

static int32_t cpdPushAggCondition(SOptimizeContext* pCxt, SAggLogicNode* pAgg) {
  // todo
  return TSDB_CODE_SUCCESS;
}

static int32_t cpdPushCondition(SOptimizeContext* pCxt, SLogicNode* pLogicNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (nodeType(pLogicNode)) {
    case QUERY_NODE_LOGIC_PLAN_SCAN:
      code = cpdOptimizeScanCondition(pCxt, (SScanLogicNode*)pLogicNode);
      break;
    case QUERY_NODE_LOGIC_PLAN_JOIN:
      code = cpdPushJoinCondition(pCxt, (SJoinLogicNode*)pLogicNode);
      break;
    case QUERY_NODE_LOGIC_PLAN_AGG:
      code = cpdPushAggCondition(pCxt, (SAggLogicNode*)pLogicNode);
      break;
    default:
      break;
  }
  if (TSDB_CODE_SUCCESS == code) {
    SNode* pChild = NULL;
    FOREACH(pChild, pLogicNode->pChildren) {
      code = cpdPushCondition(pCxt, (SLogicNode*)pChild);
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    }
  }
  return code;
}

static int32_t cpdOptimize(SOptimizeContext* pCxt, SLogicNode* pLogicNode) {
  return cpdPushCondition(pCxt, pLogicNode);
}

static const SOptimizeRule optimizeRuleSet[] = {
  { .pName = "OptimizeScanData", .optimizeFunc = osdOptimize },
  { .pName = "ConditionPushDown", .optimizeFunc = cpdOptimize }
};

static const int32_t optimizeRuleNum = (sizeof(optimizeRuleSet) / sizeof(SOptimizeRule));

static int32_t applyOptimizeRule(SLogicNode* pLogicNode) {
  SOptimizeContext cxt = { .optimized = false };
  do {
    cxt.optimized = false;
    for (int32_t i = 0; i < optimizeRuleNum; ++i) {
      int32_t code = optimizeRuleSet[i].optimizeFunc(&cxt, pLogicNode);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
    }
  } while (cxt.optimized);
  return TSDB_CODE_SUCCESS;
}

int32_t optimizeLogicPlan(SPlanContext* pCxt, SLogicNode* pLogicNode) {
  return applyOptimizeRule(pLogicNode);
}
