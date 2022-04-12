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

static bool osdMayBeOptimized(SLogicNode* pNode) {
  if (OPTIMIZE_FLAG_TEST_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_OSD)) {
    return false;
  }
  if (QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pNode)) {
    return false;
  }
  if (NULL == pNode->pParent || 
      (QUERY_NODE_LOGIC_PLAN_WINDOW != nodeType(pNode->pParent) && QUERY_NODE_LOGIC_PLAN_AGG == nodeType(pNode->pParent))) {
    return false;
  }
  return true;
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
    case FUNC_DATA_REQUIRED_ALL_NEEDED:
      return l;
    case FUNC_DATA_REQUIRED_STATIS_NEEDED:
      return FUNC_DATA_REQUIRED_ALL_NEEDED == r ? r : l;
    case FUNC_DATA_REQUIRED_NO_NEEDED:
      return FUNC_DATA_REQUIRED_DISCARD == r ? l : r;
    default:
      break;
  }
  return r;
}

static int32_t osdGetDataRequired(SNodeList* pFuncs) {
  if (NULL == pFuncs) {
    return FUNC_DATA_REQUIRED_ALL_NEEDED;
  }
  EFuncDataRequired dataRequired = FUNC_DATA_REQUIRED_DISCARD;
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

static const SOptimizeRule optimizeRuleSet[] = {
  { .pName = "OptimizeScanData", .optimizeFunc = osdOptimize }
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
