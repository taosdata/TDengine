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
#include "parser.h"

#define OPTIMIZE_FLAG_MASK(n) (1 << n)

#define OPTIMIZE_FLAG_SCAN_PATH       OPTIMIZE_FLAG_MASK(0)
#define OPTIMIZE_FLAG_PUSH_DOWN_CONDE OPTIMIZE_FLAG_MASK(1)
#define OPTIMIZE_FLAG_STB_JOIN        OPTIMIZE_FLAG_MASK(2)
#define OPTIMIZE_FLAG_ELIMINATE_PROJ  OPTIMIZE_FLAG_MASK(3)
#define OPTIMIZE_FLAG_JOIN_COND       OPTIMIZE_FLAG_MASK(4)

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

typedef struct SOptimizePKCtx {
  SNodeList* pList;
  int32_t    code;
} SOptimizePKCtx;

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
  bool       condIsNull;
} SCpdIsMultiTableCondCxt;

typedef struct SCpdIsMultiTableResCxt {
  SSHashObj* pLeftTbls;
  SSHashObj* pRightTbls;
  bool       haveLeftCol;
  bool       haveRightCol;
  bool       leftColOp;
  bool       rightColOp;
  bool       leftColNonNull;
  bool       rightColNonNull;
} SCpdIsMultiTableResCxt;

typedef struct SCpdCollRewriteTableColsCxt {
  int32_t    code;
  SSHashObj* pLeftTbls;
  SSHashObj* pRightTbls;
  SSHashObj* pLeftCols;
  SSHashObj* pRightCols;
} SCpdCollRewriteTableColsCxt;


typedef struct SCpdCollectTableColCxt {
  SSHashObj* pTables;
  SNodeList* pResCols;
  SHashObj*  pColHash;
  int32_t    errCode;
} SCpdCollectTableColCxt;


typedef enum ECondAction {
  COND_ACTION_STAY = 1,
  COND_ACTION_PUSH_JOIN,
  COND_ACTION_PUSH_LEFT_CHILD,
  COND_ACTION_PUSH_RIGHT_CHILD
  // after supporting outer join, there are other possibilities
} ECondAction;

#define PUSH_DOWN_LEFT_FLT  (1 << 0)
#define PUSH_DOWN_RIGHT_FLT (1 << 1)
#define PUSH_DOWN_ON_COND   (1 << 2)
#define PUSH_DONW_FLT_COND  (PUSH_DOWN_LEFT_FLT | PUSH_DOWN_RIGHT_FLT)
#define PUSH_DOWN_ALL_COND  (PUSH_DOWN_LEFT_FLT | PUSH_DOWN_RIGHT_FLT | PUSH_DOWN_ON_COND)

typedef struct SJoinOptimizeOpt {
  int8_t pushDownFlag;
} SJoinOptimizeOpt;

typedef bool (*FMayBeOptimized)(SLogicNode* pNode, void* pCtx);
typedef bool (*FShouldBeOptimized)(SLogicNode* pNode, void* pInfo);

#if 0
static SJoinOptimizeOpt gJoinOpt[JOIN_TYPE_MAX_VALUE][JOIN_STYPE_MAX_VALUE] = {
           /* NONE                OUTER                  SEMI                  ANTI                   ANY                    ASOF                   WINDOW */
/*INNER*/  {{PUSH_DOWN_ALL_COND}, {0},                   {0},                  {0},                   {PUSH_DOWN_ALL_COND},  {0},                   {0}},
/*LEFT*/   {{0},                  {PUSH_DOWN_LEFT_FLT},  {PUSH_DOWN_ALL_COND}, {PUSH_DOWN_LEFT_FLT},  {PUSH_DOWN_LEFT_FLT},  {PUSH_DOWN_LEFT_FLT},  {PUSH_DOWN_LEFT_FLT}},
/*RIGHT*/  {{0},                  {PUSH_DOWN_RIGHT_FLT}, {PUSH_DOWN_ALL_COND}, {PUSH_DOWN_RIGHT_FLT}, {PUSH_DOWN_RIGHT_FLT}, {PUSH_DOWN_RIGHT_FLT}, {PUSH_DOWN_RIGHT_FLT}},
/*FULL*/   {{0},                  {0},                   {0},                  {0},                   {0},                   {0},                   {0}},
};
#else
static SJoinOptimizeOpt gJoinWhereOpt[JOIN_TYPE_MAX_VALUE][JOIN_STYPE_MAX_VALUE] = {
           /* NONE                OUTER                  SEMI                  ANTI                   ASOF                   WINDOW */
/*INNER*/  {{PUSH_DOWN_ALL_COND}, {0},                   {0},                  {0},                   {0},                   {0}},
/*LEFT*/   {{0},                  {PUSH_DOWN_LEFT_FLT},  {PUSH_DOWN_LEFT_FLT}, {PUSH_DOWN_LEFT_FLT},  {PUSH_DOWN_LEFT_FLT},  {PUSH_DOWN_LEFT_FLT}},
/*RIGHT*/  {{0},                  {PUSH_DOWN_RIGHT_FLT}, {PUSH_DOWN_RIGHT_FLT},{PUSH_DOWN_RIGHT_FLT}, {PUSH_DOWN_RIGHT_FLT}, {PUSH_DOWN_RIGHT_FLT}},
/*FULL*/   {{0},                  {0},                   {0},                  {0},                   {0},                   {0}},
};

static SJoinOptimizeOpt gJoinOnOpt[JOIN_TYPE_MAX_VALUE][JOIN_STYPE_MAX_VALUE] = {
           /* NONE                OUTER                  SEMI                  ANTI                   ASOF                   WINDOW */
/*INNER*/  {{PUSH_DONW_FLT_COND}, {0},                   {0},                  {0},                   {0},                   {0}},
/*LEFT*/   {{0},                  {PUSH_DOWN_RIGHT_FLT}, {PUSH_DONW_FLT_COND}, {PUSH_DOWN_RIGHT_FLT}, {0},                   {0}},
/*RIGHT*/  {{0},                  {PUSH_DOWN_LEFT_FLT},  {PUSH_DONW_FLT_COND}, {PUSH_DOWN_LEFT_FLT},  {0},                   {0}},
/*FULL*/   {{0},                  {0},                   {0},                  {0},                   {0},                   {0}},
};


#endif

static SLogicNode* optFindPossibleNode(SLogicNode* pNode, FMayBeOptimized func, void* pCtx) {
  if (func(pNode, pCtx)) {
    return pNode;
  }
  SNode* pChild;
  FOREACH(pChild, pNode->pChildren) {
    SLogicNode* pScanNode = optFindPossibleNode((SLogicNode*)pChild, func, pCtx);
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
    SFunctionNode* pFunc = NULL;
    int32_t code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
    if (NULL == pFunc) {
      *(int32_t*)pContext = code;
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
    case QUERY_NODE_LOGIC_PLAN_AGG:
    case QUERY_NODE_LOGIC_PLAN_SORT:
    case QUERY_NODE_LOGIC_PLAN_FILL:
      if (pNode == pNodeForcePropagate) {
        pNode->outputTsOrder = order;
        break;
      } else
        return;
    case QUERY_NODE_LOGIC_PLAN_JOIN:
      pNode->outputTsOrder = order;
      break;
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

static bool scanPathOptMayBeOptimized(SLogicNode* pNode, void* pCtx) {
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
    if (!pNode->pParent || QUERY_NODE_LOGIC_PLAN_WINDOW != nodeType(pNode->pParent) ||
        WINDOW_TYPE_INTERVAL == ((SWindowLogicNode*)pNode->pParent)->winType)
      return !scanPathOptHaveNormalCol(((SPartitionLogicNode*)pNode)->pPartitionKeys);
    return false;
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
      SNode* pNew = NULL;
      code = nodesCloneNode(pNode, &pNew);
      if (TSDB_CODE_SUCCESS == code) {
        code = nodesListMakeStrictAppend(&pTmpSdrFuncs, pNew);
      }
    } else if (scanPathOptIsSpecifiedFuncType(pFunc, fmIsDynamicScanOptimizedFunc)) {
      SNode* pNew = NULL;
      code = nodesCloneNode(pNode, &pNew);
      if (TSDB_CODE_SUCCESS == code) {
        code = nodesListMakeStrictAppend(&pTmpDsoFuncs, pNew);
      }
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
  pInfo->pScan = (SScanLogicNode*)optFindPossibleNode(pLogicNode, scanPathOptMayBeOptimized, NULL);
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
    if (pCxt->pPlanCxt->streamQuery) {
      info.pScan->dataRequired = FUNC_DATA_REQUIRED_DATA_LOAD; // always load all data for stream query
    } else {
      info.pScan->dataRequired = scanPathOptGetDataRequired(info.pSdrFuncs);
    }

    info.pScan->pDynamicScanFuncs = info.pDsoFuncs;
  }
  if (TSDB_CODE_SUCCESS == code && info.pScan) {
    OPTIMIZE_FLAG_SET_MASK(info.pScan->node.optimizedFlag, OPTIMIZE_FLAG_SCAN_PATH);
    pCxt->optimized = true;
  }
  nodesDestroyList(info.pSdrFuncs);
  return code;
}

static int32_t pdcMergeCondsToLogic(SNode** pDst, SNode** pSrc) {
  SLogicConditionNode* pLogicCond = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, (SNode**)&pLogicCond);
  if (NULL == pLogicCond) {
    return code;
  }
  pLogicCond->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pLogicCond->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
  pLogicCond->condType = LOGIC_COND_TYPE_AND;
  code = nodesListMakeAppend(&pLogicCond->pParameterList, *pSrc);
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

static int32_t pdcMergeConds(SNode** pCond, SNode** pAdditionalCond) {
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
    code = pdcMergeCondsToLogic(pCond, pAdditionalCond);
  }
  return code;
}

static int32_t pushDownCondOptCalcTimeRange(SOptimizeContext* pCxt, SScanLogicNode* pScan, SNode** pPrimaryKeyCond,
                                            SNode** pOtherCond) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (pCxt->pPlanCxt->topicQuery || pCxt->pPlanCxt->streamQuery) {
    code = pdcMergeConds(pOtherCond, pPrimaryKeyCond);
  } else {
    bool isStrict = false;
    code = filterGetTimeRange(*pPrimaryKeyCond, &pScan->scanRange, &isStrict);
    if (TSDB_CODE_SUCCESS == code) {
      if (isStrict) {
        nodesDestroyNode(*pPrimaryKeyCond);
      } else {
        code = pdcMergeConds(pOtherCond, pPrimaryKeyCond);
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

static int32_t pdcDealScan(SOptimizeContext* pCxt, SScanLogicNode* pScan) {
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

static bool pdcColBelongThisTable(SNode* pCondCol, SNodeList* pTableCols) {
  SNode* pTableCol = NULL;
  FOREACH(pTableCol, pTableCols) {
    if (QUERY_NODE_COLUMN == nodeType(pCondCol) && QUERY_NODE_COLUMN == nodeType(pTableCol)) {
      SColumnNode* pCondColNode = (SColumnNode*)pCondCol;
      SColumnNode* pTblColNode = (SColumnNode*)pTableCol;
      if (0 == strcmp(pCondColNode->tableAlias, pTblColNode->tableAlias) && 0 == strcmp(pCondColNode->colName, pTblColNode->colName)) {
        return true;
      }
    }
    
    if (nodesEqualNode(pCondCol, pTableCol)) {
      return true;
    }
  }
  return false;
}

static bool pdcJoinColInTableColList(SNode* pNode, SNodeList* pTableCols) {
  if (QUERY_NODE_COLUMN != nodeType(pNode)) {
    return false;
  }
  SColumnNode* pCol = (SColumnNode*)pNode;
  return pdcColBelongThisTable(pNode, pTableCols);
}

static bool pdcJoinColInTableList(SNode* pCondCol, SSHashObj* pTables) {
  SColumnNode* pTableCol = (SColumnNode*)pCondCol;
  if (NULL == tSimpleHashGet(pTables, pTableCol->tableAlias, strlen(pTableCol->tableAlias))) {
    return false;
  }
  return true;
}

static EDealRes pdcJoinIsCrossTableCond(SNode* pNode, void* pContext) {
  SCpdIsMultiTableCondCxt* pCxt = pContext;
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    if (pdcJoinColInTableList(pNode, pCxt->pLeftTbls)) {
      pCxt->havaLeftCol = true;
    } else if (pdcJoinColInTableList(pNode, pCxt->pRightTbls)) {
      pCxt->haveRightCol = true;
    }
    return pCxt->havaLeftCol && pCxt->haveRightCol ? DEAL_RES_END : DEAL_RES_CONTINUE;
  }
  return DEAL_RES_CONTINUE;
}

static ECondAction pdcJoinGetCondAction(SJoinLogicNode* pJoin, SSHashObj* pLeftTbls, SSHashObj* pRightTbls,
                                                SNode* pNode, bool whereCond) {
  EJoinType t = pJoin->joinType;   
  EJoinSubType s = pJoin->subType;
  SCpdIsMultiTableCondCxt cxt = {
      .pLeftTbls = pLeftTbls, .pRightTbls = pRightTbls, .havaLeftCol = false, .haveRightCol = false};
  nodesWalkExpr(pNode, pdcJoinIsCrossTableCond, &cxt);

  if (cxt.havaLeftCol) {
    if (cxt.haveRightCol) {
      if (whereCond && gJoinWhereOpt[t][s].pushDownFlag & PUSH_DOWN_ON_COND) {
        return COND_ACTION_PUSH_JOIN;
      }
      return COND_ACTION_STAY;
    }
    if ((whereCond && gJoinWhereOpt[t][s].pushDownFlag & PUSH_DOWN_LEFT_FLT) || (!whereCond && gJoinOnOpt[t][s].pushDownFlag & PUSH_DOWN_LEFT_FLT)) {
      return COND_ACTION_PUSH_LEFT_CHILD;
    }
    return COND_ACTION_STAY;
  }

  if (cxt.haveRightCol) {
    if ((whereCond && gJoinWhereOpt[t][s].pushDownFlag & PUSH_DOWN_RIGHT_FLT) || (!whereCond && gJoinOnOpt[t][s].pushDownFlag & PUSH_DOWN_RIGHT_FLT)) {
      return COND_ACTION_PUSH_RIGHT_CHILD;
    }
    return COND_ACTION_STAY;
  }

  return COND_ACTION_STAY;
}

static int32_t pdcJoinSplitLogicCond(SJoinLogicNode* pJoin, SNode** pSrcCond, SNode** pOnCond, SNode** pLeftChildCond,
                                            SNode** pRightChildCond, bool whereCond) {
  SLogicConditionNode* pLogicCond = (SLogicConditionNode*)*pSrcCond;
  if (LOGIC_COND_TYPE_AND != pLogicCond->condType) {
    if (whereCond) {
      return TSDB_CODE_SUCCESS;
    }
    return TSDB_CODE_PLAN_NOT_SUPPORT_JOIN_COND;
  }

  int32_t    code = TSDB_CODE_SUCCESS;
  SSHashObj* pLeftTables = NULL;
  SSHashObj* pRightTables = NULL;
  code = collectTableAliasFromNodes(nodesListGetNode(pJoin->node.pChildren, 0), &pLeftTables);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  code = collectTableAliasFromNodes(nodesListGetNode(pJoin->node.pChildren, 1), &pRightTables);
  if (TSDB_CODE_SUCCESS != code) {
    tSimpleHashCleanup(pLeftTables);
    return code;
  }

  SNodeList* pOnConds = NULL;
  SNodeList* pLeftChildConds = NULL;
  SNodeList* pRightChildConds = NULL;
  SNodeList* pRemainConds = NULL;
  SNode*     pCond = NULL;
  FOREACH(pCond, pLogicCond->pParameterList) {
    ECondAction condAction = pdcJoinGetCondAction(pJoin, pLeftTables, pRightTables, pCond, whereCond);
    SNode* pNew = NULL;
    code = nodesCloneNode(pCond, &pNew);
    if (TSDB_CODE_SUCCESS != code) { break; }
    if (COND_ACTION_PUSH_JOIN == condAction && NULL != pOnCond) {
      code = nodesListMakeAppend(&pOnConds, pNew);
    } else if (COND_ACTION_PUSH_LEFT_CHILD == condAction) {
      code = nodesListMakeAppend(&pLeftChildConds, pNew);
    } else if (COND_ACTION_PUSH_RIGHT_CHILD == condAction) {
      code = nodesListMakeAppend(&pRightChildConds, pNew);
    } else {
      code = nodesListMakeAppend(&pRemainConds, pNew);
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
    if (pOnCond) {
      *pOnCond = pTempOnCond;
    }
    *pLeftChildCond = pTempLeftChildCond;
    *pRightChildCond = pTempRightChildCond;
    nodesDestroyNode(*pSrcCond);
    *pSrcCond = pTempRemainCond;
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

static int32_t pdcJoinSplitOpCond(SJoinLogicNode* pJoin, SNode** pSrcCond, SNode** pOnCond, SNode** pLeftChildCond,
                                         SNode** pRightChildCond, bool whereCond) {
  SSHashObj* pLeftTables = NULL;
  SSHashObj* pRightTables = NULL;
  int32_t code = collectTableAliasFromNodes(nodesListGetNode(pJoin->node.pChildren, 0), &pLeftTables);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  code = collectTableAliasFromNodes(nodesListGetNode(pJoin->node.pChildren, 1), &pRightTables);
  if (TSDB_CODE_SUCCESS != code) {
    tSimpleHashCleanup(pLeftTables);
    return code;
  }
  
  ECondAction condAction = pdcJoinGetCondAction(pJoin, pLeftTables, pRightTables, *pSrcCond, whereCond);

  tSimpleHashCleanup(pLeftTables);
  tSimpleHashCleanup(pRightTables);

  if (COND_ACTION_STAY == condAction || (COND_ACTION_PUSH_JOIN == condAction && NULL == pOnCond)) {
    return TSDB_CODE_SUCCESS;
  }

  if (COND_ACTION_PUSH_JOIN == condAction) {
    *pOnCond = *pSrcCond;
  } else if (COND_ACTION_PUSH_LEFT_CHILD == condAction) {
    *pLeftChildCond = *pSrcCond;
  } else if (COND_ACTION_PUSH_RIGHT_CHILD == condAction) {
    *pRightChildCond = *pSrcCond;
  }
  *pSrcCond = NULL;
  return TSDB_CODE_SUCCESS;
}

static int32_t pdcJoinSplitCond(SJoinLogicNode* pJoin, SNode** pSrcCond, SNode** pOnCond, SNode** pLeftChildCond,
                                       SNode** pRightChildCond, bool whereCond) {
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(*pSrcCond)) {
    return pdcJoinSplitLogicCond(pJoin, pSrcCond, pOnCond, pLeftChildCond, pRightChildCond, whereCond);
  } else {
    return pdcJoinSplitOpCond(pJoin, pSrcCond, pOnCond, pLeftChildCond, pRightChildCond, whereCond);
  }
}

static int32_t pdcJoinPushDownOnCond(SOptimizeContext* pCxt, SJoinLogicNode* pJoin, SNode** pCond) {
  return pdcMergeConds(&pJoin->pFullOnCond, pCond);
}

static int32_t pdcPushDownCondToChild(SOptimizeContext* pCxt, SLogicNode* pChild, SNode** pCond) {
  return pdcMergeConds(&pChild->pConditions, pCond);
}

static bool pdcJoinIsPrim(SNode* pNode, SSHashObj* pTables) {
  if (QUERY_NODE_COLUMN != nodeType(pNode) && QUERY_NODE_FUNCTION != nodeType(pNode)) {
    return false;
  }

  if (QUERY_NODE_FUNCTION == nodeType(pNode)) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    if (FUNCTION_TYPE_TIMETRUNCATE != pFunc->funcType) {
      return false;
    }
    SListCell* pCell = nodesListGetCell(pFunc->pParameterList, 0);
    if (NULL == pCell || NULL == pCell->pNode || QUERY_NODE_COLUMN != nodeType(pCell->pNode)) {
      return false;
    }
    pNode = pCell->pNode;
  }

  SColumnNode* pCol = (SColumnNode*)pNode;
  if (PRIMARYKEY_TIMESTAMP_COL_ID != pCol->colId || TSDB_SYSTEM_TABLE == pCol->tableType) {
    return false;
  }
  return pdcJoinColInTableList(pNode, pTables);
}

static bool pdcJoinIsPrimEqualCond(SJoinLogicNode* pJoin, SNode* pCond) {
  if (QUERY_NODE_OPERATOR != nodeType(pCond)) {
    return false;
  }

  SOperatorNode* pOper = (SOperatorNode*)pCond;
  if (OP_TYPE_EQUAL != pOper->opType) {
    if (JOIN_STYPE_ASOF != pJoin->subType) {
      return false;
    }
    if (OP_TYPE_GREATER_THAN != pOper->opType && OP_TYPE_GREATER_EQUAL != pOper->opType &&
        OP_TYPE_LOWER_THAN != pOper->opType && OP_TYPE_LOWER_EQUAL != pOper->opType) {
      return false;
    }
  }

  SSHashObj* pLeftTables = NULL;
  SSHashObj* pRightTables = NULL;
  int32_t code = collectTableAliasFromNodes(nodesListGetNode(pJoin->node.pChildren, 0), &pLeftTables);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  code = collectTableAliasFromNodes(nodesListGetNode(pJoin->node.pChildren, 1), &pRightTables);
  if (TSDB_CODE_SUCCESS != code) {
    tSimpleHashCleanup(pLeftTables);
    return code;
  }

  bool res = false;
  if (pdcJoinIsPrim(pOper->pLeft, pLeftTables)) {
    res = pdcJoinIsPrim(pOper->pRight, pRightTables);
  } else if (pdcJoinIsPrim(pOper->pLeft, pRightTables)) {
    res = pdcJoinIsPrim(pOper->pRight, pLeftTables);
  }

  tSimpleHashCleanup(pLeftTables);
  tSimpleHashCleanup(pRightTables);

  return res;
}

static bool pdcJoinHasPrimEqualCond(SJoinLogicNode* pJoin, SNode* pCond, bool* errCond) {
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
      if (pdcJoinHasPrimEqualCond(pJoin, pCond, NULL)) {
        hasPrimaryKeyEqualCond = true;
        break;
      }
    }
    return hasPrimaryKeyEqualCond;
  } else {
    return pdcJoinIsPrimEqualCond(pJoin, pCond);
  }
}

static int32_t pdcJoinSplitPrimInLogicCond(SJoinLogicNode* pJoin, SNode** ppPrimEqCond, SNode** ppOnCond) {
  SLogicConditionNode* pLogicCond = (SLogicConditionNode*)(pJoin->pFullOnCond);

  int32_t    code = TSDB_CODE_SUCCESS;
  SNodeList* pOnConds = NULL;
  SNode*     pCond = NULL;
  WHERE_EACH(pCond, pLogicCond->pParameterList) {
    SNode* pNew = NULL;
    code = nodesCloneNode(pCond, &pNew);
    if (TSDB_CODE_SUCCESS != code) break;
    if (pdcJoinIsPrimEqualCond(pJoin, pCond) && (NULL == *ppPrimEqCond)) {
      *ppPrimEqCond = pNew;
      ERASE_NODE(pLogicCond->pParameterList);
    } else {
      code = nodesListMakeAppend(&pOnConds, pNew);
      if (TSDB_CODE_SUCCESS != code) break;
      WHERE_NEXT;
    }
  }

  SNode* pTempOnCond = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesMergeConds(&pTempOnCond, &pOnConds);
  }

  if (TSDB_CODE_SUCCESS == code) {
    if (NULL != *ppPrimEqCond) {
      *ppOnCond = pTempOnCond;
      nodesDestroyNode(pJoin->pFullOnCond);
      pJoin->pFullOnCond = NULL;
      return TSDB_CODE_SUCCESS;
    }
    planError("no primary key equal cond found, condListNum:%d", pLogicCond->pParameterList->length);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  } else {
    nodesDestroyList(pOnConds);
    nodesDestroyNode(pTempOnCond);
    return code;
  }
}

static int32_t pdcJoinSplitPrimEqCond(SOptimizeContext* pCxt, SJoinLogicNode* pJoin) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pPrimKeyEqCond = NULL;
  SNode*  pJoinOnCond = NULL;

  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pJoin->pFullOnCond) &&
      LOGIC_COND_TYPE_AND == ((SLogicConditionNode*)(pJoin->pFullOnCond))->condType) {
    code = pdcJoinSplitPrimInLogicCond(pJoin, &pPrimKeyEqCond, &pJoinOnCond);
  } else if (pdcJoinIsPrimEqualCond(pJoin, pJoin->pFullOnCond)) {
    pPrimKeyEqCond = pJoin->pFullOnCond;
    pJoinOnCond = NULL;
  } else {
    return TSDB_CODE_SUCCESS;
  }

  if (TSDB_CODE_SUCCESS == code) {
    pJoin->pPrimKeyEqCond = pPrimKeyEqCond;
    pJoin->pFullOnCond = pJoinOnCond;
  } else {
    nodesDestroyNode(pPrimKeyEqCond);
    nodesDestroyNode(pJoinOnCond);
  }
  return code;
}

static int32_t pdcJoinIsEqualOnCond(SJoinLogicNode* pJoin, SNode* pCond, bool* allTags, bool* pRes) {
  *pRes = false;
  int32_t code = 0;
  if (QUERY_NODE_OPERATOR != nodeType(pCond)) {
    return code;
  }
  SOperatorNode* pOper = (SOperatorNode*)pCond;
  if ((QUERY_NODE_COLUMN != nodeType(pOper->pLeft) && !(QUERY_NODE_OPERATOR == nodeType(pOper->pLeft) && OP_TYPE_JSON_GET_VALUE ==((SOperatorNode*)pOper->pLeft)->opType)) 
      || NULL == pOper->pRight || 
      (QUERY_NODE_COLUMN != nodeType(pOper->pRight) && !(QUERY_NODE_OPERATOR == nodeType(pOper->pRight) && OP_TYPE_JSON_GET_VALUE ==((SOperatorNode*)pOper->pRight)->opType))) {
    return code;
  }
  
  if (OP_TYPE_EQUAL != pOper->opType) {
    return code;
  }

  if ((QUERY_NODE_OPERATOR == nodeType(pOper->pLeft) || QUERY_NODE_OPERATOR == nodeType(pOper->pRight)) &&
      !(IS_ASOF_JOIN(pJoin->subType) || IS_WINDOW_JOIN(pJoin->subType))) {
    return code;
  }
  
  SColumnNode* pLeft = (SColumnNode*)(pOper->pLeft);
  SColumnNode* pRight = (SColumnNode*)(pOper->pRight);

  if (QUERY_NODE_OPERATOR == nodeType(pOper->pLeft)) {
    pLeft = (SColumnNode*)((SOperatorNode*)pOper->pLeft)->pLeft;
  }

  if (QUERY_NODE_OPERATOR == nodeType(pOper->pRight)) {
    pRight = (SColumnNode*)((SOperatorNode*)pOper->pRight)->pLeft;
  }
  
  *allTags = (COLUMN_TYPE_TAG == pLeft->colType) && (COLUMN_TYPE_TAG == pRight->colType);

  if (pLeft->node.resType.type != pRight->node.resType.type ||
      pLeft->node.resType.bytes != pRight->node.resType.bytes) {
    return code;
  }
  SNodeList* pLeftCols = ((SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0))->pTargets;
  SNodeList* pRightCols = ((SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1))->pTargets;
  bool isEqual = false;
  if (pdcJoinColInTableColList((SNode*)pLeft, pLeftCols)) {
    isEqual = pdcJoinColInTableColList((SNode*)pRight, pRightCols);
    if (isEqual) {
      SNode* pNewLeft = NULL;
      code = nodesCloneNode(pOper->pLeft, &pNewLeft);
      if (TSDB_CODE_SUCCESS != code) return code;
      SNode* pNewRight = NULL;
      code = nodesCloneNode(pOper->pRight, &pNewRight);
      if (TSDB_CODE_SUCCESS != code) {
        nodesDestroyNode(pNewLeft);
        return code;
      }
      code = nodesListMakeStrictAppend(&pJoin->pLeftEqNodes, pNewLeft);
      if (TSDB_CODE_SUCCESS != code) {
        nodesDestroyNode(pNewRight);
        return code;
      }
      code = nodesListMakeStrictAppend(&pJoin->pRightEqNodes, pNewRight);
    }
  } else if (pdcJoinColInTableColList((SNode*)pLeft, pRightCols)) {
    isEqual = pdcJoinColInTableColList((SNode*)pRight, pLeftCols);
    if (isEqual) {
      SNode* pNewLeft = NULL;
      code = nodesCloneNode(pOper->pLeft, &pNewLeft);
      if (TSDB_CODE_SUCCESS != code) return code;
      SNode* pNewRight = NULL;
      code = nodesCloneNode(pOper->pRight, &pNewRight);
      if (TSDB_CODE_SUCCESS != code) {
        nodesDestroyNode(pNewLeft);
        return code;
      }
      code = nodesListMakeStrictAppend(&pJoin->pLeftEqNodes, pNewRight);
      if (TSDB_CODE_SUCCESS != code) {
        nodesDestroyNode(pNewLeft);
        return code;
      }
      code = nodesListMakeStrictAppend(&pJoin->pRightEqNodes, pNewLeft);
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    *pRes = isEqual;
  }
  return code;
}

static int32_t pdcJoinPartLogicEqualOnCond(SJoinLogicNode* pJoin) {
  SLogicConditionNode* pLogicCond = (SLogicConditionNode*)(pJoin->pFullOnCond);

  int32_t    code = TSDB_CODE_SUCCESS;
  SNodeList* pColEqOnConds = NULL;
  SNodeList* pTagEqOnConds = NULL;
  SNodeList* pTagOnConds = NULL;
  SNodeList* pColOnConds = NULL;
  SNode*     pCond = NULL;
  bool       allTags = false;
  FOREACH(pCond, pLogicCond->pParameterList) {
    allTags = false;
    bool   eqOnCond = false;
    SNode* pNew = NULL;
    code = pdcJoinIsEqualOnCond(pJoin, pCond, &allTags, &eqOnCond);
    if (TSDB_CODE_SUCCESS != code) break;
    code = nodesCloneNode(pCond, &pNew);
    if (TSDB_CODE_SUCCESS != code) break;

    if (eqOnCond) {
      if (allTags) {
        code = nodesListMakeStrictAppend(&pTagEqOnConds, pNew);
      } else {
        code = nodesListMakeStrictAppend(&pColEqOnConds, pNew);
        pJoin->allEqTags = false;  
      }
    } else if (allTags) {
      code = nodesListMakeStrictAppend(&pTagOnConds, pNew);
    } else {
      code = nodesListMakeStrictAppend(&pColOnConds, pNew);
      pJoin->allEqTags = false;  
    }
    
    if (code) {
      break;
    }
  }

  SNode* pTempTagEqCond = NULL;
  SNode* pTempColEqCond = NULL;
  SNode* pTempTagOnCond = NULL;
  SNode* pTempColOnCond = NULL;
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
    code = nodesMergeConds(&pTempColOnCond, &pColOnConds);
  }

  if (TSDB_CODE_SUCCESS == code) {
    pJoin->pColEqCond = pTempColEqCond;
    pJoin->pColOnCond = pTempColOnCond;
    pJoin->pTagEqCond = pTempTagEqCond;
    pJoin->pTagOnCond = pTempTagOnCond;
  } else {
    nodesDestroyList(pColEqOnConds);
    nodesDestroyList(pTagEqOnConds);
    nodesDestroyList(pColOnConds);
    nodesDestroyList(pTagOnConds);
    nodesDestroyNode(pTempTagEqCond);
    nodesDestroyNode(pTempColEqCond);
    nodesDestroyNode(pTempTagOnCond);
    nodesDestroyNode(pTempColOnCond);
  }
  
  return code;
}

static int32_t pdcJoinPartEqualOnCond(SOptimizeContext* pCxt, SJoinLogicNode* pJoin) {
  if (NULL == pJoin->pFullOnCond) {
    pJoin->pColEqCond = NULL;
    pJoin->pTagEqCond = NULL;
    return TSDB_CODE_SUCCESS;
  }

  pJoin->allEqTags = true;  

  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pJoin->pFullOnCond) &&
      LOGIC_COND_TYPE_AND == ((SLogicConditionNode*)(pJoin->pFullOnCond))->condType) {
    return pdcJoinPartLogicEqualOnCond(pJoin);
  }

  bool allTags = false;
  bool eqOnCond = false;
  int32_t code = pdcJoinIsEqualOnCond(pJoin, pJoin->pFullOnCond, &allTags, &eqOnCond);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  SNode* pNew = NULL;
  code = nodesCloneNode(pJoin->pFullOnCond, &pNew);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  if (eqOnCond) {
    if (allTags) {
      pJoin->pTagEqCond = pNew;
    } else {
      pJoin->pColEqCond = pNew;
      pJoin->allEqTags = false;  
    }
  } else if (allTags) {
    pJoin->pTagOnCond = pNew;
  } else {
    pJoin->pColOnCond = pNew;
    pJoin->allEqTags = false;  
  }

  return TSDB_CODE_SUCCESS;
}

static EDealRes pdcJoinCollectCondCol(SNode* pNode, void* pContext) {
  SCpdCollectTableColCxt* pCxt = pContext;
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    if (pdcJoinColInTableList(pNode, pCxt->pTables)) {
      SColumnNode* pCol = (SColumnNode*)pNode;
      char    name[TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN];
      int32_t len = 0;
      if ('\0' == pCol->tableAlias[0]) {
        len = tsnprintf(name, sizeof(name), "%s", pCol->colName);
      } else {
        len = tsnprintf(name, sizeof(name), "%s.%s", pCol->tableAlias, pCol->colName);
      }
      if (NULL == taosHashGet(pCxt->pColHash, name, len)) {
        pCxt->errCode = taosHashPut(pCxt->pColHash, name, len, NULL, 0);
        if (TSDB_CODE_SUCCESS == pCxt->errCode) {
          SNode* pNew = NULL;
          pCxt->errCode = nodesCloneNode(pNode, &pNew);
          if (TSDB_CODE_SUCCESS == pCxt->errCode) {
            pCxt->errCode = nodesListStrictAppend(pCxt->pResCols, pNew);
          }
        }
      }
    }
  }
  
  return (TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_CONTINUE : DEAL_RES_ERROR);
}

static int32_t pdcJoinCollectColsFromParent(SJoinLogicNode* pJoin, SSHashObj* pTables, SNodeList* pCondCols) {
  SCpdCollectTableColCxt cxt = {
    .errCode = TSDB_CODE_SUCCESS,
    .pTables = pTables, 
    .pResCols = pCondCols, 
    .pColHash = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK)
  };
  if (NULL == cxt.pColHash) {
    return terrno;
  }
  
  nodesWalkExpr(pJoin->pPrimKeyEqCond, pdcJoinCollectCondCol, &cxt);
  nodesWalkExpr(pJoin->node.pConditions, pdcJoinCollectCondCol, &cxt);
  if (TSDB_CODE_SUCCESS == cxt.errCode) {
    nodesWalkExpr(pJoin->pFullOnCond, pdcJoinCollectCondCol, &cxt);
  }
  
  taosHashCleanup(cxt.pColHash);
  return cxt.errCode;
}

static int32_t pdcJoinAddParentOnColsToTarget(SOptimizeContext* pCxt, SJoinLogicNode* pJoin) {
  if (NULL == pJoin->node.pParent || QUERY_NODE_LOGIC_PLAN_JOIN != nodeType(pJoin->node.pParent)) {
    return TSDB_CODE_SUCCESS;
  }

  SNodeList* pTargets = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  SNodeList* pCondCols = NULL;
  code = nodesMakeList(&pCondCols);
  if (NULL == pCondCols) {
    return code;
  }

  SSHashObj* pTables = NULL;
  code = collectTableAliasFromNodes(nodesListGetNode(pJoin->node.pChildren, 0), &pTables);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  code = collectTableAliasFromNodes(nodesListGetNode(pJoin->node.pChildren, 1), &pTables);
  if (TSDB_CODE_SUCCESS != code) {
    tSimpleHashCleanup(pTables);
    return code;
  }
  
  SJoinLogicNode* pTmp = (SJoinLogicNode*)pJoin->node.pParent;
  do {
    code = pdcJoinCollectColsFromParent(pTmp, pTables, pCondCols);
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
    if (NULL == pTmp->node.pParent || QUERY_NODE_LOGIC_PLAN_JOIN != nodeType(pTmp->node.pParent)) {
      break;
    }
    pTmp = (SJoinLogicNode*)pTmp->node.pParent;
  } while (true);

  tSimpleHashCleanup(pTables);
  
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
        SNode* pNew = NULL;
        code = nodesCloneNode(pNode, &pNew);
        if (TSDB_CODE_SUCCESS == code) {
          code = nodesListStrictAppend(pJoin->node.pTargets, pNew);
        }
        if (TSDB_CODE_SUCCESS != code) {
          break;
        }
      }
    }
  }    

  nodesDestroyList(pTargets);

  return code;
}


static int32_t pdcJoinAddPreFilterColsToTarget(SOptimizeContext* pCxt, SJoinLogicNode* pJoin) {
  if (NULL == pJoin->pFullOnCond) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  SNodeList* pCondCols = NULL;
  code = nodesMakeList(&pCondCols);
  SNodeList* pTargets = NULL;
  if (NULL == pCondCols) {
    code = code;
  } else {
    code = nodesCollectColumnsFromNode(pJoin->pColOnCond, NULL, COLLECT_COL_TYPE_ALL, &pCondCols);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesCollectColumnsFromNode(pJoin->pTagOnCond, NULL, COLLECT_COL_TYPE_ALL, &pCondCols);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExprs(pCondCols, &pTargets);
  }

  nodesDestroyList(pCondCols);

  if (TSDB_CODE_SUCCESS == code) {
    SNode* pNode = NULL;
    FOREACH(pNode, pTargets) {
      SNode* pTmp = NULL;
      bool   found = false;
      FOREACH(pTmp, pJoin->node.pTargets) {
        if (nodesEqualNode(pTmp, pNode)) {
          found = true;
          break;
        }
      }
      if (!found) {
        SNode* pNew = NULL;
        code = nodesCloneNode(pNode, &pNew);
        if (TSDB_CODE_SUCCESS == code) {
          code = nodesListStrictAppend(pJoin->node.pTargets, pNew);
        }
        if (TSDB_CODE_SUCCESS != code) {
          break;
        }
      }
    }
  }

  nodesDestroyList(pTargets);

  return code;
}

static int32_t pdcJoinAddFilterColsToTarget(SOptimizeContext* pCxt, SJoinLogicNode* pJoin) {
  if (NULL == pJoin->node.pConditions && NULL == pJoin->pFullOnCond) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  SNodeList* pCondCols = NULL;
  code = nodesMakeList(&pCondCols);
  SNodeList* pTargets = NULL;
  if (NULL == pCondCols) {
    return code;
  }

  if (NULL != pJoin->node.pConditions) {
    code = nodesCollectColumnsFromNode(pJoin->node.pConditions, NULL, COLLECT_COL_TYPE_ALL, &pCondCols);
  }
  if (TSDB_CODE_SUCCESS == code && NULL != pJoin->pFullOnCond) {
    code = nodesCollectColumnsFromNode(pJoin->pFullOnCond, NULL, COLLECT_COL_TYPE_ALL, &pCondCols);
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
        SNode* pNew = NULL;
        code = nodesCloneNode(pNode, &pNew);
        if (TSDB_CODE_SUCCESS == code) {
          code = nodesListStrictAppend(pJoin->node.pTargets, pNew);
        }
        if (TSDB_CODE_SUCCESS != code) {
          break;
        }
      }
    }
  }    

  nodesDestroyList(pTargets);

  return code;
}


static int32_t pdcJoinCheckAllCond(SOptimizeContext* pCxt, SJoinLogicNode* pJoin) {
  if (NULL == pJoin->pFullOnCond) {
    if (IS_WINDOW_JOIN(pJoin->subType) || IS_ASOF_JOIN(pJoin->subType)) {
      return TSDB_CODE_SUCCESS;
    }

    if (NULL == pJoin->node.pConditions) {      
      return generateUsageErrMsg(pCxt->pPlanCxt->pMsg, pCxt->pPlanCxt->msgLen, TSDB_CODE_PLAN_NOT_SUPPORT_CROSS_JOIN);
    }
    
    if (!IS_INNER_NONE_JOIN(pJoin->joinType, pJoin->subType)) {
      return generateUsageErrMsg(pCxt->pPlanCxt->pMsg, pCxt->pPlanCxt->msgLen, TSDB_CODE_PLAN_NOT_SUPPORT_CROSS_JOIN);
    }
  }

  SNode* pCond = pJoin->pFullOnCond ? pJoin->pFullOnCond : pJoin->node.pConditions;
  bool errCond = false;
  if (!pdcJoinHasPrimEqualCond(pJoin, pCond, &errCond)) {
    if (errCond && !(IS_INNER_NONE_JOIN(pJoin->joinType, pJoin->subType) && NULL != pJoin->pFullOnCond && NULL != pJoin->node.pConditions)) {
      return generateUsageErrMsg(pCxt->pPlanCxt->pMsg, pCxt->pPlanCxt->msgLen, TSDB_CODE_PLAN_NOT_SUPPORT_JOIN_COND);
    }
    
    if (IS_INNER_NONE_JOIN(pJoin->joinType, pJoin->subType) && NULL != pJoin->pFullOnCond && NULL != pJoin->node.pConditions) {
      if (pdcJoinHasPrimEqualCond(pJoin, pJoin->node.pConditions, &errCond)) {
        return TSDB_CODE_SUCCESS;
      }
      if (errCond) {
        return generateUsageErrMsg(pCxt->pPlanCxt->pMsg, pCxt->pPlanCxt->msgLen, TSDB_CODE_PLAN_NOT_SUPPORT_JOIN_COND);
      }
    }
    
    if (IS_WINDOW_JOIN(pJoin->subType) || IS_ASOF_JOIN(pJoin->subType)) {
      return TSDB_CODE_SUCCESS;
    }
    
    return generateUsageErrMsg(pCxt->pPlanCxt->pMsg, pCxt->pPlanCxt->msgLen, TSDB_CODE_PLAN_EXPECTED_TS_EQUAL);
  }

  if (IS_ASOF_JOIN(pJoin->subType)) {
    nodesDestroyNode(pJoin->addPrimEqCond);
    pJoin->addPrimEqCond = NULL;
  } 

  if (IS_WINDOW_JOIN(pJoin->subType)) {
    return generateUsageErrMsg(pCxt->pPlanCxt->pMsg, pCxt->pPlanCxt->msgLen, TSDB_CODE_PLAN_NOT_SUPPORT_JOIN_COND);
  } 

  return TSDB_CODE_SUCCESS;
}

static int32_t pdcJoinHandleGrpJoinCond(SOptimizeContext* pCxt, SJoinLogicNode* pJoin) {
  switch (pJoin->subType) {
    case JOIN_STYPE_ASOF:
    case JOIN_STYPE_WIN:
      if (NULL != pJoin->pColOnCond || NULL != pJoin->pTagOnCond) {
        return generateUsageErrMsg(pCxt->pPlanCxt->pMsg, pCxt->pPlanCxt->msgLen, TSDB_CODE_PLAN_NOT_SUPPORT_JOIN_COND);
      }
      nodesDestroyNode(pJoin->pColEqCond);
      pJoin->pColEqCond = NULL;
      nodesDestroyNode(pJoin->pTagEqCond);
      pJoin->pTagEqCond = NULL;
      nodesDestroyNode(pJoin->pFullOnCond);
      pJoin->pFullOnCond = NULL;
      if (!pJoin->allEqTags) {
        SNode* pNode = NULL;
        FOREACH(pNode, pJoin->pLeftEqNodes) {
          SColumnNode* pCol = (SColumnNode*)pNode;
          if (COLUMN_TYPE_TAG != pCol->colType && PRIMARYKEY_TIMESTAMP_COL_ID == pCol->colId) {
            return generateUsageErrMsg(pCxt->pPlanCxt->pMsg, pCxt->pPlanCxt->msgLen, TSDB_CODE_PLAN_NOT_SUPPORT_JOIN_COND);
          }
        }
        FOREACH(pNode, pJoin->pRightEqNodes) {
          SColumnNode* pCol = (SColumnNode*)pNode;
          if (COLUMN_TYPE_TAG != pCol->colType && PRIMARYKEY_TIMESTAMP_COL_ID == pCol->colId) {
            return generateUsageErrMsg(pCxt->pPlanCxt->pMsg, pCxt->pPlanCxt->msgLen, TSDB_CODE_PLAN_NOT_SUPPORT_JOIN_COND);
          }
        }
      }
      break;
    default:
      break;
  }

  return TSDB_CODE_SUCCESS;
}

static EDealRes pdcCheckTableCondType(SNode* pNode, void* pContext) {
  SCpdIsMultiTableCondCxt* pCxt = pContext;
  switch (nodeType(pNode)) {
    case QUERY_NODE_COLUMN: {
      if (pdcJoinColInTableList(pNode, pCxt->pLeftTbls)) {
        pCxt->havaLeftCol = true;
      } else if (pdcJoinColInTableList(pNode, pCxt->pRightTbls)) {
        pCxt->haveRightCol = true;
      }
      
      break;
    }
    case QUERY_NODE_OPERATOR: {
      SOperatorNode* pOp = (SOperatorNode*)pNode;
      if (OP_TYPE_IS_NULL == pOp->opType) {
        pCxt->condIsNull = true;
      }
      break;
    }
    default:
      break;
  }
  
  return DEAL_RES_CONTINUE;
}

static int32_t pdcJoinGetOpTableCondTypes(SNode* pCond, SSHashObj* pLeftTables, SSHashObj* pRightTables, bool* tableCondTypes) {
  SCpdIsMultiTableCondCxt cxt = {
      .pLeftTbls = pLeftTables, .pRightTbls = pRightTables, .havaLeftCol = false, .haveRightCol = false, .condIsNull = false};
  nodesWalkExpr(pCond, pdcCheckTableCondType, &cxt);

  if (cxt.havaLeftCol) {
    if (cxt.haveRightCol) {
      if (cxt.condIsNull) {
        tableCondTypes[1] = true;
        tableCondTypes[3] = true;
      } else {
        tableCondTypes[0] = true;
        tableCondTypes[2] = true;
      }      
      return TSDB_CODE_SUCCESS;
    }

    if (cxt.condIsNull) {
      tableCondTypes[1] = true;
    } else {
      tableCondTypes[0] = true;
    }

    return TSDB_CODE_SUCCESS;
  }

  if (cxt.haveRightCol) {
    if (cxt.condIsNull) {
      tableCondTypes[3] = true;
    } else {
      tableCondTypes[2] = true;
    }
  }

  return TSDB_CODE_SUCCESS;
}



static int32_t pdcJoinGetLogicTableCondTypes(SNode* pCond, SSHashObj* pLeftTables, SSHashObj* pRightTables, bool* tableCondTypes) {
  SLogicConditionNode* pLogicCond = (SLogicConditionNode*)pCond;
  int32_t code = 0;
  SNode* pSCond = NULL;
  FOREACH(pSCond, pLogicCond->pParameterList) {
    if (QUERY_NODE_LOGIC_CONDITION == nodeType(pSCond)) {
      code = pdcJoinGetLogicTableCondTypes(pSCond, pLeftTables, pRightTables, tableCondTypes);
    } else {
      code = pdcJoinGetOpTableCondTypes(pSCond, pLeftTables, pRightTables, tableCondTypes);
    }

    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }

  return code;
}

static int32_t pdcGetTableCondTypes(SNode* pCond, SSHashObj* pLeftTables, SSHashObj* pRightTables, bool* tableCondTypes) {
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pCond)) {
    return pdcJoinGetLogicTableCondTypes(pCond, pLeftTables, pRightTables, tableCondTypes);
  } else {
    return pdcJoinGetOpTableCondTypes(pCond, pLeftTables, pRightTables, tableCondTypes);
  }
}

static int32_t pdcRewriteTypeBasedOnConds(SOptimizeContext* pCxt, SJoinLogicNode* pJoin) {
  if (JOIN_TYPE_INNER == pJoin->joinType || JOIN_STYPE_OUTER != pJoin->subType) {
    return TSDB_CODE_SUCCESS;
  }

  SSHashObj* pLeftTables = NULL;
  SSHashObj* pRightTables = NULL;
  int32_t code = collectTableAliasFromNodes(nodesListGetNode(pJoin->node.pChildren, 0), &pLeftTables);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  code = collectTableAliasFromNodes(nodesListGetNode(pJoin->node.pChildren, 1), &pRightTables);
  if (TSDB_CODE_SUCCESS != code) {
    tSimpleHashCleanup(pLeftTables);
    return code;
  }
  
  bool tableCondTypes[4] = {0};
  code = pdcGetTableCondTypes(pJoin->node.pConditions, pLeftTables, pRightTables, tableCondTypes);
  tSimpleHashCleanup(pLeftTables);
  tSimpleHashCleanup(pRightTables);

  if (TSDB_CODE_SUCCESS != code) return code;

  switch (pJoin->joinType) {
    case JOIN_TYPE_LEFT:
      if (tableCondTypes[2] && !tableCondTypes[3]) {
        pJoin->joinType = JOIN_TYPE_INNER;
        pJoin->subType = JOIN_STYPE_NONE;
      }
      break;
    case JOIN_TYPE_RIGHT:
      if (tableCondTypes[0] && !tableCondTypes[1]) {
        pJoin->joinType = JOIN_TYPE_INNER;
        pJoin->subType = JOIN_STYPE_NONE;
      }
      break;
    case JOIN_TYPE_FULL:
      if (tableCondTypes[0] && !tableCondTypes[1]) {
        if (tableCondTypes[2] && !tableCondTypes[3]) {
          pJoin->joinType = JOIN_TYPE_INNER;
          pJoin->subType = JOIN_STYPE_NONE;
        } else {
          pJoin->joinType = JOIN_TYPE_LEFT;
        }
      } else if (tableCondTypes[2] && !tableCondTypes[3]) {
        pJoin->joinType = JOIN_TYPE_RIGHT;
      }
      break;
    default:
      break;
  }

  return code;
}

static EDealRes pdcCheckTableResType(SNode* pNode, void* pContext) {
  SCpdIsMultiTableResCxt* pCxt = pContext;
  switch (nodeType(pNode)) {
    case QUERY_NODE_COLUMN: {
      if (pdcJoinColInTableList(pNode, pCxt->pLeftTbls)) {
        pCxt->haveLeftCol = true;
      } else if (pdcJoinColInTableList(pNode, pCxt->pRightTbls)) {
        pCxt->haveRightCol = true;
      }
      break;
    }
    case QUERY_NODE_VALUE:
    case QUERY_NODE_GROUPING_SET:
      break;
    case QUERY_NODE_FUNCTION: {
      SFunctionNode* pFunc = (SFunctionNode*)pNode;
      SCpdIsMultiTableResCxt cxt = {.pLeftTbls = pCxt->pLeftTbls, .pRightTbls = pCxt->pRightTbls, 
        .haveLeftCol = false, .haveRightCol = false, .leftColNonNull = true, .rightColNonNull = true};
      
      nodesWalkExprs(pFunc->pParameterList, pdcCheckTableResType, &cxt);
      if (!cxt.leftColNonNull) {
        pCxt->leftColNonNull = false;
      }
      if (!cxt.rightColNonNull) {
        pCxt->rightColNonNull = false;
      }
      if (cxt.leftColOp) {
        pCxt->leftColOp = true;
      }
      if (cxt.rightColOp) {
        pCxt->rightColOp = true;
      }
      if (!cxt.haveLeftCol && !cxt.haveRightCol) {
        pCxt->leftColNonNull = false;
        pCxt->rightColNonNull = false;
        return DEAL_RES_END;
      } else if (!fmIsIgnoreNullFunc(pFunc->funcId)) {
        if (cxt.haveLeftCol) {
          pCxt->leftColNonNull = false;
        }
        if (cxt.haveRightCol) {
          pCxt->rightColNonNull = false;
        }
      } else {
        if (cxt.haveLeftCol) {
          pCxt->leftColOp = true;
        } else if (cxt.haveRightCol) {
          pCxt->rightColOp = true;
        }
      }
      if (!pCxt->leftColNonNull && !pCxt->rightColNonNull) {
        return DEAL_RES_END;
      }
      break;
    }
    default:
      pCxt->leftColNonNull = false;
      pCxt->rightColNonNull = false;
      return DEAL_RES_END;
  }
  
  return DEAL_RES_CONTINUE;
}

static int32_t pdcRewriteTypeBasedOnJoinRes(SOptimizeContext* pCxt, SJoinLogicNode* pJoin) {
  if (JOIN_TYPE_INNER == pJoin->joinType || JOIN_STYPE_OUTER != pJoin->subType) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = 0;
  SSHashObj* pLeftTables = NULL;
  SSHashObj* pRightTables = NULL;
  code = collectTableAliasFromNodes(nodesListGetNode(pJoin->node.pChildren, 0), &pLeftTables);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  code = collectTableAliasFromNodes(nodesListGetNode(pJoin->node.pChildren, 1), &pRightTables);
  if (TSDB_CODE_SUCCESS != code) {
    tSimpleHashCleanup(pLeftTables);
    return code;
  }

  SLogicNode* pParent = pJoin->node.pParent;
  bool tableResNonNull[2] = {true, true};
  bool tableResOp[2] = {false, false};
  if (QUERY_NODE_LOGIC_PLAN_AGG == nodeType(pParent)) {
    SAggLogicNode* pAgg = (SAggLogicNode*)pParent;
    if (NULL != pAgg->pGroupKeys) {
      tableResNonNull[0] = false;
      tableResNonNull[1] = false;
    } else {
      SCpdIsMultiTableResCxt cxt = {.pLeftTbls = pLeftTables, .pRightTbls = pRightTables, 
        .haveLeftCol = false, .haveRightCol = false, .leftColNonNull = true, .rightColNonNull = true, .leftColOp = false, .rightColOp = false};
      
      nodesWalkExprs(pAgg->pAggFuncs, pdcCheckTableResType, &cxt);
      if (!cxt.leftColNonNull) {
        tableResNonNull[0] = false;
      }
      if (!cxt.rightColNonNull) {
        tableResNonNull[1] = false;
      }
      if (cxt.leftColOp) {
        tableResOp[0] = true;
      }
      if (cxt.rightColOp) {
        tableResOp[1] = true;
      }
    }
  } else {
    tableResNonNull[0] = false;
    tableResNonNull[1] = false;
  }

  tSimpleHashCleanup(pLeftTables);
  tSimpleHashCleanup(pRightTables);

  switch (pJoin->joinType) {
    case JOIN_TYPE_LEFT:
      if (tableResNonNull[1] && !tableResOp[0]) {
        pJoin->joinType = JOIN_TYPE_INNER;
        pJoin->subType = JOIN_STYPE_NONE;
      }
      break;
    case JOIN_TYPE_RIGHT:
      if (tableResNonNull[0] && !tableResOp[1]) {
        pJoin->joinType = JOIN_TYPE_INNER;
        pJoin->subType = JOIN_STYPE_NONE;
      }
      break;
    case JOIN_TYPE_FULL:
      if (tableResNonNull[1] && !tableResOp[0]) {
        if (tableResNonNull[0] && !tableResOp[1]) {
          pJoin->joinType = JOIN_TYPE_INNER;
          pJoin->subType = JOIN_STYPE_NONE;
        } else {
          pJoin->joinType = JOIN_TYPE_RIGHT;
        }
      } else if (tableResNonNull[0] && !tableResOp[1]) {
        pJoin->joinType = JOIN_TYPE_LEFT;
      }
      break;
    default:
      break;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t pdcDealJoin(SOptimizeContext* pCxt, SJoinLogicNode* pJoin) {
  if (OPTIMIZE_FLAG_TEST_MASK(pJoin->node.optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE)) {
    return TSDB_CODE_SUCCESS;
  }
  if (pJoin->joinAlgo != JOIN_ALGO_UNKNOWN) {
    return TSDB_CODE_SUCCESS;
  }

  EJoinType t = pJoin->joinType;
  EJoinSubType s = pJoin->subType;
  SNode*  pOnCond = NULL;
  SNode*  pLeftChildCond = NULL;
  SNode*  pRightChildCond = NULL;
  int32_t code = pdcJoinCheckAllCond(pCxt, pJoin);
  while (true) {
    if (TSDB_CODE_SUCCESS == code && NULL != pJoin->node.pConditions) {
      if (0 != gJoinWhereOpt[t][s].pushDownFlag) {
        code = pdcJoinSplitCond(pJoin, &pJoin->node.pConditions, &pOnCond, &pLeftChildCond, &pRightChildCond, true);
        if (TSDB_CODE_SUCCESS == code && NULL != pOnCond) {
          code = pdcJoinPushDownOnCond(pCxt, pJoin, &pOnCond);
        }
        if (TSDB_CODE_SUCCESS == code && NULL != pLeftChildCond) {
          code = pdcPushDownCondToChild(pCxt, (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0), &pLeftChildCond);
        }
        if (TSDB_CODE_SUCCESS == code && NULL != pRightChildCond) {
          code = pdcPushDownCondToChild(pCxt, (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1), &pRightChildCond);
        }
      }
      if (TSDB_CODE_SUCCESS == code && NULL != pJoin->node.pConditions) {
        code = pdcRewriteTypeBasedOnConds(pCxt, pJoin);
      }
    }

    if (TSDB_CODE_SUCCESS == code) {
      code = pdcRewriteTypeBasedOnJoinRes(pCxt, pJoin);
    }
    
    if (TSDB_CODE_SUCCESS != code || t == pJoin->joinType) {
      break;
    }
    
    t = pJoin->joinType;
    s = pJoin->subType;
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pJoin->pFullOnCond && 0 != gJoinOnOpt[t][s].pushDownFlag) {
    code = pdcJoinSplitCond(pJoin, &pJoin->pFullOnCond, NULL, &pLeftChildCond, &pRightChildCond, false);
    if (TSDB_CODE_SUCCESS == code && NULL != pLeftChildCond) {
      code = pdcPushDownCondToChild(pCxt, (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0), &pLeftChildCond);
    }
    if (TSDB_CODE_SUCCESS == code && NULL != pRightChildCond) {
      code = pdcPushDownCondToChild(pCxt, (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1), &pRightChildCond);
    }
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pJoin->pFullOnCond && !IS_WINDOW_JOIN(pJoin->subType) && NULL == pJoin->addPrimEqCond) {
    code = pdcJoinSplitPrimEqCond(pCxt, pJoin);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = pdcJoinPartEqualOnCond(pCxt, pJoin);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = pdcJoinHandleGrpJoinCond(pCxt, pJoin);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = pdcJoinAddParentOnColsToTarget(pCxt, pJoin);
  }

  //if (TSDB_CODE_SUCCESS == code) {
  //  code = pdcJoinAddPreFilterColsToTarget(pCxt, pJoin);
  //}

  if (TSDB_CODE_SUCCESS == code) {
    code = pdcJoinAddFilterColsToTarget(pCxt, pJoin);
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
    SNode* pNew = NULL;
    code = nodesCloneNode(pCond, &pNew);
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
    if (partitionAggCondHasAggFunc(pAgg, pCond)) {
      code = nodesListMakeStrictAppend(&pAggFuncConds, pNew);
    } else {
      code = nodesListMakeStrictAppend(&pGroupKeyConds, pNew);
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
  return pdcMergeConds(&pAgg->node.pConditions, pAggFuncCond);
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
          SNode* pExpr = NULL;
          pCxt->errCode = nodesCloneNode(pGroup, &pExpr);
          if (pExpr == NULL) {
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

static int32_t pdcDealAgg(SOptimizeContext* pCxt, SAggLogicNode* pAgg) {
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
    code = pdcPushDownCondToChild(pCxt, pChild, &pGroupKeyCond);
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
            SNode* pExpr = NULL;
            pCxt->errCode = nodesCloneNode(pProjection, &pExpr);
            if (pExpr == NULL) {
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

static int32_t pdcDealProject(SOptimizeContext* pCxt, SProjectLogicNode* pProject) {
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
    code = pdcPushDownCondToChild(pCxt, pChild, &pProjCond);
  }

  if (TSDB_CODE_SUCCESS == code) {
    OPTIMIZE_FLAG_SET_MASK(pProject->node.optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE);
    pCxt->optimized = true;
  } else {
    nodesDestroyNode(pProjCond);
  }
  return code;
}

static int32_t pdcTrivialPushDown(SOptimizeContext* pCxt, SLogicNode* pLogicNode) {
  if (NULL == pLogicNode->pConditions ||
      OPTIMIZE_FLAG_TEST_MASK(pLogicNode->optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE)) {
    return TSDB_CODE_SUCCESS;
  }
  SLogicNode* pChild = (SLogicNode*)nodesListGetNode(pLogicNode->pChildren, 0);
  int32_t     code = pdcPushDownCondToChild(pCxt, pChild, &pLogicNode->pConditions);
  if (TSDB_CODE_SUCCESS == code) {
    OPTIMIZE_FLAG_SET_MASK(pLogicNode->optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE);
    pCxt->optimized = true;
  }
  return code;
}

static int32_t pdcOptimizeImpl(SOptimizeContext* pCxt, SLogicNode* pLogicNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (nodeType(pLogicNode)) {
    case QUERY_NODE_LOGIC_PLAN_SCAN:
      code = pdcDealScan(pCxt, (SScanLogicNode*)pLogicNode);
      break;
    case QUERY_NODE_LOGIC_PLAN_JOIN:
      code = pdcDealJoin(pCxt, (SJoinLogicNode*)pLogicNode);
      break;
    case QUERY_NODE_LOGIC_PLAN_AGG:
      code = pdcDealAgg(pCxt, (SAggLogicNode*)pLogicNode);
      break;
    case QUERY_NODE_LOGIC_PLAN_PROJECT:
      code = pdcDealProject(pCxt, (SProjectLogicNode*)pLogicNode);
      break;
    case QUERY_NODE_LOGIC_PLAN_SORT:
    case QUERY_NODE_LOGIC_PLAN_PARTITION:
      code = pdcTrivialPushDown(pCxt, pLogicNode);
      break;
    default:
      break;
  }
  if (TSDB_CODE_SUCCESS == code) {
    SNode* pChild = NULL;
    FOREACH(pChild, pLogicNode->pChildren) {
      code = pdcOptimizeImpl(pCxt, (SLogicNode*)pChild);
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    }
  }
  return code;
}

static int32_t pdcOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  return pdcOptimizeImpl(pCxt, pLogicSubplan->pNode);
}


static bool eliminateNotNullCondMayBeOptimized(SLogicNode* pNode, void* pCtx) {
  if (QUERY_NODE_LOGIC_PLAN_AGG != nodeType(pNode)) {
    return false;
  }
  
  SAggLogicNode* pAgg = (SAggLogicNode*)pNode;
  if (pNode->pChildren->length != 1 || NULL != pAgg->pGroupKeys) {
    return false;
  }

  SLogicNode* pChild = (SLogicNode*)nodesListGetNode(pAgg->node.pChildren, 0);
  if (QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pChild)) {
    return false;
  }

  SScanLogicNode* pScan = (SScanLogicNode*)pChild;
  if (NULL == pScan->node.pConditions || QUERY_NODE_OPERATOR != nodeType(pScan->node.pConditions)) {
    return false;
  }

  SOperatorNode* pOp = (SOperatorNode*)pScan->node.pConditions;
  if (OP_TYPE_IS_NOT_NULL != pOp->opType) {
    return false;
  }

  if (QUERY_NODE_COLUMN != nodeType(pOp->pLeft)) {
    return false;
  }

  SNode* pTmp = NULL;
  FOREACH(pTmp, pAgg->pAggFuncs) {
    SFunctionNode* pFunc = (SFunctionNode*)pTmp;
    if (!fmIsIgnoreNullFunc(pFunc->funcId)) {
      return false;
    }
    if (fmIsMultiResFunc(pFunc->funcId)) {
      SNode* pParam = NULL;
      FOREACH(pParam, pFunc->pParameterList) {
        if (QUERY_NODE_COLUMN != nodeType(pParam)) {
          return false;
        }
        if (!nodesEqualNode(pParam, pOp->pLeft)) {
          return false;
        }
      }
    } else {
      SNode* pParam = nodesListGetNode(pFunc->pParameterList, 0);
      if (QUERY_NODE_COLUMN != nodeType(pParam)) {
        return false;
      }
      if (!nodesEqualNode(pParam, pOp->pLeft)) {
        return false;
      }
    }
  }

  return true;
}

static int32_t eliminateNotNullCondOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SLogicNode* pNode = (SLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, eliminateNotNullCondMayBeOptimized, NULL);
  if (NULL == pNode) {
    return TSDB_CODE_SUCCESS;
  }
  
  SScanLogicNode* pScan = (SScanLogicNode*)nodesListGetNode(pNode->pChildren, 0);
  nodesDestroyNode(pScan->node.pConditions);
  pScan->node.pConditions = NULL;

  pCxt->optimized = true;

  return TSDB_CODE_SUCCESS;
}


static bool sortPriKeyOptIsPriKeyOrderBy(SNodeList* pSortKeys) {
  if (1 != LIST_LENGTH(pSortKeys)) {
    return false;
  }
  SNode* pNode = ((SOrderByExprNode*)nodesListGetNode(pSortKeys, 0))->pExpr;
  return (QUERY_NODE_COLUMN == nodeType(pNode) ? isPrimaryKeyImpl(pNode) : false);
}

static bool sortPriKeyOptMayBeOptimized(SLogicNode* pNode, void* pCtx) {
  if (QUERY_NODE_LOGIC_PLAN_SORT != nodeType(pNode)) {
    return false;
  }
  SSortLogicNode* pSort = (SSortLogicNode*)pNode;
  if (pSort->skipPKSortOpt || !sortPriKeyOptIsPriKeyOrderBy(pSort->pSortKeys) ||
      1 != LIST_LENGTH(pSort->node.pChildren)) {
    return false;
  }
  SNode* pChild = nodesListGetNode(pSort->node.pChildren, 0);
  if (QUERY_NODE_LOGIC_PLAN_JOIN == nodeType(pChild)) {
    SJoinLogicNode* pJoin = (SJoinLogicNode*)pChild;
    if (JOIN_TYPE_FULL == pJoin->joinType) {
      return false;
    }
  }
  
  FOREACH(pChild, pSort->node.pChildren) {
    SLogicNode* pSortDescendent = optFindPossibleNode((SLogicNode*)pChild, sortPriKeyOptMayBeOptimized, NULL);
    if (pSortDescendent != NULL) {
      return false;
    }
  }
  return true;
}

static int32_t sortPriKeyOptHandleLeftRightJoinSort(SJoinLogicNode* pJoin, SSortLogicNode* pSort, bool* pNotOptimize, bool* keepSort) {
  if (JOIN_STYPE_SEMI == pJoin->subType || JOIN_STYPE_NONE == pJoin->subType) {
    return TSDB_CODE_SUCCESS;
  }

  SSHashObj* pLeftTables = NULL;
  SSHashObj* pRightTables = NULL;
  bool sortByProbe = true;
/*  
  bool sortByLeft = true, sortByRight = true, sortByProbe = false;
  collectTableAliasFromNodes(nodesListGetNode(pJoin->node.pChildren, 0), &pLeftTables);
  collectTableAliasFromNodes(nodesListGetNode(pJoin->node.pChildren, 1), &pRightTables);

  SOrderByExprNode* pExprNode = (SOrderByExprNode*)nodesListGetNode(pSort->pSortKeys, 0);
  SColumnNode* pSortCol = (SColumnNode*)pExprNode->pExpr;
  if (NULL == tSimpleHashGet(pLeftTables, pSortCol->tableAlias, strlen(pSortCol->tableAlias))) {
    sortByLeft = false;
  }
  if (NULL == tSimpleHashGet(pRightTables, pSortCol->tableAlias, strlen(pSortCol->tableAlias))) {
    sortByRight = false;
  }

  tSimpleHashCleanup(pLeftTables);
  tSimpleHashCleanup(pRightTables);

  if (!sortByLeft && !sortByRight) {
    planError("sort by primary key not in any join subtable, tableAlias: %s", pSortCol->tableAlias);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  if (sortByLeft && sortByRight) {
    planError("sort by primary key in both join subtables, tableAlias: %s", pSortCol->tableAlias);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  if ((JOIN_TYPE_LEFT == pJoin->joinType && sortByLeft) || (JOIN_TYPE_RIGHT == pJoin->joinType && sortByRight)) {
    sortByProbe = true;
  }
*/
  switch (pJoin->subType) {
    case JOIN_STYPE_OUTER: {
      if (sortByProbe) {
        return TSDB_CODE_SUCCESS;
      }
    }
    case JOIN_STYPE_ANTI: {
      if (sortByProbe) {
        return TSDB_CODE_SUCCESS;
      }
    }
    case JOIN_STYPE_ASOF: 
    case JOIN_STYPE_WIN: {
      if (sortByProbe) {
        if (NULL != pJoin->pLeftEqNodes && pJoin->pLeftEqNodes->length > 0) {
          *pNotOptimize = true;
        }
        return TSDB_CODE_SUCCESS;
      }
    }
    default:
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  return TSDB_CODE_SUCCESS;
}


static int32_t sortPriKeyOptHandleJoinSort(SLogicNode* pNode, bool groupSort, SSortLogicNode* pSort,
                                                   bool* pNotOptimize, SNodeList** pSequencingNodes, bool* keepSort) {
  SJoinLogicNode* pJoin = (SJoinLogicNode*)pNode;
  int32_t code = TSDB_CODE_SUCCESS;

  switch (pJoin->joinType) {
    case JOIN_TYPE_LEFT:
    case JOIN_TYPE_RIGHT: {
      code = sortPriKeyOptHandleLeftRightJoinSort(pJoin, pSort, pNotOptimize, keepSort);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      if (*pNotOptimize || !(*keepSort)) {
        return TSDB_CODE_SUCCESS;
      }
      break;
    }
    default:
      break;
  }

  code = sortPriKeyOptGetSequencingNodesImpl((SLogicNode*)nodesListGetNode(pNode->pChildren, 0), groupSort,
                                                     pSort, pNotOptimize, pSequencingNodes, keepSort);
  if (TSDB_CODE_SUCCESS == code) {
    code = sortPriKeyOptGetSequencingNodesImpl((SLogicNode*)nodesListGetNode(pNode->pChildren, 1), groupSort,
                                               pSort, pNotOptimize, pSequencingNodes, keepSort);
  }

  return code;
}

                                                   
static EOrder sortPriKeyOptGetPriKeyOrder(SSortLogicNode* pSort) {
 return ((SOrderByExprNode*)nodesListGetNode(pSort->pSortKeys, 0))->order;
}


static bool sortPriKeyOptHasUnsupportedPkFunc(SLogicNode* pLogicNode, EOrder sortOrder) {
  if (sortOrder == ORDER_ASC) {
    return false;
  }

  SNodeList* pFuncList = NULL;
  switch (nodeType(pLogicNode)) {
    case QUERY_NODE_LOGIC_PLAN_AGG:
      pFuncList = ((SAggLogicNode*)pLogicNode)->pAggFuncs;
      break;
    case QUERY_NODE_LOGIC_PLAN_WINDOW:
      pFuncList = ((SWindowLogicNode*)pLogicNode)->pFuncs;
      break;
    case QUERY_NODE_LOGIC_PLAN_PARTITION:
      pFuncList = ((SPartitionLogicNode*)pLogicNode)->pAggFuncs;
      break;
    case QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC:
      pFuncList = ((SIndefRowsFuncLogicNode*)pLogicNode)->pFuncs;
      break;
    case QUERY_NODE_LOGIC_PLAN_INTERP_FUNC:
      pFuncList = ((SInterpFuncLogicNode*)pLogicNode)->pFuncs;
      break;
    case QUERY_NODE_LOGIC_PLAN_FORECAST_FUNC:
      pFuncList = ((SForecastFuncLogicNode*)pLogicNode)->pFuncs;
    default:
      break;
  }
  
  SNode* pNode = 0;
  FOREACH(pNode, pFuncList) {
    if (nodeType(pNode) != QUERY_NODE_FUNCTION) {
      continue;
    }
    SFunctionNode* pFuncNode = (SFunctionNode*)pLogicNode;
    if (pFuncNode->hasPk && 
        (pFuncNode->funcType == FUNCTION_TYPE_DIFF || 
         pFuncNode->funcType == FUNCTION_TYPE_DERIVATIVE || 
         pFuncNode->funcType == FUNCTION_TYPE_IRATE ||
         pFuncNode->funcType == FUNCTION_TYPE_TWA)) {
      return true;
    }
  }
  return false;
}

int32_t sortPriKeyOptGetSequencingNodesImpl(SLogicNode* pNode, bool groupSort, SSortLogicNode* pSort,
                                                   bool* pNotOptimize, SNodeList** pSequencingNodes, bool* keepSort) {
  EOrder sortOrder = sortPriKeyOptGetPriKeyOrder(pSort);
  if (sortPriKeyOptHasUnsupportedPkFunc(pNode, sortOrder)) {
    *pNotOptimize = true;
    return TSDB_CODE_SUCCESS;
  }

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
    case QUERY_NODE_LOGIC_PLAN_SORT: {
      *keepSort = true;
      NODES_CLEAR_LIST(*pSequencingNodes);      
      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_LOGIC_PLAN_JOIN: {
      return sortPriKeyOptHandleJoinSort(pNode, groupSort, pSort, pNotOptimize, pSequencingNodes, keepSort);
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

  return sortPriKeyOptGetSequencingNodesImpl((SLogicNode*)nodesListGetNode(pNode->pChildren, 0), groupSort, pSort,
                                             pNotOptimize, pSequencingNodes, keepSort);
}


static int32_t sortPriKeyOptGetSequencingNodes(SSortLogicNode* pSort, bool groupSort, SNodeList** pSequencingNodes, bool* keepSort) {
  bool    notOptimize = false;
  int32_t code =
      sortPriKeyOptGetSequencingNodesImpl((SLogicNode*)nodesListGetNode(pSort->node.pChildren, 0), groupSort,
                                          pSort, &notOptimize, pSequencingNodes, keepSort);
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
  bool keepSort = true;
  int32_t    code = sortPriKeyOptGetSequencingNodes(pSort, pSort->groupSort, &pSequencingNodes, &keepSort);
  if (TSDB_CODE_SUCCESS == code) {
    if (pSequencingNodes != NULL) {
      code = sortPriKeyOptApply(pCxt, pLogicSubplan, pSort, pSequencingNodes);
    } else if (!keepSort) {
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
  SSortLogicNode* pSort = (SSortLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, sortPriKeyOptMayBeOptimized, NULL);
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

  int32_t code = collectTableAliasFromNodes((SNode*)pChild, &pTables);  
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  if (NULL != tSimpleHashGet(pTables, ((SColumnNode*)pOp->pLeft)->tableAlias, strlen(((SColumnNode*)pOp->pLeft)->tableAlias))) {
    pOrderByNode = pOp->pLeft;
  } else if (NULL != tSimpleHashGet(pTables, ((SColumnNode*)pOp->pRight)->tableAlias, strlen(((SColumnNode*)pOp->pRight)->tableAlias))) {
    pOrderByNode = pOp->pRight;
  }

  tSimpleHashCleanup(pTables);

  if (NULL == pOrderByNode) {
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  SSortLogicNode* pSort = NULL;
  code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SORT, (SNode**)&pSort);
  if (NULL == pSort) {
    return code;
  }

  pSort->node.outputTsOrder = targetOrder;
  pSort->node.pTargets = NULL;
  code = nodesCloneList(pChild->pTargets, &pSort->node.pTargets);
  if (NULL == pSort->node.pTargets) {
    nodesDestroyNode((SNode *)pSort);
    return code;
  }
  
  pSort->groupSort = false;
  SOrderByExprNode* pOrder = NULL;
  code = nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR, (SNode**)&pOrder);
  if (NULL == pOrder) {
    nodesDestroyNode((SNode *)pSort);
    return code;
  }

  code = nodesListMakeStrictAppend(&pSort->pSortKeys, (SNode*)pOrder);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pSort);
    return code;
  }
  pOrder->order = targetOrder;
  pOrder->pExpr = NULL;
  pOrder->nullOrder = (ORDER_ASC == pOrder->order) ? NULL_ORDER_FIRST : NULL_ORDER_LAST;
  code = nodesCloneNode(pOrderByNode, &pOrder->pExpr);
  if (!pOrder->pExpr) {
    nodesDestroyNode((SNode *)pSort);
    return code;
  }

  pChild->pParent = (SLogicNode*)pSort;
  code = nodesListMakeAppend(&pSort->node.pChildren, (SNode*)pChild);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  *pChildPos = (SNode*)pSort;
  pSort->node.pParent = (SLogicNode*)pJoin;;

_return:

  pCxt->optimized = true;

  return TSDB_CODE_SUCCESS;
}


static bool sortForJoinOptMayBeOptimized(SLogicNode* pNode, void* pCtx) {
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
    pLeft->outputTsOrder = ORDER_ASC;
  }
  if (ORDER_ASC != pRight->outputTsOrder && ORDER_DESC != pRight->outputTsOrder) {
    pRight->outputTsOrder = ORDER_ASC;
  }

  if (pLeft->outputTsOrder == pRight->outputTsOrder) {
    return false;
  }

  return true;
}


static int32_t sortForJoinOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SJoinLogicNode* pJoin = (SJoinLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, sortForJoinOptMayBeOptimized, NULL);
  if (NULL == pJoin) {
    return TSDB_CODE_SUCCESS;
  }
  return sortForJoinOptimizeImpl(pCxt, pLogicSubplan, pJoin);
}

static SScanLogicNode* joinCondGetScanNode(SLogicNode* pNode) {
  switch (nodeType(pNode)) {
    case QUERY_NODE_LOGIC_PLAN_SCAN:
      return (SScanLogicNode*)pNode;
    case QUERY_NODE_LOGIC_PLAN_JOIN: {
      SJoinLogicNode* pJoin = (SJoinLogicNode*)pNode;
      if (JOIN_TYPE_INNER != pJoin->joinType) {
        return NULL;
      }
      return joinCondGetScanNode((SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0));
    }
    default:
      return NULL;
  }
}

static int32_t joinCondGetAllScanNodes(SLogicNode* pNode, SNodeList** pList) {
  switch (nodeType(pNode)) {
    case QUERY_NODE_LOGIC_PLAN_SCAN:
      return nodesListMakeStrictAppend(pList, (SNode*)pNode);
    case QUERY_NODE_LOGIC_PLAN_JOIN: {
      SJoinLogicNode* pJoin = (SJoinLogicNode*)pNode;
      if (JOIN_TYPE_INNER != pJoin->joinType) {
        return TSDB_CODE_SUCCESS;
      }
      int32_t code = joinCondGetAllScanNodes((SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0), pList);
      if (TSDB_CODE_SUCCESS == code) {
        code = joinCondGetAllScanNodes((SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1), pList);
      }
      return code;
    }
    default:
      return TSDB_CODE_SUCCESS;
  }
}


static bool joinCondMayBeOptimized(SLogicNode* pNode, void* pCtx) {
  if (QUERY_NODE_LOGIC_PLAN_JOIN != nodeType(pNode) || OPTIMIZE_FLAG_TEST_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_JOIN_COND)) {
    return false;
  }
  
  SJoinLogicNode* pJoin = (SJoinLogicNode*)pNode;
  if (pNode->pChildren->length != 2 || JOIN_STYPE_ASOF == pJoin->subType || JOIN_STYPE_WIN == pJoin->subType || JOIN_TYPE_FULL == pJoin->joinType) {
    OPTIMIZE_FLAG_SET_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_JOIN_COND);
    return false;
  }

  SLogicNode* pLeft = (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0);
  SLogicNode* pRight = (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1);

  if ((JOIN_TYPE_LEFT == pJoin->joinType || JOIN_TYPE_RIGHT == pJoin->joinType) && (QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pLeft) || QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pRight))) {
    OPTIMIZE_FLAG_SET_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_JOIN_COND);
    return false;
  }

  if (JOIN_TYPE_INNER == pJoin->joinType && ((QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pLeft) && QUERY_NODE_LOGIC_PLAN_JOIN != nodeType(pLeft)) || (QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pRight) && QUERY_NODE_LOGIC_PLAN_JOIN != nodeType(pRight)))) {
    OPTIMIZE_FLAG_SET_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_JOIN_COND);
    return false;
  }

  if (JOIN_TYPE_INNER == pJoin->joinType) {
    if (QUERY_NODE_LOGIC_PLAN_JOIN == nodeType(pLeft) && !OPTIMIZE_FLAG_TEST_MASK(pLeft->optimizedFlag, OPTIMIZE_FLAG_JOIN_COND)) {
      return false;
    }
    if (QUERY_NODE_LOGIC_PLAN_JOIN == nodeType(pRight) && !OPTIMIZE_FLAG_TEST_MASK(pRight->optimizedFlag, OPTIMIZE_FLAG_JOIN_COND)) {
      return false;
    }
  }

  SScanLogicNode* pLScan = joinCondGetScanNode(pLeft);
  SScanLogicNode* pRScan = joinCondGetScanNode(pRight);

  if (NULL == pLScan || NULL == pRScan) {
    OPTIMIZE_FLAG_SET_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_JOIN_COND);
    return false;
  }

  if (!IS_TSWINDOW_SPECIFIED(pLScan->scanRange) && !IS_TSWINDOW_SPECIFIED(pRScan->scanRange)) {
    OPTIMIZE_FLAG_SET_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_JOIN_COND);
    return false;
  }
  
  return true;
}

static void joinCondMergeScanRand(STimeWindow* pDst, STimeWindow* pSrc) {
  if (pSrc->skey > pDst->skey) {
    pDst->skey = pSrc->skey;
  }
  if (pSrc->ekey < pDst->ekey) {
    pDst->ekey = pSrc->ekey;
  }
}

static int32_t joinCondOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SJoinLogicNode* pJoin = (SJoinLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, joinCondMayBeOptimized, NULL);
  if (NULL == pJoin) {
    return TSDB_CODE_SUCCESS;
  }

  switch (pJoin->joinType) {
    case JOIN_TYPE_INNER: {
      SNodeList* pScanList = NULL;
      int32_t code = joinCondGetAllScanNodes((SLogicNode*)pJoin, &pScanList);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      if (NULL == pScanList || pScanList->length <= 0) {
        nodesClearList(pScanList);
        return TSDB_CODE_SUCCESS;
      }
      SNode* pNode = NULL;
      STimeWindow   scanRange = TSWINDOW_INITIALIZER;
      FOREACH(pNode, pScanList) {
        joinCondMergeScanRand(&scanRange, &((SScanLogicNode*)pNode)->scanRange);
      }
      FOREACH(pNode, pScanList) {
        ((SScanLogicNode*)pNode)->scanRange.skey = scanRange.skey;
        ((SScanLogicNode*)pNode)->scanRange.ekey = scanRange.ekey;
      }
      nodesClearList(pScanList);
      break;
    }
    case JOIN_TYPE_LEFT: {
      SLogicNode* pLeft = (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0);
      SLogicNode* pRight = (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1);
      SScanLogicNode* pLScan = joinCondGetScanNode(pLeft);
      SScanLogicNode* pRScan = joinCondGetScanNode(pRight);
      
      if (NULL == pLScan || NULL == pRScan) {
        return TSDB_CODE_SUCCESS;
      }
      joinCondMergeScanRand(&pRScan->scanRange, &pLScan->scanRange);
      break;
    }
    case JOIN_TYPE_RIGHT: {
      SLogicNode* pLeft = (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0);
      SLogicNode* pRight = (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1);
      SScanLogicNode* pLScan = joinCondGetScanNode(pLeft);
      SScanLogicNode* pRScan = joinCondGetScanNode(pRight);
      
      if (NULL == pLScan || NULL == pRScan) {
        return TSDB_CODE_SUCCESS;
      }
      joinCondMergeScanRand(&pLScan->scanRange, &pRScan->scanRange);
      break;
    }
    default:
      return TSDB_CODE_SUCCESS;
  }

  OPTIMIZE_FLAG_SET_MASK(pJoin->node.optimizedFlag, OPTIMIZE_FLAG_JOIN_COND);

  pCxt->optimized = true;

  return TSDB_CODE_SUCCESS;
}

static bool smaIndexOptMayBeOptimized(SLogicNode* pNode, void* pCtx) {
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
  SScanLogicNode* pSmaScan = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SCAN, (SNode**)&pSmaScan);
  if (NULL == pSmaScan) {
    return code;
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
  if (!pSmaScan->pVgroupList) {
    nodesDestroyNode((SNode*)pSmaScan);
    return terrno;
  }
  code = nodesCloneList(pCols, &pSmaScan->node.pTargets);
  if (NULL == pSmaScan->node.pTargets) {
    nodesDestroyNode((SNode*)pSmaScan);
    return code;
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

static int32_t smaIndexOptCreateSmaCol(SNode* pFunc, uint64_t tableId, int32_t colId, SColumnNode** ppNode) {
  SColumnNode* pCol = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  if (NULL == pCol) {
    return code;
  }
  pCol->tableId = tableId;
  pCol->tableType = TSDB_SUPER_TABLE;
  pCol->colId = colId;
  pCol->colType = COLUMN_TYPE_COLUMN;
  strcpy(pCol->colName, ((SExprNode*)pFunc)->aliasName);
  pCol->node.resType = ((SExprNode*)pFunc)->resType;
  strcpy(pCol->node.aliasName, ((SExprNode*)pFunc)->aliasName);
  *ppNode = pCol;
  return code;
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
      SColumnNode* pCol = NULL;
      code = smaIndexOptCreateSmaCol(pFunc, tableId, smaFuncIndex + 1, &pCol);
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
      code = nodesListMakeStrictAppend(&pCols, (SNode*)pCol);
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
      SColumnNode* pkNode = NULL;
      code = smaIndexOptCreateSmaCol((SNode*)&exprNode, tableId, PRIMARYKEY_TIMESTAMP_COL_ID, &pkNode);
      if (TSDB_CODE_SUCCESS != code) {
        nodesDestroyList(pCols);
        return code;
      }
      code = nodesListPushFront(pCols, (SNode*)pkNode);
      if (TSDB_CODE_SUCCESS != code) {
        nodesDestroyNode((SNode*)pkNode);
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
  SScanLogicNode* pScan = (SScanLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, smaIndexOptMayBeOptimized, NULL);
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
      if (pNode->pParent) {
        if (nodeType(pNode->pParent) == QUERY_NODE_LOGIC_PLAN_WINDOW) {
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
        } else if (nodeType(pNode->pParent) == QUERY_NODE_LOGIC_PLAN_JOIN) {
          ret = false;
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

static bool partTagsOptMayBeOptimized(SLogicNode* pNode, void* pCtx) {
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
  int32_t len = tsnprintf(name, TSDB_COL_FNAME_LEN, "%s.%s", pTableAlias, pColName);

  (void)taosHashBinary(name, len);
  strncpy(pAlias, name, TSDB_COL_NAME_LEN - 1);
}

static int32_t partTagsCreateWrapperFunc(const char* pFuncName, SNode* pNode, SFunctionNode** ppNode) {
  SNode*         pNew = NULL;
  SFunctionNode* pFunc = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }

  snprintf(pFunc->functionName, sizeof(pFunc->functionName), "%s", pFuncName);
  if ((QUERY_NODE_COLUMN == nodeType(pNode) && COLUMN_TYPE_TBNAME != ((SColumnNode*)pNode)->colType) ||
   (QUERY_NODE_COLUMN == nodeType(pNode) && COLUMN_TYPE_TBNAME == ((SColumnNode*)pNode)->colType &&
   ((SColumnNode*)pNode)->tableAlias[0] != '\0')){
    SColumnNode* pCol = (SColumnNode*)pNode;
    partTagsSetAlias(pFunc->node.aliasName, pCol->tableAlias, pCol->colName);
  } else {
    strcpy(pFunc->node.aliasName, ((SExprNode*)pNode)->aliasName);
  }
  code = nodesCloneNode(pNode, &pNew);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pFunc->pParameterList, pNew);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = fmGetFuncInfo(pFunc, NULL, 0);
  }

  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pFunc);
    return code;
  }
  *ppNode = pFunc;
  return code;
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
    SFunctionNode* pFunc = NULL;
    if (hasIndefRowsSelectFunc) {
      code = partTagsCreateWrapperFunc("_select_value", pNode, &pFunc);
      if (TSDB_CODE_SUCCESS == code) {
        code = nodesListStrictAppend(pAgg->pAggFuncs, (SNode*)pFunc);
      }
    } else {
      code = partTagsCreateWrapperFunc("_group_key", pNode, &pFunc);
      if (TSDB_CODE_SUCCESS == code) {
        code = nodesListStrictAppend(pAgg->pAggFuncs, (SNode*)pFunc);
      }
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }
  return code;
}

static int32_t partTagsOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SLogicNode* pNode = optFindPossibleNode(pLogicSubplan->pNode, partTagsOptMayBeOptimized, NULL);
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
      if (pNode->pParent->pSlimit) pScan->groupOrderScan = true;

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
      SNode* pNew = NULL;
      code = nodesCloneNode(pGroupExpr, &pNew);
      if (TSDB_CODE_SUCCESS == code) {
        code = nodesListMakeStrictAppend(&pScan->pGroupTags, pNew);
      }
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

static int32_t eliminateProjOptCheckProjColumnNames(SProjectLogicNode* pProjectNode, bool* pRet) {
  int32_t   code = 0;
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
      code = taosHashPut(pProjColNameHash, projColumnName, strlen(projColumnName), &exist, sizeof(exist));
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    }
  }
  taosHashCleanup(pProjColNameHash);
  *pRet = true;
  return code;
}

static bool eliminateProjOptMayBeOptimized(SLogicNode* pNode, void* pCtx) {
  // Super table scan requires project operator to merge packets to improve performance.
  if (NULL == pNode->pParent && (QUERY_NODE_LOGIC_PLAN_PROJECT != nodeType(pNode) || 1 != LIST_LENGTH(pNode->pChildren) ||
      (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(nodesListGetNode(pNode->pChildren, 0)) &&
       TSDB_SUPER_TABLE == ((SScanLogicNode*)nodesListGetNode(pNode->pChildren, 0))->tableType))) {
    return false;
  }

  if (OPTIMIZE_FLAG_TEST_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_ELIMINATE_PROJ)) {
    return false;
  }

  if (NULL != pNode->pParent && (QUERY_NODE_LOGIC_PLAN_PROJECT != nodeType(pNode) || 1 != LIST_LENGTH(pNode->pChildren) ||
      QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(nodesListGetNode(pNode->pChildren, 0)) || QUERY_NODE_LOGIC_PLAN_JOIN != nodeType(pNode->pParent))) {
    return false;
  }  

  if (QUERY_NODE_LOGIC_PLAN_PROJECT != nodeType(pNode) || 1 != LIST_LENGTH(pNode->pChildren)) {
    return false;
  }
  
  if (QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL == nodeType(nodesListGetNode(pNode->pChildren, 0))) {
    SLogicNode* pChild = (SLogicNode*)nodesListGetNode(pNode->pChildren, 0);
    if(LIST_LENGTH(pChild->pTargets) != LIST_LENGTH(pNode->pTargets)) {
      return false;
    }
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
    SColumnNode* pCol = (SColumnNode*)pExprNode;
    if (NULL != pNode->pParent && 0 != strcmp(pCol->colName, pCol->node.aliasName)) {
      return false;
    }
  }
  int32_t* pCode = pCtx;
  bool     ret = false;
  int32_t  code = eliminateProjOptCheckProjColumnNames(pProjectNode, &ret);
  if (TSDB_CODE_SUCCESS != code) {
    *pCode = code;
  }
  return ret;
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
  if (QUERY_NODE_LOGIC_PLAN_JOIN == nodeType(pChild) && ((SJoinLogicNode*)pChild)->pFullOnCond) {
    SJoinLogicNode*         pJoinLogicNode = (SJoinLogicNode*)pChild;
    CheckNewChildTargetsCxt cxt = {.pNewChildTargets = pNewChildTargets, .canUse = false};
    nodesWalkExpr(pJoinLogicNode->pFullOnCond, eliminateProjOptCanUseNewChildTargetsImpl, &cxt);
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
      (void)nodesListErase(pProjectNode->pProjections, cell);
    }
  }
}

typedef struct RewriteTableAliasCxt {
  char* newTableAlias;
  bool  rewriteColName;
} RewriteTableAliasCxt;

static EDealRes eliminateProjOptRewriteScanTableAlias(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    RewriteTableAliasCxt* pCtx = (RewriteTableAliasCxt*)pContext;
    strncpy(pCol->tableAlias, pCtx->newTableAlias, TSDB_TABLE_NAME_LEN);
  }
  return DEAL_RES_CONTINUE;
}


static void eliminateProjPushdownProjIdx(SNodeList* pParentProjects, SNodeList* pChildTargets) {
  SNode* pChildTarget = NULL, *pParentProject = NULL;
  FOREACH(pChildTarget, pChildTargets) {
    SColumnNode* pTargetCol = (SColumnNode*)pChildTarget;
    FOREACH(pParentProject, pParentProjects) {
      SExprNode* pProject = (SExprNode*)pParentProject;
      if (0 == strcmp(pTargetCol->colName, pProject->aliasName)) {
        pTargetCol->resIdx = pProject->projIdx;
        break;
      }
    }
  }
}

static int32_t eliminateProjOptimizeImpl(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan,
                                         SProjectLogicNode* pProjectNode) {
  SLogicNode* pChild = (SLogicNode*)nodesListGetNode(pProjectNode->node.pChildren, 0);
  int32_t     code = 0;

  if (NULL == pProjectNode->node.pParent) {
    SNodeList* pNewChildTargets = NULL;
    code = nodesMakeList(&pNewChildTargets);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    SNode *    pProjection = NULL, *pChildTarget = NULL;
    bool       orderMatch = true;
    bool       needOrderMatch =
        QUERY_NODE_LOGIC_PLAN_PROJECT == nodeType(pChild) && ((SProjectLogicNode*)pChild)->isSetOpProj;
    if (needOrderMatch) {
      // For sql: select ... from (select ... union all select ...);
      // When eliminating the outer proj (the outer select), we have to make sure that the outer proj projections and
      // union all project targets have same columns in the same order. See detail in TD-30188
      FORBOTH(pProjection, pProjectNode->pProjections, pChildTarget, pChild->pTargets) {
        if (!pProjection) break;
        if (0 != strcmp(((SColumnNode*)pProjection)->colName, ((SColumnNode*)pChildTarget)->colName)) {
          orderMatch = false;
          break;
        }
        SNode* pNew = NULL;
        code = nodesCloneNode(pChildTarget, &pNew);
        if (TSDB_CODE_SUCCESS == code) {
          code = nodesListStrictAppend(pNewChildTargets, pNew);
        }
        if (TSDB_CODE_SUCCESS != code) break;
      }
    } else {
      FOREACH(pProjection, pProjectNode->pProjections) {
        FOREACH(pChildTarget, pChild->pTargets) {
          if (0 == strcmp(((SColumnNode*)pProjection)->colName, ((SColumnNode*)pChildTarget)->colName)) {
            SNode* pNew = NULL;
            code = nodesCloneNode(pChildTarget, &pNew);
            if (TSDB_CODE_SUCCESS == code) {
              code = nodesListStrictAppend(pNewChildTargets, pNew);
            }
            break;
          }
        }
        if (TSDB_CODE_SUCCESS != code) {
          break;
        }
      }
    }
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyList(pNewChildTargets);
      return code;
    }

    if (eliminateProjOptCanChildConditionUseChildTargets(pChild, pNewChildTargets) &&
        (!needOrderMatch || (needOrderMatch && orderMatch))) {
      nodesDestroyList(pChild->pTargets);
      pChild->pTargets = pNewChildTargets;
    } else {
      nodesDestroyList(pNewChildTargets);
      OPTIMIZE_FLAG_SET_MASK(pProjectNode->node.optimizedFlag, OPTIMIZE_FLAG_ELIMINATE_PROJ);
      pCxt->optimized = true;
      return TSDB_CODE_SUCCESS;
    }
  } else {
    RewriteTableAliasCxt cxt = {.newTableAlias = pProjectNode->stmtName, .rewriteColName = false};
    SScanLogicNode* pScan = (SScanLogicNode*)pChild;
    nodesWalkExprs(pScan->pScanCols, eliminateProjOptRewriteScanTableAlias, &cxt);
    nodesWalkExprs(pScan->pScanPseudoCols, eliminateProjOptRewriteScanTableAlias, &cxt);    
    nodesWalkExpr(pScan->node.pConditions, eliminateProjOptRewriteScanTableAlias, &cxt);
    nodesWalkExprs(pChild->pTargets, eliminateProjOptRewriteScanTableAlias, &cxt);
    eliminateProjPushdownProjIdx(pProjectNode->pProjections, pChild->pTargets);
  }
  
  if (TSDB_CODE_SUCCESS == code) {
    code = replaceLogicNode(pLogicSubplan, (SLogicNode*)pProjectNode, pChild);
  }
  if (TSDB_CODE_SUCCESS == code) {
    if (pProjectNode->node.pHint && !pChild->pHint) TSWAP(pProjectNode->node.pHint, pChild->pHint);
    NODES_CLEAR_LIST(pProjectNode->node.pChildren);
    nodesDestroyNode((SNode*)pProjectNode);
    // if pChild is a project logic node, remove its projection which is not reference by its target.
    alignProjectionWithTarget(pChild);
  }
  pCxt->optimized = true;
  return code;
}

static int32_t eliminateProjOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  int32_t            code = 0;
  SProjectLogicNode* pProjectNode =
      (SProjectLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, eliminateProjOptMayBeOptimized, &code);

  if (NULL == pProjectNode) {
    return TSDB_CODE_SUCCESS;
  }

  return eliminateProjOptimizeImpl(pCxt, pLogicSubplan, pProjectNode);
}

static bool rewriteTailOptMayBeOptimized(SLogicNode* pNode, void* pCtx) {
  return QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC == nodeType(pNode) && ((SIndefRowsFuncLogicNode*)pNode)->isTailFunc;
}

static int32_t rewriteTailOptCreateOrderByExpr(SNode* pSortKey, SNode** ppNode) {
  SOrderByExprNode* pOrder = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR, (SNode**)&pOrder);
  if (NULL == pOrder) {
    return code;
  }
  pOrder->order = ORDER_DESC;
  pOrder->pExpr = NULL;
  code = nodesCloneNode(pSortKey, &pOrder->pExpr);
  if (NULL == pOrder->pExpr) {
    nodesDestroyNode((SNode*)pOrder);
    return code;
  }
  *ppNode = (SNode*)pOrder;
  return code;
}

static int32_t rewriteTailOptCreateLimit(SNode* pLimit, SNode* pOffset, SNode** pOutput) {
  SLimitNode* pLimitNode = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_LIMIT, (SNode**)&pLimitNode);
  if (NULL == pLimitNode) {
    return code;
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
  SSortLogicNode* pSort = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SORT, (SNode**)&pSort);
  if (NULL == pSort) {
    return code;
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

  SNode* pNewNode = NULL;
  code = rewriteTailOptCreateOrderByExpr(nodesListGetNode(pTail->pParameterList, rowtsIndex), &pNewNode);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pSort->pSortKeys, pNewNode);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesCloneList(((SLogicNode*)nodesListGetNode(pSort->node.pChildren, 0))->pTargets, &pSort->node.pTargets);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pOutput = (SLogicNode*)pSort;
  } else {
    nodesDestroyNode((SNode*)pSort);
  }

  return code;
}

static int32_t rewriteTailOptCreateProjectExpr(SFunctionNode* pFunc, SNode** ppNode) {
  SNode* pExpr = NULL;
  int32_t code = nodesCloneNode(nodesListGetNode(pFunc->pParameterList, 0), &pExpr);
  if (NULL == pExpr) {
    return code;
  }
  strcpy(((SExprNode*)pExpr)->aliasName, pFunc->node.aliasName);
  *ppNode = pExpr;
  return code;
}

static int32_t rewriteTailOptCreateProject(SIndefRowsFuncLogicNode* pIndef, SLogicNode** pOutput) {
  SProjectLogicNode* pProject = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_PROJECT, (SNode**)&pProject);
  if (NULL == pProject) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  TSWAP(pProject->node.pTargets, pIndef->node.pTargets);
  pProject->node.precision = pIndef->node.precision;

  SFunctionNode* pTail = NULL;
  SNode*         pFunc = NULL;
  FOREACH(pFunc, pIndef->pFuncs) {
    SNode* pNew = NULL;
    code = rewriteTailOptCreateProjectExpr((SFunctionNode*)pFunc, &pNew);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListMakeStrictAppend(&pProject->pProjections, pNew);
    }
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
      (SIndefRowsFuncLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, rewriteTailOptMayBeOptimized, NULL);

  if (NULL == pIndef) {
    return TSDB_CODE_SUCCESS;
  }

  return rewriteTailOptimizeImpl(pCxt, pLogicSubplan, pIndef);
}

static bool eliminateSetOpMayBeOptimized(SLogicNode* pNode, void* pCtx) {
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
  SLogicNode* pSetOpNode = optFindPossibleNode(pLogicSubplan->pNode, eliminateSetOpMayBeOptimized, NULL);
  if (NULL == pSetOpNode) {
    return TSDB_CODE_SUCCESS;
  }

  return eliminateSetOpOptimizeImpl(pCxt, pLogicSubplan, pSetOpNode);
}

static bool rewriteUniqueOptMayBeOptimized(SLogicNode* pNode, void* pCtx) {
  return QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC == nodeType(pNode) && ((SIndefRowsFuncLogicNode*)pNode)->isUniqueFunc;
}

static int32_t rewriteUniqueOptCreateGroupingSet(SNode* pExpr, SNode** ppNode) {
  SGroupingSetNode* pGroupingSet = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_GROUPING_SET, (SNode**)&pGroupingSet);
  if (NULL == pGroupingSet) {
    return code;
  }
  pGroupingSet->groupingSetType = GP_TYPE_NORMAL;
  SExprNode* pGroupExpr = NULL;
  code = nodesCloneNode(pExpr, (SNode**)&pGroupExpr);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pGroupingSet->pParameterList, (SNode*)pGroupExpr);
  }
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pGroupingSet);
    return code;
  }
  *ppNode = (SNode*)pGroupingSet;
  return code;
}

static int32_t rewriteUniqueOptCreateFirstFunc(SFunctionNode* pSelectValue, SNode* pCol, SNode** ppNode) {
  SFunctionNode* pFunc = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }

  strcpy(pFunc->functionName, "first");
  if (NULL != pSelectValue) {
    strcpy(pFunc->node.aliasName, pSelectValue->node.aliasName);
  } else {
    int64_t pointer = (int64_t)pFunc;
    char    name[TSDB_FUNC_NAME_LEN + TSDB_POINTER_PRINT_BYTES + TSDB_NAME_DELIMITER_LEN + 1] = {0};
    int32_t len = tsnprintf(name, sizeof(name) - 1, "%s.%" PRId64 "", pFunc->functionName, pointer);
    (void)taosHashBinary(name, len);
    strncpy(pFunc->node.aliasName, name, TSDB_COL_NAME_LEN - 1);
  }
  SNode* pNew = NULL;
  code = nodesCloneNode(pCol, &pNew);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pFunc->pParameterList, pNew);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = fmGetFuncInfo(pFunc, NULL, 0);
  }

  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pFunc);
    return code;
  }
  *ppNode = (SNode*)pFunc;
  return code;
}

static int32_t rewriteUniqueOptCreateAgg(SIndefRowsFuncLogicNode* pIndef, SLogicNode** pOutput) {
  SAggLogicNode* pAgg = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_AGG, (SNode**)&pAgg);
  if (NULL == pAgg) {
    return code;
  }

  TSWAP(pAgg->node.pChildren, pIndef->node.pChildren);
  optResetParent((SLogicNode*)pAgg);
  pAgg->node.precision = pIndef->node.precision;
  pAgg->node.requireDataOrder = DATA_ORDER_LEVEL_IN_BLOCK;  // first function requirement
  pAgg->node.resultDataOrder = DATA_ORDER_LEVEL_NONE;

  bool    hasSelectPrimaryKey = false;
  SNode*  pPrimaryKey = NULL;
  SNode*  pNode = NULL;
  FOREACH(pNode, pIndef->pFuncs) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    SNode*         pExpr = nodesListGetNode(pFunc->pParameterList, 0);
    if (FUNCTION_TYPE_UNIQUE == pFunc->funcType) {
      pPrimaryKey = nodesListGetNode(pFunc->pParameterList, 1);
      SNode* pNew = NULL;
      code = rewriteUniqueOptCreateGroupingSet(pExpr, &pNew);
      if (TSDB_CODE_SUCCESS == code) {
        code = nodesListMakeStrictAppend(&pAgg->pGroupKeys, pNew);
      }
    } else if (PRIMARYKEY_TIMESTAMP_COL_ID == ((SColumnNode*)pExpr)->colId) {  // _select_value(ts) => first(ts)
      hasSelectPrimaryKey = true;
      SNode* pNew = NULL;
      code = rewriteUniqueOptCreateFirstFunc(pFunc, pExpr, &pNew);
      if (TSDB_CODE_SUCCESS == code) {
        code = nodesListMakeStrictAppend(&pAgg->pAggFuncs, pNew);
      }
    } else {  // _select_value(other_col)
      SNode* pNew = NULL;
      code = nodesCloneNode(pNode, &pNew);
      if (TSDB_CODE_SUCCESS == code) {
        code = nodesListMakeStrictAppend(&pAgg->pAggFuncs, pNew);
      }
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
    SNode* pNew = NULL;
    code = rewriteUniqueOptCreateFirstFunc(NULL, pPrimaryKey, &pNew);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListMakeStrictAppend(&pAgg->pAggFuncs, pNew);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pOutput = (SLogicNode*)pAgg;
  } else {
    nodesDestroyNode((SNode*)pAgg);
  }
  return code;
}

static int32_t rewriteUniqueOptCreateProjectCol(SFunctionNode* pFunc, SNode** ppNode) {
  SColumnNode* pCol = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  if (NULL == pCol) {
    return code;
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
  *ppNode = (SNode*)pCol;
  return code;
}

static int32_t rewriteUniqueOptCreateProject(SIndefRowsFuncLogicNode* pIndef, SLogicNode** pOutput) {
  SProjectLogicNode* pProject = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_PROJECT, (SNode**)&pProject);
  if (NULL == pProject) {
    return code;
  }

  TSWAP(pProject->node.pTargets, pIndef->node.pTargets);
  pProject->node.precision = pIndef->node.precision;
  pProject->node.requireDataOrder = DATA_ORDER_LEVEL_NONE;
  pProject->node.resultDataOrder = DATA_ORDER_LEVEL_NONE;

  SNode*  pNode = NULL;
  FOREACH(pNode, pIndef->pFuncs) {
    SNode* pNew = NULL;
    code = rewriteUniqueOptCreateProjectCol((SFunctionNode*)pNode, &pNew);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListMakeStrictAppend(&pProject->pProjections, pNew);
    }
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
      (SIndefRowsFuncLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, rewriteUniqueOptMayBeOptimized, NULL);

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
static bool lastRowScanOptCheckColNum(int32_t lastColNum, col_id_t lastColId, int32_t selectNonPKColNum,
                                      col_id_t selectNonPKColId) {
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
         QUERY_NODE_OPERATOR == nodeType(nodesListGetNode(pFunc->pParameterList, 0)) || QUERY_NODE_VALUE == nodeType(nodesListGetNode(pFunc->pParameterList, 0)) ||
         COLUMN_TYPE_COLUMN != ((SColumnNode*)nodesListGetNode(pFunc->pParameterList, 0))->colType))) &&
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
        if (pCol->colType != COLUMN_TYPE_COLUMN && TSDB_CACHE_MODEL_LAST_ROW != cacheLastModel) {
          needSplitFuncCount++;
          *hasOtherFunc = true;
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
      if (!lastRowScanOptCheckColNum(lastColNum, lastColId, selectNonPKColNum, selectNonPKColId)) return false;
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

static bool lastRowScanOptMayBeOptimized(SLogicNode* pNode, void* pCtx) {
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
  int32_t    pkBytes;
  int32_t    code;
} SLastRowScanOptSetColDataTypeCxt;

static EDealRes lastRowScanOptSetColDataType(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    SLastRowScanOptSetColDataTypeCxt* pCxt = pContext;
    if (pCxt->doAgg) {
      pCxt->code = nodesListMakeAppend(&pCxt->pLastCols, pNode);
      if (TSDB_CODE_SUCCESS != pCxt->code) {
        return DEAL_RES_ERROR;
      }
      getLastCacheDataType(&(((SColumnNode*)pNode)->node.resType), pCxt->pkBytes);
    } else {
      SNode* pCol = NULL;
      FOREACH(pCol, pCxt->pLastCols) {
        if (nodesEqualNode(pCol, pNode)) {
          getLastCacheDataType(&(((SColumnNode*)pNode)->node.resType), pCxt->pkBytes);
          break;
        }
      }
    }
    return DEAL_RES_IGNORE_CHILD;
  }
  return DEAL_RES_CONTINUE;
}

static void lastRowScanOptSetLastTargets(SNodeList* pTargets, SNodeList* pLastCols, SNodeList* pLastRowCols, bool erase, int32_t pkBytes) {
  SNode* pTarget = NULL;
  WHERE_EACH(pTarget, pTargets) {
    bool   found = false;
    SNode* pCol = NULL;
    FOREACH(pCol, pLastCols) {
      if (nodesEqualNode(pCol, pTarget)) {
        getLastCacheDataType(&(((SColumnNode*)pTarget)->node.resType), pkBytes);
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
    return terrno;
  }
  pFuncTypeParam->type = funcType;
  if (NULL == pScan->pFuncTypes) {
    pScan->pFuncTypes = taosArrayInit(pScan->pScanCols->length, sizeof(SFunctParam));
    if (NULL == pScan->pFuncTypes) {
      taosMemoryFree(pFuncTypeParam);
      return terrno;
    }
  }
 
  pFuncTypeParam->pCol = taosMemoryCalloc(1, sizeof(SColumn));
  if (NULL == pFuncTypeParam->pCol) {
    taosMemoryFree(pFuncTypeParam);
    return terrno;
  }
  pFuncTypeParam->pCol->colId = pColNode->colId;
  strcpy(pFuncTypeParam->pCol->name, pColNode->colName);
  if (NULL == taosArrayPush(pScan->pFuncTypes, pFuncTypeParam)) {
    taosMemoryFree(pFuncTypeParam);
    return terrno;
  }

  taosMemoryFree(pFuncTypeParam);
  return TSDB_CODE_SUCCESS;
}

static int32_t lastRowScanOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SAggLogicNode* pAgg = (SAggLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, lastRowScanOptMayBeOptimized, NULL);

  if (NULL == pAgg) {
    return TSDB_CODE_SUCCESS;
  }

  SLastRowScanOptSetColDataTypeCxt cxt = {.doAgg = true, .pLastCols = NULL, .pOtherCols = NULL};
  SNode*                           pNode = NULL;
  SColumnNode*                     pPKTsCol = NULL;
  SColumnNode*                     pNonPKCol = NULL;
  SScanLogicNode*                  pScan = (SScanLogicNode*)nodesListGetNode(pAgg->node.pChildren, 0);
  pScan->scanType = SCAN_TYPE_LAST_ROW;
  pScan->igLastNull = pAgg->hasLast ? true : false;
  SArray*    isDuplicateCol = taosArrayInit(pScan->pScanCols->length, sizeof(bool));
  SNodeList* pLastRowCols = NULL;
  bool       adjLastRowTsColName = false;
  char       tsColName[TSDB_COL_NAME_LEN] = {0};
  int32_t    code = 0;

  FOREACH(pNode, pAgg->pAggFuncs) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    int32_t        funcType = pFunc->funcType;
    SNode*         pParamNode = NULL;
    if (FUNCTION_TYPE_LAST == funcType) {
      (void)nodesListErase(pFunc->pParameterList, nodesListGetCell(pFunc->pParameterList, 1));
      nodesWalkExpr(nodesListGetNode(pFunc->pParameterList, 0), lastRowScanOptSetColDataType, &cxt);
      if (TSDB_CODE_SUCCESS != cxt.code) break;
    }
    FOREACH(pParamNode, pFunc->pParameterList) {
      if (FUNCTION_TYPE_LAST_ROW == funcType || FUNCTION_TYPE_LAST == funcType) {
        int32_t len = tsnprintf(pFunc->functionName, sizeof(pFunc->functionName),
                              FUNCTION_TYPE_LAST_ROW == funcType ? "_cache_last_row" : "_cache_last");
        pFunc->functionName[len] = '\0';
        code = fmGetFuncInfo(pFunc, NULL, 0);
        if (TSDB_CODE_SUCCESS != code) {
          nodesClearList(cxt.pLastCols);
          break;
        }
        cxt.funcType = pFunc->funcType;
        cxt.pkBytes = (pFunc->hasPk) ? pFunc->pkBytes : 0;
        // add duplicate cols which be removed for both last_row, last
        if (pAgg->hasLast && pAgg->hasLastRow) {
          if (QUERY_NODE_COLUMN == nodeType(pParamNode)) {
            SNode* pColNode = NULL;
            int i = 0;
            FOREACH(pColNode, pScan->pScanCols) {
              bool isDup = false;
              bool* isDuplicate = taosArrayGet(isDuplicateCol, i);
              if (NULL == isDuplicate) {
                if (NULL == taosArrayInsert(isDuplicateCol, i, &isDup)) {
                  code = terrno;
                  break;
                }
                isDuplicate = taosArrayGet(isDuplicateCol, i);
              }
              i++;
              if (nodesEqualNode(pParamNode, pColNode)) {
                if (*isDuplicate) {
                  if (0 == strncmp(((SColumnNode*)pColNode)->colName, "#dup_col.", 9)) {
                    continue;
                  }
                  SNode* newColNode = NULL;
                  code = nodesCloneNode(pColNode, &newColNode);
                  if (TSDB_CODE_SUCCESS != code) {
                    break;
                  }
                  sprintf(((SColumnNode*)newColNode)->colName, "#dup_col.%p", newColNode);
                  sprintf(((SColumnNode*)pParamNode)->colName, "#dup_col.%p", newColNode);
                  if (FUNCTION_TYPE_LAST_ROW == funcType &&
                      ((SColumnNode*)pParamNode)->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
                    if (!adjLastRowTsColName) {
                      adjLastRowTsColName = true;
                      strncpy(tsColName, ((SColumnNode*)pParamNode)->colName, TSDB_COL_NAME_LEN);
                    } else {
                      strncpy(((SColumnNode*)pParamNode)->colName, tsColName, TSDB_COL_NAME_LEN);
                      nodesDestroyNode(newColNode);
                      continue;
                    }
                  }

                  code = nodesListStrictAppend(pScan->pScanCols, newColNode);
                  if (TSDB_CODE_SUCCESS != code) break;
                  isDup = true;
                  if (NULL == taosArrayInsert(isDuplicateCol, pScan->pScanCols->length, &isDup)) {
                    code = terrno;
                    break;
                  }
                  SNode* pNew = NULL;
                  code = nodesCloneNode(newColNode, &pNew);
                  if (TSDB_CODE_SUCCESS != code) break;
                  code = nodesListStrictAppend(pScan->node.pTargets, pNew);
                  if (TSDB_CODE_SUCCESS != code) break;
                  if (funcType != FUNCTION_TYPE_LAST) {
                    pNew = NULL;
                    code = nodesCloneNode(newColNode, &pNew);
                    if (TSDB_CODE_SUCCESS != code) break;
                    code = nodesListMakeAppend(&pLastRowCols, pNew);
                    if (TSDB_CODE_SUCCESS != code) break;
                  }

                  code = lastRowScanBuildFuncTypes(pScan, (SColumnNode*)newColNode, pFunc->funcType);
                  if (TSDB_CODE_SUCCESS != code) break;
                } else {
                  isDup = true;
                  *isDuplicate = isDup;
                  if (funcType != FUNCTION_TYPE_LAST && !nodeListNodeEqual(cxt.pLastCols, pColNode)) {
                    SNode* pNew = NULL;
                    code = nodesCloneNode(pColNode, &pNew);
                    if (TSDB_CODE_SUCCESS != code) break;
                    code = nodesListMakeStrictAppend(&pLastRowCols, pNew);
                    if (TSDB_CODE_SUCCESS != code) break;
                  }
                  code = lastRowScanBuildFuncTypes(pScan, (SColumnNode*)pColNode, pFunc->funcType);
                  if (TSDB_CODE_SUCCESS != code) break;
                }
                continue;
              } else if (nodeListNodeEqual(pFunc->pParameterList, pColNode)) {
                if (funcType != FUNCTION_TYPE_LAST && ((SColumnNode*)pColNode)->colId == PRIMARYKEY_TIMESTAMP_COL_ID &&
                    !nodeListNodeEqual(pLastRowCols, pColNode)) {
                  SNode* pNew = NULL;
                  code = nodesCloneNode(pColNode, &pNew);
                  if (TSDB_CODE_SUCCESS != code) break;
                  code = nodesListMakeAppend(&pLastRowCols, pNew);
                  if (TSDB_CODE_SUCCESS != code) break;

                  code = lastRowScanBuildFuncTypes(pScan, (SColumnNode*)pColNode, pFunc->funcType);
                  if (TSDB_CODE_SUCCESS != code) break;
                  isDup = true;
                  *isDuplicate = isDup;
                }
              }
            }
            if (TSDB_CODE_SUCCESS != code) break;;
            FOREACH(pColNode, pScan->pScanPseudoCols) {
              if (nodesEqualNode(pParamNode, pColNode)) {
                if (funcType != FUNCTION_TYPE_LAST) {
                  SNode* pNew = NULL;
                  code = nodesCloneNode(pColNode, &pNew);
                  if (TSDB_CODE_SUCCESS != code) break;
                  code = nodesListMakeAppend(&pLastRowCols, pNew);
                  if (TSDB_CODE_SUCCESS != code) break;
                }
              }
            }
          }
        }

        if (TSDB_CODE_SUCCESS != code) break;
        if (pFunc->hasPk) {
          code = nodesListMakeAppend(&cxt.pOtherCols, nodesListGetNode(pFunc->pParameterList, LIST_LENGTH(pFunc->pParameterList) - 1));
        }
        if (TSDB_CODE_SUCCESS != code) break;
      } else {
        pNode = nodesListGetNode(pFunc->pParameterList, 0);
        code = nodesListMakeAppend(&cxt.pOtherCols, pNode);
        if (TSDB_CODE_SUCCESS != code) break;

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
    if (TSDB_CODE_SUCCESS != code) break;
  }

  if (TSDB_CODE_SUCCESS != code) {
    taosArrayDestroy(isDuplicateCol);
    nodesClearList(cxt.pLastCols);
    return code;
  }

  if (NULL != cxt.pLastCols) {
    cxt.doAgg = false;
    cxt.funcType = FUNCTION_TYPE_CACHE_LAST;

    lastRowScanOptSetLastTargets(pScan->pScanCols, cxt.pLastCols, pLastRowCols, true, cxt.pkBytes);
    nodesWalkExprs(pScan->pScanPseudoCols, lastRowScanOptSetColDataType, &cxt);
    if (TSDB_CODE_SUCCESS != cxt.code) {
      nodesClearList(cxt.pLastCols);
      nodesClearList(cxt.pOtherCols);
      taosArrayDestroy(isDuplicateCol);
      return cxt.code;
    }
    lastRowScanOptSetLastTargets(pScan->node.pTargets, cxt.pLastCols, pLastRowCols, false, cxt.pkBytes);
    lastRowScanOptRemoveUslessTargets(pScan->node.pTargets, cxt.pLastCols, cxt.pOtherCols, pLastRowCols);
    if (pPKTsCol &&
        ((cxt.pLastCols->length == 1 && nodesEqualNode((SNode*)pPKTsCol, nodesListGetNode(cxt.pLastCols, 0))) ||
         (pScan->node.pTargets->length == 2 && cxt.pkBytes > 0))) {
      // when select last(ts),tbname,ts from ..., we add another ts to targets
      sprintf(pPKTsCol->colName, "#sel_val.%p", pPKTsCol);
      SNode* pNew = NULL;
      code = nodesCloneNode((SNode*)pPKTsCol, &pNew);
      if (TSDB_CODE_SUCCESS == code) {
        code = nodesListAppend(pScan->node.pTargets, pNew);
      }
      if (TSDB_CODE_SUCCESS != code) {
        nodesClearList(cxt.pLastCols);
        nodesClearList(cxt.pOtherCols);
        taosArrayDestroy(isDuplicateCol);
        return code;
      }
    }

    if (pNonPKCol && cxt.pLastCols->length == 1 &&
        nodesEqualNode((SNode*)pNonPKCol, nodesListGetNode(cxt.pLastCols, 0))) {
      // when select last(c1), c1 from ..., we add c1 to targets
      sprintf(pNonPKCol->colName, "#sel_val.%p", pNonPKCol);
      SNode* pNew = NULL;
      code = nodesCloneNode((SNode*)pNonPKCol, &pNew);
      if (TSDB_CODE_SUCCESS == code) {
        code = nodesListAppend(pScan->node.pTargets, pNew);
      }
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

static bool splitCacheLastFuncOptMayBeOptimized(SLogicNode* pNode, void* pCtx) {
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

static int32_t splitCacheLastFuncOptCreateAggLogicNode(SAggLogicNode** pNewAgg, SAggLogicNode* pAgg, SNodeList* pFunc,
                                                       SNodeList* pTargets) {
  SAggLogicNode* pNew = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_AGG, (SNode**)&pNew);
  if (NULL == pNew) {
    nodesDestroyList(pFunc);
    nodesDestroyList(pTargets);
    return code;
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
  code = nodesCloneList(pAgg->pGroupKeys, &pNew->pGroupKeys);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pNew);
    return code;
  }
  code = nodesCloneNode(pAgg->node.pConditions, &pNew->node.pConditions);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pNew);
    return code;
  }
  pNew->isGroupTb = pAgg->isGroupTb;
  pNew->isPartTb = pAgg->isPartTb;
  pNew->hasGroup = pAgg->hasGroup;
  code = nodesCloneList(pAgg->node.pChildren, &pNew->node.pChildren);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pNew);
    return code;
  }

  SNode*  pNode = nodesListGetNode(pNew->node.pChildren, 0);
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    SScanLogicNode* pScan = (SScanLogicNode*)pNode;
    SNodeList*      pOldScanCols = NULL;
    TSWAP(pScan->pScanCols, pOldScanCols);
    nodesDestroyList(pScan->pScanPseudoCols);
    pScan->pScanPseudoCols = NULL;
    nodesDestroyList(pScan->node.pTargets);
    pScan->node.pTargets = NULL;
    SNodeListNode* list = NULL;
    code = nodesMakeNode(QUERY_NODE_NODE_LIST, (SNode**)&list);
    if (!list) {
      nodesDestroyNode((SNode*)pNew);
      return code;
    }
    list->pNodeList = pFunc;
    code = nodesCollectColumnsFromNode((SNode*)list, NULL, COLLECT_COL_TYPE_COL, &pScan->pScanCols);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode((SNode*)pNew);
      return code;
    }
    code = nodesCollectColumnsFromNode((SNode*)list, NULL, COLLECT_COL_TYPE_TAG, &pScan->pScanPseudoCols);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode((SNode*)pNew);
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
          SNode* pTmp = NULL;
          code = nodesCloneNode(pNode, &pTmp);
          if (TSDB_CODE_SUCCESS == code) {
            code = nodesListMakeStrictAppend(&pScan->pScanCols, pTmp);
          }
          break;
        }
      }
      if (TSDB_CODE_SUCCESS != code) {
        nodesDestroyNode((SNode*)pNew);
        return code;
      }
    }
    nodesDestroyList(pOldScanCols);
    code = createColumnByRewriteExprs(pScan->pScanCols, &pScan->node.pTargets);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode((SNode*)pNew);
      return code;
    }
    code = createColumnByRewriteExprs(pScan->pScanPseudoCols, &pScan->node.pTargets);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode((SNode*)pNew);
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

static int32_t splitCacheLastFuncOptCreateMergeLogicNode(SMergeLogicNode** pNew, SAggLogicNode* pAgg1,
                                                         SAggLogicNode* pAgg2) {
  SMergeLogicNode* pMerge = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_MERGE, (SNode**)&pMerge);
  if (NULL == pMerge) {
    return code;
  }
  pMerge->colsMerge = true;
  pMerge->numOfChannels = 2;
  pMerge->srcGroupId = -1;
  pMerge->node.precision = pAgg1->node.precision;

  SNode* pNewAgg1 = NULL;
  code = nodesCloneNode((SNode*)pAgg1, &pNewAgg1);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pMerge);
    return code;
  }
  SNode* pNewAgg2 = NULL;
  code = nodesCloneNode((SNode*)pAgg2, &pNewAgg2);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pNewAgg1);
    nodesDestroyNode((SNode*)pMerge);
    return code;
  }

  ((SAggLogicNode*)pNewAgg1)->node.pParent = (SLogicNode*)pMerge;
  ((SAggLogicNode*)pNewAgg2)->node.pParent = (SLogicNode*)pMerge;

  SNode* pNode = NULL;
  FOREACH(pNode, ((SAggLogicNode*)pNewAgg1)->node.pChildren) { ((SLogicNode*)pNode)->pParent = (SLogicNode*)pNewAgg1; }
  FOREACH(pNode, ((SAggLogicNode*)pNewAgg2)->node.pChildren) { ((SLogicNode*)pNode)->pParent = (SLogicNode*)pNewAgg2; }

  SNodeList* pNewTargets1 = NULL;
  code = nodesCloneList(pAgg1->node.pTargets, &pNewTargets1);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppendList(&pMerge->node.pTargets, pNewTargets1);
  }
  SNodeList* pNewTargets2 = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesCloneList(pAgg2->node.pTargets, &pNewTargets2);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppendList(&pMerge->node.pTargets, pNewTargets2);
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
  SAggLogicNode* pAgg = (SAggLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, splitCacheLastFuncOptMayBeOptimized, NULL);

  if (NULL == pAgg) {
    return TSDB_CODE_SUCCESS;
  }
  SScanLogicNode* pScan = (SScanLogicNode*)nodesListGetNode(pAgg->node.pChildren, 0);
  SNode*          pNode = NULL;
  SNodeList*      pAggFuncList = NULL;
  int32_t         code = 0;

  {
    bool hasLast = false;
    bool hasLastRow = false;
    WHERE_EACH(pNode, pAgg->pAggFuncs) {
      SFunctionNode* pFunc = (SFunctionNode*)pNode;
      int32_t        funcType = pFunc->funcType;

      if (isNeedSplitCacheLastFunc(pFunc, pScan)) {
        SNode* pNew = NULL;
        code = nodesCloneNode(pNode, &pNew);
        if (TSDB_CODE_SUCCESS == code) {
          code = nodesListMakeStrictAppend(&pAggFuncList, pNew);
        }
        if (TSDB_CODE_SUCCESS != code) {
          break;
        }
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
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyList(pAggFuncList);
    return code;
  }

  if (NULL == pAggFuncList) {
    planError("empty agg func list while splite projections, funcNum:%d", pAgg->pAggFuncs->length);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  SNodeList* pTargets = NULL;
  {
    WHERE_EACH(pNode, pAgg->node.pTargets) {
      SColumnNode* pCol = (SColumnNode*)pNode;
      SNode*       pFuncNode = NULL;
      bool         found = false;
      FOREACH(pFuncNode, pAggFuncList) {
        SFunctionNode* pFunc = (SFunctionNode*)pFuncNode;
        if (0 == strcmp(pFunc->node.aliasName, pCol->colName)) {
          SNode* pNew = NULL;
          code = nodesCloneNode(pNode, &pNew);
          if (TSDB_CODE_SUCCESS == code) {
            code = nodesListMakeStrictAppend(&pTargets, pNew);
          }
          found = true;
          break;
        }
      }
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
      if (found) {
        ERASE_NODE(pAgg->node.pTargets);
        continue;
      }
      WHERE_NEXT;
    }
  }
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyList(pTargets);
    return code;
  }

  if (NULL == pTargets) {
    planError("empty target func list while splite projections, targetsNum:%d", pAgg->node.pTargets->length);
    nodesDestroyList(pAggFuncList);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  SMergeLogicNode* pMerge = NULL;
  SAggLogicNode*   pNewAgg = NULL;
  code = splitCacheLastFuncOptCreateAggLogicNode(&pNewAgg, pAgg, pAggFuncList, pTargets);
  if (TSDB_CODE_SUCCESS == code) {
    code = splitCacheLastFuncOptModifyAggLogicNode(pAgg);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = splitCacheLastFuncOptCreateMergeLogicNode(&pMerge, pNewAgg, pAgg);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = replaceLogicNode(pLogicSubplan, (SLogicNode*)pAgg, (SLogicNode*)pMerge);
  }

  nodesDestroyNode((SNode*)pAgg);
  nodesDestroyNode((SNode*)pNewAgg);

  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pMerge);
  }

  pCxt->optimized = true;
  return code;
}

// merge projects
static bool mergeProjectsMayBeOptimized(SLogicNode* pNode, void* pCtx) {
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

static EDealRes mergeProjectionsExpr2(SNode** pNode, void* pContext) {
  SMergeProjectionsContext* pCxt = pContext;
  SProjectLogicNode*        pChildProj = pCxt->pChildProj;
  if (QUERY_NODE_COLUMN == nodeType(*pNode)) {
    SColumnNode* pProjCol = (SColumnNode*)(*pNode);
    SNode* pProjection;
    int32_t projIdx = 1;
    FOREACH(pProjection, pChildProj->pProjections) {
      if (isColRefExpr(pProjCol, (SExprNode*)pProjection)) {
        SNode* pExpr = NULL;
        pCxt->errCode = nodesCloneNode(pProjection, &pExpr);
        if (pExpr == NULL) {
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
  return DEAL_RES_CONTINUE;
}

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
            SNode* pExpr = NULL;
            pCxt->errCode = nodesCloneNode(pProjection, &pExpr);
            if (pExpr == NULL) {
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
  nodesRewriteExprs(((SProjectLogicNode*)pSelfNode)->pProjections, mergeProjectionsExpr2, &cxt);
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
  SLogicNode* pProjectNode = optFindPossibleNode(pLogicSubplan->pNode, mergeProjectsMayBeOptimized, NULL);
  if (NULL == pProjectNode) {
    return TSDB_CODE_SUCCESS;
  }

  return mergeProjectsOptimizeImpl(pCxt, pLogicSubplan, pProjectNode);
}

static bool tagScanOptShouldBeOptimized(SLogicNode* pNode, void* pCtx) {
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
  SScanLogicNode* pScanNode = (SScanLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, tagScanOptShouldBeOptimized, NULL);
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
    SNodeList* pScanTargets = NULL;
    int32_t code = nodesMakeList(&pScanTargets);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }

    SNode* pAggTarget = NULL;
    FOREACH(pAggTarget, pAgg->pTargets) {
      SNode* pScanTarget = NULL;
      FOREACH(pScanTarget, pScanNode->node.pTargets) {
        if (0 == strcmp(((SColumnNode*)pAggTarget)->colName, ((SColumnNode*)pScanTarget)->colName)) {
          SNode* pNew = NULL;
          code = nodesCloneNode(pScanTarget, &pNew);
          if (TSDB_CODE_SUCCESS == code) {
           code = nodesListAppend(pScanTargets, pNew);
          }
          break;
        }
      }
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    }
    nodesDestroyList(pScanNode->node.pTargets);
    pScanNode->node.pTargets = pScanTargets;
  }

  pScanNode->onlyMetaCtbIdx = false;

  pCxt->optimized = true;
  return TSDB_CODE_SUCCESS;
}

static bool pushDownLimitOptShouldBeOptimized(SLogicNode* pNode, void* pCtx) {
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

static int32_t pushDownLimitHow(SLogicNode* pNodeWithLimit, SLogicNode* pNodeLimitPushTo, bool* pPushed);
static int32_t pushDownLimitTo(SLogicNode* pNodeWithLimit, SLogicNode* pNodeLimitPushTo, bool* pPushed) {
  int32_t code = 0;
  bool cloned;
  switch (nodeType(pNodeLimitPushTo)) {
    case QUERY_NODE_LOGIC_PLAN_WINDOW: {
      SWindowLogicNode* pWindow = (SWindowLogicNode*)pNodeLimitPushTo;
      if (pWindow->winType != WINDOW_TYPE_INTERVAL) break;
      code = cloneLimit(pNodeWithLimit, pNodeLimitPushTo, CLONE_LIMIT_SLIMIT, &cloned);
      if (TSDB_CODE_SUCCESS == code) {
        *pPushed = true;
      }
      return code;
    }
    case QUERY_NODE_LOGIC_PLAN_SORT:
      if (((SSortLogicNode*)pNodeLimitPushTo)->calcGroupId) break;
      // fall through
    case QUERY_NODE_LOGIC_PLAN_FILL: {
      SNode* pChild = NULL;
      code = cloneLimit(pNodeWithLimit, pNodeLimitPushTo, CLONE_LIMIT_SLIMIT, &cloned);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      FOREACH(pChild, pNodeLimitPushTo->pChildren) {
        code = pushDownLimitHow(pNodeLimitPushTo, (SLogicNode*)pChild, &cloned);
        if (TSDB_CODE_SUCCESS != code) {
          return code;
        }
      }
      *pPushed = true;
      return code;
    }
    case QUERY_NODE_LOGIC_PLAN_AGG: {
      if (nodeType(pNodeWithLimit) == QUERY_NODE_LOGIC_PLAN_PROJECT &&
          (isPartTagAgg((SAggLogicNode*)pNodeLimitPushTo) || isPartTableAgg((SAggLogicNode*)pNodeLimitPushTo))) {
        // when part by tag/tbname, slimit will be cloned to agg, and it will be pipelined.
        // The scan below will do scanning with group order
        code = cloneLimit(pNodeWithLimit, pNodeLimitPushTo, CLONE_SLIMIT, &cloned);
        if (TSDB_CODE_SUCCESS == code) {
          *pPushed = cloned;
        }
        return code;
      }
      // else if not part by tag and tbname, the partition node below indicates that results are sorted, the agg node
      // can be pipelined.
      if (nodeType(pNodeWithLimit) == QUERY_NODE_LOGIC_PLAN_PROJECT && LIST_LENGTH(pNodeLimitPushTo->pChildren) == 1) {
        SLogicNode* pChild = (SLogicNode*)nodesListGetNode(pNodeLimitPushTo->pChildren, 0);
        if (nodeType(pChild) == QUERY_NODE_LOGIC_PLAN_PARTITION) {
          pNodeLimitPushTo->forceCreateNonBlockingOptr = true;
          code = cloneLimit(pNodeWithLimit, pNodeLimitPushTo, CLONE_SLIMIT, &cloned);
          if (TSDB_CODE_SUCCESS == code) {
            *pPushed = cloned;
          }
          return code;
        }
        // Currently, partColOpt is executed after pushDownLimitOpt, and partColOpt will replace partition node with
        // sort node.
        // To avoid dependencies between these two optimizations, we add sort node too.
        if (nodeType(pChild) == QUERY_NODE_LOGIC_PLAN_SORT && ((SSortLogicNode*)pChild)->calcGroupId) {
          pNodeLimitPushTo->forceCreateNonBlockingOptr = true;
          code = cloneLimit(pNodeWithLimit, pNodeLimitPushTo, CLONE_SLIMIT, &cloned);
          if (TSDB_CODE_SUCCESS == code) {
            *pPushed = cloned;
          }
          return code;
        }
      }
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_SCAN:
      if (nodeType(pNodeWithLimit) == QUERY_NODE_LOGIC_PLAN_PROJECT && pNodeWithLimit->pLimit) {
        if (((SProjectLogicNode*)pNodeWithLimit)->inputIgnoreGroup) {
          code = cloneLimit(pNodeWithLimit, pNodeLimitPushTo, CLONE_LIMIT, &cloned);
        } else {
          swapLimit(pNodeWithLimit, pNodeLimitPushTo);
        }
        if (TSDB_CODE_SUCCESS == code) {
          *pPushed = true;
        }
        return code;
      }
      break;
    case QUERY_NODE_LOGIC_PLAN_JOIN: {
      code = cloneLimit(pNodeWithLimit, pNodeLimitPushTo, CLONE_LIMIT, &cloned);
      break;
    }
    default:
      break;
  }
  *pPushed = false;
  return code;
}

static int32_t pushDownLimitHow(SLogicNode* pNodeWithLimit, SLogicNode* pNodeLimitPushTo, bool* pPushed) {
  switch (nodeType(pNodeWithLimit)) {
    case QUERY_NODE_LOGIC_PLAN_PROJECT:
    case QUERY_NODE_LOGIC_PLAN_FILL:
      return pushDownLimitTo(pNodeWithLimit, pNodeLimitPushTo, pPushed);
    case QUERY_NODE_LOGIC_PLAN_SORT: {
      SSortLogicNode* pSort = (SSortLogicNode*)pNodeWithLimit;
      if (sortPriKeyOptIsPriKeyOrderBy(pSort->pSortKeys)) return pushDownLimitTo(pNodeWithLimit, pNodeLimitPushTo, pPushed);
    }
    default:
      break;
  }
  *pPushed = false;
  return TSDB_CODE_SUCCESS;
}

static int32_t pushDownLimitOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SLogicNode* pNode = optFindPossibleNode(pLogicSubplan->pNode, pushDownLimitOptShouldBeOptimized, NULL);
  if (NULL == pNode) {
    return TSDB_CODE_SUCCESS;
  }

  SLogicNode* pChild = (SLogicNode*)nodesListGetNode(pNode->pChildren, 0);
  nodesDestroyNode(pChild->pLimit);
  bool pushed = false;
  int32_t code = pushDownLimitHow(pNode, pChild, &pushed);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  if (pushed) {
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

static int32_t tbCntScanOptCreateTableCountFunc(SNode** ppNode) {
  SFunctionNode* pFunc = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }
  strcpy(pFunc->functionName, "_table_count");
  strcpy(pFunc->node.aliasName, "_table_count");
  code = fmGetFuncInfo(pFunc, NULL, 0);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pFunc);
    return code;
  }
  *ppNode = (SNode*)pFunc;
  return code;
}

static int32_t tbCntScanOptRewriteScan(STbCntScanOptInfo* pInfo) {
  pInfo->pScan->scanType = SCAN_TYPE_TABLE_COUNT;
  strcpy(pInfo->pScan->tableName.dbname, pInfo->table.dbname);
  strcpy(pInfo->pScan->tableName.tname, pInfo->table.tname);
  NODES_DESTORY_LIST(pInfo->pScan->node.pTargets);
  NODES_DESTORY_LIST(pInfo->pScan->pScanCols);
  NODES_DESTORY_NODE(pInfo->pScan->node.pConditions);
  NODES_DESTORY_LIST(pInfo->pScan->pScanPseudoCols);
  SNode* pNew = NULL;
  int32_t code = tbCntScanOptCreateTableCountFunc(&pNew);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pInfo->pScan->pScanPseudoCols, pNew);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExpr(nodesListGetNode(pInfo->pScan->pScanPseudoCols, 0), &pInfo->pScan->node.pTargets);
  }
  SNode* pGroupKey = NULL;
  if (TSDB_CODE_SUCCESS  == code) {
    FOREACH(pGroupKey, pInfo->pAgg->pGroupKeys) {
      SNode* pGroupCol = nodesListGetNode(((SGroupingSetNode*)pGroupKey)->pParameterList, 0);
      SNode* pNew = NULL;
      code = nodesCloneNode(pGroupCol, &pNew);
      if (TSDB_CODE_SUCCESS == code) {
        code = nodesListMakeStrictAppend(&pInfo->pScan->pGroupTags, pNew);
      }
      if (TSDB_CODE_SUCCESS == code) {
        pNew = NULL;
        code = nodesCloneNode(pGroupCol, &pNew);
        if (TSDB_CODE_SUCCESS == code) {
          code = nodesListMakeStrictAppend(&pInfo->pScan->pScanCols, pNew);
        }
      }
      if (TSDB_CODE_SUCCESS == code) {
        pNew = NULL;
        code = nodesCloneNode(pGroupCol, &pNew);
        if (TSDB_CODE_SUCCESS == code) {
          code = nodesListMakeStrictAppend(&pInfo->pScan->node.pTargets, pNew);
        }
      }
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    }
  }
  return code;
}

static int32_t tbCntScanOptCreateSumFunc(SFunctionNode* pCntFunc, SNode* pParam, SNode** pOutput) {
  SFunctionNode* pFunc = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }
  strcpy(pFunc->functionName, "sum");
  strcpy(pFunc->node.aliasName, pCntFunc->node.aliasName);
  code = createColumnByRewriteExpr(pParam, &pFunc->pParameterList);
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
  SOptimizePKCtx* ctx = pInfo;
  ctx->code = nodesListAppend(ctx->pList, (SNode*)pSort);
  return false;
}

static int32_t sortNonPriKeyOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SNodeList* pNodeList = NULL;
  int32_t code = nodesMakeList(&pNodeList);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  SOptimizePKCtx ctx = {.pList = pNodeList, .code = 0};
  (void)optFindEligibleNode(pLogicSubplan->pNode, sortNonPriKeyShouldOptimize, &ctx);
  if (TSDB_CODE_SUCCESS != ctx.code) {
    nodesClearList(pNodeList);
    return code;
  }
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

static bool hashJoinOptShouldBeOptimized(SLogicNode* pNode, void* pCtx) {
  bool res = false;
  if (QUERY_NODE_LOGIC_PLAN_JOIN != nodeType(pNode)) {
    return res;
  }

  SJoinLogicNode* pJoin = (SJoinLogicNode*)pNode;
  if (pJoin->joinAlgo != JOIN_ALGO_UNKNOWN) {
    return res;
  }
  
  if (!pJoin->hashJoinHint) {
    goto _return;
  }

  if ((JOIN_STYPE_NONE != pJoin->subType && JOIN_STYPE_OUTER != pJoin->subType) || JOIN_TYPE_FULL == pJoin->joinType || pNode->pChildren->length != 2 ) {
    goto _return;
  }

  res = true;

_return:

  if (!res && DATA_ORDER_LEVEL_NONE == pJoin->node.requireDataOrder) {
    pJoin->node.requireDataOrder = DATA_ORDER_LEVEL_GLOBAL;
    int32_t *pCode = pCtx;
    int32_t code = adjustLogicNodeDataRequirement(pNode, pJoin->node.requireDataOrder);
    if (TSDB_CODE_SUCCESS != code) {
      *pCode = code;
    }
  }

  return res;
}

static int32_t hashJoinOptSplitPrimFromLogicCond(SNode **pCondition, SNode **pPrimaryKeyCond) {
  SLogicConditionNode *pLogicCond = (SLogicConditionNode *)(*pCondition);
  int32_t code = TSDB_CODE_SUCCESS;
  SNodeList *pPrimaryKeyConds = NULL;
  SNode     *pCond = NULL;
  WHERE_EACH(pCond, pLogicCond->pParameterList) {
    bool result = false;
    code = filterIsMultiTableColsCond(pCond, &result);
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }

    if (result || COND_TYPE_PRIMARY_KEY != filterClassifyCondition(pCond)) {
      WHERE_NEXT;
      continue;
    }

    SNode* pNew = NULL;
    code = nodesCloneNode(pCond, &pNew);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListMakeAppend(&pPrimaryKeyConds, pNew);
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
    
    ERASE_NODE(pLogicCond->pParameterList);
  }

  SNode *pTempPrimaryKeyCond = NULL;
  if (TSDB_CODE_SUCCESS == code && pPrimaryKeyConds) {
    code = nodesMergeConds(&pTempPrimaryKeyCond, &pPrimaryKeyConds);
  }

  if (TSDB_CODE_SUCCESS == code && pTempPrimaryKeyCond) {
    *pPrimaryKeyCond = pTempPrimaryKeyCond;

    if (pLogicCond->pParameterList->length <= 0) {
      nodesDestroyNode(*pCondition);
      *pCondition = NULL;
    }
  } else {
    nodesDestroyList(pPrimaryKeyConds);
    nodesDestroyNode(pTempPrimaryKeyCond);
  }

  return code;
}


int32_t hashJoinOptSplitPrimCond(SNode **pCondition, SNode **pPrimaryKeyCond) {
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(*pCondition)) {
    if (LOGIC_COND_TYPE_AND == ((SLogicConditionNode *)*pCondition)->condType) {
      return hashJoinOptSplitPrimFromLogicCond(pCondition, pPrimaryKeyCond);
    }

    return TSDB_CODE_SUCCESS;
  }

  bool needOutput = false;
  bool result = false;
  int32_t code = filterIsMultiTableColsCond(*pCondition, &result);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  if (result) {
    return TSDB_CODE_SUCCESS;
  }

  EConditionType type = filterClassifyCondition(*pCondition);
  if (COND_TYPE_PRIMARY_KEY == type) {
    *pPrimaryKeyCond = *pCondition;
    *pCondition = NULL;
  }

  return TSDB_CODE_SUCCESS;
}


static int32_t hashJoinOptRewriteJoin(SOptimizeContext* pCxt, SLogicNode* pNode, SLogicSubplan* pLogicSubplan) {
  SJoinLogicNode* pJoin = (SJoinLogicNode*)pNode;
  int32_t code = TSDB_CODE_SUCCESS;

  pJoin->joinAlgo = JOIN_ALGO_HASH;

  if (NULL != pJoin->pColOnCond) {
#if 0  
    EJoinType t = pJoin->joinType;
    EJoinSubType s = pJoin->subType;

    pJoin->joinType = JOIN_TYPE_INNER;
    pJoin->subType = JOIN_STYPE_NONE;
    
    code = pdcJoinSplitCond(pJoin, &pJoin->pColOnCond, NULL, &pJoin->pLeftOnCond, &pJoin->pRightOnCond, true);

    pJoin->joinType = t;
    pJoin->subType = s;

    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }

    STimeWindow ltimeRange = TSWINDOW_INITIALIZER;
    STimeWindow rtimeRange = TSWINDOW_INITIALIZER;
    SNode* pPrimaryKeyCond = NULL;
    if (NULL != pJoin->pLeftOnCond) {
      hashJoinOptSplitPrimCond(&pJoin->pLeftOnCond, &pPrimaryKeyCond);
      if (NULL != pPrimaryKeyCond) {
        bool isStrict = false;
        code = getTimeRangeFromNode(&pPrimaryKeyCond, &ltimeRange, &isStrict);
        nodesDestroyNode(pPrimaryKeyCond);
      }
    }

    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    
    if (NULL != pJoin->pRightOnCond) {
      pPrimaryKeyCond = NULL;
      hashJoinOptSplitPrimCond(&pJoin->pRightOnCond, &pPrimaryKeyCond);
      if (NULL != pPrimaryKeyCond) {
        bool isStrict = false;
        code = getTimeRangeFromNode(&pPrimaryKeyCond, &rtimeRange, &isStrict);
        nodesDestroyNode(pPrimaryKeyCond);
      }
    }    

    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }

    if (TSWINDOW_IS_EQUAL(ltimeRange, TSWINDOW_INITIALIZER)) {
      pJoin->timeRange.skey = rtimeRange.skey;
      pJoin->timeRange.ekey = rtimeRange.ekey;
    } else if (TSWINDOW_IS_EQUAL(rtimeRange, TSWINDOW_INITIALIZER)) {
      pJoin->timeRange.skey = ltimeRange.skey;
      pJoin->timeRange.ekey = ltimeRange.ekey;
    } else if (ltimeRange.ekey < rtimeRange.skey || ltimeRange.skey > rtimeRange.ekey) {
      pJoin->timeRange = TSWINDOW_DESC_INITIALIZER;
    } else {
      pJoin->timeRange.skey = TMAX(ltimeRange.skey, rtimeRange.skey);
      pJoin->timeRange.ekey = TMIN(ltimeRange.ekey, rtimeRange.ekey);
    }
#else 
    SNode* pPrimaryKeyCond = NULL;
    code = hashJoinOptSplitPrimCond(&pJoin->pColOnCond, &pPrimaryKeyCond);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    if (NULL != pPrimaryKeyCond) {
      bool isStrict = false;
      code = getTimeRangeFromNode(&pPrimaryKeyCond, &pJoin->timeRange, &isStrict);
      nodesDestroyNode(pPrimaryKeyCond);
    }
#endif
  } else {
    pJoin->timeRange = TSWINDOW_INITIALIZER;
  }

#if 0
  if (NULL != pJoin->pTagOnCond && !TSWINDOW_IS_EQUAL(pJoin->timeRange, TSWINDOW_DESC_INITIALIZER)) {
    EJoinType t = pJoin->joinType;
    EJoinSubType s = pJoin->subType;
    SNode*  pLeftChildCond = NULL;
    SNode*  pRightChildCond = NULL;

    pJoin->joinType = JOIN_TYPE_INNER;
    pJoin->subType = JOIN_STYPE_NONE;
    
    code = pdcJoinSplitCond(pJoin, &pJoin->pTagOnCond, NULL, &pLeftChildCond, &pRightChildCond, true);

    pJoin->joinType = t;
    pJoin->subType = s;

    if (TSDB_CODE_SUCCESS == code) {
      code = mergeJoinConds(&pJoin->pLeftOnCond, &pLeftChildCond);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = mergeJoinConds(&pJoin->pRightOnCond, &pRightChildCond);
    }

    nodesDestroyNode(pLeftChildCond);
    nodesDestroyNode(pRightChildCond);

    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }
#endif

  if (!TSWINDOW_IS_EQUAL(pJoin->timeRange, TSWINDOW_DESC_INITIALIZER)) {
    SNode* pChild = NULL;
    FOREACH(pChild, pJoin->node.pChildren) {
      if (QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pChild)) {
        continue;
      }

      SScanLogicNode* pScan = (SScanLogicNode*)pChild;
      if (TSWINDOW_IS_EQUAL(pScan->scanRange, TSWINDOW_INITIALIZER)) {
        continue;
      } else if (pJoin->timeRange.ekey < pScan->scanRange.skey || pJoin->timeRange.skey > pScan->scanRange.ekey) {
        pJoin->timeRange = TSWINDOW_DESC_INITIALIZER;
        break;
      } else {
        pJoin->timeRange.skey = TMAX(pJoin->timeRange.skey, pScan->scanRange.skey);
        pJoin->timeRange.ekey = TMIN(pJoin->timeRange.ekey, pScan->scanRange.ekey);
      }
    }
  }

  pJoin->timeRangeTarget = 0;

  if (!TSWINDOW_IS_EQUAL(pJoin->timeRange, TSWINDOW_INITIALIZER)) {
    SNode* pChild = NULL;
    int32_t timeRangeTarget = 1;
    FOREACH(pChild, pJoin->node.pChildren) {
      if (QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pChild)) {
        timeRangeTarget++;
        continue;
      }
    
      SScanLogicNode* pScan = (SScanLogicNode*)pChild;
      if (TSWINDOW_IS_EQUAL(pScan->scanRange, pJoin->timeRange)) {
        timeRangeTarget++;
        continue;
      }

      bool replaced = false;
      switch (pJoin->joinType) {
        case JOIN_TYPE_INNER:
          pScan->scanRange.skey = pJoin->timeRange.skey;
          pScan->scanRange.ekey = pJoin->timeRange.ekey;
          replaced = true;
          break;
        case JOIN_TYPE_LEFT:
          if (2 == timeRangeTarget) {
            pScan->scanRange.skey = pJoin->timeRange.skey;
            pScan->scanRange.ekey = pJoin->timeRange.ekey;
            replaced = true;
          }
          break;
        case JOIN_TYPE_RIGHT:
          if (1 == timeRangeTarget) {
            pScan->scanRange.skey = pJoin->timeRange.skey;
            pScan->scanRange.ekey = pJoin->timeRange.ekey;
            replaced = true;
          }
          break;
        default:
          break;
      }

      if (replaced) {
        timeRangeTarget++;
        continue;
      }
      
      pJoin->timeRangeTarget += timeRangeTarget;
      timeRangeTarget++;
    }
  }

  pCxt->optimized = true;
  OPTIMIZE_FLAG_SET_MASK(pJoin->node.optimizedFlag, OPTIMIZE_FLAG_STB_JOIN);
  
  return TSDB_CODE_SUCCESS;
}


static int32_t hashJoinOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  int32_t code = 0;
  SLogicNode* pNode = optFindPossibleNode(pLogicSubplan->pNode, hashJoinOptShouldBeOptimized, &code);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  if (NULL == pNode) {
    return TSDB_CODE_SUCCESS;
  }

  return hashJoinOptRewriteJoin(pCxt, pNode, pLogicSubplan);
}

static bool stbJoinOptShouldBeOptimized(SLogicNode* pNode, void* pCtx) {
  if (QUERY_NODE_LOGIC_PLAN_JOIN != nodeType(pNode) || OPTIMIZE_FLAG_TEST_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_STB_JOIN)) {
    return false;
  }

  SJoinLogicNode* pJoin = (SJoinLogicNode*)pNode;
  if (pJoin->joinAlgo == JOIN_ALGO_UNKNOWN) {
    pJoin->joinAlgo = JOIN_ALGO_MERGE;
  }

  if (JOIN_STYPE_NONE != pJoin->subType || pJoin->isSingleTableJoin || NULL == pJoin->pTagEqCond || pNode->pChildren->length != 2 
      || pJoin->isLowLevelJoin) {
    return false;
  }

  SNode* pLeft = nodesListGetNode(pJoin->node.pChildren, 0);
  SNode* pRight = nodesListGetNode(pJoin->node.pChildren, 1);
  if (QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pLeft) || QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pRight)) {
    return false;
  }

  return true;
}

int32_t stbJoinOptAddFuncToScanNode(char* funcName, SScanLogicNode* pScan) {
  SFunctionNode* pUidFunc = NULL;
  int32_t code = createFunction(funcName, NULL, &pUidFunc);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  snprintf(pUidFunc->node.aliasName, sizeof(pUidFunc->node.aliasName), "%s.%p", pUidFunc->functionName, pUidFunc);
  code = nodesListStrictAppend(pScan->pScanPseudoCols, (SNode*)pUidFunc);
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

  SNodeList* pTags = NULL;
  int32_t code = nodesMakeList(&pTags);
  if (TSDB_CODE_SUCCESS  == code) {
    code = nodesCollectColumnsFromNode(pJoinNode->pTagEqCond, NULL, COLLECT_COL_TYPE_TAG, &pTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesCollectColumnsFromNode(pJoinNode->pTagOnCond, NULL, COLLECT_COL_TYPE_TAG, &pTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    SNode* pTarget = NULL;
    SNode* pTag = NULL;
    bool   found = false;
    WHERE_EACH(pTarget, pScan->node.pTargets) {
      found = false;
      SColumnNode* pTargetCol = (SColumnNode*)pTarget;
      FOREACH(pTag, pTags) {
        SColumnNode* pTagCol = (SColumnNode*)pTag;
        if (0 == strcasecmp(pTargetCol->node.aliasName, pTagCol->colName) && 0 == strcasecmp(pTargetCol->tableAlias, pTagCol->tableAlias)) {
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

  if (TSDB_CODE_SUCCESS == code) {
    code = tagScanSetExecutionMode(pScan);
  }

  if (code) {
    nodesDestroyList(pTags);
  }

  return code;
}

static int32_t stbJoinOptCreateTagScanNode(SLogicNode* pJoin, SNodeList** ppList) {
  SNodeList* pList = NULL;
  int32_t code = nodesCloneList(pJoin->pChildren, &pList);
  if (NULL == pList) {
    return code;
  }

  SNode*  pNode = NULL;
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
  SJoinLogicNode* pJoin = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_JOIN, (SNode**)&pJoin);
  if (NULL == pJoin) {
    return code;
  }

  pJoin->joinType = pOrigJoin->joinType;
  pJoin->subType = pOrigJoin->subType;
  pJoin->joinAlgo = JOIN_ALGO_HASH;
  pJoin->isSingleTableJoin = pOrigJoin->isSingleTableJoin;
  pJoin->hasSubQuery = pOrigJoin->hasSubQuery;
  pJoin->node.inputTsOrder = pOrigJoin->node.inputTsOrder;
  pJoin->node.groupAction = pOrigJoin->node.groupAction;
  pJoin->node.requireDataOrder = DATA_ORDER_LEVEL_NONE;
  pJoin->node.resultDataOrder = DATA_ORDER_LEVEL_NONE;
  code = nodesCloneNode(pOrigJoin->pTagEqCond, &pJoin->pTagEqCond);
  if (TSDB_CODE_SUCCESS  != code) {
    nodesDestroyNode((SNode*)pJoin);
    return code;
  }
  code = nodesCloneNode(pOrigJoin->pTagOnCond, &pJoin->pTagOnCond);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pJoin);
    return code;
  }
  
  pJoin->node.pChildren = pChildren;

  SNode* pNode = NULL;
  FOREACH(pNode, pChildren) {
    SScanLogicNode* pScan = (SScanLogicNode*)pNode;
    SNode*          pCol = NULL;
    FOREACH(pCol, pScan->pScanPseudoCols) {
      if (QUERY_NODE_FUNCTION == nodeType(pCol) && (((SFunctionNode*)pCol)->funcType == FUNCTION_TYPE_TBUID ||
                                                    ((SFunctionNode*)pCol)->funcType == FUNCTION_TYPE_VGID)) {
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

  SNodeList* pCols = NULL;
  code = nodesCollectColumnsFromNode(pJoin->pFullOnCond, NULL, COLLECT_COL_TYPE_ALL, &pCols);

  if (TSDB_CODE_SUCCESS == code) {
    FOREACH(pNode, pCols) {
      code = createColumnByRewriteExpr(pNode, &pJoin->node.pTargets);
      if (code) {
        break;
      }
    }
  }
  nodesDestroyList(pCols);

  if (TSDB_CODE_SUCCESS == code) {
    *ppLogic = (SLogicNode*)pJoin;    
    OPTIMIZE_FLAG_SET_MASK(pJoin->node.optimizedFlag, OPTIMIZE_FLAG_STB_JOIN);
  } else {
    nodesDestroyNode((SNode*)pJoin);
  }

  return code;
}

static int32_t stbJoinOptCreateTableScanNodes(SLogicNode* pJoin, SNodeList** ppList, bool* srcScan) {
  SNodeList* pList = NULL;
  int32_t code = nodesCloneList(pJoin->pChildren, &pList);
  if (NULL == pList) {
    return code;
  }

  int32_t i = 0;
  SNode*  pNode = NULL;
  FOREACH(pNode, pList) {
    SScanLogicNode* pScan = (SScanLogicNode*)pNode;
    //code = stbJoinOptAddFuncToScanNode("_tbuid", pScan);
    //if (code) {
    //  break;
    //}

    nodesDestroyNode(pScan->pTagCond);
    pScan->pTagCond = NULL;
    nodesDestroyNode(pScan->pTagIndexCond);
    pScan->pTagIndexCond = NULL;
    
    pScan->node.dynamicOp = true;
    *(srcScan + i++) = pScan->pVgroupList->numOfVgroups <= 1;

    pScan->scanType = SCAN_TYPE_TABLE;
  }

  *ppList = pList;

  return code;
}

static int32_t stbJoinOptCreateGroupCacheNode(SLogicNode* pRoot, SNodeList* pChildren, SLogicNode** ppLogic) {
  int32_t               code = TSDB_CODE_SUCCESS;
  SGroupCacheLogicNode* pGrpCache = NULL;
  code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_GROUP_CACHE, (SNode**)&pGrpCache);
  if (NULL == pGrpCache) {
    return code;
  }

  // pGrpCache->node.dynamicOp = true;
  pGrpCache->grpColsMayBeNull = false;
  pGrpCache->grpByUid = true;
  pGrpCache->batchFetch = getBatchScanOptionFromHint(pRoot->pHint);
  pGrpCache->node.pChildren = pChildren;
  pGrpCache->node.pTargets = NULL;
  code = nodesMakeList(&pGrpCache->node.pTargets);
  if (TSDB_CODE_SUCCESS == code) {
    SScanLogicNode* pScan = (SScanLogicNode*)nodesListGetNode(pChildren, 0);
    SNodeList* pNewList = NULL;
    code = nodesCloneList(pScan->node.pTargets, &pNewList);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListStrictAppendList(pGrpCache->node.pTargets, pNewList);
    }
  }

  SScanLogicNode* pScan = (SScanLogicNode*)nodesListGetNode(pChildren, 0);
  SNode*          pCol = NULL;
  FOREACH(pCol, pScan->pScanPseudoCols) {
    if (QUERY_NODE_FUNCTION == nodeType(pCol) && (((SFunctionNode*)pCol)->funcType == FUNCTION_TYPE_TBUID ||
                                                  ((SFunctionNode*)pCol)->funcType == FUNCTION_TYPE_VGID)) {
      code = createColumnByRewriteExpr(pCol, &pGrpCache->pGroupCols);
      if (code) {
        break;
      }
    }
  }

  bool   hasCond = false;
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
  if (QUERY_NODE_OPERATOR == nodeType(pJoin->pFullOnCond) && nodesEqualNode(pJoin->pFullOnCond, pJoin->pTagEqCond)) {
    NODES_DESTORY_NODE(pJoin->pFullOnCond);
    return;
  }
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pJoin->pFullOnCond)) {
    SLogicConditionNode* pLogic = (SLogicConditionNode*)pJoin->pFullOnCond;
    SNode* pNode = NULL;
    FOREACH(pNode, pLogic->pParameterList) {
      if (nodesEqualNode(pNode, pJoin->pTagEqCond)) {
        ERASE_NODE(pLogic->pParameterList);
        break;
      } else if (QUERY_NODE_LOGIC_CONDITION == nodeType(pJoin->pTagEqCond)) {
        SLogicConditionNode* pTags = (SLogicConditionNode*)pJoin->pTagEqCond;
        SNode* pTag = NULL;
        bool found = false;
        FOREACH(pTag, pTags->pParameterList) {
          if (nodesEqualNode(pTag, pNode)) {
            found = true;
            break;
          }
        }
        if (found) {
          ERASE_NODE(pLogic->pParameterList);
        }
      }
    }

    if (pLogic->pParameterList->length <= 0) {
      NODES_DESTORY_NODE(pJoin->pFullOnCond);
    }
  }
}

static int32_t stbJoinOptCreateMergeJoinNode(SLogicNode* pOrig, SLogicNode* pChild, SLogicNode** ppLogic) {
  SJoinLogicNode* pOrigJoin = (SJoinLogicNode*)pOrig;
  SJoinLogicNode* pJoin = NULL;
  int32_t code = nodesCloneNode((SNode*)pOrig, (SNode**)&pJoin);
  if (NULL == pJoin) {
    return code;
  }

  pJoin->joinAlgo = JOIN_ALGO_MERGE;
  // pJoin->node.dynamicOp = true;

  stbJoinOptRemoveTagEqCond(pJoin);
  NODES_DESTORY_NODE(pJoin->pTagEqCond);

  SNode* pNode = NULL;
  FOREACH(pNode, pJoin->node.pChildren) { ERASE_NODE(pJoin->node.pChildren); }
  code = nodesListStrictAppend(pJoin->node.pChildren, (SNode*)pChild);
  if (TSDB_CODE_SUCCESS == code) {
    pChild->pParent = (SLogicNode*)pJoin;
    *ppLogic = (SLogicNode*)pJoin;
    OPTIMIZE_FLAG_SET_MASK(pJoin->node.optimizedFlag, OPTIMIZE_FLAG_STB_JOIN);
  } else {
    nodesDestroyNode((SNode*)pJoin);
  }

  return code;
}

static int32_t stbJoinOptCreateDynQueryCtrlNode(SLogicNode* pRoot, SLogicNode* pPrev, SLogicNode* pPost, bool* srcScan,
                                                SLogicNode** ppDynNode) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  SDynQueryCtrlLogicNode* pDynCtrl = NULL;
  code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL, (SNode**)&pDynCtrl);
  if (NULL == pDynCtrl) {
    return code;
  }

  pDynCtrl->qType = DYN_QTYPE_STB_HASH;
  pDynCtrl->stbJoin.batchFetch = getBatchScanOptionFromHint(pRoot->pHint);
  memcpy(pDynCtrl->stbJoin.srcScan, srcScan, sizeof(pDynCtrl->stbJoin.srcScan));

  if (TSDB_CODE_SUCCESS == code) {
    pDynCtrl->node.pChildren = NULL;
    code = nodesMakeList(&pDynCtrl->node.pChildren);
    if (NULL == pDynCtrl->node.pChildren) {
      code = code;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    pDynCtrl->stbJoin.pVgList = NULL;
    code = nodesMakeList(&pDynCtrl->stbJoin.pVgList);
    if (NULL == pDynCtrl->stbJoin.pVgList) {
      code = code;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    pDynCtrl->stbJoin.pUidList = NULL;
    code = nodesMakeList(&pDynCtrl->stbJoin.pUidList);
    if (NULL == pDynCtrl->stbJoin.pUidList) {
      code = code;
    }
  }

  SJoinLogicNode* pHJoin = (SJoinLogicNode*)pPrev;
  code = nodesListStrictAppend(pDynCtrl->stbJoin.pUidList, nodesListGetNode(pHJoin->node.pTargets, 0));
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListStrictAppend(pDynCtrl->stbJoin.pUidList, nodesListGetNode(pHJoin->node.pTargets, 2));
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListStrictAppend(pDynCtrl->stbJoin.pVgList, nodesListGetNode(pHJoin->node.pTargets, 1));
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListStrictAppend(pDynCtrl->stbJoin.pVgList, nodesListGetNode(pHJoin->node.pTargets, 3));
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListStrictAppend(pDynCtrl->node.pChildren, (SNode*)pPrev);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListStrictAppend(pDynCtrl->node.pChildren, (SNode*)pPost);
    }
    if (TSDB_CODE_SUCCESS == code) {
      pDynCtrl->node.pTargets = NULL;
      code = nodesCloneList(pPost->pTargets, &pDynCtrl->node.pTargets);
      if (!pDynCtrl->node.pTargets) {
        code = code;
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
  SLogicNode* pNode = optFindPossibleNode(pLogicSubplan->pNode, stbJoinOptShouldBeOptimized, NULL);
  if (NULL == pNode) {
    return TSDB_CODE_SUCCESS;
  }

  return stbJoinOptRewriteStableJoin(pCxt, pNode, pLogicSubplan);
}

static bool grpJoinOptShouldBeOptimized(SLogicNode* pNode, void* pCtx) {
  if (QUERY_NODE_LOGIC_PLAN_JOIN != nodeType(pNode)) {
    return false;
  }

  SJoinLogicNode* pJoin = (SJoinLogicNode*)pNode;
  if (JOIN_STYPE_ASOF != pJoin->subType && JOIN_STYPE_WIN != pJoin->subType) {
    return false;
  }

  if (NULL == pJoin->pLeftEqNodes || pJoin->grpJoin) {
    return false;
  }

  return true;
}

static int32_t grpJoinOptCreatePartitionNode(SLogicNode* pParent, SLogicNode* pChild, bool leftChild, SLogicNode** pNew) {
  SPartitionLogicNode* pPartition = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_PARTITION, (SNode**)&pPartition);
  if (NULL == pPartition) {
    return code;
  }

  pPartition->node.groupAction = GROUP_ACTION_SET;
  pPartition->node.requireDataOrder = DATA_ORDER_LEVEL_GLOBAL;
  pPartition->node.resultDataOrder = DATA_ORDER_LEVEL_IN_GROUP;

  pPartition->node.pTargets = NULL;
  code = nodesCloneList(pChild->pTargets, &pPartition->node.pTargets);
  if (NULL == pPartition->node.pTargets) {
    nodesDestroyNode((SNode*)pPartition);
    return code;
  }

  SJoinLogicNode* pJoin = (SJoinLogicNode*)pParent;
  pPartition->pPartitionKeys = NULL;
  code = nodesCloneList(leftChild ? pJoin->pLeftEqNodes : pJoin->pRightEqNodes, &pPartition->pPartitionKeys);
  if (TSDB_CODE_SUCCESS  != code) {
    nodesDestroyNode((SNode*)pPartition);
    return code;
  }
  code = nodesListMakeStrictAppend(&pPartition->node.pChildren, (SNode *)pChild);
  if (TSDB_CODE_SUCCESS  == code) {
    *pNew = (SLogicNode*)pPartition;
    pChild->pParent = (SLogicNode*)pPartition;
    pPartition->node.pParent = pParent;
  } else {
    nodesDestroyNode((SNode*)pPartition);
  }
  return code;
}

static int32_t grpJoinOptInsertPartitionNode(SLogicNode* pJoin) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode* pNode = NULL;
  SNode* pNew = NULL;
  bool leftChild = true;
  FOREACH(pNode, pJoin->pChildren) {
    code = grpJoinOptCreatePartitionNode(pJoin, (SLogicNode*)pNode, leftChild, (SLogicNode**)&pNew);
    if (code) {
      break;
    }
    REPLACE_NODE(pNew);
    leftChild = false;
  }

  return code;
}

static int32_t grpJoinOptPartByTags(SLogicNode* pNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode* pChild = NULL;
  SNode* pNew = NULL;
  bool leftChild = true;
  SJoinLogicNode* pJoin = (SJoinLogicNode*)pNode;
  FOREACH(pChild, pNode->pChildren) {
    if (QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pChild)) {
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }

    SScanLogicNode* pScan = (SScanLogicNode*)pChild;
    SNodeList* pNewList = NULL;
    code = nodesCloneList(pJoin->pLeftEqNodes, &pNewList);
    if (TSDB_CODE_SUCCESS == code) {
      if (leftChild) {
        code = nodesListMakeStrictAppendList(&pScan->pGroupTags, pNewList);
        leftChild = false;
      } else {
        code = nodesListMakeStrictAppendList(&pScan->pGroupTags, pNewList);
      }
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
    
    pScan->groupSort = true;
    pScan->groupOrderScan = true;
  }

  return code;
}

static int32_t grpJoinOptRewriteGroupJoin(SOptimizeContext* pCxt, SLogicNode* pNode, SLogicSubplan* pLogicSubplan) {
  SJoinLogicNode* pJoin = (SJoinLogicNode*)pNode;
  int32_t code = (pJoin->allEqTags && !pJoin->hasSubQuery && !pJoin->batchScanHint) ? grpJoinOptPartByTags(pNode) : grpJoinOptInsertPartitionNode(pNode);
  if (TSDB_CODE_SUCCESS == code) {
    pJoin->grpJoin = true;
    pCxt->optimized = true;
  }
  return code;
}


static int32_t groupJoinOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SLogicNode* pNode = optFindPossibleNode(pLogicSubplan->pNode, grpJoinOptShouldBeOptimized, NULL);
  if (NULL == pNode) {
    return TSDB_CODE_SUCCESS;
  }

  return grpJoinOptRewriteGroupJoin(pCxt, pNode, pLogicSubplan);
}

static bool partColOptShouldBeOptimized(SLogicNode* pNode, void* pCtx) {
  if (QUERY_NODE_LOGIC_PLAN_PARTITION == nodeType(pNode)) {
    SPartitionLogicNode* pPartition = (SPartitionLogicNode*)pNode;
    if (keysHasCol(pPartition->pPartitionKeys)) return true;
  }
  return false;
}

static int32_t partColOptCreateSort(SPartitionLogicNode* pPartition, SSortLogicNode** ppSort) {
  SNode*          node;
  int32_t         code = TSDB_CODE_SUCCESS;
  SSortLogicNode* pSort = NULL;
  code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SORT, (SNode**)&pSort);
  if (pSort) {
    bool alreadyPartByPKTs = false;
    pSort->groupSort = false;
    FOREACH(node, pPartition->pPartitionKeys) {
      SOrderByExprNode* pOrder = NULL;
      code = nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR, (SNode**)&pOrder);
      if (TSDB_CODE_SUCCESS  != code) {
        break;
      }
      if (QUERY_NODE_COLUMN == nodeType(node) && ((SColumnNode*)node)->colId == pPartition->pkTsColId &&
          ((SColumnNode*)node)->tableId == pPartition->pkTsColTbId)
        alreadyPartByPKTs = true;
      code = nodesListMakeStrictAppend(&pSort->pSortKeys, (SNode*)pOrder);
      if (TSDB_CODE_SUCCESS  == code) {
        pOrder->order = ORDER_ASC;
        pOrder->pExpr = NULL;
        pOrder->nullOrder = NULL_ORDER_FIRST;
        code = nodesCloneNode(node, &pOrder->pExpr);
      }
      if (TSDB_CODE_SUCCESS  != code) {
        break;
      }
    }
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode((SNode*)pSort);
      return code;
    }

    if (pPartition->needBlockOutputTsOrder && !alreadyPartByPKTs) {
      SOrderByExprNode* pOrder = NULL;
      code = nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR, (SNode**)&pOrder);
      if (pOrder) {
        pSort->excludePkCol = true;
        code = nodesListMakeStrictAppend(&pSort->pSortKeys, (SNode*)pOrder);
        if (TSDB_CODE_SUCCESS  == code) {
          pOrder->order = ORDER_ASC;
          pOrder->pExpr = 0;
          FOREACH(node, pPartition->node.pTargets) {
            if (nodeType(node) == QUERY_NODE_COLUMN) {
              SColumnNode* pCol = (SColumnNode*)node;
              if (pCol->colId == pPartition->pkTsColId && pCol->tableId == pPartition->pkTsColTbId) {
                pOrder->pExpr = NULL;
                code = nodesCloneNode((SNode*)pCol, &pOrder->pExpr);
                break;
              }
            }
          }
        }
      }
    }
  }
  if (code != TSDB_CODE_SUCCESS) {
    nodesDestroyNode((SNode*)pSort);
    pSort = NULL;
  } else {
    *ppSort = pSort;
  }
  return code;
}

static int32_t partitionColsOpt(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SNode*               node;
  int32_t              code = TSDB_CODE_SUCCESS;
  SPartitionLogicNode* pNode =
      (SPartitionLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, partColOptShouldBeOptimized, NULL);
  if (NULL == pNode) return TSDB_CODE_SUCCESS;
  SLogicNode* pRootNode = getLogicNodeRootNode((SLogicNode*)pNode);

  if (pRootNode->pHint && getSortForGroupOptHint(pRootNode->pHint)) {
    // replace with sort node
    SSortLogicNode* pSort = NULL;
    code = partColOptCreateSort(pNode, &pSort);
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
        nodesDestroyNode((SNode*)pNode);
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
      SGroupingSetNode* pgsNode = NULL;
      code = nodesMakeNode(QUERY_NODE_GROUPING_SET, (SNode**)&pgsNode);
      if (code == TSDB_CODE_SUCCESS) {
        pgsNode->groupingSetType = GP_TYPE_NORMAL;
        pgsNode->pParameterList = NULL;
        code = nodesMakeList(&pgsNode->pParameterList);
      }
      if (code == TSDB_CODE_SUCCESS) {
        SNode* pNew = NULL;
        code = nodesCloneNode(node, &pNew);
        if (TSDB_CODE_SUCCESS == code) {
          code = nodesListStrictAppend(pgsNode->pParameterList, pNew);
        }
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

static bool tsmaOptMayBeOptimized(SLogicNode* pNode, void* pCtx) {
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    SNode*          pTmpNode;
    SNodeList*      pFuncs = NULL;
    SScanLogicNode* pScan = (SScanLogicNode*)pNode;
    SLogicNode*     pParent = pScan->node.pParent;
    SNode*          pConds = pScan->node.pConditions;

    if (pScan->scanType != SCAN_TYPE_TABLE || !pParent || pConds) return false;
    if (!pScan->pTsmas || pScan->pTsmas->size <= 0) {
      return false;
    }

    switch (nodeType(pParent)) {
      case QUERY_NODE_LOGIC_PLAN_WINDOW: {
        SWindowLogicNode* pWindow = (SWindowLogicNode*)pParent;
        // only time window interval supported
        if (pWindow->winType != WINDOW_TYPE_INTERVAL) return false;
        pFuncs = pWindow->pFuncs;
      } break;
      case QUERY_NODE_LOGIC_PLAN_AGG: {
        SAggLogicNode* pAgg = (SAggLogicNode*)pParent;
        // group/partition by normal cols not supported
        if (pAgg->pGroupKeys) return false;
        pFuncs = pAgg->pAggFuncs;
      } break;
      default:
        return false;
    }

    FOREACH(pTmpNode, pFuncs) {
      SFunctionNode* pFunc = (SFunctionNode*)pTmpNode;
      if (!fmIsTSMASupportedFunc(pFunc->funcId) && !fmIsPseudoColumnFunc(pFunc->funcId) &&
          !fmIsGroupKeyFunc(pFunc->funcId)) {
        return false;
      }
    }

    return true;
  }
  return false;
}

typedef struct STSMAOptUsefulTsma {
  const STableTSMAInfo* pTsma;          // NULL if no tsma available, which will use original data for calculation
  STimeWindow           scanRange;      // scan time range for this tsma
  SArray*               pTsmaScanCols;  // SArray<int32_t> index of tsmaFuncs array
  char                  targetTbName[TSDB_TABLE_NAME_LEN];  // the scanning table name, used only when pTsma is not NULL
  uint64_t              targetTbUid;                        // the scanning table uid, used only when pTsma is not NULL
  int8_t                precision;
} STSMAOptUsefulTsma;

typedef struct STSMAOptCtx {
  // input
  SScanLogicNode*    pScan;
  SLogicNode*        pParent;  // parent of Table Scan, Agg or Interval
  const SNodeList*   pAggFuncs;
  const STimeWindow* pTimeRange;
  const SArray*      pTsmas;
  SInterval*         queryInterval;  // not null with window logic node
  int8_t             precision;

  // output
  SArray*        pUsefulTsmas;  // SArray<STSMAOptUseFulTsma>, sorted by tsma interval from long to short
  SArray*        pUsedTsmas;
  SLogicSubplan* generatedSubPlans[2];
  SNodeList**    ppParentTsmaSubplans;
} STSMAOptCtx;

static int32_t fillTSMAOptCtx(STSMAOptCtx* pTsmaOptCtx, SScanLogicNode* pScan) {
  int32_t code = 0;
  pTsmaOptCtx->pScan = pScan;
  pTsmaOptCtx->pParent = pScan->node.pParent;
  pTsmaOptCtx->pTsmas = pScan->pTsmas;
  pTsmaOptCtx->pTimeRange = &pScan->scanRange;
  pTsmaOptCtx->precision = pScan->node.precision;

  if (nodeType(pTsmaOptCtx->pParent) == QUERY_NODE_LOGIC_PLAN_WINDOW) {
    pTsmaOptCtx->queryInterval = taosMemoryCalloc(1, sizeof(SInterval));
    if (!pTsmaOptCtx->queryInterval) return terrno;

    SWindowLogicNode* pWindow = (SWindowLogicNode*)pTsmaOptCtx->pParent;
    pTsmaOptCtx->queryInterval->interval = pWindow->interval;
    pTsmaOptCtx->queryInterval->intervalUnit = pWindow->intervalUnit;
    pTsmaOptCtx->queryInterval->offset = pWindow->offset;
    pTsmaOptCtx->queryInterval->offsetUnit = pWindow->intervalUnit;
    pTsmaOptCtx->queryInterval->sliding = pWindow->sliding;
    pTsmaOptCtx->queryInterval->slidingUnit = pWindow->slidingUnit;
    pTsmaOptCtx->queryInterval->precision = pWindow->node.precision;
    pTsmaOptCtx->queryInterval->tz = tsTimezone;
    pTsmaOptCtx->pAggFuncs = pWindow->pFuncs;
    pTsmaOptCtx->ppParentTsmaSubplans = &pWindow->pTsmaSubplans;
  } else {
    SAggLogicNode* pAgg = (SAggLogicNode*)pTsmaOptCtx->pParent;
    pTsmaOptCtx->pAggFuncs = pAgg->pAggFuncs;
    pTsmaOptCtx->ppParentTsmaSubplans = &pAgg->pTsmaSubplans;
  }
  pTsmaOptCtx->pUsefulTsmas = taosArrayInit(pScan->pTsmas->size, sizeof(STSMAOptUsefulTsma));
  pTsmaOptCtx->pUsedTsmas = taosArrayInit(3, sizeof(STSMAOptUsefulTsma));
  if (!pTsmaOptCtx->pUsefulTsmas || !pTsmaOptCtx->pUsedTsmas) {
    code = terrno;
  }
  return code;
}

static void tsmaOptFreeUsefulTsma(void* p) {
  STSMAOptUsefulTsma* pTsma = p;
  taosArrayDestroy(pTsma->pTsmaScanCols);
}

static void clearTSMAOptCtx(STSMAOptCtx* pTsmaOptCtx) {
  taosArrayDestroyEx(pTsmaOptCtx->pUsefulTsmas, tsmaOptFreeUsefulTsma);
  taosArrayDestroy(pTsmaOptCtx->pUsedTsmas);
  taosMemoryFreeClear(pTsmaOptCtx->queryInterval);
}

static bool tsmaOptCheckValidInterval(int64_t tsmaInterval, int8_t unit, const STSMAOptCtx* pTsmaOptCtx) {
  if (!pTsmaOptCtx->queryInterval) return true;

  bool validInterval = checkRecursiveTsmaInterval(tsmaInterval, unit, pTsmaOptCtx->queryInterval->interval,
                                                  pTsmaOptCtx->queryInterval->intervalUnit,
                                                  pTsmaOptCtx->queryInterval->precision, false);
  bool validSliding =
      checkRecursiveTsmaInterval(tsmaInterval, unit, pTsmaOptCtx->queryInterval->sliding,
                                 pTsmaOptCtx->queryInterval->slidingUnit, pTsmaOptCtx->queryInterval->precision, false);
  bool validOffset =
      pTsmaOptCtx->queryInterval->offset == 0 ||
      checkRecursiveTsmaInterval(tsmaInterval, unit, pTsmaOptCtx->queryInterval->offset,
                                 pTsmaOptCtx->queryInterval->offsetUnit, pTsmaOptCtx->queryInterval->precision, false);
  return validInterval && validSliding && validOffset;
}

static int32_t tsmaOptCheckValidFuncs(const SArray* pTsmaFuncs, const SNodeList* pQueryFuncs, SArray* pTsmaScanCols,
                                      bool* pIsValid) {
  SNode*  pNode;
  bool    failed = false, found = false;

  taosArrayClear(pTsmaScanCols);
  FOREACH(pNode, pQueryFuncs) {
    SFunctionNode* pQueryFunc = (SFunctionNode*)pNode;
    if (fmIsPseudoColumnFunc(pQueryFunc->funcId) || fmIsGroupKeyFunc(pQueryFunc->funcId)) continue;
    if (nodeType(pQueryFunc->pParameterList->pHead->pNode) != QUERY_NODE_COLUMN) {
      failed = true;
      break;
    }
    int32_t queryColId = ((SColumnNode*)pQueryFunc->pParameterList->pHead->pNode)->colId;
    found = false;
    int32_t notMyStateFuncId = -1;
    // iterate funcs
    for (int32_t i = 0; i < pTsmaFuncs->size; i++) {
      STableTSMAFuncInfo* pTsmaFuncInfo = taosArrayGet(pTsmaFuncs, i);
      if (pTsmaFuncInfo->funcId == notMyStateFuncId) continue;

      if (!fmIsMyStateFunc(pQueryFunc->funcId, pTsmaFuncInfo->funcId)) {
        notMyStateFuncId = pTsmaFuncInfo->funcId;
        continue;
      }

      if (queryColId != pTsmaFuncInfo->colId) {
        continue;
      }
      found = true;
      if (NULL == taosArrayPush(pTsmaScanCols, &i)) {
        return terrno;
      }
      break;
    }
    if (failed || !found) {
      break;
    }
  }
  *pIsValid = found;
  return TSDB_CODE_SUCCESS;
}

typedef struct STsmaOptTagCheckCtx {
  const STableTSMAInfo* pTsma;
  bool  ok;
} STsmaOptTagCheckCtx;

static EDealRes tsmaOptTagCheck(SNode* pNode, void* pContext) {
  bool found = false;
  if (nodeType(pNode) == QUERY_NODE_COLUMN) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    if (pCol->colType == COLUMN_TYPE_TAG) {
      STsmaOptTagCheckCtx* pCtx = pContext;
      for (int32_t i = 0; i < pCtx->pTsma->pTags->size; ++i) {
        SSchema* pSchema = taosArrayGet(pCtx->pTsma->pTags, i);
        if (strcmp(pSchema->name, pCol->colName) == 0) {
          found = true;
        }
      }
      if (!found) {
        pCtx->ok = false;
        return DEAL_RES_END;
      }
    }
  }
  return DEAL_RES_CONTINUE;
}

static bool tsmaOptCheckTags(STSMAOptCtx* pCtx, const STableTSMAInfo* pTsma) {
  const SScanLogicNode* pScan = pCtx->pScan;
  STsmaOptTagCheckCtx ctx = {.pTsma = pTsma, .ok = true};
  nodesWalkExpr(pScan->pTagCond, tsmaOptTagCheck, &ctx);
  if (!ctx.ok) return false;
  nodesWalkExprs(pScan->pScanPseudoCols, tsmaOptTagCheck, &ctx);
  if (!ctx.ok) return false;
  nodesWalkExprs(pScan->pGroupTags, tsmaOptTagCheck, &ctx);
  return ctx.ok;
}

static int32_t tsmaOptFilterTsmas(STSMAOptCtx* pTsmaOptCtx) {
  STSMAOptUsefulTsma usefulTsma = {
      .pTsma = NULL, .scanRange = {.skey = TSKEY_MIN, .ekey = TSKEY_MAX}, .precision = pTsmaOptCtx->precision};
  SArray*            pTsmaScanCols = NULL;
  int32_t            code = 0;

  for (int32_t i = 0; i < pTsmaOptCtx->pTsmas->size; ++i) {
    if (!pTsmaScanCols) {
      pTsmaScanCols = taosArrayInit(pTsmaOptCtx->pAggFuncs->length, sizeof(int32_t));
      if (!pTsmaScanCols) return terrno;
    }
    if (pTsmaOptCtx->pScan->tableType == TSDB_CHILD_TABLE || pTsmaOptCtx->pScan->tableType == TSDB_NORMAL_TABLE) {
      const STsmaTargetTbInfo* ptbInfo = taosArrayGet(pTsmaOptCtx->pScan->pTsmaTargetTbInfo, i);
      if (ptbInfo->uid == 0) continue; // tsma res table meta not found, skip this tsma, this is possible when there is no data in this ctb
    }

    STableTSMAInfo* pTsma = taosArrayGetP(pTsmaOptCtx->pTsmas, i);
    if (!pTsma->fillHistoryFinished || tsMaxTsmaCalcDelay * 1000 < (pTsma->rspTs - pTsma->reqTs) + pTsma->delayDuration) {
      continue;
    }
    // filter with interval
    if (!tsmaOptCheckValidInterval(pTsma->interval, pTsma->unit, pTsmaOptCtx)) {
      continue;
    }
    // filter with funcs, note that tsma funcs has been sorted by funcId and ColId
    bool valid = false;
    int32_t code = tsmaOptCheckValidFuncs(pTsma->pFuncs, pTsmaOptCtx->pAggFuncs, pTsmaScanCols, &valid);
    if (TSDB_CODE_SUCCESS != code)  break;
    if (!valid) continue;

    if (!tsmaOptCheckTags(pTsmaOptCtx, pTsma)) continue;
    usefulTsma.pTsma = pTsma;
    usefulTsma.pTsmaScanCols = pTsmaScanCols;
    pTsmaScanCols = NULL;
    if (NULL == taosArrayPush(pTsmaOptCtx->pUsefulTsmas, &usefulTsma)) {
      if (pTsmaScanCols) {
        taosArrayDestroy(pTsmaScanCols);
      }
      return terrno;
    }
  }
  if (pTsmaScanCols) taosArrayDestroy(pTsmaScanCols);
  return code;
}

static int32_t tsmaInfoCompWithIntervalDesc(const void* pLeft, const void* pRight) {
  const int64_t factors[3] = {NANOSECOND_PER_MSEC, NANOSECOND_PER_USEC, 1};
  const STSMAOptUsefulTsma *p = pLeft, *q = pRight;
  int64_t                   pInterval = p->pTsma->interval, qInterval = q->pTsma->interval;
  int8_t                    pUnit = p->pTsma->unit, qUnit = q->pTsma->unit;
  if (TIME_UNIT_MONTH == pUnit) {
    pInterval = pInterval * 31 * (NANOSECOND_PER_DAY / factors[p->precision]);
  } else if (TIME_UNIT_YEAR == pUnit){
    pInterval = pInterval * 365 * (NANOSECOND_PER_DAY / factors[p->precision]);
  }
  if (TIME_UNIT_MONTH == qUnit) {
    qInterval = qInterval * 31 * (NANOSECOND_PER_DAY / factors[q->precision]);
  } else if (TIME_UNIT_YEAR == qUnit){
    qInterval = qInterval * 365 * (NANOSECOND_PER_DAY / factors[q->precision]);
  }

  if (pInterval > qInterval) return -1;
  if (pInterval < qInterval) return 1;
  return 0;
}

static void tsmaOptInitIntervalFromTsma(SInterval* pInterval, const STableTSMAInfo* pTsma, int8_t precision) {
  pInterval->interval = pTsma->interval;
  pInterval->intervalUnit = pTsma->unit;
  pInterval->sliding = pTsma->interval;
  pInterval->slidingUnit = pTsma->unit;
  pInterval->offset = 0;
  pInterval->offsetUnit = pTsma->unit;
  pInterval->precision = precision;
}

static const STSMAOptUsefulTsma* tsmaOptFindUsefulTsma(const SArray* pUsefulTsmas, int32_t startIdx,
                                                       int64_t startAlignInterval, int64_t endAlignInterval,
                                                       int8_t precision) {
  SInterval tsmaInterval;
  for (int32_t i = startIdx; i < pUsefulTsmas->size; ++i) {
    const STSMAOptUsefulTsma* pUsefulTsma = taosArrayGet(pUsefulTsmas, i);
    tsmaOptInitIntervalFromTsma(&tsmaInterval, pUsefulTsma->pTsma, precision);
    if (taosTimeTruncate(startAlignInterval, &tsmaInterval) == startAlignInterval &&
        taosTimeTruncate(endAlignInterval, &tsmaInterval) == endAlignInterval) {
      return pUsefulTsma;
    }
  }
  return NULL;
}

static int32_t tsmaOptSplitWindows(STSMAOptCtx* pTsmaOptCtx, const STimeWindow* pScanRange) {
  bool                      needTailWindow = false;
  bool                      isSkeyAlignedWithTsma = true, isEkeyAlignedWithTsma = true;
  int32_t                   code = 0;
  int64_t                   winSkey = TSKEY_MIN, winEkey = TSKEY_MAX;
  int64_t                   startOfSkeyFirstWin = pScanRange->skey, endOfSkeyFirstWin;
  int64_t                   startOfEkeyFirstWin = pScanRange->ekey, endOfEkeyFirstWin;
  SInterval                 interval, tsmaInterval;
  STimeWindow               scanRange = *pScanRange;
  const SInterval*          pInterval = pTsmaOptCtx->queryInterval;
  const STSMAOptUsefulTsma* pUsefulTsma = taosArrayGet(pTsmaOptCtx->pUsefulTsmas, 0);
  const STableTSMAInfo*     pTsma = pUsefulTsma->pTsma;

  if (pScanRange->ekey <= pScanRange->skey) return code;

  if (!pInterval) {
    tsmaOptInitIntervalFromTsma(&interval, pTsma, pTsmaOptCtx->precision);
    pInterval = &interval;
  }

  tsmaOptInitIntervalFromTsma(&tsmaInterval, pTsma, pTsmaOptCtx->precision);

  // check for head windows
  if (pScanRange->skey != TSKEY_MIN) {
    startOfSkeyFirstWin = taosTimeTruncate(pScanRange->skey, pInterval);
    endOfSkeyFirstWin =
        taosTimeAdd(startOfSkeyFirstWin, pInterval->interval, pInterval->intervalUnit, pTsmaOptCtx->precision);
    isSkeyAlignedWithTsma = taosTimeTruncate(pScanRange->skey, &tsmaInterval) == pScanRange->skey;
  } else {
    endOfSkeyFirstWin = TSKEY_MIN;
  }

  // check for tail windows
  if (pScanRange->ekey != TSKEY_MAX) {
    startOfEkeyFirstWin = taosTimeTruncate(pScanRange->ekey, pInterval);
    endOfEkeyFirstWin =
        taosTimeAdd(startOfEkeyFirstWin, pInterval->interval, pInterval->intervalUnit, pTsmaOptCtx->precision);
    isEkeyAlignedWithTsma = taosTimeTruncate(pScanRange->ekey + 1, &tsmaInterval) == (pScanRange->ekey + 1);
    if (startOfEkeyFirstWin > startOfSkeyFirstWin) {
      needTailWindow = true;
    }
  }

  // add head tsma if possible
  if (!isSkeyAlignedWithTsma) {
    scanRange.ekey = TMIN(
        scanRange.ekey,
        taosTimeAdd(startOfSkeyFirstWin, pInterval->interval * 1, pInterval->intervalUnit, pTsmaOptCtx->precision) - 1);
    const STSMAOptUsefulTsma* pTsmaFound =
        tsmaOptFindUsefulTsma(pTsmaOptCtx->pUsefulTsmas, 1, scanRange.skey, scanRange.ekey + 1, pTsmaOptCtx->precision);
    STSMAOptUsefulTsma usefulTsma = {.pTsma = pTsmaFound ? pTsmaFound->pTsma : NULL,
                                     .scanRange = scanRange,
                                     .pTsmaScanCols = pTsmaFound ? pTsmaFound->pTsmaScanCols : NULL};
    if (NULL == taosArrayPush(pTsmaOptCtx->pUsedTsmas, &usefulTsma))
      return terrno;
  }

  // the main tsma
  if (endOfSkeyFirstWin < startOfEkeyFirstWin || (endOfSkeyFirstWin == startOfEkeyFirstWin && (isSkeyAlignedWithTsma || isEkeyAlignedWithTsma))) {
    scanRange.ekey =
        TMIN(pScanRange->ekey, isEkeyAlignedWithTsma ? pScanRange->ekey : startOfEkeyFirstWin - 1);
    if (!isSkeyAlignedWithTsma) {
      scanRange.skey = endOfSkeyFirstWin;
    }
    STSMAOptUsefulTsma usefulTsma = {
        .pTsma = pTsma, .scanRange = scanRange, .pTsmaScanCols = pUsefulTsma->pTsmaScanCols};
    if (NULL == taosArrayPush(pTsmaOptCtx->pUsedTsmas, &usefulTsma))
      return terrno;
  }

  // add tail tsma if possible
  if (!isEkeyAlignedWithTsma && needTailWindow) {
    scanRange.skey = startOfEkeyFirstWin;
    scanRange.ekey = pScanRange->ekey;
    const STSMAOptUsefulTsma* pTsmaFound =
        tsmaOptFindUsefulTsma(pTsmaOptCtx->pUsefulTsmas, 1, scanRange.skey - startOfEkeyFirstWin,
                              scanRange.ekey + 1 - startOfEkeyFirstWin, pTsmaOptCtx->precision);
    STSMAOptUsefulTsma usefulTsma = {.pTsma = pTsmaFound ? pTsmaFound->pTsma : NULL,
                                     .scanRange = scanRange,
                                     .pTsmaScanCols = pTsmaFound ? pTsmaFound->pTsmaScanCols : NULL};
    if (NULL == taosArrayPush(pTsmaOptCtx->pUsedTsmas, &usefulTsma))
      return terrno;
  }
  return code;
}

int32_t tsmaOptCreateTsmaScanCols(const STSMAOptUsefulTsma* pTsma, const SNodeList* pAggFuncs, SNodeList** ppList) {
  if (!pTsma->pTsma || !pTsma->pTsmaScanCols) {
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }
  int32_t    code;
  SNode*     pNode;
  SNodeList* pScanCols = NULL;

  int32_t i = 0;

  FOREACH(pNode, pAggFuncs) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    if (fmIsPseudoColumnFunc(pFunc->funcId) || fmIsGroupKeyFunc(pFunc->funcId)) {
      continue;
    }
    const int32_t* idx = taosArrayGet(pTsma->pTsmaScanCols, i);
    SColumnNode*   pCol = NULL;
    code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
    if (pCol) {
      pCol->colId = *idx + 2;
      pCol->tableType = TSDB_SUPER_TABLE;
      pCol->tableId = pTsma->targetTbUid;
      pCol->colType = COLUMN_TYPE_COLUMN;
      strcpy(pCol->tableName, pTsma->targetTbName);
      strcpy(pCol->dbName, pTsma->pTsma->targetDbFName);
      strcpy(pCol->colName, pFunc->node.aliasName);
      strcpy(pCol->node.aliasName, pFunc->node.aliasName);
      pCol->node.resType.type = TSDB_DATA_TYPE_BINARY;
      code = nodesListMakeStrictAppend(&pScanCols, (SNode*)pCol);
    }
    if (code) break;
    ++i;
  }

  if (code) {
    nodesDestroyList(pScanCols);
    pScanCols = NULL;
  } else {
    *ppList = pScanCols;
  }
  return code;
}

static int32_t tsmaOptRewriteTag(const STSMAOptCtx* pTsmaOptCtx, const STSMAOptUsefulTsma* pTsma,
                                 SColumnNode* pTagCol) {
  bool found = false;
  if (pTagCol->colType != COLUMN_TYPE_TAG) return 0;
  for (int32_t i = 0; i < pTsma->pTsma->pTags->size; ++i) {
    const SSchema* pSchema = taosArrayGet(pTsma->pTsma->pTags, i);
    if (strcmp(pTagCol->colName, pSchema->name) == 0) {
      strcpy(pTagCol->tableName, pTsma->targetTbName);
      strcpy(pTagCol->tableAlias, pTsma->targetTbName);
      pTagCol->tableId = pTsma->targetTbUid;
      pTagCol->tableType = TSDB_SUPER_TABLE;
      pTagCol->colId = pSchema->colId;
      found = true;
      break;
    }
  }
  return found ? TSDB_CODE_SUCCESS : TSDB_CODE_PLAN_INTERNAL_ERROR;
}

static int32_t tsmaOptRewriteTbname(const STSMAOptCtx* pTsmaOptCtx, SNode** pTbNameNode,
                                    const STSMAOptUsefulTsma* pTsma) {
  int32_t     code = 0;
  SExprNode*  pRewrittenFunc = NULL;
  code = nodesMakeNode(pTsma ? QUERY_NODE_COLUMN : QUERY_NODE_FUNCTION, (SNode**)&pRewrittenFunc);
  SValueNode* pValue = NULL;
  if (code == TSDB_CODE_SUCCESS) {
    pRewrittenFunc->resType = ((SExprNode*)(*pTbNameNode))->resType;
  }

  if (pTsma && code == TSDB_CODE_SUCCESS) {
    nodesDestroyNode(*pTbNameNode);
    SColumnNode* pCol = (SColumnNode*)pRewrittenFunc;
    const SSchema* pSchema = taosArrayGet(pTsma->pTsma->pTags, pTsma->pTsma->pTags->size - 1);
    strcpy(pCol->tableName, pTsma->targetTbName);
    strcpy(pCol->tableAlias, pTsma->targetTbName);
    pCol->tableId = pTsma->targetTbUid;
    pCol->tableType = TSDB_SUPER_TABLE;
    pCol->colId = pSchema->colId;
    pCol->colType = COLUMN_TYPE_TAG;
  } else if (code == TSDB_CODE_SUCCESS) {
    // if no tsma, we replace func tbname with concat('', tbname)
    SFunctionNode* pFunc = (SFunctionNode*)pRewrittenFunc;
    pFunc->funcId = fmGetFuncId("concat");
    snprintf(pFunc->functionName, TSDB_FUNC_NAME_LEN, "concat");
    code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pValue);
    if (!pValue) code = TSDB_CODE_OUT_OF_MEMORY;

    if (code == TSDB_CODE_SUCCESS) {
      pValue->translate = true;
      pValue->node.resType = ((SExprNode*)(*pTbNameNode))->resType;
      pValue->literal = taosMemoryCalloc(1, TSDB_TABLE_FNAME_LEN + 1);
      pValue->datum.p = taosMemoryCalloc(1, TSDB_TABLE_FNAME_LEN + 1 + VARSTR_HEADER_SIZE);
      if (!pValue->literal || !pValue->datum.p) code = terrno;
    }

    if (code == TSDB_CODE_SUCCESS) {
      code = nodesListMakeStrictAppend(&pFunc->pParameterList, (SNode*)pValue);
      pValue = NULL;
    }
    if (code == TSDB_CODE_SUCCESS) {
      code = nodesListStrictAppend(pFunc->pParameterList, *pTbNameNode);
    }
  }

  if (code == TSDB_CODE_SUCCESS) {
    *pTbNameNode = (SNode*)pRewrittenFunc;
  } else {
    nodesDestroyNode((SNode*)pRewrittenFunc);
    if (pValue) nodesDestroyNode((SNode*)pValue);
  }

  return code;
}

struct TsmaOptRewriteCtx {
  const STSMAOptCtx*        pTsmaOptCtx;
  const STSMAOptUsefulTsma* pTsma;
  bool                      rewriteTag;
  bool                      rewriteTbname;
  int32_t                   code;
};

EDealRes tsmaOptNodeRewriter(SNode** ppNode, void* ctx) {
  SNode*                    pNode = *ppNode;
  int32_t                   code = 0;
  struct TsmaOptRewriteCtx* pCtx = ctx;
  if (pCtx->rewriteTag && nodeType(pNode) == QUERY_NODE_COLUMN && ((SColumnNode*)pNode)->colType == COLUMN_TYPE_TAG) {
    code = tsmaOptRewriteTag(pCtx->pTsmaOptCtx, pCtx->pTsma, (SColumnNode*)pNode);
  } else if (pCtx->rewriteTbname &&
             ((nodeType(pNode) == QUERY_NODE_FUNCTION && ((SFunctionNode*)pNode)->funcType == FUNCTION_TYPE_TBNAME) ||
              (nodeType(pNode) == QUERY_NODE_COLUMN && ((SColumnNode*)pNode)->colType == COLUMN_TYPE_TBNAME))) {
    code = tsmaOptRewriteTbname(pCtx->pTsmaOptCtx, ppNode, pCtx->pTsma);
    if (code == TSDB_CODE_SUCCESS) return DEAL_RES_IGNORE_CHILD;
  }
  if (code) {
    pCtx->code = code;
    return DEAL_RES_ERROR;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t tsmaOptRewriteNode(SNode** pNode, STSMAOptCtx* pCtx, const STSMAOptUsefulTsma* pTsma, bool rewriteTbName, bool rewriteTag) {
  struct TsmaOptRewriteCtx ctx = {
      .pTsmaOptCtx = pCtx, .pTsma = pTsma, .rewriteTag = rewriteTag, .rewriteTbname = rewriteTbName, .code = 0};
  SNode* pOut = *pNode;
  nodesRewriteExpr(&pOut, tsmaOptNodeRewriter, &ctx);
  if (ctx.code == TSDB_CODE_SUCCESS) *pNode = pOut;
  return ctx.code;
}

static int32_t tsmaOptRewriteNodeList(SNodeList* pNodes, STSMAOptCtx* pCtx, const STSMAOptUsefulTsma* pTsma,
                                      bool rewriteTbName, bool rewriteTag) {
  int32_t code = 0;
  SNode*  pNode;
  FOREACH(pNode, pNodes) {
    SNode* pOut = pNode;
    code = tsmaOptRewriteNode(&pOut, pCtx, pTsma, rewriteTbName, rewriteTag);
    if (TSDB_CODE_SUCCESS != code) break;
    REPLACE_NODE(pOut);
  }
  return code;
}

static int32_t tsmaOptRewriteScan(STSMAOptCtx* pTsmaOptCtx, SScanLogicNode* pNewScan, const STSMAOptUsefulTsma* pTsma) {
  SNode*  pNode;
  int32_t code = 0;

  pNewScan->scanRange.skey = pTsma->scanRange.skey;
  pNewScan->scanRange.ekey = pTsma->scanRange.ekey;

  if (pTsma->pTsma) {
    // PK col
    SColumnNode* pPkTsCol = NULL;
    FOREACH(pNode, pNewScan->pScanCols) {
      SColumnNode* pCol = (SColumnNode*)pNode;
      if (pCol->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
        pPkTsCol = NULL;
        code = nodesCloneNode((SNode*)pCol, (SNode**)&pPkTsCol);
        break;
      }
    }
    if (code == TSDB_CODE_SUCCESS) {
      nodesDestroyList(pNewScan->pScanCols);
      // normal cols
      pNewScan->pScanCols = NULL;
      code = tsmaOptCreateTsmaScanCols(pTsma, pTsmaOptCtx->pAggFuncs, &pNewScan->pScanCols);
    }
    if (code == TSDB_CODE_SUCCESS && pPkTsCol) {
      tstrncpy(pPkTsCol->tableName, pTsma->targetTbName, TSDB_TABLE_NAME_LEN);
      tstrncpy(pPkTsCol->tableAlias, pTsma->targetTbName, TSDB_TABLE_NAME_LEN);
      pPkTsCol->tableId = pTsma->targetTbUid;
      code = nodesListMakeStrictAppend(&pNewScan->pScanCols, (SNode*)pPkTsCol);
    } else if (pPkTsCol){
      nodesDestroyNode((SNode*)pPkTsCol);
    }
    if (code == TSDB_CODE_SUCCESS) {
      pNewScan->stableId = pTsma->pTsma->destTbUid;
      pNewScan->tableId = pTsma->targetTbUid;
      strcpy(pNewScan->tableName.tname, pTsma->targetTbName);
    }
    if (code == TSDB_CODE_SUCCESS) {
      code = tsmaOptRewriteNodeList(pNewScan->pScanPseudoCols, pTsmaOptCtx, pTsma, true, true);
    }
    if (code == TSDB_CODE_SUCCESS) {
      code = tsmaOptRewriteNode(&pNewScan->pTagCond, pTsmaOptCtx, pTsma, true, true);
    }
    if (code == TSDB_CODE_SUCCESS) {
      code = tsmaOptRewriteNodeList(pNewScan->pGroupTags, pTsmaOptCtx, pTsma, true, true);
    }
    if (TSDB_CODE_SUCCESS == code) {
      pTsmaOptCtx->pScan->dataRequired = FUNC_DATA_REQUIRED_DATA_LOAD;
      if (pTsmaOptCtx->pScan->pTsmaTargetTbVgInfo && pTsmaOptCtx->pScan->pTsmaTargetTbVgInfo->size > 0) {
        for (int32_t i = 0; i < taosArrayGetSize(pTsmaOptCtx->pScan->pTsmas); ++i) {
          STableTSMAInfo* pTsmaInfo = taosArrayGetP(pTsmaOptCtx->pScan->pTsmas, i);
          if (pTsmaInfo == pTsma->pTsma) {
            const SVgroupsInfo* pVgpsInfo = taosArrayGetP(pTsmaOptCtx->pScan->pTsmaTargetTbVgInfo, i);
            taosMemoryFreeClear(pNewScan->pVgroupList);
            int32_t len = sizeof(int32_t) + sizeof(SVgroupInfo) * pVgpsInfo->numOfVgroups;
            pNewScan->pVgroupList = taosMemoryCalloc(1, len);
            if (!pNewScan->pVgroupList) {
              code = terrno;
              break;
            }
            memcpy(pNewScan->pVgroupList, pVgpsInfo, len);
            break;
          }
        }
      }
    }
  } else {
    FOREACH(pNode, pNewScan->pGroupTags) {
      // rewrite tbname recursively
      struct TsmaOptRewriteCtx ctx = {
          .pTsmaOptCtx = pTsmaOptCtx, .pTsma = NULL, .rewriteTag = false, .rewriteTbname = true, .code = 0};
      nodesRewriteExpr(&pNode, tsmaOptNodeRewriter, &ctx);
      if (ctx.code) {
        code = ctx.code;
      } else {
        REPLACE_NODE(pNode);
      }
    }
  }
  return code;
}

static int32_t tsmaOptCreateWStart(int8_t precision, SFunctionNode** pWStartOut) {
  SFunctionNode* pWStart = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pWStart);
  if (NULL == pWStart) {
    return code;
  }
  strcpy(pWStart->functionName, "_wstart");
  int64_t pointer = (int64_t)pWStart;
  char    name[TSDB_COL_NAME_LEN + TSDB_POINTER_PRINT_BYTES + TSDB_NAME_DELIMITER_LEN + 1] = {0};
  int32_t len = tsnprintf(name, sizeof(name) - 1, "%s.%" PRId64 "", pWStart->functionName, pointer);
  (void)taosHashBinary(name, len);
  strncpy(pWStart->node.aliasName, name, TSDB_COL_NAME_LEN - 1);
  pWStart->node.resType.precision = precision;

  code = fmGetFuncInfo(pWStart, NULL, 0);
  if (code) {
    nodesDestroyNode((SNode*)pWStart);
  } else {
    *pWStartOut = pWStart;
  }
  return code;
}

static int32_t tsmaOptRewriteParent(STSMAOptCtx* pTsmaOptCtx, SLogicNode* pParent, SScanLogicNode* pScan,
                                  const STSMAOptUsefulTsma* pTsma) {
  int32_t           code = 0;
  SColumnNode*      pColNode;
  SWindowLogicNode* pWindow = NULL;
  SAggLogicNode*    pAgg;
  SNodeList*        pAggFuncs;
  SListCell*        pScanListCell;
  SNode*            pAggFuncNode;
  SNodeList*        pAggStateFuncs = NULL;
  bool              isFirstMergeNode = pTsmaOptCtx->pScan == pScan;
  SFunctionNode *   pPartial = NULL, *pMerge = NULL;

  if (nodeType(pParent) == QUERY_NODE_LOGIC_PLAN_WINDOW) {
    pWindow = (SWindowLogicNode*)pParent;
    pAggFuncs = pWindow->pFuncs;
  } else {
    pAgg = (SAggLogicNode*)pParent;
    pAggFuncs = pAgg->pAggFuncs;
  }
  pScanListCell = pScan->pScanCols->pHead;

  FOREACH(pAggFuncNode, pAggFuncs) {
    SFunctionNode* pAggFunc = (SFunctionNode*)pAggFuncNode;
    if (fmIsGroupKeyFunc(pAggFunc->funcId)) {
      struct TsmaOptRewriteCtx ctx = {
          .pTsmaOptCtx = pTsmaOptCtx, .pTsma = pTsma, .rewriteTag = true, .rewriteTbname = true, .code = 0};
      nodesRewriteExpr(&pAggFuncNode, tsmaOptNodeRewriter, &ctx);
      if (ctx.code) {
        code = ctx.code;
        break;
      } else {
        REPLACE_NODE(pAggFuncNode);
      }
      continue;
    } else if (fmIsPseudoColumnFunc(pAggFunc->funcId)) {
      continue;
    }
    code = fmGetDistMethod(pAggFunc, &pPartial, NULL, &pMerge);
    if (code) break;

    pColNode = (SColumnNode*)pScanListCell->pNode;
    pScanListCell = pScanListCell->pNext;
    pColNode->node.resType = pPartial->node.resType;
    // currently we assume that the first parameter must be the scan column
    (void)nodesListErase(pMerge->pParameterList, pMerge->pParameterList->pHead);
    SNode* pNew = NULL;
    code = nodesCloneNode((SNode*)pColNode, &pNew);
    if (TSDB_CODE_SUCCESS  == code) {
      code = nodesListPushFront(pMerge->pParameterList, pNew);
    }
    nodesDestroyNode((SNode*)pPartial);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode(pNew);
      break;
    }

    REPLACE_NODE(pMerge);
  }

  if (code == TSDB_CODE_SUCCESS && pWindow) {
    SColumnNode* pCol = (SColumnNode*)pScan->pScanCols->pTail->pNode;
    nodesDestroyNode(pWindow->pTspk);
    pWindow->pTspk = NULL;
    code = nodesCloneNode((SNode*)pCol, &pWindow->pTspk);
  }

  if (code == TSDB_CODE_SUCCESS) {
    nodesDestroyList(pScan->node.pTargets);
    code = createColumnByRewriteExprs(pScan->pScanCols, &pScan->node.pTargets);
  }
  if (code == TSDB_CODE_SUCCESS) {
    code = createColumnByRewriteExprs(pScan->pScanPseudoCols, &pScan->node.pTargets);
  }

  return code;
}

static int32_t tsmaOptGeneratePlan(STSMAOptCtx* pTsmaOptCtx) {
  int32_t                   code = 0;
  const STSMAOptUsefulTsma* pTsma = NULL;
  SNodeList*                pAggFuncs = NULL;
  bool                      hasSubPlan = false;

  for (int32_t i = 0; i < pTsmaOptCtx->pUsedTsmas->size; ++i) {
    STSMAOptUsefulTsma* pTsma = taosArrayGet(pTsmaOptCtx->pUsedTsmas, i);
    if (!pTsma->pTsma) continue;
    if (pTsmaOptCtx->pScan->tableType == TSDB_CHILD_TABLE || pTsmaOptCtx->pScan->tableType == TSDB_NORMAL_TABLE) {
      for (int32_t j = 0; j < pTsmaOptCtx->pScan->pTsmas->size; ++j) {
        if (taosArrayGetP(pTsmaOptCtx->pScan->pTsmas, j) == pTsma->pTsma) {
          const STsmaTargetTbInfo* ptbInfo = taosArrayGet(pTsmaOptCtx->pScan->pTsmaTargetTbInfo, j);
          strcpy(pTsma->targetTbName, ptbInfo->tableName);
          pTsma->targetTbUid = ptbInfo->uid;
        }
      }
    } else {
      strcpy(pTsma->targetTbName, pTsma->pTsma->targetTb);
      pTsma->targetTbUid = pTsma->pTsma->destTbUid;
    }
  }

  for (int32_t i = 1; i < pTsmaOptCtx->pUsedTsmas->size && code == TSDB_CODE_SUCCESS; ++i) {
    pTsma = taosArrayGet(pTsmaOptCtx->pUsedTsmas, i);
    SLogicSubplan* pSubplan = NULL;
    code = nodesMakeNode(QUERY_NODE_LOGIC_SUBPLAN, (SNode**)&pSubplan);
    if (!pSubplan) {
      break;
    }
    pSubplan->subplanType = SUBPLAN_TYPE_SCAN;
    pTsmaOptCtx->generatedSubPlans[i - 1] = pSubplan;
    hasSubPlan = true;
    SLogicNode* pParent = NULL;
    code = nodesCloneNode((SNode*)pTsmaOptCtx->pParent, (SNode**)&pParent);
    if (!pParent) {
      break;
    }
    pSubplan->pNode = pParent;
    pParent->pParent = NULL;
    pParent->groupAction = GROUP_ACTION_KEEP;
    SScanLogicNode* pScan = (SScanLogicNode*)pParent->pChildren->pHead->pNode;
    code = tsmaOptRewriteScan(pTsmaOptCtx, pScan, pTsma);
    if (code == TSDB_CODE_SUCCESS && pTsma->pTsma) {
      code = tsmaOptRewriteParent(pTsmaOptCtx, pParent, pScan, pTsma);
    }
  }

  if (code == TSDB_CODE_SUCCESS) {
    pTsma = taosArrayGet(pTsmaOptCtx->pUsedTsmas, 0);
    pTsmaOptCtx->pScan->needSplit = hasSubPlan;
    code = tsmaOptRewriteScan(pTsmaOptCtx, pTsmaOptCtx->pScan, pTsma);
    if (code == TSDB_CODE_SUCCESS && pTsma->pTsma) {
      code = tsmaOptRewriteParent(pTsmaOptCtx, pTsmaOptCtx->pParent, pTsmaOptCtx->pScan, pTsma);
    }
  }

  return code;
}

static bool tsmaOptIsUsingTsmas(STSMAOptCtx* pCtx) {
  if (pCtx->pUsedTsmas->size == 0) {
    return false;
  }
  for (int32_t i = 0; i < pCtx->pUsedTsmas->size; ++i) {
    const STSMAOptUsefulTsma*pTsma = taosArrayGet(pCtx->pUsedTsmas, i);
    if (pTsma->pTsma) return true;
  }
  return false;
}

static int32_t tsmaOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  int32_t         code = 0;
  STSMAOptCtx     tsmaOptCtx = {0};
  SScanLogicNode* pScan = (SScanLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, tsmaOptMayBeOptimized, NULL);
  if (!pScan) return code;

  SLogicNode* pRootNode = getLogicNodeRootNode((SLogicNode*)pScan);
  if (getOptHint(pRootNode->pHint, HINT_SKIP_TSMA)) return code;

  code = fillTSMAOptCtx(&tsmaOptCtx, pScan);
  if (code == TSDB_CODE_SUCCESS) {
    // 1. extract useful tsmas
    code = tsmaOptFilterTsmas(&tsmaOptCtx);

    if (code == TSDB_CODE_SUCCESS && tsmaOptCtx.pUsefulTsmas->size > 0) {
      // 2. sort useful tsmas with interval
      taosArraySort(tsmaOptCtx.pUsefulTsmas, tsmaInfoCompWithIntervalDesc);
      // 3. split windows
      code = tsmaOptSplitWindows(&tsmaOptCtx, tsmaOptCtx.pTimeRange);
      if (TSDB_CODE_SUCCESS == code && tsmaOptIsUsingTsmas(&tsmaOptCtx)) {
        // 4. create logic plan
        code = tsmaOptGeneratePlan(&tsmaOptCtx);

        if (TSDB_CODE_SUCCESS == code) {
          for (int32_t i = 0; i < 2 && (TSDB_CODE_SUCCESS == code); i++) {
            SLogicSubplan* pSubplan = tsmaOptCtx.generatedSubPlans[i];
            if (!pSubplan) continue;
            pSubplan->subplanType = SUBPLAN_TYPE_SCAN;
            code = nodesListMakeAppend(tsmaOptCtx.ppParentTsmaSubplans, (SNode*)pSubplan);
          }
          pCxt->optimized = true;
        }
      }
    }
  }
  pScan->pTsmas = NULL;
  clearTSMAOptCtx(&tsmaOptCtx);
  return code;
}

// clang-format off
static const SOptimizeRule optimizeRuleSet[] = {
  {.pName = "ScanPath",                   .optimizeFunc = scanPathOptimize},
  {.pName = "PushDownCondition",          .optimizeFunc = pdcOptimize},
  {.pName = "EliminateNotNullCond",       .optimizeFunc = eliminateNotNullCondOptimize},
  {.pName = "JoinCondOptimize",           .optimizeFunc = joinCondOptimize},
  {.pName = "HashJoin",                   .optimizeFunc = hashJoinOptimize},
  {.pName = "StableJoin",                 .optimizeFunc = stableJoinOptimize},
  {.pName = "GroupJoin",                  .optimizeFunc = groupJoinOptimize},
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
  {.pName = "Tsma",                       .optimizeFunc = tsmaOptimize},
};
// clang-format on

static const int32_t optimizeRuleNum = (sizeof(optimizeRuleSet) / sizeof(SOptimizeRule));

static int32_t dumpLogicSubplan(const char* pRuleName, SLogicSubplan* pSubplan) {
  int32_t code = 0;
  if (!tsQueryPlannerTrace) {
    return code;
  }
  char* pStr = NULL;
  code = nodesNodeToString((SNode*)pSubplan, false, &pStr, NULL);
  if (TSDB_CODE_SUCCESS == code) {
    if (NULL == pRuleName) {
      qDebugL("before optimize, JsonPlan: %s", pStr);
    } else {
      qDebugL("apply optimize %s rule, JsonPlan: %s", pRuleName, pStr);
    }
    taosMemoryFree(pStr);
  }
  return code;
}

static int32_t applyOptimizeRule(SPlanContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SOptimizeContext cxt = {.pPlanCxt = pCxt, .optimized = false};
  bool             optimized = false;
  int32_t code = dumpLogicSubplan(NULL, pLogicSubplan);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
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
        code = dumpLogicSubplan(optimizeRuleSet[i].pName, pLogicSubplan);
        break;
      }
    }
  } while (optimized && (TSDB_CODE_SUCCESS == code));
  return code;
}

int32_t optimizeLogicPlan(SPlanContext* pCxt, SLogicSubplan* pLogicSubplan) {
  if (SUBPLAN_TYPE_MODIFY == pLogicSubplan->subplanType && NULL == pLogicSubplan->pNode->pChildren) {
    return TSDB_CODE_SUCCESS;
  }
  return applyOptimizeRule(pCxt, pLogicSubplan);
}
