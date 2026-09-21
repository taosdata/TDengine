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

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "extConnector.h"
#include "extTypeMap.h"
#include "filter.h"
#include "functionMgt.h"
#include "nodes.h"
#include "osMemPool.h"
#include "parser.h"
#include "planInt.h"
#include "systable.h"
#include "taoserror.h"
#include "tarray.h"
#include "tglobal.h"
#include "ttime.h"
#include "scalar.h"

#define OPTIMIZE_FLAG_MASK(n) (1 << n)

#define OPTIMIZE_FLAG_SCAN_PATH       OPTIMIZE_FLAG_MASK(0)
#define OPTIMIZE_FLAG_PUSH_DOWN_CONDE OPTIMIZE_FLAG_MASK(1)
#define OPTIMIZE_FLAG_STB_JOIN        OPTIMIZE_FLAG_MASK(2)
#define OPTIMIZE_FLAG_ELIMINATE_PROJ  OPTIMIZE_FLAG_MASK(3)
#define OPTIMIZE_FLAG_JOIN_COND       OPTIMIZE_FLAG_MASK(4)
#define OPTIMIZE_FLAG_VTB_WINDOW      OPTIMIZE_FLAG_MASK(5)
#define OPTIMIZE_FLAG_VTB_AGG         OPTIMIZE_FLAG_MASK(6)
#define OPTIMIZE_FLAG_ELIMINATE_VSCAN OPTIMIZE_FLAG_MASK(5)
#define OPTIMIZE_FLAG_FQ_PUSHDOWN     OPTIMIZE_FLAG_MASK(7)
#define OPTIMIZE_FLAG_LAST_SMA_NOTNULL OPTIMIZE_FLAG_MASK(8)
#define OPTIMIZE_FLAG_FQ_UNIQUE_LOCAL OPTIMIZE_FLAG_MASK(10)

#define OPTIMIZE_FLAG_SET_MASK(val, mask)   (val) |= (mask)
#define OPTIMIZE_FLAG_CLEAR_MASK(val, mask) (val) &= (~(mask))
#define OPTIMIZE_FLAG_TEST_MASK(val, mask)  (((val) & (mask)) != 0)

typedef struct SOptimizeContext {
  SPlanContext*         pPlanCxt;
  SExtSourceCapability* pExtCapability;
  SExtCapSessionOverlay* pSessionOverlay;
  int32_t*              pPushdownAttempt;
  bool                  optimized;
} SOptimizeContext;

typedef int32_t (*FOptimize)(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan);

typedef struct SOptimizeRule {
  char*     pName;
  FOptimize optimizeFunc;
} SOptimizeRule;

// Forward declarations for functions used before their definitions.
static int32_t fqPushdownOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan);
static int32_t fqInterpOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan);
static int32_t fqUnionAllOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan);
static int32_t fqUnionDeduplicateOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan);
static SLogicNode* fqFirstChild(const SLogicNode* pNode);
static SScanLogicNode* fqFindExternalLogicLeafScan(SLogicNode* pNode);
static SNode* findDepScanCol(SScanLogicNode* pScan, SColumnNode* pCol);
static void fqPlanDebugLogExprList(const char* pTag, const SNodeList* pExprs);
static void fqPlanDebugLogNodeOutputs(const char* pTag, const SLogicNode* pNode);

static bool fqDiagEnabled(void) {
  return NULL != getenv("FQ_DIAG");
}

static void fqDiagDumpExprList(const char* pTag, const SNodeList* pExprs) {
  if (!fqDiagEnabled()) {
    return;
  }

  fprintf(stderr, "[FQ-DIAG] %s count=%d\n", pTag ? pTag : "(null)",
          pExprs ? LIST_LENGTH((SNodeList*)pExprs) : 0);

  if (NULL == pExprs) {
    return;
  }

  int32_t idx = 0;
  SNode* pExpr = NULL;
  FOREACH(pExpr, pExprs) {
    const SNode* pInner = pExpr;
    if (QUERY_NODE_TARGET == nodeType((SNode*)pInner)) {
      pInner = ((const STargetNode*)pInner)->pExpr;
    }

    const char* pAlias = nodesIsExprNode((SNode*)pInner) ? ((const SExprNode*)pInner)->aliasName : "";
    const char* pUserAlias = nodesIsExprNode((SNode*)pInner) ? ((const SExprNode*)pInner)->userAlias : "";
    const char* pColName = (NULL != pInner && QUERY_NODE_COLUMN == nodeType((SNode*)pInner))
                               ? ((const SColumnNode*)pInner)->colName
                               : "";
    const char* pFuncName = (NULL != pInner && QUERY_NODE_FUNCTION == nodeType((SNode*)pInner))
                                ? ((const SFunctionNode*)pInner)->functionName
                                : "";
    fprintf(stderr,
            "[FQ-DIAG]   [%d] rawType=%d innerType=%d alias=%s userAlias=%s col=%s func=%s\n",
            idx, pExpr ? (int)nodeType(pExpr) : -1, pInner ? (int)nodeType((SNode*)pInner) : -1,
            pAlias, pUserAlias, pColName, pFuncName);
    ++idx;
  }
}

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

typedef struct SCpdIsVtableTsCondCxt {
  bool       condHasTs;
} SCpdIsVtableTsCondCxt;

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

# if 0
typedef struct SCpdCollRewriteTableColsCxt {
  int32_t    code;
  SSHashObj* pLeftTbls;
  SSHashObj* pRightTbls;
  SSHashObj* pLeftCols;
  SSHashObj* pRightCols;
} SCpdCollRewriteTableColsCxt;
# endif

typedef struct SCpdCollectTableColCxt {
  SSHashObj* pTables;
  SNodeList* pResCols;
  SHashObj*  pColHash;
  int32_t    errCode;
} SCpdCollectTableColCxt;

typedef struct SCollectColsCxt {
  SHashObj*  pColHash;
  int32_t    errCode;
} SCollectColsCxt;


typedef enum ECondAction {
  COND_ACTION_STAY = 1,
  COND_ACTION_PUSH_JOIN,
  COND_ACTION_PUSH_LEFT_CHILD,
  COND_ACTION_PUSH_RIGHT_CHILD,
  COND_ACTION_COPY_LEFT_CHILD,
  COND_ACTION_COPY_RIGHT_CHILD,
  // after supporting outer join, there are other possibilities
} ECondAction;

#define PUSH_DOWN_LEFT_FLT       (1 << 0)
#define PUSH_DOWN_RIGHT_FLT      (1 << 1)
#define PUSH_DOWN_ON_COND        (1 << 2)
#define PUSH_DOWN_LEFT_FLT_COPY  (1 << 3)
#define PUSH_DOWN_RIGHT_FLT_COPY (1 << 4)
#define PUSH_DONW_FLT_COND  (PUSH_DOWN_LEFT_FLT | PUSH_DOWN_RIGHT_FLT)
#define PUSH_DOWN_ALL_COND  (PUSH_DOWN_LEFT_FLT | PUSH_DOWN_RIGHT_FLT | PUSH_DOWN_ON_COND)

typedef struct SJoinOptimizeOpt {
  int8_t pushDownFlag;
} SJoinOptimizeOpt;

typedef bool (*FMayBeOptimized)(SLogicNode* pNode, void* pCtx);
typedef bool (*FShouldBeOptimized)(SLogicNode* pNode, void* pInfo);

static bool          pdcJoinIsSafeAsofCopyCond(SJoinLogicNode* pJoin, SNode* pCond, SSHashObj* pTables);
static bool          joinCondHasValidPrimKeyCond(SJoinLogicNode* pJoin);
static const char*   getJoinCondPKTable(SNode* pNode);
static EOperatorType asofJoinGetReverseOp(EOperatorType opType);
static bool          asofJoinGetMatchedCmpOp(SJoinLogicNode* pJoin, EOperatorType* pMatchedOpType);
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
    /* NONE                OUTER                  SEMI                  ANTI                   ASOF WINDOW */
    /*INNER*/ {{PUSH_DOWN_ALL_COND}, {0}, {0}, {0}, {0}, {0}},
    /*LEFT*/
    {{0},
     {PUSH_DOWN_LEFT_FLT},
     {PUSH_DOWN_LEFT_FLT},
     {PUSH_DOWN_LEFT_FLT},
     {PUSH_DOWN_LEFT_FLT | PUSH_DOWN_RIGHT_FLT_COPY},
     {PUSH_DOWN_LEFT_FLT}},
    /*RIGHT*/
    {{0},
     {PUSH_DOWN_RIGHT_FLT},
     {PUSH_DOWN_RIGHT_FLT},
     {PUSH_DOWN_RIGHT_FLT},
     {PUSH_DOWN_RIGHT_FLT | PUSH_DOWN_LEFT_FLT_COPY},
     {PUSH_DOWN_RIGHT_FLT}},
    /*FULL*/ {{0}, {0}, {0}, {0}, {0}, {0}},
};

static SJoinOptimizeOpt gJoinOnOpt[JOIN_TYPE_MAX_VALUE][JOIN_STYPE_MAX_VALUE] = {
    /* NONE                OUTER                  SEMI                  ANTI                   ASOF WINDOW */
    /*INNER*/ {{PUSH_DONW_FLT_COND}, {0}, {0}, {0}, {0}, {0}},
    /*LEFT*/ {{0}, {PUSH_DOWN_RIGHT_FLT}, {PUSH_DONW_FLT_COND}, {PUSH_DOWN_RIGHT_FLT}, {0}, {0}},
    /*RIGHT*/ {{0}, {PUSH_DOWN_LEFT_FLT}, {PUSH_DONW_FLT_COND}, {PUSH_DOWN_LEFT_FLT}, {0}, {0}},
    /*FULL*/ {{0}, {0}, {0}, {0}, {0}, {0}},
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
    int32_t        code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
    if (NULL == pFunc) {
      *(int32_t*)pContext = code;
      return DEAL_RES_ERROR;
    }
    tstrncpy(pFunc->functionName, "tbname", TSDB_FUNC_NAME_LEN);
    pFunc->funcType = FUNCTION_TYPE_TBNAME;
    pFunc->node.resType = ((SColumnNode*)*pNode)->node.resType;
    code = fmGetFuncInfo(pFunc, NULL, 0);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode((SNode*)pFunc);
      *(int32_t*)pContext = code;
      return DEAL_RES_ERROR;
    }
    nodesDestroyNode(*pNode);
    *pNode = (SNode*)pFunc;
    return DEAL_RES_IGNORE_CHILD;
  }

  if (QUERY_NODE_FUNCTION == nodeType(*pNode)) {
    SFunctionNode* pFunc = (SFunctionNode*)*pNode;
    if (0 == strcmp(pFunc->functionName, "tbname") && pFunc->funcId == 0) {
      int32_t code = fmGetFuncInfo(pFunc, NULL, 0);
      if (TSDB_CODE_SUCCESS != code) {
        *(int32_t*)pContext = code;
        return DEAL_RES_ERROR;
      }
      return DEAL_RES_IGNORE_CHILD;
    }
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
      } else {
        return;
      }
    case QUERY_NODE_LOGIC_PLAN_JOIN:
      pNode->outputTsOrder = order;
      break;
    case QUERY_NODE_LOGIC_PLAN_WINDOW:
      if (((SWindowLogicNode*)pNode)->winType == WINDOW_TYPE_INTERVAL && pNode->groupAction != GROUP_ACTION_KEEP) {
        return;
      }
      // Window output ts order default to be asc, and changed when doing sort by primary key optimization.
      // We stop propagate the original order to parents.
      // Use window output ts order instead.
      order = pNode->outputTsOrder;
      break;
    case QUERY_NODE_LOGIC_PLAN_PROJECT:
      if (projectCouldMergeUnsortDataBlock((SProjectLogicNode*)pNode)) {
        pNode->outputTsOrder = TSDB_ORDER_NONE;
        return;
      }
      pNode->outputTsOrder = order;
      break;
    case QUERY_NODE_LOGIC_PLAN_PARTITION: {
      return;
    }
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
  // FQ guard: external scan has no scanOrder/dataRequired/dynamicScanFuncs semantics.
  if (SCAN_TYPE_EXTERNAL == ((SScanLogicNode*)pNode)->scanType) {
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

static const SWindowLogicNode* scanPathOptGetWindowParent(SScanLogicNode* pScan) {
  SLogicNode* pParent = pScan->node.pParent;
  if (QUERY_NODE_LOGIC_PLAN_PARTITION == nodeType(pParent) && pParent->pParent &&
      QUERY_NODE_LOGIC_PLAN_WINDOW == nodeType(pParent->pParent)) {
    pParent = pParent->pParent;
  }
  return QUERY_NODE_LOGIC_PLAN_WINDOW == nodeType(pParent) ? (SWindowLogicNode*)pParent : NULL;
}

static bool intervalWindowHasOverlappingWindows(const SWindowLogicNode* pWindow) {
  return pWindow->interval != pWindow->sliding || pWindow->intervalUnit != pWindow->slidingUnit;
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

static bool scanPathOptLastNeedsAscScan(SScanLogicNode* pScan) {
  const SWindowLogicNode* pWindow = scanPathOptGetWindowParent(pScan);
  return NULL != pWindow && WINDOW_TYPE_INTERVAL == pWindow->winType && intervalWindowHasOverlappingWindows(pWindow);
}

static int32_t scanPathOptGetScanOrder(SScanLogicNode* pScan, EScanOrder* pScanOrder) {
  SNodeList* pAllFuncs = scanPathOptGetAllFuncs(pScan->node.pParent);
  SNode*     pNode = NULL;
  bool       hasFirst = false;
  bool       hasLast = false;
  bool       otherFunc = false;
  FOREACH(pNode, pAllFuncs) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    if (FUNCTION_TYPE_FIRST == pFunc->funcType || FUNCTION_TYPE_FIRST_PARTIAL == pFunc->funcType) {
      hasFirst = true;
    } else if (FUNCTION_TYPE_LAST == pFunc->funcType || FUNCTION_TYPE_LAST_ROW == pFunc->funcType || FUNCTION_TYPE_LAST_PARTIAL == pFunc->funcType) {
      hasLast = true;
    } else if (FUNCTION_TYPE_SELECT_VALUE != pFunc->funcType) {
      otherFunc = true;
    }
  }
  if (hasFirst && hasLast && !otherFunc) {
    *pScanOrder = SCAN_ORDER_BOTH;
  } else if (hasLast && !scanPathOptLastNeedsAscScan(pScan)) {
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

static void scanPathOptSetScanWin(SOsdInfo* pInfo) {
  SScanLogicNode*         pScan = pInfo->pScan;
  const SWindowLogicNode* pWindow = scanPathOptGetWindowParent(pScan);
  if (NULL != pWindow) {
    if (pInfo->scanOrder == SCAN_ORDER_BOTH) {
      // for window, only keep asc order when both first and last exist
      pInfo->scanOrder = SCAN_ORDER_ASC;
    }
    pScan->interval = pWindow->interval;
    pScan->offset = pWindow->offset;
    pScan->sliding = pWindow->sliding;
    pScan->intervalUnit = pWindow->intervalUnit;
    pScan->slidingUnit = pWindow->slidingUnit;
    pScan->firstDayOfWeek = pWindow->firstDayOfWeek;
    pScan->timezone = pWindow->timezone;
    tstrncpy(pScan->timezoneName, pWindow->timezoneName, sizeof(pScan->timezoneName));
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
  if (inStreamCalcClause(pCxt->pPlanCxt)) {
    // stream calc query does not support scan path optimization
    return TSDB_CODE_SUCCESS;
  }
  SOsdInfo info = {.scanOrder = SCAN_ORDER_ASC};
  int32_t  code = TSDB_CODE_SUCCESS;

  PLAN_ERR_JRET(scanPathOptMatch(pCxt, pLogicSubplan->pNode, &info));

  if (!info.pScan) {
    goto _return;
  }

  scanPathOptSetScanWin(&info);
  scanPathOptSetScanOrder(info.scanOrder, info.pScan);
  scanPathOptSetGroupOrderScan(info.pScan);

  if (NULL != info.pDsoFuncs || NULL != info.pSdrFuncs) {
    info.pScan->dataRequired = scanPathOptGetDataRequired(info.pSdrFuncs);

    info.pScan->pDynamicScanFuncs = info.pDsoFuncs;
  }

  if (nodeType(info.pScan->node.pParent) == QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN) {
    SNode* pScan = NULL;
    FOREACH(pScan, info.pScan->node.pParent->pChildren) {
      OPTIMIZE_FLAG_SET_MASK(((SScanLogicNode*)pScan)->node.optimizedFlag, OPTIMIZE_FLAG_SCAN_PATH);
    }
  } else {
    OPTIMIZE_FLAG_SET_MASK(info.pScan->node.optimizedFlag, OPTIMIZE_FLAG_SCAN_PATH);
  }

  pCxt->optimized = true;

_return:
  if (code) {
    planError("%s failed since %s", __func__, tstrerror(code));
  }
  nodesDestroyList(info.pSdrFuncs);
  return code;
}


/*
 * Split a primary-key condition into local timerange pushdown and residual
 * conditions, and keep remote-only predicates for later scan-level rewrite.
 * @param pCxt Optimizer context for current plan rewrite.
 * @param pScanRange Output scan timerange derived from the primary-key filter.
 * @param pPrimaryKeyCond Input primary-key condition to consume or preserve.
 * @param pOtherCond Output condition list that cannot be pushed as timerange.
 * @param pRemotePrimaryCond Output remote primary condition kept for child scans.
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code.
 */
static int32_t pushDownCondOptCalcTimeRange(SOptimizeContext* pCxt, STimeWindow* pScanRange, SNode** pPrimaryKeyCond,
                                            SNode** pOtherCond, SNode** pRemotePrimaryCond) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (pCxt->pPlanCxt->topicQuery) {
    PLAN_ERR_JRET(nodesMergeNode(pOtherCond, pPrimaryKeyCond));
  } else {
    bool isStrict = false, hasRemoteNode = false;
    PLAN_ERR_JRET(filterGetTimeRange(*pPrimaryKeyCond, pScanRange, &isStrict, &hasRemoteNode));
    if (isStrict) {
      nodesDestroyNode(*pPrimaryKeyCond);
    } else if (hasRemoteNode) {
      *pRemotePrimaryCond = *pPrimaryKeyCond;
    } else {
      PLAN_ERR_JRET(nodesMergeNode(pOtherCond, pPrimaryKeyCond));
    }
    *pPrimaryKeyCond = NULL;
  }
  return code;
_return:
  planError("%s failed since %s", __func__, tstrerror(code));
  return code;
}

static int32_t pushDownCondOptRebuildTbanme(SNode** pTagCond) {
  int32_t code = TSDB_CODE_SUCCESS;
  nodesRewriteExpr(pTagCond, optRebuildTbanme, &code);
  return code;
}

static int32_t rewriteDnodeConds(SNode** pCond, SNodeList* pDnodeConds) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (nodeType(*pCond) == QUERY_NODE_LOGIC_CONDITION) {
    SLogicConditionNode* pCondNode = *(SLogicConditionNode**)pCond;
    if (pCondNode->condType == LOGIC_COND_TYPE_AND) {
      SNode* pNode = NULL;
      WHERE_EACH(pNode, pCondNode->pParameterList) {
        PLAN_ERR_JRET(rewriteDnodeConds(&cell->pNode, pDnodeConds));
        if (cell->pNode == NULL) {
          ERASE_NODE(pCondNode->pParameterList);
          continue;
        }
        WHERE_NEXT;
      }
      if (pCondNode->pParameterList->length == 1) {
        PLAN_ERR_JRET(nodesCloneNode(pCondNode->pParameterList->pHead->pNode, pCond));
        nodesDestroyList(pCondNode->pParameterList);
      } else if (pCondNode->pParameterList->length == 0) {
        nodesDestroyNode(*pCond);
        *pCond = NULL;
      }
    }
  } else if (nodeType(*pCond) == QUERY_NODE_OPERATOR) {
    SOperatorNode* pOperNode = *(SOperatorNode**)pCond;
    if (pOperNode->opType == OP_TYPE_EQUAL || pOperNode->opType == OP_TYPE_NOT_EQUAL) {
      SNode* pLeft = pOperNode->pLeft;
      SNode* pRight = pOperNode->pRight;
      if ((QUERY_NODE_COLUMN == nodeType(pLeft) && strcmp(((SColumnNode*)pLeft)->node.aliasName, "dnode_id") == 0) ||
          (QUERY_NODE_COLUMN == nodeType(pRight) && strcmp(((SColumnNode*)pRight)->node.aliasName, "dnode_id") == 0)) {
        PLAN_ERR_JRET(nodesListAppend(pDnodeConds, (SNode*)pOperNode));
        *pCond = NULL;
      }
    }
  }

_return:
  if (TSDB_CODE_SUCCESS != code) {
    planError("%s failed since %s", __func__, tstrerror(code));
  }
  return code;
}

static int32_t filterDnodeConds(SOptimizeContext* pCxt, SScanLogicNode* pScan, SNodeList** pDnodeConds) {
  if(pScan->node.pConditions == NULL) return TSDB_CODE_SUCCESS;
  if(pScan->pVgroupList == NULL) {
    return TSDB_CODE_SUCCESS;
  } 
  if(TSDB_SYSTEM_TABLE != pScan->tableType || strcmp(pScan->tableName.tname, "ins_dnode_variables") != 0) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t code = nodesMakeList(pDnodeConds);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  code = rewriteDnodeConds(&pScan->node.pConditions, *pDnodeConds);
  return code;
}

static int32_t pushDownDnodeConds(SScanLogicNode* pScan, SNodeList* pDnodeConds) {
  int32_t code = TSDB_CODE_SUCCESS;
  SVgroupsInfo* pNewVgroupList =  NULL;
  if (!pDnodeConds || pDnodeConds->length == 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t dnodeCount = pScan->pVgroupList->numOfVgroups;
  bool*   dnodeDelState = taosMemoryCalloc(dnodeCount, sizeof(bool));
  if (NULL == dnodeDelState) {
    return terrno;
  }

  SNode* pNode = NULL;
  SNode* pNewCond = NULL;
  FOREACH(pNewCond, pDnodeConds) {
    if (nodeType(pNewCond) != QUERY_NODE_OPERATOR) {
      code = TSDB_CODE_TSC_INTERNAL_ERROR;
      goto _exit;
    }
    SOperatorNode* pOperNode = (SOperatorNode*)pNewCond;
    int32_t        nodeId = -1;
    EOperatorType  operType = pOperNode->opType;
    SNode*         pLeft = pOperNode->pLeft;
    SNode*         pRight = pOperNode->pRight;
    if ((QUERY_NODE_COLUMN == nodeType(pLeft) && strcmp(((SColumnNode*)pLeft)->node.aliasName, "dnode_id") == 0)) {
      if (nodeType(pRight) == QUERY_NODE_VALUE) {
        SValueNode* pVal = (SValueNode*)pRight;
        if (IS_SIGNED_NUMERIC_TYPE(pVal->node.resType.type)) {
          nodeId = pVal->datum.i;
        }
      }
    } else if ((QUERY_NODE_COLUMN == nodeType(pRight) &&
                strcmp(((SColumnNode*)pRight)->node.aliasName, "dnode_id") == 0)) {
      if (nodeType(pLeft) == QUERY_NODE_VALUE) {
        SValueNode* pVal = (SValueNode*)pLeft;
        if (IS_SIGNED_NUMERIC_TYPE(pVal->node.resType.type)) {
          nodeId = pVal->datum.i;
        }
      }
    } else {
      code = TSDB_CODE_TSC_INTERNAL_ERROR;
      goto _exit;
    }
    if (operType == OP_TYPE_EQUAL) {
      for (int i = 0; i < dnodeCount; i++) {
        if(pScan->pVgroupList->vgroups[i].vgId != nodeId) {
          dnodeDelState[i] = true;
        }
      }
    } else {
      for (int i = 0; i < dnodeCount; i++) {
        if (pScan->pVgroupList->vgroups[i].vgId == nodeId) {
          dnodeDelState[i] = true;
          break;
        }
      }
    }
  }

  int32_t resultCount = 0;
  for (int i = 0; i < dnodeCount; i++) {
    if (!dnodeDelState[i]) {
      resultCount++;
    }
  }

  if(resultCount == dnodeCount) goto _exit;
  if (resultCount == 0) {
    taosMemoryFree(pScan->pVgroupList);
    pScan->pVgroupList = NULL;
    code = TSDB_CODE_MND_DNODE_NOT_EXIST;
    goto _exit;
  }

  pNewVgroupList = (SVgroupsInfo*)taosMemoryMalloc(sizeof(SVgroupsInfo) + sizeof(SVgroupInfo) * resultCount);
  if (NULL == pNewVgroupList) {
    code = terrno;
    goto _exit;
  }
  pNewVgroupList->numOfVgroups = 0;
  for (int num = 0; num < dnodeCount; num++) {
    SVgroupInfo* pVgInfo = &pScan->pVgroupList->vgroups[num];
    if (!dnodeDelState[num]) {
      pNewVgroupList->vgroups[pNewVgroupList->numOfVgroups] = *pVgInfo;
      pNewVgroupList->numOfVgroups++;
    }
  }
  taosMemoryFree(pScan->pVgroupList);
  pScan->pVgroupList = pNewVgroupList;

_exit:
  taosMemoryFree(dnodeDelState);
  if(TSDB_CODE_SUCCESS != code) {
    taosMemoryFree(pNewVgroupList);
  }
  return code;
}

static int32_t pdcDealScan(SOptimizeContext* pCxt, SScanLogicNode* pScan) {
  // FQ guard: external scan conditions must stay intact for fqPushdownOptimize to harvest.
  // Splitting into primaryKeyCond/tagCond/otherCond would destroy the WHERE clause.
  if (SCAN_TYPE_EXTERNAL == pScan->scanType) {
    planDebug("FQ-PDC: keep scan conditions on external scan=%p table=%s source=%s for fq collect",
              pScan, pScan->tableName.tname,
              (NULL != pScan->pExtTableNode) ? ((SExtTableNode*)pScan->pExtTableNode)->sourceName : "(null)");
    OPTIMIZE_FLAG_SET_MASK(pScan->node.optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE);
    return TSDB_CODE_SUCCESS;
  }
  if (NULL == pScan->node.pConditions ||
      OPTIMIZE_FLAG_TEST_MASK(pScan->node.optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE)
      // || TSDB_SYSTEM_TABLE == pScan->tableType
     ) {
    return TSDB_CODE_SUCCESS;
  }

  SNode*  pPrimaryKeyCond = NULL;
  SNode*  pOtherCond = NULL;
  SNodeList*  pDnodeConds = NULL;
  int32_t code = filterPartitionCond(&pScan->node.pConditions, &pPrimaryKeyCond, &pScan->pTagIndexCond,
                                     &pScan->pTagCond, &pOtherCond);
  if (TSDB_CODE_SUCCESS == code && NULL != pScan->pTagCond) {
    code = pushDownCondOptRebuildTbanme(&pScan->pTagCond);
  }
  if (TSDB_CODE_SUCCESS == code && NULL != pPrimaryKeyCond) {
    code = pushDownCondOptCalcTimeRange(pCxt, &pScan->scanRange, &pPrimaryKeyCond, &pOtherCond, &pScan->pPrimaryCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pScan->node.pConditions = pOtherCond;
  }
  code = filterDnodeConds(pCxt, pScan, &pDnodeConds);
  if (TSDB_CODE_SUCCESS == code) {
    code = pushDownDnodeConds(pScan, pDnodeConds);
  }
  if(pDnodeConds != NULL) {
    nodesDestroyList(pDnodeConds);
    pDnodeConds = NULL;
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

static bool nodeHasExternalScan(const SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    if (((const SScanLogicNode*)pNode)->scanType == SCAN_TYPE_EXTERNAL) {
      return true;
    }
  }
  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    if (nodeHasExternalScan((const SLogicNode*)pChild)) {
      return true;
    }
  }
  return false;
}

static bool pdcShouldKeepNodeConditionsForExternalSubtree(SLogicNode* pNode, const char* ownerName) {
  if (NULL == pNode || NULL == pNode->pConditions) {
    return false;
  }

  if (!nodeHasExternalScan(pNode)) {
    return false;
  }

  planDebug("FQ-PDC: keep node conditions on %s node=%p type=%d for external subtree; fq collect must judge this layer before generic pushdown",
            ownerName ? ownerName : "(unknown)", pNode, (int)nodeType(pNode));
  return true;
}
// Check if a condition involves any tag column (COLUMN_TYPE_TAG).
// Used when the virtual super table has tag refs  -  since we cannot determine
// at the super table level which specific tag columns have refs, any tag
// column condition must conservatively stay on VirtualScan.
// Pure tbname conditions use FUNCTION_TYPE_TBNAME with no tag columns.
static int32_t pdcCondHasTagColumn(SNode* pCond, bool* pHasTag) {
  *pHasTag = false;
  SNodeList* pCondCols = NULL;
  int32_t code = nodesCollectColumnsFromNode(pCond, NULL, COLLECT_COL_TYPE_ALL, &pCondCols);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  if (NULL == pCondCols) {
    return TSDB_CODE_SUCCESS;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pCondCols) {
    if (QUERY_NODE_COLUMN != nodeType(pNode)) continue;
    SColumnNode* pCol = (SColumnNode*)pNode;
    if (COLUMN_TYPE_TAG == pCol->colType) {
      *pHasTag = true;
      break;
    }
  }

  nodesDestroyList(pCondCols);
  return TSDB_CODE_SUCCESS;
}

// Split a tag condition into SystemScan-safe part and VirtualScan-only part.
// When hasTagRef is true, we cannot determine at the super table level which
// specific tags have refs, so any condition with COLUMN_TYPE_TAG columns must
// conservatively stay on VirtualScan. Only pure tbname conditions (which use
// FUNCTION_TYPE_TBNAME, no tag columns) are safe for SystemScan.
// When hasTagRef is false, all tag conditions can be safely pushed to SystemScan
// because all tag values exist in vnode meta (no ref-tag resolution needed).
static int32_t pdcSplitTagCondForVstb(SNode** ppTagCond, SNode** ppSysTagCond, SNode** ppRefTagCond, bool hasTagRef) {
  if (NULL == ppTagCond || NULL == *ppTagCond) {
    return TSDB_CODE_SUCCESS;
  }

  // No tag refs at all  -  all tag conditions can go to SystemScan
  if (!hasTagRef) {
    *ppSysTagCond = *ppTagCond;
    *ppTagCond = NULL;
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pCond = *ppTagCond;

  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pCond) &&
      LOGIC_COND_TYPE_AND == ((SLogicConditionNode*)pCond)->condType) {
    // AND-ed conditions: split each child
    SNode* pChild = NULL;
    FOREACH(pChild, ((SLogicConditionNode*)pCond)->pParameterList) {
      SNode* pClone = NULL;
      code = nodesCloneNode(pChild, &pClone);
      if (TSDB_CODE_SUCCESS != code) return code;

      bool hasTag = false;
      code = pdcCondHasTagColumn(pChild, &hasTag);
      if (TSDB_CODE_SUCCESS != code) {
        nodesDestroyNode(pClone);
        return code;
      }

      if (hasTag) {
        code = nodesMergeNode(ppRefTagCond, &pClone);
      } else {
        code = nodesMergeNode(ppSysTagCond, &pClone);
      }
      if (TSDB_CODE_SUCCESS != code) {
        nodesDestroyNode(pClone);
        return code;
      }
    }
    nodesDestroyNode(pCond);
    *ppTagCond = NULL;
  } else {
    // Single condition
    bool hasTag = false;
    code = pdcCondHasTagColumn(pCond, &hasTag);
    if (TSDB_CODE_SUCCESS != code) return code;
    if (hasTag) {
      *ppRefTagCond = pCond;
    } else {
      *ppSysTagCond = pCond;
    }
    *ppTagCond = NULL;
  }
  return code;
}

static int32_t pdcDealVirtualSuperTableScan(SOptimizeContext* pCxt, SDynQueryCtrlLogicNode* pScan) {
  if (NULL == pScan->node.pConditions ||
      OPTIMIZE_FLAG_TEST_MASK(pScan->node.optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE)) {
    return TSDB_CODE_SUCCESS;
  }

  SNode*  pTagIndexCond = NULL;
  SNode*  pTagCond = NULL;
  SNode*  pPrimaryKeyCond = NULL;
  SNode*  pOtherCond = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  bool    hasExternalScan = false;

  // For virtual super tables, ref-tag values store NULL in vnode meta,
  // so ref-tag predicates cannot be pushed to SystemScan's tag filter.
  // However, tbname and local-tag predicates CAN be pushed to SystemScan
  // (they have real values). Use hasTagRef from VirtualScan to decide:
  // if no tag refs exist, all tag conditions are safe for SystemScan.
  SNode* pSysTagCond = NULL;
  SNode* pRefTagCond = NULL;
  SNode* pSysTagIdxCond = NULL;
  SNode* pRefTagIdxCond = NULL;

  if (inStreamCalcClause(pCxt->pPlanCxt)) {
    SVirtualScanLogicNode *pVscan = (SVirtualScanLogicNode*)nodesListGetNode(pScan->node.pChildren, 0);
    if (NULL == pVscan || QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN != nodeType(pVscan)) {
      PLAN_ERR_JRET(generateUsageErrMsg(pCxt->pPlanCxt->pMsg, pCxt->pPlanCxt->msgLen, TSDB_CODE_PLAN_INTERNAL_ERROR,
                                        "pdcDealVirtualSuperTableScan get invalid vtable scan logic node from dyn query ctrl node"));
    }
    TSWAP(pVscan->node.pConditions, pScan->node.pConditions);
    return code;
  }

  PLAN_ERR_JRET(pushDownCondOptRebuildTbanme(&pScan->node.pConditions));
  PLAN_ERR_JRET(filterPartitionCond(&pScan->node.pConditions, &pPrimaryKeyCond, &pTagIndexCond, &pTagCond, &pOtherCond));
  if (NULL != pTagCond) {
    PLAN_ERR_JRET(pushDownCondOptRebuildTbanme(&pTagCond));
  }
  if (NULL != pTagIndexCond) {
    PLAN_ERR_JRET(pushDownCondOptRebuildTbanme(&pTagIndexCond));
  }

  SVirtualScanLogicNode *pVscan = (SVirtualScanLogicNode*)nodesListGetNode(pScan->node.pChildren, 0);
  if (NULL == pVscan || QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN != nodeType(pVscan)) {
    PLAN_ERR_JRET(generateUsageErrMsg(pCxt->pPlanCxt->pMsg, pCxt->pPlanCxt->msgLen, TSDB_CODE_PLAN_INTERNAL_ERROR,
                                      "pdcDealVirtualSuperTableScan get invalid vtable scan logic node from dyn query ctrl node"));
  }
  // Find the SUPER_TABLE scan among pVscan's children (may not be last when external scans are present).
  // Must also exclude Tag Scan nodes which share the same tableType but use SCAN_TYPE_TAG.
  SScanLogicNode* pOrgScan = NULL;
  SNode*          pChild = NULL;
  FOREACH(pChild, pVscan->node.pChildren) {
    if (QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pChild)) {
      continue;
    }
    SScanLogicNode* pChildScan = (SScanLogicNode*)pChild;
    if (SCAN_TYPE_EXTERNAL == pChildScan->scanType) {
      hasExternalScan = true;
    } else if (pChildScan->tableType == TSDB_SUPER_TABLE && pChildScan->scanType != SCAN_TYPE_TAG) {
      pOrgScan = (SScanLogicNode*)pChild;
    }
  }
  if (NULL == pOrgScan) {
    PLAN_ERR_JRET(generateUsageErrMsg(pCxt->pPlanCxt->pMsg, pCxt->pPlanCxt->msgLen, TSDB_CODE_PLAN_INTERNAL_ERROR,
                                      "pdcDealVirtualSuperTableScan get invalid org scan logic node from vtable scan node"));
  }
  SScanLogicNode* pSysScan = (SScanLogicNode*)nodesListGetNode(pScan->node.pChildren, 1);
  if (NULL == pSysScan || QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pSysScan) || pSysScan->tableType != TSDB_SYSTEM_TABLE) {
    PLAN_ERR_JRET(generateUsageErrMsg(pCxt->pPlanCxt->pMsg, pCxt->pPlanCxt->msgLen, TSDB_CODE_PLAN_INTERNAL_ERROR,
                                      "pdcDealVirtualSuperTableScan get invalid sys scan logic node from vtable scan node"));
  }

  if (NULL != pPrimaryKeyCond) {
    PLAN_ERR_JRET(pushDownCondOptCalcTimeRange(pCxt, &pOrgScan->scanRange, &pPrimaryKeyCond, &pOtherCond, &pOrgScan->pPrimaryCond));
    // Propagate the resolved time range to external scan children so that
    // federated queries include a WHERE ts clause on the remote source.
    SNode* pExtChild = NULL;
    FOREACH(pExtChild, pVscan->node.pChildren) {
      if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pExtChild) &&
          SCAN_TYPE_EXTERNAL == ((SScanLogicNode*)pExtChild)->scanType) {
        ((SScanLogicNode*)pExtChild)->scanRange = pOrgScan->scanRange;
      }
    }
    if (hasExternalScan && NULL != pOrgScan->pPrimaryCond) {
      PLAN_ERR_JRET(nodesMergeNode(&pOtherCond, &pOrgScan->pPrimaryCond));
    }
  }


  PLAN_ERR_JRET(pdcSplitTagCondForVstb(&pTagCond, &pSysTagCond, &pRefTagCond, pVscan->hasTagRef));
  PLAN_ERR_JRET(pdcSplitTagCondForVstb(&pTagIndexCond, &pSysTagIdxCond, &pRefTagIdxCond, pVscan->hasTagRef));

  // Ref-tag conditions (or conditions we can't verify) stay on VirtualScan
  if (pRefTagCond) {
    PLAN_ERR_JRET(nodesMergeNode(&pOtherCond, &pRefTagCond));
  }
  if (pRefTagIdxCond) {
    PLAN_ERR_JRET(nodesMergeNode(&pOtherCond, &pRefTagIdxCond));
  }

  // tbname conditions go to SystemScan (original behavior)
  pSysScan->pTagCond = pSysTagCond;
  pSysTagCond = NULL;
  pSysScan->pTagIndexCond = pSysTagIdxCond;
  pSysTagIdxCond = NULL;

  pVscan->node.pConditions = pOtherCond;
  pOtherCond = NULL;

  OPTIMIZE_FLAG_SET_MASK(pScan->node.optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE);
  pCxt->optimized = true;

  return code;
_return:
  nodesDestroyNode(pPrimaryKeyCond);
  nodesDestroyNode(pOtherCond);
  nodesDestroyNode(pTagIndexCond);
  nodesDestroyNode(pTagCond);
  nodesDestroyNode(pSysTagCond);
  nodesDestroyNode(pRefTagCond);
  nodesDestroyNode(pSysTagIdxCond);
  nodesDestroyNode(pRefTagIdxCond);
  return code;
}

static bool pdcColBelongThisTable(SNode* pCondCol, SNodeList* pTableCols) {
  SNode* pTableCol = NULL;
  FOREACH(pTableCol, pTableCols) {
    if (QUERY_NODE_COLUMN == nodeType(pCondCol) && QUERY_NODE_COLUMN == nodeType(pTableCol)) {
      SColumnNode* pCondColNode = (SColumnNode*)pCondCol;
      SColumnNode* pTblColNode = (SColumnNode*)pTableCol;
      if (0 == strcmp(pCondColNode->tableAlias, pTblColNode->tableAlias) &&
          0 == strcmp(pCondColNode->colName, pTblColNode->colName)) {
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
  char* pTableAlias = NULL;
  if (QUERY_NODE_COLUMN == nodeType(pCondCol)) {
    SColumnNode* pTableCol = (SColumnNode*)pCondCol;
    pTableAlias = pTableCol->tableAlias;
  } else if (QUERY_NODE_VALUE == nodeType(pCondCol)) {
    SValueNode* pVal = (SValueNode*)pCondCol;
    pTableAlias = pVal->node.srcTable;
  }
  
  if (NULL == tSimpleHashGet(pTables, pTableAlias, strlen(pTableAlias))) {
    return false;
  }
  return true;
}

static EDealRes pdcJoinIsCrossTableCond(SNode* pNode, void* pContext) {
  SCpdIsMultiTableCondCxt* pCxt = pContext;
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    if (pdcJoinColInTableList(pNode, pCxt->pLeftTbls)) {
      pCxt->havaLeftCol = true;
    } 

    if (pdcJoinColInTableList(pNode, pCxt->pRightTbls)) {
      pCxt->haveRightCol = true;
    }
    return pCxt->havaLeftCol && pCxt->haveRightCol ? DEAL_RES_END : DEAL_RES_CONTINUE;
  }
  return DEAL_RES_CONTINUE;
}

static bool pdcJoinCopyTargetIsLocalTableScan(SJoinLogicNode* pJoin, bool leftChild) {
  SLogicNode* pChild = (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, leftChild ? 0 : 1);
  return NULL != pChild && QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pChild) &&
         SCAN_TYPE_TABLE == ((SScanLogicNode*)pChild)->scanType;
}

static ECondAction pdcJoinGetCondAction(SJoinLogicNode* pJoin, SSHashObj* pLeftTbls, SSHashObj* pRightTbls,
                                        SNode* pNode, bool whereCond) {
  EJoinType               t = pJoin->joinType;
  EJoinSubType            s = pJoin->subType;
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
    if ((whereCond && gJoinWhereOpt[t][s].pushDownFlag & PUSH_DOWN_LEFT_FLT) ||
        (!whereCond && gJoinOnOpt[t][s].pushDownFlag & PUSH_DOWN_LEFT_FLT)) {
      return COND_ACTION_PUSH_LEFT_CHILD;
    }
    if (whereCond && gJoinWhereOpt[t][s].pushDownFlag & PUSH_DOWN_LEFT_FLT_COPY &&
        pdcJoinCopyTargetIsLocalTableScan(pJoin, true) && joinCondHasValidPrimKeyCond(pJoin) &&
        pdcJoinIsSafeAsofCopyCond(pJoin, pNode, pLeftTbls)) {
      return COND_ACTION_COPY_LEFT_CHILD;
    }
    return COND_ACTION_STAY;
  }

  if (cxt.haveRightCol) {
    if ((whereCond && gJoinWhereOpt[t][s].pushDownFlag & PUSH_DOWN_RIGHT_FLT) ||
        (!whereCond && gJoinOnOpt[t][s].pushDownFlag & PUSH_DOWN_RIGHT_FLT)) {
      return COND_ACTION_PUSH_RIGHT_CHILD;
    }
    if (whereCond && gJoinWhereOpt[t][s].pushDownFlag & PUSH_DOWN_RIGHT_FLT_COPY &&
        pdcJoinCopyTargetIsLocalTableScan(pJoin, false) && joinCondHasValidPrimKeyCond(pJoin) &&
        pdcJoinIsSafeAsofCopyCond(pJoin, pNode, pRightTbls)) {
      return COND_ACTION_COPY_RIGHT_CHILD;
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
    SNode*      pNew = NULL;
    code = nodesCloneNode(pCond, &pNew);
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
    if (COND_ACTION_PUSH_JOIN == condAction && NULL != pOnCond) {
      code = nodesListMakeAppend(&pOnConds, pNew);
    } else if (COND_ACTION_PUSH_LEFT_CHILD == condAction) {
      code = nodesListMakeAppend(&pLeftChildConds, pNew);
    } else if (COND_ACTION_PUSH_RIGHT_CHILD == condAction) {
      code = nodesListMakeAppend(&pRightChildConds, pNew);
    } else if (COND_ACTION_COPY_LEFT_CHILD == condAction || COND_ACTION_COPY_RIGHT_CHILD == condAction) {
      SNode* pRemain = NULL;
      code = nodesCloneNode(pCond, &pRemain);
      if (TSDB_CODE_SUCCESS != code) {
        nodesDestroyNode(pNew);
        break;
      }
      SNodeList** ppChildConds =
          (COND_ACTION_COPY_LEFT_CHILD == condAction) ? &pLeftChildConds : &pRightChildConds;
      code = nodesListMakeAppend(&pRemainConds, pRemain);
      if (TSDB_CODE_SUCCESS == code) {
        code = nodesListMakeAppend(ppChildConds, pNew);
        if (TSDB_CODE_SUCCESS != code) {
          nodesDestroyNode(pNew);
        }
      } else {
        nodesDestroyNode(pRemain);
        nodesDestroyNode(pNew);
      }
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
  int32_t    code = collectTableAliasFromNodes(nodesListGetNode(pJoin->node.pChildren, 0), &pLeftTables);
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
  } else if (COND_ACTION_COPY_LEFT_CHILD == condAction) {
    return nodesCloneNode(*pSrcCond, pLeftChildCond);
  } else if (COND_ACTION_COPY_RIGHT_CHILD == condAction) {
    return nodesCloneNode(*pSrcCond, pRightChildCond);
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
  return nodesMergeNode(&pJoin->pFullOnCond, pCond);
}

static bool vscanHasCondColumn(SNodeList* pList, const SColumnNode* pCol) {
  if (pList == NULL || pCol == NULL) {
    return false;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    if (nodeType(pNode) != QUERY_NODE_COLUMN) {
      continue;
    }

    SColumnNode* pExist = (SColumnNode*)pNode;
    if (pExist->colId == pCol->colId && pExist->colType == pCol->colType && pExist->hasRef == pCol->hasRef &&
        0 == strcmp(pExist->tableAlias, pCol->tableAlias) && 0 == strcmp(pExist->refDbName, pCol->refDbName) &&
        0 == strcmp(pExist->refTableName, pCol->refTableName) && 0 == strcmp(pExist->refColName, pCol->refColName)) {
      return true;
    }
  }

  return false;
}

static int32_t vscanAppendCondColumn(SNodeList** ppList, const SColumnNode* pCol) {
  if (ppList == NULL || pCol == NULL || vscanHasCondColumn(*ppList, pCol)) {
    return TSDB_CODE_SUCCESS;
  }

  SNode* pClone = NULL;
  PLAN_ERR_RET(nodesCloneNode((SNode*)pCol, &pClone));
  return nodesListMakeStrictAppend(ppList, pClone);
}

static int32_t cloneVgroups(SVgroupsInfo** pDst, SVgroupsInfo* pSrc) {
  if (pSrc == NULL) {
    *pDst = NULL;
    return TSDB_CODE_SUCCESS;
  }

  int32_t len = VGROUPS_INFO_SIZE(pSrc);
  *pDst = taosMemoryMalloc(len);
  if (NULL == *pDst) {
    return terrno;
  }

  memcpy(*pDst, pSrc, len);
  return TSDB_CODE_SUCCESS;
}

static SScanLogicNode* findVirtualTagScanChild(SVirtualScanLogicNode* pScan) {
  if (pScan == NULL || pScan->node.pChildren == NULL) {
    return NULL;
  }

  SNode* pChild = NULL;
  FOREACH(pChild, pScan->node.pChildren) {
    if (nodeType(pChild) == QUERY_NODE_LOGIC_PLAN_SCAN && ((SScanLogicNode*)pChild)->scanType == SCAN_TYPE_TAG) {
      return (SScanLogicNode*)pChild;
    }
  }

  return NULL;
}

static int32_t ensureVirtualTagScanChild(SVirtualScanLogicNode* pScan) {
  if (pScan == NULL || pScan->pScanPseudoCols == NULL || LIST_LENGTH(pScan->pScanPseudoCols) <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  if (findVirtualTagScanChild(pScan) != NULL) {
    return TSDB_CODE_SUCCESS;
  }

  SScanLogicNode* pTagScan = NULL;
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;

  PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SCAN, (SNode**)&pTagScan));
  PLAN_ERR_JRET(cloneVgroups(&pTagScan->pVgroupList, pScan->pVgroupList));

  pTagScan->tableId = pScan->tableId;
  pTagScan->stableId = pScan->stableId;
  pTagScan->tableType = pScan->tableType;
  pTagScan->scanSeq[0] = 1;
  pTagScan->scanSeq[1] = 0;
  TAOS_SET_OBJ_ALIGNED(&pTagScan->scanRange, TSWINDOW_INITIALIZER);
  pTagScan->tableName = pScan->tableName;
  pTagScan->dataRequired = FUNC_DATA_REQUIRED_DATA_LOAD;
  pTagScan->node.groupAction = GROUP_ACTION_NONE;
  pTagScan->node.resultDataOrder = DATA_ORDER_LEVEL_GLOBAL;
  pTagScan->scanType = SCAN_TYPE_TAG;
  pTagScan->onlyMetaCtbIdx = false;

  PLAN_ERR_JRET(nodesCloneList(pScan->pScanPseudoCols, &pTagScan->pScanPseudoCols));
  PLAN_ERR_JRET(createColumnByRewriteExprs(pTagScan->pScanPseudoCols, &pTagScan->node.pTargets));
  SScanLogicNode* pTemp = pTagScan;
  pTagScan = NULL;  // nodesListMakeStrictAppend takes ownership and destroys on failure
  PLAN_ERR_JRET(nodesListMakeStrictAppend(&pScan->node.pChildren, (SNode*)pTemp));
  pTemp->node.pParent = (SLogicNode*)pScan;

  return code;

_return:
  planError("%s failed at line %d, code: %d", __func__, lino, code);
  nodesDestroyNode((SNode*)pTagScan);
  return code;
}

static int32_t pdcSyncVirtualScanCondCols(SVirtualScanLogicNode* pScan, SNode* pCond) {
  if (pScan == NULL || pCond == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  SNodeList* pCondCols = NULL;
  SScanLogicNode* pTagScan = findVirtualTagScanChild(pScan);
  int32_t    code = nodesCollectColumnsFromNode(pCond, NULL, COLLECT_COL_TYPE_ALL, &pCondCols);
  if (TSDB_CODE_SUCCESS != code || pCondCols == NULL) {
    nodesDestroyList(pCondCols);
    return code;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pCondCols) {
    if (nodeType(pNode) != QUERY_NODE_COLUMN) {
      continue;
    }

    SColumnNode* pCol = (SColumnNode*)pNode;
    if (pCol->colType == COLUMN_TYPE_TAG) {
      if (pCol->hasRef) {
        code = vscanAppendCondColumn(&pScan->pRefTagCols, pCol);
      } else {
        code = vscanAppendCondColumn(&pScan->pScanPseudoCols, pCol);
        if (TSDB_CODE_SUCCESS == code) {
          code = vscanAppendCondColumn(&pScan->pLocalTags, pCol);
        }
        if (TSDB_CODE_SUCCESS == code && pTagScan != NULL) {
          code = vscanAppendCondColumn(&pTagScan->pScanPseudoCols, pCol);
        }
        if (TSDB_CODE_SUCCESS == code && pTagScan != NULL) {
          code = vscanAppendCondColumn(&pTagScan->node.pTargets, pCol);
        }
      }
    } else {
      code = vscanAppendCondColumn(&pScan->pScanCols, pCol);
    }

    if (TSDB_CODE_SUCCESS == code) {
      code = vscanAppendCondColumn(&pScan->node.pTargets, pCol);
    }

    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }

  if (TSDB_CODE_SUCCESS == code &&
      pScan->pScanPseudoCols != NULL &&
      LIST_LENGTH(pScan->pScanPseudoCols) > 0 &&
      findVirtualTagScanChild(pScan) == NULL) {
    code = ensureVirtualTagScanChild(pScan);
  }

  if (TSDB_CODE_SUCCESS == code) {
    pScan->hasLocalTag = (pScan->pLocalTags != NULL && LIST_LENGTH(pScan->pLocalTags) > 0);
  }

  nodesDestroyList(pCondCols);
  return code;
}

static int32_t pdcPushDownCondToChild(SOptimizeContext* pCxt, SLogicNode* pChild, SNode** pCond) {
  int32_t code = nodesMergeNode(&pChild->pConditions, pCond);
  if (TSDB_CODE_SUCCESS == code && pChild != NULL &&
      nodeType((SNode*)pChild) == QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN) {
    code = pdcSyncVirtualScanCondCols((SVirtualScanLogicNode*)pChild, pChild->pConditions);
  }
  return code;
}

static bool pdcJoinIsPrim(SNode* pNode, SSHashObj* pTables, bool constAsPrim, bool* constPrimGot) {
  if (QUERY_NODE_COLUMN != nodeType(pNode) && QUERY_NODE_FUNCTION != nodeType(pNode) &&
      (!constAsPrim || QUERY_NODE_VALUE != nodeType(pNode))) {
    return false;
  }

  if (QUERY_NODE_VALUE == nodeType(pNode)) {
    SValueNode* pVal = (SValueNode*)pNode;
    if (TSDB_DATA_TYPE_NULL != pVal->node.resType.type && !pVal->isNull) {
      if (pdcJoinColInTableList(pNode, pTables)) {
        *constPrimGot = true;
        return true;
      } else if (pVal->node.srcTable[0] == '\0') {
        *constPrimGot = true;
        return true;
      }
    }

    return false;
  }

  if (QUERY_NODE_FUNCTION == nodeType(pNode)) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    if (FUNCTION_TYPE_TIMETRUNCATE != pFunc->funcType &&
        FUNCTION_TYPE_TIMESTAMP_SCALE != pFunc->funcType) {
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
  // External tables (e.g. MySQL without a timestamp column) assign colId=1 to the first
  // column sequentially, even when it is not a timestamp.  Require TIMESTAMP type so that
  // non-timestamp columns with colId==PRIMARYKEY_TIMESTAMP_COL_ID are not mistaken for
  // primary key columns, which would bypass the "primary timestamp equal condition" check
  // and cause the Merge Join executor to misread INT columns as int64 timestamps.
  if (TSDB_DATA_TYPE_TIMESTAMP != pCol->node.resType.type) {
    return false;
  }
  return pdcJoinColInTableList(pNode, pTables);
}

static EOperatorType asofJoinGetReverseOp(EOperatorType opType) {
  switch (opType) {
    case OP_TYPE_GREATER_THAN:
      return OP_TYPE_LOWER_THAN;
    case OP_TYPE_GREATER_EQUAL:
      return OP_TYPE_LOWER_EQUAL;
    case OP_TYPE_LOWER_THAN:
      return OP_TYPE_GREATER_THAN;
    case OP_TYPE_LOWER_EQUAL:
      return OP_TYPE_GREATER_EQUAL;
    case OP_TYPE_EQUAL:
      return OP_TYPE_EQUAL;
    default:
      break;
  }

  return opType;
}

static bool asofJoinGetMatchedCmpOp(SJoinLogicNode* pJoin, EOperatorType* pMatchedOpType) {
  if (NULL == pMatchedOpType || NULL == pJoin->pPrimKeyEqCond ||
      QUERY_NODE_OPERATOR != nodeType(pJoin->pPrimKeyEqCond)) {
    return false;
  }

  SOperatorNode* pOp = (SOperatorNode*)pJoin->pPrimKeyEqCond;
  const char*    pLeftTable = getJoinCondPKTable(pOp->pLeft);
  const char*    pRightTable = getJoinCondPKTable(pOp->pRight);
  if (NULL == pLeftTable || NULL == pRightTable) {
    return false;
  }

  SSHashObj* pLeftTables = NULL;
  SSHashObj* pRightTables = NULL;
  int32_t    code = collectTableAliasFromNodes(nodesListGetNode(pJoin->node.pChildren, 0), &pLeftTables);
  if (TSDB_CODE_SUCCESS != code) {
    return false;
  }
  code = collectTableAliasFromNodes(nodesListGetNode(pJoin->node.pChildren, 1), &pRightTables);
  if (TSDB_CODE_SUCCESS != code) {
    tSimpleHashCleanup(pLeftTables);
    return false;
  }

  bool leftExprOnLeft = NULL != tSimpleHashGet(pLeftTables, pLeftTable, strlen(pLeftTable));
  bool leftExprOnRight = NULL != tSimpleHashGet(pRightTables, pLeftTable, strlen(pLeftTable));
  bool rightExprOnLeft = NULL != tSimpleHashGet(pLeftTables, pRightTable, strlen(pRightTable));
  bool rightExprOnRight = NULL != tSimpleHashGet(pRightTables, pRightTable, strlen(pRightTable));

  tSimpleHashCleanup(pLeftTables);
  tSimpleHashCleanup(pRightTables);

  bool matchedOnLeftExpr = false;
  bool probeOnRightExpr = false;
  bool probeOnLeftExpr = false;
  bool matchedOnRightExpr = false;
  if (JOIN_TYPE_LEFT == pJoin->joinType) {
    matchedOnLeftExpr = leftExprOnRight;
    probeOnRightExpr = rightExprOnLeft;
    probeOnLeftExpr = leftExprOnLeft;
    matchedOnRightExpr = rightExprOnRight;
  } else if (JOIN_TYPE_RIGHT == pJoin->joinType) {
    matchedOnLeftExpr = leftExprOnLeft;
    probeOnRightExpr = rightExprOnRight;
    probeOnLeftExpr = leftExprOnRight;
    matchedOnRightExpr = rightExprOnLeft;
  } else {
    return false;
  }

  if (matchedOnLeftExpr && probeOnRightExpr) {
    *pMatchedOpType = pOp->opType;
    return true;
  }
  if (probeOnLeftExpr && matchedOnRightExpr) {
    *pMatchedOpType = asofJoinGetReverseOp(pOp->opType);
    return true;
  }

  return false;
}

static bool asofJoinCanDeriveMatchedUpperBound(SJoinLogicNode* pJoin) {
  EOperatorType matchedOpType = OP_TYPE_EQUAL;
  if (!asofJoinGetMatchedCmpOp(pJoin, &matchedOpType)) {
    return false;
  }

  return OP_TYPE_LOWER_THAN == matchedOpType || OP_TYPE_LOWER_EQUAL == matchedOpType || OP_TYPE_EQUAL == matchedOpType;
}

static bool asofJoinCanDeriveMatchedLowerBound(SJoinLogicNode* pJoin) {
  EOperatorType matchedOpType = OP_TYPE_EQUAL;
  if (!asofJoinGetMatchedCmpOp(pJoin, &matchedOpType)) {
    return false;
  }

  return OP_TYPE_GREATER_THAN == matchedOpType || OP_TYPE_GREATER_EQUAL == matchedOpType ||
         OP_TYPE_EQUAL == matchedOpType;
}

static bool pdcJoinIsSafeAsofCopyCond(SJoinLogicNode* pJoin, SNode* pCond, SSHashObj* pTables) {
  if (QUERY_NODE_OPERATOR != nodeType(pCond)) {
    return false;
  }

  SOperatorNode* pOper = (SOperatorNode*)pCond;
  if (OP_TYPE_GREATER_THAN != pOper->opType && OP_TYPE_GREATER_EQUAL != pOper->opType &&
      OP_TYPE_LOWER_THAN != pOper->opType && OP_TYPE_LOWER_EQUAL != pOper->opType) {
    return false;
  }

  bool leftDummy = false;
  bool rightDummy = false;
  bool primOnLeft = pdcJoinIsPrim(pOper->pLeft, pTables, false, &leftDummy);
  bool primOnRight = pdcJoinIsPrim(pOper->pRight, pTables, false, &rightDummy);
  if (primOnLeft == primOnRight) {
    return false;
  }

  if (primOnLeft) {
    bool rightConst = false;
    if (!pdcJoinIsPrim(pOper->pRight, pTables, true, &rightConst) || !rightConst) {
      return false;
    }
    if (OP_TYPE_GREATER_THAN == pOper->opType || OP_TYPE_GREATER_EQUAL == pOper->opType) {
      return asofJoinCanDeriveMatchedUpperBound(pJoin);
    }
    return asofJoinCanDeriveMatchedLowerBound(pJoin);
  }

  bool leftConst = false;
  if (!pdcJoinIsPrim(pOper->pLeft, pTables, true, &leftConst) || !leftConst) {
    return false;
  }
  if (OP_TYPE_LOWER_THAN == pOper->opType || OP_TYPE_LOWER_EQUAL == pOper->opType) {
    return asofJoinCanDeriveMatchedUpperBound(pJoin);
  }
  return asofJoinCanDeriveMatchedLowerBound(pJoin);
}
static bool pdcJoinIsPrimEqualCond(SJoinLogicNode* pJoin, SNode* pCond, bool constAsPrim) {
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
  int32_t    code = collectTableAliasFromNodes(nodesListGetNode(pJoin->node.pChildren, 0), &pLeftTables);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  code = collectTableAliasFromNodes(nodesListGetNode(pJoin->node.pChildren, 1), &pRightTables);
  if (TSDB_CODE_SUCCESS != code) {
    tSimpleHashCleanup(pLeftTables);
    return code;
  }

  bool res = false, constGot = false;
  if (pdcJoinIsPrim(pOper->pLeft, pLeftTables, constAsPrim, &pJoin->leftConstPrimGot)) {
    res = pdcJoinIsPrim(pOper->pRight, pRightTables, constAsPrim, &pJoin->rightConstPrimGot);
  } else if (pdcJoinIsPrim(pOper->pLeft, pRightTables, constAsPrim, &pJoin->rightConstPrimGot)) {
    res = pdcJoinIsPrim(pOper->pRight, pLeftTables, constAsPrim, &pJoin->leftConstPrimGot);
    if (pJoin->rightConstPrimGot || pJoin->leftConstPrimGot) {
      TSWAP(pOper->pLeft, pOper->pRight);
    }
  }

  if (constAsPrim && ((pJoin->leftNoOrderedSubQuery && !pJoin->leftConstPrimGot) || (pJoin->rightNoOrderedSubQuery && !pJoin->rightConstPrimGot))) {
    res = false;
  }

  tSimpleHashCleanup(pLeftTables);
  tSimpleHashCleanup(pRightTables);

  return res;
}

static bool pdcJoinPrimEqualCondHasFunc(SNode* pCond) {
  if (QUERY_NODE_OPERATOR != nodeType(pCond)) {
    return false;
  }

  SOperatorNode* pOper = (SOperatorNode*)pCond;
  SNode*         pNodes[2] = {pOper->pLeft, pOper->pRight};
  for (int32_t i = 0; i < 2; ++i) {
    if (NULL == pNodes[i] || QUERY_NODE_FUNCTION != nodeType(pNodes[i])) {
      continue;
    }

    SFunctionNode* pFunc = (SFunctionNode*)pNodes[i];
    if (FUNCTION_TYPE_TIMETRUNCATE == pFunc->funcType || FUNCTION_TYPE_TIMESTAMP_SCALE == pFunc->funcType) {
      return true;
    }
  }

  return false;
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
    SNode* pCondition = NULL;
    FOREACH(pCondition, pLogicCond->pParameterList) {
      if (pdcJoinHasPrimEqualCond(pJoin, pCondition, NULL)) {
        hasPrimaryKeyEqualCond = true;
        break;
      }
    }
    return hasPrimaryKeyEqualCond;
  } else {
    return pdcJoinIsPrimEqualCond(pJoin, pCond, false);
  }
}

static int32_t pdcJoinSplitPrimInLogicCond(SJoinLogicNode* pJoin, SNode** ppInput, SNode** ppPrimEqCond, SNode** ppOnCond, bool constAsPrim) {
  SLogicConditionNode* pLogicCond = (SLogicConditionNode*)(*ppInput);

  int32_t    code = TSDB_CODE_SUCCESS;
  SNodeList* pOnConds = NULL;
  SNode*     pCond = NULL;

  bool preferFuncPrim = false;
  if (!constAsPrim) {
    FOREACH(pCond, pLogicCond->pParameterList) {
      if (pdcJoinIsPrimEqualCond(pJoin, pCond, constAsPrim) && pdcJoinPrimEqualCondHasFunc(pCond)) {
        preferFuncPrim = true;
        break;
      }
    }
  }

  WHERE_EACH(pCond, pLogicCond->pParameterList) {
    SNode* pNew = NULL;
    if (pdcJoinIsPrimEqualCond(pJoin, pCond, constAsPrim) && (NULL == *ppPrimEqCond) &&
        (!constAsPrim || pJoin->leftConstPrimGot || pJoin->rightConstPrimGot) &&
        (!preferFuncPrim || pdcJoinPrimEqualCondHasFunc(pCond))) {
      code = nodesCloneNode(pCond, &pNew);
      if (TSDB_CODE_SUCCESS != code) break;
      *ppPrimEqCond = pNew;
      ERASE_NODE(pLogicCond->pParameterList);
    } else {
      code = nodesCloneNode(pCond, &pNew);
      if (TSDB_CODE_SUCCESS != code) break;
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
    nodesDestroyNode(*ppInput);
    *ppInput = NULL;

    if (NULL != *ppPrimEqCond) {
      *ppOnCond = pTempOnCond;
      return TSDB_CODE_SUCCESS;
    }
    
    nodesDestroyNode(pTempOnCond);
    if (!constAsPrim) {
      planError("no primary key equal cond found, condListNum:%d", pLogicCond->pParameterList->length);
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }
  } else {
    nodesDestroyList(pOnConds);
    nodesDestroyNode(pTempOnCond);
    return code;
  }

  return code;
}

static int32_t pdcJoinSplitPrimEqCond(SOptimizeContext* pCxt, SJoinLogicNode* pJoin) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pPrimKeyEqCond = NULL;
  SNode*  pJoinOnCond = NULL;

  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pJoin->pFullOnCond) &&
      LOGIC_COND_TYPE_AND == ((SLogicConditionNode*)(pJoin->pFullOnCond))->condType) {
    code = pdcJoinSplitPrimInLogicCond(pJoin, &pJoin->pFullOnCond, &pPrimKeyEqCond, &pJoinOnCond, false);
  } else if (pdcJoinIsPrimEqualCond(pJoin, pJoin->pFullOnCond, false)) {
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

  // Helper lambda-like checks: accept COLUMN, JSON_GET_VALUE operator, or timestamp-scale/timetruncate function
  bool leftOk = false, rightOk = false;
  if (QUERY_NODE_COLUMN == nodeType(pOper->pLeft)) {
    leftOk = true;
  } else if (QUERY_NODE_OPERATOR == nodeType(pOper->pLeft) &&
             OP_TYPE_JSON_GET_VALUE == ((SOperatorNode*)pOper->pLeft)->opType) {
    leftOk = true;
  } else if (QUERY_NODE_FUNCTION == nodeType(pOper->pLeft)) {
    SFunctionNode* pFunc = (SFunctionNode*)pOper->pLeft;
    if (FUNCTION_TYPE_TIMESTAMP_SCALE == pFunc->funcType || FUNCTION_TYPE_TIMETRUNCATE == pFunc->funcType) {
      SListCell* pCell = nodesListGetCell(pFunc->pParameterList, 0);
      if (pCell && pCell->pNode && QUERY_NODE_COLUMN == nodeType(pCell->pNode)) {
        leftOk = true;
      }
    }
  }
  if (NULL == pOper->pRight) {
    return code;
  }
  if (QUERY_NODE_COLUMN == nodeType(pOper->pRight)) {
    rightOk = true;
  } else if (QUERY_NODE_OPERATOR == nodeType(pOper->pRight) &&
             OP_TYPE_JSON_GET_VALUE == ((SOperatorNode*)pOper->pRight)->opType) {
    rightOk = true;
  } else if (QUERY_NODE_FUNCTION == nodeType(pOper->pRight)) {
    SFunctionNode* pFunc = (SFunctionNode*)pOper->pRight;
    if (FUNCTION_TYPE_TIMESTAMP_SCALE == pFunc->funcType || FUNCTION_TYPE_TIMETRUNCATE == pFunc->funcType) {
      SListCell* pCell = nodesListGetCell(pFunc->pParameterList, 0);
      if (pCell && pCell->pNode && QUERY_NODE_COLUMN == nodeType(pCell->pNode)) {
        rightOk = true;
      }
    }
  }
  if (!leftOk || !rightOk) {
    return code;
  }

  if (OP_TYPE_EQUAL != pOper->opType) {
    return code;
  }

  if ((QUERY_NODE_OPERATOR == nodeType(pOper->pLeft) || QUERY_NODE_OPERATOR == nodeType(pOper->pRight)) &&
      !(IS_ASOF_JOIN(pJoin->subType) || IS_WINDOW_JOIN(pJoin->subType))) {
    return code;
  }

  // Extract the inner column node, unwrapping __timestamp_scale / timetruncate / JSON operators
  SColumnNode* pLeft = NULL;
  if (QUERY_NODE_COLUMN == nodeType(pOper->pLeft)) {
    pLeft = (SColumnNode*)(pOper->pLeft);
  } else if (QUERY_NODE_OPERATOR == nodeType(pOper->pLeft)) {
    pLeft = (SColumnNode*)((SOperatorNode*)pOper->pLeft)->pLeft;
  } else if (QUERY_NODE_FUNCTION == nodeType(pOper->pLeft)) {
    SListCell* pCell = nodesListGetCell(((SFunctionNode*)pOper->pLeft)->pParameterList, 0);
    pLeft = (SColumnNode*)pCell->pNode;
  }

  SColumnNode* pRight = NULL;
  if (QUERY_NODE_COLUMN == nodeType(pOper->pRight)) {
    pRight = (SColumnNode*)(pOper->pRight);
  } else if (QUERY_NODE_OPERATOR == nodeType(pOper->pRight)) {
    pRight = (SColumnNode*)((SOperatorNode*)pOper->pRight)->pLeft;
  } else if (QUERY_NODE_FUNCTION == nodeType(pOper->pRight)) {
    SListCell* pCell = nodesListGetCell(((SFunctionNode*)pOper->pRight)->pParameterList, 0);
    pRight = (SColumnNode*)pCell->pNode;
  }

  *allTags = (COLUMN_TYPE_TAG == pLeft->colType) && (COLUMN_TYPE_TAG == pRight->colType);

  if (pLeft->node.resType.type != pRight->node.resType.type ||
      pLeft->node.resType.bytes != pRight->node.resType.bytes) {
    return code;
  }
  SNodeList* pLeftCols = ((SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0))->pTargets;
  SNodeList* pRightCols = ((SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1))->pTargets;
  bool       isEqual = false;
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

  bool    allTags = false;
  bool    eqOnCond = false;
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
      char         name[TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN];
      int32_t      len = 0;
      if ('\0' == pCol->tableAlias[0]) {
        len = snprintf(name, sizeof(name), "%s", pCol->colName);
      } else {
        len = snprintf(name, sizeof(name), "%s.%s", pCol->tableAlias, pCol->colName);
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
      .pColHash = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK)};
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
  int32_t    code = TSDB_CODE_SUCCESS;
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

# if 0
static int32_t pdcJoinAddPreFilterColsToTarget(SOptimizeContext* pCxt, SJoinLogicNode* pJoin) {
  if (NULL == pJoin->pFullOnCond) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t    code = TSDB_CODE_SUCCESS;
  SNodeList* pCondCols = NULL;
  code = nodesMakeList(&pCondCols);
  SNodeList* pTargets = NULL;
  if (TSDB_CODE_SUCCESS == code) {
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
# endif

static int32_t pdcJoinAddFilterColsToTarget(SOptimizeContext* pCxt, SJoinLogicNode* pJoin) {
  if (NULL == pJoin->node.pConditions && NULL == pJoin->pFullOnCond) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t    code = TSDB_CODE_SUCCESS;
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

static int32_t pdcJoinSplitConstPrimEqCond(SOptimizeContext* pCxt, SJoinLogicNode* pJoin, SNode** ppCond) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pPrimKeyEqCond = NULL;
  SNode*  pJoinOnCond = NULL;

  if (QUERY_NODE_LOGIC_CONDITION == nodeType(*ppCond) &&
      LOGIC_COND_TYPE_AND == ((SLogicConditionNode*)*ppCond)->condType) {
    code = pdcJoinSplitPrimInLogicCond(pJoin, ppCond, &pPrimKeyEqCond, &pJoinOnCond, true);
  } else if (pdcJoinIsPrimEqualCond(pJoin, *ppCond, true) && (pJoin->leftConstPrimGot || pJoin->rightConstPrimGot)) {
    pPrimKeyEqCond = *ppCond;
    pJoinOnCond = NULL;
  } else {
    return TSDB_CODE_SUCCESS;
  }

  if (TSDB_CODE_SUCCESS == code) {
    pJoin->pPrimKeyEqCond = pPrimKeyEqCond;
    *ppCond = pJoinOnCond;
    if (pJoin->pPrimKeyEqCond && (pJoin->rightConstPrimGot || pJoin->leftConstPrimGot)) {
      code = scalarConvertOpValueNodeTs((SOperatorNode*)pJoin->pPrimKeyEqCond);
    }    
  } else {
    nodesDestroyNode(pPrimKeyEqCond);
    nodesDestroyNode(pJoinOnCond);
  }
  
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

  SNode** ppCond = pJoin->pFullOnCond ? &pJoin->pFullOnCond : &pJoin->node.pConditions;
  bool   errCond = false;
  bool   primCondGot = pdcJoinHasPrimEqualCond(pJoin, *ppCond, &errCond);
  if (!primCondGot) {
    if (errCond && !(IS_INNER_NONE_JOIN(pJoin->joinType, pJoin->subType) && NULL != pJoin->pFullOnCond &&
                     NULL != pJoin->node.pConditions)) {
      return generateUsageErrMsg(pCxt->pPlanCxt->pMsg, pCxt->pPlanCxt->msgLen, TSDB_CODE_PLAN_NOT_SUPPORT_JOIN_COND);
    }

    if (IS_INNER_NONE_JOIN(pJoin->joinType, pJoin->subType) && NULL != pJoin->pFullOnCond &&
        NULL != pJoin->node.pConditions) {
      primCondGot = pdcJoinHasPrimEqualCond(pJoin, pJoin->node.pConditions, &errCond);  
      if (primCondGot) {
        return TSDB_CODE_SUCCESS;
      }
      if (errCond) {
        return generateUsageErrMsg(pCxt->pPlanCxt->pMsg, pCxt->pPlanCxt->msgLen, TSDB_CODE_PLAN_NOT_SUPPORT_JOIN_COND);
      }
    }

    if (IS_WINDOW_JOIN(pJoin->subType) || IS_ASOF_JOIN(pJoin->subType)) {
      return TSDB_CODE_SUCCESS;
    }
  } 

  if ((!pJoin->hashJoinHint) && (pJoin->leftNoOrderedSubQuery || pJoin->rightNoOrderedSubQuery || !primCondGot)) {
    pJoin->noPrimKeyEqCond = true;
    int32_t code = pdcJoinSplitConstPrimEqCond(pCxt, pJoin, ppCond);
    if (code || (pJoin->pPrimKeyEqCond)) {
      return code;
    }
    
    if (IS_INNER_NONE_JOIN(pJoin->joinType, pJoin->subType) && NULL != pJoin->pFullOnCond &&
        NULL != pJoin->node.pConditions) {
      code = pdcJoinSplitConstPrimEqCond(pCxt, pJoin, &pJoin->node.pConditions);
      if (code || pJoin->pPrimKeyEqCond) {
        return code;
      }
    }

    return generateUsageErrMsg(pCxt->pPlanCxt->pMsg, pCxt->pPlanCxt->msgLen, TSDB_CODE_PAR_NOT_SUPPORT_JOIN,
                                     "Join requires valid time series input and primary timestamp equal condition");
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
            return generateUsageErrMsg(pCxt->pPlanCxt->pMsg, pCxt->pPlanCxt->msgLen,
                                       TSDB_CODE_PLAN_NOT_SUPPORT_JOIN_COND);
          }
        }
        FOREACH(pNode, pJoin->pRightEqNodes) {
          SColumnNode* pCol = (SColumnNode*)pNode;
          if (COLUMN_TYPE_TAG != pCol->colType && PRIMARYKEY_TIMESTAMP_COL_ID == pCol->colId) {
            return generateUsageErrMsg(pCxt->pPlanCxt->pMsg, pCxt->pPlanCxt->msgLen,
                                       TSDB_CODE_PLAN_NOT_SUPPORT_JOIN_COND);
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

static EDealRes pdcCheckVirtualTableCond(SNode* pNode, void* pContext) {
  SCpdIsVtableTsCondCxt* pCxt = pContext;
  switch (nodeType(pNode)) {
    case QUERY_NODE_COLUMN: {
      SColumnNode* pCol = (SColumnNode*)pNode;
      if (pCol->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
        pCxt->condHasTs = true;
      }
      break;
    }
    default:
      break;
  }

  return DEAL_RES_CONTINUE;
}

typedef struct SVtablePrimaryCondRewriteCxt {
  int32_t      errCode;
  SColumnNode* pTsScanCol;
} SVtablePrimaryCondRewriteCxt;

/*
 * Find the origin scan timestamp column used by a virtual table child scan.
 * @param pScan Child scan node under the virtual table scan.
 * @return The timestamp column node from the child scan, or NULL if absent.
 */
static SColumnNode* pdcFindVtableScanTsCol(SScanLogicNode* pScan) {
  SNode* pNode = NULL;
  FOREACH(pNode, pScan->pScanCols) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    if (pCol->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
      return pCol;
    }
  }

  return NULL;
}

static int32_t pdcGetExternalCondPrecision(SScanLogicNode* pScan, int8_t *precision) {
  int32_t        code = TSDB_CODE_SUCCESS;
  SColumnNode*   pTsCol = pdcFindVtableScanTsCol(pScan);
  int8_t         fallback = pTsCol != NULL ? (int8_t)pTsCol->node.resType.precision : TSDB_TIME_PRECISION_MILLI;
  SExtTableNode* pExt = (SExtTableNode*)pScan->pExtTableNode;
  if (pExt == NULL || pExt->pExtMeta == NULL || pExt->tsPrimaryColIdx < 0 ||
      pExt->tsPrimaryColIdx >= pExt->pExtMeta->numOfCols) {
    planError("%s get invalid ext table node", __func__ );
    PLAN_ERR_JRET(TSDB_CODE_INVALID_PARA);
  }

  *precision = extTimestampTypePrecision((EExtSourceType)pExt->sourceType, pExt->pExtMeta->pCols[pExt->tsPrimaryColIdx].extTypeName,
                                   pExt->pExtMeta->tsPrecision);

  return code;
_return:
  planError("%s failed since %d", __FUNCTION__ , code);
  return code;
}

static int64_t pdcSaturatingMultiply(int64_t value, int64_t factor) {
  if (value > 0 && value > INT64_MAX / factor) {
    return INT64_MAX;
  }
  if (value < 0 && value < INT64_MIN / factor) {
    return INT64_MIN;
  }
  return value * factor;
}

static int64_t pdcSaturatingAdd(int64_t value, int64_t delta) {
  if (delta > 0 && value > INT64_MAX - delta) {
    return INT64_MAX;
  }
  if (delta < 0 && value < INT64_MIN - delta) {
    return INT64_MIN;
  }
  return value + delta;
}

/*
 * Convert a closed virtual-table range boundary to the source timestamp domain.
 * When the source is finer, include every source tick that renders as the same
 * virtual timestamp after executor precision conversion.
 */
static int64_t pdcConvertVtableRangeBoundary(int64_t value, int8_t vtablePrecision,
                                             int8_t sourcePrecision, bool lowerBound) {
  int64_t vtableTicks = TSDB_TICK_PER_SECOND(vtablePrecision);
  int64_t sourceTicks = TSDB_TICK_PER_SECOND(sourcePrecision);
  if (vtableTicks == sourceTicks) {
    return value;
  }

  if (sourceTicks > vtableTicks) {
    int64_t factor = sourceTicks / vtableTicks;
    int64_t result = pdcSaturatingMultiply(value, factor);
    if (lowerBound && value <= 0) {
      return pdcSaturatingAdd(result, -(factor - 1));
    }
    if (!lowerBound && value >= 0) {
      return pdcSaturatingAdd(result, factor - 1);
    }
    return result;
  }

  int64_t factor = vtableTicks / sourceTicks;
  int64_t result = value / factor;
  int64_t remainder = value % factor;
  if (lowerBound && remainder > 0) {
    ++result;
  } else if (!lowerBound && remainder < 0) {
    --result;
  }
  return result;
}

static int32_t pdcCreateVtableRangeCond(SColumnNode* pTsScanCol, EOperatorType opType,
                                        int64_t value, int8_t sourcePrecision, SNode** ppCond) {
  int32_t        code = TSDB_CODE_SUCCESS;
  SOperatorNode* pOp = NULL;

  PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pOp));
  pOp->opType = opType;
  pOp->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;

  PLAN_ERR_JRET(nodesCloneNode((SNode*)pTsScanCol, &pOp->pLeft));
  ((SColumnNode*)pOp->pLeft)->node.resType.precision = sourcePrecision;
  PLAN_ERR_JRET(nodesMakeValueNodeFromTimestamp(value, &pOp->pRight));
  ((SValueNode*)pOp->pRight)->node.resType.precision = sourcePrecision;

  *ppCond = (SNode*)pOp;
  return code;

_return:
  nodesDestroyNode((SNode*)pOp);
  if (TSDB_CODE_SUCCESS != code) {
    planError("%s failed since %s", __func__, tstrerror(code));
  }
  return code;
}

/*
 * Rewrite virtual table timestamp columns in a pushed-down primary condition.
 * @param pNode Condition node being rewritten.
 * @param pContext Rewrite context containing the origin scan ts column.
 * @return Traversal action for expression rewriting.
 */
static EDealRes pdcRewriteVtablePrimaryTsCol(SNode** pNode, void* pContext) {
  if (QUERY_NODE_COLUMN != nodeType(*pNode)) {
    return DEAL_RES_CONTINUE;
  }

  SColumnNode* pCol = (SColumnNode*)*pNode;
  if (pCol->colId != PRIMARYKEY_TIMESTAMP_COL_ID) {
    return DEAL_RES_CONTINUE;
  }

  SVtablePrimaryCondRewriteCxt* pCxt = pContext;
  SNode*                        pNew = NULL;
  pCxt->errCode = nodesCloneNode((SNode*)pCxt->pTsScanCol, &pNew);
  if (TSDB_CODE_SUCCESS != pCxt->errCode || NULL == pNew) {
    return DEAL_RES_ERROR;
  }
  nodesDestroyNode(*pNode);
  *pNode = pNew;
  return DEAL_RES_IGNORE_CHILD;
}

/*
 * Build an external-source timestamp condition from the closed range extracted
 * from a virtual table predicate. The source timestamp column is cloned from
 * the external scan.
 * @param pScanRange Closed range extracted from the virtual-table condition.
 * @param pOrgScan External scan that will consume the rewritten condition.
 * @param vtablePrecision Virtual table timestamp precision.
 * @param sourcePrecision External source timestamp precision.
 * @param ppRewrittenCond Output source condition AST.
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code.
 */
static int32_t pdcCreateVtableExternalPrimaryCondFromRange(
    const STimeWindow* pScanRange, SScanLogicNode* pOrgScan, int8_t vtablePrecision,
    int8_t sourcePrecision, SNode** ppRewrittenCond) {
  int32_t      code = TSDB_CODE_SUCCESS;
  SNode*       pBoundCond = NULL;
  SColumnNode* pTsScanCol = NULL;

  *ppRewrittenCond = NULL;
  if (vtablePrecision < TSDB_TIME_PRECISION_MILLI ||
      vtablePrecision > TSDB_TIME_PRECISION_NANO ||
      sourcePrecision < TSDB_TIME_PRECISION_MILLI ||
      sourcePrecision > TSDB_TIME_PRECISION_NANO) {
    planError("%s get invalid table precision, vtablePrecision:%d, sourcePrecision:%d", __func__, vtablePrecision, sourcePrecision);
    PLAN_ERR_JRET(TSDB_CODE_INVALID_PARA);
  }

  pTsScanCol = pdcFindVtableScanTsCol(pOrgScan);
  if (NULL == pTsScanCol) {
    planError("%s failed since origin scan has no timestamp column", __func__);
    PLAN_ERR_JRET(TSDB_CODE_VTABLE_INVALID_ORIGIN_TS_COL);
  }

  if (pScanRange->skey != INT64_MIN) {
    int64_t lower = pdcConvertVtableRangeBoundary(pScanRange->skey, vtablePrecision,
                                                   sourcePrecision, true);
    PLAN_ERR_JRET(pdcCreateVtableRangeCond(pTsScanCol, OP_TYPE_GREATER_EQUAL, lower,
                                           sourcePrecision, &pBoundCond));
    PLAN_ERR_JRET(nodesMergeNode(ppRewrittenCond, &pBoundCond));
  }

  if (pScanRange->ekey != INT64_MAX) {
    int64_t upper = pdcConvertVtableRangeBoundary(pScanRange->ekey, vtablePrecision,
                                                   sourcePrecision, false);
    PLAN_ERR_JRET(pdcCreateVtableRangeCond(pTsScanCol, OP_TYPE_LOWER_EQUAL, upper,
                                           sourcePrecision, &pBoundCond));
    PLAN_ERR_JRET(nodesMergeNode(ppRewrittenCond, &pBoundCond));
  }

_return:
  if (code) {
    planError("%s failed since %s", __func__, tstrerror(code));
    nodesDestroyNode(pBoundCond);
    NODES_DESTORY_NODE(*ppRewrittenCond);
  }
  return code;
}

/*
 * Clone a virtual table primary condition for one origin scan and rewrite
 * virtual table ts columns to that scan's source table column.
 * @param pPrimaryKeyCond Primary-key condition extracted from the virtual scan.
 * @param pOrgScan Origin scan that will consume the rewritten condition.
 * @param ppRewrittenCond Output cloned and rewritten condition node.
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code.
 */
static int32_t pdcCloneRewriteVtablePrimaryCond(SNode* pPrimaryKeyCond,
                                                SScanLogicNode* pOrgScan,
                                                SNode** ppRewrittenCond) {
  int32_t                      code = TSDB_CODE_SUCCESS;
  SCpdIsVtableTsCondCxt        checkCxt = {.condHasTs = false};
  SVtablePrimaryCondRewriteCxt rewriteCxt = {.errCode = TSDB_CODE_SUCCESS, .pTsScanCol = NULL};

  PLAN_ERR_JRET(nodesCloneNode(pPrimaryKeyCond, ppRewrittenCond));

  nodesWalkExpr(*ppRewrittenCond, pdcCheckVirtualTableCond, &checkCxt);
  if (!checkCxt.condHasTs) {
    goto _return;
  }

  rewriteCxt.pTsScanCol = pdcFindVtableScanTsCol(pOrgScan);
  if (NULL == rewriteCxt.pTsScanCol) {
    PLAN_ERR_JRET(TSDB_CODE_VTABLE_INVALID_ORIGIN_TS_COL);
  }

  nodesRewriteExpr(ppRewrittenCond, pdcRewriteVtablePrimaryTsCol, &rewriteCxt);
  PLAN_ERR_JRET(rewriteCxt.errCode);

_return:

  if (code) {
    planError("%s failed since %s", __func__, tstrerror(code));
    NODES_DESTORY_NODE(*ppRewrittenCond);
  }
  return code;
}


static int32_t pdcJoinGetOpTableCondTypes(SNode* pCond, SSHashObj* pLeftTables, SSHashObj* pRightTables,
                                          bool* tableCondTypes) {
  SCpdIsMultiTableCondCxt cxt = {.pLeftTbls = pLeftTables,
                                 .pRightTbls = pRightTables,
                                 .havaLeftCol = false,
                                 .haveRightCol = false,
                                 .condIsNull = false};
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

static int32_t pdcJoinGetLogicTableCondTypes(SNode* pCond, SSHashObj* pLeftTables, SSHashObj* pRightTables,
                                             bool* tableCondTypes) {
  SLogicConditionNode* pLogicCond = (SLogicConditionNode*)pCond;
  int32_t              code = 0;
  SNode*               pSCond = NULL;
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

static int32_t pdcGetTableCondTypes(SNode* pCond, SSHashObj* pLeftTables, SSHashObj* pRightTables,
                                    bool* tableCondTypes) {
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
  int32_t    code = collectTableAliasFromNodes(nodesListGetNode(pJoin->node.pChildren, 0), &pLeftTables);
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
      SFunctionNode*         pFunc = (SFunctionNode*)pNode;
      SCpdIsMultiTableResCxt cxt = {.pLeftTbls = pCxt->pLeftTbls,
                                    .pRightTbls = pCxt->pRightTbls,
                                    .haveLeftCol = false,
                                    .haveRightCol = false,
                                    .leftColNonNull = true,
                                    .rightColNonNull = true};

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

  int32_t    code = 0;
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
  bool        tableResNonNull[2] = {true, true};
  bool        tableResOp[2] = {false, false};
  if (QUERY_NODE_LOGIC_PLAN_AGG == nodeType(pParent)) {
    SAggLogicNode* pAgg = (SAggLogicNode*)pParent;
    if (NULL != pAgg->pGroupKeys) {
      tableResNonNull[0] = false;
      tableResNonNull[1] = false;
    } else {
      SCpdIsMultiTableResCxt cxt = {.pLeftTbls = pLeftTables,
                                    .pRightTbls = pRightTables,
                                    .haveLeftCol = false,
                                    .haveRightCol = false,
                                    .leftColNonNull = true,
                                    .rightColNonNull = true,
                                    .leftColOp = false,
                                    .rightColOp = false};

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
  if (pdcShouldKeepNodeConditionsForExternalSubtree((SLogicNode*)pJoin, "join")) {
    // Preserve the parent WHERE condition for federated pushdown, but still
    // split the primary time-key condition from the JOIN ON clause.  The
    // physical merge join consumes pPrimKeyEqCond to build its timestamp
    // scale expression; skipping this split makes a cross-precision external
    // join compare the provider-native timestamp directly with the local one.
    if (NULL != pJoin->pFullOnCond && !IS_WINDOW_JOIN(pJoin->subType) && NULL == pJoin->addPrimEqCond &&
        NULL == pJoin->pPrimKeyEqCond) {
      int32_t code = pdcJoinSplitPrimEqCond(pCxt, pJoin);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
    }
    OPTIMIZE_FLAG_SET_MASK(pJoin->node.optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE);
    return TSDB_CODE_SUCCESS;
  }
  if (pJoin->joinAlgo != JOIN_ALGO_UNKNOWN) {
    return TSDB_CODE_SUCCESS;
  }
  EJoinType    t = pJoin->joinType;
  EJoinSubType s = pJoin->subType;
  SNode*       pOnCond = NULL;
  SNode*       pLeftChildCond = NULL;
  SNode*       pRightChildCond = NULL;
  int32_t      code = pdcJoinCheckAllCond(pCxt, pJoin);

  if (TSDB_CODE_SUCCESS == code && IS_ASOF_JOIN(pJoin->subType) && NULL != pJoin->pFullOnCond &&
      NULL == pJoin->addPrimEqCond && NULL == pJoin->pPrimKeyEqCond) {
    code = pdcJoinSplitPrimEqCond(pCxt, pJoin);
  }
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
          code =
              pdcPushDownCondToChild(pCxt, (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1), &pRightChildCond);
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

  if (TSDB_CODE_SUCCESS == code && NULL != pJoin->pFullOnCond && !IS_WINDOW_JOIN(pJoin->subType) &&
      NULL == pJoin->addPrimEqCond && NULL == pJoin->pPrimKeyEqCond) {
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

  // if (TSDB_CODE_SUCCESS == code) {
  //   code = pdcJoinAddPreFilterColsToTarget(pCxt, pJoin);
  // }

  if (TSDB_CODE_SUCCESS == code) {
    code = pdcJoinAddFilterColsToTarget(pCxt, pJoin);
  }

  // For external-table JOINs, the entire plan stays in one subplan (no Exchange between
  // the JOIN and the Sort/Project above it). The physical plan builder uses the JOIN logic
  // node's pTargets to create the JOIN's output block desc; if pTargets is NULL the block
  // is empty and the parent Sort/Project physi node cannot resolve its columns, causing
  // TSDB_CODE_PLAN_SLOT_NOT_FOUND. Normal TDengine-table JOINs are split into separate
  // subplans, so their pTargets never needs to be set here.
  if (TSDB_CODE_SUCCESS == code && NULL == pJoin->node.pTargets) {
    bool hasExternalChild = false;
    SNode* pChildNode = NULL;
    FOREACH(pChildNode, pJoin->node.pChildren) {
      if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pChildNode) &&
          SCAN_TYPE_EXTERNAL == ((SScanLogicNode*)pChildNode)->scanType) {
        hasExternalChild = true;
        break;
      }
    }
    if (hasExternalChild && NULL != pJoin->node.pParent && NULL != pJoin->node.pParent->pTargets) {
      code = nodesCloneList(pJoin->node.pParent->pTargets, &pJoin->node.pTargets);
    }
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
  if (QUERY_NODE_FUNCTION == nodeType(pNode)) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    if (fmIsAggFunc(pFunc->funcId) && !fmIsGroupKeyFunc(pFunc->funcId)) {
      planDebug("PDC-agg: HAVING condition contains aggregate func name=%s funcId=%d alias=%s",
                pFunc->functionName, pFunc->funcId, pFunc->node.aliasName);
      pCxt->hasAggFunc = true;
      return DEAL_RES_END;
    }
  }

  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    SNode* pAggFunc = NULL;
    FOREACH(pAggFunc, pCxt->pAgg->pAggFuncs) {
      if (strcmp(((SColumnNode*)pNode)->colName, ((SFunctionNode*)pAggFunc)->node.aliasName) == 0) {
        planDebug("PDC-agg: HAVING condition references aggregate alias col=%s aggAlias=%s",
                  ((SColumnNode*)pNode)->colName, ((SFunctionNode*)pAggFunc)->node.aliasName);
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
  return nodesMergeNode(&pAgg->node.pConditions, pAggFuncCond);
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
  if (pdcShouldKeepNodeConditionsForExternalSubtree((SLogicNode*)pAgg, "agg")) {
    OPTIMIZE_FLAG_SET_MASK(pAgg->node.optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE);
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

static EDealRes pdcSetOpExprHasColumnImpl(SNode* pNode, void* pCtx) {
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    *(bool*)pCtx = true;
    return DEAL_RES_END;
  }
  return DEAL_RES_CONTINUE;
}

static bool pdcSetOpExprHasColumn(SNode* pExpr) {
  bool hasColumn = false;
  nodesWalkExpr(pExpr, pdcSetOpExprHasColumnImpl, &hasColumn);
  return hasColumn;
}

static bool pdcSetOpIsRangeOp(EOperatorType opType) {
  return OP_TYPE_GREATER_THAN == opType || OP_TYPE_GREATER_EQUAL == opType || OP_TYPE_LOWER_THAN == opType ||
         OP_TYPE_LOWER_EQUAL == opType || OP_TYPE_EQUAL == opType;
}

static bool pdcSetOpCondIsPushablePrimKeyRange(SNode* pCond) {
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pCond)) {
    SLogicConditionNode* pLogicCond = (SLogicConditionNode*)pCond;
    /* Conservative: only AND trees of simple pk range predicates can be pushed down.
     * OR/NOT ranges are not represented as a single contiguous scan time range here,
     * so they stay on the setop project for final filtering. */
    if (LOGIC_COND_TYPE_AND != pLogicCond->condType || NULL == pLogicCond->pParameterList) {
      return false;
    }

    SNode* pItem = NULL;
    FOREACH(pItem, pLogicCond->pParameterList) {
      if (!pdcSetOpCondIsPushablePrimKeyRange(pItem)) {
        return false;
      }
    }
    return true;
  }

  if (QUERY_NODE_OPERATOR != nodeType(pCond)) {
    return false;
  }

  SOperatorNode* pOp = (SOperatorNode*)pCond;
  if (!pdcSetOpIsRangeOp(pOp->opType)) {
    return false;
  }

  if (NULL == pOp->pLeft || NULL == pOp->pRight) {
    return false;
  }

  bool leftIsPk = QUERY_NODE_COLUMN == nodeType(pOp->pLeft) && isPrimaryKeyImpl(pOp->pLeft);
  bool rightIsPk = QUERY_NODE_COLUMN == nodeType(pOp->pRight) && isPrimaryKeyImpl(pOp->pRight);
  if (!leftIsPk && !rightIsPk) {
    return false;
  }
  if (leftIsPk && rightIsPk) {
    return false;
  }

  SNode* pOther = leftIsPk ? pOp->pRight : pOp->pLeft;
  return !pdcSetOpExprHasColumn(pOther);
}

/* Extract the simple primary-timestamp range sub-conditions from pCond and
 * return them as a new node (owned by caller) suitable for pushing to branch
 * scans.  Sets *pIsPureTsCond = true when the entire condition is composed of
 * pushable primary-timestamp range predicates.
 *
 * - Simple pk-range condition: returns a clone of the condition.
 * - Top-level AND with some simple pk-range sub-terms: returns AND of those clones.
 * - Non-AND mixed condition (OR, single non-ts term): returns NULL.
 *
 * The returned node is used only as an I/O hint pushed to branch scans;
 * the original pCond stays on the setop project for definitive filtering.
 */
static int32_t pdcExtractPrimKeyPushCond(SNode* pCond, SNode** ppOut, bool* pIsPureTsCond) {
  *ppOut         = NULL;
  *pIsPureTsCond = false;

  /* Only simple comparisons with a bare primary key on one side and a
   * column-free expression on the other side can be cloned and pushed as scan
   * ranges.  Expressions such as `ts + 1000 <= const` still reference only the
   * primary key, but pushing them to a branch changes the slot context and can
   * fail physical slot resolution.  Keep those expressions on the setop
   * project for final filtering.
   * OR conditions (e.g. ts < X OR ts > Y) cannot be expressed as a single
   * contiguous scan time range and must stay as a Filter on the setop project;
   * pushing them would cause a slot-key resolution failure in the physical
   * planner because the column reference context changes at the branch boundary. */
  if (pdcSetOpCondIsPushablePrimKeyRange(pCond)) {
    *pIsPureTsCond = true;
    return nodesCloneNode(pCond, ppOut);
  }

  /* Non-AND top-level condition with non-pushable shape: nothing pushable. */
  if (QUERY_NODE_LOGIC_CONDITION != nodeType(pCond) ||
      LOGIC_COND_TYPE_AND != ((SLogicConditionNode*)pCond)->condType) {
    return TSDB_CODE_SUCCESS;
  }

  /* AND: collect clones of simple pk-range sub-terms. */
  SNodeList* pTsList = NULL;
  int32_t    code    = TSDB_CODE_SUCCESS;
  SNode*     pItem   = NULL;
  int32_t    itemNum  = 0;
  int32_t    pushNum  = 0;
  FOREACH(pItem, ((SLogicConditionNode*)pCond)->pParameterList) {
    itemNum++;
    if (!pdcSetOpCondIsPushablePrimKeyRange(pItem)) continue;
    SNode* pClone = NULL;
    code = nodesCloneNode(pItem, &pClone);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyList(pTsList);
      return code;
    }
    code = nodesListMakeAppend(&pTsList, pClone);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode(pClone);
      nodesDestroyList(pTsList);
      return code;
    }
    pushNum++;
  }

  if (NULL == pTsList) return TSDB_CODE_SUCCESS;
  *pIsPureTsCond = (pushNum == itemNum);

  /* nodesMergeConds: single item  ->  unwrap; multiple  ->  AND wrapper.
   * On failure it does not free *pSrc, so destroy pTsList explicitly. */
  code = nodesMergeConds(ppOut, &pTsList);
  if (TSDB_CODE_SUCCESS != code) {
    *ppOut = NULL;
    nodesDestroyList(pTsList);
  }
  return code;
}

/*
 * Distribute the primary-timestamp part of the condition from a set-operator
 * project (UNION ALL) to child nodes for I/O pruning.
 *
 * - If the setop project itself has LIMIT/SLIMIT, do not distribute at all.
 * - For pure-ts conditions with no LIMIT on any child: clear the condition
 *   from the setop project (it is now fully handled by branch scans).
 * - For mixed AND conditions (ts + non-ts): push only the ts sub-conditions
 *   as I/O hints; keep the full original condition on the setop project for
 *   final filtering of non-ts predicates.
 * - Children that have their own LIMIT/SLIMIT are skipped (cannot push past
 *   a LIMIT boundary); if any child is skipped the setop project retains
 *   the condition for those children.
 */
/* Returns the 0-based index of the primary-key column in the setop output
 * projection list, or -1 if not found. */
static int32_t pdcSetOpFindPkProjIdx(SProjectLogicNode* pSetOpProj) {
  int32_t idx  = 0;
  SNode*  pCol = NULL;
  FOREACH(pCol, pSetOpProj->pProjections) {
    if (QUERY_NODE_COLUMN == nodeType(pCol) &&
        PRIMARYKEY_TIMESTAMP_COL_ID == ((SColumnNode*)pCol)->colId) {
      return idx;
    }
    idx++;
  }
  return -1;
}

/* Returns true when the projected expression at position pkIdx of a UNION ALL
 * branch project node is an arithmetic operator (e.g. _rowts + 3600000).
 * In that case the outer ts condition must NOT be pushed to the scan: pushing
 * ts >= X to the scan as _rowts >= X is wrong because the actual scan boundary
 * should be _rowts >= X - offset.  The condition must stay on the setop project
 * to be applied correctly after the arithmetic is evaluated. */
static bool pdcSetOpBranchProjAtIdxIsArithExpr(SLogicNode* pBranch, int32_t pkIdx) {
  if (QUERY_NODE_LOGIC_PLAN_PROJECT != nodeType(pBranch) || pkIdx < 0) {
    return false;
  }
  SProjectLogicNode* pProj = (SProjectLogicNode*)pBranch;
  if (NULL == pProj->pProjections || LIST_LENGTH(pProj->pProjections) <= pkIdx) {
    return false;
  }
  SNode* pPkExpr = nodesListGetNode(pProj->pProjections, pkIdx);
  return QUERY_NODE_OPERATOR == nodeType(pPkExpr);
}

static bool pdcSetOpBranchHasVirtualScan(SLogicNode* pNode) {
  if (NULL == pNode) {
    return false;
  }

  if (QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN == nodeType(pNode)) {
    return true;
  }

  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    if (pdcSetOpBranchHasVirtualScan((SLogicNode*)pChild)) {
      return true;
    }
  }

  return false;
}

typedef struct SSetOpRewritePkCondCxt {
  SNode*  pChildPkTarget;
  int32_t errCode;
  bool    rewritten;
} SSetOpRewritePkCondCxt;

static EDealRes pdcSetOpRewritePkCondForChildImpl(SNode** ppNode, void* pCtx) {
  if (QUERY_NODE_COLUMN != nodeType(*ppNode) || !isPrimaryKeyImpl(*ppNode)) {
    return DEAL_RES_CONTINUE;
  }

  SSetOpRewritePkCondCxt* pCxt = pCtx;
  SNode*                  pNew = NULL;
  pCxt->errCode = nodesCloneNode(pCxt->pChildPkTarget, &pNew);
  if (TSDB_CODE_SUCCESS != pCxt->errCode || NULL == pNew) {
    return DEAL_RES_ERROR;
  }

  nodesDestroyNode(*ppNode);
  *ppNode = pNew;
  pCxt->rewritten = true;
  return DEAL_RES_IGNORE_CHILD;
}

static int32_t pdcSetOpRewritePkCondForChild(SNode** ppCond, SLogicNode* pChild, int32_t pkIdx, bool* pRewritten) {
  *pRewritten = false;
  if (NULL == ppCond || NULL == *ppCond || QUERY_NODE_LOGIC_PLAN_PROJECT != nodeType(pChild) || pkIdx < 0) {
    return TSDB_CODE_SUCCESS;
  }

  SProjectLogicNode* pChildProj = (SProjectLogicNode*)pChild;
  if (NULL == pChildProj->node.pTargets || LIST_LENGTH(pChildProj->node.pTargets) <= pkIdx) {
    return TSDB_CODE_SUCCESS;
  }

  SNode* pChildPkTarget = nodesListGetNode(pChildProj->node.pTargets, pkIdx);
  if (NULL == pChildPkTarget) {
    return TSDB_CODE_SUCCESS;
  }

  SSetOpRewritePkCondCxt cxt = {.pChildPkTarget = pChildPkTarget, .errCode = TSDB_CODE_SUCCESS, .rewritten = false};
  nodesRewriteExpr(ppCond, pdcSetOpRewritePkCondForChildImpl, &cxt);
  *pRewritten = cxt.rewritten;
  return cxt.errCode;
}
static int32_t pdcDealSetOpProject(SOptimizeContext* pCxt, SProjectLogicNode* pSetOpProj) {
  int32_t code = TSDB_CODE_SUCCESS;

  /* Do not push through the setop project's own LIMIT/SLIMIT boundary. */
  if (NULL != pSetOpProj->node.pLimit || NULL != pSetOpProj->node.pSlimit) {
    OPTIMIZE_FLAG_SET_MASK(pSetOpProj->node.optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE);
    return code;
  }

  /* Extract the pushable (primary-timestamp-only) part of the condition in a
   * single pass.  pdcExtractPrimKeyPushCond also tells us whether the entire
   * condition is pure-ts (isPureTsCond) so we do not need a second traversal. */
  bool   isPureTsCond = false;
  SNode* pTsPushCond  = NULL;
  code = pdcExtractPrimKeyPushCond(pSetOpProj->node.pConditions, &pTsPushCond, &isPureTsCond);
  if (TSDB_CODE_SUCCESS != code) return code;

  if (NULL == pTsPushCond) {
    /* No pushable ts sub-condition (non-ts predicate or OR with non-ts). */
    OPTIMIZE_FLAG_SET_MASK(pSetOpProj->node.optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE);
    return code;
  }

  /* Clone-push the ts condition into each child that has no LIMIT/SLIMIT and
   * whose primary-key projected column is not an arithmetic expression on _rowts.
   * Arithmetic expressions (e.g. _rowts + offset) require the outer condition
   * to be applied after evaluation; pushing ts >= X to the scan would
   * incorrectly interpret it as _rowts >= X, ignoring the offset. */
  int32_t pkIdx          = pdcSetOpFindPkProjIdx(pSetOpProj);
  bool   anyChildSkipped = false;
  bool   pushedAny       = false;
  SNode* pChildNode      = NULL;
  FOREACH(pChildNode, pSetOpProj->node.pChildren) {
    SLogicNode* pChild = (SLogicNode*)pChildNode;
    if (NULL != pChild->pLimit || NULL != pChild->pSlimit) {
      anyChildSkipped = true;
      continue;
    }
    if (pdcSetOpBranchProjAtIdxIsArithExpr(pChild, pkIdx)) {
      anyChildSkipped = true;
      continue;
    }
    bool childHasVirtualScan = pdcSetOpBranchHasVirtualScan(pChild);
    SNode* pPushCond = NULL;
    code = nodesCloneNode(pTsPushCond, &pPushCond);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode(pTsPushCond);
      return code;
    }
    if (childHasVirtualScan) {
      bool rewritten = false;
      code = pdcSetOpRewritePkCondForChild(&pPushCond, pChild, pkIdx, &rewritten);
      if (TSDB_CODE_SUCCESS != code) {
        nodesDestroyNode(pPushCond);
        nodesDestroyNode(pTsPushCond);
        return code;
      }
      if (!rewritten) {
        nodesDestroyNode(pPushCond);
        anyChildSkipped = true;
        continue;
      }
    }
    code = nodesMergeNode(&pChild->pConditions, &pPushCond);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode(pPushCond);
      nodesDestroyNode(pTsPushCond);
      return code;
    }
    pushedAny = true;
  }

  nodesDestroyNode(pTsPushCond);

  if (pushedAny) {
    /* For a pure-ts condition fully delivered to all children, remove it
     * from the setop project (redundant re-filter).  For mixed conditions
     * or when some children had LIMIT or arithmetic-expression aliases, the
     * original full condition must stay on the setop project for final
     * filtering. */
    if (isPureTsCond && !anyChildSkipped) {
      nodesDestroyNode(pSetOpProj->node.pConditions);
      pSetOpProj->node.pConditions = NULL;
    }
    pCxt->optimized = true;
  }

  OPTIMIZE_FLAG_SET_MASK(pSetOpProj->node.optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE);
  return code;
}

static int32_t pdcDealProject(SOptimizeContext* pCxt, SProjectLogicNode* pProject) {
  if (NULL == pProject->node.pConditions ||
      OPTIMIZE_FLAG_TEST_MASK(pProject->node.optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE)) {
    return TSDB_CODE_SUCCESS;
  }
  if (pdcShouldKeepNodeConditionsForExternalSubtree((SLogicNode*)pProject, "project")) {
    OPTIMIZE_FLAG_SET_MASK(pProject->node.optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE);
    return TSDB_CODE_SUCCESS;
  }

  /* UNION ALL merge-point: distribute ts conditions to branch children. */
  if (pProject->isSetOpProj) {
    return pdcDealSetOpProject(pCxt, pProject);
  }

  // TODO: remove it after full implementation of pushing down to child
  if (1 != LIST_LENGTH(pProject->node.pChildren)) {
    return TSDB_CODE_SUCCESS;
  }

  SLogicNode* pChild = (SLogicNode*)nodesListGetNode(pProject->node.pChildren, 0);

  if (NULL != pProject->node.pLimit || NULL != pProject->node.pSlimit) {
    return TSDB_CODE_SUCCESS;
  }

  if (NULL != pChild->pLimit || NULL != pChild->pSlimit) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pProjCond = NULL;
  code = rewriteProjectCondForPushDown(pCxt, pProject, &pProjCond);
  if (TSDB_CODE_SUCCESS == code) {
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
  if (pdcShouldKeepNodeConditionsForExternalSubtree(pLogicNode,
                                                    QUERY_NODE_LOGIC_PLAN_SORT == nodeType(pLogicNode) ? "sort" : "partition")) {
    OPTIMIZE_FLAG_SET_MASK(pLogicNode->optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE);
    return TSDB_CODE_SUCCESS;
  }
  /* Do not push a condition past a LIMIT/SLIMIT boundary on the current node.
   * The condition must be evaluated on this node's output (after LIMIT is
   * applied), so pushing it to a child would change how many rows the LIMIT
   * sees and alter query semantics. */
  if (NULL != pLogicNode->pLimit || NULL != pLogicNode->pSlimit) {
    OPTIMIZE_FLAG_SET_MASK(pLogicNode->optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE);
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

/*
 * Push virtual-table timestamp conditions down to each origin scan and keep
 * non-pushable predicates on the virtual scan node.
 * @param pCxt Optimizer context for the current subplan.
 * @param pVScan Virtual table scan node being rewritten in place.
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code.
 */
static int32_t pdcDealVirtualTable(SOptimizeContext* pCxt, SVirtualScanLogicNode* pVScan) {
  if (NULL == pVScan->node.pConditions || OPTIMIZE_FLAG_TEST_MASK(pVScan->node.optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE) ||
      pVScan->tableType == TSDB_SUPER_TABLE ||
      (pVScan->node.pParent && nodeType(pVScan->node.pParent) == QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL)) {
    return TSDB_CODE_SUCCESS;
  }

  if (inStreamCalcClause(pCxt->pPlanCxt)) {
    return TSDB_CODE_SUCCESS;
  }

  SNode*      pPrimaryKeyCond = NULL;
  SNode*      pTagIndexCond = NULL;
  SNode*      pTagCond = NULL;
  SNode*      pOtherCond = NULL;
  SNode*      pPushCond = NULL;
  SNode*      pPushDownOtherCond = NULL;
  SNode*      pRemotePrimaryKeyCond = NULL;
  int32_t     code = TSDB_CODE_SUCCESS;
  bool        hasExternalScan = nodeHasExternalScan((SLogicNode*)pVScan);
  STimeWindow timeRange = {0};
  TAOS_SET_OBJ_ALIGNED(&timeRange, TSWINDOW_INITIALIZER);

  PLAN_ERR_JRET(filterPartitionCond(&pVScan->node.pConditions, &pPrimaryKeyCond, &pTagIndexCond, &pTagCond, &pOtherCond));
  if (NULL == pPrimaryKeyCond) {
    goto _return1;
  }

  PLAN_ERR_JRET(pushDownCondOptCalcTimeRange(pCxt, &timeRange, &pPrimaryKeyCond, &pPushDownOtherCond, &pRemotePrimaryKeyCond));
  if (!IS_TSWINDOW_SPECIFIED(timeRange) && !pRemotePrimaryKeyCond) {
    // no need to push down
    goto _return1;
  }

  for (int32_t i = 0; i < LIST_LENGTH(pVScan->node.pChildren); ++i) {
    SLogicNode* pChild = (SLogicNode*)nodesListGetNode(pVScan->node.pChildren, i);
    if (NULL == pChild) {
      PLAN_ERR_JRET(
          generateUsageErrMsg(pCxt->pPlanCxt->pMsg, pCxt->pPlanCxt->msgLen, TSDB_CODE_VTABLE_INVALID_ORIGIN_SCAN,
                              "pdcDealVirtualTable get invalid origin scan logic node from vtable scan node"));
    }
    // TAG_REF_SOURCE children are valid vtable children but carry no timestamp column  -  skip pushdown for them.
    if (QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pChild)) {
      continue;
    }
    SScanLogicNode* pOrgScan = (SScanLogicNode*)pChild;

    if (pOrgScan->scanType == SCAN_TYPE_EXTERNAL) {
      if (IS_TSWINDOW_SPECIFIED(timeRange)) {
        int8_t precision = TSDB_DEFAULT_PRECISION;
        PLAN_ERR_JRET(pdcGetExternalCondPrecision(pOrgScan, &precision));
        PLAN_ERR_JRET(pdcCreateVtableExternalPrimaryCondFromRange(
            &timeRange, pOrgScan, pVScan->node.precision, precision, &pPushCond));
        PLAN_ERR_JRET(nodesMergeNode(&pOrgScan->node.pConditions, &pPushCond));
        pPushCond = NULL;
      }
      continue;
    }

    if (pOrgScan->scanType != SCAN_TYPE_TABLE) {
      continue;
    }

    if (pRemotePrimaryKeyCond) {
      PLAN_ERR_JRET(pdcCloneRewriteVtablePrimaryCond(pRemotePrimaryKeyCond, pOrgScan, &pPushCond));
      PLAN_ERR_JRET(nodesMergeNode(&pOrgScan->pPrimaryCond, &pPushCond));
      pPushCond = NULL;
    }
    if (pVScan->node.precision != pOrgScan->node.precision) {
      // Skip sentinel values (TSKEY_MIN/TSKEY_MAX = unbounded) — convertTimePrecision
      // only guards positive overflow, so TSKEY_MIN * 1000 would be signed-overflow UB.
      // scanoperator.c applies the same guard for its time-window conversion.
      pOrgScan->scanRange.skey =
          (timeRange.skey == TSKEY_MIN || timeRange.skey == TSKEY_MAX)
              ? timeRange.skey
              : convertTimePrecision(timeRange.skey, pVScan->node.precision, pOrgScan->node.precision);
      pOrgScan->scanRange.ekey =
          (timeRange.ekey == TSKEY_MIN || timeRange.ekey == TSKEY_MAX)
              ? timeRange.ekey
              : convertTimePrecision(timeRange.ekey, pVScan->node.precision, pOrgScan->node.precision);
    } else {
      pOrgScan->scanRange = timeRange;
    }
  }

  NODES_DESTORY_NODE(pPrimaryKeyCond);
  OPTIMIZE_FLAG_SET_MASK(pVScan->node.optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE);
  pCxt->optimized = true;

_return1:
  if (pPrimaryKeyCond) {
    PLAN_ERR_JRET(nodesMergeNode(&pVScan->node.pConditions, &pPrimaryKeyCond));
  }
  if (pTagIndexCond) {
    PLAN_ERR_JRET(nodesMergeNode(&pVScan->node.pConditions, &pTagIndexCond));
  }
  if (pTagCond) {
    PLAN_ERR_JRET(nodesMergeNode(&pVScan->node.pConditions, &pTagCond));
  }
  if (pOtherCond) {
    PLAN_ERR_JRET(nodesMergeNode(&pVScan->node.pConditions, &pOtherCond));
  }
  if (pPushDownOtherCond) {
    PLAN_ERR_JRET(nodesMergeNode(&pVScan->node.pConditions, &pPushDownOtherCond));
  }
  if (hasExternalScan && pRemotePrimaryKeyCond) {
    PLAN_ERR_JRET(nodesMergeNode(&pVScan->node.pConditions, &pRemotePrimaryKeyCond));
  }
  NODES_DESTORY_NODE(pRemotePrimaryKeyCond);
  return code;

_return:
  planError("%s failed since %s", __func__, tstrerror(code));
  nodesDestroyNode(pPrimaryKeyCond);
  nodesDestroyNode(pTagIndexCond);
  nodesDestroyNode(pTagCond);
  nodesDestroyNode(pOtherCond);
  nodesDestroyNode(pPushDownOtherCond);
  nodesDestroyNode(pRemotePrimaryKeyCond);
  nodesDestroyNode(pPushCond);
  return code;
}

static bool pdcInterpInputEligible(const SLogicNode* pInput) {
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pInput)) {
    return true;
  }

  if (QUERY_NODE_LOGIC_PLAN_PROJECT == nodeType(pInput)) {
    // A remaining filter means the project has not been fully pushed down.
    // A surviving ordered subquery must also preserve the ASC timeline that
    // the MAIN_FIRST scan protocol requires.
    return NULL == pInput->pConditions &&
           ORDER_ASC == pInput->outputTsOrder;
  }

  if (QUERY_NODE_LOGIC_PLAN_PARTITION == nodeType(pInput)) {
    return true;
  }

  return false;
}

static bool pdcInterpInputNeedsMultiGroup(const SLogicNode* pInput) {
  if (NULL == pInput ||
      QUERY_NODE_LOGIC_PLAN_PARTITION != nodeType(pInput)) {
    return false;
  }

  const SPartitionLogicNode* pPartition =
      (const SPartitionLogicNode*)pInput;
  return keysHasCol(pPartition->pPartitionKeys);
}

static bool pdcInterpScanFromVirtualTable(const SScanLogicNode* pScan) {
  SNode* pNode = NULL;
  FOREACH(pNode, pScan->pScanCols) {
    if (QUERY_NODE_COLUMN != nodeType(pNode)) {
      continue;
    }

    int8_t tableType = ((SColumnNode*)pNode)->tableType;
    if (TSDB_VIRTUAL_NORMAL_TABLE == tableType ||
        TSDB_VIRTUAL_CHILD_TABLE == tableType) {
      return true;
    }
  }
  return false;
}

// T5.1a provides the in-memory result store and the MAIN_FIRST consumer for
// final ordinary TableScan shapes. Later shapes keep their own final gates.
static const bool pdcInterpMainFirstConsumerReady = true;

// Locate the scan feeding an interp node through at most one unary node.
// This topology-only lookup also serves legacy predicate pushdown. MAIN_FIRST
// callers must validate the direct input with pdcInterpInputEligible().
// ppInput is optional and receives that direct input.
static SScanLogicNode* pdcInterpFindScan(SInterpFuncLogicNode* pInterp,
                                         SLogicNode** ppInput) {
  if (1 != LIST_LENGTH(pInterp->node.pChildren)) {
    return NULL;
  }

  SLogicNode* pInput =
      (SLogicNode*)nodesListGetNode(pInterp->node.pChildren, 0);
  if (NULL != ppInput) {
    *ppInput = pInput;
  }
  SLogicNode* pChild = pInput;
  if (QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pChild)) {
    if (1 != LIST_LENGTH(pChild->pChildren)) {
      return NULL;
    }
    pChild = (SLogicNode*)nodesListGetNode(pChild->pChildren, 0);
  }

  if (QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pChild)) {
    return NULL;
  }

  return (SScanLogicNode*)pChild;
}

// Pre-split optimization only marks semantic candidates. splitLogicPlan may
// still promote a table scan or fan it out into child subplans, so the final
// mode is decided by postSplitOptimize.
static int8_t pdcGetInterpCandidateMode(const SOptimizeContext* pCxt,
                                        const SInterpFuncLogicNode* pInterp,
                                        const SLogicNode* pInput,
                                        const SScanLogicNode* pScan) {
  if (inStreamCalcClause(pCxt->pPlanCxt) ||
      pInterp->timelineSource != TIME_LINE_SOURCE_PRIMARY_TS ||
      !pdcInterpInputEligible(pInput) || pScan->paraTablesSort ||
      pScan->virtualStableScan || pdcInterpScanFromVirtualTable(pScan) ||
      pScan->scanType == SCAN_TYPE_STREAM ||
      pScan->scanType == SCAN_TYPE_EXTERNAL) {
    return INTERP_MODE_LEGACY;
  }

  if (NULL == pScan->pExtScanRange && NULL == pScan->pExtTimeRange) {
    return INTERP_MODE_LEGACY;
  }

  return pdcInterpInputNeedsMultiGroup(pInput)
             ? INTERP_MODE_MULTI_GROUP
             : INTERP_MODE_MAIN_FIRST;
}

static void pdcSetInterpCandidateMode(const SOptimizeContext* pCxt,
                                      SInterpFuncLogicNode* pInterp) {
  SLogicNode*     pInput = NULL;
  SScanLogicNode* pScan = pdcInterpFindScan(pInterp, &pInput);
  pInterp->requireMainMembership =
      pdcInterpInputNeedsMultiGroup(pInput);

  pInterp->interpMode = (NULL == pScan)
                            ? INTERP_MODE_LEGACY
                            : pdcGetInterpCandidateMode(pCxt, pInterp, pInput,
                                                        pScan);
  if (NULL != pScan) {
    pScan->interpMode = pInterp->interpMode;
  }
  if (NULL != pInput &&
      QUERY_NODE_LOGIC_PLAN_PARTITION == nodeType(pInput)) {
    SPartitionLogicNode* pPartition = (SPartitionLogicNode*)pInput;
    pPartition->phasedMode =
        INTERP_MODE_MULTI_GROUP == pInterp->interpMode;
  }
}

static int32_t pdcDealInterp(SOptimizeContext* pCxt,
                             SInterpFuncLogicNode* pInterp) {
  int32_t code = 0, lino = 0;
  if (OPTIMIZE_FLAG_TEST_MASK(pInterp->node.optimizedFlag,
                              OPTIMIZE_FLAG_PUSH_DOWN_CONDE)) {
    return TSDB_CODE_SUCCESS;
  }

  // TODO: remove it after full implementation of pushing down to child
  SScanLogicNode* pScan = pdcInterpFindScan(pInterp, NULL);
  if (NULL == pScan) {
    return TSDB_CODE_SUCCESS;
  }

  bool interpOnPrimaryTs =
      (pInterp->timelineSource == TIME_LINE_SOURCE_PRIMARY_TS);

  // let the scan skip the one ext-window FILL(PREV)/FILL(NEXT) never reads
  // (see interpPruneExtWindows; other modes keep both sides)
  pScan->interpFillMode = pInterp->fillMode;

  // External scan INTERP is handled by the dedicated FqInterp optimizer rule; skip here.
  if (pScan->scanType == SCAN_TYPE_EXTERNAL) {
    OPTIMIZE_FLAG_SET_MASK(pInterp->node.optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE);
    pCxt->optimized = true;
    return TSDB_CODE_SUCCESS;
  }

  if (NULL == pInterp->pTimeRange && interpOnPrimaryTs) {
    pScan->pExtScanRange = taosMemoryMalloc(sizeof(*pScan->pExtScanRange));
    TSDB_CHECK_NULL(pScan->pExtScanRange, code, lino, _exit, terrno);

    pScan->pExtScanRange->skey = pInterp->timeRange.skey;
    pScan->pExtScanRange->ekey = pInterp->timeRange.ekey;
  } else if (interpOnPrimaryTs) {
    TAOS_CHECK_EXIT(nodesCloneNode(pInterp->pTimeRange, &pScan->pExtTimeRange));
  } else {
    /*
     * Only a real primary timestamp timeline may use scan-time range pushdown.
     * Degraded timelines keep the full input so execution-stage validation can
     * see NULL / duplicate / disorder rows on the actual interpolation axis.
     */
  }
  
_exit:

  if (TSDB_CODE_SUCCESS == code) {
    OPTIMIZE_FLAG_SET_MASK(pInterp->node.optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE);
    pCxt->optimized = true;
  } else {
    planError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
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
    case QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN:
      code = pdcDealVirtualTable(pCxt, (SVirtualScanLogicNode*)pLogicNode);
      break;
    case QUERY_NODE_LOGIC_PLAN_INTERP_FUNC:
      code = pdcDealInterp(pCxt, (SInterpFuncLogicNode*)pLogicNode);
      break;
    case QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL:
      code = pdcDealVirtualSuperTableScan(pCxt, (SDynQueryCtrlLogicNode*)pLogicNode);
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
  if (TSDB_CODE_SUCCESS == code &&
      QUERY_NODE_LOGIC_PLAN_INTERP_FUNC == nodeType(pLogicNode)) {
    // Decided after the subtree is pushed down: eligibility reads the
    // child's post-pushdown shape (see pdcInterpInputEligible).
    pdcSetInterpCandidateMode(pCxt, (SInterpFuncLogicNode*)pLogicNode);
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
  // FQ guard: external columns have no NOT NULL guarantee, do not eliminate IS NOT NULL.
  if (SCAN_TYPE_EXTERNAL == pScan->scanType) {
    return false;
  }
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

static bool sortPriKeyOptHasAscPriKeyPrefix(SNodeList* pSortKeys) {
  if (NULL == pSortKeys || LIST_LENGTH(pSortKeys) < 1) {
    return false;
  }

  SOrderByExprNode* pOrderBy = (SOrderByExprNode*)nodesListGetNode(pSortKeys, 0);
  if (ORDER_DESC == pOrderBy->order) {
    return false;
  }

  SNode* pNode = pOrderBy->pExpr;
  return (QUERY_NODE_COLUMN == nodeType(pNode) ? isPrimaryKeyImpl(pNode) : false);
}

static const SLogicNode* fqScanGetPkOrderProofNode(const SLogicNode* pNode) {
  const SLogicNode* pCurr = pNode;

  while (NULL != pCurr && QUERY_NODE_LOGIC_PLAN_PROJECT == nodeType(pCurr) && 1 == LIST_LENGTH(pCurr->pChildren)) {
    pCurr = (const SLogicNode*)nodesListGetNode(pCurr->pChildren, 0);
  }

  return pCurr;
}

static bool fqScanHasProvenPkOrder(const SScanLogicNode* pScan, EOrder* pOrder) {
  if (NULL == pScan || SCAN_TYPE_EXTERNAL != pScan->scanType || NULL == pScan->pRemoteLogicPlan) {
    return false;
  }

  const SLogicNode* pProofNode = fqScanGetPkOrderProofNode((const SLogicNode*)pScan->pRemoteLogicPlan);
  if (NULL == pProofNode || QUERY_NODE_LOGIC_PLAN_SORT != nodeType(pProofNode)) {
    return false;
  }

  const SSortLogicNode* pSort = (const SSortLogicNode*)pProofNode;
  if (!sortPriKeyOptIsPriKeyOrderBy(pSort->pSortKeys)) {
    return false;
  }

  if (NULL != pOrder) {
    *pOrder = ((SOrderByExprNode*)nodesListGetNode(pSort->pSortKeys, 0))->order;
  }
  return true;
}

static bool sortPriKeyOptExternalScanProofReady(const SLogicNode* pNode) {
  if (NULL == pNode) {
    return true;
  }

  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    const SScanLogicNode* pScan = (const SScanLogicNode*)pNode;
    return (SCAN_TYPE_EXTERNAL == pScan->scanType) ? fqScanHasProvenPkOrder(pScan, NULL) : true;
  }

  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    if (!sortPriKeyOptExternalScanProofReady((const SLogicNode*)pChild)) {
      return false;
    }
  }

  return true;
}

static void fqSyncScanOrderFromRemotePkSort(SScanLogicNode* pScan) {
  EOrder order = TSDB_ORDER_NONE;

  if (!fqScanHasProvenPkOrder(pScan, &order)) {
    return;
  }

  pScan->node.outputTsOrder = order;
  pScan->node.resultDataOrder = DATA_ORDER_LEVEL_GLOBAL;
}

static bool sortPriKeyOptExternalWindowReady(const SWindowLogicNode* pWindow, EOrder sortOrder) {
  if (NULL == pWindow || WINDOW_TYPE_EXTERNAL != pWindow->winType) {
    return false;
  }

  // External window can substitute a top-level primary-key Sort only when it
  // already promises global timestamp order on its own output path.
  if (pWindow->needGroupSort || pWindow->node.resultDataOrder < DATA_ORDER_LEVEL_GLOBAL) {
    return false;
  }

  return pWindow->node.outputTsOrder == sortOrder;
}

static bool sortPriKeyOptMayBeOptimized(SLogicNode* pNode, void* pCtx) {
  if (QUERY_NODE_LOGIC_PLAN_SORT != nodeType(pNode)) {
    return false;
  }
  // External scans can participate only after FQ has materialized a remote
  // primary-key Sort and synced that ordering fact back to the docking scan.
  if (nodeHasExternalScan(pNode) && !sortPriKeyOptExternalScanProofReady(pNode)) {
    return false;
  }
  SSortLogicNode* pSort = (SSortLogicNode*)pNode;
  if (pSort->skipPKSortOpt ||
      !sortPriKeyOptIsPriKeyOrderBy(pSort->pSortKeys) || 1 != LIST_LENGTH(pSort->node.pChildren)) {
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

static int32_t sortPriKeyOptHandleLeftRightJoinSort(SJoinLogicNode* pJoin, SSortLogicNode* pSort, bool* pNotOptimize,
                                                    bool* keepSort) {
  if (JOIN_STYPE_SEMI == pJoin->subType || JOIN_STYPE_NONE == pJoin->subType) {
    return TSDB_CODE_SUCCESS;
  }

  SSHashObj* pLeftTables = NULL;
  SSHashObj* pRightTables = NULL;
  bool       sortByProbe = true;
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
    case JOIN_STYPE_OUTER:
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

static int32_t sortPriKeyOptHandleJoinSort(SLogicNode* pNode, bool groupSort, SSortLogicNode* pSort, bool* pNotOptimize,
                                           SNodeList** pSequencingNodes, bool* keepSort) {
  SJoinLogicNode* pJoin = (SJoinLogicNode*)pNode;
  int32_t         code = TSDB_CODE_SUCCESS;

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

  code = sortPriKeyOptGetSequencingNodesImpl((SLogicNode*)nodesListGetNode(pNode->pChildren, 0), groupSort, pSort,
                                             pNotOptimize, pSequencingNodes, keepSort);
  if (TSDB_CODE_SUCCESS == code) {
    code = sortPriKeyOptGetSequencingNodesImpl((SLogicNode*)nodesListGetNode(pNode->pChildren, 1), groupSort, pSort,
                                               pNotOptimize, pSequencingNodes, keepSort);
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
    case QUERY_NODE_LOGIC_PLAN_ANALYSIS_FUNC:
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
    SFunctionNode* pFuncNode = (SFunctionNode*)pNode;
    if (pFuncNode->hasPk &&
        (pFuncNode->funcType == FUNCTION_TYPE_DIFF || pFuncNode->funcType == FUNCTION_TYPE_DERIVATIVE ||
         pFuncNode->funcType == FUNCTION_TYPE_IRATE || pFuncNode->funcType == FUNCTION_TYPE_TWA ||
         pFuncNode->funcType == FUNCTION_TYPE_TWAVG_LINEAR || pFuncNode->funcType == FUNCTION_TYPE_TWAVG_LOCF ||
         pFuncNode->funcType == FUNCTION_TYPE_TWSUM_LINEAR || pFuncNode->funcType == FUNCTION_TYPE_TWSUM_LOCF ||
         pFuncNode->funcType == FUNCTION_TYPE_TWVAR_LINEAR || pFuncNode->funcType == FUNCTION_TYPE_TWVAR_LOCF)) {
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
      // Hint SMALLDATA_SCAN_SORT: this scan must stay a plain Table Scan, so do not
      // let the optimization eliminate the Sort by promoting it to a table merge
      // scan.  Checking here -- at the sequencing scan that would actually be
      // promoted -- keeps the guard scoped to the hinted scan, instead of vetoing
      // the optimization for the whole Sort subtree (e.g. an unrelated join branch).
      if ((!groupSort && NULL != pScan->pGroupTags) ||
          TSDB_SYSTEM_TABLE == pScan->tableType ||
          scanIsSmallDataScanSortHinted(pScan)) {
        *pNotOptimize = true;
        return TSDB_CODE_SUCCESS;
      }
      if (SCAN_TYPE_EXTERNAL == pScan->scanType) {
        EOrder extOrder = TSDB_ORDER_NONE;
        if (!fqScanHasProvenPkOrder(pScan, &extOrder) || pScan->node.outputTsOrder != extOrder ||
            pScan->node.resultDataOrder < DATA_ORDER_LEVEL_GLOBAL || extOrder != sortOrder) {
          *pNotOptimize = true;
          return TSDB_CODE_SUCCESS;
        }
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
      if (pWindowLogicNode->winType == WINDOW_TYPE_EXTERNAL) {
        if (!sortPriKeyOptExternalWindowReady(pWindowLogicNode, sortOrder)) {
          *pNotOptimize = true;
          return TSDB_CODE_SUCCESS;
        }
        return nodesListMakeAppend(pSequencingNodes, (SNode*)pNode);
      }
      // For interval window, we always apply sortPriKey optimization.
      // For session/event/state window, the output ts order will always be ASC.
      // If sort order is also asc, we apply optimization, otherwise we keep sort node to get correct output order.
      if ((pWindowLogicNode->partType & WINDOW_PART_HAS) == 0) {
        if (pWindowLogicNode->winType == WINDOW_TYPE_INTERVAL || sortOrder == ORDER_ASC) {
          return nodesListMakeAppend(pSequencingNodes, (SNode*)pNode);
        }
      }
    }
    case QUERY_NODE_LOGIC_PLAN_AGG:
    case QUERY_NODE_LOGIC_PLAN_PARTITION:
    case QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL:
      *pNotOptimize = true;
      return TSDB_CODE_SUCCESS;
    case QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC:
      if(pNode->outputTsOrder != sortOrder) {
        *pNotOptimize = true;
        return TSDB_CODE_SUCCESS;
      }
      break;
    case QUERY_NODE_LOGIC_PLAN_INTERP_FUNC:
      if (sortOrder == ORDER_DESC) {  // interpolation function currently supports only ascending input order
        *pNotOptimize = true;
        return TSDB_CODE_SUCCESS;
      }
      break;
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

static int32_t sortPriKeyOptGetSequencingNodes(SSortLogicNode* pSort, bool groupSort, SNodeList** pSequencingNodes,
                                               bool* keepSort) {
  bool    notOptimize = false;
  int32_t code = sortPriKeyOptGetSequencingNodesImpl((SLogicNode*)nodesListGetNode(pSort->node.pChildren, 0), groupSort,
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
      if (SCAN_TYPE_EXTERNAL != pScan->scanType &&
          ((ORDER_DESC == order && pScan->scanSeq[0] > 0) || (ORDER_ASC == order && pScan->scanSeq[1] > 0))) {
        TSWAP(pScan->scanSeq[0], pScan->scanSeq[1]);
      }
      pScan->node.outputTsOrder = order;
      if (SCAN_TYPE_EXTERNAL != pScan->scanType && TSDB_SUPER_TABLE == pScan->tableType && !pScan->phTbnameScan) {
        planPromoteScanToTableMerge(pScan, DATA_ORDER_LEVEL_GLOBAL, DATA_ORDER_LEVEL_GLOBAL);
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
  /* Do not eliminate a SORT that carries WHERE conditions or a LIMIT/SLIMIT.
   * Both must be evaluated on the sort's output; removing the SORT would
   * destroy them and silently change query semantics. */
  if (NULL != pSort->node.pConditions ||
      NULL != pSort->node.pLimit || NULL != pSort->node.pSlimit) {
    pSort->skipPKSortOpt = true;
    optSetParentOrder(pSort->node.pParent, sortPriKeyOptGetPriKeyOrder(pSort), NULL);
    pCxt->optimized = true;
    return TSDB_CODE_SUCCESS;
  }

  SNodeList* pSequencingNodes = NULL;
  bool       keepSort = true;
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
      optSetParentOrder(pSort->node.pParent, sortPriKeyOptGetPriKeyOrder(pSort), NULL);
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

static const char* getJoinCondPKTable(SNode* pNode) {
  if (!pNode) {
    return NULL;
  }
  ENodeType type = nodeType(pNode);

  if (QUERY_NODE_COLUMN == type) {
    return ((SColumnNode*)pNode)->tableAlias;
  } else if (QUERY_NODE_FUNCTION == type) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    if (pFunc->funcType != FUNCTION_TYPE_TIMETRUNCATE || LIST_LENGTH(pFunc->pParameterList) == 0) {
      return NULL;
    }
    SNode* pParam = nodesListGetNode(pFunc->pParameterList, 0);
    if (QUERY_NODE_COLUMN != nodeType(pParam)) {
      return NULL;
    }
    return ((SColumnNode*)pParam)->tableAlias;
  } else if (QUERY_NODE_VALUE == type) {
    return ((SValueNode*)pNode)->node.srcTable;
  }

  return NULL;
}

static int32_t sortForJoinOptimizeImpl(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan, SJoinLogicNode* pJoin) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SLogicNode*     pLeft = (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0);
  SLogicNode*     pRight = (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1);
  SScanLogicNode* pScan = NULL;
  SLogicNode*     pChild = NULL;
  SNode**         pChildPos = NULL;
  EOrder          targetOrder = 0;
  SSHashObj*      pTables = NULL;

  if (pJoin->node.inputTsOrder) {
    targetOrder = pJoin->node.inputTsOrder;

    if (pRight->outputTsOrder == pJoin->node.inputTsOrder) {
      pChild = pLeft;
      pChildPos = &pJoin->node.pChildren->pHead->pNode;
    } else if (pLeft->outputTsOrder == pJoin->node.inputTsOrder) {
      pChild = pRight;
      pChildPos = &pJoin->node.pChildren->pTail->pNode;
    } else {
      pChild = pRight;
      pChildPos = &pJoin->node.pChildren->pTail->pNode;
      targetOrder = pLeft->outputTsOrder;
    }
  } else {
    if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pLeft) &&
        !(((SScanLogicNode*)pLeft)->scanSeq[0] && ((SScanLogicNode*)pLeft)->scanSeq[1])){
      pScan = (SScanLogicNode*)pLeft;
      pChild = pRight;
      pChildPos = &pJoin->node.pChildren->pTail->pNode;
      targetOrder = pScan->node.outputTsOrder;
    } else if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pRight) &&
        !(((SScanLogicNode*)pRight)->scanSeq[0] && ((SScanLogicNode*)pRight)->scanSeq[1])) {
      pScan = (SScanLogicNode*)pRight;
      pChild = pLeft;
      pChildPos = &pJoin->node.pChildren->pHead->pNode;
      targetOrder = pScan->node.outputTsOrder;
    } else {
      pChild = pRight;
      pChildPos = &pJoin->node.pChildren->pTail->pNode;
      targetOrder = pLeft->outputTsOrder;
    }
    pJoin->node.inputTsOrder = targetOrder;
  }

  if (QUERY_NODE_OPERATOR != nodeType(pJoin->pPrimKeyEqCond)) {
    code = TSDB_CODE_PLAN_INTERNAL_ERROR;
    QUERY_CHECK_CODE(code, lino, _return);
  }

  bool           res = false;
  SOperatorNode* pOp = (SOperatorNode*)pJoin->pPrimKeyEqCond;

  if ((QUERY_NODE_COLUMN != nodeType(pOp->pLeft) && QUERY_NODE_VALUE != nodeType(pOp->pLeft) &&
       QUERY_NODE_FUNCTION != nodeType(pOp->pLeft)) ||
      (QUERY_NODE_COLUMN != nodeType(pOp->pRight) && QUERY_NODE_VALUE != nodeType(pOp->pRight) &&
       QUERY_NODE_FUNCTION != nodeType(pOp->pRight))) {
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  SNode* pOrderByNode = NULL;

  code = collectTableAliasFromNodes((SNode*)pChild, &pTables);
  QUERY_CHECK_CODE(code, lino, _return);

  const char* opLeftTable = getJoinCondPKTable(pOp->pLeft);
  const char* opRightTable = getJoinCondPKTable(pOp->pRight);

  if (!opLeftTable || !opRightTable) {
    code = TSDB_CODE_PLAN_INTERNAL_ERROR;
    QUERY_CHECK_CODE(code, lino, _return);
  }

  if (NULL != tSimpleHashGet(pTables, opLeftTable, strlen(opLeftTable))) {
    pOrderByNode = pOp->pLeft;
  } else if (NULL != tSimpleHashGet(pTables, opRightTable, strlen(opRightTable))) {
    pOrderByNode = pOp->pRight;
  }

  tSimpleHashCleanup(pTables);

  if (NULL == pOrderByNode) {
    code = TSDB_CODE_PLAN_INTERNAL_ERROR;
    QUERY_CHECK_CODE(code, lino, _return);
  }

  SSortLogicNode* pSort = NULL;
  code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SORT, (SNode**)&pSort);
  if (NULL == pSort) {
    QUERY_CHECK_CODE(code, lino, _return);
  }

  pSort->node.outputTsOrder = targetOrder;
  pSort->node.pTargets = NULL;
  code = nodesCloneList(pChild->pTargets, &pSort->node.pTargets);
  if (NULL == pSort->node.pTargets) {
    nodesDestroyNode((SNode*)pSort);
    QUERY_CHECK_CODE(code, lino, _return);
  }

  pSort->groupSort = false;
  SOrderByExprNode* pOrder = NULL;
  code = nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR, (SNode**)&pOrder);
  if (NULL == pOrder) {
    nodesDestroyNode((SNode*)pSort);
    QUERY_CHECK_CODE(code, lino, _return);
  }

  code = nodesListMakeStrictAppend(&pSort->pSortKeys, (SNode*)pOrder);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pSort);
    QUERY_CHECK_CODE(code, lino, _return);
  }
  pOrder->order = targetOrder;
  pOrder->pExpr = NULL;
  pOrder->nullOrder = (ORDER_ASC == pOrder->order) ? NULL_ORDER_FIRST : NULL_ORDER_LAST;
  code = nodesCloneNode(pOrderByNode, &pOrder->pExpr);
  if (!pOrder->pExpr) {
    nodesDestroyNode((SNode*)pSort);
    QUERY_CHECK_CODE(code, lino, _return);
  }

  pChild->pParent = (SLogicNode*)pSort;
  code = nodesListMakeAppend(&pSort->node.pChildren, (SNode*)pChild);
  if (TSDB_CODE_SUCCESS != code) {
    QUERY_CHECK_CODE(code, lino, _return);
  }
  *pChildPos = (SNode*)pSort;
  pSort->node.pParent = (SLogicNode*)pJoin;

_return:
  if (TSDB_CODE_SUCCESS == code) {
    pCxt->optimized = true;
  } else {
    planError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
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

  if (pJoin->leftConstPrimGot) {
    pJoin->node.inputTsOrder = pRight->outputTsOrder;
    return false;
  } else if (pJoin->rightConstPrimGot) {
    pJoin->node.inputTsOrder = pLeft->outputTsOrder;
    return false;
  }

  if (pLeft->outputTsOrder == pRight->outputTsOrder) {
    return false;
  }

  return true;
}

static int32_t sortForJoinOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SJoinLogicNode* pJoin =
      (SJoinLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, sortForJoinOptMayBeOptimized, NULL);
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
  if (QUERY_NODE_LOGIC_PLAN_JOIN != nodeType(pNode) ||
      OPTIMIZE_FLAG_TEST_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_JOIN_COND)) {
    return false;
  }

  SJoinLogicNode* pJoin = (SJoinLogicNode*)pNode;
  if (JOIN_STYPE_ASOF == pJoin->subType) {
    // ASOF joins are handled by AsofJoinCondOptimize, which shares this flag.
    return false;
  }

  if (pNode->pChildren->length != 2 || JOIN_STYPE_WIN == pJoin->subType || JOIN_TYPE_FULL == pJoin->joinType) {
    OPTIMIZE_FLAG_SET_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_JOIN_COND);
    return false;
  }

  SLogicNode* pLeft = (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0);
  SLogicNode* pRight = (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1);

  if ((JOIN_TYPE_LEFT == pJoin->joinType || JOIN_TYPE_RIGHT == pJoin->joinType) &&
      (QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pLeft) || QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pRight))) {
    OPTIMIZE_FLAG_SET_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_JOIN_COND);
    return false;
  }

  if (JOIN_TYPE_INNER == pJoin->joinType &&
      ((QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pLeft) && QUERY_NODE_LOGIC_PLAN_JOIN != nodeType(pLeft)) ||
       (QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pRight) && QUERY_NODE_LOGIC_PLAN_JOIN != nodeType(pRight)))) {
    OPTIMIZE_FLAG_SET_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_JOIN_COND);
    return false;
  }

  if (JOIN_TYPE_INNER == pJoin->joinType) {
    if (QUERY_NODE_LOGIC_PLAN_JOIN == nodeType(pLeft) &&
        !OPTIMIZE_FLAG_TEST_MASK(pLeft->optimizedFlag, OPTIMIZE_FLAG_JOIN_COND)) {
      return false;
    }
    if (QUERY_NODE_LOGIC_PLAN_JOIN == nodeType(pRight) &&
        !OPTIMIZE_FLAG_TEST_MASK(pRight->optimizedFlag, OPTIMIZE_FLAG_JOIN_COND)) {
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

  if (!joinCondHasValidPrimKeyCond(pJoin)) {
    OPTIMIZE_FLAG_SET_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_JOIN_COND);
    return false;
  }
  // FQ guard (defensive): if any participating SCAN is an ExternalScan, skip this
  // rule. FqPushdown runs earlier in optimizeRuleSet[] and will normally have
  // stripped the FQ subplan before joinCondOptimize executes, but a mixed
  // local+external JOIN that survives FqPushdown must not have its scanRange
  // overwritten here (ExternalScan pushes time filters via pRemoteLogicPlan,
  // not via SScanLogicNode::scanRange).
  if (SCAN_TYPE_EXTERNAL == pLScan->scanType || SCAN_TYPE_EXTERNAL == pRScan->scanType) {
    OPTIMIZE_FLAG_SET_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_JOIN_COND);
    return false;
  }
  if (JOIN_TYPE_INNER == pJoin->joinType) {
    SNodeList* pAllScans = NULL;
    if (TSDB_CODE_SUCCESS == joinCondGetAllScanNodes((SLogicNode*)pJoin, &pAllScans)) {
      bool   hasExt = false;
      SNode* pSc = NULL;
      FOREACH(pSc, pAllScans) {
        if (SCAN_TYPE_EXTERNAL == ((SScanLogicNode*)pSc)->scanType) {
          hasExt = true;
          break;
        }
      }
      nodesClearList(pAllScans);
      if (hasExt) {
        OPTIMIZE_FLAG_SET_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_JOIN_COND);
        return false;
      }
    }
  }

  if (pJoin->pPrimKeyEqCond && QUERY_NODE_OPERATOR == nodeType(pJoin->pPrimKeyEqCond)) {
    SOperatorNode* pOp = (SOperatorNode*)pJoin->pPrimKeyEqCond;
    if ((pOp->pLeft && QUERY_NODE_COLUMN != nodeType(pOp->pLeft)) || (pOp->pRight && QUERY_NODE_COLUMN != nodeType(pOp->pRight))) {
      return false;
    }
  }

  return true;
}

static bool joinCondHasValidPrimKeyCond(SJoinLogicNode* pJoin) {
  if (pJoin->pPrimKeyEqCond && QUERY_NODE_OPERATOR == nodeType(pJoin->pPrimKeyEqCond)) {
    SOperatorNode* pOp = (SOperatorNode*)pJoin->pPrimKeyEqCond;
    if ((pOp->pLeft && QUERY_NODE_COLUMN != nodeType(pOp->pLeft)) ||
        (pOp->pRight && QUERY_NODE_COLUMN != nodeType(pOp->pRight))) {
      return false;
    }
  }

  return true;
}

static void joinCondMergeScanRange(STimeWindow* pDst, STimeWindow* pSrc) {
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

  // TODO(smj) : optimize pTimeRange for join
  switch (pJoin->joinType) {
    case JOIN_TYPE_INNER: {
      SNodeList* pScanList = NULL;
      int32_t    code = joinCondGetAllScanNodes((SLogicNode*)pJoin, &pScanList);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      if (NULL == pScanList || pScanList->length <= 0) {
        nodesClearList(pScanList);
        return TSDB_CODE_SUCCESS;
      }
      SNode*      pNode = NULL;
      STimeWindow scanRange = TSWINDOW_INITIALIZER;
      FOREACH(pNode, pScanList) { joinCondMergeScanRange(&scanRange, &((SScanLogicNode*)pNode)->scanRange); }
      FOREACH(pNode, pScanList) {
        ((SScanLogicNode*)pNode)->scanRange.skey = scanRange.skey;
        ((SScanLogicNode*)pNode)->scanRange.ekey = scanRange.ekey;
      }
      nodesClearList(pScanList);
      break;
    }
    case JOIN_TYPE_LEFT: {
      SLogicNode*     pLeft = (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0);
      SLogicNode*     pRight = (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1);
      SScanLogicNode* pLScan = joinCondGetScanNode(pLeft);
      SScanLogicNode* pRScan = joinCondGetScanNode(pRight);

      if (NULL == pLScan || NULL == pRScan) {
        return TSDB_CODE_SUCCESS;
      }
      joinCondMergeScanRange(&pRScan->scanRange, &pLScan->scanRange);
      break;
    }
    case JOIN_TYPE_RIGHT: {
      SLogicNode*     pLeft = (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0);
      SLogicNode*     pRight = (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1);
      SScanLogicNode* pLScan = joinCondGetScanNode(pLeft);
      SScanLogicNode* pRScan = joinCondGetScanNode(pRight);

      if (NULL == pLScan || NULL == pRScan) {
        return TSDB_CODE_SUCCESS;
      }
      joinCondMergeScanRange(&pLScan->scanRange, &pRScan->scanRange);
      break;
    }
    default:
      return TSDB_CODE_SUCCESS;
  }

  OPTIMIZE_FLAG_SET_MASK(pJoin->node.optimizedFlag, OPTIMIZE_FLAG_JOIN_COND);

  pCxt->optimized = true;

  return TSDB_CODE_SUCCESS;
}
static bool asofJoinCondMayBeOptimized(SLogicNode* pNode, void* pCtx) {
  if (QUERY_NODE_LOGIC_PLAN_JOIN != nodeType(pNode) ||
      OPTIMIZE_FLAG_TEST_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_JOIN_COND)) {
    return false;
  }

  SJoinLogicNode* pJoin = (SJoinLogicNode*)pNode;
  if (pNode->pChildren->length != 2 || JOIN_STYPE_ASOF != pJoin->subType || JOIN_TYPE_FULL == pJoin->joinType) {
    return false;
  }

  if (JOIN_TYPE_LEFT != pJoin->joinType && JOIN_TYPE_RIGHT != pJoin->joinType) {
    OPTIMIZE_FLAG_SET_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_JOIN_COND);
    return false;
  }

  SLogicNode*     pLeft = (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0);
  SLogicNode*     pRight = (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1);
  SScanLogicNode* pLScan = joinCondGetScanNode(pLeft);
  SScanLogicNode* pRScan = joinCondGetScanNode(pRight);

  if (NULL == pLScan || NULL == pRScan || SCAN_TYPE_TABLE != pLScan->scanType ||
      SCAN_TYPE_TABLE != pRScan->scanType || !joinCondHasValidPrimKeyCond(pJoin)) {
    OPTIMIZE_FLAG_SET_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_JOIN_COND);
    return false;
  }

  STimeWindow* pProbeRange = (JOIN_TYPE_LEFT == pJoin->joinType) ? &pLScan->scanRange : &pRScan->scanRange;
  bool         canDerive = false;

  if (asofJoinCanDeriveMatchedLowerBound(pJoin) && pProbeRange->skey != TSKEY_MIN) {
    canDerive = true;
  }
  if (asofJoinCanDeriveMatchedUpperBound(pJoin) && pProbeRange->ekey != TSKEY_MAX) {
    canDerive = true;
  }

  if (!canDerive) {
    OPTIMIZE_FLAG_SET_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_JOIN_COND);
  }

  return canDerive;
}

static bool asofJoinDeriveMatchedScanRange(SJoinLogicNode* pJoin, SScanLogicNode* pLScan, SScanLogicNode* pRScan) {
  STimeWindow* pProbeRange = NULL;
  STimeWindow* pMatchedRange = NULL;

  if (JOIN_TYPE_LEFT == pJoin->joinType) {
    pProbeRange = &pLScan->scanRange;
    pMatchedRange = &pRScan->scanRange;
  } else if (JOIN_TYPE_RIGHT == pJoin->joinType) {
    pProbeRange = &pRScan->scanRange;
    pMatchedRange = &pLScan->scanRange;
  } else {
    return false;
  }

  bool    updated = false;
  int64_t oldSKey = pMatchedRange->skey;
  int64_t oldEKey = pMatchedRange->ekey;

  if (asofJoinCanDeriveMatchedUpperBound(pJoin) && pProbeRange->ekey != TSKEY_MAX &&
      pProbeRange->ekey < pMatchedRange->ekey) {
    pMatchedRange->ekey = pProbeRange->ekey;
    updated = true;
  }
  if (asofJoinCanDeriveMatchedLowerBound(pJoin) && pProbeRange->skey != TSKEY_MIN &&
      pProbeRange->skey > pMatchedRange->skey) {
    pMatchedRange->skey = pProbeRange->skey;
    updated = true;
  }

  if (pMatchedRange->skey > pMatchedRange->ekey) {
    *pMatchedRange = TSWINDOW_DESC_INITIALIZER;
    updated = true;
  }

  return updated || oldSKey != pMatchedRange->skey || oldEKey != pMatchedRange->ekey;
}

static int32_t asofJoinCondOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  while (true) {
    SJoinLogicNode* pJoin =
        (SJoinLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, asofJoinCondMayBeOptimized, NULL);
    if (NULL == pJoin) {
      return TSDB_CODE_SUCCESS;
    }

    SLogicNode*     pLeft = (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0);
    SLogicNode*     pRight = (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1);
    SScanLogicNode* pLScan = joinCondGetScanNode(pLeft);
    SScanLogicNode* pRScan = joinCondGetScanNode(pRight);
    if (NULL == pLScan || NULL == pRScan) {
      OPTIMIZE_FLAG_SET_MASK(pJoin->node.optimizedFlag, OPTIMIZE_FLAG_JOIN_COND);
      continue;
    }

    OPTIMIZE_FLAG_SET_MASK(pJoin->node.optimizedFlag, OPTIMIZE_FLAG_JOIN_COND);
    if (!asofJoinDeriveMatchedScanRange(pJoin, pLScan, pRScan)) {
      continue;
    }

    pCxt->optimized = true;
    break;
  }

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
  int32_t         code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SCAN, (SNode**)&pSmaScan);
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

static bool smaIndexOptEqualInterval(SScanLogicNode* pScan, SWindowLogicNode* pWindow, STableIndexInfo* pIndex, void* tz) {
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
                          .timezone = tz,
                          .precision = pScan->node.precision};
    return (pScan->scanRange.skey == taosTimeTruncate(pScan->scanRange.skey, &interval)) &&
           (pScan->scanRange.ekey + 1 == taosTimeTruncate(pScan->scanRange.ekey + 1, &interval));
  }
  return true;
}

static int32_t smaIndexOptCreateSmaCol(SNode* pFunc, uint64_t tableId, int32_t colId, SColumnNode** ppNode) {
  SColumnNode* pCol = NULL;
  int32_t      code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  if (NULL == pCol) {
    return code;
  }
  pCol->tableId = tableId;
  pCol->tableType = TSDB_SUPER_TABLE;
  pCol->colId = colId;
  pCol->colType = COLUMN_TYPE_COLUMN;
  tstrncpy(pCol->colName, ((SExprNode*)pFunc)->aliasName, TSDB_COL_NAME_LEN);
  pCol->node.resType = ((SExprNode*)pFunc)->resType;
  tstrncpy(pCol->node.aliasName, ((SExprNode*)pFunc)->aliasName, TSDB_COL_NAME_LEN);
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
      rewriteExprAliasName(&exprNode, index + 1);
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

static int32_t smaIndexOptCouldApplyIndex(SScanLogicNode* pScan, STableIndexInfo* pIndex, SNodeList** pCols, void* tz) {
  SWindowLogicNode* pWindow = (SWindowLogicNode*)pScan->node.pParent;
  if (!smaIndexOptEqualInterval(pScan, pWindow, pIndex, tz)) {
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
    code = smaIndexOptCouldApplyIndex(pScan, pIndex, &pSmaCols, pCxt->pPlanCxt->timezone);
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

static bool isImplicitPartitionKeyList(SNodeList* pKeys) {
  if (1 != LIST_LENGTH(pKeys)) {
    return false;
  }
  // The translator removes user-written constant keys before planning, so a
  // value key with this reserved alias can only come from implicit HAVING.
  SNode* pKey = nodesListGetNode(pKeys, 0);
  return QUERY_NODE_VALUE == nodeType(pKey) &&
         0 == strcmp(((SExprNode*)pKey)->aliasName,
                     IMPLICIT_PARTITION_KEY_ALIAS);
}

static bool partTagsIsOptimizableNode(SLogicNode* pNode) {
  bool ret = 1 == LIST_LENGTH(pNode->pChildren) &&
             QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(nodesListGetNode(pNode->pChildren, 0)) &&
             SCAN_TYPE_TAG != ((SScanLogicNode*)nodesListGetNode(pNode->pChildren, 0))->scanType;
  if (!ret) return ret;
  // FQ guard: external scan has no tag concept, tag-related optimization is not
  // applicable.
  // This rejection is also what keeps partTagsOptimize() from dropping a
  // filter: an external subtree is the only case where conditions stay on the
  // node instead of being pushed into the scan. See the note there before
  // relaxing this.
  if (SCAN_TYPE_EXTERNAL == ((SScanLogicNode*)nodesListGetNode(pNode->pChildren, 0))->scanType) {
    return false;
  }
  switch (nodeType(pNode)) {
    case QUERY_NODE_LOGIC_PLAN_PARTITION: {
      SPartitionLogicNode* pPartition = (SPartitionLogicNode*)pNode;
      if (isImplicitPartitionKeyList(pPartition->pPartitionKeys) &&
          pNode->pParent &&
          QUERY_NODE_LOGIC_PLAN_AGG == nodeType(pNode->pParent)) {
        SAggLogicNode* pAgg = (SAggLogicNode*)pNode->pParent;
        if (pAgg->hasLast || planAggFuncsHasFirst(pAgg->pAggFuncs)) {
          return false;
        }
      }
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

static bool partTagsOptIsPartitionFirst(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_PARTITION != nodeType(pNode)) {
    return false;
  }

  SLogicNode* pParent = pNode->pParent;
  if (NULL == pParent || QUERY_NODE_LOGIC_PLAN_WINDOW != nodeType(pParent) ||
      WINDOW_TYPE_STATE != ((SWindowLogicNode*)pParent)->winType) {
    return false;
  }

  SLogicNode* pRoot = getLogicNodeRootNode(pNode);
  return NULL != pRoot && getOptHint(pRoot->pHint, HINT_PARTITION_FIRST);
}

static bool partTagsOptShouldPreservePartitionOrder(SLogicNode* pNode) {
  if (!keysHasCol(partTagsGetPartKeys(pNode))) {
    return partTagsOptIsPartitionFirst(pNode);
  }

  // A data-column Partition remains explicit in both plan variants. STATE_WINDOW
  // requires its known scan order for deterministic distributed merging.
  SLogicNode* pParent = pNode->pParent;
  return NULL != pParent && QUERY_NODE_LOGIC_PLAN_WINDOW == nodeType(pParent) &&
         WINDOW_TYPE_STATE == ((SWindowLogicNode*)pParent)->winType;
}

static void partTagsOptPreservePartitionOrder(SLogicNode* pNode) {
  if (partTagsIsOptimizableNode(pNode) && partTagsOptShouldPreservePartitionOrder(pNode)) {
    SLogicNode* pScan = (SLogicNode*)nodesListGetNode(pNode->pChildren, 0);
    if (ORDER_ASC == pScan->outputTsOrder || ORDER_DESC == pScan->outputTsOrder) {
      pNode->inputTsOrder = pScan->outputTsOrder;
      pNode->outputTsOrder = pScan->outputTsOrder;
      if (NULL != pNode->pParent) {
        pNode->pParent->inputTsOrder = pNode->outputTsOrder;
      }
    }
  }

  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) { partTagsOptPreservePartitionOrder((SLogicNode*)pChild); }
}

static bool partTagsOptCanSplitInterpPartition(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_PARTITION != nodeType(pNode) ||
      NULL == pNode->pParent ||
      QUERY_NODE_LOGIC_PLAN_INTERP_FUNC != nodeType(pNode->pParent)) {
    return false;
  }

  SPartitionLogicNode* pPartition = (SPartitionLogicNode*)pNode;
  SInterpFuncLogicNode* pInterp =
      (SInterpFuncLogicNode*)pNode->pParent;
  if (!pPartition->phasedMode ||
      INTERP_MODE_MULTI_GROUP != pInterp->interpMode) {
    return false;
  }

  // PartitionCols runs after PartitionTags in optimizeRuleSet[]. Under
  // sort_for_group() it replaces a column-keyed Partition with a Sort and
  // destroys the node, taking the pre-split rollback state with it. The
  // post-split check would then find no Partition to commit or roll back,
  // leaving the static keys stranded in the scan group tags while the Sort
  // computes group ids from the column keys alone. Never start the split
  // when that rule can fire.
  SLogicNode* pRoot = getLogicNodeRootNode(pNode);
  if (NULL != pRoot && getSortForGroupOptHint(pRoot->pHint)) {
    return false;
  }

  bool hasColumnKey = false;
  bool hasStaticKey = false;
  SNode* pKey = NULL;
  FOREACH(pKey, pPartition->pPartitionKeys) {
    bool hasColumn = partitionKeyHasOrdinaryColumn(pKey);
    bool hasStatic = partitionKeyHasTagOrTbname(pKey);
    if (hasColumn == hasStatic) {
      return false;
    }
    hasColumnKey |= hasColumn;
    hasStaticKey |= hasStatic;
  }
  return hasColumnKey && hasStaticKey;
}

static bool partTagsOptMayBeOptimized(SLogicNode* pNode, void* pCtx) {
  if (!partTagsIsOptimizableNode(pNode)) {
    return false;
  }

  if (partTagsOptIsPartitionFirst(pNode)) {
    return false;
  }

  return partTagsOptCanSplitInterpPartition(pNode) ||
         (!keysHasCol(partTagsGetPartKeys(pNode)) &&
          partTagsOptAreSupportedFuncs(partTagsGetFuncs(pNode)));
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

  (void)taosHashBinary(name, len, sizeof(name));
  tstrncpy(pAlias, name, TSDB_COL_NAME_LEN);
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
       ((SColumnNode*)pNode)->tableAlias[0] != '\0')) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    partTagsSetAlias(pFunc->node.aliasName, pCol->tableAlias, pCol->colName);
  } else {
    tstrncpy(pFunc->node.aliasName, ((SExprNode*)pNode)->aliasName, TSDB_COL_NAME_LEN);
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

typedef struct SPartTagsExprConsumerCxt {
  const SNode* pExpr;
  bool         found;
} SPartTagsExprConsumerCxt;

static void partTagsDebugLogConsumerChain(const SAggLogicNode* pAgg);

static EDealRes partTagsFindExprConsumer(SNode* pNode, void* pContext) {
  SPartTagsExprConsumerCxt* pCxt = pContext;
  if (NULL == pNode || NULL == pCxt || NULL == pCxt->pExpr) {
    return DEAL_RES_CONTINUE;
  }

  if (nodesEqualNode((SNode*)pCxt->pExpr, pNode)) {
    pCxt->found = true;
    return DEAL_RES_END;
  }

  return DEAL_RES_CONTINUE;
}

static bool partTagsExprReferencedInExpr(SNode* pExpr, SNode* pRoot) {
  if (NULL == pExpr || NULL == pRoot) {
    return false;
  }

  SPartTagsExprConsumerCxt cxt = {.pExpr = pExpr, .found = false};
  nodesWalkExpr(pRoot, partTagsFindExprConsumer, &cxt);
  return cxt.found;
}

static bool partTagsExprReferencedInExprs(SNode* pExpr, SNodeList* pExprs) {
  if (NULL == pExpr || NULL == pExprs) {
    return false;
  }

  SPartTagsExprConsumerCxt cxt = {.pExpr = pExpr, .found = false};
  nodesWalkExprs(pExprs, partTagsFindExprConsumer, &cxt);
  return cxt.found;
}

static bool partTagsNodeConsumesExpr(SNode* pExpr, const SLogicNode* pNode, bool* pStop) {
  if (NULL != pStop) {
    *pStop = false;
  }

  if (NULL == pExpr || NULL == pNode) {
    return false;
  }

  if (partTagsExprReferencedInExpr(pExpr, pNode->pConditions)) {
    return true;
  }

  switch (nodeType((SNode*)pNode)) {
    case QUERY_NODE_LOGIC_PLAN_PROJECT:
      if (NULL != pStop) {
        *pStop = true;
      }
      return partTagsExprReferencedInExprs(pExpr, ((const SProjectLogicNode*)pNode)->pProjections);
    case QUERY_NODE_LOGIC_PLAN_SORT:
      return partTagsExprReferencedInExprs(pExpr, ((const SSortLogicNode*)pNode)->pSortKeys);
    default:
      return false;
  }
}

static bool partTagsHasRealConsumer(const SAggLogicNode* pAgg, SNode* pExpr) {
  if (NULL == pAgg || NULL == pExpr) {
    return false;
  }

  if (partTagsNeedOutput(pExpr, pAgg->node.pTargets)) {
    return true;
  }

  if (partTagsExprReferencedInExpr(pExpr, pAgg->node.pConditions)) {
    return true;
  }

  const SLogicNode* pCurr = pAgg->node.pParent;
  while (NULL != pCurr) {
    bool stop = false;
    if (partTagsNodeConsumesExpr(pExpr, pCurr, &stop)) {
      return true;
    }
    if (stop) {
      return false;
    }
    pCurr = pCurr->pParent;
  }

  return false;
}

static int32_t partTagsRewriteGroupTagsToFuncs(SNodeList* pGroupTags, int32_t start, SAggLogicNode* pAgg,
                                               bool appendWrapperTarget) {
  bool    hasIndefRowsSelectFunc = partTagsHasIndefRowsSelectFunc(pAgg->pAggFuncs);
  partTagsDebugLogConsumerChain(pAgg);
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t index = 0;
  SNode*  pNode = NULL;
  FOREACH(pNode, pGroupTags) {
    int32_t currIndex = index++;
    bool    needOutput = partTagsHasRealConsumer(pAgg, pNode);
    planDebug("FQ-partTags: groupTag[%d] nodeType=%d needOutput=%d", currIndex, (int)nodeType(pNode), needOutput);
    if (currIndex < start || !needOutput) {
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
    if (TSDB_CODE_SUCCESS == code && appendWrapperTarget) {
      code = createColumnByRewriteExpr((SNode*)pFunc, &pAgg->node.pTargets);
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }
  return code;
}

static bool tbCntScanOptIsSameSysTableCol(SNode* pLeft, SNode* pRight) {
  if (QUERY_NODE_COLUMN != nodeType(pLeft) || QUERY_NODE_COLUMN != nodeType(pRight)) {
    return false;
  }

  SColumnNode* pLeftCol = (SColumnNode*)pLeft;
  SColumnNode* pRightCol = (SColumnNode*)pRight;
  return 0 == strcmp(pLeftCol->colName, pRightCol->colName) &&
         0 == strcmp(pLeftCol->dbName, pRightCol->dbName) &&
         0 == strcmp(pLeftCol->tableName, pRightCol->tableName);
}

static bool tbCntScanOptIsGroupTagFunc(SFunctionNode* pFunc) {
  return 0 == strcmp(pFunc->functionName, "_group_key") || 0 == strcmp(pFunc->functionName, "_select_value");
}

static bool tbCntScanOptFuncContainsGroupTag(SFunctionNode* pFunc, SNode* pGroupTag) {
  if (!tbCntScanOptIsGroupTagFunc(pFunc)) {
    return false;
  }

  SNode* pParam = NULL;
  FOREACH(pParam, pFunc->pParameterList) {
    if (nodesEqualNode(pGroupTag, pParam) || tbCntScanOptIsSameSysTableCol(pGroupTag, pParam)) {
      return true;
    }
  }
  return false;
}

static SFunctionNode* tbCntScanOptFindGroupTagFunc(SNode* pGroupTag, SNodeList* pOrigAggFuncs) {
  SNode* pFunc = NULL;
  FOREACH(pFunc, pOrigAggFuncs) {
    if (QUERY_NODE_FUNCTION == nodeType(pFunc) && tbCntScanOptFuncContainsGroupTag((SFunctionNode*)pFunc, pGroupTag)) {
      return (SFunctionNode*)pFunc;
    }
  }
  return NULL;
}

static bool tbCntScanOptNeedGroupTagOutput(SNode* pGroupTag, SAggLogicNode* pAgg, SNodeList* pOrigAggFuncs) {
  if (partTagsNeedOutput(pGroupTag, pAgg->node.pTargets)) {
    return true;
  }

  return NULL != tbCntScanOptFindGroupTagFunc(pGroupTag, pOrigAggFuncs);
}

static int32_t tbCntScanOptRewriteGroupTagsToFuncs(SNodeList* pGroupTags, SAggLogicNode* pAgg, SNodeList* pOrigAggFuncs) {
  bool    hasIndefRowsSelectFunc = partTagsHasIndefRowsSelectFunc(pOrigAggFuncs);
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pNode = NULL;
  FOREACH(pNode, pGroupTags) {
    if (!tbCntScanOptNeedGroupTagOutput(pNode, pAgg, pOrigAggFuncs)) {
      continue;
    }

    SFunctionNode* pFunc = NULL;
    SFunctionNode* pOrigFunc = tbCntScanOptFindGroupTagFunc(pNode, pOrigAggFuncs);
    code = partTagsCreateWrapperFunc(hasIndefRowsSelectFunc ? "_select_value" : "_group_key", pNode, &pFunc);
    if (TSDB_CODE_SUCCESS == code && NULL != pOrigFunc) {
      tstrncpy(pFunc->node.aliasName, pOrigFunc->node.aliasName, TSDB_COL_NAME_LEN);
      tstrncpy(pFunc->node.userAlias, pOrigFunc->node.userAlias, TSDB_COL_NAME_LEN);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListStrictAppend(pAgg->pAggFuncs, (SNode*)pFunc);
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }
  return code;
}

static EDealRes partTagsCollectColsNodes(SNode* pNode, void* pContext) {
  SCollectColsCxt* pCxt = pContext;
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    if (NULL == taosHashGet(pCxt->pColHash, pCol->colName, strlen(pCol->colName))) {
      pCxt->errCode = taosHashPut(pCxt->pColHash, pCol->colName, strlen(pCol->colName), NULL, 0);
    }
  }

  return (TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_CONTINUE : DEAL_RES_ERROR);
}


static bool partTagsIsScanPseudoColsInConds(SScanLogicNode* pScan) {
  SCollectColsCxt cxt = {
      .errCode = TSDB_CODE_SUCCESS,
      .pColHash = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK)};
  if (NULL == cxt.pColHash) {
    return true;
  }

  nodesWalkExpr(pScan->node.pConditions, partTagsCollectColsNodes, &cxt);
  if (cxt.errCode) {
    taosHashCleanup(cxt.pColHash);
    return true;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pScan->pScanPseudoCols) {
    if (taosHashGet(cxt.pColHash, ((SExprNode*)pNode)->aliasName, strlen(((SExprNode*)pNode)->aliasName))) {
      taosHashCleanup(cxt.pColHash);
      return true;
    }
  }

  taosHashCleanup(cxt.pColHash);
  return false;
}

static int32_t partTagsOptRemovePseudoCols(SScanLogicNode* pScan) {
  if (!pScan->noPseudoRefAfterGrp || NULL == pScan->pScanPseudoCols || pScan->pScanPseudoCols->length <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  if (pScan->node.pConditions && partTagsIsScanPseudoColsInConds(pScan)) {
    return TSDB_CODE_SUCCESS;
  }

  SNode* pNode = NULL, *pTarget = NULL;
  FOREACH(pNode, pScan->pScanPseudoCols) {
    FOREACH(pTarget, pScan->node.pTargets) {
      if (0 == strcmp(((SExprNode*)pNode)->aliasName, ((SColumnNode*)pTarget)->colName)) {
        ERASE_NODE(pScan->node.pTargets);
        break;
      }
    }
  }
  
  nodesDestroyList(pScan->pScanPseudoCols);
  pScan->pScanPseudoCols = NULL;

  return TSDB_CODE_SUCCESS;
}

static int32_t partTagsOptSplitInterpPartition(
    SPartitionLogicNode* pPartition, SScanLogicNode* pScan) {
  int32_t    code = TSDB_CODE_SUCCESS;
  SNodeList* pColumnKeys = NULL;
  SNodeList* pGroupTags = NULL;

  code = nodesCloneList(pScan->pGroupTags, &pGroupTags);
  SNode* pKey = NULL;
  FOREACH(pKey, pPartition->pPartitionKeys) {
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }

    SNode* pClone = NULL;
    code = nodesCloneNode(pKey, &pClone);
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
    SNodeList** ppTarget = partitionKeyHasOrdinaryColumn(pKey)
                               ? &pColumnKeys
                               : &pGroupTags;
    code = nodesListMakeStrictAppend(ppTarget, pClone);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = partTagsOptRebuildTbanme(pGroupTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pPartition->pInterpPreSplitPartitionKeys =
        pPartition->pPartitionKeys;
    pPartition->pInterpPreSplitGroupTags = pScan->pGroupTags;
    pPartition->interpPreSplitGroupSort = pScan->groupSort;
    pPartition->pPartitionKeys = pColumnKeys;
    pScan->pGroupTags = pGroupTags;
    pScan->groupSort = true;
    pColumnKeys = NULL;
    pGroupTags = NULL;
  }

  nodesDestroyList(pColumnKeys);
  nodesDestroyList(pGroupTags);
  return code;
}

static bool partTagsOptHasPendingInterpSplit(
    const SPartitionLogicNode* pPartition) {
  return NULL != pPartition->pInterpPreSplitPartitionKeys;
}

static int32_t partTagsOptSyncMergeTargets(
    SMergeLogicNode* pMerge, const SScanLogicNode* pScan) {
  if (NULL == pMerge) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t    code = TSDB_CODE_SUCCESS;
  SNodeList* pInputs = NULL;
  SNodeList* pTargets = NULL;
  code = nodesCloneList(pScan->node.pTargets, &pInputs);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesCloneList(pScan->node.pTargets, &pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) {
    nodesDestroyList(pMerge->pInputs);
    nodesDestroyList(pMerge->node.pTargets);
    pMerge->pInputs = pInputs;
    pMerge->node.pTargets = pTargets;
    pInputs = NULL;
    pTargets = NULL;
  }
  nodesDestroyList(pInputs);
  nodesDestroyList(pTargets);
  return code;
}

static int32_t partTagsOptCommitInterpSplit(
    SPartitionLogicNode* pPartition, SMergeLogicNode* pMerge,
    SScanLogicNode* pScan) {
  if (!partTagsOptHasPendingInterpSplit(pPartition)) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = partTagsOptRemovePseudoCols(pScan);
  if (TSDB_CODE_SUCCESS == code) {
    code = partTagsOptSyncMergeTargets(pMerge, pScan);
  }
  if (TSDB_CODE_SUCCESS == code) {
    nodesDestroyList(pPartition->pInterpPreSplitPartitionKeys);
    nodesDestroyList(pPartition->pInterpPreSplitGroupTags);
    pPartition->pInterpPreSplitPartitionKeys = NULL;
    pPartition->pInterpPreSplitGroupTags = NULL;
    pPartition->interpPreSplitGroupSort = false;
  }
  return code;
}

static int32_t partTagsOptRollbackInterpSplit(
    SPartitionLogicNode* pPartition, SScanLogicNode* pScan) {
  if (!partTagsOptHasPendingInterpSplit(pPartition)) {
    return TSDB_CODE_SUCCESS;
  }
  if (NULL == pScan) {
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  nodesDestroyList(pPartition->pPartitionKeys);
  nodesDestroyList(pScan->pGroupTags);
  pPartition->pPartitionKeys =
      pPartition->pInterpPreSplitPartitionKeys;
  pScan->pGroupTags = pPartition->pInterpPreSplitGroupTags;
  pScan->groupSort = pPartition->interpPreSplitGroupSort;
  pPartition->pInterpPreSplitPartitionKeys = NULL;
  pPartition->pInterpPreSplitGroupTags = NULL;
  pPartition->interpPreSplitGroupSort = false;
  return TSDB_CODE_SUCCESS;
}

static int32_t partTagsOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  partTagsOptPreservePartitionOrder(pLogicSubplan->pNode);

  SLogicNode* pNode = optFindPossibleNode(pLogicSubplan->pNode, partTagsOptMayBeOptimized, NULL);
  if (NULL == pNode) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t         code = TSDB_CODE_SUCCESS;
  SScanLogicNode* pScan = (SScanLogicNode*)nodesListGetNode(pNode->pChildren, 0);
  if (partTagsOptCanSplitInterpPartition(pNode)) {
    code = partTagsOptSplitInterpPartition(
        (SPartitionLogicNode*)pNode, pScan);
    pCxt->optimized = TSDB_CODE_SUCCESS == code;
    return code;
  }

  if (QUERY_NODE_LOGIC_PLAN_PARTITION == nodeType(pNode)) {
    // The partition node is destroyed below without inspecting pConditions, so
    // any filter still attached to it would be dropped silently. That is safe
    // only because the two ways a condition can reach here are disjoint:
    //   1. PushDownCondition runs far earlier in optimizeRuleSet[] and has
    //      already moved conditions above a plain scan into the scan itself;
    //   2. the only case that keeps conditions on the node is an external
    //      subtree, and partTagsIsOptimizableNode rejects SCAN_TYPE_EXTERNAL.
    // Implicit-HAVING FIRST/LAST partitions are rejected by
    // partTagsIsOptimizableNode because folding them would lose their
    // single-group identity.
    // If a future scan type both skips condition pushdown and passes
    // partTagsIsOptimizableNode, add a pConditions guard here (migrate the
    // filter onto the scan) together with a regression case that reaches it.
    TSWAP(((SPartitionLogicNode*)pNode)->pPartitionKeys, pScan->pGroupTags);
    int32_t code = replaceLogicNode(pLogicSubplan, pNode, (SLogicNode*)pScan);
    if (TSDB_CODE_SUCCESS == code) {
      code = adjustLogicNodeDataRequirement((SLogicNode*)pScan, pNode->resultDataOrder);
    }
    if (TSDB_CODE_SUCCESS == code) {
      // After partition elimination, propagate ts order from scan to its new parent.
      // scanPathOpt previously set scan's outputTsOrder but optSetParentOrder stopped
      // at the partition node. Now that partition is removed, the parent (e.g. interval)
      // should inherit the scan's output ts order as its input ts order.
      if (pScan->node.pParent && pScan->node.outputTsOrder != 0) {
        pScan->node.pParent->inputTsOrder = pScan->node.outputTsOrder;
      }

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
      // The external connector needs the wrapper in the declared remote output.
      // Local scans materialize it through pAggFuncs, as in 3.0; adding both
      // representations makes the aggregate descriptor count the tag twice.
      code = partTagsRewriteGroupTagsToFuncs(pScan->pGroupTags, start, pAgg, pScan->scanType == SCAN_TYPE_EXTERNAL);
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = partTagsOptRemovePseudoCols(pScan);
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
  // Local super-table scans keep the root Project so the top-level project
  // operator can merge result blocks efficiently. External scans may reuse
  // the super-table model for semantic checks, but should still participate
  // in normal Project elimination.
  if (NULL == pNode->pParent &&
      (QUERY_NODE_LOGIC_PLAN_PROJECT != nodeType(pNode) || 1 != LIST_LENGTH(pNode->pChildren) ||
       (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(nodesListGetNode(pNode->pChildren, 0)) &&
        SCAN_TYPE_EXTERNAL != ((SScanLogicNode*)nodesListGetNode(pNode->pChildren, 0))->scanType &&
        TSDB_SUPER_TABLE == ((SScanLogicNode*)nodesListGetNode(pNode->pChildren, 0))->tableType))) {
    return false;
  }

  if (OPTIMIZE_FLAG_TEST_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_ELIMINATE_PROJ)) {
    return false;
  }

  if (NULL != pNode->pParent &&
      (QUERY_NODE_LOGIC_PLAN_PROJECT != nodeType(pNode) || 1 != LIST_LENGTH(pNode->pChildren) ||
       QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(nodesListGetNode(pNode->pChildren, 0)) ||
       QUERY_NODE_LOGIC_PLAN_JOIN != nodeType(pNode->pParent))) {
    return false;
  }

  if (QUERY_NODE_LOGIC_PLAN_PROJECT != nodeType(pNode) || 1 != LIST_LENGTH(pNode->pChildren)) {
    return false;
  }

  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(nodesListGetNode(pNode->pChildren, 0))) {
    SScanLogicNode* pChild = (SScanLogicNode*)nodesListGetNode(pNode->pChildren, 0);
    if (pChild && OPTIMIZE_FLAG_TEST_MASK(pChild->node.optimizedFlag, OPTIMIZE_FLAG_ELIMINATE_VSCAN)) {
      return false;
    }
  }

  if (QUERY_NODE_LOGIC_PLAN_WINDOW == nodeType(nodesListGetNode(pNode->pChildren, 0))) {
    SWindowLogicNode* pChild = (SWindowLogicNode*)nodesListGetNode(pNode->pChildren, 0);
    if (pChild->winType == WINDOW_TYPE_EXTERNAL) {
      return false;
    }
  }

  if (QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL == nodeType(nodesListGetNode(pNode->pChildren, 0))) {
    SLogicNode* pChild = (SLogicNode*)nodesListGetNode(pNode->pChildren, 0);
    if (LIST_LENGTH(pChild->pTargets) != LIST_LENGTH(pNode->pTargets)) {
      return false;
    }
    SNode* pProjTarget = NULL;
    SNode* pChildTarget = NULL;
    FORBOTH(pProjTarget, pNode->pTargets, pChildTarget, pChild->pTargets) {
      if (strcmp(((SColumnNode*)pProjTarget)->colName, ((SColumnNode*)pChildTarget)->colName) != 0) {
        return false;
      }
    }
  }

  if (QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN == nodeType(nodesListGetNode(pNode->pChildren, 0))) {
    SLogicNode* pChild = (SLogicNode*)nodesListGetNode(pNode->pChildren, 0);
    if (LIST_LENGTH(((SScanLogicNode *)pChild)->pScanPseudoCols) != 0) {
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

  if (QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN == nodeType(pChild)) {
    return false;
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
    SColumnNode*          pCol = (SColumnNode*)pNode;
    RewriteTableAliasCxt* pCtx = (RewriteTableAliasCxt*)pContext;
    tstrncpy(pCol->tableAlias, pCtx->newTableAlias, TSDB_TABLE_NAME_LEN);
  }
  return DEAL_RES_CONTINUE;
}

static void eliminateProjPushdownProjIdx(SNodeList* pParentProjects, SNodeList* pChildTargets) {
  SNode *pChildTarget = NULL, *pParentProject = NULL;
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

static int32_t eliminateProjOptAlignExtScanOutputs(SProjectLogicNode* pProjectNode, SLogicNode* pChild,
                                                   bool* pAligned, bool* pBlocked) {
  if (NULL != pAligned) {
    *pAligned = false;
  }
  if (NULL != pBlocked) {
    *pBlocked = false;
  }

  if (NULL == pProjectNode || NULL == pChild || QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pChild)) {
    return TSDB_CODE_SUCCESS;
  }

  SScanLogicNode* pScan = (SScanLogicNode*)pChild;
  if (SCAN_TYPE_EXTERNAL != pScan->scanType) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t projCnt = (NULL != pProjectNode->pProjections) ? LIST_LENGTH(pProjectNode->pProjections) : 0;
  planDebug("FQ-eliminateProject: inspect external scan child for project alignment project=%p scan=%p projCnt=%d scanCols=%d targets=%d pseudoCols=%d hasCond=%d",
            pProjectNode, pScan, projCnt,
            pScan->pScanCols ? LIST_LENGTH(pScan->pScanCols) : 0,
            pScan->node.pTargets ? LIST_LENGTH(pScan->node.pTargets) : 0,
            pScan->pScanPseudoCols ? LIST_LENGTH(pScan->pScanPseudoCols) : 0,
            NULL != pScan->node.pConditions);
  fqPlanDebugLogExprList("eliminateProj.project.projections", pProjectNode->pProjections);
  fqPlanDebugLogNodeOutputs("eliminateProj.scan.before", (SLogicNode*)pScan);

  if (NULL != pScan->pScanPseudoCols && LIST_LENGTH(pScan->pScanPseudoCols) > 0) {
    if (NULL != pBlocked) {
      *pBlocked = true;
    }
    planDebug("FQ-eliminateProject: keep project=%p over external scan=%p because scan has pseudo cols=%d",
              pProjectNode, pScan, LIST_LENGTH(pScan->pScanPseudoCols));
    return TSDB_CODE_SUCCESS;
  }

  SNodeList* pNewScanCols = NULL;
  int32_t code = nodesCloneList(pProjectNode->pProjections, &pNewScanCols);
  if (TSDB_CODE_SUCCESS != code) {
    planError("FQ-eliminateProject: clone project projections for external scan failed project=%p scan=%p code=0x%x",
              pProjectNode, pScan, code);
    return code;
  }

  SNodeList* pNewTargets = NULL;
  code = createColumnByRewriteExprs(pNewScanCols, &pNewTargets);
  if (TSDB_CODE_SUCCESS != code) {
    planError("FQ-eliminateProject: rebuild external scan targets from project projections failed project=%p scan=%p projCnt=%d code=0x%x",
              pProjectNode, pScan, projCnt, code);
    nodesDestroyList(pNewScanCols);
    return code;
  }

  nodesDestroyList(pScan->pScanCols);
  pScan->pScanCols = pNewScanCols;
  nodesDestroyList(pScan->node.pTargets);
  pScan->node.pTargets = pNewTargets;

  if (NULL != pAligned) {
    *pAligned = true;
  }
  planDebug("FQ-eliminateProject: aligned external scan outputs to project project=%p scan=%p scanCols=%d targets=%d",
            pProjectNode, pScan,
            pScan->pScanCols ? LIST_LENGTH(pScan->pScanCols) : 0,
            pScan->node.pTargets ? LIST_LENGTH(pScan->node.pTargets) : 0);
  fqPlanDebugLogNodeOutputs("eliminateProj.scan.after", (SLogicNode*)pScan);
  return TSDB_CODE_SUCCESS;
}

static int32_t eliminateProjOptFindProjPrefixWithOrderCheck(SProjectLogicNode* pProj, SProjectLogicNode* pChild,
                                                            SNodeList** pNewChildTargets, bool* orderMatch) {
  int32_t code = 0;
  SNode * pProjection = NULL, *pChildTarget = NULL;
  *orderMatch = true;
  FORBOTH(pProjection, pProj->pProjections, pChildTarget, pChild->node.pTargets) {
    if (!pProjection) break;
    if (0 != strcmp(((SColumnNode*)pProjection)->colName, ((SColumnNode*)pChildTarget)->colName)) {
      *orderMatch = false;
      break;
    }
    if (pNewChildTargets) {
      SNode* pNew = NULL;
      code = nodesCloneNode(pChildTarget, &pNew);
      if (TSDB_CODE_SUCCESS == code) {
        code = nodesListMakeStrictAppend(pNewChildTargets, pNew);
      }
      if (TSDB_CODE_SUCCESS != code && pNewChildTargets) {
        nodesDestroyList(*pNewChildTargets);
        *pNewChildTargets = NULL;
        break;
      }
    }
  }
  return code;
}

static int32_t eliminateProjOptPushTargetsToSetOpChildren(SProjectLogicNode* pSetOp) {
  SNode*  pChildProj = NULL;
  int32_t code = 0;
  bool    orderMatch = false;
  FOREACH(pChildProj, pSetOp->node.pChildren) {
    if (QUERY_NODE_LOGIC_PLAN_PROJECT == nodeType(pChildProj)) {
      SProjectLogicNode* pChildLogic = (SProjectLogicNode*)pChildProj;
      SNodeList*         pNewChildTargetsForChild = NULL;
      code = eliminateProjOptFindProjPrefixWithOrderCheck(pSetOp, pChildLogic, &pNewChildTargetsForChild, &orderMatch);
      if (TSDB_CODE_SUCCESS != code) break;
      nodesDestroyList(pChildLogic->node.pTargets);
      pChildLogic->node.pTargets = pNewChildTargetsForChild;
      alignProjectionWithTarget((SLogicNode*)pChildLogic);
      if (pChildLogic->isSetOpProj) {
        code = eliminateProjOptPushTargetsToSetOpChildren(pChildLogic);
        if (TSDB_CODE_SUCCESS != code) break;
      }
    }
  }
  return code;
}

static int32_t eliminateProjOptimizeImpl(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan,
                                         SProjectLogicNode* pProjectNode) {
  SLogicNode* pChild = (SLogicNode*)nodesListGetNode(pProjectNode->node.pChildren, 0);
  int32_t     code = 0;
  int32_t     lino = 0;
  bool        isSetOpProj = false;
  bool        orderMatch = false;
  bool        sizeMatch = LIST_LENGTH(pProjectNode->pProjections) == LIST_LENGTH(pChild->pTargets);
  bool        needReplaceTargets = true;
  SSHashObj*  pTargetHash = NULL;
  SNodeList*  pNewChildTargets = NULL;
  SNode*      pNew = NULL;
  bool        extScanAligned = false;
  bool        extScanBlocked = false;

  if (NULL == pProjectNode->node.pParent) {
    SNode*    pProjection = NULL, *pChildTarget = NULL;
    isSetOpProj = QUERY_NODE_LOGIC_PLAN_PROJECT == nodeType(pChild) && ((SProjectLogicNode*)pChild)->isSetOpProj;
    if (isSetOpProj) {
      // For sql: select ... from (select ... union all select ...);
      // When eliminating the outer proj (the outer select), we have to make sure that the outer proj projections and
      // union all project targets have same columns in the same order. See detail in TD-30188
      PLAN_ERR_JRET(eliminateProjOptFindProjPrefixWithOrderCheck(pProjectNode, (SProjectLogicNode*)pChild,
                                                          sizeMatch ? NULL : &pNewChildTargets, &orderMatch));
      if (sizeMatch && orderMatch) {
        pNewChildTargets = pChild->pTargets;
        needReplaceTargets = false;
      }
    } else {
      pTargetHash = tSimpleHashInit(LIST_LENGTH(pChild->pTargets), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
      QUERY_CHECK_NULL(pTargetHash, code, lino, _return, terrno)
      FOREACH(pChildTarget, pChild->pTargets) {
        char*   pName = ((SColumnNode*)pChildTarget)->colName;
        int32_t len = (int32_t)strlen(pName);
        if (tSimpleHashGet(pTargetHash, pName, len) == NULL) {
          PLAN_ERR_JRET(tSimpleHashPut(pTargetHash, pName, len, &pChildTarget, POINTER_BYTES));
        }
      }
      FOREACH(pProjection, pProjectNode->pProjections) {
        char*   pName = ((SColumnNode*)pProjection)->colName;
        int32_t len = (int32_t)strlen(pName);
        SNode** p = tSimpleHashGet(pTargetHash, pName, len);
        if (p) {
          PLAN_ERR_JRET(nodesCloneNode(*p, &pNew));
          PLAN_ERR_JRET(nodesListMakeStrictAppend(&pNewChildTargets, pNew));
          pNew = NULL;
        }
      }
    }

    // Do NOT prune targets of a rowset source node: its pBlockBuf has already serialized ALL
    // columns at parse time. Replacing pTargets here would cause the physi node's
    // pOutputDataBlockDesc to have fewer slots than columns in pBlockBuf, making
    // blockDataFromBuf read garbage data and overflow the heap (TD-35151).
    bool canReplaceChildTargets = nodeType(pChild) != QUERY_NODE_LOGIC_PLAN_ROWSET_SOURCE &&
                                  eliminateProjOptCanChildConditionUseChildTargets(pChild, pNewChildTargets) &&
                                  (!isSetOpProj || orderMatch);
    if (canReplaceChildTargets) {
      if (needReplaceTargets) {
        nodesDestroyList(pChild->pTargets);
        pChild->pTargets = pNewChildTargets;
      }
    } else {
      if (needReplaceTargets) {
        nodesDestroyList(pNewChildTargets);
        pNewChildTargets = NULL;
      }
      OPTIMIZE_FLAG_SET_MASK(pProjectNode->node.optimizedFlag, OPTIMIZE_FLAG_ELIMINATE_PROJ);
      pCxt->optimized = true;
      tSimpleHashCleanup(pTargetHash);
      return code;
    }
  } else {
    RewriteTableAliasCxt cxt = {.newTableAlias = pProjectNode->stmtName, .rewriteColName = false};
    SScanLogicNode*      pScan = (SScanLogicNode*)pChild;
    nodesWalkExprs(pScan->pScanCols, eliminateProjOptRewriteScanTableAlias, &cxt);
    nodesWalkExprs(pScan->pScanPseudoCols, eliminateProjOptRewriteScanTableAlias, &cxt);
    nodesWalkExpr(pScan->node.pConditions, eliminateProjOptRewriteScanTableAlias, &cxt);
    nodesWalkExprs(pChild->pTargets, eliminateProjOptRewriteScanTableAlias, &cxt);
    eliminateProjPushdownProjIdx(pProjectNode->pProjections, pChild->pTargets);
  }

  PLAN_ERR_JRET(eliminateProjOptAlignExtScanOutputs(pProjectNode, pChild, &extScanAligned, &extScanBlocked));
  if (extScanBlocked) {
    planDebug("FQ-eliminateProject: skip eliminating project=%p because external scan child=%p cannot be aligned to project outputs safely",
              pProjectNode, pChild);
    OPTIMIZE_FLAG_SET_MASK(pProjectNode->node.optimizedFlag, OPTIMIZE_FLAG_ELIMINATE_PROJ);
    pCxt->optimized = true;
    tSimpleHashCleanup(pTargetHash);
    return code;
  }

  PLAN_ERR_JRET(replaceLogicNode(pLogicSubplan, (SLogicNode*)pProjectNode, pChild));
  if (pProjectNode->node.pHint && !pChild->pHint) {
    TSWAP(pProjectNode->node.pHint, pChild->pHint);
  }

  NODES_CLEAR_LIST(pProjectNode->node.pChildren);
  nodesDestroyNode((SNode*)pProjectNode);
  // if pChild is a project logic node, remove its projection which is not reference by its target.
  if (needReplaceTargets) {
    alignProjectionWithTarget(pChild);
    // Since we have eliminated the outer proj, we need to push down the new targets to the children of the set
    // operation.
    if (isSetOpProj && orderMatch && !sizeMatch) {
      PLAN_ERR_JRET(eliminateProjOptPushTargetsToSetOpChildren((SProjectLogicNode*)pChild));
    }
  }

  pCxt->optimized = true;
  tSimpleHashCleanup(pTargetHash);
  return code;

_return:
  planError("%s failed at line:%d, code:%d", __func__, lino, code);
  nodesDestroyNode(pNew);
  nodesDestroyList(pNewChildTargets);
  tSimpleHashCleanup(pTargetHash);
  return code;
}

static int32_t eliminateProjOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  if (inStreamCalcClause(pCxt->pPlanCxt)) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t            code = 0;
  SProjectLogicNode* pProjectNode =
      (SProjectLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, eliminateProjOptMayBeOptimized, &code);

  if (NULL == pProjectNode) {
    return TSDB_CODE_SUCCESS;
  }

  return eliminateProjOptimizeImpl(pCxt, pLogicSubplan, pProjectNode);
}

static bool rewriteTailOptMayBeOptimized(SLogicNode* pNode, void* pCtx) {
  if (QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC != nodeType(pNode) ||
      !((SIndefRowsFuncLogicNode*)pNode)->isTailFunc) {
    return false;
  }
  if (nodeHasExternalScan(pNode)) {
    planDebug("FQ-rewriteTail: allow rewrite for external subtree indef=%p to keep operator-based TAIL semantics",
              pNode);
  }
  return true;
}

static int32_t rewriteTailOptCreateOrderByExpr(SNode* pSortKey, SNode** ppNode) {
  SOrderByExprNode* pOrder = NULL;
  int32_t           code = nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR, (SNode**)&pOrder);
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
  int32_t     code = nodesMakeNode(QUERY_NODE_LIMIT, (SNode**)&pLimitNode);
  if (NULL == pLimitNode) {
    return code;
  }
  code = nodesMakeValueNodeFromInt64(NULL == pLimit ? -1 : ((SValueNode*)pLimit)->datum.i, (SNode**)&pLimitNode->limit);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  code = nodesMakeValueNodeFromInt64(NULL == pOffset ? 0 : ((SValueNode*)pOffset)->datum.i, (SNode**)&pLimitNode->offset);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
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
  int32_t         code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SORT, (SNode**)&pSort);
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
  SNode*  pExpr = NULL;
  int32_t code = nodesCloneNode(nodesListGetNode(pFunc->pParameterList, 0), &pExpr);
  if (NULL == pExpr) {
    return code;
  }
  tstrncpy(((SExprNode*)pExpr)->aliasName, pFunc->node.aliasName, TSDB_COL_NAME_LEN);
  *ppNode = pExpr;
  return code;
}

static int32_t rewriteTailOptCreateProject(SIndefRowsFuncLogicNode* pIndef, SLogicNode** pOutput) {
  SProjectLogicNode* pProject = NULL;
  int32_t            code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_PROJECT, (SNode**)&pProject);
  if (NULL == pProject) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  TSWAP(pProject->node.pTargets, pIndef->node.pTargets);
  pProject->node.precision = pIndef->node.precision;

  SFunctionNode* pTail = NULL;
  SNode*         pFunc = NULL;
  FOREACH(pFunc, pIndef->pFuncs) {
    SNode* pNew = NULL;
    if (QUERY_NODE_FUNCTION != nodeType(pFunc)) {
      code = nodesCloneNode(pFunc, &pNew);
      if (TSDB_CODE_SUCCESS == code) {
        tstrncpy(((SExprNode*)pNew)->aliasName, ((SExprNode*)pFunc)->aliasName, TSDB_COL_NAME_LEN);
        code = nodesListMakeStrictAppend(&pProject->pProjections, pNew);
      }
    } else {
      code = rewriteTailOptCreateProjectExpr((SFunctionNode*)pFunc, &pNew);
      if (TSDB_CODE_SUCCESS == code) {
        code = nodesListMakeStrictAppend(&pProject->pProjections, pNew);
      }
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
    if (QUERY_NODE_FUNCTION == nodeType(pFunc) && FUNCTION_TYPE_TAIL == ((SFunctionNode*)pFunc)->funcType) {
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
  if (TSDB_CODE_SUCCESS == code && ((SSortLogicNode*)pSort)->groupSort) {
    // Project applies LIMIT/OFFSET per group; GroupSort does not consume pLimit.
    TSWAP(pSort->pLimit, pProject->pLimit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeAppend(&pProject->pChildren, (SNode*)pSort);
    pSort->pParent = pProject;
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
  if (QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC != nodeType(pNode) ||
      !((SIndefRowsFuncLogicNode*)pNode)->isUniqueFunc) {
    return false;
  }
  if (nodeHasExternalScan(pNode)) {
    planDebug("FQ-rewriteUnique: allow rewrite for external subtree indef=%p to keep operator-based UNIQUE semantics",
              pNode);
  }
  return true;
}

static int32_t rewriteUniqueOptCreateGroupingSet(SNode* pExpr, SNode** ppNode) {
  SGroupingSetNode* pGroupingSet = NULL;
  int32_t           code = nodesMakeNode(QUERY_NODE_GROUPING_SET, (SNode**)&pGroupingSet);
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
  int32_t        code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }

  tstrncpy(pFunc->functionName, "first", TSDB_FUNC_NAME_LEN);
  if (NULL != pSelectValue) {
    tstrncpy(pFunc->node.aliasName, pSelectValue->node.aliasName, TSDB_COL_NAME_LEN);
  } else {
    int64_t pointer = (int64_t)pFunc;
    char    name[TSDB_FUNC_NAME_LEN + TSDB_POINTER_PRINT_BYTES + TSDB_NAME_DELIMITER_LEN + 1] = {0};
    int32_t len = snprintf(name, sizeof(name) - 1, "%s.%" PRId64, pFunc->functionName, pointer);
    (void)taosHashBinary(name, len, sizeof(name));
    tstrncpy(pFunc->node.aliasName, name, TSDB_COL_NAME_LEN);
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

static int32_t rewriteUniqueOptCreateSelectValueFunc(SNode* pCol, SNode** ppNode) {
  SFunctionNode* pFunc = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }

  tstrncpy(pFunc->functionName, "_select_value", TSDB_FUNC_NAME_LEN);
  tstrncpy(pFunc->node.aliasName, ((SExprNode*)pCol)->aliasName, TSDB_COL_NAME_LEN);
  pFunc->node.resType = ((SExprNode*)pCol)->resType;
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
  int32_t        code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_AGG, (SNode**)&pAgg);
  if (NULL == pAgg) {
    return code;
  }

  TSWAP(pAgg->node.pChildren, pIndef->node.pChildren);
  optResetParent((SLogicNode*)pAgg);
  pAgg->node.precision = pIndef->node.precision;
  pAgg->node.requireDataOrder = DATA_ORDER_LEVEL_IN_BLOCK;  // first function requirement
  pAgg->node.resultDataOrder = DATA_ORDER_LEVEL_NONE;

  if (LIST_LENGTH(pAgg->node.pChildren) == 1 &&
      QUERY_NODE_LOGIC_PLAN_PARTITION == nodeType(nodesListGetNode(pAgg->node.pChildren, 0))) {
    SPartitionLogicNode* pPart = (SPartitionLogicNode*)nodesListGetNode(pAgg->node.pChildren, 0);
    pAgg->isPartTb = keysHasTbname(pPart->pPartitionKeys);
    pAgg->isGroupTb = pAgg->isPartTb;
  }

  bool   hasSelectPrimaryKey = false;
  bool   needNoTimelineTupleSelector = false;
  SNode* pPrimaryKey = NULL;
  SNode* pNode = NULL;
  FOREACH(pNode, pIndef->pFuncs) {
    if (QUERY_NODE_FUNCTION != nodeType(pNode)) {
      SNode* pNew = NULL;
      code = rewriteUniqueOptCreateSelectValueFunc(pNode, &pNew);
      if (TSDB_CODE_SUCCESS == code) {
        code = nodesListMakeStrictAppend(&pAgg->pAggFuncs, pNew);
      }
      if (TSDB_CODE_SUCCESS != code) break;
      continue;
    }
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    SNode*         pExpr = nodesListGetNode(pFunc->pParameterList, 0);
    if (FUNCTION_TYPE_UNIQUE == pFunc->funcType) {
      if (LIST_LENGTH(pFunc->pParameterList) > 1) {
        pPrimaryKey = nodesListGetNode(pFunc->pParameterList, 1);
      } else {
        // Parser did not append an implicit timeline argument, so _select_value
        // columns need a hidden selector to save the representative input tuple.
        needNoTimelineTupleSelector = true;
      }
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

  if (TSDB_CODE_SUCCESS == code && !hasSelectPrimaryKey && NULL != pPrimaryKey && NULL != pAgg->pAggFuncs) {
    SNode* pNew = NULL;
    code = rewriteUniqueOptCreateFirstFunc(NULL, pPrimaryKey, &pNew);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListMakeStrictAppend(&pAgg->pAggFuncs, pNew);
    }
  }
  if (TSDB_CODE_SUCCESS == code && needNoTimelineTupleSelector && NULL == pPrimaryKey && NULL != pAgg->pAggFuncs) {
    SNode* pConst = NULL;
    code = nodesMakeValueNodeFromInt32(1, &pConst);
    SNode* pNew = NULL;
    if (TSDB_CODE_SUCCESS == code) {
      code = rewriteUniqueOptCreateFirstFunc(NULL, pConst, &pNew);
    }
    nodesDestroyNode(pConst);
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
  int32_t      code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  if (NULL == pCol) {
    return code;
  }

  pCol->node.resType = pFunc->node.resType;
  if (FUNCTION_TYPE_UNIQUE == pFunc->funcType) {
    SExprNode* pExpr = (SExprNode*)nodesListGetNode(pFunc->pParameterList, 0);
    if (QUERY_NODE_COLUMN == nodeType(pExpr)) {
      PLAN_ERR_RET(nodesCloneNode((SNode*)pExpr, (SNode**)&pCol));
    } else {
      tstrncpy(pCol->colName, pExpr->aliasName, TSDB_COL_NAME_LEN);
    }
  } else {
    tstrncpy(pCol->colName, pFunc->node.aliasName, TSDB_COL_NAME_LEN);
  }
  tstrncpy(pCol->node.aliasName, pFunc->node.aliasName, TSDB_COL_NAME_LEN);
  *ppNode = (SNode*)pCol;
  return code;
}

static int32_t rewriteUniqueOptCreateProject(SIndefRowsFuncLogicNode* pIndef, SLogicNode** pOutput) {
  SProjectLogicNode* pProject = NULL;
  int32_t            code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_PROJECT, (SNode**)&pProject);
  if (NULL == pProject) {
    return code;
  }

  TSWAP(pProject->node.pTargets, pIndef->node.pTargets);
  pProject->node.precision = pIndef->node.precision;
  pProject->node.requireDataOrder = DATA_ORDER_LEVEL_NONE;
  pProject->node.resultDataOrder = DATA_ORDER_LEVEL_NONE;

  SNode* pNode = NULL;
  FOREACH(pNode, pIndef->pFuncs) {
    SNode* pNew = NULL;
    if (QUERY_NODE_FUNCTION != nodeType(pNode)) {
      code = nodesCloneNode(pNode, &pNew);
    } else {
      code = rewriteUniqueOptCreateProjectCol((SFunctionNode*)pNode, &pNew);
    }
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
    OPTIMIZE_FLAG_SET_MASK(pAgg->optimizedFlag, OPTIMIZE_FLAG_FQ_UNIQUE_LOCAL);
    OPTIMIZE_FLAG_SET_MASK(pProject->optimizedFlag, OPTIMIZE_FLAG_FQ_UNIQUE_LOCAL);
    planDebug("FQ-rewriteUnique: mark rewritten UNIQUE subtree local-only project=%p agg=%p indef=%p",
              pProject, pAgg, pIndef);
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

static bool lastRowScanOptMayBeOptimized(SLogicNode* pNode, void* pCtx);

typedef struct SLastSmaNotNullOptCxt {
  SLogicNode*     pRoot;
  SScanLogicNode* pScan;
  SNodeList*      pLastCols;
  int32_t         code;
  bool            hasLast;
  bool            valid;
} SLastSmaNotNullOptCxt;

static bool lastSmaNotNullOptIsAllowedFunc(const SFunctionNode* pFunc) {
  if (NULL == pFunc) {
    return false;
  }

  if (FUNCTION_TYPE_LAST == pFunc->funcType) {
    return true;
  }

  if (fmIsScalarFunc(pFunc->funcId) || fmIsPseudoColumnFunc(pFunc->funcId) || fmIsSelectValueFunc(pFunc->funcId) ||
      fmIsGroupKeyFunc(pFunc->funcId) || fmisSelectGroupConstValueFunc(pFunc->funcId)) {
    return true;
  }

  return false;
}

static int32_t lastSmaNotNullOptCollectLastCol(SNodeList** ppLastCols, const SColumnNode* pCol) {
  if (NULL == pCol) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, *ppLastCols) {
    SColumnNode* pExist = (SColumnNode*)pNode;
    if (pExist->colId == pCol->colId && pExist->tableId == pCol->tableId && pExist->colType == pCol->colType) {
      return TSDB_CODE_SUCCESS;
    }
  }

  SNode* pClone = NULL;
  PLAN_ERR_RET(nodesCloneNode((SNode*)pCol, &pClone));
  PLAN_ERR_RET(nodesListMakeStrictAppend(ppLastCols, pClone));
  return TSDB_CODE_SUCCESS;
}

static int32_t lastSmaNotNullOptInspectFuncList(SNodeList* pFuncs, SLastSmaNotNullOptCxt* pCxt) {
  if (NULL == pFuncs || NULL == pCxt) {
    return TSDB_CODE_SUCCESS;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pFuncs) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    if (!lastSmaNotNullOptIsAllowedFunc(pFunc)) {
      pCxt->valid = false;
      return TSDB_CODE_SUCCESS;
    }

    if (FUNCTION_TYPE_LAST != pFunc->funcType) {
      continue;
    }

    SNode* pParam = nodesListGetNode(pFunc->pParameterList, 0);
    if (NULL == pParam || QUERY_NODE_COLUMN != nodeType(pParam) || ((SColumnNode*)pParam)->colType != COLUMN_TYPE_COLUMN) {
      pCxt->valid = false;
      return TSDB_CODE_SUCCESS;
    }

    SNode* pLastParam = NULL;
    FOREACH(pLastParam, pFunc->pParameterList) {
      if (QUERY_NODE_COLUMN != nodeType(pLastParam)) {
        pCxt->valid = false;
        return TSDB_CODE_SUCCESS;
      }
    }

    pCxt->hasLast = true;
    PLAN_ERR_RET(lastSmaNotNullOptCollectLastCol(&pCxt->pLastCols, (SColumnNode*)pParam));
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t lastSmaNotNullOptInspectPlan(SLogicNode* pNode, SLastSmaNotNullOptCxt* pCxt) {
  if (NULL == pNode || NULL == pCxt || !pCxt->valid) {
    return TSDB_CODE_SUCCESS;
  }

  if (pNode != pCxt->pRoot && pNode->stmtRoot) {
    pCxt->valid = false;
    return TSDB_CODE_SUCCESS;
  }

  switch (nodeType(pNode)) {
    case QUERY_NODE_LOGIC_PLAN_SCAN: {
      SScanLogicNode* pScan = (SScanLogicNode*)pNode;
      if (OPTIMIZE_FLAG_TEST_MASK(pScan->node.optimizedFlag, OPTIMIZE_FLAG_LAST_SMA_NOTNULL)) {
        pCxt->valid = false;
        return TSDB_CODE_SUCCESS;
      }
      if (NULL != pCxt->pScan && pCxt->pScan != pScan) {
        pCxt->valid = false;
        return TSDB_CODE_SUCCESS;
      }
      pCxt->pScan = pScan;
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_JOIN:
      pCxt->valid = false;
      return TSDB_CODE_SUCCESS;
    case QUERY_NODE_LOGIC_PLAN_AGG:
      PLAN_ERR_RET(lastSmaNotNullOptInspectFuncList(((SAggLogicNode*)pNode)->pAggFuncs, pCxt));
      break;
    case QUERY_NODE_LOGIC_PLAN_WINDOW:
      PLAN_ERR_RET(lastSmaNotNullOptInspectFuncList(((SWindowLogicNode*)pNode)->pFuncs, pCxt));
      break;
    case QUERY_NODE_LOGIC_PLAN_PARTITION:
      pCxt->valid = false;
      return TSDB_CODE_SUCCESS;
    case QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC:
    case QUERY_NODE_LOGIC_PLAN_INTERP_FUNC:
    case QUERY_NODE_LOGIC_PLAN_ANALYSIS_FUNC:
    case QUERY_NODE_LOGIC_PLAN_FORECAST_FUNC:
      pCxt->valid = false;
      return TSDB_CODE_SUCCESS;
    default:
      break;
  }

  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    PLAN_ERR_RET(lastSmaNotNullOptInspectPlan((SLogicNode*)pChild, pCxt));
    if (!pCxt->valid) {
      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static bool lastSmaNotNullOptHasScanFilter(const SScanLogicNode* pScan) {
  return IS_TSWINDOW_SPECIFIED(pScan->scanRange) || NULL != pScan->node.pConditions || NULL != pScan->pTagCond ||
         NULL != pScan->pTagIndexCond;
}

static bool lastSmaNotNullOptCanUseScan(const SScanLogicNode* pScan) {
  if (NULL == pScan) {
    return false;
  }

  switch (pScan->scanType) {
    case SCAN_TYPE_TABLE:
    case SCAN_TYPE_TABLE_MERGE:
      return true;
    default:
      break;
  }

  return false;
}

static int32_t lastSmaNotNullOptCreateIsNotNullCond(const SColumnNode* pCol, SNode** ppCond) {
  SOperatorNode* pOp = NULL;
  PLAN_ERR_RET(nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pOp));
  pOp->opType = OP_TYPE_IS_NOT_NULL;
  pOp->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;

  SNode* pLeft = NULL;
  int32_t code = nodesCloneNode((SNode*)pCol, &pLeft);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pOp);
    return code;
  }

  pOp->pLeft = pLeft;
  *ppCond = (SNode*)pOp;
  return TSDB_CODE_SUCCESS;
}

static int32_t lastSmaNotNullOptCreateOrCond(SNodeList* pLastCols, SNode** ppCond) {
  if (NULL == pLastCols || LIST_LENGTH(pLastCols) <= 0) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  if (LIST_LENGTH(pLastCols) == 1) {
    return lastSmaNotNullOptCreateIsNotNullCond((SColumnNode*)nodesListGetNode(pLastCols, 0), ppCond);
  }

  SLogicConditionNode* pOr = NULL;
  PLAN_ERR_RET(nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, (SNode**)&pOr));
  pOr->condType = LOGIC_COND_TYPE_OR;
  pOr->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pOr->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;

  SNode* pNode = NULL;
  FOREACH(pNode, pLastCols) {
    SNode* pChild = NULL;
    int32_t code = lastSmaNotNullOptCreateIsNotNullCond((SColumnNode*)pNode, &pChild);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListMakeStrictAppend(&pOr->pParameterList, pChild);
    }
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode((SNode*)pOr);
      return code;
    }
  }

  *ppCond = (SNode*)pOr;
  return TSDB_CODE_SUCCESS;
}

static bool lastSmaNotNullOptHasUnsupportedQueryShape(const SPlanContext* pPlanCxt) {
  if (NULL == pPlanCxt || NULL == pPlanCxt->pAstRoot) {
    return false;
  }

  switch (nodeType(pPlanCxt->pAstRoot)) {
    case QUERY_NODE_SELECT_STMT: {
      const SSelectStmt* pSelect = (const SSelectStmt*)pPlanCxt->pAstRoot;
      return pSelect->isSubquery || E_SUB_QUERY_NOT_SET != pSelect->subQType || NULL != pSelect->pPartitionByList ||
             (NULL != pSelect->pFromTable && QUERY_NODE_TEMP_TABLE == nodeType(pSelect->pFromTable));
    }
    case QUERY_NODE_SET_OPERATOR:
      return E_SUB_QUERY_NOT_SET != ((const SSetOperator*)pPlanCxt->pAstRoot)->subQType;
    default:
      return false;
  }
}

static int32_t lastSmaNotNullPrefilterOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  if (inStreamCalcClause(pCxt->pPlanCxt) || lastSmaNotNullOptHasUnsupportedQueryShape(pCxt->pPlanCxt)) {
    return TSDB_CODE_SUCCESS;
  }

  SLastSmaNotNullOptCxt cxt = {.pRoot = pLogicSubplan->pNode, .valid = true};
  int32_t               code = lastSmaNotNullOptInspectPlan(pLogicSubplan->pNode, &cxt);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyList(cxt.pLastCols);
    return code;
  }

  if (!cxt.valid || !cxt.hasLast || NULL == cxt.pScan || NULL == cxt.pLastCols) {
    nodesDestroyList(cxt.pLastCols);
    return TSDB_CODE_SUCCESS;
  }

  if (!lastSmaNotNullOptCanUseScan(cxt.pScan) || !lastSmaNotNullOptHasScanFilter(cxt.pScan)) {
    nodesDestroyList(cxt.pLastCols);
    return TSDB_CODE_SUCCESS;
  }

  if (NULL != optFindPossibleNode(pLogicSubplan->pNode, lastRowScanOptMayBeOptimized, NULL)) {
    nodesDestroyList(cxt.pLastCols);
    return TSDB_CODE_SUCCESS;
  }

  SNode* pAdditionalCond = NULL;
  code = lastSmaNotNullOptCreateOrCond(cxt.pLastCols, &pAdditionalCond);
  nodesDestroyList(cxt.pLastCols);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  code = nodesMergeNode(&cxt.pScan->node.pConditions, &pAdditionalCond);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pAdditionalCond);
    return code;
  }

  OPTIMIZE_FLAG_SET_MASK(cxt.pScan->node.optimizedFlag, OPTIMIZE_FLAG_LAST_SMA_NOTNULL);
  pCxt->optimized = true;
  return TSDB_CODE_SUCCESS;
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

static bool isNeedSplitCacheLastFunc(SFunctionNode* pFunc, SScanLogicNode* pScan, SNodeList* pSplitAggFuncList) {
  int32_t funcType = pFunc->funcType;
  SNode* pParam = nodesListGetNode(pFunc->pParameterList, 0);
  if (funcType == FUNCTION_TYPE_LAST_ROW) {
    if (pScan->cacheLastMode == TSDB_CACHE_MODEL_LAST_VALUE) {
      return true;
    }
  } else if (funcType == FUNCTION_TYPE_LAST) {
    if (pScan->cacheLastMode == TSDB_CACHE_MODEL_LAST_ROW) {
      return true;
    }
    if (pParam != NULL) {
      if (nodeType(pParam) != QUERY_NODE_COLUMN || ((SColumnNode*)pParam)->colType != COLUMN_TYPE_COLUMN) {
        return true;
      }
    }
  } else if (funcType != FUNCTION_TYPE_SELECT_VALUE && funcType != FUNCTION_TYPE_GROUP_KEY &&
             funcType != FUNCTION_TYPE_GROUP_CONST_VALUE) {
    return true;
  }

  // return true if this func is related to a split func
  if (pFunc->node.relatedTo > 0 && pSplitAggFuncList != NULL) {
    SNode* pNode = NULL;
    FOREACH(pNode, pSplitAggFuncList) {
      SFunctionNode* pSplitFunc = (SFunctionNode*)pNode;
      if (pFunc->node.relatedTo == pSplitFunc->node.bindExprID) {
        return true;
      }
    }
  }

  return false;
}

static bool lastRowScanOptCheckFuncList(SLogicNode* pNode, int8_t cacheLastModel, bool* hasOtherFunc) {
  bool            hasNonPKSelectFunc = false;
  bool            canOptimize = true;
  SNode*          pFunc = NULL;
  int32_t         lastColNum = 0, selectNonPKColNum = 0;
  col_id_t        lastColId = -1, selectNonPKColId = -1;
  SScanLogicNode* pScan = (SScanLogicNode*)nodesListGetNode(((SAggLogicNode*)pNode)->node.pChildren, 0);
  uint32_t        needSplitFuncCount = 0;
  SHashObj*       pSplitExprId = taosHashInit(4, 
    taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  if (pSplitExprId == NULL) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
    return false;
  }

  WHERE_EACH(pFunc, ((SAggLogicNode*)pNode)->pAggFuncs) {
    SFunctionNode* pAggFunc = (SFunctionNode*)pFunc;
    SNode*         pParam = nodesListGetNode(pAggFunc->pParameterList, 0);
    bool           curNeedSplit = false;
    if (pAggFunc->node.relatedTo > 0 && 
      taosHashGet(pSplitExprId, (void*)&pAggFunc->node.relatedTo, sizeof(int32_t)) != NULL) {
      curNeedSplit = true;
    } else if (FUNCTION_TYPE_LAST == pAggFunc->funcType) {
      if (QUERY_NODE_COLUMN == nodeType(pParam)) {
        SColumnNode* pCol = (SColumnNode*)pParam;
        if (pCol->colType != COLUMN_TYPE_COLUMN && TSDB_CACHE_MODEL_LAST_ROW != cacheLastModel) {
          curNeedSplit = true;
        }
        if (lastColId != pCol->colId) {
          lastColId = pCol->colId;
          lastColNum++;
        }
      } else if (QUERY_NODE_VALUE == nodeType(pParam) || QUERY_NODE_OPERATOR == nodeType(pParam)) {
        curNeedSplit = true;
      }
      if (!lastRowScanOptCheckColNum(lastColNum, lastColId, selectNonPKColNum, selectNonPKColId)) {
        canOptimize = false;
        break;
      }
      if (TSDB_CACHE_MODEL_LAST_ROW == cacheLastModel) {
        curNeedSplit = true;
      }
    } else if (FUNCTION_TYPE_SELECT_VALUE == pAggFunc->funcType) {
      if (QUERY_NODE_COLUMN == nodeType(pParam)) {
        SColumnNode* pCol = (SColumnNode*)pParam;
        if (COLUMN_TYPE_COLUMN == pCol->colType &&
            PRIMARYKEY_TIMESTAMP_COL_ID != pCol->colId &&
            !pCol->isPk) {
          // found a non-pk select column
          if (selectNonPKColId != pCol->colId) {
            selectNonPKColId = pCol->colId;
            selectNonPKColNum++;
          }
        }
      } else if (lastColNum > 0) {
        canOptimize = false;
        break;
      }
      if (!lastRowScanOptCheckColNum(lastColNum, lastColId, selectNonPKColNum, selectNonPKColId)) {
        canOptimize = false;
        break;
      }
    } else if (FUNCTION_TYPE_GROUP_KEY == pAggFunc->funcType || 
                FUNCTION_TYPE_GROUP_CONST_VALUE == pAggFunc->funcType) {
      if (!lastRowScanOptLastParaIsTag(pParam)) {
        canOptimize = false;
        break;
      }
    } else if (FUNCTION_TYPE_LAST_ROW != pAggFunc->funcType) {
      curNeedSplit = true;
    } else if (FUNCTION_TYPE_LAST_ROW == pAggFunc->funcType && TSDB_CACHE_MODEL_LAST_VALUE == cacheLastModel) {
      curNeedSplit = true;
    }

    if (curNeedSplit) {
      int32_t exprId = pAggFunc->node.bindExprID;
      if (exprId > 0) {
        if (NULL == taosHashGet(pSplitExprId, (void*)&exprId, sizeof(int32_t))) {
          int32_t code = taosHashPut(pSplitExprId, (void*)&exprId, sizeof(int32_t), NULL, 0);
          if (code != TSDB_CODE_SUCCESS) {
            qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
            return false;
          }
          *hasOtherFunc = true;
          needSplitFuncCount++;
          // when a basic function needs to be split, 
          // all functions related to it should be split too
          // thus we need to re-evaluate the remaining funcs
          WHERE_FIRST(((SAggLogicNode*)pNode)->pAggFuncs);
          continue;
        } else {
          // already counted
        }
      } else {
        *hasOtherFunc = true;
        needSplitFuncCount++;
      }
    }
    WHERE_NEXT;
  }

  if (needSplitFuncCount >= LIST_LENGTH(((SAggLogicNode*)pNode)->pAggFuncs)) {
    canOptimize = false;
  }

  taosHashCleanup(pSplitExprId);
  return canOptimize;
}

static bool lastRowScanOptCheckLastCache(SAggLogicNode* pAgg, SScanLogicNode* pScan) {
  if ((pAgg->hasLastRow == pAgg->hasLast && !pAgg->hasLastRow) || (!pAgg->hasLast && !pAgg->hasLastRow) ||
      NULL != pAgg->pGroupKeys || NULL != pScan->node.pConditions ||
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

static EDealRes lastRowScanOptGetColAndSetDataType(SNode* pNode, void* pContext, bool setType) {
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    SLastRowScanOptSetColDataTypeCxt* pCxt = pContext;
    if (pCxt->doAgg) {
      pCxt->code = nodesListMakeAppend(&pCxt->pLastCols, pNode);
      if (TSDB_CODE_SUCCESS != pCxt->code) {
        return DEAL_RES_ERROR;
      }
      if (setType) getLastCacheDataType(&(((SColumnNode*)pNode)->node.resType), pCxt->pkBytes);
    } else {
      SNode* pCol = NULL;
      FOREACH(pCol, pCxt->pLastCols) {
        if (nodesEqualNode(pCol, pNode)) {
          if (setType) getLastCacheDataType(&(((SColumnNode*)pNode)->node.resType), pCxt->pkBytes);
          break;
        }
      }
    }
    return DEAL_RES_IGNORE_CHILD;
  }
  return DEAL_RES_CONTINUE;
}

# if 0
static EDealRes lastRowScanOptGetLastCols(SNode* pNode, void* pContext) {
  return lastRowScanOptGetColAndSetDataType(pNode, pContext, false);
}

static EDealRes lastRowScanOptSetColDataType(SNode* pNode, void* pContext) {
  return lastRowScanOptGetColAndSetDataType(pNode, pContext, true);
}

static void lastRowScanOptSetLastTargets(SNodeList* pTargets, SNodeList* pLastCols, SNodeList* pLastRowCols, bool erase,
                                         int32_t pkBytes) {
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

static void lastRowScanOptRemoveUslessTargets(SNodeList* pTargets, SNodeList* pList1, SNodeList* pList2,
                                              SNodeList* pList3) {
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
  tstrncpy(pFuncTypeParam->pCol->name, pColNode->colName, TSDB_COL_NAME_LEN);
  if (NULL == taosArrayPush(pScan->pFuncTypes, pFuncTypeParam)) {
    taosMemoryFree(pFuncTypeParam);
    return terrno;
  }

  taosMemoryFree(pFuncTypeParam);
  return TSDB_CODE_SUCCESS;
}
# endif
static int32_t lastRowScanOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  if (inStreamCalcClause(pCxt->pPlanCxt)) {
    // stream calc query does not support last/last_row optimization
    return TSDB_CODE_SUCCESS;
  }
  SAggLogicNode* pAgg = (SAggLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, lastRowScanOptMayBeOptimized, NULL);

  if (NULL == pAgg) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = 0;
  int32_t lino = 0;
  SScanLogicNode* pScan = (SScanLogicNode*)nodesListGetNode(pAgg->node.pChildren, 0);
  pScan->scanType = SCAN_TYPE_LAST_ROW;
  pScan->igLastNull = pAgg->hasLast ? true : false;

  SNode* pNode = NULL;
  FOREACH(pNode, pAgg->pAggFuncs) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    int32_t        funcType = pFunc->funcType;
    SNode*         pParamNode = NULL;
    if (FUNCTION_TYPE_LAST_ROW == funcType || FUNCTION_TYPE_LAST == funcType) {
      int32_t len = snprintf(pFunc->functionName, sizeof(pFunc->functionName),
                             FUNCTION_TYPE_LAST_ROW == funcType ? "_cache_last_row" : "_cache_last");
      pFunc->functionName[len] = '\0';
      code = fmGetFuncInfo(pFunc, NULL, 0);
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    }
  }

  if (TSDB_CODE_SUCCESS != code) {
    qError("%s failed at %d, msg:%s", __func__, lino, tstrerror(code));
    return code;
  }

  pAgg->hasLastRow = false;
  pAgg->hasLast = false;
  pCxt->optimized = true;
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

/*
  This function is used to update the scan node(scan columns, targets) after
  the cache last/last_row function is split.
*/
static int32_t splitCacheLastFuncOptUpdateScanNode(SAggLogicNode* pAgg,
                                                   SNodeList* pFunc,
                                                   bool isNewAgg) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SNode* pNode = nodesListGetNode(pAgg->node.pChildren, 0);
  if (QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pNode)) {
    return TSDB_CODE_SUCCESS;
  }

  SScanLogicNode* pScan = (SScanLogicNode*)pNode;
  SNodeList*      pOldScanCols = NULL;
  TSWAP(pScan->pScanCols, pOldScanCols);
  nodesDestroyList(pScan->pScanPseudoCols);
  pScan->pScanPseudoCols = NULL;
  nodesDestroyList(pScan->node.pTargets);
  pScan->node.pTargets = NULL;
  SNodeListNode* list = NULL;
  code = nodesMakeNode(QUERY_NODE_NODE_LIST, (SNode**)&list);
  QUERY_CHECK_CODE(code, lino, _return);

  list->pNodeList = pFunc;
  code = nodesCollectColumnsFromNode((SNode*)list, NULL, COLLECT_COL_TYPE_COL,
                                     &pScan->pScanCols);
  QUERY_CHECK_CODE(code, lino, _return);

  code = nodesCollectColumnsFromNode((SNode*)list, NULL, COLLECT_COL_TYPE_TAG,
                                     &pScan->pScanPseudoCols);
  QUERY_CHECK_CODE(code, lino, _return);

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
    QUERY_CHECK_CODE(code, lino, _return);
  }

  code = createColumnByRewriteExprs(pScan->pScanCols, &pScan->node.pTargets);
  QUERY_CHECK_CODE(code, lino, _return);

  code = createColumnByRewriteExprs(pScan->pScanPseudoCols,
                                    &pScan->node.pTargets);
  QUERY_CHECK_CODE(code, lino, _return);

  OPTIMIZE_FLAG_CLEAR_MASK(pScan->node.optimizedFlag, OPTIMIZE_FLAG_SCAN_PATH);

_return:
  nodesFree((void*)list);
  nodesDestroyList(pOldScanCols);
  if (TSDB_CODE_SUCCESS != code) {
    if (isNewAgg) {
      nodesDestroyNode((SNode*)pAgg);
    }
    planError("failed to execute %s at line %d, code:%d - %s",
              __func__, lino, code, tstrerror(code));
  }
  return code;
}

static int32_t splitCacheLastFuncOptCreateAggLogicNode(SAggLogicNode** pNewAgg,
                                                       SAggLogicNode* pAgg,
                                                       SNodeList* pFunc,
                                                       SNodeList* pTargets) {
  SAggLogicNode* pNew = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_AGG, (SNode**)&pNew);
  if (NULL == pNew) {
    nodesDestroyList(pFunc);
    nodesDestroyList(pTargets);
    return code;
  }

  pNew->hasLastRow = false;
  pNew->hasLast = false;
  SNode*         pFuncNode = NULL;
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

  code = splitCacheLastFuncOptUpdateScanNode(pNew, pFunc, true);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
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
  int32_t          code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_MERGE, (SNode**)&pMerge);
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
  if (inStreamCalcClause(pCxt->pPlanCxt)) {
    // stream calc query does not support last/last_row optimization
    return TSDB_CODE_SUCCESS;
  }
  SAggLogicNode* pAgg =
      (SAggLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, splitCacheLastFuncOptMayBeOptimized, NULL);

  if (NULL == pAgg) {
    return TSDB_CODE_SUCCESS;
  }
  SScanLogicNode* pScan = (SScanLogicNode*)nodesListGetNode(pAgg->node.pChildren, 0);
  SNode*          pNode = NULL;
  SNodeList*      pSplitAggFuncList = NULL;
  int32_t         code = 0;

  {
    bool hasLast = false;
    bool hasLastRow = false;
    WHERE_EACH(pNode, pAgg->pAggFuncs) {
      SFunctionNode* pFunc = (SFunctionNode*)pNode;
      int32_t        funcType = pFunc->funcType;

      if (isNeedSplitCacheLastFunc(pFunc, pScan, pSplitAggFuncList)) {
        SNode* pNew = NULL;
        code = nodesCloneNode(pNode, &pNew);
        if (TSDB_CODE_SUCCESS == code) {
          code = nodesListMakeStrictAppend(&pSplitAggFuncList, pNew);
        }
        if (TSDB_CODE_SUCCESS != code) {
          break;
        }
        ERASE_NODE(pAgg->pAggFuncs);
        // when a basic function needs to be split, 
        // all functions related to it should be split too
        // thus we need to re-evaluate the remaining funcs
        WHERE_FIRST(pAgg->pAggFuncs);
        continue;
      }
      if (FUNCTION_TYPE_LAST_ROW == funcType) {
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
    nodesDestroyList(pSplitAggFuncList);
    return code;
  }

  if (NULL == pSplitAggFuncList) {
    planError("empty agg func list while splite projections, funcNum:%d", pAgg->pAggFuncs->length);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  SNodeList* pTargets = NULL;
  {
    WHERE_EACH(pNode, pAgg->node.pTargets) {
      SColumnNode* pCol = (SColumnNode*)pNode;
      SNode*       pFuncNode = NULL;
      bool         found = false;
      FOREACH(pFuncNode, pSplitAggFuncList) {
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
    nodesDestroyList(pSplitAggFuncList);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  SMergeLogicNode* pMerge = NULL;
  SAggLogicNode*   pNewAgg = NULL;
  code = splitCacheLastFuncOptCreateAggLogicNode(&pNewAgg, pAgg, pSplitAggFuncList, pTargets);
  if (TSDB_CODE_SUCCESS == code) {
    code = splitCacheLastFuncOptModifyAggLogicNode(pAgg);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = splitCacheLastFuncOptUpdateScanNode(pAgg, pAgg->pAggFuncs, false);
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
      NULL != pChild->pConditions || NULL != pChild->pLimit || NULL != pChild->pSlimit || LIST_LENGTH(pChild->pChildren) <= 0) {
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
    SColumnNode* pProjCol = (SColumnNode*)(*pNode);
    SNode*       pProjection;
    int32_t      projIdx = 1;
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
  SScanLogicNode* pScanNode =
      (SScanLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, tagScanOptShouldBeOptimized, NULL);
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
    int32_t    code = nodesMakeList(&pScanTargets);
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

static bool vtableTagScanOptShouldBeOptimized(SLogicNode* pNode, void* pCtx) {
  if (QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL != nodeType(pNode)) {
    return false;
  }
  SDynQueryCtrlLogicNode* pDyn = (SDynQueryCtrlLogicNode*)pNode;
  SVirtualScanLogicNode*  pVtableScan = (SVirtualScanLogicNode*)nodesListGetNode(pDyn->node.pChildren, 0);
  if (!pVtableScan || !pDyn->vtbScan.useTagScan) {
    return false;
  } else {
    int32_t nonExtChildCount = 0;
    SNode*  pChild = NULL;
    FOREACH(pChild, pVtableScan->node.pChildren) {
      if (!(nodeType(pChild) == QUERY_NODE_LOGIC_PLAN_SCAN &&
            ((SScanLogicNode*)pChild)->scanType == SCAN_TYPE_EXTERNAL)) {
        nonExtChildCount++;
      }
    }
    if (nonExtChildCount != 2) {
      return false;
    }
  }
  SScanLogicNode*         pTagScan = (SScanLogicNode*)nodesListGetNode(pVtableScan->node.pChildren, 0);
  SScanLogicNode*         pScan = (SScanLogicNode*)nodesListGetNode(pVtableScan->node.pChildren, 1);

  if (NULL == pTagScan || nodeType((SLogicNode*)pTagScan) != QUERY_NODE_LOGIC_PLAN_SCAN ||
      NULL == pScan || nodeType((SLogicNode*)pScan) != QUERY_NODE_LOGIC_PLAN_SCAN ||
      pTagScan->scanType != SCAN_TYPE_TAG || pScan->hasNormalCols) {
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

static int32_t vtableTagScanOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SDynQueryCtrlLogicNode* pDyn = (SDynQueryCtrlLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, vtableTagScanOptShouldBeOptimized, NULL);
  if (NULL == pDyn) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;
  SVirtualScanLogicNode*  pVtableScan = NULL;
  SScanLogicNode*         pTagScan = NULL;
  SScanLogicNode*         pScan = NULL;
  SNodeList*              pScanTargets = NULL;
  SNode*                  pNewTarget = NULL;
  bool                    freeTargets = false;
  bool                    freeTarget = false;

  pVtableScan = (SVirtualScanLogicNode*)nodesListGetNode(pDyn->node.pChildren, 0);
  QUERY_CHECK_NULL(pVtableScan, code, lino, _return, terrno);
  pTagScan = (SScanLogicNode*)nodesListGetNode(pVtableScan->node.pChildren, 0);
  QUERY_CHECK_NULL(pTagScan, code, lino, _return, terrno);
  pScan = (SScanLogicNode*)nodesListGetNode(pVtableScan->node.pChildren, 1);
  QUERY_CHECK_NULL(pScan, code, lino, _return, terrno);

  SLogicNode* pAgg = pDyn->node.pParent;
  if (NULL == pAgg->pParent) {
    PLAN_ERR_JRET(nodesMakeList(&pScanTargets));
    freeTargets = true;
    SNode* pAggTarget = NULL;
    FOREACH(pAggTarget, pAgg->pTargets) {
      SNode* pScanTarget = NULL;
      FOREACH(pScanTarget, pTagScan->node.pTargets) {
        if (0 == strcmp(((SColumnNode*)pAggTarget)->colName, ((SColumnNode*)pScanTarget)->colName)) {
          PLAN_ERR_JRET(nodesCloneNode(pScanTarget, &pNewTarget));
          freeTarget = true;
          PLAN_ERR_JRET(nodesListAppend(pScanTargets, pNewTarget));
          freeTarget = false;
          break;
        }
      }
    }
    nodesDestroyList(pTagScan->node.pTargets);
    pTagScan->node.pTargets = pScanTargets;
    freeTargets = false;
  }

  NODES_CLEAR_LIST(pVtableScan->node.pChildren);
  nodesDestroyNode((SNode*)pScan);
  nodesDestroyNode((SNode*)pDyn);
  NODES_CLEAR_LIST(pAgg->pChildren);
  PLAN_ERR_JRET(nodesListMakeAppend(&pAgg->pChildren, (SNode*)pTagScan));
  pTagScan->node.pParent = pAgg;
  pTagScan->onlyMetaCtbIdx = false;
  pTagScan->node.dynamicOp = false;
  pTagScan->virtualStableScan = false;

  pCxt->optimized = true;
_return:
  if (code) {
    planError("%s failed at %d, msg:%s", __func__, lino, tstrerror(code));
  }
  if (freeTargets) {
    nodesDestroyList(pScanTargets);
  }
  if (freeTarget) {
    nodesDestroyNode(pNewTarget);
  }
  return code;
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

static bool dynQueryCtrlSupportsLimitPushdown(SDynQueryCtrlLogicNode* pDyn) {
  return DYN_QTYPE_VTB_TS_SCAN == pDyn->qType || DYN_QTYPE_VTB_SCAN == pDyn->qType;
}

static bool limitBelongsToDescPriKeySort(SLogicNode* pNodeWithLimit, SLogicNode* pNodeLimitPushTo) {
  if (QUERY_NODE_LOGIC_PLAN_SORT != nodeType(pNodeWithLimit) ||
      QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL != nodeType(pNodeLimitPushTo)) {
    return false;
  }
  SSortLogicNode* pSort = (SSortLogicNode*)pNodeWithLimit;
  if (1 != LIST_LENGTH(pSort->pSortKeys)) {
    return false;
  }
  SOrderByExprNode* pOrderBy = (SOrderByExprNode*)nodesListGetNode(pSort->pSortKeys, 0);
  return ORDER_DESC == pOrderBy->order && QUERY_NODE_COLUMN == nodeType(pOrderBy->pExpr) &&
         isPrimaryKeyImpl(pOrderBy->pExpr);
}

static int32_t pushDownLimitHow(SLogicNode* pNodeWithLimit, SLogicNode* pNodeLimitPushTo, bool* pPushed);
static int32_t pushDownLimitTo(SLogicNode* pNodeWithLimit, SLogicNode* pNodeLimitPushTo, bool* pPushed) {
  int32_t code = 0;
  bool    cloned;
  switch (nodeType(pNodeLimitPushTo)) {
    case QUERY_NODE_LOGIC_PLAN_WINDOW: {
      SWindowLogicNode* pWindow = (SWindowLogicNode*)pNodeLimitPushTo;
      if (pWindow->winType != WINDOW_TYPE_INTERVAL) break;
      code = cloneLimit(pNodeWithLimit, pNodeLimitPushTo, CLONE_LIMIT_SLIMIT, &cloned);
      if (TSDB_CODE_SUCCESS == code) {
        *pPushed = cloned;
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
      bool pushed = cloned;
      FOREACH(pChild, pNodeLimitPushTo->pChildren) {
        bool childPushed = false;
        code = pushDownLimitHow(pNodeLimitPushTo, (SLogicNode*)pChild, &childPushed);
        if (TSDB_CODE_SUCCESS != code) {
          return code;
        }
        pushed = pushed || childPushed;
      }
      *pPushed = pushed;
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
      // FQ guard: do not push LIMIT into external scan; fqPushdownOptimize will harvest it.
      if (SCAN_TYPE_EXTERNAL == ((SScanLogicNode*)pNodeLimitPushTo)->scanType) {
        break;
      }
      if (nodeType(pNodeWithLimit) == QUERY_NODE_LOGIC_PLAN_PROJECT && limitHasFiniteRows(pNodeWithLimit->pLimit)) {
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
    case QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL: {
      SDynQueryCtrlLogicNode* pDyn = (SDynQueryCtrlLogicNode*)pNodeLimitPushTo;
      if (!dynQueryCtrlSupportsLimitPushdown(pDyn) || limitBelongsToDescPriKeySort(pNodeWithLimit, pNodeLimitPushTo)) {
        break;
      }
      code = cloneLimit(pNodeWithLimit, pNodeLimitPushTo, CLONE_LIMIT, &cloned);
      if (TSDB_CODE_SUCCESS == code) {
        *pPushed = cloned;
      }
      return code;
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
      if (sortPriKeyOptIsPriKeyOrderBy(pSort->pSortKeys) ||
          (QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL == nodeType(pNodeLimitPushTo) &&
           sortPriKeyOptHasAscPriKeyPrefix(pSort->pSortKeys))) {
        return pushDownLimitTo(pNodeWithLimit, pNodeLimitPushTo, pPushed);
      }
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
  bool    pushed = false;
  int32_t code = pushDownLimitHow(pNode, pChild, &pushed);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  if (pushed) {
    pCxt->optimized = true;
  }
  return TSDB_CODE_SUCCESS;
}

/*----------------------------------.
|   Table Count Scan Optimization   |
`----------------------------------*/

typedef struct STbCntScanOptInfo {
  SAggLogicNode*  pAgg;
  SScanLogicNode* pScan;
  SName           table;
} STbCntScanOptInfo;

/**
  Check whether the group keys are eligible 
  for table count scan optimization.
  The group keys should be db_name and/or stable_name only.
*/
static bool tbCntScanOptIsEligibleGroupKeys(SNodeList* pGroupKeys) {
  if (NULL == pGroupKeys) {
    return true;
  }

  SNode* pGroupKey = NULL;
  FOREACH(pGroupKey, pGroupKeys) {
    SNode* pKey = nodesListGetNode(
        ((SGroupingSetNode*)pGroupKey)->pParameterList, 0);
    if (QUERY_NODE_COLUMN != nodeType(pKey)) {
      return false;
    }
    SColumnNode* pCol = (SColumnNode*)pKey;
    if (0 != strcmp(pCol->colName, "db_name") &&
        0 != strcmp(pCol->colName, "stable_name")) {
      return false;
    }
  }

  return true;
}

/**
  Check whether the expression is on non-nullable columns.
*/
static bool tbCntScanOptNotNullableExpr(SNode* pNode) {
  if (QUERY_NODE_COLUMN != nodeType(pNode)) {
    return false;
  }
  const char* pColName = ((SColumnNode*)pNode)->colName;
  return 0 == strcmp(pColName, "*") ||
         0 == strcmp(pColName, "table_name") ||
         0 == strcmp(pColName, "db_name");
}

/**
  Check whether all aggregation functions are eligible 
  for table count scan optimization.
  The aggregation functions should satisfy:
    1. function type is COUNT
    2. parameter is on non-nullable columns
*/
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

/**
  Check whether the aggregation node is eligible 
  for table count scan optimization.
  The aggregation node should satisfy:
    1. group keys are db_name and/or stable_name only
    2. all aggregation functions are COUNT on non-nullable columns
*/
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

/**
  Check whether the logic condition is eligible for optimization.
  The condition should be in the form of:
    db_name = 'xxx' AND stable_name = 'yyy'
*/
static bool tbCntScanOptIsEligibleLogicCond(STbCntScanOptInfo* pInfo, 
                                            SLogicConditionNode* pCond) {
  if (LOGIC_COND_TYPE_AND != pCond->condType) {
    return false;
  }

  bool         hasDbCond = false;
  bool         hasStbCond = false;
  SColumnNode* pCol = NULL;
  SValueNode*  pVal = NULL;
  SNode*       pNode = NULL;
  FOREACH(pNode, pCond->pParameterList) {
    if (QUERY_NODE_OPERATOR != nodeType(pNode) ||
        !tbCntScanOptGetColValFromCond((SOperatorNode*)pNode, &pCol, &pVal)) {
      return false;
    }
    if (!hasDbCond && 0 == strcmp(pCol->colName, "db_name")) {
      hasDbCond = true;
      tstrncpy(pInfo->table.dbname, pVal->literal, TSDB_DB_NAME_LEN);
    } else if (!hasStbCond && 0 == strcmp(pCol->colName, "stable_name")) {
      hasStbCond = true;
      tstrncpy(pInfo->table.tname, pVal->literal, TSDB_TABLE_NAME_LEN);
    } else {
      return false;
    }
  }
  return hasDbCond && hasStbCond;
}

/**
  Check whether the operator condition is eligible
  for table count scan optimization.
  The condition should be in the form of:
    db_name = 'xxx',
  or
    stable_name = 'yyy'
*/
static bool tbCntScanOptIsEligibleOpCond(STbCntScanOptInfo* pInfo,
                                         SOperatorNode* pCond) {
  SColumnNode* pCol = NULL;
  SValueNode*  pVal = NULL;
  if (!tbCntScanOptGetColValFromCond(pCond, &pCol, &pVal)) {
    return false;
  }
  if (0 == strcmp(pCol->colName, "db_name")) {
    tstrncpy(pInfo->table.dbname, pVal->literal, TSDB_DB_NAME_LEN);
    return true;
  }
  if (0 == strcmp(pCol->colName, "stable_name")) {
    tstrncpy(pInfo->table.tname, pVal->literal, TSDB_TABLE_NAME_LEN);
    return true;
  }
  return false;
}

/**
  Check whether the conditions are eligible
  for table count scan optimization.
  The conditions should be in the form of:
    db_name = 'xxx' AND stable_name = 'yyy',
  or
    db_name = 'xxx',
  or
    stable_name = 'yyy'
*/
static bool tbCntScanOptIsEligibleConds(STbCntScanOptInfo* pInfo,
                                        SNode* pConditions) {
  if (NULL == pConditions) {
    return true;
  }

  if (LIST_LENGTH(pInfo->pAgg->pGroupKeys) != 0) {
    return false;
  }

  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pConditions)) {
    return tbCntScanOptIsEligibleLogicCond(pInfo,
                                           (SLogicConditionNode*)pConditions);
  }

  if (QUERY_NODE_OPERATOR == nodeType(pConditions)) {
    return tbCntScanOptIsEligibleOpCond(pInfo, (SOperatorNode*)pConditions);
  }

  return false;
}

/**
  Check whether the scan node is eligible 
  for table count scan optimization.
*/
static bool tbCntScanOptIsEligibleScan(STbCntScanOptInfo* pInfo) {
  // only support information_schema.ins_tables
  // don't add any other system tables here
  if (strcmp(pInfo->pScan->tableName.dbname, TSDB_INFORMATION_SCHEMA_DB) != 0 ||
      strcmp(pInfo->pScan->tableName.tname, TSDB_INS_TABLE_TABLES) != 0 || 
      pInfo->pScan->pGroupTags != NULL) {
    return false;
  }
  return tbCntScanOptIsEligibleConds(pInfo, pInfo->pScan->node.pConditions);
}

/**
  Check whether the tb count scan optimization should be applied.
*/
static bool tbCntScanOptShouldBeOptimized(SLogicNode* pNode, STbCntScanOptInfo* pInfo) {
  if (QUERY_NODE_LOGIC_PLAN_AGG != nodeType(pNode) || 1 != LIST_LENGTH(pNode->pChildren) ||
      QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(nodesListGetNode(pNode->pChildren, 0))) {
    return false;
  }

  pInfo->pAgg = (SAggLogicNode*)pNode;
  pInfo->pScan = (SScanLogicNode*)nodesListGetNode(pNode->pChildren, 0);
  return tbCntScanOptIsEligibleAgg(pInfo->pAgg) && tbCntScanOptIsEligibleScan(pInfo);
}

/**
  Create the function node of _table_count.
*/
static int32_t tbCntScanOptCreateTableCountFunc(SNode** ppNode) {
  SFunctionNode* pFunc = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }
  tstrncpy(pFunc->functionName, "_table_count", TSDB_FUNC_NAME_LEN);
  tstrncpy(pFunc->node.aliasName, "_table_count", TSDB_COL_NAME_LEN);
  code = fmGetFuncInfo(pFunc, NULL, 0);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pFunc);
    return code;
  }
  *ppNode = (SNode*)pFunc;
  return code;
}

/**
  Rewrite the scan node to table count scan.
*/
static int32_t tbCntScanOptRewriteScan(STbCntScanOptInfo* pInfo) {
  pInfo->pScan->scanType = SCAN_TYPE_TABLE_COUNT;
  tstrncpy(pInfo->pScan->tableName.dbname,
           pInfo->table.dbname, TSDB_DB_NAME_LEN);
  tstrncpy(pInfo->pScan->tableName.tname,
           pInfo->table.tname, TSDB_TABLE_NAME_LEN);
  NODES_DESTORY_LIST(pInfo->pScan->node.pTargets);
  NODES_DESTORY_LIST(pInfo->pScan->pScanCols);
  NODES_DESTORY_NODE(pInfo->pScan->node.pConditions);
  NODES_DESTORY_LIST(pInfo->pScan->pScanPseudoCols);
  SNode*  pNew = NULL;
  int32_t code = tbCntScanOptCreateTableCountFunc(&pNew);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pInfo->pScan->pScanPseudoCols, pNew);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExpr(
        nodesListGetNode(pInfo->pScan->pScanPseudoCols, 0),
        &pInfo->pScan->node.pTargets);
  }
  SNode* pGroupKey = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    FOREACH(pGroupKey, pInfo->pAgg->pGroupKeys) {
      SNode* pGroupCol = nodesListGetNode(
          ((SGroupingSetNode*)pGroupKey)->pParameterList, 0);
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

/**
  Create the function node of sum for table count scan.
*/
static int32_t tbCntScanOptCreateSumFunc(SFunctionNode* pCntFunc,
                                         SNode* pParam, SNode** pOutput) {
  SFunctionNode* pFunc = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }
  tstrncpy(pFunc->functionName, "sum", TSDB_FUNC_NAME_LEN);
  tstrncpy(pFunc->node.aliasName, pCntFunc->node.aliasName, TSDB_COL_NAME_LEN);
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

/**
  Rewrite the agg node to use sum function on table count scan.
*/
static int32_t tbCntScanOptRewriteAgg(SAggLogicNode* pAgg) {
  SScanLogicNode* pScan = (SScanLogicNode*)nodesListGetNode(pAgg->node.pChildren, 0);
  SNodeList*      pOrigAggFuncs = pAgg->pAggFuncs;
  SNode*          pSum = NULL;
  int32_t         code = tbCntScanOptCreateSumFunc((SFunctionNode*)nodesListGetNode(pOrigAggFuncs, 0),
                                                   nodesListGetNode(pScan->pScanPseudoCols, 0), &pSum);
  if (TSDB_CODE_SUCCESS == code) {
    pAgg->pAggFuncs = NULL;
    code = nodesListMakeStrictAppend(&pAgg->pAggFuncs, pSum);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tbCntScanOptRewriteGroupTagsToFuncs(pScan->pGroupTags, pAgg, pOrigAggFuncs);
  }
  nodesDestroyList(pOrigAggFuncs);
  NODES_DESTORY_LIST(pAgg->pGroupKeys);
  return code;
}

/**
  Apply table count scan optimization on the logic subplan.
*/
static int32_t tableCountScanOptimize(SOptimizeContext* pCxt,
                                      SLogicSubplan* pLogicSubplan) {
  STbCntScanOptInfo info = {0};
  if (!optFindEligibleNode(pLogicSubplan->pNode,
                           (FShouldBeOptimized)tbCntScanOptShouldBeOptimized,
                           &info)) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = tbCntScanOptRewriteScan(&info);
  if (TSDB_CODE_SUCCESS == code) {
    code = tbCntScanOptRewriteAgg(info.pAgg);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pCxt->optimized = true;
  }
  return code;
}

static SSortLogicNode* sortNonPriKeySatisfied(SLogicNode* pNode) {
  return NULL;
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
  int32_t    code = nodesMakeList(&pNodeList);
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
  if (pJoin->multiGroupJoin) {
    return JOIN_ALGO_HASH != pJoin->joinAlgo;
  }
  if (pJoin->joinAlgo != JOIN_ALGO_UNKNOWN) {
    return res;
  }

  if (!pJoin->hashJoinHint) {
    goto _return;
  }

  if ((JOIN_STYPE_NONE != pJoin->subType && JOIN_STYPE_OUTER != pJoin->subType) || JOIN_TYPE_FULL == pJoin->joinType ||
      pNode->pChildren->length != 2) {
    goto _return;
  }

  res = true;

_return:

  if (!res && DATA_ORDER_LEVEL_NONE == pJoin->node.requireDataOrder) {
    pJoin->node.requireDataOrder = DATA_ORDER_LEVEL_GLOBAL;
    int32_t* pCode = pCtx;
    int32_t  code = adjustLogicNodeDataRequirement(pNode, pJoin->node.requireDataOrder);
    if (TSDB_CODE_SUCCESS != code) {
      *pCode = code;
    }
  }

  return res;
}

static int32_t hashJoinOptSplitPrimFromLogicCond(SNode** pCondition, SNode** pPrimaryKeyCond) {
  SLogicConditionNode* pLogicCond = (SLogicConditionNode*)(*pCondition);
  int32_t              code = TSDB_CODE_SUCCESS;
  SNodeList*           pPrimaryKeyConds = NULL;
  SNode*               pCond = NULL;
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

  SNode* pTempPrimaryKeyCond = NULL;
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

int32_t hashJoinOptSplitPrimCond(SNode** pCondition, SNode** pPrimaryKeyCond) {
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(*pCondition)) {
    if (LOGIC_COND_TYPE_AND == ((SLogicConditionNode*)*pCondition)->condType) {
      return hashJoinOptSplitPrimFromLogicCond(pCondition, pPrimaryKeyCond);
    }

    return TSDB_CODE_SUCCESS;
  }

  bool    needOutput = false;
  bool    result = false;
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
  int32_t         code = TSDB_CODE_SUCCESS;

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
    TAOS_SET_OBJ_ALIGNED(&pJoin->timeRange, TSWINDOW_INITIALIZER);
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
        TAOS_SET_OBJ_ALIGNED(&pJoin->timeRange, TSWINDOW_DESC_INITIALIZER);
        break;
      } else {
        pJoin->timeRange.skey = TMAX(pJoin->timeRange.skey, pScan->scanRange.skey);
        pJoin->timeRange.ekey = TMIN(pJoin->timeRange.ekey, pScan->scanRange.ekey);
      }
    }
  }

  pJoin->timeRangeTarget = 0;

  if (!TSWINDOW_IS_EQUAL(pJoin->timeRange, TSWINDOW_INITIALIZER)) {
    SNode*  pChild = NULL;
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
  int32_t     code = 0;
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
  if (QUERY_NODE_LOGIC_PLAN_JOIN != nodeType(pNode) ||
      OPTIMIZE_FLAG_TEST_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_STB_JOIN)) {
    return false;
  }

  SJoinLogicNode* pJoin = (SJoinLogicNode*)pNode;
  if (pJoin->multiGroupJoin) {
    return false;
  }
  if (pJoin->joinAlgo == JOIN_ALGO_UNKNOWN) {
    pJoin->joinAlgo = JOIN_ALGO_MERGE;
  }

  if (JOIN_STYPE_NONE != pJoin->subType || pJoin->isSingleTableJoin || NULL == pJoin->pTagEqCond ||
      pNode->pChildren->length != 2 || pJoin->isLowLevelJoin) {
    return false;
  }

  SNode* pLeft = nodesListGetNode(pJoin->node.pChildren, 0);
  SNode* pRight = nodesListGetNode(pJoin->node.pChildren, 1);
  if (QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pLeft) || QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pRight)) {
    return false;
  }

  // FQ guard (defensive): ExternalScan must never have its scanType rewritten
  // to SCAN_TYPE_TAG. FqPushdown runs earlier in optimizeRuleSet[] and the
  // parser does not synthesize pTagEqCond for external tables, so this branch
  // is unreachable today, but the guard keeps the invariant explicit.
  if (SCAN_TYPE_EXTERNAL == ((SScanLogicNode*)pLeft)->scanType ||
      SCAN_TYPE_EXTERNAL == ((SScanLogicNode*)pRight)->scanType) {
    return false;
  }

  return true;
}

int32_t addFuncToScanNode(char* funcName, SScanLogicNode* pScan) {
  SFunctionNode* pUidFunc = NULL;
  int32_t        code = TSDB_CODE_SUCCESS;

  PLAN_ERR_JRET(createFunction(funcName, NULL, &pUidFunc));
  snprintf(pUidFunc->node.aliasName, sizeof(pUidFunc->node.aliasName), "%s.%p", pUidFunc->functionName, pUidFunc);
  PLAN_ERR_JRET(nodesListStrictAppend(pScan->pScanPseudoCols, (SNode*)pUidFunc));
  PLAN_ERR_JRET(createColumnByRewriteExpr((SNode*)pUidFunc, &pScan->node.pTargets));
  return code;
_return:
  planError("%s failed, code:%d", __func__, code);
  nodesDestroyNode((SNode*)pUidFunc);
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
  int32_t    code = nodesMakeList(&pTags);
  if (TSDB_CODE_SUCCESS == code) {
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
        if (0 == taosStrcasecmp(pTargetCol->node.aliasName, pTagCol->colName) &&
            0 == taosStrcasecmp(pTargetCol->tableAlias, pTagCol->tableAlias)) {
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
    code = addFuncToScanNode("_tbuid", pScan);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = addFuncToScanNode("_vgid", pScan);
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
  int32_t    code = nodesCloneList(pJoin->pChildren, &pList);
  if (NULL == pList) {
    return code;
  }

  SNode* pNode = NULL;
  SName* pPrev = NULL;
  FOREACH(pNode, pList) {
    code = stbJoinOptRewriteToTagScan(pJoin, pNode);
    if (code) {
      break;
    }

    SScanLogicNode* pScan = (SScanLogicNode*)pNode;
    // If join node's two children are scan on same table and on only one vgroup, don't need to split. Vice versa.
    if (pScan->pVgroupList && 1 == pScan->pVgroupList->numOfVgroups) {
      if (NULL == pPrev || 0 == strcmp(pPrev->dbname, pScan->tableName.dbname)) {
        pPrev = &pScan->tableName;
        continue;
      }

      pScan->needSplit = true;
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
  int32_t         code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_JOIN, (SNode**)&pJoin);
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
  if (TSDB_CODE_SUCCESS != code) {
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

static int32_t stbJoinOptCreateTableScanNodes(SOptimizeContext* pCxt, SLogicNode* pJoin, SNodeList** ppList, bool* srcScan) {
  SNodeList* pList = NULL;
  int32_t    code = nodesCloneList(pJoin->pChildren, &pList);
  if (NULL == pList) {
    return code;
  }

  int32_t i = 0;
  SName* pPrev = NULL;
  SNode*  pNode = NULL;
  FOREACH(pNode, pList) {
    SScanLogicNode* pScan = (SScanLogicNode*)pNode;
    // code = addFuncToScanNode("_tbuid", pScan);
    // if (code) {
    //   break;
    // }

    nodesDestroyNode(pScan->pTagCond);
    pScan->pTagCond = NULL;
    nodesDestroyNode(pScan->pTagIndexCond);
    pScan->pTagIndexCond = NULL;

    pScan->node.dynamicOp = true;
    *(srcScan + i++) = (pScan->pVgroupList->numOfVgroups <= 1 && !inStreamCalcClause(pCxt->pPlanCxt));

    pScan->scanType = SCAN_TYPE_TABLE;

    if (pScan->pVgroupList && 1 == pScan->pVgroupList->numOfVgroups) {
      if (NULL == pPrev || 0 == strcmp(pPrev->dbname, pScan->tableName.dbname)) {
        pPrev = &pScan->tableName;
        continue;
      }

      pScan->needSplit = true;
      *(srcScan + i - 1) = false;
    }
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
    SNodeList*      pNewList = NULL;
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
    SNode*               pNode = NULL;
    FOREACH(pNode, pLogic->pParameterList) {
      if (nodesEqualNode(pNode, pJoin->pTagEqCond)) {
        ERASE_NODE(pLogic->pParameterList);
        break;
      } else if (QUERY_NODE_LOGIC_CONDITION == nodeType(pJoin->pTagEqCond)) {
        SLogicConditionNode* pTags = (SLogicConditionNode*)pJoin->pTagEqCond;
        SNode*               pTag = NULL;
        bool                 found = false;
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
  int32_t         code = nodesCloneNode((SNode*)pOrig, (SNode**)&pJoin);
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
  }

  if (TSDB_CODE_SUCCESS == code) {
    pDynCtrl->stbJoin.pVgList = NULL;
    code = nodesMakeList(&pDynCtrl->stbJoin.pVgList);
  }

  if (TSDB_CODE_SUCCESS == code) {
    pDynCtrl->stbJoin.pUidList = NULL;
    code = nodesMakeList(&pDynCtrl->stbJoin.pUidList);
  }

  SJoinLogicNode* pHJoin = (SJoinLogicNode*)pPrev;
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListStrictAppend(pDynCtrl->stbJoin.pUidList, nodesListGetNode(pHJoin->node.pTargets, 0));
  }
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
    code = stbJoinOptCreateTableScanNodes(pCxt, pJoin, &pTbScanNodes, srcScan);
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

static int32_t grpJoinOptCreatePartitionNode(SLogicNode* pParent, SLogicNode* pChild, bool leftChild,
                                             SLogicNode** pNew) {
  SPartitionLogicNode* pPartition = NULL;
  int32_t              code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_PARTITION, (SNode**)&pPartition);
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
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pPartition);
    return code;
  }
  code = nodesListMakeStrictAppend(&pPartition->node.pChildren, (SNode*)pChild);
  if (TSDB_CODE_SUCCESS == code) {
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
  SNode*  pNode = NULL;
  SNode*  pNew = NULL;
  bool    leftChild = true;
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
  int32_t         code = TSDB_CODE_SUCCESS;
  SNode*          pChild = NULL;
  SNode*          pNew = NULL;
  bool            leftChild = true;
  SJoinLogicNode* pJoin = (SJoinLogicNode*)pNode;
  FOREACH(pChild, pNode->pChildren) {
    if (QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pChild)) {
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }

    SScanLogicNode* pScan = (SScanLogicNode*)pChild;
    SNodeList*      pNewList = NULL;
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
  int32_t         code = (pJoin->allEqTags && !pJoin->hasSubQuery && !pJoin->batchScanHint)
                             ? grpJoinOptPartByTags(pNode)
                             : grpJoinOptInsertPartitionNode(pNode);
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

static bool partColOptHasSortKey(const SSortLogicNode* pSort, const SNode* pExpr) {
  SNode* pNode = NULL;
  FOREACH(pNode, pSort->pSortKeys) {
    if (nodesEqualNode(((SOrderByExprNode*)pNode)->pExpr, pExpr)) {
      return true;
    }
  }
  return false;
}

static int32_t partColOptAppendStateWindowOrderKeys(SPartitionLogicNode* pPartition, SSortLogicNode* pSort) {
  SLogicNode* pParent = pPartition->node.pParent;
  EOrder      order = pPartition->node.outputTsOrder;
  if (NULL == pParent || QUERY_NODE_LOGIC_PLAN_WINDOW != nodeType(pParent) ||
      WINDOW_TYPE_STATE != ((SWindowLogicNode*)pParent)->winType) {
    return TSDB_CODE_SUCCESS;
  }
  if (ORDER_ASC != order && ORDER_DESC != order) {
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  if (1 != LIST_LENGTH(pPartition->node.pChildren)) {
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  SNode* pChild = nodesListGetNode(pPartition->node.pChildren, 0);
  if (QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pChild)) {
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  SNode*          pTs = NULL;
  SNode*          pPk = NULL;
  SScanLogicNode* pScan = (SScanLogicNode*)pChild;
  int32_t         code = stbSplFindPrimaryKeyFromScan(pScan, &pTs);
  if (TSDB_CODE_SUCCESS == code && NULL == pTs) {
    code = TSDB_CODE_PLAN_INTERNAL_ERROR;
  }
  if (TSDB_CODE_SUCCESS == code && !partColOptHasSortKey(pSort, pTs)) {
    code = stbSplCreateMergeKeysByExpr(pTs, order, &pSort->pSortKeys);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = stbSplFindPkFromScan(pScan, &pPk);
  }
  if (TSDB_CODE_SUCCESS == code && NULL != pPk && !partColOptHasSortKey(pSort, pPk)) {
    code = stbSplCreateMergeKeysByExpr(pPk, order, &pSort->pSortKeys);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pSort->node.inputTsOrder = pPartition->node.inputTsOrder;
    pSort->node.outputTsOrder = order;
    // Timestamp and second-PK suffixes order rows within a business partition.
    pSort->numOfGroupKeys = LIST_LENGTH(pPartition->pPartitionKeys);
  }
  return code;
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
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
      if (QUERY_NODE_COLUMN == nodeType(node) && ((SColumnNode*)node)->colId == pPartition->pkTsColId &&
          ((SColumnNode*)node)->tableId == pPartition->pkTsColTbId)
        alreadyPartByPKTs = true;
      code = nodesListMakeStrictAppend(&pSort->pSortKeys, (SNode*)pOrder);
      if (TSDB_CODE_SUCCESS == code) {
        pOrder->order = ORDER_ASC;
        pOrder->pExpr = NULL;
        pOrder->nullOrder = NULL_ORDER_FIRST;
        code = nodesCloneNode(node, &pOrder->pExpr);
      }
      if (TSDB_CODE_SUCCESS != code) {
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
        if (TSDB_CODE_SUCCESS == code) {
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
    if (TSDB_CODE_SUCCESS == code) {
      code = partColOptAppendStateWindowOrderKeys(pPartition, pSort);
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

    if (nodeType(pParent) == QUERY_NODE_LOGIC_PLAN_WINDOW) {
      SWindowLogicNode* pWindow = (SWindowLogicNode*)pParent;
      if (pWindow->winType == WINDOW_TYPE_INTERVAL && pWindow->offset == AUTO_DURATION_VALUE) {
        SLogicNode* pRootNode = getLogicNodeRootNode(pParent);
        // When using interval auto offset, tsma optimization cannot take effect.
        // Unless the user explicitly specifies not to use tsma, an error should be reported.
        if (!getOptHint(pRootNode->pHint, HINT_SKIP_TSMA)) {
          planError("%s failed since tsma optimization cannot be applied with interval auto offset", __func__);
          *(int32_t*)pCtx = TSDB_CODE_TSMA_INVALID_AUTO_OFFSET;
          return false;
        }
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

static int32_t fillTSMAOptCtx(STSMAOptCtx* pTsmaOptCtx, SScanLogicNode* pScan, void* tz) {
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
    pTsmaOptCtx->queryInterval->timezone = tz;
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
  SNode* pNode;
  bool   failed = false, found = false;

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
  bool                  ok;
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
  STsmaOptTagCheckCtx   ctx = {.pTsma = pTsma, .ok = true};
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
  SArray* pTsmaScanCols = NULL;
  int32_t code = 0;

  for (int32_t i = 0; i < pTsmaOptCtx->pTsmas->size; ++i) {
    if (!pTsmaScanCols) {
      pTsmaScanCols = taosArrayInit(pTsmaOptCtx->pAggFuncs->length, sizeof(int32_t));
      if (!pTsmaScanCols) return terrno;
    }
    if (pTsmaOptCtx->pScan->tableType == TSDB_CHILD_TABLE || pTsmaOptCtx->pScan->tableType == TSDB_NORMAL_TABLE) {
      const STsmaTargetTbInfo* ptbInfo = taosArrayGet(pTsmaOptCtx->pScan->pTsmaTargetTbInfo, i);
      if (ptbInfo->uid == 0)
        continue;  // tsma res table meta not found, skip this tsma, this is possible when there is no data in this ctb
    }

    STableTSMAInfo* pTsma = taosArrayGetP(pTsmaOptCtx->pTsmas, i);
    if (!pTsma->fillHistoryFinished ||
        tsMaxTsmaCalcDelay * 1000 < (pTsma->rspTs - pTsma->reqTs) + pTsma->delayDuration) {
      continue;
    }
    // filter with interval
    if (!tsmaOptCheckValidInterval(pTsma->interval, pTsma->unit, pTsmaOptCtx)) {
      continue;
    }
    // filter with funcs, note that tsma funcs has been sorted by funcId and ColId
    bool    valid = false;
    int32_t code = tsmaOptCheckValidFuncs(pTsma->pFuncs, pTsmaOptCtx->pAggFuncs, pTsmaScanCols, &valid);
    if (TSDB_CODE_SUCCESS != code) break;
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
  const int64_t             factors[3] = {NANOSECOND_PER_MSEC, NANOSECOND_PER_USEC, 1};
  const STSMAOptUsefulTsma *p = pLeft, *q = pRight;
  int64_t                   pInterval = p->pTsma->interval, qInterval = q->pTsma->interval;
  int8_t                    pUnit = p->pTsma->unit, qUnit = q->pTsma->unit;
  if (TIME_UNIT_MONTH == pUnit) {
    pInterval = pInterval * 31 * (NANOSECOND_PER_DAY / factors[p->precision]);
  } else if (TIME_UNIT_YEAR == pUnit) {
    pInterval = pInterval * 365 * (NANOSECOND_PER_DAY / factors[p->precision]);
  }
  if (TIME_UNIT_MONTH == qUnit) {
    qInterval = qInterval * 31 * (NANOSECOND_PER_DAY / factors[q->precision]);
  } else if (TIME_UNIT_YEAR == qUnit) {
    qInterval = qInterval * 365 * (NANOSECOND_PER_DAY / factors[q->precision]);
  }

  if (pInterval > qInterval) return -1;
  if (pInterval < qInterval) return 1;
  return 0;
}

static void tsmaOptInitIntervalFromTsma(SInterval* pInterval, const STableTSMAInfo* pTsma, int8_t precision, void* tz) {
  pInterval->interval = pTsma->interval;
  pInterval->intervalUnit = pTsma->unit;
  pInterval->sliding = pTsma->interval;
  pInterval->slidingUnit = pTsma->unit;
  pInterval->offset = 0;
  pInterval->offsetUnit = pTsma->unit;
  pInterval->precision = precision;
  pInterval->timezone = tz;
}

static const STSMAOptUsefulTsma* tsmaOptFindUsefulTsma(const SArray* pUsefulTsmas, int32_t startIdx,
                                                       int64_t startAlignInterval, int64_t endAlignInterval,
                                                       int8_t precision, void* tz) {
  SInterval tsmaInterval;
  for (int32_t i = startIdx; i < pUsefulTsmas->size; ++i) {
    const STSMAOptUsefulTsma* pUsefulTsma = taosArrayGet(pUsefulTsmas, i);
    tsmaOptInitIntervalFromTsma(&tsmaInterval, pUsefulTsma->pTsma, precision, tz);
    if (taosTimeTruncate(startAlignInterval, &tsmaInterval) == startAlignInterval &&
        taosTimeTruncate(endAlignInterval, &tsmaInterval) == endAlignInterval) {
      return pUsefulTsma;
    }
  }
  return NULL;
}

static int32_t tsmaOptSplitWindows(STSMAOptCtx* pTsmaOptCtx, const STimeWindow* pScanRange, void* tz) {
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
    tsmaOptInitIntervalFromTsma(&interval, pTsma, pTsmaOptCtx->precision, tz);
    pInterval = &interval;
  }

  tsmaOptInitIntervalFromTsma(&tsmaInterval, pTsma, pTsmaOptCtx->precision, tz);

  // check for head windows
  if (pScanRange->skey != TSKEY_MIN) {
    startOfSkeyFirstWin = taosTimeTruncate(pScanRange->skey, pInterval);
    endOfSkeyFirstWin = taosTimeGetIntervalEnd(startOfSkeyFirstWin, pInterval) + 1;
    isSkeyAlignedWithTsma = taosTimeTruncate(pScanRange->skey, &tsmaInterval) == pScanRange->skey;
  } else {
    endOfSkeyFirstWin = TSKEY_MIN;
  }

  // check for tail windows
  if (pScanRange->ekey != TSKEY_MAX) {
    startOfEkeyFirstWin = taosTimeTruncate(pScanRange->ekey, pInterval);
    endOfEkeyFirstWin = taosTimeGetIntervalEnd(startOfEkeyFirstWin, pInterval) + 1;
    isEkeyAlignedWithTsma = taosTimeTruncate(pScanRange->ekey + 1, &tsmaInterval) == (pScanRange->ekey + 1);
    if (startOfEkeyFirstWin > startOfSkeyFirstWin) {
      needTailWindow = true;
    }
  }

  // add head tsma if possible
  if (!isSkeyAlignedWithTsma) {
    scanRange.ekey = TMIN(
        scanRange.ekey,
        taosTimeAdd(startOfSkeyFirstWin, pInterval->interval * 1, pInterval->intervalUnit, pTsmaOptCtx->precision, tz) - 1);
    const STSMAOptUsefulTsma* pTsmaFound =
        tsmaOptFindUsefulTsma(pTsmaOptCtx->pUsefulTsmas, 1, scanRange.skey, scanRange.ekey + 1, pTsmaOptCtx->precision, tz);
    STSMAOptUsefulTsma usefulTsma = {.pTsma = pTsmaFound ? pTsmaFound->pTsma : NULL,
                                     .scanRange = scanRange,
                                     .pTsmaScanCols = pTsmaFound ? pTsmaFound->pTsmaScanCols : NULL};
    if (NULL == taosArrayPush(pTsmaOptCtx->pUsedTsmas, &usefulTsma)) return terrno;
  }

  // the main tsma
  if (endOfSkeyFirstWin < startOfEkeyFirstWin ||
      (endOfSkeyFirstWin == startOfEkeyFirstWin && (isSkeyAlignedWithTsma || isEkeyAlignedWithTsma))) {
    scanRange.ekey = TMIN(pScanRange->ekey, isEkeyAlignedWithTsma ? pScanRange->ekey : startOfEkeyFirstWin - 1);
    if (!isSkeyAlignedWithTsma) {
      scanRange.skey = endOfSkeyFirstWin;
    }
    STSMAOptUsefulTsma usefulTsma = {
        .pTsma = pTsma, .scanRange = scanRange, .pTsmaScanCols = pUsefulTsma->pTsmaScanCols};
    if (NULL == taosArrayPush(pTsmaOptCtx->pUsedTsmas, &usefulTsma)) return terrno;
  }

  // add tail tsma if possible
  if (!isEkeyAlignedWithTsma && needTailWindow) {
    scanRange.skey = startOfEkeyFirstWin;
    scanRange.ekey = pScanRange->ekey;
    const STSMAOptUsefulTsma* pTsmaFound =
        tsmaOptFindUsefulTsma(pTsmaOptCtx->pUsefulTsmas, 1, scanRange.skey - startOfEkeyFirstWin,
                              scanRange.ekey + 1 - startOfEkeyFirstWin, pTsmaOptCtx->precision, tz);
    STSMAOptUsefulTsma usefulTsma = {.pTsma = pTsmaFound ? pTsmaFound->pTsma : NULL,
                                     .scanRange = scanRange,
                                     .pTsmaScanCols = pTsmaFound ? pTsmaFound->pTsmaScanCols : NULL};
    if (NULL == taosArrayPush(pTsmaOptCtx->pUsedTsmas, &usefulTsma)) return terrno;
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
      tstrncpy(pCol->tableName, pTsma->targetTbName, TSDB_TABLE_NAME_LEN);
      tstrncpy(pCol->colName, pFunc->node.aliasName, TSDB_COL_NAME_LEN);
      tstrncpy(pCol->node.aliasName, pFunc->node.aliasName, TSDB_COL_NAME_LEN);
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
      tstrncpy(pTagCol->tableName, pTsma->targetTbName, TSDB_TABLE_NAME_LEN);
      tstrncpy(pTagCol->tableAlias, pTsma->targetTbName, TSDB_TABLE_NAME_LEN);
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
  int32_t    code = 0;
  SExprNode* pRewrittenFunc = NULL;
  code = nodesMakeNode(pTsma ? QUERY_NODE_COLUMN : QUERY_NODE_FUNCTION, (SNode**)&pRewrittenFunc);
  SValueNode* pValue = NULL;
  if (code == TSDB_CODE_SUCCESS) {
    pRewrittenFunc->resType = ((SExprNode*)(*pTbNameNode))->resType;
  }

  if (pTsma && code == TSDB_CODE_SUCCESS) {
    nodesDestroyNode(*pTbNameNode);
    SColumnNode*   pCol = (SColumnNode*)pRewrittenFunc;
    const SSchema* pSchema = taosArrayGet(pTsma->pTsma->pTags, pTsma->pTsma->pTags->size - 1);
    tstrncpy(pCol->tableName, pTsma->targetTbName, TSDB_TABLE_NAME_LEN);
    tstrncpy(pCol->tableAlias, pTsma->targetTbName, TSDB_TABLE_NAME_LEN);
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

static int32_t tsmaOptRewriteNode(SNode** pNode, STSMAOptCtx* pCtx, const STSMAOptUsefulTsma* pTsma, bool rewriteTbName,
                                  bool rewriteTag) {
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
    } else if (pPkTsCol) {
      nodesDestroyNode((SNode*)pPkTsCol);
    }
    if (code == TSDB_CODE_SUCCESS) {
      pNewScan->stableId = pTsma->pTsma->destTbUid;
      pNewScan->tableId = pTsma->targetTbUid;
      tstrncpy(pNewScan->tableName.tname, pTsma->targetTbName, TSDB_TABLE_NAME_LEN);
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
    if (TSDB_CODE_SUCCESS == code) {
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
          tstrncpy(pTsma->targetTbName, ptbInfo->tableName, TSDB_TABLE_NAME_LEN);
          pTsma->targetTbUid = ptbInfo->uid;
        }
      }
    } else {
      tstrncpy(pTsma->targetTbName, pTsma->pTsma->targetTb, TSDB_TABLE_NAME_LEN);
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
    const STSMAOptUsefulTsma* pTsma = taosArrayGet(pCtx->pUsedTsmas, i);
    if (pTsma->pTsma) return true;
  }
  return false;
}

static int32_t tsmaOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  int32_t         code = 0;
  STSMAOptCtx     tsmaOptCtx = {0};
  SScanLogicNode* pScan = (SScanLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, tsmaOptMayBeOptimized, &code);
  if (!pScan) return code;

  SLogicNode* pRootNode = getLogicNodeRootNode((SLogicNode*)pScan);
  if (getOptHint(pRootNode->pHint, HINT_SKIP_TSMA)) return code;

  code = fillTSMAOptCtx(&tsmaOptCtx, pScan, pCxt->pPlanCxt->timezone);
  if (code == TSDB_CODE_SUCCESS) {
    // 1. extract useful tsmas
    code = tsmaOptFilterTsmas(&tsmaOptCtx);

    if (code == TSDB_CODE_SUCCESS && tsmaOptCtx.pUsefulTsmas->size > 0) {
      // 2. sort useful tsmas with interval
      taosArraySort(tsmaOptCtx.pUsefulTsmas, tsmaInfoCompWithIntervalDesc);
      // 3. split windows
      code = tsmaOptSplitWindows(&tsmaOptCtx, tsmaOptCtx.pTimeRange, pCxt->pPlanCxt->timezone);
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

static bool eliminateVirtualScanMayBeOptimized(SLogicNode* pNode, void* pCtx) {
  if (NULL == pNode->pParent) {
    return false;
  }

  if (nodeType(pNode) != QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN || LIST_LENGTH(pNode->pChildren) != 1) {
    return false;
  }

  if (nodeType(pNode->pParent) == QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL) {
    return false;
  }

  if (nodeHasExternalScan(pNode)) {
    return false;
  }

  if (pNode->pConditions || ((SVirtualScanLogicNode *)pNode)->pScanPseudoCols) {
    return false;
  }

  // Don't eliminate if the child scan references a virtual table  -  it needs its own
  // Virtual Table Scan  ->  DynQueryCtrl resolution chain at runtime.
  SNode* pChild = nodesListGetNode(pNode->pChildren, 0);
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pChild)) {
    int8_t childTableType = ((SScanLogicNode*)pChild)->tableType;
    if (TSDB_VIRTUAL_CHILD_TABLE == childTableType || TSDB_VIRTUAL_NORMAL_TABLE == childTableType) {
      return false;
    }
    if (((SScanLogicNode*)pChild)->scanType != SCAN_TYPE_EXTERNAL &&
        ((SScanLogicNode*)pChild)->node.precision != pNode->precision) {
      return false;
    }
  }

  SVirtualScanLogicNode* pVScan = (SVirtualScanLogicNode *)pNode;
  SNode*                 pCol;
  FOREACH(pCol, pVScan->pScanCols) {
    SColumnNode* pScanCol = (SColumnNode* )pCol;
    if (!pScanCol->hasRef) {
      // if col don't have ref, it can't be eliminated. If the vscan operator is eliminated,
      // the upper operator can't find the right slot id.
      return false;
    }
  }
  return true;
}

static int32_t eliminateVirtualScanOptimizeImpl(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan,
                                                SLogicNode* pVirtualScanNode) {
  SNode* pTargets = NULL;
  SNode* pSibling = NULL;

  FOREACH(pSibling, pVirtualScanNode->pParent->pChildren) {
    if (nodesEqualNode(pSibling, (SNode*)pVirtualScanNode)) {
      SNode* pChild;
      FOREACH(pChild, pVirtualScanNode->pChildren) {
        // clear the mask to try scanPathOptimize again.
        OPTIMIZE_FLAG_CLEAR_MASK(((SScanLogicNode*)pChild)->node.optimizedFlag, OPTIMIZE_FLAG_SCAN_PATH);
        OPTIMIZE_FLAG_SET_MASK(((SScanLogicNode*)pChild)->node.optimizedFlag, OPTIMIZE_FLAG_ELIMINATE_VSCAN);
        ((SLogicNode*)pChild)->pParent = pVirtualScanNode->pParent;
      }
      INSERT_LIST(pVirtualScanNode->pParent->pChildren, pVirtualScanNode->pChildren);
      pVirtualScanNode->pChildren = NULL;

      ERASE_NODE(pVirtualScanNode->pParent->pChildren);
      pCxt->optimized = true;
      return TSDB_CODE_SUCCESS;
    }
  }

  return TSDB_CODE_PLAN_INTERNAL_ERROR;
}

// eliminate virtual scan node when it has only one child
static int32_t eliminateVirtualScanOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  if (inStreamCalcClause(pCxt->pPlanCxt)) {
    // Stream calc plans still depend on the virtual scan's column mapping.
    return TSDB_CODE_SUCCESS;
  }
  SLogicNode* pVirtualScanNode = optFindPossibleNode(pLogicSubplan->pNode, eliminateVirtualScanMayBeOptimized, NULL);
  if (NULL == pVirtualScanNode) {
    return TSDB_CODE_SUCCESS;
  }

  return eliminateVirtualScanOptimizeImpl(pCxt, pLogicSubplan, pVirtualScanNode);
}

static bool pdaMayBeOptimized(SLogicNode* pNode, void* pCtx) {
  if (QUERY_NODE_LOGIC_PLAN_AGG != nodeType(pNode) || 1 != LIST_LENGTH(pNode->pChildren) ||
      QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN != nodeType(nodesListGetNode(pNode->pChildren, 0))) {
    return false;
  }

  // virtual table scan should have more than one child.
  if (LIST_LENGTH(((SVirtualScanLogicNode *)nodesListGetNode(pNode->pChildren, 0))->node.pChildren) < 2) {
    return false;
  }

  SVirtualScanLogicNode* pVScan = (SVirtualScanLogicNode *)nodesListGetNode(pNode->pChildren, 0);

  // All VirtualScan children must be plain table-scan nodes.
  // TagRefSource children cannot participate in PDA restructuring.
  bool   hasExternalChild = false;
  SNode* pChild;
  FOREACH(pChild, pVScan->node.pChildren) {
    if (nodeType(pChild) != QUERY_NODE_LOGIC_PLAN_SCAN) {
      return false;
    }
    hasExternalChild |= ((SScanLogicNode*)pChild)->scanType == SCAN_TYPE_EXTERNAL;
  }
  if (hasExternalChild &&
      (NULL != pVScan->node.pConditions || NULL != pVScan->node.pLimit || NULL != pVScan->node.pSlimit)) {
    return false;
  }

  SNode*                 pCol;
  FOREACH(pCol, pVScan->pScanCols) {
    SColumnNode* pScanCol = (SColumnNode* )pCol;
    if (!pScanCol->hasRef) {
      // if col don't have ref, it can't be eliminated. If the vscan operator is eliminated,
      // the upper operator can't find the right slot id.
      return false;
    }
  }

  SAggLogicNode*  pAgg = (SAggLogicNode*)pNode;
  SNode*          pAggFunc = NULL;
  FOREACH(pAggFunc, pAgg->pAggFuncs) {
    SFunctionNode *pFunc = (SFunctionNode *)pAggFunc;
    if (fmIsSelectFunc(pFunc->funcId) || fmIsGroupKeyFunc(pFunc->funcId)) {
      return false;
    }
    if (hasExternalChild &&
        (1 != LIST_LENGTH(pFunc->pParameterList) ||
         QUERY_NODE_COLUMN != nodeType(nodesListGetNode(pFunc->pParameterList, 0)))) {
      return false;
    }
  }

  if (pAgg->hasGroup || 0 != LIST_LENGTH(pAgg->pTsmaSubplans) || NULL != pAgg->node.pConditions ||
      NULL != pAgg->node.pLimit || NULL != pAgg->node.pSlimit) {
    return false;
  }

  return true;
}

static bool scanNodeContainColumn(SNode* pScan, SColumnNode* pCol) {
  SScanLogicNode* pScanNode = (SScanLogicNode*)pScan;
  if ('\0' != pCol->refSourceName[0]) {
    if (SCAN_TYPE_EXTERNAL != pScanNode->scanType || NULL == pScanNode->pExtTableNode ||
        0 != strcmp(((SExtTableNode*)pScanNode->pExtTableNode)->sourceName, pCol->refSourceName)) {
      return false;
    }
  } else if (SCAN_TYPE_EXTERNAL == pScanNode->scanType) {
    return false;
  }

  SNode *pScanCol = NULL;
  FOREACH(pScanCol, pScanNode->pScanCols) {
    if (QUERY_NODE_COLUMN == nodeType(pScanCol)) {
      SColumnNode *pScanColNode = (SColumnNode *)pScanCol;
      if (pScanColNode->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
        continue;
      }
      if (pScanColNode->hasDep && pCol->hasRef) {
        if (strcmp(pScanColNode->dbName, pCol->refDbName) == 0 &&
            strcmp(pScanColNode->tableAlias, pCol->refTableName) == 0 &&
            strcmp(pScanColNode->colName, pCol->refColName) == 0) {
          return true;
        }
      }
    }
  }
  return false;
}

static int32_t findDepTableScanNode(SColumnNode* pCol, SVirtualScanLogicNode *pVScan, SNode **ppNode,
                                      SScanLogicNode** ppOrigin) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode  *pScan = NULL;
  FOREACH(pScan, pVScan->node.pChildren) {
    SScanLogicNode *pScanNode = (SScanLogicNode *)pScan;
    if (scanNodeContainColumn(pScan, pCol)) {
      if (NULL != ppOrigin) {
        *ppOrigin = pScanNode;
      }
      PLAN_ERR_JRET(nodesCloneNode(pScan, ppNode));
      goto _return;
    }
  }
  *ppNode = NULL;
  if (NULL != ppOrigin) {
    *ppOrigin = NULL;
  }
  // Not found is not an error. E.g. COUNT(*) unfolds to COUNT(ts) and the virtual table's own
  // primary timestamp has no matching source-table scan. Return SUCCESS with *ppNode=NULL so the
  // caller can skip this aggregate func  -  but a skipped func means PDA must be abandoned
  // (the caller bails via pSkipped), otherwise the rewritten plan would silently lose that
  // func's output slot.
  qDebug("findDepTableScanNode: column %s has no source-table scan (e.g. COUNT(*)->COUNT(ts)); PDA must bail",
         pCol->colName);

_return:
  if (TSDB_CODE_SUCCESS != code) {
    planError("%s failed since %s", __func__, tstrerror(code));
  }
  return code;
}

static void syncPdaColumnRef(SVirtualScanLogicNode* pVScan, SColumnNode* pCol) {
  SNode* pNode = NULL;
  FOREACH(pNode, pVScan->pScanCols) {
    if (QUERY_NODE_COLUMN != nodeType(pNode)) {
      continue;
    }

    SColumnNode* pVScanCol = (SColumnNode*)pNode;
    if (pVScanCol->colId != pCol->colId && 0 != strcmp(pVScanCol->colName, pCol->colName)) {
      continue;
    }

    pCol->hasRef = pVScanCol->hasRef;
    tstrncpy(pCol->refSourceName, pVScanCol->refSourceName, sizeof(pCol->refSourceName));
    tstrncpy(pCol->refDbName, pVScanCol->refDbName, sizeof(pCol->refDbName));
    tstrncpy(pCol->refTableName, pVScanCol->refTableName, sizeof(pCol->refTableName));
    tstrncpy(pCol->refColName, pVScanCol->refColName, sizeof(pCol->refColName));
    return;
  }
}

typedef struct SPdaRewriteCxt {
  SScanLogicNode* pScan;
  int32_t         code;
} SPdaRewriteCxt;

static EDealRes pdaRewriteExternalColumn(SNode** ppNode, void* pContext) {
  if (QUERY_NODE_COLUMN != nodeType(*ppNode)) return DEAL_RES_CONTINUE;

  SPdaRewriteCxt* pCxt = (SPdaRewriteCxt*)pContext;
  SNode*          pScanCol = findDepScanCol(pCxt->pScan, (SColumnNode*)*ppNode);
  if (NULL == pScanCol) return DEAL_RES_CONTINUE;

  SNode* pNew = NULL;
  pCxt->code = nodesCloneNode(pScanCol, &pNew);
  if (TSDB_CODE_SUCCESS != pCxt->code) return DEAL_RES_ERROR;
  nodesDestroyNode(*ppNode);
  *ppNode = pNew;
  return DEAL_RES_IGNORE_CHILD;
}

static int32_t mergeAggFuncToAggNode(SAggLogicNode* pAgg, SFunctionNode* pFunc, SScanLogicNode* pScan) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode  *ppFuncNode = NULL;
  PLAN_ERR_JRET(nodesCloneNode((SNode *)pFunc, &ppFuncNode));
  if (SCAN_TYPE_EXTERNAL == pScan->scanType) {
    SPdaRewriteCxt cxt = {.pScan = pScan, .code = TSDB_CODE_SUCCESS};
    nodesRewriteExpr(&ppFuncNode, pdaRewriteExternalColumn, &cxt);
    PLAN_ERR_JRET(cxt.code);
  }
  PLAN_ERR_JRET(nodesListMakeAppend(&pAgg->pAggFuncs, (SNode *)ppFuncNode));
  pAgg->hasLast |= fmIsLastFunc(((SFunctionNode *)ppFuncNode)->funcId);
  pAgg->hasLastRow |= fmIsLastRowFunc(((SFunctionNode *)ppFuncNode)->funcId);
  pAgg->hasTimeLineFunc |= fmIsTimelineFunc(((SFunctionNode *)ppFuncNode)->funcId);
  pAgg->onlyHasKeepOrderFunc &= fmIsKeepOrderFunc((SFunctionNode *)ppFuncNode);
  pAgg->hasGroupKeyOptimized = false;
  pAgg->hasGroup = false;
  pAgg->isGroupTb = false;
  pAgg->isPartTb = false;
  return code;
_return:
  nodesDestroyNode(ppFuncNode);
  if (TSDB_CODE_SUCCESS != code) {
    planError("%s failed since %s", __func__, tstrerror(code));
  }
  return code;
}

static void pdaRemoveSyntheticRemotePkSort(SScanLogicNode* pScan) {
  if (SCAN_TYPE_EXTERNAL != pScan->scanType || NULL == pScan->pRemoteLogicPlan ||
      QUERY_NODE_LOGIC_PLAN_SORT != nodeType(pScan->pRemoteLogicPlan)) {
    return;
  }

  SSortLogicNode* pSort = (SSortLogicNode*)pScan->pRemoteLogicPlan;
  if (1 != LIST_LENGTH(pSort->node.pChildren) ||
      NULL != pSort->node.pConditions || NULL != pSort->node.pLimit ||
      NULL != pSort->node.pSlimit) {
    return;
  }

  SLogicNode* pChild =
      (SLogicNode*)nodesListGetNode(pSort->node.pChildren, 0);
  NODES_CLEAR_LIST(pSort->node.pChildren);
  pChild->pParent = NULL;
  pScan->pRemoteLogicPlan = (SNode*)pChild;
  nodesDestroyNode((SNode*)pSort);
}

static int32_t rebuildPlanForPdaOptimize(SColumnNode* pCol, SFunctionNode* pAggFunc, SVirtualScanLogicNode* pVScan,
                                         SHashObj* pAggNodeMap, bool* pSkipped) {
  int32_t         code = TSDB_CODE_SUCCESS;
  SScanLogicNode* pDepScan = NULL;
  SAggLogicNode*  pAggNode = NULL;
  SScanLogicNode* pDepScanOrigin = NULL;
  bool            append = false;
  SAggLogicNode** pNodeFound = NULL;

  // Aggregate columns from the same source scan in one branch. The scan pointer
  // is unambiguous even when different external sources expose identical names.
  PLAN_ERR_JRET(findDepTableScanNode(pCol, pVScan, (SNode**)&pDepScan, &pDepScanOrigin));
  if (NULL == pDepScan) {
    // column has no matching source-table scan (e.g. virtual table's own ts from COUNT(*));
    // signal the caller to abandon PDA  -  proceeding would rewrite the plan without this func.
    *pSkipped = true;
    return TSDB_CODE_SUCCESS;
  }
  pdaRemoveSyntheticRemotePkSort(pDepScan);
  pNodeFound = (SAggLogicNode **)taosHashGet(pAggNodeMap, &pDepScanOrigin, POINTER_BYTES);
  if (NULL == pNodeFound) {
    // make new agg node.
    PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_AGG, (SNode**)&pAggNode));
    PLAN_ERR_JRET(mergeAggFuncToAggNode(pAggNode, pAggFunc, pDepScan));
    PLAN_ERR_JRET(nodesListMakeAppend(&pAggNode->node.pChildren, (SNode*)pDepScan));
    append = true;
    PLAN_ERR_JRET(taosHashPut(pAggNodeMap, &pDepScanOrigin, POINTER_BYTES, &pAggNode, POINTER_BYTES));
  } else {
    pAggNode = *pNodeFound;
    // merge the agg func to the existed agg node.
    PLAN_ERR_JRET(mergeAggFuncToAggNode(pAggNode, pAggFunc, pDepScan));
    nodesDestroyNode((SNode*)pDepScan);
  }
  return code;
_return:
  if (!append) {
    nodesDestroyNode((SNode*)pDepScan);
  }
  if (TSDB_CODE_SUCCESS != code) {
    planError("%s failed since %s", __func__, tstrerror(code));
  }
  return code;
}

static void destroyAggLogicNode(void* data) {
  if (data == NULL) {
    return;
  }
  SAggLogicNode* pNode = *(SAggLogicNode **)data;
  nodesDestroyNode((SNode*)pNode);
}

// This optimization rule will optimize plan tree from
// Agg -> VirtualScan -> TableScan
//                    \> TableScan
//                    \> TableScan
// to
// Merge -> Agg -> TableScan
//       \> Agg -> TableScan
//       \> Agg -> TableScan

static int32_t pdaOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  int32_t          code = TSDB_CODE_SUCCESS;
  SMergeLogicNode* pMerge = NULL;
  SAggLogicNode*   pAgg = (SAggLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, pdaMayBeOptimized, &code);
  if (NULL == pAgg) {
    return code;
  }

  SVirtualScanLogicNode *pVScan = (SVirtualScanLogicNode*)nodesListGetNode(pAgg->node.pChildren, 0);
  SNode                 *pAggFunc = NULL;
  bool                   skipped = false;
  SHashObj              *pAggNodeMap = taosHashInit(LIST_LENGTH(pAgg->pAggFuncs), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (NULL == pAggNodeMap) {
    PLAN_ERR_JRET(terrno);
  }

  FOREACH(pAggFunc, pAgg->pAggFuncs) {
    SFunctionNode *pFunc = (SFunctionNode *)pAggFunc;
    SNode         *pParam = NULL;
    FOREACH(pParam, pFunc->pParameterList) {
      if (QUERY_NODE_COLUMN == nodeType(pParam)) {
        SColumnNode *pCol = (SColumnNode *)pParam;
        syncPdaColumnRef(pVScan, pCol);
        if (pCol->colType == COLUMN_TYPE_TAG) {
          // If the column is a tag column, we should not optimize the aggregation. Since tag will be read from the
          // virtual tablescan operator.
          goto _return;
        }
        PLAN_ERR_JRET(rebuildPlanForPdaOptimize(pCol, pFunc, pVScan, pAggNodeMap, &skipped));
        if (skipped) {
          // An agg func has no source-table scan (e.g. COUNT(*) -> COUNT(ts)); rewriting without it
          // would produce a plan with fewer outputs than the original agg. Bail out entirely.
          goto _return;
        }
      }
    }
  }

  if (taosHashGetSize(pAggNodeMap) != LIST_LENGTH(pVScan->node.pChildren)) {
      // The number of agg nodes should be equal to the number of virtual scan nodes.
      // If not, it means that some virtual scan nodes are not used in the aggregation.
      // In this case, we should not optimize the aggregation.
      goto _return;
  }


  PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_MERGE, (SNode**)&pMerge));

  pMerge->colsMerge = true;
  pMerge->numOfChannels = taosHashGetSize(pAggNodeMap);
  pMerge->srcGroupId = -1;
  pMerge->node.precision = pAgg->node.precision;
  PLAN_ERR_JRET(nodesCloneList(pAgg->node.pTargets, &pMerge->node.pTargets));

  SNode* pVScanChild = NULL;
  FOREACH(pVScanChild, pVScan->node.pChildren) {
    SAggLogicNode** pAggNode =
        (SAggLogicNode**)taosHashGet(pAggNodeMap, &pVScanChild, POINTER_BYTES);
    (*pAggNode)->node.pParent = (SLogicNode*)pMerge;
    SNode* pNode = NULL;
    FOREACH(pNode, (*pAggNode)->node.pChildren) {
      // clear the mask to try scanPathOptimize again.
      OPTIMIZE_FLAG_CLEAR_MASK(((SScanLogicNode*)pNode)->node.optimizedFlag, OPTIMIZE_FLAG_SCAN_PATH);
      if (SCAN_TYPE_EXTERNAL == ((SScanLogicNode*)pNode)->scanType) {
        OPTIMIZE_FLAG_CLEAR_MASK(((SScanLogicNode*)pNode)->node.optimizedFlag, OPTIMIZE_FLAG_FQ_PUSHDOWN);
      }
      ((SLogicNode*)pNode)->pParent = (SLogicNode*)*pAggNode;
    }
    PLAN_ERR_JRET(createColumnByRewriteExprs((*pAggNode)->pAggFuncs, &(*pAggNode)->node.pTargets));
    PLAN_ERR_JRET(nodesListMakeAppend(&pMerge->node.pChildren, (SNode*)*pAggNode));
  }

  PLAN_ERR_JRET(replaceLogicNode(pLogicSubplan, (SLogicNode*)pAgg, (SLogicNode*)pMerge));

  nodesDestroyNode((SNode*)pVScan);
  nodesDestroyNode((SNode*)pAgg);
  taosHashCleanup(pAggNodeMap);

  pCxt->optimized = true;
  return code;
_return:
  nodesDestroyNode((SNode*)pMerge);
  taosHashSetFreeFp(pAggNodeMap, destroyAggLogicNode);
  taosHashCleanup(pAggNodeMap);
  return code;
}

static bool functionHasTagOrPkParam(SFunctionNode* pFunc) {
  SNode* pParam = NULL;
  FOREACH(pParam, pFunc->pParameterList) {
    if (QUERY_NODE_FUNCTION == nodeType(pParam) &&
        (FUNCTION_TYPE_TBNAME == ((SFunctionNode*)pParam)->funcType ||
         0 == strcasecmp(((SFunctionNode*)pParam)->functionName, "tbname"))) {
      return true;
    }
    if (nodeType(pParam) == QUERY_NODE_COLUMN) {
      SColumnNode* pCol = (SColumnNode*)pParam;
      if (pCol->colType == COLUMN_TYPE_TAG || pCol->colType == COLUMN_TYPE_TBNAME ||
          0 == strcasecmp(pCol->colName, "tbname") || pCol->isPrimTs || pCol->isPk ||
          pCol->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
        return true;
      }
    }
  }
  return false;
}

static bool functionHasTagParam(SFunctionNode* pFunc) {
  SNode* pParam = NULL;
  FOREACH(pParam, pFunc->pParameterList) {
    if (nodeType(pParam) == QUERY_NODE_COLUMN) {
      SColumnNode* pCol = (SColumnNode*)pParam;
      if (pCol->colType == COLUMN_TYPE_TAG || pCol->colType == COLUMN_TYPE_TBNAME) {
        return true;
      }
    }
  }
  return false;
}

static bool functionHasTagRefParam(SFunctionNode* pFunc) {
  SNode* pParam = NULL;
  FOREACH(pParam, pFunc->pParameterList) {
    if (nodeType(pParam) == QUERY_NODE_COLUMN) {
      SColumnNode* pCol = (SColumnNode*)pParam;
      if (pCol->colType == COLUMN_TYPE_TAG && pCol->hasRef) {
        return true;
      }
    }
  }
  return false;
}

// forward declarations for helpers used by trimVirtualScanForStateCols
static bool hasTargetInColumnList(SColumnNode* pTargetCol, SNodeList* pCols);
static void removeUselessTargetFromNodeByColumnList(SLogicNode* pNode, SNodeList* pCols);

static void setTimestampPrecisionInList(SNodeList* pList, int8_t precision) {
  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    if (NULL != pNode && ((SExprNode*)pNode)->resType.type == TSDB_DATA_TYPE_TIMESTAMP) {
      ((SExprNode*)pNode)->resType.precision = precision;
    }
  }
}

static void normalizeRemoteVtablePrecision(SLogicNode* pNode, int8_t precision) {
  if (NULL == pNode) {
    return;
  }
  setTimestampPrecisionInList(pNode->pTargets, precision);
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    setTimestampPrecisionInList(((SScanLogicNode*)pNode)->pScanCols, precision);
  }
  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    normalizeRemoteVtablePrecision((SLogicNode*)pChild, precision);
  }
}

static void normalizeExternalScanVtablePrecision(SScanLogicNode* pScan, int8_t precision) {
  if (NULL == pScan || SCAN_TYPE_EXTERNAL != pScan->scanType) {
    return;
  }

  setTimestampPrecisionInList(pScan->pScanCols, precision);
  setTimestampPrecisionInList(pScan->node.pTargets, precision);

  normalizeRemoteVtablePrecision((SLogicNode*)pScan->pRemoteLogicPlan, precision);
}

/*
 * Check whether all state key columns resolve to the same origin table scan.
 *
 * When true, sets *ppDepScan to a cloned copy of that scan node.
 * When false, the state columns span multiple origin tables.
 */
static int32_t checkAllStateExprsSameOriginTable(SNodeList* pStateExprs, SVirtualScanLogicNode* pVScan,
                                                 bool* pSameOrigin, SNode** ppDepScan) {
  int32_t code = TSDB_CODE_SUCCESS;
  *pSameOrigin = true;
  *ppDepScan = NULL;

  SNode*          pFirstScan = NULL;
  SNode*          pOtherScan = NULL;
  SScanLogicNode* pFirstOrigin = NULL;
  SScanLogicNode* pOtherOrigin = NULL;

  SNode* pFirstExpr = nodesListGetNode(pStateExprs, 0);
  if (pFirstExpr == NULL || nodeType(pFirstExpr) != QUERY_NODE_COLUMN) {
    planError("%s first state expr is not a column node, nodeType:%d",
              __func__, pFirstExpr ? nodeType(pFirstExpr) : -1);
    return TSDB_CODE_INVALID_PARA;
  }
  PLAN_ERR_JRET(findDepTableScanNode((SColumnNode*)pFirstExpr, pVScan, &pFirstScan, &pFirstOrigin));
  if (NULL == pFirstScan) {
    // the state column has no source-table scan (e.g. vtable's own ts)  -  cannot prove same origin
    *pSameOrigin = false;
    goto _return;
  }

  SNode* pExpr = NULL;
  int32_t idx = 0;
  FOREACH(pExpr, pStateExprs) {
    if (idx++ == 0) {
      continue;
    }
    if (nodeType(pExpr) != QUERY_NODE_COLUMN) {
      planError("%s state expr at index %d is not a column node, nodeType:%d",
                __func__, idx - 1, nodeType(pExpr));
      code = TSDB_CODE_PLAN_INTERNAL_ERROR;
      goto _return;
    }
    PLAN_ERR_JRET(findDepTableScanNode((SColumnNode*)pExpr, pVScan, &pOtherScan, &pOtherOrigin));
    if (NULL == pOtherScan) {
      // no matching source-table scan for this state column  -  treat as not-same-origin
      *pSameOrigin = false;
      goto _return;
    }
    if (pOtherOrigin != pFirstOrigin) {
      *pSameOrigin = false;
    }
    NODES_DESTORY_NODE(pOtherScan);
    if (!(*pSameOrigin)){
      break;
    }
  }

  if (*pSameOrigin) {
    *ppDepScan = pFirstScan;
    pFirstScan = NULL;
  }

_return:
  nodesDestroyNode(pFirstScan);
  nodesDestroyNode(pOtherScan);
  return code;
}

// Helper: trim a single child origin scan's targets and scanCols using ref relationship
static bool isChildColNeededByStateExprs(SScanLogicNode* pScan, SColumnNode* pCol, SNodeList* pStateExprs) {
  SListCell* pCell = (pStateExprs ? pStateExprs->pHead : NULL);
  while (pCell) {
    SColumnNode* pStateCol = (SColumnNode*)pCell->pNode;
    bool sourceMatches = ('\0' == pStateCol->refSourceName[0] && SCAN_TYPE_EXTERNAL != pScan->scanType) ||
                         (SCAN_TYPE_EXTERNAL == pScan->scanType && NULL != pScan->pExtTableNode &&
                          0 == strcmp(((SExtTableNode*)pScan->pExtTableNode)->sourceName,
                                     pStateCol->refSourceName));
    if (sourceMatches && pStateCol->hasRef && pCol->hasDep &&
        strcmp(pCol->dbName, pStateCol->refDbName) == 0 &&
        strcmp(pCol->tableAlias, pStateCol->refTableName) == 0 &&
        strcmp(pCol->colName, pStateCol->refColName) == 0) {
      return true;
    }
    pCell = pCell->pNext;
  }
  return false;
}

static void trimChildScanForStateCols(SScanLogicNode* pScan, SNodeList* pStateExprs) {
  // trim child's pTargets
  {
    SNode* pTarget = NULL;
    WHERE_EACH(pTarget, pScan->node.pTargets) {
      if (nodeType(pTarget) == QUERY_NODE_COLUMN) {
        SColumnNode* pTgtCol = (SColumnNode*)pTarget;
        if (pTgtCol->colId == PRIMARYKEY_TIMESTAMP_COL_ID || pTgtCol->isPrimTs) {
          WHERE_NEXT;
          continue;
        }
        if (!isChildColNeededByStateExprs(pScan, pTgtCol, pStateExprs)) {
          REPLACE_NODE(NULL);
          ERASE_NODE(pScan->node.pTargets);
          continue;
        }
      }
      WHERE_NEXT;
    }
  }

  // trim child's pScanCols
  {
    SNode* pSc = NULL;
    WHERE_EACH(pSc, pScan->pScanCols) {
      if (nodeType(pSc) == QUERY_NODE_COLUMN) {
        SColumnNode* pScCol = (SColumnNode*)pSc;
        if (pScCol->colId == PRIMARYKEY_TIMESTAMP_COL_ID || pScCol->isPrimTs) {
          WHERE_NEXT;
          continue;
        }
        if (!isChildColNeededByStateExprs(pScan, pScCol, pStateExprs)) {
          REPLACE_NODE(NULL);
          ERASE_NODE(pScan->pScanCols);
          continue;
        }
      }
      WHERE_NEXT;
    }
  }
}

static void trimVirtualScanForStateCols(SVirtualScanLogicNode* pVScan, SNodeList* pStateExprs) {
  // trim VirtualScan's own targets
  removeUselessTargetFromNodeByColumnList((SLogicNode*)pVScan, pStateExprs);

  // trim VirtualScan's virtual columns by output identity
  SNode* pCol = NULL;
  WHERE_EACH(pCol, pVScan->pScanCols) {
    if (nodeType(pCol) == QUERY_NODE_COLUMN) {
      SColumnNode* pScanCol = (SColumnNode*)pCol;
      if (pScanCol->colId == PRIMARYKEY_TIMESTAMP_COL_ID || pScanCol->isPrimTs) {
        // keep timestamp
      } else if (hasTargetInColumnList(pScanCol, pStateExprs)) {
        // keep state column
      } else {
        REPLACE_NODE(NULL);
        ERASE_NODE(pVScan->pScanCols);
        continue;
      }
    }
    WHERE_NEXT;
  }

  // A regular virtual table has fixed column references, so trim every
  // origin scan using the static ref relationship.
  SNode* pChild = NULL;
  FOREACH(pChild, pVScan->node.pChildren) {
    if (nodeType(pChild) == QUERY_NODE_LOGIC_PLAN_SCAN) {
      trimChildScanForStateCols((SScanLogicNode*)pChild, pStateExprs);
    }
  }
}

static void trimVstableWindowColumnList(SNodeList* pCols, SNodeList* pRequiredCols) {
  SNode* pNode = NULL;
  WHERE_EACH(pNode, pCols) {
    if (QUERY_NODE_COLUMN == nodeType(pNode)) {
      SColumnNode* pCol = (SColumnNode*)pNode;
      if (PRIMARYKEY_TIMESTAMP_COL_ID != pCol->colId && !pCol->isPrimTs &&
          !hasTargetInColumnList(pCol, pRequiredCols)) {
        REPLACE_NODE(NULL);
        ERASE_NODE(pCols);
        continue;
      }
    }
    WHERE_NEXT;
  }
}

static void trimVstableWindowBoundaryScan(SDynQueryCtrlLogicNode* pDynScan,
                                           SVirtualScanLogicNode* pVScan,
                                           SNodeList* pRequiredCols) {
  // Virtual-stable columns have no fixed source ref. Keep one virtual schema
  // for the whole boundary branch and let DynQueryCtrl resolve each virtual
  // child through its runtime colMap.
  removeUselessTargetFromNodeByColumnList((SLogicNode*)pVScan, pRequiredCols);
  trimVstableWindowColumnList(pVScan->pScanCols, pRequiredCols);

  // Local dynamic scans emit virtual columns directly, so their physical
  // slots must be trimmed to the same schema. Preserve the original column
  // nodes so their slot keys remain stable. External scans remain source
  // templates; their runtime batch colMap selects the required real columns.
  SNode* pChild = NULL;
  FOREACH(pChild, pVScan->node.pChildren) {
    if (QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pChild) ||
        SCAN_TYPE_EXTERNAL == ((SScanLogicNode*)pChild)->scanType) {
      continue;
    }
    SScanLogicNode* pScan = (SScanLogicNode*)pChild;
    removeUselessTargetFromNodeByColumnList((SLogicNode*)pScan, pRequiredCols);
    trimVstableWindowColumnList(pScan->pScanCols, pRequiredCols);
  }

  removeUselessTargetFromNodeByColumnList((SLogicNode*)pDynScan, pRequiredCols);
  trimVstableWindowColumnList(pDynScan->vtbScan.pScanCols, pRequiredCols);
}

static bool externalScanSafeForVtableWindow(const SScanLogicNode* pScan) {
  if (SCAN_TYPE_EXTERNAL != pScan->scanType) {
    return true;
  }
  if (NULL == pScan->pExtTableNode) {
    return false;
  }

  const SExtTableNode* pExtTable = (const SExtTableNode*)pScan->pExtTableNode;
  if (pScan->node.dynamicOp && '\0' == pExtTable->remoteTableName[0]) {
    // A virtual-stable external child is only a source-level template here.
    // Its real table and column mapping (including the primary timestamp) are
    // supplied by DynQueryCtrl at runtime, so the empty template scanCols list
    // must not make the VStableWindow rule miss.  The optimized execution scan
    // is rebuilt from the fixed virtual schema before physical-plan creation.
    return true;
  }

  bool hasPrimaryTs = false;
  SNode* pCol = NULL;
  FOREACH(pCol, pScan->pScanCols) {
    if (QUERY_NODE_COLUMN == nodeType(pCol) &&
        (((SColumnNode*)pCol)->isPrimTs || ((SColumnNode*)pCol)->colId == PRIMARYKEY_TIMESTAMP_COL_ID)) {
      hasPrimaryTs = true;
      break;
    }
  }

  if (!hasPrimaryTs) {
    return false;
  }
  if (NULL == pScan->pRemoteLogicPlan) {
    return true;
  }

  EOrder order = ORDER_UNKNOWN;
  return fqScanHasProvenPkOrder(pScan, &order) && ORDER_ASC == order;
}

static bool externalScanSafeForVstableAgg(const SScanLogicNode* pScan) {
  if (NULL == pScan || SCAN_TYPE_EXTERNAL != pScan->scanType) {
    return NULL != pScan;
  }
  if (NULL == pScan->pExtTableNode) {
    return false;
  }

  const SExtTableNode* pExtTable = (const SExtTableNode*)pScan->pExtTableNode;
  if (pScan->node.dynamicOp && '\0' == pExtTable->remoteTableName[0]) {
    // DynCtrl rebuilds this source-level template into raw FederatedScan
    // batches.  Any attached remote logic tree would be lost when the
    // VirtualScan is removed, so only a bare template is safe here.
    return NULL == pScan->pRemoteLogicPlan;
  }
  return externalScanSafeForVtableWindow(pScan);
}

static bool vtableWindowMayBeOptimized(SLogicNode* pNode, void* pCtx) {
  if (OPTIMIZE_FLAG_TEST_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_VTB_WINDOW)) {
    return false;
  }
  if (nodeType(pNode) != QUERY_NODE_LOGIC_PLAN_WINDOW || LIST_LENGTH(pNode->pChildren) != 1) {
    return false;
  }

  SNode *pChild = nodesListGetNode(pNode->pChildren, 0);
  if (NULL == pChild || nodeType(pChild) != QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN) {
    return false;
  }

  SVirtualScanLogicNode* pVScan = (SVirtualScanLogicNode*)pChild;
  if (TSDB_SUPER_TABLE == pVScan->tableType && NULL != pVScan->node.pConditions) {
    return false;
  }

  SScanLogicNode* pOnlyOrigin =
      1 == LIST_LENGTH(pVScan->node.pChildren)
          ? (SScanLogicNode*)nodesListGetNode(pVScan->node.pChildren, 0)
          : NULL;
  if (NULL != pOnlyOrigin && QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pOnlyOrigin) &&
      SCAN_TYPE_EXTERNAL == pOnlyOrigin->scanType) {
    // Splitting a single external input into a boundary scan and a
    // calculation scan always reads the same remote table twice.  Keep the
    // original one-pass window plan unless there is another origin whose
    // rows can be fetched independently.
    return false;
  }

  SNode* pOrigin = NULL;
  FOREACH(pOrigin, pVScan->node.pChildren) {
    if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pOrigin) &&
        !externalScanSafeForVtableWindow((SScanLogicNode*)pOrigin)) {
      return false;
    }
  }

  SWindowLogicNode* pWindow = (SWindowLogicNode*)pNode;
  if (pWindow->winType != WINDOW_TYPE_STATE) {
    return false;
  }
  if (pWindow->pStateExprs == NULL || LIST_LENGTH(pWindow->pStateExprs) < 1) {
    return false;
  }
  SNode* pStateExpr = NULL;
  FOREACH(pStateExpr, pWindow->pStateExprs) {
    if (nodeType(pStateExpr) != QUERY_NODE_COLUMN) {
      return false;
    }
  }

  SNode* pFunc = NULL;
  FOREACH(pFunc, pWindow->pFuncs) {
    SFunctionNode* pFunction = (SFunctionNode*)pFunc;
    if (functionHasTagOrPkParam(pFunction)) {
      return false;
    }
  }
  return true;
}

static int32_t rebuildPlanForVtableWindowOptimize(SColumnNode* pCol, SFunctionNode* pAggFunc,
                                                  SVirtualScanLogicNode* pVScan, SHashObj* pAggNodeMap,
                                                  SArray* pExtWinOrder) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SScanLogicNode*    pDepScan = NULL;
  SWindowLogicNode*  pExtWindow = NULL;
  SScanLogicNode*    pDepScanOrigin = NULL;
  bool               append = false;
  SWindowLogicNode** pNodeFound = NULL;

  PLAN_ERR_JRET(findDepTableScanNode(pCol, pVScan, (SNode**)&pDepScan, &pDepScanOrigin));
  if (NULL == pDepScan) {
    // the func's column has no source-table scan (e.g. vtable's own ts); bail out instead of
    // dereferencing NULL  -  caller must skip this rewrite.
    planError("rebuildPlanForVtableWindowOptimize: column %s has no source-table scan", pCol->colName);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }
  normalizeExternalScanVtablePrecision(pDepScan, (int8_t)pVScan->node.precision);
  pNodeFound = (SWindowLogicNode **)taosHashGet(pAggNodeMap, &pDepScanOrigin, POINTER_BYTES);
  if (NULL == pNodeFound) {
    // make new agg node.
    PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_WINDOW, (SNode**)&pExtWindow));
    pExtWindow->winType = WINDOW_TYPE_EXTERNAL;
    pExtWindow->isSingleTable = true;
    pExtWindow->extFill.mode = FILL_MODE_NONE;
    PLAN_ERR_JRET(nodesListMakeAppend(&pExtWindow->pFuncs, (SNode*)pAggFunc));
    PLAN_ERR_JRET(nodesListMakeAppend(&pExtWindow->node.pChildren, (SNode*)pDepScan));
    PLAN_ERR_JRET(nodesCloneNode(nodesListGetNode(pDepScan->pScanCols, 0), (SNode**)&pExtWindow->pTspk));
    append = true;
    PLAN_ERR_JRET(taosHashPut(pAggNodeMap, &pDepScanOrigin, POINTER_BYTES, &pExtWindow, POINTER_BYTES));
    QUERY_CHECK_NULL(taosArrayPush(pExtWinOrder, &pExtWindow), code, lino, _return, terrno);
  } else {
    pExtWindow = *pNodeFound;
    // merge the agg func to the existed agg node.
    PLAN_ERR_JRET(nodesListMakeAppend(&pExtWindow->pFuncs, (SNode*)pAggFunc));
    nodesDestroyNode((SNode*)pDepScan);
  }
  return code;
_return:
  if (!append) {
    nodesDestroyNode((SNode*)pDepScan);
  }
  return code;
}

static int32_t windowAggFuncSplit(SFunctionNode* pFunction, SVirtualScanLogicNode* pVirtualScan,
                                  SHashObj* pExtWinMap, SArray* pExtWinOrder) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pParam = NULL;

  FOREACH(pParam, pFunction->pParameterList) {
    if (QUERY_NODE_COLUMN == nodeType(pParam)) {
      SColumnNode *pCol = (SColumnNode *)pParam;
      PLAN_ERR_RET(rebuildPlanForVtableWindowOptimize(pCol, pFunction, pVirtualScan, pExtWinMap, pExtWinOrder));
    }
  }

  return code;
}

static int32_t splitWindowAggFuncsToMultiExtWindows(SDynQueryCtrlLogicNode* pDyn, SWindowLogicNode* pWin,
                                                    SVirtualScanLogicNode* pVirtualScan, SHashObj* pExtWinMap,
                                                    SArray* pExtWinOrder) {
  int32_t    code = TSDB_CODE_SUCCESS;
  SNode*     pFunc = NULL;
  int32_t    index = 0;

  WHERE_EACH(pFunc, pWin->pFuncs) {
    SFunctionNode *pFunction = (SFunctionNode *)pFunc;
    if (!fmIsWindowPseudoColumnFunc(pFunction->funcId)) {
      PLAN_ERR_RET(windowAggFuncSplit(pFunction, pVirtualScan, pExtWinMap, pExtWinOrder));
      REPLACE_NODE(NULL);
      ERASE_NODE(pWin->pFuncs);
      continue;
    } else {
      if (pFunction->funcType == FUNCTION_TYPE_WSTART) {
        pDyn->vtbWindow.wstartSlotId = index;
      } else if (pFunction->funcType == FUNCTION_TYPE_WEND) {
        pDyn->vtbWindow.wendSlotId = index;
      } else if (pFunction->funcType == FUNCTION_TYPE_WDURATION) {
        pDyn->vtbWindow.wdurationSlotId = index;
      }
    }
    index++;
    WHERE_NEXT;
  }
  return code;
}

typedef struct SVtableWindowCondRewriteCxt {
  SScanLogicNode*        pTargetScan;
  SVirtualScanLogicNode* pVirtualScan;
  int32_t                errCode;
  bool                   canOptimize;
} SVtableWindowCondRewriteCxt;

static SNode* findDepScanCol(SScanLogicNode* pScan, SColumnNode* pCol) {
  if ('\0' != pCol->refSourceName[0]) {
    if (SCAN_TYPE_EXTERNAL != pScan->scanType || NULL == pScan->pExtTableNode ||
        0 != strcmp(((SExtTableNode*)pScan->pExtTableNode)->sourceName, pCol->refSourceName)) {
      return NULL;
    }
  } else if (SCAN_TYPE_EXTERNAL == pScan->scanType) {
    return NULL;
  }

  SNode* pScanCol = NULL;
  FOREACH(pScanCol, pScan->pScanCols) {
    if (QUERY_NODE_COLUMN != nodeType(pScanCol)) {
      continue;
    }
    SColumnNode* pScanColNode = (SColumnNode*)pScanCol;
    if (pScanColNode->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
      continue;
    }
    if (pScanColNode->hasDep && pCol->hasRef && strcmp(pScanColNode->dbName, pCol->refDbName) == 0 &&
        strcmp(pScanColNode->tableAlias, pCol->refTableName) == 0 &&
        strcmp(pScanColNode->colName, pCol->refColName) == 0) {
      return pScanCol;
    }
  }
  return NULL;
}

static SNode* findPrimaryTsScanCol(SScanLogicNode* pScan) {
  SNode* pScanCol = NULL;
  FOREACH(pScanCol, pScan->pScanCols) {
    if (QUERY_NODE_COLUMN == nodeType(pScanCol) &&
        (((SColumnNode*)pScanCol)->isPrimTs || ((SColumnNode*)pScanCol)->colId == PRIMARYKEY_TIMESTAMP_COL_ID)) {
      return pScanCol;
    }
  }
  return NULL;
}

static EDealRes rewriteVtableWindowCondCol(SNode** pNode, void* pContext) {
  SVtableWindowCondRewriteCxt* pCxt = pContext;
  if (QUERY_NODE_COLUMN != nodeType(*pNode)) {
    return DEAL_RES_CONTINUE;
  }

  SColumnNode* pCol = (SColumnNode*)*pNode;
  if (pCol->isPrimTs || pCol->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
    SNode* pScanCol = findPrimaryTsScanCol(pCxt->pTargetScan);
    if (NULL == pScanCol) {
      pCxt->canOptimize = false;
      return DEAL_RES_END;
    }

    SNode* pNew = NULL;
    pCxt->errCode = nodesCloneNode(pScanCol, &pNew);
    if (TSDB_CODE_SUCCESS != pCxt->errCode || NULL == pNew) {
      return DEAL_RES_ERROR;
    }

    nodesDestroyNode(*pNode);
    *pNode = pNew;
    return DEAL_RES_IGNORE_CHILD;
  }

  if (!pCol->hasRef || pCol->colType == COLUMN_TYPE_TAG) {
    pCxt->canOptimize = false;
    return DEAL_RES_END;
  }

  SNode* pScanCol = findDepScanCol(pCxt->pTargetScan, pCol);
  if (NULL == pScanCol) {
    pCxt->canOptimize = false;
    return DEAL_RES_END;
  }

  SNode* pNew = NULL;
  pCxt->errCode = nodesCloneNode(pScanCol, &pNew);
  if (TSDB_CODE_SUCCESS != pCxt->errCode || NULL == pNew) {
    return DEAL_RES_ERROR;
  }

  nodesDestroyNode(*pNode);
  *pNode = pNew;
  return DEAL_RES_IGNORE_CHILD;
}

static int32_t rewriteAndPushVtableWindowConditions(SVirtualScanLogicNode* pVirtualScan, SScanLogicNode* pTargetScan,
                                                    bool* pCanOptimize) {
  if (NULL == pVirtualScan->node.pConditions) {
    return TSDB_CODE_SUCCESS;
  }

  SVtableWindowCondRewriteCxt cxt = {
      .pTargetScan = pTargetScan, .pVirtualScan = pVirtualScan, .errCode = TSDB_CODE_SUCCESS, .canOptimize = true};
  nodesRewriteExpr(&pVirtualScan->node.pConditions, rewriteVtableWindowCondCol, &cxt);
  *pCanOptimize = cxt.canOptimize;
  if (TSDB_CODE_SUCCESS != cxt.errCode || !cxt.canOptimize) {
    return cxt.errCode;
  }

  int32_t code = nodesMergeNode(&pTargetScan->node.pConditions, &pVirtualScan->node.pConditions);
  if (TSDB_CODE_SUCCESS == code) {
    OPTIMIZE_FLAG_CLEAR_MASK(pTargetScan->node.optimizedFlag, OPTIMIZE_FLAG_PUSH_DOWN_CONDE);
  }
  return code;
}

static int32_t addWstartAndWendToWindowFuncsIfMissing(SDynQueryCtrlLogicNode* pDyn, SWindowLogicNode* pNewWindow) {
  int32_t code = TSDB_CODE_SUCCESS;
  SFunctionNode* pWstart = NULL;
  SFunctionNode* pWend = NULL;

  if (pDyn->vtbWindow.wstartSlotId == -1) {
    PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pWstart));
    tstrncpy(pWstart->functionName, "_wstart", TSDB_FUNC_NAME_LEN);
    tstrncpy(pWstart->node.userAlias, "_wstart", TSDB_FUNC_NAME_LEN);
    tstrncpy(pWstart->node.aliasName, "_wstart", TSDB_FUNC_NAME_LEN);
    pWstart->funcType = FUNCTION_TYPE_WSTART;
    pWstart->funcId = fmGetFuncId("_wstart");
    pWstart->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes,
                                        .type = TSDB_DATA_TYPE_TIMESTAMP,
                                        .precision = pNewWindow->node.precision};
    PLAN_ERR_JRET(nodesListMakeAppend(&pNewWindow->pFuncs, (SNode*)pWstart));
    pDyn->vtbWindow.wstartSlotId = LIST_LENGTH(pNewWindow->pFuncs) - 1;
  }

  if (pDyn->vtbWindow.wendSlotId == -1) {
    PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pWend));
    tstrncpy(pWend->functionName, "_wend", TSDB_FUNC_NAME_LEN);
    tstrncpy(pWend->node.userAlias, "_wend", TSDB_FUNC_NAME_LEN);
    tstrncpy(pWend->node.aliasName, "_wend", TSDB_FUNC_NAME_LEN);
    pWend->funcType = FUNCTION_TYPE_WEND;
    pWend->funcId = fmGetFuncId("_wend");
    pWend->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes,
                                      .type = TSDB_DATA_TYPE_TIMESTAMP,
                                      .precision = pNewWindow->node.precision};
    PLAN_ERR_JRET(nodesListMakeAppend(&pNewWindow->pFuncs, (SNode*)pWend));
    pDyn->vtbWindow.wendSlotId = LIST_LENGTH(pNewWindow->pFuncs) - 1;
  }

  if (pDyn->vtbWindow.wdurationSlotId == -1) {
    PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pWend));
    tstrncpy(pWend->functionName, "_wduration", TSDB_FUNC_NAME_LEN);
    tstrncpy(pWend->node.userAlias, "_wduration", TSDB_FUNC_NAME_LEN);
    tstrncpy(pWend->node.aliasName, "_wduration", TSDB_FUNC_NAME_LEN);
    pWend->funcType = FUNCTION_TYPE_WDURATION;
    pWend->funcId = fmGetFuncId("_wduration");
    pWend->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes,
                                      .type = TSDB_DATA_TYPE_BIGINT,
                                      .precision = pNewWindow->node.precision};
    PLAN_ERR_JRET(nodesListMakeAppend(&pNewWindow->pFuncs, (SNode*)pWend));
    pDyn->vtbWindow.wdurationSlotId = LIST_LENGTH(pNewWindow->pFuncs) - 1;
  }

  return code;
_return:
  nodesDestroyNode((SNode*)pWstart);
  nodesDestroyNode((SNode*)pWend);
  return code;
}

static int32_t generateMergeNodeForExtWindows(SHashObj* pExtWinMap, SArray* pExtWinOrder,
                                              SMergeLogicNode** ppMerge, SWindowLogicNode* pWindow) {
  SMergeLogicNode* pMerge = NULL;
  int32_t          code = TSDB_CODE_SUCCESS;

  PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_MERGE, (SNode**)&pMerge));
  pMerge->colsMerge = true;
  pMerge->numOfChannels = taosHashGetSize(pExtWinMap);
  pMerge->srcGroupId = -1;
  pMerge->node.precision = pWindow->node.precision;
  TSWAP(pWindow->node.pConditions, pMerge->node.pConditions);

  for (int32_t i = 0; i < taosArrayGetSize(pExtWinOrder); ++i) {
    SWindowLogicNode** pExtWindow = taosArrayGet(pExtWinOrder, i);
    (*pExtWindow)->node.pParent = (SLogicNode*)pMerge;
    SNode* pNode = NULL;
    FOREACH(pNode, (*pExtWindow)->node.pChildren) {
      // clear the mask to try scanPathOptimize again.
      ((SLogicNode*)pNode)->dynamicOp = true;
      ((SLogicNode*)pNode)->pParent = (SLogicNode*)*pExtWindow;
      SScanLogicNode* pScan = (SScanLogicNode*)pNode;
      if (SCAN_TYPE_EXTERNAL == pScan->scanType) {
        (*pExtWindow)->orgTableUid = 0;
        (*pExtWindow)->orgTableVgId = -1;
      } else {
        if (NULL == pScan->pVgroupList) {
          planError("%s failed since external scan has no vgroup list", __func__);
          PLAN_ERR_JRET(TSDB_CODE_INVALID_PARA);
        }
        (*pExtWindow)->orgTableUid = pScan->tableId;
        (*pExtWindow)->orgTableVgId = pScan->pVgroupList->vgroups[0].vgId;
      }
    }
    OPTIMIZE_FLAG_SET_MASK((*pExtWindow)->node.optimizedFlag, OPTIMIZE_FLAG_VTB_WINDOW);
    PLAN_ERR_JRET(createColumnByRewriteExprs((*pExtWindow)->pFuncs, &(*pExtWindow)->node.pTargets));
    PLAN_ERR_JRET(nodesListMakeAppend(&pMerge->node.pChildren, (SNode*)*pExtWindow));
    FOREACH(pNode, (*pExtWindow)->node.pTargets) {
      PLAN_ERR_JRET(nodesListMakeStrictAppend(&pMerge->node.pTargets, pNode));
    }
  }
  *ppMerge = pMerge;
  return code;
_return:
  if (TSDB_CODE_SUCCESS != code) {
    planError("%s failed since %s", __func__, tstrerror(code));
  }
  return code;
}

static int32_t findWindowTspkFromScanCols(SNodeList* pScanCols, const SNode* pTspk,
                                          SNode** ppMatchedTspk);
static int32_t vtableWindowOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  if (inStreamCalcClause(pCxt->pPlanCxt)) {
    // stream calc query does not support scan path optimization
    return TSDB_CODE_SUCCESS;
  }
  SWindowLogicNode* pWindow = (SWindowLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, vtableWindowMayBeOptimized, NULL);
  if (NULL == pWindow) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;
  SWindowLogicNode*       pNewWindow = NULL;
  SVirtualScanLogicNode*  pVirtualScan = NULL;
  SNode*                  pStateColScan = NULL;
  SNode*                  pMatchedTspk = NULL;
  SMergeLogicNode*        pMerge = NULL;
  SHashObj*               pExtWinMap = NULL;
  SArray*                 pExtWinOrder = NULL;
  SDynQueryCtrlLogicNode* pDynWindowNode = NULL;
  SNode*                  pFunc = NULL;
  SVirtualScanLogicNode*  pTrimmedVScan = NULL;
  bool                    sameOrigin = true;
  bool                    canOptimize = true;

  PLAN_ERR_JRET(nodesCloneNode((SNode*)pWindow, (SNode**)&pNewWindow));
  pVirtualScan = (SVirtualScanLogicNode*)nodesListGetNode(pNewWindow->node.pChildren, 0);
  QUERY_CHECK_NULL(pVirtualScan, code, lino, _return, TSDB_CODE_INVALID_PARA)

  PLAN_ERR_JRET(checkAllStateExprsSameOriginTable(pNewWindow->pStateExprs, pVirtualScan, &sameOrigin, &pStateColScan));
  if (!sameOrigin && NULL != pVirtualScan->node.pConditions) {
    nodesDestroyNode((SNode*)pNewWindow);
    pNewWindow = NULL;
    pVirtualScan = NULL;
    goto _return;
  }
  pNewWindow->node.pChildren = NULL;

  if (sameOrigin) {
    normalizeExternalScanVtablePrecision((SScanLogicNode*)pStateColScan, (int8_t)pVirtualScan->node.precision);
    // same origin table: attach StateWindow directly to the single origin TableScan
    PLAN_ERR_JRET(findWindowTspkFromScanCols(((SScanLogicNode*)pStateColScan)->pScanCols,
                                             pNewWindow->pTspk, &pMatchedTspk));
    NODES_DESTORY_NODE(pNewWindow->pTspk);
    PLAN_ERR_JRET(nodesCloneNode(pMatchedTspk, (SNode**)&pNewWindow->pTspk));
    PLAN_ERR_JRET(nodesListMakeAppend(&pNewWindow->node.pChildren, pStateColScan));
    ((SLogicNode*)pStateColScan)->pParent = (SLogicNode*)pNewWindow;
    PLAN_ERR_JRET(rewriteAndPushVtableWindowConditions(pVirtualScan, (SScanLogicNode*)pStateColScan, &canOptimize));
    if (!canOptimize) {
      nodesDestroyNode((SNode*)pNewWindow);
      pNewWindow = NULL;
      nodesDestroyNode((SNode*)pVirtualScan);
      pVirtualScan = NULL;
      goto _return;
    }
  } else {
    // cross origin tables: keep a trimmed VirtualScan under StateWindow
    PLAN_ERR_JRET(nodesCloneNode((SNode*)pVirtualScan, (SNode**)&pTrimmedVScan));
    trimVirtualScanForStateCols(pTrimmedVScan, pNewWindow->pStateExprs);
    PLAN_ERR_JRET(findWindowTspkFromScanCols(pTrimmedVScan->pScanCols, pNewWindow->pTspk, &pMatchedTspk));
    NODES_DESTORY_NODE(pNewWindow->pTspk);
    PLAN_ERR_JRET(nodesCloneNode(pMatchedTspk, (SNode**)&pNewWindow->pTspk));
    PLAN_ERR_JRET(nodesListMakeAppend(&pNewWindow->node.pChildren, (SNode*)pTrimmedVScan));
    pTrimmedVScan->node.pParent = (SLogicNode*)pNewWindow;
    pTrimmedVScan = NULL;  // ownership transferred
  }
  pExtWinMap = taosHashInit(LIST_LENGTH(pNewWindow->pFuncs), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true,
                            HASH_ENTRY_LOCK);
  QUERY_CHECK_NULL(pExtWinMap, code, lino, _return, terrno)
  // The hash provides scan-identity lookup; preserve first-seen target order for stable Merge children and slots.
  pExtWinOrder = taosArrayInit(LIST_LENGTH(pNewWindow->pFuncs), sizeof(SWindowLogicNode*));
  QUERY_CHECK_NULL(pExtWinOrder, code, lino, _return, terrno)

  PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL, (SNode**)&pDynWindowNode));
  pDynWindowNode->vtbWindow.wstartSlotId = -1;
  pDynWindowNode->vtbWindow.wendSlotId = -1;
  pDynWindowNode->vtbWindow.wdurationSlotId = -1;
  pDynWindowNode->vtbWindow.isVstb = false;
  pDynWindowNode->qType = DYN_QTYPE_VTB_WINDOW;
  pDynWindowNode->vtbWindow.extendOption = pWindow->extendOption;

  PLAN_ERR_JRET(splitWindowAggFuncsToMultiExtWindows(pDynWindowNode, pNewWindow, pVirtualScan, pExtWinMap,
                                                     pExtWinOrder));

  PLAN_ERR_JRET(addWstartAndWendToWindowFuncsIfMissing(pDynWindowNode, pNewWindow));

  PLAN_ERR_JRET(nodesCloneList(pNewWindow->node.pTargets, &pDynWindowNode->node.pTargets));

  nodesDestroyList(pNewWindow->node.pTargets);

  // re-generate target columns for new window node
  PLAN_ERR_JRET(createColumnByRewriteExprs(pNewWindow->pFuncs, &pNewWindow->node.pTargets));

  if (taosHashGetSize(pExtWinMap) > 0) {
    PLAN_ERR_JRET(generateMergeNodeForExtWindows(pExtWinMap, pExtWinOrder, &pMerge, pNewWindow));
    pMerge->node.pParent = (SLogicNode*)pDynWindowNode;
  }
  pNewWindow->node.pParent = (SLogicNode*)pDynWindowNode;

  PLAN_ERR_JRET(nodesListMakeAppend(&pDynWindowNode->node.pChildren, (SNode*)pNewWindow));
  if (pMerge != NULL) {
    PLAN_ERR_JRET(nodesListMakeAppend(&pDynWindowNode->node.pChildren, (SNode*)pMerge));
  }

  PLAN_ERR_JRET(replaceLogicNode(pLogicSubplan, (SLogicNode*)pWindow, (SLogicNode*)pDynWindowNode));

  nodesDestroyNode((SNode*)pWindow);
  nodesDestroyNode((SNode*)pVirtualScan);
  OPTIMIZE_FLAG_SET_MASK(pNewWindow->node.optimizedFlag, OPTIMIZE_FLAG_VTB_WINDOW);
  OPTIMIZE_FLAG_SET_MASK(pDynWindowNode->node.optimizedFlag, OPTIMIZE_FLAG_VTB_WINDOW);

  pCxt->optimized = true;

_return:
  if (TSDB_CODE_SUCCESS != code) {
    planError("%s failed since %s", __func__, tstrerror(code));
  }
  nodesDestroyNode((SNode*)pTrimmedVScan);
  taosArrayDestroy(pExtWinOrder);
  taosHashCleanup(pExtWinMap);
  return code;
}

/*
 * Detect whether an expression node is IS NULL / IS NOT NULL.
 *
 * @param pNode Expression node visited by walker.
 * @param pCtx Output bool flag pointer.
 *
 * @return DEAL_RES_END when null condition is found, otherwise DEAL_RES_CONTINUE.
 */
static EDealRes eventWindowHasNullCond(SNode* pNode, void* pCtx) {
  if (QUERY_NODE_OPERATOR != nodeType(pNode)) {
    return DEAL_RES_CONTINUE;
  }

  SOperatorNode* pOp = (SOperatorNode*)pNode;
  if (OP_TYPE_IS_NULL == pOp->opType || OP_TYPE_IS_NOT_NULL == pOp->opType) {
    *(bool*)pCtx = true;
    return DEAL_RES_END;
  }

  return DEAL_RES_CONTINUE;
}

/*
 * Check whether event window start/end conditions contain null predicates.
 *
 * @param pWindow Window logic node.
 *
 * @return true when null predicate exists, otherwise false.
 */
static bool eventWindowContainsNullCond(const SWindowLogicNode* pWindow) {
  bool hasNullCond = false;

  nodesWalkExpr(pWindow->pStartCond, eventWindowHasNullCond, &hasNullCond);
  if (hasNullCond) {
    return true;
  }

  nodesWalkExpr(pWindow->pEndCond, eventWindowHasNullCond, &hasNullCond);
  return hasNullCond;
}

static bool vstableIntervalWindowFuncsCanSplit(const SWindowLogicNode* pWindow);

static const char* vstableExternalSourceName(const SScanLogicNode* pScan) {
  if (NULL == pScan || SCAN_TYPE_EXTERNAL != pScan->scanType || NULL == pScan->pExtTableNode) {
    return NULL;
  }
  const char* pSourceName = ((SExtTableNode*)pScan->pExtTableNode)->sourceName;
  return '\0' != pSourceName[0] ? pSourceName : NULL;
}

static bool vstableHasExternalExecutionUnit(const SVirtualScanLogicNode* pVScan) {
  SNode* pScan = NULL;
  FOREACH(pScan, pVScan->node.pChildren) {
    if (NULL != vstableExternalSourceName((SScanLogicNode*)pScan)) {
      return true;
    }
  }
  return false;
}

static bool vstableSameExecutionUnit(const SScanLogicNode* pLeft, const SScanLogicNode* pRight) {
  const char* pLeftSource = vstableExternalSourceName(pLeft);
  const char* pRightSource = vstableExternalSourceName(pRight);
  if (NULL == pLeftSource || NULL == pRightSource) {
    return NULL == pLeftSource && NULL == pRightSource;
  }
  return 0 == strcmp(pLeftSource, pRightSource);
}

static int32_t vstableExecutionUnitCount(const SVirtualScanLogicNode* pVScan) {
  int32_t count = 0;
  SNode*  pNode = NULL;
  FOREACH(pNode, pVScan->node.pChildren) {
    if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode) &&
        SCAN_TYPE_TAG == ((SScanLogicNode*)pNode)->scanType) {
      continue;
    }
    bool seen = false;
    SNode* pPrevious = NULL;
    FOREACH(pPrevious, pVScan->node.pChildren) {
      if (pPrevious == pNode) {
        break;
      }
      if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pPrevious) &&
          SCAN_TYPE_TAG == ((SScanLogicNode*)pPrevious)->scanType) {
        continue;
      }
      if (vstableSameExecutionUnit((SScanLogicNode*)pPrevious, (SScanLogicNode*)pNode)) {
        seen = true;
        break;
      }
    }
    if (!seen) {
      ++count;
    }
  }
  return count;
}

static bool vstableTagScanHasColumn(const SVirtualScanLogicNode* pVScan,
                                    const SColumnNode* pColumn);

typedef struct SVstableOriginCxt {
  SVirtualScanLogicNode* pVScan;
  SScanLogicNode*        pOrigin;
  bool                   crossOrigin;
  int32_t                code;
} SVstableOriginCxt;

static EDealRes vstableFindExprOrigin(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN != nodeType(pNode)) {
    return DEAL_RES_CONTINUE;
  }

  SVstableOriginCxt* pCxt = pContext;
  SColumnNode*       pCol = (SColumnNode*)pNode;
  if (pCol->isPrimTs || PRIMARYKEY_TIMESTAMP_COL_ID == pCol->colId ||
      COLUMN_TYPE_COLUMN != pCol->colType ||
      vstableTagScanHasColumn(pCxt->pVScan, pCol)) {
    return DEAL_RES_CONTINUE;
  }

  // Restore the registered real-column reference. This is the same metadata
  // synchronization used by the existing partial-distribution rewrite and is
  // also needed by the later owner-specific parameter rewrite.
  syncPdaColumnRef(pCxt->pVScan, pCol);

  SScanLogicNode* pMatched = NULL;
  SNode*          pChild = NULL;
  FOREACH(pChild, pCxt->pVScan->node.pChildren) {
    if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pChild) &&
        scanNodeContainColumn(pChild, pCol)) {
      pMatched = (SScanLogicNode*)pChild;
      break;
    }
  }
  if (NULL == pMatched) {
    if (!pCol->hasRef) {
      return DEAL_RES_CONTINUE;
    }
    planError("%s failed to resolve virtual column id:%d name:%s hasRef:%d source:%s ref:%s.%s.%s",
              __func__, pCol->colId, pCol->colName, pCol->hasRef,
              pCol->refSourceName, pCol->refDbName, pCol->refTableName,
              pCol->refColName);
    pCxt->code = TSDB_CODE_PLAN_INTERNAL_ERROR;
    return DEAL_RES_ERROR;
  }
  if (NULL == pCxt->pOrigin) {
    pCxt->pOrigin = pMatched;
  } else if (!vstableSameExecutionUnit(pCxt->pOrigin, pMatched)) {
    pCxt->crossOrigin = true;
    return DEAL_RES_END;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t vstableExprOriginScan(SNode* pExpr, SVirtualScanLogicNode* pVScan,
                                     SScanLogicNode** ppOrigin, bool* pCrossOrigin) {
  int32_t code = TSDB_CODE_SUCCESS;

  *ppOrigin = NULL;
  *pCrossOrigin = false;
  SVstableOriginCxt cxt = {
      .pVScan = pVScan, .pOrigin = NULL, .crossOrigin = false,
      .code = TSDB_CODE_SUCCESS};
  nodesWalkExpr(pExpr, vstableFindExprOrigin, &cxt);
  PLAN_ERR_JRET(cxt.code);
  *ppOrigin = cxt.pOrigin;
  *pCrossOrigin = cxt.crossOrigin;
  return code;

_return:
  planError("%s failed since %s", __func__, tstrerror(code));
  return code;
}

static int32_t vstableExprDataColumnCount(SNode* pExpr, int32_t* pCount) {
  int32_t    code = TSDB_CODE_SUCCESS;
  SNodeList* pCols = NULL;
  SArray*    pIds = NULL;

  *pCount = 0;
  PLAN_ERR_JRET(nodesCollectColumnsFromNode(pExpr, NULL, COLLECT_COL_TYPE_ALL, &pCols));
  pIds = taosArrayInit(LIST_LENGTH(pCols), sizeof(col_id_t));
  if (NULL == pIds) {
    PLAN_ERR_JRET(terrno);
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pCols) {
    if (QUERY_NODE_COLUMN != nodeType(pNode)) {
      continue;
    }
    SColumnNode* pCol = (SColumnNode*)pNode;
    if (pCol->isPrimTs || PRIMARYKEY_TIMESTAMP_COL_ID == pCol->colId ||
        COLUMN_TYPE_TAG == pCol->colType || COLUMN_TYPE_TBNAME == pCol->colType ||
        0 == strcasecmp(pCol->colName, "tbname")) {
      continue;
    }
    bool seen = false;
    for (int32_t i = 0; i < taosArrayGetSize(pIds); ++i) {
      if (*(col_id_t*)taosArrayGet(pIds, i) == pCol->colId) {
        seen = true;
        break;
      }
    }
    if (!seen && NULL == taosArrayPush(pIds, &pCol->colId)) {
      PLAN_ERR_JRET(terrno);
    }
  }
  *pCount = taosArrayGetSize(pIds);

_return:
  nodesDestroyList(pCols);
  taosArrayDestroy(pIds);
  if (TSDB_CODE_SUCCESS != code) {
    planError("%s failed since %s", __func__, tstrerror(code));
  }
  return code;
}

static bool vstableHasAmbiguousCountStar(const SWindowLogicNode* pWindow) {
  SNode* pNode = NULL;
  FOREACH(pNode, pWindow->pFuncs) {
    if (QUERY_NODE_FUNCTION != nodeType(pNode)) {
      continue;
    }
    const SFunctionNode* pFunc = (const SFunctionNode*)pNode;
    if (fmIsCountLikeFunc(pFunc->funcId) &&
        0 == strcasecmp(pFunc->node.userAlias, "count(*)")) {
      return true;
    }
  }
  return false;
}

static bool intervalWindowHasFill(const SWindowLogicNode* pWindow) {
  if (pWindow->hasFill) {
    return true;
  }

  const SLogicNode* pUp = pWindow->node.pParent;

  while (pUp != NULL &&
         (nodeType(pUp) == QUERY_NODE_LOGIC_PLAN_PROJECT ||
          nodeType(pUp) == QUERY_NODE_LOGIC_PLAN_SORT)) {
    pUp = pUp->pParent;
  }

  return pUp != NULL && nodeType(pUp) == QUERY_NODE_LOGIC_PLAN_FILL;
}

static bool vstableWindowMayBeOptimized(SLogicNode* pNode, void* pCtx) {
  if (OPTIMIZE_FLAG_TEST_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_VTB_WINDOW)) {
    return false;
  }

  if (nodeType(pNode) != QUERY_NODE_LOGIC_PLAN_WINDOW || LIST_LENGTH(pNode->pChildren) != 1) {
    return false;
  }

  SWindowLogicNode* pWindow = (SWindowLogicNode*)pNode;
  if (pWindow->winType == WINDOW_TYPE_INTERVAL && intervalWindowHasFill(pWindow)) {
    return false;
  }

  SNode *pChild = nodesListGetNode(pNode->pChildren, 0);
  if (NULL == pChild || nodeType(pChild) != QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL) {
    return false;
  }

  SDynQueryCtrlLogicNode* pDynCtrl = (SDynQueryCtrlLogicNode*)pChild;
  if (DYN_QTYPE_VTB_SCAN != pDynCtrl->qType || pDynCtrl->node.pConditions ||
      2 != LIST_LENGTH(pDynCtrl->node.pChildren)) {
    return false;
  }

  // check dyn query ctrl don't have tag scan
  SNode *pVirtualScan = nodesListGetNode(((SLogicNode*)pDynCtrl)->pChildren, 0);
  if (NULL == pVirtualScan || nodeType(pVirtualScan) != QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN ||
      LIST_LENGTH(((SLogicNode*)pVirtualScan)->pChildren) < 1 ||
      ((SVirtualScanLogicNode*)pVirtualScan)->hasFilterCond ||
      NULL != ((SVirtualScanLogicNode*)pVirtualScan)->node.pConditions) {
    return false;
  }

  // Keep the established pure-local rule path byte-for-byte equivalent in
  // shape and eligibility.  Local virtual columns may intentionally have no
  // static source reference because DynQueryCtrl resolves them per child at
  // runtime; applying the federated source-affinity checks to them would make
  // the original VStableWindow rule miss and change existing plans.
  bool hasExternalExecutionUnit = vstableHasExternalExecutionUnit((SVirtualScanLogicNode*)pVirtualScan);
  if (!hasExternalExecutionUnit) {
    if (((SVirtualScanLogicNode*)pVirtualScan)->tableType != TSDB_SUPER_TABLE) {
      return false;
    }
    if (LIST_LENGTH(((SLogicNode*)pVirtualScan)->pChildren) != 1) {
      return false;
    }

    SNode* pLocalOrigin = nodesListGetNode(((SLogicNode*)pVirtualScan)->pChildren, 0);
    if (NULL == pLocalOrigin || QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pLocalOrigin) ||
        !externalScanSafeForVtableWindow((SScanLogicNode*)pLocalOrigin)) {
      return false;
    }

    SWindowLogicNode* pLocalWindow = (SWindowLogicNode*)pNode;
    switch (pLocalWindow->winType) {
      case WINDOW_TYPE_EVENT:
        if (eventWindowContainsNullCond(pLocalWindow)) {
          return false;
        }
        break;
      case WINDOW_TYPE_STATE: {
        if (NULL == pLocalWindow->pStateExprs || LIST_LENGTH(pLocalWindow->pStateExprs) < 1) {
          return false;
        }
        SNode* pStateExpr = NULL;
        FOREACH(pStateExpr, pLocalWindow->pStateExprs) {
          if (QUERY_NODE_COLUMN != nodeType(pStateExpr)) {
            return false;
          }
        }
        break;
      }
      case WINDOW_TYPE_SESSION:
      case WINDOW_TYPE_INTERVAL:
        break;
      default:
        return false;
    }

    SNode* pLocalFunc = NULL;
    FOREACH(pLocalFunc, pLocalWindow->pFuncs) {
      SFunctionNode* pFunction = (SFunctionNode*)pLocalFunc;
      if (functionHasTagParam(pFunction) ||
          (fmIsAggFunc(pFunction->funcId) && !fmIsSelectFunc(pFunction->funcId) &&
           functionHasTagOrPkParam(pFunction))) {
        return false;
      }
    }
    return true;
  }

  SNode* pOrigin = NULL;
  FOREACH(pOrigin, ((SLogicNode*)pVirtualScan)->pChildren) {
    if (QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pOrigin) ||
        SCAN_TYPE_TAG == ((SScanLogicNode*)pOrigin)->scanType ||
        !externalScanSafeForVtableWindow((SScanLogicNode*)pOrigin)) {
      return false;
    }
  }

  int32_t executionUnitCount =
      vstableExecutionUnitCount((SVirtualScanLogicNode*)pVirtualScan);
  if (WINDOW_TYPE_INTERVAL != pWindow->winType && executionUnitCount <= 2) {
    // Split window execution first scans the external unit for window
    // boundaries and then scans it again for aggregates. A federated virtual
    // stable always retains one local scan template, so at most two units
    // means there is only one real external unit to split. In that shape the
    // rewrite cannot save work and is slower than the original one-pass plan.
    return false;
  }
  if (vstableHasAmbiguousCountStar(pWindow)) {
    // count(*) has no owning data column.  Across execution units there is no
    // generally valid way to split it without first rebuilding virtual rows.
    return false;
  }
  switch (pWindow->winType) {
    case WINDOW_TYPE_EVENT: {
      if (eventWindowContainsNullCond(pWindow)) {
        return false;
      }
      SScanLogicNode* pCondOrigin = NULL;
      bool crossOrigin = false;
      if (TSDB_CODE_SUCCESS != vstableExprOriginScan(pWindow->pStartCond, (SVirtualScanLogicNode*)pVirtualScan,
                                                     &pCondOrigin, &crossOrigin) ||
          TSDB_CODE_SUCCESS != vstableExprOriginScan(pWindow->pEndCond, (SVirtualScanLogicNode*)pVirtualScan,
                                                     &pCondOrigin, &crossOrigin)) {
        return false;
      }
      break;
    }
    case WINDOW_TYPE_STATE: {
      if (pWindow->pStateExprs == NULL || LIST_LENGTH(pWindow->pStateExprs) < 1) {
        return false;
      }
      SNode* pStateExpr = NULL;
      FOREACH(pStateExpr, pWindow->pStateExprs) {
        if (nodeType(pStateExpr) != QUERY_NODE_COLUMN) {
          return false;
        }
        SScanLogicNode* pStateOrigin = NULL;
        bool crossOrigin = false;
        int32_t originCode = vstableExprOriginScan(pStateExpr, (SVirtualScanLogicNode*)pVirtualScan,
                                                   &pStateOrigin, &crossOrigin);
        if (TSDB_CODE_SUCCESS != originCode ||
            (NULL == pStateOrigin && executionUnitCount <= 1)) {
          return false;
        }
      }
      break;
    }
    case WINDOW_TYPE_SESSION:
      break;
    case WINDOW_TYPE_INTERVAL:
      {
        SScanLogicNode* pFirstScan =
            (SScanLogicNode*)nodesListGetNode(((SLogicNode*)pVirtualScan)->pChildren, 0);
        bool multipleLocalScans = LIST_LENGTH(((SLogicNode*)pVirtualScan)->pChildren) > 1 &&
                                  NULL != pFirstScan && SCAN_TYPE_EXTERNAL != pFirstScan->scanType;
      if ((executionUnitCount > 1 || multipleLocalScans) &&
          (0 != LIST_LENGTH(pWindow->pTsmaSubplans) || !vstableIntervalWindowFuncsCanSplit(pWindow))) {
        return false;
      }
      break;
      }
    default:
      return false;
  }

  SNode* pFunc = NULL;
  bool hasCalcFunc = false;
  FOREACH(pFunc, pWindow->pFuncs) {
    SFunctionNode* pFunction = (SFunctionNode*)pFunc;
    if (functionHasTagParam(pFunction)) {
      return false;
    }
    if ((fmIsAggFunc(pFunction->funcId) && !fmIsSelectFunc(pFunction->funcId) && functionHasTagOrPkParam(pFunction))) {
      return false;
    }
    if (!fmIsWindowPseudoColumnFunc(pFunction->funcId)) {
      hasCalcFunc = true;
      int32_t dataCols = 0;
      if (TSDB_CODE_SUCCESS != vstableExprDataColumnCount(pFunc, &dataCols) || dataCols > 1 ||
          (0 == dataCols && executionUnitCount > 1)) {
        return false;
      }
      if (executionUnitCount > 1) {
        if (!fmIsDistExecFunc(pFunction->funcId) || pFunction->isDistinct) {
          return false;
        }
        SScanLogicNode* pFuncOrigin = NULL;
        bool            crossOrigin = false;
        int32_t originCode =
            vstableExprOriginScan(pFunc, (SVirtualScanLogicNode*)pVirtualScan, &pFuncOrigin, &crossOrigin);
        if (TSDB_CODE_SUCCESS != originCode || crossOrigin) {
          return false;
        }
      }
    }
  }
  if (!hasCalcFunc && executionUnitCount > 1) {
    return false;
  }

  return true;
}

/*
 * Check whether target column exists in a column list.
 *
 * @param pTargetCol Target column to search.
 * @param pCols Candidate column list.
 *
 * @return true when matched by name and table alias, otherwise false.
 */
static bool hasTargetInColumnList(SColumnNode* pTargetCol, SNodeList* pCols) {
  if (NULL == pCols) {
    return false;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pCols) {
    if (QUERY_NODE_COLUMN != nodeType(pNode)) {
      continue;
    }

    SColumnNode* pCol = (SColumnNode*)pNode;
    if (strcmp(pCol->colName, pTargetCol->colName) == 0 &&
        strcmp(pCol->tableAlias, pTargetCol->tableAlias) == 0) {
      return true;
    }
  }

  return false;
}

/*
 * Remove node targets that are not required by the given column list.
 *
 * @param pNode Logic node whose targets will be filtered.
 * @param pCols Required column list.
 *
 * @return No return value.
 */
static void removeUselessTargetFromNodeByColumnList(SLogicNode* pNode, SNodeList* pCols) {
  SNode* pTarget = NULL;
  WHERE_EACH(pTarget, pNode->pTargets) {
    if (nodeType(pTarget) == QUERY_NODE_COLUMN) {
      SColumnNode* pCol = (SColumnNode*)pTarget;
      if (pCol->colId == PRIMARYKEY_TIMESTAMP_COL_ID || hasTargetInColumnList(pCol, pCols)) {
        // keep cond column and timestamp column
      } else {
        REPLACE_NODE(NULL);
        ERASE_NODE(pNode->pTargets);
        continue;
      }
    }
    WHERE_NEXT;
  }
}

/*
 * Create one order-by expression node from an expression.
 *
 * @param pExpr Source expression node.
 * @param order Sort order.
 * @param ppOrderByExpr Output order-by node.
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise error code.
 */
static int32_t createOrderByExprNode(SNode* pExpr, EOrder order, SOrderByExprNode** ppOrderByExpr) {
  int32_t           code = TSDB_CODE_SUCCESS;
  SOrderByExprNode* pOrderByExpr = NULL;

  PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR, (SNode**)&pOrderByExpr));
  pOrderByExpr->pExpr = NULL;
  PLAN_ERR_JRET(nodesCloneNode(pExpr, &pOrderByExpr->pExpr));

  pOrderByExpr->order = order;
  pOrderByExpr->nullOrder = (order == ORDER_ASC) ? NULL_ORDER_FIRST : NULL_ORDER_LAST;
  *ppOrderByExpr = pOrderByExpr;
  return code;

_return:
  planError("%s failed to create order by expr node, error code: %d", __func__, code);
  nodesDestroyNode((SNode*)pOrderByExpr);
  *ppOrderByExpr = NULL;
  return code;
}

/*
 * Clone one scan node and convert it to table-merge scan mode.
 *
 * @param pScan Source scan node.
 * @param pOutputMergeScan Output merge-scan node.
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise error code.
 */
static int32_t createMergeScanNodeByScanNode(SScanLogicNode* pScan, SLogicNode** pOutputMergeScan) {
  int32_t         code = TSDB_CODE_SUCCESS;
  SScanLogicNode* pMergeScan = NULL;

  PLAN_ERR_JRET(nodesCloneNode((SNode*)pScan, (SNode**)&pMergeScan));
  planPromoteScanToTableMerge(pMergeScan, pMergeScan->node.requireDataOrder, pMergeScan->node.resultDataOrder);
  optResetParent((SLogicNode*)pMergeScan);

  *pOutputMergeScan = (SLogicNode*)pMergeScan;

  return code;

_return:
  planError("%s failed to create merge scan node by scan node, error code: %d", __func__, code);
  nodesDestroyNode((SNode*)pMergeScan);
  *pOutputMergeScan = NULL;
  return code;
}

/*
 * Restrict merge-scan node to one vgroup.
 *
 * @param pMergeScan Merge-scan node to update.
 * @param pVgroup Vgroup info to keep.
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise error code.
 */
static int32_t setSingleVgroupForMergeScan(SScanLogicNode* pMergeScan, const SVgroupInfo* pVgroup) {
  if (NULL == pMergeScan || NULL == pVgroup || NULL == pMergeScan->pVgroupList ||
      pMergeScan->pVgroupList->numOfVgroups <= 0) {
    planError("%s invalid parameter for merge scan or vgroup info", __func__);
    return TSDB_CODE_INVALID_PARA;
  }

  pMergeScan->pVgroupList->vgroups[0] = *pVgroup;
  pMergeScan->pVgroupList->numOfVgroups = 1;
  return TSDB_CODE_SUCCESS;
}

/*
 * Append one dynamic single-vgroup merge-scan child into merge node.
 *
 * @param pScanNode Template scan node.
 * @param pMerge Merge node receiving the new child.
 * @param pVgroup Vgroup to bind into the new child.
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise error code.
 */
static int32_t appendSingleVgroupMergeScan(
    SScanLogicNode* pScanNode, SMergeLogicNode* pMerge, const SVgroupInfo* pVgroup) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SLogicNode* pMergeScan = NULL;

  PLAN_ERR_JRET(createMergeScanNodeByScanNode(pScanNode, &pMergeScan));
  PLAN_ERR_JRET(setSingleVgroupForMergeScan((SScanLogicNode*)pMergeScan, pVgroup));

  pMergeScan->pParent = (SLogicNode*)pMerge;
  pMergeScan->dynamicOp = true;
  PLAN_ERR_JRET(nodesListMakeAppend(&pMerge->node.pChildren, (SNode*)pMergeScan));

  return code;

_return:
  planError("%s failed to append single vgroup merge scan, error code: %d", __func__, code);
  nodesDestroyNode((SNode*)pMergeScan);
  return code;
}

// create sort node for win col scan node, sort by ts column asc
static int32_t createSortNodeForWinColScan(SColumnNode* pSortCol, SLogicNode* pWinColScan, SSortLogicNode** ppSort) {
  int32_t           code = TSDB_CODE_SUCCESS;
  SSortLogicNode*   pSort = NULL;
  SOrderByExprNode* pOrder = NULL;

  *ppSort = NULL;
  PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SORT, (SNode**)&pSort));

  pSort->node.outputTsOrder = ORDER_ASC;
  pSort->groupSort = false;
  pSort->node.pTargets = NULL;
  PLAN_ERR_JRET(nodesCloneList(pWinColScan->pTargets, &pSort->node.pTargets));

  PLAN_ERR_JRET(createOrderByExprNode((SNode*)pSortCol, ORDER_ASC, &pOrder));

  SNode* pPendingOrder = (SNode*)pOrder;
  pOrder = NULL;
  PLAN_ERR_JRET(nodesListMakeStrictAppend(&pSort->pSortKeys, pPendingOrder));

  *ppSort = pSort;
  return code;
_return:
  nodesDestroyNode((SNode*)pOrder);
  nodesDestroyNode((SNode*)pSort);
  qError("failed to create sort node for win col scan, error code: %d", code);
  return code;
}

typedef struct SVstbCalcUnit {
  SScanLogicNode*   pOrigin;
  SWindowLogicNode* pWindow;
} SVstbCalcUnit;

static int32_t rebuildScanColsAndTargetsByTsEnd(SScanLogicNode* pScanNode, SNode* pTsEnd);
static int32_t vstableRewritePartialFuncForScan(SFunctionNode* pFunc, SScanLogicNode* pScan, bool ownsFunc);
static int32_t vstableAppendWstartPlaceholder(SNodeList** ppFuncs, int8_t precision, int32_t* pIndex);
static int32_t appendNewChild(SLogicNode* pParent, SLogicNode* pChild);

static bool vstableScanHasColumn(const SScanLogicNode* pScan, const SColumnNode* pColumn) {
  SNode* pNode = NULL;
  FOREACH(pNode, pScan->pScanCols) {
    if (QUERY_NODE_COLUMN == nodeType(pNode) &&
        ((SColumnNode*)pNode)->colId == pColumn->colId) {
      return true;
    }
  }
  return false;
}

static int32_t vstableMergeVgroups(SScanLogicNode* pTarget, const SScanLogicNode* pSource) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (NULL == pSource->pVgroupList) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t missing = 0;
  for (int32_t i = 0; i < pSource->pVgroupList->numOfVgroups; ++i) {
    bool found = false;
    for (int32_t j = 0; NULL != pTarget->pVgroupList && j < pTarget->pVgroupList->numOfVgroups; ++j) {
      if (pTarget->pVgroupList->vgroups[j].vgId == pSource->pVgroupList->vgroups[i].vgId) {
        found = true;
        break;
      }
    }
    if (!found) {
      ++missing;
    }
  }
  if (0 == missing) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t oldCount = NULL != pTarget->pVgroupList ? pTarget->pVgroupList->numOfVgroups : 0;
  int32_t newCount = oldCount + missing;
  SVgroupsInfo* pMerged = taosMemoryCalloc(1, sizeof(SVgroupsInfo) + newCount * sizeof(SVgroupInfo));
  if (NULL == pMerged) {
    PLAN_ERR_JRET(terrno);
  }
  pMerged->numOfVgroups = newCount;
  if (oldCount > 0) {
    memcpy(pMerged->vgroups, pTarget->pVgroupList->vgroups, oldCount * sizeof(SVgroupInfo));
  }
  int32_t index = oldCount;
  for (int32_t i = 0; i < pSource->pVgroupList->numOfVgroups; ++i) {
    bool found = false;
    for (int32_t j = 0; j < oldCount; ++j) {
      if (pMerged->vgroups[j].vgId == pSource->pVgroupList->vgroups[i].vgId) {
        found = true;
        break;
      }
    }
    if (!found) {
      pMerged->vgroups[index++] = pSource->pVgroupList->vgroups[i];
    }
  }
  taosMemoryFree(pTarget->pVgroupList);
  pTarget->pVgroupList = pMerged;
  return code;

_return:
  planError("%s failed since %s", __func__, tstrerror(code));
  return code;
}

static int32_t vstableMergeExecutionScan(SScanLogicNode* pTarget, const SScanLogicNode* pSource) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pNode = NULL;
  FOREACH(pNode, pSource->pScanCols) {
    if (QUERY_NODE_COLUMN != nodeType(pNode) ||
        vstableScanHasColumn(pTarget, (SColumnNode*)pNode)) {
      continue;
    }
    SNode* pClone = NULL;
    PLAN_ERR_JRET(nodesCloneNode(pNode, &pClone));
    PLAN_ERR_JRET(nodesListMakeStrictAppend(&pTarget->pScanCols, pClone));
  }
  if (SCAN_TYPE_EXTERNAL != pTarget->scanType) {
    PLAN_ERR_JRET(vstableMergeVgroups(pTarget, pSource));
  }
  NODES_DESTORY_LIST(pTarget->node.pTargets);
  PLAN_ERR_JRET(createColumnByRewriteExprs(pTarget->pScanCols, &pTarget->node.pTargets));
  return code;

_return:
  if (TSDB_CODE_SUCCESS != code) {
    planError("%s failed since %s", __func__, tstrerror(code));
  }
  return code;
}

static int32_t vstableBuildExecutionScans(SVirtualScanLogicNode* pVScan, SArray** ppScans) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SArray* pScans = taosArrayInit(LIST_LENGTH(pVScan->node.pChildren), POINTER_BYTES);
  QUERY_CHECK_NULL(pScans, code, lino, _return, terrno)

  SNode* pOriginNode = NULL;
  FOREACH(pOriginNode, pVScan->node.pChildren) {
    SScanLogicNode* pOrigin = (SScanLogicNode*)pOriginNode;
    if (SCAN_TYPE_TAG == pOrigin->scanType) {
      continue;
    }
    SScanLogicNode* pUnit = NULL;
    for (int32_t i = 0; i < taosArrayGetSize(pScans); ++i) {
      SScanLogicNode* pCandidate = taosArrayGetP(pScans, i);
      if (vstableSameExecutionUnit(pCandidate, pOrigin)) {
        pUnit = pCandidate;
        break;
      }
    }
    if (NULL != pUnit) {
      PLAN_ERR_JRET(vstableMergeExecutionScan(pUnit, pOrigin));
      continue;
    }

    PLAN_ERR_JRET(nodesCloneNode(pOriginNode, (SNode**)&pUnit));
    optResetParent((SLogicNode*)pUnit);
    pUnit->node.dynamicOp = true;
    if (SCAN_TYPE_EXTERNAL != pUnit->scanType) {
      pUnit->outputTsPrecision = pVScan->node.precision;
    }
    QUERY_CHECK_NULL(taosArrayPush(pScans, &pUnit), code, lino, _return, terrno)
  }

  // VStable scan templates do not carry per-child references.  DynQueryCtrl
  // resolves those references at runtime and both Exchange and FederatedScan
  // scatter their result into the virtual column ids.  Give every execution
  // unit the same fixed virtual schema so upper window functions can be built
  // independently of the eventual local/foreign mapping.
  for (int32_t i = 0; i < taosArrayGetSize(pScans); ++i) {
    SScanLogicNode* pUnit = taosArrayGetP(pScans, i);
    SNode* pVirtualCol = NULL;
    FOREACH(pVirtualCol, pVScan->pScanCols) {
      if (QUERY_NODE_COLUMN != nodeType(pVirtualCol) ||
          vstableScanHasColumn(pUnit, (SColumnNode*)pVirtualCol)) {
        continue;
      }
      SNode* pClone = NULL;
      PLAN_ERR_JRET(nodesCloneNode(pVirtualCol, &pClone));
      PLAN_ERR_JRET(nodesListMakeStrictAppend(&pUnit->pScanCols, pClone));
    }
    NODES_DESTORY_LIST(pUnit->node.pTargets);
    PLAN_ERR_JRET(createColumnByRewriteExprs(pUnit->pScanCols, &pUnit->node.pTargets));
  }
  *ppScans = pScans;
  return code;

_return:
  taosArrayDestroyP(pScans, (FDelete)nodesDestroyNode);
  if (TSDB_CODE_SUCCESS != code) {
    planError("%s failed since %s", __func__, tstrerror(code));
  }
  return code;
}

static void vstableDestroyCalcUnit(void* pData) {
  SVstbCalcUnit* pUnit = pData;
  nodesDestroyNode((SNode*)pUnit->pWindow);
}

static int32_t vstableCreateCalcUnits(SWindowLogicNode* pBoundary, SVirtualScanLogicNode* pVScan,
                                      SArray* pUnits, SArray** ppExecutionScans) {
  int32_t       code = TSDB_CODE_SUCCESS;
  SArray*       pExecutionScans = NULL;
  SLogicNode*   pCalcSource = NULL;
  SVstbCalcUnit unit = {0};

  PLAN_ERR_JRET(vstableBuildExecutionScans(pVScan, &pExecutionScans));
  for (int32_t i = 0; i < taosArrayGetSize(pExecutionScans); ++i) {
    SScanLogicNode* pScan = taosArrayGetP(pExecutionScans, i);
    pCalcSource = (SLogicNode*)pScan;
    SScanLogicNode* pNullScan = NULL;
    taosArraySet(pExecutionScans, i, &pNullScan);
    unit = (SVstbCalcUnit){.pOrigin = pScan};
    PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_WINDOW, (SNode**)&unit.pWindow));
    unit.pWindow->winType = WINDOW_TYPE_EXTERNAL;
    unit.pWindow->windowAlgo = EXTERNAL_ALGO_HASH;
    unit.pWindow->isSingleTable = false;
    unit.pWindow->extFill.mode = FILL_MODE_NONE;
    unit.pWindow->node.dynamicOp = true;
    unit.pWindow->node.precision = pBoundary->node.precision;
    OPTIMIZE_FLAG_SET_MASK(unit.pWindow->node.optimizedFlag, OPTIMIZE_FLAG_VTB_WINDOW);

    SNode* pTspk = NULL;
    PLAN_ERR_JRET(findWindowTspkFromScanCols(pScan->pScanCols, pBoundary->pTspk, &pTspk));
    PLAN_ERR_JRET(nodesCloneNode(pTspk, &unit.pWindow->pTspk));

    PLAN_ERR_JRET(appendNewChild((SLogicNode*)unit.pWindow, pCalcSource));
    pCalcSource = NULL;
    if (NULL == taosArrayPush(pUnits, &unit)) {
      PLAN_ERR_JRET(terrno);
    }
    unit.pWindow = NULL;
  }
  *ppExecutionScans = pExecutionScans;
  return code;

_return:
  nodesDestroyNode((SNode*)pCalcSource);
  nodesDestroyNode((SNode*)unit.pWindow);
  taosArrayDestroyP(pExecutionScans, (FDelete)nodesDestroyNode);
  if (TSDB_CODE_SUCCESS != code) {
    planError("%s failed since %s", __func__, tstrerror(code));
  }
  return code;
}

static int32_t vstableAssignCalcFuncs(SWindowLogicNode* pBoundary, SVirtualScanLogicNode* pVScan,
                                      SDynQueryCtrlLogicNode* pDyn, SArray* pUnits,
                                      SNodeList** ppMergeFuncs) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        outputIndex = 0;
  bool           splitCalc = taosArrayGetSize(pUnits) > 1;
  SFunctionNode* pUnitFunc = NULL;
  SFunctionNode* pMergeFunc = NULL;
  SNode* pFuncNode = NULL;
  WHERE_EACH(pFuncNode, pBoundary->pFuncs) {
    SFunctionNode* pFunc = (SFunctionNode*)pFuncNode;
    if (fmIsWindowPseudoColumnFunc(pFunc->funcId)) {
      if (FUNCTION_TYPE_WSTART == pFunc->funcType) pDyn->vtbWindow.wstartSlotId = outputIndex;
      if (FUNCTION_TYPE_WEND == pFunc->funcType) pDyn->vtbWindow.wendSlotId = outputIndex;
      if (FUNCTION_TYPE_WDURATION == pFunc->funcType) pDyn->vtbWindow.wdurationSlotId = outputIndex;
      ++outputIndex;
      WHERE_NEXT;
      continue;
    }

    SScanLogicNode* pFuncOrigin = NULL;
    bool            crossOrigin = false;
    PLAN_ERR_JRET(vstableExprOriginScan(pFuncNode, pVScan, &pFuncOrigin, &crossOrigin));
    if (crossOrigin) {
      code = TSDB_CODE_PLAN_INTERNAL_ERROR;
      goto _return;
    }
    for (int32_t i = 0; i < taosArrayGetSize(pUnits); ++i) {
      SVstbCalcUnit* pUnit = taosArrayGet(pUnits, i);
      if (splitCalc) {
        PLAN_ERR_JRET(fmGetDistMethod(pFunc, &pUnitFunc, NULL, &pMergeFunc));
      } else {
        PLAN_ERR_JRET(nodesCloneNode(pFuncNode, (SNode**)&pUnitFunc));
      }
      PLAN_ERR_JRET(vstableRewritePartialFuncForScan(
          pUnitFunc, pUnit->pOrigin,
          NULL == pFuncOrigin || vstableSameExecutionUnit(pUnit->pOrigin, pFuncOrigin)));
      SNode* pPendingUnitFunc = (SNode*)pUnitFunc;
      pUnitFunc = NULL;
      PLAN_ERR_JRET(nodesListMakeStrictAppend(&pUnit->pWindow->pFuncs, pPendingUnitFunc));
      if (splitCalc && 0 == i) {
        SNode* pPendingMergeFunc = (SNode*)pMergeFunc;
        pMergeFunc = NULL;
        PLAN_ERR_JRET(nodesListMakeStrictAppend(ppMergeFuncs, pPendingMergeFunc));
      } else {
        nodesDestroyNode((SNode*)pMergeFunc);
        pMergeFunc = NULL;
      }
    }
    REPLACE_NODE(NULL);
    ERASE_NODE(pBoundary->pFuncs);
  }

  PLAN_ERR_JRET(addWstartAndWendToWindowFuncsIfMissing(pDyn, pBoundary));
  NODES_DESTORY_LIST(pBoundary->node.pTargets);
  PLAN_ERR_JRET(createColumnByRewriteExprs(pBoundary->pFuncs, &pBoundary->node.pTargets));
  return code;

_return:
  nodesDestroyNode((SNode*)pUnitFunc);
  nodesDestroyNode((SNode*)pMergeFunc);
  if (TSDB_CODE_SUCCESS != code) {
    planError("%s failed since %s", __func__, tstrerror(code));
  }
  return code;
}

static int32_t vstableCreateSingleCalc(SArray* pUnits, SLogicNode** ppCalc) {
  SVstbCalcUnit* pUnit = taosArrayGet(pUnits, 0);
  if (NULL == pUnit || NULL == pUnit->pWindow) {
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }
  int32_t code = createColumnByRewriteExprs(pUnit->pWindow->pFuncs, &pUnit->pWindow->node.pTargets);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  *ppCalc = (SLogicNode*)pUnit->pWindow;
  pUnit->pWindow = NULL;
  return TSDB_CODE_SUCCESS;
}

static int32_t vstableCreateSplitCalc(SWindowLogicNode* pBoundary, SArray* pUnits,
                                      SNodeList** ppMergeFuncs, SLogicNode** ppCalc) {
  int32_t           code = TSDB_CODE_SUCCESS;
  SMergeLogicNode*  pMerge = NULL;
  SWindowLogicNode* pMergeWindow = NULL;

  PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_MERGE, (SNode**)&pMerge));
  pMerge->colsMerge = true;
  pMerge->numOfChannels = taosArrayGetSize(pUnits);
  pMerge->srcGroupId = -1;
  pMerge->node.dynamicOp = true;
  pMerge->node.precision = pBoundary->node.precision;

  PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_WINDOW, (SNode**)&pMergeWindow));
  pMergeWindow->winType = WINDOW_TYPE_EXTERNAL;
  pMergeWindow->windowAlgo = EXTERNAL_ALGO_MERGE;
  pMergeWindow->isSingleTable = false;
  pMergeWindow->extFill.mode = FILL_MODE_NONE;
  pMergeWindow->node.dynamicOp = true;
  pMergeWindow->node.precision = pBoundary->node.precision;
  pMergeWindow->pFuncs = *ppMergeFuncs;
  *ppMergeFuncs = NULL;
  OPTIMIZE_FLAG_SET_MASK(pMergeWindow->node.optimizedFlag, OPTIMIZE_FLAG_VTB_WINDOW);

  for (int32_t i = 0; i < taosArrayGetSize(pUnits); ++i) {
    SVstbCalcUnit* pUnit = taosArrayGet(pUnits, i);
    int32_t        wstartIndex = -1;
    PLAN_ERR_JRET(vstableAppendWstartPlaceholder(&pUnit->pWindow->pFuncs,
                                                  pBoundary->node.precision, &wstartIndex));
    PLAN_ERR_JRET(createColumnByRewriteExprs(pUnit->pWindow->pFuncs,
                                             &pUnit->pWindow->node.pTargets));
    if (0 == i) {
      SNode* pWstartTarget = nodesListGetNode(pUnit->pWindow->node.pTargets, wstartIndex);
      PLAN_ERR_JRET(nodesCloneList(pUnit->pWindow->node.pTargets, &pMerge->node.pTargets));
      PLAN_ERR_JRET(nodesCloneNode(pWstartTarget, &pMergeWindow->pTspk));
    }
    PLAN_ERR_JRET(appendNewChild((SLogicNode*)pMerge, (SLogicNode*)pUnit->pWindow));
    pUnit->pWindow = NULL;
  }

  PLAN_ERR_JRET(createColumnByRewriteExprs(pMergeWindow->pFuncs,
                                           &pMergeWindow->node.pTargets));
  PLAN_ERR_JRET(appendNewChild((SLogicNode*)pMergeWindow, (SLogicNode*)pMerge));
  pMerge = NULL;
  *ppCalc = (SLogicNode*)pMergeWindow;
  pMergeWindow = NULL;
  return code;

_return:
  nodesDestroyNode((SNode*)pMerge);
  nodesDestroyNode((SNode*)pMergeWindow);
  if (TSDB_CODE_SUCCESS != code) {
    planError("%s failed since %s", __func__, tstrerror(code));
  }
  return code;
}

static int32_t vstableCreateCalcBranches(SWindowLogicNode* pBoundary, SVirtualScanLogicNode* pVScan,
                                         SDynQueryCtrlLogicNode* pDyn, SLogicNode** ppCalc) {
  int32_t    code = TSDB_CODE_SUCCESS;
  int32_t    lino = 0;
  SArray*    pUnits = NULL;
  SArray*    pExecutionScans = NULL;
  SNodeList* pMergeFuncs = NULL;

  pUnits = taosArrayInit(LIST_LENGTH(pVScan->node.pChildren), sizeof(SVstbCalcUnit));
  QUERY_CHECK_NULL(pUnits, code, lino, _return, terrno);

  PLAN_ERR_JRET(vstableCreateCalcUnits(pBoundary, pVScan, pUnits, &pExecutionScans));
  PLAN_ERR_JRET(vstableAssignCalcFuncs(pBoundary, pVScan, pDyn, pUnits, &pMergeFuncs));
  if (1 == taosArrayGetSize(pUnits)) {
    PLAN_ERR_JRET(vstableCreateSingleCalc(pUnits, ppCalc));
  } else {
    PLAN_ERR_JRET(vstableCreateSplitCalc(pBoundary, pUnits, &pMergeFuncs, ppCalc));
  }

_return:
  nodesDestroyList(pMergeFuncs);
  taosArrayDestroyP(pExecutionScans, (FDelete)nodesDestroyNode);
  taosArrayDestroyEx(pUnits, vstableDestroyCalcUnit);
  if (TSDB_CODE_SUCCESS != code) {
    planError("%s failed since %s", __func__, tstrerror(code));
  }
  return code;
}

static int32_t vstableCreateSessionTsMerge(SVirtualScanLogicNode* pVScan, SNode* pTsEnd,
                                           SMergeLogicNode** ppMerge) {
  int32_t code = TSDB_CODE_SUCCESS;
  SMergeLogicNode* pMerge = NULL;
  SOrderByExprNode* pMergeKey = NULL;
  SArray* pExecutionScans = NULL;
  int32_t channels = 0;

  PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_MERGE, (SNode**)&pMerge));
  pMerge->needSort = true;
  pMerge->groupSort = true;
  pMerge->numOfSubplans = 0;
  pMerge->node.dynamicOp = true;
  pMerge->node.precision = pVScan->node.precision;

  PLAN_ERR_JRET(vstableBuildExecutionScans(pVScan, &pExecutionScans));
  for (int32_t unitIdx = 0; unitIdx < taosArrayGetSize(pExecutionScans); ++unitIdx) {
    SScanLogicNode* pScan = taosArrayGetP(pExecutionScans, unitIdx);
    SScanLogicNode* pOrigin = pScan;
    PLAN_ERR_JRET(rebuildScanColsAndTargetsByTsEnd(pScan, pTsEnd));
    if (NULL == pMerge->node.pTargets) {
      PLAN_ERR_JRET(nodesCloneList(pScan->node.pTargets, &pMerge->node.pTargets));
      PLAN_ERR_JRET(nodesCloneList(pScan->node.pTargets, &pMerge->pInputs));
      PLAN_ERR_JRET(createOrderByExprNode(nodesListGetNode(pScan->node.pTargets, 0), ORDER_ASC, &pMergeKey));
      PLAN_ERR_JRET(nodesListMakeStrictAppend(&pMerge->pMergeKeys, (SNode*)pMergeKey));
      pMergeKey = NULL;
    }
    if (SCAN_TYPE_EXTERNAL == pScan->scanType) {
      // Each remote table is read with ORDER BY ts ASC. FederatedScan keeps a
      // single-table stream as-is and performs a streaming k-way merge for a
      // multi-table batch, so this scan is already globally ordered.
      pScan->node.outputTsOrder = ORDER_ASC;
      pScan->node.dynamicOp = true;
      PLAN_ERR_JRET(appendNewChild((SLogicNode*)pMerge,
                                   (SLogicNode*)pScan));
      pScan = NULL;
      ++channels;
    } else {
      int32_t vgNum = NULL != pOrigin->pVgroupList ? pOrigin->pVgroupList->numOfVgroups : 0;
      if (vgNum <= 0) {
        code = TSDB_CODE_PLAN_INTERNAL_ERROR;
        goto _return;
      }
      for (int32_t i = 0; i < vgNum; ++i) {
        PLAN_ERR_JRET(appendSingleVgroupMergeScan(pScan, pMerge, pOrigin->pVgroupList->vgroups + i));
        ++channels;
      }
      nodesDestroyNode((SNode*)pScan);
    }
    SScanLogicNode* pNullScan = NULL;
    taosArraySet(pExecutionScans, unitIdx, &pNullScan);
  }
  pMerge->numOfChannels = channels;
  *ppMerge = pMerge;
  taosArrayDestroyP(pExecutionScans, (FDelete)nodesDestroyNode);
  return code;

_return:
  nodesDestroyNode((SNode*)pMergeKey);
  nodesDestroyNode((SNode*)pMerge);
  taosArrayDestroyP(pExecutionScans, (FDelete)nodesDestroyNode);
  if (TSDB_CODE_SUCCESS != code) {
    planError("%s failed since %s", __func__, tstrerror(code));
  }
  return code;
}

static void clearChildList(SLogicNode* pNode) {
  nodesClearList(pNode->pChildren);
  pNode->pChildren = NULL;
}

static int32_t appendNewChild(SLogicNode* pParent, SLogicNode* pChild) {
  pChild->pParent = pParent;
  PLAN_RET(nodesListMakeAppend(&pParent->pChildren, (SNode*)pChild));
}

/*
 * Rebuild scan columns/targets so scan only keeps ts_end expression.
 *
 * @param pScanNode Scan node to rewrite.
 * @param pTsEnd ts_end expression used as scan column.
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise error code.
 */
static int32_t rebuildScanColsAndTargetsByTsEnd(SScanLogicNode* pScanNode, SNode* pTsEnd) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pCol = NULL;

  NODES_DESTORY_LIST(pScanNode->pScanCols);
  PLAN_ERR_JRET(nodesCloneNode(pTsEnd, &pCol));
  PLAN_ERR_JRET(nodesListMakeAppend(&pScanNode->pScanCols, pCol));
  pCol = NULL;

  NODES_DESTORY_LIST(pScanNode->node.pTargets);
  PLAN_ERR_JRET(createColumnByRewriteExprs(pScanNode->pScanCols, &pScanNode->node.pTargets));

  return code;
_return:
  planError("%s failed since %d", __func__, code);
  nodesDestroyNode(pCol);
  return code;
}

/*
 * Ensure primary timestamp exists in scan columns and rebuild targets.
 *
 * @param pScanNode Scan node to rewrite.
 * @param pTspk Primary timestamp expression.
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise error code.
 */
static int32_t rebuildScanTargetsWithPrimaryTs(SScanLogicNode* pScanNode, const SNode* pTspk) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pScanCol = NULL;
  SNode*  pNewCol = NULL;
  bool    hasTspk = false;

  if (NULL == pScanNode || NULL == pTspk) {
    planError("%s invalid parameter: scan node or tspk is null", __func__);
    return TSDB_CODE_INVALID_PARA;
  }

  FOREACH(pScanCol, pScanNode->pScanCols) {
    if (nodesEqualNode(pScanCol, pTspk)) {
      hasTspk = true;
      break;
    }
  }

  if (!hasTspk) {
    PLAN_ERR_JRET(nodesCloneNode(pTspk, &pNewCol));
    PLAN_ERR_JRET(nodesListMakeStrictAppend(&pScanNode->pScanCols, pNewCol));
    pNewCol = NULL;
  }

  NODES_DESTORY_LIST(pScanNode->node.pTargets);
  PLAN_ERR_JRET(createColumnByRewriteExprs(pScanNode->pScanCols, &pScanNode->node.pTargets));
  PLAN_ERR_JRET(createColumnByRewriteExprs(pScanNode->pScanPseudoCols, &pScanNode->node.pTargets));

  return code;

_return:
  planError("%s failed , code: %d", __func__, code);
  nodesDestroyNode(pNewCol);
  return code;
}

static int32_t findWindowTspkFromScanCols(SNodeList* pScanCols, const SNode* pTspk,
                                          SNode** ppMatchedTspk) {
  if (NULL == ppMatchedTspk) {
    return TSDB_CODE_INVALID_PARA;
  }
  *ppMatchedTspk = NULL;
  if (NULL == pScanCols || NULL == pTspk) {
    return TSDB_CODE_INVALID_PARA;
  }

  SNode* pScanCol = NULL;
  FOREACH(pScanCol, pScanCols) {
    if (nodesEqualNode(pScanCol, pTspk)) {
      *ppMatchedTspk = pScanCol;
      return TSDB_CODE_SUCCESS;
    }
  }

  if (QUERY_NODE_COLUMN == nodeType(pTspk)) {
    const SColumnNode* pTargetCol = (const SColumnNode*)pTspk;
    FOREACH(pScanCol, pScanCols) {
      if (QUERY_NODE_COLUMN != nodeType(pScanCol)) {
        continue;
      }
      SColumnNode* pCol = (SColumnNode*)pScanCol;
      if (pCol->tableId == pTargetCol->tableId && pCol->colId == pTargetCol->colId) {
        *ppMatchedTspk = pScanCol;
        return TSDB_CODE_SUCCESS;
      }
    }
  }

  FOREACH(pScanCol, pScanCols) {
    if (QUERY_NODE_COLUMN == nodeType(pScanCol) &&
        (((SColumnNode*)pScanCol)->isPrimTs || ((SColumnNode*)pScanCol)->colId == PRIMARYKEY_TIMESTAMP_COL_ID)) {
      *ppMatchedTspk = pScanCol;
      return TSDB_CODE_SUCCESS;
    }
  }

  return TSDB_CODE_PLAN_TSPK_NOT_FOUND;
}

static int32_t rebuildVstbScanTargets(SDynQueryCtrlLogicNode* pDynVstbScan, SLogicNode* pNode);
static int32_t createPartAggRewriteFuns(const SNodeList* pFuncs, SNodeList** pPartialFuncs,
                                        SNodeList** pMidFuncs, SNodeList** pMergeFuncs);
static int32_t vstableWindowOptimizeIntervalSplit(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan,
                                                  SWindowLogicNode* pWindow);
static int32_t vstableWindowOptimizeIntervalNoSplit(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan,
                                                    SWindowLogicNode* pWindow);

/*
 * Check whether all interval-window functions support split execution.
 *
 * @param pWindow Interval window node.
 *
 * @return true when all functions can be split, otherwise false.
 */
static bool vstableIntervalWindowFuncsCanSplit(const SWindowLogicNode* pWindow) {
  SNode* pFunc = NULL;
  FOREACH(pFunc, pWindow->pFuncs) {
    SFunctionNode* pFunction = (SFunctionNode*)pFunc;
    if (!fmIsWindowPseudoColumnFunc(pFunction->funcId) && !fmIsDistExecFunc(pFunction->funcId)) {
      return false;
    }
    if (pFunction->isDistinct) {
      return false;
    }
  }

  return true;
}

typedef struct SVstbNullRewriteCxt {
  int32_t code;
} SVstbNullRewriteCxt;

static EDealRes vstableRewriteColumnToNull(SNode** ppNode, void* pContext) {
  if (QUERY_NODE_COLUMN != nodeType(*ppNode)) {
    return DEAL_RES_CONTINUE;
  }

  SVstbNullRewriteCxt* pCxt = pContext;
  SValueNode* pValue = NULL;
  pCxt->code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pValue);
  if (TSDB_CODE_SUCCESS != pCxt->code || NULL == pValue) {
    return DEAL_RES_ERROR;
  }
  pValue->node.resType = ((SExprNode*)*ppNode)->resType;
  pValue->translate = true;
  pValue->isNull = true;
  nodesDestroyNode(*ppNode);
  *ppNode = (SNode*)pValue;
  return DEAL_RES_IGNORE_CHILD;
}

static int32_t vstableRewritePartialFuncForScan(SFunctionNode* pFunc, SScanLogicNode* pScan, bool ownsFunc) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (ownsFunc || FUNC_RELATED_TO_VSTABLE_LAST_ROW_PARTIAL == pFunc->node.relatedTo) {
    // A split LAST_ROW partial must keep every unit's timestamp stream. A
    // column unavailable in this unit remains a sparse virtual column and is
    // materialized as NULL by the scan; its timestamp still participates in
    // the final LAST_ROW merge.
    SPdaRewriteCxt cxt = {.pScan = pScan, .code = TSDB_CODE_SUCCESS};
    SNode* pNode = (SNode*)pFunc;
    nodesRewriteExpr(&pNode, pdaRewriteExternalColumn, &cxt);
    PLAN_ERR_JRET(cxt.code);
  } else {
    SVstbNullRewriteCxt cxt = {.code = TSDB_CODE_SUCCESS};
    SNode* pNode = (SNode*)pFunc;
    nodesRewriteExpr(&pNode, vstableRewriteColumnToNull, &cxt);
    PLAN_ERR_JRET(cxt.code);
  }
  return code;

_return:
  planError("%s failed since %s", __func__, tstrerror(code));
  return code;
}

static int32_t vstableAppendWstartPlaceholder(SNodeList** ppFuncs, int8_t precision, int32_t* pIndex) {
  int32_t        code = TSDB_CODE_SUCCESS;
  SFunctionNode* pWstart = NULL;
  int32_t index = 0;
  SNode*  pNode = NULL;
  FOREACH(pNode, *ppFuncs) {
    if (QUERY_NODE_FUNCTION == nodeType(pNode) && FUNCTION_TYPE_WSTART == ((SFunctionNode*)pNode)->funcType) {
      *pIndex = index;
      return TSDB_CODE_SUCCESS;
    }
    ++index;
  }

  PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pWstart));
  tstrncpy(pWstart->functionName, "_wstart", TSDB_FUNC_NAME_LEN);
  tstrncpy(pWstart->node.aliasName, "_wstart", TSDB_FUNC_NAME_LEN);
  tstrncpy(pWstart->node.userAlias, "_wstart", TSDB_FUNC_NAME_LEN);
  pWstart->funcType = FUNCTION_TYPE_WSTART;
  pWstart->funcId = fmGetFuncId("_wstart");
  pWstart->node.resType = (SDataType){.type = TSDB_DATA_TYPE_TIMESTAMP,
                                      .bytes = tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes,
                                      .precision = precision};
  SNode* pPendingWstart = (SNode*)pWstart;
  pWstart = NULL;
  PLAN_ERR_JRET(nodesListMakeStrictAppend(ppFuncs, pPendingWstart));
  *pIndex = index;
  return code;

_return:
  nodesDestroyNode((SNode*)pWstart);
  planError("%s failed since %s", __func__, tstrerror(code));
  return code;
}

static int32_t vstableBuildIntervalFuncsForUnit(const SWindowLogicNode* pWindow,
                                                SVirtualScanLogicNode* pVScan,
                                                SScanLogicNode* pUnitOrigin,
                                                SScanLogicNode* pUnitScan,
                                                SNodeList** ppPartialFuncs,
                                                SNodeList** ppMergeFuncs) {
  int32_t        code = TSDB_CODE_SUCCESS;
  SNode*         pNode = NULL;
  SFunctionNode* pPartial = NULL;
  SFunctionNode* pMerge = NULL;
  FOREACH(pNode, pWindow->pFuncs) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    pPartial = NULL;
    pMerge = NULL;

    if (fmIsWindowPseudoColumnFunc(pFunc->funcId) || fmIsPlaceHolderFunc(pFunc->funcId)) {
      PLAN_ERR_JRET(nodesCloneNode(pNode, (SNode**)&pPartial));
      if (NULL != ppMergeFuncs) {
        PLAN_ERR_JRET(nodesCloneNode(pNode, (SNode**)&pMerge));
      }
    } else {
      PLAN_ERR_JRET(fmGetDistMethod(pFunc, &pPartial, NULL, &pMerge));
      SScanLogicNode* pFuncOrigin = NULL;
      bool            crossOrigin = false;
      PLAN_ERR_JRET(vstableExprOriginScan(pNode, pVScan, &pFuncOrigin, &crossOrigin));
      if (crossOrigin) {
        code = TSDB_CODE_PLAN_INTERNAL_ERROR;
        goto _return;
      }
      PLAN_ERR_JRET(vstableRewritePartialFuncForScan(
          pPartial, pUnitScan,
          NULL == pFuncOrigin || vstableSameExecutionUnit(pUnitOrigin, pFuncOrigin)));
    }

    PLAN_ERR_JRET(nodesListMakeStrictAppend(ppPartialFuncs, (SNode*)pPartial));
    pPartial = NULL;
    if (NULL != ppMergeFuncs) {
      PLAN_ERR_JRET(nodesListMakeStrictAppend(ppMergeFuncs, (SNode*)pMerge));
      pMerge = NULL;
    } else {
      nodesDestroyNode((SNode*)pMerge);
      pMerge = NULL;
    }
  }
  return code;

_return:
  nodesDestroyNode((SNode*)pPartial);
  nodesDestroyNode((SNode*)pMerge);
  planError("%s failed since %s", __func__, tstrerror(code));
  return code;
}

static int32_t vstableWindowOptimizeIntervalOneExternalUnit(SOptimizeContext* pCxt,
                                                             SLogicSubplan* pLogicSubplan,
                                                             SWindowLogicNode* pWindow) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  SDynQueryCtrlLogicNode* pDyn = NULL;
  SVirtualScanLogicNode*  pVScan = NULL;
  SScanLogicNode*         pSysScan = NULL;
  SWindowLogicNode*       pHashWindow = NULL;
  SArray*                 pExecutionScans = NULL;

  PLAN_ERR_JRET(nodesCloneNode(nodesListGetNode(pWindow->node.pChildren, 0), (SNode**)&pDyn));
  pVScan = (SVirtualScanLogicNode*)nodesListGetNode(pDyn->node.pChildren, 0);
  pSysScan = (SScanLogicNode*)nodesListGetNode(pDyn->node.pChildren, 1);
  if (NULL == pVScan || NULL == pSysScan) {
    PLAN_ERR_JRET(TSDB_CODE_PLAN_INTERNAL_ERROR);
  }
  PLAN_ERR_JRET(vstableBuildExecutionScans(pVScan, &pExecutionScans));
  if (1 != taosArrayGetSize(pExecutionScans)) {
    PLAN_ERR_JRET(TSDB_CODE_PLAN_INTERNAL_ERROR);
  }
  SScanLogicNode* pScan = taosArrayGetP(pExecutionScans, 0);
  if (NULL == pScan || SCAN_TYPE_EXTERNAL != pScan->scanType) {
    PLAN_ERR_JRET(TSDB_CODE_PLAN_INTERNAL_ERROR);
  }

  PLAN_ERR_JRET(nodesCloneNode((SNode*)pWindow, (SNode**)&pHashWindow));
  NODES_DESTORY_LIST(pHashWindow->node.pChildren);
  pHashWindow->node.pChildren = NULL;
  pHashWindow->windowAlgo = INTERVAL_ALGO_HASH;
  pHashWindow->node.dynamicOp = true;
  SNode* pMatchedTspk = NULL;
  PLAN_ERR_JRET(findWindowTspkFromScanCols(pScan->pScanCols, pHashWindow->pTspk, &pMatchedTspk));
  NODES_DESTORY_NODE(pHashWindow->pTspk);
  PLAN_ERR_JRET(nodesCloneNode(pMatchedTspk, &pHashWindow->pTspk));
  PLAN_ERR_JRET(rebuildScanTargetsWithPrimaryTs(pScan, pHashWindow->pTspk));

  SNode* pFunc = NULL;
  FOREACH(pFunc, pHashWindow->pFuncs) {
    if (!fmIsWindowPseudoColumnFunc(((SFunctionNode*)pFunc)->funcId)) {
      PLAN_ERR_JRET(vstableRewritePartialFuncForScan((SFunctionNode*)pFunc, pScan, true));
    }
  }
  NODES_DESTORY_LIST(pHashWindow->node.pTargets);
  PLAN_ERR_JRET(createColumnByRewriteExprs(pHashWindow->pFuncs, &pHashWindow->node.pTargets));
  PLAN_ERR_JRET(appendNewChild((SLogicNode*)pHashWindow, (SLogicNode*)pScan));
  SScanLogicNode* pNullScan = NULL;
  taosArraySet(pExecutionScans, 0, &pNullScan);

  clearChildList((SLogicNode*)pDyn);
  nodesDestroyNode((SNode*)pVScan);
  pVScan = NULL;
  pDyn->qType = DYN_QTYPE_VTB_INTERVAL;
  pDyn->vtbScan.batchProcessChild = true;
  pDyn->vtbScan.hasPartition = false;
  PLAN_ERR_JRET(rebuildVstbScanTargets(pDyn, (SLogicNode*)pHashWindow));
  PLAN_ERR_JRET(appendNewChild((SLogicNode*)pDyn, (SLogicNode*)pSysScan));
  pSysScan = NULL;
  PLAN_ERR_JRET(appendNewChild((SLogicNode*)pDyn, (SLogicNode*)pHashWindow));
  pHashWindow = NULL;
  PLAN_ERR_JRET(replaceLogicNode(pLogicSubplan, (SLogicNode*)pWindow, (SLogicNode*)pDyn));
  OPTIMIZE_FLAG_SET_MASK(pDyn->node.optimizedFlag, OPTIMIZE_FLAG_VTB_WINDOW);
  pDyn = NULL;
  nodesDestroyNode((SNode*)pWindow);
  pCxt->optimized = true;

_return:
  taosArrayDestroyP(pExecutionScans, (FDelete)nodesDestroyNode);
  nodesDestroyNode((SNode*)pHashWindow);
  nodesDestroyNode((SNode*)pDyn);
  if (TSDB_CODE_SUCCESS != code) {
    planError("%s failed since %s", __func__, tstrerror(code));
  }
  return code;
}

static int32_t vstableWindowOptimizeIntervalMultiUnit(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan,
                                                       SWindowLogicNode* pWindow) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  SDynQueryCtrlLogicNode* pDyn = NULL;
  SVirtualScanLogicNode*  pVScan = NULL;
  SScanLogicNode*         pSysScan = NULL;
  SWindowLogicNode*       pMergeWindow = NULL;
  SMergeLogicNode*        pMerge = NULL;
  SArray*                 pExecutionScans = NULL;
  SNodeList*              pMergeFuncs = NULL;
  SNode*                  pFirstWstartTarget = NULL;
  SScanLogicNode*         pPendingScan = NULL;
  SWindowLogicNode*       pPendingPartial = NULL;
  SOrderByExprNode*       pPendingMergeKey = NULL;

  PLAN_ERR_JRET(nodesCloneNode(nodesListGetNode(pWindow->node.pChildren, 0), (SNode**)&pDyn));
  pVScan = (SVirtualScanLogicNode*)nodesListGetNode(pDyn->node.pChildren, 0);
  pSysScan = (SScanLogicNode*)nodesListGetNode(pDyn->node.pChildren, 1);
  if (NULL == pVScan || NULL == pSysScan) {
    PLAN_ERR_JRET(TSDB_CODE_PLAN_INTERNAL_ERROR);
  }

  PLAN_ERR_JRET(nodesCloneNode((SNode*)pWindow, (SNode**)&pMergeWindow));
  NODES_DESTORY_LIST(pMergeWindow->node.pChildren);
  pMergeWindow->node.pChildren = NULL;
  pMergeWindow->node.dynamicOp = true;
  pMergeWindow->windowAlgo = INTERVAL_ALGO_MERGE;
  NODES_DESTORY_LIST(pMergeWindow->pFuncs);
  pMergeWindow->pFuncs = NULL;

  PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_MERGE, (SNode**)&pMerge));
  pMerge->needSort = true;
  pMerge->groupSort = true;
  pMerge->numOfSubplans = 0;
  pMerge->node.dynamicOp = true;
  pMerge->node.precision = pWindow->node.precision;

  PLAN_ERR_JRET(vstableBuildExecutionScans(pVScan, &pExecutionScans));
  pMerge->numOfChannels = taosArrayGetSize(pExecutionScans);
  int32_t unitIdx = 0;
  for (; unitIdx < taosArrayGetSize(pExecutionScans); ++unitIdx) {
    pPendingScan = taosArrayGetP(pExecutionScans, unitIdx);
    SScanLogicNode* pOrigin = pPendingScan;
    int32_t wstartIdx = -1;

    OPTIMIZE_FLAG_CLEAR_MASK(pPendingScan->node.optimizedFlag, OPTIMIZE_FLAG_SCAN_PATH);

    PLAN_ERR_JRET(nodesCloneNode((SNode*)pWindow, (SNode**)&pPendingPartial));
    NODES_DESTORY_LIST(pPendingPartial->node.pChildren);
    pPendingPartial->node.pChildren = NULL;
    NODES_DESTORY_LIST(pPendingPartial->node.pTargets);
    pPendingPartial->node.pTargets = NULL;
    NODES_DESTORY_LIST(pPendingPartial->pFuncs);
    pPendingPartial->pFuncs = NULL;
    nodesDestroyNode(pPendingPartial->node.pConditions);
    pPendingPartial->node.pConditions = NULL;
    pPendingPartial->windowAlgo = INTERVAL_ALGO_HASH;
    pPendingPartial->node.dynamicOp = true;
    pPendingPartial->node.groupAction = GROUP_ACTION_KEEP;

    SNode* pMatchedTspk = NULL;
    PLAN_ERR_JRET(
        findWindowTspkFromScanCols(pPendingScan->pScanCols, pPendingPartial->pTspk, &pMatchedTspk));
    NODES_DESTORY_NODE(pPendingPartial->pTspk);
    PLAN_ERR_JRET(nodesCloneNode(pMatchedTspk, &pPendingPartial->pTspk));
    PLAN_ERR_JRET(rebuildScanTargetsWithPrimaryTs(pPendingScan, pPendingPartial->pTspk));

    PLAN_ERR_JRET(vstableBuildIntervalFuncsForUnit(pWindow, pVScan, pOrigin, pPendingScan,
                                                   &pPendingPartial->pFuncs,
                                                   0 == unitIdx ? &pMergeFuncs : NULL));
    PLAN_ERR_JRET(vstableAppendWstartPlaceholder(&pPendingPartial->pFuncs, pWindow->node.precision, &wstartIdx));
    PLAN_ERR_JRET(createColumnByRewriteExprs(pPendingPartial->pFuncs, &pPendingPartial->node.pTargets));
    PLAN_ERR_JRET(appendNewChild((SLogicNode*)pPendingPartial, (SLogicNode*)pPendingScan));
    pPendingScan = NULL;
    SScanLogicNode* pNullScan = NULL;
    taosArraySet(pExecutionScans, unitIdx, &pNullScan);

    SNode* pWstartTarget = nodesListGetNode(pPendingPartial->node.pTargets, wstartIdx);
    if (NULL == pWstartTarget) {
      PLAN_ERR_JRET(TSDB_CODE_PLAN_INTERNAL_ERROR);
    }
    if (0 == unitIdx) {
      PLAN_ERR_JRET(nodesCloneList(pPendingPartial->node.pTargets, &pMerge->pInputs));
      PLAN_ERR_JRET(nodesCloneList(pPendingPartial->node.pTargets, &pMerge->node.pTargets));
      PLAN_ERR_JRET(nodesCloneNode(pWstartTarget, &pFirstWstartTarget));
      PLAN_ERR_JRET(createOrderByExprNode(pWstartTarget, ORDER_ASC, &pPendingMergeKey));
      PLAN_ERR_JRET(nodesListMakeStrictAppend(&pMerge->pMergeKeys, (SNode*)pPendingMergeKey));
      pPendingMergeKey = NULL;
    }
    PLAN_ERR_JRET(appendNewChild((SLogicNode*)pMerge, (SLogicNode*)pPendingPartial));
    pPendingPartial = NULL;
  }

  pMergeWindow->pFuncs = pMergeFuncs;
  pMergeFuncs = NULL;
  nodesDestroyNode(pMergeWindow->pTspk);
  pMergeWindow->pTspk = pFirstWstartTarget;
  pFirstWstartTarget = NULL;
  PLAN_ERR_JRET(appendNewChild((SLogicNode*)pMergeWindow, (SLogicNode*)pMerge));
  pMerge = NULL;

  clearChildList((SLogicNode*)pDyn);
  nodesDestroyNode((SNode*)pVScan);
  pVScan = NULL;
  pDyn->qType = DYN_QTYPE_VTB_INTERVAL;
  pDyn->vtbScan.batchProcessChild = true;
  pDyn->vtbScan.hasPartition = false;
  PLAN_ERR_JRET(rebuildVstbScanTargets(pDyn, (SLogicNode*)pMergeWindow));
  PLAN_ERR_JRET(appendNewChild((SLogicNode*)pDyn, (SLogicNode*)pSysScan));
  pSysScan = NULL;
  PLAN_ERR_JRET(appendNewChild((SLogicNode*)pDyn, (SLogicNode*)pMergeWindow));
  pMergeWindow = NULL;

  PLAN_ERR_JRET(replaceLogicNode(pLogicSubplan, (SLogicNode*)pWindow, (SLogicNode*)pDyn));
  OPTIMIZE_FLAG_SET_MASK(pDyn->node.optimizedFlag, OPTIMIZE_FLAG_VTB_WINDOW);
  pDyn = NULL;
  nodesDestroyNode((SNode*)pWindow);
  pCxt->optimized = true;
  taosArrayDestroyP(pExecutionScans, (FDelete)nodesDestroyNode);
  return code;

_return:
  nodesDestroyNode((SNode*)pPendingMergeKey);
  nodesDestroyNode((SNode*)pPendingPartial);
  taosArrayDestroyP(pExecutionScans, (FDelete)nodesDestroyNode);
  nodesDestroyList(pMergeFuncs);
  nodesDestroyNode(pFirstWstartTarget);
  nodesDestroyNode((SNode*)pMerge);
  nodesDestroyNode((SNode*)pMergeWindow);
  nodesDestroyNode((SNode*)pDyn);
  planError("%s failed since %s", __func__, tstrerror(code));
  return code;
}

/*
 * Check whether interval scan requires split by topology or scan flag.
 *
 * @param pScan Scan node.
 *
 * @return true when split is required, otherwise false.
 */
static bool vstableIntervalScanNeedSplit(const SScanLogicNode* pScan) {
  if (SCAN_TYPE_EXTERNAL == pScan->scanType) {
    return false;
  }
  return (NULL != pScan->pVgroupList && pScan->pVgroupList->numOfVgroups > 1) || pScan->needSplit;
}

/*
 * Rewrite interval window into legacy dynamic interval execution layout.
 *
 * @param pCxt Optimizer context.
 * @param pLogicSubplan Logic subplan being optimized.
 * @param pWindow Interval window node.
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise error code.
 */
static int32_t vstableWindowOptimizeIntervalNoSplit(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan,
                                                    SWindowLogicNode* pWindow) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  SWindowLogicNode*       pNewWindow = NULL;
  SWindowLogicNode*       pVirtualChildWindow = NULL;
  SDynQueryCtrlLogicNode* pDynVstbScan = NULL;
  SVirtualScanLogicNode*  pVirtualScanNode = NULL;
  SScanLogicNode*         pScanNode = NULL;
  SScanLogicNode*         pSysScan = NULL;

  OPTIMIZE_FLAG_SET_MASK(pWindow->node.optimizedFlag, OPTIMIZE_FLAG_VTB_WINDOW);

  PLAN_ERR_JRET(nodesCloneNode((SNode*)pWindow, (SNode**)&pNewWindow));

  pDynVstbScan = (SDynQueryCtrlLogicNode*)nodesListGetNode(pNewWindow->node.pChildren, 0);
  if (NULL == pDynVstbScan) {
    PLAN_ERR_JRET(TSDB_CODE_PLAN_INTERNAL_ERROR);
  }

  pVirtualScanNode = (SVirtualScanLogicNode*)nodesListGetNode(pDynVstbScan->node.pChildren, 0);
  pSysScan = (SScanLogicNode*)nodesListGetNode(pDynVstbScan->node.pChildren, 1);
  if (NULL == pVirtualScanNode || NULL == pSysScan) {
    PLAN_ERR_JRET(TSDB_CODE_PLAN_INTERNAL_ERROR);
  }

  pScanNode = (SScanLogicNode*)nodesListGetNode(pVirtualScanNode->node.pChildren, 0);
  if (NULL == pScanNode) {
    PLAN_ERR_JRET(TSDB_CODE_PLAN_INTERNAL_ERROR);
  }

  pDynVstbScan->qType = DYN_QTYPE_VTB_INTERVAL;
  pDynVstbScan->vtbScan.batchProcessChild = true;
  pDynVstbScan->vtbScan.hasPartition = false;

  PLAN_ERR_JRET(nodesCloneNode((SNode*)pNewWindow, (SNode**)&pVirtualChildWindow));
  pVirtualChildWindow->node.dynamicOp = true;
  nodesDestroyList(pVirtualChildWindow->node.pChildren);
  pVirtualChildWindow->node.pChildren = NULL;

  if (SCAN_TYPE_EXTERNAL == pScanNode->scanType) {
    SNode* pMatchedTspk = NULL;
    PLAN_ERR_JRET(
        findWindowTspkFromScanCols(pScanNode->pScanCols, pVirtualChildWindow->pTspk, &pMatchedTspk));
    NODES_DESTORY_NODE(pVirtualChildWindow->pTspk);
    PLAN_ERR_JRET(nodesCloneNode(pMatchedTspk, &pVirtualChildWindow->pTspk));
  }

  PLAN_ERR_JRET(rebuildVstbScanTargets(pDynVstbScan, (SLogicNode*)pVirtualChildWindow));

  clearChildList((SLogicNode*)pNewWindow);
  clearChildList((SLogicNode*)pVirtualScanNode);
  clearChildList((SLogicNode*)pVirtualChildWindow);
  clearChildList((SLogicNode*)pDynVstbScan);

  PLAN_ERR_JRET(appendNewChild((SLogicNode*)pVirtualChildWindow, (SLogicNode*)pScanNode));
  PLAN_ERR_JRET(appendNewChild((SLogicNode*)pDynVstbScan, (SLogicNode*)pSysScan));
  PLAN_ERR_JRET(appendNewChild((SLogicNode*)pDynVstbScan, (SLogicNode*)pVirtualChildWindow));

  PLAN_ERR_JRET(replaceLogicNode(pLogicSubplan, (SLogicNode*)pWindow, (SLogicNode*)pDynVstbScan));

  nodesDestroyNode((SNode*)pNewWindow);
  nodesDestroyNode((SNode*)pVirtualScanNode);
  nodesDestroyNode((SNode*)pWindow);
  pCxt->optimized = true;

  return code;

_return:
  nodesDestroyNode((SNode*)pNewWindow);
  planError("%s failed since %s", __func__, tstrerror(code));
  return code;
}

/*
 * Choose interval-window optimization strategy and apply it.
 *
 * @param pCxt Optimizer context.
 * @param pLogicSubplan Logic subplan being optimized.
 * @param pWindow Interval window node.
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise error code.
 */
static int32_t vstableWindowOptimizeInterval(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan,
                                             SWindowLogicNode* pWindow) {
  int32_t code = TSDB_CODE_SUCCESS;

  SDynQueryCtrlLogicNode* pDynVstbScan = (SDynQueryCtrlLogicNode*)nodesListGetNode(pWindow->node.pChildren, 0);
  if (NULL == pDynVstbScan || nodeType(pDynVstbScan) != QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL) {
    PLAN_ERR_JRET(TSDB_CODE_INVALID_PARA);
  }

  SVirtualScanLogicNode* pVirtualScanNode = (SVirtualScanLogicNode*)nodesListGetNode(pDynVstbScan->node.pChildren, 0);
  if (NULL == pVirtualScanNode || nodeType(pVirtualScanNode) != QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN) {
    PLAN_ERR_JRET(TSDB_CODE_INVALID_PARA);
  }

  if (LIST_LENGTH(pVirtualScanNode->node.pChildren) > 1) {
    int32_t executionUnitCount = vstableExecutionUnitCount(pVirtualScanNode);
    if (1 == executionUnitCount) {
      SScanLogicNode* pFirstScan = (SScanLogicNode*)nodesListGetNode(pVirtualScanNode->node.pChildren, 0);
      if (NULL != pFirstScan && SCAN_TYPE_EXTERNAL == pFirstScan->scanType) {
        PLAN_ERR_JRET(vstableWindowOptimizeIntervalOneExternalUnit(pCxt, pLogicSubplan, pWindow));
        return code;
      }
    }
    if (executionUnitCount > 1 &&
        (0 != LIST_LENGTH(pWindow->pTsmaSubplans) || !vstableIntervalWindowFuncsCanSplit(pWindow))) {
      return TSDB_CODE_SUCCESS;
    }
    PLAN_ERR_JRET(vstableWindowOptimizeIntervalMultiUnit(pCxt, pLogicSubplan, pWindow));
    return code;
  }

  SScanLogicNode* pScanNode = (SScanLogicNode*)nodesListGetNode(pVirtualScanNode->node.pChildren, 0);
  if (NULL != pScanNode && nodeType(pScanNode) == QUERY_NODE_LOGIC_PLAN_SCAN &&
      0 == LIST_LENGTH(pWindow->pTsmaSubplans) && vstableIntervalWindowFuncsCanSplit(pWindow) &&
      vstableIntervalScanNeedSplit(pScanNode)) {
    PLAN_ERR_JRET(vstableWindowOptimizeIntervalSplit(pCxt, pLogicSubplan, pWindow));
    return code;
  }

  PLAN_ERR_JRET(vstableWindowOptimizeIntervalNoSplit(pCxt, pLogicSubplan, pWindow));
  return code;

_return:
  planError("%s failed since %s", __func__, tstrerror(code));
  return code;
}

static int32_t collectVstableWindowBoundaryCols(SWindowLogicNode* pWindow,
                                                SNodeList** ppRequiredCols) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (WINDOW_TYPE_STATE == pWindow->winType) {
    if (NULL == pWindow->pStateExprs || LIST_LENGTH(pWindow->pStateExprs) < 1) {
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }
    PLAN_ERR_JRET(nodesCloneList(pWindow->pStateExprs, ppRequiredCols));
  } else if (WINDOW_TYPE_EVENT == pWindow->winType) {
    PLAN_ERR_JRET(nodesCollectColumnsFromNode(pWindow->pStartCond, NULL,
                                              COLLECT_COL_TYPE_ALL, ppRequiredCols));
    PLAN_ERR_JRET(nodesCollectColumnsFromNode(pWindow->pEndCond, NULL,
                                              COLLECT_COL_TYPE_ALL, ppRequiredCols));
  } else {
    return TSDB_CODE_INVALID_PARA;
  }

_return:
  return code;
}

static int32_t vstableWindowOptimizeImpl(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan,
                                         SWindowLogicNode* pWindow) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  SDynQueryCtrlLogicNode* pOriginDyn = (SDynQueryCtrlLogicNode*)nodesListGetNode(pWindow->node.pChildren, 0);
  SVirtualScanLogicNode*  pOriginVScan =
      (SVirtualScanLogicNode*)nodesListGetNode(pOriginDyn->node.pChildren, 0);
  SWindowLogicNode*       pBoundary = NULL;
  SDynQueryCtrlLogicNode* pRootDyn = NULL;
  SDynQueryCtrlLogicNode* pWinScan = NULL;
  SScanLogicNode*         pRootSysScan = NULL;
  SLogicNode*             pCalc = NULL;
  SSortLogicNode*         pSort = NULL;
  SNodeList*              pCondCols = NULL;

  PLAN_ERR_JRET(nodesCloneNode((SNode*)pWindow, (SNode**)&pBoundary));
  NODES_DESTORY_LIST(pBoundary->node.pChildren);
  pBoundary->node.pChildren = NULL;
  OPTIMIZE_FLAG_SET_MASK(pBoundary->node.optimizedFlag, OPTIMIZE_FLAG_VTB_WINDOW);

  PLAN_ERR_JRET(nodesCloneNode((SNode*)pOriginDyn, (SNode**)&pRootDyn));
  PLAN_ERR_JRET(nodesCloneNode((SNode*)pOriginDyn, (SNode**)&pWinScan));
  pRootDyn->qType = DYN_QTYPE_VTB_WINDOW;
  pRootDyn->vtbWindow.wstartSlotId = -1;
  pRootDyn->vtbWindow.wendSlotId = -1;
  pRootDyn->vtbWindow.wdurationSlotId = -1;
  pRootDyn->vtbWindow.isVstb = true;
  pRootDyn->vtbWindow.extendOption = pWindow->extendOption;
  OPTIMIZE_FLAG_SET_MASK(pRootDyn->node.optimizedFlag, OPTIMIZE_FLAG_VTB_WINDOW);
  NODES_DESTORY_LIST(pRootDyn->node.pTargets);
  PLAN_ERR_JRET(nodesCloneList(pWindow->node.pTargets, &pRootDyn->node.pTargets));
  NODES_DESTORY_LIST(pRootDyn->vtbScan.pScanCols);
  PLAN_ERR_JRET(nodesCloneList(pOriginDyn->node.pTargets, &pRootDyn->vtbScan.pScanCols));

  SVirtualScanLogicNode* pRootVScan =
      (SVirtualScanLogicNode*)nodesListGetNode(pRootDyn->node.pChildren, 0);
  pRootSysScan = (SScanLogicNode*)nodesListGetNode(pRootDyn->node.pChildren, 1);
  clearChildList((SLogicNode*)pRootDyn);
  nodesDestroyNode((SNode*)pRootVScan);
  pRootVScan = NULL;

  PLAN_ERR_JRET(vstableCreateCalcBranches(pBoundary, pOriginVScan, pRootDyn, &pCalc));

  SVirtualScanLogicNode* pWinVScan =
      (SVirtualScanLogicNode*)nodesListGetNode(pWinScan->node.pChildren, 0);
  SScanLogicNode* pWinSysScan = (SScanLogicNode*)nodesListGetNode(pWinScan->node.pChildren, 1);
  if (NULL == pWinVScan || NULL == pWinSysScan) {
    PLAN_ERR_JRET(TSDB_CODE_INVALID_PARA);
  }

  // nodesCloneNode() clones the child list but does not rebuild logic-node
  // parent links.  The split rules use those links to recognize the system
  // scan below DynamicQueryCtrl and the real scans below VirtualScan. Session
  // rebuilds its child list, while State/Event keep the cloned VirtualScan, so
  // repair both levels up front.
  optResetParent((SLogicNode*)pWinScan);
  optResetParent((SLogicNode*)pWinVScan);

  if (WINDOW_TYPE_SESSION == pWindow->winType) {
    SMergeLogicNode* pTsMerge = NULL;
    PLAN_ERR_JRET(vstableCreateSessionTsMerge(pWinVScan, pBoundary->pTsEnd, &pTsMerge));
    clearChildList((SLogicNode*)pWinScan);
    nodesDestroyNode((SNode*)pWinVScan);
    pWinVScan = NULL;
    pWinScan->qType = DYN_QTYPE_VTB_TS_SCAN;
    PLAN_ERR_JRET(appendNewChild((SLogicNode*)pWinScan, (SLogicNode*)pTsMerge));
    PLAN_ERR_JRET(appendNewChild((SLogicNode*)pWinScan, (SLogicNode*)pWinSysScan));
    pWinSysScan = NULL;
    PLAN_ERR_JRET(appendNewChild((SLogicNode*)pBoundary, (SLogicNode*)pWinScan));
    pWinScan = NULL;
  } else {
    PLAN_ERR_JRET(collectVstableWindowBoundaryCols(pBoundary, &pCondCols));
    trimVstableWindowBoundaryScan(pWinScan, pWinVScan, pCondCols);
    SNode* pMatchedTspk = NULL;
    PLAN_ERR_JRET(
        findWindowTspkFromScanCols(pWinScan->node.pTargets, pBoundary->pTspk, &pMatchedTspk));
    NODES_DESTORY_NODE(pBoundary->pTspk);
    PLAN_ERR_JRET(nodesCloneNode(pMatchedTspk, &pBoundary->pTspk));

    PLAN_ERR_JRET(createSortNodeForWinColScan((SColumnNode*)pBoundary->pTspk,
                                              (SLogicNode*)pWinScan, &pSort));
    PLAN_ERR_JRET(appendNewChild((SLogicNode*)pSort, (SLogicNode*)pWinScan));
    pWinScan = NULL;
    PLAN_ERR_JRET(appendNewChild((SLogicNode*)pBoundary, (SLogicNode*)pSort));
    pSort = NULL;
  }

  PLAN_ERR_JRET(appendNewChild((SLogicNode*)pRootDyn, (SLogicNode*)pBoundary));
  pBoundary = NULL;
  PLAN_ERR_JRET(appendNewChild((SLogicNode*)pRootDyn, (SLogicNode*)pRootSysScan));
  pRootSysScan = NULL;
  PLAN_ERR_JRET(appendNewChild((SLogicNode*)pRootDyn, pCalc));
  pCalc = NULL;
  PLAN_ERR_JRET(replaceLogicNode(pLogicSubplan, (SLogicNode*)pWindow, (SLogicNode*)pRootDyn));
  pRootDyn = NULL;
  nodesDestroyNode((SNode*)pWindow);
  pCxt->optimized = true;

_return:
  nodesDestroyList(pCondCols);
  nodesDestroyNode((SNode*)pSort);
  nodesDestroyNode((SNode*)pCalc);
  nodesDestroyNode((SNode*)pBoundary);
  nodesDestroyNode((SNode*)pWinScan);
  nodesDestroyNode((SNode*)pRootDyn);
  if (TSDB_CODE_SUCCESS != code) {
    planError("%s failed since %s", __func__, tstrerror(code));
  }
  return code;
}

// this rule optimize plan from

// Window -> DynQueryCtrl -> VirtualScan -> TableScan
//
// to plan
//
// DynQueryCtrl -> Window -> Sort(order by ts) -> DynQueryCtrl(only scan cols needed by window) -> VirtualScan -> TableScan
//              \
//               \> ExternalWindow -> DynQueryCtrl(scan other cols) -> VirtualScan -> TableScan
static int32_t vstableWindowOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  if (inStreamCalcClause(pCxt->pPlanCxt)) {
    // stream calc query does not support
    return TSDB_CODE_SUCCESS;
  }
  SWindowLogicNode* pWindow = (SWindowLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, vstableWindowMayBeOptimized, NULL);
  if (NULL == pWindow) {
    return TSDB_CODE_SUCCESS;
  }

  switch (pWindow->winType) {
    case WINDOW_TYPE_INTERVAL:
      return vstableWindowOptimizeInterval(pCxt, pLogicSubplan, pWindow);
    case WINDOW_TYPE_EVENT:
    case WINDOW_TYPE_SESSION:
    case WINDOW_TYPE_STATE:
      return vstableWindowOptimizeImpl(pCxt, pLogicSubplan, pWindow);
    default:
      return TSDB_CODE_SUCCESS;
  }

}

static bool vstableWindowSortMayBeOptimized(SLogicNode* pNode, void* pCtx) {
  if (OPTIMIZE_FLAG_TEST_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_VTB_WINDOW)) {
    return false;
  }
  if (nodeType(pNode) != QUERY_NODE_LOGIC_PLAN_WINDOW || LIST_LENGTH(pNode->pChildren) != 1) {
    return false;
  }

  SNode *pChild = nodesListGetNode(pNode->pChildren, 0);
  if (NULL == pChild || nodeType(pChild) != QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL || ((SDynQueryCtrlLogicNode*)pChild)->qType != DYN_QTYPE_VTB_SCAN) {
    return false;
  }

  return true;
}

static int32_t vstableWindowSortOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SWindowLogicNode* pWindow = (SWindowLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, vstableWindowSortMayBeOptimized, NULL);
  if (NULL == pWindow) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;
  SWindowLogicNode*       pNewWindow = NULL;
  SDynQueryCtrlLogicNode* pDynVstbScan = NULL;
  SNode*                  pFunc = NULL;
  SSortLogicNode*         pSort = NULL;
  SVirtualScanLogicNode*  pVirtualScanNode = NULL;
  SScanLogicNode*         pSysScan = NULL;

  PLAN_ERR_JRET(nodesCloneNode((SNode*)pWindow, (SNode**)&pNewWindow));
  OPTIMIZE_FLAG_SET_MASK(pNewWindow->node.optimizedFlag, OPTIMIZE_FLAG_VTB_WINDOW);

  pDynVstbScan = (SDynQueryCtrlLogicNode*)nodesListGetNode(pNewWindow->node.pChildren, 0);
  QUERY_CHECK_NULL(pDynVstbScan, code, lino, _return, terrno)

  pVirtualScanNode = (SVirtualScanLogicNode*)nodesListGetNode(pDynVstbScan->node.pChildren, 0);
  pVirtualScanNode->node.pParent = (SLogicNode*)pDynVstbScan;

  pSysScan = (SScanLogicNode*)nodesListGetNode(pDynVstbScan->node.pChildren, 1);
  pSysScan->node.pParent = (SLogicNode*)pDynVstbScan;

  SNode* pScanNode = NULL;
  FOREACH(pScanNode, pVirtualScanNode->node.pChildren) {
    ((SLogicNode*)pScanNode)->pParent = (SLogicNode*)pVirtualScanNode;
  }

  // create sort node
  PLAN_ERR_JRET(createSortNodeForWinColScan((SColumnNode*)pNewWindow->pTspk,(SLogicNode*)pDynVstbScan, &pSort));
  pDynVstbScan->node.pParent = (SLogicNode*)pSort;
  pSort->node.pParent = (SLogicNode*)pNewWindow;
  PLAN_ERR_JRET(nodesListMakeAppend(&pSort->node.pChildren, (SNode*)pDynVstbScan));

  nodesClearList(pNewWindow->node.pChildren);
  pNewWindow->node.pChildren = NULL;
  PLAN_ERR_JRET(nodesListMakeAppend(&pNewWindow->node.pChildren, (SNode*)pSort));

  PLAN_ERR_JRET(replaceLogicNode(pLogicSubplan, (SLogicNode*)pWindow, (SLogicNode*)pNewWindow));

  nodesDestroyNode((SNode*)pWindow);
  pCxt->optimized = true;

  return code;
_return:
  nodesDestroyNode((SNode*)pNewWindow);
  planError("failed to optimize vstable window at line %d, error code: %d", lino, code);
  return code;
}

static bool aggNodeHasTag(SAggLogicNode* pAgg) {
  SNode* pAggFunc = NULL;
  FOREACH(pAggFunc, pAgg->pAggFuncs) {
    SFunctionNode *pFunc = (SFunctionNode *)pAggFunc;
    if (functionHasTagParam(pFunc)) {
      return true;
    }
  }
  return false;
}

typedef struct SVstableTagDetectCxt {
  const SVirtualScanLogicNode* pVScan;
  bool                         hasTag;
} SVstableTagDetectCxt;

static bool vstableTagScanHasColumn(const SVirtualScanLogicNode* pVScan,
                                    const SColumnNode* pColumn) {
  SNode* pNode = NULL;
  FOREACH(pNode, pVScan->node.pChildren) {
    if (QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pNode) ||
        SCAN_TYPE_TAG != ((SScanLogicNode*)pNode)->scanType) {
      continue;
    }
    const SScanLogicNode* pTagScan = (const SScanLogicNode*)pNode;
    SNode* pTagNode = NULL;
    FOREACH(pTagNode, pTagScan->pScanCols) {
      if (QUERY_NODE_COLUMN == nodeType(pTagNode) &&
          (((SColumnNode*)pTagNode)->colId == pColumn->colId ||
           0 == strcasecmp(((SColumnNode*)pTagNode)->colName, pColumn->colName))) {
        return true;
      }
    }
    FOREACH(pTagNode, pTagScan->pScanPseudoCols) {
      if (QUERY_NODE_COLUMN == nodeType(pTagNode) &&
          (((SColumnNode*)pTagNode)->colId == pColumn->colId ||
           0 == strcasecmp(((SColumnNode*)pTagNode)->colName, pColumn->colName))) {
        return true;
      }
    }
  }
  return false;
}

static EDealRes vstableFindTagParam(SNode* pNode, void* pContext) {
  SVstableTagDetectCxt* pCxt = pContext;
  if ((QUERY_NODE_FUNCTION == nodeType(pNode) &&
       (FUNCTION_TYPE_TBNAME == ((SFunctionNode*)pNode)->funcType ||
        0 == strcasecmp(((SFunctionNode*)pNode)->functionName, "tbname"))) ||
      (QUERY_NODE_COLUMN == nodeType(pNode) &&
       (COLUMN_TYPE_TAG == ((SColumnNode*)pNode)->colType ||
        COLUMN_TYPE_TBNAME == ((SColumnNode*)pNode)->colType ||
        0 == strcasecmp(((SColumnNode*)pNode)->colName, "tbname") ||
        vstableTagScanHasColumn(pCxt->pVScan, (SColumnNode*)pNode)))) {
    pCxt->hasTag = true;
    return DEAL_RES_END;
  }
  return DEAL_RES_CONTINUE;
}

static bool vstableExprHasTagParam(SNode* pExpr,
                                   const SVirtualScanLogicNode* pVScan) {
  SVstableTagDetectCxt cxt = {.pVScan = pVScan, .hasTag = false};
  nodesWalkExpr(pExpr, vstableFindTagParam, &cxt);
  return cxt.hasTag;
}

static bool vstableAggNodeHasTag(const SAggLogicNode* pAgg,
                                 const SVirtualScanLogicNode* pVScan) {
  SNode* pFunc = NULL;
  FOREACH(pFunc, pAgg->pAggFuncs) {
    if (vstableExprHasTagParam(pFunc, pVScan)) {
      return true;
    }
  }
  return false;
}

static bool vstableAggFederatedCanOptimize(SOptimizeContext* pCxt, SAggLogicNode* pAgg,
                                            SDynQueryCtrlLogicNode* pDyn,
                                            SVirtualScanLogicNode* pVScan);

static EDealRes vstableFindNestedAggColumn(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode) ||
      (QUERY_NODE_FUNCTION == nodeType(pNode) &&
       (FUNCTION_TYPE_TBNAME == ((SFunctionNode*)pNode)->funcType ||
        0 == strcasecmp(((SFunctionNode*)pNode)->functionName, "tbname")))) {
    *(bool*)pContext = true;
    return DEAL_RES_END;
  }
  return DEAL_RES_CONTINUE;
}

static bool vstableAggFuncHasOnlyDirectColumnParams(const SFunctionNode* pFunc) {
  SNode* pParam = NULL;
  FOREACH(pParam, pFunc->pParameterList) {
    if (QUERY_NODE_COLUMN == nodeType(pParam) ||
        (QUERY_NODE_FUNCTION == nodeType(pParam) &&
         (FUNCTION_TYPE_TBNAME == ((SFunctionNode*)pParam)->funcType ||
          0 == strcasecmp(((SFunctionNode*)pParam)->functionName, "tbname")))) {
      continue;
    }

    bool hasNestedColumn = false;
    nodesWalkExpr(pParam, vstableFindNestedAggColumn, &hasNestedColumn);
    if (hasNestedColumn) {
      return false;
    }
  }
  return true;
}

static bool vstableAggFederatedZeroDataFuncCanOptimize(const SFunctionNode* pFunc) {
  if (NULL == pFunc) {
    return false;
  }
  switch (pFunc->funcType) {
    case FUNCTION_TYPE_MIN:
    case FUNCTION_TYPE_MAX:
    case FUNCTION_TYPE_FIRST:
    case FUNCTION_TYPE_LAST:
    case FUNCTION_TYPE_LAST_ROW:
      return true;
    default:
      return false;
  }
}

// In stream calc, the VTB_AGG pushdown reader scatters per-source-table results back to
// virtual-table columns. When a virtual table's columns interleave references across more
// than one physical source table (e.g. cols [a.x, b.x, a.y]  -  table `a` reused
// non-contiguously), the pushdown reader's scatter does not handle the regrouped order and
// silently drops/swaps results, so the stream emits nothing. The non-stream (batch) path and
// the plain VTB_SCAN reader handle this correctly. Detect the shape on the VirtualScan's
// scan columns: bail if any source (refDbName.refTableName) appears non-contiguously.
static bool vscanRefsInterleaveSourceTables(SVirtualScanLogicNode* pVscan) {
  if (NULL == pVscan) {
    return false;
  }
  char    prevDb[TSDB_DB_NAME_LEN] = {0};
  char    prevTb[TSDB_TABLE_NAME_LEN] = {0};
  bool    havePrev = false;
  SSHashObj* pSeen = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
  if (NULL == pSeen) {
    return false;  // on OOM, don't change behavior
  }
  bool    interleaved = false;
  SNode*  pNode = NULL;
  FOREACH(pNode, pVscan->pScanCols) {
    if (QUERY_NODE_COLUMN != nodeType(pNode)) {
      continue;
    }
    SColumnNode* pCol = (SColumnNode*)pNode;
    if (!pCol->hasRef) {
      continue;
    }
    char key[TSDB_DB_NAME_LEN + TSDB_TABLE_NAME_LEN + 2] = {0};
    int32_t klen = snprintf(key, sizeof(key), "%s.%s", pCol->refDbName, pCol->refTableName);
    bool sameAsPrev = havePrev && 0 == strcmp(prevDb, pCol->refDbName) &&
                      0 == strcmp(prevTb, pCol->refTableName);
    if (!sameAsPrev) {
      // a new source-table run begins; if we have seen this source before, it is non-contiguous
      if (NULL != tSimpleHashGet(pSeen, key, (size_t)klen)) {
        interleaved = true;
        break;
      }
      int32_t putCode = tSimpleHashPut(pSeen, key, (size_t)klen, &interleaved, sizeof(interleaved));
      if (TSDB_CODE_SUCCESS != putCode) {
        planError("vscanRefsInterleaveSourceTables failed to record source %s, code:%s",
                  key, tstrerror(putCode));
      }
    }
    tstrncpy(prevDb, pCol->refDbName, TSDB_DB_NAME_LEN);
    tstrncpy(prevTb, pCol->refTableName, TSDB_TABLE_NAME_LEN);
    havePrev = true;
  }
  tSimpleHashCleanup(pSeen);
  return interleaved;
}

static bool vstableAggShouldBeOptimized(SLogicNode* pNode, void* pCtx) {
  SOptimizeContext* pCxt = (SOptimizeContext*)pCtx;
  if (QUERY_NODE_LOGIC_PLAN_AGG != nodeType(pNode)) {
    return false;
  }

  if (OPTIMIZE_FLAG_TEST_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_VTB_AGG)) {
    return false;
  }

  SAggLogicNode* pAgg = (SAggLogicNode*)pNode;
  bool           isFederatedVstable = false;
  SVirtualScanLogicNode* pFederatedVScan = NULL;

  // In a stream calc subplan, pdcDealVirtualSuperTableScan keeps all WHERE conditions on
  // the VirtualScan instead of splitting them down to the system/original scans. Since this
  // optimization destroys the VirtualScan while pushing the aggregation down, any residual
  // filter (WHERE on the virtual table, or HAVING on the aggregate) would be silently lost.
  // Only optimize the stream case when no such condition remains, which is the common
  // `select agg(col) from vstb` shape; otherwise fall back to the unoptimized correct path.
  bool isStreamCalc = (NULL != pCxt && inStreamCalcClause(pCxt->pPlanCxt));
  if (isStreamCalc && NULL != pAgg->node.pConditions) {
    return false;
  }

  // Stream calc does not support pushing the virtual super table aggregation down when the
  // query groups/partitions by virtual-child identity (e.g. `group by tbname/tag` or
  // `partition by tbname/tag`). In stream the partial aggregate is fused into the per-vgroup
  // original-table reader (see SPLIT_FLAG_VTB_AGG_PUSHDOWN), where the virtual-child identity
  // injected by DynQueryCtrl is not available, so the per-group key would collapse and the
  // result would be wrong. Fall back to the unoptimized path for these shapes.
  if (isStreamCalc) {
    SLogicNode* pAggChild = (SLogicNode*)nodesListGetNode(pAgg->node.pChildren, 0);
    if (NULL != pAgg->pGroupKeys ||
        (NULL != pAggChild && QUERY_NODE_LOGIC_PLAN_PARTITION == nodeType(pAggChild))) {
      return false;
    }
  }

  if (NULL != pAgg->pGroupKeys) {
    if (keysHasCol(pAgg->pGroupKeys)) {
      return false;
    }

    SNode* pGroupKey = NULL;
    FOREACH(pGroupKey, pAgg->pGroupKeys) {
      if (nodeType(pGroupKey) != QUERY_NODE_GROUPING_SET) {
        return false;
      }
      SNode* pGroupExpr = nodesListGetNode(((SGroupingSetNode*)pGroupKey)->pParameterList, 0);
      if (!pGroupExpr || partTagsHasRealConsumer(pAgg, pGroupExpr)) {
        return false;
      }
    }
  }

  if (LIST_LENGTH(pAgg->node.pChildren) == 1) {
    SDynQueryCtrlLogicNode* pDynCtrl = NULL;
    if (nodeType(nodesListGetNode(pAgg->node.pChildren, 0)) == QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL) {
      pDynCtrl = (SDynQueryCtrlLogicNode*)nodesListGetNode(pAgg->node.pChildren, 0);
      if (pDynCtrl->qType != DYN_QTYPE_VTB_SCAN) {
        return false;
      }
    } else if (nodeType(nodesListGetNode(pAgg->node.pChildren, 0)) == QUERY_NODE_LOGIC_PLAN_PARTITION) {
        SPartitionLogicNode* pPart = (SPartitionLogicNode*)nodesListGetNode(pAgg->node.pChildren, 0);
        if (LIST_LENGTH(pPart->node.pChildren) != 1 ||
            nodeType(nodesListGetNode(pPart->node.pChildren, 0)) != QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL) {
          return false;
        }
        pDynCtrl = (SDynQueryCtrlLogicNode*)nodesListGetNode(pPart->node.pChildren, 0);
        if (pDynCtrl->qType != DYN_QTYPE_VTB_SCAN) {
          return false;
        }
        if (keysHasCol(pPart->pPartitionKeys)) {
          return false;
        }
    } else {
      return false;
    }

    // VStableAgg restructures the plan, destroying VirtualScan.  If the virtual
    // super table has tag refs (hasTagRef), VirtualScan may carry ref-tag conditions
    // that cannot be evaluated elsewhere (ref-tag values are NULL in vnode meta).
    // Only skip VStableAgg when hasTagRef is true AND VirtualScan has conditions.
    // When hasTagRef is false, all tag conditions have been pushed to SystemScan
    // by pdcSplitTagCondForVstb, so VStableAgg is safe.
    if (pDynCtrl && LIST_LENGTH(pDynCtrl->node.pChildren) >= 1) {
      // In stream calc, residual filters are not split out of the DynQueryCtrl/VirtualScan
      // (see comment above), so bail out whenever any condition is still attached to them.
      if (isStreamCalc && NULL != pDynCtrl->node.pConditions) {
        return false;
      }
      SLogicNode* pFirst = (SLogicNode*)nodesListGetNode(pDynCtrl->node.pChildren, 0);
      if (QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN == nodeType(pFirst)) {
        SVirtualScanLogicNode* pVscan = (SVirtualScanLogicNode*)pFirst;
        // Filter groups can overlap in time; aggregate only after VirtualScan reconstructs each row.
        if (pVscan->hasFilterCond) {
          return false;
        }
        if ((pVscan->hasTagRef || isStreamCalc) && pFirst->pConditions) {
          return false;
        }
        // In stream calc, the VTB_AGG pushdown reader cannot scatter results correctly when
        // the virtual table interleaves column refs across multiple physical source tables
        // (see vscanRefsInterleaveSourceTables). Fall back to the unoptimized VTB_SCAN path.
        if (isStreamCalc && vscanRefsInterleaveSourceTables(pVscan)) {
          return false;
        }
        if (vstableHasExternalExecutionUnit(pVscan) &&
            !vstableAggFederatedCanOptimize(pCxt, pAgg, pDynCtrl, pVscan)) {
          return false;
        }
        isFederatedVstable = vstableHasExternalExecutionUnit(pVscan);
        pFederatedVScan = isFederatedVstable ? pVscan : NULL;
      }
    }
  } else {
    return false;
  }

  if (LIST_LENGTH(pAgg->pAggFuncs) == 0) {
    return false;
  }
  SNode* pAggFunc = NULL;
  FOREACH(pAggFunc, pAgg->pAggFuncs) {
    SFunctionNode *pFunc = (SFunctionNode *)pAggFunc;
    if (fmIsSelectValueFunc(pFunc->funcId) || fmisSelectGroupConstValueFunc(pFunc->funcId) ||
        fmIsGroupKeyFunc(pFunc->funcId) ||
        functionHasTagRefParam(pFunc) ||
        (fmIsAggFunc(pFunc->funcId) && !fmIsSelectFunc(pFunc->funcId) &&
         functionHasTagOrPkParam(pFunc) &&
         !(isFederatedVstable &&
           vstableExprHasTagParam(pAggFunc, pFederatedVScan))) ||
        !fmIsDistExecFunc(pFunc->funcId) || pFunc->isDistinct) {
      return false;
    }
  }

  return true;
}

static bool vstableAggFederatedCanOptimize(SOptimizeContext* pCxt, SAggLogicNode* pAgg,
                                            SDynQueryCtrlLogicNode* pDyn,
                                            SVirtualScanLogicNode* pVScan) {
  if (NULL == pCxt || NULL == pAgg || NULL == pDyn || NULL == pVScan ||
      inStreamCalcClause(pCxt->pPlanCxt) || NULL != pDyn->node.pConditions ||
      NULL != pVScan->node.pConditions || !vstableHasExternalExecutionUnit(pVScan)) {
    return false;
  }

  int32_t executionUnitCount = vstableExecutionUnitCount(pVScan);
  if (executionUnitCount <= 0) {
    return false;
  }

  bool hasTagAgg = vstableAggNodeHasTag(pAgg, pVScan);
  bool hasPartition =
      1 == LIST_LENGTH(pAgg->node.pChildren) &&
      QUERY_NODE_LOGIC_PLAN_PARTITION ==
          nodeType(nodesListGetNode(pAgg->node.pChildren, 0));

  if (hasPartition && vscanRefsInterleaveSourceTables(pVScan)) {
    return false;
  }

  // A tag argument or tag/tbname partition needs one row-producing table per
  // execution unit. If an execution unit batches multiple origin tables, their
  // timestamp domains may overlap; partial aggregation can no longer prove the
  // virtual row count or bind every dynamic partition scope.
  if (hasTagAgg || hasPartition) {
    SNode* pScanNode = NULL;
    FOREACH(pScanNode, pVScan->node.pChildren) {
      SScanLogicNode* pScan = (SScanLogicNode*)pScanNode;
      if (SCAN_TYPE_TAG == pScan->scanType) {
        continue;
      }
      SColumnNode* pFirstRef = NULL;
      SNode*       pScanColNode = NULL;
      FOREACH(pScanColNode, pScan->pScanCols) {
        if (QUERY_NODE_COLUMN != nodeType(pScanColNode)) {
          continue;
        }
        SColumnNode* pScanCol = (SColumnNode*)pScanColNode;
        if (!pScanCol->hasRef || '\0' == pScanCol->refTableName[0]) {
          continue;
        }
        if (NULL == pFirstRef) {
          pFirstRef = pScanCol;
        } else if (0 != strcmp(pFirstRef->refSourceName, pScanCol->refSourceName) ||
                   0 != strcmp(pFirstRef->refDbName, pScanCol->refDbName) ||
                   0 != strcmp(pFirstRef->refTableName, pScanCol->refTableName)) {
          return false;
        }
      }
      SNode* pPreviousNode = NULL;
      FOREACH(pPreviousNode, pVScan->node.pChildren) {
        if (pPreviousNode == pScanNode) {
          break;
        }
        SScanLogicNode* pPrevious = (SScanLogicNode*)pPreviousNode;
        if (SCAN_TYPE_TAG != pPrevious->scanType &&
            vstableSameExecutionUnit(pPrevious, pScan)) {
          return false;
        }
      }
    }
  }

  SNode* pOrigin = NULL;
  FOREACH(pOrigin, pVScan->node.pChildren) {
    if (QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pOrigin)) {
      return false;
    }
    SScanLogicNode* pScan = (SScanLogicNode*)pOrigin;
    if (SCAN_TYPE_TAG == pScan->scanType) {
      continue;
    }
    if (NULL != pScan->node.pConditions || !externalScanSafeForVstableAgg(pScan)) {
      return false;
    }
  }

  SNode* pFuncNode = NULL;
  FOREACH(pFuncNode, pAgg->pAggFuncs) {
    SFunctionNode* pFunc = (SFunctionNode*)pFuncNode;
    if (!vstableAggFuncHasOnlyDirectColumnParams(pFunc)) {
      return false;
    }

    int32_t dataColumnCount = 0;
    if (TSDB_CODE_SUCCESS != vstableExprDataColumnCount(pFuncNode, &dataColumnCount)) {
      return false;
    }
    if (0 == dataColumnCount && !vstableAggFederatedZeroDataFuncCanOptimize(pFunc)) {
      // MIN/MAX are idempotent for TS/Tag/TBNAME. FIRST/LAST/LAST_ROW carry
      // their ordering timestamp in the partial state. Other zero-data-column
      // functions are row-count-sensitive over the cross-source timestamp
      // union and must keep the VirtualScan fallback.
      return false;
    }

    SScanLogicNode* pFuncOrigin = NULL;
    bool            crossOrigin = false;
    if (TSDB_CODE_SUCCESS != vstableExprOriginScan(pFuncNode, pVScan, &pFuncOrigin, &crossOrigin) ||
        crossOrigin) {
      // Multiple ordinary parameters are valid when every one resolves to the
      // same execution unit; only a true cross-unit function is unsupported.
      return false;
    }

    SFunctionNode* pPartial = NULL;
    SFunctionNode* pMid = NULL;
    SFunctionNode* pMerge = NULL;
    int32_t code = fmGetDistMethod(pFunc, &pPartial, &pMid, &pMerge);
    nodesDestroyNode((SNode*)pPartial);
    nodesDestroyNode((SNode*)pMid);
    nodesDestroyNode((SNode*)pMerge);
    if (TSDB_CODE_SUCCESS != code) {
      return false;
    }
  }

  return true;
}

static int32_t createPartAggRewriteFuns(const SNodeList* pFuncs, SNodeList** pPartialFuncs, SNodeList** pMidFuncs, SNodeList** pMergeFuncs) {
  int32_t        code = TSDB_CODE_SUCCESS;
  SFunctionNode* pPartFunc = NULL;
  SFunctionNode* pMidFunc = NULL;
  SFunctionNode* pMergeFunc = NULL;
  SNode*         pNode = NULL;
  
  FOREACH(pNode, pFuncs) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    if (fmIsWindowPseudoColumnFunc(pFunc->funcId) || fmIsPlaceHolderFunc(pFunc->funcId)) {
      PLAN_ERR_JRET(nodesCloneNode(pNode, (SNode**)&pPartFunc));
      PLAN_ERR_JRET(nodesCloneNode(pNode, (SNode**)&pMergeFunc));
      if (pMidFuncs != NULL){
        PLAN_ERR_JRET(nodesCloneNode(pNode, (SNode**)&pMidFunc));
      }
    } else {
      PLAN_ERR_JRET(fmGetDistMethod(pFunc, &pPartFunc, &pMidFunc, &pMergeFunc));
    }
    PLAN_ERR_JRET(nodesListMakeStrictAppend(pPartialFuncs, (SNode*)pPartFunc));
    pPartFunc = NULL;
    if (pMidFuncs != NULL) {
      PLAN_ERR_JRET(nodesListMakeStrictAppend(pMidFuncs, (SNode*)pMidFunc));
    } else {
      nodesDestroyNode((SNode*)pMidFunc);
    }
    pMidFunc = NULL;
    PLAN_ERR_JRET(nodesListMakeStrictAppend(pMergeFuncs, (SNode*)pMergeFunc));
    pMergeFunc = NULL;
  }
  return code;
_return:
  planError("%s failed, code: %d", __func__, code);
  nodesDestroyNode((SNode*)pPartFunc);
  nodesDestroyNode((SNode*)pMidFunc);
  nodesDestroyNode((SNode*)pMergeFunc);
  return code;
}

/*
 * Rewrite interval window into split-interval dynamic execution layout.
 *
 * @param pCxt Optimizer context.
 * @param pLogicSubplan Logic subplan being optimized.
 * @param pWindow Interval window node.
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise error code.
 */
static int32_t vstableWindowOptimizeIntervalSplit(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan,
                                                  SWindowLogicNode* pWindow) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;
  SWindowLogicNode*       pMergeWindow = NULL;
  SDynQueryCtrlLogicNode* pDynVstbScan = NULL;
  SVirtualScanLogicNode*  pVirtualScanNode = NULL;
  SScanLogicNode*         pScanNode = NULL;
  SScanLogicNode*         pSysScan = NULL;
  SVirtualScanLogicNode*  pDetachedVirtualScan = NULL;
  SScanLogicNode*         pDetachedScan = NULL;
  SScanLogicNode*         pDetachedSysScan = NULL;
  bool                    dynDetached = false;

  PLAN_ERR_JRET(nodesCloneNode((SNode*)pWindow, (SNode**)&pMergeWindow));
  OPTIMIZE_FLAG_SET_MASK(pMergeWindow->node.optimizedFlag, OPTIMIZE_FLAG_VTB_WINDOW);
  pMergeWindow->node.dynamicOp = true;

  pDynVstbScan = (SDynQueryCtrlLogicNode*)nodesListGetNode(pMergeWindow->node.pChildren, 0);
  QUERY_CHECK_NULL(pDynVstbScan, code, lino, _return, terrno)

  pVirtualScanNode = (SVirtualScanLogicNode*)nodesListGetNode(pDynVstbScan->node.pChildren, 0);
  pSysScan = (SScanLogicNode*)nodesListGetNode(pDynVstbScan->node.pChildren, 1);
  QUERY_CHECK_NULL(pVirtualScanNode, code, lino, _return, terrno)
  QUERY_CHECK_NULL(pSysScan, code, lino, _return, terrno)

  pScanNode = (SScanLogicNode*)nodesListGetNode(pVirtualScanNode->node.pChildren, 0);
  QUERY_CHECK_NULL(pScanNode, code, lino, _return, terrno)

  if (SCAN_TYPE_EXTERNAL == pScanNode->scanType) {
    SNode* pMatchedTspk = NULL;
    PLAN_ERR_JRET(
        findWindowTspkFromScanCols(pScanNode->pScanCols, pMergeWindow->pTspk, &pMatchedTspk));
    NODES_DESTORY_NODE(pMergeWindow->pTspk);
    PLAN_ERR_JRET(nodesCloneNode(pMatchedTspk, &pMergeWindow->pTspk));
  }

  pDynVstbScan->qType = DYN_QTYPE_VTB_INTERVAL;
  pDynVstbScan->vtbScan.batchProcessChild = true;
  pDynVstbScan->vtbScan.hasPartition = false;

  clearChildList((SLogicNode*)pVirtualScanNode);
  pDetachedScan = pScanNode;
  clearChildList((SLogicNode*)pMergeWindow);
  dynDetached = true;
  PLAN_ERR_JRET(appendNewChild((SLogicNode*)pMergeWindow, (SLogicNode*)pScanNode));
  pDetachedScan = NULL;

  OPTIMIZE_FLAG_CLEAR_MASK(pScanNode->node.optimizedFlag, OPTIMIZE_FLAG_SCAN_PATH);
  PLAN_ERR_JRET(rebuildScanTargetsWithPrimaryTs(pScanNode, pMergeWindow->pTspk));
  PLAN_ERR_JRET(rebuildVstbScanTargets(pDynVstbScan, (SLogicNode*)pMergeWindow));

  clearChildList((SLogicNode*)pDynVstbScan);
  pDetachedVirtualScan = pVirtualScanNode;
  pDetachedSysScan = pSysScan;
  nodesDestroyNode((SNode*)pDetachedVirtualScan);
  pDetachedVirtualScan = NULL;
  pVirtualScanNode = NULL;

  PLAN_ERR_JRET(appendNewChild((SLogicNode*)pDynVstbScan, (SLogicNode*)pSysScan));
  pDetachedSysScan = NULL;
  PLAN_ERR_JRET(appendNewChild((SLogicNode*)pDynVstbScan, (SLogicNode*)pMergeWindow));
  pMergeWindow = NULL;

  PLAN_ERR_JRET(replaceLogicNode(pLogicSubplan, (SLogicNode*)pWindow, (SLogicNode*)pDynVstbScan));
  pDynVstbScan = NULL;

  nodesDestroyNode((SNode*)pWindow);
  pCxt->optimized = true;
  return code;

_return:
  nodesDestroyNode((SNode*)pDetachedVirtualScan);
  nodesDestroyNode((SNode*)pDetachedSysScan);
  nodesDestroyNode((SNode*)pDetachedScan);
  if (dynDetached) {
    nodesDestroyNode((SNode*)pDynVstbScan);
  }
  nodesDestroyNode((SNode*)pMergeWindow);
  planError("failed to split optimize vstable interval window at line %d, error code: %d", lino, code);
  return code;
}

static int32_t createPartAggNode(SAggLogicNode* pMergeAgg, SLogicNode** pOutput) {
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

  SAggLogicNode* pPartAgg = NULL;
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;

  PLAN_ERR_JRET(nodesCloneNode((SNode*)pMergeAgg, (SNode**)&pPartAgg));

  pPartAgg->node.groupAction = GROUP_ACTION_KEEP;

  if (NULL != pGroupKeys) {
    pPartAgg->pGroupKeys = pGroupKeys;
    PLAN_ERR_JRET(createColumnByRewriteExprs(pPartAgg->pGroupKeys, &pPartAgg->node.pTargets));
    PLAN_ERR_JRET(nodesCloneList(pPartAgg->node.pTargets, &pMergeAgg->pGroupKeys));
  }

  pMergeAgg->node.pConditions = pConditions;
  pMergeAgg->node.pTargets = pTargets;
  pPartAgg->node.pChildren = pChildren;

  PLAN_ERR_JRET(createPartAggRewriteFuns(pFunc, &pPartAgg->pAggFuncs, NULL, &pMergeAgg->pAggFuncs));

  PLAN_ERR_JRET(createColumnByRewriteExprs(pPartAgg->pAggFuncs, &pPartAgg->node.pTargets));

  *pOutput = (SLogicNode*)pPartAgg;

_return:
  if (code) {
    planError("%s failed at line %d, code: %d", __func__, lino, code);
    nodesDestroyNode((SNode*)pPartAgg);
  }
  nodesDestroyList(pFunc);
  return code;
}


static int32_t rebuildTableScanTargets(SScanLogicNode* pTableScan, SScanLogicNode* pTagScan) {
  int32_t code = TSDB_CODE_SUCCESS;

  nodesDestroyList(pTableScan->pScanPseudoCols);
  pTableScan->pScanPseudoCols = NULL;
  nodesDestroyList(pTableScan->node.pTargets);
  pTableScan->node.pTargets = NULL;

  PLAN_ERR_JRET(nodesCloneList(pTagScan->pScanPseudoCols, &pTableScan->pScanPseudoCols));
  PLAN_ERR_JRET(createColumnByRewriteExprs(pTableScan->pScanCols, &pTableScan->node.pTargets));
  PLAN_ERR_JRET(createColumnByRewriteExprs(pTableScan->pScanPseudoCols, &pTableScan->node.pTargets));

  return code;
_return:
  planError("%s failed , code: %d", __func__, code);
  return code;
}

static int32_t rebuildPartitionTargets(SScanLogicNode* pTagScan, SPartitionLogicNode* pPartition) {
  int32_t code = TSDB_CODE_SUCCESS;

  nodesDestroyList(pPartition->node.pTargets);
  nodesDestroyList(pPartition->pAggFuncs);
  pPartition->node.pTargets = NULL;
  pPartition->pAggFuncs = NULL;
  pPartition->node.dynamicOp = true;
  PLAN_ERR_JRET(createColumnByRewriteExprs(pTagScan->pScanPseudoCols, &pPartition->node.pTargets));

  return code;
_return:
  planError("%s failed , code: %d", __func__, code);
  return code;
}

/*
 * Rebuild dynamic virtual-stable scan targets from a rewritten parent node.
 *
 * @param pDynVstbScan Dynamic virtual-stable scan node.
 * @param pNode Rewritten parent logic node whose targets are copied.
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise error code.
 */
static int32_t rebuildVstbScanTargets(SDynQueryCtrlLogicNode* pDynVstbScan, SLogicNode* pNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pScanCol = NULL;

  PLAN_ERR_JRET(nodesCloneList(pDynVstbScan->node.pTargets, &pDynVstbScan->vtbScan.pScanCols));
  FOREACH(pScanCol, pDynVstbScan->vtbScan.pScanCols) {
    if (QUERY_NODE_COLUMN == nodeType(pScanCol) &&
        0 == strcasecmp(((SColumnNode*)pScanCol)->colName, "tbname")) {
      // createColumnByRewriteExprs() materializes a function target as a
      // generic result column. Preserve the pseudo-column identity so the
      // physical slot key matches the TBNAME slot registered by TagScan and
      // FederatedScan.
      ((SColumnNode*)pScanCol)->colType = COLUMN_TYPE_TBNAME;
    }
  }

  if (QUERY_NODE_LOGIC_PLAN_WINDOW == nodeType(pNode)) {
    SWindowLogicNode* pWindow = (SWindowLogicNode*)pNode;
    if (NULL != pWindow->pTspk) {
      bool hasTspk = false;
      FOREACH(pScanCol, pDynVstbScan->vtbScan.pScanCols) {
        if (nodesEqualNode(pScanCol, pWindow->pTspk)) {
          hasTspk = true;
          break;
        }
      }

      if (!hasTspk) {
        SNode* pTspk = NULL;
        PLAN_ERR_JRET(nodesCloneNode(pWindow->pTspk, &pTspk));
        PLAN_ERR_JRET(nodesListMakeStrictAppend(&pDynVstbScan->vtbScan.pScanCols, pTspk));
      }
    }
  }

  NODES_DESTORY_LIST(pDynVstbScan->node.pTargets);
  PLAN_ERR_JRET(nodesCloneList(pNode->pTargets, &pDynVstbScan->node.pTargets));

  return code;
_return:
  planError("%s failed , code: %d", __func__, code);
  return code;
}

static SScanLogicNode* vstableFindTagScan(SVirtualScanLogicNode* pVScan) {
  SNode* pNode = NULL;
  FOREACH(pNode, pVScan->node.pChildren) {
    if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode) &&
        SCAN_TYPE_TAG == ((SScanLogicNode*)pNode)->scanType) {
      return (SScanLogicNode*)pNode;
    }
  }
  return NULL;
}

static int32_t vstableRegisterExternalTagSlots(SScanLogicNode* pScan) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pClone = NULL;

  if (NULL == pScan || SCAN_TYPE_EXTERNAL != pScan->scanType) {
    return TSDB_CODE_SUCCESS;
  }
  SNode* pPseudo = NULL;
  FOREACH(pPseudo, pScan->pScanPseudoCols) {
    PLAN_ERR_JRET(nodesCloneNode(pPseudo, &pClone));
    PLAN_ERR_JRET(nodesListMakeStrictAppend(&pScan->pScanCols, pClone));
    pClone = NULL;
  }
  return code;

_return:
  nodesDestroyNode(pClone);
  planError("%s failed since %s", __func__, tstrerror(code));
  return code;
}

static int32_t vstableAggOptimizeFederated(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan,
                                           SAggLogicNode* pAgg) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  bool                    hasPartition = false;
  bool                    aggHasTag = false;
  bool                    batchProcessChild = !aggHasTag;
  SAggLogicNode*          pFinalAgg = NULL;
  SAggLogicNode*          pCanonicalPartial = NULL;
  SAggLogicNode*          pPendingPartial = NULL;
  SDynQueryCtrlLogicNode* pDyn = NULL;
  SVirtualScanLogicNode*  pVScan = NULL;
  SPartitionLogicNode*    pPartition = NULL;
  SScanLogicNode*         pSysScan = NULL;
  SScanLogicNode*         pTagScan = NULL;
  SScanLogicNode*         pTagScanClone = NULL;
  SScanLogicNode*         pPendingScan = NULL;
  SMergeLogicNode*        pMerge = NULL;
  SArray*                 pExecutionScans = NULL;

  PLAN_ERR_JRET(nodesCloneNode((SNode*)pAgg, (SNode**)&pFinalAgg));
  SLogicNode* pRootChild = (SLogicNode*)nodesListGetNode(pFinalAgg->node.pChildren, 0);
  if (NULL == pRootChild) {
    PLAN_ERR_JRET(TSDB_CODE_PLAN_INTERNAL_ERROR);
  }
  if (QUERY_NODE_LOGIC_PLAN_PARTITION == nodeType(pRootChild)) {
    hasPartition = true;
    pPartition = (SPartitionLogicNode*)pRootChild;
    pDyn = (SDynQueryCtrlLogicNode*)nodesListGetNode(pPartition->node.pChildren, 0);
  } else {
    pDyn = (SDynQueryCtrlLogicNode*)pRootChild;
  }
  if (NULL == pDyn || QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL != nodeType(pDyn)) {
    PLAN_ERR_JRET(TSDB_CODE_PLAN_INTERNAL_ERROR);
  }

  pVScan = (SVirtualScanLogicNode*)nodesListGetNode(pDyn->node.pChildren, 0);
  pSysScan = (SScanLogicNode*)nodesListGetNode(pDyn->node.pChildren, 1);
  if (NULL == pVScan || QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN != nodeType(pVScan) || NULL == pSysScan) {
    PLAN_ERR_JRET(TSDB_CODE_PLAN_INTERNAL_ERROR);
  }
  aggHasTag = vstableAggNodeHasTag(pFinalAgg, pVScan);
  batchProcessChild = !aggHasTag;
  pTagScan = vstableFindTagScan(pVScan);
  if (aggHasTag || hasPartition) {
    if (NULL == pTagScan) {
      PLAN_ERR_JRET(TSDB_CODE_PLAN_INTERNAL_ERROR);
    }
    PLAN_ERR_JRET(nodesCloneNode((SNode*)pTagScan, (SNode**)&pTagScanClone));
    PLAN_ERR_JRET(addFuncToScanNode("_tbuid", pTagScanClone));
    pTagScanClone->node.dynamicOp = false;
  }

  if (!vstableAggFederatedCanOptimize(pCxt, pFinalAgg, pDyn, pVScan)) {
    goto _return;
  }

  PLAN_ERR_JRET(vstableBuildExecutionScans(pVScan, &pExecutionScans));
  if (taosArrayGetSize(pExecutionScans) <= 0) {
    PLAN_ERR_JRET(TSDB_CODE_PLAN_INTERNAL_ERROR);
  }

  PLAN_ERR_JRET(createPartAggNode(pFinalAgg, (SLogicNode**)&pCanonicalPartial));
  clearChildList((SLogicNode*)pCanonicalPartial);

  PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_MERGE, (SNode**)&pMerge));
  pMerge->colsMerge = false;
  pMerge->needSort = false;
  pMerge->numOfSubplans = 0;
  pMerge->numOfChannels = taosArrayGetSize(pExecutionScans);
  pMerge->srcGroupId = -1;
  pMerge->node.dynamicOp = true;
  pMerge->node.precision = pAgg->node.precision;

  for (int32_t unitIdx = 0; unitIdx < taosArrayGetSize(pExecutionScans); ++unitIdx) {
    pPendingScan = taosArrayGetP(pExecutionScans, unitIdx);
    SScanLogicNode* pUnitOrigin = pPendingScan;
    OPTIMIZE_FLAG_CLEAR_MASK(pPendingScan->node.optimizedFlag, OPTIMIZE_FLAG_SCAN_PATH);
    pPendingScan->virtualStableScan = true;

    if (aggHasTag) {
      PLAN_ERR_JRET(rebuildTableScanTargets(pPendingScan, pTagScanClone));
      PLAN_ERR_JRET(vstableRegisterExternalTagSlots(pPendingScan));
    }

    PLAN_ERR_JRET(nodesCloneNode((SNode*)pCanonicalPartial, (SNode**)&pPendingPartial));
    clearChildList((SLogicNode*)pPendingPartial);
    pPendingPartial->node.dynamicOp = true;
    pPendingPartial->node.groupAction = GROUP_ACTION_KEEP;
    pPendingPartial->hasGroupKeyOptimized = hasPartition && !aggHasTag;

    SNode* pPartialFuncNode = NULL;
    FOREACH(pPartialFuncNode, pPendingPartial->pAggFuncs) {
      SFunctionNode* pPartialFunc = (SFunctionNode*)pPartialFuncNode;
      if (FUNCTION_TYPE_LAST_PARTIAL == pPartialFunc->funcType &&
          0 == strcasecmp(pPartialFunc->functionName, "_last_row_partial")) {
        pPartialFunc->node.relatedTo = FUNC_RELATED_TO_VSTABLE_LAST_ROW_PARTIAL;
      }
      SScanLogicNode* pFuncOrigin = NULL;
      bool            crossOrigin = false;
      PLAN_ERR_JRET(vstableExprOriginScan(pPartialFuncNode, pVScan, &pFuncOrigin, &crossOrigin));
      if (crossOrigin) {
        PLAN_ERR_JRET(TSDB_CODE_PLAN_INTERNAL_ERROR);
      }
      PLAN_ERR_JRET(vstableRewritePartialFuncForScan(
          pPartialFunc, pPendingScan,
          NULL == pFuncOrigin || vstableSameExecutionUnit(pUnitOrigin, pFuncOrigin)));
    }

    NODES_DESTORY_LIST(pPendingPartial->node.pTargets);
    PLAN_ERR_JRET(createColumnByRewriteExprs(pPendingPartial->pAggFuncs,
                                             &pPendingPartial->node.pTargets));
    PLAN_ERR_JRET(appendNewChild((SLogicNode*)pPendingPartial, (SLogicNode*)pPendingScan));
    pPendingScan = NULL;
    SScanLogicNode* pNullScan = NULL;
    taosArraySet(pExecutionScans, unitIdx, &pNullScan);

    if (0 == unitIdx) {
      PLAN_ERR_JRET(nodesCloneList(pPendingPartial->node.pTargets, &pMerge->pInputs));
      PLAN_ERR_JRET(nodesCloneList(pPendingPartial->node.pTargets, &pMerge->node.pTargets));
    }
    PLAN_ERR_JRET(appendNewChild((SLogicNode*)pMerge, (SLogicNode*)pPendingPartial));
    pPendingPartial = NULL;
  }

  pFinalAgg->hasGroupKeyOptimized = hasPartition;
  pFinalAgg->node.dynamicOp = batchProcessChild;

  clearChildList((SLogicNode*)pDyn);
  if (hasPartition) {
    clearChildList((SLogicNode*)pPartition);
    PLAN_ERR_JRET(rebuildPartitionTargets(pTagScanClone, pPartition));
  }
  nodesDestroyNode((SNode*)pVScan);
  pVScan = NULL;

  pDyn->qType = DYN_QTYPE_VTB_AGG;
  pDyn->vtbScan.batchProcessChild = batchProcessChild;
  pDyn->vtbScan.hasPartition = hasPartition;
  PLAN_ERR_JRET(rebuildVstbScanTargets(
      pDyn, batchProcessChild ? (SLogicNode*)pFinalAgg : (SLogicNode*)pMerge));

  PLAN_ERR_JRET(appendNewChild((SLogicNode*)pDyn, (SLogicNode*)pSysScan));
  pSysScan = NULL;
  if (hasPartition) {
    PLAN_ERR_JRET(appendNewChild((SLogicNode*)pPartition, (SLogicNode*)pTagScanClone));
    pTagScanClone = NULL;
    PLAN_ERR_JRET(appendNewChild((SLogicNode*)pDyn, (SLogicNode*)pPartition));
    pPartition = NULL;
  } else if (aggHasTag) {
    PLAN_ERR_JRET(appendNewChild((SLogicNode*)pDyn, (SLogicNode*)pTagScanClone));
    pTagScanClone = NULL;
  }

  if (batchProcessChild) {
    PLAN_ERR_JRET(appendNewChild((SLogicNode*)pFinalAgg, (SLogicNode*)pMerge));
    pMerge = NULL;
    PLAN_ERR_JRET(appendNewChild((SLogicNode*)pDyn, (SLogicNode*)pFinalAgg));
    pFinalAgg = NULL;
    PLAN_ERR_JRET(replaceLogicNode(pLogicSubplan, (SLogicNode*)pAgg, (SLogicNode*)pDyn));
    pDyn = NULL;
  } else {
    PLAN_ERR_JRET(appendNewChild((SLogicNode*)pDyn, (SLogicNode*)pMerge));
    pMerge = NULL;
    PLAN_ERR_JRET(appendNewChild((SLogicNode*)pFinalAgg, (SLogicNode*)pDyn));
    pDyn = NULL;
    PLAN_ERR_JRET(replaceLogicNode(pLogicSubplan, (SLogicNode*)pAgg, (SLogicNode*)pFinalAgg));
    pFinalAgg = NULL;
  }

  nodesDestroyNode((SNode*)pCanonicalPartial);
  pCanonicalPartial = NULL;
  nodesDestroyNode((SNode*)pAgg);
  pCxt->optimized = true;
  taosArrayDestroyP(pExecutionScans, (FDelete)nodesDestroyNode);
  return code;

_return:
  nodesDestroyNode((SNode*)pPendingPartial);
  taosArrayDestroyP(pExecutionScans, (FDelete)nodesDestroyNode);
  nodesDestroyNode((SNode*)pCanonicalPartial);
  nodesDestroyNode((SNode*)pMerge);
  nodesDestroyNode((SNode*)pTagScanClone);
  nodesDestroyNode((SNode*)pFinalAgg);
  if (TSDB_CODE_SUCCESS != code) {
    planError("%s failed since %s", __func__, tstrerror(code));
  }
  return code;
}

static int32_t vstableAggOptimizeWithoutPartition(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan,
                                                         SAggLogicNode* pAgg) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;
  SDynQueryCtrlLogicNode* pDynVstbScan = NULL;
  SVirtualScanLogicNode*  pVirtualScanNode = NULL;
  SScanLogicNode*         pTagScan = NULL;
  SScanLogicNode*         pTableScan = NULL;
  SScanLogicNode*         pSysScan = NULL;
  SAggLogicNode*          pVirtualChildAgg = NULL;
  SAggLogicNode*          pNewAgg = NULL;
  bool                    batchProcessChild = !aggNodeHasTag(pAgg);

  PLAN_ERR_JRET(nodesCloneNode((SNode*)pAgg, (SNode**)&pNewAgg));

  pDynVstbScan = (SDynQueryCtrlLogicNode*)nodesListGetNode(pNewAgg->node.pChildren, 0);
  QUERY_CHECK_NULL(pDynVstbScan, code, lino, _return, terrno)

  pVirtualScanNode = (SVirtualScanLogicNode*)nodesListGetNode(pDynVstbScan->node.pChildren, 0);
  pSysScan = (SScanLogicNode*)nodesListGetNode(pDynVstbScan->node.pChildren, 1);
  QUERY_CHECK_NULL(pSysScan, code, lino, _return, terrno)
  QUERY_CHECK_NULL(pVirtualScanNode, code, lino, _return, terrno)

  if (batchProcessChild) {
    pTableScan = (SScanLogicNode*)nodesListGetNode(pVirtualScanNode->node.pChildren, LIST_LENGTH(pVirtualScanNode->node.pChildren) - 1);
    QUERY_CHECK_NULL(pTableScan, code, lino, _return, terrno)
    if (LIST_LENGTH(pVirtualScanNode->node.pChildren) == 2) {
      pTagScan = (SScanLogicNode*)nodesListGetNode(pVirtualScanNode->node.pChildren, 0);
      QUERY_CHECK_NULL(pTagScan, code, lino, _return, terrno)
      nodesDestroyNode((SNode*)pTagScan);
      pTagScan = NULL;
    }
  } else {
    pTagScan = (SScanLogicNode*)nodesListGetNode(pVirtualScanNode->node.pChildren, 0);
    pTableScan = (SScanLogicNode*)nodesListGetNode(pVirtualScanNode->node.pChildren, 1);
    QUERY_CHECK_NULL(pTagScan, code, lino, _return, terrno)
    QUERY_CHECK_NULL(pTableScan, code, lino, _return, terrno)

    PLAN_ERR_JRET(rebuildTableScanTargets(pTableScan, pTagScan));

    PLAN_ERR_JRET(addFuncToScanNode("_tbuid", pTagScan));
    pTagScan->node.dynamicOp = false;
  }


  pDynVstbScan->qType = DYN_QTYPE_VTB_AGG;
  pDynVstbScan->vtbScan.batchProcessChild = batchProcessChild;
  pDynVstbScan->vtbScan.hasPartition = false;

  if (batchProcessChild) {
    PLAN_ERR_JRET(nodesCloneNode((SNode*)pNewAgg, (SNode**)&pVirtualChildAgg));
    nodesDestroyList(pVirtualChildAgg->node.pChildren);
    pVirtualChildAgg->node.pChildren = NULL;
  } else {
    PLAN_ERR_JRET(createPartAggNode(pNewAgg, (SLogicNode**)&pVirtualChildAgg));
  }
  pVirtualChildAgg->node.dynamicOp = true;

  PLAN_ERR_JRET(rebuildVstbScanTargets(pDynVstbScan, (SLogicNode*)pVirtualChildAgg));

  clearChildList((SLogicNode*)pNewAgg);
  clearChildList((SLogicNode*)pVirtualScanNode);
  clearChildList((SLogicNode*)pVirtualChildAgg);
  clearChildList((SLogicNode*)pDynVstbScan);

  if (!batchProcessChild) {
    PLAN_ERR_JRET(appendNewChild((SLogicNode*)pNewAgg, (SLogicNode*)pDynVstbScan));
  }
  PLAN_ERR_JRET(appendNewChild((SLogicNode*)pVirtualChildAgg, (SLogicNode*)pTableScan));

  PLAN_ERR_JRET(appendNewChild((SLogicNode*)pDynVstbScan, (SLogicNode*)pSysScan));
  if (!batchProcessChild) {
    PLAN_ERR_JRET(appendNewChild((SLogicNode*)pDynVstbScan, (SLogicNode*)pTagScan));
  }
  PLAN_ERR_JRET(appendNewChild((SLogicNode*)pDynVstbScan, (SLogicNode*)pVirtualChildAgg));

  // cross-precision: have the source Table Scan emit ts already converted to
  // vtable precision, so the partial Aggregate below runs in vtable precision
  // and the coordinator's cross-source first/last/min/max merge is correct.
  pTableScan->outputTsPrecision = pVirtualScanNode->node.precision;

  OPTIMIZE_FLAG_CLEAR_MASK(pTableScan->node.optimizedFlag, OPTIMIZE_FLAG_SCAN_PATH);

  if (batchProcessChild) {
    PLAN_ERR_JRET(replaceLogicNode(pLogicSubplan, (SLogicNode*)pAgg, (SLogicNode*)pDynVstbScan));
    nodesDestroyNode((SNode*)pNewAgg);
  } else {
    PLAN_ERR_JRET(replaceLogicNode(pLogicSubplan, (SLogicNode*)pAgg, (SLogicNode*)pNewAgg));
  }

  nodesDestroyNode((SNode*)pVirtualScanNode);
  nodesDestroyNode((SNode*)pAgg);

  return code;
_return:
  if (code) {
    planError("%s failed, code: %d", __func__ , code);
  }
  nodesDestroyNode((SNode*)pNewAgg);
  return code;
}

static int32_t vstableAggOptimizeWithPartition(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan,
                                               SAggLogicNode* pAgg) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;
  SPartitionLogicNode*    pPartition = NULL;
  SDynQueryCtrlLogicNode* pDynVstbScan = NULL;
  SVirtualScanLogicNode*  pVirtualScanNode = NULL;
  SScanLogicNode*         pTableScan = NULL;
  SScanLogicNode*         pSysScan = NULL;
  SScanLogicNode*         pTagScan = NULL;
  SAggLogicNode*          pVirtualAgg = NULL;
  bool                    batchProcessChild = !aggNodeHasTag(pAgg);
  SAggLogicNode*          pNewAgg = NULL;

  PLAN_ERR_JRET(nodesCloneNode((SNode*)pAgg, (SNode**)&pNewAgg));

  pPartition = (SPartitionLogicNode*)nodesListGetNode(pNewAgg->node.pChildren, 0);
  QUERY_CHECK_NULL(pPartition, code, lino, _return, terrno)

  pDynVstbScan = (SDynQueryCtrlLogicNode*)nodesListGetNode(pPartition->node.pChildren, 0);
  QUERY_CHECK_NULL(pDynVstbScan, code, lino, _return, terrno)

  pVirtualScanNode = (SVirtualScanLogicNode*)nodesListGetNode(pDynVstbScan->node.pChildren, 0);
  pSysScan = (SScanLogicNode*)nodesListGetNode(pDynVstbScan->node.pChildren, 1);
  QUERY_CHECK_NULL(pSysScan, code, lino, _return, terrno)
  QUERY_CHECK_NULL(pVirtualScanNode, code, lino, _return, terrno)

  pTagScan = (SScanLogicNode*)nodesListGetNode(pVirtualScanNode->node.pChildren, 0);
  pTableScan = (SScanLogicNode*)nodesListGetNode(pVirtualScanNode->node.pChildren, 1);
  QUERY_CHECK_NULL(pTableScan, code, lino, _return, terrno)
  QUERY_CHECK_NULL(pTagScan, code, lino, _return, terrno)

  if (!batchProcessChild) {
    PLAN_ERR_JRET(rebuildTableScanTargets(pTableScan, pTagScan));
  }

  PLAN_ERR_JRET(addFuncToScanNode("_tbuid", pTagScan));
  pTagScan->node.dynamicOp = false;
  PLAN_ERR_JRET(rebuildPartitionTargets(pTagScan, pPartition));

  pDynVstbScan->qType = DYN_QTYPE_VTB_AGG;
  pDynVstbScan->vtbScan.batchProcessChild = batchProcessChild;
  pDynVstbScan->vtbScan.hasPartition = true;

  if (batchProcessChild) {
    PLAN_ERR_JRET(nodesCloneNode((SNode*)pNewAgg, (SNode**)&pVirtualAgg));
    pVirtualAgg->node.dynamicOp = true;
    pVirtualAgg->hasGroupKeyOptimized = true;
    nodesDestroyList(pVirtualAgg->node.pChildren);
    pVirtualAgg->node.pChildren = NULL;
  } else {
    PLAN_ERR_JRET(createPartAggNode(pNewAgg, (SLogicNode**)&pVirtualAgg));
    pVirtualAgg->node.dynamicOp = true;
    pNewAgg->hasGroupKeyOptimized = true;
  }

  PLAN_ERR_JRET(rebuildVstbScanTargets(pDynVstbScan, (SLogicNode*)pVirtualAgg));

  clearChildList((SLogicNode *)pVirtualAgg);
  clearChildList((SLogicNode *)pVirtualScanNode);
  clearChildList((SLogicNode *)pDynVstbScan);
  clearChildList((SLogicNode *)pPartition);
  clearChildList((SLogicNode *)pNewAgg);

  if (!batchProcessChild) {
    PLAN_ERR_JRET(appendNewChild((SLogicNode*)pNewAgg, (SLogicNode*)pDynVstbScan));
  }

  PLAN_ERR_JRET(appendNewChild((SLogicNode*)pVirtualAgg, (SLogicNode*)pTableScan));
  PLAN_ERR_JRET(appendNewChild((SLogicNode*)pPartition, (SLogicNode*)pTagScan));
  PLAN_ERR_JRET(appendNewChild((SLogicNode*)pDynVstbScan, (SLogicNode*)pSysScan));
  PLAN_ERR_JRET(appendNewChild((SLogicNode*)pDynVstbScan, (SLogicNode*)pPartition));
  PLAN_ERR_JRET(appendNewChild((SLogicNode*)pDynVstbScan, (SLogicNode*)pVirtualAgg));

  // cross-precision: have the source Table Scan emit ts already converted to
  // vtable precision, so the partial Aggregate below runs in vtable precision
  // and the coordinator's cross-source first/last/min/max merge is correct.
  pTableScan->outputTsPrecision = pVirtualScanNode->node.precision;

  OPTIMIZE_FLAG_CLEAR_MASK(pTableScan->node.optimizedFlag, OPTIMIZE_FLAG_SCAN_PATH);

  if (batchProcessChild) {
    PLAN_ERR_JRET(replaceLogicNode(pLogicSubplan, (SLogicNode*)pAgg, (SLogicNode*)pDynVstbScan));
    nodesDestroyNode((SNode*)pNewAgg);
  } else {
    PLAN_ERR_JRET(replaceLogicNode(pLogicSubplan, (SLogicNode*)pAgg, (SLogicNode*)pNewAgg));
  }

  nodesDestroyNode((SNode*)pVirtualScanNode);
  nodesDestroyNode((SNode*)pAgg);
  return code;

_return:
  if (code) {
    planError("%s failed, code: %d", __func__ , code);
  }
  nodesDestroyNode((SNode*)pNewAgg);
  return code;
}

static int32_t vstableAggOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SAggLogicNode* pAgg = (SAggLogicNode*)optFindPossibleNode(pLogicSubplan->pNode, vstableAggShouldBeOptimized, pCxt);
  if (NULL == pAgg) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t              code = TSDB_CODE_SUCCESS;
  SPartitionLogicNode* pPartNode = NULL;

  OPTIMIZE_FLAG_SET_MASK(pAgg->node.optimizedFlag, OPTIMIZE_FLAG_VTB_AGG);

  if (pAgg->pGroupKeys) {
    // make a partition node to do the group calculation
    pAgg->isGroupTb = false;
    PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_PARTITION, (SNode**)&pPartNode));
    TSWAP(pPartNode->pPartitionKeys, pAgg->pGroupKeys);
    pPartNode->node.groupAction = GROUP_ACTION_SET;
    pPartNode->node.requireDataOrder = DATA_ORDER_LEVEL_NONE;
    pPartNode->node.resultDataOrder = DATA_ORDER_LEVEL_NONE;

    SLogicNode* pChild = (SLogicNode*)nodesListGetNode(pAgg->node.pChildren, 0);
    if (NULL == pChild) {
      PLAN_ERR_JRET(TSDB_CODE_PLAN_INTERNAL_ERROR);
    }

    pChild->pParent = (SLogicNode*)pPartNode;
    TSWAP(pPartNode->node.pChildren, pAgg->node.pChildren);
    clearChildList((SLogicNode*)pAgg);
    PLAN_ERR_JRET(appendNewChild((SLogicNode*)pAgg, (SLogicNode*)pPartNode));
    pPartNode = NULL;
  }

  SLogicNode* pAggChild = (SLogicNode*)nodesListGetNode(pAgg->node.pChildren, 0);
  SDynQueryCtrlLogicNode* pDyn = NULL;
  if (QUERY_NODE_LOGIC_PLAN_PARTITION == nodeType(pAggChild)) {
    pDyn = (SDynQueryCtrlLogicNode*)nodesListGetNode(pAggChild->pChildren, 0);
  } else if (QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL == nodeType(pAggChild)) {
    pDyn = (SDynQueryCtrlLogicNode*)pAggChild;
  }
  SVirtualScanLogicNode* pVScan =
      NULL != pDyn ? (SVirtualScanLogicNode*)nodesListGetNode(pDyn->node.pChildren, 0) : NULL;
  if (NULL != pVScan && QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN == nodeType(pVScan) &&
      vstableHasExternalExecutionUnit(pVScan)) {
    PLAN_ERR_JRET(vstableAggOptimizeFederated(pCxt, pLogicSubplan, pAgg));
    goto _return;
  }

  if (nodeType(nodesListGetNode(pAgg->node.pChildren, 0)) == QUERY_NODE_LOGIC_PLAN_PARTITION) {
    PLAN_ERR_JRET(vstableAggOptimizeWithPartition(pCxt, pLogicSubplan, pAgg));
  } else if (nodeType(nodesListGetNode(pAgg->node.pChildren, 0)) == QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL) {
    PLAN_ERR_JRET(vstableAggOptimizeWithoutPartition(pCxt, pLogicSubplan, pAgg));
  } else {
    PLAN_ERR_JRET(TSDB_CODE_INVALID_PARA);
  }

  pCxt->optimized = true;
_return:
  if (TSDB_CODE_SUCCESS != code) {
    planError("%s failed since %s", __func__, tstrerror(code));
    nodesDestroyNode((SNode*)pPartNode);
  }
  return code;
}

// DISTINCT aggregates no longer use a plan node. COUNT/SUM/AVG dedup internally via
// SSpillDistinctSet (see builtinsimpl.c), which handles multi-column DISTINCT, mixing DISTINCT
// with plain aggregates, and spilling past a memory budget -- none of which the row-deleting
// DistinctFilter operator could express. The operator and its serialization are retained so a
// plan built by an older node still executes during a rolling upgrade; nothing generates one now.

// clang-format off
static const SOptimizeRule optimizeRuleSet[] = {
  {.pName = "RewriteTail",                .optimizeFunc = rewriteTailOptimize},
  {.pName = "RewriteUnique",              .optimizeFunc = rewriteUniqueOptimize},
  {.pName = "ScanPath",                   .optimizeFunc = scanPathOptimize},
  {.pName = "PushDownCondition",          .optimizeFunc = pdcOptimize},
  {.pName = "EliminateNotNullCond",       .optimizeFunc = eliminateNotNullCondOptimize},
  // FqPushdown must run BEFORE Sort/Limit/Project optimizations.  Once it
  // extracts the Sort+Project chain into pRemoteLogicPlan, later rules
  // (PushDownLimit, EliminateProject, etc.) see no Sort/Project above the
  // ExternalScan and naturally become no-ops for that subtree.
  // Rules above (RewriteTail, RewriteUnique, ScanPath, PushDownCondition,
  // EliminateNotNullCond) have been audited:
  //   - RewriteTail/RewriteUnique: only match IndefRowsFunc nodes (N/A)
  //   - ScanPath: explicit SCAN_TYPE_EXTERNAL guard
  //   - PushDownCondition: pdcDealScan has SCAN_TYPE_EXTERNAL guard
  //   - EliminateNotNullCond: explicit SCAN_TYPE_EXTERNAL guard
  {.pName = "FqPushdown",                 .optimizeFunc = fqPushdownOptimize},
  {.pName = "FqInterp",                   .optimizeFunc = fqInterpOptimize},
  {.pName = "FqUnionAll",                 .optimizeFunc = fqUnionAllOptimize},
  {.pName = "FqUnionDedup",               .optimizeFunc = fqUnionDeduplicateOptimize},
  {.pName = "JoinCondOptimize",           .optimizeFunc = joinCondOptimize},
  {.pName = "AsofJoinCondOptimize",       .optimizeFunc = asofJoinCondOptimize},
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
  {.pName = "LastSmaNotNull",             .optimizeFunc = lastSmaNotNullPrefilterOptimize},
  {.pName = "splitCacheLastFunc",         .optimizeFunc = splitCacheLastFuncOptimize},
  {.pName = "LastRowScan",                .optimizeFunc = lastRowScanOptimize},
  {.pName = "TagScan",                    .optimizeFunc = tagScanOptimize},
  {.pName = "TableCountScan",             .optimizeFunc = tableCountScanOptimize},
  {.pName = "EliminateProject",           .optimizeFunc = eliminateProjOptimize},
  {.pName = "EliminateSetOperator",       .optimizeFunc = eliminateSetOpOptimize},
  {.pName = "PartitionCols",              .optimizeFunc = partitionColsOpt},
  {.pName = "Tsma",                       .optimizeFunc = tsmaOptimize},
  {.pName = "EliminateVirtualScan",       .optimizeFunc = eliminateVirtualScanOptimize},
  {.pName = "PushDownAgg",                .optimizeFunc = pdaOptimize},
  {.pName = "VtableWindow",               .optimizeFunc = vtableWindowOptimize},
  {.pName = "VStableWindow",              .optimizeFunc = vstableWindowOptimize},
  {.pName = "VStableWindowSort",          .optimizeFunc = vstableWindowSortOptimize},
  {.pName = "VtableTagScan",              .optimizeFunc = vtableTagScanOptimize},
  {.pName = "VStableAgg",                 .optimizeFunc = vstableAggOptimize},
};
// clang-format on

static const int32_t optimizeRuleNum = (sizeof(optimizeRuleSet) / sizeof(SOptimizeRule));

static bool subplanHasExternalScan(SLogicSubplan* pSubplan) {
  return pSubplan->pNode != NULL && nodeHasExternalScan(pSubplan->pNode);
}

// ─── Federated Query Pushdown Optimizer ────────────────────────────────────
// Walks each external scan bottom-up, finds the highest contiguous pushable
// logic subtree, and clones that subtree into SScanLogicNode.pRemoteLogicPlan.
// The main tree keeps only the docking external scan plus whatever non-pushable
// layers remain above it.
//
// This avoids duplicate local/remote plan generation and preserves a single
// source of truth for the final pushdown shape before physical planning.
// ─────────────────────────────────────────────────────────────────────────────

static SScanLogicNode* fqFindExternalScan(SLogicNode* pNode) {
  if (NULL == pNode) {
    return NULL;
  }
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode) &&
      ((SScanLogicNode*)pNode)->scanType == SCAN_TYPE_EXTERNAL &&
      !((SScanLogicNode*)pNode)->virtualStableScan &&
      !OPTIMIZE_FLAG_TEST_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_FQ_PUSHDOWN)) {
    return (SScanLogicNode*)pNode;
  }
  SNode* pChild;
  FOREACH(pChild, pNode->pChildren) {
    SScanLogicNode* pFound = fqFindExternalScan((SLogicNode*)pChild);
    if (pFound) return pFound;
  }
  return NULL;
}

// FqInterp runs AFTER FqPushdown.  At that point the docking external scan has
// already been marked OPTIMIZE_FLAG_FQ_PUSHDOWN, so the generic
// fqFindExternalScan() intentionally skips it.  INTERP still needs to revisit
// that docking scan to replace the bare remote scan with its branch-aware
// remote plan.  Therefore this finder ignores the FQ_PUSHDOWN flag and returns
// only an external scan that still has a local INTERP ancestor above it.
static SScanLogicNode* fqFindInterpExternalScan(SLogicNode* pNode) {
  if (NULL == pNode) {
    return NULL;
  }

  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    SScanLogicNode* pScan = (SScanLogicNode*)pNode;
    if (pScan->scanType == SCAN_TYPE_EXTERNAL) {
      SLogicNode* pCur = pScan->node.pParent;
      SInterpFuncLogicNode* pInterp = NULL;
      while (NULL != pCur) {
        if (QUERY_NODE_LOGIC_PLAN_INTERP_FUNC == nodeType(pCur)) {
          pInterp = (SInterpFuncLogicNode*)pCur;
          break;
        }
        pCur = pCur->pParent;
      }
      planDebug("FqInterp: probe scan=%p optimizedFlag=%d hasInterpAncestor=%d remotePlan=%p",
                pScan, (int)pScan->node.optimizedFlag, (int)(NULL != pInterp), pScan->pRemoteLogicPlan);
      if (NULL != pInterp) {
        return pScan;
      }
    }
  }

  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    SScanLogicNode* pFound = fqFindInterpExternalScan((SLogicNode*)pChild);
    if (NULL != pFound) {
      return pFound;
    }
  }

  return NULL;
}

// Phase 1: keep Project pushdown conservative.  Only pure column projections
// are pushed down; expression projections stay local to avoid remote type
// mapping instability in mixed-source parity paths.
static bool fqExprIsPushable(SNode* pExpr, SScanLogicNode* pScan, const SExtSourceCapability* pCap,
                             const SExtCapSessionOverlay* pOverlay, const SPlanContext* pPlanCxt);
static int32_t fqAlignRemoteRootTargetsForRender(SLogicNode* pRemoteRoot);

static bool fqNodeIsSingleChildSetOpPassthrough(const SLogicNode* pNode) {
  if (NULL == pNode || NULL == pNode->pChildren || LIST_LENGTH(pNode->pChildren) != 1) {
    return false;
  }

  ENodeType type = nodeType((const SNode*)pNode);
  return QUERY_NODE_LOGIC_PLAN_PROJECT == type || QUERY_NODE_LOGIC_PLAN_SORT == type;
}

static bool fqProjectIsCrossSourceSetOpBranch(const SProjectLogicNode* pProj, const SScanLogicNode* pScan) {
  if (NULL == pProj || NULL == pScan || NULL == pScan->pExtTableNode) {
    return false;
  }

  const SProjectLogicNode* pSetOpProj = NULL;
  for (const SLogicNode* pUp = pProj->node.pParent; NULL != pUp; pUp = pUp->pParent) {
    if (QUERY_NODE_LOGIC_PLAN_PROJECT == nodeType((const SNode*)pUp)) {
      const SProjectLogicNode* pAncestorProj = (const SProjectLogicNode*)pUp;
      if (pAncestorProj->isSetOpProj &&
          NULL != pAncestorProj->node.pChildren &&
          LIST_LENGTH(pAncestorProj->node.pChildren) >= 2) {
        pSetOpProj = pAncestorProj;
        break;
      }
    }

    if (!fqNodeIsSingleChildSetOpPassthrough(pUp)) {
      break;
    }
  }

  if (NULL == pSetOpProj) {
    return false;
  }

  const SExtTableNode* pRefExt = (const SExtTableNode*)pScan->pExtTableNode;
  SNode*               pBranch = NULL;
  FOREACH(pBranch, pSetOpProj->node.pChildren) {
    SScanLogicNode* pBranchScan = fqFindExternalLogicLeafScan((SLogicNode*)pBranch);
    if (NULL == pBranchScan || NULL == pBranchScan->pExtTableNode) {
      continue;
    }

    const SExtTableNode* pBranchExt = (const SExtTableNode*)pBranchScan->pExtTableNode;
    if (pBranchExt->sourceType != pRefExt->sourceType ||
        0 != strncmp(pBranchExt->sourceName, pRefExt->sourceName, TSDB_EXT_SOURCE_NAME_LEN)) {
      return true;
    }
  }

  return false;
}

static bool fqProjectIsPushdownable(const SLogicNode* pNode, SScanLogicNode* pScan,
                                     const SExtSourceCapability* pCap,
                                     const SExtCapSessionOverlay* pOverlay,
                                     const SPlanContext* pPlanCxt) {
  if (nodeType(pNode) != QUERY_NODE_LOGIC_PLAN_PROJECT) return true;
  const SProjectLogicNode* pProj = (const SProjectLogicNode*)pNode;
  if (fqDiagEnabled()) {
    fprintf(stderr,
            "[FQ-DIAG] project-pushable-enter project=%p isSetOpProj=%d parentType=%d childCnt=%d scan=%p table=%s\n",
            pProj, (int)pProj->isSetOpProj, pNode->pParent ? (int)nodeType((SNode*)pNode->pParent) : -1,
            pNode->pChildren ? LIST_LENGTH(pNode->pChildren) : 0, pScan,
            pScan ? pScan->tableName.tname : "(null)");
    fqDiagDumpExprList("project.projections", pProj->pProjections);
    fqDiagDumpExprList("project.targets", pProj->node.pTargets);
  }
  if (OPTIMIZE_FLAG_TEST_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_FQ_UNIQUE_LOCAL)) {
    planDebug("FQ-push: Project NOT pushable - rewritten UNIQUE must keep local semantics project=%p",
              pProj);
    return false;
  }
  if (!pProj->isSetOpProj && fqProjectIsCrossSourceSetOpBranch(pProj, pScan)) {
    // UNION/UNION ALL branch-normalization projects carry set-op aligned types
    // via current-scope refs such as `time` with BIGINT/VARCHAR result types.
    // Local execution can honor that metadata, but remote SQL rendering would
    // collapse them back to bare source columns and lose CAST/TO_ISO8601
    // semantics. Keep these branch wrappers local for cross-source merges.
    planDebug("FQ-push: Project NOT pushable - cross-source set-op branch normalization must stay local project=%p",
              pProj);
    return false;
  }
  if (pProj->isSetOpProj && pNode->pChildren != NULL && LIST_LENGTH(pNode->pChildren) > 1) {
    const SExtTableNode* pRefExt = (const SExtTableNode*)pScan->pExtTableNode;
    if (NULL == pRefExt) {
      planDebug("FQ-push: SetOp Project NOT pushable - reference ext table missing");
      return false;
    }

    int32_t branchIdx = 0;
    SNode*  pBranch = NULL;
    FOREACH(pBranch, pNode->pChildren) {
      SScanLogicNode* pBranchScan = fqFindExternalLogicLeafScan((SLogicNode*)pBranch);
      if (NULL == pBranchScan || NULL == pBranchScan->pExtTableNode) {
        planDebug("FQ-push: SetOp Project NOT pushable - branch[%d] missing external scan", branchIdx);
        return false;
      }

      const SExtTableNode* pBranchExt = (const SExtTableNode*)pBranchScan->pExtTableNode;
      if (pBranchExt->sourceType != pRefExt->sourceType ||
          0 != strncmp(pBranchExt->sourceName, pRefExt->sourceName, TSDB_EXT_SOURCE_NAME_LEN)) {
        planDebug("FQ-push: SetOp Project NOT pushable - branch[%d] cross-source ref=(%d,%s) branch=(%d,%s)",
                  branchIdx, (int)pRefExt->sourceType, pRefExt->sourceName,
                  (int)pBranchExt->sourceType, pBranchExt->sourceName);
        return false;
      }

      ++branchIdx;
    }

    planDebug("FQ-push: SetOp Project same-source check passed, branches=%d refSource=(%d,%s)",
              branchIdx, (int)pRefExt->sourceType, pRefExt->sourceName);
  }

  int32_t idx = 0;
  SNode* pExpr = NULL;
  FOREACH(pExpr, pProj->pProjections) {
    SNode* pInner = pExpr;
    if (nodeType(pInner) == QUERY_NODE_TARGET) pInner = ((STargetNode*)pInner)->pExpr;
    if (pInner == NULL) { idx++; continue; }
    bool ok = fqExprIsPushable(pInner, pScan, pCap, pOverlay, pPlanCxt);
    if (fqDiagEnabled()) {
      fprintf(stderr,
              "[FQ-DIAG] project-expr-result project=%p idx=%d exprType=%d pushable=%d scan=%p table=%s\n",
              pProj, idx, (int)nodeType(pInner), (int)ok, pScan,
              pScan ? pScan->tableName.tname : "(null)");
    }
    planDebug("FQ-push: Project expr[%d] nodeType=%d pushable=%d", idx, (int)nodeType(pInner), (int)ok);
    if (!ok) {
      planDebug("FQ-push: Project NOT pushable - expr[%d] blocked (nodeType=%d)", idx, (int)nodeType(pInner));
      return false;
    }
    idx++;
  }
  if (pProj->pProjections == NULL) {
    FOREACH(pExpr, pProj->node.pTargets) {
      SNode* pInner = pExpr;
      if (nodeType(pInner) == QUERY_NODE_TARGET) pInner = ((STargetNode*)pInner)->pExpr;
      if (pInner == NULL) { idx++; continue; }
      bool ok = fqExprIsPushable(pInner, pScan, pCap, pOverlay, pPlanCxt);
      planDebug("FQ-push: Project target[%d] nodeType=%d pushable=%d", idx, (int)nodeType(pInner), (int)ok);
      if (!ok) {
        planDebug("FQ-push: Project NOT pushable - target[%d] blocked (nodeType=%d)", idx, (int)nodeType(pInner));
        return false;
      }
      idx++;
    }
  }
  planDebug("FQ-push: Project pushable OK (%d exprs)", idx);
  return true;
}

static bool fqProjectCanPartiallyPushdown(const SProjectLogicNode* pProj, SScanLogicNode* pScan,
                                          const SExtSourceCapability* pCap,
                                          const SExtCapSessionOverlay* pOverlay,
                                          const SPlanContext* pPlanCxt, int32_t* pPushableCnt,
                                          int32_t* pLocalCnt) {
  if (NULL != pPushableCnt) {
    *pPushableCnt = 0;
  }
  if (NULL != pLocalCnt) {
    *pLocalCnt = 0;
  }
  if (NULL == pProj || pProj->isSetOpProj || NULL == pProj->pProjections) {
    return false;
  }
  if (fqProjectIsCrossSourceSetOpBranch(pProj, pScan)) {
    planDebug("FQ-partialProject: skip - cross-source set-op branch normalization must stay local project=%p",
              pProj);
    return false;
  }

  int32_t pushableCnt = 0;
  int32_t localCnt = 0;
  int32_t idx = 0;
  SNode* pExpr = NULL;
  FOREACH(pExpr, pProj->pProjections) {
    SNode* pInner = pExpr;
    if (QUERY_NODE_TARGET == nodeType(pInner)) {
      pInner = ((STargetNode*)pInner)->pExpr;
    }
    if (NULL == pInner) {
      ++localCnt;
      planDebug("FQ-partialProject: expr[%d] is NULL, keep local", idx);
      ++idx;
      continue;
    }

    bool pushable = fqExprIsPushable(pInner, pScan, pCap, pOverlay, pPlanCxt);
    bool partialPushable = pushable && QUERY_NODE_COLUMN == nodeType(pInner);
    planDebug("FQ-partialProject: expr[%d] nodeType=%d pushable=%d partialPushable=%d",
              idx, (int)nodeType(pInner), (int)pushable, (int)partialPushable);
    if (pushable && !partialPushable) {
      planDebug("FQ-partialProject: expr[%d] renderable but not a plain column, keep local in mixed project",
                idx);
    }
    if (partialPushable) {
      ++pushableCnt;
    } else {
      ++localCnt;
    }
    ++idx;
  }

  if (NULL != pPushableCnt) {
    *pPushableCnt = pushableCnt;
  }
  if (NULL != pLocalCnt) {
    *pLocalCnt = localCnt;
  }

  bool partial = (pushableCnt > 0 && localCnt > 0);
  planDebug("FQ-partialProject: project=%p partial=%d pushableCnt=%d localCnt=%d total=%d",
            pProj, (int)partial, pushableCnt, localCnt, idx);
  return partial;
}

// Forward declaration for fqAggIsPushdownable (uses fqExprIsPushable)
static bool fqAggIsPushdownable(const SLogicNode* pNode, SScanLogicNode* pScan,
                                 const SExtSourceCapability* pCap,
                                 const SExtCapSessionOverlay* pOverlay,
                                 const SPlanContext* pPlanCxt);
static bool fqLayerIsPushable(SLogicNode* pNode, SScanLogicNode* pScan, const SExtSourceCapability* pCap,
                              const SExtCapSessionOverlay* pOverlay, const SPlanContext* pPlanCxt);

// Check if all sort keys in a Sort node are pushable expressions.
// Allows simple column references and renderable function expressions.
// When aboveAgg=true, colId==0 column references are permitted (they are
// AGG output aliases such as "cnt" from ORDER BY cnt).
static bool fqSortAllKeysPushdownable(const SLogicNode* pNode, SScanLogicNode* pScan,
                                      const SExtSourceCapability* pCap,
                                      const SExtCapSessionOverlay* pOverlay,
                                      bool aboveAgg, const SPlanContext* pPlanCxt) {
  if (nodeType(pNode) != QUERY_NODE_LOGIC_PLAN_SORT) return true;
  EExtClauseKind orderClause = EXT_CLAUSE_ORDER;
  if (!fqClauseIsPushable(orderClause, pCap ? pCap->dialect : -1, pOverlay) ||
      capDisabled(pCap, pOverlay, EXT_DEMOTE_KIND_CLAUSE, &orderClause)) {
    planDebug("FQ-push: Sort NOT pushable - ORDER clause disabled by capability");
    return false;
  }

  const SSortLogicNode* pSort = (const SSortLogicNode*)pNode;
  SNode*                pKey  = NULL;
  int32_t               kidx  = 0;
  FOREACH(pKey, pSort->pSortKeys) {
    const SNode* pInner = pKey;
    if (nodeType(pInner) == QUERY_NODE_ORDER_BY_EXPR) {
      pInner = ((const SOrderByExprNode*)pInner)->pExpr;
    }
    if (pInner == NULL) {
      planDebug("FQ-push: Sort NOT pushable - key[%d] expr is NULL", kidx);
      return false;
    }
    // colId==0 indicates a synthetic/precalculated column — not pushable
    // UNLESS Sort is above a pushed AGG, in which case colId==0 cols are
    // valid AGG output aliases (e.g. ORDER BY cnt where cnt = COUNT(*)).
    if (nodeType(pInner) == QUERY_NODE_COLUMN) {
      const SColumnNode* pCol = (const SColumnNode*)pInner;
      planDebug("FQ-push: Sort key[%d] col=%s colId=%d aboveAgg=%d type=%d",
             kidx, pCol->colName, (int)pCol->colId, (int)aboveAgg,
             (int)((const SExprNode*)pInner)->resType.type);
      if (pCol->colId == 0 && !aboveAgg) {
        planDebug("FQ-push: Sort NOT pushable - key[%d] col=%s colId==0 (synthetic, not above AGG)", kidx, pCol->colName);
        return false;
      }
      int8_t keyType = (int8_t)((const SExprNode*)pInner)->resType.type;
      if (keyType > 0 && capDisabled(pCap, pOverlay, EXT_DEMOTE_KIND_TYPE, &keyType)) {
        planDebug("FQ-push: Sort NOT pushable - key[%d] col=%s type=%d disabled by capability", kidx, pCol->colName, (int)keyType);
        return false;
      }
    } else {
      bool ok = fqExprIsPushable((SNode*)pInner, pScan, pCap, pOverlay, pPlanCxt);
      planDebug("FQ-push: Sort key[%d] non-col nodeType=%d pushable=%d", kidx, (int)nodeType(pInner), (int)ok);
      if (!ok) {
        planDebug("FQ-push: Sort NOT pushable - key[%d] non-col expr (nodeType=%d) not pushable", kidx, (int)nodeType(pInner));
        return false;
      }
    }
    kidx++;
  }
  planDebug("FQ-push: Sort keys pushable OK (%d keys)", kidx);
  return true;
}

static bool fqSortAboveAggInChain(const SLogicNode* pChildInChain) {
  for (const SLogicNode* pNode = pChildInChain; pNode != NULL; pNode = fqFirstChild(pNode)) {
    if (nodeType(pNode) == QUERY_NODE_LOGIC_PLAN_AGG) {
      return true;
    }

    if (nodeType(pNode) == QUERY_NODE_LOGIC_PLAN_PARTITION) {
      const SPartitionLogicNode* pPart = (const SPartitionLogicNode*)pNode;
      if (pPart->pAggFuncs != NULL && LIST_LENGTH(pPart->pAggFuncs) > 0) {
        return true;
      }
    }
  }

  return false;
}

// Check if an AGG (GROUP BY / aggregate) node can be pushed to the remote source.
static bool fqAggIsPushdownable(const SLogicNode* pNode, SScanLogicNode* pScan,
                                 const SExtSourceCapability* pCap,
                                 const SExtCapSessionOverlay* pOverlay,
                                 const SPlanContext* pPlanCxt) {
  if (nodeType(pNode) != QUERY_NODE_LOGIC_PLAN_AGG) return true;
  const SAggLogicNode* pAgg = (const SAggLogicNode*)pNode;
  if (OPTIMIZE_FLAG_TEST_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_FQ_UNIQUE_LOCAL)) {
    planDebug("FQ-push: AGG NOT pushable - rewritten UNIQUE must keep local semantics agg=%p",
              pAgg);
    return false;
  }

  // A multi-branch AGG is the logical dedup container of UNION/INTERSECT-like
  // set-op shapes. The generic fqPushdown collect/commit path is single-docking-
  // scan oriented: if it accepts this AGG it only preserves one branch and marks
  // the sibling scans as already processed, which collapses UNION DISTINCT into a
  // single-branch DISTINCT/GROUP BY remote SQL. Keep these containers for the
  // dedicated FqUnionDedup rule, which builds a multi-child remote set-op plan.
  if (pNode->pChildren != NULL && LIST_LENGTH(pNode->pChildren) > 1) {
    planDebug("FQ-push: AGG NOT pushable - multi-branch dedup/set-op container children=%d handled by FqUnionDedup",
              (int)LIST_LENGTH(pNode->pChildren));
    return false;
  }

  // Check AGG clause capability
  EExtClauseKind aggClause = EXT_CLAUSE_AGG;
  if (!fqClauseIsPushable(aggClause, pCap ? pCap->dialect : -1, pOverlay) ||
      capDisabled(pCap, pOverlay, EXT_DEMOTE_KIND_CLAUSE, &aggClause)) {
    planDebug("FQ-push: AGG NOT pushable - AGG clause disabled by capability");
    return false;
  }

  // Check DISTINCT capability when there are no agg funcs (SELECT DISTINCT)
  bool isDistinct = (pAgg->pAggFuncs == NULL || LIST_LENGTH(pAgg->pAggFuncs) == 0);
  if (isDistinct) {
    EExtClauseKind distClause = EXT_CLAUSE_DISTINCT;
    if (!fqClauseIsPushable(distClause, pCap ? pCap->dialect : -1, pOverlay) ||
        capDisabled(pCap, pOverlay, EXT_DEMOTE_KIND_CLAUSE, &distClause)) {
      planDebug("FQ-push: AGG NOT pushable - DISTINCT clause disabled by capability");
      return false;
    }
  }

  // Check group key expressions
  int32_t kidx = 0;
  SNode* pGS = NULL;
  FOREACH(pGS, pAgg->pGroupKeys) {
    SNode* pExpr = pGS;
    if (nodeType(pExpr) == QUERY_NODE_GROUPING_SET) {
      pExpr = nodesListGetNode(((SGroupingSetNode*)pExpr)->pParameterList, 0);
    }
    if (pExpr == NULL) { kidx++; continue; }
    bool ok = fqExprIsPushable(pExpr, pScan, pCap, pOverlay, pPlanCxt);
    planDebug("FQ-push: AGG groupKey[%d] nodeType=%d pushable=%d", kidx, (int)nodeType(pExpr), (int)ok);
    if (!ok) {
      planDebug("FQ-push: AGG NOT pushable - groupKey[%d] blocked (nodeType=%d)", kidx, (int)nodeType(pExpr));
      return false;
    }
    kidx++;
  }

  // Check aggregate function expressions
  int32_t fidx = 0;
  SNode* pFunc = NULL;
  FOREACH(pFunc, pAgg->pAggFuncs) {
    // _group_key(expr) is a TDengine internal pass-through for a GROUP BY key
    // (it re-emits the grouping column), not a real aggregate.  It is rendered
    // via the GROUP BY clause, not pushed as an aggregate function — skip it
    // when judging pushability (same treatment as window pseudo-columns).
    if (nodeType(pFunc) == QUERY_NODE_FUNCTION &&
        fmIsGroupKeyFunc(((SFunctionNode*)pFunc)->funcId)) {
      fidx++;
      continue;
    }
    bool ok = fqExprIsPushable(pFunc, pScan, pCap, pOverlay, pPlanCxt);
    if (nodeType(pFunc) == QUERY_NODE_FUNCTION) {
      planDebug("FQ-push: AGG func[%d] name=%s nodeType=%d pushable=%d",
             fidx, ((SFunctionNode*)pFunc)->functionName, (int)nodeType(pFunc), (int)ok);
    } else {
      planDebug("FQ-push: AGG func[%d] nodeType=%d pushable=%d", fidx, (int)nodeType(pFunc), (int)ok);
    }
    if (!ok) {
      planDebug("FQ-push: AGG NOT pushable - func[%d] blocked (nodeType=%d)", fidx, (int)nodeType(pFunc));
      return false;
    }
    fidx++;
  }

  // Check HAVING condition (pConditions on the AGG node)
  if (pAgg->node.pConditions) {
    EExtClauseKind havingClause = EXT_CLAUSE_HAVING;
    if (!fqClauseIsPushable(havingClause, pCap ? pCap->dialect : -1, pOverlay) ||
        capDisabled(pCap, pOverlay, EXT_DEMOTE_KIND_CLAUSE, &havingClause)) {
      planDebug("FQ-push: AGG NOT pushable - HAVING clause disabled by capability");
      return false;
    }
    bool ok = fqExprIsPushable(pAgg->node.pConditions, pScan, pCap, pOverlay, pPlanCxt);
    planDebug("FQ-push: AGG HAVING nodeType=%d pushable=%d", (int)nodeType(pAgg->node.pConditions), (int)ok);
    if (!ok) {
      planDebug("FQ-push: AGG NOT pushable - HAVING expr blocked");
      return false;
    }
  }

  // Guard: if the scan's current WHERE cannot be harvested into the remote
  // leaf, do NOT push the AGG. Under the collect->commit model the scan still
  // owns its WHERE while AGG pushability is being judged, so pScan->node
  // .pConditions != NULL by itself does not mean the filter will remain local.
  // We must re-judge the WHERE against the remote capability matrix and the
  // expression pushability rules. Only a truly local-only WHERE blocks AGG
  // pushdown; a fully pushable WHERE will be moved onto the remote leaf by
  // fqCommitPushChain and is safe to combine with remote AGG/DISTINCT.
  if (pScan->node.pConditions != NULL) {
    EExtClauseKind whereClause = EXT_CLAUSE_FILTER;
    bool whereClauseEnabled = fqClauseIsPushable(whereClause, pCap ? pCap->dialect : -1, pOverlay) &&
                              !capDisabled(pCap, pOverlay, EXT_DEMOTE_KIND_CLAUSE, &whereClause);
    bool whereExprPushable = whereClauseEnabled &&
                             fqExprIsPushable(pScan->node.pConditions, pScan, pCap, pOverlay, pPlanCxt);
    planDebug("FQ-push: AGG guard scanWhere nodeType=%d whereClauseEnabled=%d whereExprPushable=%d",
              (int)nodeType(pScan->node.pConditions), (int)whereClauseEnabled, (int)whereExprPushable);
    if (!whereExprPushable) {
      planDebug("FQ-push: AGG NOT pushable - scan WHERE must stay local");
      return false;
    }
  }

  planDebug("FQ-push: AGG pushable OK (groupKeys=%d aggFuncs=%d)", kidx, fidx);
  return true;
}

// Check if an INDEF_ROWS_FUNC node (LAG/LEAD/etc.) can be pushed to the remote source.
// Pushability is determined solely by the fqPushCap capability matrix of each function.
// IndefRowsFunc is treated the same as AGG for FQ pushdown purposes.
static bool fqIndefIsPushdownable(const SLogicNode* pNode, SScanLogicNode* pScan,
                                   const SExtSourceCapability* pCap,
                                   const SExtCapSessionOverlay* pOverlay,
                                   const SPlanContext* pPlanCxt) {
  if (nodeType(pNode) != QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC) return true;
  const SIndefRowsFuncLogicNode* pIndef = (const SIndefRowsFuncLogicNode*)pNode;

  // All functions must pass fqPushCap check
  SNode* pFunc = NULL;
  int32_t fidx = 0;
  FOREACH(pFunc, pIndef->pFuncs) {
    bool ok = fqExprIsPushable(pFunc, pScan, pCap, pOverlay, pPlanCxt);
    if (nodeType(pFunc) == QUERY_NODE_FUNCTION) {
      planDebug("FQ-push: INDEF func[%d] name=%s pushable=%d",
             fidx, ((SFunctionNode*)pFunc)->functionName, (int)ok);
    }
    if (!ok) {
      planDebug("FQ-push: INDEF NOT pushable - func[%d] blocked", fidx);
      return false;
    }
    fidx++;
  }

  planDebug("FQ-push: INDEF pushable OK (funcs=%d)", fidx);
  return true;
}

// ─── FQ pushdown sub-function helpers ──────────────────────────────────────
// Each helper handles one category of pushdown for federated queries.
// Constructs not handled here (Window/JOIN/Subquery) execute locally over
// rows returned by the ExternalScan — the local executor is the correct
// fallback path, not a missing feature.
// ─────────────────────────────────────────────────────────────────────────────

// Returns true if this condition is LIKE / NOT LIKE on a PG JSONB column.
// Such conditions cannot be sent to the remote DB (PG rejects LIKE on JSONB);
// they must be executed locally by the FederatedScan operator instead.
static bool fqIsJsonbLikeOp(SNode* pCond, SScanLogicNode* pScan) {
  if (pCond == NULL || pScan == NULL) return false;
  if (nodeType(pCond) != QUERY_NODE_OPERATOR) return false;
  SOperatorNode* pOp = (SOperatorNode*)pCond;
  if (pOp->opType != OP_TYPE_LIKE && pOp->opType != OP_TYPE_NOT_LIKE) return false;
  SNode* pLeft = pOp->pLeft;
  if (pLeft == NULL || nodeType(pLeft) != QUERY_NODE_COLUMN) return false;
  const char* colName = ((SColumnNode*)pLeft)->colName;
  if (colName == NULL || colName[0] == '\0') return false;

  // Look up the column's external type in the ext table metadata.
  if (pScan->pExtTableNode == NULL) return false;
  SExtTableNode* pExtNode = (SExtTableNode*)pScan->pExtTableNode;
  SExtTableMeta* pExtMeta = pExtNode->pExtMeta;
  if (pExtMeta == NULL || pExtMeta->pCols == NULL) return false;

  for (int32_t i = 0; i < pExtMeta->numOfCols; i++) {
    if (strcasecmp(pExtMeta->pCols[i].colName, colName) == 0) {
      return strcasecmp(pExtMeta->pCols[i].extTypeName, "jsonb") == 0;
    }
  }
  return false;
}

// InfluxDB 3.0 (DataFusion) requires all time-column filters to produce a
// contiguous time range.  IN / NOT IN on the time column cannot be expressed
// in any SQL syntax that DataFusion accepts (!=, NOT, NOT IN, OR on time all
// fail with "The operator must be a comparison operator to propagate intervals").
// Keep these conditions local so the executor applies them via doFilter().
static bool fqIsInfluxTimeInOp(SNode* pCond, SScanLogicNode* pScan) {
  if (pCond == NULL || pScan == NULL) return false;
  if (nodeType(pCond) != QUERY_NODE_OPERATOR) return false;
  SOperatorNode* pOp = (SOperatorNode*)pCond;
  if (pOp->opType != OP_TYPE_IN && pOp->opType != OP_TYPE_NOT_IN) return false;
  // Check if target is InfluxDB
  if (pScan->pExtTableNode == NULL) return false;
  SExtTableNode* pExtNode = (SExtTableNode*)pScan->pExtTableNode;
  if ((EExtSourceType)pExtNode->sourceType != EXT_SOURCE_INFLUXDB) return false;
  // Check if left operand is a TIMESTAMP column
  if (pOp->pLeft == NULL) return false;
  if (nodeType(pOp->pLeft) == QUERY_NODE_COLUMN) {
    return ((SExprNode*)pOp->pLeft)->resType.type == TSDB_DATA_TYPE_TIMESTAMP;
  }
  return false;
}

// REMOTE_* descendant judgement must follow logic plans rather than AST so the
// collect phase sees the exact expression/operator tree that optimizer rules are
// mutating.  Stage 1 has already created every subquery logic plan, so collect
// can recurse through the shared planner registry by subQIdx and decide both:
//   1. whether the referenced subquery is pushable, and
//   2. which render mode the REMOTE_* node should carry into stage 3 rewiring.
typedef struct SFqLogicExprCtx {
  bool                      hasFunction;
  bool                      pushable;
  SScanLogicNode*           pScan;
  const SExtSourceCapability* pCap;
  const SExtCapSessionOverlay* pOverlay;
  const SPlanContext*       pPlanCxt;
  int32_t                   ownerSubQIdx;
} SFqLogicExprCtx;

static EDealRes fqExprHasFunctionWalker(SNode* pNode, void* pContext) {
  SFqLogicExprCtx* pCtx = (SFqLogicExprCtx*)pContext;
  if (QUERY_NODE_FUNCTION == nodeType(pNode)) {
    pCtx->hasFunction = true;
    return DEAL_RES_END;
  }
  return DEAL_RES_CONTINUE;
}

static EDealRes fqExprPushableWalker(SNode* pNode, void* pContext) {
  SFqLogicExprCtx* pCtx = (SFqLogicExprCtx*)pContext;
  SNode*           pRoot = pNode;
  if (QUERY_NODE_TARGET == nodeType(pRoot)) {
    pRoot = ((STargetNode*)pRoot)->pExpr;
  } else if (QUERY_NODE_ORDER_BY_EXPR == nodeType(pRoot)) {
    pRoot = ((SOrderByExprNode*)pRoot)->pExpr;
  } else if (QUERY_NODE_GROUPING_SET == nodeType(pRoot)) {
    pRoot = nodesListGetNode(((SGroupingSetNode*)pRoot)->pParameterList, 0);
  }
  if (NULL == pRoot) {
    return DEAL_RES_CONTINUE;
  }

  bool ok = fqExprIsPushable(pRoot, pCtx->pScan, pCtx->pCap, pCtx->pOverlay, pCtx->pPlanCxt);
  planDebug("FQ-subq: ownerSubQIdx:%d logicExprRootType:%d pushable:%d", pCtx->ownerSubQIdx, (int)nodeType(pRoot),
            (int)ok);
  if (!ok) {
    pCtx->pushable = false;
    return DEAL_RES_END;
  }
  return DEAL_RES_IGNORE_CHILD;
}

static void fqWalkLogicExprFields(SLogicNode* pNode, FNodeWalker walker, void* pContext) {
  if (NULL == pNode) {
    return;
  }

  nodesWalkExprs(pNode->pTargets, walker, pContext);
  nodesWalkExpr(pNode->pConditions, walker, pContext);
  nodesWalkExpr(pNode->pLimit, walker, pContext);
  nodesWalkExpr(pNode->pSlimit, walker, pContext);

  switch (nodeType(pNode)) {
    case QUERY_NODE_LOGIC_PLAN_SCAN: {
      SScanLogicNode* pScan = (SScanLogicNode*)pNode;
      nodesWalkExprs(pScan->pScanCols, walker, pContext);
      nodesWalkExprs(pScan->pScanPseudoCols, walker, pContext);
      nodesWalkExpr(pScan->pTimeRange, walker, pContext);
      nodesWalkExpr(pScan->pExtTimeRange, walker, pContext);
      nodesWalkExpr(pScan->pPrimaryCond, walker, pContext);
      nodesWalkExpr(pScan->pTagCond, walker, pContext);
      nodesWalkExpr(pScan->pTagIndexCond, walker, pContext);
      nodesWalkExprs(pScan->pDynamicScanFuncs, walker, pContext);
      nodesWalkExprs(pScan->pGroupTags, walker, pContext);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_JOIN: {
      SJoinLogicNode* pJoin = (SJoinLogicNode*)pNode;
      nodesWalkExpr(pJoin->pWindowOffset, walker, pContext);
      nodesWalkExpr(pJoin->pJLimit, walker, pContext);
      nodesWalkExpr(pJoin->addPrimEqCond, walker, pContext);
      nodesWalkExpr(pJoin->pPrimKeyEqCond, walker, pContext);
      nodesWalkExpr(pJoin->pColEqCond, walker, pContext);
      nodesWalkExpr(pJoin->pColOnCond, walker, pContext);
      nodesWalkExpr(pJoin->pTagEqCond, walker, pContext);
      nodesWalkExpr(pJoin->pTagOnCond, walker, pContext);
      nodesWalkExpr(pJoin->pFullOnCond, walker, pContext);
      nodesWalkExprs(pJoin->pLeftEqNodes, walker, pContext);
      nodesWalkExprs(pJoin->pRightEqNodes, walker, pContext);
      nodesWalkExpr(pJoin->pLeftOnCond, walker, pContext);
      nodesWalkExpr(pJoin->pRightOnCond, walker, pContext);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_AGG: {
      SAggLogicNode* pAgg = (SAggLogicNode*)pNode;
      nodesWalkExprs(pAgg->pGroupKeys, walker, pContext);
      nodesWalkExprs(pAgg->pAggFuncs, walker, pContext);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_PROJECT:
      nodesWalkExprs(((SProjectLogicNode*)pNode)->pProjections, walker, pContext);
      break;
    case QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC:
      nodesWalkExprs(((SIndefRowsFuncLogicNode*)pNode)->pFuncs, walker, pContext);
      break;
    case QUERY_NODE_LOGIC_PLAN_INTERP_FUNC: {
      SInterpFuncLogicNode* pInterp = (SInterpFuncLogicNode*)pNode;
      nodesWalkExprs(pInterp->pFuncs, walker, pContext);
      nodesWalkExpr(pInterp->pTimeRange, walker, pContext);
      nodesWalkExpr(pInterp->pFillValues, walker, pContext);
      nodesWalkExpr(pInterp->pTimeSeries, walker, pContext);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_FORECAST_FUNC:
    case QUERY_NODE_LOGIC_PLAN_ANALYSIS_FUNC:
      nodesWalkExprs(((SForecastFuncLogicNode*)pNode)->pFuncs, walker, pContext);
      break;
    case QUERY_NODE_LOGIC_PLAN_GROUP_CACHE:
      nodesWalkExprs(((SGroupCacheLogicNode*)pNode)->pGroupCols, walker, pContext);
      break;
    case QUERY_NODE_LOGIC_PLAN_WINDOW: {
      SWindowLogicNode* pWin = (SWindowLogicNode*)pNode;
      nodesWalkExprs(pWin->pFuncs, walker, pContext);
      nodesWalkExpr(pWin->pTimeRange, walker, pContext);
      nodesWalkExpr(pWin->pTspk, walker, pContext);
      nodesWalkExpr(pWin->pTsEnd, walker, pContext);
      nodesWalkExprs(pWin->pStateExprs, walker, pContext);
      nodesWalkExpr(pWin->pStartCond, walker, pContext);
      nodesWalkExpr(pWin->pEndCond, walker, pContext);
      nodesWalkExprs(pWin->pColList, walker, pContext);
      nodesWalkExprs(pWin->pProjs, walker, pContext);
      nodesWalkExprs(pWin->extFill.pFillExprs, walker, pContext);
      nodesWalkExpr(pWin->extFill.pFillValues, walker, pContext);
      nodesWalkExprs(pWin->pAnomalyExpr, walker, pContext);
      nodesWalkExpr(pWin->pSubquery, walker, pContext);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_FILL: {
      SFillLogicNode* pFill = (SFillLogicNode*)pNode;
      nodesWalkExprs(pFill->pFillExprs, walker, pContext);
      nodesWalkExprs(pFill->pNotFillExprs, walker, pContext);
      nodesWalkExpr(pFill->pWStartTs, walker, pContext);
      nodesWalkExpr(pFill->pValues, walker, pContext);
      nodesWalkExpr(pFill->pTimeRange, walker, pContext);
      nodesWalkExprs(pFill->pFillNullExprs, walker, pContext);
      nodesWalkExpr(pFill->pSurroundingTime, walker, pContext);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_SORT:
      nodesWalkExprs(((SSortLogicNode*)pNode)->pSortKeys, walker, pContext);
      break;
    case QUERY_NODE_LOGIC_PLAN_PARTITION: {
      SPartitionLogicNode* pPart = (SPartitionLogicNode*)pNode;
      nodesWalkExprs(pPart->pPartitionKeys, walker, pContext);
      nodesWalkExprs(pPart->pTags, walker, pContext);
      nodesWalkExpr(pPart->pSubtable, walker, pContext);
      nodesWalkExprs(pPart->pAggFuncs, walker, pContext);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN: {
      SVirtualScanLogicNode* pVScan = (SVirtualScanLogicNode*)pNode;
      nodesWalkExprs(pVScan->pScanCols, walker, pContext);
      nodesWalkExprs(pVScan->pScanPseudoCols, walker, pContext);
      nodesWalkExprs(pVScan->pLocalTags, walker, pContext);
      nodesWalkExprs(pVScan->pRefTagCols, walker, pContext);
      nodesWalkExpr(pVScan->pTagFilterCond, walker, pContext);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL:
      nodesWalkExprs(((SDynQueryCtrlLogicNode*)pNode)->vtbScan.pScanCols, walker, pContext);
      break;
    default:
      break;
  }
}

// Returns true when the inner FROM external table requires re-rendering (2b)
// rather than inlining the raw user SQL (DIRECT_SQL).
//
// DIRECT_SQL is valid ONLY when the raw subquery text carries NO TDengine
// source-name qualifier — i.e. the user wrote a bare table (or db.tbl) under an
// active USE <source> context, so the source name was injected from context and
// the raw text (e.g. "region_info" / "mydb.region_info") is already a valid
// remote identifier.  This is recorded at parse time as
// SExtTableNode.srcNameFromPath == false.
//
// When srcNameFromPath == true the user wrote "src.tbl" / "src.schema.tbl"; the
// raw text carries the source name that the remote DB cannot resolve, so the
// inner SELECT MUST be re-rendered (dynAppendTablePath strips the source name
// and emits the real remote db/schema/table).  The predicate FORM is preserved
// in either render mode — 2b only re-renders the inner SELECT body, never the
// surrounding IN/ALL/ANY/EXISTS shape.
static bool fqExtTableNeedsNameXlate(const SExtTableNode* pExt) {
  if (pExt == NULL) return false;
  bool needXlate = pExt->srcNameFromPath;
  planDebug("FQ-xlate: ext table fedDb=%s fedTbl=%s schema=%s srcDb=%s remoteTbl=%s srcNameFromPath=%d -> needXlate=%d",
            pExt->table.dbName, pExt->table.tableName, pExt->schemaName,
            pExt->srcDatabase, pExt->remoteTableName, (int)pExt->srcNameFromPath, (int)needXlate);
  return needXlate;
}

static bool fqLogicPlanNeedsTransform(const SLogicNode* pNode) {
  if (pNode == NULL) {
    return false;
  }

  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    const SScanLogicNode* pScan = (const SScanLogicNode*)pNode;
    if (pScan->pExtTableNode != NULL && QUERY_NODE_EXTERNAL_TABLE == nodeType(pScan->pExtTableNode) &&
        fqExtTableNeedsNameXlate((const SExtTableNode*)pScan->pExtTableNode)) {
      planDebug("FQ-xlate: logic scan needs name translate, logicRootType:%d", (int)nodeType(pNode));
      return true;
    }
  }

  SFqLogicExprCtx ctx = {.hasFunction = false};
  fqWalkLogicExprFields((SLogicNode*)pNode, fqExprHasFunctionWalker, &ctx);
  if (ctx.hasFunction) {
    planDebug("FQ-xlate: logic plan needs transform because function exists, logicRootType:%d", (int)nodeType(pNode));
    return true;
  }

  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    const SScanLogicNode* pScan = (const SScanLogicNode*)pNode;
    if (pScan->pRemoteLogicPlan != NULL && fqLogicPlanNeedsTransform((const SLogicNode*)pScan->pRemoteLogicPlan)) {
      return true;
    }
  }

  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    if (fqLogicPlanNeedsTransform((const SLogicNode*)pChild)) {
      return true;
    }
  }
  return false;
}

static bool fqLogicTreeUsesOnlySourceRecursive(const SLogicNode* pNode, int32_t ownerSubQIdx,
                                               int8_t sourceType, const char* pSourceName) {
  if (NULL == pNode) {
    return true;
  }

  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType((SNode*)pNode)) {
    const SScanLogicNode* pScan = (const SScanLogicNode*)pNode;
    if (SCAN_TYPE_EXTERNAL != pScan->scanType) {
      planDebug("FQ-subq: subQIdx:%d same-source failed - non-external scan node=%p scanType=%d",
                ownerSubQIdx, pScan, (int)pScan->scanType);
      return false;
    }

    const SExtTableNode* pExt = (const SExtTableNode*)pScan->pExtTableNode;
    if (NULL == pExt) {
      planDebug("FQ-subq: subQIdx:%d same-source failed - external scan missing ext node=%p",
                ownerSubQIdx, pScan);
      return false;
    }

    if (pExt->sourceType != sourceType ||
        0 != strncmp(pExt->sourceName, pSourceName, TSDB_EXT_SOURCE_NAME_LEN)) {
      planDebug("FQ-subq: subQIdx:%d same-source failed - scan=%p source=(%d,%s) expected=(%d,%s)",
                ownerSubQIdx, pScan, (int)pExt->sourceType, pExt->sourceName,
                (int)sourceType, NULL != pSourceName ? pSourceName : "(null)");
      return false;
    }

    if (NULL != pScan->pRemoteLogicPlan &&
        !fqLogicTreeUsesOnlySourceRecursive((const SLogicNode*)pScan->pRemoteLogicPlan, ownerSubQIdx,
                                            sourceType, pSourceName)) {
      planDebug("FQ-subq: subQIdx:%d same-source failed - nested remote logic on scan=%p",
                ownerSubQIdx, pScan);
      return false;
    }
  }

  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    if (!fqLogicTreeUsesOnlySourceRecursive((const SLogicNode*)pChild, ownerSubQIdx, sourceType, pSourceName)) {
      return false;
    }
  }

  return true;
}

static bool fqLogicTreeIsPushableRecursive(const SLogicNode* pNode, int32_t ownerSubQIdx, SScanLogicNode* pScan,
                                           const SExtSourceCapability* pCap,
                                           const SExtCapSessionOverlay* pOverlay,
                                           const SPlanContext* pPlanCxt) {
  if (pNode == NULL) {
    return true;
  }

  SScanLogicNode* pEvalScan = (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode))
                                  ? (SScanLogicNode*)pNode
                                  : fqFindExternalLogicLeafScan((SLogicNode*)pNode);
  bool usedFallbackScan = false;
  if (NULL == pEvalScan) {
    pEvalScan = pScan;
    usedFallbackScan = true;
  }

  planDebug("FQ-subq: subQIdx:%d inspect logic nodeType:%d evalScan:%p evalScanType:%d fallbackOuterScan:%d outerScan:%p",
            ownerSubQIdx, (int)nodeType(pNode), pEvalScan,
            (NULL != pEvalScan) ? (int)pEvalScan->scanType : -1, (int)usedFallbackScan, pScan);

  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    const SScanLogicNode* pLogicScan = (const SScanLogicNode*)pNode;
    if (pLogicScan->scanType != SCAN_TYPE_EXTERNAL) {
      planDebug("FQ-subq: subQIdx:%d logic scanType:%d not external, keep local", ownerSubQIdx,
                (int)pLogicScan->scanType);
      return false;
    }
  } else {
    bool layerOk = fqLayerIsPushable((SLogicNode*)pNode, pEvalScan, pCap, pOverlay, pPlanCxt);
    planDebug("FQ-subq: subQIdx:%d logic layerType:%d layerPushable:%d evalScan:%p fallbackOuterScan:%d",
              ownerSubQIdx, (int)nodeType(pNode), (int)layerOk, pEvalScan, (int)usedFallbackScan);
    if (!layerOk) {
      return false;
    }
  }

  SFqLogicExprCtx ctx = {
      .hasFunction = false,
      .pushable = true,
      .pScan = pEvalScan,
      .pCap = pCap,
      .pOverlay = pOverlay,
      .pPlanCxt = pPlanCxt,
      .ownerSubQIdx = ownerSubQIdx,
  };
  fqWalkLogicExprFields((SLogicNode*)pNode, fqExprPushableWalker, &ctx);
  if (!ctx.pushable) {
    planDebug("FQ-subq: subQIdx:%d logic node blocked, nodeType:%d", ownerSubQIdx, (int)nodeType(pNode));
    return false;
  }

  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    SScanLogicNode* pLogicScan = (SScanLogicNode*)pNode;
    if (pLogicScan->pRemoteLogicPlan != NULL &&
        !fqLogicTreeIsPushableRecursive((SLogicNode*)pLogicScan->pRemoteLogicPlan, ownerSubQIdx, pEvalScan, pCap,
                                        pOverlay, pPlanCxt)) {
      return false;
    }
  }

  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    if (!fqLogicTreeIsPushableRecursive((SLogicNode*)pChild, ownerSubQIdx, pEvalScan, pCap, pOverlay, pPlanCxt)) {
      return false;
    }
  }
  return true;
}

static bool fqSubqueryLogicIsPushable(const SPlanContext* pPlanCxt, int32_t subQIdx, ENodeType remoteNodeType,
                                      SScanLogicNode* pScan, const SExtSourceCapability* pCap,
                                      const SExtCapSessionOverlay* pOverlay) {
  if (NULL == pPlanCxt) {
    planDebug("FQ-subq: missing plan context for subQIdx:%d remoteNodeType:%d", subQIdx, (int)remoteNodeType);
    return false;
  }
  if (planIsSubqueryPushedDown(pPlanCxt, subQIdx)) {
    planDebug("FQ-subq: subQIdx:%d already marked pushedDown, treat as absorbed", subQIdx);
    return true;
  }

  SLogicNode* pLogicRoot = planGetSubqueryRenderLogic(pPlanCxt, subQIdx, remoteNodeType);
  if (NULL == pLogicRoot) {
    planDebug("FQ-subq: subQIdx:%d logic root missing for remoteNodeType:%d", subQIdx, (int)remoteNodeType);
    return false;
  }

  SScanLogicNode* pSubqScan = fqFindExternalLogicLeafScan(pLogicRoot);
  planDebug("FQ-subq: subQIdx:%d logic rootType:%d rootEvalScan:%p rootEvalScanType:%d outerScan:%p outerScanType:%d remoteNodeType:%d",
            subQIdx, (int)nodeType(pLogicRoot), pSubqScan, (NULL != pSubqScan) ? (int)pSubqScan->scanType : -1,
            pScan, (NULL != pScan) ? (int)pScan->scanType : -1, (int)remoteNodeType);

  const SExtTableNode* pOuterExt = (NULL != pScan) ? (const SExtTableNode*)pScan->pExtTableNode : NULL;
  if (NULL == pOuterExt) {
    planDebug("FQ-subq: subQIdx:%d not pushable - outer scan missing external source info outerScan=%p",
              subQIdx, pScan);
    return false;
  }

  if (!fqLogicTreeUsesOnlySourceRecursive(pLogicRoot, subQIdx, pOuterExt->sourceType, pOuterExt->sourceName)) {
    planDebug("FQ-subq: subQIdx:%d not pushable - subquery source differs from outer source=(%d,%s)",
              subQIdx, (int)pOuterExt->sourceType, pOuterExt->sourceName);
    return false;
  }

  bool pushable = fqLogicTreeIsPushableRecursive(pLogicRoot, subQIdx, pSubqScan ? pSubqScan : pScan, pCap,
                                                 pOverlay, pPlanCxt);
  planDebug("FQ-subq: subQIdx:%d logic rootType:%d pushable:%d evalScan:%p", subQIdx, (int)nodeType(pLogicRoot),
            (int)pushable, pSubqScan ? pSubqScan : pScan);
  return pushable;
}

static bool fqAssignNodeRenderMode(SNode* pNode, EExtClauseKind clause, int32_t dialect,
                                   const SPlanContext* pPlanCxt, int32_t subQIdx,
                                   EFqSubqRenderMode* pModeOut);

// Decide the two-state render mode for a subquery REMOTE_* node by combining
// native-form clause pushability with inner-logic content analysis.
//   needXlate= inner SELECT needs 2b transform (function/table-name)
// Result:
//   !needXlate  -> DIRECT_SQL   (inline original SQL)
//    needXlate  -> ORIG_TYPE_2B (keep predicate form, 2b inner)
// ─────────────────────────────────────────────────────────────────────────────
// v2 two-state pushdown — render-mode assignment (POST-COMMIT, single pass).
//
// Runs AFTER fqCommitPushChain has committed the pushable WHERE into
// pRemoteLogicPlan (WHERE subquery predicates flow through the SAME commit path
// as ordinary WHERE — fqCloneScanAsLeaf deep-clones node.pConditions).  This
// pass walks the committed remote subtree's conditions, and for each REMOTE_*
// subquery node decides its render mode by combining direct clause pushability
// with inner-logic content analysis (needs 2b transform).  Collect stores only
// renderMode; stage 3 later clones the referenced logic subtree onto pRemotePlan after final
// pushedDown/remap decisions are complete.
//
// Assigning here (not as a side-effect of the pushability predicate) keeps
// fqExprIsPushable pure and avoids mutating nodes mid-probe.
// ─────────────────────────────────────────────────────────────────────────────
static bool fqSubqueryBodyTranslateIsSupported(int32_t dialect) {
  return dialect == EXT_SQL_DIALECT_MYSQL ||
         dialect == EXT_SQL_DIALECT_POSTGRES ||
         dialect == EXT_SQL_DIALECT_INFLUXQL;
}

static bool fqAssignNodeRenderMode(SNode* pNode, EExtClauseKind clause, int32_t dialect,
                                   const SPlanContext* pPlanCxt, int32_t subQIdx,
                                   EFqSubqRenderMode* pModeOut) {
  SLogicNode* pRenderRoot = planGetSubqueryRenderLogic(pPlanCxt, subQIdx, nodeType(pNode));
  bool needXlate = (NULL != pRenderRoot) ? fqLogicPlanNeedsTransform(pRenderRoot) : false;
  if (needXlate && !fqSubqueryBodyTranslateIsSupported(dialect)) {
    planDebug("FQ-render: subq node type=%d clause=%d subQIdx=%d needTransform=1 but dialect=%d has no 2b body translation support",
              (int)nodeType(pNode), (int)clause, subQIdx, dialect);
    return false;
  }

  EFqSubqRenderMode mode = needXlate ? FQ_SUBQ_RENDER_ORIG_TYPE_2B : FQ_SUBQ_RENDER_DIRECT_SQL;
  *pModeOut = mode;
  planDebug("FQ-render: subq node type=%d clause=%d renderMode=%d subQIdx=%d renderRootType=%d needTransform=%d",
            (int)nodeType(pNode), (int)clause, (int)mode, subQIdx,
            pRenderRoot ? (int)nodeType(pRenderRoot) : -1, (int)needXlate);
  return true;
}

// Render-mode decision happens in-place inside fqExprIsPushable's REMOTE_*
// branches during the collect probe, but collect no longer clones any plan onto
// pRemotePlan.  The node only carries renderMode here; stage 3 rewiring later
// clones the referenced logic subtree after all pushedDown decisions are fixed.

static bool fqFuncArgIsType(const SFunctionNode* pFunc, int32_t argIdx, int8_t tdType) {
  if (NULL == pFunc || NULL == pFunc->pParameterList || argIdx < 0 ||
      argIdx >= LIST_LENGTH(pFunc->pParameterList)) {
    return false;
  }

  SNode* pArg = nodesListGetNode(pFunc->pParameterList, argIdx);
  return NULL != pArg && ((SExprNode*)pArg)->resType.type == tdType;
}

static bool fqFuncArgInt64Equals(const SFunctionNode* pFunc, int32_t argIdx, int64_t expected) {
  if (NULL == pFunc || NULL == pFunc->pParameterList || argIdx < 0 ||
      argIdx >= LIST_LENGTH(pFunc->pParameterList)) {
    return false;
  }

  SNode* pArg = nodesListGetNode(pFunc->pParameterList, argIdx);
  if (NULL == pArg || nodeType(pArg) != QUERY_NODE_VALUE) {
    return false;
  }

  SValueNode* pVal = (SValueNode*)pArg;
  if (!IS_INTEGER_TYPE(pVal->node.resType.type)) {
    return false;
  }

  return pVal->datum.i == expected;
}

static bool fqWeekFuncIsPushable(const SFunctionNode* pFunc, int32_t dialect) {
  if (NULL == pFunc || pFunc->funcType != FUNCTION_TYPE_WEEK) {
    return true;
  }

  // After translateAddPrecOutBigint():
  //   WEEK(ts)         -> [ts, precision]
  //   WEEK(ts, mode)   -> [ts, mode, precision]
  //
  // PG / InfluxDB only have ISO-week semantics (EXTRACT(WEEK ...)), which are
  // equivalent to TDengine WEEK(..., 3) / WEEKOFYEAR(). Default WEEK(ts) and
  // non-3 modes must stay local to preserve TDengine semantics.
  if (dialect != EXT_SQL_DIALECT_POSTGRES && dialect != EXT_SQL_DIALECT_INFLUXQL) {
    return true;
  }

  int32_t argc = LIST_LENGTH(pFunc->pParameterList);
  bool    isoWeek = (argc >= 3) && fqFuncArgInt64Equals(pFunc, 1, 3);
  planDebug("FQ-push: WEEK dialect=%d argc=%d isoWeekMode=%d",
            dialect, argc, (int)isoWeek);
  return isoWeek;
}

static bool fqExprTreeHasRemoteScalarValue(const SNode* pExpr) {
  if (NULL == pExpr) {
    return false;
  }

  switch (nodeType(pExpr)) {
    case QUERY_NODE_REMOTE_VALUE:
      return true;
    case QUERY_NODE_TARGET:
      return fqExprTreeHasRemoteScalarValue(((const STargetNode*)pExpr)->pExpr);
    case QUERY_NODE_OPERATOR: {
      const SOperatorNode* pOp = (const SOperatorNode*)pExpr;
      return fqExprTreeHasRemoteScalarValue(pOp->pLeft) ||
             fqExprTreeHasRemoteScalarValue(pOp->pRight);
    }
    case QUERY_NODE_LOGIC_CONDITION: {
      const SLogicConditionNode* pCond = (const SLogicConditionNode*)pExpr;
      const SNode*               pChild = NULL;
      FOREACH(pChild, pCond->pParameterList) {
        if (fqExprTreeHasRemoteScalarValue(pChild)) {
          return true;
        }
      }
      return false;
    }
    case QUERY_NODE_CASE_WHEN: {
      const SCaseWhenNode* pCW = (const SCaseWhenNode*)pExpr;
      if (fqExprTreeHasRemoteScalarValue(pCW->pCase) ||
          fqExprTreeHasRemoteScalarValue(pCW->pElse)) {
        return true;
      }

      const SNode* pItem = NULL;
      FOREACH(pItem, pCW->pWhenThenList) {
        const SWhenThenNode* pWT = (const SWhenThenNode*)pItem;
        if (fqExprTreeHasRemoteScalarValue(pWT->pWhen) ||
            fqExprTreeHasRemoteScalarValue(pWT->pThen)) {
          return true;
        }
      }
      return false;
    }
    case QUERY_NODE_FUNCTION: {
      const SFunctionNode* pFunc = (const SFunctionNode*)pExpr;
      const SNode*         pArg = NULL;
      FOREACH(pArg, pFunc->pParameterList) {
        if (fqExprTreeHasRemoteScalarValue(pArg)) {
          return true;
        }
      }
      return false;
    }
    case QUERY_NODE_NODE_LIST: {
      const SNodeListNode* pList = (const SNodeListNode*)pExpr;
      const SNode*         pItem = NULL;
      FOREACH(pItem, pList->pNodeList) {
        if (fqExprTreeHasRemoteScalarValue(pItem)) {
          return true;
        }
      }
      return false;
    }
    default:
      return false;
  }
}

static bool fqShouldKeepAnalyzeInfluxScalarAggLocal(const SFunctionNode* pFunc, int32_t dialect,
                                                    const SPlanContext* pPlanCxt) {
  if (NULL == pFunc || NULL == pPlanCxt || NULL == pPlanCxt->pAstRoot ||
      dialect != EXT_SQL_DIALECT_INFLUXQL || !fmIsAggFunc(pFunc->funcId)) {
    return false;
  }

  if (QUERY_NODE_EXPLAIN_STMT != nodeType(pPlanCxt->pAstRoot) ||
      !((const SExplainStmt*)pPlanCxt->pAstRoot)->analyze) {
    return false;
  }

  const SNode* pArg = NULL;
  FOREACH(pArg, pFunc->pParameterList) {
    if (fqExprTreeHasRemoteScalarValue(pArg)) {
      return true;
    }
  }

  return false;
}

// Returns true if the expression tree is fully pushable to the remote DB.
// Uses an allowlist: only explicitly allowed node types and operator types
// are considered pushable.  Everything else (FUNCTION, BIT_AND, JSON ops,
// unknown node types) stays in local execution (node.pConditions).
static bool fqExprIsPushable(SNode* pExpr, SScanLogicNode* pScan, const SExtSourceCapability* pCap,
                             const SExtCapSessionOverlay* pOverlay, const SPlanContext* pPlanCxt) {
  if (pExpr == NULL) return true;

  switch (nodeType(pExpr)) {
    case QUERY_NODE_COLUMN:
    case QUERY_NODE_VALUE: {
      int8_t exprType = (int8_t)((SExprNode*)pExpr)->resType.type;
      if (exprType > 0 && capDisabled(pCap, pOverlay, EXT_DEMOTE_KIND_TYPE, &exprType)) {
        return false;
      }
      return true;
    }

    case QUERY_NODE_OPERATOR: {
      SOperatorNode* pOp = (SOperatorNode*)pExpr;
      // Use the bitmap capability matrix to check operator pushability.
      bool opPushable = fqOpIsPushable(pOp->opType, pCap ? pCap->dialect : -1, pOverlay);
      int32_t leftResType = (NULL != pOp->pLeft) ? ((SExprNode*)pOp->pLeft)->resType.type : -1;
      int32_t rightResType = (NULL != pOp->pRight) ? ((SExprNode*)pOp->pRight)->resType.type : -1;
      int32_t compareConvertType = -1;
      if (pOp->opType == OP_TYPE_EQUAL || pOp->opType == OP_TYPE_NOT_EQUAL ||
          pOp->opType == OP_TYPE_GREATER_THAN || pOp->opType == OP_TYPE_GREATER_EQUAL ||
          pOp->opType == OP_TYPE_LOWER_THAN || pOp->opType == OP_TYPE_LOWER_EQUAL) {
        compareConvertType = vectorGetConvertType(leftResType, rightResType);
      }
      planDebug("FQ-expr: operator node=%p opType=%d dialect=%d pushable=%d leftType=%d rightType=%d",
                pOp, (int)pOp->opType, pCap ? (int)pCap->dialect : -1, (int)opPushable,
                pOp->pLeft ? (int)nodeType(pOp->pLeft) : -1,
                pOp->pRight ? (int)nodeType(pOp->pRight) : -1);
      if (compareConvertType >= 0) {
        planDebug("FQ-expr: comparison opType=%d dialect=%d leftResType=%d rightResType=%d convertType=%d",
                  (int)pOp->opType, pCap ? (int)pCap->dialect : -1,
                  leftResType, rightResType, compareConvertType);
      }
      if (!opPushable) {
        if (fqDiagEnabled()) {
          fprintf(stderr,
                  "[FQ-DIAG] expr-operator node=%p opType=%d blocked=op-pushable leftType=%d rightType=%d\n",
                  pOp, (int)pOp->opType, pOp->pLeft ? (int)nodeType(pOp->pLeft) : -1,
                  pOp->pRight ? (int)nodeType(pOp->pRight) : -1);
        }
        return false;
      }
      // EXISTS/NOT EXISTS are rewritten by the parser into REMOTE_ZERO_ROWS
      // compared with 0, so the comparison operands carry the subquery node;
      // pushability is decided when the recursion reaches QUERY_NODE_REMOTE_ZERO_ROWS
      // (capability matrix EXT_CLAUSE_EXISTS_SUBQUERY + inner SELECT pushable).
      // LIKE/NOT LIKE on JSONB column cannot be pushed (PG rejects LIKE on JSONB)
      if (fqIsJsonbLikeOp(pExpr, pScan)) {
        if (fqDiagEnabled()) {
          fprintf(stderr, "[FQ-DIAG] expr-operator node=%p opType=%d blocked=jsonb-like\n",
                  pOp, (int)pOp->opType);
        }
        return false;
      }
      // IN/NOT IN on InfluxDB time column cannot be pushed (DataFusion limitation)
      if (fqIsInfluxTimeInOp(pExpr, pScan)) {
        if (fqDiagEnabled()) {
          fprintf(stderr, "[FQ-DIAG] expr-operator node=%p opType=%d blocked=influx-time-in\n",
                  pOp, (int)pOp->opType);
        }
        return false;
      }
      bool leftOk = fqExprIsPushable(pOp->pLeft, pScan, pCap, pOverlay, pPlanCxt);
      bool rightOk = fqExprIsPushable(pOp->pRight, pScan, pCap, pOverlay, pPlanCxt);
      if (fqDiagEnabled()) {
        fprintf(stderr,
                "[FQ-DIAG] expr-operator node=%p opType=%d leftOk=%d rightOk=%d leftType=%d rightType=%d\n",
                pOp, (int)pOp->opType, (int)leftOk, (int)rightOk,
                pOp->pLeft ? (int)nodeType(pOp->pLeft) : -1,
                pOp->pRight ? (int)nodeType(pOp->pRight) : -1);
      }
      return leftOk && rightOk;
    }

    case QUERY_NODE_LOGIC_CONDITION: {
      SLogicConditionNode* pLogic = (SLogicConditionNode*)pExpr;
      SNode* pChild = NULL;
      FOREACH(pChild, pLogic->pParameterList) {
        if (!fqExprIsPushable(pChild, pScan, pCap, pOverlay, pPlanCxt)) return false;
      }
      return true;
    }

    case QUERY_NODE_CASE_WHEN: {
      SCaseWhenNode* pCW = (SCaseWhenNode*)pExpr;
      if (pCW->pCase && !fqExprIsPushable(pCW->pCase, pScan, pCap, pOverlay, pPlanCxt)) return false;
      SNode* pItem = NULL;
      FOREACH(pItem, pCW->pWhenThenList) {
        SWhenThenNode* pWT = (SWhenThenNode*)pItem;
        if (!fqExprIsPushable(pWT->pWhen, pScan, pCap, pOverlay, pPlanCxt)) return false;
        if (!fqExprIsPushable(pWT->pThen, pScan, pCap, pOverlay, pPlanCxt)) return false;
      }
      if (pCW->pElse && !fqExprIsPushable(pCW->pElse, pScan, pCap, pOverlay, pPlanCxt)) return false;
      return true;
    }

    case QUERY_NODE_NODE_LIST: {
      SNodeListNode* pList = (SNodeListNode*)pExpr;
      SNode* pItem = NULL;
      FOREACH(pItem, pList->pNodeList) {
        if (!fqExprIsPushable(pItem, pScan, pCap, pOverlay, pPlanCxt)) return false;
      }
      return true;
    }

    // Executor-resolved nodes for subquery correlation.
    // Pushability requires two conditions:
    //   1. capability matrix allows the subquery clause type
    //   2. the subquery SELECT's expression nodes are all pushable
    case QUERY_NODE_REMOTE_VALUE_LIST: {
      // IN/NOT IN (subquery)
      EExtClauseKind inClause = EXT_CLAUSE_IN_SUBQUERY;
      int32_t dialect = pCap ? pCap->dialect : -1;
      bool directOk = fqClauseIsPushable(inClause, dialect, pOverlay) &&
                      !capDisabled(pCap, pOverlay, EXT_DEMOTE_KIND_CLAUSE, &inClause);
      if (fqDiagEnabled()) {
        fprintf(stderr,
                "[FQ-DIAG] expr-remote-value-list-enter node=%p subQIdx=%d dialect=%d directOk=%d scan=%p table=%s\n",
                pExpr, ((SRemoteValueListNode*)pExpr)->subQIdx, dialect, (int)directOk, pScan,
                pScan ? pScan->tableName.tname : "(null)");
      }
      planDebug("FQ-push: REMOTE_VALUE_LIST dialect=%d directOk=%d", dialect, (int)directOk);
      if (!directOk) {
        planDebug("FQ-push: REMOTE_VALUE_LIST not pushable - IN_SUBQUERY disabled by capability");
        return false;
      }
      SRemoteValueListNode* pRVL = (SRemoteValueListNode*)pExpr;
      int32_t subQIdx = pRVL->subQIdx;
      planDebug("FQ-push: REMOTE_VALUE_LIST collect subQIdx:%d ownerSubQIdx:%d", subQIdx,
                pPlanCxt ? pPlanCxt->subqueryIndex : -2);
      bool subqueryPushable =
          fqSubqueryLogicIsPushable(pPlanCxt, subQIdx, QUERY_NODE_REMOTE_VALUE_LIST, pScan, pCap, pOverlay);
      if (fqDiagEnabled()) {
        fprintf(stderr,
                "[FQ-DIAG] expr-remote-value-list node=%p subQIdx=%d directOk=%d subqueryPushable=%d scan=%p table=%s\n",
                pRVL, subQIdx, (int)directOk, (int)subqueryPushable, pScan,
                pScan ? pScan->tableName.tname : "(null)");
      }
      if (!subqueryPushable) {
        planDebug("FQ-push: REMOTE_VALUE_LIST not pushable - descendant logic plan blocked (subQIdx=%d)", subQIdx);
        return false;
      }
      bool renderModeOk =
          fqAssignNodeRenderMode((SNode*)pRVL, inClause, dialect, pPlanCxt, subQIdx, &pRVL->renderMode);
      if (fqDiagEnabled()) {
        fprintf(stderr,
                "[FQ-DIAG] expr-remote-value-list node=%p subQIdx=%d renderModeOk=%d renderMode=%d\n",
                pRVL, subQIdx, (int)renderModeOk, (int)pRVL->renderMode);
      }
      if (!renderModeOk) {
        planDebug("FQ-push: REMOTE_VALUE_LIST not pushable - translated inner body unsupported for dialect=%d (subQIdx=%d)",
                  dialect, subQIdx);
        return false;
      }
      return true;
    }

    case QUERY_NODE_REMOTE_VALUE: {
      // scalar subquery
      EExtClauseKind scalarClause = EXT_CLAUSE_SCALAR_SUBQUERY;
      int32_t dialect = pCap ? pCap->dialect : -1;
      bool directOk = fqClauseIsPushable(scalarClause, dialect, pOverlay) &&
                      !capDisabled(pCap, pOverlay, EXT_DEMOTE_KIND_CLAUSE, &scalarClause);
      if (!directOk) {
        planDebug("FQ-push: REMOTE_VALUE not pushable - SCALAR_SUBQUERY disabled by capability");
        return false;
      }
      SRemoteValueNode* pRV = (SRemoteValueNode*)pExpr;
      int32_t subQIdx = pRV->subQIdx;
      planDebug("FQ-push: REMOTE_VALUE collect subQIdx:%d ownerSubQIdx:%d", subQIdx,
                pPlanCxt ? pPlanCxt->subqueryIndex : -2);
      if (!fqSubqueryLogicIsPushable(pPlanCxt, subQIdx, QUERY_NODE_REMOTE_VALUE, pScan, pCap, pOverlay)) {
        planDebug("FQ-push: REMOTE_VALUE not pushable - descendant logic plan blocked (subQIdx=%d)", subQIdx);
        return false;
      }
      if (!fqAssignNodeRenderMode((SNode*)pRV, scalarClause, dialect, pPlanCxt, subQIdx, &pRV->renderMode)) {
        planDebug("FQ-push: REMOTE_VALUE not pushable - translated inner body unsupported for dialect=%d (subQIdx=%d)",
                  dialect, subQIdx);
        return false;
      }
      return true;
    }

// ALL/ANY/SOME are parser-rewritten into a MIN/MAX scalar wrapper:
//   SELECT MIN|MAX(col), _has_null(col) FROM ( <original inner SELECT> )
// The wrapper's projections (MIN/MAX/_has_null) are NOT what gets pushed — the
// renderer inlines the ORIGINAL inner SQL directly as  op ALL|ANY (subq).  So
// pushability must be judged on the ORIGINAL inner SELECT, not the wrapper.
// This unwraps one TempTable layer to reach it; returns the wrapper itself when
// the structure is not the expected MIN/MAX wrapper.
    case QUERY_NODE_REMOTE_ROW: {
      // ANY/ALL/SOME subquery
      EExtClauseKind allAnyClause = EXT_CLAUSE_ALL_ANY_SUBQUERY;
      int32_t dialect = pCap ? pCap->dialect : -1;
      bool directOk = fqClauseIsPushable(allAnyClause, dialect, pOverlay) &&
                      !capDisabled(pCap, pOverlay, EXT_DEMOTE_KIND_CLAUSE, &allAnyClause);
      if (!directOk) {
        planDebug("FQ-push: REMOTE_ROW not pushable - ALL_ANY_SUBQUERY disabled by capability");
        return false;
      }
      SRemoteRowNode* pRR = (SRemoteRowNode*)pExpr;
      int32_t subQIdx = pRR->subQIdx;
      planDebug("FQ-push: REMOTE_ROW collect subQIdx:%d ownerSubQIdx:%d", subQIdx,
                pPlanCxt ? pPlanCxt->subqueryIndex : -2);
      if (!fqSubqueryLogicIsPushable(pPlanCxt, subQIdx, QUERY_NODE_REMOTE_ROW, pScan, pCap, pOverlay)) {
        planDebug("FQ-push: REMOTE_ROW not pushable - descendant logic plan blocked (subQIdx=%d)", subQIdx);
        return false;
      }
      if (!fqAssignNodeRenderMode((SNode*)pRR, allAnyClause, dialect, pPlanCxt, subQIdx, &pRR->renderMode)) {
        planDebug("FQ-push: REMOTE_ROW not pushable - translated inner body unsupported for dialect=%d (subQIdx=%d)",
                  dialect, subQIdx);
        return false;
      }
      return true;
    }

    case QUERY_NODE_REMOTE_ZERO_ROWS: {
      // EXISTS/NOT EXISTS (subquery).  Pushable iff the capability matrix allows
      // EXISTS subquery for this dialect AND the inner SELECT is itself pushable.
      // Otherwise the whole predicate stays local (collect-phase split).
      EExtClauseKind existsClause = EXT_CLAUSE_EXISTS_SUBQUERY;
      int32_t dialect = pCap ? pCap->dialect : -1;
      bool directOk = fqClauseIsPushable(existsClause, dialect, pOverlay) &&
                      !capDisabled(pCap, pOverlay, EXT_DEMOTE_KIND_CLAUSE, &existsClause);
      if (!directOk) {
        planDebug("FQ-push: REMOTE_ZERO_ROWS not pushable - EXISTS_SUBQUERY disabled by capability");
        return false;
      }
      SRemoteZeroRowsNode* pRZ = (SRemoteZeroRowsNode*)pExpr;
      int32_t subQIdx = pRZ->subQIdx;
      planDebug("FQ-push: REMOTE_ZERO_ROWS collect subQIdx:%d ownerSubQIdx:%d", subQIdx,
                pPlanCxt ? pPlanCxt->subqueryIndex : -2);
      if (!fqSubqueryLogicIsPushable(pPlanCxt, subQIdx, QUERY_NODE_REMOTE_ZERO_ROWS, pScan, pCap, pOverlay)) {
        planDebug("FQ-push: REMOTE_ZERO_ROWS not pushable - descendant logic plan blocked (subQIdx=%d)", subQIdx);
        return false;
      }
      if (!fqAssignNodeRenderMode((SNode*)pRZ, existsClause, dialect, pPlanCxt, subQIdx, &pRZ->renderMode)) {
        planDebug("FQ-push: REMOTE_ZERO_ROWS not pushable - translated inner body unsupported for dialect=%d (subQIdx=%d)",
                  dialect, subQIdx);
        return false;
      }
      return true;
    }

    case QUERY_NODE_FUNCTION: {
      SFunctionNode* pFunc = (SFunctionNode*)pExpr;
      int32_t dialect = pCap ? pCap->dialect : -1;
      if (fqShouldKeepAnalyzeInfluxScalarAggLocal(pFunc, dialect, pPlanCxt)) {
        planDebug("FQ-push: func=%s funcId=%d dialect=%d keep local - explain analyze keeps aggregate with scalar subquery local",
                  pFunc->functionName, pFunc->funcId, dialect);
        return false;
      }
      // Bitmap capability matrix: checks static per-dialect push-down support.
      // fqFuncIsPushable uses funcMgtBuiltins[funcId].fqPushCap; returns false
      // for UDFs, out-of-range funcIds, and functions with FQ_CAP_NONE.
      if (!fqFuncIsPushable(pFunc->funcId, dialect, pOverlay)) return false;
      // Dynamic catalog-level demote: connector may have disabled specific
      // functions at runtime via ALTER EXTERNAL SOURCE ... DISABLE FUNC ...
      if (capDisabled(pCap, pOverlay, EXT_DEMOTE_KIND_FUNC, pFunc->functionName)) return false;
      if (fqFuncArgIsType(pFunc, 0, TSDB_DATA_TYPE_NCHAR) &&
          !fqFuncArg0NcharIsPushable(pFunc->funcId, dialect, pOverlay)) {
        planDebug("FQ-push: func=%s funcId=%d dialect=%d keep local - arg0 NCHAR disabled by function capability matrix to preserve TDengine UCS4 byte semantics",
                  pFunc->functionName, pFunc->funcId, dialect);
        return false;
      }
      if (!fqWeekFuncIsPushable(pFunc, dialect)) {
        planDebug("FQ-push: func=%s funcId=%d dialect=%d keep local - non-ISO WEEK semantics are not equivalent on this dialect",
                  pFunc->functionName, pFunc->funcId, dialect);
        return false;
      }
      // CAST(timestamp AS integer/bigint/uint/ubigint) sub-capability check.
      // Only PostgreSQL supports pushing this down (see FQ_CAP_CAST_TS_INT_PG in builtins.c).
      // MySQL: binary protocol returns MYSQL_TIME for TIMESTAMP columns; the connector
      //   cannot safely reinterpret that as an integer epoch, so local conversion is required.
      // InfluxDB 3 (DataFusion): does not support CAST(timestamp AS BIGINT).
      if ((EFunctionType)pFunc->funcType == FUNCTION_TYPE_CAST) {
        bool resIsInt = (pFunc->node.resType.type == TSDB_DATA_TYPE_BIGINT ||
                        pFunc->node.resType.type == TSDB_DATA_TYPE_UBIGINT ||
                        pFunc->node.resType.type == TSDB_DATA_TYPE_INT    ||
                        pFunc->node.resType.type == TSDB_DATA_TYPE_UINT);
        if (resIsInt && pFunc->pParameterList != NULL && LIST_LENGTH(pFunc->pParameterList) >= 1) {
          SNode* pArg0 = nodesListGetNode(pFunc->pParameterList, 0);
          if (pArg0 && ((SExprNode*)pArg0)->resType.type == TSDB_DATA_TYPE_TIMESTAMP) {
            // Requires CAST(ts->int) sub-capability support for this dialect.
            if (!fqFuncSubCapIsPushable(pFunc->funcId, FQ_CAP_CAST_TS_INT_SHIFT, dialect, pOverlay)) {
              return false;
            }
          }
        }
      }
      // Check all arguments are pushable
      SNode* pArg = NULL;
      FOREACH(pArg, pFunc->pParameterList) {
        if (!fqExprIsPushable(pArg, pScan, pCap, pOverlay, pPlanCxt)) return false;
      }
      return true;
    }

    default:
      // Any unknown or unhandled node type — not pushable
      return false;
  }
}

// Harvest WHERE conditions that can be pushed to the remote source.
// Classifies each condition:
//   - EXISTS/NOT EXISTS with RAW_SQL_FRAG  ->  always pushed (pPushedConditions)
//   - Non-pushable expression (FUNCTION, BIT ops, JSONB LIKE, unknown nodes)
//                                          ->  kept local (node.pConditions)
//   - Pushable expression (comparison, arithmetic, pattern, IN, CASE WHEN, etc.)
//                                          ->  pushed to remote SQL (pPushedConditions)
//
// If a condition (or sub-condition) is non-pushable, the ENTIRE condition stays
// in node.pConditions for local execution by FederatedScan.  This is conservative
// for mixed AND conditions (the whole AND stays local) but always correct.
// fqEnsureLocalFilterColsInScanCols ensures all referenced columns are fetched.

// ---------------------------------------------------------------------------
// fqFoldNowTodayInExpr / fqFoldNowTodayForInflux
//
// For InfluxDB (EXT_SQL_DIALECT_INFLUXQL), NOW() and TODAY() cannot be
// rendered as InfluxQL syntax.  Instead, we constant-fold them into a
// TSDB_DATA_TYPE_TIMESTAMP literal using the external source's time
// precision (pExtMeta->tsPrecision) before the pushdown harvest step.
//
// After folding, fqExprIsPushable sees a plain QUERY_NODE_VALUE node, which
// is always pushable, so the condition can be pushed to remote SQL as a
// numeric literal (e.g.  "time < 1749999999000000000").
//
// The walk is recursive: we handle OPERATOR (pLeft/pRight), LOGIC_CONDITION
// (parameter list), CASE_WHEN, and FUNCTION argument lists.  For non-InfluxDB
// dialects the function is a no-op.
// ---------------------------------------------------------------------------
static int32_t fqFoldNowTodayInExpr(SNode** ppNode, int32_t prec, timezone_t tz) {
  if (ppNode == NULL || *ppNode == NULL) return TSDB_CODE_SUCCESS;

  SNode* pNode = *ppNode;

  switch (nodeType(pNode)) {
    case QUERY_NODE_FUNCTION: {
      SFunctionNode* pFunc = (SFunctionNode*)pNode;
      EFunctionType  ft    = (EFunctionType)pFunc->funcType;
      if (ft == FUNCTION_TYPE_NOW || ft == FUNCTION_TYPE_TODAY) {
        int64_t ts = (ft == FUNCTION_TYPE_NOW)
                       ? taosGetTimestamp(prec)
                       : taosGetTimestampToday(prec, tz);
        SNode* pVal = NULL;
        int32_t code = nodesMakeValueNodeFromTimestamp(ts, &pVal);
        if (TSDB_CODE_SUCCESS != code) return code;
        nodesDestroyNode(pNode);
        *ppNode = pVal;
        planDebug("FQ-fold: folded %s() -> %" PRId64 " (prec=%d)",
                  ft == FUNCTION_TYPE_NOW ? "NOW" : "TODAY", ts, prec);
        return TSDB_CODE_SUCCESS;
      }
      /* Recurse into function arguments */
      if (pFunc->pParameterList != NULL) {
        SNode* pArg = NULL;
        WHERE_EACH(pArg, pFunc->pParameterList) {
          int32_t code = fqFoldNowTodayInExpr(&cell->pNode, prec, tz);
          if (TSDB_CODE_SUCCESS != code) return code;
          WHERE_NEXT;
        }
      }
      break;
    }
    case QUERY_NODE_OPERATOR: {
      SOperatorNode* pOp = (SOperatorNode*)pNode;
      int32_t code = fqFoldNowTodayInExpr(&pOp->pLeft, prec, tz);
      if (TSDB_CODE_SUCCESS != code) return code;
      code = fqFoldNowTodayInExpr(&pOp->pRight, prec, tz);
      if (TSDB_CODE_SUCCESS != code) return code;
      break;
    }
    case QUERY_NODE_LOGIC_CONDITION: {
      SLogicConditionNode* pLogic = (SLogicConditionNode*)pNode;
      SNode* pChild = NULL;
      WHERE_EACH(pChild, pLogic->pParameterList) {
        int32_t code = fqFoldNowTodayInExpr(&cell->pNode, prec, tz);
        if (TSDB_CODE_SUCCESS != code) return code;
        WHERE_NEXT;
      }
      break;
    }
    case QUERY_NODE_CASE_WHEN: {
      SCaseWhenNode* pCW = (SCaseWhenNode*)pNode;
      int32_t code = fqFoldNowTodayInExpr(&pCW->pCase, prec, tz);
      if (TSDB_CODE_SUCCESS != code) return code;
      SNode* pItem = NULL;
      WHERE_EACH(pItem, pCW->pWhenThenList) {
        SWhenThenNode* pWT = (SWhenThenNode*)cell->pNode;
        code = fqFoldNowTodayInExpr((SNode**)&pWT->pWhen, prec, tz);
        if (TSDB_CODE_SUCCESS != code) return code;
        code = fqFoldNowTodayInExpr((SNode**)&pWT->pThen, prec, tz);
        if (TSDB_CODE_SUCCESS != code) return code;
        WHERE_NEXT;
      }
      code = fqFoldNowTodayInExpr(&pCW->pElse, prec, tz);
      if (TSDB_CODE_SUCCESS != code) return code;
      break;
    }
    default:
      break;
  }
  return TSDB_CODE_SUCCESS;
}

// Fold NOW()/TODAY() in the scan's WHERE condition for InfluxDB.
// Precision is taken from pExtMeta->tsPrecision; if 0 (unknown) defaults to
// TSDB_TIME_PRECISION_NANO since InfluxDB always uses nanoseconds.



// ---------------------------------------------------------------------------
// fqEnsureLocalFilterColsInScanCols
//
// After the pushdown optimizer reduces pScanCols to only the SELECT output
// columns, any column referenced exclusively in the local WHERE clause
// (node.pConditions) would be missing from pScanCols.  The remote SQL would
// then not fetch those columns, making local filter evaluation impossible.
//
// This helper re-adds any such missing columns to:
//   1. pScan->pScanCols        -  so the remote SQL's SELECT includes them
//   2. pRemoteLogicPlan->pTargets  -  so planPhysiCreater uses the right list
//      for pLeaf->pScanCols (when pRemoteLogicPlan != NULL)
//   3. The Project logic node's pProjections (if a Project is in the chain)
//       -  so assembleRemoteSQL emits them in the SELECT clause
//
// Called immediately after pScanCols is reduced in fqPushdownOptimize.
// ---------------------------------------------------------------------------
static int32_t fqEnsureLocalFilterColsInScanCols(SScanLogicNode* pScan) {
  if (pScan->node.pConditions == NULL) return TSDB_CODE_SUCCESS;

  // Collect all column references from the local filter condition.
  SNodeList* pCondCols = NULL;
  int32_t    code      = nodesCollectColumnsFromNode(pScan->node.pConditions, NULL,
                                                     COLLECT_COL_TYPE_ALL, &pCondCols);
  if (TSDB_CODE_SUCCESS != code || pCondCols == NULL) return code;

  SLogicNode* pTopRemote = (pScan->pRemoteLogicPlan != NULL)
                               ? (SLogicNode*)pScan->pRemoteLogicPlan
                               : NULL;

  // Find the Project logic node in the chain (if any).
  // It may be pTopRemote itself, or a child node in the chain.
  SProjectLogicNode* pProjLogic = NULL;
  if (pTopRemote != NULL) {
    SLogicNode* pCurr = pTopRemote;
    while (pCurr != NULL) {
      if (nodeType(pCurr) == QUERY_NODE_LOGIC_PLAN_PROJECT) {
        pProjLogic = (SProjectLogicNode*)pCurr;
        break;
      }
      pCurr = (pCurr->pChildren && LIST_LENGTH(pCurr->pChildren) > 0)
                  ? (SLogicNode*)nodesListGetNode(pCurr->pChildren, 0)
                  : NULL;
    }
  }

  SNode* pCondCol = NULL;
  FOREACH(pCondCol, pCondCols) {
    if (nodeType(pCondCol) != QUERY_NODE_COLUMN) continue;
    const char* condColName = ((const SColumnNode*)pCondCol)->colName;

    // Check whether condColName already exists in pScan->pScanCols.
    bool found = false;
    SNode* pScanCol = NULL;
    FOREACH(pScanCol, pScan->pScanCols) {
      const SNode* pInner = pScanCol;
      if (nodeType(pInner) == QUERY_NODE_TARGET)
        pInner = ((const STargetNode*)pInner)->pExpr;
      if (pInner != NULL && nodeType(pInner) == QUERY_NODE_COLUMN) {
        const SColumnNode* pScanColNode = (const SColumnNode*)pInner;
        if (pScanColNode->colName[0] != '\0') {
          if (strcasecmp(pScanColNode->colName, condColName) == 0) { found = true; break; }
        } else {
          int32_t condColId = ((const SColumnNode*)pCondCol)->colId;
          if (condColId != 0 && pScanColNode->colId == condColId) { found = true; break; }
        }
      }
    }

    // Also check pProjections to avoid adding a duplicate when the same column
    // already appears in the SELECT list (handles the case where pScanCols has
    // slot-id-only refs that fail name comparison above).
    if (!found && pProjLogic != NULL) {
      SNode* pProjCol = NULL;
      FOREACH(pProjCol, pProjLogic->pProjections) {
        const SNode* pInner = pProjCol;
        if (nodeType(pInner) == QUERY_NODE_TARGET)
          pInner = ((const STargetNode*)pInner)->pExpr;
        if (pInner != NULL && nodeType(pInner) == QUERY_NODE_COLUMN) {
          if (strcasecmp(((const SColumnNode*)pInner)->colName, condColName) == 0) {
            found = true; break;
          }
        }
      }
    }
    if (found) continue;

    // Column is only in the local filter  -  add it to pScanCols.
    SNode* pClone1 = NULL;
    code = nodesCloneNode(pCondCol, &pClone1);
    if (TSDB_CODE_SUCCESS != code) goto _done;
    code = nodesListMakeStrictAppend(&pScan->pScanCols, pClone1);
    if (TSDB_CODE_SUCCESS != code) goto _done;

    // Also add to pRemoteLogicPlan->pTargets so that pLeaf->pScanCols in
    // planPhysiCreater.c (which uses pTargets when pRemoteLogicPlan is set)
    // also includes the column.
    if (pTopRemote != NULL) {
      SNode* pClone2 = NULL;
      code = nodesCloneNode(pCondCol, &pClone2);
      if (TSDB_CODE_SUCCESS != code) goto _done;
      code = nodesListMakeStrictAppend(&pTopRemote->pTargets, pClone2);
      if (TSDB_CODE_SUCCESS != code) goto _done;
    }

    // If a Project node is in the chain, add the column to its pProjections
    // so that assembleRemoteSQL emits it in the SELECT clause.
    // Without this, the SELECT list is built from pProjections = {id} only,
    // so the WHERE-only column (e.g. data) would not be fetched by the
    // external DB, making local LIKE evaluation impossible.
    if (pProjLogic != NULL) {
      SNode* pClone3 = NULL;
      code = nodesCloneNode(pCondCol, &pClone3);
      if (TSDB_CODE_SUCCESS != code) goto _done;
      code = nodesListMakeStrictAppend(&pProjLogic->pProjections, pClone3);
      if (TSDB_CODE_SUCCESS != code) goto _done;
    }
  }

_done:
  nodesDestroyList(pCondCols);
  return code;
}

static int32_t fqAppendUniqueScanColFromExpr(SScanLogicNode* pScan, SNode* pExpr) {
  SNodeList* pCols = NULL;
  int32_t    code = nodesCollectColumnsFromNode(pExpr, NULL, COLLECT_COL_TYPE_ALL, &pCols);
  if (TSDB_CODE_SUCCESS != code || NULL == pCols) {
    nodesDestroyList(pCols);
    return code;
  }

  SNode* pColNode = NULL;
  FOREACH(pColNode, pCols) {
    if (QUERY_NODE_COLUMN != nodeType(pColNode)) {
      continue;
    }

    SColumnNode* pNeedCol = (SColumnNode*)pColNode;
    bool found = false;
    SNode* pScanCol = NULL;
    FOREACH(pScanCol, pScan->pScanCols) {
      if (QUERY_NODE_COLUMN != nodeType(pScanCol)) {
        continue;
      }
      SColumnNode* pExistCol = (SColumnNode*)pScanCol;
      if ((pExistCol->colId != 0 && pNeedCol->colId != 0 && pExistCol->colId == pNeedCol->colId) ||
          (0 == strcmp(pExistCol->colName, pNeedCol->colName) &&
           0 == strcmp(pExistCol->tableAlias, pNeedCol->tableAlias))) {
        found = true;
        break;
      }
    }

    if (found) {
      continue;
    }

    SNode* pClone = NULL;
    code = nodesCloneNode(pColNode, &pClone);
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
    code = nodesListMakeStrictAppend(&pScan->pScanCols, pClone);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode(pClone);
      break;
    }
    planDebug("FQ-partialProject: append local dependency col=%s tableAlias=%s colId=%d into scanCols",
              pNeedCol->colName, pNeedCol->tableAlias, (int)pNeedCol->colId);
  }

  nodesDestroyList(pCols);
  return code;
}

static bool fqColumnRefMatches(const SColumnNode* pExistCol, const SColumnNode* pNeedCol) {
  if (NULL == pExistCol || NULL == pNeedCol) {
    return false;
  }

  return (pExistCol->colId != 0 && pNeedCol->colId != 0 && pExistCol->colId == pNeedCol->colId) ||
         (0 == strcmp(pExistCol->colName, pNeedCol->colName) &&
          0 == strcmp(pExistCol->tableAlias, pNeedCol->tableAlias));
}

static bool fqProjectHasColumnExpr(const SNodeList* pExprs, const SColumnNode* pNeedCol) {
  if (NULL == pExprs || NULL == pNeedCol) {
    return false;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pExprs) {
    const SNode* pInner = pNode;
    if (QUERY_NODE_TARGET == nodeType(pInner)) {
      pInner = ((const STargetNode*)pInner)->pExpr;
    }
    if (NULL == pInner || QUERY_NODE_COLUMN != nodeType(pInner)) {
      continue;
    }

    const SColumnNode* pExistCol = (const SColumnNode*)pInner;
    if (fqColumnRefMatches(pExistCol, pNeedCol)) {
      return true;
    }
  }

  return false;
}

static int32_t fqAppendRemoteProjectDeps(SProjectLogicNode* pRemoteProj, SNode* pExpr) {
  SNodeList* pCols = NULL;
  int32_t    code = nodesCollectColumnsFromNode(pExpr, NULL, COLLECT_COL_TYPE_ALL, &pCols);
  if (TSDB_CODE_SUCCESS != code || NULL == pCols) {
    nodesDestroyList(pCols);
    return code;
  }

  SNode* pColNode = NULL;
  FOREACH(pColNode, pCols) {
    if (QUERY_NODE_COLUMN != nodeType(pColNode)) {
      continue;
    }

    SColumnNode* pNeedCol = (SColumnNode*)pColNode;
    if (fqProjectHasColumnExpr(pRemoteProj->pProjections, pNeedCol)) {
      continue;
    }

    SNode* pProjClone = NULL;
    code = nodesCloneNode(pColNode, &pProjClone);
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
    code = nodesListMakeStrictAppend(&pRemoteProj->pProjections, pProjClone);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode(pProjClone);
      break;
    }

    SNode* pTargetClone = NULL;
    code = nodesCloneNode(pColNode, &pTargetClone);
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
    code = nodesListMakeStrictAppend(&pRemoteProj->node.pTargets, pTargetClone);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode(pTargetClone);
      break;
    }

    planDebug("FQ-partialProject: append remote dependency col=%s tableAlias=%s colId=%d",
              pNeedCol->colName, pNeedCol->tableAlias, (int)pNeedCol->colId);
  }

  nodesDestroyList(pCols);
  return code;
}

static int32_t fqAppendRemoteProjectScanCols(SProjectLogicNode* pRemoteProj, const SNodeList* pScanCols) {
  if (NULL == pRemoteProj || NULL == pScanCols) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pScanCol = NULL;
  FOREACH(pScanCol, pScanCols) {
    if (QUERY_NODE_COLUMN != nodeType(pScanCol)) {
      continue;
    }

    if (fqProjectHasColumnExpr(pRemoteProj->pProjections, (const SColumnNode*)pScanCol)) {
      continue;
    }

    SNode* pProjClone = NULL;
    code = nodesCloneNode(pScanCol, &pProjClone);
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
    code = nodesListMakeStrictAppend(&pRemoteProj->pProjections, pProjClone);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode(pProjClone);
      break;
    }

    SNode* pTargetClone = NULL;
    code = nodesCloneNode(pScanCol, &pTargetClone);
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
    code = nodesListMakeStrictAppend(&pRemoteProj->node.pTargets, pTargetClone);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode(pTargetClone);
      break;
    }

    planDebug("FQ-partialProject: append scan col to remote project col=%s tableAlias=%s colId=%d",
              ((SColumnNode*)pScanCol)->colName, ((SColumnNode*)pScanCol)->tableAlias,
              (int)((SColumnNode*)pScanCol)->colId);
  }

  return code;
}

static int32_t fqBuildPartialRemoteProject(SScanLogicNode* pScan, SProjectLogicNode* pProj,
                                           const SExtSourceCapability* pCap,
                                           const SExtCapSessionOverlay* pOverlay,
                                           const SPlanContext* pPlanCxt) {
  if (NULL == pScan || NULL == pProj || NULL == pProj->pProjections) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  int32_t pushableCnt = 0;
  int32_t localCnt = 0;
  if (!fqProjectCanPartiallyPushdown(pProj, pScan, pCap, pOverlay, pPlanCxt, &pushableCnt, &localCnt)) {
    planDebug("FQ-partialProject: skip build, project=%p pushableCnt=%d localCnt=%d",
              pProj, pushableCnt, localCnt);
    return TSDB_CODE_SUCCESS;
  }

  fqPlanDebugLogNodeOutputs("partialProject.local.project", (SLogicNode*)pProj);
  fqPlanDebugLogNodeOutputs("partialProject.local.scan", (SLogicNode*)pScan);

  SProjectLogicNode* pRemoteProj = NULL;
  code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_PROJECT, (SNode**)&pRemoteProj);
  if (TSDB_CODE_SUCCESS != code || NULL == pRemoteProj) {
    planError("FQ-partialProject: create remote project failed, code=0x%x", code);
    return (TSDB_CODE_SUCCESS == code) ? terrno : code;
  }

  pRemoteProj->ignoreGroupId = true;
  pRemoteProj->inputIgnoreGroup = pProj->inputIgnoreGroup;
  pRemoteProj->node.precision = pProj->node.precision;
  pRemoteProj->node.stmtRoot = pProj->node.stmtRoot;

  int32_t idx = 0;
  SNode* pExpr = NULL;
  FOREACH(pExpr, pProj->pProjections) {
    SNode* pInner = pExpr;
    if (QUERY_NODE_TARGET == nodeType(pInner)) {
      pInner = ((STargetNode*)pInner)->pExpr;
    }
    if (NULL == pInner) {
      ++idx;
      continue;
    }

    bool pushable = fqExprIsPushable(pInner, pScan, pCap, pOverlay, pPlanCxt);
    bool partialPushable = pushable && QUERY_NODE_COLUMN == nodeType(pInner);
    if (partialPushable) {
      SNode* pRemoteExpr = NULL;
      code = nodesCloneNode(pInner, &pRemoteExpr);
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
      code = nodesListMakeStrictAppend(&pRemoteProj->pProjections, pRemoteExpr);
      if (TSDB_CODE_SUCCESS != code) {
        nodesDestroyNode(pRemoteExpr);
        break;
      }

      SNode* pRemoteTargetExpr = NULL;
      code = nodesCloneNode(pInner, &pRemoteTargetExpr);
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
      code = nodesListMakeStrictAppend(&pRemoteProj->node.pTargets, pRemoteTargetExpr);
      if (TSDB_CODE_SUCCESS != code) {
        nodesDestroyNode(pRemoteTargetExpr);
        break;
      }

      SColumnNode* pRewriteCol = NULL;
      code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pRewriteCol);
      if (TSDB_CODE_SUCCESS != code || NULL == pRewriteCol) {
        code = (TSDB_CODE_SUCCESS == code) ? terrno : code;
        break;
      }
      pRewriteCol->node.resType = ((SExprNode*)pInner)->resType;
      tstrncpy(pRewriteCol->colName, ((SExprNode*)pInner)->aliasName, TSDB_COL_NAME_LEN);
      tstrncpy(pRewriteCol->node.aliasName, ((SExprNode*)pInner)->aliasName, TSDB_COL_NAME_LEN);
      tstrncpy(pRewriteCol->node.userAlias, ((SExprNode*)pInner)->userAlias, TSDB_COL_NAME_LEN);
      nodesDestroyNode(pExpr);
      cell->pNode = (SNode*)pRewriteCol;
      planDebug("FQ-partialProject: expr[%d] pushed remote alias=%s userAlias=%s",
                idx, pRewriteCol->colName, pRewriteCol->node.userAlias);
    } else {
      code = fqAppendUniqueScanColFromExpr(pScan, pInner);
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
      code = fqAppendRemoteProjectDeps(pRemoteProj, pInner);
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
      planDebug("FQ-partialProject: expr[%d] stays local nodeType=%d",
                idx, (int)nodeType(pInner));
    }
    ++idx;
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = fqAppendRemoteProjectScanCols(pRemoteProj, pScan->pScanCols);
  }

  if (TSDB_CODE_SUCCESS == code) {
    if (pScan->pRemoteLogicPlan != NULL) {
      code = nodesMakeList(&pRemoteProj->node.pChildren);
      if (TSDB_CODE_SUCCESS == code) {
        code = nodesListStrictAppend(pRemoteProj->node.pChildren, pScan->pRemoteLogicPlan);
      }
      if (TSDB_CODE_SUCCESS == code) {
        ((SLogicNode*)pScan->pRemoteLogicPlan)->pParent = (SLogicNode*)pRemoteProj;
        pScan->pRemoteLogicPlan = (SNode*)pRemoteProj;
      }
    } else {
      pScan->pRemoteLogicPlan = (SNode*)pRemoteProj;
    }
  }

  if (TSDB_CODE_SUCCESS != code) {
    planError("FQ-partialProject: build failed project=%p code=0x%x", pProj, code);
    if ((SNode*)pRemoteProj != pScan->pRemoteLogicPlan) {
      nodesDestroyNode((SNode*)pRemoteProj);
    }
    return code;
  }

  code = fqAlignRemoteRootTargetsForRender((SLogicNode*)pScan->pRemoteLogicPlan);
  if (TSDB_CODE_SUCCESS != code) {
    planError("FQ-partialProject: align remote root targets failed, code=0x%x", code);
    return code;
  }

  fqPlanDebugLogNodeOutputs("partialProject.remote.afterAlign", (SLogicNode*)pScan->pRemoteLogicPlan);

  code = fqEnsureLocalFilterColsInScanCols(pScan);
  if (TSDB_CODE_SUCCESS != code) {
    planError("FQ-partialProject: ensure local filter cols failed, code=0x%x", code);
    return code;
  }

  SNodeList* pScanTargets = NULL;
  code = nodesCloneList(((SLogicNode*)pScan->pRemoteLogicPlan)->pTargets, &pScanTargets);
  if (TSDB_CODE_SUCCESS != code) {
    planError("FQ-partialProject: clone remote root targets for scan failed, code=0x%x", code);
    return code;
  }
  nodesDestroyList(pScan->node.pTargets);
  pScan->node.pTargets = pScanTargets;
  planDebug("FQ-partialProject: scan targets cloned from remote root targets=%d",
            pScanTargets ? LIST_LENGTH(pScanTargets) : 0);

  planDebug("FQ-partialProject: done scan=%p project=%p remoteRootType=%d pushableCnt=%d localCnt=%d scanCols=%d remoteTargets=%d",
            pScan, pProj, pScan->pRemoteLogicPlan ? (int)nodeType(pScan->pRemoteLogicPlan) : -1,
            pushableCnt, localCnt,
            pScan->pScanCols ? LIST_LENGTH(pScan->pScanCols) : 0,
            (pScan->pRemoteLogicPlan && ((SLogicNode*)pScan->pRemoteLogicPlan)->pTargets)
                ? LIST_LENGTH(((SLogicNode*)pScan->pRemoteLogicPlan)->pTargets)
                : 0);
  return TSDB_CODE_SUCCESS;
}


// Convert TDengine PARTITION BY semantics into standard SQL GROUP BY for remote.
//
// For InfluxDB: when the query is `SELECT agg(...) FROM t PARTITION BY TBNAME`,
// TDengine rewrites TBNAME into a partition key SFunctionNode.  The physical
// plan's PARTITION operator resolves its partition key slot via a cast from
// SFunctionNode  ->  SColumnNode, reading an uninitialized (zero) slotId.  To
// make the zero-init slotId point to a meaningful column, we prepend all
// isTag=true columns from the external metadata to pScanCols so that the
// first slot (slot 0) is a tag column.  The PARTITION operator then groups by
// that tag and the downstream AGG computes the aggregate per group.
//
// For MySQL / PostgreSQL: PARTITION BY TBNAME has no sensible mapping (those
// sources are not multi-measurement and have no "tbname" concept), so we
// reject the query with TSDB_CODE_EXT_SYNTAX_UNSUPPORTED.

// Helper: find a PARTITION BY TBNAME logic node in the given logic subtree.
static SPartitionLogicNode* fqFindPartitionTbname(SLogicNode* pNode) {
  if (pNode == NULL) return NULL;
  if (nodeType(pNode) == QUERY_NODE_LOGIC_PLAN_PARTITION) {
    SPartitionLogicNode* pPart = (SPartitionLogicNode*)pNode;
    SNode* pKey = NULL;
    FOREACH(pKey, pPart->pPartitionKeys) {
      if (nodeType(pKey) == QUERY_NODE_FUNCTION &&
          ((SFunctionNode*)pKey)->funcType == FUNCTION_TYPE_TBNAME)
        return pPart;
      if (nodeType(pKey) == QUERY_NODE_COLUMN &&
          ((SColumnNode*)pKey)->colType == COLUMN_TYPE_TBNAME)
        return pPart;
    }
  }
  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    SPartitionLogicNode* pFound = fqFindPartitionTbname((SLogicNode*)pChild);
    if (pFound != NULL) return pFound;
  }
  return NULL;
}

// NOTE: SESSION/STATE/EVENT/COUNT window operators above an ExternalScan are
// NOT pushed to remote SQL and remain in the main plan for local execution.
// INTERVAL windows are handled separately by fqConvertInterval (below), which
// converts them to a remote GROUP BY time-bucket query.
// INTERP windows are handled by the dedicated `FqInterp` rule.

// ---------------------------------------------------------------------------
// fqConvertInterval — push INTERVAL window down as remote GROUP BY
// ---------------------------------------------------------------------------
// For a query like:
//   SELECT _wstart, COUNT(*) FROM ext.sensor INTERVAL(1m)
//
// TDengine creates: Scan -> Window(INTERVAL) -> Agg(count) -> Project
//
// This function converts the INTERVAL+Agg pair into a remote AGG node with:
//   GROUP BY  FUNCTION_TYPE_FQ_TIME_BUCKET(ts_col, bucket_ms)
//   aggFuncs  cloned from Window->pFuncs
//
// nodesRemotePlanToSQL then emits per-dialect:
//   MySQL:    SELECT COUNT(*) ... GROUP BY FROM_UNIXTIME(FLOOR(...))
//   PG:       SELECT COUNT(*) ... GROUP BY to_timestamp(FLOOR(...))
//   InfluxDB: SELECT COUNT(*) ... GROUP BY time(60000ms)
//
// Constraints (any violation -> skip, leave INTERVAL local):
//   1. winType == WINDOW_TYPE_INTERVAL
//   2. pWindow->sliding == 0 (no SLIDING — non-tumbling stays local)
//   3. No PARTITION BY between Scan and Window (handled by fqConvertPartition;
//      two-stage partition+interval stays local)
//   4. All aggregate functions in pFuncs are pushable (fqFuncTypeIsPushable)
//   5. Source capability has EXT_CLAUSE_AGG and EXT_CLAUSE_GROUP_BY
// ---------------------------------------------------------------------------
// fqCloneScanAsLeaf — clone a SCAN logic node into a pure remote-plan leaf
// (no children, no parent, no nested remote plan).  Carries FROM/pScanCols/
// pPushedConditions for SQL rendering.  Shared by commit and JOIN builder.
static int32_t fqCloneScanAsLeaf(SScanLogicNode* pSrc, SScanLogicNode** ppLeaf) {
  SScanLogicNode* pLeaf = NULL;
  int32_t code = nodesCloneNode((SNode*)pSrc, (SNode**)&pLeaf);
  if (TSDB_CODE_SUCCESS != code) return code;
  if (pLeaf->node.pChildren != NULL) {
    nodesDestroyList(pLeaf->node.pChildren);
    pLeaf->node.pChildren = NULL;
  }
  pLeaf->node.pParent = NULL;
  if (pLeaf->pRemoteLogicPlan != NULL) {
    nodesDestroyNode(pLeaf->pRemoteLogicPlan);
    pLeaf->pRemoteLogicPlan = NULL;
  }
  planDebug("FQ-cloneLeaf: src=%p table=%s hasCond=%d scanCols=%d targets=%d",
            pSrc,
            pSrc->tableName.tname,
            NULL != pSrc->node.pConditions,
            pSrc->pScanCols ? LIST_LENGTH(pSrc->pScanCols) : 0,
            pSrc->node.pTargets ? LIST_LENGTH(pSrc->node.pTargets) : 0);
  *ppLeaf = pLeaf;
  return TSDB_CODE_SUCCESS;
}

static int32_t fqBuildRemoteSubtree(SLogicNode* pNode, SScanLogicNode* pScanForMeta,
                                    SLogicNode** ppRemote);
static int32_t fqRewriteRemoteTbnameTags(SLogicNode* pRemoteNode, SNode* pRemoteBase,
                                         const SExtTableMeta* pMeta);

static int32_t fqHarvestParentJoinWhereForRemoteSubtree(SLogicNode* pBoundaryRoot, SLogicNode* pRemoteSubtree,
                                                        const SExtSourceCapability* pCap,
                                                        const SExtCapSessionOverlay* pOverlay,
                                                        const SPlanContext* pPlanCxt) {
  if (NULL == pBoundaryRoot || NULL == pRemoteSubtree || NULL == pBoundaryRoot->pParent ||
      QUERY_NODE_LOGIC_PLAN_JOIN != nodeType((SNode*)pBoundaryRoot->pParent)) {
    return TSDB_CODE_SUCCESS;
  }

  SJoinLogicNode* pJoin = (SJoinLogicNode*)pBoundaryRoot->pParent;
  if (NULL == pJoin->node.pConditions) {
    planDebug("FQ-joinWhereHarvest: boundary=%p parentJoin=%p has no join-level WHERE",
              pBoundaryRoot, pJoin);
    return TSDB_CODE_SUCCESS;
  }

  SLogicNode* pDirectChild = pBoundaryRoot;
  while (NULL != pDirectChild->pParent && pDirectChild->pParent != (SLogicNode*)pJoin) {
    pDirectChild = pDirectChild->pParent;
  }
  if (NULL == pDirectChild->pParent) {
    planDebug("FQ-joinWhereHarvest: boundary=%p is not under expected parentJoin=%p",
              pBoundaryRoot, pJoin);
    return TSDB_CODE_SUCCESS;
  }

  SLogicNode* pLeft = (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0);
  SLogicNode* pRight = (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1);
  bool isLeftLeg = (pDirectChild == pLeft);
  bool isRightLeg = (pDirectChild == pRight);
  if (!isLeftLeg && !isRightLeg) {
    planError("FQ-joinWhereHarvest: direct child=%p not found in parentJoin=%p",
              pDirectChild, pJoin);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  SScanLogicNode* pBoundaryScan = fqFindExternalLogicLeafScan(pBoundaryRoot);
  if (NULL == pBoundaryScan) {
    planDebug("FQ-joinWhereHarvest: boundary=%p parentJoin=%p has no external leaf scan",
              pBoundaryRoot, pJoin);
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  SNode* pJoinWhere = NULL;
  SNode* pOnCond = NULL;
  SNode* pLeftChildCond = NULL;
  SNode* pRightChildCond = NULL;
  code = nodesCloneNode(pJoin->node.pConditions, &pJoinWhere);
  if (TSDB_CODE_SUCCESS != code) {
    planError("FQ-joinWhereHarvest: clone join-level WHERE failed join=%p boundary=%p code=0x%x",
              pJoin, pBoundaryRoot, code);
    return code;
  }

  code = pdcJoinSplitCond(pJoin, &pJoinWhere, &pOnCond, &pLeftChildCond, &pRightChildCond, true);
  if (TSDB_CODE_SUCCESS != code) {
    planError("FQ-joinWhereHarvest: split join-level WHERE failed join=%p boundary=%p code=0x%x",
              pJoin, pBoundaryRoot, code);
    nodesDestroyNode(pJoinWhere);
    nodesDestroyNode(pOnCond);
    nodesDestroyNode(pLeftChildCond);
    nodesDestroyNode(pRightChildCond);
    return code;
  }

  SNode** ppChosen = isLeftLeg ? &pLeftChildCond : &pRightChildCond;
  SNode* pChosen = *ppChosen;
  planDebug("FQ-joinWhereHarvest: parentJoin=%p boundary=%p directChild=%p leg=%s chosen=%d other=%d remain=%d on=%d remoteRootType=%d",
            pJoin, pBoundaryRoot, pDirectChild, isLeftLeg ? "left" : "right",
            NULL != pChosen, isLeftLeg ? (NULL != pRightChildCond) : (NULL != pLeftChildCond),
            NULL != pJoinWhere, NULL != pOnCond, (int)nodeType((SNode*)pRemoteSubtree));

  if (NULL != pChosen) {
    bool pushable = fqExprIsPushable(pChosen, pBoundaryScan, pCap, pOverlay, pPlanCxt);
    planDebug("FQ-joinWhereHarvest: parentJoin=%p boundary=%p leg=%s chosenPushable=%d remoteRootHasCondBefore=%d",
              pJoin, pBoundaryRoot, isLeftLeg ? "left" : "right", (int)pushable,
              NULL != pRemoteSubtree->pConditions);
    if (pushable) {
      code = nodesMergeNode(&pRemoteSubtree->pConditions, ppChosen);
      if (TSDB_CODE_SUCCESS != code) {
        planError("FQ-joinWhereHarvest: merge chosen WHERE into remote subtree failed join=%p boundary=%p code=0x%x",
                  pJoin, pBoundaryRoot, code);
      } else {
        planDebug("FQ-joinWhereHarvest: merged chosen WHERE into remote subtree parentJoin=%p boundary=%p remoteRootHasCondAfter=%d",
                  pJoin, pBoundaryRoot, NULL != pRemoteSubtree->pConditions);
      }
    } else {
      planDebug("FQ-joinWhereHarvest: chosen WHERE stays local parentJoin=%p boundary=%p",
                pJoin, pBoundaryRoot);
    }
  }

  nodesDestroyNode(pJoinWhere);
  nodesDestroyNode(pOnCond);
  nodesDestroyNode(pLeftChildCond);
  nodesDestroyNode(pRightChildCond);
  return code;
}

// fqBuildRemoteJoin — pure builder: constructs the remote JOIN node (joinType,
// subType, ON conditions) with recursively-built remote children, returned via
// *ppJoin.  NO main-tree surgery.  Child legs may be bare external scans or a
// previously-built remote subtree hanging off a docking scan (nested join push).
static int32_t fqBuildRemoteJoin(SJoinLogicNode* pJoin, SScanLogicNode* pScanForMeta,
                                 SJoinLogicNode** ppJoin) {
  *ppJoin = NULL;
  SLogicNode* pLeft  = (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0);
  SLogicNode* pRight = (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1);
  planDebug("FQ-buildRemoteJoin: joinType=%d subType=%d leftType=%d rightType=%d",
            (int)pJoin->joinType, (int)pJoin->subType,
            pLeft ? (int)nodeType((SNode*)pLeft) : -1,
            pRight ? (int)nodeType((SNode*)pRight) : -1);

  SJoinLogicNode* pRemoteJoin = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_JOIN, (SNode**)&pRemoteJoin);
  if (TSDB_CODE_SUCCESS != code) return code;
  pRemoteJoin->joinType = pJoin->joinType;
  pRemoteJoin->subType  = pJoin->subType;

  // ON clause: combine pPrimKeyEqCond + pFullOnCond into pRemoteJoin->pFullOnCond.
  if (pJoin->pPrimKeyEqCond != NULL && pJoin->pFullOnCond != NULL) {
    SLogicConditionNode* pAndCond = NULL;
    code = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, (SNode**)&pAndCond);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pRemoteJoin); return code; }
    pAndCond->condType = LOGIC_COND_TYPE_AND;
    code = nodesMakeList(&pAndCond->pParameterList);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pRemoteJoin); nodesDestroyNode((SNode*)pAndCond); return code; }
    SNode* pPrimClone = NULL; SNode* pFullClone = NULL;
    code = nodesCloneNode(pJoin->pPrimKeyEqCond, &pPrimClone);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pRemoteJoin); nodesDestroyNode((SNode*)pAndCond); return code; }
    code = nodesCloneNode(pJoin->pFullOnCond, &pFullClone);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pRemoteJoin); nodesDestroyNode((SNode*)pAndCond); nodesDestroyNode(pPrimClone); return code; }
    (void)nodesListStrictAppend(pAndCond->pParameterList, pPrimClone);
    (void)nodesListStrictAppend(pAndCond->pParameterList, pFullClone);
    pRemoteJoin->pFullOnCond = (SNode*)pAndCond;
  } else if (pJoin->pPrimKeyEqCond != NULL) {
    code = nodesCloneNode(pJoin->pPrimKeyEqCond, &pRemoteJoin->pFullOnCond);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pRemoteJoin); return code; }
  } else if (pJoin->pFullOnCond != NULL) {
    code = nodesCloneNode(pJoin->pFullOnCond, &pRemoteJoin->pFullOnCond);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pRemoteJoin); return code; }
  }
  if (pJoin->pColEqCond != NULL) {
    code = nodesCloneNode(pJoin->pColEqCond, &pRemoteJoin->pColEqCond);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pRemoteJoin); return code; }
  }
  if (pJoin->pColOnCond != NULL) {
    code = nodesCloneNode(pJoin->pColOnCond, &pRemoteJoin->pColOnCond);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pRemoteJoin); return code; }
  }
  if (pJoin->node.pConditions != NULL) {
    code = nodesCloneNode(pJoin->node.pConditions, &pRemoteJoin->node.pConditions);
    if (TSDB_CODE_SUCCESS != code) {
      planError("FQ-buildRemoteJoin: clone join WHERE failed join=%p code=0x%x", pJoin, code);
      nodesDestroyNode((SNode*)pRemoteJoin);
      return code;
    }
    planDebug("FQ-buildRemoteJoin: cloned join WHERE join=%p condType=%d",
              pJoin, (int)nodeType(pJoin->node.pConditions));
  } else {
    planDebug("FQ-buildRemoteJoin: join=%p has no join-level WHERE", pJoin);
  }

  SLogicNode* pLeftRemote = NULL;
  SLogicNode* pRightRemote = NULL;
  code = fqBuildRemoteSubtree(pLeft, pScanForMeta, &pLeftRemote);
  if (TSDB_CODE_SUCCESS != code) {
    planError("FQ-buildRemoteJoin: build left child failed join=%p code=0x%x childType=%d",
              pJoin, code, pLeft ? (int)nodeType((SNode*)pLeft) : -1);
    nodesDestroyNode((SNode*)pRemoteJoin);
    return code;
  }
  code = fqBuildRemoteSubtree(pRight, pScanForMeta, &pRightRemote);
  if (TSDB_CODE_SUCCESS != code) {
    planError("FQ-buildRemoteJoin: build right child failed join=%p code=0x%x childType=%d",
              pJoin, code, pRight ? (int)nodeType((SNode*)pRight) : -1);
    nodesDestroyNode((SNode*)pRemoteJoin);
    nodesDestroyNode((SNode*)pLeftRemote);
    return code;
  }

  code = nodesMakeList(&pRemoteJoin->node.pChildren);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pRemoteJoin);
    nodesDestroyNode((SNode*)pLeftRemote);
    nodesDestroyNode((SNode*)pRightRemote);
    return code;
  }
  code = nodesListStrictAppend(pRemoteJoin->node.pChildren, (SNode*)pLeftRemote);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pRemoteJoin);
    nodesDestroyNode((SNode*)pLeftRemote);
    nodesDestroyNode((SNode*)pRightRemote);
    return code;
  }
  code = nodesListStrictAppend(pRemoteJoin->node.pChildren, (SNode*)pRightRemote);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pRemoteJoin);
    nodesDestroyNode((SNode*)pRightRemote);
    return code;
  }
  pLeftRemote->pParent  = (SLogicNode*)pRemoteJoin;
  pRightRemote->pParent = (SLogicNode*)pRemoteJoin;

  // Output schema = JOIN's targets (both legs' columns).
  if (pJoin->node.pTargets != NULL) {
    code = nodesCloneList(pJoin->node.pTargets, &pRemoteJoin->node.pTargets);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pRemoteJoin); return code; }
  }
  *ppJoin = pRemoteJoin;
  planDebug("FQ-buildRemoteJoin: built remote JOIN with recursive children leftRemoteType=%d rightRemoteType=%d hasJoinWhere=%d hasFullOn=%d hasColEq=%d hasColOn=%d",
            (int)nodeType((SNode*)pLeftRemote), (int)nodeType((SNode*)pRightRemote),
            NULL != pRemoteJoin->node.pConditions, NULL != pRemoteJoin->pFullOnCond,
            NULL != pRemoteJoin->pColEqCond, NULL != pRemoteJoin->pColOnCond);
  return TSDB_CODE_SUCCESS;
}

// fqBuildIntervalAgg — pure builder: constructs the remote GROUP BY time_bucket
// AGG node for an INTERVAL window and returns it via *ppAgg.  NO main-tree
// surgery, NO mutation of pScan fields.  The caller (three-step commit) wires
// the returned AGG into the remote logic plan and performs the main-tree
// relocation generically.  Assumes preconditions (ts pk present, interval>0)
// already checked by fqWindowIntervalIsPushable.
static int32_t fqBuildIntervalAgg(SScanLogicNode* pScan, SWindowLogicNode* pWin,
                                  SAggLogicNode** ppAgg) {
  *ppAgg = NULL;
  SExtTableNode* pExtNode = (SExtTableNode*)pScan->pExtTableNode;
  const char* pkColName = pExtNode->pExtMeta->pCols[pExtNode->tsPrimaryColIdx].colName;
  int8_t srcPrecision = pExtNode->pExtMeta->tsPrecision;
  if (srcPrecision == 0) srcPrecision = TSDB_TIME_PRECISION_MICRO;
  int64_t intervalTicks = pWin->interval;
  int64_t ticksPerSec = TSDB_TICK_PER_SECOND(srcPrecision);
  (void)ticksPerSec;
  planDebug("FQ-buildIntervalAgg: pk=%s interval=%" PRId64 " ticks prec=%d nWinFuncs=%d",
            pkColName, intervalTicks, (int)srcPrecision,
            pWin->pFuncs ? (int)LIST_LENGTH(pWin->pFuncs) : 0);
  int32_t code = TSDB_CODE_SUCCESS;

  // ── 7. Build FUNCTION_TYPE_FQ_TIME_BUCKET node ──
  // pParameterList[0] = SColumnNode (ts pk)
  // pParameterList[1] = SValueNode  (bucket_ticks, int64 — ticks in srcPrecision)
  // pParameterList[2] = SValueNode  (precision, int8  — TSDB_TIME_PRECISION_*)
  SFunctionNode* pBucketFunc = NULL;
  code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pBucketFunc);
  if (TSDB_CODE_SUCCESS != code) return code;
  pBucketFunc->funcType = FUNCTION_TYPE_FQ_TIME_BUCKET;
  // funcId 0 is safe here: this is an internal FQ node that never goes through
  // the normal function registry.  nodesRemotePlanToSQL dispatches on funcType.
  pBucketFunc->funcId   = 0;
  tstrncpy(pBucketFunc->functionName, "_fq_time_bucket", TSDB_FUNC_NAME_LEN);
  // Set aliasName to the WSTART pseudo-column's INTERNAL slot name so the remote
  // SELECT emits "FLOOR(...) AS `expr_N`" — the enclosing outer query references
  // the bucket column by that slot name (its colName), not the user-facing
  // "_wstart".  Prefer the WSTART func's aliasName (slot name); fall back to its
  // userAlias, then to "_wstart".  (Consistent with fqSelectAlias: remote-SELECT
  // output columns are always aliased by internal slot name.)
  {
    const char* wstartAlias = "_wstart";
    if (pWin->pFuncs) {
      SNode* pWF = NULL;
      FOREACH(pWF, pWin->pFuncs) {
        if (nodeType(pWF) != QUERY_NODE_FUNCTION) continue;
        const SFunctionNode* pF2 = (const SFunctionNode*)pWF;
        if (fmIsPlaceHolderFuncForExternalWin(pF2->funcId) && pF2->funcType == FUNCTION_TYPE_WSTART) {
          const char* aa = pF2->node.aliasName;
          const char* ua = pF2->node.userAlias;
          if (aa && aa[0] != '\0') { wstartAlias = aa; break; }
          if (ua && ua[0] != '\0') { wstartAlias = ua; break; }
        }
      }
  }
  tstrncpy(pBucketFunc->node.aliasName, wstartAlias, TSDB_COL_NAME_LEN);
  }
  pBucketFunc->node.resType.type  = TSDB_DATA_TYPE_TIMESTAMP;
  pBucketFunc->node.resType.bytes = (int32_t)sizeof(int64_t);

  code = nodesMakeList(&pBucketFunc->pParameterList);
  if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pBucketFunc); return code; }

  // param[0]: ts column node
  SColumnNode* pTsCol = NULL;
  code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pTsCol);
  if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pBucketFunc); return code; }
  tstrncpy(pTsCol->colName, pkColName, TSDB_COL_NAME_LEN);
  pTsCol->node.resType.type  = TSDB_DATA_TYPE_TIMESTAMP;
  pTsCol->node.resType.bytes = (int32_t)sizeof(int64_t);

  code = nodesListStrictAppend(pBucketFunc->pParameterList, (SNode*)pTsCol);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pBucketFunc);
    return code;
  }

  // param[1]: bucket size in ticks (unit = srcPrecision)
  SValueNode* pMsVal = NULL;
  code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pMsVal);
  if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pBucketFunc); return code; }
  pMsVal->node.resType.type  = TSDB_DATA_TYPE_BIGINT;
  pMsVal->node.resType.bytes = (int32_t)sizeof(int64_t);
  pMsVal->datum.i   = intervalTicks;
  pMsVal->typeData  = TSDB_DATA_TYPE_BIGINT;
  pMsVal->translate = true;  // mark datum as valid so serialization/clone preserves the value

  code = nodesListStrictAppend(pBucketFunc->pParameterList, (SNode*)pMsVal);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pBucketFunc);
    return code;
  }

  // param[2]: source timestamp precision (TSDB_TIME_PRECISION_MILLI/MICRO/NANO)
  // Carried through the plan so nodesRemotePlanToSQL can compute the correct
  // dialect-specific time-bucket expression without knowing the precision at
  // SQL-render time.
  SValueNode* pPrecVal = NULL;
  code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pPrecVal);
  if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pBucketFunc); return code; }
  pPrecVal->node.resType.type  = TSDB_DATA_TYPE_TINYINT;
  pPrecVal->node.resType.bytes = 1;
  pPrecVal->datum.i   = srcPrecision;
  pPrecVal->typeData  = TSDB_DATA_TYPE_TINYINT;
  pPrecVal->translate = true;

  code = nodesListStrictAppend(pBucketFunc->pParameterList, (SNode*)pPrecVal);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pBucketFunc);
    return code;
  }

  // param[3]: interval origin offset in source precision ticks. This keeps
  // INTERVAL(step, offset) bucket anchors identical after remote DATE_BIN/FLOOR
  // rendering.
  SValueNode* pOffsetVal = NULL;
  code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pOffsetVal);
  if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pBucketFunc); return code; }
  pOffsetVal->node.resType.type  = TSDB_DATA_TYPE_BIGINT;
  pOffsetVal->node.resType.bytes = (int32_t)sizeof(int64_t);
  pOffsetVal->datum.i   = pWin->offset;
  pOffsetVal->typeData  = TSDB_DATA_TYPE_BIGINT;
  pOffsetVal->translate = true;

  code = nodesListStrictAppend(pBucketFunc->pParameterList, (SNode*)pOffsetVal);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pBucketFunc);
    return code;
  }

  // ── 8. Build SAggLogicNode: GROUP BY bucket + aggregate functions ──
  SAggLogicNode* pAgg = NULL;
  code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_AGG, (SNode**)&pAgg);
  if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pBucketFunc); return code; }

  // pGroupKeys = [pBucketFunc]
  code = nodesMakeList(&pAgg->pGroupKeys);
  if (TSDB_CODE_SUCCESS != code) goto _cleanup_agg;
  code = nodesListStrictAppend(pAgg->pGroupKeys, (SNode*)pBucketFunc);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pBucketFunc);
    goto _cleanup_agg;
  }
  pBucketFunc = NULL;  // owned by pGroupKeys

  // pAggFuncs = clone of pWin->pFuncs, excluding window pseudo-column functions.
  // _wstart/_wend/_wduration are computed locally from the time-bucket result,
  // not pushed to the remote source.
  if (pWin->pFuncs != NULL && LIST_LENGTH(pWin->pFuncs) > 0) {
    SNode* pWinFunc = NULL;
    FOREACH(pWinFunc, pWin->pFuncs) {
      if (nodeType(pWinFunc) != QUERY_NODE_FUNCTION) continue;
      const SFunctionNode* pF = (const SFunctionNode*)pWinFunc;
      if (fmIsWindowPseudoColumnFunc(pF->funcId) || fmIsPseudoColumnFunc(pF->funcId)) continue;
      if (fmIsGroupKeyFunc(pF->funcId)) {
        planDebug("FQ-buildIntervalAgg: skip window group-key func name=%s alias=%s",
                  pF->functionName, pF->node.aliasName);
        continue;
      }
      SNode* pCloned = NULL;
      code = nodesCloneNode(pWinFunc, &pCloned);
      if (TSDB_CODE_SUCCESS != code) goto _cleanup_agg;
      if (pAgg->pAggFuncs == NULL) {
        code = nodesMakeList(&pAgg->pAggFuncs);
        if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode(pCloned); goto _cleanup_agg; }
      }
      code = nodesListStrictAppend(pAgg->pAggFuncs, pCloned);
      if (TSDB_CODE_SUCCESS != code) goto _cleanup_agg;
    }
  }

  // pTargets = group keys + agg funcs (order must match renderAggSelectList SQL output)
  if (pAgg->pGroupKeys != NULL) {
    code = createColumnByRewriteExprs(pAgg->pGroupKeys, &pAgg->node.pTargets);
    if (TSDB_CODE_SUCCESS != code) goto _cleanup_agg;
  }
  if (pAgg->pAggFuncs != NULL) {
    code = createColumnByRewriteExprs(pAgg->pAggFuncs, &pAgg->node.pTargets);
    if (TSDB_CODE_SUCCESS != code) goto _cleanup_agg;
  }

  if (pWin->node.pConditions != NULL) {
    code = nodesCloneNode(pWin->node.pConditions, &pAgg->node.pConditions);
    if (TSDB_CODE_SUCCESS != code) goto _cleanup_agg;
    planDebug("FQ-buildIntervalAgg: cloned window conditions into remote agg win=%p agg=%p condType=%d",
              pWin, pAgg, (int)nodeType(pWin->node.pConditions));
  } else {
    planDebug("FQ-buildIntervalAgg: window has no conditions to clone win=%p agg=%p", pWin, pAgg);
  }

  // Builder done: return the fully-constructed remote AGG.  Main-tree surgery
  // and pScan target/pseudo-col handling are performed generically by the
  // three-step commit (fqCommitPushChain), not here.
  *ppAgg = pAgg;
  planDebug("FQ-buildIntervalAgg: built remote AGG=%p pGroupKeys=%d pAggFuncs=%d hasCond=%d (time_bucket GROUP BY)",
            pAgg, pAgg->pGroupKeys ? (int)LIST_LENGTH(pAgg->pGroupKeys) : 0,
            pAgg->pAggFuncs ? (int)LIST_LENGTH(pAgg->pAggFuncs) : 0,
            pAgg->node.pConditions != NULL);
  return TSDB_CODE_SUCCESS;

_cleanup_agg:
  nodesDestroyNode((SNode*)pAgg);
  return code;
}

static SExprNode* fqGetAliasCarrierFromGroupKey(SNode* pKey) {
  SNode* pActual = pKey;
  if (NULL == pActual) {
    return NULL;
  }

  if (QUERY_NODE_GROUPING_SET == nodeType(pActual)) {
    SGroupingSetNode* pGrouping = (SGroupingSetNode*)pActual;
    pActual = nodesListGetNode(pGrouping->pParameterList, 0);
  }

  if (NULL != pActual && QUERY_NODE_FUNCTION == nodeType(pActual)) {
    SFunctionNode* pFunc = (SFunctionNode*)pActual;
    if (fmIsGroupKeyFunc(pFunc->funcId) && pFunc->pParameterList != NULL &&
        LIST_LENGTH(pFunc->pParameterList) > 0) {
      pActual = nodesListGetNode(pFunc->pParameterList, 0);
    }
  }

  return (NULL != pActual && nodesIsExprNode(pActual)) ? (SExprNode*)pActual : NULL;
}

static bool fqIsGroupKeyFuncNode(const SNode* pNode) {
  const SNode* pActual = pNode;
  if (NULL == pActual) {
    return false;
  }

  if (QUERY_NODE_TARGET == nodeType(pActual)) {
    pActual = ((const STargetNode*)pActual)->pExpr;
  }

  return (NULL != pActual && QUERY_NODE_FUNCTION == nodeType(pActual) &&
          fmIsGroupKeyFunc(((const SFunctionNode*)pActual)->funcId));
}

static bool fqAggHasPartitionGroupKey(const SAggLogicNode* pAgg, const SNode* pPartKey) {
  if (NULL == pAgg || NULL == pPartKey) {
    return false;
  }

  SExprNode* pPartExpr = fqGetAliasCarrierFromGroupKey((SNode*)pPartKey);
  if (NULL == pPartExpr) {
    return false;
  }

  bool partIsCol = (QUERY_NODE_COLUMN == nodeType((SNode*)pPartExpr));
  const SColumnNode* pPartCol = partIsCol ? (const SColumnNode*)pPartExpr : NULL;

  SNode* pAggKey = NULL;
  FOREACH(pAggKey, pAgg->pGroupKeys) {
    SExprNode* pAggExpr = fqGetAliasCarrierFromGroupKey(pAggKey);
    bool matched = (NULL != pAggExpr && nodesEqualNode((SNode*)pAggExpr, (SNode*)pPartExpr));
    if (!matched && NULL != pAggExpr && partIsCol && QUERY_NODE_COLUMN == nodeType((SNode*)pAggExpr)) {
      const SColumnNode* pAggCol = (const SColumnNode*)pAggExpr;
      matched = (0 == strcmp(pAggCol->colName, pPartCol->colName));
    }
    if (matched) {
      return true;
    }
  }

  SNode* pAggFunc = NULL;
  FOREACH(pAggFunc, pAgg->pAggFuncs) {
    if (!fqIsGroupKeyFuncNode(pAggFunc)) {
      continue;
    }

    SExprNode* pAggExpr = fqGetAliasCarrierFromGroupKey(pAggFunc);
    bool matched = (NULL != pAggExpr && nodesEqualNode((SNode*)pAggExpr, (SNode*)pPartExpr));
    if (!matched && NULL != pAggExpr && partIsCol && QUERY_NODE_COLUMN == nodeType((SNode*)pAggExpr)) {
      const SColumnNode* pAggCol = (const SColumnNode*)pAggExpr;
      matched = (0 == strcmp(pAggCol->colName, pPartCol->colName));
    }
    if (matched) {
      return true;
    }
  }

  return false;
}

static bool fqAggCoversPartitionKeys(const SAggLogicNode* pAgg, const SPartitionLogicNode* pPart) {
  if (NULL == pAgg || NULL == pPart || NULL == pPart->pPartitionKeys) {
    return false;
  }

  if (NULL == pAgg->pGroupKeys && NULL == pAgg->pAggFuncs) {
    return false;
  }

  SNode* pPartKey = NULL;
  FOREACH(pPartKey, pPart->pPartitionKeys) {
    if (!fqAggHasPartitionGroupKey(pAgg, pPartKey)) {
      return false;
    }
  }

  return true;
}

static int32_t fqSyncMergedPartitionKeyAliases(SNodeList* pGroupKeys, int32_t groupKeyStartIdx,
                                               const SNodeList* pWinFuncs) {
  if (NULL == pGroupKeys || NULL == pWinFuncs || LIST_LENGTH(pWinFuncs) == 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t groupKeyCount = LIST_LENGTH(pGroupKeys);
  if (groupKeyStartIdx < 0 || groupKeyStartIdx >= groupKeyCount) {
    planDebug("FQ-buildIntervalAgg: skip alias sync, invalid groupKeyStartIdx=%d groupKeyCount=%d winFuncCount=%d",
              groupKeyStartIdx, groupKeyCount, LIST_LENGTH((SNodeList*)pWinFuncs));
    return TSDB_CODE_SUCCESS;
  }

  int32_t appendedCount = groupKeyCount - groupKeyStartIdx;
  int32_t aliasIdx = 0;
  SNode*  pWinFuncNode = NULL;
  FOREACH(pWinFuncNode, pWinFuncs) {
    if (aliasIdx >= appendedCount) {
      break;
    }
    if (QUERY_NODE_FUNCTION != nodeType(pWinFuncNode)) {
      continue;
    }

    SFunctionNode* pWinFunc = (SFunctionNode*)pWinFuncNode;
    if (!fmIsGroupKeyFunc(pWinFunc->funcId)) {
      continue;
    }

    SNode* pKey = nodesListGetNode(pGroupKeys, groupKeyStartIdx + aliasIdx);
    SExprNode* pAliasCarrier = fqGetAliasCarrierFromGroupKey(pKey);
    if (NULL == pAliasCarrier) {
      planDebug("FQ-buildIntervalAgg: alias sync skip idx=%d keyType=%d (no expr carrier)",
                aliasIdx, NULL != pKey ? (int)nodeType(pKey) : -1);
      ++aliasIdx;
      continue;
    }

    const char* pInternalAlias = pWinFunc->node.aliasName;
    if (NULL == pInternalAlias || '\0' == pInternalAlias[0]) {
      planDebug("FQ-buildIntervalAgg: alias sync skip idx=%d group-key func has empty alias", aliasIdx);
      ++aliasIdx;
      continue;
    }

    char oldAlias[TSDB_COL_NAME_LEN] = {0};
    char oldUserAlias[TSDB_COL_NAME_LEN] = {0};
    tstrncpy(oldAlias, pAliasCarrier->aliasName, sizeof(oldAlias));
    tstrncpy(oldUserAlias, pAliasCarrier->userAlias, sizeof(oldUserAlias));

    tstrncpy(pAliasCarrier->aliasName, pInternalAlias, sizeof(pAliasCarrier->aliasName));
    if ('\0' == pAliasCarrier->userAlias[0] && '\0' != oldAlias[0]) {
      tstrncpy(pAliasCarrier->userAlias, oldAlias, sizeof(pAliasCarrier->userAlias));
    }

    if ('\0' == pAliasCarrier->userAlias[0] && '\0' != pWinFunc->node.userAlias[0]) {
      tstrncpy(pAliasCarrier->userAlias, pWinFunc->node.userAlias, sizeof(pAliasCarrier->userAlias));
    }

    planDebug("FQ-buildIntervalAgg: alias sync idx=%d groupKeyFunc=%s funcAlias=%s funcUserAlias=%s oldAlias=%s oldUserAlias=%s newAlias=%s newUserAlias=%s",
              aliasIdx, pWinFunc->functionName, pWinFunc->node.aliasName, pWinFunc->node.userAlias, oldAlias, oldUserAlias,
              pAliasCarrier->aliasName, pAliasCarrier->userAlias);
    ++aliasIdx;
  }

  if (aliasIdx != appendedCount) {
    planDebug("FQ-buildIntervalAgg: alias sync count mismatch, appended=%d synced=%d winFuncCount=%d",
              appendedCount, aliasIdx, LIST_LENGTH((SNodeList*)pWinFuncs));
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t fqAppendPartitionKeysToIntervalAgg(SAggLogicNode* pAgg, const SPartitionLogicNode* pPart) {
  if (NULL == pAgg || NULL == pPart || NULL == pPart->pPartitionKeys || LIST_LENGTH(pPart->pPartitionKeys) == 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  int32_t groupKeyStartIdx = pAgg->pGroupKeys ? LIST_LENGTH(pAgg->pGroupKeys) : 0;
  SNode*  pKey = NULL;
  FOREACH(pKey, pPart->pPartitionKeys) {
    SNode* pClone = NULL;
    code = nodesCloneNode(pKey, &pClone);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    code = nodesListStrictAppend(pAgg->pGroupKeys, pClone);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode(pClone);
      return code;
    }
    planDebug("FQ-buildIntervalAgg: append partition key nodeType=%d into interval agg group keys",
              (int)nodeType(pKey));
  }

  code = fqSyncMergedPartitionKeyAliases(pAgg->pGroupKeys, groupKeyStartIdx, pPart->node.pParent != NULL &&
                                                                           QUERY_NODE_LOGIC_PLAN_WINDOW == nodeType(pPart->node.pParent)
                                                                       ? ((SWindowLogicNode*)pPart->node.pParent)->pFuncs
                                                                       : NULL);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  nodesDestroyList(pAgg->node.pTargets);
  pAgg->node.pTargets = NULL;
  if (pAgg->pGroupKeys != NULL) {
    code = createColumnByRewriteExprs(pAgg->pGroupKeys, &pAgg->node.pTargets);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }
  if (pAgg->pAggFuncs != NULL) {
    code = createColumnByRewriteExprs(pAgg->pAggFuncs, &pAgg->node.pTargets);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  planDebug("FQ-buildIntervalAgg: merged partition keys into interval agg groupKeys=%d aggFuncs=%d targets=%d",
            pAgg->pGroupKeys ? (int)LIST_LENGTH(pAgg->pGroupKeys) : 0,
            pAgg->pAggFuncs ? (int)LIST_LENGTH(pAgg->pAggFuncs) : 0,
            pAgg->node.pTargets ? (int)LIST_LENGTH(pAgg->node.pTargets) : 0);
  return TSDB_CODE_SUCCESS;
}

// Harvest aggregation nodes (AGG) detached by aggOptimize and push them
// into the remote plan for remote-side aggregation.
// Check if a logic node (expected: AGG) contains any REPEAT_SCAN_FUNC (e.g. PERCENTILE).
static bool fqNodeHasRepeatScanFunc(const SLogicNode* pNode) {
  if (NULL == pNode || nodeType(pNode) != QUERY_NODE_LOGIC_PLAN_AGG) {
    return false;
  }
  const SAggLogicNode* pAgg = (const SAggLogicNode*)pNode;
  int32_t cnt = pAgg->pAggFuncs ? (int32_t)LIST_LENGTH(pAgg->pAggFuncs) : 0;
  const SNode* pFunc = NULL;
  FOREACH(pFunc, pAgg->pAggFuncs) {
    if (QUERY_NODE_FUNCTION == nodeType(pFunc)) {
      int32_t funcId   = ((const SFunctionNode*)pFunc)->funcId;
      bool    isRepeat = fmIsRepeatScanFunc(funcId);
      if (isRepeat) {
        return true;
      }
    }
  }
  return false;
}

static bool fqAggFuncNeedsOrderedInput(const SFunctionNode* pFunc) {
  if (NULL == pFunc) {
    return false;
  }

  // Skip TDengine internal pass-through wrappers for GROUP BY keys. They do
  // not consume row order and must not trigger synthetic remote ORDER BY.
  if (fmIsGroupKeyFunc(pFunc->funcId)) {
    return false;
  }

  // IMPORTANT: FUNC_MGT_KEEP_ORDER_FUNC is broader than "needs ordered input".
  // Many ordinary aggs (for example MIN/MAX) are tagged keep-order for planner
  // result-order propagation, but they do not consume timestamp row order and
  // must not trigger synthetic remote ORDER BY pk injection.  Restrict this
  // helper to functions that truly require time-ordered raw rows when they stay
  // local above an external scan.
  if (fmIsTimelineFunc(pFunc->funcId)) {
    return true;
  }

  switch (pFunc->funcType) {
    case FUNCTION_TYPE_FIRST:
    case FUNCTION_TYPE_FIRST_PARTIAL:
    case FUNCTION_TYPE_FIRST_MERGE:
    case FUNCTION_TYPE_LAST:
    case FUNCTION_TYPE_LAST_PARTIAL:
    case FUNCTION_TYPE_LAST_MERGE:
    case FUNCTION_TYPE_LAST_ROW:
    case FUNCTION_TYPE_CACHE_LAST:
    case FUNCTION_TYPE_GROUP_CONCAT:
    case FUNCTION_TYPE_LEASTSQUARES:
      return true;
    case FUNCTION_TYPE_TOP:
    case FUNCTION_TYPE_BOTTOM:
    case FUNCTION_TYPE_SAMPLE:
      return fmIsKeepOrderFunc((SFunctionNode*)pFunc);
    default:
      return false;
  }
}

static bool fqAggNodeNeedsOrderedInput(const SAggLogicNode* pAgg) {
  if (NULL == pAgg || NULL == pAgg->pAggFuncs) {
    return false;
  }

  int32_t funcIdx = 0;
  SNode*  pFuncNode = NULL;
  FOREACH(pFuncNode, pAgg->pAggFuncs) {
    if (nodeType(pFuncNode) != QUERY_NODE_FUNCTION) {
      planDebug("FQ-DBG: local AGG func[%d] nodeType=%d skipped for ordered-input probe",
                funcIdx, (int)nodeType(pFuncNode));
      ++funcIdx;
      continue;
    }

    SFunctionNode* pFunc = (SFunctionNode*)pFuncNode;
    bool keepOrder = fmIsKeepOrderFunc(pFunc);
    bool timeline  = fmIsTimelineFunc(pFunc->funcId);
    planDebug("FQ-DBG: local AGG func[%d] name=%s keepOrder=%d timeline=%d",
              funcIdx, pFunc->functionName, (int)keepOrder, (int)timeline);
    if (fqAggFuncNeedsOrderedInput(pFunc)) {
      return true;
    }
    ++funcIdx;
  }

  return false;
}

typedef struct {
  const SColumnNode* pPkCol;
  int32_t            code;
  bool               foundGroupConcat;
  bool               injectedOrderParam;
} SFqRemoteGroupConcatOrderCtx;

static bool fqOrderKeyColumnMatches(const SColumnNode* pCol, const SColumnNode* pPkCol) {
  if (NULL == pCol || NULL == pPkCol) {
    return false;
  }

  return pCol->isPrimTs || pCol->colId == PRIMARYKEY_TIMESTAMP_COL_ID ||
         (pPkCol->colName[0] != '\0' && 0 == strcmp(pCol->colName, pPkCol->colName));
}

static int32_t fqCloneRemotePkColRef(const SScanLogicNode* pDockScan, SColumnNode** ppPkCol) {
  if (NULL == pDockScan || NULL == ppPkCol || NULL == pDockScan->pExtTableNode) {
    return TSDB_CODE_INVALID_PARA;
  }

  SExtTableNode* pExtNode = (SExtTableNode*)pDockScan->pExtTableNode;
  if (NULL == pExtNode->pExtMeta || pExtNode->tsPrimaryColIdx < 0 ||
      pExtNode->tsPrimaryColIdx >= pExtNode->pExtMeta->numOfCols) {
    planDebug("FQ-gc-order: skip clone pk col - no ext primary key metadata dockScan=%p", pDockScan);
    *ppPkCol = NULL;
    return TSDB_CODE_SUCCESS;
  }

  const char* pkColName = pExtNode->pExtMeta->pCols[pExtNode->tsPrimaryColIdx].colName;
  SNode*      pColNode = NULL;
  FOREACH(pColNode, pDockScan->pScanCols) {
    if (nodeType(pColNode) == QUERY_NODE_COLUMN &&
        fqOrderKeyColumnMatches((SColumnNode*)pColNode, (const SColumnNode*)pColNode) &&
        (0 == strcmp(((SColumnNode*)pColNode)->colName, pkColName) ||
         ((SColumnNode*)pColNode)->isPrimTs ||
         ((SColumnNode*)pColNode)->colId == PRIMARYKEY_TIMESTAMP_COL_ID)) {
      int32_t code = nodesCloneNode(pColNode, (SNode**)ppPkCol);
      if (TSDB_CODE_SUCCESS == code) {
        (*ppPkCol)->isPrimTs = true;
        (*ppPkCol)->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
        (*ppPkCol)->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
        (*ppPkCol)->node.resType.bytes = (int32_t)sizeof(int64_t);
      }
      return code;
    }
  }

  SColumnNode* pPkCol = NULL;
  int32_t      code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pPkCol);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  SColumnNode* pTemplate = NULL;
  if (pDockScan->pScanCols && LIST_LENGTH(pDockScan->pScanCols) > 0) {
    SNode* pFirst = nodesListGetNode(pDockScan->pScanCols, 0);
    if (pFirst && nodeType(pFirst) == QUERY_NODE_COLUMN) {
      pTemplate = (SColumnNode*)pFirst;
    }
  }

  if (pTemplate != NULL) {
    tstrncpy(pPkCol->dbName, pTemplate->dbName, sizeof(pPkCol->dbName));
    tstrncpy(pPkCol->tableName, pTemplate->tableName, sizeof(pPkCol->tableName));
    tstrncpy(pPkCol->tableAlias, pTemplate->tableAlias, sizeof(pPkCol->tableAlias));
    pPkCol->tableType = pTemplate->tableType;
    pPkCol->tableId = pTemplate->tableId;
    pPkCol->colType = pTemplate->colType;
  } else {
    tstrncpy(pPkCol->dbName, pDockScan->tableName.dbname, sizeof(pPkCol->dbName));
    tstrncpy(pPkCol->tableName, pDockScan->tableName.tname, sizeof(pPkCol->tableName));
    tstrncpy(pPkCol->tableAlias, pDockScan->tableName.tname, sizeof(pPkCol->tableAlias));
    pPkCol->tableType = TSDB_NORMAL_TABLE;
    pPkCol->colType = COLUMN_TYPE_COLUMN;
  }

  tstrncpy(pPkCol->colName, pkColName, sizeof(pPkCol->colName));
  tstrncpy(pPkCol->node.aliasName, pkColName, sizeof(pPkCol->node.aliasName));
  tstrncpy(pPkCol->node.userAlias, pkColName, sizeof(pPkCol->node.userAlias));
  pPkCol->isPrimTs = true;
  pPkCol->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  pPkCol->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
  pPkCol->node.resType.bytes = (int32_t)sizeof(int64_t);

  *ppPkCol = pPkCol;
  return TSDB_CODE_SUCCESS;
}

static bool fqNodeListHasOrderKeyCol(SNodeList* pList, const SColumnNode* pPkCol) {
  if (NULL == pList || NULL == pPkCol) {
    return false;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    if (nodeType(pNode) == QUERY_NODE_COLUMN &&
        fqOrderKeyColumnMatches((SColumnNode*)pNode, pPkCol)) {
      return true;
    }
  }

  return false;
}

static int32_t fqAppendOrderKeyColToList(SNodeList** ppList, const SColumnNode* pPkCol) {
  if (NULL == ppList || NULL == pPkCol) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (NULL == *ppList) {
    int32_t code = nodesMakeList(ppList);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  if (fqNodeListHasOrderKeyCol(*ppList, pPkCol)) {
    return TSDB_CODE_SUCCESS;
  }

  SNode* pClone = NULL;
  int32_t code = nodesCloneNode((SNode*)pPkCol, &pClone);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  code = nodesListStrictAppend(*ppList, pClone);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pClone);
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

static EDealRes fqRemoteGroupConcatOrderWalker(SNode* pNode, void* pContext) {
  SFqRemoteGroupConcatOrderCtx* pCtx = (SFqRemoteGroupConcatOrderCtx*)pContext;
  if (NULL == pCtx || TSDB_CODE_SUCCESS != pCtx->code) {
    return DEAL_RES_ERROR;
  }

  if (QUERY_NODE_FUNCTION != nodeType(pNode)) {
    return DEAL_RES_CONTINUE;
  }

  SFunctionNode* pFunc = (SFunctionNode*)pNode;
  if (pFunc->funcType != FUNCTION_TYPE_GROUP_CONCAT) {
    return DEAL_RES_CONTINUE;
  }

  pCtx->foundGroupConcat = true;
  if (NULL != pFunc->pParameterList) {
    int32_t paramNum = LIST_LENGTH(pFunc->pParameterList);
    if (paramNum >= 3) {
      SNode* pLast = nodesListGetNode(pFunc->pParameterList, paramNum - 1);
      if (pLast != NULL && QUERY_NODE_COLUMN == nodeType(pLast) &&
          fqOrderKeyColumnMatches((SColumnNode*)pLast, pCtx->pPkCol)) {
        planDebug("FQ-gc-order: skip duplicate order param func=%p paramNum=%d col=%s",
                  pFunc, paramNum, ((SColumnNode*)pLast)->colName);
        return DEAL_RES_CONTINUE;
      }
    }
  }

  if (NULL == pFunc->pParameterList) {
    pCtx->code = nodesMakeList(&pFunc->pParameterList);
    if (TSDB_CODE_SUCCESS != pCtx->code) {
      planError("FQ-gc-order: create param list failed func=%p code=0x%x", pFunc, pCtx->code);
      return DEAL_RES_ERROR;
    }
  }

  SNode* pClone = NULL;
  pCtx->code = nodesCloneNode((SNode*)pCtx->pPkCol, &pClone);
  if (TSDB_CODE_SUCCESS != pCtx->code) {
    planError("FQ-gc-order: clone pk col failed func=%p code=0x%x", pFunc, pCtx->code);
    return DEAL_RES_ERROR;
  }

  pCtx->code = nodesListStrictAppend(pFunc->pParameterList, pClone);
  if (TSDB_CODE_SUCCESS != pCtx->code) {
    nodesDestroyNode(pClone);
    planError("FQ-gc-order: append order param failed func=%p code=0x%x", pFunc, pCtx->code);
    return DEAL_RES_ERROR;
  }

  pCtx->injectedOrderParam = true;
  planDebug("FQ-gc-order: appended hidden order param func=%p funcName=%s col=%s newParamNum=%d",
            pFunc, pFunc->functionName, pCtx->pPkCol->colName,
            LIST_LENGTH(pFunc->pParameterList));
  return DEAL_RES_CONTINUE;
}

static void fqWalkLogicExprFieldsForGcOrder(SLogicNode* pNode, SFqRemoteGroupConcatOrderCtx* pCtx) {
  if (NULL == pNode || NULL == pCtx || TSDB_CODE_SUCCESS != pCtx->code) {
    return;
  }

  nodesWalkExprs(pNode->pTargets, fqRemoteGroupConcatOrderWalker, pCtx);
  nodesWalkExpr(pNode->pConditions, fqRemoteGroupConcatOrderWalker, pCtx);
  nodesWalkExpr(pNode->pLimit, fqRemoteGroupConcatOrderWalker, pCtx);
  nodesWalkExpr(pNode->pSlimit, fqRemoteGroupConcatOrderWalker, pCtx);

  switch (nodeType(pNode)) {
    case QUERY_NODE_LOGIC_PLAN_SCAN: {
      SScanLogicNode* pScan = (SScanLogicNode*)pNode;
      nodesWalkExprs(pScan->pScanCols, fqRemoteGroupConcatOrderWalker, pCtx);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_AGG: {
      SAggLogicNode* pAgg = (SAggLogicNode*)pNode;
      nodesWalkExprs(pAgg->pGroupKeys, fqRemoteGroupConcatOrderWalker, pCtx);
      nodesWalkExprs(pAgg->pAggFuncs, fqRemoteGroupConcatOrderWalker, pCtx);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_PROJECT:
      nodesWalkExprs(((SProjectLogicNode*)pNode)->pProjections, fqRemoteGroupConcatOrderWalker, pCtx);
      break;
    default:
      break;
  }
}

static int32_t fqPrepareRemoteGroupConcatOrderRecursive(SLogicNode* pNode, const SColumnNode* pPkCol,
                                                        SFqRemoteGroupConcatOrderCtx* pCtx) {
  if (NULL == pNode || NULL == pPkCol || NULL == pCtx) {
    return TSDB_CODE_SUCCESS;
  }

  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    SScanLogicNode* pScan = (SScanLogicNode*)pNode;
    int32_t code = fqAppendOrderKeyColToList(&pScan->pScanCols, pPkCol);
    if (TSDB_CODE_SUCCESS != code) {
      planError("FQ-gc-order: append pk to remote scanCols failed scan=%p code=0x%x", pScan, code);
      return code;
    }
    code = fqAppendOrderKeyColToList(&pScan->node.pTargets, pPkCol);
    if (TSDB_CODE_SUCCESS != code) {
      planError("FQ-gc-order: append pk to remote targets failed scan=%p code=0x%x", pScan, code);
      return code;
    }
    planDebug("FQ-gc-order: ensured remote scan order col scan=%p source=%s col=%s scanCols=%d targets=%d",
              pScan,
              pScan->pExtTableNode ? ((SExtTableNode*)pScan->pExtTableNode)->sourceName : "(null)",
              pPkCol->colName,
              pScan->pScanCols ? LIST_LENGTH(pScan->pScanCols) : 0,
              pScan->node.pTargets ? LIST_LENGTH(pScan->node.pTargets) : 0);
  }

  fqWalkLogicExprFieldsForGcOrder(pNode, pCtx);
  if (TSDB_CODE_SUCCESS != pCtx->code) {
    return pCtx->code;
  }

  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    int32_t code = fqPrepareRemoteGroupConcatOrderRecursive((SLogicNode*)pChild, pPkCol, pCtx);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t fqPrepareRemoteGroupConcatOrder(SScanLogicNode* pScan) {
  if (NULL == pScan || NULL == pScan->pRemoteLogicPlan) {
    return TSDB_CODE_SUCCESS;
  }

  SColumnNode* pPkCol = NULL;
  int32_t code = fqCloneRemotePkColRef(pScan, &pPkCol);
  if (TSDB_CODE_SUCCESS != code || NULL == pPkCol) {
    if (TSDB_CODE_SUCCESS == code) {
      planDebug("FQ-gc-order: skip remote group_concat order prep scan=%p - no pk col cloned", pScan);
    }
    return code;
  }

  SFqRemoteGroupConcatOrderCtx cxt = {
      .pPkCol = pPkCol,
      .code = TSDB_CODE_SUCCESS,
      .foundGroupConcat = false,
      .injectedOrderParam = false,
  };

  code = fqPrepareRemoteGroupConcatOrderRecursive((SLogicNode*)pScan->pRemoteLogicPlan, pPkCol, &cxt);
  if (TSDB_CODE_SUCCESS == code) {
    planDebug("FQ-gc-order: remote plan prepared scan=%p foundGroupConcat=%d injectedOrderParam=%d pkCol=%s remoteRootType=%d",
              pScan, (int)cxt.foundGroupConcat, (int)cxt.injectedOrderParam, pPkCol->colName,
              pScan->pRemoteLogicPlan ? (int)nodeType(pScan->pRemoteLogicPlan) : -1);
  }

  nodesDestroyNode((SNode*)pPkCol);
  return code;
}

// Append a SSortLogicNode (ORDER BY <pk_col> ASC) at the bottom of
// pScan->pRemoteLogicPlan so that nodesRemotePlanToSQL emits:
//
//   SELECT ... FROM <table> [WHERE ...] ORDER BY <pk_col> ASC
//
// This guarantees external DB returns rows ordered by timestamp pk, matching
// TDengine's implicit ordering guarantee for scan results (DS section 5.2.x).
//
// Called only when:
//   (a) no Sort node is present in pRemoteLogicPlan (user/optimizer did not
//       specify ORDER BY), and
//   (b) the outer query is projection-only  -  no AGG or WINDOW above the scan
//       in the main logical plan (verified by the caller).
// ─────────────────────────────────────────────────────────────────────────────
static int32_t fqInjectPkOrderBy(SScanLogicNode* pScan, bool preserveScanWhere) {
  planDebug("FQ-DBG: fqInjectPkOrderBy called pRemoteLogicPlan=%p type=%d",
            pScan->pRemoteLogicPlan,
            pScan->pRemoteLogicPlan ? (int)nodeType(pScan->pRemoteLogicPlan) : -1);
  SExtTableNode* pExtNode = (SExtTableNode*)pScan->pExtTableNode;
  if (NULL == pExtNode || NULL == pExtNode->pExtMeta ||
      pExtNode->tsPrimaryColIdx < 0 ||
      pExtNode->tsPrimaryColIdx >= pExtNode->pExtMeta->numOfCols) {
    // No pk info available  -  skip silently; local Sort will handle ordering if needed.
    return TSDB_CODE_SUCCESS;
  }
  const char* pkColName = pExtNode->pExtMeta->pCols[pExtNode->tsPrimaryColIdx].colName;

  int32_t code = TSDB_CODE_SUCCESS;

  // ── SColumnNode for the pk column ──
  // Do NOT fabricate a bare column (colName only): in a JOIN remote plan both legs
  // expose a pk column of the same name, so a prefix-less ORDER BY "<pk>" is
  // ambiguous (e.g. MySQL error 1052).  Clone the real pk column from pScanCols
  // (which carries tableAlias/tableName/colId), so JOIN rendering emits a
  // leg-qualified "alias"."<pk>".  Fall back to a fabricated column only when no
  // matching pk column is found in pScanCols.
  SColumnNode* pPkCol = NULL;
  if (pScan->pScanCols != NULL) {
    SNode* pColNode = NULL;
    FOREACH(pColNode, pScan->pScanCols) {
      if (nodeType(pColNode) == QUERY_NODE_COLUMN &&
          0 == strcmp(((SColumnNode*)pColNode)->colName, pkColName)) {
        code = nodesCloneNode(pColNode, (SNode**)&pPkCol);
        if (TSDB_CODE_SUCCESS != code) return code;
        break;
      }
    }
  }
  if (NULL == pPkCol) {
    code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pPkCol);
    if (TSDB_CODE_SUCCESS != code) return code;
    tstrncpy(pPkCol->colName, pkColName, TSDB_COL_NAME_LEN);
    pPkCol->node.resType.type  = TSDB_DATA_TYPE_TIMESTAMP;
    pPkCol->node.resType.bytes = (int32_t)sizeof(int64_t);  // TIMESTAMP is always 8 bytes
  }

  // -- SOrderByExprNode wrapping the column --
  SOrderByExprNode* pOrdExpr = NULL;
  code = nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR, (SNode**)&pOrdExpr);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pPkCol);
    return code;
  }
  pOrdExpr->pExpr      = (SNode*)pPkCol;
  pOrdExpr->order      = ORDER_ASC;
  pOrdExpr->nullOrder  = NULL_ORDER_DEFAULT;  // ts pk cannot be NULL; use dialect default ordering

  // -- SSortLogicNode with pSortKeys = [pOrdExpr] --
  SSortLogicNode* pSortLogic = NULL;
  code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SORT, (SNode**)&pSortLogic);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pOrdExpr);  // pPkCol owned by pOrdExpr
    return code;
  }
  code = nodesMakeList(&pSortLogic->pSortKeys);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pSortLogic);
    nodesDestroyNode((SNode*)pOrdExpr);
    return code;
  }
  code = nodesListStrictAppend(pSortLogic->pSortKeys, (SNode*)pOrdExpr);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pSortLogic);
    nodesDestroyNode((SNode*)pOrdExpr);
    return code;
  }

  // -- Wire pSortLogic into pRemoteLogicPlan --
  // If pRemoteLogicPlan is NULL: sort becomes the sole remote plan node.
  // Otherwise: insert Sort at the TOP of the chain as the new root.
  // This ensures renderNode(Sort) calls renderNode(child=Project/FedScan)
  // first, then appends ORDER BY — producing flat SQL:
  //   SELECT expr FROM table [WHERE] ORDER BY pk
  // instead of a subquery wrapping.
  if (NULL == pScan->pRemoteLogicPlan) {
    // No remote plan yet: the injected Sort becomes the root, but it must carry
    // a SCAN leaf so the remote plan is a self-contained tree (Sort -> SCAN).
    // Clone pScan as a pure raw-row leaf (FROM + pScanCols only).  Any local
    // scan WHERE that blocked collect must stay on the docking scan; pk-order
    // injection here is only for deterministic row order, never for filter push.
    // Virtual-table direct-child scans are the exception: when their scan WHERE
    // itself is pushable, the raw leaf must preserve that condition so the
    // shared external source table is constrained before the vtable merge.
    SScanLogicNode* pLeafScan = NULL;
    code = nodesCloneNode((SNode*)pScan, (SNode**)&pLeafScan);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pSortLogic); return code; }
    if (pLeafScan->node.pChildren != NULL) {
      nodesDestroyList(pLeafScan->node.pChildren);
      pLeafScan->node.pChildren = NULL;
    }
    pLeafScan->node.pParent = NULL;
    if (pLeafScan->pRemoteLogicPlan != NULL) {
      nodesDestroyNode(pLeafScan->pRemoteLogicPlan);
      pLeafScan->pRemoteLogicPlan = NULL;
    }
    if (!preserveScanWhere && pLeafScan->node.pConditions != NULL) {
      planDebug("FQ-injectPkOrderBy: clear local-only scan WHERE from injected raw leaf scan=%p", pLeafScan);
      nodesDestroyNode(pLeafScan->node.pConditions);
      pLeafScan->node.pConditions = NULL;
    }
    code = nodesMakeList(&pSortLogic->node.pChildren);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pLeafScan); nodesDestroyNode((SNode*)pSortLogic); return code; }
    code = nodesListStrictAppend(pSortLogic->node.pChildren, (SNode*)pLeafScan);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pLeafScan); nodesDestroyNode((SNode*)pSortLogic); return code; }
    pLeafScan->node.pParent = (SLogicNode*)pSortLogic;
    pScan->pRemoteLogicPlan = (SNode*)pSortLogic;
  } else {
    SLogicNode* pOldRoot = (SLogicNode*)pScan->pRemoteLogicPlan;
    code = nodesMakeList(&pSortLogic->node.pChildren);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode((SNode*)pSortLogic);
      return code;
    }
    code = nodesListStrictAppend(pSortLogic->node.pChildren, (SNode*)pOldRoot);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyList(pSortLogic->node.pChildren);
      pSortLogic->node.pChildren = NULL;
      nodesDestroyNode((SNode*)pSortLogic);
      return code;
    }
    pOldRoot->pParent = (SLogicNode*)pSortLogic;
    pScan->pRemoteLogicPlan = (SNode*)pSortLogic;
    // Move LIMIT from the old root to the new Sort so ORDER BY appears before
    // LIMIT in the generated SQL.  SQL execution order: ORDER BY (rank 6) before
    // LIMIT (rank 7), so Sort must own pLimit when it wraps a LIMIT-bearing child.
    if (pOldRoot->pLimit != NULL) {
      pSortLogic->node.pLimit = pOldRoot->pLimit;
      pOldRoot->pLimit = NULL;
    }
  }

  // ── Set pTargets on Sort to match its child's output ──
  // planPhysiCreater expects every logic node to have pTargets set so it can
  // build pOutputDataBlockDesc correctly.  Inherit pTargets from the child
  // (the original pRemoteLogicPlan root, or the scan's current pTargets if
  // Sort is the sole node).
  {
    SLogicNode* pChild = (SLogicNode*)nodesListGetNode(pSortLogic->node.pChildren, 0);
    SNodeList*  pSrcTargets = (pChild && pChild->pTargets) ? pChild->pTargets
                                                            : pScan->node.pTargets;
    if (pSrcTargets != NULL) {
      SNodeList* pNewTargets = NULL;
      code = nodesCloneList(pSrcTargets, &pNewTargets);
      if (TSDB_CODE_SUCCESS != code) {
        nodesDestroyNode((SNode*)pSortLogic);
        return code;
      }
      pSortLogic->node.pTargets = pNewTargets;
    }
  }

  planDebug("FQ-injectPkOrderBy: injected remote ORDER BY %s ASC (remotePlan root type=%d)",
            pkColName, pScan->pRemoteLogicPlan ? (int)nodeType(pScan->pRemoteLogicPlan) : -1);
  return TSDB_CODE_SUCCESS;
}

// --- FqInterp: build UNION ALL remote plan for INTERP on external scan ------
//
// For INTERP queries on external tables, the local TimeSlice operator needs
// boundary data rows (PREV / NEXT) in addition to the RANGE data for
// fill modes that require interpolation beyond the query range.
//
// This rule runs AFTER FqPushdown.  It finds the INTERP logic node above an
// external scan, reads fillMode + timeRange, and constructs a complete
// pRemoteLogicPlan sub-tree using UNION ALL (SProjectLogicNode with
// isSetOpProj=true and multiple children) so that the remote source returns
// all necessary data in a single query.
//
// Fill mode  ->  branches:
//   NULL/VALUE/VALUE_F/NULL_F/NONE  ->  1 branch  (MAIN only)
//   PREV                           ->  2 branches (PREV + MAIN)
//   NEXT                           ->  2 branches (MAIN + NEXT)
//   LINEAR/NEAR                    ->  3 branches (PREV + MAIN + NEXT)
//
// Each branch is: SSortLogicNode  ->  SScanLogicNode (with WHERE + optional LIMIT).
// The UNION ALL container is: SProjectLogicNode(isSetOpProj=true, ignoreGroupId=true).
// -----------------------------------------------------------------------------

// Helper: find SInterpFuncLogicNode ancestor above a given scan node.
static SInterpFuncLogicNode* fqFindInterpAboveScan(SScanLogicNode* pScan) {
  SLogicNode* pCur = pScan->node.pParent;
  while (pCur != NULL) {
    if (nodeType(pCur) == QUERY_NODE_LOGIC_PLAN_INTERP_FUNC) {
      return (SInterpFuncLogicNode*)pCur;
    }
    pCur = pCur->pParent;
  }
  return NULL;
}

// Helper: create one branch (Sort -> Scan) for the UNION ALL plan.
// pScan: the original external scan node (used for column/table info cloning).
// pkColName: primary key column name for ORDER BY.
// order: ORDER_ASC or ORDER_DESC.
// pCondExpr: WHERE condition (owned by caller, will be cloned).
// hasLimit1: whether to add LIMIT 1 to the scan.
static int32_t fqInterpCreateBranch(SScanLogicNode* pScan, const char* pkColName,
                                    EOrder order, SNode* pCondExpr, bool hasLimit1,
                                    SLogicNode** ppBranch) {
  int32_t code = TSDB_CODE_SUCCESS;

  // -- Create SScanLogicNode (leaf of the branch) --
  SScanLogicNode* pNewScan = NULL;
  code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SCAN, (SNode**)&pNewScan);
  if (TSDB_CODE_SUCCESS != code) return code;

  pNewScan->scanType = SCAN_TYPE_EXTERNAL;
  // Clone pScanCols, pExtTableNode
  code = nodesCloneList(pScan->pScanCols, &pNewScan->pScanCols);
  if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pNewScan); return code; }
  code = nodesCloneNode(pScan->pExtTableNode, &pNewScan->pExtTableNode);
  if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pNewScan); return code; }
  code = nodesCloneList(pScan->node.pTargets, &pNewScan->node.pTargets);
  if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pNewScan); return code; }

  // Set WHERE condition
  if (pCondExpr != NULL) {
    code = nodesCloneNode(pCondExpr, &pNewScan->node.pConditions);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pNewScan); return code; }
  }

  // Set LIMIT 1
  if (hasLimit1) {
    SLimitNode* pLimit = NULL;
    code = nodesMakeNode(QUERY_NODE_LIMIT, (SNode**)&pLimit);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pNewScan); return code; }
    code = nodesMakeValueNodeFromInt64(1, (SNode**)&pLimit->limit);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pNewScan); nodesDestroyNode((SNode*)pLimit); return code; }
    code = nodesMakeValueNodeFromInt64(0, (SNode**)&pLimit->offset);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pNewScan); nodesDestroyNode((SNode*)pLimit); return code; }
    pNewScan->node.pLimit = (SNode*)pLimit;
  }

  // -- Create SSortLogicNode (wraps the scan) --
  SSortLogicNode* pSort = NULL;
  code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SORT, (SNode**)&pSort);
  if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pNewScan); return code; }

  // Create ORDER BY key
  SColumnNode* pPkCol = NULL;
  code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pPkCol);
  if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pNewScan); nodesDestroyNode((SNode*)pSort); return code; }
  tstrncpy(pPkCol->colName, pkColName, TSDB_COL_NAME_LEN);
  pPkCol->node.resType.type  = TSDB_DATA_TYPE_TIMESTAMP;
  pPkCol->node.resType.bytes = (int32_t)sizeof(int64_t);

  SOrderByExprNode* pOrd = NULL;
  code = nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR, (SNode**)&pOrd);
  if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pNewScan); nodesDestroyNode((SNode*)pSort); nodesDestroyNode((SNode*)pPkCol); return code; }
  pOrd->pExpr     = (SNode*)pPkCol;
  pOrd->order     = order;
  pOrd->nullOrder = NULL_ORDER_FIRST;

  code = nodesMakeList(&pSort->pSortKeys);
  if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pNewScan); nodesDestroyNode((SNode*)pSort); nodesDestroyNode((SNode*)pOrd); return code; }
  code = nodesListStrictAppend(pSort->pSortKeys, (SNode*)pOrd);
  if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pNewScan); nodesDestroyNode((SNode*)pSort); return code; }

  // Clone pTargets for sort
  code = nodesCloneList(pScan->node.pTargets, &pSort->node.pTargets);
  if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pNewScan); nodesDestroyNode((SNode*)pSort); return code; }

  // Wire scan as child of sort
  code = nodesMakeList(&pSort->node.pChildren);
  if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pNewScan); nodesDestroyNode((SNode*)pSort); return code; }
  code = nodesListStrictAppend(pSort->node.pChildren, (SNode*)pNewScan);
  if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pNewScan); nodesDestroyNode((SNode*)pSort); return code; }
  pNewScan->node.pParent = (SLogicNode*)pSort;

  *ppBranch = (SLogicNode*)pSort;
  return TSDB_CODE_SUCCESS;
}

// Helper: create a comparison condition node: <pkCol> <op> <tsValue>
static int32_t fqInterpCreateTsCond(const char* pkColName, EOperatorType opType,
                                    int64_t tsVal, SNode** ppCond) {
  int32_t code = TSDB_CODE_SUCCESS;

  SOperatorNode* pOp = NULL;
  code = nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pOp);
  if (TSDB_CODE_SUCCESS != code) return code;
  pOp->opType = opType;
  pOp->node.resType.type  = TSDB_DATA_TYPE_BOOL;
  pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;

  // Left: column node
  SColumnNode* pCol = NULL;
  code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pOp); return code; }
  tstrncpy(pCol->colName, pkColName, TSDB_COL_NAME_LEN);
  pCol->node.resType.type  = TSDB_DATA_TYPE_TIMESTAMP;
  pCol->node.resType.bytes = (int32_t)sizeof(int64_t);
  pOp->pLeft = (SNode*)pCol;

  // Right: value node
  SValueNode* pVal = NULL;
  code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pVal);
  if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pOp); return code; }
  pVal->node.resType.type  = TSDB_DATA_TYPE_TIMESTAMP;
  pVal->node.resType.bytes = (int32_t)sizeof(int64_t);
  pVal->datum.i   = tsVal;
  pVal->translate  = true;
  pOp->pRight = (SNode*)pVal;

  *ppCond = (SNode*)pOp;
  return TSDB_CODE_SUCCESS;
}

// Helper: create AND(left, right) condition node.  Takes ownership of pLeft and pRight.
static int32_t fqInterpCreateAndCond(SNode* pLeft, SNode* pRight, SNode** ppCond) {
  int32_t code = TSDB_CODE_SUCCESS;
  SLogicConditionNode* pAnd = NULL;
  code = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, (SNode**)&pAnd);
  if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode(pLeft); nodesDestroyNode(pRight); return code; }
  pAnd->condType = LOGIC_COND_TYPE_AND;
  pAnd->node.resType.type  = TSDB_DATA_TYPE_BOOL;
  pAnd->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
  code = nodesMakeList(&pAnd->pParameterList);
  if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode(pLeft); nodesDestroyNode(pRight); nodesDestroyNode((SNode*)pAnd); return code; }
  code = nodesListStrictAppend(pAnd->pParameterList, pLeft);
  if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode(pRight); nodesDestroyNode((SNode*)pAnd); return code; }
  code = nodesListStrictAppend(pAnd->pParameterList, pRight);
  if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pAnd); return code; }
  *ppCond = (SNode*)pAnd;
  return TSDB_CODE_SUCCESS;
}

static int32_t fqInterpOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  int32_t dialect = pCxt->pExtCapability ? (int32_t)pCxt->pExtCapability->dialect : -1;
  EExtClauseKind interpClause = EXT_CLAUSE_INTERP;
  if (!fqClauseIsPushable(interpClause, dialect, pCxt->pSessionOverlay) ||
      capDisabled(pCxt->pExtCapability, pCxt->pSessionOverlay, EXT_DEMOTE_KIND_CLAUSE, &interpClause)) {
    planDebug("FqInterp: skip - INTERP clause disabled by capability matrix dialect=%d subQIdx=%d",
              dialect, pCxt->pPlanCxt ? pCxt->pPlanCxt->subqueryIndex : -2);
    return TSDB_CODE_SUCCESS;
  }

  // Find the docking external scan below the local INTERP node.  This must
  // ignore OPTIMIZE_FLAG_FQ_PUSHDOWN because FqPushdown has already processed
  // the scan in an earlier rule round.
  SScanLogicNode* pScan = fqFindInterpExternalScan(pLogicSubplan->pNode);
  if (NULL == pScan) {
    planDebug("FqInterp: skip - no external scan found under local INTERP subtree");
    return TSDB_CODE_SUCCESS;
  }

  // Find INTERP ancestor above the scan
  SInterpFuncLogicNode* pInterp = fqFindInterpAboveScan(pScan);
  if (NULL == pInterp) {
    planDebug("FqInterp: skip - matched external scan=%p but no INTERP ancestor remains", pScan);
    return TSDB_CODE_SUCCESS;
  }

  // Get pk column name
  SExtTableNode* pExtNode = (SExtTableNode*)pScan->pExtTableNode;
  if (NULL == pExtNode || NULL == pExtNode->pExtMeta ||
      pExtNode->tsPrimaryColIdx < 0 ||
      pExtNode->tsPrimaryColIdx >= pExtNode->pExtMeta->numOfCols) {
    return TSDB_CODE_SUCCESS;  // no pk info  -  cannot build time conditions
  }
  const char* pkColName = pExtNode->pExtMeta->pCols[pExtNode->tsPrimaryColIdx].colName;

  int64_t   t1       = pInterp->timeRange.skey;
  int64_t   t2       = pInterp->timeRange.ekey;
  EFillMode fillMode = pInterp->fillMode;

  // Determine which branches are needed
  bool needPrev = (fillMode == FILL_MODE_PREV || fillMode == FILL_MODE_LINEAR || fillMode == FILL_MODE_NEAR);
  bool needNext = (fillMode == FILL_MODE_NEXT || fillMode == FILL_MODE_LINEAR || fillMode == FILL_MODE_NEAR);
  int  nBranches = 1 + (needPrev ? 1 : 0) + (needNext ? 1 : 0);

  int32_t code = TSDB_CODE_SUCCESS;

  // Destroy existing pRemoteLogicPlan (injected by FqPushdown's fqInjectPkOrderBy)
  if (pScan->pRemoteLogicPlan != NULL) {
    nodesDestroyNode(pScan->pRemoteLogicPlan);
    pScan->pRemoteLogicPlan = NULL;
  }

  if (nBranches == 1) {
    // Single branch: Sort(ASC)  ->  Scan(WHERE ts >= t1 AND ts <= t2)
    SNode* pGe = NULL;
    code = fqInterpCreateTsCond(pkColName, OP_TYPE_GREATER_EQUAL, t1, &pGe);
    if (TSDB_CODE_SUCCESS != code) return code;
    SNode* pLe = NULL;
    code = fqInterpCreateTsCond(pkColName, OP_TYPE_LOWER_EQUAL, t2, &pLe);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode(pGe); return code; }
    SNode* pCond = NULL;
    code = fqInterpCreateAndCond(pGe, pLe, &pCond);
    if (TSDB_CODE_SUCCESS != code) return code;

    SLogicNode* pBranch = NULL;
    code = fqInterpCreateBranch(pScan, pkColName, ORDER_ASC, pCond, false, &pBranch);
    nodesDestroyNode(pCond);
    if (TSDB_CODE_SUCCESS != code) return code;

    pScan->pRemoteLogicPlan = (SNode*)pBranch;
  } else {
    // Multiple branches: wrap in SProjectLogicNode(isSetOpProj=true)
    SProjectLogicNode* pUnion = NULL;
    code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_PROJECT, (SNode**)&pUnion);
    if (TSDB_CODE_SUCCESS != code) return code;
    pUnion->isSetOpProj   = true;
    pUnion->ignoreGroupId = true;

    // Clone pTargets from scan
    code = nodesCloneList(pScan->node.pTargets, &pUnion->node.pTargets);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pUnion); return code; }

    // Build projections = simple column refs matching pTargets
    code = nodesCloneList(pScan->node.pTargets, &pUnion->pProjections);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pUnion); return code; }

    code = nodesMakeList(&pUnion->node.pChildren);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pUnion); return code; }

    // -- PREV branch: Sort(DESC)  ->  Scan(WHERE ts < t1, LIMIT 1) --
    if (needPrev) {
      SNode* pCond = NULL;
      code = fqInterpCreateTsCond(pkColName, OP_TYPE_LOWER_THAN, t1, &pCond);
      if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pUnion); return code; }
      SLogicNode* pBranch = NULL;
      code = fqInterpCreateBranch(pScan, pkColName, ORDER_DESC, pCond, true, &pBranch);
      nodesDestroyNode(pCond);
      if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pUnion); return code; }
      pBranch->pParent = (SLogicNode*)pUnion;
      code = nodesListStrictAppend(pUnion->node.pChildren, (SNode*)pBranch);
      if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pUnion); return code; }
    }

    // -- MAIN branch: Sort(ASC)  ->  Scan(WHERE ts >= t1 AND ts <= t2) --
    {
      SNode* pGe = NULL;
      code = fqInterpCreateTsCond(pkColName, OP_TYPE_GREATER_EQUAL, t1, &pGe);
      if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pUnion); return code; }
      SNode* pLe = NULL;
      code = fqInterpCreateTsCond(pkColName, OP_TYPE_LOWER_EQUAL, t2, &pLe);
      if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode(pGe); nodesDestroyNode((SNode*)pUnion); return code; }
      SNode* pCond = NULL;
      code = fqInterpCreateAndCond(pGe, pLe, &pCond);
      if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pUnion); return code; }
      SLogicNode* pBranch = NULL;
      code = fqInterpCreateBranch(pScan, pkColName, ORDER_ASC, pCond, false, &pBranch);
      nodesDestroyNode(pCond);
      if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pUnion); return code; }
      pBranch->pParent = (SLogicNode*)pUnion;
      code = nodesListStrictAppend(pUnion->node.pChildren, (SNode*)pBranch);
      if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pUnion); return code; }
    }

    // -- NEXT branch: Sort(ASC)  ->  Scan(WHERE ts > t2, LIMIT 1) --
    if (needNext) {
      SNode* pCond = NULL;
      code = fqInterpCreateTsCond(pkColName, OP_TYPE_GREATER_THAN, t2, &pCond);
      if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pUnion); return code; }
      SLogicNode* pBranch = NULL;
      code = fqInterpCreateBranch(pScan, pkColName, ORDER_ASC, pCond, true, &pBranch);
      nodesDestroyNode(pCond);
      if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pUnion); return code; }
      pBranch->pParent = (SLogicNode*)pUnion;
      code = nodesListStrictAppend(pUnion->node.pChildren, (SNode*)pBranch);
      if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pUnion); return code; }
    }

    pScan->pRemoteLogicPlan = (SNode*)pUnion;
  }

  planInfo("FqInterp: built %d-branch remote plan for fillMode=%d range=[%" PRId64 ",%" PRId64 "]",
           nBranches, fillMode, t1, t2);

  // Mark scan as processed by FqPushdown flag to prevent re-entry
  OPTIMIZE_FLAG_SET_MASK(pScan->node.optimizedFlag, OPTIMIZE_FLAG_FQ_PUSHDOWN);
  pCxt->optimized = true;
  return TSDB_CODE_SUCCESS;
}

// ─── FQ pushdown result logger ───────────────────────────────────────────────
// Logs the final optimizer decision for one ExternalScan node at DEBUG level.
// Gated by qDebugFlag so no string-building overhead when debug logging is off.
// Call once per fqPushdownOptimize invocation, after all decisions are made.
// Output example:
//   FQ-OPT result: dialect=2 pScanCols=[time,val] remoteChain=[Sort] localWHERE=none remoteWHERE=YES pLimit=none
static void fqLogPushdownResult(const SScanLogicNode* pScan, int32_t dialect) {
  if (!(qDebugFlag & DEBUG_DEBUG)) return;

  // Build pScanCols string
  char scanColsBuf[512] = {0};
  int  off = 0;
  if (pScan->pScanCols != NULL) {
    SNode* pSCol = NULL;
    FOREACH(pSCol, pScan->pScanCols) {
      const char* colName = "?";
      if (nodeType(pSCol) == QUERY_NODE_COLUMN) {
        colName = ((const SColumnNode*)pSCol)->colName;
      } else if (nodeType(pSCol) == QUERY_NODE_TARGET) {
        const SNode* pInner = ((const STargetNode*)pSCol)->pExpr;
        if (pInner && nodeType(pInner) == QUERY_NODE_COLUMN)
          colName = ((const SColumnNode*)pInner)->colName;
      }
      off += snprintf(scanColsBuf + off, sizeof(scanColsBuf) - (size_t)off - 1, "%s,", colName);
      if (off >= (int)sizeof(scanColsBuf) - 2) break;
    }
    if (off > 0 && scanColsBuf[off - 1] == ',') scanColsBuf[off - 1] = '\0';
  } else {
    tstrncpy(scanColsBuf, "(EMPTY - BUG)", sizeof(scanColsBuf));
  }

  // Build pRemoteLogicPlan chain string
  char chainBuf[256] = {0};
  int  chainOff = 0;
  const SNode* pRLP = pScan->pRemoteLogicPlan;
  while (pRLP != NULL) {
    ENodeType rtype = nodeType(pRLP);
    const char* name = (rtype == QUERY_NODE_LOGIC_PLAN_SORT)    ? "Sort" :
                       (rtype == QUERY_NODE_LOGIC_PLAN_PROJECT)  ? "Project" :
                       (rtype == QUERY_NODE_LOGIC_PLAN_AGG)      ? "Agg" : "?";
    chainOff += snprintf(chainBuf + chainOff, sizeof(chainBuf) - (size_t)chainOff - 1, "%s->", name);
    if (chainOff >= (int)sizeof(chainBuf) - 2) break;
    const SNodeList* pCh = ((const SLogicNode*)pRLP)->pChildren;
    pRLP = (pCh && LIST_LENGTH(pCh) > 0) ? nodesListGetNode((SNodeList*)pCh, 0) : NULL;
  }
  if (chainOff > 1 && chainBuf[chainOff - 1] == '>') chainBuf[chainOff - 1] = '\0';
  if (chainOff == 0) tstrncpy(chainBuf, "(none)", sizeof(chainBuf));

  planDebug("FQ-OPT result: dialect=%d pScanCols=[%s] remoteChain=[%s] "
            "localWHERE=%s remotePlan=%s pLimit=%s",
            dialect, scanColsBuf, chainBuf,
            pScan->node.pConditions    ? "YES" : "none",
            pScan->pRemoteLogicPlan    ? "YES" : "none",
            pScan->node.pLimit         ? "YES" : "none");
}

// ─── Unified FQ pipeline notes: pk ORDER BY injection fallback ───────────────
//
// Rules for pk ORDER BY injection (DS section 5.2.x  -  "fallback flow ordering"):
//   Inject when ALL of the following hold:
//   1. No Sort node was pushed down (user did not specify ORDER BY).
//   2. The outer query is projection-only: no AGG or WINDOW node sits above
//      the external scan in the main logical plan.
//   3. The remote subtree remains raw-row passthrough, so local upstream
//      operators still depend on deterministic timestamp order.
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// fqPushLimit — Step 4 of the unified FQ pushdown pipeline.
//
// Called after collect+commit.  At this point every local operator that could
// be pushed or eliminated has already been processed.
// If pScan->node.pParent == NULL the entire statement has been pushed to the
// remote plan and LIMIT can follow.
//
// LIMIT always sits at the top of the logical plan (it controls the final
// output of the entire statement).  We clone it onto pRemoteLogicPlan's top
// node so renderNode emits it at the outermost SQL level.
//
// OFFSET is only included when the capability gate permits it; otherwise the
// entire LIMIT+OFFSET is kept local (they must stay together for correctness).
static int32_t fqPushLimit(SScanLogicNode* pScan, int32_t dialect,
                            SOptimizeContext* pCxt) {
  if (pScan->pRemoteLogicPlan == NULL) return TSDB_CODE_SUCCESS;
  // Only push LIMIT when the whole statement is pushed down.
  if (pScan->node.pParent != NULL) return TSDB_CODE_SUCCESS;

  EExtClauseKind limitClause  = EXT_CLAUSE_LIMIT;
  EExtClauseKind offsetClause = EXT_CLAUSE_OFFSET;
  if (!fqClauseIsPushable(limitClause, dialect, pCxt->pSessionOverlay) ||
      capDisabled(pCxt->pExtCapability, pCxt->pSessionOverlay, EXT_DEMOTE_KIND_CLAUSE, &limitClause)) {
    return TSDB_CODE_SUCCESS;
  }

  // Walk main-plan parent chain to find LIMIT.
  // FqPushdown runs before PushDownLimit, so LIMIT is still on its original
  // parser-assigned node (PROJECT or the outermost operator above SCAN).
  // After steps 2+3 the chain has been consumed and pScan->node.pParent == NULL,
  // so we must look at the pRemoteLogicPlan top node instead — all operators
  // that were in the main-plan parent chain have been moved there.
  // The LIMIT node travels with its operator into pRemoteLogicPlan.
  // Walk the remote plan top-down and find the first node carrying pLimit.
  SNode*      pLimitSrc = NULL;
  SLogicNode* pLimitOwner = NULL;
  {
    SNode* pCurr = pScan->pRemoteLogicPlan;
    while (pCurr != NULL) {
      SLogicNode* pL = (SLogicNode*)pCurr;
      if (pL->pLimit != NULL) {
        pLimitSrc = pL->pLimit;
        pLimitOwner = pL;
        break;
      }
      SNodeList* pCh = pL->pChildren;
      pCurr = (pCh != NULL && LIST_LENGTH(pCh) > 0) ? nodesListGetNode(pCh, 0) : NULL;
    }
  }
  if (pLimitSrc == NULL) return TSDB_CODE_SUCCESS;

  SLimitNode* pLim = (SLimitNode*)pLimitSrc;
  bool hasOffset = (pLim->offset != NULL && pLim->offset->datum.i > 0);
  if (hasOffset && (!fqClauseIsPushable(offsetClause, dialect, pCxt->pSessionOverlay) ||
      capDisabled(pCxt->pExtCapability, pCxt->pSessionOverlay, EXT_DEMOTE_KIND_CLAUSE, &offsetClause))) {
    planDebug("FqPushLimit: OFFSET disabled - keeping LIMIT+OFFSET local");
    // Clear pLimit from pRemoteLogicPlan so it is not rendered there.
    SNode* pCurr = pScan->pRemoteLogicPlan;
    while (pCurr != NULL) {
      SLogicNode* pL = (SLogicNode*)pCurr;
      if (pL->pLimit != NULL) { nodesDestroyNode(pL->pLimit); pL->pLimit = NULL; break; }
      SNodeList* pCh = pL->pChildren;
      pCurr = (pCh != NULL && LIST_LENGTH(pCh) > 0) ? nodesListGetNode(pCh, 0) : NULL;
    }
    return TSDB_CODE_SUCCESS;
  }

  // Keep ORDER-sensitive LIMIT attached to the SORT node. Rewriting TAIL to
  // ordinary ORDER BY + LIMIT relies on the two clauses staying in the same
  // remote SELECT layer; hoisting LIMIT above wrapper projects changes
  // semantics for connectors such as Influx/DataFusion.
  SLogicNode* pTop = (SLogicNode*)pScan->pRemoteLogicPlan;
  if (pLimitOwner != NULL && nodeType((SNode*)pLimitOwner) == QUERY_NODE_LOGIC_PLAN_SORT) {
    planDebug("FqPushLimit: keep LIMIT on remote sort owner=%p top=%p to preserve ORDER BY + LIMIT semantics",
              pLimitOwner, pTop);
    return TSDB_CODE_SUCCESS;
  }

  // Move pLimit to the top of pRemoteLogicPlan.
  if ((SNode*)pTop->pLimit != pLimitSrc) {
    // Detach from current position.
    SNode* pCurr = pScan->pRemoteLogicPlan;
    while (pCurr != NULL) {
      SLogicNode* pL = (SLogicNode*)pCurr;
      if (pL->pLimit != NULL) { pL->pLimit = NULL; break; }
      SNodeList* pCh = pL->pChildren;
      pCurr = (pCh != NULL && LIST_LENGTH(pCh) > 0) ? nodesListGetNode(pCh, 0) : NULL;
    }
    nodesDestroyNode(pTop->pLimit);
    pTop->pLimit = pLimitSrc;
  }
  planDebug("FqPushLimit: pushed LIMIT to top of pRemoteLogicPlan (nodeType=%d)",
            (int)nodeType((SNode*)pTop));
  return TSDB_CODE_SUCCESS;
}


// ─────────────────────────────────────────────────────────────────────────────
// FQ pushdown now runs as a collect->commit pipeline:
//
// Step 1 (collect, READ-ONLY): fqCollectPushChain walks scan->parent bottom-up,
//   judging each layer purely via capability matrices (clause/operator-function/
//   type) + node-local expression pushability.  It records the highest
//   contiguous pushable boundary WITHOUT mutating the main tree.
// Step 2 (commit, MUTATE): fqCommitPushChain clones the final pushable subtree
//   into pRemoteLogicPlan and docks the original scan back into the local tree.
// ─────────────────────────────────────────────────────────────────────────────

// Read-only pushability judge for a WINDOW(INTERVAL) node — extracted from
// fqConvertInterval steps 2-6 (sliding / agg+interval clause / agg funcs / ts pk
// / interval>0).  No FILL-above check here: the caller's bottom-up walk reaches
// WINDOW only when nothing non-pushable sits below it, and FILL sits ABOVE the
// window so it is handled as its own (non-pushable) layer.  No mutation.
static bool fqWindowIntervalIsPushable(const SWindowLogicNode* pWin, SScanLogicNode* pScan,
                                       const SExtSourceCapability* pCap,
                                       const SExtCapSessionOverlay* pOverlay,
                                       const SPlanContext* pPlanCxt) {
  if (pWin == NULL || pWin->winType != WINDOW_TYPE_INTERVAL) return false;

  if (pPlanCxt != NULL && pPlanCxt->subqueryIndex >= 0 &&
      planIsExternalWindowSubquery(pPlanCxt, pPlanCxt->subqueryIndex)) {
    planDebug("FqWindowIntervalIsPushable: no - subQIdx=%d is EXTERNAL_WINDOW subquery, keep INTERVAL local",
              pPlanCxt->subqueryIndex);
    return false;
  }

  // 1. No explicit FILL clause.  FILL(NONE) has no Fill logic node, but must
  // follow the same local interval plan as all other FILL modes.
  if (intervalWindowHasFill(pWin)) {
    planDebug("FqWindowIntervalIsPushable: no - FILL clause (keep INTERVAL local)");
    return false;
  }

  // 2. No SLIDING (unless sliding == interval, i.e. tumbling) unless pushable.
  if (pWin->sliding != 0 && pWin->sliding != pWin->interval) {
    EExtClauseKind slidingClause = EXT_CLAUSE_INTERVAL_SLIDING;
    if (!fqClauseIsPushable(slidingClause, pCap ? pCap->dialect : -1, pOverlay) ||
        capDisabled(pCap, pOverlay, EXT_DEMOTE_KIND_CLAUSE, &slidingClause)) {
      planDebug("FqWindowIntervalIsPushable: no - SLIDING not pushable");
      return false;
    }
  }

  // 3. AGG + INTERVAL_TUMBLING clauses must be enabled.
  EExtClauseKind aggClause      = EXT_CLAUSE_AGG;
  EExtClauseKind intervalClause = EXT_CLAUSE_INTERVAL_TUMBLING;
  if (!fqClauseIsPushable(aggClause, pCap ? pCap->dialect : -1, pOverlay) ||
      capDisabled(pCap, pOverlay, EXT_DEMOTE_KIND_CLAUSE, &aggClause)) {
    planDebug("FqWindowIntervalIsPushable: no - AGG clause disabled");
    return false;
  }
  if (!fqClauseIsPushable(intervalClause, pCap ? pCap->dialect : -1, pOverlay) ||
      capDisabled(pCap, pOverlay, EXT_DEMOTE_KIND_CLAUSE, &intervalClause)) {
    planDebug("FqWindowIntervalIsPushable: no - INTERVAL_TUMBLING clause disabled");
    return false;
  }

  // 4. All real aggregate functions must be pushable (skip window pseudo-cols).
  SNode* pFuncNode = NULL;
  FOREACH(pFuncNode, pWin->pFuncs) {
    if (nodeType(pFuncNode) != QUERY_NODE_FUNCTION) continue;
    const SFunctionNode* pFunc = (const SFunctionNode*)pFuncNode;
    if (fmIsWindowPseudoColumnFunc(pFunc->funcId) || fmIsPseudoColumnFunc(pFunc->funcId)) continue;
    if (fmIsGroupKeyFunc(pFunc->funcId)) {
      planDebug("FqWindowIntervalIsPushable: skip group-key func name=%s alias=%s",
                pFunc->functionName, pFunc->node.aliasName);
      continue;
    }
    if (!fqFuncIsPushable(pFunc->funcId, pCap ? pCap->dialect : -1, pOverlay)) {
      planDebug("FqWindowIntervalIsPushable: no - func type %d not pushable", (int)pFunc->funcType);
      return false;
    }
  }

  // 5. Source must expose a timestamp primary key.
  if (pScan->pExtTableNode == NULL) return false;
  SExtTableNode* pExtNode = (SExtTableNode*)pScan->pExtTableNode;
  if (pExtNode->pExtMeta == NULL || pExtNode->tsPrimaryColIdx < 0 ||
      pExtNode->tsPrimaryColIdx >= pExtNode->pExtMeta->numOfCols) {
    planDebug("FqWindowIntervalIsPushable: no - no ts primary key in ext meta");
    return false;
  }

  // 6. interval must be positive.
  if (pWin->interval <= 0) {
    planDebug("FqWindowIntervalIsPushable: no - interval<=0");
    return false;
  }
  return true;
}

// Clause-matrix gate: clause enabled for this dialect and not session-demoted.
static bool fqClauseGateOpen(EExtClauseKind clause, const SExtSourceCapability* pCap,
                             const SExtCapSessionOverlay* pOverlay) {
  if (!fqClauseIsPushable(clause, pCap ? pCap->dialect : -1, pOverlay) ||
      capDisabled(pCap, pOverlay, EXT_DEMOTE_KIND_CLAUSE, &clause)) {
    planDebug("FQ-layer: clause %d gated off by capability matrix", (int)clause);
    return false;
  }
  return true;
}

static bool fqJoinNodeClausePushable(const SJoinLogicNode* pJoin, const SExtSourceCapability* pCap,
                                     const SExtCapSessionOverlay* pOverlay) {
  if (!fqClauseGateOpen(EXT_CLAUSE_JOIN, pCap, pOverlay)) {
    planDebug("FqJoinNodeClausePushable: no - main JOIN gate closed join=%p", pJoin);
    return false;
  }

  switch (pJoin->subType) {
    case JOIN_STYPE_SEMI:
      if (!fqClauseGateOpen(EXT_CLAUSE_SEMI_JOIN, pCap, pOverlay)) return false;
      break;
    case JOIN_STYPE_ANTI:
      if (!fqClauseGateOpen(EXT_CLAUSE_ANTI_JOIN, pCap, pOverlay)) return false;
      break;
    case JOIN_STYPE_ASOF:
      if (!fqClauseGateOpen(EXT_CLAUSE_ASOF_JOIN, pCap, pOverlay)) return false;
      break;
    case JOIN_STYPE_WIN:
      if (!fqClauseGateOpen(EXT_CLAUSE_WINDOW_JOIN, pCap, pOverlay)) return false;
      break;
    default:
      break;
  }

  EExtClauseKind joinSubClause = EXT_CLAUSE_MAX;
  switch (pJoin->joinType) {
    case JOIN_TYPE_INNER: joinSubClause = EXT_CLAUSE_INNER_JOIN;      break;
    case JOIN_TYPE_LEFT:  joinSubClause = EXT_CLAUSE_LEFT_JOIN;       break;
    case JOIN_TYPE_RIGHT: joinSubClause = EXT_CLAUSE_RIGHT_JOIN;      break;
    case JOIN_TYPE_FULL:  joinSubClause = EXT_CLAUSE_FULL_OUTER_JOIN; break;
    default:
      planDebug("FqJoinNodeClausePushable: no - unknown joinType=%d join=%p", (int)pJoin->joinType, pJoin);
      return false;
  }

  if (!fqClauseGateOpen(joinSubClause, pCap, pOverlay)) {
    planDebug("FqJoinNodeClausePushable: no - joinSubClause=%d gated off join=%p", (int)joinSubClause, pJoin);
    return false;
  }

  return true;
}

static bool fqJoinLegSubtreePushable(SLogicNode* pNode, const SExtSourceCapability* pCap,
                                     const SExtCapSessionOverlay* pOverlay,
                                     int8_t sourceType, const char* pSourceName) {
  if (NULL == pNode) {
    planDebug("FqJoinLegSubtreePushable: no - NULL subtree");
    return false;
  }

  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType((SNode*)pNode)) {
    SScanLogicNode* pScan = (SScanLogicNode*)pNode;
    if (SCAN_TYPE_EXTERNAL != pScan->scanType) {
      planDebug("FqJoinLegSubtreePushable: no - non-external scan=%p", pScan);
      return false;
    }
    SExtTableNode* pExt = (SExtTableNode*)pScan->pExtTableNode;
    if (NULL == pExt) {
      planDebug("FqJoinLegSubtreePushable: no - scan ext table missing scan=%p", pScan);
      return false;
    }
    if (pExt->sourceType != sourceType ||
        0 != strncmp(pExt->sourceName, pSourceName, TSDB_EXT_SOURCE_NAME_LEN)) {
      planDebug("FqJoinLegSubtreePushable: no - cross-source scan=%p source=%s expected=%s type=%d expectedType=%d",
                pScan, pExt->sourceName, pSourceName, (int)pExt->sourceType, (int)sourceType);
      return false;
    }
    return true;
  }

  if (QUERY_NODE_LOGIC_PLAN_JOIN == nodeType((SNode*)pNode)) {
    SJoinLogicNode* pJoin = (SJoinLogicNode*)pNode;
    if (!fqJoinNodeClausePushable(pJoin, pCap, pOverlay)) {
      planDebug("FqJoinLegSubtreePushable: no - nested join clause gate failed join=%p", pJoin);
      return false;
    }
    if (NULL == pJoin->node.pChildren || LIST_LENGTH(pJoin->node.pChildren) != 2) {
      planDebug("FqJoinLegSubtreePushable: no - nested join child count=%d join=%p",
                pJoin->node.pChildren ? (int)LIST_LENGTH(pJoin->node.pChildren) : 0, pJoin);
      return false;
    }
    SLogicNode* pLeft = (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0);
    SLogicNode* pRight = (SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1);
    return fqJoinLegSubtreePushable(pLeft, pCap, pOverlay, sourceType, pSourceName) &&
           fqJoinLegSubtreePushable(pRight, pCap, pOverlay, sourceType, pSourceName);
  }

  planDebug("FqJoinLegSubtreePushable: no - unsupported nodeType=%d node=%p", (int)nodeType((SNode*)pNode), pNode);
  return false;
}

// Read-only pushability judge for a JOIN node — extracted from fqConvertJoin's
// judgment half (JOIN main gate + subtype + jointype clause matrices, same-source
// external leg subtrees).  No mutation, no error.
static bool fqJoinIsPushable(SLogicNode* pNode, SScanLogicNode* pScan,
                             const SExtSourceCapability* pCap, const SExtCapSessionOverlay* pOverlay,
                             const SPlanContext* pPlanCxt) {
  (void)pScan;
  (void)pPlanCxt;
  const SJoinLogicNode* pJoin = (const SJoinLogicNode*)pNode;

  if (!fqJoinNodeClausePushable(pJoin, pCap, pOverlay)) return false;

  // Structural precondition: exactly two same-source external leg subtrees.
  if (pNode->pChildren == NULL || LIST_LENGTH(pNode->pChildren) != 2) return false;
  SLogicNode* pL = (SLogicNode*)nodesListGetNode(pNode->pChildren, 0);
  SLogicNode* pR = (SLogicNode*)nodesListGetNode(pNode->pChildren, 1);
  SScanLogicNode* pFirstLeaf = fqFindExternalLogicLeafScan(pL);
  if (NULL == pFirstLeaf || NULL == pFirstLeaf->pExtTableNode) {
    planDebug("FqJoinIsPushable: no - left subtree has no external leaf");
    return false;
  }
  SExtTableNode* pBaseExt = (SExtTableNode*)pFirstLeaf->pExtTableNode;
  if (!fqJoinLegSubtreePushable(pL, pCap, pOverlay, pBaseExt->sourceType, pBaseExt->sourceName) ||
      !fqJoinLegSubtreePushable(pR, pCap, pOverlay, pBaseExt->sourceType, pBaseExt->sourceName)) {
    planDebug("FqJoinIsPushable: no - child subtree not pushable/same-source join=%p", pJoin);
    return false;
  }
  return true;
}

// Read-only pushability judge for a PARTITION node (DS §5.3.6 行839-855:
// "转换下推(有条件)").  PARTITION BY expr is pushable as a remote GROUP BY only
// when the partition key is a plain column / scalar expression.  PARTITION BY
// TBNAME (or a TDengine tag column) is pushable ONLY for sources that expose the
// PARTITION_TBNAME capability (InfluxDB: GROUP BY tag1,tag2,…); MySQL/PG have no
// super-table concept so TBNAME stays local.  The "followed only by aggregation,
// no window" condition is enforced structurally by the bottom-up walk: a window
// between SCAN and PARTITION is its own (separately judged) layer, and a remote
// GROUP BY is rendered only when an AGG sits above (renderer uses the parent
// plan's agg funcs when the partition node has none).
static bool fqPartitionIsPushable(SLogicNode* pNode, SScanLogicNode* pScan,
                                  const SExtSourceCapability* pCap, const SExtCapSessionOverlay* pOverlay,
                                  const SPlanContext* pPlanCxt) {
  const SPartitionLogicNode* pPart = (const SPartitionLogicNode*)pNode;
  if (pPart->pPartitionKeys == NULL || LIST_LENGTH(pPart->pPartitionKeys) == 0) return false;

  const SWindowLogicNode* pIntervalWinAbove = NULL;
  for (const SLogicNode* pUp = pPart->node.pParent; pUp != NULL; pUp = pUp->pParent) {
    if (nodeType(pUp) == QUERY_NODE_LOGIC_PLAN_WINDOW) {
      const SWindowLogicNode* pWin = (const SWindowLogicNode*)pUp;
      if (pWin->winType != WINDOW_TYPE_INTERVAL) {
        planDebug("FqPartitionIsPushable: no - non-INTERVAL WINDOW above PARTITION (winType=%d)",
                  (int)pWin->winType);
        return false;
      }
      if (!fqWindowIntervalIsPushable(pWin, pScan, pCap, pOverlay, pPlanCxt)) {
        planDebug("FqPartitionIsPushable: no - INTERVAL WINDOW above PARTITION is not pushable");
        return false;
      }
      pIntervalWinAbove = pWin;
      break;
    }

    if (nodeType(pUp) != QUERY_NODE_LOGIC_PLAN_AGG &&
        nodeType(pUp) != QUERY_NODE_LOGIC_PLAN_PARTITION &&
        nodeType(pUp) != QUERY_NODE_LOGIC_PLAN_PROJECT &&
        nodeType(pUp) != QUERY_NODE_LOGIC_PLAN_SORT) {
      break;
    }
  }

  // PARTITION BY is pushable as a remote GROUP BY when it is followed by
  // aggregation: either the partition node carries its own pAggFuncs, or an AGG
  // sits directly above it.  A bare PARTITION (e.g. "partition by region limit 1")
  // has per-partition semantics no single remote SQL can express -> stays local.
  bool followedByAgg = (pPart->pAggFuncs != NULL && LIST_LENGTH(pPart->pAggFuncs) > 0) ||
                       (pPart->node.pParent != NULL &&
                        nodeType(pPart->node.pParent) == QUERY_NODE_LOGIC_PLAN_AGG) ||
                       (pIntervalWinAbove != NULL);
  if (!followedByAgg) {
    planDebug("FqPartitionIsPushable: no - PARTITION not followed by aggregation (per-partition semantics, local)");
    return false;
  }

  if (pIntervalWinAbove != NULL) {
    planDebug("FqPartitionIsPushable: yes - INTERVAL WINDOW above PARTITION can be merged into one remote GROUP BY");
  }

  bool hasTbname = keysHasTbname(pPart->pPartitionKeys);
  if (hasTbname) {
    // TBNAME / tag-based partition: requires PARTITION_TBNAME (Influx only).
    return fqClauseGateOpen(EXT_CLAUSE_PARTITION_TBNAME, pCap, pOverlay);
  }

  // Plain column / scalar-expression partition keys: gate on PARTITION_COLUMN,
  // then every key expression must itself be renderable to the remote dialect.
  if (!fqClauseGateOpen(EXT_CLAUSE_PARTITION_COLUMN, pCap, pOverlay)) return false;
  SNode* pKey = NULL;
  FOREACH(pKey, pPart->pPartitionKeys) {
    SNode* pExpr = pKey;
    if (nodeType(pExpr) == QUERY_NODE_GROUPING_SET) {
      pExpr = nodesListGetNode(((SGroupingSetNode*)pExpr)->pParameterList, 0);
    }
    // Unwrap the _group_key(col) internal wrapper the planner inserts.
    if (pExpr != NULL && nodeType(pExpr) == QUERY_NODE_FUNCTION) {
      SFunctionNode* pFn = (SFunctionNode*)pExpr;
      if ((pFn->funcType == FUNCTION_TYPE_GROUP_KEY || strcmp(pFn->functionName, "_group_key") == 0) &&
          pFn->pParameterList != NULL && LIST_LENGTH(pFn->pParameterList) > 0) {
        pExpr = nodesListGetNode(pFn->pParameterList, 0);
      }
    }
    if (pExpr == NULL) return false;
    if (!fqExprIsPushable(pExpr, (SScanLogicNode*)pScan, pCap, pOverlay, pPlanCxt)) {
      planDebug("FqPartitionIsPushable: no - partition key not renderable (nodeType=%d)", (int)nodeType(pExpr));
      return false;
    }
  }
  return true;
}

// ── Step-1 layer-pushability: table-driven (node type -> clause + judge) ──────
//
// kFqNodePushTable maps each logic-plan node type (indexed by nodeType minus
// QUERY_NODE_LOGIC_PLAN_SCAN) to:
//   .clause  — the governing clause-matrix kind (the single knob for dynamic
//              capability control).  EXT_CLAUSE_MAX = "no governing clause" ->
//              the clause gate is skipped (used when the judge does its own
//              per-subtype clause check, e.g. WINDOW).
//   .judge   — optional finer judgment (operator/function/type matrices,
//              structural preconditions, window-subtype clause).  NULL = the
//              clause gate alone decides.
// A node type NOT present in the table (zero-initialized -> clause==0==FILTER
// but markedAbsent handled via the `mapped` flag) is treated as NOT pushable.
// Pure capability-matrix driven; no hard-coded pushability outside the matrices.
typedef bool (*FFqLayerJudge)(SLogicNode* pNode, SScanLogicNode* pScan,
                              const SExtSourceCapability* pCap, const SExtCapSessionOverlay* pOverlay,
                              const SPlanContext* pPlanCxt);

#define FQ_RULE_MAX_CLAUSES 3  // max governing clauses a node maps to (AND)

typedef struct SFqNodePushRule {
  bool           mapped;                          // false -> no pushdown mapping (local)
  EExtClauseKind clauses[FQ_RULE_MAX_CLAUSES];    // governing clauses, AND'd; EXT_CLAUSE_MAX terminates/none
  FFqLayerJudge  judge;                           // optional finer judge; NULL -> clause gates decide
} SFqNodePushRule;

// Judge wrappers — uniform signature for the table.  pPlanCxt threads the
// planner's shared subquery registry so REMOTE_* descendants in any clause use
// the already-built child logic plans instead of falling back to AST lookup.
static bool fqJudgeProject(SLogicNode* p, SScanLogicNode* s, const SExtSourceCapability* c,
                           const SExtCapSessionOverlay* o, const SPlanContext* q) {
  return fqProjectIsPushdownable(p, s, c, o, q);
}
static bool fqJudgeSort(SLogicNode* p, SScanLogicNode* s, const SExtSourceCapability* c,
                        const SExtCapSessionOverlay* o, const SPlanContext* q) {
  return fqSortAllKeysPushdownable(p, s, c, o, /*aboveAgg=*/false, q);
}
static bool fqJudgeAgg(SLogicNode* p, SScanLogicNode* s, const SExtSourceCapability* c,
                       const SExtCapSessionOverlay* o, const SPlanContext* q) {
  return fqAggIsPushdownable(p, s, c, o, q);
}
static bool fqJudgeIndef(SLogicNode* p, SScanLogicNode* s, const SExtSourceCapability* c,
                         const SExtCapSessionOverlay* o, const SPlanContext* q) {
  return fqIndefIsPushdownable(p, s, c, o, q);
}
// WINDOW: clause depends on winType; INTERVAL has its own full judge, the rest
// gate on their per-subtype window clause.  (clause column is EXT_CLAUSE_MAX.)
static bool fqJudgeWindow(SLogicNode* p, SScanLogicNode* s, const SExtSourceCapability* c,
                          const SExtCapSessionOverlay* o, const SPlanContext* q) {
  (void)q;
  const SWindowLogicNode* pWin = (const SWindowLogicNode*)p;
  switch (pWin->winType) {
    case WINDOW_TYPE_INTERVAL: return fqWindowIntervalIsPushable(pWin, s, c, o, q);
    case WINDOW_TYPE_STATE:    return fqClauseGateOpen(EXT_CLAUSE_STATE_WINDOW,   c, o);
    case WINDOW_TYPE_SESSION:  return fqClauseGateOpen(EXT_CLAUSE_SESSION_WINDOW, c, o);
    case WINDOW_TYPE_EVENT:    return fqClauseGateOpen(EXT_CLAUSE_EVENT_WINDOW,   c, o);
    case WINDOW_TYPE_COUNT:    return fqClauseGateOpen(EXT_CLAUSE_COUNT_WINDOW,   c, o);
    default: return false;
  }
}

#define FQ_NODE_RULE_IDX(nt) ((int)(nt) - (int)QUERY_NODE_LOGIC_PLAN_SCAN)
#define FQ_NODE_RULE_NUM     (FQ_NODE_RULE_IDX(QUERY_NODE_LOGIC_PLAN_TAG_REF_SOURCE) + 1)

// The single source of truth: logic node type -> pushdown rule.  Any node type
// not given an entry here defaults to {mapped=false} -> not pushable.
// clauses[] lists ALL target capabilities the node's remote form requires (AND);
// EXT_CLAUSE_MAX terminates the list (or means "no outer gate, judge decides").
static const SFqNodePushRule kFqNodePushTable[FQ_NODE_RULE_NUM] = {
  [FQ_NODE_RULE_IDX(QUERY_NODE_LOGIC_PLAN_PROJECT)]         = {true, {EXT_CLAUSE_PROJECTION, EXT_CLAUSE_MAX}, fqJudgeProject},
  [FQ_NODE_RULE_IDX(QUERY_NODE_LOGIC_PLAN_SORT)]            = {true, {EXT_CLAUSE_ORDER,      EXT_CLAUSE_MAX}, fqJudgeSort},
  [FQ_NODE_RULE_IDX(QUERY_NODE_LOGIC_PLAN_AGG)]             = {true, {EXT_CLAUSE_AGG,        EXT_CLAUSE_MAX}, fqJudgeAgg},
  // INDEF (lag/lead/csum/…) -> remote SQL is an OVER() window function over an
  // aggregate-capable engine: requires BOTH AGG and FEAT_WINDOW_FUNCTIONS.
  [FQ_NODE_RULE_IDX(QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC)] = {true, {EXT_CLAUSE_AGG, EXT_CLAUSE_FEAT_WINDOW_FUNCTIONS, EXT_CLAUSE_MAX}, fqJudgeIndef},
  [FQ_NODE_RULE_IDX(QUERY_NODE_LOGIC_PLAN_WINDOW)]          = {true, {EXT_CLAUSE_MAX},                fqJudgeWindow},
  [FQ_NODE_RULE_IDX(QUERY_NODE_LOGIC_PLAN_FILL)]            = {true, {EXT_CLAUSE_FILL,   EXT_CLAUSE_MAX}, NULL},
  [FQ_NODE_RULE_IDX(QUERY_NODE_LOGIC_PLAN_INTERP_FUNC)]     = {true, {EXT_CLAUSE_INTERP, EXT_CLAUSE_MAX}, NULL},
  [FQ_NODE_RULE_IDX(QUERY_NODE_LOGIC_PLAN_PARTITION)]       = {true, {EXT_CLAUSE_MAX}, fqPartitionIsPushable},
  [FQ_NODE_RULE_IDX(QUERY_NODE_LOGIC_PLAN_JOIN)]            = {true, {EXT_CLAUSE_JOIN,   EXT_CLAUSE_MAX}, fqJoinIsPushable},
  // All other logic node types (SCAN/EXCHANGE/MERGE/VNODE_MODIFY/GROUP_CACHE/
  // DYN_QUERY_CTRL/FORECAST/ANALYSIS/VIRTUAL_TABLE_SCAN/…) are left zero ->
  // {mapped=false} -> not pushable (execute locally).
};

// Step-1 single-layer judgment (READ-ONLY).  Looks up the node type in
// kFqNodePushTable: unmapped -> not pushable; else require ALL governing clauses
// (AND) and the optional finer judge.  No mutation, no error.
static bool fqLayerIsPushable(SLogicNode* pNode, SScanLogicNode* pScan,
                              const SExtSourceCapability* pCap, const SExtCapSessionOverlay* pOverlay,
                              const SPlanContext* pPlanCxt) {
  int idx = FQ_NODE_RULE_IDX(nodeType(pNode));
  if (idx < 0 || idx >= FQ_NODE_RULE_NUM) {
    planDebug("FQ-layer: node type=%d out of rule-table range -> not pushable", (int)nodeType(pNode));
    return false;
  }
  const SFqNodePushRule* pRule = &kFqNodePushTable[idx];
  if (!pRule->mapped) {
    planDebug("FQ-layer: node type=%d not mapped -> local", (int)nodeType(pNode));
    return false;
  }
  for (int i = 0; i < FQ_RULE_MAX_CLAUSES; ++i) {
    if (pRule->clauses[i] == EXT_CLAUSE_MAX) break;  // end of list
    if (!fqClauseGateOpen(pRule->clauses[i], pCap, pOverlay)) {
      planDebug("FQ-layer: node type=%d -> NOT pushable (clause %d gated off)", (int)nodeType(pNode), (int)pRule->clauses[i]);
      return false;
    }
  }
  if (pRule->judge != NULL) {
    bool ok = pRule->judge(pNode, pScan, pCap, pOverlay, pPlanCxt);
    planDebug("FQ-layer: node type=%d clause-gates passed, judge=%d -> %s",
              (int)nodeType(pNode), (int)ok, ok ? "PUSHABLE" : "not pushable");
    return ok;
  }
  planDebug("FQ-layer: node type=%d -> PUSHABLE (clause gates only, no finer judge)", (int)nodeType(pNode));
  return true;
}

// Step 1 (COLLECT, READ-ONLY): find the contiguous pushable subtree above pScan
// and return its ROOT (the highest contiguously-pushable node) via *ppBoundary.
//
// The pushable region is a connected bottom subtree of the logic plan (the walk
// stops at the first non-pushable node, so there are no holes).  Returning its
// root lets commit recursively clone the WHOLE subtree — JOIN's two legs and any
// future subquery legs are handled naturally by the recursion, with no linear
// chain / chain[0] / startIdx / JOIN-base special cases.
//
// Pushability is decided SOLELY by the capability matrix (fqLayerIsPushable ->
// kFqNodePushTable + per-node judge) plus a per-node WHERE-pushability check
// (fqExprIsPushable on node.pConditions): a node whose own WHERE is not pushable
// to the remote dialect cannot be pushed (its filter would be lost), so it and
// everything above it stays local.  AGG is exempt (its pConditions is HAVING,
// judged by fqAggIsPushdownable).
//
// *ppBoundary is NULL when nothing (not even the scan) is pushable.
static int32_t fqCollectPushChain(SScanLogicNode* pScan, const SExtSourceCapability* pCap,
                                  const SExtCapSessionOverlay* pOverlay, const SPlanContext* pPlanCxt,
                                  SLogicNode** ppBoundary) {
  *ppBoundary = NULL;
  // The scan itself is the bottom of the pushable subtree.  Its own WHERE
  // (node.pConditions) must be pushable, else nothing pushes.
  if (pScan->node.pConditions != NULL &&
      !fqExprIsPushable(pScan->node.pConditions, pScan, pCap, pOverlay, pPlanCxt)) {
    planDebug("FQ-collect: scan WHERE not pushable - nothing pushes");
    return TSDB_CODE_SUCCESS;
  }
  SLogicNode* pBoundary = (SLogicNode*)pScan;  // scan always in the subtree
  planDebug("FQ-collect: start scan=%p (scan is subtree leaf)", pScan);

  SLogicNode* pChildInChain = (SLogicNode*)pScan;  // lowest node already in the subtree
  SLogicNode* pParent = pScan->node.pParent;
  while (pParent != NULL) {
    if (fqDiagEnabled()) {
      fprintf(stderr,
              "[FQ-DIAG] collect-loop scan=%p childType=%d parentType=%d parent=%p cond=%p\n",
              pScan, (int)nodeType((SNode*)pChildInChain), (int)nodeType((SNode*)pParent), pParent,
              pParent->pConditions);
    }
    if (nodeType(pParent) == QUERY_NODE_LOGIC_PLAN_JOIN) {
      if (!fqLayerIsPushable(pParent, pScan, pCap, pOverlay, pPlanCxt)) {
        planDebug("FQ-collect: stop - JOIN not pushable (capability/structure)");
        break;
      }
      SLogicNode* pOther = NULL;
      SNode* pCh = NULL;
      FOREACH(pCh, pParent->pChildren) {
        if ((SLogicNode*)pCh != pChildInChain) { pOther = (SLogicNode*)pCh; break; }
      }
      if (pOther == NULL) {
        planDebug("FQ-collect: stop - JOIN missing other leg");
        break;
      }
      // JOIN's own post-join WHERE must be pushable (renderJoinLegWhere emits it
      // into the JOIN's combined WHERE); else the filter would be lost.
      if (pParent->pConditions != NULL &&
          !fqExprIsPushable(pParent->pConditions, pScan, pCap, pOverlay, pPlanCxt)) {
        planDebug("FQ-collect: stop - JOIN own WHERE not pushable");
        break;
      }
      pBoundary = pParent;
      planDebug("FQ-collect: pushed JOIN layer (both legs pushable)");
      pChildInChain = pParent;
      pParent = pParent->pParent;
      continue;
    }
    // Single-child layers (SORT/PROJECT/AGG/INDEF/WINDOW/PARTITION).
    bool layerPushable = false;
    if (nodeType(pParent) == QUERY_NODE_LOGIC_PLAN_SORT) {
      bool aboveAgg = fqSortAboveAggInChain(pChildInChain);
      planDebug("FQ-collect: evaluate SORT layer=%p child=%p aboveAgg=%d childType=%d",
                pParent, pChildInChain, (int)aboveAgg, (int)nodeType(pChildInChain));
      layerPushable = fqSortAllKeysPushdownable(pParent, pScan, pCap, pOverlay, aboveAgg, pPlanCxt);
      planDebug("FQ-layer: SORT special-judge aboveAgg=%d result=%d", (int)aboveAgg, (int)layerPushable);
    } else {
      layerPushable = fqLayerIsPushable(pParent, pScan, pCap, pOverlay, pPlanCxt);
    }
    if (!layerPushable) {
      planDebug("FQ-collect: stop - layer type=%d not pushable", (int)nodeType(pParent));
      break;
    }
    // A node's own WHERE must be pushable to push the node.  AGG's pConditions is
    // HAVING (judged by fqAggIsPushdownable), so exempt it here.
    if (pParent->pConditions != NULL && nodeType(pParent) != QUERY_NODE_LOGIC_PLAN_AGG &&
        !fqExprIsPushable(pParent->pConditions, pScan, pCap, pOverlay, pPlanCxt)) {
      planDebug("FQ-collect: stop - layer type=%d WHERE not pushable", (int)nodeType(pParent));
      break;
    }
    pBoundary = pParent;
    planDebug("FQ-collect: pushed layer type=%d", (int)nodeType(pParent));
    pChildInChain = pParent;
    pParent = pParent->pParent;
  }
  *ppBoundary = pBoundary;
  planDebug("FQ-collect: done, boundary type=%d", (int)nodeType(pBoundary));
  return TSDB_CODE_SUCCESS;
}

typedef struct SFqMarkSubqueryCtx {
  SPlanContext* pPlanCxt;
  int32_t       ownerSubQIdx;
  int32_t       code;
} SFqMarkSubqueryCtx;

static bool fqGetRemoteSubQIdx(SNode* pNode, int32_t* pSubQIdx) {
  switch (nodeType(pNode)) {
    case QUERY_NODE_REMOTE_VALUE:
      *pSubQIdx = ((SRemoteValueNode*)pNode)->subQIdx;
      return true;
    case QUERY_NODE_REMOTE_VALUE_LIST:
      *pSubQIdx = ((SRemoteValueListNode*)pNode)->subQIdx;
      return true;
    case QUERY_NODE_REMOTE_ROW:
      *pSubQIdx = ((SRemoteRowNode*)pNode)->subQIdx;
      return true;
    case QUERY_NODE_REMOTE_ZERO_ROWS:
      *pSubQIdx = ((SRemoteZeroRowsNode*)pNode)->subQIdx;
      return true;
    default:
      return false;
  }
}

static EDealRes fqMarkCommittedRemoteSubqueryWalker(SNode* pNode, void* pContext) {
  SFqMarkSubqueryCtx* pCtx = (SFqMarkSubqueryCtx*)pContext;
  int32_t             subQIdx = -1;
  if (!fqGetRemoteSubQIdx(pNode, &subQIdx)) {
    return DEAL_RES_CONTINUE;
  }

  planDebug("FQ-mark: ownerSubQIdx:%d absorbed childSubQIdx:%d remoteNodeType:%d", pCtx->ownerSubQIdx, subQIdx,
            (int)nodeType(pNode));
  pCtx->code = planMarkSubqueryPushedDown(pCtx->pPlanCxt, subQIdx);
  return (TSDB_CODE_SUCCESS == pCtx->code) ? DEAL_RES_IGNORE_CHILD : DEAL_RES_END;
}

static void fqWalkLogicTreeForMark(SLogicNode* pNode, SFqMarkSubqueryCtx* pCtx) {
  if (NULL == pNode || TSDB_CODE_SUCCESS != pCtx->code) {
    return;
  }

  fqWalkLogicExprFields(pNode, fqMarkCommittedRemoteSubqueryWalker, pCtx);
  if (TSDB_CODE_SUCCESS != pCtx->code) {
    return;
  }

  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    SScanLogicNode* pScan = (SScanLogicNode*)pNode;
    if (pScan->pRemoteLogicPlan != NULL) {
      fqWalkLogicTreeForMark((SLogicNode*)pScan->pRemoteLogicPlan, pCtx);
      if (TSDB_CODE_SUCCESS != pCtx->code) {
        return;
      }
    }
  }

  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    fqWalkLogicTreeForMark((SLogicNode*)pChild, pCtx);
    if (TSDB_CODE_SUCCESS != pCtx->code) {
      return;
    }
  }
}

static int32_t fqMarkCommittedRemoteSubqueries(SPlanContext* pPlanCxt, SScanLogicNode* pScan) {
  if (NULL == pPlanCxt || NULL == pScan || NULL == pScan->pRemoteLogicPlan) {
    return TSDB_CODE_SUCCESS;
  }

  SFqMarkSubqueryCtx ctx = {
      .pPlanCxt = pPlanCxt,
      .ownerSubQIdx = pPlanCxt->subqueryIndex,
      .code = TSDB_CODE_SUCCESS,
  };
  planDebug("FQ-mark: start ownerSubQIdx:%d remoteRootType:%d", ctx.ownerSubQIdx,
            (int)nodeType(pScan->pRemoteLogicPlan));
  fqWalkLogicTreeForMark((SLogicNode*)pScan->pRemoteLogicPlan, &ctx);
  if (TSDB_CODE_SUCCESS == ctx.code) {
    planDebug("FQ-mark: done ownerSubQIdx:%d", ctx.ownerSubQIdx);
  }
  return ctx.code;
}

// Step 3 (COMMIT, mutates main tree): graft the collected pushable chain into
// pScan->pRemoteLogicPlan, with a CLONED logic SCAN as the remote leaf (the
// permanent base — this is the fix for the legacy headless-chain bug where the
// real SCAN was severed via nodesClearList and a physical leaf re-grafted).
//
// pChain holds the pushable nodes bottom-up: pChain[0] is the lowest (its child
// is the main-tree SCAN), pChain[n-1] is the topmost.  After commit:
//   - main tree: the SCAN replaces pTopmost's slot; pScan->node.pTargets is set
//     to pTopmost's output columns (local docking for the executor).
//   - pRemoteLogicPlan = pTopmost -> … -> pBottommost -> clonedScanLeaf.
//
// This generic relocation covers SORT/PROJECT/AGG/INDEF chain nodes.  Nodes that
// require transformation (WINDOW->remote AGG, PARTITION->GROUP BY) keep their own
// convert helpers, invoked before this step; their product already sits in
// pRemoteLogicPlan and the chain here only carries the wrappers above them.
//
// fqRewriteRemoteTbnameTags — for a remote PARTITION/AGG node whose grouping
// keys contain TBNAME on a tag-aware source (InfluxDB), substitute the real tag
// columns for TBNAME and add those tag columns to the remote leaf SCAN's
// pScanCols (so the remote SELECT fetches them).  Operates on the CLONED remote
// node + remote base subtree — no main-tree mutation.  No-op for sources without
// tag columns (the partition collapses to a single group) or non-TBNAME keys.
static int32_t fqRewriteRemoteTbnameTags(SLogicNode* pRemoteNode, SNode* pRemoteBase,
                                         const SExtTableMeta* pMeta) {
  if (pMeta == NULL || pMeta->pCols == NULL || pMeta->numOfCols <= 0) return TSDB_CODE_SUCCESS;

  // The grouping keys live in pPartitionKeys (PARTITION) or pGroupKeys (AGG).
  SNodeList** ppKeys = NULL;
  if (nodeType(pRemoteNode) == QUERY_NODE_LOGIC_PLAN_PARTITION) {
    ppKeys = &((SPartitionLogicNode*)pRemoteNode)->pPartitionKeys;
  } else if (nodeType(pRemoteNode) == QUERY_NODE_LOGIC_PLAN_AGG) {
    ppKeys = &((SAggLogicNode*)pRemoteNode)->pGroupKeys;
  } else {
    return TSDB_CODE_SUCCESS;
  }
  if (!keysHasTbname(*ppKeys)) return TSDB_CODE_SUCCESS;

  // Locate the remote leaf SCAN (bottom of the remote base subtree).
  SLogicNode* pLeaf = (SLogicNode*)pRemoteBase;
  while (pLeaf != NULL && nodeType(pLeaf) != QUERY_NODE_LOGIC_PLAN_SCAN) {
    SNodeList* pCh = pLeaf->pChildren;
    pLeaf = (pCh && LIST_LENGTH(pCh) > 0) ? (SLogicNode*)nodesListGetNode(pCh, 0) : NULL;
  }
  if (pLeaf == NULL) return TSDB_CODE_SUCCESS;
  SScanLogicNode* pLeafScan = (SScanLogicNode*)pLeaf;

  bool hasTags = false;
  for (int32_t i = 0; i < pMeta->numOfCols; i++) {
    if (pMeta->pCols[i].isTag) { hasTags = true; break; }
  }

  int32_t code = TSDB_CODE_SUCCESS;
  if (!hasTags) {
    // Tagless: drop TBNAME keys entirely (single-group semantics).
    SNodeList* pNewKeys = NULL;
    SNode* pKey = NULL;
    FOREACH(pKey, *ppKeys) {
      SNode* pActual = pKey;
      if (QUERY_NODE_GROUPING_SET == nodeType(pActual))
        pActual = nodesListGetNode(((SGroupingSetNode*)pActual)->pParameterList, 0);
      bool isTb = (pActual && ((nodeType(pActual) == QUERY_NODE_FUNCTION && ((SFunctionNode*)pActual)->funcType == FUNCTION_TYPE_TBNAME) ||
                   (nodeType(pActual) == QUERY_NODE_COLUMN && ((SColumnNode*)pActual)->colType == COLUMN_TYPE_TBNAME)));
      if (isTb) continue;
      SNode* pClone = NULL;
      code = nodesCloneNode(pKey, &pClone);
      if (TSDB_CODE_SUCCESS != code) { nodesDestroyList(pNewKeys); return code; }
      code = nodesListMakeStrictAppend(&pNewKeys, pClone);
      if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode(pClone); nodesDestroyList(pNewKeys); return code; }
    }
    nodesDestroyList(*ppKeys);
    *ppKeys = pNewKeys;
    planDebug("FQ-tagRewrite: tagless source - dropped TBNAME from remote grouping keys");
    return TSDB_CODE_SUCCESS;
  }

  // Tag-aware: add tag columns to the leaf pScanCols (skip dups), then rebuild
  // grouping keys: keep non-TBNAME keys, append tag-column keys for TBNAME.
  for (int32_t i = pMeta->numOfCols - 1; i >= 0; i--) {
    if (!pMeta->pCols[i].isTag) continue;
    const char* tagColName = pMeta->pCols[i].colName;
    bool found = false;
    SNode* pE = NULL;
    FOREACH(pE, pLeafScan->pScanCols) {
      if (nodeType(pE) == QUERY_NODE_COLUMN && strcmp(((SColumnNode*)pE)->colName, tagColName) == 0) { found = true; break; }
    }
    if (found) continue;
    SColumnNode* pTagCol = NULL;
    code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pTagCol);
    if (TSDB_CODE_SUCCESS != code) return code;
    tstrncpy(pTagCol->colName, tagColName, TSDB_COL_NAME_LEN);
    tstrncpy(pTagCol->node.aliasName, tagColName, TSDB_COL_NAME_LEN);
    pTagCol->node.resType.type  = TSDB_DATA_TYPE_VARCHAR;
    pTagCol->node.resType.bytes = TSDB_MAX_BINARY_LEN + VARSTR_HEADER_SIZE;
    code = nodesListMakePushFront(&pLeafScan->pScanCols, (SNode*)pTagCol);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pTagCol); return code; }
  }

  SNodeList* pNewKeys = NULL;
  SNode* pKey = NULL;
  FOREACH(pKey, *ppKeys) {
    SNode* pActual = pKey;
    if (QUERY_NODE_GROUPING_SET == nodeType(pActual))
      pActual = nodesListGetNode(((SGroupingSetNode*)pActual)->pParameterList, 0);
    bool isTb = (pActual && ((nodeType(pActual) == QUERY_NODE_FUNCTION && ((SFunctionNode*)pActual)->funcType == FUNCTION_TYPE_TBNAME) ||
                 (nodeType(pActual) == QUERY_NODE_COLUMN && ((SColumnNode*)pActual)->colType == COLUMN_TYPE_TBNAME)));
    if (isTb) continue;
    SNode* pClone = NULL;
    code = nodesCloneNode(pKey, &pClone);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyList(pNewKeys); return code; }
    code = nodesListMakeStrictAppend(&pNewKeys, pClone);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode(pClone); nodesDestroyList(pNewKeys); return code; }
  }
  for (int32_t i = 0; i < pMeta->numOfCols; i++) {
    if (!pMeta->pCols[i].isTag) continue;
    SColumnNode* pTagKey = NULL;
    code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pTagKey);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyList(pNewKeys); return code; }
    tstrncpy(pTagKey->colName, pMeta->pCols[i].colName, TSDB_COL_NAME_LEN);
    tstrncpy(pTagKey->node.aliasName, pMeta->pCols[i].colName, TSDB_COL_NAME_LEN);
    pTagKey->node.resType.type  = TSDB_DATA_TYPE_VARCHAR;
    pTagKey->node.resType.bytes = TSDB_MAX_BINARY_LEN + VARSTR_HEADER_SIZE;
    code = nodesListMakeStrictAppend(&pNewKeys, (SNode*)pTagKey);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pTagKey); nodesDestroyList(pNewKeys); return code; }
  }
  nodesDestroyList(*ppKeys);
  *ppKeys = pNewKeys;
  planDebug("FQ-tagRewrite: tag-aware source - TBNAME->tag-cols in remote grouping keys + leaf pScanCols");
  return TSDB_CODE_SUCCESS;
}

// Count SCAN leaves in a subtree (used to detect multi-leg JOIN shapes).
static SScanLogicNode* fqFindExternalLogicLeafScan(SLogicNode* pNode) {
  if (NULL == pNode) {
    return NULL;
  }

  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType((SNode*)pNode)) {
    SScanLogicNode* pScan = (SScanLogicNode*)pNode;
    if (SCAN_TYPE_EXTERNAL == pScan->scanType) {
      return pScan;
    }
  }

  if (NULL == pNode->pChildren) {
    return NULL;
  }

  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    SScanLogicNode* pFound = fqFindExternalLogicLeafScan((SLogicNode*)pChild);
    if (pFound != NULL) {
      return pFound;
    }
  }

  return NULL;
}

static SLogicNode* fqFirstChild(const SLogicNode* pNode) {
  if (pNode == NULL || pNode->pChildren == NULL || LIST_LENGTH(pNode->pChildren) == 0) return NULL;
  return (SLogicNode*)nodesListGetNode(pNode->pChildren, 0);
}

static void fqPlanDebugAppendIfPresent(char* pBuf, int32_t bufLen, int32_t* pPos,
                                       const char* pLabel, const char* pValue) {
  if (NULL == pBuf || NULL == pPos || NULL == pLabel || NULL == pValue || '\0' == pValue[0]) {
    return;
  }

  int32_t pos = *pPos;
  if (pos < 0 || pos >= bufLen - 1) {
    return;
  }

  int32_t written = snprintf(pBuf + pos, (size_t)(bufLen - pos), " %s=%s", pLabel, pValue);
  if (written > 0) {
    *pPos += written;
  }
}

static void fqPlanDebugDescribeExprNode(const SNode* pExpr, char* pBuf, int32_t bufLen) {
  if (NULL == pBuf || bufLen <= 0) {
    return;
  }

  pBuf[0] = '\0';
  if (NULL == pExpr) {
    tstrncpy(pBuf, "expr=(null)", bufLen);
    return;
  }

  int32_t pos = snprintf(pBuf, (size_t)bufLen, "nodeType=%s(%d)",
                         nodesNodeName(nodeType((SNode*)pExpr)), (int)nodeType((SNode*)pExpr));
  if (pos < 0 || pos >= bufLen - 1) {
    return;
  }

  if (nodesIsExprNode((SNode*)pExpr)) {
    const SExprNode* pExprNode = (const SExprNode*)pExpr;
    fqPlanDebugAppendIfPresent(pBuf, bufLen, &pos, "alias", pExprNode->aliasName);
    fqPlanDebugAppendIfPresent(pBuf, bufLen, &pos, "userAlias", pExprNode->userAlias);
  }

  switch (nodeType((SNode*)pExpr)) {
    case QUERY_NODE_COLUMN: {
      const SColumnNode* pCol = (const SColumnNode*)pExpr;
      fqPlanDebugAppendIfPresent(pBuf, bufLen, &pos, "col", pCol->colName);
      fqPlanDebugAppendIfPresent(pBuf, bufLen, &pos, "tblAlias", pCol->tableAlias);
      char colIdBuf[64] = {0};
      snprintf(colIdBuf, sizeof(colIdBuf), "%d", (int)pCol->colId);
      fqPlanDebugAppendIfPresent(pBuf, bufLen, &pos, "colId", colIdBuf);
      break;
    }
    case QUERY_NODE_FUNCTION: {
      const SFunctionNode* pFunc = (const SFunctionNode*)pExpr;
      fqPlanDebugAppendIfPresent(pBuf, bufLen, &pos, "func", pFunc->functionName);
      char argcBuf[64] = {0};
      snprintf(argcBuf, sizeof(argcBuf), "%d",
               pFunc->pParameterList ? LIST_LENGTH(pFunc->pParameterList) : 0);
      fqPlanDebugAppendIfPresent(pBuf, bufLen, &pos, "argc", argcBuf);
      break;
    }
    case QUERY_NODE_TARGET: {
      const STargetNode* pTarget = (const STargetNode*)pExpr;
      if (NULL != pTarget->pExpr) {
        char inner[512] = {0};
        fqPlanDebugDescribeExprNode(pTarget->pExpr, inner, sizeof(inner));
        fqPlanDebugAppendIfPresent(pBuf, bufLen, &pos, "targetExpr", inner);
      }
      break;
    }
    default:
      break;
  }
}

static void fqPlanDebugLogExprList(const char* pTag, const SNodeList* pExprs) {
  int32_t exprCnt = pExprs ? LIST_LENGTH((SNodeList*)pExprs) : 0;
  planDebug("FQ-DIAG: %s exprCount=%d", pTag ? pTag : "(null)", exprCnt);
  if (NULL == pExprs) {
    return;
  }

  int32_t idx = 0;
  SNode*  pExpr = NULL;
  FOREACH(pExpr, pExprs) {
    char desc[512] = {0};
    fqPlanDebugDescribeExprNode(pExpr, desc, sizeof(desc));
    planDebug("FQ-DIAG: %s[%d] %s", pTag ? pTag : "(null)", idx, desc);
    ++idx;
  }
}

static void fqPlanDebugLogNodeOutputs(const char* pTag, const SLogicNode* pNode) {
  if (NULL == pNode) {
    planDebug("FQ-DIAG: %s node=(null)", pTag ? pTag : "(null)");
    return;
  }

  planDebug("FQ-DIAG: %s node=%p type=%s(%d) children=%d targets=%d cond=%p limit=%p",
            pTag ? pTag : "(null)", pNode, nodesNodeName(nodeType((SNode*)pNode)),
            (int)nodeType((SNode*)pNode), pNode->pChildren ? LIST_LENGTH(pNode->pChildren) : 0,
            pNode->pTargets ? LIST_LENGTH(pNode->pTargets) : 0, pNode->pConditions, pNode->pLimit);
  fqPlanDebugLogExprList("node.targets", pNode->pTargets);

  if (QUERY_NODE_LOGIC_PLAN_PROJECT == nodeType((SNode*)pNode)) {
    fqPlanDebugLogExprList("node.project.projections", ((const SProjectLogicNode*)pNode)->pProjections);
  } else if (QUERY_NODE_LOGIC_PLAN_SORT == nodeType((SNode*)pNode)) {
    fqPlanDebugLogExprList("node.sort.sortKeys", ((const SSortLogicNode*)pNode)->pSortKeys);
  } else if (QUERY_NODE_LOGIC_PLAN_AGG == nodeType((SNode*)pNode)) {
    const SAggLogicNode* pAgg = (const SAggLogicNode*)pNode;
    fqPlanDebugLogExprList("node.agg.groupKeys", pAgg->pGroupKeys);
    fqPlanDebugLogExprList("node.agg.aggFuncs", pAgg->pAggFuncs);
  } else if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType((SNode*)pNode)) {
    const SScanLogicNode* pScan = (const SScanLogicNode*)pNode;
    fqPlanDebugLogExprList("node.scan.scanCols", pScan->pScanCols);
    fqPlanDebugLogExprList("node.scan.groupTags", pScan->pGroupTags);
    fqPlanDebugLogExprList("node.scan.pseudoCols", pScan->pScanPseudoCols);
  }
}

static void partTagsDebugLogConsumerChain(const SAggLogicNode* pAgg) {
  if (NULL == pAgg) {
    return;
  }

  const SLogicNode* pCurr = &pAgg->node;
  int32_t           depth = 0;
  while (NULL != pCurr && depth < 4) {
    char tag[64] = {0};
    snprintf(tag, sizeof(tag), "partTags.chain[%d]", depth);
    fqPlanDebugLogNodeOutputs(tag, pCurr);
    pCurr = pCurr->pParent;
    ++depth;
  }
}

static bool fqNonEmptyNameEquals(const char* pLeft, const char* pRight) {
  return NULL != pLeft && NULL != pRight && '\0' != pLeft[0] && '\0' != pRight[0] && 0 == strcmp(pLeft, pRight);
}

static bool fqIsParserStmtAlias(const char* pTableAlias) {
  return NULL != pTableAlias && pTableAlias[0] == '0' && pTableAlias[1] == 'x' && pTableAlias[2] != '\0';
}

static bool fqIsQuantifyWrapperAgg(const SAggLogicNode* pAgg) {
  if (NULL == pAgg || (pAgg->pGroupKeys != NULL && LIST_LENGTH(pAgg->pGroupKeys) > 0) ||
      NULL == pAgg->pAggFuncs || LIST_LENGTH(pAgg->pAggFuncs) != 2 || NULL == pAgg->node.pChildren ||
      LIST_LENGTH(pAgg->node.pChildren) != 1) {
    return false;
  }

  bool   hasHasNull = false;
  bool   hasMinOrMax = false;
  SNode* pFuncNode = NULL;
  FOREACH(pFuncNode, pAgg->pAggFuncs) {
    if (QUERY_NODE_FUNCTION != nodeType(pFuncNode)) {
      return false;
    }

    const SFunctionNode* pFunc = (const SFunctionNode*)pFuncNode;
    if (0 == strcmp(pFunc->functionName, "_has_null")) {
      hasHasNull = true;
    } else if (0 == strcasecmp(pFunc->functionName, "min") || 0 == strcasecmp(pFunc->functionName, "max")) {
      hasMinOrMax = true;
    } else {
      return false;
    }
  }

  return hasHasNull && hasMinOrMax;
}

static bool fqQuantifyWrapperConsumesFedScan(const SAggLogicNode* pAgg, const SScanLogicNode* pScan) {
  if (NULL == pAgg || NULL == pScan) {
    return false;
  }

  SLogicNode* pCurr = fqFirstChild(&pAgg->node);
  while (NULL != pCurr && pCurr != (const SLogicNode*)pScan) {
    if (QUERY_NODE_LOGIC_PLAN_PROJECT != nodeType(pCurr) || NULL == pCurr->pChildren ||
        1 != LIST_LENGTH(pCurr->pChildren)) {
      return false;
    }
    pCurr = fqFirstChild(pCurr);
  }

  return pCurr == (const SLogicNode*)pScan;
}

static bool fqQuantifyWrapperColMatchesTarget(const SColumnNode* pCol, const SColumnNode* pTarget) {
  if (NULL == pCol || NULL == pTarget) {
    return false;
  }

  return fqNonEmptyNameEquals(pCol->colName, pTarget->colName) ||
         fqNonEmptyNameEquals(pCol->colName, pTarget->node.aliasName) ||
         fqNonEmptyNameEquals(pCol->node.aliasName, pTarget->colName) ||
         fqNonEmptyNameEquals(pCol->node.aliasName, pTarget->node.aliasName) ||
         fqNonEmptyNameEquals(pCol->node.userAlias, pTarget->node.userAlias) ||
         fqNonEmptyNameEquals(pCol->node.userAlias, pTarget->colName) ||
         fqNonEmptyNameEquals(pCol->node.userAlias, pTarget->node.aliasName);
}

static const SColumnNode* fqFindQuantifyWrapperTarget(const SNodeList* pTargets, const SColumnNode* pCol) {
  if (NULL == pTargets || NULL == pCol) {
    return NULL;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pTargets) {
    if (QUERY_NODE_COLUMN != nodeType(pNode)) {
      continue;
    }

    const SColumnNode* pTarget = (const SColumnNode*)pNode;
    if (('\0' != pTarget->tableAlias[0] || '\0' != pTarget->tableName[0]) ||
        !fqQuantifyWrapperColMatchesTarget(pCol, pTarget)) {
      continue;
    }

    return pTarget;
  }

  return NULL;
}

typedef struct SFqNormalizeQuantifyWrapperCtx {
  const SNodeList* pTargets;
  int32_t          rewriteCount;
} SFqNormalizeQuantifyWrapperCtx;

static EDealRes fqNormalizeQuantifyWrapperAggParam(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN != nodeType(pNode)) {
    return DEAL_RES_CONTINUE;
  }

  SFqNormalizeQuantifyWrapperCtx* pCtx = (SFqNormalizeQuantifyWrapperCtx*)pContext;
  SColumnNode*                    pCol = (SColumnNode*)pNode;
  if (!fqIsParserStmtAlias(pCol->tableAlias) || '\0' != pCol->tableName[0] || pCol->hasRef || pCol->hasDep) {
    return DEAL_RES_IGNORE_CHILD;
  }

  const SColumnNode* pTarget = fqFindQuantifyWrapperTarget(pCtx->pTargets, pCol);
  if (NULL == pTarget) {
    return DEAL_RES_IGNORE_CHILD;
  }

  pCol->dbName[0] = '\0';
  pCol->tableName[0] = '\0';
  pCol->tableAlias[0] = '\0';
  tstrncpy(pCol->colName, pTarget->colName, sizeof(pCol->colName));
  tstrncpy(pCol->node.aliasName, pTarget->node.aliasName, sizeof(pCol->node.aliasName));
  tstrncpy(pCol->node.userAlias, pTarget->node.userAlias, sizeof(pCol->node.userAlias));
  ++pCtx->rewriteCount;

  planDebug("FQ-quantifyWrapper: normalized local agg param to current scope col=%s alias=%s userAlias=%s",
            pCol->colName, pCol->node.aliasName, pCol->node.userAlias);
  return DEAL_RES_IGNORE_CHILD;
}

static int32_t fqNormalizeLocalQuantifyWrapperAggs(SScanLogicNode* pScan) {
  if (NULL == pScan) {
    return TSDB_CODE_SUCCESS;
  }

  for (SLogicNode* pUp = pScan->node.pParent; pUp != NULL; pUp = pUp->pParent) {
    if (QUERY_NODE_LOGIC_PLAN_AGG != nodeType(pUp)) {
      continue;
    }

    SAggLogicNode* pAgg = (SAggLogicNode*)pUp;
    if (!fqIsQuantifyWrapperAgg(pAgg) || !fqQuantifyWrapperConsumesFedScan(pAgg, pScan)) {
      continue;
    }

    SLogicNode* pChild = fqFirstChild(pUp);
    const SNodeList* pTargets = (NULL != pChild && NULL != pChild->pTargets) ? pChild->pTargets : pScan->node.pTargets;
    if (NULL == pTargets || LIST_LENGTH(pTargets) == 0) {
      continue;
    }

    SFqNormalizeQuantifyWrapperCtx cxt = {.pTargets = pTargets, .rewriteCount = 0};
    nodesWalkExprs(pAgg->pAggFuncs, fqNormalizeQuantifyWrapperAggParam, &cxt);
    if (cxt.rewriteCount > 0) {
      planDebug("FQ-quantifyWrapper: normalized %d local agg param refs agg=%p childType=%d",
                cxt.rewriteCount, pAgg, pChild ? (int)nodeType(pChild) : -1);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static SScanLogicNode* fqGetWholePushBranchScan(SLogicNode* pNode);

static int32_t fqBuildUnionBranchRemoteSubtree(const SLogicNode* pBranchNode,
                                               SLogicNode** ppRemoteBranch) {
  if (NULL == pBranchNode || NULL == ppRemoteBranch) {
    return TSDB_CODE_INVALID_PARA;
  }

  *ppRemoteBranch = NULL;

  SLogicNode* pBranchClone = NULL;
  int32_t code = nodesCloneNode((SNode*)pBranchNode, (SNode**)&pBranchClone);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  fqPlanDebugLogNodeOutputs("unionBranch.local.orig", pBranchNode);
  fqPlanDebugLogNodeOutputs("unionBranch.clone.beforeRemoteBase", pBranchClone);

  SScanLogicNode* pOrigScan = fqGetWholePushBranchScan((SLogicNode*)pBranchNode);
  if (NULL == pOrigScan) {
    planError("FQ-UNION: build branch remote subtree failed, branchType=%d is not a whole-push scan branch",
              (int)nodeType((SNode*)pBranchNode));
    nodesDestroyNode((SNode*)pBranchClone);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  if (pOrigScan->pRemoteLogicPlan != NULL) {
    SScanLogicNode* pCloneLeafScan = fqFindExternalLogicLeafScan(pBranchClone);
    if (NULL == pCloneLeafScan) {
      planError("FQ-UNION: cloned branch missing external scan leaf, branchType=%d cloneType=%d remoteRootType=%d",
                (int)nodeType((SNode*)pBranchNode), (int)nodeType((SNode*)pBranchClone),
                (int)nodeType(pOrigScan->pRemoteLogicPlan));
      nodesDestroyNode((SNode*)pBranchClone);
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }

    SLogicNode* pRemoteBase = NULL;
    code = nodesCloneNode(pOrigScan->pRemoteLogicPlan, (SNode**)&pRemoteBase);
    if (TSDB_CODE_SUCCESS != code) {
      planError("FQ-UNION: clone branch remote base failed, branchType=%d remoteRootType=%d code=0x%x",
                (int)nodeType((SNode*)pBranchNode), (int)nodeType(pOrigScan->pRemoteLogicPlan), code);
      nodesDestroyNode((SNode*)pBranchClone);
      return code;
    }
    pRemoteBase->pParent = NULL;

    SLogicNode* pLeafParent = pCloneLeafScan->node.pParent;
    if (NULL == pLeafParent) {
      nodesDestroyNode((SNode*)pBranchClone);
      pBranchClone = pRemoteBase;
    } else {
      SNode* pChildNode = NULL;
      bool replaced = false;
      FOREACH(pChildNode, pLeafParent->pChildren) {
        if (pChildNode == (SNode*)pCloneLeafScan) {
          cell->pNode = (SNode*)pRemoteBase;
          pRemoteBase->pParent = pLeafParent;
          replaced = true;
          break;
        }
      }
      if (!replaced) {
        planError("FQ-UNION: cannot replace cloned leaf scan in branch subtree, branchType=%d parentType=%d",
                  (int)nodeType((SNode*)pBranchNode), (int)nodeType((SNode*)pLeafParent));
        nodesDestroyNode((SNode*)pRemoteBase);
        nodesDestroyNode((SNode*)pBranchClone);
        return TSDB_CODE_PLAN_INTERNAL_ERROR;
      }

      pCloneLeafScan->node.pParent = NULL;
      nodesDestroyNode((SNode*)pCloneLeafScan);
    }
  }

  fqPlanDebugLogNodeOutputs("unionBranch.clone.afterRemoteBase", pBranchClone);

  code = fqAlignRemoteRootTargetsForRender(pBranchClone);
  if (TSDB_CODE_SUCCESS != code) {
    planError("FQ-UNION: align branch remote subtree targets failed, branchType=%d cloneType=%d code=0x%x",
              (int)nodeType((SNode*)pBranchNode), (int)nodeType((SNode*)pBranchClone), code);
    nodesDestroyNode((SNode*)pBranchClone);
    return code;
  }

  fqPlanDebugLogNodeOutputs("unionBranch.clone.afterAlign", pBranchClone);

  *ppRemoteBranch = pBranchClone;
  return TSDB_CODE_SUCCESS;
}

static int32_t fqAlignRemoteRootTargetsForRender(SLogicNode* pRemoteRoot) {
  if (pRemoteRoot == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  fqPlanDebugLogNodeOutputs("alignRemoteRoot.before", pRemoteRoot);

  SNodesRemoteSelectOutputs outputs = {0};
  int32_t code = nodesCollectRemoteQuerySelectOutputs(pRemoteRoot, &outputs);
  if (TSDB_CODE_SUCCESS != code) {
    planError("FQ-remoteTargets: collect shared outputs failed rootType=%d code=0x%x",
              (int)nodeType(pRemoteRoot), code);
    return code;
  }

  if (outputs.pSelectExprs == NULL || LIST_LENGTH(outputs.pSelectExprs) == 0) {
    planError("FQ-remoteTargets: collector returned success but outputs are empty rootType=%d source=%d oldTargets=%d",
              (int)nodeType(pRemoteRoot), (int)outputs.source,
              pRemoteRoot->pTargets ? LIST_LENGTH(pRemoteRoot->pTargets) : 0);
    nodesDestroyRemoteSelectOutputs(&outputs);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  {
    int32_t idx = 0;
    SNode* pOut = NULL;
    FOREACH(pOut, outputs.pSelectExprs) {
      const SNode* pExpr = pOut;
      if (QUERY_NODE_TARGET == nodeType(pExpr)) {
        pExpr = ((const STargetNode*)pExpr)->pExpr;
      }
      planDebug("FQ-remoteTargets: collected output idx=%d rootType=%d exprType=%d alias=%s userAlias=%s",
                idx, (int)nodeType(pRemoteRoot), pExpr ? (int)nodeType(pExpr) : -1,
                (pExpr != NULL && nodesIsExprNode((SNode*)pExpr)) ? ((SExprNode*)pExpr)->aliasName : "",
                (pExpr != NULL && nodesIsExprNode((SNode*)pExpr)) ? ((SExprNode*)pExpr)->userAlias : "");
      ++idx;
    }
  }

  SNodeList* pNewTargets = NULL;
  code = createColumnByRemoteOutputExprs(outputs.pSelectExprs, &pNewTargets);
  if (TSDB_CODE_SUCCESS != code) {
    planError("FQ-remoteTargets: rebuild from shared outputs failed rootType=%d source=%d selectExprs=%d code=0x%x",
              (int)nodeType(pRemoteRoot), (int)outputs.source,
              outputs.pSelectExprs ? LIST_LENGTH(outputs.pSelectExprs) : 0, code);
    nodesDestroyRemoteSelectOutputs(&outputs);
    return code;
  }

  if (pNewTargets != NULL) {
    nodesDestroyList(pRemoteRoot->pTargets);
    pRemoteRoot->pTargets = pNewTargets;
  }

  code = replaceProjectLogicProjections(pRemoteRoot, outputs.pSelectExprs);
  if (TSDB_CODE_SUCCESS != code) {
    planError("FQ-remoteTargets: replace project projections failed rootType=%d source=%d selectExprs=%d code=0x%x",
              (int)nodeType(pRemoteRoot), (int)outputs.source,
              outputs.pSelectExprs ? LIST_LENGTH(outputs.pSelectExprs) : 0, code);
    nodesDestroyRemoteSelectOutputs(&outputs);
    return code;
  }

  planDebug("FQ-remoteTargets: rootType=%d source=%d keyExprs=%d targets=%d",
            (int)nodeType(pRemoteRoot), (int)outputs.source, outputs.keyExprCount,
            pRemoteRoot->pTargets ? LIST_LENGTH(pRemoteRoot->pTargets) : 0);
  fqPlanDebugLogNodeOutputs("alignRemoteRoot.after", pRemoteRoot);
  nodesDestroyRemoteSelectOutputs(&outputs);
  return TSDB_CODE_SUCCESS;
}

static int32_t findLeafScanCount(SLogicNode* pNode) {
  if (pNode == NULL) return 0;
  if (nodeType(pNode) == QUERY_NODE_LOGIC_PLAN_SCAN) return 1;
  int32_t n = 0;
  SNode*  pCh = NULL;
  FOREACH(pCh, pNode->pChildren) n += findLeafScanCount((SLogicNode*)pCh);
  return n;
}

// Mark every SCAN leaf in pBoundary's subtree EXCEPT pKeep with FQ_PUSHDOWN, so
// later optimizer rounds (fqFindExternalScan) skip them — their remote SQL is
// already captured inside pBoundary's remote plan.
static void fqMarkOtherLegsPushdown(SLogicNode* pNode, SScanLogicNode* pKeep) {
  if (pNode == NULL) return;
  if (nodeType(pNode) == QUERY_NODE_LOGIC_PLAN_SCAN) {
    if ((SScanLogicNode*)pNode != pKeep) {
      OPTIMIZE_FLAG_SET_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_FQ_PUSHDOWN);
      planDebug("FQ-commit: marked other leg scan=%p as FQ_PUSHDOWN", pNode);
    }
    return;
  }
  SNode* pCh = NULL;
  FOREACH(pCh, pNode->pChildren) fqMarkOtherLegsPushdown((SLogicNode*)pCh, pKeep);
}

// Detach pKeep (the docking scan) from pBoundary's subtree so nodesDestroyNode on
// the boundary does not free pKeep, which lives on in the main tree.  Finds the
// list cell holding pKeep and NULLs it out (subtree destroy then skips it).
static void fqDetachScanFromSubtree(SLogicNode* pNode, SScanLogicNode* pKeep) {
  if (pNode == NULL || pNode->pChildren == NULL) return;
  SNode* pCh = NULL;
  FOREACH(pCh, pNode->pChildren) {
    if ((SScanLogicNode*)pCh == pKeep) {
      cell->pNode = NULL;  // detach without freeing; pKeep lives on in main tree
      return;
    }
  }
  FOREACH(pCh, pNode->pChildren) fqDetachScanFromSubtree((SLogicNode*)pCh, pKeep);
}

// fqBuildRemoteSubtree — recursively clone the pushable subtree rooted at pNode
// into a remote logic subtree, applying per-node transforms.  The recursion
// terminates at SCAN leaves (and JOIN, whose two leg scans are cloned by
// fqBuildRemoteJoin).  Mirrors the original tree shape, so JOIN's two legs and
// any future subquery legs are handled with no linear-chain special cases.
//
// pScanForMeta: the originating external scan, used by transforms that need ext
// table meta (INTERVAL time_bucket, TBNAME tag rewrite, two-pass scanSeq).
static int32_t fqBuildRemoteSubtree(SLogicNode* pNode, SScanLogicNode* pScanForMeta,
                                    SLogicNode** ppRemote) {
  *ppRemote = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  bool    foldRemoteAggChildPartition = false;
  const SPartitionLogicNode* pFoldedPartition = NULL;

  fqPlanDebugLogNodeOutputs("commit.remoteSubtree.src", pNode);

  // SCAN leaf -> pure remote leaf (FROM + SELECT + own WHERE in node.pConditions).
  // If the docking scan already carries a lower-level remote subtree (for
  // example, a previously-pushed join leg that is now being wrapped by an outer
  // join/project/sort), clone that subtree instead of collapsing back to a bare
  // scan.  This preserves the full remote join shape across iterative pushdown.
  if (nodeType(pNode) == QUERY_NODE_LOGIC_PLAN_SCAN) {
    SScanLogicNode* pScan = (SScanLogicNode*)pNode;
    if (pScan->pRemoteLogicPlan != NULL) {
      SLogicNode* pRemoteClone = NULL;
      code = nodesCloneNode(pScan->pRemoteLogicPlan, (SNode**)&pRemoteClone);
      if (TSDB_CODE_SUCCESS != code) {
        planError("FQ-commit: clone existing remote subtree failed scan=%p remoteRootType=%d code=0x%x",
                  pScan, (int)nodeType(pScan->pRemoteLogicPlan), code);
        return code;
      }
      if (pRemoteClone != NULL) {
        pRemoteClone->pParent = NULL;
      }
      *ppRemote = pRemoteClone;
      planDebug("FQ-commit: reuse existing remote subtree from docking scan=%p remoteRootType=%d",
                pScan, (int)nodeType(pScan->pRemoteLogicPlan));
      return TSDB_CODE_SUCCESS;
    }

    SScanLogicNode* pLeaf = NULL;
    code = fqCloneScanAsLeaf(pScan, &pLeaf);
    if (TSDB_CODE_SUCCESS != code) return code;
    *ppRemote = (SLogicNode*)pLeaf;
    return TSDB_CODE_SUCCESS;
  }

  // JOIN -> remote JOIN with recursively-built child subtrees.  Child legs may be
  // bare scans, nested joins, or docking scans that already carry a remote
  // subtree from an earlier optimizer round.
  if (nodeType(pNode) == QUERY_NODE_LOGIC_PLAN_JOIN) {
    SJoinLogicNode* pRemoteJoin = NULL;
    code = fqBuildRemoteJoin((SJoinLogicNode*)pNode, pScanForMeta, &pRemoteJoin);
    if (TSDB_CODE_SUCCESS != code) return code;
    *ppRemote = (SLogicNode*)pRemoteJoin;
    return TSDB_CODE_SUCCESS;
  }

  // Multi-child set-op PROJECT (UNION/UNION ALL) -> recursively clone every
  // branch subtree.  The generic whole-push path can reach this shape directly
  // for a fully-pushable set-op query, so we must preserve all children here
  // instead of following only the first-child linear chain.
  if (nodeType(pNode) == QUERY_NODE_LOGIC_PLAN_PROJECT &&
      pNode->pChildren != NULL && LIST_LENGTH(pNode->pChildren) > 1) {
    SProjectLogicNode* pRemoteProj = NULL;
    code = nodesCloneNode((SNode*)pNode, (SNode**)&pRemoteProj);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    if (pRemoteProj->node.pChildren != NULL) {
      nodesDestroyList(pRemoteProj->node.pChildren);
      pRemoteProj->node.pChildren = NULL;
    }
    pRemoteProj->node.pParent = NULL;

    code = nodesMakeList(&pRemoteProj->node.pChildren);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode((SNode*)pRemoteProj);
      return code;
    }

    int32_t branchIdx = 0;
    SNode* pBranch = NULL;
    FOREACH(pBranch, pNode->pChildren) {
      SLogicNode* pRemoteBranch = NULL;
      code = fqBuildRemoteSubtree((SLogicNode*)pBranch, pScanForMeta, &pRemoteBranch);
      if (TSDB_CODE_SUCCESS != code) {
        planError("FQ-commit: build set-op branch failed, rootType=%d branchIdx=%d branchType=%d code=0x%x",
                  (int)nodeType(pNode), branchIdx, (int)nodeType(pBranch), code);
        nodesDestroyNode((SNode*)pRemoteProj);
        return code;
      }

      code = nodesListStrictAppend(pRemoteProj->node.pChildren, (SNode*)pRemoteBranch);
      if (TSDB_CODE_SUCCESS != code) {
        nodesDestroyNode((SNode*)pRemoteBranch);
        nodesDestroyNode((SNode*)pRemoteProj);
        return code;
      }
      pRemoteBranch->pParent = (SLogicNode*)pRemoteProj;
      planDebug("FQ-commit: built remote set-op branch idx=%d rootType=%d branchType=%d remoteBranchType=%d",
                branchIdx, (int)nodeType(pNode), (int)nodeType(pBranch), (int)nodeType((SNode*)pRemoteBranch));
      ++branchIdx;
    }

    *ppRemote = (SLogicNode*)pRemoteProj;
    planDebug("FQ-commit: built remote multi-child project type=%d nChildren=%d isSetOpProj=%d isUnionDedup=%d",
              (int)nodeType(pNode), LIST_LENGTH(pRemoteProj->node.pChildren),
              (int)pRemoteProj->isSetOpProj, (int)pRemoteProj->isUnionDedup);
    return TSDB_CODE_SUCCESS;
  }

  // Single-child nodes: recurse into the (sole) child first, then build this node.
  SLogicNode* pChild = fqFirstChild(pNode);
  const SPartitionLogicNode* pIntervalPart = NULL;
  if (nodeType(pNode) == QUERY_NODE_LOGIC_PLAN_WINDOW && pChild != NULL &&
      nodeType(pChild) == QUERY_NODE_LOGIC_PLAN_PARTITION) {
    const SWindowLogicNode* pWin = (const SWindowLogicNode*)pNode;
    if (pWin->winType == WINDOW_TYPE_INTERVAL) {
      pIntervalPart = (const SPartitionLogicNode*)pChild;
      pChild = fqFirstChild((SLogicNode*)pIntervalPart);
      planDebug("FQ-commit: WINDOW(INTERVAL) over PARTITION detected, fold partition keys into remote interval agg");
    }
  }
  if (pChild == NULL) {
    planError("FQ-commit: node type=%d has no child in pushable subtree", (int)nodeType(pNode));
    return TSDB_CODE_EXT_SQL_RENDER_ERROR;
  }
  SLogicNode* pRemoteChild = NULL;
  code = fqBuildRemoteSubtree(pChild, pScanForMeta, &pRemoteChild);
  if (TSDB_CODE_SUCCESS != code) return code;

  if (nodeType(pNode) == QUERY_NODE_LOGIC_PLAN_AGG &&
      pChild != NULL &&
      nodeType((SNode*)pChild) == QUERY_NODE_LOGIC_PLAN_PARTITION &&
      pRemoteChild != NULL &&
      nodeType((SNode*)pRemoteChild) == QUERY_NODE_LOGIC_PLAN_PARTITION) {
    const SPartitionLogicNode* pPart = (const SPartitionLogicNode*)pChild;
    bool partKeysCovered = fqAggCoversPartitionKeys((const SAggLogicNode*)pNode, pPart);
    planDebug("FQ-commit: remote AGG child PARTITION probe agg=%p part=%p coversKeys=%d hasTbname=%d hasPartCond=%d aggFuncs=%d groupKeys=%d",
              pNode, pPart, partKeysCovered ? 1 : 0,
              keysHasTbname(pPart->pPartitionKeys) ? 1 : 0,
              pPart->node.pConditions ? 1 : 0,
              ((const SAggLogicNode*)pNode)->pAggFuncs ? (int32_t)LIST_LENGTH(((const SAggLogicNode*)pNode)->pAggFuncs) : 0,
              ((const SAggLogicNode*)pNode)->pGroupKeys ? (int32_t)LIST_LENGTH(((const SAggLogicNode*)pNode)->pGroupKeys) : 0);
    if (!keysHasTbname(pPart->pPartitionKeys) &&
        NULL == pPart->node.pConditions &&
        partKeysCovered) {
      SLogicNode* pRemoteGrandChild = fqFirstChild(pRemoteChild);
      if (NULL == pRemoteGrandChild) {
        nodesDestroyNode((SNode*)pRemoteChild);
        return TSDB_CODE_PLAN_INTERNAL_ERROR;
      }

      NODES_CLEAR_LIST(pRemoteChild->pChildren);
      pRemoteGrandChild->pParent = NULL;
      nodesDestroyNode((SNode*)pRemoteChild);
      pRemoteChild = pRemoteGrandChild;
      foldRemoteAggChildPartition = true;
      pFoldedPartition = pPart;
      planDebug("FQ-commit: drop remote PARTITION under AGG agg=%p part=%p childType=%d",
                pNode, pPart, (int)nodeType((SNode*)pRemoteChild));
    }
  }

  SLogicNode* pRemoteNode = NULL;
  if (nodeType(pNode) == QUERY_NODE_LOGIC_PLAN_WINDOW) {
    // INTERVAL window -> remote GROUP BY time_bucket AGG.
    SAggLogicNode* pAgg = NULL;
    code = fqBuildIntervalAgg(pScanForMeta, (SWindowLogicNode*)pNode, &pAgg);
    if (TSDB_CODE_SUCCESS != code || pAgg == NULL) { nodesDestroyNode((SNode*)pRemoteChild); return code ? code : TSDB_CODE_FAILED; }
    if (pIntervalPart != NULL) {
      code = fqAppendPartitionKeysToIntervalAgg(pAgg, pIntervalPart);
      if (TSDB_CODE_SUCCESS != code) {
        nodesDestroyNode((SNode*)pAgg);
        nodesDestroyNode((SNode*)pRemoteChild);
        return code;
      }
    }
    pRemoteNode = (SLogicNode*)pAgg;
  } else {
    // PARTITION / AGG / INDEF / SORT / PROJECT -> clone as a pure single-child node.
    code = nodesCloneNode((SNode*)pNode, (SNode**)&pRemoteNode);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pRemoteChild); return code; }
    if (pRemoteNode->pChildren != NULL) {
      nodesDestroyList(pRemoteNode->pChildren);
      pRemoteNode->pChildren = NULL;
    }
    pRemoteNode->pParent = NULL;
    if (foldRemoteAggChildPartition && QUERY_NODE_LOGIC_PLAN_AGG == nodeType(pRemoteNode)) {
      SAggLogicNode* pRemoteAgg = (SAggLogicNode*)pRemoteNode;
      if ((NULL == pRemoteAgg->pGroupKeys || 0 == LIST_LENGTH(pRemoteAgg->pGroupKeys)) &&
          NULL != pFoldedPartition && NULL != pFoldedPartition->pPartitionKeys) {
        code = nodesCloneList(pFoldedPartition->pPartitionKeys, &pRemoteAgg->pGroupKeys);
        if (TSDB_CODE_SUCCESS != code) {
          nodesDestroyNode((SNode*)pRemoteNode);
          nodesDestroyNode((SNode*)pRemoteChild);
          return code;
        }
        planDebug("FQ-commit: cloned PARTITION keys into remote AGG groupKeys=%d agg=%p",
                  pRemoteAgg->pGroupKeys ? (int32_t)LIST_LENGTH(pRemoteAgg->pGroupKeys) : 0,
                  pRemoteAgg);
      }
    }
    // PARTITION/AGG with TBNAME grouping (InfluxDB) -> substitute real tag cols
    // and add them to the remote leaf SCAN's pScanCols.
    if ((nodeType(pNode) == QUERY_NODE_LOGIC_PLAN_PARTITION ||
         nodeType(pNode) == QUERY_NODE_LOGIC_PLAN_AGG) &&
        pScanForMeta->pExtTableNode != NULL) {
      const SExtTableMeta* pMeta = ((SExtTableNode*)pScanForMeta->pExtTableNode)->pExtMeta;
      code = fqRewriteRemoteTbnameTags(pRemoteNode, (SNode*)pRemoteChild, pMeta);
      if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pRemoteNode); nodesDestroyNode((SNode*)pRemoteChild); return code; }
    }
    // PERCENTILE / REPEAT_SCAN_FUNC AGG -> two-pass scan flag on the FederatedScan.
    if (nodeType(pNode) == QUERY_NODE_LOGIC_PLAN_AGG && fqNodeHasRepeatScanFunc(pNode)) {
      pScanForMeta->scanSeq[0] = 2;
      planDebug("FQ-commit: set scanSeq[0]=2 (two-pass) for REPEAT_SCAN_FUNC AGG");
    }
  }

  // Wire the recursively-built child under this remote node.
  code = nodesMakeList(&pRemoteNode->pChildren);
  if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pRemoteNode); nodesDestroyNode((SNode*)pRemoteChild); return code; }
  code = nodesListStrictAppend(pRemoteNode->pChildren, (SNode*)pRemoteChild);
  if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pRemoteNode); return code; }
  pRemoteChild->pParent = pRemoteNode;
  *ppRemote = pRemoteNode;
  planDebug("FQ-commit: built remote node type=%d", (int)nodeType(pNode));
  return TSDB_CODE_SUCCESS;
}

// Step 3 (COMMIT): clone the pushable subtree (root = pBoundary) into
// pScan->pRemoteLogicPlan, then dock the main tree — pScan replaces pBoundary as
// the local FederatedScan, carrying pBoundary's output targets.  The old pBoundary
// subtree (minus pScan, which lives on as the docking node) is destroyed.
static int32_t fqCommitPushChain(SOptimizeContext* pCxt, SScanLogicNode* pScan,
                                 SLogicSubplan* pLogicSubplan, SLogicNode* pBoundary) {
  if (pBoundary == NULL) {
    planDebug("FQ-commit: nothing pushable - scan stays fully local");
    return TSDB_CODE_SUCCESS;
  }
  int32_t code = TSDB_CODE_SUCCESS;
  planDebug("FQ-commit: enter scan=%p boundary type=%d", pScan, (int)nodeType(pBoundary));
  fqPlanDebugLogNodeOutputs("commit.boundary", pBoundary);
  for (SLogicNode* pDbg = pBoundary; pDbg != NULL; pDbg = fqFirstChild(pDbg)) {
    fqPlanDebugLogNodeOutputs("commit.boundary.chain", pDbg);
  }

  // Build the remote subtree.
  SNode* pRemoteBase = NULL;
  if (pScan->pRemoteLogicPlan != NULL) {
    // Outer re-push over an existing remote root (UNION container): the boundary
    // subtree wraps ON TOP of it.  Recursion would re-clone the scan leaf, so for
    // this case we keep the existing remote root and recurse only the chain above.
    // (Currently only the UNION container path sets pRemoteLogicPlan pre-commit.)
    pRemoteBase = pScan->pRemoteLogicPlan;
    pScan->pRemoteLogicPlan = NULL;
    planDebug("FQ-commit: base = existing remote root type=%d (outer re-push)", (int)nodeType(pRemoteBase));
    // Build the boundary subtree but graft the existing root at the bottom.
    // For simplicity the existing-root path keeps prior behaviour: wrap the
    // boundary chain above pRemoteBase by recursing with a sentinel leaf swap.
    // (Handled below by treating pBoundary's subtree as single-child down to scan.)
  }

  SLogicNode* pRemoteSubtree = NULL;
  code = fqBuildRemoteSubtree(pBoundary, pScan, &pRemoteSubtree);
  if (TSDB_CODE_SUCCESS != code) { if (pRemoteBase) nodesDestroyNode(pRemoteBase); return code; }

  code = fqHarvestParentJoinWhereForRemoteSubtree(pBoundary, pRemoteSubtree,
                                                  pCxt->pExtCapability, pCxt->pSessionOverlay,
                                                  pCxt->pPlanCxt);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pRemoteSubtree);
    if (pRemoteBase) {
      nodesDestroyNode(pRemoteBase);
    }
    return code;
  }

  if (pRemoteBase != NULL) {
    // Splice the existing remote root in as the deepest child of the new subtree,
    // replacing the freshly-cloned scan leaf.
    SLogicNode* pDeepest = pRemoteSubtree;
    while (fqFirstChild(pDeepest) != NULL) pDeepest = fqFirstChild(pDeepest);
    SLogicNode* pLeafParent = pDeepest->pParent;
    if (pLeafParent != NULL) {
      // Replace the leaf scan with pRemoteBase under pLeafParent.
      nodesDestroyList(pLeafParent->pChildren);
      pLeafParent->pChildren = NULL;
      code = nodesMakeList(&pLeafParent->pChildren);
      if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pRemoteSubtree); nodesDestroyNode(pRemoteBase); return code; }
      code = nodesListStrictAppend(pLeafParent->pChildren, pRemoteBase);
      if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pRemoteSubtree); return code; }
      ((SLogicNode*)pRemoteBase)->pParent = pLeafParent;
    } else {
      // pBoundary was the scan itself; the existing root replaces the subtree.
      nodesDestroyNode((SNode*)pRemoteSubtree);
      pRemoteSubtree = (SLogicNode*)pRemoteBase;
    }
  }

  code = fqAlignRemoteRootTargetsForRender(pRemoteSubtree);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pRemoteSubtree);
    return code;
  }

  // Mark a JOIN's other leg(s) FQ_PUSHDOWN so fqFindExternalScan skips them.
  if (nodeType(pBoundary) == QUERY_NODE_LOGIC_PLAN_JOIN ||
      findLeafScanCount(pBoundary) > 1) {
    fqMarkOtherLegsPushdown(pBoundary, pScan);
  }

  // Main tree: pScan replaces pBoundary as the local docking FederatedScan.
  pScan->node.stmtRoot = pBoundary->stmtRoot;
  if (pBoundary->pParent == NULL) {
    pLogicSubplan->pNode = (SLogicNode*)pScan;
    pScan->node.pParent  = NULL;
  } else {
    SNode* pIterNode = NULL;
    FOREACH(pIterNode, pBoundary->pParent->pChildren) {
      if (pIterNode == (SNode*)pBoundary) {
        REPLACE_NODE(pScan);
        pScan->node.pParent = pBoundary->pParent;
        break;
      }
    }
  }
  if (pRemoteSubtree->pTargets != NULL && pBoundary != (SLogicNode*)pScan) {
    SNodeList* pNewTargets = NULL;
    code = nodesCloneList(pRemoteSubtree->pTargets, &pNewTargets);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pRemoteSubtree); return code; }
    nodesDestroyList(pScan->node.pTargets);
    pScan->node.pTargets = pNewTargets;
    planDebug("FQ-commit: docking scan targets cloned from remote root type=%d len=%d",
              (int)nodeType(pRemoteSubtree), LIST_LENGTH(pNewTargets));
  }

  // Destroy the old boundary subtree, preserving pScan (it lives on as docking).
  if (pBoundary != (SLogicNode*)pScan) {
    fqDetachScanFromSubtree(pBoundary, pScan);
    nodesDestroyNode((SNode*)pBoundary);
  }

  // The docking pScan's WHERE was cloned into the remote leaf (it is pushable —
  // collect verified this before including the scan).  Clear it on the local
  // docking node so the same predicate is not also applied locally.
  if (pScan->node.pConditions != NULL) {
    nodesDestroyNode(pScan->node.pConditions);
    pScan->node.pConditions = NULL;
  }

  pScan->pRemoteLogicPlan = (SNode*)pRemoteSubtree;
  pRemoteSubtree->pParent = NULL;
  planDebug("FQ-commit: done - pScan=%p pRemoteLogicPlan=%p type=%d",
            pScan, pRemoteSubtree, (int)nodeType(pRemoteSubtree));
  return TSDB_CODE_SUCCESS;
}

static int32_t fqPushdownOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  // Stream calc plans need a local Project/Sort left above the ExternalScan so that
  // streamScanSplit (planSpliter.c) has a parent node to split the scan away from,
  // producing a genuine runner subplan fed by Exchange from the calc-reader scan.
  // Harvesting that chain down to the remote source (as this optimization normally
  // does) collapses a bare passthrough calc query like "AS SELECT * FROM <ext table>"
  // down to a single parentless scan, leaving the calc plan with zero runner
  // subplans -- which the stream trigger/mnode deploy path cannot execute (calc
  // requests can only be dispatched to a runner task, not a calc reader directly).
  if (inStreamCalcClause(pCxt->pPlanCxt)) {
    return TSDB_CODE_SUCCESS;
  }

  SScanLogicNode* pScan = fqFindExternalScan(pLogicSubplan->pNode);
  if (NULL == pScan) {
    return TSDB_CODE_SUCCESS;
  }

  // EXT stream calc readers fetch source rows into the stream cache, then the
  // runner evaluates the original calc subtree locally.  FQ collect/commit
  // replaces that subtree with remote aggregate/project outputs, while the
  // reader still materializes pScanCols.  Keep both sides on the raw-row
  // contract for stream calc; normal FQ and stream trigger plans still push.
  if (inStreamCalcClause(pCxt->pPlanCxt)) {
    planDebug("FQ-push: skip remote rewrite for stream calc scan=%p", pScan);
    return TSDB_CODE_SUCCESS;
  }

  // PARTITION->AGG collapse must happen BEFORE we build the remote plan.  The
  // generic PartitionCols rule (partitionColsOpt) runs LATER in optimizeRuleSet[]
  // than FqPushdown, so without this the remote plan would still carry a separate
  // AGG-over-PARTITION pair (rendered as broken nested subqueries).  Reuse the
  // exact same rule here so an external `... partition by k` with aggregation
  // collapses into one AGG(GROUP BY k) before FQ collects it.  Idempotent: when
  // PartitionCols runs again later the PARTITION is already gone (no-op).
  {
    int32_t mergeCode = partitionColsOpt(pCxt, pLogicSubplan);
    if (TSDB_CODE_SUCCESS != mergeCode) return mergeCode;
    // Do NOT pre-run the generic eliminateProjOptimize() in the FQ path.
    // The remote renderer natively supports PROJECT and must preserve it as the
    // single source of truth for final remote output order.  Using the generic
    // main-plan Project elimination here mutates the logic tree before collect,
    // which can drop the only explicit final-output owner and force later FQ
    // helpers to infer outputs from child nodes (e.g. Sort/Scan), causing hidden
    // ORDER/WHERE dependency columns to leak into remote SELECT results.
    //
    // Pass-through PROJECT folding for flat remote SQL is handled inside the FQ
    // renderer/collector path itself, but ONLY when the Project is semantically
    // pure column pruning/reordering over the child outputs.
    //
    // pScan is unaffected by partition collapse (it removes PARTITION above the
    // scan), but re-find in case tree pointers shifted.
    pScan = fqFindExternalScan(pLogicSubplan->pNode);
    if (NULL == pScan) return TSDB_CODE_SUCCESS;
  }

  if (NULL != pScan->pExtTableNode && QUERY_NODE_EXTERNAL_TABLE == nodeType(pScan->pExtTableNode)) {
    pCxt->pExtCapability = &((SExtTableNode*)pScan->pExtTableNode)->capability;
  }

  // Restore 3.0 virtual-table semantics for direct external children under a
  // VirtualTableScan.  This path must not enter the generic FQ collect/commit
  // flow, otherwise local column routing and dynamic child resolution can drift
  // away from the vtable-specific execution contract.
  if (pScan->node.pParent != NULL &&
      nodeType(pScan->node.pParent) == QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN) {
    bool keepPushableScanWhere =
        (pScan->node.pConditions != NULL) &&
        fqExprIsPushable(pScan->node.pConditions, pScan, pCxt->pExtCapability,
                         pCxt->pSessionOverlay, pCxt->pPlanCxt);
    int32_t code = fqInjectPkOrderBy(pScan, keepPushableScanWhere);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    if (keepPushableScanWhere && pScan->pRemoteLogicPlan != NULL) {
      nodesDestroyNode(pScan->node.pConditions);
      pScan->node.pConditions = NULL;
    }

    fqSyncScanOrderFromRemotePkSort(pScan);
    OPTIMIZE_FLAG_SET_MASK(pScan->node.optimizedFlag, OPTIMIZE_FLAG_FQ_PUSHDOWN);
    pCxt->optimized = true;
    return TSDB_CODE_SUCCESS;
  }

  int32_t dialect = pCxt->pExtCapability ? (int32_t)pCxt->pExtCapability->dialect : -1;
  bool    skipCollectCommit = false;

  // Idempotency guard: fqPushdownOptimize may be called multiple times for the
  // same logical plan (e.g. different sub-tasks with LIMIT).  If
  planDebug("FQ-push: optimize start - scan=%p dialect=%d", pScan, dialect);
  // DIAG: log pScanCols at entry to fqOptimizeScan
  {
    char _scBuf[512] = {0}; int _scOff = 0;
    if (pScan->pScanCols) {
      SNode* _sc = NULL;
      FOREACH(_sc, pScan->pScanCols) {
        const char* _cn = (nodeType(_sc) == QUERY_NODE_COLUMN) ? ((SColumnNode*)_sc)->colName : "?";
        _scOff += snprintf(_scBuf + _scOff, sizeof(_scBuf) - (size_t)_scOff - 1, "%s,", _cn);
      }
    }
    planDebug("FQ-push: pScanCols at entry=[%s]", _scBuf);
  }

  if (NULL == pScan->pScanCols || LIST_LENGTH(pScan->pScanCols) == 0) {
    planError("FQ-push: external scan invariant violated at optimize entry, source=%s table=%s scan=%p remotePlan=%p",
              (pScan->pExtTableNode != NULL) ? ((SExtTableNode*)pScan->pExtTableNode)->sourceName : "(null)",
              pScan->tableName.tname,
              pScan,
              pScan->pRemoteLogicPlan);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  if (NULL != pCxt->pSessionOverlay && NULL != pCxt->pPushdownAttempt &&
      *pCxt->pPushdownAttempt > pCxt->pSessionOverlay->attemptCount) {
    pCxt->pSessionOverlay->attemptCount = *pCxt->pPushdownAttempt;
  }

  if (NULL != pCxt->pExtCapability &&
      extCapShouldUseBaseline(pCxt->pExtCapability, pCxt->pSessionOverlay)) {
    skipCollectCommit = true;
    planDebug("FQ-push: skip collect/commit - baseline mode enabled (dialect=%d)", dialect);
  }

  if (!skipCollectCommit) {
    for (SLogicNode* pUp = pScan->node.pParent; pUp != NULL; pUp = pUp->pParent) {
      if (pUp->pSlimit != NULL) {
        skipCollectCommit = true;
        planDebug("FQ-push: skip collect/commit - ancestor carries SLIMIT/SOFFSET and remote render has no slimit clause support ancestor=%p type=%s",
                  pUp, nodesNodeName(nodeType((SNode*)pUp)));
        break;
      }
    }
  }

  // DS §5.3.10.3.1: each rule checks its own clause capability independently.
  // Do NOT do a global OR-check here — InfluxDB has EXT_CLAUSE_JOIN in its
  // default negative table (no JOIN support), and an OR-check would cause ALL
  // pushdown to be skipped for InfluxDB even though filter/projection/order/limit
  // are all available.  extCapShouldUseBaseline() already handles the
  // "everything in the session overlay is demoted" baseline path above.

  // Unified pushdown: harvest the scan-node WHERE (the condition lives on the
  // logic node that carries it), then collect the contiguous pushable chain
  // bottom-up and commit it into pRemoteLogicPlan.  No phase-ordered prefix —
  // INTERVAL/PARTITION/JOIN/AGG are all handled inside collect (judge) + commit
  // (transform), each node minding only itself.
  int32_t code = TSDB_CODE_SUCCESS;
  bool    ranCollectCommit = false;
  bool    hasSortInChain = false;

  // ── collect -> commit (recursive subtree model) ────────────────────────────
  // collect (READ-ONLY): walk scan->parent bottom-up, judging each node via
  // fqLayerIsPushable (capability matrix + per-type judge) and fqExprIsPushable
  // (the node's own WHERE).  Returns the highest contiguously-pushable node
  // (pBoundary) — the root of the connected pushable bottom subtree.  No tree
  // mutation, no pre-collect WHERE rewrite.
  // commit: recursively clone pBoundary's subtree into pScan->pRemoteLogicPlan
  // (each node owns its WHERE in node.pConditions; renderer renders it), then
  // dock pScan into pBoundary's slot for local execution.
  if (!skipCollectCommit) {
    SLogicNode* pBoundary = NULL;
    code = fqCollectPushChain(pScan, pCxt->pExtCapability, pCxt->pSessionOverlay, pCxt->pPlanCxt, &pBoundary);
    if (TSDB_CODE_SUCCESS != code) goto _cleanup;

    // Track whether a Sort is in the pushable subtree (controls pk ORDER BY below).
    for (SLogicNode* pNd = pBoundary; pNd != NULL && pNd != (SLogicNode*)pScan;
         pNd = fqFirstChild(pNd)) {
      if (nodeType(pNd) == QUERY_NODE_LOGIC_PLAN_SORT) {
        hasSortInChain = true;
        break;
      }
    }

    code = fqCommitPushChain(pCxt, pScan, pLogicSubplan, pBoundary);
    if (TSDB_CODE_SUCCESS != code) goto _cleanup;
    ranCollectCommit = true;
  }

  if (ranCollectCommit &&
      pScan->node.pParent != NULL && nodeType(pScan->node.pParent) == QUERY_NODE_LOGIC_PLAN_PROJECT &&
      (pScan->pRemoteLogicPlan == NULL || nodeType(pScan->pRemoteLogicPlan) != QUERY_NODE_LOGIC_PLAN_PROJECT)) {
    SProjectLogicNode* pParentProj = (SProjectLogicNode*)pScan->node.pParent;
    int32_t pushableCnt = 0;
    int32_t localCnt = 0;
    bool partialProj = fqProjectCanPartiallyPushdown(pParentProj, pScan, pCxt->pExtCapability,
                                                     pCxt->pSessionOverlay, pCxt->pPlanCxt,
                                                     &pushableCnt, &localCnt);
    planDebug("FQ-partialProject: probe parent project=%p partial=%d pushableCnt=%d localCnt=%d",
              pParentProj, (int)partialProj, pushableCnt, localCnt);
    if (partialProj) {
      code = fqBuildPartialRemoteProject(pScan, pParentProj, pCxt->pExtCapability,
                                         pCxt->pSessionOverlay, pCxt->pPlanCxt);
      if (TSDB_CODE_SUCCESS != code) {
        goto _cleanup;
      }
    }
  }

  if (ranCollectCommit) {
    code = fqNormalizeLocalQuantifyWrapperAggs(pScan);
    if (TSDB_CODE_SUCCESS != code) {
      goto _cleanup;
    }

    code = fqMarkCommittedRemoteSubqueries(pCxt->pPlanCxt, pScan);
    if (TSDB_CODE_SUCCESS != code) {
      planError("FQ-mark: commit succeeded but absorbed subquery marking failed, ownerSubQIdx:%d code:0x%x",
                pCxt->pPlanCxt ? pCxt->pPlanCxt->subqueryIndex : -2, code);
      goto _cleanup;
    }
  }

  // v2 render mode: decided + assigned IN-PLACE during the collect
  // probe (fqExprIsPushable's REMOTE_* branches), then carried into
  // pRemoteLogicPlan by fqCommitPushChain's whole-node clone.  No separate
  // post-commit walk — render mode is the direct product of the pushability
  // judgement that already consulted the capability matrix.

  // ── Default pk ORDER BY injection (DS §5.2.x — fallback flow ordering) ──
  // Inject ORDER BY <pk> ASC into the remote plan ONLY when ALL hold:
  //   (1) no Sort was pushed into the chain (!hasSortInChain), AND
  //   (2) the remote plan contains ONLY PROJECT and SCAN nodes — i.e. a plain
  //       row-passthrough SELECT (no remote AGG/INDEF/PARTITION/WINDOW/JOIN that
  //       would make raw-row ordering irrelevant), AND
  //   (3) a row-order-sensitive operator (AGG/INDEF/PARTITION/WINDOW/JOIN) stays
  //       LOCAL above the scan and needs the remote source to return rows in
  //       ascending timestamp order (e.g. csum/diff/partition rely on input order).
  // When any of these fails, the remote plan either already orders/aggregates the
  // rows, or no local consumer cares about raw-row order — so no pk ORDER BY.
  if (!hasSortInChain) {
    // (2) remote plan is PROJECT/SCAN only.
    bool remoteIsProjScanOnly = true;
    if (pScan->pRemoteLogicPlan != NULL) {
      SNode* pWalk = pScan->pRemoteLogicPlan;
      while (pWalk != NULL) {
        ENodeType wt = nodeType(pWalk);
        if (wt != QUERY_NODE_LOGIC_PLAN_PROJECT && wt != QUERY_NODE_LOGIC_PLAN_SCAN) {
          remoteIsProjScanOnly = false;
          break;
        }
        SNodeList* pCh = ((SLogicNode*)pWalk)->pChildren;
        pWalk = (pCh && LIST_LENGTH(pCh) > 0) ? nodesListGetNode(pCh, 0) : NULL;
      }
    }
    // (3) a row-order-sensitive operator stays LOCAL above the scan.
    // INTERP is intentionally local per current capability matrix, but it still
    // requires the remote source rows to arrive in ascending timestamp order.
    // Without this gate, bare-scan pushdown on unordered sources (for example
    // InfluxDB) can feed local INTERP with reversed / arbitrary row order and
    // produce wrong PREV/NEXT/LINEAR results even though no remote INTERP
    // clause was pushed.
    //
    // IMPORTANT: generic AGG is still order-insensitive by default. Only AGG
    // nodes that actually contain keep-order / timeline functions should
    // request synthetic pk ordering from the remote scan. This keeps ordinary
    // COUNT/SUM/MIN/MAX fallback queries from carrying an unnecessary
    // ORDER BY <pk>, while preserving correctness for functions such as
    // GROUP_CONCAT / LEASTSQUARES / TWA / ELAPSED that consume row order.
    bool localHasOrderSensitive = false;
    for (SLogicNode* pUp = pScan->node.pParent; pUp != NULL; pUp = pUp->pParent) {
      ENodeType ut = nodeType(pUp);
      planDebug("FQ-DBG: pk-inject ancestor scan=%p source=%s table=%s ancestor=%p type=%s",
                pScan,
                (pScan->pExtTableNode != NULL) ? ((SExtTableNode*)pScan->pExtTableNode)->sourceName : "(null)",
                pScan->tableName.tname,
                pUp,
                nodesNodeName(ut));
      if (ut == QUERY_NODE_LOGIC_PLAN_AGG) {
        SAggLogicNode* pAgg = (SAggLogicNode*)pUp;
        bool aggNeedsOrder = fqAggNodeNeedsOrderedInput(pAgg);
        planDebug("FQ-DBG: local AGG ancestor=%p aggNeedsOrder=%d hasTimeLine=%d onlyHasKeepOrder=%d groupKeys=%d",
                  pAgg, (int)aggNeedsOrder, (int)pAgg->hasTimeLineFunc,
                  (int)pAgg->onlyHasKeepOrderFunc, LIST_LENGTH(pAgg->pGroupKeys));
        if (aggNeedsOrder) {
          localHasOrderSensitive = true;
          break;
        }
        continue;
      }
      if (ut == QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC ||
          ut == QUERY_NODE_LOGIC_PLAN_PARTITION || ut == QUERY_NODE_LOGIC_PLAN_WINDOW ||
          ut == QUERY_NODE_LOGIC_PLAN_INTERP_FUNC ||
          ut == QUERY_NODE_LOGIC_PLAN_JOIN) {
        planDebug("FQ-DBG: local order-sensitive ancestor=%p type=%s keeps default pk ORDER BY injection",
                  pUp, nodesNodeName(ut));
        localHasOrderSensitive = true;
        break;
      }
    }
    planDebug("FQ-DBG: pk-inject gate scan=%p source=%s table=%s remoteIsProjScanOnly=%d localHasOrderSensitive=%d remoteRootType=%s",
              pScan,
              (pScan->pExtTableNode != NULL) ? ((SExtTableNode*)pScan->pExtTableNode)->sourceName : "(null)",
              pScan->tableName.tname,
              (int)remoteIsProjScanOnly,
              (int)localHasOrderSensitive,
              pScan->pRemoteLogicPlan ? nodesNodeName(nodeType(pScan->pRemoteLogicPlan)) : "(null)");
    if (remoteIsProjScanOnly && localHasOrderSensitive) {
      code = fqInjectPkOrderBy(pScan, false);
      if (TSDB_CODE_SUCCESS != code) goto _cleanup;
    }

  }

  if (TSDB_CODE_SUCCESS == code &&
      (NULL == pScan->pScanCols || LIST_LENGTH(pScan->pScanCols) == 0)) {
    planError("FQ-push: external scan invariant violated after pushdown, source=%s table=%s scan=%p remoteRootType=%d",
              (pScan->pExtTableNode != NULL) ? ((SExtTableNode*)pScan->pExtTableNode)->sourceName : "(null)",
              pScan->tableName.tname,
              pScan,
              pScan->pRemoteLogicPlan ? (int)nodeType(pScan->pRemoteLogicPlan) : -1);
    code = TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  // ── Step 4: push LIMIT to the top of the remote plan ───────────────────────
  // Sort push / pass-through Project elimination are now handled generically by
  // the collect+commit chain above (the legacy fqPushAggOrderLimit R2/R4 pass is
  // gone).  fqPushLimit moves LIMIT onto pRemoteLogicPlan when the whole
  // statement was pushed down.
  if (TSDB_CODE_SUCCESS == code) {
    code = fqPushLimit(pScan, dialect, pCxt);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = fqPrepareRemoteGroupConcatOrder(pScan);
  }

  if (TSDB_CODE_SUCCESS == code) {
    fqSyncScanOrderFromRemotePkSort(pScan);
  }

  // ── FQ pushdown result summary ──
  if (TSDB_CODE_SUCCESS == code) {
    fqLogPushdownResult(pScan, dialect);
  }

  if (ranCollectCommit || skipCollectCommit) {
    // Mark this ExternalScan as processed so subsequent rounds skip it.
    OPTIMIZE_FLAG_SET_MASK(pScan->node.optimizedFlag, OPTIMIZE_FLAG_FQ_PUSHDOWN);
    // Signal optimized so the outer loop re-scans for additional ExternalScan nodes.
    pCxt->optimized = true;
  }
  planDebug("FQ-OPT: fqPushdownOptimize end pScan=%p pRemoteLogicPlan=%p type=%d",
            pScan, pScan->pRemoteLogicPlan,
            pScan->pRemoteLogicPlan ? (int)nodeType(pScan->pRemoteLogicPlan) : -1);

_cleanup:
  return code;
}

// ─── fqUnionAllOptimize ──────────────────────────────────────────────────────
// DS §5.3.8.6: same-source UNION ALL — push the entire UNION ALL as a single
// remote SQL.
//
// Runs AFTER FqPushdown (all FedScan nodes have been individually processed)
// and AFTER FqInterp. Whole-branch same-source pushdown is only allowed when
// each branch local tree has already collapsed to a single processed external
// scan. If a branch still keeps any PROJECT/SORT/AGG/... wrapper locally, that
// branch is not a whole-push branch and UNION ALL stays local.
//
// If all branches share the same external source (sourceType + sourceName),
// we merge them into a single FedScan whose pRemoteLogicPlan is a
// SProjectLogicNode(isSetOpProj=true) whose children are built directly from
// each branch's collapsed scan / remote subtree.
//
// The union_all container PROJECT is then replaced in the main plan by the
// representative FedScan (first branch), and the other branch FedScans are
// destroyed (their WHERE conditions are now captured in the UNION ALL container).
// ─────────────────────────────────────────────────────────────────────────────

// Find first unprocessed isSetOpProj PROJECT node in the subtree.
// "Unprocessed" means it doesn't have FQ_UNION_ALL flag set.
#define OPTIMIZE_FLAG_FQ_UNION_ALL  OPTIMIZE_FLAG_MASK(8)

static SProjectLogicNode* fqFindUnprocessedSetOpProj(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_PROJECT == nodeType(pNode)) {
    SProjectLogicNode* pProj = (SProjectLogicNode*)pNode;
    if (pProj->isSetOpProj &&
        !OPTIMIZE_FLAG_TEST_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_FQ_UNION_ALL)) {
      return pProj;
    }
  }
  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    SProjectLogicNode* pFound = fqFindUnprocessedSetOpProj((SLogicNode*)pChild);
    if (pFound) return pFound;
  }
  return NULL;
}

// Whole-branch UNION pushdown requires the branch local tree to have already
// collapsed to a single processed external FederatedScan. If any wrapper node
// still remains locally, that wrapper was not fully pushed and the branch must
// stay out of same-source whole-push.
static SScanLogicNode* fqGetWholePushBranchScan(SLogicNode* pNode) {
  if (NULL == pNode || QUERY_NODE_LOGIC_PLAN_SCAN != nodeType((SNode*)pNode)) {
    return NULL;
  }

  SScanLogicNode* pScan = (SScanLogicNode*)pNode;
  if (pScan->scanType != SCAN_TYPE_EXTERNAL ||
      !OPTIMIZE_FLAG_TEST_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_FQ_PUSHDOWN)) {
    return NULL;
  }

  return pScan;
}

static int32_t fqUnionAllOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  // Find first unprocessed same-source union_all container
  SProjectLogicNode* pSetOp = fqFindUnprocessedSetOpProj(pLogicSubplan->pNode);
  if (NULL == pSetOp) return TSDB_CODE_SUCCESS;

  // Must have at least 2 children
  if (NULL == pSetOp->node.pChildren || LIST_LENGTH(pSetOp->node.pChildren) < 2) {
    OPTIMIZE_FLAG_SET_MASK(pSetOp->node.optimizedFlag, OPTIMIZE_FLAG_FQ_UNION_ALL);
    return TSDB_CODE_SUCCESS;
  }

  // Collect FedScan for each branch; verify all processed + same-source
  int32_t nBranches = LIST_LENGTH(pSetOp->node.pChildren);
  SScanLogicNode* pRepScan = NULL;   // representative (first) FedScan
  const char*     sourceName = NULL;
  int32_t         sourceType = -1;

  SNode* pBranchNode = NULL;
  FOREACH(pBranchNode, pSetOp->node.pChildren) {
    SScanLogicNode* pScan = fqGetWholePushBranchScan((SLogicNode*)pBranchNode);
    if (NULL == pScan) {
      planDebug("FQ-UNION-ALL: branch is not a whole-push scan node - keep local set-op merge branchType=%d",
                (int)nodeType((SNode*)pBranchNode));
      OPTIMIZE_FLAG_SET_MASK(pSetOp->node.optimizedFlag, OPTIMIZE_FLAG_FQ_UNION_ALL);
      return TSDB_CODE_SUCCESS;
    }
    if (pScan->pRemoteLogicPlan != NULL) {
      planDebug("FQ-UNION-ALL: branch remote root type=%d accepted for same-source whole-push",
                (int)nodeType(pScan->pRemoteLogicPlan));
    }
    SExtTableNode* pExt = (SExtTableNode*)pScan->pExtTableNode;
    if (NULL == pExt) {
      planDebug("FQ-UNION-ALL: branch pExtTableNode NULL - skip");
      OPTIMIZE_FLAG_SET_MASK(pSetOp->node.optimizedFlag, OPTIMIZE_FLAG_FQ_UNION_ALL);
      return TSDB_CODE_SUCCESS;
    }
    if (sourceName == NULL) {
      sourceName = pExt->sourceName;
      sourceType = (int32_t)pExt->sourceType;
      pRepScan   = pScan;
    } else {
      // Cross-source check
      if ((int32_t)pExt->sourceType != sourceType ||
          strncmp(pExt->sourceName, sourceName, TSDB_EXT_SOURCE_NAME_LEN) != 0) {
        planDebug("FQ-UNION-ALL: cross-source union - skip (local merge)");
        OPTIMIZE_FLAG_SET_MASK(pSetOp->node.optimizedFlag, OPTIMIZE_FLAG_FQ_UNION_ALL);
        return TSDB_CODE_SUCCESS;
      }
    }
  }

  // All branches are processed + same-source.
  // Build the UNION ALL remote plan container (SProjectLogicNode, isSetOpProj=true).
  // Each child is a cloned SScanLogicNode with WHERE set, ORDER from
  // pScan->pRemoteLogicPlan (Sort), and pScanCols.  nodesRemotePlanToSQL renders
  // a multi-child isSetOpProj PROJECT as "branch1 UNION ALL branch2 …".
  int32_t code = TSDB_CODE_SUCCESS;
  SProjectLogicNode* pUnion = NULL;
  code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_PROJECT, (SNode**)&pUnion);
  if (TSDB_CODE_SUCCESS != code) return code;
  pUnion->isSetOpProj   = true;
  pUnion->ignoreGroupId = true;
  planDebug("FQ-UNION-ALL: pUnion=%p isSetOpProj=%d after set", (void*)pUnion, (int)pUnion->isSetOpProj);
  code = nodesMakeList(&pUnion->node.pChildren);
  if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pUnion); return code; }
  // Clone targets from pSetOp
  if (pSetOp->node.pTargets) {
    code = nodesCloneList(pSetOp->node.pTargets, &pUnion->node.pTargets);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pUnion); return code; }
  }
  if (pSetOp->pProjections) {
    code = nodesCloneList(pSetOp->pProjections, &pUnion->pProjections);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pUnion); return code; }
  }

  // Each branch local tree has already collapsed to a single processed
  // FederatedScan. Build the UNION children directly from that scan branch.
  FOREACH(pBranchNode, pSetOp->node.pChildren) {
    SLogicNode* pRemoteBranch = NULL;
    code = fqBuildUnionBranchRemoteSubtree((SLogicNode*)pBranchNode, &pRemoteBranch);
    if (TSDB_CODE_SUCCESS != code) {
      planError("FQ-UNION-ALL: build branch remote subtree failed branchType=%d code=0x%x",
                (int)nodeType(pBranchNode), code);
      nodesDestroyNode((SNode*)pUnion);
      return code;
    }

    pRemoteBranch->pParent = (SLogicNode*)pUnion;
    code = nodesListStrictAppend(pUnion->node.pChildren, (SNode*)pRemoteBranch);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pUnion); return code; }
    planDebug("FQ-UNION-ALL: appended branch remote subtree rootType=%d from branchType=%d",
              (int)nodeType((SNode*)pRemoteBranch), (int)nodeType(pBranchNode));
  }

  // Set representative scan's pRemoteLogicPlan to the UNION ALL container
  // (clear existing pRemoteLogicPlan first — it was a single-scan plan,
  //  now superseded by the UNION ALL container)
  if (pRepScan->pRemoteLogicPlan != NULL) {
    nodesDestroyNode(pRepScan->pRemoteLogicPlan);
    pRepScan->pRemoteLogicPlan = NULL;
  }
  pRepScan->pRemoteLogicPlan = (SNode*)pUnion;
  planDebug("FQ-UNION-ALL: pRepScan=%p pRemoteLogicPlan=%p isSetOpProj=%d nChildren=%d",
            (void*)pRepScan, (void*)pUnion, (int)pUnion->isSetOpProj,
            pUnion->node.pChildren ? (int)LIST_LENGTH(pUnion->node.pChildren) : -1);

  // Detach pRepScan from its branch parent (branch PROJECT) so we can
  // wire it directly under pSetOp's parent via replaceLogicNode.
  SLogicNode* pRepParent = pRepScan->node.pParent;
  if (pRepParent != NULL && pRepParent != (SLogicNode*)pSetOp) {
    nodesClearList(pRepParent->pChildren);
    pRepParent->pChildren = NULL;
    pRepScan->node.pParent = NULL;
  }

  // Update pRepScan targets to match the union_all container output schema
  if (pSetOp->node.pTargets) {
    nodesDestroyList(pRepScan->node.pTargets);
    pRepScan->node.pTargets = NULL;
    code = nodesCloneList(pSetOp->node.pTargets, &pRepScan->node.pTargets);
    if (TSDB_CODE_SUCCESS != code) return code;
  }

  // Destroy non-representative branch subtrees.
  // Their FedScans' remote plans were transferred to the UNION ALL container,
  // so set pRemoteLogicPlan=NULL before destroy to avoid double-free.
  for (int32_t bi = 0; bi < LIST_LENGTH(pSetOp->node.pChildren); bi++) {
    SNode* pB = nodesListGetNode(pSetOp->node.pChildren, bi);
    SScanLogicNode* pScan = fqGetWholePushBranchScan((SLogicNode*)pB);
    if (pScan == pRepScan) continue;  // rep scan is being reused, not destroyed
    if (pScan != NULL) {
      pScan->pRemoteLogicPlan = NULL;  // already transferred to pUnion
    }
    nodesDestroyNode(pB);
  }
  // Clear pSetOp children (already destroyed above, rep scan detached)
  nodesClearList(pSetOp->node.pChildren);
  pSetOp->node.pChildren = NULL;

  // Replace pSetOp with pRepScan in the main plan
  code = replaceLogicNode(pLogicSubplan, (SLogicNode*)pSetOp, (SLogicNode*)pRepScan);
  if (TSDB_CODE_SUCCESS != code) {
    planError("FQ-UNION-ALL: replaceLogicNode failed code=0x%x", code);
    return code;
  }
  nodesDestroyNode((SNode*)pSetOp);

  // Mark representative scan as UNION-ALL processed (so FqUnionAll skips it).
  OPTIMIZE_FLAG_SET_MASK(pRepScan->node.optimizedFlag, OPTIMIZE_FLAG_FQ_UNION_ALL);
  // D2 (design §2.5): clear the FQ_PUSHDOWN terminal flag.  pRepScan now sits
  // under a NEW upper operator (the outer query above the former UNION node,
  // e.g. ORDER BY/LIMIT in `select c from (a union all b) order by …`).  Clearing
  // the flag lets FqPushdown re-examine pRepScan and push that outer chain into
  // the UNION remote plan (DS §5.3.7.1.1 nested-subquery whole-pushdown).
  OPTIMIZE_FLAG_CLEAR_MASK(pRepScan->node.optimizedFlag, OPTIMIZE_FLAG_FQ_PUSHDOWN);

  planDebug("FQ-UNION-ALL: merged %d branches into single FedScan with UNION ALL remote plan (pUnion children=%d); cleared FQ_PUSHDOWN for outer re-push",
            nBranches, pUnion->node.pChildren ? (int)LIST_LENGTH(pUnion->node.pChildren) : 0);
  pCxt->optimized = true;
  return TSDB_CODE_SUCCESS;
}

// ─── fqUnionDeduplicateOptimize ──────────────────────────────────────────────
// DS §5.3.8.6: same-source UNION (dedup) — push the entire UNION as a single
// remote SQL using "branch1 UNION branch2".
//
// Runs AFTER FqUnionAll. Whole-branch same-source pushdown is only allowed
// when each branch local tree has already collapsed to a single processed
// external scan. If all branches share the same external source, we collapse
// the whole tree into a single representative FedScan whose pRemoteLogicPlan
// is a SProjectLogicNode(isSetOpProj=true, isUnionDedup=true) with branch
// scan / remote-subtree children — the same layout as UNION ALL, but flagged
// for UNION output.
// ─────────────────────────────────────────────────────────────────────────────

#define OPTIMIZE_FLAG_FQ_UNION_DEDUP  OPTIMIZE_FLAG_MASK(9)

// Find a SAggLogicNode that looks like a UNION-dedup container:
//   - All pGroupKeys non-empty (dedup via full-row grouping)
//   - No pAggFuncs (pure dedup, not aggregation)
//   - 2 children (left and right branches of UNION)
//   - Not yet processed (FQ_UNION_DEDUP flag not set)
static SAggLogicNode* fqFindUnprocessedUnionDedup(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_AGG == nodeType(pNode)) {
    SAggLogicNode* pAgg = (SAggLogicNode*)pNode;
    if (pAgg->pGroupKeys != NULL && LIST_LENGTH(pAgg->pGroupKeys) > 0 &&
        (pAgg->pAggFuncs == NULL || LIST_LENGTH(pAgg->pAggFuncs) == 0) &&
        pNode->pChildren != NULL && LIST_LENGTH(pNode->pChildren) == 2 &&
        !OPTIMIZE_FLAG_TEST_MASK(pNode->optimizedFlag, OPTIMIZE_FLAG_FQ_UNION_DEDUP)) {
      return pAgg;
    }
  }
  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    SAggLogicNode* pFound = fqFindUnprocessedUnionDedup((SLogicNode*)pChild);
    if (pFound) return pFound;
  }
  return NULL;
}

static int32_t fqUnionDeduplicateOptimize(SOptimizeContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SAggLogicNode* pDedup = fqFindUnprocessedUnionDedup(pLogicSubplan->pNode);
  if (NULL == pDedup) return TSDB_CODE_SUCCESS;

  // Collect FedScan for each branch; verify all processed + same-source
  int32_t nBranches = LIST_LENGTH(pDedup->node.pChildren);
  SScanLogicNode* pRepScan  = NULL;
  const char*     sourceName = NULL;
  int32_t         sourceType = -1;

  SNode* pBranchNode = NULL;
  FOREACH(pBranchNode, pDedup->node.pChildren) {
    SScanLogicNode* pScan = fqGetWholePushBranchScan((SLogicNode*)pBranchNode);
    if (NULL == pScan) {
      planDebug("FQ-UNION-DEDUP: branch is not a whole-push scan node - skip branchType=%d",
                (int)nodeType((SNode*)pBranchNode));
      OPTIMIZE_FLAG_SET_MASK(pDedup->node.optimizedFlag, OPTIMIZE_FLAG_FQ_UNION_DEDUP);
      return TSDB_CODE_SUCCESS;
    }
    if (pScan->pRemoteLogicPlan != NULL) {
      planDebug("FQ-UNION-DEDUP: branch remote root type=%d accepted for same-source whole-push",
                (int)nodeType(pScan->pRemoteLogicPlan));
    }
    SExtTableNode* pExt = (SExtTableNode*)pScan->pExtTableNode;
    if (NULL == pExt) {
      planDebug("FQ-UNION-DEDUP: branch pExtTableNode NULL - skip");
      OPTIMIZE_FLAG_SET_MASK(pDedup->node.optimizedFlag, OPTIMIZE_FLAG_FQ_UNION_DEDUP);
      return TSDB_CODE_SUCCESS;
    }
    if (sourceName == NULL) {
      sourceName = pExt->sourceName;
      sourceType = (int32_t)pExt->sourceType;
      pRepScan   = pScan;
    } else {
      if ((int32_t)pExt->sourceType != sourceType ||
          strncmp(pExt->sourceName, sourceName, TSDB_EXT_SOURCE_NAME_LEN) != 0) {
        planDebug("FQ-UNION-DEDUP: cross-source union - skip (local)");
        OPTIMIZE_FLAG_SET_MASK(pDedup->node.optimizedFlag, OPTIMIZE_FLAG_FQ_UNION_DEDUP);
        return TSDB_CODE_SUCCESS;
      }
    }
  }

  // All branches are same-source.  Build UNION dedup remote plan container.
  int32_t code = TSDB_CODE_SUCCESS;
  SProjectLogicNode* pUnion = NULL;
  code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_PROJECT, (SNode**)&pUnion);
  if (TSDB_CODE_SUCCESS != code) return code;
  pUnion->isSetOpProj  = true;
  pUnion->isUnionDedup = true;  // ← key flag: emit UNION not UNION ALL
  pUnion->ignoreGroupId = true;
  code = nodesMakeList(&pUnion->node.pChildren);
  if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pUnion); return code; }
  if (pDedup->node.pTargets) {
    code = nodesCloneList(pDedup->node.pTargets, &pUnion->node.pTargets);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pUnion); return code; }
  }
  if (pDedup->pGroupKeys) {
    code = nodesCloneList(pDedup->pGroupKeys, &pUnion->pProjections);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pUnion); return code; }
  }

  // Each branch local tree has already collapsed to a single processed
  // FederatedScan. Build the UNION children directly from that scan branch.
  FOREACH(pBranchNode, pDedup->node.pChildren) {
    SLogicNode* pRemoteBranch = NULL;
    code = fqBuildUnionBranchRemoteSubtree((SLogicNode*)pBranchNode, &pRemoteBranch);
    if (TSDB_CODE_SUCCESS != code) {
      planError("FQ-UNION-DEDUP: build branch remote subtree failed branchType=%d code=0x%x",
                (int)nodeType(pBranchNode), code);
      nodesDestroyNode((SNode*)pUnion);
      return code;
    }

    pRemoteBranch->pParent = (SLogicNode*)pUnion;
    code = nodesListStrictAppend(pUnion->node.pChildren, (SNode*)pRemoteBranch);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pUnion); return code; }
    planDebug("FQ-UNION-DEDUP: appended branch remote subtree rootType=%d from branchType=%d",
              (int)nodeType((SNode*)pRemoteBranch), (int)nodeType(pBranchNode));
  }

  // Wire pRepScan with the UNION dedup container
  if (pRepScan->pRemoteLogicPlan != NULL) {
    nodesDestroyNode(pRepScan->pRemoteLogicPlan);
    pRepScan->pRemoteLogicPlan = NULL;
  }
  pRepScan->pRemoteLogicPlan = (SNode*)pUnion;

  // Detach pRepScan from its branch parent
  SLogicNode* pRepParent = pRepScan->node.pParent;
  if (pRepParent != NULL && pRepParent != (SLogicNode*)pDedup) {
    nodesClearList(pRepParent->pChildren);
    pRepParent->pChildren = NULL;
    pRepScan->node.pParent = NULL;
  }

  // Update pRepScan targets to match the dedup container output schema
  if (pDedup->node.pTargets) {
    nodesDestroyList(pRepScan->node.pTargets);
    pRepScan->node.pTargets = NULL;
    code = nodesCloneList(pDedup->node.pTargets, &pRepScan->node.pTargets);
    if (TSDB_CODE_SUCCESS != code) return code;
  }

  // Destroy non-representative branch subtrees
  for (int32_t bi = 0; bi < LIST_LENGTH(pDedup->node.pChildren); bi++) {
    SNode* pB = nodesListGetNode(pDedup->node.pChildren, bi);
    SScanLogicNode* pScan = fqGetWholePushBranchScan((SLogicNode*)pB);
    if (pScan == pRepScan) continue;
    if (pScan != NULL) pScan->pRemoteLogicPlan = NULL;
    nodesDestroyNode(pB);
  }
  nodesClearList(pDedup->node.pChildren);
  pDedup->node.pChildren = NULL;

  // Replace pDedup with pRepScan in the main plan
  code = replaceLogicNode(pLogicSubplan, (SLogicNode*)pDedup, (SLogicNode*)pRepScan);
  if (TSDB_CODE_SUCCESS != code) {
    planError("FQ-UNION-DEDUP: replaceLogicNode failed code=0x%x", code);
    return code;
  }
  nodesDestroyNode((SNode*)pDedup);

  OPTIMIZE_FLAG_SET_MASK(pRepScan->node.optimizedFlag, OPTIMIZE_FLAG_FQ_UNION_DEDUP);
  // D2 (design §2.5): clear FQ_PUSHDOWN so FqPushdown re-examines pRepScan and
  // pushes any outer operator above the former UNION into the dedup remote plan.
  OPTIMIZE_FLAG_CLEAR_MASK(pRepScan->node.optimizedFlag, OPTIMIZE_FLAG_FQ_PUSHDOWN);
  planDebug("FQ-UNION-DEDUP: merged %d branches into single FedScan with UNION dedup remote plan; cleared FQ_PUSHDOWN for outer re-push",
            nBranches);
  pCxt->optimized = true;
  return TSDB_CODE_SUCCESS;
}

static int32_t applyOptimizeRule(SPlanContext* pCxt, SLogicSubplan* pLogicSubplan);

int32_t optimizeLogicPlan(SPlanContext* pCxt, SLogicSubplan* pLogicSubplan) {
  if (NULL != pLogicSubplan && pLogicSubplan->pushedDown) {
    planDebug("QID:0x%" PRIx64 ", optimize logic plan skipped, subplanGroupId:%d already absorbed", pCxt->queryId,
              pLogicSubplan->id.groupId);
    return TSDB_CODE_SUCCESS;
  }
  if (SUBPLAN_TYPE_MODIFY == pLogicSubplan->subplanType && NULL == pLogicSubplan->pNode->pChildren) {
    return TSDB_CODE_SUCCESS;
  }
  return applyOptimizeRule(pCxt, pLogicSubplan);
}

// Post-split optimization: eliminate redundant Sort when its child (through
// passthrough Projection) is a Merge that already provides globally ordered output.
static bool postSplitTargetsEqual(SNodeList* pLeft, SNodeList* pRight) {
  if (LIST_LENGTH(pLeft) != LIST_LENGTH(pRight)) return false;

  SNode* pLeftNode = NULL;
  SNode* pRightNode = NULL;
  FORBOTH(pLeftNode, pLeft, pRightNode, pRight) {
    if (!nodesEqualNode(pLeftNode, pRightNode)) {
      return false;
    }
  }
  return true;
}

static int32_t postSplitEliminateSortImpl(SLogicSubplan* pSubplan, SLogicNode* pNode) {
  if (NULL == pNode) return TSDB_CODE_SUCCESS;

  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    code = postSplitEliminateSortImpl(pSubplan, (SLogicNode*)pChild);
    if (TSDB_CODE_SUCCESS != code) return code;
  }

  if (QUERY_NODE_LOGIC_PLAN_SORT != nodeType(pNode)) return TSDB_CODE_SUCCESS;

  SSortLogicNode* pSort = (SSortLogicNode*)pNode;
  // calcGroupId Sort recalculates partition group ids for sort_for_group().
  if (pSort->calcGroupId) return TSDB_CODE_SUCCESS;
  if (1 != LIST_LENGTH(pSort->pSortKeys)) return TSDB_CODE_SUCCESS;
  SNode* pSortExpr = ((SOrderByExprNode*)nodesListGetNode(pSort->pSortKeys, 0))->pExpr;
  if (QUERY_NODE_COLUMN != nodeType(pSortExpr)) return TSDB_CODE_SUCCESS;
  if (NULL != pSort->node.pLimit || NULL != pSort->node.pSlimit || NULL != pSort->node.pConditions) {
    return TSDB_CODE_SUCCESS;
  }

  // Sort may also serve as the projection that trims helper columns kept by its child
  // for filters/order-by evaluation.  Replacing Sort with a child that exposes a wider
  // target list breaks exchange/scan schema alignment after split.
  SLogicNode* pDirectChild = (SLogicNode*)nodesListGetNode(pSort->node.pChildren, 0);
  if (NULL == pDirectChild) return TSDB_CODE_SUCCESS;

  if (LIST_LENGTH(pSort->node.pTargets) != LIST_LENGTH(pDirectChild->pTargets)) {
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < LIST_LENGTH(pSort->node.pTargets); ++i) {
    SNode* pSortTarget = nodesListGetNode(pSort->node.pTargets, i);
    SNode* pChildTarget = nodesListGetNode(pDirectChild->pTargets, i);
    if (!nodesEqualNode(pSortTarget, pChildTarget)) {
      return TSDB_CODE_SUCCESS;
    }
  }


  // Walk past passthrough Projections to find the actual data-producing child.
  SLogicNode* pDataChild = pDirectChild;
  while (NULL != pDataChild && QUERY_NODE_LOGIC_PLAN_PROJECT == nodeType(pDataChild) &&
         1 == LIST_LENGTH(pDataChild->pChildren)) {
    pDataChild = (SLogicNode*)nodesListGetNode(pDataChild->pChildren, 0);
  }

  if (NULL == pDataChild ||
      (QUERY_NODE_LOGIC_PLAN_MERGE != nodeType(pDataChild) && QUERY_NODE_LOGIC_PLAN_EXCHANGE != nodeType(pDataChild))) {
    return TSDB_CODE_SUCCESS;
  }

  // Check: the child already provides GLOBAL order in the direction Sort requires.
  EOrder sortOrder = ((SOrderByExprNode*)nodesListGetNode(pSort->pSortKeys, 0))->order;
  if (!isPrimaryKeyImpl(pSortExpr) && pSort->node.outputTsOrder != sortOrder) {
    return TSDB_CODE_SUCCESS;
  }
  if (pDataChild->resultDataOrder < DATA_ORDER_LEVEL_GLOBAL) {
    return TSDB_CODE_SUCCESS;
  }
  if (pDataChild->outputTsOrder != sortOrder) {
    return TSDB_CODE_SUCCESS;
  }

  planInfo("QID:0x%" PRIx64 " post-split: eliminating redundant Sort (child provides GLOBAL order=%d)",
           pSubplan->id.queryId, sortOrder);

  // Eliminate the Sort: replace it with its direct child.
  if (QUERY_NODE_LOGIC_PLAN_EXCHANGE == nodeType(pDirectChild) &&
      !postSplitTargetsEqual(pSort->node.pTargets, pDirectChild->pTargets)) {
    return TSDB_CODE_SUCCESS;
  }
  if (NULL == pSort->node.pParent) {
    TSWAP(pSort->node.pTargets, pDirectChild->pTargets);
  }
  code = replaceLogicNode(pSubplan, (SLogicNode*)pSort, pDirectChild);
  if (TSDB_CODE_SUCCESS == code) {
    NODES_CLEAR_LIST(pSort->node.pChildren);
    nodesDestroyNode((SNode*)pSort);
  }
  return code;
}

static void postSplitDowngradeInterpProducers(SLogicNode* pNode) {
  if (NULL == pNode) {
    return;
  }

  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    SScanLogicNode* pScan = (SScanLogicNode*)pNode;
    if (INTERP_MODE_LEGACY != pScan->interpMode) {
      pScan->interpMode = INTERP_MODE_LEGACY;
    }
  } else if (QUERY_NODE_LOGIC_PLAN_PARTITION == nodeType(pNode)) {
    ((SPartitionLogicNode*)pNode)->phasedMode = false;
  }

  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    postSplitDowngradeInterpProducers((SLogicNode*)pChild);
  }
}

static bool postSplitInterpScanExecutionReady(const SScanLogicNode* pScan) {
  // MAIN_FIRST owns its per-phase reader directions. The physical scan must
  // contribute exactly one static ASC pass.
  return NULL != pScan && 1 == pScan->scanSeq[0] &&
         0 == pScan->scanSeq[1] && !pScan->node.dynamicOp;
}

// Pure plan-shape check: is pInterp a finalizable interp-over-scan? Callers
// decide separately whether the MAIN_FIRST consumer is enabled yet.
static bool postSplitInterpShapeReady(SInterpFuncLogicNode* pInterp,
                                      SScanLogicNode** ppScan) {
  if (INTERP_MODE_MAIN_FIRST != pInterp->interpMode &&
      INTERP_MODE_GROUP_FIRST != pInterp->interpMode) {
    return false;
  }

  SLogicNode*     pInput = NULL;
  SScanLogicNode* pScan = pdcInterpFindScan(pInterp, &pInput);
  if (NULL == pScan) {
    return false;
  }

  bool scanReady = postSplitInterpScanExecutionReady(pScan) &&
                   (SCAN_TYPE_TABLE == pScan->scanType ||
                    SCAN_TYPE_TABLE_MERGE == pScan->scanType);
  if (!scanReady || !pdcInterpInputEligible(pInput)) {
    return false;
  }

  *ppScan = pScan;
  return true;
}

static bool postSplitMultiGroupShapeReady(
    SInterpFuncLogicNode* pInterp,
    SPartitionLogicNode** ppPartition,
    SScanLogicNode** ppScan) {
  if (INTERP_MODE_MULTI_GROUP != pInterp->interpMode) {
    return false;
  }

  SLogicNode*     pInput = NULL;
  SScanLogicNode* pScan = pdcInterpFindScan(pInterp, &pInput);
  if (NULL == pScan || NULL == pInput ||
      QUERY_NODE_LOGIC_PLAN_PARTITION != nodeType(pInput)) {
    return false;
  }

  bool scanReady =
      partitionKeysAreOrdinaryColumns(
          ((SPartitionLogicNode*)pInput)->pPartitionKeys) &&
      postSplitInterpScanExecutionReady(pScan) &&
      (SCAN_TYPE_TABLE == pScan->scanType ||
       SCAN_TYPE_TABLE_MERGE == pScan->scanType) &&
      !pScan->paraTablesSort && !pScan->virtualStableScan &&
      !pdcInterpScanFromVirtualTable(pScan) &&
      (NULL != pScan->pExtScanRange ||
       NULL != pScan->pExtTimeRange) &&
      (NULL == pScan->pGroupTags || pScan->groupSort);
  if (!scanReady) {
    return false;
  }

  *ppPartition = (SPartitionLogicNode*)pInput;
  *ppScan = pScan;
  return true;
}

static SLogicSubplan* postSplitFindChildSubplan(
    SLogicSubplan* pSubplan, int32_t groupId) {
  SNode* pChild = NULL;
  FOREACH(pChild, pSubplan->pChildren) {
    SLogicSubplan* pChildSubplan = (SLogicSubplan*)pChild;
    if (pChildSubplan->id.groupId == groupId) {
      return pChildSubplan;
    }
  }
  return NULL;
}

static SPartitionLogicNode* postSplitFindPendingInterpSplit(
    SInterpFuncLogicNode* pInterp) {
  if (1 != LIST_LENGTH(pInterp->node.pChildren)) {
    return NULL;
  }

  SLogicNode* pInput = (SLogicNode*)nodesListGetNode(
      pInterp->node.pChildren, 0);
  if (QUERY_NODE_LOGIC_PLAN_PARTITION != nodeType(pInput)) {
    return NULL;
  }

  SPartitionLogicNode* pPartition = (SPartitionLogicNode*)pInput;
  return partTagsOptHasPendingInterpSplit(pPartition)
             ? pPartition
             : NULL;
}

static SScanLogicNode* postSplitFindInterpSplitScan(
    SLogicSubplan* pSubplan, SPartitionLogicNode* pPartition) {
  if (1 != LIST_LENGTH(pPartition->node.pChildren)) {
    return NULL;
  }

  SLogicNode* pInput = (SLogicNode*)nodesListGetNode(
      pPartition->node.pChildren, 0);
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pInput)) {
    return (SScanLogicNode*)pInput;
  }
  if (QUERY_NODE_LOGIC_PLAN_MERGE != nodeType(pInput)) {
    return NULL;
  }

  SLogicSubplan* pSourceSubplan = postSplitFindChildSubplan(
      pSubplan, ((SMergeLogicNode*)pInput)->srcGroupId);
  if (NULL == pSourceSubplan || NULL == pSourceSubplan->pNode ||
      QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pSourceSubplan->pNode)) {
    return NULL;
  }
  return (SScanLogicNode*)pSourceSubplan->pNode;
}

static bool postSplitDistributedInterpShapeReady(
    SLogicSubplan* pSubplan, SInterpFuncLogicNode* pInterp,
    SScanLogicNode** ppScan) {
  if (INTERP_MODE_MAIN_FIRST != pInterp->interpMode ||
      1 != LIST_LENGTH(pInterp->node.pChildren)) {
    return false;
  }

  SLogicNode* pInput = (SLogicNode*)nodesListGetNode(
      pInterp->node.pChildren, 0);
  if (QUERY_NODE_LOGIC_PLAN_MERGE != nodeType(pInput)) {
    return false;
  }

  SMergeLogicNode* pMerge = (SMergeLogicNode*)pInput;
  if (!pMerge->needSort || pMerge->numOfChannels <= 0 ||
      pMerge->srcGroupId != pMerge->srcEndGroupId ||
      pMerge->node.outputTsOrder != ORDER_ASC ||
      pMerge->node.resultDataOrder < DATA_ORDER_LEVEL_IN_GROUP ||
      !pMerge->groupSort || pMerge->ignoreGroupId) {
    return false;
  }

  SLogicSubplan* pSourceSubplan = postSplitFindChildSubplan(
      pSubplan, pMerge->srcGroupId);
  if (pSourceSubplan == NULL || pSourceSubplan->pNode == NULL ||
      QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pSourceSubplan->pNode)) {
    return false;
  }

  SScanLogicNode* pScan = (SScanLogicNode*)pSourceSubplan->pNode;
  if (!postSplitInterpScanExecutionReady(pScan) ||
      pScan->scanType != SCAN_TYPE_TABLE_MERGE ||
      pScan->paraTablesSort || pScan->virtualStableScan ||
      (pScan->pExtScanRange == NULL && pScan->pExtTimeRange == NULL)) {
    return false;
  }
  if (pMerge->node.resultDataOrder == DATA_ORDER_LEVEL_IN_GROUP &&
      pScan->pGroupTags == NULL) {
    return false;
  }

  *ppScan = pScan;
  return true;
}

static bool postSplitDistributedMultiGroupShapeReady(
    SLogicSubplan* pSubplan, SInterpFuncLogicNode* pInterp,
    SPartitionLogicNode** ppPartition,
    SMergeLogicNode** ppMerge,
    SScanLogicNode** ppScan) {
  if (INTERP_MODE_MULTI_GROUP != pInterp->interpMode ||
      1 != LIST_LENGTH(pInterp->node.pChildren)) {
    return false;
  }

  SLogicNode* pInput = (SLogicNode*)nodesListGetNode(
      pInterp->node.pChildren, 0);
  if (QUERY_NODE_LOGIC_PLAN_PARTITION != nodeType(pInput)) {
    return false;
  }

  SPartitionLogicNode* pPartition = (SPartitionLogicNode*)pInput;
  if (!partitionKeysAreOrdinaryColumns(pPartition->pPartitionKeys) ||
      1 != LIST_LENGTH(pPartition->node.pChildren)) {
    return false;
  }

  SLogicNode* pPartitionInput = (SLogicNode*)nodesListGetNode(
      pPartition->node.pChildren, 0);
  if (QUERY_NODE_LOGIC_PLAN_MERGE != nodeType(pPartitionInput)) {
    return false;
  }

  SMergeLogicNode* pMerge = (SMergeLogicNode*)pPartitionInput;
  if (!pMerge->needSort || pMerge->numOfChannels <= 0 ||
      pMerge->srcGroupId != pMerge->srcEndGroupId ||
      pMerge->node.outputTsOrder != ORDER_ASC ||
      pMerge->groupSort || pMerge->ignoreGroupId ||
      0 == LIST_LENGTH(pMerge->pMergeKeys)) {
    return false;
  }

  SLogicSubplan* pSourceSubplan = postSplitFindChildSubplan(
      pSubplan, pMerge->srcGroupId);
  if (NULL == pSourceSubplan || NULL == pSourceSubplan->pNode ||
      QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pSourceSubplan->pNode)) {
    return false;
  }

  SScanLogicNode* pScan = (SScanLogicNode*)pSourceSubplan->pNode;
  if (!postSplitInterpScanExecutionReady(pScan) ||
      SCAN_TYPE_TABLE_MERGE != pScan->scanType ||
      pScan->paraTablesSort || pScan->virtualStableScan ||
      pdcInterpScanFromVirtualTable(pScan) ||
      (NULL != pScan->pGroupTags && !pScan->groupSort) ||
      (NULL == pScan->pExtScanRange &&
       NULL == pScan->pExtTimeRange)) {
    return false;
  }

  *ppPartition = pPartition;
  *ppMerge = pMerge;
  *ppScan = pScan;
  return true;
}

static int32_t postSplitFinalizeInterpNodes(SLogicSubplan* pSubplan,
                                            SLogicNode* pNode) {
  if (NULL == pNode) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  if (QUERY_NODE_LOGIC_PLAN_INTERP_FUNC == nodeType(pNode)) {
    SInterpFuncLogicNode* pInterp = (SInterpFuncLogicNode*)pNode;
    SScanLogicNode*       pScan = NULL;
    SPartitionLogicNode*  pPartition = NULL;
    SMergeLogicNode*      pMerge = NULL;
    bool mainFirstReady =
        pdcInterpMainFirstConsumerReady &&
        (postSplitInterpShapeReady(pInterp, &pScan) ||
         postSplitDistributedInterpShapeReady(
             pSubplan, pInterp, &pScan));
    bool multiGroupReady =
        postSplitMultiGroupShapeReady(
            pInterp, &pPartition, &pScan) ||
        postSplitDistributedMultiGroupShapeReady(
            pSubplan, pInterp, &pPartition, &pMerge, &pScan);
    if (mainFirstReady) {
      pInterp->interpMode = pScan->scanType == SCAN_TYPE_TABLE
                                ? INTERP_MODE_GROUP_FIRST
                                : INTERP_MODE_MAIN_FIRST;
      pScan->interpMode = pInterp->interpMode;
    } else if (multiGroupReady) {
      pScan->interpMode = INTERP_MODE_MULTI_GROUP;
      pScan->dataRequired = FUNC_DATA_REQUIRED_DATA_LOAD;
      pPartition->phasedMode = true;
      if (NULL != pMerge && NULL != pScan->pGroupTags) {
        pMerge->inputWithGroupId = true;
        pMerge->outerGroupPhaseMode = true;
      }
    } else if (INTERP_MODE_LEGACY != pInterp->interpMode) {
      pInterp->interpMode = INTERP_MODE_LEGACY;
    }

    SPartitionLogicNode* pPendingPartition =
        postSplitFindPendingInterpSplit(pInterp);
    if (NULL != pPendingPartition) {
      if (multiGroupReady) {
        code = partTagsOptCommitInterpSplit(
            pPendingPartition, pMerge, pScan);
      } else {
        SScanLogicNode* pPendingScan =
            postSplitFindInterpSplitScan(pSubplan,
                                         pPendingPartition);
        code = partTagsOptRollbackInterpSplit(
            pPendingPartition, pPendingScan);
      }
    }
  } else if (QUERY_NODE_LOGIC_PLAN_PARTITION == nodeType(pNode) &&
             partTagsOptHasPendingInterpSplit(
                 (SPartitionLogicNode*)pNode)) {
    // This Partition was not finalized through direct Interp adjacency.
    SPartitionLogicNode* pPartition = (SPartitionLogicNode*)pNode;
    code = partTagsOptRollbackInterpSplit(
        pPartition,
        postSplitFindInterpSplitScan(pSubplan, pPartition));
  }

  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
    code = postSplitFinalizeInterpNodes(
        pSubplan, (SLogicNode*)pChild);
  }
  return code;
}

static int32_t postSplitFinalizeInterpSubplans(SLogicSubplan* pSubplan) {
  if (NULL == pSubplan) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  SNode* pChild = NULL;
  FOREACH(pChild, pSubplan->pChildren) {
    code = postSplitFinalizeInterpSubplans(
        (SLogicSubplan*)pChild);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  // Child subplans are cleared first so a parent Interp can re-enable only
  // the TableMergeScan selected by its Merge source group.
  postSplitDowngradeInterpProducers(pSubplan->pNode);
  return postSplitFinalizeInterpNodes(pSubplan, pSubplan->pNode);
}

int32_t postSplitOptimize(SPlanContext* pCxt, SLogicSubplan* pLogicSubplan) {
  if (NULL == pLogicSubplan) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  if (NULL != pLogicSubplan->pNode) {
    code = postSplitEliminateSortImpl(pLogicSubplan,
                                      pLogicSubplan->pNode);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = postSplitFinalizeInterpSubplans(pLogicSubplan);
  }
  return code;
}

static int32_t dumpLogicSubplan(const char* pRuleName, SLogicSubplan* pSubplan) {
  int32_t code = TSDB_CODE_SUCCESS;
  char*   pStr = NULL;

  if (!tsQueryPlannerTrace) {
    return code;
  }

  PLAN_ERR_JRET(nodesNodeToString((SNode*)pSubplan, false, &pStr, NULL));
  if (NULL == pRuleName) {
    qDebugL("before optimize, JsonPlan: %s", pStr);
  } else {
    qDebugL("apply optimize %s rule, JsonPlan: %s", pRuleName, pStr);
  }

_return:
  taosMemoryFree(pStr);
  if (TSDB_CODE_SUCCESS != code) {
    planError("%s failed since %s", __func__, tstrerror(code));
  }
  return code;
}

static int32_t applyOptimizeRule(SPlanContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SOptimizeContext cxt = {
      .pPlanCxt = pCxt,
      .pExtCapability = (NULL != pCxt) ? pCxt->pExtCapability : NULL,
      .pSessionOverlay = (NULL != pCxt) ? pCxt->pExtSessionOverlay : NULL,
      .pPushdownAttempt = (NULL != pCxt) ? pCxt->pExtPushdownAttempt : NULL,
      .optimized = false,
  };
  bool             optimized = false;
  int32_t          code = TSDB_CODE_SUCCESS;
  PLAN_ERR_JRET(dumpLogicSubplan(NULL, pLogicSubplan));
  do {
    optimized = false;
    for (int32_t i = 0; i < optimizeRuleNum; ++i) {
      cxt.optimized = false;
      code = optimizeRuleSet[i].optimizeFunc(&cxt, pLogicSubplan);
      if (TSDB_CODE_SUCCESS != code) {
        planError("optimize rule [%s] failed, code:%s", optimizeRuleSet[i].pName, tstrerror(code));
        goto _return;
      }
      if (cxt.optimized) {
        optimized = true;
        PLAN_ERR_JRET(dumpLogicSubplan(optimizeRuleSet[i].pName, pLogicSubplan));
        break;
      }
    }
  } while (optimized && (TSDB_CODE_SUCCESS == code));

_return:
  if (TSDB_CODE_SUCCESS != code) {
    planError("%s failed since %s", __func__, tstrerror(code));
  }
  return code;
}
