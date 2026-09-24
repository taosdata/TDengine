/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#ifndef _TD_PLANNER_IMPL_H_
#define _TD_PLANNER_IMPL_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "planner.h"
#include "tsimplehash.h"
#include "taoserror.h"


typedef struct SPhysiPlanContext {
  SPlanContext* pPlanCxt;
  int32_t       errCode;
  int64_t       nextDataBlockId;
  SArray*       pLocationHelper;
  SArray*       pProjIdxLocHelper;
  bool          hasFederatedScan;  // set when SCAN_TYPE_EXTERNAL subplan is built
} SPhysiPlanContext;

#define planFatal(...)  qFatal ("plan " __VA_ARGS__)
#define planError(...)  qError ("plan " __VA_ARGS__)
#define planWarn(...)   qWarn  ("plan " __VA_ARGS__)
#define planInfo(...)   qInfo  ("plan " __VA_ARGS__)
#define planDebug(...)  qDebug ("plan " __VA_ARGS__)
#define planDebugL(...) qDebugL("plan " __VA_ARGS__)
#define planTrace(...)  qTrace ("plan " __VA_ARGS__)

#define IMPLICIT_PARTITION_KEY_ALIAS "#implicit_partition"

#define PLAN_ERR_RET(c)                \
  do {                                 \
    int32_t _code = c;                 \
    if (_code != TSDB_CODE_SUCCESS) {  \
      terrno = _code;                  \
      return _code;                    \
    }                                  \
  } while (0)
#define PLAN_RET(c)                    \
  do {                                 \
    int32_t _code = c;                 \
    if (_code != TSDB_CODE_SUCCESS) {  \
      terrno = _code;                  \
    }                                  \
    return _code;                      \
  } while (0)
#define PLAN_ERR_JRET(c)              \
  do {                                \
    code = c;                         \
    if (code != TSDB_CODE_SUCCESS) {  \
      terrno = code;                  \
      goto _return;                   \
    }                                 \
  } while (0)


int32_t generateUsageErrMsg(char* pBuf, int32_t len, int32_t errCode, ...);
int32_t createColumnByRewriteExprs(SNodeList* pExprs, SNodeList** pList);
int32_t createColumnByRewriteExpr(SNode* pExpr, SNodeList** pList);
int32_t createColumnByRemoteOutputExprs(SNodeList* pExprs, SNodeList** pList);
int32_t replaceProjectLogicProjections(SLogicNode* pNode, const SNodeList* pExprs);
int32_t replaceLogicNode(SLogicSubplan* pSubplan, SLogicNode* pOld, SLogicNode* pNew);
int32_t adjustLogicNodeDataRequirement(SLogicNode* pNode, EDataOrderLevel requirement);
SLogicNode*    planGetSubqueryRenderLogic(const SPlanContext* pCxt, int32_t subQIdx, ENodeType remoteNodeType);
bool           planIsSubqueryPushedDown(const SPlanContext* pCxt, int32_t subQIdx);
bool           planIsExternalWindowSubquery(const SPlanContext* pCxt, int32_t subQIdx);
int32_t        planMarkSubqueryPushedDown(SPlanContext* pCxt, int32_t subQIdx);

int32_t createLogicPlan(SPlanContext* pCxt, SLogicSubplan** pLogicSubplan);
void    planWalkLogicExprFields(SLogicNode* pNode, FNodeWalker walker, void* pContext);
int32_t optimizeLogicPlan(SPlanContext* pCxt, SLogicSubplan* pLogicSubplan);
int32_t splitLogicPlan(SPlanContext* pCxt, SLogicSubplan* pLogicSubplan);
int32_t postSplitOptimize(SPlanContext* pCxt, SLogicSubplan* pLogicSubplan);
int32_t scaleOutLogicPlan(SPlanContext* pCxt, SLogicSubplan* pLogicSubplan, SQueryLogicPlan** pLogicPlan);
int32_t createPhysiPlan(SPlanContext* pCxt, SQueryLogicPlan* pLogicPlan, SQueryPlan** pPlan, SArray* pExecNodeList);
int32_t validateQueryPlan(SPlanContext* pCxt, SQueryPlan* pPlan);
int32_t planCheckRequiredExtColumns(const SExtTableNode* pExtNode, const SNodeList* pScanCols);

bool        getBatchScanOptionFromHint(SNodeList* pList);
bool        getSortForGroupOptHint(SNodeList* pList);
bool        getParaTablesSortOptHint(SNodeList* pList);
bool        getSmallDataTsSortOptHint(SNodeList* pList);
bool        getSmallDataScanSortOptHint(SNodeList* pList);
bool        getHashJoinOptHint(SNodeList* pList);
bool        getOptHint(SNodeList* pList, EHintOption hint);
SLogicNode* getLogicNodeRootNode(SLogicNode* pCurr);
int32_t     collectTableAliasFromNodes(SNode* pNode, SSHashObj** ppRes);
bool        isPartTableAgg(SAggLogicNode* pAgg);
bool        isPartTagAgg(SAggLogicNode* pAgg);
bool        isPartTableWinodw(SWindowLogicNode* pWindow);
bool        isPartTableInterp(SInterpFuncLogicNode* pInterp);
void        planPromoteScanToTableMerge(SScanLogicNode* pScan, EDataOrderLevel requireLevel,
                                        EDataOrderLevel resultLevel);
int32_t     planReplaceMergeWithSort(SScanLogicNode* pScan, bool* pReplaced);
bool        scanIsSmallDataScanSortHinted(const SScanLogicNode* pScan);
// Defined in planSpliter.c; shared so planReplaceMergeWithSort builds the same
// scan sort keys as the table-merge-scan split path instead of duplicating them.
int32_t     stbSplFindPrimaryKeyFromScan(SScanLogicNode* pScan, SNode** ppNode);
int32_t     stbSplFindPkFromScan(SScanLogicNode* pScan, SNode** ppNode);
int32_t     stbSplCreateMergeKeysByExpr(SNode* pExpr, EOrder order, SNodeList** pMergeKeys);
bool        keysHasCol(SNodeList* pKeys);
bool        keysHasTagOrTbname(SNodeList* pKeys);
bool        partitionKeyHasOrdinaryColumn(SNode* pKey);
bool        partitionKeyHasTagOrTbname(SNode* pKey);
bool        partitionKeysAreOrdinaryColumns(SNodeList* pKeys);
bool        keysHasTbname(SNodeList* pKeys);
bool        planAggFuncsHasFirst(SNodeList* pFuncs);
bool        projectCouldMergeUnsortDataBlock(SProjectLogicNode* pProject);
SFunctionNode* createGroupKeyAggFunc(SColumnNode* pGroupCol);
int32_t getTimeRangeFromNode(SNode** pPrimaryKeyCond, STimeWindow* pTimeRange, bool* pIsStrict);
int32_t tagScanSetExecutionMode(SScanLogicNode* pScan);

#define CLONE_LIMIT 1
#define CLONE_SLIMIT 1 << 1
#define CLONE_LIMIT_SLIMIT (CLONE_LIMIT | CLONE_SLIMIT)
bool    limitHasFiniteRows(const SNode* pLimit);
int32_t cloneLimit(SLogicNode* pParent, SLogicNode* pChild, uint8_t cloneWhat, bool* pCloned);
int32_t sortPriKeyOptGetSequencingNodesImpl(SLogicNode* pNode, bool groupSort, SSortLogicNode* pSort,
                                                   bool* pNotOptimize, SNodeList** pSequencingNodes, bool* keepSort);
bool isColRefExpr(const SColumnNode* pCol, const SExprNode* pExpr);
void rewriteTargetsWithResId(SNodeList* pTargets);
bool checkScanLogicNode(SLogicNode* pNode);
bool inStreamCalcClause(SPlanContext* pCxt);
bool inStreamTriggerClause(SPlanContext* pCxt);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PLANNER_IMPL_H_*/
