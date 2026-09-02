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
#include "scalar.h"
#include "filter.h"

static char* getUsageErrFormat(int32_t errCode) {
  switch (errCode) {
    case TSDB_CODE_PLAN_EXPECTED_TS_EQUAL:
      return "primary timestamp equal condition is expected in join conditions";
    case TSDB_CODE_PLAN_NOT_SUPPORT_CROSS_JOIN:
      return "not support cross join";
    case TSDB_CODE_PLAN_NOT_SUPPORT_JOIN_COND:
      return "Not supported join conditions";
    case TSDB_CODE_PAR_NOT_SUPPORT_JOIN:
      return "Not supported join since '%s'";
    case TSDB_CODE_PLAN_SLOT_NOT_FOUND:
      return "not found slot id by slot key";
    case TSDB_CODE_PLAN_INVALID_TABLE_TYPE:
      return "Planner invalid table type";
    case TSDB_CODE_PLAN_INVALID_DYN_CTRL_TYPE:
      return "Planner invalid query control plan type";
    case TSDB_CODE_PLAN_INVALID_WINDOW_TYPE:
      return "Planner invalid window type";
    default:
      break;
  }
  return "Unknown error";
}

int32_t generateUsageErrMsg(char* pBuf, int32_t len, int32_t errCode, ...) {
  va_list vArgList;
  va_start(vArgList, errCode);
  (void)vsnprintf(pBuf, len, getUsageErrFormat(errCode), vArgList);
  va_end(vArgList);
  return errCode;
}

void planPromoteScanToTableMerge(SScanLogicNode* pScan, EDataOrderLevel requireLevel, EDataOrderLevel resultLevel) {
  if (requireLevel < DATA_ORDER_LEVEL_IN_BLOCK) {
    requireLevel = DATA_ORDER_LEVEL_IN_BLOCK;
  }
  if (resultLevel < DATA_ORDER_LEVEL_IN_BLOCK) {
    resultLevel = DATA_ORDER_LEVEL_IN_BLOCK;
  }

  pScan->scanType = SCAN_TYPE_TABLE_MERGE;
  pScan->filesetDelimited = true;
  pScan->node.requireDataOrder = requireLevel;
  pScan->node.resultDataOrder = resultLevel;
}

typedef struct SCreateColumnCxt {
  int32_t    errCode;
  SNodeList* pList;
} SCreateColumnCxt;

static EDealRes doCreateColumn(SNode* pNode, void* pContext) {
  SCreateColumnCxt* pCxt = (SCreateColumnCxt*)pContext;
  switch (nodeType(pNode)) {
    case QUERY_NODE_COLUMN: {
      SNode* pCol = NULL;
      pCxt->errCode = nodesCloneNode(pNode, &pCol);
      if (NULL == pCol) {
        return DEAL_RES_ERROR;
      }
      return (TSDB_CODE_SUCCESS == nodesListAppend(pCxt->pList, pCol) ? DEAL_RES_IGNORE_CHILD : DEAL_RES_ERROR);
    }
    case QUERY_NODE_VALUE:
    case QUERY_NODE_OPERATOR:
    case QUERY_NODE_LOGIC_CONDITION:
    case QUERY_NODE_FUNCTION:
    case QUERY_NODE_CASE_WHEN: 
    case QUERY_NODE_REMOTE_VALUE: {
      SExprNode*   pExpr = (SExprNode*)pNode;
      SColumnNode* pCol = NULL;
      pCxt->errCode = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
      if (NULL == pCol) {
        return DEAL_RES_ERROR;
      }
      pCol->node.resType = pExpr->resType;
      tstrncpy(pCol->colName, pExpr->aliasName, TSDB_COL_NAME_LEN);
      if (QUERY_NODE_FUNCTION == nodeType(pNode)) {
        SFunctionNode* pFunc = (SFunctionNode*)pNode;
        if (pFunc->funcType == FUNCTION_TYPE_TBNAME) {
          SValueNode* pVal = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 0);
          if (NULL != pVal) {
            tstrncpy(pCol->tableAlias, pVal->literal, TSDB_TABLE_NAME_LEN);
            tstrncpy(pCol->tableName, pVal->literal, TSDB_TABLE_NAME_LEN);
          }
        }
      }
      pCol->node.relatedTo = pExpr->relatedTo;
      return (TSDB_CODE_SUCCESS == nodesListStrictAppend(pCxt->pList, (SNode*)pCol) ? DEAL_RES_IGNORE_CHILD
                                                                                    : DEAL_RES_ERROR);
    }
    default:
      break;
  }

  return DEAL_RES_CONTINUE;
}

int32_t createColumnByRewriteExprs(SNodeList* pExprs, SNodeList** pList) {
  SCreateColumnCxt cxt = {.errCode = TSDB_CODE_SUCCESS, .pList = *pList};
  if (!cxt.pList) {
    int32_t code = nodesMakeList(&cxt.pList);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  nodesWalkExprs(pExprs, doCreateColumn, &cxt);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyList(cxt.pList);
    return cxt.errCode;
  }
  if (NULL == *pList) {
    *pList = cxt.pList;
  }
  return cxt.errCode;
}

int32_t createColumnByRewriteExpr(SNode* pExpr, SNodeList** pList) {
  SCreateColumnCxt cxt = {.errCode = TSDB_CODE_SUCCESS, .pList = *pList};
  if (!cxt.pList) {
    int32_t code = nodesMakeList(&cxt.pList);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  nodesWalkExpr(pExpr, doCreateColumn, &cxt);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyList(cxt.pList);
    return cxt.errCode;
  }
  if (NULL == *pList) {
    *pList = cxt.pList;
  }
  return cxt.errCode;
}

int32_t replaceLogicNode(SLogicSubplan* pSubplan, SLogicNode* pOld, SLogicNode* pNew) {
  pNew->stmtRoot = pOld->stmtRoot;
  if (NULL == pOld->pParent) {
    pSubplan->pNode = (SLogicNode*)pNew;
    pNew->pParent = NULL;
    return TSDB_CODE_SUCCESS;
  }

  SNode* pNode;
  FOREACH(pNode, pOld->pParent->pChildren) {
    if (nodesEqualNode(pNode, (SNode*)pOld)) {
      REPLACE_NODE(pNew);
      pNew->pParent = pOld->pParent;
      return TSDB_CODE_SUCCESS;
    }
  }
  return TSDB_CODE_PLAN_INTERNAL_ERROR;
}

SLogicNode* getLogicNodeRootNode(SLogicNode* pCurr) {
  while (pCurr) {
    if (pCurr->stmtRoot || NULL == pCurr->pParent) {
      return pCurr;
    }

    pCurr = pCurr->pParent;
  }

  return NULL;
}

// SMALLDATA_SCAN_SORT applies only to super-table scans (the only scan kind that
// fuses into a table merge scan), so the hint takes effect only on one.
bool scanIsSmallDataScanSortHinted(const SScanLogicNode* pScan) {
  return pScan->smallDataScanSort && TSDB_SUPER_TABLE == pScan->tableType;
}

// Hint SMALLDATA_SCAN_SORT: replace a would-be table-merge scan with a plain
// table scan plus a Sort node, inserted between pScan and its current parent.
// The Sort provides the ts (and pk) ordering that the table merge scan used to
// fuse in.  Any scan-level limit is moved onto the Sort (sort-then-limit).
//
// The ts/pk sort keys are built with the same helpers the table-merge-scan split
// path uses (stbSplFindPrimaryKeyFromScan / stbSplFindPkFromScan /
// stbSplCreateMergeKeysByExpr in planSpliter.c), so the inserted Sort orders by
// exactly what the merge scan would have.
int32_t planReplaceMergeWithSort(SScanLogicNode* pScan, bool* pReplaced) {
  int32_t         code = TSDB_CODE_SUCCESS;
  SSortLogicNode* pSort = NULL;
  SNode*          pTs = NULL;
  SNode*          pPk = NULL;
  // This runs during data-requirement adjustment (logic-plan creation), before the
  // optimizer can flip scanSeq for a DESC scan, so scanSeq[0] is always its creation
  // default (>0 => ASC).  Every consumer that reaches this branch (session/state/
  // interval windows with no ORDER BY) requires ASC ts input, so ASC is correct
  // here; the DESC flip happens later in sortPrimaryKeyOptimize.
  EOrder          order = (pScan->scanSeq[0] > 0) ? ORDER_ASC : ORDER_DESC;

  *pReplaced = false;

  // Locate the primary ts sort key BEFORE touching the scan.  If the scan carries
  // no primary ts column we cannot build an ordering Sort; leave the scan untouched
  // and report it (pReplaced stays false) so the caller keeps the correct,
  // order-providing table merge scan instead of an unordered plain scan.  A
  // super-table scan that needs global order always carries the ts column, so this
  // is a defensive guard, not an expected path.
  code = stbSplFindPrimaryKeyFromScan(pScan, &pTs);
  if (TSDB_CODE_SUCCESS != code || NULL == pTs) {
    return code;
  }

  // We have a ts key: demote to a plain table scan and build the Sort above it.
  pScan->scanType = SCAN_TYPE_TABLE;
  pScan->filesetDelimited = false;

  code = stbSplFindPkFromScan(pScan, &pPk);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SORT, (SNode**)&pSort);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pSort->groupSort = (NULL != pScan->pGroupTags);
    pSort->node.precision = pScan->node.precision;
    pSort->node.outputTsOrder = order;
    pSort->node.inputTsOrder = pScan->node.outputTsOrder;
    // A Sort needs no order from its input and guarantees globally ordered output.
    pSort->node.requireDataOrder = DATA_ORDER_LEVEL_NONE;
    pSort->node.resultDataOrder = DATA_ORDER_LEVEL_GLOBAL;
    code = nodesCloneList(pScan->node.pTargets, &pSort->node.pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = stbSplCreateMergeKeysByExpr(pTs, order, &pSort->pSortKeys);
  }
  if (TSDB_CODE_SUCCESS == code && NULL != pPk) {
    code = stbSplCreateMergeKeysByExpr(pPk, order, &pSort->pSortKeys);
  }

  // Splice pSort between pScan and pScan->node.pParent: pSort takes the exact slot
  // pScan occupied in the parent's child list, and pScan becomes pSort's only
  // child.  The parent-slot swap is done first (it can fail); pScan is appended to
  // pSort only after success, so destroying pSort on the error path never frees the
  // still-live pScan.
  if (TSDB_CODE_SUCCESS == code && NULL != pScan->node.pParent) {
    SLogicNode* pParent = pScan->node.pParent;
    bool        replaced = false;
    SNode*      pNode = NULL;
    FOREACH(pNode, pParent->pChildren) {
      if (nodesEqualNode(pNode, (SNode*)pScan)) {
        REPLACE_NODE(pSort);
        replaced = true;
        break;
      }
    }
    code = replaced ? TSDB_CODE_SUCCESS : TSDB_CODE_PLAN_INTERNAL_ERROR;
  }
  if (TSDB_CODE_SUCCESS == code) {
    pSort->node.stmtRoot = pScan->node.stmtRoot;
    pSort->node.pParent = pScan->node.pParent;
    pScan->node.pParent = (SLogicNode*)pSort;
    pScan->node.stmtRoot = false;
    code = nodesListMakeAppend(&pSort->node.pChildren, (SNode*)pScan);
    if (TSDB_CODE_SUCCESS != code) {
      // Roll back the parent-slot swap so pScan is reachable again, then bail.
      SNode* pNode = NULL;
      if (NULL != pSort->node.pParent) {
        FOREACH(pNode, pSort->node.pParent->pChildren) {
          if (nodesEqualNode(pNode, (SNode*)pSort)) {
            REPLACE_NODE(pScan);
            break;
          }
        }
      }
      pScan->node.pParent = pSort->node.pParent;
      pSort->node.pParent = NULL;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    // Move any scan-level limit above the Sort (sort-then-limit, not limit-then-sort)
    // only after the splice succeeded, so a failed splice never strands the moved
    // limit on the to-be-destroyed Sort.
    if (NULL != pScan->node.pLimit) {
      pSort->node.pLimit = pScan->node.pLimit;
      pScan->node.pLimit = NULL;
    }
    *pReplaced = true;
  } else {
    nodesDestroyNode((SNode*)pSort);
  }
  return code;
}

// Hint SMALLDATA_SCAN_SORT: decide whether a Sort already orders this scan's
// output.  Walk up the parent chain; an explicit Sort (the ORDER BY case) is the
// order source and no Sort needs to be inserted.  If an order-consuming operator
// (window/join/agg/partition/...) is reached first, the consumer relies on the
// (would-be) merge scan for ordering, so a Sort must be inserted instead.
static bool planScanHasSortAncestor(SScanLogicNode* pScan) {
  SLogicNode* pParent = pScan->node.pParent;
  while (NULL != pParent) {
    switch (nodeType(pParent)) {
      case QUERY_NODE_LOGIC_PLAN_SORT:
        return true;
      case QUERY_NODE_LOGIC_PLAN_PROJECT:
        // Projection preserves row order; keep walking toward an ORDER BY Sort.
        pParent = pParent->pParent;
        break;
      default:
        // Any other consumer (window, join, agg, partition, merge, fill, ...) is
        // the order requester; there is no Sort providing the order.  Defaulting an
        // unrecognized node to "requester" is the safe direction: it inserts a Sort
        // (at worst redundant) rather than leaving an order-needing consumer unordered.
        return false;
    }
  }
  return false;
}

// Note on phase ordering (load-bearing): this runs during logic-plan creation
// (adjustLogicNodeDataRequirement), BEFORE optimizeLogicPlan.  Inserting the Sort
// here, between an order-consuming partition/window and the scan, is what stops
// partTagsOptimize from later folding a tag partition into the scan's pGroupTags
// (partTagsIsOptimizableNode requires the partition's direct child to be a SCAN).
// A separate Partition operator therefore survives to regroup the ts-ordered
// stream, and pScan->pGroupTags stays NULL when planReplaceMergeWithSort runs (so
// it never builds a group-sort over a non-group-ordered scan).  Do NOT move this
// into an optimize-phase rule: running after partTagsOptimize would re-enable that
// fold and produce a group-sort scan whose per-partition order is not established.
static int32_t adjustScanDataRequirement(SScanLogicNode* pScan, EDataOrderLevel requirement) {
  int32_t code = TSDB_CODE_SUCCESS;
  if ((SCAN_TYPE_TABLE != pScan->scanType && SCAN_TYPE_TABLE_MERGE != pScan->scanType) ||
      DATA_ORDER_LEVEL_GLOBAL == pScan->node.requireDataOrder) {
    return TSDB_CODE_SUCCESS;
  }
  // The lowest sort level of scan output data is DATA_ORDER_LEVEL_IN_BLOCK
  if (requirement < DATA_ORDER_LEVEL_IN_BLOCK) {
    requirement = DATA_ORDER_LEVEL_IN_BLOCK;
  }
  // An interp RANGE scan grouped by tbname makes every group a single child
  // table, which the storage layer already returns in timestamp order, so the
  // in-group order requirement needs no multi-table merge: a plain table scan
  // suffices and lets interp reuse the mature per-table RANGE-pruning
  // optimizations instead of the merge-scan probe. Scoped to interp
  // (pExtScanRange set) to leave other tbname-grouped queries on their existing
  // merge-scan path.
  bool interpRangeByTbname = (NULL != pScan->pExtScanRange) && keysHasTbname(pScan->pGroupTags);
  if (DATA_ORDER_LEVEL_IN_BLOCK == requirement ||
      pScan->placeholderType == SP_PARTITION_TBNAME ||
      pScan->placeholderType == SP_PARTITION_ROWS ||
      interpRangeByTbname) {
    pScan->scanType = SCAN_TYPE_TABLE;
    if (interpRangeByTbname) {
      // Emit one group per child table so each gets its own scan pass. An
      // interp RANGE scan opens a per-reader external (3-segment) reader,
      // which is not per-table; without per-table groups all child tables
      // would share one external reader and only the first would get its
      // fill-reference rows.
      pScan->groupSort = true;
    }
  } else if (TSDB_SUPER_TABLE == pScan->tableType) {
    if (pScan->smallDataScanSort) {
      if (planScanHasSortAncestor(pScan)) {
        // ORDER BY case: a Sort already orders the scan output (Task 1 behavior).
        // Leave a plain table scan; the Sort above provides global order.
        pScan->scanType = SCAN_TYPE_TABLE;
      } else {
        // Window/interval/session/state case: no Sort exists, the consumer expects
        // the merge scan to be the order source.  Demote to a plain table scan and
        // insert a Sort above it so the required order is still provided.
        bool replaced = false;
        code = planReplaceMergeWithSort(pScan, &replaced);
        if (TSDB_CODE_SUCCESS == code && !replaced) {
          // No ts sort key to build a Sort from: keep the correct table merge scan
          // rather than leave an unordered plain scan for an order-requiring consumer.
          planPromoteScanToTableMerge(pScan, pScan->node.requireDataOrder, requirement);
        }
      }
    } else {
      planPromoteScanToTableMerge(pScan, pScan->node.requireDataOrder, requirement);
    }
  }

  if (TSDB_CODE_SUCCESS == code && TSDB_NORMAL_TABLE != pScan->tableType && TSDB_CHILD_TABLE != pScan->tableType) {
    // A hinted plain scan (ORDER BY case, or after the merge scan was replaced by a
    // Sort + plain scan) only guarantees in-block order; the Sort above raises the
    // order back to GLOBAL for the consumer.  Either way the scan is left as
    // SCAN_TYPE_TABLE, so that flag distinguishes it from the table-merge fallback.
    pScan->node.resultDataOrder = (pScan->smallDataScanSort && SCAN_TYPE_TABLE == pScan->scanType)
                                      ? DATA_ORDER_LEVEL_IN_BLOCK
                                      : requirement;
  }
  return code;
}

static int32_t adjustJoinDataRequirement(SJoinLogicNode* pJoin, EDataOrderLevel requirement) {
  // The lowest sort level of join input and output data is DATA_ORDER_LEVEL_GLOBAL
  int32_t code = TSDB_CODE_SUCCESS;
  if (!pJoin->leftConstPrimGot) {
    code = adjustLogicNodeDataRequirement((SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0),
                                          pJoin->node.requireDataOrder);
  } else {
    code =
        adjustScanDataRequirement((SScanLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0), DATA_ORDER_LEVEL_NONE);
  }
  if (TSDB_CODE_SUCCESS == code && pJoin->node.pChildren->length > 1) {
    if (!pJoin->rightConstPrimGot) {
      code = adjustLogicNodeDataRequirement((SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1),
                                            pJoin->node.requireDataOrder);
    } else {
      code =
          adjustScanDataRequirement((SScanLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1), DATA_ORDER_LEVEL_NONE);
    }
  }
  if (code != TSDB_CODE_SUCCESS) {
    planError("adjust join input data requirement failed, err:%s", tstrerror(code));
  }
  return code;
}

static int32_t adjustAggDataRequirement(SAggLogicNode* pAgg, EDataOrderLevel requirement) {
  // The sort level of agg with group by output data can only be DATA_ORDER_LEVEL_NONE
  /* agg could meet the requirement when the primary key is const like function, so this check may be failed
  if (requirement > DATA_ORDER_LEVEL_NONE && (NULL != pAgg->pGroupKeys || !pAgg->onlyHasKeepOrderFunc)) {
    planError(
        "The output of aggregate cannot meet the requirements(%s) of the upper operator. "
        "Illegal statement, should be intercepted in parser",
        dataOrderStr(requirement));
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }
  */
  pAgg->node.resultDataOrder = requirement;
  if (pAgg->hasTimeLineFunc) {
    pAgg->node.requireDataOrder = requirement < DATA_ORDER_LEVEL_IN_GROUP ? DATA_ORDER_LEVEL_IN_GROUP : requirement;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t adjustProjectDataRequirement(SProjectLogicNode* pProject, EDataOrderLevel requirement) {
  pProject->node.resultDataOrder = requirement;
  pProject->node.requireDataOrder = requirement;
  return TSDB_CODE_SUCCESS;
}

static int32_t adjustIntervalDataRequirement(SWindowLogicNode* pWindow, EDataOrderLevel requirement) {
  // The lowest sort level of interval output data is DATA_ORDER_LEVEL_IN_GROUP
  if (requirement < DATA_ORDER_LEVEL_IN_GROUP) {
    requirement = DATA_ORDER_LEVEL_IN_GROUP;
  }
  // Interval can produce ordered output (IN_GROUP or stronger) from block-ordered
  // input.  Do NOT push the parent's output requirement into requireDataOrder —
  // that would over-demand from children (e.g. promoting Table Scan to Table
  // Merge Scan unnecessarily).  Keep the minimum input requirement as-is.
  if (pWindow->node.requireDataOrder < DATA_ORDER_LEVEL_IN_BLOCK) {
    pWindow->node.requireDataOrder = DATA_ORDER_LEVEL_IN_BLOCK;
  }
  pWindow->node.resultDataOrder = requirement;
  return TSDB_CODE_SUCCESS;
}

static int32_t adjustExternalDataRequirement(SWindowLogicNode* pWindow, EDataOrderLevel requirement) {
  // The lowest sort level of interval output data is DATA_ORDER_LEVEL_IN_GROUP
  if (requirement < DATA_ORDER_LEVEL_IN_GROUP) {
    requirement = DATA_ORDER_LEVEL_IN_GROUP;
  }
  // The sort level of interval input data is always DATA_ORDER_LEVEL_IN_BLOCK
  pWindow->node.resultDataOrder = requirement;
  return TSDB_CODE_SUCCESS;
}

static int32_t adjustSessionDataRequirement(SWindowLogicNode* pWindow, EDataOrderLevel requirement) {
  if (requirement <= pWindow->node.resultDataOrder) {
    return TSDB_CODE_SUCCESS;
  }
  pWindow->node.resultDataOrder = requirement;
  pWindow->node.requireDataOrder = requirement;
  return TSDB_CODE_SUCCESS;
}

static int32_t adjustStateDataRequirement(SWindowLogicNode* pWindow, EDataOrderLevel requirement) {
  if (requirement <= pWindow->node.resultDataOrder) {
    return TSDB_CODE_SUCCESS;
  }
  pWindow->node.resultDataOrder = requirement;
  pWindow->node.requireDataOrder = requirement;
  return TSDB_CODE_SUCCESS;
}

static int32_t adjustEventDataRequirement(SWindowLogicNode* pWindow, EDataOrderLevel requirement) {
  if (requirement <= pWindow->node.resultDataOrder) {
    return TSDB_CODE_SUCCESS;
  }
  pWindow->node.resultDataOrder = requirement;
  pWindow->node.requireDataOrder = requirement;
  return TSDB_CODE_SUCCESS;
}

static int32_t adjustCountDataRequirement(SWindowLogicNode* pWindow, EDataOrderLevel requirement) {
  if (requirement <= pWindow->node.resultDataOrder) {
    return TSDB_CODE_SUCCESS;
  }
  pWindow->node.resultDataOrder = requirement;
  pWindow->node.requireDataOrder = requirement;
  return TSDB_CODE_SUCCESS;
}

static int32_t adjustAnomalyDataRequirement(SWindowLogicNode* pWindow, EDataOrderLevel requirement) {
  if (requirement <= pWindow->node.resultDataOrder) {
    return TSDB_CODE_SUCCESS;
  }
  pWindow->node.resultDataOrder = requirement;
  pWindow->node.requireDataOrder = requirement;
  return TSDB_CODE_SUCCESS;
}

static int32_t adjustWindowDataRequirement(SWindowLogicNode* pWindow, EDataOrderLevel requirement) {
  switch (pWindow->winType) {
    case WINDOW_TYPE_INTERVAL:
      return adjustIntervalDataRequirement(pWindow, requirement);
    case WINDOW_TYPE_SESSION:
      return adjustSessionDataRequirement(pWindow, requirement);
    case WINDOW_TYPE_STATE:
      return adjustStateDataRequirement(pWindow, requirement);
    case WINDOW_TYPE_EVENT:
      return adjustEventDataRequirement(pWindow, requirement);
    case WINDOW_TYPE_COUNT:
      return adjustCountDataRequirement(pWindow, requirement);
    case WINDOW_TYPE_ANOMALY:
      return adjustAnomalyDataRequirement(pWindow, requirement);
    case WINDOW_TYPE_EXTERNAL:
      return adjustExternalDataRequirement(pWindow, requirement);
    default:
      break;
  }
  return TSDB_CODE_PLAN_INTERNAL_ERROR;
}

static int32_t adjustFillDataRequirement(SFillLogicNode* pFill, EDataOrderLevel requirement) {
  if (requirement <= pFill->node.requireDataOrder) {
    return TSDB_CODE_SUCCESS;
  }
  pFill->node.resultDataOrder = requirement;
  pFill->node.requireDataOrder = requirement;
  return TSDB_CODE_SUCCESS;
}

static int32_t adjustSortDataRequirement(SSortLogicNode* pSort, EDataOrderLevel requirement) {
  return TSDB_CODE_SUCCESS;
}

static int32_t adjustPartitionDataRequirement(SPartitionLogicNode* pPart, EDataOrderLevel requirement) {
  if (DATA_ORDER_LEVEL_GLOBAL == requirement) {
    planError(
        "The output of partition cannot meet the requirements(%s) of the upper operator. "
        "Illegal statement, should be intercepted in parser",
        dataOrderStr(requirement));
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }
  pPart->node.resultDataOrder = requirement;
  pPart->node.requireDataOrder = requirement;
  return TSDB_CODE_SUCCESS;
}

static int32_t adjustIndefRowsDataRequirement(SIndefRowsFuncLogicNode* pIndef, EDataOrderLevel requirement) {
  if (requirement <= pIndef->node.resultDataOrder) {
    return TSDB_CODE_SUCCESS;
  }
  pIndef->node.resultDataOrder = requirement;
  pIndef->node.requireDataOrder = requirement;
  return TSDB_CODE_SUCCESS;
}

static int32_t adjustInterpDataRequirement(SInterpFuncLogicNode* pInterp, EDataOrderLevel requirement) {
  if (requirement <= pInterp->node.requireDataOrder) {
    return TSDB_CODE_SUCCESS;
  }
  pInterp->node.resultDataOrder = requirement;
  pInterp->node.requireDataOrder = requirement;
  return TSDB_CODE_SUCCESS;
}

static int32_t adjustForecastDataRequirement(SForecastFuncLogicNode* pForecast, EDataOrderLevel requirement) {
  if (requirement <= pForecast->node.requireDataOrder) {
    return TSDB_CODE_SUCCESS;
  }
  pForecast->node.resultDataOrder = requirement;
  pForecast->node.requireDataOrder = requirement;
  return TSDB_CODE_SUCCESS;
}

int32_t adjustLogicNodeDataRequirement(SLogicNode* pNode, EDataOrderLevel requirement) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (nodeType(pNode)) {
    case QUERY_NODE_LOGIC_PLAN_SCAN:
      code = adjustScanDataRequirement((SScanLogicNode*)pNode, requirement);
      break;
    case QUERY_NODE_LOGIC_PLAN_JOIN:
      code = adjustJoinDataRequirement((SJoinLogicNode*)pNode, requirement);
      return code;
    case QUERY_NODE_LOGIC_PLAN_AGG:
      code = adjustAggDataRequirement((SAggLogicNode*)pNode, requirement);
      break;
    case QUERY_NODE_LOGIC_PLAN_PROJECT:
      code = adjustProjectDataRequirement((SProjectLogicNode*)pNode, requirement);
      break;
    case QUERY_NODE_LOGIC_PLAN_VNODE_MODIFY:
    case QUERY_NODE_LOGIC_PLAN_EXCHANGE:
    case QUERY_NODE_LOGIC_PLAN_MERGE:
      break;
    case QUERY_NODE_LOGIC_PLAN_WINDOW:
      code = adjustWindowDataRequirement((SWindowLogicNode*)pNode, requirement);
      break;
    case QUERY_NODE_LOGIC_PLAN_FILL:
      code = adjustFillDataRequirement((SFillLogicNode*)pNode, requirement);
      break;
    case QUERY_NODE_LOGIC_PLAN_SORT:
      code = adjustSortDataRequirement((SSortLogicNode*)pNode, requirement);
      break;
    case QUERY_NODE_LOGIC_PLAN_WINDOW_FUNC:
      pNode->resultDataOrder = requirement;
      pNode->requireDataOrder = requirement;
      break;
    case QUERY_NODE_LOGIC_PLAN_PARTITION:
      code = adjustPartitionDataRequirement((SPartitionLogicNode*)pNode, requirement);
      break;
    case QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC:
      code = adjustIndefRowsDataRequirement((SIndefRowsFuncLogicNode*)pNode, requirement);
      break;
    case QUERY_NODE_LOGIC_PLAN_INTERP_FUNC:
      code = adjustInterpDataRequirement((SInterpFuncLogicNode*)pNode, requirement);
      break;
    case QUERY_NODE_LOGIC_PLAN_FORECAST_FUNC:
    case QUERY_NODE_LOGIC_PLAN_ANALYSIS_FUNC:
      code = adjustForecastDataRequirement((SForecastFuncLogicNode*)pNode, requirement);
      break;
    default:
      break;
  }
  if (TSDB_CODE_SUCCESS == code) {
    SNode* pChild = NULL;
    FOREACH(pChild, pNode->pChildren) {
      code = adjustLogicNodeDataRequirement((SLogicNode*)pChild, pNode->requireDataOrder);
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    }
  }
  return code;
}

static bool stbNotSystemScan(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    return SCAN_TYPE_SYSTEM_TABLE != ((SScanLogicNode*)pNode)->scanType;
  } else if (QUERY_NODE_LOGIC_PLAN_PARTITION == nodeType(pNode)) {
    return stbNotSystemScan((SLogicNode*)nodesListGetNode(pNode->pChildren, 0));
  } else {
    return true;
  }
}

bool keysHasTbname(SNodeList* pKeys) {
  if (NULL == pKeys) {
    return false;
  }
  SNode* pPartKey = NULL;
  FOREACH(pPartKey, pKeys) {
    if (QUERY_NODE_GROUPING_SET == nodeType(pPartKey)) {
      pPartKey = nodesListGetNode(((SGroupingSetNode*)pPartKey)->pParameterList, 0);
    }
    if ((QUERY_NODE_FUNCTION == nodeType(pPartKey) && FUNCTION_TYPE_TBNAME == ((SFunctionNode*)pPartKey)->funcType) ||
        (QUERY_NODE_COLUMN == nodeType(pPartKey) && COLUMN_TYPE_TBNAME == ((SColumnNode*)pPartKey)->colType)) {
      return true;
    }
  }
  return false;
}

static SNodeList* stbGetPartKeys(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    return ((SScanLogicNode*)pNode)->pGroupTags;
  } else if (QUERY_NODE_LOGIC_PLAN_PARTITION == nodeType(pNode)) {
    return ((SPartitionLogicNode*)pNode)->pPartitionKeys;
  } else {
    return NULL;
  }
}

bool isPartTableAgg(SAggLogicNode* pAgg) {
  if (1 != LIST_LENGTH(pAgg->node.pChildren)) {
    return false;
  }
  if (NULL != pAgg->pGroupKeys) {
    return (pAgg->isGroupTb || keysHasTbname(pAgg->pGroupKeys)) &&
           stbNotSystemScan((SLogicNode*)nodesListGetNode(pAgg->node.pChildren, 0));
  }
  return pAgg->isPartTb || keysHasTbname(stbGetPartKeys((SLogicNode*)nodesListGetNode(pAgg->node.pChildren, 0)));
}

static bool stbHasPartTag(SNodeList* pPartKeys) {
  if (NULL == pPartKeys) {
    return false;
  }
  SNode* pPartKey = NULL;
  FOREACH(pPartKey, pPartKeys) {
    if (QUERY_NODE_GROUPING_SET == nodeType(pPartKey)) {
      pPartKey = nodesListGetNode(((SGroupingSetNode*)pPartKey)->pParameterList, 0);
    }
    if ((QUERY_NODE_FUNCTION == nodeType(pPartKey) && FUNCTION_TYPE_TAGS == ((SFunctionNode*)pPartKey)->funcType) ||
        (QUERY_NODE_COLUMN == nodeType(pPartKey) && COLUMN_TYPE_TAG == ((SColumnNode*)pPartKey)->colType)) {
      return true;
    }
  }
  return false;
}

bool getBatchScanOptionFromHint(SNodeList* pList) {
  SNode* pNode = NULL;
  bool batchScan = true;
  FOREACH(pNode, pList) {
    SHintNode* pHint = (SHintNode*)pNode;
    if (pHint->option == HINT_BATCH_SCAN) {
      batchScan = true;
      break;
    } else if (pHint->option == HINT_NO_BATCH_SCAN) {
      batchScan = false;
      break;
    }
  }

  return batchScan;
}

bool getSortForGroupOptHint(SNodeList* pList) {
  if (!pList) return false;
  SNode* pNode;
  FOREACH(pNode, pList) {
    SHintNode* pHint = (SHintNode*)pNode;
    if (pHint->option == HINT_SORT_FOR_GROUP) {
      return true;
    }
  }
  return false;
}

bool getOptHint(SNodeList* pList, EHintOption hint) {
  if (!pList) return false;
  SNode* pNode;
  FOREACH(pNode, pList) {
    SHintNode* pHint = (SHintNode*)pNode;
    if (pHint->option == hint) {
      return true;
    }
  }
  return false;
}

bool getParaTablesSortOptHint(SNodeList* pList) {
  if (!pList) return false;
  SNode* pNode;
  FOREACH(pNode, pList) {
    SHintNode* pHint = (SHintNode*)pNode;
    if (pHint->option == HINT_PARA_TABLES_SORT) {
      return true;
    }
  }
  return false;
}

bool getSmallDataTsSortOptHint(SNodeList* pList) {
  if (!pList) return false;
  SNode* pNode;
  FOREACH(pNode, pList) {
    SHintNode* pHint = (SHintNode*)pNode;
    if (pHint->option == HINT_SMALLDATA_TS_SORT) {
      return true;
    }
  }
  return false;
}

bool getSmallDataScanSortOptHint(SNodeList* pList) { return getOptHint(pList, HINT_SMALLDATA_SCAN_SORT); }

bool getHashJoinOptHint(SNodeList* pList) {
  if (!pList) return false;
  SNode* pNode;
  FOREACH(pNode, pList) {
    SHintNode* pHint = (SHintNode*)pNode;
    if (pHint->option == HINT_HASH_JOIN) {
      return true;
    }
  }
  return false;
}


int32_t collectTableAliasFromNodes(SNode* pNode, SSHashObj** ppRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  SLogicNode* pCurr = (SLogicNode*)pNode;
  FOREACH(pNode, pCurr->pTargets) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    if (NULL == *ppRes) {
      *ppRes = tSimpleHashInit(5, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
      if (NULL == *ppRes) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
    }

    if(pCol->tableAlias[0] == '\0') {
      continue;
    }

    code = tSimpleHashPut(*ppRes, pCol->tableAlias, strlen(pCol->tableAlias), NULL, 0);
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }
  
  if (TSDB_CODE_SUCCESS == code) {
    FOREACH(pNode, pCurr->pChildren) {
      code = collectTableAliasFromNodes(pNode, ppRes);
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    }
  }
  if (TSDB_CODE_SUCCESS != code) {
    tSimpleHashCleanup(*ppRes);
    *ppRes = NULL;
  }

  return code;
}

bool isPartTagAgg(SAggLogicNode* pAgg) {
  if (1 != LIST_LENGTH(pAgg->node.pChildren)) {
    return false;
  }
  if (pAgg->pGroupKeys) {
    return stbHasPartTag(pAgg->pGroupKeys) &&
      stbNotSystemScan((SLogicNode*)nodesListGetNode(pAgg->node.pChildren, 0));
  }
  return stbHasPartTag(stbGetPartKeys((SLogicNode*)nodesListGetNode(pAgg->node.pChildren, 0)));
}

bool isPartTableWinodw(SWindowLogicNode* pWindow) {
  if ((pWindow->partType & WINDOW_PART_TB) || keysHasTbname(stbGetPartKeys((SLogicNode*)nodesListGetNode(pWindow->node.pChildren, 0)))) {
    return true;
  }
  return false;
}

bool limitHasFiniteRows(const SNode* pLimit) { return NULL != pLimit && NULL != ((SLimitNode*)pLimit)->limit; }

static void adjustLimitWithOffset(SLimitNode* pLimit) {
  if (NULL == pLimit || NULL == pLimit->limit) {
    return;
  }
  if (pLimit->offset) {
    pLimit->limit->datum.i += pLimit->offset->datum.i;
    pLimit->offset->datum.i = 0;
  }
}

bool isPartTableInterp(SInterpFuncLogicNode* pInterp) {
  if (1 != LIST_LENGTH(pInterp->node.pChildren)) {
    return false;
  }
  SLogicNode* pChild = (SLogicNode*)nodesListGetNode(pInterp->node.pChildren, 0);
  if (QUERY_NODE_LOGIC_PLAN_SCAN != nodeType(pChild)) {
    return false;
  }
  SScanLogicNode* pScan = (SScanLogicNode*)pChild;
  // Each partition-by-tbname group lives entirely on a single vnode, and the
  // scan already emits group-ordered rows (groupSort, set by the interp RANGE
  // by-tbname rewrite), so interp can be computed per vnode and pushed below the
  // exchange instead of going through the cross-table Merge. partition by tag may
  // span vnodes, so it is excluded and keeps the merge path.
  return keysHasTbname(pScan->pGroupTags) && pScan->groupSort;
}

int32_t cloneLimit(SLogicNode* pParent, SLogicNode* pChild, uint8_t cloneWhat, bool* pCloned) {
  SLimitNode *pLimit = NULL, *pSlimit = NULL;
  int32_t     code = 0;
  bool        cloned = false;
  if (limitHasFiniteRows(pParent->pLimit) && (cloneWhat & CLONE_LIMIT)) {
    code = nodesCloneNode(pParent->pLimit, (SNode**)&pLimit);
    if (TSDB_CODE_SUCCESS == code) {
      adjustLimitWithOffset(pLimit);
      cloned = true;
    }
  }

  if (limitHasFiniteRows(pParent->pSlimit) && (cloneWhat & CLONE_SLIMIT)) {
    code = nodesCloneNode(pParent->pSlimit, (SNode**)&pSlimit);
    if (TSDB_CODE_SUCCESS == code) {
      adjustLimitWithOffset(pSlimit);
      cloned = true;
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    pChild->pLimit = (SNode*)pLimit;
    pChild->pSlimit = (SNode*)pSlimit;
    *pCloned = cloned;
  } else {
    nodesDestroyNode((SNode*)pLimit);
    nodesDestroyNode((SNode*)pSlimit);
  }
  return code;
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

bool keysHasCol(SNodeList* pKeys) {
  bool hasCol = false;
  nodesWalkExprs(pKeys, partTagsOptHasColImpl, &hasCol);
  return hasCol;
}

SFunctionNode* createGroupKeyAggFunc(SColumnNode* pGroupCol) {
  SFunctionNode* pFunc = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (pFunc) {
    tstrncpy(pFunc->functionName, "_group_key", TSDB_FUNC_NAME_LEN);
    tstrncpy(pFunc->node.aliasName, pGroupCol->node.aliasName, TSDB_COL_NAME_LEN);
    tstrncpy(pFunc->node.userAlias, pGroupCol->node.userAlias, TSDB_COL_NAME_LEN);
    SNode* pNew = NULL;
    code = nodesCloneNode((SNode*)pGroupCol, &pNew);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListMakeStrictAppend(&pFunc->pParameterList, pNew);
    }
    if (code == TSDB_CODE_SUCCESS) {
      code = fmGetFuncInfo(pFunc, NULL, 0);
    }
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode((SNode*)pFunc);
      pFunc = NULL;
    }
    if (TSDB_CODE_SUCCESS == code) {
      char    name[TSDB_FUNC_NAME_LEN + TSDB_NAME_DELIMITER_LEN + TSDB_POINTER_PRINT_BYTES + 1] = {0};
      int32_t len = snprintf(name, sizeof(name) - 1, "%s.%p", pFunc->functionName, pFunc);
      (void)taosHashBinary(name, len, sizeof(name));
      tstrncpy(pFunc->node.aliasName, name, TSDB_COL_NAME_LEN);
    }
  }
  if (TSDB_CODE_SUCCESS != code) {
    terrno = code;
    nodesDestroyNode((SNode*)pFunc);
    pFunc = NULL;
  }
  return pFunc;
}

int32_t getTimeRangeFromNode(SNode** pPrimaryKeyCond, STimeWindow* pTimeRange, bool* pIsStrict) {
  SNode*  pNew = NULL;
  int32_t code = scalarCalculateConstants(*pPrimaryKeyCond, &pNew);
  if (TSDB_CODE_SUCCESS == code) {
    *pPrimaryKeyCond = pNew;
    code = filterGetTimeRange(*pPrimaryKeyCond, pTimeRange, pIsStrict, NULL);
  }
  return code;
}


static EDealRes tagScanNodeHasTbnameFunc(SNode* pNode, void* pContext) {
  if (QUERY_NODE_FUNCTION == nodeType(pNode) && FUNCTION_TYPE_TBNAME == ((SFunctionNode*)pNode)->funcType ||
        (QUERY_NODE_COLUMN == nodeType(pNode) && COLUMN_TYPE_TBNAME == ((SColumnNode*)pNode)->colType)) {
    *(bool*)pContext = true;
    return DEAL_RES_END;
  }
  return DEAL_RES_CONTINUE;
}

static bool tagScanNodeListHasTbname(SNodeList* pCols) {
  bool hasTbname = false;
  nodesWalkExprs(pCols, tagScanNodeHasTbnameFunc, &hasTbname);
  return hasTbname;
}

static bool tagScanNodeHasTbname(SNode* pKeys) {
  bool hasTbname = false;
  nodesWalkExpr(pKeys, tagScanNodeHasTbnameFunc, &hasTbname);
  return hasTbname;
}



int32_t tagScanSetExecutionMode(SScanLogicNode* pScan) {
  pScan->onlyMetaCtbIdx = false;

  if (pScan->tableType != TSDB_SUPER_TABLE) {
    pScan->onlyMetaCtbIdx = false;
    return TSDB_CODE_SUCCESS;
  }

  if (tagScanNodeListHasTbname(pScan->pScanPseudoCols)) {
    pScan->onlyMetaCtbIdx = false;
    return TSDB_CODE_SUCCESS;
  }

  if (pScan->node.pConditions == NULL) {
    pScan->onlyMetaCtbIdx = true;
    return TSDB_CODE_SUCCESS;
  }

  SNode* pCond = NULL;
  int32_t code = nodesCloneNode(pScan->node.pConditions, &pCond);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  SNode* pTagCond = NULL;
  SNode* pTagIndexCond = NULL;
  code = filterPartitionCond(&pCond, NULL, &pTagIndexCond, &pTagCond, NULL);
  if (TSDB_CODE_SUCCESS == code) {
    if (pTagIndexCond || tagScanNodeHasTbname(pTagCond)) {
      pScan->onlyMetaCtbIdx = false;
    } else {
      pScan->onlyMetaCtbIdx = true;
    }
  }
  nodesDestroyNode(pCond);
  nodesDestroyNode(pTagIndexCond);
  nodesDestroyNode(pTagCond);
  return TSDB_CODE_SUCCESS;
}

bool isColRefExpr(const SColumnNode* pCol, const SExprNode* pExpr) {
  if (pCol->projRefIdx > 0) return pCol->projRefIdx == pExpr->projIdx;

  return 0 == strcmp(pCol->colName, pExpr->aliasName);
}

void rewriteTargetsWithResId(SNodeList* pTargets) {
  SNode* pNode;
  FOREACH(pNode, pTargets) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    pCol->resIdx = pCol->projRefIdx;
  }
}

bool checkScanLogicNode(SLogicNode* pNode) {
  if (NULL == pNode) {
    return false;
  }

  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    return true;
  }

  SNode* node = NULL;
  FOREACH(node, pNode->pChildren) {
    if (checkScanLogicNode((SLogicNode*)node)) {
      return true;
    }
  }

  return false;
}

bool inStreamCalcClause(SPlanContext* pCxt) {
  return pCxt->streamCxt.isCalc;
}

bool inStreamTriggerClause(SPlanContext* pCxt) {
  return pCxt->streamCxt.isTrigger;
}
