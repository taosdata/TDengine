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

#include "catalog.h"
#include "functionMgt.h"
#include "systable.h"
#include "tglobal.h"

typedef struct SValidatePlanContext {
  SPlanContext* pPlanCxt;
  int32_t       errCode;
} SValidatePlanContext;

int32_t doValidatePhysiNode(SValidatePlanContext* pCxt, SNode* pNode);

int32_t validateMergePhysiNode(SValidatePlanContext* pCxt, SMergePhysiNode* pMerge) {
  if ((NULL != pMerge->node.pLimit || NULL != pMerge->node.pSlimit) && pMerge->type == MERGE_TYPE_NON_SORT) {
    planError("no limit&slimit supported for non sort merge, pLimit:%p", pMerge->node.pLimit);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t validateSubplanNode(SValidatePlanContext* pCxt, SSubplan* pSubPlan) {
  if (SUBPLAN_TYPE_MODIFY == pSubPlan->subplanType) {
    return TSDB_CODE_SUCCESS;
  }
  return doValidatePhysiNode(pCxt, (SNode*)pSubPlan->pNode);
}

int32_t validateQueryPlanNode(SValidatePlanContext* pCxt, SQueryPlan* pPlan) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode* pNode = NULL;
  FOREACH(pNode, pPlan->pSubplans) {
    if (QUERY_NODE_NODE_LIST != nodeType(pNode)) {
      code = TSDB_CODE_PLAN_INTERNAL_ERROR;
      break;
    }

    SNode* pSubNode = NULL;
    SNodeListNode* pSubplans = (SNodeListNode*)pNode;
    FOREACH(pSubNode, pSubplans->pNodeList) {
      if (QUERY_NODE_PHYSICAL_SUBPLAN != nodeType(pNode)) {
        code = TSDB_CODE_PLAN_INTERNAL_ERROR;
        break;
      }
      
      code = doValidatePhysiNode(pCxt, pSubNode);
      if (code) {
        break;
      }
    }
  }

  return code;
}

int32_t doValidatePhysiNode(SValidatePlanContext* pCxt, SNode* pNode) {
  switch (nodeType(pNode)) {
    case QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_PROJECT:
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN:
    case QUERY_NODE_PHYSICAL_PLAN_HASH_AGG:
    case QUERY_NODE_PHYSICAL_PLAN_EXCHANGE:
      break;
    case QUERY_NODE_PHYSICAL_PLAN_MERGE:
      return validateMergePhysiNode(pCxt, (SMergePhysiNode*)pNode);
    case QUERY_NODE_PHYSICAL_PLAN_SORT:
    case QUERY_NODE_PHYSICAL_PLAN_GROUP_SORT:
    case QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_MID_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_FILL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_FILL:
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_SESSION:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_SESSION:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION:
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_STATE:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_STATE:
    case QUERY_NODE_PHYSICAL_PLAN_PARTITION:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_PARTITION:
    case QUERY_NODE_PHYSICAL_PLAN_INDEF_ROWS_FUNC:
    case QUERY_NODE_PHYSICAL_PLAN_INTERP_FUNC:
    case QUERY_NODE_PHYSICAL_PLAN_DISPATCH:
    case QUERY_NODE_PHYSICAL_PLAN_INSERT:
    case QUERY_NODE_PHYSICAL_PLAN_QUERY_INSERT:
    case QUERY_NODE_PHYSICAL_PLAN_DELETE:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_EVENT:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_EVENT:
    case QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN:
    case QUERY_NODE_PHYSICAL_PLAN_GROUP_CACHE:
    case QUERY_NODE_PHYSICAL_PLAN_DYN_QUERY_CTRL:
      break;
    case QUERY_NODE_PHYSICAL_SUBPLAN:
      return validateSubplanNode(pCxt, (SSubplan*)pNode);
    case QUERY_NODE_PHYSICAL_PLAN:
      return validateQueryPlanNode(pCxt, (SQueryPlan *)pNode);
    default:
      break;
  }

  return TSDB_CODE_SUCCESS;
}

static void destoryValidatePlanContext(SValidatePlanContext* pCxt) {

}

int32_t validateQueryPlan(SPlanContext* pCxt, SQueryPlan* pPlan) {
  SValidatePlanContext cxt = {.pPlanCxt = pCxt,
                              .errCode = TSDB_CODE_SUCCESS
                             };

  int32_t code = TSDB_CODE_SUCCESS;
  SNode* pNode = NULL;
  FOREACH(pNode, pPlan->pSubplans) {
    if (QUERY_NODE_NODE_LIST != nodeType(pNode)) {
      code = TSDB_CODE_PLAN_INTERNAL_ERROR;
      break;
    }

    SNode* pSubNode = NULL;
    SNodeListNode* pSubplans = (SNodeListNode*)pNode;
    FOREACH(pSubNode, pSubplans->pNodeList) {
      code = doValidatePhysiNode(&cxt, pSubNode);
      if (code) {
        break;
      }
    }
    if (code) {
      break;
    }
  }

  destoryValidatePlanContext(&cxt);
  return code;
}
