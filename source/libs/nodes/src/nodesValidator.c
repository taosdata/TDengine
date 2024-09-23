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

#include "nodesValidator.h"

#define qLogWarn() qWarn("validate failed at %s:%d", __func__, __LINE__)
#define QUERY_NODE_PHYSICAL_NODE_MIN QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN
#define QUERY_NODE_PHYSICAL_NODE_MAX QUERY_NODE_PHYSICAL_PLAN_STREAM_MID_INTERVAL
#define TSDB_CODE_PLAN_VALIDATION_ERR TSDB_CODE_PLAN_INTERNAL_ERROR;

bool shouldHaveChild(SPhysiNode* pNode) {
  bool ret = true;
  switch (nodeType(pNode)) {
    case QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN:
      ret = false;
      break;
    default:
      break;
  }
  return ret;
}

static SDataBlockDescNode* getDownstreamOutputDesc(SPhysiNode* pNode) {
  SNodeList* pChildren = pNode->pChildren;
  if (LIST_LENGTH(pChildren) == 0) {
    return NULL;
  }

  SNode* pFirstNode = pChildren->pHead->pNode;
  if (nodeType(pFirstNode) < QUERY_NODE_PHYSICAL_NODE_MIN || nodeType(pFirstNode) > QUERY_NODE_PHYSICAL_NODE_MAX) {
    qLogWarn();
    terrno = TSDB_CODE_PLAN_VALIDATION_ERR;
    return NULL;
  }
  SPhysiNode* pPhysiNode = (SPhysiNode*)pFirstNode;
  return pPhysiNode->pOutputDataBlockDesc;
}

static int32_t doValidateColRef(SColumnNode* pCol, SDataBlockDescNode* pDownstreamOutput) {
  if (!pCol || !pDownstreamOutput || !pDownstreamOutput->pSlots) {
    qLogWarn();
    return TSDB_CODE_PLAN_VALIDATION_ERR;
  }
  int32_t slotId = pCol->slotId;
  SNode*  pNode = NULL;
  bool    found = false;
  // TODO if we validate output first, here we do not need to loop all slots
  FOREACH(pNode, pDownstreamOutput->pSlots) {
    if (nodeType(pNode) != QUERY_NODE_SLOT_DESC) {
      qLogWarn();
      return TSDB_CODE_PLAN_VALIDATION_ERR;
    }
    SSlotDescNode* pSlotDesc = (SSlotDescNode*)pNode;
    if (pSlotDesc->slotId == pCol->slotId) {
      found = true;
      break;
    }
  }
  return found ? TSDB_CODE_SUCCESS : TSDB_CODE_PLAN_VALIDATION_ERR;
}

typedef struct SValidateColumnCtx {
  int32_t code;
  SDataBlockDescNode* pDownstreamOutput;
}SValidateColumnCtx;

static EDealRes validateColumns(SNode* pNode, void* pCtxVoid) {
  SValidateColumnCtx* pCtx = pCtxVoid;
  if (!pNode || !pCtx || !pCtx->pDownstreamOutput) {
    qLogWarn();
    if (pCtx) pCtx->code = TSDB_CODE_PLAN_VALIDATION_ERR;
    return DEAL_RES_ERROR;
  }
  if (nodeType(pNode) == QUERY_NODE_COLUMN) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    pCtx->code = doValidateColRef(pCol, pCtx->pDownstreamOutput);
    if (TSDB_CODE_SUCCESS != pCtx->code) {
      return DEAL_RES_ERROR;
    }
  }
  return DEAL_RES_CONTINUE;
}

static int32_t walkAndValidateColumnNodes(SNodeList* pNodes, SPhysiNode* pNode) {
  if (shouldHaveChild(pNode)) {
    SDataBlockDescNode* pOutputDesc = getDownstreamOutputDesc(pNode);
    if (!pOutputDesc) {
      qLogWarn();
      return TSDB_CODE_PLAN_VALIDATION_ERR;
    }
    SValidateColumnCtx ctx = {.code = TSDB_CODE_SUCCESS, .pDownstreamOutput = pOutputDesc};
    nodesWalkExprs(pNodes, validateColumns, &ctx);
    if (TSDB_CODE_SUCCESS != ctx.code) {
      qLogWarn();
      return ctx.code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t walkAndValidateColumnNode(SNode* pNode, SPhysiNode* pPhysiNode) {
  if (shouldHaveChild(pPhysiNode)) {
    SDataBlockDescNode* pOutputDesc = getDownstreamOutputDesc(pPhysiNode);
    if (!pOutputDesc) {
      qLogWarn();
      return TSDB_CODE_PLAN_VALIDATION_ERR;
    }
    SValidateColumnCtx ctx = {.code = TSDB_CODE_SUCCESS, .pDownstreamOutput = pOutputDesc};
    nodesWalkExpr(pNode, validateColumns, &ctx);
    if (TSDB_CODE_SUCCESS != ctx.code) {
      qLogWarn();
      return ctx.code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t nodeTypeExpect(SNodeList* pNodes, ENodeType type) {
  if (!pNodes) return TSDB_CODE_SUCCESS;
  SNode * pNode = NULL;
  FOREACH(pNode, pNodes) {
    if (nodeType(pNode) != type) {
      qLogWarn();
      return TSDB_CODE_PLAN_VALIDATION_ERR;
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t doValidateBasePhysiNode(SPhysiNode* pNode) {
  if (!pNode->pOutputDataBlockDesc || !pNode->pOutputDataBlockDesc->pSlots) return TSDB_CODE_PLAN_VALIDATION_ERR;
  // walk conditions, check all Columns
  int32_t code = walkAndValidateColumnNode(pNode->pConditions, pNode);
  if (TSDB_CODE_SUCCESS != code) {
    qLogWarn();
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t doValidateScanPhysiNode(SScanPhysiNode* pNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  code = doValidateBasePhysiNode(&pNode->node);
  if (TSDB_CODE_SUCCESS != code) {
    qLogWarn();
    return code;
  }
  SNode* pTmp = NULL;
  code = nodeTypeExpect(pNode->pScanCols, QUERY_NODE_TARGET);
  if (TSDB_CODE_SUCCESS != code) {
    qLogWarn();
    return code;
  }
  return code;
}

int32_t doValidateTableScanPhysiNode(STableScanPhysiNode* pNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  code = doValidateScanPhysiNode(&pNode->scan);
  return code;
}

