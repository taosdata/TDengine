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

typedef struct NodeValidationCtx {
  int32_t             code;
  SPhysiNode*         pNode;
  SDataBlockDescNode* pOutputDesc;
  SDataBlockDescNode* pDownstreamOutputDesc;
} NodeValidationCtx;

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

static int32_t doValidateSlotId(int32_t slotId, SDataBlockDescNode* pOutputDesc) {
  if (!pOutputDesc || !pOutputDesc->pSlots) {
    qLogWarn();
    return TSDB_CODE_PLAN_VALIDATION_ERR;
  }
  SNode*  pNode = NULL;
  bool    found = false;
  // TODO if we validate output first, here we do not need to loop all slots
  FOREACH(pNode, pOutputDesc->pSlots) {
    if (nodeType(pNode) != QUERY_NODE_SLOT_DESC) {
      qLogWarn();
      return TSDB_CODE_PLAN_VALIDATION_ERR;
    }
    SSlotDescNode* pSlotDesc = (SSlotDescNode*)pNode;
    if (pSlotDesc->slotId == slotId) {
      found = true;
      break;
    }
  }
  return found ? TSDB_CODE_SUCCESS : TSDB_CODE_PLAN_VALIDATION_ERR;
}

static EDealRes validateColumns(SNode* pNode, void* pCtxVoid) {
  NodeValidationCtx* pCtx = pCtxVoid;
  if (!pNode || !pCtx || !pCtx->pOutputDesc) {
    qLogWarn();
    if (pCtx) pCtx->code = TSDB_CODE_PLAN_VALIDATION_ERR;
    return DEAL_RES_ERROR;
  }
  if (nodeType(pNode) == QUERY_NODE_COLUMN) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    pCtx->code = doValidateSlotId(pCol->slotId, pCtx->pOutputDesc);
    if (TSDB_CODE_SUCCESS != pCtx->code) {
      return DEAL_RES_ERROR;
    }
  }
  return DEAL_RES_CONTINUE;
}

static int32_t tryGetDownstreamOutput(SPhysiNode* pNode, NodeValidationCtx* pCtx) {
  pCtx->code = TSDB_CODE_SUCCESS;
  if (!pCtx->pDownstreamOutputDesc && shouldHaveChild(pNode)) {
    pCtx->pDownstreamOutputDesc = getDownstreamOutputDesc(pNode);
    if (!pCtx->pDownstreamOutputDesc) {
      qLogWarn();
      return TSDB_CODE_PLAN_VALIDATION_ERR;
    }
  }
  return pCtx->code;
}

static int32_t tryGetOutputDesc(SPhysiNode* pNode, NodeValidationCtx* pCtx) {
  if (!pNode->pOutputDataBlockDesc) {
    qLogWarn();
    return pCtx->code = TSDB_CODE_PLAN_VALIDATION_ERR;
  }
  pCtx->pOutputDesc = pNode->pOutputDataBlockDesc;
  return TSDB_CODE_SUCCESS;
}

static int32_t validateTargetNodes(SNodeList* pTargets, NodeValidationCtx* pCtx) {
  if (!pTargets) return TSDB_CODE_SUCCESS;
  pCtx->code = tryGetOutputDesc(pCtx->pNode, pCtx);
  if (TSDB_CODE_SUCCESS != pCtx->code) {
    qLogWarn();
    return pCtx->code;
  }
  SNode* pNode = NULL;
  FOREACH(pNode, pTargets) {
    STargetNode* pTarget = (STargetNode*)pNode;
    pCtx->code = doValidateSlotId(pTarget->slotId, pCtx->pOutputDesc);
    if (TSDB_CODE_SUCCESS != pCtx->code) {
      return pCtx->code;
    }
  }
  return pCtx->code;
}

static int32_t walkAndValidateColumnNodes(SNodeList* pNodes, SPhysiNode* pNode, NodeValidationCtx* pCtx) {
  pCtx->code = tryGetOutputDesc(pNode, pCtx);
  if (TSDB_CODE_SUCCESS != pCtx->code) {
    qLogWarn();
    return pCtx->code;
  }
  nodesWalkExprs(pNodes, validateColumns, pCtx);
  if (TSDB_CODE_SUCCESS != pCtx->code) {
    qLogWarn();
    return pCtx->code;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t walkAndValidateColumnNode(SNode* pNode, SPhysiNode* pPhysiNode, NodeValidationCtx* pCtx) {
  pCtx->code = tryGetOutputDesc(pPhysiNode, pCtx);
  if (TSDB_CODE_SUCCESS != pCtx->code) {
    qLogWarn();
    return pCtx->code;
  }
  nodesWalkExpr(pNode, validateColumns, pCtx);
  if (TSDB_CODE_SUCCESS != pCtx->code) {
    qLogWarn();
    return pCtx->code;
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

static int32_t validateBasePhysiNode(SPhysiNode* pNode, NodeValidationCtx* pCtx) {
  if (!pNode->pOutputDataBlockDesc || !pNode->pOutputDataBlockDesc->pSlots) {
    qLogWarn();
    return TSDB_CODE_PLAN_VALIDATION_ERR;
  }
  // walk conditions, check all Columns
  int32_t code = walkAndValidateColumnNode(pNode->pConditions, pNode, pCtx);
  if (TSDB_CODE_SUCCESS != code) {
    qLogWarn();
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t doValidateScanPhysiNode(SScanPhysiNode* pNode, NodeValidationCtx* pCtx) {
  int32_t code = TSDB_CODE_SUCCESS;
  code = validateBasePhysiNode(&pNode->node, pCtx);
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
  code = validateTargetNodes(pNode->pScanCols, pCtx);
  if (TSDB_CODE_SUCCESS != code) {
    qLogWarn();
    return code;
  }
  code = nodeTypeExpect(pNode->pScanPseudoCols, QUERY_NODE_TARGET);
  if (TSDB_CODE_SUCCESS != code) {
    qLogWarn();
    return code;
  }
  code = validateTargetNodes(pNode->pScanPseudoCols, pCtx);
  if (TSDB_CODE_SUCCESS != code) {
    qLogWarn();
    return code;
  }
  return code;
}

int32_t doValidateTableScanPhysiNode(STableScanPhysiNode* pNode) {
  NodeValidationCtx ctx = {.code = TSDB_CODE_SUCCESS, .pNode = &pNode->scan.node};
  ctx.code = doValidateScanPhysiNode(&pNode->scan, &ctx);
  return ctx.code;
}

