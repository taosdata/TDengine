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
#include "functionMgt.h"

#define qLogWarn() qWarn("plan validation failed at %s:%d", __func__, __LINE__)
#define QUERY_NODE_PHYSICAL_NODE_MIN QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN
#define QUERY_NODE_PHYSICAL_NODE_MAX QUERY_NODE_PHYSICAL_PLAN_STREAM_MID_INTERVAL
#define TSDB_CODE_PLAN_VALIDATION_ERR TSDB_CODE_PLAN_INTERNAL_ERROR;

typedef enum ValidationType {
  CONST_VALIDATION = 1,
  BASE_VALIDATION = 1 << 1,
  STRICT_VALIDATION = 1 << 2,
} ValidationType;

typedef struct ColumnValidation {
  bool ignoreSlotId;
  bool validateFromDownstream;
  bool ignoreOthers;
} ColumnValidation;

typedef struct NodeValidationCtx {
  int32_t             code;
  SPhysiNode*         pNode;
  SDataBlockDescNode* pOutputDesc;
  SDataBlockDescNode* pDownstreamOutputDesc;
  ColumnValidation    validateCol;
  ValidationType      validateType;
} NodeValidationCtx;

void setColumnValidation(NodeValidationCtx* pCtx, bool ignoreSlotId, bool validateFromDownstream) {
  pCtx->validateCol.ignoreSlotId = ignoreSlotId;
  pCtx->validateCol.validateFromDownstream = validateFromDownstream;
}

void setValidateCol_IgnoreSlotId(NodeValidationCtx* pCtx, bool ignoreSlotId) {
  pCtx->validateCol.ignoreSlotId = ignoreSlotId;
}

void setValidateCol_ValidateFromDownstream(NodeValidationCtx* pCtx, bool validateFromDownstream) {
  if (LIST_LENGTH(pCtx->pNode->pChildren) > 0) {
    pCtx->validateCol.validateFromDownstream = validateFromDownstream;
  }
}

void setValidateCol_IgnoreOthers(NodeValidationCtx* pCtx, bool ignoreOthers) {
  pCtx->validateCol.ignoreOthers = ignoreOthers;
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

static int32_t validateTableUid(int8_t tableType, uint64_t uid, NodeValidationCtx* pCtx) {
  switch (tableType) {
    case TSDB_SUPER_TABLE:
      if (uid == 0) {
        qLogWarn();
        return TSDB_CODE_PLAN_VALIDATION_ERR;
      }
      break;
    case TSDB_CHILD_TABLE:
    case TSDB_NORMAL_TABLE:
      if (uid == 0) {
        qLogWarn();
        return TSDB_CODE_PLAN_VALIDATION_ERR;
      }
      break;
    case TSDB_TEMP_TABLE:
    case TSDB_SYSTEM_TABLE:
    case TSDB_TSMA_TABLE:
    case TSDB_VIEW_TABLE:
      // TODO check what for these tables?
      break;
    default:
      qLogWarn();
      return TSDB_CODE_PLAN_VALIDATION_ERR;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t validateScanTableUid(SScanPhysiNode* pNode, NodeValidationCtx* pCtx) {
  if (pNode->tableType == TSDB_SUPER_TABLE) {
    return validateTableUid(pNode->tableType, pNode->suid, pCtx);
  } else {
    return validateTableUid(pNode->tableType, pNode->uid, pCtx);
  }
}

static int32_t validateSlotId(int32_t slotId, SDataBlockDescNode* pOutputDesc, const SSlotDescNode** pSlotDescNode) {
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
      if (pSlotDescNode) *pSlotDescNode = pSlotDesc;
      break;
    }
  }
  return found ? TSDB_CODE_SUCCESS : TSDB_CODE_PLAN_VALIDATION_ERR;
}

static int32_t validateColDataBlockId(SColumnNode* pCol, NodeValidationCtx* pCtx) {
  int16_t dataBlockId;
  if (pCtx->validateCol.validateFromDownstream) {
    if (!pCtx->pDownstreamOutputDesc) {
      qLogWarn();
      return pCtx->code = TSDB_CODE_PLAN_VALIDATION_ERR;
    }
    dataBlockId = pCtx->pDownstreamOutputDesc->dataBlockId;
  } else {
    dataBlockId = pCtx->pOutputDesc->dataBlockId;
  }
  if (pCol->dataBlockId != dataBlockId) {
    qLogWarn();
    return pCtx->code = TSDB_CODE_PLAN_VALIDATION_ERR;
  }
  return pCtx->code;
}

static int32_t validateColumn(SColumnNode* pCol, NodeValidationCtx* pCtx) {
  if (!pCtx->validateCol.ignoreSlotId) {
    const SSlotDescNode* pSlotDescNode = NULL;
    pCtx->code = validateSlotId(
        pCol->slotId, pCtx->validateCol.validateFromDownstream ? pCtx->pDownstreamOutputDesc : pCtx->pOutputDesc,
        &pSlotDescNode);
    if (TSDB_CODE_SUCCESS != pCtx->code) {
      qLogWarn();
      return pCtx->code;
    }
    if (memcmp(&pSlotDescNode->dataType, &pCol->node.resType, sizeof(SDataType)) != 0) {
      qLogWarn();
      return pCtx->code = TSDB_CODE_PLAN_VALIDATION_ERR;
    }
    pCtx->code = validateColDataBlockId(pCol, pCtx);
    if (TSDB_CODE_SUCCESS != pCtx->code) {
      qLogWarn();
      return pCtx->code;
    }
  }
  if (!pCtx->validateCol.ignoreOthers) {
    pCtx->code = validateTableUid(pCol->tableType, pCol->tableId, pCtx);
    if (TSDB_CODE_SUCCESS != pCtx->code) {
      qLogWarn();
      return pCtx->code = TSDB_CODE_PLAN_VALIDATION_ERR;
    }
    if (pCol->colId == 0 && pCol->node.resType.type != TSDB_DATA_TYPE_TIMESTAMP) {
      qLogWarn();
      return pCtx->code = TSDB_CODE_PLAN_VALIDATION_ERR;
    }
    if (pCol->colId < 0) {
      qLogWarn();
      return pCtx->code = TSDB_CODE_PLAN_VALIDATION_ERR;
    }
    if (pCol->colType > COLUMN_TYPE_MAX) {
      qLogWarn();
      return pCtx->code = TSDB_CODE_PLAN_VALIDATION_ERR;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t validateFunction(SFunctionNode* pFunc, NodeValidationCtx* pCtx) {
  return pCtx->code;
}

static EDealRes validateColumnInWalk(SNode* pNode, void* pCtxVoid) {
  NodeValidationCtx* pCtx = pCtxVoid;
  if (!pNode || !pCtx || !pCtx->pOutputDesc) {
    qLogWarn();
    if (pCtx) pCtx->code = TSDB_CODE_PLAN_VALIDATION_ERR;
    return DEAL_RES_ERROR;
  }
  if (nodeType(pNode) == QUERY_NODE_COLUMN) {
    pCtx->code = validateColumn((SColumnNode*)pNode, pCtx);
    if (TSDB_CODE_SUCCESS != pCtx->code) {
      return DEAL_RES_ERROR;
    }
  }
  return DEAL_RES_CONTINUE;
}

static int32_t tryGetDownstreamOutput(SPhysiNode* pNode, NodeValidationCtx* pCtx) {
  pCtx->code = TSDB_CODE_SUCCESS;
  if (!pCtx->pDownstreamOutputDesc && LIST_LENGTH(pNode->pChildren) > 0) {
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

static int32_t validateNode(SNode* pNode, NodeValidationCtx* pCtx) {
  return pCtx->code;
}

static int32_t validateTargetNode(STargetNode* pTarget, NodeValidationCtx* pCtx) {
  const SSlotDescNode* pSlotDescNode = NULL;
  pCtx->code = validateSlotId(pTarget->slotId, pCtx->pOutputDesc, &pSlotDescNode);
  if (TSDB_CODE_SUCCESS != pCtx->code) {
    qLogWarn();
    return pCtx->code;
  }
  if (pTarget->pExpr) {
    if (nodeType(pTarget->pExpr) == QUERY_NODE_COLUMN) {
      pCtx->code = validateColumn((SColumnNode*)pTarget->pExpr, pCtx);
      if (TSDB_CODE_SUCCESS != pCtx->code) {
        qLogWarn();
        return pCtx->code;
      }
      if (memcmp(&pSlotDescNode->dataType, &((SColumnNode*)pTarget->pExpr)->node.resType, sizeof(SDataType)) != 0) {
        qLogWarn();
        return TSDB_CODE_PLAN_VALIDATION_ERR;
      }
    } else if (nodeType(pTarget->pExpr) == QUERY_NODE_FUNCTION) {
      pCtx->code = validateFunction((SFunctionNode*)pTarget->pExpr, pCtx);
      if (TSDB_CODE_SUCCESS != pCtx->code) {
        qLogWarn();
        return pCtx->code;
      }
    } else if (nodeType(pTarget->pExpr) == QUERY_NODE_VALUE) {
      return pCtx->code;
    } else {
      qLogWarn();
      return TSDB_CODE_PLAN_VALIDATION_ERR;
    }
  } else {
    qLogWarn();
    return pCtx->code = TSDB_CODE_PLAN_VALIDATION_ERR;
  }
  return pCtx->code;
}

static int32_t validateTargetNodes(SNodeList* pTargets, NodeValidationCtx* pCtx) {
  if (!pTargets) return TSDB_CODE_SUCCESS;
  SNode* pNode = NULL;
  FOREACH(pNode, pTargets) {
    if (nodeType(pNode) != QUERY_NODE_TARGET) {
      qLogWarn();
      return TSDB_CODE_PLAN_VALIDATION_ERR;
    }
    STargetNode* pTarget = (STargetNode*)pNode;
    pCtx->code = validateTargetNode(pTarget, pCtx);
    if (TSDB_CODE_SUCCESS != pCtx->code) {
      qLogWarn();
      return pCtx->code;
    }
  }
  return pCtx->code;
}

static int32_t walkAndValidateColumnNodes(SNodeList* pNodes, SPhysiNode* pNode, NodeValidationCtx* pCtx) {
  nodesWalkExprs(pNodes, validateColumnInWalk, pCtx);
  if (TSDB_CODE_SUCCESS != pCtx->code) {
    qLogWarn();
    return pCtx->code;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t validateGroupingSetNodes(SNodeList* pGroupingSets, NodeValidationCtx* pCtx) {
  if (!pGroupingSets) return TSDB_CODE_SUCCESS;

  SNode* pNode = NULL;
  FOREACH(pNode, pGroupingSets) {
    SGroupingSetNode* pGroupNode = (SGroupingSetNode*)pNode;
    if (LIST_LENGTH(pGroupNode->pParameterList) != 1) {
      qLogWarn();
      return TSDB_CODE_PLAN_VALIDATION_ERR;
    }
    pCtx->code = walkAndValidateColumnNodes(pGroupNode->pParameterList, pCtx->pNode, pCtx);
    if (TSDB_CODE_SUCCESS != pCtx->code) {
      qLogWarn();
      return pCtx->code;
    }
  }
  return pCtx->code;
}

static int32_t walkAndValidateColumnNode(SNode* pNode, SPhysiNode* pPhysiNode, NodeValidationCtx* pCtx) {
  nodesWalkExpr(pNode, validateColumnInWalk, pCtx);
  if (TSDB_CODE_SUCCESS != pCtx->code) {
    qLogWarn();
    return pCtx->code;
  }
  return TSDB_CODE_SUCCESS;
}

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

static int32_t validateBasePhysiNode(SPhysiNode* pNode, NodeValidationCtx* pCtx) {
  if (!pNode->pOutputDataBlockDesc || !pNode->pOutputDataBlockDesc->pSlots) {
    qLogWarn();
    return TSDB_CODE_PLAN_VALIDATION_ERR;
  }
  pCtx->code = tryGetOutputDesc(pNode, pCtx);
  if (TSDB_CODE_SUCCESS != pCtx->code) {
    qLogWarn();
    return pCtx->code;
  }
  pCtx->code = tryGetDownstreamOutput(pNode, pCtx);
  if (TSDB_CODE_SUCCESS != pCtx->code) {
    qLogWarn();
    return pCtx->code;
  }
  // walk conditions, check all Columns
  // TODO walk all nodes, all types
  int32_t code = walkAndValidateColumnNode(pNode->pConditions, pNode, pCtx);
  if (TSDB_CODE_SUCCESS != code) {
    qLogWarn();
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t validateScanPhysiNode(SScanPhysiNode* pNode, NodeValidationCtx* pCtx) {
  int32_t code = TSDB_CODE_SUCCESS;
  code = validateBasePhysiNode(&pNode->node, pCtx);
  if (TSDB_CODE_SUCCESS != code) {
    qLogWarn();
    return code;
  }
  SNode* pTmp = NULL;
  setValidateCol_IgnoreSlotId(pCtx, true);
  code = validateTargetNodes(pNode->pScanCols, pCtx);
  if (TSDB_CODE_SUCCESS != code) {
    qLogWarn();
    return code;
  }
  code = validateTargetNodes(pNode->pScanPseudoCols, pCtx);
  setValidateCol_IgnoreSlotId(pCtx, false);
  if (TSDB_CODE_SUCCESS != code) {
    qLogWarn();
    return code;
  }
  code = validateScanTableUid(pNode, pCtx);
  if (TSDB_CODE_SUCCESS != code) {
    qLogWarn();
    return code;
  }
  return code;
}

#define VALIDATE_BASE_SCAN_NODE(pNode)                                             \
  NodeValidationCtx ctx = {.code = TSDB_CODE_SUCCESS, .pNode = &pNode->scan.node}; \
  ctx.code = validateScanPhysiNode(&pNode->scan, &ctx);                            \
  if (TSDB_CODE_SUCCESS != ctx.code) {                                             \
    qLogWarn();                                                                    \
    return ctx.code;                                                               \
  }

int32_t doValidateTableScanPhysiNode(STableScanPhysiNode* pNode) {
  VALIDATE_BASE_SCAN_NODE(pNode);
  // SNodeList*     pDynamicScanFuncs; // seems not used
  // SNodeList*     pGroupTags;
  // TODO check all types of all nodes
  // TODO why we should ignore slotId check for pGroupTags
  setValidateCol_IgnoreSlotId(&ctx, true);
  ctx.code = walkAndValidateColumnNodes(pNode->pGroupTags, ctx.pNode, &ctx);
  setValidateCol_IgnoreSlotId(&ctx, false);
  if (TSDB_CODE_SUCCESS != ctx.code) {
    qLogWarn();
    return ctx.code;
  }

  // SNodeList*     pTags; // only for create stream
  // SNode*         pSubtable; // only for create stream
  return ctx.code;
}

int32_t doValidateTagScanPhysiNode(STagScanPhysiNode *pNode) {
  VALIDATE_BASE_SCAN_NODE(pNode);
  return ctx.code;
}

int32_t doValidateLastRowScanPhysiNode(SLastRowScanPhysiNode* pNode) {
  VALIDATE_BASE_SCAN_NODE(pNode);
  ctx.code = walkAndValidateColumnNodes(pNode->pGroupTags, ctx.pNode, &ctx);
  if (TSDB_CODE_SUCCESS != ctx.code) {
    qLogWarn();
    return ctx.code;
  }
  ctx.code = nodeTypeExpect(pNode->pTargets, QUERY_NODE_COLUMN);
  if (TSDB_CODE_SUCCESS != ctx.code) {
    qLogWarn();
    return ctx.code;
  }
  ctx.code = walkAndValidateColumnNodes(pNode->pTargets, ctx.pNode, &ctx);
  if (TSDB_CODE_SUCCESS != ctx.code) {
    qLogWarn();
    return ctx.code;
  }

  // validate pFuncTypes
  if (!pNode->pFuncTypes) {
    qLogWarn();
    return TSDB_CODE_PLAN_VALIDATION_ERR;
  }
  for (int32_t i = 0; i < pNode->pFuncTypes->size; ++i) {
    void* p = taosArrayGet(pNode->pFuncTypes, i);
    if (!p) {
      qLogWarn();
      return TSDB_CODE_PLAN_VALIDATION_ERR;
    }
    int32_t type = *(int32_t*)p;
    if (type <= 0 || type > FUNC_PARAM_TYPE_MAX) {
      qLogWarn();
      return TSDB_CODE_PLAN_VALIDATION_ERR;
    }
  }

  return ctx.code;
}

int32_t doValidateSystemTableScanPhysiNode(SSystemTableScanPhysiNode* pNode) {
  VALIDATE_BASE_SCAN_NODE(pNode);
  return ctx.code;
}

int32_t doValidateTableSeqScanPhysiNode(STableSeqScanPhysiNode *pNode) {
  return doValidateTableScanPhysiNode(pNode);
}

int32_t doValidateTableMergeScanPhysiNode(STableMergeScanPhysiNode* pNode) {
  return doValidateTableScanPhysiNode(pNode);
}

int32_t doValidateStreamScanPhysiNode(SStreamScanPhysiNode *pNode) {
  return doValidateTableScanPhysiNode(pNode);
}

int32_t doValidateBlockDistScanPhysiNode(SBlockDistScanPhysiNode* pNode) {
  NodeValidationCtx ctx = {.code = TSDB_CODE_SUCCESS, .pNode = &pNode->node};
  ctx.code = validateScanPhysiNode(pNode, &ctx);
  if (TSDB_CODE_SUCCESS != ctx.code) {
    qLogWarn();
    return ctx.code;
  }
  return ctx.code;
}

#define VALIDATE_BASE_PHYSI_NODE(pNode)                                       \
  NodeValidationCtx ctx = {.code = TSDB_CODE_SUCCESS, .pNode = &pNode->node}; \
  ctx.code = validateBasePhysiNode(&pNode->node, &ctx);                       \
  if (TSDB_CODE_SUCCESS != ctx.code) {                                        \
    qLogWarn();                                                               \
    return ctx.code;                                                          \
  }

int32_t doValidateProjectPhysiNode(SProjectPhysiNode* pNode) {
  NodeValidationCtx ctx = {.code = TSDB_CODE_SUCCESS, .pNode = &pNode->node};
  setValidateCol_ValidateFromDownstream(&ctx, true);
  ctx.code = validateBasePhysiNode(&pNode->node, &ctx);
  if (TSDB_CODE_SUCCESS != ctx.code) {
    qLogWarn();
    return ctx.code;
  }
  SNode* pC = NULL;
  setValidateCol_IgnoreOthers(&ctx, true);
  ctx.code = validateTargetNodes(pNode->pProjections, &ctx);
  setValidateCol_IgnoreOthers(&ctx, false);
  setValidateCol_ValidateFromDownstream(&ctx, false);
  if (TSDB_CODE_SUCCESS != ctx.code) {
    qLogWarn();
    return ctx.code;
  }
  return ctx.code;
}
