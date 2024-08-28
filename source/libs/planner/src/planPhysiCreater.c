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

typedef struct SSlotIdInfo {
  int16_t slotId;
  bool    set;
} SSlotIdInfo;

typedef struct SSlotIndex {
  int16_t dataBlockId;
  SArray* pSlotIdsInfo;  // duplicate name slot
} SSlotIndex;

typedef struct SPhysiPlanContext {
  SPlanContext* pPlanCxt;
  int32_t       errCode;
  int16_t       nextDataBlockId;
  SArray*       pLocationHelper;
  bool          hasScan;
  bool          hasSysScan;
} SPhysiPlanContext;

static int32_t getSlotKey(SNode* pNode, const char* pStmtName, char** ppKey, int32_t *pLen) {
  int32_t code = 0;
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    if (NULL != pStmtName) {
      if ('\0' != pStmtName[0]) {
        *ppKey = taosMemoryCalloc(1, TSDB_TABLE_NAME_LEN + 1 + TSDB_COL_NAME_LEN + 1);
        if (!*ppKey) {
          return terrno;
        }
        strcat(*ppKey, pStmtName);
        strcat(*ppKey, ".");
        strcat(*ppKey, pCol->node.aliasName);
        *pLen = taosHashBinary(*ppKey, strlen(*ppKey));
        return code;
      } else {
        *ppKey = taosMemoryCalloc(1, TSDB_COL_NAME_LEN + 1);
        if (!*ppKey) {
          return terrno;
        }
        strcat(*ppKey, pCol->node.aliasName);
        *pLen = strlen(*ppKey);
        return code;
      }
    }
    if ('\0' == pCol->tableAlias[0]) {
      *ppKey = taosMemoryCalloc(1, TSDB_COL_NAME_LEN + 1);
      if (!*ppKey) {
        return terrno;
      }
      strcat(*ppKey, pCol->colName);
      *pLen = strlen(*ppKey);
      return code;
    }

    *ppKey = taosMemoryCalloc(1, TSDB_TABLE_NAME_LEN + 1 + TSDB_COL_NAME_LEN + 1);
    if (!*ppKey) {
      return terrno;
    }
    strcat(*ppKey, pCol->tableAlias);
    strcat(*ppKey, ".");
    strcat(*ppKey, pCol->colName);
    *pLen = taosHashBinary(*ppKey, strlen(*ppKey));
    return code;
  } else if (QUERY_NODE_FUNCTION == nodeType(pNode)) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    if (FUNCTION_TYPE_TBNAME == pFunc->funcType) {
      SValueNode* pVal = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 0);
      if (pVal) {
        if (NULL != pStmtName && '\0' != pStmtName[0]) {
          *ppKey = taosMemoryCalloc(1, TSDB_TABLE_NAME_LEN + 1 + TSDB_COL_NAME_LEN + 1);
          if (!*ppKey) {
            return terrno;
          }
          strcat(*ppKey, pStmtName);
          strcat(*ppKey, ".");
          strcat(*ppKey, ((SExprNode*)pNode)->aliasName);
          *pLen = taosHashBinary(*ppKey, strlen(*ppKey));
          return code;
        }
        *ppKey = taosMemoryCalloc(1, strlen(pVal->literal) + 1 + TSDB_COL_NAME_LEN + 1);
        if (!*ppKey) {
          return terrno;
        }
        strcat(*ppKey, pVal->literal);
        strcat(*ppKey, ".");
        strcat(*ppKey, ((SExprNode*)pNode)->aliasName);
        *pLen = taosHashBinary(*ppKey, strlen(*ppKey));
        return code;
      }
    }
  }

  if (NULL != pStmtName && '\0' != pStmtName[0]) {
    *ppKey = taosMemoryCalloc(1, TSDB_TABLE_NAME_LEN + 1 + TSDB_COL_NAME_LEN + 1);
    if (!*ppKey) {
      return terrno;
    }
    strcat(*ppKey, pStmtName);
    strcat(*ppKey, ".");
    strcat(*ppKey, ((SExprNode*)pNode)->aliasName);
    *pLen = taosHashBinary(*ppKey, strlen(*ppKey));
    return code;
  }

  *ppKey = taosMemoryCalloc(1, TSDB_COL_NAME_LEN + 1);
  if (!*ppKey) {
    return terrno;
  }
  strcat(*ppKey, ((SExprNode*)pNode)->aliasName);
  *pLen = strlen(*ppKey);
  return code;
}


static SNode* createSlotDesc(SPhysiPlanContext* pCxt, const char* pName, const SNode* pNode, int16_t slotId,
                             bool output, bool reserve) {
  SSlotDescNode* pSlot = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_SLOT_DESC, (SNode**)&pSlot);
  if (NULL == pSlot) {
    terrno = code;
    return NULL;
  }
  snprintf(pSlot->name, sizeof(pSlot->name), "%s", pName);
  pSlot->slotId = slotId;
  pSlot->dataType = ((SExprNode*)pNode)->resType;
  pSlot->reserve = reserve;
  pSlot->output = output;
  return (SNode*)pSlot;
}

static int32_t createTarget(SNode* pNode, int16_t dataBlockId, int16_t slotId, SNode** pOutput) {
  STargetNode* pTarget = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_TARGET, (SNode**)&pTarget);
  if (NULL == pTarget) {
    return code;
  }

  pTarget->dataBlockId = dataBlockId;
  pTarget->slotId = slotId;
  pTarget->pExpr = pNode;

  *pOutput = (SNode*)pTarget;
  return TSDB_CODE_SUCCESS;
}

static int32_t putSlotToHashImpl(int16_t dataBlockId, int16_t slotId, const char* pName, int32_t len, SHashObj* pHash) {
  qInfo("wjm put slot into hash: %p, name: %s, slotId: %d", pHash, pName, slotId);
  SSlotIndex* pIndex = taosHashGet(pHash, pName, len);
  if (NULL != pIndex) {
    SSlotIdInfo info = {.slotId = slotId, .set = false};
    if (NULL == taosArrayPush(pIndex->pSlotIdsInfo, &info)) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    return TSDB_CODE_SUCCESS;
  }

  SSlotIndex index = {.dataBlockId = dataBlockId, .pSlotIdsInfo = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SSlotIdInfo))};
  if (NULL == index.pSlotIdsInfo) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  SSlotIdInfo info = {.slotId = slotId, .set = false};
  if (NULL == taosArrayPush(index.pSlotIdsInfo, &info)) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return taosHashPut(pHash, pName, len, &index, sizeof(SSlotIndex));
}

static int32_t putSlotToHash(const char* pName, int32_t len, int16_t dataBlockId, int16_t slotId, SNode* pNode, SHashObj* pHash) {
  return putSlotToHashImpl(dataBlockId, slotId, pName, len, pHash);
}

static int32_t createDataBlockDescHash(SPhysiPlanContext* pCxt, int32_t capacity, int16_t dataBlockId,
                                       SHashObj** pDescHash) {
  SHashObj* pHash = taosHashInit(capacity, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (NULL == pHash) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (NULL == taosArrayInsert(pCxt->pLocationHelper, dataBlockId, &pHash)) {
    taosHashCleanup(pHash);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  *pDescHash = pHash;
  return TSDB_CODE_SUCCESS;
}

static int32_t buildDataBlockSlots(SPhysiPlanContext* pCxt, SNodeList* pList, SDataBlockDescNode* pDataBlockDesc,
                                   SHashObj* pHash) {
  pDataBlockDesc->pSlots = NULL;
  int32_t code = nodesMakeList(&pDataBlockDesc->pSlots);
  if (NULL == pDataBlockDesc->pSlots) {
    return code;
  }

  int16_t slotId = 0;
  SNode*  pNode = NULL;
  FOREACH(pNode, pList) {
    char* name = NULL;
    int32_t len = 0;
    code = getSlotKey(pNode, NULL, &name, &len);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListStrictAppend(pDataBlockDesc->pSlots, createSlotDesc(pCxt, name, pNode, slotId, true, false));
    }
    if (TSDB_CODE_SUCCESS == code) {
      qInfo("wjm append slot name: %s, slotId: %d, aliasName: %s", name, slotId, ((SExprNode*)pNode)->aliasName);
      code = putSlotToHash(name, len, pDataBlockDesc->dataBlockId, slotId, pNode, pHash);
    }
    if (TSDB_CODE_SUCCESS == code) {
      if (nodeType(pNode) == QUERY_NODE_COLUMN && ((SColumnNode*)pNode)->projRefIdx > 0) {
        sprintf(name + strlen(name), "%d", ((SColumnNode*)pNode)->projRefIdx);
      }
      qInfo("wjm append slot name: %s, slotId: %d, aliasName: %s", name, slotId, ((SExprNode*)pNode)->aliasName);
      code = putSlotToHash(name, strlen(name), pDataBlockDesc->dataBlockId, slotId, pNode, pHash);
    }
    taosMemoryFree(name);
    if (TSDB_CODE_SUCCESS == code) {
      pDataBlockDesc->totalRowSize += ((SExprNode*)pNode)->resType.bytes;
      pDataBlockDesc->outputRowSize += ((SExprNode*)pNode)->resType.bytes;
      ++slotId;
    } else {
      break;
    }
  }
  return code;
}

static int32_t createDataBlockDesc(SPhysiPlanContext* pCxt, SNodeList* pList, SDataBlockDescNode** pDataBlockDesc) {
  SDataBlockDescNode* pDesc = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_DATABLOCK_DESC, (SNode**)&pDesc);
  if (NULL == pDesc) {
    return code;
  }
  pDesc->dataBlockId = pCxt->nextDataBlockId++;

  SHashObj* pHash = NULL;
  code = createDataBlockDescHash(pCxt, LIST_LENGTH(pList), pDesc->dataBlockId, &pHash);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildDataBlockSlots(pCxt, pList, pDesc, pHash);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pDataBlockDesc = pDesc;
  } else {
    nodesDestroyNode((SNode*)pDesc);
  }

  return code;
}

static int16_t getUnsetSlotId(const SArray* pSlotIdsInfo) {
  int32_t size = taosArrayGetSize(pSlotIdsInfo);
  for (int32_t i = 0; i < size; ++i) {
    SSlotIdInfo* pInfo = taosArrayGet(pSlotIdsInfo, i);
    if (!pInfo->set) {
      pInfo->set = true;
      return pInfo->slotId;
    }
  }
  return ((SSlotIdInfo*)taosArrayGet(pSlotIdsInfo, 0))->slotId;
}

static int32_t addDataBlockSlotsImpl(SPhysiPlanContext* pCxt, SNodeList* pList, SDataBlockDescNode* pDataBlockDesc,
                                     const char* pStmtName, bool output, bool reserve) {
  if (NULL == pList) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t   code = TSDB_CODE_SUCCESS;
  SHashObj* pHash = taosArrayGetP(pCxt->pLocationHelper, pDataBlockDesc->dataBlockId);
  int16_t   nextSlotId = LIST_LENGTH(pDataBlockDesc->pSlots), slotId = 0;
  SNode*    pNode = NULL;
  FOREACH(pNode, pList) {
    SNode*      pExpr = QUERY_NODE_ORDER_BY_EXPR == nodeType(pNode) ? ((SOrderByExprNode*)pNode)->pExpr : pNode;
    char        *name = NULL;
    int32_t     len = 0;
    code = getSlotKey(pExpr, pStmtName, &name, &len);
    if (TSDB_CODE_SUCCESS == code) {
      SSlotIndex* pIndex = taosHashGet(pHash, name, len);
      if (NULL == pIndex) {
        code =
          nodesListStrictAppend(pDataBlockDesc->pSlots, createSlotDesc(pCxt, name, pExpr, nextSlotId, output, reserve));
        if (TSDB_CODE_SUCCESS == code) {
          code = putSlotToHashImpl(pDataBlockDesc->dataBlockId, nextSlotId, name, len, pHash);
        }
        pDataBlockDesc->totalRowSize += ((SExprNode*)pExpr)->resType.bytes;
        if (output) {
          pDataBlockDesc->outputRowSize += ((SExprNode*)pExpr)->resType.bytes;
        }
        slotId = nextSlotId;
        ++nextSlotId;
      } else {
        slotId = getUnsetSlotId(pIndex->pSlotIdsInfo);
      }
    }

      qInfo("wjm add datablock slots for: %s id: %d, aliasName: %s", name, slotId, ((SExprNode*)pNode)->aliasName);
      taosMemoryFree(name);
    if (TSDB_CODE_SUCCESS == code) {
      SNode* pTarget = NULL;
      code = createTarget(pNode, pDataBlockDesc->dataBlockId, slotId, &pTarget);
      if (TSDB_CODE_SUCCESS == code) {
        REPLACE_NODE(pTarget);
      }
    }

    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }
  return code;
}

static int32_t addDataBlockSlots(SPhysiPlanContext* pCxt, SNodeList* pList, SDataBlockDescNode* pDataBlockDesc) {
  return addDataBlockSlotsImpl(pCxt, pList, pDataBlockDesc, NULL, false, false);
}

static int32_t addDataBlockSlot(SPhysiPlanContext* pCxt, SNode** pNode, SDataBlockDescNode* pDataBlockDesc) {
  if (NULL == pNode || NULL == *pNode) {
    return TSDB_CODE_SUCCESS;
  }

  SNodeList* pList = NULL;
  int32_t    code = nodesListMakeAppend(&pList, *pNode);
  if (TSDB_CODE_SUCCESS == code) {
    code = addDataBlockSlots(pCxt, pList, pDataBlockDesc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    *pNode = nodesListGetNode(pList, 0);
  }
  nodesClearList(pList);
  return code;
}

static int32_t addDataBlockSlotsForProject(SPhysiPlanContext* pCxt, const char* pStmtName, SNodeList* pList,
                                           SDataBlockDescNode* pDataBlockDesc) {
  return addDataBlockSlotsImpl(pCxt, pList, pDataBlockDesc, pStmtName, false, false);
}

static int32_t pushdownDataBlockSlots(SPhysiPlanContext* pCxt, SNodeList* pList, SDataBlockDescNode* pDataBlockDesc) {
  return addDataBlockSlotsImpl(pCxt, pList, pDataBlockDesc, NULL, true, true);
}

typedef struct SSetSlotIdCxt {
  int32_t   errCode;
  SHashObj* pLeftHash;
  SHashObj* pRightHash;
} SSetSlotIdCxt;

static void dumpSlots(const char* pName, SHashObj* pHash) {
  if (NULL == pHash) {
    return;
  }
  planDebug("%s", pName);
  void* pIt = taosHashIterate(pHash, NULL);
  while (NULL != pIt) {
    size_t len = 0;
    char*  pKey = taosHashGetKey(pIt, &len);
    char   name[TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN] = {0};
    strncpy(name, pKey, len);
    planDebug("\tslot name = %s", name);
    pIt = taosHashIterate(pHash, pIt);
  }
}

static EDealRes doSetSlotId(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode) && 0 != strcmp(((SColumnNode*)pNode)->colName, "*")) {
    SSetSlotIdCxt* pCxt = (SSetSlotIdCxt*)pContext;
    char           *name = NULL;
    int32_t        len = 0;
    pCxt->errCode = getSlotKey(pNode, NULL, &name, &len);
    if (TSDB_CODE_SUCCESS != pCxt->errCode) {
      return DEAL_RES_ERROR;
    }
    if (((SColumnNode*)pNode)->projRefIdx > 0) {
      sprintf(name + strlen(name), "%d", ((SColumnNode*)pNode)->projRefIdx);
    }
    SSlotIndex*    pIndex = taosHashGet(pCxt->pLeftHash, name, strlen(name));
    if (NULL == pIndex) {
      pIndex = taosHashGet(pCxt->pRightHash, name, strlen(name));
    }
    // pIndex is definitely not NULL, otherwise it is a bug
    if (NULL == pIndex) {
      planError("doSetSlotId failed, invalid slot name %s", name);
      dumpSlots("left datablock desc", pCxt->pLeftHash);
      dumpSlots("right datablock desc", pCxt->pRightHash);
      pCxt->errCode = TSDB_CODE_PLAN_INTERNAL_ERROR;
      taosMemoryFree(name);
      return DEAL_RES_ERROR;
    }
    ((SColumnNode*)pNode)->dataBlockId = pIndex->dataBlockId;
    ((SColumnNode*)pNode)->slotId = ((SSlotIdInfo*)taosArrayGet(pIndex->pSlotIdsInfo, 0))->slotId;
    qInfo("wjm set slotId for %s, slotId: %d, aliasName: %s", name, ((SColumnNode*)pNode)->slotId, ((SExprNode*)pNode)->aliasName);
    taosMemoryFree(name);
    return DEAL_RES_IGNORE_CHILD;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t setNodeSlotId(SPhysiPlanContext* pCxt, int16_t leftDataBlockId, int16_t rightDataBlockId, SNode* pNode,
                             SNode** pOutput) {
  if (NULL == pNode) {
    return TSDB_CODE_SUCCESS;
  }

  SNode* pRes = NULL;
  int32_t code = nodesCloneNode(pNode, &pRes);
  if (NULL == pRes) {
    return code;
  }

  SSetSlotIdCxt cxt = {
      .errCode = TSDB_CODE_SUCCESS,
      .pLeftHash = taosArrayGetP(pCxt->pLocationHelper, leftDataBlockId),
      .pRightHash = (rightDataBlockId < 0 ? NULL : taosArrayGetP(pCxt->pLocationHelper, rightDataBlockId))};
  nodesWalkExpr(pRes, doSetSlotId, &cxt);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyNode(pRes);
    return cxt.errCode;
  }

  *pOutput = pRes;
  return TSDB_CODE_SUCCESS;
}

static int32_t setListSlotId(SPhysiPlanContext* pCxt, int16_t leftDataBlockId, int16_t rightDataBlockId,
                             const SNodeList* pList, SNodeList** pOutput) {
  if (NULL == pList) {
    return TSDB_CODE_SUCCESS;
  }

  SNodeList* pRes = NULL;
  int32_t code = nodesCloneList(pList, &pRes);
  if (NULL == pRes) {
    return code;
  }

  SSetSlotIdCxt cxt = {
      .errCode = TSDB_CODE_SUCCESS,
      .pLeftHash = taosArrayGetP(pCxt->pLocationHelper, leftDataBlockId),
      .pRightHash = (rightDataBlockId < 0 ? NULL : taosArrayGetP(pCxt->pLocationHelper, rightDataBlockId))};
  nodesWalkExprs(pRes, doSetSlotId, &cxt);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyList(pRes);
    return cxt.errCode;
  }
  *pOutput = pRes;
  return TSDB_CODE_SUCCESS;
}

static SPhysiNode* makePhysiNode(SPhysiPlanContext* pCxt, SLogicNode* pLogicNode, ENodeType type) {
  SPhysiNode* pPhysiNode = NULL;
  int32_t code = nodesMakeNode(type, (SNode**)&pPhysiNode);
  if (NULL == pPhysiNode) {
    terrno = code;
    return NULL;
  }
  qInfo("wjm create node: %s", nodesNodeName(type));

  TSWAP(pPhysiNode->pLimit, pLogicNode->pLimit);
  TSWAP(pPhysiNode->pSlimit, pLogicNode->pSlimit);
  pPhysiNode->dynamicOp = pLogicNode->dynamicOp;
  pPhysiNode->inputTsOrder = pLogicNode->inputTsOrder;
  pPhysiNode->outputTsOrder = pLogicNode->outputTsOrder;

  code = createDataBlockDesc(pCxt, pLogicNode->pTargets, &pPhysiNode->pOutputDataBlockDesc);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pPhysiNode);
    terrno = code;
    return NULL;
  }
  pPhysiNode->pOutputDataBlockDesc->precision = pLogicNode->precision;
  return pPhysiNode;
}

static int32_t setConditionsSlotId(SPhysiPlanContext* pCxt, const SLogicNode* pLogicNode, SPhysiNode* pPhysiNode) {
  if (NULL != pLogicNode->pConditions) {
    return setNodeSlotId(pCxt, pPhysiNode->pOutputDataBlockDesc->dataBlockId, -1, pLogicNode->pConditions,
                         &pPhysiNode->pConditions);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t colIdCompare(const void* pLeft, const void* pRight) {
  SColumnNode* pLeftCol = *(SColumnNode**)pLeft;
  SColumnNode* pRightCol = *(SColumnNode**)pRight;
  return pLeftCol->colId > pRightCol->colId ? 1 : -1;
}

static int32_t sortScanCols(SNodeList* pScanCols) {
  SArray* pArray = taosArrayInit(LIST_LENGTH(pScanCols), POINTER_BYTES);
  if (NULL == pArray) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = 0;
  SNode* pCol = NULL;
  FOREACH(pCol, pScanCols) {
    if (NULL == taosArrayPush(pArray, &pCol)) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      break;
    }
  }
  if (TSDB_CODE_SUCCESS != code) {
    taosArrayDestroy(pArray);
    return code;
  }
  taosArraySort(pArray, colIdCompare);

  int32_t index = 0;
  FOREACH(pCol, pScanCols) { REPLACE_NODE(taosArrayGetP(pArray, index++)); }
  taosArrayDestroy(pArray);

  return TSDB_CODE_SUCCESS;
}

static int32_t createScanCols(SPhysiPlanContext* pCxt, SScanPhysiNode* pScanPhysiNode, SNodeList* pScanCols) {
  if (NULL == pScanCols) {
    return TSDB_CODE_SUCCESS;
  }

  pScanPhysiNode->pScanCols = NULL;
  int32_t code = nodesCloneList(pScanCols, &pScanPhysiNode->pScanCols);
  if (NULL == pScanPhysiNode->pScanCols) {
    return code;
  }
  return sortScanCols(pScanPhysiNode->pScanCols);
}

static int32_t createScanPhysiNodeFinalize(SPhysiPlanContext* pCxt, SSubplan* pSubplan, SScanLogicNode* pScanLogicNode,
                                           SScanPhysiNode* pScanPhysiNode, SPhysiNode** pPhyNode) {
  int32_t code = createScanCols(pCxt, pScanPhysiNode, pScanLogicNode->pScanCols);
  if (TSDB_CODE_SUCCESS == code) {
    code = addDataBlockSlots(pCxt, pScanPhysiNode->pScanCols, pScanPhysiNode->node.pOutputDataBlockDesc);
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pScanLogicNode->pScanPseudoCols) {
    pScanPhysiNode->pScanPseudoCols = NULL;
    code = nodesCloneList(pScanLogicNode->pScanPseudoCols, &pScanPhysiNode->pScanPseudoCols);
    if (NULL == pScanPhysiNode->pScanPseudoCols) {
      code = code;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = addDataBlockSlots(pCxt, pScanPhysiNode->pScanPseudoCols, pScanPhysiNode->node.pOutputDataBlockDesc);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setConditionsSlotId(pCxt, (const SLogicNode*)pScanLogicNode, (SPhysiNode*)pScanPhysiNode);
  }

  if (TSDB_CODE_SUCCESS == code) {
    pScanPhysiNode->uid = pScanLogicNode->tableId;
    pScanPhysiNode->suid = pScanLogicNode->stableId;
    pScanPhysiNode->tableType = pScanLogicNode->tableType;
    pScanPhysiNode->groupOrderScan = pScanLogicNode->groupOrderScan;
    memcpy(&pScanPhysiNode->tableName, &pScanLogicNode->tableName, sizeof(SName));
    if (NULL != pScanLogicNode->pTagCond) {
      pSubplan->pTagCond = NULL;
      code = nodesCloneNode(pScanLogicNode->pTagCond, &pSubplan->pTagCond);
      if (NULL == pSubplan->pTagCond) {
        code = code;
      }
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    if (NULL != pScanLogicNode->pTagIndexCond) {
      pSubplan->pTagIndexCond = NULL;
      code = nodesCloneNode(pScanLogicNode->pTagIndexCond, &pSubplan->pTagIndexCond);
      if (NULL == pSubplan->pTagIndexCond) {
        code = code;
      }
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pScanPhysiNode;
  } else {
    nodesDestroyNode((SNode*)pScanPhysiNode);
  }

  return code;
}

static void vgroupInfoToNodeAddr(const SVgroupInfo* vg, SQueryNodeAddr* pNodeAddr) {
  pNodeAddr->nodeId = vg->vgId;
  pNodeAddr->epSet = vg->epSet;
}

static ENodeType getScanOperatorType(EScanType scanType) {
  switch (scanType) {
    case SCAN_TYPE_TAG:
      return QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN;
    case SCAN_TYPE_TABLE:
      return QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN;
    case SCAN_TYPE_STREAM:
      return QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN;
    case SCAN_TYPE_TABLE_MERGE:
      return QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN;
    case SCAN_TYPE_BLOCK_INFO:
      return QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN;
    case SCAN_TYPE_TABLE_COUNT:
      return QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN;
    default:
      break;
  }
  return QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN;
}

static int32_t createSimpleScanPhysiNode(SPhysiPlanContext* pCxt, SSubplan* pSubplan, SScanLogicNode* pScanLogicNode,
                                         SPhysiNode** pPhyNode) {
  SScanPhysiNode* pScan =
      (SScanPhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pScanLogicNode, getScanOperatorType(pScanLogicNode->scanType));
  if (NULL == pScan) {
    return terrno;
  }

  if (pScanLogicNode->pVgroupList) {
    vgroupInfoToNodeAddr(pScanLogicNode->pVgroupList->vgroups, &pSubplan->execNode);
  }
  return createScanPhysiNodeFinalize(pCxt, pSubplan, pScanLogicNode, pScan, pPhyNode);
}

static int32_t createTagScanPhysiNode(SPhysiPlanContext* pCxt, SSubplan* pSubplan, SScanLogicNode* pScanLogicNode,
                                         SPhysiNode** pPhyNode) {
  STagScanPhysiNode* pScan =
      (STagScanPhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pScanLogicNode, QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN);
  if (NULL == pScan) {
    return terrno;
  }
  if (pScanLogicNode->pVgroupList) {
    vgroupInfoToNodeAddr(pScanLogicNode->pVgroupList->vgroups, &pSubplan->execNode);
  }
  pScan->onlyMetaCtbIdx = pScanLogicNode->onlyMetaCtbIdx;

  return createScanPhysiNodeFinalize(pCxt, pSubplan, pScanLogicNode, (SScanPhysiNode*)pScan, pPhyNode);
}

static int32_t createLastRowScanPhysiNode(SPhysiPlanContext* pCxt, SSubplan* pSubplan, SScanLogicNode* pScanLogicNode,
                                          SPhysiNode** pPhyNode) {
  SLastRowScanPhysiNode* pScan =
      (SLastRowScanPhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pScanLogicNode, QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN);
  if (NULL == pScan) {
    return terrno;
  }
  pScan->pTargets = NULL;
  int32_t code = nodesCloneList(pScanLogicNode->node.pTargets, &pScan->pTargets);
  if (TSDB_CODE_SUCCESS  != code) {
    nodesDestroyNode((SNode*)pScan);
    return code;
  }
  pScan->pGroupTags = NULL;
  code = nodesCloneList(pScanLogicNode->pGroupTags, &pScan->pGroupTags);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pScan);
    return code;
  }

  pScan->groupSort = pScanLogicNode->groupSort;
  pScan->ignoreNull = pScanLogicNode->igLastNull;

  if (pScanLogicNode->pVgroupList) {
    vgroupInfoToNodeAddr(pScanLogicNode->pVgroupList->vgroups, &pSubplan->execNode);
  }

  code = createScanPhysiNodeFinalize(pCxt, pSubplan, pScanLogicNode, (SScanPhysiNode*)pScan, pPhyNode);
  if (TSDB_CODE_SUCCESS == code && pScanLogicNode->pFuncTypes != NULL) {
    pScan->pFuncTypes = taosArrayInit(taosArrayGetSize(pScanLogicNode->pFuncTypes), sizeof(int32_t));
    if (NULL == pScan->pFuncTypes) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    SNode* pTargetNode = NULL;
    int funcTypeIndex = 0;
    FOREACH(pTargetNode, ((SScanPhysiNode*)pScan)->pScanCols) {
      if (((STargetNode*)pTargetNode)->pExpr->type != QUERY_NODE_COLUMN) {
        continue;
      }
      SColumnNode* pColNode = (SColumnNode*)((STargetNode*)pTargetNode)->pExpr;

      for (int i = 0; i < TARRAY_SIZE(pScanLogicNode->pFuncTypes); ++i) {
        SFunctParam* pFunctParam = taosArrayGet(pScanLogicNode->pFuncTypes, i);
        if (pColNode->colId == pFunctParam->pCol->colId &&
             0 == strncmp(pColNode->colName, pFunctParam->pCol->name, strlen(pColNode->colName))) {
          if (NULL == taosArrayInsert(pScan->pFuncTypes, funcTypeIndex, &pFunctParam->type)) {
            code = TSDB_CODE_OUT_OF_MEMORY;
          }
          break;
        }
      }
      funcTypeIndex++;
    }
  }
  return code;
}

static int32_t createTableCountScanPhysiNode(SPhysiPlanContext* pCxt, SSubplan* pSubplan,
                                             SScanLogicNode* pScanLogicNode, SPhysiNode** pPhyNode) {
  STableCountScanPhysiNode* pScan = (STableCountScanPhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pScanLogicNode,
                                                                             QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN);
  if (NULL == pScan) {
    return terrno;
  }

  pScan->pGroupTags = NULL;
  int32_t code = nodesCloneList(pScanLogicNode->pGroupTags, &pScan->pGroupTags);
  if (NULL != pScanLogicNode->pGroupTags && NULL == pScan->pGroupTags) {
    nodesDestroyNode((SNode*)pScan);
    return code;
  }

  pScan->groupSort = pScanLogicNode->groupSort;
  if (pScanLogicNode->pVgroupList) {
    vgroupInfoToNodeAddr(pScanLogicNode->pVgroupList->vgroups, &pSubplan->execNode);
  }
  return createScanPhysiNodeFinalize(pCxt, pSubplan, pScanLogicNode, (SScanPhysiNode*)pScan, pPhyNode);
}

static int32_t createTableScanPhysiNode(SPhysiPlanContext* pCxt, SSubplan* pSubplan, SScanLogicNode* pScanLogicNode,
                                        SPhysiNode** pPhyNode) {
  STableScanPhysiNode* pTableScan = (STableScanPhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pScanLogicNode,
                                                                        getScanOperatorType(pScanLogicNode->scanType));
  if (NULL == pTableScan) {
    return terrno;
  }

  memcpy(pTableScan->scanSeq, pScanLogicNode->scanSeq, sizeof(pScanLogicNode->scanSeq));
  pTableScan->scanRange = pScanLogicNode->scanRange;
  pTableScan->ratio = pScanLogicNode->ratio;
  if (pScanLogicNode->pVgroupList) {
    vgroupInfoToNodeAddr(pScanLogicNode->pVgroupList->vgroups, &pSubplan->execNode);
    pSubplan->execNodeStat.tableNum = pScanLogicNode->pVgroupList->vgroups[0].numOfTable;
  }
  (void)tNameGetFullDbName(&pScanLogicNode->tableName, pSubplan->dbFName);
  pTableScan->dataRequired = pScanLogicNode->dataRequired;
  pTableScan->pDynamicScanFuncs = NULL;
  int32_t code = nodesCloneList(pScanLogicNode->pDynamicScanFuncs, &pTableScan->pDynamicScanFuncs);
  if (TSDB_CODE_SUCCESS  != code) {
    nodesDestroyNode((SNode*)pTableScan);
    return code;
  }
  pTableScan->pGroupTags = NULL;
  code = nodesCloneList(pScanLogicNode->pGroupTags, &pTableScan->pGroupTags);
  if (TSDB_CODE_SUCCESS  != code) {
    nodesDestroyNode((SNode*)pTableScan);
    return code;
  }
  pTableScan->groupSort = pScanLogicNode->groupSort;
  pTableScan->interval = pScanLogicNode->interval;
  pTableScan->offset = pScanLogicNode->offset;
  pTableScan->sliding = pScanLogicNode->sliding;
  pTableScan->intervalUnit = pScanLogicNode->intervalUnit;
  pTableScan->slidingUnit = pScanLogicNode->slidingUnit;
  pTableScan->triggerType = pScanLogicNode->triggerType;
  pTableScan->watermark = pScanLogicNode->watermark;
  pTableScan->igExpired = pScanLogicNode->igExpired;
  pTableScan->igCheckUpdate = pScanLogicNode->igCheckUpdate;
  pTableScan->assignBlockUid = pCxt->pPlanCxt->rSmaQuery ? true : false;
  pTableScan->filesetDelimited = pScanLogicNode->filesetDelimited;
  pTableScan->needCountEmptyTable = pScanLogicNode->isCountByTag;
  pTableScan->paraTablesSort = pScanLogicNode->paraTablesSort;
  pTableScan->smallDataTsSort = pScanLogicNode->smallDataTsSort;

  code = createScanPhysiNodeFinalize(pCxt, pSubplan, pScanLogicNode, (SScanPhysiNode*)pTableScan, pPhyNode);
  if (TSDB_CODE_SUCCESS == code) {
    code = setListSlotId(pCxt, pTableScan->scan.node.pOutputDataBlockDesc->dataBlockId, -1, pScanLogicNode->pTags,
                         &pTableScan->pTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setNodeSlotId(pCxt, pTableScan->scan.node.pOutputDataBlockDesc->dataBlockId, -1, pScanLogicNode->pSubtable,
                         &pTableScan->pSubtable);
  }
  return code;
}

static int32_t createSystemTableScanPhysiNode(SPhysiPlanContext* pCxt, SSubplan* pSubplan,
                                              SScanLogicNode* pScanLogicNode, SPhysiNode** pPhyNode) {
  SSystemTableScanPhysiNode* pScan = (SSystemTableScanPhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pScanLogicNode,
                                                                               QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN);
  if (NULL == pScan) {
    return terrno;
  }

  pSubplan->showRewrite = pScanLogicNode->showRewrite;
  pScan->showRewrite = pScanLogicNode->showRewrite;
  pScan->accountId = pCxt->pPlanCxt->acctId;
  pScan->sysInfo = pCxt->pPlanCxt->sysInfo;
  if (0 == strcmp(pScanLogicNode->tableName.tname, TSDB_INS_TABLE_TABLES) ||
      0 == strcmp(pScanLogicNode->tableName.tname, TSDB_INS_TABLE_TAGS) ||
      0 == strcmp(pScanLogicNode->tableName.tname, TSDB_INS_TABLE_COLS)) {
    if (pScanLogicNode->pVgroupList) {
      vgroupInfoToNodeAddr(pScanLogicNode->pVgroupList->vgroups, &pSubplan->execNode);
    }
  } else {
    pSubplan->execNode.nodeId = MNODE_HANDLE;
    pSubplan->execNode.epSet = pCxt->pPlanCxt->mgmtEpSet;
  }
  if (0 == strcmp(pScanLogicNode->tableName.tname, TSDB_INS_TABLE_DNODE_VARIABLES) && pScanLogicNode->pVgroupList) {
    pScan->mgmtEpSet = pScanLogicNode->pVgroupList->vgroups->epSet;
  } else {
    pScan->mgmtEpSet = pCxt->pPlanCxt->mgmtEpSet;
  }
  (void)tNameGetFullDbName(&pScanLogicNode->tableName, pSubplan->dbFName);

  pCxt->hasSysScan = true;
  return createScanPhysiNodeFinalize(pCxt, pSubplan, pScanLogicNode, (SScanPhysiNode*)pScan, pPhyNode);
}

static int32_t createStreamScanPhysiNode(SPhysiPlanContext* pCxt, SSubplan* pSubplan, SScanLogicNode* pScanLogicNode,
                                         SPhysiNode** pPhyNode) {
  return createTableScanPhysiNode(pCxt, pSubplan, pScanLogicNode, pPhyNode);
}

static int32_t createTableMergeScanPhysiNode(SPhysiPlanContext* pCxt, SSubplan* pSubplan,
                                             SScanLogicNode* pScanLogicNode, SPhysiNode** pPhyNode) {
  return createTableScanPhysiNode(pCxt, pSubplan, pScanLogicNode, pPhyNode);
}

static int32_t createScanPhysiNode(SPhysiPlanContext* pCxt, SSubplan* pSubplan, SScanLogicNode* pScanLogicNode,
                                   SPhysiNode** pPhyNode) {
  pCxt->hasScan = true;
  switch (pScanLogicNode->scanType) {
    case SCAN_TYPE_TAG:
      return createTagScanPhysiNode(pCxt, pSubplan, pScanLogicNode, pPhyNode);
    case SCAN_TYPE_BLOCK_INFO:
      return createSimpleScanPhysiNode(pCxt, pSubplan, pScanLogicNode, pPhyNode);
    case SCAN_TYPE_TABLE_COUNT:
      return createTableCountScanPhysiNode(pCxt, pSubplan, pScanLogicNode, pPhyNode);
    case SCAN_TYPE_LAST_ROW:
      return createLastRowScanPhysiNode(pCxt, pSubplan, pScanLogicNode, pPhyNode);
    case SCAN_TYPE_TABLE:
      return createTableScanPhysiNode(pCxt, pSubplan, pScanLogicNode, pPhyNode);
    case SCAN_TYPE_SYSTEM_TABLE:
      return createSystemTableScanPhysiNode(pCxt, pSubplan, pScanLogicNode, pPhyNode);
    case SCAN_TYPE_STREAM:
      return createStreamScanPhysiNode(pCxt, pSubplan, pScanLogicNode, pPhyNode);
    case SCAN_TYPE_TABLE_MERGE:
      return createTableMergeScanPhysiNode(pCxt, pSubplan, pScanLogicNode, pPhyNode);
    default:
      break;
  }
  return TSDB_CODE_FAILED;
}

static int32_t getJoinDataBlockDescNode(SNodeList* pChildren, int32_t idx, SDataBlockDescNode** ppDesc) {
  if (2 == pChildren->length) {
    *ppDesc = ((SPhysiNode*)nodesListGetNode(pChildren, idx))->pOutputDataBlockDesc;
  } else if (1 == pChildren->length && nodeType(nodesListGetNode(pChildren, 0)) == QUERY_NODE_PHYSICAL_PLAN_GROUP_CACHE) {
    SGroupCachePhysiNode* pGrpCache = (SGroupCachePhysiNode*)nodesListGetNode(pChildren, 0);
    *ppDesc = ((SPhysiNode*)nodesListGetNode(pGrpCache->node.pChildren, idx))->pOutputDataBlockDesc;
  } else {
    planError("Invalid join children num:%d or child type:%d", pChildren->length, nodeType(nodesListGetNode(pChildren, 0)));
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t setColEqList(SNode* pEqCond, int16_t leftBlkId, int16_t rightBlkId, SNodeList** ppLeft, SNodeList** ppRight) {
  int32_t code = 0;
  if (QUERY_NODE_OPERATOR == nodeType(pEqCond) && ((SOperatorNode*)pEqCond)->opType == OP_TYPE_EQUAL) {
    SOperatorNode* pOp = (SOperatorNode*)pEqCond;
    SNode* pNew = NULL;
    if (leftBlkId == ((SColumnNode*)pOp->pLeft)->dataBlockId) {
      code = nodesCloneNode(pOp->pLeft, &pNew);
      if (TSDB_CODE_SUCCESS == code) {
        code = nodesListMakeStrictAppend(ppLeft, pNew);
      }
    } else if (rightBlkId == ((SColumnNode*)pOp->pLeft)->dataBlockId) {
      code = nodesCloneNode(pOp->pLeft, &pNew);
      if (TSDB_CODE_SUCCESS == code) {
        code = nodesListMakeStrictAppend(ppRight, pNew);
      }
    } else {
      planError("invalid col equal list, leftBlockId:%d", ((SColumnNode*)pOp->pLeft)->dataBlockId);
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }
    if (TSDB_CODE_SUCCESS  == code) {
      pNew = NULL;
      if (leftBlkId == ((SColumnNode*)pOp->pRight)->dataBlockId) {
        code = nodesCloneNode(pOp->pRight, &pNew);
        if (TSDB_CODE_SUCCESS == code) {
          code = nodesListMakeStrictAppend(ppLeft, pNew);
        }
      } else if (rightBlkId == ((SColumnNode*)pOp->pRight)->dataBlockId) {
        code = nodesCloneNode(pOp->pRight, &pNew);
        if (TSDB_CODE_SUCCESS == code) {
          code = nodesListMakeStrictAppend(ppRight, pNew);
        }
      } else {
        planError("invalid col equal list, rightBlockId:%d", ((SColumnNode*)pOp->pRight)->dataBlockId);
        return TSDB_CODE_PLAN_INTERNAL_ERROR;
      }
    }
  } else if (QUERY_NODE_LOGIC_CONDITION == nodeType(pEqCond) && ((SLogicConditionNode*)pEqCond)->condType == LOGIC_COND_TYPE_AND) {
    SLogicConditionNode* pLogic = (SLogicConditionNode*)pEqCond;
    SNode* pNode = NULL;
    FOREACH(pNode, pLogic->pParameterList) {
      int32_t code = setColEqList(pNode, leftBlkId, rightBlkId, ppLeft, ppRight);
      if (code) {
        return code;
      }
    }
  } else {
    planError("invalid col equal cond, type:%d", nodeType(pEqCond));
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  return code;
}

static int32_t setMergeJoinPrimColEqCond(SNode* pEqCond, int32_t subType, int16_t leftBlkId, int16_t rightBlkId, SSortMergeJoinPhysiNode* pJoin) {
  int32_t code = 0;
  if (QUERY_NODE_OPERATOR == nodeType(pEqCond)) {
    SOperatorNode* pOp = (SOperatorNode*)pEqCond;
    if (pOp->opType != OP_TYPE_EQUAL && JOIN_STYPE_ASOF != subType) {
      planError("invalid primary cond opType, opType:%d", pOp->opType);
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }

    switch (nodeType(pOp->pLeft)) {
      case QUERY_NODE_COLUMN: {
        SColumnNode* pCol = (SColumnNode*)pOp->pLeft;
        if (leftBlkId == pCol->dataBlockId) {
          pJoin->leftPrimSlotId = pCol->slotId;
          pJoin->asofOpType = pOp->opType;
        } else if (rightBlkId == pCol->dataBlockId) {
          pJoin->rightPrimSlotId = pCol->slotId;
        } else {
          planError("invalid primary key col equal cond, leftBlockId:%d", pCol->dataBlockId);
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        break;
      }
      case QUERY_NODE_FUNCTION: {
        SFunctionNode* pFunc = (SFunctionNode*)pOp->pLeft;
        if (FUNCTION_TYPE_TIMETRUNCATE != pFunc->funcType) {
          planError("invalid primary cond left function type, leftFuncType:%d", pFunc->funcType);
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        SNode* pParam = nodesListGetNode(pFunc->pParameterList, 0);
        if (QUERY_NODE_COLUMN != nodeType(pParam)) {
          planError("invalid primary cond left timetruncate param type, leftParamType:%d", nodeType(pParam));
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        SColumnNode* pCol = (SColumnNode*)pParam;
        if (leftBlkId == pCol->dataBlockId) {
          pJoin->leftPrimSlotId = pCol->slotId;
          pJoin->asofOpType = pOp->opType;
          pJoin->leftPrimExpr = NULL;
          code = nodesCloneNode((SNode*)pFunc, &pJoin->leftPrimExpr);
        } else if (rightBlkId == pCol->dataBlockId) {
          pJoin->rightPrimSlotId = pCol->slotId;
          pJoin->rightPrimExpr = NULL;
          code = nodesCloneNode((SNode*)pFunc, &pJoin->rightPrimExpr);
        } else {
          planError("invalid primary key col equal cond, leftBlockId:%d", pCol->dataBlockId);
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        break;
      }
      default:
        planError("invalid primary cond left node type, leftNodeType:%d", nodeType(pOp->pLeft));
        return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }
    if (TSDB_CODE_SUCCESS  != code) {
      return code;
    }
    switch (nodeType(pOp->pRight)) {
      case QUERY_NODE_COLUMN: {
        SColumnNode* pCol = (SColumnNode*)pOp->pRight;
        if (leftBlkId == pCol->dataBlockId) {
          pJoin->leftPrimSlotId = pCol->slotId;
          pJoin->asofOpType = getAsofJoinReverseOp(pOp->opType);
        } else if (rightBlkId == pCol->dataBlockId) {
          pJoin->rightPrimSlotId = pCol->slotId;
        } else {
          planError("invalid primary key col equal cond, rightBlockId:%d", pCol->dataBlockId);
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        break;
      }
      case QUERY_NODE_FUNCTION: {
        SFunctionNode* pFunc = (SFunctionNode*)pOp->pRight;
        if (FUNCTION_TYPE_TIMETRUNCATE != pFunc->funcType) {
          planError("invalid primary cond right function type, rightFuncType:%d", pFunc->funcType);
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        SNode* pParam = nodesListGetNode(pFunc->pParameterList, 0);
        if (QUERY_NODE_COLUMN != nodeType(pParam)) {
          planError("invalid primary cond right timetruncate param type, rightParamType:%d", nodeType(pParam));
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        SColumnNode* pCol = (SColumnNode*)pParam;
        if (leftBlkId == pCol->dataBlockId) {
          pJoin->leftPrimSlotId = pCol->slotId;
          pJoin->asofOpType = getAsofJoinReverseOp(pOp->opType);
          pJoin->leftPrimExpr = NULL;
          code = nodesCloneNode((SNode*)pFunc, &pJoin->leftPrimExpr);
        } else if (rightBlkId == pCol->dataBlockId) {
          pJoin->rightPrimSlotId = pCol->slotId;
          pJoin->rightPrimExpr = NULL;
          code = nodesCloneNode((SNode*)pFunc, &pJoin->rightPrimExpr);
        } else {
          planError("invalid primary key col equal cond, rightBlockId:%d", pCol->dataBlockId);
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        break;
      }
      default:
        planError("invalid primary cond right node type, rightNodeType:%d", nodeType(pOp->pRight));
        return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }
  } else {
    planError("invalid primary key col equal cond, type:%d", nodeType(pEqCond));
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  return code;
}

static int32_t createMergeJoinPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SJoinLogicNode* pJoinLogicNode,
                                   SPhysiNode** pPhyNode) {
  SSortMergeJoinPhysiNode* pJoin =
      (SSortMergeJoinPhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pJoinLogicNode, QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN);
  if (NULL == pJoin) {
    return terrno;
  }

  SDataBlockDescNode* pLeftDesc = NULL;
  SDataBlockDescNode* pRightDesc = NULL;
  pJoin->joinType = pJoinLogicNode->joinType;
  pJoin->subType = pJoinLogicNode->subType;
  pJoin->pWindowOffset = NULL;
  int32_t code = nodesCloneNode(pJoinLogicNode->pWindowOffset, &pJoin->pWindowOffset);
  if (TSDB_CODE_SUCCESS  == code) {
    pJoin->pJLimit = NULL;
    code = nodesCloneNode(pJoinLogicNode->pJLimit, (SNode**)&pJoin->pJLimit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pJoin->node.inputTsOrder = pJoinLogicNode->node.inputTsOrder;
    pJoin->seqWinGroup = pJoinLogicNode->seqWinGroup;
    pJoin->grpJoin = pJoinLogicNode->grpJoin;
    code = getJoinDataBlockDescNode(pChildren, 0, &pLeftDesc);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = getJoinDataBlockDescNode(pChildren, 1, &pRightDesc);
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pJoinLogicNode->pPrimKeyEqCond) {
    code = setNodeSlotId(pCxt, pLeftDesc->dataBlockId, pRightDesc->dataBlockId, pJoinLogicNode->pPrimKeyEqCond,
                  &pJoin->pPrimKeyCond);
    if (TSDB_CODE_SUCCESS == code) {
      code = setMergeJoinPrimColEqCond(pJoin->pPrimKeyCond, pJoin->subType, pLeftDesc->dataBlockId, pRightDesc->dataBlockId, pJoin);  
    }
    if (TSDB_CODE_SUCCESS == code && NULL != pJoin->leftPrimExpr) {
      code = addDataBlockSlot(pCxt, &pJoin->leftPrimExpr, pLeftDesc);
    }
    if (TSDB_CODE_SUCCESS == code && NULL != pJoin->rightPrimExpr) {
      code = addDataBlockSlot(pCxt, &pJoin->rightPrimExpr, pRightDesc);
    }
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pJoinLogicNode->addPrimEqCond) {
    SNode* pPrimKeyCond = NULL;
    code = setNodeSlotId(pCxt, pLeftDesc->dataBlockId, pRightDesc->dataBlockId, pJoinLogicNode->addPrimEqCond,
                  &pPrimKeyCond);
    if (TSDB_CODE_SUCCESS == code) {
      code = setMergeJoinPrimColEqCond(pPrimKeyCond, pJoin->subType, pLeftDesc->dataBlockId, pRightDesc->dataBlockId, pJoin);  
    }
    if (TSDB_CODE_SUCCESS == code && NULL != pJoin->leftPrimExpr) {
      code = addDataBlockSlot(pCxt, &pJoin->leftPrimExpr, pLeftDesc);
    }
    if (TSDB_CODE_SUCCESS == code && NULL != pJoin->rightPrimExpr) {
      code = addDataBlockSlot(pCxt, &pJoin->rightPrimExpr, pRightDesc);
    }
    nodesDestroyNode(pPrimKeyCond);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setListSlotId(pCxt, pLeftDesc->dataBlockId, pRightDesc->dataBlockId, pJoinLogicNode->node.pTargets,
                         &pJoin->pTargets);
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pJoinLogicNode->pFullOnCond) {
    code = setNodeSlotId(pCxt, ((SPhysiNode*)pJoin)->pOutputDataBlockDesc->dataBlockId, -1,
                         pJoinLogicNode->pFullOnCond, &pJoin->pFullOnCond);
  }

  if (TSDB_CODE_SUCCESS == code && ((NULL != pJoinLogicNode->pColEqCond) || (NULL != pJoinLogicNode->pTagEqCond))) {
    code = mergeJoinConds(&pJoinLogicNode->pColEqCond, &pJoinLogicNode->pTagEqCond);
  }
  //TODO set from input blocks for group algo
/*  
  if (TSDB_CODE_SUCCESS == code && NULL != pJoinLogicNode->pColEqCond) {
    code = setNodeSlotId(pCxt, pLeftDesc->dataBlockId, pRightDesc->dataBlockId, pJoinLogicNode->pColEqCond, &pJoin->pColEqCond);
  }
*/
  if (TSDB_CODE_SUCCESS == code && NULL != pJoinLogicNode->pColEqCond) {
    code = setNodeSlotId(pCxt, pLeftDesc->dataBlockId, pRightDesc->dataBlockId,
                         pJoinLogicNode->pColEqCond, &pJoin->pColEqCond);
    if (TSDB_CODE_SUCCESS == code) {        
      code = setColEqList(pJoin->pColEqCond, pLeftDesc->dataBlockId, pRightDesc->dataBlockId, &pJoin->pEqLeft, &pJoin->pEqRight);  
    }
  }

  if (TSDB_CODE_SUCCESS == code && ((NULL != pJoinLogicNode->pColOnCond) || (NULL != pJoinLogicNode->pTagOnCond))) {
    code = mergeJoinConds(&pJoinLogicNode->pColOnCond, &pJoinLogicNode->pTagOnCond);
  }
  //TODO set from input blocks for group algo
  /*  
    if (TSDB_CODE_SUCCESS == code && NULL != pJoinLogicNode->pColOnCond) {
      code = setNodeSlotId(pCxt, pLeftDesc->dataBlockId, pRightDesc->dataBlockId, pJoinLogicNode->pColOnCond, &pJoin->pColOnCond);
    }
  */
  if (TSDB_CODE_SUCCESS == code && NULL != pJoinLogicNode->pColOnCond) {
    code = setNodeSlotId(pCxt, ((SPhysiNode*)pJoin)->pOutputDataBlockDesc->dataBlockId, -1,
                         pJoinLogicNode->pColOnCond, &pJoin->pColOnCond);
  }

  
  if (TSDB_CODE_SUCCESS == code) {
    code = setConditionsSlotId(pCxt, (const SLogicNode*)pJoinLogicNode, (SPhysiNode*)pJoin);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = addDataBlockSlots(pCxt, pJoin->pTargets, pJoin->node.pOutputDataBlockDesc);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pJoin;
  } else {
    nodesDestroyNode((SNode*)pJoin);
  }

  return code;
}

static int32_t extractHashJoinOpCols(int16_t lBlkId, int16_t rBlkId, SNode* pEq, SHashJoinPhysiNode* pJoin) {
  int32_t code = 0;
  if (QUERY_NODE_OPERATOR == nodeType(pEq)) {
    SOperatorNode* pOp = (SOperatorNode*)pEq;
    SColumnNode* pLeft = (SColumnNode*)pOp->pLeft;
    SColumnNode* pRight = (SColumnNode*)pOp->pRight;
    if (lBlkId == pLeft->dataBlockId && rBlkId == pRight->dataBlockId) {
      SNode* pL = NULL, *pR = NULL;
      code = nodesCloneNode(pOp->pLeft, &pL);
      if (TSDB_CODE_SUCCESS == code) {
        code = nodesListStrictAppend(pJoin->pOnLeft, pL);
      }
      if (TSDB_CODE_SUCCESS  == code) {
        code = nodesCloneNode(pOp->pRight, &pR);
      }
      if (TSDB_CODE_SUCCESS  == code) {
        code = nodesListStrictAppend(pJoin->pOnRight, pR);
      }
    } else if (rBlkId == pLeft->dataBlockId && lBlkId == pRight->dataBlockId) {
      SNode* pL = NULL, *pR = NULL;
      code = nodesCloneNode(pOp->pRight, &pR);
      if (TSDB_CODE_SUCCESS  == code) {
        code = nodesListStrictAppend(pJoin->pOnLeft, pR);
      }
      if (TSDB_CODE_SUCCESS  == code) {
        code = nodesCloneNode(pOp->pLeft, &pL);
      }
      if (TSDB_CODE_SUCCESS  == code) {
        code = nodesListStrictAppend(pJoin->pOnRight, pL);
      }
    } else {
      planError("Invalid join equal cond, lbid:%d, rbid:%d, oplid:%d, oprid:%d", lBlkId, rBlkId, pLeft->dataBlockId, pRight->dataBlockId);
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }

    return code;
  }

  planError("Invalid join equal node type:%d", nodeType(pEq));
  return TSDB_CODE_PLAN_INTERNAL_ERROR;
}

static int32_t extractHashJoinOnCols(int16_t lBlkId, int16_t rBlkId, SNode* pEq, SHashJoinPhysiNode* pJoin) {
  if (NULL == pEq) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  if (QUERY_NODE_OPERATOR == nodeType(pEq)) {
    code = extractHashJoinOpCols(lBlkId, rBlkId, pEq, pJoin);
  } else if (QUERY_NODE_LOGIC_CONDITION == nodeType(pEq)) {
    SLogicConditionNode* pLogic = (SLogicConditionNode*)pEq;
    SNode* pNode = NULL;
    FOREACH(pNode, pLogic->pParameterList) {
      code = extractHashJoinOpCols(lBlkId, rBlkId, pNode, pJoin);
      if (code) {
        break;
      }
    }
  } else {
    planError("Invalid join equal node type:%d", nodeType(pEq));
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  return code;
}

static int32_t createHashJoinColList(int16_t lBlkId, int16_t rBlkId, SNode* pEq1, SNode* pEq2, SNode* pEq3, SHashJoinPhysiNode* pJoin) {
  int32_t code = TSDB_CODE_SUCCESS;
  pJoin->pOnLeft = NULL;
  code = nodesMakeList(&pJoin->pOnLeft);
  if (TSDB_CODE_SUCCESS  != code) {
    return code;
  }
  pJoin->pOnRight = NULL;
  code = nodesMakeList(&pJoin->pOnRight);
  if (TSDB_CODE_SUCCESS  != code) {
    return code;
  }

  code = extractHashJoinOnCols(lBlkId, rBlkId, pEq1, pJoin);
  if (TSDB_CODE_SUCCESS == code) {
    code = extractHashJoinOnCols(lBlkId, rBlkId, pEq2, pJoin);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = extractHashJoinOnCols(lBlkId, rBlkId, pEq3, pJoin);
  }
  if (TSDB_CODE_SUCCESS == code && pJoin->pOnLeft->length <= 0) {
    planError("Invalid join equal column num: %d", pJoin->pOnLeft->length);
    code = TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  return code;
}

static int32_t sortHashJoinTargets(int16_t lBlkId, int16_t rBlkId, SHashJoinPhysiNode* pJoin) {
  SNode*  pNode = NULL;
  SSHashObj* pHash = tSimpleHashInit(pJoin->pTargets->length, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
  if (NULL == pHash) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  SNodeList* pNew = NULL;
  int32_t code = nodesMakeList(&pNew);

  if (TSDB_CODE_SUCCESS  == code) {
    FOREACH(pNode, pJoin->pTargets) {
      SColumnNode* pCol = (SColumnNode*)pNode;
      char *pName = NULL;
      int32_t len = 0;
      code = getSlotKey(pNode, NULL, &pName, &len);
      if (TSDB_CODE_SUCCESS == code) {
        code = tSimpleHashPut(pHash, pName, len, &pCol, POINTER_BYTES);
      }
      taosMemoryFree(pName);
      if (TSDB_CODE_SUCCESS  != code) {
        break;
      }
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    nodesClearList(pJoin->pTargets);
    pJoin->pTargets = pNew;

    FOREACH(pNode, pJoin->pOnLeft) {
      char* pName = NULL;
      SColumnNode* pCol = (SColumnNode*)pNode;
      int32_t len = 0;
      code = getSlotKey(pNode, NULL, &pName, &len);
      if (TSDB_CODE_SUCCESS == code) {
        SNode** p = tSimpleHashGet(pHash, pName, len);
        if (p) {
          code = nodesListStrictAppend(pJoin->pTargets, *p);
          if (TSDB_CODE_SUCCESS == code) {
            code = tSimpleHashRemove(pHash, pName, len);
          }
        }
      }
      taosMemoryFree(pName);
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    FOREACH(pNode, pJoin->pOnRight) {
      char* pName = NULL;
      SColumnNode* pCol = (SColumnNode*)pNode;
      int32_t len = 0;
      code = getSlotKey(pNode, NULL, &pName, &len);
      if (TSDB_CODE_SUCCESS == code) {
        SNode** p = tSimpleHashGet(pHash, pName, len);
        if (p) {
          code = nodesListStrictAppend(pJoin->pTargets, *p);
          if (TSDB_CODE_SUCCESS == code) {
            code = tSimpleHashRemove(pHash, pName, len);
          }
        }
      }
      taosMemoryFree(pName);
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    if (tSimpleHashGetSize(pHash) > 0) {
      SNode** p = NULL;
      int32_t iter = 0;
      while (1) {
        p = tSimpleHashIterate(pHash, p, &iter);
        if (p == NULL) {
          break;
        }

        code = nodesListStrictAppend(pJoin->pTargets, *p);
        if (TSDB_CODE_SUCCESS != code) {
          break;
        }
      }
    }
  }

  tSimpleHashCleanup(pHash);

  return code;
}


static int32_t setHashJoinPrimColEqCond(SNode* pEqCond, int16_t leftBlkId, int16_t rightBlkId, SHashJoinPhysiNode* pJoin) {
  int32_t code = 0;
  if (QUERY_NODE_OPERATOR == nodeType(pEqCond)) {
    SOperatorNode* pOp = (SOperatorNode*)pEqCond;
    if (pOp->opType != OP_TYPE_EQUAL) {
      planError("invalid primary cond opType, opType:%d", pOp->opType);
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }

    switch (nodeType(pOp->pLeft)) {
      case QUERY_NODE_COLUMN: {
        SColumnNode* pCol = (SColumnNode*)pOp->pLeft;
        if (leftBlkId == pCol->dataBlockId) {
          pJoin->leftPrimSlotId = pCol->slotId;
        } else if (rightBlkId == pCol->dataBlockId) {
          pJoin->rightPrimSlotId = pCol->slotId;
        } else {
          planError("invalid primary key col equal cond, leftBlockId:%d", pCol->dataBlockId);
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        break;
      }
      case QUERY_NODE_FUNCTION: {
        SFunctionNode* pFunc = (SFunctionNode*)pOp->pLeft;
        if (FUNCTION_TYPE_TIMETRUNCATE != pFunc->funcType) {
          planError("invalid primary cond left function type, leftFuncType:%d", pFunc->funcType);
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        SNode* pParam = nodesListGetNode(pFunc->pParameterList, 0);
        if (QUERY_NODE_COLUMN != nodeType(pParam)) {
          planError("invalid primary cond left timetruncate param type, leftParamType:%d", nodeType(pParam));
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        SColumnNode* pCol = (SColumnNode*)pParam;
        if (leftBlkId == pCol->dataBlockId) {
          pJoin->leftPrimSlotId = pCol->slotId;
          pJoin->leftPrimExpr = NULL;
          code = nodesCloneNode((SNode*)pFunc, &pJoin->leftPrimExpr);
        } else if (rightBlkId == pCol->dataBlockId) {
          pJoin->rightPrimSlotId = pCol->slotId;
          pJoin->rightPrimExpr = NULL;
          code = nodesCloneNode((SNode*)pFunc, &pJoin->rightPrimExpr);
        } else {
          planError("invalid primary key col equal cond, leftBlockId:%d", pCol->dataBlockId);
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        break;
      }
      default:
        planError("invalid primary cond left node type, leftNodeType:%d", nodeType(pOp->pLeft));
        return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    switch (nodeType(pOp->pRight)) {
      case QUERY_NODE_COLUMN: {
        SColumnNode* pCol = (SColumnNode*)pOp->pRight;
        if (leftBlkId == pCol->dataBlockId) {
          pJoin->leftPrimSlotId = pCol->slotId;
        } else if (rightBlkId == pCol->dataBlockId) {
          pJoin->rightPrimSlotId = pCol->slotId;
        } else {
          planError("invalid primary key col equal cond, rightBlockId:%d", pCol->dataBlockId);
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        break;
      }
      case QUERY_NODE_FUNCTION: {
        SFunctionNode* pFunc = (SFunctionNode*)pOp->pRight;
        if (FUNCTION_TYPE_TIMETRUNCATE != pFunc->funcType) {
          planError("invalid primary cond right function type, rightFuncType:%d", pFunc->funcType);
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        SNode* pParam = nodesListGetNode(pFunc->pParameterList, 0);
        if (QUERY_NODE_COLUMN != nodeType(pParam)) {
          planError("invalid primary cond right timetruncate param type, rightParamType:%d", nodeType(pParam));
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        SColumnNode* pCol = (SColumnNode*)pParam;
        if (leftBlkId == pCol->dataBlockId) {
          pJoin->leftPrimSlotId = pCol->slotId;
          pJoin->leftPrimExpr = NULL;
          code = nodesCloneNode((SNode*)pFunc, &pJoin->leftPrimExpr);
        } else if (rightBlkId == pCol->dataBlockId) {
          pJoin->rightPrimSlotId = pCol->slotId;
          pJoin->rightPrimExpr = NULL;
          code = nodesCloneNode((SNode*)pFunc, &pJoin->rightPrimExpr);
        } else {
          planError("invalid primary key col equal cond, rightBlockId:%d", pCol->dataBlockId);
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        break;
      }
      default:
        planError("invalid primary cond right node type, rightNodeType:%d", nodeType(pOp->pRight));
        return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }
  } else {
    planError("invalid primary key col equal cond, type:%d", nodeType(pEqCond));
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  return code;
}


static int32_t createHashJoinPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SJoinLogicNode* pJoinLogicNode,
                                  SPhysiNode** pPhyNode) {
  SHashJoinPhysiNode* pJoin =
     (SHashJoinPhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pJoinLogicNode, QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN);
  if (NULL == pJoin) {
    return terrno;
  }

  SDataBlockDescNode* pLeftDesc = ((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc;
  SDataBlockDescNode* pRightDesc = ((SPhysiNode*)nodesListGetNode(pChildren, 1))->pOutputDataBlockDesc;
  int32_t             code = TSDB_CODE_SUCCESS;

  pJoin->joinType = pJoinLogicNode->joinType;
  pJoin->subType = pJoinLogicNode->subType;
  pJoin->pWindowOffset = NULL;
  code = nodesCloneNode(pJoinLogicNode->pWindowOffset, &pJoin->pWindowOffset);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pJoin);
    return code;
  }
  pJoin->pJLimit = NULL;
  code = nodesCloneNode(pJoinLogicNode->pJLimit, &pJoin->pJLimit);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pJoin);
    return code;
  }
  pJoin->node.inputTsOrder = pJoinLogicNode->node.inputTsOrder;
  pJoin->timeRangeTarget = pJoinLogicNode->timeRangeTarget;
  pJoin->timeRange.skey = pJoinLogicNode->timeRange.skey;
  pJoin->timeRange.ekey = pJoinLogicNode->timeRange.ekey;

  if (NULL != pJoinLogicNode->pPrimKeyEqCond) {
    code = setNodeSlotId(pCxt, pLeftDesc->dataBlockId, pRightDesc->dataBlockId, pJoinLogicNode->pPrimKeyEqCond,
                  &pJoin->pPrimKeyCond);
    if (TSDB_CODE_SUCCESS == code) {
      code = setHashJoinPrimColEqCond(pJoin->pPrimKeyCond, pLeftDesc->dataBlockId, pRightDesc->dataBlockId, pJoin);  
    }
    if (TSDB_CODE_SUCCESS == code && NULL != pJoin->leftPrimExpr) {
      code = addDataBlockSlot(pCxt, &pJoin->leftPrimExpr, pLeftDesc);
    }
    if (TSDB_CODE_SUCCESS == code && NULL != pJoin->rightPrimExpr) {
      code = addDataBlockSlot(pCxt, &pJoin->rightPrimExpr, pRightDesc);
    }    
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setNodeSlotId(pCxt, pLeftDesc->dataBlockId, pRightDesc->dataBlockId, pJoinLogicNode->pColEqCond, &pJoin->pColEqCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setNodeSlotId(pCxt, pLeftDesc->dataBlockId, pRightDesc->dataBlockId, pJoinLogicNode->pTagEqCond, &pJoin->pTagEqCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setNodeSlotId(pCxt, pLeftDesc->dataBlockId, -1, pJoinLogicNode->pLeftOnCond, &pJoin->pLeftOnCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setNodeSlotId(pCxt, -1, pRightDesc->dataBlockId, pJoinLogicNode->pRightOnCond, &pJoin->pRightOnCond);
  }
  if (TSDB_CODE_SUCCESS == code && ((NULL != pJoinLogicNode->pColOnCond) || (NULL != pJoinLogicNode->pTagOnCond))) {
    code = mergeJoinConds(&pJoinLogicNode->pColOnCond, &pJoinLogicNode->pTagOnCond);
  }  
  SNode* pOnCond = (NULL != pJoinLogicNode->pColOnCond) ? pJoinLogicNode->pColOnCond : pJoinLogicNode->pTagOnCond;
  if (TSDB_CODE_SUCCESS == code && NULL != pOnCond) {
    code = setNodeSlotId(pCxt, ((SPhysiNode*)pJoin)->pOutputDataBlockDesc->dataBlockId, -1, pOnCond, &pJoin->pFullOnCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setListSlotId(pCxt, pLeftDesc->dataBlockId, pRightDesc->dataBlockId, pJoinLogicNode->node.pTargets, &pJoin->pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setConditionsSlotId(pCxt, (const SLogicNode*)pJoinLogicNode, (SPhysiNode*)pJoin);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createHashJoinColList(pLeftDesc->dataBlockId, pRightDesc->dataBlockId, pJoin->pPrimKeyCond, pJoin->pColEqCond, pJoin->pTagEqCond, pJoin);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = sortHashJoinTargets(pLeftDesc->dataBlockId, pRightDesc->dataBlockId, pJoin);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = addDataBlockSlots(pCxt, pJoin->pTargets, pJoin->node.pOutputDataBlockDesc);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pJoin;
  } else {
    nodesDestroyNode((SNode*)pJoin);
  }

  return code;
}

static int32_t createJoinPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SJoinLogicNode* pJoinLogicNode,
                                   SPhysiNode** pPhyNode) {
  switch (pJoinLogicNode->joinAlgo) {
    case JOIN_ALGO_MERGE:
      return createMergeJoinPhysiNode(pCxt, pChildren, pJoinLogicNode, pPhyNode);
    case JOIN_ALGO_HASH:
      return createHashJoinPhysiNode(pCxt, pChildren, pJoinLogicNode, pPhyNode);
    default:
      planError("Invalid join algorithm:%d", pJoinLogicNode->joinAlgo);
      break;
  }

  return TSDB_CODE_FAILED;
}

static int32_t createGroupCachePhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SGroupCacheLogicNode* pLogicNode,
                                    SPhysiNode** pPhyNode) {
  SGroupCachePhysiNode* pGrpCache =
     (SGroupCachePhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pLogicNode, QUERY_NODE_PHYSICAL_PLAN_GROUP_CACHE);
  if (NULL == pGrpCache) {
    return terrno;
  }

  pGrpCache->grpColsMayBeNull = pLogicNode->grpColsMayBeNull;
  pGrpCache->grpByUid = pLogicNode->grpByUid;
  pGrpCache->globalGrp = pLogicNode->globalGrp;
  pGrpCache->batchFetch = pLogicNode->batchFetch;
  SDataBlockDescNode* pChildDesc = ((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc;
  int32_t             code = TSDB_CODE_SUCCESS;
/*
  if (TSDB_CODE_SUCCESS == code) {
    code = setListSlotId(pCxt, pChildDesc->dataBlockId, -1, pLogicNode->pGroupCols, &pGrpCache->pGroupCols);
  }
*/

  *pPhyNode = (SPhysiNode*)pGrpCache;

  return code;
}

static int32_t updateDynQueryCtrlStbJoinInfo(SPhysiPlanContext* pCxt, SNodeList* pChildren, SDynQueryCtrlLogicNode* pLogicNode,
                                            SDynQueryCtrlPhysiNode* pDynCtrl) {
  SDataBlockDescNode* pPrevDesc = ((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc;
  SNodeList* pVgList = NULL;
  SNodeList* pUidList = NULL;
  int32_t code = setListSlotId(pCxt, pPrevDesc->dataBlockId, -1, pLogicNode->stbJoin.pVgList, &pVgList);
  if (TSDB_CODE_SUCCESS == code) {
    code = setListSlotId(pCxt, pPrevDesc->dataBlockId, -1, pLogicNode->stbJoin.pUidList, &pUidList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    memcpy(pDynCtrl->stbJoin.srcScan, pLogicNode->stbJoin.srcScan, sizeof(pDynCtrl->stbJoin.srcScan));

    SNode* pNode = NULL;
    int32_t i = 0;
    FOREACH(pNode, pVgList) {
      pDynCtrl->stbJoin.vgSlot[i] = ((SColumnNode*)pNode)->slotId;
      ++i;
    }
    i = 0;
    FOREACH(pNode, pUidList) {
      pDynCtrl->stbJoin.uidSlot[i] = ((SColumnNode*)pNode)->slotId;
      ++i;
    }
    pDynCtrl->stbJoin.batchFetch = pLogicNode->stbJoin.batchFetch;
  }
  nodesDestroyList(pVgList);
  nodesDestroyList(pUidList);

  return code;
}

static int32_t createDynQueryCtrlPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SDynQueryCtrlLogicNode* pLogicNode,
                                            SPhysiNode** pPhyNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  SDynQueryCtrlPhysiNode* pDynCtrl =
  (SDynQueryCtrlPhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pLogicNode, QUERY_NODE_PHYSICAL_PLAN_DYN_QUERY_CTRL);
  if (NULL == pDynCtrl) {
    return terrno;
  }

  switch (pLogicNode->qType) {
    case DYN_QTYPE_STB_HASH:
      code = updateDynQueryCtrlStbJoinInfo(pCxt, pChildren, pLogicNode, pDynCtrl);
      break;
    default:
      planError("Invalid dyn query ctrl type:%d", pLogicNode->qType);
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  if (TSDB_CODE_SUCCESS == code) {
    pDynCtrl->qType = pLogicNode->qType;
    *pPhyNode = (SPhysiNode*)pDynCtrl;
  }

  return code;
}

typedef struct SRewritePrecalcExprsCxt {
  int32_t    errCode;
  int32_t    planNodeId;
  int32_t    rewriteId;
  SNodeList* pPrecalcExprs;
} SRewritePrecalcExprsCxt;

static EDealRes collectAndRewrite(SRewritePrecalcExprsCxt* pCxt, SNode** pNode) {
  SNode* pExpr = NULL;
  pCxt->errCode = nodesCloneNode(*pNode, &pExpr);
  if (NULL == pExpr) {
    return DEAL_RES_ERROR;
  }
  if (TSDB_CODE_SUCCESS != (pCxt->errCode = nodesListAppend(pCxt->pPrecalcExprs, pExpr))) {
    nodesDestroyNode(pExpr);
    return DEAL_RES_ERROR;
  }
  SColumnNode* pCol = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  if (NULL == pCol) {
    nodesDestroyNode(pExpr);
    return DEAL_RES_ERROR;
  }
  SExprNode* pRewrittenExpr = (SExprNode*)pExpr;
  pCol->node.resType = pRewrittenExpr->resType;
  if ('\0' != pRewrittenExpr->aliasName[0]) {
    strcpy(pCol->colName, pRewrittenExpr->aliasName);
  } else {
    snprintf(pRewrittenExpr->aliasName, sizeof(pRewrittenExpr->aliasName), "#expr_%d_%d", pCxt->planNodeId,
             pCxt->rewriteId);
    strcpy(pCol->colName, pRewrittenExpr->aliasName);
  }
  nodesDestroyNode(*pNode);
  *pNode = (SNode*)pCol;
  return DEAL_RES_IGNORE_CHILD;
}

static int32_t rewriteValueToOperator(SRewritePrecalcExprsCxt* pCxt, SNode** pNode) {
  SOperatorNode* pOper = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pOper);
  if (NULL == pOper) {
    return code;
  }
  pOper->pLeft = NULL;
  code = nodesMakeNode(QUERY_NODE_LEFT_VALUE, &pOper->pLeft);
  if (NULL == pOper->pLeft) {
    nodesDestroyNode((SNode*)pOper);
    return code;
  }
  SValueNode* pVal = (SValueNode*)*pNode;
  pOper->node.resType = pVal->node.resType;
  strcpy(pOper->node.aliasName, pVal->node.aliasName);
  pOper->opType = OP_TYPE_ASSIGN;
  pOper->pRight = *pNode;
  *pNode = (SNode*)pOper;
  return TSDB_CODE_SUCCESS;
}

static EDealRes doRewritePrecalcExprs(SNode** pNode, void* pContext) {
  SRewritePrecalcExprsCxt* pCxt = (SRewritePrecalcExprsCxt*)pContext;
  switch (nodeType(*pNode)) {
    case QUERY_NODE_VALUE: {
      if (((SValueNode*)*pNode)->notReserved) {
        break;
      }
      pCxt->errCode = rewriteValueToOperator(pCxt, pNode);
      if (TSDB_CODE_SUCCESS != pCxt->errCode) {
        return DEAL_RES_ERROR;
      }
      return collectAndRewrite(pCxt, pNode);
    }
    case QUERY_NODE_OPERATOR:
    case QUERY_NODE_LOGIC_CONDITION:
    case QUERY_NODE_CASE_WHEN: {
      return collectAndRewrite(pCxt, pNode);
    }
    case QUERY_NODE_FUNCTION: {
      if (fmIsScalarFunc(((SFunctionNode*)(*pNode))->funcId)) {
        return collectAndRewrite(pCxt, pNode);
      }
    }
    default:
      break;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t rewritePrecalcExprs(SPhysiPlanContext* pCxt, SNodeList* pList, SNodeList** pPrecalcExprs,
                                   SNodeList** pRewrittenList) {
  if (NULL == pList) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t code = 0;
  if (NULL == *pPrecalcExprs) {
    code = nodesMakeList(pPrecalcExprs);
    if (NULL == *pPrecalcExprs) {
      return code;
    }
  }
  if (NULL == *pRewrittenList) {
    code = nodesMakeList(pRewrittenList);
    if (NULL == *pRewrittenList) {
      return code;
    }
  }
  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    SNode* pNew = NULL;
    if (QUERY_NODE_GROUPING_SET == nodeType(pNode)) {
      code = nodesCloneNode(nodesListGetNode(((SGroupingSetNode*)pNode)->pParameterList, 0), &pNew);
    } else {
      code = nodesCloneNode(pNode, &pNew);
    }
    if (NULL == pNew) {
      return code;
    }
    if (TSDB_CODE_SUCCESS != (code = nodesListAppend(*pRewrittenList, pNew))) {
      return code;
    }
  }
  SRewritePrecalcExprsCxt cxt = {.errCode = TSDB_CODE_SUCCESS, .pPrecalcExprs = *pPrecalcExprs};
  nodesRewriteExprs(*pRewrittenList, doRewritePrecalcExprs, &cxt);
  if (0 == LIST_LENGTH(cxt.pPrecalcExprs) || TSDB_CODE_SUCCESS != cxt.errCode) {
    NODES_DESTORY_LIST(*pPrecalcExprs);
  }
  return cxt.errCode;
}

static int32_t rewritePrecalcExpr(SPhysiPlanContext* pCxt, SNode* pNode, SNodeList** pPrecalcExprs,
                                  SNode** pRewritten) {
  if (NULL == pNode) {
    return TSDB_CODE_SUCCESS;
  }

  SNodeList* pList = NULL;
  int32_t    code = nodesListMakeAppend(&pList, pNode);
  SNodeList* pRewrittenList = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = rewritePrecalcExprs(pCxt, pList, pPrecalcExprs, &pRewrittenList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    *pRewritten = nodesListGetNode(pRewrittenList, 0);
  }
  nodesClearList(pList);
  nodesClearList(pRewrittenList);
  return code;
}

static EDealRes hasCountLikeFunc(SNode* pNode, void* res) {
  if (QUERY_NODE_FUNCTION == nodeType(pNode)) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    if (fmIsCountLikeFunc(pFunc->funcId) || (pFunc->hasOriginalFunc && fmIsCountLikeFunc(pFunc->originalFuncId))) {
      *(bool*)res = true;
      return DEAL_RES_END;
    }
  }
  return DEAL_RES_CONTINUE;
}

static int32_t createAggPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SAggLogicNode* pAggLogicNode,
                                  SPhysiNode** pPhyNode, SSubplan* pSubPlan) {
  SAggPhysiNode* pAgg =
      (SAggPhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pAggLogicNode, QUERY_NODE_PHYSICAL_PLAN_HASH_AGG);
  if (NULL == pAgg) {
    return terrno;
  }
  if (pAgg->node.pSlimit) {
    pSubPlan->dynamicRowThreshold = true;
    pSubPlan->rowsThreshold = ((SLimitNode*)pAgg->node.pSlimit)->limit;
  }

  pAgg->mergeDataBlock = (GROUP_ACTION_KEEP == pAggLogicNode->node.groupAction ? false : true);
  pAgg->groupKeyOptimized = pAggLogicNode->hasGroupKeyOptimized;
  pAgg->node.forceCreateNonBlockingOptr = pAggLogicNode->node.forceCreateNonBlockingOptr;

  SNodeList* pPrecalcExprs = NULL;
  SNodeList* pGroupKeys = NULL;
  SNodeList* pAggFuncs = NULL;
  int32_t    code = rewritePrecalcExprs(pCxt, pAggLogicNode->pGroupKeys, &pPrecalcExprs, &pGroupKeys);
  if (TSDB_CODE_SUCCESS == code) {
    code = rewritePrecalcExprs(pCxt, pAggLogicNode->pAggFuncs, &pPrecalcExprs, &pAggFuncs);
  }
  nodesWalkExprs(pAggFuncs, hasCountLikeFunc, &pAgg->hasCountLikeFunc);

  SDataBlockDescNode* pChildTupe = (((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc);
  // push down expression to pOutputDataBlockDesc of child node
  if (TSDB_CODE_SUCCESS == code && NULL != pPrecalcExprs) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pPrecalcExprs, &pAgg->pExprs);
    if (TSDB_CODE_SUCCESS == code) {
      code = pushdownDataBlockSlots(pCxt, pAgg->pExprs, pChildTupe);
    }
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pGroupKeys) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pGroupKeys, &pAgg->pGroupKeys);
    if (TSDB_CODE_SUCCESS == code) {
      code = addDataBlockSlots(pCxt, pAgg->pGroupKeys, pAgg->node.pOutputDataBlockDesc);
    }
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pAggFuncs) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pAggFuncs, &pAgg->pAggFuncs);
    if (TSDB_CODE_SUCCESS == code) {
      code = addDataBlockSlots(pCxt, pAgg->pAggFuncs, pAgg->node.pOutputDataBlockDesc);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setConditionsSlotId(pCxt, (const SLogicNode*)pAggLogicNode, (SPhysiNode*)pAgg);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pAgg;
  } else {
    nodesDestroyNode((SNode*)pAgg);
  }

  nodesDestroyList(pPrecalcExprs);
  nodesDestroyList(pGroupKeys);
  nodesDestroyList(pAggFuncs);

  return code;
}

static int32_t createIndefRowsFuncPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren,
                                            SIndefRowsFuncLogicNode* pFuncLogicNode, SPhysiNode** pPhyNode) {
  SIndefRowsFuncPhysiNode* pIdfRowsFunc = (SIndefRowsFuncPhysiNode*)makePhysiNode(
      pCxt, (SLogicNode*)pFuncLogicNode, QUERY_NODE_PHYSICAL_PLAN_INDEF_ROWS_FUNC);
  if (NULL == pIdfRowsFunc) {
    return terrno;
  }

  SNodeList* pPrecalcExprs = NULL;
  SNodeList* pFuncs = NULL;
  int32_t    code = rewritePrecalcExprs(pCxt, pFuncLogicNode->pFuncs, &pPrecalcExprs, &pFuncs);

  if (pIdfRowsFunc->node.inputTsOrder == 0) {
    // default to asc
    pIdfRowsFunc->node.inputTsOrder = TSDB_ORDER_ASC;
  }

  SDataBlockDescNode* pChildTupe = (((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc);
  // push down expression to pOutputDataBlockDesc of child node
  if (TSDB_CODE_SUCCESS == code && NULL != pPrecalcExprs) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pPrecalcExprs, &pIdfRowsFunc->pExprs);
    if (TSDB_CODE_SUCCESS == code) {
      code = pushdownDataBlockSlots(pCxt, pIdfRowsFunc->pExprs, pChildTupe);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pFuncs, &pIdfRowsFunc->pFuncs);
    if (TSDB_CODE_SUCCESS == code) {
      code = addDataBlockSlots(pCxt, pIdfRowsFunc->pFuncs, pIdfRowsFunc->node.pOutputDataBlockDesc);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setConditionsSlotId(pCxt, (const SLogicNode*)pFuncLogicNode, (SPhysiNode*)pIdfRowsFunc);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pIdfRowsFunc;
  } else {
    nodesDestroyNode((SNode*)pIdfRowsFunc);
  }

  nodesDestroyList(pPrecalcExprs);
  nodesDestroyList(pFuncs);

  return code;
}

static int32_t createInterpFuncPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren,
                                         SInterpFuncLogicNode* pFuncLogicNode, SPhysiNode** pPhyNode) {
  SInterpFuncPhysiNode* pInterpFunc =
      (SInterpFuncPhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pFuncLogicNode, QUERY_NODE_PHYSICAL_PLAN_INTERP_FUNC);
  if (NULL == pInterpFunc) {
    return terrno;
  }

  SNodeList* pPrecalcExprs = NULL;
  SNodeList* pFuncs = NULL;
  int32_t    code = rewritePrecalcExprs(pCxt, pFuncLogicNode->pFuncs, &pPrecalcExprs, &pFuncs);

  SDataBlockDescNode* pChildTupe = (((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc);
  // push down expression to pOutputDataBlockDesc of child node
  if (TSDB_CODE_SUCCESS == code && NULL != pPrecalcExprs) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pPrecalcExprs, &pInterpFunc->pExprs);
    if (TSDB_CODE_SUCCESS == code) {
      code = pushdownDataBlockSlots(pCxt, pInterpFunc->pExprs, pChildTupe);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pFuncs, &pInterpFunc->pFuncs);
    if (TSDB_CODE_SUCCESS == code) {
      code = addDataBlockSlots(pCxt, pInterpFunc->pFuncs, pInterpFunc->node.pOutputDataBlockDesc);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    pInterpFunc->timeRange = pFuncLogicNode->timeRange;
    pInterpFunc->interval = pFuncLogicNode->interval;
    pInterpFunc->fillMode = pFuncLogicNode->fillMode;
    pInterpFunc->pFillValues = NULL;
    code = nodesCloneNode(pFuncLogicNode->pFillValues, &pInterpFunc->pFillValues);
    if (TSDB_CODE_SUCCESS != code) {
      code = code;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setNodeSlotId(pCxt, pChildTupe->dataBlockId, -1, pFuncLogicNode->pTimeSeries, &pInterpFunc->pTimeSeries);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setConditionsSlotId(pCxt, (const SLogicNode*)pFuncLogicNode, (SPhysiNode*)pInterpFunc);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pInterpFunc;
  } else {
    nodesDestroyNode((SNode*)pInterpFunc);
  }

  nodesDestroyList(pPrecalcExprs);
  nodesDestroyList(pFuncs);

  return code;
}

static bool projectCanMergeDataBlock(SProjectLogicNode* pProject) {
  if (GROUP_ACTION_KEEP == pProject->node.groupAction) {
    return false;
  }
  if (DATA_ORDER_LEVEL_NONE == pProject->node.resultDataOrder) {
    return true;
  }
  if (1 != LIST_LENGTH(pProject->node.pChildren)) {
    return true;
  }
  SLogicNode* pChild = (SLogicNode*)nodesListGetNode(pProject->node.pChildren, 0);
  return DATA_ORDER_LEVEL_GLOBAL == pChild->resultDataOrder ? true : false;
}

static int32_t createProjectPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren,
                                      SProjectLogicNode* pProjectLogicNode, SPhysiNode** pPhyNode) {
  SProjectPhysiNode* pProject =
      (SProjectPhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pProjectLogicNode, QUERY_NODE_PHYSICAL_PLAN_PROJECT);
  if (NULL == pProject) {
    return terrno;
  }

  pProject->mergeDataBlock = projectCanMergeDataBlock(pProjectLogicNode);
  pProject->ignoreGroupId = pProjectLogicNode->ignoreGroupId;
  pProject->inputIgnoreGroup = pProjectLogicNode->inputIgnoreGroup;

  int32_t code = TSDB_CODE_SUCCESS;
  if (0 == LIST_LENGTH(pChildren)) {
    code = nodesCloneList(pProjectLogicNode->pProjections, &pProject->pProjections);
  } else {
    code = setListSlotId(pCxt, ((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc->dataBlockId, -1,
                         pProjectLogicNode->pProjections, &pProject->pProjections);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = addDataBlockSlotsForProject(pCxt, pProjectLogicNode->stmtName, pProject->pProjections,
                                       pProject->node.pOutputDataBlockDesc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setConditionsSlotId(pCxt, (const SLogicNode*)pProjectLogicNode, (SPhysiNode*)pProject);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pProject;
  } else {
    nodesDestroyNode((SNode*)pProject);
  }

  return code;
}

static int32_t doCreateExchangePhysiNode(SPhysiPlanContext* pCxt, SExchangeLogicNode* pExchangeLogicNode,
                                         SPhysiNode** pPhyNode) {
  SExchangePhysiNode* pExchange =
      (SExchangePhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pExchangeLogicNode, QUERY_NODE_PHYSICAL_PLAN_EXCHANGE);
  if (NULL == pExchange) {
    return terrno;
  }

  pExchange->srcStartGroupId = pExchangeLogicNode->srcStartGroupId;
  pExchange->srcEndGroupId = pExchangeLogicNode->srcEndGroupId;
  pExchange->seqRecvData = pExchangeLogicNode->seqRecvData;

  int32_t code = setConditionsSlotId(pCxt, (const SLogicNode*)pExchangeLogicNode, (SPhysiNode*)pExchange);
  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pExchange;
  } else {
    nodesDestroyNode((SNode*)pExchange);
  }

  return code;
}

static int32_t createStreamScanPhysiNodeByExchange(SPhysiPlanContext* pCxt, SExchangeLogicNode* pExchangeLogicNode,
                                                   SPhysiNode** pPhyNode) {
  SScanPhysiNode* pScan =
      (SScanPhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pExchangeLogicNode, QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN);
  if (NULL == pScan) {
    return terrno;
  }

  int32_t code = TSDB_CODE_SUCCESS;

  pScan->pScanCols = NULL;
  code = nodesCloneList(pExchangeLogicNode->node.pTargets, &pScan->pScanCols);

  if (TSDB_CODE_SUCCESS == code) {
    code = sortScanCols(pScan->pScanCols);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = sortScanCols(pScan->pScanCols);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = addDataBlockSlots(pCxt, pScan->pScanCols, pScan->node.pOutputDataBlockDesc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setConditionsSlotId(pCxt, (const SLogicNode*)pExchangeLogicNode, (SPhysiNode*)pScan);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pScan;
  } else {
    nodesDestroyNode((SNode*)pScan);
  }

  return code;
}

static int32_t createExchangePhysiNode(SPhysiPlanContext* pCxt, SExchangeLogicNode* pExchangeLogicNode,
                                       SPhysiNode** pPhyNode) {
  if (pCxt->pPlanCxt->streamQuery) {
    return createStreamScanPhysiNodeByExchange(pCxt, pExchangeLogicNode, pPhyNode);
  } else {
    return doCreateExchangePhysiNode(pCxt, pExchangeLogicNode, pPhyNode);
  }
}

static int32_t createWindowPhysiNodeFinalize(SPhysiPlanContext* pCxt, SNodeList* pChildren, SWindowPhysiNode* pWindow,
                                             SWindowLogicNode* pWindowLogicNode) {
  pWindow->triggerType = pWindowLogicNode->triggerType;
  pWindow->watermark = pWindowLogicNode->watermark;
  pWindow->deleteMark = pWindowLogicNode->deleteMark;
  pWindow->igExpired = pWindowLogicNode->igExpired;
  if (pCxt->pPlanCxt->streamQuery) {
    pWindow->destHasPrimayKey = pCxt->pPlanCxt->destHasPrimaryKey;
  }
  pWindow->mergeDataBlock = (GROUP_ACTION_KEEP == pWindowLogicNode->node.groupAction ? false : true);
  pWindow->node.inputTsOrder = pWindowLogicNode->node.inputTsOrder;
  pWindow->node.outputTsOrder = pWindowLogicNode->node.outputTsOrder;
  if (nodeType(pWindow) == QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL) {
    pWindow->node.inputTsOrder = pWindowLogicNode->node.outputTsOrder;
  }

  SNodeList* pPrecalcExprs = NULL;
  SNodeList* pFuncs = NULL;
  int32_t    code = rewritePrecalcExprs(pCxt, pWindowLogicNode->pFuncs, &pPrecalcExprs, &pFuncs);

  SDataBlockDescNode* pChildTupe = (((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc);
  // push down expression to pOutputDataBlockDesc of child node
  if (TSDB_CODE_SUCCESS == code && NULL != pPrecalcExprs) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pPrecalcExprs, &pWindow->pExprs);
    if (TSDB_CODE_SUCCESS == code) {
      code = addDataBlockSlots(pCxt, pWindow->pExprs, pChildTupe);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setNodeSlotId(pCxt, pChildTupe->dataBlockId, -1, pWindowLogicNode->pTspk, &pWindow->pTspk);
  }
  if (TSDB_CODE_SUCCESS == code && pWindowLogicNode->pTsEnd) {
    code = setNodeSlotId(pCxt, pChildTupe->dataBlockId, -1, pWindowLogicNode->pTsEnd, &pWindow->pTsEnd);
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pFuncs) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pFuncs, &pWindow->pFuncs);
    if (TSDB_CODE_SUCCESS == code) {
      code = addDataBlockSlots(pCxt, pWindow->pFuncs, pWindow->node.pOutputDataBlockDesc);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setConditionsSlotId(pCxt, (const SLogicNode*)pWindowLogicNode, (SPhysiNode*)pWindow);
  }

  nodesDestroyList(pPrecalcExprs);
  nodesDestroyList(pFuncs);

  return code;
}

static ENodeType getIntervalOperatorType(EWindowAlgorithm windowAlgo) {
  switch (windowAlgo) {
    case INTERVAL_ALGO_HASH:
      return QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL;
    case INTERVAL_ALGO_MERGE:
      return QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL;
    case INTERVAL_ALGO_STREAM_FINAL:
      return QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL;
    case INTERVAL_ALGO_STREAM_SEMI:
      return QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL;
    case INTERVAL_ALGO_STREAM_MID:
      return QUERY_NODE_PHYSICAL_PLAN_STREAM_MID_INTERVAL;
    case INTERVAL_ALGO_STREAM_SINGLE:
      return QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL;
    case SESSION_ALGO_STREAM_FINAL:
      return QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION;
    case SESSION_ALGO_STREAM_SEMI:
      return QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_SESSION;
    case SESSION_ALGO_STREAM_SINGLE:
      return QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION;
    case SESSION_ALGO_MERGE:
      return QUERY_NODE_PHYSICAL_PLAN_MERGE_SESSION;
    default:
      break;
  }
  return QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL;
}

static int32_t createIntervalPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren,
                                       SWindowLogicNode* pWindowLogicNode, SPhysiNode** pPhyNode) {
  SIntervalPhysiNode* pInterval = (SIntervalPhysiNode*)makePhysiNode(
      pCxt, (SLogicNode*)pWindowLogicNode, getIntervalOperatorType(pWindowLogicNode->windowAlgo));
  if (NULL == pInterval) {
    return terrno;
  }

  pInterval->interval = pWindowLogicNode->interval;
  pInterval->offset = pWindowLogicNode->offset;
  pInterval->sliding = pWindowLogicNode->sliding;
  pInterval->intervalUnit = pWindowLogicNode->intervalUnit;
  pInterval->slidingUnit = pWindowLogicNode->slidingUnit;

  int32_t code = createWindowPhysiNodeFinalize(pCxt, pChildren, &pInterval->window, pWindowLogicNode);
  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pInterval;
  } else {
    nodesDestroyNode((SNode*)pInterval);
  }

  return code;
}

static int32_t createSessionWindowPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren,
                                            SWindowLogicNode* pWindowLogicNode, SPhysiNode** pPhyNode) {
  SSessionWinodwPhysiNode* pSession = (SSessionWinodwPhysiNode*)makePhysiNode(
      pCxt, (SLogicNode*)pWindowLogicNode, getIntervalOperatorType(pWindowLogicNode->windowAlgo));
  if (NULL == pSession) {
    return terrno;
  }

  pSession->gap = pWindowLogicNode->sessionGap;

  int32_t code = createWindowPhysiNodeFinalize(pCxt, pChildren, &pSession->window, pWindowLogicNode);
  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pSession;
  } else {
    nodesDestroyNode((SNode*)pSession);
  }

  return code;
}

static int32_t createStateWindowPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren,
                                          SWindowLogicNode* pWindowLogicNode, SPhysiNode** pPhyNode) {
  SStateWinodwPhysiNode* pState = (SStateWinodwPhysiNode*)makePhysiNode(
      pCxt, (SLogicNode*)pWindowLogicNode,
      (pCxt->pPlanCxt->streamQuery ? QUERY_NODE_PHYSICAL_PLAN_STREAM_STATE : QUERY_NODE_PHYSICAL_PLAN_MERGE_STATE));
  if (NULL == pState) {
    return terrno;
  }

  SNodeList* pPrecalcExprs = NULL;
  SNode*     pStateKey = NULL;
  int32_t    code = rewritePrecalcExpr(pCxt, pWindowLogicNode->pStateExpr, &pPrecalcExprs, &pStateKey);

  SDataBlockDescNode* pChildTupe = (((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc);
  // push down expression to pOutputDataBlockDesc of child node
  if (TSDB_CODE_SUCCESS == code && NULL != pPrecalcExprs) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pPrecalcExprs, &pState->window.pExprs);
    if (TSDB_CODE_SUCCESS == code) {
      code = addDataBlockSlots(pCxt, pState->window.pExprs, pChildTupe);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setNodeSlotId(pCxt, pChildTupe->dataBlockId, -1, pStateKey, &pState->pStateKey);
    // if (TSDB_CODE_SUCCESS == code) {
    //   code = addDataBlockSlot(pCxt, &pState->pStateKey, pState->window.node.pOutputDataBlockDesc);
    // }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = createWindowPhysiNodeFinalize(pCxt, pChildren, &pState->window, pWindowLogicNode);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pState;
  } else {
    nodesDestroyNode((SNode*)pState);
  }

  nodesDestroyList(pPrecalcExprs);
  nodesDestroyNode(pStateKey);

  return code;
}

static int32_t createEventWindowPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren,
                                          SWindowLogicNode* pWindowLogicNode, SPhysiNode** pPhyNode) {
  SEventWinodwPhysiNode* pEvent = (SEventWinodwPhysiNode*)makePhysiNode(
      pCxt, (SLogicNode*)pWindowLogicNode,
      (pCxt->pPlanCxt->streamQuery ? QUERY_NODE_PHYSICAL_PLAN_STREAM_EVENT : QUERY_NODE_PHYSICAL_PLAN_MERGE_EVENT));
  if (NULL == pEvent) {
    return terrno;
  }

  SDataBlockDescNode* pChildTupe = (((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc);
  int32_t code = setNodeSlotId(pCxt, pChildTupe->dataBlockId, -1, pWindowLogicNode->pStartCond, &pEvent->pStartCond);
  if (TSDB_CODE_SUCCESS == code) {
    code = setNodeSlotId(pCxt, pChildTupe->dataBlockId, -1, pWindowLogicNode->pEndCond, &pEvent->pEndCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createWindowPhysiNodeFinalize(pCxt, pChildren, &pEvent->window, pWindowLogicNode);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pEvent;
  } else {
    nodesDestroyNode((SNode*)pEvent);
  }

  return code;
}

static int32_t createCountWindowPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren,
                                          SWindowLogicNode* pWindowLogicNode, SPhysiNode** pPhyNode) {
  SCountWinodwPhysiNode* pCount = (SCountWinodwPhysiNode*)makePhysiNode(
      pCxt, (SLogicNode*)pWindowLogicNode,
      (pCxt->pPlanCxt->streamQuery ? QUERY_NODE_PHYSICAL_PLAN_STREAM_COUNT : QUERY_NODE_PHYSICAL_PLAN_MERGE_COUNT));
  if (NULL == pCount) {
    return terrno;
  }
  pCount->windowCount = pWindowLogicNode->windowCount;
  pCount->windowSliding = pWindowLogicNode->windowSliding;

  int32_t  code = createWindowPhysiNodeFinalize(pCxt, pChildren, &pCount->window, pWindowLogicNode);
  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pCount;
  } else {
    nodesDestroyNode((SNode*)pCount);
  }

  return code;
}

static int32_t createWindowPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SWindowLogicNode* pWindowLogicNode,
                                     SPhysiNode** pPhyNode) {
  switch (pWindowLogicNode->winType) {
    case WINDOW_TYPE_INTERVAL:
      return createIntervalPhysiNode(pCxt, pChildren, pWindowLogicNode, pPhyNode);
    case WINDOW_TYPE_SESSION:
      return createSessionWindowPhysiNode(pCxt, pChildren, pWindowLogicNode, pPhyNode);
    case WINDOW_TYPE_STATE:
      return createStateWindowPhysiNode(pCxt, pChildren, pWindowLogicNode, pPhyNode);
    case WINDOW_TYPE_EVENT:
      return createEventWindowPhysiNode(pCxt, pChildren, pWindowLogicNode, pPhyNode);
    case WINDOW_TYPE_COUNT:
      return createCountWindowPhysiNode(pCxt, pChildren, pWindowLogicNode, pPhyNode);
    default:
      break;
  }
  return TSDB_CODE_FAILED;
}

static int32_t createSortPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SSortLogicNode* pSortLogicNode,
                                   SPhysiNode** pPhyNode) {
  SSortPhysiNode* pSort = (SSortPhysiNode*)makePhysiNode(
      pCxt, (SLogicNode*)pSortLogicNode,
      pSortLogicNode->groupSort ? QUERY_NODE_PHYSICAL_PLAN_GROUP_SORT : QUERY_NODE_PHYSICAL_PLAN_SORT);
  if (NULL == pSort) {
    return terrno;
  }

  SNodeList* pPrecalcExprs = NULL;
  SNodeList* pSortKeys = NULL;
  int32_t    code = rewritePrecalcExprs(pCxt, pSortLogicNode->pSortKeys, &pPrecalcExprs, &pSortKeys);
  pSort->calcGroupId = pSortLogicNode->calcGroupId;
  pSort->excludePkCol = pSortLogicNode->excludePkCol;

  SDataBlockDescNode* pChildTupe = (((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc);
  // push down expression to pOutputDataBlockDesc of child node
  if (TSDB_CODE_SUCCESS == code && NULL != pPrecalcExprs) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pPrecalcExprs, &pSort->pExprs);
    if (TSDB_CODE_SUCCESS == code) {
      code = pushdownDataBlockSlots(pCxt, pSort->pExprs, pChildTupe);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pSortKeys, &pSort->pSortKeys);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pSortLogicNode->node.pTargets, &pSort->pTargets);
    if (TSDB_CODE_SUCCESS == code) {
      code = addDataBlockSlots(pCxt, pSort->pTargets, pSort->node.pOutputDataBlockDesc);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setConditionsSlotId(pCxt, (const SLogicNode*)pSortLogicNode, (SPhysiNode*)pSort);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pSort;
  } else {
    nodesDestroyNode((SNode*)pSort);
  }

  nodesDestroyList(pPrecalcExprs);
  nodesDestroyList(pSortKeys);

  return code;
}

static int32_t createPartitionPhysiNodeImpl(SPhysiPlanContext* pCxt, SNodeList* pChildren,
                                            SPartitionLogicNode* pPartLogicNode, ENodeType type,
                                            SPhysiNode** pPhyNode) {
  SPartitionPhysiNode* pPart = (SPartitionPhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pPartLogicNode, type);
  if (NULL == pPart) {
    return terrno;
  }

  SNodeList* pPrecalcExprs = NULL;
  SNodeList* pPartitionKeys = NULL;
  int32_t    code = rewritePrecalcExprs(pCxt, pPartLogicNode->pPartitionKeys, &pPrecalcExprs, &pPartitionKeys);
  pPart->needBlockOutputTsOrder = pPartLogicNode->needBlockOutputTsOrder;

  SDataBlockDescNode* pChildTupe = (((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc);
  // push down expression to pOutputDataBlockDesc of child node
  if (TSDB_CODE_SUCCESS == code && NULL != pPrecalcExprs) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pPrecalcExprs, &pPart->pExprs);
    if (TSDB_CODE_SUCCESS == code) {
      code = pushdownDataBlockSlots(pCxt, pPart->pExprs, pChildTupe);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pPartitionKeys, &pPart->pPartitionKeys);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pPartLogicNode->node.pTargets, &pPart->pTargets);
    if (TSDB_CODE_SUCCESS == code) {
      code = addDataBlockSlots(pCxt, pPart->pTargets, pPart->node.pOutputDataBlockDesc);
    }
  }

  if (pPart->needBlockOutputTsOrder) {
    SNode* node;
    bool found = false;
    FOREACH(node, pPartLogicNode->node.pTargets) {
      if (nodeType(node) == QUERY_NODE_COLUMN) {
        SColumnNode* pCol = (SColumnNode*)node;
        if (pCol->tableId == pPartLogicNode->pkTsColTbId && pCol->colId == pPartLogicNode->pkTsColId) {
          pPart->tsSlotId = pCol->slotId;
          found = true;
          break;
        }
      }
    }
    if (!found) code = TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setConditionsSlotId(pCxt, (const SLogicNode*)pPartLogicNode, (SPhysiNode*)pPart);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pPart;
  } else {
    nodesDestroyNode((SNode*)pPart);
  }

  nodesDestroyList(pPrecalcExprs);
  nodesDestroyList(pPartitionKeys);

  return code;
}

static int32_t createStreamPartitionPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren,
                                              SPartitionLogicNode* pPartLogicNode, SPhysiNode** pPhyNode) {
  SStreamPartitionPhysiNode* pPart = NULL;
  int32_t                    code = createPartitionPhysiNodeImpl(pCxt, pChildren, pPartLogicNode,
                                                                 QUERY_NODE_PHYSICAL_PLAN_STREAM_PARTITION, (SPhysiNode**)&pPart);
  SDataBlockDescNode*        pChildTupe = (((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc);
  if (TSDB_CODE_SUCCESS == code) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pPartLogicNode->pTags, &pPart->pTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setNodeSlotId(pCxt, pChildTupe->dataBlockId, -1, pPartLogicNode->pSubtable, &pPart->pSubtable);
  }
  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pPart;
  } else {
    nodesDestroyNode((SNode*)pPart);
  }
  return code;
}

static int32_t createPartitionPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren,
                                        SPartitionLogicNode* pPartLogicNode, SPhysiNode** pPhyNode) {
  if (pCxt->pPlanCxt->streamQuery) {
    return createStreamPartitionPhysiNode(pCxt, pChildren, pPartLogicNode, pPhyNode);
  }
  return createPartitionPhysiNodeImpl(pCxt, pChildren, pPartLogicNode, QUERY_NODE_PHYSICAL_PLAN_PARTITION, pPhyNode);
}

static int32_t createFillPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SFillLogicNode* pFillNode,
                                   SPhysiNode** pPhyNode) {
  SFillPhysiNode* pFill = (SFillPhysiNode*)makePhysiNode(
      pCxt, (SLogicNode*)pFillNode,
      pCxt->pPlanCxt->streamQuery ? QUERY_NODE_PHYSICAL_PLAN_STREAM_FILL : QUERY_NODE_PHYSICAL_PLAN_FILL);
  if (NULL == pFill) {
    return terrno;
  }

  pFill->mode = pFillNode->mode;
  pFill->timeRange = pFillNode->timeRange;
  pFill->node.inputTsOrder = pFillNode->node.inputTsOrder;

  SDataBlockDescNode* pChildTupe = (((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc);
  int32_t code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pFillNode->pFillExprs, &pFill->pFillExprs);
  if (TSDB_CODE_SUCCESS == code) {
    code = addDataBlockSlots(pCxt, pFill->pFillExprs, pFill->node.pOutputDataBlockDesc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pFillNode->pNotFillExprs, &pFill->pNotFillExprs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = addDataBlockSlots(pCxt, pFill->pNotFillExprs, pFill->node.pOutputDataBlockDesc);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setNodeSlotId(pCxt, pChildTupe->dataBlockId, -1, pFillNode->pWStartTs, &pFill->pWStartTs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = addDataBlockSlot(pCxt, &pFill->pWStartTs, pFill->node.pOutputDataBlockDesc);
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pFillNode->pValues) {
    code = nodesCloneNode(pFillNode->pValues, &pFill->pValues);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setConditionsSlotId(pCxt, (const SLogicNode*)pFillNode, (SPhysiNode*)pFill);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pFill;
  } else {
    nodesDestroyNode((SNode*)pFill);
  }

  return code;
}

static int32_t createExchangePhysiNodeByMerge(SMergePhysiNode* pMerge, int32_t idx) {
  SExchangePhysiNode* pExchange = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_EXCHANGE, (SNode**)&pExchange);
  if (NULL == pExchange) {
    return code;
  }
  pExchange->srcStartGroupId = pMerge->srcGroupId + idx;
  pExchange->srcEndGroupId = pMerge->srcGroupId + idx;
  pExchange->singleChannel = true;
  pExchange->node.pParent = (SPhysiNode*)pMerge;
  pExchange->node.pOutputDataBlockDesc = NULL;
  code = nodesCloneNode((SNode*)pMerge->node.pOutputDataBlockDesc, (SNode**)&pExchange->node.pOutputDataBlockDesc);
  if (NULL == pExchange->node.pOutputDataBlockDesc) {
    nodesDestroyNode((SNode*)pExchange);
    return code;
  }
  SNode* pSlot = NULL;
  FOREACH(pSlot, pExchange->node.pOutputDataBlockDesc->pSlots) { ((SSlotDescNode*)pSlot)->output = true; }
  return nodesListMakeStrictAppend(&pMerge->node.pChildren, (SNode*)pExchange);
}

static int32_t createMergePhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SMergeLogicNode* pMergeLogicNode, SPhysiNode** pPhyNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  SMergePhysiNode* pMerge =
      (SMergePhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pMergeLogicNode, QUERY_NODE_PHYSICAL_PLAN_MERGE);
  if (NULL == pMerge) {
    return terrno;
  }

  if (pMergeLogicNode->colsMerge) {
    pMerge->type = MERGE_TYPE_COLUMNS;
  } else if (pMergeLogicNode->needSort) {
    pMerge->type = MERGE_TYPE_SORT;
  } else {
    pMerge->type = MERGE_TYPE_NON_SORT;
  }
  
  pMerge->numOfChannels = pMergeLogicNode->numOfChannels;
  pMerge->srcGroupId = pMergeLogicNode->srcGroupId;
  pMerge->srcEndGroupId = pMergeLogicNode->srcEndGroupId;
  pMerge->groupSort = pMergeLogicNode->groupSort;
  pMerge->ignoreGroupId = pMergeLogicNode->ignoreGroupId;
  pMerge->inputWithGroupId = pMergeLogicNode->inputWithGroupId;

  if (!pMergeLogicNode->colsMerge) {
    code = addDataBlockSlots(pCxt, pMergeLogicNode->pInputs, pMerge->node.pOutputDataBlockDesc);

    if (TSDB_CODE_SUCCESS == code) {
      for (int32_t j = 0; j < pMergeLogicNode->numOfSubplans; ++j) {
        for (int32_t i = 0; i < pMerge->numOfChannels; ++i) {
          code = createExchangePhysiNodeByMerge(pMerge, j);
          if (TSDB_CODE_SUCCESS != code) {
            break;
          }
        }
        if (code) break;
      }
    }

    if (TSDB_CODE_SUCCESS == code && NULL != pMergeLogicNode->pMergeKeys) {
      code = setListSlotId(pCxt, pMerge->node.pOutputDataBlockDesc->dataBlockId, -1, pMergeLogicNode->pMergeKeys,
                           &pMerge->pMergeKeys);
    }

    if (TSDB_CODE_SUCCESS == code) {
      code = setListSlotId(pCxt, pMerge->node.pOutputDataBlockDesc->dataBlockId, -1, pMergeLogicNode->node.pTargets,
                           &pMerge->pTargets);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = addDataBlockSlots(pCxt, pMerge->pTargets, pMerge->node.pOutputDataBlockDesc);
    }
  } else {
    SDataBlockDescNode* pLeftDesc = ((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc;
    SDataBlockDescNode* pRightDesc = ((SPhysiNode*)nodesListGetNode(pChildren, 1))->pOutputDataBlockDesc;

    code = setListSlotId(pCxt, pLeftDesc->dataBlockId, pRightDesc->dataBlockId, pMergeLogicNode->node.pTargets, &pMerge->pTargets);
    if (TSDB_CODE_SUCCESS == code) {
      code = addDataBlockSlots(pCxt, pMerge->pTargets, pMerge->node.pOutputDataBlockDesc);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pMerge;
  } else {
    nodesDestroyNode((SNode*)pMerge);
  }

  return code;
}

static int32_t doCreatePhysiNode(SPhysiPlanContext* pCxt, SLogicNode* pLogicNode, SSubplan* pSubplan,
                                 SNodeList* pChildren, SPhysiNode** pPhyNode) {
  switch (nodeType(pLogicNode)) {
    case QUERY_NODE_LOGIC_PLAN_SCAN:
      return createScanPhysiNode(pCxt, pSubplan, (SScanLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_JOIN:
      return createJoinPhysiNode(pCxt, pChildren, (SJoinLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_AGG:
      return createAggPhysiNode(pCxt, pChildren, (SAggLogicNode*)pLogicNode, pPhyNode, pSubplan);
    case QUERY_NODE_LOGIC_PLAN_PROJECT:
      return createProjectPhysiNode(pCxt, pChildren, (SProjectLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_EXCHANGE:
      return createExchangePhysiNode(pCxt, (SExchangeLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_WINDOW:
      return createWindowPhysiNode(pCxt, pChildren, (SWindowLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_SORT:
      return createSortPhysiNode(pCxt, pChildren, (SSortLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_PARTITION:
      return createPartitionPhysiNode(pCxt, pChildren, (SPartitionLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_FILL:
      return createFillPhysiNode(pCxt, pChildren, (SFillLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC:
      return createIndefRowsFuncPhysiNode(pCxt, pChildren, (SIndefRowsFuncLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_INTERP_FUNC:
      return createInterpFuncPhysiNode(pCxt, pChildren, (SInterpFuncLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_MERGE:
      return createMergePhysiNode(pCxt, pChildren, (SMergeLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_GROUP_CACHE:
      return createGroupCachePhysiNode(pCxt, pChildren, (SGroupCacheLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL:
      return createDynQueryCtrlPhysiNode(pCxt, pChildren, (SDynQueryCtrlLogicNode*)pLogicNode, pPhyNode);
    default:
      break;
  }

  return TSDB_CODE_FAILED;
}

static int32_t createPhysiNode(SPhysiPlanContext* pCxt, SLogicNode* pLogicNode, SSubplan* pSubplan,
                               SPhysiNode** pPhyNode) {
  SNodeList* pChildren = NULL;
  int32_t code = nodesMakeList(&pChildren);
  if (NULL == pChildren) {
    return code;
  }

  SNode* pLogicChild;
  FOREACH(pLogicChild, pLogicNode->pChildren) {
    SPhysiNode* pChild = NULL;
    code = createPhysiNode(pCxt, (SLogicNode*)pLogicChild, pSubplan, &pChild);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListStrictAppend(pChildren, (SNode*)pChild);
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = doCreatePhysiNode(pCxt, pLogicNode, pSubplan, pChildren, pPhyNode);
  }

  if (TSDB_CODE_SUCCESS == code) {
    if (LIST_LENGTH(pChildren) > 0) {
      (*pPhyNode)->pChildren = pChildren;
      SNode* pChild;
      FOREACH(pChild, (*pPhyNode)->pChildren) { ((SPhysiNode*)pChild)->pParent = (*pPhyNode); }
    } else {
      nodesDestroyList(pChildren);
    }
  } else {
    nodesDestroyList(pChildren);
  }

  return code;
}

static int32_t createDataInserter(SPhysiPlanContext* pCxt, SVgDataBlocks* pBlocks, SDataSinkNode** pSink) {
  SDataInserterNode* pInserter = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_INSERT, (SNode**)&pInserter);
  if (NULL == pInserter) {
    return code;
  }

  pInserter->numOfTables = pBlocks->numOfTables;
  pInserter->size = pBlocks->size;
  TSWAP(pInserter->pData, pBlocks->pData);

  *pSink = (SDataSinkNode*)pInserter;
  return TSDB_CODE_SUCCESS;
}

static int32_t createDataDispatcher(SPhysiPlanContext* pCxt, const SPhysiNode* pRoot, SDataSinkNode** pSink) {
  SDataDispatcherNode* pDispatcher = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_DISPATCH, (SNode**)&pDispatcher);
  if (NULL == pDispatcher) {
    return code;
  }

  pDispatcher->sink.pInputDataBlockDesc = NULL;
  code = nodesCloneNode((SNode*)pRoot->pOutputDataBlockDesc, (SNode**)&pDispatcher->sink.pInputDataBlockDesc);
  if (NULL == pDispatcher->sink.pInputDataBlockDesc) {
    nodesDestroyNode((SNode*)pDispatcher);
    return code;
  }

  *pSink = (SDataSinkNode*)pDispatcher;
  return TSDB_CODE_SUCCESS;
}

static int32_t makeSubplan(SPhysiPlanContext* pCxt, SLogicSubplan* pLogicSubplan, SSubplan** ppSubplan) {
  SSubplan* pSubplan = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, (SNode**)&pSubplan);
  if (NULL == pSubplan) {
    return code;
  }
  pSubplan->id = pLogicSubplan->id;
  pSubplan->subplanType = pLogicSubplan->subplanType;
  pSubplan->level = pLogicSubplan->level;
  pSubplan->rowsThreshold = 4096;
  pSubplan->dynamicRowThreshold = false;
  pSubplan->isView = pCxt->pPlanCxt->isView;
  pSubplan->isAudit = pCxt->pPlanCxt->isAudit;
  if (NULL != pCxt->pPlanCxt->pUser) {
    snprintf(pSubplan->user, sizeof(pSubplan->user), "%s", pCxt->pPlanCxt->pUser);
  }
  *ppSubplan = pSubplan;
  return code;
}

static int32_t buildInsertValuesSubplan(SPhysiPlanContext* pCxt, SVnodeModifyLogicNode* pModify, SSubplan* pSubplan) {
  pSubplan->msgType = pModify->msgType;
  pSubplan->execNode.nodeId = pModify->pVgDataBlocks->vg.vgId;
  pSubplan->execNode.epSet = pModify->pVgDataBlocks->vg.epSet;
  return createDataInserter(pCxt, pModify->pVgDataBlocks, &pSubplan->pDataSink);
}

static int32_t createQueryInserter(SPhysiPlanContext* pCxt, SVnodeModifyLogicNode* pModify, SSubplan* pSubplan,
                                   SDataSinkNode** pSink) {
  SQueryInserterNode* pInserter = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_QUERY_INSERT, (SNode**)&pInserter);
  if (NULL == pInserter) {
    return code;
  }

  pInserter->tableId = pModify->tableId;
  pInserter->stableId = pModify->stableId;
  pInserter->tableType = pModify->tableType;
  strcpy(pInserter->tableName, pModify->tableName);
  pInserter->explain = (QUERY_NODE_EXPLAIN_STMT == nodeType(pCxt->pPlanCxt->pAstRoot) ? true : false);
  if (pModify->pVgroupList) {
    pInserter->vgId = pModify->pVgroupList->vgroups[0].vgId;
    pInserter->epSet = pModify->pVgroupList->vgroups[0].epSet;
    vgroupInfoToNodeAddr(pModify->pVgroupList->vgroups, &pSubplan->execNode);
  }
  code = setListSlotId(pCxt, pSubplan->pNode->pOutputDataBlockDesc->dataBlockId, -1, pModify->pInsertCols,
                               &pInserter->pCols);
  if (TSDB_CODE_SUCCESS == code) {
    pInserter->sink.pInputDataBlockDesc = NULL;
    code = nodesCloneNode((SNode*)pSubplan->pNode->pOutputDataBlockDesc, (SNode**)&pInserter->sink.pInputDataBlockDesc);
    if (NULL == pInserter->sink.pInputDataBlockDesc) {
      code = code;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pSink = (SDataSinkNode*)pInserter;
  } else {
    nodesDestroyNode((SNode*)pInserter);
  }

  return code;
}

static int32_t buildInsertSelectSubplan(SPhysiPlanContext* pCxt, SVnodeModifyLogicNode* pModify, SSubplan* pSubplan) {
  int32_t code =
      createPhysiNode(pCxt, (SLogicNode*)nodesListGetNode(pModify->node.pChildren, 0), pSubplan, &pSubplan->pNode);
  if (TSDB_CODE_SUCCESS == code) {
    code = createQueryInserter(pCxt, pModify, pSubplan, &pSubplan->pDataSink);
  }
  pSubplan->msgType = TDMT_SCH_MERGE_QUERY;
  return code;
}

static int32_t buildInsertSubplan(SPhysiPlanContext* pCxt, SVnodeModifyLogicNode* pModify, SSubplan* pSubplan) {
  if (NULL == pModify->node.pChildren) {
    return buildInsertValuesSubplan(pCxt, pModify, pSubplan);
  }
  return buildInsertSelectSubplan(pCxt, pModify, pSubplan);
}

static int32_t createDataDeleter(SPhysiPlanContext* pCxt, SVnodeModifyLogicNode* pModify, const SPhysiNode* pRoot,
                                 SDataSinkNode** pSink) {
  SDataDeleterNode* pDeleter = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_DELETE, (SNode**)&pDeleter);
  if (NULL == pDeleter) {
    return code;
  }

  pDeleter->tableId = pModify->tableId;
  pDeleter->tableType = pModify->tableType;
  strcpy(pDeleter->tableFName, pModify->tableName);
  strcpy(pDeleter->tsColName, pModify->tsColName);
  pDeleter->deleteTimeRange = pModify->deleteTimeRange;

  code = setNodeSlotId(pCxt, pRoot->pOutputDataBlockDesc->dataBlockId, -1, pModify->pAffectedRows,
                               &pDeleter->pAffectedRows);
  if (TSDB_CODE_SUCCESS == code) {
    code = setNodeSlotId(pCxt, pRoot->pOutputDataBlockDesc->dataBlockId, -1, pModify->pStartTs, &pDeleter->pStartTs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setNodeSlotId(pCxt, pRoot->pOutputDataBlockDesc->dataBlockId, -1, pModify->pEndTs, &pDeleter->pEndTs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pDeleter->sink.pInputDataBlockDesc = NULL;
    code = nodesCloneNode((SNode*)pRoot->pOutputDataBlockDesc, (SNode**)&pDeleter->sink.pInputDataBlockDesc);
    if (NULL == pDeleter->sink.pInputDataBlockDesc) {
      code = code;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pSink = (SDataSinkNode*)pDeleter;
  } else {
    nodesDestroyNode((SNode*)pDeleter);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t buildDeleteSubplan(SPhysiPlanContext* pCxt, SVnodeModifyLogicNode* pModify, SSubplan* pSubplan) {
  int32_t code =
      createPhysiNode(pCxt, (SLogicNode*)nodesListGetNode(pModify->node.pChildren, 0), pSubplan, &pSubplan->pNode);
  if (TSDB_CODE_SUCCESS == code) {
    code = createDataDeleter(pCxt, pModify, pSubplan->pNode, &pSubplan->pDataSink);
  }
  pSubplan->msgType = TDMT_VND_DELETE;
  return code;
}

static int32_t buildVnodeModifySubplan(SPhysiPlanContext* pCxt, SLogicSubplan* pLogicSubplan, SSubplan* pSubplan) {
  int32_t                code = TSDB_CODE_SUCCESS;
  SVnodeModifyLogicNode* pModify = (SVnodeModifyLogicNode*)pLogicSubplan->pNode;
  switch (pModify->modifyType) {
    case MODIFY_TABLE_TYPE_INSERT:
      code = buildInsertSubplan(pCxt, pModify, pSubplan);
      break;
    case MODIFY_TABLE_TYPE_DELETE:
      code = buildDeleteSubplan(pCxt, pModify, pSubplan);
      break;
    default:
      code = TSDB_CODE_FAILED;
      break;
  }
  return code;
}

static int32_t createPhysiSubplan(SPhysiPlanContext* pCxt, SLogicSubplan* pLogicSubplan, SSubplan** pPhysiSubplan) {
  SSubplan* pSubplan = NULL;
  int32_t code = makeSubplan(pCxt, pLogicSubplan, &pSubplan);
  if (NULL == pSubplan) {
    return code;
  }

  if (SUBPLAN_TYPE_MODIFY == pLogicSubplan->subplanType) {
    code = buildVnodeModifySubplan(pCxt, pLogicSubplan, pSubplan);
  } else {
    if (SUBPLAN_TYPE_SCAN == pSubplan->subplanType) {
      pSubplan->msgType = TDMT_SCH_QUERY;
    } else {
      pSubplan->msgType = TDMT_SCH_MERGE_QUERY;
    }
    code = createPhysiNode(pCxt, pLogicSubplan->pNode, pSubplan, &pSubplan->pNode);
    if (TSDB_CODE_SUCCESS == code && !pCxt->pPlanCxt->streamQuery && !pCxt->pPlanCxt->topicQuery) {
      code = createDataDispatcher(pCxt, pSubplan->pNode, &pSubplan->pDataSink);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhysiSubplan = pSubplan;
  } else {
    nodesDestroyNode((SNode*)pSubplan);
  }

  return code;
}

static int32_t makeQueryPhysiPlan(SPhysiPlanContext* pCxt, SQueryPlan** ppQueryPlan) {
  SQueryPlan* pPlan = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN, (SNode**)&pPlan);
  if (NULL == pPlan) {
    return code;
  }
  pPlan->pSubplans = NULL;
  code = nodesMakeList(&pPlan->pSubplans);
  if (NULL == pPlan->pSubplans) {
    nodesDestroyNode((SNode*)pPlan);
    return code;
  }
  pPlan->queryId = pCxt->pPlanCxt->queryId;
  *ppQueryPlan = pPlan;
  return code;
}

static int32_t pushSubplan(SPhysiPlanContext* pCxt, SNode* pSubplan, int32_t level, SNodeList* pSubplans) {
  SNodeListNode* pGroup = NULL;
  if (level >= LIST_LENGTH(pSubplans)) {
    pGroup = NULL;
    int32_t code = nodesMakeNode(QUERY_NODE_NODE_LIST, (SNode**)&pGroup);
    if (NULL == pGroup) {
      return code;
    }
    if (TSDB_CODE_SUCCESS != nodesListStrictAppend(pSubplans, (SNode*)pGroup)) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  } else {
    pGroup = (SNodeListNode*)nodesListGetNode(pSubplans, level);
  }
  if (NULL == pGroup->pNodeList) {
    int32_t code = nodesMakeList(&pGroup->pNodeList);
    if (NULL == pGroup->pNodeList) {
      return code;
    }
  }
  return nodesListAppend(pGroup->pNodeList, (SNode*)pSubplan);
}

static int32_t buildPhysiPlan(SPhysiPlanContext* pCxt, SLogicSubplan* pLogicSubplan, SSubplan* pParent,
                              SQueryPlan* pQueryPlan) {
  SSubplan* pSubplan = NULL;
  int32_t   code = createPhysiSubplan(pCxt, pLogicSubplan, &pSubplan);

  if (TSDB_CODE_SUCCESS == code) {
    code = pushSubplan(pCxt, (SNode*)pSubplan, pLogicSubplan->level, pQueryPlan->pSubplans);
    ++(pQueryPlan->numOfSubplans);
  }

  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pSubplan);
    return code;
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pParent) {
    code = nodesListMakeAppend(&pParent->pChildren, (SNode*)pSubplan);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListMakeAppend(&pSubplan->pParents, (SNode*)pParent);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    SNode* pChild = NULL;
    FOREACH(pChild, pLogicSubplan->pChildren) {
      code = buildPhysiPlan(pCxt, (SLogicSubplan*)pChild, pSubplan, pQueryPlan);
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    }
  }

  return code;
}

static int32_t doCreatePhysiPlan(SPhysiPlanContext* pCxt, SQueryLogicPlan* pLogicPlan, SQueryPlan** pPhysiPlan) {
  SQueryPlan* pPlan = NULL;
  int32_t code = makeQueryPhysiPlan(pCxt, &pPlan);
  if (NULL == pPlan) {
    return code;
  }

  SNode* pSubplan = NULL;
  FOREACH(pSubplan, pLogicPlan->pTopSubplans) {
    code = buildPhysiPlan(pCxt, (SLogicSubplan*)pSubplan, NULL, pPlan);
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhysiPlan = pPlan;
  } else {
    nodesDestroyNode((SNode*)pPlan);
  }

  return code;
}

static void destoryLocationHash(void* p) {
  SHashObj*   pHash = *(SHashObj**)p;
  SSlotIndex* pIndex = taosHashIterate(pHash, NULL);
  while (NULL != pIndex) {
    taosArrayDestroy(pIndex->pSlotIdsInfo);
    pIndex = taosHashIterate(pHash, pIndex);
  }
  taosHashCleanup(pHash);
}

static void destoryPhysiPlanContext(SPhysiPlanContext* pCxt) {
  taosArrayDestroyEx(pCxt->pLocationHelper, destoryLocationHash);
}

static void setExplainInfo(SPlanContext* pCxt, SQueryPlan* pPlan) {
  if (QUERY_NODE_EXPLAIN_STMT == nodeType(pCxt->pAstRoot)) {
    SExplainStmt* pStmt = (SExplainStmt*)pCxt->pAstRoot;
    pPlan->explainInfo.mode = pStmt->analyze ? EXPLAIN_MODE_ANALYZE : EXPLAIN_MODE_STATIC;
    pPlan->explainInfo.verbose = pStmt->pOptions->verbose;
    pPlan->explainInfo.ratio = pStmt->pOptions->ratio;
  } else {
    pPlan->explainInfo.mode = EXPLAIN_MODE_DISABLE;
  }
}

static int32_t setExecNodeList(SPhysiPlanContext* pCxt, SArray* pExecNodeList) {
  int32_t code = 0;
  if (NULL == pExecNodeList) {
    return code;
  }
  if (pCxt->hasSysScan || !pCxt->hasScan) {
    SQueryNodeLoad node = {.addr = {.nodeId = MNODE_HANDLE, .epSet = pCxt->pPlanCxt->mgmtEpSet}, .load = 0};
    if (NULL == taosArrayPush(pExecNodeList, &node))
      code = TSDB_CODE_OUT_OF_MEMORY;
  }
  return code;
}

int32_t createPhysiPlan(SPlanContext* pCxt, SQueryLogicPlan* pLogicPlan, SQueryPlan** pPlan, SArray* pExecNodeList) {
  SPhysiPlanContext cxt = {.pPlanCxt = pCxt,
                           .errCode = TSDB_CODE_SUCCESS,
                           .nextDataBlockId = 0,
                           .pLocationHelper = taosArrayInit(32, POINTER_BYTES),
                           .hasScan = false,
                           .hasSysScan = false};
  if (NULL == cxt.pLocationHelper) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = doCreatePhysiPlan(&cxt, pLogicPlan, pPlan);
  if (TSDB_CODE_SUCCESS == code) {
    setExplainInfo(pCxt, *pPlan);
    code = setExecNodeList(&cxt, pExecNodeList);
  }

  destoryPhysiPlanContext(&cxt);
  return code;
}
