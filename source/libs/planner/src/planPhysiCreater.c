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

#include <stdio.h>
#include <stdlib.h>

#include "extConnector.h"
#include "nodes.h"
#include "planInt.h"

#include "catalog.h"
#include "functionMgt.h"
#include "planner.h"
#include "scalar.h"
#include "plannodes.h"
#include "systable.h"
#include "tglobal.h"
#include "ttypes.h"

typedef struct SSlotIdInfo {
  int16_t slotId;
  bool    set;
} SSlotIdInfo;

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

typedef struct SSlotIndex {
  int64_t dataBlockId;
  SArray*  pSlotIdsInfo;  // duplicate name slot
} SSlotIndex;

enum {
  SLOT_KEY_TYPE_ALL = 1,
  SLOT_KEY_TYPE_COLNAME = 2,
  SLOT_KEY_TYPE_REF = 3,
};

static bool fqRemotePlanHasRepeatScanAgg(const SLogicNode* pNode) {
  if (NULL == pNode) {
    return false;
  }

  if (QUERY_NODE_LOGIC_PLAN_AGG == nodeType(pNode)) {
    const SAggLogicNode* pAgg = (const SAggLogicNode*)pNode;
    const SNode*         pFunc = NULL;
    FOREACH(pFunc, pAgg->pAggFuncs) {
      if (QUERY_NODE_FUNCTION == nodeType(pFunc) &&
          fmIsRepeatScanFunc(((const SFunctionNode*)pFunc)->funcId)) {
        return true;
      }
    }
  }

  if (NULL == pNode->pChildren) {
    return false;
  }

  const SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    if (fqRemotePlanHasRepeatScanAgg((const SLogicNode*)pChild)) {
      return true;
    }
  }

  return false;
}

static bool fqExprHasAnalyzeInfluxScalarAgg(const SNode* pNode, bool underAgg) {
  if (NULL == pNode) {
    return false;
  }

  switch (nodeType(pNode)) {
    case QUERY_NODE_TARGET:
      return fqExprHasAnalyzeInfluxScalarAgg((const SNode*)((const STargetNode*)pNode)->pExpr, underAgg);
    case QUERY_NODE_REMOTE_VALUE:
      return underAgg;
    case QUERY_NODE_OPERATOR: {
      const SOperatorNode* pOp = (const SOperatorNode*)pNode;
      return fqExprHasAnalyzeInfluxScalarAgg(pOp->pLeft, underAgg) ||
             fqExprHasAnalyzeInfluxScalarAgg(pOp->pRight, underAgg);
    }
    case QUERY_NODE_LOGIC_CONDITION: {
      const SLogicConditionNode* pCond = (const SLogicConditionNode*)pNode;
      const SNode*               pChild = NULL;
      FOREACH(pChild, pCond->pParameterList) {
        if (fqExprHasAnalyzeInfluxScalarAgg(pChild, underAgg)) {
          return true;
        }
      }
      return false;
    }
    case QUERY_NODE_CASE_WHEN: {
      const SCaseWhenNode* pCase = (const SCaseWhenNode*)pNode;
      if (fqExprHasAnalyzeInfluxScalarAgg(pCase->pCase, underAgg) ||
          fqExprHasAnalyzeInfluxScalarAgg(pCase->pElse, underAgg)) {
        return true;
      }

      const SNode* pWhenThen = NULL;
      FOREACH(pWhenThen, pCase->pWhenThenList) {
        const SWhenThenNode* pItem = (const SWhenThenNode*)pWhenThen;
        if (fqExprHasAnalyzeInfluxScalarAgg(pItem->pWhen, underAgg) ||
            fqExprHasAnalyzeInfluxScalarAgg(pItem->pThen, underAgg)) {
          return true;
        }
      }
      return false;
    }
    case QUERY_NODE_FUNCTION: {
      const SFunctionNode* pFunc = (const SFunctionNode*)pNode;
      bool                 nextUnderAgg = underAgg || fmIsAggFunc(pFunc->funcId);
      const SNode*         pArg = NULL;
      FOREACH(pArg, pFunc->pParameterList) {
        if (fqExprHasAnalyzeInfluxScalarAgg(pArg, nextUnderAgg)) {
          return true;
        }
      }
      return false;
    }
    case QUERY_NODE_NODE_LIST: {
      const SNodeListNode* pList = (const SNodeListNode*)pNode;
      const SNode*         pItem = NULL;
      FOREACH(pItem, pList->pNodeList) {
        if (fqExprHasAnalyzeInfluxScalarAgg(pItem, underAgg)) {
          return true;
        }
      }
      return false;
    }
    default:
      return false;
  }
}

static bool fqRemoteLogicNeedsAnalyzeInfluxScalarAggFallback(const SLogicNode* pNode) {
  if (NULL == pNode) {
    return false;
  }

  if (fqExprHasAnalyzeInfluxScalarAgg((const SNode*)pNode->pConditions, false)) {
    return true;
  }

  const SNode* pTarget = NULL;
  FOREACH(pTarget, pNode->pTargets) {
    if (fqExprHasAnalyzeInfluxScalarAgg(pTarget, false)) {
      return true;
    }
  }

  switch (nodeType((const SNode*)pNode)) {
    case QUERY_NODE_LOGIC_PLAN_AGG: {
      const SAggLogicNode* pAgg = (const SAggLogicNode*)pNode;
      const SNode*         pExpr = NULL;
      FOREACH(pExpr, pAgg->pAggFuncs) {
        if (fqExprHasAnalyzeInfluxScalarAgg(pExpr, false)) {
          return true;
        }
      }
      FOREACH(pExpr, pAgg->pGroupKeys) {
        if (fqExprHasAnalyzeInfluxScalarAgg(pExpr, false)) {
          return true;
        }
      }
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_PROJECT: {
      const SProjectLogicNode* pProj = (const SProjectLogicNode*)pNode;
      const SNode*             pExpr = NULL;
      FOREACH(pExpr, pProj->pProjections) {
        if (fqExprHasAnalyzeInfluxScalarAgg(pExpr, false)) {
          return true;
        }
      }
      break;
    }
    default:
      break;
  }

  const SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    if (fqRemoteLogicNeedsAnalyzeInfluxScalarAggFallback((const SLogicNode*)pChild)) {
      return true;
    }
  }

  return false;
}

static bool fqShouldUseAnalyzeInfluxScalarAggFallback(const SPhysiPlanContext* pCxt,
                                                      const SScanLogicNode* pScanLogicNode) {
  if (NULL == pCxt || NULL == pCxt->pPlanCxt || NULL == pScanLogicNode ||
      NULL == pScanLogicNode->pExtTableNode || NULL == pScanLogicNode->pRemoteLogicPlan) {
    return false;
  }

  if (((const SExtTableNode*)pScanLogicNode->pExtTableNode)->sourceType != EXT_SOURCE_INFLUXDB) {
    return false;
  }

  if (QUERY_NODE_EXPLAIN_STMT != nodeType(pCxt->pPlanCxt->pAstRoot)) {
    return false;
  }

  if (!((const SExplainStmt*)pCxt->pPlanCxt->pAstRoot)->analyze) {
    return false;
  }

  return fqRemoteLogicNeedsAnalyzeInfluxScalarAggFallback((const SLogicNode*)pScanLogicNode->pRemoteLogicPlan);
}

static int32_t addDataBlockSlotsImpl(SPhysiPlanContext* pCxt, SNodeList* pList, SDataBlockDescNode* pDataBlockDesc,
                                     const char* pStmtName, bool output, bool reserve);

static int32_t getSlotKeyHelper(SNode* pNode, const char* pPreName, const char* name, char** ppKey, int32_t callocLen,
                                int32_t* pLen, uint16_t extraBufLen, int8_t slotKeyType) {
  int32_t code = 0;
  *ppKey = taosMemoryCalloc(1, callocLen);
  if (!*ppKey) {
    return terrno;
  }
  if (slotKeyType == SLOT_KEY_TYPE_ALL) {
    TAOS_STRNCAT(*ppKey, pPreName, TSDB_TABLE_NAME_LEN);
    TAOS_STRNCAT(*ppKey, ".", 2);
    TAOS_STRNCAT(*ppKey, name, TSDB_COL_NAME_LEN);
    *pLen = taosHashBinary(*ppKey, strlen(*ppKey), callocLen);
  } else {
    TAOS_STRNCAT(*ppKey, name, TSDB_COL_NAME_LEN);
    *pLen = strlen(*ppKey);
  }
  return code;
}

static int32_t getSlotKey(SNode* pNode, const char* pStmtName, char** ppKey, int32_t* pLen, uint16_t extraBufLen,
                          int32_t* cap) {
  int32_t code = 0;
  int32_t callocLen = 0;
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    if (NULL != pStmtName) {
      if ('\0' != pStmtName[0]) {
        callocLen = TSDB_TABLE_NAME_LEN + 1 + TSDB_COL_NAME_LEN + 1 + extraBufLen;
        *cap = callocLen;
        return getSlotKeyHelper(pNode, pStmtName, pCol->node.aliasName, ppKey, callocLen, pLen, extraBufLen,
                                SLOT_KEY_TYPE_ALL);
      } else {
        callocLen = TSDB_COL_NAME_LEN + 1 + extraBufLen;
        *cap = callocLen;
        return getSlotKeyHelper(pNode, pStmtName, pCol->node.aliasName, ppKey, callocLen, pLen, extraBufLen,
                                SLOT_KEY_TYPE_COLNAME);
      }
    }
    if ('\0' == pCol->tableAlias[0]) {
      callocLen = TSDB_COL_NAME_LEN + 1 + extraBufLen;
      *cap = callocLen;
      return getSlotKeyHelper(pNode, pStmtName, pCol->colName, ppKey, callocLen, pLen, extraBufLen,
                              SLOT_KEY_TYPE_COLNAME);
    }
    if (pCol->hasRef) {
      int32_t allocLen = TSDB_EXT_SOURCE_NAME_LEN + TSDB_TABLE_FNAME_LEN + TSDB_COL_NAME_LEN * 2 + 5 + extraBufLen;
      *ppKey = taosMemoryCalloc(1, allocLen);
      if (!*ppKey) {
        return terrno;
      }
      // For tag columns, include colName to prevent slot collisions when STB-level
      // metadata non-deterministically reports the same ref source for different tags
      if (pCol->colType == COLUMN_TYPE_TAG) {
        TAOS_STRNCAT(*ppKey, pCol->colName, TSDB_COL_NAME_LEN);
        TAOS_STRNCAT(*ppKey, ":", 2);
      }
      if ('\0' != pCol->refSourceName[0]) {
        TAOS_STRNCAT(*ppKey, pCol->refSourceName, TSDB_EXT_SOURCE_NAME_LEN);
        TAOS_STRNCAT(*ppKey, ".", 2);
      }
      TAOS_STRNCAT(*ppKey, pCol->refDbName, TSDB_DB_NAME_LEN);
      TAOS_STRNCAT(*ppKey, ".", 2);
      TAOS_STRNCAT(*ppKey, pCol->refTableName, TSDB_TABLE_NAME_LEN);
      TAOS_STRNCAT(*ppKey, ".", 2);
      TAOS_STRNCAT(*ppKey, pCol->refColName, TSDB_COL_NAME_LEN);
      *pLen = taosHashBinary(*ppKey, strlen(*ppKey), allocLen);
      *cap = allocLen;
      return code;
    }
    if (pCol->hasDep) {
      int32_t allocLen = TSDB_EXT_SOURCE_NAME_LEN + TSDB_TABLE_FNAME_LEN + TSDB_COL_NAME_LEN + 4 + extraBufLen;
      *ppKey = taosMemoryCalloc(1, allocLen);
      if (!*ppKey) {
        return terrno;
      }
      if ('\0' != pCol->refSourceName[0]) {
        TAOS_STRNCAT(*ppKey, pCol->refSourceName, TSDB_EXT_SOURCE_NAME_LEN);
        TAOS_STRNCAT(*ppKey, ".", 2);
      }
      TAOS_STRNCAT(*ppKey, pCol->dbName, TSDB_DB_NAME_LEN);
      TAOS_STRNCAT(*ppKey, ".", 2);
      TAOS_STRNCAT(*ppKey, pCol->tableAlias, TSDB_TABLE_NAME_LEN);
      TAOS_STRNCAT(*ppKey, ".", 2);
      TAOS_STRNCAT(*ppKey, pCol->colName, TSDB_COL_NAME_LEN);
      planDebug("getSlotKey hasDep: '%s' (len=%d)", *ppKey, (int)strlen(*ppKey));
      *cap = allocLen;
      *pLen = taosHashBinary(*ppKey, strlen(*ppKey), allocLen);
      return code;
    }
    callocLen = TSDB_TABLE_NAME_LEN + 1 + TSDB_COL_NAME_LEN + 1 + extraBufLen;
    *cap = callocLen;
    planDebug("getSlotKey default: tableAlias='%s' colName='%s' hasRef=%d hasDep=%d", pCol->tableAlias, pCol->colName, pCol->hasRef, pCol->hasDep);
    return getSlotKeyHelper(pNode, pCol->tableAlias, pCol->colName, ppKey, callocLen, pLen, extraBufLen,
                            SLOT_KEY_TYPE_ALL);
  } else if (QUERY_NODE_FUNCTION == nodeType(pNode)) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    if (FUNCTION_TYPE_TBNAME == pFunc->funcType) {
      SValueNode* pVal = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 0);
      if (pVal) {
        if (NULL != pStmtName && '\0' != pStmtName[0]) {
          callocLen = TSDB_TABLE_NAME_LEN + 1 + TSDB_COL_NAME_LEN + 1 + extraBufLen;
          return getSlotKeyHelper(pNode, pStmtName, ((SExprNode*)pNode)->aliasName, ppKey, callocLen, pLen, extraBufLen,
                                  SLOT_KEY_TYPE_ALL);
        }
        int32_t literalLen = strlen(pVal->literal);
        callocLen = literalLen + 1 + TSDB_COL_NAME_LEN + 1 + extraBufLen;
        *cap = callocLen;
        return getSlotKeyHelper(pNode, pVal->literal, ((SExprNode*)pNode)->aliasName, ppKey, callocLen, pLen,
                                extraBufLen, SLOT_KEY_TYPE_ALL);
      }
    }
  }

  if (NULL != pStmtName && '\0' != pStmtName[0]) {
    callocLen = TSDB_TABLE_NAME_LEN + 1 + TSDB_COL_NAME_LEN + 1 + extraBufLen;
    *cap = callocLen;
    return getSlotKeyHelper(pNode, pStmtName, ((SExprNode*)pNode)->aliasName, ppKey, callocLen, pLen, extraBufLen,
                            SLOT_KEY_TYPE_ALL);
  }

  callocLen = TSDB_COL_NAME_LEN + 1 + extraBufLen;
  *cap = callocLen;
  code = getSlotKeyHelper(pNode, pStmtName, ((SExprNode*)pNode)->aliasName, ppKey, callocLen, pLen, extraBufLen,
                          SLOT_KEY_TYPE_COLNAME);
  if (TSDB_CODE_SUCCESS == code) {
    qDebug("FQ-Slots: getSlotKey exprType=%d stmtName=%s aliasName=%s key=%s len=%d cap=%d",
           (int)nodeType(pNode), (NULL != pStmtName ? pStmtName : "(null)"),
           ((SExprNode*)pNode)->aliasName, (NULL != *ppKey ? *ppKey : "(null)"), *pLen, *cap);
  } else {
    planError("FQ-Slots: getSlotKey failed exprType=%d stmtName=%s aliasName=%s code=0x%x",
              (int)nodeType(pNode), (NULL != pStmtName ? pStmtName : "(null)"),
              ((SExprNode*)pNode)->aliasName, code);
  }
  return code;
  return code;
}

static SNode* createSlotDesc(SPhysiPlanContext* pCxt, const char* pName, const SNode* pNode, int16_t slotId,
                             bool output, bool reserve) {
  SSlotDescNode* pSlot = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_SLOT_DESC, (SNode**)&pSlot);
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

static int32_t createTarget(SNode* pNode, int64_t dataBlockId, int16_t slotId, SNode** pOutput) {
  STargetNode* pTarget = NULL;
  int32_t      code = nodesMakeNode(QUERY_NODE_TARGET, (SNode**)&pTarget);
  if (NULL == pTarget) {
    return code;
  }

  pTarget->dataBlockId = dataBlockId;
  pTarget->slotId = slotId;
  pTarget->pExpr = pNode;

  *pOutput = (SNode*)pTarget;
  return TSDB_CODE_SUCCESS;
}

static int32_t putSlotToHashImpl(int64_t dataBlockId, int16_t slotId, const char* pName, int32_t len, SHashObj* pHash) {
  SSlotIndex* pIndex = taosHashGet(pHash, pName, len);
  if (NULL != pIndex) {
    SSlotIdInfo info = {.slotId = slotId, .set = false};
    if (NULL == taosArrayPush(pIndex->pSlotIdsInfo, &info)) {
      return terrno;
    }
    return TSDB_CODE_SUCCESS;
  }

  SSlotIndex index = {.dataBlockId = dataBlockId, .pSlotIdsInfo = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SSlotIdInfo))};
  if (NULL == index.pSlotIdsInfo) {
    return terrno;
  }
  SSlotIdInfo info = {.slotId = slotId, .set = false};
  if (NULL == taosArrayPush(index.pSlotIdsInfo, &info)) {
    taosArrayDestroy(index.pSlotIdsInfo);
    return terrno;
  }
  int32_t code = taosHashPut(pHash, pName, len, &index, sizeof(SSlotIndex));
  if (TSDB_CODE_SUCCESS != code) {
    taosArrayDestroy(index.pSlotIdsInfo);
  }
  return code;
}

static int32_t putSlotToHash(const char* pName, int32_t len, int64_t dataBlockId, int16_t slotId, SNode* pNode,
                             SHashObj* pHash) {
  return putSlotToHashImpl(dataBlockId, slotId, pName, len, pHash);
}

static int32_t createDataBlockDescHash(SPhysiPlanContext* pCxt, int32_t capacity, int64_t dataBlockId,
                                       SHashObj** pDescHash, SHashObj** ppProjIdxDescHash) {
  SHashObj* pHash = taosHashInit(capacity, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (NULL == pHash) {
    return terrno;
  }
  SHashObj* pProjIdxHash = taosHashInit(capacity, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (!pProjIdxHash) {
    taosHashCleanup(pHash);
    return terrno;
  }
  if (NULL == taosArrayInsert(pCxt->pLocationHelper, (size_t)dataBlockId, &pHash)) {
    taosHashCleanup(pHash);
    taosHashCleanup(pProjIdxHash);
    return terrno;
  }
  if (NULL == taosArrayInsert(pCxt->pProjIdxLocHelper, (size_t)dataBlockId, &pProjIdxHash)) {
    taosHashCleanup(pHash);
    taosHashCleanup(pProjIdxHash);
    return terrno;
  }

  *pDescHash = pHash;
  *ppProjIdxDescHash = pProjIdxHash;
  return TSDB_CODE_SUCCESS;
}

static void fqResetLocationHashSlotSet(SHashObj* pHash) {
  if (NULL == pHash) {
    return;
  }

  SSlotIndex* pIndex = taosHashIterate(pHash, NULL);
  while (NULL != pIndex) {
    if (NULL != pIndex->pSlotIdsInfo) {
      int32_t size = taosArrayGetSize(pIndex->pSlotIdsInfo);
      for (int32_t i = 0; i < size; ++i) {
        SSlotIdInfo* pInfo = taosArrayGet(pIndex->pSlotIdsInfo, i);
        if (NULL != pInfo) {
          pInfo->set = false;
        }
      }
    }
    pIndex = taosHashIterate(pHash, pIndex);
  }
}

static int32_t buildDataBlockSlots(SPhysiPlanContext* pCxt, SNodeList* pList, SDataBlockDescNode* pDataBlockDesc,
                                   SHashObj* pHash, SHashObj* pProjIdxDescHash) {
  pDataBlockDesc->pSlots = NULL;
  int32_t code = nodesMakeList(&pDataBlockDesc->pSlots);
  if (NULL == pDataBlockDesc->pSlots) {
    return code;
  }

  int16_t slotId = 0;
  SNode*  pNode = NULL;
  FOREACH(pNode, pList) {
    char*   name = NULL;
    int32_t len = 0;
    int32_t cap = 0;
    code = getSlotKey(pNode, NULL, &name, &len, 16, &cap);
    planDebug("FQ-Slots: buildDataBlockSlots dataBlockId=%" PRId64 " slotId=%d exprNodeType=%d key=%s len=%d code=0x%x",
              pDataBlockDesc->dataBlockId, slotId, (int)nodeType(pNode),
              (NULL != name ? name : "(null)"), len, code);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListStrictAppend(pDataBlockDesc->pSlots, createSlotDesc(pCxt, name, pNode, slotId, true, false));
    }
    code = putSlotToHash(name, len, pDataBlockDesc->dataBlockId, slotId, pNode, pHash);
    if (TSDB_CODE_SUCCESS == code) {
      if (nodeType(pNode) == QUERY_NODE_COLUMN && ((SColumnNode*)pNode)->resIdx > 0) {
        snprintf(name + strlen(name), cap, "_%d", ((SColumnNode*)pNode)->resIdx);
        code = putSlotToHash(name, strlen(name), pDataBlockDesc->dataBlockId, slotId, pNode, pProjIdxDescHash);
      }
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
  int32_t             code = nodesMakeNode(QUERY_NODE_DATABLOCK_DESC, (SNode**)&pDesc);
  if (NULL == pDesc) {
    return code;
  }
  pDesc->dataBlockId = pCxt->nextDataBlockId++;
  planDebug("FQ-Slots: createDataBlockDesc start dataBlockId=%" PRId64 " targets=%d",
            pDesc->dataBlockId, pList ? LIST_LENGTH(pList) : 0);

  SHashObj* pHash = NULL;
  SHashObj* pProjIdxHash = NULL;
  code = createDataBlockDescHash(pCxt, LIST_LENGTH(pList), pDesc->dataBlockId, &pHash, &pProjIdxHash);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildDataBlockSlots(pCxt, pList, pDesc, pHash, pProjIdxHash);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pDataBlockDesc = pDesc;
    planDebug("FQ-Slots: createDataBlockDesc done dataBlockId=%" PRId64 " slots=%d totalRowSize=%d outputRowSize=%d",
              pDesc->dataBlockId, pDesc->pSlots ? LIST_LENGTH(pDesc->pSlots) : 0,
              pDesc->totalRowSize, pDesc->outputRowSize);
  } else {
    planError("FQ-Slots: createDataBlockDesc failed dataBlockId=%" PRId64 " targets=%d code=0x%x",
              pDesc->dataBlockId, pList ? LIST_LENGTH(pList) : 0, code);
    nodesDestroyNode((SNode*)pDesc);
  }

  return code;
}

static void destroySlotIndexHash(SHashObj* pHash) {
  if (NULL == pHash) {
    return;
  }

  SSlotIndex* pIndex = taosHashIterate(pHash, NULL);
  while (NULL != pIndex) {
    taosArrayDestroy(pIndex->pSlotIdsInfo);
    pIndex = taosHashIterate(pHash, pIndex);
  }
  taosHashCleanup(pHash);
}

static void fqClearDataBlockDescHashes(SPhysiPlanContext* pCxt, int64_t dataBlockId) {
  if (NULL == pCxt) {
    return;
  }

  if (NULL != pCxt->pLocationHelper && dataBlockId < taosArrayGetSize(pCxt->pLocationHelper)) {
    SHashObj* pHash = taosArrayGetP(pCxt->pLocationHelper, (size_t)dataBlockId);
    if (NULL != pHash) {
      SHashObj* pNull = NULL;
      destroySlotIndexHash(pHash);
      taosArraySet(pCxt->pLocationHelper, (size_t)dataBlockId, &pNull);
    }
  }

  if (NULL != pCxt->pProjIdxLocHelper && dataBlockId < taosArrayGetSize(pCxt->pProjIdxLocHelper)) {
    SHashObj* pHash = taosArrayGetP(pCxt->pProjIdxLocHelper, (size_t)dataBlockId);
    if (NULL != pHash) {
      SHashObj* pNull = NULL;
      destroySlotIndexHash(pHash);
      taosArraySet(pCxt->pProjIdxLocHelper, (size_t)dataBlockId, &pNull);
    }
  }
}

static int32_t fqRebuildDataBlockDesc(SPhysiPlanContext* pCxt, SDataBlockDescNode* pDesc, SNodeList* pOutputCols,
                                      SNodeList* pReserveCols) {
  if (NULL == pCxt || NULL == pDesc) {
    return TSDB_CODE_INVALID_PARA;
  }

  int64_t dataBlockId = pDesc->dataBlockId;
  planDebug("FQ-Slots: fqRebuildDataBlockDesc start dataBlockId=%" PRId64 " outputCols=%d reserveCols=%d",
            dataBlockId, pOutputCols ? LIST_LENGTH(pOutputCols) : 0,
            pReserveCols ? LIST_LENGTH(pReserveCols) : 0);
  int32_t cap = 0;
  if (pOutputCols != NULL) {
    cap += LIST_LENGTH(pOutputCols);
  }
  if (pReserveCols != NULL) {
    cap += LIST_LENGTH(pReserveCols);
  }
  if (cap <= 0) {
    cap = 1;
  }

  nodesDestroyList(pDesc->pSlots);
  pDesc->pSlots = NULL;
  pDesc->totalRowSize = 0;
  pDesc->outputRowSize = 0;

  fqClearDataBlockDescHashes(pCxt, dataBlockId);

  SHashObj* pHash = NULL;
  SHashObj* pProjIdxHash = NULL;
  int32_t code = createDataBlockDescHash(pCxt, cap, dataBlockId, &pHash, &pProjIdxHash);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  code = nodesMakeList(&pDesc->pSlots);
  if (TSDB_CODE_SUCCESS != code) {
    fqClearDataBlockDescHashes(pCxt, dataBlockId);
    return code;
  }

  if (pOutputCols != NULL) {
    code = addDataBlockSlotsImpl(pCxt, pOutputCols, pDesc, NULL, true, false);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  if (pReserveCols != NULL) {
    code = addDataBlockSlotsImpl(pCxt, pReserveCols, pDesc, NULL, false, true);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  planDebug("FQ-Slots: rebuilt dataBlockDesc=%" PRId64 " outputCols=%d reserveCols=%d totalSlots=%d totalRowSize=%d outputRowSize=%d",
            dataBlockId,
            pOutputCols ? LIST_LENGTH(pOutputCols) : 0,
            pReserveCols ? LIST_LENGTH(pReserveCols) : 0,
            pDesc->pSlots ? LIST_LENGTH(pDesc->pSlots) : 0,
            pDesc->totalRowSize, pDesc->outputRowSize);
  return TSDB_CODE_SUCCESS;
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
    SNode*  pExpr = QUERY_NODE_ORDER_BY_EXPR == nodeType(pNode) ? ((SOrderByExprNode*)pNode)->pExpr : pNode;
    char*   name = NULL;
    int32_t len = 0;
    int32_t cap = 0;
    code = getSlotKey(pExpr, pStmtName, &name, &len, 0, &cap);
    planDebug("FQ-Slots: addDataBlockSlots dataBlockId=%" PRId64 " nextSlotId=%d output=%d reserve=%d exprNodeType=%d exprAlias=%s key=%s len=%d code=0x%x",
              pDataBlockDesc->dataBlockId, nextSlotId, (int)output, (int)reserve,
              (int)nodeType(pExpr), ((SExprNode*)pExpr)->aliasName,
              (NULL != name ? name : "(null)"), len, code);
    if (TSDB_CODE_SUCCESS == code) {
      SSlotIndex* pIndex = taosHashGet(pHash, name, len);
      if (NULL == pIndex) {
        code = nodesListStrictAppend(pDataBlockDesc->pSlots,
                                     createSlotDesc(pCxt, name, pExpr, nextSlotId, output, reserve));
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
  return addDataBlockSlotsImpl(pCxt, pList, pDataBlockDesc, NULL, false, true);
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
  return addDataBlockSlotsImpl(pCxt, pList, pDataBlockDesc, pStmtName, false, true);
}

static int32_t pushdownDataBlockSlots(SPhysiPlanContext* pCxt, SNodeList* pList, SDataBlockDescNode* pDataBlockDesc) {
  return addDataBlockSlotsImpl(pCxt, pList, pDataBlockDesc, NULL, true, true);
}

typedef struct SSetSlotIdCxt {
  int32_t     errCode;
  SHashObj*   pLeftHash;
  SHashObj*   pLeftProjIdxHash;
  SHashObj*   pRightHash;
  SHashObj*   pRightProdIdxHash;
  const char* pLeftTableAlias;
  const char* pRightTableAlias;
  const char* pLeftAlias;
  const char* pRightAlias;
} SSetSlotIdCxt;

typedef struct SMultiTableSetSlotIdCxt {
  int32_t    errCode;
  SArray*    hashArray;
  SArray*    projIdxHashArray;
  SHashObj*  mergedHash;
  SHashObj*  mergedProjIdxHash;
  SNodeList* pChild;
  bool       isVtb;
} SMultiTableSetSlotIdCxt;

static int32_t fqBuildUnqualifiedSlotKey(const char* pName, uint16_t extraBufLen, char** ppKey,
                                         int32_t* pLen, int32_t* pCap) {
  if (NULL == pName || '\0' == pName[0] || NULL == ppKey || NULL == pLen || NULL == pCap) {
    return TSDB_CODE_INVALID_PARA;
  }

  *ppKey = NULL;
  *pLen = 0;
  *pCap = TSDB_COL_NAME_LEN + 1 + extraBufLen;
  return getSlotKeyHelper(NULL, "", pName, ppKey, *pCap, pLen, extraBufLen, SLOT_KEY_TYPE_COLNAME);
}

static int32_t fqBuildQualifiedSlotKey(const char* pTableName, const char* pColName, uint16_t extraBufLen,
                                       char** ppKey, int32_t* pLen, int32_t* pCap) {
  if (NULL == pTableName || '\0' == pTableName[0] || NULL == pColName || '\0' == pColName[0] ||
      NULL == ppKey || NULL == pLen || NULL == pCap) {
    return TSDB_CODE_INVALID_PARA;
  }

  *ppKey = NULL;
  *pLen = 0;
  *pCap = TSDB_TABLE_NAME_LEN + 1 + TSDB_COL_NAME_LEN + 1 + extraBufLen;
  return getSlotKeyHelper(NULL, pTableName, pColName, ppKey, *pCap, pLen, extraBufLen, SLOT_KEY_TYPE_ALL);
}

static int32_t fqBuildRefSlotKey(const SColumnNode* pCol, uint16_t extraBufLen, char** ppKey, int32_t* pLen,
                                 int32_t* pCap) {
  if (NULL == pCol || NULL == ppKey || NULL == pLen || NULL == pCap || !pCol->hasRef ||
      '\0' == pCol->refDbName[0] || '\0' == pCol->refTableName[0] || '\0' == pCol->refColName[0]) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t allocLen = TSDB_EXT_SOURCE_NAME_LEN + TSDB_TABLE_FNAME_LEN + TSDB_COL_NAME_LEN * 2 + 5 + extraBufLen;
  *ppKey = taosMemoryCalloc(1, allocLen);
  if (NULL == *ppKey) {
    return terrno;
  }

  if (COLUMN_TYPE_TAG == pCol->colType) {
    TAOS_STRNCAT(*ppKey, pCol->colName, TSDB_COL_NAME_LEN);
    TAOS_STRNCAT(*ppKey, ":", 2);
  }
  if ('\0' != pCol->refSourceName[0]) {
    TAOS_STRNCAT(*ppKey, pCol->refSourceName, TSDB_EXT_SOURCE_NAME_LEN);
    TAOS_STRNCAT(*ppKey, ".", 2);
  }
  TAOS_STRNCAT(*ppKey, pCol->refDbName, TSDB_DB_NAME_LEN);
  TAOS_STRNCAT(*ppKey, ".", 2);
  TAOS_STRNCAT(*ppKey, pCol->refTableName, TSDB_TABLE_NAME_LEN);
  TAOS_STRNCAT(*ppKey, ".", 2);
  TAOS_STRNCAT(*ppKey, pCol->refColName, TSDB_COL_NAME_LEN);
  *pLen = taosHashBinary(*ppKey, strlen(*ppKey), allocLen);
  *pCap = allocLen;
  return TSDB_CODE_SUCCESS;
}

static bool fqSlotLookupHasHit(SHashObj* pHash, const char* pKey, int32_t keyLen) {
  return (NULL != pHash && NULL != pKey && keyLen > 0 && NULL != taosHashGet(pHash, pKey, keyLen));
}

static void fqAppendSlotDiagItem(char* pBuf, int32_t bufLen, int32_t* pPos, const char* pItem) {
  if (NULL == pBuf || bufLen <= 0 || NULL == pPos || NULL == pItem || *pPos >= bufLen - 1) {
    return;
  }

  int32_t remain = bufLen - *pPos;
  int32_t written = snprintf(pBuf + *pPos, remain, "%s%s", (*pPos > 0 ? "; " : ""), pItem);
  if (written < 0) {
    return;
  }

  if (written >= remain) {
    *pPos = bufLen - 1;
    pBuf[bufLen - 1] = '\0';
    return;
  }

  *pPos += written;
}

static void fqAppendSlotKeyAttempt(char* pBuf, int32_t bufLen, int32_t* pPos, const char* pLabel, const char* pKey,
                                   bool hit1, bool hit2, const char* pHash1Label, const char* pHash2Label) {
  if (NULL == pLabel || NULL == pKey || '\0' == pKey[0]) {
    return;
  }

  char item[256] = {0};
  if (NULL != pHash2Label) {
    (void)snprintf(item, sizeof(item), "%s=%s[%s=%d,%s=%d]", pLabel, pKey, pHash1Label, (int)hit1, pHash2Label,
                   (int)hit2);
  } else {
    (void)snprintf(item, sizeof(item), "%s=%s[%s=%d]", pLabel, pKey, pHash1Label, (int)hit1);
  }
  fqAppendSlotDiagItem(pBuf, bufLen, pPos, item);
}

static bool fqSlotKeyMatchesFocus(const char* pKey, const char* pFocus) {
  return (NULL != pKey && NULL != pFocus && '\0' != pFocus[0] && NULL != strstr(pKey, pFocus));
}

static void fqBuildSlotHashSummary(SHashObj* pHash, const char* pFocus1, const char* pFocus2, const char* pFocus3,
                                   char* pBuf, int32_t bufLen) {
  if (NULL == pBuf || bufLen <= 0) {
    return;
  }

  pBuf[0] = '\0';
  if (NULL == pHash) {
    (void)snprintf(pBuf, bufLen, "size=0 hash=(nil)");
    return;
  }

  char    focusBuf[512] = {0};
  char    sampleBuf[512] = {0};
  int32_t focusPos = 0;
  int32_t samplePos = 0;
  int32_t focusCnt = 0;
  int32_t sampleCnt = 0;
  int32_t iterCnt = 0;
  void*   pIter = taosHashIterate(pHash, NULL);
  while (NULL != pIter && iterCnt < 64) {
    size_t      keyLen = 0;
    const char* pKey = taosHashGetKey(pIter, &keyLen);
    if (NULL != pKey && '\0' != pKey[0]) {
      if (sampleCnt < 8) {
        fqAppendSlotDiagItem(sampleBuf, sizeof(sampleBuf), &samplePos, pKey);
        ++sampleCnt;
      }
      if (focusCnt < 6 &&
          (fqSlotKeyMatchesFocus(pKey, pFocus1) || fqSlotKeyMatchesFocus(pKey, pFocus2) ||
           fqSlotKeyMatchesFocus(pKey, pFocus3))) {
        fqAppendSlotDiagItem(focusBuf, sizeof(focusBuf), &focusPos, pKey);
        ++focusCnt;
      }
    }
    ++iterCnt;
    pIter = taosHashIterate(pHash, pIter);
  }

  (void)snprintf(pBuf, bufLen, "size=%d iter=%d focus=[%s] sample=[%s]", (int32_t)taosHashGetSize(pHash), iterCnt,
                 ('\0' != focusBuf[0] ? focusBuf : "-"), ('\0' != sampleBuf[0] ? sampleBuf : "-"));
}

static void fqLogSlotLookupMiss(const char* pTag, const SColumnNode* pCol, const char* pRawKey, const char* pProjKey,
                                SHashObj* pHash1, const char* pHash1Label, SHashObj* pHash2,
                                const char* pHash2Label, SHashObj* pProjHash1, const char* pProjHash1Label,
                                SHashObj* pProjHash2, const char* pProjHash2Label) {
  if (NULL == pTag || NULL == pCol) {
    return;
  }

  char attempts[1024] = {0};
  int32_t attemptsPos = 0;

  fqAppendSlotKeyAttempt(attempts, sizeof(attempts), &attemptsPos, "raw", pRawKey,
                         fqSlotLookupHasHit(pHash1, pRawKey, strlen(pRawKey)),
                         fqSlotLookupHasHit(pHash2, pRawKey, strlen(pRawKey)), pHash1Label, pHash2Label);

  if (NULL != pProjKey && '\0' != pProjKey[0]) {
    fqAppendSlotKeyAttempt(attempts, sizeof(attempts), &attemptsPos, "projRef", pProjKey,
                           fqSlotLookupHasHit(pProjHash1, pProjKey, strlen(pProjKey)),
                           fqSlotLookupHasHit(pProjHash2, pProjKey, strlen(pProjKey)), pProjHash1Label,
                           pProjHash2Label);
  }

  char*   pKey = NULL;
  int32_t keyLen = 0;
  int32_t keyCap = 0;
  int32_t code = fqBuildUnqualifiedSlotKey(pCol->colName, 64, &pKey, &keyLen, &keyCap);
  if (TSDB_CODE_SUCCESS == code) {
    fqAppendSlotKeyAttempt(attempts, sizeof(attempts), &attemptsPos, "plainCol", pKey,
                           fqSlotLookupHasHit(pHash1, pKey, keyLen), fqSlotLookupHasHit(pHash2, pKey, keyLen),
                           pHash1Label, pHash2Label);
  }
  taosMemoryFree(pKey);

  pKey = NULL;
  keyLen = 0;
  keyCap = 0;
  if ('\0' != pCol->node.aliasName[0]) {
    code = fqBuildUnqualifiedSlotKey(pCol->node.aliasName, 64, &pKey, &keyLen, &keyCap);
    if (TSDB_CODE_SUCCESS == code) {
      fqAppendSlotKeyAttempt(attempts, sizeof(attempts), &attemptsPos, "alias", pKey,
                             fqSlotLookupHasHit(pHash1, pKey, keyLen), fqSlotLookupHasHit(pHash2, pKey, keyLen),
                             pHash1Label, pHash2Label);
    }
    taosMemoryFree(pKey);
  }

  pKey = NULL;
  keyLen = 0;
  keyCap = 0;
  if ('\0' != pCol->node.userAlias[0]) {
    code = fqBuildUnqualifiedSlotKey(pCol->node.userAlias, 64, &pKey, &keyLen, &keyCap);
    if (TSDB_CODE_SUCCESS == code) {
      fqAppendSlotKeyAttempt(attempts, sizeof(attempts), &attemptsPos, "userAlias", pKey,
                             fqSlotLookupHasHit(pHash1, pKey, keyLen), fqSlotLookupHasHit(pHash2, pKey, keyLen),
                             pHash1Label, pHash2Label);
    }
    taosMemoryFree(pKey);
  }

  pKey = NULL;
  keyLen = 0;
  keyCap = 0;
  if ('\0' != pCol->tableAlias[0]) {
    code = fqBuildQualifiedSlotKey(pCol->tableAlias, pCol->colName, 64, &pKey, &keyLen, &keyCap);
    if (TSDB_CODE_SUCCESS == code) {
      fqAppendSlotKeyAttempt(attempts, sizeof(attempts), &attemptsPos, "tableAlias.col", pKey,
                             fqSlotLookupHasHit(pHash1, pKey, keyLen), fqSlotLookupHasHit(pHash2, pKey, keyLen),
                             pHash1Label, pHash2Label);
    }
    taosMemoryFree(pKey);
  }

  pKey = NULL;
  keyLen = 0;
  keyCap = 0;
  if (pCol->hasRef) {
    code = fqBuildRefSlotKey(pCol, 64, &pKey, &keyLen, &keyCap);
    if (TSDB_CODE_SUCCESS == code) {
      fqAppendSlotKeyAttempt(attempts, sizeof(attempts), &attemptsPos, "refKey", pKey,
                             fqSlotLookupHasHit(pHash1, pKey, keyLen), fqSlotLookupHasHit(pHash2, pKey, keyLen),
                             pHash1Label, pHash2Label);
    }
    taosMemoryFree(pKey);
  }

  char hash1Summary[640] = {0};
  char hash2Summary[640] = {0};
  char projHash1Summary[640] = {0};
  char projHash2Summary[640] = {0};
  fqBuildSlotHashSummary(pHash1, pCol->colName, pCol->node.aliasName, pCol->node.userAlias, hash1Summary,
                         sizeof(hash1Summary));
  fqBuildSlotHashSummary(pHash2, pCol->colName, pCol->node.aliasName, pCol->node.userAlias, hash2Summary,
                         sizeof(hash2Summary));
  fqBuildSlotHashSummary(pProjHash1, pCol->colName, pCol->node.aliasName, pCol->node.userAlias, projHash1Summary,
                         sizeof(projHash1Summary));
  fqBuildSlotHashSummary(pProjHash2, pCol->colName, pCol->node.aliasName, pCol->node.userAlias, projHash2Summary,
                         sizeof(projHash2Summary));

  planDebug("%s: attempts=%s", pTag, attempts);
  planDebug("%s: normalHash %s={%s} %s={%s}", pTag, pHash1Label, hash1Summary,
            (NULL != pHash2Label ? pHash2Label : "hash2"), hash2Summary);
  planDebug("%s: projHash %s={%s} %s={%s}", pTag, pProjHash1Label, projHash1Summary,
            (NULL != pProjHash2Label ? pProjHash2Label : "projHash2"), projHash2Summary);
}

static EDealRes doSetSlotId(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode) && 0 != strcmp(((SColumnNode*)pNode)->colName, "*")) {
    SSetSlotIdCxt* pCxt = (SSetSlotIdCxt*)pContext;
    SColumnNode*   pCol = (SColumnNode*)pNode;
    bool preferLeft = NULL != pCxt->pLeftTableAlias && '\0' != pCxt->pLeftTableAlias[0] &&
                      0 == strcmp(pCol->tableAlias, pCxt->pLeftTableAlias);
    bool preferRight = !preferLeft && NULL != pCxt->pRightTableAlias && '\0' != pCxt->pRightTableAlias[0] &&
                       0 == strcmp(pCol->tableAlias, pCxt->pRightTableAlias);
    SHashObj* pFirstHash = preferRight ? pCxt->pRightHash : pCxt->pLeftHash;
    SHashObj* pSecondHash = preferRight ? pCxt->pLeftHash : pCxt->pRightHash;
    SHashObj* pFirstProjIdxHash = preferRight ? pCxt->pRightProdIdxHash : pCxt->pLeftProjIdxHash;
    SHashObj* pSecondProjIdxHash = preferRight ? pCxt->pLeftProjIdxHash : pCxt->pRightProdIdxHash;
    char*          name = NULL;
    char           rawLookupKey[TSDB_TABLE_FNAME_LEN + TSDB_COL_NAME_LEN + 96] = {0};
    char           projLookupKey[TSDB_TABLE_FNAME_LEN + TSDB_COL_NAME_LEN + 96] = {0};
    int32_t        len = 0;
    int32_t        cap = 0;
    pCxt->errCode = getSlotKey(pNode, NULL, &name, &len, 64, &cap);
    if (TSDB_CODE_SUCCESS != pCxt->errCode) {
      return DEAL_RES_ERROR;
    }
    if (NULL != name) {
      tstrncpy(rawLookupKey, name, sizeof(rawLookupKey));
    }
    SSlotIndex* pIndex = NULL;
    if (((SColumnNode*)pNode)->projRefIdx > 0) {
      snprintf(name + strlen(name), cap, "_%d", ((SColumnNode*)pNode)->projRefIdx);
      tstrncpy(projLookupKey, name, sizeof(projLookupKey));
      pIndex = taosHashGet(pFirstProjIdxHash, name, strlen(name));
      if (!pIndex) {
        pIndex = taosHashGet(pSecondProjIdxHash, name, strlen(name));
      }
    }
    if (NULL == pIndex) {
      SSlotIndex* pFullIdx = taosHashGet(pFirstHash, name, len);
      if (NULL == pFullIdx) {
        pFullIdx = taosHashGet(pSecondHash, name, len);
      }
      if (NULL != pFullIdx) {
        pIndex = pFullIdx;
      }
      if (NULL == pIndex && false == pCol->hasRef && false == pCol->hasDep) {
        int32_t colLen = (int32_t)strlen(pCol->colName);
        if (colLen > 0) {
          pIndex = taosHashGet(pFirstHash, pCol->colName, colLen);
          if (NULL == pIndex) {
            pIndex = taosHashGet(pSecondHash, pCol->colName, colLen);
          }
        }

        if (NULL == pIndex && ((SExprNode*)pCol)->aliasName[0] != '\0') {
          int32_t aliasLen = (int32_t)strlen(((SExprNode*)pCol)->aliasName);
          pIndex = taosHashGet(pFirstHash, ((SExprNode*)pCol)->aliasName, aliasLen);
          if (NULL == pIndex) {
            pIndex = taosHashGet(pSecondHash, ((SExprNode*)pCol)->aliasName, aliasLen);
          }
        }
      }
    }
    if (NULL == pIndex) {
      name[len] = 0;
      pIndex = taosHashGet(pFirstHash, name, len);
      if (NULL == pIndex) {
        pIndex = taosHashGet(pSecondHash, name, len);
      }
    }
    // pIndex is definitely not NULL, otherwise it is a bug
    if (NULL == pIndex) {
      planError("doSetSlotId failed, invalid slot name %s (colId=%d, tableAlias=%s, tableName=%s)",
                name,
                (QUERY_NODE_COLUMN == nodeType(pNode)) ? ((SColumnNode*)pNode)->colId : -1,
                (QUERY_NODE_COLUMN == nodeType(pNode)) ? ((SColumnNode*)pNode)->tableAlias : "?",
                (QUERY_NODE_COLUMN == nodeType(pNode)) ? ((SColumnNode*)pNode)->tableName : "?");
      if (QUERY_NODE_COLUMN == nodeType(pNode)) {
        SColumnNode* pDbgCol = (SColumnNode*)pNode;
        planError("FQ-slotMiss: colName=%s aliasName=%s userAlias=%s hasRef=%d hasDep=%d projRefIdx=%d projIdx=%d relatedTo=%d leftHash=%p rightHash=%p",
                  pDbgCol->colName, pDbgCol->node.aliasName, pDbgCol->node.userAlias, (int)pDbgCol->hasRef,
                  (int)pDbgCol->hasDep, (int)pDbgCol->projRefIdx, (int)pDbgCol->projIdx,
                  (int)pDbgCol->node.relatedTo, pCxt->pLeftHash, pCxt->pRightHash);
        fqLogSlotLookupMiss("FQ-slotMissDiag", pDbgCol, rawLookupKey, projLookupKey, pCxt->pLeftHash, "leftHash",
                            pCxt->pRightHash, "rightHash", pCxt->pLeftProjIdxHash, "leftProjIdxHash",
                            pCxt->pRightProdIdxHash, "rightProjIdxHash");
      }
      pCxt->errCode = TSDB_CODE_PLAN_SLOT_NOT_FOUND;
      taosMemoryFree(name);
      return DEAL_RES_ERROR;
    }
    ((SColumnNode*)pNode)->dataBlockId = pIndex->dataBlockId;
    int16_t assignedSlot = ((SSlotIdInfo*)taosArrayGet(pIndex->pSlotIdsInfo, 0))->slotId;
    qDebug("doSetSlotId: colName='%s' aliasName='%s' tableAlias='%s' numSlots=%d assignedSlotId=%d",
           ((SColumnNode*)pNode)->colName,
           ((SExprNode*)pNode)->aliasName,
           ((SColumnNode*)pNode)->tableAlias,
           (int32_t)taosArrayGetSize(pIndex->pSlotIdsInfo),
           (int32_t)assignedSlot);
    ((SColumnNode*)pNode)->slotId = assignedSlot;
    taosMemoryFree(name);
    return DEAL_RES_IGNORE_CHILD;
  }
  return DEAL_RES_CONTINUE;
}

static EDealRes doSetMultiTableSlotId(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode) && 0 != strcmp(((SColumnNode*)pNode)->colName, "*")) {
    SMultiTableSetSlotIdCxt* pCxt = (SMultiTableSetSlotIdCxt*)pContext;
    char*                    name = NULL;
    char                     rawLookupKey[TSDB_TABLE_FNAME_LEN + TSDB_COL_NAME_LEN + 96] = {0};
    char                     projLookupKey[TSDB_TABLE_FNAME_LEN + TSDB_COL_NAME_LEN + 96] = {0};
    int32_t                  len = 0;
    int32_t cap = 0;
    SColumnNode*             pCol = (SColumnNode*)pNode;
    if (pCxt->isVtb && ((!pCol->hasRef && pCol->colType != COLUMN_TYPE_TAG && '\0' != pCol->tableAlias[0]) || (pCol->colId == PRIMARYKEY_TIMESTAMP_COL_ID))) {
      // set slot id for :
      // 1. column with ref
      // 2. tag column
      // 3. pseudo column function

      // even if ts column has ref, still skip it (VTB only).
      return DEAL_RES_CONTINUE;
    }
  
    pCxt->errCode = getSlotKey(pNode, NULL, &name, &len, 16, &cap);
    if (TSDB_CODE_SUCCESS != pCxt->errCode) {
      return DEAL_RES_ERROR;
    }
    if (NULL != name) {
      tstrncpy(rawLookupKey, name, sizeof(rawLookupKey));
    }
    SSlotIndex* pIndex = NULL;
    if (pCol->projRefIdx > 0) {
      snprintf(name + strlen(name), cap - strlen(name), "_%d", pCol->projRefIdx);
      tstrncpy(projLookupKey, name, sizeof(projLookupKey));
      SSlotIndex** ppIndex = (SSlotIndex**)taosHashGet(pCxt->mergedProjIdxHash, name, strlen(name));
      if (ppIndex) {
        pIndex = *ppIndex;
      }
    } else {
      SSlotIndex** ppIndex = (SSlotIndex**)taosHashGet(pCxt->mergedHash, name, len);
      if (ppIndex) {
        pIndex = *ppIndex;
      }
    }
    // pIndex is definitely not NULL, otherwise it is a bug
    if (NULL == pIndex) {
      planError("doSetMultiTableSlotId failed, invalid slot name %s (col=%s, db=%s, tbl=%s, alias=%s, hasRef=%d, refDb=%s, refTbl=%s, refCol=%s, colType=%d, len=%d)",
                name, pCol->colName, pCol->dbName, pCol->tableName, pCol->tableAlias,
                pCol->hasRef, pCol->refDbName, pCol->refTableName, pCol->refColName,
                pCol->colType, len);
      fqLogSlotLookupMiss("FQ-multiSlotMissDiag", pCol, rawLookupKey, projLookupKey, pCxt->mergedHash, "mergedHash",
                          NULL, NULL, pCxt->mergedProjIdxHash, "mergedProjIdxHash", NULL, NULL);
      pCxt->errCode = TSDB_CODE_PLAN_SLOT_NOT_FOUND;
      taosMemoryFree(name);
      return DEAL_RES_ERROR;
    }
    pCol->dataBlockId = pIndex->dataBlockId;
    pCol->slotId = ((SSlotIdInfo*)taosArrayGet(pIndex->pSlotIdsInfo, 0))->slotId;
    taosMemoryFree(name);
    return DEAL_RES_IGNORE_CHILD;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t setNodeSlotIdImpl(SPhysiPlanContext* pCxt, int64_t leftDataBlockId, int64_t rightDataBlockId,
                                 const char* pLeftTableAlias, const char* pRightTableAlias, SNode* pNode,
                                 SNode** pOutput) {
  if (NULL == pNode) {
    return TSDB_CODE_SUCCESS;
  }

  SNode*  pRes = NULL;
  int32_t code = nodesCloneNode(pNode, &pRes);
  if (NULL == pRes) {
    return code;
  }

  SSetSlotIdCxt cxt = {
      .errCode = TSDB_CODE_SUCCESS,
      .pLeftHash = taosArrayGetP(pCxt->pLocationHelper, leftDataBlockId),
      .pLeftProjIdxHash = taosArrayGetP(pCxt->pProjIdxLocHelper, leftDataBlockId),
      .pRightHash = (rightDataBlockId < 0 ? NULL : taosArrayGetP(pCxt->pLocationHelper, rightDataBlockId)),
      .pRightProdIdxHash = (rightDataBlockId < 0 ? NULL : taosArrayGetP(pCxt->pProjIdxLocHelper, rightDataBlockId)),
      .pLeftTableAlias = pLeftTableAlias,
      .pRightTableAlias = pRightTableAlias};
  nodesWalkExpr(pRes, doSetSlotId, &cxt);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyNode(pRes);
    return cxt.errCode;
  }
  *pOutput = pRes;
  return TSDB_CODE_SUCCESS;
}

static int32_t setNodeSlotIdWithAliases(SPhysiPlanContext* pCxt, int64_t leftDataBlockId, int64_t rightDataBlockId,
                                        const char* pLeftAlias, const char* pRightAlias, SNode* pNode,
                                        SNode** pOutput) {
  return setNodeSlotIdImpl(pCxt, leftDataBlockId, rightDataBlockId, pLeftAlias, pRightAlias, pNode, pOutput);
}

static int32_t setNodeSlotId(SPhysiPlanContext* pCxt, int64_t leftDataBlockId, int64_t rightDataBlockId, SNode* pNode,
                             SNode** pOutput) {
  return setNodeSlotIdWithAliases(pCxt, leftDataBlockId, rightDataBlockId, NULL, NULL, pNode, pOutput);
}

static int32_t setListSlotId(SPhysiPlanContext* pCxt, int64_t leftDataBlockId, int64_t rightDataBlockId,
                             const SNodeList* pList, SNodeList** pOutput) {
  if (NULL == pList) {
    return TSDB_CODE_SUCCESS;
  }

  SNodeList* pRes = NULL;
  int32_t    code = nodesCloneList(pList, &pRes);
  if (NULL == pRes) {
    return code;
  }

  SSetSlotIdCxt cxt = {
      .errCode = TSDB_CODE_SUCCESS,
      .pLeftHash = taosArrayGetP(pCxt->pLocationHelper, leftDataBlockId),
      .pLeftProjIdxHash = taosArrayGetP(pCxt->pProjIdxLocHelper, leftDataBlockId),
      .pRightHash = (rightDataBlockId < 0 ? NULL : taosArrayGetP(pCxt->pLocationHelper, rightDataBlockId)),
      .pRightProdIdxHash = (rightDataBlockId < 0 ? NULL : taosArrayGetP(pCxt->pProjIdxLocHelper, rightDataBlockId))};
  nodesWalkExprs(pRes, doSetSlotId, &cxt);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyList(pRes);
    return cxt.errCode;
  }
  *pOutput = pRes;
  return TSDB_CODE_SUCCESS;
}

static int32_t mergeSlotHashForChildren(SNodeList* pChild, SArray* hashArray, SHashObj** ppMerged) {
  int32_t capacity = 0;
  SNode*  node = NULL;
  FOREACH(node, pChild) {
    SPhysiNode* pChildNode = (SPhysiNode*)node;
    if (pChildNode->pOutputDataBlockDesc && pChildNode->pOutputDataBlockDesc->pSlots) {
      capacity += LIST_LENGTH(pChildNode->pOutputDataBlockDesc->pSlots);
    }
  }
  if (capacity <= 0) {
    capacity = 1;
  }

  SHashObj* pMerged =
      taosHashInit(capacity, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (NULL == pMerged) {
    return terrno;
  }

  FOREACH(node, pChild) {
    SPhysiNode* pChildNode = (SPhysiNode*)node;
    if (NULL == pChildNode->pOutputDataBlockDesc) {
      continue;
    }
    SHashObj* pSrc = taosArrayGetP(hashArray, (size_t)pChildNode->pOutputDataBlockDesc->dataBlockId);
    if (NULL == pSrc) {
      continue;
    }
    void* pIt = taosHashIterate(pSrc, NULL);
    while (pIt) {
      size_t keyLen = 0;
      char*  pKey = taosHashGetKey(pIt, &keyLen);
      if (NULL == taosHashGet(pMerged, pKey, keyLen)) {
        SSlotIndex* pIndex = (SSlotIndex*)pIt;
        int32_t     code = taosHashPut(pMerged, pKey, keyLen, &pIndex, POINTER_BYTES);
        if (TSDB_CODE_SUCCESS != code) {
          taosHashCleanup(pMerged);
          return code;
        }
      }
      pIt = taosHashIterate(pSrc, pIt);
    }
  }

  *ppMerged = pMerged;
  return TSDB_CODE_SUCCESS;
}

static int32_t setMultiBlockSlotId(SPhysiPlanContext* pCxt, SNodeList* pChild, bool isVtb, const SNodeList* pList,
                                   SNodeList** pOutput) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (NULL == pList) {
    PLAN_RET(code);
  }

  SNodeList* pRes = NULL;
  SHashObj*  mergedHash = NULL;
  SHashObj*  mergedProjIdxHash = NULL;

  PLAN_ERR_JRET(nodesCloneList(pList, &pRes));

  PLAN_ERR_JRET(mergeSlotHashForChildren(pChild, pCxt->pLocationHelper, &mergedHash));
  PLAN_ERR_JRET(mergeSlotHashForChildren(pChild, pCxt->pProjIdxLocHelper, &mergedProjIdxHash));

  SMultiTableSetSlotIdCxt cxt = {
    .errCode = TSDB_CODE_SUCCESS,
    .hashArray = pCxt->pLocationHelper,
    .projIdxHashArray = pCxt->pProjIdxLocHelper,
    .mergedHash = mergedHash,
    .mergedProjIdxHash = mergedProjIdxHash,
    .pChild = pChild,
    .isVtb = isVtb
  };

  nodesWalkExprs(pRes, doSetMultiTableSlotId, &cxt);
  PLAN_ERR_JRET(cxt.errCode);

  *pOutput = pRes;
  taosHashCleanup(mergedHash);
  taosHashCleanup(mergedProjIdxHash);
  return code;
_return:
  if (mergedHash) {
    taosHashCleanup(mergedHash);
  }
  if (mergedProjIdxHash) {
    taosHashCleanup(mergedProjIdxHash);
  }
  nodesDestroyList(pRes);
  return code;
}

static SPhysiNode* makePhysiNode(SPhysiPlanContext* pCxt, SLogicNode* pLogicNode, ENodeType type) {
  SPhysiNode* pPhysiNode = NULL;
  int32_t     code = nodesMakeNode(type, (SNode**)&pPhysiNode);
  if (NULL == pPhysiNode) {
    terrno = code;
    return NULL;
  }

  TSWAP(pPhysiNode->pLimit, pLogicNode->pLimit);
  TSWAP(pPhysiNode->pSlimit, pLogicNode->pSlimit);
  pPhysiNode->dynamicOp = pLogicNode->dynamicOp;
  pPhysiNode->inputTsOrder = pLogicNode->inputTsOrder;
  pPhysiNode->outputTsOrder = pLogicNode->outputTsOrder;
  pPhysiNode->requireDataOrder = pLogicNode->requireDataOrder;
  pPhysiNode->resultDataOrder = pLogicNode->resultDataOrder;

  code = createDataBlockDesc(pCxt, pLogicNode->pTargets, &pPhysiNode->pOutputDataBlockDesc);
  if (TSDB_CODE_SUCCESS != code) {
    planError("FQ-Slots: makePhysiNode failed type=%d logicType=%d targets=%d code=0x%x",
              (int)type, (int)nodeType((SNode*)pLogicNode),
              pLogicNode->pTargets ? LIST_LENGTH(pLogicNode->pTargets) : 0, code);
    nodesDestroyNode((SNode*)pPhysiNode);
    terrno = code;
    return NULL;
  }
  pPhysiNode->pOutputDataBlockDesc->precision = pLogicNode->precision;
  return pPhysiNode;
}

static int32_t setConditionsSlotId(SPhysiPlanContext* pCxt, const SLogicNode* pLogicNode, SPhysiNode* pPhysiNode) {
  if (NULL != pLogicNode->pConditions) {
    planDebug("FQ-slotCond: logicType=%d physiType=%d dataBlockId=%" PRId64 " conditions=%p",
              (int)nodeType((SNode*)pLogicNode), (int)nodeType((SNode*)pPhysiNode),
              pPhysiNode->pOutputDataBlockDesc ? pPhysiNode->pOutputDataBlockDesc->dataBlockId : -1,
              pLogicNode->pConditions);
    return setNodeSlotId(pCxt, pPhysiNode->pOutputDataBlockDesc->dataBlockId, -1, pLogicNode->pConditions,
                         &pPhysiNode->pConditions);
  }
  return TSDB_CODE_SUCCESS;
}

static bool isTableScanPhysiNode(const SScanPhysiNode* pScanPhysiNode) {
  switch (nodeType(pScanPhysiNode)) {
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN:
      return true;
    default:
      return false;
  }
}

static int32_t setTableScanPrimaryCondSlotId(SPhysiPlanContext* pCxt, const SScanLogicNode* pScanLogicNode,
                                             SScanPhysiNode* pScanPhysiNode) {
  if (NULL == pScanLogicNode->pPrimaryCond || !isTableScanPhysiNode(pScanPhysiNode)) {
    return TSDB_CODE_SUCCESS;
  }

  STableScanPhysiNode* pTableScan = (STableScanPhysiNode*)pScanPhysiNode;
  return setNodeSlotId(pCxt, pScanPhysiNode->node.pOutputDataBlockDesc->dataBlockId, -1, pScanLogicNode->pPrimaryCond,
                       &pTableScan->pPrimaryCond);
}

static int32_t colIdCompare(const void* pLeft, const void* pRight) {
  SColumnNode* pLeftCol = *(SColumnNode**)pLeft;
  SColumnNode* pRightCol = *(SColumnNode**)pRight;
  return pLeftCol->colId > pRightCol->colId ? 1 : -1;
}

static int32_t sortScanCols(SNodeList* pScanCols) {
  SArray* pArray = taosArrayInit(LIST_LENGTH(pScanCols), POINTER_BYTES);
  if (NULL == pArray) {
    return terrno;
  }

  int32_t code = 0;
  SNode*  pCol = NULL;
  FOREACH(pCol, pScanCols) {
    if (NULL == taosArrayPush(pArray, &pCol)) {
      code = terrno;
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

static bool physiScanHasColumn(SNodeList* pList, const SColumnNode* pCol) {
  if (pList == NULL || pCol == NULL) {
    return false;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    SColumnNode* pExist = NULL;
    if (nodeType(pNode) == QUERY_NODE_TARGET) {
      STargetNode* pTarget = (STargetNode*)pNode;
      if (pTarget->pExpr != NULL && nodeType(pTarget->pExpr) == QUERY_NODE_COLUMN) {
        pExist = (SColumnNode*)pTarget->pExpr;
      }
    } else if (nodeType(pNode) == QUERY_NODE_COLUMN) {
      pExist = (SColumnNode*)pNode;
    }

    if (pExist == NULL) {
      continue;
    }

    if (pExist->colId == pCol->colId && pExist->colType == pCol->colType && pExist->hasRef == pCol->hasRef &&
        0 == strcmp(pExist->tableAlias, pCol->tableAlias) && 0 == strcmp(pExist->refDbName, pCol->refDbName) &&
        0 == strcmp(pExist->refTableName, pCol->refTableName) && 0 == strcmp(pExist->refColName, pCol->refColName)) {
      return true;
    }
  }

  return false;
}

static int32_t appendPhysiScanTbnameFunc(SNodeList** ppList, const SColumnNode* pCol) {
  if (ppList == NULL || pCol == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, *ppList) {
    if (QUERY_NODE_FUNCTION == nodeType(pNode) && FUNCTION_TYPE_TBNAME == ((SFunctionNode*)pNode)->funcType) {
      return TSDB_CODE_SUCCESS;
    }
  }

  SFunctionNode* pFunc = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  tstrncpy(pFunc->functionName, "tbname", TSDB_FUNC_NAME_LEN);
  pFunc->funcType = FUNCTION_TYPE_TBNAME;
  pFunc->node.resType = pCol->node.resType;
  code = fmGetFuncInfo(pFunc, NULL, 0);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pFunc);
    return code;
  }
  // Strict append consumes the node on both success and failure.
  return nodesListMakeStrictAppend(ppList, (SNode*)pFunc);
}

static int32_t appendPhysiScanColumn(SNodeList** ppList, const SColumnNode* pCol) {
  if (ppList == NULL || pCol == NULL || physiScanHasColumn(*ppList, pCol)) {
    return TSDB_CODE_SUCCESS;
  }

  SNode* pClone = NULL;
  int32_t code = nodesCloneNode((SNode*)pCol, &pClone);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(ppList, pClone);
  }
  return code;
}

static int32_t appendPhysiLocalTagsFromList(SNodeList* pSrc, SNodeList** ppDst) {
  if (pSrc == NULL || ppDst == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pNode = NULL;
  FOREACH(pNode, pSrc) {
    SColumnNode* pCol = NULL;
    if (nodeType(pNode) == QUERY_NODE_TARGET) {
      STargetNode* pTarget = (STargetNode*)pNode;
      if (pTarget->pExpr != NULL && nodeType(pTarget->pExpr) == QUERY_NODE_COLUMN) {
        pCol = (SColumnNode*)pTarget->pExpr;
      }
    } else if (nodeType(pNode) == QUERY_NODE_COLUMN) {
      pCol = (SColumnNode*)pNode;
    }

    if (pCol == NULL || pCol->colType != COLUMN_TYPE_TAG || pCol->hasRef) {
      continue;
    }

    code = appendPhysiScanColumn(ppDst, pCol);
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }

  return code;
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

static int32_t appendConditionColsToPhysiScan(SScanPhysiNode* pScanPhysiNode, SNode* pConditions) {
  if (NULL == pScanPhysiNode || NULL == pConditions) {
    return TSDB_CODE_SUCCESS;
  }

  SNodeList* pCondCols = NULL;
  int32_t    code = nodesCollectColumnsFromNode(pConditions, NULL, COLLECT_COL_TYPE_ALL, &pCondCols);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pCondCols) {
    if (QUERY_NODE_COLUMN != nodeType(pNode)) {
      continue;
    }

    SColumnNode* pCol = (SColumnNode*)pNode;
    if (COLUMN_TYPE_TAG == pCol->colType) {
      code = appendPhysiScanColumn(&pScanPhysiNode->pScanPseudoCols, pCol);
    } else if (COLUMN_TYPE_TBNAME == pCol->colType) {
      code = appendPhysiScanTbnameFunc(&pScanPhysiNode->pScanPseudoCols, pCol);
    } else {
      code = appendPhysiScanColumn(&pScanPhysiNode->pScanCols, pCol);
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }

  nodesDestroyList(pCondCols);

  if (TSDB_CODE_SUCCESS == code) {
    code = sortScanCols(pScanPhysiNode->pScanCols);
  }

  return code;
}

static int32_t createScanPhysiNodeFinalize(SPhysiPlanContext* pCxt, SSubplan* pSubplan, SScanLogicNode* pScanLogicNode,
                                           SScanPhysiNode* pScanPhysiNode, SPhysiNode** pPhyNode) {
  int32_t code = createScanCols(pCxt, pScanPhysiNode, pScanLogicNode->pScanCols);
  if (TSDB_CODE_SUCCESS == code && NULL != pScanLogicNode->pScanPseudoCols) {
    pScanPhysiNode->pScanPseudoCols = NULL;
    code = nodesCloneList(pScanLogicNode->pScanPseudoCols, &pScanPhysiNode->pScanPseudoCols);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = appendConditionColsToPhysiScan(pScanPhysiNode, pScanLogicNode->node.pConditions);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = addDataBlockSlots(pCxt, pScanPhysiNode->pScanCols, pScanPhysiNode->node.pOutputDataBlockDesc);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = addDataBlockSlots(pCxt, pScanPhysiNode->pScanPseudoCols, pScanPhysiNode->node.pOutputDataBlockDesc);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setConditionsSlotId(pCxt, (const SLogicNode*)pScanLogicNode, (SPhysiNode*)pScanPhysiNode);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setTableScanPrimaryCondSlotId(pCxt, pScanLogicNode, pScanPhysiNode);
  }

  if (TSDB_CODE_SUCCESS == code) {
    pScanPhysiNode->uid = pScanLogicNode->tableId;
    pScanPhysiNode->suid = pScanLogicNode->stableId;
    pScanPhysiNode->tableType = pScanLogicNode->tableType;
    pScanPhysiNode->groupOrderScan = pScanLogicNode->groupOrderScan;
    pScanPhysiNode->virtualStableScan = pScanLogicNode->virtualStableScan;
    pScanPhysiNode->outputTsPrecision = pScanLogicNode->outputTsPrecision;
    pScanPhysiNode->useTriggerTableList = pScanLogicNode->phTbnameScan;
    memcpy(&pScanPhysiNode->tableName, &pScanLogicNode->tableName, sizeof(SName));
    nodesDestroyNode(pSubplan->pTagCond);
    pSubplan->pTagCond = NULL;
    if (NULL != pScanLogicNode->pTagCond) {
      code = nodesCloneNode(pScanLogicNode->pTagCond, &pSubplan->pTagCond);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    nodesDestroyNode(pSubplan->pTagIndexCond);
    pSubplan->pTagIndexCond = NULL;
    if (NULL != pScanLogicNode->pTagIndexCond) {
      code = nodesCloneNode(pScanLogicNode->pTagIndexCond, &pSubplan->pTagIndexCond);
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

static bool canBatchLastRowScanRead(const SScanLogicNode* pScan) {
  return NULL != pScan && SCAN_TYPE_LAST_ROW == pScan->scanType;
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
  if (TSDB_CODE_SUCCESS != code) {
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
  pScan->batchMultiTableRead = canBatchLastRowScanRead(pScanLogicNode);

  if (pScanLogicNode->pVgroupList) {
    vgroupInfoToNodeAddr(pScanLogicNode->pVgroupList->vgroups, &pSubplan->execNode);
  }

  code = createScanPhysiNodeFinalize(pCxt, pSubplan, pScanLogicNode, (SScanPhysiNode*)pScan, pPhyNode);
  if (TSDB_CODE_SUCCESS == code && pScanLogicNode->pFuncTypes != NULL) {
    pScan->pFuncTypes = taosArrayInit(taosArrayGetSize(pScanLogicNode->pFuncTypes), sizeof(int32_t));
    if (NULL == pScan->pFuncTypes) {
      return terrno;
    }

    SNode* pTargetNode = NULL;
    int    funcTypeIndex = 0;
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
            code = terrno;
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
  // A dedicated node type carries the row type filter: older vnodes fail loudly on the unknown node
  // type instead of silently ignoring the filter and returning an unfiltered count.
  ENodeType nodeType = (0 != pScanLogicNode->tbCountType) ? QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN_BY_TYPE
                                                          : QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN;
  STableCountScanPhysiNode* pScan =
      (STableCountScanPhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pScanLogicNode, nodeType);
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
  pScan->tbCountType = pScanLogicNode->tbCountType;
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
  pTableScan->twoPassRescanSource = pScanLogicNode->twoPassRescanSource;
  pTableScan->scanRange = pScanLogicNode->scanRange;
  TSWAP(pTableScan->pExtScanRange, pScanLogicNode->pExtScanRange);
  pTableScan->interpMode = pScanLogicNode->interpMode;
  pTableScan->interpFillMode = pScanLogicNode->interpFillMode;
  pTableScan->ratio = pScanLogicNode->ratio;
  if (pScanLogicNode->pVgroupList) {
    vgroupInfoToNodeAddr(pScanLogicNode->pVgroupList->vgroups, &pSubplan->execNode);
    pSubplan->execNodeStat.tableNum = pScanLogicNode->pVgroupList->vgroups[0].numOfTable;
  }
  (void)tNameGetFullDbName(&pScanLogicNode->tableName, pSubplan->dbFName);
  pTableScan->dataRequired = pScanLogicNode->dataRequired;
  pTableScan->lastOnlyScan = pScanLogicNode->lastOnlyScan;
  pTableScan->pDynamicScanFuncs = NULL;
  int32_t code = nodesCloneList(pScanLogicNode->pDynamicScanFuncs, &pTableScan->pDynamicScanFuncs);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pTableScan);
    return code;
  }
  pTableScan->pGroupTags = NULL;
  code = nodesCloneList(pScanLogicNode->pGroupTags, &pTableScan->pGroupTags);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pTableScan);
    return code;
  }
  code = nodesCloneNode(pScanLogicNode->pTimeRange, &pTableScan->pTimeRange);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pTableScan);
    return code;
  }
  code = nodesCloneNode(pScanLogicNode->pExtTimeRange, &pTableScan->pExtTimeRange);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pTableScan);
    return code;
  }
  pTableScan->groupSort = pScanLogicNode->groupSort;
  pTableScan->interval = pScanLogicNode->interval;
  pTableScan->offset = pScanLogicNode->offset;
  pTableScan->sliding = pScanLogicNode->sliding;
  pTableScan->intervalUnit = pScanLogicNode->intervalUnit;
  pTableScan->slidingUnit = pScanLogicNode->slidingUnit;
  pTableScan->fastInterval = pScanLogicNode->fastInterval;
  pTableScan->exInterval = pScanLogicNode->exInterval;
  pTableScan->firstDayOfWeek = pScanLogicNode->firstDayOfWeek;
  pTableScan->timezone = pScanLogicNode->timezone;
  tstrncpy(pTableScan->timezoneName, pScanLogicNode->timezoneName, sizeof(pTableScan->timezoneName));
  pTableScan->triggerType = pScanLogicNode->triggerType;
  pTableScan->watermark = pScanLogicNode->watermark;
  pTableScan->igExpired = pScanLogicNode->igExpired;
  pTableScan->igCheckUpdate = pScanLogicNode->igCheckUpdate;
  pTableScan->assignBlockUid = pCxt->pPlanCxt->rSmaQuery ? true : false;
  pTableScan->filesetDelimited = pScanLogicNode->filesetDelimited;
  pTableScan->needCountEmptyTable = pScanLogicNode->isCountByTag;
  pTableScan->paraTablesSort = pScanLogicNode->paraTablesSort;
  pTableScan->smallDataTsSort = pScanLogicNode->smallDataTsSort;

  return createScanPhysiNodeFinalize(pCxt, pSubplan, pScanLogicNode, (SScanPhysiNode*)pTableScan, pPhyNode);;
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
  pScan->xnodeDsnOutputMode = pScanLogicNode->xnodeDsnOutputMode;
  pScan->accountId = pCxt->pPlanCxt->acctId;
  pScan->sysInfo = pCxt->pPlanCxt->sysInfo;
  pScan->privInfo = pCxt->pPlanCxt->privInfo;
  // Bind the connection's effective charset so the executor renders NCHAR/JSON
  // system-table tag values in the client charset. taosConvCharset resolves a
  // NULL context to the local process default (client-side plans carry the
  // client charset; server-built plans carry the server charset), so serialize
  // it unconditionally; only genuinely old plans lack the field and fall back to
  // the server default at decode.
  tstrncpy(pScan->charsetName, taosConvCharset(pCxt->pPlanCxt->charsetCxt),
           TD_CHARSET_LEN);
  if (0 == strcmp(pScanLogicNode->tableName.tname, TSDB_INS_TABLE_TABLES) ||
      0 == strcmp(pScanLogicNode->tableName.tname, TSDB_INS_TABLE_TAGS) ||
      0 == strcmp(pScanLogicNode->tableName.tname, TSDB_INS_TABLE_COLS) ||
      0 == strcmp(pScanLogicNode->tableName.tname, TSDB_INS_DISK_USAGE) ||
      0 == strcmp(pScanLogicNode->tableName.tname, TSDB_INS_TABLE_LAST_CACHE_STATS) ||
      0 == strcmp(pScanLogicNode->tableName.tname, TSDB_INS_TABLE_FILESETS) ||
      0 == strcmp(pScanLogicNode->tableName.tname, TSDB_INS_TABLE_VC_COLS) ||
      0 == strcmp(pScanLogicNode->tableName.tname, TSDB_INS_TABLE_VIRTUAL_TABLES_REFERENCING) ||
      0 == strcmp(pScanLogicNode->tableName.tname, TSDB_INS_TABLE_TABLE_FIXED_DISTRIBUTED)) {
    if (pScanLogicNode->pVgroupList) {
      vgroupInfoToNodeAddr(pScanLogicNode->pVgroupList->vgroups, &pSubplan->execNode);
    }
  } else {
    pSubplan->execNode.nodeId = MNODE_HANDLE;
    pSubplan->execNode.epSet = pCxt->pPlanCxt->mgmtEpSet;
  }
  if ((0 == strcmp(pScanLogicNode->tableName.tname, TSDB_INS_TABLE_DNODE_VARIABLES) ||
       0 == strcmp(pScanLogicNode->tableName.tname, TSDB_INS_TABLE_CPU_ALLOCATION)) &&
      pScanLogicNode->pVgroupList) {
    pScan->mgmtEpSet = pScanLogicNode->pVgroupList->vgroups->epSet;
  } else {
    pScan->mgmtEpSet = pCxt->pPlanCxt->mgmtEpSet;
  }
  (void)tNameGetFullDbName(&pScanLogicNode->tableName, pSubplan->dbFName);

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

// ---------------------------------------------------------------------------
// --- Remote-plan helpers ----------------------------------------------------
// Convert one logic node from pRemoteLogicPlan into its lightweight physi
// counterpart and wire pChild as its single child.
// pLeaf is passed so LIMIT can be propagated down to the leaf WHERE node.
static int32_t remoteLogicNodeToPhysi(SLogicNode* pLogicNode, SPhysiNode* pChild,
                                      SFederatedScanPhysiNode* pLeaf, SPhysiNode** pOut) {
  int32_t     code  = TSDB_CODE_SUCCESS;
  ENodeType   type  = nodeType(pLogicNode);
  SPhysiNode* pPhys = NULL;

  if (type == QUERY_NODE_LOGIC_PLAN_SORT) {
    SSortLogicNode* pSortLogic = (SSortLogicNode*)pLogicNode;
    SSortPhysiNode* pSort      = NULL;
    code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_SORT, (SNode**)&pSort);
    if (TSDB_CODE_SUCCESS != code) return code;

    code = nodesCloneList(pSortLogic->pSortKeys, &pSort->pSortKeys);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pSort); return code; }

    // Clone pTargets so the executor can derive output column info from the
    // topmost remote physical node (used for pColTypeMappings reconstruction).
    code = nodesCloneList(pSortLogic->node.pTargets, &pSort->pTargets);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pSort); return code; }

    if (NULL == pLeaf->node.pLimit && NULL != pSortLogic->node.pLimit) {
      code = nodesCloneNode(pSortLogic->node.pLimit, &pLeaf->node.pLimit);
      if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pSort); return code; }
    }
    code = nodesMakeList(&pSort->node.pChildren);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pSort); return code; }
    code = nodesListStrictAppend(pSort->node.pChildren, (SNode*)pChild);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pSort); return code; }
    pChild->pParent = (SPhysiNode*)pSort;
    pPhys = (SPhysiNode*)pSort;

  } else if (type == QUERY_NODE_LOGIC_PLAN_PROJECT) {
    SProjectLogicNode* pProjLogic = (SProjectLogicNode*)pLogicNode;
    SProjectPhysiNode* pProj      = NULL;
    code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_PROJECT, (SNode**)&pProj);
    if (TSDB_CODE_SUCCESS != code) return code;

    code = nodesCloneList(pProjLogic->pProjections, &pProj->pProjections);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pProj); return code; }

    if (NULL == pLeaf->node.pLimit && NULL != pProjLogic->node.pLimit) {
      code = nodesCloneNode(pProjLogic->node.pLimit, &pLeaf->node.pLimit);
      if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pProj); return code; }
    }
    code = nodesMakeList(&pProj->node.pChildren);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pProj); return code; }
    code = nodesListStrictAppend(pProj->node.pChildren, (SNode*)pChild);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pProj); return code; }
    pChild->pParent = (SPhysiNode*)pProj;
    pPhys = (SPhysiNode*)pProj;

  } else {
    planError("remoteLogicNodeToPhysi: unsupported logic node type %d in pRemoteLogicPlan", type);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  *pOut = pPhys;
  return TSDB_CODE_SUCCESS;
}
static bool fqPhysiExprListAllColumns(const SNodeList* pExprList) {
  if (NULL == pExprList) {
    return true;
  }

  SNode* pExpr = NULL;
  FOREACH(pExpr, pExprList) {
    const SNode* pInner = pExpr;
    if (nodeType(pInner) == QUERY_NODE_TARGET) {
      pInner = ((const STargetNode*)pInner)->pExpr;
    }
    if (NULL == pInner || nodeType(pInner) != QUERY_NODE_COLUMN) {
      return false;
    }
  }

  return true;
}

static bool fqPhysiOutputNeedsRemoteMapping(const SNode* pExprNode) {
  if (NULL == pExprNode) {
    return false;
  }

  const SNode* pExpr = pExprNode;
  if (QUERY_NODE_TARGET == nodeType(pExpr)) {
    pExpr = ((const STargetNode*)pExpr)->pExpr;
  }

  // Folded constant outputs (VALUE) do not need remote data mapping.
  // Remote side may execute SELECT 1 as row anchor, while the executor
  // overwrites output slots from VALUE expressions locally.
  return pExpr != NULL && nodeType(pExpr) != QUERY_NODE_VALUE;
}

static void fqCopyTimezoneNameOnly(char* pDst, int32_t dstSize, const char* pSrc) {
  if (pDst == NULL || dstSize <= 0) {
    return;
  }
  pDst[0] = '\0';
  if (pSrc == NULL || pSrc[0] == '\0') {
    return;
  }

  const char* pSpace = strchr(pSrc, ' ');
  int32_t     len = pSpace ? (int32_t)(pSpace - pSrc) : (int32_t)strlen(pSrc);
  if (len <= 0) {
    return;
  }
  if (len >= dstSize) {
    len = dstSize - 1;
  }
  memcpy(pDst, pSrc, len);
  pDst[len] = '\0';
}

static void fqSetFederatedScanTimezone(const SPhysiPlanContext* pCxt, SFederatedScanPhysiNode* pScan) {
  if (pCxt == NULL || pScan == NULL) {
    return;
  }

  const SPlanContext* pPlanCxt = pCxt->pPlanCxt;
  if (pPlanCxt != NULL && pPlanCxt->timezoneExplicit && pPlanCxt->timezoneName[0] != '\0') {
    tstrncpy(pScan->timezone, pPlanCxt->timezoneName, sizeof(pScan->timezone));
    return;
  }

  fqCopyTimezoneNameOnly(pScan->timezone, sizeof(pScan->timezone), tsTimezoneStr);
  if (pScan->timezone[0] != '\0') {
    return;
  }

  if (pPlanCxt != NULL && pPlanCxt->timezoneName[0] != '\0') {
    tstrncpy(pScan->timezone, pPlanCxt->timezoneName, sizeof(pScan->timezone));
    return;
  }

  char sysTz[TD_TIMEZONE_LEN] = {0};
  (void)taosGetSystemTimezone(sysTz);
  fqCopyTimezoneNameOnly(pScan->timezone, sizeof(pScan->timezone), sysTz);
}

static SNodeList* fqFindRemoteProjectOutputs(SLogicNode* pRoot) {
  SLogicNode* pCur = pRoot;
  while (pCur != NULL) {
    if (QUERY_NODE_LOGIC_PLAN_PROJECT == nodeType((SNode*)pCur)) {
      SNodeList* pProj = ((SProjectLogicNode*)pCur)->pProjections;
      if (pProj != NULL && LIST_LENGTH(pProj) > 0) {
        return pProj;
      }
    }

    if (NULL == pCur->pChildren || LIST_LENGTH(pCur->pChildren) != 1) {
      break;
    }
    pCur = (SLogicNode*)nodesListGetNode(pCur->pChildren, 0);
  }

  return NULL;
}

static void fqFormatLogicColList(const SNodeList* pCols, char* pBuf, int32_t bufLen) {
  if (NULL == pBuf || bufLen <= 0) {
    return;
  }

  pBuf[0] = '\0';
  if (NULL == pCols) {
    tstrncpy(pBuf, "(NULL)", (size_t)bufLen);
    return;
  }

  int32_t off = 0;
  SNode* pCol = NULL;
  FOREACH(pCol, pCols) {
    const char* name = "?";
    if (QUERY_NODE_COLUMN == nodeType(pCol)) {
      name = ((const SColumnNode*)pCol)->colName;
    } else if (QUERY_NODE_TARGET == nodeType(pCol)) {
      const SNode* pExpr = ((const STargetNode*)pCol)->pExpr;
      if (pExpr != NULL && QUERY_NODE_COLUMN == nodeType(pExpr)) {
        name = ((const SColumnNode*)pExpr)->colName;
      }
    }

    off += snprintf(pBuf + off, (size_t)(bufLen - off), "%s%s", off > 0 ? "," : "", name);
    if (off >= bufLen - 1) {
      break;
    }
  }

  if ('\0' == pBuf[0]) {
    tstrncpy(pBuf, "(EMPTY)", (size_t)bufLen);
  }
}

static int32_t fqReplaceLogicTargets(SLogicNode* pNode, const SNodeList* pAlignedCols, const char* pTag) {
  if (NULL == pNode || NULL == pAlignedCols) {
    return TSDB_CODE_SUCCESS;
  }

  char beforeBuf[512] = {0};
  char afterBuf[512] = {0};
  fqFormatLogicColList(pNode->pTargets, beforeBuf, sizeof(beforeBuf));
  fqFormatLogicColList(pAlignedCols, afterBuf, sizeof(afterBuf));

  SNodeList* pCloned = NULL;
  int32_t code = nodesCloneList((SNodeList*)pAlignedCols, &pCloned);
  if (TSDB_CODE_SUCCESS != code) {
    planError("FQ-RemoteCols: clone aligned targets failed, tag=%s rootType=%d code:0x%x",
              NULL != pTag ? pTag : "(null)", (int)nodeType((SNode*)pNode), code);
    return code;
  }

  nodesDestroyList(pNode->pTargets);
  pNode->pTargets = pCloned;
  planDebug("FQ-RemoteCols: aligned %s targets, nodeType=%d targetsBefore=[%s] targetsAfter=[%s]",
            NULL != pTag ? pTag : "(null)", (int)nodeType((SNode*)pNode), beforeBuf, afterBuf);
  return TSDB_CODE_SUCCESS;
}

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

// Build the pRemotePlan physical tree from pScanLogicNode->pRemoteLogicPlan.
// Walks the logic chain top-down (collecting), then converts bottom-up so the
// leaf SFederatedScanPhysiNode ends up at the bottom of the chain.
// On error the entire chain (including pLeaf) is left for the caller to destroy.
//
// UNION ALL support: when the root of pRemoteLogicPlan is a SProjectLogicNode
// with isSetOpProj=true and multiple children, each child branch is converted
// independently (each gets its own Mode-2 FederatedScan leaf cloned from
// pLeaf) and wired as children of a SProjectPhysiNode container.

// Helper: convert a single-chain logic plan into a physi plan chain.
// pChainRoot: root of the chain (Sort or Scan logic node).
// pLeafTemplate: the Mode-1 outer FederatedScanPhysiNode (for cloning Mode-2 leaves).
// pScanLogic: the original SScanLogicNode (for pExtTable, pScanCols info).
// On success, *pOut is the physi chain root; the Mode-2 leaf is at the bottom.
static int32_t buildRemoteBranchPhysi(SLogicNode* pChainRoot,
                                      SFederatedScanPhysiNode* pLeafTemplate,
                                      SScanLogicNode* pScanLogic,
                                      SPhysiNode** pOut) {
  // Collect chain into array
  SArray* pArr = taosArrayInit(4, POINTER_BYTES);
  if (NULL == pArr) return terrno;

  int32_t code = TSDB_CODE_SUCCESS;
  SLogicNode* pCurr = pChainRoot;
  while (pCurr != NULL) {
    // Stop at SScanLogicNode  -  it becomes the leaf, not a logic-to-physi conversion
    if (nodeType(pCurr) == QUERY_NODE_LOGIC_PLAN_SCAN) break;
    if (NULL == taosArrayPush(pArr, &pCurr)) {
      code = terrno;
      taosArrayDestroy(pArr);
      return code;
    }
    SNodeList* pChildren = pCurr->pChildren;
    pCurr = (pChildren && LIST_LENGTH(pChildren) > 0)
                ? (SLogicNode*)nodesListGetNode(pChildren, 0) : NULL;
  }

  // Create the Mode-2 FederatedScan leaf for this branch
  SFederatedScanPhysiNode* pBranchLeaf = NULL;
  code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN, (SNode**)&pBranchLeaf);
  if (TSDB_CODE_SUCCESS != code) { taosArrayDestroy(pArr); return code; }

  // The branch leaf has its own pExtTable, pScanCols, and pConditions
  // from the SScanLogicNode at the bottom of this branch's chain.
  SScanLogicNode* pBranchScan = NULL;
  if (pCurr != NULL && nodeType(pCurr) == QUERY_NODE_LOGIC_PLAN_SCAN) {
    pBranchScan = (SScanLogicNode*)pCurr;
  }

  if (pBranchScan != NULL) {
    // Clone from the branch's scan node
    code = nodesCloneNode(pBranchScan->pExtTableNode, &pBranchLeaf->pExtTable);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pBranchLeaf); taosArrayDestroy(pArr); return code; }
    code = nodesCloneList(pBranchScan->pScanCols, &pBranchLeaf->pScanCols);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pBranchLeaf); taosArrayDestroy(pArr); return code; }
    if (pBranchScan->node.pConditions != NULL) {
      code = nodesCloneNode(pBranchScan->node.pConditions, &pBranchLeaf->node.pConditions);
      if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pBranchLeaf); taosArrayDestroy(pArr); return code; }
    }
    if (pBranchScan->node.pLimit != NULL) {
      code = nodesCloneNode(pBranchScan->node.pLimit, &pBranchLeaf->node.pLimit);
      if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pBranchLeaf); taosArrayDestroy(pArr); return code; }
    }
  } else {
    // Fallback: clone from the outer scan's info
    code = nodesCloneNode(pScanLogic->pExtTableNode, &pBranchLeaf->pExtTable);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pBranchLeaf); taosArrayDestroy(pArr); return code; }
    code = nodesCloneList(pScanLogic->pScanCols, &pBranchLeaf->pScanCols);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pBranchLeaf); taosArrayDestroy(pArr); return code; }
  }

  pBranchLeaf->pRemotePlan = NULL;  // Mode-2 leaf

  // Convert chain bottom-up
  int32_t n = (int32_t)taosArrayGetSize(pArr);
  SPhysiNode* pBottom = (SPhysiNode*)pBranchLeaf;
  for (int32_t i = n - 1; i >= 0; i--) {
    SLogicNode* pLogic  = *(SLogicNode**)taosArrayGet(pArr, i);
    SPhysiNode* pNewTop = NULL;
    code = remoteLogicNodeToPhysi(pLogic, pBottom, pBranchLeaf, &pNewTop);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode((SNode*)pBottom);
      taosArrayDestroy(pArr);
      return code;
    }
    pBottom = pNewTop;
  }

  taosArrayDestroy(pArr);
  *pOut = pBottom;
  return TSDB_CODE_SUCCESS;
}

static int32_t buildRemotePlanFromLogicPlan(SScanLogicNode* pScanLogic,
                                            SFederatedScanPhysiNode* pLeaf,
                                            SPhysiNode** pRemoteRoot) {
  if (NULL == pScanLogic->pRemoteLogicPlan) {
    *pRemoteRoot = (SPhysiNode*)pLeaf;
    return TSDB_CODE_SUCCESS;
  }

  SLogicNode* pLogicRoot = (SLogicNode*)pScanLogic->pRemoteLogicPlan;

  // -- UNION ALL: multi-child Project with isSetOpProj=true --
  if (nodeType(pLogicRoot) == QUERY_NODE_LOGIC_PLAN_PROJECT &&
      ((SProjectLogicNode*)pLogicRoot)->isSetOpProj &&
      pLogicRoot->pChildren != NULL &&
      LIST_LENGTH(pLogicRoot->pChildren) > 1) {
    SProjectLogicNode* pSetOpLogic = (SProjectLogicNode*)pLogicRoot;
    int32_t code = TSDB_CODE_SUCCESS;

    // Create container SProjectPhysiNode
    SProjectPhysiNode* pUnionPhysi = NULL;
    code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_PROJECT, (SNode**)&pUnionPhysi);
    if (TSDB_CODE_SUCCESS != code) { *pRemoteRoot = (SPhysiNode*)pLeaf; return code; }

    // Clone projections
    code = nodesCloneList(pSetOpLogic->pProjections, &pUnionPhysi->pProjections);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pUnionPhysi); *pRemoteRoot = (SPhysiNode*)pLeaf; return code; }

    code = nodesMakeList(&pUnionPhysi->node.pChildren);
    if (TSDB_CODE_SUCCESS != code) { nodesDestroyNode((SNode*)pUnionPhysi); *pRemoteRoot = (SPhysiNode*)pLeaf; return code; }

    // Convert each child branch
    SNode* pChild = NULL;
    FOREACH(pChild, pLogicRoot->pChildren) {
      SPhysiNode* pBranchPhysi = NULL;
      code = buildRemoteBranchPhysi((SLogicNode*)pChild, pLeaf, pScanLogic, &pBranchPhysi);
      if (TSDB_CODE_SUCCESS != code) {
        nodesDestroyNode((SNode*)pUnionPhysi);
        *pRemoteRoot = (SPhysiNode*)pLeaf;
        return code;
      }
      pBranchPhysi->pParent = (SPhysiNode*)pUnionPhysi;
      code = nodesListStrictAppend(pUnionPhysi->node.pChildren, (SNode*)pBranchPhysi);
      if (TSDB_CODE_SUCCESS != code) {
        nodesDestroyNode((SNode*)pUnionPhysi);
        *pRemoteRoot = (SPhysiNode*)pLeaf;
        return code;
      }
    }

    *pRemoteRoot = (SPhysiNode*)pUnionPhysi;
    return TSDB_CODE_SUCCESS;
  }

  // -- Single-chain: existing behavior --
  // Collect chain top-down into a temporary array.
  SArray* pArr = taosArrayInit(4, POINTER_BYTES);
  if (NULL == pArr) {
    *pRemoteRoot = (SPhysiNode*)pLeaf;
    return terrno;
  }

  int32_t code  = TSDB_CODE_SUCCESS;
  SNode*  pCurr = pScanLogic->pRemoteLogicPlan;
  while (pCurr != NULL) {
    if (NULL == taosArrayPush(pArr, &pCurr)) {
      code = terrno;
      *pRemoteRoot = (SPhysiNode*)pLeaf;
      goto _done;
    }
    SNodeList* pChildren = ((SLogicNode*)pCurr)->pChildren;
    pCurr = (pChildren && LIST_LENGTH(pChildren) > 0) ? nodesListGetNode(pChildren, 0) : NULL;
  }

  // Convert bottom-up: start with pLeaf as the initial "current bottom".
  {
    SPhysiNode* pBottom = (SPhysiNode*)pLeaf;
    int32_t     n       = (int32_t)taosArrayGetSize(pArr);
    for (int32_t i = n - 1; i >= 0; i--) {
      SLogicNode* pLogic  = *(SLogicNode**)taosArrayGet(pArr, i);
      SPhysiNode* pNewTop = NULL;
      code = remoteLogicNodeToPhysi(pLogic, pBottom, pLeaf, &pNewTop);
      if (TSDB_CODE_SUCCESS != code) {
        // On error, pBottom owns the partial chain (including pLeaf at the
        // bottom).  Let the caller destroy it via pRemoteRoot.
        *pRemoteRoot = pBottom;
        goto _done;
      }
      pBottom = pNewTop;
    }
    *pRemoteRoot = pBottom;
  }

_done:
  taosArrayDestroy(pArr);
  return code;
}
static int32_t fqAlignRemotePassthroughOutputsImpl(SLogicNode* pNode, const SNodeList* pAlignedCols,
                                                   bool* pPassthrough) {
  if (NULL != pPassthrough) {
    *pPassthrough = false;
  }
  if (NULL == pNode || NULL == pAlignedCols) {
    return TSDB_CODE_SUCCESS;
  }

  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType((SNode*)pNode)) {
    SScanLogicNode* pScan = (SScanLogicNode*)pNode;
    if (SCAN_TYPE_EXTERNAL != pScan->scanType) {
      return TSDB_CODE_SUCCESS;
    }

    char beforeBuf[512] = {0};
    char afterBuf[512] = {0};
    fqFormatLogicColList(pScan->pScanCols, beforeBuf, sizeof(beforeBuf));
    fqFormatLogicColList(pAlignedCols, afterBuf, sizeof(afterBuf));

    SNodeList* pCloned = NULL;
    int32_t code = nodesCloneList((SNodeList*)pAlignedCols, &pCloned);
    if (TSDB_CODE_SUCCESS != code) {
      planError("FQ-RemoteCols: clone aligned scan cols failed, remoteRootType=%d code:0x%x",
                (int)nodeType((SNode*)pNode), code);
      return code;
    }

    nodesDestroyList(pScan->pScanCols);
    pScan->pScanCols = pCloned;
    planDebug("FQ-RemoteCols: aligned remote leaf scan cols, remoteRootType=%d leafBefore=[%s] leafAfter=[%s]",
              (int)nodeType((SNode*)pNode), beforeBuf, afterBuf);

    code = fqReplaceLogicTargets((SLogicNode*)pScan, pAlignedCols, "leaf-scan");
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }

    if (NULL != pPassthrough) {
      *pPassthrough = true;
    }
    return TSDB_CODE_SUCCESS;
  }

  if (QUERY_NODE_LOGIC_PLAN_SORT == nodeType((SNode*)pNode) &&
      NULL != pNode->pChildren && 1 == LIST_LENGTH(pNode->pChildren)) {
    bool childPassthrough = false;
    int32_t code =
        fqAlignRemotePassthroughOutputsImpl((SLogicNode*)nodesListGetNode(pNode->pChildren, 0),
                                            pAlignedCols, &childPassthrough);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    if (childPassthrough) {
      code = fqReplaceLogicTargets(pNode, pAlignedCols, "sort-passthrough");
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      if (NULL != pPassthrough) {
        *pPassthrough = true;
      }
    } else {
      planDebug("FQ-RemoteCols: skip sort target alignment, child is not passthrough, rootType=%d",
                (int)nodeType((SNode*)pNode));
    }
    return TSDB_CODE_SUCCESS;
  }

  planDebug("FQ-RemoteCols: skip passthrough alignment for remoteRootType=%d", (int)nodeType((SNode*)pNode));
  return TSDB_CODE_SUCCESS;
}

static int32_t fqAlignRemotePassthroughOutputs(SNode* pRemoteRoot, const SNodeList* pAlignedCols) {
  if (NULL == pRemoteRoot || NULL == pAlignedCols) {
    return TSDB_CODE_SUCCESS;
  }

  bool passthrough = false;
  int32_t code = fqAlignRemotePassthroughOutputsImpl((SLogicNode*)pRemoteRoot, pAlignedCols, &passthrough);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  if (!passthrough) {
    SScanLogicNode* pLeaf = fqFindExternalLogicLeafScan((SLogicNode*)pRemoteRoot);
    if (NULL == pLeaf) {
      planDebug("FQ-RemoteCols: no external logic leaf found for remoteRootType=%d", (int)nodeType(pRemoteRoot));
    }
  }
  return TSDB_CODE_SUCCESS;
}

static SNodeList* fqGetRemoteOutputTargets(SNode* pRemoteRoot) {
  if (NULL == pRemoteRoot) {
    return NULL;
  }

  return ((SLogicNode*)pRemoteRoot)->pTargets;
}

static int32_t fqRefreshRemoteOutputTargets(SNode* pRemoteRoot) {
  if (NULL == pRemoteRoot) {
    return TSDB_CODE_SUCCESS;
  }

  SNodesRemoteSelectOutputs outputs = {0};
  int32_t code = nodesCollectRemoteQuerySelectOutputs((SLogicNode*)pRemoteRoot, &outputs);
  if (TSDB_CODE_SUCCESS != code) {
    planError("FQ-RemoteCols: collect remote select outputs failed, rootType=%d code:0x%x",
              (int)nodeType(pRemoteRoot), code);
    return code;
  }

  SNodeList* pNewTargets = NULL;
  code = createColumnByRemoteOutputExprs(outputs.pSelectExprs, &pNewTargets);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyRemoteSelectOutputs(&outputs);
    return code;
  }

  code = fqReplaceLogicTargets((SLogicNode*)pRemoteRoot, pNewTargets, "remote-select-outputs");
  nodesDestroyList(pNewTargets);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyRemoteSelectOutputs(&outputs);
    return code;
  }

  code = replaceProjectLogicProjections((SLogicNode*)pRemoteRoot, outputs.pSelectExprs);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyRemoteSelectOutputs(&outputs);
    return code;
  }

  planDebug("FQ-RemoteCols: refreshed remote output targets, rootType=%d source=%d keyExprs=%d outputs=%d",
            (int)nodeType(pRemoteRoot), (int)outputs.source, outputs.keyExprCount,
            outputs.pSelectExprs ? LIST_LENGTH(outputs.pSelectExprs) : 0);
  nodesDestroyRemoteSelectOutputs(&outputs);
  return TSDB_CODE_SUCCESS;
}

static SNode* fqResolveRemoteMappingExpr(SNode* pExprNode) {
  SNode* pExpr = pExprNode;
  if (NULL == pExpr) {
    return NULL;
  }

  if (QUERY_NODE_TARGET == nodeType(pExpr)) {
    pExpr = ((STargetNode*)pExpr)->pExpr;
  }

  if (NULL != pExpr && QUERY_NODE_GROUPING_SET == nodeType(pExpr)) {
    pExpr = nodesListGetNode(((SGroupingSetNode*)pExpr)->pParameterList, 0);
  }

  // Mapping cardinality and order always follow the final remote output target
  // list exactly.  This helper only unwraps planner-internal wrappers so we can
  // read the concrete result type for that SAME output slot.
  return pExpr;
}

// The physi federated scan only consumes the TOP-LEVEL remote SELECT output.
// Its column-name mappings therefore must follow the same output-name policy as
// the top-level remote renderer:
//   - current-scope output refs expose userAlias when present, otherwise colName
//   - leaf/external columns keep colName
//   - computed expressions expose userAlias
static const char* fqResolveRemoteOutputName(const SNode* pExprNode) {
  const SNode* pExpr = pExprNode;
  if (NULL == pExpr) {
    return NULL;
  }

  if (QUERY_NODE_TARGET == nodeType(pExpr)) {
    pExpr = ((const STargetNode*)pExpr)->pExpr;
  }

  if (NULL != pExpr && QUERY_NODE_GROUPING_SET == nodeType(pExpr)) {
    pExpr = nodesListGetNode(((const SGroupingSetNode*)pExpr)->pParameterList, 0);
  }

  if (NULL == pExpr || !nodesIsExprNode((SNode*)pExpr)) {
    return NULL;
  }

  const SExprNode* pResExpr = (const SExprNode*)pExpr;
  if (QUERY_NODE_COLUMN == nodeType(pExpr)) {
    const SColumnNode* pCol = (const SColumnNode*)pExpr;
    bool isCurrentScopeOutputRef = ('\0' == pCol->tableAlias[0] && '\0' == pCol->tableName[0]);
    if (isCurrentScopeOutputRef) {
      if ('\0' != pResExpr->userAlias[0]) {
        return pResExpr->userAlias;
      }
      if ('\0' != pCol->colName[0]) {
        return pCol->colName;
      }
      return NULL;
    }
    if ('\0' != pCol->colName[0]) {
      return pCol->colName;
    }
  } else {
    if ('\0' != pResExpr->userAlias[0]) {
      return pResExpr->userAlias;
    }
  }

  return NULL;
}
// -----------------------------------------------------------------------------

// fqLogFedScanPhysi - logs the result of createFederatedScanPhysiNode at DEBUG
// level.  Gated by qDebugFlag so no string-building overhead when debug logging
// is off.  Output example:
//   FQ-PHY createFedScan: pScanCols=[time,val] pLeaf.pScanCols=[time,val] pRemoteRoot=Sort numColTypeMappings=2
static void fqLogFedScanPhysi(const SFederatedScanPhysiNode* pScan,
                               const SFederatedScanPhysiNode* pLeaf,
                               const SPhysiNode*              pRemoteRoot) {
  if (!(qDebugFlag & DEBUG_DEBUG)) return;

  // pScanCols (outer Mode-1 node)
  char scanColsBuf[512] = {0};
  int  off = 0;
  if (pScan->pScanCols) {
    SNode* pSc = NULL;
    FOREACH(pSc, pScan->pScanCols) {
      const char* cn = "?";
      if (nodeType(pSc) == QUERY_NODE_COLUMN) cn = ((const SColumnNode*)pSc)->colName;
      else if (nodeType(pSc) == QUERY_NODE_TARGET) {
        const SNode* pi = ((const STargetNode*)pSc)->pExpr;
        if (pi && nodeType(pi) == QUERY_NODE_COLUMN) cn = ((const SColumnNode*)pi)->colName;
      }
      off += snprintf(scanColsBuf + off, sizeof(scanColsBuf) - (size_t)off - 1, "%s,", cn);
      if (off >= (int)sizeof(scanColsBuf) - 2) break;
    }
    if (off > 0 && scanColsBuf[off - 1] == ',') scanColsBuf[off - 1] = '\0';
  } else {
    tstrncpy(scanColsBuf, "(NULL)", sizeof(scanColsBuf));
  }

  // pScanCols (inner Mode-2 leaf - what remote SQL SELECT is built from)
  char leafColsBuf[512] = {0};
  off = 0;
  if (pLeaf->pScanCols) {
    SNode* pSc = NULL;
    FOREACH(pSc, pLeaf->pScanCols) {
      const char* cn = "?";
      if (nodeType(pSc) == QUERY_NODE_COLUMN) cn = ((const SColumnNode*)pSc)->colName;
      off += snprintf(leafColsBuf + off, sizeof(leafColsBuf) - (size_t)off - 1, "%s,", cn);
      if (off >= (int)sizeof(leafColsBuf) - 2) break;
    }
    if (off > 0 && leafColsBuf[off - 1] == ',') leafColsBuf[off - 1] = '\0';
  } else {
    tstrncpy(leafColsBuf, "(NULL)", sizeof(leafColsBuf));
  }

  // pRemoteRoot node type
  const char* rootTypeName = "(null)";
  if (pRemoteRoot) {
    ENodeType rnt = nodeType(pRemoteRoot);
    rootTypeName = (rnt == QUERY_NODE_PHYSICAL_PLAN_SORT)         ? "Sort" :
                   (rnt == QUERY_NODE_PHYSICAL_PLAN_HASH_AGG)     ? "HashAgg" :
                   (rnt == QUERY_NODE_PHYSICAL_PLAN_PROJECT)      ? "Project" :
                   (rnt == QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN) ? "FedScan(leaf)" : "?";
  }

  planDebug("FQ-PHY createFedScan: pScanCols=[%s] pLeaf.pScanCols=[%s] "
            "pRemoteRoot=%s numColTypeMappings=%d",
            scanColsBuf, leafColsBuf, rootTypeName,
            pScan->numColTypeMappings);
}

static bool fqTypeStartsWithIgnoreCase(const char* pTypeName, const char* pPrefix) {
  if (pTypeName == NULL || pPrefix == NULL) {
    return false;
  }

  size_t prefixLen = strlen(pPrefix);
  return prefixLen > 0 && 0 == strncasecmp(pTypeName, pPrefix, prefixLen);
}

static bool fqTypeContainsIgnoreCase(const char* pTypeName, const char* pNeedle) {
  if (pTypeName == NULL || pNeedle == NULL || pNeedle[0] == '\0') {
    return false;
  }

  size_t hayLen = strlen(pTypeName);
  size_t needleLen = strlen(pNeedle);
  if (needleLen > hayLen) {
    return false;
  }

  for (size_t i = 0; i + needleLen <= hayLen; ++i) {
    if (0 == strncasecmp(pTypeName + i, pNeedle, needleLen)) {
      return true;
    }
  }

  return false;
}

static EExtTimestampTzClass fqPlanClassifyTimestampType(int8_t sourceType, const char* pExtTypeName) {
  if (pExtTypeName == NULL || pExtTypeName[0] == '\0') {
    return EXT_TIMESTAMP_TZ_CLASS_UNKNOWN;
  }

  switch ((EExtSourceType)sourceType) {
    case EXT_SOURCE_MYSQL:
      if (fqTypeStartsWithIgnoreCase(pExtTypeName, "timestamp")) {
        return EXT_TIMESTAMP_TZ_CLASS_AWARE;
      }
      if (fqTypeStartsWithIgnoreCase(pExtTypeName, "datetime") ||
          fqTypeStartsWithIgnoreCase(pExtTypeName, "date")) {
        return EXT_TIMESTAMP_TZ_CLASS_NAIVE;
      }
      break;
    case EXT_SOURCE_POSTGRESQL:
      if (fqTypeStartsWithIgnoreCase(pExtTypeName, "timestamptz") ||
          fqTypeContainsIgnoreCase(pExtTypeName, "with time zone")) {
        return EXT_TIMESTAMP_TZ_CLASS_AWARE;
      }
      if (fqTypeStartsWithIgnoreCase(pExtTypeName, "timestamp") ||
          fqTypeStartsWithIgnoreCase(pExtTypeName, "date")) {
        return EXT_TIMESTAMP_TZ_CLASS_NAIVE;
      }
      break;
    case EXT_SOURCE_INFLUXDB:
      if (fqTypeStartsWithIgnoreCase(pExtTypeName, "timestamp")) {
        return EXT_TIMESTAMP_TZ_CLASS_AWARE;
      }
      if (fqTypeStartsWithIgnoreCase(pExtTypeName, "date32") ||
          fqTypeStartsWithIgnoreCase(pExtTypeName, "date64")) {
        return EXT_TIMESTAMP_TZ_CLASS_NAIVE;
      }
      break;
    default:
      break;
  }

  return EXT_TIMESTAMP_TZ_CLASS_UNKNOWN;
}

static int32_t fqBuildTimestampTypeHints(SFederatedScanPhysiNode* pScan, const SExtTableNode* pExtNode) {
  if (pScan == NULL || pExtNode == NULL || pExtNode->pExtMeta == NULL || pExtNode->pExtMeta->pCols == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t hintCount = 0;
  for (int32_t i = 0; i < pExtNode->pExtMeta->numOfCols; ++i) {
    if (fqPlanClassifyTimestampType(pExtNode->sourceType, pExtNode->pExtMeta->pCols[i].extTypeName) !=
        EXT_TIMESTAMP_TZ_CLASS_UNKNOWN) {
      ++hintCount;
    }
  }

  if (hintCount == 0) {
    return TSDB_CODE_SUCCESS;
  }

  pScan->pTimestampTypeHints =
      (SExtTimestampTypeHint*)taosMemoryCalloc(hintCount, sizeof(SExtTimestampTypeHint));
  if (pScan->pTimestampTypeHints == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t hintIdx = 0;
  for (int32_t i = 0; i < pExtNode->pExtMeta->numOfCols; ++i) {
    const SExtColumnDef* pCol = &pExtNode->pExtMeta->pCols[i];
    EExtTimestampTzClass tzClass = fqPlanClassifyTimestampType(pExtNode->sourceType, pCol->extTypeName);
    if (tzClass == EXT_TIMESTAMP_TZ_CLASS_UNKNOWN) {
      continue;
    }

    tstrncpy(pScan->pTimestampTypeHints[hintIdx].colName, pCol->colName,
             sizeof(pScan->pTimestampTypeHints[hintIdx].colName));
    tstrncpy(pScan->pTimestampTypeHints[hintIdx].extTypeName, pCol->extTypeName,
             sizeof(pScan->pTimestampTypeHints[hintIdx].extTypeName));
    pScan->pTimestampTypeHints[hintIdx].tzClass = (int8_t)tzClass;
    planDebug("FQ-TsTypeHints[%d]: source=%s col=%s extType=%s tzClass=%d",
              hintIdx, pExtNode->sourceName, pCol->colName, pCol->extTypeName, (int)tzClass);
    ++hintIdx;
  }

  pScan->numTimestampTypeHints = hintIdx;
  planDebug("FQ-TsTypeHints: source=%s builtHints=%d",
            pExtNode->sourceName, pScan->numTimestampTypeHints);
  return TSDB_CODE_SUCCESS;
}

// PostgreSQL federated scans require an explicit schema at both the planner
// node and connector-config levels. Keep them aligned before executor handoff.
static int32_t fqEnsurePgExtSchemaNode(SExtTableNode* pExtNode) {
  if (NULL == pExtNode || EXT_SOURCE_POSTGRESQL != pExtNode->sourceType) {
    return TSDB_CODE_SUCCESS;
  }

  if ('\0' == pExtNode->schemaName[0] && '\0' != pExtNode->srcSchema[0]) {
    tstrncpy(pExtNode->schemaName, pExtNode->srcSchema, sizeof(pExtNode->schemaName));
  } else if ('\0' == pExtNode->srcSchema[0] && '\0' != pExtNode->schemaName[0]) {
    tstrncpy(pExtNode->srcSchema, pExtNode->schemaName, sizeof(pExtNode->srcSchema));
  }

  if ('\0' == pExtNode->schemaName[0] || '\0' == pExtNode->srcSchema[0]) {
    planError("FQ-PHY: PostgreSQL schema missing source=%s table=%s schemaName=%s srcSchema=%s",
              pExtNode->sourceName,
              pExtNode->table.tableName,
              '\0' != pExtNode->schemaName[0] ? pExtNode->schemaName : "(empty)",
              '\0' != pExtNode->srcSchema[0] ? pExtNode->srcSchema : "(empty)");
    return TSDB_CODE_PLAN_INVALID_FEDERATED_SCAN;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t fqInitFederatedScanSource(SFederatedScanPhysiNode* pScan,
                                         const SExtTableNode* pExtNode) {
  PLAN_ERR_RET(nodesCloneNode((const SNode*)pExtNode, &pScan->pExtTable));
  pScan->sourceType = pExtNode->sourceType;
  tstrncpy(pScan->srcHost, pExtNode->srcHost, sizeof(pScan->srcHost));
  pScan->srcPort = pExtNode->srcPort;
  tstrncpy(pScan->srcUser, pExtNode->srcUser, sizeof(pScan->srcUser));
  tstrncpy(pScan->srcPassword, pExtNode->srcPassword, sizeof(pScan->srcPassword));
  tstrncpy(pScan->srcDatabase, pExtNode->srcDatabase, sizeof(pScan->srcDatabase));
  tstrncpy(pScan->srcSchema, pExtNode->srcSchema, sizeof(pScan->srcSchema));
  tstrncpy(pScan->srcOptions, pExtNode->srcOptions, sizeof(pScan->srcOptions));
  pScan->metaVersion = pExtNode->metaVersion;
  return TSDB_CODE_SUCCESS;
}

// Consume required filters only in the remote-plan copy, after optional pushdown.
static int32_t fqAttachRequiredScanConditions(SLogicNode* pNode, uint32_t* pPushdownFlags) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    SScanLogicNode* pScan = (SScanLogicNode*)pNode;
    if (NULL != pScan->pPushedConditions) {
      if (NULL != pNode->pConditions) {
        code = nodesMergeNode(&pScan->pPushedConditions, &pNode->pConditions);
        QUERY_CHECK_CODE(code, lino, _return);
      }
      TSWAP(pNode->pConditions, pScan->pPushedConditions);
      *pPushdownFlags |= FQ_PUSHDOWN_REQUIRED_FILTER;
    }
  }

  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    code = fqAttachRequiredScanConditions((SLogicNode*)pChild, pPushdownFlags);
    QUERY_CHECK_CODE(code, lino, _return);
  }

_return:
  if (TSDB_CODE_SUCCESS != code) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

// createFederatedScanPhysiNode: builds SFederatedScanPhysiNode from a logic
// node whose scanType == SCAN_TYPE_EXTERNAL.
// ---------------------------------------------------------------------------
static int32_t createFederatedScanPhysiNode(SPhysiPlanContext* pCxt, SSubplan* pSubplan,
                                            SScanLogicNode* pScanLogicNode, SPhysiNode** pPhyNode) {
  int32_t                     code = TSDB_CODE_SUCCESS;
  int32_t                     lino = 0;
  SFederatedScanPhysiNode*    pScan = NULL;
  SNodeList*                  pReordered = NULL;
  SNodeList*                  pReserveSlotCols = NULL;
  SNode*                      pRemoteRoot = NULL;

  if (NULL == pScanLogicNode->pExtTableNode) {
    planError("createFederatedScanPhysiNode: pExtTableNode is NULL");
    PLAN_ERR_JRET(TSDB_CODE_PLAN_INVALID_FEDERATED_SCAN);
  }
  bool isDynamicVstableScan = pScanLogicNode->virtualStableScan && pScanLogicNode->node.dynamicOp;
  if (!isDynamicVstableScan &&
      (NULL == pScanLogicNode->pScanCols || LIST_LENGTH(pScanLogicNode->pScanCols) == 0)) {
    planError("FQ-PHY: logic scan invariant violated, pScanCols empty source=%s table=%s remotePlan=%p",
              ((SExtTableNode*)pScanLogicNode->pExtTableNode)->sourceName,
              pScanLogicNode->tableName.tname,
              pScanLogicNode->pRemoteLogicPlan);
    PLAN_ERR_JRET(TSDB_CODE_PLAN_INVALID_FEDERATED_SCAN);
  }
  if (!isDynamicVstableScan &&
      (NULL == pScanLogicNode->node.pTargets || LIST_LENGTH(pScanLogicNode->node.pTargets) == 0)) {
    planError("FQ-PHY: logic scan invariant violated, node targets empty source=%s table=%s scanCols=%d",
              ((SExtTableNode*)pScanLogicNode->pExtTableNode)->sourceName,
              pScanLogicNode->tableName.tname,
              (int)LIST_LENGTH(pScanLogicNode->pScanCols));
    PLAN_ERR_JRET(TSDB_CODE_PLAN_INVALID_FEDERATED_SCAN);
  }
  SExtTableNode* pExtNode = (SExtTableNode*)pScanLogicNode->pExtTableNode;
  PLAN_ERR_JRET(fqEnsurePgExtSchemaNode(pExtNode));
  bool useRemoteLogicPlan =
      (NULL != pScanLogicNode->pRemoteLogicPlan) &&
      !fqShouldUseAnalyzeInfluxScalarAggFallback(pCxt, pScanLogicNode);
  // Stream %%trows readers materialize source rows and run their calc plan locally.
  bool useRemoteResultSchema = useRemoteLogicPlan && !inStreamCalcClause(pCxt->pPlanCxt);

  pScan = (SFederatedScanPhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pScanLogicNode,
                                                  QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN);
  if (NULL == pScan) {
    planError("createFederatedScanPhysiNode: makePhysiNode OOM");
    PLAN_ERR_JRET(terrno);
  }
  PLAN_ERR_JRET(fqInitFederatedScanSource(pScan, pExtNode));

  // dynamicOp external scan nodes (VStb path) have NULL pTargets/pScanCols;
  // DynQueryCtrl fills them at runtime after the common source initialization.
  if (isDynamicVstableScan) {
    pScan->scanRange = pScanLogicNode->scanRange;
    PLAN_ERR_JRET(nodesCloneList(pScanLogicNode->pScanCols, &pScan->pScanCols));
    fqSetFederatedScanTimezone(pCxt, pScan);
    pCxt->hasFederatedScan = true;
    *pPhyNode = (SPhysiNode*)pScan;
    pScan = NULL;
    goto _return;
  }

  // When pRemoteLogicPlan is set, the remote connector executes the full
  // pushed-down plan and returns exactly the topmost node's output columns
  // (pTargets).  The executor rebuilds pColTypeMappings from the physi-plan
  // topmost node's pTargets (SSortPhysiNode.pTargets / SProjectPhysiNode.pProjections).
  // We do NOT rebuild pOutputDataBlockDesc here because it uses pCxt's
  // dataBlockId registry and downstream slot-ID resolution depends on it.

  // Clone scan columns in pTargets order.
  // makePhysiNode() already built the output data block descriptor from
  // pLogicNode->pTargets (which may have been reordered by
  // eliminateProjOptimize to match the user's SELECT list).  The output desc
  // slot order = pTargets order.  We must ensure pScanCols, pColTypeMappings,
  // and the remote SQL SELECT list all use the same order so that the data
  // block columns returned by the connector match the slot descriptors.
  //
  // pScanCols (from nodesCollectColumns) may differ in order from pTargets
  // because the AST walker visits ORDER BY before PROJECTION.  Reorder the
  // clone to match pTargets.
  {
    bool       reorderOk  = true;

    if (pScanLogicNode->node.pTargets != NULL && pScanLogicNode->pScanCols != NULL) {
      SNode* pTarget = NULL;
      FOREACH(pTarget, pScanLogicNode->node.pTargets) {
        // Unwrap STargetNode wrapper if present
        const SNode* pTgtInner = pTarget;
        if (nodeType(pTgtInner) == QUERY_NODE_TARGET) {
          pTgtInner = ((const STargetNode*)pTgtInner)->pExpr;
        }
        if (NULL == pTgtInner || nodeType(pTgtInner) != QUERY_NODE_COLUMN) {
          reorderOk = false;
          break;
        }
        const SColumnNode* pTgtCol = (const SColumnNode*)pTgtInner;
        bool               found   = false;
        SNode*             pScanCol = NULL;
        FOREACH(pScanCol, pScanLogicNode->pScanCols) {
          SColumnNode* pSCol = (SColumnNode*)pScanCol;
          if ((pSCol->colId != 0 && pSCol->colId == pTgtCol->colId) ||
              (pSCol->colId == 0 && 0 == strcmp(pSCol->colName, pTgtCol->colName))) {
            SNode* pClone = NULL;
            PLAN_ERR_JRET(nodesCloneNode(pScanCol, &pClone));
            PLAN_ERR_JRET(nodesListMakeStrictAppend(&pReordered, pClone));
            found = true;
            break;
          }
        }
        if (!found || !reorderOk) { reorderOk = false; break; }
      }
      if (!reorderOk || pReordered == NULL ||
          LIST_LENGTH(pReordered) != LIST_LENGTH(pScanLogicNode->node.pTargets)) {
        NODES_DESTORY_LIST(pReordered);
      } else {
        // Reorder succeeded. Append any extra pScanCols columns that are NOT in
        // pTargets (e.g. primary-key columns added for WINDOW JOIN addPrimEqCond
        // slot resolution that are absent from the SELECT list).  These must be
        // present in the slot hash so that createMergeJoinPhysiNode can resolve
        // addPrimEqCond column slot IDs without PLAN_SLOT_NOT_FOUND.
        SNode* pSc = NULL;
        FOREACH(pSc, pScanLogicNode->pScanCols) {
          if (QUERY_NODE_COLUMN != nodeType(pSc)) continue;
          SColumnNode* pSCol = (SColumnNode*)pSc;
          bool alreadyIn = false;
          SNode* pEx = NULL;
          FOREACH(pEx, pReordered) {
            if (QUERY_NODE_COLUMN != nodeType(pEx)) continue;
            SColumnNode* pExCol = (SColumnNode*)pEx;
            if ((pExCol->colId != 0 && pExCol->colId == pSCol->colId) ||
                (pExCol->colId == 0 && strcmp(pExCol->colName, pSCol->colName) == 0 &&
                 strcmp(pExCol->tableAlias, pSCol->tableAlias) == 0)) {
              alreadyIn = true;
              break;
            }
          }
          if (!alreadyIn) {
            SNode* pClone = NULL;
            PLAN_ERR_JRET(nodesCloneNode(pSc, &pClone));
            PLAN_ERR_JRET(nodesListMakeStrictAppend(&pReordered, pClone));
          }
        }
      }
    }

    if (pReordered != NULL) {
      pScan->pScanCols = pReordered;
      pReordered = NULL;
    } else {
      // Fallback: clone in original pScanCols order
      PLAN_ERR_JRET(nodesCloneList(pScanLogicNode->pScanCols, &pScan->pScanCols));
    }
  }

  // Build pColTypeMappings from the same single source used by remote SQL
  // rendering: the top-level remote plan's pTargets.  Only when there is no
  // remote plan at all do we fall back to the local scan columns.  If a remote
  // plan exists but its top-level outputs are still empty, that is a real plan
  // inconsistency and must fail loudly; otherwise the descriptor, pTargets and
  // remote SQL output order will diverge.
  {
    SNodeList* pOutputCols = pScan->pScanCols;  // default: no pushed plan
    const char* pOutputColsSource = "pScanCols";
    if (useRemoteResultSchema) {
      PLAN_ERR_JRET(
          fqAlignRemotePassthroughOutputs(pScanLogicNode->pRemoteLogicPlan, pScan->pScanCols));
      PLAN_ERR_JRET(fqRefreshRemoteOutputTargets(pScanLogicNode->pRemoteLogicPlan));

      SNodeList* pRemoteTargets = fqGetRemoteOutputTargets(pScanLogicNode->pRemoteLogicPlan);
      planDebug("FQ-ColMappings: inspect top remote targets, remoteRootType=%d targets=%p len=%d",
                (int)nodeType((SNode*)pScanLogicNode->pRemoteLogicPlan), pRemoteTargets,
                pRemoteTargets ? LIST_LENGTH(pRemoteTargets) : 0);
      if (pRemoteTargets != NULL && LIST_LENGTH(pRemoteTargets) > 0) {
        pOutputCols = pRemoteTargets;
        pOutputColsSource = "pRemotePlan->pTargets";
      } else {
        planError("FQ-ColMappings: remote plan exists but top remote targets are empty, rootType=%d scanCols=%d",
                  (int)nodeType((SNode*)pScanLogicNode->pRemoteLogicPlan),
                  pScan->pScanCols ? LIST_LENGTH(pScan->pScanCols) : 0);
        PLAN_ERR_JRET(TSDB_CODE_PLAN_INVALID_FEDERATED_SCAN);
      }
    }

    int32_t numCols = pOutputCols ? LIST_LENGTH(pOutputCols) : 0;
    int32_t outputIdx = 0;
    SNode* pExprNode = NULL;
    FOREACH(pExprNode, pOutputCols) {
      SNode* pExpr = fqResolveRemoteMappingExpr(pExprNode);
      if (NULL == pExpr) {
        planError("FQ-ColMappings: outputIdx=%d has NULL mapping expr, source=%s exprNodeType=%d",
                  outputIdx, pOutputColsSource, (int)nodeType(pExprNode));
        PLAN_ERR_JRET(TSDB_CODE_PLAN_INVALID_FEDERATED_SCAN);
      }
      planDebug("FQ-ColMappings: count outputIdx=%d exprNodeType=%d exprType=%d includeInMappings=1",
                outputIdx, (int)nodeType(pExprNode), (int)nodeType(pExpr));
      ++outputIdx;
    }

    planDebug("FQ-ColMappings: source=%s outputLen=%d mappingCols=%d",
              pOutputColsSource, pOutputCols ? LIST_LENGTH(pOutputCols) : 0, numCols);
    if (numCols > 0) {
      pScan->pColTypeMappings =
          (SExtColTypeMapping*)taosMemoryCalloc(numCols, sizeof(SExtColTypeMapping));
      if (NULL == pScan->pColTypeMappings) {
        PLAN_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
      }
      pScan->numColTypeMappings = numCols;

      int32_t colIdx = 0;
      outputIdx = 0;
      FOREACH(pExprNode, pOutputCols) {
        SNode* pExpr = fqResolveRemoteMappingExpr(pExprNode);
        if (NULL == pExpr) {
          planError("FQ-ColMappings: outputIdx=%d resolved NULL expr during fill, source=%s exprNodeType=%d",
                    outputIdx, pOutputColsSource, (int)nodeType(pExprNode));
          PLAN_ERR_JRET(TSDB_CODE_PLAN_INVALID_FEDERATED_SCAN);
        }

        planDebug("FQ-ColMappings[%d]: outputIdx=%d exprNodeType=%d exprType=%d type=%d precision=%d scale=%d bytes=%d",
                  colIdx, outputIdx, (int)nodeType(pExprNode), (int)nodeType(pExpr),
                  (int)((SExprNode*)pExpr)->resType.type, (int)((SExprNode*)pExpr)->resType.precision,
                  (int)((SExprNode*)pExpr)->resType.scale, (int)((SExprNode*)pExpr)->resType.bytes);
        pScan->pColTypeMappings[colIdx].tdType = ((SExprNode*)pExpr)->resType;
        const char* pOutputName = fqResolveRemoteOutputName(pExprNode);
        if (NULL != pOutputName && '\0' != pOutputName[0]) {
          tstrncpy(pScan->pColTypeMappings[colIdx].colName, pOutputName,
                   sizeof(pScan->pColTypeMappings[colIdx].colName));
        }
        planDebug("FQ-ColMappings[%d]: outputIdx=%d outputName=%s source=%s",
                  colIdx, outputIdx,
                  pScan->pColTypeMappings[colIdx].colName[0] != '\0' ? pScan->pColTypeMappings[colIdx].colName : "(empty)",
                  pOutputColsSource);
        ++colIdx;
        ++outputIdx;
      }

      if (colIdx != numCols) {
        planError("FQ-ColMappings: filled mapping count mismatch, source=%s expected=%d actual=%d",
                  pOutputColsSource, numCols, colIdx);
        PLAN_ERR_JRET(TSDB_CODE_PLAN_INVALID_FEDERATED_SCAN);
      }
    }
  }

  PLAN_ERR_JRET(fqBuildTimestampTypeHints(pScan, pExtNode));
  // Propagate two-pass mode only when the repeat-scan aggregate still runs locally.
  // If the remote subtree already contains the repeat-scan AGG, FederatedScan must
  // return the final remote rows once, not PRE_SCAN + MAIN_SCAN duplicates.
  bool requestTwoPass = (pScanLogicNode->scanSeq[0] > 1);
  bool remoteOwnsRepeatScan =
      useRemoteLogicPlan && fqRemotePlanHasRepeatScanAgg((const SLogicNode*)pScanLogicNode->pRemoteLogicPlan);
  pScan->twoPassMode = requestTwoPass && !remoteOwnsRepeatScan;
  planDebug("FQ-PhysiFedScan: source=%s scanSeq0=%d requestTwoPass=%d remoteOwnsRepeatScan=%d finalTwoPass=%d"
            " useRemoteLogicPlan=%d remoteRootType=%d",
            pExtNode->sourceName, (int)pScanLogicNode->scanSeq[0], (int)requestTwoPass,
            (int)remoteOwnsRepeatScan, (int)pScan->twoPassMode, (int)useRemoteLogicPlan,
            pScanLogicNode->pRemoteLogicPlan ? (int)nodeType((SNode*)pScanLogicNode->pRemoteLogicPlan) : -1);
  // Copy client timezone name so the Executor can reconstruct the timezone_t via tzalloc().
  // Used by the Connector to convert timezone-naive source timestamps (MySQL DATETIME,
  // PG timestamp without time zone, etc.) using the correct client timezone.
  fqSetFederatedScanTimezone(pCxt, pScan);

  // Add output data block slots.
  // When pRemoteLogicPlan exists, FedScan executes the remote plan and returns its
  // top-level output.  The output descriptor must reflect what the remote plan actually
  // produces, not the original scan columns.  Only when pRemoteLogicPlan is NULL (pure
  // scan, no pushdown) do we fall back to pScanCols.  A non-NULL remote plan with an
  // empty output list is a real inconsistency and must fail loudly.
  SNodeList* pSlotCols = pScan->pScanCols;  // default: no remote plan
  {
    if (useRemoteResultSchema) {
      PLAN_ERR_JRET(fqRefreshRemoteOutputTargets(pScanLogicNode->pRemoteLogicPlan));

      SNodeList* pRemoteTargets = fqGetRemoteOutputTargets(pScanLogicNode->pRemoteLogicPlan);
      planDebug("FQ-Slots: inspect top remote targets for FedScan output descriptor, type=%d len=%d",
                (int)nodeType((SNode*)pScanLogicNode->pRemoteLogicPlan),
                pRemoteTargets ? LIST_LENGTH(pRemoteTargets) : 0);
      if (pRemoteTargets != NULL && LIST_LENGTH(pRemoteTargets) > 0) {
        pSlotCols = pRemoteTargets;
      } else {
        planError("FQ-Slots: remote plan exists but top remote targets are empty, rootType=%d scanCols=%d",
                  (int)nodeType((SNode*)pScanLogicNode->pRemoteLogicPlan),
                  pScan->pScanCols ? LIST_LENGTH(pScan->pScanCols) : 0);
        PLAN_ERR_JRET(TSDB_CODE_PLAN_INVALID_FEDERATED_SCAN);
      }
    }
    // When pRemoteLogicPlan is non-NULL, the FedScan output descriptor must be
    // rebuilt from scratch in remote-result order, then local-only dependency
    // columns are appended as reserve slots. Reusing makePhysiNode's initial
    // descriptor leaks the local scan-target order into output slots and can
    // swap visible/hidden columns for partial-project pushdown.
    if (useRemoteLogicPlan && pSlotCols != pScan->pScanCols && pScan->pScanCols != NULL) {
      PLAN_ERR_JRET(nodesCloneList(pScan->pScanCols, &pReserveSlotCols));
      planDebug("FQ-Slots: rebuilt FedScan descriptor from remote targets (len=%d) plus reserve scanCols (len=%d)",
                (int)LIST_LENGTH(pSlotCols), (int)LIST_LENGTH(pReserveSlotCols));
      PLAN_ERR_JRET(
          fqRebuildDataBlockDesc(pCxt, pScan->node.pOutputDataBlockDesc, pSlotCols, pReserveSlotCols));
    } else {
      PLAN_ERR_JRET(addDataBlockSlots(pCxt, pSlotCols, pScan->node.pOutputDataBlockDesc));
    }
  }

  // Add pseudo-column slots (e.g. TBNAME for PARTITION BY TBNAME on InfluxDB).
  // Normal table scans handle this via pScanPseudoCols in createSimpleScanPhysiNode.
  // Without this, PARTITION BY TBNAME on a federated scan crashes: the partition
  // operator's group-key column references a slot that doesn't exist in the data
  // block, causing a type mismatch and buffer overflow in doHashPartition.
  if (NULL != pScanLogicNode->pScanPseudoCols) {
    PLAN_ERR_JRET(
        addDataBlockSlots(pCxt, pScanLogicNode->pScanPseudoCols, pScan->node.pOutputDataBlockDesc));
  }

  if (pReserveSlotCols != NULL) {
    nodesDestroyList(pReserveSlotCols);
    pReserveSlotCols = NULL;
  }
  // Set correct slot IDs in the cloned conditions using setNodeSlotId.
  // Only columns that are present in the output/reserve slot descriptor can be
  // resolved here; conditions pushed to the remote side are handled remotely.
  if (pScanLogicNode->node.pConditions != NULL) {
    PLAN_ERR_JRET(setNodeSlotId(pCxt, pScan->node.pOutputDataBlockDesc->dataBlockId, -1,
                                pScanLogicNode->node.pConditions, &pScan->node.pConditions));
    planDebug("FQ-PHY FedScan pConditions: setNodeSlotId code=0x%x pConditions=%p",
              code, pScan->node.pConditions);
  } else {
    planDebug("FQ-PHY FedScan pConditions: logic pConditions is NULL (already pushed or none)");
  }

  // Force MERGE subplan type to avoid DATA_SRC_EP_MISS (external has no vnode)
  pSubplan->subplanType = SUBPLAN_TYPE_MERGE;
  pSubplan->msgType     = TDMT_SCH_MERGE_QUERY;

  // * Mark the physical plan as containing a federated scan
  pCxt->hasFederatedScan = true;

  // -- Build the remote plan as a LOGIC tree (pure clone - no physi conversion) --
  //         .pScanCols    -> flat leaf SELECT list (must already be non-empty)
  //         .pConditions  -> WHERE clause
  //         .node.pLimit  -> LIMIT / OFFSET

  // pRemotePlan is a logic-node subtree: nodesRemotePlanToSQL renders it directly
  // (it reads only column names / expressions / functions - never physi slot ids),
  // and TLV ships it to the server which re-renders.  Two cases:
  //   (a) pRemoteLogicPlan set (FqPushdown collected a chain): clone it verbatim -
  //       it already carries its own logic SCAN leaf (cloned by fqCommitPushChain).
  //   (b) pRemoteLogicPlan NULL: build a bare logic SCAN leaf carrying the remote
  //       FROM/SELECT/WHERE/LIMIT so the connector runs a flat "SELECT ... FROM ...".
  planDebug("FQ-PHY: pScanLogicNode=%p pRemoteLogicPlan=%p type=%d useRemoteLogicPlan=%d", pScanLogicNode,
            pScanLogicNode->pRemoteLogicPlan,
            pScanLogicNode->pRemoteLogicPlan ? (int)nodeType(pScanLogicNode->pRemoteLogicPlan) : -1,
            (int)useRemoteLogicPlan);
  if (useRemoteLogicPlan) {
    PLAN_ERR_JRET(nodesCloneNode(pScanLogicNode->pRemoteLogicPlan, &pRemoteRoot));
    planDebug("FQ-PHY: after clone pRemoteRoot=%p type=%d code=0x%x",
              pRemoteRoot, pRemoteRoot ? (int)nodeType(pRemoteRoot) : -1, code);
    if (pRemoteRoot != NULL) {
      SLogicNode* pRemoteLogicRoot = (SLogicNode*)pRemoteRoot;
      planDebug("FQ-PHY: cloned remote root detail type=%d nChildren=%d targets=%d limit=%p",
                (int)nodeType(pRemoteRoot),
                pRemoteLogicRoot->pChildren ? LIST_LENGTH(pRemoteLogicRoot->pChildren) : 0,
                pRemoteLogicRoot->pTargets ? LIST_LENGTH(pRemoteLogicRoot->pTargets) : 0,
                pRemoteLogicRoot->pLimit);
      SScanLogicNode* pRemoteLeaf = fqFindExternalLogicLeafScan((SLogicNode*)pRemoteRoot);
      planDebug("FQ-PHY: cloned remote leaf detail leaf=%p hasCond=%d scanCols=%d targets=%d remotePlan=%p source=%s table=%s",
                pRemoteLeaf, (NULL != pRemoteLeaf && NULL != pRemoteLeaf->node.pConditions),
                (NULL != pRemoteLeaf && NULL != pRemoteLeaf->pScanCols) ? LIST_LENGTH(pRemoteLeaf->pScanCols) : 0,
                (NULL != pRemoteLeaf && NULL != pRemoteLeaf->node.pTargets) ? LIST_LENGTH(pRemoteLeaf->node.pTargets) : 0,
                NULL != pRemoteLeaf ? pRemoteLeaf->pRemoteLogicPlan : NULL,
                (NULL != pRemoteLeaf && NULL != pRemoteLeaf->pExtTableNode)
                    ? ((SExtTableNode*)pRemoteLeaf->pExtTableNode)->sourceName
                    : "(null)",
                (NULL != pRemoteLeaf && NULL != pRemoteLeaf->pExtTableNode)
                    ? ((SExtTableNode*)pRemoteLeaf->pExtTableNode)->table.tableName
                    : "(null)");
    }
    PLAN_ERR_JRET(fqAlignRemotePassthroughOutputs(pRemoteRoot, pScan->pScanCols));
    if (pRemoteRoot != NULL) {
      SLogicNode* pRemoteLogicRoot = (SLogicNode*)pRemoteRoot;
      planDebug("FQ-PHY: aligned remote root detail type=%d nChildren=%d targets=%d limit=%p",
                (int)nodeType(pRemoteRoot),
                pRemoteLogicRoot->pChildren ? LIST_LENGTH(pRemoteLogicRoot->pChildren) : 0,
                pRemoteLogicRoot->pTargets ? LIST_LENGTH(pRemoteLogicRoot->pTargets) : 0,
                pRemoteLogicRoot->pLimit);
    }
    PLAN_ERR_JRET(fqRefreshRemoteOutputTargets(pRemoteRoot));
    if (pRemoteRoot != NULL) {
      SLogicNode* pRemoteLogicRoot = (SLogicNode*)pRemoteRoot;
      planDebug("FQ-PHY: refreshed remote output targets detail type=%d nChildren=%d targets=%d limit=%p",
                (int)nodeType(pRemoteRoot),
                pRemoteLogicRoot->pChildren ? LIST_LENGTH(pRemoteLogicRoot->pChildren) : 0,
                pRemoteLogicRoot->pTargets ? LIST_LENGTH(pRemoteLogicRoot->pTargets) : 0,
                pRemoteLogicRoot->pLimit);
      SScanLogicNode* pRemoteLeaf = fqFindExternalLogicLeafScan((SLogicNode*)pRemoteRoot);
      planDebug("FQ-PHY: refreshed remote leaf detail leaf=%p hasCond=%d scanCols=%d targets=%d remotePlan=%p source=%s table=%s",
                pRemoteLeaf, (NULL != pRemoteLeaf && NULL != pRemoteLeaf->node.pConditions),
                (NULL != pRemoteLeaf && NULL != pRemoteLeaf->pScanCols) ? LIST_LENGTH(pRemoteLeaf->pScanCols) : 0,
                (NULL != pRemoteLeaf && NULL != pRemoteLeaf->node.pTargets) ? LIST_LENGTH(pRemoteLeaf->node.pTargets) : 0,
                NULL != pRemoteLeaf ? pRemoteLeaf->pRemoteLogicPlan : NULL,
                (NULL != pRemoteLeaf && NULL != pRemoteLeaf->pExtTableNode)
                    ? ((SExtTableNode*)pRemoteLeaf->pExtTableNode)->sourceName
                    : "(null)",
                (NULL != pRemoteLeaf && NULL != pRemoteLeaf->pExtTableNode)
                    ? ((SExtTableNode*)pRemoteLeaf->pExtTableNode)->table.tableName
                    : "(null)");
    }
  } else {
    PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SCAN, &pRemoteRoot));
    SScanLogicNode* pLeaf = (SScanLogicNode*)pRemoteRoot;
    pLeaf->scanType = SCAN_TYPE_EXTERNAL;
    // FROM: clone the external table node (table identity for SQL generation).
    PLAN_ERR_JRET(nodesCloneNode(pScanLogicNode->pExtTableNode, &pLeaf->pExtTableNode));
    // SELECT: clone the de-duplicated column set the optimizer collected.
    // Use pScan->pScanCols (already reordered to pTargets order above), NOT the
    // raw pScanLogicNode->pScanCols.  The remote SQL SELECT list rendered from
    // this leaf MUST match the column order of pColTypeMappings (built from
    // pScan->pScanCols below); otherwise the connector decodes result column N
    // with mapping[N] of a different column -> 0x6418 (e.g. "unexpected OID 701
    // for TIMESTAMP column 0").  nodesCollectColumns visits WHERE-subquery /
    // ORDER BY columns before the projection, so the raw order diverges from
    // pTargets only when a WHERE subquery (REMOTE_*) is present - exactly the
    // failing case.
    PLAN_ERR_JRET(nodesCloneList(pScan->pScanCols, &pLeaf->pScanCols));
    // Optional query conditions stay local in this fallback. Saved binding filters
    // still belong to the remote leaf, even when no optimizer rule runs.
    code = nodesCloneNode(pScanLogicNode->pPushedConditions, &pLeaf->pPushedConditions);
    QUERY_CHECK_CODE(code, lino, _return);
    // LIMIT: makePhysiNode moved pScanLogicNode->pLimit to pScan->node.pLimit.
    if (pScan->node.pLimit != NULL) {
      PLAN_ERR_JRET(nodesCloneNode(pScan->node.pLimit, &pLeaf->node.pLimit));
    }
    planDebug("FQ-PHY: built fallback bare remote scan leaf=%p scanCols=%d targets=%d hasCond=%d hasLimit=%d",
              pLeaf, pLeaf->pScanCols ? LIST_LENGTH(pLeaf->pScanCols) : 0,
              pLeaf->node.pTargets ? LIST_LENGTH(pLeaf->node.pTargets) : 0,
              NULL != pLeaf->node.pConditions, NULL != pLeaf->node.pLimit);
  }

  if (pRemoteRoot != NULL) {
    code = fqAttachRequiredScanConditions((SLogicNode*)pRemoteRoot, &pScan->pushdownFlags);
    QUERY_CHECK_CODE(code, lino, _return);
    PLAN_ERR_JRET(fqRefreshRemoteOutputTargets(pRemoteRoot));

    SLogicNode* pRemoteLogicRoot = (SLogicNode*)pRemoteRoot;
    planDebug("FQ-PHY: finalized remote root detail type=%d nChildren=%d targets=%d hasCond=%d limit=%p",
              (int)nodeType(pRemoteRoot),
              pRemoteLogicRoot->pChildren ? LIST_LENGTH(pRemoteLogicRoot->pChildren) : 0,
              pRemoteLogicRoot->pTargets ? LIST_LENGTH(pRemoteLogicRoot->pTargets) : 0,
              NULL != pRemoteLogicRoot->pConditions, pRemoteLogicRoot->pLimit);
    SScanLogicNode* pRemoteLeaf = fqFindExternalLogicLeafScan((SLogicNode*)pRemoteRoot);
    planDebug("FQ-PHY: finalized remote leaf detail leaf=%p hasCond=%d scanCols=%d targets=%d remotePlan=%p source=%s table=%s",
              pRemoteLeaf, (NULL != pRemoteLeaf && NULL != pRemoteLeaf->node.pConditions),
              (NULL != pRemoteLeaf && NULL != pRemoteLeaf->pScanCols) ? LIST_LENGTH(pRemoteLeaf->pScanCols) : 0,
              (NULL != pRemoteLeaf && NULL != pRemoteLeaf->node.pTargets) ? LIST_LENGTH(pRemoteLeaf->node.pTargets) : 0,
              NULL != pRemoteLeaf ? pRemoteLeaf->pRemoteLogicPlan : NULL,
              (NULL != pRemoteLeaf && NULL != pRemoteLeaf->pExtTableNode)
                  ? ((SExtTableNode*)pRemoteLeaf->pExtTableNode)->sourceName
                  : "(null)",
              (NULL != pRemoteLeaf && NULL != pRemoteLeaf->pExtTableNode)
                  ? ((SExtTableNode*)pRemoteLeaf->pExtTableNode)->table.tableName
                  : "(null)");
  }

  // Attach the inner pRemotePlan (logic) tree to the outer Mode-1 wrapper.
  pScan->pRemotePlan = pRemoteRoot;
  pRemoteRoot = NULL;

  planDebug("FQ-PHY: FedScan=%p pRemotePlan=%p type=%d",
            pScan, pScan->pRemotePlan,
            pScan->pRemotePlan ? (int)nodeType(pScan->pRemotePlan) : -1);
  *pPhyNode = (SPhysiNode*)pScan;
  pScan = NULL;

_return:
  nodesDestroyList(pReordered);
  nodesDestroyList(pReserveSlotCols);
  nodesDestroyNode(pRemoteRoot);
  nodesDestroyNode((SNode*)pScan);
  if (TSDB_CODE_SUCCESS != code) {
    planError("%s failed since %s", __func__, tstrerror(code));
  }
  return code;
}

static int32_t createScanPhysiNode(SPhysiPlanContext* pCxt, SSubplan* pSubplan, SScanLogicNode* pScanLogicNode,
                                   SPhysiNode** pPhyNode) {

  switch (pScanLogicNode->scanType) {
    case SCAN_TYPE_TAG:
      PLAN_ERR_RET(createTagScanPhysiNode(pCxt, pSubplan, pScanLogicNode, pPhyNode));
      break;
    case SCAN_TYPE_BLOCK_INFO:
      PLAN_ERR_RET(createSimpleScanPhysiNode(pCxt, pSubplan, pScanLogicNode, pPhyNode));
      break;
    case SCAN_TYPE_TABLE_COUNT:
      PLAN_ERR_RET(createTableCountScanPhysiNode(pCxt, pSubplan, pScanLogicNode, pPhyNode));
      break;
    case SCAN_TYPE_LAST_ROW:
      PLAN_ERR_RET(createLastRowScanPhysiNode(pCxt, pSubplan, pScanLogicNode, pPhyNode));
      break;
    case SCAN_TYPE_TABLE:
      PLAN_ERR_RET(createTableScanPhysiNode(pCxt, pSubplan, pScanLogicNode, pPhyNode));
      break;
    case SCAN_TYPE_SYSTEM_TABLE:
      PLAN_ERR_RET(createSystemTableScanPhysiNode(pCxt, pSubplan, pScanLogicNode, pPhyNode));
      break;
    case SCAN_TYPE_STREAM:
      PLAN_ERR_RET(createStreamScanPhysiNode(pCxt, pSubplan, pScanLogicNode, pPhyNode));
      break;
    case SCAN_TYPE_TABLE_MERGE:
      PLAN_ERR_RET(createTableMergeScanPhysiNode(pCxt, pSubplan, pScanLogicNode, pPhyNode));
      break;
    case SCAN_TYPE_EXTERNAL:
      PLAN_ERR_RET(createFederatedScanPhysiNode(pCxt, pSubplan, pScanLogicNode, pPhyNode));
      break;
    default:
      PLAN_ERR_RET(generateUsageErrMsg(pCxt->pPlanCxt->pMsg, pCxt->pPlanCxt->msgLen, TSDB_CODE_PLAN_INTERNAL_ERROR,
                                       "invalid scan type:%d", pScanLogicNode->scanType));
      break;
  }

  if (inStreamTriggerClause(pCxt->pPlanCxt) && !pCxt->pPlanCxt->streamCxt.triggerScanSubplan) {
    pCxt->pPlanCxt->streamCxt.triggerScanSubplan = (SNode*)pSubplan;
  }
  if (inStreamCalcClause(pCxt->pPlanCxt)) {
    SStreamCalcScan pStreamCalcScan = {0};
    pStreamCalcScan.vgList = taosArrayInit(1, sizeof(int32_t));
    // External-source scans have no vgroup; execNode.nodeId is 0 (unset).
    // Use SNODE_HANDLE as a sentinel so mnode can distinguish ext-source calc
    // readers from normal vnode readers and assign the real snodeId / epset.
    int32_t calcNodeId = (pScanLogicNode->scanType == SCAN_TYPE_EXTERNAL)
                             ? SNODE_HANDLE
                             : pSubplan->execNode.nodeId;
    if (NULL == taosArrayPush(pStreamCalcScan.vgList, &calcNodeId)) {
      PLAN_ERR_RET(terrno);
    }
    pStreamCalcScan.scanPlan = (void*)pSubplan;
    // Federated calc: record this scan's external source/table identity so the
    // mnode can bind each calc reader to the correct per-source spec when the
    // calc query JOINs multiple external tables/sources. tsColumn is resolved
    // here (from pExtMeta) because pExtMeta is runtime-only and not serialized.
    if (pScanLogicNode->scanType == SCAN_TYPE_EXTERNAL && pScanLogicNode->pExtTableNode != NULL) {
      SExtTableNode* pExt = (SExtTableNode*)pScanLogicNode->pExtTableNode;
      tstrncpy(pStreamCalcScan.sourceName, pExt->sourceName, sizeof(pStreamCalcScan.sourceName));
      const char* remoteTbl =
          (pExt->remoteTableName[0] != '\0') ? pExt->remoteTableName : pExt->table.tableName;
      tstrncpy(pStreamCalcScan.extTable, remoteTbl, sizeof(pStreamCalcScan.extTable));
      if (pExt->tsPrimaryColIdx >= 0 && pExt->pExtMeta != NULL &&
          pExt->tsPrimaryColIdx < pExt->pExtMeta->numOfCols && pExt->pExtMeta->pCols != NULL) {
        tstrncpy(pStreamCalcScan.tsColumn, pExt->pExtMeta->pCols[pExt->tsPrimaryColIdx].colName,
                 sizeof(pStreamCalcScan.tsColumn));
      }
    }
    if (pScanLogicNode->placeholderType == SP_PARTITION_ROWS) {
      pStreamCalcScan.readFromCache = true;
    }
    if (NULL == taosArrayPush(pCxt->pPlanCxt->streamCxt.calcScanPlanArray, &pStreamCalcScan)) {
      PLAN_ERR_RET(terrno);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t getJoinDataBlockDescNode(SNodeList* pChildren, int32_t idx, SDataBlockDescNode** ppDesc) {
  if (2 == pChildren->length) {
    *ppDesc = ((SPhysiNode*)nodesListGetNode(pChildren, idx))->pOutputDataBlockDesc;
  } else if (1 == pChildren->length &&
             nodeType(nodesListGetNode(pChildren, 0)) == QUERY_NODE_PHYSICAL_PLAN_GROUP_CACHE) {
    SGroupCachePhysiNode* pGrpCache = (SGroupCachePhysiNode*)nodesListGetNode(pChildren, 0);
    *ppDesc = ((SPhysiNode*)nodesListGetNode(pGrpCache->node.pChildren, idx))->pOutputDataBlockDesc;
  } else {
    planError("Invalid join children num:%d or child type:%d", pChildren->length,
              nodeType(nodesListGetNode(pChildren, 0)));
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t setColEqList(SNode* pEqCond, int64_t leftBlkId, int64_t rightBlkId, SNodeList** ppLeft,
                            SNodeList** ppRight) {
  int32_t code = 0;
  if (QUERY_NODE_OPERATOR == nodeType(pEqCond) && ((SOperatorNode*)pEqCond)->opType == OP_TYPE_EQUAL) {
    SOperatorNode* pOp = (SOperatorNode*)pEqCond;
    SNode*         pNew = NULL;
    if (QUERY_NODE_COLUMN != nodeType(pOp->pLeft) || QUERY_NODE_COLUMN != nodeType(pOp->pRight)) {
      planError("invalid col equal list node type, leftType:%d, rightType:%d", nodeType(pOp->pLeft), nodeType(pOp->pRight));
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }
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
      planError("invalid col equal list, leftBlockId:%" PRId64, ((SColumnNode*)pOp->pLeft)->dataBlockId);
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }
    if (TSDB_CODE_SUCCESS == code) {
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
        planError("invalid col equal list, rightBlockId:%" PRId64, ((SColumnNode*)pOp->pRight)->dataBlockId);
        return TSDB_CODE_PLAN_INTERNAL_ERROR;
      }
    }
  } else if (QUERY_NODE_LOGIC_CONDITION == nodeType(pEqCond) &&
             ((SLogicConditionNode*)pEqCond)->condType == LOGIC_COND_TYPE_AND) {
    SLogicConditionNode* pLogic = (SLogicConditionNode*)pEqCond;
    SNode*               pNode = NULL;
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

static int32_t setMergeJoinPrimColEqCond(SNode* pEqCond, int32_t subType, int64_t leftBlkId, int64_t rightBlkId,
                                         SSortMergeJoinPhysiNode* pJoin, SJoinLogicNode* pJoinLogicNode) {
  int32_t code = 0;
  if (QUERY_NODE_OPERATOR == nodeType(pEqCond)) {
    SOperatorNode* pOp = (SOperatorNode*)pEqCond;
    if (pOp->opType != OP_TYPE_EQUAL && JOIN_STYPE_ASOF != subType) {
      planError("invalid primary cond opType, opType:%d", pOp->opType);
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }

    // Track whether the LEFT operand already set asofOpType.  When two tables
    // share the same column name (e.g. both have "ts"), doSetSlotId may
    // incorrectly map the RIGHT operand to the left block too, causing the
    // RIGHT branch below to overwrite asofOpType with the reversed operator.
    // Giving priority to the LEFT operand preserves the correct direction.
    bool asofOpSetFromLeft = false;

    switch (nodeType(pOp->pLeft)) {
      case QUERY_NODE_COLUMN: {
        SColumnNode* pCol = (SColumnNode*)pOp->pLeft;
        if (leftBlkId == pCol->dataBlockId) {
          pJoin->leftPrimSlotId = pCol->slotId;
          pJoin->asofOpType = pOp->opType;
          asofOpSetFromLeft = true;
        } else if (rightBlkId == pCol->dataBlockId) {
          pJoin->rightPrimSlotId = pCol->slotId;
        } else {
          planError("invalid primary key col equal cond, leftBlockId:%" PRId64, pCol->dataBlockId);
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        break;
      }
      case QUERY_NODE_VALUE: {
        if (pJoinLogicNode && pJoinLogicNode->leftConstPrimGot) {
          pJoin->leftPrimExpr = NULL;
          code = nodesCloneNode(pOp->pLeft, &pJoin->leftPrimExpr);
          break;
        }
        
        planError("value node got in prim eq left cond, rightType:%d", pOp->pRight ? nodeType(pOp->pRight) : 0);
        return TSDB_CODE_PLAN_INTERNAL_ERROR;
      }
      case QUERY_NODE_FUNCTION: {
        SFunctionNode* pFunc = (SFunctionNode*)pOp->pLeft;
        if (FUNCTION_TYPE_TIMETRUNCATE != pFunc->funcType &&
            FUNCTION_TYPE_TIMESTAMP_SCALE != pFunc->funcType) {
          planError("invalid primary cond left function type, leftFuncType:%d", pFunc->funcType);
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        SNode* pParam = nodesListGetNode(pFunc->pParameterList, 0);
        if (QUERY_NODE_COLUMN != nodeType(pParam)) {
          planError("invalid primary cond left function param type, leftParamType:%d", nodeType(pParam));
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        SColumnNode* pCol = (SColumnNode*)pParam;
        if (leftBlkId == pCol->dataBlockId) {
          pJoin->leftPrimSlotId = pCol->slotId;
          pJoin->asofOpType = pOp->opType;
          asofOpSetFromLeft = true;
          pJoin->leftPrimExpr = NULL;
          code = nodesCloneNode((SNode*)pFunc, &pJoin->leftPrimExpr);
        } else if (rightBlkId == pCol->dataBlockId) {
          pJoin->rightPrimSlotId = pCol->slotId;
          pJoin->rightPrimExpr = NULL;
          code = nodesCloneNode((SNode*)pFunc, &pJoin->rightPrimExpr);
        } else {
          planError("invalid primary key col equal cond, leftBlockId:%" PRId64, pCol->dataBlockId);
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        break;
      }
      default: {
        SNodeList* pCols = NULL;
        code = nodesCollectColumnsFromNode(pOp->pLeft, NULL, COLLECT_COL_TYPE_ALL, &pCols);
        if (TSDB_CODE_SUCCESS != code || NULL == pCols || 0 == LIST_LENGTH(pCols)) {
          nodesDestroyList(pCols);
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        SColumnNode* pCol = (SColumnNode*)nodesListGetNode(pCols, 0);
        for (SListCell* pCell = pCols->pHead; pCell; pCell = pCell->pNext) {
          SColumnNode* pExprCol = (SColumnNode*)pCell->pNode;
          if (pExprCol->dataBlockId != pCol->dataBlockId) {
            nodesDestroyList(pCols);
            return TSDB_CODE_PLAN_INTERNAL_ERROR;
          }
        }
        if (leftBlkId == pCol->dataBlockId) {
          pJoin->leftPrimSlotId = pCol->slotId;
          pJoin->asofOpType = pOp->opType;
          asofOpSetFromLeft = true;
          code = nodesCloneNode(pOp->pLeft, &pJoin->leftPrimExpr);
        } else if (rightBlkId == pCol->dataBlockId) {
          pJoin->rightPrimSlotId = pCol->slotId;
          code = nodesCloneNode(pOp->pLeft, &pJoin->rightPrimExpr);
        } else {
          code = TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        nodesDestroyList(pCols);
        break;
      }
    }
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    switch (nodeType(pOp->pRight)) {
      case QUERY_NODE_COLUMN: {
        SColumnNode* pCol = (SColumnNode*)pOp->pRight;
        if (leftBlkId == pCol->dataBlockId && !asofOpSetFromLeft) {
          pJoin->leftPrimSlotId = pCol->slotId;
          pJoin->asofOpType = getAsofJoinReverseOp(pOp->opType);
        } else if (rightBlkId == pCol->dataBlockId) {
          pJoin->rightPrimSlotId = pCol->slotId;
        } else if (leftBlkId != pCol->dataBlockId) {
          planError("invalid primary key col equal cond, rightBlockId:%" PRId64, pCol->dataBlockId);
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        break;
      }
      case QUERY_NODE_VALUE: {
        if (pJoinLogicNode && pJoinLogicNode->rightConstPrimGot) {
          pJoin->rightPrimExpr = NULL;
          code = nodesCloneNode(pOp->pRight, &pJoin->rightPrimExpr);
          break;
        }
        
        planError("value node got in prim eq right cond, leftType:%d", pOp->pLeft ? nodeType(pOp->pLeft) : 0);
        return TSDB_CODE_PLAN_INTERNAL_ERROR;
      }      
      case QUERY_NODE_FUNCTION: {
        SFunctionNode* pFunc = (SFunctionNode*)pOp->pRight;
        if (FUNCTION_TYPE_TIMETRUNCATE != pFunc->funcType &&
            FUNCTION_TYPE_TIMESTAMP_SCALE != pFunc->funcType) {
          planError("invalid primary cond right function type, rightFuncType:%d", pFunc->funcType);
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        SNode* pParam = nodesListGetNode(pFunc->pParameterList, 0);
        if (QUERY_NODE_COLUMN != nodeType(pParam)) {
          planError("invalid primary cond right function param type, rightParamType:%d", nodeType(pParam));
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        SColumnNode* pCol = (SColumnNode*)pParam;
        if (leftBlkId == pCol->dataBlockId && !asofOpSetFromLeft) {
          pJoin->leftPrimSlotId = pCol->slotId;
          pJoin->asofOpType = getAsofJoinReverseOp(pOp->opType);
          pJoin->leftPrimExpr = NULL;
          code = nodesCloneNode((SNode*)pFunc, &pJoin->leftPrimExpr);
        } else if (rightBlkId == pCol->dataBlockId) {
          pJoin->rightPrimSlotId = pCol->slotId;
          pJoin->rightPrimExpr = NULL;
          code = nodesCloneNode((SNode*)pFunc, &pJoin->rightPrimExpr);
        } else if (leftBlkId != pCol->dataBlockId) {
          planError("invalid primary key col equal cond, rightBlockId:%" PRId64, pCol->dataBlockId);
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        break;
      }
      default: {
        SNodeList* pCols = NULL;
        code = nodesCollectColumnsFromNode(pOp->pRight, NULL, COLLECT_COL_TYPE_ALL, &pCols);
        if (TSDB_CODE_SUCCESS != code || NULL == pCols || 0 == LIST_LENGTH(pCols)) {
          nodesDestroyList(pCols);
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        SColumnNode* pCol = (SColumnNode*)nodesListGetNode(pCols, 0);
        if (leftBlkId == pCol->dataBlockId && !asofOpSetFromLeft) {
          pJoin->leftPrimSlotId = pCol->slotId;
          pJoin->asofOpType = getAsofJoinReverseOp(pOp->opType);
          code = nodesCloneNode(pOp->pRight, &pJoin->leftPrimExpr);
        } else if (rightBlkId == pCol->dataBlockId) {
          pJoin->rightPrimSlotId = pCol->slotId;
          code = nodesCloneNode(pOp->pRight, &pJoin->rightPrimExpr);
        } else {
          planError("invalid primary key col equal cond, rightBlockId:%" PRId64, pCol->dataBlockId);
          code = TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        nodesDestroyList(pCols);
        break;
      }
    }
  } else {
    planError("invalid primary key col equal cond, type:%d", nodeType(pEqCond));
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  return code;
}

static int32_t removePrimColFromJoinTargets(SNodeList* pTargets, SValueNode* pPrimExpr, SColumnNode** ppRemoved) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode* pNode = NULL;
  FOREACH(pNode, pTargets) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    if (0 == strcmp(pCol->tableAlias, pPrimExpr->node.srcTable) && 0 == strcmp(pCol->colName, pPrimExpr->node.aliasName)) {
      code = nodesCloneNode(pNode, (SNode**)ppRemoved);
      ERASE_NODE(pTargets);
      break;
    }
  }

  return code;
}

static int32_t appendPrimColToJoinTargets(SSortMergeJoinPhysiNode* pJoin, SColumnNode** ppTarget, STargetNode* primExpr, int64_t blkId) {
  SColumnNode* pCol = *ppTarget;
  if (TSDB_DATA_TYPE_TIMESTAMP != pCol->node.resType.type) {
    planError("primary key output type is not ts, type:%d", pCol->node.resType.type);
    return TSDB_CODE_PAR_PRIM_KEY_MUST_BE_TS;
  }
  pCol->dataBlockId = blkId;
  pCol->slotId = primExpr->slotId;
  int32_t code = nodesListMakeStrictAppend(&pJoin->pTargets, (SNode *)pCol);
  if (TSDB_CODE_SUCCESS == code) {
    *ppTarget = NULL;
  }

  return code;
}

static const char* getLogicNodeTableAlias(const SLogicNode* pLogicNode) {
  if (NULL == pLogicNode) {
    return NULL;
  }

  const SNodeList* pCols = NULL;
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pLogicNode)) {
    pCols = ((const SScanLogicNode*)pLogicNode)->pScanCols;
  } else if (QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN == nodeType(pLogicNode)) {
    pCols = ((const SVirtualScanLogicNode*)pLogicNode)->pScanCols;
  }

  const SNode* pNode = NULL;
  FOREACH(pNode, pCols) {
    if (QUERY_NODE_COLUMN == nodeType(pNode) && '\0' != ((const SColumnNode*)pNode)->tableAlias[0]) {
      return ((const SColumnNode*)pNode)->tableAlias;
    }
  }
  FOREACH(pNode, pLogicNode->pTargets) {
    if (QUERY_NODE_COLUMN == nodeType(pNode) && '\0' != ((const SColumnNode*)pNode)->tableAlias[0]) {
      return ((const SColumnNode*)pNode)->tableAlias;
    }
  }
  return NULL;
}

static const char* getLogicNodeTargetAlias(const SLogicNode* pLogicNode) {
  if (NULL != pLogicNode && QUERY_NODE_LOGIC_PLAN_PROJECT == nodeType(pLogicNode)) {
    const char* pStmtName = ((const SProjectLogicNode*)pLogicNode)->stmtName;
    if ('\0' != pStmtName[0]) {
      return pStmtName;
    }
  }
  return getLogicNodeTableAlias(pLogicNode);
}

static void getJoinChildTableAliases(const SJoinLogicNode* pJoinLogicNode, const char** ppLeftAlias,
                                     const char** ppRightAlias) {
  const SLogicNode* pLeftLogic =
      (const SLogicNode*)nodesListGetNode(pJoinLogicNode->node.pChildren, 0);
  const SLogicNode* pRightLogic =
      (const SLogicNode*)nodesListGetNode(pJoinLogicNode->node.pChildren, 1);
  *ppLeftAlias = getLogicNodeTableAlias(pLeftLogic);
  *ppRightAlias = getLogicNodeTableAlias(pRightLogic);
}

static int32_t setJoinNodeSlotId(SPhysiPlanContext* pCxt, const SJoinLogicNode* pJoin, int64_t leftDataBlockId,
                                 int64_t rightDataBlockId, SNode* pNode, SNode** pOutput) {
  // Conditions are translated with the aliases exposed by the child scan
  // targets.  For subqueries, stmtName (the outer alias) may differ from
  // those target aliases; using it here makes both TIMETRUNCATE join sides
  // resolve to the wrong descriptor and ends in "Planner slot key not found".
  const char* leftAlias = NULL;
  const char* rightAlias = NULL;
  getJoinChildTableAliases(pJoin, &leftAlias, &rightAlias);
  return setNodeSlotIdWithAliases(pCxt, leftDataBlockId, rightDataBlockId, leftAlias, rightAlias, pNode, pOutput);
}

static bool getJoinExprTableAlias(SNode* pExpr, char* pAlias, int32_t aliasLen) {
  SNodeList* pCols = NULL;
  if (NULL == pExpr || TSDB_CODE_SUCCESS != nodesCollectColumnsFromNode(pExpr, NULL, COLLECT_COL_TYPE_ALL, &pCols)) {
    return false;
  }

  bool   found = false;
  SNode* pNode = NULL;
  FOREACH(pNode, pCols) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    if ('\0' != pCol->tableAlias[0]) {
      tstrncpy(pAlias, pCol->tableAlias, aliasLen);
      found = true;
      break;
    }
  }
  nodesDestroyList(pCols);
  return found;
}

static bool getJoinCondSideAlias(SNode* pCond, int32_t childIndex, char* pAlias, int32_t aliasLen) {
  if (NULL == pCond) return false;
  if (QUERY_NODE_OPERATOR == nodeType(pCond)) {
    SOperatorNode* pOp = (SOperatorNode*)pCond;
    return getJoinExprTableAlias(0 == childIndex ? pOp->pLeft : pOp->pRight, pAlias, aliasLen);
  }
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pCond)) {
    SNode* pNode = NULL;
    FOREACH(pNode, ((SLogicConditionNode*)pCond)->pParameterList) {
      if (getJoinCondSideAlias(pNode, childIndex, pAlias, aliasLen)) return true;
    }
  }
  return false;
}

static int32_t setJoinTargetListSlotId(SPhysiPlanContext* pCxt, const SJoinLogicNode* pJoin,
                                       int64_t leftDataBlockId, int64_t rightDataBlockId,
                                       const SNodeList* pList, SNodeList** pOutput) {
  if (NULL == pList) return TSDB_CODE_SUCCESS;

  SNodeList* pRes = NULL;
  int32_t    code = nodesCloneList(pList, &pRes);
  if (TSDB_CODE_SUCCESS != code) return code;

  char leftAlias[TSDB_TABLE_NAME_LEN] = {0};
  char rightAlias[TSDB_TABLE_NAME_LEN] = {0};
  SNode* pEqCond = NULL != pJoin->pPrimKeyEqCond ? pJoin->pPrimKeyEqCond : pJoin->pColEqCond;
  const char* pLeftAlias = getJoinCondSideAlias(pEqCond, 0, leftAlias, sizeof(leftAlias)) ? leftAlias : NULL;
  const char* pRightAlias = getJoinCondSideAlias(pEqCond, 1, rightAlias, sizeof(rightAlias)) ? rightAlias : NULL;
  if (pJoin->isCrossJoin &&
      (NULL == pLeftAlias || NULL == pRightAlias || 0 == strcmp(pLeftAlias, pRightAlias))) {
    const SLogicNode* pLeftLogic = (const SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0);
    const SLogicNode* pRightLogic = (const SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1);
    pLeftAlias = getLogicNodeTargetAlias(pLeftLogic);
    pRightAlias = getLogicNodeTargetAlias(pRightLogic);
  }
  SSetSlotIdCxt cxt = {
      .errCode = TSDB_CODE_SUCCESS,
      .pLeftHash = taosArrayGetP(pCxt->pLocationHelper, leftDataBlockId),
      .pLeftProjIdxHash = taosArrayGetP(pCxt->pProjIdxLocHelper, leftDataBlockId),
      .pRightHash = taosArrayGetP(pCxt->pLocationHelper, rightDataBlockId),
      .pRightProdIdxHash = taosArrayGetP(pCxt->pProjIdxLocHelper, rightDataBlockId),
      .pLeftTableAlias = pLeftAlias,
      .pRightTableAlias = pRightAlias};
  nodesWalkExprs(pRes, doSetSlotId, &cxt);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyList(pRes);
    return cxt.errCode;
  }
  *pOutput = pRes;
  return TSDB_CODE_SUCCESS;
}

static int32_t addCrossJoinChildAliasKeys(SPhysiPlanContext* pCxt, const SLogicNode* pLogicChild,
                                          const SDataBlockDescNode* pDesc) {
  if (NULL == pLogicChild || NULL == pLogicChild->pTargets || NULL == pDesc || NULL == pDesc->pSlots) {
    return TSDB_CODE_SUCCESS;
  }

  SHashObj* pHash = taosArrayGetP(pCxt->pLocationHelper, pDesc->dataBlockId);
  int32_t targetNum = LIST_LENGTH(pLogicChild->pTargets);
  int32_t slotNum = LIST_LENGTH(pDesc->pSlots);
  for (int32_t i = 0; NULL != pHash && i < targetNum && i < slotNum; ++i) {
    SNode* pTarget = nodesListGetNode(pLogicChild->pTargets, i);
    SSlotDescNode* pSlot = (SSlotDescNode*)nodesListGetNode(pDesc->pSlots, i);
    if (QUERY_NODE_COLUMN != nodeType(pTarget) || '\0' == ((SColumnNode*)pTarget)->tableAlias[0]) {
      continue;
    }

    char* pKey = NULL;
    int32_t keyLen = 0;
    int32_t keyCap = 0;
    int32_t code = getSlotKey(pTarget, NULL, &pKey, &keyLen, 0, &keyCap);
    if (TSDB_CODE_SUCCESS == code) {
      code = putSlotToHashImpl(pDesc->dataBlockId, pSlot->slotId, pKey, keyLen, pHash);
    }
    taosMemoryFree(pKey);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static bool fqJoinTargetExprExistsBySlotKey(const SNodeList* pTargets, SNode* pExpr) {
  if (NULL == pTargets || NULL == pExpr) {
    return false;
  }

  char*   pExprKey = NULL;
  int32_t exprKeyLen = 0;
  int32_t exprKeyCap = 0;
  int32_t code = getSlotKey(pExpr, NULL, &pExprKey, &exprKeyLen, 0, &exprKeyCap);
  if (TSDB_CODE_SUCCESS != code) {
    planError("FQ-joinCondTargets: build expr slot key failed exprType=%d code=0x%x",
              (int)nodeType(pExpr), code);
    return false;
  }

  bool exists = false;
  SNode* pNode = NULL;
  FOREACH(pNode, pTargets) {
    char*   pTargetKey = NULL;
    int32_t targetKeyLen = 0;
    int32_t targetKeyCap = 0;
    code = getSlotKey(pNode, NULL, &pTargetKey, &targetKeyLen, 0, &targetKeyCap);
    if (TSDB_CODE_SUCCESS == code &&
        exprKeyLen == targetKeyLen &&
        0 == memcmp(pExprKey, pTargetKey, exprKeyLen)) {
      exists = true;
    }
    taosMemoryFree(pTargetKey);
    if (exists || TSDB_CODE_SUCCESS != code) {
      if (TSDB_CODE_SUCCESS != code) {
        planError("FQ-joinCondTargets: build target slot key failed exprType=%d code=0x%x",
                  (int)nodeType(pNode), code);
      }
      break;
    }
  }

  taosMemoryFree(pExprKey);
  return exists;
}

static int32_t fqAppendJoinConditionTargetsFromNode(SPhysiPlanContext* pCxt,
                                                    SNode* pCond,
                                                    int64_t leftDataBlockId,
                                                    int64_t rightDataBlockId,
                                                    SSHashObj** pTables,
                                                    SNodeList* pTargets,
                                                    int32_t* pAppended) {
  SNodeList* pCondCols = NULL;
  int32_t code = nodesCollectColumnsFromNode(pCond, NULL, COLLECT_COL_TYPE_ALL,
                                            &pCondCols);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyList(pCondCols);
    return code;
  }

  int32_t appended = 0;
  SNode* pNode = NULL;
  FOREACH(pNode, pCondCols) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    size_t aliasLen = strlen(pCol->tableAlias);
    bool left = NULL != tSimpleHashGet(pTables[0], pCol->tableAlias, aliasLen);
    bool right = NULL != tSimpleHashGet(pTables[1], pCol->tableAlias, aliasLen);
    int64_t firstBlockId = leftDataBlockId;
    int64_t secondBlockId = rightDataBlockId;
    // A bare-name fallback must stay within the column's owning input.
    if (left != right) {
      firstBlockId = left ? leftDataBlockId : rightDataBlockId;
      secondBlockId = -1;
    }

    SNode* pBound = NULL;
    code = setNodeSlotId(pCxt, firstBlockId, secondBlockId, pNode, &pBound);
    if (TSDB_CODE_SUCCESS != code) {
      planError("FQ-joinCondTargets: bind condition columns failed "
                "leftBlockId=%" PRId64 " rightBlockId=%" PRId64
                " code=0x%x cond=%p",
                leftDataBlockId, rightDataBlockId, code, pCond);
      break;
    }
    if (fqJoinTargetExprExistsBySlotKey(pTargets, pBound)) {
      nodesDestroyNode(pBound);
      continue;
    }
    code = nodesListStrictAppend(pTargets, pBound);
    if (TSDB_CODE_SUCCESS != code) {
      planError("FQ-joinCondTargets: append hidden target failed col=%s "
                "tableAlias=%s code=0x%x", pCol->colName, pCol->tableAlias,
                code);
      break;
    }
    ++appended;
    SColumnNode* pBoundCol = (SColumnNode*)pBound;
    planDebug("FQ-joinCondTargets: appended hidden condition column col=%s "
              "alias=%s tableAlias=%s dataBlockId=%" PRId64 " slotId=%d",
              pBoundCol->colName, pBoundCol->node.aliasName,
              pBoundCol->tableAlias, pBoundCol->dataBlockId, pBoundCol->slotId);
  }
  *pAppended += appended;
  planDebug("FQ-joinCondTargets: leftBlockId=%" PRId64 " rightBlockId=%" PRId64
            " appended=%d condCols=%d finalTargets=%d cond=%p",
            leftDataBlockId, rightDataBlockId, appended,
            LIST_LENGTH(pCondCols), LIST_LENGTH(pTargets), pCond);
  nodesDestroyList(pCondCols);
  return code;
}

static int32_t fqAppendJoinConditionTargets(SPhysiPlanContext* pCxt,
                                            SJoinLogicNode* pJoinLogicNode,
                                            int64_t leftDataBlockId,
                                            int64_t rightDataBlockId,
                                            SNodeList* pTargets) {
  if (NULL == pCxt || NULL == pJoinLogicNode || NULL == pTargets) {
    return TSDB_CODE_SUCCESS;
  }

  SNode* pConditions[] = {pJoinLogicNode->pFullOnCond,
                         pJoinLogicNode->pColOnCond,
                         pJoinLogicNode->node.pConditions};
  if (NULL == pConditions[0] && NULL == pConditions[1] &&
      NULL == pConditions[2]) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  int32_t appended = 0;
  SSHashObj* pTables[2] = {NULL, NULL};
  for (int32_t i = 0; i < 2 && TSDB_CODE_SUCCESS == code; ++i) {
    SNode* pChild = nodesListGetNode(pJoinLogicNode->node.pChildren, i);
    code = collectTableAliasFromNodes(pChild, &pTables[i]);
    if (TSDB_CODE_SUCCESS == code) {
      code = collectSourceTableAliasFromNodes(pChild, &pTables[i]);
    }
  }

  // ON and WHERE predicates may read hidden columns from the JOIN output.
  for (int32_t i = 0; i < 3 && TSDB_CODE_SUCCESS == code; ++i) {
    if (NULL != pConditions[i]) {
      code = fqAppendJoinConditionTargetsFromNode(pCxt, pConditions[i],
                                                 leftDataBlockId,
                                                 rightDataBlockId, pTables,
                                                 pTargets, &appended);
    }
  }
  planDebug("FQ-joinCondTargets: joinType=%d leftBlockId=%" PRId64
            " rightBlockId=%" PRId64 " totalAppended=%d finalTargets=%d "
            "fullOn=%p colOn=%p where=%p", (int)pJoinLogicNode->joinType,
            leftDataBlockId, rightDataBlockId, appended, LIST_LENGTH(pTargets),
            pConditions[0], pConditions[1], pConditions[2]);
  tSimpleHashCleanup(pTables[0]);
  tSimpleHashCleanup(pTables[1]);
  return code;
}
static int32_t createMergeJoinPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SJoinLogicNode* pJoinLogicNode,
                                        SPhysiNode** pPhyNode) {
  bool constPrimEqCond =
      NULL != pJoinLogicNode->pPrimKeyEqCond && QUERY_NODE_OPERATOR == nodeType(pJoinLogicNode->pPrimKeyEqCond) &&
      QUERY_NODE_VALUE == nodeType(((SOperatorNode*)pJoinLogicNode->pPrimKeyEqCond)->pLeft) &&
      QUERY_NODE_VALUE == nodeType(((SOperatorNode*)pJoinLogicNode->pPrimKeyEqCond)->pRight);
  if (!IS_WINDOW_JOIN(pJoinLogicNode->subType) && !IS_ASOF_JOIN(pJoinLogicNode->subType) &&
      (NULL == pJoinLogicNode->pPrimKeyEqCond ||
       ((!constPrimEqCond) &&
        (pJoinLogicNode->noPrimKeyEqCond || pJoinLogicNode->leftNoOrderedSubQuery ||
         pJoinLogicNode->rightNoOrderedSubQuery)))) {
    return generateUsageErrMsg(pCxt->pPlanCxt->pMsg, pCxt->pPlanCxt->msgLen, TSDB_CODE_PAR_NOT_SUPPORT_JOIN,
                               "Merge Join requires ordered inputs and primary timestamp equal condition");
  }

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
  if (TSDB_CODE_SUCCESS == code) {
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
    code = setJoinNodeSlotId(pCxt, pJoinLogicNode, pLeftDesc->dataBlockId, pRightDesc->dataBlockId,
                             pJoinLogicNode->pPrimKeyEqCond, &pJoin->pPrimKeyCond);
    if (TSDB_CODE_SUCCESS == code) {
      code = setMergeJoinPrimColEqCond(pJoin->pPrimKeyCond, pJoin->subType, pLeftDesc->dataBlockId,
                                       pRightDesc->dataBlockId, pJoin, pJoinLogicNode);
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
    code = setJoinNodeSlotId(pCxt, pJoinLogicNode, pLeftDesc->dataBlockId,
                            pRightDesc->dataBlockId,
                            pJoinLogicNode->addPrimEqCond, &pPrimKeyCond);
    if (TSDB_CODE_SUCCESS == code) {
      code = setMergeJoinPrimColEqCond(pPrimKeyCond, pJoin->subType, pLeftDesc->dataBlockId, pRightDesc->dataBlockId,
                                       pJoin, NULL);
    }
    if (TSDB_CODE_SUCCESS == code && NULL != pJoin->leftPrimExpr) {
      code = addDataBlockSlot(pCxt, &pJoin->leftPrimExpr, pLeftDesc);
    }
    if (TSDB_CODE_SUCCESS == code && NULL != pJoin->rightPrimExpr) {
      code = addDataBlockSlot(pCxt, &pJoin->rightPrimExpr, pRightDesc);
    }
    if (TSDB_CODE_SUCCESS == code && NULL == pJoin->pPrimKeyCond) {
      pJoin->pPrimKeyCond = pPrimKeyCond;
      pPrimKeyCond = NULL;
    }
    nodesDestroyNode(pPrimKeyCond);
  }

  SValueNode* pLeftPrimExpr = NULL, *pRightPrimExpr = NULL;
  SColumnNode* pLeftTarget = NULL, *pRightTarget = NULL;
  if (TSDB_CODE_SUCCESS == code && pJoinLogicNode->leftConstPrimGot && pJoin->leftPrimExpr 
      && QUERY_NODE_VALUE == nodeType(((STargetNode*)pJoin->leftPrimExpr)->pExpr)) {
    pLeftPrimExpr = (SValueNode*)((STargetNode*)pJoin->leftPrimExpr)->pExpr;
    code = removePrimColFromJoinTargets(pJoinLogicNode->node.pTargets, pLeftPrimExpr, &pLeftTarget);
  }

  if (TSDB_CODE_SUCCESS == code && pJoinLogicNode->rightConstPrimGot && pJoin->rightPrimExpr 
      && QUERY_NODE_VALUE == nodeType(((STargetNode*)pJoin->rightPrimExpr)->pExpr)) {
    pRightPrimExpr = (SValueNode*)((STargetNode*)pJoin->rightPrimExpr)->pExpr;
    code = removePrimColFromJoinTargets(pJoinLogicNode->node.pTargets, pRightPrimExpr, &pRightTarget);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setJoinTargetListSlotId(pCxt, pJoinLogicNode, pLeftDesc->dataBlockId, pRightDesc->dataBlockId,
                                   pJoinLogicNode->node.pTargets, &pJoin->pTargets);
  }

  if (TSDB_CODE_SUCCESS == code && pLeftPrimExpr && pLeftTarget) {
    code = appendPrimColToJoinTargets(pJoin, &pLeftTarget, (STargetNode*)pJoin->leftPrimExpr, pLeftDesc->dataBlockId);
  }
  if (TSDB_CODE_SUCCESS == code && pRightPrimExpr && pRightTarget) {
    code = appendPrimColToJoinTargets(pJoin, &pRightTarget, (STargetNode*)pJoin->rightPrimExpr, pRightDesc->dataBlockId);
  }

  if (TSDB_CODE_SUCCESS == code && ((NULL != pJoinLogicNode->pColEqCond) || (NULL != pJoinLogicNode->pTagEqCond))) {
    code = mergeJoinConds(&pJoinLogicNode->pColEqCond, &pJoinLogicNode->pTagEqCond);
  }
  // TODO set from input blocks for group algo
  /*
    if (TSDB_CODE_SUCCESS == code && NULL != pJoinLogicNode->pColEqCond) {
      code = setNodeSlotId(pCxt, pLeftDesc->dataBlockId, pRightDesc->dataBlockId, pJoinLogicNode->pColEqCond,
    &pJoin->pColEqCond);
    }
  */
  if (TSDB_CODE_SUCCESS == code && NULL != pJoinLogicNode->pColEqCond) {
    code = setJoinNodeSlotId(pCxt, pJoinLogicNode, pLeftDesc->dataBlockId, pRightDesc->dataBlockId,
                             pJoinLogicNode->pColEqCond, &pJoin->pColEqCond);
    if (TSDB_CODE_SUCCESS == code) {
      code = setColEqList(pJoin->pColEqCond, pLeftDesc->dataBlockId, pRightDesc->dataBlockId, &pJoin->pEqLeft,
                          &pJoin->pEqRight);
    }
  }

  if (TSDB_CODE_SUCCESS == code && ((NULL != pJoinLogicNode->pColOnCond) || (NULL != pJoinLogicNode->pTagOnCond))) {
    code = mergeJoinConds(&pJoinLogicNode->pColOnCond, &pJoinLogicNode->pTagOnCond);
  }
  // TODO set from input blocks for group algo
  /*
    if (TSDB_CODE_SUCCESS == code && NULL != pJoinLogicNode->pColOnCond) {
    code = setJoinNodeSlotId(pCxt, pJoinLogicNode, pLeftDesc->dataBlockId, pRightDesc->dataBlockId,
                             pJoinLogicNode->pColOnCond, &pJoin->pColOnCond);
    }
  */

  if (TSDB_CODE_SUCCESS == code) {
    code = fqAppendJoinConditionTargets(pCxt, pJoinLogicNode, pLeftDesc->dataBlockId, pRightDesc->dataBlockId,
                                        pJoin->pTargets);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = addDataBlockSlots(pCxt, pJoin->pTargets, pJoin->node.pOutputDataBlockDesc);
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pJoinLogicNode->pFullOnCond) {
    code = setNodeSlotId(pCxt, pJoin->node.pOutputDataBlockDesc->dataBlockId, -1, pJoinLogicNode->pFullOnCond,
                         &pJoin->pFullOnCond);
    planDebug("FQ-joinSlots: mergeJoin bind fullOnCond code=0x%x outputBlockId=%" PRId64 " cond=%p bound=%p",
              code, pJoin->node.pOutputDataBlockDesc->dataBlockId, pJoinLogicNode->pFullOnCond, pJoin->pFullOnCond);
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pJoinLogicNode->pColOnCond) {
    code = setNodeSlotId(pCxt, pJoin->node.pOutputDataBlockDesc->dataBlockId, -1, pJoinLogicNode->pColOnCond,
                         &pJoin->pColOnCond);
    planDebug("FQ-joinSlots: mergeJoin bind colOnCond code=0x%x outputBlockId=%" PRId64 " cond=%p bound=%p",
              code, pJoin->node.pOutputDataBlockDesc->dataBlockId, pJoinLogicNode->pColOnCond, pJoin->pColOnCond);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setConditionsSlotId(pCxt, (const SLogicNode*)pJoinLogicNode, (SPhysiNode*)pJoin);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pJoin;
  } else {
    nodesDestroyNode((SNode*)pJoin);
    nodesDestroyNode((SNode*)pLeftTarget);
    nodesDestroyNode((SNode*)pRightTarget);
  }

  return code;
}

static int32_t createHashJoinKeyColFromPrimExpr(SPhysiPlanContext* pCxt, SNode* pExpr, SNode** ppCol) {
  if (NULL == pExpr || NULL == ppCol) {
    planError("Invalid hash join key target");
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  if (QUERY_NODE_FUNCTION == nodeType(pExpr)) {
    SFunctionNode* pFunc = (SFunctionNode*)pExpr;
    if (FUNCTION_TYPE_TIMETRUNCATE != pFunc->funcType &&
        FUNCTION_TYPE_TIMESTAMP_SCALE != pFunc->funcType) {
      planError("Invalid hash join key function type:%d", pFunc->funcType);
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }
    SNode* pParam = nodesListGetNode(pFunc->pParameterList, 0);
    if (NULL == pParam || QUERY_NODE_COLUMN != nodeType(pParam)) {
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }

    char* pName = NULL;
    int32_t len = 0;
    int32_t cap = 0;
    int32_t code = getSlotKey(pExpr, NULL, &pName, &len, 0, &cap);
    if (TSDB_CODE_SUCCESS != code) return code;
    SHashObj* pHash = taosArrayGetP(pCxt->pLocationHelper, ((SColumnNode*)pParam)->dataBlockId);
    SSlotIndex* pIndex = pHash ? taosHashGet(pHash, pName, len) : NULL;
    taosMemoryFree(pName);
    if (NULL == pIndex) {
      return TSDB_CODE_PLAN_SLOT_NOT_FOUND;
    }
    if (NULL == pIndex->pSlotIdsInfo || 0 == taosArrayGetSize(pIndex->pSlotIdsInfo)) {
      return TSDB_CODE_PLAN_SLOT_NOT_FOUND;
    }
    SSlotIdInfo* pSlotInfo = taosArrayGet(pIndex->pSlotIdsInfo, 0);
    if (NULL == pSlotInfo) {
      return TSDB_CODE_PLAN_SLOT_NOT_FOUND;
    }

    SColumnNode* pCol = NULL;
    code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
    if (TSDB_CODE_SUCCESS != code) return code;
    pCol->dataBlockId = pIndex->dataBlockId;
    pCol->slotId = pSlotInfo->slotId;
    pCol->colType = COLUMN_TYPE_COLUMN;
    pCol->node.resType = ((SExprNode*)pExpr)->resType;
    tstrncpy(pCol->colName, ((SExprNode*)pExpr)->aliasName, TSDB_COL_NAME_LEN);
    tstrncpy(pCol->node.aliasName, ((SExprNode*)pExpr)->aliasName, TSDB_COL_NAME_LEN);
    tstrncpy(pCol->node.userAlias, ((SExprNode*)pExpr)->aliasName, TSDB_COL_NAME_LEN);
    *ppCol = (SNode*)pCol;
    return TSDB_CODE_SUCCESS;
  }

  if (QUERY_NODE_TARGET != nodeType(pExpr)) {
    planError("Invalid hash join key target type:%d", nodeType(pExpr));
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }
  STargetNode* pTarget = (STargetNode*)pExpr;
  if (NULL == pTarget->pExpr) return TSDB_CODE_PLAN_INTERNAL_ERROR;

  SColumnNode* pCol = NULL;
  int32_t      code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  pCol->dataBlockId = pTarget->dataBlockId;
  pCol->slotId = pTarget->slotId;
  pCol->colType = COLUMN_TYPE_COLUMN;
  pCol->node.resType = ((SExprNode*)pTarget->pExpr)->resType;
  tstrncpy(pCol->colName, ((SExprNode*)pTarget->pExpr)->aliasName, TSDB_COL_NAME_LEN);
  tstrncpy(pCol->node.aliasName, ((SExprNode*)pTarget->pExpr)->aliasName, TSDB_COL_NAME_LEN);
  tstrncpy(pCol->node.userAlias, ((SExprNode*)pTarget->pExpr)->aliasName, TSDB_COL_NAME_LEN);
  *ppCol = (SNode*)pCol;
  return TSDB_CODE_SUCCESS;
}

static int32_t getHashJoinNodeBlkId(SPhysiPlanContext* pCxt, SNode* pNode, int64_t leftBlkId, int64_t rightBlkId,
                                    int64_t* pBlkId) {
  // Columns have already been slot-bound; block zero is a valid input.
  if (QUERY_NODE_COLUMN == nodeType(pNode) && ((SColumnNode*)pNode)->dataBlockId >= 0) {
    int64_t blockId = ((SColumnNode*)pNode)->dataBlockId;
    if (blockId != leftBlkId && blockId != rightBlkId) return TSDB_CODE_PLAN_NOT_SUPPORT_JOIN_COND;
    *pBlkId = blockId;
    return TSDB_CODE_SUCCESS;
  }

  char*   pName = NULL;
  int32_t len = 0;
  int32_t cap = 0;
  int32_t code = getSlotKey(pNode, NULL, &pName, &len, 0, &cap);
  if (TSDB_CODE_SUCCESS != code) return code;

  int64_t blockId = -1;
  SHashObj* pHash = taosArrayGetP(pCxt->pLocationHelper, leftBlkId);
  if (NULL != pHash && NULL != taosHashGet(pHash, pName, len)) {
    blockId = leftBlkId;
  } else {
    pHash = taosArrayGetP(pCxt->pLocationHelper, rightBlkId);
    if (NULL != pHash && NULL != taosHashGet(pHash, pName, len)) blockId = rightBlkId;
  }
  taosMemoryFree(pName);
  if (blockId < 0) return TSDB_CODE_PLAN_SLOT_NOT_FOUND;

  *pBlkId = blockId;
  return TSDB_CODE_SUCCESS;
}

static int32_t getHashJoinExprBlkId(SPhysiPlanContext* pCxt, SNode* pExpr, int64_t leftBlkId, int64_t rightBlkId,
                                    int64_t* pBlkId) {
  if (QUERY_NODE_COLUMN == nodeType(pExpr)) {
    return getHashJoinNodeBlkId(pCxt, pExpr, leftBlkId, rightBlkId, pBlkId);
  }

  if (QUERY_NODE_FUNCTION == nodeType(pExpr) && FUNCTION_TYPE_TBNAME == ((SFunctionNode*)pExpr)->funcType) {
    int32_t code = getHashJoinNodeBlkId(pCxt, pExpr, leftBlkId, rightBlkId, pBlkId);
    if (TSDB_CODE_PLAN_SLOT_NOT_FOUND == code) {
      planError("%s", "Invalid hash join tbname key slot");
    }
    return code;
  }

  SNodeList* pCols = NULL;
  int32_t code = nodesCollectColumnsFromNode(pExpr, NULL, COLLECT_COL_TYPE_ALL, &pCols);
  if (TSDB_CODE_SUCCESS != code) return code;
  if (NULL == pCols || 0 == LIST_LENGTH(pCols)) {
    nodesDestroyList(pCols);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }
  int64_t blockId = -1;
  SNode* pCol = NULL;
  FOREACH(pCol, pCols) {
    if (QUERY_NODE_COLUMN != nodeType(pCol)) {
      nodesDestroyList(pCols);
      return TSDB_CODE_PLAN_NOT_SUPPORT_JOIN_COND;
    }

    int64_t colBlockId = -1;
    code = getHashJoinNodeBlkId(pCxt, pCol, leftBlkId, rightBlkId, &colBlockId);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyList(pCols);
      return code;
    }

    if (blockId < 0) {
      blockId = colBlockId;
    } else if (blockId != colBlockId) {
      nodesDestroyList(pCols);
      return TSDB_CODE_PLAN_NOT_SUPPORT_JOIN_COND;
    }
  }
  nodesDestroyList(pCols);
  if (blockId != leftBlkId && blockId != rightBlkId) {
    return TSDB_CODE_PLAN_NOT_SUPPORT_JOIN_COND;
  }
  *pBlkId = blockId;
  return TSDB_CODE_SUCCESS;
}

static int32_t createHashJoinKeyCol(SPhysiPlanContext* pCxt, SNode* pExpr, int64_t inputBlockId, SNode** ppCol) {
  if (QUERY_NODE_COLUMN == nodeType(pExpr)) {
    return nodesCloneNode(pExpr, ppCol);
  }

  if (QUERY_NODE_FUNCTION == nodeType(pExpr) && FUNCTION_TYPE_TBNAME == ((SFunctionNode*)pExpr)->funcType) {
    char*   pName = NULL;
    int32_t len = 0;
    int32_t cap = 0;
    int32_t code = getSlotKey(pExpr, NULL, &pName, &len, 0, &cap);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }

    // The caller has already resolved which JOIN input owns this expression.
    SHashObj* pHash = taosArrayGetP(pCxt->pLocationHelper, inputBlockId);
    SSlotIndex* pIndex = pHash != NULL ? taosHashGet(pHash, pName, len) : NULL;
    taosMemoryFree(pName);

    if (NULL == pIndex) {
      planError("%s", "Invalid hash join tbname key slot");
      return TSDB_CODE_PLAN_SLOT_NOT_FOUND;
    }
    if (NULL == pIndex->pSlotIdsInfo || 0 == taosArrayGetSize(pIndex->pSlotIdsInfo)) {
      planError("%s", "Invalid hash join tbname key slot info");
      return TSDB_CODE_PLAN_SLOT_NOT_FOUND;
    }

    SColumnNode* pCol = NULL;
    code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    pCol->dataBlockId = pIndex->dataBlockId;
    pCol->slotId = ((SSlotIdInfo*)taosArrayGet(pIndex->pSlotIdsInfo, 0))->slotId;
    pCol->colType = COLUMN_TYPE_COLUMN;
    pCol->node.resType = ((SExprNode*)pExpr)->resType;
    tstrncpy(pCol->colName, ((SExprNode*)pExpr)->aliasName, TSDB_COL_NAME_LEN);
    tstrncpy(pCol->node.aliasName, ((SExprNode*)pExpr)->aliasName, TSDB_COL_NAME_LEN);
    tstrncpy(pCol->node.userAlias, ((SExprNode*)pExpr)->aliasName, TSDB_COL_NAME_LEN);
    *ppCol = (SNode*)pCol;
    return TSDB_CODE_SUCCESS;
  }

  planError("Invalid hash join key node type:%d", nodeType(pExpr));
  return TSDB_CODE_PLAN_INTERNAL_ERROR;
}

static int32_t normalizeHashJoinKeyTypes(SNode* pLeft, SNode* pRight) {
  if (NULL == pLeft || NULL == pRight) {
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  SNode* pLeftExpr = QUERY_NODE_TARGET == nodeType(pLeft) ? ((STargetNode*)pLeft)->pExpr : pLeft;
  SNode* pRightExpr = QUERY_NODE_TARGET == nodeType(pRight) ? ((STargetNode*)pRight)->pExpr : pRight;
  if (!nodesIsExprNode(pLeftExpr) || !nodesIsExprNode(pRightExpr)) return TSDB_CODE_PLAN_INTERNAL_ERROR;
  SDataType* pLeftType = &((SExprNode*)pLeftExpr)->resType;
  SDataType* pRightType = &((SExprNode*)pRightExpr)->resType;
  if (pLeftType->type == pRightType->type) {
    return TSDB_CODE_SUCCESS;
  }

  if (!IS_NUMERIC_TYPE(pLeftType->type) || !IS_NUMERIC_TYPE(pRightType->type)) {
    return TSDB_CODE_PLAN_NOT_SUPPORT_JOIN_COND;
  }

  int32_t commonType = vectorGetConvertType(pLeftType->type, pRightType->type);
  if (commonType < TSDB_DATA_TYPE_NULL || commonType >= TSDB_DATA_TYPE_MAX) {
    return TSDB_CODE_PLAN_NOT_SUPPORT_JOIN_COND;
  }

  pLeftType->type = commonType;
  pRightType->type = commonType;
  pLeftType->bytes = tDataTypes[commonType].bytes;
  pRightType->bytes = tDataTypes[commonType].bytes;
  return TSDB_CODE_SUCCESS;
}

static int32_t createHashJoinExprTarget(SPhysiPlanContext* pCxt, SNode* pExpr, SDataBlockDescNode* pDesc,
                                        SNode** ppTarget) {
  int32_t code = setNodeSlotId(pCxt, pDesc->dataBlockId, -1, pExpr, ppTarget);
  if (TSDB_CODE_SUCCESS == code) {
    code = addDataBlockSlot(pCxt, ppTarget, pDesc);
  }
  return code;
}

static int32_t extractHashJoinOpCols(SPhysiPlanContext* pCxt, SDataBlockDescNode* pLeftDesc,
                                     SDataBlockDescNode* pRightDesc, SNode* pEq, SHashJoinPhysiNode* pJoin) {
  int32_t code = 0;
  int64_t lBlkId = pLeftDesc->dataBlockId;
  int64_t rBlkId = pRightDesc->dataBlockId;
  if (QUERY_NODE_OPERATOR != nodeType(pEq)) {
    planError("Invalid join equal node type:%d", nodeType(pEq));
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  SOperatorNode* pOp = (SOperatorNode*)pEq;
  int64_t        leftExprBlkId = 0;
  int64_t        rightExprBlkId = 0;

  switch (nodeType(pOp->pLeft)) {
    case QUERY_NODE_COLUMN:
      leftExprBlkId = ((SColumnNode*)pOp->pLeft)->dataBlockId;
      break;
    case QUERY_NODE_TARGET:
      leftExprBlkId = ((STargetNode*)pOp->pLeft)->dataBlockId;
      break;
    case QUERY_NODE_VALUE:
      if (NULL == pJoin->leftPrimExpr || QUERY_NODE_TARGET != nodeType(pJoin->leftPrimExpr)) {
        return TSDB_CODE_PLAN_INTERNAL_ERROR;
      }
      leftExprBlkId = ((STargetNode*)pJoin->leftPrimExpr)->dataBlockId;
      break;
    case QUERY_NODE_FUNCTION: {
      SFunctionNode* pFunc = (SFunctionNode*)pOp->pLeft;
      if (FUNCTION_TYPE_TBNAME == pFunc->funcType) {
        code = getHashJoinExprBlkId(pCxt, pOp->pLeft, lBlkId, rBlkId, &leftExprBlkId);
        if (TSDB_CODE_SUCCESS != code) {
          return code;
        }
        break;
      }
      if (FUNCTION_TYPE_TIMETRUNCATE != pFunc->funcType && FUNCTION_TYPE_TIMESTAMP_SCALE != pFunc->funcType) {
        code = getHashJoinExprBlkId(pCxt, pOp->pLeft, lBlkId, rBlkId, &leftExprBlkId);
        if (TSDB_CODE_SUCCESS != code) return code;
        break;
      }
      code = getHashJoinExprBlkId(pCxt, pOp->pLeft, lBlkId, rBlkId, &leftExprBlkId);
      if (TSDB_CODE_SUCCESS != code) return code;
      break;
    }
    default:
      code = getHashJoinExprBlkId(pCxt, pOp->pLeft, lBlkId, rBlkId, &leftExprBlkId);
      if (TSDB_CODE_SUCCESS != code) return code;
      break;
  }

  switch (nodeType(pOp->pRight)) {
    case QUERY_NODE_COLUMN:
      rightExprBlkId = ((SColumnNode*)pOp->pRight)->dataBlockId;
      break;
    case QUERY_NODE_TARGET:
      rightExprBlkId = ((STargetNode*)pOp->pRight)->dataBlockId;
      break;
    case QUERY_NODE_VALUE:
      if (NULL == pJoin->rightPrimExpr || QUERY_NODE_TARGET != nodeType(pJoin->rightPrimExpr)) {
        return TSDB_CODE_PLAN_INTERNAL_ERROR;
      }
      rightExprBlkId = ((STargetNode*)pJoin->rightPrimExpr)->dataBlockId;
      break;
    case QUERY_NODE_FUNCTION: {
      SFunctionNode* pFunc = (SFunctionNode*)pOp->pRight;
      if (FUNCTION_TYPE_TBNAME == pFunc->funcType) {
        code = getHashJoinExprBlkId(pCxt, pOp->pRight, lBlkId, rBlkId, &rightExprBlkId);
        if (TSDB_CODE_SUCCESS != code) {
          return code;
        }
        break;
      }
      if (FUNCTION_TYPE_TIMETRUNCATE != pFunc->funcType && FUNCTION_TYPE_TIMESTAMP_SCALE != pFunc->funcType) {
        code = getHashJoinExprBlkId(pCxt, pOp->pRight, lBlkId, rBlkId, &rightExprBlkId);
        if (TSDB_CODE_SUCCESS != code) return code;
        break;
      }
      code = getHashJoinExprBlkId(pCxt, pOp->pRight, lBlkId, rBlkId, &rightExprBlkId);
      if (TSDB_CODE_SUCCESS != code) return code;
      break;
    }
    default:
      code = getHashJoinExprBlkId(pCxt, pOp->pRight, lBlkId, rBlkId, &rightExprBlkId);
      if (TSDB_CODE_SUCCESS != code) return code;
      break;
  }

  bool validExprBlocks = (lBlkId == leftExprBlkId && rBlkId == rightExprBlkId) ||
                         (rBlkId == leftExprBlkId && lBlkId == rightExprBlkId);
  if (!validExprBlocks && QUERY_NODE_FUNCTION == nodeType(pOp->pLeft) &&
      QUERY_NODE_FUNCTION == nodeType(pOp->pRight)) {
    SFunctionNode* pLeftFunc = (SFunctionNode*)pOp->pLeft;
    SFunctionNode* pRightFunc = (SFunctionNode*)pOp->pRight;
    bool leftIsPrimFunc = FUNCTION_TYPE_TIMETRUNCATE == pLeftFunc->funcType ||
                          FUNCTION_TYPE_TIMESTAMP_SCALE == pLeftFunc->funcType;
    bool rightIsPrimFunc = FUNCTION_TYPE_TIMETRUNCATE == pRightFunc->funcType ||
                           FUNCTION_TYPE_TIMESTAMP_SCALE == pRightFunc->funcType;
    if (leftIsPrimFunc && rightIsPrimFunc) {
      leftExprBlkId = lBlkId;
      rightExprBlkId = rBlkId;
    }
  }

  SNode* pLeftExpr = NULL;
  SNode* pRightExpr = NULL;

  if (lBlkId == leftExprBlkId && rBlkId == rightExprBlkId) {
    if (QUERY_NODE_COLUMN == nodeType(pOp->pLeft)) {
      code = nodesCloneNode(pOp->pLeft, &pLeftExpr);
    } else if (QUERY_NODE_TARGET == nodeType(pOp->pLeft)) {
      code = createHashJoinKeyColFromPrimExpr(pCxt, pOp->pLeft, &pLeftExpr);
    } else if (QUERY_NODE_FUNCTION == nodeType(pOp->pLeft) &&
               FUNCTION_TYPE_TBNAME == ((SFunctionNode*)pOp->pLeft)->funcType) {
      code = createHashJoinKeyCol(pCxt, pOp->pLeft, leftExprBlkId, &pLeftExpr);
    } else if (NULL != pJoin->leftPrimExpr && QUERY_NODE_FUNCTION == nodeType(pOp->pLeft) &&
               (FUNCTION_TYPE_TIMETRUNCATE == ((SFunctionNode*)pOp->pLeft)->funcType ||
                FUNCTION_TYPE_TIMESTAMP_SCALE == ((SFunctionNode*)pOp->pLeft)->funcType)) {
      code = createHashJoinKeyColFromPrimExpr(pCxt, pJoin->leftPrimExpr, &pLeftExpr);
    } else {
      code = createHashJoinExprTarget(pCxt, pOp->pLeft, pLeftDesc, &pLeftExpr);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListStrictAppend(pJoin->pOnLeft, pLeftExpr);
    }
    if (TSDB_CODE_SUCCESS == code) {
      if (QUERY_NODE_COLUMN == nodeType(pOp->pRight)) {
        code = nodesCloneNode(pOp->pRight, &pRightExpr);
      } else if (QUERY_NODE_TARGET == nodeType(pOp->pRight)) {
        code = createHashJoinKeyColFromPrimExpr(pCxt, pOp->pRight, &pRightExpr);
      } else if (QUERY_NODE_FUNCTION == nodeType(pOp->pRight) &&
                 FUNCTION_TYPE_TBNAME == ((SFunctionNode*)pOp->pRight)->funcType) {
        code = createHashJoinKeyCol(pCxt, pOp->pRight, rightExprBlkId, &pRightExpr);
      } else if (NULL != pJoin->rightPrimExpr && QUERY_NODE_FUNCTION == nodeType(pOp->pRight) &&
                 (FUNCTION_TYPE_TIMETRUNCATE == ((SFunctionNode*)pOp->pRight)->funcType ||
                  FUNCTION_TYPE_TIMESTAMP_SCALE == ((SFunctionNode*)pOp->pRight)->funcType)) {
        code = createHashJoinKeyColFromPrimExpr(pCxt, pJoin->rightPrimExpr, &pRightExpr);
      } else {
        code = createHashJoinExprTarget(pCxt, pOp->pRight, pRightDesc, &pRightExpr);
      }
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListStrictAppend(pJoin->pOnRight, pRightExpr);
    }
  } else if (rBlkId == leftExprBlkId && lBlkId == rightExprBlkId) {
    if (QUERY_NODE_COLUMN == nodeType(pOp->pRight)) {
      code = nodesCloneNode(pOp->pRight, &pRightExpr);
    } else if (QUERY_NODE_TARGET == nodeType(pOp->pRight)) {
      code = createHashJoinKeyColFromPrimExpr(pCxt, pOp->pRight, &pRightExpr);
    } else if (QUERY_NODE_FUNCTION == nodeType(pOp->pRight) &&
               FUNCTION_TYPE_TBNAME == ((SFunctionNode*)pOp->pRight)->funcType) {
      code = createHashJoinKeyCol(pCxt, pOp->pRight, rightExprBlkId, &pRightExpr);
    } else if (NULL != pJoin->leftPrimExpr && QUERY_NODE_FUNCTION == nodeType(pOp->pRight) &&
               (FUNCTION_TYPE_TIMETRUNCATE == ((SFunctionNode*)pOp->pRight)->funcType ||
                FUNCTION_TYPE_TIMESTAMP_SCALE == ((SFunctionNode*)pOp->pRight)->funcType)) {
      code = createHashJoinKeyColFromPrimExpr(pCxt, pJoin->leftPrimExpr, &pRightExpr);
    } else {
      code = createHashJoinExprTarget(pCxt, pOp->pRight, pLeftDesc, &pRightExpr);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListStrictAppend(pJoin->pOnLeft, pRightExpr);
    }
    if (TSDB_CODE_SUCCESS == code) {
      if (QUERY_NODE_COLUMN == nodeType(pOp->pLeft)) {
        code = nodesCloneNode(pOp->pLeft, &pLeftExpr);
      } else if (QUERY_NODE_TARGET == nodeType(pOp->pLeft)) {
        code = createHashJoinKeyColFromPrimExpr(pCxt, pOp->pLeft, &pLeftExpr);
      } else if (QUERY_NODE_FUNCTION == nodeType(pOp->pLeft) &&
                 FUNCTION_TYPE_TBNAME == ((SFunctionNode*)pOp->pLeft)->funcType) {
        code = createHashJoinKeyCol(pCxt, pOp->pLeft, leftExprBlkId, &pLeftExpr);
      } else if (NULL != pJoin->rightPrimExpr && QUERY_NODE_FUNCTION == nodeType(pOp->pLeft) &&
                 (FUNCTION_TYPE_TIMETRUNCATE == ((SFunctionNode*)pOp->pLeft)->funcType ||
                  FUNCTION_TYPE_TIMESTAMP_SCALE == ((SFunctionNode*)pOp->pLeft)->funcType)) {
        code = createHashJoinKeyColFromPrimExpr(pCxt, pJoin->rightPrimExpr, &pLeftExpr);
      } else {
        code = createHashJoinExprTarget(pCxt, pOp->pLeft, pRightDesc, &pLeftExpr);
      }
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListStrictAppend(pJoin->pOnRight, pLeftExpr);
    }
  } else {
    planError("Invalid join equal cond, lbid:%" PRId64 ", rbid:%" PRId64 ", oplid:%" PRId64 ", oprid:%" PRId64,
              lBlkId, rBlkId, leftExprBlkId, rightExprBlkId);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = normalizeHashJoinKeyTypes(pLeftExpr, pRightExpr);
  }
  return code;
}

static int32_t extractHashJoinOnCols(SPhysiPlanContext* pCxt, SDataBlockDescNode* pLeftDesc,
                                     SDataBlockDescNode* pRightDesc, SNode* pEq, SHashJoinPhysiNode* pJoin) {
  if (NULL == pEq) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  if (QUERY_NODE_OPERATOR == nodeType(pEq)) {
    code = extractHashJoinOpCols(pCxt, pLeftDesc, pRightDesc, pEq, pJoin);
  } else if (QUERY_NODE_LOGIC_CONDITION == nodeType(pEq)) {
    SLogicConditionNode* pLogic = (SLogicConditionNode*)pEq;
    SNode*               pNode = NULL;
    FOREACH(pNode, pLogic->pParameterList) {
      code = extractHashJoinOpCols(pCxt, pLeftDesc, pRightDesc, pNode, pJoin);
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

static int32_t createHashJoinColList(SPhysiPlanContext* pCxt, SDataBlockDescNode* pLeftDesc,
                                     SDataBlockDescNode* pRightDesc, SNode* pEq1, SNode* pEq2, SNode* pEq3,
                                     SHashJoinPhysiNode* pJoin) {
  int32_t code = TSDB_CODE_SUCCESS;
  pJoin->pOnLeft = NULL;
  code = nodesMakeList(&pJoin->pOnLeft);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  pJoin->pOnRight = NULL;
  code = nodesMakeList(&pJoin->pOnRight);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  code = extractHashJoinOnCols(pCxt, pLeftDesc, pRightDesc, pEq1, pJoin);
  if (TSDB_CODE_SUCCESS == code) {
    code = extractHashJoinOnCols(pCxt, pLeftDesc, pRightDesc, pEq2, pJoin);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = extractHashJoinOnCols(pCxt, pLeftDesc, pRightDesc, pEq3, pJoin);
  }
  if (TSDB_CODE_SUCCESS == code && pJoin->pOnLeft->length <= 0) {
    planError("Invalid join equal column num: %d", pJoin->pOnLeft->length);
    code = TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  return code;
}

static int32_t sortHashJoinTargets(int64_t lBlkId, int64_t rBlkId, SHashJoinPhysiNode* pJoin) {
  SNode*     pNode = NULL;
  SSHashObj* pHash = tSimpleHashInit(pJoin->pTargets->length, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
  if (NULL == pHash) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  SNodeList* pNew = NULL;
  int32_t    code = nodesMakeList(&pNew);

  if (TSDB_CODE_SUCCESS == code) {
    FOREACH(pNode, pJoin->pTargets) {
      SColumnNode* pCol = (SColumnNode*)pNode;
      char*        pName = NULL;
      int32_t      len = 0;
      int32_t      cap = 0;
      code = getSlotKey(pNode, NULL, &pName, &len, 0, &cap);
      if (TSDB_CODE_SUCCESS == code) {
        code = tSimpleHashPut(pHash, pName, len, &pCol, POINTER_BYTES);
      }
      taosMemoryFree(pName);
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    nodesClearList(pJoin->pTargets);
    pJoin->pTargets = pNew;

    FOREACH(pNode, pJoin->pOnLeft) {
      char*        pName = NULL;
      SColumnNode* pCol = (SColumnNode*)pNode;
      int32_t      len = 0;
      int32_t      cap = 0;
      code = getSlotKey(pNode, NULL, &pName, &len, 0, &cap);
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
      char*        pName = NULL;
      SColumnNode* pCol = (SColumnNode*)pNode;
      int32_t      len = 0;
      int32_t cap = 0;
      code = getSlotKey(pNode, NULL, &pName, &len, 0, &cap);
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

static int32_t setHashJoinPrimColEqCond(SNode* pEqCond, int64_t leftBlkId, int64_t rightBlkId,
                                        SHashJoinPhysiNode* pJoin) {
  int32_t code = 0;
  if (QUERY_NODE_OPERATOR == nodeType(pEqCond)) {
    SOperatorNode* pOp = (SOperatorNode*)pEqCond;
    if (pOp->opType != OP_TYPE_EQUAL) {
      planError("invalid primary cond opType, opType:%d", pOp->opType);
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }

    // Projection-backed subqueries can leave one direct column operand with
    // an unset dataBlockId even after slot binding.  When the other operand
    // identifies its side, the missing side is unambiguous; normalize it
    // before assigning the hash-join primary slots.
    SColumnNode* pLeftCol = NULL;
    SColumnNode* pRightCol = NULL;
    if (QUERY_NODE_COLUMN == nodeType(pOp->pLeft)) {
      pLeftCol = (SColumnNode*)pOp->pLeft;
    } else if (QUERY_NODE_FUNCTION == nodeType(pOp->pLeft)) {
      SFunctionNode* pFunc = (SFunctionNode*)pOp->pLeft;
      SNode* pParam = nodesListGetNode(pFunc->pParameterList, 0);
      if (NULL != pParam && QUERY_NODE_COLUMN == nodeType(pParam)) {
        pLeftCol = (SColumnNode*)pParam;
      }
    }
    if (QUERY_NODE_COLUMN == nodeType(pOp->pRight)) {
      pRightCol = (SColumnNode*)pOp->pRight;
    } else if (QUERY_NODE_FUNCTION == nodeType(pOp->pRight)) {
      SFunctionNode* pFunc = (SFunctionNode*)pOp->pRight;
      SNode* pParam = nodesListGetNode(pFunc->pParameterList, 0);
      if (NULL != pParam && QUERY_NODE_COLUMN == nodeType(pParam)) {
        pRightCol = (SColumnNode*)pParam;
      }
    }
    if (NULL != pLeftCol && NULL != pRightCol) {
      bool leftKnown = (pLeftCol->dataBlockId == leftBlkId || pLeftCol->dataBlockId == rightBlkId);
      bool rightKnown = (pRightCol->dataBlockId == leftBlkId || pRightCol->dataBlockId == rightBlkId);
      if (leftKnown && rightKnown && pLeftCol->dataBlockId == pRightCol->dataBlockId) {
        pLeftCol->dataBlockId = leftBlkId;
        pRightCol->dataBlockId = rightBlkId;
      } else if (!leftKnown && rightKnown) {
        pLeftCol->dataBlockId = (pRightCol->dataBlockId == rightBlkId) ? leftBlkId : rightBlkId;
      } else if (leftKnown && !rightKnown) {
        pRightCol->dataBlockId = (pLeftCol->dataBlockId == leftBlkId) ? rightBlkId : leftBlkId;
      } else if (!leftKnown && !rightKnown) {
        pLeftCol->dataBlockId = leftBlkId;
        pRightCol->dataBlockId = rightBlkId;
      }
    }

    switch (nodeType(pOp->pLeft)) {
      case QUERY_NODE_COLUMN: {
        SColumnNode* pCol = (SColumnNode*)pOp->pLeft;
        if (leftBlkId == pCol->dataBlockId) {
          pJoin->leftPrimSlotId = pCol->slotId;
        } else if (rightBlkId == pCol->dataBlockId) {
          pJoin->rightPrimSlotId = pCol->slotId;
        } else {
          planError("invalid primary key col equal cond, leftBlockId:%" PRId64, pCol->dataBlockId);
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        break;
      }
      case QUERY_NODE_VALUE: {
        pJoin->leftPrimExpr = NULL;
        code = nodesCloneNode(pOp->pLeft, &pJoin->leftPrimExpr);
        break;
      }
      case QUERY_NODE_FUNCTION: {
        SFunctionNode* pFunc = (SFunctionNode*)pOp->pLeft;
        if (FUNCTION_TYPE_TIMETRUNCATE != pFunc->funcType &&
            FUNCTION_TYPE_TIMESTAMP_SCALE != pFunc->funcType) {
          planError("invalid primary cond left function type, leftFuncType:%d", pFunc->funcType);
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        SNode* pParam = nodesListGetNode(pFunc->pParameterList, 0);
        if (NULL == pParam || QUERY_NODE_COLUMN != nodeType(pParam)) {
          planError("invalid primary cond left timetruncate param");
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
          planError("invalid primary key col equal cond, leftBlockId:%" PRId64, pCol->dataBlockId);
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
          planError("invalid primary key col equal cond, rightBlockId:%" PRId64, pCol->dataBlockId);
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        break;
      }
      case QUERY_NODE_VALUE: {
        pJoin->rightPrimExpr = NULL;
        code = nodesCloneNode(pOp->pRight, &pJoin->rightPrimExpr);
        break;
      }
      case QUERY_NODE_FUNCTION: {
        SFunctionNode* pFunc = (SFunctionNode*)pOp->pRight;
        if (FUNCTION_TYPE_TIMETRUNCATE != pFunc->funcType &&
            FUNCTION_TYPE_TIMESTAMP_SCALE != pFunc->funcType) {
          planError("invalid primary cond right function type, rightFuncType:%d", pFunc->funcType);
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        SNode* pParam = nodesListGetNode(pFunc->pParameterList, 0);
        if (NULL == pParam || QUERY_NODE_COLUMN != nodeType(pParam)) {
          planError("invalid primary cond right timetruncate param");
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
          planError("invalid primary key col equal cond, rightBlockId:%" PRId64, pCol->dataBlockId);
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

static bool getTbnameFuncTableAlias(SFunctionNode* pFunc, char* pAlias, int32_t aliasLen) {
  if (FUNCTION_TYPE_TBNAME != pFunc->funcType) {
    return false;
  }

  SNode* pParam = nodesListGetNode(pFunc->pParameterList, 0);
  if (NULL != pParam && QUERY_NODE_VALUE == nodeType(pParam)) {
    SValueNode* pVal = (SValueNode*)pParam;
    tstrncpy(pAlias, pVal->literal, aliasLen);
    return pAlias[0] != '\0';
  }

  char* pSuffix = NULL;
  if (NULL != pFunc->node.userAlias) {
    pSuffix = strstr(pFunc->node.userAlias, ".tbname");
  }
  if (NULL != pSuffix && pSuffix != pFunc->node.userAlias) {
    int32_t len = TMIN((int32_t)(pSuffix - pFunc->node.userAlias), aliasLen - 1);
    memcpy(pAlias, pFunc->node.userAlias, len);
    pAlias[len] = '\0';
    return true;
  }

  return false;
}

static bool logicNodeHasTableAlias(SLogicNode* pLogic, const char* pAlias) {
  if (NULL == pLogic || NULL == pLogic->pTargets || NULL == pAlias || '\0' == pAlias[0]) {
    return false;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pLogic->pTargets) {
    if (QUERY_NODE_COLUMN == nodeType(pNode) && 0 == strcmp(((SColumnNode*)pNode)->tableAlias, pAlias)) {
      return true;
    }
    if (QUERY_NODE_FUNCTION == nodeType(pNode) && FUNCTION_TYPE_TBNAME == ((SFunctionNode*)pNode)->funcType) {
      char funcAlias[TSDB_TABLE_NAME_LEN] = {0};
      if (getTbnameFuncTableAlias((SFunctionNode*)pNode, funcAlias, sizeof(funcAlias)) &&
          0 == strcmp(funcAlias, pAlias)) {
        return true;
      }
    }
  }

  return false;
}

static int32_t ensureHashJoinTbnameKeySlot(SPhysiPlanContext* pCxt, SFunctionNode* pFunc,
                                           SJoinLogicNode* pJoinLogicNode,
                                           SDataBlockDescNode* pLeftDesc,
                                           SDataBlockDescNode* pRightDesc) {
  char alias[TSDB_TABLE_NAME_LEN] = {0};
  if (!getTbnameFuncTableAlias(pFunc, alias, sizeof(alias))) {
    return TSDB_CODE_SUCCESS;
  }

  SLogicNode*          pLeftLogic = (SLogicNode*)nodesListGetNode(pJoinLogicNode->node.pChildren, 0);
  SLogicNode*          pRightLogic = (SLogicNode*)nodesListGetNode(pJoinLogicNode->node.pChildren, 1);
  SDataBlockDescNode*  pDesc = NULL;
  if (logicNodeHasTableAlias(pLeftLogic, alias)) {
    pDesc = pLeftDesc;
  } else if (logicNodeHasTableAlias(pRightLogic, alias)) {
    pDesc = pRightDesc;
  } else {
    planError("Invalid hash join tbname key alias:%s", alias);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  SNode*  pNode = NULL;
  int32_t code = nodesCloneNode((SNode*)pFunc, &pNode);
  if (TSDB_CODE_SUCCESS == code) {
    code = addDataBlockSlot(pCxt, &pNode, pDesc);
  }
  nodesDestroyNode(pNode);
  return code;
}

static int32_t ensureHashJoinTbnameKeySlots(SPhysiPlanContext* pCxt, SNode* pEqCond,
                                            SJoinLogicNode* pJoinLogicNode,
                                            SDataBlockDescNode* pLeftDesc,
                                            SDataBlockDescNode* pRightDesc) {
  if (NULL == pEqCond) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  if (QUERY_NODE_OPERATOR == nodeType(pEqCond)) {
    SOperatorNode* pOp = (SOperatorNode*)pEqCond;
    if (QUERY_NODE_FUNCTION == nodeType(pOp->pLeft) &&
        FUNCTION_TYPE_TBNAME == ((SFunctionNode*)pOp->pLeft)->funcType) {
      code = ensureHashJoinTbnameKeySlot(pCxt, (SFunctionNode*)pOp->pLeft, pJoinLogicNode, pLeftDesc, pRightDesc);
    }
    if (TSDB_CODE_SUCCESS == code && QUERY_NODE_FUNCTION == nodeType(pOp->pRight) &&
        FUNCTION_TYPE_TBNAME == ((SFunctionNode*)pOp->pRight)->funcType) {
      code = ensureHashJoinTbnameKeySlot(pCxt, (SFunctionNode*)pOp->pRight, pJoinLogicNode, pLeftDesc, pRightDesc);
    }
  } else if (QUERY_NODE_LOGIC_CONDITION == nodeType(pEqCond)) {
    SNode* pNode = NULL;
    FOREACH(pNode, ((SLogicConditionNode*)pEqCond)->pParameterList) {
      code = ensureHashJoinTbnameKeySlots(pCxt, pNode, pJoinLogicNode, pLeftDesc, pRightDesc);
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    }
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
  pJoin->multiGroupJoin = pJoinLogicNode->multiGroupJoin;
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
    code = setJoinNodeSlotId(pCxt, pJoinLogicNode, pLeftDesc->dataBlockId, pRightDesc->dataBlockId,
                             pJoinLogicNode->pPrimKeyEqCond, &pJoin->pPrimKeyCond);
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
    code = ensureHashJoinTbnameKeySlots(pCxt, pJoinLogicNode->pColEqCond, pJoinLogicNode, pLeftDesc, pRightDesc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = ensureHashJoinTbnameKeySlots(pCxt, pJoinLogicNode->pTagEqCond, pJoinLogicNode, pLeftDesc, pRightDesc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setJoinNodeSlotId(pCxt, pJoinLogicNode, pLeftDesc->dataBlockId, pRightDesc->dataBlockId,
                             pJoinLogicNode->pColEqCond, &pJoin->pColEqCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setJoinNodeSlotId(pCxt, pJoinLogicNode, pLeftDesc->dataBlockId, pRightDesc->dataBlockId,
                             pJoinLogicNode->pTagEqCond, &pJoin->pTagEqCond);
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
  if (TSDB_CODE_SUCCESS == code) {
    // Output targets retain outer aliases (a/b); use them only to disambiguate
    // same-name slots across the two child descriptors.
    code = setJoinTargetListSlotId(pCxt, pJoinLogicNode, pLeftDesc->dataBlockId, pRightDesc->dataBlockId,
                                   pJoinLogicNode->node.pTargets, &pJoin->pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createHashJoinColList(pCxt, pLeftDesc, pRightDesc, pJoin->pPrimKeyCond, pJoin->pColEqCond,
                                 pJoin->pTagEqCond, pJoin);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = sortHashJoinTargets(pLeftDesc->dataBlockId, pRightDesc->dataBlockId, pJoin);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = fqAppendJoinConditionTargets(pCxt, pJoinLogicNode, pLeftDesc->dataBlockId, pRightDesc->dataBlockId,
                                        pJoin->pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = addDataBlockSlots(pCxt, pJoin->pTargets, pJoin->node.pOutputDataBlockDesc);
  }
  if (TSDB_CODE_SUCCESS == code && NULL != pOnCond) {
    code = setNodeSlotId(pCxt, pJoin->node.pOutputDataBlockDesc->dataBlockId, -1, pOnCond, &pJoin->pFullOnCond);
    planDebug("FQ-joinSlots: hashJoin bind fullOnCond code=0x%x outputBlockId=%" PRId64 " cond=%p bound=%p",
              code, pJoin->node.pOutputDataBlockDesc->dataBlockId, pOnCond, pJoin->pFullOnCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setConditionsSlotId(pCxt, (const SLogicNode*)pJoinLogicNode, (SPhysiNode*)pJoin);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pJoin;
  } else {
    nodesDestroyNode((SNode*)pJoin);
  }

  return code;
}

static int32_t createThetaJoinPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren,
                                        SJoinLogicNode* pJoinLogicNode, SPhysiNode** pPhyNode) {
  SThetaJoinPhysiNode* pJoin =
      (SThetaJoinPhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pJoinLogicNode, QUERY_NODE_PHYSICAL_PLAN_THETA_JOIN);
  if (NULL == pJoin) return terrno;

  SDataBlockDescNode* pLeftDesc = ((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc;
  SDataBlockDescNode* pRightDesc = ((SPhysiNode*)nodesListGetNode(pChildren, 1))->pOutputDataBlockDesc;
  int32_t code = TSDB_CODE_SUCCESS;
  pJoin->joinType = pJoinLogicNode->joinType;
  pJoin->subType = pJoinLogicNode->subType;
  pJoin->leftPrimSlotId = (int32_t)pLeftDesc->dataBlockId;
  pJoin->rightPrimSlotId = (int32_t)pRightDesc->dataBlockId;
  if (pJoinLogicNode->isCrossJoin) {
    code = addCrossJoinChildAliasKeys(
        pCxt, (SLogicNode*)nodesListGetNode(pJoinLogicNode->node.pChildren, 0), pLeftDesc);
    if (TSDB_CODE_SUCCESS == code) {
      code = addCrossJoinChildAliasKeys(
          pCxt, (SLogicNode*)nodesListGetNode(pJoinLogicNode->node.pChildren, 1), pRightDesc);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = setJoinTargetListSlotId(pCxt, pJoinLogicNode, pLeftDesc->dataBlockId, pRightDesc->dataBlockId,
                                     pJoinLogicNode->node.pTargets, &pJoin->pTargets);
    }
  } else {
    code = setListSlotId(pCxt, pLeftDesc->dataBlockId, pRightDesc->dataBlockId, pJoinLogicNode->node.pTargets,
                         &pJoin->pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = fqAppendJoinConditionTargets(pCxt, pJoinLogicNode, pLeftDesc->dataBlockId, pRightDesc->dataBlockId,
                                        pJoin->pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) code = addDataBlockSlots(pCxt, pJoin->pTargets, pJoin->node.pOutputDataBlockDesc);
  if (TSDB_CODE_SUCCESS == code) {
    code = setNodeSlotId(pCxt, pJoin->node.pOutputDataBlockDesc->dataBlockId, -1, pJoinLogicNode->pFullOnCond,
                         &pJoin->pFullOnCond);
  }
  if (TSDB_CODE_SUCCESS == code) code = setConditionsSlotId(pCxt, (SLogicNode*)pJoinLogicNode, (SPhysiNode*)pJoin);
  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pJoin;
  } else {
    nodesDestroyNode((SNode*)pJoin);
  }
  return code;
}

static int32_t createJoinPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SJoinLogicNode* pJoinLogicNode,
                                   SPhysiNode** pPhyNode) {
  if (pJoinLogicNode->multiGroupJoin) {
    if (JOIN_ALGO_HASH != pJoinLogicNode->joinAlgo) {
      planError("multi-group JOIN requires hash algorithm, actual:%d", pJoinLogicNode->joinAlgo);
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }
    int32_t code = createHashJoinPhysiNode(pCxt, pChildren, pJoinLogicNode, pPhyNode);
    if (TSDB_CODE_SUCCESS == code && (QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN != nodeType(*pPhyNode) ||
                                      !((SHashJoinPhysiNode*)*pPhyNode)->multiGroupJoin)) {
      planError("%s", "marked logical JOIN did not produce a marked physical hash JOIN");
      nodesDestroyNode((SNode*)*pPhyNode);
      *pPhyNode = NULL;
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }
    return code;
  }

  switch (pJoinLogicNode->joinAlgo) {
    case JOIN_ALGO_MERGE:
      return createMergeJoinPhysiNode(pCxt, pChildren, pJoinLogicNode, pPhyNode);
    case JOIN_ALGO_HASH:
      return createHashJoinPhysiNode(pCxt, pChildren, pJoinLogicNode, pPhyNode);
    case JOIN_ALGO_THETA:
      return createThetaJoinPhysiNode(pCxt, pChildren, pJoinLogicNode, pPhyNode);
    default:
      planError("Invalid join algorithm:%d", pJoinLogicNode->joinAlgo);
      break;
  }

  return TSDB_CODE_FAILED;
}

static int32_t createVirtualScanCols(SPhysiPlanContext* pCxt, SVirtualScanPhysiNode* pScanPhysiNode, SNodeList* pScanCols) {
  if (NULL == pScanCols) {
    return TSDB_CODE_SUCCESS;
  }

  pScanPhysiNode->scan.pScanCols = NULL;
  int32_t code = nodesCloneList(pScanCols, &pScanPhysiNode->scan.pScanCols);
  if (NULL == pScanPhysiNode->scan.pScanCols) {
    return code;
  }
  return sortScanCols(pScanPhysiNode->scan.pScanCols);
}

static const SColumnNode* getFederatedScanColumn(const SNode* pNode) {
  if (pNode != NULL && QUERY_NODE_TARGET == nodeType(pNode)) {
    pNode = ((const STargetNode*)pNode)->pExpr;
  }
  return (pNode != NULL && QUERY_NODE_COLUMN == nodeType(pNode)) ? (const SColumnNode*)pNode : NULL;
}

static bool federatedOutputNameMatchesScanColumn(const char* pOutputName, const SColumnNode* pCol) {
  if (pOutputName == NULL || pOutputName[0] == '\0' || pCol == NULL) {
    return false;
  }

  const char* pRefName = pCol->hasRef && pCol->refColName[0] != '\0' ? pCol->refColName : pCol->colName;
  return (pRefName[0] != '\0' && strcasecmp(pOutputName, pRefName) == 0) ||
         (pCol->colName[0] != '\0' && strcasecmp(pOutputName, pCol->colName) == 0) ||
         (pCol->node.aliasName[0] != '\0' && strcasecmp(pOutputName, pCol->node.aliasName) == 0) ||
         (pCol->node.userAlias[0] != '\0' && strcasecmp(pOutputName, pCol->node.userAlias) == 0);
}

static int16_t getFederatedOutputSlot(const SFederatedScanPhysiNode* pScan, const SColumnNode* pCol,
                                      int32_t outputIdx) {
  if (pScan == NULL || pCol == NULL || pScan->pColTypeMappings == NULL ||
      outputIdx < 0 || outputIdx >= pScan->numColTypeMappings || pScan->node.pOutputDataBlockDesc == NULL ||
      pScan->node.pOutputDataBlockDesc->pSlots == NULL ||
      outputIdx >= LIST_LENGTH(pScan->node.pOutputDataBlockDesc->pSlots)) {
    return -1;
  }

  const SExtColTypeMapping* pMapping = &pScan->pColTypeMappings[outputIdx];
  SSlotDescNode* pSlot = (SSlotDescNode*)nodesListGetNode(pScan->node.pOutputDataBlockDesc->pSlots, outputIdx);
  if (pMapping->tdType.type != pCol->node.resType.type ||
      !federatedOutputNameMatchesScanColumn(pMapping->colName, pCol) || pSlot == NULL || !pSlot->output ||
      pSlot->reserve) {
    return -1;
  }
  return pSlot->slotId;
}

static int32_t prioritizeFederatedOutputSlot(SSlotIndex* pIndex, int16_t outputSlotId) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (pIndex == NULL || pIndex->pSlotIdsInfo == NULL || taosArrayGetSize(pIndex->pSlotIdsInfo) == 0) {
    PLAN_ERR_JRET(TSDB_CODE_INVALID_PARA);
  }

  SSlotIdInfo* pFirst = taosArrayGet(pIndex->pSlotIdsInfo, 0);
  if (pFirst->slotId == outputSlotId) {
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 1; i < taosArrayGetSize(pIndex->pSlotIdsInfo); ++i) {
    SSlotIdInfo* pInfo = taosArrayGet(pIndex->pSlotIdsInfo, i);
    if (pInfo->slotId == outputSlotId) {
      SSlotIdInfo tmp = *pFirst;
      *pFirst = *pInfo;
      *pInfo = tmp;
      return TSDB_CODE_SUCCESS;
    }
  }

  SSlotIdInfo oldFirst = *pFirst;
  SSlotIdInfo outputInfo = {.slotId = outputSlotId, .set = false};
  if (NULL == taosArrayPush(pIndex->pSlotIdsInfo, &oldFirst)) {
    PLAN_ERR_JRET(terrno);
  }
  pFirst = taosArrayGet(pIndex->pSlotIdsInfo, 0);
  *pFirst = outputInfo;
  return code;

_return:
  planError("%s failed since %s", __func__, tstrerror(code));
  return code;
}

static int32_t registerVtableFederatedOutputSlots(SPhysiPlanContext* pCxt, SFederatedScanPhysiNode* pScan) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  char*   pKey = NULL;

  if (pCxt == NULL || pScan == NULL || pScan->pScanCols == NULL || pScan->node.pOutputDataBlockDesc == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SHashObj* pHash = taosArrayGetP(pCxt->pLocationHelper, pScan->node.pOutputDataBlockDesc->dataBlockId);
  QUERY_CHECK_NULL(pHash, code, lino, _return, terrno);

  // createFederatedScanPhysiNode keeps scan columns, type mappings, and output
  // slots in the same order; reserve-only scan columns are appended last.
  int32_t outputIdx = 0;
  SNode*  pNode = NULL;
  FOREACH(pNode, pScan->pScanCols) {
    const SNode* pScanExpr = pNode;
    if (NULL != pScanExpr && QUERY_NODE_TARGET == nodeType(pScanExpr)) {
      pScanExpr = ((const STargetNode*)pScanExpr)->pExpr;
    }
    const SColumnNode* pCol = getFederatedScanColumn(pNode);
    int16_t            outputSlotId = -1;
    bool               mappedOutput = outputIdx < pScan->numColTypeMappings;
    if (mappedOutput) {
      outputSlotId = getFederatedOutputSlot(pScan, pCol, outputIdx);
    } else {
      SSlotDescNode* pSlot =
          (SSlotDescNode*)nodesListGetNode(pScan->node.pOutputDataBlockDesc->pSlots, outputIdx);
      outputSlotId = NULL != pSlot ? pSlot->slotId : -1;
    }
    ++outputIdx;
    if (NULL == pScanExpr || outputSlotId < 0 || (mappedOutput && NULL == pCol)) {
      planError("Invalid federated scan expression or output slot, expr=%p, outputSlotId=%d", pScanExpr,
                outputSlotId);
      PLAN_ERR_JRET(TSDB_CODE_INVALID_PARA);
    }

    int32_t keyLen = 0;
    int32_t cap = 0;
    PLAN_ERR_JRET(getSlotKey((SNode*)pScanExpr, NULL, &pKey, &keyLen, 0, &cap));

    SSlotIndex* pIndex = taosHashGet(pHash, pKey, keyLen);
    if (pIndex == NULL) {
      code = putSlotToHashImpl(pScan->node.pOutputDataBlockDesc->dataBlockId, outputSlotId, pKey, keyLen, pHash);
    } else {
      code = prioritizeFederatedOutputSlot(pIndex, outputSlotId);
    }
    taosMemoryFreeClear(pKey);
    PLAN_ERR_JRET(code);
  }
  return code;

_return:
  taosMemoryFree(pKey);
  planError("%s failed since %s", __func__, tstrerror(code));
  return code;
}

static int32_t initializeDynamicVtableFederatedOutput(SPhysiPlanContext* pCxt,
                                                      const SVirtualScanLogicNode* pVirtualScan,
                                                      SFederatedScanPhysiNode* pFederatedScan) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (NULL == pCxt || NULL == pVirtualScan || NULL == pFederatedScan ||
      NULL == pFederatedScan->node.pOutputDataBlockDesc ||
      (NULL != pFederatedScan->pScanCols && LIST_LENGTH(pFederatedScan->pScanCols) > 0)) {
    return TSDB_CODE_SUCCESS;
  }

  PLAN_ERR_JRET(nodesCloneList(pVirtualScan->pScanCols, &pFederatedScan->pScanCols));
  if (NULL == pFederatedScan->pScanCols) {
    PLAN_ERR_JRET(TSDB_CODE_PLAN_INVALID_FEDERATED_SCAN);
  }

  // A dynamic FederatedScan chooses its real table and SELECT list only after
  // DynQueryCtrl supplies a batch.  Its physical template still needs the
  // complete virtual-row schema, though: every table in the batch is
  // normalized into this schema before it reaches VirtualScan.  Without this
  // common shape, the second table in a source batch inherits the first
  // table's slot mapping and writes values into the wrong virtual column.
  PLAN_ERR_JRET(addDataBlockSlotsImpl(pCxt, pFederatedScan->pScanCols,
                                      pFederatedScan->node.pOutputDataBlockDesc,
                                      NULL, true, false));

_return:
  if (TSDB_CODE_SUCCESS != code) {
    planError("%s failed since %s", __func__, tstrerror(code));
  }
  return code;
}

static int32_t createVirtualTableScanPhysiNodeFinalize(SPhysiPlanContext* pCxt,
                                                       SNodeList* pChild,
                                                       SVirtualScanLogicNode* pScanLogicNode,
                                                       SVirtualScanPhysiNode* pScanPhysiNode,
                                                       SPhysiNode** pPhyNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNodeList* pCondCols = NULL;
  SNodeList* pCondScanCols = NULL;

  PLAN_ERR_JRET(createVirtualScanCols(pCxt, pScanPhysiNode, pScanLogicNode->pScanCols));
  PLAN_ERR_JRET(addDataBlockSlots(pCxt, pScanPhysiNode->scan.pScanCols, pScanPhysiNode->scan.node.pOutputDataBlockDesc));
  if (NULL != pScanLogicNode->pScanPseudoCols) {
    pScanPhysiNode->scan.pScanPseudoCols = NULL;
    PLAN_ERR_JRET(nodesCloneList(pScanLogicNode->pScanPseudoCols, &pScanPhysiNode->scan.pScanPseudoCols));
    PLAN_ERR_JRET(addDataBlockSlots(pCxt, pScanPhysiNode->scan.pScanPseudoCols, pScanPhysiNode->scan.node.pOutputDataBlockDesc));
  }

  if (NULL != pScanLogicNode->node.pConditions) {
    PLAN_ERR_JRET(nodesCollectColumnsFromNode(pScanLogicNode->node.pConditions, NULL, COLLECT_COL_TYPE_ALL, &pCondCols));
    if (pCondCols != NULL) {
      PLAN_ERR_JRET(nodesCloneList(pCondCols, &pCondScanCols));
    }
    PLAN_ERR_JRET(addDataBlockSlots(pCxt, pCondCols, pScanPhysiNode->scan.node.pOutputDataBlockDesc));
  }

  PLAN_ERR_JRET(setConditionsSlotId(pCxt, (const SLogicNode*)pScanLogicNode, (SPhysiNode*)pScanPhysiNode));

  pScanPhysiNode->scan.uid = pScanLogicNode->tableId;
  pScanPhysiNode->scan.suid = pScanLogicNode->stableId;
  pScanPhysiNode->scan.tableType = pScanLogicNode->tableType;
  memcpy(&pScanPhysiNode->scan.tableName, &pScanLogicNode->tableName, sizeof(SName));
  pScanPhysiNode->scanAllCols = pScanLogicNode->scanAllCols;
  // clone the const-ref tag list right after the scalar fields and BEFORE the sibling
  // list clones (pTagRefSources/pLocalTags) that follow -- not before the slot
  // machinery above (createVirtualScanCols/addDataBlockSlots/setConditionsSlotId may
  // already have failed by this point); the physi node must own its own copy.
  // Unlike pRefTagCols, this list deliberately gets NO setMultiBlockSlotId rewrite in
  // createVirtualTableScanPhysiNode: its only runtime consumer (ensureRefTagsResolved)
  // reads logical identity fields only (colId, refDb/Table/ColName, resType) and never
  // slotId/dataBlockId -- output placement flows through scan.pScanPseudoCols' TARGET
  // slots (assigned by addDataBlockSlots above) and findResolvedTagVal's colId lookup.
  // A rewrite would fail outright anyway: no child operator emits const-ref tags (by
  // design no TagRefSource scan node is built for them), so the merged-children slot
  // hash lookup would raise TSDB_CODE_PLAN_SLOT_NOT_FOUND.
  if (pScanLogicNode->pConstRefTags) {
    PLAN_ERR_JRET(nodesCloneList(pScanLogicNode->pConstRefTags, &pScanPhysiNode->pConstRefTags));
  }

  // Clone TagRef fields from logic node to physical node
  pScanPhysiNode->hasTagRef = pScanLogicNode->hasTagRef;
  pScanPhysiNode->hasLocalTag = pScanLogicNode->hasLocalTag;
  pScanPhysiNode->hasConstRefTag = pScanLogicNode->hasConstRefTag;
  pScanPhysiNode->mgmtEpSet = pCxt->pPlanCxt->mgmtEpSet;  // for const-ref chain remote hops

  if (pScanLogicNode->pTagRefSources) {
    PLAN_ERR_JRET(nodesCloneList(pScanLogicNode->pTagRefSources, &pScanPhysiNode->pTagRefSources));
  }
  if (pScanLogicNode->pLocalTags) {
    PLAN_ERR_JRET(nodesCloneList(pScanLogicNode->pLocalTags, &pScanPhysiNode->pLocalTags));
  }
  if (NULL == pScanPhysiNode->pLocalTags || LIST_LENGTH(pScanPhysiNode->pLocalTags) <= 0) {
    if (pScanPhysiNode->pLocalTags != NULL && LIST_LENGTH(pScanPhysiNode->pLocalTags) <= 0) {
      nodesDestroyList(pScanPhysiNode->pLocalTags);
      pScanPhysiNode->pLocalTags = NULL;
    }
    PLAN_ERR_JRET(appendPhysiLocalTagsFromList(pScanLogicNode->pScanPseudoCols, &pScanPhysiNode->pLocalTags));
    pScanPhysiNode->hasLocalTag = (pScanPhysiNode->pLocalTags != NULL && LIST_LENGTH(pScanPhysiNode->pLocalTags) > 0);
  }
  if (pScanLogicNode->pRefTagCols) {
    PLAN_ERR_JRET(nodesCloneList(pScanLogicNode->pRefTagCols, &pScanPhysiNode->pRefTagCols));
  }
  if (pScanLogicNode->pTagFilterCond) {
    PLAN_ERR_JRET(nodesCloneNode(pScanLogicNode->pTagFilterCond, &pScanPhysiNode->pTagFilterCond));
  }

  SNode*  pNode = NULL;
  int32_t condIdx = 0;
  FOREACH(pNode, pCondScanCols) {
    if (nodeType(pNode) != QUERY_NODE_COLUMN) {
      ++condIdx;
      continue;
    }

    SColumnNode* pCondCol = (SColumnNode*)pNode;
    if (pCondCol->colType == COLUMN_TYPE_TAG) {
      if (pCondCol->hasRef) {
        PLAN_ERR_JRET(appendPhysiScanColumn(&pScanPhysiNode->pRefTagCols, pCondCol));
      } else {
        PLAN_ERR_JRET(appendPhysiScanColumn(&pScanPhysiNode->scan.pScanPseudoCols, pCondCol));
      }
    } else {
      PLAN_ERR_JRET(appendPhysiScanColumn(&pScanPhysiNode->scan.pScanCols, pCondCol));
    }
    ++condIdx;
  }

  // Mark every federate-scan downstream as feeding a virtual-table scan so
  // the executor can gate vtable-specific behavior (e.g. per-block TIMESTAMP
  // precision conversion) at plan time without the parent operator reaching
  // into child state at runtime.
  if (pChild != NULL) {
    SNode* pChildNode = NULL;
    FOREACH(pChildNode, pChild) {
      if (pChildNode != NULL &&
          nodeType(pChildNode) == QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN) {
        SFederatedScanPhysiNode* pFedScan = (SFederatedScanPhysiNode*)pChildNode;
        pFedScan->underVTableScan = true;
        PLAN_ERR_JRET(initializeDynamicVtableFederatedOutput(pCxt, pScanLogicNode, pFedScan));
        PLAN_ERR_JRET(registerVtableFederatedOutputSlots(pCxt, pFedScan));
      }
    }
  }
  *pPhyNode = (SPhysiNode*)pScanPhysiNode;
  nodesDestroyList(pCondCols);
  nodesDestroyList(pCondScanCols);
  return code;

_return:
  nodesDestroyList(pCondCols);
  nodesDestroyList(pCondScanCols);
  nodesDestroyNode((SNode*)pScanPhysiNode);
  return code;
}

static int32_t createVirtualTableScanPhysiNode(SPhysiPlanContext* pCxt, SSubplan* pSubplan, SNodeList* pChildren,
                                               SVirtualScanLogicNode* pScanLogicNode, SPhysiNode** pPhyNode) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  SVirtualScanPhysiNode * pVirtualScan =
      (SVirtualScanPhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pScanLogicNode, QUERY_NODE_PHYSICAL_PLAN_VIRTUAL_TABLE_SCAN);
  if (NULL == pVirtualScan) {
    return terrno;
  }

  pVirtualScan->twoPassMode = pScanLogicNode->twoPassMode;

  if (pScanLogicNode->pVgroupList) {
    vgroupInfoToNodeAddr(pScanLogicNode->pVgroupList->vgroups, &pSubplan->execNode);
    pSubplan->execNodeStat.tableNum = pScanLogicNode->pVgroupList->vgroups[0].numOfTable;
  }

  (void)tNameGetFullDbName(&pScanLogicNode->tableName, pSubplan->dbFName);
  PLAN_ERR_RET(createVirtualTableScanPhysiNodeFinalize(pCxt, pChildren, pScanLogicNode, (SVirtualScanPhysiNode*)pVirtualScan, pPhyNode));
  PLAN_ERR_RET(setMultiBlockSlotId(pCxt, pChildren, true, pScanLogicNode->node.pTargets, &pVirtualScan->pTargets));
  if (pVirtualScan->pRefTagCols != NULL) {
    nodesDestroyList(pVirtualScan->pRefTagCols);
    pVirtualScan->pRefTagCols = NULL;
  }
  PLAN_ERR_RET(setMultiBlockSlotId(pCxt, pChildren, true, pScanLogicNode->pRefTagCols, &pVirtualScan->pRefTagCols));
  return code;
}

static int32_t createGroupCachePhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren,
                                         SGroupCacheLogicNode* pLogicNode, SPhysiNode** pPhyNode) {
  SGroupCachePhysiNode* pGrpCache =
      (SGroupCachePhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pLogicNode, QUERY_NODE_PHYSICAL_PLAN_GROUP_CACHE);
  if (NULL == pGrpCache) {
    return terrno;
  }

  pGrpCache->grpColsMayBeNull = pLogicNode->grpColsMayBeNull;
  pGrpCache->grpByUid = pLogicNode->grpByUid;
  pGrpCache->globalGrp = pLogicNode->globalGrp;
  pGrpCache->batchFetch = pLogicNode->batchFetch;
  pGrpCache->twoPassMode = pLogicNode->twoPassMode;
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

static int32_t createTagRefSourcePhysiNode(SPhysiPlanContext* pCxt, SSubplan* pSubplan, SNodeList* pChildren,
                                           STagRefSourceLogicNode* pLogicNode, SPhysiNode** pPhyNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  STagRefSourcePhysiNode* pTagRefSource =
      (STagRefSourcePhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pLogicNode, QUERY_NODE_PHYSICAL_PLAN_TAG_REF_SOURCE);
  if (NULL == pTagRefSource) {
    return terrno;
  }

  // Copy source table information
  memcpy(&pTagRefSource->sourceTableName, &pLogicNode->sourceTableName, sizeof(SName));
  pTagRefSource->sourceSuid = pLogicNode->sourceSuid;
  pTagRefSource->sourceId = pLogicNode->sourceId;
  pTagRefSource->isUsedInFilter = pLogicNode->isUsedInFilter;
  pTagRefSource->isUsedInProjection = pLogicNode->isUsedInProjection;

  // Clone referenced columns
  if (pLogicNode->pRefCols) {
    PLAN_ERR_JRET(nodesCloneList(pLogicNode->pRefCols, &pTagRefSource->pRefCols));
  }

  // Clone vgroup list to physical node
  // Note: After split phase, pVgroupList is transferred from logic node to logic subplan via TSWAP
  // The execNode should already be set in createPhysiSubplan from the logic subplan's vgroup list
  if (pLogicNode->pVgroupList) {
    // vgroup list is still in the logic node (no split or special case)
    int32_t numOfVgroups = pLogicNode->pVgroupList->numOfVgroups;
    pTagRefSource->pVgroupList = taosMemoryCalloc(1, sizeof(SVgroupsInfo) + numOfVgroups * sizeof(SVgroupInfo));
    if (NULL == pTagRefSource->pVgroupList) {
      PLAN_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
    }
    pTagRefSource->pVgroupList->numOfVgroups = numOfVgroups;
    memcpy(pTagRefSource->pVgroupList->vgroups, pLogicNode->pVgroupList->vgroups, numOfVgroups * sizeof(SVgroupInfo));

    // Set subplan execNode from vgroup info for scheduling
    vgroupInfoToNodeAddr(pLogicNode->pVgroupList->vgroups, &pSubplan->execNode);
    pSubplan->execNodeStat.tableNum = pLogicNode->pVgroupList->vgroups[0].numOfTable;
  } else {
    // vgroup list was transferred to logic subplan during split
    // execNode should already be set in createPhysiSubplan
    // We still create a minimal vgroup list for the physical node if needed
    if (pSubplan->execNode.nodeId > 0) {
      pTagRefSource->pVgroupList = taosMemoryCalloc(1, sizeof(SVgroupsInfo) + sizeof(SVgroupInfo));
      if (NULL == pTagRefSource->pVgroupList) {
        PLAN_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
      }
      pTagRefSource->pVgroupList->numOfVgroups = 1;
      // Reverse the vgroupInfoToNodeAddr conversion to populate vgroup info
      pTagRefSource->pVgroupList->vgroups[0].vgId = pSubplan->execNode.nodeId;
      pTagRefSource->pVgroupList->vgroups[0].epSet = pSubplan->execNode.epSet;
      // tableNum should already be set in execNodeStat
    }
  }

  // Create scan columns from referenced columns for source table scan.
  // IMPORTANT: pScanCols must use hasRef=true with the same refDbName/refTableName/refColName
  // as the pTargets columns. getSlotKey() uses these ref fields to generate the slot key,
  // so matching keys ensure addDataBlockSlots reuses existing slots from pTargets
  // rather than creating duplicate slots with different keys.
  if (pLogicNode->pRefCols) {
    PLAN_ERR_JRET(nodesMakeList(&pTagRefSource->pScanCols));
    SNode* pRefCol = NULL;
    FOREACH(pRefCol, pLogicNode->pRefCols) {
      STagRefColumn* pTagRef = (STagRefColumn*)pRefCol;
      SColumnNode* pScanCol = NULL;
      PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pScanCol));
      pScanCol->tableId = pLogicNode->sourceSuid;
      pScanCol->colId = pTagRef->sourceColId;
      pScanCol->colType = COLUMN_TYPE_TAG;
      pScanCol->node.resType.type = pTagRef->dataType;
      pScanCol->node.resType.bytes = pTagRef->bytes;
      tstrncpy(pScanCol->colName, pTagRef->colName, TSDB_COL_NAME_LEN);
      tstrncpy(pScanCol->dbName, pLogicNode->sourceTableName.dbname, TSDB_DB_NAME_LEN);
      tstrncpy(pScanCol->tableName, pLogicNode->sourceTableName.tname, TSDB_TABLE_NAME_LEN);
      // tableAlias must be non-empty so getSlotKey reaches the hasRef branch
      // (empty tableAlias causes early return with just colName as key)
      tstrncpy(pScanCol->tableAlias, pLogicNode->sourceTableName.tname, TSDB_TABLE_NAME_LEN);
      pScanCol->hasRef = true;
      tstrncpy(pScanCol->refDbName, pLogicNode->sourceTableName.dbname, TSDB_DB_NAME_LEN);
      tstrncpy(pScanCol->refTableName, pLogicNode->sourceTableName.tname, TSDB_TABLE_NAME_LEN);
      tstrncpy(pScanCol->refColName, pTagRef->sourceColName, TSDB_COL_NAME_LEN);
      PLAN_ERR_JRET(nodesListStrictAppend(pTagRefSource->pScanCols, (SNode*)pScanCol));
    }
  }

  // addDataBlockSlots reuses existing slots from pTargets since slot keys match
  if (pTagRefSource->pScanCols) {
    PLAN_ERR_JRET(addDataBlockSlots(pCxt, pTagRefSource->pScanCols, pTagRefSource->node.pOutputDataBlockDesc));
  }

  // Set conditions slot id
  PLAN_ERR_JRET(setConditionsSlotId(pCxt, (const SLogicNode*)pLogicNode, (SPhysiNode*)pTagRefSource));

  *pPhyNode = (SPhysiNode*)pTagRefSource;
  return code;

_return:
  nodesDestroyNode((SNode*)pTagRefSource);
  return code;
}

static int32_t updateDynQueryCtrlStbJoinInfo(SPhysiPlanContext* pCxt, SNodeList* pChildren,
                                             SDynQueryCtrlLogicNode* pLogicNode, SDynQueryCtrlPhysiNode* pDynCtrl) {
  SDataBlockDescNode* pPrevDesc = ((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc;
  SNodeList*          pVgList = NULL;
  SNodeList*          pUidList = NULL;
  int32_t             code = setListSlotId(pCxt, pPrevDesc->dataBlockId, -1, pLogicNode->stbJoin.pVgList, &pVgList);
  if (TSDB_CODE_SUCCESS == code) {
    code = setListSlotId(pCxt, pPrevDesc->dataBlockId, -1, pLogicNode->stbJoin.pUidList, &pUidList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    memcpy(pDynCtrl->stbJoin.srcScan, pLogicNode->stbJoin.srcScan, sizeof(pDynCtrl->stbJoin.srcScan));

    SNode*  pNode = NULL;
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

static int32_t updateDynQueryCtrlVtbScanInfo(SPhysiPlanContext* pCxt, SNodeList* pChildren,
                                             SDynQueryCtrlLogicNode* pLogicNode, SDynQueryCtrlPhysiNode* pDynCtrl,
                                             SSubplan* pSubPlan) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (pLogicNode->vtbScan.pVgroupList) {
    vgroupInfoToNodeAddr(pLogicNode->vtbScan.pVgroupList->vgroups, &pSubPlan->execNode);
    pSubPlan->execNodeStat.tableNum = pLogicNode->vtbScan.pVgroupList->vgroups[0].numOfTable;
  }

  TSWAP(pLogicNode->vtbScan.pOrgVgIds, pDynCtrl->vtbScan.pOrgVgIds);
  if (pLogicNode->vtbScan.pScanCols) {
    PLAN_ERR_JRET(nodesCloneList(pLogicNode->vtbScan.pScanCols, &pDynCtrl->vtbScan.pScanCols));
  } else {
    PLAN_ERR_JRET(nodesCloneList(pLogicNode->node.pTargets, &pDynCtrl->vtbScan.pScanCols));
  }

  pDynCtrl->vtbScan.scanAllCols = pLogicNode->vtbScan.scanAllCols;
  pDynCtrl->vtbScan.suid = pLogicNode->vtbScan.suid;
  pDynCtrl->vtbScan.uid = pLogicNode->vtbScan.uid;
  pDynCtrl->vtbScan.isSuperTable = pLogicNode->vtbScan.isSuperTable;
  pDynCtrl->vtbScan.hasLocalTag = false;
  pDynCtrl->vtbScan.batchProcessChild = pLogicNode->vtbScan.batchProcessChild;
  pDynCtrl->vtbScan.hasPartition = pLogicNode->vtbScan.hasPartition;
  pDynCtrl->vtbScan.rversion = pLogicNode->vtbScan.rversion;
  pDynCtrl->vtbScan.mgmtEpSet = pCxt->pPlanCxt->mgmtEpSet;
  pDynCtrl->vtbScan.accountId = pCxt->pPlanCxt->acctId;
  tstrncpy(pDynCtrl->vtbScan.dbName, pLogicNode->vtbScan.dbName, TSDB_DB_NAME_LEN);
  tstrncpy(pDynCtrl->vtbScan.tbName, pLogicNode->vtbScan.tbName, TSDB_TABLE_NAME_LEN);

  // Propagate tag-ref filter info from VirtualScanLogicNode (logic node's child, still intact)
  // NOTE: pChildren (physi) are Exchange nodes after plan splitting - VirtualTableScan is NOT there.
  // We access the logic tree directly to get pTagFilterCond and pRefTagCols.
  SNode* pLogicChild = NULL;
  FOREACH(pLogicChild, pLogicNode->node.pChildren) {
    if (nodeType(pLogicChild) == QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN) {
      SVirtualScanLogicNode* pVScanLogic = (SVirtualScanLogicNode*)pLogicChild;
      if (pVScanLogic->pTagFilterCond) {
        PLAN_ERR_JRET(nodesCloneNode(pVScanLogic->pTagFilterCond, &pDynCtrl->vtbScan.pTagFilterCond));
      }
      if (pVScanLogic->pRefTagCols) {
        PLAN_ERR_JRET(nodesCloneList(pVScanLogic->pRefTagCols, &pDynCtrl->vtbScan.pRefTagCols));
      }
      pDynCtrl->vtbScan.hasLocalTag = pVScanLogic->hasLocalTag;
      break;
    }
  }
  // Copy tag-ref source scalars from logic node (populated before plan splitting)
  pDynCtrl->vtbScan.tagRefSourceSuid = pLogicNode->vtbScan.tagRefSourceSuid;
  pDynCtrl->vtbScan.tagRefSourceColId = pLogicNode->vtbScan.tagRefSourceColId;
  pDynCtrl->vtbScan.tagRefSourceColType = pLogicNode->vtbScan.tagRefSourceColType;
  tstrncpy(pDynCtrl->vtbScan.tagRefTerminalColName, pLogicNode->vtbScan.tagRefTerminalColName, TSDB_COL_NAME_LEN);

  return code;
_return:
  planError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
  return code;
}

static int32_t rewritePrecalcExpr(SPhysiPlanContext* pCxt, SNode* pNode, SNodeList** pPrecalcExprs,
                                  SNode** pRewritten);
static int32_t rewritePrecalcExprs(SPhysiPlanContext* pCxt, SNodeList* pList, SNodeList** pPrecalcExprs,
                                   SNodeList** pRewrittenList);
static int32_t getChildTuple(SDataBlockDescNode **pChildTuple, SNodeList* pChildren);

static int32_t updateDynQueryCtrlVtbWindowInfo(SPhysiPlanContext* pCxt, SNodeList* pChildren,
                                               SDynQueryCtrlLogicNode* pLogicNode, SDynQueryCtrlPhysiNode* pDynCtrl) {
  int32_t   code = TSDB_CODE_SUCCESS;

  pDynCtrl->vtbWindow.wstartSlotId = pLogicNode->vtbWindow.wstartSlotId;
  pDynCtrl->vtbWindow.wendSlotId = pLogicNode->vtbWindow.wendSlotId;
  pDynCtrl->vtbWindow.wdurationSlotId = pLogicNode->vtbWindow.wdurationSlotId;
  pDynCtrl->vtbWindow.isVstb = pLogicNode->vtbWindow.isVstb;
  pDynCtrl->vtbWindow.extendOption = pLogicNode->vtbWindow.extendOption;

  PLAN_ERR_RET(setMultiBlockSlotId(pCxt, pChildren, false, pLogicNode->node.pTargets, &pDynCtrl->vtbWindow.pTargets));
  PLAN_ERR_RET(addDataBlockSlots(pCxt, pDynCtrl->vtbWindow.pTargets, pDynCtrl->node.pOutputDataBlockDesc));

  return code;
}

static int32_t markVtableFederatedScans(SPhysiPlanContext* pCxt, SPhysiNode* pNode) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (NULL == pNode) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN == nodeType((SNode*)pNode)) {
    SFederatedScanPhysiNode* pScan = (SFederatedScanPhysiNode*)pNode;
    pScan->underVTableScan = true;
    PLAN_ERR_JRET(registerVtableFederatedOutputSlots(pCxt, pScan));
    goto _return;
  }

  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    PLAN_ERR_JRET(markVtableFederatedScans(pCxt, (SPhysiNode*)pChild));
  }

_return:
  if (TSDB_CODE_SUCCESS != code) {
    planError("%s failed since %s", __func__, tstrerror(code));
  }
  return code;
}

static void markVtableWindowFederatedScansFlagOnly(SPhysiNode* pNode) {
  if (NULL == pNode) {
    return;
  }
  if (QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN == nodeType((SNode*)pNode)) {
    ((SFederatedScanPhysiNode*)pNode)->underVTableScan = true;
    return;
  }
  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    markVtableWindowFederatedScansFlagOnly((SPhysiNode*)pChild);
  }
}

static int32_t createDynQueryCtrlPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren,
                                           SDynQueryCtrlLogicNode* pLogicNode, SPhysiNode** pPhyNode, SSubplan* pSubPlan) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  SNode*                  pChild = NULL;
  SDynQueryCtrlPhysiNode* pDynCtrl =
      (SDynQueryCtrlPhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pLogicNode, QUERY_NODE_PHYSICAL_PLAN_DYN_QUERY_CTRL);
  if (NULL == pDynCtrl) {
    PLAN_ERR_JRET(terrno);
  }

  switch (pLogicNode->qType) {
    case DYN_QTYPE_STB_HASH:
      PLAN_ERR_JRET(updateDynQueryCtrlStbJoinInfo(pCxt, pChildren, pLogicNode, pDynCtrl));
      break;
    case DYN_QTYPE_VTB_TS_SCAN:
      PLAN_ERR_JRET(updateDynQueryCtrlVtbScanInfo(pCxt, pChildren, pLogicNode, pDynCtrl, pSubPlan));
      FOREACH(pChild, pChildren) {
        markVtableWindowFederatedScansFlagOnly((SPhysiNode*)pChild);
      }
      break;
    case DYN_QTYPE_VTB_SCAN:
      PLAN_ERR_JRET(updateDynQueryCtrlVtbScanInfo(pCxt, pChildren, pLogicNode, pDynCtrl, pSubPlan));
      break;
    case DYN_QTYPE_VTB_AGG:
      PLAN_ERR_JRET(updateDynQueryCtrlVtbScanInfo(pCxt, pChildren, pLogicNode, pDynCtrl, pSubPlan));
      FOREACH(pChild, pChildren) {
        PLAN_ERR_JRET(markVtableFederatedScans(pCxt, (SPhysiNode*)pChild));
      }
      break;
    case DYN_QTYPE_VTB_INTERVAL:
      PLAN_ERR_JRET(updateDynQueryCtrlVtbScanInfo(pCxt, pChildren, pLogicNode, pDynCtrl, pSubPlan));
      FOREACH(pChild, pChildren) {
        PLAN_ERR_JRET(markVtableFederatedScans(pCxt, (SPhysiNode*)pChild));
      }
      break;
    case DYN_QTYPE_VTB_WINDOW:
      PLAN_ERR_JRET(updateDynQueryCtrlVtbScanInfo(pCxt, pChildren, pLogicNode, pDynCtrl, pSubPlan));
      PLAN_ERR_JRET(updateDynQueryCtrlVtbWindowInfo(pCxt, pChildren, pLogicNode, pDynCtrl));
      FOREACH(pChild, pChildren) {
        PLAN_ERR_JRET(markVtableFederatedScans(pCxt, (SPhysiNode*)pChild));
      }
      break;
    default:
      PLAN_ERR_JRET(TSDB_CODE_PLAN_INVALID_DYN_CTRL_TYPE);
  }

  pDynCtrl->qType = pLogicNode->qType;
  pDynCtrl->dynTbname = pLogicNode->dynTbname;
  *pPhyNode = (SPhysiNode*)pDynCtrl;

  return code;
_return:
  planError("%s failed since %s", __func__, tstrerror(code));
  return code;
}

typedef struct SRewritePrecalcExprsCxt {
  int32_t    errCode;
  int32_t    planNodeId;
  int32_t    rewriteId;
  SNodeList* pPrecalcExprs;
  SHashObj*  pSeenNames;  // tracks aliasNames already used as slot keys
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
  if ('\0' == pRewrittenExpr->aliasName[0]) {
    snprintf(pRewrittenExpr->aliasName, sizeof(pRewrittenExpr->aliasName), "#expr_%d_%d", pCxt->planNodeId,
             pCxt->rewriteId++);
  } else if (pCxt->pSeenNames != NULL) {
    int32_t nameLen = (int32_t)strlen(pRewrittenExpr->aliasName);
    SNode** ppSeenExpr = taosHashGet(pCxt->pSeenNames, pRewrittenExpr->aliasName, nameLen);
    if (NULL != ppSeenExpr && !nodesEqualNode(*ppSeenExpr, pExpr)) {
      snprintf(pRewrittenExpr->aliasName, sizeof(pRewrittenExpr->aliasName), "#expr_%d_%d", pCxt->planNodeId,
               pCxt->rewriteId++);
    }
  }
  int32_t nameLen = (int32_t)strlen(pRewrittenExpr->aliasName);
  if (pCxt->pSeenNames != NULL && nameLen > 0 &&
      NULL == taosHashGet(pCxt->pSeenNames, pRewrittenExpr->aliasName, nameLen)) {
    pCxt->errCode = taosHashPut(pCxt->pSeenNames, pRewrittenExpr->aliasName, nameLen, &pExpr, POINTER_BYTES);
    if (TSDB_CODE_SUCCESS != pCxt->errCode) {
      nodesDestroyNode((SNode*)pCol);
      return DEAL_RES_ERROR;
    }
  }
  tstrncpy(pCol->colName, pRewrittenExpr->aliasName, TSDB_COL_NAME_LEN);
  tstrncpy(pCol->node.userAlias, pRewrittenExpr->userAlias, TSDB_COL_NAME_LEN);
  nodesDestroyNode(*pNode);
  *pNode = (SNode*)pCol;
  return DEAL_RES_IGNORE_CHILD;
}

static int32_t rewriteValueToOperator(SRewritePrecalcExprsCxt* pCxt, SNode** pNode) {
  SOperatorNode* pOper = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pOper);
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
  tstrncpy(pOper->node.aliasName, pVal->node.aliasName, TSDB_COL_NAME_LEN);
  tstrncpy(pOper->node.userAlias, pVal->node.userAlias, TSDB_COL_NAME_LEN);
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
      SFunctionNode* pFunc = (SFunctionNode*)(*pNode);
      if (fmIsScalarFunc(pFunc->funcId) && !fmIsNoPushdownFunc(pFunc->funcId) &&
          !fmIsVolatileFunc(pFunc->funcId)) {
        return collectAndRewrite(pCxt, pNode);
      }
      break;
    }
    case QUERY_NODE_REMOTE_VALUE: {
      return collectAndRewrite(pCxt, pNode);
    }
    case QUERY_NODE_CORRELATED_SUBQUERY: {
      return collectAndRewrite(pCxt, pNode);
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
  SHashObj* pSeenNames =
      taosHashInit(LIST_LENGTH(pList), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  SRewritePrecalcExprsCxt cxt = {.errCode = TSDB_CODE_SUCCESS, .pPrecalcExprs = *pPrecalcExprs,
                                 .pSeenNames = pSeenNames};
  nodesRewriteExprs(*pRewrittenList, doRewritePrecalcExprs, &cxt);
  taosHashCleanup(pSeenNames);
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

static bool isDynVirtualStableScan(SNode* pNode) {
  if (NULL == pNode || QUERY_NODE_PHYSICAL_PLAN_DYN_QUERY_CTRL != nodeType(pNode)) {
    return false;
  }
  SDynQueryCtrlPhysiNode* pDynCtrl = (SDynQueryCtrlPhysiNode*)pNode;
  if (DYN_QTYPE_VTB_SCAN != pDynCtrl->qType) {
    return false;
  }
  if (nodesListGetNode(pDynCtrl->node.pChildren, 0)== NULL) {
    return false;
  }
  if (QUERY_NODE_PHYSICAL_PLAN_VIRTUAL_TABLE_SCAN != nodeType(nodesListGetNode(pDynCtrl->node.pChildren, 0))) {
    return false;
  }
  return true;
}

static bool isDynVirtualStableAgg(SNode* pNode) {
  if (NULL == pNode || QUERY_NODE_PHYSICAL_PLAN_DYN_QUERY_CTRL != nodeType(pNode)) {
    return false;
  }
  SDynQueryCtrlPhysiNode* pDynCtrl = (SDynQueryCtrlPhysiNode*)pNode;
  if (DYN_QTYPE_VTB_AGG != pDynCtrl->qType && DYN_QTYPE_VTB_INTERVAL != pDynCtrl->qType) {
    return false;
  }
  if (nodesListGetNode(pDynCtrl->node.pChildren, 0)== NULL) {
    return false;
  }
  return true;
}

static int32_t getChildTuple(SDataBlockDescNode **pChildTuple, SNodeList* pChildren) {
  if (isDynVirtualStableScan(nodesListGetNode(pChildren, 0))) {
    SPhysiNode* pDynNode = (SPhysiNode*)nodesListGetNode(pChildren, 0);
    if (pDynNode == NULL) {
      planError("get child tuple failed, pDynNode is NULL, childrenNum:%d", pChildren->length);
      return TSDB_CODE_INVALID_PARA;
    }
    SPhysiNode* pVtbScan = (SPhysiNode*)nodesListGetNode(pDynNode->pChildren, 0);
    if (pVtbScan == NULL) {
      planError("get child tuple failed, pVtbScan is NULL, dynNode childrenNum:%d", pDynNode->pChildren->length);
      return TSDB_CODE_INVALID_PARA;
    }
    *pChildTuple = pVtbScan->pOutputDataBlockDesc;
  } else if (isDynVirtualStableAgg(nodesListGetNode(pChildren, 0))) {
    SPhysiNode* pDynNode = (SPhysiNode*)nodesListGetNode(pChildren, 0);
    if (pDynNode == NULL) {
      planError("get child tuple failed, pDynNode is NULL, childrenNum:%d", pChildren->length);
      return TSDB_CODE_INVALID_PARA;
    }
    SPhysiNode* pAgg = (SPhysiNode*)nodesListGetNode(pDynNode->pChildren, LIST_LENGTH(pDynNode->pChildren) - 1);
    if (pAgg == NULL) {
      planError("get child tuple failed, pVtbScan is NULL, dynNode childrenNum:%d", pDynNode->pChildren->length);
      return TSDB_CODE_INVALID_PARA;
    }
    *pChildTuple = pAgg->pOutputDataBlockDesc;
  } else {
    SPhysiNode* pChild = (SPhysiNode*)nodesListGetNode(pChildren, 0);
    if (pChild == NULL) {
      planError("get child tuple failed, pChild is NULL, childrenNum:%d", pChildren->length);
      return TSDB_CODE_INVALID_PARA;
    }
    *pChildTuple = (((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc);
  }
  return TSDB_CODE_SUCCESS;
}

static bool forceNoMergeDataBlock(const SPhysiPlanContext* pCxt) {
  return (pCxt != NULL && pCxt->pPlanCxt != NULL && pCxt->pPlanCxt->forceNoMergeDataBlock);
}

static int32_t createAggPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SAggLogicNode* pAggLogicNode,
                                  SPhysiNode** pPhyNode, SSubplan* pSubPlan) {
  if (fqDiagEnabled()) {
    fprintf(stderr,
            "[FQ-DIAG] createAggPhysiNode logic=%p targets=%d groupKeys=%d aggFuncs=%d cond=%p childCnt=%d\n",
            pAggLogicNode, pAggLogicNode->node.pTargets ? LIST_LENGTH(pAggLogicNode->node.pTargets) : 0,
            pAggLogicNode->pGroupKeys ? LIST_LENGTH(pAggLogicNode->pGroupKeys) : 0,
            pAggLogicNode->pAggFuncs ? LIST_LENGTH(pAggLogicNode->pAggFuncs) : 0,
            pAggLogicNode->node.pConditions, pChildren ? LIST_LENGTH(pChildren) : 0);
    fqDiagDumpExprList("agg.logic.targets", pAggLogicNode->node.pTargets);
    fqDiagDumpExprList("agg.logic.groupKeys", pAggLogicNode->pGroupKeys);
    fqDiagDumpExprList("agg.logic.aggFuncs", pAggLogicNode->pAggFuncs);
  }
  SAggPhysiNode* pAgg =
      (SAggPhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pAggLogicNode, QUERY_NODE_PHYSICAL_PLAN_HASH_AGG);
  if (NULL == pAgg) {
    return terrno;
  }
  if (pAgg->node.pSlimit && ((SLimitNode*)pAgg->node.pSlimit)->limit) {
    pSubPlan->dynamicRowThreshold = true;
    pSubPlan->rowsThreshold = ((SLimitNode*)pAgg->node.pSlimit)->limit->datum.i;
  }

  pAgg->mergeDataBlock =
      (forceNoMergeDataBlock(pCxt) || GROUP_ACTION_KEEP == pAggLogicNode->node.groupAction ? false : true);
  pAgg->groupKeyOptimized = pAggLogicNode->hasGroupKeyOptimized;
  pAgg->hasMixedDistinct = pAggLogicNode->hasMixedDistinct;
  pAgg->numDistinctFuncs = pAggLogicNode->numDistinctFuncs;
  pAgg->node.forceCreateNonBlockingOptr = pAggLogicNode->node.forceCreateNonBlockingOptr;

  SNodeList* pPrecalcExprs = NULL;
  SNodeList* pGroupKeys = NULL;
  SNodeList* pAggFuncs = NULL;
  int32_t    code = rewritePrecalcExprs(pCxt, pAggLogicNode->pGroupKeys, &pPrecalcExprs, &pGroupKeys);
  if (TSDB_CODE_SUCCESS == code) {
    code = rewritePrecalcExprs(pCxt, pAggLogicNode->pAggFuncs, &pPrecalcExprs, &pAggFuncs);
  }
  nodesWalkExprs(pAggFuncs, hasCountLikeFunc, &pAgg->hasCountLikeFunc);

  SDataBlockDescNode* pChildTupe = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = getChildTuple(&pChildTupe, pChildren);
  }

  // push down expression to pOutputDataBlockDesc of child node
  if (TSDB_CODE_SUCCESS == code && NULL != pPrecalcExprs) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pPrecalcExprs, &pAgg->pExprs);
    if (TSDB_CODE_SUCCESS == code) {
      code = pushdownDataBlockSlots(pCxt, pAgg->pExprs, pChildTupe);
    }
    // For mixed distinct: also push precalc exprs to child[1]'s descriptor
    if (TSDB_CODE_SUCCESS == code && pAggLogicNode->hasMixedDistinct && pChildren->length >= 2) {
      SPhysiNode* pChild1 = (SPhysiNode*)nodesListGetNode(pChildren, 1);
      if (pChild1 != NULL) {
        code = pushdownDataBlockSlots(pCxt, pAgg->pExprs, pChild1->pOutputDataBlockDesc);
      }
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

  SDataBlockDescNode* pChildTupe = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = getChildTuple(&pChildTupe, pChildren);
  }

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
  SInterpFuncPhysiNode* pInterpFunc = (SInterpFuncPhysiNode*)makePhysiNode(
      pCxt, (SLogicNode*)pFuncLogicNode, QUERY_NODE_PHYSICAL_PLAN_INTERP_FUNC);
  if (NULL == pInterpFunc) {
    return terrno;
  }

  pInterpFunc->mergeDataBlock = !forceNoMergeDataBlock(pCxt) &&
      GROUP_ACTION_KEEP != pFuncLogicNode->node.groupAction;

  SNodeList* pPrecalcExprs = NULL;
  SNodeList* pFuncs = NULL;
  int32_t    code = rewritePrecalcExprs(pCxt, pFuncLogicNode->pFuncs, &pPrecalcExprs, &pFuncs);

  SDataBlockDescNode* pChildTupe = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = getChildTuple(&pChildTupe, pChildren);
  }

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
    pInterpFunc->timelineSource = pFuncLogicNode->timelineSource;
    pInterpFunc->interpMode = pFuncLogicNode->interpMode;
    pInterpFunc->requireMainMembership =
        pFuncLogicNode->requireMainMembership;
    pInterpFunc->intervalUnit = pFuncLogicNode->intervalUnit;
    pInterpFunc->precision = pFuncLogicNode->node.precision;
    pInterpFunc->timezone = pFuncLogicNode->timezone;
    tstrncpy(pInterpFunc->timezoneName, pFuncLogicNode->timezoneName,
             sizeof(pInterpFunc->timezoneName));
    pInterpFunc->pFillValues = NULL;
    pInterpFunc->surroundingTime = pFuncLogicNode->surroundingTime;
    code = nodesCloneNode(pFuncLogicNode->pTimeRange, &pInterpFunc->pTimeRange);
  }
  
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesCloneNode(pFuncLogicNode->pFillValues, &pInterpFunc->pFillValues);
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

static int32_t createForecastFuncPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren,
                                           SForecastFuncLogicNode* pFuncLogicNode, SPhysiNode** pPhyNode) {
  SForecastFuncPhysiNode* pForecastFunc =
      (SForecastFuncPhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pFuncLogicNode, QUERY_NODE_PHYSICAL_PLAN_FORECAST_FUNC);
  if (NULL == pForecastFunc) {
    return terrno;
  }

  SNodeList* pPrecalcExprs = NULL;
  SNodeList* pFuncs = NULL;
  int32_t    code = rewritePrecalcExprs(pCxt, pFuncLogicNode->pFuncs, &pPrecalcExprs, &pFuncs);

  SDataBlockDescNode* pChildTupe = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = getChildTuple(&pChildTupe, pChildren);
  }

  // push down expression to pOutputDataBlockDesc of child node
  if (TSDB_CODE_SUCCESS == code && NULL != pPrecalcExprs) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pPrecalcExprs, &pForecastFunc->pExprs);
    if (TSDB_CODE_SUCCESS == code) {
      code = pushdownDataBlockSlots(pCxt, pForecastFunc->pExprs, pChildTupe);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pFuncs, &pForecastFunc->pFuncs);
    if (TSDB_CODE_SUCCESS == code) {
      code = addDataBlockSlots(pCxt, pForecastFunc->pFuncs, pForecastFunc->node.pOutputDataBlockDesc);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setConditionsSlotId(pCxt, (const SLogicNode*)pFuncLogicNode, (SPhysiNode*)pForecastFunc);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pForecastFunc;
  } else {
    nodesDestroyNode((SNode*)pForecastFunc);
  }

  nodesDestroyList(pPrecalcExprs);
  nodesDestroyList(pFuncs);

  return code;
}

static int32_t createWindowFuncPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren,
                                         SWindowFuncLogicNode* pWindowLogicNode, SPhysiNode** pPhyNode) {
  SWindowFuncPhysiNode* pWindow =
      (SWindowFuncPhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pWindowLogicNode, QUERY_NODE_PHYSICAL_PLAN_WINDOW_FUNC);
  if (NULL == pWindow) {
    return terrno;
  }

  SNodeList* pPrecalcExprs = NULL;
  SNodeList* pFuncs = NULL;
  SNodeList* pOrderKeys = NULL;
  int32_t    code = rewritePrecalcExprs(pCxt, pWindowLogicNode->pFuncs, &pPrecalcExprs, &pFuncs);

  if (TSDB_CODE_SUCCESS == code) {
    code = rewritePrecalcExprs(pCxt, pWindowLogicNode->pOrderKeys, &pPrecalcExprs, &pOrderKeys);
  }

  SDataBlockDescNode* pChildTuple = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = getChildTuple(&pChildTuple, pChildren);
  }
  if (TSDB_CODE_SUCCESS == code && NULL != pPrecalcExprs) {
    code = setListSlotId(pCxt, pChildTuple->dataBlockId, -1, pPrecalcExprs, &pWindow->pExprs);
    if (TSDB_CODE_SUCCESS == code) {
      code = pushdownDataBlockSlots(pCxt, pWindow->pExprs, pChildTuple);
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    code =
        setListSlotId(pCxt, pChildTuple->dataBlockId, -1, pWindowLogicNode->pPartitionKeys, &pWindow->pPartitionKeys);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setListSlotId(pCxt, pChildTuple->dataBlockId, -1, pOrderKeys, &pWindow->pOrderKeys);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setListSlotId(pCxt, pChildTuple->dataBlockId, -1, pFuncs, &pWindow->pFuncs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesCloneNode(pWindowLogicNode->pFrame, &pWindow->pFrame);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = addDataBlockSlots(pCxt, pWindow->pFuncs, pWindow->node.pOutputDataBlockDesc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setConditionsSlotId(pCxt, (const SLogicNode*)pWindowLogicNode, (SPhysiNode*)pWindow);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pWindow;
  } else {
    nodesDestroyNode((SNode*)pWindow);
  }

  nodesDestroyList(pPrecalcExprs);
  nodesDestroyList(pFuncs);
  nodesDestroyList(pOrderKeys);

  return code;
}

static int32_t createGenericAnalysisPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren,
                                              SGenericAnalysisLogicNode* pFuncLogicNode, SPhysiNode** pPhyNode) {
  SGenericAnalysisPhysiNode* pFunc = (SGenericAnalysisPhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pFuncLogicNode,
                                                                               QUERY_NODE_PHYSICAL_PLAN_ANALYSIS_FUNC);
  if (NULL == pFunc) {
    return terrno;
  }

  SNodeList* pPrecalcExprs = NULL;
  SNodeList* pFuncs = NULL;
  int32_t    code = rewritePrecalcExprs(pCxt, pFuncLogicNode->pFuncs, &pPrecalcExprs, &pFuncs);

  SDataBlockDescNode* pChildTupe = (((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc);
  // push down expression to pOutputDataBlockDesc of child node
  if (TSDB_CODE_SUCCESS == code && NULL != pPrecalcExprs) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pPrecalcExprs, &pFunc->pExprs);
    if (TSDB_CODE_SUCCESS == code) {
      code = pushdownDataBlockSlots(pCxt, pFunc->pExprs, pChildTupe);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pFuncs, &pFunc->pFuncs);
    if (TSDB_CODE_SUCCESS == code) {
      code = addDataBlockSlots(pCxt, pFunc->pFuncs, pFunc->node.pOutputDataBlockDesc);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setConditionsSlotId(pCxt, (const SLogicNode*)pFuncLogicNode, (SPhysiNode*)pFunc);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pFunc;
  } else {
    nodesDestroyNode((SNode*)pFunc);
  }

  nodesDestroyList(pPrecalcExprs);
  nodesDestroyList(pFuncs);

  return code;
}

static int32_t createRowsetSourcePhysiNode(SPhysiPlanContext* pCxt, SRowsetSourceLogicNode* pLogicNode,
                                           SPhysiNode** pPhyNode) {
  SRowsetSourcePhysiNode* pRowset =
      (SRowsetSourcePhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pLogicNode, QUERY_NODE_PHYSICAL_PLAN_ROWSET_SOURCE);
  if (NULL == pRowset) {
    return terrno;
  }

  pRowset->colCount      = pLogicNode->colCount;
  pRowset->numBlocks     = pLogicNode->numBlocks;
  pRowset->totalRows     = pLogicNode->totalRows;
  pRowset->hasPrimaryTs  = pLogicNode->hasPrimaryTs;
  pRowset->isSortedByTs  = pLogicNode->isSortedByTs;
  pRowset->primaryTsSlot = pLogicNode->primaryTsSlot;
  pRowset->blockBufLen   = pLogicNode->blockBufLen;
  pRowset->pBlockBuf     = pLogicNode->pBlockBuf;
  pLogicNode->pBlockBuf  = NULL;  // ownership transferred

  int32_t code = setConditionsSlotId(pCxt, (const SLogicNode*)pLogicNode, (SPhysiNode*)pRowset);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pRowset);
    return code;
  }

  *pPhyNode = (SPhysiNode*)pRowset;
  return TSDB_CODE_SUCCESS;
}

static bool projectCanMergeDataBlock(SProjectLogicNode* pProject, bool topLevelSubplan) {
  // Split/scale-out may reset logic-parent pointers at subplan boundaries.
  // Allow merge only when Project is the root output of the top-level subplan.
  if (!topLevelSubplan || pProject->node.pParent != NULL) {
    return false;
  }

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

bool projectCouldMergeUnsortDataBlock(SProjectLogicNode* pProject) {
  SLogicNode* pChild = (SLogicNode*)nodesListGetNode(pProject->node.pChildren, 0);
  if (DATA_ORDER_LEVEL_GLOBAL == pChild->resultDataOrder) {
    return false;
  }
  if (GROUP_ACTION_KEEP == pProject->node.groupAction) {
    return false;
  }
  if (DATA_ORDER_LEVEL_NONE == pProject->node.resultDataOrder) {
    return true;
  }
  if (1 != LIST_LENGTH(pProject->node.pChildren)) {
    return true;
  }
  return false;
}

static int32_t createProjectPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren,
                                      SProjectLogicNode* pProjectLogicNode, SSubplan* pSubplan,
                                      SPhysiNode** pPhyNode) {
  SProjectPhysiNode* pProject =
      (SProjectPhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pProjectLogicNode, QUERY_NODE_PHYSICAL_PLAN_PROJECT);
  if (NULL == pProject) {
    return terrno;
  }

  pProject->mergeDataBlock =
      (forceNoMergeDataBlock(pCxt) ? false : projectCanMergeDataBlock(pProjectLogicNode, (0 == pSubplan->level)));
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
  pExchange->dynTbname = pExchangeLogicNode->dynTbname;
  pExchange->fastInterval = pExchangeLogicNode->fastInterval;
  pExchange->localBlockExchange = pExchangeLogicNode->localBlockExchange;

  int32_t code = setConditionsSlotId(pCxt, (const SLogicNode*)pExchangeLogicNode, (SPhysiNode*)pExchange);
  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pExchange;
  } else {
    nodesDestroyNode((SNode*)pExchange);
  }

  return code;
}

static int32_t createExchangePhysiNode(SPhysiPlanContext* pCxt, SExchangeLogicNode* pExchangeLogicNode,
                                       SPhysiNode** pPhyNode) {
  return doCreateExchangePhysiNode(pCxt, pExchangeLogicNode, pPhyNode);
}

static int32_t createWindowPhysiNodeFinalize(SPhysiPlanContext* pCxt, SNodeList* pChildren, SWindowPhysiNode* pWindow,
                                             SWindowLogicNode* pWindowLogicNode) {
  pWindow->indefRowsFunc = pWindowLogicNode->indefRowsFunc;
  pWindow->mergeDataBlock =
      (forceNoMergeDataBlock(pCxt) || GROUP_ACTION_KEEP == pWindowLogicNode->node.groupAction ? false : true);
  pWindow->node.inputTsOrder = pWindowLogicNode->node.inputTsOrder;
  pWindow->node.outputTsOrder = pWindowLogicNode->node.outputTsOrder;
  if (nodeType(pWindow) == QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL) {
    pWindow->node.inputTsOrder = pWindowLogicNode->node.outputTsOrder;
  }

  SNodeList* pPrecalcExprs = NULL;
  SNodeList* pFuncs = NULL;
  SNodeList* pProjs = pWindowLogicNode->pProjs;
  int32_t    code = rewritePrecalcExprs(pCxt, pWindowLogicNode->pFuncs, &pPrecalcExprs, &pFuncs);

  SDataBlockDescNode* pChildTupe = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = getChildTuple(&pChildTupe, pChildren);
  }
  // push down expression to pOutputDataBlockDesc of child node
  if (TSDB_CODE_SUCCESS == code && NULL != pPrecalcExprs) {
    SNodeList* pOutput;
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pPrecalcExprs, &pOutput);
    if (TSDB_CODE_SUCCESS == code) {
      code = addDataBlockSlots(pCxt, pOutput, pChildTupe);
    }
    if (TSDB_CODE_SUCCESS == code) {
      if (pWindow->pExprs == NULL) {
        pWindow->pExprs = pOutput;
      } else {
        code = nodesListAppendList(pWindow->pExprs, pOutput);
      }
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

  if (TSDB_CODE_SUCCESS == code && NULL != pProjs) {
    // For the agg + fill case, pProjs contains expressions like sum(v)_col_ref + 1
    // where sum(v)_col_ref was substituted by the logic-layer rewrite.  The agg result
    // slot (sum(v)) lives in the ExtWin output block, which was populated by
    // addDataBlockSlots(pFuncs) above.  Use that block as the primary lookup target
    // so the col_ref resolves to the correct slot.  Fall back to the child block for
    // any other column refs (e.g. _wstart pseudo-column on the child side).
    int64_t projPrimaryBlockId = (NULL != pFuncs)
                                     ? pWindow->node.pOutputDataBlockDesc->dataBlockId
                                     : pChildTupe->dataBlockId;
    int64_t projSecondaryBlockId = (NULL != pFuncs) ? pChildTupe->dataBlockId : -1;
    code = setListSlotId(pCxt, projPrimaryBlockId, projSecondaryBlockId, pProjs, &pWindow->pProjs);
    if (TSDB_CODE_SUCCESS == code) {
      code = addDataBlockSlots(pCxt, pWindow->pProjs, pWindow->node.pOutputDataBlockDesc);
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
    default:
      break;
  }
  return QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL;
}

static ENodeType getExternalOperatorType(EWindowAlgorithm windowAlgo) {
  switch (windowAlgo) {
    case EXTERNAL_ALGO_HASH:
      return QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL;
    case EXTERNAL_ALGO_MERGE:
      return QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL;
    default:
      break;
  }
  return QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL;
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
  pInterval->timeRange = pWindowLogicNode->timeRange;
  pInterval->firstDayOfWeek = pWindowLogicNode->firstDayOfWeek;
  pInterval->fastInterval = pWindowLogicNode->fastInterval;
  pInterval->exInterval = pWindowLogicNode->exInterval;
  pInterval->timezone = pWindowLogicNode->timezone;
  tstrncpy(pInterval->timezoneName, pWindowLogicNode->timezoneName, sizeof(pInterval->timezoneName));
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
      pCxt, (SLogicNode*)pWindowLogicNode, QUERY_NODE_PHYSICAL_PLAN_MERGE_SESSION);
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
  ENodeType type = QUERY_NODE_PHYSICAL_PLAN_MERGE_STATE;
  SStateWindowPhysiNode* pState = (SStateWindowPhysiNode*)makePhysiNode(
      pCxt, (SLogicNode*)pWindowLogicNode, type);
  if (NULL == pState) {
    return terrno;
  }

  SNodeList* pPrecalcExprs = NULL;
  SNodeList* pStateKeys = NULL;
  int32_t    code = rewritePrecalcExprs(pCxt, pWindowLogicNode->pStateExprs, &pPrecalcExprs, &pStateKeys);

  SDataBlockDescNode* pChildTupe = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = getChildTuple(&pChildTupe, pChildren);
  }
  // push down expression to pOutputDataBlockDesc of child node
  if (TSDB_CODE_SUCCESS == code && NULL != pPrecalcExprs) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pPrecalcExprs, &pState->window.pExprs);
    if (TSDB_CODE_SUCCESS == code) {
      code = addDataBlockSlots(pCxt, pState->window.pExprs, pChildTupe);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pStateKeys, &pState->pStateKeys);
  }

  pState->extendOption = pWindowLogicNode->extendOption;
  pState->trueForType = pWindowLogicNode->trueForType;
  pState->trueForCount = pWindowLogicNode->trueForCount;
  pState->trueForDuration = pWindowLogicNode->trueForDuration;

  if (TSDB_CODE_SUCCESS == code) {
    code = createWindowPhysiNodeFinalize(pCxt, pChildren, &pState->window, pWindowLogicNode);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pState;
  } else {
    nodesDestroyNode((SNode*)pState);
  }

  nodesDestroyList(pPrecalcExprs);
  nodesDestroyList(pStateKeys);

  return code;
}

static int32_t createEventWindowPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren,
                                          SWindowLogicNode* pWindowLogicNode, SPhysiNode** pPhyNode) {
  ENodeType type = QUERY_NODE_PHYSICAL_PLAN_MERGE_EVENT;

  SEventWinodwPhysiNode* pEvent = (SEventWinodwPhysiNode*)makePhysiNode(
      pCxt, (SLogicNode*)pWindowLogicNode, type);
  if (NULL == pEvent) {
    return terrno;
  }
  int32_t             code = TSDB_CODE_SUCCESS;
  SDataBlockDescNode* pChildTupe = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = getChildTuple(&pChildTupe, pChildren);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setNodeSlotId(pCxt, pChildTupe->dataBlockId, -1, pWindowLogicNode->pStartCond, &pEvent->pStartCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setNodeSlotId(pCxt, pChildTupe->dataBlockId, -1, pWindowLogicNode->pEndCond, &pEvent->pEndCond);
  }
  pEvent->trueForType = pWindowLogicNode->trueForType;
  pEvent->trueForCount = pWindowLogicNode->trueForCount;
  pEvent->trueForDuration = pWindowLogicNode->trueForDuration;
  pEvent->startTrueForType     = pWindowLogicNode->startTrueForType;
  pEvent->startTrueForCount    = pWindowLogicNode->startTrueForCount;
  pEvent->startTrueForDuration = pWindowLogicNode->startTrueForDuration;
  pEvent->endTrueForType       = pWindowLogicNode->endTrueForType;
  pEvent->endTrueForCount      = pWindowLogicNode->endTrueForCount;
  pEvent->endTrueForDuration   = pWindowLogicNode->endTrueForDuration;
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
  ENodeType type = QUERY_NODE_PHYSICAL_PLAN_MERGE_COUNT;

  SCountWindowPhysiNode* pCount = (SCountWindowPhysiNode*)makePhysiNode(
      pCxt, (SLogicNode*)pWindowLogicNode, type);
  if (NULL == pCount) {
    return terrno;
  }
  pCount->windowCount = pWindowLogicNode->windowCount;
  pCount->windowSliding = pWindowLogicNode->windowSliding;

  int32_t code = createWindowPhysiNodeFinalize(pCxt, pChildren, &pCount->window, pWindowLogicNode);
  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pCount;
  } else {
    nodesDestroyNode((SNode*)pCount);
  }

  return code;
}

static int32_t createAnomalyWindowPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren,
                                            SWindowLogicNode* pWindowLogicNode, SPhysiNode** pPhyNode) {
  SAnomalyWindowPhysiNode* pAnomaly = (SAnomalyWindowPhysiNode*)makePhysiNode(pCxt, (SLogicNode*)pWindowLogicNode,
                                                                              QUERY_NODE_PHYSICAL_PLAN_MERGE_ANOMALY);
  if (NULL == pAnomaly) {
    return terrno;
  }

  SNodeList*          pPrecalcExprs = NULL;
  SNodeList*          pAnomalyKeys = NULL;
  SDataBlockDescNode* pChildTupe = NULL;

  int32_t code = rewritePrecalcExprs(pCxt, pWindowLogicNode->pAnomalyExpr, &pPrecalcExprs, &pAnomalyKeys);
  if (TSDB_CODE_SUCCESS == code) {
    code = getChildTuple(&pChildTupe, pChildren);
  }

  // push down expression to pOutputDataBlockDesc of child node
  if (TSDB_CODE_SUCCESS == code && NULL != pPrecalcExprs) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pPrecalcExprs, &pAnomaly->window.pExprs);
    if (TSDB_CODE_SUCCESS == code) {
      code = addDataBlockSlots(pCxt, pAnomaly->window.pExprs, pChildTupe);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pAnomalyKeys, &pAnomaly->pAnomalyKeys);
    if (TSDB_CODE_SUCCESS == code) {
      // code = addDataBlockSlots(pCxt, pAnomaly->pAnomalyKeys, pAnomaly->window.node.pOutputDataBlockDesc);
    }
  }

  tstrncpy(pAnomaly->anomalyOpt, pWindowLogicNode->anomalyOpt, sizeof(pAnomaly->anomalyOpt));

  if (TSDB_CODE_SUCCESS == code) {
    code = createWindowPhysiNodeFinalize(pCxt, pChildren, &pAnomaly->window, pWindowLogicNode);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pAnomaly;
  } else {
    nodesDestroyNode((SNode*)pAnomaly);
  }

  nodesDestroyList(pPrecalcExprs);
  nodesDestroyList(pAnomalyKeys);

  return code;
}

static const char* getExternalWindowExprOutputName(SNode* pNode) {
  if (pNode == NULL) {
    return NULL;
  }

  if (QUERY_NODE_TARGET == nodeType(pNode)) {
    pNode = ((STargetNode*)pNode)->pExpr;
    if (pNode == NULL) {
      return NULL;
    }
  }

  if ('\0' != ((SExprNode*)pNode)->aliasName[0]) {
    return ((SExprNode*)pNode)->aliasName;
  }

  if (QUERY_NODE_COLUMN == nodeType(pNode) && '\0' != ((SColumnNode*)pNode)->colName[0]) {
    return ((SColumnNode*)pNode)->colName;
  }

  return NULL;
}

static int32_t createExternalWindowOutputColumn(const SExternalWindowPhysiNode* pExternal, int32_t targetIdx,
                                                SNode** pOutput) {
  if (pExternal == NULL || pOutput == NULL || pExternal->window.node.pOutputDataBlockDesc == NULL) {
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  SSlotDescNode* pSlotDesc =
      (SSlotDescNode*)nodesListGetNode(pExternal->window.node.pOutputDataBlockDesc->pSlots, targetIdx);
  if (pSlotDesc == NULL) {
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  SColumnNode* pCol = NULL;
  int32_t      code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  if (NULL == pCol) {
    return code;
  }

  pCol->dataBlockId = pExternal->window.node.pOutputDataBlockDesc->dataBlockId;
  pCol->slotId = pSlotDesc->slotId;
  pCol->colType = COLUMN_TYPE_COLUMN;
  pCol->node.resType = pSlotDesc->dataType;
  tstrncpy(pCol->colName, pSlotDesc->name, TSDB_COL_NAME_LEN);
  tstrncpy(pCol->node.aliasName, pSlotDesc->name, TSDB_COL_NAME_LEN);
  tstrncpy(pCol->node.userAlias, pSlotDesc->name, TSDB_COL_NAME_LEN);

  *pOutput = (SNode*)pCol;
  return TSDB_CODE_SUCCESS;
}

// Build a fill output column using the TARGET node from pProjs (after addDataBlockSlots has run).
// The TARGET node's slotId is the correct OPERATOR slot, not the phantom slot from makePhysiNode.
static int32_t createExternalWindowOutputColumnByProjIdx(const SExternalWindowPhysiNode* pExternal,
                                                         int32_t projIdx, SNode** pOutput) {
  if (pExternal == NULL || pOutput == NULL || pExternal->window.node.pOutputDataBlockDesc == NULL) {
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  STargetNode* pTargetNode =
      (STargetNode*)nodesListGetNode(pExternal->window.pProjs, projIdx);
  if (pTargetNode == NULL || QUERY_NODE_TARGET != nodeType(pTargetNode)) {
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  // Find the SlotDescNode that matches this TARGET's slotId.
  SSlotDescNode* pSlotDesc = NULL;
  SNode*         pSlotNode = NULL;
  FOREACH(pSlotNode, pExternal->window.node.pOutputDataBlockDesc->pSlots) {
    SSlotDescNode* pSlot = (SSlotDescNode*)pSlotNode;
    if (pSlot->slotId == pTargetNode->slotId) {
      pSlotDesc = pSlot;
      break;
    }
  }
  if (pSlotDesc == NULL) {
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  SColumnNode* pCol = NULL;
  int32_t      code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  if (NULL == pCol) {
    return code;
  }

  pCol->dataBlockId = pExternal->window.node.pOutputDataBlockDesc->dataBlockId;
  pCol->slotId = pSlotDesc->slotId;
  pCol->colType = COLUMN_TYPE_COLUMN;
  pCol->node.resType = pSlotDesc->dataType;
  tstrncpy(pCol->colName, pSlotDesc->name, TSDB_COL_NAME_LEN);
  tstrncpy(pCol->node.aliasName, pSlotDesc->name, TSDB_COL_NAME_LEN);
  tstrncpy(pCol->node.userAlias, pSlotDesc->name, TSDB_COL_NAME_LEN);

  *pOutput = (SNode*)pCol;
  return TSDB_CODE_SUCCESS;
}

static int32_t getExternalWindowOutputIndexByExpr(const SWindowLogicNode* pWindowLogicNode, SNode* pFillExpr) {
  if (LIST_LENGTH(pWindowLogicNode->node.pTargets) == LIST_LENGTH(pWindowLogicNode->pProjs)) {
    int32_t targetIdx = 0;
    SNode*  pTarget = NULL;

    FOREACH(pTarget, pWindowLogicNode->pProjs) {
      if (nodesEqualNode(pFillExpr, pTarget)) {
        return targetIdx;
      }
      ++targetIdx;
    }

    return -1;
  }

  int32_t targetIdx = 0;
  SNode*  pTarget = NULL;

  FOREACH(pTarget, pWindowLogicNode->pFuncs) {
    if (nodesEqualNode(pFillExpr, pTarget)) {
      return targetIdx;
    }
    ++targetIdx;
  }

  FOREACH(pTarget, pWindowLogicNode->pProjs) {
    if (nodesEqualNode(pFillExpr, pTarget)) {
      return targetIdx;
    }
    ++targetIdx;
  }

  return -1;
}

static int32_t createExternalWindowPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren,
                                             SWindowLogicNode* pWindowLogicNode, SPhysiNode** pPhyNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  SExternalWindowPhysiNode* pExternal = (SExternalWindowPhysiNode*)makePhysiNode(
      pCxt, (SLogicNode*)pWindowLogicNode, getExternalOperatorType(pWindowLogicNode->windowAlgo));
  if (NULL == pExternal) {
    return terrno;
  }
  pExternal->timeRange = pWindowLogicNode->timeRange;
  pExternal->isSingleTable = pWindowLogicNode->isSingleTable;
  pExternal->inputHasOrder = pWindowLogicNode->inputHasOrder;
  pExternal->needGroupSort = pWindowLogicNode->needGroupSort;
  pExternal->extWinSplit = pWindowLogicNode->extWinSplit;
  pExternal->calcWithPartition = pWindowLogicNode->calcWithPartition;
  pExternal->matchPartitionBy = pWindowLogicNode->pSubquery != NULL &&
                                nodeType(pWindowLogicNode->pSubquery) == QUERY_NODE_REMOTE_TABLE &&
                                ((((SRemoteTableNode*)pWindowLogicNode->pSubquery)->flag &
                                  REMOTE_TABLE_FLAG_EXT_PART_KEYS_MATCH) != 0);
  pExternal->extWinInputMode = pWindowLogicNode->extWinInputMode;
  pExternal->extFill.mode = pWindowLogicNode->extFill.mode;
  pExternal->orgTableUid = pWindowLogicNode->orgTableUid;
  pExternal->orgTableVgId = pWindowLogicNode->orgTableVgId;
  PLAN_ERR_JRET(nodesCloneNode(pWindowLogicNode->pSubquery, &pExternal->pSubquery));
  PLAN_ERR_JRET(nodesCloneNode(pWindowLogicNode->pTimeRange, &pExternal->pTimeRange));
  PLAN_ERR_JRET(nodesCloneNode(pWindowLogicNode->pWindowConditions, &pExternal->pWindowConditions));

  PLAN_ERR_JRET(createWindowPhysiNodeFinalize(pCxt, pChildren, &pExternal->window, pWindowLogicNode));
  if (pWindowLogicNode->extFill.pFillExprs != NULL) {
    // Whether targets.len == projs.len (fast path in getExternalWindowOutputIndexByExpr).
    // In this case targetIdx is a pProjs-relative index and we must read the slotId from
    // pExternal->window.pProjs[targetIdx] (TARGET node) because addDataBlockSlots may have
    // created a different slot key for the OPERATOR expression than buildDataBlockSlots
    // created for the corresponding pTargets column.
    bool useProjIdx = (LIST_LENGTH(pWindowLogicNode->node.pTargets) == LIST_LENGTH(pWindowLogicNode->pProjs));

    SNode* pFillExpr = NULL;
    FOREACH(pFillExpr, pWindowLogicNode->extFill.pFillExprs) {
      int32_t targetIdx = getExternalWindowOutputIndexByExpr(pWindowLogicNode, pFillExpr);
      bool    fromProj = useProjIdx && (targetIdx >= 0);

      if (targetIdx < 0) {
        const char* pOutputName = getExternalWindowExprOutputName(pFillExpr);
        if (pOutputName == NULL) {
          PLAN_ERR_JRET(TSDB_CODE_PLAN_INTERNAL_ERROR);
        }

        SNode* pTarget = NULL;
        bool   found = false;
        targetIdx = 0;
        FOREACH(pTarget, pWindowLogicNode->node.pTargets) {
          if (QUERY_NODE_COLUMN == nodeType(pTarget) &&
              (0 == strcmp(((SColumnNode*)pTarget)->colName, pOutputName) ||
               0 == strcmp(((SExprNode*)pTarget)->aliasName, pOutputName))) {
            found = true;
            break;
          }
          ++targetIdx;
        }

        if (!found) {
          PLAN_ERR_JRET(TSDB_CODE_PLAN_INTERNAL_ERROR);
        }
      }

      SNode* pClone = NULL;
      if (fromProj) {
        // Use the TARGET node from pProjs to get the correct OPERATOR slot id.
        PLAN_ERR_JRET(createExternalWindowOutputColumnByProjIdx(pExternal, targetIdx, &pClone));
      } else {
        PLAN_ERR_JRET(createExternalWindowOutputColumn(pExternal, targetIdx, &pClone));
      }
      PLAN_ERR_JRET(nodesListMakeStrictAppend(&pExternal->extFill.pFillExprs, pClone));
    }
  }
  if (pWindowLogicNode->extFill.pFillValues != NULL) {
    PLAN_ERR_JRET(nodesCloneNode(pWindowLogicNode->extFill.pFillValues, &pExternal->extFill.pFillValues));
  }
  *pPhyNode = (SPhysiNode*)pExternal;

  return code;
_return:
  nodesDestroyNode((SNode*)pExternal);
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
    case WINDOW_TYPE_ANOMALY:
      return createAnomalyWindowPhysiNode(pCxt, pChildren, pWindowLogicNode, pPhyNode);
    case WINDOW_TYPE_EXTERNAL:
      return createExternalWindowPhysiNode(pCxt, pChildren, pWindowLogicNode, pPhyNode);
    default:
      return generateUsageErrMsg(pCxt->pPlanCxt->pMsg, pCxt->pPlanCxt->msgLen, TSDB_CODE_PLAN_INVALID_WINDOW_TYPE, " invalid window type");
  }
  return TSDB_CODE_FAILED;
}

// createDistinctFilterPhysiNode() was removed with the DistinctAggFilter rule. New logical plans
// no longer contain this node; the physical node and its serialization remain only for already
// materialized plans received from older nodes during rolling upgrades.

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
  pSort->numOfGroupKeys = pSortLogicNode->numOfGroupKeys;

  SDataBlockDescNode* pChildTupe = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = getChildTuple(&pChildTupe, pChildren);
  }

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

  // For external-window + fill queries: the fill operator writes its output into the
  // ExtWin output block (slots for s1, s2, ...) but Sort only copies columns listed in
  // its pTargets.  Those columns are only in pTargets when they appear in the ORDER BY
  // clause; projection-only columns are missing.  Append them here so Sort passes the
  // fill values through to the parent Project operator.
  //
  // NOTE: This injection only handles the single-node topology where Sort's direct
  // child is ExtWin.  In distributed plans (extWinSplit=true), the plan is split into
  // HASH_EXTERNAL (partition) + Merge + MERGE_ALIGNED_EXTERNAL (coordinator); Sort
  // sits above the Merge node, so this code does not fire.  Fill + ORDER BY in
  // distributed external-window queries is currently not supported.
  if (TSDB_CODE_SUCCESS == code) {
    SPhysiNode* pChildNode = (SPhysiNode*)nodesListGetNode(pChildren, 0);
    if (pChildNode != NULL && (nodeType(pChildNode) == QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL ||
                               nodeType(pChildNode) == QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL)) {
      SExternalWindowPhysiNode* pExtWin = (SExternalWindowPhysiNode*)pChildNode;
      if (pExtWin->extFill.pFillExprs != NULL && pExtWin->window.pProjs != NULL) {
        // Iterate ExtWin's pProjs (STargetNode list): each contains the output slotId for
        // a fill-output column (s1, s2, ...).  For each one that is not yet in Sort's
        // pTargets, create a passthrough STargetNode and append it.
        SNode* pProjNode = NULL;
        FOREACH(pProjNode, pExtWin->window.pProjs) {
          if (nodeType(pProjNode) != QUERY_NODE_TARGET) continue;
          STargetNode* pExtTarget = (STargetNode*)pProjNode;
          int16_t      projSlotId = pExtTarget->slotId;

          // Check if this slot is already covered by Sort's pTargets.
          bool alreadyCovered = false;
          SNode* pSortTarget = NULL;
          FOREACH(pSortTarget, pSort->pTargets) {
            if (nodeType(pSortTarget) == QUERY_NODE_TARGET) {
              STargetNode* pST = (STargetNode*)pSortTarget;
              if (nodeType(pST->pExpr) == QUERY_NODE_COLUMN) {
                SColumnNode* pSC = (SColumnNode*)pST->pExpr;
                if (pSC->dataBlockId == pChildTupe->dataBlockId && pSC->slotId == projSlotId) {
                  alreadyCovered = true;
                  break;
                }
              }
            }
          }
          if (alreadyCovered) continue;

          // Find the corresponding SlotDescNode in ExtWin's output to get the data type.
          SSlotDescNode* pSlotDesc = NULL;
          SNode* pSlotNode = NULL;
          FOREACH(pSlotNode, pExtWin->window.node.pOutputDataBlockDesc->pSlots) {
            SSlotDescNode* pSD = (SSlotDescNode*)pSlotNode;
            if (pSD->slotId == projSlotId) { pSlotDesc = pSD; break; }
          }
          if (pSlotDesc == NULL) continue;

          // Create a column node referring to ExtWin's projSlotId.
          SColumnNode* pCol = NULL;
          code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
          if (code != TSDB_CODE_SUCCESS) break;
          pCol->dataBlockId = pChildTupe->dataBlockId;
          pCol->slotId = projSlotId;
          pCol->colType = COLUMN_TYPE_COLUMN;
          pCol->node.resType = pSlotDesc->dataType;
          tstrncpy(pCol->colName, pSlotDesc->name, TSDB_COL_NAME_LEN);
          tstrncpy(pCol->node.aliasName, pSlotDesc->name, TSDB_COL_NAME_LEN);
          tstrncpy(pCol->node.userAlias, pSlotDesc->name, TSDB_COL_NAME_LEN);

          // Allocate a new slot in Sort's output block.
          int16_t nextSlot = (int16_t)LIST_LENGTH(pSort->node.pOutputDataBlockDesc->pSlots);
          code = nodesListStrictAppend(pSort->node.pOutputDataBlockDesc->pSlots,
                                       createSlotDesc(pCxt, pSlotDesc->name, (SNode*)pCol, nextSlot, true, false));
          if (code != TSDB_CODE_SUCCESS) { nodesDestroyNode((SNode*)pCol); break; }
          SHashObj* pHash = taosArrayGetP(pCxt->pLocationHelper, pSort->node.pOutputDataBlockDesc->dataBlockId);
          int32_t   nameLen = strlen(pSlotDesc->name);
          code = putSlotToHashImpl(pSort->node.pOutputDataBlockDesc->dataBlockId, nextSlot,
                                   pSlotDesc->name, nameLen, pHash);
          if (code != TSDB_CODE_SUCCESS) { nodesDestroyNode((SNode*)pCol); break; }
          pSort->node.pOutputDataBlockDesc->totalRowSize += pSlotDesc->dataType.bytes;
          pSort->node.pOutputDataBlockDesc->outputRowSize += pSlotDesc->dataType.bytes;

          // Create and append the STargetNode.
          SNode* pTarget = NULL;
          code = createTarget((SNode*)pCol, pSort->node.pOutputDataBlockDesc->dataBlockId, nextSlot, &pTarget);
          if (code != TSDB_CODE_SUCCESS) { nodesDestroyNode((SNode*)pCol); break; }
          code = nodesListMakeStrictAppend(&pSort->pTargets, pTarget);
          if (code != TSDB_CODE_SUCCESS) { nodesDestroyNode(pTarget); break; }
        }
      }
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
  pPart->phasedMode = pPartLogicNode->phasedMode;
  pPart->needBlockOutputTsOrder = pPartLogicNode->needBlockOutputTsOrder;
  SDataBlockDescNode* pChildTupe = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = getChildTuple(&pChildTupe, pChildren);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pPartLogicNode->node.pTargets, &pPart->pTargets);
    if (TSDB_CODE_SUCCESS == code) {
      code = addDataBlockSlots(pCxt, pPart->pTargets, pPart->node.pOutputDataBlockDesc);
    }
  }
  // push down expression to pOutputDataBlockDesc of child node
  if (TSDB_CODE_SUCCESS == code && NULL != pPrecalcExprs) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pPrecalcExprs, &pPart->pExprs);
    if (TSDB_CODE_SUCCESS == code) {
      code = pushdownDataBlockSlots(pCxt, pPart->pExprs, pChildTupe);
    }
  }

  // Convert pseudo-column functions (e.g. TBNAME) in partition keys to column
  // nodes.  doSetSlotId only resolves QUERY_NODE_COLUMN; a FUNCTION node passes
  // through with an uninitialised slotId, and the executor's
  // makeColumnArrayFromList then casts it to SColumnNode*  =>  garbage slotId/type
  //  =>  buffer overflow in doHashPartition.  Replacing with a COLUMN whose
  // colName matches the slot added by addDataBlockSlots(pScanPseudoCols) lets
  // doSetSlotId resolve the correct slotId.
  if (TSDB_CODE_SUCCESS == code) {
    SNode* pKeyNode = NULL;
    FOREACH(pKeyNode, pPartitionKeys) {
      if (QUERY_NODE_FUNCTION == nodeType(pKeyNode) &&
          fmIsScanPseudoColumnFunc(((SFunctionNode*)pKeyNode)->funcId)) {
        SFunctionNode* pFunc = (SFunctionNode*)pKeyNode;
        SColumnNode*   pCol  = NULL;
        code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
        if (TSDB_CODE_SUCCESS != code) break;
        pCol->node.resType = pFunc->node.resType;
        tstrncpy(pCol->colName, pFunc->node.aliasName, TSDB_COL_NAME_LEN);
        tstrncpy(pCol->node.aliasName, pFunc->node.aliasName, sizeof(pCol->node.aliasName));
        tstrncpy(pCol->node.userAlias, pFunc->node.userAlias, sizeof(pCol->node.userAlias));
        nodesDestroyNode(pKeyNode);
        REPLACE_NODE(pCol);
      }
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pPartitionKeys, &pPart->pPartitionKeys);
  }

  if (pPart->needBlockOutputTsOrder) {
    SNode* node;
    bool   found = false;
    // Iterate the PHYSICAL targets (pPart->pTargets) to get correct physical slot IDs.
    // pPartLogicNode->node.pTargets has logical slotIds (often 0) which are wrong here.
    FOREACH(node, pPart->pTargets) {
      if (nodeType(node) == QUERY_NODE_TARGET) {
        STargetNode* pTgt = (STargetNode*)node;
        if (nodeType(pTgt->pExpr) == QUERY_NODE_COLUMN) {
          SColumnNode* pCol = (SColumnNode*)pTgt->pExpr;
          if (pCol->tableId == pPartLogicNode->pkTsColTbId && pCol->colId == pPartLogicNode->pkTsColId) {
            pPart->tsSlotId = pTgt->slotId;
            found = true;
            break;
          }
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

static int32_t createPartitionPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren,
                                        SPartitionLogicNode* pPartLogicNode, SPhysiNode** pPhyNode) {
  return createPartitionPhysiNodeImpl(pCxt, pChildren, pPartLogicNode, QUERY_NODE_PHYSICAL_PLAN_PARTITION, pPhyNode);
}

static int32_t createFillPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SFillLogicNode* pFillNode,
                                   SPhysiNode** pPhyNode) {
  SFillPhysiNode* pFill = (SFillPhysiNode*)makePhysiNode(
      pCxt, (SLogicNode*)pFillNode, QUERY_NODE_PHYSICAL_PLAN_FILL);
  if (NULL == pFill) {
    return terrno;
  }

  pFill->mode = pFillNode->mode;
  pFill->indefRowsMode = pFillNode->indefRowsMode;
  pFill->interval = pFillNode->interval;
  pFill->offset = pFillNode->offset;
  pFill->sliding = pFillNode->sliding;
  pFill->intervalUnit = pFillNode->intervalUnit;
  pFill->slidingUnit = pFillNode->slidingUnit;
  pFill->firstDayOfWeek = pFillNode->firstDayOfWeek;
  pFill->timezone = pFillNode->timezone;
  pFill->ownsTimezone = false;
  tstrncpy(pFill->timezoneName, pFillNode->timezoneName, sizeof(pFill->timezoneName));
  pFill->timeRange = pFillNode->timeRange;
  TSWAP(pFill->pTimeRange, pFillNode->pTimeRange);
  pFill->node.inputTsOrder = pFillNode->node.inputTsOrder;
  int32_t             code = TSDB_CODE_SUCCESS;
  SDataBlockDescNode* pChildTupe = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = getChildTuple(&pChildTupe, pChildren);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pFillNode->pFillExprs, &pFill->pFillExprs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = addDataBlockSlots(pCxt, pFill->pFillExprs, pFill->node.pOutputDataBlockDesc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pFillNode->pNotFillExprs, &pFill->pNotFillExprs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = addDataBlockSlots(pCxt, pFill->pNotFillExprs, pFill->node.pOutputDataBlockDesc);
  }
  if (TSDB_CODE_SUCCESS == code && LIST_LENGTH(pFillNode->pFillNullExprs) > 0) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pFillNode->pFillNullExprs, &pFill->pFillNullExprs);
    if (TSDB_CODE_SUCCESS == code) {
      code = addDataBlockSlots(pCxt, pFill->pFillNullExprs, pFill->node.pOutputDataBlockDesc);
    }
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

  if (TSDB_CODE_SUCCESS == code && NULL != pFillNode->pSurroundingTime) {
    code = nodesCloneNode(pFillNode->pSurroundingTime,
                          &pFill->pSurroundingTime);
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

static int32_t createExchangePhysiNodeByMerge(SMergePhysiNode* pMerge, int32_t idx, bool grpSingleChannel) {
  SExchangePhysiNode* pExchange = NULL;
  int32_t             code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_EXCHANGE, (SNode**)&pExchange);
  if (NULL == pExchange) {
    return code;
  }
  pExchange->srcStartGroupId = pMerge->srcGroupId + idx;
  pExchange->srcEndGroupId = pMerge->srcGroupId + idx;
  pExchange->grpSingleChannel = grpSingleChannel;
  pExchange->singleSrc = true;
  pExchange->node.dynamicOp = pMerge->node.dynamicOp;
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

static int32_t createMergePhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SMergeLogicNode* pMergeLogicNode,
                                    SPhysiNode** pPhyNode) {
  int32_t          code = TSDB_CODE_SUCCESS;
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
  pMerge->outerGroupPhaseMode = pMergeLogicNode->outerGroupPhaseMode;

  if (!pMergeLogicNode->colsMerge) {
    PLAN_ERR_JRET(addDataBlockSlots(pCxt, pMergeLogicNode->pInputs, pMerge->node.pOutputDataBlockDesc));

    for (int32_t j = 0; j < pMergeLogicNode->numOfSubplans; ++j) {
      for (int32_t i = 0; i < pMerge->numOfChannels; ++i) {
        PLAN_ERR_JRET(createExchangePhysiNodeByMerge(pMerge, j, pMerge->numOfChannels == 1));
      }
    }

    if (NULL != pMergeLogicNode->pMergeKeys) {
      PLAN_ERR_JRET(setListSlotId(pCxt, pMerge->node.pOutputDataBlockDesc->dataBlockId, -1, pMergeLogicNode->pMergeKeys,
                                  &pMerge->pMergeKeys));
    }

    PLAN_ERR_JRET(setListSlotId(pCxt, pMerge->node.pOutputDataBlockDesc->dataBlockId, -1, pMergeLogicNode->node.pTargets,
                                &pMerge->pTargets));
    PLAN_ERR_JRET(addDataBlockSlots(pCxt, pMerge->pTargets, pMerge->node.pOutputDataBlockDesc));
  } else {
    PLAN_ERR_JRET(setMultiBlockSlotId(pCxt, pChildren, false, pMergeLogicNode->node.pTargets, &pMerge->pTargets));
    PLAN_ERR_JRET(addDataBlockSlots(pCxt, pMerge->pTargets, pMerge->node.pOutputDataBlockDesc));
  }

  *pPhyNode = (SPhysiNode*)pMerge;
  return code;
_return:
  nodesDestroyNode((SNode*)pMerge);
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
      return createProjectPhysiNode(pCxt, pChildren, (SProjectLogicNode*)pLogicNode, pSubplan, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_EXCHANGE:
      return createExchangePhysiNode(pCxt, (SExchangeLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_WINDOW:
      return createWindowPhysiNode(pCxt, pChildren, (SWindowLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_SORT:
      return createSortPhysiNode(pCxt, pChildren, (SSortLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_WINDOW_FUNC:
      return createWindowFuncPhysiNode(pCxt, pChildren, (SWindowFuncLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_PARTITION:
      return createPartitionPhysiNode(pCxt, pChildren, (SPartitionLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_FILL:
      return createFillPhysiNode(pCxt, pChildren, (SFillLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC:
      return createIndefRowsFuncPhysiNode(pCxt, pChildren, (SIndefRowsFuncLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_INTERP_FUNC:
      return createInterpFuncPhysiNode(pCxt, pChildren, (SInterpFuncLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_ANALYSIS_FUNC:
      return createGenericAnalysisPhysiNode(pCxt, pChildren, (SGenericAnalysisLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_FORECAST_FUNC:
      return createForecastFuncPhysiNode(pCxt, pChildren, (SForecastFuncLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_ROWSET_SOURCE:
      return createRowsetSourcePhysiNode(pCxt, (SRowsetSourceLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_MERGE:
      return createMergePhysiNode(pCxt, pChildren, (SMergeLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_GROUP_CACHE:
      return createGroupCachePhysiNode(pCxt, pChildren, (SGroupCacheLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL:
      return createDynQueryCtrlPhysiNode(pCxt, pChildren, (SDynQueryCtrlLogicNode*)pLogicNode, pPhyNode, pSubplan);
    case QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN:
      return createVirtualTableScanPhysiNode(pCxt, pSubplan, pChildren, (SVirtualScanLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_TAG_REF_SOURCE:
      return createTagRefSourcePhysiNode(pCxt, pSubplan, pChildren, (STagRefSourceLogicNode*)pLogicNode, pPhyNode);
    default:
      break;
  }

  return TSDB_CODE_FAILED;
}

static int32_t createPhysiNode(SPhysiPlanContext* pCxt, SLogicNode* pLogicNode, SSubplan* pSubplan,
                               SPhysiNode** pPhyNode) {
  SNodeList* pChildren = NULL;
  int32_t    code = nodesMakeList(&pChildren);
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
  int32_t            code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_INSERT, (SNode**)&pInserter);
  if (NULL == pInserter) {
    return code;
  }

  pInserter->numOfTables = pBlocks->numOfTables;
  pInserter->size = pBlocks->size;
  pInserter->pData = pBlocks->pData;
  pBlocks->pData = NULL;

  *pSink = (SDataSinkNode*)pInserter;
  return TSDB_CODE_SUCCESS;
}

static int32_t createDataDispatcher(SPhysiPlanContext* pCxt, const SPhysiNode* pRoot, bool dynamicSchema,
                                    bool localBlockExchange, SDataSinkNode** pSink) {
  SDataDispatcherNode* pDispatcher = NULL;
  int32_t              code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_DISPATCH, (SNode**)&pDispatcher);
  if (NULL == pDispatcher) {
    return code;
  }

  pDispatcher->sink.pInputDataBlockDesc = NULL;
  code = nodesCloneNode((SNode*)pRoot->pOutputDataBlockDesc, (SNode**)&pDispatcher->sink.pInputDataBlockDesc);
  if (NULL == pDispatcher->sink.pInputDataBlockDesc) {
    nodesDestroyNode((SNode*)pDispatcher);
    return code;
  }
  pDispatcher->dynamicSchema = dynamicSchema;
  pDispatcher->localBlockExchange = localBlockExchange;

  *pSink = (SDataSinkNode*)pDispatcher;
  return TSDB_CODE_SUCCESS;
}

static int32_t makeSubplan(SPhysiPlanContext* pCxt, SLogicSubplan* pLogicSubplan, SSubplan** ppSubplan) {
  SSubplan* pSubplan = NULL;
  int32_t   code = nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, (SNode**)&pSubplan);
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
  pSubplan->userAppId = pCxt->pPlanCxt->userAppId;
  pSubplan->processOneBlock = pLogicSubplan->processOneBlock;
  pSubplan->dynTbname = pLogicSubplan->dynTbname;
  pSubplan->localBlockExchange = (pLogicSubplan->splitFlag & SUBPLAN_SPLIT_FLAG_LOCAL_BLOCK_EXCHANGE) != 0;
  pSubplan->execOnChildNode = pLogicSubplan->execOnChildNode;
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
  int32_t             code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_QUERY_INSERT, (SNode**)&pInserter);
  if (NULL == pInserter) {
    return code;
  }

  pInserter->tableId = pModify->tableId;
  pInserter->stableId = pModify->stableId;
  pInserter->tableType = pModify->tableType;
  tstrncpy(pInserter->tableName, pModify->tableName, TSDB_TABLE_NAME_LEN);
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
  int32_t           code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_DELETE, (SNode**)&pDeleter);
  if (NULL == pDeleter) {
    return code;
  }

  pDeleter->tableId = pModify->tableId;
  pDeleter->tableType = pModify->tableType;
  tstrncpy(pDeleter->tableFName, pModify->tableName, TSDB_TABLE_NAME_LEN);
  tstrncpy(pDeleter->tsColName, pModify->tsColName, TSDB_COL_NAME_LEN);
  pDeleter->deleteTimeRange = pModify->deleteTimeRange;
  pDeleter->secureDelete = pModify->secureDelete;
  pDeleter->rangeMode = pModify->deleteRangeMode;
  pDeleter->tsSlotId = -1;

  if (pDeleter->rangeMode) {
    // pStartTs carries the ts column here, not first(ts); resolve it against the scan output so the
    // deleter can read the timestamps out of every incoming block.
    code = setNodeSlotId(pCxt, pRoot->pOutputDataBlockDesc->dataBlockId, -1, pModify->pStartTs, &pDeleter->pStartTs);
    if (TSDB_CODE_SUCCESS == code) {
      if (QUERY_NODE_COLUMN != nodeType(pDeleter->pStartTs)) {
        nodesDestroyNode((SNode*)pDeleter);
        return TSDB_CODE_PLAN_INTERNAL_ERROR;
      }
      pDeleter->tsSlotId = ((SColumnNode*)pDeleter->pStartTs)->slotId;
    }
  } else {
    code = setNodeSlotId(pCxt, pRoot->pOutputDataBlockDesc->dataBlockId, -1, pModify->pAffectedRows,
                         &pDeleter->pAffectedRows);
    if (TSDB_CODE_SUCCESS == code) {
      code = setNodeSlotId(pCxt, pRoot->pOutputDataBlockDesc->dataBlockId, -1, pModify->pStartTs, &pDeleter->pStartTs);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = setNodeSlotId(pCxt, pRoot->pOutputDataBlockDesc->dataBlockId, -1, pModify->pEndTs, &pDeleter->pEndTs);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    pDeleter->sink.pInputDataBlockDesc = NULL;
    code = nodesCloneNode((SNode*)pRoot->pOutputDataBlockDesc, (SNode**)&pDeleter->sink.pInputDataBlockDesc);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pSink = (SDataSinkNode*)pDeleter;
  } else {
    nodesDestroyNode((SNode*)pDeleter);
  }

  return code;
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
  int32_t   code = makeSubplan(pCxt, pLogicSubplan, &pSubplan);
  if (NULL == pSubplan) {
    return code;
  }

  // Set execNode from logic subplan's vgroup list
  // After split phase, vgroup list is transferred from logic node to logic subplan via TSWAP
  // We need to set execNode here before creating physical nodes
  if (pLogicSubplan->pVgroupList && pLogicSubplan->pVgroupList->numOfVgroups > 0) {
    vgroupInfoToNodeAddr(pLogicSubplan->pVgroupList->vgroups, &pSubplan->execNode);
    pSubplan->execNodeStat.tableNum = pLogicSubplan->pVgroupList->vgroups[0].numOfTable;
  }

  if (SUBPLAN_TYPE_MODIFY == pLogicSubplan->subplanType) {
    code = buildVnodeModifySubplan(pCxt, pLogicSubplan, pSubplan);
  } else {
    if (SUBPLAN_TYPE_SCAN == pSubplan->subplanType || SUBPLAN_TYPE_HSYSSCAN == pSubplan->subplanType) {
      pSubplan->msgType = TDMT_SCH_QUERY;
    } else {
      pSubplan->msgType = TDMT_SCH_MERGE_QUERY;
    }
    code = createPhysiNode(pCxt, pLogicSubplan->pNode, pSubplan, &pSubplan->pNode);
    if (TSDB_CODE_SUCCESS == code &&
        (SUBPLAN_TYPE_SCAN == pSubplan->subplanType || SUBPLAN_TYPE_HSYSSCAN == pSubplan->subplanType) &&
        QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL == nodeType(pSubplan->pNode) &&
        ((SIntervalPhysiNode*)pSubplan->pNode)->exInterval) {
      pSubplan->delayFreeTask = true;
    }
    if (TSDB_CODE_SUCCESS == code && pSubplan->localBlockExchange) {
      pSubplan->delayFreeTask = true;
    }
    if (TSDB_CODE_SUCCESS == code && !pCxt->pPlanCxt->topicQuery) {
      code = createDataDispatcher(pCxt, pSubplan->pNode, pSubplan->processOneBlock,
                                  pSubplan->localBlockExchange, &pSubplan->pDataSink);
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
  int32_t     code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN, (SNode**)&pPlan);
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

static bool physiNodeRoutesSubplanGroup(const SPhysiNode* pNode, int32_t groupId) {
  if (QUERY_NODE_PHYSICAL_PLAN_EXCHANGE == nodeType(pNode)) {
    const SExchangePhysiNode* pExchange = (const SExchangePhysiNode*)pNode;
    return groupId >= pExchange->srcStartGroupId && groupId <= pExchange->srcEndGroupId;
  }
  if (QUERY_NODE_PHYSICAL_PLAN_MERGE == nodeType(pNode)) {
    const SMergePhysiNode* pMerge = (const SMergePhysiNode*)pNode;
    return groupId >= pMerge->srcGroupId && groupId <= pMerge->srcEndGroupId;
  }
  return false;
}

static bool physiSubplanEdgeInheritsMultiGroupJoin(const SPhysiNode* pNode, int32_t groupId, bool belowMultiGroupJoin) {
  if (NULL == pNode) {
    return false;
  }

  if (physiNodeRoutesSubplanGroup(pNode, groupId)) {
    return belowMultiGroupJoin;
  }

  bool   childBelowMultiGroupJoin = belowMultiGroupJoin || (QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN == nodeType(pNode) &&
                                                          ((const SHashJoinPhysiNode*)pNode)->multiGroupJoin);
  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    if (physiSubplanEdgeInheritsMultiGroupJoin((const SPhysiNode*)pChild, groupId, childBelowMultiGroupJoin)) {
      return true;
    }
  }
  return false;
}

static int32_t buildPhysiPlan(SPhysiPlanContext* pCxt, SLogicSubplan* pLogicSubplan, SSubplan* pParent,
                              SQueryPlan* pQueryPlan, bool inheritedMultiGroupJoin) {
  SSubplan* pSubplan = NULL;
  int32_t   code = createPhysiSubplan(pCxt, pLogicSubplan, &pSubplan);

  if (TSDB_CODE_SUCCESS == code) {
    pSubplan->inheritedMultiGroupJoin = inheritedMultiGroupJoin;
  }

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
      SLogicSubplan* pLogicChild = (SLogicSubplan*)pChild;
      bool           childInheritedMultiGroupJoin =
          inheritedMultiGroupJoin ||
          physiSubplanEdgeInheritsMultiGroupJoin(pSubplan->pNode, pLogicChild->id.groupId, false);
      code = buildPhysiPlan(pCxt, pLogicChild, pSubplan, pQueryPlan, childInheritedMultiGroupJoin);
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    }
  }

  return code;
}

static int32_t doCreatePhysiPlan(SPhysiPlanContext* pCxt, SQueryLogicPlan* pLogicPlan, SQueryPlan** pPhysiPlan) {
  SQueryPlan* pPlan = NULL;
  int32_t     code = makeQueryPhysiPlan(pCxt, &pPlan);
  if (NULL == pPlan) {
    return code;
  }

  SNode* pSubplan = NULL;
  FOREACH(pSubplan, pLogicPlan->pTopSubplans) {
    code = buildPhysiPlan(pCxt, (SLogicSubplan*)pSubplan, NULL, pPlan, false);
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    // * Propagate hasFederatedScan / hasScan flags to the plan output
    pPlan->hasFederatedScan = pCxt->hasFederatedScan;
    pPlan->hasScan          = pCxt->pPlanCxt->hasScan;
    *pPhysiPlan = pPlan;
  } else {
    nodesDestroyNode((SNode*)pPlan);
  }

  return code;
}

static void destoryLocationHash(void* p) {
  destroySlotIndexHash(*(SHashObj**)p);
}

static void destoryPhysiPlanContext(SPhysiPlanContext* pCxt) {
  taosArrayDestroyEx(pCxt->pLocationHelper, destoryLocationHash);
  taosArrayDestroyEx(pCxt->pProjIdxLocHelper, destoryLocationHash);
}

static void setExplainInfo(SPlanContext* pCxt, SQueryPlan* pPlan) {
  if (QUERY_NODE_EXPLAIN_STMT == nodeType(pCxt->pAstRoot)) {
    SExplainStmt* pStmt = (SExplainStmt*)pCxt->pAstRoot;
    pPlan->explainInfo.mode = pStmt->analyze ? EXPLAIN_MODE_ANALYZE : EXPLAIN_MODE_STATIC;
    pPlan->explainInfo.verbose = pStmt->pOptions->verbose;
    pPlan->explainInfo.ratio = pStmt->pOptions->ratio;
    pPlan->explainInfo.tz = pCxt->timezone;
  } else {
    pPlan->explainInfo.mode = EXPLAIN_MODE_DISABLE;
  }
}

static int32_t setExecNodeList(SPhysiPlanContext* pCxt, SArray* pExecNodeList) {
  int32_t code = 0;
  if (NULL == pExecNodeList) {
    return code;
  }

  if (IS_SYS_SCAN(pCxt->pPlanCxt->sysScanFlag) || !pCxt->pPlanCxt->hasScan) {
    SQueryNodeLoad node = {.addr = {.nodeId = MNODE_HANDLE, .epSet = pCxt->pPlanCxt->mgmtEpSet}, .load = 0};
    if (NULL == taosArrayPush(pExecNodeList, &node)) code = terrno;
  }
  return code;
}

int32_t createPhysiPlan(SPlanContext* pCxt, SQueryLogicPlan* pLogicPlan, SQueryPlan** pPlan, SArray* pExecNodeList) {
  SPhysiPlanContext cxt = {.pPlanCxt = pCxt,
                           .errCode = TSDB_CODE_SUCCESS,
                           .nextDataBlockId = 0,
                           .pLocationHelper = taosArrayInit(32, POINTER_BYTES),
                           .pProjIdxLocHelper = taosArrayInit(32, POINTER_BYTES),
                           };
  if (NULL == cxt.pLocationHelper || !cxt.pProjIdxLocHelper) {
    taosArrayDestroy(cxt.pLocationHelper);
    taosArrayDestroy(cxt.pProjIdxLocHelper);
    return terrno;
  }

  int32_t code = doCreatePhysiPlan(&cxt, pLogicPlan, pPlan);
  if (TSDB_CODE_SUCCESS == code) {
    setExplainInfo(pCxt, *pPlan);
    code = setExecNodeList(&cxt, pExecNodeList);
  }

  destoryPhysiPlanContext(&cxt);
  return code;
}
