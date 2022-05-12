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

#include "planner.h"

#include "planInt.h"
#include "scalar.h"

typedef struct SCollectPlaceholderValuesCxt {
  int32_t errCode;
  SArray* pValues;
} SCollectPlaceholderValuesCxt;

static EDealRes collectPlaceholderValuesImpl(SNode* pNode, void* pContext) {
  if (QUERY_NODE_VALUE == nodeType(pNode) && ((SValueNode*)pNode)->placeholderNo > 0) {
    SCollectPlaceholderValuesCxt* pCxt = pContext;
    taosArrayInsert(pCxt->pValues, ((SValueNode*)pNode)->placeholderNo - 1, &pNode);
    return TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_IGNORE_CHILD : DEAL_RES_ERROR;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t collectPlaceholderValues(SPlanContext* pCxt, SQueryPlan* pPlan) {
  pPlan->pPlaceholderValues = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);

  SCollectPlaceholderValuesCxt cxt = {.errCode = TSDB_CODE_SUCCESS, .pValues = pPlan->pPlaceholderValues};
  nodesWalkPhysiPlan((SNode*)pPlan, collectPlaceholderValuesImpl, &cxt);
  return cxt.errCode;
}

int32_t qCreateQueryPlan(SPlanContext* pCxt, SQueryPlan** pPlan, SArray* pExecNodeList) {
  SLogicNode*      pLogicNode = NULL;
  SLogicSubplan*   pLogicSubplan = NULL;
  SQueryLogicPlan* pLogicPlan = NULL;

  int32_t code = createLogicPlan(pCxt, &pLogicNode);
  if (TSDB_CODE_SUCCESS == code) {
    code = optimizeLogicPlan(pCxt, pLogicNode);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = splitLogicPlan(pCxt, pLogicNode, &pLogicSubplan);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = scaleOutLogicPlan(pCxt, pLogicSubplan, &pLogicPlan);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createPhysiPlan(pCxt, pLogicPlan, pPlan, pExecNodeList);
  }
  if (TSDB_CODE_SUCCESS == code && pCxt->placeholderNum > 0) {
    code = collectPlaceholderValues(pCxt, *pPlan);
  }

  nodesDestroyNode(pLogicNode);
  nodesDestroyNode(pLogicSubplan);
  nodesDestroyNode(pLogicPlan);
  terrno = code;
  return code;
}

static int32_t setSubplanExecutionNode(SPhysiNode* pNode, int32_t groupId, SDownstreamSourceNode* pSource) {
  if (QUERY_NODE_PHYSICAL_PLAN_EXCHANGE == nodeType(pNode)) {
    SExchangePhysiNode* pExchange = (SExchangePhysiNode*)pNode;
    if (pExchange->srcGroupId == groupId) {
      if (NULL == pExchange->pSrcEndPoints) {
        pExchange->pSrcEndPoints = nodesMakeList();
        if (NULL == pExchange->pSrcEndPoints) {
          return TSDB_CODE_OUT_OF_MEMORY;
        }
      }
      if (TSDB_CODE_SUCCESS != nodesListStrictAppend(pExchange->pSrcEndPoints, nodesCloneNode(pSource))) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      return TSDB_CODE_SUCCESS;
    }
  }

  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    if (TSDB_CODE_SUCCESS != setSubplanExecutionNode((SPhysiNode*)pChild, groupId, pSource)) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t qSetSubplanExecutionNode(SSubplan* subplan, int32_t groupId, SDownstreamSourceNode* pSource) {
  return setSubplanExecutionNode(subplan->pNode, groupId, pSource);
}

static int32_t setValueByBindParam(SValueNode* pVal, TAOS_MULTI_BIND* pParam) {
  if (pParam->is_null && 1 == *(pParam->is_null)) {
    pVal->node.resType.type = TSDB_DATA_TYPE_NULL;
    pVal->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_NULL].bytes;
    return TSDB_CODE_SUCCESS;
  }
  int32_t inputSize = (NULL != pParam->length ? *(pParam->length) : tDataTypes[pParam->buffer_type].bytes);
  pVal->node.resType.type = pParam->buffer_type;
  pVal->node.resType.bytes = inputSize;
  switch (pParam->buffer_type) {
    case TSDB_DATA_TYPE_BOOL:
      pVal->datum.b = *((bool*)pParam->buffer);
      break;
    case TSDB_DATA_TYPE_TINYINT:
      pVal->datum.i = *((int8_t*)pParam->buffer);
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      pVal->datum.i = *((int16_t*)pParam->buffer);
      break;
    case TSDB_DATA_TYPE_INT:
      pVal->datum.i = *((int32_t*)pParam->buffer);
      break;
    case TSDB_DATA_TYPE_BIGINT:
      pVal->datum.i = *((int64_t*)pParam->buffer);
      break;
    case TSDB_DATA_TYPE_FLOAT:
      pVal->datum.d = *((float*)pParam->buffer);
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      pVal->datum.d = *((double*)pParam->buffer);
      break;
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY:
      pVal->datum.p = taosMemoryCalloc(1, pVal->node.resType.bytes + VARSTR_HEADER_SIZE + 1);
      if (NULL == pVal->datum.p) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      varDataSetLen(pVal->datum.p, pVal->node.resType.bytes);
      strncpy(varDataVal(pVal->datum.p), (const char*)pParam->buffer, pVal->node.resType.bytes);
      break;
    case TSDB_DATA_TYPE_NCHAR: {
      pVal->node.resType.bytes *= TSDB_NCHAR_SIZE;
      pVal->datum.p = taosMemoryCalloc(1, pVal->node.resType.bytes + VARSTR_HEADER_SIZE + 1);
      if (NULL == pVal->datum.p) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }

      int32_t output = 0;
      if (!taosMbsToUcs4(pParam->buffer, inputSize, (TdUcs4*)varDataVal(pVal->datum.p), pVal->node.resType.bytes,
                         &output)) {
        return errno;
      }
      varDataSetLen(pVal->datum.p, output);
      pVal->node.resType.bytes = output;
      break;
    }
    case TSDB_DATA_TYPE_TIMESTAMP:
      pVal->datum.i = *((int64_t*)pParam->buffer);
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      pVal->datum.u = *((uint8_t*)pParam->buffer);
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      pVal->datum.u = *((uint16_t*)pParam->buffer);
      break;
    case TSDB_DATA_TYPE_UINT:
      pVal->datum.u = *((uint32_t*)pParam->buffer);
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      pVal->datum.u = *((uint64_t*)pParam->buffer);
      break;
    case TSDB_DATA_TYPE_JSON:
    case TSDB_DATA_TYPE_DECIMAL:
    case TSDB_DATA_TYPE_BLOB:
    case TSDB_DATA_TYPE_MEDIUMBLOB:
      // todo
    default:
      break;
  }
  pVal->translate = true;
  return TSDB_CODE_SUCCESS;
}

static EDealRes updatePlanQueryId(SNode* pNode, void* pContext) {
  int64_t queryId = *(uint64_t*)pContext;

  if (QUERY_NODE_PHYSICAL_PLAN == nodeType(pNode)) {
    SQueryPlan* planNode = (SQueryPlan*)pNode;
    planNode->queryId = queryId;
  } else if (QUERY_NODE_PHYSICAL_SUBPLAN == nodeType(pNode)) {
    SSubplan* subplanNode = (SSubplan*)pNode;
    subplanNode->id.queryId = queryId;
  }

  return DEAL_RES_CONTINUE;
}

static int32_t calcConstNode(SNode** pNode) {
  if (NULL == *pNode) {
    return TSDB_CODE_SUCCESS;
  }

  SNode*  pNew = NULL;
  int32_t code = scalarCalculateConstants(*pNode, &pNew);
  if (TSDB_CODE_SUCCESS == code) {
    *pNode = pNew;
  }
  return code;
}

static int32_t calcConstList(SNodeList* pList) {
  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    SNode*  pNew = NULL;
    int32_t code = scalarCalculateConstants(pNode, &pNew);
    if (TSDB_CODE_SUCCESS == code) {
      REPLACE_NODE(pNew);
    } else {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static bool isEmptyResultCond(SNode** pCond) {
  if (NULL == *pCond || QUERY_NODE_VALUE != nodeType(*pCond)) {
    return false;
  }
  if (((SValueNode*)*pCond)->datum.b) {
    nodesDestroyNode(*pCond);
    *pCond = NULL;
    return false;
  }
  return true;
}

static int32_t calcConstSpecificPhysiNode(SPhysiNode* pPhyNode) {
  switch (nodeType(pPhyNode)) {
    case QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_EXCHANGE:
    case QUERY_NODE_PHYSICAL_PLAN_FILL:
      return TSDB_CODE_SUCCESS;
    case QUERY_NODE_PHYSICAL_PLAN_PROJECT:
      return calcConstList(((SProjectPhysiNode*)pPhyNode)->pProjections);
    case QUERY_NODE_PHYSICAL_PLAN_JOIN:
      return calcConstNode(&(((SJoinPhysiNode*)pPhyNode)->pOnConditions));
    case QUERY_NODE_PHYSICAL_PLAN_AGG:
      return calcConstList(((SAggPhysiNode*)pPhyNode)->pExprs);
    case QUERY_NODE_PHYSICAL_PLAN_SORT:
      return calcConstList(((SSortPhysiNode*)pPhyNode)->pExprs);
    case QUERY_NODE_PHYSICAL_PLAN_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_SESSION_WINDOW:
    case QUERY_NODE_PHYSICAL_PLAN_STATE_WINDOW:
      return calcConstList(((SWinodwPhysiNode*)pPhyNode)->pExprs);
    case QUERY_NODE_PHYSICAL_PLAN_PARTITION:
      return calcConstList(((SPartitionPhysiNode*)pPhyNode)->pExprs);
    default:
      break;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t calcConstSubplan(SPhysiNode* pPhyNode, bool* pEmptyResult) {
  int32_t code = calcConstNode(&pPhyNode->pConditions);
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstSpecificPhysiNode(pPhyNode);
  }
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  *pEmptyResult = isEmptyResultCond(&pPhyNode->pConditions);
  if (*pEmptyResult) {
    return TSDB_CODE_SUCCESS;
  }

  *pEmptyResult = true;

  bool   subEmptyResult = false;
  SNode* pChild = NULL;
  FOREACH(pChild, pPhyNode->pChildren) {
    code = calcConstSubplan((SPhysiNode*)pChild, &subEmptyResult);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    if (!subEmptyResult) {
      *pEmptyResult = false;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t calcConstPhysiPlan(SQueryPlan* pPlan, bool* pEmptyResult) {
  *pEmptyResult = true;

  bool           subEmptyResult = false;
  SNodeListNode* pNode = nodesListGetNode(pPlan->pSubplans, 0);
  SNode*         pSubplan = NULL;
  FOREACH(pSubplan, pNode->pNodeList) {
    int32_t code = calcConstSubplan(((SSubplan*)pSubplan)->pNode, pEmptyResult);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    if (!subEmptyResult) {
      *pEmptyResult = false;
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t qStmtBindParam(SQueryPlan* pPlan, TAOS_MULTI_BIND* pParams, int32_t colIdx, uint64_t queryId,
                       bool* pEmptyResult) {
  int32_t size = taosArrayGetSize(pPlan->pPlaceholderValues);
  int32_t code = 0;

  if (colIdx < 0) {
    for (int32_t i = 0; i < size; ++i) {
      code = setValueByBindParam((SValueNode*)taosArrayGetP(pPlan->pPlaceholderValues, i), pParams + i);
      if (code) {
        return code;
      }
    }
  } else {
    code = setValueByBindParam((SValueNode*)taosArrayGetP(pPlan->pPlaceholderValues, colIdx), pParams);
    if (code) {
      return code;
    }
  }

  if (colIdx < 0 || ((colIdx + 1) == size)) {
    nodesWalkPhysiPlan((SNode*)pPlan, updatePlanQueryId, &queryId);
    code = calcConstPhysiPlan(pPlan, pEmptyResult);
  }

  return code;
}

int32_t qSubPlanToString(const SSubplan* pSubplan, char** pStr, int32_t* pLen) {
  if (SUBPLAN_TYPE_MODIFY == pSubplan->subplanType) {
    SDataInserterNode* insert = (SDataInserterNode*)pSubplan->pDataSink;
    *pLen = insert->size;
    *pStr = insert->pData;
    insert->pData = NULL;
    return TSDB_CODE_SUCCESS;
  }
  return nodesNodeToString((const SNode*)pSubplan, false, pStr, pLen);
}

int32_t qStringToSubplan(const char* pStr, SSubplan** pSubplan) { return nodesStringToNode(pStr, (SNode**)pSubplan); }

char* qQueryPlanToString(const SQueryPlan* pPlan) {
  char*   pStr = NULL;
  int32_t len = 0;
  if (TSDB_CODE_SUCCESS != nodesNodeToString(pPlan, false, &pStr, &len)) {
    return NULL;
  }
  return pStr;
}

SQueryPlan* qStringToQueryPlan(const char* pStr) {
  SQueryPlan* pPlan = NULL;
  if (TSDB_CODE_SUCCESS != nodesStringToNode(pStr, (SNode**)&pPlan)) {
    return NULL;
  }
  return pPlan;
}

void qDestroyQueryPlan(SQueryPlan* pPlan) { nodesDestroyNode(pPlan); }
