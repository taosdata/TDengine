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

typedef struct SCollectPlaceholderValuesCxt {
  int32_t    errCode;
  SNodeList* pValues;
} SCollectPlaceholderValuesCxt;

static EDealRes collectPlaceholderValuesImpl(SNode* pNode, void* pContext) {
  if (QUERY_NODE_VALUE == nodeType(pNode) && ((SValueNode*)pNode)->placeholderNo > 0) {
    SCollectPlaceholderValuesCxt* pCxt = pContext;
    pCxt->errCode = nodesListMakeAppend(&pCxt->pValues, pNode);
    return TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_IGNORE_CHILD : DEAL_RES_ERROR;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t collectPlaceholderValues(SPlanContext* pCxt, SQueryPlan* pPlan) {
  SCollectPlaceholderValuesCxt cxt = {.errCode = TSDB_CODE_SUCCESS, .pValues = NULL};
  nodesWalkPhysiPlan((SNode*)pPlan, collectPlaceholderValuesImpl, &cxt);
  if (TSDB_CODE_SUCCESS == cxt.errCode) {
    pPlan->pPlaceholderValues = cxt.pValues;
  } else {
    nodesDestroyList(cxt.pValues);
  }
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
  if (TSDB_CODE_SUCCESS == code && pCxt->isStmtQuery) {
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
  pVal->node.resType.type = pParam->buffer_type;
  pVal->node.resType.bytes = *(pParam->length);
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
    case TSDB_DATA_TYPE_NCHAR:
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

int32_t qStmtBindParam(SQueryPlan* pPlan, TAOS_MULTI_BIND* pParams, int32_t colIdx) {
  if (colIdx < 0) {
    int32_t index = 0;
    SNode* pNode = NULL;

    FOREACH(pNode, pPlan->pPlaceholderValues) {
      setValueByBindParam((SValueNode*)pNode, pParams + index);
      ++index;
    }
  } else {
    setValueByBindParam((SValueNode*)nodesListGetNode(pPlan->pPlaceholderValues, colIdx), pParams);
  }
  return TSDB_CODE_SUCCESS;
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
