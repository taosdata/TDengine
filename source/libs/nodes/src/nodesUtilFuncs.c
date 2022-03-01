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

#include "querynodes.h"
#include "plannodes.h"
#include "taos.h"
#include "taoserror.h"
#include "taos.h"
#include "thash.h"

static SNode* makeNode(ENodeType type, size_t size) {
  SNode* p = calloc(1, size);
  if (NULL == p) {
    return NULL;
  }
  setNodeType(p, type);
  return p;
}

SNode* nodesMakeNode(ENodeType type) {
  switch (type) {
    case QUERY_NODE_COLUMN:
      return makeNode(type, sizeof(SColumnNode));
    case QUERY_NODE_VALUE:
      return makeNode(type, sizeof(SValueNode));
    case QUERY_NODE_OPERATOR:
      return makeNode(type, sizeof(SOperatorNode));
    case QUERY_NODE_LOGIC_CONDITION:
      return makeNode(type, sizeof(SLogicConditionNode));
    case QUERY_NODE_FUNCTION:
      return makeNode(type, sizeof(SFunctionNode));
    case QUERY_NODE_REAL_TABLE:
      return makeNode(type, sizeof(SRealTableNode));
    case QUERY_NODE_TEMP_TABLE:
      return makeNode(type, sizeof(STempTableNode));
    case QUERY_NODE_JOIN_TABLE:
      return makeNode(type, sizeof(SJoinTableNode));
    case QUERY_NODE_GROUPING_SET:
      return makeNode(type, sizeof(SGroupingSetNode));
    case QUERY_NODE_ORDER_BY_EXPR:
      return makeNode(type, sizeof(SOrderByExprNode));
    case QUERY_NODE_LIMIT:
      return makeNode(type, sizeof(SLimitNode));
    case QUERY_NODE_STATE_WINDOW:
      return makeNode(type, sizeof(SStateWindowNode));
    case QUERY_NODE_SESSION_WINDOW:
      return makeNode(type, sizeof(SSessionWindowNode));
    case QUERY_NODE_INTERVAL_WINDOW:
      return makeNode(type, sizeof(SIntervalWindowNode));
    case QUERY_NODE_NODE_LIST:
      return makeNode(type, sizeof(SNodeListNode));
    case QUERY_NODE_FILL:
      return makeNode(type, sizeof(SFillNode));
    case QUERY_NODE_RAW_EXPR:
      return makeNode(type, sizeof(SRawExprNode));
    case QUERY_NODE_SET_OPERATOR:
      return makeNode(type, sizeof(SSetOperator));
    case QUERY_NODE_SELECT_STMT:
      return makeNode(type, sizeof(SSelectStmt));
    // case QUERY_NODE_SHOW_STMT:
    //   return makeNode(type, sizeof(SShowStmt));
    case QUERY_NODE_LOGIC_PLAN_SCAN:
      return makeNode(type, sizeof(SScanLogicNode));
    case QUERY_NODE_LOGIC_PLAN_JOIN:
      return makeNode(type, sizeof(SJoinLogicNode));
    case QUERY_NODE_LOGIC_PLAN_AGG:
      return makeNode(type, sizeof(SAggLogicNode));
    case QUERY_NODE_LOGIC_PLAN_PROJECT:
      return makeNode(type, sizeof(SProjectLogicNode));
    case QUERY_NODE_TARGET:
      return makeNode(type, sizeof(STargetNode));
    case QUERY_NODE_DATABLOCK_DESC:
      return makeNode(type, sizeof(SDataBlockDescNode));
    case QUERY_NODE_SLOT_DESC:
      return makeNode(type, sizeof(SSlotDescNode));
    case QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN:
      return makeNode(type, sizeof(STagScanPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN:
      return makeNode(type, sizeof(STableScanPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_PROJECT:
      return makeNode(type, sizeof(SProjectPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_JOIN:
      return makeNode(type, sizeof(SJoinPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_AGG:
      return makeNode(type, sizeof(SAggPhysiNode));
    default:
      break;
  }
  return NULL;
}

static EDealRes destroyNode(SNode** pNode, void* pContext) {
  if (NULL == pNode || NULL == *pNode) {
    return DEAL_RES_IGNORE_CHILD;
  }
  
  switch (nodeType(*pNode)) {
    case QUERY_NODE_VALUE: {
      SValueNode* pValue = (SValueNode*)*pNode;
      
      tfree(pValue->literal);
      if (IS_VAR_DATA_TYPE(pValue->node.resType.type)) {
        tfree(pValue->datum.p);
      }
      
      break;
    }
    case QUERY_NODE_LOGIC_CONDITION:
      nodesDestroyList(((SLogicConditionNode*)(*pNode))->pParameterList);
      break;
    case QUERY_NODE_FUNCTION:
      nodesDestroyList(((SFunctionNode*)(*pNode))->pParameterList);
      break;
    case QUERY_NODE_GROUPING_SET:
      nodesDestroyList(((SGroupingSetNode*)(*pNode))->pParameterList);
      break;
    case QUERY_NODE_NODE_LIST:
      nodesDestroyList(((SNodeListNode*)(*pNode))->pNodeList);
      break;
    default:
      break;
  }
  tfree(*pNode);
  return DEAL_RES_CONTINUE;
}

void nodesDestroyNode(SNode* pNode) {
  if (NULL == pNode) {
    return;
  }
  
  nodesRewriteNodePostOrder(&pNode, destroyNode, NULL);
}

SNodeList* nodesMakeList() {
  SNodeList* p = calloc(1, sizeof(SNodeList));
  if (NULL == p) {
    return NULL;
  }
  return p;
}

int32_t nodesListAppend(SNodeList* pList, SNode* pNode) {
  if (NULL == pList || NULL == pNode) {
    return TSDB_CODE_SUCCESS;
  }
  SListCell* p = calloc(1, sizeof(SListCell));
  if (NULL == p) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  p->pNode = pNode;
  if (NULL == pList->pHead) {
    pList->pHead = p;
  }
  if (NULL != pList->pTail) {
    pList->pTail->pNext = p;
  }
  pList->pTail = p;
  ++(pList->length);
  return TSDB_CODE_SUCCESS;
}

int32_t nodesListAppendList(SNodeList* pTarget, SNodeList* pSrc) {
  if (NULL == pTarget || NULL == pSrc) {
    return TSDB_CODE_SUCCESS;
  }

  if (NULL == pTarget->pHead) {
    pTarget->pHead = pSrc->pHead;
  } else {
    pTarget->pTail->pNext = pSrc->pHead;
    if (NULL != pSrc->pHead) {
      pSrc->pHead->pPrev = pTarget->pTail;
    }
  }
  pTarget->pTail = pSrc->pTail;
  pTarget->length += pSrc->length;
  tfree(pSrc);

  return TSDB_CODE_SUCCESS;
}

SListCell* nodesListErase(SNodeList* pList, SListCell* pCell) {
  if (NULL == pCell->pPrev) {
    pList->pHead = pCell->pNext;
  } else {
    pCell->pPrev->pNext = pCell->pNext;
    pCell->pNext->pPrev = pCell->pPrev;
  }
  SListCell* pNext = pCell->pNext;
  nodesDestroyNode(pCell->pNode);
  tfree(pCell);
  --(pList->length);
  return pNext;
}

SNode* nodesListGetNode(SNodeList* pList, int32_t index) {
  SNode* node;
  FOREACH(node, pList) {
    if (0 == index--) {
      return node;
    }
  }
  return NULL;
}

void nodesDestroyList(SNodeList* pList) {
  SListCell* pNext = pList->pHead;
  while (NULL != pNext) {
    pNext = nodesListErase(pList, pNext);
  }
  tfree(pList);
}

void* nodesGetValueFromNode(SValueNode *pNode) {
  switch (pNode->node.resType.type) {
    case TSDB_DATA_TYPE_BOOL:
      return (void*)&pNode->datum.b;
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      return (void*)&pNode->datum.i;
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT:
      return (void*)&pNode->datum.u;
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE: 
      return (void*)&pNode->datum.d;
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY: 
      return (void*)pNode->datum.p;
    default:
      break;
  }

  return NULL;
}

bool nodesIsExprNode(const SNode* pNode) {
  ENodeType type = nodeType(pNode);
  return (QUERY_NODE_COLUMN == type || QUERY_NODE_VALUE == type || QUERY_NODE_OPERATOR == type || QUERY_NODE_FUNCTION == type);
}

bool nodesIsArithmeticOp(const SOperatorNode* pOp) {
  switch (pOp->opType) {
    case OP_TYPE_ADD:
    case OP_TYPE_SUB:
    case OP_TYPE_MULTI:
    case OP_TYPE_DIV:
    case OP_TYPE_MOD:
      return true;
    default:
      break;
  }
  return false;
}

bool nodesIsComparisonOp(const SOperatorNode* pOp) {
  switch (pOp->opType) {
    case OP_TYPE_GREATER_THAN:
    case OP_TYPE_GREATER_EQUAL:
    case OP_TYPE_LOWER_THAN:
    case OP_TYPE_LOWER_EQUAL:
    case OP_TYPE_EQUAL:
    case OP_TYPE_NOT_EQUAL:
    case OP_TYPE_IN:
    case OP_TYPE_NOT_IN:
    case OP_TYPE_LIKE:
    case OP_TYPE_NOT_LIKE:
    case OP_TYPE_MATCH:
    case OP_TYPE_NMATCH:
    case OP_TYPE_IS_NULL:
    case OP_TYPE_IS_NOT_NULL:
    case OP_TYPE_IS_TRUE:
    case OP_TYPE_IS_FALSE:
    case OP_TYPE_IS_UNKNOWN:
    case OP_TYPE_IS_NOT_TRUE:
    case OP_TYPE_IS_NOT_FALSE:
    case OP_TYPE_IS_NOT_UNKNOWN:
      return true;
    default:
      break;
  }
  return false;
}

bool nodesIsJsonOp(const SOperatorNode* pOp) {
  switch (pOp->opType) {
    case OP_TYPE_JSON_GET_VALUE:
    case OP_TYPE_JSON_CONTAINS:
      return true;
    default:
      break;
  }
  return false;
}

bool nodesIsTimeorderQuery(const SNode* pQuery) {
  return false;
}

bool nodesIsTimelineQuery(const SNode* pQuery) {
  return false;
}

typedef struct SCollectColumnsCxt {
  int32_t errCode;
  const char* pTableAlias;
  SNodeList* pCols;
  SHashObj* pColIdHash;
} SCollectColumnsCxt;

static EDealRes doCollect(SCollectColumnsCxt* pCxt, int32_t id, SNode* pNode) {
  if (NULL == taosHashGet(pCxt->pColIdHash, &id, sizeof(id))) {
    pCxt->errCode = taosHashPut(pCxt->pColIdHash, &id, sizeof(id), NULL, 0);
    if (TSDB_CODE_SUCCESS == pCxt->errCode) {
      pCxt->errCode = nodesListAppend(pCxt->pCols, pNode);
    }
    return (TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_IGNORE_CHILD : DEAL_RES_ERROR);
  }
  return DEAL_RES_CONTINUE;
}

static EDealRes collectColumns(SNode* pNode, void* pContext) {
  SCollectColumnsCxt* pCxt = (SCollectColumnsCxt*)pContext;
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    int32_t colId = pCol->colId;
    if (0 == strcmp(pCxt->pTableAlias, pCol->tableAlias)) {
      return doCollect(pCxt, colId, pNode);
    }
  }
  return DEAL_RES_CONTINUE;
}

int32_t nodesCollectColumns(SSelectStmt* pSelect, ESqlClause clause, const char* pTableAlias, SNodeList** pCols) {
  if (NULL == pSelect || NULL == pCols) {
    return TSDB_CODE_SUCCESS;
  }

  SCollectColumnsCxt cxt = {
    .errCode = TSDB_CODE_SUCCESS,
    .pTableAlias = pTableAlias,
    .pCols = nodesMakeList(),
    .pColIdHash = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK)
  };
  if (NULL == cxt.pCols || NULL == cxt.pColIdHash) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  nodesWalkSelectStmt(pSelect, clause, collectColumns, &cxt);
  taosHashCleanup(cxt.pColIdHash);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyList(cxt.pCols);
    return cxt.errCode;
  }
  if (0 == LIST_LENGTH(cxt.pCols)) {
    nodesDestroyList(cxt.pCols);
    cxt.pCols = NULL;
  }
  *pCols = cxt.pCols;
  return TSDB_CODE_SUCCESS;
}

typedef struct SCollectFuncsCxt {
  int32_t errCode;
  FFuncClassifier classifier;
  SNodeList* pFuncs;
} SCollectFuncsCxt;

static EDealRes collectFuncs(SNode* pNode, void* pContext) {
  SCollectFuncsCxt* pCxt = (SCollectFuncsCxt*)pContext;
  if (QUERY_NODE_FUNCTION == nodeType(pNode) && pCxt->classifier(((SFunctionNode*)pNode)->funcId)) {
    pCxt->errCode = nodesListAppend(pCxt->pFuncs, pNode);
    return (TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_IGNORE_CHILD : DEAL_RES_ERROR);
  }
  return DEAL_RES_CONTINUE;
}

int32_t nodesCollectFuncs(SSelectStmt* pSelect, FFuncClassifier classifier, SNodeList** pFuncs) {
  if (NULL == pSelect || NULL == pFuncs) {
    return TSDB_CODE_SUCCESS;
  }

  SCollectFuncsCxt cxt = {
    .errCode = TSDB_CODE_SUCCESS,
    .classifier = classifier,
    .pFuncs = nodesMakeList()
  };
  if (NULL == cxt.pFuncs) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  nodesWalkSelectStmt(pSelect, SQL_CLAUSE_GROUP_BY, collectFuncs, &cxt);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyList(cxt.pFuncs);
    return cxt.errCode;
  }
  if (LIST_LENGTH(cxt.pFuncs) > 0) {
    *pFuncs = cxt.pFuncs;
  } else {
    nodesDestroyList(cxt.pFuncs);
    *pFuncs = NULL;
  }
  
  return TSDB_CODE_SUCCESS;
}
