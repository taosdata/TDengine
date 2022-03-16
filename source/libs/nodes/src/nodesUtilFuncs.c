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

#include "cmdnodes.h"
#include "nodesUtil.h"
#include "plannodes.h"
#include "querynodes.h"
#include "taos.h"
#include "taoserror.h"
#include "thash.h"

static SNode* makeNode(ENodeType type, size_t size) {
  SNode* p = calloc(1, size);
  if (NULL == p) {
    return NULL;
  }
  setNodeType(p, type);
  return p;
}

SNodeptr nodesMakeNode(ENodeType type) {
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
    case QUERY_NODE_TARGET:
      return makeNode(type, sizeof(STargetNode));
    case QUERY_NODE_DATABLOCK_DESC:
      return makeNode(type, sizeof(SDataBlockDescNode));
    case QUERY_NODE_SLOT_DESC:
      return makeNode(type, sizeof(SSlotDescNode));
    case QUERY_NODE_COLUMN_DEF:
      return makeNode(type, sizeof(SColumnDefNode));
    case QUERY_NODE_DOWNSTREAM_SOURCE:
      return makeNode(type, sizeof(SDownstreamSourceNode));
    case QUERY_NODE_DATABASE_OPTIONS:
      return makeNode(type, sizeof(SDatabaseOptions));
    case QUERY_NODE_TABLE_OPTIONS:
      return makeNode(type, sizeof(STableOptions));
    case QUERY_NODE_INDEX_OPTIONS:
      return makeNode(type, sizeof(SIndexOptions));
    case QUERY_NODE_SET_OPERATOR:
      return makeNode(type, sizeof(SSetOperator));
    case QUERY_NODE_SELECT_STMT:
      return makeNode(type, sizeof(SSelectStmt));
    case QUERY_NODE_VNODE_MODIF_STMT:
      return makeNode(type, sizeof(SVnodeModifOpStmt));
    case QUERY_NODE_CREATE_DATABASE_STMT:
      return makeNode(type, sizeof(SCreateDatabaseStmt));
    case QUERY_NODE_DROP_DATABASE_STMT:
      return makeNode(type, sizeof(SDropDatabaseStmt));
    case QUERY_NODE_ALTER_DATABASE_STMT:
      return makeNode(type, sizeof(SAlterDatabaseStmt));
    case QUERY_NODE_SHOW_DATABASES_STMT:
      return makeNode(type, sizeof(SShowStmt));
    case QUERY_NODE_CREATE_TABLE_STMT:
      return makeNode(type, sizeof(SCreateTableStmt));
    case QUERY_NODE_CREATE_SUBTABLE_CLAUSE:
      return makeNode(type, sizeof(SCreateSubTableClause));
    case QUERY_NODE_CREATE_MULTI_TABLE_STMT:
      return makeNode(type, sizeof(SCreateMultiTableStmt));
    case QUERY_NODE_DROP_TABLE_CLAUSE:
      return makeNode(type, sizeof(SDropTableClause));
    case QUERY_NODE_DROP_TABLE_STMT:
      return makeNode(type, sizeof(SDropTableStmt));
    case QUERY_NODE_DROP_SUPER_TABLE_STMT:
      return makeNode(type, sizeof(SDropSuperTableStmt));
    case QUERY_NODE_SHOW_TABLES_STMT:
    case QUERY_NODE_SHOW_STABLES_STMT:
      return makeNode(type, sizeof(SShowStmt));
    case QUERY_NODE_CREATE_USER_STMT:
      return makeNode(type, sizeof(SCreateUserStmt));
    case QUERY_NODE_ALTER_USER_STMT:
      return makeNode(type, sizeof(SAlterUserStmt));
    case QUERY_NODE_DROP_USER_STMT:
      return makeNode(type, sizeof(SDropUserStmt));
    case QUERY_NODE_SHOW_USERS_STMT:
      return makeNode(type, sizeof(SShowStmt));
    case QUERY_NODE_USE_DATABASE_STMT:
      return makeNode(type, sizeof(SUseDatabaseStmt));
    case QUERY_NODE_CREATE_DNODE_STMT:
      return makeNode(type, sizeof(SCreateDnodeStmt));
    case QUERY_NODE_DROP_DNODE_STMT:
      return makeNode(type, sizeof(SDropDnodeStmt));
    case QUERY_NODE_SHOW_DNODES_STMT:
      return makeNode(type, sizeof(SShowStmt));
    case QUERY_NODE_SHOW_VGROUPS_STMT:
    case QUERY_NODE_SHOW_MNODES_STMT:
    case QUERY_NODE_SHOW_QNODES_STMT:
      return makeNode(type, sizeof(SShowStmt));
    case QUERY_NODE_CREATE_INDEX_STMT:
      return makeNode(type, sizeof(SCreateIndexStmt));
    case QUERY_NODE_DROP_INDEX_STMT:
      return makeNode(type, sizeof(SDropIndexStmt));
    case QUERY_NODE_CREATE_QNODE_STMT:
      return makeNode(type, sizeof(SCreateQnodeStmt));
    case QUERY_NODE_DROP_QNODE_STMT:
      return makeNode(type, sizeof(SDropQnodeStmt));
    case QUERY_NODE_CREATE_TOPIC_STMT:
      return makeNode(type, sizeof(SCreateTopicStmt));
    case QUERY_NODE_DROP_TOPIC_STMT:
      return makeNode(type, sizeof(SDropTopicStmt));
    case QUERY_NODE_LOGIC_PLAN_SCAN:
      return makeNode(type, sizeof(SScanLogicNode));
    case QUERY_NODE_LOGIC_PLAN_JOIN:
      return makeNode(type, sizeof(SJoinLogicNode));
    case QUERY_NODE_LOGIC_PLAN_AGG:
      return makeNode(type, sizeof(SAggLogicNode));
    case QUERY_NODE_LOGIC_PLAN_PROJECT:
      return makeNode(type, sizeof(SProjectLogicNode));
    case QUERY_NODE_LOGIC_PLAN_VNODE_MODIF:
      return makeNode(type, sizeof(SVnodeModifLogicNode));
    case QUERY_NODE_LOGIC_PLAN_EXCHANGE:
      return makeNode(type, sizeof(SExchangeLogicNode));
    case QUERY_NODE_LOGIC_PLAN_WINDOW:
      return makeNode(type, sizeof(SWindowLogicNode));
    case QUERY_NODE_LOGIC_SUBPLAN:
      return makeNode(type, sizeof(SSubLogicPlan));
    case QUERY_NODE_LOGIC_PLAN:
      return makeNode(type, sizeof(SQueryLogicPlan));
    case QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN:
      return makeNode(type, sizeof(STagScanPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN:
      return makeNode(type, sizeof(STableScanPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN:
      return makeNode(type, sizeof(STableSeqScanPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN:
      return makeNode(type, sizeof(SNode));
    case QUERY_NODE_PHYSICAL_PLAN_PROJECT:
      return makeNode(type, sizeof(SProjectPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_JOIN:
      return makeNode(type, sizeof(SJoinPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_AGG:
      return makeNode(type, sizeof(SAggPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_EXCHANGE:
      return makeNode(type, sizeof(SExchangePhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_SORT:
      return makeNode(type, sizeof(SNode));
    case QUERY_NODE_PHYSICAL_PLAN_INTERVAL:
      return makeNode(type, sizeof(SIntervalPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_DISPATCH:
      return makeNode(type, sizeof(SDataDispatcherNode));
    case QUERY_NODE_PHYSICAL_PLAN_INSERT:
      return makeNode(type, sizeof(SDataInserterNode));
    case QUERY_NODE_PHYSICAL_SUBPLAN:
      return makeNode(type, sizeof(SSubplan));
    case QUERY_NODE_PHYSICAL_PLAN:
      return makeNode(type, sizeof(SQueryPlan));
    default:
      break;
  }
  nodesError("nodesMakeNode unknown node = %s", nodesNodeName(type));
  return NULL;
}

static EDealRes destroyNode(SNode** pNode, void* pContext) {
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
      nodesClearList(((SLogicConditionNode*)(*pNode))->pParameterList);
      break;
    case QUERY_NODE_FUNCTION:
      nodesClearList(((SFunctionNode*)(*pNode))->pParameterList);
      break;
    case QUERY_NODE_REAL_TABLE: {
      SRealTableNode* pReal = (SRealTableNode*)*pNode;
      tfree(pReal->pMeta);
      tfree(pReal->pVgroupList);
      break;
    }
    case QUERY_NODE_TEMP_TABLE:
      nodesDestroyNode(((STempTableNode*)(*pNode))->pSubquery);
      break;
    case QUERY_NODE_GROUPING_SET:
      nodesClearList(((SGroupingSetNode*)(*pNode))->pParameterList);
      break;
    case QUERY_NODE_NODE_LIST:
      nodesClearList(((SNodeListNode*)(*pNode))->pNodeList);
      break;
    case QUERY_NODE_INDEX_OPTIONS: {
      SIndexOptions* pStmt = (SIndexOptions*)*pNode;
      nodesDestroyList(pStmt->pFuncs);
      nodesDestroyNode(pStmt->pInterval);
      nodesDestroyNode(pStmt->pOffset);
      nodesDestroyNode(pStmt->pSliding);
      break;
    }
    case QUERY_NODE_SELECT_STMT: {
      SSelectStmt* pStmt = (SSelectStmt*)*pNode;
      nodesDestroyList(pStmt->pProjectionList);
      nodesDestroyNode(pStmt->pFromTable);
      nodesDestroyNode(pStmt->pWhere);
      nodesDestroyList(pStmt->pPartitionByList);
      nodesDestroyNode(pStmt->pWindow);
      nodesDestroyList(pStmt->pGroupByList);
      nodesDestroyNode(pStmt->pHaving);
      nodesDestroyList(pStmt->pOrderByList);
      nodesDestroyNode(pStmt->pLimit);
      nodesDestroyNode(pStmt->pSlimit);
      break;
    }
    case QUERY_NODE_VNODE_MODIF_STMT: {
      SVnodeModifOpStmt* pStmt = (SVnodeModifOpStmt*)*pNode;
      size_t size = taosArrayGetSize(pStmt->pDataBlocks);
      for (size_t i = 0; i < size; ++i) {
        SVgDataBlocks* pVg = taosArrayGetP(pStmt->pDataBlocks, i);
        tfree(pVg->pData);
        tfree(pVg);
      }
      taosArrayDestroy(pStmt->pDataBlocks);
      break;
    }
    case QUERY_NODE_CREATE_TABLE_STMT: {
      SCreateTableStmt* pStmt = (SCreateTableStmt*)*pNode;
      nodesDestroyList(pStmt->pCols);
      nodesDestroyList(pStmt->pTags);
      break;
    }
    case QUERY_NODE_CREATE_SUBTABLE_CLAUSE: {
      SCreateSubTableClause* pStmt = (SCreateSubTableClause*)*pNode;
      nodesDestroyList(pStmt->pSpecificTags);
      nodesDestroyList(pStmt->pValsOfTags);
      break;
    }
    case QUERY_NODE_CREATE_MULTI_TABLE_STMT:
      nodesDestroyList(((SCreateMultiTableStmt*)(*pNode))->pSubTables);
      break;
    case QUERY_NODE_CREATE_INDEX_STMT: {
      SCreateIndexStmt* pStmt = (SCreateIndexStmt*)*pNode;
      nodesDestroyNode(pStmt->pOptions);
      nodesDestroyList(pStmt->pCols);
      break;
    }
    default:
      break;
  }
  tfree(*pNode);
  return DEAL_RES_CONTINUE;
}

void nodesDestroyNode(SNodeptr pNode) {
  if (NULL == pNode) {
    return;
  }
  nodesRewriteNodePostOrder((SNode**)&pNode, destroyNode, NULL);
}

SNodeList* nodesMakeList() {
  SNodeList* p = calloc(1, sizeof(SNodeList));
  if (NULL == p) {
    return NULL;
  }
  return p;
}

int32_t nodesListAppend(SNodeList* pList, SNodeptr pNode) {
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

int32_t nodesListStrictAppend(SNodeList* pList, SNodeptr pNode) {
  if (NULL == pNode) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  int32_t code = nodesListAppend(pList, pNode);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pNode);
  }
  return code;
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

SNodeptr nodesListGetNode(SNodeList* pList, int32_t index) {
  SNode* node;
  FOREACH(node, pList) {
    if (0 == index--) {
      return node;
    }
  }
  return NULL;
}

void nodesDestroyList(SNodeList* pList) {
  if (NULL == pList) {
    return;
  }

  SListCell* pNext = pList->pHead;
  while (NULL != pNext) {
    pNext = nodesListErase(pList, pNext);
  }
  tfree(pList);
}

void nodesClearList(SNodeList* pList) {
  if (NULL == pList) {
    return;
  }

  SListCell* pNext = pList->pHead;
  while (NULL != pNext) {
    SListCell* tmp = pNext;
    pNext = pNext->pNext;
    tfree(tmp);
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
