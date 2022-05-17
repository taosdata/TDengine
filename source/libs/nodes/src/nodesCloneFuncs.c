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

#include "nodesUtil.h"
#include "plannodes.h"
#include "querynodes.h"
#include "taos.h"
#include "taoserror.h"

#define COPY_CHAR_POINT_FIELD(fldname)         \
  do {                                         \
    if (NULL == (pSrc)->fldname) {             \
      break;                                   \
    }                                          \
    (pDst)->fldname = strdup((pSrc)->fldname); \
  } while (0)

#define CLONE_NODE_FIELD(fldname)                      \
  do {                                                 \
    if (NULL == (pSrc)->fldname) {                     \
      break;                                           \
    }                                                  \
    (pDst)->fldname = nodesCloneNode((pSrc)->fldname); \
    if (NULL == (pDst)->fldname) {                     \
      nodesDestroyNode((SNode*)(pDst));                \
      return NULL;                                     \
    }                                                  \
  } while (0)

#define CLONE_NODE_LIST_FIELD(fldname)                 \
  do {                                                 \
    if (NULL == (pSrc)->fldname) {                     \
      break;                                           \
    }                                                  \
    (pDst)->fldname = nodesCloneList((pSrc)->fldname); \
    if (NULL == (pDst)->fldname) {                     \
      nodesDestroyNode((SNode*)(pDst));                \
      return NULL;                                     \
    }                                                  \
  } while (0)

#define CLONE_OBJECT_FIELD(fldname, cloneFunc)    \
  do {                                            \
    if (NULL == (pSrc)->fldname) {                \
      break;                                      \
    }                                             \
    (pDst)->fldname = cloneFunc((pSrc)->fldname); \
    if (NULL == (pDst)->fldname) {                \
      nodesDestroyNode((SNode*)(pDst));           \
      return NULL;                                \
    }                                             \
  } while (0)

#define COPY_BASE_OBJECT_FIELD(fldname, copyFunc)                   \
  do {                                                              \
    if (NULL == copyFunc(&((pSrc)->fldname), &((pDst)->fldname))) { \
      return NULL;                                                  \
    }                                                               \
  } while (0)

static void dataTypeCopy(const SDataType* pSrc, SDataType* pDst) {}

static SNode* exprNodeCopy(const SExprNode* pSrc, SExprNode* pDst) {
  dataTypeCopy(&pSrc->resType, &pDst->resType);
  pDst->pAssociation = NULL;
  return (SNode*)pDst;
}

static SNode* columnNodeCopy(const SColumnNode* pSrc, SColumnNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, exprNodeCopy);
  pDst->pProjectRef = NULL;
  return (SNode*)pDst;
}

static SNode* valueNodeCopy(const SValueNode* pSrc, SValueNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, exprNodeCopy);
  COPY_CHAR_POINT_FIELD(literal);
  if (!pSrc->translate) {
    return (SNode*)pDst;
  }
  switch (pSrc->node.resType.type) {
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY:
      pDst->datum.p = taosMemoryMalloc(pSrc->node.resType.bytes + VARSTR_HEADER_SIZE + 1);
      if (NULL == pDst->datum.p) {
        nodesDestroyNode(pDst);
        return NULL;
      }
      memcpy(pDst->datum.p, pSrc->datum.p, pSrc->node.resType.bytes + VARSTR_HEADER_SIZE + 1);
      break;
    case TSDB_DATA_TYPE_JSON:
    case TSDB_DATA_TYPE_DECIMAL:
    case TSDB_DATA_TYPE_BLOB:
      // todo
    default:
      break;
  }
  return (SNode*)pDst;
}

static SNode* operatorNodeCopy(const SOperatorNode* pSrc, SOperatorNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, exprNodeCopy);
  CLONE_NODE_FIELD(pLeft);
  CLONE_NODE_FIELD(pRight);
  return (SNode*)pDst;
}

static SNode* logicConditionNodeCopy(const SLogicConditionNode* pSrc, SLogicConditionNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, exprNodeCopy);
  CLONE_NODE_LIST_FIELD(pParameterList);
  return (SNode*)pDst;
}

static SNode* functionNodeCopy(const SFunctionNode* pSrc, SFunctionNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, exprNodeCopy);
  CLONE_NODE_LIST_FIELD(pParameterList);
  return (SNode*)pDst;
}

static SNode* tableNodeCopy(const STableNode* pSrc, STableNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, exprNodeCopy);
  return (SNode*)pDst;
}

static STableMeta* tableMetaClone(const STableMeta* pSrc) {
  int32_t     len = TABLE_META_SIZE(pSrc);
  STableMeta* pDst = taosMemoryMalloc(len);
  if (NULL == pDst) {
    return NULL;
  }
  memcpy(pDst, pSrc, len);
  return pDst;
}

static SVgroupsInfo* vgroupsInfoClone(const SVgroupsInfo* pSrc) {
  int32_t       len = VGROUPS_INFO_SIZE(pSrc);
  SVgroupsInfo* pDst = taosMemoryMalloc(len);
  if (NULL == pDst) {
    return NULL;
  }
  memcpy(pDst, pSrc, len);
  return pDst;
}

static SNode* realTableNodeCopy(const SRealTableNode* pSrc, SRealTableNode* pDst) {
  COPY_BASE_OBJECT_FIELD(table, tableNodeCopy);
  CLONE_OBJECT_FIELD(pMeta, tableMetaClone);
  CLONE_OBJECT_FIELD(pVgroupList, vgroupsInfoClone);
  return (SNode*)pDst;
}

static SNode* tempTableNodeCopy(const STempTableNode* pSrc, STempTableNode* pDst) {
  COPY_BASE_OBJECT_FIELD(table, tableNodeCopy);
  CLONE_NODE_FIELD(pSubquery);
  return (SNode*)pDst;
}

static SNode* joinTableNodeCopy(const SJoinTableNode* pSrc, SJoinTableNode* pDst) {
  COPY_BASE_OBJECT_FIELD(table, tableNodeCopy);
  CLONE_NODE_FIELD(pLeft);
  CLONE_NODE_FIELD(pRight);
  CLONE_NODE_FIELD(pOnCond);
  return (SNode*)pDst;
}

static SNode* targetNodeCopy(const STargetNode* pSrc, STargetNode* pDst) {
  CLONE_NODE_FIELD(pExpr);
  return (SNode*)pDst;
}

static SNode* groupingSetNodeCopy(const SGroupingSetNode* pSrc, SGroupingSetNode* pDst) {
  CLONE_NODE_LIST_FIELD(pParameterList);
  return (SNode*)pDst;
}

static SNode* orderByExprNodeCopy(const SOrderByExprNode* pSrc, SOrderByExprNode* pDst) {
  CLONE_NODE_FIELD(pExpr);
  return (SNode*)pDst;
}

static SNode* limitNodeCopy(const SLimitNode* pSrc, SLimitNode* pDst) { return (SNode*)pDst; }

static SNode* stateWindowNodeCopy(const SStateWindowNode* pSrc, SStateWindowNode* pDst) {
  CLONE_NODE_FIELD(pCol);
  CLONE_NODE_FIELD(pExpr);
  return (SNode*)pDst;
}

static SNode* sessionWindowNodeCopy(const SSessionWindowNode* pSrc, SSessionWindowNode* pDst) {
  CLONE_NODE_FIELD(pCol);
  CLONE_NODE_FIELD(pGap);
  return (SNode*)pDst;
}

static SNode* intervalWindowNodeCopy(const SIntervalWindowNode* pSrc, SIntervalWindowNode* pDst) {
  CLONE_NODE_FIELD(pCol);
  CLONE_NODE_FIELD(pInterval);
  CLONE_NODE_FIELD(pOffset);
  CLONE_NODE_FIELD(pSliding);
  CLONE_NODE_FIELD(pFill);
  return (SNode*)pDst;
}

static SNode* nodeListNodeCopy(const SNodeListNode* pSrc, SNodeListNode* pDst) {
  CLONE_NODE_LIST_FIELD(pNodeList);
  return (SNode*)pDst;
}

static SNode* fillNodeCopy(const SFillNode* pSrc, SFillNode* pDst) {
  CLONE_NODE_FIELD(pValues);
  CLONE_NODE_FIELD(pWStartTs);
  return (SNode*)pDst;
}

static SNode* logicNodeCopy(const SLogicNode* pSrc, SLogicNode* pDst) {
  CLONE_NODE_LIST_FIELD(pTargets);
  CLONE_NODE_FIELD(pConditions);
  CLONE_NODE_LIST_FIELD(pChildren);
  pDst->pParent = NULL;
  return (SNode*)pDst;
}

static SNode* logicScanCopy(const SScanLogicNode* pSrc, SScanLogicNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  CLONE_NODE_LIST_FIELD(pScanCols);
  CLONE_NODE_LIST_FIELD(pScanPseudoCols);
  CLONE_OBJECT_FIELD(pMeta, tableMetaClone);
  CLONE_OBJECT_FIELD(pVgroupList, vgroupsInfoClone);
  CLONE_NODE_LIST_FIELD(pDynamicScanFuncs);
  return (SNode*)pDst;
}

static SNode* logicJoinCopy(const SJoinLogicNode* pSrc, SJoinLogicNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  CLONE_NODE_FIELD(pOnConditions);
  return (SNode*)pDst;
}

static SNode* logicAggCopy(const SAggLogicNode* pSrc, SAggLogicNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  CLONE_NODE_LIST_FIELD(pGroupKeys);
  CLONE_NODE_LIST_FIELD(pAggFuncs);
  return (SNode*)pDst;
}

static SNode* logicProjectCopy(const SProjectLogicNode* pSrc, SProjectLogicNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  CLONE_NODE_LIST_FIELD(pProjections);
  return (SNode*)pDst;
}

static SNode* logicVnodeModifCopy(const SVnodeModifLogicNode* pSrc, SVnodeModifLogicNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  pDst->pDataBlocks = NULL;
  pDst->pVgDataBlocks = NULL;
  return (SNode*)pDst;
}

static SNode* logicExchangeCopy(const SExchangeLogicNode* pSrc, SExchangeLogicNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  return (SNode*)pDst;
}

static SNode* logicWindowCopy(const SWindowLogicNode* pSrc, SWindowLogicNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  CLONE_NODE_LIST_FIELD(pFuncs);
  CLONE_NODE_FIELD(pTspk);
  return (SNode*)pDst;
}

static SNode* logicFillCopy(const SFillLogicNode* pSrc, SFillLogicNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  CLONE_NODE_FIELD(pWStartTs);
  CLONE_NODE_FIELD(pValues);
  return (SNode*)pDst;
}

static SNode* logicSortCopy(const SSortLogicNode* pSrc, SSortLogicNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  CLONE_NODE_LIST_FIELD(pSortKeys);
  return (SNode*)pDst;
}

static SNode* logicPartitionCopy(const SPartitionLogicNode* pSrc, SPartitionLogicNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  CLONE_NODE_LIST_FIELD(pPartitionKeys);
  return (SNode*)pDst;
}

static SNode* logicSubplanCopy(const SLogicSubplan* pSrc, SLogicSubplan* pDst) {
  CLONE_NODE_FIELD(pNode);
  pDst->pChildren = NULL;
  pDst->pParents = NULL;
  pDst->pVgroupList = NULL;
  return (SNode*)pDst;
}

static SNode* dataBlockDescCopy(const SDataBlockDescNode* pSrc, SDataBlockDescNode* pDst) {
  CLONE_NODE_LIST_FIELD(pSlots);
  return (SNode*)pDst;
}

static SNode* slotDescCopy(const SSlotDescNode* pSrc, SSlotDescNode* pDst) {
  dataTypeCopy(&pSrc->dataType, &pDst->dataType);
  return (SNode*)pDst;
}

static SNode* downstreamSourceCopy(const SDownstreamSourceNode* pSrc, SDownstreamSourceNode* pDst) {
  return (SNode*)pDst;
}

static SNode* selectStmtCopy(const SSelectStmt* pSrc, SSelectStmt* pDst) {
  CLONE_NODE_LIST_FIELD(pProjectionList);
  CLONE_NODE_FIELD(pFromTable);
  CLONE_NODE_FIELD(pWhere);
  CLONE_NODE_LIST_FIELD(pPartitionByList);
  CLONE_NODE_FIELD(pWindow);
  CLONE_NODE_LIST_FIELD(pGroupByList);
  CLONE_NODE_FIELD(pHaving);
  CLONE_NODE_LIST_FIELD(pOrderByList);
  CLONE_NODE_FIELD(pLimit);
  CLONE_NODE_FIELD(pLimit);
  return (SNode*)pDst;
}

SNodeptr nodesCloneNode(const SNodeptr pNode) {
  if (NULL == pNode) {
    return NULL;
  }
  SNode* pDst = nodesMakeNode(nodeType(pNode));
  if (NULL == pDst) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  memcpy(pDst, pNode, nodesNodeSize(nodeType(pNode)));
  switch (nodeType(pNode)) {
    case QUERY_NODE_COLUMN:
      return columnNodeCopy((const SColumnNode*)pNode, (SColumnNode*)pDst);
    case QUERY_NODE_VALUE:
      return valueNodeCopy((const SValueNode*)pNode, (SValueNode*)pDst);
    case QUERY_NODE_OPERATOR:
      return operatorNodeCopy((const SOperatorNode*)pNode, (SOperatorNode*)pDst);
    case QUERY_NODE_LOGIC_CONDITION:
      return logicConditionNodeCopy((const SLogicConditionNode*)pNode, (SLogicConditionNode*)pDst);
    case QUERY_NODE_FUNCTION:
      return functionNodeCopy((const SFunctionNode*)pNode, (SFunctionNode*)pDst);
    case QUERY_NODE_REAL_TABLE:
      return realTableNodeCopy((const SRealTableNode*)pNode, (SRealTableNode*)pDst);
    case QUERY_NODE_TEMP_TABLE:
      return tempTableNodeCopy((const STempTableNode*)pNode, (STempTableNode*)pDst);
    case QUERY_NODE_JOIN_TABLE:
      return joinTableNodeCopy((const SJoinTableNode*)pNode, (SJoinTableNode*)pDst);
    case QUERY_NODE_GROUPING_SET:
      return groupingSetNodeCopy((const SGroupingSetNode*)pNode, (SGroupingSetNode*)pDst);
    case QUERY_NODE_ORDER_BY_EXPR:
      return orderByExprNodeCopy((const SOrderByExprNode*)pNode, (SOrderByExprNode*)pDst);
    case QUERY_NODE_LIMIT:
      return limitNodeCopy((const SLimitNode*)pNode, (SLimitNode*)pDst);
    case QUERY_NODE_STATE_WINDOW:
      return stateWindowNodeCopy((const SStateWindowNode*)pNode, (SStateWindowNode*)pDst);
    case QUERY_NODE_SESSION_WINDOW:
      return sessionWindowNodeCopy((const SSessionWindowNode*)pNode, (SSessionWindowNode*)pDst);
    case QUERY_NODE_INTERVAL_WINDOW:
      return intervalWindowNodeCopy((const SIntervalWindowNode*)pNode, (SIntervalWindowNode*)pDst);
    case QUERY_NODE_NODE_LIST:
      return nodeListNodeCopy((const SNodeListNode*)pNode, (SNodeListNode*)pDst);
    case QUERY_NODE_FILL:
      return fillNodeCopy((const SFillNode*)pNode, (SFillNode*)pDst);
    case QUERY_NODE_TARGET:
      return targetNodeCopy((const STargetNode*)pNode, (STargetNode*)pDst);
    case QUERY_NODE_DATABLOCK_DESC:
      return dataBlockDescCopy((const SDataBlockDescNode*)pNode, (SDataBlockDescNode*)pDst);
    case QUERY_NODE_SLOT_DESC:
      return slotDescCopy((const SSlotDescNode*)pNode, (SSlotDescNode*)pDst);
    case QUERY_NODE_DOWNSTREAM_SOURCE:
      return downstreamSourceCopy((const SDownstreamSourceNode*)pNode, (SDownstreamSourceNode*)pDst);
    case QUERY_NODE_SELECT_STMT:
      return selectStmtCopy((const SSelectStmt*)pNode, (SSelectStmt*)pDst);
    case QUERY_NODE_LOGIC_PLAN_SCAN:
      return logicScanCopy((const SScanLogicNode*)pNode, (SScanLogicNode*)pDst);
    case QUERY_NODE_LOGIC_PLAN_JOIN:
      return logicJoinCopy((const SJoinLogicNode*)pNode, (SJoinLogicNode*)pDst);
    case QUERY_NODE_LOGIC_PLAN_AGG:
      return logicAggCopy((const SAggLogicNode*)pNode, (SAggLogicNode*)pDst);
    case QUERY_NODE_LOGIC_PLAN_PROJECT:
      return logicProjectCopy((const SProjectLogicNode*)pNode, (SProjectLogicNode*)pDst);
    case QUERY_NODE_LOGIC_PLAN_VNODE_MODIF:
      return logicVnodeModifCopy((const SVnodeModifLogicNode*)pNode, (SVnodeModifLogicNode*)pDst);
    case QUERY_NODE_LOGIC_PLAN_EXCHANGE:
      return logicExchangeCopy((const SExchangeLogicNode*)pNode, (SExchangeLogicNode*)pDst);
    case QUERY_NODE_LOGIC_PLAN_WINDOW:
      return logicWindowCopy((const SWindowLogicNode*)pNode, (SWindowLogicNode*)pDst);
    case QUERY_NODE_LOGIC_PLAN_FILL:
      return logicFillCopy((const SFillLogicNode*)pNode, (SFillLogicNode*)pDst);
    case QUERY_NODE_LOGIC_PLAN_SORT:
      return logicSortCopy((const SSortLogicNode*)pNode, (SSortLogicNode*)pDst);
    case QUERY_NODE_LOGIC_PLAN_PARTITION:
      return logicPartitionCopy((const SPartitionLogicNode*)pNode, (SPartitionLogicNode*)pDst);
    case QUERY_NODE_LOGIC_SUBPLAN:
      return logicSubplanCopy((const SLogicSubplan*)pNode, (SLogicSubplan*)pDst);
    default:
      break;
  }
  nodesDestroyNode(pDst);
  nodesError("nodesCloneNode unknown node = %s", nodesNodeName(nodeType(pNode)));
  return NULL;
}

SNodeList* nodesCloneList(const SNodeList* pList) {
  if (NULL == pList) {
    return NULL;
  }

  SNodeList* pDst = nodesMakeList();
  if (NULL == pDst) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  SNode* pNode;
  FOREACH(pNode, pList) {
    SNode* pNewNode = nodesCloneNode(pNode);
    if (NULL == pNewNode) {
      nodesDestroyList(pDst);
      return NULL;
    }
    nodesListAppend(pDst, pNewNode);
  }
  return pDst;
}
