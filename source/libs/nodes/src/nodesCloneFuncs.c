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

#define COPY_ALL_SCALAR_FIELDS             \
  do {                                     \
    memcpy((pDst), (pSrc), sizeof(*pSrc)); \
  } while (0)

#define COPY_SCALAR_FIELD(fldname)     \
  do {                                 \
    (pDst)->fldname = (pSrc)->fldname; \
  } while (0)

#define COPY_CHAR_ARRAY_FIELD(fldname)        \
  do {                                        \
    strcpy((pDst)->fldname, (pSrc)->fldname); \
  } while (0)

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

static void dataTypeCopy(const SDataType* pSrc, SDataType* pDst) {
  COPY_SCALAR_FIELD(type);
  COPY_SCALAR_FIELD(precision);
  COPY_SCALAR_FIELD(scale);
  COPY_SCALAR_FIELD(bytes);
}

static void exprNodeCopy(const SExprNode* pSrc, SExprNode* pDst) {
  dataTypeCopy(&pSrc->resType, &pDst->resType);
  COPY_CHAR_ARRAY_FIELD(aliasName);
}

static SNode* columnNodeCopy(const SColumnNode* pSrc, SColumnNode* pDst) {
  exprNodeCopy((const SExprNode*)pSrc, (SExprNode*)pDst);
  COPY_SCALAR_FIELD(colId);
  COPY_SCALAR_FIELD(colType);
  COPY_CHAR_ARRAY_FIELD(dbName);
  COPY_CHAR_ARRAY_FIELD(tableName);
  COPY_CHAR_ARRAY_FIELD(tableAlias);
  COPY_CHAR_ARRAY_FIELD(colName);
  COPY_SCALAR_FIELD(dataBlockId);
  COPY_SCALAR_FIELD(slotId);
  return (SNode*)pDst;
}

static SNode* valueNodeCopy(const SValueNode* pSrc, SValueNode* pDst) {
  COPY_ALL_SCALAR_FIELDS;
  exprNodeCopy((const SExprNode*)pSrc, (SExprNode*)pDst);
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
  exprNodeCopy((const SExprNode*)pSrc, (SExprNode*)pDst);
  COPY_SCALAR_FIELD(opType);
  CLONE_NODE_FIELD(pLeft);
  CLONE_NODE_FIELD(pRight);
  return (SNode*)pDst;
}

static SNode* logicConditionNodeCopy(const SLogicConditionNode* pSrc, SLogicConditionNode* pDst) {
  exprNodeCopy((const SExprNode*)pSrc, (SExprNode*)pDst);
  COPY_SCALAR_FIELD(condType);
  CLONE_NODE_LIST_FIELD(pParameterList);
  return (SNode*)pDst;
}

static SNode* functionNodeCopy(const SFunctionNode* pSrc, SFunctionNode* pDst) {
  exprNodeCopy((const SExprNode*)pSrc, (SExprNode*)pDst);
  COPY_CHAR_ARRAY_FIELD(functionName);
  COPY_SCALAR_FIELD(funcId);
  COPY_SCALAR_FIELD(funcType);
  CLONE_NODE_LIST_FIELD(pParameterList);
  return (SNode*)pDst;
}

static SNode* targetNodeCopy(const STargetNode* pSrc, STargetNode* pDst) {
  COPY_SCALAR_FIELD(dataBlockId);
  COPY_SCALAR_FIELD(slotId);
  CLONE_NODE_FIELD(pExpr);
  return (SNode*)pDst;
}

static SNode* groupingSetNodeCopy(const SGroupingSetNode* pSrc, SGroupingSetNode* pDst) {
  COPY_SCALAR_FIELD(groupingSetType);
  CLONE_NODE_LIST_FIELD(pParameterList);
  return (SNode*)pDst;
}

static SNode* orderByExprNodeCopy(const SOrderByExprNode* pSrc, SOrderByExprNode* pDst) {
  COPY_ALL_SCALAR_FIELDS;
  CLONE_NODE_FIELD(pExpr);
  return (SNode*)pDst;
}

static SNode* nodeListNodeCopy(const SNodeListNode* pSrc, SNodeListNode* pDst) {
  COPY_ALL_SCALAR_FIELDS;
  CLONE_NODE_LIST_FIELD(pNodeList);
  return (SNode*)pDst;
}

static SNode* fillNodeCopy(const SFillNode* pSrc, SFillNode* pDst) {
  COPY_SCALAR_FIELD(mode);
  CLONE_NODE_FIELD(pValues);
  return (SNode*)pDst;
}

static SNode* logicNodeCopy(const SLogicNode* pSrc, SLogicNode* pDst) {
  CLONE_NODE_LIST_FIELD(pTargets);
  CLONE_NODE_FIELD(pConditions);
  CLONE_NODE_LIST_FIELD(pChildren);
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

static SNode* logicScanCopy(const SScanLogicNode* pSrc, SScanLogicNode* pDst) {
  COPY_ALL_SCALAR_FIELDS;
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  CLONE_NODE_LIST_FIELD(pScanCols);
  CLONE_NODE_LIST_FIELD(pScanPseudoCols);
  CLONE_OBJECT_FIELD(pMeta, tableMetaClone);
  CLONE_OBJECT_FIELD(pVgroupList, vgroupsInfoClone);
  CLONE_NODE_LIST_FIELD(pDynamicScanFuncs);
  return (SNode*)pDst;
}

static SNode* logicJoinCopy(const SJoinLogicNode* pSrc, SJoinLogicNode* pDst) {
  COPY_ALL_SCALAR_FIELDS;
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
  COPY_ALL_SCALAR_FIELDS;
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  CLONE_NODE_LIST_FIELD(pProjections);
  return (SNode*)pDst;
}

static SNode* logicVnodeModifCopy(const SVnodeModifLogicNode* pSrc, SVnodeModifLogicNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  COPY_SCALAR_FIELD(msgType);
  return (SNode*)pDst;
}

static SNode* logicExchangeCopy(const SExchangeLogicNode* pSrc, SExchangeLogicNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  COPY_SCALAR_FIELD(srcGroupId);
  return (SNode*)pDst;
}

static SNode* logicWindowCopy(const SWindowLogicNode* pSrc, SWindowLogicNode* pDst) {
  COPY_ALL_SCALAR_FIELDS;
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  CLONE_NODE_LIST_FIELD(pFuncs);
  CLONE_NODE_FIELD(pFill);
  CLONE_NODE_FIELD(pTspk);
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
  COPY_SCALAR_FIELD(subplanType);
  return (SNode*)pDst;
}

static SNode* dataBlockDescCopy(const SDataBlockDescNode* pSrc, SDataBlockDescNode* pDst) {
  COPY_ALL_SCALAR_FIELDS;
  CLONE_NODE_LIST_FIELD(pSlots);
  return (SNode*)pDst;
}

static SNode* slotDescCopy(const SSlotDescNode* pSrc, SSlotDescNode* pDst) {
  COPY_SCALAR_FIELD(slotId);
  dataTypeCopy(&pSrc->dataType, &pDst->dataType);
  COPY_SCALAR_FIELD(reserve);
  COPY_SCALAR_FIELD(output);
  COPY_SCALAR_FIELD(tag);
  return (SNode*)pDst;
}

static SNode* downstreamSourceCopy(const SDownstreamSourceNode* pSrc, SDownstreamSourceNode* pDst) {
  COPY_SCALAR_FIELD(addr);
  COPY_SCALAR_FIELD(taskId);
  COPY_SCALAR_FIELD(schedId);
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
    case QUERY_NODE_TARGET:
      return targetNodeCopy((const STargetNode*)pNode, (STargetNode*)pDst);
    case QUERY_NODE_REAL_TABLE:
    case QUERY_NODE_TEMP_TABLE:
    case QUERY_NODE_JOIN_TABLE:
      break;
    case QUERY_NODE_GROUPING_SET:
      return groupingSetNodeCopy((const SGroupingSetNode*)pNode, (SGroupingSetNode*)pDst);
    case QUERY_NODE_ORDER_BY_EXPR:
      return orderByExprNodeCopy((const SOrderByExprNode*)pNode, (SOrderByExprNode*)pDst);
    case QUERY_NODE_LIMIT:
      break;
    case QUERY_NODE_NODE_LIST:
      return nodeListNodeCopy((const SNodeListNode*)pNode, (SNodeListNode*)pDst);
    case QUERY_NODE_FILL:
      return fillNodeCopy((const SFillNode*)pNode, (SFillNode*)pDst);
    case QUERY_NODE_DATABLOCK_DESC:
      return dataBlockDescCopy((const SDataBlockDescNode*)pNode, (SDataBlockDescNode*)pDst);
    case QUERY_NODE_SLOT_DESC:
      return slotDescCopy((const SSlotDescNode*)pNode, (SSlotDescNode*)pDst);
    case QUERY_NODE_DOWNSTREAM_SOURCE:
      return downstreamSourceCopy((const SDownstreamSourceNode*)pNode, (SDownstreamSourceNode*)pDst);
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
