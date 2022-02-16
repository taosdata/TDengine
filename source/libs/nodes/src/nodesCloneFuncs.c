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
#include "taos.h"

#define COPY_SCALAR_FIELD(fldname) \
	do { \
    (pDst)->fldname = (pSrc)->fldname; \
	} while (0)

#define COPY_CHAR_ARRAY_FIELD(fldname) \
	do { \
    strcpy((pDst)->fldname, (pSrc)->fldname); \
	} while (0)

#define COPY_CHAR_POINT_FIELD(fldname) \
	do { \
    (pDst)->fldname = strdup((pSrc)->fldname); \
	} while (0)

#define COPY_NODE_FIELD(fldname) \
	do { \
    (pDst)->fldname = nodesCloneNode((pSrc)->fldname); \
    if (NULL == (pDst)->fldname) { \
      nodesDestroyNode((SNode*)(pDst)); \
      return NULL; \
    } \
	} while (0)

#define COPY_NODE_LIST_FIELD(fldname) \
	do { \
    (pDst)->fldname = nodesCloneList((pSrc)->fldname); \
    if (NULL == (pDst)->fldname) { \
      nodesDestroyNode((SNode*)(pDst)); \
      return NULL; \
    } \
	} while (0)

static void dataTypeCopy(const SDataType* pSrc, SDataType* pDst) {
  COPY_SCALAR_FIELD(type);
  COPY_SCALAR_FIELD(precision);
  COPY_SCALAR_FIELD(scale);
  COPY_SCALAR_FIELD(bytes);
}

static void exprNodeCopy(const SExprNode* pSrc, SExprNode* pDst) {
  COPY_SCALAR_FIELD(type);
  dataTypeCopy(&pSrc->resType, &pDst->resType);
  COPY_CHAR_ARRAY_FIELD(aliasName);
  // COPY_NODE_LIST_FIELD(pAssociationList);
}

static SNode* columnNodeCopy(const SColumnNode* pSrc, SColumnNode* pDst) {
  exprNodeCopy((const SExprNode*)&pSrc, (SExprNode*)&pDst);
  COPY_SCALAR_FIELD(colId);
  COPY_SCALAR_FIELD(colType);
  COPY_CHAR_ARRAY_FIELD(dbName);
  COPY_CHAR_ARRAY_FIELD(tableName);
  COPY_CHAR_ARRAY_FIELD(tableAlias);
  COPY_CHAR_ARRAY_FIELD(colName);
  // COPY_NODE_FIELD(pProjectRef);
  return (SNode*)pDst;
}

static SNode* valueNodeCopy(const SValueNode* pSrc, SValueNode* pDst) {
  exprNodeCopy((const SExprNode*)&pSrc, (SExprNode*)&pDst);
  COPY_CHAR_POINT_FIELD(literal);
  COPY_SCALAR_FIELD(isDuration);
  switch (pSrc->node.resType.type) {
    case TSDB_DATA_TYPE_NULL:
      break;
    case TSDB_DATA_TYPE_BOOL:
      COPY_SCALAR_FIELD(datum.b);
      break;
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      COPY_SCALAR_FIELD(datum.i);
      break;
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT:
      COPY_SCALAR_FIELD(datum.u);
      break;
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE:
      COPY_SCALAR_FIELD(datum.d);
      break;
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY:
      COPY_CHAR_POINT_FIELD(datum.p);
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
  exprNodeCopy((const SExprNode*)&pSrc, (SExprNode*)&pDst);
  COPY_SCALAR_FIELD(opType);
  COPY_NODE_FIELD(pLeft);
  COPY_NODE_FIELD(pRight);
  return (SNode*)pDst;
}

static SNode* logicConditionNodeCopy(const SLogicConditionNode* pSrc, SLogicConditionNode* pDst) {
  exprNodeCopy((const SExprNode*)&pSrc, (SExprNode*)&pDst);
  COPY_SCALAR_FIELD(condType);
  COPY_NODE_LIST_FIELD(pParameterList);
  return (SNode*)pDst;
}

static SNode* functionNodeCopy(const SFunctionNode* pSrc, SFunctionNode* pDst) {
  exprNodeCopy((const SExprNode*)&pSrc, (SExprNode*)&pDst);
  COPY_CHAR_ARRAY_FIELD(functionName);
  COPY_SCALAR_FIELD(funcId);
  COPY_SCALAR_FIELD(funcType);
  COPY_NODE_LIST_FIELD(pParameterList);
  return (SNode*)pDst;
}

SNode* nodesCloneNode(const SNode* pNode) {
  if (NULL == pNode) {
    return NULL;
  }
  SNode* pDst = nodesMakeNode(nodeType(pNode));
  if (NULL == pDst) {
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
    case QUERY_NODE_REAL_TABLE:
    case QUERY_NODE_TEMP_TABLE:
    case QUERY_NODE_JOIN_TABLE:
    case QUERY_NODE_GROUPING_SET:
    case QUERY_NODE_ORDER_BY_EXPR:
    case QUERY_NODE_LIMIT:
    default:
      break;
  }
  return pDst;
}

SNodeList* nodesCloneList(const SNodeList* pList) {
  SNodeList* pDst = nodesMakeList();
  if (NULL == pDst) {
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
