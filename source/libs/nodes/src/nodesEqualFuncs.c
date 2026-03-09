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

#include "functionMgt.h"
#include "querynodes.h"

static bool nodeListNodeEqual(const SNodeListNode* a, const SNodeListNode* b);

#define COMPARE_SCALAR_FIELD(fldname)           \
  do {                                          \
    if (a->fldname != b->fldname) return false; \
  } while (0)

#define COMPARE_STRING(a, b) (((a) != NULL && (b) != NULL) ? (strcmp((a), (b)) == 0) : (a) == (b))

#define COMPARE_VARDATA(a, b)                                                                                   \
  (((a) != NULL && (b) != NULL)                                                                                 \
       ? (varDataLen((a)) == varDataLen((b)) && memcmp(varDataVal((a)), varDataVal((b)), varDataLen((a))) == 0) \
       : (a) == (b))
#define COMPARE_BLOBDATA(a, b)                                                                                       \
  (((a) != NULL && (b) != NULL)                                                                                      \
       ? (blobDataLen((a)) == blobDataLen((b)) && memcmp(blobDataVal((a)), blobDataVal((b)), blobDataLen((a))) == 0) \
       : (a) == (b))
#define COMPARE_STRING_FIELD(fldname)                          \
  do {                                                         \
    if (!COMPARE_STRING(a->fldname, b->fldname)) return false; \
  } while (0)

#define COMPARE_VARDATA_FIELD(fldname)                          \
  do {                                                          \
    if (!COMPARE_VARDATA(a->fldname, b->fldname)) return false; \
  } while (0)

#define COMPARE_BLOBDATA_FIELD(fldname)                          \
  do {                                                           \
    if (!COMPARE_BLOBDATA(a->fldname, b->fldname)) return false; \
  } while (0)

#define COMPARE_OBJECT_FIELD(fldname, equalFunc)          \
  do {                                                    \
    if (!equalFunc(a->fldname, b->fldname)) return false; \
  } while (0)

#define COMPARE_NODE_FIELD(fldname)                            \
  do {                                                         \
    if (!nodesEqualNode(a->fldname, b->fldname)) return false; \
  } while (0)

#define COMPARE_NODE_LIST_FIELD(fldname)                          \
  do {                                                            \
    if (!nodeNodeListEqual(a->fldname, b->fldname)) return false; \
  } while (0)

static bool nodeNodeListEqual(const SNodeList* a, const SNodeList* b) {
  if (a == b) {
    return true;
  }

  if (NULL == a || NULL == b) {
    return false;
  }

  if (LIST_LENGTH(a) != LIST_LENGTH(b)) {
    return false;
  }

  SNode *na, *nb;
  FORBOTH(na, a, nb, b) {
    if (!nodesEqualNode(na, nb)) {
      return false;
    }
  }
  return true;
}

static bool dataTypeEqual(SDataType a, SDataType b) {
  return a.type == b.type && a.bytes == b.bytes && a.precision == b.precision && a.scale == b.scale;
}

static bool columnNodeEqual(const SColumnNode* a, const SColumnNode* b) {
  COMPARE_STRING_FIELD(dbName);
  COMPARE_STRING_FIELD(tableName);
  COMPARE_STRING_FIELD(colName);
  COMPARE_STRING_FIELD(tableAlias);
  return true;
}

static bool valueNodeEqual(const SValueNode* a, const SValueNode* b) {
  COMPARE_OBJECT_FIELD(node.resType, dataTypeEqual);
  COMPARE_STRING_FIELD(literal);
  switch (a->node.resType.type) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT:
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE:
    case TSDB_DATA_TYPE_TIMESTAMP:
      COMPARE_SCALAR_FIELD(typeData);
      break;
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_GEOMETRY:
      COMPARE_VARDATA_FIELD(datum.p);
      break;
    case TSDB_DATA_TYPE_JSON:
      return false;
    case TSDB_DATA_TYPE_DECIMAL:
      return false;
    case TSDB_DATA_TYPE_BLOB:
      COMPARE_BLOBDATA_FIELD(datum.p);
    default:
      break;
  }
  return true;
}

static bool operatorNodeEqual(const SOperatorNode* a, const SOperatorNode* b) {
  COMPARE_SCALAR_FIELD(opType);
  COMPARE_SCALAR_FIELD(flag);
  COMPARE_NODE_FIELD(pLeft);
  COMPARE_NODE_FIELD(pRight);
  return true;
}

static bool logicConditionNodeEqual(const SLogicConditionNode* a, const SLogicConditionNode* b) {
  COMPARE_SCALAR_FIELD(condType);
  COMPARE_NODE_LIST_FIELD(pParameterList);
  return true;
}

static bool functionNodeEqual(const SFunctionNode* a, const SFunctionNode* b) {
  COMPARE_SCALAR_FIELD(funcId);
  COMPARE_STRING_FIELD(functionName);
  COMPARE_NODE_LIST_FIELD(pParameterList);
  if (a->funcType == FUNCTION_TYPE_SELECT_VALUE) {
    if ((a->node.relatedTo != b->node.relatedTo)) return false;
  } else {
    // select cols(cols(first(c0), ts),  first(c0) from meters;
    if ((a->node.bindExprID != b->node.bindExprID)) {
      return false;
    }
  }

  return true;
}

static bool whenThenNodeEqual(const SWhenThenNode* a, const SWhenThenNode* b) {
  COMPARE_NODE_FIELD(pWhen);
  COMPARE_NODE_FIELD(pThen);
  return true;
}

static bool caseWhenNodeEqual(const SCaseWhenNode* a, const SCaseWhenNode* b) {
  COMPARE_NODE_FIELD(pCase);
  COMPARE_NODE_FIELD(pElse);
  COMPARE_NODE_LIST_FIELD(pWhenThenList);
  return true;
}

static bool groupingSetNodeEqual(const SGroupingSetNode* a, const SGroupingSetNode* b) {
  COMPARE_SCALAR_FIELD(groupingSetType);
  COMPARE_NODE_LIST_FIELD(pParameterList);
  return true;
}

static bool remoteValueNodeEqual(const SRemoteValueNode* a, const SRemoteValueNode* b) {
  COMPARE_SCALAR_FIELD(subQIdx);
  return valueNodeEqual(&a->val, &b->val);
}

static bool remoteValueNodeListEqual(const SRemoteValueListNode* a, const SRemoteValueListNode* b) {
  COMPARE_OBJECT_FIELD(node.resType, dataTypeEqual);
  COMPARE_SCALAR_FIELD(subQIdx);
  return true;
}

static bool remoteRowNodeEqual(const SRemoteRowNode* a, const SRemoteRowNode* b) {
  COMPARE_SCALAR_FIELD(valSet);
  COMPARE_SCALAR_FIELD(hasValue);
  COMPARE_SCALAR_FIELD(hasNull);
  COMPARE_SCALAR_FIELD(subQIdx);
  return valueNodeEqual(&a->val, &b->val);
}

static bool remoteZeroRowsNodeEqual(const SRemoteZeroRowsNode* a, const SRemoteZeroRowsNode* b) {
  return remoteValueNodeEqual((const SRemoteValueNode*)a, (const SRemoteValueNode*)b);
}

static bool nodeListNodeEqual(const SNodeListNode* a, const SNodeListNode* b) {
  if (LIST_LENGTH(a->pNodeList) != LIST_LENGTH(b->pNodeList)) {
    return false;
  }

  COMPARE_NODE_LIST_FIELD(pNodeList);
  return true;
}


bool nodesEqualNode(const SNode* a, const SNode* b) {
  if (a == b) {
    return true;
  }

  if (NULL == a || NULL == b) {
    return false;
  }

  if (nodeType(a) != nodeType(b)) {
    return false;
  }

  switch (nodeType(a)) {
    case QUERY_NODE_COLUMN:
      return columnNodeEqual((const SColumnNode*)a, (const SColumnNode*)b);
    case QUERY_NODE_VALUE:
      return valueNodeEqual((const SValueNode*)a, (const SValueNode*)b);
    case QUERY_NODE_OPERATOR:
      return operatorNodeEqual((const SOperatorNode*)a, (const SOperatorNode*)b);
    case QUERY_NODE_LOGIC_CONDITION:
      return logicConditionNodeEqual((const SLogicConditionNode*)a, (const SLogicConditionNode*)b);
    case QUERY_NODE_FUNCTION:
      return functionNodeEqual((const SFunctionNode*)a, (const SFunctionNode*)b);
    case QUERY_NODE_WHEN_THEN:
      return whenThenNodeEqual((const SWhenThenNode*)a, (const SWhenThenNode*)b);
    case QUERY_NODE_CASE_WHEN:
      return caseWhenNodeEqual((const SCaseWhenNode*)a, (const SCaseWhenNode*)b);
    case QUERY_NODE_GROUPING_SET:
      return groupingSetNodeEqual((const SGroupingSetNode*)a, (const SGroupingSetNode*)b);
    case QUERY_NODE_NODE_LIST:
      return nodeListNodeEqual((const SNodeListNode*)a, (const SNodeListNode*)b);
    case QUERY_NODE_REAL_TABLE:
    case QUERY_NODE_VIRTUAL_TABLE:
    case QUERY_NODE_TEMP_TABLE:
    case QUERY_NODE_JOIN_TABLE:
    case QUERY_NODE_ORDER_BY_EXPR:
    case QUERY_NODE_LIMIT:
      return false;
    case QUERY_NODE_REMOTE_VALUE:
      return remoteValueNodeEqual((const SRemoteValueNode*)a, (const SRemoteValueNode*)b);
    case QUERY_NODE_REMOTE_VALUE_LIST:
      return remoteValueNodeListEqual((const SRemoteValueListNode*)a, (const SRemoteValueListNode*)b);
    case QUERY_NODE_REMOTE_ROW:
      return remoteRowNodeEqual((const SRemoteRowNode*)a, (const SRemoteRowNode*)b);
    case QUERY_NODE_REMOTE_ZERO_ROWS:
      return remoteZeroRowsNodeEqual((const SRemoteZeroRowsNode*)a, (const SRemoteZeroRowsNode*)b);
    default:
      break;
  }

  return false;
}
