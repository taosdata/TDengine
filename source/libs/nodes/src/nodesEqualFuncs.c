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

#define COMPARE_SCALAR_FIELD(fldname)           \
  do {                                          \
    if (a->fldname != b->fldname) return false; \
  } while (0)

#define COMPARE_STRING(a, b) (((a) != NULL && (b) != NULL) ? (strcmp((a), (b)) == 0) : (a) == (b))

#define COMPARE_VARDATA(a, b)                                                                                   \
  (((a) != NULL && (b) != NULL)                                                                                 \
       ? (varDataLen((a)) == varDataLen((b)) && memcmp(varDataVal((a)), varDataVal((b)), varDataLen((a))) == 0) \
       : (a) == (b))

#define COMPARE_STRING_FIELD(fldname)                          \
  do {                                                         \
    if (!COMPARE_STRING(a->fldname, b->fldname)) return false; \
  } while (0)

#define COMPARE_VARDATA_FIELD(fldname)                          \
  do {                                                          \
    if (!COMPARE_VARDATA(a->fldname, b->fldname)) return false; \
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
    case TSDB_DATA_TYPE_DECIMAL:
    case TSDB_DATA_TYPE_BLOB:
      return false;
    default:
      break;
  }
  return true;
}

static bool operatorNodeEqual(const SOperatorNode* a, const SOperatorNode* b) {
  COMPARE_SCALAR_FIELD(opType);
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
    case QUERY_NODE_REAL_TABLE:
    case QUERY_NODE_TEMP_TABLE:
    case QUERY_NODE_JOIN_TABLE:
    case QUERY_NODE_GROUPING_SET:
    case QUERY_NODE_ORDER_BY_EXPR:
    case QUERY_NODE_LIMIT:
      return false;
    default:
      break;
  }

  return false;
}

 bool nodeListNodeEqual(const SNodeList* a, const SNode* b) {
  if (NULL == a || NULL == b) {
    return false;
  }

  if (LIST_LENGTH(a) < 1) {
    return false;
  }

  SNode *na;
  FOREACH(na, a) {
    if (nodesEqualNode(na, b)) {
      return true;
    }
  }
  return false;
}
