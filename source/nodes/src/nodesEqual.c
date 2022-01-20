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

#include "nodes.h"

#define COMPARE_SCALAR_FIELD(fldname) \
	do { \
		if (a->fldname != b->fldname) \
			return false; \
	} while (0)

#define COMPARE_STRING(a, b)	\
	(((a) != NULL && (b) != NULL) ? (strcmp(a, b) == 0) : (a) == (b))

#define COMPARE_STRING_FIELD(fldname) \
	do { \
		if (!COMPARE_STRING(a->fldname, b->fldname)) \
			return false; \
	} while (0)

#define COMPARE_NODE_FIELD(fldname) \
	do { \
		if (!nodeEqual(a->fldname, b->fldname)) \
			return false; \
	} while (0)

#define COMPARE_ARRAY_FIELD(fldname) \
	do { \
		if (!nodeArrayEqual(a->fldname, b->fldname)) \
			return false; \
	} while (0)

static bool nodeArrayEqual(const SArray* a, const SArray* b) {
  if (a == b) {
    return true;
  }

  if (NULL == a || NULL == b) {
    return false;
  }

  if (taosArrayGetSize(a) != taosArrayGetSize(b)) {
    return false;
  }

  size_t size = taosArrayGetSize(a);
  for (size_t i = 0; i < size; ++i) {
    if (!nodeEqual((SNode*)taosArrayGetP(a, i), (SNode*)taosArrayGetP(b, i))) {
      return false;
    }
  }
  return true;
}

static bool columnNodeEqual(const SColumnNode* a, const SColumnNode* b) {
  COMPARE_STRING_FIELD(dbName);
  COMPARE_STRING_FIELD(tableName);
  COMPARE_STRING_FIELD(colName);
  return true;
}

static bool valueNodeEqual(const SValueNode* a, const SValueNode* b) {
  COMPARE_STRING_FIELD(literal);
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
  COMPARE_ARRAY_FIELD(pParameterList);
  return true;
}

static bool isNullConditionNodeEqual(const SIsNullCondNode* a, const SIsNullCondNode* b) {
  COMPARE_NODE_FIELD(pExpr);
  COMPARE_SCALAR_FIELD(isNot);
  return true;
}

static bool functionNodeEqual(const SFunctionNode* a, const SFunctionNode* b) {
  COMPARE_SCALAR_FIELD(funcId);
  COMPARE_ARRAY_FIELD(pParameterList);
  return true;
}

bool nodeEqual(const SNode* a, const SNode* b) {
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
    case QUERY_NODE_IS_NULL_CONDITION:
      return isNullConditionNodeEqual((const SIsNullCondNode*)a, (const SIsNullCondNode*)b);
    case QUERY_NODE_FUNCTION:
      return functionNodeEqual((const SFunctionNode*)a, (const SFunctionNode*)b);
    case QUERY_NODE_REAL_TABLE:
    case QUERY_NODE_TEMP_TABLE:
    case QUERY_NODE_JOIN_TABLE:
    case QUERY_NODE_GROUPING_SET:
    case QUERY_NODE_ORDER_BY_EXPR:
      return false; // todo
    default:
      break;
  }

  return false;
}
