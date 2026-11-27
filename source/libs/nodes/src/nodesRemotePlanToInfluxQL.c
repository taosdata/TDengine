/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#include "influxql.h"

#include <math.h>

#include "functionMgt.h"
#include "nodes.h"
#include "taoserror.h"
#include "tstrbuild.h"
#include "tconv.h"
#include "tvariant.h"

// The renderer accepts a single InfluxQL statement. Planner decisions and
// connection/RP discovery remain with their respective callers.
typedef struct SInfluxQLContext {
  SStringBuilder builder;
  SInfluxQLRender *render;
  const SScanLogicNode *scan;
  const SAggLogicNode *agg;
  const SPartitionLogicNode *partition;
  const SSortLogicNode *sort;
  const SLogicNode *layers[32];
  int32_t numLayers;
  int32_t nextHiddenAlias;
  char *stringValue;
  SNodesRemoteSelectOutputs outputs;
} SInfluxQLContext;

void nodesInfluxQLAppendQuoted(SStringBuilder *pBuilder, const char *pValue,
                               char quote) {
  taosStringBuilderAppendChar(pBuilder, quote);
  for (const char *p = pValue; *p != '\0'; ++p) {
    char ch = *p;
    if (ch == quote || ch == '\\') {
      taosStringBuilderAppendChar(pBuilder, '\\');
    }
    if (ch == '\n' || ch == '\r' || ch == '\t') {
      taosStringBuilderAppendChar(pBuilder, '\\');
      ch = ch == '\n' ? 'n' : (ch == '\r' ? 'r' : 't');
    }
    taosStringBuilderAppendChar(pBuilder, ch);
  }
  taosStringBuilderAppendChar(pBuilder, quote);
}

char *nodesInfluxQLQuote(const char *pValue, char quote) {
  if (pValue == NULL || (quote != '\'' && quote != '"')) return NULL;
  SStringBuilder *pBuilder = taosMemoryCalloc(1, sizeof(*pBuilder));
  if (pBuilder == NULL) return NULL;
  char *pResult = NULL;
  if (taosStringBuilderSetJmp(pBuilder) == 0) {
    nodesInfluxQLAppendQuoted(pBuilder, pValue, quote);
    pResult = taosStringBuilderGetResult(pBuilder, NULL);
    pBuilder->buf = NULL;
  }
  taosStringBuilderDestroy(pBuilder);
  taosMemoryFree(pBuilder);
  return pResult;
}

void nodesDestroyInfluxQLRender(SInfluxQLRender *pRender) {
  if (pRender == NULL) return;
  taosMemoryFree(pRender->pSelect);
  taosMemoryFree(pRender->pWhere);
  taosMemoryFree(pRender->pGroupBy);
  taosMemoryFree(pRender->pOrderBy);
  taosMemoryFree(pRender->pLimit);
  taosArrayDestroy(pRender->pOutputs);
  taosArrayDestroy(pRender->pReferencedColumns);
  taosMemoryFree(pRender);
}

static void influxQLFinishClause(SInfluxQLContext *pCtx, char **ppClause) {
  if (pCtx->builder.pos > 0) {
    *ppClause = taosStringBuilderGetResult(&pCtx->builder, NULL);
    pCtx->builder.buf = NULL;
    pCtx->builder.pos = 0;
    pCtx->builder.size = 0;
  }
}

static const SNode *influxQLUnwrap(const SNode *pNode) {
  if (pNode != NULL && nodeType(pNode) == QUERY_NODE_TARGET) {
    pNode = ((const STargetNode *)pNode)->pExpr;
  }
  if (pNode != NULL && nodeType(pNode) == QUERY_NODE_GROUPING_SET) {
    const SGroupingSetNode *pGroup = (const SGroupingSetNode *)pNode;
    if (LIST_LENGTH(pGroup->pParameterList) != 1) return NULL;
    pNode = nodesListGetNode(pGroup->pParameterList, 0);
  }
  if (pNode != NULL && nodeType(pNode) == QUERY_NODE_FUNCTION) {
    const SFunctionNode *pFunc = (const SFunctionNode *)pNode;
    if (strcmp(pFunc->functionName, "_group_key") == 0) {
      pNode = nodesListGetNode(pFunc->pParameterList, 0);
    }
  }
  return pNode;
}

static const char *influxQLColumnName(const SColumnNode *pCol) {
  return pCol->refColName[0] != '\0' ? pCol->refColName : pCol->colName;
}

static bool influxQLIsTag(const SInfluxQLContext *pCtx,
                          const SColumnNode *pCol) {
  const SExtTableNode *pTable = (const SExtTableNode *)pCtx->scan->pExtTableNode;
  const char *pName = influxQLColumnName(pCol);
  if (pTable != NULL && pTable->pExtMeta != NULL) {
    for (int32_t i = 0; i < pTable->pExtMeta->numOfCols; ++i) {
      if (strcmp(pName, pTable->pExtMeta->pCols[i].colName) == 0) {
        return pTable->pExtMeta->pCols[i].isTag;
      }
    }
  }
  SNode *pNode = NULL;
  if (pTable != NULL) {
    FOREACH(pNode, pTable->pInfluxSchema) {
      const SColumnNode *pSchema = (const SColumnNode *)pNode;
      if (strcmp(pName, pSchema->colName) == 0) {
        return pSchema->colType == COLUMN_TYPE_TAG;
      }
    }
  }
  return pCol->colType == COLUMN_TYPE_TAG;
}

static int32_t influxQLReference(SInfluxQLContext *pCtx,
                                const SColumnNode *pCol) {
  const char *pName = influxQLColumnName(pCol);
  for (int32_t i = 0; i < taosArrayGetSize(pCtx->render->pReferencedColumns); ++i) {
    const SInfluxQLColumnRef *pRef = taosArrayGet(pCtx->render->pReferencedColumns, i);
    if (strcmp(pRef->name, pName) == 0) return TSDB_CODE_SUCCESS;
  }
  SInfluxQLColumnRef ref = {0};
  tstrncpy(ref.name, pName, sizeof(ref.name));
  return taosArrayPush(pCtx->render->pReferencedColumns, &ref) != NULL
             ? TSDB_CODE_SUCCESS : terrno;
}

static int32_t influxQLColumn(SInfluxQLContext *pCtx, const SColumnNode *pCol) {
  int32_t code = influxQLReference(pCtx, pCol);
  if (code != TSDB_CODE_SUCCESS) return code;
  const char *pName = influxQLColumnName(pCol);
  if (pCol->isPrimTs || pCol->colType == COLUMN_TYPE_WINDOW_START) pName = "time";
  nodesInfluxQLAppendQuoted(&pCtx->builder, pName, '"');
  return TSDB_CODE_SUCCESS;
}

static void influxQLDouble(SStringBuilder *pBuilder, double value) {
  char number[64];
  snprintf(number, sizeof(number), "%.17g", value);
  taosStringBuilderAppendString(pBuilder, number);
  if (strchr(number, '.') == NULL && strchr(number, 'e') == NULL &&
      strchr(number, 'E') == NULL) {
    taosStringBuilderAppendString(pBuilder, ".0");
  }
}

int32_t nodesInfluxQLValueString(const SValueNode *value, char **out) {
  if (value == NULL || out == NULL) return TSDB_CODE_INVALID_PARA;
  *out = NULL;
  if (value->isNull || (value->flag & VALUE_FLAG_RAW_SQL_FRAG) != 0 ||
      (value->node.resType.type != TSDB_DATA_TYPE_BINARY &&
       value->node.resType.type != TSDB_DATA_TYPE_NCHAR)) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  if (value->datum.p == NULL) {
    if (value->literal == NULL) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    *out = taosStrdup(value->literal);
    return *out != NULL ? TSDB_CODE_SUCCESS : TSDB_CODE_OUT_OF_MEMORY;
  }
  int32_t length = varDataLen(value->datum.p);
  char *text = taosMemoryCalloc((size_t)length + 1, 1);
  if (text == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  if (value->node.resType.type == TSDB_DATA_TYPE_NCHAR) {
    void *utf8 = taosConvInit("UTF-8");
    if (utf8 == NULL) {
      taosMemoryFree(text);
      return terrno != TSDB_CODE_SUCCESS ? terrno : TSDB_CODE_OUT_OF_MEMORY;
    }
    if (length % TSDB_NCHAR_SIZE != 0) {
      taosMemoryFree(text);
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    }
    length = taosUcs4ToMbs((TdUcs4 *)varDataVal(value->datum.p), length, text, utf8);
    if (length < 0) {
      taosMemoryFree(text);
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    }
  } else {
    memcpy(text, varDataVal(value->datum.p), length);
  }
  if (memchr(text, '\0', length) != NULL) {
    taosMemoryFree(text);
    return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }
  *out = text;
  return TSDB_CODE_SUCCESS;
}

static int32_t influxQLString(SInfluxQLContext *pCtx, const SValueNode *pValue) {
  taosMemoryFreeClear(pCtx->stringValue);
  return nodesInfluxQLValueString(pValue, &pCtx->stringValue);
}

static int32_t influxQLValue(SInfluxQLContext *pCtx, const SValueNode *pValue,
                             bool forceDouble) {
  if (pValue->isNull || (pValue->flag & VALUE_FLAG_RAW_SQL_FRAG) != 0) {
    return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }
  uint8_t type = pValue->node.resType.type;
  if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
    int32_t code = influxQLString(pCtx, pValue);
    if (code != TSDB_CODE_SUCCESS) return code;
    nodesInfluxQLAppendQuoted(&pCtx->builder, pCtx->stringValue, '\'');
  } else if (type == TSDB_DATA_TYPE_BOOL) {
    taosStringBuilderAppendString(&pCtx->builder, pValue->datum.b ? "true" : "false");
  } else if (IS_SIGNED_NUMERIC_TYPE(type) || type == TSDB_DATA_TYPE_TIMESTAMP) {
    if (forceDouble) influxQLDouble(&pCtx->builder, (double)pValue->datum.i);
    else taosStringBuilderAppendInteger(&pCtx->builder, pValue->datum.i);
  } else if (IS_FLOAT_TYPE(type)) {
    if (!isfinite(pValue->datum.d)) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    influxQLDouble(&pCtx->builder, pValue->datum.d);
  } else {
    return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }
  return TSDB_CODE_SUCCESS;
}

static const char *influxQLOperator(EOperatorType op) {
  switch (op) {
    case OP_TYPE_ADD: return " + ";
    case OP_TYPE_SUB: return " - ";
    case OP_TYPE_MULTI: return " * ";
    case OP_TYPE_DIV: return " / ";
    case OP_TYPE_EQUAL: return " = ";
    case OP_TYPE_NOT_EQUAL: return " != ";
    case OP_TYPE_GREATER_THAN: return " > ";
    case OP_TYPE_GREATER_EQUAL: return " >= ";
    case OP_TYPE_LOWER_THAN: return " < ";
    case OP_TYPE_LOWER_EQUAL: return " <= ";
    default: return NULL;
  }
}

static int32_t influxQLExpr(SInfluxQLContext *pCtx, const SNode *pNode);

static void influxQLTagExists(SInfluxQLContext *pCtx, const SColumnNode *pCol) {
  taosStringBuilderAppendString(&pCtx->builder, " AND ");
  nodesInfluxQLAppendQuoted(&pCtx->builder, influxQLColumnName(pCol), '"');
  taosStringBuilderAppendString(&pCtx->builder, " != ''");
}

static int32_t influxQLIn(SInfluxQLContext *pCtx, const SOperatorNode *pOp) {
  if (pOp->pLeft == NULL || pOp->pRight == NULL ||
      nodeType(pOp->pLeft) != QUERY_NODE_COLUMN ||
      nodeType(pOp->pRight) != QUERY_NODE_NODE_LIST) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  const SNodeList *pValues = ((const SNodeListNode *)pOp->pRight)->pNodeList;
  if (LIST_LENGTH(pValues) == 0 || LIST_LENGTH(pValues) > 64) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  bool negated = pOp->opType == OP_TYPE_NOT_IN;
  const SColumnNode *pCol = (const SColumnNode *)pOp->pLeft;
  bool tag = influxQLIsTag(pCtx, pCol);
  taosStringBuilderAppendString(&pCtx->builder, tag ? "((" : "(");
  SNode *pValue = NULL;
  int32_t index = 0;
  FOREACH(pValue, pValues) {
    if (nodeType(pValue) != QUERY_NODE_VALUE) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    if (tag) {
      int32_t code = influxQLString(pCtx, (const SValueNode *)pValue);
      if (code != TSDB_CODE_SUCCESS) return code;
      if (pCtx->stringValue[0] == '\0') return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    }
    if (index++ > 0) taosStringBuilderAppendString(&pCtx->builder, negated ? " AND " : " OR ");
    int32_t code = influxQLColumn(pCtx, pCol);
    if (code != TSDB_CODE_SUCCESS) return code;
    taosStringBuilderAppendString(&pCtx->builder, negated ? " != " : " = ");
    code = influxQLValue(pCtx, (const SValueNode *)pValue, false);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  if (tag) {
    taosStringBuilderAppendChar(&pCtx->builder, ')');
    influxQLTagExists(pCtx, pCol);
  }
  taosStringBuilderAppendChar(&pCtx->builder, ')');
  return TSDB_CODE_SUCCESS;
}

static int32_t influxQLLike(SInfluxQLContext *pCtx, const SOperatorNode *pOp) {
  if (pOp->pLeft == NULL || pOp->pRight == NULL ||
      nodeType(pOp->pLeft) != QUERY_NODE_COLUMN ||
      nodeType(pOp->pRight) != QUERY_NODE_VALUE) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  const SValueNode *pValue = (const SValueNode *)pOp->pRight;
  if (pValue->isNull) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  int32_t code = influxQLString(pCtx, pValue);
  if (code != TSDB_CODE_SUCCESS) return code;
  const char *pPattern = pCtx->stringValue;
  const SColumnNode *pCol = (const SColumnNode *)pOp->pLeft;
  taosStringBuilderAppendChar(&pCtx->builder, '(');
  code = influxQLColumn(pCtx, pCol);
  if (code != TSDB_CODE_SUCCESS) return code;
  taosStringBuilderAppendString(&pCtx->builder,
      pOp->opType == OP_TYPE_LIKE ? " =~ /(?s)^" : " !~ /(?s)^");
  for (const char *p = pPattern; *p != '\0'; ++p) {
    bool escaped = *p == '\\' && p[1] != '\0';
    if (escaped) ++p;
    if (!escaped && *p == '%') taosStringBuilderAppendString(&pCtx->builder, ".*");
    else if (!escaped && *p == '_') taosStringBuilderAppendChar(&pCtx->builder, '.');
    else {
      if (strchr("\\.^$|?*+()[]{}/", *p) != NULL) taosStringBuilderAppendChar(&pCtx->builder, '\\');
      if (*p == '\n') taosStringBuilderAppendString(&pCtx->builder, "\\n");
      else if (*p == '\r') taosStringBuilderAppendString(&pCtx->builder, "\\r");
      else if (*p == '\t') taosStringBuilderAppendString(&pCtx->builder, "\\t");
      else taosStringBuilderAppendChar(&pCtx->builder, *p);
    }
  }
  taosStringBuilderAppendString(&pCtx->builder, "$/");
  if (influxQLIsTag(pCtx, pCol)) influxQLTagExists(pCtx, pCol);
  taosStringBuilderAppendChar(&pCtx->builder, ')');
  return TSDB_CODE_SUCCESS;
}

static bool influxQLIsTime(const SColumnNode *pCol) {
  return pCol->isPrimTs ||
         (pCol->node.resType.type == TSDB_DATA_TYPE_TIMESTAMP &&
          strcmp(influxQLColumnName(pCol), "time") == 0);
}

static EOperatorType influxQLReverseComparison(EOperatorType op) {
  switch (op) {
    case OP_TYPE_GREATER_THAN: return OP_TYPE_LOWER_THAN;
    case OP_TYPE_GREATER_EQUAL: return OP_TYPE_LOWER_EQUAL;
    case OP_TYPE_LOWER_THAN: return OP_TYPE_GREATER_THAN;
    case OP_TYPE_LOWER_EQUAL: return OP_TYPE_GREATER_EQUAL;
    default: return op;
  }
}

static int32_t influxQLTimePredicate(SInfluxQLContext *pCtx,
                                    const SColumnNode *pCol,
                                    const SValueNode *pValue,
                                    EOperatorType op) {
  if (pValue->isNull || (!IS_SIGNED_NUMERIC_TYPE(pValue->node.resType.type) &&
      pValue->node.resType.type != TSDB_DATA_TYPE_TIMESTAMP)) {
    return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }
  int8_t precision = pCol->node.resType.precision;
  if (precision < TSDB_TIME_PRECISION_MILLI || precision > TSDB_TIME_PRECISION_NANO) {
    return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }
  const int64_t minimum = INT64_MIN + 2;
  const int64_t maximum = INT64_MAX - 1;
  int64_t factor = precision == TSDB_TIME_PRECISION_MILLI ? 1000000 :
                   (precision == TSDB_TIME_PRECISION_MICRO ? 1000 : 1);
  int64_t value = pValue->datum.i;
  int32_t outside = value > maximum / factor ? 1 :
                    (value < minimum / factor ? -1 : 0);
  bool empty = false;
  if (outside != 0) {
    if (op == OP_TYPE_EQUAL) empty = true;
    else if (op == OP_TYPE_GREATER_THAN || op == OP_TYPE_GREATER_EQUAL) empty = outside > 0;
    else if (op == OP_TYPE_LOWER_THAN || op == OP_TYPE_LOWER_EQUAL) empty = outside < 0;
    else return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    value = minimum;
    op = empty ? OP_TYPE_LOWER_THAN : OP_TYPE_GREATER_EQUAL;
  } else {
    value *= factor;
  }
  // InfluxQL ranges use inclusive nanosecond endpoints. Adjust strictly
  // after precision conversion so one microsecond does not become the step.
  if (op == OP_TYPE_GREATER_THAN) {
    empty = empty || value >= maximum;
    if (!empty) ++value;
    op = OP_TYPE_GREATER_EQUAL;
  } else if (op == OP_TYPE_LOWER_THAN) {
    empty = empty || value <= minimum;
    if (!empty) --value;
    op = OP_TYPE_LOWER_EQUAL;
  }
  if (empty) {
    pCtx->render->scanRange.skey = 1;
    pCtx->render->scanRange.ekey = 0;
    pCtx->render->hasTimeRange = true;
  }
  const char *pOperator = influxQLOperator(op);
  if (pOperator == NULL) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  int32_t code = influxQLReference(pCtx, pCol);
  if (code != TSDB_CODE_SUCCESS) return code;
  taosStringBuilderAppendString(&pCtx->builder, "(time");
  if (empty || op == OP_TYPE_EQUAL) {
    taosStringBuilderAppendString(&pCtx->builder, " >= ");
    taosStringBuilderAppendInteger(&pCtx->builder, empty ? minimum : value);
    taosStringBuilderAppendString(&pCtx->builder, " AND time <= ");
    taosStringBuilderAppendInteger(&pCtx->builder, empty ? minimum - 1 : value);
  } else {
    taosStringBuilderAppendString(&pCtx->builder, pOperator);
    taosStringBuilderAppendInteger(&pCtx->builder, value);
  }
  taosStringBuilderAppendChar(&pCtx->builder, ')');
  return TSDB_CODE_SUCCESS;
}

static bool influxQLDoubleOperand(const SNode *pNode) {
  return pNode != NULL && nodesIsExprNode(pNode) &&
         ((const SExprNode *)pNode)->resType.type == TSDB_DATA_TYPE_DOUBLE;
}

static bool influxQLArithmeticValid(const SOperatorNode *pOp) {
  switch (pOp->opType) {
    case OP_TYPE_ADD:
    case OP_TYPE_SUB:
    case OP_TYPE_MULTI:
      return influxQLDoubleOperand(pOp->pLeft) || influxQLDoubleOperand(pOp->pRight);
    case OP_TYPE_MINUS:
      return influxQLDoubleOperand(pOp->pLeft);
    case OP_TYPE_DIV: {
      if (pOp->pRight == NULL || nodeType(pOp->pRight) != QUERY_NODE_VALUE) return false;
      const SValueNode *pValue = (const SValueNode *)pOp->pRight;
      if (pValue->isNull) return false;
      if (IS_FLOAT_TYPE(pValue->node.resType.type)) return isfinite(pValue->datum.d) && pValue->datum.d != 0;
      return IS_SIGNED_NUMERIC_TYPE(pValue->node.resType.type) && pValue->datum.i != 0;
    }
    default: return true;
  }
}

static uint8_t influxQLPredicateRoles(const SInfluxQLContext *pCtx,
                                     const SNode *pNode) {
  if (pNode == NULL) return 0;
  if (nodeType(pNode) == QUERY_NODE_COLUMN) {
    const SColumnNode *pCol = (const SColumnNode *)pNode;
    return influxQLIsTime(pCol) ? 4 : (influxQLIsTag(pCtx, pCol) ? 1 : 2);
  }
  if (nodeType(pNode) == QUERY_NODE_OPERATOR) {
    const SOperatorNode *pOp = (const SOperatorNode *)pNode;
    return influxQLPredicateRoles(pCtx, pOp->pLeft) |
           influxQLPredicateRoles(pCtx, pOp->pRight);
  }
  uint8_t roles = 0;
  SNode *pChild = NULL;
  if (nodeType(pNode) == QUERY_NODE_LOGIC_CONDITION) {
    FOREACH(pChild, ((const SLogicConditionNode *)pNode)->pParameterList) {
      roles |= influxQLPredicateRoles(pCtx, pChild);
    }
  }
  return roles;
}

static int32_t influxQLBinary(SInfluxQLContext *pCtx, const SOperatorNode *pOp) {
  if (!influxQLArithmeticValid(pOp)) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  if (pOp->opType == OP_TYPE_IN || pOp->opType == OP_TYPE_NOT_IN) return influxQLIn(pCtx, pOp);
  if (pOp->opType == OP_TYPE_LIKE || pOp->opType == OP_TYPE_NOT_LIKE) return influxQLLike(pCtx, pOp);
  if (pOp->opType == OP_TYPE_IS_NULL || pOp->opType == OP_TYPE_IS_NOT_NULL) {
    if (pOp->pLeft == NULL || nodeType(pOp->pLeft) != QUERY_NODE_COLUMN ||
        !influxQLIsTag(pCtx, (const SColumnNode *)pOp->pLeft)) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    int32_t code = influxQLColumn(pCtx, (const SColumnNode *)pOp->pLeft);
    if (code != TSDB_CODE_SUCCESS) return code;
    taosStringBuilderAppendString(&pCtx->builder,
        pOp->opType == OP_TYPE_IS_NULL ? " = ''" : " != ''");
    return TSDB_CODE_SUCCESS;
  }
  if (pOp->opType == OP_TYPE_MINUS) {
    taosStringBuilderAppendString(&pCtx->builder, "(-");
    int32_t code = influxQLExpr(pCtx, pOp->pLeft);
    taosStringBuilderAppendChar(&pCtx->builder, ')');
    return code;
  }
  const char *pOperator = influxQLOperator(pOp->opType);
  if (pOperator == NULL || pOp->pLeft == NULL || pOp->pRight == NULL) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  if (nodeType(pOp->pLeft) == QUERY_NODE_COLUMN &&
      influxQLIsTime((const SColumnNode *)pOp->pLeft) &&
      nodeType(pOp->pRight) == QUERY_NODE_VALUE) {
    return influxQLTimePredicate(pCtx, (const SColumnNode *)pOp->pLeft,
        (const SValueNode *)pOp->pRight, pOp->opType);
  }
  if (nodeType(pOp->pRight) == QUERY_NODE_COLUMN &&
      influxQLIsTime((const SColumnNode *)pOp->pRight) &&
      nodeType(pOp->pLeft) == QUERY_NODE_VALUE) {
    return influxQLTimePredicate(pCtx, (const SColumnNode *)pOp->pRight,
        (const SValueNode *)pOp->pLeft, influxQLReverseComparison(pOp->opType));
  }
  const SColumnNode *pTag = NULL;
  if (nodeType(pOp->pLeft) == QUERY_NODE_COLUMN && influxQLIsTag(pCtx, (const SColumnNode *)pOp->pLeft)) {
    pTag = (const SColumnNode *)pOp->pLeft;
  } else if (nodeType(pOp->pRight) == QUERY_NODE_COLUMN && influxQLIsTag(pCtx, (const SColumnNode *)pOp->pRight)) {
    pTag = (const SColumnNode *)pOp->pRight;
  }
  if (pTag != NULL) {
    if (pOp->opType != OP_TYPE_EQUAL && pOp->opType != OP_TYPE_NOT_EQUAL) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    const SNode *pOther = pOp->pLeft == (const SNode *)pTag ? pOp->pRight : pOp->pLeft;
    if (nodeType(pOther) != QUERY_NODE_VALUE) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    int32_t code = influxQLString(pCtx, (const SValueNode *)pOther);
    if (code != TSDB_CODE_SUCCESS) return code;
    if (pCtx->stringValue[0] == '\0') return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }
  taosStringBuilderAppendChar(&pCtx->builder, '(');
  int32_t code = influxQLExpr(pCtx, pOp->pLeft);
  if (code != TSDB_CODE_SUCCESS) return code;
  taosStringBuilderAppendString(&pCtx->builder, pOperator);
  if (pOp->opType == OP_TYPE_DIV && nodeType(pOp->pRight) == QUERY_NODE_VALUE) {
    code = influxQLValue(pCtx, (const SValueNode *)pOp->pRight, true);
  } else {
    code = influxQLExpr(pCtx, pOp->pRight);
  }
  if (code != TSDB_CODE_SUCCESS) return code;
  if (pTag != NULL && pOp->opType == OP_TYPE_NOT_EQUAL) influxQLTagExists(pCtx, pTag);
  taosStringBuilderAppendChar(&pCtx->builder, ')');
  return TSDB_CODE_SUCCESS;
}

static int32_t influxQLExpr(SInfluxQLContext *pCtx, const SNode *pNode) {
  pNode = influxQLUnwrap(pNode);
  if (pNode == NULL) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  switch (nodeType(pNode)) {
    case QUERY_NODE_COLUMN: return influxQLColumn(pCtx, (const SColumnNode *)pNode);
    case QUERY_NODE_VALUE: return influxQLValue(pCtx, (const SValueNode *)pNode, false);
    case QUERY_NODE_OPERATOR: return influxQLBinary(pCtx, (const SOperatorNode *)pNode);
    case QUERY_NODE_LOGIC_CONDITION: {
      const SLogicConditionNode *pCond = (const SLogicConditionNode *)pNode;
      if (pCond->condType != LOGIC_COND_TYPE_AND && pCond->condType != LOGIC_COND_TYPE_OR) {
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      if (pCond->condType == LOGIC_COND_TYPE_OR) {
        uint8_t roles = influxQLPredicateRoles(pCtx, pNode);
        if (roles != 1 && roles != 2) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      taosStringBuilderAppendChar(&pCtx->builder, '(');
      int32_t index = 0;
      SNode *pChild = NULL;
      FOREACH(pChild, pCond->pParameterList) {
        if (index++ > 0) taosStringBuilderAppendString(&pCtx->builder,
            pCond->condType == LOGIC_COND_TYPE_AND ? " AND " : " OR ");
        int32_t code = influxQLExpr(pCtx, pChild);
        if (code != TSDB_CODE_SUCCESS) return code;
      }
      taosStringBuilderAppendChar(&pCtx->builder, ')');
      return index > 0 ? TSDB_CODE_SUCCESS : TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    }
    default: return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }
}

static int32_t influxQLCollectLayers(SInfluxQLContext *pCtx, const SLogicNode *pPlan) {
  const SLogicNode *pNode = pPlan;
  while (pNode != NULL) {
    if (pCtx->numLayers == 32 || LIST_LENGTH(pNode->pChildren) > 1 || pNode->pSlimit != NULL) {
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    }
    pCtx->layers[pCtx->numLayers++] = pNode;
    switch (nodeType(pNode)) {
      case QUERY_NODE_LOGIC_PLAN_SCAN:
        if (pCtx->scan != NULL || LIST_LENGTH(pNode->pChildren) != 0) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
        pCtx->scan = (const SScanLogicNode *)pNode;
        break;
      case QUERY_NODE_LOGIC_PLAN_AGG:
        if (pCtx->agg != NULL || pNode->pConditions != NULL) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
        pCtx->agg = (const SAggLogicNode *)pNode;
        break;
      case QUERY_NODE_LOGIC_PLAN_PARTITION:
        if (pCtx->partition != NULL) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
        pCtx->partition = (const SPartitionLogicNode *)pNode;
        break;
      case QUERY_NODE_LOGIC_PLAN_SORT:
        if (pCtx->sort != NULL) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
        pCtx->sort = (const SSortLogicNode *)pNode;
        break;
      case QUERY_NODE_LOGIC_PLAN_PROJECT: break;
      default: return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    }
    pNode = (const SLogicNode *)nodesListGetNode(pNode->pChildren, 0);
  }
  if (pCtx->scan == NULL || pCtx->scan->scanType != SCAN_TYPE_EXTERNAL ||
      (pCtx->partition != NULL && pCtx->agg == NULL)) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  if (pCtx->agg == NULL) {
    for (int32_t i = 0; i < pCtx->numLayers; ++i) {
      if (nodeType(pCtx->layers[i]) != QUERY_NODE_LOGIC_PLAN_PROJECT) continue;
      SNode *pExpr = NULL;
      FOREACH(pExpr, ((const SProjectLogicNode *)pCtx->layers[i])->pProjections) {
        const SNode *pActual = influxQLUnwrap(pExpr);
        if (pActual == NULL || nodeType(pActual) != QUERY_NODE_COLUMN) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
    }
  }
  if (pCtx->agg != NULL) {
    for (int32_t i = 0; i < pCtx->numLayers; ++i) {
      if (pCtx->layers[i]->pConditions != NULL) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      if (pCtx->layers[i] == &pCtx->agg->node) break;
    }
  }
  pCtx->render->aggregate = pCtx->agg != NULL;
  pCtx->render->scanRange = pCtx->scan->scanRange;
  pCtx->render->rangePrecision = pCtx->scan->node.precision;
  pCtx->render->hasTimeRange = pCtx->scan->scanRange.skey != INT64_MIN || pCtx->scan->scanRange.ekey != INT64_MAX;
  return TSDB_CODE_SUCCESS;
}

static int32_t influxQLWhere(SInfluxQLContext *pCtx) {
  for (int32_t i = pCtx->numLayers - 1; i >= 0; --i) {
    const SNode *pCondition = pCtx->layers[i]->pConditions;
    if (pCondition == NULL) continue;
    if (pCtx->builder.pos > 0) taosStringBuilderAppendString(&pCtx->builder, " AND ");
    int32_t code = influxQLExpr(pCtx, pCondition);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  if (pCtx->scan->pPrimaryCond != NULL) {
    if (pCtx->builder.pos > 0) taosStringBuilderAppendString(&pCtx->builder, " AND ");
    int32_t code = influxQLExpr(pCtx, pCtx->scan->pPrimaryCond);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  influxQLFinishClause(pCtx, &pCtx->render->pWhere);
  return TSDB_CODE_SUCCESS;
}

static int32_t influxQLBucket(SInfluxQLContext *pCtx, const SFunctionNode *pFunc) {
  if (LIST_LENGTH(pFunc->pParameterList) != 4) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  const SNode *pStepNode = nodesListGetNode(pFunc->pParameterList, 1);
  const SNode *pPrecisionNode = nodesListGetNode(pFunc->pParameterList, 2);
  const SNode *pOffsetNode = nodesListGetNode(pFunc->pParameterList, 3);
  if (nodeType(pStepNode) != QUERY_NODE_VALUE || nodeType(pPrecisionNode) != QUERY_NODE_VALUE ||
      nodeType(pOffsetNode) != QUERY_NODE_VALUE) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  const SNode *pTime = nodesListGetNode(pFunc->pParameterList, 0);
  if (nodeType(pTime) != QUERY_NODE_COLUMN ||
      !influxQLIsTime((const SColumnNode *)pTime)) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  int32_t code = influxQLReference(pCtx, (const SColumnNode *)pTime);
  if (code != TSDB_CODE_SUCCESS) return code;
  int64_t step = ((const SValueNode *)pStepNode)->datum.i;
  int64_t offset = ((const SValueNode *)pOffsetNode)->datum.i;
  int64_t precision = ((const SValueNode *)pPrecisionNode)->datum.i;
  int64_t factor = precision == TSDB_TIME_PRECISION_MILLI ? 1000000 :
                   (precision == TSDB_TIME_PRECISION_MICRO ? 1000 : 1);
  if (precision < 0 || precision > TSDB_TIME_PRECISION_NANO || step <= 0 ||
      step > INT64_MAX / factor || offset > INT64_MAX / factor || offset < INT64_MIN / factor) {
    return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }
  pCtx->render->timeGrouped = true;
  pCtx->render->windowStep = step;
  pCtx->render->windowPrecision = (int8_t)precision;
  taosStringBuilderAppendString(&pCtx->builder, "time(");
  taosStringBuilderAppendInteger(&pCtx->builder, step * factor);
  taosStringBuilderAppendString(&pCtx->builder, "ns");
  if (offset != 0) {
    taosStringBuilderAppendString(&pCtx->builder, ", ");
    taosStringBuilderAppendInteger(&pCtx->builder, offset * factor);
    taosStringBuilderAppendString(&pCtx->builder, "ns");
  }
  taosStringBuilderAppendChar(&pCtx->builder, ')');
  return TSDB_CODE_SUCCESS;
}

static int32_t influxQLGroupKey(SInfluxQLContext *pCtx, const SNode *pNode) {
  pNode = influxQLUnwrap(pNode);
  if (pNode == NULL) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  if (pCtx->builder.pos > 0) taosStringBuilderAppendString(&pCtx->builder, ", ");
  if (nodeType(pNode) == QUERY_NODE_FUNCTION &&
      strcmp(((const SFunctionNode *)pNode)->functionName, "_fq_time_bucket") == 0) {
    return influxQLBucket(pCtx, (const SFunctionNode *)pNode);
  }
  if (nodeType(pNode) != QUERY_NODE_COLUMN || !influxQLIsTag(pCtx, (const SColumnNode *)pNode)) {
    return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }
  pCtx->render->tagGrouped = true;
  return influxQLColumn(pCtx, (const SColumnNode *)pNode);
}

static int32_t influxQLGrouping(SInfluxQLContext *pCtx) {
  if (pCtx->agg == NULL) return TSDB_CODE_SUCCESS;
  SNode *pNode = NULL;
  FOREACH(pNode, pCtx->agg->pGroupKeys) {
    int32_t code = influxQLGroupKey(pCtx, pNode);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  if (pCtx->partition != NULL) {
    FOREACH(pNode, pCtx->partition->pPartitionKeys) {
      const SNode *pKey = influxQLUnwrap(pNode);
      bool present = false;
      SNode *pAggKey = NULL;
      FOREACH(pAggKey, pCtx->agg->pGroupKeys) {
        const SNode *pActual = influxQLUnwrap(pAggKey);
        if (nodesEqualNode(pActual, pKey)) present = true;
        if (pActual != NULL && pKey != NULL &&
            nodeType(pActual) == QUERY_NODE_COLUMN && nodeType(pKey) == QUERY_NODE_COLUMN &&
            strcmp(influxQLColumnName((const SColumnNode *)pActual),
                   influxQLColumnName((const SColumnNode *)pKey)) == 0) present = true;
      }
      if (!present) {
        int32_t code = influxQLGroupKey(pCtx, pNode);
        if (code != TSDB_CODE_SUCCESS) return code;
      }
    }
  }
  pCtx->render->grouped = pCtx->render->timeGrouped || pCtx->render->tagGrouped;
  if (pCtx->render->grouped) taosStringBuilderAppendString(&pCtx->builder, " fill(none)");
  influxQLFinishClause(pCtx, &pCtx->render->pGroupBy);
  return TSDB_CODE_SUCCESS;
}

static const char *influxQLAggName(const SFunctionNode *pFunc) {
  if (pFunc->isDistinct) return NULL;
  if (strcmp(pFunc->functionName, "count") == 0) return "COUNT";
  if (strcmp(pFunc->functionName, "sum") == 0) return "SUM";
  if (strcmp(pFunc->functionName, "min") == 0) return "MIN";
  if (strcmp(pFunc->functionName, "max") == 0) return "MAX";
  if (strcmp(pFunc->functionName, "avg") == 0) return "MEAN";
  if (strcmp(pFunc->functionName, "spread") == 0) return "SPREAD";
  if (strcmp(pFunc->functionName, "stddev_samp") == 0) return "STDDEV";
  return NULL;
}

static void influxQLHiddenAlias(SInfluxQLContext *pCtx, const char *pPrefix,
                                char *pAlias, size_t capacity) {
  bool unique = false;
  while (!unique) {
    snprintf(pAlias, capacity, "%s%d", pPrefix, pCtx->nextHiddenAlias++);
    unique = true;
    SNode *pRef = NULL;
    FOREACH(pRef, pCtx->outputs.pRefExprs) {
      const SNode *pActual = influxQLUnwrap(pRef);
      if (pActual == NULL || !nodesIsExprNode(pActual)) continue;
      const SExprNode *pExpr = (const SExprNode *)pActual;
      const char *pName = nodeType(pActual) == QUERY_NODE_COLUMN
                             ? ((const SColumnNode *)pActual)->colName
                             : pExpr->aliasName;
      if (strcmp(pName, pAlias) == 0) unique = false;
    }
  }
}

static int32_t influxQLAppendHiddenCount(SInfluxQLContext *pCtx,
                                        const SColumnNode *pColumn,
                                        const char *pAlias) {
  taosStringBuilderAppendString(&pCtx->builder, ", COUNT(");
  int32_t code = influxQLColumn(pCtx, pColumn);
  if (code != TSDB_CODE_SUCCESS) return code;
  taosStringBuilderAppendString(&pCtx->builder, ") AS ");
  nodesInfluxQLAppendQuoted(&pCtx->builder, pAlias, '"');
  return TSDB_CODE_SUCCESS;
}

static int32_t influxQLSelectOutput(SInfluxQLContext *pCtx, const SNode *pExpr,
                                    const SNode *pRef) {
  pExpr = influxQLUnwrap(pExpr);
  pRef = influxQLUnwrap(pRef);
  if (pExpr == NULL || pRef == NULL || !nodesIsExprNode(pExpr) || !nodesIsExprNode(pRef)) {
    return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }
  SInfluxQLOutput output = {0};
  const SExprNode *pExprNode = (const SExprNode *)pExpr;
  const SExprNode *pRefNode = (const SExprNode *)pRef;
  const char *pAlias = pRefNode->aliasName;
  if (nodeType(pRef) == QUERY_NODE_COLUMN) pAlias = ((const SColumnNode *)pRef)->colName;
  if (pAlias[0] == '\0') pAlias = pExprNode->aliasName;
  if (pAlias[0] == '\0') return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  tstrncpy(output.name, pAlias, sizeof(output.name));
  output.dataType = pExprNode->resType;
  if (nodeType(pExpr) == QUERY_NODE_COLUMN) {
    const SColumnNode *pCol = (const SColumnNode *)pExpr;
    tstrncpy(output.sourceName, influxQLColumnName(pCol), sizeof(output.sourceName));
    if (pCol->colType == COLUMN_TYPE_WINDOW_START) output.role = INFLUXQL_OUTPUT_TIME;
    else if (pCol->colType == COLUMN_TYPE_WINDOW_END) output.role = INFLUXQL_OUTPUT_WEND;
    else if (pCol->colType == COLUMN_TYPE_WINDOW_DURATION) output.role = INFLUXQL_OUTPUT_WDURATION;
    else if (influxQLIsTag(pCtx, pCol)) output.role = INFLUXQL_OUTPUT_TAG;
    else return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    if (output.role == INFLUXQL_OUTPUT_TAG) {
      int32_t code = influxQLReference(pCtx, pCol);
      if (code != TSDB_CODE_SUCCESS) return code;
    }
  } else if (nodeType(pExpr) == QUERY_NODE_FUNCTION) {
    const SFunctionNode *pFunc = (const SFunctionNode *)pExpr;
    if (strcmp(pFunc->functionName, "_fq_time_bucket") == 0 || pFunc->funcType == FUNCTION_TYPE_WSTART) {
      output.role = INFLUXQL_OUTPUT_TIME;
    } else if (pFunc->funcType == FUNCTION_TYPE_WEND) {
      output.role = INFLUXQL_OUTPUT_WEND;
    } else if (pFunc->funcType == FUNCTION_TYPE_WDURATION) {
      output.role = INFLUXQL_OUTPUT_WDURATION;
    } else {
      const char *pName = influxQLAggName(pFunc);
      if (pName == NULL || LIST_LENGTH(pFunc->pParameterList) != 1) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      const SNode *pArg = nodesListGetNode(pFunc->pParameterList, 0);
      if (nodeType(pArg) != QUERY_NODE_COLUMN || influxQLIsTag(pCtx, (const SColumnNode *)pArg) ||
          influxQLIsTime((const SColumnNode *)pArg)) {
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      uint8_t type = ((const SColumnNode *)pArg)->node.resType.type;
      if (strcmp(pFunc->functionName, "count") != 0 &&
          type != TSDB_DATA_TYPE_BIGINT && type != TSDB_DATA_TYPE_DOUBLE) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      if (strcmp(pFunc->functionName, "spread") == 0 && type != TSDB_DATA_TYPE_DOUBLE) {
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      if (pCtx->builder.pos > 0) taosStringBuilderAppendString(&pCtx->builder, ", ");
      taosStringBuilderAppendString(&pCtx->builder, pName);
      taosStringBuilderAppendChar(&pCtx->builder, '(');
      int32_t code = influxQLColumn(pCtx, (const SColumnNode *)pArg);
      if (code != TSDB_CODE_SUCCESS) return code;
      taosStringBuilderAppendString(&pCtx->builder, ") AS ");
      nodesInfluxQLAppendQuoted(&pCtx->builder, output.name, '"');
      output.emptyCount = strcmp(pFunc->functionName, "count") == 0;
      if (strcmp(pFunc->functionName, "stddev_samp") == 0) {
        influxQLHiddenAlias(pCtx, "__fq_sample_", output.countName,
                           sizeof(output.countName));
        code = influxQLAppendHiddenCount(pCtx, (const SColumnNode *)pArg,
                                         output.countName);
        if (code != TSDB_CODE_SUCCESS) return code;
      }
    }
  } else {
    return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }
  return taosArrayPush(pCtx->render->pOutputs, &output) != NULL ? TSDB_CODE_SUCCESS : terrno;
}

static bool influxQLAggregatesColumn(const SInfluxQLContext *pCtx,
                                      const char *pName) {
  SNode *pNode = NULL;
  FOREACH(pNode, pCtx->agg->pAggFuncs) {
    if (nodeType(pNode) != QUERY_NODE_FUNCTION) continue;
    const SFunctionNode *pFunc = (const SFunctionNode *)pNode;
    if (influxQLAggName(pFunc) == NULL) continue;
    const SNode *pArg = nodesListGetNode(pFunc->pParameterList, 0);
    if (pArg != NULL && nodeType(pArg) == QUERY_NODE_COLUMN &&
        strcmp(influxQLColumnName((const SColumnNode *)pArg), pName) == 0) return true;
  }
  return false;
}

static int32_t influxQLSparseAnchor(SInfluxQLContext *pCtx, const char *pName) {
  if (strcmp(pName, "time") == 0 || influxQLAggregatesColumn(pCtx, pName)) {
    return TSDB_CODE_SUCCESS;
  }
  char alias[TSDB_COL_NAME_LEN];
  influxQLHiddenAlias(pCtx, "__fq_sparse_", alias, sizeof(alias));
  SColumnNode column = {0};
  tstrncpy(column.colName, pName, sizeof(column.colName));
  return influxQLAppendHiddenCount(pCtx, &column, alias);
}

static int32_t influxQLSparseAnchors(SInfluxQLContext *pCtx) {
  const SExtTableNode *pTable = (const SExtTableNode *)pCtx->scan->pExtTableNode;
  if (pTable == NULL) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  // Input containing only an unselected Field still exists in a raw TDengine
  // scan, both with and without groups. Hidden counts preserve its result row
  // when every visible aggregate argument is absent, without exposing a slot.
  if (pTable->pExtMeta != NULL) {
    for (int32_t i = 0; i < pTable->pExtMeta->numOfCols; ++i) {
      const SExtColumnDef *pCol = &pTable->pExtMeta->pCols[i];
      if (pCol->isTag || pCol->isPrimaryKey) continue;
      int32_t code = influxQLSparseAnchor(pCtx, pCol->colName);
      if (code != TSDB_CODE_SUCCESS) return code;
    }
  } else {
    if (LIST_LENGTH(pTable->pInfluxSchema) == 0) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    SNode *pNode = NULL;
    FOREACH(pNode, pTable->pInfluxSchema) {
      const SColumnNode *pCol = (const SColumnNode *)pNode;
      if (pCol->colType == COLUMN_TYPE_TAG) continue;
      int32_t code = influxQLSparseAnchor(pCtx, pCol->colName);
      if (code != TSDB_CODE_SUCCESS) return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t influxQLSelect(SInfluxQLContext *pCtx, const SLogicNode *pPlan) {
  if (!pCtx->render->aggregate) return TSDB_CODE_SUCCESS;
  int32_t code = nodesCollectRemoteQuerySelectOutputs(pPlan, &pCtx->outputs);
  if (code != TSDB_CODE_SUCCESS) return code;
  for (int32_t i = 0; i < LIST_LENGTH(pCtx->outputs.pSelectExprs); ++i) {
    code = influxQLSelectOutput(pCtx, nodesListGetNode(pCtx->outputs.pSelectExprs, i),
                                nodesListGetNode(pCtx->outputs.pRefExprs, i));
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  if (pCtx->builder.pos == 0) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  code = influxQLSparseAnchors(pCtx);
  if (code != TSDB_CODE_SUCCESS) return code;
  influxQLFinishClause(pCtx, &pCtx->render->pSelect);
  return TSDB_CODE_SUCCESS;
}

static bool influxQLSortKeyIsTime(const SInfluxQLContext *pCtx,
                                   const SNode *pNode) {
  pNode = influxQLUnwrap(pNode);
  if (pNode == NULL || nodeType(pNode) != QUERY_NODE_COLUMN) return false;
  const SColumnNode *pCol = (const SColumnNode *)pNode;
  if (!pCtx->render->aggregate) return influxQLIsTime(pCol);
  if (!pCtx->render->timeGrouped) return false;
  if (pCol->colType == COLUMN_TYPE_WINDOW_START || strcmp(pCol->colName, "_wstart") == 0) return true;
  for (int32_t i = 0; i < taosArrayGetSize(pCtx->render->pOutputs); ++i) {
    const SInfluxQLOutput *pOutput = taosArrayGet(pCtx->render->pOutputs, i);
    if (pOutput->role == INFLUXQL_OUTPUT_TIME && strcmp(pOutput->name, pCol->colName) == 0) return true;
  }
  return false;
}

static int32_t influxQLSortLimit(SInfluxQLContext *pCtx, const SLogicNode *pPlan) {
  if (pCtx->sort != NULL) {
    if (pCtx->render->tagGrouped || LIST_LENGTH(pCtx->sort->pSortKeys) != 1) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    const SOrderByExprNode *pOrder = (const SOrderByExprNode *)nodesListGetNode(pCtx->sort->pSortKeys, 0);
    if (nodeType(pOrder) != QUERY_NODE_ORDER_BY_EXPR ||
        !influxQLSortKeyIsTime(pCtx, pOrder->pExpr)) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    taosStringBuilderAppendString(&pCtx->builder, pOrder->order == ORDER_DESC ? "time DESC" : "time ASC");
    influxQLFinishClause(pCtx, &pCtx->render->pOrderBy);
  }
  for (int32_t i = 1; i < pCtx->numLayers; ++i) {
    if (pCtx->layers[i]->pLimit != NULL) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }
  if (pPlan->pLimit == NULL) return TSDB_CODE_SUCCESS;
  const SLimitNode *pLimit = (const SLimitNode *)pPlan->pLimit;
  if (pCtx->render->tagGrouped || pLimit->limit == NULL || pLimit->limit->datum.i <= 0) {
    return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }
  pCtx->render->hasLimit = true;
  pCtx->render->limit = pLimit->limit->datum.i;
  pCtx->render->offset = pLimit->offset != NULL ? pLimit->offset->datum.i : 0;
  taosStringBuilderAppendInteger(&pCtx->builder, pCtx->render->limit);
  if (pLimit->offset != NULL && pLimit->offset->datum.i < 0) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  if (pLimit->offset != NULL && pLimit->offset->datum.i > 0) {
    taosStringBuilderAppendString(&pCtx->builder, " OFFSET ");
    taosStringBuilderAppendInteger(&pCtx->builder, pLimit->offset->datum.i);
  }
  influxQLFinishClause(pCtx, &pCtx->render->pLimit);
  return TSDB_CODE_SUCCESS;
}

int32_t nodesRemotePlanToInfluxQL(const SLogicNode *pPlan, SInfluxQLRender **ppRender) {
  if (pPlan == NULL || ppRender == NULL) return TSDB_CODE_INVALID_PARA;
  *ppRender = NULL;
  SInfluxQLContext *pCtx = taosMemoryCalloc(1, sizeof(*pCtx));
  if (pCtx == NULL) return terrno;
  pCtx->render = taosMemoryCalloc(1, sizeof(*pCtx->render));
  if (pCtx->render == NULL) {
    taosMemoryFree(pCtx);
    return terrno;
  }
  pCtx->render->pOutputs = taosArrayInit(4, sizeof(SInfluxQLOutput));
  pCtx->render->pReferencedColumns = taosArrayInit(4, sizeof(SInfluxQLColumnRef));
  int32_t code = TSDB_CODE_OUT_OF_MEMORY;
  if (pCtx->render->pOutputs != NULL && pCtx->render->pReferencedColumns != NULL) {
    if (taosStringBuilderSetJmp(&pCtx->builder) != 0) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    } else {
      code = influxQLCollectLayers(pCtx, pPlan);
      if (code == TSDB_CODE_SUCCESS) code = influxQLWhere(pCtx);
      if (code == TSDB_CODE_SUCCESS) code = influxQLGrouping(pCtx);
      if (code == TSDB_CODE_SUCCESS) code = influxQLSelect(pCtx, pPlan);
      if (code == TSDB_CODE_SUCCESS) code = influxQLSortLimit(pCtx, pPlan);
    }
  }
  if (code == TSDB_CODE_SUCCESS) {
    *ppRender = pCtx->render;
    pCtx->render = NULL;
  }
  nodesDestroyRemoteSelectOutputs(&pCtx->outputs);
  nodesDestroyInfluxQLRender(pCtx->render);
  taosStringBuilderDestroy(&pCtx->builder);
  taosMemoryFree(pCtx->stringValue);
  taosMemoryFree(pCtx);
  return code;
}
