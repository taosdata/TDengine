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

#include "cmdnodes.h"
#include "functionMgt.h"
#include "plannodes.h"
#include "querynodes.h"

static bool nodeListNodeEqual(const SNodeListNode* a, const SNodeListNode* b);
bool        nodesEqualNode(const SNode* a, const SNode* b);

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
  COMPARE_SCALAR_FIELD(isDistinct);
  COMPARE_NODE_LIST_FIELD(pParameterList);
  COMPARE_NODE_FIELD(pOver);
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

static bool sqlWindowBoundEqual(const SSqlWindowBound* a, const SSqlWindowBound* b) {
  COMPARE_SCALAR_FIELD(boundType);
  COMPARE_NODE_FIELD(pOffset);
  return true;
}

static bool sqlWindowFrameEqual(const SWindowFrameNode* a, const SWindowFrameNode* b) {
  COMPARE_SCALAR_FIELD(frameUnit);
  COMPARE_SCALAR_FIELD(explicitFrame);
  if (!sqlWindowBoundEqual(&a->start, &b->start)) return false;
  if (!sqlWindowBoundEqual(&a->end, &b->end)) return false;
  return true;
}

static bool sqlWindowSpecEqual(const SWindowSpecNode* a, const SWindowSpecNode* b) {
  COMPARE_STRING_FIELD(refWindowName);
  COMPARE_NODE_LIST_FIELD(pPartitionByList);
  COMPARE_NODE_LIST_FIELD(pOrderByList);
  if (!nodesEqualNode(a->pFrame, b->pFrame)) return false;
  return true;
}

static bool sqlNamedWindowEqual(const SNamedWindowNode* a, const SNamedWindowNode* b) {
  COMPARE_STRING_FIELD(windowName);
  COMPARE_NODE_FIELD(pSpec);
  return true;
}

static bool logicWindowFuncNodeEqual(const SWindowFuncLogicNode* a, const SWindowFuncLogicNode* b) {
  COMPARE_NODE_LIST_FIELD(pPartitionKeys);
  COMPARE_NODE_LIST_FIELD(pOrderKeys);
  COMPARE_NODE_LIST_FIELD(pFuncs);
  COMPARE_NODE_FIELD(pFrame);
  return true;
}

static bool physiWindowFuncNodeEqual(const SWindowFuncPhysiNode* a, const SWindowFuncPhysiNode* b) {
  COMPARE_NODE_LIST_FIELD(pExprs);
  COMPARE_NODE_LIST_FIELD(pPartitionKeys);
  COMPARE_NODE_LIST_FIELD(pOrderKeys);
  COMPARE_NODE_LIST_FIELD(pFuncs);
  COMPARE_NODE_FIELD(pFrame);
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

static bool streamTriggerOptionsEqual(const SStreamTriggerOptions* a, const SStreamTriggerOptions* b) {
  COMPARE_NODE_FIELD(pPreFilter);
  COMPARE_NODE_FIELD(pWaterMark);
  COMPARE_NODE_FIELD(pMaxDelay);
  COMPARE_NODE_FIELD(pExpiredTime);
  COMPARE_NODE_FIELD(pFillHisStartTime);
  COMPARE_NODE_FIELD(pIdleTimeout);
  COMPARE_SCALAR_FIELD(pEventType);
  COMPARE_SCALAR_FIELD(fillHistoryStartTime);
  COMPARE_SCALAR_FIELD(ignoreDisorder);
  COMPARE_SCALAR_FIELD(deleteRecalc);
  COMPARE_SCALAR_FIELD(deleteOutputTable);
  COMPARE_SCALAR_FIELD(fillHistory);
  COMPARE_SCALAR_FIELD(fillHistoryFirst);
  COMPARE_SCALAR_FIELD(calcNotifyOnly);
  COMPARE_SCALAR_FIELD(lowLatencyCalc);
  COMPARE_SCALAR_FIELD(forceOutput);
  COMPARE_SCALAR_FIELD(ignoreNoDataTrigger);
  COMPARE_SCALAR_FIELD(flushOnOuterClose);
  return true;
}

static bool streamNotifyOptionsEqual(const SStreamNotifyOptions* a, const SStreamNotifyOptions* b) {
  COMPARE_NODE_LIST_FIELD(pAddrUrls);
  COMPARE_NODE_FIELD(pWhere);
  COMPARE_SCALAR_FIELD(eventType);
  COMPARE_SCALAR_FIELD(notifyType);
  return true;
}

static bool streamTriggerNodeEqual(const SStreamTriggerNode* a, const SStreamTriggerNode* b) {
  COMPARE_NODE_FIELD(pTriggerWindow);
  COMPARE_NODE_FIELD(pTrigerTable);
  COMPARE_NODE_FIELD(pOptions);
  COMPARE_NODE_FIELD(pNotify);
  COMPARE_NODE_LIST_FIELD(pPartitionList);
  COMPARE_NODE_LIST_FIELD(pRollupTagList);
  return true;
}

static bool streamTagDefNodeEqual(const SStreamTagDefNode* a, const SStreamTagDefNode* b) {
  COMPARE_STRING_FIELD(tagName);
  COMPARE_OBJECT_FIELD(dataType, dataTypeEqual);
  COMPARE_NODE_FIELD(pTagExpr);
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

static bool remoteTableNodeEqual(const SRemoteTableNode* a, const SRemoteTableNode* b) {
  COMPARE_SCALAR_FIELD(resCols);
  COMPARE_SCALAR_FIELD(subQIdx);
  return true;
}

static bool nodeListNodeEqual(const SNodeListNode* a, const SNodeListNode* b) {
  if (LIST_LENGTH(a->pNodeList) != LIST_LENGTH(b->pNodeList)) {
    return false;
  }

  COMPARE_NODE_LIST_FIELD(pNodeList);
  return true;
}

static bool streamSqlSpanEqual(SStreamSqlSpan a, SStreamSqlSpan b) { return a.begin == b.begin && a.end == b.end; }

static bool streamSqlSpanArrayEqual(const SArray* a, const SArray* b) {
  if (a == b) {
    return true;
  }
  if (a == NULL || b == NULL || taosArrayGetSize(a) != taosArrayGetSize(b)) {
    return false;
  }
  for (int32_t i = 0; i < taosArrayGetSize(a); ++i) {
    const SStreamSqlSpan* pA = taosArrayGet(a, i);
    const SStreamSqlSpan* pB = taosArrayGet(b, i);
    if (!streamSqlSpanEqual(*pA, *pB)) {
      return false;
    }
  }
  return true;
}

static bool streamCreateTimeInfoEqual(const SStreamCreateTimeInfo* a, const SStreamCreateTimeInfo* b) {
  if (a == b) {
    return true;
  }
  if (a == NULL || b == NULL) {
    return false;
  }
  return a->hasTimezone == b->hasTimezone && a->hasFirstDayOfWeek == b->hasFirstDayOfWeek &&
         streamSqlSpanEqual(a->timezoneValue, b->timezoneValue) &&
         streamSqlSpanEqual(a->firstDayOfWeekValue, b->firstDayOfWeekValue) &&
         streamSqlSpanEqual(a->streamOptions, b->streamOptions) &&
         a->streamOptionsInsertPos == b->streamOptionsInsertPos &&
         streamSqlSpanArrayEqual(a->pNonConfigOptionSpans, b->pNonConfigOptionSpans) &&
         strncmp(a->config.timezone, b->config.timezone, sizeof(a->config.timezone)) == 0 &&
         a->config.firstDayOfWeek == b->config.firstDayOfWeek;
}

static bool createStreamStmtEqual(const SCreateStreamStmt* a, const SCreateStreamStmt* b) {
  return strncmp(a->streamDbName, b->streamDbName, sizeof(a->streamDbName)) == 0 &&
         strncmp(a->streamName, b->streamName, sizeof(a->streamName)) == 0 &&
         strncmp(a->targetDbName, b->targetDbName, sizeof(a->targetDbName)) == 0 &&
         strncmp(a->targetTabName, b->targetTabName, sizeof(a->targetTabName)) == 0 &&
         a->ignoreExists == b->ignoreExists && a->nodelayCreateSubtable == b->nodelayCreateSubtable &&
         nodesEqualNode(a->pTrigger, b->pTrigger) && nodesEqualNode(a->pQuery, b->pQuery) &&
         nodesEqualNode(a->pSubtable, b->pSubtable) && nodeNodeListEqual(a->pTags, b->pTags) &&
         nodeNodeListEqual(a->pCols, b->pCols) && streamCreateTimeInfoEqual(a->pTimeInfo, b->pTimeInfo);
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
    case QUERY_NODE_LEFT_VALUE:
      return true;
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
    case QUERY_NODE_SQL_WINDOW_SPEC:
      return sqlWindowSpecEqual((const SWindowSpecNode*)a, (const SWindowSpecNode*)b);
    case QUERY_NODE_SQL_WINDOW_FRAME:
      return sqlWindowFrameEqual((const SWindowFrameNode*)a, (const SWindowFrameNode*)b);
    case QUERY_NODE_SQL_NAMED_WINDOW:
      return sqlNamedWindowEqual((const SNamedWindowNode*)a, (const SNamedWindowNode*)b);
    case QUERY_NODE_LOGIC_PLAN_WINDOW_FUNC:
      return logicWindowFuncNodeEqual((const SWindowFuncLogicNode*)a, (const SWindowFuncLogicNode*)b);
    case QUERY_NODE_PHYSICAL_PLAN_WINDOW_FUNC:
      return physiWindowFuncNodeEqual((const SWindowFuncPhysiNode*)a, (const SWindowFuncPhysiNode*)b);
    case QUERY_NODE_WHEN_THEN:
      return whenThenNodeEqual((const SWhenThenNode*)a, (const SWhenThenNode*)b);
    case QUERY_NODE_CASE_WHEN:
      return caseWhenNodeEqual((const SCaseWhenNode*)a, (const SCaseWhenNode*)b);
    case QUERY_NODE_GROUPING_SET:
      return groupingSetNodeEqual((const SGroupingSetNode*)a, (const SGroupingSetNode*)b);
    case QUERY_NODE_NODE_LIST:
      return nodeListNodeEqual((const SNodeListNode*)a, (const SNodeListNode*)b);
    case QUERY_NODE_STREAM_TRIGGER:
      return streamTriggerNodeEqual((const SStreamTriggerNode*)a, (const SStreamTriggerNode*)b);
    case QUERY_NODE_STREAM_TRIGGER_OPTIONS:
      return streamTriggerOptionsEqual((const SStreamTriggerOptions*)a, (const SStreamTriggerOptions*)b);
    case QUERY_NODE_STREAM_NOTIFY_OPTIONS:
      return streamNotifyOptionsEqual((const SStreamNotifyOptions*)a, (const SStreamNotifyOptions*)b);
    case QUERY_NODE_STREAM_TAG_DEF:
      return streamTagDefNodeEqual((const SStreamTagDefNode*)a, (const SStreamTagDefNode*)b);
    case QUERY_NODE_CREATE_STREAM_STMT:
      return createStreamStmtEqual((const SCreateStreamStmt*)a, (const SCreateStreamStmt*)b);
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
    case QUERY_NODE_REMOTE_TABLE:
      return remoteTableNodeEqual((const SRemoteTableNode*)a, (const SRemoteTableNode*)b);
    default:
      break;
  }

  return false;
}
