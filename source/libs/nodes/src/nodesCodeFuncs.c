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

#include "plannodes.h"
#include "querynodes.h"
#include "query.h"
#include "taoserror.h"
#include "tjson.h"

static int32_t nodeToJson(const void* pObj, SJson* pJson);

static char* nodeName(ENodeType type) {
  switch (type) {
    case QUERY_NODE_COLUMN:
      return "Column";    
    case QUERY_NODE_VALUE:
      return "Value";
    case QUERY_NODE_OPERATOR:
      return "Operator";
    case QUERY_NODE_LOGIC_CONDITION:
      return "LogicCondition";
    case QUERY_NODE_FUNCTION:
      return "Function";
    case QUERY_NODE_REAL_TABLE:
      return "RealTable";
    case QUERY_NODE_TEMP_TABLE:
      return "TempTable";
    case QUERY_NODE_JOIN_TABLE:
      return "JoinTable";
    case QUERY_NODE_GROUPING_SET:
      return "GroupingSet";
    case QUERY_NODE_ORDER_BY_EXPR:
      return "OrderByExpr";
    case QUERY_NODE_LIMIT:
      return "Limit";
    case QUERY_NODE_STATE_WINDOW:
      return "StateWindow";
    case QUERY_NODE_SESSION_WINDOW:
      return "SessionWinow";
    case QUERY_NODE_INTERVAL_WINDOW:
      return "IntervalWindow";
    case QUERY_NODE_NODE_LIST:
      return "NodeList";
    case QUERY_NODE_FILL:
      return "Fill";
    case QUERY_NODE_COLUMN_REF:
      return "ColumnRef";
    case QUERY_NODE_TARGET:
      return "Target";
    case QUERY_NODE_RAW_EXPR:
      return "RawExpr";
    case QUERY_NODE_TUPLE_DESC:
      return "TupleDesc";
    case QUERY_NODE_SLOT_DESC:
      return "SlotDesc";
    case QUERY_NODE_SET_OPERATOR:
      return "SetOperator";
    case QUERY_NODE_SELECT_STMT:
      return "SelectStmt";
    case QUERY_NODE_SHOW_STMT:
      return "ShowStmt";
    case QUERY_NODE_LOGIC_PLAN_SCAN:
      return "LogicScan";
    case QUERY_NODE_LOGIC_PLAN_JOIN:
      return "LogicJoin";
    case QUERY_NODE_LOGIC_PLAN_AGG:
      return "LogicAgg";
    case QUERY_NODE_LOGIC_PLAN_PROJECT:
      return "LogicProject";
    case QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN:
      return "PhysiTagScan";
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN:
      return "PhysiTableScan";
    case QUERY_NODE_PHYSICAL_PLAN_PROJECT:
      return "PhysiProject";
    default:
      break;
  }
  static char tmp[20];
  snprintf(tmp, sizeof(tmp), "Unknown %d", type);
  return tmp;
}

static int32_t addNodeList(SJson* pJson, const char* pName, FToJson func, const SNodeList* pList) {
  if (LIST_LENGTH(pList) > 0) {
    SJson* jList = tjsonAddArrayToObject(pJson, pName);
    if (NULL == jList) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    SNode* pNode;
    FOREACH(pNode, pList) {
      int32_t code = tjsonAddItem(jList, func, pNode);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

static const char* jkTableMetaUid = "TableMetaUid";
static const char* jkTableMetaSuid = "TableMetaSuid";

static int32_t tableMetaToJson(const void* pObj, SJson* pJson) {
  const STableMeta* pNode = (const STableMeta*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkTableMetaUid, pNode->uid);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableMetaSuid, pNode->suid);
  }

  return code;
}

static const char* jkLogicPlanId = "Id";
static const char* jkLogicPlanTargets = "Targets";
static const char* jkLogicPlanConditions = "Conditions";
static const char* jkLogicPlanChildren = "Children";

static int32_t logicPlanNodeToJson(const void* pObj, SJson* pJson) {
  const SLogicNode* pNode = (const SLogicNode*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkLogicPlanId, pNode->id);
  if (TSDB_CODE_SUCCESS == code) {
    code = addNodeList(pJson, jkLogicPlanTargets, nodeToJson, pNode->pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkLogicPlanConditions, nodeToJson, pNode->pConditions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = addNodeList(pJson, jkLogicPlanChildren, nodeToJson, pNode->pChildren);
  }

  return code;
}

static const char* jkScanLogicPlanScanCols = "ScanCols";
static const char* jkScanLogicPlanTableMeta = "TableMeta";

static int32_t logicScanNodeToJson(const void* pObj, SJson* pJson) {
  const SScanLogicNode* pNode = (const SScanLogicNode*)pObj;

  int32_t code = logicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = addNodeList(pJson, jkScanLogicPlanScanCols, nodeToJson, pNode->pScanCols);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkScanLogicPlanTableMeta, tableMetaToJson, pNode->pMeta);
  }

  return code;
}

static const char* jkProjectLogicPlanProjections = "Projections";

static int32_t logicProjectNodeToJson(const void* pObj, SJson* pJson) {
  const SProjectLogicNode* pNode = (const SProjectLogicNode*)pObj;

  int32_t code = logicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = addNodeList(pJson, jkProjectLogicPlanProjections, nodeToJson, pNode->pProjections);
  }

  return code;
}

static const char* jkJoinLogicPlanJoinType = "JoinType";
static const char* jkJoinLogicPlanOnConditions = "OnConditions";

static int32_t logicJoinNodeToJson(const void* pObj, SJson* pJson) {
  const SJoinLogicNode* pNode = (const SJoinLogicNode*)pObj;

  int32_t code = logicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkJoinLogicPlanJoinType, pNode->joinType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkJoinLogicPlanOnConditions, nodeToJson, pNode->pOnConditions);
  }

  return code;
}

static const char* jkPhysiPlanOutputTuple = "OutputTuple";
static const char* jkPhysiPlanConditions = "Conditions";
static const char* jkPhysiPlanChildren = "Children";

static int32_t physicPlanNodeToJson(const void* pObj, SJson* pJson) {
  const SPhysiNode* pNode = (const SPhysiNode*)pObj;

  int32_t code = tjsonAddObject(pJson, jkPhysiPlanOutputTuple, nodeToJson, &pNode->outputTuple);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkPhysiPlanConditions, nodeToJson, pNode->pConditions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = addNodeList(pJson, jkPhysiPlanChildren, nodeToJson, pNode->pChildren);
  }

  return code;
}

static const char* jkScanPhysiPlanScanCols = "ScanCols";
static const char* jkScanPhysiPlanTableId = "TableId";
static const char* jkScanPhysiPlanTableType = "TableType";
static const char* jkScanPhysiPlanScanOrder = "ScanOrder";
static const char* jkScanPhysiPlanScanCount = "ScanCount";
static const char* jkScanPhysiPlanReverseScanCount = "ReverseScanCount";

static int32_t physiScanNodeToJson(const void* pObj, SJson* pJson) {
  const STagScanPhysiNode* pNode = (const STagScanPhysiNode*)pObj;

  int32_t code = physicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = addNodeList(pJson, jkScanPhysiPlanScanCols, nodeToJson, pNode->pScanCols);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkScanPhysiPlanTableId, pNode->uid);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkScanPhysiPlanTableType, pNode->tableType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkScanPhysiPlanScanOrder, pNode->order);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkScanPhysiPlanScanCount, pNode->count);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkScanPhysiPlanReverseScanCount, pNode->reverse);
  }

  return code;
}

static int32_t physiTagScanNodeToJson(const void* pObj, SJson* pJson) {
  return physiScanNodeToJson(pObj, pJson);
}

static const char* jkTableScanPhysiPlanScanFlag = "ScanFlag";
static const char* jkTableScanPhysiPlanStartKey = "StartKey";
static const char* jkTableScanPhysiPlanEndKey = "EndKey";

static int32_t physiTableScanNodeToJson(const void* pObj, SJson* pJson) {
  const STableScanPhysiNode* pNode = (const STableScanPhysiNode*)pObj;

  int32_t code = physiScanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableScanPhysiPlanScanFlag, pNode->scanFlag);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableScanPhysiPlanStartKey, pNode->scanRange.skey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableScanPhysiPlanEndKey, pNode->scanRange.ekey);
  }

  return code;
}

static const char* jkProjectPhysiPlanProjections = "Projections";

static int32_t physiProjectNodeToJson(const void* pObj, SJson* pJson) {
  const SProjectPhysiNode* pNode = (const SProjectPhysiNode*)pObj;

  int32_t code = physicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = addNodeList(pJson, jkProjectPhysiPlanProjections, nodeToJson, pNode->pProjections);
  }

  return code;
}

static const char* jkAggLogicPlanGroupKeys = "GroupKeys";
static const char* jkAggLogicPlanAggFuncs = "AggFuncs";

static int32_t logicAggNodeToJson(const void* pObj, SJson* pJson) {
  const SAggLogicNode* pNode = (const SAggLogicNode*)pObj;

  int32_t code = logicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = addNodeList(pJson, jkAggLogicPlanGroupKeys, nodeToJson, pNode->pGroupKeys);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = addNodeList(pJson, jkAggLogicPlanAggFuncs, nodeToJson, pNode->pAggFuncs);
  }

  return code;
}

static const char* jkDataTypeType = "Type";
static const char* jkDataTypePrecision = "Precision";
static const char* jkDataTypeScale = "Scale";
static const char* jkDataTypeDataBytes = "Bytes";

static int32_t dataTypeToJson(const void* pObj, SJson* pJson) {
  const SDataType* pNode = (const SDataType*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkDataTypeType, pNode->type);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDataTypePrecision, pNode->precision);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDataTypeScale, pNode->scale);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDataTypeDataBytes, pNode->bytes);
  }

  return code;
}

static const char* jkExprDataType = "DataType";
static const char* jkExprAliasName = "AliasName";

static int32_t exprNodeToJson(const void* pObj, SJson* pJson) {
  const SExprNode* pNode = (const SExprNode*)pObj;

  int32_t code = tjsonAddObject(pJson, jkExprDataType, dataTypeToJson, &pNode->resType);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkExprAliasName, pNode->aliasName);
  }

  return code;
}

static const char* jkColumnTableId = "TableId";
static const char* jkColumnColId = "ColId";
static const char* jkColumnColType = "ColType";
static const char* jkColumnDbName = "DbName";
static const char* jkColumnTableName = "TableName";
static const char* jkColumnTableAlias = "TableAlias";
static const char* jkColumnColName = "ColName";

static int32_t columnNodeToJson(const void* pObj, SJson* pJson) {
  const SColumnNode* pNode = (const SColumnNode*)pObj;

  int32_t code = exprNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkColumnTableId, pNode->tableId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkColumnColId, pNode->colId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkColumnColType, pNode->colType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkColumnDbName, pNode->dbName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkColumnTableName, pNode->tableName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkColumnTableAlias, pNode->tableAlias);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkColumnColName, pNode->colName);
  }

  return code;
}

static const char* jkValueLiteral = "Literal";
static const char* jkValueDuration = "Duration";
static const char* jkValueDatum = "Datum";

static int32_t valueNodeToJson(const void* pObj, SJson* pJson) {
  const SValueNode* pNode = (const SValueNode*)pObj;

  int32_t code = exprNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkValueLiteral, pNode->literal);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkValueDuration, pNode->isDuration);
  }
  switch (pNode->node.resType.type) {
    case TSDB_DATA_TYPE_NULL:
      break;
    case TSDB_DATA_TYPE_BOOL:
      code = tjsonAddIntegerToObject(pJson, jkValueDuration, pNode->datum.b);
      break;
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      code = tjsonAddIntegerToObject(pJson, jkValueDuration, pNode->datum.i);
      break;
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT:
      code = tjsonAddIntegerToObject(pJson, jkValueDuration, pNode->datum.u);
      break;
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE:
      code = tjsonAddDoubleToObject(pJson, jkValueDuration, pNode->datum.d);
      break;
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY:
      code = tjsonAddStringToObject(pJson, jkValueLiteral, pNode->datum.p);
      break;
    case TSDB_DATA_TYPE_JSON:
    case TSDB_DATA_TYPE_DECIMAL:
    case TSDB_DATA_TYPE_BLOB:
      // todo
    default:
      break;
  }

  return code;
}

static const char* jkOperatorType = "OpType";
static const char* jkOperatorLeft = "Left";
static const char* jkOperatorRight = "Right";

static int32_t operatorNodeToJson(const void* pObj, SJson* pJson) {
  const SOperatorNode* pNode = (const SOperatorNode*)pObj;

  int32_t code = exprNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkOperatorType, pNode->opType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkOperatorLeft, nodeToJson, pNode->pLeft);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkOperatorRight, nodeToJson, pNode->pRight);
  }

  return code;
}

static const char* jkLogicCondType = "CondType";
static const char* jkLogicCondParameters = "Parameters";

static int32_t logicConditionNodeToJson(const void* pObj, SJson* pJson) {
  const SLogicConditionNode* pNode = (const SLogicConditionNode*)pObj;

  int32_t code = exprNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkLogicCondType, pNode->condType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = addNodeList(pJson, jkLogicCondParameters, nodeToJson, pNode->pParameterList);
  }

  return code;
}

static const char* jkFunctionName = "Name";
static const char* jkFunctionId = "Id";
static const char* jkFunctionType = "Type";
static const char* jkFunctionParameter = "Parameters";

static int32_t functionNodeToJson(const void* pObj, SJson* pJson) {
  const SFunctionNode* pNode = (const SFunctionNode*)pObj;

  int32_t code = exprNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkFunctionName, pNode->functionName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkFunctionId, pNode->funcId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkFunctionType, pNode->funcType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = addNodeList(pJson, jkFunctionParameter, nodeToJson, pNode->pParameterList);
  }

  return code;
}

static const char* jkGroupingSetType = "GroupingSetType";
static const char* jkGroupingSetParameter = "Parameters";

static int32_t groupingSetNodeToJson(const void* pObj, SJson* pJson) {
  const SGroupingSetNode* pNode = (const SGroupingSetNode*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkGroupingSetType, pNode->groupingSetType);
  if (TSDB_CODE_SUCCESS == code) {
    code = addNodeList(pJson, jkGroupingSetParameter, nodeToJson, pNode->pParameterList);
  }

  return code;
}

static const char* jkColumnRefDataType = "DataType";
static const char* jkColumnRefTupleId = "TupleId";
static const char* jkColumnRefSlotId = "SlotId";
static const char* jkColumnRefColumnId = "ColumnId";

static int32_t columnRefNodeToJson(const void* pObj, SJson* pJson) {
  const SColumnRefNode* pNode = (const SColumnRefNode*)pObj;

  int32_t code = tjsonAddObject(pJson, jkColumnRefDataType, dataTypeToJson, &pNode->dataType);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkColumnRefTupleId, pNode->tupleId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkColumnRefSlotId, pNode->slotId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkColumnRefColumnId, pNode->columnId);
  }

  return code;
}

static const char* jkTargetTupleId = "TupleId";
static const char* jkTargetSlotId = "SlotId";
static const char* jkTargetExpr = "Expr";

static int32_t targetNodeToJson(const void* pObj, SJson* pJson) {
  const STargetNode* pNode = (const STargetNode*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkTargetTupleId, pNode->tupleId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTargetSlotId, pNode->slotId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkTargetExpr, nodeToJson, pNode->pExpr);
  }

  return code;
}

static const char* jkSlotDescSlotId = "SlotId";
static const char* jkSlotDescDataType = "DataType";

static int32_t slotDescNodeToJson(const void* pObj, SJson* pJson) {
  const SSlotDescNode* pNode = (const SSlotDescNode*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkSlotDescSlotId, pNode->slotId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkSlotDescDataType, dataTypeToJson, &pNode->dataType);
  }

  return code;
}

static const char* jkTupleDescTupleId = "TupleId";
static const char* jkTupleDescSlots = "Slots";

static int32_t tupleDescNodeToJson(const void* pObj, SJson* pJson) {
  const STupleDescNode* pNode = (const STupleDescNode*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkTupleDescTupleId, pNode->tupleId);
  if (TSDB_CODE_SUCCESS == code) {
    code = addNodeList(pJson, jkTupleDescSlots, nodeToJson, pNode->pSlots);
  }

  return code;
}

static const char* jkSelectStmtDistinct = "Distinct";
static const char* jkSelectStmtProjections = "Projections";
static const char* jkSelectStmtFrom = "From";
static const char* jkSelectStmtWhere = "Where";
static const char* jkSelectStmtPartitionBy = "PartitionBy";
static const char* jkSelectStmtWindow = "Window";
static const char* jkSelectStmtGroupBy = "GroupBy";
static const char* jkSelectStmtHaving = "Having";
static const char* jkSelectStmtOrderBy = "OrderBy";
static const char* jkSelectStmtLimit = "Limit";
static const char* jkSelectStmtSlimit = "Slimit";

static int32_t selectStmtTojson(const void* pObj, SJson* pJson) {
  const SSelectStmt* pNode = (const SSelectStmt*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkSelectStmtDistinct, pNode->isDistinct);
  if (TSDB_CODE_SUCCESS == code) {
    code = addNodeList(pJson, jkSelectStmtProjections, nodeToJson, pNode->pProjectionList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkSelectStmtFrom, nodeToJson, pNode->pFromTable);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkSelectStmtWhere, nodeToJson, pNode->pWhere);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = addNodeList(pJson, jkSelectStmtPartitionBy, nodeToJson, pNode->pPartitionByList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkSelectStmtWindow, nodeToJson, pNode->pWindow);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = addNodeList(pJson, jkSelectStmtGroupBy, nodeToJson, pNode->pGroupByList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkSelectStmtHaving, nodeToJson, pNode->pHaving);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = addNodeList(pJson, jkSelectStmtOrderBy, nodeToJson, pNode->pOrderByList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkSelectStmtLimit, nodeToJson, pNode->pLimit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkSelectStmtSlimit, nodeToJson, pNode->pSlimit);
  }

  return code;
}

static int32_t specificNodeToJson(const void* pObj, SJson* pJson) {
  switch (nodeType(pObj)) {
    case QUERY_NODE_COLUMN:
      return columnNodeToJson(pObj, pJson);
    case QUERY_NODE_VALUE:
      return valueNodeToJson(pObj, pJson);
    case QUERY_NODE_OPERATOR:
      return operatorNodeToJson(pObj, pJson);
    case QUERY_NODE_LOGIC_CONDITION:
      return logicConditionNodeToJson(pObj, pJson);
    case QUERY_NODE_FUNCTION:
      return functionNodeToJson(pObj, pJson);
    case QUERY_NODE_REAL_TABLE:
    case QUERY_NODE_TEMP_TABLE:
    case QUERY_NODE_JOIN_TABLE:
      break;
    case QUERY_NODE_GROUPING_SET:
      return groupingSetNodeToJson(pObj, pJson);
    case QUERY_NODE_ORDER_BY_EXPR:
    case QUERY_NODE_LIMIT:
    case QUERY_NODE_STATE_WINDOW:
    case QUERY_NODE_SESSION_WINDOW:
    case QUERY_NODE_INTERVAL_WINDOW:
    case QUERY_NODE_NODE_LIST:
    case QUERY_NODE_FILL:
    case QUERY_NODE_COLUMN_REF:
      return columnRefNodeToJson(pObj, pJson);
    case QUERY_NODE_TARGET:
      return targetNodeToJson(pObj, pJson);
    case QUERY_NODE_RAW_EXPR:
      break;
    case QUERY_NODE_TUPLE_DESC:
      return tupleDescNodeToJson(pObj, pJson);
    case QUERY_NODE_SLOT_DESC:
      return slotDescNodeToJson(pObj, pJson);
    case QUERY_NODE_SET_OPERATOR:
      break;
    case QUERY_NODE_SELECT_STMT:
      return selectStmtTojson(pObj, pJson);
    case QUERY_NODE_SHOW_STMT:
      break;
    case QUERY_NODE_LOGIC_PLAN_SCAN:
      return logicScanNodeToJson(pObj, pJson);
    case QUERY_NODE_LOGIC_PLAN_JOIN:
      return logicJoinNodeToJson(pObj, pJson);
    case QUERY_NODE_LOGIC_PLAN_AGG:
      return logicAggNodeToJson(pObj, pJson);
    case QUERY_NODE_LOGIC_PLAN_PROJECT:
      return logicProjectNodeToJson(pObj, pJson);
    case QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN:
      return physiTagScanNodeToJson(pObj, pJson);
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN:
      return physiTableScanNodeToJson(pObj, pJson);
    case QUERY_NODE_PHYSICAL_PLAN_PROJECT:
      return physiProjectNodeToJson(pObj, pJson);
    default:
      break;
  }
  return TSDB_CODE_SUCCESS;
}

static const char* jkNodeType = "Type";
static int32_t nodeToJson(const void* pObj, SJson* pJson) {
  const SNode* pNode = (const SNode*)pObj;

  char* pNodeName = nodeName(nodeType(pNode));
  int32_t code = tjsonAddStringToObject(pJson, jkNodeType, pNodeName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, pNodeName, specificNodeToJson, pNode);
  }

  return code;
}

int32_t nodesNodeToString(const SNode* pNode, bool format, char** pStr, int32_t* pLen) {
  if (NULL == pNode || NULL == pStr || NULL == pLen) {
    return TSDB_CODE_SUCCESS;
  }

  SJson* pJson = tjsonCreateObject();
  if (NULL == pJson) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = nodeToJson(pNode, pJson);
  if (TSDB_CODE_SUCCESS != code) {
    terrno = code;
    return code;
  }

  *pStr = format ? tjsonToString(pJson) : tjsonToUnformattedString(pJson);
  tjsonDelete(pJson);

  *pLen = strlen(*pStr) + 1;
  return TSDB_CODE_SUCCESS;
}

int32_t nodesStringToNode(const char* pStr, SNode** pNode) {
  return TSDB_CODE_SUCCESS;
}
