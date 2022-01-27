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

#include "plannerInt.h"
#include "parser.h"
#include "cJSON.h"

typedef bool (*FToJson)(const void* obj, cJSON* json); 
typedef bool (*FFromJson)(const cJSON* json, void* obj); 

static char* getString(const cJSON* json, const char* name) {
  char* p = cJSON_GetStringValue(cJSON_GetObjectItem(json, name));
  return strdup(p);
}

static void copyString(const cJSON* json, const char* name, char* dst) {
  strcpy(dst, cJSON_GetStringValue(cJSON_GetObjectItem(json, name)));
}

static uint64_t getBigintFromString(const cJSON* json, const char* name) {
  char* val = getString(json, name);
  uint64_t intVal = strtoul(val, NULL, 10);
  tfree(val);

  return intVal;
}

static int64_t getNumber(const cJSON* json, const char* name) {
  double d = cJSON_GetNumberValue(cJSON_GetObjectItem(json, name));
  return (int64_t) d;
}

static bool addObject(cJSON* json, const char* name, FToJson func, const void* obj) {
  if (NULL == obj) {
    return true;
  }

  cJSON* jObj = cJSON_CreateObject();
  if (NULL == jObj || !func(obj, jObj)) {
    cJSON_Delete(jObj);
    return false;
  }
  return cJSON_AddItemToObject(json, name, jObj);
}

static bool addItem(cJSON* json, FToJson func, const void* obj) {
  cJSON* jObj = cJSON_CreateObject();
  if (NULL == jObj || !func(obj, jObj)) {
    cJSON_Delete(jObj);
    return false;
  }
  return cJSON_AddItemToArray(json, jObj);
}

static bool fromObject(const cJSON* json, const char* name, FFromJson func, void* obj, bool required) {
  cJSON* jObj = cJSON_GetObjectItem(json, name);
  if (NULL == jObj) {
    return !required;
  }
  return func(jObj, obj);
}

static bool fromObjectWithAlloc(const cJSON* json, const char* name, FFromJson func, void** obj, int32_t size, bool required) {
  cJSON* jObj = cJSON_GetObjectItem(json, name);
  if (NULL == jObj) {
    return !required;
  }
  *obj = calloc(1, size);
  if (NULL == *obj) {
    return false;
  }
  return func(jObj, *obj);
}

static const char* jkPnodeType = "Type";
static int32_t getPnodeTypeSize(cJSON* json) {
  switch (getNumber(json, jkPnodeType)) {
    case OP_StreamScan:
    case OP_TableScan:
    case OP_DataBlocksOptScan:
    case OP_TableSeqScan:
      return sizeof(STableScanPhyNode);
    case OP_TagScan:
      return sizeof(STagScanPhyNode);
    case OP_SystemTableScan:
      return sizeof(SSystemTableScanPhyNode);
    case OP_Aggregate:
      return sizeof(SAggPhyNode);
    case OP_Exchange:
      return sizeof(SExchangePhyNode);
    default:
      break;
  };
  return -1;
}

static bool fromPnode(const cJSON* json, const char* name, FFromJson func, void** obj) {
  cJSON* jObj = cJSON_GetObjectItem(json, name);
  if (NULL == jObj) {
    return true;
  }
  *obj = calloc(1, getPnodeTypeSize(jObj));
  if (NULL == *obj) {
    return false;
  }
  return func(jObj, *obj);
}

static bool fromPnodeArray(const cJSON* json, const char* name, FFromJson func, SArray** array) {
  const cJSON* jArray = cJSON_GetObjectItem(json, name);
  int32_t size = (NULL == jArray ? 0 : cJSON_GetArraySize(jArray));
  if (size > 0) {
    *array = taosArrayInit(size, POINTER_BYTES);
    if (NULL == *array) {
      return false;
    }
  }
  for (int32_t i = 0; i < size; ++i) {
    cJSON* jItem = cJSON_GetArrayItem(jArray, i);
    void* item = calloc(1, getPnodeTypeSize(jItem));
    if (NULL == item || !func(jItem, item)) {
      return false;
    }
    taosArrayPush(*array, &item);
  }
  return true;
}

static bool addTarray(cJSON* json, const char* name, FToJson func, const SArray* array, bool isPoint) {
  size_t size = (NULL == array) ? 0 : taosArrayGetSize(array);
  if (size > 0) {
    cJSON* jArray = cJSON_AddArrayToObject(json, name);
    if (NULL == jArray) {
      return false;
    }
    for (size_t i = 0; i < size; ++i) {
      if (!addItem(jArray, func, isPoint ? taosArrayGetP(array, i) : taosArrayGet(array, i))) {
        return false;
      }
    }
  }
  return true;
}

static bool addInlineArray(cJSON* json, const char* name, FToJson func, const SArray* array) {
  return addTarray(json, name, func, array, false);
}

static bool addArray(cJSON* json, const char* name, FToJson func, const SArray* array) {
  return addTarray(json, name, func, array, true);
}

static bool fromTarray(const cJSON* json, const char* name, FFromJson func, SArray** array, int32_t itemSize, bool isPoint) {
  const cJSON* jArray = cJSON_GetObjectItem(json, name);
  int32_t size = (NULL == jArray ? 0 : cJSON_GetArraySize(jArray));
  if (size > 0) {
    *array = taosArrayInit(size, isPoint ? POINTER_BYTES : itemSize);
    if (NULL == *array) {
      return false;
    }
  }
  for (int32_t i = 0; i < size; ++i) {
    void* item = calloc(1, itemSize);
    if (NULL == item || !func(cJSON_GetArrayItem(jArray, i), item)) {
      return false;
    }
    taosArrayPush(*array, isPoint ? &item : item);
  }
  return true;
}

static bool fromInlineArray(const cJSON* json, const char* name, FFromJson func, SArray** array, int32_t itemSize) {
  return fromTarray(json, name, func, array, itemSize, false);
}

static bool fromArray(const cJSON* json, const char* name, FFromJson func, SArray** array, int32_t itemSize) {
  return fromTarray(json, name, func, array, itemSize, true);
}

static bool addRawArray(cJSON* json, const char* name, FToJson func, const void* array, int32_t itemSize, int32_t size) {
  if (size > 0) {
    cJSON* jArray = cJSON_AddArrayToObject(json, name);
    if (NULL == jArray) {
      return false;
    }
    for (size_t i = 0; i < size; ++i) {
      if (!addItem(jArray, func, (const char*)array + itemSize * i)) {
        return false;
      }
    }
  }
  return true;
}

static const cJSON* getArray(const cJSON* json, const char* name, int32_t* size) {
  const cJSON* jArray = cJSON_GetObjectItem(json, name);
  *size = (NULL == jArray ? 0 : cJSON_GetArraySize(jArray));
  return jArray;
}

static bool fromItem(const cJSON* jArray, FFromJson func, void* array, int32_t itemSize, int32_t size) {
  for (int32_t i = 0; i < size; ++i) {
    if (!func(cJSON_GetArrayItem(jArray, i), (char*)array + itemSize * i)) {
      return false;
    }
  }
  return true;
}

static bool fromRawArrayWithAlloc(const cJSON* json, const char* name, FFromJson func, void** array, int32_t itemSize, int32_t* size) {
  const cJSON* jArray = getArray(json, name, size);
  if (*size > 0) {
    *array = calloc(1, itemSize * (*size));
    if (NULL == *array) {
      return false;
    }
  }
  return fromItem(jArray, func, *array, itemSize, *size);
}

static bool fromRawArray(const cJSON* json, const char* name, FFromJson func, void* array, int32_t itemSize, int32_t* size) {
  const cJSON* jArray = getArray(json, name, size);
  return fromItem(jArray, func, array, itemSize, *size);
}

static const char* jkSchemaType = "Type";
static const char* jkSchemaColId = "ColId";
static const char* jkSchemaBytes = "Bytes";
// The 'name' field do not need to be serialized.
static bool schemaToJson(const void* obj, cJSON* jSchema) {
  const SSlotSchema* schema = (const SSlotSchema*)obj;
  bool res = cJSON_AddNumberToObject(jSchema, jkSchemaType, schema->type);
  if (res) {
    res = cJSON_AddNumberToObject(jSchema, jkSchemaColId, schema->colId);
  }
  if (res) {
    res = cJSON_AddNumberToObject(jSchema, jkSchemaBytes, schema->bytes);
  }
  return res;
}

static bool schemaFromJson(const cJSON* json, void* obj) {
  SSlotSchema* schema = (SSlotSchema*)obj;
  schema->type = getNumber(json, jkSchemaType);
  schema->colId = getNumber(json, jkSchemaColId);
  schema->bytes = getNumber(json, jkSchemaBytes);
  return true;
}

static const char* jkDataBlockSchemaSlotSchema = "SlotSchema";
static const char* jkDataBlockSchemaResultRowSize = "resultRowSize";
static const char* jkDataBlockSchemaPrecision = "Precision";

static bool dataBlockSchemaToJson(const void* obj, cJSON* json) {
  const SDataBlockSchema* schema = (const SDataBlockSchema*)obj;
  bool res = addRawArray(json, jkDataBlockSchemaSlotSchema, schemaToJson, schema->pSchema, sizeof(SSlotSchema), schema->numOfCols);
  if (res) {
    res = cJSON_AddNumberToObject(json, jkDataBlockSchemaResultRowSize, schema->resultRowSize);
  }
  if (res) {
    res = cJSON_AddNumberToObject(json, jkDataBlockSchemaPrecision, schema->precision);
  }
  return res;
}

static bool dataBlockSchemaFromJson(const cJSON* json, void* obj) {
  SDataBlockSchema* schema = (SDataBlockSchema*)obj;
  schema->resultRowSize = getNumber(json, jkDataBlockSchemaResultRowSize);
  schema->precision = getNumber(json, jkDataBlockSchemaPrecision);

  return fromRawArrayWithAlloc(json, jkDataBlockSchemaSlotSchema, schemaFromJson, (void**)&(schema->pSchema), sizeof(SSlotSchema), &schema->numOfCols);
}

static const char* jkColumnFilterInfoLowerRelOptr = "LowerRelOptr";
static const char* jkColumnFilterInfoUpperRelOptr = "UpperRelOptr";
static const char* jkColumnFilterInfoFilterstr = "Filterstr";
static const char* jkColumnFilterInfoLowerBnd = "LowerBnd";
static const char* jkColumnFilterInfoUpperBnd = "UpperBnd";

static bool columnFilterInfoToJson(const void* obj, cJSON* jFilter) {
  const SColumnFilterInfo* filter = (const SColumnFilterInfo*)obj;
  bool res = cJSON_AddNumberToObject(jFilter, jkColumnFilterInfoLowerRelOptr, filter->lowerRelOptr);
  if (res) {
    res = cJSON_AddNumberToObject(jFilter, jkColumnFilterInfoUpperRelOptr, filter->upperRelOptr);
  }
  if (res) {
    res = cJSON_AddNumberToObject(jFilter, jkColumnFilterInfoFilterstr, filter->filterstr);
  }
  if (res) {
    res = cJSON_AddNumberToObject(jFilter, jkColumnFilterInfoLowerBnd, filter->lowerBndd);
  }
  if (res) {
    res = cJSON_AddNumberToObject(jFilter, jkColumnFilterInfoUpperBnd, filter->upperBndd);
  }
  return res;
}

static bool columnFilterInfoFromJson(const cJSON* json, void* obj) {
  SColumnFilterInfo* filter = (SColumnFilterInfo*)obj;
  filter->lowerRelOptr = getNumber(json, jkColumnFilterInfoLowerRelOptr);
  filter->upperRelOptr = getNumber(json, jkColumnFilterInfoUpperRelOptr);
  filter->filterstr = getNumber(json, jkColumnFilterInfoFilterstr);
  filter->lowerBndd = getNumber(json, jkColumnFilterInfoLowerBnd);
  filter->upperBndd = getNumber(json, jkColumnFilterInfoUpperBnd);
  return true;
}

static const char* jkColumnInfoColId = "ColId";
static const char* jkColumnInfoType = "Type";
static const char* jkColumnInfoBytes = "Bytes";
static const char* jkColumnInfoFilterList = "FilterList";

static bool columnInfoToJson(const void* obj, cJSON* jCol) {
  const SColumnInfo* col = (const SColumnInfo*)obj;
  bool res = cJSON_AddNumberToObject(jCol, jkColumnInfoColId, col->colId);
  if (res) {
    res = cJSON_AddNumberToObject(jCol, jkColumnInfoType, col->type);
  }
  if (res) {
    res = cJSON_AddNumberToObject(jCol, jkColumnInfoBytes, col->bytes);
  }

  if (res) { // TODO: temporarily disable it
//    res = addRawArray(jCol, jkColumnInfoFilterList, columnFilterInfoToJson, col->flist.filterInfo, sizeof(SColumnFilterInfo), col->flist.numOfFilters);
  }

  return res;
}

static bool columnInfoFromJson(const cJSON* json, void* obj) {
  SColumnInfo* col = (SColumnInfo*)obj;
  col->colId = getNumber(json, jkColumnInfoColId);
  col->type  = getNumber(json, jkColumnInfoType);
  col->bytes = getNumber(json, jkColumnInfoBytes);
  int32_t size = 0;
  bool res = fromRawArrayWithAlloc(json, jkColumnInfoFilterList, columnFilterInfoFromJson, (void**)&col->flist.filterInfo, sizeof(SColumnFilterInfo), &size);
  col->flist.numOfFilters = size;
  return res;
}

static const char* jkColumnTableId = "TableId";
static const char* jkColumnFlag = "Flag";
static const char* jkColumnInfo = "Info";

static bool columnToJson(const void* obj, cJSON* jCol) {
  const SColumn* col = (const SColumn*)obj;
  bool res = cJSON_AddNumberToObject(jCol, jkColumnTableId, col->uid);
  if (res) {
    res = cJSON_AddNumberToObject(jCol, jkColumnFlag, col->flag);
  }
  if (res) {
    res = addObject(jCol, jkColumnInfo, columnInfoToJson, &col->info);
  }
  return res;
}

static bool columnFromJson(const cJSON* json, void* obj) {
  SColumn* col = (SColumn*)obj;
  col->uid = getNumber(json, jkColumnTableId);
  col->flag = getNumber(json, jkColumnFlag);
  return fromObject(json, jkColumnInfo, columnInfoFromJson, &col->info, true);
}

static bool exprNodeToJson(const void* obj, cJSON* jExprInfo);
static bool exprNodeFromJson(const cJSON* json, void* obj);

static const char* jkExprNodeOper = "Oper";
static const char* jkExprNodeLeft = "Left";
static const char* jkExprNodeRight = "Right";

static bool operatorToJson(const void* obj, cJSON* jOper) {
  const tExprNode* exprInfo = (const tExprNode*)obj;
  bool res = cJSON_AddNumberToObject(jOper, jkExprNodeOper, exprInfo->_node.optr);
  if (res) {
    res = addObject(jOper, jkExprNodeLeft, exprNodeToJson, exprInfo->_node.pLeft);
  }
  if (res) {
    res = addObject(jOper, jkExprNodeRight, exprNodeToJson, exprInfo->_node.pRight);
  }
  return res;
}

static bool operatorFromJson(const cJSON* json, void* obj) {
  tExprNode* exprInfo = (tExprNode*)obj;
  exprInfo->_node.optr = getNumber(json, jkExprNodeOper);
  bool res = fromObject(json, jkExprNodeLeft, exprNodeFromJson, exprInfo->_node.pLeft, false);
  if (res) {
    res = fromObject(json, jkExprNodeRight, exprNodeFromJson, exprInfo->_node.pRight, false);
  }
  return res;
}

static const char* jkFunctionName = "Name";
static const char* jkFunctionChild = "Child";

static bool functionToJson(const void* obj, cJSON* jFunc) {
  const tExprNode* exprInfo = (const tExprNode*)obj;
  bool res = cJSON_AddStringToObject(jFunc, jkFunctionName, exprInfo->_function.functionName);
  if (res && NULL != exprInfo->_function.pChild) {
      res = addRawArray(jFunc, jkFunctionChild, exprNodeToJson, exprInfo->_function.pChild, sizeof(tExprNode*), exprInfo->_function.num);
  }
  return res;
}

static bool functionFromJson(const cJSON* json, void* obj) {
  tExprNode* exprInfo = (tExprNode*)obj;
  copyString(json, jkFunctionName, exprInfo->_function.functionName);
  exprInfo->_function.pChild = calloc(1, sizeof(tExprNode*));
  if (NULL == exprInfo->_function.pChild) {
    return false;
  }
  return fromRawArrayWithAlloc(json, jkFunctionChild, exprNodeFromJson, (void**)exprInfo->_function.pChild, sizeof(tExprNode*), &exprInfo->_function.num);
}

static const char* jkVariantType = "Type";
static const char* jkVariantLen = "Len";
static const char* jkVariantvalues = "values";
static const char* jkVariantValue = "Value";

static bool variantToJson(const void* obj, cJSON* jVar) {
  const SVariant* var = (const SVariant*)obj;
  bool res = cJSON_AddNumberToObject(jVar, jkVariantType, var->nType);
  if (res) {
    res = cJSON_AddNumberToObject(jVar, jkVariantLen, var->nLen);
  }
  if (res) {
    if (0/* in */) {
      res = addArray(jVar, jkVariantvalues, variantToJson, var->arr);
    } else if (IS_NUMERIC_TYPE(var->nType)) {
      res = cJSON_AddNumberToObject(jVar, jkVariantValue, var->d);
    } else {
      res = cJSON_AddStringToObject(jVar, jkVariantValue, var->pz);
    }
  }
  return res;
}

static bool variantFromJson(const cJSON* json, void* obj) {
  SVariant* var = (SVariant*)obj;
  var->nType = getNumber(json, jkVariantType);
  var->nLen = getNumber(json, jkVariantLen);
  if (0/* in */) {
    return fromArray(json, jkVariantvalues, variantFromJson, &var->arr, sizeof(SVariant));
  } else if (IS_NUMERIC_TYPE(var->nType)) {
    var->d = getNumber(json, jkVariantValue);
  } else {
    var->pz = getString(json, jkVariantValue);
  }
  return true;
}

static const char* jkExprNodeType = "Type";
static const char* jkExprNodeOperator = "Operator";
static const char* jkExprNodeFunction = "Function";
static const char* jkExprNodeColumn = "Column";
static const char* jkExprNodeValue = "Value";

static bool exprNodeToJson(const void* obj, cJSON* jExprInfo) {
  const tExprNode* exprInfo = *(const tExprNode**)obj;
  bool res = cJSON_AddNumberToObject(jExprInfo, jkExprNodeType, exprInfo->nodeType);
  if (res) {
    switch (exprInfo->nodeType) {
      case TEXPR_BINARYEXPR_NODE:
      case TEXPR_UNARYEXPR_NODE:
        res = addObject(jExprInfo, jkExprNodeOperator, operatorToJson, exprInfo);
        break;
      case TEXPR_FUNCTION_NODE:
        res = addObject(jExprInfo, jkExprNodeFunction, functionToJson, exprInfo);
        break;
      case TEXPR_COL_NODE:
        res = addObject(jExprInfo, jkExprNodeColumn, schemaToJson, exprInfo->pSchema);
        break;
      case TEXPR_VALUE_NODE:
        res = addObject(jExprInfo, jkExprNodeValue, variantToJson, exprInfo->pVal);
        break;
      default:
        res = false;
        break;
    }
  }
  return res;
}

static bool exprNodeFromJson(const cJSON* json, void* obj) {
  tExprNode* exprInfo = (tExprNode*)obj;
  exprInfo->nodeType = getNumber(json, jkExprNodeType);
  switch (exprInfo->nodeType) {
    case TEXPR_BINARYEXPR_NODE:
    case TEXPR_UNARYEXPR_NODE:
      return fromObject(json, jkExprNodeOperator, operatorFromJson, exprInfo, false);
    case TEXPR_FUNCTION_NODE:
      return fromObject(json, jkExprNodeFunction, functionFromJson, exprInfo, false);
    case TEXPR_COL_NODE:
      return fromObjectWithAlloc(json, jkExprNodeColumn, schemaFromJson, (void**)&exprInfo->pSchema, sizeof(SSchema), false);
    case TEXPR_VALUE_NODE:
      return fromObject(json, jkExprNodeValue, variantFromJson, exprInfo->pVal, false);
    default:
      break;
  }
  return false;
}

static const char* jkSqlExprSchema = "Schema";
static const char* jkSqlExprColumns = "Columns";
static const char* jkSqlExprInterBytes = "InterBytes";
static const char* jkSqlExprParams = "Params";
// token does not need to be serialized.
static bool sqlExprToJson(const void* obj, cJSON* jExpr) {
  const SSqlExpr* expr = (const SSqlExpr*)obj;
  bool res = addObject(jExpr, jkSqlExprSchema, schemaToJson, &expr->resSchema);
  if (res) {
    res = addRawArray(jExpr, jkSqlExprColumns, columnToJson, expr->pColumns, sizeof(SColumn), expr->numOfCols);
  }
  if (res) {
    res = cJSON_AddNumberToObject(jExpr, jkSqlExprInterBytes, expr->interBytes);
  }
  if (res) {
    res = addRawArray(jExpr, jkSqlExprParams, variantToJson, expr->param, sizeof(SVariant), expr->numOfParams);
  }
  return res;
}

static bool sqlExprFromJson(const cJSON* json, void* obj) {
  SSqlExpr* expr = (SSqlExpr*)obj;
  bool res = fromObject(json, jkSqlExprSchema, schemaFromJson, &expr->resSchema, false);
  if (res) {
    res = fromRawArrayWithAlloc(json, jkSqlExprColumns, columnFromJson, (void**)&expr->pColumns, sizeof(SColumn), &expr->numOfCols);
  }
  if (res) {
    expr->interBytes = getNumber(json, jkSqlExprInterBytes);
  }
  if (res) {
    int32_t size = 0;
    res = fromRawArray(json, jkSqlExprParams, variantFromJson, expr->param, sizeof(SVariant), &size);
    expr->numOfParams = size;
  }
  return res;
}

static const char* jkExprInfoBase = "Base";
static const char* jkExprInfoExpr = "Expr";

static bool exprInfoToJson(const void* obj, cJSON* jExprInfo) {
  const SExprInfo* exprInfo = (const SExprInfo*)obj;
  bool res = addObject(jExprInfo, jkExprInfoBase, sqlExprToJson, &exprInfo->base);
  if (res) {
    res = addObject(jExprInfo, jkExprInfoExpr, exprNodeToJson, &exprInfo->pExpr);
  }
  return res;
}

static bool exprInfoFromJson(const cJSON* json, void* obj) {
  SExprInfo* exprInfo = (SExprInfo*)obj;
  bool res = fromObject(json, jkExprInfoBase, sqlExprFromJson, &exprInfo->base, true);
  if (res) {
    res = fromObjectWithAlloc(json, jkExprInfoExpr, exprNodeFromJson, (void**)&exprInfo->pExpr, sizeof(tExprNode), true);
  }
  return res;
}

static const char* jkTimeWindowStartKey = "StartKey";
static const char* jkTimeWindowEndKey = "EndKey";

static bool timeWindowToJson(const void* obj, cJSON* json) {
  const STimeWindow* win = (const STimeWindow*)obj;

  char tmp[40] = {0};
  snprintf(tmp, tListLen(tmp),"%"PRId64, win->skey);

  bool res = cJSON_AddStringToObject(json, jkTimeWindowStartKey, tmp);
  if (res) {
    memset(tmp, 0, tListLen(tmp));
    snprintf(tmp, tListLen(tmp),"%"PRId64, win->ekey);
    res = cJSON_AddStringToObject(json, jkTimeWindowEndKey, tmp);
  }
  return res;
}

static bool timeWindowFromJson(const cJSON* json, void* obj) {
  STimeWindow* win = (STimeWindow*)obj;
  win->skey = getBigintFromString(json, jkTimeWindowStartKey);
  win->ekey = getBigintFromString(json, jkTimeWindowEndKey);
  return true;
}

static const char* jkScanNodeTableId = "TableId";
static const char* jkScanNodeTableType = "TableType";
static const char* jkScanNodeTableOrder = "Order";
static const char* jkScanNodeTableCount = "Count";
static const char* jkScanNodeTableRevCount = "Reverse";

static bool scanNodeToJson(const void* obj, cJSON* json) {
  const SScanPhyNode* pNode = (const SScanPhyNode*)obj;

  char uid[40] = {0};
  snprintf(uid, tListLen(uid), "%"PRIu64, pNode->uid);
  bool res = cJSON_AddStringToObject(json, jkScanNodeTableId, uid);

  if (res) {
    res = cJSON_AddNumberToObject(json, jkScanNodeTableType, pNode->tableType);
  }

  if (res) {
    res = cJSON_AddNumberToObject(json, jkScanNodeTableOrder, pNode->order);
  }

  if (res) {
    res = cJSON_AddNumberToObject(json, jkScanNodeTableCount, pNode->count);
  }

  if (res) {
    res = cJSON_AddNumberToObject(json, jkScanNodeTableRevCount, pNode->reverse);
  }

  return res;
}

static bool scanNodeFromJson(const cJSON* json, void* obj) {
  SScanPhyNode* pNode = (SScanPhyNode*)obj;

  pNode->uid       = getBigintFromString(json, jkScanNodeTableId);
  pNode->tableType = getNumber(json, jkScanNodeTableType);
  pNode->count     = getNumber(json, jkScanNodeTableCount);
  pNode->order     = getNumber(json, jkScanNodeTableOrder);
  pNode->reverse   = getNumber(json, jkScanNodeTableRevCount);
  return true;
}

static const char* jkColIndexColId = "ColId";
static const char* jkColIndexColIndex = "ColIndex";
static const char* jkColIndexFlag = "Flag";
static const char* jkColIndexName = "Name";

static bool colIndexToJson(const void* obj, cJSON* json) {
  const SColIndex* col = (const SColIndex*)obj;
  bool res = cJSON_AddNumberToObject(json, jkColIndexColId, col->colId);
  if (res) {
    res = cJSON_AddNumberToObject(json, jkColIndexColIndex, col->colIndex);
  }
  if (res) {
    res = cJSON_AddNumberToObject(json, jkColIndexFlag, col->flag);
  }
  if (res) {
    res = cJSON_AddStringToObject(json, jkColIndexName, col->name);
  }
  return res;
}

static bool colIndexFromJson(const cJSON* json, void* obj) {
  SColIndex* col = (SColIndex*)obj;
  col->colId = getNumber(json, jkColIndexColId);
  col->colIndex = getNumber(json, jkColIndexColIndex);
  col->flag = getNumber(json, jkColIndexFlag);
  copyString(json, jkColIndexName, col->name);
  return true;
}

static const char* jkAggNodeAggAlgo = "AggAlgo";
static const char* jkAggNodeAggSplit = "AggSplit";
static const char* jkAggNodeExprs = "Exprs";
static const char* jkAggNodeGroupByList = "GroupByList";

static bool aggNodeToJson(const void* obj, cJSON* json) {
  const SAggPhyNode* agg = (const SAggPhyNode*)obj;
  bool res = cJSON_AddNumberToObject(json, jkAggNodeAggAlgo, agg->aggAlgo);
  if (res) {
    res = cJSON_AddNumberToObject(json, jkAggNodeAggSplit, agg->aggSplit);
  }
  if (res) {
    res = addArray(json, jkAggNodeExprs, exprInfoToJson, agg->pExprs);
  }
  if (res) {
    res = addArray(json, jkAggNodeGroupByList, colIndexToJson, agg->pGroupByList);
  }
  return res;
}

static bool aggNodeFromJson(const cJSON* json, void* obj) {
  SAggPhyNode* agg = (SAggPhyNode*)obj;
  agg->aggAlgo = getNumber(json, jkAggNodeAggAlgo);
  agg->aggSplit = getNumber(json, jkAggNodeAggSplit);
  bool res = fromArray(json, jkAggNodeExprs, exprInfoFromJson, &agg->pExprs, sizeof(SExprInfo));
  if (res) {
    res = fromArray(json, jkAggNodeGroupByList, colIndexFromJson, &agg->pGroupByList, sizeof(SExprInfo));
  }
  return res;
}

static const char* jkTableScanNodeFlag = "Flag";
static const char* jkTableScanNodeWindow = "Window";
static const char* jkTableScanNodeTagsConditions = "TagsConditions";

static bool tableScanNodeToJson(const void* obj, cJSON* json) {
  const STableScanPhyNode* scan = (const STableScanPhyNode*)obj;
  bool res = scanNodeToJson(obj, json);
  if (res) {
    res = cJSON_AddNumberToObject(json, jkTableScanNodeFlag, scan->scanFlag);
  }
  if (res) {
    res = addObject(json, jkTableScanNodeWindow, timeWindowToJson, &scan->window);
  }
  if (res) {
    res = addArray(json, jkTableScanNodeTagsConditions, exprInfoToJson, scan->pTagsConditions);
  }
  return res;
}

static bool tableScanNodeFromJson(const cJSON* json, void* obj) {
  STableScanPhyNode* scan = (STableScanPhyNode*)obj;
  bool res = scanNodeFromJson(json, obj);
  if (res) {
    scan->scanFlag = getNumber(json, jkTableScanNodeFlag);
  }
  if (res) {
    res = fromObject(json, jkTableScanNodeWindow, timeWindowFromJson, &scan->window, true);
  }
  if (res) {
    res = fromArray(json, jkTableScanNodeTagsConditions, exprInfoFromJson, &scan->pTagsConditions, sizeof(SExprInfo));
  }
  return res;
}

static const char* jkEpAddrFqdn = "Fqdn";
static const char* jkEpAddrPort = "Port";

static bool epAddrToJson(const void* obj, cJSON* json) {
  const SEp* ep = (const SEp*)obj;
  bool res = cJSON_AddStringToObject(json, jkEpAddrFqdn, ep->fqdn);
  if (res) {
    res = cJSON_AddNumberToObject(json, jkEpAddrPort, ep->port);
  }
  return res;
}

static bool epAddrFromJson(const cJSON* json, void* obj) {
  SEp* ep = (SEp*)obj;
  copyString(json, jkEpAddrFqdn, ep->fqdn);
  ep->port = getNumber(json, jkEpAddrPort);
  return true;
}

static const char* jkNodeAddrId      = "NodeId";
static const char* jkNodeAddrInUse   = "InUse";
static const char* jkNodeAddrEpAddrs = "Ep";
static const char* jkNodeAddr        = "NodeAddr";
static const char* jkNodeTaskId      = "TaskId";
static const char* jkNodeTaskSchedId = "SchedId";

static bool queryNodeAddrToJson(const void* obj, cJSON* json) {
  const SQueryNodeAddr* pAddr = (const SQueryNodeAddr*) obj;
  bool res = cJSON_AddNumberToObject(json, jkNodeAddrId, pAddr->nodeId);

  if (res) {
    res = cJSON_AddNumberToObject(json, jkNodeAddrInUse, pAddr->epset.inUse);
  }

  if (res) {
    res = addRawArray(json, jkNodeAddrEpAddrs, epAddrToJson, pAddr->epset.eps, sizeof(SEp), pAddr->epset.numOfEps);
  }
  return res;
}

static bool queryNodeAddrFromJson(const cJSON* json, void* obj) {
  SQueryNodeAddr* pAddr = (SQueryNodeAddr*) obj;

  pAddr->nodeId = getNumber(json, jkNodeAddrId);
  pAddr->epset.inUse = getNumber(json, jkNodeAddrInUse);

  int32_t numOfEps = 0;
  bool res = fromRawArray(json, jkNodeAddrEpAddrs, epAddrFromJson, pAddr->epset.eps, sizeof(SEp), &numOfEps);
  pAddr->epset.numOfEps = numOfEps;
  return res;
}

static bool nodeAddrToJson(const void* obj, cJSON* json) {
  const SDownstreamSource* pSource = (const SDownstreamSource*) obj;
  bool res = cJSON_AddNumberToObject(json, jkNodeTaskId, pSource->taskId);

  if (res) {
    char t[30] = {0};
    snprintf(t, tListLen(t), "%"PRIu64, pSource->schedId);
    res = cJSON_AddStringToObject(json, jkNodeTaskSchedId, t);
  }

  if (res) {
    res = addObject(json, jkNodeAddr, queryNodeAddrToJson, &pSource->addr);
  }
  return res;
}

static bool nodeAddrFromJson(const cJSON* json, void* obj) {
  SDownstreamSource* pSource = (SDownstreamSource*)obj;
  pSource->taskId = getNumber(json, jkNodeTaskId);

  pSource->schedId = getBigintFromString(json, jkNodeTaskSchedId);
  bool res = fromObject(json, jkNodeAddr, queryNodeAddrFromJson, &pSource->addr, true);
  return res;
}

static const char* jkExchangeNodeSrcTemplateId = "SrcTemplateId";
static const char* jkExchangeNodeSrcEndPoints = "SrcAddrs";

static bool exchangeNodeToJson(const void* obj, cJSON* json) {
  const SExchangePhyNode* exchange = (const SExchangePhyNode*)obj;
  bool res = cJSON_AddNumberToObject(json, jkExchangeNodeSrcTemplateId, exchange->srcTemplateId);
  if (res) {
    res = addRawArray(json, jkExchangeNodeSrcEndPoints, nodeAddrToJson, exchange->pSrcEndPoints->pData, sizeof(SDownstreamSource), taosArrayGetSize(exchange->pSrcEndPoints));
  }
  return res;
}

static bool exchangeNodeFromJson(const cJSON* json, void* obj) {
  SExchangePhyNode* exchange = (SExchangePhyNode*)obj;
  exchange->srcTemplateId = getNumber(json, jkExchangeNodeSrcTemplateId);
  return fromInlineArray(json, jkExchangeNodeSrcEndPoints, nodeAddrFromJson, &exchange->pSrcEndPoints, sizeof(SDownstreamSource));
}

static bool specificPhyNodeToJson(const void* obj, cJSON* json) {
  const SPhyNode* phyNode = (const SPhyNode*)obj;
  switch (phyNode->info.type) {
    case OP_StreamScan:
    case OP_TableScan:
    case OP_DataBlocksOptScan:
    case OP_TableSeqScan:
      return tableScanNodeToJson(obj, json);
    case OP_TagScan:
    case OP_SystemTableScan:
      return scanNodeToJson(obj, json);
    case OP_Aggregate:
      return aggNodeToJson(obj, json);
    case OP_Project:
      return true;
    // case OP_Groupby:
    case OP_Limit:
    case OP_SLimit:
    case OP_TimeWindow:
    case OP_SessionWindow:
    case OP_StateWindow:
    case OP_Fill:
    case OP_MultiTableAggregate:
    case OP_MultiTableTimeInterval:
    case OP_Filter:
    case OP_Distinct:
    case OP_Join:
    case OP_AllTimeWindow:
    case OP_AllMultiTableTimeInterval:
    case OP_Order:
      break; // todo
    case OP_Exchange:
      return exchangeNodeToJson(obj, json);
    default:
      break;
  }
  return false;
}

static bool specificPhyNodeFromJson(const cJSON* json, void* obj) {
  SPhyNode* phyNode = (SPhyNode*)obj;
  switch (phyNode->info.type) {
    case OP_TableScan:
    case OP_StreamScan:
    case OP_DataBlocksOptScan:
    case OP_TableSeqScan:
      return tableScanNodeFromJson(json, obj);
    case OP_TagScan:
    case OP_SystemTableScan:
      return scanNodeFromJson(json, obj);
    case OP_Aggregate:
      return aggNodeFromJson(json, obj);
    case OP_Project:
      return true;
    // case OP_Groupby:
    case OP_Limit:
    case OP_SLimit:
    case OP_TimeWindow:
    case OP_SessionWindow:
    case OP_StateWindow:
    case OP_Fill:
    case OP_MultiTableAggregate:
    case OP_MultiTableTimeInterval:
    case OP_Filter:
    case OP_Distinct:
    case OP_Join:
    case OP_AllTimeWindow:
    case OP_AllMultiTableTimeInterval:
    case OP_Order:
      break; // todo
    case OP_Exchange:
      return exchangeNodeFromJson(json, obj);
    default:
      break;
  }
  return false;
}

static const char* jkPnodeName = "Name";
static const char* jkPnodeTargets = "Targets";
static const char* jkPnodeConditions = "Conditions";
static const char* jkPnodeSchema = "TargetSchema";
static const char* jkPnodeChildren = "Children";
// The 'pParent' field do not need to be serialized.
static bool phyNodeToJson(const void* obj, cJSON* jNode) {
  const SPhyNode* phyNode = (const SPhyNode*)obj;
  bool res = cJSON_AddNumberToObject(jNode, jkPnodeType, phyNode->info.type);
  if (res) {
    res = cJSON_AddStringToObject(jNode, jkPnodeName, phyNode->info.name);
  }
  if (res) {
    res = addArray(jNode, jkPnodeTargets, exprInfoToJson, phyNode->pTargets);
  }
  if (res) {
    res = addArray(jNode, jkPnodeConditions, exprInfoToJson, phyNode->pConditions);
  }
  if (res) {
    res = addObject(jNode, jkPnodeSchema, dataBlockSchemaToJson, &phyNode->targetSchema);
  }
  if (res) {
    res = addArray(jNode, jkPnodeChildren, phyNodeToJson, phyNode->pChildren);
  }
  if (res) {
    res = addObject(jNode, phyNode->info.name, specificPhyNodeToJson, phyNode);
  }
  return res;
}

static bool phyNodeFromJson(const cJSON* json, void* obj) {
  SPhyNode* node = (SPhyNode*) obj;

  node->info.type = getNumber(json, jkPnodeType);
  node->info.name = opTypeToOpName(node->info.type);

  bool res = fromArray(json, jkPnodeTargets, exprInfoFromJson, &node->pTargets, sizeof(SExprInfo));
  if (res) {
    res = fromArray(json, jkPnodeConditions, exprInfoFromJson, &node->pConditions, sizeof(SExprInfo));
  }
  if (res) {
    res = fromObject(json, jkPnodeSchema, dataBlockSchemaFromJson, &node->targetSchema, true);
  }
  if (res) {
    res = fromPnodeArray(json, jkPnodeChildren, phyNodeFromJson, &node->pChildren);
  }
  if (res) {
    res = fromObject(json, node->info.name, specificPhyNodeFromJson, node, true);
  }
  return res;
}

static const char* jkInserterNumOfTables = "NumOfTables";
static const char* jkInserterDataSize = "DataSize";

static bool inserterToJson(const void* obj, cJSON* json) {
  const SDataInserter* inserter = (const SDataInserter*)obj;
  bool res = cJSON_AddNumberToObject(json, jkInserterNumOfTables, inserter->numOfTables);
  if (res) {
    res = cJSON_AddNumberToObject(json, jkInserterDataSize, inserter->size);
  }
  // todo pData
  return res;
}

static bool inserterFromJson(const cJSON* json, void* obj) {
  SDataInserter* inserter = (SDataInserter*)obj;
  inserter->numOfTables = getNumber(json, jkInserterNumOfTables);
  inserter->size = getNumber(json, jkInserterDataSize);
  // todo pData
}

static bool specificDataSinkToJson(const void* obj, cJSON* json) {
  const SDataSink* dsink = (const SDataSink*)obj;
  switch (dsink->info.type) {
    case DSINK_Dispatch:
      return true;
    case DSINK_Insert:
      return inserterToJson(obj, json);
    default:
      break;
  }
  return false;
}

static bool specificDataSinkFromJson(const cJSON* json, void* obj) {
  SDataSink* dsink = (SDataSink*)obj;
  switch (dsink->info.type) {
    case DSINK_Dispatch:
      return true;
    case DSINK_Insert:
      return inserterFromJson(json, obj);
    default:
      break;
  }
  return false;
}

static const char* jkDataSinkName = "Name";
static const char* jkDataSinkSchema = "Schema";

static bool dataSinkToJson(const void* obj, cJSON* json) {
  const SDataSink* dsink = (const SDataSink*)obj;
  bool res = cJSON_AddStringToObject(json, jkDataSinkName, dsink->info.name);
  if (res) {
    res = addObject(json, dsink->info.name, specificDataSinkToJson, dsink);
  }
  if (res) {
    res = addObject(json, jkDataSinkSchema, dataBlockSchemaToJson, &dsink->schema);
  }
  return res;
}

static bool dataSinkFromJson(const cJSON* json, void* obj) {
  SDataSink* dsink = (SDataSink*)obj;
  dsink->info.name = getString(json, jkDataSinkName);
  dsink->info.type = dsinkNameToDsinkType(dsink->info.name);
  bool res = fromObject(json, jkDataSinkSchema, dataBlockSchemaFromJson, &dsink->schema, true);
  if (res) {
    res = fromObject(json, dsink->info.name, specificDataSinkFromJson, dsink, true);
  }
  return res;
}

static const char* jkIdQueryId = "QueryId";
static const char* jkIdTemplateId = "TemplateId";
static const char* jkIdSubplanId = "SubplanId";

static bool subplanIdToJson(const void* obj, cJSON* jId) {
  const SSubplanId* id = (const SSubplanId*)obj;

  char ids[40] = {0};
  snprintf(ids, tListLen(ids), "%"PRIu64, id->queryId);

  bool res = cJSON_AddStringToObject(jId, jkIdQueryId, ids);
  if (res) {
    res = cJSON_AddNumberToObject(jId, jkIdTemplateId, id->templateId);
  }
  if (res) {
    res = cJSON_AddNumberToObject(jId, jkIdSubplanId, id->subplanId);
  }
  return res;
}

static bool subplanIdFromJson(const cJSON* json, void* obj) {
  SSubplanId* id = (SSubplanId*)obj;

  id->queryId    = getBigintFromString(json, jkIdQueryId);
  id->templateId = getNumber(json, jkIdTemplateId);
  id->subplanId  = getNumber(json, jkIdSubplanId);
  return true;
}

static const char* jkSubplanId = "Id";
static const char* jkSubplanNode = "Node";
static const char* jkSubplanDataSink = "DataSink";

static cJSON* subplanToJson(const SSubplan* subplan) {
  cJSON* jSubplan = cJSON_CreateObject();
  if (NULL == jSubplan) {
    return NULL;
  }

  // The 'type', 'level', 'execEpSet', 'pChildren' and 'pParents' fields do not need to be serialized.
  bool res = addObject(jSubplan, jkSubplanId, subplanIdToJson, &subplan->id);
  if (res) {
    res = addObject(jSubplan, jkSubplanNode, phyNodeToJson, subplan->pNode);
  }
  if (res) {
    res = addObject(jSubplan, jkSubplanDataSink, dataSinkToJson, subplan->pDataSink);
  }
  if (!res) {
    cJSON_Delete(jSubplan);
    return NULL;
  }

  return jSubplan;
}

static SSubplan* subplanFromJson(const cJSON* json) {
  SSubplan* subplan = calloc(1, sizeof(SSubplan));
  if (NULL == subplan) {
    return NULL;
  }

  bool res = fromObject(json, jkSubplanId, subplanIdFromJson, &subplan->id, true);

  if (res) {
    res = fromPnode(json, jkSubplanNode, phyNodeFromJson, (void**)&subplan->pNode);
  }

  if (res) {
    res = fromObjectWithAlloc(json, jkSubplanDataSink, dataSinkFromJson, (void**)&subplan->pDataSink, sizeof(SDataSink), false);
  }
  
  if (!res) {
    qDestroySubplan(subplan);
    return NULL;
  }
  return subplan;
}

int32_t subPlanToString(const SSubplan* subplan, char** str, int32_t* len) {
  if (QUERY_TYPE_MODIFY == subplan->type) {
    SDataInserter* insert = (SDataInserter*)(subplan->pDataSink);
    *len = insert->size;
    *str = insert->pData;
    insert->pData = NULL;
    return TSDB_CODE_SUCCESS;
  }

  cJSON* json = subplanToJson(subplan);
  if (NULL == json) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return TSDB_CODE_FAILED;
  }

  *str = cJSON_Print(json);
  cJSON_Delete(json);

  *len = strlen(*str) + 1;
  return TSDB_CODE_SUCCESS;
}

int32_t stringToSubplan(const char* str, SSubplan** subplan) {
  cJSON* json = cJSON_Parse(str);
  if (NULL == json) {
    return TSDB_CODE_FAILED;
  }
  *subplan = subplanFromJson(json);
  return (NULL == *subplan ? TSDB_CODE_FAILED : TSDB_CODE_SUCCESS);
}

cJSON* qDagToJson(const SQueryDag* pDag) {
  cJSON* pRoot = cJSON_CreateObject();
  if(pRoot == NULL) {
    return NULL;
  }

  cJSON_AddNumberToObject(pRoot, "Number", pDag->numOfSubplans);
  cJSON_AddNumberToObject(pRoot, "QueryId", pDag->queryId);

  cJSON *pLevels = cJSON_CreateArray();
  if(pLevels == NULL) {
    cJSON_Delete(pRoot);
    return NULL;
  }

  cJSON_AddItemToObject(pRoot, "Subplans", pLevels);

  size_t level = taosArrayGetSize(pDag->pSubplans);
  for(size_t i = 0; i < level; i++) {
    const SArray* pSubplans = (const SArray*)taosArrayGetP(pDag->pSubplans, i);
    size_t num = taosArrayGetSize(pSubplans);
    cJSON* plansOneLevel = cJSON_CreateArray();
    if(plansOneLevel == NULL) {
      cJSON_Delete(pRoot);
      return NULL;
    }

    cJSON_AddItemToArray(pLevels, plansOneLevel);
    for(size_t j = 0; j < num; j++) {
      cJSON* pSubplan = subplanToJson((const SSubplan*)taosArrayGetP(pSubplans, j));
      if(pSubplan == NULL) {
        cJSON_Delete(pRoot);
        return NULL;
      }

      cJSON_AddItemToArray(plansOneLevel, pSubplan);
    }
  }
  return pRoot;
}

char* qDagToString(const SQueryDag* pDag) {
  cJSON* pRoot = qDagToJson(pDag);
  return cJSON_Print(pRoot);
}

SQueryDag* qJsonToDag(const cJSON* pRoot) {
  SQueryDag* pDag = malloc(sizeof(SQueryDag));
  if(pDag == NULL) {
    return NULL;
  }
  pDag->numOfSubplans = cJSON_GetNumberValue(cJSON_GetObjectItem(pRoot, "Number"));
  pDag->queryId = cJSON_GetNumberValue(cJSON_GetObjectItem(pRoot, "QueryId"));
  pDag->pSubplans = taosArrayInit(0, sizeof(SArray));
  if (pDag->pSubplans == NULL) {
    free(pDag);
    return NULL;
  }
  cJSON* pLevels = cJSON_GetObjectItem(pRoot, "Subplans");
  int level = cJSON_GetArraySize(pLevels);
  for(int i = 0; i < level; i++) {
    SArray* plansOneLevel = taosArrayInit(0, sizeof(void*));
    if(plansOneLevel == NULL) {
      for(int j = 0; j < i; j++) {
        taosArrayDestroy(taosArrayGetP(pDag->pSubplans, j));
      }
      taosArrayDestroy(pDag->pSubplans);
      free(pDag);
      return NULL;
    }
    cJSON* pItem = cJSON_GetArrayItem(pLevels, i);
    int sz = cJSON_GetArraySize(pItem);
    for(int j = 0; j < sz; j++) {
      cJSON* pSubplanJson = cJSON_GetArrayItem(pItem, j);
      SSubplan* pSubplan = subplanFromJson(pSubplanJson);
      taosArrayPush(plansOneLevel, &pSubplan);
    }
    taosArrayPush(pDag->pSubplans, plansOneLevel);
  }
  return pDag;
}

SQueryDag* qStringToDag(const char* pStr) {
  cJSON* pRoot = cJSON_Parse(pStr);
  return qJsonToDag(pRoot);
}
