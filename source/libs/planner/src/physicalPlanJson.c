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

typedef cJSON* (*FToObj)(const void* obj); 

static bool addObject(cJSON* json, const char* name, FToObj func, const void* obj) {
  if (NULL == obj) {
    return true;
  }

  cJSON* jObj = func(obj);
  if (NULL == jObj) {
    return false;
  }
  return cJSON_AddItemToObject(json, name, jObj);
}

static bool addItem(cJSON* json, FToObj func, const void* item) {
  cJSON* jItem = func(item);
  if (NULL == jItem) {
    return false;
  }
  return cJSON_AddItemToArray(json, jItem);
}

static bool addArray(cJSON* json, const char* name, FToObj func, const SArray* array) {
  size_t size = (NULL == array) ? 0 : taosArrayGetSize(array);
  if (size > 0) {
    cJSON* jArray = cJSON_AddArrayToObject(json, name);
    if (NULL == jArray) {
      return false;
    }
    for (size_t i = 0; i < size; ++i) {
      if (!addItem(jArray, func, taosArrayGetP(array, i))) {
        return false;
      }
    }
  }
  return true;
}

static bool addRawArray(cJSON* json, const char* name, FToObj func, const void* array, int32_t itemSize, int32_t size) {
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

static cJSON* schemaToJson(const void* obj) {
  const SSlotSchema* schema = (const SSlotSchema*)obj;
  cJSON* jSchema = cJSON_CreateObject();
  if (NULL == jSchema) {
    return NULL;
  }

  // The 'name' field do not need to be serialized.

  bool res = cJSON_AddNumberToObject(jSchema, "Type", schema->type);
  if (res) {
    res = cJSON_AddNumberToObject(jSchema, "ColId", schema->colId);
  }
  if (res) {
    res = cJSON_AddNumberToObject(jSchema, "Bytes", schema->bytes);
  }

  if (!res) {
    cJSON_Delete(jSchema);
    return NULL;
  }
  return jSchema;
}

static cJSON* columnFilterInfoToJson(const void* obj) {
  const SColumnFilterInfo* filter = (const SColumnFilterInfo*)obj;
  cJSON* jFilter = cJSON_CreateObject();
  if (NULL == jFilter) {
    return NULL;
  }

  bool res = cJSON_AddNumberToObject(jFilter, "LowerRelOptr", filter->lowerRelOptr);
  if (res) {
    res = cJSON_AddNumberToObject(jFilter, "UpperRelOptr", filter->upperRelOptr);
  }
  if (res) {
    res = cJSON_AddNumberToObject(jFilter, "Filterstr", filter->filterstr);
  }
  if (res) {
    res = cJSON_AddNumberToObject(jFilter, "LowerBnd", filter->lowerBndd);
  }
  if (res) {
    res = cJSON_AddNumberToObject(jFilter, "UpperBnd", filter->upperBndd);
  }

  if (!res) {
    cJSON_Delete(jFilter);
    return NULL;
  }
  return jFilter;
}

static cJSON* columnInfoToJson(const void* obj) {
  const SColumnInfo* col = (const SColumnInfo*)obj;
  cJSON* jCol = cJSON_CreateObject();
  if (NULL == jCol) {
    return NULL;
  }

  bool res = cJSON_AddNumberToObject(jCol, "ColId", col->colId);
  if (res) {
    res = cJSON_AddNumberToObject(jCol, "Type", col->type);
  }
  if (res) {
    res = cJSON_AddNumberToObject(jCol, "Bytes", col->bytes);
  }
  if (res) {
    res = addRawArray(jCol, "FilterList", columnFilterInfoToJson, col->flist.filterInfo, sizeof(SColumnFilterInfo), col->flist.numOfFilters);
  }

  if (!res) {
    cJSON_Delete(jCol);
    return NULL;
  }
  return jCol;
}

static cJSON* columnToJson(const void* obj) {
  const SColumn* col = (const SColumn*)obj;
  cJSON* jCol = cJSON_CreateObject();
  if (NULL == jCol) {
    return NULL;
  }

  bool res = cJSON_AddNumberToObject(jCol, "TableId", col->uid);
  if (res) {
    res = cJSON_AddNumberToObject(jCol, "Flag", col->flag);
  }
  if (res) {
    res = addObject(jCol, "Info", columnInfoToJson, &col->info);
  }

  if (!res) {
    cJSON_Delete(jCol);
    return NULL;
  }
  return jCol;
}

static cJSON* exprNodeToJson(const void* obj);

static cJSON* operatorToJson(const void* obj) {
  const tExprNode* exprInfo = (const tExprNode*)obj;
  cJSON* jOper = cJSON_CreateObject();
  if (NULL == jOper) {
    return NULL;
  }

  bool res = cJSON_AddNumberToObject(jOper, "Oper", exprInfo->_node.optr);
  if (res) {
    res = addObject(jOper, "Left", exprNodeToJson, exprInfo->_node.pLeft);
  }
  if (res) {
    res = addObject(jOper, "Right", exprNodeToJson, exprInfo->_node.pRight);
  }

  if (!res) {
    cJSON_Delete(jOper);
    return NULL;
  }
  return jOper;
}

static cJSON* functionToJson(const void* obj) {
  const tExprNode* exprInfo = (const tExprNode*)obj;
  cJSON* jFunc = cJSON_CreateObject();
  if (NULL == jFunc) {
    return NULL;
  }

  bool res = cJSON_AddStringToObject(jFunc, "Name", exprInfo->_function.functionName);
  if (res) {
    res = addRawArray(jFunc, "Child", exprNodeToJson, exprInfo->_function.pChild, sizeof(tExprNode*), exprInfo->_function.num);
  }

  if (!res) {
    cJSON_Delete(jFunc);
    return NULL;
  }
  return jFunc;
}

static cJSON* variantToJson(const void* obj) {
  const SVariant* var = (const SVariant*)obj;
  cJSON* jVar = cJSON_CreateObject();
  if (NULL == jVar) {
    return NULL;
  }

  bool res = cJSON_AddNumberToObject(jVar, "Type", var->nType);
  if (res) {
    res = cJSON_AddNumberToObject(jVar, "Len", var->nLen);
  }
  if (res) {
    if (0/* in */) {
      res = addArray(jVar, "values", variantToJson, var->arr);
    } else if (IS_NUMERIC_TYPE(var->nType)) {
      res = cJSON_AddNumberToObject(jVar, "Value", var->d);
    } else {
      res = cJSON_AddStringToObject(jVar, "Value", var->pz);
    }
  }

  if (!res) {
    cJSON_Delete(jVar);
    return NULL;
  }
  return jVar;
}

static cJSON* exprNodeToJson(const void* obj) {
  const tExprNode* exprInfo = (const tExprNode*)obj;
  cJSON* jExprInfo = cJSON_CreateObject();
  if (NULL == jExprInfo) {
    return NULL;
  }

  bool res = cJSON_AddNumberToObject(jExprInfo, "Type", exprInfo->nodeType);
  if (res) {
    switch (exprInfo->nodeType) {
      case TEXPR_BINARYEXPR_NODE:
      case TEXPR_UNARYEXPR_NODE:
        res = addObject(jExprInfo, "Operator", operatorToJson, exprInfo);
        break;
      case TEXPR_FUNCTION_NODE:
        res = addObject(jExprInfo, "Function", functionToJson, exprInfo);
        break;
      case TEXPR_COL_NODE:
        res = addObject(jExprInfo, "Column", schemaToJson, exprInfo->pSchema);
        break;
      case TEXPR_VALUE_NODE:
        res = addObject(jExprInfo, "Value", variantToJson, exprInfo->pVal);
        break;
      default:
        res = false;
        break;
    }
  }

  if (!res) {
    cJSON_Delete(jExprInfo);
    return NULL;
  }
  return jExprInfo;
}

static cJSON* sqlExprToJson(const void* obj) {
  const SSqlExpr* expr = (const SSqlExpr*)obj;
  cJSON* jExpr = cJSON_CreateObject();
  if (NULL == jExpr) {
    return NULL;
  }

  // token does not need to be serialized.

  bool res = addObject(jExpr, "Schema", schemaToJson, &expr->resSchema);
  if (res) {
    res = addRawArray(jExpr, "Columns", columnToJson, expr->pColumns, sizeof(SColumn), expr->numOfCols);
  }
  if (res) {
    res = cJSON_AddNumberToObject(jExpr, "InterBytes", expr->interBytes);
  }
  if (res) {
    res = addRawArray(jExpr, "Params", variantToJson, expr->param, sizeof(SVariant), expr->numOfParams);
  }

  if (!res) {
    cJSON_Delete(jExpr);
    return NULL;
  }
  return jExpr;
}

static cJSON* exprInfoToJson(const void* obj) {
  const SExprInfo* exprInfo = (const SExprInfo*)obj;
  cJSON* jExprInfo = cJSON_CreateObject();
  if (NULL == jExprInfo) {
    return NULL;
  }

  bool res = addObject(jExprInfo, "Base", sqlExprToJson, &exprInfo->base);
  if (res) {
    res = addObject(jExprInfo, "Expr", exprNodeToJson, exprInfo->pExpr);
  }

  if (!res) {
    cJSON_Delete(jExprInfo);
    return NULL;
  }
  return jExprInfo;
}

static cJSON* phyNodeToJson(const void* obj) {
  const SPhyNode* phyNode = (const SPhyNode*)obj;
  cJSON* jNode = cJSON_CreateObject();
  if (NULL == jNode) {
    return NULL;
  }

  // The 'pParent' field do not need to be serialized.

  bool res = cJSON_AddStringToObject(jNode, "Name", phyNode->info.name);
  if (res) {
    res = addArray(jNode, "Targets", exprInfoToJson, phyNode->pTargets);
  }
  if (res) {
    res = addArray(jNode, "Conditions", exprInfoToJson, phyNode->pConditions);
  }
  if (res) {
    res = addRawArray(jNode, "Schema", schemaToJson, phyNode->targetSchema.pSchema, sizeof(SSlotSchema), phyNode->targetSchema.numOfCols);
  }
  if (res) {
    res = addArray(jNode, "Children", phyNodeToJson, phyNode->pChildren);
  }

  if (!res) {
    cJSON_Delete(jNode);
    return NULL;
  }
  return jNode;
}

static cJSON* subplanIdToJson(const void* obj) {
  const SSubplanId* id = (const SSubplanId*)obj;
  cJSON* jId = cJSON_CreateObject();
  if (NULL == jId) {
    return NULL;
  }

  bool res = cJSON_AddNumberToObject(jId, "QueryId", id->queryId);
  if (res) {
    res = cJSON_AddNumberToObject(jId, "TemplateId", id->templateId);
  }
  if (res) {
    res = cJSON_AddNumberToObject(jId, "SubplanId", id->subplanId);
  }

  if (!res) {
    cJSON_Delete(jId);
    return NULL;
  }
  return jId;
}

static cJSON* subplanToJson(const SSubplan* subplan) {
  cJSON* jSubplan = cJSON_CreateObject();
  if (NULL == jSubplan) {
    return NULL;
  }

  // The 'type', 'level', 'execEpSet', 'pChildern' and 'pParents' fields do not need to be serialized.

  bool res = addObject(jSubplan, "Id", subplanIdToJson, &subplan->id);
  if (res) {
    res = addObject(jSubplan, "Node", phyNodeToJson, subplan->pNode);
  }

  if (!res) {
    cJSON_Delete(jSubplan);
    return NULL;
  }
  return jSubplan;
}

int32_t subPlanToString(const SSubplan* subplan, char** str) {
  cJSON* json = subplanToJson(subplan);
  if (NULL == json) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return TSDB_CODE_FAILED;
  }
  *str = cJSON_Print(json);
  return TSDB_CODE_SUCCESS;
}

int32_t stringToSubplan(const char* str, SSubplan** subplan) {
  // todo
  return TSDB_CODE_SUCCESS;
}
