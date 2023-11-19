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

#include "cmdnodes.h"
#include "nodesUtil.h"
#include "plannodes.h"
#include "query.h"
#include "querynodes.h"
#include "taoserror.h"
#include "tdatablock.h"
#include "tjson.h"

static int32_t nodeToJson(const void* pObj, SJson* pJson);
static int32_t jsonToNode(const SJson* pJson, void* pObj);
static int32_t jsonToNodeObject(const SJson* pJson, const char* pName, SNode** pNode);
static int32_t makeNodeByJson(const SJson* pJson, SNode** pNode);


typedef int32_t (*FExecNodeToJson)(const void* pObj, SJson* pJson);
typedef int32_t (*FExecJsonToNode)(const SJson* pJson, void* pObj);
typedef void (*FExecDestoryNode)(SNode* pNode);

/**
 * @brief Node operation to binding function set 
 */
typedef struct SBuiltinNodeDefinition {
  const char* name;
  int32_t     nodeSize;
  FExecNodeToJson   toJsonFunc;
  FExecJsonToNode   toNodeFunc;
  FExecDestoryNode  destoryFunc;
} SBuiltinNodeDefinition;

SBuiltinNodeDefinition funcNodes[QUERY_NODE_END] = {NULL};

static TdThreadOnce    functionNodeInit = PTHREAD_ONCE_INIT;
static int32_t         initNodeCode = -1;

static void setFunc(const char* name, int32_t type, int32_t nodeSize, FExecNodeToJson toJsonFunc,
                    FExecJsonToNode toNodeFunc, FExecDestoryNode destoryFunc) {
  funcNodes[type].name = name;
  funcNodes[type].nodeSize = nodeSize;
  funcNodes[type].toJsonFunc = toJsonFunc;
  funcNodes[type].toNodeFunc = toNodeFunc;
  funcNodes[type].destoryFunc = destoryFunc;
}

static void doInitNodeFuncArray();

void nodesInit() {
  taosThreadOnce(&functionNodeInit, doInitNodeFuncArray);
}

bool funcArrayCheck(int32_t type) {
  if (type < 0 || QUERY_NODE_END < (type+1)) {
    nodesError("funcArrayCheck unknown type = %d", type);
    return false;
  }
  if (initNodeCode != 0) {
    nodesInit();
  }
  if (!funcNodes[type].name) {
    return false;
  }
  return true;
}

int32_t getNodeSize(ENodeType type) {
  if (!funcArrayCheck(type)) {
    return 0;
  }
  return funcNodes[type].nodeSize;
}

const char* nodesNodeName(ENodeType type) {
  if (!funcArrayCheck(type)) {
    return NULL;
  }
  return funcNodes[type].name;
}

static int32_t nodeListToJson(SJson* pJson, const char* pName, const SNodeList* pList) {
  if (LIST_LENGTH(pList) > 0) {
    SJson* jList = tjsonAddArrayToObject(pJson, pName);
    if (NULL == jList) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    SNode* pNode;
    FOREACH(pNode, pList) {
      int32_t code = tjsonAddItem(jList, nodeToJson, pNode);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t jsonToNodeListImpl(const SJson* pJsonArray, SNodeList** pList) {
  int32_t size = (NULL == pJsonArray ? 0 : tjsonGetArraySize(pJsonArray));
  if (size > 0) {
    *pList = nodesMakeList();
    if (NULL == *pList) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  int32_t code = TSDB_CODE_SUCCESS;
  for (int32_t i = 0; i < size; ++i) {
    SJson* pJsonItem = tjsonGetArrayItem(pJsonArray, i);
    SNode* pNode = NULL;
    code = makeNodeByJson(pJsonItem, &pNode);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListAppend(*pList, pNode);
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }
  return code;
}

static int32_t jsonToNodeList(const SJson* pJson, const char* pName, SNodeList** pList) {
  return jsonToNodeListImpl(tjsonGetObjectItem(pJson, pName), pList);
}

static const char* jkTableComInfoNumOfTags = "NumOfTags";
static const char* jkTableComInfoPrecision = "Precision";
static const char* jkTableComInfoNumOfColumns = "NumOfColumns";
static const char* jkTableComInfoRowSize = "RowSize";

static int32_t tableComInfoToJson(const void* pObj, SJson* pJson) {
  const STableComInfo* pNode = (const STableComInfo*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkTableComInfoNumOfTags, pNode->numOfTags);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableComInfoPrecision, pNode->precision);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableComInfoNumOfColumns, pNode->numOfColumns);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableComInfoRowSize, pNode->rowSize);
  }

  return code;
}

static int32_t jsonToTableComInfo(const SJson* pJson, void* pObj) {
  STableComInfo* pNode = (STableComInfo*)pObj;

  int32_t code;
  tjsonGetNumberValue(pJson, jkTableComInfoNumOfTags, pNode->numOfTags, code);
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkTableComInfoPrecision, pNode->precision, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkTableComInfoNumOfColumns, pNode->numOfColumns, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkTableComInfoRowSize, pNode->rowSize, code);
  }

  return code;
}

static const char* jkSchemaType = "Type";
static const char* jkSchemaColId = "ColId";
static const char* jkSchemaBytes = "bytes";
static const char* jkSchemaName = "Name";

static int32_t schemaToJson(const void* pObj, SJson* pJson) {
  const SSchema* pNode = (const SSchema*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkSchemaType, pNode->type);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkSchemaColId, pNode->colId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkSchemaBytes, pNode->bytes);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkSchemaName, pNode->name);
  }

  return code;
}

static int32_t jsonToSchema(const SJson* pJson, void* pObj) {
  SSchema* pNode = (SSchema*)pObj;

  int32_t code;
  tjsonGetNumberValue(pJson, jkSchemaType, pNode->type, code);
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkSchemaColId, pNode->colId, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkSchemaBytes, pNode->bytes, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkSchemaName, pNode->name);
  }

  return code;
}

static const char* jkTableMetaVgId = "VgId";
static const char* jkTableMetaTableType = "TableType";
static const char* jkTableMetaUid = "Uid";
static const char* jkTableMetaSuid = "Suid";
static const char* jkTableMetaSversion = "Sversion";
static const char* jkTableMetaTversion = "Tversion";
static const char* jkTableMetaComInfo = "ComInfo";
static const char* jkTableMetaColSchemas = "ColSchemas";

static int32_t tableMetaToJson(const void* pObj, SJson* pJson) {
  const STableMeta* pNode = (const STableMeta*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkTableMetaVgId, pNode->vgId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableMetaTableType, pNode->tableType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableMetaUid, pNode->uid);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableMetaSuid, pNode->suid);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableMetaSversion, pNode->sversion);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableMetaTversion, pNode->tversion);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkTableMetaComInfo, tableComInfoToJson, &pNode->tableInfo);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddArray(pJson, jkTableMetaColSchemas, schemaToJson, pNode->schema, sizeof(SSchema),
                         TABLE_TOTAL_COL_NUM(pNode));
  }

  return code;
}

static int32_t jsonToTableMeta(const SJson* pJson, void* pObj) {
  STableMeta* pNode = (STableMeta*)pObj;

  int32_t code;
  tjsonGetNumberValue(pJson, jkTableMetaVgId, pNode->vgId, code);
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkTableMetaTableType, pNode->tableType, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkTableMetaUid, pNode->uid, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkTableMetaSuid, pNode->suid, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkTableMetaSversion, pNode->sversion, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkTableMetaTversion, pNode->tversion, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonToObject(pJson, jkTableMetaComInfo, jsonToTableComInfo, &pNode->tableInfo);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonToArray(pJson, jkTableMetaColSchemas, jsonToSchema, pNode->schema, sizeof(SSchema));
  }

  return code;
}

static const char* jkLogicPlanTargets = "Targets";
static const char* jkLogicPlanConditions = "Conditions";
static const char* jkLogicPlanChildren = "Children";
static const char* jkLogicPlanLimit = "Limit";
static const char* jkLogicPlanSlimit = "SLimit";
static const char* jkLogicPlanRequireDataOrder = "RequireDataOrder";
static const char* jkLogicPlanResultDataOrder = "ResultDataOrder";
static const char* jkLogicPlanGroupAction = "GroupAction";

static int32_t logicPlanNodeToJson(const void* pObj, SJson* pJson) {
  const SLogicNode* pNode = (const SLogicNode*)pObj;

  int32_t code = nodeListToJson(pJson, jkLogicPlanTargets, pNode->pTargets);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkLogicPlanConditions, nodeToJson, pNode->pConditions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkLogicPlanChildren, pNode->pChildren);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkLogicPlanLimit, nodeToJson, pNode->pLimit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkLogicPlanSlimit, nodeToJson, pNode->pSlimit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkLogicPlanRequireDataOrder, pNode->requireDataOrder);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkLogicPlanResultDataOrder, pNode->resultDataOrder);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkLogicPlanGroupAction, pNode->groupAction);
  }

  return code;
}

static int32_t jsonToLogicPlanNode(const SJson* pJson, void* pObj) {
  SLogicNode* pNode = (SLogicNode*)pObj;

  int32_t code = jsonToNodeList(pJson, jkLogicPlanTargets, &pNode->pTargets);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkLogicPlanConditions, &pNode->pConditions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkLogicPlanChildren, &pNode->pChildren);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkLogicPlanLimit, &pNode->pLimit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkLogicPlanSlimit, &pNode->pSlimit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkLogicPlanRequireDataOrder, pNode->requireDataOrder, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkLogicPlanResultDataOrder, pNode->resultDataOrder, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkLogicPlanGroupAction, pNode->groupAction, code);
  }

  return code;
}

static const char* jkScanLogicPlanScanCols = "ScanCols";
static const char* jkScanLogicPlanScanPseudoCols = "ScanPseudoCols";
static const char* jkScanLogicPlanTableType = "TableType";
static const char* jkScanLogicPlanTableId = "TableId";
static const char* jkScanLogicPlanStableId = "StableId";
static const char* jkScanLogicPlanScanType = "ScanType";
static const char* jkScanLogicPlanScanCount = "ScanCount";
static const char* jkScanLogicPlanReverseScanCount = "ReverseScanCount";
static const char* jkScanLogicPlanDynamicScanFuncs = "DynamicScanFuncs";
static const char* jkScanLogicPlanDataRequired = "DataRequired";
static const char* jkScanLogicPlanTagCond = "TagCond";
static const char* jkScanLogicPlanGroupTags = "GroupTags";
static const char* jkScanLogicPlanOnlyMetaCtbIdx = "OnlyMetaCtbIdx";

static int32_t logicScanNodeToJson(const void* pObj, SJson* pJson) {
  const SScanLogicNode* pNode = (const SScanLogicNode*)pObj;

  int32_t code = logicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkScanLogicPlanScanCols, pNode->pScanCols);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkScanLogicPlanScanPseudoCols, pNode->pScanPseudoCols);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkScanLogicPlanTableType, pNode->tableType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkScanLogicPlanTableId, pNode->tableId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkScanLogicPlanStableId, pNode->stableId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkScanLogicPlanScanType, pNode->scanType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkScanLogicPlanScanCount, pNode->scanSeq[0]);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkScanLogicPlanReverseScanCount, pNode->scanSeq[1]);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkScanLogicPlanDynamicScanFuncs, nodeToJson, pNode->pDynamicScanFuncs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkScanLogicPlanDataRequired, pNode->dataRequired);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkScanLogicPlanTagCond, nodeToJson, pNode->pTagCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkScanLogicPlanGroupTags, pNode->pGroupTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkScanLogicPlanOnlyMetaCtbIdx, pNode->onlyMetaCtbIdx);
  }
  return code;
}

static int32_t jsonToLogicScanNode(const SJson* pJson, void* pObj) {
  SScanLogicNode* pNode = (SScanLogicNode*)pObj;

  int32_t objSize = 0;
  int32_t code = jsonToLogicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkScanLogicPlanScanCols, &pNode->pScanCols);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkScanLogicPlanScanPseudoCols, &pNode->pScanPseudoCols);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkScanLogicPlanTableType, &pNode->tableType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetUBigIntValue(pJson, jkScanLogicPlanTableId, &pNode->tableId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetUBigIntValue(pJson, jkScanLogicPlanStableId, &pNode->stableId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkScanLogicPlanScanType, pNode->scanType, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetUTinyIntValue(pJson, jkScanLogicPlanScanCount, &pNode->scanSeq[0]);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetUTinyIntValue(pJson, jkScanLogicPlanReverseScanCount, &pNode->scanSeq[1]);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkScanLogicPlanDynamicScanFuncs, &pNode->pDynamicScanFuncs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkScanLogicPlanDataRequired, &pNode->dataRequired);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkScanLogicPlanTagCond, &pNode->pTagCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkScanLogicPlanGroupTags, &pNode->pGroupTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkScanLogicPlanOnlyMetaCtbIdx, &pNode->onlyMetaCtbIdx);
  }
  
  return code;
}

static const char* jkProjectLogicPlanProjections = "Projections";
static const char* jkProjectLogicPlanIgnoreGroupId = "IgnoreGroupId";

static int32_t logicProjectNodeToJson(const void* pObj, SJson* pJson) {
  const SProjectLogicNode* pNode = (const SProjectLogicNode*)pObj;

  int32_t code = logicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkProjectLogicPlanProjections, pNode->pProjections);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkProjectLogicPlanIgnoreGroupId, pNode->ignoreGroupId);
  }

  return code;
}

static int32_t jsonToLogicProjectNode(const SJson* pJson, void* pObj) {
  SProjectLogicNode* pNode = (SProjectLogicNode*)pObj;

  int32_t code = jsonToLogicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkProjectLogicPlanProjections, &pNode->pProjections);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkProjectLogicPlanIgnoreGroupId, &pNode->ignoreGroupId);
  }

  return code;
}

static const char* jkVnodeModifyLogicPlanModifyType = "ModifyType";
static const char* jkVnodeModifyLogicPlanMsgType = "MsgType";
static const char* jkVnodeModifyLogicPlanAffectedRows = "AffectedRows";

static int32_t logicVnodeModifyNodeToJson(const void* pObj, SJson* pJson) {
  const SVnodeModifyLogicNode* pNode = (const SVnodeModifyLogicNode*)pObj;

  int32_t code = logicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkVnodeModifyLogicPlanModifyType, pNode->modifyType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkVnodeModifyLogicPlanMsgType, pNode->msgType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkVnodeModifyLogicPlanAffectedRows, nodeToJson, pNode->pAffectedRows);
  }

  return code;
}

static int32_t jsonToLogicVnodeModifyNode(const SJson* pJson, void* pObj) {
  SVnodeModifyLogicNode* pNode = (SVnodeModifyLogicNode*)pObj;

  int32_t code = jsonToLogicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkVnodeModifyLogicPlanModifyType, pNode->modifyType, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkVnodeModifyLogicPlanMsgType, &pNode->msgType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkVnodeModifyLogicPlanAffectedRows, &pNode->pAffectedRows);
  }

  return code;
}

static const char* jkExchangeLogicPlanSrcStartGroupId = "SrcStartGroupId";
static const char* jkExchangeLogicPlanSrcEndGroupId = "SrcEndGroupId";

static int32_t logicExchangeNodeToJson(const void* pObj, SJson* pJson) {
  const SExchangeLogicNode* pNode = (const SExchangeLogicNode*)pObj;

  int32_t code = logicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkExchangeLogicPlanSrcStartGroupId, pNode->srcStartGroupId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkExchangeLogicPlanSrcEndGroupId, pNode->srcEndGroupId);
  }

  return code;
}

static int32_t jsonToLogicExchangeNode(const SJson* pJson, void* pObj) {
  SExchangeLogicNode* pNode = (SExchangeLogicNode*)pObj;

  int32_t code = jsonToLogicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkExchangeLogicPlanSrcStartGroupId, &pNode->srcStartGroupId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkExchangeLogicPlanSrcEndGroupId, &pNode->srcEndGroupId);
  }

  return code;
}

static const char* jkMergeLogicPlanMergeKeys = "MergeKeys";
static const char* jkMergeLogicPlanInputs = "Inputs";
static const char* jkMergeLogicPlanNumOfChannels = "NumOfChannels";
static const char* jkMergeLogicPlanSrcGroupId = "SrcGroupId";

static int32_t logicMergeNodeToJson(const void* pObj, SJson* pJson) {
  const SMergeLogicNode* pNode = (const SMergeLogicNode*)pObj;

  int32_t code = logicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkMergeLogicPlanMergeKeys, pNode->pMergeKeys);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkMergeLogicPlanInputs, pNode->pInputs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkMergeLogicPlanNumOfChannels, pNode->numOfChannels);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkMergeLogicPlanSrcGroupId, pNode->srcGroupId);
  }

  return code;
}

static int32_t jsonToLogicMergeNode(const SJson* pJson, void* pObj) {
  SMergeLogicNode* pNode = (SMergeLogicNode*)pObj;

  int32_t code = jsonToLogicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkMergeLogicPlanMergeKeys, &pNode->pMergeKeys);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkMergeLogicPlanInputs, &pNode->pInputs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkMergeLogicPlanNumOfChannels, &pNode->numOfChannels);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkMergeLogicPlanSrcGroupId, &pNode->srcGroupId);
  }

  return code;
}

static const char* jkWindowLogicPlanWinType = "WinType";
static const char* jkWindowLogicPlanFuncs = "Funcs";
static const char* jkWindowLogicPlanInterval = "Interval";
static const char* jkWindowLogicPlanOffset = "Offset";
static const char* jkWindowLogicPlanSliding = "Sliding";
static const char* jkWindowLogicPlanIntervalUnit = "IntervalUnit";
static const char* jkWindowLogicPlanSlidingUnit = "SlidingUnit";
static const char* jkWindowLogicPlanSessionGap = "SessionGap";
static const char* jkWindowLogicPlanTspk = "Tspk";
static const char* jkWindowLogicPlanStateExpr = "StateExpr";
static const char* jkWindowLogicPlanTriggerType = "TriggerType";
static const char* jkWindowLogicPlanWatermark = "Watermark";
static const char* jkWindowLogicPlanDeleteMark = "DeleteMark";

static int32_t logicWindowNodeToJson(const void* pObj, SJson* pJson) {
  const SWindowLogicNode* pNode = (const SWindowLogicNode*)pObj;

  int32_t code = logicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkWindowLogicPlanWinType, pNode->winType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkWindowLogicPlanFuncs, pNode->pFuncs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkWindowLogicPlanInterval, pNode->interval);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkWindowLogicPlanOffset, pNode->offset);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkWindowLogicPlanSliding, pNode->sliding);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkWindowLogicPlanIntervalUnit, pNode->intervalUnit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkWindowLogicPlanSlidingUnit, pNode->slidingUnit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkWindowLogicPlanSessionGap, pNode->sessionGap);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkWindowLogicPlanTspk, nodeToJson, pNode->pTspk);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkWindowLogicPlanStateExpr, nodeToJson, pNode->pStateExpr);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkWindowLogicPlanTriggerType, pNode->triggerType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkWindowLogicPlanWatermark, pNode->watermark);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkWindowLogicPlanDeleteMark, pNode->deleteMark);
  }

  return code;
}

static int32_t jsonToLogicWindowNode(const SJson* pJson, void* pObj) {
  SWindowLogicNode* pNode = (SWindowLogicNode*)pObj;

  int32_t code = jsonToLogicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkWindowLogicPlanWinType, pNode->winType, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkWindowLogicPlanFuncs, &pNode->pFuncs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkWindowLogicPlanInterval, &pNode->interval);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkWindowLogicPlanOffset, &pNode->offset);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkWindowLogicPlanSliding, &pNode->sliding);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkWindowLogicPlanIntervalUnit, &pNode->intervalUnit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkWindowLogicPlanSlidingUnit, &pNode->slidingUnit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkWindowLogicPlanSessionGap, &pNode->sessionGap);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkWindowLogicPlanTspk, &pNode->pTspk);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkWindowLogicPlanStateExpr, &pNode->pStateExpr);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkWindowLogicPlanTriggerType, &pNode->triggerType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkWindowLogicPlanWatermark, &pNode->watermark);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkWindowLogicPlanDeleteMark, &pNode->deleteMark);
  }

  return code;
}

static const char* jkFillLogicPlanMode = "Mode";
static const char* jkFillLogicPlanWStartTs = "WStartTs";
static const char* jkFillLogicPlanValues = "Values";
static const char* jkFillLogicPlanStartTime = "StartTime";
static const char* jkFillLogicPlanEndTime = "EndTime";

static int32_t logicFillNodeToJson(const void* pObj, SJson* pJson) {
  const SFillLogicNode* pNode = (const SFillLogicNode*)pObj;

  int32_t code = logicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkFillLogicPlanMode, pNode->mode);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkFillLogicPlanWStartTs, nodeToJson, pNode->pWStartTs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkFillLogicPlanValues, nodeToJson, pNode->pValues);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkFillLogicPlanStartTime, pNode->timeRange.skey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkFillLogicPlanEndTime, pNode->timeRange.ekey);
  }

  return code;
}

static int32_t jsonToLogicFillNode(const SJson* pJson, void* pObj) {
  SFillLogicNode* pNode = (SFillLogicNode*)pObj;

  int32_t code = jsonToLogicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkFillLogicPlanMode, pNode->mode, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkFillLogicPlanWStartTs, &pNode->pWStartTs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkFillLogicPlanValues, &pNode->pValues);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkFillLogicPlanStartTime, &pNode->timeRange.skey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkFillLogicPlanEndTime, &pNode->timeRange.ekey);
  }

  return code;
}

static const char* jkSortLogicPlanSortKeys = "SortKeys";

static int32_t logicSortNodeToJson(const void* pObj, SJson* pJson) {
  const SSortLogicNode* pNode = (const SSortLogicNode*)pObj;

  int32_t code = logicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkSortLogicPlanSortKeys, pNode->pSortKeys);
  }

  return code;
}

static int32_t jsonToLogicSortNode(const SJson* pJson, void* pObj) {
  SSortLogicNode* pNode = (SSortLogicNode*)pObj;

  int32_t code = jsonToLogicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkSortLogicPlanSortKeys, &pNode->pSortKeys);
  }

  return code;
}

static const char* jkPartitionLogicPlanPartitionKeys = "PartitionKeys";

static int32_t logicPartitionNodeToJson(const void* pObj, SJson* pJson) {
  const SPartitionLogicNode* pNode = (const SPartitionLogicNode*)pObj;

  int32_t code = logicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkPartitionLogicPlanPartitionKeys, pNode->pPartitionKeys);
  }

  return code;
}

static int32_t jsonToLogicPartitionNode(const SJson* pJson, void* pObj) {
  SPartitionLogicNode* pNode = (SPartitionLogicNode*)pObj;

  int32_t code = jsonToLogicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkPartitionLogicPlanPartitionKeys, &pNode->pPartitionKeys);
  }

  return code;
}

static const char* jkIndefRowsFuncLogicPlanFuncs = "Funcs";

static int32_t logicIndefRowsFuncNodeToJson(const void* pObj, SJson* pJson) {
  const SIndefRowsFuncLogicNode* pNode = (const SIndefRowsFuncLogicNode*)pObj;

  int32_t code = logicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkIndefRowsFuncLogicPlanFuncs, pNode->pFuncs);
  }

  return code;
}

static int32_t jsonToLogicIndefRowsFuncNode(const SJson* pJson, void* pObj) {
  SIndefRowsFuncLogicNode* pNode = (SIndefRowsFuncLogicNode*)pObj;

  int32_t code = jsonToLogicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkIndefRowsFuncLogicPlanFuncs, &pNode->pFuncs);
  }

  return code;
}

static const char* jkInterpFuncLogicPlanFuncs = "Funcs";
static const char* jkInterpFuncLogicPlanStartTime = "StartTime";
static const char* jkInterpFuncLogicPlanEndTime = "EndTime";
static const char* jkInterpFuncLogicPlanInterval = "Interval";

static int32_t logicInterpFuncNodeToJson(const void* pObj, SJson* pJson) {
  const SInterpFuncLogicNode* pNode = (const SInterpFuncLogicNode*)pObj;

  int32_t code = logicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkInterpFuncLogicPlanFuncs, pNode->pFuncs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkInterpFuncLogicPlanStartTime, pNode->timeRange.skey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkInterpFuncLogicPlanEndTime, pNode->timeRange.ekey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkInterpFuncLogicPlanInterval, pNode->interval);
  }

  return code;
}

static int32_t jsonToLogicInterpFuncNode(const SJson* pJson, void* pObj) {
  SInterpFuncLogicNode* pNode = (SInterpFuncLogicNode*)pObj;

  int32_t code = jsonToLogicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkInterpFuncLogicPlanFuncs, &pNode->pFuncs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkInterpFuncLogicPlanStartTime, &pNode->timeRange.skey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkInterpFuncLogicPlanEndTime, &pNode->timeRange.ekey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkInterpFuncLogicPlanInterval, &pNode->interval);
  }

  return code;
}

static const char* jkGroupCacheLogicPlanGrpColsMayBeNull = "GroupColsMayBeNull";
static const char* jkGroupCacheLogicPlanGroupByUid = "GroupByUid";
static const char* jkGroupCacheLogicPlanGlobalGroup = "GlobalGroup";
static const char* jkGroupCacheLogicPlanGroupCols = "GroupCols";

static int32_t logicGroupCacheNodeToJson(const void* pObj, SJson* pJson) {
  const SGroupCacheLogicNode* pNode = (const SGroupCacheLogicNode*)pObj;

  int32_t code = logicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkGroupCacheLogicPlanGrpColsMayBeNull, pNode->grpColsMayBeNull);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkGroupCacheLogicPlanGroupByUid, pNode->grpByUid);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkGroupCacheLogicPlanGlobalGroup, pNode->globalGrp);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkGroupCacheLogicPlanGroupCols, pNode->pGroupCols);
  }

  return code;
}

static int32_t jsonToLogicGroupCacheNode(const SJson* pJson, void* pObj) {
  SGroupCacheLogicNode* pNode = (SGroupCacheLogicNode*)pObj;

  int32_t code = jsonToLogicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkGroupCacheLogicPlanGrpColsMayBeNull, &pNode->grpColsMayBeNull);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkGroupCacheLogicPlanGroupByUid, &pNode->grpByUid);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkGroupCacheLogicPlanGlobalGroup, &pNode->globalGrp);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkGroupCacheLogicPlanGroupCols, &pNode->pGroupCols);
  }

  return code;
}

static const char* jkDynQueryCtrlLogicPlanQueryType = "QueryType";
static const char* jkDynQueryCtrlLogicPlanStbJoinBatchFetch = "BatchFetch";
static const char* jkDynQueryCtrlLogicPlanStbJoinVgList = "VgroupList";
static const char* jkDynQueryCtrlLogicPlanStbJoinUidList = "UidList";

static int32_t logicDynQueryCtrlNodeToJson(const void* pObj, SJson* pJson) {
  const SDynQueryCtrlLogicNode* pNode = (const SDynQueryCtrlLogicNode*)pObj;

  int32_t code = logicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDynQueryCtrlLogicPlanQueryType, pNode->qType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkDynQueryCtrlLogicPlanStbJoinBatchFetch, pNode->stbJoin.batchFetch);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkDynQueryCtrlLogicPlanStbJoinVgList, pNode->stbJoin.pVgList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkDynQueryCtrlLogicPlanStbJoinUidList, pNode->stbJoin.pUidList);
  }

  return code;
}

static int32_t jsonToLogicDynQueryCtrlNode(const SJson* pJson, void* pObj) {
  SDynQueryCtrlLogicNode* pNode = (SDynQueryCtrlLogicNode*)pObj;

  int32_t code = jsonToLogicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkDynQueryCtrlLogicPlanQueryType, pNode->qType, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetBoolValue(pJson, jkDynQueryCtrlLogicPlanStbJoinBatchFetch, &pNode->stbJoin.batchFetch);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkDynQueryCtrlLogicPlanStbJoinVgList, &pNode->stbJoin.pVgList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkDynQueryCtrlLogicPlanStbJoinUidList, &pNode->stbJoin.pUidList);
  }

  return code;
}


static const char* jkSubplanIdQueryId = "QueryId";
static const char* jkSubplanIdGroupId = "GroupId";
static const char* jkSubplanIdSubplanId = "SubplanId";

static int32_t subplanIdToJson(const void* pObj, SJson* pJson) {
  const SSubplanId* pNode = (const SSubplanId*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkSubplanIdQueryId, pNode->queryId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkSubplanIdGroupId, pNode->groupId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkSubplanIdSubplanId, pNode->subplanId);
  }

  return code;
}

static int32_t jsonToSubplanId(const SJson* pJson, void* pObj) {
  SSubplanId* pNode = (SSubplanId*)pObj;

  int32_t code = tjsonGetUBigIntValue(pJson, jkSubplanIdQueryId, &pNode->queryId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkSubplanIdGroupId, &pNode->groupId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkSubplanIdSubplanId, &pNode->subplanId);
  }

  return code;
}

static const char* jkEndPointFqdn = "Fqdn";
static const char* jkEndPointPort = "Port";

static int32_t epToJson(const void* pObj, SJson* pJson) {
  const SEp* pNode = (const SEp*)pObj;

  int32_t code = tjsonAddStringToObject(pJson, jkEndPointFqdn, pNode->fqdn);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkEndPointPort, pNode->port);
  }

  return code;
}

static int32_t jsonToEp(const SJson* pJson, void* pObj) {
  SEp* pNode = (SEp*)pObj;

  int32_t code = tjsonGetStringValue(pJson, jkEndPointFqdn, pNode->fqdn);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetSmallIntValue(pJson, jkEndPointPort, &pNode->port);
  }

  return code;
}

static const char* jkEpSetInUse = "InUse";
static const char* jkEpSetNumOfEps = "NumOfEps";
static const char* jkEpSetEps = "Eps";

static int32_t epSetToJson(const void* pObj, SJson* pJson) {
  const SEpSet* pNode = (const SEpSet*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkEpSetInUse, pNode->inUse);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkEpSetNumOfEps, pNode->numOfEps);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddArray(pJson, jkEpSetEps, epToJson, pNode->eps, sizeof(SEp), pNode->numOfEps);
  }

  return code;
}

static int32_t jsonToEpSet(const SJson* pJson, void* pObj) {
  SEpSet* pNode = (SEpSet*)pObj;

  int32_t code = tjsonGetTinyIntValue(pJson, jkEpSetInUse, &pNode->inUse);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkEpSetNumOfEps, &pNode->numOfEps);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonToArray(pJson, jkEpSetEps, jsonToEp, pNode->eps, sizeof(SEp));
  }

  return code;
}

static const char* jkVgroupInfoVgId = "VgId";
static const char* jkVgroupInfoHashBegin = "HashBegin";
static const char* jkVgroupInfoHashEnd = "HashEnd";
static const char* jkVgroupInfoEpSet = "EpSet";
static const char* jkVgroupInfoNumOfTable = "NumOfTable";

static int32_t vgroupInfoToJson(const void* pObj, SJson* pJson) {
  const SVgroupInfo* pNode = (const SVgroupInfo*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkVgroupInfoVgId, pNode->vgId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkVgroupInfoHashBegin, pNode->hashBegin);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkVgroupInfoHashEnd, pNode->hashEnd);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkVgroupInfoEpSet, epSetToJson, &pNode->epSet);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkVgroupInfoNumOfTable, pNode->numOfTable);
  }

  return code;
}

static int32_t jsonToVgroupInfo(const SJson* pJson, void* pObj) {
  SVgroupInfo* pNode = (SVgroupInfo*)pObj;

  int32_t code = tjsonGetIntValue(pJson, jkVgroupInfoVgId, &pNode->vgId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetUIntValue(pJson, jkVgroupInfoHashBegin, &pNode->hashBegin);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetUIntValue(pJson, jkVgroupInfoHashEnd, &pNode->hashEnd);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonToObject(pJson, jkVgroupInfoEpSet, jsonToEpSet, &pNode->epSet);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkVgroupInfoNumOfTable, &pNode->numOfTable);
  }

  return code;
}

static const char* jkVgroupsInfoNum = "Num";
static const char* jkVgroupsInfoVgroups = "Vgroups";

static int32_t vgroupsInfoToJson(const void* pObj, SJson* pJson) {
  const SVgroupsInfo* pNode = (const SVgroupsInfo*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkVgroupsInfoNum, pNode->numOfVgroups);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddArray(pJson, jkVgroupsInfoVgroups, vgroupInfoToJson, pNode->vgroups, sizeof(SVgroupInfo),
                         pNode->numOfVgroups);
  }

  return code;
}

static int32_t jsonToVgroupsInfo(const SJson* pJson, void* pObj) {
  SVgroupsInfo* pNode = (SVgroupsInfo*)pObj;

  int32_t code = tjsonGetIntValue(pJson, jkVgroupsInfoNum, &pNode->numOfVgroups);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonToArray(pJson, jkVgroupsInfoVgroups, jsonToVgroupInfo, pNode->vgroups, sizeof(SVgroupInfo));
  }

  return code;
}

static const char* jkLogicSubplanId = "Id";
static const char* jkLogicSubplanChildren = "Children";
static const char* jkLogicSubplanRootNode = "RootNode";
static const char* jkLogicSubplanType = "SubplanType";
static const char* jkLogicSubplanVgroupsSize = "VgroupsSize";
static const char* jkLogicSubplanVgroups = "Vgroups";
static const char* jkLogicSubplanLevel = "Level";
static const char* jkLogicSubplanSplitFlag = "SplitFlag";
static const char* jkLogicSubplanNumOfComputeNodes = "NumOfComputeNodes";

static int32_t logicSubplanToJson(const void* pObj, SJson* pJson) {
  const SLogicSubplan* pNode = (const SLogicSubplan*)pObj;

  int32_t code = tjsonAddObject(pJson, jkLogicSubplanId, subplanIdToJson, &pNode->id);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkLogicSubplanChildren, pNode->pChildren);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkLogicSubplanRootNode, nodeToJson, pNode->pNode);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkLogicSubplanType, pNode->subplanType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkLogicSubplanVgroupsSize, VGROUPS_INFO_SIZE(pNode->pVgroupList));
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkLogicSubplanVgroups, vgroupsInfoToJson, pNode->pVgroupList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkLogicSubplanLevel, pNode->level);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkLogicSubplanSplitFlag, pNode->splitFlag);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkLogicSubplanNumOfComputeNodes, pNode->numOfComputeNodes);
  }

  return code;
}

static int32_t jsonToLogicSubplan(const SJson* pJson, void* pObj) {
  SLogicSubplan* pNode = (SLogicSubplan*)pObj;

  int32_t code = tjsonToObject(pJson, jkLogicSubplanId, jsonToSubplanId, &pNode->id);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkLogicSubplanChildren, &pNode->pChildren);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkLogicSubplanRootNode, (SNode**)&pNode->pNode);
  }
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkLogicSubplanType, pNode->subplanType, code);
  }
  int32_t objSize = 0;
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkLogicSubplanVgroupsSize, &objSize);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonMakeObject(pJson, jkLogicSubplanVgroups, jsonToVgroupsInfo, (void**)&pNode->pVgroupList, objSize);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkLogicSubplanLevel, &pNode->level);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkLogicSubplanSplitFlag, &pNode->splitFlag);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkLogicSubplanNumOfComputeNodes, &pNode->numOfComputeNodes);
  }

  return code;
}

static const char* jkLogicPlanSubplans = "Subplans";

static int32_t logicPlanToJson(const void* pObj, SJson* pJson) {
  const SQueryLogicPlan* pNode = (const SQueryLogicPlan*)pObj;
  return tjsonAddObject(pJson, jkLogicPlanSubplans, nodeToJson, nodesListGetNode(pNode->pTopSubplans, 0));
}

static int32_t jsonToLogicPlan(const SJson* pJson, void* pObj) {
  SQueryLogicPlan* pNode = (SQueryLogicPlan*)pObj;
  SNode*           pChild = NULL;
  int32_t          code = jsonToNodeObject(pJson, jkLogicPlanSubplans, &pChild);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pNode->pTopSubplans, pChild);
  }
  return code;
}

static const char* jkJoinLogicPlanJoinType = "JoinType";
static const char* jkJoinLogicPlanJoinAlgo = "JoinAlgo";
static const char* jkJoinLogicPlanOnConditions = "OtherOnCond";
static const char* jkJoinLogicPlanPrimKeyEqCondition = "PrimKeyEqCond";
static const char* jkJoinLogicPlanColEqCondition = "ColumnEqCond";
static const char* jkJoinLogicPlanTagEqCondition = "TagEqCond";

static int32_t logicJoinNodeToJson(const void* pObj, SJson* pJson) {
  const SJoinLogicNode* pNode = (const SJoinLogicNode*)pObj;

  int32_t code = logicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkJoinLogicPlanJoinType, pNode->joinType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkJoinLogicPlanJoinAlgo, pNode->joinAlgo);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkJoinLogicPlanPrimKeyEqCondition, nodeToJson, pNode->pPrimKeyEqCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkJoinLogicPlanColEqCondition, nodeToJson, pNode->pColEqCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkJoinLogicPlanTagEqCondition, nodeToJson, pNode->pTagEqCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkJoinLogicPlanOnConditions, nodeToJson, pNode->pOtherOnCond);
  }
  return code;
}

static int32_t jsonToLogicJoinNode(const SJson* pJson, void* pObj) {
  SJoinLogicNode* pNode = (SJoinLogicNode*)pObj;

  int32_t code = jsonToLogicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkJoinLogicPlanJoinType, pNode->joinType, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkJoinLogicPlanJoinAlgo, pNode->joinAlgo, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkJoinLogicPlanPrimKeyEqCondition, &pNode->pPrimKeyEqCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkJoinLogicPlanColEqCondition, &pNode->pColEqCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkJoinLogicPlanTagEqCondition, &pNode->pTagEqCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkJoinLogicPlanOnConditions, &pNode->pOtherOnCond);
  }

  return code;
}

static const char* jkPhysiPlanOutputDataBlockDesc = "OutputDataBlockDesc";
static const char* jkPhysiPlanConditions = "Conditions";
static const char* jkPhysiPlanChildren = "Children";
static const char* jkPhysiPlanLimit = "Limit";
static const char* jkPhysiPlanSlimit = "SLimit";

static int32_t physicPlanNodeToJson(const void* pObj, SJson* pJson) {
  const SPhysiNode* pNode = (const SPhysiNode*)pObj;

  int32_t code = tjsonAddObject(pJson, jkPhysiPlanOutputDataBlockDesc, nodeToJson, pNode->pOutputDataBlockDesc);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkPhysiPlanConditions, nodeToJson, pNode->pConditions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkPhysiPlanChildren, pNode->pChildren);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkPhysiPlanLimit, nodeToJson, pNode->pLimit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkPhysiPlanSlimit, nodeToJson, pNode->pSlimit);
  }

  return code;
}

static int32_t jsonToPhysicPlanNode(const SJson* pJson, void* pObj) {
  SPhysiNode* pNode = (SPhysiNode*)pObj;

  int32_t code = jsonToNodeObject(pJson, jkPhysiPlanOutputDataBlockDesc, (SNode**)&pNode->pOutputDataBlockDesc);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkPhysiPlanConditions, &pNode->pConditions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkPhysiPlanChildren, &pNode->pChildren);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkPhysiPlanLimit, &pNode->pLimit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkPhysiPlanSlimit, &pNode->pSlimit);
  }

  return code;
}

static const char* jkNameType = "NameType";
static const char* jkNameAcctId = "AcctId";
static const char* jkNameDbName = "DbName";
static const char* jkNameTableName = "TableName";

static int32_t nameToJson(const void* pObj, SJson* pJson) {
  const SName* pNode = (const SName*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkNameType, pNode->type);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkNameAcctId, pNode->acctId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkNameDbName, pNode->dbname);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkNameTableName, pNode->tname);
  }

  return code;
}

static int32_t jsonToName(const SJson* pJson, void* pObj) {
  SName* pNode = (SName*)pObj;

  int32_t code = tjsonGetUTinyIntValue(pJson, jkNameType, &pNode->type);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkNameAcctId, &pNode->acctId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkNameDbName, pNode->dbname);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkNameTableName, pNode->tname);
  }

  return code;
}

static const char* jkScanPhysiPlanScanCols = "ScanCols";
static const char* jkScanPhysiPlanScanPseudoCols = "ScanPseudoCols";
static const char* jkScanPhysiPlanTableId = "TableId";
static const char* jkScanPhysiPlanSTableId = "STableId";
static const char* jkScanPhysiPlanTableType = "TableType";
static const char* jkScanPhysiPlanTableName = "TableName";
static const char* jkScanPhysiPlanGroupOrderScan = "GroupOrderScan";

static int32_t physiScanNodeToJson(const void* pObj, SJson* pJson) {
  const SScanPhysiNode* pNode = (const SScanPhysiNode*)pObj;

  int32_t code = physicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkScanPhysiPlanScanCols, pNode->pScanCols);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkScanPhysiPlanScanPseudoCols, pNode->pScanPseudoCols);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkScanPhysiPlanTableId, pNode->uid);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkScanPhysiPlanSTableId, pNode->suid);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkScanPhysiPlanTableType, pNode->tableType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkScanPhysiPlanTableName, nameToJson, &pNode->tableName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkScanPhysiPlanGroupOrderScan, pNode->groupOrderScan);
  }

  return code;
}

static int32_t jsonToPhysiScanNode(const SJson* pJson, void* pObj) {
  SScanPhysiNode* pNode = (SScanPhysiNode*)pObj;

  int32_t code = jsonToPhysicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkScanPhysiPlanScanCols, &pNode->pScanCols);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkScanPhysiPlanScanPseudoCols, &pNode->pScanPseudoCols);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetUBigIntValue(pJson, jkScanPhysiPlanTableId, &pNode->uid);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetUBigIntValue(pJson, jkScanPhysiPlanSTableId, &pNode->suid);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkScanPhysiPlanTableType, &pNode->tableType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonToObject(pJson, jkScanPhysiPlanTableName, jsonToName, &pNode->tableName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkScanPhysiPlanGroupOrderScan, &pNode->groupOrderScan);
  }

  return code;
}

static const char* jkTagScanPhysiOnlyMetaCtbIdx = "OnlyMetaCtbIdx";

static int32_t physiTagScanNodeToJson(const void* pObj, SJson* pJson) {
  const STagScanPhysiNode* pNode = (const STagScanPhysiNode*)pObj;

  int32_t code = physiScanNodeToJson(pObj, pJson);

  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkTagScanPhysiOnlyMetaCtbIdx, pNode->onlyMetaCtbIdx);
  }
  return code;
}

static int32_t jsonToPhysiTagScanNode(const SJson* pJson, void* pObj) {
  STagScanPhysiNode* pNode = (STagScanPhysiNode*)pObj;

  int32_t code = jsonToPhysiScanNode(pJson, pObj);

  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkTagScanPhysiOnlyMetaCtbIdx, &pNode->onlyMetaCtbIdx);
  }
  return code;
}

static const char* jkLastRowScanPhysiPlanGroupTags = "GroupTags";
static const char* jkLastRowScanPhysiPlanGroupSort = "GroupSort";
static const char* jkLastRowScanPhysiPlanTargets = "Targets";

static int32_t physiLastRowScanNodeToJson(const void* pObj, SJson* pJson) {
  const SLastRowScanPhysiNode* pNode = (const SLastRowScanPhysiNode*)pObj;

  int32_t code = physiScanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkLastRowScanPhysiPlanGroupTags, pNode->pGroupTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkLastRowScanPhysiPlanGroupSort, pNode->groupSort);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkLastRowScanPhysiPlanTargets, pNode->pTargets);
  }

  return code;
}

static int32_t jsonToPhysiLastRowScanNode(const SJson* pJson, void* pObj) {
  SLastRowScanPhysiNode* pNode = (SLastRowScanPhysiNode*)pObj;

  int32_t code = jsonToPhysiScanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkLastRowScanPhysiPlanGroupTags, &pNode->pGroupTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkLastRowScanPhysiPlanGroupSort, &pNode->groupSort);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkLastRowScanPhysiPlanTargets, &pNode->pTargets);
  }

  return code;
}

static const char* jkTableScanPhysiPlanScanCount = "ScanCount";
static const char* jkTableScanPhysiPlanReverseScanCount = "ReverseScanCount";
static const char* jkTableScanPhysiPlanStartKey = "StartKey";
static const char* jkTableScanPhysiPlanEndKey = "EndKey";
static const char* jkTableScanPhysiPlanRatio = "Ratio";
static const char* jkTableScanPhysiPlanDataRequired = "DataRequired";
static const char* jkTableScanPhysiPlanDynamicScanFuncs = "DynamicScanFuncs";
static const char* jkTableScanPhysiPlanInterval = "Interval";
static const char* jkTableScanPhysiPlanOffset = "Offset";
static const char* jkTableScanPhysiPlanSliding = "Sliding";
static const char* jkTableScanPhysiPlanIntervalUnit = "IntervalUnit";
static const char* jkTableScanPhysiPlanSlidingUnit = "SlidingUnit";
static const char* jkTableScanPhysiPlanTriggerType = "TriggerType";
static const char* jkTableScanPhysiPlanWatermark = "Watermark";
static const char* jkTableScanPhysiPlanIgnoreExpired = "IgnoreExpired";
static const char* jkTableScanPhysiPlanGroupTags = "GroupTags";
static const char* jkTableScanPhysiPlanGroupSort = "GroupSort";
static const char* jkTableScanPhysiPlanTags = "Tags";
static const char* jkTableScanPhysiPlanSubtable = "Subtable";
static const char* jkTableScanPhysiPlanAssignBlockUid = "AssignBlockUid";
static const char* jkTableScanPhysiPlanIgnoreUpdate = "IgnoreUpdate";

static int32_t physiTableScanNodeToJson(const void* pObj, SJson* pJson) {
  const STableScanPhysiNode* pNode = (const STableScanPhysiNode*)pObj;

  int32_t code = physiScanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableScanPhysiPlanScanCount, pNode->scanSeq[0]);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableScanPhysiPlanReverseScanCount, pNode->scanSeq[1]);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableScanPhysiPlanStartKey, pNode->scanRange.skey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableScanPhysiPlanEndKey, pNode->scanRange.ekey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddDoubleToObject(pJson, jkTableScanPhysiPlanRatio, pNode->ratio);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableScanPhysiPlanDataRequired, pNode->dataRequired);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkTableScanPhysiPlanDynamicScanFuncs, pNode->pDynamicScanFuncs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableScanPhysiPlanInterval, pNode->interval);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableScanPhysiPlanOffset, pNode->offset);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableScanPhysiPlanSliding, pNode->sliding);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableScanPhysiPlanIntervalUnit, pNode->intervalUnit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableScanPhysiPlanSlidingUnit, pNode->slidingUnit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableScanPhysiPlanTriggerType, pNode->triggerType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableScanPhysiPlanWatermark, pNode->watermark);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableScanPhysiPlanIgnoreExpired, pNode->igExpired);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkTableScanPhysiPlanGroupTags, pNode->pGroupTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkTableScanPhysiPlanGroupSort, pNode->groupSort);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkTableScanPhysiPlanTags, pNode->pTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkTableScanPhysiPlanSubtable, nodeToJson, pNode->pSubtable);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkTableScanPhysiPlanAssignBlockUid, pNode->assignBlockUid);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableScanPhysiPlanIgnoreUpdate, pNode->igCheckUpdate);
  }

  return code;
}

static int32_t jsonToPhysiTableScanNode(const SJson* pJson, void* pObj) {
  STableScanPhysiNode* pNode = (STableScanPhysiNode*)pObj;

  int32_t code = jsonToPhysiScanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetUTinyIntValue(pJson, jkTableScanPhysiPlanScanCount, &pNode->scanSeq[0]);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetUTinyIntValue(pJson, jkTableScanPhysiPlanReverseScanCount, &pNode->scanSeq[1]);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkTableScanPhysiPlanStartKey, &pNode->scanRange.skey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkTableScanPhysiPlanEndKey, &pNode->scanRange.ekey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetDoubleValue(pJson, jkTableScanPhysiPlanRatio, &pNode->ratio);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkTableScanPhysiPlanDataRequired, &pNode->dataRequired);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkTableScanPhysiPlanDynamicScanFuncs, &pNode->pDynamicScanFuncs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkTableScanPhysiPlanInterval, &pNode->interval);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkTableScanPhysiPlanOffset, &pNode->offset);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkTableScanPhysiPlanSliding, &pNode->sliding);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkTableScanPhysiPlanIntervalUnit, &pNode->intervalUnit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkTableScanPhysiPlanSlidingUnit, &pNode->slidingUnit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkTableScanPhysiPlanTriggerType, &pNode->triggerType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkTableScanPhysiPlanWatermark, &pNode->watermark);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkTableScanPhysiPlanIgnoreExpired, &pNode->igExpired);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkTableScanPhysiPlanGroupTags, &pNode->pGroupTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkTableScanPhysiPlanGroupSort, &pNode->groupSort);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkTableScanPhysiPlanTags, &pNode->pTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkTableScanPhysiPlanSubtable, &pNode->pSubtable);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkTableScanPhysiPlanAssignBlockUid, &pNode->assignBlockUid);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkTableScanPhysiPlanIgnoreUpdate, &pNode->igCheckUpdate);
  }

  return code;
}

static const char* jkSysTableScanPhysiPlanMnodeEpSet = "MnodeEpSet";
static const char* jkSysTableScanPhysiPlanShowRewrite = "ShowRewrite";
static const char* jkSysTableScanPhysiPlanAccountId = "AccountId";
static const char* jkSysTableScanPhysiPlanSysInfo = "SysInfo";

static int32_t physiSysTableScanNodeToJson(const void* pObj, SJson* pJson) {
  const SSystemTableScanPhysiNode* pNode = (const SSystemTableScanPhysiNode*)pObj;

  int32_t code = physiScanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkSysTableScanPhysiPlanMnodeEpSet, epSetToJson, &pNode->mgmtEpSet);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkSysTableScanPhysiPlanShowRewrite, pNode->showRewrite);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkSysTableScanPhysiPlanAccountId, pNode->accountId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkSysTableScanPhysiPlanSysInfo, pNode->sysInfo);
  }

  return code;
}

static int32_t jsonToPhysiSysTableScanNode(const SJson* pJson, void* pObj) {
  SSystemTableScanPhysiNode* pNode = (SSystemTableScanPhysiNode*)pObj;

  int32_t code = jsonToPhysiScanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonToObject(pJson, jkSysTableScanPhysiPlanMnodeEpSet, jsonToEpSet, &pNode->mgmtEpSet);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkSysTableScanPhysiPlanShowRewrite, &pNode->showRewrite);
  }
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkSysTableScanPhysiPlanAccountId, pNode->accountId, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkSysTableScanPhysiPlanSysInfo, &pNode->sysInfo);
  }

  return code;
}

static const char* jkProjectPhysiPlanProjections = "Projections";
static const char* jkProjectPhysiPlanMergeDataBlock = "MergeDataBlock";
static const char* jkProjectPhysiPlanIgnoreGroupId = "IgnoreGroupId";

static int32_t physiProjectNodeToJson(const void* pObj, SJson* pJson) {
  const SProjectPhysiNode* pNode = (const SProjectPhysiNode*)pObj;

  int32_t code = physicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkProjectPhysiPlanProjections, pNode->pProjections);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkProjectPhysiPlanMergeDataBlock, pNode->mergeDataBlock);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkProjectPhysiPlanIgnoreGroupId, pNode->ignoreGroupId);
  }

  return code;
}

static int32_t jsonToPhysiProjectNode(const SJson* pJson, void* pObj) {
  SProjectPhysiNode* pNode = (SProjectPhysiNode*)pObj;

  int32_t code = jsonToPhysicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkProjectPhysiPlanProjections, &pNode->pProjections);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkProjectPhysiPlanMergeDataBlock, &pNode->mergeDataBlock);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkProjectPhysiPlanIgnoreGroupId, &pNode->ignoreGroupId);
  }

  return code;
}

static const char* jkJoinPhysiPlanJoinType = "JoinType";
static const char* jkJoinPhysiPlanInputTsOrder = "InputTsOrder";
static const char* jkJoinPhysiPlanOnLeftCols = "OnLeftColumns";
static const char* jkJoinPhysiPlanOnRightCols = "OnRightColumns";
static const char* jkJoinPhysiPlanPrimKeyCondition = "PrimKeyCondition";
static const char* jkJoinPhysiPlanOnConditions = "OnConditions";
static const char* jkJoinPhysiPlanTargets = "Targets";
static const char* jkJoinPhysiPlanColEqualOnConditions = "ColumnEqualOnConditions";
static const char* jkJoinPhysiPlanInputRowNum = "InputRowNum";
static const char* jkJoinPhysiPlanInputRowSize = "InputRowSize";

static int32_t physiMergeJoinNodeToJson(const void* pObj, SJson* pJson) {
  const SSortMergeJoinPhysiNode* pNode = (const SSortMergeJoinPhysiNode*)pObj;

  int32_t code = physicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkJoinPhysiPlanJoinType, pNode->joinType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkJoinPhysiPlanPrimKeyCondition, nodeToJson, pNode->pPrimKeyCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkJoinPhysiPlanOnConditions, nodeToJson, pNode->pOtherOnCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkJoinPhysiPlanTargets, pNode->pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkJoinPhysiPlanColEqualOnConditions, nodeToJson, pNode->pColEqCond);
  }
  return code;
}

static int32_t jsonToPhysiMergeJoinNode(const SJson* pJson, void* pObj) {
  SSortMergeJoinPhysiNode* pNode = (SSortMergeJoinPhysiNode*)pObj;

  int32_t code = jsonToPhysicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkJoinPhysiPlanJoinType, pNode->joinType, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkJoinPhysiPlanOnConditions, &pNode->pOtherOnCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkJoinPhysiPlanPrimKeyCondition, &pNode->pPrimKeyCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkJoinPhysiPlanTargets, &pNode->pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkJoinPhysiPlanColEqualOnConditions, &pNode->pColEqCond);
  }
  return code;
}

static int32_t physiHashJoinNodeToJson(const void* pObj, SJson* pJson) {
  const SHashJoinPhysiNode* pNode = (const SHashJoinPhysiNode*)pObj;

  int32_t code = physicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkJoinPhysiPlanJoinType, pNode->joinType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkJoinPhysiPlanOnLeftCols, pNode->pOnLeft);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkJoinPhysiPlanOnRightCols, pNode->pOnRight);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkJoinPhysiPlanOnConditions, nodeToJson, pNode->pFilterConditions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkJoinPhysiPlanTargets, pNode->pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkJoinPhysiPlanInputRowNum, pNode->inputStat[0].inputRowNum);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkJoinPhysiPlanInputRowSize, pNode->inputStat[0].inputRowSize);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkJoinPhysiPlanInputRowNum, pNode->inputStat[1].inputRowNum);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkJoinPhysiPlanInputRowSize, pNode->inputStat[1].inputRowSize);
  }
  return code;
}


static int32_t jsonToPhysiHashJoinNode(const SJson* pJson, void* pObj) {
  SHashJoinPhysiNode* pNode = (SHashJoinPhysiNode*)pObj;

  int32_t code = jsonToPhysicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkJoinPhysiPlanJoinType, pNode->joinType, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkJoinPhysiPlanOnLeftCols, &pNode->pOnLeft);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkJoinPhysiPlanOnRightCols, &pNode->pOnRight);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkJoinPhysiPlanOnConditions, &pNode->pFilterConditions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkJoinPhysiPlanTargets, &pNode->pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkJoinPhysiPlanInputRowNum, pNode->inputStat[0].inputRowNum, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkJoinPhysiPlanInputRowSize, pNode->inputStat[0].inputRowSize, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkJoinPhysiPlanInputRowNum, pNode->inputStat[1].inputRowNum, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkJoinPhysiPlanInputRowSize, pNode->inputStat[1].inputRowSize, code);
  }
  return code;
}


static const char* jkAggPhysiPlanExprs = "Exprs";
static const char* jkAggPhysiPlanGroupKeys = "GroupKeys";
static const char* jkAggPhysiPlanAggFuncs = "AggFuncs";
static const char* jkAggPhysiPlanMergeDataBlock = "MergeDataBlock";
static const char* jkAggPhysiPlanGroupKeyOptimized = "GroupKeyOptimized";

static int32_t physiAggNodeToJson(const void* pObj, SJson* pJson) {
  const SAggPhysiNode* pNode = (const SAggPhysiNode*)pObj;

  int32_t code = physicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkAggPhysiPlanExprs, pNode->pExprs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkAggPhysiPlanGroupKeys, pNode->pGroupKeys);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkAggPhysiPlanAggFuncs, pNode->pAggFuncs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkAggPhysiPlanMergeDataBlock, pNode->mergeDataBlock);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkAggPhysiPlanGroupKeyOptimized, pNode->groupKeyOptimized);
  }

  return code;
}

static int32_t jsonToPhysiAggNode(const SJson* pJson, void* pObj) {
  SAggPhysiNode* pNode = (SAggPhysiNode*)pObj;

  int32_t code = jsonToPhysicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkAggPhysiPlanExprs, &pNode->pExprs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkAggPhysiPlanGroupKeys, &pNode->pGroupKeys);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkAggPhysiPlanAggFuncs, &pNode->pAggFuncs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkAggPhysiPlanMergeDataBlock, &pNode->mergeDataBlock);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkAggPhysiPlanGroupKeyOptimized, &pNode->groupKeyOptimized);
  }

  return code;
}

static const char* jkExchangePhysiPlanSrcStartGroupId = "SrcStartGroupId";
static const char* jkExchangePhysiPlanSrcEndGroupId = "SrcEndGroupId";
static const char* jkExchangePhysiPlanSrcEndPoints = "SrcEndPoints";
static const char* jkExchangePhysiPlanSeqRecvData = "SeqRecvData";

static int32_t physiExchangeNodeToJson(const void* pObj, SJson* pJson) {
  const SExchangePhysiNode* pNode = (const SExchangePhysiNode*)pObj;

  int32_t code = physicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkExchangePhysiPlanSrcStartGroupId, pNode->srcStartGroupId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkExchangePhysiPlanSrcEndGroupId, pNode->srcEndGroupId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkExchangePhysiPlanSrcEndPoints, pNode->pSrcEndPoints);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkExchangePhysiPlanSeqRecvData, pNode->seqRecvData);
  }

  return code;
}

static int32_t jsonToPhysiExchangeNode(const SJson* pJson, void* pObj) {
  SExchangePhysiNode* pNode = (SExchangePhysiNode*)pObj;

  int32_t code = jsonToPhysicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkExchangePhysiPlanSrcStartGroupId, &pNode->srcStartGroupId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkExchangePhysiPlanSrcEndGroupId, &pNode->srcEndGroupId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkExchangePhysiPlanSrcEndPoints, &pNode->pSrcEndPoints);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkExchangePhysiPlanSeqRecvData, &pNode->seqRecvData);
  }

  return code;
}

static const char* jkMergePhysiPlanMergeKeys = "MergeKeys";
static const char* jkMergePhysiPlanTargets = "Targets";
static const char* jkMergePhysiPlanNumOfChannels = "NumOfChannels";
static const char* jkMergePhysiPlanSrcGroupId = "SrcGroupId";
static const char* jkMergePhysiPlanGroupSort = "GroupSort";
static const char* jkMergePhysiPlanIgnoreGroupID = "IgnoreGroupID";
static const char* jkMergePhysiPlanInputWithGroupId = "InputWithGroupId";
static const char* jkMergePhysiPlanType = "Type";

static int32_t physiMergeNodeToJson(const void* pObj, SJson* pJson) {
  const SMergePhysiNode* pNode = (const SMergePhysiNode*)pObj;

  int32_t code = physicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkMergePhysiPlanMergeKeys, pNode->pMergeKeys);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkMergePhysiPlanTargets, pNode->pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkMergePhysiPlanNumOfChannels, pNode->numOfChannels);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkMergePhysiPlanSrcGroupId, pNode->srcGroupId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkMergePhysiPlanGroupSort, pNode->groupSort);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkMergePhysiPlanIgnoreGroupID, pNode->ignoreGroupId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkMergePhysiPlanInputWithGroupId, pNode->inputWithGroupId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkMergePhysiPlanType, pNode->type);
  }

  return code;
}

static int32_t jsonToPhysiMergeNode(const SJson* pJson, void* pObj) {
  SMergePhysiNode* pNode = (SMergePhysiNode*)pObj;

  int32_t code = jsonToPhysicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkMergePhysiPlanMergeKeys, &pNode->pMergeKeys);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkMergePhysiPlanTargets, &pNode->pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkMergePhysiPlanNumOfChannels, &pNode->numOfChannels);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkMergePhysiPlanSrcGroupId, &pNode->srcGroupId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkMergePhysiPlanGroupSort, &pNode->groupSort);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkMergePhysiPlanIgnoreGroupID, &pNode->ignoreGroupId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkMergePhysiPlanType, (int32_t*)&pNode->type);
  }

  return code;
}

static const char* jkSortPhysiPlanExprs = "Exprs";
static const char* jkSortPhysiPlanSortKeys = "SortKeys";
static const char* jkSortPhysiPlanTargets = "Targets";
static const char* jkSortPhysiPlanCalcGroupIds = "CalcGroupIds";
static const char* jkSortPhysiPlanExcludePKCol = "ExcludePKCol";

static int32_t physiSortNodeToJson(const void* pObj, SJson* pJson) {
  const SSortPhysiNode* pNode = (const SSortPhysiNode*)pObj;

  int32_t code = physicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkSortPhysiPlanExprs, pNode->pExprs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkSortPhysiPlanSortKeys, pNode->pSortKeys);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkSortPhysiPlanTargets, pNode->pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkSortPhysiPlanCalcGroupIds, pNode->calcGroupId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkSortPhysiPlanExcludePKCol, pNode->excludePkCol);
  }

  return code;
}

static int32_t jsonToPhysiSortNode(const SJson* pJson, void* pObj) {
  SSortPhysiNode* pNode = (SSortPhysiNode*)pObj;

  int32_t code = jsonToPhysicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkSortPhysiPlanExprs, &pNode->pExprs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkSortPhysiPlanSortKeys, &pNode->pSortKeys);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkSortPhysiPlanTargets, &pNode->pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkSortPhysiPlanCalcGroupIds, &pNode->calcGroupId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code= tjsonGetBoolValue(pJson, jkSortPhysiPlanExcludePKCol, &pNode->excludePkCol);
  }

  return code;
}

static const char* jkWindowPhysiPlanExprs = "Exprs";
static const char* jkWindowPhysiPlanFuncs = "Funcs";
static const char* jkWindowPhysiPlanTsPk = "TsPk";
static const char* jkWindowPhysiPlanTsEnd = "TsEnd";
static const char* jkWindowPhysiPlanTriggerType = "TriggerType";
static const char* jkWindowPhysiPlanWatermark = "Watermark";
static const char* jkWindowPhysiPlanDeleteMark = "DeleteMark";
static const char* jkWindowPhysiPlanIgnoreExpired = "IgnoreExpired";
static const char* jkWindowPhysiPlanInputTsOrder = "InputTsOrder";
static const char* jkWindowPhysiPlanMergeDataBlock = "MergeDataBlock";

static int32_t physiWindowNodeToJson(const void* pObj, SJson* pJson) {
  const SWindowPhysiNode* pNode = (const SWindowPhysiNode*)pObj;

  int32_t code = physicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkWindowPhysiPlanExprs, pNode->pExprs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkWindowPhysiPlanFuncs, pNode->pFuncs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkWindowPhysiPlanTsPk, nodeToJson, pNode->pTspk);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkWindowPhysiPlanTsEnd, nodeToJson, pNode->pTsEnd);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkWindowPhysiPlanTriggerType, pNode->triggerType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkWindowPhysiPlanWatermark, pNode->watermark);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkWindowPhysiPlanDeleteMark, pNode->deleteMark);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkWindowPhysiPlanIgnoreExpired, pNode->igExpired);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkWindowPhysiPlanMergeDataBlock, pNode->mergeDataBlock);
  }

  return code;
}

static int32_t jsonToPhysiWindowNode(const SJson* pJson, void* pObj) {
  SWindowPhysiNode* pNode = (SWindowPhysiNode*)pObj;

  int32_t code = jsonToPhysicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkWindowPhysiPlanExprs, &pNode->pExprs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkWindowPhysiPlanFuncs, &pNode->pFuncs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkWindowPhysiPlanTsPk, (SNode**)&pNode->pTspk);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkWindowPhysiPlanTsEnd, (SNode**)&pNode->pTsEnd);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkWindowPhysiPlanTriggerType, &pNode->triggerType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkWindowPhysiPlanWatermark, &pNode->watermark);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkWindowPhysiPlanDeleteMark, &pNode->deleteMark);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkWindowPhysiPlanIgnoreExpired, &pNode->igExpired);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkWindowPhysiPlanMergeDataBlock, &pNode->mergeDataBlock);
  }

  return code;
}

static const char* jkIntervalPhysiPlanInterval = "Interval";
static const char* jkIntervalPhysiPlanOffset = "Offset";
static const char* jkIntervalPhysiPlanSliding = "Sliding";
static const char* jkIntervalPhysiPlanIntervalUnit = "intervalUnit";
static const char* jkIntervalPhysiPlanSlidingUnit = "slidingUnit";

static int32_t physiIntervalNodeToJson(const void* pObj, SJson* pJson) {
  const SIntervalPhysiNode* pNode = (const SIntervalPhysiNode*)pObj;

  int32_t code = physiWindowNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkIntervalPhysiPlanInterval, pNode->interval);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkIntervalPhysiPlanOffset, pNode->offset);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkIntervalPhysiPlanSliding, pNode->sliding);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkIntervalPhysiPlanIntervalUnit, pNode->intervalUnit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkIntervalPhysiPlanSlidingUnit, pNode->slidingUnit);
  }

  return code;
}

static int32_t jsonToPhysiIntervalNode(const SJson* pJson, void* pObj) {
  SIntervalPhysiNode* pNode = (SIntervalPhysiNode*)pObj;

  int32_t code = jsonToPhysiWindowNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkIntervalPhysiPlanInterval, &pNode->interval);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkIntervalPhysiPlanOffset, &pNode->offset);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkIntervalPhysiPlanSliding, &pNode->sliding);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkIntervalPhysiPlanIntervalUnit, &pNode->intervalUnit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkIntervalPhysiPlanSlidingUnit, &pNode->slidingUnit);
  }

  return code;
}

static const char* jkFillPhysiPlanMode = "Mode";
static const char* jkFillPhysiPlanFillExprs = "FillExprs";
static const char* jkFillPhysiPlanNotFillExprs = "NotFillExprs";
static const char* jkFillPhysiPlanWStartTs = "WStartTs";
static const char* jkFillPhysiPlanValues = "Values";
static const char* jkFillPhysiPlanStartTime = "StartTime";
static const char* jkFillPhysiPlanEndTime = "EndTime";

static int32_t physiFillNodeToJson(const void* pObj, SJson* pJson) {
  const SFillPhysiNode* pNode = (const SFillPhysiNode*)pObj;

  int32_t code = physicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkFillPhysiPlanMode, pNode->mode);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkFillPhysiPlanFillExprs, pNode->pFillExprs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkFillPhysiPlanNotFillExprs, pNode->pNotFillExprs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkFillPhysiPlanWStartTs, nodeToJson, pNode->pWStartTs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkFillPhysiPlanValues, nodeToJson, pNode->pValues);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkFillPhysiPlanStartTime, pNode->timeRange.skey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkFillPhysiPlanEndTime, pNode->timeRange.ekey);
  }

  return code;
}

static int32_t jsonToPhysiFillNode(const SJson* pJson, void* pObj) {
  SFillPhysiNode* pNode = (SFillPhysiNode*)pObj;

  int32_t code = jsonToPhysicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkFillPhysiPlanMode, pNode->mode, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkFillPhysiPlanFillExprs, &pNode->pFillExprs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkFillPhysiPlanNotFillExprs, &pNode->pNotFillExprs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkFillPhysiPlanWStartTs, &pNode->pWStartTs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkFillPhysiPlanValues, &pNode->pValues);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkFillPhysiPlanStartTime, &pNode->timeRange.skey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkFillPhysiPlanEndTime, &pNode->timeRange.ekey);
  }

  return code;
}

static const char* jkSessionWindowPhysiPlanGap = "Gap";

static int32_t physiSessionWindowNodeToJson(const void* pObj, SJson* pJson) {
  const SSessionWinodwPhysiNode* pNode = (const SSessionWinodwPhysiNode*)pObj;

  int32_t code = physiWindowNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkSessionWindowPhysiPlanGap, pNode->gap);
  }

  return code;
}

static int32_t jsonToPhysiSessionWindowNode(const SJson* pJson, void* pObj) {
  SSessionWinodwPhysiNode* pNode = (SSessionWinodwPhysiNode*)pObj;

  int32_t code = jsonToPhysiWindowNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkSessionWindowPhysiPlanGap, pNode->gap, code);
  }

  return code;
}

static const char* jkStateWindowPhysiPlanStateKey = "StateKey";

static int32_t physiStateWindowNodeToJson(const void* pObj, SJson* pJson) {
  const SStateWinodwPhysiNode* pNode = (const SStateWinodwPhysiNode*)pObj;

  int32_t code = physiWindowNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkStateWindowPhysiPlanStateKey, nodeToJson, pNode->pStateKey);
  }

  return code;
}

static int32_t jsonToPhysiStateWindowNode(const SJson* pJson, void* pObj) {
  SStateWinodwPhysiNode* pNode = (SStateWinodwPhysiNode*)pObj;

  int32_t code = jsonToPhysiWindowNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkStateWindowPhysiPlanStateKey, &pNode->pStateKey);
  }

  return code;
}

static const char* jkEventWindowPhysiPlanStartCond = "StartCond";
static const char* jkEventWindowPhysiPlanEndCond = "EndCond";

static int32_t physiEventWindowNodeToJson(const void* pObj, SJson* pJson) {
  const SEventWinodwPhysiNode* pNode = (const SEventWinodwPhysiNode*)pObj;

  int32_t code = physiWindowNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkEventWindowPhysiPlanStartCond, nodeToJson, pNode->pStartCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkEventWindowPhysiPlanEndCond, nodeToJson, pNode->pEndCond);
  }

  return code;
}

static int32_t jsonToPhysiEventWindowNode(const SJson* pJson, void* pObj) {
  SEventWinodwPhysiNode* pNode = (SEventWinodwPhysiNode*)pObj;

  int32_t code = jsonToPhysiWindowNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkEventWindowPhysiPlanStartCond, &pNode->pStartCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkEventWindowPhysiPlanEndCond, &pNode->pEndCond);
  }

  return code;
}

static const char* jkPartitionPhysiPlanExprs = "Exprs";
static const char* jkPartitionPhysiPlanPartitionKeys = "PartitionKeys";
static const char* jkPartitionPhysiPlanTargets = "Targets";
static const char* jkPartitionPhysiPlanNeedBlockOutputTsOrder = "NeedBlockOutputTsOrder";
static const char* jkPartitionPhysiPlanTsSlotId = "tsSlotId";

static int32_t physiPartitionNodeToJson(const void* pObj, SJson* pJson) {
  const SPartitionPhysiNode* pNode = (const SPartitionPhysiNode*)pObj;

  int32_t code = physicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkPartitionPhysiPlanExprs, pNode->pExprs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkPartitionPhysiPlanPartitionKeys, pNode->pPartitionKeys);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkPartitionPhysiPlanTargets, pNode->pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) {
    tjsonAddBoolToObject(pJson, jkPartitionPhysiPlanNeedBlockOutputTsOrder, pNode->needBlockOutputTsOrder);
  }
  if (TSDB_CODE_SUCCESS == code) {
    tjsonAddIntegerToObject(pJson, jkPartitionPhysiPlanTsSlotId, pNode->tsSlotId);
  }

  return code;
}

static int32_t jsonToPhysiPartitionNode(const SJson* pJson, void* pObj) {
  SPartitionPhysiNode* pNode = (SPartitionPhysiNode*)pObj;

  int32_t code = jsonToPhysicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkPartitionPhysiPlanExprs, &pNode->pExprs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkPartitionPhysiPlanPartitionKeys, &pNode->pPartitionKeys);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkPartitionPhysiPlanTargets, &pNode->pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkPartitionPhysiPlanNeedBlockOutputTsOrder, &pNode->needBlockOutputTsOrder);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkPartitionPhysiPlanTsSlotId, &pNode->tsSlotId);
  }

  return code;
}

static const char* jkStreamPartitionPhysiPlanTags = "Tags";
static const char* jkStreamPartitionPhysiPlanSubtable = "Subtable";

static int32_t physiStreamPartitionNodeToJson(const void* pObj, SJson* pJson) {
  const SStreamPartitionPhysiNode* pNode = (const SStreamPartitionPhysiNode*)pObj;

  int32_t code = physiPartitionNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkStreamPartitionPhysiPlanTags, pNode->pTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkStreamPartitionPhysiPlanSubtable, nodeToJson, pNode->pSubtable);
  }

  return code;
}

static int32_t jsonToPhysiStreamPartitionNode(const SJson* pJson, void* pObj) {
  SStreamPartitionPhysiNode* pNode = (SStreamPartitionPhysiNode*)pObj;

  int32_t code = jsonToPhysiPartitionNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkStreamPartitionPhysiPlanTags, &pNode->pTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkStreamPartitionPhysiPlanSubtable, &pNode->pSubtable);
  }

  return code;
}

static const char* jkIndefRowsFuncPhysiPlanExprs = "Exprs";
static const char* jkIndefRowsFuncPhysiPlanFuncs = "Funcs";

static int32_t physiIndefRowsFuncNodeToJson(const void* pObj, SJson* pJson) {
  const SIndefRowsFuncPhysiNode* pNode = (const SIndefRowsFuncPhysiNode*)pObj;

  int32_t code = physicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkIndefRowsFuncPhysiPlanExprs, pNode->pExprs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkIndefRowsFuncPhysiPlanFuncs, pNode->pFuncs);
  }

  return code;
}

static int32_t jsonToPhysiIndefRowsFuncNode(const SJson* pJson, void* pObj) {
  SIndefRowsFuncPhysiNode* pNode = (SIndefRowsFuncPhysiNode*)pObj;

  int32_t code = jsonToPhysicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkIndefRowsFuncPhysiPlanExprs, &pNode->pExprs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkIndefRowsFuncPhysiPlanFuncs, &pNode->pFuncs);
  }

  return code;
}

static const char* jkInterpFuncPhysiPlanExprs = "Exprs";
static const char* jkInterpFuncPhysiPlanFuncs = "Funcs";
static const char* jkInterpFuncPhysiPlanStartTime = "StartTime";
static const char* jkInterpFuncPhysiPlanEndTime = "EndTime";
static const char* jkInterpFuncPhysiPlanInterval = "Interval";
static const char* jkInterpFuncPhysiPlanFillMode = "FillMode";
static const char* jkInterpFuncPhysiPlanFillValues = "FillValues";
static const char* jkInterpFuncPhysiPlanTimeSeries = "TimeSeries";

static int32_t physiInterpFuncNodeToJson(const void* pObj, SJson* pJson) {
  const SInterpFuncPhysiNode* pNode = (const SInterpFuncPhysiNode*)pObj;

  int32_t code = physicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkInterpFuncPhysiPlanExprs, pNode->pExprs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkInterpFuncPhysiPlanFuncs, pNode->pFuncs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkInterpFuncPhysiPlanStartTime, pNode->timeRange.skey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkInterpFuncPhysiPlanEndTime, pNode->timeRange.ekey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkInterpFuncPhysiPlanInterval, pNode->interval);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkInterpFuncPhysiPlanFillMode, pNode->fillMode);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkInterpFuncPhysiPlanFillValues, nodeToJson, pNode->pFillValues);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkInterpFuncPhysiPlanTimeSeries, nodeToJson, pNode->pTimeSeries);
  }

  return code;
}

static int32_t jsonToPhysiInterpFuncNode(const SJson* pJson, void* pObj) {
  SInterpFuncPhysiNode* pNode = (SInterpFuncPhysiNode*)pObj;

  int32_t code = jsonToPhysicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkInterpFuncPhysiPlanExprs, &pNode->pExprs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkInterpFuncPhysiPlanFuncs, &pNode->pFuncs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkInterpFuncPhysiPlanStartTime, &pNode->timeRange.skey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkInterpFuncPhysiPlanEndTime, &pNode->timeRange.ekey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkInterpFuncPhysiPlanInterval, &pNode->interval);
  }
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkInterpFuncPhysiPlanFillMode, pNode->fillMode, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkInterpFuncPhysiPlanFillValues, &pNode->pFillValues);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkInterpFuncPhysiPlanTimeSeries, &pNode->pTimeSeries);
  }

  return code;
}

static const char* jkDataSinkInputDataBlockDesc = "InputDataBlockDesc";

static int32_t physicDataSinkNodeToJson(const void* pObj, SJson* pJson) {
  const SDataSinkNode* pNode = (const SDataSinkNode*)pObj;
  return tjsonAddObject(pJson, jkDataSinkInputDataBlockDesc, nodeToJson, pNode->pInputDataBlockDesc);
}

static int32_t jsonToPhysicDataSinkNode(const SJson* pJson, void* pObj) {
  SDataSinkNode* pNode = (SDataSinkNode*)pObj;
  return jsonToNodeObject(pJson, jkDataSinkInputDataBlockDesc, (SNode**)&pNode->pInputDataBlockDesc);
}

static int32_t physiDispatchNodeToJson(const void* pObj, SJson* pJson) { return physicDataSinkNodeToJson(pObj, pJson); }

static int32_t jsonToPhysiDispatchNode(const SJson* pJson, void* pObj) { return jsonToPhysicDataSinkNode(pJson, pObj); }

static const char* jkQueryInsertPhysiPlanInsertCols = "InsertCols";
static const char* jkQueryInsertPhysiPlanStableId = "StableId";
static const char* jkQueryInsertPhysiPlanTableId = "TableId";
static const char* jkQueryInsertPhysiPlanTableType = "TableType";
static const char* jkQueryInsertPhysiPlanTableFName = "TableFName";
static const char* jkQueryInsertPhysiPlanVgId = "VgId";
static const char* jkQueryInsertPhysiPlanEpSet = "EpSet";
static const char* jkQueryInsertPhysiPlanExplain = "Explain";

static int32_t physiQueryInsertNodeToJson(const void* pObj, SJson* pJson) {
  const SQueryInserterNode* pNode = (const SQueryInserterNode*)pObj;

  int32_t code = physicDataSinkNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkQueryInsertPhysiPlanInsertCols, pNode->pCols);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkQueryInsertPhysiPlanStableId, pNode->stableId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkQueryInsertPhysiPlanTableId, pNode->tableId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkQueryInsertPhysiPlanTableType, pNode->tableType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkQueryInsertPhysiPlanTableFName, pNode->tableName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkQueryInsertPhysiPlanVgId, pNode->vgId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkQueryInsertPhysiPlanEpSet, epSetToJson, &pNode->epSet);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkQueryInsertPhysiPlanExplain, pNode->explain);
  }

  return code;
}

static int32_t jsonToPhysiQueryInsertNode(const SJson* pJson, void* pObj) {
  SQueryInserterNode* pNode = (SQueryInserterNode*)pObj;

  int32_t code = jsonToPhysicDataSinkNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkQueryInsertPhysiPlanInsertCols, &pNode->pCols);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetUBigIntValue(pJson, jkQueryInsertPhysiPlanStableId, &pNode->stableId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetUBigIntValue(pJson, jkQueryInsertPhysiPlanTableId, &pNode->tableId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkQueryInsertPhysiPlanTableType, &pNode->tableType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkQueryInsertPhysiPlanTableFName, pNode->tableName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkQueryInsertPhysiPlanVgId, &pNode->vgId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonToObject(pJson, jkQueryInsertPhysiPlanEpSet, jsonToEpSet, &pNode->epSet);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkQueryInsertPhysiPlanExplain, &pNode->explain);
  }

  return code;
}

static const char* jkDeletePhysiPlanTableId = "TableId";
static const char* jkDeletePhysiPlanTableType = "TableType";
static const char* jkDeletePhysiPlanTableFName = "TableFName";
static const char* jkDeletePhysiPlanTsColName = "TsColName";
static const char* jkDeletePhysiPlanDeleteTimeRangeStartKey = "DeleteTimeRangeStartKey";
static const char* jkDeletePhysiPlanDeleteTimeRangeEndKey = "DeleteTimeRangeEndKey";
static const char* jkDeletePhysiPlanAffectedRows = "AffectedRows";
static const char* jkDeletePhysiPlanStartTs = "StartTs";
static const char* jkDeletePhysiPlanEndTs = "EndTs";

static int32_t physiDeleteNodeToJson(const void* pObj, SJson* pJson) {
  const SDataDeleterNode* pNode = (const SDataDeleterNode*)pObj;

  int32_t code = physicDataSinkNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDeletePhysiPlanTableId, pNode->tableId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDeletePhysiPlanTableType, pNode->tableType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkDeletePhysiPlanTableFName, pNode->tableFName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkDeletePhysiPlanTsColName, pNode->tsColName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDeletePhysiPlanDeleteTimeRangeStartKey, pNode->deleteTimeRange.skey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDeletePhysiPlanDeleteTimeRangeEndKey, pNode->deleteTimeRange.ekey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkDeletePhysiPlanAffectedRows, nodeToJson, pNode->pAffectedRows);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkDeletePhysiPlanStartTs, nodeToJson, pNode->pStartTs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkDeletePhysiPlanEndTs, nodeToJson, pNode->pEndTs);
  }

  return code;
}

static int32_t jsonToPhysiDeleteNode(const SJson* pJson, void* pObj) {
  SDataDeleterNode* pNode = (SDataDeleterNode*)pObj;

  int32_t code = jsonToPhysicDataSinkNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetUBigIntValue(pJson, jkDeletePhysiPlanTableId, &pNode->tableId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkDeletePhysiPlanTableType, &pNode->tableType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkDeletePhysiPlanTableFName, pNode->tableFName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkDeletePhysiPlanTsColName, pNode->tsColName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkDeletePhysiPlanDeleteTimeRangeStartKey, &pNode->deleteTimeRange.skey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkDeletePhysiPlanDeleteTimeRangeEndKey, &pNode->deleteTimeRange.ekey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkDeletePhysiPlanAffectedRows, &pNode->pAffectedRows);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkDeletePhysiPlanStartTs, &pNode->pStartTs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkDeletePhysiPlanEndTs, &pNode->pEndTs);
  }

  return code;
}

static const char* jkGroupCachePhysiPlanGroupCols = "GroupColumns";
static const char* jkGroupCachePhysiPlanGrpColsMayBeNull = "GroupColumnsMayBeNull";
static const char* jkGroupCachePhysiPlanGroupByUid = "GroupByUid";
static const char* jkGroupCachePhysiPlanGlobalGroup = "GlobalGroup";
static const char* jkGroupCachePhysiPlanBatchFetch = "BatchFetch";


static int32_t physiGroupCacheNodeToJson(const void* pObj, SJson* pJson) {
  const SGroupCachePhysiNode* pNode = (const SGroupCachePhysiNode*)pObj;

  int32_t code = physicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkGroupCachePhysiPlanGrpColsMayBeNull, pNode->grpColsMayBeNull);
  }  
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkGroupCachePhysiPlanGroupByUid, pNode->grpByUid);
  }  
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkGroupCachePhysiPlanGlobalGroup, pNode->globalGrp);
  }  
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkGroupCachePhysiPlanBatchFetch, pNode->batchFetch);
  }  
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkGroupCachePhysiPlanGroupCols, pNode->pGroupCols);
  }
  return code;
}

static int32_t jsonToPhysiGroupCacheNode(const SJson* pJson, void* pObj) {
  SGroupCachePhysiNode* pNode = (SGroupCachePhysiNode*)pObj;

  int32_t code = jsonToPhysicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkGroupCachePhysiPlanGrpColsMayBeNull, &pNode->grpColsMayBeNull);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkGroupCachePhysiPlanGroupByUid, &pNode->grpByUid);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkGroupCachePhysiPlanGlobalGroup, &pNode->globalGrp);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkGroupCachePhysiPlanBatchFetch, &pNode->batchFetch);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkGroupCachePhysiPlanGroupCols, &pNode->pGroupCols);
  }
  return code;
}

static const char* jkDynQueryCtrlPhysiPlanQueryType = "QueryType";
static const char* jkDynQueryCtrlPhysiPlanBatchFetch = "BatchFetch";
static const char* jkDynQueryCtrlPhysiPlanVgSlot0 = "VgSlot[0]";
static const char* jkDynQueryCtrlPhysiPlanVgSlot1 = "VgSlot[1]";
static const char* jkDynQueryCtrlPhysiPlanUidSlot0 = "UidSlot[0]";
static const char* jkDynQueryCtrlPhysiPlanUidSlot1 = "UidSlot[1]";
static const char* jkDynQueryCtrlPhysiPlanSrcScan0 = "SrcScan[0]";
static const char* jkDynQueryCtrlPhysiPlanSrcScan1 = "SrcScan[1]";

static int32_t physiDynQueryCtrlNodeToJson(const void* pObj, SJson* pJson) {
  const SDynQueryCtrlPhysiNode* pNode = (const SDynQueryCtrlPhysiNode*)pObj;

  int32_t code = physicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDynQueryCtrlPhysiPlanQueryType, pNode->qType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    switch (pNode->qType) {
      case DYN_QTYPE_STB_HASH: {
        code = tjsonAddBoolToObject(pJson, jkDynQueryCtrlPhysiPlanBatchFetch, pNode->stbJoin.batchFetch);
        if (TSDB_CODE_SUCCESS == code) {
          code = tjsonAddIntegerToObject(pJson, jkDynQueryCtrlPhysiPlanVgSlot0, pNode->stbJoin.vgSlot[0]);
        }
        if (TSDB_CODE_SUCCESS == code) {
          code = tjsonAddIntegerToObject(pJson, jkDynQueryCtrlPhysiPlanVgSlot1, pNode->stbJoin.vgSlot[1]);
        }
        if (TSDB_CODE_SUCCESS == code) {
          code = tjsonAddIntegerToObject(pJson, jkDynQueryCtrlPhysiPlanUidSlot0, pNode->stbJoin.uidSlot[0]);
        }
        if (TSDB_CODE_SUCCESS == code) {
          code = tjsonAddIntegerToObject(pJson, jkDynQueryCtrlPhysiPlanUidSlot1, pNode->stbJoin.uidSlot[1]);
        }
        if (TSDB_CODE_SUCCESS == code) {
          code = tjsonAddBoolToObject(pJson, jkDynQueryCtrlPhysiPlanSrcScan0, pNode->stbJoin.srcScan[0]);
        }
        if (TSDB_CODE_SUCCESS == code) {
          code = tjsonAddBoolToObject(pJson, jkDynQueryCtrlPhysiPlanSrcScan1, pNode->stbJoin.srcScan[1]);
        }
        break;
      }
      default:
        return TSDB_CODE_INVALID_PARA;
    }
  }
  return code;
}

static int32_t jsonToPhysiDynQueryCtrlNode(const SJson* pJson, void* pObj) {
  SDynQueryCtrlPhysiNode* pNode = (SDynQueryCtrlPhysiNode*)pObj;

  int32_t code = jsonToPhysicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkDynQueryCtrlPhysiPlanQueryType, pNode->qType, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    switch (pNode->qType) {
      case DYN_QTYPE_STB_HASH: {
        tjsonGetNumberValue(pJson, jkDynQueryCtrlPhysiPlanQueryType, pNode->qType, code);
        if (TSDB_CODE_SUCCESS == code) {
          code = tjsonGetBoolValue(pJson, jkDynQueryCtrlPhysiPlanBatchFetch, &pNode->stbJoin.batchFetch);
        }
        if (TSDB_CODE_SUCCESS == code) {
          tjsonGetNumberValue(pJson, jkDynQueryCtrlPhysiPlanVgSlot0, pNode->stbJoin.vgSlot[0], code);
        }
        if (TSDB_CODE_SUCCESS == code) {
          tjsonGetNumberValue(pJson, jkDynQueryCtrlPhysiPlanVgSlot1, pNode->stbJoin.vgSlot[1], code);
        }
        if (TSDB_CODE_SUCCESS == code) {
          tjsonGetNumberValue(pJson, jkDynQueryCtrlPhysiPlanUidSlot0, pNode->stbJoin.uidSlot[0], code);
        }
        if (TSDB_CODE_SUCCESS == code) {
          tjsonGetNumberValue(pJson, jkDynQueryCtrlPhysiPlanUidSlot1, pNode->stbJoin.uidSlot[1], code);
        }
        if (TSDB_CODE_SUCCESS == code) {
          code = tjsonGetBoolValue(pJson, jkDynQueryCtrlPhysiPlanSrcScan0, &pNode->stbJoin.srcScan[0]);
        }
        if (TSDB_CODE_SUCCESS == code) {
          code = tjsonGetBoolValue(pJson, jkDynQueryCtrlPhysiPlanSrcScan1, &pNode->stbJoin.srcScan[1]);
        }
        break;
      }
      default:
        return TSDB_CODE_INVALID_PARA;
    }
  }

  return code;
}



static const char* jkQueryNodeAddrId = "Id";
static const char* jkQueryNodeAddrInUse = "InUse";
static const char* jkQueryNodeAddrNumOfEps = "NumOfEps";
static const char* jkQueryNodeAddrEps = "Eps";

static int32_t queryNodeAddrToJson(const void* pObj, SJson* pJson) {
  const SQueryNodeAddr* pNode = (const SQueryNodeAddr*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkQueryNodeAddrId, pNode->nodeId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkQueryNodeAddrInUse, pNode->epSet.inUse);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkQueryNodeAddrNumOfEps, pNode->epSet.numOfEps);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddArray(pJson, jkQueryNodeAddrEps, epToJson, pNode->epSet.eps, sizeof(SEp), pNode->epSet.numOfEps);
  }

  return code;
}

static int32_t jsonToQueryNodeAddr(const SJson* pJson, void* pObj) {
  SQueryNodeAddr* pNode = (SQueryNodeAddr*)pObj;

  int32_t code = tjsonGetIntValue(pJson, jkQueryNodeAddrId, &pNode->nodeId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkQueryNodeAddrInUse, &pNode->epSet.inUse);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkQueryNodeAddrNumOfEps, &pNode->epSet.numOfEps);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonToArray(pJson, jkQueryNodeAddrEps, jsonToEp, pNode->epSet.eps, sizeof(SEp));
  }

  return code;
}

static const char* jkSubplanId = "Id";
static const char* jkSubplanType = "SubplanType";
static const char* jkSubplanMsgType = "MsgType";
static const char* jkSubplanLevel = "Level";
static const char* jkSubplanDbFName = "DbFName";
static const char* jkSubplanUser = "User";
static const char* jkSubplanNodeAddr = "NodeAddr";
static const char* jkSubplanRootNode = "RootNode";
static const char* jkSubplanDataSink = "DataSink";
static const char* jkSubplanTagCond = "TagCond";
static const char* jkSubplanTagIndexCond = "TagIndexCond";
static const char* jkSubplanShowRewrite = "ShowRewrite";
static const char* jkSubplanRowsThreshold = "RowThreshold";
static const char* jkSubplanDynamicRowsThreshold = "DyRowThreshold";

static int32_t subplanToJson(const void* pObj, SJson* pJson) {
  const SSubplan* pNode = (const SSubplan*)pObj;

  int32_t code = tjsonAddObject(pJson, jkSubplanId, subplanIdToJson, &pNode->id);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkSubplanType, pNode->subplanType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkSubplanMsgType, pNode->msgType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkSubplanLevel, pNode->level);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkSubplanDbFName, pNode->dbFName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkSubplanUser, pNode->user);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkSubplanNodeAddr, queryNodeAddrToJson, &pNode->execNode);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkSubplanRootNode, nodeToJson, pNode->pNode);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkSubplanDataSink, nodeToJson, pNode->pDataSink);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkSubplanTagCond, nodeToJson, pNode->pTagCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkSubplanTagIndexCond, nodeToJson, pNode->pTagIndexCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkSubplanShowRewrite, pNode->showRewrite);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkSubplanRowsThreshold, pNode->rowsThreshold);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkSubplanDynamicRowsThreshold, pNode->dynamicRowThreshold);
  }

  return code;
}

static int32_t jsonToSubplan(const SJson* pJson, void* pObj) {
  SSubplan* pNode = (SSubplan*)pObj;

  int32_t code = tjsonToObject(pJson, jkSubplanId, jsonToSubplanId, &pNode->id);
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkSubplanType, pNode->subplanType, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkSubplanMsgType, &pNode->msgType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkSubplanLevel, &pNode->level);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkSubplanDbFName, pNode->dbFName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkSubplanUser, pNode->user);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonToObject(pJson, jkSubplanNodeAddr, jsonToQueryNodeAddr, &pNode->execNode);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkSubplanRootNode, (SNode**)&pNode->pNode);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkSubplanDataSink, (SNode**)&pNode->pDataSink);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkSubplanTagCond, (SNode**)&pNode->pTagCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkSubplanTagIndexCond, (SNode**)&pNode->pTagIndexCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkSubplanShowRewrite, &pNode->showRewrite);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkSubplanRowsThreshold, &pNode->rowsThreshold);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkSubplanDynamicRowsThreshold, &pNode->dynamicRowThreshold);
  }

  return code;
}

static const char* jkPlanQueryId = "QueryId";
static const char* jkPlanNumOfSubplans = "NumOfSubplans";
static const char* jkPlanSubplans = "Subplans";

static int32_t planToJson(const void* pObj, SJson* pJson) {
  const SQueryPlan* pNode = (const SQueryPlan*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkPlanQueryId, pNode->queryId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkPlanNumOfSubplans, pNode->numOfSubplans);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkPlanSubplans, pNode->pSubplans);
  }

  return code;
}

static int32_t jsonToPlan(const SJson* pJson, void* pObj) {
  SQueryPlan* pNode = (SQueryPlan*)pObj;

  int32_t code = tjsonGetUBigIntValue(pJson, jkPlanQueryId, &pNode->queryId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkPlanNumOfSubplans, &pNode->numOfSubplans);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkPlanSubplans, &pNode->pSubplans);
  }

  return code;
}

static const char* jkAggLogicPlanGroupKeys = "GroupKeys";
static const char* jkAggLogicPlanAggFuncs = "AggFuncs";

static int32_t logicAggNodeToJson(const void* pObj, SJson* pJson) {
  const SAggLogicNode* pNode = (const SAggLogicNode*)pObj;

  int32_t code = logicPlanNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkAggLogicPlanGroupKeys, pNode->pGroupKeys);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkAggLogicPlanAggFuncs, pNode->pAggFuncs);
  }

  return code;
}

static int32_t jsonToLogicAggNode(const SJson* pJson, void* pObj) {
  SAggLogicNode* pNode = (SAggLogicNode*)pObj;

  int32_t code = jsonToLogicPlanNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkAggLogicPlanGroupKeys, &pNode->pGroupKeys);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkAggLogicPlanAggFuncs, &pNode->pAggFuncs);
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

static int32_t jsonToDataType(const SJson* pJson, void* pObj) {
  SDataType* pNode = (SDataType*)pObj;

  int32_t code = tjsonGetUTinyIntValue(pJson, jkDataTypeType, &pNode->type);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetUTinyIntValue(pJson, jkDataTypePrecision, &pNode->precision);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetUTinyIntValue(pJson, jkDataTypeScale, &pNode->scale);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkDataTypeDataBytes, &pNode->bytes);
  }

  return TSDB_CODE_SUCCESS;
}

static const char* jkExprDataType = "DataType";
static const char* jkExprAliasName = "AliasName";
static const char* jkExprUserAlias = "UserAlias";

static int32_t exprNodeToJson(const void* pObj, SJson* pJson) {
  const SExprNode* pNode = (const SExprNode*)pObj;

  int32_t code = tjsonAddObject(pJson, jkExprDataType, dataTypeToJson, &pNode->resType);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkExprAliasName, pNode->aliasName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkExprUserAlias, pNode->userAlias);
  }

  return code;
}

static int32_t jsonToExprNode(const SJson* pJson, void* pObj) {
  SExprNode* pNode = (SExprNode*)pObj;

  int32_t code = tjsonToObject(pJson, jkExprDataType, jsonToDataType, &pNode->resType);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkExprAliasName, pNode->aliasName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkExprUserAlias, pNode->userAlias);
  }

  return code;
}

static const char* jkColumnTableId = "TableId";
static const char* jkColumnTableType = "TableType";
static const char* jkColumnColId = "ColId";
static const char* jkColumnColType = "ColType";
static const char* jkColumnProjId = "ProjId";
static const char* jkColumnDbName = "DbName";
static const char* jkColumnTableName = "TableName";
static const char* jkColumnTableAlias = "TableAlias";
static const char* jkColumnColName = "ColName";
static const char* jkColumnDataBlockId = "DataBlockId";
static const char* jkColumnSlotId = "SlotId";

static int32_t columnNodeToJson(const void* pObj, SJson* pJson) {
  const SColumnNode* pNode = (const SColumnNode*)pObj;

  int32_t code = exprNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkColumnTableId, pNode->tableId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkColumnTableType, pNode->tableType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkColumnColId, pNode->colId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkColumnProjId, pNode->projIdx);
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
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkColumnDataBlockId, pNode->dataBlockId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkColumnSlotId, pNode->slotId);
  }

  return code;
}

static int32_t jsonToColumnNode(const SJson* pJson, void* pObj) {
  SColumnNode* pNode = (SColumnNode*)pObj;

  int32_t code = jsonToExprNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetUBigIntValue(pJson, jkColumnTableId, &pNode->tableId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkColumnTableType, &pNode->tableType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetSmallIntValue(pJson, jkColumnColId, &pNode->colId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetSmallIntValue(pJson, jkColumnProjId, &pNode->projIdx);
  }
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkColumnColType, pNode->colType, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkColumnDbName, pNode->dbName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkColumnTableName, pNode->tableName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkColumnTableAlias, pNode->tableAlias);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkColumnColName, pNode->colName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetSmallIntValue(pJson, jkColumnDataBlockId, &pNode->dataBlockId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetSmallIntValue(pJson, jkColumnSlotId, &pNode->slotId);
  }

  return code;
}

static const char* jkValueLiteralSize = "LiteralSize";
static const char* jkValueLiteral = "Literal";
static const char* jkValueDuration = "Duration";
static const char* jkValueTranslate = "Translate";
static const char* jkValueNotReserved = "NotReserved";
static const char* jkValueIsNull = "IsNull";
static const char* jkValueUnit = "Unit";
static const char* jkValueDatum = "Datum";

static int32_t datumToJson(const void* pObj, SJson* pJson) {
  const SValueNode* pNode = (const SValueNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  switch (pNode->node.resType.type) {
    case TSDB_DATA_TYPE_NULL:
      break;
    case TSDB_DATA_TYPE_BOOL:
      code = tjsonAddBoolToObject(pJson, jkValueDatum, pNode->datum.b);
      break;
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      code = tjsonAddIntegerToObject(pJson, jkValueDatum, pNode->datum.i);
      break;
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT:
      code = tjsonAddIntegerToObject(pJson, jkValueDatum, pNode->datum.u);
      break;
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE:
      code = tjsonAddDoubleToObject(pJson, jkValueDatum, pNode->datum.d);
      break;
    case TSDB_DATA_TYPE_NCHAR: {
      // cJSON only support utf-8 encoding. Convert memory content to hex string.
      char* buf = taosMemoryCalloc(varDataLen(pNode->datum.p) * 2 + 1, sizeof(char));
      code = taosHexEncode(varDataVal(pNode->datum.p), buf, varDataLen(pNode->datum.p));
      if (code != TSDB_CODE_SUCCESS) {
        taosMemoryFree(buf);
        return TSDB_CODE_TSC_INVALID_VALUE;
      }
      code = tjsonAddStringToObject(pJson, jkValueDatum, buf);
      taosMemoryFree(buf);
      break;
    }
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_GEOMETRY:
      code = tjsonAddStringToObject(pJson, jkValueDatum, varDataVal(pNode->datum.p));
      break;
    case TSDB_DATA_TYPE_JSON: {
      int32_t len = getJsonValueLen(pNode->datum.p);
      char*   buf = taosMemoryCalloc(len * 2 + 1, sizeof(char));
      code = taosHexEncode(pNode->datum.p, buf, len);
      if (code != TSDB_CODE_SUCCESS) {
        taosMemoryFree(buf);
        return TSDB_CODE_TSC_INVALID_VALUE;
      }
      code = tjsonAddStringToObject(pJson, jkValueDatum, buf);
      taosMemoryFree(buf);
      break;
    }
    case TSDB_DATA_TYPE_DECIMAL:
    case TSDB_DATA_TYPE_BLOB:
      // todo
    default:
      break;
  }

  return code;
}

static int32_t valueNodeToJson(const void* pObj, SJson* pJson) {
  const SValueNode* pNode = (const SValueNode*)pObj;

  int32_t code = exprNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkValueLiteralSize, NULL != pNode->literal ? strlen(pNode->literal) : 0);
  }
  if (TSDB_CODE_SUCCESS == code && NULL != pNode->literal) {
    code = tjsonAddStringToObject(pJson, jkValueLiteral, pNode->literal);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkValueDuration, pNode->isDuration);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkValueTranslate, pNode->translate);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkValueNotReserved, pNode->notReserved);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkValueIsNull, pNode->isNull);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkValueUnit, pNode->unit);
  }
  if (TSDB_CODE_SUCCESS == code && pNode->translate && !pNode->isNull) {
    code = datumToJson(pNode, pJson);
  }

  return code;
}

static int32_t jsonToDatum(const SJson* pJson, void* pObj) {
  SValueNode* pNode = (SValueNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  switch (pNode->node.resType.type) {
    case TSDB_DATA_TYPE_NULL:
      break;
    case TSDB_DATA_TYPE_BOOL:
      code = tjsonGetBoolValue(pJson, jkValueDatum, &pNode->datum.b);
      *(bool*)&pNode->typeData = pNode->datum.b;
      break;
    case TSDB_DATA_TYPE_TINYINT:
      code = tjsonGetBigIntValue(pJson, jkValueDatum, &pNode->datum.i);
      *(int8_t*)&pNode->typeData = pNode->datum.i;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      code = tjsonGetBigIntValue(pJson, jkValueDatum, &pNode->datum.i);
      *(int16_t*)&pNode->typeData = pNode->datum.i;
      break;
    case TSDB_DATA_TYPE_INT:
      code = tjsonGetBigIntValue(pJson, jkValueDatum, &pNode->datum.i);
      *(int32_t*)&pNode->typeData = pNode->datum.i;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      code = tjsonGetBigIntValue(pJson, jkValueDatum, &pNode->datum.i);
      *(int64_t*)&pNode->typeData = pNode->datum.i;
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      code = tjsonGetBigIntValue(pJson, jkValueDatum, &pNode->datum.i);
      *(int64_t*)&pNode->typeData = pNode->datum.i;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      code = tjsonGetUBigIntValue(pJson, jkValueDatum, &pNode->datum.u);
      *(uint8_t*)&pNode->typeData = pNode->datum.u;
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      code = tjsonGetUBigIntValue(pJson, jkValueDatum, &pNode->datum.u);
      *(uint16_t*)&pNode->typeData = pNode->datum.u;
      break;
    case TSDB_DATA_TYPE_UINT:
      code = tjsonGetUBigIntValue(pJson, jkValueDatum, &pNode->datum.u);
      *(uint32_t*)&pNode->typeData = pNode->datum.u;
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      code = tjsonGetUBigIntValue(pJson, jkValueDatum, &pNode->datum.u);
      *(uint64_t*)&pNode->typeData = pNode->datum.u;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      code = tjsonGetDoubleValue(pJson, jkValueDatum, &pNode->datum.d);
      *(float*)&pNode->typeData = pNode->datum.d;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      code = tjsonGetDoubleValue(pJson, jkValueDatum, &pNode->datum.d);
      *(double*)&pNode->typeData = pNode->datum.d;
      break;
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_GEOMETRY: {
      pNode->datum.p = taosMemoryCalloc(1, pNode->node.resType.bytes + 1);
      if (NULL == pNode->datum.p) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        break;
      }
      varDataSetLen(pNode->datum.p, pNode->node.resType.bytes - VARSTR_HEADER_SIZE);
      if (TSDB_DATA_TYPE_NCHAR == pNode->node.resType.type) {
        char* buf = taosMemoryCalloc(1, pNode->node.resType.bytes * 2 + VARSTR_HEADER_SIZE + 1);
        if (NULL == buf) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          break;
        }
        code = tjsonGetStringValue(pJson, jkValueDatum, buf);
        if (code != TSDB_CODE_SUCCESS) {
          taosMemoryFree(buf);
          break;
        }
        code = taosHexDecode(buf, varDataVal(pNode->datum.p), pNode->node.resType.bytes - VARSTR_HEADER_SIZE);
        if (code != TSDB_CODE_SUCCESS) {
          taosMemoryFree(buf);
          break;
        }
        taosMemoryFree(buf);
      } else {
        code = tjsonGetStringValue(pJson, jkValueDatum, varDataVal(pNode->datum.p));
      }
      break;
    }
    case TSDB_DATA_TYPE_JSON: {
      pNode->datum.p = taosMemoryCalloc(1, pNode->node.resType.bytes);
      if (NULL == pNode->datum.p) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        break;
      }
      char* buf = taosMemoryCalloc(1, pNode->node.resType.bytes * 2 + 1);
      if (NULL == buf) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        break;
      }
      code = tjsonGetStringValue(pJson, jkValueDatum, buf);
      if (code != TSDB_CODE_SUCCESS) {
        taosMemoryFree(buf);
        break;
      }
      code = taosHexDecode(buf, pNode->datum.p, pNode->node.resType.bytes);
      if (code != TSDB_CODE_SUCCESS) {
        taosMemoryFree(buf);
        break;
      }
      taosMemoryFree(buf);
      break;
    }
    case TSDB_DATA_TYPE_DECIMAL:
    case TSDB_DATA_TYPE_BLOB:
      // todo
    default:
      break;
  }

  return code;
}

static int32_t jsonToValueNode(const SJson* pJson, void* pObj) {
  SValueNode* pNode = (SValueNode*)pObj;

  int32_t code = jsonToExprNode(pJson, pObj);
  int32_t literalSize = 0;
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkValueLiteralSize, &literalSize);
  }
  if (TSDB_CODE_SUCCESS == code && literalSize > 0) {
    code = tjsonDupStringValue(pJson, jkValueLiteral, &pNode->literal);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkValueDuration, &pNode->isDuration);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkValueTranslate, &pNode->translate);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkValueNotReserved, &pNode->notReserved);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkValueIsNull, &pNode->isNull);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkValueUnit, &pNode->unit);
  }
  if (TSDB_CODE_SUCCESS == code && pNode->translate && !pNode->isNull) {
    code = jsonToDatum(pJson, pNode);
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

static int32_t jsonToOperatorNode(const SJson* pJson, void* pObj) {
  SOperatorNode* pNode = (SOperatorNode*)pObj;

  int32_t code = jsonToExprNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkOperatorType, pNode->opType, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkOperatorLeft, &pNode->pLeft);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkOperatorRight, &pNode->pRight);
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
    code = nodeListToJson(pJson, jkLogicCondParameters, pNode->pParameterList);
  }

  return code;
}

static int32_t jsonToLogicConditionNode(const SJson* pJson, void* pObj) {
  SLogicConditionNode* pNode = (SLogicConditionNode*)pObj;

  int32_t code = jsonToExprNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkLogicCondType, pNode->condType, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkLogicCondParameters, &pNode->pParameterList);
  }

  return code;
}

static const char* jkFunctionName = "Name";
static const char* jkFunctionId = "Id";
static const char* jkFunctionType = "Type";
static const char* jkFunctionParameter = "Parameters";
static const char* jkFunctionUdfBufSize = "UdfBufSize";

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
    code = nodeListToJson(pJson, jkFunctionParameter, pNode->pParameterList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkFunctionUdfBufSize, pNode->udfBufSize);
  }

  return code;
}

static int32_t jsonToFunctionNode(const SJson* pJson, void* pObj) {
  SFunctionNode* pNode = (SFunctionNode*)pObj;

  int32_t code = jsonToExprNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkFunctionName, pNode->functionName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkFunctionId, &pNode->funcId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkFunctionType, &pNode->funcType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkFunctionParameter, &pNode->pParameterList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkFunctionUdfBufSize, &pNode->udfBufSize);
  }

  return code;
}

static const char* jkTableDbName = "DbName";
static const char* jkTableTableName = "tableName";
static const char* jkTableTableAlias = "tableAlias";

static int32_t tableNodeToJson(const void* pObj, SJson* pJson) {
  const STableNode* pNode = (const STableNode*)pObj;

  int32_t code = exprNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkTableDbName, pNode->dbName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkTableTableName, pNode->tableName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkTableTableAlias, pNode->tableAlias);
  }

  return code;
}

static int32_t jsonToTableNode(const SJson* pJson, void* pObj) {
  STableNode* pNode = (STableNode*)pObj;

  int32_t code = jsonToExprNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkTableDbName, pNode->dbName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkTableTableName, pNode->tableName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkTableTableAlias, pNode->tableAlias);
  }

  return code;
}

static const char* jkTableIndexInfoIntervalUnit = "IntervalUnit";
static const char* jkTableIndexInfoSlidingUnit = "SlidingUnit";
static const char* jkTableIndexInfoInterval = "Interval";
static const char* jkTableIndexInfoOffset = "Offset";
static const char* jkTableIndexInfoSliding = "Sliding";
static const char* jkTableIndexInfoDstTbUid = "DstTbUid";
static const char* jkTableIndexInfoDstVgId = "DstVgId";
static const char* jkTableIndexInfoEpSet = "EpSet";
static const char* jkTableIndexInfoExpr = "Expr";

static int32_t tableIndexInfoToJson(const void* pObj, SJson* pJson) {
  const STableIndexInfo* pNode = (const STableIndexInfo*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkTableIndexInfoIntervalUnit, pNode->intervalUnit);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableIndexInfoSlidingUnit, pNode->slidingUnit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableIndexInfoInterval, pNode->interval);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableIndexInfoOffset, pNode->offset);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableIndexInfoSliding, pNode->sliding);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableIndexInfoDstTbUid, pNode->dstTbUid);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableIndexInfoDstVgId, pNode->dstVgId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkTableIndexInfoEpSet, epSetToJson, &pNode->epSet);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkTableIndexInfoExpr, pNode->expr);
  }

  return code;
}

static int32_t jsonToTableIndexInfo(const SJson* pJson, void* pObj) {
  STableIndexInfo* pNode = (STableIndexInfo*)pObj;

  int32_t code = tjsonGetTinyIntValue(pJson, jkTableIndexInfoIntervalUnit, &pNode->intervalUnit);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkTableIndexInfoSlidingUnit, &pNode->slidingUnit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkTableIndexInfoInterval, &pNode->interval);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkTableIndexInfoOffset, &pNode->offset);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkTableIndexInfoSliding, &pNode->sliding);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkTableIndexInfoDstTbUid, &pNode->dstTbUid);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkTableIndexInfoDstVgId, &pNode->dstVgId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonToObject(pJson, jkTableIndexInfoEpSet, jsonToEpSet, &pNode->epSet);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonDupStringValue(pJson, jkTableIndexInfoExpr, &pNode->expr);
  }

  return code;
}

static const char* jkRealTableMetaSize = "MetaSize";
static const char* jkRealTableMeta = "Meta";
static const char* jkRealTableVgroupsInfoSize = "VgroupsInfoSize";
static const char* jkRealTableVgroupsInfo = "VgroupsInfo";
static const char* jkRealTableSmaIndexes = "SmaIndexes";

static int32_t realTableNodeToJson(const void* pObj, SJson* pJson) {
  const SRealTableNode* pNode = (const SRealTableNode*)pObj;

  int32_t code = tableNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkRealTableMetaSize, TABLE_META_SIZE(pNode->pMeta));
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkRealTableMeta, tableMetaToJson, pNode->pMeta);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkRealTableVgroupsInfoSize, VGROUPS_INFO_SIZE(pNode->pVgroupList));
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkRealTableVgroupsInfo, vgroupsInfoToJson, pNode->pVgroupList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddTArray(pJson, jkRealTableSmaIndexes, tableIndexInfoToJson, pNode->pSmaIndexes);
  }

  return code;
}

static int32_t jsonToRealTableNode(const SJson* pJson, void* pObj) {
  SRealTableNode* pNode = (SRealTableNode*)pObj;

  int32_t objSize = 0;
  int32_t code = jsonToTableNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkRealTableMetaSize, &objSize);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonMakeObject(pJson, jkRealTableMeta, jsonToTableMeta, (void**)&pNode->pMeta, objSize);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkRealTableVgroupsInfoSize, &objSize);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonMakeObject(pJson, jkRealTableVgroupsInfo, jsonToVgroupsInfo, (void**)&pNode->pVgroupList, objSize);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code =
        tjsonToTArray(pJson, jkRealTableSmaIndexes, jsonToTableIndexInfo, &pNode->pSmaIndexes, sizeof(STableIndexInfo));
  }

  return code;
}

static const char* jkTempTableSubquery = "Subquery";

static int32_t tempTableNodeToJson(const void* pObj, SJson* pJson) {
  const STempTableNode* pNode = (const STempTableNode*)pObj;

  int32_t code = tableNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkTempTableSubquery, nodeToJson, pNode->pSubquery);
  }

  return code;
}

static int32_t jsonToTempTableNode(const SJson* pJson, void* pObj) {
  STempTableNode* pNode = (STempTableNode*)pObj;

  int32_t code = jsonToTableNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkTempTableSubquery, &pNode->pSubquery);
  }

  return code;
}

static const char* jkJoinTableJoinType = "JoinType";
static const char* jkJoinTableLeft = "Left";
static const char* jkJoinTableRight = "Right";
static const char* jkJoinTableOnCond = "OnCond";

static int32_t joinTableNodeToJson(const void* pObj, SJson* pJson) {
  const SJoinTableNode* pNode = (const SJoinTableNode*)pObj;

  int32_t code = tableNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkJoinTableJoinType, pNode->joinType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkJoinTableLeft, nodeToJson, pNode->pLeft);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkJoinTableRight, nodeToJson, pNode->pRight);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkJoinTableOnCond, nodeToJson, pNode->pOnCond);
  }

  return code;
}

static int32_t jsonToJoinTableNode(const SJson* pJson, void* pObj) {
  SJoinTableNode* pNode = (SJoinTableNode*)pObj;

  int32_t code = jsonToTableNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkJoinTableJoinType, pNode->joinType, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkJoinTableLeft, &pNode->pLeft);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkJoinTableRight, &pNode->pRight);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkJoinTableOnCond, &pNode->pOnCond);
  }

  return code;
}

static const char* jkGroupingSetType = "GroupingSetType";
static const char* jkGroupingSetParameter = "Parameters";

static int32_t groupingSetNodeToJson(const void* pObj, SJson* pJson) {
  const SGroupingSetNode* pNode = (const SGroupingSetNode*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkGroupingSetType, pNode->groupingSetType);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkGroupingSetParameter, pNode->pParameterList);
  }

  return code;
}

static int32_t jsonToGroupingSetNode(const SJson* pJson, void* pObj) {
  SGroupingSetNode* pNode = (SGroupingSetNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  tjsonGetNumberValue(pJson, jkGroupingSetType, pNode->groupingSetType, code);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkGroupingSetParameter, &pNode->pParameterList);
  }

  return code;
}

static const char* jkOrderByExprExpr = "Expr";
static const char* jkOrderByExprOrder = "Order";
static const char* jkOrderByExprNullOrder = "NullOrder";

static int32_t orderByExprNodeToJson(const void* pObj, SJson* pJson) {
  const SOrderByExprNode* pNode = (const SOrderByExprNode*)pObj;

  int32_t code = tjsonAddObject(pJson, jkOrderByExprExpr, nodeToJson, pNode->pExpr);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkOrderByExprOrder, pNode->order);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkOrderByExprNullOrder, pNode->nullOrder);
  }

  return code;
}

static int32_t jsonToOrderByExprNode(const SJson* pJson, void* pObj) {
  SOrderByExprNode* pNode = (SOrderByExprNode*)pObj;

  int32_t code = jsonToNodeObject(pJson, jkOrderByExprExpr, &pNode->pExpr);
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkOrderByExprOrder, pNode->order, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkOrderByExprNullOrder, pNode->nullOrder, code);
  }

  return code;
}

static const char* jkLimitLimit = "Limit";
static const char* jkLimitOffset = "Offset";

static int32_t limitNodeToJson(const void* pObj, SJson* pJson) {
  const SLimitNode* pNode = (const SLimitNode*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkLimitLimit, pNode->limit);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkLimitOffset, pNode->offset);
  }

  return code;
}

static int32_t jsonToLimitNode(const SJson* pJson, void* pObj) {
  SLimitNode* pNode = (SLimitNode*)pObj;

  int32_t code = tjsonGetBigIntValue(pJson, jkLimitLimit, &pNode->limit);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkLimitOffset, &pNode->offset);
  }

  return code;
}

static const char* jkStateWindowCol = "StateWindowCol";
static const char* jkStateWindowExpr = "StateWindowExpr";

static int32_t stateWindowNodeToJson(const void* pObj, SJson* pJson) {
  const SStateWindowNode* pNode = (const SStateWindowNode*)pObj;
  int32_t                 code = tjsonAddObject(pJson, jkStateWindowCol, nodeToJson, pNode->pCol);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkStateWindowExpr, nodeToJson, pNode->pExpr);
  }
  return code;
}

static int32_t jsonToStateWindowNode(const SJson* pJson, void* pObj) {
  SStateWindowNode* pNode = (SStateWindowNode*)pObj;

  int32_t code = jsonToNodeObject(pJson, jkStateWindowCol, (SNode**)&pNode->pCol);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkStateWindowExpr, (SNode**)&pNode->pExpr);
  }
  return code;
}

static const char* jkSessionWindowTsPrimaryKey = "TsPrimaryKey";
static const char* jkSessionWindowGap = "Gap";

static int32_t sessionWindowNodeToJson(const void* pObj, SJson* pJson) {
  const SSessionWindowNode* pNode = (const SSessionWindowNode*)pObj;

  int32_t code = tjsonAddObject(pJson, jkSessionWindowTsPrimaryKey, nodeToJson, pNode->pCol);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkSessionWindowGap, nodeToJson, pNode->pGap);
  }
  return code;
}

static int32_t jsonToSessionWindowNode(const SJson* pJson, void* pObj) {
  SSessionWindowNode* pNode = (SSessionWindowNode*)pObj;

  int32_t code = jsonToNodeObject(pJson, jkSessionWindowTsPrimaryKey, (SNode**)&pNode->pCol);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkSessionWindowGap, (SNode**)&pNode->pGap);
  }
  return code;
}

static const char* jkEventWindowTsPrimaryKey = "TsPrimaryKey";
static const char* jkEventWindowStartCond = "StartCond";
static const char* jkEventWindowEndCond = "EndCond";

static int32_t eventWindowNodeToJson(const void* pObj, SJson* pJson) {
  const SEventWindowNode* pNode = (const SEventWindowNode*)pObj;

  int32_t code = tjsonAddObject(pJson, jkEventWindowTsPrimaryKey, nodeToJson, pNode->pCol);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkEventWindowStartCond, nodeToJson, pNode->pStartCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkEventWindowEndCond, nodeToJson, pNode->pEndCond);
  }
  return code;
}

static int32_t jsonToEventWindowNode(const SJson* pJson, void* pObj) {
  SEventWindowNode* pNode = (SEventWindowNode*)pObj;

  int32_t code = jsonToNodeObject(pJson, jkEventWindowTsPrimaryKey, &pNode->pCol);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkEventWindowStartCond, &pNode->pStartCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkEventWindowEndCond, &pNode->pEndCond);
  }
  return code;
}

static const char* jkIntervalWindowInterval = "Interval";
static const char* jkIntervalWindowOffset = "Offset";
static const char* jkIntervalWindowSliding = "Sliding";
static const char* jkIntervalWindowFill = "Fill";
static const char* jkIntervalWindowTsPk = "TsPk";

static int32_t intervalWindowNodeToJson(const void* pObj, SJson* pJson) {
  const SIntervalWindowNode* pNode = (const SIntervalWindowNode*)pObj;

  int32_t code = tjsonAddObject(pJson, jkIntervalWindowInterval, nodeToJson, pNode->pInterval);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkIntervalWindowOffset, nodeToJson, pNode->pOffset);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkIntervalWindowSliding, nodeToJson, pNode->pSliding);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkIntervalWindowFill, nodeToJson, pNode->pFill);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkIntervalWindowTsPk, nodeToJson, pNode->pCol);
  }

  return code;
}

static int32_t jsonToIntervalWindowNode(const SJson* pJson, void* pObj) {
  SIntervalWindowNode* pNode = (SIntervalWindowNode*)pObj;

  int32_t code = jsonToNodeObject(pJson, jkIntervalWindowInterval, &pNode->pInterval);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkIntervalWindowOffset, &pNode->pOffset);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkIntervalWindowSliding, &pNode->pSliding);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkIntervalWindowFill, &pNode->pFill);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkIntervalWindowTsPk, &pNode->pCol);
  }

  return code;
}

static const char* jkNodeListDataType = "DataType";
static const char* jkNodeListNodeList = "NodeList";

static int32_t nodeListNodeToJson(const void* pObj, SJson* pJson) {
  const SNodeListNode* pNode = (const SNodeListNode*)pObj;

  int32_t code = tjsonAddObject(pJson, jkNodeListDataType, dataTypeToJson, &pNode->node.resType);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkNodeListNodeList, pNode->pNodeList);
  }

  return code;
}

static int32_t jsonToNodeListNode(const SJson* pJson, void* pObj) {
  SNodeListNode* pNode = (SNodeListNode*)pObj;

  int32_t code = tjsonToObject(pJson, jkNodeListDataType, jsonToDataType, &pNode->node.resType);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkNodeListNodeList, &pNode->pNodeList);
  }

  return code;
}

static const char* jkFillMode = "Mode";
static const char* jkFillValues = "Values";
static const char* jkFillWStartTs = "WStartTs";
static const char* jkFillStartTime = "StartTime";
static const char* jkFillEndTime = "EndTime";

static int32_t fillNodeToJson(const void* pObj, SJson* pJson) {
  const SFillNode* pNode = (const SFillNode*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkFillMode, pNode->mode);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkFillValues, nodeToJson, pNode->pValues);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkFillWStartTs, nodeToJson, pNode->pWStartTs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkFillStartTime, pNode->timeRange.skey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkFillEndTime, pNode->timeRange.ekey);
  }

  return code;
}

static int32_t jsonToFillNode(const SJson* pJson, void* pObj) {
  SFillNode* pNode = (SFillNode*)pObj;

  int32_t code;
  tjsonGetNumberValue(pJson, jkFillMode, pNode->mode, code);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkFillValues, &pNode->pValues);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkFillWStartTs, &pNode->pWStartTs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkFillStartTime, &pNode->timeRange.skey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkFillEndTime, &pNode->timeRange.ekey);
  }

  return code;
}

static const char* jkTargetDataBlockId = "DataBlockId";
static const char* jkTargetSlotId = "SlotId";
static const char* jkTargetExpr = "Expr";

static int32_t targetNodeToJson(const void* pObj, SJson* pJson) {
  const STargetNode* pNode = (const STargetNode*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkTargetDataBlockId, pNode->dataBlockId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTargetSlotId, pNode->slotId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkTargetExpr, nodeToJson, pNode->pExpr);
  }

  return code;
}

static int32_t jsonToTargetNode(const SJson* pJson, void* pObj) {
  STargetNode* pNode = (STargetNode*)pObj;

  int32_t code = tjsonGetSmallIntValue(pJson, jkTargetDataBlockId, &pNode->dataBlockId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetSmallIntValue(pJson, jkTargetSlotId, &pNode->slotId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkTargetExpr, &pNode->pExpr);
  }

  return code;
}

static const char* jkSlotDescSlotId = "SlotId";
static const char* jkSlotDescDataType = "DataType";
static const char* jkSlotDescReserve = "Reserve";
static const char* jkSlotDescOutput = "Output";
static const char* jkSlotDescName = "Name";

static int32_t slotDescNodeToJson(const void* pObj, SJson* pJson) {
  const SSlotDescNode* pNode = (const SSlotDescNode*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkSlotDescSlotId, pNode->slotId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkSlotDescDataType, dataTypeToJson, &pNode->dataType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkSlotDescReserve, pNode->reserve);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkSlotDescOutput, pNode->output);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkSlotDescName, pNode->name);
  }

  return code;
}

static int32_t jsonToSlotDescNode(const SJson* pJson, void* pObj) {
  SSlotDescNode* pNode = (SSlotDescNode*)pObj;

  int32_t code = tjsonGetSmallIntValue(pJson, jkSlotDescSlotId, &pNode->slotId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonToObject(pJson, jkSlotDescDataType, jsonToDataType, &pNode->dataType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkSlotDescReserve, &pNode->reserve);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkSlotDescOutput, &pNode->output);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkSlotDescName, pNode->name);
  }

  return code;
}

static const char* jkColumnDefColName = "ColName";
static const char* jkColumnDefDataType = "DataType";
static const char* jkColumnDefComments = "Comments";
static const char* jkColumnDefSma = "Sma";

static int32_t columnDefNodeToJson(const void* pObj, SJson* pJson) {
  const SColumnDefNode* pNode = (const SColumnDefNode*)pObj;

  int32_t code = tjsonAddStringToObject(pJson, jkColumnDefColName, pNode->colName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkColumnDefDataType, dataTypeToJson, &pNode->dataType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkColumnDefComments, pNode->comments);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkColumnDefSma, pNode->sma);
  }

  return code;
}

static int32_t jsonToColumnDefNode(const SJson* pJson, void* pObj) {
  SColumnDefNode* pNode = (SColumnDefNode*)pObj;

  int32_t code = tjsonGetStringValue(pJson, jkColumnDefColName, pNode->colName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonToObject(pJson, jkColumnDefDataType, jsonToDataType, &pNode->dataType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkColumnDefComments, pNode->comments);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkColumnDefSma, &pNode->sma);
  }

  return code;
}

static const char* jkDownstreamSourceAddr = "Addr";
static const char* jkDownstreamSourceTaskId = "TaskId";
static const char* jkDownstreamSourceSchedId = "SchedId";
static const char* jkDownstreamSourceExecId = "ExecId";
static const char* jkDownstreamSourceFetchMsgType = "FetchMsgType";

static int32_t downstreamSourceNodeToJson(const void* pObj, SJson* pJson) {
  const SDownstreamSourceNode* pNode = (const SDownstreamSourceNode*)pObj;

  int32_t code = tjsonAddObject(pJson, jkDownstreamSourceAddr, queryNodeAddrToJson, &pNode->addr);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDownstreamSourceTaskId, pNode->taskId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDownstreamSourceSchedId, pNode->schedId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDownstreamSourceExecId, pNode->execId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDownstreamSourceFetchMsgType, pNode->fetchMsgType);
  }

  return code;
}

static int32_t jsonToDownstreamSourceNode(const SJson* pJson, void* pObj) {
  SDownstreamSourceNode* pNode = (SDownstreamSourceNode*)pObj;

  int32_t code = tjsonToObject(pJson, jkDownstreamSourceAddr, jsonToQueryNodeAddr, &pNode->addr);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetUBigIntValue(pJson, jkDownstreamSourceTaskId, &pNode->taskId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetUBigIntValue(pJson, jkDownstreamSourceSchedId, &pNode->schedId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkDownstreamSourceExecId, &pNode->execId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkDownstreamSourceFetchMsgType, &pNode->fetchMsgType);
  }

  return code;
}

static const char* jkDatabaseOptionsBuffer = "Buffer";
static const char* jkDatabaseOptionsCacheModel = "CacheModel";
static const char* jkDatabaseOptionsCompressionLevel = "CompressionLevel";
static const char* jkDatabaseOptionsDaysPerFileNode = "DaysPerFileNode";
static const char* jkDatabaseOptionsDaysPerFile = "DaysPerFile";
static const char* jkDatabaseOptionsFsyncPeriod = "FsyncPeriod";
static const char* jkDatabaseOptionsMaxRowsPerBlock = "MaxRowsPerBlock";
static const char* jkDatabaseOptionsMinRowsPerBlock = "MinRowsPerBlock";
static const char* jkDatabaseOptionsKeep = "Keep";
static const char* jkDatabaseOptionsPages = "Pages";
static const char* jkDatabaseOptionsPagesize = "Pagesize";
static const char* jkDatabaseOptionsPrecision = "Precision";
static const char* jkDatabaseOptionsReplica = "Replica";
static const char* jkDatabaseOptionsStrict = "Strict";
static const char* jkDatabaseOptionsWalLevel = "WalLevel";
static const char* jkDatabaseOptionsNumOfVgroups = "NumOfVgroups";
static const char* jkDatabaseOptionsSingleStable = "SingleStable";
static const char* jkDatabaseOptionsRetentions = "Retentions";
static const char* jkDatabaseOptionsSchemaless = "Schemaless";

static int32_t databaseOptionsToJson(const void* pObj, SJson* pJson) {
  const SDatabaseOptions* pNode = (const SDatabaseOptions*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkDatabaseOptionsBuffer, pNode->buffer);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDatabaseOptionsCacheModel, pNode->cacheModel);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDatabaseOptionsCompressionLevel, pNode->compressionLevel);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkDatabaseOptionsDaysPerFileNode, nodeToJson, pNode->pDaysPerFile);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDatabaseOptionsDaysPerFile, pNode->daysPerFile);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDatabaseOptionsFsyncPeriod, pNode->fsyncPeriod);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDatabaseOptionsMaxRowsPerBlock, pNode->maxRowsPerBlock);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDatabaseOptionsMinRowsPerBlock, pNode->minRowsPerBlock);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkDatabaseOptionsKeep, pNode->pKeep);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDatabaseOptionsPages, pNode->pages);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDatabaseOptionsPagesize, pNode->pagesize);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkDatabaseOptionsPrecision, pNode->precisionStr);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDatabaseOptionsReplica, pNode->replica);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDatabaseOptionsStrict, pNode->strict);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDatabaseOptionsWalLevel, pNode->walLevel);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDatabaseOptionsNumOfVgroups, pNode->numOfVgroups);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDatabaseOptionsSingleStable, pNode->singleStable);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkDatabaseOptionsRetentions, pNode->pRetentions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDatabaseOptionsSchemaless, pNode->schemaless);
  }

  return code;
}

static int32_t jsonToDatabaseOptions(const SJson* pJson, void* pObj) {
  SDatabaseOptions* pNode = (SDatabaseOptions*)pObj;

  int32_t code = tjsonGetIntValue(pJson, jkDatabaseOptionsBuffer, &pNode->buffer);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkDatabaseOptionsCacheModel, &pNode->cacheModel);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkDatabaseOptionsCompressionLevel, &pNode->compressionLevel);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkDatabaseOptionsDaysPerFileNode, (SNode**)&pNode->pDaysPerFile);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkDatabaseOptionsDaysPerFile, &pNode->daysPerFile);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkDatabaseOptionsFsyncPeriod, &pNode->fsyncPeriod);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkDatabaseOptionsMaxRowsPerBlock, &pNode->maxRowsPerBlock);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkDatabaseOptionsMinRowsPerBlock, &pNode->minRowsPerBlock);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkDatabaseOptionsKeep, &pNode->pKeep);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkDatabaseOptionsPages, &pNode->pages);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkDatabaseOptionsPagesize, &pNode->pagesize);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkDatabaseOptionsPrecision, pNode->precisionStr);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkDatabaseOptionsReplica, &pNode->replica);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkDatabaseOptionsStrict, &pNode->strict);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkDatabaseOptionsWalLevel, &pNode->walLevel);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkDatabaseOptionsNumOfVgroups, &pNode->numOfVgroups);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkDatabaseOptionsSingleStable, &pNode->singleStable);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkDatabaseOptionsRetentions, &pNode->pRetentions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkDatabaseOptionsSchemaless, &pNode->schemaless);
  }

  return code;
}

static const char* jkTableOptionsComment = "Comment";
static const char* jkTableOptionsMaxDelay = "MaxDelay";
static const char* jkTableOptionsWatermark = "Watermark";
static const char* jkTableOptionsDeleteMark = "DeleteMark";
static const char* jkTableOptionsRollupFuncs = "RollupFuncs";
static const char* jkTableOptionsTtl = "Ttl";
static const char* jkTableOptionsSma = "Sma";

static int32_t tableOptionsToJson(const void* pObj, SJson* pJson) {
  const STableOptions* pNode = (const STableOptions*)pObj;

  int32_t code = tjsonAddStringToObject(pJson, jkTableOptionsComment, pNode->comment);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkTableOptionsMaxDelay, pNode->pMaxDelay);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkTableOptionsWatermark, pNode->pWatermark);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkTableOptionsDeleteMark, pNode->pDeleteMark);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkTableOptionsRollupFuncs, pNode->pRollupFuncs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTableOptionsTtl, pNode->ttl);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkTableOptionsSma, pNode->pSma);
  }

  return code;
}

static int32_t jsonToTableOptions(const SJson* pJson, void* pObj) {
  STableOptions* pNode = (STableOptions*)pObj;

  int32_t code = tjsonGetStringValue(pJson, jkTableOptionsComment, pNode->comment);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkTableOptionsMaxDelay, &pNode->pMaxDelay);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkTableOptionsWatermark, &pNode->pWatermark);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkTableOptionsDeleteMark, &pNode->pDeleteMark);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkTableOptionsRollupFuncs, &pNode->pRollupFuncs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkTableOptionsTtl, &pNode->ttl);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkTableOptionsSma, &pNode->pSma);
  }

  return code;
}

static const char* jkIndexOptionsFuncs = "Funcs";
static const char* jkIndexOptionsInterval = "Interval";
static const char* jkIndexOptionsOffset = "Offset";
static const char* jkIndexOptionsSliding = "Sliding";
static const char* jkIndexOptionsStreamOptions = "StreamOptions";

static int32_t indexOptionsToJson(const void* pObj, SJson* pJson) {
  const SIndexOptions* pNode = (const SIndexOptions*)pObj;

  int32_t code = nodeListToJson(pJson, jkIndexOptionsFuncs, pNode->pFuncs);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkIndexOptionsInterval, nodeToJson, pNode->pInterval);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkIndexOptionsOffset, nodeToJson, pNode->pOffset);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkIndexOptionsSliding, nodeToJson, pNode->pSliding);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkIndexOptionsStreamOptions, nodeToJson, pNode->pStreamOptions);
  }

  return code;
}

static int32_t jsonToIndexOptions(const SJson* pJson, void* pObj) {
  SIndexOptions* pNode = (SIndexOptions*)pObj;

  int32_t code = jsonToNodeList(pJson, jkIndexOptionsFuncs, &pNode->pFuncs);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkIndexOptionsInterval, &pNode->pInterval);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkIndexOptionsOffset, &pNode->pOffset);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkIndexOptionsSliding, &pNode->pSliding);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkIndexOptionsStreamOptions, &pNode->pStreamOptions);
  }

  return code;
}

static const char* jkExplainOptionsVerbose = "Verbose";
static const char* jkExplainOptionsRatio = "Ratio";

static int32_t explainOptionsToJson(const void* pObj, SJson* pJson) {
  const SExplainOptions* pNode = (const SExplainOptions*)pObj;

  int32_t code = tjsonAddBoolToObject(pJson, jkExplainOptionsVerbose, pNode->verbose);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddDoubleToObject(pJson, jkExplainOptionsRatio, pNode->ratio);
  }

  return code;
}

static int32_t jsonToExplainOptions(const SJson* pJson, void* pObj) {
  SExplainOptions* pNode = (SExplainOptions*)pObj;

  int32_t code = tjsonGetBoolValue(pJson, jkExplainOptionsVerbose, &pNode->verbose);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetDoubleValue(pJson, jkExplainOptionsRatio, &pNode->ratio);
  }

  return code;
}

static const char* jkStreamOptionsTriggerType = "TriggerType";
static const char* jkStreamOptionsDelay = "Delay";
static const char* jkStreamOptionsWatermark = "Watermark";
static const char* jkStreamOptionsDeleteMark = "DeleteMark";
static const char* jkStreamOptionsFillHistory = "FillHistory";
static const char* jkStreamOptionsIgnoreExpired = "IgnoreExpired";

static int32_t streamOptionsToJson(const void* pObj, SJson* pJson) {
  const SStreamOptions* pNode = (const SStreamOptions*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkStreamOptionsTriggerType, pNode->triggerType);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkStreamOptionsDelay, nodeToJson, pNode->pDelay);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkStreamOptionsWatermark, nodeToJson, pNode->pWatermark);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkStreamOptionsDeleteMark, nodeToJson, pNode->pDeleteMark);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkStreamOptionsFillHistory, pNode->fillHistory);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkStreamOptionsIgnoreExpired, pNode->ignoreExpired);
  }

  return code;
}

static int32_t jsonToStreamOptions(const SJson* pJson, void* pObj) {
  SStreamOptions* pNode = (SStreamOptions*)pObj;

  int32_t code = tjsonGetTinyIntValue(pJson, jkStreamOptionsTriggerType, &pNode->triggerType);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkStreamOptionsDelay, &pNode->pDelay);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkStreamOptionsWatermark, &pNode->pWatermark);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkStreamOptionsDeleteMark, &pNode->pDeleteMark);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkStreamOptionsFillHistory, &pNode->fillHistory);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkStreamOptionsIgnoreExpired, &pNode->ignoreExpired);
  }

  return code;
}

static const char* jkWhenThenWhen = "When";
static const char* jkWhenThenThen = "Then";

static int32_t whenThenNodeToJson(const void* pObj, SJson* pJson) {
  const SWhenThenNode* pNode = (const SWhenThenNode*)pObj;

  int32_t code = exprNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkWhenThenWhen, nodeToJson, pNode->pWhen);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkWhenThenThen, nodeToJson, pNode->pThen);
  }

  return code;
}

static int32_t jsonToWhenThenNode(const SJson* pJson, void* pObj) {
  SWhenThenNode* pNode = (SWhenThenNode*)pObj;

  int32_t code = jsonToExprNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkWhenThenWhen, &pNode->pWhen);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkWhenThenThen, &pNode->pThen);
  }

  return code;
}

static const char* jkCaseWhenCase = "Case";
static const char* jkCaseWhenWhenThenList = "WhenThenList";
static const char* jkCaseWhenElse = "Else";

static int32_t caseWhenNodeToJson(const void* pObj, SJson* pJson) {
  const SCaseWhenNode* pNode = (const SCaseWhenNode*)pObj;

  int32_t code = exprNodeToJson(pObj, pJson);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkCaseWhenCase, nodeToJson, pNode->pCase);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkCaseWhenWhenThenList, pNode->pWhenThenList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkCaseWhenElse, nodeToJson, pNode->pElse);
  }

  return code;
}

static int32_t jsonToCaseWhenNode(const SJson* pJson, void* pObj) {
  SCaseWhenNode* pNode = (SCaseWhenNode*)pObj;

  int32_t code = jsonToExprNode(pJson, pObj);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkCaseWhenCase, &pNode->pCase);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkCaseWhenWhenThenList, &pNode->pWhenThenList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkCaseWhenElse, &pNode->pElse);
  }

  return code;
}

static const char* jkDataBlockDescDataBlockId = "DataBlockId";
static const char* jkDataBlockDescSlots = "Slots";
static const char* jkDataBlockTotalRowSize = "TotalRowSize";
static const char* jkDataBlockOutputRowSize = "OutputRowSize";
static const char* jkDataBlockPrecision = "Precision";

static int32_t dataBlockDescNodeToJson(const void* pObj, SJson* pJson) {
  const SDataBlockDescNode* pNode = (const SDataBlockDescNode*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkDataBlockDescDataBlockId, pNode->dataBlockId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDataBlockTotalRowSize, pNode->totalRowSize);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDataBlockOutputRowSize, pNode->outputRowSize);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkDataBlockDescSlots, pNode->pSlots);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDataBlockPrecision, pNode->precision);
  }

  return code;
}

static int32_t jsonToDataBlockDescNode(const SJson* pJson, void* pObj) {
  SDataBlockDescNode* pNode = (SDataBlockDescNode*)pObj;

  int32_t code = tjsonGetSmallIntValue(pJson, jkDataBlockDescDataBlockId, &pNode->dataBlockId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkDataBlockTotalRowSize, &pNode->totalRowSize);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkDataBlockOutputRowSize, &pNode->outputRowSize);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkDataBlockDescSlots, &pNode->pSlots);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetUTinyIntValue(pJson, jkDataBlockPrecision, &pNode->precision);
  }

  return code;
}

static const char* jkSetOperatorOpType = "OpType";
static const char* jkSetOperatorProjections = "Projections";
static const char* jkSetOperatorLeft = "Left";
static const char* jkSetOperatorRight = "Right";
static const char* jkSetOperatorOrderByList = "OrderByList";
static const char* jkSetOperatorLimit = "Limit";

static int32_t setOperatorToJson(const void* pObj, SJson* pJson) {
  const SSetOperator* pNode = (const SSetOperator*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkSetOperatorOpType, pNode->opType);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkSetOperatorProjections, pNode->pProjectionList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkSetOperatorLeft, nodeToJson, pNode->pLeft);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkSetOperatorRight, nodeToJson, pNode->pRight);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkSetOperatorOrderByList, pNode->pOrderByList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkSetOperatorLimit, nodeToJson, pNode->pLimit);
  }

  return code;
}

static int32_t jsonToSetOperator(const SJson* pJson, void* pObj) {
  SSetOperator* pNode = (SSetOperator*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  tjsonGetNumberValue(pJson, jkSetOperatorOpType, pNode->opType, code);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkSetOperatorProjections, &pNode->pProjectionList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkSetOperatorLeft, &pNode->pLeft);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkSetOperatorRight, &pNode->pRight);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkSetOperatorOrderByList, &pNode->pOrderByList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkSetOperatorLimit, &pNode->pLimit);
  }

  return code;
}

static const char* jkSelectStmtDistinct = "Distinct";
static const char* jkSelectStmtProjections = "Projections";
static const char* jkSelectStmtFrom = "From";
static const char* jkSelectStmtWhere = "Where";
static const char* jkSelectStmtPartitionBy = "PartitionBy";
static const char* jkSelectStmtTags = "Tags";
static const char* jkSelectStmtSubtable = "Subtable";
static const char* jkSelectStmtWindow = "Window";
static const char* jkSelectStmtGroupBy = "GroupBy";
static const char* jkSelectStmtHaving = "Having";
static const char* jkSelectStmtOrderBy = "OrderBy";
static const char* jkSelectStmtLimit = "Limit";
static const char* jkSelectStmtSlimit = "Slimit";
static const char* jkSelectStmtStmtName = "StmtName";
static const char* jkSelectStmtHasAggFuncs = "HasAggFuncs";

static int32_t selectStmtToJson(const void* pObj, SJson* pJson) {
  const SSelectStmt* pNode = (const SSelectStmt*)pObj;

  int32_t code = tjsonAddBoolToObject(pJson, jkSelectStmtDistinct, pNode->isDistinct);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkSelectStmtProjections, pNode->pProjectionList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkSelectStmtFrom, nodeToJson, pNode->pFromTable);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkSelectStmtWhere, nodeToJson, pNode->pWhere);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkSelectStmtPartitionBy, pNode->pPartitionByList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkSelectStmtTags, pNode->pTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkSelectStmtSubtable, nodeToJson, pNode->pSubtable);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkSelectStmtWindow, nodeToJson, pNode->pWindow);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkSelectStmtGroupBy, pNode->pGroupByList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkSelectStmtHaving, nodeToJson, pNode->pHaving);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkSelectStmtOrderBy, pNode->pOrderByList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkSelectStmtLimit, nodeToJson, pNode->pLimit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkSelectStmtSlimit, nodeToJson, pNode->pSlimit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkSelectStmtStmtName, pNode->stmtName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkSelectStmtHasAggFuncs, pNode->hasAggFuncs);
  }

  return code;
}

static int32_t jsonToSelectStmt(const SJson* pJson, void* pObj) {
  SSelectStmt* pNode = (SSelectStmt*)pObj;

  int32_t code = tjsonGetBoolValue(pJson, jkSelectStmtDistinct, &pNode->isDistinct);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkSelectStmtProjections, &pNode->pProjectionList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkSelectStmtFrom, &pNode->pFromTable);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkSelectStmtWhere, &pNode->pWhere);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkSelectStmtPartitionBy, &pNode->pPartitionByList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkSelectStmtTags, &pNode->pTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkSelectStmtSubtable, &pNode->pSubtable);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkSelectStmtWindow, &pNode->pWindow);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkSelectStmtGroupBy, &pNode->pGroupByList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkSelectStmtHaving, &pNode->pHaving);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkSelectStmtOrderBy, &pNode->pOrderByList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkSelectStmtLimit, (SNode**)&pNode->pLimit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkSelectStmtSlimit, (SNode**)&pNode->pSlimit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkSelectStmtStmtName, pNode->stmtName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkSelectStmtHasAggFuncs, &pNode->hasAggFuncs);
  }

  return code;
}

static const char* jkVnodeModifyOpStmtSqlNodeType = "SqlNodeType";
static const char* jkVnodeModifyOpStmtTotalRowsNum = "TotalRowsNum";
static const char* jkVnodeModifyOpStmtTotalTbNum = "TotalTbNum";

static int32_t vnodeModifyStmtToJson(const void* pObj, SJson* pJson) {
  const SVnodeModifyOpStmt* pNode = (const SVnodeModifyOpStmt*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkVnodeModifyOpStmtSqlNodeType, pNode->sqlNodeType);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkVnodeModifyOpStmtTotalRowsNum, pNode->totalRowsNum);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkVnodeModifyOpStmtTotalTbNum, pNode->totalTbNum);
  }

  return code;
}

static int32_t jsonToVnodeModifyStmt(const SJson* pJson, void* pObj) {
  SVnodeModifyOpStmt* pNode = (SVnodeModifyOpStmt*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  tjsonGetNumberValue(pJson, jkVnodeModifyOpStmtSqlNodeType, pNode->sqlNodeType, code);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkVnodeModifyOpStmtTotalRowsNum, &pNode->totalRowsNum);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkVnodeModifyOpStmtTotalTbNum, &pNode->totalTbNum);
  }

  return code;
}

static const char* jkCreateDatabaseStmtDbName = "DbName";
static const char* jkCreateDatabaseStmtIgnoreExists = "IgnoreExists";
static const char* jkCreateDatabaseStmtOptions = "Options";

static int32_t createDatabaseStmtToJson(const void* pObj, SJson* pJson) {
  const SCreateDatabaseStmt* pNode = (const SCreateDatabaseStmt*)pObj;

  int32_t code = tjsonAddStringToObject(pJson, jkCreateDatabaseStmtDbName, pNode->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkCreateDatabaseStmtIgnoreExists, pNode->ignoreExists);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkCreateDatabaseStmtOptions, nodeToJson, pNode->pOptions);
  }

  return code;
}

static int32_t jsonToCreateDatabaseStmt(const SJson* pJson, void* pObj) {
  SCreateDatabaseStmt* pNode = (SCreateDatabaseStmt*)pObj;

  int32_t code = tjsonGetStringValue(pJson, jkCreateDatabaseStmtDbName, pNode->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkCreateDatabaseStmtIgnoreExists, &pNode->ignoreExists);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkCreateDatabaseStmtOptions, (SNode**)&pNode->pOptions);
  }

  return code;
}

static const char* jkAlterDatabaseStmtDbName = "DbName";
static const char* jkAlterDatabaseStmtOptions = "Options";

static int32_t alterDatabaseStmtToJson(const void* pObj, SJson* pJson) {
  const SAlterDatabaseStmt* pNode = (const SAlterDatabaseStmt*)pObj;

  int32_t code = tjsonAddStringToObject(pJson, jkAlterDatabaseStmtDbName, pNode->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkAlterDatabaseStmtOptions, nodeToJson, pNode->pOptions);
  }

  return code;
}

static int32_t jsonToAlterDatabaseStmt(const SJson* pJson, void* pObj) {
  SAlterDatabaseStmt* pNode = (SAlterDatabaseStmt*)pObj;

  int32_t code = tjsonGetStringValue(pJson, jkAlterDatabaseStmtDbName, pNode->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkAlterDatabaseStmtOptions, (SNode**)&pNode->pOptions);
  }

  return code;
}

static const char* jkTrimDatabaseStmtDbName = "DbName";
static const char* jkTrimDatabaseStmtMaxSpeed = "MaxSpeed";

static int32_t trimDatabaseStmtToJson(const void* pObj, SJson* pJson) {
  const STrimDatabaseStmt* pNode = (const STrimDatabaseStmt*)pObj;

  int32_t code = tjsonAddStringToObject(pJson, jkTrimDatabaseStmtDbName, pNode->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkTrimDatabaseStmtMaxSpeed, pNode->maxSpeed);
  }

  return code;
}

static int32_t jsonToTrimDatabaseStmt(const SJson* pJson, void* pObj) {
  STrimDatabaseStmt* pNode = (STrimDatabaseStmt*)pObj;

  int32_t code = tjsonGetStringValue(pJson, jkTrimDatabaseStmtDbName, pNode->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkTrimDatabaseStmtMaxSpeed, &pNode->maxSpeed);
  }

  return code;
}

static const char* jkCreateTableStmtDbName = "DbName";
static const char* jkCreateTableStmtTableName = "TableName";
static const char* jkCreateTableStmtIgnoreExists = "IgnoreExists";
static const char* jkCreateTableStmtCols = "Cols";
static const char* jkCreateTableStmtTags = "Tags";
static const char* jkCreateTableStmtOptions = "Options";

static int32_t createTableStmtToJson(const void* pObj, SJson* pJson) {
  const SCreateTableStmt* pNode = (const SCreateTableStmt*)pObj;

  int32_t code = tjsonAddStringToObject(pJson, jkCreateTableStmtDbName, pNode->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkCreateTableStmtTableName, pNode->tableName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkCreateTableStmtIgnoreExists, pNode->ignoreExists);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkCreateTableStmtCols, pNode->pCols);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkCreateTableStmtTags, pNode->pTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkCreateTableStmtOptions, nodeToJson, pNode->pOptions);
  }

  return code;
}

static int32_t jsonToCreateTableStmt(const SJson* pJson, void* pObj) {
  SCreateTableStmt* pNode = (SCreateTableStmt*)pObj;

  int32_t code = tjsonGetStringValue(pJson, jkCreateTableStmtDbName, pNode->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkCreateTableStmtTableName, pNode->tableName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkCreateTableStmtIgnoreExists, &pNode->ignoreExists);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkCreateTableStmtCols, &pNode->pCols);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkCreateTableStmtTags, &pNode->pTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkCreateTableStmtOptions, (SNode**)&pNode->pOptions);
  }

  return code;
}

static const char* jkCreateSubTableClauseDbName = "DbName";
static const char* jkCreateSubTableClauseTableName = "TableName";
static const char* jkCreateSubTableClauseUseDbName = "UseDbName";
static const char* jkCreateSubTableClauseUseTableName = "UseTableName";
static const char* jkCreateSubTableClauseIgnoreExists = "IgnoreExists";
static const char* jkCreateSubTableClauseSpecificTags = "SpecificTags";
static const char* jkCreateSubTableClauseValsOfTags = "ValsOfTags";
static const char* jkCreateSubTableClauseOptions = "Options";

static int32_t createSubTableClauseToJson(const void* pObj, SJson* pJson) {
  const SCreateSubTableClause* pNode = (const SCreateSubTableClause*)pObj;

  int32_t code = tjsonAddStringToObject(pJson, jkCreateSubTableClauseDbName, pNode->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkCreateSubTableClauseTableName, pNode->tableName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkCreateSubTableClauseUseDbName, pNode->useDbName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkCreateSubTableClauseUseTableName, pNode->useTableName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkCreateSubTableClauseIgnoreExists, pNode->ignoreExists);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkCreateSubTableClauseSpecificTags, pNode->pSpecificTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkCreateSubTableClauseValsOfTags, pNode->pValsOfTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkCreateSubTableClauseOptions, nodeToJson, pNode->pOptions);
  }

  return code;
}

static int32_t jsonToCreateSubTableClause(const SJson* pJson, void* pObj) {
  SCreateSubTableClause* pNode = (SCreateSubTableClause*)pObj;

  int32_t code = tjsonGetStringValue(pJson, jkCreateSubTableClauseDbName, pNode->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkCreateSubTableClauseTableName, pNode->tableName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkCreateSubTableClauseUseDbName, pNode->useDbName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkCreateSubTableClauseUseTableName, pNode->useTableName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkCreateSubTableClauseIgnoreExists, &pNode->ignoreExists);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkCreateSubTableClauseSpecificTags, &pNode->pSpecificTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkCreateSubTableClauseValsOfTags, &pNode->pValsOfTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkCreateSubTableClauseOptions, (SNode**)&pNode->pOptions);
  }

  return code;
}

static const char* jkCreateMultiTablesStmtSubTables = "SubTables";

static int32_t createMultiTablesStmtToJson(const void* pObj, SJson* pJson) {
  const SCreateMultiTablesStmt* pNode = (const SCreateMultiTablesStmt*)pObj;
  return nodeListToJson(pJson, jkCreateMultiTablesStmtSubTables, pNode->pSubTables);
}

static int32_t jsonToCreateMultiTablesStmt(const SJson* pJson, void* pObj) {
  SCreateMultiTablesStmt* pNode = (SCreateMultiTablesStmt*)pObj;
  return jsonToNodeList(pJson, jkCreateMultiTablesStmtSubTables, &pNode->pSubTables);
}

static const char* jkDropTableClauseDbName = "DbName";
static const char* jkDropTableClauseTableName = "TableName";
static const char* jkDropTableClauseIgnoreNotExists = "IgnoreNotExists";

static int32_t dropTableClauseToJson(const void* pObj, SJson* pJson) {
  const SDropTableClause* pNode = (const SDropTableClause*)pObj;

  int32_t code = tjsonAddStringToObject(pJson, jkDropTableClauseDbName, pNode->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkDropTableClauseTableName, pNode->tableName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkDropTableClauseIgnoreNotExists, pNode->ignoreNotExists);
  }

  return code;
}

static int32_t jsonToDropTableClause(const SJson* pJson, void* pObj) {
  SDropTableClause* pNode = (SDropTableClause*)pObj;

  int32_t code = tjsonGetStringValue(pJson, jkDropTableClauseDbName, pNode->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkDropTableClauseTableName, pNode->tableName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkDropTableClauseIgnoreNotExists, &pNode->ignoreNotExists);
  }

  return code;
}

static const char* jkDropTableStmtTables = "Tables";

static int32_t dropTableStmtToJson(const void* pObj, SJson* pJson) {
  const SDropTableStmt* pNode = (const SDropTableStmt*)pObj;
  return nodeListToJson(pJson, jkDropTableStmtTables, pNode->pTables);
}

static int32_t jsonToDropTableStmt(const SJson* pJson, void* pObj) {
  SDropTableStmt* pNode = (SDropTableStmt*)pObj;
  return jsonToNodeList(pJson, jkDropTableStmtTables, &pNode->pTables);
}

static const char* jkDropSuperTableStmtDbName = "DbName";
static const char* jkDropSuperTableStmtTableName = "TableName";
static const char* jkDropSuperTableStmtIgnoreNotExists = "IgnoreNotExists";

static int32_t dropStableStmtToJson(const void* pObj, SJson* pJson) {
  const SDropSuperTableStmt* pNode = (const SDropSuperTableStmt*)pObj;

  int32_t code = tjsonAddStringToObject(pJson, jkDropSuperTableStmtDbName, pNode->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkDropSuperTableStmtTableName, pNode->tableName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkDropSuperTableStmtIgnoreNotExists, pNode->ignoreNotExists);
  }

  return code;
}

static int32_t jsonToDropStableStmt(const SJson* pJson, void* pObj) {
  SDropSuperTableStmt* pNode = (SDropSuperTableStmt*)pObj;

  int32_t code = tjsonGetStringValue(pJson, jkDropSuperTableStmtDbName, pNode->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkDropSuperTableStmtTableName, pNode->tableName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkDropSuperTableStmtIgnoreNotExists, &pNode->ignoreNotExists);
  }

  return code;
}

static const char* jkAlterTableStmtDbName = "DbName";
static const char* jkAlterTableStmtTableName = "TableName";
static const char* jkAlterTableStmtAlterType = "AlterType";
static const char* jkAlterTableStmtColName = "ColName";
static const char* jkAlterTableStmtNewColName = "NewColName";
static const char* jkAlterTableStmtOptions = "Options";
static const char* jkAlterTableStmtNewDataType = "NewDataType";
static const char* jkAlterTableStmtNewTagVal = "NewTagVal";

static int32_t alterTableStmtToJson(const void* pObj, SJson* pJson) {
  const SAlterTableStmt* pNode = (const SAlterTableStmt*)pObj;

  int32_t code = tjsonAddStringToObject(pJson, jkAlterTableStmtDbName, pNode->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkAlterTableStmtTableName, pNode->tableName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkAlterTableStmtAlterType, pNode->alterType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkAlterTableStmtColName, pNode->colName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkAlterTableStmtNewColName, pNode->newColName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkAlterTableStmtOptions, nodeToJson, pNode->pOptions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkAlterTableStmtNewDataType, dataTypeToJson, &pNode->dataType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkAlterTableStmtOptions, nodeToJson, pNode->pVal);
  }

  return code;
}

static int32_t jsonToAlterTableStmt(const SJson* pJson, void* pObj) {
  SAlterTableStmt* pNode = (SAlterTableStmt*)pObj;

  int32_t code = tjsonGetStringValue(pJson, jkAlterTableStmtDbName, pNode->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkAlterTableStmtTableName, pNode->tableName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkAlterTableStmtAlterType, &pNode->alterType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkAlterTableStmtColName, pNode->colName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkAlterTableStmtNewColName, pNode->newColName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkAlterTableStmtOptions, (SNode**)&pNode->pOptions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonToObject(pJson, jkAlterTableStmtNewDataType, jsonToDataType, &pNode->dataType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkAlterTableStmtOptions, (SNode**)&pNode->pVal);
  }

  return code;
}

static int32_t alterStableStmtToJson(const void* pObj, SJson* pJson) { return alterTableStmtToJson(pObj, pJson); }

static int32_t jsonToAlterStableStmt(const SJson* pJson, void* pObj) { return jsonToAlterTableStmt(pJson, pObj); }

static const char* jkCreateUserStmtUserName = "UserName";
static const char* jkCreateUserStmtPassword = "Password";
static const char* jkCreateUserStmtSysinfo = "Sysinfo";

static int32_t createUserStmtToJson(const void* pObj, SJson* pJson) {
  const SCreateUserStmt* pNode = (const SCreateUserStmt*)pObj;

  int32_t code = tjsonAddStringToObject(pJson, jkCreateUserStmtUserName, pNode->userName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkCreateUserStmtPassword, pNode->password);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkCreateUserStmtSysinfo, pNode->sysinfo);
  }

  return code;
}

static int32_t jsonToCreateUserStmt(const SJson* pJson, void* pObj) {
  SCreateUserStmt* pNode = (SCreateUserStmt*)pObj;

  int32_t code = tjsonGetStringValue(pJson, jkCreateUserStmtUserName, pNode->userName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkCreateUserStmtPassword, pNode->password);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkCreateUserStmtSysinfo, &pNode->sysinfo);
  }

  return code;
}

static const char* jkAlterUserStmtUserName = "UserName";
static const char* jkAlterUserStmtAlterType = "AlterType";
static const char* jkAlterUserStmtPassword = "Password";
static const char* jkAlterUserStmtEnable = "Enable";
static const char* jkAlterUserStmtSysinfo = "Sysinfo";

static int32_t alterUserStmtToJson(const void* pObj, SJson* pJson) {
  const SAlterUserStmt* pNode = (const SAlterUserStmt*)pObj;

  int32_t code = tjsonAddStringToObject(pJson, jkAlterUserStmtUserName, pNode->userName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkAlterUserStmtAlterType, pNode->alterType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkAlterUserStmtPassword, pNode->password);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkAlterUserStmtEnable, pNode->enable);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkAlterUserStmtSysinfo, pNode->sysinfo);
  }

  return code;
}

static int32_t jsonToAlterUserStmt(const SJson* pJson, void* pObj) {
  SAlterUserStmt* pNode = (SAlterUserStmt*)pObj;

  int32_t code = tjsonGetStringValue(pJson, jkAlterUserStmtUserName, pNode->userName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkAlterUserStmtAlterType, &pNode->alterType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkAlterUserStmtPassword, pNode->password);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkAlterUserStmtEnable, &pNode->enable);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetTinyIntValue(pJson, jkAlterUserStmtSysinfo, &pNode->sysinfo);
  }

  return code;
}

static const char* jkDropUserStmtUserName = "UserName";

static int32_t dropUserStmtToJson(const void* pObj, SJson* pJson) {
  const SDropUserStmt* pNode = (const SDropUserStmt*)pObj;
  return tjsonAddStringToObject(pJson, jkDropUserStmtUserName, pNode->userName);
}

static int32_t jsonToDropUserStmt(const SJson* pJson, void* pObj) {
  SDropUserStmt* pNode = (SDropUserStmt*)pObj;
  return tjsonGetStringValue(pJson, jkDropUserStmtUserName, pNode->userName);
}

static const char* jkUseDatabaseStmtDbName = "DbName";

static int32_t useDatabaseStmtToJson(const void* pObj, SJson* pJson) {
  const SUseDatabaseStmt* pNode = (const SUseDatabaseStmt*)pObj;
  return tjsonAddStringToObject(pJson, jkUseDatabaseStmtDbName, pNode->dbName);
}

static int32_t jsonToUseDatabaseStmt(const SJson* pJson, void* pObj) {
  SUseDatabaseStmt* pNode = (SUseDatabaseStmt*)pObj;
  return tjsonGetStringValue(pJson, jkUseDatabaseStmtDbName, pNode->dbName);
}

static const char* jkCreateDnodeStmtFqdn = "Fqdn";
static const char* jkCreateDnodeStmtPort = "Port";

static int32_t createDnodeStmtToJson(const void* pObj, SJson* pJson) {
  const SCreateDnodeStmt* pNode = (const SCreateDnodeStmt*)pObj;

  int32_t code = tjsonAddStringToObject(pJson, jkCreateDnodeStmtFqdn, pNode->fqdn);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkCreateDnodeStmtPort, pNode->port);
  }

  return code;
}

static int32_t jsonToCreateDnodeStmt(const SJson* pJson, void* pObj) {
  SCreateDnodeStmt* pNode = (SCreateDnodeStmt*)pObj;

  int32_t code = tjsonGetStringValue(pJson, jkCreateDnodeStmtFqdn, pNode->fqdn);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkCreateDnodeStmtPort, &pNode->port);
  }

  return code;
}

static const char* jkAlterDnodeStmtDnodeId = "DnodeId";
static const char* jkAlterDnodeStmtConfig = "Config";
static const char* jkAlterDnodeStmtValue = "Value";

static int32_t alterDnodeStmtToJson(const void* pObj, SJson* pJson) {
  const SAlterDnodeStmt* pNode = (const SAlterDnodeStmt*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkAlterDnodeStmtDnodeId, pNode->dnodeId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkAlterDnodeStmtConfig, pNode->config);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkAlterDnodeStmtValue, pNode->value);
  }

  return code;
}

static int32_t jsonToAlterDnodeStmt(const SJson* pJson, void* pObj) {
  SAlterDnodeStmt* pNode = (SAlterDnodeStmt*)pObj;

  int32_t code = tjsonGetIntValue(pJson, jkAlterDnodeStmtDnodeId, &pNode->dnodeId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkAlterDnodeStmtConfig, pNode->config);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkAlterDnodeStmtValue, pNode->value);
  }

  return code;
}

static const char* jkCreateIndexStmtIndexType = "IndexType";
static const char* jkCreateIndexStmtIgnoreExists = "IgnoreExists";
static const char* jkCreateIndexStmtIndexDbName = "IndexDbName";
static const char* jkCreateIndexStmtIndexName = "indexName";
static const char* jkCreateIndexStmtDbName = "DbName";
static const char* jkCreateIndexStmtTableName = "TableName";
static const char* jkCreateIndexStmtCols = "Cols";
static const char* jkCreateIndexStmtOptions = "Options";

static int32_t createIndexStmtToJson(const void* pObj, SJson* pJson) {
  const SCreateIndexStmt* pNode = (const SCreateIndexStmt*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkCreateIndexStmtIndexType, pNode->indexType);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkCreateIndexStmtIgnoreExists, pNode->ignoreExists);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkCreateIndexStmtIndexDbName, pNode->indexDbName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkCreateIndexStmtIndexName, pNode->indexName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkCreateIndexStmtDbName, pNode->dbName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkCreateIndexStmtTableName, pNode->tableName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkCreateIndexStmtCols, pNode->pCols);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkCreateIndexStmtOptions, nodeToJson, pNode->pOptions);
  }

  return code;
}

static int32_t jsonToCreateIndexStmt(const SJson* pJson, void* pObj) {
  SCreateIndexStmt* pNode = (SCreateIndexStmt*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  tjsonGetNumberValue(pJson, jkCreateIndexStmtIndexType, pNode->indexType, code);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkCreateIndexStmtIgnoreExists, &pNode->ignoreExists);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkCreateIndexStmtIndexDbName, pNode->indexDbName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkCreateIndexStmtIndexName, pNode->indexName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkCreateIndexStmtDbName, pNode->dbName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkCreateIndexStmtTableName, pNode->tableName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkCreateIndexStmtCols, &pNode->pCols);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkCreateIndexStmtOptions, (SNode**)&pNode->pOptions);
  }

  return code;
}

static const char* jkDropIndexStmtIgnoreNotExists = "IgnoreNotExists";
static const char* jkDropIndexStmtIndexDbName = "IndexDbName";
static const char* jkDropIndexStmtIndexName = "IndexName";

static int32_t dropIndexStmtToJson(const void* pObj, SJson* pJson) {
  const SDropIndexStmt* pNode = (const SDropIndexStmt*)pObj;

  int32_t code = tjsonAddBoolToObject(pJson, jkDropIndexStmtIgnoreNotExists, pNode->ignoreNotExists);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkDropIndexStmtIndexDbName, pNode->indexDbName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkDropIndexStmtIndexName, pNode->indexName);
  }

  return code;
}

static int32_t jsonToDropIndexStmt(const SJson* pJson, void* pObj) {
  SDropIndexStmt* pNode = (SDropIndexStmt*)pObj;

  int32_t code = tjsonGetBoolValue(pJson, jkDropIndexStmtIgnoreNotExists, &pNode->ignoreNotExists);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkDropIndexStmtIndexDbName, pNode->indexDbName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkDropIndexStmtIndexName, pNode->indexName);
  }

  return code;
}

static const char* jkCreateComponentNodeStmtDnodeId = "DnodeId";

static int32_t createComponentNodeStmtToJson(const void* pObj, SJson* pJson) {
  const SCreateComponentNodeStmt* pNode = (const SCreateComponentNodeStmt*)pObj;
  return tjsonAddIntegerToObject(pJson, jkCreateComponentNodeStmtDnodeId, pNode->dnodeId);
}

static int32_t jsonToCreateComponentNodeStmt(const SJson* pJson, void* pObj) {
  SCreateComponentNodeStmt* pNode = (SCreateComponentNodeStmt*)pObj;
  return tjsonGetIntValue(pJson, jkCreateComponentNodeStmtDnodeId, &pNode->dnodeId);
}

static const char* jkDropComponentNodeStmtDnodeId = "DnodeId";

static int32_t dropComponentNodeStmtToJson(const void* pObj, SJson* pJson) {
  const SDropComponentNodeStmt* pNode = (const SDropComponentNodeStmt*)pObj;
  return tjsonAddIntegerToObject(pJson, jkDropComponentNodeStmtDnodeId, pNode->dnodeId);
}

static int32_t jsonToDropComponentNodeStmt(const SJson* pJson, void* pObj) {
  SDropComponentNodeStmt* pNode = (SDropComponentNodeStmt*)pObj;
  return tjsonGetIntValue(pJson, jkDropComponentNodeStmtDnodeId, &pNode->dnodeId);
}

static int32_t createQnodeStmtToJson(const void* pObj, SJson* pJson) {
  return createComponentNodeStmtToJson(pObj, pJson);
}

static int32_t jsonToCreateQnodeStmt(const SJson* pJson, void* pObj) {
  return jsonToCreateComponentNodeStmt(pJson, pObj);
}

static int32_t dropQnodeStmtToJson(const void* pObj, SJson* pJson) { return dropComponentNodeStmtToJson(pObj, pJson); }

static int32_t jsonToDropQnodeStmt(const SJson* pJson, void* pObj) { return jsonToDropComponentNodeStmt(pJson, pObj); }

static int32_t createSnodeStmtToJson(const void* pObj, SJson* pJson) {
  return createComponentNodeStmtToJson(pObj, pJson);
}

static int32_t jsonToCreateSnodeStmt(const SJson* pJson, void* pObj) {
  return jsonToCreateComponentNodeStmt(pJson, pObj);
}

static int32_t dropSnodeStmtToJson(const void* pObj, SJson* pJson) { return dropComponentNodeStmtToJson(pObj, pJson); }

static int32_t jsonToDropSnodeStmt(const SJson* pJson, void* pObj) { return jsonToDropComponentNodeStmt(pJson, pObj); }

static int32_t createMnodeStmtToJson(const void* pObj, SJson* pJson) {
  return createComponentNodeStmtToJson(pObj, pJson);
}

static int32_t jsonToCreateMnodeStmt(const SJson* pJson, void* pObj) {
  return jsonToCreateComponentNodeStmt(pJson, pObj);
}

static int32_t dropMnodeStmtToJson(const void* pObj, SJson* pJson) { return dropComponentNodeStmtToJson(pObj, pJson); }

static int32_t jsonToDropMnodeStmt(const SJson* pJson, void* pObj) { return jsonToDropComponentNodeStmt(pJson, pObj); }

static const char* jkDropDnodeStmtDnodeId = "DnodeId";
static const char* jkDropDnodeStmtFqdn = "Fqdn";
static const char* jkDropDnodeStmtPort = "Port";
static const char* jkDropDnodeStmtForce = "Force";
static const char* jkDropDnodeStmtUnsafe = "Unsafe";

static int32_t dropDnodeStmtToJson(const void* pObj, SJson* pJson) {
  const SDropDnodeStmt* pNode = (const SDropDnodeStmt*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkDropDnodeStmtDnodeId, pNode->dnodeId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkDropDnodeStmtFqdn, pNode->fqdn);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDropDnodeStmtPort, pNode->port);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkDropDnodeStmtForce, pNode->force);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkDropDnodeStmtUnsafe, pNode->unsafe);
  }

  return code;
}

static int32_t jsonToDropDnodeStmt(const SJson* pJson, void* pObj) {
  SDropDnodeStmt* pNode = (SDropDnodeStmt*)pObj;

  int32_t code = tjsonGetIntValue(pJson, jkDropDnodeStmtDnodeId, &pNode->dnodeId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkDropDnodeStmtFqdn, pNode->fqdn);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkDropDnodeStmtPort, &pNode->port);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkDropDnodeStmtForce, &pNode->force);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkDropDnodeStmtUnsafe, &pNode->unsafe);
  }

  return code;
}

static const char* jkRestoreComponentNodeStmtDnodeId = "DnodeId";

static int32_t restoreComponentNodeStmtToJson(const void* pObj, SJson* pJson) {
  const SRestoreComponentNodeStmt* pNode = (const SRestoreComponentNodeStmt*)pObj;
  return tjsonAddIntegerToObject(pJson, jkRestoreComponentNodeStmtDnodeId, pNode->dnodeId);
}

static int32_t jsonToRestoreComponentNodeStmt(const SJson* pJson, void* pObj) {
  SRestoreComponentNodeStmt* pNode = (SRestoreComponentNodeStmt*)pObj;
  return tjsonGetIntValue(pJson, jkRestoreComponentNodeStmtDnodeId, &pNode->dnodeId);
}

static int32_t jsonToRestoreDnodeStmt(const SJson* pJson, void* pObj) {
  return jsonToRestoreComponentNodeStmt(pJson, pObj);
}
static int32_t jsonToRestoreQnodeStmt(const SJson* pJson, void* pObj) {
  return jsonToRestoreComponentNodeStmt(pJson, pObj);
}
static int32_t jsonToRestoreMnodeStmt(const SJson* pJson, void* pObj) {
  return jsonToRestoreComponentNodeStmt(pJson, pObj);
}
static int32_t jsonToRestoreVnodeStmt(const SJson* pJson, void* pObj) {
  return jsonToRestoreComponentNodeStmt(pJson, pObj);
}





static const char* jkCreateTopicStmtTopicName = "TopicName";
static const char* jkCreateTopicStmtSubscribeDbName = "SubscribeDbName";
static const char* jkCreateTopicStmtIgnoreExists = "IgnoreExists";
static const char* jkCreateTopicStmtQuery = "Query";

static int32_t createTopicStmtToJson(const void* pObj, SJson* pJson) {
  const SCreateTopicStmt* pNode = (const SCreateTopicStmt*)pObj;

  int32_t code = tjsonAddStringToObject(pJson, jkCreateTopicStmtTopicName, pNode->topicName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkCreateTopicStmtSubscribeDbName, pNode->subDbName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkCreateTopicStmtIgnoreExists, pNode->ignoreExists);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkCreateTopicStmtQuery, nodeToJson, pNode->pQuery);
  }

  return code;
}

static int32_t jsonToCreateTopicStmt(const SJson* pJson, void* pObj) {
  SCreateTopicStmt* pNode = (SCreateTopicStmt*)pObj;

  int32_t code = tjsonGetStringValue(pJson, jkCreateTopicStmtTopicName, pNode->topicName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkCreateTopicStmtSubscribeDbName, pNode->subDbName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkCreateTopicStmtIgnoreExists, &pNode->ignoreExists);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkCreateTopicStmtQuery, &pNode->pQuery);
  }

  return code;
}

static const char* jkDropTopicStmtTopicName = "TopicName";
static const char* jkDropTopicStmtIgnoreNotExists = "IgnoreNotExists";

static int32_t dropTopicStmtToJson(const void* pObj, SJson* pJson) {
  const SDropTopicStmt* pNode = (const SDropTopicStmt*)pObj;

  int32_t code = tjsonAddStringToObject(pJson, jkDropTopicStmtTopicName, pNode->topicName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkDropTopicStmtIgnoreNotExists, pNode->ignoreNotExists);
  }

  return code;
}

static int32_t jsonToDropTopicStmt(const SJson* pJson, void* pObj) {
  SDropTopicStmt* pNode = (SDropTopicStmt*)pObj;

  int32_t code = tjsonGetStringValue(pJson, jkDropTopicStmtTopicName, pNode->topicName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkDropTopicStmtIgnoreNotExists, &pNode->ignoreNotExists);
  }

  return code;
}

static const char* jkDropCGroupStmtTopicName = "TopicName";
static const char* jkDropCGroupStmtConsumerGroup = "ConsumerGroup";
static const char* jkDropCGroupStmtIgnoreNotExists = "IgnoreNotExists";

static int32_t dropConsumerGroupStmtToJson(const void* pObj, SJson* pJson) {
  const SDropCGroupStmt* pNode = (const SDropCGroupStmt*)pObj;

  int32_t code = tjsonAddStringToObject(pJson, jkDropCGroupStmtTopicName, pNode->topicName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkDropCGroupStmtConsumerGroup, pNode->cgroup);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkDropCGroupStmtIgnoreNotExists, pNode->ignoreNotExists);
  }

  return code;
}

static int32_t jsonToDropConsumerGroupStmt(const SJson* pJson, void* pObj) {
  SDropCGroupStmt* pNode = (SDropCGroupStmt*)pObj;

  int32_t code = tjsonGetStringValue(pJson, jkDropCGroupStmtTopicName, pNode->topicName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkDropCGroupStmtConsumerGroup, pNode->cgroup);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkDropCGroupStmtIgnoreNotExists, &pNode->ignoreNotExists);
  }

  return code;
}

static const char* jkAlterLocalStmtConfig = "Config";
static const char* jkAlterLocalStmtValue = "Value";

static int32_t alterLocalStmtToJson(const void* pObj, SJson* pJson) {
  const SAlterLocalStmt* pNode = (const SAlterLocalStmt*)pObj;

  int32_t code = tjsonAddStringToObject(pJson, jkAlterLocalStmtConfig, pNode->config);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkAlterLocalStmtValue, pNode->value);
  }

  return code;
}

static int32_t jsonToAlterLocalStmt(const SJson* pJson, void* pObj) {
  SAlterLocalStmt* pNode = (SAlterLocalStmt*)pObj;

  int32_t code = tjsonGetStringValue(pJson, jkAlterLocalStmtConfig, pNode->config);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkAlterLocalStmtValue, pNode->value);
  }

  return code;
}

static const char* jkExplainStmtAnalyze = "Analyze";
static const char* jkExplainStmtOptions = "Options";
static const char* jkExplainStmtQuery = "Query";

static int32_t explainStmtToJson(const void* pObj, SJson* pJson) {
  const SExplainStmt* pNode = (const SExplainStmt*)pObj;

  int32_t code = tjsonAddBoolToObject(pJson, jkExplainStmtAnalyze, pNode->analyze);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkExplainStmtOptions, nodeToJson, pNode->pOptions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkExplainStmtQuery, nodeToJson, pNode->pQuery);
  }

  return code;
}

static int32_t jsonToExplainStmt(const SJson* pJson, void* pObj) {
  SExplainStmt* pNode = (SExplainStmt*)pObj;

  int32_t code = tjsonGetBoolValue(pJson, jkExplainStmtAnalyze, &pNode->analyze);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkExplainStmtOptions, (SNode**)&pNode->pOptions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkExplainStmtQuery, &pNode->pQuery);
  }

  return code;
}

static const char* jkDescribeStmtDbName = "DbName";
static const char* jkDescribeStmtTableName = "TableName";

static int32_t describeStmtToJson(const void* pObj, SJson* pJson) {
  const SDescribeStmt* pNode = (const SDescribeStmt*)pObj;

  int32_t code = tjsonAddStringToObject(pJson, jkDescribeStmtDbName, pNode->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkDescribeStmtTableName, pNode->tableName);
  }

  return code;
}

static int32_t jsonToDescribeStmt(const SJson* pJson, void* pObj) {
  SDescribeStmt* pNode = (SDescribeStmt*)pObj;

  int32_t code = tjsonGetStringValue(pJson, jkDescribeStmtDbName, pNode->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkDescribeStmtTableName, pNode->tableName);
  }

  return code;
}

static const char* jkCompactDatabaseStmtDbName = "DbName";

static int32_t compactDatabaseStmtToJson(const void* pObj, SJson* pJson) {
  const SCompactDatabaseStmt* pNode = (const SCompactDatabaseStmt*)pObj;
  return tjsonAddStringToObject(pJson, jkCompactDatabaseStmtDbName, pNode->dbName);
}

static int32_t jsonToCompactDatabaseStmt(const SJson* pJson, void* pObj) {
  SCompactDatabaseStmt* pNode = (SCompactDatabaseStmt*)pObj;
  return tjsonGetStringValue(pJson, jkCompactDatabaseStmtDbName, pNode->dbName);
}

static const char* jkCreateStreamStmtStreamName = "StreamName";
static const char* jkCreateStreamStmtTargetDbName = "TargetDbName";
static const char* jkCreateStreamStmtTargetTabName = "TargetTabName";
static const char* jkCreateStreamStmtIgnoreExists = "IgnoreExists";
static const char* jkCreateStreamStmtOptions = "Options";
static const char* jkCreateStreamStmtQuery = "Query";
static const char* jkCreateStreamStmtTags = "Tags";
static const char* jkCreateStreamStmtSubtable = "Subtable";

static int32_t createStreamStmtToJson(const void* pObj, SJson* pJson) {
  const SCreateStreamStmt* pNode = (const SCreateStreamStmt*)pObj;

  int32_t code = tjsonAddStringToObject(pJson, jkCreateStreamStmtStreamName, pNode->streamName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkCreateStreamStmtTargetDbName, pNode->targetDbName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkCreateStreamStmtTargetTabName, pNode->targetTabName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkCreateStreamStmtIgnoreExists, pNode->ignoreExists);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkCreateStreamStmtOptions, nodeToJson, pNode->pOptions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkCreateStreamStmtQuery, nodeToJson, pNode->pQuery);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkCreateStreamStmtTags, pNode->pTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkCreateStreamStmtSubtable, nodeToJson, pNode->pSubtable);
  }

  return code;
}

static int32_t jsonToCreateStreamStmt(const SJson* pJson, void* pObj) {
  SCreateStreamStmt* pNode = (SCreateStreamStmt*)pObj;

  int32_t code = tjsonGetStringValue(pJson, jkCreateStreamStmtStreamName, pNode->streamName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkCreateStreamStmtTargetDbName, pNode->targetDbName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkCreateStreamStmtTargetTabName, pNode->targetTabName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkCreateStreamStmtIgnoreExists, &pNode->ignoreExists);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkCreateStreamStmtOptions, (SNode**)&pNode->pOptions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkCreateStreamStmtQuery, &pNode->pQuery);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkCreateStreamStmtTags, &pNode->pTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkCreateStreamStmtSubtable, &pNode->pSubtable);
  }

  return code;
}

static const char* jkDropStreamStmtStreamName = "StreamName";
static const char* jkDropStreamStmtIgnoreNotExists = "IgnoreNotExists";

static int32_t dropStreamStmtToJson(const void* pObj, SJson* pJson) {
  const SDropStreamStmt* pNode = (const SDropStreamStmt*)pObj;

  int32_t code = tjsonAddStringToObject(pJson, jkDropStreamStmtStreamName, pNode->streamName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkDropStreamStmtIgnoreNotExists, pNode->ignoreNotExists);
  }

  return code;
}

static int32_t jsonToDropStreamStmt(const SJson* pJson, void* pObj) {
  SDropStreamStmt* pNode = (SDropStreamStmt*)pObj;

  int32_t code = tjsonGetStringValue(pJson, jkDropStreamStmtStreamName, pNode->streamName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkDropStreamStmtIgnoreNotExists, &pNode->ignoreNotExists);
  }

  return code;
}

static const char* jkMergeVgroupStmtVgroupId1 = "VgroupId1";
static const char* jkMergeVgroupStmtVgroupId2 = "VgroupId2";

static int32_t mergeVgroupStmtToJson(const void* pObj, SJson* pJson) {
  const SMergeVgroupStmt* pNode = (const SMergeVgroupStmt*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkMergeVgroupStmtVgroupId1, pNode->vgId1);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkMergeVgroupStmtVgroupId2, pNode->vgId2);
  }

  return code;
}

static int32_t jsonToMergeVgroupStmt(const SJson* pJson, void* pObj) {
  SMergeVgroupStmt* pNode = (SMergeVgroupStmt*)pObj;

  int32_t code = tjsonGetIntValue(pJson, jkMergeVgroupStmtVgroupId1, &pNode->vgId1);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkMergeVgroupStmtVgroupId2, &pNode->vgId2);
  }

  return code;
}

static const char* jkRedistributeVgroupStmtVgroupId = "VgroupId";
static const char* jkRedistributeVgroupStmtDnodeId1 = "DnodeId1";
static const char* jkRedistributeVgroupStmtDnodeId2 = "DnodeId2";
static const char* jkRedistributeVgroupStmtDnodeId3 = "DnodeId3";
static const char* jkRedistributeVgroupStmtDnodes = "Dnodes";

static int32_t redistributeVgroupStmtToJson(const void* pObj, SJson* pJson) {
  const SRedistributeVgroupStmt* pNode = (const SRedistributeVgroupStmt*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkRedistributeVgroupStmtVgroupId, pNode->vgId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkRedistributeVgroupStmtDnodeId1, pNode->dnodeId1);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkRedistributeVgroupStmtDnodeId2, pNode->dnodeId2);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkRedistributeVgroupStmtDnodeId3, pNode->dnodeId3);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkRedistributeVgroupStmtDnodes, pNode->pDnodes);
  }

  return code;
}

static int32_t jsonToRedistributeVgroupStmt(const SJson* pJson, void* pObj) {
  SRedistributeVgroupStmt* pNode = (SRedistributeVgroupStmt*)pObj;

  int32_t code = tjsonGetIntValue(pJson, jkRedistributeVgroupStmtVgroupId, &pNode->vgId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkRedistributeVgroupStmtDnodeId1, &pNode->dnodeId1);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkRedistributeVgroupStmtDnodeId2, &pNode->dnodeId2);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetIntValue(pJson, jkRedistributeVgroupStmtDnodeId3, &pNode->dnodeId3);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkRedistributeVgroupStmtDnodes, &pNode->pDnodes);
  }

  return code;
}

static const char* jkSplitVgroupStmtVgroupId = "VgroupId";

static int32_t splitVgroupStmtToJson(const void* pObj, SJson* pJson) {
  const SSplitVgroupStmt* pNode = (const SSplitVgroupStmt*)pObj;
  return tjsonAddIntegerToObject(pJson, jkSplitVgroupStmtVgroupId, pNode->vgId);
}

static int32_t jsonToSplitVgroupStmt(const SJson* pJson, void* pObj) {
  SSplitVgroupStmt* pNode = (SSplitVgroupStmt*)pObj;
  return tjsonGetIntValue(pJson, jkSplitVgroupStmtVgroupId, &pNode->vgId);
}

static const char* jkGrantStmtUserName = "UserName";
static const char* jkGrantStmtObjName = "ObjName";
static const char* jkGrantStmtPrivileges = "Privileges";

static int32_t grantStmtToJson(const void* pObj, SJson* pJson) {
  const SGrantStmt* pNode = (const SGrantStmt*)pObj;

  int32_t code = tjsonAddStringToObject(pJson, jkGrantStmtUserName, pNode->userName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkGrantStmtObjName, pNode->objName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkGrantStmtPrivileges, pNode->privileges);
  }

  return code;
}

static int32_t jsonToGrantStmt(const SJson* pJson, void* pObj) {
  SGrantStmt* pNode = (SGrantStmt*)pObj;

  int32_t code = tjsonGetStringValue(pJson, jkGrantStmtUserName, pNode->userName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkGrantStmtObjName, pNode->objName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkGrantStmtPrivileges, &pNode->privileges);
  }

  return code;
}

static int32_t revokeStmtToJson(const void* pObj, SJson* pJson) { return grantStmtToJson(pObj, pJson); }

static int32_t jsonToRevokeStmt(const SJson* pJson, void* pObj) { return jsonToGrantStmt(pJson, pObj); }

static const char* jkShowStmtDbName = "DbName";
static const char* jkShowStmtTbName = "TbName";
static const char* jkShowStmtTableCondType = "TableCondType";

static int32_t showStmtToJson(const void* pObj, SJson* pJson) {
  const SShowStmt* pNode = (const SShowStmt*)pObj;

  int32_t code = tjsonAddObject(pJson, jkShowStmtDbName, nodeToJson, pNode->pDbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkShowStmtTbName, nodeToJson, pNode->pTbName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkShowStmtTableCondType, pNode->tableCondType);
  }

  return code;
}

static int32_t jsonToShowStmt(const SJson* pJson, void* pObj) {
  SShowStmt* pNode = (SShowStmt*)pObj;

  int32_t code = jsonToNodeObject(pJson, jkShowStmtDbName, &pNode->pDbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkShowStmtTbName, &pNode->pTbName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    tjsonGetNumberValue(pJson, jkShowStmtTableCondType, pNode->tableCondType, code);
  }

  return code;
}

static int32_t showDnodesStmtToJson(const void* pObj, SJson* pJson) { return showStmtToJson(pObj, pJson); }

static int32_t jsonToShowDnodesStmt(const SJson* pJson, void* pObj) { return jsonToShowStmt(pJson, pObj); }

static int32_t showMnodesStmtToJson(const void* pObj, SJson* pJson) { return showStmtToJson(pObj, pJson); }

static int32_t jsonToShowMnodesStmt(const SJson* pJson, void* pObj) { return jsonToShowStmt(pJson, pObj); }

static int32_t showQnodesStmtToJson(const void* pObj, SJson* pJson) { return showStmtToJson(pObj, pJson); }

static int32_t jsonToShowQnodesStmt(const SJson* pJson, void* pObj) { return jsonToShowStmt(pJson, pObj); }

static int32_t showClusterStmtToJson(const void* pObj, SJson* pJson) { return showStmtToJson(pObj, pJson); }

static int32_t jsonToShowClusterStmt(const SJson* pJson, void* pObj) { return jsonToShowStmt(pJson, pObj); }

static int32_t showDatabasesStmtToJson(const void* pObj, SJson* pJson) { return showStmtToJson(pObj, pJson); }

static int32_t jsonToShowDatabasesStmt(const SJson* pJson, void* pObj) { return jsonToShowStmt(pJson, pObj); }

static int32_t showFunctionsStmtToJson(const void* pObj, SJson* pJson) { return showStmtToJson(pObj, pJson); }

static int32_t jsonToShowFunctionsStmt(const SJson* pJson, void* pObj) { return jsonToShowStmt(pJson, pObj); }

static int32_t showIndexesStmtToJson(const void* pObj, SJson* pJson) { return showStmtToJson(pObj, pJson); }

static int32_t jsonToShowIndexesStmt(const SJson* pJson, void* pObj) { return jsonToShowStmt(pJson, pObj); }

static int32_t showStablesStmtToJson(const void* pObj, SJson* pJson) { return showStmtToJson(pObj, pJson); }

static int32_t jsonToShowStablesStmt(const SJson* pJson, void* pObj) { return jsonToShowStmt(pJson, pObj); }

static int32_t showStreamsStmtToJson(const void* pObj, SJson* pJson) { return showStmtToJson(pObj, pJson); }

static int32_t jsonToShowStreamsStmt(const SJson* pJson, void* pObj) { return jsonToShowStmt(pJson, pObj); }

static int32_t showTablesStmtToJson(const void* pObj, SJson* pJson) { return showStmtToJson(pObj, pJson); }

static int32_t jsonToShowTablesStmt(const SJson* pJson, void* pObj) { return jsonToShowStmt(pJson, pObj); }

static int32_t showTagsStmtToJson(const void* pObj, SJson* pJson) { return showStmtToJson(pObj, pJson); }

static int32_t jsonToShowTagsStmt(const SJson* pJson, void* pObj) { return jsonToShowStmt(pJson, pObj); }

static int32_t showUsersStmtToJson(const void* pObj, SJson* pJson) { return showStmtToJson(pObj, pJson); }

static int32_t jsonToShowUsersStmt(const SJson* pJson, void* pObj) { return jsonToShowStmt(pJson, pObj); }

static int32_t showVgroupsStmtToJson(const void* pObj, SJson* pJson) { return showStmtToJson(pObj, pJson); }

static int32_t jsonToShowVgroupsStmt(const SJson* pJson, void* pObj) { return jsonToShowStmt(pJson, pObj); }

static int32_t showConsumersStmtToJson(const void* pObj, SJson* pJson) { return showStmtToJson(pObj, pJson); }

static int32_t jsonToShowConsumersStmt(const SJson* pJson, void* pObj) { return jsonToShowStmt(pJson, pObj); }

static int32_t showVariablesStmtToJson(const void* pObj, SJson* pJson) { return showStmtToJson(pObj, pJson); }

static int32_t jsonToShowVariablesStmt(const SJson* pJson, void* pObj) { return jsonToShowStmt(pJson, pObj); }

static const char* jkShowDnodeVariablesStmtDnodeId = "DnodeId";
static const char* jkShowDnodeVariablesStmtLikePattern = "LikePattern";

static int32_t showDnodeVariablesStmtToJson(const void* pObj, SJson* pJson) {
  const SShowDnodeVariablesStmt* pNode = (const SShowDnodeVariablesStmt*)pObj;

  int32_t code = tjsonAddObject(pJson, jkShowDnodeVariablesStmtDnodeId, nodeToJson, pNode->pDnodeId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkShowDnodeVariablesStmtLikePattern, nodeToJson, pNode->pLikePattern);
  }

  return code;
}

static int32_t jsonToShowDnodeVariablesStmt(const SJson* pJson, void* pObj) {
  SShowDnodeVariablesStmt* pNode = (SShowDnodeVariablesStmt*)pObj;

  int32_t code = jsonToNodeObject(pJson, jkShowDnodeVariablesStmtDnodeId, &pNode->pDnodeId);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkShowDnodeVariablesStmtLikePattern, &pNode->pLikePattern);
  }

  return code;
}

static int32_t showTransactionsStmtToJson(const void* pObj, SJson* pJson) { return showStmtToJson(pObj, pJson); }

static int32_t jsonToShowTransactionsStmt(const SJson* pJson, void* pObj) { return jsonToShowStmt(pJson, pObj); }

static int32_t showSubscriptionsStmtToJson(const void* pObj, SJson* pJson) { return showStmtToJson(pObj, pJson); }

static int32_t jsonToShowSubscriptionsStmt(const SJson* pJson, void* pObj) { return jsonToShowStmt(pJson, pObj); }

static const char* jkShowVnodesStmtDnodeId = "DnodeId";
static const char* jkShowVnodesStmtDnodeEndpoint = "DnodeEndpoint";

static int32_t showVnodesStmtToJson(const void* pObj, SJson* pJson) {
  const SShowVnodesStmt* pNode = (const SShowVnodesStmt*)pObj;

  int32_t code = tjsonAddObject(pJson, jkShowVnodesStmtDnodeId, nodeToJson, pNode->pDnodeId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkShowVnodesStmtDnodeEndpoint, nodeToJson, pNode->pDnodeEndpoint);
  }

  return code;
}

static int32_t jsonToShowVnodesStmt(const SJson* pJson, void* pObj) {
  SShowVnodesStmt* pNode = (SShowVnodesStmt*)pObj;

  int32_t code = jsonToNodeObject(pJson, jkShowVnodesStmtDnodeId, &pNode->pDnodeId);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkShowVnodesStmtDnodeEndpoint, &pNode->pDnodeEndpoint);
  }

  return code;
}

static int32_t showUserPrivilegesStmtToJson(const void* pObj, SJson* pJson) { return showStmtToJson(pObj, pJson); }

static int32_t jsonToShowUserPrivilegesStmt(const SJson* pJson, void* pObj) { return jsonToShowStmt(pJson, pObj); }

static const char* jkShowCreateDatabaseStmtDbName = "DbName";

static int32_t showCreateDatabaseStmtToJson(const void* pObj, SJson* pJson) {
  const SShowCreateDatabaseStmt* pNode = (const SShowCreateDatabaseStmt*)pObj;
  return tjsonAddStringToObject(pJson, jkShowCreateDatabaseStmtDbName, pNode->dbName);
}

static int32_t jsonToShowCreateDatabaseStmt(const SJson* pJson, void* pObj) {
  SShowCreateDatabaseStmt* pNode = (SShowCreateDatabaseStmt*)pObj;
  return tjsonGetStringValue(pJson, jkShowCreateDatabaseStmtDbName, pNode->dbName);
}

static const char* jkShowCreateTableStmtDbName = "DbName";
static const char* jkShowCreateTableStmtTableName = "TableName";

static int32_t showCreateTableStmtToJson(const void* pObj, SJson* pJson) {
  const SShowCreateTableStmt* pNode = (const SShowCreateTableStmt*)pObj;

  int32_t code = tjsonAddStringToObject(pJson, jkShowCreateTableStmtDbName, pNode->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkShowCreateTableStmtTableName, pNode->tableName);
  }

  return code;
}

static int32_t jsonToShowCreateTableStmt(const SJson* pJson, void* pObj) {
  SShowCreateTableStmt* pNode = (SShowCreateTableStmt*)pObj;

  int32_t code = tjsonGetStringValue(pJson, jkShowCreateTableStmtDbName, pNode->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkShowCreateTableStmtTableName, pNode->tableName);
  }

  return code;
}

static int32_t showCreateStableStmtToJson(const void* pObj, SJson* pJson) {
  return showCreateTableStmtToJson(pObj, pJson);
}

static int32_t jsonToShowCreateStableStmt(const SJson* pJson, void* pObj) {
  return jsonToShowCreateTableStmt(pJson, pObj);
}

static const char* jkShowCreateViewStmtDbName = "DbName";
static const char* jkShowCreateViewStmtViewName = "ViewName";

static int32_t showCreateViewStmtToJson(const void* pObj, SJson* pJson) {
  const SShowCreateViewStmt* pNode = (const SShowCreateViewStmt*)pObj;

  int32_t code = tjsonAddStringToObject(pJson, jkShowCreateViewStmtDbName, pNode->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkShowCreateViewStmtViewName, pNode->viewName);
  }

  return code;
}


static int32_t jsonToShowCreateViewStmt(const SJson* pJson, void* pObj) {
  SShowCreateViewStmt* pNode = (SShowCreateViewStmt*)pObj;

  int32_t code = tjsonGetStringValue(pJson, jkShowCreateViewStmtDbName, pNode->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkShowCreateViewStmtViewName, pNode->viewName);
  }

  return code;
}


static const char* jkShowTableDistributedStmtDbName = "DbName";
static const char* jkShowTableDistributedStmtTableName = "TableName";

static int32_t showTableDistributedStmtToJson(const void* pObj, SJson* pJson) {
  const SShowTableDistributedStmt* pNode = (const SShowTableDistributedStmt*)pObj;

  int32_t code = tjsonAddStringToObject(pJson, jkShowTableDistributedStmtDbName, pNode->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkShowTableDistributedStmtTableName, pNode->tableName);
  }

  return code;
}

static int32_t jsonToShowTableDistributedStmt(const SJson* pJson, void* pObj) {
  SShowTableDistributedStmt* pNode = (SShowTableDistributedStmt*)pObj;

  int32_t code = tjsonGetStringValue(pJson, jkShowTableDistributedStmtDbName, pNode->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetStringValue(pJson, jkShowTableDistributedStmtTableName, pNode->tableName);
  }

  return code;
}

static int32_t showLocalVariablesStmtToJson(const void* pObj, SJson* pJson) { return showStmtToJson(pObj, pJson); }

static int32_t jsonToShowLocalVariablesStmt(const SJson* pJson, void* pObj) { return jsonToShowStmt(pJson, pObj); }

static const char* jkShowTableTagsStmtDbName = "DbName";
static const char* jkShowTableTagsStmtTbName = "TbName";
static const char* jkShowTableTagsStmtTags = "Tags";

static int32_t showTableTagsStmtToJson(const void* pObj, SJson* pJson) {
  const SShowTableTagsStmt* pNode = (const SShowTableTagsStmt*)pObj;

  int32_t code = tjsonAddObject(pJson, jkShowTableTagsStmtDbName, nodeToJson, pNode->pDbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkShowTableTagsStmtTbName, nodeToJson, pNode->pTbName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkShowTableTagsStmtTags, pNode->pTags);
  }

  return code;
}

static int32_t jsonToShowTableTagsStmt(const SJson* pJson, void* pObj) {
  SShowTableTagsStmt* pNode = (SShowTableTagsStmt*)pObj;

  int32_t code = jsonToNodeObject(pJson, jkShowTableTagsStmtDbName, &pNode->pDbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkShowTableTagsStmtTbName, &pNode->pTbName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkShowTableTagsStmtTags, &pNode->pTags);
  }

  return code;
}

static const char* jkDeleteStmtFromTable = "FromTable";
static const char* jkDeleteStmtWhere = "Where";
static const char* jkDeleteStmtCountFunc = "CountFunc";
static const char* jkDeleteStmtTagIndexCond = "TagIndexCond";
static const char* jkDeleteStmtTimeRangeStartKey = "TimeRangeStartKey";
static const char* jkDeleteStmtTimeRangeEndKey = "TimeRangeEndKey";
static const char* jkDeleteStmtPrecision = "Precision";
static const char* jkDeleteStmtDeleteZeroRows = "DeleteZeroRows";

static int32_t deleteStmtToJson(const void* pObj, SJson* pJson) {
  const SDeleteStmt* pNode = (const SDeleteStmt*)pObj;

  int32_t code = tjsonAddObject(pJson, jkDeleteStmtFromTable, nodeToJson, pNode->pFromTable);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkDeleteStmtWhere, nodeToJson, pNode->pWhere);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkDeleteStmtCountFunc, nodeToJson, pNode->pCountFunc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkDeleteStmtTagIndexCond, nodeToJson, pNode->pTagCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDeleteStmtTimeRangeStartKey, pNode->timeRange.skey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDeleteStmtTimeRangeEndKey, pNode->timeRange.ekey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkDeleteStmtPrecision, pNode->precision);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddBoolToObject(pJson, jkDeleteStmtDeleteZeroRows, pNode->deleteZeroRows);
  }

  return code;
}

static int32_t jsonToDeleteStmt(const SJson* pJson, void* pObj) {
  SDeleteStmt* pNode = (SDeleteStmt*)pObj;

  int32_t code = jsonToNodeObject(pJson, jkDeleteStmtFromTable, &pNode->pFromTable);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkDeleteStmtWhere, &pNode->pWhere);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkDeleteStmtCountFunc, &pNode->pCountFunc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkDeleteStmtTagIndexCond, &pNode->pTagCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkDeleteStmtTimeRangeStartKey, &pNode->timeRange.skey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBigIntValue(pJson, jkDeleteStmtTimeRangeEndKey, &pNode->timeRange.ekey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetUTinyIntValue(pJson, jkDeleteStmtPrecision, &pNode->precision);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetBoolValue(pJson, jkDeleteStmtDeleteZeroRows, &pNode->deleteZeroRows);
  }

  return code;
}

static const char* jkInsertStmtTable = "Table";
static const char* jkInsertStmtCols = "Cols";
static const char* jkInsertStmtQuery = "Query";
static const char* jkInsertStmtPrecision = "Precision";

static int32_t insertStmtToJson(const void* pObj, SJson* pJson) {
  const SInsertStmt* pNode = (const SInsertStmt*)pObj;

  int32_t code = tjsonAddObject(pJson, jkInsertStmtTable, nodeToJson, pNode->pTable);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeListToJson(pJson, jkInsertStmtCols, pNode->pCols);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, jkInsertStmtQuery, nodeToJson, pNode->pQuery);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkInsertStmtPrecision, pNode->precision);
  }

  return code;
}

static int32_t jsonToInsertStmt(const SJson* pJson, void* pObj) {
  SInsertStmt* pNode = (SInsertStmt*)pObj;

  int32_t code = jsonToNodeObject(pJson, jkInsertStmtTable, &pNode->pTable);
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeList(pJson, jkInsertStmtCols, &pNode->pCols);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = jsonToNodeObject(pJson, jkInsertStmtQuery, &pNode->pQuery);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetUTinyIntValue(pJson, jkInsertStmtPrecision, &pNode->precision);
  }

  return code;
}

int32_t specificNodeToJson(const void* pObj, SJson* pJson) {
  ENodeType type = nodeType(pObj);
  if (!funcArrayCheck(type)) {
    return TSDB_CODE_SUCCESS;
  }

  if (funcNodes[type].toJsonFunc) {
    return funcNodes[type].toJsonFunc(pObj, pJson);
  }

  nodesWarn("specificNodeToJson unknown node type = %d", type);
  return TSDB_CODE_SUCCESS;
}

int32_t jsonToSpecificNode(const SJson* pJson, void* pObj) {
  ENodeType type = nodeType(pObj);
  if (!funcArrayCheck(type)) {
    return TSDB_CODE_SUCCESS;
  }

  if (funcNodes[type].toNodeFunc) {
    return funcNodes[type].toNodeFunc(pJson, pObj);
  }

  nodesWarn("jsonToSpecificNode unknown node type = %d", type);
  return TSDB_CODE_SUCCESS;
}

static const char* jkNodeType = "NodeType";
static const char* jkNodeName = "Name";

static int32_t nodeToJson(const void* pObj, SJson* pJson) {
  const SNode* pNode = (const SNode*)pObj;

  int32_t code = tjsonAddIntegerToObject(pJson, jkNodeType, pNode->type);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkNodeName, nodesNodeName(pNode->type));
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddObject(pJson, nodesNodeName(pNode->type), specificNodeToJson, pNode);
    if (TSDB_CODE_SUCCESS != code) {
      nodesError("%s ToJson error", nodesNodeName(pNode->type));
    }
  }

  return code;
}

static int32_t jsonToNode(const SJson* pJson, void* pObj) {
  SNode* pNode = (SNode*)pObj;

  int32_t code;
  tjsonGetNumberValue(pJson, jkNodeType, pNode->type, code);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonToObject(pJson, nodesNodeName(pNode->type), jsonToSpecificNode, pNode);
    if (TSDB_CODE_SUCCESS != code) {
      nodesError("%s toNode error", nodesNodeName(pNode->type));
    }
  }

  return code;
}

static int32_t makeNodeByJson(const SJson* pJson, SNode** pNode) {
  int32_t val = 0;
  int32_t code = tjsonGetIntValue(pJson, jkNodeType, &val);
  if (TSDB_CODE_SUCCESS == code) {
    *pNode = nodesMakeNode(val);
    if (NULL == *pNode) {
      return TSDB_CODE_FAILED;
    }
    code = jsonToNode(pJson, *pNode);
  }

  return code;
}

static int32_t jsonToNodeObject(const SJson* pJson, const char* pName, SNode** pNode) {
  SJson* pJsonNode = tjsonGetObjectItem(pJson, pName);
  if (NULL == pJsonNode) {
    return TSDB_CODE_SUCCESS;
  }
  return makeNodeByJson(pJsonNode, pNode);
}

int32_t nodesNodeToString(const SNode* pNode, bool format, char** pStr, int32_t* pLen) {
  if (NULL == pNode || NULL == pStr) {
    terrno = TSDB_CODE_FAILED;
    return TSDB_CODE_FAILED;
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

  if (NULL != pLen) {
    *pLen = strlen(*pStr) + 1;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t nodesStringToNode(const char* pStr, SNode** pNode) {
  if (NULL == pStr || NULL == pNode) {
    return TSDB_CODE_SUCCESS;
  }
  SJson* pJson = tjsonParse(pStr);
  if (NULL == pJson) {
    return TSDB_CODE_FAILED;
  }
  int32_t code = makeNodeByJson(pJson, pNode);
  tjsonDelete(pJson);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(*pNode);
    *pNode = NULL;
    terrno = code;
    return code;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t nodesListToString(const SNodeList* pList, bool format, char** pStr, int32_t* pLen) {
  if (NULL == pList || NULL == pStr || NULL == pLen) {
    terrno = TSDB_CODE_FAILED;
    return TSDB_CODE_FAILED;
  }

  if (0 == LIST_LENGTH(pList)) {
    return TSDB_CODE_SUCCESS;
  }

  SJson* pJson = tjsonCreateArray();
  if (NULL == pJson) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SNode* pNode;
  FOREACH(pNode, pList) {
    int32_t code = tjsonAddItem(pJson, nodeToJson, pNode);
    if (TSDB_CODE_SUCCESS != code) {
      terrno = code;
      return code;
    }
  }

  *pStr = format ? tjsonToString(pJson) : tjsonToUnformattedString(pJson);
  tjsonDelete(pJson);

  *pLen = strlen(*pStr) + 1;
  return TSDB_CODE_SUCCESS;
}

int32_t nodesStringToList(const char* pStr, SNodeList** pList) {
  if (NULL == pStr || NULL == pList) {
    return TSDB_CODE_SUCCESS;
  }
  SJson* pJson = tjsonParse(pStr);
  if (NULL == pJson) {
    return TSDB_CODE_FAILED;
  }
  int32_t code = jsonToNodeListImpl(pJson, pList);
  tjsonDelete(pJson);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyList(*pList);
    *pList = NULL;
    terrno = code;
    return code;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t emptyNodeToJson(const void* pObj, SJson* pJson) {
    return TSDB_CODE_SUCCESS;
}

static int32_t emptyJsonToNode(const  SJson* pJson, void* pObj) {
    return TSDB_CODE_SUCCESS;
}

static void destroyVgDataBlockArray(SArray* pArray) {
  size_t size = taosArrayGetSize(pArray);
  for (size_t i = 0; i < size; ++i) {
    SVgDataBlocks* pVg = taosArrayGetP(pArray, i);
    taosMemoryFreeClear(pVg->pData);
    taosMemoryFreeClear(pVg);
  }
  taosArrayDestroy(pArray);
}

static void destroyLogicNode(SLogicNode* pNode) {
  nodesDestroyList(pNode->pTargets);
  nodesDestroyNode(pNode->pConditions);
  nodesDestroyList(pNode->pChildren);
  nodesDestroyNode(pNode->pLimit);
  nodesDestroyNode(pNode->pSlimit);
  nodesDestroyList(pNode->pHint);
}

void destroyPhysiNode(SNode* pInput) {
  SPhysiNode* pNode = (SPhysiNode*)pInput;
  nodesDestroyList(pNode->pChildren);
  nodesDestroyNode(pNode->pConditions);
  nodesDestroyNode((SNode*)pNode->pOutputDataBlockDesc);
  nodesDestroyNode(pNode->pLimit);
  nodesDestroyNode(pNode->pSlimit);
}

void destroyExprNode(SNode* pNode) {
  SExprNode* pExpr = (SExprNode*)pNode;
  taosArrayDestroy(pExpr->pAssociation); 
}

void destroyDataInSmaIndex(void* pIndex) {
  taosMemoryFree(((STableIndexInfo*)pIndex)->expr); 
}

void destoryXNode(SNode* pNode) {}

void destroyColumnNode(SNode* pNode) { 
  destroyExprNode(pNode); 
}

void destroyValueNode(SNode* pNode) {
  SValueNode* pValue = (SValueNode*)pNode;
  destroyExprNode(pNode);
  taosMemoryFreeClear(pValue->literal);
  if (IS_VAR_DATA_TYPE(pValue->node.resType.type)) {
    taosMemoryFreeClear(pValue->datum.p);
  }
}

void destroyOperatorNode(SNode* pNode) {
  SOperatorNode* pOp = (SOperatorNode*)pNode;
  destroyExprNode(pNode);
  nodesDestroyNode(pOp->pLeft);
  nodesDestroyNode(pOp->pRight);
}

void destoryLogicConditionNode(SNode* pNode) {
  destroyExprNode(pNode);
  nodesDestroyList(((SLogicConditionNode*)pNode)->pParameterList);
}

void destoryFunctionNode(SNode* pNode) {
  destroyExprNode(pNode);
  nodesDestroyList(((SFunctionNode*)pNode)->pParameterList);
}

void destoryRealTableNode(SNode* pNode) {
  SRealTableNode* pReal = (SRealTableNode*)pNode;
  taosMemoryFreeClear(pReal->pMeta);
  taosMemoryFreeClear(pReal->pVgroupList);
  taosArrayDestroyEx(pReal->pSmaIndexes, destroyDataInSmaIndex);
}

void destoryTempTableNode(SNode* pNode) { nodesDestroyNode(((STempTableNode*)pNode)->pSubquery); }

void destoryJoinTableNode(SNode* pNode) {
  SJoinTableNode* pJoin = (SJoinTableNode*)pNode;
  nodesDestroyNode(pJoin->pLeft);
  nodesDestroyNode(pJoin->pRight);
  nodesDestroyNode(pJoin->pOnCond);
}

void destoryGroupingSetNode(SNode* pNode) { nodesDestroyList(((SGroupingSetNode*)pNode)->pParameterList); }

void destoryOrderByExprNode(SNode* pNode) { nodesDestroyNode(((SOrderByExprNode*)pNode)->pExpr); }

void destoryStateWindowNode(SNode* pNode) {
  SStateWindowNode* pState = (SStateWindowNode*)pNode;
  nodesDestroyNode(pState->pCol);
  nodesDestroyNode(pState->pExpr);
}

void destorySessionWindowNode(SNode* pNode) {
  SSessionWindowNode* pSession = (SSessionWindowNode*)pNode;
  nodesDestroyNode((SNode*)pSession->pCol);
  nodesDestroyNode((SNode*)pSession->pGap);
}

void destoryIntervalWindowNode(SNode* pNode) {
  SIntervalWindowNode* pJoin = (SIntervalWindowNode*)pNode;
  nodesDestroyNode(pJoin->pCol);
  nodesDestroyNode(pJoin->pInterval);
  nodesDestroyNode(pJoin->pOffset);
  nodesDestroyNode(pJoin->pSliding);
  nodesDestroyNode(pJoin->pFill);
}

void destoryNodeListNode(SNode* pNode) { nodesDestroyList(((SNodeListNode*)pNode)->pNodeList); }

void destoryFillNode(SNode* pNode) {
  SFillNode* pFill = (SFillNode*)pNode;
  nodesDestroyNode(pFill->pValues);
  nodesDestroyNode(pFill->pWStartTs);
}

void destoryRawExprNode(SNode* pNode) { nodesDestroyNode(((SRawExprNode*)pNode)->pNode); }

void destoryTargetNode(SNode* pNode) { nodesDestroyNode(((STargetNode*)pNode)->pExpr); }

void destoryDataBlockDescNode(SNode* pNode) { nodesDestroyList(((SDataBlockDescNode*)pNode)->pSlots); }

void destoryDatabaseOptions(SNode* pNode) {
  SDatabaseOptions* pOptions = (SDatabaseOptions*)pNode;
  nodesDestroyNode((SNode*)pOptions->pDaysPerFile);
  nodesDestroyList(pOptions->pKeep);
  nodesDestroyList(pOptions->pRetentions);
}

void destoryTableOptions(SNode* pNode) {
  STableOptions* pOptions = (STableOptions*)pNode;
  nodesDestroyList(pOptions->pMaxDelay);
  nodesDestroyList(pOptions->pWatermark);
  nodesDestroyList(pOptions->pRollupFuncs);
  nodesDestroyList(pOptions->pSma);
  nodesDestroyList(pOptions->pDeleteMark);
}

void destoryIndexOptions(SNode* pNode) {
  SIndexOptions* pOptions = (SIndexOptions*)pNode;
  nodesDestroyList(pOptions->pFuncs);
  nodesDestroyNode(pOptions->pInterval);
  nodesDestroyNode(pOptions->pOffset);
  nodesDestroyNode(pOptions->pSliding);
  nodesDestroyNode(pOptions->pStreamOptions);
}

void destoryStreamOptions(SNode* pNode) {
  SStreamOptions* pOptions = (SStreamOptions*)pNode;
  nodesDestroyNode(pOptions->pDelay);
  nodesDestroyNode(pOptions->pWatermark);
  nodesDestroyNode(pOptions->pDeleteMark);
}

void destoryWhenThenNode(SNode* pNode) {
  SWhenThenNode* pWhenThen = (SWhenThenNode*)pNode;
  nodesDestroyNode(pWhenThen->pWhen);
  nodesDestroyNode(pWhenThen->pThen);
}

void destoryCaseWhenNode(SNode* pNode) {
  SCaseWhenNode* pCaseWhen = (SCaseWhenNode*)pNode;
  nodesDestroyNode(pCaseWhen->pCase);
  nodesDestroyNode(pCaseWhen->pElse);
  nodesDestroyList(pCaseWhen->pWhenThenList);
}

void destoryEventWindowNode(SNode* pNode) {
  SEventWindowNode* pEvent = (SEventWindowNode*)pNode;
  nodesDestroyNode(pEvent->pCol);
  nodesDestroyNode(pEvent->pStartCond);
  nodesDestroyNode(pEvent->pEndCond);
}

void destoryHintNode(SNode* pNode) {
  SHintNode* pHint = (SHintNode*)pNode;
  taosMemoryFree(pHint->value);
}

void destoryViewNode(SNode* pNode) {
  SViewNode* pView = (SViewNode*)pNode;
  taosMemoryFreeClear(pView->pMeta);
  taosMemoryFreeClear(pView->pVgroupList);
  taosArrayDestroyEx(pView->pSmaIndexes, destroyDataInSmaIndex);
}

void destorySetOperator(SNode* pNode) {
  SSetOperator* pStmt = (SSetOperator*)pNode;
  nodesDestroyList(pStmt->pProjectionList);
  nodesDestroyNode(pStmt->pLeft);
  nodesDestroyNode(pStmt->pRight);
  nodesDestroyList(pStmt->pOrderByList);
  nodesDestroyNode(pStmt->pLimit);
}

void destorySelectStmt(SNode* pNode) {
  SSelectStmt* pStmt = (SSelectStmt*)pNode;
  nodesDestroyList(pStmt->pProjectionList);
  nodesDestroyNode(pStmt->pFromTable);
  nodesDestroyNode(pStmt->pWhere);
  nodesDestroyList(pStmt->pPartitionByList);
  nodesDestroyList(pStmt->pTags);
  nodesDestroyNode(pStmt->pSubtable);
  nodesDestroyNode(pStmt->pWindow);
  nodesDestroyList(pStmt->pGroupByList);
  nodesDestroyNode(pStmt->pHaving);
  nodesDestroyNode(pStmt->pRange);
  nodesDestroyNode(pStmt->pEvery);
  nodesDestroyNode(pStmt->pFill);
  nodesDestroyList(pStmt->pOrderByList);
  nodesDestroyNode((SNode*)pStmt->pLimit);
  nodesDestroyNode((SNode*)pStmt->pSlimit);
  nodesDestroyList(pStmt->pHint);
}

void destoryVnodeModifyOpStmt(SNode* pNode) {
  SVnodeModifyOpStmt* pStmt = (SVnodeModifyOpStmt*)pNode;
  destroyVgDataBlockArray(pStmt->pDataBlocks);
  taosMemoryFreeClear(pStmt->pTableMeta);
  nodesDestroyNode(pStmt->pTagCond);
  taosArrayDestroy(pStmt->pTableTag);
  taosHashCleanup(pStmt->pVgroupsHashObj);
  taosHashCleanup(pStmt->pSubTableHashObj);
  taosHashCleanup(pStmt->pTableNameHashObj);
  taosHashCleanup(pStmt->pDbFNameHashObj);
  if (pStmt->freeHashFunc) {
    pStmt->freeHashFunc(pStmt->pTableBlockHashObj);
  }
  if (pStmt->freeArrayFunc) {
    pStmt->freeArrayFunc(pStmt->pVgDataBlocks);
  }
  tdDestroySVCreateTbReq(pStmt->pCreateTblReq);
  taosMemoryFreeClear(pStmt->pCreateTblReq);
  if (pStmt->freeStbRowsCxtFunc) {
    pStmt->freeStbRowsCxtFunc(pStmt->pStbRowsCxt);
  }
  taosMemoryFreeClear(pStmt->pStbRowsCxt);
  taosCloseFile(&pStmt->fp);
}

void destoryCreateDatabaseStmt(SNode* pNode) { nodesDestroyNode((SNode*)((SCreateDatabaseStmt*)pNode)->pOptions); }

void destoryAlterDatabaseStmt(SNode* pNode) { nodesDestroyNode((SNode*)((SAlterDatabaseStmt*)pNode)->pOptions); }

void destoryCreateTableStmt(SNode* pNode) {
  SCreateTableStmt* pStmt = (SCreateTableStmt*)pNode;
  nodesDestroyList(pStmt->pCols);
  nodesDestroyList(pStmt->pTags);
  nodesDestroyNode((SNode*)pStmt->pOptions);
}

void destoryCreateSubTableClause(SNode* pNode) {
  SCreateSubTableClause* pStmt = (SCreateSubTableClause*)pNode;
  nodesDestroyList(pStmt->pSpecificTags);
  nodesDestroyList(pStmt->pValsOfTags);
  nodesDestroyNode((SNode*)pStmt->pOptions);
}

void destoryCreateMultiTablesStmt(SNode* pNode) { 
  nodesDestroyList(((SCreateMultiTablesStmt*)pNode)->pSubTables); 
}

void destoryDropTableStmt(SNode* pNode) { 
  nodesDestroyList(((SDropTableStmt*)pNode)->pTables); 
}

void destoryAlterTableStmt(SNode* pNode) {
  SAlterTableStmt* pStmt = (SAlterTableStmt*)pNode;
  nodesDestroyNode((SNode*)pStmt->pOptions);
  nodesDestroyNode((SNode*)pStmt->pVal);
}

void destoryCreateUserStmt(SNode* pNode) {
  SCreateUserStmt* pStmt = (SCreateUserStmt*)pNode;
  taosMemoryFree(pStmt->pIpRanges);
  nodesDestroyList(pStmt->pNodeListIpRanges);
}

void destoryAlterUserStmt(SNode* pNode) {
  SAlterUserStmt* pStmt = (SAlterUserStmt*)pNode;
  taosMemoryFree(pStmt->pIpRanges);
  nodesDestroyList(pStmt->pNodeListIpRanges);
}

void destoryCreateIndexStmt(SNode* pNode) {
  SCreateIndexStmt* pStmt = (SCreateIndexStmt*)pNode;
  nodesDestroyNode((SNode*)pStmt->pOptions);
  nodesDestroyList(pStmt->pCols);
  if (pStmt->pReq) {
    tFreeSMCreateSmaReq(pStmt->pReq);
    taosMemoryFreeClear(pStmt->pReq);
  }
}

void destoryCreateTopicStmt(SNode* pNode) {
  nodesDestroyNode(((SCreateTopicStmt*)pNode)->pQuery);
  nodesDestroyNode(((SCreateTopicStmt*)pNode)->pWhere);
}

void destoryExplainStmt(SNode* pNode) {
  SExplainStmt* pStmt = (SExplainStmt*)pNode;
  nodesDestroyNode((SNode*)pStmt->pOptions);
  nodesDestroyNode(pStmt->pQuery);
}

void destoryDescribeStmt(SNode* pNode) { 
  taosMemoryFree(((SDescribeStmt*)pNode)->pMeta); 
}

void destoryCompactDatabaseStmt(SNode* pNode) {
  SCompactDatabaseStmt* pStmt = (SCompactDatabaseStmt*)pNode;
  nodesDestroyNode(pStmt->pStart);
  nodesDestroyNode(pStmt->pEnd);
}

void destoryCreateStreamStmt(SNode* pNode) {
  SCreateStreamStmt* pStmt = (SCreateStreamStmt*)pNode;
  nodesDestroyNode((SNode*)pStmt->pOptions);
  nodesDestroyNode(pStmt->pQuery);
  nodesDestroyList(pStmt->pTags);
  nodesDestroyNode(pStmt->pSubtable);
  tFreeSCMCreateStreamReq(pStmt->pReq);
  taosMemoryFreeClear(pStmt->pReq);
}

void destoryRedistributeVgroupStmt(SNode* pNode) { 
  nodesDestroyList(((SRedistributeVgroupStmt*)pNode)->pDnodes);
}

void destoryGrantStmt(SNode* pNode) { 
  nodesDestroyNode(((SGrantStmt*)pNode)->pTagCond); 
}

void destoryRevokeStmt(SNode* pNode) { 
  nodesDestroyNode(((SRevokeStmt*)pNode)->pTagCond); 
}

void destoryShowStmt(SNode* pNode) {
  SShowStmt* pStmt = (SShowStmt*)pNode;
  nodesDestroyNode(pStmt->pDbName);
  nodesDestroyNode(pStmt->pTbName);
}

void destoryShowTableTagsStmt(SNode* pNode) {
  SShowTableTagsStmt* pStmt = (SShowTableTagsStmt*)pNode;
  nodesDestroyNode(pStmt->pDbName);
  nodesDestroyNode(pStmt->pTbName);
  nodesDestroyList(pStmt->pTags);
}

void destoryShowDnodeVariablesStmt(SNode* pNode) {
  nodesDestroyNode(((SShowDnodeVariablesStmt*)pNode)->pDnodeId);
  nodesDestroyNode(((SShowDnodeVariablesStmt*)pNode)->pLikePattern);
}

void destoryShowCreateDatabaseStmt(SNode* pNode) { 
  taosMemoryFreeClear(((SShowCreateDatabaseStmt*)pNode)->pCfg); 
}

void destoryShowCreateTableStmt(SNode* pNode) {
  STableCfg* pCfg = (STableCfg*)(((SShowCreateTableStmt*)pNode)->pTableCfg);
  taosMemoryFreeClear(((SShowCreateTableStmt*)pNode)->pDbCfg);
  if (NULL == pCfg) {
    return;
  }
  taosArrayDestroy(pCfg->pFuncs);
  taosMemoryFree(pCfg->pComment);
  taosMemoryFree(pCfg->pSchemas);
  taosMemoryFree(pCfg->pTags);
  taosMemoryFree(pCfg);
}

void destoryDeleteStmt(SNode* pNode) {
  SDeleteStmt* pStmt = (SDeleteStmt*)pNode;
  nodesDestroyNode(pStmt->pFromTable);
  nodesDestroyNode(pStmt->pWhere);
  nodesDestroyNode(pStmt->pCountFunc);
  nodesDestroyNode(pStmt->pFirstFunc);
  nodesDestroyNode(pStmt->pLastFunc);
  nodesDestroyNode(pStmt->pTagCond);
}

void destoryInsertStmt(SNode* pNode) {
  SInsertStmt* pStmt = (SInsertStmt*)pNode;
  nodesDestroyNode(pStmt->pTable);
  nodesDestroyList(pStmt->pCols);
  nodesDestroyNode(pStmt->pQuery);
}

void destoryQueryNode(SNode* pNode) {
  SQuery* pQuery = (SQuery*)pNode;
  nodesDestroyNode(pQuery->pPrevRoot);
  nodesDestroyNode(pQuery->pRoot);
  nodesDestroyNode(pQuery->pPostRoot);
  taosMemoryFreeClear(pQuery->pResSchema);
  if (NULL != pQuery->pCmdMsg) {
    taosMemoryFreeClear(pQuery->pCmdMsg->pMsg);
    taosMemoryFreeClear(pQuery->pCmdMsg);
  }
  taosArrayDestroy(pQuery->pDbList);
  taosArrayDestroy(pQuery->pTableList);
  taosArrayDestroy(pQuery->pTargetTableList);
  taosArrayDestroy(pQuery->pPlaceholderValues);
  nodesDestroyNode(pQuery->pPrepareRoot);
}

void destoryCreateViewStmt(SNode* pNode) {
  SCreateViewStmt* pStmt = (SCreateViewStmt*)pNode;
  taosMemoryFree(pStmt->pQuerySql);
  tFreeSCMCreateViewReq(&pStmt->createReq);
  nodesDestroyNode(pStmt->pQuery);
}

void destoryScanLogicNode(SNode* pNode) {
  SScanLogicNode* pLogicNode = (SScanLogicNode*)pNode;
  destroyLogicNode((SLogicNode*)pLogicNode);
  nodesDestroyList(pLogicNode->pScanCols);
  nodesDestroyList(pLogicNode->pScanPseudoCols);
  taosMemoryFreeClear(pLogicNode->pVgroupList);
  nodesDestroyList(pLogicNode->pDynamicScanFuncs);
  nodesDestroyNode(pLogicNode->pTagCond);
  nodesDestroyNode(pLogicNode->pTagIndexCond);
  taosArrayDestroyEx(pLogicNode->pSmaIndexes, destroyDataInSmaIndex);
  nodesDestroyList(pLogicNode->pGroupTags);
  nodesDestroyList(pLogicNode->pTags);
  nodesDestroyNode(pLogicNode->pSubtable);
}

void destoryJoinLogicNode(SNode* pNode) {
  SJoinLogicNode* pLogicNode = (SJoinLogicNode*)pNode;
  destroyLogicNode((SLogicNode*)pLogicNode);
  nodesDestroyNode(pLogicNode->pPrimKeyEqCond);
  nodesDestroyNode(pLogicNode->pOtherOnCond);
  nodesDestroyNode(pLogicNode->pColEqCond);
}

void destoryAggLogicNode(SNode* pNode) {
  SAggLogicNode* pLogicNode = (SAggLogicNode*)pNode;
  destroyLogicNode((SLogicNode*)pLogicNode);
  nodesDestroyList(pLogicNode->pAggFuncs);
  nodesDestroyList(pLogicNode->pGroupKeys);
}

void destoryProjectLogicNode(SNode* pNode) {
  SProjectLogicNode* pLogicNode = (SProjectLogicNode*)pNode;
  destroyLogicNode((SLogicNode*)pLogicNode);
  nodesDestroyList(pLogicNode->pProjections);
}

void destoryVnodeModifyLogicNode(SNode* pNode) {
  SVnodeModifyLogicNode* pLogicNode = (SVnodeModifyLogicNode*)pNode;
  destroyLogicNode((SLogicNode*)pLogicNode);
  destroyVgDataBlockArray(pLogicNode->pDataBlocks);
  // pVgDataBlocks is weak reference
  nodesDestroyNode(pLogicNode->pAffectedRows);
  nodesDestroyNode(pLogicNode->pStartTs);
  nodesDestroyNode(pLogicNode->pEndTs);
  taosMemoryFreeClear(pLogicNode->pVgroupList);
  nodesDestroyList(pLogicNode->pInsertCols);
}

void destoryExchangeLogicNode(SNode* pNode) { 
  destroyLogicNode((SLogicNode*)pNode); 
}

void destoryMergeLogicNode(SNode* pNode) {
  SMergeLogicNode* pLogicNode = (SMergeLogicNode*)pNode;
  destroyLogicNode((SLogicNode*)pLogicNode);
  nodesDestroyList(pLogicNode->pMergeKeys);
  nodesDestroyList(pLogicNode->pInputs);
}

void destoryWindowLogicNode(SNode* pNode) {
  SWindowLogicNode* pLogicNode = (SWindowLogicNode*)pNode;
  destroyLogicNode((SLogicNode*)pLogicNode);
  nodesDestroyList(pLogicNode->pFuncs);
  nodesDestroyNode(pLogicNode->pTspk);
  nodesDestroyNode(pLogicNode->pTsEnd);
  nodesDestroyNode(pLogicNode->pStateExpr);
  nodesDestroyNode(pLogicNode->pStartCond);
  nodesDestroyNode(pLogicNode->pEndCond);
}

void destoryFillLogicNode(SNode* pNode) {
  SFillLogicNode* pLogicNode = (SFillLogicNode*)pNode;
  destroyLogicNode((SLogicNode*)pLogicNode);
  nodesDestroyNode(pLogicNode->pWStartTs);
  nodesDestroyNode(pLogicNode->pValues);
  nodesDestroyList(pLogicNode->pFillExprs);
  nodesDestroyList(pLogicNode->pNotFillExprs);
}

void destorySortLogicNode(SNode* pNode) {
  SSortLogicNode* pLogicNode = (SSortLogicNode*)pNode;
  destroyLogicNode((SLogicNode*)pLogicNode);
  nodesDestroyList(pLogicNode->pSortKeys);
}

void destoryPartitionLogicNode(SNode* pNode) {
  SPartitionLogicNode* pLogicNode = (SPartitionLogicNode*)pNode;
  destroyLogicNode((SLogicNode*)pLogicNode);
  nodesDestroyList(pLogicNode->pPartitionKeys);
  nodesDestroyList(pLogicNode->pTags);
  nodesDestroyNode(pLogicNode->pSubtable);
  nodesDestroyList(pLogicNode->pAggFuncs);
}

void destoryIndefRowsFuncLogicNode(SNode* pNode) {
  SIndefRowsFuncLogicNode* pLogicNode = (SIndefRowsFuncLogicNode*)pNode;
  destroyLogicNode((SLogicNode*)pLogicNode);
  nodesDestroyList(pLogicNode->pFuncs);
}

void destoryInterpFuncLogicNode(SNode* pNode) {
  SInterpFuncLogicNode* pLogicNode = (SInterpFuncLogicNode*)pNode;
  destroyLogicNode((SLogicNode*)pLogicNode);
  nodesDestroyList(pLogicNode->pFuncs);
  nodesDestroyNode(pLogicNode->pFillValues);
  nodesDestroyNode(pLogicNode->pTimeSeries);
}

void destoryGroupCacheLogicNode(SNode* pNode) {
  SGroupCacheLogicNode* pLogicNode = (SGroupCacheLogicNode*)pNode;
  destroyLogicNode((SLogicNode*)pLogicNode);
  nodesDestroyList(pLogicNode->pGroupCols);
}

void destoryDynQueryCtrlLogicNode(SNode* pNode) {
  destroyLogicNode((SLogicNode*)pNode);
}

void destoryLogicSubplan(SNode* pNode) {
  SLogicSubplan* pSubplan = (SLogicSubplan*)pNode;
  nodesDestroyList(pSubplan->pChildren);
  nodesDestroyNode((SNode*)pSubplan->pNode);
  nodesClearList(pSubplan->pParents);
  taosMemoryFreeClear(pSubplan->pVgroupList);
}

void destoryQueryLogicPlan(SNode* pNode) {
  nodesDestroyList(((SQueryLogicPlan*)pNode)->pTopSubplans); 
}

void destroyScanPhysiNode(SNode* pInput) { 
  SScanPhysiNode* pNode = (SScanPhysiNode*)pInput;
  destroyPhysiNode(pInput);
  nodesDestroyList(pNode->pScanCols);
  nodesDestroyList(pNode->pScanPseudoCols);
}

void destoryLastRowScanPhysiNode(SNode* pNode) {
  SLastRowScanPhysiNode* pPhyNode = (SLastRowScanPhysiNode*)pNode;
  destroyScanPhysiNode(pNode);
  nodesDestroyList(pPhyNode->pGroupTags);
  nodesDestroyList(pPhyNode->pTargets);
}

void destoryTableScanPhysiNode(SNode* pNode) {
  STableScanPhysiNode* pPhyNode = (STableScanPhysiNode*)pNode;
  destroyScanPhysiNode(pNode);
  nodesDestroyList(pPhyNode->pDynamicScanFuncs);
  nodesDestroyList(pPhyNode->pGroupTags);
  nodesDestroyList(pPhyNode->pTags);
  nodesDestroyNode(pPhyNode->pSubtable);
}

void destoryProjectPhysiNode(SNode* pNode) {
  SProjectPhysiNode* pPhyNode = (SProjectPhysiNode*)pNode;
  destroyPhysiNode(pNode);
  nodesDestroyList(pPhyNode->pProjections);
}

void destorySortMergeJoinPhysiNode(SNode* pNode) {
  SSortMergeJoinPhysiNode* pPhyNode = (SSortMergeJoinPhysiNode*)pNode;
  destroyPhysiNode(pNode);
  nodesDestroyNode(pPhyNode->pPrimKeyCond);
  nodesDestroyNode(pPhyNode->pOtherOnCond);
  nodesDestroyList(pPhyNode->pTargets);
  nodesDestroyNode(pPhyNode->pColEqCond);
}

void destoryHashJoinPhysiNode(SNode* pNode) {
  SHashJoinPhysiNode* pPhyNode = (SHashJoinPhysiNode*)pNode;
  destroyPhysiNode(pNode);
  nodesDestroyList(pPhyNode->pOnLeft);
  nodesDestroyList(pPhyNode->pOnRight);
  nodesDestroyNode(pPhyNode->pFilterConditions);
  nodesDestroyList(pPhyNode->pTargets);

  nodesDestroyNode(pPhyNode->pPrimKeyCond);
  nodesDestroyNode(pPhyNode->pColEqCond);
  nodesDestroyNode(pPhyNode->pTagEqCond);
}

void destoryAggPhysiNode(SNode* pNode) {
  SAggPhysiNode* pPhyNode = (SAggPhysiNode*)pNode;
  destroyPhysiNode(pNode);
  nodesDestroyList(pPhyNode->pExprs);
  nodesDestroyList(pPhyNode->pAggFuncs);
  nodesDestroyList(pPhyNode->pGroupKeys);
}

void destoryExchangePhysiNode(SNode* pNode) {
  SExchangePhysiNode* pPhyNode = (SExchangePhysiNode*)pNode;
  destroyPhysiNode(pNode);
  nodesDestroyList(pPhyNode->pSrcEndPoints);
}

void destoryMergePhysiNode(SNode* pNode) {
  SMergePhysiNode* pPhyNode = (SMergePhysiNode*)pNode;
  destroyPhysiNode(pNode);
  nodesDestroyList(pPhyNode->pMergeKeys);
  nodesDestroyList(pPhyNode->pTargets);
}

void destorySortPhysiNode(SNode* pNode) {
  SSortPhysiNode* pPhyNode = (SSortPhysiNode*)pNode;
  destroyPhysiNode(pNode);
  nodesDestroyList(pPhyNode->pExprs);
  nodesDestroyList(pPhyNode->pSortKeys);
  nodesDestroyList(pPhyNode->pTargets);
}

void destroyWindowPhysiNode(SNode* pInput) {
  SWindowPhysiNode* pNode = (SWindowPhysiNode*)pInput;
  destroyPhysiNode(pInput);
  nodesDestroyList(pNode->pExprs);
  nodesDestroyList(pNode->pFuncs);
  nodesDestroyNode(pNode->pTspk);
  nodesDestroyNode(pNode->pTsEnd);
}

void destoryFillPhysiNode(SNode* pNode) {
  SFillPhysiNode* pPhyNode = (SFillPhysiNode*)pNode;
  destroyPhysiNode(pNode);
  nodesDestroyList(pPhyNode->pFillExprs);
  nodesDestroyList(pPhyNode->pNotFillExprs);
  nodesDestroyNode(pPhyNode->pWStartTs);
  nodesDestroyNode(pPhyNode->pValues);
}

void destoryStateWindowPhysiNode(SNode* pNode) {
  SStateWinodwPhysiNode* pPhyNode = (SStateWinodwPhysiNode*)pNode;
  destroyWindowPhysiNode(pNode);
  nodesDestroyNode(pPhyNode->pStateKey);
}

void destoryEventWindowPhysiNode(SNode* pNode) {
  SEventWinodwPhysiNode* pPhyNode = (SEventWinodwPhysiNode*)pNode;
  destroyWindowPhysiNode(pNode);
  nodesDestroyNode(pPhyNode->pStartCond);
  nodesDestroyNode(pPhyNode->pEndCond);
}

void destroyPartitionPhysiNode(SNode* pNode) {
  SPartitionPhysiNode* pPartitionNode = (SPartitionPhysiNode*)pNode;
  destroyPhysiNode(pNode);
  nodesDestroyList(pPartitionNode->pExprs);
  nodesDestroyList(pPartitionNode->pPartitionKeys);
  nodesDestroyList(pPartitionNode->pTargets);
}

void destoryStreamPartitionPhysiNode(SNode* pNode) {
  SStreamPartitionPhysiNode* pPhyNode = (SStreamPartitionPhysiNode*)pNode;
  destroyPartitionPhysiNode(pNode);
  nodesDestroyList(pPhyNode->pTags);
  nodesDestroyNode(pPhyNode->pSubtable);
}

void destoryIndefRowsFuncPhysiNode(SNode* pNode) {
  SIndefRowsFuncPhysiNode* pPhyNode = (SIndefRowsFuncPhysiNode*)pNode;
  destroyPhysiNode(pNode);
  nodesDestroyList(pPhyNode->pExprs);
  nodesDestroyList(pPhyNode->pFuncs);
}

void destoryInterpFuncPhysiNode(SNode* pNode) {
  SInterpFuncPhysiNode* pPhyNode = (SInterpFuncPhysiNode*)pNode;
  destroyPhysiNode(pNode);
  nodesDestroyList(pPhyNode->pExprs);
  nodesDestroyList(pPhyNode->pFuncs);
  nodesDestroyNode(pPhyNode->pFillValues);
  nodesDestroyNode(pPhyNode->pTimeSeries);
}

void destroyDataSinkNode(SNode* pNode) {
  SDataSinkNode* pDataNode = (SDataSinkNode*)pNode;
  nodesDestroyNode((SNode*)pDataNode->pInputDataBlockDesc);
}

void destoryDataInserterNode(SNode* pNode) {
  SDataInserterNode* pSink = (SDataInserterNode*)pNode;
  destroyDataSinkNode(pNode);
  taosMemoryFreeClear(pSink->pData);
}

void destoryQueryInserterNode(SNode* pNode) {
  SQueryInserterNode* pSink = (SQueryInserterNode*)pNode;
  destroyDataSinkNode(pNode);
  nodesDestroyList(pSink->pCols);
}

void destoryDataDeleterNode(SNode* pNode) {
  SDataDeleterNode* pSink = (SDataDeleterNode*)pNode;
  destroyDataSinkNode(pNode);
  nodesDestroyNode(pSink->pAffectedRows);
  nodesDestroyNode(pSink->pStartTs);
  nodesDestroyNode(pSink->pEndTs);
}

void destoryGroupCachePhysiNode(SNode* pNode) {
  SGroupCachePhysiNode* pPhyNode = (SGroupCachePhysiNode*)pNode;
  destroyPhysiNode(pNode);
  nodesDestroyList(pPhyNode->pGroupCols);
}

void destoryDynQueryCtrlPhysiNode(SNode* pNode) {
  destroyPhysiNode(pNode);
}

void destorySubplanNode(SNode* pNode) {
  SSubplan* pSubplan = (SSubplan*)pNode;
  nodesClearList(pSubplan->pChildren);
  nodesDestroyNode((SNode*)pSubplan->pNode);
  nodesDestroyNode((SNode*)pSubplan->pDataSink);
  nodesDestroyNode((SNode*)pSubplan->pTagCond);
  nodesDestroyNode((SNode*)pSubplan->pTagIndexCond);
  nodesClearList(pSubplan->pParents);
}

void destoryPlanNode(SNode* pNode) {
  nodesDestroyList(((SQueryPlan*)pNode)->pSubplans); 
}

void nodesDestroyNode(SNode* pNode) {
  if (NULL == pNode) {
    return;
  }

  int32_t index = nodeType(pNode);
  if (!funcArrayCheck(index)) {
    return;
  }
  if (funcNodes[index].destoryFunc) {
    funcNodes[index].destoryFunc(pNode);
    nodesFree(pNode);
    return;
  }
  nodesError("nodesDestroyNode unknown node type = %d", nodeType(pNode));
  nodesFree(pNode);
  return;
}

// clang-format off
static void doInitNodeFuncArray() {
  setFunc("Column",
      QUERY_NODE_COLUMN,
      sizeof(SColumnNode),
      columnNodeToJson, 
      jsonToColumnNode, 
      destroyColumnNode
    );
  setFunc("Value",
     QUERY_NODE_VALUE,
     sizeof(SValueNode),
     valueNodeToJson,
     jsonToValueNode,
     destroyValueNode
   );
  setFunc("Operator",
     QUERY_NODE_OPERATOR,
     sizeof(SOperatorNode),
     operatorNodeToJson,
     jsonToOperatorNode,
     destroyOperatorNode
   );
  setFunc("LogicCondition",
     QUERY_NODE_LOGIC_CONDITION,
     sizeof(SLogicConditionNode),
     logicConditionNodeToJson,
     jsonToLogicConditionNode,
     destoryLogicConditionNode
   );
  setFunc("Function",
     QUERY_NODE_FUNCTION,
     sizeof(SFunctionNode),
     functionNodeToJson,
     jsonToFunctionNode,
     destoryFunctionNode
   );
  setFunc("RealTable",
     QUERY_NODE_REAL_TABLE,
     sizeof(SRealTableNode),
     realTableNodeToJson,
     jsonToRealTableNode,
     destoryRealTableNode
   );
  setFunc("TempTable",
     QUERY_NODE_TEMP_TABLE,
     sizeof(STempTableNode),
     tempTableNodeToJson,
     jsonToTempTableNode,
     destoryTempTableNode
   );
  setFunc("JoinTable",
     QUERY_NODE_JOIN_TABLE,
     sizeof(SJoinTableNode),
     joinTableNodeToJson,
     jsonToJoinTableNode,
     destoryJoinTableNode
   );
  setFunc("GroupingSet",
     QUERY_NODE_GROUPING_SET,
     sizeof(SGroupingSetNode),
     groupingSetNodeToJson,
     jsonToGroupingSetNode,
     destoryGroupingSetNode
   );
  setFunc("OrderByExpr",
     QUERY_NODE_ORDER_BY_EXPR,
     sizeof(SOrderByExprNode),
     orderByExprNodeToJson,
     jsonToOrderByExprNode,
     destoryOrderByExprNode
   );
  setFunc("Limit",
     QUERY_NODE_LIMIT,
     sizeof(SLimitNode),
     limitNodeToJson,
     jsonToLimitNode,
     destoryXNode
   );
  setFunc("StateWindow",
     QUERY_NODE_STATE_WINDOW,
     sizeof(SStateWindowNode),
     stateWindowNodeToJson,
     jsonToStateWindowNode,
     destoryStateWindowNode
   );
  setFunc("SessionWinow",
     QUERY_NODE_SESSION_WINDOW,
     sizeof(SSessionWindowNode),
     sessionWindowNodeToJson,
     jsonToSessionWindowNode,
     destorySessionWindowNode
   );
  setFunc("IntervalWindow",
     QUERY_NODE_INTERVAL_WINDOW,
     sizeof(SIntervalWindowNode),
     intervalWindowNodeToJson,
     jsonToIntervalWindowNode,
     destoryIntervalWindowNode
   );
  setFunc("NodeList",
     QUERY_NODE_NODE_LIST,
     sizeof(SNodeListNode),
     nodeListNodeToJson,
     jsonToNodeListNode,
     destoryNodeListNode
   );
  setFunc("Fill",
     QUERY_NODE_FILL,
     sizeof(SFillNode),
     fillNodeToJson,
     jsonToFillNode,
     destoryFillNode
   );
  setFunc("RawExpr",
     QUERY_NODE_RAW_EXPR,
     sizeof(SRawExprNode),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryRawExprNode
   );
  setFunc("Target",
     QUERY_NODE_TARGET,
     sizeof(STargetNode),
     targetNodeToJson,
     jsonToTargetNode,
     destoryTargetNode
   );
  setFunc("DataBlockDesc",
     QUERY_NODE_DATABLOCK_DESC,
     sizeof(SDataBlockDescNode),
     dataBlockDescNodeToJson,
     jsonToDataBlockDescNode,
     destoryDataBlockDescNode
   );
  setFunc("SlotDesc",
     QUERY_NODE_SLOT_DESC,
     sizeof(SSlotDescNode),
     slotDescNodeToJson,
     jsonToSlotDescNode,
     destoryXNode
   );
  setFunc("ColumnDef",
     QUERY_NODE_COLUMN_DEF,
     sizeof(SColumnDefNode),
     columnDefNodeToJson,
     jsonToColumnDefNode,
     destoryXNode
   );
  setFunc("DownstreamSource",
     QUERY_NODE_DOWNSTREAM_SOURCE,
     sizeof(SDownstreamSourceNode),
     downstreamSourceNodeToJson,
     jsonToDownstreamSourceNode,
     destoryXNode
   );
  setFunc("DatabaseOptions",
     QUERY_NODE_DATABASE_OPTIONS,
     sizeof(SDatabaseOptions),
     databaseOptionsToJson,
     jsonToDatabaseOptions,
     destoryDatabaseOptions
   );
  setFunc("TableOptions",
     QUERY_NODE_TABLE_OPTIONS,
     sizeof(STableOptions),
     tableOptionsToJson,
     jsonToTableOptions,
     destoryTableOptions
   );
  setFunc("IndexOptions",
     QUERY_NODE_INDEX_OPTIONS,
     sizeof(SIndexOptions),
     indexOptionsToJson,
     jsonToIndexOptions,
     destoryIndexOptions
   );
  setFunc("ExplainOptions",
     QUERY_NODE_EXPLAIN_OPTIONS,
     sizeof(SExplainOptions),
     explainOptionsToJson,
     jsonToExplainOptions,
     destoryXNode
   );
  setFunc("StreamOptions",
     QUERY_NODE_STREAM_OPTIONS,
     sizeof(SStreamOptions),
     streamOptionsToJson,
     jsonToStreamOptions,
     destoryStreamOptions
   );
  setFunc("LeftValue",
     QUERY_NODE_LEFT_VALUE,
     sizeof(SLeftValueNode),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryXNode
   );
  setFunc("ColumnRef",
     QUERY_NODE_COLUMN_REF,
     sizeof(SColumnDefNode),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryXNode
   );
  setFunc("WhenThen",
     QUERY_NODE_WHEN_THEN,
     sizeof(SWhenThenNode),
     whenThenNodeToJson,
     jsonToWhenThenNode,
     destoryWhenThenNode
   );
  setFunc("CaseWhen",
     QUERY_NODE_CASE_WHEN,
     sizeof(SCaseWhenNode),
     caseWhenNodeToJson,
     jsonToCaseWhenNode,
     destoryCaseWhenNode
   );
  setFunc("EventWindow",
     QUERY_NODE_EVENT_WINDOW,
     sizeof(SEventWindowNode),
     eventWindowNodeToJson,
     jsonToEventWindowNode,
     destoryEventWindowNode
   );
  setFunc("HintNode",
     QUERY_NODE_HINT,
     sizeof(SHintNode),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryHintNode
   );
  setFunc("ViewNode",
     QUERY_NODE_VIEW,
     sizeof(SViewNode),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryViewNode
   );
  setFunc("SetOperator",
     QUERY_NODE_SET_OPERATOR,
     sizeof(SSetOperator),
     setOperatorToJson,
     jsonToSetOperator,
     destorySetOperator
   );
  setFunc("SelectStmt",
     QUERY_NODE_SELECT_STMT,
     sizeof(SSelectStmt),
     selectStmtToJson,
     jsonToSelectStmt,
     destorySelectStmt
   );
  setFunc("VnodeModifyStmt",
     QUERY_NODE_VNODE_MODIFY_STMT,
     sizeof(SVnodeModifyOpStmt),
     vnodeModifyStmtToJson,
     jsonToVnodeModifyStmt,
     destoryVnodeModifyOpStmt
   );
  setFunc("CreateDatabaseStmt",
     QUERY_NODE_CREATE_DATABASE_STMT,
     sizeof(SCreateDatabaseStmt),
     createDatabaseStmtToJson,
     jsonToCreateDatabaseStmt,
     destoryCreateDatabaseStmt
   );
  setFunc("DropDatabaseStmt",
     QUERY_NODE_DROP_DATABASE_STMT,
     sizeof(SDropDatabaseStmt),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryXNode
   );
  setFunc("AlterDatabaseStmt",
     QUERY_NODE_ALTER_DATABASE_STMT,
     sizeof(SAlterDatabaseStmt),
     alterDatabaseStmtToJson,
     jsonToAlterDatabaseStmt,
     destoryAlterDatabaseStmt
   );
  setFunc("FlushDatabaseStmt",
     QUERY_NODE_FLUSH_DATABASE_STMT,
     sizeof(SFlushDatabaseStmt),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryXNode
   );
  setFunc("TrimDatabaseStmt",
     QUERY_NODE_TRIM_DATABASE_STMT,
     sizeof(STrimDatabaseStmt),
     trimDatabaseStmtToJson,
     jsonToTrimDatabaseStmt,
     destoryXNode
   );
  setFunc("CreateTableStmt",
     QUERY_NODE_CREATE_TABLE_STMT,
     sizeof(SCreateTableStmt),
     createTableStmtToJson,
     jsonToCreateTableStmt,
     destoryCreateTableStmt
   );
  setFunc("CreateSubtableClause",
     QUERY_NODE_CREATE_SUBTABLE_CLAUSE,
     sizeof(SCreateSubTableClause),
     createSubTableClauseToJson,
     jsonToCreateSubTableClause,
     destoryCreateSubTableClause
   );
  setFunc("CreateMultiTableStmt",
     QUERY_NODE_CREATE_MULTI_TABLES_STMT,
     sizeof(SCreateMultiTablesStmt),
     createMultiTablesStmtToJson,
     jsonToCreateMultiTablesStmt,
     destoryCreateMultiTablesStmt
   );
  setFunc("DropTableClause",
     QUERY_NODE_DROP_TABLE_CLAUSE,
     sizeof(SDropTableClause),
     dropTableClauseToJson,
     jsonToDropTableClause,
     destoryXNode
   );
  setFunc("DropTableStmt",
     QUERY_NODE_DROP_TABLE_STMT,
     sizeof(SDropTableStmt),
     dropTableStmtToJson,
     jsonToDropTableStmt,
     destoryDropTableStmt
   );
  setFunc("DropSuperTableStmt",
     QUERY_NODE_DROP_SUPER_TABLE_STMT,
     sizeof(SDropSuperTableStmt),
     dropStableStmtToJson,
     jsonToDropStableStmt,
     destoryXNode
   );
  setFunc("AlterTableStmt",
     QUERY_NODE_ALTER_TABLE_STMT,
     sizeof(SAlterTableStmt),
     alterTableStmtToJson,
     jsonToAlterTableStmt,
     destoryAlterTableStmt
   );
  setFunc("AlterSuperTableStmt",
     QUERY_NODE_ALTER_SUPER_TABLE_STMT,
     sizeof(SAlterTableStmt),
     alterStableStmtToJson,
     jsonToAlterStableStmt,
     destoryAlterTableStmt
   );
  setFunc("CreateUserStmt",
     QUERY_NODE_CREATE_USER_STMT,
     sizeof(SCreateUserStmt),
     createUserStmtToJson,
     jsonToCreateUserStmt,
     destoryCreateUserStmt
   );
  setFunc("AlterUserStmt",
     QUERY_NODE_ALTER_USER_STMT,
     sizeof(SAlterUserStmt),
     alterUserStmtToJson,
     jsonToAlterUserStmt,
     destoryAlterUserStmt
   );
  setFunc("DropUserStmt",
     QUERY_NODE_DROP_USER_STMT,
     sizeof(SDropUserStmt),
     dropUserStmtToJson,
     jsonToDropUserStmt,
     destoryXNode
   );
  setFunc("UseDatabaseStmt",
     QUERY_NODE_USE_DATABASE_STMT,
     sizeof(SUseDatabaseStmt),
     useDatabaseStmtToJson,
     jsonToUseDatabaseStmt,
     destoryXNode
   );
  setFunc("CreateDnodeStmt",
     QUERY_NODE_CREATE_DNODE_STMT,
     sizeof(SCreateDnodeStmt),
     createDnodeStmtToJson,
     jsonToCreateDnodeStmt,
     destoryXNode
   );
  setFunc("DropDnodeStmt",
     QUERY_NODE_DROP_DNODE_STMT,
     sizeof(SDropDnodeStmt),
     dropDnodeStmtToJson,
     jsonToDropDnodeStmt,
     destoryXNode
   );
  setFunc("AlterDnodeStmt",
     QUERY_NODE_ALTER_DNODE_STMT,
     sizeof(SAlterDnodeStmt),
     alterDnodeStmtToJson,
     jsonToAlterDnodeStmt,
     destoryXNode
   );
  setFunc("CreateIndexStmt",
     QUERY_NODE_CREATE_INDEX_STMT,
     sizeof(SCreateIndexStmt),
     createIndexStmtToJson,
     jsonToCreateIndexStmt,
     destoryCreateIndexStmt
   );
  setFunc("DropIndexStmt",
     QUERY_NODE_DROP_INDEX_STMT,
     sizeof(SDropIndexStmt),
     dropIndexStmtToJson,
     jsonToDropIndexStmt,
     destoryXNode
   );
  setFunc("CreateQnodeStmt",
     QUERY_NODE_CREATE_QNODE_STMT,
     sizeof(SCreateComponentNodeStmt),
     createQnodeStmtToJson,
     jsonToCreateQnodeStmt,
     destoryXNode
   );
  setFunc("DropQnodeStmt",
     QUERY_NODE_DROP_QNODE_STMT,
     sizeof(SDropComponentNodeStmt),
     dropQnodeStmtToJson,
     jsonToDropQnodeStmt,
     destoryXNode
   );
  setFunc("CreateBnodeStmt",
     QUERY_NODE_CREATE_BNODE_STMT,
     sizeof(SCreateComponentNodeStmt),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryXNode
   );
  setFunc("DropBnodeStmt",
     QUERY_NODE_DROP_BNODE_STMT,
     sizeof(SDropComponentNodeStmt),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryXNode
   );
  setFunc("CreateSnodeStmt",
     QUERY_NODE_CREATE_SNODE_STMT,
     sizeof(SCreateComponentNodeStmt),
     createSnodeStmtToJson,
     jsonToCreateSnodeStmt,
     destoryXNode
   );
  setFunc("DropSnodeStmt",
     QUERY_NODE_DROP_SNODE_STMT,
     sizeof(SDropComponentNodeStmt),
     dropSnodeStmtToJson,
     jsonToDropSnodeStmt,
     destoryXNode
   );
  setFunc("CreateMnodeStmt",
     QUERY_NODE_CREATE_MNODE_STMT,
     sizeof(SCreateComponentNodeStmt),
     createMnodeStmtToJson,
     jsonToCreateMnodeStmt,
     destoryXNode
   );
  setFunc("DropMnodeStmt",
     QUERY_NODE_DROP_MNODE_STMT,
     sizeof(SDropComponentNodeStmt),
     dropMnodeStmtToJson,
     jsonToDropMnodeStmt,
     destoryXNode
   );
  setFunc("CreateTopicStmt",
     QUERY_NODE_CREATE_TOPIC_STMT,
     sizeof(SCreateTopicStmt),
     createTopicStmtToJson,
     jsonToCreateTopicStmt,
     destoryCreateTopicStmt
   );
  setFunc("DropTopicStmt",
     QUERY_NODE_DROP_TOPIC_STMT,
     sizeof(SDropTopicStmt),
     dropTopicStmtToJson,
     jsonToDropTopicStmt,
     destoryXNode
   );
  setFunc("DropConsumerGroupStmt",
     QUERY_NODE_DROP_CGROUP_STMT,
     sizeof(SDropCGroupStmt),
     dropConsumerGroupStmtToJson,
     jsonToDropConsumerGroupStmt,
     destoryXNode
   );
  setFunc("AlterLocalStmt",
     QUERY_NODE_ALTER_LOCAL_STMT,
     sizeof(SAlterLocalStmt),
     alterLocalStmtToJson,
     jsonToAlterLocalStmt,
     destoryXNode
   );
  setFunc("ExplainStmt",
     QUERY_NODE_EXPLAIN_STMT,
     sizeof(SExplainStmt),
     explainStmtToJson,
     jsonToExplainStmt,
     destoryExplainStmt
   );
  setFunc("DescribeStmt",
     QUERY_NODE_DESCRIBE_STMT,
     sizeof(SDescribeStmt),
     describeStmtToJson,
     jsonToDescribeStmt,
     destoryDescribeStmt
   );
  setFunc("QueryCacheStmt",
     QUERY_NODE_RESET_QUERY_CACHE_STMT,
     sizeof(SNode),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryXNode
   );
  setFunc("CompactDatabaseStmt",
     QUERY_NODE_COMPACT_DATABASE_STMT,
     sizeof(SCompactDatabaseStmt),
     compactDatabaseStmtToJson,
     jsonToCompactDatabaseStmt,
     destoryCompactDatabaseStmt
   );
  setFunc("CreateFunctionStmt",
     QUERY_NODE_CREATE_FUNCTION_STMT,
     sizeof(SCreateFunctionStmt),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryXNode
   );
  setFunc("DropFunctionStmt",
     QUERY_NODE_DROP_FUNCTION_STMT,
     sizeof(SDropFunctionStmt),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryXNode
   );
  setFunc("CreateStreamStmt",
     QUERY_NODE_CREATE_STREAM_STMT,
     sizeof(SCreateStreamStmt),
     createStreamStmtToJson,
     jsonToCreateStreamStmt,
     destoryCreateStreamStmt
   );
  setFunc("DropStreamStmt",
     QUERY_NODE_DROP_STREAM_STMT,
     sizeof(SDropStreamStmt),
     dropStreamStmtToJson,
     jsonToDropStreamStmt,
     destoryXNode
   );
  setFunc("PauseStreamStmt",
     QUERY_NODE_PAUSE_STREAM_STMT,
     sizeof(SPauseStreamStmt),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryXNode
   );
  setFunc("ResumeStreamStmt",
     QUERY_NODE_RESUME_STREAM_STMT,
     sizeof(SResumeStreamStmt),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryXNode
   );
  setFunc("BalanceVgroupStmt",
     QUERY_NODE_BALANCE_VGROUP_STMT,
     sizeof(SBalanceVgroupStmt),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryXNode
   );
  setFunc("BalanceVgroupLeaderStmt",
     QUERY_NODE_BALANCE_VGROUP_LEADER_STMT,
     sizeof(SBalanceVgroupLeaderStmt),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryXNode
   );
  setFunc("MergeVgroupStmt",
     QUERY_NODE_MERGE_VGROUP_STMT,
     sizeof(SMergeVgroupStmt),
     mergeVgroupStmtToJson,
     jsonToMergeVgroupStmt,
     destoryXNode
   );
  setFunc("RedistributeVgroupStmt",
     QUERY_NODE_REDISTRIBUTE_VGROUP_STMT,
     sizeof(SRedistributeVgroupStmt),
     redistributeVgroupStmtToJson,
     jsonToRedistributeVgroupStmt,
     destoryRedistributeVgroupStmt
   );
  setFunc("SplitVgroupStmt",
     QUERY_NODE_SPLIT_VGROUP_STMT,
     sizeof(SSplitVgroupStmt),
     splitVgroupStmtToJson,
     jsonToSplitVgroupStmt,
     destoryXNode
   );
  setFunc("SyncDBStmt",
     QUERY_NODE_SYNCDB_STMT,
     0,
     emptyNodeToJson,
     emptyJsonToNode,
     destoryXNode
   );
  setFunc("GrantStmt",
     QUERY_NODE_GRANT_STMT,
     sizeof(SGrantStmt),
     grantStmtToJson,
     jsonToGrantStmt,
     destoryGrantStmt
   );
  setFunc("RevokeStmt",
     QUERY_NODE_REVOKE_STMT,
     sizeof(SRevokeStmt),
     revokeStmtToJson,
     jsonToRevokeStmt,
     destoryRevokeStmt
   );
  setFunc("ShowDnodesStmt",
     QUERY_NODE_SHOW_DNODES_STMT,
     sizeof(SShowStmt),
     showDnodesStmtToJson,
     jsonToShowDnodesStmt,
     destoryShowStmt
   );
  setFunc("ShowMnodesStmt",
     QUERY_NODE_SHOW_MNODES_STMT,
     sizeof(SShowStmt),
     showMnodesStmtToJson,
     jsonToShowMnodesStmt,
     destoryShowStmt
   );
  setFunc("ShowModulesStmt",
     QUERY_NODE_SHOW_MODULES_STMT,
     sizeof(SShowStmt),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryShowStmt
   );
  setFunc("ShowQnodesStmt",
     QUERY_NODE_SHOW_QNODES_STMT,
     sizeof(SShowStmt),
     showQnodesStmtToJson,
     jsonToShowQnodesStmt,
     destoryShowStmt
   );
  setFunc("ShowSnodesStmt",
     QUERY_NODE_SHOW_SNODES_STMT,
     sizeof(SShowStmt),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryShowStmt
   );
  setFunc("ShowBnodesStmt",
     QUERY_NODE_SHOW_BNODES_STMT,
     sizeof(SShowStmt),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryShowStmt
   );
  setFunc("ShowClusterStmt",
     QUERY_NODE_SHOW_CLUSTER_STMT,
     sizeof(SShowStmt),
     showClusterStmtToJson,
     jsonToShowClusterStmt,
     destoryShowStmt
   );
  setFunc("ShowDatabaseStmt",
     QUERY_NODE_SHOW_DATABASES_STMT,
     sizeof(SShowStmt),
     showDatabasesStmtToJson,
     jsonToShowDatabasesStmt,
     destoryShowStmt
   );
  setFunc("ShowFunctionsStmt",
     QUERY_NODE_SHOW_FUNCTIONS_STMT,
     sizeof(SShowStmt),
     showFunctionsStmtToJson,
     jsonToShowFunctionsStmt,
     destoryShowStmt
   );
  setFunc("ShowIndexesStmt",
     QUERY_NODE_SHOW_INDEXES_STMT,
     sizeof(SShowStmt),
     showIndexesStmtToJson,
     jsonToShowIndexesStmt,
     destoryShowStmt
   );
  setFunc("ShowStablesStmt",
     QUERY_NODE_SHOW_STABLES_STMT,
     sizeof(SShowStmt),
     showStablesStmtToJson,
     jsonToShowStablesStmt,
     destoryShowStmt
   );
  setFunc("ShowStreamsStmt",
     QUERY_NODE_SHOW_STREAMS_STMT,
     sizeof(SShowStmt),
     showStreamsStmtToJson,
     jsonToShowStreamsStmt,
     destoryShowStmt
   );
  setFunc("ShowTablesStmt",
     QUERY_NODE_SHOW_TABLES_STMT,
     sizeof(SShowStmt),
     showTablesStmtToJson,
     jsonToShowTablesStmt,
     destoryShowStmt
   );
  setFunc("ShowTagsStmt",
     QUERY_NODE_SHOW_TAGS_STMT,
     sizeof(SShowStmt),
     showTagsStmtToJson,
     jsonToShowTagsStmt,
     destoryShowStmt
   );
  setFunc("ShowUsersStmt",
     QUERY_NODE_SHOW_USERS_STMT,
     sizeof(SShowStmt),
     showUsersStmtToJson,
     jsonToShowUsersStmt,
     destoryShowStmt
   );
  setFunc("ShowLicencesStmt",
     QUERY_NODE_SHOW_LICENCES_STMT,
     sizeof(SShowStmt),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryShowStmt
   );
  setFunc("ShowVgroupsStmt",
     QUERY_NODE_SHOW_VGROUPS_STMT,
     sizeof(SShowStmt),
     showVgroupsStmtToJson,
     jsonToShowVgroupsStmt,
     destoryShowStmt
   );
  setFunc("ShowTopicsStmt",
     QUERY_NODE_SHOW_TOPICS_STMT,
     sizeof(SShowStmt),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryShowStmt
   );
  setFunc("ShowConsumersStmt",
     QUERY_NODE_SHOW_CONSUMERS_STMT,
     sizeof(SShowStmt),
     showConsumersStmtToJson,
     jsonToShowConsumersStmt,
     destoryShowStmt
   );
  setFunc("ShowQueriesStmt",
     QUERY_NODE_SHOW_QUERIES_STMT,
     sizeof(SShowStmt),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryShowStmt
   );
  setFunc("ShowConnectionsStmt",
     QUERY_NODE_SHOW_CONNECTIONS_STMT,
     sizeof(SShowStmt),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryShowStmt
   );
  setFunc("ShowAppsStmt",
     QUERY_NODE_SHOW_APPS_STMT,
     sizeof(SShowStmt),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryShowStmt
   );
  setFunc("ShowVariablesStmt",
     QUERY_NODE_SHOW_VARIABLES_STMT,
     sizeof(SShowStmt),
     showVariablesStmtToJson,
     jsonToShowVariablesStmt,
     destoryShowStmt
   );
  setFunc("ShowDnodeVariablesStmt",
     QUERY_NODE_SHOW_DNODE_VARIABLES_STMT,
     sizeof(SShowDnodeVariablesStmt),
     showDnodeVariablesStmtToJson,
     jsonToShowDnodeVariablesStmt,
     destoryShowDnodeVariablesStmt
   );
  setFunc("ShowTransactionsStmt",
     QUERY_NODE_SHOW_TRANSACTIONS_STMT,
     sizeof(SShowStmt),
     showTransactionsStmtToJson,
     jsonToShowTransactionsStmt,
     destoryShowStmt
   );
  setFunc("ShowSubscriptionsStmt",
     QUERY_NODE_SHOW_SUBSCRIPTIONS_STMT,
     sizeof(SShowStmt),
     showSubscriptionsStmtToJson,
     jsonToShowSubscriptionsStmt,
     destoryShowStmt
   );
  setFunc("ShowVnodeStmt",
     QUERY_NODE_SHOW_VNODES_STMT,
     sizeof(SShowStmt),
     showVnodesStmtToJson,
     jsonToShowVnodesStmt,
     destoryShowStmt
   );
  setFunc("ShowUserPrivilegesStmt",
     QUERY_NODE_SHOW_USER_PRIVILEGES_STMT,
     sizeof(SShowStmt),
     showUserPrivilegesStmtToJson,
     jsonToShowUserPrivilegesStmt,
     destoryShowStmt
   );
  setFunc("ShowViewsStmt",
     QUERY_NODE_SHOW_VIEWS_STMT,
     sizeof(SShowStmt),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryShowStmt
   );
  setFunc("ShowCreateViewStmt",
     QUERY_NODE_SHOW_CREATE_VIEW_STMT,
     sizeof(SShowCreateViewStmt),
     showCreateViewStmtToJson,
     jsonToShowCreateViewStmt,
     destoryXNode
   );
  setFunc("ShowCreateDatabasesStmt",
     QUERY_NODE_SHOW_CREATE_DATABASE_STMT,
     sizeof(SShowCreateDatabaseStmt),
     showCreateDatabaseStmtToJson,
     jsonToShowCreateDatabaseStmt,
     destoryShowCreateDatabaseStmt
   );
  setFunc("ShowCreateTablesStmt",
     QUERY_NODE_SHOW_CREATE_TABLE_STMT,
     sizeof(SShowCreateTableStmt),
     showCreateTableStmtToJson,
     jsonToShowCreateTableStmt,
     destoryShowCreateTableStmt
   );
  setFunc("ShowCreateStablesStmt",
     QUERY_NODE_SHOW_CREATE_STABLE_STMT,
     sizeof(SShowCreateTableStmt),
     showCreateStableStmtToJson,
     jsonToShowCreateStableStmt,
     destoryShowCreateTableStmt
   );
  setFunc("ShowTableDistributedStmt",
     QUERY_NODE_SHOW_TABLE_DISTRIBUTED_STMT,
     sizeof(SShowTableDistributedStmt),
     showTableDistributedStmtToJson,
     jsonToShowTableDistributedStmt,
     destoryXNode
   );
  setFunc("ShowLocalVariablesStmt",
     QUERY_NODE_SHOW_LOCAL_VARIABLES_STMT,
     sizeof(SShowStmt),
     showLocalVariablesStmtToJson,
     jsonToShowLocalVariablesStmt,
     destoryShowStmt
   );
  setFunc("ShowScoresStmt",
     QUERY_NODE_SHOW_SCORES_STMT,
     sizeof(SShowStmt),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryShowStmt
   );
  setFunc("ShowTableTagsStmt",
     QUERY_NODE_SHOW_TABLE_TAGS_STMT,
     sizeof(SShowTableTagsStmt),
     showTableTagsStmtToJson,
     jsonToShowTableTagsStmt,
     destoryShowTableTagsStmt
   );
  setFunc("KillConnectionStmt",
     QUERY_NODE_KILL_CONNECTION_STMT,
     sizeof(SKillStmt),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryXNode
   );
  setFunc("KillQueryStmt",
     QUERY_NODE_KILL_QUERY_STMT,
     sizeof(SKillQueryStmt),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryXNode
   );
  setFunc("KillTransactionStmt",
     QUERY_NODE_KILL_TRANSACTION_STMT,
     sizeof(SKillStmt),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryXNode
   );
  setFunc("DeleteStmt",
     QUERY_NODE_DELETE_STMT,
     sizeof(SDeleteStmt),
     deleteStmtToJson,
     jsonToDeleteStmt,
     destoryDeleteStmt
   );
  setFunc("InsertStmt",
     QUERY_NODE_INSERT_STMT,
     sizeof(SInsertStmt),
     insertStmtToJson,
     jsonToInsertStmt,
     destoryInsertStmt
   );
  setFunc("QueryNode",
     QUERY_NODE_QUERY,
     sizeof(SQuery),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryQueryNode
   );
  setFunc("ShowDbAliveStmt",
     QUERY_NODE_SHOW_DB_ALIVE_STMT,
     sizeof(SShowAliveStmt),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryXNode
   );
  setFunc("ShowClusterAliveStmt",
     QUERY_NODE_SHOW_CLUSTER_ALIVE_STMT,
     sizeof(SShowAliveStmt),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryXNode
   );
  setFunc("RestoreDnodeStmt",
     QUERY_NODE_RESTORE_DNODE_STMT,
     sizeof(SRestoreComponentNodeStmt),
     emptyNodeToJson,
     jsonToRestoreDnodeStmt,
     destoryXNode
   );
  setFunc("RestoreQnodeStmt",
     QUERY_NODE_RESTORE_QNODE_STMT,
     sizeof(SRestoreComponentNodeStmt),
     emptyNodeToJson,
     jsonToRestoreQnodeStmt,
     destoryXNode
   );
  setFunc("RestoreMnodeStmt",
     QUERY_NODE_RESTORE_MNODE_STMT,
     sizeof(SRestoreComponentNodeStmt),
     emptyNodeToJson,
     jsonToRestoreMnodeStmt,
     destoryXNode
   );
  setFunc("RestoreVnodeStmt",
     QUERY_NODE_RESTORE_VNODE_STMT,
     sizeof(SRestoreComponentNodeStmt),
     emptyNodeToJson,
     jsonToRestoreVnodeStmt,
     destoryXNode
   );
  setFunc("CreateViewStmt",
     QUERY_NODE_CREATE_VIEW_STMT,
     sizeof(SCreateViewStmt),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryCreateViewStmt
   );
  setFunc("DropViewStmt",
     QUERY_NODE_DROP_VIEW_STMT,
     sizeof(SDropViewStmt),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryXNode
   );
  setFunc("LogicScan",
     QUERY_NODE_LOGIC_PLAN_SCAN,
     sizeof(SScanLogicNode),
     logicScanNodeToJson,
     jsonToLogicScanNode,
     destoryScanLogicNode
   );
  setFunc("LogicJoin",
     QUERY_NODE_LOGIC_PLAN_JOIN,
     sizeof(SJoinLogicNode),
     logicJoinNodeToJson,
     jsonToLogicJoinNode,
     destoryJoinLogicNode
   );
  setFunc("LogicAgg",
     QUERY_NODE_LOGIC_PLAN_AGG,
     sizeof(SAggLogicNode),
     logicAggNodeToJson,
     jsonToLogicAggNode,
     destoryAggLogicNode
   );
  setFunc("LogicProject",
     QUERY_NODE_LOGIC_PLAN_PROJECT,
     sizeof(SProjectLogicNode),
     logicProjectNodeToJson,
     jsonToLogicProjectNode,
     destoryProjectLogicNode
   );
  setFunc("LogicVnodeModify",
     QUERY_NODE_LOGIC_PLAN_VNODE_MODIFY,
     sizeof(SVnodeModifyLogicNode),
     logicVnodeModifyNodeToJson,
     jsonToLogicVnodeModifyNode,
     destoryVnodeModifyLogicNode
   );
  setFunc("LogicExchange",
     QUERY_NODE_LOGIC_PLAN_EXCHANGE,
     sizeof(SExchangeLogicNode),
     logicExchangeNodeToJson,
     jsonToLogicExchangeNode,
     destoryExchangeLogicNode
   );
  setFunc("LogicMerge",
     QUERY_NODE_LOGIC_PLAN_MERGE,
     sizeof(SMergeLogicNode),
     logicMergeNodeToJson,
     jsonToLogicMergeNode,
     destoryMergeLogicNode
   );
  setFunc("LogicWindow",
     QUERY_NODE_LOGIC_PLAN_WINDOW,
     sizeof(SWindowLogicNode),
     logicWindowNodeToJson,
     jsonToLogicWindowNode,
     destoryWindowLogicNode
   );
  setFunc("LogicFill",
     QUERY_NODE_LOGIC_PLAN_FILL,
     sizeof(SFillLogicNode),
     logicFillNodeToJson,
     jsonToLogicFillNode,
     destoryFillLogicNode
   );
  setFunc("LogicSort",
     QUERY_NODE_LOGIC_PLAN_SORT,
     sizeof(SSortLogicNode),
     logicSortNodeToJson,
     jsonToLogicSortNode,
     destorySortLogicNode
   );
  setFunc("LogicPartition",
     QUERY_NODE_LOGIC_PLAN_PARTITION,
     sizeof(SPartitionLogicNode),
     logicPartitionNodeToJson,
     jsonToLogicPartitionNode,
     destoryPartitionLogicNode
   );
  setFunc("LogicIndefRowsFunc",
     QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC,
     sizeof(SIndefRowsFuncLogicNode),
     logicIndefRowsFuncNodeToJson,
     jsonToLogicIndefRowsFuncNode,
     destoryIndefRowsFuncLogicNode
   );
  setFunc("LogicInterpFunc",
     QUERY_NODE_LOGIC_PLAN_INTERP_FUNC,
     sizeof(SInterpFuncLogicNode),
     logicInterpFuncNodeToJson,
     jsonToLogicInterpFuncNode,
     destoryInterpFuncLogicNode
   );
  setFunc("LogicGroupCache",
     QUERY_NODE_LOGIC_PLAN_GROUP_CACHE,
     sizeof(SGroupCacheLogicNode),
     logicGroupCacheNodeToJson,
     jsonToLogicGroupCacheNode,
     destoryGroupCacheLogicNode
   );
  setFunc("LogicDynamicQueryCtrl",
     QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL,
     sizeof(SDynQueryCtrlLogicNode),
     logicDynQueryCtrlNodeToJson,
     jsonToLogicDynQueryCtrlNode,
     destoryDynQueryCtrlLogicNode
   );
  setFunc("LogicSubplan",
     QUERY_NODE_LOGIC_SUBPLAN,
     sizeof(SLogicSubplan),
     logicSubplanToJson,
     jsonToLogicSubplan,
     destoryLogicSubplan
   );
  setFunc("LogicPlan",
     QUERY_NODE_LOGIC_PLAN,
     sizeof(SQueryLogicPlan),
     logicPlanToJson,
     jsonToLogicPlan,
     destoryQueryLogicPlan
   );
  setFunc("PhysiTagScan",
     QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN,
     sizeof(STagScanPhysiNode),
     physiTagScanNodeToJson,
     jsonToPhysiTagScanNode,
     destroyScanPhysiNode
   );
  setFunc("PhysiTableScan",
     QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN,
     sizeof(STableScanPhysiNode),
     physiTableScanNodeToJson,
     jsonToPhysiTableScanNode,
     destoryTableScanPhysiNode
   );
  setFunc("PhysiTableSeqScan",
     QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN,
     sizeof(STableSeqScanPhysiNode),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryTableScanPhysiNode
   );
  setFunc("PhysiTableMergeScan",
     QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN,
     sizeof(STableMergeScanPhysiNode),
     physiTableScanNodeToJson,
     jsonToPhysiTableScanNode,
     destoryTableScanPhysiNode
   );
  setFunc("PhysiSreamScan",
     QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN,
     sizeof(SStreamScanPhysiNode),
     physiTableScanNodeToJson,
     jsonToPhysiTableScanNode,
     destoryTableScanPhysiNode
   );
  setFunc("PhysiSystemTableScan",
     QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN,
     sizeof(SSystemTableScanPhysiNode),
     physiSysTableScanNodeToJson,
     jsonToPhysiSysTableScanNode,
     destroyScanPhysiNode
   );
  setFunc("PhysiBlockDistScan",
     QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN,
     sizeof(SBlockDistScanPhysiNode),
     physiScanNodeToJson,
     jsonToPhysiScanNode,
     destroyScanPhysiNode
   );
  setFunc("PhysiLastRowScan",
     QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN,
     sizeof(SLastRowScanPhysiNode),
     physiLastRowScanNodeToJson,
     jsonToPhysiLastRowScanNode,
     destoryLastRowScanPhysiNode
   );
  setFunc("PhysiProject",
     QUERY_NODE_PHYSICAL_PLAN_PROJECT,
     sizeof(SProjectPhysiNode),
     physiProjectNodeToJson,
     jsonToPhysiProjectNode,
     destoryProjectPhysiNode
   );
  setFunc("PhysiMergeJoin",
     QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN,
     sizeof(SSortMergeJoinPhysiNode),
     physiMergeJoinNodeToJson,
     jsonToPhysiMergeJoinNode,
     destorySortMergeJoinPhysiNode
   );
  setFunc("PhysiAgg",
     QUERY_NODE_PHYSICAL_PLAN_HASH_AGG,
     sizeof(SAggPhysiNode),
     physiAggNodeToJson,
     jsonToPhysiAggNode,
     destoryAggPhysiNode
   );
  setFunc("PhysiExchange",
     QUERY_NODE_PHYSICAL_PLAN_EXCHANGE,
     sizeof(SExchangePhysiNode),
     physiExchangeNodeToJson,
     jsonToPhysiExchangeNode,
     destoryExchangePhysiNode
   );
  setFunc("PhysiMerge",
     QUERY_NODE_PHYSICAL_PLAN_MERGE,
     sizeof(SMergePhysiNode),
     physiMergeNodeToJson,
     jsonToPhysiMergeNode,
     destoryMergePhysiNode
   );
  setFunc("PhysiSort",
     QUERY_NODE_PHYSICAL_PLAN_SORT,
     sizeof(SSortPhysiNode),
     physiSortNodeToJson,
     jsonToPhysiSortNode,
     destorySortPhysiNode
   );
  setFunc("PhysiGroupSort",
     QUERY_NODE_PHYSICAL_PLAN_GROUP_SORT,
     sizeof(SGroupSortPhysiNode),
     physiSortNodeToJson,
     jsonToPhysiSortNode,
     destorySortPhysiNode
   );
  setFunc("PhysiHashInterval",
     QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL,
     sizeof(SIntervalPhysiNode),
     physiIntervalNodeToJson,
     jsonToPhysiIntervalNode,
     destroyWindowPhysiNode
   );
  setFunc("PhysiMergeAlignedInterval",
     QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL,
     sizeof(SMergeAlignedIntervalPhysiNode),
     physiIntervalNodeToJson,
     jsonToPhysiIntervalNode,
     destroyWindowPhysiNode
   );
  setFunc("PhysiStreamInterval",
     QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL,
     sizeof(SStreamIntervalPhysiNode),
     physiIntervalNodeToJson,
     jsonToPhysiIntervalNode,
     destroyWindowPhysiNode
   );
  setFunc("PhysiStreamFinalInterval",
     QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL,
     sizeof(SStreamFinalIntervalPhysiNode),
     physiIntervalNodeToJson,
     jsonToPhysiIntervalNode,
     destroyWindowPhysiNode
   );
  setFunc("PhysiStreamSemiInterval",
     QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL,
     sizeof(SStreamSemiIntervalPhysiNode),
     physiIntervalNodeToJson,
     jsonToPhysiIntervalNode,
     destroyWindowPhysiNode
   );
  setFunc("PhysiFill",
     QUERY_NODE_PHYSICAL_PLAN_FILL,
     sizeof(SFillPhysiNode),
     physiFillNodeToJson,
     jsonToPhysiFillNode,
     destoryFillPhysiNode
   );
  setFunc("PhysiStreamFill",
     QUERY_NODE_PHYSICAL_PLAN_STREAM_FILL,
     sizeof(SFillPhysiNode),
     physiFillNodeToJson,
     jsonToPhysiFillNode,
     destoryFillPhysiNode
   );
  setFunc("PhysiSessionWindow",
     QUERY_NODE_PHYSICAL_PLAN_MERGE_SESSION,
     sizeof(SSessionWinodwPhysiNode),
     physiSessionWindowNodeToJson,
     jsonToPhysiSessionWindowNode,
     destroyWindowPhysiNode
   );
  setFunc("PhysiStreamSessionWindow",
     QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION,
     sizeof(SStreamSessionWinodwPhysiNode),
     physiSessionWindowNodeToJson,
     jsonToPhysiSessionWindowNode,
     destroyWindowPhysiNode
   );
  setFunc("PhysiStreamSemiSessionWindow",
     QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_SESSION,
     sizeof(SStreamSemiSessionWinodwPhysiNode),
     physiSessionWindowNodeToJson,
     jsonToPhysiSessionWindowNode,
     destroyWindowPhysiNode
   );
  setFunc("PhysiStreamFinalSessionWindow",
     QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION,
     sizeof(SStreamFinalSessionWinodwPhysiNode),
     physiSessionWindowNodeToJson,
     jsonToPhysiSessionWindowNode,
     destroyWindowPhysiNode
   );
  setFunc("PhysiStateWindow",
     QUERY_NODE_PHYSICAL_PLAN_MERGE_STATE,
     sizeof(SStateWinodwPhysiNode),
     physiStateWindowNodeToJson,
     jsonToPhysiStateWindowNode,
     destoryStateWindowPhysiNode
   );
  setFunc("PhysiStreamStateWindow",
     QUERY_NODE_PHYSICAL_PLAN_STREAM_STATE,
     sizeof(SStreamStateWinodwPhysiNode),
     physiStateWindowNodeToJson,
     jsonToPhysiStateWindowNode,
     destoryStateWindowPhysiNode
   );
  setFunc("PhysiPartition",
     QUERY_NODE_PHYSICAL_PLAN_PARTITION,
     sizeof(SPartitionPhysiNode),
     physiPartitionNodeToJson,
     jsonToPhysiPartitionNode,
     destroyPartitionPhysiNode
   );
  setFunc("PhysiStreamPartition",
     QUERY_NODE_PHYSICAL_PLAN_STREAM_PARTITION,
     sizeof(SStreamPartitionPhysiNode),
     physiStreamPartitionNodeToJson,
     jsonToPhysiStreamPartitionNode,
     destoryStreamPartitionPhysiNode
   );
  setFunc("PhysiIndefRowsFunc",
     QUERY_NODE_PHYSICAL_PLAN_INDEF_ROWS_FUNC,
     sizeof(SIndefRowsFuncPhysiNode),
     physiIndefRowsFuncNodeToJson,
     jsonToPhysiIndefRowsFuncNode,
     destoryIndefRowsFuncPhysiNode
   );
  setFunc("PhysiInterpFunc",
     QUERY_NODE_PHYSICAL_PLAN_INTERP_FUNC,
     sizeof(SInterpFuncLogicNode),
     physiInterpFuncNodeToJson,
     jsonToPhysiInterpFuncNode,
     destoryInterpFuncPhysiNode
   );
  setFunc("PhysiDispatch",
     QUERY_NODE_PHYSICAL_PLAN_DISPATCH,
     sizeof(SDataDispatcherNode),
     physiDispatchNodeToJson,
     jsonToPhysiDispatchNode,
     destroyDataSinkNode
   );
  setFunc("PhysiInsert",
     QUERY_NODE_PHYSICAL_PLAN_INSERT,
     sizeof(SDataInserterNode),
     emptyNodeToJson,
     emptyJsonToNode,
     destoryDataInserterNode
   );
  setFunc("PhysiQueryInsert",
     QUERY_NODE_PHYSICAL_PLAN_QUERY_INSERT,
     sizeof(SQueryInserterNode),
     physiQueryInsertNodeToJson,
     jsonToPhysiQueryInsertNode,
     destoryQueryInserterNode
   );
  setFunc("PhysiDelete",
     QUERY_NODE_PHYSICAL_PLAN_DELETE,
     sizeof(SDataDeleterNode),
     physiDeleteNodeToJson,
     jsonToPhysiDeleteNode,
     destoryDataDeleterNode
   );
  setFunc("PhysiGroupCache",
     QUERY_NODE_PHYSICAL_PLAN_GROUP_CACHE,
     sizeof(SGroupCachePhysiNode),
     physiGroupCacheNodeToJson,
     jsonToPhysiGroupCacheNode,
     destoryGroupCachePhysiNode
   );
  setFunc("PhysiDynamicQueryCtrl",
     QUERY_NODE_PHYSICAL_PLAN_DYN_QUERY_CTRL,
     sizeof(SDynQueryCtrlPhysiNode),
     physiDynQueryCtrlNodeToJson,
     jsonToPhysiDynQueryCtrlNode,
     destoryDynQueryCtrlPhysiNode
   );
  setFunc("PhysiSubplan",
     QUERY_NODE_PHYSICAL_SUBPLAN,
     sizeof(SSubplan),
     subplanToJson,
     jsonToSubplan,
     destorySubplanNode
   );
  setFunc("PhysiPlan",
     QUERY_NODE_PHYSICAL_PLAN,
     sizeof(SQueryPlan),
     planToJson,
     jsonToPlan,
     destoryPlanNode
   );
  setFunc("PhysiTableCountScan",
     QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN,
     sizeof(STableCountScanPhysiNode),
     physiLastRowScanNodeToJson,
     jsonToPhysiScanNode,
     destoryLastRowScanPhysiNode
   );
  setFunc("PhysiMergeEventWindow",
     QUERY_NODE_PHYSICAL_PLAN_MERGE_EVENT,
     sizeof(SEventWinodwPhysiNode),
     physiEventWindowNodeToJson,
     jsonToPhysiEventWindowNode,
     destoryEventWindowPhysiNode
   );
  setFunc("PhysiStreamEventWindow",
     QUERY_NODE_PHYSICAL_PLAN_STREAM_EVENT,
     sizeof(SStreamEventWinodwPhysiNode),
     physiEventWindowNodeToJson,
     jsonToPhysiEventWindowNode,
     destoryEventWindowPhysiNode
   );
  setFunc("PhysiHashJoin",
     QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN,
     sizeof(SHashJoinPhysiNode),
     physiHashJoinNodeToJson,
     jsonToPhysiHashJoinNode,
     destoryHashJoinPhysiNode
   );
  initNodeCode = 0;
}

// clang-format on
