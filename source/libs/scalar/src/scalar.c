#include "function.h"
#include "functionMgt.h"
#include "nodes.h"
#include "querynodes.h"
#include "sclInt.h"
#include "sclvector.h"
#include "tcommon.h"
#include "tdatablock.h"

int32_t scalarGetOperatorParamNum(EOperatorType type) {
  if (OP_TYPE_IS_NULL == type || OP_TYPE_IS_NOT_NULL == type || OP_TYPE_IS_TRUE == type || OP_TYPE_IS_NOT_TRUE == type 
   || OP_TYPE_IS_FALSE == type || OP_TYPE_IS_NOT_FALSE == type || OP_TYPE_IS_UNKNOWN == type || OP_TYPE_IS_NOT_UNKNOWN == type) {
    return 1;
  }

  return 2;
}

int32_t scalarGenerateSetFromList(void **data, void *pNode, uint32_t type) {
  SHashObj *pObj = taosHashInit(256, taosGetDefaultHashFunction(type), true, false);
  if (NULL == pObj) {
    sclError("taosHashInit failed, size:%d", 256);
    SCL_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  taosHashSetEqualFp(pObj, taosGetDefaultEqualFunction(type)); 

  int32_t code = 0;
  SNodeListNode *nodeList = (SNodeListNode *)pNode;
  SListCell *cell = nodeList->pNodeList->pHead;
  SScalarParam in = {.num = 1}, out = {.num = 1, .type = type};
  int8_t dummy = 0;
  int32_t bufLen = 60;
  out.data = malloc(bufLen);
  int32_t len = 0;
  void *buf = NULL;
  
  for (int32_t i = 0; i < nodeList->pNodeList->length; ++i) {
    SValueNode *valueNode = (SValueNode *)cell->pNode;

    if (valueNode->node.resType.type != type) {
      in.type = valueNode->node.resType.type;
      in.bytes = valueNode->node.resType.bytes;
      in.data = nodesGetValueFromNode(valueNode);
    
      code = vectorConvertImpl(&in, &out);
      if (code) {
        sclError("convert from %d to %d failed", in.type, out.type);
        SCL_ERR_JRET(code);
      }

      if (IS_VAR_DATA_TYPE(type)) {
        len = varDataLen(out.data);
        buf = varDataVal(out.data);
      } else {
        len = tDataTypes[type].bytes;
        buf = out.data;
      }
    } else {
      buf = nodesGetValueFromNode(valueNode);
      if (IS_VAR_DATA_TYPE(type)) {
        len = varDataLen(buf);
        buf = varDataVal(buf);
      } else {
        len = valueNode->node.resType.bytes;
        buf = out.data;
      }      
    }
    
    if (taosHashPut(pObj, buf, (size_t)len, &dummy, sizeof(dummy))) {
      sclError("taosHashPut failed");
      SCL_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    cell = cell->pNext;
  }

  tfree(out.data);
  *data = pObj;

  return TSDB_CODE_SUCCESS;

_return:

  tfree(out.data);
  taosHashCleanup(pObj);
  
  SCL_RET(code);
}

FORCE_INLINE bool sclIsNull(SScalarParam* param, int32_t idx) {
  if (param->dataInBlock) {
    return colDataIsNull(param->orig.columnData, 0, idx, NULL);
  }

  return param->bitmap ? colDataIsNull_f(param->bitmap, idx) : false;
}

FORCE_INLINE void sclSetNull(SScalarParam* param, int32_t idx) {
  if (NULL == param->bitmap) {
    param->bitmap = calloc(BitmapLen(param->num), sizeof(char));
    if (NULL == param->bitmap) {
      sclError("calloc %d failed", param->num);
      return;
    }
  }
  
  colDataSetNull_f(param->bitmap, idx);
}


void sclFreeRes(SHashObj *res) {
  SScalarParam *p = NULL;
  void *pIter = taosHashIterate(res, NULL);
  while (pIter) {
    p = (SScalarParam *)pIter;

    if (p) {
      sclFreeParam(p);
    }
    
    pIter = taosHashIterate(res, pIter);
  }
  
  taosHashCleanup(res);
}

void sclFreeParamNoData(SScalarParam *param) {
  tfree(param->bitmap);
}


void sclFreeParam(SScalarParam *param) {
  sclFreeParamNoData(param);
  
  if (!param->dataInBlock) {
    if (SCL_DATA_TYPE_DUMMY_HASH == param->type) {
      taosHashCleanup((SHashObj *)param->orig.data);
    } else {
      tfree(param->orig.data);
    }
  }
}


int32_t sclCopyValueNodeValue(SValueNode *pNode, void **res) {
  if (TSDB_DATA_TYPE_NULL == pNode->node.resType.type) {
    return TSDB_CODE_SUCCESS;
  }
  
  *res = malloc(pNode->node.resType.bytes);
  if (NULL == (*res)) {
    sclError("malloc %d failed", pNode->node.resType.bytes);
    SCL_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  memcpy(*res, nodesGetValueFromNode(pNode), pNode->node.resType.bytes);

  return TSDB_CODE_SUCCESS;
}

int32_t sclInitParam(SNode* node, SScalarParam *param, SScalarCtx *ctx, int32_t *rowNum) {
  switch (nodeType(node)) {
    case QUERY_NODE_VALUE: {
      SValueNode *valueNode = (SValueNode *)node;
      //SCL_ERR_RET(sclCopyValueNodeValue(valueNode, &param->data));
      param->data = nodesGetValueFromNode(valueNode);
      param->orig.data = param->data;
      param->num = 1;
      param->type = valueNode->node.resType.type;
      param->bytes = valueNode->node.resType.bytes;
      if (TSDB_DATA_TYPE_NULL == param->type) {
        sclSetNull(param, 0);
      }
      param->dataInBlock = false;
      
      break;
    }
    case QUERY_NODE_NODE_LIST: {
      SNodeListNode *nodeList = (SNodeListNode *)node;
      if (nodeList->pNodeList->length <= 0) {
        sclError("invalid length in nodeList, length:%d", nodeList->pNodeList->length);
        SCL_RET(TSDB_CODE_QRY_INVALID_INPUT);
      }

      SCL_ERR_RET(scalarGenerateSetFromList(&param->data, node, nodeList->dataType.type));
      param->orig.data = param->data;
      param->num = 1;
      param->type = SCL_DATA_TYPE_DUMMY_HASH;
      param->dataInBlock = false;

      if (taosHashPut(ctx->pRes, &node, POINTER_BYTES, param, sizeof(*param))) {
        taosHashCleanup(param->orig.data);
        param->orig.data = NULL;
        sclError("taosHashPut nodeList failed, size:%d", (int32_t)sizeof(*param));
        return TSDB_CODE_QRY_OUT_OF_MEMORY;
      }   
      break;
    }
    case QUERY_NODE_COLUMN: {
      if (NULL == ctx->pBlockList) {
        sclError("invalid node type for constant calculating, type:%d, src:%p", nodeType(node), ctx->pBlockList);
        SCL_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
      }
      
      SColumnNode *ref = (SColumnNode *)node;
      if (ref->dataBlockId >= taosArrayGetSize(ctx->pBlockList)) {
        sclError("column tupleId is too big, tupleId:%d, dataBlockNum:%d", ref->dataBlockId, (int32_t)taosArrayGetSize(ctx->pBlockList));
        SCL_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
      }

      SSDataBlock *block = *(SSDataBlock **)taosArrayGet(ctx->pBlockList, ref->dataBlockId);
      
      if (NULL == block || ref->slotId >= taosArrayGetSize(block->pDataBlock)) {
        sclError("column slotId is too big, slodId:%d, dataBlockSize:%d", ref->slotId, (int32_t)taosArrayGetSize(block->pDataBlock));
        SCL_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
      }

      SColumnInfoData *columnData = (SColumnInfoData *)taosArrayGet(block->pDataBlock, ref->slotId);
      param->data = NULL;
      param->orig.columnData = columnData;
      param->dataInBlock = true;
      
      param->num = block->info.rows;
      param->type = columnData->info.type;
      param->bytes = columnData->info.bytes;
      
      break;
    }
    case QUERY_NODE_LOGIC_CONDITION:
    case QUERY_NODE_OPERATOR: {
      SScalarParam *res = (SScalarParam *)taosHashGet(ctx->pRes, &node, POINTER_BYTES);
      if (NULL == res) {
        sclError("no result for node, type:%d, node:%p", nodeType(node), node);
        SCL_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
      }

      *param = *res;
      
      break;
    }

    default:
      break;
  }

  if (param->num > *rowNum) {
    if ((1 != param->num) && (1 < *rowNum)) {
      sclError("different row nums, rowNum:%d, newRowNum:%d", *rowNum, param->num);
      SCL_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
    }
    
    *rowNum = param->num;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t sclMoveParamListData(SScalarParam *params, int32_t listNum, int32_t idx) {
  SScalarParam *param = NULL;
  
  for (int32_t i = 0; i < listNum; ++i) {
    param = params + i;
    
    if (1 == param->num) {
      continue;
    }

    if (param->dataInBlock) {
      param->data = colDataGetData(param->orig.columnData, idx);
    } else if (idx) {
      if (IS_VAR_DATA_TYPE(param->type)) {
        param->data = (char *)(param->data) + varDataTLen(param->data);
      } else {
        param->data = (char *)(param->data) + tDataTypes[param->type].bytes;
      }
    } else {
      param->data = param->orig.data;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t sclInitParamList(SScalarParam **pParams, SNodeList* pParamList, SScalarCtx *ctx, int32_t *rowNum) {
  int32_t code = 0;
  SScalarParam *paramList = calloc(pParamList->length, sizeof(SScalarParam));
  if (NULL == paramList) {
    sclError("calloc %d failed", (int32_t)(pParamList->length * sizeof(SScalarParam)));
    SCL_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  SListCell *cell = pParamList->pHead;
  for (int32_t i = 0; i < pParamList->length; ++i) {
    if (NULL == cell || NULL == cell->pNode) {
      sclError("invalid cell, cell:%p, pNode:%p", cell, cell->pNode);
      SCL_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
    }

    SCL_ERR_JRET(sclInitParam(cell->pNode, &paramList[i], ctx, rowNum));
    
    cell = cell->pNext;
  }

  *pParams = paramList;

  return TSDB_CODE_SUCCESS;

_return:

  tfree(paramList);
  SCL_RET(code);
}

int32_t sclInitOperatorParams(SScalarParam **pParams, SOperatorNode *node, SScalarCtx *ctx, int32_t *rowNum) {
  int32_t code = 0;
  int32_t paramNum = scalarGetOperatorParamNum(node->opType);
  if (NULL == node->pLeft || (paramNum == 2 && NULL == node->pRight)) {
    sclError("invalid operation node, left:%p, right:%p", node->pLeft, node->pRight);
    SCL_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }
  
  SScalarParam *paramList = calloc(paramNum, sizeof(SScalarParam));
  if (NULL == paramList) {
    sclError("calloc %d failed", (int32_t)(paramNum * sizeof(SScalarParam)));
    SCL_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  SCL_ERR_JRET(sclInitParam(node->pLeft, &paramList[0], ctx, rowNum));
  if (paramNum > 1) {
    SCL_ERR_JRET(sclInitParam(node->pRight, &paramList[1], ctx, rowNum));
  }

  *pParams = paramList;

  return TSDB_CODE_SUCCESS;

_return:

  tfree(paramList);
  SCL_RET(code);
}


int32_t sclExecFuncion(SFunctionNode *node, SScalarCtx *ctx, SScalarParam *output) {
  if (NULL == node->pParameterList || node->pParameterList->length <= 0) {
    sclError("invalid function parameter list, list:%p, paramNum:%d", node->pParameterList, node->pParameterList ? node->pParameterList->length : 0);
    SCL_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  SScalarFuncExecFuncs ffpSet = {0};
  int32_t code = fmGetScalarFuncExecFuncs(node->funcId, &ffpSet);
  if (code) {
    sclError("fmGetFuncExecFuncs failed, funcId:%d, code:%s", node->funcId, tstrerror(code));
    SCL_ERR_RET(code);
  }

  SScalarParam *params = NULL;
  int32_t rowNum = 0;
  SCL_ERR_RET(sclInitParamList(&params, node->pParameterList, ctx, &rowNum));

  output->type = node->node.resType.type;
  output->data = calloc(rowNum, sizeof(tDataTypes[output->type].bytes));
  if (NULL == output->data) {
    sclError("calloc %d failed", (int32_t)(rowNum * sizeof(tDataTypes[output->type].bytes)));
    SCL_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }
  output->orig.data = output->data;

  for (int32_t i = 0; i < rowNum; ++i) {
    sclMoveParamListData(output, 1, i);
    sclMoveParamListData(params, node->pParameterList->length, i);

    code = (*ffpSet.process)(params, node->pParameterList->length, output);
    if (code) {
      sclError("scalar function exec failed, funcId:%d, code:%s", node->funcId, tstrerror(code));
      SCL_ERR_JRET(code);    
    }
  }

_return:

  for (int32_t i = 0; i < node->pParameterList->length; ++i) {
    sclFreeParamNoData(params + i);
  }

  tfree(params);
  
  SCL_RET(code);
}


int32_t sclExecLogic(SLogicConditionNode *node, SScalarCtx *ctx, SScalarParam *output) {
  if (NULL == node->pParameterList || node->pParameterList->length <= 0) {
    sclError("invalid logic parameter list, list:%p, paramNum:%d", node->pParameterList, node->pParameterList ? node->pParameterList->length : 0);
    SCL_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  if (TSDB_DATA_TYPE_BOOL != node->node.resType.type) {
    sclError("invalid logic resType, type:%d", node->node.resType.type);
    SCL_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  if (LOGIC_COND_TYPE_NOT == node->condType && node->pParameterList->length > 1) {
    sclError("invalid NOT operation parameter number, paramNum:%d", node->pParameterList->length);
    SCL_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  SScalarParam *params = NULL;
  int32_t rowNum = 0;
  int32_t code = 0;
  
  SCL_ERR_RET(sclInitParamList(&params, node->pParameterList, ctx, &rowNum));

  output->type = node->node.resType.type;
  output->bytes = sizeof(bool);
  output->num = rowNum;
  output->data = calloc(rowNum, sizeof(bool));
  if (NULL == output->data) {
    sclError("calloc %d failed", (int32_t)(rowNum * sizeof(bool)));
    SCL_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }
  output->orig.data = output->data;

  bool value = false;

  for (int32_t i = 0; i < rowNum; ++i) {
    sclMoveParamListData(output, 1, i);
    sclMoveParamListData(params, node->pParameterList->length, i);
  
    for (int32_t m = 0; m < node->pParameterList->length; ++m) {
      GET_TYPED_DATA(value, bool, params[m].type, params[m].data);
      
      if (LOGIC_COND_TYPE_AND == node->condType && (false == value)) {
        break;
      } else if (LOGIC_COND_TYPE_OR == node->condType && value) {
        break;
      } else if (LOGIC_COND_TYPE_NOT == node->condType) {
        value = !value;
      }
    }

    *(bool *)output->data = value;
  }

_return:

  for (int32_t i = 0; i < node->pParameterList->length; ++i) {
    sclFreeParamNoData(params + i);
  }

  tfree(params);
  SCL_RET(code);
}

int32_t sclExecOperator(SOperatorNode *node, SScalarCtx *ctx, SScalarParam *output) {
  SScalarParam *params = NULL;
  int32_t rowNum = 0;
  int32_t code = 0;
  
  SCL_ERR_RET(sclInitOperatorParams(&params, node, ctx, &rowNum));

  output->type = node->node.resType.type;
  output->num = rowNum;
  output->bytes = tDataTypes[output->type].bytes;
  output->data = calloc(rowNum, tDataTypes[output->type].bytes);
  if (NULL == output->data) {
    sclError("calloc %d failed", (int32_t)rowNum * tDataTypes[output->type].bytes);
    SCL_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }
  output->orig.data = output->data;

  _bin_scalar_fn_t OperatorFn = getBinScalarOperatorFn(node->opType);

  int32_t paramNum = scalarGetOperatorParamNum(node->opType);
  SScalarParam* pLeft = &params[0];
  SScalarParam* pRight = paramNum > 1 ? &params[1] : NULL;
  
  OperatorFn(pLeft, pRight, output, TSDB_ORDER_ASC);

_return:


  for (int32_t i = 0; i < paramNum; ++i) {
    sclFreeParamNoData(params + i);
  }

  tfree(params);
  
  SCL_RET(code);
}


EDealRes sclRewriteFunction(SNode** pNode, SScalarCtx *ctx) {
  SFunctionNode *node = (SFunctionNode *)*pNode;
  SScalarParam output = {0};
  
  ctx->code = sclExecFuncion(node, ctx, &output);
  if (ctx->code) {
    return DEAL_RES_ERROR;
  }

  SValueNode *res = (SValueNode *)nodesMakeNode(QUERY_NODE_VALUE);
  if (NULL == res) {
    sclError("make value node failed");
    sclFreeParam(&output);
    ctx->code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return DEAL_RES_ERROR;
  }

  res->node.resType = node->node.resType;

  if (IS_VAR_DATA_TYPE(output.type)) {
    res->datum.p = output.data;
    output.data = NULL;
  } else {
    memcpy(nodesGetValueFromNode(res), output.data, tDataTypes[output.type].bytes);
  }
  
  nodesDestroyNode(*pNode);
  *pNode = (SNode*)res;

  sclFreeParam(&output);

  return DEAL_RES_CONTINUE;
}

EDealRes sclRewriteLogic(SNode** pNode, SScalarCtx *ctx) {
  SLogicConditionNode *node = (SLogicConditionNode *)*pNode;
  SScalarParam output = {0};

  ctx->code = sclExecLogic(node, ctx, &output);
  if (ctx->code) {
    return DEAL_RES_ERROR;
  }

  SValueNode *res = (SValueNode *)nodesMakeNode(QUERY_NODE_VALUE);
  if (NULL == res) {
    sclError("make value node failed");
    sclFreeParam(&output);    
    ctx->code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return DEAL_RES_ERROR;
  }

  res->node.resType = node->node.resType;

  if (IS_VAR_DATA_TYPE(output.type)) {
    res->datum.p = output.data;
    output.data = NULL;
  } else {
    memcpy(nodesGetValueFromNode(res), output.data, tDataTypes[output.type].bytes);
  }

  nodesDestroyNode(*pNode);
  *pNode = (SNode*)res;

  sclFreeParam(&output);

  return DEAL_RES_CONTINUE;
}

EDealRes sclRewriteOperator(SNode** pNode, SScalarCtx *ctx) {
  SOperatorNode *node = (SOperatorNode *)*pNode;
  SScalarParam output = {0};

  ctx->code = sclExecOperator(node, ctx, &output);
  if (ctx->code) {
    return DEAL_RES_ERROR;
  }

  SValueNode *res = (SValueNode *)nodesMakeNode(QUERY_NODE_VALUE);
  if (NULL == res) {
    sclError("make value node failed");    
    sclFreeParam(&output);    
    ctx->code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return DEAL_RES_ERROR;
  }

  res->node.resType = node->node.resType;

  if (IS_VAR_DATA_TYPE(output.type)) {
    res->datum.p = output.data;
    output.data = NULL;
  } else {
    memcpy(nodesGetValueFromNode(res), output.data, tDataTypes[output.type].bytes);
  }

  nodesDestroyNode(*pNode);
  *pNode = (SNode*)res;

  sclFreeParam(&output);    

  return DEAL_RES_CONTINUE;
}


EDealRes sclConstantsRewriter(SNode** pNode, void* pContext) {
  if (QUERY_NODE_VALUE == nodeType(*pNode) || QUERY_NODE_NODE_LIST == nodeType(*pNode)) {
    return DEAL_RES_CONTINUE;
  }

  SScalarCtx *ctx = (SScalarCtx *)pContext;

  if (QUERY_NODE_FUNCTION == nodeType(*pNode)) {
    return sclRewriteFunction(pNode, ctx);
  }

  if (QUERY_NODE_LOGIC_CONDITION == nodeType(*pNode)) {
    return sclRewriteLogic(pNode, ctx);
  }

  if (QUERY_NODE_OPERATOR == nodeType(*pNode)) {
    return sclRewriteOperator(pNode, ctx);
  }  
  
  sclError("invalid node type for calculating constants, type:%d", nodeType(*pNode));

  ctx->code = TSDB_CODE_QRY_INVALID_INPUT;
  
  return DEAL_RES_ERROR;
}


EDealRes sclWalkFunction(SNode* pNode, SScalarCtx *ctx) {
  SFunctionNode *node = (SFunctionNode *)pNode;
  SScalarParam output = {0};
  
  ctx->code = sclExecFuncion(node, ctx, &output);
  if (ctx->code) {
    return DEAL_RES_ERROR;
  }

  if (taosHashPut(ctx->pRes, &pNode, POINTER_BYTES, &output, sizeof(output))) {
    ctx->code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return DEAL_RES_ERROR;
  }

  return DEAL_RES_CONTINUE;
}


EDealRes sclWalkLogic(SNode* pNode, SScalarCtx *ctx) {
  SLogicConditionNode *node = (SLogicConditionNode *)pNode;
  SScalarParam output = {0};
  
  ctx->code = sclExecLogic(node, ctx, &output);
  if (ctx->code) {
    return DEAL_RES_ERROR;
  }

  if (taosHashPut(ctx->pRes, &pNode, POINTER_BYTES, &output, sizeof(output))) {
    ctx->code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return DEAL_RES_ERROR;
  }

  return DEAL_RES_CONTINUE;
}


EDealRes sclWalkOperator(SNode* pNode, SScalarCtx *ctx) {
  SOperatorNode *node = (SOperatorNode *)pNode;
  SScalarParam output = {0};
  
  ctx->code = sclExecOperator(node, ctx, &output);
  if (ctx->code) {
    return DEAL_RES_ERROR;
  }

  if (taosHashPut(ctx->pRes, &pNode, POINTER_BYTES, &output, sizeof(output))) {
    ctx->code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return DEAL_RES_ERROR;
  }

  return DEAL_RES_CONTINUE;
}

EDealRes sclWalkTarget(SNode* pNode, SScalarCtx *ctx) {
  STargetNode *target = (STargetNode *)pNode;
  
  if (target->dataBlockId >= taosArrayGetSize(ctx->pBlockList)) {
    sclError("target tupleId is too big, tupleId:%d, dataBlockNum:%d", target->dataBlockId, (int32_t)taosArrayGetSize(ctx->pBlockList));
    ctx->code = TSDB_CODE_QRY_INVALID_INPUT;
    return DEAL_RES_ERROR;
  }

  SSDataBlock *block = *(SSDataBlock **)taosArrayGet(ctx->pBlockList, target->dataBlockId);
  if (target->slotId >= taosArrayGetSize(block->pDataBlock)) {
    sclError("target slot not exist, dataBlockId:%d, slotId:%d, dataBlockNum:%d", target->dataBlockId, target->slotId, (int32_t)taosArrayGetSize(block->pDataBlock));
    ctx->code = TSDB_CODE_QRY_INVALID_INPUT;
    return DEAL_RES_ERROR;
  }

  SColumnInfoData *col = taosArrayGet(block->pDataBlock, target->slotId);
  
  SScalarParam *res = (SScalarParam *)taosHashGet(ctx->pRes, (void *)&target->pExpr, POINTER_BYTES);
  if (NULL == res) {
    sclError("no valid res in hash, node:%p, type:%d", target->pExpr, nodeType(target->pExpr));
    ctx->code = TSDB_CODE_QRY_APP_ERROR;
    return DEAL_RES_ERROR;
  }
  
  for (int32_t i = 0; i < res->num; ++i) {
    sclMoveParamListData(res, 1, i);

    colDataAppend(col, i, res->data, sclIsNull(res, i));
  }

  sclFreeParam(res);

  taosHashRemove(ctx->pRes, (void *)&target->pExpr, POINTER_BYTES);

  return DEAL_RES_CONTINUE;
}


EDealRes sclCalcWalker(SNode* pNode, void* pContext) {
  if (QUERY_NODE_VALUE == nodeType(pNode) || QUERY_NODE_NODE_LIST == nodeType(pNode) || QUERY_NODE_COLUMN == nodeType(pNode)) {
    return DEAL_RES_CONTINUE;
  }

  SScalarCtx *ctx = (SScalarCtx *)pContext;
    
  if (QUERY_NODE_FUNCTION == nodeType(pNode)) {
    return sclWalkFunction(pNode, ctx);
  }

  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pNode)) {
    return sclWalkLogic(pNode, ctx);
  }

  if (QUERY_NODE_OPERATOR == nodeType(pNode)) {
    return sclWalkOperator(pNode, ctx);
  }

  if (QUERY_NODE_TARGET == nodeType(pNode)) {
    return sclWalkTarget(pNode, ctx);
  }

  sclError("invalid node type for scalar calculating, type:%d", nodeType(pNode));
  
  ctx->code = TSDB_CODE_QRY_INVALID_INPUT;
  
  return DEAL_RES_ERROR;
}



int32_t scalarCalculateConstants(SNode *pNode, SNode **pRes) {
  if (NULL == pNode) {
    SCL_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t code = 0;
  SScalarCtx ctx = {0};
  ctx.pRes = taosHashInit(SCL_DEFAULT_OP_NUM, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (NULL == ctx.pRes) {
    sclError("taosHashInit failed, num:%d", SCL_DEFAULT_OP_NUM);
    SCL_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }
  
  nodesRewriteNodePostOrder(&pNode, sclConstantsRewriter, (void *)&ctx);

  SCL_ERR_JRET(ctx.code);

  *pRes = pNode;

_return:
  
  sclFreeRes(ctx.pRes);

  return code;
}

int32_t scalarCalculate(SNode *pNode, SArray *pBlockList, SScalarParam *pDst) {
  if (NULL == pNode || NULL == pBlockList) {
    SCL_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t code = 0;
  SScalarCtx ctx = {.code = 0, .pBlockList = pBlockList};
  
  ctx.pRes = taosHashInit(SCL_DEFAULT_OP_NUM, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (NULL == ctx.pRes) {
    sclError("taosHashInit failed, num:%d", SCL_DEFAULT_OP_NUM);
    SCL_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }
  
  nodesWalkNodePostOrder(pNode, sclCalcWalker, (void *)&ctx);

  SCL_ERR_JRET(ctx.code);

  if (pDst) {
    SScalarParam *res = (SScalarParam *)taosHashGet(ctx.pRes, (void *)&pNode, POINTER_BYTES);
    if (NULL == res) {
      sclError("no valid res in hash, node:%p, type:%d", pNode, nodeType(pNode));
      SCL_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
    }
    
    sclMoveParamListData(res, 1, 0);
    
    *pDst = *res;
    
    taosHashRemove(ctx.pRes, (void *)&pNode, POINTER_BYTES);
  }

_return:
  
  //nodesDestroyNode(pNode);
  sclFreeRes(ctx.pRes);

  return code;
}



