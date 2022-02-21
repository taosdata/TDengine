#include "nodes.h"
#include "common.h"
#include "querynodes.h"
#include "function.h"
#include "functionMgt.h"
#include "sclvector.h"
#include "sclInt.h"

int32_t scalarGetOperatorParamNum(EOperatorType type) {
  if (OP_TYPE_IS_NULL == type || OP_TYPE_IS_NOT_NULL == type) {
    return 1;
  }

  return 2;
}

void sclFreeRes(SHashObj *res) {
  SScalarParam *p = NULL;
  void *pIter = taosHashIterate(res, NULL);
  while (pIter) {
    p = (SScalarParam *)pIter;

    if (p) {
      tfree(p->data);
    }
    
    pIter = taosHashIterate(res, pIter);
  }
  
  taosHashCleanup(res);
}

void sclFreeParam(SScalarParam *param) {
  tfree(param->data);
}

int32_t sclInitParam(SNode* node, SScalarParam *param, SScalarCtx *ctx, int32_t *rowNum) {
  switch (nodeType(node)) {
    case QUERY_NODE_VALUE: {
      SValueNode *valueNode = (SValueNode *)node;
      param->data = nodesGetValueFromNode(valueNode);
      param->num = 1;
      param->type = valueNode->node.resType.type;
      param->bytes = valueNode->node.resType.bytes;
      
      break;
    }
    case QUERY_NODE_NODE_LIST: {
      SNodeListNode *nodeList = (SNodeListNode *)node;
      //TODO BUILD HASH
      break;
    }
    case QUERY_NODE_COLUMN_REF: {
      if (NULL == ctx) {
        sclError("invalid node type for constant calculating, type:%d, ctx:%p", nodeType(node), ctx);
        SCL_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
      }
      
      SColumnRefNode *ref = (SColumnRefNode *)node;
      if (ref->slotId >= taosArrayGetSize(ctx->pSrc->pDataBlock)) {
        sclError("column ref slotId is too big, slodId:%d, dataBlockSize:%d", ref->slotId, (int32_t)taosArrayGetSize(ctx->pSrc->pDataBlock));
        SCL_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
      }

      SColumnInfoData *columnData = (SColumnInfoData *)taosArrayGet(ctx->pSrc->pDataBlock, ref->slotId);
      param->data = columnData->pData;
      param->num = ctx->pSrc->info.rows;
      param->type = columnData->info.type;
      param->bytes = columnData->info.bytes;
      
      break;
    }
    case QUERY_NODE_LOGIC_CONDITION:
    case QUERY_NODE_OPERATOR: {
      if (NULL == ctx) {
        sclError("invalid node type for constant calculating, type:%d, ctx:%p", nodeType(node), ctx);
        SCL_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
      }

      SScalarParam *res = (SScalarParam *)taosHashGet(ctx->pRes, &node, POINTER_BYTES);
      if (NULL == res) {
        sclError("no result for node, type:%d, node:%p", nodeType(node), node);
        SCL_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
      }

      *param = *res;
      
      break;
    }
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

int32_t sclParamMoveNext(SScalarParam *params, int32_t num) {
  SScalarParam *param = NULL;
  
  for (int32_t i = 0; i < num; ++i) {
    param = params + i;
    
    if (1 == param->num) {
      continue;
    }

    if (IS_VAR_DATA_TYPE(param->type)) {
      param->data = (char *)(param->data) + varDataTLen(param->data);
    } else {
      param->data = (char *)(param->data) + tDataTypes[param->type].bytes;
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
    sclError("fmGetFuncExecFuncs failed, funcId:%d, code:%s", node->funcId, tstrerror(code));
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

  for (int32_t i = 0; i < rowNum; ++i) {
    code = (*ffpSet.process)(params, node->pParameterList->length, output);
    if (code) {
      sclError("scalar function exec failed, funcId:%d, code:%s", node->funcId, tstrerror(code));
      SCL_ERR_JRET(code);    
    }

    sclParamMoveNext(output, 1);
    sclParamMoveNext(params, node->pParameterList->length);
  }

  return TSDB_CODE_SUCCESS;

_return:

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
  output->data = calloc(rowNum, sizeof(bool));
  if (NULL == output->data) {
    sclError("calloc %d failed", (int32_t)(rowNum * sizeof(bool)));
    SCL_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }
  
  bool value = false;

  for (int32_t i = 0; i < rowNum; ++i) {
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

    sclParamMoveNext(output, 1);    
    sclParamMoveNext(params, node->pParameterList->length);
  }

  return TSDB_CODE_SUCCESS;

_return:

  tfree(params);
  SCL_RET(code);
}

int32_t sclExecOperator(SOperatorNode *node, SScalarCtx *ctx, SScalarParam *output) {
  SScalarParam *params = NULL;
  int32_t rowNum = 0;
  int32_t code = 0;
  
  SCL_ERR_RET(sclInitOperatorParams(&params, node, ctx, &rowNum));

  output->type = node->node.resType.type;
  output->data = calloc(rowNum, tDataTypes[output->type].bytes);
  if (NULL == output->data) {
    sclError("calloc %d failed", (int32_t)rowNum * tDataTypes[output->type].bytes);
    SCL_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  _bin_scalar_fn_t OperatorFn = getBinScalarOperatorFn(node->opType);

  int32_t paramNum = scalarGetOperatorParamNum(node->opType);
  SScalarParam* pLeft = &params[0];
  SScalarParam* pRight = paramNum > 1 ? &params[1] : NULL;

  for (int32_t i = 0; i < rowNum; ++i) {

    OperatorFn(pLeft, pRight, output->data, TSDB_ORDER_ASC);

    sclParamMoveNext(output, 1);    
    sclParamMoveNext(pLeft, 1);
    if (pRight) {
      sclParamMoveNext(pRight, 1);
    }
  }

  return TSDB_CODE_SUCCESS;

_return:

  tfree(params);
  SCL_RET(code);
}


EDealRes sclRewriteFunction(SNode** pNode, void* pContext) {
  SFunctionNode *node = (SFunctionNode *)*pNode;
  SScalarParam output = {0};
  
  *(int32_t *)pContext = sclExecFuncion(node, NULL, &output);
  if (*(int32_t *)pContext) {
    return DEAL_RES_ERROR;
  }

  SValueNode *res = (SValueNode *)nodesMakeNode(QUERY_NODE_VALUE);
  if (NULL == res) {
    sclError("make value node failed");
    sclFreeParam(&output);
    *(int32_t *)pContext = TSDB_CODE_QRY_OUT_OF_MEMORY;
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

EDealRes sclRewriteLogic(SNode** pNode, void* pContext) {
  SLogicConditionNode *node = (SLogicConditionNode *)*pNode;
  SScalarParam output = {0};

  *(int32_t *)pContext = sclExecLogic(node, NULL, &output);
  if (*(int32_t *)pContext) {
    return DEAL_RES_ERROR;
  }

  SValueNode *res = (SValueNode *)nodesMakeNode(QUERY_NODE_VALUE);
  if (NULL == res) {
    sclError("make value node failed");
    sclFreeParam(&output);    
    *(int32_t *)pContext = TSDB_CODE_QRY_OUT_OF_MEMORY;
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

EDealRes sclRewriteOperator(SNode** pNode, void* pContext) {
  SOperatorNode *node = (SOperatorNode *)*pNode;
  SScalarParam output = {0};

  *(int32_t *)pContext = sclExecOperator(node, NULL, &output);
  if (*(int32_t *)pContext) {
    return DEAL_RES_ERROR;
  }

  SValueNode *res = (SValueNode *)nodesMakeNode(QUERY_NODE_VALUE);
  if (NULL == res) {
    sclError("make value node failed");    
    sclFreeParam(&output);    
    *(int32_t *)pContext = TSDB_CODE_QRY_OUT_OF_MEMORY;
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
  if (QUERY_NODE_VALUE == nodeType(*pNode)) {
    return DEAL_RES_CONTINUE;
  }

  if (QUERY_NODE_FUNCTION == nodeType(*pNode)) {
    return sclRewriteFunction(pNode, pContext);
  }

  if (QUERY_NODE_LOGIC_CONDITION == nodeType(*pNode)) {
    return sclRewriteLogic(pNode, pContext);
  }

  if (QUERY_NODE_OPERATOR == nodeType(*pNode)) {
    return sclRewriteOperator(pNode, pContext);
  }  
  
  sclError("invalid node type for calculating constants, type:%d", nodeType(*pNode));
  
  *(int32_t *)pContext = TSDB_CODE_QRY_INVALID_INPUT;
  
  return DEAL_RES_ERROR;
}


EDealRes sclWalkFunction(SNode* pNode, void* pContext) {
  SScalarCtx *ctx = (SScalarCtx *)pContext;
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


EDealRes sclWalkLogic(SNode* pNode, void* pContext) {
  SScalarCtx *ctx = (SScalarCtx *)pContext;
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


EDealRes sclWalkOperator(SNode* pNode, void* pContext) {
  SScalarCtx *ctx = (SScalarCtx *)pContext;
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


EDealRes sclCalcWalker(SNode* pNode, void* pContext) {
  if (QUERY_NODE_VALUE == nodeType(pNode)) {
    return DEAL_RES_CONTINUE;
  }

  if (QUERY_NODE_FUNCTION == nodeType(pNode)) {
    return sclWalkFunction(pNode, pContext);
  }

  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pNode)) {
    return sclWalkLogic(pNode, pContext);
  }

  if (QUERY_NODE_OPERATOR == nodeType(pNode)) {
    return sclWalkOperator(pNode, pContext);
  }

  sclError("invalid node type for calculating constants, type:%d", nodeType(pNode));

  SScalarCtx *ctx = (SScalarCtx *)pContext;
  
  ctx->code = TSDB_CODE_QRY_INVALID_INPUT;
  
  return DEAL_RES_ERROR;
}



int32_t scalarCalculateConstants(SNode *pNode, SNode **pRes) {
  if (NULL == pNode) {
    SCL_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t code = 0;
  
  nodesRewriteNodePostOrder(&pNode, sclConstantsRewriter, (void *)&code);

  if (code) {
    nodesDestroyNode(pNode);
    SCL_ERR_RET(code);
  }

  *pRes = pNode;

  SCL_RET(code);
}

int32_t scalarCalculate(SNode *pNode, SSDataBlock *pSrc, SScalarParam *pDst) {
  if (NULL == pNode || NULL == pSrc || NULL == pDst) {
    SCL_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t code = 0;
  SScalarCtx ctx = {.code = 0, .pSrc = pSrc};
  
  ctx.pRes = taosHashInit(SCL_DEFAULT_OP_NUM, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (NULL == ctx.pRes) {
    sclError("taosHashInit failed, num:%d", SCL_DEFAULT_OP_NUM);
    SCL_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }
  
  nodesWalkNodePostOrder(pNode, sclCalcWalker, (void *)&ctx);

  if (ctx.code) {
    nodesDestroyNode(pNode);
    sclFreeRes(ctx.pRes);
    SCL_ERR_RET(ctx.code);
  }

  SScalarParam *res = (SScalarParam *)taosHashGet(ctx.pRes, (void *)&pNode, POINTER_BYTES);
  if (NULL == res) {
    sclError("no res for calculating, node:%p, type:%d", pNode, nodeType(pNode));
    SCL_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }
  
  *pDst = *res;

  nodesDestroyNode(pNode);

  return TSDB_CODE_SUCCESS;
}



