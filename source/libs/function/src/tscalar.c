#include "nodes.h"
#include "tscalar.h"

int32_t sclGetOperatorParamNum(EOperatorType type) {
  if (OP_TYPE_ISNULL == type || OP_TYPE_NOTNULL == type) {
    return 1;
  }

  return 2;
}

int32_t sclPrepareFunctionParams(SScalarFuncParam **pParams, SNodeList* pParameterList) {
  *pParams = calloc(pParameterList->length, sizeof(SScalarFuncParam));
  if (NULL == *pParams) {
    sclError("calloc %d failed", pParameterList->length * sizeof(SScalarFuncParam));
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  SListCell *cell = pParameterList->pHead;
  for (int32_t i = 0; i < pParameterList->length; ++i) {
    if (NULL == cell || NULL == cell->pNode) {
      sclError("invalid cell, cell:%p, pNode:%p", cell, cell->pNode);
      tfree(*pParams);
      return TSDB_CODE_QRY_INVALID_INPUT;
    }

    if (QUERY_NODE_VALUE != nodeType(cell->pNode)) {
      sclError("invalid node type in cell, type:%d", nodeType(cell->pNode));
      tfree(*pParams);
      return TSDB_CODE_QRY_APP_ERROR;
    }

    SValueNode *valueNode = (SValueNode *)cell->pNode;
    pParams[i].data = nodesGetValueFromNode(valueNode);
    pParams[i].num = 1;
    pParams[i].type = valueNode->node.resType.type;
    pParams[i].bytes = valueNode->node.resType.bytes;

    cell = cell->pNext;
  }

  return TSDB_CODE_SUCCESS;
}

EDealRes sclRewriteFunction(SNode** pNode, void* pContext) {
  SFunctionNode *node = (SFunctionNode *)*pNode;
  if (NULL == node->pParameterList || node->pParameterList->length <= 0) {
    sclError("invalid function parameter list, list:%p, paramNum:%d", node->pParameterList, node->pParameterList ? node->pParameterList->length : 0);
    *(int32_t *)pContext = TSDB_CODE_QRY_INVALID_INPUT;
    return DEAL_RES_ERROR;
  }

  SScalarFuncExecFuncs ffpSet = {0};
  int32_t code = fmGetScalarFuncExecFuncs(node->funcId, &ffpSet);
  if (code) {
    sclError("fmGetFuncExecFuncs failed, funcId:%d, code:%s", node->funcId, tstrerror(code));
    *(int32_t *)pContext = code;    
    return DEAL_RES_ERROR;
  }

  SScalarFuncParam *input = NULL;
  if (sclPrepareFunctionParams(&input, node->pParameterList)) {
    return DEAL_RES_ERROR;
  }

  SScalarFuncParam output = {0};
  
  code = (*ffpSet.process)(input, node->pParameterList->length, &output);
  if (code) {
    sclError("scalar function exec failed, funcId:%d, code:%s", node->funcId, tstrerror(code));
    *(int32_t *)pContext = code;    
    return DEAL_RES_ERROR;
  }

  SValueNode *res = nodesMakeNode(QUERY_NODE_VALUE);
  if (NULL == res) {
    sclError("make value node failed");
    *(int32_t *)pContext = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return DEAL_RES_ERROR;
  }

  res->node.resType = node->node.resType;

  SET_TYPED_DATA(nodesGetValueFromNode(res), output.type, output.data);

  nodesDestroyNode(*pNode);
  *pNode = (SNode*)res;

  tfree(output.data);

  return DEAL_RES_CONTINUE;
}

EDealRes sclRewriteLogic(SNode** pNode, void* pContext) {
  SLogicConditionNode *node = (SLogicConditionNode *)*pNode;
  if (NULL == node->pParameterList || node->pParameterList->length <= 0) {
    sclError("invalid logic parameter list, list:%p, paramNum:%d", node->pParameterList, node->pParameterList ? node->pParameterList->length : 0);
    *(int32_t *)pContext = TSDB_CODE_QRY_INVALID_INPUT;
    return DEAL_RES_ERROR;
  }

  if (LOGIC_COND_TYPE_NOT == node->condType && node->pParameterList->length > 1) {
    sclError("invalid NOT operation parameter number, paramNum:%d", node->pParameterList->length);
    *(int32_t *)pContext = TSDB_CODE_QRY_INVALID_INPUT;
    return DEAL_RES_ERROR;
  }

  bool value = false;
  SListCell *cell = node->pParameterList->pHead;
  for (int32_t i = 0; i < node->pParameterList->length; ++i) {
    if (NULL == cell || NULL == cell->pNode) {
      sclError("invalid cell, cell:%p, pNode:%p", cell, cell->pNode);
      return TSDB_CODE_QRY_INVALID_INPUT;
    }

    if (QUERY_NODE_VALUE != nodeType(cell->pNode)) {
      sclError("invalid node type in cell, type:%d", nodeType(cell->pNode));
      return TSDB_CODE_QRY_APP_ERROR;
    }

    SValueNode *valueNode = (SValueNode *)cell->pNode;
    GET_TYPED_DATA(value, bool, valueNode->node.resType.type, nodesGetValueFromNode(valueNode));
    if (LOGIC_COND_TYPE_AND == node->condType && (false == value)) {
      break;
    } else if (LOGIC_COND_TYPE_OR == node->condType && value) {
      break;
    } else if (LOGIC_COND_TYPE_NOT == node->condType) {
      value = !value;
    }

    cell = cell->pNext;
  }

  SValueNode *res = nodesMakeNode(QUERY_NODE_VALUE);
  if (NULL == res) {
    sclError("make value node failed");
    *(int32_t *)pContext = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return DEAL_RES_ERROR;
  }

  res->node.resType = node->node.resType;

  SET_TYPED_DATA(nodesGetValueFromNode(res), res->node.resType.type, value);

  nodesDestroyNode(*pNode);
  *pNode = (SNode*)res;

  return DEAL_RES_CONTINUE;
}

EDealRes sclRewriteOperator(SNode** pNode, void* pContext) {
  SOperatorNode *oper = (SOperatorNode *)*pNode;
  int32_t paramNum = sclGetOperatorParamNum(oper->opType);
  if (NULL == oper->pLeft || (paramNum == 2 && NULL == oper->pRight)) {
    sclError("invalid operation node, left:%p, right:%p", oper->pLeft, oper->pRight);
    *(int32_t *)pContext = TSDB_CODE_QRY_INVALID_INPUT;
    return DEAL_RES_ERROR;
  }

  if (QUERY_NODE_VALUE != nodeType(oper->pLeft) || (paramNum == 2 && QUERY_NODE_VALUE != nodeType(oper->pRight))) {
    sclError("invalid operation node, leftType:%d, rightType:%d", nodeType(oper->pLeft), oper->pRight ? nodeType(oper->pRight) : 0);
    *(int32_t *)pContext = TSDB_CODE_QRY_INVALID_INPUT;
    return DEAL_RES_ERROR;
  }

  SValueNode *res = nodesMakeNode(QUERY_NODE_VALUE);
  if (NULL == res) {
    sclError("make value node failed");
    *(int32_t *)pContext = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return DEAL_RES_ERROR;
  }

  res->node.resType = oper->node.resType;

  SValueNode *leftValue = (SValueNode *)oper->pLeft;
  SValueNode *rightValue = (SValueNode *)oper->pRight;

  SScalarFuncParam leftParam = {0}, rightParam = {0};
  _bin_scalar_fn_t OperatorFn = getBinScalarOperatorFn(oper->opType);
  setScalarFuncParam(&leftParam, leftValue->node.resType.type, 0, nodesGetValueFromNode(leftValue), 1);
  if (2 == paramNum) {
    setScalarFuncParam(&rightParam, rightValue->node.resType.type, 0, nodesGetValueFromNode(rightValue), 1);
  }
  
  OperatorFn(&leftParam, &rightParam, nodesGetValueFromNode(res), TSDB_ORDER_ASC);

  nodesDestroyNode(*pNode);
  *pNode = (SNode*)res;

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

  if (QUERY_NODE_OPERATOR != nodeType(*pNode)) {
    sclError("invalid node type for calculating constants, type:%d", );
    *(int32_t *)pContext = TSDB_CODE_QRY_INVALID_INPUT;
    return DEAL_RES_ERROR;
  }  
  
  return sclRewriteOperator(pNode, pContext);
}

EDealRes sclCalculate(SNode** pNode, void* pContext) {
  if (QUERY_NODE_VALUE == nodeType(*pNode)) {
    return DEAL_RES_CONTINUE;
  }

  if (QUERY_NODE_FUNCTION == nodeType(*pNode)) {
    return sclCalculateFunction(pNode, pContext);
  }

  if (QUERY_NODE_LOGIC_CONDITION == nodeType(*pNode)) {
    return sclCalculateLogic(pNode, pContext);
  }

  if (QUERY_NODE_OPERATOR != nodeType(*pNode)) {
    sclError("invalid node type for calculating constants, type:%d", );
    *(int32_t *)pContext = TSDB_CODE_QRY_INVALID_INPUT;
    return DEAL_RES_ERROR;
  }  
  
  SOperatorNode *oper = (SOperatorNode *)*pNode;
  int32_t paramNum = sclGetOperatorParamNum(oper->opType);
  if (NULL == oper->pLeft || (paramNum == 2 && NULL == oper->pRight)) {
    sclError("invalid operation node, left:%p, right:%p", oper->pLeft, oper->pRight);
    *(int32_t *)pContext = TSDB_CODE_QRY_INVALID_INPUT;
    return DEAL_RES_ERROR;
  }

  if (QUERY_NODE_VALUE != nodeType(oper->pLeft) || (paramNum == 2 && QUERY_NODE_VALUE != nodeType(oper->pRight))) {
    sclError("invalid operation node, leftType:%d, rightType:%d", nodeType(oper->pLeft), oper->pRight ? nodeType(oper->pRight) : 0);
    *(int32_t *)pContext = TSDB_CODE_QRY_INVALID_INPUT;
    return DEAL_RES_ERROR;
  }

  SValueNode *res = nodesMakeNode(QUERY_NODE_VALUE);
  if (NULL == res) {
    sclError("make value node failed");
    *(int32_t *)pContext = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return DEAL_RES_ERROR;
  }

  res->node.resType = oper->node.resType;

  SValueNode *leftValue = (SValueNode *)oper->pLeft;
  SValueNode *rightValue = (SValueNode *)oper->pRight;

  SScalarFuncParam leftParam = {0}, rightParam = {0};
  _bin_scalar_fn_t OperatorFn = getBinScalarOperatorFn(oper->opType);
  setScalarFuncParam(&leftParam, leftValue->node.resType.type, 0, nodesGetValueFromNode(leftValue), 1);
  if (2 == paramNum) {
    setScalarFuncParam(&rightParam, rightValue->node.resType.type, 0, nodesGetValueFromNode(rightValue), 1);
  }
  
  OperatorFn(&leftParam, &rightParam, nodesGetValueFromNode(res), TSDB_ORDER_ASC);

  nodesDestroyNode(*pNode);
  *pNode = (SNode*)res;

  return DEAL_RES_CONTINUE;
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

int32_t scalarCalculate(SNode *pNode, SSDataBlock *pSrc, SSDataBlock *pDst) {
  if (NULL == pNode) {
    SCL_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t code = 0;
  
  nodesRewriteNodePostOrder(&pNode, sclCalculate, (void *)&code);

  if (code) {
    nodesDestroyNode(pNode);
    SCL_ERR_RET(code);
  }

  *pRes = pNode;

  SCL_RET(code);
}



