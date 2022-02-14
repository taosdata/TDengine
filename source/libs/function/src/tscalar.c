#include "nodes.h"
#include "tscalar.h"

EDealRes sclCalculateConstants(SNode** pNode, void* pContext) {
  if (QUERY_NODE_VALUE == nodeType(*pNode)) {
    return DEAL_RES_CONTINUE;
  }

  if (QUERY_NODE_OPERATOR != nodeType(*pNode)) {
    sclError("invalid node type for calculating constants, type:%d", );
    *(int32_t *)pContext = TSDB_CODE_QRY_INVALID_INPUT;
    return DEAL_RES_ERROR;
  }  
  
  SOperatorNode *oper = (SOperatorNode *)*pNode;
  if (NULL == oper->pLeft || NULL == oper->pRight) {
    sclError("invalid operation node, left:%p, right:%p", oper->pLeft, oper->pRight);
    *(int32_t *)pContext = TSDB_CODE_QRY_INVALID_INPUT;
    return DEAL_RES_ERROR;
  }
  
  if (QUERY_NODE_VALUE != nodeType(oper->pLeft) || QUERY_NODE_VALUE != nodeType(oper->pRight)) {
    sclError("invalid operation node, leftType:%d, rightType:%d", nodeType(oper->pLeft), nodeType(oper->pRight));
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
  setScalarFuncParam(&leftParam, leftValue->node.resType, 0, nodesGetValueFromNode(leftValue), 1);
  setScalarFuncParam(&rightParam, rightValue->node.resType, 0, nodesGetValueFromNode(rightValue), 1);
  
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
  
  nodesRewriteNodePostOrder(&pNode, sclCalculateConstants, (void *)&code);

  if (code) {
    nodesDestroyNode(pNode);
    SCL_ERR_RET(code);
  }

  *pRes = pNode;

  SCL_RET(code);
}



