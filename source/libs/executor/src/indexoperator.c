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

#include "indexoperator.h"
#include "executorimpl.h"
#include "nodes.h"

typedef struct SIFCtx {
  int32_t   code;
  SHashObj *pRes; /* element is SScalarParam */
} SIFCtx;

#define SIF_ERR_RET(c)                \
  do {                                \
    int32_t _code = c;                \
    if (_code != TSDB_CODE_SUCCESS) { \
      terrno = _code;                 \
      return _code;                   \
    }                                 \
  } while (0)
#define SIF_RET(c)                    \
  do {                                \
    int32_t _code = c;                \
    if (_code != TSDB_CODE_SUCCESS) { \
      terrno = _code;                 \
    }                                 \
    return _code;                     \
  } while (0)
#define SIF_ERR_JRET(c)              \
  do {                               \
    code = c;                        \
    if (code != TSDB_CODE_SUCCESS) { \
      terrno = code;                 \
      goto _return;                  \
    }                                \
  } while (0)

typedef struct SIFParam {
  SArray *  result;
  SHashObj *pFilter;
} SIFParam;

typedef int32_t (*sif_func_t)(SNode *left, SNode *rigth, SIFParam *output);
// construct tag filter operator later
static void destroyTagFilterOperatorInfo(void *param) { STagFilterOperatorInfo *pInfo = (STagFilterOperatorInfo *)param; }

static void sifFreeParam(SIFParam *param) {
  if (param == NULL) return;
  taosArrayDestroy(param->result);
}

static int32_t sifGetOperParamNum(EOperatorType ty) {
  if (OP_TYPE_IS_NULL == ty || OP_TYPE_IS_NOT_NULL == ty || OP_TYPE_IS_TRUE == ty || OP_TYPE_IS_NOT_TRUE == ty || OP_TYPE_IS_FALSE == ty ||
      OP_TYPE_IS_NOT_FALSE == ty || OP_TYPE_IS_UNKNOWN == ty || OP_TYPE_IS_NOT_UNKNOWN == ty || OP_TYPE_MINUS == ty) {
    return 1;
  }
  return 2;
}
static int32_t sifInitParam(SNode *node, SIFParam *param, SIFCtx *ctx) {
  switch (nodeType(node)) {
    case QUERY_NODE_VALUE: {
      SValueNode *vn = (SValueNode *)node;

      break;
    }
    case QUERY_NODE_COLUMN: {
      SColumnNode *cn = (SColumnNode *)node;

      break;
    }
    case QUERY_NODE_NODE_LIST: {
      SNodeListNode *nl = (SNodeListNode *)node;
      if (LIST_LENGTH(nl->pNodeList) <= 0) {
        qError("invalid length for node:%p, length: %d", node, LIST_LENGTH(nl->pNodeList));
        SIF_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
      }

      if (taosHashPut(ctx->pRes, &node, POINTER_BYTES, param, sizeof(*param))) {
        taosHashCleanup(param->pFilter);
        qError("taosHashPut nodeList failed, size:%d", (int32_t)sizeof(*param));
        SIF_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }
      break;
    }
    case QUERY_NODE_FUNCTION:
    case QUERY_NODE_OPERATOR:
    case QUERY_NODE_LOGIC_CONDITION: {
      SIFParam *res = (SIFParam *)taosHashGet(ctx->pRes, &node, POINTER_BYTES);
      if (NULL == res) {
        qError("no result for node, type:%d, node:%p", nodeType(node), node);
        SIF_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
      }
      *param = *res;
      break;
    }
    default:
      break;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t sifInitOperParams(SIFParam **params, SOperatorNode *node, SIFCtx *ctx) {
  int32_t code = 0;
  int32_t nParam = sifGetOperParamNum(node->opType);
  if (NULL == node->pLeft || (nParam == 2 && NULL == node->pRight)) {
    qError("invalid operation node, left: %p, rigth: %p", node->pLeft, node->pRight);
    SIF_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }
  SIFParam *paramList = taosMemoryCalloc(nParam, sizeof(SIFParam));
  if (NULL == paramList) {
    SIF_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  SIF_ERR_JRET(sifInitParam(node->pLeft, &paramList[0], ctx));
  if (nParam > 1) {
    SIF_ERR_JRET(sifInitParam(node->pRight, &paramList[1], ctx));
  }
  *params = paramList;
  return TSDB_CODE_SUCCESS;
_return:
  taosMemoryFree(paramList);
  SIF_RET(code);
}
static int32_t sifInitParamList(SIFParam **params, SNodeList *nodeList, SIFCtx *ctx) {
  int32_t   code = 0;
  SIFParam *tParams = taosMemoryCalloc(nodeList->length, sizeof(SIFParam));
  if (tParams == NULL) {
    qError("failed to calloc, nodeList: %p", nodeList);
    SIF_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  SListCell *cell = nodeList->pHead;
  for (int32_t i = 0; i < nodeList->length; i++) {
    if (NULL == cell || NULL == cell->pNode) {
      SIF_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
    }
    SIF_ERR_JRET(sifInitParam(cell->pNode, &tParams[i], ctx));
    cell = cell->pNext;
  }
  *params = tParams;
  return TSDB_CODE_SUCCESS;

_return:
  taosMemoryFree(tParams);
  SIF_RET(code);
}
static int32_t sifExecFunction(SFunctionNode *node, SIFCtx *ctx, SIFParam *output) {
  qError("index-filter not support buildin function");
  return TSDB_CODE_QRY_INVALID_INPUT;
}

static int32_t sifLessThanFunc(SNode *left, SNode *rigth, SIFParam *output) {
  // impl later
  return TSDB_CODE_SUCCESS;
}
static int32_t sifLessEqualFunc(SNode *left, SNode *rigth, SIFParam *output) {
  // impl later
  return TSDB_CODE_SUCCESS;
}
static int32_t sifGreaterThanFunc(SNode *left, SNode *rigth, SIFParam *output) {
  // impl later
  return TSDB_CODE_SUCCESS;
}
static int32_t sifGreaterEqualFunc(SNode *left, SNode *rigth, SIFParam *output) {
  // impl later
  return TSDB_CODE_SUCCESS;
}

static int32_t sifEqualFunc(SNode *left, SNode *rigth, SIFParam *output) {
  // impl later
  return TSDB_CODE_SUCCESS;
}
static int32_t sifNotEqualFunc(SNode *left, SNode *rigth, SIFParam *output) {
  // impl later
  return TSDB_CODE_SUCCESS;
}
static int32_t sifInFunc(SNode *left, SNode *rigth, SIFParam *output) {
  // impl later
  return TSDB_CODE_SUCCESS;
}
static int32_t sifNotInFunc(SNode *left, SNode *right, SIFParam *output) {
  // impl later
  return TSDB_CODE_SUCCESS;
}
static int32_t sifLikeFunc(SNode *left, SNode *right, SIFParam *output) {
  // impl later
  return TSDB_CODE_SUCCESS;
}
static int32_t sifNotLikeFunc(SNode *left, SNode *right, SIFParam *output) {
  // impl later
  return TSDB_CODE_SUCCESS;
}

static int32_t sifMatchFunc(SNode *left, SNode *rigth, SIFParam *output) {
  // impl later
  return TSDB_CODE_SUCCESS;
}
static int32_t sifNotMatchFunc(SNode *left, SNode *rigth, SIFParam *output) {
  // impl later
  return TSDB_CODE_SUCCESS;
}
static int32_t sifDefaultFunc(SNode *left, SNode *rigth, SIFParam *output) {
  // add more except
  return TSDB_CODE_QRY_INVALID_INPUT;
}

static sif_func_t sifGetOperFn(int32_t funcId) {
  // impl later
  switch (funcId) {
    case OP_TYPE_GREATER_THAN:
      return sifGreaterThanFunc;
    case OP_TYPE_GREATER_EQUAL:
      return sifGreaterEqualFunc;
    case OP_TYPE_LOWER_THAN:
      return sifLessThanFunc;
    case OP_TYPE_LOWER_EQUAL:
      return sifLessEqualFunc;
    case OP_TYPE_EQUAL:
      return sifEqualFunc;
    case OP_TYPE_NOT_EQUAL:
      return sifNotEqualFunc;
    case OP_TYPE_IN:
      return sifInFunc;
    case OP_TYPE_NOT_IN:
      return sifNotInFunc;
    case OP_TYPE_LIKE:
      return sifLikeFunc;
    case OP_TYPE_NOT_LIKE:
      return sifNotLikeFunc;
    case OP_TYPE_MATCH:
      return sifMatchFunc;
    case OP_TYPE_NMATCH:
      return sifNotMatchFunc;
    default:
      return sifDefaultFunc;
  }
  return sifDefaultFunc;
}
static int32_t sifExecOper(SOperatorNode *node, SIFCtx *ctx, SIFParam *output) {
  int32_t   code = 0;
  SIFParam *params = NULL;
  SIF_ERR_RET(sifInitOperParams(&params, node, ctx));

  int32_t nParam = sifGetOperParamNum(node->opType);
  if (nParam <= 1) {
    SIF_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
  }
  sif_func_t operFn = sifGetOperFn(node->opType);

  return operFn(node->pLeft, node->pRight, output);
_return:
  taosMemoryFree(params);
  SIF_RET(code);
}

static int32_t sifExecLogic(SLogicConditionNode *node, SIFCtx *ctx, SIFParam *output) {
  if (NULL == node->pParameterList || node->pParameterList->length <= 0) {
    qError("invalid logic parameter list, list:%p, paramNum:%d", node->pParameterList, node->pParameterList ? node->pParameterList->length : 0);
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  int32_t   code;
  SIFParam *params = NULL;
  SIF_ERR_RET(sifInitParamList(&params, node->pParameterList, ctx));

  for (int32_t m = 0; m < node->pParameterList->length; m++) {
    // add impl later
    if (node->condType == LOGIC_COND_TYPE_AND) {
      taosArrayAddAll(output->result, params[m].result);
    } else if (node->condType == LOGIC_COND_TYPE_OR) {
      taosArrayAddAll(output->result, params[m].result);
    } else if (node->condType == LOGIC_COND_TYPE_NOT) {
      taosArrayAddAll(output->result, params[m].result);
    }
  }
_return:
  taosMemoryFree(params);
  SIF_RET(code);
}

static EDealRes sifWalkFunction(SNode *pNode, void *context) {
  SFunctionNode *node = (SFunctionNode *)pNode;
  SIFParam       output = {0};

  SIFCtx *ctx = context;
  ctx->code = sifExecFunction(node, ctx, &output);
  if (ctx->code != TSDB_CODE_SUCCESS) {
    return DEAL_RES_ERROR;
  }

  if (taosHashPut(ctx->pRes, &pNode, POINTER_BYTES, &output, sizeof(output))) {
    ctx->code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return DEAL_RES_ERROR;
  }
  return DEAL_RES_CONTINUE;
}
static EDealRes sifWalkLogic(SNode *pNode, void *context) {
  SLogicConditionNode *node = (SLogicConditionNode *)pNode;
  SIFParam             output = {0};

  SIFCtx *ctx = context;
  ctx->code = sifExecLogic(node, ctx, &output);
  if (ctx->code) {
    return DEAL_RES_ERROR;
  }

  if (taosHashPut(ctx->pRes, &pNode, POINTER_BYTES, &output, sizeof(output))) {
    ctx->code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return DEAL_RES_ERROR;
  }
  return DEAL_RES_CONTINUE;
}
static EDealRes sifWalkOper(SNode *pNode, void *context) {
  SOperatorNode *node = (SOperatorNode *)pNode;
  SIFParam       output = {0};

  SIFCtx *ctx = context;
  ctx->code = sifExecOper(node, ctx, &output);
  if (ctx->code) {
    return DEAL_RES_ERROR;
  }

  if (taosHashPut(ctx->pRes, &pNode, POINTER_BYTES, &output, sizeof(output))) {
    ctx->code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return DEAL_RES_ERROR;
  }

  return DEAL_RES_CONTINUE;
}

EDealRes sifCalcWalker(SNode *node, void *context) {
  if (QUERY_NODE_VALUE == nodeType(node) || QUERY_NODE_NODE_LIST == nodeType(node) || QUERY_NODE_COLUMN == nodeType(node)) {
    return DEAL_RES_CONTINUE;
  }
  SIFCtx *ctx = (SIFCtx *)context;
  if (QUERY_NODE_FUNCTION == nodeType(node)) {
    return sifWalkFunction(node, ctx);
  }
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(node)) {
    return sifWalkLogic(node, ctx);
  }
  if (QUERY_NODE_OPERATOR == nodeType(node)) {
    return sifWalkOper(node, ctx);
  }

  qError("invalid node type for index filter calculating, type:%d", nodeType(node));
  ctx->code = TSDB_CODE_QRY_INVALID_INPUT;
  return DEAL_RES_ERROR;
}

void sifFreeRes(SHashObj *res) {
  void *pIter = taosHashIterate(res, NULL);
  while (pIter) {
    SIFParam *p = pIter;
    if (p) {
      sifFreeParam(p);
    }
    pIter = taosHashIterate(res, pIter);
  }
  taosHashCleanup(res);
}
static int32_t sifCalculate(SNode *pNode, SIFParam *pDst) {
  if (pNode == NULL || pDst == NULL) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }
  int32_t code = 0;
  SIFCtx  ctx = {.code = 0};
  ctx.pRes = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (NULL == ctx.pRes) {
    qError("index-filter failed to taosHashInit");
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }
  nodesWalkExprPostOrder(pNode, sifCalcWalker, &ctx);
  if (ctx.code != TSDB_CODE_SUCCESS) {
    return ctx.code;
  }
  if (pDst) {
    SIFParam *res = (SIFParam *)taosHashGet(ctx.pRes, (void *)&pNode, POINTER_BYTES);
    if (res == NULL) {
      qError("no valid res in hash, node:(%p), type(%d)", (void *)&pNode, nodeType(pNode));
      return TSDB_CODE_QRY_APP_ERROR;
    }
    taosArrayAddAll(pDst->result, res->result);

    sifFreeParam(res);
    taosHashRemove(ctx.pRes, (void *)&pNode, POINTER_BYTES);
  }
  return TSDB_CODE_SUCCESS;
}

int32_t doFilterTag(const SNode *pFilterNode, SArray *result) {
  if (pFilterNode == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  SFilterInfo *filter = NULL;
  // todo move to the initialization function
  int32_t code = filterInitFromNode((SNode *)pFilterNode, &filter, 0);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SIFParam param = {0};
  code = sifCalculate((SNode *)pFilterNode, &param);

  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  taosArrayAddAll(result, param.result);
  sifFreeParam(&param);

  return code;
}

SIdxFltStatus idxGetFltStatus(SNode *pFilterNode) {
  if (pFilterNode == NULL) {
    return SFLT_NOT_INDEX;
  }
  // impl later
  return SFLT_ACCURATE_INDEX;
}
