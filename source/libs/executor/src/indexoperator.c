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

typedef struct SIFParam {
  SArray *  result;
  SHashObj *pFilter;
} SIFParam;
// construct tag filter operator later
static void destroyTagFilterOperatorInfo(void *param) {
  STagFilterOperatorInfo *pInfo = (STagFilterOperatorInfo *)param;
}

static void sifFreeParam(SIFParam *param) {
  if (param == NULL) return;
  taosArrayDestroy(param->result);
}

int32_t sifInitOperParams(SIFParam *params, SOperatorNode *node, SIFCtx *ctx) {
  int32_t code = 0;
  return code;
}
static int32_t sifExecFunction(SFunctionNode *node, SIFCtx *ctx, SIFParam *output) {
  qError("index-filter not support buildin function");
  return TSDB_CODE_SUCCESS;
}
static int32_t sifExecOper(SOperatorNode *node, SIFCtx *ctx, SIFParam *output) {
  SIFParam *params = NULL;

  return TSDB_CODE_SUCCESS;
}

static int32_t sifExecLogic(SLogicConditionNode *node, SIFCtx *ctx, SIFParam *output) { return TSDB_CODE_SUCCESS; }

static EDealRes sifWalkFunction(SNode *pNode, void *context) {
  // impl later
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
  if (QUERY_NODE_VALUE == nodeType(node) || QUERY_NODE_NODE_LIST == nodeType(node) ||
      QUERY_NODE_COLUMN == nodeType(node)) {
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
