#include "scalar.h"
#include "function.h"
#include "functionMgt.h"
#include "nodes.h"
#include "querynodes.h"
#include "sclInt.h"
#include "sclvector.h"
#include "tcommon.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "ttime.h"
#include "tudf.h"

int32_t scalarGetOperatorParamNum(EOperatorType type) {
  if (OP_TYPE_IS_NULL == type || OP_TYPE_IS_NOT_NULL == type || OP_TYPE_IS_TRUE == type ||
      OP_TYPE_IS_NOT_TRUE == type || OP_TYPE_IS_FALSE == type || OP_TYPE_IS_NOT_FALSE == type ||
      OP_TYPE_IS_UNKNOWN == type || OP_TYPE_IS_NOT_UNKNOWN == type || OP_TYPE_MINUS == type) {
    return 1;
  }

  return 2;
}

int32_t sclConvertToTsValueNode(int8_t precision, SValueNode *valueNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  SCL_CHECK_NULL(valueNode, code, lino, _return, TSDB_CODE_INVALID_PARA)

  char   *timeStr = valueNode->datum.p;
  int64_t value = 0;
  SCL_ERR_RET(convertStringToTimestamp(valueNode->node.resType.type, valueNode->datum.p, precision, &value));
  taosMemoryFreeClear(timeStr);
  valueNode->datum.i = value;
  valueNode->typeData = valueNode->datum.i;

  valueNode->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
  valueNode->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes;

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t sclCreateColumnInfoData(SDataType *pType, int32_t numOfRows, SScalarParam *pParam) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  SCL_CHECK_NULL(pType, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(pParam, code, lino, _return, TSDB_CODE_INVALID_PARA)

  SColumnInfoData *pColumnData = taosMemoryCalloc(1, sizeof(SColumnInfoData));
  SCL_CHECK_NULL(pColumnData, code, lino, _return, terrno)

  pColumnData->info.type = pType->type;
  pColumnData->info.bytes = pType->bytes;
  pColumnData->info.scale = pType->scale;
  pColumnData->info.precision = pType->precision;

  code = colInfoDataEnsureCapacity(pColumnData, numOfRows, true);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    colDataDestroy(pColumnData);
    taosMemoryFreeClear(pColumnData);
    return terrno;
  }

  pParam->columnData = pColumnData;
  pParam->colAlloced = true;
  pParam->numOfRows = numOfRows;

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t sclConvertValueToSclParam(SValueNode *pValueNode, SScalarParam *out, int32_t *overflow) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  SCL_CHECK_NULL(pValueNode, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(out, code, lino, _return, TSDB_CODE_INVALID_PARA)

  SScalarParam in = {.numOfRows = 1};
  SCL_ERR_JRET(sclCreateColumnInfoData(&pValueNode->node.resType, 1, &in));
  SCL_ERR_JRET(colDataSetVal(in.columnData, 0, nodesGetValueFromNode(pValueNode), false));
  SCL_ERR_JRET(colInfoDataEnsureCapacity(out->columnData, 1, true));
  SCL_ERR_JRET(vectorConvertSingleColImpl(&in, out, overflow, -1, -1));

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  sclFreeParam(&in);

  return code;
}

int32_t sclExtendResRows(SScalarParam *pDst, SScalarParam *pSrc, SArray *pBlockList) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SScalarParam   *pLeft = NULL;

  SCL_CHECK_NULL(pDst, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(pSrc, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(pBlockList, code, lino, _return, TSDB_CODE_INVALID_PARA)

  SSDataBlock    *pb = taosArrayGetP(pBlockList, 0);
  SCL_CHECK_NULL(pb, code, lino, _return, terrno)
  pLeft = taosMemoryCalloc(1, sizeof(SScalarParam));
  SCL_CHECK_NULL(pLeft, code, lino, _return, terrno)

  pLeft->numOfRows = pb->info.rows;

  if (pDst->numOfRows < pb->info.rows) {
    SCL_ERR_JRET(colInfoDataEnsureCapacity(pDst->columnData, pb->info.rows, true));
  }

  _bin_scalar_fn_t OperatorFn = getBinScalarOperatorFn(OP_TYPE_ASSIGN);
  SCL_CHECK_NULL(OperatorFn, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_ERR_JRET(OperatorFn(pLeft, pSrc, pDst, TSDB_ORDER_ASC));

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  taosMemoryFreeClear(pLeft);

  SCL_RET(code);
}

int32_t scalarGenerateSetFromList(void **data, void *pNode, uint32_t type) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SHashObj       *pObj = NULL;
  SScalarParam    out;
  SCL_CHECK_NULL(data, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(pNode, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_CONDITION((nodeType(pNode) == QUERY_NODE_NODE_LIST), code, lino, _return, TSDB_CODE_SCALAR_WRONG_NODE_TYPE);

  pObj = taosHashInit(256, taosGetDefaultHashFunction(type), true, false);
  SCL_CHECK_NULL(pObj, code, lino, _return, terrno)

  taosHashSetEqualFp(pObj, taosGetDefaultEqualFunction(type));

  SNodeListNode *nodeList = (SNodeListNode *)pNode;
  SListCell     *cell = nodeList->pNodeList->pHead;
  out.columnData = taosMemoryCalloc(1, sizeof(SColumnInfoData));
  SCL_CHECK_NULL(out.columnData, code, lino, _return, terrno)

  int32_t len = 0;
  void   *buf = NULL;

  for (int32_t i = 0; i < nodeList->pNodeList->length; ++i) {
    SCL_CHECK_CONDITION((nodeType(cell->pNode) == QUERY_NODE_VALUE), code, lino, _return, TSDB_CODE_SCALAR_WRONG_NODE_TYPE);
    SValueNode *valueNode = (SValueNode *)cell->pNode;

    if (valueNode->node.resType.type != type) {
      out.columnData->info.type = type;
      if (IS_VAR_DATA_TYPE(type)) {
        if (IS_VAR_DATA_TYPE(valueNode->node.resType.type)) {
          out.columnData->info.bytes = valueNode->node.resType.bytes * TSDB_NCHAR_SIZE;
        } else {
          out.columnData->info.bytes = 64 * TSDB_NCHAR_SIZE;
        }
      } else {
        out.columnData->info.bytes = tDataTypes[type].bytes;
      }

      int32_t overflow = 0;
      SCL_ERR_JRET(sclConvertValueToSclParam(valueNode, &out, &overflow));

      if (overflow) {
        cell = cell->pNext;
        continue;
      }

      if (IS_VAR_DATA_TYPE(type)) {
        buf = colDataGetVarData(out.columnData, 0);
        len = varDataTLen(buf);
      } else {
        len = tDataTypes[type].bytes;
        buf = out.columnData->pData;
      }
    } else {
      buf = nodesGetValueFromNode(valueNode);
      if (IS_VAR_DATA_TYPE(type)) {
        len = varDataTLen(buf);
      } else {
        len = valueNode->node.resType.bytes;
      }
    }

    if (taosHashPut(pObj, buf, (size_t)len, NULL, 0)) {
      sclError("taosHashPut to set failed");
      SCL_ERR_JRET(terrno);
    }

    colInfoDataCleanup(out.columnData, out.numOfRows);
    cell = cell->pNext;
  }

  *data = pObj;

  colDataDestroy(out.columnData);
  taosMemoryFreeClear(out.columnData);
  return TSDB_CODE_SUCCESS;

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  colDataDestroy(out.columnData);
  taosMemoryFreeClear(out.columnData);
  taosHashCleanup(pObj);
  SCL_RET(code);
}

void sclFreeRes(SHashObj *res) {
  SScalarParam *p = NULL;
  void         *pIter = taosHashIterate(res, NULL);
  while (pIter) {
    p = (SScalarParam *)pIter;

    if (p) {
      sclFreeParam(p);
    }
    pIter = taosHashIterate(res, pIter);
  }
  taosHashCleanup(res);
}

void sclFreeParam(SScalarParam *param) {
  if (NULL == param || !param->colAlloced) {
    return;
  }

  if (param->columnData != NULL) {
    colDataDestroy(param->columnData);
    taosMemoryFreeClear(param->columnData);
    param->columnData = NULL;
  }

  if (param->pHashFilter != NULL) {
    taosHashCleanup(param->pHashFilter);
    param->pHashFilter = NULL;
  }
}

void sclFreeParamList(SScalarParam *param, int32_t paramNum) {
  if (NULL == param) {
    return;
  }

  for (int32_t i = 0; i < paramNum; ++i) {
    SScalarParam *p = param + i;
    sclFreeParam(p);
  }

  taosMemoryFreeClear(param);
}

void sclDowngradeValueType(SValueNode *valueNode) {
  if (!valueNode) {
    return;
  }
  switch (valueNode->node.resType.type) {
    case TSDB_DATA_TYPE_BIGINT: {
      int8_t i8 = valueNode->datum.i;
      if (i8 == valueNode->datum.i) {
        valueNode->node.resType.type = TSDB_DATA_TYPE_TINYINT;
        *(int8_t *)&valueNode->typeData = i8;
        break;
      }
      int16_t i16 = valueNode->datum.i;
      if (i16 == valueNode->datum.i) {
        valueNode->node.resType.type = TSDB_DATA_TYPE_SMALLINT;
        *(int16_t *)&valueNode->typeData = i16;
        break;
      }
      int32_t i32 = valueNode->datum.i;
      if (i32 == valueNode->datum.i) {
        valueNode->node.resType.type = TSDB_DATA_TYPE_INT;
        *(int32_t *)&valueNode->typeData = i32;
        break;
      }
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      uint8_t u8 = valueNode->datum.i;
      if (u8 == valueNode->datum.i) {
        int8_t i8 = valueNode->datum.i;
        if (i8 == valueNode->datum.i) {
          valueNode->node.resType.type = TSDB_DATA_TYPE_TINYINT;
          *(int8_t *)&valueNode->typeData = i8;
        } else {
          valueNode->node.resType.type = TSDB_DATA_TYPE_UTINYINT;
          *(uint8_t *)&valueNode->typeData = u8;
        }
        break;
      }
      uint16_t u16 = valueNode->datum.i;
      if (u16 == valueNode->datum.i) {
        int16_t i16 = valueNode->datum.i;
        if (i16 == valueNode->datum.i) {
          valueNode->node.resType.type = TSDB_DATA_TYPE_SMALLINT;
          *(int16_t *)&valueNode->typeData = i16;
        } else {
          valueNode->node.resType.type = TSDB_DATA_TYPE_USMALLINT;
          *(uint16_t *)&valueNode->typeData = u16;
        }
        break;
      }
      uint32_t u32 = valueNode->datum.i;
      if (u32 == valueNode->datum.i) {
        int32_t i32 = valueNode->datum.i;
        if (i32 == valueNode->datum.i) {
          valueNode->node.resType.type = TSDB_DATA_TYPE_INT;
          *(int32_t *)&valueNode->typeData = i32;
        } else {
          valueNode->node.resType.type = TSDB_DATA_TYPE_UINT;
          *(uint32_t *)&valueNode->typeData = u32;
        }
        break;
      }
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      float f = valueNode->datum.d;
      if (FLT_EQUAL(f, valueNode->datum.d)) {
        valueNode->node.resType.type = TSDB_DATA_TYPE_FLOAT;
        *(float *)&valueNode->typeData = f;
        break;
      }
      break;
    }
    default:
      break;
  }
}

int32_t sclInitParam(SNode *node, SScalarParam *param, SScalarCtx *ctx, int32_t *rowNum) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;

  SCL_CHECK_NULL(node, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(param, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(ctx, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(rowNum, code, lino, _return, TSDB_CODE_INVALID_PARA)

  switch (nodeType(node)) {
    case QUERY_NODE_LEFT_VALUE: {
      SSDataBlock *pb = taosArrayGetP(ctx->pBlockList, 0);
      SCL_CHECK_NULL(pb, code, lino, _return, terrno)
      param->numOfRows = pb->info.rows;
      break;
    }
    case QUERY_NODE_VALUE: {
      SValueNode *valueNode = (SValueNode *)node;

      if (param->columnData != NULL) {
        sclError("columnData should be NULL");
        SCL_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
      }
      param->numOfRows = 1;
      SCL_ERR_RET(sclCreateColumnInfoData(&valueNode->node.resType, 1, param));
      if (TSDB_DATA_TYPE_NULL == valueNode->node.resType.type || valueNode->isNull) {
        colDataSetNULL(param->columnData, 0);
      } else {
        SCL_ERR_RET(colDataSetVal(param->columnData, 0, nodesGetValueFromNode(valueNode), false));
      }
      break;
    }
    case QUERY_NODE_NODE_LIST: {
      SNodeListNode *nodeList = (SNodeListNode *)node;
      if (LIST_LENGTH(nodeList->pNodeList) <= 0) {
        sclError("invalid length in nodeList, length:%d", LIST_LENGTH(nodeList->pNodeList));
        SCL_RET(TSDB_CODE_QRY_INVALID_INPUT);
      }

      int32_t type = vectorGetConvertType(ctx->type.selfType, ctx->type.peerType);
      if (type == 0) {
        type = nodeList->node.resType.type;
      }

      SCL_ERR_RET(scalarGenerateSetFromList((void **)&param->pHashFilter, node, type));
      param->hashValueType = type;
      param->colAlloced = true;
      if (taosHashPut(ctx->pRes, &node, POINTER_BYTES, param, sizeof(*param))) {
        taosHashCleanup(param->pHashFilter);
        param->pHashFilter = NULL;
        sclError("taosHashPut nodeList failed, size:%d", (int32_t)sizeof(*param));
        return terrno;
      }
      param->colAlloced = false;
      break;
    }
    case QUERY_NODE_COLUMN: {
      if (NULL == ctx->pBlockList) {
        sclError("invalid node type for constant calculating, type:%d, src:%p", nodeType(node), ctx->pBlockList);
        SCL_ERR_RET(TSDB_CODE_APP_ERROR);
      }

      SColumnNode *ref = (SColumnNode *)node;

      int32_t index = -1;
      for (int32_t i = 0; i < taosArrayGetSize(ctx->pBlockList); ++i) {
        SSDataBlock *pb = taosArrayGetP(ctx->pBlockList, i);
        SCL_CHECK_NULL(pb, code, lino, _return, terrno)
        if (pb->info.id.blockId == ref->dataBlockId) {
          index = i;
          break;
        }
      }

      if (index == -1) {
        sclError("column tupleId is too big, tupleId:%d, dataBlockNum:%d", ref->dataBlockId,
                 (int32_t)taosArrayGetSize(ctx->pBlockList));
        SCL_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
      }

      SSDataBlock *block = *(SSDataBlock **)taosArrayGet(ctx->pBlockList, index);
      SCL_CHECK_NULL(block, code, lino, _return, terrno)

      if (ref->slotId >= taosArrayGetSize(block->pDataBlock)) {
        sclError("column slotId is too big, slodId:%d, dataBlockSize:%d", ref->slotId,
                 (int32_t)taosArrayGetSize(block->pDataBlock));
        SCL_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
      }

      SColumnInfoData *columnData = (SColumnInfoData *)taosArrayGet(block->pDataBlock, ref->slotId);
#if TAG_FILTER_DEBUG
      qDebug("tagfilter column info, slotId:%d, colId:%d, type:%d", ref->slotId, columnData->info.colId,
             columnData->info.type);
#endif
      param->numOfRows = block->info.rows;
      param->columnData = columnData;
      break;
    }
    case QUERY_NODE_FUNCTION:
    case QUERY_NODE_OPERATOR:
    case QUERY_NODE_LOGIC_CONDITION:
    case QUERY_NODE_CASE_WHEN: {
      SScalarParam *res = (SScalarParam *)taosHashGet(ctx->pRes, &node, POINTER_BYTES);
      if (NULL == res) {
        sclError("no result for node, type:%d, node:%p", nodeType(node), node);
        SCL_ERR_RET(TSDB_CODE_APP_ERROR);
      }
      *param = *res;
      param->colAlloced = false;
      break;
    }
    default:
      break;
  }

  if (param->numOfRows > *rowNum) {
    if ((1 != param->numOfRows) && (1 < *rowNum)) {
      sclError("different row nums, rowNum:%d, newRowNum:%d", *rowNum, param->numOfRows);
      SCL_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
    }

    *rowNum = param->numOfRows;
  }

  param->param = ctx->param;
_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t sclInitParamList(SScalarParam **pParams, SNodeList *pParamList, SScalarCtx *ctx, int32_t *paramNum,
                         int32_t *rowNum) {
  int32_t         code = 0;
  int32_t         lino = 0;
  SScalarParam   *paramList = NULL;
  SCL_CHECK_NULL(pParams, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(ctx, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(paramNum, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(rowNum, code, lino, _return, TSDB_CODE_INVALID_PARA)

  if (NULL == pParamList) {
    if (ctx->pBlockList) {
      SSDataBlock *pBlock = taosArrayGetP(ctx->pBlockList, 0);
      if (NULL == pBlock) {
        SCL_ERR_RET(TSDB_CODE_OUT_OF_RANGE);
      }
      *rowNum = pBlock->info.rows;
    } else {
      *rowNum = 1;
    }

    *paramNum = 1;
  } else {
    *paramNum = pParamList->length;
  }

  paramList = taosMemoryCalloc(*paramNum, sizeof(SScalarParam));
  if (NULL == paramList) {
    sclError("calloc %d failed", (int32_t)((*paramNum) * sizeof(SScalarParam)));
    SCL_ERR_RET(terrno);
  }

  if (pParamList) {
    SNode  *tnode = NULL;
    int32_t i = 0;
    if (SCL_IS_CONST_CALC(ctx)) {
      WHERE_EACH(tnode, pParamList) {
        if (!SCL_IS_CONST_NODE(tnode)) {
          WHERE_NEXT;
        } else {
          SCL_ERR_JRET(sclInitParam(tnode, &paramList[i], ctx, rowNum));
          ERASE_NODE(pParamList);
        }

        ++i;
      }
    } else {
      FOREACH(tnode, pParamList) {
        SCL_ERR_JRET(sclInitParam(tnode, &paramList[i], ctx, rowNum));
        ++i;
      }
    }
  } else {
    paramList[0].numOfRows = *rowNum;
  }

  if (0 == *rowNum) {
    taosMemoryFreeClear(paramList);
  }

  *pParams = paramList;
  return TSDB_CODE_SUCCESS;

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  taosMemoryFreeClear(paramList);
  SCL_RET(code);
}

int32_t sclGetNodeType(SNode *pNode, SScalarCtx *ctx, int32_t *type) {
  int32_t         code = 0;
  int32_t         lino = 0;
  SCL_CHECK_NULL(type, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(ctx, code, lino, _return, TSDB_CODE_INVALID_PARA)
  if (NULL == pNode) {
    *type = -1;
    return TSDB_CODE_SUCCESS;
  }
  switch ((int)nodeType(pNode)) {
    case QUERY_NODE_VALUE: {
      SValueNode *valueNode = (SValueNode *)pNode;
      *type = valueNode->node.resType.type;
      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_NODE_LIST: {
      SNodeListNode *nodeList = (SNodeListNode *)pNode;
      *type = nodeList->node.resType.type;
      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_COLUMN: {
      SColumnNode *colNode = (SColumnNode *)pNode;
      *type = colNode->node.resType.type;
      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_FUNCTION:
    case QUERY_NODE_OPERATOR:
    case QUERY_NODE_LOGIC_CONDITION: {
      SScalarParam *res = (SScalarParam *)taosHashGet(ctx->pRes, &pNode, POINTER_BYTES);
      if (NULL == res) {
        sclError("no result for node, type:%d, node:%p", nodeType(pNode), pNode);
        SCL_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
      }
      *type = (int32_t)(res->columnData->info.type);
      return TSDB_CODE_SUCCESS;
    }
  }

  *type = -1;
_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t sclSetOperatorValueType(SOperatorNode *node, SScalarCtx *ctx) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SCL_CHECK_NULL(node, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(ctx, code, lino, _return, TSDB_CODE_INVALID_PARA)
  ctx->type.opResType = node->node.resType.type;
  SCL_ERR_RET(sclGetNodeType(node->pLeft, ctx, &(ctx->type.selfType)));
  SCL_ERR_RET(sclGetNodeType(node->pRight, ctx, &(ctx->type.peerType)));
_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  SCL_RET(code);
}

int32_t sclInitOperatorParams(SScalarParam **pParams, SOperatorNode *node, SScalarCtx *ctx, int32_t *rowNum) {
  int32_t         code = 0;
  int32_t         lino = 0;
  int32_t         paramNum = 0;
  SScalarParam   *paramList = NULL;
  SCL_CHECK_NULL(node, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(ctx, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(pParams, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(rowNum, code, lino, _return, TSDB_CODE_INVALID_PARA)

  paramNum = scalarGetOperatorParamNum(node->opType);
  if (NULL == node->pLeft || (paramNum == 2 && NULL == node->pRight)) {
    sclError("invalid operation node, left:%p, right:%p", node->pLeft, node->pRight);
    SCL_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  paramList = taosMemoryCalloc(paramNum, sizeof(SScalarParam));
  SCL_CHECK_NULL(paramList, code, lino, _return, terrno)

  SCL_ERR_JRET(sclSetOperatorValueType(node, ctx));

  SCL_ERR_JRET(sclInitParam(node->pLeft, &paramList[0], ctx, rowNum));
  if (paramNum > 1) {
    TSWAP(ctx->type.selfType, ctx->type.peerType);
    SCL_ERR_JRET(sclInitParam(node->pRight, &paramList[1], ctx, rowNum));
  }

  *pParams = paramList;
  return TSDB_CODE_SUCCESS;

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  sclFreeParamList(paramList, paramNum);
  SCL_RET(code);
}

int32_t sclGetNodeRes(SNode *node, SScalarCtx *ctx, SScalarParam **res) {
  int32_t         code = 0;
  int32_t         lino = 0;
  SCL_CHECK_NULL(node, code, lino, _return, TSDB_CODE_SUCCESS)
  SCL_CHECK_NULL(ctx, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(res, code, lino, _return, TSDB_CODE_INVALID_PARA)
  int32_t rowNum = 0;
  *res = taosMemoryCalloc(1, sizeof(**res));
  SCL_CHECK_NULL(*res, code, lino, _return, terrno)
  SCL_ERR_RET(sclInitParam(node, *res, ctx, &rowNum));

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t sclWalkCaseWhenList(SScalarCtx *ctx, SNodeList *UNUSED_PARAM(pList), struct SListCell *pCell, SScalarParam *pCase,
                            SScalarParam *pElse, SScalarParam *pComp, SScalarParam *output, int32_t rowIdx,
                            int32_t totalRows, bool *complete) {
  int32_t        code = 0;
  int32_t        lino = 0;
  SNode         *node = NULL;
  SWhenThenNode *pWhenThen = NULL;
  SScalarParam  *pWhen = NULL;
  SScalarParam  *pThen = NULL;
  SCL_CHECK_NULL(ctx, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(pCase, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(pComp, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(output, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(complete, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(pComp->columnData, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(output->columnData, code, lino, _return, TSDB_CODE_INVALID_PARA)

  for (SListCell *cell = pCell; (NULL != cell ? (node = cell->pNode, true) : (node = NULL, false));
       cell = cell->pNext) {
    SCL_CHECK_CONDITION((nodeType(node) == QUERY_NODE_WHEN_THEN), code, lino, _return, TSDB_CODE_SCALAR_WRONG_NODE_TYPE);
    pWhenThen = (SWhenThenNode *)node;

    SCL_ERR_JRET(sclGetNodeRes(pWhenThen->pWhen, ctx, &pWhen));
    SCL_ERR_JRET(sclGetNodeRes(pWhenThen->pThen, ctx, &pThen));

    SCL_ERR_JRET(vectorCompareImpl(pCase, pWhen, pComp, rowIdx, 1, TSDB_ORDER_ASC, OP_TYPE_EQUAL));

    bool *equal = (bool *)colDataGetData(pComp->columnData, rowIdx);
    if (*equal) {
      bool  isNull = colDataIsNull_s(pThen->columnData, (pThen->numOfRows > 1 ? rowIdx : 0));
      char *pData = isNull ? NULL : colDataGetData(pThen->columnData, (pThen->numOfRows > 1 ? rowIdx : 0));
      SCL_ERR_JRET(colDataSetVal(output->columnData, rowIdx, pData, isNull));

      if (0 == rowIdx && 1 == pCase->numOfRows && 1 == pWhen->numOfRows && 1 == pThen->numOfRows && totalRows > 1) {
        SCL_ERR_JRET(sclExtendResRows(output, output, ctx->pBlockList));
        *complete = true;
      }

      goto _return;
    }
    sclFreeParam(pWhen);
    sclFreeParam(pThen);
    taosMemoryFreeClear(pWhen);
    taosMemoryFreeClear(pThen);
  }

  if (pElse) {
    bool  isNull = colDataIsNull_s(pElse->columnData, (pElse->numOfRows > 1 ? rowIdx : 0));
    char *pData = isNull ? NULL : colDataGetData(pElse->columnData, (pElse->numOfRows > 1 ? rowIdx : 0));
    SCL_ERR_JRET(colDataSetVal(output->columnData, rowIdx, pData, isNull));

    if (0 == rowIdx && 1 == pCase->numOfRows && 1 == pElse->numOfRows && totalRows > 1) {
      SCL_ERR_JRET(sclExtendResRows(output, output, ctx->pBlockList));
      *complete = true;
    }

    goto _return;
  }

  SCL_ERR_JRET(colDataSetVal(output->columnData, rowIdx, NULL, true));

  if (0 == rowIdx && 1 == pCase->numOfRows && totalRows > 1) {
    SCL_ERR_JRET(sclExtendResRows(output, output, ctx->pBlockList));
    *complete = true;
  }

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  sclFreeParam(pWhen);
  sclFreeParam(pThen);
  taosMemoryFreeClear(pWhen);
  taosMemoryFreeClear(pThen);

  SCL_RET(code);
}

int32_t sclWalkWhenList(SScalarCtx *ctx, SNodeList *UNUSED_PARAM(pList), struct SListCell *pCell, SScalarParam *pElse,
                        SScalarParam *output, int32_t rowIdx, int32_t totalRows, bool *complete, bool preSingle) {
  int32_t        code = 0;
  int32_t        lino = 0;
  SNode         *node = NULL;
  SWhenThenNode *pWhenThen = NULL;
  SScalarParam  *pWhen = NULL;
  SScalarParam  *pThen = NULL;
  SCL_CHECK_NULL(ctx, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(output, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(complete, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(output->columnData, code, lino, _return, TSDB_CODE_INVALID_PARA)

  for (SListCell *cell = pCell; (NULL != cell ? (node = cell->pNode, true) : (node = NULL, false));
       cell = cell->pNext) {
    SCL_CHECK_CONDITION((nodeType(node) == QUERY_NODE_WHEN_THEN), code, lino, _return, TSDB_CODE_SCALAR_WRONG_NODE_TYPE);
    pWhenThen = (SWhenThenNode *)node;
    pWhen = NULL;
    pThen = NULL;

    SCL_ERR_JRET(sclGetNodeRes(pWhenThen->pWhen, ctx, &pWhen));
    SCL_ERR_JRET(sclGetNodeRes(pWhenThen->pThen, ctx, &pThen));

    bool *whenValue = (bool *)colDataGetData(pWhen->columnData, (pWhen->numOfRows > 1 ? rowIdx : 0));

    if (*whenValue) {
      bool  isNull = colDataIsNull_s(pThen->columnData, (pThen->numOfRows > 1 ? rowIdx : 0));
      char *pData = isNull ? NULL : colDataGetData(pThen->columnData, (pThen->numOfRows > 1 ? rowIdx : 0));
      SCL_ERR_JRET(colDataSetVal(output->columnData, rowIdx, pData, isNull));

      if (preSingle && 0 == rowIdx && 1 == pWhen->numOfRows && 1 == pThen->numOfRows && totalRows > 1) {
        SCL_ERR_JRET(sclExtendResRows(output, output, ctx->pBlockList));
        *complete = true;
      }

      goto _return;
    }

    sclFreeParam(pWhen);
    sclFreeParam(pThen);
    taosMemoryFreeClear(pWhen);
    taosMemoryFreeClear(pThen);
  }

  if (pElse) {
    bool  isNull = colDataIsNull_s(pElse->columnData, (pElse->numOfRows > 1 ? rowIdx : 0));
    char *pData = isNull ? NULL : colDataGetData(pElse->columnData, (pElse->numOfRows > 1 ? rowIdx : 0));
    SCL_ERR_JRET(colDataSetVal(output->columnData, rowIdx, pData, isNull));

    if (preSingle && 0 == rowIdx && 1 == pElse->numOfRows && totalRows > 1) {
      SCL_ERR_JRET(sclExtendResRows(output, output, ctx->pBlockList));
      *complete = true;
    }

    goto _return;
  }

  SCL_ERR_JRET(colDataSetVal(output->columnData, rowIdx, NULL, true));

  if (preSingle && 0 == rowIdx && totalRows > 1) {
    SCL_ERR_JRET(sclExtendResRows(output, output, ctx->pBlockList));
    *complete = true;
  }

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  sclFreeParam(pWhen);
  sclFreeParam(pThen);
  taosMemoryFreeClear(pWhen);
  taosMemoryFreeClear(pThen);

  SCL_RET(code);
}

int32_t sclExecFunction(SFunctionNode *node, SScalarCtx *ctx, SScalarParam *output) {
  SScalarParam *params = NULL;
  int32_t       rowNum = 0;
  int32_t       paramNum = 0;
  int32_t       code = 0;
  int32_t       lino = 0;
  SCL_CHECK_NULL(node, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(ctx, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(output, code, lino, _return, TSDB_CODE_INVALID_PARA)

  SCL_ERR_RET(sclInitParamList(&params, node->pParameterList, ctx, &paramNum, &rowNum));

  if (fmIsUserDefinedFunc(node->funcId)) {
    code = callUdfScalarFunc(node->functionName, params, paramNum, output);
    if (code != 0) {
      sclError("fmExecFunction error. callUdfScalarFunc. function name: %s, udf code:%d", node->functionName, code);
      goto _return;
    }
  } else {
    SScalarFuncExecFuncs ffpSet = {0};
    code = fmGetScalarFuncExecFuncs(node->funcId, &ffpSet);
    if (code) {
      sclError("fmGetFuncExecFuncs failed, funcId:%d, code:%s", node->funcId, tstrerror(code));
      SCL_ERR_JRET(code);
    }

    SCL_ERR_JRET(sclCreateColumnInfoData(&node->node.resType, rowNum, output));

    if (rowNum == 0) {
      goto _return;
    }

    code = (*ffpSet.process)(params, paramNum, output);
    if (code) {
      sclError("scalar function exec failed, funcId:%d, code:%s", node->funcId, tstrerror(code));
      SCL_ERR_JRET(code);
    }
  }

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  sclFreeParamList(params, paramNum);
  SCL_RET(code);
}

int32_t sclExecLogic(SLogicConditionNode *node, SScalarCtx *ctx, SScalarParam *output) {
  SScalarParam *params = NULL;
  int32_t       rowNum = 0;
  int32_t       paramNum = 0;
  int32_t       code = 0;
  int32_t       lino = 0;
  SCL_CHECK_NULL(node, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(ctx, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(output, code, lino, _return, TSDB_CODE_INVALID_PARA)

  if (NULL == node->pParameterList || node->pParameterList->length <= 0) {
    sclError("invalid logic parameter list, list:%p, paramNum:%d", node->pParameterList,
             node->pParameterList ? node->pParameterList->length : 0);
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

  SCL_ERR_RET(sclInitParamList(&params, node->pParameterList, ctx, &paramNum, &rowNum));
  if (NULL == params) {
    output->numOfRows = 0;
    return TSDB_CODE_SUCCESS;
  }

  int32_t type = node->node.resType.type;
  output->numOfRows = rowNum;

  SDataType t = {.type = type, .bytes = tDataTypes[type].bytes};
  SCL_ERR_JRET(sclCreateColumnInfoData(&t, rowNum, output));

  int32_t numOfQualified = 0;

  bool value = false;
  bool complete = true;
  for (int32_t i = 0; i < rowNum; ++i) {
    complete = true;
    for (int32_t m = 0; m < paramNum; ++m) {
      if (NULL == params[m].columnData) {
        complete = false;
        continue;
      }

      // 1=1 and tag_column = 1
      int32_t ind = (i >= params[m].numOfRows) ? (params[m].numOfRows - 1) : i;
      char   *p = colDataGetData(params[m].columnData, ind);

      GET_TYPED_DATA(value, bool, params[m].columnData->info.type, p);

      if (LOGIC_COND_TYPE_AND == node->condType && (false == value)) {
        complete = true;
        break;
      } else if (LOGIC_COND_TYPE_OR == node->condType && value) {
        complete = true;
        break;
      } else if (LOGIC_COND_TYPE_NOT == node->condType) {
        value = !value;
      }
    }

    if (complete) {
      SCL_ERR_JRET(colDataSetVal(output->columnData, i, (char *)&value, false));
      if (value) {
        numOfQualified++;
      }
    }
  }

  if (SCL_IS_CONST_CALC(ctx) && (false == complete)) {
    sclFreeParam(output);
    output->numOfRows = 0;
  }

  output->numOfQualified = numOfQualified;

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  sclFreeParamList(params, paramNum);
  SCL_RET(code);
}

int32_t sclExecOperator(SOperatorNode *node, SScalarCtx *ctx, SScalarParam *output) {
  SScalarParam *params = NULL;
  int32_t       rowNum = 0;
  int32_t       code = 0;
  int32_t       paramNum = scalarGetOperatorParamNum(node->opType);
  int32_t       lino = 0;
  SCL_CHECK_NULL(node, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(ctx, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(output, code, lino, _return, TSDB_CODE_INVALID_PARA)

  // json not support in in operator
  if (nodeType(node->pLeft) == QUERY_NODE_VALUE) {
    SValueNode *valueNode = (SValueNode *)node->pLeft;
    if (valueNode->node.resType.type == TSDB_DATA_TYPE_JSON &&
        (node->opType == OP_TYPE_IN || node->opType == OP_TYPE_NOT_IN)) {
      SCL_RET(TSDB_CODE_QRY_JSON_IN_ERROR);
    }
  }

  SCL_ERR_JRET(sclInitOperatorParams(&params, node, ctx, &rowNum));
  if (output->columnData == NULL) {
    SCL_ERR_JRET(sclCreateColumnInfoData(&node->node.resType, rowNum, output));
  }

  _bin_scalar_fn_t OperatorFn = getBinScalarOperatorFn(node->opType);
  SCL_CHECK_NULL(OperatorFn, code, lino, _return, TSDB_CODE_INVALID_PARA)

  SScalarParam *pLeft = &params[0];
  SScalarParam *pRight = paramNum > 1 ? &params[1] : NULL;

  terrno = TSDB_CODE_SUCCESS;
  SCL_ERR_JRET(OperatorFn(pLeft, pRight, output, TSDB_ORDER_ASC));

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  sclFreeParamList(params, paramNum);
  SCL_RET(code);
}

int32_t sclExecCaseWhen(SCaseWhenNode *node, SScalarCtx *ctx, SScalarParam *output) {
  int32_t       code = 0;
  SScalarParam *pCase = NULL;
  SScalarParam *pElse = NULL;
  SScalarParam *pWhen = NULL;
  SScalarParam *pThen = NULL;
  SScalarParam  comp = {0};
  int32_t       rowNum = 1;
  bool          complete = false;
  int32_t       lino = 0;

  SCL_CHECK_NULL(node, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(ctx, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(output, code, lino, _return, TSDB_CODE_INVALID_PARA)

  if (NULL == node->pWhenThenList || node->pWhenThenList->length <= 0) {
    sclError("invalid whenThen list");
    SCL_ERR_RET(TSDB_CODE_INVALID_PARA);
  }

  if (ctx->pBlockList) {
    SSDataBlock *pb = taosArrayGetP(ctx->pBlockList, 0);
    SCL_CHECK_NULL(pb, code, lino, _return, terrno)
    rowNum = pb->info.rows;
    output->numOfRows = pb->info.rows;
  }

  SCL_ERR_JRET(sclCreateColumnInfoData(&node->node.resType, rowNum, output));

  SCL_ERR_JRET(sclGetNodeRes(node->pCase, ctx, &pCase));
  SCL_ERR_JRET(sclGetNodeRes(node->pElse, ctx, &pElse));

  SDataType compType = {0};
  compType.type = TSDB_DATA_TYPE_BOOL;
  compType.bytes = tDataTypes[compType.type].bytes;

  SCL_ERR_JRET(sclCreateColumnInfoData(&compType, rowNum, &comp));

  SNode         *tnode = NULL;
  SCL_CHECK_NULL(node->pWhenThenList->pHead, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(node->pWhenThenList->pHead->pNode, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_CONDITION((nodeType(node->pWhenThenList->pHead->pNode) == QUERY_NODE_WHEN_THEN), code, lino, _return,
                      TSDB_CODE_SCALAR_WRONG_NODE_TYPE);
  SWhenThenNode *pWhenThen = (SWhenThenNode *)node->pWhenThenList->pHead->pNode;

  SCL_ERR_JRET(sclGetNodeRes(pWhenThen->pWhen, ctx, &pWhen));
  SCL_ERR_JRET(sclGetNodeRes(pWhenThen->pThen, ctx, &pThen));
  if (NULL == pWhen || NULL == pThen) {
    sclError("invalid when/then in whenThen list");
    SCL_ERR_JRET(TSDB_CODE_INVALID_PARA);
  }

  if (pCase) {
    SCL_ERR_JRET(vectorCompare(pCase, pWhen, &comp, TSDB_ORDER_ASC, OP_TYPE_EQUAL));

    for (int32_t i = 0; i < rowNum; ++i) {
      bool *equal = (bool *)colDataGetData(comp.columnData, (comp.numOfRows > 1 ? i : 0));
      if (*equal) {
        SCL_ERR_JRET(colDataSetVal(output->columnData, i,
                                   colDataGetData(pThen->columnData, (pThen->numOfRows > 1 ? i : 0)),
                                   colDataIsNull_s(pThen->columnData, (pThen->numOfRows > 1 ? i : 0))));
        if (0 == i && 1 == pCase->numOfRows && 1 == pWhen->numOfRows && 1 == pThen->numOfRows && rowNum > 1) {
          SCL_ERR_JRET(sclExtendResRows(output, output, ctx->pBlockList));
          break;
        }
      } else {
        SCL_ERR_JRET(sclWalkCaseWhenList(ctx, node->pWhenThenList, node->pWhenThenList->pHead->pNext, pCase, pElse,
                                         &comp, output, i, rowNum, &complete));
        if (complete) {
          break;
        }
      }
    }
  } else {
    for (int32_t i = 0; i < rowNum; ++i) {
      bool *whenValue = (bool *)colDataGetData(pWhen->columnData, (pWhen->numOfRows > 1 ? i : 0));
      if (*whenValue) {
        SCL_ERR_JRET(colDataSetVal(output->columnData, i,
                                   colDataGetData(pThen->columnData, (pThen->numOfRows > 1 ? i : 0)),
                                   colDataIsNull_s(pThen->columnData, (pThen->numOfRows > 1 ? i : 0))));
        if (0 == i && 1 == pWhen->numOfRows && 1 == pThen->numOfRows && rowNum > 1) {
          SCL_ERR_JRET(sclExtendResRows(output, output, ctx->pBlockList));
          break;
        }
      } else {
        SCL_ERR_JRET(sclWalkWhenList(ctx, node->pWhenThenList, node->pWhenThenList->pHead->pNext, pElse, output, i,
                                     rowNum, &complete, (pWhen->numOfRows == 1 && pThen->numOfRows == 1)));
        if (complete) {
          break;
        }
      }
    }
  }

  sclFreeParam(pCase);
  sclFreeParam(pElse);
  sclFreeParam(&comp);
  sclFreeParam(pWhen);
  sclFreeParam(pThen);
  taosMemoryFreeClear(pCase);
  taosMemoryFreeClear(pElse);
  taosMemoryFreeClear(pWhen);
  taosMemoryFreeClear(pThen);

  return TSDB_CODE_SUCCESS;

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  sclFreeParam(pCase);
  sclFreeParam(pElse);
  sclFreeParam(&comp);
  sclFreeParam(pWhen);
  sclFreeParam(pThen);
  sclFreeParam(output);
  taosMemoryFreeClear(pCase);
  taosMemoryFreeClear(pElse);
  taosMemoryFreeClear(pWhen);
  taosMemoryFreeClear(pThen);

  SCL_RET(code);
}

EDealRes sclRewriteNullInOptr(SNode **pNode, SScalarCtx *ctx, EOperatorType opType) {
  if (NULL == pNode || NULL == ctx) {
    sclError("sclRewriteNullInOptr get null input param");
    return DEAL_RES_ERROR;
  }

  if (opType <= OP_TYPE_CALC_MAX) {
    SValueNode *res = NULL;
    ctx->code = nodesMakeNode(QUERY_NODE_VALUE, (SNode **)&res);
    if (NULL == res) {
      sclError("make value node failed");
      return DEAL_RES_ERROR;
    }

    res->node.resType.type = TSDB_DATA_TYPE_NULL;

    nodesDestroyNode(*pNode);
    *pNode = (SNode *)res;
  } else {
    SValueNode *res = NULL;
    ctx->code = nodesMakeNode(QUERY_NODE_VALUE, (SNode **)&res);
    if (NULL == res) {
      sclError("make value node failed");
      return DEAL_RES_ERROR;
    }

    res->node.resType.type = TSDB_DATA_TYPE_BOOL;
    res->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
    res->datum.b = false;

    nodesDestroyNode(*pNode);
    *pNode = (SNode *)res;
  }

  return DEAL_RES_CONTINUE;
}

EDealRes sclAggFuncWalker(SNode *pNode, void *pContext) {
  if (NULL == pNode || NULL == pContext) {
    sclError("sclAggFuncWalker get null input param");
    return DEAL_RES_ERROR;
  }

  if (QUERY_NODE_FUNCTION == nodeType(pNode)) {
    SFunctionNode *pFunc = (SFunctionNode *)pNode;
    *(bool *)pContext = fmIsAggFunc(pFunc->funcId);
    if (*(bool *)pContext) {
      return DEAL_RES_END;
    }
  }

  return DEAL_RES_CONTINUE;
}

bool sclContainsAggFuncNode(SNode *pNode) {
  bool aggFunc = false;
  nodesWalkExpr(pNode, sclAggFuncWalker, (void *)&aggFunc);
  return aggFunc;
}

static uint8_t sclGetOpValueNodeTsPrecision(SNode *pLeft, SNode *pRight) {
  if (NULL == pLeft || NULL == pRight) {
    return 0;
  }
  uint8_t lPrec = ((SExprNode *)pLeft)->resType.precision;
  uint8_t rPrec = ((SExprNode *)pRight)->resType.precision;

  uint8_t lType = ((SExprNode *)pLeft)->resType.type;
  uint8_t rType = ((SExprNode *)pRight)->resType.type;

  if (TSDB_DATA_TYPE_TIMESTAMP == lType && TSDB_DATA_TYPE_TIMESTAMP == rType) {
    return TMAX(lPrec, rPrec);
  } else if (TSDB_DATA_TYPE_TIMESTAMP == lType && TSDB_DATA_TYPE_TIMESTAMP != rType) {
    return lPrec;
  } else if (TSDB_DATA_TYPE_TIMESTAMP == rType && TSDB_DATA_TYPE_TIMESTAMP != lType) {
    return rPrec;
  }

  return 0;
}

int32_t sclConvertOpValueNodeTs(SOperatorNode *node) {
  if (node->pLeft && SCL_IS_VAR_VALUE_NODE(node->pLeft)) {
    if (node->pRight && (TSDB_DATA_TYPE_TIMESTAMP == ((SExprNode *)node->pRight)->resType.type)) {
      SCL_ERR_RET(
          sclConvertToTsValueNode(sclGetOpValueNodeTsPrecision(node->pLeft, node->pRight), (SValueNode *)node->pLeft));
    }
  } else if (node->pRight && SCL_IS_NOTNULL_CONST_NODE(node->pRight)) {
    if (node->pLeft && (TSDB_DATA_TYPE_TIMESTAMP == ((SExprNode *)node->pLeft)->resType.type)) {
      if (SCL_IS_VAR_VALUE_NODE(node->pRight)) {
        SCL_ERR_RET(sclConvertToTsValueNode(sclGetOpValueNodeTsPrecision(node->pLeft, node->pRight),
                                            (SValueNode *)node->pRight));
      } else if (QUERY_NODE_NODE_LIST == node->pRight->type) {
        SNode *pNode;
        FOREACH(pNode, ((SNodeListNode *)node->pRight)->pNodeList) {
          if (SCL_IS_VAR_VALUE_NODE(pNode)) {
            SCL_ERR_RET(sclConvertToTsValueNode(sclGetOpValueNodeTsPrecision(node->pLeft, pNode), (SValueNode *)pNode));
          }
        }
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t sclConvertCaseWhenValueNodeTs(SCaseWhenNode *node) {
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       lino = 0;

  SCL_CHECK_NULL(node, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(node->pCase, code, lino, _return, TSDB_CODE_SUCCESS)

  SCL_CHECK_CONDITION(nodesIsExprNode(node->pCase), code, lino, _return, TSDB_CODE_SCALAR_WRONG_NODE_TYPE)
  if (SCL_IS_VAR_VALUE_NODE(node->pCase)) {
    SNode *pNode;
    FOREACH(pNode, node->pWhenThenList) {
      SCL_CHECK_CONDITION((nodeType(pNode) == QUERY_NODE_WHEN_THEN), code, lino, _return, TSDB_CODE_SCALAR_WRONG_NODE_TYPE);
      SCL_CHECK_CONDITION(nodesIsExprNode(((SWhenThenNode *)pNode)->pWhen), code, lino, _return, TSDB_CODE_SCALAR_WRONG_NODE_TYPE);
      SExprNode *pExpr = (SExprNode *)((SWhenThenNode *)pNode)->pWhen;
      if (TSDB_DATA_TYPE_TIMESTAMP == pExpr->resType.type) {
        SCL_ERR_RET(sclConvertToTsValueNode(pExpr->resType.precision, (SValueNode *)node->pCase));
        break;
      }
    }
  } else if (TSDB_DATA_TYPE_TIMESTAMP == ((SExprNode *)node->pCase)->resType.type) {
    SNode *pNode;
    FOREACH(pNode, node->pWhenThenList) {
      if (SCL_IS_VAR_VALUE_NODE(((SWhenThenNode *)pNode)->pWhen)) {
        SCL_ERR_RET(sclConvertToTsValueNode(((SExprNode *)node->pCase)->resType.precision,
                                            (SValueNode *)((SWhenThenNode *)pNode)->pWhen));
      }
    }
  }
_return:
  return code;
}

EDealRes sclRewriteNonConstOperator(SNode **pNode, SScalarCtx *ctx) {
  if (NULL == pNode || NULL == *pNode || NULL == ctx) {
    sclError("sclRewriteNonConstOperator get null input param");
    return DEAL_RES_ERROR;
  }
  SOperatorNode *node = (SOperatorNode *)*pNode;
  int32_t        code = 0;

  if (node->pLeft && (QUERY_NODE_VALUE == nodeType(node->pLeft))) {
    SValueNode *valueNode = (SValueNode *)node->pLeft;
    if (NULL == valueNode) {
      sclError("sclRewriteNonConstOperator get null input param");
      return DEAL_RES_ERROR;
    }
    if (SCL_IS_NULL_VALUE_NODE(valueNode) && (node->opType != OP_TYPE_IS_NULL && node->opType != OP_TYPE_IS_NOT_NULL) &&
        (!sclContainsAggFuncNode(node->pRight))) {
      return sclRewriteNullInOptr(pNode, ctx, node->opType);
    }

    if (SCL_IS_COMPARISON_OPERATOR(node->opType) && SCL_DOWNGRADE_DATETYPE(valueNode->node.resType.type)) {
      sclDowngradeValueType(valueNode);
    }
  }

  if (node->pRight && (QUERY_NODE_VALUE == nodeType(node->pRight))) {
    SValueNode *valueNode = (SValueNode *)node->pRight;
    if (NULL == valueNode) {
      sclError("sclRewriteNonConstOperator get null input param");
      return DEAL_RES_ERROR;
    }
    if (SCL_IS_NULL_VALUE_NODE(valueNode) && (node->opType != OP_TYPE_IS_NULL && node->opType != OP_TYPE_IS_NOT_NULL) &&
        (!sclContainsAggFuncNode(node->pLeft))) {
      return sclRewriteNullInOptr(pNode, ctx, node->opType);
    }

    if (SCL_IS_COMPARISON_OPERATOR(node->opType) && SCL_DOWNGRADE_DATETYPE(valueNode->node.resType.type)) {
      sclDowngradeValueType(valueNode);
    }
  }

  return DEAL_RES_CONTINUE;
}

EDealRes sclRewriteFunction(SNode **pNode, SScalarCtx *ctx) {
  if (NULL == pNode || NULL == *pNode || NULL == ctx) {
    sclError("sclRewriteFunction get null input param");
    return DEAL_RES_ERROR;
  }

  SFunctionNode *node = (SFunctionNode *)*pNode;
  SNode         *tnode = NULL;
  if ((!fmIsScalarFunc(node->funcId) && (!ctx->dual)) ||
      fmIsUserDefinedFunc(node->funcId)) {
    return DEAL_RES_CONTINUE;
  }

  FOREACH(tnode, node->pParameterList) {
    if (!SCL_IS_CONST_NODE(tnode)) {
      return DEAL_RES_CONTINUE;
    }
  }

  SScalarParam output = {0};

  ctx->code = sclExecFunction(node, ctx, &output);
  if (ctx->code) {
    sclFreeParam(&output);
    return DEAL_RES_ERROR;
  }

  SValueNode *res = NULL;
  ctx->code = nodesMakeNode(QUERY_NODE_VALUE, (SNode **)&res);
  if (NULL == res) {
    sclError("make value node failed");
    sclFreeParam(&output);
    return DEAL_RES_ERROR;
  }

  res->translate = true;

  tstrncpy(res->node.aliasName, node->node.aliasName, TSDB_COL_NAME_LEN);
  res->node.resType.type = output.columnData->info.type;
  res->node.resType.bytes = output.columnData->info.bytes;
  res->node.resType.scale = output.columnData->info.scale;
  res->node.resType.precision = output.columnData->info.precision;
  if (colDataIsNull_s(output.columnData, 0)) {
    res->isNull = true;
  } else {
    int32_t type = output.columnData->info.type;
    if (type == TSDB_DATA_TYPE_JSON) {
      int32_t len = getJsonValueLen(output.columnData->pData);
      res->datum.p = taosMemoryCalloc(len, 1);
      if (NULL == res->datum.p) {
        sclError("calloc %d failed", len);
        sclFreeParam(&output);
        nodesDestroyNode((SNode *)res);
        ctx->code = TSDB_CODE_OUT_OF_MEMORY;
        return DEAL_RES_ERROR;
      }
      (void)memcpy(res->datum.p, output.columnData->pData, len);
    } else if (IS_VAR_DATA_TYPE(type)) {
      // res->datum.p = taosMemoryCalloc(res->node.resType.bytes + VARSTR_HEADER_SIZE + 1, 1);
      res->datum.p = taosMemoryCalloc(varDataTLen(output.columnData->pData) + 1, 1);
      if (NULL == res->datum.p) {
        sclError("calloc %d failed", (int)(varDataTLen(output.columnData->pData) + 1));
        sclFreeParam(&output);
        nodesDestroyNode((SNode *)res);
        ctx->code = TSDB_CODE_OUT_OF_MEMORY;
        return DEAL_RES_ERROR;
      }
      res->node.resType.bytes = varDataTLen(output.columnData->pData);
      (void)memcpy(res->datum.p, output.columnData->pData, varDataTLen(output.columnData->pData));
    } else {
      ctx->code = nodesSetValueNodeValue(res, output.columnData->pData);
      if (ctx->code) {
        sclFreeParam(&output);
        nodesDestroyNode((SNode *)res);
        return DEAL_RES_ERROR;
      }
    }
  }

  nodesDestroyNode(*pNode);
  *pNode = (SNode *)res;

  sclFreeParam(&output);
  return DEAL_RES_CONTINUE;
}

EDealRes sclRewriteLogic(SNode **pNode, SScalarCtx *ctx) {
  if (NULL == pNode || NULL == *pNode || NULL == ctx) {
    sclError("sclRewriteLogic get null input param");
    return DEAL_RES_ERROR;
  }
  SLogicConditionNode *node = (SLogicConditionNode *)*pNode;

  SScalarParam output = {0};
  ctx->code = sclExecLogic(node, ctx, &output);
  if (ctx->code) {
    sclFreeParam(&output);
    return DEAL_RES_ERROR;
  }

  if (0 == output.numOfRows) {
    sclFreeParam(&output);
    return DEAL_RES_CONTINUE;
  }

  SValueNode *res = NULL;
  ctx->code = nodesMakeNode(QUERY_NODE_VALUE, (SNode **)&res);
  if (NULL == res) {
    sclError("make value node failed");
    sclFreeParam(&output);
    return DEAL_RES_ERROR;
  }

  res->node.resType = node->node.resType;
  res->translate = true;

  tstrncpy(res->node.aliasName, node->node.aliasName, TSDB_COL_NAME_LEN);
  int32_t type = output.columnData->info.type;
  if (IS_VAR_DATA_TYPE(type)) {
    res->datum.p = output.columnData->pData;
    output.columnData->pData = NULL;
  } else {
    ctx->code = nodesSetValueNodeValue(res, output.columnData->pData);
    if (ctx->code) {
      sclFreeParam(&output);
      return DEAL_RES_ERROR;
    }
  }

  nodesDestroyNode(*pNode);
  *pNode = (SNode *)res;

  sclFreeParam(&output);
  return DEAL_RES_CONTINUE;
}

EDealRes sclRewriteOperator(SNode **pNode, SScalarCtx *ctx) {
  if (NULL == pNode || NULL == *pNode || NULL == ctx) {
    sclError("sclRewriteOperator get null input param");
    return DEAL_RES_ERROR;
  }

  SOperatorNode *node = (SOperatorNode *)*pNode;

  ctx->code = sclConvertOpValueNodeTs(node);
  if (ctx->code) {
    return DEAL_RES_ERROR;
  }

  if (node->pRight && (QUERY_NODE_NODE_LIST == nodeType(node->pRight))) {
    SNodeListNode *listNode = (SNodeListNode *)node->pRight;
    SNode         *tnode = NULL;
    WHERE_EACH(tnode, listNode->pNodeList) {
      if (SCL_IS_NULL_VALUE_NODE(tnode)) {
        if (node->opType == OP_TYPE_IN) {
          ERASE_NODE(listNode->pNodeList);
          continue;
        } else {  // OP_TYPE_NOT_IN
          return sclRewriteNullInOptr(pNode, ctx, node->opType);
        }
      }

      WHERE_NEXT;
    }

    if (listNode->pNodeList->length <= 0) {
      return sclRewriteNullInOptr(pNode, ctx, node->opType);
    }
  }

  if ((!SCL_IS_CONST_NODE(node->pLeft)) || (!SCL_IS_CONST_NODE(node->pRight))) {
    return sclRewriteNonConstOperator(pNode, ctx);
  }

  SScalarParam output = {0};
  ctx->code = sclExecOperator(node, ctx, &output);
  if (ctx->code) {
    sclFreeParam(&output);
    return DEAL_RES_ERROR;
  }

  SValueNode *res = NULL;
  ctx->code = nodesMakeNode(QUERY_NODE_VALUE, (SNode **)&res);
  if (NULL == res) {
    sclError("make value node failed");
    sclFreeParam(&output);
    return DEAL_RES_ERROR;
  }

  res->translate = true;

  tstrncpy(res->node.aliasName, node->node.aliasName, TSDB_COL_NAME_LEN);
  res->node.resType = node->node.resType;
  if (colDataIsNull_s(output.columnData, 0)) {
    res->isNull = true;
    res->node.resType = node->node.resType;
  } else {
    int32_t type = output.columnData->info.type;
    if (IS_VAR_DATA_TYPE(type)) {  // todo refactor
      res->datum.p = output.columnData->pData;
      output.columnData->pData = NULL;
    } else {
      ctx->code = nodesSetValueNodeValue(res, output.columnData->pData);
      if (ctx->code) {
        sclFreeParam(&output);
        return DEAL_RES_ERROR;
      }
    }
  }

  nodesDestroyNode(*pNode);
  *pNode = (SNode *)res;

  sclFreeParam(&output);
  return DEAL_RES_CONTINUE;
}

EDealRes sclRewriteCaseWhen(SNode **pNode, SScalarCtx *ctx) {
  if (NULL == pNode || NULL == *pNode || NULL == ctx) {
    sclError("sclRewriteCaseWhen get null input param");
    return DEAL_RES_ERROR;
  }
  SCaseWhenNode *node = (SCaseWhenNode *)*pNode;

  ctx->code = sclConvertCaseWhenValueNodeTs(node);
  if (ctx->code) {
    return DEAL_RES_ERROR;
  }

  if ((!SCL_IS_CONST_NODE(node->pCase)) || (!SCL_IS_CONST_NODE(node->pElse))) {
    return DEAL_RES_CONTINUE;
  }

  SNode *tnode = NULL;
  FOREACH(tnode, node->pWhenThenList) {
    SWhenThenNode *pWhenThen = (SWhenThenNode *)tnode;
    if (!SCL_IS_CONST_NODE(pWhenThen->pWhen) || !SCL_IS_CONST_NODE(pWhenThen->pThen)) {
      return DEAL_RES_CONTINUE;
    }
  }

  SScalarParam output = {0};
  ctx->code = sclExecCaseWhen(node, ctx, &output);
  if (ctx->code) {
    sclFreeParam(&output);
    return DEAL_RES_ERROR;
  }

  SValueNode *res = NULL;
  ctx->code = nodesMakeNode(QUERY_NODE_VALUE, (SNode **)&res);
  if (NULL == res) {
    sclError("make value node failed");
    sclFreeParam(&output);
    return DEAL_RES_ERROR;
  }

  res->translate = true;

  tstrncpy(res->node.aliasName, node->node.aliasName, TSDB_COL_NAME_LEN);
  res->node.resType = node->node.resType;
  if (colDataIsNull_s(output.columnData, 0)) {
    res->isNull = true;
    res->node.resType = node->node.resType;
  } else {
    int32_t type = output.columnData->info.type;
    if (IS_VAR_DATA_TYPE(type)) {  // todo refactor
      res->datum.p = taosMemoryCalloc(varDataTLen(output.columnData->pData) + 1,
                                      sizeof(char));  // add \0 to the end for print json value
      if (NULL == res->datum.p) {
        sclError("calloc %d failed", (int)(varDataTLen(output.columnData->pData) + 1));
        sclFreeParam(&output);
        nodesDestroyNode((SNode *)res);
        ctx->code = TSDB_CODE_OUT_OF_MEMORY;
        return DEAL_RES_ERROR;
      }
      (void)memcpy(res->datum.p, output.columnData->pData, varDataTLen(output.columnData->pData));
    } else {
      ctx->code = nodesSetValueNodeValue(res, output.columnData->pData);
      if (ctx->code) {
        sclFreeParam(&output);
        nodesDestroyNode((SNode *)res);
        return DEAL_RES_ERROR;
      }
    }
  }

  nodesDestroyNode(*pNode);
  *pNode = (SNode *)res;

  sclFreeParam(&output);
  return DEAL_RES_CONTINUE;
}

EDealRes sclConstantsRewriter(SNode **pNode, void *pContext) {
  if (NULL == pNode || NULL == *pNode || NULL == pContext) {
    sclError("sclConstantsRewriter get null input param");
    return DEAL_RES_ERROR;
  }

  SScalarCtx *ctx = (SScalarCtx *)pContext;

  if (QUERY_NODE_OPERATOR == nodeType(*pNode)) {
    return sclRewriteOperator(pNode, ctx);
  }

  if (QUERY_NODE_FUNCTION == nodeType(*pNode)) {
    return sclRewriteFunction(pNode, ctx);
  }

  if (QUERY_NODE_LOGIC_CONDITION == nodeType(*pNode)) {
    return sclRewriteLogic(pNode, ctx);
  }

  if (QUERY_NODE_CASE_WHEN == nodeType(*pNode)) {
    return sclRewriteCaseWhen(pNode, ctx);
  }

  return DEAL_RES_CONTINUE;
}

EDealRes sclWalkFunction(SNode *pNode, SScalarCtx *ctx) {
  if (NULL == pNode || NULL == ctx) {
    sclError("sclWalkFunction get null input param");
    return DEAL_RES_ERROR;
  }

  SFunctionNode *node = (SFunctionNode *)pNode;
  SScalarParam   output = {0};

  ctx->code = sclExecFunction(node, ctx, &output);
  if (ctx->code) {
    sclFreeParam(&output);
    return DEAL_RES_ERROR;
  }

  if (taosHashPut(ctx->pRes, &pNode, POINTER_BYTES, &output, sizeof(output))) {
    ctx->code = TSDB_CODE_OUT_OF_MEMORY;
    sclFreeParam(&output);
    return DEAL_RES_ERROR;
  }

  return DEAL_RES_CONTINUE;
}

EDealRes sclWalkLogic(SNode *pNode, SScalarCtx *ctx) {
  if (NULL == pNode || NULL == ctx) {
    sclError("sclWalkLogic get null input param");
    return DEAL_RES_ERROR;
  }

  SLogicConditionNode *node = (SLogicConditionNode *)pNode;
  SScalarParam         output = {0};

  ctx->code = sclExecLogic(node, ctx, &output);
  if (ctx->code) {
    sclFreeParam(&output);
    return DEAL_RES_ERROR;
  }

  if (taosHashPut(ctx->pRes, &pNode, POINTER_BYTES, &output, sizeof(output))) {
    ctx->code = TSDB_CODE_OUT_OF_MEMORY;
    sclFreeParam(&output);
    return DEAL_RES_ERROR;
  }

  return DEAL_RES_CONTINUE;
}

EDealRes sclWalkOperator(SNode *pNode, SScalarCtx *ctx) {
  if (NULL == pNode || NULL == ctx) {
    sclError("sclWalkOperator get null input param");
    return DEAL_RES_ERROR;
  }

  SOperatorNode *node = (SOperatorNode *)pNode;
  SScalarParam   output = {0};

  ctx->code = sclExecOperator(node, ctx, &output);
  if (ctx->code) {
    sclFreeParam(&output);
    return DEAL_RES_ERROR;
  }

  if (taosHashPut(ctx->pRes, &pNode, POINTER_BYTES, &output, sizeof(output))) {
    ctx->code = TSDB_CODE_OUT_OF_MEMORY;
    sclFreeParam(&output);
    return DEAL_RES_ERROR;
  }

  return DEAL_RES_CONTINUE;
}

EDealRes sclWalkTarget(SNode *pNode, SScalarCtx *ctx) {
  if (NULL == pNode || NULL == ctx) {
    sclError("sclWalkTarget get null input param");
    return DEAL_RES_ERROR;
  }

  STargetNode *target = (STargetNode *)pNode;

  if (target->dataBlockId >= taosArrayGetSize(ctx->pBlockList)) {
    sclError("target tupleId is too big, tupleId:%d, dataBlockNum:%d", target->dataBlockId,
             (int32_t)taosArrayGetSize(ctx->pBlockList));
    ctx->code = TSDB_CODE_QRY_INVALID_INPUT;
    return DEAL_RES_ERROR;
  }

  int32_t index = -1;
  for (int32_t i = 0; i < taosArrayGetSize(ctx->pBlockList); ++i) {
    SSDataBlock *pb = taosArrayGetP(ctx->pBlockList, i);
    if (NULL == pb) {
      ctx->code = terrno;
      return DEAL_RES_ERROR;
    }
    if (pb->info.id.blockId == target->dataBlockId) {
      index = i;
      break;
    }
  }

  if (index == -1) {
    sclError("column tupleId is too big, tupleId:%d, dataBlockNum:%d", target->dataBlockId,
             (int32_t)taosArrayGetSize(ctx->pBlockList));
    ctx->code = TSDB_CODE_QRY_INVALID_INPUT;
    return DEAL_RES_ERROR;
  }

  SSDataBlock *block = *(SSDataBlock **)taosArrayGet(ctx->pBlockList, index);

  if (target->slotId >= taosArrayGetSize(block->pDataBlock)) {
    sclError("target slot not exist, dataBlockId:%d, slotId:%d, dataBlockNum:%d", target->dataBlockId, target->slotId,
             (int32_t)taosArrayGetSize(block->pDataBlock));
    ctx->code = TSDB_CODE_QRY_INVALID_INPUT;
    return DEAL_RES_ERROR;
  }

  // if block->pDataBlock is not enough, there are problems if target->slotId bigger than the size of block->pDataBlock,
  SColumnInfoData *col = taosArrayGet(block->pDataBlock, target->slotId);

  SScalarParam *res = (SScalarParam *)taosHashGet(ctx->pRes, (void *)&target->pExpr, POINTER_BYTES);
  if (NULL == res) {
    sclError("no valid res in hash, node:%p, type:%d", target->pExpr, nodeType(target->pExpr));
    ctx->code = TSDB_CODE_APP_ERROR;
    return DEAL_RES_ERROR;
  }

  ctx->code = colDataAssign(col, res->columnData, res->numOfRows, NULL);
  if (ctx->code) {
    return DEAL_RES_ERROR;
  }
  block->info.rows = res->numOfRows;

  sclFreeParam(res);
  ctx->code = taosHashRemove(ctx->pRes, (void *)&target->pExpr, POINTER_BYTES);
  if (TSDB_CODE_SUCCESS != ctx->code) {
    return DEAL_RES_ERROR;
  }
  return DEAL_RES_CONTINUE;
}

EDealRes sclWalkCaseWhen(SNode *pNode, SScalarCtx *ctx) {
  if (NULL == pNode || NULL == ctx) {
    sclError("sclWalkCaseWhen get null input param");
    return DEAL_RES_ERROR;
  }

  SCaseWhenNode *node = (SCaseWhenNode *)pNode;
  SScalarParam   output = {0};

  ctx->code = sclExecCaseWhen(node, ctx, &output);
  if (ctx->code) {
    return DEAL_RES_ERROR;
  }

  if (taosHashPut(ctx->pRes, &pNode, POINTER_BYTES, &output, sizeof(output))) {
    ctx->code = terrno;
    return DEAL_RES_ERROR;
  }

  return DEAL_RES_CONTINUE;
}

EDealRes sclCalcWalker(SNode *pNode, void *pContext) {
  if (NULL == pNode || NULL == pContext) {
    sclError("sclCalcWalker get null input param");
    return DEAL_RES_ERROR;
  }

  if (QUERY_NODE_VALUE == nodeType(pNode) || QUERY_NODE_NODE_LIST == nodeType(pNode) ||
      QUERY_NODE_COLUMN == nodeType(pNode) || QUERY_NODE_LEFT_VALUE == nodeType(pNode) ||
      QUERY_NODE_WHEN_THEN == nodeType(pNode)) {
    return DEAL_RES_CONTINUE;
  }

  SScalarCtx *ctx = (SScalarCtx *)pContext;
  if (QUERY_NODE_OPERATOR == nodeType(pNode)) {
    return sclWalkOperator(pNode, ctx);
  }

  if (QUERY_NODE_FUNCTION == nodeType(pNode)) {
    return sclWalkFunction(pNode, ctx);
  }

  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pNode)) {
    return sclWalkLogic(pNode, ctx);
  }

  if (QUERY_NODE_TARGET == nodeType(pNode)) {
    return sclWalkTarget(pNode, ctx);
  }

  if (QUERY_NODE_CASE_WHEN == nodeType(pNode)) {
    return sclWalkCaseWhen(pNode, ctx);
  }

  sclError("invalid node type for scalar calculating, type:%d", nodeType(pNode));
  ctx->code = TSDB_CODE_QRY_INVALID_INPUT;
  return DEAL_RES_ERROR;
}

int32_t sclCalcConstants(SNode *pNode, bool dual, SNode **pRes) {
  if (NULL == pNode || NULL == pRes) {
    SCL_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t    code = 0;
  SScalarCtx ctx = {0};
  ctx.dual = dual;
  ctx.pRes = taosHashInit(SCL_DEFAULT_OP_NUM, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (NULL == ctx.pRes) {
    sclError("taosHashInit failed, num:%d", SCL_DEFAULT_OP_NUM);
    SCL_ERR_RET(terrno);
  }

  nodesRewriteExprPostOrder(&pNode, sclConstantsRewriter, (void *)&ctx);
  SCL_ERR_JRET(ctx.code);
  *pRes = pNode;

_return:

  sclFreeRes(ctx.pRes);
  return code;
}

static int32_t sclGetMinusOperatorResType(SOperatorNode *pOp) {
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       lino = 0;
  SCL_CHECK_NULL(pOp, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_CONDITION(IS_MATHABLE_TYPE(((SExprNode *)(pOp->pLeft))->resType.type), code, lino, _return,
                      TSDB_CODE_TSC_INVALID_OPERATION)
  pOp->node.resType.type = TSDB_DATA_TYPE_DOUBLE;
  pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes;
_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t sclGetMathOperatorResType(SOperatorNode *pOp) {
  if (pOp == NULL || pOp->pLeft == NULL || pOp->pRight == NULL) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  SDataType ldt = ((SExprNode *)(pOp->pLeft))->resType;
  SDataType rdt = ((SExprNode *)(pOp->pRight))->resType;

  if ((TSDB_DATA_TYPE_TIMESTAMP == ldt.type && TSDB_DATA_TYPE_TIMESTAMP == rdt.type) ||
      TSDB_DATA_TYPE_VARBINARY == ldt.type || TSDB_DATA_TYPE_VARBINARY == rdt.type ||
      (TSDB_DATA_TYPE_TIMESTAMP == ldt.type && (IS_VAR_DATA_TYPE(rdt.type) || IS_FLOAT_TYPE(rdt.type))) ||
      (TSDB_DATA_TYPE_TIMESTAMP == rdt.type && (IS_VAR_DATA_TYPE(ldt.type) || IS_FLOAT_TYPE(ldt.type)))) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  if ((TSDB_DATA_TYPE_TIMESTAMP == ldt.type && IS_INTEGER_TYPE(rdt.type)) ||
      (TSDB_DATA_TYPE_TIMESTAMP == rdt.type && IS_INTEGER_TYPE(ldt.type)) ||
      (TSDB_DATA_TYPE_TIMESTAMP == ldt.type && TSDB_DATA_TYPE_BOOL == rdt.type) ||
      (TSDB_DATA_TYPE_TIMESTAMP == rdt.type && TSDB_DATA_TYPE_BOOL == ldt.type)) {
    pOp->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
    pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes;
  } else {
    pOp->node.resType.type = TSDB_DATA_TYPE_DOUBLE;
    pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t sclGetCompOperatorResType(SOperatorNode *pOp) {
  if (pOp == NULL || pOp->pLeft == NULL) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  SDataType ldt = ((SExprNode *)(pOp->pLeft))->resType;

  if (OP_TYPE_IN == pOp->opType || OP_TYPE_NOT_IN == pOp->opType) {
    if (pOp->pRight == NULL) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }
    ((SExprNode *)(pOp->pRight))->resType = ldt;
  } else if (nodesIsRegularOp(pOp)) {
    if (pOp->pRight == NULL) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }
    SDataType rdt = ((SExprNode *)(pOp->pRight))->resType;
    if (ldt.type == TSDB_DATA_TYPE_VARBINARY || !IS_VAR_DATA_TYPE(ldt.type) ||
        QUERY_NODE_VALUE != nodeType(pOp->pRight) ||
        (!IS_STR_DATA_TYPE(rdt.type) && (rdt.type != TSDB_DATA_TYPE_NULL))) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }
    SValueNode *node = (SValueNode *)(pOp->pRight);
    if (!node->placeholderNo && nodesIsMatchRegularOp(pOp)) {
      if (checkRegexPattern(node->literal) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_PAR_REGULAR_EXPRESSION_ERROR;
      }
    }
  }
  pOp->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
  return TSDB_CODE_SUCCESS;
}

static int32_t sclGetJsonOperatorResType(SOperatorNode *pOp) {
  if (pOp == NULL || pOp->pLeft == NULL || pOp->pRight == NULL) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  SDataType ldt = ((SExprNode *)(pOp->pLeft))->resType;
  SDataType rdt = ((SExprNode *)(pOp->pRight))->resType;

  if (TSDB_DATA_TYPE_JSON != ldt.type || !IS_STR_DATA_TYPE(rdt.type)) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }
  if (pOp->opType == OP_TYPE_JSON_GET_VALUE) {
    pOp->node.resType.type = TSDB_DATA_TYPE_JSON;
  } else if (pOp->opType == OP_TYPE_JSON_CONTAINS) {
    pOp->node.resType.type = TSDB_DATA_TYPE_BOOL;
  }
  pOp->node.resType.bytes = tDataTypes[pOp->node.resType.type].bytes;
  return TSDB_CODE_SUCCESS;
}

static int32_t sclGetBitwiseOperatorResType(SOperatorNode *pOp) {
  if (!pOp || !pOp->pLeft || !pOp->pRight) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }
  SDataType ldt = ((SExprNode *)(pOp->pLeft))->resType;
  SDataType rdt = ((SExprNode *)(pOp->pRight))->resType;
  if (TSDB_DATA_TYPE_VARBINARY == ldt.type || TSDB_DATA_TYPE_VARBINARY == rdt.type) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }
  pOp->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
  return TSDB_CODE_SUCCESS;
}

int32_t scalarCalculateConstants(SNode *pNode, SNode **pRes) { return sclCalcConstants(pNode, false, pRes); }

int32_t scalarCalculateConstantsFromDual(SNode *pNode, SNode **pRes) { return sclCalcConstants(pNode, true, pRes); }

int32_t scalarCalculate(SNode *pNode, SArray *pBlockList, SScalarParam *pDst) {
  if (NULL == pNode || NULL == pBlockList) {
    SCL_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t    code = 0;
  SScalarCtx ctx = {.code = 0, .pBlockList = pBlockList, .param = pDst ? pDst->param : NULL};

  // TODO: OPT performance
  ctx.pRes = taosHashInit(SCL_DEFAULT_OP_NUM, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (NULL == ctx.pRes) {
    sclError("taosHashInit failed, num:%d", SCL_DEFAULT_OP_NUM);
    SCL_ERR_RET(terrno);
  }

  nodesWalkExprPostOrder(pNode, sclCalcWalker, (void *)&ctx);
  SCL_ERR_JRET(ctx.code);

  if (pDst) {
    SScalarParam *res = (SScalarParam *)taosHashGet(ctx.pRes, (void *)&pNode, POINTER_BYTES);
    if (NULL == res) {
      sclError("no valid res in hash, node:%p, type:%d", pNode, nodeType(pNode));
      SCL_ERR_JRET(TSDB_CODE_APP_ERROR);
    }

    SSDataBlock *pb = taosArrayGetP(pBlockList, 0);
    if (NULL == pb) {
      SCL_ERR_JRET(terrno);
    }
    if (1 == res->numOfRows && pb->info.rows > 0) {
      SCL_ERR_JRET(sclExtendResRows(pDst, res, pBlockList));
    } else {
      SCL_ERR_JRET(colInfoDataEnsureCapacity(pDst->columnData, res->numOfRows, true));
      SCL_ERR_JRET(colDataAssign(pDst->columnData, res->columnData, res->numOfRows, NULL));
      pDst->numOfRows = res->numOfRows;
      pDst->numOfQualified = res->numOfQualified;
    }

    sclFreeParam(res);
    SCL_ERR_JRET(taosHashRemove(ctx.pRes, (void *)&pNode, POINTER_BYTES));
  }

_return:
  sclFreeRes(ctx.pRes);
  return code;
}

int32_t scalarGetOperatorResultType(SOperatorNode *pOp) {
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       lino = 0;
  SCL_CHECK_NULL(pOp, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(pOp->pLeft, code, lino, _return, TSDB_CODE_INVALID_PARA)
  if (TSDB_DATA_TYPE_BLOB == ((SExprNode *)(pOp->pLeft))->resType.type ||
      (NULL != pOp->pRight && TSDB_DATA_TYPE_BLOB == ((SExprNode *)(pOp->pRight))->resType.type)) {
    SCL_ERR_JRET(TSDB_CODE_TSC_INVALID_OPERATION);
  }

  switch (pOp->opType) {
    case OP_TYPE_ADD:
    case OP_TYPE_SUB:
    case OP_TYPE_MULTI:
    case OP_TYPE_DIV:
    case OP_TYPE_REM:
      return sclGetMathOperatorResType(pOp);
    case OP_TYPE_MINUS:
      return sclGetMinusOperatorResType(pOp);
    case OP_TYPE_ASSIGN:
      pOp->node.resType = ((SExprNode *)(pOp->pLeft))->resType;
      break;
    case OP_TYPE_BIT_AND:
    case OP_TYPE_BIT_OR:
      return sclGetBitwiseOperatorResType(pOp);
    case OP_TYPE_GREATER_THAN:
    case OP_TYPE_GREATER_EQUAL:
    case OP_TYPE_LOWER_THAN:
    case OP_TYPE_LOWER_EQUAL:
    case OP_TYPE_EQUAL:
    case OP_TYPE_NOT_EQUAL:
    case OP_TYPE_IS_NULL:
    case OP_TYPE_IS_NOT_NULL:
    case OP_TYPE_IS_TRUE:
    case OP_TYPE_IS_FALSE:
    case OP_TYPE_IS_UNKNOWN:
    case OP_TYPE_IS_NOT_TRUE:
    case OP_TYPE_IS_NOT_FALSE:
    case OP_TYPE_IS_NOT_UNKNOWN:
    case OP_TYPE_LIKE:
    case OP_TYPE_NOT_LIKE:
    case OP_TYPE_MATCH:
    case OP_TYPE_NMATCH:
    case OP_TYPE_IN:
    case OP_TYPE_NOT_IN:
      return sclGetCompOperatorResType(pOp);
    case OP_TYPE_JSON_GET_VALUE:
    case OP_TYPE_JSON_CONTAINS:
      return sclGetJsonOperatorResType(pOp);
    default:
      break;
  }

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}
