#include "scalar.h"
#include "decimal.h"
#include "function.h"
#include "functionMgt.h"
#include "nodes.h"
#include "querynodes.h"
#include "sclInt.h"
#include "sclvector.h"
#include "taoserror.h"
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
  char   *timeStr = valueNode->datum.p;
  int64_t value = 0;
  int32_t code = convertStringToTimestamp(valueNode->node.resType.type, valueNode->datum.p, precision, &value,
                                          valueNode->tz, valueNode->charsetCxt);  // todo tz
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  taosMemoryFree(timeStr);
  valueNode->datum.i = value;
  valueNode->typeData = valueNode->datum.i;

  valueNode->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
  valueNode->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes;

  return TSDB_CODE_SUCCESS;
}

int32_t sclCreateColumnInfoData(SDataType *pType, int32_t numOfRows, SScalarParam *pParam) {
  SColumnInfoData *pColumnData = taosMemoryCalloc(1, sizeof(SColumnInfoData));
  if (pColumnData == NULL) {
    return terrno;
  }

  pColumnData->info.type = pType->type;
  pColumnData->info.bytes = pType->bytes;
  pColumnData->info.scale = pType->scale;
  pColumnData->info.precision = pType->precision;

  int32_t code = colInfoDataEnsureCapacity(pColumnData, numOfRows, true);
  if (code != TSDB_CODE_SUCCESS) {
    colDataDestroy(pColumnData);
    taosMemoryFree(pColumnData);
    return code;
  }

  pParam->columnData = pColumnData;
  pParam->colAlloced = true;
  pParam->numOfRows = numOfRows;

  return TSDB_CODE_SUCCESS;
}

int32_t sclConvertValueToSclParam(SValueNode *pValueNode, SScalarParam *out, int32_t *overflow) {
  SScalarParam in = {.numOfRows = 1};
  int32_t      code = sclCreateColumnInfoData(&pValueNode->node.resType, 1, &in);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = colDataSetVal(in.columnData, 0, nodesGetValueFromNode(pValueNode), false);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }

  code = colInfoDataEnsureCapacity(out->columnData, 1, true);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }
  setTzCharset(&in, pValueNode->tz, pValueNode->charsetCxt);
  setTzCharset(out, pValueNode->tz, pValueNode->charsetCxt);
  code = vectorConvertSingleColImpl(&in, out, overflow, -1, -1);

_exit:
  sclFreeParam(&in);

  return code;
}

int32_t sclExtendResRowsRange(SScalarParam *pDst, int32_t rowStartIdx, int32_t rowEndIdx, SScalarParam *pSrc,
                              SArray *pBlockList) {
  SSDataBlock *pb = taosArrayGetP(pBlockList, 0);
  if (NULL == pb) {
    SCL_ERR_RET(TSDB_CODE_OUT_OF_RANGE);
  }
  SScalarParam *pLeft = taosMemoryCalloc(1, sizeof(SScalarParam));
  int32_t       code = TSDB_CODE_SUCCESS;
  if (NULL == pLeft) {
    sclError("calloc %d failed", (int32_t)sizeof(SScalarParam));
    SCL_ERR_RET(terrno);
  }

  pLeft->numOfRows = pb->info.rows;

  if (pDst->numOfRows < pb->info.rows) {
    
    // When rowStartIdx equals -1, the stream computation performs a forceout operation on the window, and the capacity has been completed 
    if (rowStartIdx == -1) {
      SCL_ERR_JRET(colInfoDataEnsureCapacity(pDst->columnData, pb->info.rows, true));
    }
  }

  if (rowStartIdx < 0 || rowEndIdx < 0) {
    _bin_scalar_fn_t OperatorFn = getBinScalarOperatorFn(OP_TYPE_ASSIGN);
    SCL_ERR_JRET(OperatorFn(pLeft, pSrc, pDst, TSDB_ORDER_ASC));
  } else {
    SCL_ERR_JRET(vectorAssignRange(pLeft, pSrc, pDst, rowStartIdx, rowEndIdx, TSDB_ORDER_ASC));
  }

_return:
  taosMemoryFree(pLeft);

  SCL_RET(code);
}

int32_t sclExtendResRows(SScalarParam *pDst, SScalarParam *pSrc, SArray *pBlockList) {
  return sclExtendResRowsRange(pDst, -1, -1, pSrc, pBlockList);
}

// processType = 0 means all type. 1 means number, 2 means var, 3 means float, 4 means var&integer
int32_t scalarGenerateSetFromList(void **data, void *pNode, uint32_t type, STypeMod typeMod, int8_t processType) {
  SHashObj *pObj = taosHashInit(256, taosGetDefaultHashFunction(type), true, false);
  if (NULL == pObj) {
    sclError("taosHashInit failed, size:%d", 256);
    SCL_ERR_RET(terrno);
  }

  taosHashSetEqualFp(pObj, taosGetDefaultEqualFunction(type));

  int32_t        code = 0;
  SNodeListNode *nodeList = (SNodeListNode *)pNode;
  SScalarParam   out = {.columnData = taosMemoryCalloc(1, sizeof(SColumnInfoData))};
  if (out.columnData == NULL) {
    SCL_ERR_JRET(terrno);
  }
  int32_t len = 0;
  void   *buf = NULL;

  SNode *nodeItem = NULL;
  FOREACH(nodeItem, nodeList->pNodeList) {
    SValueNode *valueNode = (SValueNode *)nodeItem;
    if ((IS_VAR_DATA_TYPE(valueNode->node.resType.type) && (processType == 1 || processType == 3)) ||
        (IS_INTEGER_TYPE(valueNode->node.resType.type) && (processType == 2 || processType == 3)) ||
        (IS_FLOAT_TYPE(valueNode->node.resType.type) && (processType == 2 || processType == 4))) {
      continue;
    }

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
        extractTypeFromTypeMod(type, typeMod, &out.columnData->info.precision, &out.columnData->info.scale, NULL);
      }

      int32_t overflow = 0;
      code = sclConvertValueToSclParam(valueNode, &out, &overflow);
      if (code != TSDB_CODE_SUCCESS) {
        //        sclError("convert data from %d to %d failed", in.type, out.type);
        SCL_ERR_JRET(code);
      }

      if (overflow) {
        continue;
      }

      if (IS_VAR_DATA_TYPE(type)) {
        buf = colDataGetVarData(out.columnData, 0);
        if (IS_STR_DATA_BLOB(type)) {
          len = blobDataTLen(buf);
        } else {
          len = varDataTLen(buf);
        }
      } else {
        len = tDataTypes[type].bytes;
        buf = out.columnData->pData;
      }
    } else {
      buf = nodesGetValueFromNode(valueNode);
      if (IS_VAR_DATA_TYPE(type)) {
        if (IS_STR_DATA_BLOB(type)) {
          len = blobDataTLen(buf);
        } else {
          len = varDataTLen(buf);
        }
      } else {
        len = valueNode->node.resType.bytes;
      }
    }

    if (taosHashPut(pObj, buf, (size_t)len, NULL, 0)) {
      sclError("taosHashPut to set failed");
      SCL_ERR_JRET(terrno);
    }

    colInfoDataCleanup(out.columnData, out.numOfRows);
  }

  *data = pObj;

  colDataDestroy(out.columnData);
  taosMemoryFreeClear(out.columnData);
  return TSDB_CODE_SUCCESS;

_return:

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

  if (param->pHashFilterOthers != NULL) {
    taosHashCleanup(param->pHashFilterOthers);
    param->pHashFilterOthers = NULL;
  }
}

int32_t sclCopyValueNodeValue(SValueNode *pNode, void **res) {
  if (TSDB_DATA_TYPE_NULL == pNode->node.resType.type) {
    return TSDB_CODE_SUCCESS;
  }

  *res = taosMemoryMalloc(pNode->node.resType.bytes);
  if (NULL == (*res)) {
    sclError("malloc %d failed", pNode->node.resType.bytes);
    SCL_ERR_RET(terrno);
  }

  (void)memcpy(*res, nodesGetValueFromNode(pNode), pNode->node.resType.bytes);
  return TSDB_CODE_SUCCESS;
}

void sclFreeParamList(SScalarParam *param, int32_t paramNum) {
  if (NULL == param) {
    return;
  }

  for (int32_t i = 0; i < paramNum; ++i) {
    SScalarParam *p = param + i;
    sclFreeParam(p);
  }

  taosMemoryFree(param);
}

void sclDowngradeValueType(SValueNode *valueNode) {
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
      if (DBL_EQUAL(f, valueNode->datum.d)) {
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
  switch (nodeType(node)) {
    case QUERY_NODE_LEFT_VALUE: {
      SSDataBlock *pb = taosArrayGetP(ctx->pBlockList, 0);
      if (NULL == pb) {
        SCL_ERR_RET(TSDB_CODE_OUT_OF_RANGE);
      }
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

      int32_t  type = ctx->type.selfType;
      STypeMod typeMod = 0;
      SNode   *nodeItem = NULL;
      FOREACH(nodeItem, nodeList->pNodeList) {
        SValueNode *valueNode = (SValueNode *)nodeItem;
        int32_t     tmp = vectorGetConvertType(type, valueNode->node.resType.type);
        if (tmp != 0) {
          type = tmp;
        }
      }
      if (IS_NUMERIC_TYPE(type)) {
        ctx->type.peerType = type;
      }
      // Currently, all types of node list can't be decimal types.
      // Decimal op double/float/nchar/varchar types will convert these types to double type.
      // All other types do not need scale info, so here we use self scale
      // When decimal types are supported in value list, we need to check convertability of different decimal types.
      // And the new decimal scale will also be calculated.
      if (IS_DECIMAL_TYPE(type))
        typeMod = decimalCalcTypeMod(TSDB_DECIMAL_MAX_PRECISION, getScaleFromTypeMod(type, ctx->type.selfTypeMod));
      type = ctx->type.peerType;
      if (IS_VAR_DATA_TYPE(ctx->type.selfType) && IS_NUMERIC_TYPE(type)) {
        SCL_ERR_RET(scalarGenerateSetFromList((void **)&param->pHashFilter, node, type, typeMod, 1));
        SCL_ERR_RET(
            scalarGenerateSetFromList((void **)&param->pHashFilterOthers, node, ctx->type.selfType, typeMod, 2));
      } else if (IS_INTEGER_TYPE(ctx->type.selfType) && IS_FLOAT_TYPE(type)) {
        SCL_ERR_RET(scalarGenerateSetFromList((void **)&param->pHashFilter, node, type, typeMod, 2));
        SCL_ERR_RET(
            scalarGenerateSetFromList((void **)&param->pHashFilterOthers, node, ctx->type.selfType, typeMod, 4));
      } else {
        SCL_ERR_RET(scalarGenerateSetFromList((void **)&param->pHashFilter, node, type, typeMod, 0));
      }

      param->filterValueTypeMod = typeMod;
      param->filterValueType = type;
      param->colAlloced = true;
      if (taosHashPut(ctx->pRes, &node, POINTER_BYTES, param, sizeof(*param))) {
        taosHashCleanup(param->pHashFilter);
        param->pHashFilter = NULL;
        taosHashCleanup(param->pHashFilterOthers);
        param->pHashFilterOthers = NULL;
        sclError("taosHashPut nodeList failed, size:%d", (int32_t)sizeof(*param));
        return terrno;
      }
      param->colAlloced = false;
      break;
    }
    case QUERY_NODE_COLUMN: {
      if (ctx->stream.streamTsRange != NULL || ctx->stream.pWins != NULL) {
        break;
      }
      if (NULL == ctx->pBlockList) {
        sclError("invalid node type for constant calculating, type:%d, src:%p", nodeType(node), ctx->pBlockList);
        SCL_ERR_RET(TSDB_CODE_APP_ERROR);
      }

      SColumnNode *ref = (SColumnNode *)node;
      int32_t index = -1;
      for (int32_t i = 0; i < taosArrayGetSize(ctx->pBlockList); ++i) {
        SSDataBlock *pb = taosArrayGetP(ctx->pBlockList, i);
        if (NULL == pb) {
          SCL_ERR_RET(TSDB_CODE_OUT_OF_RANGE);
        }
        if (pb->info.id.blockId == ref->dataBlockId) {
          index = i;
          break;
        }
      }

      if (index == -1 && taosArrayGetSize(ctx->pBlockList) > 1) {
        sclError("column tupleId is too big, tupleId:%d, dataBlockNum:%d", ref->dataBlockId,
                 (int32_t)taosArrayGetSize(ctx->pBlockList));
        SCL_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
      } else {
        index = 0;
      }

      SSDataBlock *block = *(SSDataBlock **)taosArrayGet(ctx->pBlockList, index);
      if (NULL == block) {
        SCL_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
      }

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
  return TSDB_CODE_SUCCESS;
}



int32_t sclSetStreamExtWinParam(int32_t funcId, SNodeList* pParamNodes, SScalarParam* res, SScalarCtx *pCtx) {
  int32_t code = 0;

  int32_t t = fmGetFuncTypeFromId(funcId);
  const SStreamRuntimeFuncInfo* pInfo = pCtx->stream.pStreamRuntimeFuncInfo;

  SNode* pFirstParam = nodesListGetNode(pParamNodes, 0);
  if (nodeType(pFirstParam) != QUERY_NODE_VALUE) {
    uError("invalid param node type: %d for func: %d", nodeType(pFirstParam), funcId);
    return TSDB_CODE_INTERNAL_ERROR;
  }

  SCL_ERR_RET(sclCreateColumnInfoData(&((SValueNode*)pFirstParam)->node.resType, pInfo->pStreamPesudoFuncVals->size, res));

  if (LIST_LENGTH(pParamNodes) != 1) {
    uError("invalid placeholder paran num:%d, function type: %d in ext win range expr", LIST_LENGTH(pParamNodes), t);
    return TSDB_CODE_INTERNAL_ERROR;
  }
  
  for (int32_t i = 0; i < pInfo->pStreamPesudoFuncVals->size; ++i) {
    SSTriggerCalcParam *pParams = taosArrayGet(pInfo->pStreamPesudoFuncVals, i);
    switch (t) {
      case FUNCTION_TYPE_TPREV_TS:
        ((int64_t*)res->columnData->pData)[i] = pParams->prevTs;
        break;
      case FUNCTION_TYPE_TCURRENT_TS:
        ((int64_t*)res->columnData->pData)[i] = pParams->currentTs;
        break;
      case FUNCTION_TYPE_TNEXT_TS:
        ((int64_t*)res->columnData->pData)[i] = pParams->nextTs;
        break;
      case FUNCTION_TYPE_TWSTART:
        ((int64_t*)res->columnData->pData)[i] = pParams->wstart;
        break;
      case FUNCTION_TYPE_TWEND:
        ((int64_t*)res->columnData->pData)[i] = pParams->wend;
        break;
      case FUNCTION_TYPE_TWDURATION:
        ((int64_t*)res->columnData->pData)[i] = pParams->wduration;
        break;
      case FUNCTION_TYPE_TWROWNUM:
        ((int64_t*)res->columnData->pData)[i] = pParams->wrownum;
        break;        
      case FUNCTION_TYPE_TPREV_LOCALTIME:
        ((int64_t*)res->columnData->pData)[i] = pParams->prevLocalTime;
        break;
      case FUNCTION_TYPE_TLOCALTIME:
        ((int64_t*)res->columnData->pData)[i] = pParams->triggerTime;
        break;
      case FUNCTION_TYPE_TNEXT_LOCALTIME:
        ((int64_t*)res->columnData->pData)[i] = pParams->nextLocalTime;
        break;
      case FUNCTION_TYPE_TGRPID:
        ((int64_t*)res->columnData->pData)[i] = pInfo->groupId;
        break;
      default:
        uError("invalid placeholder function type: %d in ext win range expr", t);
        return TSDB_CODE_INTERNAL_ERROR;
    }
  }
  
  return code;
}

int32_t scalarAssignPlaceHolderRes(SColumnInfoData* pResColData, int64_t offset, int64_t rows, int16_t funcId, const void* pExtraParams) {
  int32_t t = fmGetFuncTypeFromId(funcId);
  SStreamRuntimeFuncInfo* pInfo = (SStreamRuntimeFuncInfo*)pExtraParams;
  SSTriggerCalcParam *pParams = taosArrayGet(pInfo->pStreamPesudoFuncVals, pInfo->curIdx);
  int64_t* pData = NULL;
  switch (t) {
    case FUNCTION_TYPE_TPREV_TS:
      pData = &pParams->prevTs;
      break;
    case FUNCTION_TYPE_TCURRENT_TS:
      pData = &pParams->currentTs;
      break;
    case FUNCTION_TYPE_TNEXT_TS:
      pData = &pParams->nextTs;
      break;
    case FUNCTION_TYPE_TWSTART:
      pData = &pParams->wstart;
      break;
    case FUNCTION_TYPE_TWEND:
      pData = &pParams->wend;
      break;
    case FUNCTION_TYPE_TWDURATION:
      pData = &pParams->wduration;
      break;
    case FUNCTION_TYPE_TWROWNUM:
      pData = &pParams->wrownum;
      break;        
    case FUNCTION_TYPE_TPREV_LOCALTIME:
      pData = &pParams->prevLocalTime;
      break;
    case FUNCTION_TYPE_TLOCALTIME:
      pData = &pParams->triggerTime;
      break;
    case FUNCTION_TYPE_TNEXT_LOCALTIME:
      pData = &pParams->nextLocalTime;
      break;
    case FUNCTION_TYPE_TGRPID:
      pData = &pInfo->groupId;
      break;
    case FUNCTION_TYPE_PLACEHOLDER_TBNAME: {
      // find tbname from stream part col vals
      char buf[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
      for (int32_t i = 0; i < taosArrayGetSize(pInfo->pStreamPartColVals); ++i) {
        SStreamGroupValue *pValue = taosArrayGet(pInfo->pStreamPartColVals, i);
        if (pValue != NULL && pValue->isTbname) {
          *(VarDataLenT *)(buf) = pValue->data.nData;
          (void)memcpy(((char *)(buf) + sizeof(VarDataLenT)), pValue->data.pData, pValue->data.nData);
          break;
        }
      }
      return colDataSetNItems(pResColData, offset, (const char *)buf, rows, 1, false);
    }
    default:
      uError("invalid placeholder function type: %d in ext win range expr", t);
      return TSDB_CODE_INTERNAL_ERROR;
  }

  return doCopyNItems(pResColData, offset, (const char*)pData, sizeof(int64_t), rows, false);
}

static int32_t sclInitStreamPseudoFuncParamList(int32_t funcId, SScalarParam **ppParams, SNodeList *pParamNodes,
                                                SScalarCtx *pCtx, int32_t *pParamNum, int32_t *pRowNum) {
  int32_t code = 0;
  SNode*  pNode = 0;
  *pParamNum = LIST_LENGTH(pParamNodes);
  if (*pParamNum < 1) {
    sclError("invalid func param size: %d, should at least one", *pParamNum);
    return TSDB_CODE_INTERNAL_ERROR;
  }

  SScalarParam* pParamsList = taosMemoryCalloc(*pParamNum, sizeof(SScalarParam));
  if (!pParamsList) {
    sclError("calloc %d failed", (int32_t)(*pParamNum * sizeof(SScalarParam)));
    SCL_ERR_RET(terrno);
  }

  int32_t i = 0;
  if (pCtx->stream.pWins) {
    SScalarParam* pParam = (1 == pCtx->stream.extWinType) ? &pCtx->stream.twstart : &pCtx->stream.twend;
    SCL_ERR_JRET(sclSetStreamExtWinParam(funcId, pParamNodes, pParam, pCtx));
    memcpy(pParamsList, pParam, sizeof(*pParamsList));
    *pRowNum = pParam->numOfRows;
  } else if (pCtx->stream.pStreamRuntimeFuncInfo) {
    code = fmSetStreamPseudoFuncParamVal(funcId, pParamNodes, pCtx->stream.pStreamRuntimeFuncInfo);
    if (code != 0) {
      sclError("failed to set stream pseudo func param vals: %s", tstrerror(code));
      SCL_ERR_JRET(code);
    }

    FOREACH(pNode, pParamNodes) {
      SCL_ERR_JRET(sclInitParam(pNode, &pParamsList[i], pCtx, pRowNum));
      ++i;
    }
  }

  *ppParams = pParamsList;
  return code;

_return:

  sclFreeParamList(pParamsList, *pParamNum);
  return code;
}

int32_t sclInitParamList(SScalarParam **pParams, SNodeList *pParamList, SScalarCtx *ctx, int32_t *paramNum,
                         int32_t *rowNum, int32_t funcId) {
  if (fmIsPlaceHolderFunc(funcId))
    return sclInitStreamPseudoFuncParamList(funcId, pParams, pParamList, ctx, paramNum, rowNum);
    
  int32_t code = 0;
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

  SScalarParam *paramList = taosMemoryCalloc(*paramNum, sizeof(SScalarParam));
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
    sclFreeParamList(paramList, *paramNum);
    paramList = NULL;
  }

  *pParams = paramList;
  return TSDB_CODE_SUCCESS;

_return:
  sclFreeParamList(paramList, *paramNum);
  SCL_RET(code);
}

int32_t sclGetNodeType(SNode *pNode, SScalarCtx *ctx, int32_t *type, STypeMod *pTypeMod) {
  if (NULL == pNode) {
    *type = -1;
    return TSDB_CODE_SUCCESS;
  }

  switch ((int)nodeType(pNode)) {
    case QUERY_NODE_VALUE: {
      SValueNode *valueNode = (SValueNode *)pNode;
      *type = valueNode->node.resType.type;
      *pTypeMod = typeGetTypeModFromDataType(&valueNode->node.resType);
      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_NODE_LIST: {
      SNodeListNode *nodeList = (SNodeListNode *)pNode;
      *type = nodeList->node.resType.type;
      *pTypeMod = typeGetTypeModFromDataType(&nodeList->node.resType);
      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_COLUMN: {
      SColumnNode *colNode = (SColumnNode *)pNode;
      *type = colNode->node.resType.type;
      *pTypeMod = typeGetTypeModFromDataType(&colNode->node.resType);
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
      *pTypeMod = typeGetTypeModFromColInfo(&res->columnData->info);
      return TSDB_CODE_SUCCESS;
    }
  }

  *type = -1;
  *pTypeMod = 0;
  return TSDB_CODE_SUCCESS;
}

int32_t sclSetOperatorValueType(SOperatorNode *node, SScalarCtx *ctx) {
  ctx->type.opResType = node->node.resType.type;
  SCL_ERR_RET(sclGetNodeType(node->pLeft, ctx, &(ctx->type.selfType), &ctx->type.selfTypeMod));
  SCL_ERR_RET(sclGetNodeType(node->pRight, ctx, &(ctx->type.peerType), &ctx->type.selfTypeMod));
  SCL_RET(TSDB_CODE_SUCCESS);
}

int32_t sclInitOperatorParams(SScalarParam **pParams, SOperatorNode *node, SScalarCtx *ctx, int32_t *rowNum) {
  int32_t code = 0;
  int32_t paramNum = scalarGetOperatorParamNum(node->opType);
  if (NULL == node->pLeft || (paramNum == 2 && NULL == node->pRight)) {
    sclError("invalid operation node, left:%p, right:%p", node->pLeft, node->pRight);
    SCL_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  SScalarParam *paramList = taosMemoryCalloc(paramNum, sizeof(SScalarParam));
  if (NULL == paramList) {
    sclError("calloc %d failed", (int32_t)(paramNum * sizeof(SScalarParam)));
    SCL_ERR_RET(terrno);
  }

  SCL_ERR_JRET(sclSetOperatorValueType(node, ctx));

  SCL_ERR_JRET(sclInitParam(node->pLeft, &paramList[0], ctx, rowNum));
  setTzCharset(&paramList[0], node->tz, node->charsetCxt);
  if (paramNum > 1) {
    SCL_ERR_JRET(sclInitParam(node->pRight, &paramList[1], ctx, rowNum));
    setTzCharset(&paramList[1], node->tz, node->charsetCxt);
  }

  *pParams = paramList;
  return TSDB_CODE_SUCCESS;

_return:
  sclFreeParamList(paramList, paramNum);
  SCL_RET(code);
}

int32_t sclGetNodeRes(SNode *node, SScalarCtx *ctx, SScalarParam **res) {
  if (NULL == node) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t rowNum = 0;
  *res = taosMemoryCalloc(1, sizeof(**res));
  if (NULL == *res) {
    SCL_ERR_RET(terrno);
  }

  SCL_ERR_RET(sclInitParam(node, *res, ctx, &rowNum));

  return TSDB_CODE_SUCCESS;
}

int32_t sclWalkCaseWhenList(SScalarCtx *ctx, SNodeList *pList, struct SListCell *pCell, SScalarParam *pCase,
                            SScalarParam *pElse, SScalarParam *pComp, SScalarParam *output, int32_t rowIdx,
                            int32_t totalRows, bool *complete) {
  SNode         *node = NULL;
  SWhenThenNode *pWhenThen = NULL;
  SScalarParam  *pWhen = NULL;
  SScalarParam  *pThen = NULL;
  int32_t        code = 0;

  for (SListCell *cell = pCell; (NULL != cell ? (node = cell->pNode, true) : (node = NULL, false));
       cell = cell->pNext) {
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

  sclFreeParam(pWhen);
  sclFreeParam(pThen);
  taosMemoryFreeClear(pWhen);
  taosMemoryFreeClear(pThen);

  SCL_RET(code);
}

int32_t sclWalkWhenList(SScalarCtx *ctx, SNodeList *pList, struct SListCell *pCell, SScalarParam *pElse,
                        SScalarParam *output, int32_t rowIdx, int32_t totalRows, bool *complete, bool preSingle) {
  SNode         *node = NULL;
  SWhenThenNode *pWhenThen = NULL;
  SScalarParam  *pWhen = NULL;
  SScalarParam  *pThen = NULL;
  int32_t        code = 0;

  for (SListCell *cell = pCell; (NULL != cell ? (node = cell->pNode, true) : (node = NULL, false));
       cell = cell->pNext) {
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

  sclFreeParam(pWhen);
  sclFreeParam(pThen);
  taosMemoryFree(pWhen);
  taosMemoryFree(pThen);

  SCL_RET(code);
}

int32_t sclExecFunction(SFunctionNode *node, SScalarCtx *ctx, SScalarParam *output) {
  SScalarParam *params = NULL;
  int32_t       rowNum = 0;
  int32_t       paramNum = 0;
  int32_t       code = 0;
  SCL_ERR_RET(sclInitParamList(&params, node->pParameterList, ctx, &paramNum, &rowNum, node->funcId));
  setTzCharset(params, node->tz, node->charsetCxt);

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

    code = sclCreateColumnInfoData(&node->node.resType, rowNum, output);
    if (code != TSDB_CODE_SUCCESS) {
      SCL_ERR_JRET(code);
    }

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

  sclFreeParamList(params, paramNum);
  SCL_RET(code);
}

int32_t sclExecLogic(SLogicConditionNode *node, SScalarCtx *ctx, SScalarParam *output) {
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

  SScalarParam *params = NULL;
  int32_t       rowNum = 0;
  int32_t       paramNum = 0;
  int32_t       code = 0;
  SCL_ERR_RET(sclInitParamList(&params, node->pParameterList, ctx, &paramNum, &rowNum, -1));
  if (NULL == params) {
    output->numOfRows = 0;
    return TSDB_CODE_SUCCESS;
  }

  int32_t type = node->node.resType.type;
  output->numOfRows = rowNum;

  SDataType t = {.type = type, .bytes = tDataTypes[type].bytes};
  code = sclCreateColumnInfoData(&t, rowNum, output);
  if (code != TSDB_CODE_SUCCESS) {
    SCL_ERR_JRET(code);
  }

  int32_t numOfQualified = 0;

  bool value = false;
  bool complete = true;
  for (int32_t i = 0; i < rowNum; ++i) {
    complete = true;
    //sclInfo("logic row:%d start", i);
    for (int32_t m = 0; m < paramNum; ++m) {
      if (NULL == params[m].columnData) {
        complete = false;
        //sclInfo("logic row:%d param:%d skipped", i, m);
        continue;
      }

      // 1=1 and tag_column = 1
      int32_t ind = (i >= params[m].numOfRows) ? (params[m].numOfRows - 1) : i;
      char   *p = colDataGetData(params[m].columnData, ind);

      GET_TYPED_DATA(value, bool, params[m].columnData->info.type, p,
                     typeGetTypeModFromColInfo(&params[m].columnData->info));

      //sclInfo("logic row:%d param:%d value:%d", i, m, value);

      if (LOGIC_COND_TYPE_AND == node->condType && (false == value)) {
        complete = true;
        //sclInfo("logic row:%d param:%d value:%d AND complete", i, m, value);
        break;
      } else if (LOGIC_COND_TYPE_OR == node->condType && value) {
        complete = true;
        //sclInfo("logic row:%d param:%d value:%d OR complete", i, m, value);
        break;
      } else if (LOGIC_COND_TYPE_NOT == node->condType) {
        value = !value;
        //sclInfo("logic row:%d param:%d value:%d NOT", i, m, value);
      }
    }

    if (complete) {
      //sclInfo("logic row:%d complete value:%d", i, value);
      SCL_ERR_JRET(colDataSetVal(output->columnData, i, (char *)&value, false));
      if (value) {
        numOfQualified++;
        //sclInfo("logic row:%d complete numOfQualified:%d", i, numOfQualified);
      }
    }
  }

  if (SCL_IS_CONST_CALC(ctx) && (false == complete)) {
    sclFreeParam(output);
    output->numOfRows = 0;
  }

  output->numOfQualified = numOfQualified;

_return:
  sclFreeParamList(params, paramNum);
  SCL_RET(code);
}

static int32_t sclGetFirstTsFromParam(SScalarParam* param, int64_t* ts){
  int32_t code = 0, lino = 0;
  int64_t skey = 0;
  char* buf = NULL;
  
  void* p = colDataGetData(param->columnData, 0);
  if (IS_VAR_DATA_TYPE(param->columnData->info.type)){
    buf = taosMemoryCalloc(1, varDataTLen(p));
    if (buf != NULL){
      memcpy(buf, varDataVal(p), varDataLen(p));
      TAOS_CHECK_EXIT(taosParseTime(buf, &skey, strlen(buf), param->columnData->info.precision, param->tz));
    } 
  } else {
    skey = *(int64_t*)p;
  }
  
  *ts = skey;

_exit:

  taosMemoryFree(buf);

  if (code) {
    sclError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static bool sclIsPrimTimeStampCol(SNode *pNode) {
  if (pNode == NULL) {
    return false;
  }
  if (nodeType(pNode) == QUERY_NODE_COLUMN) {
    SColumnNode *colNode = (SColumnNode *)pNode;
    if (colNode->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
      return true;
    }
  }
  return false;
}

static bool sclIsStreamRangeReq(SOperatorNode *node) {
  if (node->opType != OP_TYPE_LOWER_EQUAL && node->opType != OP_TYPE_LOWER_THAN &&
      node->opType != OP_TYPE_GREATER_EQUAL && node->opType != OP_TYPE_GREATER_THAN) {
    return false;
  }
  if (sclIsPrimTimeStampCol(node->pLeft) || sclIsPrimTimeStampCol(node->pRight)) {
    return true;
  }

  return false;
}

static int32_t sclGetExprTSValue(SScalarCtx *ctx, SNode *pNode, int64_t *tsValue) {
  int32_t code = TSDB_CODE_SUCCESS, lino = 0;
  if (nodeType(pNode) == QUERY_NODE_OPERATOR || nodeType(pNode) == QUERY_NODE_FUNCTION) {
    SScalarParam *res = (SScalarParam *)taosHashGet(ctx->pRes, (void *)&pNode, POINTER_BYTES);
    if (res == NULL || res->columnData == NULL) {
      sclError("no result for node, type:%d, node:%p", nodeType(pNode), pNode);
      return TSDB_CODE_QRY_INVALID_INPUT;
    }
    if (res->columnData->pData == NULL) {
      sclError("invalid column data for ts range expr, type:%d, node:%p", res->columnData->info.type, pNode);
      return TSDB_CODE_QRY_INVALID_INPUT;
    }
    if (res->columnData->info.type == TSDB_DATA_TYPE_TIMESTAMP) {
      TAOS_CHECK_EXIT(sclGetFirstTsFromParam(res, tsValue));
    } else {
      sclError("invalid type for ts range expr, type:%d, node:%p", res->columnData->info.type, pNode);
      return TSDB_CODE_QRY_INVALID_INPUT;
    }
  } else if (nodeType(pNode) == QUERY_NODE_VALUE) {
    SValueNode *valueNode = (SValueNode *)pNode;
    if (IS_INTEGER_TYPE(valueNode->node.resType.type) || IS_TIMESTAMP_TYPE(valueNode->node.resType.type)) {
      *tsValue = valueNode->datum.i;
    } else {
      sclError("invalid type for ts range expr, type:%d, node:%p", valueNode->node.resType.type, pNode);
      return TSDB_CODE_QRY_INVALID_INPUT;
    }
  } else {
    sclError("invalid node type for ts range expr, type:%d, node:%p", nodeType(pNode), pNode);
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

_exit:

  if (code) {
    sclError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t sclCalcStreamCurrWinTimeRange(SScalarCtx *ctx, SStreamTSRangeParas *streamTsRange,
                                                SOperatorNode *node, SScalarParam *params) {
  int32_t code = 0;
  int64_t timeValue = 0;

  if (sclIsPrimTimeStampCol(node->pRight)) {
    code = sclGetExprTSValue(ctx, node->pLeft, &timeValue);
  } else if (sclIsPrimTimeStampCol(node->pLeft)) {
    code = sclGetExprTSValue(ctx, node->pRight, &timeValue);

  } else {
    sclError("invalid stream timerange start expr, opType:%d, nodeType left:%d, nodeType right:%d", node->opType,
             nodeType(node->pLeft), nodeType(node->pRight));
    return TSDB_CODE_STREAM_INTERNAL_ERROR;         
  }

  streamTsRange->opType = node->opType;
  if (streamTsRange->opType == OP_TYPE_GREATER_EQUAL || streamTsRange->opType == OP_TYPE_GREATER_THAN) {
    streamTsRange->timeValue = (streamTsRange->timeValue > timeValue) ? streamTsRange->timeValue : timeValue;
  } else {
    streamTsRange->timeValue = (streamTsRange->timeValue < timeValue) ? streamTsRange->timeValue : timeValue;
  }
  if (code != TSDB_CODE_SUCCESS) {
    sclError("get ts value for stream timerange failed, code:%d", code);
  }
  return TSDB_CODE_SUCCESS;
}


static int32_t sclCalcStreamExtWinsTimeRange(SScalarCtx *ctx,          SOperatorNode *node) {
  int32_t code = 0;
  int64_t timeValue = 0;
  SNode* pNode = NULL;
  if (sclIsPrimTimeStampCol(node->pRight)) {
    pNode = node->pLeft;
  } else if (sclIsPrimTimeStampCol(node->pLeft)) {
    pNode = node->pRight;
  } else {
    sclError("invalid stream ext win timerange expr, opType:%d, nodeType left:%d, nodeType right:%d", node->opType,
             nodeType(node->pLeft), nodeType(node->pRight));
    return TSDB_CODE_STREAM_INTERNAL_ERROR;         
  }

  SScalarParam *res = (SScalarParam *)taosHashGet(ctx->pRes, (void *)&pNode, POINTER_BYTES);
  if (res == NULL || res->columnData == NULL) {
    sclError("no result for node, type:%d, node:%p", nodeType(pNode), pNode);
    return TSDB_CODE_QRY_INVALID_INPUT;
  }
  if (res->columnData->pData == NULL) {
    sclError("invalid column data for ts range expr, type:%d, node:%p", res->columnData->info.type, pNode);
    return TSDB_CODE_QRY_INVALID_INPUT;
  }
  if (res->columnData->info.type != TSDB_DATA_TYPE_TIMESTAMP) {
    sclError("invalid type for ts range expr, type:%d, node:%p", res->columnData->info.type, pNode);
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  if (1 == ctx->stream.extWinType) {
    if (node->opType == OP_TYPE_GREATER_THAN) {
      for (int32_t i = 0; i < res->numOfRows; ++i) {
        ctx->stream.pWins[i].tw.skey = (-1 == ctx->stream.pWins[i].winOutIdx) ? TMAX(((int64_t*)res->columnData->pData)[i] + 1, ctx->stream.pWins[i].tw.skey) : (((int64_t*)res->columnData->pData)[i] + 1);
        ctx->stream.pWins[i].winOutIdx = -1;
      }
    } else if (node->opType == OP_TYPE_GREATER_EQUAL) {
      for (int32_t i = 0; i < res->numOfRows; ++i) {
        ctx->stream.pWins[i].tw.skey = (-1 == ctx->stream.pWins[i].winOutIdx) ? TMAX(((int64_t*)res->columnData->pData)[i], ctx->stream.pWins[i].tw.skey) : (((int64_t*)res->columnData->pData)[i]);
        ctx->stream.pWins[i].winOutIdx = -1;
      }
    } else {
      qError("invalid op type:%d in ext win range start expr", node->opType);
      return TSDB_CODE_STREAM_INTERNAL_ERROR;
    }
  }
  
  if (2 == ctx->stream.extWinType) {
    //if (ctx->stream.pStreamRuntimeFuncInfo->triggerType != STREAM_TRIGGER_SLIDING) {
      // consider triggerType and keep the ekey exclude
      if (node->opType == OP_TYPE_LOWER_THAN) {
        for (int32_t i = 0; i < res->numOfRows; ++i) {
          ctx->stream.pWins[i].tw.ekey = (-2 == ctx->stream.pWins[i].winOutIdx) ? TMIN(((int64_t*)res->columnData->pData)[i], ctx->stream.pWins[i].tw.ekey) : (((int64_t*)res->columnData->pData)[i]);
          ctx->stream.pWins[i].winOutIdx = -2;
        }
      } else if (node->opType == OP_TYPE_LOWER_EQUAL) {
        for (int32_t i = 0; i < res->numOfRows; ++i) {
          ctx->stream.pWins[i].tw.ekey = (-2 == ctx->stream.pWins[i].winOutIdx) ? TMIN(((int64_t*)res->columnData->pData)[i] + 1, ctx->stream.pWins[i].tw.ekey) : (((int64_t*)res->columnData->pData)[i] + 1);
          ctx->stream.pWins[i].winOutIdx = -2;
        }
      } else {
        qError("invalid op type:%d in ext win range end expr", node->opType);
        return TSDB_CODE_STREAM_INTERNAL_ERROR;
      }
    /*
    } else {
      if (node->opType == OP_TYPE_LOWER_THAN) {
        for (int32_t i = 0; i < res->numOfRows; ++i) {
          ctx->stream.pWins[i].tw.ekey = ((int64_t*)res->columnData->pData)[i];
        }
      } else if (node->opType == OP_TYPE_LOWER_EQUAL) {
        for (int32_t i = 0; i < res->numOfRows; ++i) {
          ctx->stream.pWins[i].tw.ekey = ((int64_t*)res->columnData->pData)[i] + 1;
        }
      } else {
        qError("invalid op type:%d in ext win range end expr", node->opType);
        return TSDB_CODE_STREAM_INTERNAL_ERROR;
      }
    }
    */
  }
  
  return TSDB_CODE_SUCCESS;
}


int32_t sclExecOperator(SOperatorNode *node, SScalarCtx *ctx, SScalarParam *output) {
  SScalarParam *params = NULL;
  int32_t       rowNum = 0;
  int32_t       code = 0;
  int32_t       paramNum = scalarGetOperatorParamNum(node->opType);

  // json not support in in operator
  if (nodeType(node->pLeft) == QUERY_NODE_VALUE) {
    SValueNode *valueNode = (SValueNode *)node->pLeft;
    if (valueNode->node.resType.type == TSDB_DATA_TYPE_JSON &&
        (node->opType == OP_TYPE_IN || node->opType == OP_TYPE_NOT_IN)) {
      SCL_RET(TSDB_CODE_QRY_JSON_IN_ERROR);
    }
  }

  SCL_ERR_JRET(sclInitOperatorParams(&params, node, ctx, &rowNum));
  
  if (ctx->stream.streamTsRange != NULL && sclIsStreamRangeReq(node)) {
    code = sclCalcStreamCurrWinTimeRange(ctx, ctx->stream.streamTsRange, node, params);
    goto _return;
  }

  if (ctx->stream.pWins && sclIsStreamRangeReq(node)) {
    code = sclCalcStreamExtWinsTimeRange(ctx, node);
    goto _return;
  }

  if (output->columnData == NULL) {
    code = sclCreateColumnInfoData(&node->node.resType, rowNum, output);
    if (code != TSDB_CODE_SUCCESS) {
      SCL_ERR_JRET(code);
    }
  }
  
  _bin_scalar_fn_t OperatorFn = getBinScalarOperatorFn(node->opType);

  SScalarParam *pLeft = &params[0];
  SScalarParam *pRight = paramNum > 1 ? &params[1] : NULL;

  terrno = TSDB_CODE_SUCCESS;
  SCL_ERR_JRET(OperatorFn(pLeft, pRight, output, TSDB_ORDER_ASC));

_return:
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

  if (NULL == node->pWhenThenList || node->pWhenThenList->length <= 0) {
    sclError("invalid whenThen list");
    SCL_ERR_RET(TSDB_CODE_INVALID_PARA);
  }

  if (ctx->pBlockList) {
    SSDataBlock *pb = taosArrayGetP(ctx->pBlockList, 0);
    if (NULL == pb) {
      SCL_ERR_RET(TSDB_CODE_OUT_OF_RANGE);
    }
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
  SWhenThenNode *pWhenThen = (SWhenThenNode *)node->pWhenThenList->pHead->pNode;

  SCL_ERR_JRET(sclGetNodeRes(pWhenThen->pWhen, ctx, &pWhen));
  SCL_ERR_JRET(sclGetNodeRes(pWhenThen->pThen, ctx, &pThen));
  if (NULL == pWhen || NULL == pThen) {
    sclError("invalid when/then in whenThen list");
    SCL_ERR_JRET(TSDB_CODE_INVALID_PARA);
  }
  setTzCharset(pCase, node->tz, node->charsetCxt);
  setTzCharset(pWhen, node->tz, node->charsetCxt);
  setTzCharset(pThen, node->tz, node->charsetCxt);
  setTzCharset(pElse, node->tz, node->charsetCxt);
  setTzCharset(output, node->tz, node->charsetCxt);
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
  taosMemoryFree(pCase);
  taosMemoryFree(pElse);
  taosMemoryFree(pWhen);
  taosMemoryFree(pThen);

  return TSDB_CODE_SUCCESS;

_return:

  sclFreeParam(pCase);
  sclFreeParam(pElse);
  sclFreeParam(&comp);
  sclFreeParam(pWhen);
  sclFreeParam(pThen);
  sclFreeParam(output);
  taosMemoryFree(pCase);
  taosMemoryFree(pElse);
  taosMemoryFree(pWhen);
  taosMemoryFree(pThen);

  SCL_RET(code);
}

EDealRes sclRewriteNullInOptr(SNode **pNode, SScalarCtx *ctx, EOperatorType opType) {
  if (opType <= OP_TYPE_CALC_MAX) {
    SValueNode *res = NULL;
    ctx->code = nodesMakeNode(QUERY_NODE_VALUE, (SNode **)&res);
    if (NULL == res) {
      sclError("make value node failed");
      return DEAL_RES_ERROR;
    }

    res->node.resType.type = TSDB_DATA_TYPE_NULL;
    res->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_NULL].bytes;

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

int32_t sclConvertCaseWhenValueNodeTs(SCaseWhenNode *node) {
  if (NULL == node->pCase) {
    return TSDB_CODE_SUCCESS;
  }

  if (SCL_IS_VAR_VALUE_NODE(node->pCase)) {
    SNode *pNode;
    FOREACH(pNode, node->pWhenThenList) {
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

  return TSDB_CODE_SUCCESS;
}

EDealRes sclRewriteNonConstOperator(SNode **pNode, SScalarCtx *ctx) {
  SOperatorNode *node = (SOperatorNode *)*pNode;
  int32_t        code = 0;

  if (node->pLeft && (QUERY_NODE_VALUE == nodeType(node->pLeft))) {
    SValueNode *valueNode = (SValueNode *)node->pLeft;
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

void sclGetValueNodeSrcTable(SNode *pNode, char **ppSrcTable, bool *multiTable) {
  if (*multiTable) {
    return;
  }

  if (QUERY_NODE_NODE_LIST == nodeType(pNode)) {
    SNodeListNode *pList = (SNodeListNode *)pNode;
    SNode         *pTmp = NULL;
    FOREACH(pTmp, pList->pNodeList) { sclGetValueNodeSrcTable(pTmp, ppSrcTable, multiTable); }

    return;
  }

  if (QUERY_NODE_VALUE != nodeType(pNode)) {
    return;
  }

  SValueNode *pValue = (SValueNode *)pNode;
  if (pValue->node.srcTable[0]) {
    if (*ppSrcTable) {
      if (strcmp(*ppSrcTable, pValue->node.srcTable)) {
        *multiTable = true;
        *ppSrcTable = NULL;
      }

      return;
    }

    *ppSrcTable = pValue->node.srcTable;
  }
}

EDealRes sclRewriteFunction(SNode **pNode, SScalarCtx *ctx) {
  SFunctionNode *node = (SFunctionNode *)*pNode;
  SNode         *tnode = NULL;
  if ((!fmIsScalarFunc(node->funcId) && (!ctx->dual)) || fmIsUserDefinedFunc(node->funcId)) {
    return DEAL_RES_CONTINUE;
  }

  char *srcTable = NULL;
  bool  multiTable = false;
  FOREACH(tnode, node->pParameterList) {
    if (!SCL_IS_CONST_NODE(tnode)) {
      return DEAL_RES_CONTINUE;
    }

    if (SCL_NEED_SRC_TABLE_FUNC(node->funcType)) {
      sclGetValueNodeSrcTable(tnode, &srcTable, &multiTable);
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

  if (srcTable) {
    tstrncpy(res->node.srcTable, srcTable, TSDB_TABLE_NAME_LEN);
  }
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
        ctx->code = terrno;
        return DEAL_RES_ERROR;
      }
      (void)memcpy(res->datum.p, output.columnData->pData, len);
    } else if (IS_VAR_DATA_TYPE(type)) {
      // res->datum.p = taosMemoryCalloc(res->node.resType.bytes + VARSTR_HEADER_SIZE + 1, 1);
      if (IS_STR_DATA_BLOB(type)) {
        res->datum.p = taosMemoryCalloc(blobDataTLen(output.columnData->pData) + 1, 1);
        if (NULL == res->datum.p) {
          sclError("calloc %d failed", (int)(blobDataTLen(output.columnData->pData) + 1));
          sclFreeParam(&output);
          nodesDestroyNode((SNode *)res);
          ctx->code = terrno;
          return DEAL_RES_ERROR;
        }
        (void)memcpy(res->datum.p, output.columnData->pData, blobDataTLen(output.columnData->pData));
      } else {
        res->datum.p = taosMemoryCalloc(varDataTLen(output.columnData->pData) + 1, 1);
        if (NULL == res->datum.p) {
          sclError("calloc %d failed", (int)(varDataTLen(output.columnData->pData) + 1));
          sclFreeParam(&output);
          nodesDestroyNode((SNode *)res);
          ctx->code = terrno;
          return DEAL_RES_ERROR;
        }
        (void)memcpy(res->datum.p, output.columnData->pData, varDataTLen(output.columnData->pData));
      }
    } else if (type == TSDB_DATA_TYPE_DECIMAL) {
      res->datum.p = taosMemoryCalloc(1, DECIMAL128_BYTES);
      if (!res->datum.p) {
        sclError("calloc %d failed", DECIMAL128_BYTES);
        sclFreeParam(&output);
        nodesDestroyNode((SNode *)res);
        ctx->code = terrno;
        return DEAL_RES_ERROR;
      }
      (void)memcpy(res->datum.p, output.columnData->pData, DECIMAL128_BYTES);
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
  SOperatorNode *node = (SOperatorNode *)*pNode;

  ctx->code = scalarConvertOpValueNodeTs(node);
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

  char *srcTable = NULL;
  bool  multiTable = false;
  if (SCL_NEED_SRC_TABLE_OP(node->opType)) {
    sclGetValueNodeSrcTable(node->pLeft, &srcTable, &multiTable);
    sclGetValueNodeSrcTable(node->pRight, &srcTable, &multiTable);
  }

  res->translate = true;

  if (srcTable) {
    tstrncpy(res->node.srcTable, srcTable, TSDB_TABLE_NAME_LEN);
  }
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
      if (IS_STR_DATA_BLOB(type)) {
        res->datum.p = taosMemoryCalloc(blobDataTLen(output.columnData->pData) + 1,
                                        sizeof(char));  // add \0 to the end for print json value
        if (NULL == res->datum.p) {
          sclError("calloc %d failed", (int)(varDataTLen(output.columnData->pData) + 1));
          sclFreeParam(&output);
          nodesDestroyNode((SNode *)res);
          ctx->code = terrno;
          return DEAL_RES_ERROR;
        }
        (void)memcpy(res->datum.p, output.columnData->pData, blobDataTLen(output.columnData->pData));
      } else {
        res->datum.p = taosMemoryCalloc(varDataTLen(output.columnData->pData) + 1,
                                        sizeof(char));  // add \0 to the end for print json value
        if (NULL == res->datum.p) {
          sclError("calloc %d failed", (int)(varDataTLen(output.columnData->pData) + 1));
          sclFreeParam(&output);
          nodesDestroyNode((SNode *)res);
          ctx->code = terrno;
          return DEAL_RES_ERROR;
        }
        (void)memcpy(res->datum.p, output.columnData->pData, varDataTLen(output.columnData->pData));
      }
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
  SFunctionNode *node = (SFunctionNode *)pNode;
  SScalarParam   output = {0};

  ctx->code = sclExecFunction(node, ctx, &output);
  if (ctx->code) {
    sclFreeParam(&output);
    return DEAL_RES_ERROR;
  }

  if (taosHashPut(ctx->pRes, &pNode, POINTER_BYTES, &output, sizeof(output))) {
    ctx->code = terrno;
    sclFreeParam(&output);
    return DEAL_RES_ERROR;
  }

  return DEAL_RES_CONTINUE;
}

EDealRes sclWalkLogic(SNode *pNode, SScalarCtx *ctx) {
  SLogicConditionNode *node = (SLogicConditionNode *)pNode;
  SScalarParam         output = {0};

  ctx->code = sclExecLogic(node, ctx, &output);
  if (ctx->code) {
    sclFreeParam(&output);
    return DEAL_RES_ERROR;
  }

  if (taosHashPut(ctx->pRes, &pNode, POINTER_BYTES, &output, sizeof(output))) {
    ctx->code = terrno;
    sclFreeParam(&output);
    return DEAL_RES_ERROR;
  }

  return DEAL_RES_CONTINUE;
}

EDealRes sclWalkOperator(SNode *pNode, SScalarCtx *ctx) {
  SOperatorNode *node = (SOperatorNode *)pNode;
  SScalarParam   output = {0};

  ctx->code = sclExecOperator(node, ctx, &output);
  if (ctx->code) {
    sclFreeParam(&output);
    return DEAL_RES_ERROR;
  }

  if (taosHashPut(ctx->pRes, &pNode, POINTER_BYTES, &output, sizeof(output))) {
    ctx->code = terrno;
    sclFreeParam(&output);
    return DEAL_RES_ERROR;
  }

  return DEAL_RES_CONTINUE;
}

EDealRes sclWalkTarget(SNode *pNode, SScalarCtx *ctx) {
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
      ctx->code = TSDB_CODE_OUT_OF_RANGE;
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
  if (NULL == pNode) {
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
  const SDataType *pDt = &((SExprNode *)(pOp->pLeft))->resType;
  if (!IS_MATHABLE_TYPE(pDt->type)) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  if (IS_DECIMAL_TYPE(pDt->type)) {
    pOp->node.resType = *pDt;
  } else {
    pOp->node.resType.type = TSDB_DATA_TYPE_DOUBLE;
    pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t sclGetMathOperatorResType(SOperatorNode *pOp) {
  if (pOp == NULL || pOp->pLeft == NULL || pOp->pRight == NULL) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  SDataType ldt = ((SExprNode *)(pOp->pLeft))->resType;
  SDataType rdt = ((SExprNode *)(pOp->pRight))->resType;
  bool      hasDecimalType = IS_DECIMAL_TYPE(ldt.type) || IS_DECIMAL_TYPE(rdt.type);

  if ((TSDB_DATA_TYPE_TIMESTAMP == ldt.type && TSDB_DATA_TYPE_TIMESTAMP == rdt.type) ||
      TSDB_DATA_TYPE_VARBINARY == ldt.type || TSDB_DATA_TYPE_VARBINARY == rdt.type ||
      (TSDB_DATA_TYPE_TIMESTAMP == ldt.type && (IS_VAR_DATA_TYPE(rdt.type))) ||
      (TSDB_DATA_TYPE_TIMESTAMP == rdt.type && (IS_VAR_DATA_TYPE(ldt.type)))) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  if (checkOperatorRestypeIsTimestamp(pOp->opType, ldt.type, rdt.type)) {
    pOp->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
    pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes;
    pOp->node.resType.precision = sclGetOpValueNodeTsPrecision(pOp->pLeft, pOp->pRight);
  } else {
    if (hasDecimalType) {
      return decimalGetRetType(&ldt, &rdt, pOp->opType, &pOp->node.resType);
    } else {
      pOp->node.resType.type = TSDB_DATA_TYPE_DOUBLE;
      pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes;
    }
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
  if (!pOp->pLeft || !pOp->pRight) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }
  SDataType ldt = ((SExprNode *)(pOp->pLeft))->resType;
  SDataType rdt = ((SExprNode *)(pOp->pRight))->resType;
  if (TSDB_DATA_TYPE_VARBINARY == ldt.type || TSDB_DATA_TYPE_VARBINARY == rdt.type) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }
  if (IS_DECIMAL_TYPE(ldt.type) || IS_DECIMAL_TYPE(rdt.type)) return TSDB_CODE_TSC_INVALID_OPERATION;
  pOp->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
  return TSDB_CODE_SUCCESS;
}

int32_t scalarConvertOpValueNodeTs(SOperatorNode *node) {
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

int32_t scalarCalculateConstants(SNode *pNode, SNode **pRes) { return sclCalcConstants(pNode, false, pRes); }

int32_t scalarCalculateConstantsFromDual(SNode *pNode, SNode **pRes) { return sclCalcConstants(pNode, true, pRes); }

int32_t scalarCalculate(SNode *pNode, SArray *pBlockList, SScalarParam *pDst, const void *pExtraParam,
                        void *streamTsRange) {
  return scalarCalculateInRange(pNode, pBlockList, pDst, -1, -1, pExtraParam, streamTsRange);
}

int32_t scalarCalculateInRange(SNode *pNode, SArray *pBlockList, SScalarParam *pDst, int32_t rowStartIdx,
                               int32_t rowEndIdx, const void *pExtraParam, void *pTsRange) {
  if (NULL == pNode || (NULL == pBlockList && pTsRange == NULL)) {
    SCL_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t    code = 0;
  SScalarCtx ctx = {.code = 0, .pBlockList = pBlockList, .param = pDst ? pDst->param : NULL};
  ctx.stream.pStreamRuntimeFuncInfo = pExtraParam;
  ctx.stream.streamTsRange = pTsRange;

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
      SCL_ERR_JRET(TSDB_CODE_OUT_OF_RANGE);
    }
    if (1 == res->numOfRows && pb->info.rows > 0) {
      SCL_ERR_JRET(sclExtendResRowsRange(pDst, rowStartIdx, rowEndIdx, res, pBlockList));
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

int32_t scalarCalculateExtWinsTimeRange(STimeRangeNode *pNode, const void *pExtraParam, SExtWinTimeWindow *pWins) {
  int32_t    code = 0;
  SScalarCtx ctx = {0};
  ctx.stream.pStreamRuntimeFuncInfo = pExtraParam;
  ctx.stream.pWins = pWins;

  // TODO: OPT performance
  ctx.pRes = taosHashInit(SCL_DEFAULT_OP_NUM, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (NULL == ctx.pRes) {
    sclError("taosHashInit failed, num:%d", SCL_DEFAULT_OP_NUM);
    SCL_ERR_RET(terrno);
  }

  ctx.stream.extWinType = 1;
  nodesWalkExprPostOrder(pNode->pStart, sclCalcWalker, (void *)&ctx);
  SCL_ERR_JRET(ctx.code);

  ctx.stream.extWinType = 2;
  nodesWalkExprPostOrder(pNode->pEnd, sclCalcWalker, (void *)&ctx);
  SCL_ERR_JRET(ctx.code);

_return:

  if (code) {
    qError("%s failed since %s", __func__, tstrerror(code));
  }

  sclFreeRes(ctx.pRes);
  return code;
}

int32_t scalarGetOperatorResultType(SOperatorNode *pOp) {
  if (TSDB_DATA_TYPE_BLOB == ((SExprNode *)(pOp->pLeft))->resType.type ||
      (NULL != pOp->pRight && TSDB_DATA_TYPE_BLOB == ((SExprNode *)(pOp->pRight))->resType.type)) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
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

  return TSDB_CODE_SUCCESS;
}
