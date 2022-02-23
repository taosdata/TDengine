#include "sclfunc.h"
#include "sclvector.h"

static void assignBasicParaInfo(struct SScalarParam* dst, const struct SScalarParam* src) {
  dst->type = src->type;
  dst->bytes = src->bytes;
  dst->num = src->num;
}

static void tceil(SScalarParam* pOutput, size_t numOfInput, const SScalarParam *pLeft) {
  assignBasicParaInfo(pOutput, pLeft);
  assert(numOfInput == 1);

  switch (pLeft->bytes) {
    case TSDB_DATA_TYPE_FLOAT: {
      float* p = (float*) pLeft->data;
      float* out = (float*) pOutput->data;
      for (int32_t i = 0; i < pLeft->num; ++i) {
        out[i] = ceilf(p[i]);
      }
    }

    case TSDB_DATA_TYPE_DOUBLE: {
      double* p = (double*) pLeft->data;
      double* out = (double*)pOutput->data;
      for (int32_t i = 0; i < pLeft->num; ++i) {
        out[i] = ceil(p[i]);
      }
    }

    default:
      memcpy(pOutput->data, pLeft->data, pLeft->num* pLeft->bytes);
  }
}

static void tfloor(SScalarParam* pOutput, size_t numOfInput, const SScalarParam *pLeft) {
  assignBasicParaInfo(pOutput, pLeft);
  assert(numOfInput == 1);

  switch (pLeft->bytes) {
    case TSDB_DATA_TYPE_FLOAT: {
      float* p = (float*) pLeft->data;
      float* out = (float*) pOutput->data;

      for (int32_t i = 0; i < pLeft->num; ++i) {
        out[i] = floorf(p[i]);
      }
    }

    case TSDB_DATA_TYPE_DOUBLE: {
      double* p = (double*) pLeft->data;
      double* out = (double*) pOutput->data;

      for (int32_t i = 0; i < pLeft->num; ++i) {
        out[i] = floor(p[i]);
      }
    }

    default:
      memcpy(pOutput->data, pLeft->data, pLeft->num* pLeft->bytes);
  }
}

static void _tabs(SScalarParam* pOutput, size_t numOfInput, const SScalarParam *pLeft) {
  assignBasicParaInfo(pOutput, pLeft);
  assert(numOfInput == 1);

  switch (pLeft->bytes) {
    case TSDB_DATA_TYPE_FLOAT: {
      float* p = (float*) pLeft->data;
      float* out = (float*) pOutput->data;
      for (int32_t i = 0; i < pLeft->num; ++i) {
        out[i] = (p[i] > 0)? p[i]:-p[i];
      }
    }

    case TSDB_DATA_TYPE_DOUBLE: {
      double* p = (double*) pLeft->data;
      double* out = (double*) pOutput->data;
      for (int32_t i = 0; i < pLeft->num; ++i) {
        out[i] = (p[i] > 0)? p[i]:-p[i];
      }
    }

    case TSDB_DATA_TYPE_TINYINT: {
      int8_t* p = (int8_t*) pLeft->data;
      int8_t* out = (int8_t*) pOutput->data;
      for (int32_t i = 0; i < pLeft->num; ++i) {
        out[i] = (p[i] > 0)? p[i]:-p[i];
      }
    }

    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t* p = (int16_t*) pLeft->data;
      int16_t* out = (int16_t*) pOutput->data;
      for (int32_t i = 0; i < pLeft->num; ++i) {
        out[i] = (p[i] > 0)? p[i]:-p[i];
      }
    }

    case TSDB_DATA_TYPE_INT: {
      int32_t* p = (int32_t*) pLeft->data;
      int32_t* out = (int32_t*) pOutput->data;
      for (int32_t i = 0; i < pLeft->num; ++i) {
        out[i] = (p[i] > 0)? p[i]:-p[i];
      }
    }

    case TSDB_DATA_TYPE_BIGINT: {
      int64_t* p = (int64_t*) pLeft->data;
      int64_t* out = (int64_t*) pOutput->data;
      for (int32_t i = 0; i < pLeft->num; ++i) {
        out[i] = (p[i] > 0)? p[i]:-p[i];
      }
    }

    default:
      memcpy(pOutput->data, pLeft->data, pLeft->num* pLeft->bytes);
  }
}

static void tround(SScalarParam* pOutput, size_t numOfInput, const SScalarParam *pLeft) {
  assignBasicParaInfo(pOutput, pLeft);
  assert(numOfInput == 1);

  switch (pLeft->bytes) {
    case TSDB_DATA_TYPE_FLOAT: {
      float* p = (float*) pLeft->data;
      float* out = (float*) pOutput->data;
      for (int32_t i = 0; i < pLeft->num; ++i) {
        out[i] = roundf(p[i]);
      }
    }

    case TSDB_DATA_TYPE_DOUBLE: {
      double* p = (double*) pLeft->data;
      double* out = (double*) pOutput->data;
      for (int32_t i = 0; i < pLeft->num; ++i) {
        out[i] = round(p[i]);
      }
    }

    default:
      memcpy(pOutput->data, pLeft->data, pLeft->num* pLeft->bytes);
  }
}

static void tlength(SScalarParam* pOutput, size_t numOfInput, const SScalarParam *pLeft) {
  assert(numOfInput == 1);

  int64_t* out = (int64_t*) pOutput->data;
  char* s = pLeft->data;

  for(int32_t i = 0; i < pLeft->num; ++i) {
    out[i] = varDataLen(POINTER_SHIFT(s, i * pLeft->bytes));
  }
}

static void tconcat(SScalarParam* pOutput, size_t numOfInput, const SScalarParam *pLeft) {
  assert(numOfInput > 0);

  int32_t rowLen = 0;
  int32_t num = 1;
  for(int32_t i = 0; i < numOfInput; ++i) {
    rowLen += pLeft[i].bytes;

    if (pLeft[i].num > 1) {
      num = pLeft[i].num;
    }
  }

  pOutput->data = realloc(pOutput->data, rowLen * num);
  assert(pOutput->data);

  char* rstart = pOutput->data;
  for(int32_t i = 0; i < num; ++i) {

    char* s = rstart;
    varDataSetLen(s, 0);
    for (int32_t j = 0; j < numOfInput; ++j) {
      char* p1 = POINTER_SHIFT(pLeft[j].data, i * pLeft[j].bytes);

      memcpy(varDataVal(s) + varDataLen(s), varDataVal(p1), varDataLen(p1));
      varDataLen(s) += varDataLen(p1);
    }

    rstart += rowLen;
  }
}

static void tltrim(SScalarParam* pOutput, size_t numOfInput, const SScalarParam *pLeft) {

}

static void trtrim(SScalarParam* pOutput, size_t numOfInput, const SScalarParam *pLeft) {

}

static void reverseCopy(char* dest, const char* src, int16_t type, int32_t numOfRows) {
  switch(type) {
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_UTINYINT:{
      int8_t* p = (int8_t*) dest;
      int8_t* pSrc = (int8_t*) src;

      for(int32_t i = 0; i < numOfRows; ++i) {
        p[i] = pSrc[numOfRows - i - 1];
      }
      return;
    }

    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_USMALLINT:{
      int16_t* p = (int16_t*) dest;
      int16_t* pSrc = (int16_t*) src;

      for(int32_t i = 0; i < numOfRows; ++i) {
        p[i] = pSrc[numOfRows - i - 1];
      }
      return;
    }
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_UINT: {
      int32_t* p = (int32_t*) dest;
      int32_t* pSrc = (int32_t*) src;

      for(int32_t i = 0; i < numOfRows; ++i) {
        p[i] = pSrc[numOfRows - i - 1];
      }
      return;
    }
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_UBIGINT: {
      int64_t* p = (int64_t*) dest;
      int64_t* pSrc = (int64_t*) src;

      for(int32_t i = 0; i < numOfRows; ++i) {
        p[i] = pSrc[numOfRows - i - 1];
      }
      return;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      float* p = (float*) dest;
      float* pSrc = (float*) src;

      for(int32_t i = 0; i < numOfRows; ++i) {
        p[i] = pSrc[numOfRows - i - 1];
      }
      return;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      double* p = (double*) dest;
      double* pSrc = (double*) src;

      for(int32_t i = 0; i < numOfRows; ++i) {
        p[i] = pSrc[numOfRows - i - 1];
      }
      return;
    }
    default: assert(0);
  }
}

static void setScalarFuncParam(SScalarParam* param, int32_t type, int32_t bytes, void* pInput, int32_t numOfRows) {
  param->bytes = bytes;
  param->type = type;
  param->num  = numOfRows;
  param->data = pInput;
}


#if 0
int32_t evaluateExprNodeTree(tExprNode* pExprs, int32_t numOfRows, SScalarFuncParam* pOutput, void* param,
                          char* (*getSourceDataBlock)(void*, const char*, int32_t)) {
  if (pExprs == NULL) {
    return 0;
  }

  tExprNode* pLeft = pExprs->_node.pLeft;
  tExprNode* pRight = pExprs->_node.pRight;

  /* the left output has result from the left child syntax tree */
  SScalarFuncParam leftOutput = {0};
  SScalarFuncParam rightOutput = {0};

  if (pLeft->nodeType == TEXPR_BINARYEXPR_NODE || pLeft->nodeType == TEXPR_UNARYEXPR_NODE) {
    leftOutput.data = malloc(sizeof(int64_t) * numOfRows);
    evaluateExprNodeTree(pLeft, numOfRows, &leftOutput, param, getSourceDataBlock);
  }

  // the right output has result from the right child syntax tree
  if (pRight->nodeType == TEXPR_BINARYEXPR_NODE || pRight->nodeType == TEXPR_UNARYEXPR_NODE) {
    rightOutput.data = malloc(sizeof(int64_t) * numOfRows);
    evaluateExprNodeTree(pRight, numOfRows, &rightOutput, param, getSourceDataBlock);
  }

  if (pExprs->nodeType == TEXPR_BINARYEXPR_NODE) {
    _bin_scalar_fn_t OperatorFn = getBinScalarOperatorFn(pExprs->_node.optr);

    SScalarFuncParam left = {0}, right = {0};
    if (pLeft->nodeType == TEXPR_BINARYEXPR_NODE || pLeft->nodeType == TEXPR_UNARYEXPR_NODE) {
      setScalarFuncParam(&left, leftOutput.type, leftOutput.bytes, leftOutput.data, leftOutput.num);
    } else if (pLeft->nodeType == TEXPR_COL_NODE) {
      SSchema* pschema = pLeft->pSchema;
      char*    pLeftInputData = getSourceDataBlock(param, pschema->name, pschema->colId);
      setScalarFuncParam(&right, pschema->type, pschema->bytes, pLeftInputData, numOfRows);
    } else if (pLeft->nodeType == TEXPR_VALUE_NODE) {
      SVariant* pVar = pRight->pVal;
      setScalarFuncParam(&left, pVar->nType, pVar->nLen, &pVar->i, 1);
    }

    if (pRight->nodeType == TEXPR_BINARYEXPR_NODE || pRight->nodeType == TEXPR_UNARYEXPR_NODE) {
      setScalarFuncParam(&right, rightOutput.type, rightOutput.bytes, rightOutput.data, rightOutput.num);
    } else if (pRight->nodeType == TEXPR_COL_NODE) {  // exprLeft + columnRight
      SSchema* pschema = pRight->pSchema;
      char*    pInputData = getSourceDataBlock(param, pschema->name, pschema->colId);
      setScalarFuncParam(&right, pschema->type, pschema->bytes, pInputData, numOfRows);
    } else if (pRight->nodeType == TEXPR_VALUE_NODE) {  // exprLeft + 12
      SVariant* pVar = pRight->pVal;
      setScalarFuncParam(&right, pVar->nType, pVar->nLen, &pVar->i, 1);
    }

    void* outputBuf = pOutput->data;
    if (isStringOp(pExprs->_node.optr)) {
      outputBuf = realloc(pOutput->data, (left.bytes + right.bytes) * left.num);
    }

    OperatorFn(&left, &right, outputBuf, TSDB_ORDER_ASC);
    // Set the result info
    setScalarFuncParam(pOutput, TSDB_DATA_TYPE_DOUBLE, sizeof(double), outputBuf, numOfRows);
  } else if (pExprs->nodeType == TEXPR_UNARYEXPR_NODE) {
    _unary_scalar_fn_t OperatorFn = getUnaryScalarOperatorFn(pExprs->_node.optr);
    SScalarFuncParam   left = {0};

    if (pLeft->nodeType == TEXPR_BINARYEXPR_NODE || pLeft->nodeType == TEXPR_UNARYEXPR_NODE) {
      setScalarFuncParam(&left, leftOutput.type, leftOutput.bytes, leftOutput.data, leftOutput.num);
    } else if (pLeft->nodeType == TEXPR_COL_NODE) {
      SSchema* pschema = pLeft->pSchema;
      char*    pLeftInputData = getSourceDataBlock(param, pschema->name, pschema->colId);
      setScalarFuncParam(&left, pschema->type, pschema->bytes, pLeftInputData, numOfRows);
    } else if (pLeft->nodeType == TEXPR_VALUE_NODE) {
      SVariant* pVar = pLeft->pVal;
      setScalarFuncParam(&left, pVar->nType, pVar->nLen, &pVar->i, 1);
    }

    // reserve enough memory buffer
    if (isBinaryStringOp(pExprs->_node.optr)) {
      void* outputBuf = realloc(pOutput->data, left.bytes * left.num);
      assert(outputBuf != NULL);
      pOutput->data = outputBuf;
    }

    OperatorFn(&left, pOutput);
  }

  tfree(leftOutput.data);
  tfree(rightOutput.data);

  return 0;
}
#endif


SScalarFunctionInfo scalarFunc[8] = {
    {"ceil",   FUNCTION_TYPE_SCALAR, FUNCTION_CEIL,   tceil},
    {"floor",  FUNCTION_TYPE_SCALAR, FUNCTION_FLOOR,  tfloor},
    {"abs",    FUNCTION_TYPE_SCALAR, FUNCTION_ABS,    _tabs},
    {"round",  FUNCTION_TYPE_SCALAR, FUNCTION_ROUND,  tround},
    {"length", FUNCTION_TYPE_SCALAR, FUNCTION_LENGTH, tlength},
    {"concat", FUNCTION_TYPE_SCALAR, FUNCTION_CONCAT, tconcat},
    {"ltrim",  FUNCTION_TYPE_SCALAR, FUNCTION_LTRIM, tltrim},
    {"rtrim",  FUNCTION_TYPE_SCALAR, FUNCTION_RTRIM, trtrim},
};

void setScalarFunctionSupp(struct SScalarFunctionSupport* sas, SExprInfo *pExprInfo, SSDataBlock* pSDataBlock) {
  sas->numOfCols = (int32_t) pSDataBlock->info.numOfCols;
  sas->pExprInfo = pExprInfo;
  if (sas->colList != NULL) {
    return;
  }

  sas->colList = calloc(1, pSDataBlock->info.numOfCols*sizeof(SColumnInfo));
  for(int32_t i = 0; i < sas->numOfCols; ++i) {
    SColumnInfoData* pColData = taosArrayGet(pSDataBlock->pDataBlock, i);
    sas->colList[i] = pColData->info;
  }

  sas->data = calloc(sas->numOfCols, POINTER_BYTES);

  // set the input column data
  for (int32_t f = 0; f < pSDataBlock->info.numOfCols; ++f) {
    SColumnInfoData *pColumnInfoData = taosArrayGet(pSDataBlock->pDataBlock, f);
    sas->data[f] = pColumnInfoData->pData;
  }
}

SScalarFunctionSupport* createScalarFuncSupport(int32_t num) {
  SScalarFunctionSupport* pSupp = calloc(num, sizeof(SScalarFunctionSupport));
  return pSupp;
}

void destroyScalarFuncSupport(struct SScalarFunctionSupport* pSupport, int32_t num) {
  if (pSupport == NULL) {
    return;
  }

  for(int32_t i = 0; i < num; ++i) {
    SScalarFunctionSupport* pSupp = &pSupport[i];
    tfree(pSupp->data);
    tfree(pSupp->colList);
  }

  tfree(pSupport);
}